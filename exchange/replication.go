package exchange

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-multistore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-graphsync/storeutil"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-ipld-prime"
	"github.com/jpillora/backoff"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/mux"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	sel "github.com/myelnet/pop/selectors"
	"gonum.org/v1/gonum/stat/distuv"
)

//go:generate cbor-gen-for Request

// PopRequestProtocolID is the protocol for requesting caches to store new content
const PopRequestProtocolID = protocol.ID("/myel/pop/request/1.0")

// Request describes the content to pull
type Request struct {
	Method     Method
	PayloadCID cid.Cid
	Size       uint64
}

// Type defines Request as a datatransfer voucher for pulling the data from the request
func (Request) Type() datatransfer.TypeIdentifier {
	return "ReplicationRequestVoucher"
}

// Method is the replication request method
type Method uint64

const (
	// Dispatch is an initial request from a content plublisher
	Dispatch Method = iota
	// FetchIndex is a request from one content provider to another to retrieve their index
	FetchIndex
)

// IndexEvt is emitted when a new index is loaded in the replication service
type IndexEvt struct {
	Root cid.Cid
}

// RequestStream allows reading and writing CBOR encoded messages to a stream
type RequestStream struct {
	p   peer.ID
	rw  mux.MuxedStream
	buf *bufio.Reader
}

// ReadRequest reads and decodes a CBOR encoded Request message from a stream buffer
func (rs *RequestStream) ReadRequest() (Request, error) {
	var m Request
	if err := m.UnmarshalCBOR(rs.buf); err != nil {
		return Request{}, err
	}
	return m, nil
}

// WriteRequest encodes and writes a Request message to a stream
func (rs *RequestStream) WriteRequest(m Request) error {
	return cborutil.WriteCborRPC(rs.rw, &m)
}

// Close the stream
func (rs *RequestStream) Close() error {
	return rs.rw.Close()
}

// OtherPeer returns the peer ID of the peer at the other end of the stream
func (rs *RequestStream) OtherPeer() peer.ID {
	return rs.p
}

// RoutedRetriever is a generic interface providing a method to find and retrieve content on the exchange
type RoutedRetriever interface {
	FindAndRetrieve(context.Context, cid.Cid) error
}

// Replication manages the network replication scheme, it keeps track of read and write requests
// and decides whether to join a replication scheme or not
type Replication struct {
	h         host.Host
	dt        datatransfer.Manager
	pm        *PeerMgr
	hs        *HeyService
	idx       *Index
	rgs       []Region
	reqProtos []protocol.ID   // protocol IDs used to send and receive requests
	emitter   event.Emitter   // emitter is triggered when new content is loaded
	interval  time.Duration   // index update interval
	rtv       RoutedRetriever // used for retrieving new content

	cmu     sync.Mutex
	cluster string // cluster ID this node is part of (based on peer ID strings)

	pmu   sync.Mutex
	pulls map[cid.Cid]*peer.Set // authorized pull requests

	smu    sync.Mutex
	stores map[cid.Cid]*multistore.Store // stores used for temporarily storing peer indexes
}

// NewReplication starts the exchange replication management system
func NewReplication(h host.Host, idx *Index, dt datatransfer.Manager, rtv RoutedRetriever, rgs []Region) *Replication {
	pm := NewPeerMgr(h, rgs)
	r := &Replication{
		h:         h,
		pm:        pm,
		dt:        dt,
		rgs:       rgs,
		idx:       idx,
		rtv:       rtv,
		cluster:   h.ID().String(), // initially starts alone in its own cluster
		interval:  60 * time.Second,
		reqProtos: []protocol.ID{PopRequestProtocolID},
		pulls:     make(map[cid.Cid]*peer.Set),
		stores:    make(map[cid.Cid]*multistore.Store),
	}
	r.hs = NewHeyService(h, pm, r)
	h.SetStreamHandler(PopRequestProtocolID, r.handleRequest)
	r.dt.RegisterVoucherType(&Request{}, r)
	r.dt.RegisterTransportConfigurer(&Request{}, TransportConfigurer(r.idx, r, h.ID()))
	r.emitter, _ = h.EventBus().Emitter(new(IndexEvt))

	// TODO: clean this up
	r.dt.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		if event.Code == datatransfer.Error && channelState.Recipient() == h.ID() {
			// If transfers fail and we're the recipient we need to remove it from our index
			r.idx.DropRef(channelState.BaseCID())
		}
	})

	return r
}

// Start initiates listeners to update our scheme if new peers join
func (r *Replication) Start(ctx context.Context) error {
	sub, err := r.h.EventBus().Subscribe(new(HeyEvt), eventbus.BufSize(16))
	if err != nil {
		return err
	}
	// Any time we receive a new index, check if any refs should be added to our supply
	go r.refreshIndex(ctx)
	go r.pumpIndexes(ctx, sub)
	if err := r.hs.Run(ctx); err != nil {
		return err
	}
	return nil
}

// pumpIndexes iterates over a subscription to new Hey msg received when connecting with other provider peers
// it keeps index roots into a queue and iteratively fetches them. We could potentially fetch them in parallel
// but we ideally don't want this to be a burden on the node ressources so we take it easy
func (r *Replication) pumpIndexes(ctx context.Context, sub event.Subscription) {
	var q []HeyEvt
	var fetchDone chan fetchResult
	for {
		select {
		case <-ctx.Done():
			return
		case evt := <-sub.Out():
			hevt := evt.(HeyEvt)
			// check if we should join that peer's cluster or not
			r.upgradeCluster(hevt.Cluster, hevt.Peer)
			if hevt.IndexRoot != nil {
				if fetchDone == nil {
					fetchDone = make(chan fetchResult, 1)
					go func() {
						err := r.fetchIndex(ctx, hevt)
						fetchDone <- fetchResult{*hevt.IndexRoot, err}
					}()
					continue
				}
				q = append(q, hevt)
			}
			// We can probably ignore errors
		case res := <-fetchDone:
			if res.err == nil {
				go func(rt cid.Cid) {
					store := r.GetStore(rt)
					err := r.idx.LoadInterest(rt, cbor.NewCborStore(store.Bstore))
					if err != nil {
						fmt.Println("failed to load interest", err)
						return
					}
				}(res.root)
			}
			if len(q) > 0 {
				fetchDone = make(chan fetchResult, 1)
				go func(hvt HeyEvt) {
					err := r.fetchIndex(ctx, hvt)
					fetchDone <- fetchResult{*hvt.IndexRoot, err}
				}(q[0])
				q = q[1:]
			}
		}
	}
}

// upgradeCluster computes if the node should join the cluster of a new connected peer or not
func (r *Replication) upgradeCluster(cl string, pr peer.ID) {
	peers := r.pm.AllPeers()
	var sum, mean, sd float64
	for _, v := range peers {
		sum += float64(v.Latency)
	}
	mean = sum / float64(len(peers))
	for _, v := range peers {
		sd += math.Pow(float64(v.Latency)-mean, 2)
	}
	sd = math.Sqrt(sd / float64(len(peers)))
	dist := distuv.Normal{
		Mu:    mean,
		Sigma: sd,
	}
	bern := distuv.Bernoulli{
		P: dist.CDF(float64(peers[pr].Latency)),
	}
	p := bern.Rand()
	if p == 1 {
		r.cmu.Lock()
		r.cluster = cl
		r.cmu.Unlock()
	}
}

// Cluster returns the name of the current cluster this peer belongs to
func (r *Replication) Cluster() string {
	r.cmu.Lock()
	defer r.cmu.Unlock()
	return r.cluster
}

// refreshIndex is a long running process that regularly inspects received indexes
// and if usage is high enough retrieves the content at market price
func (r *Replication) refreshIndex(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			refs, err := r.idx.Interesting()
			if err != nil || len(refs) == 0 {
				continue
			}
			fmt.Println("tick", r.h.ID(), len(refs))

			for ref := range refs {
				// let's get it
				err := r.rtv.FindAndRetrieve(ctx, ref.PayloadCID)
				if err != nil {
					continue
				}
				err = r.idx.DropInterest(ref.PayloadCID)
				r.emitter.Emit(IndexEvt{
					Root: ref.PayloadCID,
				})
			}
		case <-ctx.Done():
			return
		}
	}
}

// fetchResult associates the root of the index fetched and a possible error
type fetchResult struct {
	root cid.Cid
	err  error
}

// fetchIndex handles the data transfer for retrieving the index of a given peer anounced in a Hey
// msg. It blocks until the transfer is completed or fails.
func (r *Replication) fetchIndex(ctx context.Context, hvt HeyEvt) error {
	done := make(chan error, 1)
	rcid := *hvt.IndexRoot
	unsub := r.dt.SubscribeToEvents(func(event datatransfer.Event, chState datatransfer.ChannelState) {
		root := chState.BaseCID()
		if root != rcid {
			return
		}
		switch chState.Status() {
		case datatransfer.Completed:
			done <- nil
		case datatransfer.Failed, datatransfer.Cancelled:
			done <- fmt.Errorf(chState.Message())
		}
	})
	defer unsub()
	req := Request{
		Method:     FetchIndex,
		PayloadCID: rcid,
	}

	store, err := r.idx.ms.Get(r.idx.ms.Next())
	if err != nil {
		return err
	}
	r.smu.Lock()
	r.stores[rcid] = store
	r.smu.Unlock()

	_, err = r.dt.OpenPullDataChannel(ctx, hvt.Peer, &req, rcid, sel.Hamt())
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// GetStore returns the store used for a given root index
func (r *Replication) GetStore(k cid.Cid) *multistore.Store {
	r.smu.Lock()
	defer r.smu.Unlock()
	return r.stores[k]
}

// balanceIndex checks if any content in the interest list is more popular than content in the supply
// in which case it will try to retrieve it from the network and insert it in there

// GetHey formats a new Hey message
func (r *Replication) GetHey() Hey {
	regions := make([]RegionCode, len(r.rgs))
	i := 0
	for _, rg := range r.rgs {
		regions[i] = rg.Code
		i++
	}
	r.cmu.Lock()
	h := Hey{
		Regions: regions,
		Cluster: r.cluster,
	}
	r.cmu.Unlock()
	idxr := r.idx.Root()
	if idxr != cid.Undef {
		h.IndexRoot = &idxr
	}
	return h
}

// NewRequestStream opens a multi stream with the given peer and sets up the interface to write requests to it
func (r *Replication) NewRequestStream(dest peer.ID) (*RequestStream, error) {
	s, err := OpenStream(context.Background(), r.h, dest, r.reqProtos)
	if err != nil {
		return nil, err
	}
	buf := bufio.NewReaderSize(s, 16)
	return &RequestStream{p: dest, rw: s, buf: buf}, nil
}

func (r *Replication) handleRequest(s network.Stream) {
	p := s.Conn().RemotePeer()
	buffered := bufio.NewReaderSize(s, 16)
	rs := &RequestStream{p, s, buffered}
	defer rs.Close()
	req, err := rs.ReadRequest()
	if err != nil {
		return
	}
	// Only the dispatch method is streamed directly at this time
	switch req.Method {
	case Dispatch:
		// TODO: validate request
		// Create a new store to receive our new blocks
		// It will be automatically picked up in the TransportConfigurer
		storeID := r.idx.ms.Next()
		err = r.idx.SetRef(&DataRef{
			PayloadCID:  req.PayloadCID,
			PayloadSize: int64(req.Size),
			StoreID:     storeID,
		})
		if err != nil {
			return
		}
		_, err = r.dt.OpenPullDataChannel(context.TODO(), p, &req, req.PayloadCID, sel.All())
		if err != nil {
			return
		}
	}
}

// PRecord is a provider <> cid mapping for recording who is storing what content
type PRecord struct {
	Provider   peer.ID
	PayloadCID cid.Cid
}

// DispatchOptions exposes parameters to affect the duration of a Dispatch operation
type DispatchOptions struct {
	BackoffMin     time.Duration
	BackoffAttemps int
	RF             int
}

// DefaultDispatchOptions provides useful defaults
// We can change these if the content requires a long transfer time
var DefaultDispatchOptions = DispatchOptions{
	BackoffMin:     2 * time.Second,
	BackoffAttemps: 4,
	RF:             7,
}

// Dispatch to the network until we have propagated the content to enough peers
func (r *Replication) Dispatch(root cid.Cid, size uint64, opt DispatchOptions) chan PRecord {
	req := Request{
		Method:     Dispatch,
		PayloadCID: root,
		Size:       size,
	}
	resChan := make(chan PRecord, opt.RF)
	out := make(chan PRecord, opt.RF)
	// listen for datatransfer events to identify the peers who pulled the content
	unsub := r.dt.SubscribeToEvents(func(event datatransfer.Event, chState datatransfer.ChannelState) {
		if chState.Status() == datatransfer.Completed {
			root := chState.BaseCID()
			if root != req.PayloadCID {
				return
			}
			// The recipient is the provider who received our content
			rec := chState.Recipient()
			resChan <- PRecord{
				Provider:   rec,
				PayloadCID: root,
			}
		}
	})
	go func() {
		defer func() {
			unsub()
			close(out)
		}()
		// The peers we already sent requests to
		rcv := make(map[peer.ID]bool)
		// Set the parameters for backing off after each try
		b := backoff.Backoff{
			Min: opt.BackoffMin,
			Max: 60 * time.Minute,
			// Factor: 2 (default)
		}
		// The number of confirmations we received so far
		n := 0

	requests:
		for {
			// Give up after 6 attemps. Maybe should make this customizable for servers that can afford it
			if int(b.Attempt()) > opt.BackoffAttemps {
				return
			}
			// Select the providers we want to send to minus those we already confirmed
			// received the requests
			providers := r.pm.Peers(opt.RF-n, r.rgs, rcv)

			// Authorize the transfer
			for _, p := range providers {
				r.AuthorizePull(req.PayloadCID, p)
				rcv[p] = true
			}
			if len(providers) > 0 {
				// sendAllRequests
				r.sendAllRequests(req, providers)
			}

			timer := time.NewTimer(b.Duration())
			for {
				select {
				case <-timer.C:

					continue requests
				case r := <-resChan:
					// forward the confirmations to the Response channel
					out <- r
					// increment our results count
					n++
					if n == opt.RF {
						return
					}
				}
			}
		}
	}()
	return out
}

func (r *Replication) sendAllRequests(req Request, peers []peer.ID) {
	for _, p := range peers {
		stream, err := r.NewRequestStream(p)
		if err != nil {
			continue
		}
		err = stream.WriteRequest(req)
		stream.Close()
		if err != nil {
			continue
		}
	}
}

// AuthorizePull adds a peer to a set giving authorization to pull content without payment
// We assume that this authorizes the peer to pull as many links from the root CID as they can
// It runs on the client side to authorize caches
func (r *Replication) AuthorizePull(k cid.Cid, p peer.ID) {
	r.pmu.Lock()
	defer r.pmu.Unlock()
	if set, ok := r.pulls[k]; ok {
		set.Add(p)
		return
	}
	set := peer.NewSet()
	set.Add(p)
	r.pulls[k] = set
}

// ValidatePush returns a stubbed result for a push validation
func (r *Replication) ValidatePush(
	sender peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) (datatransfer.VoucherResult, error) {
	return nil, fmt.Errorf("no pushed accepted")
}

// ValidatePull returns a stubbed result for a pull validation
func (r *Replication) ValidatePull(
	receiver peer.ID,
	voucher datatransfer.Voucher,
	baseCid cid.Cid,
	selector ipld.Node) (datatransfer.VoucherResult, error) {

	request, ok := voucher.(*Request)
	if !ok {
		return nil, fmt.Errorf("bad voucher")
	}
	// TODO: For now fetching someone's index it authorized by default
	// we need some permission system
	if request.Method == FetchIndex {
		return nil, nil
	}

	r.pmu.Lock()
	defer r.pmu.Unlock()
	set, ok := r.pulls[baseCid]
	if !ok {
		return nil, fmt.Errorf("unknown CID")
	}
	if !set.Contains(receiver) {
		return nil, fmt.Errorf("not authorized")
	}
	return nil, nil
}

// StoreConfigurableTransport defines the methods needed to
// configure a data transfer transport use a unique store for a given request
type StoreConfigurableTransport interface {
	UseStore(datatransfer.ChannelID, ipld.Loader, ipld.Storer) error
}

// IdxStoreGetter returns the store used for retrieving a given index root
type IdxStoreGetter interface {
	GetStore(cid.Cid) *multistore.Store
}

// TransportConfigurer configurers the graphsync transport to use a custom blockstore per content
func TransportConfigurer(idx *Index, isg IdxStoreGetter, pid peer.ID) datatransfer.TransportConfigurer {
	return func(channelID datatransfer.ChannelID, voucher datatransfer.Voucher, transport datatransfer.Transport) {
		warn := func(err error) {
			fmt.Println("attempting to configure data store:", err)
		}
		request, ok := voucher.(*Request)
		if !ok {
			return
		}
		gsTransport, ok := transport.(StoreConfigurableTransport)
		if !ok {
			return
		}
		if request.Method == FetchIndex && channelID.Initiator == pid {
			// When we're fetching a new index we store it in a new store
			store := isg.GetStore(request.PayloadCID)
			err := gsTransport.UseStore(channelID, store.Loader, store.Storer)
			if err != nil {
				warn(err)
			}
			return
		}
		if request.Method == FetchIndex {
			loader := storeutil.LoaderForBlockstore(idx.Bstore())
			storer := storeutil.StorerForBlockstore(idx.Bstore())
			err := gsTransport.UseStore(channelID, loader, storer)
			if err != nil {
				warn(err)
			}
			return
		}
		store, err := idx.GetStore(request.PayloadCID)
		if err != nil {
			warn(err)
			return
		}
		err = gsTransport.UseStore(channelID, store.Loader, store.Storer)
		if err != nil {
			warn(err)
		}
	}
}
