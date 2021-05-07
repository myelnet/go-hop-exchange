package exchange

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/jpillora/backoff"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/mux"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/myelnet/pop/internal/utils"
	"github.com/myelnet/pop/retrieval/deal"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// We stay compatible with lotus nodes so we can retrieve from lotus providers too

// FilQueryProtocolID is the protocol for querying information about retrieval
// deal parameters from Filecoin storage miners
const FilQueryProtocolID = protocol.ID("/fil/retrieval/qry/1.0.0")

// PopQueryProtocolID is the protocol for exchanging information about retrieval
// deal parameters from retrieval providers
const PopQueryProtocolID = protocol.ID("/myel/pop/query/1.0")

const (
	// MaxStreamOpenAttempts is the number of times we try opening a stream with a given peer before giving up
	MaxStreamOpenAttempts = 5
	// MinAttemptDuration is the minimum amount of time we should wait before trying again
	MinAttemptDuration = 1 * time.Second
	// MaxAttemptDuration is maximum delay we should wait before trying again
	MaxAttemptDuration = 5 * time.Minute
)

// OpenStream is a generic method for opening streams with a backoff.
func OpenStream(ctx context.Context, h host.Host, p peer.ID, protos []protocol.ID) (network.Stream, error) {
	b := &backoff.Backoff{
		Min:    MinAttemptDuration,
		Max:    MaxAttemptDuration,
		Factor: MaxStreamOpenAttempts,
		Jitter: true,
	}

	for {
		s, err := h.NewStream(ctx, p, protos...)
		if err == nil {
			return s, err
		}
		fmt.Println("trying again", err)

		nAttempts := b.Attempt()
		if nAttempts == MaxStreamOpenAttempts {
			return nil, fmt.Errorf("exhausted %d attempts but failed to open stream, err: %w", int(MaxStreamOpenAttempts), err)
		}
		d := b.Duration()
		time.Sleep(d)
	}

}

//QueryStream wraps convenience methods for writing and reading CBOR messages from a stream.
type QueryStream struct {
	p   peer.ID
	rw  mux.MuxedStream
	buf *bufio.Reader
}

// ReadQuery reads and decodes a CBOR encoded Query from a stream buffer.
func (qs *QueryStream) ReadQuery() (deal.Query, error) {
	var q deal.Query

	if err := q.UnmarshalCBOR(qs.buf); err != nil {
		return deal.Query{}, err

	}

	return q, nil
}

// WriteQuery encodes and writes a CBOR Query message to a stream.
func (qs *QueryStream) WriteQuery(q deal.Query) error {
	return cborutil.WriteCborRPC(qs.rw, &q)
}

// ReadQueryResponse reads and decodes a QueryResponse CBOR message from a stream buffer.
func (qs *QueryStream) ReadQueryResponse() (deal.QueryResponse, error) {
	var resp deal.QueryResponse

	if err := resp.UnmarshalCBOR(qs.buf); err != nil {
		return deal.QueryResponse{}, err
	}

	return resp, nil
}

// WriteQueryResponse encodes and writes a CBOR QueryResponse message to a stream
func (qs *QueryStream) WriteQueryResponse(qr deal.QueryResponse) error {
	return cborutil.WriteCborRPC(qs.rw, &qr)
}

// Close the underlying stream
func (qs *QueryStream) Close() error {
	return qs.rw.Close()
}

// OtherPeer returns the peer ID of the other peer at the end of the stream
func (qs *QueryStream) OtherPeer() peer.ID {
	return qs.p
}

// MessageTracker returns metadata about messages so we know if they're destined to this host
// or should be forwarded
type MessageTracker interface {
	// Published checks if we are actually the peer expecting this offer
	Published(string) bool
	// Sender returns the peer we think this message should be forwarded to
	Sender(string) (peer.ID, error)
}

// ReceiveResponse is fired every time we get a response
type ReceiveResponse func(peer.AddrInfo, deal.QueryResponse)

// ResponseFunc takes a Query and returns a Response or an error if request is declined
type ResponseFunc func(context.Context, peer.ID, Region, deal.Query) (deal.QueryResponse, error)

// GossipRouting is a content routing service to find content providers using pubsub gossip routing
type GossipRouting struct {
	h              host.Host
	ps             *pubsub.PubSub
	queryProtocols []protocol.ID
	meta           MessageTracker
	regions        []Region
	// Set initially when starting up the service
	ctx      context.Context
	respFunc ResponseFunc

	rmu         sync.Mutex
	receiveResp ReceiveResponse

	tmu  sync.Mutex
	tops map[*pubsub.Topic]*pubsub.Subscription
}

// NewGossipRouting creates a new GossipRouting service
func NewGossipRouting(h host.Host, ps *pubsub.PubSub, meta MessageTracker, rgs []Region) *GossipRouting {
	routing := &GossipRouting{
		h:       h,
		ps:      ps,
		meta:    meta,
		regions: rgs,
		tops:    make(map[*pubsub.Topic]*pubsub.Subscription),
		queryProtocols: []protocol.ID{
			FilQueryProtocolID,
			PopQueryProtocolID,
		},
	}
	return routing
}

// StartProviding opens up our gossip subscription and sets our stream handler
func (gr *GossipRouting) StartProviding(ctx context.Context, fn ResponseFunc) error {
	// We only need to handle the Pop query protocol since Fil is for querying storage miners
	gr.h.SetStreamHandler(PopQueryProtocolID, gr.handleQueryResponse)
	gr.ctx = ctx
	gr.respFunc = fn

	for _, r := range gr.regions {
		gr.JoinTopic(r.Name)
	}

	return nil
}

func topicName(t string) string {
	return fmt.Sprintf("%s/%s", PopQueryProtocolID, t)
}

// JoinTopic creates a new topic from the given string and subscribes to it
func (gr *GossipRouting) JoinTopic(t string) error {
	top, err := gr.ps.Join(topicName(t))
	if err != nil {
		return err
	}
	sub, err := top.Subscribe()
	if err != nil {
		return err
	}
	go gr.pump(sub)

	gr.tmu.Lock()
	gr.tops[top] = sub
	gr.tmu.Unlock()
	return nil
}

// LeaveTopic cancels a subscription and removes that topic from routing
func (gr *GossipRouting) LeaveTopic(t string) {
	gr.tmu.Lock()
	defer gr.tmu.Unlock()
	for top, sub := range gr.tops {
		if top.String() == topicName(t) {
			sub.Cancel()
			delete(gr.tops, top)
			return
		}
	}
}

func (gr *GossipRouting) pump(sub *pubsub.Subscription) {
	r := RegionFromTopic(sub.Topic())
	for {
		msg, err := sub.Next(gr.ctx)
		if err != nil {
			return
		}
		if msg.ReceivedFrom == gr.h.ID() {
			continue
		}
		m := new(deal.Query)
		if err := m.UnmarshalCBOR(bytes.NewReader(msg.Data)); err != nil {
			continue
		}
		resp, err := gr.respFunc(gr.ctx, msg.ReceivedFrom, r, *m)
		if err != nil {
			fmt.Println("declined query", err)
			continue
		}

		qs, err := gr.NewQueryStream(msg.ReceivedFrom)
		if err != nil {
			fmt.Println("failed to create response query stream", err)
			continue
		}
		resp.Message, err = gr.ResponseMsg(msg.Message)
		if err != nil {
			continue
		}
		if err := qs.WriteQueryResponse(resp); err != nil {
			fmt.Printf("retrieval query: WriteCborRPC: %s\n", err)
			continue
		}

	}
}

// ResponseMsg prepares the QueryResponse Message payload
func (gr *GossipRouting) ResponseMsg(msg *pb.Message) (string, error) {
	addrs, err := gr.Addrs()
	if err != nil {
		return "", err
	}
	mid := pubsub.DefaultMsgIdFn(msg)
	// Our message payload includes the message ID and the recipient peer address
	// The index indicates where to slice the string to extract both values
	p := fmt.Sprintf("%d%s%s", len(mid), mid, string(addrs[0].Bytes()))
	return p, nil
}

// QueryPeer asks another peer directly for retrieval conditions
func (gr *GossipRouting) QueryPeer(p peer.AddrInfo, root cid.Cid, fn ReceiveResponse) error {
	stream, err := gr.NewQueryStream(p.ID)
	if err != nil {
		return err
	}
	defer stream.Close()

	err = stream.WriteQuery(deal.Query{
		PayloadCID:  root,
		QueryParams: deal.QueryParams{}, // Filecoin does not support selectors in queries yet
	})
	if err != nil {
		return err
	}

	res, err := stream.ReadQueryResponse()
	if err != nil {
		return err
	}
	fn(p, res)
	return nil
}

// Query asks the gossip network of providers if anyone can provide the blocks we're looking for
// it blocks execution until our conditions are satisfied
func (gr *GossipRouting) Query(ctx context.Context, root cid.Cid, sel ipld.Node) error {
	params, err := deal.NewQueryParams(sel)
	if err != nil {
		return err
	}
	m := deal.Query{
		PayloadCID:  root,
		QueryParams: params,
	}

	buf := new(bytes.Buffer)
	if err := m.MarshalCBOR(buf); err != nil {
		return err
	}

	bytes := buf.Bytes()
	// publish to all regions this exchange joined
	gr.tmu.Lock()
	defer gr.tmu.Unlock()
	for topic := range gr.tops {
		if err := topic.Publish(ctx, bytes); err != nil {
			return err
		}
	}

	return nil
}

// SetReceiver sets a callback to receive discovery responses
func (gr *GossipRouting) SetReceiver(fn ReceiveResponse) {
	gr.rmu.Lock()
	gr.receiveResp = fn
	gr.rmu.Unlock()
}

// NewQueryStream creates a new query stream using the provided peer.ID to handle the Query protocols
func (gr *GossipRouting) NewQueryStream(dest peer.ID) (*QueryStream, error) {
	s, err := OpenStream(context.Background(), gr.h, dest, gr.queryProtocols)
	if err != nil {
		return nil, err
	}
	buffered := bufio.NewReaderSize(s, 16)
	return &QueryStream{p: dest, rw: s, buf: buffered}, nil
}

// handleQueryResponse reads a QueryResponse message from an incoming stream, reads the Message
// if a message is provided, it parses the ID embedded in it and checks if the reference matches
// one we published. If we did publish it means we are expecting the response to we read the response
// and send it to the receiver if not and we have a sender for the message reference we forward it back
// if there is no message attached with the response it might be sent from a Filecoin storage miner so
// we still try to send it to the receiver.
func (gr *GossipRouting) handleQueryResponse(s network.Stream) {
	buffered := bufio.NewReaderSize(s, 16)
	defer s.Close()

	buf := new(bytes.Buffer)
	msg, err := PeekResponseMsg(buffered, buf)
	if err != nil {
		fmt.Println("failed to peek message", err)
		return
	}
	// Here we handle messages from Filecoin miners
	if len(msg) == 0 {
		gr.rmu.Lock()
		defer gr.rmu.Unlock()
		if gr.receiveResp == nil {
			fmt.Println("received resp")
			return
		}
		var resp deal.QueryResponse
		if err := resp.UnmarshalCBOR(buf); err != nil && !errors.Is(err, io.EOF) {
			fmt.Println("failed to read query response", err)
			return
		}
		gr.receiveResp(gr.h.Peerstore().PeerInfo(s.Conn().RemotePeer()), resp)
		return
	}
	// Get the index where to split
	is, err := strconv.ParseInt(msg[:2], 10, 64)
	if err != nil {
		fmt.Println("failed to parse index", err)
		return
	}
	msgID := msg[2 : is+2]
	// The receiver should know if we issued the query if it's not the case
	// it means we must forward it to whichever peer sent us the query
	if !gr.meta.Published(msgID) {
		to, err := gr.meta.Sender(msgID)
		if err != nil {
			fmt.Println("failed to find message recipient", err)
			return
		}
		w, err := OpenStream(context.Background(), gr.h, to, gr.queryProtocols)
		if err != nil {
			fmt.Println("failed to open stream", err)
			return
		}
		if _, err := io.Copy(w, buf); err != nil {
			fmt.Println("failed to forward buffer", err)
		}
		return
	}
	gr.rmu.Lock()
	defer gr.rmu.Unlock()
	// Stop if we don't have a receiver set
	if gr.receiveResp == nil {
		return
	}

	rec, err := utils.AddrBytesToAddrInfo([]byte(msg[is+2:]))
	if err != nil {
		fmt.Println("failed to parse addr bytes", err)
		return
	}

	var resp deal.QueryResponse
	if err := resp.UnmarshalCBOR(buf); err != nil && !errors.Is(err, io.EOF) {
		fmt.Println("failed to read query response", err)
		return
	}

	gr.receiveResp(*rec, resp)
}

// Addrs returns the host's p2p addresses
func (gr *GossipRouting) Addrs() ([]ma.Multiaddr, error) {
	return peer.AddrInfoToP2pAddrs(host.InfoFromHost(gr.h))
}

// AddAddrs adds a new peer into the host peerstore
func (gr *GossipRouting) AddAddrs(p peer.ID, addrs []ma.Multiaddr) {
	gr.h.Peerstore().AddAddrs(p, addrs, 8*time.Hour)
}

// PeekResponseMsg decodes the Message field only and returns the value while copying the bytes in a buffer
func PeekResponseMsg(r io.Reader, buf *bytes.Buffer) (string, error) {
	tr := io.TeeReader(r, buf)
	br := cbg.GetPeeker(tr)
	scratch := make([]byte, 8)
	_, n, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return "", err
	}
	var name string
	var msg string
	for i := uint64(0); i < n; i++ {
		{
			sval, err := cbg.ReadStringBuf(br, scratch)
			if err != nil {
				return "", err
			}
			name = string(sval)
		}
		if name == "Message" {
			sval, err := cbg.ReadStringBuf(br, scratch)
			if err != nil {
				return "", err
			}
			msg = string(sval)
		} else {
			cbg.ScanForLinks(br, func(cid.Cid) {})
		}
	}
	return msg, nil
}

// GossipTracer tracks messages we've seen so we can relay responses back to the publisher
type GossipTracer struct {
	pmu       sync.Mutex
	published map[string]bool
	smu       sync.Mutex
	senders   map[string]peer.ID
}

// NewGossipTracer creates a new instance of GossipTracer
func NewGossipTracer() *GossipTracer {
	return &GossipTracer{
		published: make(map[string]bool),
		senders:   make(map[string]peer.ID),
	}
}

// Trace gets triggered for every internal gossip sub operation
func (gt *GossipTracer) Trace(evt *pb.TraceEvent) {
	if evt.PublishMessage != nil {
		gt.pmu.Lock()
		gt.published[string(evt.PublishMessage.MessageID)] = true
		gt.pmu.Unlock()
	}
	if evt.DeliverMessage != nil {
		msg := evt.DeliverMessage
		gt.smu.Lock()
		gt.senders[string(msg.MessageID)] = peer.ID(msg.ReceivedFrom)
		gt.smu.Unlock()
	}
}

// Published checks if we were the publisher of a message
func (gt *GossipTracer) Published(mid string) bool {
	gt.pmu.Lock()
	defer gt.pmu.Unlock()
	return gt.published[mid]
}

// Sender returns the peer who sent us a message
func (gt *GossipTracer) Sender(mid string) (peer.ID, error) {
	gt.smu.Lock()
	defer gt.smu.Unlock()
	p, ok := gt.senders[mid]
	if !ok {
		return "", errors.New("no sender found")
	}
	return p, nil
}
