package exchange

import (
	"context"
	"fmt"
	"math"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-multistore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/myelnet/pop/filecoin"
	"github.com/myelnet/pop/payments"
	"github.com/myelnet/pop/retrieval"
	"github.com/myelnet/pop/retrieval/client"
	"github.com/myelnet/pop/retrieval/deal"
	"github.com/myelnet/pop/selectors"
	sel "github.com/myelnet/pop/selectors"
	"github.com/myelnet/pop/wallet"
)

// Exchange is a financially incentivized IPLD  block exchange
// powered by Filecoin and IPFS
type Exchange struct {
	h    host.Host
	ds   datastore.Batching
	opts Options
	// pubsub topics
	tops []*pubsub.Topic
	// wallet interface
	w wallet.Driver
	// retrieval handles all metered data transfers
	rtv retrieval.Manager
	// Routing service
	rou *GossipRouting
	// Replication scheme
	rpl *Replication
	// Index keeps track of all content stored under this exchange
	idx *Index
}

// New creates a long running exchange process from a libp2p host, an IPFS datastore and some optional
// modules which are provided by default
func New(ctx context.Context, h host.Host, ds datastore.Batching, opts Options) (*Exchange, error) {
	opts, err := opts.fillDefaults(ctx, h, ds)
	if err != nil {
		return nil, err
	}
	idx, err := NewIndex(
		ds,
		opts.MultiStore,
		// leave a 20% lower bound so we don't evict too frequently
		WithBounds(opts.Capacity, opts.Capacity-uint64(math.Round(float64(opts.Capacity)*0.2))),
	)
	if err != nil {
		return nil, err
	}
	// register a pubsub topic for each region
	exch := &Exchange{
		h:    h,
		ds:   ds,
		opts: opts,
		idx:  idx,
		rou:  NewGossipRouting(h, opts.PubSub, opts.GossipTracer, opts.Regions),
		w:    wallet.NewFromKeystore(opts.Keystore, opts.FilecoinAPI),
	}
	exch.rpl = NewReplication(h, idx, opts.DataTransfer, exch, opts.Regions)
	exch.rpl.interval = opts.RepInterval
	// Make a new default key to be sure we have an address where to receive our payments
	if exch.w.DefaultAddress() == address.Undef {
		_, err = exch.w.NewKey(ctx, wallet.KTSecp256k1)
		if err != nil {
			return nil, err
		}
	}
	exch.rtv, err = retrieval.New(
		ctx,
		opts.MultiStore,
		ds,
		payments.New(ctx, opts.FilecoinAPI, exch.w, ds, opts.Blockstore),
		opts.DataTransfer,
		idx,
		h.ID(),
	)
	if err != nil {
		return nil, err
	}
	if err := exch.rpl.Start(ctx); err != nil {
		return nil, err
	}
	if err := exch.rou.StartProviding(ctx, exch.handleQuery); err != nil {
		return nil, err
	}
	return exch, nil
}

func (e *Exchange) handleQuery(ctx context.Context, p peer.ID, r Region, q deal.Query) (deal.QueryResponse, error) {
	store, err := e.idx.GetStore(q.PayloadCID)
	if err != nil {
		return deal.QueryResponse{}, err
	}
	if q.Selector == nil {
		return deal.QueryResponse{}, fmt.Errorf("no selector provided")
	}
	sel, err := retrieval.DecodeNode(q.QueryParams.Selector)
	if err != nil {
		sel = selectors.All()
	}
	// DAGStat is both a way of checking if we have the blocks and returning its size
	// TODO: support selector in Query
	stats, err := Stat(ctx, store, q.PayloadCID, sel)
	// We don't have the block we don't even reply to avoid taking bandwidth
	// On the client side we assume no response means they don't have it
	if err != nil || stats.Size == 0 {
		return deal.QueryResponse{}, fmt.Errorf("%s content unavailable: %w", e.h.ID(), err)
	}
	resp := deal.QueryResponse{
		Status:                     deal.QueryResponseAvailable,
		Size:                       uint64(stats.Size),
		PaymentAddress:             e.w.DefaultAddress(),
		MinPricePerByte:            r.PPB, // TODO: dynamic pricing
		MaxPaymentInterval:         deal.DefaultPaymentInterval,
		MaxPaymentIntervalIncrease: deal.DefaultPaymentIntervalIncrease,
	}
	// We need to remember the offer we made so we can validate against it once
	// clients start the retrieval
	e.rtv.Provider().SetAsk(q.PayloadCID, resp)
	return resp, nil
}

// Tx returns a new transaction
func (e *Exchange) Tx(ctx context.Context, opts ...TxOption) *Tx {
	// This cancel allows us to shutdown the retrieval process with the session if needed
	ctx, cancel := context.WithCancel(ctx)
	// Track when the session is completed
	done := make(chan TxResult, 1)
	// Track any issues with the transfer
	errs := make(chan deal.Status)
	// Subscribe to client events to send to the channel
	cl := e.rtv.Client()
	unsubscribe := cl.SubscribeToEvents(func(event client.Event, state deal.ClientState) {
		switch state.Status {
		case deal.StatusCompleted:
			select {
			case done <- TxResult{
				Size:  state.TotalReceived,
				Spent: state.FundsSpent,
			}:
			default:
			}
			return
		case deal.StatusCancelled, deal.StatusErrored:
			select {
			case errs <- state.Status:
			default:
			}
			return
		}
	})
	ms := e.opts.MultiStore
	storeID := ms.Next()
	store, err := ms.Get(storeID)
	tx := &Tx{
		ctx:        ctx,
		cancelCtx:  cancel,
		ms:         e.opts.MultiStore,
		rou:        e.rou,
		retriever:  cl,
		index:      e.idx,
		repl:       e.rpl,
		chunkSize:  256000,
		clientAddr: e.w.DefaultAddress(),
		sel:        selectors.All(),
		done:       done,
		errs:       errs,
		ongoing:    make(chan DealRef),
		// Triage should be manually activated with WithTriage option
		// triage:  make(chan DealSelection),
		entries: make(map[string]Entry),
		unsub:   unsubscribe,
		storeID: storeID,
		store:   store,
		Err:     err,
	}
	for _, opt := range opts {
		opt(tx)
	}
	return tx
}

// FindAndRetrieve starts a new transaction for fetching an entire dag on the market.
// It handles everything from content routing to offer selection and blocks until done.
// It is used in the replication protocol for retrieving new content to serve.
// It also sets the new received content in the index.
func (e *Exchange) FindAndRetrieve(ctx context.Context, root cid.Cid) error {
	tx := e.Tx(ctx, WithRoot(root), WithStrategy(SelectFirst))
	defer tx.Close()
	err := tx.Query(sel.All())
	if err != nil {
		return err
	}
	select {
	case res := <-tx.Done():
		if res.Err != nil {
			return res.Err
		}
		return e.idx.SetRef(&DataRef{
			PayloadCID:  root,
			StoreID:     tx.StoreID(),
			PayloadSize: int64(res.Size),
		})
	case <-ctx.Done():
		return ctx.Err()
	}
}

// UpgradeCluster takes a previous and next cluster ID to propgate cluster changes across routing
func (e *Exchange) UpgradeCluster(prev string, next string) {
	e.rou.LeaveTopic(prev)
	e.rou.JoinTopic(next)
}

// Wallet returns the wallet API
func (e *Exchange) Wallet() wallet.Driver {
	return e.w
}

// DataTransfer returns the data transfer manager instance for this exchange
func (e *Exchange) DataTransfer() datatransfer.Manager {
	return e.opts.DataTransfer
}

// FilecoinAPI returns the FilecoinAPI instance for this exchange
// may be nil so check with IsFilecoinOnline first
func (e *Exchange) FilecoinAPI() filecoin.API {
	return e.opts.FilecoinAPI
}

// IsFilecoinOnline returns whether we are connected to a Filecoin blockchain gateway
func (e *Exchange) IsFilecoinOnline() bool {
	return e.opts.FilecoinAPI != nil
}

// Retrieval exposes the retrieval manager module
func (e *Exchange) Retrieval() retrieval.Manager {
	return e.rtv
}

// R exposes replication scheme methods
func (e *Exchange) R() *Replication {
	return e.rpl
}

// Index returns the exchange data index
func (e *Exchange) Index() *Index {
	return e.idx
}

// ListMiners returns a list of miners based on the regions this exchange is part of
// We keep a context as this could also query a remote service or API
func (e *Exchange) ListMiners(ctx context.Context) ([]address.Address, error) {
	var strList []string
	for _, r := range e.opts.Regions {
		// Global region is already a list of miners in all regions
		if r.Name == "Global" {
			strList = r.StorageMiners
			break
		}
		strList = append(strList, r.StorageMiners...)
	}
	var addrList []address.Address
	for _, s := range strList {
		addr, err := address.NewFromString(s)
		if err != nil {
			return addrList, err
		}
		addrList = append(addrList, addr)
	}
	return addrList, nil
}

// GetStoreID exposes a method to get the store ID used by a given CID
func (e *Exchange) GetStoreID(id cid.Cid) (multistore.StoreID, error) {
	return e.idx.GetStoreID(id)
}
