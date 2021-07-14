package exchange

import (
	"context"
	"fmt"
	"math"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-multistore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/myelnet/pop/filecoin"
	"github.com/myelnet/pop/internal/utils"
	"github.com/myelnet/pop/payments"
	"github.com/myelnet/pop/retrieval"
	"github.com/myelnet/pop/retrieval/deal"
	"github.com/myelnet/pop/selectors"
	sel "github.com/myelnet/pop/selectors"
	"github.com/myelnet/pop/wallet"
	"github.com/rs/zerolog/log"
)

// Exchange is a financially incentivized IPLD  block exchange
// powered by Filecoin and IPFS
type Exchange struct {
	h    host.Host
	ds   datastore.Batching
	opts Options
	// pubsub topics
	tops []*pubsub.Topic
	// payment manager for more control over payment channels
	pay payments.Manager
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
		opts.Blockstore,
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
		pay:  payments.New(ctx, opts.FilecoinAPI, opts.Wallet, ds, opts.Blockstore),
	}

	exch.rpl, err = NewReplication(h, idx, opts.DataTransfer, exch, opts)
	if err != nil {
		return nil, err
	}

	if opts.Wallet.DefaultAddress() == address.Undef {
		_, err = opts.Wallet.NewKey(ctx, wallet.KTSecp256k1)
		if err != nil {
			return nil, err
		}
	}

	exch.rtv, err = retrieval.New(
		ctx,
		opts.MultiStore,
		ds,
		exch.pay,
		opts.DataTransfer,
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

func (e *Exchange) handleQuery(ctx context.Context, p peer.ID, r Region, q deal.Query) (deal.Offer, error) {
	// This is used to increment LFU cache if the node is available
	// the Stat method actually checks if the content is available.
	_, _ = e.idx.GetRef(q.PayloadCID)
	if q.Selector == nil {
		return deal.Offer{}, fmt.Errorf("no selector provided")
	}
	sel, err := retrieval.DecodeNode(q.QueryParams.Selector)
	if err != nil {
		sel = selectors.All()
	}

	bal, err := e.Wallet().Balance(ctx, e.Wallet().DefaultAddress())
	if err != nil {
		log.Error().Err(err).Msg("error when fetching balance")
	}
	fmt.Printf("Fil balance: %s attofil\n", humanize.Comma(bal.Int64()))

	// DAGStat is both a way of checking if we have the blocks and returning its size
	stats, err := utils.Stat(ctx, &multistore.Store{Bstore: e.opts.Blockstore}, q.PayloadCID, sel)
	// We don't have the block we don't even reply to avoid taking bandwidth
	// On the client side we assume no response means they don't have it
	if err != nil || stats.Size == 0 {
		return deal.Offer{}, fmt.Errorf("%s content unavailable: %w", e.h.ID(), err)
	}
	ask := deal.Offer{
		PayloadCID:                 q.PayloadCID,
		Size:                       uint64(stats.Size),
		PaymentAddress:             e.opts.Wallet.DefaultAddress(),
		MinPricePerByte:            r.PPB, // TODO: dynamic pricing
		MaxPaymentInterval:         deal.DefaultPaymentInterval,
		MaxPaymentIntervalIncrease: deal.DefaultPaymentIntervalIncrease,
	}
	// We need to remember the offer we made so we can validate against it once
	// clients start the retrieval
	e.rtv.Provider().SetAsk(q.PayloadCID, ask)
	return ask, nil
}

// Tx returns a new transaction. The caller must also call tx.Close to cleanup and perist the new blocks
// retrieved or created by the transaction.
func (e *Exchange) Tx(ctx context.Context, opts ...TxOption) *Tx {
	// This cancel allows us to shutdown the retrieval process with the session if needed
	ctx, cancel := context.WithCancel(ctx)
	ms := e.opts.MultiStore
	storeID := ms.Next()
	store, err := ms.Get(storeID)
	tx := &Tx{
		ctx:        ctx,
		cancelCtx:  cancel,
		bs:         e.opts.Blockstore,
		ms:         e.opts.MultiStore,
		rou:        e.rou,
		retriever:  e.rtv.Client(),
		index:      e.idx,
		repl:       e.rpl,
		cacheRF:    6,
		clientAddr: e.opts.Wallet.DefaultAddress(),
		sel:        selectors.All(),
		done:       make(chan TxResult, 1),
		errs:       make(chan deal.Status),
		ongoing:    make(chan DealRef),
		// Triage should be manually activated with WithTriage option
		// triage:  make(chan DealSelection),
		entries: make(map[string]Entry),
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

		keys, err := utils.MapLoadableKeys(ctx, root, tx.Store().Loader)
		if err != nil {
			return err
		}

		return e.idx.SetRef(&DataRef{
			PayloadCID:  root,
			PayloadSize: int64(res.Size),
			Keys:        keys.AsBytes(),
		})

	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wallet returns the wallet API
func (e *Exchange) Wallet() wallet.Driver {
	return e.opts.Wallet
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

// Payments returns the payment manager
func (e *Exchange) Payments() payments.Manager {
	return e.pay
}
