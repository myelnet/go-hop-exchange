package node

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/filecoin-project/go-multistore"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	badgerds "github.com/ipfs/go-ds-badger"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	chunk "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	keystore "github.com/ipfs/go-ipfs-keystore"
	cbor "github.com/ipfs/go-ipld-cbor"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-path"
	"github.com/ipfs/go-unixfs/importer/balanced"
	"github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/p2p/net/conngater"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/myelnet/pop/build"
	"github.com/myelnet/pop/exchange"
	"github.com/myelnet/pop/filecoin"
	"github.com/myelnet/pop/filecoin/storage"
	"github.com/myelnet/pop/internal/utils"
	"github.com/myelnet/pop/retrieval/client"
	"github.com/myelnet/pop/retrieval/deal"
	sel "github.com/myelnet/pop/selectors"
	"github.com/myelnet/pop/wallet"
	"github.com/rs/zerolog/log"
)

// KContentBatch is the keystore used for storing the root CID of the HAMT used to aggregate content for storage
const KContentBatch = "content-batch"

// ErrFilecoinRPCOffline is returned when the node is running without a provided filecoin api endpoint + token
var ErrFilecoinRPCOffline = errors.New("filecoin RPC is offline")

// ErrAllDealsFailed is returned when all storage deals failed to get started
var ErrAllDealsFailed = errors.New("all deals failed")

// ErrNoTx is returned when no transaction is staged and we attempt to commit
var ErrNoTx = errors.New("no tx to commit")

// ErrNodeNotFound is returned when we cannot find the node in the given root
var ErrNodeNotFound = errors.New("node not found")

// ErrQuoteNotFound is returned when we are trying to store but couldn't get a quote
var ErrQuoteNotFound = errors.New("quote not found")

// ErrInvalidPeer is returned when trying to ping a peer with invalid peer ID or address
var ErrInvalidPeer = errors.New("invalid peer ID or address")

// Options determines configurations for the IPFS node
type Options struct {
	// RepoPath is the file system path to use to persist our datastore
	RepoPath string
	// SocketPath is the unix socket path to listen on
	SocketPath string
	// BootstrapPeers is a peer address to connect to for discovering other peers
	BootstrapPeers []string
	// FilEndpoint is the websocket url for accessing a remote filecoin api
	FilEndpoint string
	// FilToken is the authorization token to access the filecoin api
	FilToken string
	// PrivKey is a hex encoded private key to use for default address
	PrivKey string
	// MaxPPB is the maximum price per byte
	MaxPPB int64
	// Regions is a list of regions a provider chooses to support.
	// Nothing prevents providers from participating in regions outside of their geographic location however they may get less deals since the latency is likely to be higher
	Regions []string
	// Capacity is the maxium storage capacity dedicated to the exchange
	Capacity uint64
	// ReplInterval defines how often the node attempts to find new content from connected peers
	ReplInterval time.Duration
	// GracefulShutdown is the CancelFunc used for gracefully shutting down the node
	GracefulShutdown context.CancelFunc
}

// RemoteStorer is the interface used to store content on decentralized storage networks (Filecoin)
type RemoteStorer interface {
	Store(context.Context, storage.Params) (*storage.Receipt, error)
	GetMarketQuote(context.Context, storage.QuoteParams) (*storage.Quote, error)
	PeerInfo(context.Context, address.Address) (*peer.AddrInfo, error)
}

type node struct {
	host host.Host
	ds   datastore.Batching
	bs   blockstore.Blockstore
	ms   *multistore.MultiStore
	is   cbor.IpldStore
	dag  ipldformat.DAGService
	exch *exchange.Exchange
	rs   RemoteStorer

	// opts keeps all the node params set when starting the node
	opts Options

	// root of any pending HAMT for storage. This HAMT indexes multiple transactions to
	// be stored in a single CAR for storage.
	pieceHAMT *hamt.Node

	mu     sync.Mutex
	notify func(Notify)

	// keep track of an ongoing transaction
	txmu sync.Mutex
	tx   *exchange.Tx

	// Save context cancelFunc for graceful node shutdown
	gracefulShutdown context.CancelFunc
}

// New puts together all the components of the ipfs node
func New(ctx context.Context, opts Options) (*node, error) {
	var err error
	nd := &node{
		opts: opts,
	}

	dsopts := badgerds.DefaultOptions
	dsopts.SyncWrites = false
	dsopts.Truncate = true

	nd.ds, err = badgerds.NewDatastore(filepath.Join(opts.RepoPath, "datastore"), &dsopts)
	if err != nil {
		return nil, err
	}

	nd.ms, err = multistore.NewMultiDstore(nd.ds)
	if err != nil {
		return nil, err
	}

	nd.bs = blockstore.NewBlockstore(nd.ds)

	if err := nd.loadPieceHAMT(); err != nil {
		return nil, err
	}

	nd.dag = merkledag.NewDAGService(blockservice.New(nd.bs, offline.Exchange(nd.bs)))

	ks, err := keystore.NewFSKeystore(filepath.Join(opts.RepoPath, "keystore"))
	if err != nil {
		return nil, err
	}
	priv, err := utils.Libp2pKey(ks)
	if err != nil {
		return nil, err
	}

	gater, err := conngater.NewBasicConnectionGater(nd.ds)
	if err != nil {
		return nil, err
	}

	nd.host, err = libp2p.New(
		ctx,
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			"/ip4/0.0.0.0/tcp/41505",
			"/ip6/::/tcp/41505",
		),
		libp2p.ConnectionManager(connmgr.NewConnManager(
			20,             // Lowwater
			60,             // HighWater,
			20*time.Second, // GracePeriod
		)),
		libp2p.ConnectionGater(gater),
		libp2p.DisableRelay(),
		// Attempt to open ports using uPNP for NATed hosts.
		libp2p.NATPortMap(),
		libp2p.EnableNATService(),
		// Let this host use the DHT to find other hosts
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			return dht.New(ctx, h)
		}),
		// user-agent is sent along the identify protocol
		libp2p.UserAgent("pop-"+build.Version),
	)
	if err != nil {
		return nil, err
	}

	// Convert region names to region structs
	regions := exchange.ParseRegions(opts.Regions)

	eopts := exchange.Options{
		Blockstore:          nd.bs,
		MultiStore:          nd.ms,
		RepoPath:            opts.RepoPath,
		FilecoinRPCEndpoint: opts.FilEndpoint,
		FilecoinRPCHeader: http.Header{
			"Authorization": []string{opts.FilToken},
		},
		Regions:      regions,
		Capacity:     opts.Capacity,
		ReplInterval: opts.ReplInterval,
	}

	if eopts.FilecoinRPCEndpoint != "" {
		eopts.FilecoinAPI, err = filecoin.NewLotusRPC(ctx, eopts.FilecoinRPCEndpoint, eopts.FilecoinRPCHeader)
		if err != nil {
			log.Error().Err(err).Msg("failed to connect with Lotus RPC")
		}
	}

	eopts.Wallet = wallet.NewFromKeystore(
		ks,
		wallet.WithFilAPI(eopts.FilecoinAPI),
		wallet.WithBLSSig(bls{}),
	)

	var addr address.Address
	if eopts.Wallet.DefaultAddress() == address.Undef && opts.PrivKey == "" {
		addr, err = eopts.Wallet.NewKey(ctx, wallet.KTSecp256k1)
		if err != nil {
			return nil, err
		}
		fmt.Printf("==> Generated new FIL address: %s\n", addr)
	}

	nd.exch, err = exchange.New(ctx, nd.host, nd.ds, eopts)
	if err != nil {
		return nil, err
	}

	if opts.PrivKey != "" {
		err = nd.importPrivateKey(ctx, opts.PrivKey)
		if err != nil {
			return nil, err
		}
		fmt.Printf("==> Imported private keys: %s\n", nd.exch.Wallet().DefaultAddress())

	} else if addr.Empty() {
		fmt.Printf("==> Loaded default FIL address: %s\n", nd.exch.Wallet().DefaultAddress())
	}

	// set Max Price Per Byte
	fmt.Printf("==> Set default Max Price Per Byte (MaxPPB) at %d attoFIL\n", nd.opts.MaxPPB)

	nd.gracefulShutdown = opts.GracefulShutdown

	nd.rs, err = storage.New(
		nd.host,
		nd.exch.DataTransfer(),
		nd.exch.Wallet(),
		nd.exch.FilecoinAPI(),
	)
	if err != nil {
		return nil, err
	}

	// start connecting with peers
	go utils.Bootstrap(ctx, nd.host, opts.BootstrapPeers)

	// remove unwanted blocks that might be in the blockstore but are removed from the index
	err = nd.exch.Index().CleanBlockStore(ctx)
	if err != nil {
		return nil, err
	}

	return nd, nil
}

// load HAMT from the datastore or create new one
func (nd *node) loadPieceHAMT() error {
	enc, err := nd.ds.Get(datastore.NewKey(KContentBatch))

	nd.is = cbor.NewCborStore(nd.bs)
	if err != nil && errors.Is(err, datastore.ErrNotFound) {
		hnd, err := hamt.NewNode(nd.is, hamt.UseTreeBitWidth(5), utils.HAMTHashOption)
		if err != nil {
			return err
		}
		nd.pieceHAMT = hnd
		return nil
	}
	if err != nil {
		return err
	}

	r, err := cid.Cast(enc)
	if err != nil {
		return err
	}
	hnd, err := hamt.LoadNode(context.TODO(), nd.is, r, hamt.UseTreeBitWidth(5), utils.HAMTHashOption)
	if err != nil {
		return err
	}
	nd.pieceHAMT = hnd
	return nil
}

// send hits out notify callback if we attached one
func (nd *node) send(n Notify) {
	nd.mu.Lock()
	notify := nd.notify
	nd.mu.Unlock()

	if notify != nil {
		notify(n)
	} else {
		log.Info().Interface("notif", n).Msg("nil notify callback; dropping")
	}
}

// Off
func (nd *node) Off(ctx context.Context) {
	nd.send(Notify{OffResult: &OffResult{}})
	fmt.Println("Gracefully shutdown node")

	nd.gracefulShutdown()
}

// Ping the node for sanity check more than anything
func (nd *node) Ping(ctx context.Context, who string) {
	sendErr := func(err error) {
		nd.send(Notify{PingResult: &PingResult{
			Err: err.Error(),
		}})
	}
	// Ping local node if no address is passed
	if who == "" {
		peers := nd.connPeers()
		var pstr []string
		for _, p := range peers {
			pstr = append(pstr, p.String())
		}
		var addrs []string
		for _, a := range nd.host.Addrs() {
			addrs = append(addrs, a.String())
		}
		nd.send(Notify{PingResult: &PingResult{
			ID:      nd.host.ID().String(),
			Addrs:   addrs,
			Peers:   pstr,
			Version: build.Version,
		}})
		return
	}

	addr, err := address.NewFromString(who)
	if err == nil {
		info, err := nd.rs.PeerInfo(ctx, addr)
		if err != nil {
			sendErr(err)
			return
		}
		err = nd.ping(ctx, *info)
		if err != nil {
			sendErr(err)
		}
		return
	}
	pid, err := peer.Decode(who)
	if err == nil {
		err = nd.ping(ctx, nd.host.Peerstore().PeerInfo(pid))
		if err != nil {
			sendErr(err)
		}
		return
	}
	sendErr(ErrInvalidPeer)
}

func (nd *node) ping(ctx context.Context, pi peer.AddrInfo) error {
	strs := make([]string, 0, len(pi.Addrs))
	for _, a := range pi.Addrs {
		strs = append(strs, a.String())
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pings := ping.Ping(ctx, nd.host, pi.ID)

	select {
	case res := <-pings:
		if res.Error != nil {
			return res.Error
		}
		var v string
		agent, _ := nd.host.Peerstore().Get(pi.ID, "AgentVersion")
		vparts := strings.Split(agent.(string), "-")
		if len(vparts) == 3 {
			v = fmt.Sprintf("%s-%s", vparts[1], vparts[2])
		}
		nd.send(Notify{PingResult: &PingResult{
			ID:             pi.ID.String(),
			Addrs:          strs,
			LatencySeconds: res.RTT.Seconds(),
			Version:        v,
		}})
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Put a file into a new or pending transaction
func (nd *node) Put(ctx context.Context, args *PutArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			PutResult: &PutResult{
				Err: err.Error(),
			},
		})
	}

	nd.txmu.Lock()
	defer nd.txmu.Unlock()
	if nd.tx == nil {
		nd.tx = nd.exch.Tx(ctx)
	}

	fstat, err := os.Stat(args.Path)
	if err != nil {
		sendErr(err)
		return
	}

	fnd, err := files.NewSerialFile(args.Path, false, fstat)
	if err != nil {
		sendErr(err)
		return
	}

	added := make(map[string]bool)
	err = nd.addRecursive(ctx, args.Path, fnd, added)
	if err != nil {
		sendErr(err)
		return
	}

	entries, err := nd.tx.Status()
	if err != nil {
		sendErr(err)
		return
	}

	var totalSize int64
	// only notify about entries added by this operation
	for k := range added {
		e := entries[k]
		totalSize += e.Size
		nd.send(Notify{
			PutResult: &PutResult{
				Key:  k,
				Cid:  e.Value.String(),
				Size: filecoin.SizeStr(filecoin.NewInt(uint64(e.Size))),
				// NumBlocks: stats.NumBlocks, TODO: should Entry contain the number of blocks?
				RootCid:   nd.tx.Root().String(),
				TotalSize: filecoin.SizeStr(filecoin.NewInt(uint64(totalSize))),
				Len:       len(added),
			}})
	}
}

// Status prints the current transaction status. It shows which files have been added but not yet committed
// to the network
func (nd *node) Status(ctx context.Context, args *StatusArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			StatusResult: &StatusResult{
				Err: err.Error(),
			},
		})
	}
	nd.txmu.Lock()
	defer nd.txmu.Unlock()
	if nd.tx != nil {
		s, err := nd.tx.Status()
		if err != nil {
			sendErr(err)
			return
		}

		nd.send(Notify{
			StatusResult: &StatusResult{
				RootCid: nd.tx.Root().String(),
				Entries: s.String(),
			},
		})
		return
	}
	sendErr(ErrNoTx)
}

// WalletList
func (nd *node) WalletList(ctx context.Context, args *WalletListArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			WalletResult: &WalletResult{
				Err: err.Error(),
			},
		})
	}

	addresses, err := nd.exch.Wallet().List()
	if err != nil {
		sendErr(fmt.Errorf("failed to list addresses: %v", err))
		return
	}

	var stringAddresses = make([]string, len(addresses))

	for i, addr := range addresses {
		stringAddresses[i] = addr.String()
	}

	nd.send(Notify{
		WalletResult: &WalletResult{Addresses: stringAddresses},
	})
}

// WalletExport
func (nd *node) WalletExport(ctx context.Context, args *WalletExportArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			WalletResult: &WalletResult{
				Err: err.Error(),
			},
		})
	}

	err := nd.exportPrivateKey(ctx, args.Address, args.OutputPath)
	if err != nil {
		sendErr(fmt.Errorf("cannot export private key: %v", err))
		return
	}

	nd.send(Notify{
		WalletResult: &WalletResult{},
	})
}

// WalletPay
func (nd *node) WalletPay(ctx context.Context, args *WalletPayArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			WalletResult: &WalletResult{
				Err: err.Error(),
			},
		})
	}

	from, err := address.NewFromString(args.From)
	if err != nil {
		sendErr(fmt.Errorf("failed to decode address %s : %v", args.From, err))
		return
	}

	to, err := address.NewFromString(args.To)
	if err != nil {
		sendErr(fmt.Errorf("failed to decode address %s : %v", args.To, err))
		return
	}

	err = nd.exch.Wallet().Transfer(ctx, from, to, args.Amount)
	if err != nil {
		sendErr(err)
		return
	}

	nd.send(Notify{
		WalletResult: &WalletResult{},
	})
}

// Quote returns an estimation of market price for storing a list of transactions on Filecoin
func (nd *node) Quote(ctx context.Context, args *QuoteArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			QuoteResult: &QuoteResult{
				Err: err.Error(),
			},
		})
	}
	if !nd.exch.IsFilecoinOnline() {
		sendErr(ErrFilecoinRPCOffline)
		return
	}

	cbs := cbor.NewCborStore(nd.bs)
	hnd, err := hamt.NewNode(cbs, hamt.UseTreeBitWidth(5), utils.HAMTHashOption)
	if err != nil {
		sendErr(err)
		return
	}
	for _, ref := range args.Refs {
		ccid, err := cid.Parse(ref)
		if err != nil {
			sendErr(err)
			return
		}

		cref := CommitRef{ccid}
		if err := hnd.Set(ctx, ref, &cref); err != nil {
			sendErr(err)
			return
		}
		if err := hnd.Flush(ctx); err != nil {
			sendErr(err)
			return
		}
	}

	proot, err := cbs.Put(ctx, hnd)
	if err != nil {
		sendErr(err)
		return
	}

	piece, err := nd.archive(ctx, proot)
	if err != nil {
		sendErr(err)
		return
	}

	quote, err := nd.rs.GetMarketQuote(ctx, storage.QuoteParams{
		PieceSize: uint64(piece.PieceSize),
		Duration:  args.Duration,
		RF:        args.StorageRF,
		MaxPrice:  args.MaxPrice,
	})
	if err != nil {
		sendErr(err)
		return
	}
	quotes := make(map[string]string)
	for _, m := range quote.Miners {
		addr := m.Info.Address
		quotes[addr.String()] = quote.Prices[addr].String()
	}

	nd.send(Notify{
		QuoteResult: &QuoteResult{
			Ref:         piece.CID.String(),
			Quotes:      quotes,
			PayloadSize: uint64(piece.PayloadSize),
			PieceSize:   uint64(piece.PieceSize),
		},
	})
}

// Commit a content transaction for storage
func (nd *node) Commit(ctx context.Context, args *CommArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			CommResult: &CommResult{
				Err: err.Error(),
			},
		})
	}
	nd.txmu.Lock()
	if nd.tx == nil {
		nd.txmu.Unlock()
		sendErr(ErrNoTx)
		return
	}
	nd.tx.SetCacheRF(args.CacheRF)
	err := nd.tx.Commit()
	if err != nil {
		sendErr(err)
		return
	}
	ref := nd.tx.Ref()
	nd.tx.WatchDispatch(func(r exchange.PRecord) {
		nd.send(Notify{
			CommResult: &CommResult{
				Caches: []string{
					r.Provider.String(),
				},
			},
		})
	})
	if err := nd.exch.Index().SetRef(ref); err != nil {
		sendErr(err)
		return
	}

	nd.tx.Close()
	nd.tx = nil
	nd.txmu.Unlock()

	// Run the garbage collector to remove tagged Refs
	err = nd.exch.Index().GC()
	if err != nil {
		sendErr(err)
		return
	}

	nd.send(Notify{CommResult: &CommResult{
		Size: filecoin.SizeStr(filecoin.NewInt(uint64(ref.PayloadSize))),
		Ref:  ref.PayloadCID.String(),
	}})
}

// Store aggregates multiple transactions together into a storage deal if the piece is large enough
// the method may need to be called until there is enough content to reach the minimum storage size
func (nd *node) Store(ctx context.Context, args *StoreArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			StoreResult: &StoreResult{
				Err: err.Error(),
			},
		})
	}

	if !nd.exch.IsFilecoinOnline() {
		sendErr(ErrFilecoinRPCOffline)
		return
	}

	for _, c := range args.Refs {
		ref, err := nd.getRef(c)
		if err != nil {
			sendErr(err)
			return
		}

		cref := CommitRef{ref.PayloadCID}
		if err := nd.pieceHAMT.Set(ctx, ref.PayloadCID.String(), &cref); err != nil {
			sendErr(err)
			return
		}
		if err := nd.pieceHAMT.Flush(ctx); err != nil {
			sendErr(err)
			return
		}
	}
	proot, err := nd.is.Put(ctx, nd.pieceHAMT)
	if err != nil {
		sendErr(err)
		return
	}

	piece, err := nd.archive(ctx, proot)
	if err != nil {
		sendErr(err)
		return
	}
	log.Info().Msg("getting quote for a piece")

	quote, err := nd.rs.GetMarketQuote(ctx, storage.QuoteParams{
		PieceSize: uint64(piece.PieceSize),
		Duration:  args.Duration,
		RF:        args.StorageRF,
		MaxPrice:  args.MaxPrice,
		Region:    "Europe",
		Verified:  args.Verified,
	})
	if err != nil {
		sendErr(err)
		return
	}
	log.Info().Uint64("MinPieceSize", quote.MinPieceSize).Msg("got a quote")

	if quote.MinPieceSize > uint64(piece.PieceSize) {
		nd.send(Notify{
			StoreResult: &StoreResult{
				Capacity: quote.MinPieceSize - uint64(piece.PieceSize),
			},
		})
		return
	}

	rcpt, err := nd.rs.Store(ctx, storage.NewParams(
		proot,
		args.Duration,
		nd.exch.Wallet().DefaultAddress(),
		quote.Miners,
		args.Verified,
	))
	if err != nil {
		sendErr(err)
		return
	}
	if len(rcpt.DealRefs) == 0 {
		sendErr(ErrAllDealsFailed)
		return
	}
}

// Get sends a request for content with the given arguments. It also sends feedback to any open cli
// connections
func (nd *node) Get(ctx context.Context, args *GetArgs) {
	sendErr := func(err error) {
		nd.send(Notify{
			GetResult: &GetResult{
				Err: err.Error(),
			}})
	}
	p := path.FromString(args.Cid)
	// /<cid>/path/file.ext => cid, ["path", file.ext"]
	root, segs, err := path.SplitAbsPath(p)
	if err != nil {
		sendErr(err)
		return
	}

	if args.MaxPPB == -1 {
		// if maxppb is set at -1, force MaxPPB at 0
		args.MaxPPB = 0
	} else if args.MaxPPB == 0 {
		// if maxppb is set at 0, use default node's value
		args.MaxPPB = nd.opts.MaxPPB
	}

	// Check if we're trying to get from an ongoing transaction
	nd.txmu.Lock()
	if nd.tx != nil && nd.tx.Root() == root {
		f, err := nd.tx.GetFile(segs[0])
		if err != nil {
			sendErr(err)
			return
		}
		if args.Out != "" {
			err = files.WriteTo(f, args.Out)
			if err != nil {
				sendErr(err)
				return
			}
		}
		nd.send(Notify{
			GetResult: &GetResult{
				Local: true,
			},
		})
		return
	}
	nd.txmu.Unlock()

	// Only support a single segment for now
	args.Key = segs[0]
	// Log progress
	if args.Verbose {
		unsub := nd.exch.Retrieval().Client().SubscribeToEvents(
			func(event client.Event, state deal.ClientState) {
				log.Info().
					Str("event", client.Events[event]).
					Str("status", deal.Statuses[state.Status]).
					Uint64("bytes received", state.TotalReceived).
					Msg("Retrieving")
			},
		)
		defer unsub()
	}
	results := make(chan GetResult)
	go func() {
		for res := range results {
			nd.send(Notify{
				GetResult: &res,
			})
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(args.Timeout)*time.Minute)
	defer cancel()
	err = nd.get(ctx, root, args, results)
	if err != nil {
		sendErr(err)
	}
	close(results)
}

// get is a synchronous content retrieval operation which can be called by a CLI request or HTTP
func (nd *node) get(ctx context.Context, c cid.Cid, args *GetArgs, results chan<- GetResult) error {
	// Check our supply if we may already have it
	tx := nd.exch.Tx(ctx, exchange.WithRoot(c))
	local := tx.IsLocal(args.Key)
	if local && args.Out != "" {
		f, err := tx.GetFile(args.Key)
		if err != nil {
			return err
		}
		err = files.WriteTo(f, args.Out)
		if err != nil {
			return err
		}
	}
	if local {
		log.Info().Msg("content is available locally")
		results <- GetResult{
			Local: true,
		}
		return nil
	}

	var strategy exchange.SelectionStrategy
	switch args.Strategy {
	case "SelectFirst":
		strategy = exchange.SelectFirst
	case "SelectCheapest":
		strategy = exchange.SelectCheapest(5, 4*time.Second)
	case "SelectFirstLowerThan":
		strategy = exchange.SelectFirstLowerThan(abi.NewTokenAmount(args.MaxPPB))
	default:
		return errors.New("unknown strategy")
	}

	start := time.Now()

	tx = nd.exch.Tx(ctx, exchange.WithRoot(c), exchange.WithStrategy(strategy), exchange.WithTriage())
	defer tx.Close()
	var sl ipld.Node
	if args.Key != "" {
		sl = sel.Key(args.Key)
	} else {
		sl = sel.All()
	}

	log.Info().Msg("starting query")

	if err := tx.Query(sl); err != nil {
		return err
	}
	// We can query a specific miner on top of gossip
	if args.Miner != "" {
		miner, err := address.NewFromString(args.Miner)
		if err != nil {
			return err
		}
		info, err := nd.rs.PeerInfo(ctx, miner)
		if err != nil {
			// Maybe fall back to a discovery session?
			return err
		}
		err = tx.QueryFrom(*info, args.Key)
		if err != nil {
			// Maybe we shouldn't fail here, the transfer could still work with other peers
			return err
		}
	}

	log.Info().Msg("waiting for triage")

	// Triage waits until we select the first offer it might not mean the first
	// offer that we receive depending on the strategy used
	selection, err := tx.Triage()
	if err != nil {
		return err
	}
	now := time.Now()
	discDuration := now.Sub(start)
	resp := selection.Offer.Response

	log.Info().Msg("selected an offer")

	results <- GetResult{
		Size:         int64(resp.Size),
		Status:       "StatusSelectedOffer",
		UnsealPrice:  filecoin.FIL(resp.UnsealPrice).Short(),
		TotalPrice:   filecoin.FIL(resp.PieceRetrievalPrice()).Short(),
		PricePerByte: filecoin.FIL(resp.MinPricePerByte).Short(),
	}

	// TODO: accept all by default but we should be able to pass flag to provide
	// confirmation before retrieving
	selection.Incline()

	var dref exchange.DealRef
	select {
	case dref = <-tx.Ongoing():
	case <-ctx.Done():
		return ctx.Err()
	}

	log.Info().Msg("started transfer")

	results <- GetResult{
		DealID: dref.ID.String(),
	}

	select {
	case res := <-tx.Done():
		log.Info().Msg("finished transfer")
		if res.Err != nil {
			return res.Err
		}
		end := time.Now()
		transDuration := end.Sub(start) - discDuration
		if args.Out != "" {
			f, err := tx.GetFile(args.Key)
			if err != nil {
				return err
			}
			err = files.WriteTo(f, args.Out)
			if err != nil {
				return err
			}
		}

		var keys [][]byte
		if args.Key != "" {
			keys = append(keys, []byte(args.Key))
		} else {
			mk, err := utils.MapKeys(ctx, c, tx.Store().Loader)
			if err != nil {
				return err
			}
			keys = mk.AsBytes()
		}

		ref := &exchange.DataRef{
			PayloadCID:  c,
			PayloadSize: int64(res.Size),
			Keys:        keys,
		}

		err = nd.exch.Index().SetRef(ref)
		if err == exchange.ErrRefAlreadyExists {
			if err := nd.exch.Index().UpdateRef(ref); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		results <- GetResult{
			DiscLatSeconds:  discDuration.Seconds(),
			TransLatSeconds: transDuration.Seconds(),
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Load is an RPC method that retrieves a given CID and key to the local blockstore.
// It sends feedback events to a result channel that it returns.
func (nd *node) Load(ctx context.Context, args *GetArgs) (chan GetResult, error) {
	results := make(chan GetResult)

	go func() {

		p := path.FromString(args.Cid)
		root, segs, err := path.SplitAbsPath(p)
		if err != nil {
			return
		}

		if len(segs) > 0 {
			args.Key = segs[0]
		}

		if args.Strategy == "" {
			args.Strategy = "SelectFirst"
		}

		if args.Strategy == "" && args.MaxPPB > 0 {
			args.Strategy = "SelectFirstLowerThan"
		}

		unsub := nd.exch.Retrieval().Client().SubscribeToEvents(
			func(event client.Event, state deal.ClientState) {
				if state.PayloadCID == root {
					results <- GetResult{
						TotalPrice:    filecoin.FIL(state.TotalFunds).Short(),
						Status:        deal.Statuses[state.Status],
						TotalReceived: int64(state.TotalReceived),
					}
				}
			},
		)
		defer unsub()

		err = nd.get(ctx, root, args, results)
		if err != nil {
			results <- GetResult{
				Err: err.Error(),
			}
		}
		close(results)
	}()

	return results, nil
}

// List returns all the roots for the content stored by this node
func (nd *node) List(ctx context.Context, args *ListArgs) {
	list, err := nd.exch.Index().ListRefs()
	if err != nil {
		nd.send(Notify{
			ListResult: &ListResult{
				Err: err.Error(),
			},
		})
		return
	}
	if len(list) == 0 {
		nd.send(Notify{
			ListResult: &ListResult{
				Err: "no refs stored",
			},
		})
		return
	}
	for i, ref := range list {
		nd.send(Notify{
			ListResult: &ListResult{
				Root: ref.PayloadCID.String(),
				Size: ref.PayloadSize,
				Freq: ref.Freq,
				Last: i == len(list)-1,
			},
		})
	}
}

// Add a buffer into the node global DAG. These DAGs can eventually be put into transactions.
func (nd *node) Add(ctx context.Context, dag ipldformat.DAGService, buf io.Reader) (cid.Cid, error) {
	bufferedDS := ipldformat.NewBufferedDAG(ctx, dag)

	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return cid.Undef, err
	}
	prefix.MhType = exchange.DefaultHashFunction

	params := helpers.DagBuilderParams{
		Maxlinks:   1024,
		RawLeaves:  true,
		CidBuilder: prefix,
		Dagserv:    bufferedDS,
	}

	db, err := params.New(chunk.NewSizeSplitter(buf, int64(128000)))
	if err != nil {
		return cid.Undef, err
	}

	n, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}

	err = bufferedDS.Commit()
	if err != nil {
		return cid.Undef, err
	}

	return n.Cid(), nil
}

// getRef is an internal function to find a ref with a given string cid
// it is used when quoting the commit storage price or pushing to storage providers
func (nd *node) getRef(cstr string) (*exchange.DataRef, error) {
	// Select the commit with the matching CID
	// TODO: should prob error out if we don't find it
	if cstr != "" {
		ccid, err := cid.Parse(cstr)
		if err != nil {
			return nil, err
		}
		ref, err := nd.exch.Index().PeekRef(ccid)
		if err != nil {
			return nil, err
		}
		return ref, nil
	}

	nd.txmu.Lock()
	defer nd.txmu.Unlock()
	if nd.tx != nil {
		return nd.tx.Ref(), nil
	}

	return nil, ErrNoTx
}

// addRecursive adds entire file trees into a single transaction
// it assumes the caller is holding the tx lock until it returns
// it currently flattens the keys though we may want to maintain the full keys to keep the structure
func (nd *node) addRecursive(ctx context.Context, name string, file files.Node, added map[string]bool) error {
	switch f := file.(type) {
	case files.Directory:
		it := f.Entries()
		for it.Next() {
			err := nd.addRecursive(ctx, it.Name(), it.Node(), added)
			if err != nil {
				return err
			}
		}
		return it.Err()
	case files.File:
		froot, err := nd.Add(ctx, nd.tx.Store().DAG, f)
		if err != nil {
			return err
		}

		size, err := file.Size()
		if err != nil {
			return err
		}

		key := exchange.KeyFromPath(name)
		err = nd.tx.Put(key, froot, size)
		if err != nil {
			return err
		}
		added[key] = true
		return nil
	default:
		return errors.New("unknown file type")
	}
}

// connPeers returns a list of connected peer IDs
func (nd *node) connPeers() []peer.ID {
	conns := nd.host.Network().Conns()
	var out []peer.ID
	for _, c := range conns {
		pid := c.RemotePeer()
		out = append(out, pid)
	}
	return out
}

// importPrivateKey from a hex encoded private key to use as default on the exchange instead of
// the auto generated one. This is mostly for development and will be reworked into a nicer command
// eventually
func (nd *node) importPrivateKey(ctx context.Context, pk string) error {
	var iki wallet.KeyInfo

	data, err := hex.DecodeString(pk)
	if err != nil {
		return fmt.Errorf("failed to decode key: %v", err)
	}

	err = iki.FromBytes(data)
	if err != nil {
		return fmt.Errorf("failed to decode keyInfo: %v", err)
	}

	addr, err := nd.exch.Wallet().ImportKey(ctx, &iki)
	if err != nil {
		return fmt.Errorf("failed to import key: %v", err)
	}

	err = nd.exch.Wallet().SetDefaultAddress(addr)
	if err != nil {
		return fmt.Errorf("failed to set default address: %v", err)
	}

	return nil
}

// exportPrivateKey exports the private key of a given address to an output file
func (nd *node) exportPrivateKey(ctx context.Context, addr, outputPath string) error {
	adr, err := address.NewFromString(addr)
	if err != nil {
		return fmt.Errorf("failed to decode address: %v", err)
	}

	key, err := nd.exch.Wallet().ExportKey(ctx, adr)
	if err != nil {
		return fmt.Errorf("address %s does not exist", addr)
	}

	data, err := key.ToBytes()
	if err != nil {
		return fmt.Errorf("failed to convert address to bytes: %v", err)
	}

	encodedPk := make([]byte, hex.EncodedLen(len(data)))
	hex.Encode(encodedPk, data)

	err = os.WriteFile(outputPath, encodedPk, 0666)
	if err != nil {
		return fmt.Errorf("failed to export KeyInfo to %s: %v", addr, err)
	}

	return nil
}
