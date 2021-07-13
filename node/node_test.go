package node

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin"
	"github.com/filecoin-project/specs-actors/v4/actors/builtin/paych"
	"github.com/filecoin-project/specs-actors/v4/support/mock"
	tutils "github.com/filecoin-project/specs-actors/v4/support/testing"
	"github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	keystore "github.com/ipfs/go-ipfs-keystore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/myelnet/pop/exchange"
	"github.com/myelnet/pop/filecoin"
	"github.com/myelnet/pop/filecoin/storage"
	"github.com/myelnet/pop/internal/testutil"
	"github.com/myelnet/pop/wallet"
	"github.com/stretchr/testify/require"
)

type mockStorer struct {
	receipt *storage.Receipt
	quote   *storage.Quote
	pInfo   *peer.AddrInfo
}

func (ms *mockStorer) Start(context.Context) error {
	return nil
}

func (ms *mockStorer) Store(ctx context.Context, params storage.Params) (*storage.Receipt, error) {
	return ms.receipt, nil
}

func (ms *mockStorer) GetMarketQuote(ctx context.Context, params storage.QuoteParams) (*storage.Quote, error) {
	return ms.quote, nil
}

func (ms *mockStorer) PeerInfo(ctx context.Context, addr address.Address) (*peer.AddrInfo, error) {
	return ms.pInfo, nil
}

func newTestNode(ctx context.Context, mn mocknet.Mocknet, t *testing.T) *node {
	var err error

	tn := testutil.NewTestNode(mn, t)

	nd := &node{}
	nd.ds = tn.Ds
	nd.bs = tn.Bs
	nd.ms = tn.Ms
	nd.dag = tn.DAG
	nd.host = tn.Host
	nd.omg = NewOfferMgr()
	opts := exchange.Options{
		Blockstore:  nd.bs,
		MultiStore:  nd.ms,
		RepoPath:    t.TempDir(),
		FilecoinAPI: filecoin.NewMockLotusAPI(),
	}
	opts.Wallet = wallet.NewFromKeystore(keystore.NewMemKeystore(), wallet.WithFilAPI(opts.FilecoinAPI), wallet.WithBLSSig(bls{}))
	nd.exch, err = exchange.New(ctx, nd.host, nd.ds, opts)
	require.NoError(t, err)

	return nd
}

func TestPing(t *testing.T) {
	ctx := context.Background()
	mn := mocknet.New(ctx)

	nd := newTestNode(ctx, mn, t)

	nd.notify = func(n Notify) {
		require.Equal(t, n.PingResult.ID, nd.host.ID().String())
	}
	nd.Ping(ctx, "")
}

func TestPut(t *testing.T) {
	ctx := context.Background()
	mn := mocknet.New(ctx)

	cn := newTestNode(ctx, mn, t)

	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	data := make([]byte, 256000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)

	file, err := ioutil.TempFile("/tmp", "data")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	_, err = file.Write(data)
	require.NoError(t, err)

	added := make(chan string, 1)
	cn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")

		added <- n.PutResult.Cid
	}
	cn.Put(ctx, &PutArgs{
		Path:      file.Name(),
		ChunkSize: 1024,
	})
	<-added

	// We can also add a directory
	dir := t.TempDir()
	data1 := make([]byte, 1024)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data1)
	require.NoError(t, os.WriteFile(path.Join(dir, "data1"), data1, 0666))
	data2 := make([]byte, 3072)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data2)
	require.NoError(t, os.WriteFile(path.Join(dir, "data2"), data2, 0666))

	dirAdded := make(chan string, 2)
	cn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")

		dirAdded <- n.PutResult.Key
	}
	cn.Put(ctx, &PutArgs{
		Path:      dir,
		ChunkSize: 1024,
	})
	close(dirAdded)
	for k := range dirAdded {
		if k != "data1" && k != "data2" {
			t.Fatal("added wrong key")
		}
	}
}

// Put shouldn't race as it's protected with a mutex
func TestPutRace(t *testing.T) {
	ctx := context.Background()
	mn := mocknet.New(ctx)

	cn := newTestNode(ctx, mn, t)

	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	var wg sync.WaitGroup
	cn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")

		wg.Done()
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			data := make([]byte, 56000)
			rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)

			file, err := ioutil.TempFile("/tmp", "data")
			require.NoError(t, err)
			defer os.Remove(file.Name())

			_, err = file.Write(data)
			require.NoError(t, err)

			cn.Put(ctx, &PutArgs{
				Path:      file.Name(),
				ChunkSize: 1024,
			})
		}()
	}
	wg.Wait()
}

func TestPutGet(t *testing.T) {
	ctx := context.Background()
	mn := mocknet.New(ctx)

	cn := newTestNode(ctx, mn, t)
	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	dir := t.TempDir()

	data := make([]byte, 256000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)
	p1 := filepath.Join(dir, "data1")
	err := os.WriteFile(p1, data, 0666)
	require.NoError(t, err)

	data2 := make([]byte, 512000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data2)
	p2 := filepath.Join(dir, "data2")
	err = os.WriteFile(p2, data2, 0666)
	require.NoError(t, err)

	added := make(chan string, 2)
	cn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")

		added <- n.PutResult.Cid
	}
	cn.Put(ctx, &PutArgs{
		Path:      p1,
		ChunkSize: 1024,
	})
	<-added

	cn.Put(ctx, &PutArgs{
		Path:      p2,
		ChunkSize: 1024,
	})
	<-added

	stat := make(chan string, 1)
	cn.notify = func(n Notify) {
		require.Equal(t, n.StatusResult.Err, "")

		stat <- n.StatusResult.RootCid
	}
	cn.Status(ctx, &StatusArgs{})
	out := <-stat

	// Export back out
	path := fmt.Sprintf("/%s/data2", out)
	loc := make(chan bool, 1)
	cn.notify = func(n Notify) {
		require.Equal(t, n.GetResult.Err, "")

		loc <- n.GetResult.Local
	}
	newp := filepath.Join(dir, "newdata2")
	cn.Get(ctx, &GetArgs{
		Cid:      path,
		Out:      newp,
		Strategy: "SelectFirst",
		Timeout:  1,
	})
	<-loc

	f, err := os.Open(newp)
	require.NoError(t, err)

	newb, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, data2, newb)
}

var getAddr = address.NewForTestGetter()

func TestQuote(t *testing.T) {
	ctx := context.Background()
	mn := mocknet.New(ctx)

	cn := newTestNode(ctx, mn, t)
	require.NoError(t, cn.loadPieceHAMT())

	m1 := getAddr()
	p1, _ := filecoin.ParseFIL("0.001")
	m2 := getAddr()
	p2, _ := filecoin.ParseFIL("0.002")
	rs := &mockStorer{
		receipt: &storage.Receipt{},
		quote: &storage.Quote{
			Miners: []storage.Miner{
				{
					Info: &storagemarket.StorageProviderInfo{
						Address: m1,
					},
				},
				{
					Info: &storagemarket.StorageProviderInfo{
						Address: m2,
					},
				},
			},
			Prices: map[address.Address]filecoin.FIL{
				m1: p1,
				m2: p2,
			},
		},
		pInfo: host.InfoFromHost(cn.host),
	}
	cn.rs = rs

	dir := t.TempDir()

	data := make([]byte, 256000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)
	p := filepath.Join(dir, "data1")
	err := os.WriteFile(p, data, 0666)
	require.NoError(t, err)

	added := make(chan string, 2)
	cn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")

		added <- n.PutResult.Cid
	}
	cn.Put(ctx, &PutArgs{
		Path:      p,
		ChunkSize: 1024,
	})
	<-added

	data = make([]byte, 256000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)
	p = filepath.Join(dir, "data2")
	err = os.WriteFile(p, data, 0666)
	require.NoError(t, err)

	cn.Put(ctx, &PutArgs{
		Path:      p,
		ChunkSize: 1024,
	})
	<-added

	committed := make(chan CommResult, 1)
	cn.notify = func(n Notify) {
		require.Equal(t, n.CommResult.Err, "")
		committed <- *n.CommResult
	}
	cn.Commit(ctx, &CommArgs{
		CacheRF: 0,
	})
	com := <-committed

	quoted := make(chan QuoteResult, 1)
	cn.notify = func(n Notify) {
		require.Equal(t, "", n.QuoteResult.Err)
		require.Equal(t, 2, len(n.QuoteResult.Quotes))
		quoted <- *n.QuoteResult
	}
	cn.Quote(ctx, &QuoteArgs{
		Refs:      []string{com.Ref},
		Duration:  24 * time.Hour * time.Duration(180),
		StorageRF: 6,
		MaxPrice:  uint64(20000000000),
	})
	quote := <-quoted
	// Piece size should be deterministic
	require.Equal(t, uint64(524288), quote.PieceSize)
}

func TestCommit(t *testing.T) {
	var err error
	ctx := context.Background()
	mn := mocknet.New(ctx)
	tn := testutil.NewTestNode(mn, t)

	cn := &node{}
	cn.ds = tn.Ds
	cn.bs = tn.Bs
	cn.ms = tn.Ms
	cn.dag = tn.DAG
	cn.host = tn.Host
	opts := exchange.Options{
		Blockstore:  cn.bs,
		MultiStore:  cn.ms,
		RepoPath:    t.TempDir(),
		FilecoinAPI: filecoin.NewMockLotusAPI(),
		Capacity:    255000,
	}
	opts.Wallet = wallet.NewFromKeystore(keystore.NewMemKeystore(), wallet.WithFilAPI(opts.FilecoinAPI), wallet.WithBLSSig(bls{}))

	cn.exch, err = exchange.New(ctx, cn.host, cn.ds, opts)
	require.NoError(t, err)

	var nds []*node
	nds = append(nds, newTestNode(ctx, mn, t))
	nds = append(nds, newTestNode(ctx, mn, t))

	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	dir := t.TempDir()

	// we create a file larger than the node.Capacity to force eviction when adding later the second file
	data := make([]byte, 256000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)
	p := filepath.Join(dir, "data1")
	err = os.WriteFile(p, data, 0666)
	require.NoError(t, err)

	added := make(chan string, 1)
	cn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")
		added <- n.PutResult.Cid
	}
	cn.Put(ctx, &PutArgs{
		Path:      p,
		ChunkSize: 1024,
	})
	cid1 := <-added

	committed := make(chan []string, 3)
	cn.notify = func(n Notify) {
		require.Equal(t, n.CommResult.Err, "")
		committed <- n.CommResult.Caches
	}
	cn.Commit(ctx, &CommArgs{
		CacheRF: 2,
	})
	close(committed)
	for range committed {
	}

	c1, err := cid.Decode(cid1)
	require.NoError(t, err)

	has, err := cn.bs.Has(c1)
	require.NoError(t, err)
	require.Equal(t, true, has)

	// test Garbage Collectors by adding a new file, forcing an eviction
	dir2 := t.TempDir()
	data2 := make([]byte, 1000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data2)
	p2 := filepath.Join(dir2, "data2")
	err = os.WriteFile(p2, data2, 0666)
	require.NoError(t, err)

	cn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")
		added <- n.PutResult.Cid
	}
	cn.Put(ctx, &PutArgs{
		Path:      p2,
		ChunkSize: 1024,
	})
	cid2 := <-added

	committed = make(chan []string, 3)
	cn.notify = func(n Notify) {
		require.Equal(t, n.CommResult.Err, "")
		committed <- n.CommResult.Caches
	}
	cn.Commit(ctx, &CommArgs{
		CacheRF: 2,
	})
	close(committed)
	for range committed {
	}

	c2, err := cid.Decode(cid2)
	require.NoError(t, err)

	// check if blockstore contains new file
	has2, err := cn.bs.Has(c2)
	require.NoError(t, err)
	require.Equal(t, true, has2)

	// check if garbage collector evicted the first file
	has, err = cn.bs.Has(c1)
	require.NoError(t, err)
	require.Equal(t, false, has)
}

func TestGet(t *testing.T) {
	bgCtx := context.Background()

	ctx, cancel := context.WithTimeout(bgCtx, 4*time.Second)
	defer cancel()
	mn := mocknet.New(bgCtx)

	pn := newTestNode(bgCtx, mn, t)
	cn := newTestNode(bgCtx, mn, t)

	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	// Let the routing propagate to gossip
	time.Sleep(time.Second)

	dir := t.TempDir()

	data := make([]byte, 256000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data)
	p := filepath.Join(dir, "data1")
	err := os.WriteFile(p, data, 0666)
	require.NoError(t, err)

	added := make(chan string, 1)
	pn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")
		added <- n.PutResult.Cid
	}
	pn.Put(ctx, &PutArgs{
		Path:      p,
		ChunkSize: 1024,
	})
	<-added

	ref, err := pn.getRef("")
	require.NoError(t, err)
	committed := make(chan struct{}, 1)
	pn.notify = func(n Notify) {
		require.Equal(t, n.CommResult.Err, "")
		committed <- struct{}{}
	}
	pn.Commit(ctx, &CommArgs{
		CacheRF: 0,
	})
	<-committed

	cn.notify = func(n Notify) {
		require.Equal(t, n.GetResult.Err, "")
	}
	cn.Get(ctx, &GetArgs{
		Cid:      fmt.Sprintf("/%s/data1", ref.PayloadCID),
		Strategy: "SelectFirst",
		Timeout:  1,
	})

	// We should be able to request again this time from local storage
	got := make(chan GetResult, 1)
	cn.notify = func(n Notify) {
		require.Equal(t, n.GetResult.Err, "")
		got <- *n.GetResult
	}
	out := filepath.Join(dir, "dataout")
	cn.Get(ctx, &GetArgs{
		Cid:      fmt.Sprintf("/%s/data1", ref.PayloadCID.String()),
		Strategy: "SelectFirst",
		Timeout:  1,
		Out:      out,
	})
	<-got
	dataout := make([]byte, len(data))
	file, err := os.Open(out)
	require.NoError(t, err)

	_, err = file.Read(dataout)
	require.NoError(t, err)
	require.EqualValues(t, data, dataout)
}

func TestList(t *testing.T) {
	blockGen := blocksutil.NewBlockGenerator()
	ctx := context.Background()
	mn := mocknet.New(ctx)

	cn := newTestNode(ctx, mn, t)

	for i := 0; i < 10; i++ {
		require.NoError(t, cn.exch.Index().SetRef(&exchange.DataRef{
			PayloadCID:  blockGen.Next().Cid(),
			PayloadSize: 100,
		}))
	}
	out := make(chan *ListResult, 10)
	cn.notify = func(n Notify) {
		require.Equal(t, n.ListResult.Err, "")
		out <- n.ListResult
		if n.ListResult.Last {
			close(out)
		}
	}
	cn.List(ctx, &ListArgs{})
	for range out {
	}
}

// Commit 2 different files into a single transaction and then retrieve (Get)
// the files individually with 2 separate operations. Both Get operations are on the
// same transaction (ref) and based on the same root CID but retrieve 2 different files.
func TestMultipleGet(t *testing.T) {
	bgCtx := context.Background()

	ctx, cancel := context.WithTimeout(bgCtx, 10*time.Second)
	defer cancel()
	mn := mocknet.New(bgCtx)

	pn := newTestNode(bgCtx, mn, t)
	cn := newTestNode(bgCtx, mn, t)
	cn2 := newTestNode(bgCtx, mn, t)

	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	// Let the routing propagate to gossip
	time.Sleep(time.Second)

	dir := t.TempDir()

	// create data1
	data1 := make([]byte, 256000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data1)
	p1 := filepath.Join(dir, "data1")
	err := os.WriteFile(p1, data1, 0666)
	require.NoError(t, err)

	// add data1
	added1 := make(chan string, 1)
	pn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")
		added1 <- n.PutResult.Cid
	}
	pn.Put(ctx, &PutArgs{
		Path:      p1,
		ChunkSize: 1024,
	})
	<-added1

	// create data2
	data2 := make([]byte, 124000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data2)
	p2 := filepath.Join(dir, "data2")
	err = os.WriteFile(p2, data2, 0666)
	require.NoError(t, err)

	// add data2
	added2 := make(chan string, 1)
	pn.notify = func(n Notify) {
		require.Equal(t, n.PutResult.Err, "")
		added2 <- n.PutResult.Cid
	}
	pn.Put(ctx, &PutArgs{
		Path:      p2,
		ChunkSize: 1024,
	})
	<-added2

	// commit data1 + data2
	ref, err := pn.getRef("")
	require.NoError(t, err)
	committed := make(chan struct{}, 1)
	pn.notify = func(n Notify) {
		require.Equal(t, n.CommResult.Err, "")
		committed <- struct{}{}
	}
	pn.Commit(ctx, &CommArgs{
		CacheRF: 0,
	})
	<-committed

	cn.notify = func(n Notify) {
		require.Equal(t, n.GetResult.Err, "")
	}
	cn.Get(ctx, &GetArgs{
		Cid:      fmt.Sprintf("/%s/data1", ref.PayloadCID.String()),
		Strategy: "SelectFirst",
		Timeout:  1,
	})

	cn.Get(ctx, &GetArgs{
		Cid:      fmt.Sprintf("/%s/data2", ref.PayloadCID.String()),
		Strategy: "SelectFirst",
		Timeout:  1,
	})

	cn2.notify = func(n Notify) {
		require.Equal(t, "", n.GetResult.Err)
	}
	cn2.Get(ctx, &GetArgs{
		Cid:      fmt.Sprintf("/%s/data2", ref.PayloadCID.String()),
		Strategy: "SelectFirst",
		Timeout:  1,
	})
}

//todo TesExportKey
func TestImportKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)
	n := newTestNode(ctx, mn, t)

	h := "7b2254797065223a22626c73222c22507269766174654b6579223a226a6b55704e6a53493749664a4632434f6f505169344f79477a475241532b766b616c314e5a616f7a3853633d227d"
	err := n.importPrivateKey(ctx, h)
	require.NoError(t, err)

	expected, _ := address.NewFromString("f3w2ll4guubkslpmxseiqhtemwtmxdnhnshogd25gfrbhe6dso6kly2aj756wmcx2gq4jehn6x2z3ji4zlzioq")
	require.Equal(t, expected, n.exch.Wallet().DefaultAddress())
}

// Preload is a full integration test for gradually retrieving a DAG paid with a single
// payment channel
func TestPreload(t *testing.T) {
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	region := exchange.Regions["Europe"]

	// Provider setup
	tn1 := testutil.NewTestNode(mn, t)
	pn := &node{}
	pn.ds = tn1.Ds
	pn.bs = tn1.Bs
	pn.ms = tn1.Ms
	pn.dag = tn1.DAG
	pn.host = tn1.Host
	pfapi := filecoin.NewMockLotusAPI()
	popts := exchange.Options{
		Blockstore:  pn.bs,
		MultiStore:  pn.ms,
		RepoPath:    t.TempDir(),
		Regions:     []exchange.Region{region},
		FilecoinAPI: pfapi,
	}
	popts.Wallet = wallet.NewFromKeystore(keystore.NewMemKeystore(), wallet.WithFilAPI(popts.FilecoinAPI))
	pn.exch, err = exchange.New(ctx, pn.host, pn.ds, popts)
	require.NoError(t, err)

	// Client setup
	tn2 := testutil.NewTestNode(mn, t)
	cn := &node{}
	cn.ds = tn2.Ds
	cn.bs = tn2.Bs
	cn.ms = tn2.Ms
	cn.dag = tn2.DAG
	cn.host = tn2.Host
	cn.omg = NewOfferMgr()
	cfapi := filecoin.NewMockLotusAPI()
	copts := exchange.Options{
		Blockstore:  cn.bs,
		MultiStore:  cn.ms,
		RepoPath:    t.TempDir(),
		Regions:     []exchange.Region{region},
		FilecoinAPI: cfapi,
	}
	copts.Wallet = wallet.NewFromKeystore(keystore.NewMemKeystore(), wallet.WithFilAPI(copts.FilecoinAPI))
	cn.exch, err = exchange.New(ctx, cn.host, cn.ds, copts)
	require.NoError(t, err)

	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	// Let the routing propagate to gossip
	time.Sleep(time.Second)

	tx := pn.exch.Tx(ctx)

	data1 := make([]byte, 10000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data1)
	cid1, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data1))
	require.NoError(t, err)
	require.NoError(t, tx.Put("first", cid1, 10000))

	data2 := make([]byte, 14000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data2)
	cid2, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data2))
	require.NoError(t, err)
	require.NoError(t, tx.Put("second", cid2, 14000))

	data3 := make([]byte, 26000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data3)
	cid3, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data3))
	require.NoError(t, err)
	require.NoError(t, tx.Put("third", cid3, 26000))

	tx.SetCacheRF(0)
	require.NoError(t, tx.Commit())
	ref := tx.Ref()
	root := ref.PayloadCID
	require.NoError(t, pn.exch.Index().SetRef(ref))
	require.NoError(t, tx.Close())

	var chAddr address.Address
	var settle func()

	results, err := cn.Load(ctx, &GetArgs{Cid: root.String()})
	require.NoError(t, err)

	// Discovery went well, we got an offer
	select {
	case res := <-results:
		require.Equal(t, "DealStatusSelectedOffer", res.Status)

		// Prepare the payment channel mocks
		from := cn.exch.Wallet().DefaultAddress()
		to := pn.exch.Wallet().DefaultAddress()

		price, err := filecoin.ParseFIL(res.TotalFunds)
		require.NoError(t, err)

		createAmt := big.NewFromGo(price.Int)
		chAddr, settle = prepChannel(t, from, to, cfapi, pfapi, createAmt)

	case <-ctx.Done():
		t.Fatal("could not select an offer")
	}

	// Deal started correctly, we have a deal ID
	select {
	case res := <-results:
		require.NotEqual(t, "", res.DealID)
	case <-ctx.Done():
		t.Fatal("could not start the transfer")
	}

	lookup := testutil.FormatMsgLookup(t, chAddr)
	cfapi.SetMsgLookup(lookup)

loop:
	for {
		select {
		case res := <-results:
			if res.Status == "Completed" {
				break loop
			}
			require.Equal(t, "", res.Err)
		case <-ctx.Done():
			t.Fatal("could not complete transfer")
		}
	}

	// We should have a single channel
	channels, err := cn.exch.Payments().ListChannels()
	require.NoError(t, err)
	require.Equal(t, 1, len(channels))

	// Check if the ref is here
	ref, err = cn.exch.Index().GetRef(root)
	require.NoError(t, err)
	require.Equal(t, false, ref.Has("first"))

	// from now on we should have the funds to retrieve everything progressively
	// from the same peer using the same payment channel

	results, err = cn.Load(ctx, &GetArgs{Cid: fmt.Sprintf("%s/first", root)})
	require.NoError(t, err)

	for res := range results {
		require.Equal(t, "", res.Err)
	}

	// We should still have a single channel
	channels, err = cn.exch.Payments().ListChannels()
	require.NoError(t, err)
	require.Equal(t, 1, len(channels))

	results, err = cn.Load(ctx, &GetArgs{Cid: fmt.Sprintf("%s/second", root)})
	require.NoError(t, err)

	for res := range results {
		require.Equal(t, "", res.Err)
	}

	// We should still have a single channel
	channels, err = cn.exch.Payments().ListChannels()
	require.NoError(t, err)
	require.Equal(t, 1, len(channels))

	results, err = cn.Load(ctx, &GetArgs{Cid: fmt.Sprintf("%s/third", root)})
	require.NoError(t, err)

	for res := range results {
		require.Equal(t, "", res.Err)
	}

	// We should still have a single channel
	channels, err = cn.exch.Payments().ListChannels()
	require.NoError(t, err)
	require.Equal(t, 1, len(channels))

	// We retrieved all the keys so the offer should be removed
	_, err = cn.omg.GetOffer(root)
	require.Error(t, err)

	// update the actor state so the manager can settle things
	settle()
}

// LoadKey tests loading a single key in a transaction
func TestLoadKey(t *testing.T) {
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	region := exchange.Regions["Europe"]

	// Provider setup
	tn1 := testutil.NewTestNode(mn, t)
	pn := &node{}
	pn.ds = tn1.Ds
	pn.bs = tn1.Bs
	pn.ms = tn1.Ms
	pn.dag = tn1.DAG
	pn.host = tn1.Host
	pfapi := filecoin.NewMockLotusAPI()
	popts := exchange.Options{
		Blockstore:  pn.bs,
		MultiStore:  pn.ms,
		RepoPath:    t.TempDir(),
		Regions:     []exchange.Region{region},
		FilecoinAPI: pfapi,
	}
	popts.Wallet = wallet.NewFromKeystore(keystore.NewMemKeystore(), wallet.WithFilAPI(popts.FilecoinAPI))
	pn.exch, err = exchange.New(ctx, pn.host, pn.ds, popts)
	require.NoError(t, err)

	// Client setup
	tn2 := testutil.NewTestNode(mn, t)
	cn := &node{}
	cn.ds = tn2.Ds
	cn.bs = tn2.Bs
	cn.ms = tn2.Ms
	cn.dag = tn2.DAG
	cn.host = tn2.Host
	cn.omg = NewOfferMgr()
	cfapi := filecoin.NewMockLotusAPI()
	copts := exchange.Options{
		Blockstore:  cn.bs,
		MultiStore:  cn.ms,
		RepoPath:    t.TempDir(),
		Regions:     []exchange.Region{region},
		FilecoinAPI: cfapi,
	}
	copts.Wallet = wallet.NewFromKeystore(keystore.NewMemKeystore(), wallet.WithFilAPI(copts.FilecoinAPI))
	cn.exch, err = exchange.New(ctx, cn.host, cn.ds, copts)
	require.NoError(t, err)

	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	// Let the routing propagate to gossip
	time.Sleep(time.Second)

	tx := pn.exch.Tx(ctx)

	data1 := make([]byte, 10000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data1)
	cid1, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data1))
	require.NoError(t, err)
	require.NoError(t, tx.Put("first", cid1, 10000))

	data2 := make([]byte, 14000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data2)
	cid2, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data2))
	require.NoError(t, err)
	require.NoError(t, tx.Put("second", cid2, 14000))

	data3 := make([]byte, 26000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data3)
	cid3, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data3))
	require.NoError(t, err)
	require.NoError(t, tx.Put("third", cid3, 26000))

	tx.SetCacheRF(0)
	require.NoError(t, tx.Commit())
	ref := tx.Ref()
	root := ref.PayloadCID
	require.NoError(t, pn.exch.Index().SetRef(ref))
	require.NoError(t, tx.Close())

	// Prepare the payment channel mocks
	from := cn.exch.Wallet().DefaultAddress()
	to := pn.exch.Wallet().DefaultAddress()
	var chAddr address.Address
	var settle func()

	results, err := cn.Load(ctx, &GetArgs{Cid: root.String(), Key: "second"})
	require.NoError(t, err)

	// Discovery went well, we got an offer
	select {
	case res := <-results:
		require.Equal(t, "DealStatusSelectedOffer", res.Status)

		price, err := filecoin.ParseFIL(res.TotalFunds)
		require.NoError(t, err)

		createAmt := big.NewFromGo(price.Int)

		chAddr, settle = prepChannel(t, from, to, cfapi, pfapi, createAmt)
	case <-ctx.Done():
		t.Fatal("could not select an offer")
	}

	// Deal started correctly, we have a deal ID
	select {
	case res := <-results:
		require.NotEqual(t, "", res.DealID)
	case <-ctx.Done():
		t.Fatal("could not start the transfer")
	}

	lookup := testutil.FormatMsgLookup(t, chAddr)
	cfapi.SetMsgLookup(lookup)

loop:
	for {
		select {
		case res := <-results:
			if res.Status == "Completed" {
				break loop
			}
			require.Equal(t, "", res.Err)
		case <-ctx.Done():
			t.Fatal("could not complete transfer")
		}
	}

	// We should have a single channel
	channels, err := cn.exch.Payments().ListChannels()
	require.NoError(t, err)
	require.Equal(t, 1, len(channels))

	// Check if the ref is here
	ref, err = cn.exch.Index().GetRef(root)
	require.NoError(t, err)
	require.Equal(t, true, ref.Has("second"))

	settle()
}

// LoadAll tests loading all the content from a transaction
func TestLoadAll(t *testing.T) {
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mn := mocknet.New(ctx)

	region := exchange.Regions["Europe"]

	// Provider setup
	tn1 := testutil.NewTestNode(mn, t)
	pn := &node{}
	pn.ds = tn1.Ds
	pn.bs = tn1.Bs
	pn.ms = tn1.Ms
	pn.dag = tn1.DAG
	pn.host = tn1.Host
	pfapi := filecoin.NewMockLotusAPI()
	popts := exchange.Options{
		Blockstore:  pn.bs,
		MultiStore:  pn.ms,
		RepoPath:    t.TempDir(),
		Regions:     []exchange.Region{region},
		FilecoinAPI: pfapi,
	}
	popts.Wallet = wallet.NewFromKeystore(keystore.NewMemKeystore(), wallet.WithFilAPI(popts.FilecoinAPI))
	pn.exch, err = exchange.New(ctx, pn.host, pn.ds, popts)
	require.NoError(t, err)

	// Client setup
	tn2 := testutil.NewTestNode(mn, t)
	cn := &node{}
	cn.ds = tn2.Ds
	cn.bs = tn2.Bs
	cn.ms = tn2.Ms
	cn.dag = tn2.DAG
	cn.host = tn2.Host
	cn.omg = NewOfferMgr()
	cfapi := filecoin.NewMockLotusAPI()
	copts := exchange.Options{
		Blockstore:  cn.bs,
		MultiStore:  cn.ms,
		RepoPath:    t.TempDir(),
		Regions:     []exchange.Region{region},
		FilecoinAPI: cfapi,
	}
	copts.Wallet = wallet.NewFromKeystore(keystore.NewMemKeystore(), wallet.WithFilAPI(copts.FilecoinAPI))
	cn.exch, err = exchange.New(ctx, cn.host, cn.ds, copts)
	require.NoError(t, err)

	require.NoError(t, mn.LinkAll())
	require.NoError(t, mn.ConnectAllButSelf())

	// Let the routing propagate to gossip
	time.Sleep(time.Second)

	tx := pn.exch.Tx(ctx)

	data1 := make([]byte, 10000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data1)
	cid1, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data1))
	require.NoError(t, err)
	require.NoError(t, tx.Put("first", cid1, 10000))

	data2 := make([]byte, 14000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data2)
	cid2, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data2))
	require.NoError(t, err)
	require.NoError(t, tx.Put("second", cid2, 14000))

	data3 := make([]byte, 26000)
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(data3)
	cid3, err := pn.Add(ctx, "", tx.Store().DAG, bytes.NewReader(data3))
	require.NoError(t, err)
	require.NoError(t, tx.Put("third", cid3, 26000))

	tx.SetCacheRF(0)
	require.NoError(t, tx.Commit())
	ref := tx.Ref()
	root := ref.PayloadCID
	require.NoError(t, pn.exch.Index().SetRef(ref))
	require.NoError(t, tx.Close())

	// Prepare the payment channel mocks
	from := cn.exch.Wallet().DefaultAddress()
	to := pn.exch.Wallet().DefaultAddress()
	var chAddr address.Address
	var settle func()

	results, err := cn.Load(ctx, &GetArgs{Cid: root.String(), Key: "*"})
	require.NoError(t, err)

	// Discovery went well, we got an offer
	select {
	case res := <-results:
		require.Equal(t, "DealStatusSelectedOffer", res.Status)

		price, err := filecoin.ParseFIL(res.TotalFunds)
		require.NoError(t, err)

		createAmt := big.NewFromGo(price.Int)

		chAddr, settle = prepChannel(t, from, to, cfapi, pfapi, createAmt)
	case <-ctx.Done():
		t.Fatal("could not select an offer")
	}

	// Deal started correctly, we have a deal ID
	select {
	case res := <-results:
		require.NotEqual(t, "", res.DealID)
	case <-ctx.Done():
		t.Fatal("could not start the transfer")
	}

	lookup := testutil.FormatMsgLookup(t, chAddr)
	cfapi.SetMsgLookup(lookup)

loop:
	for {
		select {
		case res := <-results:
			if res.Status == "Completed" {
				break loop
			}
			require.Equal(t, "", res.Err)
		case <-ctx.Done():
			t.Fatal("could not complete transfer")
		}
	}

	// We should have a single channel
	channels, err := cn.exch.Payments().ListChannels()
	require.NoError(t, err)
	require.Equal(t, 1, len(channels))

	// Check if the ref is here
	ref, err = cn.exch.Index().GetRef(root)
	require.NoError(t, err)
	require.Equal(t, true, ref.Has("first"))
	require.Equal(t, true, ref.Has("second"))
	require.Equal(t, true, ref.Has("third"))

	settle()
}

func prepChannel(t *testing.T, from, to address.Address, c, p *filecoin.MockLotusAPI, createAmt big.Int) (address.Address, func()) {
	var blockGen = blocksutil.NewBlockGenerator()
	payerAddr := tutils.NewIDAddr(t, 102)
	payeeAddr := tutils.NewIDAddr(t, 103)

	act := &filecoin.Actor{
		Code:    blockGen.Next().Cid(),
		Head:    blockGen.Next().Cid(),
		Nonce:   1,
		Balance: createAmt,
	}
	c.SetActor(act)
	p.SetActor(act)

	chAddr := tutils.NewIDAddr(t, 101)
	initActorAddr := tutils.NewIDAddr(t, 100)
	hasher := func(data []byte) [32]byte { return [32]byte{} }

	builder := mock.NewBuilder(chAddr).
		WithBalance(createAmt, abi.NewTokenAmount(0)).
		WithEpoch(abi.ChainEpoch(1)).
		WithCaller(initActorAddr, builtin.InitActorCodeID).
		WithActorType(payeeAddr, builtin.AccountActorCodeID).
		WithActorType(payerAddr, builtin.AccountActorCodeID).
		WithHasher(hasher)

	// We need this mutex so we can read the actor state from inside goroutines
	var mu sync.Mutex
	rt := builder.Build(t)
	params := &paych.ConstructorParams{To: payeeAddr, From: payerAddr}
	rt.ExpectValidateCallerType(builtin.InitActorCodeID)
	actor := paych.Actor{}
	rt.Call(actor.Constructor, params)

	var st paych.State
	rt.GetState(&st)

	actState := filecoin.ActorState{
		Balance: createAmt,
		State:   st,
	}
	// We need to set an actor state as creating a voucher will query the state from the chain
	// both apis are sharing the same state
	c.SetActorState(&actState)
	p.SetActorState(&actState)
	// See channel tests for note about this
	objReader := func(c cid.Cid) []byte {
		mu.Lock()
		defer mu.Unlock()
		var bg testutil.BytesGetter
		rt.StoreGet(c, &bg)
		return bg.Bytes()
	}
	c.SetObjectReader(objReader)
	p.SetObjectReader(objReader)

	c.SetAccountKey(payerAddr, from)
	// provider is resolving the account key to verify the voucher signature
	p.SetAccountKey(payerAddr, from)
	p.SetAccountKey(payeeAddr, to)

	// Return success exit code from calls to check if voucher is spendable
	p.SetInvocResult(&filecoin.InvocResult{
		MsgRct: &filecoin.MessageReceipt{
			ExitCode: 0,
		},
	})

	collect := func() {
		mu.Lock()
		var st paych.State
		ep := abi.ChainEpoch(10)
		rt.SetEpoch(ep)

		rt.GetState(&st)
		rt.SetCaller(st.From, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(st.From, st.To)
		rt.Call(actor.Settle, nil)

		rt.GetState(&st)
		mu.Unlock()
		actState := filecoin.ActorState{
			Balance: createAmt,
			State:   st,
		}
		// update our actor state to the api so it's queryable
		p.SetActorState(&actState)

		lookup := testutil.FormatMsgLookup(t, chAddr)
		// We should have 2 chain txs we're waiting for
		for i := 0; i < 2; i++ {
			p.SetMsgLookup(lookup)
		}
	}

	return chAddr, collect
}
