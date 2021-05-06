package exchange

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-eventbus"
	connmgr "github.com/libp2p/go-libp2p-connmgr"
	peer "github.com/libp2p/go-libp2p-core/peer"
	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/myelnet/pop/internal/testutil"
	sel "github.com/myelnet/pop/selectors"
	"github.com/stretchr/testify/require"
	bhost "github.com/tchardin/go-libp2p-blankhost"
)

type mockRetriever struct {
	dt      datatransfer.Manager
	idx     *Index
	mu      sync.Mutex
	routing map[cid.Cid]peer.ID
}

func NewMockRetriever(dt datatransfer.Manager, idx *Index) *mockRetriever {
	dt.RegisterVoucherType(&testutil.FakeDTType{}, &testutil.FakeDTValidator{})
	dt.RegisterTransportConfigurer(&testutil.FakeDTType{}, func(
		chID datatransfer.ChannelID,
		voucher datatransfer.Voucher,
		tp datatransfer.Transport,
	) {
		k := voucher.(*testutil.FakeDTType).Data
		c, err := cid.Decode(k)
		if err != nil {
			panic("bad CID")
		}
		store, err := idx.GetStore(c)
		if err != nil {
			panic("no store for content")
		}
		tp.(StoreConfigurableTransport).UseStore(chID, store.Loader, store.Storer)
	})
	return &mockRetriever{
		dt:      dt,
		idx:     idx,
		routing: make(map[cid.Cid]peer.ID),
	}
}

func (mr *mockRetriever) SetRoute(k cid.Cid, p peer.ID) {
	mr.mu.Lock()
	mr.routing[k] = p
	mr.mu.Unlock()
}

func (mr *mockRetriever) FindAndRetrieve(ctx context.Context, l cid.Cid) error {
	done := make(chan error, 1)
	unsub := mr.dt.SubscribeToEvents(func(event datatransfer.Event, chState datatransfer.ChannelState) {
		switch chState.Status() {
		case datatransfer.Completed:
			done <- nil
		case datatransfer.Failed, datatransfer.Cancelled:
			done <- fmt.Errorf(chState.Message())
		}
	})
	defer unsub()
	mr.mu.Lock()
	peer, ok := mr.routing[l]
	mr.mu.Unlock()
	if !ok {
		panic("fail to find provider in mock routing")
	}
	mr.idx.SetRef(&DataRef{
		PayloadCID:  l,
		PayloadSize: int64(256000),
		StoreID:     mr.idx.ms.Next(),
	})
	_, err := mr.dt.OpenPullDataChannel(ctx, peer, &testutil.FakeDTType{Data: l.String()}, l, sel.All())
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

func TestReplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	mn := mocknet.New(ctx)

	withSwarmT := func(tn *testutil.TestNode) {
		netw := swarmt.GenSwarm(t, context.Background())
		h := bhost.NewBlankHost(netw, bhost.WithConnectionManager(
			connmgr.NewConnManager(10, 11, time.Second),
		))
		tn.Host = h
	}
	names := make(map[string]peer.ID)
	setupNode := func(name string) (*testutil.TestNode, *Replication, *mockRetriever) {
		n := testutil.NewTestNode(mn, t, withSwarmT)
		names[name] = n.Host.ID()
		n.SetupDataTransfer(ctx, t)
		idx, err := NewIndex(n.Ds, n.Ms, WithBounds(2000000, 1800000))
		require.NoError(t, err)
		rtv := NewMockRetriever(n.Dt, idx)
		repl := NewReplication(
			n.Host,
			idx,
			n.Dt,
			rtv,
			[]Region{global},
		)
		repl.interval = 2 * time.Second
		require.NoError(t, repl.Start(ctx))
		return n, repl, rtv
	}

	// Topology:
	/*
	   A -- B -- C -- D
	   | \/ | \/ | \/ |
	   | /\	| /\ | /\ |
	   H -- G -- F -- E
	*/

	nA, _, _ := setupNode("A")

	nB, rB, _ := setupNode("B")

	testutil.Connect(nA, nB)

	nC, _, _ := setupNode("C")

	testutil.Connect(nB, nC)

	nD, rD, _ := setupNode("D")

	testutil.Connect(nC, nD)

	nE, _, _ := setupNode("E")

	testutil.Connect(nD, nE)
	testutil.Connect(nC, nE)

	nF, rF, _ := setupNode("F")

	testutil.Connect(nD, nF)
	testutil.Connect(nE, nF)
	testutil.Connect(nC, nF)
	testutil.Connect(nB, nF)

	time.Sleep(time.Second)

	// 1) D write
	fnameD := nD.CreateRandomFile(t, 256000)
	linkD, storeIDD, _ := nD.LoadFileToNewStore(ctx, t, fnameD)
	rootCidD := linkD.(cidlink.Link).Cid
	require.NoError(t, rD.idx.SetRef(&DataRef{
		PayloadCID: rootCidD,
		StoreID:    storeIDD,
	}))
	optsD := DefaultDispatchOptions
	optsD.RF = 3
	resD := rD.Dispatch(rootCidD, uint64(256000), optsD)
	for r := range resD {
		switch r.Provider {
		case names["C"], names["E"], names["F"]:
		default:
			t.Fatal("sent to wrong peer")
		}
	}

	// 2) F write
	fnameF := nF.CreateRandomFile(t, 256000)
	linkF, storeIDF, _ := nF.LoadFileToNewStore(ctx, t, fnameF)
	rootCidF := linkF.(cidlink.Link).Cid
	require.NoError(t, rF.idx.SetRef(&DataRef{
		PayloadCID: rootCidF,
		StoreID:    storeIDF,
	}))
	optsF := DefaultDispatchOptions
	optsF.RF = 4
	resF := rF.Dispatch(rootCidF, uint64(256000), optsF)
	for r := range resF {
		switch r.Provider {
		case names["E"], names["D"], names["C"], names["B"]:
		default:
			t.Fatal("wrong peer")
		}
	}

	// New node G joins the network
	nG, _, rtvG := setupNode("G")
	isubG, err := nG.Host.EventBus().Subscribe(new(IndexEvt), eventbus.BufSize(16))
	require.NoError(t, err)

	// Set the content routing
	rtvG.SetRoute(rootCidD, nC.Host.ID())
	rtvG.SetRoute(rootCidF, nF.Host.ID())

	testutil.Connect(nC, nG)
	testutil.Connect(nF, nG)
	testutil.Connect(nB, nG)
	testutil.Connect(nA, nG)

	time.Sleep(time.Second)

	// We should be loading 2 CIDs
	for i := 0; i < 2; i++ {
		select {
		case <-isubG.Out():
		case <-ctx.Done():
			t.Fatal("G could not receive all indexes")
		}
	}
	isubG.Close()

	// 3) B write
	fnameB := nB.CreateRandomFile(t, 256000)
	linkB, storeIDB, _ := nB.LoadFileToNewStore(ctx, t, fnameB)
	rootCidB := linkB.(cidlink.Link).Cid
	require.NoError(t, rB.idx.SetRef(&DataRef{
		PayloadCID: rootCidB,
		StoreID:    storeIDB,
	}))
	optsB := DefaultDispatchOptions
	optsB.RF = 4
	resB := rB.Dispatch(rootCidB, uint64(256000), optsB)
	for r := range resB {
		switch r.Provider {
		case names["C"], names["F"], names["G"], names["A"]:
		default:
			t.Fatal("wrong peer")
		}
	}

	// New node H joins the network
	nH, rH, rtvH := setupNode("H")
	isubH, err := nH.Host.EventBus().Subscribe(new(IndexEvt), eventbus.BufSize(16))
	require.NoError(t, err)

	// Set the content routing
	rtvH.SetRoute(rootCidD, nG.Host.ID())
	rtvH.SetRoute(rootCidF, nB.Host.ID())
	rtvH.SetRoute(rootCidB, nA.Host.ID())

	testutil.Connect(nB, nH)
	testutil.Connect(nG, nH)
	testutil.Connect(nA, nH)

	time.Sleep(time.Second)

	// We should be receiving 3 CIDs
	for i := 0; i < 3; i++ {
		select {
		case <-isubH.Out():
		case <-ctx.Done():
			t.Fatal("H could not receive all indexes")
		}
	}
	isubH.Close()

	// 4) H write
	fnameH := nH.CreateRandomFile(t, 256000)
	linkH, storeIDH, _ := nH.LoadFileToNewStore(ctx, t, fnameH)
	rootCidH := linkH.(cidlink.Link).Cid
	require.NoError(t, rH.idx.SetRef(&DataRef{
		PayloadCID: rootCidH,
		StoreID:    storeIDH,
	}))
	optsH := DefaultDispatchOptions
	optsH.RF = 3
	resH := rH.Dispatch(rootCidH, uint64(256000), optsH)
	for r := range resH {
		switch r.Provider {
		case names["A"], names["B"], names["G"]:
		default:
			t.Fatal("wrong peer")
		}
	}
}

func TestMultiDispatchStreams(t *testing.T) {
	// Loop is useful for detecting any flakiness
	for i := 0; i < 1; i++ {
		t.Run(fmt.Sprintf("Run %d", i), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			mn := mocknet.New(ctx)

			n1 := testutil.NewTestNode(mn, t)
			n1.SetupDataTransfer(ctx, t)
			t.Cleanup(func() {
				err := n1.Dt.Stop(ctx)
				require.NoError(t, err)
			})

			fname := n1.CreateRandomFile(t, 256000)

			root, storeID, origBytes := n1.LoadFileToNewStore(ctx, t, fname)
			rootCid := root.(cidlink.Link).Cid

			regions := []Region{
				{
					Name: "TestRegion",
					Code: CustomRegion,
				},
			}

			idx, err := NewIndex(n1.Ds, n1.Ms)
			require.NoError(t, err)
			hn := NewReplication(n1.Host, idx, n1.Dt, NewMockRetriever(n1.Dt, idx), regions)
			require.NoError(t, idx.SetRef(&DataRef{
				PayloadCID: rootCid,
				StoreID:    storeID,
			}))
			sub, err := hn.h.EventBus().Subscribe(new(HeyEvt), eventbus.BufSize(16))
			require.NoError(t, err)
			require.NoError(t, hn.Start(ctx))

			tnds := make(map[peer.ID]*testutil.TestNode)
			receivers := make(map[peer.ID]*Replication)

			for i := 0; i < 7; i++ {
				tnode := testutil.NewTestNode(mn, t)
				tnode.SetupDataTransfer(ctx, t)
				t.Cleanup(func() {
					err := tnode.Dt.Stop(ctx)
					require.NoError(t, err)
				})
				idx, err := NewIndex(tnode.Ds, tnode.Ms)
				require.NoError(t, err)
				hn1 := NewReplication(tnode.Host, idx, tnode.Dt, NewMockRetriever(tnode.Dt, idx), regions)
				require.NoError(t, hn1.Start(ctx))
				receivers[tnode.Host.ID()] = hn1
				tnds[tnode.Host.ID()] = tnode
			}

			err = mn.LinkAll()
			require.NoError(t, err)

			err = mn.ConnectAllButSelf()
			require.NoError(t, err)

			time.Sleep(time.Second)

			// Wait for all peers to be received in the peer manager
			for i := 0; i < 7; i++ {
				select {
				case <-sub.Out():
				case <-ctx.Done():
					t.Fatal("all peers didn't get in the peermgr")
				}
			}

			res := hn.Dispatch(rootCid, uint64(len(origBytes)), DefaultDispatchOptions)

			var recs []PRecord
			for rec := range res {
				recs = append(recs, rec)
			}
			require.Equal(t, len(recs), 7)

			time.Sleep(time.Second)
			for _, r := range recs {
				store, err := receivers[r.Provider].idx.GetStore(rootCid)
				require.NoError(t, err)
				tnds[r.Provider].VerifyFileTransferred(ctx, t, store.DAG, rootCid, origBytes)
			}

		})
	}
}

// In some rare cases where our node isn't connected to any peer we should still
// be able to fail gracefully
func TestSendDispatchNoPeers(t *testing.T) {
	bgCtx := context.Background()

	mn := mocknet.New(bgCtx)

	n1 := testutil.NewTestNode(mn, t)
	n1.SetupDataTransfer(bgCtx, t)

	fname := n1.CreateRandomFile(t, 256000)

	link, storeID, origBytes := n1.LoadFileToNewStore(bgCtx, t, fname)
	rootCid := link.(cidlink.Link).Cid

	regions := []Region{
		{
			Name: "TestRegion",
			Code: CustomRegion,
		},
	}

	idx, err := NewIndex(n1.Ds, n1.Ms)
	require.NoError(t, err)
	supply := NewReplication(n1.Host, idx, n1.Dt, NewMockRetriever(n1.Dt, idx), regions)
	require.NoError(t, idx.SetRef(&DataRef{
		PayloadCID: rootCid,
		StoreID:    storeID,
	}))
	require.NoError(t, supply.Start(bgCtx))

	options := DispatchOptions{
		BackoffMin:     10 * time.Millisecond,
		BackoffAttemps: 4,
		RF:             5,
	}
	res := supply.Dispatch(rootCid, uint64(len(origBytes)), options)
	for range res {
	}
}

// The role of this test is to make sure we never dispatch content to unwanted regions
func TestSendDispatchDiffRegions(t *testing.T) {
	bgCtx := context.Background()

	ctx, cancel := context.WithTimeout(bgCtx, 10*time.Second)
	defer cancel()

	mn := mocknet.New(bgCtx)

	n1 := testutil.NewTestNode(mn, t)
	n1.SetupDataTransfer(bgCtx, t)
	t.Cleanup(func() {
		err := n1.Dt.Stop(ctx)
		require.NoError(t, err)
	})

	fname := n1.CreateRandomFile(t, 512000)

	// n1 is our client is adding a file to the store
	link, storeID, origBytes := n1.LoadFileToNewStore(bgCtx, t, fname)
	rootCid := link.(cidlink.Link).Cid

	asia := []Region{
		Regions["Asia"],
	}

	idx, err := NewIndex(n1.Ds, n1.Ms)
	require.NoError(t, err)
	supply := NewReplication(n1.Host, idx, n1.Dt, NewMockRetriever(n1.Dt, idx), asia)
	sub, err := n1.Host.EventBus().Subscribe(new(HeyEvt), eventbus.BufSize(16))
	require.NoError(t, err)
	require.NoError(t, supply.Start(ctx))

	asiaNodes := make(map[peer.ID]*testutil.TestNode)
	asiaSupplies := make(map[peer.ID]*Replication)
	// We add a bunch of asian retrieval providers
	for i := 0; i < 5; i++ {
		n := testutil.NewTestNode(mn, t)
		n.SetupDataTransfer(bgCtx, t)
		t.Cleanup(func() {
			err := n.Dt.Stop(ctx)
			require.NoError(t, err)
		})

		idx, err := NewIndex(n.Ds, n.Ms)
		require.NoError(t, err)
		s := NewReplication(n.Host, idx, n.Dt, NewMockRetriever(n1.Dt, idx), asia)
		require.NoError(t, s.Start(ctx))

		asiaNodes[n.Host.ID()] = n
		asiaSupplies[n.Host.ID()] = s
	}

	africa := []Region{
		Regions["Africa"],
	}

	africaNodes := make(map[peer.ID]*testutil.TestNode)
	var africaSupplies []*Replication
	// Add african providers
	for i := 0; i < 3; i++ {
		n := testutil.NewTestNode(mn, t)
		n.SetupDataTransfer(bgCtx, t)
		t.Cleanup(func() {
			err := n.Dt.Stop(ctx)
			require.NoError(t, err)
		})

		idx, err := NewIndex(n.Ds, n.Ms)
		require.NoError(t, err)

		s := NewReplication(n.Host, idx, n.Dt, NewMockRetriever(n.Dt, idx), africa)
		require.NoError(t, s.Start(ctx))

		africaNodes[n.Host.ID()] = n
		africaSupplies = append(africaSupplies, s)
	}

	err = mn.LinkAll()
	require.NoError(t, err)

	err = mn.ConnectAllButSelf()
	require.NoError(t, err)

	time.Sleep(time.Second)

	require.NoError(t, idx.SetRef(&DataRef{
		PayloadCID: rootCid,
		StoreID:    storeID,
	}))

	// Wait for all peers to be received in the peer manager
	for i := 0; i < 5; i++ {
		select {
		case <-sub.Out():
		case <-ctx.Done():
			t.Fatal("all peers didn't get in the peermgr")
		}
	}
	// get 5 requests and give up after 4 attemps
	options := DispatchOptions{
		BackoffMin:     100 * time.Millisecond,
		BackoffAttemps: 4,
		RF:             7,
	}
	res := supply.Dispatch(rootCid, uint64(len(origBytes)), options)

	var recipients []PRecord
	for rec := range res {
		recipients = append(recipients, rec)
	}
	for _, p := range recipients {
		store, err := asiaSupplies[p.Provider].idx.GetStore(rootCid)
		require.NoError(t, err)

		asiaNodes[p.Provider].VerifyFileTransferred(ctx, t, store.DAG, rootCid, origBytes)
	}
	require.Equal(t, 5, len(recipients))
}

func TestClustering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	mn := mocknet.New(ctx)

	withSwarmT := func(tn *testutil.TestNode) {
		netw := swarmt.GenSwarm(t, context.Background())
		h := bhost.NewBlankHost(netw, bhost.WithConnectionManager(
			connmgr.NewConnManager(10, 11, time.Second),
		))
		tn.Host = h
	}
	setupNode := func(name string) (*testutil.TestNode, *Replication) {
		n := testutil.NewTestNode(mn, t, withSwarmT)
		n.SetupDataTransfer(ctx, t)
		idx, err := NewIndex(n.Ds, n.Ms, WithBounds(2000000, 1800000))
		require.NoError(t, err)
		repl := &Replication{
			h:         n.Host,
			idx:       idx,
			pm:        NewPeerMgr(n.Host, []Region{global}),
			clusterID: n.Host.ID().String(),
			rgs:       []Region{global},
		}
		return n, repl
	}

	nA, rA := setupNode("A")
	nB, rB := setupNode("B")
	nC, rC := setupNode("C")
	nD, rD := setupNode("D")
	nE, rE := setupNode("E")
	nF, rF := setupNode("F")
	nG, rG := setupNode("G")
	nH, rH := setupNode("H")

	// Doesn't really matter
	err := mn.LinkAll()
	require.NoError(t, err)

	err = mn.ConnectAllButSelf()
	require.NoError(t, err)

	// A connects with B
	rA.pm.Receive(nB.Host.ID(), rB.GetHey())
	require.NoError(t, rA.pm.RecordLatency(nB.Host.ID(), 100*time.Millisecond))
	rB.pm.Receive(nA.Host.ID(), rA.GetHey())
	require.NoError(t, rB.pm.RecordLatency(nA.Host.ID(), 100*time.Millisecond))

	rA.upgradeCluster(rB.ClusterID(), nB.Host.ID())
	rB.upgradeCluster(rA.ClusterID(), nA.Host.ID())

	// A connects with C
	rA.pm.Receive(nC.Host.ID(), rC.GetHey())
	require.NoError(t, rA.pm.RecordLatency(nC.Host.ID(), 200*time.Millisecond))
	rC.pm.Receive(nA.Host.ID(), rA.GetHey())
	require.NoError(t, rC.pm.RecordLatency(nA.Host.ID(), 200*time.Millisecond))

	rA.upgradeCluster(rC.ClusterID(), nC.Host.ID())
	rC.upgradeCluster(rA.ClusterID(), nA.Host.ID())

	// B connects with C
	rB.pm.Receive(nC.Host.ID(), rC.GetHey())
	require.NoError(t, rB.pm.RecordLatency(nC.Host.ID(), 100*time.Millisecond))
	rC.pm.Receive(nB.Host.ID(), rB.GetHey())
	require.NoError(t, rC.pm.RecordLatency(nB.Host.ID(), 100*time.Millisecond))

	rB.upgradeCluster(rC.ClusterID(), nC.Host.ID())
	rC.upgradeCluster(rB.ClusterID(), nB.Host.ID())

	require.Equal(t, nC.Host.ID().String(), rA.ClusterID())
	require.Equal(t, nB.Host.ID().String(), rB.ClusterID())
	require.Equal(t, nC.Host.ID().String(), rC.ClusterID())
	require.Equal(t, nD.Host.ID().String(), rD.ClusterID())
	require.Equal(t, nE.Host.ID().String(), rE.ClusterID())
	require.Equal(t, nF.Host.ID().String(), rF.ClusterID())
	require.Equal(t, nG.Host.ID().String(), rG.ClusterID())
	require.Equal(t, nH.Host.ID().String(), rH.ClusterID())
}
