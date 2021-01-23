package payments

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	cbortypes "github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/specs-actors/actors/builtin/paych"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	init2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/hannahhoward/go-pubsub"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/myelnet/go-hop-exchange/filecoin"
	"github.com/myelnet/go-hop-exchange/wallet"
)

// channel is an interface to manage the lifecycle of a payment channel
type channel struct {
	from          address.Address
	to            address.Address
	ctx           context.Context
	api           filecoin.API
	wal           wallet.Driver
	actStore      *cbor.BasicIpldStore
	store         *Store
	lk            *multiLock
	fundsReqQueue []*fundsReq
	msgListeners  msgListeners
}

func (ch *channel) messageBuilder(ctx context.Context, from address.Address) (MessageBuilder, error) {
	// TODO: check network version and make adjustments on actor version
	return message{from}, nil
}

// create the payment channel with an initial amout
func (ch *channel) create(ctx context.Context, amt filecoin.BigInt) (cid.Cid, error) {
	mb, err := ch.messageBuilder(ctx, ch.from)
	if err != nil {
		return cid.Undef, err
	}

	msg, err := mb.Create(ch.to, amt)
	cp := *msg
	msg = &cp
	if err != nil {
		return cid.Undef, err
	}

	smsg, err := ch.mpoolPush(ctx, msg)
	if err != nil {
		return cid.Undef, err
	}

	ci, err := ch.store.CreateChannel(ch.from, ch.to, smsg.Cid(), amt)
	if err != nil {
		return cid.Undef, fmt.Errorf("Unable to create channel in store: %v", err)
	}

	// Wait for the channel to be created on chain
	go ch.waitForPaychCreateMsg(ci.ChannelID, smsg.Cid())

	return smsg.Cid(), nil
}

func (ch *channel) mpoolPush(ctx context.Context, msg *filecoin.Message) (*filecoin.SignedMessage, error) {
	msg, err := ch.api.GasEstimateMessageGas(ctx, msg, nil, filecoin.EmptyTSK)
	if err != nil {
		return nil, err
	}

	act, err := ch.api.StateGetActor(ctx, msg.From, filecoin.EmptyTSK)
	if err != nil {
		return nil, err
	}
	msg.Nonce = act.Nonce
	mbl, err := msg.ToStorageBlock()
	if err != nil {
		return nil, err
	}

	sig, err := ch.wal.Sign(ctx, msg.From, mbl.Cid().Bytes())
	if err != nil {
		return nil, err
	}

	smsg := &filecoin.SignedMessage{
		Message:   *msg,
		Signature: *sig,
	}

	if _, err := ch.api.MpoolPush(ctx, smsg); err != nil {
		return nil, fmt.Errorf("MpoolPush failed with error: %v", err)
	}

	return smsg, nil
}

// Change the state of the channel in the store
func (ch *channel) mutateChannelInfo(channelID string, mutate func(*ChannelInfo)) {
	channelInfo, err := ch.store.ByChannelID(channelID)

	// If there's an error reading or writing to the store just log an error.
	// For now we're assuming it's unlikely to happen in practice.
	// Later we may want to implement a transactional approach, whereby
	// we record to the store that we're going to send a message, send
	// the message, and then record that the message was sent.
	if err != nil {
		fmt.Printf("Error reading channel info from store: %s", err)
		return
	}

	mutate(channelInfo)

	err = ch.store.putChannelInfo(channelInfo)
	if err != nil {
		fmt.Printf("Error writing channel info to store: %s\n", err)
	}
}

// waitForPaychCreateMsg waits for mcid to appear on chain and stores the robust address of the
// created payment channel
func (ch *channel) waitForPaychCreateMsg(channelID string, mcid cid.Cid) {
	err := ch.waitPaychCreateMsg(channelID, mcid)
	ch.msgWaitComplete(mcid, err)
}

// waitPaychCreateMsg wait for a given confidence index and cleans up if the message failed
func (ch *channel) waitPaychCreateMsg(channelID string, mcid cid.Cid) error {
	mwait, err := ch.api.StateWaitMsg(ch.ctx, mcid, uint64(5))
	if err != nil {
		fmt.Printf("wait msg: %v", err)
		return err
	}

	// If channel creation failed
	if mwait.Receipt.ExitCode != 0 {
		ch.lk.Lock()
		defer ch.lk.Unlock()

		// Channel creation failed, so remove the channel from the datastore
		dserr := ch.store.RemoveChannel(channelID)
		if dserr != nil {
			fmt.Printf("failed to remove channel %s: %s", channelID, dserr)
		}

		// Exit code 7 means out of gas
		err := fmt.Errorf("payment channel creation failed (exit code %d)", mwait.Receipt.ExitCode)
		fmt.Printf("Error: %v", err)
		return err
	}

	// TODO: ActorUpgrade abstract over this.
	// This "works" because it hasn't changed from v0 to v2, but we still
	// need an abstraction here.
	var decodedReturn init2.ExecReturn
	err = decodedReturn.UnmarshalCBOR(bytes.NewReader(mwait.Receipt.Return))
	if err != nil {
		fmt.Printf("Error decoding Receipt: %v", err)
		return err
	}

	ch.lk.Lock()
	defer ch.lk.Unlock()

	// Store robust address of channel
	ch.mutateChannelInfo(channelID, func(channelInfo *ChannelInfo) {
		channelInfo.Channel = &decodedReturn.RobustAddress
		channelInfo.Amount = channelInfo.PendingAmount
		channelInfo.PendingAmount = filecoin.NewInt(0)
		channelInfo.CreateMsg = nil
	})

	return nil
}

// msgWaitComplete is called when the message for a previous task is confirmed
// or there is an error.
func (ch *channel) msgWaitComplete(mcid cid.Cid, err error) {
	ch.lk.Lock()
	defer ch.lk.Unlock()

	// Save the message result to the store
	dserr := ch.store.SaveMessageResult(mcid, err)
	if dserr != nil {
		fmt.Printf("saving message result: %s", dserr)
	}

	// Inform listeners that the message has completed
	ch.msgListeners.fireMsgComplete(mcid, err)

	// The queue may have been waiting for msg completion to proceed, so
	// process the next queue item
	// if len(ca.fundsReqQueue) > 0 {
	// 	go ca.processQueue("")
	// }
}

// addFunds sends a message to add funds to the channel and returns the message cid
func (ch *channel) addFunds(ctx context.Context, channelInfo *ChannelInfo, amt filecoin.BigInt) (*cid.Cid, error) {
	msg := &filecoin.Message{
		To:     *channelInfo.Channel,
		From:   channelInfo.Control,
		Value:  amt,
		Method: 0,
	}

	smsg, err := ch.mpoolPush(ctx, msg)
	if err != nil {
		return nil, err
	}
	mcid := smsg.Cid()

	// Store the add funds message CID on the channel
	ch.mutateChannelInfo(channelInfo.ChannelID, func(ci *ChannelInfo) {
		ci.PendingAmount = amt
		ci.AddFundsMsg = &mcid
	})

	// Store a reference from the message CID to the channel, so that we can
	// look up the channel from the message CID
	err = ch.store.SaveNewMessage(channelInfo.ChannelID, mcid)
	if err != nil {
		fmt.Printf("saving add funds message CID %s: %s", mcid, err)
	}

	go ch.waitForAddFundsMsg(channelInfo.ChannelID, mcid)

	return &mcid, nil
}

// waitForAddFundsMsg waits for mcid to appear on chain and returns error, if any
func (ch *channel) waitForAddFundsMsg(channelID string, mcid cid.Cid) {
	err := ch.waitAddFundsMsg(channelID, mcid)
	ch.msgWaitComplete(mcid, err)
}

func (ch *channel) waitAddFundsMsg(channelID string, mcid cid.Cid) error {
	mwait, err := ch.api.StateWaitMsg(ch.ctx, mcid, uint64(5))
	if err != nil {
		fmt.Printf("Error waiting for chain message: %v", err)
		return err
	}

	if mwait.Receipt.ExitCode != 0 {
		err := fmt.Errorf("voucher channel creation failed: adding funds (exit code %d)", mwait.Receipt.ExitCode)
		fmt.Printf("Error: %v", err)

		ch.lk.Lock()
		defer ch.lk.Unlock()

		ch.mutateChannelInfo(channelID, func(channelInfo *ChannelInfo) {
			channelInfo.PendingAmount = filecoin.NewInt(0)
			channelInfo.AddFundsMsg = nil
		})

		return err
	}

	ch.lk.Lock()
	defer ch.lk.Unlock()

	// Store updated amount
	ch.mutateChannelInfo(channelID, func(channelInfo *ChannelInfo) {
		channelInfo.Amount = filecoin.BigAdd(channelInfo.Amount, channelInfo.PendingAmount)
		channelInfo.PendingAmount = filecoin.NewInt(0)
		channelInfo.AddFundsMsg = nil
	})

	return nil
}

// load the actor state from the lotus chain when we don't have a record of it locally
func (ch *channel) loadActorState(chAddr address.Address) (ChannelState, error) {
	actorState, err := ch.api.StateReadState(ch.ctx, chAddr, filecoin.EmptyTSK)

	// TODO: this is a hack to cast the types into the proper data model
	// there's probably a nicer way to do it
	stateEncod, err := json.Marshal(actorState.State)

	adtStore := adt.WrapStore(ch.ctx, ch.actStore)
	state := channelState{store: adtStore}
	err = json.Unmarshal(stateEncod, &state)
	if err != nil {
		return nil, fmt.Errorf("Error parsing actor state: %v", err)
	}

	raw, err := ch.api.ChainReadObj(ch.ctx, state.LaneStates)
	if err != nil {
		return nil, fmt.Errorf("Unable to read lane states from chain: %v", err)
	}

	block, err := blocks.NewBlockWithCid(raw, state.LaneStates)
	if err != nil {
		return nil, fmt.Errorf("Unable to make a block with obj: %v", err)
	}

	if err := ch.actStore.Blocks.Put(block); err != nil {
		return nil, fmt.Errorf("Unable to set block in store: %v", err)
	}

	return &state, nil
}

// ChannelState is an abstract version of payment channel state that works across
// versions. TODO: we need to handle future actor version upgrades
type ChannelState interface {
	cbortypes.Marshaler
	// Channel owner, who has funded the actor
	From() (address.Address, error)
	// Recipient of payouts from channel
	To() (address.Address, error)

	// Height at which the channel can be `Collected`
	SettlingAt() (abi.ChainEpoch, error)

	// Amount successfully redeemed through the payment channel, paid out on `Collect()`
	ToSend() (abi.TokenAmount, error)

	// Get total number of lanes
	LaneCount() (uint64, error)

	// Iterate lane states
	ForEachLaneState(cb func(idx uint64, dl LaneState) error) error
}

// channelState struct is a model for interacting with the payment channel actor state
type channelState struct {
	paych.State
	store adt.Store
	lsAmt *adt.Array
}

// Channel owner, who has funded the actor
func (s *channelState) From() (address.Address, error) {
	return s.State.From, nil
}

// Recipient of payouts from channel
func (s *channelState) To() (address.Address, error) {
	return s.State.To, nil
}

// Height at which the channel can be `Collected`
func (s *channelState) SettlingAt() (abi.ChainEpoch, error) {
	return s.State.SettlingAt, nil
}

// Amount successfully redeemed through the payment channel, paid out on `Collect()`
func (s *channelState) ToSend() (abi.TokenAmount, error) {
	return s.State.ToSend, nil
}

func (s *channelState) getOrLoadLsAmt() (*adt.Array, error) {
	if s.lsAmt != nil {
		return s.lsAmt, nil
	}

	// Get the lane state from the chain
	lsamt, err := adt.AsArray(s.store, s.State.LaneStates)
	if err != nil {
		return nil, err
	}

	s.lsAmt = lsamt
	return lsamt, nil
}

// Get total number of lanes
func (s *channelState) LaneCount() (uint64, error) {
	lsamt, err := s.getOrLoadLsAmt()
	if err != nil {
		return 0, err
	}
	return lsamt.Length(), nil
}

// Iterate lane states
func (s *channelState) ForEachLaneState(cb func(idx uint64, dl LaneState) error) error {
	// Get the lane state from the chain
	lsamt, err := s.getOrLoadLsAmt()
	if err != nil {
		return err
	}

	// Note: we use a map instead of an array to store laneStates because the
	// client sets the lane ID (the index) and potentially they could use a
	// very large index.
	var ls paych.LaneState
	return lsamt.ForEach(&ls, func(i int64) error {
		return cb(uint64(i), &laneState{ls})
	})
}

// LaneState is an abstract copy of the state of a single lane
// not to be mixed up with the original paych.LaneState struct
type LaneState interface {
	Redeemed() (big.Int, error)
	Nonce() (uint64, error)
}

type laneState struct {
	paych.LaneState
}

func (ls *laneState) Redeemed() (big.Int, error) {
	return ls.LaneState.Redeemed, nil
}

func (ls *laneState) Nonce() (uint64, error) {
	return ls.LaneState.Nonce, nil
}

// paychFundsRes is the response to a create channel or add funds request
type paychFundsRes struct {
	channel address.Address
	mcid    cid.Cid
	err     error
}

// fundsReq is a request to create a channel or add funds to a channel
type fundsReq struct {
	ctx     context.Context
	promise chan *paychFundsRes
	amt     filecoin.BigInt

	lk sync.Mutex
	// merge parent, if this req is part of a merge
	merge *mergedFundsReq
	// whether the req's context has been cancelled
	active bool
}

func newFundsReq(ctx context.Context, amt filecoin.BigInt) *fundsReq {
	promise := make(chan *paychFundsRes)
	return &fundsReq{
		ctx:     ctx,
		promise: promise,
		amt:     amt,
		active:  true,
	}
}

// onComplete is called when the funds request has been executed
func (r *fundsReq) onComplete(res *paychFundsRes) {
	select {
	case <-r.ctx.Done():
	case r.promise <- res:
	}
}

// cancel is called when the req's context is cancelled
func (r *fundsReq) cancel() {
	r.lk.Lock()

	r.active = false
	m := r.merge

	r.lk.Unlock()

	// If there's a merge parent, tell the merge parent to check if it has any
	// active reqs left
	if m != nil {
		m.checkActive()
	}
}

// isActive indicates whether the req's context has been cancelled
func (r *fundsReq) isActive() bool {
	r.lk.Lock()
	defer r.lk.Unlock()

	return r.active
}

// setMergeParent sets the merge that this req is part of
func (r *fundsReq) setMergeParent(m *mergedFundsReq) {
	r.lk.Lock()
	defer r.lk.Unlock()

	r.merge = m
}

// mergedFundsReq merges together multiple add funds requests that are queued
// up, so that only one message is sent for all the requests (instead of one
// message for each request)
type mergedFundsReq struct {
	ctx    context.Context
	cancel context.CancelFunc
	reqs   []*fundsReq
}

func newMergedFundsReq(reqs []*fundsReq) *mergedFundsReq {
	ctx, cancel := context.WithCancel(context.Background())
	m := &mergedFundsReq{
		ctx:    ctx,
		cancel: cancel,
		reqs:   reqs,
	}

	for _, r := range m.reqs {
		r.setMergeParent(m)
	}

	// If the requests were all cancelled while being added, cancel the context
	// immediately
	m.checkActive()

	return m
}

// Called when a fundsReq is cancelled
func (m *mergedFundsReq) checkActive() {
	// Check if there are any active fundsReqs
	for _, r := range m.reqs {
		if r.isActive() {
			return
		}
	}

	// If all fundsReqs have been cancelled, cancel the context
	m.cancel()
}

// onComplete is called when the queue has executed the mergeFundsReq.
// Calls onComplete on each fundsReq in the mergeFundsReq.
func (m *mergedFundsReq) onComplete(res *paychFundsRes) {
	for _, r := range m.reqs {
		if r.isActive() {
			r.onComplete(res)
		}
	}
}

// sum is the sum of the amounts in all requests in the merge
func (m *mergedFundsReq) sum() filecoin.BigInt {
	sum := filecoin.NewInt(0)
	for _, r := range m.reqs {
		if r.isActive() {
			sum = filecoin.BigAdd(sum, r.amt)
		}
	}
	return sum
}

type rwlock interface {
	RLock()
	RUnlock()
}

// multiLock manages locking for a specific channel.
// Some operations update the state of a single channel, and need to block
// other operations only on the same channel's state.
// Some operations update state that affects all channels, and need to block
// any operation against any channel.
type multiLock struct {
	globalLock rwlock
	chanLock   sync.Mutex
}

func (l *multiLock) Lock() {
	// Wait for other operations by this channel to finish.
	// Exclusive per-channel (no other ops by this channel allowed).
	l.chanLock.Lock()
	// Wait for operations affecting all channels to finish.
	// Allows ops by other channels in parallel, but blocks all operations
	// if global lock is taken exclusively (eg when adding a channel)
	l.globalLock.RLock()
}

func (l *multiLock) Unlock() {
	l.globalLock.RUnlock()
	l.chanLock.Unlock()
}

type msgListeners struct {
	ps *pubsub.PubSub
}

type msgCompleteEvt struct {
	mcid cid.Cid
	err  error
}

type subscriberFn func(msgCompleteEvt)

func newMsgListeners() msgListeners {
	ps := pubsub.New(func(event pubsub.Event, subFn pubsub.SubscriberFn) error {
		evt, ok := event.(msgCompleteEvt)
		if !ok {
			return fmt.Errorf("wrong type of event")
		}
		sub, ok := subFn.(subscriberFn)
		if !ok {
			return fmt.Errorf("wrong type of subscriber")
		}
		sub(evt)
		return nil
	})
	return msgListeners{ps: ps}
}

// onMsgComplete registers a callback for when the message with the given cid
// completes
func (ml *msgListeners) onMsgComplete(mcid cid.Cid, cb func(error)) pubsub.Unsubscribe {
	var fn subscriberFn = func(evt msgCompleteEvt) {
		if mcid.Equals(evt.mcid) {
			cb(evt.err)
		}
	}
	return ml.ps.Subscribe(fn)
}

// fireMsgComplete is called when a message completes
func (ml *msgListeners) fireMsgComplete(mcid cid.Cid, err error) {
	e := ml.ps.Publish(msgCompleteEvt{mcid: mcid, err: err})
	if e != nil {
		// In theory we shouldn't ever get an error here
		fmt.Printf("unexpected error publishing message complete: %s", e)
	}
}
