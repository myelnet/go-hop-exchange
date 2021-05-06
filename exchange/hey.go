package exchange

import (
	"context"
	"fmt"
	"io"
	"time"

	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

//go:generate cbor-gen-for Hey

// HeyProtocol identifies the supply greeter protocol
const HeyProtocol = "/myel/pop/hey/1.0"

// HeyReceiver is the interface the HeyService expects to receive the hey messages
type HeyReceiver interface {
	Receive(peer.ID, Hey)
}

// HeyGetter is the interface they HeyService expects to provide a Hey message to send
type HeyGetter interface {
	GetHey() Hey
}

// LatencyRecorder is an interface to record RTT latency measured during the hey operation
type LatencyRecorder interface {
	RecordLatency(peer.ID, time.Duration) error
}

// PeerManager all the methods to maintain an optimal list of peers
type PeerManager interface {
	HeyReceiver
	LatencyRecorder
}

// HeyService greets new peers upon connecting to learn more about their region
// and their supply.
type HeyService struct {
	h  host.Host
	pm PeerManager
	hg HeyGetter
}

// NewHeyService creates new instance of the HeyService
func NewHeyService(h host.Host, pm PeerManager, hg HeyGetter) *HeyService {
	return &HeyService{h, pm, hg}
}

// Hey is the greeting message which takes in network info
type Hey struct {
	Regions   []RegionCode
	IndexRoot *cid.Cid // If the node has an empty index the root will be nil
	Cluster   string
}

// Run starts a new goroutine in which we listen for new peers we successfully connected to
// and sends a hey message
func (hs *HeyService) Run(ctx context.Context) error {
	hs.h.SetStreamHandler(HeyProtocol, hs.HandleStream)

	sub, err := hs.h.EventBus().Subscribe(new(event.EvtPeerIdentificationCompleted), eventbus.BufSize(1024))
	if err != nil {
		return fmt.Errorf("failed to subscribe to event bus: %w", err)
	}

	go func() {
		for evt := range sub.Out() {
			pic := evt.(event.EvtPeerIdentificationCompleted)
			go func() {
				if err := hs.SendHey(ctx, pic.Peer); err != nil {
					return
				}
			}()
		}
	}()
	return nil
}

// HandleStream is the multistream handler for the Hey protocol, it reads a Hey message and handles it
func (hs *HeyService) HandleStream(s network.Stream) {
	var hmsg Hey
	if err := cborutil.ReadCborRPC(s, &hmsg); err != nil {
		_ = s.Conn().Close()
		fmt.Println("failed to read CBOR Hey msg", err)
		return
	}
	hs.pm.Receive(s.Conn().RemotePeer(), hmsg)
	// We send back the seed to measure roundrip time
	go func() {
		defer s.Close()
		buf := make([]byte, 32)
		s.Write(buf)
	}()
}

// SendHey message to a given peer
func (hs *HeyService) SendHey(ctx context.Context, pid peer.ID) error {
	s, err := hs.h.NewStream(ctx, pid, HeyProtocol)
	if err != nil {
		return err
	}

	hmsg := hs.hg.GetHey()

	start := time.Now()
	if err := cborutil.WriteCborRPC(s, &hmsg); err != nil {
		return err
	}
	go func() {
		defer s.Close()

		_ = s.SetReadDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 32)
		_, err := io.ReadFull(s, buf)
		if err != nil {
			fmt.Println("failed to read pong msg", err)
		}
		now := time.Now()
		lat := now.Sub(start)
		hs.pm.RecordLatency(pid, lat)
	}()
	return nil
}
