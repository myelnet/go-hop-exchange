package exchange

import (
	"errors"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

// Peer contains information recorded while interacted with a peer
type Peer struct {
	Regions []RegionCode
	Latency time.Duration
}

// PeerMgr is in charge of maintaining an optimal network of peers to coordinate with
type PeerMgr struct {
	h       host.Host
	regions map[RegionCode]Region
	emitter event.Emitter

	mu    sync.Mutex
	peers map[peer.ID]Peer
}

// HeyEvt is emitted when a Hey is received and accessible via the libp2p event bus subscription
type HeyEvt struct {
	Peer      peer.ID
	Cluster   string
	IndexRoot *cid.Cid // nil index root means empty index i.e. brand new node
}

// NewPeerMgr prepares a new PeerMgr instance
func NewPeerMgr(h host.Host, regions []Region) *PeerMgr {
	reg := make(map[RegionCode]Region, len(regions))
	for _, r := range regions {
		reg[r.Code] = r
	}

	pm := &PeerMgr{
		h:       h,
		regions: reg,
		peers:   make(map[peer.ID]Peer),
	}
	pm.emitter, _ = h.EventBus().Emitter(new(HeyEvt))
	h.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(_ network.Network, c network.Conn) {
			pm.mu.Lock()
			defer pm.mu.Unlock()
			if _, ok := pm.peers[c.RemotePeer()]; ok {
				delete(pm.peers, c.RemotePeer())
			}
		},
	})
	return pm
}

// Receive a new greeting from peer
func (pm *PeerMgr) Receive(p peer.ID, h Hey) {
	for _, r := range h.Regions {
		// We only save peers who are in the same region as us
		if reg, ok := pm.regions[r]; ok {
			pm.emitter.Emit(HeyEvt{
				Peer:      p,
				Cluster:   h.Cluster,
				IndexRoot: h.IndexRoot,
			})
			// These peers should be trimmed last when the number of connections overflows
			pm.h.ConnManager().TagPeer(p, reg.Name, 10)
			pm.mu.Lock()
			pm.peers[p] = Peer{
				Regions: h.Regions,
			}
			pm.mu.Unlock()
		}
	}
}

// RecordLatency for a given peer
func (pm *PeerMgr) RecordLatency(p peer.ID, t time.Duration) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	peer, ok := pm.peers[p]
	if !ok {
		return errors.New("no peer given ID")
	}
	peer.Latency = t
	pm.peers[p] = peer
	return nil
}

// Peers returns n active peers for a given list of regions and peers to ignore
func (pm *PeerMgr) Peers(n int, rl []Region, ignore map[peer.ID]bool) []peer.ID {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	var peers []peer.ID
	for _, r := range rl {
		for p, v := range pm.peers {
			if ignore[p] {
				continue
			}
			for _, rc := range v.Regions {
				if rc == r.Code {
					peers = append(peers, p)
				}
			}
			// Check if we have enough peers and return
			if len(peers) == n {
				return peers
			}
		}
	}
	return peers
}

// AllPeers returns all the peers known by the manager
func (pm *PeerMgr) AllPeers() map[peer.ID]Peer {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.peers
}
