package gossip

import (
	"sync"
	"time"
)

type NodeHealth int

const (
	ALIVE NodeHealth = iota
	SUSPECT
	DEAD
)

type NodeState struct {
	NodeID         string
	Group          string
	Health         NodeHealth
	LastSeen       time.Time
	SuspicionCount int
	Version        uint64
}

/*
Provides an interface for the gossip protocol to interaact with the cluster metadata,
Gossip protocol can only read the cluster state and node information through this interface, it cannot
modify it directly or access any other part of the Server struct
*/
type ClusterView interface {
	GetGroupMembers(group string) []string
	GetNodeAddr(nodeID string) (string, bool)
	GetAllGroups() []string
	GetLocalNodeID() string
	GetLocalGroup() string
}

type GossipTable map[string]NodeState

type Gossip struct {
	localID string
	Group   string
	view    ClusterView
	table   GossipTable
	mu      sync.RWMutex

	onDead func(nodeID string, evidenceCount int)
}

func NewGossip(view ClusterView, onDead func(nodeID string, evidenceCount int)) *Gossip {
	gossip := &Gossip{
		localID: view.GetLocalNodeID(),
		Group:   view.GetLocalGroup(),
		view:    view,
		table:   make(GossipTable),
		onDead:  onDead,
	}
	gossip.seedFromView()
	return gossip
}

func (g *Gossip) seedFromView() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, group_name := range g.view.GetAllGroups() {
		for _, nodeID := range g.view.GetGroupMembers(group_name) {
			if nodeID == g.localID {
				continue
			}
			g.table[nodeID] = NodeState{
				NodeID:   nodeID,
				Group:    group_name,
				Health:   ALIVE,
				LastSeen: time.Now(),
				Version:  0,
			}
		}
	}
}
