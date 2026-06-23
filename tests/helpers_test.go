package tests

import (
	"fmt"
	"iris/config"
	"iris/engine"
	"iris/gossip"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Server helpers
// ---------------------------------------------------------------------------

// NewTestServer creates a config.Server with sensible defaults.
// No real ports are bound; the server is ready for in-memory unit testing.
func NewTestServer(id, addr string) *config.Server {
	s := &config.Server{
		ServerID:              id,
		Host:                  "127.0.0.1",
		Addr:                  addr,
		Port:                  "8008",
		N:                     16384,
		Nnode:                 1,
		Nodes:                 make(map[string]*config.Node),
		Metadata:              []*config.SlotRange{},
		ReplicationFactor:     1,
		Cluster_Version:       1,
		BusPort:               "18008",
		Prepared:              make(map[string]*config.PrepareMessage),
		MasterNodeID:          id,
		LastSeen:              make(map[string]time.Time),
		Group:                 make(map[string]*config.GroupInfo),
		UnreahableNodes:       make(map[string]time.Time),
		SuspectLeaderMsg:      make(map[string]time.Time),
		Votes:                 make(map[string]bool),
		MASTER_FAIL_THRESHOLD: 3,
	}

	s.Nodes[id] = &config.Node{
		ServerID:      id,
		Addr:          addr,
		Status:        config.ALIVE,
		Group:         "default",
		ResourceScore: 1.0,
	}

	s.Group["default"] = &config.GroupInfo{
		Name:   "default",
		Nodes:  []string{id},
		Status: config.HEALTHY,
	}

	// Single full range covering all slots.
	s.Metadata = append(s.Metadata, &config.SlotRange{
		Start:    0,
		End:      16383,
		MasterID: id,
		Nodes:    []string{},
	})

	return s
}

// AddNodeToServer registers a new node into an existing test server.
func AddNodeToServer(s *config.Server, id, addr, group string, score float64) {
	s.Nodes[id] = &config.Node{
		ServerID:      id,
		Addr:          addr,
		Status:        config.ALIVE,
		Group:         group,
		ResourceScore: score,
	}
	s.Nnode++

	gi, ok := s.Group[group]
	if !ok {
		gi = &config.GroupInfo{Name: group, Nodes: []string{}, Status: config.HEALTHY}
		s.Group[group] = gi
	}
	gi.Nodes = append(gi.Nodes, id)
}

// ---------------------------------------------------------------------------
// Engine helpers
// ---------------------------------------------------------------------------

// NewTestEngine creates a Pebble-backed Engine in a temporary directory.
// The engine is automatically closed when the test finishes.
func NewTestEngine(t *testing.T) *engine.Engine {
	t.Helper()
	dir := t.TempDir()
	e, err := engine.NewEngine(dir)
	if err != nil {
		t.Fatalf("failed to create test engine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// ---------------------------------------------------------------------------
// Mock ClusterView (for gossip tests)
// ---------------------------------------------------------------------------

// MockClusterView implements gossip.ClusterView for unit testing.
type MockClusterView struct {
	Groups     map[string][]string // group name → node IDs
	Addrs      map[string]string   // node ID → addr
	Scores     map[string]float64  // node ID → ResourceScore
	LocalID    string
	LocalGroup string
}

func (m *MockClusterView) GetGroupMembers(group string) []string {
	return m.Groups[group]
}

func (m *MockClusterView) GetNodeAddr(nodeID string) (string, bool) {
	addr, ok := m.Addrs[nodeID]
	return addr, ok
}

func (m *MockClusterView) GetAllGroups() []string {
	out := make([]string, 0, len(m.Groups))
	for g := range m.Groups {
		out = append(out, g)
	}
	return out
}

func (m *MockClusterView) GetLocalNodeID() string { return m.LocalID }
func (m *MockClusterView) GetLocalGroup() string  { return m.LocalGroup }

// GetNodeResourceScore returns a pre-configured score or 0 if not set.
func (m *MockClusterView) GetNodeResourceScore(nodeID string) float64 {
	if m.Scores != nil {
		if score, ok := m.Scores[nodeID]; ok {
			return score
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// Gossip helpers
// ---------------------------------------------------------------------------

// newGossipForTest creates a real gossip.Gossip from a MockClusterView.
// Useful for tests that need gossip channels (DeadEvents, JoinEvents, etc).
func newGossipForTest(view *MockClusterView) *gossip.Gossip {
	return gossip.NewGossip(view)
}

// ---------------------------------------------------------------------------
// Mock AntiEntropyDB (for anti-entropy tests)
// ---------------------------------------------------------------------------

type MockAntiEntropyDB struct {
	Digests         map[string]config.RangeDigest // "start-end" → digest
	TransferCalls   []TransferCall
}

type TransferCall struct {
	ReplicaID string
	Start     uint16
	End       uint16
}

func NewMockAntiEntropyDB() *MockAntiEntropyDB {
	return &MockAntiEntropyDB{
		Digests: make(map[string]config.RangeDigest),
	}
}

func (m *MockAntiEntropyDB) ComputeRangeDigest(start, end, totalSlots uint16) config.RangeDigest {
	key := fmt.Sprintf("%d-%d", start, end)
	if d, ok := m.Digests[key]; ok {
		return d
	}
	return config.RangeDigest{Start: start, End: end}
}

func (m *MockAntiEntropyDB) InitiateDataTransferToReplica(replicaID string, start, end uint16) {
	m.TransferCalls = append(m.TransferCalls, TransferCall{replicaID, start, end})
}
