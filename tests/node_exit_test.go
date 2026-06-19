package tests

import (
	"iris/config"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// NodeExit — promotes first replica to master
// ---------------------------------------------------------------------------

func TestNodeExit_PromotesReplica(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "replica-1", "127.0.0.1:8009", "default", 1.0)
	AddNodeToServer(s, "replica-2", "127.0.0.1:8010", "default", 1.0)

	// Set replica-1 and replica-2 as replicas for the range.
	s.Metadata[0].Nodes = []string{"replica-1", "replica-2"}

	// Add a second range also mastered by master-1.
	s.Metadata = append(s.Metadata, &config.SlotRange{
		Start:    0, // overlapping for test simplicity
		End:      0,
		MasterID: "replica-1",
		Nodes:    []string{"master-1"},
	})

	err := s.NodeExit("master-1")
	if err != nil {
		t.Fatalf("NodeExit failed: %v", err)
	}

	// The first range's master should now be replica-1 (first replica).
	meta := s.GetServerMetadata()
	if meta[0].MasterID != "replica-1" {
		t.Errorf("range[0].MasterID = %q, want replica-1", meta[0].MasterID)
	}

	// Remaining replicas should be just replica-2.
	if len(meta[0].Nodes) != 1 || meta[0].Nodes[0] != "replica-2" {
		t.Errorf("range[0].Nodes = %v, want [replica-2]", meta[0].Nodes)
	}
}

// ---------------------------------------------------------------------------
// NodeExit — removes from all replica lists
// ---------------------------------------------------------------------------

func TestNodeExit_RemovesFromAllReplicas(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "node-2", "127.0.0.1:8009", "default", 1.0)
	AddNodeToServer(s, "node-3", "127.0.0.1:8010", "default", 1.0)

	// node-2 is a replica of master-1's range.
	s.Metadata[0].Nodes = []string{"node-2", "node-3"}

	// Add a range mastered by node-3 with node-2 as replica.
	s.Metadata = append(s.Metadata, &config.SlotRange{
		Start:    0,
		End:      0,
		MasterID: "node-3",
		Nodes:    []string{"node-2", "master-1"},
	})

	err := s.NodeExit("node-2")
	if err != nil {
		t.Fatalf("NodeExit failed: %v", err)
	}

	// node-2 should be removed from all replica lists.
	meta := s.GetServerMetadata()
	for i, r := range meta {
		for _, id := range r.Nodes {
			if id == "node-2" {
				t.Errorf("node-2 still present in range[%d] replicas: %v", i, r.Nodes)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// NodeExit — removes from Nodes map
// ---------------------------------------------------------------------------

func TestNodeExit_RemovesFromNodesMap(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "victim", "127.0.0.1:8009", "default", 1.0)

	// Give victim a range with a replica so exit works.
	s.Metadata = append(s.Metadata, &config.SlotRange{
		Start:    0,
		End:      0,
		MasterID: "victim",
		Nodes:    []string{"master-1"},
	})

	_ = s.NodeExit("victim")

	if s.HasNode("victim") {
		t.Error("victim should be removed from Nodes map")
	}
}

// ---------------------------------------------------------------------------
// NodeExit — removes from group
// ---------------------------------------------------------------------------

func TestNodeExit_RemovesFromGroup(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "victim", "127.0.0.1:8009", "default", 1.0)

	s.Metadata = append(s.Metadata, &config.SlotRange{
		Start: 0, End: 0, MasterID: "victim",
		Nodes: []string{"master-1"},
	})

	_ = s.NodeExit("victim")

	members := s.GetGroupMembers("default")
	for _, m := range members {
		if m == "victim" {
			t.Error("victim still in group 'default'")
		}
	}
}

// ---------------------------------------------------------------------------
// NodeExit — updates Nnode
// ---------------------------------------------------------------------------

func TestNodeExit_UpdatesNnode(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "victim", "127.0.0.1:8009", "default", 1.0)

	s.Metadata = append(s.Metadata, &config.SlotRange{
		Start: 0, End: 0, MasterID: "victim",
		Nodes: []string{"master-1"},
	})

	_ = s.NodeExit("victim")

	if s.Nnode != 1 {
		t.Errorf("Nnode = %d, want 1", s.Nnode)
	}
}

// ---------------------------------------------------------------------------
// NodeExit — increments version
// ---------------------------------------------------------------------------

func TestNodeExit_IncrementsVersion(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "victim", "127.0.0.1:8009", "default", 1.0)

	s.Metadata = append(s.Metadata, &config.SlotRange{
		Start: 0, End: 0, MasterID: "victim",
		Nodes: []string{"master-1"},
	})

	vBefore := s.GetClusterVersion()
	_ = s.NodeExit("victim")
	vAfter := s.GetClusterVersion()

	if vAfter <= vBefore {
		t.Errorf("version not incremented: before=%d, after=%d", vBefore, vAfter)
	}
}

// ---------------------------------------------------------------------------
// NodeExit — non-existent node
// ---------------------------------------------------------------------------

func TestNodeExit_NonExistentNode(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	err := s.NodeExit("ghost-node")
	if err == nil {
		t.Error("expected error for non-existent node")
	}
}

// ---------------------------------------------------------------------------
// NodeExit — no replicas to promote
// ---------------------------------------------------------------------------

func TestNodeExit_NoReplicas(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	// master-1 owns range [0, 16383] with NO replicas (empty Nodes slice).

	err := s.NodeExit("master-1")
	if err == nil {
		t.Error("expected error when no replicas available to promote")
	}
}

// ---------------------------------------------------------------------------
// NodeExit — gossip DeadEvents channel receives the nodeID
// ---------------------------------------------------------------------------

func TestNodeExit_GossipDeadEvent(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "victim", "127.0.0.1:8009", "default", 1.0)

	// Set up gossip with a mock view.
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"master-1", "victim"}},
		Addrs:      map[string]string{"master-1": "127.0.0.1:8008", "victim": "127.0.0.1:8009"},
		LocalID:    "master-1",
		LocalGroup: "default",
	}

	// We import gossip only for the NewGossip call. Use the real one.
	g := newGossipForTest(view)
	s.Gossip = g

	// Give victim a range it can exit from.
	s.Metadata = append(s.Metadata, &config.SlotRange{
		Start: 0, End: 0, MasterID: "victim",
		Nodes: []string{"master-1"},
	})

	_ = s.NodeExit("victim")

	// Check DeadEvents channel.
	select {
	case id := <-g.DeadEvents:
		if id != "victim" {
			t.Errorf("DeadEvents received %q, want %q", id, "victim")
		}
	case <-time.After(time.Second):
		t.Error("DeadEvents channel did not receive nodeID within timeout")
	}
}

// ---------------------------------------------------------------------------
// NodeExit — node mastering multiple ranges
// ---------------------------------------------------------------------------

func TestNodeExit_MultipleRanges(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "victim", "127.0.0.1:8009", "default", 1.0)
	AddNodeToServer(s, "replica-1", "127.0.0.1:8010", "default", 1.0)

	// victim masters two ranges.
	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 5000, MasterID: "master-1", Nodes: []string{"victim"}},
		{Start: 5001, End: 10000, MasterID: "victim", Nodes: []string{"replica-1"}},
		{Start: 10001, End: 16383, MasterID: "victim", Nodes: []string{"master-1"}},
	}

	err := s.NodeExit("victim")
	if err != nil {
		t.Fatalf("NodeExit failed: %v", err)
	}

	meta := s.GetServerMetadata()
	// Both ranges previously mastered by victim should have new masters.
	for _, r := range meta {
		if r.MasterID == "victim" {
			t.Errorf("range %d-%d still mastered by victim", r.Start, r.End)
		}
	}

	// victim should not appear in any replica list.
	for _, r := range meta {
		for _, id := range r.Nodes {
			if id == "victim" {
				t.Errorf("victim still in replica list for range %d-%d", r.Start, r.End)
			}
		}
	}
}
