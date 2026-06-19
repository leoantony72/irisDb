package tests

import (
	"iris/config"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Build → Apply snapshot round-trip
// ---------------------------------------------------------------------------

func TestBuildAndApplySnapshot_Roundtrip(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	AddNodeToServer(s, "node-2", "127.0.0.1:8009", "default", 2.0)
	s.Metadata[0].Nodes = []string{"node-2"}
	s.Cluster_Version = 10

	snap := s.BuildClusterSnapshot()

	// Apply to a fresh server.
	target := NewTestServer("node-3", "127.0.0.1:8010")
	target.ApplyClusterSnapshot(snap)

	if target.GetClusterVersion() != 10 {
		t.Errorf("version = %d, want 10", target.GetClusterVersion())
	}
	if target.GetNodeCount() != 2 {
		t.Errorf("node count = %d, want 2", target.GetNodeCount())
	}
	if target.MasterNodeID != "node-1" {
		t.Errorf("MasterNodeID = %q, want node-1", target.MasterNodeID)
	}

	meta := target.GetServerMetadata()
	if len(meta) != 1 {
		t.Fatalf("metadata len = %d, want 1", len(meta))
	}
	if meta[0].MasterID != "node-1" {
		t.Errorf("range MasterID = %q", meta[0].MasterID)
	}
}

// ---------------------------------------------------------------------------
// ApplySnapshot — overwrites old state
// ---------------------------------------------------------------------------

func TestApplySnapshot_OverwritesOldState(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	AddNodeToServer(s, "old-node", "127.0.0.1:9999", "default", 0.5)

	snap := config.ClusterSnapshot{
		ClusterVersion: 99,
		TotalNodes:     1,
		TotalSlots:     16384,
		Nodes: []config.Node{
			{ServerID: "fresh-node", Addr: "10.0.0.1:8008", Group: "new-group"},
		},
		Metadata: []config.SlotRange{
			{Start: 0, End: 16383, MasterID: "fresh-node"},
		},
		MasterNodeID: "fresh-node",
	}

	s.ApplyClusterSnapshot(snap)

	if s.HasNode("old-node") {
		t.Error("old-node should have been replaced")
	}
	if !s.HasNode("fresh-node") {
		t.Error("fresh-node should be present")
	}
	if s.GetClusterVersion() != 99 {
		t.Errorf("version = %d, want 99", s.GetClusterVersion())
	}
}

// ---------------------------------------------------------------------------
// ApplySnapshot — rebuilds groups from node Group fields
// ---------------------------------------------------------------------------

func TestApplySnapshot_RebuildGroups(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	snap := config.ClusterSnapshot{
		ClusterVersion: 5,
		TotalNodes:     3,
		TotalSlots:     16384,
		Nodes: []config.Node{
			{ServerID: "a", Addr: "1.2.3.4:8008", Group: "us-east"},
			{ServerID: "b", Addr: "1.2.3.5:8008", Group: "eu-west"},
			{ServerID: "c", Addr: "1.2.3.6:8008", Group: "us-east"},
		},
		Metadata: []config.SlotRange{
			{Start: 0, End: 16383, MasterID: "a"},
		},
		MasterNodeID: "a",
	}

	s.ApplyClusterSnapshot(snap)

	groups := s.GetAllGroups()
	if len(groups) != 2 {
		t.Errorf("expected 2 groups, got %d: %v", len(groups), groups)
	}

	usEast := s.GetGroupMembers("us-east")
	if len(usEast) != 2 {
		t.Errorf("us-east members = %v, want [a, c]", usEast)
	}

	euWest := s.GetGroupMembers("eu-west")
	if len(euWest) != 1 || euWest[0] != "b" {
		t.Errorf("eu-west members = %v, want [b]", euWest)
	}
}

// ---------------------------------------------------------------------------
// ApplySnapshot — master change clears SuspectLeaderMsg
// ---------------------------------------------------------------------------

func TestApplySnapshot_MasterChange(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.SuspectLeaderMsg["suspect-1"] = time.Now()
	s.SuspectLeaderMsg["suspect-2"] = time.Now()

	snap := config.ClusterSnapshot{
		ClusterVersion: 5,
		TotalNodes:     1,
		TotalSlots:     16384,
		Nodes: []config.Node{
			{ServerID: "new-master", Addr: "10.0.0.1:8008", Group: "default"},
		},
		Metadata: []config.SlotRange{
			{Start: 0, End: 16383, MasterID: "new-master"},
		},
		MasterNodeID: "new-master", // different from current "node-1"
	}

	s.ApplyClusterSnapshot(snap)

	if len(s.SuspectLeaderMsg) != 0 {
		t.Errorf("SuspectLeaderMsg should be cleared on master change, got %d entries", len(s.SuspectLeaderMsg))
	}
}

// ---------------------------------------------------------------------------
// ApplySnapshot — empty cluster
// ---------------------------------------------------------------------------

func TestSnapshot_EmptyCluster(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	snap := config.ClusterSnapshot{
		ClusterVersion: 1,
		TotalNodes:     0,
		TotalSlots:     16384,
		Nodes:          []config.Node{},
		Metadata:       []config.SlotRange{},
		MasterNodeID:   "",
	}

	s.ApplyClusterSnapshot(snap)

	if s.GetNodeCount() != 0 {
		t.Errorf("node count = %d, want 0", s.GetNodeCount())
	}
	if s.GetSlotRangeCount() != 0 {
		t.Errorf("slot range count = %d, want 0", s.GetSlotRangeCount())
	}
}

// ---------------------------------------------------------------------------
// ApplySnapshot — nodes with empty group default to "default"
// ---------------------------------------------------------------------------

func TestApplySnapshot_DefaultGroup(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	snap := config.ClusterSnapshot{
		ClusterVersion: 1,
		TotalNodes:     1,
		TotalSlots:     16384,
		Nodes: []config.Node{
			{ServerID: "orphan", Addr: "1.2.3.4:8008", Group: ""}, // empty group
		},
		Metadata: []config.SlotRange{
			{Start: 0, End: 16383, MasterID: "orphan"},
		},
		MasterNodeID: "orphan",
	}

	s.ApplyClusterSnapshot(snap)

	members := s.GetGroupMembers("default")
	if len(members) != 1 || members[0] != "orphan" {
		t.Errorf("expected orphan in 'default' group, got %v", members)
	}
}
