package tests

import (
	"iris/config"
	"sort"
	"testing"
)

func TestDetermineRange_PicksLowestResourceScore(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8001")
	s.ReplicationFactor = 1

	// Setup 3 nodes with different resource scores
	s.Nodes["node-1"] = &config.Node{ServerID: "node-1", ResourceScore: 10.0}
	s.Nodes["node-2"] = &config.Node{ServerID: "node-2", ResourceScore: 3.0}
	s.Nodes["node-3"] = &config.Node{ServerID: "node-3", ResourceScore: 5.0}

	// Setup 3 ranges owned by these nodes
	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 5000, MasterID: "node-1"},
		{Start: 5001, End: 10000, MasterID: "node-2"},
		{Start: 10001, End: 16383, MasterID: "node-3"},
	}

	selectedIdx, start, end, _, _ := s.DetermineRange()

	// The node with the lowest resource score is node-2 (score 3.0).
	// Its range is index 1 (5001-10000).
	if selectedIdx != 1 {
		t.Errorf("expected to select index 1 (node-2, score 3.0), got index %d", selectedIdx)
	}

	expectedMid := uint16((5001 + 10000) / 2)
	if start != expectedMid+1 || end != 10000 {
		t.Errorf("expected split range start-end to be %d-10000, got %d-%d", expectedMid+1, start, end)
	}
}

func TestDetermineRange_TieBreakerLargerRange(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8001")
	s.ReplicationFactor = 1

	// Setup 2 nodes with equal lowest resource scores
	s.Nodes["node-1"] = &config.Node{ServerID: "node-1", ResourceScore: 3.0}
	s.Nodes["node-2"] = &config.Node{ServerID: "node-2", ResourceScore: 3.0}

	// Setup 2 ranges: Range 2 is larger than Range 1
	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 3000, MasterID: "node-1"},
		{Start: 3001, End: 10000, MasterID: "node-2"},
	}

	selectedIdx, _, _, _, _ := s.DetermineRange()

	// Range 2 (index 1) is larger, so it should be selected.
	if selectedIdx != 1 {
		t.Errorf("expected to select index 1 (larger range tie-breaker), got %d", selectedIdx)
	}
}

func TestSelectReplicas_PicksHighestScores(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8001")
	s.ReplicationFactor = 2

	// Setup 4 candidates in the cluster
	s.Nodes["node-1"] = &config.Node{ServerID: "node-1", ResourceScore: 10.0}
	s.Nodes["node-2"] = &config.Node{ServerID: "node-2", ResourceScore: 3.0}
	s.Nodes["node-3"] = &config.Node{ServerID: "node-3", ResourceScore: 5.0}
	s.Nodes["node-4"] = &config.Node{ServerID: "node-4", ResourceScore: 8.0}

	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 16383, MasterID: "node-1"},
	}

	_, _, _, newReplicaServer, _ := s.DetermineRange()

	if len(newReplicaServer) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(newReplicaServer))
	}

	// Replicas should be the ones with the highest scores: node-1 (10.0) and node-4 (8.0)
	sort.Strings(newReplicaServer)
	if newReplicaServer[0] != "node-1" || newReplicaServer[1] != "node-4" {
		t.Errorf("expected replicas to be [node-1, node-4], got %v", newReplicaServer)
	}
}

func TestRepairReplication_PicksHighestScores(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8001")
	s.ReplicationFactor = 1 // 1 replica needed (needed = 1)

	s.Nodes["node-1"] = &config.Node{ServerID: "node-1", ResourceScore: 10.0}
	s.Nodes["node-2"] = &config.Node{ServerID: "node-2", ResourceScore: 3.0}
	s.Nodes["node-3"] = &config.Node{ServerID: "node-3", ResourceScore: 8.0}
	s.Nodes["node-4"] = &config.Node{ServerID: "node-4", ResourceScore: 5.0}

	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 16383, MasterID: "node-1", Nodes: []string{}},
	}

	// RepairReplication returns a mapping of new replicas added.
	// Since we mock the actual network call failures, it might return some empty map or fail,
	// but the metadata r.Nodes should still get populated or candidates sorted.
	// Actually, RepairReplication makes a network call which will fail in a unit test.
	// Let's test that candidate nodes selection logic sorts and picks highest.
	// Since RepairReplication sends TCP messages to peers, we'll see a connection failure, but wait:
	// Let's verify that the local candidates are chosen by ResourceScore.
	// Actually, look at config/repair_replication.go. If the Dial fails, it continues.
	// So we can still inspect s.Metadata[0].Nodes after calling RepairReplication.
	_ = s.RepairReplication("node-1")

	// The candidate replicas are node-2, node-3, node-4.
	// The highest score is node-3 (8.0).
	// Therefore, s.Metadata[0].Nodes should have node "node-3".
	nodes := s.Metadata[0].Nodes
	if len(nodes) != 1 {
		t.Fatalf("expected 1 replica to be assigned, got %d (%v)", len(nodes), nodes)
	}
	if nodes[0] != "node-3" {
		t.Errorf("expected repaired replica to be node-3, got %s", nodes[0])
	}
}

func TestCommit_EnsureReplica_PicksHighestScores(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8001")
	s.ReplicationFactor = 1

	s.Nodes["node-1"] = &config.Node{ServerID: "node-1", ResourceScore: 10.0}
	s.Nodes["node-2"] = &config.Node{ServerID: "node-2", ResourceScore: 3.0}
	s.Nodes["node-3"] = &config.Node{ServerID: "node-3", ResourceScore: 8.0}

	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 16383, MasterID: "node-1"},
	}

	// Prepare a commit message
	msgID := "test-msg"
	s.Prepared[msgID] = &config.PrepareMessage{
		MessageID:               msgID,
		TargetNodeID:            "node-new",
		Addr:                    "127.0.0.1:8002",
		Start:                   8192,
		End:                     16383,
		ModifiedNodeID:          "node-1",
		ModifiedNodeReplicaList: []string{},
		TargetNodeReplicaList:   []string{},
		ResourceScore:           6.0,
	}

	err := s.ApplyCommitByID(msgID)
	if err != nil {
		t.Fatalf("ApplyCommitByID failed: %v", err)
	}

	// After commit, we have 2 ranges:
	// Range 0: 0-8191, Master: node-1
	// Range 1: 8192-16383, Master: node-new
	// Each should have 1 replica selected from s.Nodes (which contains node-1, node-2, node-3, node-new).
	// For Range 0: Master is node-1. Candidate is node-3 (score 8.0) and node-2 (score 3.0). node-3 should be selected.
	// For Range 1: Master is node-new. Candidates are node-1 (10.0), node-3 (8.0). node-1 should be selected.
	
	if len(s.Metadata) != 2 {
		t.Fatalf("expected 2 metadata ranges, got %d", len(s.Metadata))
	}

	r0 := s.Metadata[0]
	if len(r0.Nodes) != 1 || r0.Nodes[0] != "node-3" {
		t.Errorf("expected range 0 to have replica node-3, got %v", r0.Nodes)
	}

	r1 := s.Metadata[1]
	if len(r1.Nodes) != 1 || r1.Nodes[0] != "node-1" {
		t.Errorf("expected range 1 to have replica node-1, got %v", r1.Nodes)
	}
}
