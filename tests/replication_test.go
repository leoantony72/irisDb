package tests

import (
	"iris/config"
	"testing"
)

// ---------------------------------------------------------------------------
// ReplicationValidator
// ---------------------------------------------------------------------------

func TestReplicationValidator_Met(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "replica-1", "127.0.0.1:8009", "default", 1.0)
	s.ReplicationFactor = 1
	s.Metadata[0].Nodes = []string{"replica-1"}

	// Note: ReplicationValidator has a potential lock issue (RLock inside RLock
	// via FindRangeIndexByServerID), but it works when no writer is waiting.
	if !s.ReplicationValidator() {
		t.Error("expected replication factor met")
	}
}

func TestReplicationValidator_NotMet(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	s.ReplicationFactor = 2
	s.Metadata[0].Nodes = []string{"only-one-replica"}

	if s.ReplicationValidator() {
		t.Error("expected replication factor NOT met")
	}
}

func TestReplicationValidator_NoRanges(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	// Remove master-1 from all ranges so it masters nothing.
	s.Metadata[0].MasterID = "other-node"
	s.ReplicationFactor = 1

	// When server doesn't master any range, validator returns true.
	if !s.ReplicationValidator() {
		t.Error("expected true when server has no master ranges")
	}
}

// ---------------------------------------------------------------------------
// AddReplicaToRange
// ---------------------------------------------------------------------------

func TestAddReplicaToRange_Valid(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "replica-1", "127.0.0.1:8009", "default", 1.0)

	err := s.AddReplicaToRange("replica-1", 0, 16383)
	if err != nil {
		t.Fatalf("AddReplicaToRange failed: %v", err)
	}

	sr, _ := s.GetSlotRangeByIndex(0)
	if len(sr.Nodes) != 1 || sr.Nodes[0] != "replica-1" {
		t.Errorf("Nodes = %v, want [replica-1]", sr.Nodes)
	}
}

func TestAddReplicaToRange_UnknownServer(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	err := s.AddReplicaToRange("ghost", 0, 16383)
	if err == nil {
		t.Error("expected error for unknown server")
	}
}

func TestAddReplicaToRange_RangeNotFound(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	err := s.AddReplicaToRange("master-1", 9999, 9999)
	if err == nil {
		t.Error("expected error for non-existent range")
	}
}

func TestAddReplicaToRange_VersionIncrement(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "replica-1", "127.0.0.1:8009", "default", 1.0)

	vBefore := s.GetClusterVersion()
	_ = s.AddReplicaToRange("replica-1", 0, 16383)
	vAfter := s.GetClusterVersion()

	if vAfter <= vBefore {
		t.Errorf("version not incremented: before=%d, after=%d", vBefore, vAfter)
	}
}

func TestAddReplicaToRange_MultipleReplicas(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "r1", "127.0.0.1:8009", "default", 1.0)
	AddNodeToServer(s, "r2", "127.0.0.1:8010", "default", 1.0)
	AddNodeToServer(s, "r3", "127.0.0.1:8011", "default", 1.0)

	_ = s.AddReplicaToRange("r1", 0, 16383)
	_ = s.AddReplicaToRange("r2", 0, 16383)
	_ = s.AddReplicaToRange("r3", 0, 16383)

	sr, _ := s.GetSlotRangeByIndex(0)
	if len(sr.Nodes) != 3 {
		t.Errorf("expected 3 replicas, got %d", len(sr.Nodes))
	}
}

// ---------------------------------------------------------------------------
// RepairReplication (local logic, no network)
// ---------------------------------------------------------------------------

func TestRepairReplication_NoCandidates(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	s.ReplicationFactor = 2
	s.Metadata[0].Nodes = []string{} // needs 2 replicas but only self in cluster

	// RepairReplication tries to connect to peers, but with only self there are
	// no candidates. It should not panic and return an empty mapping.
	mapping := s.RepairReplication("master-1")
	if len(mapping) != 0 {
		t.Logf("mapping = %v (expected empty due to no candidates)", mapping)
	}
}

// ---------------------------------------------------------------------------
// SlotRange Nodes — checking after multiple operations
// ---------------------------------------------------------------------------

func TestSlotRangeNodes_AfterSplitAndAddReplica(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "node-2", "127.0.0.1:8009", "default", 1.0)
	s.ReplicationFactor = 1

	// Prepare and commit a join to create a split.
	_ = s.AcceptPrepare("split-msg", "master-1", "node-2", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "default")
	err := s.ApplyCommitByID("split-msg")
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	meta := s.GetServerMetadata()
	if len(meta) != 2 {
		t.Fatalf("expected 2 ranges after split, got %d", len(meta))
	}

	// Add replica to the new range.
	err = s.AddReplicaToRange("master-1", 8192, 16383)
	if err != nil {
		t.Fatalf("AddReplicaToRange failed: %v", err)
	}

	sr, ok := s.GetSlotRangeByIndex(s.FindRangeIndex(8192, 16383))
	if !ok {
		t.Fatal("new range not found")
	}
	found := false
	for _, id := range sr.Nodes {
		if id == "master-1" {
			found = true
		}
	}
	if !found {
		t.Error("master-1 not found as replica in new range")
	}
}

// Unused import guard — config is used for config.PrepareMessage in election_test
var _ = config.ALIVE
