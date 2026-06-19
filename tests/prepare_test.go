package tests

import (
	"iris/config"
	"testing"
)

// ---------------------------------------------------------------------------
// AcceptPrepare — valid
// ---------------------------------------------------------------------------

func TestAcceptPrepare_Valid(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	err := s.AcceptPrepare(
		"msg-001",           // messageID
		"master-1",          // sourceNodeID
		"new-node",          // targetNodeID
		"127.0.0.1:8009",    // targetNodeAddr
		8192, 16383,         // start, end
		"master-1",          // modifiedNodeID (current owner)
		[]string{"replica1"}, // modifiedReplicas
		[]string{"replica2"}, // targetReplicas
		1.5,                 // resourceScore
		"default",           // group
	)
	if err != nil {
		t.Fatalf("AcceptPrepare failed: %v", err)
	}

	// Verify it was stored.
	if _, ok := s.Prepared["msg-001"]; !ok {
		t.Error("prepared message not stored")
	}
}

// ---------------------------------------------------------------------------
// AcceptPrepare — duplicate messageID
// ---------------------------------------------------------------------------

func TestAcceptPrepare_DuplicateMessageID(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	_ = s.AcceptPrepare("msg-001", "master-1", "new-1", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "default")

	err := s.AcceptPrepare("msg-001", "master-1", "new-2", "127.0.0.1:8010",
		8192, 16383, "master-1", nil, nil, 1.0, "default")

	if err == nil {
		t.Error("expected error for duplicate messageID")
	}
}

// ---------------------------------------------------------------------------
// AcceptPrepare — modifiedNodeID doesn't own range
// ---------------------------------------------------------------------------

func TestAcceptPrepare_InvalidRange(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	// "ghost-node" does not own any slot range.
	err := s.AcceptPrepare("msg-002", "master-1", "new-node", "127.0.0.1:8009",
		8192, 16383, "ghost-node", nil, nil, 1.0, "default")

	if err == nil {
		t.Error("expected error when modifiedNodeID doesn't own the range")
	}
}

// ---------------------------------------------------------------------------
// AcceptPrepare — replica lists are deep-copied
// ---------------------------------------------------------------------------

func TestAcceptPrepare_ReplicaListsCopied(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	modReplicas := []string{"r1", "r2"}
	tgtReplicas := []string{"r3"}

	_ = s.AcceptPrepare("msg-003", "master-1", "new-node", "127.0.0.1:8009",
		8192, 16383, "master-1", modReplicas, tgtReplicas, 1.0, "default")

	// Mutate the original slices.
	modReplicas[0] = "MUTATED"
	tgtReplicas[0] = "MUTATED"

	pm := s.Prepared["msg-003"]
	if pm.ModifiedNodeReplicaList[0] == "MUTATED" {
		t.Error("modifiedReplicas not deep-copied")
	}
	if pm.TargetNodeReplicaList[0] == "MUTATED" {
		t.Error("targetReplicas not deep-copied")
	}
}

// ---------------------------------------------------------------------------
// AddLocalPrepare sets SourceNodeID to self
// ---------------------------------------------------------------------------

func TestAddLocalPrepare_SetsSourceNode(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	err := s.AddLocalPrepare("msg-004", "new-node", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "default")
	if err != nil {
		t.Fatalf("AddLocalPrepare failed: %v", err)
	}

	pm := s.Prepared["msg-004"]
	if pm.SourceNodeID != "master-1" {
		t.Errorf("SourceNodeID = %q, want %q", pm.SourceNodeID, "master-1")
	}
}

// ---------------------------------------------------------------------------
// DeletePrepared
// ---------------------------------------------------------------------------

func TestDeletePrepared(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	_ = s.AcceptPrepare("msg-005", "master-1", "new-node", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "default")

	s.DeletePrepared("msg-005")

	if _, ok := s.Prepared["msg-005"]; ok {
		t.Error("expected prepared message to be deleted")
	}
}

// ---------------------------------------------------------------------------
// AcceptPrepare — multiple simultaneous prepares
// ---------------------------------------------------------------------------

func TestAcceptPrepare_MultiplePrepares(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	err1 := s.AcceptPrepare("msg-A", "master-1", "nodeA", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "default")
	err2 := s.AcceptPrepare("msg-B", "master-1", "nodeB", "127.0.0.1:8010",
		8192, 16383, "master-1", nil, nil, 1.0, "default")

	if err1 != nil || err2 != nil {
		t.Errorf("expected both to succeed: err1=%v, err2=%v", err1, err2)
	}
	if len(s.Prepared) != 2 {
		t.Errorf("Prepared count = %d, want 2", len(s.Prepared))
	}
}

// ---------------------------------------------------------------------------
// AcceptPrepare — nil metadata entries handled
// ---------------------------------------------------------------------------

func TestAcceptPrepare_NilMetadataEntry(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	// Insert a nil entry in metadata.
	s.Metadata = append(s.Metadata, nil)

	// Should still work because the valid range is checked, nil entries skipped.
	err := s.AcceptPrepare("msg-006", "master-1", "new-node", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "default")
	if err != nil {
		t.Fatalf("AcceptPrepare with nil metadata entry failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// AcceptPrepare — verifies stored fields
// ---------------------------------------------------------------------------

func TestAcceptPrepare_StoredFields(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	_ = s.AcceptPrepare("msg-007", "source-1", "target-1", "127.0.0.1:8009",
		8192, 16383, "master-1", []string{"r1"}, []string{"r2"}, 2.5, "us-east")

	pm := s.Prepared["msg-007"]
	checks := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"MessageID", pm.MessageID, "msg-007"},
		{"SourceNodeID", pm.SourceNodeID, "source-1"},
		{"TargetNodeID", pm.TargetNodeID, "target-1"},
		{"Addr", pm.Addr, "127.0.0.1:8009"},
		{"Start", pm.Start, uint16(8192)},
		{"End", pm.End, uint16(16383)},
		{"ModifiedNodeID", pm.ModifiedNodeID, "master-1"},
		{"Group", pm.Group, "us-east"},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	if pm.ResourceScore != 2.5 {
		t.Errorf("ResourceScore = %f, want 2.5", pm.ResourceScore)
	}
	if len(pm.ModifiedNodeReplicaList) != 1 || pm.ModifiedNodeReplicaList[0] != "r1" {
		t.Errorf("ModifiedNodeReplicaList = %v", pm.ModifiedNodeReplicaList)
	}
	if len(pm.TargetNodeReplicaList) != 1 || pm.TargetNodeReplicaList[0] != "r2" {
		t.Errorf("TargetNodeReplicaList = %v", pm.TargetNodeReplicaList)
	}
}

// ---------------------------------------------------------------------------
// Helper: set up a server with a pending prepare for commit tests
// ---------------------------------------------------------------------------

func setupServerWithPrepare(t *testing.T) (*config.Server, string) {
	t.Helper()
	s := NewTestServer("master-1", "127.0.0.1:8008")
	s.ReplicationFactor = 1

	msgID := "commit-msg-001"
	err := s.AcceptPrepare(msgID, "master-1", "new-node", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.5, "default")
	if err != nil {
		t.Fatalf("setup AcceptPrepare failed: %v", err)
	}
	return s, msgID
}
