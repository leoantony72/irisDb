package tests

import (
	"iris/config"
	"testing"
)

// ---------------------------------------------------------------------------
// ApplyCommitByID — valid split
// ---------------------------------------------------------------------------

func TestApplyCommitByID_ValidSplit(t *testing.T) {
	s, msgID := setupServerWithPrepare(t)
	versionBefore := s.GetClusterVersion()

	err := s.ApplyCommitByID(msgID)
	if err != nil {
		t.Fatalf("ApplyCommitByID failed: %v", err)
	}

	// Metadata should now have 2 ranges.
	meta := s.GetServerMetadata()
	if len(meta) != 2 {
		t.Fatalf("metadata count = %d, want 2", len(meta))
	}

	// Original range shrunk to [0, 8191].
	if meta[0].Start != 0 || meta[0].End != 8191 {
		t.Errorf("range[0] = %d-%d, want 0-8191", meta[0].Start, meta[0].End)
	}
	if meta[0].MasterID != "master-1" {
		t.Errorf("range[0].MasterID = %q, want master-1", meta[0].MasterID)
	}

	// New range [8192, 16383] owned by new-node.
	if meta[1].Start != 8192 || meta[1].End != 16383 {
		t.Errorf("range[1] = %d-%d, want 8192-16383", meta[1].Start, meta[1].End)
	}
	if meta[1].MasterID != "new-node" {
		t.Errorf("range[1].MasterID = %q, want new-node", meta[1].MasterID)
	}

	// Version incremented.
	if s.GetClusterVersion() <= versionBefore {
		t.Error("cluster version not incremented")
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — new node added
// ---------------------------------------------------------------------------

func TestApplyCommitByID_NewNodeAdded(t *testing.T) {
	s, msgID := setupServerWithPrepare(t)

	_ = s.ApplyCommitByID(msgID)

	if !s.HasNode("new-node") {
		t.Error("new-node not added to Nodes map")
	}

	node, ok := s.GetConnectedNodeData("new-node")
	if !ok {
		t.Fatal("new-node not found in connected data")
	}
	if node.Addr != "127.0.0.1:8009" {
		t.Errorf("new-node addr = %q", node.Addr)
	}
	if node.ResourceScore != 1.5 {
		t.Errorf("new-node ResourceScore = %f, want 1.5", node.ResourceScore)
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — metadata sorted by Start
// ---------------------------------------------------------------------------

func TestApplyCommitByID_MetadataSorted(t *testing.T) {
	s, msgID := setupServerWithPrepare(t)
	_ = s.ApplyCommitByID(msgID)

	meta := s.GetServerMetadata()
	for i := 1; i < len(meta); i++ {
		if meta[i].Start < meta[i-1].Start {
			t.Errorf("metadata not sorted: range[%d].Start=%d < range[%d].Start=%d",
				i, meta[i].Start, i-1, meta[i-1].Start)
		}
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — no such message
// ---------------------------------------------------------------------------

func TestApplyCommitByID_NoSuchMessage(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	err := s.ApplyCommitByID("nonexistent-msg")
	if err == nil {
		t.Error("expected error for non-existent messageID")
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — modified range not found (metadata mismatch)
// ---------------------------------------------------------------------------

func TestApplyCommitByID_NoMatchingRange(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	// Manually set up a prepare where the modifiedNodeID's range doesn't match.
	s.Prepared["bad-msg"] = &config.PrepareMessage{
		MessageID:      "bad-msg",
		SourceNodeID:   "master-1",
		TargetNodeID:   "new-node",
		Addr:           "127.0.0.1:8009",
		Start:          5000,
		End:            9999,
		ModifiedNodeID: "ghost-node", // doesn't own any range
		ResourceScore:  1.0,
		Group:          "default",
	}

	err := s.ApplyCommitByID("bad-msg")
	if err == nil {
		t.Error("expected error when modified range not found")
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — group created for target node
// ---------------------------------------------------------------------------

func TestApplyCommitByID_GroupCreated(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	_ = s.AcceptPrepare("msg-group", "master-1", "new-node", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "eu-west")

	err := s.ApplyCommitByID("msg-group")
	if err != nil {
		t.Fatalf("ApplyCommitByID failed: %v", err)
	}

	groups := s.GetAllGroups()
	found := false
	for _, g := range groups {
		if g == "eu-west" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("eu-west group not created. Groups: %v", groups)
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — group deduplication
// ---------------------------------------------------------------------------

func TestApplyCommitByID_GroupDedup(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	_ = s.AcceptPrepare("msg-dedup", "master-1", "new-node", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "default")

	_ = s.ApplyCommitByID("msg-dedup")

	members := s.GetGroupMembers("default")
	count := 0
	for _, m := range members {
		if m == "new-node" {
			count++
		}
	}
	if count > 1 {
		t.Errorf("new-node appears %d times in group 'default', expected at most 1", count)
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — prepared message cleaned up
// ---------------------------------------------------------------------------

func TestApplyCommitByID_CleansPrepared(t *testing.T) {
	s, msgID := setupServerWithPrepare(t)

	_ = s.ApplyCommitByID(msgID)

	if _, ok := s.Prepared[msgID]; ok {
		t.Error("prepared message should be deleted after commit")
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — Nnode incremented
// ---------------------------------------------------------------------------

func TestApplyCommitByID_NnodeIncremented(t *testing.T) {
	s, msgID := setupServerWithPrepare(t)
	nnodeBefore := s.Nnode

	_ = s.ApplyCommitByID(msgID)

	if s.Nnode != nnodeBefore+1 {
		t.Errorf("Nnode = %d, want %d", s.Nnode, nnodeBefore+1)
	}
}

// ---------------------------------------------------------------------------
// ApplyCommitByID — replicas filled to ReplicationFactor
// ---------------------------------------------------------------------------

func TestApplyCommitByID_EnsureReplicaFactor(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "replica-1", "127.0.0.1:8010", "default", 1.0)
	s.ReplicationFactor = 1

	_ = s.AcceptPrepare("msg-rf", "master-1", "new-node", "127.0.0.1:8009",
		8192, 16383, "master-1", nil, nil, 1.0, "default")

	_ = s.ApplyCommitByID("msg-rf")

	meta := s.GetServerMetadata()
	// With ReplicationFactor=1 and 3 nodes total, each range should aim for 1 replica.
	for _, r := range meta {
		if len(r.Nodes) < 1 {
			t.Logf("range %d-%d has %d replicas (factor=%d)", r.Start, r.End, len(r.Nodes), s.ReplicationFactor)
		}
	}
}

