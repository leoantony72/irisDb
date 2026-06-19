package tests

import (
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Basic getters
// ---------------------------------------------------------------------------

func TestGetClusterVersion(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Cluster_Version = 42

	if got := s.GetClusterVersion(); got != 42 {
		t.Errorf("GetClusterVersion() = %d, want 42", got)
	}
}

func TestGetNodeCount(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	if got := s.GetNodeCount(); got != 1 {
		t.Errorf("GetNodeCount() = %d, want 1", got)
	}

	AddNodeToServer(s, "node-2", "127.0.0.1:8009", "default", 1.0)
	if got := s.GetNodeCount(); got != 2 {
		t.Errorf("GetNodeCount() after add = %d, want 2", got)
	}
}

func TestGetSlotRangeCount(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	if got := s.GetSlotRangeCount(); got != 1 {
		t.Errorf("GetSlotRangeCount() = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Master failed attempts
// ---------------------------------------------------------------------------

func TestMasterFailedAttempts_IncrResetGet(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	if got := s.GetrMasterFailedAttempts(); got != 0 {
		t.Fatalf("initial = %d, want 0", got)
	}

	s.IncrMasterFailedAttempts()
	s.IncrMasterFailedAttempts()
	if got := s.GetrMasterFailedAttempts(); got != 2 {
		t.Fatalf("after 2 increments = %d, want 2", got)
	}

	s.ResetMasterFailedAttempts()
	if got := s.GetrMasterFailedAttempts(); got != 0 {
		t.Fatalf("after reset = %d, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// UpdateMasterNodeID
// ---------------------------------------------------------------------------

func TestUpdateMasterNodeID(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.UpdateMasterNodeID("node-99")

	if s.MasterNodeID != "node-99" {
		t.Errorf("MasterNodeID = %q, want %q", s.MasterNodeID, "node-99")
	}
}

// ---------------------------------------------------------------------------
// GetBasicInfo
// ---------------------------------------------------------------------------

func TestGetBasicInfo(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Cluster_Version = 5

	sid, host, addr, busPort, version, totalNodes, totalSlots := s.GetBasicInfo()
	if sid != "node-1" {
		t.Errorf("ServerID = %q", sid)
	}
	if host != "127.0.0.1" {
		t.Errorf("Host = %q", host)
	}
	if addr != "127.0.0.1:8008" {
		t.Errorf("Addr = %q", addr)
	}
	if busPort != "18008" {
		t.Errorf("BusPort = %q", busPort)
	}
	if version != 5 {
		t.Errorf("Version = %d", version)
	}
	if totalNodes != 1 {
		t.Errorf("TotalNodes = %d", totalNodes)
	}
	if totalSlots != 16384 {
		t.Errorf("TotalSlots = %d", totalSlots)
	}
}

// ---------------------------------------------------------------------------
// GetNodesSnapshot (deep copy)
// ---------------------------------------------------------------------------

func TestGetNodesSnapshot_DeepCopy(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	AddNodeToServer(s, "node-2", "127.0.0.1:8009", "default", 2.0)

	snap := s.GetNodesSnapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(snap))
	}

	// Mutating snapshot must not affect the server.
	for i := range snap {
		snap[i].Addr = "MUTATED"
	}

	n, ok := s.GetConnectedNodeData("node-2")
	if !ok {
		t.Fatal("node-2 not found")
	}
	if n.Addr == "MUTATED" {
		t.Error("mutation of snapshot leaked into server")
	}
}

// ---------------------------------------------------------------------------
// GetCommitPeers excludes self
// ---------------------------------------------------------------------------

func TestGetCommitPeers_ExcludesSelf(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	AddNodeToServer(s, "node-2", "127.0.0.1:8009", "default", 1.0)
	AddNodeToServer(s, "node-3", "127.0.0.1:8010", "default", 1.0)

	peers := s.GetCommitPeers()
	for _, p := range peers {
		if p.ServerID == "node-1" {
			t.Error("self should not appear in commit peers")
		}
	}
	if len(peers) != 2 {
		t.Errorf("len(peers) = %d, want 2", len(peers))
	}
}

// ---------------------------------------------------------------------------
// HasNode
// ---------------------------------------------------------------------------

func TestHasNode(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	if !s.HasNode("node-1") {
		t.Error("HasNode(node-1) = false, want true")
	}
	if s.HasNode("nonexistent") {
		t.Error("HasNode(nonexistent) = true, want false")
	}
}

// ---------------------------------------------------------------------------
// GetConnectedNodeData returns a copy
// ---------------------------------------------------------------------------

func TestGetConnectedNodeData_ReturnsCopy(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	n, ok := s.GetConnectedNodeData("node-1")
	if !ok {
		t.Fatal("node-1 not found")
	}

	n.Addr = "MUTATED"
	original, _ := s.GetConnectedNodeData("node-1")
	if original.Addr == "MUTATED" {
		t.Error("GetConnectedNodeData returned a reference, not a copy")
	}
}

func TestGetConnectedNodeData_NotFound(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	_, ok := s.GetConnectedNodeData("ghost")
	if ok {
		t.Error("expected not found for ghost node")
	}
}

// ---------------------------------------------------------------------------
// GetServerIDFromAddr
// ---------------------------------------------------------------------------

func TestGetServerIDFromAddr(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	// GetServerIDFromAddr expects a bus address (port+10000) and reverse-bumps it.
	id, ok := s.GetServerIDFromAddr("127.0.0.1:18008")
	if !ok {
		t.Fatal("expected to find node-1 from bus addr")
	}
	if id != "node-1" {
		t.Errorf("got ID = %q, want node-1", id)
	}
}

func TestGetServerIDFromAddr_NotFound(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	_, ok := s.GetServerIDFromAddr("127.0.0.1:29999")
	if ok {
		t.Error("expected not found for unknown addr")
	}
}

// ---------------------------------------------------------------------------
// Group operations
// ---------------------------------------------------------------------------

func TestGetServerGroup(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	g := s.GetServerGroup()
	if g != "default" {
		t.Errorf("GetServerGroup() = %q, want %q", g, "default")
	}
}

func TestGetGroupMembers(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	AddNodeToServer(s, "node-2", "127.0.0.1:8009", "default", 1.0)

	members := s.GetGroupMembers("default")
	if len(members) != 2 {
		t.Errorf("GetGroupMembers(default) len = %d, want 2", len(members))
	}

	members = s.GetGroupMembers("nonexistent")
	if len(members) != 0 {
		t.Errorf("GetGroupMembers(nonexistent) len = %d, want 0", len(members))
	}
}

func TestGetAllGroups(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	AddNodeToServer(s, "node-2", "127.0.0.1:8009", "eu-west", 1.0)

	groups := s.GetAllGroups()
	if len(groups) != 2 {
		t.Errorf("GetAllGroups() len = %d, want 2", len(groups))
	}
}

func TestGetLocalNodeID(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	if s.GetLocalNodeID() != "node-1" {
		t.Errorf("GetLocalNodeID() = %q", s.GetLocalNodeID())
	}
}

func TestGetLocalGroup(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	g := s.GetLocalGroup()
	if g != "default" {
		t.Errorf("GetLocalGroup() = %q, want %q", g, "default")
	}
}

func TestGetNodeAddr(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	addr, ok := s.GetNodeAddr("node-1")
	if !ok || addr != "127.0.0.1:8008" {
		t.Errorf("GetNodeAddr(node-1) = (%q, %v)", addr, ok)
	}

	_, ok = s.GetNodeAddr("ghost")
	if ok {
		t.Error("expected not found for ghost node")
	}
}

// ---------------------------------------------------------------------------
// UnreachableNodeList
// ---------------------------------------------------------------------------

func TestUnreachableNodeList_Empty(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	if got := s.UnreacableNodeList(); got != "" {
		t.Errorf("UnreacableNodeList() = %q, want empty", got)
	}
}

func TestUnreachableNodeList_WithEntries(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.UnreahableNodes["node-2"] = s.LastSeen["node-1"]

	got := s.UnreacableNodeList()
	if got == "" {
		t.Error("expected non-empty unreachable list")
	}
}

// ---------------------------------------------------------------------------
// Concurrent access safety (race detector)
// ---------------------------------------------------------------------------

func TestConcurrentVersionAccess(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.GetClusterVersion()
		}()
		go func() {
			defer wg.Done()
			s.UpdateClusterVersion(uint64(i))
		}()
	}
	wg.Wait()
	// If the race detector fires, the test fails automatically.
}

func TestConcurrentNodeAccess(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			s.GetNodeCount()
		}()
		go func() {
			defer wg.Done()
			s.HasNode("node-1")
		}()
		go func() {
			defer wg.Done()
			s.GetNodesSnapshot()
		}()
	}
	wg.Wait()
}
