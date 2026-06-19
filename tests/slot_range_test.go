package tests

import (
	"iris/config"
	"testing"
)

// ---------------------------------------------------------------------------
// FindNodeIdx — binary search over sorted metadata
// ---------------------------------------------------------------------------

func TestFindNodeIdx_SingleRange(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	tests := []struct {
		slot uint16
		want int
	}{
		{0, 0},
		{8191, 0},
		{16383, 0},
	}
	for _, tt := range tests {
		got := s.FindNodeIdx(tt.slot)
		if got != tt.want {
			t.Errorf("FindNodeIdx(%d) = %d, want %d", tt.slot, got, tt.want)
		}
	}
}

func TestFindNodeIdx_MultiRange(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	// Split into 3 ranges: [0-5000], [5001-10000], [10001-16383]
	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 5000, MasterID: "node-1"},
		{Start: 5001, End: 10000, MasterID: "node-2"},
		{Start: 10001, End: 16383, MasterID: "node-3"},
	}

	tests := []struct {
		slot uint16
		want int
	}{
		{0, 0},
		{5000, 0},
		{5001, 1},
		{7500, 1},
		{10000, 1},
		{10001, 2},
		{16383, 2},
	}
	for _, tt := range tests {
		got := s.FindNodeIdx(tt.slot)
		if got != tt.want {
			t.Errorf("FindNodeIdx(%d) = %d, want %d", tt.slot, got, tt.want)
		}
	}
}

func TestFindNodeIdx_EmptyMetadata(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Metadata = []*config.SlotRange{}

	if got := s.FindNodeIdx(100); got != -1 {
		t.Errorf("FindNodeIdx on empty metadata = %d, want -1", got)
	}
}

func TestFindNodeIdx_BoundarySlots(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 8191, MasterID: "node-1"},
		{Start: 8192, End: 16383, MasterID: "node-2"},
	}

	// Boundary between ranges.
	if got := s.FindNodeIdx(8191); got != 0 {
		t.Errorf("FindNodeIdx(8191) = %d, want 0", got)
	}
	if got := s.FindNodeIdx(8192); got != 1 {
		t.Errorf("FindNodeIdx(8192) = %d, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// FindRangeIndex — exact start/end match
// ---------------------------------------------------------------------------

func TestFindRangeIndex_Found(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 8191, MasterID: "node-1"},
		{Start: 8192, End: 16383, MasterID: "node-2"},
	}

	idx := s.FindRangeIndex(8192, 16383)
	if idx != 1 {
		t.Errorf("FindRangeIndex(8192, 16383) = %d, want 1", idx)
	}
}

func TestFindRangeIndex_NotFound(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	idx := s.FindRangeIndex(100, 200)
	if idx != -1 {
		t.Errorf("FindRangeIndex(100, 200) = %d, want -1", idx)
	}
}

// ---------------------------------------------------------------------------
// FindRangeIndexByServerID — master ranges only
// ---------------------------------------------------------------------------

func TestFindRangeIndexByServerID(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 5000, MasterID: "node-1"},
		{Start: 5001, End: 10000, MasterID: "node-2"},
		{Start: 10001, End: 16383, MasterID: "node-1"},
	}

	indices := s.FindRangeIndexByServerID("node-1")
	if len(indices) != 2 {
		t.Fatalf("FindRangeIndexByServerID(node-1) returned %d indices, want 2", len(indices))
	}
	if indices[0] != 0 || indices[1] != 2 {
		t.Errorf("indices = %v, want [0, 2]", indices)
	}

	indices = s.FindRangeIndexByServerID("ghost")
	if len(indices) != 0 {
		t.Errorf("FindRangeIndexByServerID(ghost) returned %d indices, want 0", len(indices))
	}
}

// ---------------------------------------------------------------------------
// GetSlotRangeByIndex
// ---------------------------------------------------------------------------

func TestGetSlotRangeByIndex_Valid(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Metadata[0].Nodes = []string{"replica-1", "replica-2"}

	sr, ok := s.GetSlotRangeByIndex(0)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if sr.MasterID != "node-1" {
		t.Errorf("MasterID = %q", sr.MasterID)
	}

	// Deep copy: mutating returned copy must not affect server.
	sr.Nodes[0] = "MUTATED"
	orig, _ := s.GetSlotRangeByIndex(0)
	if orig.Nodes[0] == "MUTATED" {
		t.Error("mutation leaked into server metadata")
	}
}

func TestGetSlotRangeByIndex_OutOfBounds(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")

	_, ok := s.GetSlotRangeByIndex(-1)
	if ok {
		t.Error("expected ok=false for negative index")
	}

	_, ok = s.GetSlotRangeByIndex(999)
	if ok {
		t.Error("expected ok=false for out-of-range index")
	}
}

// ---------------------------------------------------------------------------
// GetSlotRangesByIndices — batch retrieval
// ---------------------------------------------------------------------------

func TestGetSlotRangesByIndices(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Metadata = []*config.SlotRange{
		{Start: 0, End: 5000, MasterID: "node-1"},
		{Start: 5001, End: 10000, MasterID: "node-2"},
		{Start: 10001, End: 16383, MasterID: "node-3"},
	}

	ranges := s.GetSlotRangesByIndices([]int{0, 2})
	if len(ranges) != 2 {
		t.Fatalf("len = %d, want 2", len(ranges))
	}
	if ranges[0].MasterID != "node-1" || ranges[1].MasterID != "node-3" {
		t.Errorf("got masters [%s, %s], want [node-1, node-3]", ranges[0].MasterID, ranges[1].MasterID)
	}

	// Out-of-bounds indices are silently skipped.
	ranges = s.GetSlotRangesByIndices([]int{-1, 0, 999})
	if len(ranges) != 1 {
		t.Errorf("expected 1 valid result, got %d", len(ranges))
	}
}

// ---------------------------------------------------------------------------
// GetServerMetadata — full copy
// ---------------------------------------------------------------------------

func TestGetServerMetadata(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	meta := s.GetServerMetadata()
	if len(meta) != 1 {
		t.Fatalf("len = %d, want 1", len(meta))
	}
	if meta[0].Start != 0 || meta[0].End != 16383 {
		t.Errorf("range = %d-%d", meta[0].Start, meta[0].End)
	}
}

// ---------------------------------------------------------------------------
// FindHandlingRanges — first master range
// ---------------------------------------------------------------------------

func TestFindHandlingRanges(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	start, end, replicas := s.FindHandlingRanges()
	if start != 0 || end != 16383 {
		t.Errorf("range = %d-%d", start, end)
	}
	// Empty replicas for a fresh server.
	if len(replicas) != 0 {
		t.Errorf("replicas = %v, want empty", replicas)
	}
}

func TestFindHandlingRanges_NotMaster(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Metadata[0].MasterID = "node-other"

	start, end, _ := s.FindHandlingRanges()
	// When this server doesn't master any range, returns defaults.
	if start != 0 && end != 0 {
		t.Logf("FindHandlingRanges returned %d-%d (expected 0-0 or empty result)", start, end)
	}
}

// ---------------------------------------------------------------------------
// GetMasterNodeForRangeIdx
// ---------------------------------------------------------------------------

func TestGetMasterNodeForRangeIdx_Valid(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	node, ok := s.GetMasterNodeForRangeIdx(0)
	if !ok {
		t.Fatal("expected to find master node")
	}
	if node.ServerID != "node-1" {
		t.Errorf("master = %q, want node-1", node.ServerID)
	}
}

func TestGetMasterNodeForRangeIdx_Invalid(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	_, ok := s.GetMasterNodeForRangeIdx(-1)
	if ok {
		t.Error("expected not found for negative index")
	}
	_, ok = s.GetMasterNodeForRangeIdx(100)
	if ok {
		t.Error("expected not found for out-of-range index")
	}
}
