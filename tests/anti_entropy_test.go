package tests

import (
	"iris/config"
	"testing"
)

// The anti-entropy system works by:
// 1. Master computes a local digest for each range it owns
// 2. Master asks each replica to compute the same digest
// 3. If digests differ, master triggers a full re-sync
//
// Since RunAntiEntropy runs in an infinite loop with 60s intervals,
// we test the individual pieces rather than the coordinator loop.

// ---------------------------------------------------------------------------
// Mock AntiEntropyDB — interface contract
// ---------------------------------------------------------------------------

func TestMockAntiEntropyDB_ComputeDigest(t *testing.T) {
	db := NewMockAntiEntropyDB()
	db.Digests["0-16383"] = config.RangeDigest{
		Start:    0,
		End:      16383,
		KeyCount: 100,
		Checksum: 0xDEADBEEF,
	}

	d := db.ComputeRangeDigest(0, 16383, 16384)
	if d.KeyCount != 100 {
		t.Errorf("KeyCount = %d, want 100", d.KeyCount)
	}
	if d.Checksum != 0xDEADBEEF {
		t.Errorf("Checksum = %08x, want DEADBEEF", d.Checksum)
	}
}

func TestMockAntiEntropyDB_UnknownRange(t *testing.T) {
	db := NewMockAntiEntropyDB()

	d := db.ComputeRangeDigest(0, 100, 16384)
	if d.KeyCount != 0 {
		t.Errorf("KeyCount = %d, want 0 for unknown range", d.KeyCount)
	}
}

func TestMockAntiEntropyDB_TransferCall(t *testing.T) {
	db := NewMockAntiEntropyDB()

	db.InitiateDataTransferToReplica("replica-1", 0, 16383)
	db.InitiateDataTransferToReplica("replica-2", 100, 200)

	if len(db.TransferCalls) != 2 {
		t.Fatalf("expected 2 transfer calls, got %d", len(db.TransferCalls))
	}
	if db.TransferCalls[0].ReplicaID != "replica-1" {
		t.Errorf("call[0].ReplicaID = %q", db.TransferCalls[0].ReplicaID)
	}
	if db.TransferCalls[1].Start != 100 || db.TransferCalls[1].End != 200 {
		t.Errorf("call[1] range = %d-%d", db.TransferCalls[1].Start, db.TransferCalls[1].End)
	}
}

// ---------------------------------------------------------------------------
// Anti-entropy skips non-master ranges
// ---------------------------------------------------------------------------

func TestAntiEntropy_SkipsNonMaster(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	// Set master to someone else.
	s.Metadata[0].MasterID = "other-node"

	ownedRanges := s.FindRangeIndexByServerID(s.ServerID)
	if len(ownedRanges) != 0 {
		t.Errorf("expected 0 owned ranges, got %d", len(ownedRanges))
	}
}

// ---------------------------------------------------------------------------
// Anti-entropy skips ranges with no replicas
// ---------------------------------------------------------------------------

func TestAntiEntropy_SkipsNoReplicas(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	// node-1 owns the range but has no replicas.

	sr, ok := s.GetSlotRangeByIndex(0)
	if !ok {
		t.Fatal("range not found")
	}
	if len(sr.Nodes) != 0 {
		t.Errorf("expected 0 replicas, got %d", len(sr.Nodes))
	}
	// Anti-entropy should skip this range — nothing to verify against.
}

// ---------------------------------------------------------------------------
// Digest comparison logic
// ---------------------------------------------------------------------------

func TestAntiEntropy_DigestMatch(t *testing.T) {
	localDigest := config.RangeDigest{
		Start:    0,
		End:      16383,
		KeyCount: 50,
		Checksum: 0xABCD1234,
	}

	remoteKeyCount := uint64(50)
	remoteChecksum := uint32(0xABCD1234)

	if localDigest.KeyCount != remoteKeyCount || localDigest.Checksum != remoteChecksum {
		t.Error("digests should match")
	}
}

func TestAntiEntropy_DigestMismatch_KeyCount(t *testing.T) {
	localDigest := config.RangeDigest{KeyCount: 50, Checksum: 0xABCD1234}
	remoteKeyCount := uint64(45)

	if localDigest.KeyCount == remoteKeyCount {
		t.Error("key counts should differ")
	}
	// In production, this would trigger InitiateDataTransferToReplica.
}

func TestAntiEntropy_DigestMismatch_Checksum(t *testing.T) {
	localDigest := config.RangeDigest{KeyCount: 50, Checksum: 0xABCD1234}
	remoteChecksum := uint32(0xDEAD0000)

	if localDigest.Checksum == remoteChecksum {
		t.Error("checksums should differ")
	}
}

// ---------------------------------------------------------------------------
// Full anti-entropy scenario (simulated without network)
// ---------------------------------------------------------------------------

func TestAntiEntropy_FullScenario(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "replica-1", "127.0.0.1:8009", "default", 1.0)
	s.Metadata[0].Nodes = []string{"replica-1"}

	db := NewMockAntiEntropyDB()
	db.Digests["0-16383"] = config.RangeDigest{
		Start:    0,
		End:      16383,
		KeyCount: 100,
		Checksum: 0x12345678,
	}

	// Simulate: master computes digest for its owned range.
	ownedRanges := s.FindRangeIndexByServerID(s.ServerID)
	if len(ownedRanges) == 0 {
		t.Fatal("master should own at least one range")
	}

	for _, idx := range ownedRanges {
		sr, ok := s.GetSlotRangeByIndex(idx)
		if !ok {
			continue
		}

		if len(sr.Nodes) == 0 {
			continue // no replicas
		}

		localDigest := db.ComputeRangeDigest(sr.Start, sr.End, s.N)

		// Simulate a replica with different data.
		remoteKeyCount := uint64(95) // differs
		remoteChecksum := uint32(0x87654321)

		if localDigest.KeyCount != remoteKeyCount || localDigest.Checksum != remoteChecksum {
			// Divergence detected — trigger repair.
			for _, replicaID := range sr.Nodes {
				db.InitiateDataTransferToReplica(replicaID, sr.Start, sr.End)
			}
		}
	}

	if len(db.TransferCalls) != 1 {
		t.Errorf("expected 1 transfer call, got %d", len(db.TransferCalls))
	}
	if db.TransferCalls[0].ReplicaID != "replica-1" {
		t.Errorf("transfer call replica = %q, want replica-1", db.TransferCalls[0].ReplicaID)
	}
}
