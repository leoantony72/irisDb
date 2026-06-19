package config

import (
	"bufio"
	"fmt"
	"iris/utils"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

// AntiEntropyDB is the interface that the anti-entropy coordinator uses to
// compute digests and trigger re-syncs. It is implemented by the adapter
// in main.go to avoid circular imports between config and engine.
type AntiEntropyDB interface {
	// ComputeRangeDigest returns a digest (key count + XOR checksum) for
	// all key-value pairs whose slot falls within [start, end].
	ComputeRangeDigest(start, end, totalSlots uint16) RangeDigest

	// InitiateDataTransferToReplica sends all keys in [start, end] to the
	// given replica. This is used as the repair action when divergence is detected.
	InitiateDataTransferToReplica(replicaID string, start, end uint16)
}

// RangeDigest mirrors engine.RangeDigest so the config package can work
// with digest results without importing engine.
type RangeDigest struct {
	Start    uint16
	End      uint16
	KeyCount uint64
	Checksum uint32
}

const (
	antiEntropyInterval = 60 * time.Second
	antiEntropyTimeout  = 10 * time.Second
)

// RunAntiEntropy is a long-running goroutine that periodically checks data
// consistency between this node (as master) and its replicas for every slot
// range it owns.
//
// For each owned range, it:
//  1. Computes a local digest (key count + XOR checksum)
//  2. Asks each replica to compute the same digest via the bus protocol
//  3. Compares the digests — if they differ, triggers a full re-sync
func (s *Server) RunAntiEntropy(db AntiEntropyDB) {
	// Give the cluster time to stabilize after startup
	time.Sleep(10 * time.Second)

	for {
		time.Sleep(antiEntropyInterval)

		if s.ShuttingDown.Load() {
			log.Println("[ANTI_ENTROPY] Stopping: server is shutting down")
			return
		}

		// Only masters run anti-entropy checks
		ownedRanges := s.FindRangeIndexByServerID(s.ServerID)
		if len(ownedRanges) == 0 {
			continue
		}

		totalSlots := s.N

		for _, idx := range ownedRanges {
			sr, ok := s.GetSlotRangeByIndex(idx)
			if !ok || sr == nil {
				continue
			}

			if len(sr.Nodes) == 0 {
				// No replicas for this range — nothing to check
				continue
			}

			// Compute master's own digest
			localDigest := db.ComputeRangeDigest(sr.Start, sr.End, totalSlots)

			log.Printf("[ANTI_ENTROPY] Master digest for range %d-%d: keys=%d checksum=%08x",
				sr.Start, sr.End, localDigest.KeyCount, localDigest.Checksum)

			// Check each replica
			for _, replicaID := range sr.Nodes {
				if replicaID == s.ServerID {
					continue // shouldn't happen, but guard anyway
				}

				s.checkReplicaDigest(db, replicaID, sr.Start, sr.End, totalSlots, localDigest)
			}
		}
	}
}

// checkReplicaDigest connects to a single replica, requests its digest for the
// given range, and compares it with the master's local digest.
func (s *Server) checkReplicaDigest(
	db AntiEntropyDB,
	replicaID string,
	start, end, totalSlots uint16,
	localDigest RangeDigest,
) {
	replicaNode, ok := s.GetConnectedNodeData(replicaID)
	if !ok {
		log.Printf("[ANTI_ENTROPY] Replica %s not found in node map, skipping", replicaID)
		return
	}

	busAddr, err := utils.BumpPort(replicaNode.Addr, 10000)
	if err != nil {
		log.Printf("[ANTI_ENTROPY] Failed to derive bus port for replica %s: %v", replicaID, err)
		return
	}

	conn, err := net.DialTimeout("tcp", busAddr, antiEntropyTimeout)
	if err != nil {
		log.Printf("[ANTI_ENTROPY] Failed to connect to replica %s at %s: %v", replicaID, busAddr, err)
		return
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(antiEntropyTimeout))

	// Send: ANTI_ENTROPY <start> <end>
	msg := fmt.Sprintf("ANTI_ENTROPY %d %d\n", start, end)
	if _, err := conn.Write([]byte(msg)); err != nil {
		log.Printf("[ANTI_ENTROPY] Failed to send digest request to replica %s: %v", replicaID, err)
		return
	}

	// Read response: DIGEST <keycount> <checksum>
	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[ANTI_ENTROPY] Failed to read digest response from replica %s: %v", replicaID, err)
		return
	}

	resp = strings.TrimSpace(resp)
	parts := strings.Fields(resp)

	if len(parts) != 3 || strings.ToUpper(parts[0]) != "DIGEST" {
		log.Printf("[ANTI_ENTROPY] Unexpected response from replica %s: %q", replicaID, resp)
		return
	}

	remoteKeyCount, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		log.Printf("[ANTI_ENTROPY] Invalid key count from replica %s: %v", replicaID, err)
		return
	}

	remoteChecksum, err := strconv.ParseUint(parts[2], 16, 32)
	if err != nil {
		log.Printf("[ANTI_ENTROPY] Invalid checksum from replica %s: %v", replicaID, err)
		return
	}

	// Compare digests
	if localDigest.KeyCount == remoteKeyCount && localDigest.Checksum == uint32(remoteChecksum) {
		log.Printf("[ANTI_ENTROPY] ✅ Replica %s is consistent for range %d-%d (keys=%d)",
			replicaID, start, end, localDigest.KeyCount)
		return
	}

	// Divergence detected!
	log.Printf("[DIVERGENCE] ⚠️ Replica %s diverged for range %d-%d! master(keys=%d, crc=%08x) replica(keys=%d, crc=%08x)",
		replicaID, start, end,
		localDigest.KeyCount, localDigest.Checksum,
		remoteKeyCount, uint32(remoteChecksum))

	log.Printf("[ANTI_ENTROPY] Initiating re-sync for replica %s range %d-%d", replicaID, start, end)
	go db.InitiateDataTransferToReplica(replicaID, start, end)
}
