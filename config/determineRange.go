package config

import (
	"log"
	"math/rand"
)

// determineRange selects a random existing slot range to split.
// It returns the index of the selected range in s.Metadata, and the start/end of the new sub-range.
func (s *Server) DetermineRange() (int, uint16, uint16, []string, []string) {
	if s.GetNodeCount() == 0 || s.GetSlotRangeCount() == 0 {
		log.Fatal("No nodes or metadata found to determine range from. Cluster is empty?")
	}

	// Determine replica for the coordinating server if len(replicas) < ReplicationFactor.
	// (Assumes FindHandlingRanges is itself thread-safe.)
	_, _, replicas := s.FindHandlingRanges()
	if len(replicas) < s.ReplicationFactor {
		// plug extra logic here later
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var selectedIdx int
	var selectedRange *SlotRange
	attempts := 0
	maxAttempts := 100

	for attempts < maxAttempts {
		selectedIdx = rand.Intn(len(s.Metadata))
		tempRange := s.Metadata[selectedIdx]
		if tempRange.End > tempRange.Start {
			selectedRange = tempRange
			break
		}
		attempts++
	}

	if selectedRange == nil {
		log.Printf("Warning: Could not find a splittable range after %d attempts. Selecting any range.", maxAttempts)
		selectedIdx = rand.Intn(len(s.Metadata))
		selectedRange = s.Metadata[selectedIdx]
	}

	start := selectedRange.Start
	end := selectedRange.End

	mid := (start + end) / 2

	newRangeStart := mid + 1
	newRangeEnd := end

	log.Printf("Selected range for split: %d-%d (owned by %s). New node will take %d-%d. Old node keeps %d-%d.",
		selectedRange.Start, selectedRange.End, selectedRange.MasterID,
		newRangeStart, newRangeEnd, start, mid)

	newReplicaServer := s.selectReplicasLocked()
	existingReplicas := append([]string(nil), s.Metadata[selectedIdx].Nodes...)
	return selectedIdx, newRangeStart, newRangeEnd, newReplicaServer, existingReplicas
}

// selectReplicasLocked chooses replica IDs for a range.
// It assumes the caller does NOT hold s.mu; this method locks internally.
func (s *Server) selectReplicasLocked() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	candidates := make([]string, 0, len(s.Nodes))
	for _, node := range s.Nodes {
		candidates = append(candidates, node.ServerID)
	}

	if len(candidates) <= s.ReplicationFactor {
		return candidates
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	return candidates[:s.ReplicationFactor]
}
