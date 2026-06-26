package config

import (
	"log"
	"math"
	"sort"
)

// determineRange selects the slot range handled by the node with the lowest ResourceScore to split.
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

	var selectedIdx = -1
	var minScore float64 = math.MaxFloat64
	var maxRangeSize uint16 = 0

	for i, r := range s.Metadata {
		if r.End <= r.Start {
			continue // not splittable
		}
		masterScore := 0.0
		if node, ok := s.Nodes[r.MasterID]; ok {
			masterScore = node.ResourceScore
		}

		rangeSize := r.End - r.Start
		// Find range owned by master with the lowest ResourceScore.
		// Break ties by choosing the range with the larger slot footprint.
		if selectedIdx == -1 || masterScore < minScore || (masterScore == minScore && rangeSize > maxRangeSize) {
			selectedIdx = i
			minScore = masterScore
			maxRangeSize = rangeSize
		}
	}

	if selectedIdx == -1 {
		log.Printf("Warning: Could not find any splittable range. Selecting first range.")
		if len(s.Metadata) > 0 {
			selectedIdx = 0
		} else {
			log.Fatal("No metadata ranges found.")
		}
	}

	selectedRange := s.Metadata[selectedIdx]
	start := selectedRange.Start
	end := selectedRange.End

	mid := (start + end) / 2

	newRangeStart := mid + 1
	newRangeEnd := end

	log.Printf("Selected range for split: %d-%d (owned by %s with score %.6f). New node will take %d-%d. Old node keeps %d-%d.",
		selectedRange.Start, selectedRange.End, selectedRange.MasterID, minScore,
		newRangeStart, newRangeEnd, start, mid)

	newReplicaServer := s.selectReplicasLocked()
	existingReplicas := append([]string(nil), s.Metadata[selectedIdx].Nodes...)
	return selectedIdx, newRangeStart, newRangeEnd, newReplicaServer, existingReplicas
}

// selectReplicasLocked chooses replica IDs for a range.
// It assumes the caller holds s.mu (either Lock or RLock).
func (s *Server) selectReplicasLocked() []string {
	type candidateInfo struct {
		id    string
		score float64
	}
	candidates := make([]candidateInfo, 0, len(s.Nodes))
	for id, node := range s.Nodes {
		candidates = append(candidates, candidateInfo{id: id, score: node.ResourceScore})
	}

	if len(candidates) <= s.ReplicationFactor {
		res := make([]string, 0, len(candidates))
		for _, c := range candidates {
			res = append(res, c.id)
		}
		return res
	}

	// Sort candidates by ResourceScore in descending order, with ID lexicographically as a tie-breaker.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].id < candidates[j].id
	})

	res := make([]string, 0, len(candidates))
	for _, c := range candidates {
		res = append(res, c.id)
	}

	return res[:s.ReplicationFactor]
}
