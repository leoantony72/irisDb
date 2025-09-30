package bus

import (
	"iris/config"
	"log"
	"math/rand"
)

// determineRange selects a random existing slot range to split.
// It returns the index of the selected range in s.Metadata, and the start/end of the new sub-range.
func DetermineRange(s *config.Server) (int, uint16, uint16, []string, []string) {
	if s.Nnode == 0 || len(s.Metadata) == 0 {
		log.Fatal("No nodes or metadata found to determine range from. Cluster is empty?")
	}

	//determine replica for the coordinating server if the len(replica) < replicationFactor
	_,_, replicas := s.FindHandlingRanges()
	if len(replicas) < s.ReplicationFactor{
		
	}

	// Iterate to find a range that can actually be split (has more than 1 slot)
	var selectedIdx int
	var selectedRange *config.SlotRange
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
		selectedRange.Start, selectedRange.End, selectedRange.MasterID, newRangeStart, newRangeEnd, start, mid)

	// Logic for the determination of Replica for the splitted range
	// 1) One of the replica will always be the modified server since it already have the
	//    data in the server
	// 2) The modified server, will already have a list of replica for the original range
	//    Copy the already existing replica list to the new joined server.
	// 3) If the range.Nodes < replication_factor, We need a algorithm to decide which
	//    server the new splitted range will handle.

	newReplicaServer := selectReplicas(s)


	return selectedIdx, newRangeStart, newRangeEnd, newReplicaServer, s.Metadata[selectedIdx].Nodes
}


func selectReplicas(s *config.Server) []string {
    candidates := []string{}
    for _, node := range s.Nodes {
        // if node.ServerID != s.ServerID {
            candidates = append(candidates, node.ServerID)
        // }
    }

    if len(candidates) <= s.ReplicationFactor {
        return candidates
    }

    rand.Shuffle(len(candidates), func(i, j int) {
        candidates[i], candidates[j] = candidates[j], candidates[i]
    })
    return candidates[:s.ReplicationFactor]
}