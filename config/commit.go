package config

import (
	"fmt"
	"log"
	"sort"
)

// ApplyCommitByID applies a COMMIT for the given messageID locally.
// It encapsulates all mutation of Prepared, Metadata, Nodes, Cluster_Version.
func (s *Server) ApplyCommitByID(messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	preparedMsg, exists := s.Prepared[messageID]
	if !exists {
		return fmt.Errorf("messageID %s doesn't exist in prepared state", messageID)
	}

	log.Printf("COMMIT message %s received. Applying changes locally.", messageID)

	// Ensure the target node exists in the cluster node map.
	if _, ok := s.Nodes[preparedMsg.TargetNodeID]; !ok {
		s.Nodes[preparedMsg.TargetNodeID] = &Node{
			ServerID: preparedMsg.TargetNodeID,
			Addr:     preparedMsg.Addr,
		}
		s.Nnode++
		log.Printf("New node %s added to the cluster.", preparedMsg.TargetNodeID)
	}

	// Find the range that should be split.
	modifiedRangeIdx := -1
	for i, sr := range s.Metadata {
		if sr.MasterID == preparedMsg.ModifiedNodeID &&
			preparedMsg.Start > sr.Start && preparedMsg.End == sr.End {
			modifiedRangeIdx = i
			break
		}
	}

	if modifiedRangeIdx == -1 {
		return fmt.Errorf(
			"COMMIT failed for message ID %s: ModifiedNode SlotRange not found",
			messageID,
		)
	}

	// Shrink the existing range.
	s.Metadata[modifiedRangeIdx].End = preparedMsg.Start - 1
	s.Metadata[modifiedRangeIdx].Nodes = preparedMsg.ModifiedNodeReplicaList

	// Create the new slot range for the joining node.
	newJoinNodeRange := &SlotRange{
		Start:    preparedMsg.Start,
		End:      preparedMsg.End,
		MasterID: preparedMsg.TargetNodeID,
		Nodes:    preparedMsg.TargetNodeReplicaList,
	}
	s.Metadata = append(s.Metadata, newJoinNodeRange)

	// Keep metadata sorted by Start.
	sort.Slice(s.Metadata, func(i, j int) bool {
		return s.Metadata[i].Start < s.Metadata[j].Start
	})

	s.Cluster_Version++
	delete(s.Prepared, messageID)

	log.Printf("COMMIT %s successful. Cluster version is now %d. Metadata updated.",
		messageID, s.Cluster_Version)

	return nil
}
