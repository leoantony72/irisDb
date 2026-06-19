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
			ServerID:      preparedMsg.TargetNodeID,
			Addr:          preparedMsg.Addr,
			ResourceScore: preparedMsg.ResourceScore,
			Group:         preparedMsg.Group,
		}
		s.Nnode++
		log.Printf("New node %s added to the cluster.", preparedMsg.TargetNodeID)
	}

	// Ensure the Group map reflects the new node
	if preparedMsg.Group != "" {
		gi, ok := s.Group[preparedMsg.Group]
		if !ok {
			gi = &GroupInfo{Name: preparedMsg.Group, Nodes: []string{}, Status: HEALTHY}
			s.Group[preparedMsg.Group] = gi
		}
		// append if not already present
		found := false
		for _, id := range gi.Nodes {
			if id == preparedMsg.TargetNodeID {
				found = true
				break
			}
		}
		if !found {
			gi.Nodes = append(gi.Nodes, preparedMsg.TargetNodeID)
		}
	}

	// Debug: log replica lists contained in the prepared message
	log.Printf("COMMIT %s: ModifiedNodeReplicaList=%v TargetNodeReplicaList=%v", messageID, preparedMsg.ModifiedNodeReplicaList, preparedMsg.TargetNodeReplicaList)

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
	// Only overwrite replica list if the prepared message provided non-empty list
	if len(preparedMsg.ModifiedNodeReplicaList) > 0 {
		s.Metadata[modifiedRangeIdx].Nodes = preparedMsg.ModifiedNodeReplicaList
	}
	// Create the new slot range for the joining node.
	newJoinNodeRange := &SlotRange{
		Start:    preparedMsg.Start,
		End:      preparedMsg.End,
		MasterID: preparedMsg.TargetNodeID,
		Nodes:    nil,
	}
	// set replica list for new join range only if provided
	if len(preparedMsg.TargetNodeReplicaList) > 0 {
		newJoinNodeRange.Nodes = preparedMsg.TargetNodeReplicaList
	}
	s.Metadata = append(s.Metadata, newJoinNodeRange)

	// Keep metadata sorted by Start.
	sort.Slice(s.Metadata, func(i, j int) bool {
		return s.Metadata[i].Start < s.Metadata[j].Start
	})

	s.Cluster_Version++
	delete(s.Prepared, messageID)

	// Ensure replica lists meet ReplicationFactor for modified and new ranges
	ensureReplica := func(idx int) {
		if idx < 0 || idx >= len(s.Metadata) {
			return
		}
		r := s.Metadata[idx]
		needed := s.ReplicationFactor - len(r.Nodes)
		if needed <= 0 {
			return
		}
		// build existing set
		existing := make(map[string]bool)
		existing[r.MasterID] = true
		for _, id := range r.Nodes {
			existing[id] = true
		}
		// pick candidates from s.Nodes
		for id := range s.Nodes {
			if needed <= 0 {
				break
			}
			if existing[id] {
				continue
			}
			r.Nodes = append(r.Nodes, id)
			existing[id] = true
			needed--
		}
	}

	ensureReplica(modifiedRangeIdx)
	// find index of new join range (MasterID == preparedMsg.TargetNodeID)
	newIdx := -1
	for i, rr := range s.Metadata {
		if rr.MasterID == preparedMsg.TargetNodeID {
			newIdx = i
			break
		}
	}
	ensureReplica(newIdx)

	log.Printf("COMMIT %s successful. Cluster version is now %d. Metadata updated.",
		messageID, s.Cluster_Version)

	return nil
}
