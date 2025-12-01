package config

import (
	"fmt"
	"log"
)

// Validate+store a PREPARE message (used by remote handler and local coordinator).
func (s *Server) AcceptPrepare(
	messageID string,
	sourceNodeID string,
	targetNodeID string,
	targetNodeAddr string,
	start, end uint16,
	modifiedNodeID string,
	modifiedReplicas []string,
	targetReplicas []string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.Prepared[messageID]; exists {
		return fmt.Errorf("MessageID %s already exists", messageID)
	}

	// Check that modifiedNodeID actually owns the [start,end] range.
	isModifiedNodeMaster := false
	for _, sr := range s.Metadata {
		if sr == nil {
			continue
		}
		if sr.MasterID == modifiedNodeID &&
			start >= sr.Start && end <= sr.End {
			isModifiedNodeMaster = true
			break
		}
	}

	if !isModifiedNodeMaster {
		return fmt.Errorf("Node %s does not control slot range %d-%d",
			modifiedNodeID, start, end)
	}

	s.Prepared[messageID] = &PrepareMessage{
		MessageID:               messageID,
		SourceNodeID:            sourceNodeID,
		TargetNodeID:            targetNodeID,
		Addr:                    targetNodeAddr,
		Start:                   start,
		End:                     end,
		ModifiedNodeID:          modifiedNodeID,
		ModifiedNodeReplicaList: append([]string(nil), modifiedReplicas...),
		TargetNodeReplicaList:   append([]string(nil), targetReplicas...),
	}

	log.Printf("PREPARE message %s stored from %s.", messageID, sourceNodeID)
	return nil
}

// Local helper to store prepared state on the coordinator
// without doing another SourceNodeID argument at callsites.
func (s *Server) AddLocalPrepare(
	messageID string,
	targetNodeID string,
	targetNodeAddr string,
	start, end uint16,
	modifiedNodeID string,
	modifiedReplicas []string,
	targetReplicas []string,
) error {
	return s.AcceptPrepare(
		messageID,
		s.ServerID,
		targetNodeID,
		targetNodeAddr,
		start,
		end,
		modifiedNodeID,
		modifiedReplicas,
		targetReplicas,
	)
}
