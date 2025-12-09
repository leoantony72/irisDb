package config

import (
	"fmt"
	"log"
)

func (s *Server) AddReplicaToRange(serverID string, start, end uint16) error {
	log.Printf("AddReplicaToRange: About to acquire write lock for server %s, range %d-%d", serverID, start, end)
	s.mu.Lock()
	log.Printf("AddReplicaToRange: Write lock acquired")
	defer s.mu.Unlock()

	// Ensure node exists
	if _, exists := s.Nodes[serverID]; !exists {
		return fmt.Errorf("server ID %s not found", serverID)
	}

	// Use the locked version to avoid deadlock (we already hold the write lock)
	idx := s.findRangeIndexLocked(start, end)
	if idx == -1 {
		return fmt.Errorf("range %d-%d not found", start, end)
	}

	// Append replica ID (no duplicate checking here – add if you want)
	s.Metadata[idx].Nodes = append(s.Metadata[idx].Nodes, serverID)
	s.Cluster_Version++

	log.Printf("AddReplicaToRange: Completed, releasing lock")
	return nil
}
