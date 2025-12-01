package config

import "fmt"

func (s *Server) AddReplicaToRange(serverID string, start, end uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure node exists
	if _, exists := s.Nodes[serverID]; !exists {
		return fmt.Errorf("server ID %s not found", serverID)
	}

	idx := s.FindRangeIndex(start, end)
	if idx == -1 {
		return fmt.Errorf("range %d-%d not found", start, end)
	}

	// Append replica ID (no duplicate checking here – add if you want)
	s.Metadata[idx].Nodes = append(s.Metadata[idx].Nodes, serverID)
	s.Cluster_Version++

	return nil
}
