package config

// ReplicationValidator checks if the handling range has enough replica.
func (s *Server) ReplicationValidator() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the ranges this server is master for
	ranges := s.FindRangeIndexByServerID(s.ServerID)
	for _, idx := range ranges {
		r := s.Metadata[idx]
		if len(r.Nodes) < s.ReplicationFactor {
			return false
		}
	}

	// No range found for this server
	return true
}

//should use 'return len(replica) >= s.ReplicationFactor' instead of 'if len(replica) < s.ReplicationFactor { return false }; return true'
