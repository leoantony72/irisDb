package config

// ReplicationValidator checks if the handling range has enough replica.
func (s *Server) ReplicationValidator() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find the range this server is master for
	for _, nodeRange := range s.Metadata {
		if nodeRange.MasterID == s.ServerID {
			replicaCount := len(nodeRange.Nodes)
			required := s.ReplicationFactor

			if replicaCount < required {
				return false
			}
			return true
		}
	}

	// No range found for this server
	return false
}

//should use 'return len(replica) >= s.ReplicationFactor' instead of 'if len(replica) < s.ReplicationFactor { return false }; return true'
