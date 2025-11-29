package config

// ReplicationValidator checks if the handling range has enough replica.
func (s *Server) ReplicationValidator() bool {
	_, _, replica := s.FindHandlingRanges()
	return len(replica) >= s.ReplicationFactor
}


//should use 'return len(replica) >= s.ReplicationFactor' instead of 'if len(replica) < s.ReplicationFactor { return false }; return true'