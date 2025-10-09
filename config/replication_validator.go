package config

func (s *Server) ReplicationValidator() bool {
	_, _, replica := s.FindHandlingRanges()
	return len(replica) >= s.ReplicationFactor
}


//should use 'return len(replica) >= s.ReplicationFactor' instead of 'if len(replica) < s.ReplicationFactor { return false }; return true'