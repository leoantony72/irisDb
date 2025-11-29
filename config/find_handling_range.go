package config

func (s *Server) FindHandlingRanges() (uint16, uint16, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, nodeRange := range s.Metadata {
		if nodeRange.MasterID == s.ServerID {
			return nodeRange.Start, nodeRange.End, nodeRange.Nodes
		}
	}
	return 0, 0, []string{""}
}
