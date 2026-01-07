package config

func (s *Server) UpdateHeartbeat(peerId string, unreableNodes []string, group string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
}
