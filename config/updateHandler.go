package config

import "time"

func (s *Server) UpdateHeartbeat(peerId string, unreableNodes []string, group string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	//before updating, check if peerId exists in s.Nodes
	if !s.HasNode(peerId) {
		return false // maybe in the future add some logic to handle this err
	}

	s.LastSeen[peerId] = time.Now()

	return true
}
