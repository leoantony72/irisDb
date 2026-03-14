package config

import "log"

func (s *Server) UpdateRejoiningNode(serverID, addr, group string, resourceScore float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.Nodes[serverID]
	if exists && node != nil {
		node.Addr = addr
		node.ResourceScore = resourceScore
		node.Status = ALIVE
		node.Group = group
		// Clear suspect messages for this node
		delete(s.SuspectLeaderMsg, serverID)
		log.Printf("[INFO]: Updated rejoining node %s - addr: %s, status: ALIVE\n", serverID, addr)
	}
}
