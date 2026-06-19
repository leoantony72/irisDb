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
		// ensure group membership map includes this node
		gi, ok := s.Group[group]
		if !ok {
			gi = &GroupInfo{Name: group, Nodes: []string{}, Status: HEALTHY}
			s.Group[group] = gi
		}
		found := false
		for _, id := range gi.Nodes {
			if id == serverID {
				found = true
				break
			}
		}
		if !found {
			gi.Nodes = append(gi.Nodes, serverID)
		}
		log.Printf("[INFO]: Updated rejoining node %s - addr: %s, status: ALIVE\n", serverID, addr)
	}
}
