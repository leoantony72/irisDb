package config

import (
	"errors"
	"fmt"
)

func (s *Server) NodeExit(serverID string) error {
	ok := s.HasNode(serverID)
	if !ok {
		return errors.New("SERVER DOESN'T EXIST")
	}

	//get all the ranges the serverID is handling
	masterRangeIndices := s.FindRangeIndexByServerID(serverID)

	s.mu.Lock()
	// promote the first replica to master
	for _, idx := range masterRangeIndices {
		slot := s.Metadata[idx]

		if len(slot.Nodes) == 0 {
			// no replicas to promote – you decide policy here
			return fmt.Errorf(
				"cannot remove %s: range %d-%d has no replicas to promote",
				serverID, slot.Start, slot.End,
			)
		}

		firstReplica := slot.Nodes[0]
		slot.MasterID = firstReplica
		slot.Nodes = slot.Nodes[1:] // remove promoted replica from replica list
	}

	// remove this node from all the replica
	for _, slot := range s.Metadata {
		if len(slot.Nodes) == 0 {
			continue
		}
		// Rebuild Nodes slice without serverID (in-place, no extra alloc)
		out := slot.Nodes[:0]
		for _, id := range slot.Nodes {
			if id != serverID {
				out = append(out, id)
			}
		}
		slot.Nodes = out
	}

	delete(s.Nodes, serverID)
	s.Nnode = uint16(len(s.Nodes))
	s.Cluster_Version++
	s.mu.Unlock()
	return nil
}
