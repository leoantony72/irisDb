package config

import (
	"log"
)


// ApplyClusterMetadata atomically replaces the cluster metadata and node map.
func (s *Server) ApplyClusterMetadata(newMetadata []*SlotRange, newNodes map[string]*Node) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var self *Node
	if oldSelf, ok := s.Nodes[s.ServerID]; ok {
		self = oldSelf
	}

	s.Metadata = newMetadata
	s.Nodes = newNodes

	if self != nil {
		s.Nodes[s.ServerID] = self
	}

	s.Nnode = uint16(len(s.Nodes))
	s.Cluster_Version++

	log.Printf(
		"Successfully updated metadata. New Cluster Version: %d, Nodes: %d, Slot Ranges: %d",
		s.Cluster_Version, s.Nnode, len(s.Metadata),
	)
}
