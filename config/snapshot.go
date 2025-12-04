package config

import (
	"encoding/gob"
	"fmt"
	"net"
)

type ClusterSnapshot struct {
	ClusterVersion uint64
	TotalNodes     uint16
	TotalSlots     uint16

	Nodes    []Node      // value copies of nodes
	Metadata []SlotRange // value copies of slot ranges
}

func (s *Server) BuildClusterSnapshot() ClusterSnapshot {
	serverID, host, addr, busPort, version, totalNodes, totalSlots :=
		s.GetBasicInfo()
	_ = serverID
	_ = host
	_ = addr
	_ = busPort

	nodes := s.GetNodesSnapshot()
	metadata := s.GetServerMetadata()

	return ClusterSnapshot{
		ClusterVersion: version,
		TotalNodes:     totalNodes,
		TotalSlots:     totalSlots,
		Nodes:          nodes,
		Metadata:       metadata,
	}
}

func (s *Server) ApplyClusterSnapshot(snapshot ClusterSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newNodes := make(map[string]*Node, len(snapshot.Nodes))
	for i := range snapshot.Nodes {
		n := snapshot.Nodes[i]
		nCopy := n
		newNodes[n.ServerID] = &nCopy
	}

	newMetadata := make([]*SlotRange, 0, len(snapshot.Metadata))
	for i := range snapshot.Metadata {
		r := snapshot.Metadata[i]
		rCopy := r
		newMetadata = append(newMetadata, &rCopy)
	}

	s.Nodes = newNodes
	s.Metadata = newMetadata
	s.Nnode = snapshot.TotalNodes
	s.N = snapshot.TotalSlots
	s.Cluster_Version = snapshot.ClusterVersion
}

func (s *Server) SendClusterSnapshot(conn net.Conn) error {
	snap := s.BuildClusterSnapshot()

	enc := gob.NewEncoder(conn)
	if err := enc.Encode(&snap); err != nil {
		return fmt.Errorf("failed to encode cluster snapshot: %w", err)
	}
	return nil
}
