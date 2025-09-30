package bus

import (
	"fmt"
	"iris/config"
	"net"
	"strings"
)

func HandleShow(conn net.Conn, s *config.Server) {
	defer conn.Write([]byte("---------------\n"))

	// Basic server info
	conn.Write([]byte("---------------\n"))
	conn.Write([]byte(fmt.Sprintf(
		"Server ID: %s | Host: %s | Addr: %s | BusPort: %s\n",
		s.ServerID, s.Host, s.Addr, s.BusPort,
	)))
	conn.Write([]byte(fmt.Sprintf(
		"Cluster Version: %d | Total Nodes: %d | Total Slots: %d\n",
		s.Cluster_Version, s.Nnode, s.N,
	)))

	// List all nodes in the cluster
	conn.Write([]byte("--- Nodes in Cluster ---\n"))
	for _, node := range s.Nodes {
		msg := fmt.Sprintf("  ServerID: %s | Addr: %s\n", node.ServerID, node.Addr)
		conn.Write([]byte(msg))
	}

	// Slot range info
	conn.Write([]byte("--- Slot Ranges ---\n"))
	if len(s.Metadata) == 0 {
		conn.Write([]byte("  No metadata available\n"))
	} else {
		for i, sr := range s.Metadata {
			nodeAddrs := []string{}

			if len(sr.Nodes) == 0 {
				nodeAddrs = append(nodeAddrs, "NONE")
			} else {
				for _, nodeID := range sr.Nodes {
					if nodeID == "NONE" {
						nodeAddrs = append(nodeAddrs, "NONE")
						break
					}
					node, ok := s.Nodes[nodeID]
					if !ok {
						nodeAddrs = append(nodeAddrs, fmt.Sprintf("UNKNOWN(%s)", nodeID))
						continue
					}
					nodeAddrs = append(nodeAddrs, fmt.Sprintf("%s@%s", node.ServerID, node.Addr))
				}
			}

			nodesStr := strings.Join(nodeAddrs, ",")
			if nodesStr == "" {
				nodesStr = "NONE"
			}

			msg := fmt.Sprintf(
				"  [%d] Start:%d | End:%d | MasterID: %s | Nodes: %s\n",
				i, sr.Start, sr.End, sr.MasterID, nodesStr,
			)
			conn.Write([]byte(msg))
		}
	}
}
