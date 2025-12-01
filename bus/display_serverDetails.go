package bus

import (
	"fmt"
	"iris/config"
	"net"
	"strings"
)

func HandleShow(conn net.Conn, s *config.Server) {
	defer conn.Write([]byte("---------------\n"))

	serverID, host, addr, busPort, version, totalNodes, totalSlots :=
		s.GetBasicInfo()
	nodes := s.GetNodesSnapshot()
	slots := s.GetServerMetadata()

	// build a local lookup from ID → Node
	nodeMap := make(map[string]config.Node, len(nodes))
	for _, n := range nodes {
		nodeMap[n.ServerID] = n
	}

	// Basic server info
	conn.Write([]byte("---------------\n"))
	conn.Write([]byte(fmt.Sprintf(
		"Server ID: %s | Host: %s | Addr: %s | BusPort: %s\n",
		serverID, host, addr, busPort,
	)))
	conn.Write([]byte(fmt.Sprintf(
		"Cluster Version: %d | Total Nodes: %d | Total Slots: %d\n",
		version, totalNodes, totalSlots,
	)))

	// List all nodes in the cluster
	conn.Write([]byte("--- Nodes in Cluster ---\n"))
	for _, node := range nodes {
		msg := fmt.Sprintf("  ServerID: %s | Addr: %s\n", node.ServerID, node.Addr)
		conn.Write([]byte(msg))
	}

	// Slot range info
	conn.Write([]byte("--- Slot Ranges ---\n"))
	if len(slots) == 0 {
		conn.Write([]byte("  No metadata available\n"))
		return
	}

	for i, sr := range slots {
		nodeAddrs := []string{}

		if len(sr.Nodes) == 0 {
			nodeAddrs = append(nodeAddrs, "NONE")
		} else {
			for _, nodeID := range sr.Nodes {
				if nodeID == "NONE" {
					nodeAddrs = append(nodeAddrs, "NONE")
					break
				}
				node, ok := nodeMap[nodeID]
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
