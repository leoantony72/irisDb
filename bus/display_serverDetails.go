package bus

import (
	"fmt"
	"iris/config"
	"log"
	"net"
	"strings"
)

func HandleShow(conn net.Conn, s *config.Server) {
	serverID, host, addr, busPort, version, totalNodes, totalSlots :=
		s.GetBasicInfo()
	nodes := s.GetNodesSnapshot()
	slots := s.GetServerMetadata()

	// build a local lookup from ID → Node
	nodeMap := make(map[string]config.Node, len(nodes))
	for _, n := range nodes {
		nodeMap[n.ServerID] = n
	}

	// Current server score (prefer node snapshot; fallback 0)
	selfScore := 0.0
	if self, ok := nodeMap[serverID]; ok {
		selfScore = self.ResourceScore
	}

	var response strings.Builder
	response.WriteString("---------------\n")

	// Basic server info
	response.WriteString(fmt.Sprintf(
		"Server ID: %s | Host: %s | Addr: %s | BusPort: %s | MasterNodeID: %s | ResourceScore: %.6f\n",
		serverID, host, addr, busPort, s.MasterNodeID, selfScore,
	))
	response.WriteString(fmt.Sprintf(
		"Cluster Version: %d | Total Nodes: %d | Total Slots: %d\n",
		version, totalNodes, totalSlots,
	))

	// List all nodes in the cluster (with ResourceScore)
	response.WriteString("--- Nodes in Cluster ---\n")
	for _, node := range nodes {
		response.WriteString(fmt.Sprintf(
			"  ServerID: %s | Addr: %s | ResourceScore: %.6f\n",
			node.ServerID, node.Addr, node.ResourceScore,
		))
	}

	// Slot range info
	response.WriteString("--- Slot Ranges ---\n")
	if len(slots) == 0 {
		response.WriteString("  No metadata available\n")
	} else {
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

			response.WriteString(fmt.Sprintf(
				"  [%d] Start:%d | End:%d | MasterID: %s | Nodes: %s\n",
				i, sr.Start, sr.End, sr.MasterID, nodesStr,
			))
		}
	}

	response.WriteString("---------------\n")
	if _, err := conn.Write([]byte(response.String())); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}
