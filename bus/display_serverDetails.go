package bus

import (
	"fmt"
	"iris/config"
	"log"
	"net"
	"strings"
)

func (b *Bus) HandleShow(conn net.Conn) {
	serverID, host, addr, busPort, version, totalNodes, totalSlots :=
		b.server.GetBasicInfo()
	nodes := b.server.GetNodesSnapshot()
	slots := b.server.GetServerMetadata()

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
		serverID, host, addr, busPort, b.server.MasterNodeID, selfScore,
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
			rangeNodeIDs := make([]string, 0, len(sr.Nodes)+1)
			if sr.MasterID != "" {
				rangeNodeIDs = append(rangeNodeIDs, sr.MasterID)
			}
			rangeNodeIDs = append(rangeNodeIDs, sr.Nodes...)

			seen := make(map[string]bool, len(rangeNodeIDs))
			nodeAddrs := make([]string, 0, len(rangeNodeIDs))
			for _, nodeID := range rangeNodeIDs {
				if nodeID == "" || nodeID == "NONE" || seen[nodeID] {
					continue
				}
				seen[nodeID] = true
				node, ok := nodeMap[nodeID]
				if !ok {
					nodeAddrs = append(nodeAddrs, fmt.Sprintf("UNKNOWN(%s)", nodeID))
					continue
				}
				nodeAddrs = append(nodeAddrs, fmt.Sprintf("%s@%s", node.ServerID, node.Addr))
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

	// Gossip messages sent/received
	response.WriteString("--- Gossip Sent ---\n")
	if b.gossip == nil {
		response.WriteString("  Gossip subsystem not initialized\n")
	} else {
		sent := b.gossip.GetSent()
		if len(sent) == 0 {
			response.WriteString("  None\n")
		} else {
			for _, s := range sent {
				response.WriteString("  " + s + "\n")
			}
		}
	}

	response.WriteString("--- Gossip Received ---\n")
	if b.gossip == nil {
		response.WriteString("  Gossip subsystem not initialized\n")
	} else {
		recv := b.gossip.GetRecv()
		if len(recv) == 0 {
			response.WriteString("  None\n")
		} else {
			for _, r := range recv {
				response.WriteString("  " + r + "\n")
			}
		}
	}
	if _, err := conn.Write([]byte(response.String())); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}
