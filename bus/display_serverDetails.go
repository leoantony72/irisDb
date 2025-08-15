package bus

import (
	"fmt"
	"iris/config"
	"net"
	"strings"
)

func HandleShow(conn net.Conn, s *config.Server) {
	conn.Write([]byte("---------------\n"))
	conn.Write([]byte(fmt.Sprintf("Server ID: %s | Host: %s | Addr: %s | BusPort: %s\n", s.ServerID, s.Host, s.Addr, s.BusPort)))
	conn.Write([]byte(fmt.Sprintf("Cluster Version: %d | Total Nodes: %d | Total Slots: %d\n", s.Cluster_Version, s.Nnode, s.N)))
	conn.Write([]byte("--- Nodes in Cluster ---\n"))
	for _, node := range s.Nodes {
		msg := fmt.Sprintf("  ServerID: %s | Addr: %s\n", node.ServerID, node.Addr)
		conn.Write([]byte(msg))
	}
	conn.Write([]byte("--- Slot Ranges ---\n"))
	fmt.Printf("Metadata:%v | lenNodes:%s\n", s.Metadata[2], len(s.Metadata[2].Nodes))
	for _, sr := range s.Metadata {
		nodeAddrs := []string{}
		// fmt.Printf("all nODES:%v\n", sr.Nodes)
		for _, node := range sr.Nodes {
			fmt.Printf("@@Node:%s999999\n", node)
			if node == "NONE" {
				nodeAddrs = append(nodeAddrs, "NONE")
				break
			}
			fmt.Printf("Node:%s, Working:%s\n", node, s.Nodes[node])
			nodeAddrs = append(nodeAddrs, fmt.Sprintf("%s@%s", s.Nodes[node].ServerID, s.Nodes[node].Addr))
		}
		nodesStr := strings.Join(nodeAddrs, ",")
		if nodesStr == "" {
			nodesStr = "NONE"
		}
		msg := fmt.Sprintf("  Start:%d | End:%d | MasterID: %s | Nodes: %s\n", sr.Start, sr.End, sr.MasterID, nodesStr)
		conn.Write([]byte(msg))
	}
	conn.Write([]byte("---------------\n"))
}
