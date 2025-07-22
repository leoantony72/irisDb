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
	for _, sr := range s.Metadata {
		nodeAddrs := []string{}
		for _, node := range sr.Nodes {
			nodeAddrs = append(nodeAddrs, fmt.Sprintf("%s@%s", node.ServerID, node.Addr))
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
