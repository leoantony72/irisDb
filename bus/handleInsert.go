package bus

import (
	"bufio"
	"fmt"
	"iris/config"
	"iris/engine"
	"iris/utils"
	"net"
	"strings"

	"github.com/cockroachdb/pebble"
)

// MESSAGE FORMAT: INS KEY VALUE
// RESPONSE FORMAT: ACK KEY
func HandleINS(conn net.Conn, parts []string, server *config.Server, db *engine.Engine) {
	if len(parts) < 3 {
		conn.Write([]byte("Err: Incorrect Format: INS KEY VALUE"))
		return
	}
	fmt.Printf("RECEIVED FORWARD REQ: KEY//%s\n", parts[1])

	hash := utils.CalculateCRC16([]byte(parts[1]))
	master_slot := server.FindNodeIdx(hash % server.N)
	sr, ok := server.GetSlotRangeByIndex(master_slot)
	if !ok {
		// handle error (range not found)
		conn.Write([]byte("ERR Internal Error\n"))
		return
	}
	g := sr.MasterID

	if g != server.ServerID {
		conn.Write([]byte("ERR NOT MASTER NODE\n"))
		return
	}

	// db.Set(parts[1], parts[2], conn)
	db.Db.Set([]byte(parts[1]), []byte(parts[2]), pebble.Sync)
	//should handle the replication as well.
	replicaNodes := sr.Nodes
	for _, replicaNodeID := range replicaNodes {
		replicaNode, exists := server.GetConnectedNodeData(replicaNodeID)
		if !exists {
			continue // Skip if the replica node is not found
		}
		busaddr, _ := utils.BumpPort(replicaNode.Addr, 10000)
		Sconn, err := net.Dial("tcp", busaddr)
		if err != nil {
			fmt.Printf("ERR: HandleINS: %s\n", err.Error())
			continue
		}
		defer Sconn.Close()

		insCmd := fmt.Sprintf("REP %s %s\n", parts[1], parts[2])
		Sconn.Write([]byte(insCmd))

		expectedResponse := "ACK REP"
		reader := bufio.NewReader(Sconn)
		resp, _ := reader.ReadString('\n')

		resp = strings.TrimSpace(resp)
		if resp != expectedResponse {
			fmt.Printf("ERR: HandleINS: Expected %s, got %s\n", expectedResponse, resp)
		}
	}
	conn.Write([]byte("ACK INS\n"))
}
