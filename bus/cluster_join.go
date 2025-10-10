package bus

import (
	"fmt"
	"iris/config"
	"log"
	"net"
	"strconv"
	"strings"
)

func HandleJoin(conn net.Conn, parts []string, s *config.Server) {
	if len(parts) != 3 {
		conn.Write([]byte("ERR usage: JOIN <SERVER_ID> <PORT>\n"))
		return
	}
	ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	newServerID := parts[1]
	newServerPort := parts[2]
	newNodeAddr := net.JoinHostPort(ip, newServerPort)

	newNode := config.Node{ServerID: newServerID, Addr: newNodeAddr}

	if(!s.ReplicationValidator()){
		//current server has bootstrap problem in replication
		s.RepairReplication()
	}

	modifiedRangeIdx, startRangeForNewNode, endRangeForNewNode, newReplicaList, modifiedServerReplicaList := DetermineRange(s)

	// if len(s.Metadata[modifiedRangeIdx].Nodes) == 0 {
	// 	conn.Write([]byte("ERR: JOIN failed. Selected slot range has no assigned nodes.\n"))
	// 	return
	// }
	// modifiedNode := s.Metadata[modifiedRangeIdx].Nodes[0]
	modifiedNode := s.Nodes[s.Metadata[modifiedRangeIdx].MasterID]
	log.Printf("Joining node %s (addr: %s). Selected slot range %d-%d from node %s (addr: %s) to split.",
		newNode.ServerID, newNode.Addr, startRangeForNewNode, endRangeForNewNode, modifiedNode.ServerID, modifiedNode.Addr)

	mid, prepareSuccess, err := Prepare(&newNode, startRangeForNewNode, endRangeForNewNode, modifiedNode, s, modifiedServerReplicaList, newReplicaList)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERR: JOIN PREPARE(ERR) failed: %s\n", err.Error())))
		log.Printf("Prepare err: %s", err.Error())
		return
	}
	if !prepareSuccess {
		conn.Write([]byte("ERR: JOIN PREPARE failed\n"))
		log.Println("Prepare failed for unknown reason (prepareSuccess was false)")
		return
	}
	log.Printf("PREPARE successful for new node %s, MessageID: %s", newNode.ServerID, mid)

	commitSuccess, err := commit(mid, s)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERR: JOIN COMMIT(ERR) failed: %s\n", err.Error())))
		log.Printf("Commit err: %s", err.Error())
		return
	}
	if !commitSuccess {
		delete(s.Prepared, mid)
		conn.Write([]byte("ERR: JOIN COMMIT failed\n"))
		log.Println("Commit failed for unknown reason (commitSuccess was false)")
		return
	}
	log.Printf("COMMIT successful for MessageID: %s", mid)

	log.Printf("Cluster metadata updated. New version: %d, Nodes: %d, Slot Ranges: %d",
		s.Cluster_Version, len(s.Nodes), len(s.Metadata))

	conn.Write([]byte("CLUSTER_METADATA_BEGIN\n"))
	for _, slot := range s.Metadata {
		var nodeInfos []string
		if len(slot.Nodes) > 0 {
			for _, node := range slot.Nodes {
				nodeInfo := fmt.Sprintf("%s@%s", s.Nodes[node].ServerID, s.Nodes[node].Addr)
				nodeInfos = append(nodeInfos, nodeInfo)
			}
		} else {
			nodeInfos = append(nodeInfos, "NONE")
		}

		// SLOT <Start> <End> <Node1@Addr1>,<Node2@Addr2>,... (using ALL nodes in slot.Nodes) *old
		// SLOT <Start> <End> <MasterNodeID> <Node1@Addr1>,<Node2@Addr2>,... (using ALL nodes in slot.Nodes)
		masterNode := slot.MasterID +"@"+ s.Nodes[slot.MasterID].Addr
		msg := fmt.Sprintf("SLOT %d %d %s %s\n", slot.Start, slot.End, masterNode, strings.Join(nodeInfos, ","))
		fmt.Printf("Cluster Messages:%s\n", msg)
		conn.Write([]byte(msg))
	}
	conn.Write([]byte("CLUSTER_METADATA_END\n"))

	msg := fmt.Sprintf("JOIN_SUCCESS %s %s", strconv.Itoa(int(startRangeForNewNode)), strconv.Itoa(int(endRangeForNewNode)))
	conn.Write([]byte(msg + "\n"))
	log.Printf("JOIN command completed successfully for %s", newServerID)
}
