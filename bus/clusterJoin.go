package bus

import (
	"fmt"
	"iris/config"
	"iris/engine"
	"log"
	"net"
	"strings"
)

func HandleJoin(conn net.Conn, parts []string, s *config.Server, db *engine.Engine) {
	if len(parts) != 3 {
		conn.Write([]byte("ERR usage: JOIN <SERVER_ID> <PORT>\n"))
		return
	}
	serverID := parts[1]
	if s.HasNode(serverID) {
		log.Printf("🧀SERVER ID:%s REJOINED SUCESSFULLY", serverID)
		err := sendClusterMetadata(conn, s)
		if err != nil {
			log.Println("Error sending cluster metadata:", err)
			return
		}

		rangeIndices := s.FindRangeIndexByServerID(serverID)
		sendReJoinSuccess(s, conn, serverID, rangeIndices)
		return
	}
	ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	newServerID := parts[1]
	newServerPort := parts[2]
	newNodeAddr := net.JoinHostPort(ip, newServerPort)

	newNode := config.Node{ServerID: newServerID, Addr: newNodeAddr}

	modifiedRangeIdx, startRangeForNewNode, endRangeForNewNode, newReplicaList, modifiedServerReplicaList := s.DetermineRange()

	modifiedNode, ok := s.GetMasterNodeForRangeIdx(modifiedRangeIdx)
	if !ok {
		log.Println("Master node not found for modified range")
		return
	}
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

	commitSuccess, err := commit(mid, s, db)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERR: JOIN COMMIT(ERR) failed: %s\n", err.Error())))
		log.Printf("Commit err: %s", err.Error())
		return
	}
	if !commitSuccess {
		s.DeletePrepared(mid)
		conn.Write([]byte("ERR: JOIN COMMIT failed\n"))
		log.Println("Commit failed for unknown reason (commitSuccess was false)")
		return
	}
	log.Printf("COMMIT successful for MessageID: %s", mid)

	log.Printf("Cluster metadata updated. New version: %d, Nodes: %d, Slot Ranges: %d",
		s.GetClusterVersion(), s.GetNodeCount(), s.GetSlotRangeCount())

	err = sendClusterMetadata(conn, s)
	if err != nil {
		log.Println("Error sending cluster metadata:", err)
	}

	err = sendJoinSuccess(conn, newServerID, int(startRangeForNewNode), int(endRangeForNewNode))
	if err != nil {
		log.Println("Error:", err)
	}
}

func sendJoinSuccess(conn net.Conn, newServerID string, startRange, endRange int) error {
	msg := fmt.Sprintf("JOIN_SUCCESS %d %d", startRange, endRange)
	if _, err := conn.Write([]byte(msg + "\n")); err != nil {
		return fmt.Errorf("failed to send JOIN_SUCCESS to %s: %w", newServerID, err)
	}

	log.Printf("JOIN command completed successfully for %s", newServerID)
	return nil
}

func sendReJoinSuccess(s *config.Server, conn net.Conn, ServerID string, ranges []int) error {

	var b strings.Builder

	fmt.Fprintf(&b, "JOIN_SUCCESS %d", len(ranges))

	slotRanges := s.GetSlotRangesByIndices(ranges)
	for _, r := range slotRanges {
		fmt.Fprintf(&b, " %d %d", r.Start, r.End)
	}

	if _, err := conn.Write([]byte(b.String() + "\n")); err != nil {
		return fmt.Errorf("failed to send REJOIN")
	}

	log.Printf("JOIN command completed successfully for %s", ServerID)
	return nil
}

func sendClusterMetadata(conn net.Conn, s *config.Server) error {
	// Start of metadata transmission
	_, err := conn.Write([]byte("CLUSTER_METADATA_BEGIN\n"))
	if err != nil {
		return fmt.Errorf("failed to write cluster metadata start: %w", err)
	}
	meta := s.GetServerMetadata()
	for _, slot := range meta {
		var nodeInfos []string

		if len(slot.Nodes) > 0 {
			for _, node := range slot.Nodes {
				n, _ := s.GetConnectedNodeData(node)
				nodeInfo := fmt.Sprintf("%s@%s", n.ServerID, n.Addr)
				nodeInfos = append(nodeInfos, nodeInfo)
			}
		} else {
			nodeInfos = append(nodeInfos, "NONE")
		}

		// Build master node info
		Mn, ok := s.GetConnectedNodeData(slot.MasterID)
		if !ok{
			return fmt.Errorf("failed to write slot info: %s", ok)
		}
		masterNode := slot.MasterID + "@" + Mn.Addr

		// Format: SLOT <Start> <End> <MasterNodeID> <Node1@Addr1>,<Node2@Addr2>,...
		msg := fmt.Sprintf("SLOT %d %d %s %s\n", slot.Start, slot.End, masterNode, strings.Join(nodeInfos, ","))

		fmt.Printf("Cluster Message: %s\n", msg)

		_, err = conn.Write([]byte(msg))
		if err != nil {
			return fmt.Errorf("failed to write slot info: %w", err)
		}

	}
	_, err = conn.Write([]byte("CLUSTER_METADATA_END\n"))
	if err != nil {
		return fmt.Errorf("failed to write cluster metadata start: %w", err)
	}

	return nil
}
