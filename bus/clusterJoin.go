package bus

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"iris/config"
	"iris/engine"
	"log"
	"net"
	"strings"
)

func HandleJoin(conn net.Conn, parts []string, s *config.Server, db *engine.Engine) {
	if len(parts) != 5 {
		conn.Write([]byte("ERR usage: JOIN <SERVER_ID> <PORT> <RESOURCE_SCORE> <GROUP>\n"))
		return
	}
	serverID := parts[1]
	group := parts[4]

	if s.HasNode(serverID) {
		log.Printf("🧀SERVER ID:%s REJOINED SUCESSFULLY", serverID)

		// ✅ UPDATE the rejoined node's information
		ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		if ip == "::1" || ip == "127.0.0.1" || ip == "localhost" {
			ip = "localhost"
		}
		newServerPort := parts[2]
		resourceScoreStr := parts[3]
		resourceScore, err := parseResourceScore(resourceScoreStr)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("ERR invalid resource score: %s\n", err.Error())))
			return
		}
		newNodeAddr := net.JoinHostPort(ip, newServerPort)

		// ✅ Use public method to update node
		s.UpdateRejoiningNode(serverID, newNodeAddr, group, resourceScore)

		// Send metadata to rejoining node
		err = sendClusterMetadata(conn, s)
		if err != nil {
			log.Println("Error sending cluster metadata:", err)
			return
		}

		rangeIndices := s.FindRangeIndexByServerID(serverID)
		sendReJoinSuccess(s, conn, serverID, rangeIndices, int(s.GetClusterVersion()))
		return
	}

	ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())

	// Normalize loopback addresses to 127.0.0.1 for IPv4-only consistency
	if ip == "::1" || ip == "127.0.0.1" || ip == "localhost" {
		ip = "localhost"
	}

	newServerID := parts[1]
	newServerPort := parts[2]
	resourceScoreStr := parts[3]
	resourceScore, err := parseResourceScore(resourceScoreStr)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERR invalid resource score: %s\n", err.Error())))
		return
	}
	newNodeAddr := net.JoinHostPort(ip, newServerPort)

	newNode := config.Node{ServerID: newServerID, Addr: newNodeAddr, ResourceScore: resourceScore, Group: group}

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

	if err := sendClusterMetadata(conn, s); err != nil {
		log.Printf("[ERROR] sendClusterMetadata failed: %v", err)
		_, _ = conn.Write([]byte("ERR failed to send cluster metadata\n"))
		return
	}

	err = sendJoinSuccess(conn, newServerID, int(startRangeForNewNode), int(endRangeForNewNode), int(s.GetClusterVersion()))
	if err != nil {
		log.Println("Error:", err)
	}
}

func sendJoinSuccess(conn net.Conn, newServerID string, startRange, endRange int, cluster_version int) error {
	msg := fmt.Sprintf("JOIN_SUCCESS %d %d %d", startRange, endRange, cluster_version)
	if _, err := conn.Write([]byte(msg + "\n")); err != nil {
		return fmt.Errorf("failed to send JOIN_SUCCESS to %s: %w", newServerID, err)
	}

	log.Printf("JOIN command completed successfully for %s", newServerID)
	return nil
}

func sendReJoinSuccess(s *config.Server, conn net.Conn, ServerID string, ranges []int, cluster_version int) error {

	var b strings.Builder

	fmt.Fprintf(&b, "JOIN_SUCCESS %d", len(ranges))

	slotRanges := s.GetSlotRangesByIndices(ranges)
	for _, r := range slotRanges {
		fmt.Fprintf(&b, " %d %d", r.Start, r.End)
	}
	fmt.Fprintf(&b, " %d", cluster_version)

	if _, err := conn.Write([]byte(b.String() + "\n")); err != nil {
		return fmt.Errorf("failed to send REJOIN")
	}

	log.Printf("JOIN command completed successfully for %s", ServerID)
	return nil
}

func sendClusterMetadata(conn net.Conn, s *config.Server) error {
	snap := s.BuildClusterSnapshot()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&snap); err != nil {
		return fmt.Errorf("sendClusterMetadata: gob encode snapshot: %w", err)
	}

	payload := buf.Bytes()

	// Header tells the client exactly how many bytes to read for the gob payload.
	if _, err := conn.Write([]byte(fmt.Sprintf("CLUSTER_SNAPSHOT %d\n", len(payload)))); err != nil {
		return fmt.Errorf("sendClusterMetadata: write header: %w", err)
	}

	if _, err := conn.Write(payload); err != nil {
		return fmt.Errorf("sendClusterMetadata: write payload: %w", err)
	}

	return nil
}

func parseResourceScore(scoreStr string) (float64, error) {
	var score float64
	_, err := fmt.Sscanf(scoreStr, "%f", &score)
	if err != nil {
		return 0, fmt.Errorf("invalid resource score format: %w", err)
	}
	return score, nil
}
