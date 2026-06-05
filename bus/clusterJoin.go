package bus

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"iris/config"
	"log"
	"net"
	"strings"
)

func (b *Bus) HandleJoin(conn net.Conn, parts []string) {
	if len(parts) != 5 {
		conn.Write([]byte("ERR usage: JOIN <SERVER_ID> <PORT> <RESOURCE_SCORE> <GROUP>\n"))
		return
	}
	serverID := parts[1]
	group := parts[4]

	if b.server.HasNode(serverID) {
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

		// Use public method to update node
		b.server.UpdateRejoiningNode(serverID, newNodeAddr, group, resourceScore)

		// Send metadata to rejoining node
		err = b.sendClusterMetadata(conn)
		if err != nil {
			log.Println("Error sending cluster metadata:", err)
			return
		}

		rangeIndices := b.server.FindRangeIndexByServerID(serverID)
		b.sendReJoinSuccess(conn, serverID, rangeIndices, int(b.server.GetClusterVersion()))
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

	modifiedRangeIdx, startRangeForNewNode, endRangeForNewNode, newReplicaList, modifiedServerReplicaList := b.server.DetermineRange()

	modifiedNode, ok := b.server.GetMasterNodeForRangeIdx(modifiedRangeIdx)
	if !ok {
		log.Println("Master node not found for modified range")
		return
	}
	log.Printf("Joining node %s (addr: %s). Selected slot range %d-%d from node %s (addr: %s) to split.",
		newNode.ServerID, newNode.Addr, startRangeForNewNode, endRangeForNewNode, modifiedNode.ServerID, modifiedNode.Addr)

	mid, prepareSuccess, err := Prepare(&newNode, startRangeForNewNode, endRangeForNewNode, modifiedNode, b.server, modifiedServerReplicaList, newReplicaList)
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

	commitSuccess, err := b.commit(mid)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("ERR: JOIN COMMIT(ERR) failed: %s\n", err.Error())))
		log.Printf("Commit err: %s", err.Error())
		return
	}
	if !commitSuccess {
		b.server.DeletePrepared(mid)
		conn.Write([]byte("ERR: JOIN COMMIT failed\n"))
		log.Println("Commit failed for unknown reason (commitSuccess was false)")
		return
	}
	log.Printf("COMMIT successful for MessageID: %s", mid)

	log.Printf("Cluster metadata updated. New version: %d, Nodes: %d, Slot Ranges: %d",
		b.server.GetClusterVersion(), b.server.GetNodeCount(), b.server.GetSlotRangeCount())

	if err := b.sendClusterMetadata(conn); err != nil {
		log.Printf("[ERROR] sendClusterMetadata failed: %v", err)
		_, _ = conn.Write([]byte("ERR failed to send cluster metadata\n"))
		return
	}

	err = sendJoinSuccess(conn, newServerID, int(startRangeForNewNode), int(endRangeForNewNode), int(b.server.GetClusterVersion()))
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

func(b *Bus) sendReJoinSuccess(conn net.Conn, ServerID string, ranges []int, cluster_version int) error {

	var build strings.Builder

	fmt.Fprintf(&build, "JOIN_SUCCESS %d", len(ranges))

	slotRanges := b.server.GetSlotRangesByIndices(ranges)
	for _, r := range slotRanges {
		fmt.Fprintf(&build, " %d %d", r.Start, r.End)
	}
	fmt.Fprintf(&build, " %d", cluster_version)

	if _, err := conn.Write([]byte(build.String() + "\n")); err != nil {
		return fmt.Errorf("failed to send REJOIN")
	}

	log.Printf("JOIN command completed successfully for %s", ServerID)
	return nil
}

func(b *Bus) sendClusterMetadata(conn net.Conn) error {
	snap := b.server.BuildClusterSnapshot()

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
