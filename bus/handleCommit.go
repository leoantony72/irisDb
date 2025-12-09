package bus

import (
	"fmt"
	"iris/config"
	"iris/engine"
	"iris/utils"
	"log"
	"net"
	"strings"
	"time"
)

// HandleCommit is the bus handler for a remote COMMIT request.
// It only parses args, calls into config.Server, and writes a response.
func HandleCommit(conn net.Conn, parts []string, s *config.Server) {
	if len(parts) != 2 {
		_, _ = conn.Write([]byte("ERR: Not Enough Arguments\n"))
		return
	}

	messageID := parts[1]

	if err := s.ApplyCommitByID(messageID); err != nil {
		log.Printf("COMMIT failed for message ID %s: %v", messageID, err)
		_, _ = conn.Write([]byte(fmt.Sprintf("ERR: COMMIT failed: %v\n", err)))
		return
	}

	_, _ = conn.Write([]byte("COMMIT SUCCESS\n"))
}

// / sends a COMMIT message to all other nodes in the cluster.
func commit(mid string, s *config.Server, db *engine.Engine) (bool, error) {
	msg := fmt.Sprintf("COMMIT %s\n", mid)

	// Snapshot peers from config package
	peers := s.GetCommitPeers()

	for _, peer := range peers {
		busport, err := utils.BumpPort(peer.Addr, 10000)
		if err != nil {
			log.Printf("WARN: Failed to derive bus port for node %s (%s): %v",
				peer.ServerID, peer.Addr, err)
			return false, fmt.Errorf("failed to derive bus port for peer(ID:%s): %w", peer.ServerID, err)
		}

		log.Printf("Sending COMMIT %s to %s via bus port %s", mid, peer.ServerID, busport)
		conn, err := net.DialTimeout("tcp", busport, 10*time.Second)
		if err != nil {
			return false, fmt.Errorf("failed to connect to peer(ID:%s) %s: %w", peer.ServerID, busport, err)
		}

		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

		if _, err = conn.Write([]byte(msg)); err != nil {
			conn.Close()
			return false, fmt.Errorf("failed to write to peer(ID:%s) %s: %w", peer.ServerID, busport, err)
		}

		response := make([]byte, 1024)
		n, err := conn.Read(response)
		conn.Close()
		if err != nil {
			return false, fmt.Errorf("failed to read from peer(ID:%s) %s: %w", peer.ServerID, busport, err)
		}

		respStr := strings.TrimSpace(string(response[:n]))
		if respStr != "COMMIT SUCCESS" {
			return false, fmt.Errorf("unexpected response from peer(ID:%s) %s: %s", peer.ServerID, busport, respStr)
		}
		log.Printf("Received successful COMMIT response from %s for %s.", peer.ServerID, mid)
	}

	// Apply commit changes locally on the coordinating server.
	if err := s.ApplyCommitByID(mid); err != nil {
		log.Printf("Local commit failed for MessageID %s: %s", mid, err.Error())
		return false, fmt.Errorf("local commit failed: %s", err.Error())
	}

	log.Printf("Local commit for MessageID %s succeeded.", mid)
	db.SaveServerMetadata(s)

	return true, nil
}
