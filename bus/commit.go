package bus

import (
	"fmt"
	"iris/config"
	"iris/utils"
	"log"
	"net"
	"sort"
	"strings"
	"time"
)

func HandleCommit(conn net.Conn, parts []string, s *config.Server) {
	if len(parts) != 2 {
		conn.Write([]byte("ERR: Not Enough Arguments\n"))
		return
	}

	messageID := parts[1]
	preparedMsg, exists := s.Prepared[messageID]
	if !exists {
		conn.Write([]byte("ERR: MessageID doesn't exists in prepared state.\n"))
		log.Printf("COMMIT failed for message ID %s: not found in prepared state.", messageID)
		return
	}

	log.Printf("COMMIT message %s received. Applying changes locally.", messageID)

	// 1. Add the new node to the global nodes map
	if _, ok := s.Nodes[preparedMsg.TargetNodeID]; !ok {
		s.Nodes[preparedMsg.TargetNodeID] = &config.Node{ServerID: preparedMsg.TargetNodeID, Addr: preparedMsg.Addr}
		s.Nnode++
	}

	// 2. Find the modified SlotRange
	var modifiedRangeIdx = -1
	for i, sr := range s.Metadata {
		if sr.MasterID == preparedMsg.ModifiedNodeID &&
			preparedMsg.Start > sr.Start && preparedMsg.End == sr.End {
			modifiedRangeIdx = i
			break
		}
	}

	if modifiedRangeIdx == -1 {
		conn.Write([]byte("ERR: COMMIT failed. ModifiedNode SlotRange not found for expected split pattern.\n"))
		log.Printf("COMMIT failed for message ID %s: ModifiedNode SlotRange not found or range mismatch for upper-half split pattern. PreparedMsg: %+v, Current Metadata: %+v", messageID, preparedMsg, s.Metadata)
		return
	}

	//reference to the existing slot range that needs modification
	originalSR := s.Metadata[modifiedRangeIdx]

	// Modify the existing range for the modified node to newNodeStart range - 1
	originalSR.End = preparedMsg.Start - 1
	originalSR.Nodes = preparedMsg.ModifiedNodeReplicaList

	// Create the new slot range for the joining node
	newJoinNodeRange := &config.SlotRange{
		Start:    preparedMsg.Start,
		End:      preparedMsg.End,
		MasterID: preparedMsg.TargetNodeID,
		Nodes:    preparedMsg.TargetNodeReplicaList,
	}
	s.Metadata = append(s.Metadata, newJoinNodeRange)
	sort.Slice(s.Metadata, func(i, j int) bool {
		return s.Metadata[i].Start < s.Metadata[j].Start
	})

	newNode := &config.Node{Addr: preparedMsg.Addr, ServerID: preparedMsg.TargetNodeID}
	s.Nodes[preparedMsg.TargetNodeID] = newNode

	s.Cluster_Version++           // Increment cluster version on successful commit
	delete(s.Prepared, messageID) // Remove PrepareMessage from prepared state

	log.Printf("COMMIT %s successful. Cluster version incremented to %d. Metadata updated.", messageID, s.Cluster_Version)
	conn.Write([]byte("COMMIT SUCCESS\n"))
}

// sends a COMMIT message to all other nodes in the cluster.
func commit(mid string, s *config.Server) (bool, error) {
	msg := fmt.Sprintf("COMMIT %s\n", mid)

	for _, node := range s.Nodes {
		if node.ServerID == s.ServerID {
			continue
		}
		busport, err := utils.BumpPort(node.Addr, 10000)
		if err != nil {
			log.Printf("WARN: Failed to derive bus port for node %s (%s): %v", node.ServerID, node.Addr, err)
			return false, fmt.Errorf("failed to derive bus port for peer(ID:%s): %w", node.ServerID, err)
		}

		log.Printf("Sending COMMIT %s to %s via bus port %s", mid, node.ServerID, busport)
		conn, err := net.DialTimeout("tcp", busport, 2*time.Second)
		if err != nil {
			return false, fmt.Errorf("failed to connect to peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Write([]byte(msg))
		if err != nil {
			conn.Close()
			return false, fmt.Errorf("failed to write to peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}

		response := make([]byte, 1024)
		n, err := conn.Read(response)
		conn.Close()
		if err != nil {
			return false, fmt.Errorf("failed to read from peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}

		respStr := strings.TrimSpace(string(response[:n]))
		if respStr != "COMMIT SUCCESS" {
			return false, fmt.Errorf("unexpected response from peer(ID:%s) %s: %s", node.ServerID, busport, respStr)
		}
		log.Printf("Received successful COMMIT response from %s for %s.", node.ServerID, mid)
	}
	// Apply commit changes locally on the coordinating server
	preparedMsg, exists := s.Prepared[mid]
	if !exists {
		log.Printf("Local commit failed for MessageID %s: prepared state not found.", mid)
		return false, fmt.Errorf("local commit failed: prepared state for message ID %s not found", mid)
	}

	err := applyCommitChanges(s, preparedMsg)
	if err != nil {
		log.Printf("Local commit failed for MessageID %s: %s", mid, err.Error())
		return false, fmt.Errorf("local commit failed: %s", err.Error())
	}
	log.Printf("Local commit for MessageID %s succeeded.", mid)
	return true, nil
}
