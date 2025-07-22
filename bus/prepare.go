package bus

import (
	"bufio"
	"fmt"
	"iris/config"
	"iris/utils"
	"log"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
)

func HandlePrepare(conn net.Conn, parts []string, s *config.Server) {
	// PREPARE <MessageID> <TargetNodeID> <TargetNodeAddr> <Start> <End> <ModifiedNodeID>
	if len(parts) != 7 {
		conn.Write([]byte("ERR: Usage: PREPARE <MessageID> <TargetNodeID> <TargetNodeAddr> <Start> <End> <ModifiedNodeID>\n"))
		return
	}
	messageID := parts[1]
	targetNodeID := parts[2]
	targetNodeAddr := parts[3]
	start, err := utils.ParseUint16(parts[4])
	if err != nil {
		conn.Write([]byte("ERR: Couldn't Parse StartRange\n"))
		return
	}
	end, err := utils.ParseUint16(parts[5])
	if err != nil {
		conn.Write([]byte("ERR: Couldn't Parse EndRange\n"))
		return
	}
	modifiedNodeID := parts[6]

	// --- PREPARE phase logic on participating nodes ---
	// 1. Check for conflicts: Ensure the node is not currently involved in another migration
	//    that overlaps with the proposed range.
	if _, exists := s.Prepared[messageID]; exists {
		conn.Write([]byte(fmt.Sprintf("ERR: MessageID %s already exists in prepared state.\n", messageID)))
		return
	}

	// 2. Check if the 'ModifiedNodeID' actually owns the range it's supposed to give up.
	//    This is crucial to prevent unauthorized modifications.
	var isModifiedNodeMaster bool
	for _, sr := range s.Metadata {
		if len(sr.Nodes) > 0 && sr.Nodes[0].ServerID == modifiedNodeID { // Assuming first node in list is master
			if start >= sr.Start && end <= sr.End {
				// This server is being asked to modify a range it (or its designated master) owns.
				isModifiedNodeMaster = true
				break
			}
		}
	}

	if !isModifiedNodeMaster {
		conn.Write([]byte(fmt.Sprintf("ERR: PREPARE failed. Node %s does not control slot range %d-%d or range is invalid.\n", modifiedNodeID, start, end)))
		log.Printf("PREPARE failed for %s: Node %s does not control slot range %d-%d. Current metadata: %+v", messageID, modifiedNodeID, start, end, s.Metadata)
		return
	}

	// Store the prepare message in the 'Prepared' map
	s.Prepared[messageID] = &config.PrepareMessage{
		MessageID:      messageID,
		SourceNodeID:   s.ServerID, // This server is acting as the source for the prepare.
		TargetNodeID:   targetNodeID,
		Addr:           targetNodeAddr, // Addr of the new Node
		Start:          start,
		End:            end,
		ModifiedNodeID: modifiedNodeID,
	}
	log.Printf("PREPARE message %s received and accepted. State stored locally.", messageID)

	msg := fmt.Sprintf("PREPARE SUCCESS %s", parts[1])
	conn.Write([]byte(msg + "\n"))
}

// sends a PREPARE message to all other nodes in the cluster.
// also adds the message to coordinating servers PreparedMsg Map
func Prepare(newNode *config.Node, start, end uint16, modifiedNode *config.Node, s *config.Server) (string, bool, error) {
	messageID := uuid.New().String()
	// PREPARE MESSAGEID TargetNodeID ADDR START END ModifiedNodeID
	message := fmt.Sprintf("PREPARE %s %s %s %d %d %s\n",
		messageID, newNode.ServerID, newNode.Addr, start, end, modifiedNode.ServerID)
	expectedResp := fmt.Sprintf("PREPARE SUCCESS %s", messageID)

	// Update the coordinating server's own prepared state directly
	s.Prepared[messageID] = &config.PrepareMessage{
		MessageID:      messageID,
		SourceNodeID:   s.ServerID, // This server is the coordinator/source
		TargetNodeID:   newNode.ServerID,
		Addr:           newNode.Addr,
		Start:          start,
		End:            end,
		ModifiedNodeID: modifiedNode.ServerID,
	}
	log.Printf("Local prepared state for MessageID %s updated on coordinator.", messageID)

	// Send to other nodes
	for _, node := range s.Nodes {
		if node.ServerID == s.ServerID {
			continue
		}

		busport, err := utils.BumpPort(node.Addr, 10000)
		if err != nil {
			log.Printf("WARN: Failed to derive bus port for node %s (%s): %v", node.ServerID, node.Addr, err)
			return "", false, fmt.Errorf("failed to derive bus port for peer(ID:%s): %w", node.ServerID, err)
		}

		log.Printf("Sending PREPARE %s to %s via bus port %s", messageID, node.ServerID, busport)
		conn, err := net.DialTimeout("tcp", busport, 2*time.Second)
		if err != nil {
			return "", false, fmt.Errorf("failed to connect to peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))

		_, err = conn.Write([]byte(message))
		if err != nil {
			conn.Close()
			return "", false, fmt.Errorf("failed to write to peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}

		reader := bufio.NewReader(conn)
		resp, err := reader.ReadString('\n')
		conn.Close()
		if err != nil {
			return "", false, fmt.Errorf("failed to read response from peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}

		if strings.TrimSpace(resp) != expectedResp {
			return "", false, fmt.Errorf("unexpected response from peer(ID:%s) %s: got %q, expected %q",
				node.ServerID, busport, strings.TrimSpace(resp), expectedResp)
		}
		log.Printf("Received successful PREPARE response from %s for %s.", node.ServerID, messageID)
	}

	return messageID, true, nil
}
