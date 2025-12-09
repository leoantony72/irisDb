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

// PREPARE <MessageID> <TargetNodeID> <TargetNodeAddr> <Start> <End> <ModifiedNodeID> <ModifiedReplicas> <TargetReplicas>
func HandlePrepare(conn net.Conn, parts []string, s *config.Server) {
	if len(parts) != 9 {
		conn.Write([]byte("ERR: Usage: PREPARE <MessageID> <TargetNodeID> <TargetNodeAddr> <Start> <End> <ModifiedNodeID> <ModifiedReplicas> <TargetReplicas>\n"))
		return
	}

	messageID := parts[1]
	targetNodeID := parts[2]
	targetNodeAddr := parts[3]

	start, err := utils.ParseUint16(parts[4])
	if err != nil {
		conn.Write([]byte("ERR: invalid START value\n"))
		return
	}
	end, err := utils.ParseUint16(parts[5])
	if err != nil {
		conn.Write([]byte("ERR: invalid END value\n"))
		return
	}

	modifiedNodeID := parts[6]

	var Mreplicas []string
	modifiedNode_replicaList := parts[7]
	if modifiedNode_replicaList != "NONE" {
		Mreplicas = strings.Split(modifiedNode_replicaList, ",")
	}

	var Treplicas []string
	targetNode_replicaList := parts[8]
	if targetNode_replicaList != "NONE" {
		Treplicas = strings.Split(targetNode_replicaList, ",")
	}

	// SourceNodeID is this server (the receiver of PREPARE)
	if err := s.AcceptPrepare(
		messageID,
		s.ServerID,
		targetNodeID,
		targetNodeAddr,
		start,
		end,
		modifiedNodeID,
		Mreplicas,
		Treplicas,
	); err != nil {
		log.Printf("PREPARE %s rejected: %v", messageID, err)
		conn.Write([]byte(fmt.Sprintf("ERR: %v\n", err)))
		return
	}

	log.Printf("PREPARE message %s received and accepted.", messageID)
	msg := fmt.Sprintf("PREPARE SUCCESS %s", messageID)
	conn.Write([]byte(msg + "\n"))
}

// sends a PREPARE message to all other nodes in the cluster.
// also adds the message to coordinating server's Prepared map.
func Prepare(
	newNode *config.Node,
	start, end uint16,
	modifiedNode *config.Node,
	s *config.Server,
	modifiedNode_replica_list []string,
	targetNode_replica_list []string,
) (string, bool, error) {
	messageID := uuid.New().String()

	var modifiedReplicaList string
	if len(modifiedNode_replica_list) == 0 {
		modifiedReplicaList = "NONE"
	} else {
		modifiedReplicaList = strings.Join(modifiedNode_replica_list, ",")
	}

	var targetReplicaList string
	if len(targetNode_replica_list) == 0 {
		targetReplicaList = "NONE"
	} else {
		targetReplicaList = strings.Join(targetNode_replica_list, ",")
	}

	message := fmt.Sprintf(
		"PREPARE %s %s %s %d %d %s %s %s\n",
		messageID,
		newNode.ServerID,
		newNode.Addr,
		start,
		end,
		modifiedNode.ServerID,
		modifiedReplicaList,
		targetReplicaList,
	)
	expectedResp := fmt.Sprintf("PREPARE SUCCESS %s", messageID)

	// Update the coordinating server's own prepared state via config method
	if err := s.AddLocalPrepare(
		messageID,
		newNode.ServerID,
		newNode.Addr,
		start,
		end,
		modifiedNode.ServerID,
		modifiedNode_replica_list,
		targetNode_replica_list,
	); err != nil {
		log.Printf("Local prepared state update failed for MessageID %s: %v", messageID, err)
		return "", false, fmt.Errorf("local prepare failed: %w", err)
	}
	log.Printf("Local prepared state for MessageID %s updated on coordinator.", messageID)

	nodes := s.GetNodesSnapshot()

	for _, node := range nodes {
		if node.ServerID == s.ServerID {
			continue
		}

		busport, err := utils.BumpPort(node.Addr, 10000)
		if err != nil {
			log.Printf("WARN: Failed to derive bus port for node %s (%s): %v", node.ServerID, node.Addr, err)
			return "", false, fmt.Errorf("failed to derive bus port for peer(ID:%s): %w", node.ServerID, err)
		}

		log.Printf("Sending PREPARE %s to %s via bus port %s", messageID, node.ServerID, busport)
		conn, err := net.Dial("tcp", busport)
		if err != nil {
			return "", false, fmt.Errorf("failed to connect to peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}
		conn.SetDeadline(time.Now().Add(15 * time.Second))

		if _, err = conn.Write([]byte(message)); err != nil {
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
			return "", false, fmt.Errorf(
				"unexpected response from peer(ID:%s) %s: got %q, expected %q",
				node.ServerID, busport, strings.TrimSpace(resp), expectedResp,
			)
		}
		log.Printf("Received successful PREPARE response from %s for %s.", node.ServerID, messageID)
	}

	return messageID, true, nil
}
