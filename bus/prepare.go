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
	if len(parts) != 9 {
		conn.Write([]byte("ERR: Usage: PREPARE <MessageID> <TargetNodeID> <TargetNodeAddr> <Start> <End> <ModifiedNodeID> <ModifiedReplicas> <TargetReplicas>\n"))
		return
	}
	// ... parsing of message parts ...
	messageID := parts[1]
	targetNodeID := parts[2]
	targetNodeAddr := parts[3]
	start, _ := utils.ParseUint16(parts[4])
	end, _ := utils.ParseUint16(parts[5])
	modifiedNodeID := parts[6]

	_, exists := s.Prepared[messageID]
	if exists {
		conn.Write([]byte(fmt.Sprintf("ERR: MessageID %s already exists.\n", messageID)))
		return
	}

	// Check ownership
	var isModifiedNodeMaster bool
	for _, sr := range s.Metadata {
		if sr.MasterID == modifiedNodeID && start >= sr.Start && end <= sr.End {
			isModifiedNodeMaster = true
			break
		}
	}

	if !isModifiedNodeMaster {
		conn.Write([]byte(fmt.Sprintf("ERR: Node %s does not control slot range %d-%d.\n", modifiedNodeID, start, end)))
		return
	}

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


	s.Prepared[messageID] = &config.PrepareMessage{
		MessageID:               messageID,
		SourceNodeID:            s.ServerID,
		TargetNodeID:            targetNodeID,
		Addr:                    targetNodeAddr,
		Start:                   start,
		End:                     end,
		ModifiedNodeID:          modifiedNodeID,
		ModifiedNodeReplicaList: Mreplicas,
		TargetNodeReplicaList:   Treplicas,
	}
	log.Printf("PREPARE message %s received and accepted.", messageID)

	msg := fmt.Sprintf("PREPARE SUCCESS %s", parts[1])
	conn.Write([]byte(msg + "\n"))
}

// sends a PREPARE message to all other nodes in the cluster.
// also adds the message to coordinating servers PreparedMsg Map
func Prepare(newNode *config.Node, start, end uint16, modifiedNode *config.Node, s *config.Server, modifiedNode_replica_list []string, targetNode_replica_list []string) (string, bool, error) {
	messageID := uuid.New().String()
	// PREPARE MESSAGEID TargetNodeID ADDR START END ModifiedNodeID *old
	// PREPARE MESSAGEID TargetNodeID ADDR START END ModifiedNodeID <ModifiedNode_new_replicaList> <NewNode_replica_list>

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

	message := fmt.Sprintf("PREPARE %s %s %s %d %d %s %s %s\n",
		messageID, newNode.ServerID, newNode.Addr, start, end, modifiedNode.ServerID, modifiedReplicaList, targetReplicaList)
	expectedResp := fmt.Sprintf("PREPARE SUCCESS %s", messageID)

	// Update the coordinating server's own prepared state directly
	s.Prepared[messageID] = &config.PrepareMessage{
		MessageID:               messageID,
		SourceNodeID:            s.ServerID, // This server is the coordinator/source
		TargetNodeID:            newNode.ServerID,
		Addr:                    newNode.Addr,
		Start:                   start,
		End:                     end,
		ModifiedNodeID:          modifiedNode.ServerID,
		ModifiedNodeReplicaList: modifiedNode_replica_list,
		TargetNodeReplicaList:   targetNode_replica_list,
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
