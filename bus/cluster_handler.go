package bus

import (
	"bufio"
	"fmt"
	"iris/config"
	"iris/utils"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

func HandleClusterCommand(cmd string, conn net.Conn, s *config.Server) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		conn.Write([]byte("ERR empty command\n"))
		return
	}

	switch strings.ToUpper(parts[0]) {
	case "JOIN":
		{
			if len(parts) != 3 {
				conn.Write([]byte("ERR usage: JOIN <SERVER_ID> <PORT>\n"))
				return
			}
			ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
			newServerID := parts[1]
			newServerPort := parts[2]
			newNodeAddr := net.JoinHostPort(ip, newServerPort)

			newNode := config.Node{ServerID: newServerID, Addr: newNodeAddr}

			modifiedRangeIdx, startRangeForNewNode, endRangeForNewNode := determineRange(s)

			if len(s.Metadata[modifiedRangeIdx].Nodes) == 0 {
				conn.Write([]byte("ERR: JOIN failed. Selected slot range has no assigned nodes.\n"))
				return
			}
			modifiedNode := s.Metadata[modifiedRangeIdx].Nodes[0]
			log.Printf("Joining node %s (addr: %s). Selected slot range %d-%d from node %s (addr: %s) to split.",
				newNode.ServerID, newNode.Addr, startRangeForNewNode, endRangeForNewNode, modifiedNode.ServerID, modifiedNode.Addr)

			mid, prepareSuccess, err := Prepare(&newNode, startRangeForNewNode, endRangeForNewNode, modifiedNode, s)
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
						nodeInfo := fmt.Sprintf("%s@%s", node.ServerID, node.Addr)
						nodeInfos = append(nodeInfos, nodeInfo)
					}
				} else {
					nodeInfos = append(nodeInfos, "NONE")
				}

				// SLOT <Start> <End> <Node1@Addr1>,<Node2@Addr2>,... (using ALL nodes in slot.Nodes)
				msg := fmt.Sprintf("SLOT %d %d %s\n", slot.Start, slot.End, strings.Join(nodeInfos, ","))
				conn.Write([]byte(msg))
			}
			conn.Write([]byte("CLUSTER_METADATA_END\n"))

			msg := fmt.Sprintf("JOIN_SUCCESS %s %s", strconv.Itoa(int(startRangeForNewNode)), strconv.Itoa(int(endRangeForNewNode)))
			conn.Write([]byte(msg + "\n"))
			log.Printf("JOIN command completed successfully for %s", newServerID)
		}

	case "PREPARE":
		{
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

	case "COMMIT":
		{
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
				if len(sr.Nodes) > 0 && sr.Nodes[0].ServerID == preparedMsg.ModifiedNodeID &&
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

			// Create the new slot range for the joining node
			newJoinNodeRange := &config.SlotRange{
				Start:    preparedMsg.Start,
				End:      preparedMsg.End,
				MasterID: preparedMsg.TargetNodeID,
				Nodes:    []*config.Node{s.Nodes[preparedMsg.TargetNodeID]},
			}
			s.Metadata = append(s.Metadata, newJoinNodeRange)
			sort.Slice(s.Metadata, func(i, j int) bool {
				return s.Metadata[i].Start < s.Metadata[j].Start
			})

			s.Cluster_Version++           // Increment cluster version on successful commit
			delete(s.Prepared, messageID) // Remove PrepareMessage from prepared state

			log.Printf("COMMIT %s successful. Cluster version incremented to %d. Metadata updated.", messageID, s.Cluster_Version)
			conn.Write([]byte("COMMIT SUCCESS\n"))
		}

	case "SHOW":
		{
			conn.Write([]byte("---------------\n"))
			conn.Write([]byte(fmt.Sprintf("Server ID: %s | Host: %s | Addr: %s | BusPort: %s\n", s.ServerID, s.Host, s.Addr, s.BusPort)))
			conn.Write([]byte(fmt.Sprintf("Cluster Version: %d | Total Nodes: %d | Total Slots: %d\n", s.Cluster_Version, s.Nnode, s.N)))
			conn.Write([]byte("--- Nodes in Cluster ---\n"))
			for _, node := range s.Nodes {
				msg := fmt.Sprintf("  ServerID: %s | Addr: %s\n", node.ServerID, node.Addr)
				conn.Write([]byte(msg))
			}
			conn.Write([]byte("--- Slot Ranges ---\n"))
			for _, sr := range s.Metadata {
				nodeAddrs := []string{}
				for _, node := range sr.Nodes {
					nodeAddrs = append(nodeAddrs, fmt.Sprintf("%s@%s", node.ServerID, node.Addr))
				}
				nodesStr := strings.Join(nodeAddrs, ",")
				if nodesStr == "" {
					nodesStr = "NONE"
				}
				msg := fmt.Sprintf("  Start:%d | End:%d | MasterID: %s | Nodes: %s\n", sr.Start, sr.End, sr.MasterID, nodesStr)
				conn.Write([]byte(msg))
			}
			conn.Write([]byte("---------------\n"))
		}

	case "CLUSTER_METADATA_BEGIN":
		{
			reader := bufio.NewReader(conn)
			err := HandleIncomingClusterMetadata(reader, s)
			if err != nil {
				log.Printf("Error handling incoming cluster metadata: %v", err)
				conn.Write([]byte(fmt.Sprintf("ERR: Failed to process incoming metadata: %v\n", err)))
			} else {
				conn.Write([]byte("METADATA_RECEIVED_SUCCESS\n")) // Acknowledge receipt
			}
		}

	default:
		conn.Write([]byte("ERR unknown command\n"))
	}
}

// determineRange selects a random existing slot range to split.
// It returns the index of the selected range in s.Metadata, and the start/end of the new sub-range.
func determineRange(s *config.Server) (int, uint16, uint16) {
	if s.Nnode == 0 || len(s.Metadata) == 0 {
		log.Fatal("No nodes or metadata found to determine range from. Cluster is empty?")
	}

	// Iterate to find a range that can actually be split (has more than 1 slot)
	var selectedIdx int
	var selectedRange *config.SlotRange
	attempts := 0
	maxAttempts := 100 // Prevent infinite loop if all ranges are tiny

	for attempts < maxAttempts {
		selectedIdx = rand.Intn(len(s.Metadata))
		tempRange := s.Metadata[selectedIdx]
		if tempRange.End > tempRange.Start {
			selectedRange = tempRange
			break
		}
		attempts++
	}

	if selectedRange == nil {
		log.Printf("Warning: Could not find a splittable range after %d attempts. Selecting any range.", maxAttempts)
		selectedIdx = rand.Intn(len(s.Metadata)) // Fallback to any range
		selectedRange = s.Metadata[selectedIdx]
	}

	start := selectedRange.Start
	end := selectedRange.End

	mid := (start + end) / 2

	newRangeStart := mid + 1
	newRangeEnd := end

	log.Printf("Selected range for split: %d-%d (owned by %s). New node will take %d-%d. Old node keeps %d-%d.",
		selectedRange.Start, selectedRange.End, selectedRange.Nodes[0].ServerID, newRangeStart, newRangeEnd, start, mid)

	return selectedIdx, newRangeStart, newRangeEnd
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

// applyCommitChanges encapsulates the logic for updating metadata and node lists
// after a successful COMMIT phase. This function is called by both the coordinating
// node and participating nodes.
func applyCommitChanges(s *config.Server, preparedMsg *config.PrepareMessage) error {
	// 1. Add the new node to the global nodes map
	if _, ok := s.Nodes[preparedMsg.TargetNodeID]; !ok {
		s.Nodes[preparedMsg.TargetNodeID] = &config.Node{ServerID: preparedMsg.TargetNodeID, Addr: preparedMsg.Addr}
		s.Nnode++
	}

	// 2. Find the modified SlotRange
	var modifiedRangeIdx = -1
	for i, sr := range s.Metadata {
		if len(sr.Nodes) > 0 && sr.Nodes[0].ServerID == preparedMsg.ModifiedNodeID &&
			preparedMsg.Start > sr.Start && preparedMsg.End == sr.End {
			modifiedRangeIdx = i
			break
		}
	}

	if modifiedRangeIdx == -1 {
		return fmt.Errorf("ModifiedNode SlotRange not found for expected split pattern. PreparedMsg: %+v, Current Metadata: %+v", preparedMsg, s.Metadata)
	}

	originalSR := s.Metadata[modifiedRangeIdx]
	originalSR.End = preparedMsg.Start - 1

	newJoinNodeRange := &config.SlotRange{
		Start:    preparedMsg.Start,
		End:      preparedMsg.End,
		MasterID: preparedMsg.TargetNodeID,
		Nodes:    []*config.Node{s.Nodes[preparedMsg.TargetNodeID]}, // Use the globally registered node object
	}
	s.Metadata = append(s.Metadata, newJoinNodeRange)
	sort.Slice(s.Metadata, func(i, j int) bool {
		return s.Metadata[i].Start < s.Metadata[j].Start
	})

	s.Cluster_Version++                       //Increment cluster version on successful commit
	delete(s.Prepared, preparedMsg.MessageID) // Remove from preparedMsg state
	return nil
}

// HandleIncomingClusterMetadata processes metadata received from another node.
// This is typically used by a joining node to sync its view of the cluster.
func HandleIncomingClusterMetadata(reader *bufio.Reader, s *config.Server) error {
	newMetadata := []*config.SlotRange{}
	newNodeMap := map[string]*config.Node{} // Temporarily build a new node map

	log.Println("Starting to handle incoming cluster metadata...")

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read metadata line: %w", err)
		}
		line = strings.TrimSpace(line)

		if line == "CLUSTER_METADATA_END" {
			log.Println("Received CLUSTER_METADATA_END. Finishing metadata sync.")
			break
		}
		if line == "CLUSTER_METADATA_BEGIN" {
			log.Println("Received CLUSTER_METADATA_BEGIN.")
			continue
		}
		if !strings.HasPrefix(line, "SLOT") {
			log.Printf("Skipping unrecognized metadata line: %q", line)
			continue
		}

		//Expected format: SLOT <start> <end> <Node1@Addr1>,<Node2@Addr2>,
		parts := strings.Fields(line)
		if len(parts) < 4 {
			log.Printf("Skipping malformed SLOT line (not enough parts): %q", line)
			continue
		}

		start, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			return fmt.Errorf("invalid slot start %q in line %q: %w", parts[1], line, err)
		}
		end, err := strconv.ParseUint(parts[2], 10, 16)
		if err != nil {
			return fmt.Errorf("invalid slot end %q in line %q: %w", parts[2], line, err)
		}

		var slotNodes []*config.Node
		nodeEntries := strings.Split(parts[3], ",")
		var masterID string // Capture master ID if implied by the first node
		for i, entry := range nodeEntries {
			if entry == "NONE" {
				continue
			}
			nodeParts := strings.Split(entry, "@")
			if len(nodeParts) != 2 {
				log.Printf("Skipping malformed node entry %q in line %q", entry, line)
				continue
			}
			id := nodeParts[0]
			addr := nodeParts[1]

			node, ok := newNodeMap[id] // Check if we've already parsed this node during this metadata sync
			if !ok {
				node = &config.Node{ServerID: id, Addr: addr}
				newNodeMap[id] = node
			}
			slotNodes = append(slotNodes, node)

			//first node in the part[3] will be the masterNode
			if i == 0 {
				masterID = id
			}
		}

		newMetadata = append(newMetadata, &config.SlotRange{
			Start:    uint16(start),
			End:      uint16(end),
			MasterID: masterID,
			Nodes:    slotNodes,
		})
	}

	//Update the server's metadata and node list
	s.Metadata = newMetadata

	// Rebuild s.Nodes map
	s.Nodes = newNodeMap
	s.Nnode = uint16(len(s.Nodes))
	s.Cluster_Version++ // Increment version as metadata has been updated

	log.Printf("Successfully updated metadata. New Cluster Version: %d, Nodes: %d, Slot Ranges: %d",
		s.Cluster_Version, s.Nnode, len(s.Metadata))

	return nil
}
