package bus

import (
	"bufio"
	"fmt"
	"iris/config"
	"log"
	"net"
	"strconv"
	"strings"
)

func HandleMetadata(conn net.Conn, s *config.Server) {
	reader := bufio.NewReader(conn)
	err := HandleIncomingClusterMetadata(reader, s)
	if err != nil {
		log.Printf("Error handling incoming cluster metadata: %v", err)
		conn.Write([]byte(fmt.Sprintf("ERR: Failed to process incoming metadata: %v\n", err)))
	} else {
		conn.Write([]byte("METADATA_RECEIVED_SUCCESS\n")) // Acknowledge receipt
	}
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
