package bus

import (
	"bufio"
	"fmt"
	"io"
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
		conn.Write([]byte("METADATA_RECEIVED_SUCCESS\n"))
	}
}

// HandleIncomingClusterMetadata processes metadata received from another node.
// This is typically used by a joining node to sync its view of the cluster.
func HandleIncomingClusterMetadata(reader *bufio.Reader, s *config.Server) error {
	newMetadata := []*config.SlotRange{}
	newNodeMap := map[string]*config.Node{}

	log.Println("Starting to handle incoming cluster metadata...")

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("Connection closed by peer during metadata sync.")
				break
			}
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

		fmt.Printf("Receiver Metadata: %s\n", line)

		// Expected format:
		// SLOT <start> <end> <MASTERID@ADDR> <Node1ID@ADDR1>,<Node2ID@ADDR2>,...
		parts := strings.Fields(line)
		if len(parts) < 5 {
			log.Printf("Skipping malformed SLOT line (not enough parts): %q", line)
			continue
		}

		start, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			return fmt.Errorf("invalid slot start %q: %w", parts[1], err)
		}
		end, err := strconv.ParseUint(parts[2], 10, 16)
		if err != nil {
			return fmt.Errorf("invalid slot end %q: %w", parts[2], err)
		}

		masterParts := strings.Split(parts[3], "@")
		if len(masterParts) != 2 {
			return fmt.Errorf("invalid master node format: %q", parts[3])
		}
		masterID, masterAddr := masterParts[0], masterParts[1]

		// Ensure master node exists in node map
		if _, ok := newNodeMap[masterID]; !ok {
			newNodeMap[masterID] = &config.Node{
				ServerID: masterID,
				Addr:     masterAddr,
			}
		}

		var slotNodes []string
		nodeEntries := strings.Split(parts[4], ",")
		for _, entry := range nodeEntries {
			if entry == "" || entry == "NONE" {
				continue
			}
			nodeParts := strings.Split(entry, "@")
			if len(nodeParts) != 2 {
				log.Printf("Skipping malformed node entry %q", entry)
				continue
			}
			id, addr := nodeParts[0], nodeParts[1]

			if _, ok := newNodeMap[id]; !ok {
				newNodeMap[id] = &config.Node{
					ServerID: id,
					Addr:     addr,
				}
			}
			slotNodes = append(slotNodes, id)
		}

		newMetadata = append(newMetadata, &config.SlotRange{
			Start:    uint16(start),
			End:      uint16(end),
			MasterID: masterID,
			Nodes:    slotNodes,
		})
	}

	// state mutation + locking happens inside config.Server
	s.ApplyClusterMetadata(newMetadata, newNodeMap)

	return nil
}
