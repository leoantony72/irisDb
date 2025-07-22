package bus

import (
	"fmt"
	"iris/config"
	"net"
	"sort"
	"strings"
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
			HandleJoin(conn, parts, s)
		}

	case "PREPARE":
		{
			HandlePrepare(conn, parts, s)
		}

	case "COMMIT":
		{
			HandleCommit(conn, parts, s)
		}

	case "SHOW":
		{
			HandleShow(conn, s)
		}

	case "CLUSTER_METADATA_BEGIN":
		{
			HandleMetadata(conn, s)
		}

	default:
		conn.Write([]byte("ERR unknown command\n"))
	}
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
