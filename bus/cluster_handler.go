package bus

import (
	"fmt"
	"iris/config"
	"iris/engine"
	"net"
	"sort"
	"strings"
)

func HandleClusterCommand(cmd string, conn net.Conn, s *config.Server, db *engine.Engine) {
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
	case "REP":
		{
			HandleReplication(conn, parts, s, db)
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

	case "INS":
		{
			HandleINS(conn, parts, s, db)
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

	// 2. Find the modified SlotRange.
	var modifiedRangeIdx = -1
	for i, sr := range s.Metadata {
		if sr.MasterID == preparedMsg.ModifiedNodeID &&
			preparedMsg.Start >= sr.Start && preparedMsg.End == sr.End &&
			preparedMsg.Start > sr.Start {
			modifiedRangeIdx = i
			break
		}
	}

	if modifiedRangeIdx == -1 {
		return fmt.Errorf("ModifiedNode SlotRange not found for expected split pattern. PreparedMsg: %+v, Current Metadata: %+v", preparedMsg, s.Metadata)
	}

	s.Metadata[modifiedRangeIdx].End = preparedMsg.Start - 1
	s.Metadata[modifiedRangeIdx].Nodes = preparedMsg.ModifiedNodeReplicaList

	// Create the new slot range for the joining node.
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

	s.Cluster_Version++

	delete(s.Prepared, preparedMsg.MessageID)
	return nil
}
