package bus

import (
	"iris/config"
	"log"
	"net"
	"strconv"
)

func HandleReqVote(conn net.Conn, parts []string, s *config.Server) {
	if len(parts) < 4 {
		conn.Write([]byte("ERR invalid REQ_VOTE command\n"))
		return
	}

	CandidateNodeID := parts[1]
	FailedMasterNodeID := parts[2]
	clusterVersion, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		conn.Write([]byte("ERR invalid cluster version\n"))
		return
	}

	log.Printf("[INFO]: Received REQ_VOTE from node %s for failed master %s (version %d) on server %s\n",
		CandidateNodeID, FailedMasterNodeID, clusterVersion, s.ServerID)

	if s.MasterNodeID == FailedMasterNodeID && s.GetClusterVersion() == clusterVersion {
		log.Printf("[INFO]: Granting vote to candidate %s on server %s\n", CandidateNodeID, s.ServerID)
		conn.Write([]byte("VOTE_GRANTED\n"))
	} else {
		log.Printf("[INFO]: Denying vote to candidate %s on server %s (master mismatch or version mismatch)\n", CandidateNodeID, s.ServerID)
		conn.Write([]byte("ERR_STALE_CLUSTER_VERSION\n"))
	}
}
