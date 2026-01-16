package bus

import (
	"iris/config"
	"log"
	"net"
	"strconv"
)

func HandleSuspectLeader(conn net.Conn, parts []string, s *config.Server) {
	if len(parts) < 4 {
		conn.Write([]byte("ERR invalid SUSPECT_LEADER command\n"))
		return
	}

	SenderNodeID := parts[1]
	masterNodeID := parts[2]
	clusterVersion, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		conn.Write([]byte("ERR invalid cluster version\n"))
		return
	}

	log.Printf("[INFO]: Received SUSPECT_LEADER from node %s for master %s on server %s\n", SenderNodeID, masterNodeID, s.ServerID)

	if s.MasterNodeID == masterNodeID && s.GetClusterVersion() == clusterVersion {
		log.Printf("[INFO]: Initiating failover on server %s\n", s.ServerID)

		s.AddSuspectLeaderMsg(SenderNodeID)
		s.CheckMasterFailover()
	} else {
		log.Printf("[INFO]: Ignoring SUSPECT_LEADER from node %s on server %s\n", SenderNodeID, s.ServerID)
	}
}
