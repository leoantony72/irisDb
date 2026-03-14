package bus

import (
	"iris/config"
	"log"
	"net"
	"strconv"
)

func HandleSuspectLeader(conn net.Conn, parts []string, s *config.Server) {
	if len(parts) < 4 {
		log.Printf("[ERROR]: Invalid SUSPECT_LEADER command, parts length: %d\n", len(parts))
		conn.Write([]byte("ERR invalid SUSPECT_LEADER command\n"))
		return
	}

	SenderNodeID := parts[1]
	masterNodeID := parts[2]
	clusterVersion, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		log.Printf("[ERROR]: Failed to parse cluster version '%s': %v\n", parts[3], err)
		conn.Write([]byte("ERR invalid cluster version\n"))
		return
	}

	log.Printf("[INFO]: Received SUSPECT_LEADER from node %s for master %s (version %d) on server %s\n",
		SenderNodeID, masterNodeID, clusterVersion, s.ServerID)

	localMasterID := s.MasterNodeID
	localVersion := s.GetClusterVersion()
	log.Printf("[DEBUG]: Comparing - local master: %s vs remote master: %s | local version: %d vs remote version: %d\n",
		localMasterID, masterNodeID, localVersion, clusterVersion)

	if s.MasterNodeID == masterNodeID && s.GetClusterVersion() == clusterVersion {
		log.Printf("[INFO]: Initiating failover on server %s\n", s.ServerID)
		s.AddSuspectLeaderMsg(SenderNodeID)
		s.CheckMasterFailover()
	} else {
		log.Printf("[INFO]: Ignoring SUSPECT_LEADER from node %s on server %s (master mismatch or version mismatch)\n", SenderNodeID, s.ServerID)
	}
}
