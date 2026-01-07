package bus

import (
	"iris/config"
	"net"
	"strconv"
	"strings"
)

// HandleHeartbeat processes heartbeat messages from other cluster nodes.
// expected command format is:
// HEARTBEAT SID:<server_id> UNREACHABLE:<comma_separated_sids_or_empty> GROUP:<group> VERSION:<cluster_version>
func HandleHeartbeat(conn net.Conn, cmd []string, s *config.Server) {
	if len(cmd) < 5 {
		conn.Write([]byte("Expected Format : HEARTBEAT SID:<server_id> UNREACHABLE:<comma_separated_sids_or_empty> GROUP:<group> VERSION:<cluster_version>\n"))
		return
	}

	serverid := cmd[1]
	unreachable := cmd[2]
	unreachableNodes := strings.Split(unreachable, ",")
	group := cmd[3]
	peer_version, _ := strconv.Atoi(cmd[4]) // don't forget to handle errr here

	if uint64(peer_version) != s.GetClusterVersion() {
		conn.Write([]byte("VERSION_MISMATCH\n"))
		return
	}

	ok := s.UpdateHeartbeat(serverid, unreachableNodes, group)
	if ok {
		conn.Write([]byte("OK\n"))
	} else {
		conn.Write([]byte("ERROR\n"))
	}

}
