package bus

import (
	"iris/config"
	"log"
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
		log.Printf("[WARNING] 💖💖 %s \n", cmd)
		return
	}

	serverid := cmd[1]
	unreachable := cmd[2]
	group := cmd[3]
	peer_version, _ := strconv.Atoi(cmd[4]) // don't forget to handle errr here

	if uint64(peer_version) != s.GetClusterVersion() {
		conn.Write([]byte("VERSION_MISMATCH\n"))
		return
	}

	ok := true
	if unreachable != "NONE" {
		unreachableNodes := strings.Split(unreachable, ",")
		ok = s.UpdateHeartbeat(serverid, unreachableNodes, group)
	}

	if ok {
		conn.Write([]byte("OK\n"))
	} else {
		conn.Write([]byte("ERROR\n"))
	}

	// Discarded Idea's
	// // send the lastseen of servers to the peer as response, this will help the peer to know which servers are alive in the cluster
	// // this lastseen info will be used to elect a new master if the current master goes down suddenly

	// lastSeenInfo := strings.Builder{}

	// fmt.Fprintf(&lastSeenInfo, "OK LASTSEEN ")

}
