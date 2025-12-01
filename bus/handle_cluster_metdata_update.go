package bus

import (
	"fmt"
	"iris/config"
	"iris/utils"
	"net"
	"strings"
)

// CMU REP : ADD/REMOVE/UPDATE replica list of a range/server
// CMU REP ADD SERVERID START END
func HandleClusterMetdataUpdate(conn net.Conn, parts []string, s *config.Server) {
	switch strings.ToUpper(parts[1]) {
	case "REP":
		{
			if len(parts) != 6 {
				conn.Write([]byte("ERR: invalid format, CMU REP ADD SERVERID START END\n"))
				return
			}

			serverID := parts[3]
			start, err := utils.ParseUint16(parts[4])
			if err != nil {
				conn.Write([]byte("ERR: invalid START value\n"))
				return
			}
			end, err := utils.ParseUint16(parts[5])
			if err != nil {
				conn.Write([]byte("ERR: invalid END value\n"))
				return
			}

			if err := s.AddReplicaToRange(serverID, start, end); err != nil {
				conn.Write([]byte(fmt.Sprintf("ERR: %s\n", err.Error())))
				return
			}

			conn.Write([]byte("CMU ACK\n"))
		}
	default:
		{
			conn.Write([]byte("ERR: invalid message\n"))
			return
		}
	}
}
