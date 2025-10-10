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
	case "REP ADD":
		{
			if len(parts) != 6 {
				conn.Write([]byte("ERR: invalid format, CMU REP ADD SERVERID START END"))
				return
			}

			fmt.Println("server id: ", parts[3])
			fmt.Println("server start: ", parts[4])
			fmt.Println("server end: ", parts[5])

			if _, exists := s.Nodes[parts[3]]; !exists {
				conn.Write([]byte("ERR: server Id not found"))
				return
			}
			start, _ := utils.ParseUint16(parts[4])
			end, _ := utils.ParseUint16(parts[5])
			idx := s.FindRangeIndex(start, end)

			s.Metadata[idx].Nodes = append(s.Metadata[idx].Nodes, parts[6])
			conn.Write([]byte("CMU ACK"))

		}
	default:
		{
			conn.Write([]byte("ERR: invalid message"))
			return
		}
	}
}
