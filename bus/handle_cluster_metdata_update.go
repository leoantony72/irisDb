package bus

import (
	"fmt"
	"iris/config"
	"iris/engine"
	"iris/utils"
	"log"
	"net"
	"strings"
)

// CMU REP : ADD/REMOVE/UPDATE replica list of a range/server
// CMU REP ADD SERVERID START END
func HandleClusterMetdataUpdate(conn net.Conn, parts []string, s *config.Server, db *engine.Engine) {
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

			// check if the current server is master for the range
			masterIdx := s.FindRangeIndex(start, end)
			master, ok := s.GetSlotRangeByIndex(masterIdx)
			if !ok {
				conn.Write([]byte("ERR: internal error\n"))
				return
			}

			log.Printf("[🌹INFO] %s | %s", master.MasterID, serverID)
			if master.MasterID == s.ServerID {
				// this server is master for the range, send the data to the new replica
				// initiate data transfer to the new replica

				log.Println("[🌹INFO] this server is the master")
				// @@disable writes for the range during transfer
				go InitiateDataTransferToReplica(serverID, start, end, db, s)
			}

			if err := s.AddReplicaToRange(serverID, start, end); err != nil {
				conn.Write([]byte(fmt.Sprintf("ERR: %s\n", err.Error())))
				return
			}

			conn.Write([]byte("CMU ACK\n"))
		}
	case "REPAIR":
		{
			//CMU REPAIR REQ SERVERID
			if len(parts) != 4 {
				conn.Write([]byte("ERR: invalid format, CMU REPAIR REQ SERVERID START END\n"))
				return
			}

			serverID := parts[3]
			s.RepairRangeOnMaster(serverID)
		}
	default:
		{
			conn.Write([]byte("ERR: invalid message\n"))
			return
		}
	}
}
