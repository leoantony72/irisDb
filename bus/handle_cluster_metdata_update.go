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
			mapping, _ := s.RepairRangeOnMaster(serverID)

			for key, replicas := range mapping {
				// key is "start-end"
				parts := strings.Split(key, "-")
				if len(parts) != 2 {
					log.Printf("[WARN] invalid mapping key: %q", key)
					continue
				}

				start, err := utils.ParseUint16(parts[0])
				if err != nil {
					log.Printf("[WARN] bad start in mapping key %q: %v", key, err)
					continue
				}
				end, err := utils.ParseUint16(parts[1])
				if err != nil {
					log.Printf("[WARN] bad end in mapping key %q: %v", key, err)
					continue
				}

				idx := s.FindRangeIndex(start, end)
				r, ok := s.GetSlotRangeByIndex(idx)
				if !ok {
					log.Printf("[WARN] no slot-range found for %d-%d (key=%s)", start, end, key)
					continue
				}

				log.Printf("[💖INFO] rangeMaster: %s | ServerID: %s")
				// only the master for this range should initiate transfers
				if r.MasterID != s.ServerID {
					continue
				}

				log.Printf("[💖INFO] MASTER")

				for _, replicaID := range replicas {
					// replicate: master -> replicaID for this range
					log.Printf("[INFO] master %s initiating transfer for range %d-%d -> %s", s.ServerID, start, end, replicaID)
					go InitiateDataTransferToReplica(replicaID, start, end, db, s)
				}
			}

		}
	default:
		{
			conn.Write([]byte("ERR: invalid message\n"))
			return
		}
	}
}
