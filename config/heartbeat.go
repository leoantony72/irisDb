package config

import (
	"bufio"
	"fmt"
	"iris/utils"
	"log"
	"net"
	"time"
)

func (server *Server) Heartbeat() {
	for {
		time.Sleep(15 * time.Second)
		masterNodeID := server.MasterNodeID
		if masterNodeID == server.ServerID {
			continue
		}
		masterNode, ok := server.GetConnectedNodeData(masterNodeID)
		if !ok {
			log.Printf("[WARNING]: Master node data not found for ID %s\n", masterNodeID)
			server.IncrMasterFailedAttempts()
			if server.GetrMasterFailedAttempts() >= server.MASTER_FAIL_THRESHOLD {
				log.Printf("[ERROR]: Master node unreachable for %d attempts. Initiating failover...\n", server.MASTER_FAIL_THRESHOLD)
				server.InitiateMasterFailover()
			}
			continue
		}
		addr := masterNode.Addr
		busAddr, _ := utils.BumpPort(addr, 10000)

		conn, err := net.DialTimeout("tcp", busAddr, 2*time.Second)
		if err != nil {
			log.Printf("[WARNING]: Master node is unreachable: %v\n", err)
			// disable writes and reads
			server.IncrMasterFailedAttempts()
			if server.GetrMasterFailedAttempts() >=  server.MASTER_FAIL_THRESHOLD {
				log.Printf("[ERROR]: Master node unreachable for %d attempts. Initiating failover...\n",  server.MASTER_FAIL_THRESHOLD)
				server.InitiateMasterFailover()
			}
			continue
		}

		server.ResetMasterFailedAttempts()

		//HEARTBEAT SID:<server_id> UNREACHABLE:<comma_separated_sids_or_empty> GROUP:<group> VERSION:<cluster_version>
		unreachable_ids := server.UnreacableNodeList()
		server_group := server.GetServerGroup()
		version := server.GetClusterVersion()
		if unreachable_ids == "" {
			unreachable_ids = "NONE"
		}
		fmt.Printf("🍕🍕ServerGroup:%s\n", server_group)
		msg := fmt.Sprintf("HEARTBEAT %s %s %s %d\n", server.ServerID, unreachable_ids, server_group, version)

		_, err = conn.Write([]byte(msg))
		if err != nil {
			log.Printf("[WARNING]: Failed to send HEARTBEAT to master: %v\n", err)
			conn.Close()
			continue
		}

		response, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			log.Printf("[WARNING]: Failed to read HEARTBEAT response: %v\n", err)
			conn.Close()
			continue
		}

		conn.Close()
		switch response {
		case "OK\n":
			continue

		case "VERSION_MISMATCH\n":
			log.Println("[WARNING] VERSION MISMATCH FOUND")
			// stop all the operations until metadata is updated
			server.GlobalPause.Store(true)
			server.RequestMetadataSnapShot()
			server.GlobalPause.Store(false)
			continue

		case "ERROR\n":
			log.Println("[WARNING] Heartbeat ERROR from Master server")
			continue

		default:
			log.Printf("[WARNING] Unknown Response %s\n", response)
		}

	}
}
