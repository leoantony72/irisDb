package config

import (
	"bufio"
	"fmt"
	"iris/utils"
	"log"
	"net"
	"strings"
	"time"
)

func (server *Server) Heartbeat() {
	for {
		time.Sleep(15 * time.Second)
		masterNodeID := server.MasterNodeID
		if masterNodeID == server.ServerID {
			server.CheckReplicaTimeouts()
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

func (server *Server) CheckReplicaTimeouts() {
	server.mu.Lock()
	var timedOutNodes []string
	now := time.Now()
	timeoutDuration := 45 * time.Second

	for id := range server.Nodes {
		if id == server.ServerID {
			continue // don't time out ourselves
		}
		lastSeen, ok := server.LastSeen[id]
		if !ok {
			server.LastSeen[id] = now
			continue
		}
		if now.Sub(lastSeen) > timeoutDuration {
			timedOutNodes = append(timedOutNodes, id)
		}
	}
	server.mu.Unlock()

	anyRemoved := false
	for _, id := range timedOutNodes {
		log.Printf("[FAILURE_DETECTION] Node %s has not sent heartbeat for >%v. Removing from cluster.", id, timeoutDuration)
		err := server.NodeExit(id)
		if err != nil {
			log.Printf("[FAILURE_DETECTION] Failed to remove node %s: %v", id, err)
		} else {
			log.Printf("[FAILURE_DETECTION] Node %s successfully removed from cluster.", id)
			anyRemoved = true
		}
	}

	if anyRemoved {
		log.Println("[FAILURE_DETECTION] Replicas updated. Broadcasting new cluster snapshot to remaining peers.")
		server.BroadcastSnapshotToPeers()
	}
}

// BroadcastSnapshotToPeers broadcasts the current cluster metadata snapshot to all peers.
// It must NOT be called while holding s.mu.
func (server *Server) BroadcastSnapshotToPeers() {
	peers := server.GetCommitPeers()
	for _, p := range peers {
		busAddr, err := utils.BumpPort(p.Addr, 10000)
		if err != nil {
			log.Printf("[FAILURE_DETECTION] Failed to bump port for peer %s: %v\n", p.ServerID, err)
			continue
		}
		peerConn, err := net.DialTimeout("tcp", busAddr, 10*time.Second)
		if err != nil {
			log.Printf("[FAILURE_DETECTION] Failed to connect to peer %s for snapshot: %v\n", p.ServerID, err)
			continue
		}

		peerConn.Write([]byte("SNAPSHOT \n"))
		if err := server.SendClusterSnapshot(peerConn); err != nil {
			log.Printf("[FAILURE_DETECTION] Failed to send snapshot to peer %s: %v\n", p.ServerID, err)
			peerConn.Close()
			continue
		}

		reader := bufio.NewReader(peerConn)
		resp, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("[FAILURE_DETECTION] Failed to read SNAPSHOT_OK from peer %s: %v\n", p.ServerID, err)
			peerConn.Close()
			continue
		}
		resp = strings.TrimSpace(resp)
		if resp != "SNAPSHOT_OK" {
			log.Printf("[FAILURE_DETECTION] Peer %s rejected snapshot with: %s\n", p.ServerID, resp)
		} else {
			log.Printf("[FAILURE_DETECTION] Successfully updated peer %s with new cluster snapshot after node failure", p.ServerID)
		}
		peerConn.Close()
	}
}
