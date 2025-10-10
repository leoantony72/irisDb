package config

import (
	"fmt"
	"iris/utils"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

func (s *Server) RepairReplication() {
	start, end, replicaNodes := s.FindHandlingRanges() //existing replicas

	currentCount := len(replicaNodes)
	required := s.ReplicationFactor

	if currentCount >= required {
		return
	}

	existing := make(map[string]bool)
	for _, id := range replicaNodes {
		existing[id] = true
	}

	candidates := []string{}
	for _, node := range s.Nodes {
		if node.ServerID == s.ServerID {
			continue
		}
		if existing[node.ServerID] {
			continue
		}
		candidates = append(candidates, node.ServerID)
	}

	//Not enough nodes in cluster
	if len(candidates) == 0 {
		log.Printf("[WARN] Server %s: no available nodes for new replicas (cluster too small)", s.ServerID)
		return
	}

	//Not enough nodes to fully satisfy replication factor
	if len(replicaNodes)+len(candidates) < required {
		log.Printf("[WARN] Server %s: cannot reach replication factor %d (only %d candidates available)",
			s.ServerID, required, len(candidates))
	}

	//Shuffle for load distribution
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	needed := required - currentCount
	if needed > len(candidates) {
		needed = len(candidates)
	}
	newReplicas := candidates[:needed]

	for _, replicaID := range newReplicas {
		log.Printf("[INFO] Assigning %s as new replica for server %s", replicaID, s.ServerID)
	}

	for _, node := range s.Nodes {
		if node.ServerID == s.ServerID {
			continue
		}

		addr, _ := utils.BumpPort(node.Addr, 10000)
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			fmt.Printf("ERR: RepairReplication: %s\n", err.Error())
			// return false
			conn.Close()
			continue
		}

		msg := fmt.Sprintf("CMU ADD REP %s %s %s", s.ServerID, strconv.FormatUint(uint64(start), 10), strconv.FormatUint(uint64(end), 10))
		conn.Write([]byte(msg))

		response := make([]byte, 1024)
		n, err := conn.Read(response)
		if err != nil {
			fmt.Printf("SendReplicaCMD:failed to read from peer(ID:%s) %s: %w\n", s.ServerID, addr, err.Error())
			conn.Close()
			return
		}

		str := strings.TrimSpace(string(response[:n]))
		if str != "ACK REP" {
			fmt.Printf("SendReplicaCMD:failed to get ACK for cmd:CMU REP ADD\n")
			conn.Close()
			return
		}

		fmt.Println("[CMU REP UPDATE] for %s SUCCESS", addr)
	}
}
