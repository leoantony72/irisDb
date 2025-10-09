package config

import (
	"log"
	"math/rand"
	"time"
)

func (s *Server) RepairReplication() {
	_, _, replicaNodes := s.FindHandlingRanges() // existing replicas

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
}
