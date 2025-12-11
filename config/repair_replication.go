package config

import (
	"fmt"
	"iris/utils"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"
)

// RepairReplication checks if each handling range has enough replica
// nodes. If not, it assigns replicas until numberOfReplica == ReplicationFactor
func (s *Server) RepairReplication(serverID string) {
	ranges := s.FindRangeIndexByServerID(serverID)

	for _, idx := range ranges {
		// Get range info inside lock to be safe
		var replicaNodes []string
		var start, end uint16
		s.mu.RLock()
		if idx < 0 || idx >= len(s.Metadata) {
			s.mu.RUnlock()
			continue
		}
		node := s.Metadata[idx]
		replicaNodes = append([]string(nil), node.Nodes...)
		start = node.Start
		end = node.End
		s.mu.RUnlock()

		currentCount := len(replicaNodes)
		required := s.ReplicationFactor
		if currentCount >= required {
			log.Printf("[INFO] Server %s range %d-%d already has %d replicas (required: %d)",
				s.ServerID, start, end, currentCount, required)
			continue
		}

		// Build set of existing replicas
		existing := make(map[string]bool)
		existing[s.ServerID] = true // Don't assign self as replica
		for _, id := range replicaNodes {
			existing[id] = true
		}

		//Get candidate nodes
		candidates := []string{}
		s.mu.RLock()
		for _, node := range s.Nodes {
			if existing[node.ServerID] {
				continue
			}
			candidates = append(candidates, node.ServerID)
		}
		s.mu.RUnlock()

		// Check if we have any candidates
		if len(candidates) == 0 {
			log.Printf("[WARN] Server %s range %d-%d: no available nodes for new replicas (cluster too small)",
				s.ServerID, start, end)
			continue
		}

		// Calculate how many replicas we need to add
		needed := required - currentCount
		if needed > len(candidates) {
			needed = len(candidates)
			log.Printf("[WARN] Server %s range %d-%d: cannot reach replication factor %d (only %d candidates available)",
				s.ServerID, start, end, required, len(candidates))
		}

		// Shuffle candidates for load distribution
		rand.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})

		newReplicas := candidates[:needed]

		log.Printf("[INFO] Server %s range %d-%d: need to add %d replicas", s.ServerID, start, end, needed)
		for _, replicaID := range newReplicas {
			log.Printf("[INFO] Assigning %s as new replica for server %s range %d-%d",
				replicaID, s.ServerID, start, end)
		}
		// Send command to master of each modified range to notify about new replicas. Tell the master to
		// Send the data to the new replicas. During which the master should not accept writes for that range. 

		// How about when the nodes receive the CMU REP ADD command, they check if they are master for that range,
		// if yes, they initiate a data transfer to the new replica?


		// Send CMU REP ADD to every peer (every node except this master)
		peers := s.GetNodesSnapshot()
		for _, peer := range peers {
			if peer.ServerID == s.ServerID {
				continue // skip master (self)
			}

			busAddr, err := utils.BumpPort(peer.Addr, 10000)
			if err != nil {
				log.Printf("[WARN] Server %s: failed to derive bus port for peer %s: %v", s.ServerID, peer.ServerID, err)
				continue
			}

			// Open a single connection to the peer and reuse it for all replica additions
			conn, err := net.DialTimeout("tcp", busAddr, 10*time.Second)
			if err != nil {
				log.Printf("[ERROR] Server %s: failed to connect to peer %s (%s): %v", s.ServerID, peer.ServerID, busAddr, err)
				continue
			}

			func() {
				defer conn.Close()

				for _, replicaID := range newReplicas {
					// Send CMU REP ADD message: CMU REP ADD <SERVERID> <START> <END>
					msg := fmt.Sprintf("CMU REP ADD %s %d %d\n", replicaID, start, end)
					if _, err := conn.Write([]byte(msg)); err != nil {
						log.Printf("[ERROR] Server %s: failed to send CMU REP ADD to %s: %v", s.ServerID, peer.ServerID, err)
						continue
					}

					// Read response for this replica addition
					response := make([]byte, 1024)
					n, err := conn.Read(response)
					if err != nil {
						log.Printf("[ERROR] Server %s: failed to read CMU ACK from %s: %v", s.ServerID, peer.ServerID, err)
						return
					}

					respStr := strings.TrimSpace(string(response[:n]))
					if respStr != "CMU ACK" {
						log.Printf("[ERROR] Server %s: unexpected response from %s for CMU REP ADD: %s", s.ServerID, peer.ServerID, respStr)
						continue
					}

					log.Printf("[SUCCESS] Server %s: notified peer %s about new replica %s for range %d-%d", s.ServerID, peer.ServerID, replicaID, start, end)
				}
			}()
		}

		// Update metadata
		// Find the range index without holding lock first
		var metaIdx int
		s.mu.RLock()
		metaIdx = -1
		for i, r := range s.Metadata {
			if r.Start == start && r.End == end {
				metaIdx = i
				break
			}
		}
		s.mu.RUnlock()

		if metaIdx == -1 {
			log.Printf("[ERROR] Server %s: could not find range %d-%d after repair", s.ServerID, start, end)
			continue
		}

		s.mu.Lock()
		for _, replicaID := range newReplicas {
			if existing[replicaID] {
				continue
			}
			s.Metadata[metaIdx].Nodes = append(s.Metadata[metaIdx].Nodes, replicaID)
			existing[replicaID] = true
			s.Cluster_Version++
		}
		s.mu.Unlock()
	}
}

func (s *Server) RepairRangeOnMaster(serverID string) error {
	log.Printf("RepairRangeOnMaster: Requesting repair for server %s", serverID)
	s.RepairReplication(serverID)
	log.Printf("RepairRangeOnMaster: Completed repair request for server %s,", serverID)
	return nil
}

func (s *Server) ForwardRepairRequestToMaster() {
	// Forward a repair request for this server to the cluster master.
	// Master is determined from the first slot range's MasterID (slot 0 owner).
	s.mu.RLock()
	if len(s.Metadata) == 0 {
		s.mu.RUnlock()
		log.Printf("ForwardRepairRequestToMaster: no metadata available on server %s", s.ServerID)
		return
	}
	masterID := s.Metadata[0].MasterID
	s.mu.RUnlock()

	// If this server is the master, handle locally
	if masterID == s.ServerID {
		log.Printf("ForwardRepairRequestToMaster: server %s is master, performing repair locally", s.ServerID)
		if err := s.RepairRangeOnMaster(s.ServerID); err != nil {
			log.Printf("ForwardRepairRequestToMaster: local repair failed: %v", err)
		}
		return
	}

	// Get master node info
	masterNode, ok := s.GetConnectedNodeData(masterID)
	if !ok {
		log.Printf("ForwardRepairRequestToMaster: master node %s not found in nodes map", masterID)
		return
	}

	busAddr, err := utils.BumpPort(masterNode.Addr, 10000)
	if err != nil {
		log.Printf("ForwardRepairRequestToMaster: failed to compute bus address for master %s: %v", masterID, err)
		return
	}

	conn, err := net.DialTimeout("tcp", busAddr, 10*time.Second)
	if err != nil {
		log.Printf("ForwardRepairRequestToMaster: failed to connect to master %s at %s: %v", masterID, busAddr, err)
		return
	}
	defer conn.Close()

	// Send repair request: CMU REPAIR REQ <ServerID>
	msg := fmt.Sprintf("CMU REPAIR REQ %s\n", s.ServerID)
	if _, err := conn.Write([]byte(msg)); err != nil {
		log.Printf("ForwardRepairRequestToMaster: failed to send repair request to master %s: %v", masterID, err)
		return
	}

	// Read response (optional) and log it
	resp := make([]byte, 1024)
	n, err := conn.Read(resp)
	if err != nil {
		log.Printf("ForwardRepairRequestToMaster: error reading response from master %s: %v", masterID, err)
		return
	}
	reply := strings.TrimSpace(string(resp[:n]))
	log.Printf("ForwardRepairRequestToMaster: master %s replied: %s", masterID, reply)
}
