package config

import (
	"bufio"
	"fmt"
	"iris/utils"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type NodeStatus int

const (
	ALIVE NodeStatus = iota
	SUSPECT
	DEAD
	PARTITIONED
)

type Node struct {
	ServerID      string
	Addr          string
	Status        NodeStatus
	Group         string //asia-ind, eu-west, us-east
	ResourceScore float64
}

type GroupStatus int

const (
	HEALTHY GroupStatus = iota
	SUSPECT_GROUP
	PARTITIONED_GROUP
)

type GroupInfo struct {
	Name   string
	Nodes  []string
	Status GroupStatus
}

// SlotRange defines each range, masterID and Replica Nodes
// of each available range in the network
type SlotRange struct {
	Start    uint16
	End      uint16
	MasterID string
	Nodes    []string //list of replica node IDs
}

// PREPARE MESSAGEID TargetNodeID ADDR START END ModifiedNodeID
type PrepareMessage struct {
	MessageID               string
	SourceNodeID            string // The node initiating the preparation
	TargetNodeID            string // The new node ID to be initialized
	Addr                    string // Addr of the new Node
	Start                   uint16
	End                     uint16
	ModifiedNodeID          string // ID of the node from which the slots for the new nodes are taken
	ModifiedNodeReplicaList []string
	TargetNodeReplicaList   []string
	// mu                      *sync.RWMutex
	ResourceScore float64
	Group         string
}

type ResourceTracker struct {
	MaxRam      float64
	MaxCpuCores float64
	MaxDisk     float64
}

type Server struct {
	ServerID          string
	Host              string
	Addr              string
	Port              string
	N                 uint16           //hosh slots 2^14
	Nnode             uint16           //number of nodes
	Nodes             map[string]*Node //list of connected nodes
	Metadata          []*SlotRange     //hash slots
	ReplicationFactor int
	Cluster_Version   uint64
	GlobalPause       atomic.Bool // Used to Pause all the read and write during version mismatch
	BusPort           string
	Prepared          map[string]*PrepareMessage
	MasterNodeID      string

	mu           sync.RWMutex
	Listener     net.Listener
	BusListener  net.Listener
	ShuttingDown atomic.Bool
	Wg           sync.WaitGroup
	ShutdownOnce sync.Once

	LastSeen        map[string]time.Time  // used by the master to track last seen times of nodes
	Group           map[string]*GroupInfo //maps groups to node IDs
	UnreahableNodes map[string]time.Time

	MasterFailedAttempts  int
	SuspectLeaderMsg      map[string]time.Time
	Votes                 map[string]bool
	Rt                    ResourceTracker
	ResourceScore         float64
	MASTER_FAIL_THRESHOLD int
}

func (s *Server) GetClusterVersion() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Cluster_Version
}

func (s *Server) GetNodeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Nodes)
}

func (s *Server) GetSlotRangeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.Metadata)
}

func (s *Server) IncrMasterFailedAttempts() {
	s.mu.Lock()
	s.MasterFailedAttempts++
	s.mu.Unlock()
}

func (s *Server) ResetMasterFailedAttempts() {
	s.mu.Lock()
	s.MasterFailedAttempts = 0
	s.mu.Unlock()
}

func (s *Server) GetrMasterFailedAttempts() int {
	s.mu.RLock()

	defer s.mu.RUnlock()
	return s.MasterFailedAttempts
}

func (s *Server) UpdateMasterNodeID(nodeId string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.MasterNodeID = nodeId
}

func (s *Server) AddSuspectLeaderMsg(senderNodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SuspectLeaderMsg[senderNodeID] = time.Now()
}

func (s *Server) CheckMasterFailover() {
	s.mu.RLock()
	failedMaster := s.MasterNodeID
	suspects := len(s.SuspectLeaderMsg)
	quorum := s.quorumForFailoverLocked(failedMaster)
	s.mu.RUnlock()

	if suspects >= quorum {
		log.Printf("[INFO]: Failover quorum reached on server %s (suspects=%d quorum=%d). Starting election.\n",
			s.ServerID, suspects, quorum,
		)
		s.SendReqVoteToAll()
		return
	}

	log.Printf("[INFO]: Failover quorum not reached on server %s (suspects=%d quorum=%d)\n",
		s.ServerID, suspects, quorum,
	)
}

// Change Votes from map[string]bool to atomic counters:
func (s *Server) SendReqVoteToAll() {
	log.Printf("[DEBUG]: SendReqVoteToAll() called on server %s\n", s.ServerID)

	connectedNodes := s.GetNodesSnapshot()
	if connectedNodes == nil {
		log.Printf("[ERROR]: GetNodesSnapshot() returned nil on server %s\n", s.ServerID)
		return
	}

	s.mu.Lock()
	s.Votes = make(map[string]bool)
	s.Votes[s.ServerID] = true
	failedMaster := s.MasterNodeID
	clusterVersion := s.Cluster_Version
	s.mu.Unlock() // ✅ Release BEFORE spawning goroutines

	var wg sync.WaitGroup
	voteChan := make(chan string, len(connectedNodes)) // ✅ Use channel instead

	for _, node := range connectedNodes {
		if node.ServerID == "" || node.ServerID == s.ServerID || node.ServerID == failedMaster {
			continue
		}

		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()

			bumpAddr, _ := utils.BumpPort(n.Addr, 10000)
			conn, err := net.DialTimeout("tcp", bumpAddr, 2*time.Second)
			if err != nil {
				log.Printf("[ERROR]: Failed to connect to node %s: %v\n", n.ServerID, err)
				return
			}
			defer conn.Close()

			msg := fmt.Sprintf("REQ_VOTE %s %s %d\n", s.ServerID, failedMaster, clusterVersion)
			if _, err := conn.Write([]byte(msg)); err != nil {
				return
			}

			reader := bufio.NewReader(conn)
			response, _ := reader.ReadString('\n')
			response = strings.TrimSpace(response)

			// ✅ Send result through channel instead of acquiring lock
			if response == "VOTE_GRANTED" {
				voteChan <- n.ServerID
			}
		}(&node)
	}

	wg.Wait()
	close(voteChan)

	// ✅ Acquire lock ONCE after all goroutines finish
	s.mu.Lock()
	for voteID := range voteChan {
		s.Votes[voteID] = true
	}
	s.mu.Unlock()

	s.EvaluateElectionResult()
}

func (s *Server) EvaluateElectionResult() {
	s.mu.RLock()
	failedMaster := s.MasterNodeID
	majority := s.quorumForFailoverLocked(failedMaster)

	granted := 0
	for _, v := range s.Votes {
		if v {
			granted++
		}
	}
	s.mu.RUnlock()

	if granted >= majority {
		log.Printf("[ELECTION]: Won with %d votes (majority=%d)\n", granted, majority)
		s.BecomeLeader()
		return
	}
	log.Printf("[ELECTION]: Lost with %d votes (majority=%d)\n", granted, majority)
}

func (s *Server) BecomeLeader() {
	log.Printf("[ELECTION]: Node %s becoming new master\n", s.ServerID)

	oldMaster := s.MasterNodeID

	// Update master and increment cluster version FIRST (version 3)
	s.mu.Lock()
	s.MasterNodeID = s.ServerID
	s.Cluster_Version++
	newVersion := s.Cluster_Version
	s.SuspectLeaderMsg = make(map[string]time.Time)
	s.mu.Unlock()

	log.Printf("[INFO]: Cluster version incremented to %d on server %s\n", newVersion, s.ServerID)

	// ✅ DO NOT call NodeExit() yet - snapshot needs current metadata

	// Get all nodes and broadcast snapshot
	allNodes := s.GetNodesSnapshot()
	if allNodes == nil {
		log.Printf("[ERROR]: GetNodesSnapshot() returned nil on server %s\n", s.ServerID)
		return
	}

	log.Printf("[INFO]: Broadcasting new cluster snapshot (version %d) to %d nodes on server %s\n", newVersion, len(allNodes), s.ServerID)

	successCount := 0
	for i, p := range allNodes {
		log.Printf("[DEBUG]: Processing node[%d]: ServerID=%s\n", i, p.ServerID)

		if p.ServerID == "" {
			log.Printf("[DEBUG]: Skipping node[%d] - empty ServerID\n", i)
			continue
		}
		if p.ServerID == s.ServerID {
			log.Printf("[DEBUG]: Skipping self node %s\n", p.ServerID)
			continue
		}
		if p.ServerID == oldMaster {
			log.Printf("[DEBUG]: Skipping failed master %s\n", p.ServerID)
			continue
		}

		log.Printf("[INFO]: Connecting to peer %s at %s for snapshot broadcast\n", p.ServerID, p.Addr)
		busAddr, _ := utils.BumpPort(p.Addr, 10000)
		peerConn, err := net.DialTimeout("tcp", busAddr, 10*time.Second)
		if err != nil {
			log.Printf("[ERROR]: Failed to connect to peer %s for snapshot: %v\n", p.ServerID, err)
			continue
		}

		log.Printf("[INFO]: Connected to peer %s, sending SNAPSHOT command\n", p.ServerID)
		peerConn.Write([]byte("SNAPSHOT\n"))

		log.Printf("[INFO]: Sending cluster snapshot data to peer %s\n", p.ServerID)
		if err := s.SendClusterSnapshot(peerConn); err != nil {
			log.Printf("[ERROR]: Failed to send snapshot to peer %s: %v\n", p.ServerID, err)
			peerConn.Close()
			continue
		}

		log.Printf("[INFO]: Snapshot data sent to peer %s, waiting for SNAPSHOT_OK\n", p.ServerID)
		reader := bufio.NewReader(peerConn)
		resp, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("[ERROR]: Failed to read SNAPSHOT_OK from peer %s: %v\n", p.ServerID, err)
			peerConn.Close()
			continue
		}

		resp = strings.TrimSpace(resp)
		log.Printf("[DEBUG]: Received response from peer %s: '%s'\n", p.ServerID, resp)

		if resp != "SNAPSHOT_OK" {
			log.Printf("[ERROR]: Peer %s rejected snapshot with: %s\n", p.ServerID, resp)
			peerConn.Close()
			continue
		}

		log.Printf("[✅INFO]: Peer %s acknowledged new cluster snapshot (version %d)\n", p.ServerID, newVersion)
		successCount++
		peerConn.Close()
	}

	log.Printf("[INFO]: Snapshot broadcast complete - %d peers updated on server %s\n", successCount, s.ServerID)

	// ✅ NOW remove old master (this will increment version to 4, but replicas are already at 3)
	s.NodeExit(oldMaster)
}

func (s *Server) InitiateMasterFailover() {
	log.Printf("[INFO]: Initiating master failover on server %s\n", s.ServerID)

	s.mu.RLock()
	bestID := ""
	bestScore := -1.0

	for id, n := range s.Nodes {
		if id == "" || n == nil {
			continue
		}
		// never consider the current master as the failover candidate
		if id == s.MasterNodeID {
			continue
		}

		score := n.ResourceScore
		if bestID == "" || score > bestScore || (score == bestScore && id < bestID) {
			bestID = id
			bestScore = score
		}
	}
	s.mu.RUnlock()

	if bestID == "" {
		log.Printf("[ERROR]: No available nodes to promote to master on server %s\n", s.ServerID)
		return
	}

	log.Printf("[INFO]: Selected failover master candidate %s (ResourceScore=%.6f) on server %s\n", bestID, bestScore, s.ServerID)

	node, ok := s.GetConnectedNodeData(bestID)
	if !ok {
		log.Printf("[ERROR]: Node %s not found in connected nodes on server %s\n", bestID, s.ServerID)
		return
	}

	bumpAddr, _ := utils.BumpPort(node.Addr, 10000)
	conn, err := net.DialTimeout("tcp", bumpAddr, 2*time.Second)
	if err != nil {
		log.Printf("[ERROR]: Failed to connect to node %s on server %s: %v\n", bestID, s.ServerID, err)
		return
	}
	defer conn.Close()

	// SUSPECT_LEADER <current_node_id> <master_node_id> <cluster_version>
	msg := fmt.Sprintf("SUSPECT_LEADER %s %s %d \n", s.ServerID, s.MasterNodeID, s.GetClusterVersion())
	if _, err := conn.Write([]byte(msg)); err != nil {
		log.Printf("[ERROR]: Failed to send SUSPECT_LEADER to node %s on server %s: %v\n", bestID, s.ServerID, err)
		return
	}

	log.Printf("[INFO]: SUSPECT_LEADER sent to node %s on server %s\n", bestID, s.ServerID)
}

// quorumForFailoverLocked returns majority of known nodes excluding the failed master.
// Caller must hold at least RLock.
func (s *Server) quorumForFailoverLocked(failedMaster string) int {
	total := len(s.Nodes) // typically includes self + all known peers (including master)
	if total <= 0 {
		return 1
	}

	// Exclude failed master from the voter set if present.
	if failedMaster != "" {
		if _, ok := s.Nodes[failedMaster]; ok {
			total--
		}
	}
	if total <= 0 {
		return 1
	}
	return (total / 2) + 1
}
