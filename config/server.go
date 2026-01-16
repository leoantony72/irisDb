package config

import (
	"fmt"
	"iris/utils"
	"log"
	"net"
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
	ServerID string
	Addr     string
	Status   NodeStatus
	Group    string //asia-ind, eu-west, us-east
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

	MasterFailedAttempts int
	SuspectLeaderMsg     map[string]time.Time
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

	totalNodes := len(s.Nodes)
	s.mu.RUnlock()
	quorum := (totalNodes / 2) + 1

	if len(s.SuspectLeaderMsg) >= quorum {
		// send REQ VOTE TO ALL the nodes to become master
	} else {
		log.Printf("[INFO]: Not enough SUSPECT_LEADER messages to initiate failover on server %s\n", s.ServerID)
		return
	}

}

func (s *Server) InitiateMasterFailover(IrisDb *Server) {
	log.Printf("[INFO]: Initiating master failover on server %s\n", s.ServerID)

	// send the failover command to next node in the cluster, from the metadata index
	//  if the current master node is of metdata[0] then next node is metadata[1]

	const timeout = 10 * time.Second
	s.mu.RLock()
	now := time.Now()
	var newMasterID string
	for _, metadata := range s.Metadata {
		nextCandidateID := metadata.MasterID

		if now.Sub(s.LastSeen[nextCandidateID]) < timeout {
			newMasterID = nextCandidateID
			break
		}
	}
	s.mu.RUnlock()

	if newMasterID == "" {
		log.Printf("[ERROR]: No available nodes to promote to master on server %s\n", s.ServerID)
		return
	}

	log.Printf("[INFO]: Sending node %s REQ to become master on server %s\n", newMasterID, s.ServerID)

	node, ok := s.GetConnectedNodeData(newMasterID)
	if !ok {
		log.Printf("[ERROR]: Node %s not found in connected nodes on server %s\n", newMasterID, s.ServerID)
		return
	}

	bumpAddr, _ := utils.BumpPort(node.Addr, 10000)
	conn, err := net.DialTimeout("tcp", bumpAddr, 2*time.Second)
	if err != nil {
		log.Printf("[ERROR]: Failed to connect to node %s on server %s: %v\n", newMasterID, s.ServerID, err)
		return
	}
	defer conn.Close()
	// Send the msg to the new master candidate
	// SUSPECT_LEADER <current_node_id> <master_node_id> <cluster_version>
	msg := fmt.Sprintf("SUSPECT_LEADER %s %s %d \n", s.ServerID, s.MasterNodeID, s.GetClusterVersion())
	_, err = conn.Write([]byte(msg))
	if err != nil {
		log.Printf("[ERROR]: Failed to send SUSPECT_LEADER to node %s on server %s: %v\n", newMasterID, s.ServerID, err)
		return
	}

	log.Printf("[INFO]: SUSPECT_LEADER sent to node %s on server %s\n", newMasterID, s.ServerID)
}
