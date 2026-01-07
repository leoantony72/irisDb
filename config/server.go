package config

import (
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

	mu           sync.RWMutex
	Listener     net.Listener
	BusListener  net.Listener
	ShuttingDown atomic.Bool
	Wg           sync.WaitGroup
	ShutdownOnce sync.Once

	LastSeen        map[string]time.Time  // used by the master to track last seen times of nodes
	Group           map[string]*GroupInfo //maps groups to node IDs
	UnreahableNodes map[string]time.Time
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
