package config

type Node struct {
	ServerID string
	Addr     string
}

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
}

