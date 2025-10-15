package config

import (
	"iris/utils"
	"log"
	"net"
)

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

func NewServer(name string) *Server {
	ip, err := utils.GetLocalIp()
	if err != nil {
		log.Fatalf("Couldn't configure the database: %v", err)
	}

	// List of preferred ports to try for main server
	possiblePorts := []string{"8008", "8009", "8010", "8011"}
	var selectedPort string

	for _, port := range possiblePorts {
		lis, err := net.Listen("tcp", ip+":"+port)
		if err == nil {
			lis.Close()
			selectedPort = port
			break
		}
	}
	if selectedPort == "" {
		log.Fatalf("No available main ports found from list: %v", possiblePorts)
	}

	// List of preferred ports to try for bus communication
	possibleBusPorts := []string{"18008", "18009", "18010", "18011"}
	var selectedBusPort string

	for _, port := range possibleBusPorts {
		lis, err := net.Listen("tcp", ip+":"+port)
		if err == nil {
			lis.Close()
			selectedBusPort = port
			break
		}
	}
	if selectedBusPort == "" {
		log.Fatalf("No available bus ports found from list: %v", possibleBusPorts)
	}

	addr := ip + ":" + selectedPort
	server := Server{
		ServerID:          name,
		Addr:              addr,
		N:                 16384,
		Port:              selectedPort,
		Host:              ip,
		BusPort:           selectedBusPort,
		Nodes:             map[string]*Node{},
		ReplicationFactor: 1,
		Prepared:          make(map[string]*PrepareMessage),
	}

	// node.Nodes = append(node.Nodes, &Node{ServerID: name, Addr: addr})
	server.Nodes[name] = &Node{ServerID: name, Addr: addr}
	server.Nnode = 1

	server.Metadata = append(server.Metadata, &SlotRange{
		Start:    0,
		End:      16383,
		MasterID: name,
		Nodes:    []string{},
	})

	log.Printf("🚀Server started on %s (bus: %s)", selectedPort, selectedBusPort)

	return &server
}
