package config

import (
	"iris/utils"
	"log"
	"net"
	"sync"

	"github.com/google/uuid"
)

func NewServer() *Server {
	name := uuid.New().String()
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
		mu:                sync.RWMutex{},
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
