package config

import (
	"iris/utils"
	"log"
	"net"
	"sync"

	"github.com/google/uuid"
)

func NewServer(group_name *string) *Server {
	name := uuid.New().String()
	ip, err := utils.GetLocalIp()
	if err != nil {
		log.Fatalf("Couldn't configure the database: %v", err)
	}

	// List of preferred ports to try for main server
	possiblePorts := []string{"8008", "8009", "8010", "8011"}
	var selectedPort string

	for _, port := range possiblePorts {
		lis, err := net.Listen("tcp", ":"+port)
		if err == nil {
			lis.Close()
			selectedPort = port
			break
		}
	}
	if selectedPort == "" {
		log.Fatalf("No available main ports found from list: %v", possiblePorts)
	}

	// Calculate corresponding bus port based on selected main port
	// Bus ports are offset by 10000: 8008→18008, 8009→18009, etc.
	possibleBusPorts := []string{"18008", "18009", "18010", "18011"}
	mainPortNum := 0
	for i, port := range possiblePorts {
		if port == selectedPort {
			mainPortNum = i
			break
		}
	}
	selectedBusPort := possibleBusPorts[mainPortNum]

	// Verify bus port is available
	busLis, err := net.Listen("tcp", ":"+selectedBusPort)
	if err != nil {
		log.Fatalf("Bus port %s not available: %v", selectedBusPort, err)
	}
	busLis.Close()

	// Use 127.0.0.1 instead of localhost to force IPv4 and avoid Windows dual-stack resolution issues
	addr := "127.0.0.1" + ":" + selectedPort
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
	server.Nodes[name] = &Node{ServerID: name, Addr: addr, Status: ALIVE, Group: *group_name}
	server.Nnode = 1
	server.Group = make(map[string]*GroupInfo)
	server.Group[*group_name] = &GroupInfo{Name: *group_name, Nodes: []string{name}, Status: HEALTHY}

	server.Metadata = append(server.Metadata, &SlotRange{
		Start:    0,
		End:      16383,
		MasterID: name,
		Nodes:    []string{},
	})
 
	log.Printf("🚀Server started on %s (bus: %s)", selectedPort, selectedBusPort)

	return &server
}
