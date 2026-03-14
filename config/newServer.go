package config

import (
	"fmt"
	"iris/utils"
	"log"
	"math"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

func NewServer(group_name *string) *Server {
	name := uuid.New().String()
	ip, err := utils.GetLocalIp()
	if err != nil {
		log.Fatalf("Couldn't configure the database: %v", err)
	}

	group := "default"
	if group_name != nil && *group_name != "" {
		group = *group_name
	}

	// List of preferred ports to try for main server
	possiblePorts := []string{"8008", "8009", "8010", "8011"}
	var selectedPort string

	// Bind/probe host (IPv4). Do NOT advertise this; advertise `ip` instead.
	bindHost := "0.0.0.0"

	for _, port := range possiblePorts {
		lis, err := net.Listen("tcp4", net.JoinHostPort(bindHost, port))
		if err == nil {
			_ = lis.Close()
			selectedPort = port
			break
		}
	}
	if selectedPort == "" {
		log.Fatalf("No available main ports found from list: %v", possiblePorts)
	}

	mainPortInt, err := strconv.Atoi(selectedPort)
	if err != nil {
		log.Fatalf("Invalid selected port %q: %v", selectedPort, err)
	}
	selectedBusPort := strconv.Itoa(mainPortInt + 10000)

	busLis, err := net.Listen("tcp4", net.JoinHostPort(bindHost, selectedBusPort))
	if err != nil {
		log.Fatalf("Bus port %s not available: %v", selectedBusPort, err)
	}
	_ = busLis.Close()

	// Advertise the reachable IP (not loopback), so other nodes can reconnect after failover/failback.
	addr := net.JoinHostPort(ip, selectedPort)
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
		ResourceScore:     0,
		UnreahableNodes:   make(map[string]time.Time),
		SuspectLeaderMsg:  make(map[string]time.Time),
	}

	// node.Nodes = append(node.Nodes, &Node{ServerID: name, Addr: addr})
	fmt.Printf("🌮🌮🥪🥪: serverid:%s\n", group)
	server.Nodes[name] = &Node{ServerID: name, Addr: addr, Status: ALIVE, Group: group}
	server.Nnode = 1
	server.Group = make(map[string]*GroupInfo)
	server.Group[group] = &GroupInfo{Name: group, Nodes: []string{name}, Status: HEALTHY}
	server.MasterNodeID = name

	server.Votes = make(map[string]bool)
	server.Metadata = append(server.Metadata, &SlotRange{
		Start:    0,
		End:      16383,
		MasterID: name,
		Nodes:    []string{},
	})

	server.ResourceScore = server.DetermineResourceScore(".")
	server.Nodes[name].ResourceScore = server.ResourceScore

	log.Printf("🚀Server started on %s (bus: %s)", selectedPort, selectedBusPort)

	return &server
}

func (s *Server) DetermineResourceScore(dataDir string) float64 {
	vm, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	cores, err := cpu.Counts(true)
	if err != nil {
		return 0
	}
	diskUsage, err := disk.Usage(dataDir)
	if err != nil {
		// fallback for odd/unsupported paths on some platforms
		diskUsage, err = disk.Usage(".")
		if err != nil {
			return 0
		}
	}

	const giB = 1024.0 * 1024.0 * 1024.0

	ramGiB := float64(vm.Total) / giB
	diskGiB := float64(diskUsage.Total) / giB
	cpuCores := float64(cores)

	ramScore := math.Log2(1 + ramGiB)
	diskScore := math.Log2(1 + diskGiB)
	cpuScore := cpuCores

	return 0.5*ramScore + 0.3*cpuScore + 0.2*diskScore
}
