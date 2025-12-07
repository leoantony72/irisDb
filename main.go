package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"iris/bus"
	"iris/config"
	"iris/engine"
	"iris/utils"
)

func main() {
	clusterAddr := flag.String("cluster_server", "", "Address of a server in the cluster to join (optional)")
	flag.Parse()

	IrisDb, err := engine.NewEngine()
	if err != nil {
		log.Fatalf("Failed to init Pebble DB: %v", err)
	}
	defer IrisDb.Close()
	var server *config.Server
	loaded_data, err := CheckAndLoadMetadata(IrisDb)
	if err == nil {
		server = loaded_data
		log.Printf("[✅] Loaded server config from database. ServerID: %s", server.ServerID)
	} else {
		server = config.NewServer()
		log.Printf("[✅] Created new server config. ServerID: %s", server.ServerID)
	}

	// IMPORTANT: Always refresh the Host field with current machine IP
	// This ensures that if the server is moved to a different network or restarted
	// with a different network configuration, the Host field is up to date.
	// The Host field is used for informational purposes and cluster metadata.
	ip, err := utils.GetLocalIp()
	if err != nil {
		log.Printf("[WARN] Failed to get local IP: %v (using stored value)", err)
	} else {
		if server.Host != ip {
			log.Printf("[INFO] Updating Host IP from %s to %s", server.Host, ip)
		}
		server.Host = ip
	}

	lis, err := net.Listen("tcp", ":"+server.Port)
	if err != nil {
		log.Fatalf("Coudn't start Irisdb at port:%s, err: %s \n", server.Port, err.Error())
		//exits
	}
	defer lis.Close()
	log.Printf("🍔IrisDb started at port:%s \n", server.Port)
	log.Printf("📦Server ID:%s\n", server.ServerID)
	log.Printf("🌐Host IP:%s | Addr:%s | Bus Port:%s\n", server.Host, server.Addr, server.BusPort)
	log.Printf("📊Cluster Info - Version: %d, Nodes: %d, Slot Ranges: %d\n", server.Cluster_Version, server.Nnode, server.GetSlotRangeCount())

	go bus.NewBusRoute(server, IrisDb)

	if *clusterAddr != "" {
		err := joinCluster(*clusterAddr, server, IrisDb)
		if err != nil {
			log.Printf("Warning: failed to join cluster at %s: %v", *clusterAddr, err)
		} else {
			log.Printf("[✅] Cluster join successful. Server configuration saved to database.")
		}
	}

	// Start replica validator AFTER cluster metadata is loaded
	go func() {
		// Wait a moment to ensure metadata is stable
		time.Sleep(2 * time.Second)

		for {
			time.Sleep(30 * time.Second)
			log.Printf("[✅INFO]: RUNNING REPLICA VALIDATOR for Server %s\n", server.ServerID)
			if !server.ReplicationValidator() {
				log.Printf("[⚠️ WARNING]: Replication factor not met for server %s. Starting repair...\n", server.ServerID)
				server.RepairReplication()
			} else {
				log.Printf("[✅ SUCCESS]: Replication factor met for server %s\n", server.ServerID)
			}
		}
	}()

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("Coudn't accept connection, err:%s\n", err.Error())
			continue
		}
		go handleConnection(conn, IrisDb, server)
	}
}

func handleConnection(conn net.Conn, db *engine.Engine, server *config.Server) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	// buffer := make([]byte, 1024)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Reading err: %s", err.Error())
			}
			break
		}

		db.HandleCommand(strings.TrimSpace(line), conn, server)
	}
}

// joinCluster connects to an existing node in the cluster and requests to join.
func joinCluster(addr string, server *config.Server, db *engine.Engine) error {
	log.Printf("Attempting to join cluster via %s...", addr)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	joinMsg := fmt.Sprintf("JOIN %s %s\n", server.ServerID, server.Port)
	_, err = conn.Write([]byte(joinMsg))
	if err != nil {
		return fmt.Errorf("failed to send JOIN message: %w", err)
	}
	log.Printf("Sent JOIN message: %s", strings.TrimSpace(joinMsg))

	reader := bufio.NewReader(conn)

	// 1. Handle CLUSTER_METADATA_BEGIN block first
	line, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read first response line: %w", err)
	}
	line = strings.TrimSpace(line)

	if line != "CLUSTER_METADATA_BEGIN" {
		// Log the unexpected response for debugging
		log.Printf("Error: Expected CLUSTER_METADATA_BEGIN, got: %s", line)
		return fmt.Errorf("expected CLUSTER_METADATA_BEGIN, got: %s", line)
	}
	log.Println("Received CLUSTER_METADATA_BEGIN.")

	// Parse metadata block using the bus package's handler
	// This will update server.Metadata and server.Nodes based on the received information
	err = bus.HandleIncomingClusterMetadata(reader, server)
	if err != nil {
		return fmt.Errorf("failed to parse cluster metadata: %v", err)
	}
	log.Printf("Successfully parsed incoming cluster metadata. Current metadata version: %d", server.Cluster_Version)

	// 2. Handle JOIN_SUCCESS message after metadata
	responseLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read JOIN_SUCCESS response: %w", err)
	}

	responseLine = strings.TrimSpace(responseLine)
	parts := strings.Fields(responseLine)

	//Expected:JOIN_SUCCESS <start_slot> <end_slot>
	//or
	//Expected: REJOIN_SUCCESS count <start_slot> <end_slot> ...
	if parts[0] == "JOIN_SUCCESS" || parts[0] == "REJOIN_SUCCESS" {
		// ✅ Valid success
	} else {
		log.Printf("Error: Unexpected JOIN response format: %q", responseLine)
		return fmt.Errorf("unexpected JOIN response: %s", responseLine)
	}

	if parts[0] == "JOIN_SUCCESS" {

		assignedStart, err := utils.ParseUint16(parts[1])
		if err != nil {
			return fmt.Errorf("invalid start slot in JOIN_SUCCESS: %w", err)
		}
		assignedEnd, err := utils.ParseUint16(parts[2])
		if err != nil {
			return fmt.Errorf("invalid end slot in JOIN_SUCCESS: %w", err)
		}

		log.Printf("✅ Successfully joined cluster via %s. This server (%s) is responsible for SlotRange [%d - %d].", addr, server.ServerID, assignedStart, assignedEnd)
	} else {

		if len(parts) < 4 {
			log.Printf("Error: Unexpected REJOIN response format: %q", responseLine)
			return fmt.Errorf("unexpected REJOIN response: %s", responseLine)
		}

		count, _ := strconv.Atoi(parts[0])
		var b strings.Builder
		for i := 0; i < count; i++ {
			start, _ := strconv.Atoi(parts[2+2*i])
			end, _ := strconv.Atoi(parts[3+2*i])

			fmt.Fprintf(&b, " [%d-%d]", start, end)
		}

		log.Printf("✅ Successfully joined cluster via %s. This server (%s) is responsible for SlotRange %s", addr, server.ServerID, b.String())

	}

	// ✅ Save configuration to database after successful cluster join
	if err := db.SaveServerMetadata(server); err != nil {
		log.Printf("[WARN] Failed to save server config to database after cluster join: %v", err)
	}

	return nil
}
