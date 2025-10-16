package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"iris/bus"
	"iris/config"
	"iris/engine"
	"iris/utils"
)

var Peers []*config.Node

func main() {
	clusterAddr := flag.String("cluster_server", "", "Address of a server in the cluster to join (optional)")
	flag.Parse()

	IrisDb, err := engine.NewEngine()
	if err != nil {
		log.Fatalf("Failed to init Pebble DB: %v", err)
	}
	defer IrisDb.Close()

	server := config.NewServer()

	lis, err := net.Listen("tcp", ":"+server.Port)
	if err != nil {
		log.Fatalf("Coudn't start Irisdb at port:%s, err: %s \n", server.Port, err.Error())
		//exits
	}
	defer lis.Close()
	log.Printf("IrisDb started at port:%s \n", server.Port)
	go bus.NewBusRoute(server, IrisDb)

	if *clusterAddr != "" {
		err := joinCluster(*clusterAddr, server)
		if err != nil {
			log.Printf("Warning: failed to join cluster at %s: %v", *clusterAddr, err)
		}
	}

	go func() {
		for {
			time.Sleep(10 * time.Second)
			log.Printf("[✅INFO]: RUNNING REPLICA VALIDATOR\n")
			if !server.ReplicationValidator() {
				server.RepairReplication()
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
func joinCluster(addr string, server *config.Server) error {
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
	if len(parts) != 3 || parts[0] != "JOIN_SUCCESS" {
		log.Printf("Error: Unexpected JOIN response format: %q", responseLine)
		return fmt.Errorf("unexpected JOIN response: %s", responseLine)
	}

	assignedStart, err := utils.ParseUint16(parts[1])
	if err != nil {
		return fmt.Errorf("invalid start slot in JOIN_SUCCESS: %w", err)
	}
	assignedEnd, err := utils.ParseUint16(parts[2])
	if err != nil {
		return fmt.Errorf("invalid end slot in JOIN_SUCCESS: %w", err)
	}

	log.Printf("✅ Successfully joined cluster via %s. This server (%s) is responsible for SlotRange [%d - %d].", addr, server.ServerID, assignedStart, assignedEnd)
	return nil
}
