package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"iris/bus"
	"iris/config"
	"iris/engine"
	"iris/utils"

	"github.com/google/uuid"
)

var Peers []*config.Node

func main() {
	clusterAddr := flag.String("cluster_server", "", "Address of a server in the cluster to join (optional)")
	flag.Parse()

	ID := uuid.New()
	server := config.NewServer(ID.String())
	IrisDb, err := engine.NewEngine()
	if err != nil {
		log.Fatalf("Failed to init Pebble DB: %v", err)
	}
	defer IrisDb.Close()

	lis, err := net.Listen("tcp", ":"+server.Port)
	if err != nil {
		log.Fatalf("Coudn't start Irisdb at port:%s, err: %s \n", server.Port, err.Error())
		//exits
	}
	defer lis.Close()
	log.Printf("IrisDb started at port:%s \n", server.Port)

	if *clusterAddr != "" {
		err := joinCluster(*clusterAddr, server)
		if err != nil {
			log.Printf("Warning: failed to join cluster at %s: %v", *clusterAddr, err)
		}
	}

	go bus.NewBusRoute(server)
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

func joinCluster(addr string, server *config.Server) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	joinMsg := fmt.Sprintf("JOIN %s %s\n", server.ServerID, server.Port)
	_, err = conn.Write([]byte(joinMsg))
	if err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	responseLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read JOIN response: %v", err)
	}

	responseLine = strings.TrimSpace(responseLine)
	parts := strings.Fields(responseLine)

	// Expecting: JOIN_SUCCESS START END
	if len(parts) != 3 || parts[0] != "JOIN_SUCCESS" {
		return fmt.Errorf("unexpected JOIN response: %s", responseLine)
	}

	start, err := utils.ParseUint16(parts[1])
	if err != nil {
		return err
	}
	end, err := utils.ParseUint16(parts[2])
	if err != nil {
		return err
	}

	server.Metadata[0].Start = start
	server.Metadata[0].End = end

	log.Printf("Sent JOIN request to %s: %s", addr, strings.TrimSpace(joinMsg))
	return nil
}
