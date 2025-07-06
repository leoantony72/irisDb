package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"

	"iris/config"
	"iris/engine"

	"github.com/google/uuid"
)

var Peers []*config.Node

func main() {
	ID := uuid.New()
	server := config.NewServer(ID.String())
	IrisDb, err := engine.NewEngine()
	if err != nil {
		log.Fatalf("Failed to init Pebble DB: %v", err)
	}
	defer IrisDb.Close()

	lis, err := net.Listen("tcp", server.Port)
	if err != nil {
		log.Fatalf("Coudn't start Irisdb at port:%s, err: %s \n", server.Port, err.Error())
		//exits
	}
	defer lis.Close()
	log.Printf("IrisDb started at port:%s \n", server.Port)

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
