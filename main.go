package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strings"

	"iris/engine"
)

type Node struct {
	ServerID string
	Addr     string
}

type Metadata struct {
	RangeMap map[string]string
}
type Server struct {
	ServerID string
	Addr     string
	N        int
	Nnode    int
	Nodes    []*Node
	Metadata string
}

func NewServer(name string) {
	ip, err := GetLocalIp()
	if err != nil {
		log.Fatalf("Coudn't Configure the Database")
	}
	addr := ip + ":8008"
	node := Server{ServerID: name, Addr: addr, N: 16384}

}

func main() {
	IrisDb, err := engine.NewEngine()
	if err != nil {
		log.Fatalf("Failed to init Pebble DB: %v", err)
	}
	defer IrisDb.Close()

	port := ":8008"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Coudn't start Irisdb at port:%s, err: %s \n", port, err.Error())
		//exits
	}
	defer lis.Close()
	log.Printf("IrisDb started at port:%s \n", port)

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("Coudn't accept connection, err:%s\n", err.Error())
			continue
		}
		go handleConnection(conn, IrisDb)
	}
}

func handleConnection(conn net.Conn, db *engine.Engine) {
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

		db.HandleCommand(strings.TrimSpace(line), conn)
	}
}
