package bus

import (
	"bufio"
	"io"
	"iris/config"
	"log"
	"net"
	"strings"
)

func NewBusRoute(server *config.Server) {
	lis, err := net.Listen("tcp", ":"+server.BusPort)
	if err != nil {
		log.Fatalf("Coudn't start bus at port:%s, err: %s \n", server.BusPort, err.Error())
		//exits
	}

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("Coudn't accept connection, err:%s\n", err.Error())
			continue
		}
		go handleConnection(conn, server)
	}
}

func handleConnection(conn net.Conn, server *config.Server) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Reading err: %s", err.Error())
			}
			break
		}
		HandleClusterCommand(strings.TrimSpace(line), conn, server)
	}
}
