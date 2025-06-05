package main

import (
	"io"
	"log"
	"net"
)

func main() {
	port := ":8008"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Coudn't start Serpicodb at port:%s, err: %s \n", port, err.Error())
		//exits
	}
	defer lis.Close()
	log.Printf("SerpicoDb started at port:%s \n", port)

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("Coudn't accept connection, err:%s\n", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Printf("Reading err: %s", err.Error())
			}
			break
		}
		_, writeErr := conn.Write(buffer[0:n])
		if writeErr != nil {
			log.Printf("Write error: %v\n", writeErr)
			break
		}
	}
}
