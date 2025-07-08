package bus

import (
	"bufio"
	"io"
	"log"
	"net"
)

func NewBusRoute(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Coudn't start bus at port:%s, err: %s \n", port, err.Error())
		//exits
	}

	for {
		_, err := lis.Accept()
		if err != nil {
			log.Printf("Coudn't accept connection, err:%s\n", err.Error())
			continue
		}
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	// buffer := make([]byte, 1024)
	for {
		_, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Reading err: %s", err.Error())
			}
			break
		}
	}
}
