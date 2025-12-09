package bus

import (
	"bufio"
	"io"
	"iris/config"
	"iris/engine"
	"log"
	"net"
	"strings"
	"time"
)

func NewBusRoute(server *config.Server, db *engine.Engine) {
	// Force IPv4-only listening to avoid Windows dual-stack issues
	// Use "tcp4" instead of "tcp" to prevent IPv6 dual-stack binding
	lis, err := net.Listen("tcp4", "127.0.0.1:"+server.BusPort)
	if err != nil {
		log.Fatalf("Coudn't start bus at port:%s, err: %s \n", server.BusPort, err.Error())
		//exits
	}

	log.Printf("🚀Running BusPort at 127.0.0.1:%s (IPv4 only)", server.BusPort)

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("Coudn't accept connection, err:%s\n", err.Error())
			continue
		}
		go handleConnection(conn, server, db)
	}
}

func handleConnection(conn net.Conn, server *config.Server, db *engine.Engine) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		// Set read deadline to detect stalled connections
		conn.SetReadDeadline(time.Now().Add(1 * time.Minute))
		
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Reading err: %s", err.Error())
			}
			break
		}
		
		trimmedCmd := strings.TrimSpace(line)
		
		// Special handling for SNAPSHOT: it has binary data following the text command
		// We need to handle it before the buffered reader interferes
		if strings.ToUpper(trimmedCmd) == "SNAPSHOT" {
			// For SNAPSHOT, we need to use the raw connection without bufio interference
			// Close the current connection and let the sender establish a new one for binary data
			// Actually, SNAPSHOT data comes on the same connection right after this line
			// We pass the reader to HandleClusterSnapshot so it can continue reading from the buffered stream
			HandleClusterSnapshot(reader, conn, server)
			continue
		}
		
		HandleClusterCommand(trimmedCmd, conn, server, db)
	}
}
