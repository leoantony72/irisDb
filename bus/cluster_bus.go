package bus

import (
	"bufio"
	"io"
	"iris/config"
	"iris/engine"
	"iris/gossip"
	"log"
	"net"
	"strings"
	"time"
)

type Bus struct {
	server *config.Server
	db     *engine.Engine
	gossip *gossip.Gossip
}

func NewBus(server *config.Server, db *engine.Engine, gossip *gossip.Gossip) *Bus {
	return &Bus{
		server: server,
		db:     db,
		gossip: gossip,
	}
}

func (b *Bus) NewBusRoute() {
	// Force IPv4-only listening to avoid Windows dual-stack issues
	// Use "tcp4" instead of "tcp" to prevent IPv6 dual-stack binding
	lis, err := net.Listen("tcp4", ":"+b.server.BusPort)
	if err != nil {
		log.Fatalf("Coudn't start bus at port:%s, err: %s \n", b.server.BusPort, err.Error())
	}
	b.server.BusListener = lis

	log.Printf("🚀Running BusPort at localhost:%s (IPv4 only)", b.server.BusPort)

	for {
		conn, err := lis.Accept()
		if err != nil {
			if b.server.ShuttingDown.Load() {
				log.Println("[INFO] Bus listener stopped (shutdown in progress)")
				return
			}
			log.Printf("Coudn't accept connection, err:%s\n", err.Error())
			continue
		}

		b.server.Wg.Add(1)
		go b.handleConnection(conn)
	}
}

func (b *Bus) handleConnection(conn net.Conn) {
	defer b.server.Wg.Done()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		if b.server.ShuttingDown.Load() {
			log.Println("[INFO] Bus connection closing due to b.server shutdown")
			return
		}
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
			b.HandleClusterSnapshot(reader, conn)
			continue
		}

		b.HandleClusterCommand(trimmedCmd, conn)
	}
}
