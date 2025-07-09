package bus

import (
	// "crypto/rand"
	"fmt"
	"iris/config"
	"math/rand"
	"net"
	"strings"
	"time"
)

func HandleClusterCommand(cmd string, conn net.Conn, s *config.Server) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		conn.Write([]byte("ERR empty command\n"))
		return
	}

	switch strings.ToUpper(parts[0]) {
	case "JOIN":
		{
			if len(parts) != 2 {
				conn.Write([]byte("ERR usage: JOIN SERVERID\n"))
				return
			}
			ip := conn.RemoteAddr().String()
			ServerID := parts[1]

			//send to all the existing server with 2phase commit

			//if success
		}

	}
}

func determineRange(s *config.Server) {
	newRangeStart := uint16(0)
	newRangeEnd := uint16(0)
	idx := rand.Intn(len(s.Metadata))
	if len(s.Nodes) == 0 {
		newRangeStart = s.N / 2
		newRangeEnd = s.N
	} else {
		newRangeStart = (s.Metadata[idx].End / 2) + 1
		newRangeEnd = s.Metadata[idx].End
	}

	s.Metadata[idx].End = newRangeStart - 1
}

func cordinator(new *config.Node, s *config.Server) {
	for _, node := range s.Nodes {
		conn, err := net.DialTimeout("tcp", node.Addr, 1*time.Second)
		if err != nil {
			// return "", log.Fatalln("failed to connect to peer %s: %w", node.Addr, err.Error())
		}
		defer conn.Close()

		// PREPARE SERVERID ADDR START END
		message := fmt.Sprintf("PREPARE %s %s %s %s", new.ServerID, new.Addr, "0", "150")
		_, err = conn.Write([]byte(message + "\n"))
		if err != nil {
			// return "", fmt.Errorf("failed to write to peer %s: %w", address, err)
		}
	}
}
