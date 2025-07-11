package bus

import (
	// "crypto/rand"
	"errors"
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
			idx, startRange, EndRange := determineRange(s)
			modifiedNode := s.Nodes[idx]

			newNode := config.Node{ServerID: ServerID, Addr: ip}

			//send to all the existing server with 2phase commit
			prepareSuccess, err := cordinator(&newNode, startRange, EndRange, modifiedNode, s)

			//if success
		}

	}
}

func determineRange(s *config.Server) (int, uint16, uint16) {
	newRangeStart := uint16(0)
	newRangeEnd := uint16(0)
	idx := rand.Intn(len(s.Metadata))
	if len(s.Nodes) == 0 {
		newRangeStart = (s.N / 2) + 1
		newRangeEnd = s.N
	} else {
		newRangeStart = (s.Metadata[idx].End / 2) + 1
		newRangeEnd = s.Metadata[idx].End
	}

	return idx, newRangeStart, newRangeEnd
}

func cordinator(new *config.Node, start uint16, end uint16, mod *config.Node, s *config.Server) (bool, error) {
	for _, node := range s.Nodes {
		conn, err := net.DialTimeout("tcp", node.Addr, 1*time.Second)
		if err != nil {
			msg := fmt.Sprintf("failed to connect to peer(ID:%s) %s: %w", node.ServerID, node.Addr, err)
			return false, errors.New(msg)
		}
		defer conn.Close()

		// PREPARE SERVERID ADDR START END MODIFIED_SERVERID
		message := fmt.Sprintf("PREPARE %s %s %d %d %s", new.ServerID, new.Addr, start, end, mod.ServerID)
		_, err = conn.Write([]byte(message + "\n"))
		if err != nil {
			msg := fmt.Sprintf("failed to write to peer(ID:%s) %s: %w", node.ServerID, node.Addr, err)
			return false, errors.New(msg)
		}
	}
	return true, nil
}
