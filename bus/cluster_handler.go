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

	"github.com/google/uuid"
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
			if len(parts) != 3 {
				conn.Write([]byte("ERR usage: JOIN SERVERID\n"))
				return
			}
			ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
			ServerID := parts[1]
			ServerPort := parts[2]
			idx, startRange, EndRange := determineRange(s)
			modifiedNode := s.Nodes[idx]

			newNode := config.Node{ServerID: ServerID, Addr: ip + ":" + ServerPort}

			//send to all the existing server with 2phase commit
			// !Prepare Phase
			mid, prepareSuccess, err := Prepare(&newNode, startRange, EndRange, modifiedNode, s)
			if err != nil {
				conn.Write([]byte("ERR: JOIN failed\n"))
				return
			}
			if !prepareSuccess {
				conn.Write([]byte("ERR: JOIN failed\n"))
				return
			}

			commitSuccess, err := commit(mid, s)
			if err != nil {
				conn.Write([]byte("ERR: JOIN failed\n"))
				return
			}
			if !commitSuccess {
				delete(s.Prepared, mid)
				conn.Write([]byte("ERR: JOIN failed\n"))
				return
			}

			//update metadata
			newRange := config.SlotRange{Start: startRange, End: EndRange, Nodes: []*config.Node{&newNode}}
			s.Metadata = append(s.Metadata, &newRange)
			s.Nodes = append(s.Nodes, &newNode)
			s.Nnode++
		}
	case "PREPARE":
		{
			if len(parts) != 6 {
				conn.Write([]byte("ERR usage: PREPARE SERVERID ADDR START END MODIFIED_SERVERID\n"))
				return
			}
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

func Prepare(new *config.Node, start uint16, end uint16, mod *config.Node, s *config.Server) (string, bool, error) {
	MessageID := uuid.New().String()
	message := fmt.Sprintf("PREPARE %s %s %s %d %d %s", MessageID, new.ServerID, new.Addr, start, end, mod.ServerID)
	for _, node := range s.Nodes {
		conn, err := net.DialTimeout("tcp", node.Addr, time.Second)
		if err != nil {
			msg := fmt.Sprintf("failed to connect to peer(ID:%s) %s: %s", node.ServerID, node.Addr, err.Error())
			return "", false, errors.New(msg)
		}
		defer conn.Close()

		// PREPARE SERVERID ADDR START END MODIFIED_SERVERID
		_, err = conn.Write([]byte(message + "\n"))
		if err != nil {
			msg := fmt.Sprintf("failed to write to peer(ID:%s) %s: %s", node.ServerID, node.Addr, err.Error())
			return "", false, errors.New(msg)
		}
	}
	s.Prepared[MessageID] = message
	return MessageID, true, nil
}

func commit(mid string, s *config.Server) (bool, error) {
	msg := fmt.Sprintf("COMMIT %s", mid)
	for _, node := range s.Nodes {
		conn, err := net.DialTimeout("tcp", node.Addr, time.Second)
		if err != nil {
			msg := fmt.Sprintf("failed to connect to peer(ID:%s) %s: %s", node.ServerID, node.Addr, err.Error())
			return false, errors.New(msg)
		}
		defer conn.Close()

		// COMMIT MESSAGEID
		_, err = conn.Write([]byte(msg + "\n"))
		if err != nil {
			msg := fmt.Sprintf("failed to write to peer(ID:%s) %s: %s", node.ServerID, node.Addr, err.Error())
			return false, errors.New(msg)
		}
	}
	return true, nil
}
