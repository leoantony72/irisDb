package bus

import (
	"bufio"
	"errors"
	"fmt"
	"iris/config"
	"iris/utils"
	"math/rand"
	"net"
	"strconv"
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
				conn.Write([]byte("ERR usage: JOIN SERVERID PORT\n"))
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

			//JOIN_SUCCESS START END
			msg := fmt.Sprintf("JOIN_SUCCESS %s %s", strconv.Itoa(int(startRange)), strconv.Itoa(int(EndRange)))
			conn.Write([]byte(msg))
		}

	case "PREPARE":
		{
			if len(parts) != 7 {
				conn.Write([]byte("ERR: Not Enough Arguments\n"))
				return
			}
			start, err := utils.ParseUint16(parts[4])
			if err != nil {
				conn.Write([]byte("ERR: Coudn't Parse StartRange\n"))
				return
			}
			end, err := utils.ParseUint16(parts[5])
			if err != nil {
				conn.Write([]byte("ERR: Coudn't Parse EndRange\n"))
				return
			}
			s.Prepared[parts[1]] = &config.PrepareMessage{MessageID: parts[1], ServerID: parts[2], Addr: parts[3], Start: start, End: end}
			msg := fmt.Sprintf("PREPARE SUCCESS %s", parts[1])
			conn.Write([]byte(msg + "\n"))
		}

	case "COMMIT":
		{
			if len(parts) != 2 {
				conn.Write([]byte("ERR: Not Enough Arguments\n"))
				return
			}

			key, exists := s.Prepared[parts[1]]
			if !exists {
				conn.Write([]byte("ERR: MessageID doesn't exists"))
				return
			}

			newNode := &config.Node{ServerID: key.ServerID, Addr: key.Addr}
			slotRange := &config.SlotRange{Start: key.Start, End: key.End, Nodes: []*config.Node{newNode}}

			//update old range
			var modifiedRangeIdx = -1
			for i, slot := range s.Metadata {
				if len(slot.Nodes) > 0 && slot.Nodes[0].ServerID == key.ModifiedNodeID {
					modifiedRangeIdx = i
					break
				}
			}

			if modifiedRangeIdx == -1 {
				conn.Write([]byte("ERR: ModifiedNode SlotRange not found\n"))
				return
			}

			s.Metadata[modifiedRangeIdx].End = key.Start - 1
			s.Metadata = append(s.Metadata, slotRange)
			s.Nodes = append(s.Nodes, newNode)
			s.Nnode++

			conn.Write([]byte("COMMIT SUCCESS\n"))
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

func Prepare(new *config.Node, start, end uint16, mod *config.Node, s *config.Server) (string, bool, error) {
	messageID := uuid.New().String()
	message := fmt.Sprintf("PREPARE %s %s %s %d %d %s\n",
		messageID, new.ServerID, new.Addr, start, end, mod.ServerID)
	expectedResp := fmt.Sprintf("PREPARE SUCCESS %s", messageID)

	for _, node := range s.Nodes {
		conn, err := net.DialTimeout("tcp", node.Addr, time.Second)
		if err != nil {
			return "", false, fmt.Errorf("failed to connect to peer(ID:%s) %s: %w", node.ServerID, node.Addr, err)
		}
		conn.SetDeadline(time.Now().Add(2 * time.Second))

		_, err = conn.Write([]byte(message))
		if err != nil {
			conn.Close()
			return "", false, fmt.Errorf("failed to write to peer(ID:%s) %s: %w", node.ServerID, node.Addr, err)
		}

		reader := bufio.NewReader(conn)
		resp, err := reader.ReadString('\n')
		conn.Close()
		if err != nil {
			return "", false, fmt.Errorf("failed to read response from peer(ID:%s) %s: %w", node.ServerID, node.Addr, err)
		}

		if strings.TrimSpace(resp) != expectedResp {
			return "", false, fmt.Errorf("unexpected response from peer(ID:%s) %s: got %q, expected %q",
				node.ServerID, node.Addr, strings.TrimSpace(resp), expectedResp)
		}
	}

	//adds the Prepare Message to TMP if success
	s.Prepared[messageID] = &config.PrepareMessage{
		MessageID:      messageID,
		ServerID:       new.ServerID,
		Addr:           new.Addr,
		Start:          start,
		End:            end,
		ModifiedNodeID: mod.ServerID,
	}

	return messageID, true, nil
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

		_, err = conn.Write([]byte(msg + "\n"))
		if err != nil {
			msg := fmt.Sprintf("failed to write to peer(ID:%s) %s: %s", node.ServerID, node.Addr, err.Error())
			return false, errors.New(msg)
		}

		response := make([]byte, 1024)
		n, err := conn.Read(response)
		if err != nil {
			msg := fmt.Sprintf("failed to read from peer(ID:%s) %s: %s", node.ServerID, node.Addr, err.Error())
			return false, errors.New(msg)
		}

		respStr := strings.TrimSpace(string(response[:n]))
		if respStr != "COMMIT SUCCESS" {
			msg := fmt.Sprintf("unexpected response from peer(ID:%s) %s: %s", node.ServerID, node.Addr, respStr)
			return false, errors.New(msg)
		}
	}
	return true, nil
}
