package bus

import (
	"bufio"
	"errors"
	"fmt"
	"iris/config"
	"iris/utils"
	"math/rand"
	"net"
	"sort"
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
			fmt.Println("modNode:", modifiedNode)

			newNode := config.Node{ServerID: ServerID, Addr: ip + ":" + ServerPort}

			//send to all the existing server with 2phase commit
			// !Prepare Phase
			mid, prepareSuccess, err := Prepare(&newNode, startRange, EndRange, modifiedNode, s)
			if err != nil {
				conn.Write([]byte("ERR: JOIN PREPARE(ERR) failed\n"))
				fmt.Println("Prepare err:", err.Error())
				return
			}
			if !prepareSuccess {
				conn.Write([]byte("ERR: JOIN PREPARE failed\n"))
				return
			}

			commitSuccess, err := commit(mid, s)
			if err != nil {
				conn.Write([]byte("ERR: JOIN COMMIT(ERR) failed\n"))
				return
			}
			if !commitSuccess {
				delete(s.Prepared, mid)
				conn.Write([]byte("ERR: JOIN COMMIT failed\n"))
				return
			}

			newRange := config.SlotRange{Start: startRange, End: EndRange, Nodes: []*config.Node{&newNode}}
			s.Metadata = append(s.Metadata, &newRange)
			s.Nodes = append(s.Nodes, &newNode)
			s.Nnode++
			sort.Slice(s.Metadata, func(i, j int) bool {
				return s.Metadata[i].Start < s.Metadata[j].Start
			})

			//Sends the metadata to the new server
			conn.Write([]byte("CLUSTER_METADATA_BEGIN\n"))
			for _, slot := range s.Metadata {
				var nodeInfos []string
				if len(slot.Nodes) > 0 {
					for _, node := range slot.Nodes {
						nodeInfo := fmt.Sprintf("%s@%s", node.ServerID, node.Addr)
						nodeInfos = append(nodeInfos, nodeInfo)
					}
				} else {
					nodeInfos = append(nodeInfos, "NONE")
				}
				//SLOT <Start> <End> <Node1@Addr1>,<Node2@Addr2>,...
				msg := fmt.Sprintf("SLOT %d %d %s\n", slot.Start, slot.End, strings.Join(nodeInfos, ","))
				conn.Write([]byte(msg))
			}

			conn.Write([]byte("CLUSTER_METADATA_END\n"))

			//JOIN_SUCCESS START END
			msg := fmt.Sprintf("JOIN_SUCCESS %s %s", strconv.Itoa(int(startRange)), strconv.Itoa(int(EndRange)))
			conn.Write([]byte(msg + "\n"))
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
			s.Prepared[parts[1]] = &config.PrepareMessage{MessageID: parts[1], ServerID: parts[2], Addr: parts[3], Start: start, End: end, ModifiedNodeID: parts[6]}
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
	case "SHOW":
		{
			for _, node := range s.Nodes {
				msg := fmt.Sprintf("ServerID: %s | Addr: %s\n", node.ServerID, node.Addr)
				conn.Write([]byte(msg))
			}
		}

	case "CLUSTER_METADATA_BEGIN":
		{
			reader := bufio.NewReader(conn)
			HandleIncomingClusterMetadata(reader, s)
		}
	}

}

func determineRange(s *config.Server) (int, uint16, uint16) {
	idx := rand.Intn(len(s.Nodes))
	selected := s.Metadata[idx]

	if selected.End <= selected.Start {
		return idx, selected.End, selected.End
	}

	start := selected.Start
	end := selected.End
	mid := (start + end) / 2

	newRangeStart := mid + 1
	newRangeEnd := end

	fmt.Printf("idx,start,end: %d,%d,%d\n", idx, start, end)
	fmt.Printf("NEW,idx,start,end: %d,%d,%d\n", idx, newRangeStart, newRangeEnd)
	return idx, newRangeStart, newRangeEnd
}

func Prepare(new *config.Node, start, end uint16, mod *config.Node, s *config.Server) (string, bool, error) {
	messageID := uuid.New().String()
	fmt.Println("everything:", new, mod, start, end)
	message := fmt.Sprintf("PREPARE %s %s %s %d %d %s\n",
		messageID, new.ServerID, new.Addr, start, end, mod.ServerID)
	expectedResp := fmt.Sprintf("PREPARE SUCCESS %s", messageID)

	for _, node := range s.Nodes {
		if node.ServerID == s.ServerID {
			continue
		}
		busport, _ := utils.BumpPort(node.Addr, 10000)
		conn, err := net.DialTimeout("tcp", busport, time.Second)
		if err != nil {
			return "", false, fmt.Errorf("failed to connect to peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}
		conn.SetDeadline(time.Now().Add(2 * time.Second))

		_, err = conn.Write([]byte(message))
		if err != nil {
			conn.Close()
			return "", false, fmt.Errorf("failed to write to peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}

		reader := bufio.NewReader(conn)
		resp, err := reader.ReadString('\n')
		conn.Close()
		if err != nil {
			return "", false, fmt.Errorf("failed to read response from peer(ID:%s) %s: %w", node.ServerID, busport, err)
		}

		if strings.TrimSpace(resp) != expectedResp {
			return "", false, fmt.Errorf("unexpected response from peer(ID:%s) %s: got %q, expected %q",
				node.ServerID, busport, strings.TrimSpace(resp), expectedResp)
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
		if node.ServerID == s.ServerID {
			continue
		}
		busport, _ := utils.BumpPort(node.Addr, 10000)
		conn, err := net.DialTimeout("tcp", busport, time.Second)
		if err != nil {
			msg := fmt.Sprintf("failed to connect to peer(ID:%s) %s: %s", node.ServerID, busport, err.Error())
			return false, errors.New(msg)
		}

		_, err = conn.Write([]byte(msg + "\n"))
		if err != nil {
			msg := fmt.Sprintf("failed to write to peer(ID:%s) %s: %s", node.ServerID, busport, err.Error())
			return false, errors.New(msg)
		}

		response := make([]byte, 1024)
		n, err := conn.Read(response)
		if err != nil {
			msg := fmt.Sprintf("failed to read from peer(ID:%s) %s: %s", node.ServerID, busport, err.Error())
			return false, errors.New(msg)
		}

		respStr := strings.TrimSpace(string(response[:n]))
		if respStr != "COMMIT SUCCESS" {
			msg := fmt.Sprintf("unexpected response from peer(ID:%s) %s: %s", node.ServerID, busport, respStr)
			return false, errors.New(msg)
		}
		conn.Close()
	}

	return true, nil
}

func HandleIncomingClusterMetadata(reader *bufio.Reader, s *config.Server) error {
	s.Metadata = []*config.SlotRange{}
	nodeMap := map[string]*config.Node{}

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read metadata: %w", err)
		}
		line = strings.TrimSpace(line)

		if line == "CLUSTER_METADATA_END" {
			break
		}
		if line == "CLUSTER_METADATA_BEGIN" {
			continue
		}
		if !strings.HasPrefix(line, "SLOT") {
			continue
		}

		//Format SLOT <start> <end> <node1@addr1>,<node2@addr2>
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}

		start, err := strconv.ParseUint(parts[1], 10, 16)
		if err != nil {
			return fmt.Errorf("invalid slot start: %v", err)
		}
		end, err := strconv.ParseUint(parts[2], 10, 16)
		if err != nil {
			return fmt.Errorf("invalid slot end: %v", err)
		}

		var slotNodes []*config.Node
		nodeEntries := strings.Split(parts[3], ",")
		for _, entry := range nodeEntries {
			if entry == "NONE" {
				continue
			}
			parts := strings.Split(entry, "@")
			if len(parts) != 2 {
				continue
			}
			id := parts[0]
			addr := parts[1]

			if existing, ok := nodeMap[id]; ok {
				slotNodes = append(slotNodes, existing)
			} else {
				node := &config.Node{ServerID: id, Addr: addr}
				slotNodes = append(slotNodes, node)
				nodeMap[id] = node
			}
		}

		s.Metadata = append(s.Metadata, &config.SlotRange{
			Start: uint16(start),
			End:   uint16(end),
			Nodes: slotNodes,
		})
	}

	//Rebuild s.Nodes
	s.Nodes = []*config.Node{}
	for _, node := range nodeMap {
		s.Nodes = append(s.Nodes, node)
	}
	s.Nnode = uint16(len(s.Nodes))
	return nil
}
