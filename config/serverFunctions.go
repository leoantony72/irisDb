package config

import (
	"fmt"
	"iris/utils"
	"net"
	"strings"
	"time"
)

// Sends the cmd(SET, DEL) to the replica's
func (s *Server) SendReplicaCMD(cmd string, replicaID string) bool {
	s.mu.RLock()
	r := s.Nodes[replicaID]
	s.mu.RUnlock()
	busaddr, _ := utils.BumpPort(r.Addr, 10000)
	conn, err := net.DialTimeout("tcp", busaddr, 2*time.Second)
	if err != nil {
		fmt.Printf("ERR: SendReplicaCMD: %s\n", err.Error())
		return false
	}

	conn.Write([]byte(cmd))

	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		fmt.Printf("SendReplicaCMD:failed to read from peer(ID:%s) %s: %w\n", r.ServerID, busaddr, err.Error())
		return false
	}

	str := strings.TrimSpace(string(response[:n]))
	if str != "ACK REP" {
		fmt.Printf("SendReplicaCMD:failed to get ACK for cmd:%s\n", cmd)
		return false
	}
	return true
}

// FindRangeIndex returns the index of the SlotRange in s.Metadata
// that matches the given start and end values.
// If no match is found, it returns -1.
func (s *Server) FindRangeIndex(start, end uint16) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, r := range s.Metadata {
		if r.Start == start && r.End == end {
			return i
		}
	}
	return -1
}

// FindRangeIndexByServerId finds and returns all the range indexes
// for which the given serverID is the master.
// Replica ranges are not returned.
func (s *Server) FindRangeIndexByServerID(serverID string) []int {
	var indices []int
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i, r := range s.Metadata {
		if r.MasterID == serverID {
			indices = append(indices, i)
		}
	}
	return indices
}
