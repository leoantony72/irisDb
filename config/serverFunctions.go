package config

import (
	"fmt"
	"iris/utils"
	"log"
	"net"
	"sort"
	"strings"
	"time"
)

// Sends the cmd(SET, DEL) to the replica's
func (s *Server) SendReplicaCMD(cmd string, replicaID string) bool {
	s.mu.RLock()
	r := s.Nodes[replicaID]
	s.mu.RUnlock()
	busaddr, _ := utils.BumpPort(r.Addr, 10000)
	conn, err := net.DialTimeout("tcp", busaddr, 10*time.Second)
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
	return s.findRangeIndexLocked(start, end)
}

// findRangeIndexLocked is the internal non-locking version of FindRangeIndex.
// Caller must already hold at least a read lock (s.mu.RLock()) on the server.
func (s *Server) findRangeIndexLocked(start, end uint16) int {
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

// applyCommitChanges encapsulates the logic for updating metadata and node lists
// after a successful COMMIT phase. This function is called by both the coordinating
// node and participating nodes.
func (s *Server) ApplyCommitChanges(preparedMsg *PrepareMessage) error {
	// 1. Add the new node to the global nodes map
	s.mu.Lock()
	if _, ok := s.Nodes[preparedMsg.TargetNodeID]; !ok {
		s.Nodes[preparedMsg.TargetNodeID] = &Node{ServerID: preparedMsg.TargetNodeID, Addr: preparedMsg.Addr}
		s.Nnode++
	}
	s.mu.Unlock()

	// 2. Find the modified SlotRange.
	var modifiedRangeIdx = -1
	s.mu.RLock()
	for i, sr := range s.Metadata {
		if sr.MasterID == preparedMsg.ModifiedNodeID &&
			preparedMsg.Start >= sr.Start && preparedMsg.End == sr.End &&
			preparedMsg.Start > sr.Start {
			modifiedRangeIdx = i
			break
		}
	}
	s.mu.Unlock()

	if modifiedRangeIdx == -1 {
		return fmt.Errorf("ModifiedNode SlotRange not found for expected split pattern. PreparedMsg: %+v, Current Metadata: %+v", preparedMsg, s.Metadata)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.Metadata[modifiedRangeIdx].End = preparedMsg.Start - 1
	s.Metadata[modifiedRangeIdx].Nodes = preparedMsg.ModifiedNodeReplicaList

	// Create the new slot range for the joining node.
	newJoinNodeRange := &SlotRange{
		Start:    preparedMsg.Start,
		End:      preparedMsg.End,
		MasterID: preparedMsg.TargetNodeID,
		Nodes:    preparedMsg.TargetNodeReplicaList,
	}

	s.Metadata = append(s.Metadata, newJoinNodeRange)
	sort.Slice(s.Metadata, func(i, j int) bool {
		return s.Metadata[i].Start < s.Metadata[j].Start
	})

	s.Cluster_Version++

	delete(s.Prepared, preparedMsg.MessageID)
	return nil
}

// HasNode checks if a node with the given ID exists in the server's node list.
func (s *Server) HasNode(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.Nodes[id]
	return ok
}

func (s *Server) GetMasterNodeForRangeIdx(modifiedRangeIdx int) (*Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if modifiedRangeIdx < 0 || modifiedRangeIdx >= len(s.Metadata) {
		return nil, false
	}

	masterID := s.Metadata[modifiedRangeIdx].MasterID
	node, ok := s.Nodes[masterID]
	return node, ok
}

func (s *Server) DeletePrepared(mid string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Prepared, mid)
}

// GetSlotRangesByIndices returns []SlotRange of the given indices of the s.Metadata
// More efficient than getting individual SlotRanges cause GetSlotRangesByIndices only
// uses 1 locking.
func (s *Server) GetSlotRangesByIndices(indices []int) []SlotRange {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]SlotRange, 0, len(indices))
	for _, idx := range indices {
		if idx < 0 || idx >= len(s.Metadata) {
			continue
		}
		r := s.Metadata[idx]
		c := *r
		if r.Nodes != nil {
			c.Nodes = append([]string(nil), r.Nodes...)
		}
		out = append(out, c)
	}
	return out
}

// GetSlotRangeByIndex returns a safe snapshot copy of the slot range at idx.
func (s *Server) GetSlotRangeByIndex(idx int) (*SlotRange, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if idx < 0 || idx >= len(s.Metadata) {
		return nil, false
	}
	r := s.Metadata[idx]
	copyRange := *r
	if r.Nodes != nil {
		copyRange.Nodes = append([]string(nil), r.Nodes...)
	}

	return &copyRange, true
}

func (s *Server) GetServerMetadata() []SlotRange {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slots := make([]SlotRange, 0, len(s.Metadata))

	for _, r := range s.Metadata {
		if r == nil {
			continue
		}
		copyRange := *r
		if r.Nodes != nil {
			copyRange.Nodes = append([]string(nil), r.Nodes...)
		}

		slots = append(slots, copyRange)
	}

	return slots
}

// GetConnectedNodeData returns the copy of Node given the node ID
func (s *Server) GetConnectedNodeData(id string) (Node, bool) {
	s.mu.RLock()
	n, ok := s.Nodes[id]
	s.mu.RUnlock()

	if !ok || n == nil {
		return Node{}, false
	}
	copy := *n
	return copy, true
}

// GetCommitPeers returns all peers except self, as a snapshot.
func (s *Server) GetCommitPeers() []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()

	peers := make([]Node, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		if n.ServerID == s.ServerID {
			continue
		}
		peers = append(peers, Node{
			ServerID: n.ServerID,
			Addr:     n.Addr,
		})
	}
	return peers
}

// Returns the basic info needed for the header.
func (s *Server) GetBasicInfo() (serverID, host, addr, busPort string, version uint64, totalNodes, totalSlots uint16) {
	log.Printf("GetBasicInfo: About to acquire read lock")
	s.mu.RLock()
	log.Printf("GetBasicInfo: Read lock acquired")
	defer s.mu.RUnlock()

	return s.ServerID, s.Host, s.Addr, s.BusPort, s.Cluster_Version, s.Nnode, s.N
}

// Snapshot of nodes(MAP) as value copies.
func (s *Server) GetNodesSnapshot() []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodes := make([]Node, 0, len(s.Nodes))
	for _, n := range s.Nodes {
		if n == nil {
			continue
		}
		nodes = append(nodes, *n)
	}
	return nodes
}

// FindNodeIdx returns the index in s.Metadata of the SlotRange that contains `slot`.
// Returns -1 if metadata is empty or no matching range is found.
// Assumes s.Metadata is sorted by SlotRange.End ascending.
func (s *Server) FindNodeIdx(slot uint16) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n := len(s.Metadata)
	if n == 0 {
		return -1
	}

	idx := sort.Search(n, func(i int) bool {
		return s.Metadata[i].End >= slot
	})

	if idx == n {
		idx = 0
	}
	r := s.Metadata[idx]
	if r == nil {
		return -1
	}
	if slot < r.Start || slot > r.End {
		return -1
	}
	return idx
}

func (s *Server) UnreacableNodeList() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var b strings.Builder
	for k, _ := range s.UnreahableNodes {
		fmt.Fprintf(&b, "%s,", k)
	}
	return b.String()
}

func (s *Server) GetServerGroup() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	node := s.Nodes[s.ServerID]
	return node.Group
}

// func (s *Server) BeginShutdown() {
// 	s.ShutdownOnce.Do(func() {
// 		log.Println("[INFO] Shutdown initiated")

// 		s.ShuttingDown.Store(true)

// 		if s.Listener != nil {
// 			s.Listener.Close()
// 		}

// 		go func() {
// 			s.Wg.Wait()
// 			s.ShutdownCh <- struct{}{}
// 		}()
// 	})
// }
