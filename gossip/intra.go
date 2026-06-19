package gossip

import (
	"encoding/base64"
	"fmt"
	"iris/serializer/pb"
	"iris/utils"
	"log"
	"math/rand"
	"net"
	"time"

	"google.golang.org/protobuf/proto"
)

/*Gossips the state of the node to randomly selected nodes in the group same as the current node.*/
func (g *Gossip) IntraGossip() {
	for {
		time.Sleep(10 * time.Second)

		members := g.view.GetGroupMembers(g.view.GetLocalGroup())
		// debug
		log.Printf("[INTRA] localGroup=%s members=%v", g.view.GetLocalGroup(), members)
		// build peer list excluding self
		peers := make([]string, 0, len(members))
		for _, m := range members {
			if m == g.localID {
				continue
			}
			peers = append(peers, m)
		}
		if len(peers) < 1 {
			log.Println("IntraGossip: no peers in local group")
			continue
		}
		randomNum := rand.Uint32() % uint32(len(peers))
		selectedNode := peers[randomNum]

		addr, exist := g.view.GetNodeAddr(selectedNode)
		if !exist {
			log.Println("Beware the node is not present in the server")
			continue
		}

		// connect to peer's bus port (main port + 10000)
		busAddr, err := utils.BumpPort(addr, 10000)
		if err != nil {
			log.Printf("failed to derive bus addr for %s: %v", addr, err)
			continue
		}

		conn, err := net.DialTimeout("tcp", busAddr, time.Second*3)
		if err != nil {
			continue
		}

		states := g.ToGossipStates()

		data := &pb.GossipMessage{
			MessageType: 1,
			SenderId:    g.localID,
			States:      states,
		}
		payload, err := proto.Marshal(data)
		if err != nil {
			conn.Close()
			continue
		}
		encoded := base64.StdEncoding.EncodeToString(payload)
		msg := "GOSSIP " + encoded + "\n"
		_, err = conn.Write([]byte(msg))
		if err != nil {
			conn.Close()
			continue
		}
		// record the sent gossip summary
		log.Printf("[INTRA] selected peer %s (peers=%v)", selectedNode, peers)
		summary := fmt.Sprintf("%s SENT INTRA to %s | States:%d", time.Now().Format(time.RFC3339), selectedNode, len(states))
		g.AddSent(summary)
		conn.Close()
	}

}

func ToNodeStateProtobuf(nodestate *NodeState) *pb.NodeState {
	return &pb.NodeState{
		NodeId:         nodestate.NodeID,
		Group:          nodestate.Group,
		Health:         pb.NodeHealth(nodestate.Health),
		LastSeen:       nodestate.LastSeen.Unix(),
		SuspicionCount: int32(nodestate.SuspicionCount),
		Version:        nodestate.Version,
	}
}

func (g *Gossip) ToGossipStates() []*pb.NodeState {
	g.mu.Lock()
	defer g.mu.Unlock()

	states := make([]*pb.NodeState, 0, len(g.table))

	for _, node := range g.table {
		states = append(states, ToNodeStateProtobuf(node))
	}
	return states
}
