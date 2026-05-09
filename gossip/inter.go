package gossip

import (
	"encoding/base64"
	"iris/serializer/pb"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/gogo/protobuf/proto"
)

/*Gossips the state of the node to randomly selected nodes in the group same as the current node.*/
func (g *Gossip) InterGossip() {
	members := g.view.GetGroupMembers(g.Group)

	randomNum := rand.Uint32() * uint32(len(members))

	selectedNode := members[randomNum]

	addr, exist := g.view.GetNodeAddr(selectedNode)
	if !exist {
		log.Println("Beware the node is not present in the server")
		return
	}

	conn, err := net.DialTimeout("tcp", addr, time.Second*3)
	if err != nil {
		return
	}

	states := g.ToGossipStates()

	data := &pb.Gossip{
		MessageType: 0,
		SenderId:    g.localID,
		States:      states,
	}
	payload, err := proto.Marshal(data)
	if err != nil {
		return
	}
	encoded := base64.StdEncoding.EncodeToString(payload)
	msg := "GOSSIP " + encoded + "\n"
	conn.Write([]byte(msg))

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

	states := make([]pb.NodeState, 0, len(g.table))

	for _, node := range g.table {
		states = append(states, ToNodeStateProtobuf(node))
	}
	return states
}
