package gossip

import (
	"iris/serializer/pb"
	"time"
)

func (g *Gossip) handleGossipMessage(msg *pb.GossipMessage) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, incoming := range msg.States {

		local, exist := g.table[incoming.NodeId]
		if !exist {
			g.table[incoming.NodeId] = protoMessageToNodeState(incoming)
			continue
		}

		if incoming.Version > local.Version {
			local.Health = NodeHealth(incoming.Health)
			local.Version = incoming.Version
		}

		if incoming.Health == pb.NodeHealth(SUSPECT) && incoming.Version > local.Version {
			if local.LastSeen.After(time.Unix(incoming.LastSeen, 0)) {
				local.Version = incoming.Version + 1
				continue
			}
		}

		if msg.SenderId == local.NodeID {
			local.LastSeen = time.Now()
		}
	}
}

func protoMessageToNodeState(msg *pb.NodeState) *NodeState {
	return &NodeState{
		NodeID:         msg.NodeId,
		Group:          msg.Group,
		Health:         NodeHealth(msg.Health),
		LastSeen:       time.Time{},
		SuspicionCount: int(msg.SuspicionCount),
		Version:        msg.Version,
	}
}
