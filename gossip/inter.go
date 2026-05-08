package gossip

import (
	"math/rand"
)

/*Gossips the state of the node to randomly selected nodes in the group same as the current node.*/
func (g *Gossip) InterGossip() {
	members := g.view.GetGroupMembers(g.Group)

	randomNum := rand.Uint32() * uint32(len(members))

	selectedNode := members[randomNum]

	
}
