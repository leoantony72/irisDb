package gossip

import (
	"sync"
	"time"
)

type NodeHealth int

const (
	ALIVE NodeHealth = iota
	SUSPECT
	DEAD
)

type NodeState struct {
	NodeID         string
	Group          string
	Health         NodeHealth
	LastSeen       time.Time
	SuspicionCount int
	Version        uint64
}

/*
Provides an interface for the gossip protocol to interaact with the cluster metadata,
Gossip protocol can only read the cluster state and node information through this interface, it cannot
modify it directly or access any other part of the Server struct
*/
type ClusterView interface {
	GetGroupMembers(group string) []string
	GetNodeAddr(nodeID string) (string, bool)
	GetAllGroups() []string
	GetLocalNodeID() string
	GetLocalGroup() string
}

type GossipTable map[string]*NodeState

type Gossip struct {
	localID string
	Group   string
	view    ClusterView
	table   GossipTable
	mu      sync.RWMutex

	/*
		used to update the node states in the gossip protocol, when a nodes is marked as DEAD, the nodeID will
		sent to this channel to update the GossipTable. The master node will be responsible for senting the dead nodeID to the busport of the current node.
	*/
	DeadEvents <- chan string

	/*
		used to update the node states in the gossipTable, when a new node joins the cluster, the nodeID will be sent to this channel. The master node will be responsible for senting the new nodeID to the busport of the current node.
	*/
	JoinEvents <- chan NodeState

	/*
		used to forward the incoming gossips(both inter and intra) received through the busport to the gossip protocol, the gossips will be processed and update the gossipTable accordingly.
	*/
	IntraGossips <- chan string
	InterGossips <- chan string

	onDead func(nodeID string, evidenceCount int)
}

func NewGossip(view ClusterView, onDead func(nodeID string, evidenceCount int)) *Gossip {
	gossip := &Gossip{
		localID:      view.GetLocalNodeID(),
		Group:        view.GetLocalGroup(),
		view:         view,
		table:        make(GossipTable),
		onDead:       onDead,
		DeadEvents:   make(chan string, 10),
		JoinEvents:   make(chan NodeState, 10),
		IntraGossips: make(chan string, 10),
		InterGossips: make(chan string, 10),
	}
	gossip.seedFromView()
	return gossip
}

// will initialize the gossip table with the all the current nodes present in the servers current server config.
func (g *Gossip) seedFromView() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, group_name := range g.view.GetAllGroups() {
		for _, nodeID := range g.view.GetGroupMembers(group_name) {
			if nodeID == g.localID {
				continue
			}
			g.table[nodeID] = &NodeState{
				NodeID:   nodeID,
				Group:    group_name,
				Health:   ALIVE,
				LastSeen: time.Now(),
				Version:  0,
			}
		}
	}
}

/*
	These messges are received by the BUSPORT, sent by the MasterNode to all the nodes in the cluster, when a node is marked as DEAD, and this function will update the gossip table with the new state of the nodeID and also incremnt the version of the nodeID in the gossip table.
*/
func (g *Gossip) MonitorChannel(){
	for{
		select{
		case nodeID:= <- g.DeadEvents:
			g.mu.Lock()
			if NodeState, exists := g.table[nodeID];exists{
				NodeState.Health = DEAD
				NodeState.Version += 1
			}
			g.mu.Unlock()

		case node := <-g.JoinEvents:
			g.mu.Lock()
			g.table[node.NodeID] = &node
			g.mu.Unlock()
		}
	}
}


// @ Todo: eth markaruth !!!!!!!!!!!!
// what happens when a nodeExit message send by the master fails to reach the node ? and in the gossip protocol we receive a message saying there is a newNode in the cluster?
// HOW Will the node resolve this issue, will it req a snapshot from the master node, allengil accept the data from the gossip protocol and update the goosip table with it.
// Also if the master sends the nodeExit, and the node pass it to the gossip protocol, it deletes node entry with NodeID, but if the node receives a gossip (before the sending node has the updated cluster state) in which the same nodeID is marked as alive, then the node will update the gossip table with the new state of the nodeID as alive and then the node that is dead will still be alive in the system as a zombies. myre .  ahh no we could use versions for this. fk


/* 
//send only 
func(send chan<- string){
	send <- "hi" 
}

//receive only
func(t <- chan string){
	msg := <-t
} 

*/