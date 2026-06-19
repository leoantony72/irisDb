package tests

import (
	"iris/gossip"
	"iris/serializer/pb"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// NewGossip seeds table from view
// ---------------------------------------------------------------------------

func TestNewGossip_SeedsTable(t *testing.T) {
	view := &MockClusterView{
		Groups: map[string][]string{
			"us-east": {"node-1", "node-2"},
			"eu-west": {"node-3"},
		},
		Addrs: map[string]string{
			"node-1": "127.0.0.1:8008",
			"node-2": "127.0.0.1:8009",
			"node-3": "127.0.0.1:8010",
		},
		LocalID:    "node-1",
		LocalGroup: "us-east",
	}

	g := gossip.NewGossip(view)

	// Gossip table should have 3 entries (all nodes from all groups).
	states := g.ToGossipStates()
	if len(states) != 3 {
		t.Errorf("gossip table has %d entries, want 3", len(states))
	}
}

// ---------------------------------------------------------------------------
// MonitorChannel — DeadEvent
// ---------------------------------------------------------------------------

func TestMonitorChannel_DeadEvent(t *testing.T) {
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"node-1", "node-2"}},
		Addrs:      map[string]string{"node-1": "127.0.0.1:8008", "node-2": "127.0.0.1:8009"},
		LocalID:    "node-1",
		LocalGroup: "default",
	}

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	// Send dead event.
	g.DeadEvents <- "node-2"

	// Give goroutine time to process.
	time.Sleep(100 * time.Millisecond)

	// Check gossip states — node-2 should be DEAD.
	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "node-2" {
			if s.Health != pb.NodeHealth(gossip.DEAD) {
				t.Errorf("node-2 health = %v, want DEAD", s.Health)
			}
			if s.Version < 1 {
				t.Errorf("node-2 version = %d, want >= 1", s.Version)
			}
			return
		}
	}
	t.Error("node-2 not found in gossip states")
}

// ---------------------------------------------------------------------------
// MonitorChannel — JoinEvent
// ---------------------------------------------------------------------------

func TestMonitorChannel_JoinEvent(t *testing.T) {
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"node-1"}},
		Addrs:      map[string]string{"node-1": "127.0.0.1:8008"},
		LocalID:    "node-1",
		LocalGroup: "default",
	}

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	g.JoinEvents <- gossip.NodeState{
		NodeID:   "node-new",
		Group:    "default",
		Health:   gossip.ALIVE,
		LastSeen: time.Now(),
		Version:  1,
	}

	time.Sleep(100 * time.Millisecond)

	states := g.ToGossipStates()
	found := false
	for _, s := range states {
		if s.NodeId == "node-new" {
			found = true
			if s.Health != pb.NodeHealth(gossip.ALIVE) {
				t.Errorf("node-new health = %v, want ALIVE", s.Health)
			}
		}
	}
	if !found {
		t.Error("node-new not found in gossip table after JoinEvent")
	}
}

// ---------------------------------------------------------------------------
// HandleGossipMessage — new node
// ---------------------------------------------------------------------------

func TestHandleGossipMessage_NewNode(t *testing.T) {
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"node-1"}},
		Addrs:      map[string]string{"node-1": "127.0.0.1:8008"},
		LocalID:    "node-1",
		LocalGroup: "default",
	}

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	msg := &pb.GossipMessage{
		MessageType: 0,
		SenderId:    "node-1",
		States: []*pb.NodeState{
			{
				NodeId:   "remote-node",
				Group:    "eu-west",
				Health:   pb.NodeHealth(gossip.ALIVE),
				LastSeen: time.Now().Unix(),
				Version:  1,
			},
		},
	}

	// Send via inter gossip channel.
	g.InterGossipsChan <- msg
	time.Sleep(100 * time.Millisecond)

	states := g.ToGossipStates()
	found := false
	for _, s := range states {
		if s.NodeId == "remote-node" {
			found = true
		}
	}
	if !found {
		t.Error("remote-node not added to gossip table")
	}
}

// ---------------------------------------------------------------------------
// HandleGossipMessage — higher version updates
// ---------------------------------------------------------------------------

func TestHandleGossipMessage_HigherVersion(t *testing.T) {
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"node-1", "node-2"}},
		Addrs:      map[string]string{"node-1": "127.0.0.1:8008", "node-2": "127.0.0.1:8009"},
		LocalID:    "node-1",
		LocalGroup: "default",
	}

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	// node-2 becomes SUSPECT with higher version.
	msg := &pb.GossipMessage{
		MessageType: 0,
		SenderId:    "node-1",
		States: []*pb.NodeState{
			{
				NodeId:   "node-2",
				Group:    "default",
				Health:   pb.NodeHealth(gossip.SUSPECT),
				LastSeen: time.Now().Unix(),
				Version:  100, // much higher than initial 0
			},
		},
	}

	g.IntraGossipsChan <- msg
	time.Sleep(100 * time.Millisecond)

	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "node-2" {
			if s.Health != pb.NodeHealth(gossip.SUSPECT) {
				t.Errorf("node-2 health = %v, want SUSPECT", s.Health)
			}
			return
		}
	}
	t.Error("node-2 not found in states")
}

// ---------------------------------------------------------------------------
// HandleGossipMessage — stale version ignored
// ---------------------------------------------------------------------------

func TestHandleGossipMessage_StaleVersion(t *testing.T) {
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"node-1", "node-2"}},
		Addrs:      map[string]string{"node-1": "127.0.0.1:8008", "node-2": "127.0.0.1:8009"},
		LocalID:    "node-1",
		LocalGroup: "default",
	}

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	// First: update node-2 to version 10 with SUSPECT.
	g.InterGossipsChan <- &pb.GossipMessage{
		SenderId: "node-1",
		States: []*pb.NodeState{
			{NodeId: "node-2", Health: pb.NodeHealth(gossip.SUSPECT), Version: 10, LastSeen: time.Now().Unix()},
		},
	}
	time.Sleep(100 * time.Millisecond)

	// Now send stale version 5 saying ALIVE — should be ignored.
	g.IntraGossipsChan <- &pb.GossipMessage{
		SenderId: "node-1",
		States: []*pb.NodeState{
			{NodeId: "node-2", Health: pb.NodeHealth(gossip.ALIVE), Version: 5, LastSeen: time.Now().Unix()},
		},
	}
	time.Sleep(100 * time.Millisecond)

	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "node-2" {
			// Should still be SUSPECT since version 5 < 10.
			if s.Health != pb.NodeHealth(gossip.SUSPECT) {
				t.Errorf("stale update applied: health = %v, want SUSPECT", s.Health)
			}
			return
		}
	}
}

// ---------------------------------------------------------------------------
// AddSent / AddRecv / GetSent / GetRecv
// ---------------------------------------------------------------------------

func TestAddSent_AddRecv(t *testing.T) {
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"node-1"}},
		Addrs:      map[string]string{"node-1": "127.0.0.1:8008"},
		LocalID:    "node-1",
		LocalGroup: "default",
	}

	g := gossip.NewGossip(view)

	g.AddSent("sent-msg-1")
	g.AddSent("sent-msg-2")
	g.AddRecv("recv-msg-1")

	sent := g.GetSent()
	if len(sent) != 2 {
		t.Errorf("GetSent() len = %d, want 2", len(sent))
	}

	recv := g.GetRecv()
	if len(recv) != 1 {
		t.Errorf("GetRecv() len = %d, want 1", len(recv))
	}
}

// ---------------------------------------------------------------------------
// ToGossipStates / ToNodeStateProtobuf
// ---------------------------------------------------------------------------

func TestToGossipStates(t *testing.T) {
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"node-1"}},
		Addrs:      map[string]string{"node-1": "127.0.0.1:8008"},
		LocalID:    "node-1",
		LocalGroup: "default",
	}

	g := gossip.NewGossip(view)
	states := g.ToGossipStates()

	if len(states) != 1 {
		t.Fatalf("expected 1 state, got %d", len(states))
	}
	if states[0].NodeId != "node-1" {
		t.Errorf("NodeId = %q, want node-1", states[0].NodeId)
	}
	if states[0].Health != pb.NodeHealth(gossip.ALIVE) {
		t.Errorf("Health = %v, want ALIVE", states[0].Health)
	}
}

// ---------------------------------------------------------------------------
// MonitorChannel — stop channel
// ---------------------------------------------------------------------------

func TestMonitorChannel_StopChannel(t *testing.T) {
	view := &MockClusterView{
		Groups:     map[string][]string{"default": {"node-1"}},
		Addrs:      map[string]string{"node-1": "127.0.0.1:8008"},
		LocalID:    "node-1",
		LocalGroup: "default",
	}

	g := gossip.NewGossip(view)

	done := make(chan struct{})
	go func() {
		g.MonitorChannel()
		close(done)
	}()

	// Closing DeadEvents should cause MonitorChannel to return.
	close(g.DeadEvents)

	select {
	case <-done:
		// MonitorChannel exited as expected.
	case <-time.After(2 * time.Second):
		t.Error("MonitorChannel did not exit after channel close")
	}
}
