package tests

import (
	"iris/gossip"
	"iris/serializer/pb"
	"testing"
	"time"
)


func newViewWithScores(groups map[string][]string, addrs map[string]string, scores map[string]float64, localID, localGroup string) *MockClusterView {
	return &MockClusterView{
		Groups:     groups,
		Addrs:      addrs,
		Scores:     scores,
		LocalID:    localID,
		LocalGroup: localGroup,
	}
}
func TestSeedFromView_ResourceScore(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{
			"us-east": {"node-1", "node-2"},
		},
		map[string]string{
			"node-1": "127.0.0.1:8008",
			"node-2": "127.0.0.1:8009",
		},
		map[string]float64{
			"node-1": 3.14,
			"node-2": 2.71,
		},
		"node-1", "us-east",
	)

	g := gossip.NewGossip(view)
	states := g.ToGossipStates()

	scoreByID := make(map[string]float64)
	for _, s := range states {
		scoreByID[s.NodeId] = s.ResourceScore
	}

	if got := scoreByID["node-1"]; got != 3.14 {
		t.Errorf("node-1 ResourceScore = %.4f, want 3.14", got)
	}
	if got := scoreByID["node-2"]; got != 2.71 {
		t.Errorf("node-2 ResourceScore = %.4f, want 2.71", got)
	}
}

func TestSeedFromView_MissingScore_DefaultsToZero(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1"}},
		map[string]string{"node-1": "127.0.0.1:8008"},
		nil,
		"node-1", "default",
	)

	g := gossip.NewGossip(view)
	states := g.ToGossipStates()
	if len(states) != 1 {
		t.Fatalf("expected 1 state, got %d", len(states))
	}
	if states[0].ResourceScore != 0 {
		t.Errorf("ResourceScore = %.4f, want 0 (no score configured)", states[0].ResourceScore)
	}
}
func TestResourceScoreEvent_UpdatesGossipTable(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1"}},
		map[string]string{"node-1": "127.0.0.1:8008"},
		map[string]float64{"node-1": 1.0},
		"node-1", "default",
	)

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	g.ResourceScoreEvents <- gossip.ResourceScoreEvent{
		NodeID:  "node-1",
		Score:   9.99,
		Version: 1,
	}

	time.Sleep(100 * time.Millisecond)

	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "node-1" {
			if s.ResourceScore != 9.99 {
				t.Errorf("ResourceScore = %.4f, want 9.99", s.ResourceScore)
			}
			if s.Version != 1 {
				t.Errorf("Version = %d, want 1", s.Version)
			}
			return
		}
	}
	t.Error("node-1 not found in gossip table")
}

// ---------------------------------------------------------------------------
// 3. ResourceScoreEvent with stale version is ignored
// ---------------------------------------------------------------------------

func TestResourceScoreEvent_StaleVersionIgnored(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1"}},
		map[string]string{"node-1": "127.0.0.1:8008"},
		map[string]float64{"node-1": 1.0},
		"node-1", "default",
	)

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	// Bump to version 5 first.
	g.ResourceScoreEvents <- gossip.ResourceScoreEvent{NodeID: "node-1", Score: 5.0, Version: 5}
	time.Sleep(60 * time.Millisecond)

	// Now send a stale event with version 3.
	g.ResourceScoreEvents <- gossip.ResourceScoreEvent{NodeID: "node-1", Score: 1.0, Version: 3}
	time.Sleep(60 * time.Millisecond)

	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "node-1" {
			if s.ResourceScore != 5.0 {
				t.Errorf("stale event applied: ResourceScore = %.4f, want 5.0", s.ResourceScore)
			}
			return
		}
	}
	t.Error("node-1 not found")
}

// ---------------------------------------------------------------------------
// 4. ResourceScoreEvent creates entry when node is not yet in table
// ---------------------------------------------------------------------------

func TestResourceScoreEvent_CreatesNewEntry(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1"}},
		map[string]string{"node-1": "127.0.0.1:8008"},
		nil,
		"node-1", "default",
	)

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	g.ResourceScoreEvents <- gossip.ResourceScoreEvent{
		NodeID:  "node-brand-new",
		Score:   7.77,
		Version: 1,
	}
	time.Sleep(100 * time.Millisecond)

	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "node-brand-new" {
			if s.ResourceScore != 7.77 {
				t.Errorf("ResourceScore = %.4f, want 7.77", s.ResourceScore)
			}
			return
		}
	}
	t.Error("node-brand-new not created in gossip table via ResourceScoreEvent")
}

// ---------------------------------------------------------------------------
// 5. Incoming gossip message propagates ResourceScore (version-gated)
// ---------------------------------------------------------------------------

func TestHandleGossipMessage_PropagatesResourceScore(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1", "node-2"}},
		map[string]string{
			"node-1": "127.0.0.1:8008",
			"node-2": "127.0.0.1:8009",
		},
		map[string]float64{"node-1": 1.0, "node-2": 1.0},
		"node-1", "default",
	)

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	// Send gossip from node-2 saying its score is now 8.88, version 10.
	g.IntraGossipsChan <- &pb.GossipMessage{
		MessageType: 1,
		SenderId:    "node-2",
		States: []*pb.NodeState{
			{
				NodeId:        "node-2",
				Group:         "default",
				Health:        pb.NodeHealth(gossip.ALIVE),
				LastSeen:      time.Now().Unix(),
				Version:       10,
				ResourceScore: 8.88,
			},
		},
	}
	time.Sleep(100 * time.Millisecond)

	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "node-2" {
			if s.ResourceScore != 8.88 {
				t.Errorf("ResourceScore = %.4f, want 8.88", s.ResourceScore)
			}
			if s.Version != 10 {
				t.Errorf("Version = %d, want 10", s.Version)
			}
			return
		}
	}
	t.Error("node-2 not found in gossip states")
}

// ---------------------------------------------------------------------------
// 6. Stale gossip version does NOT overwrite a newer ResourceScore
// ---------------------------------------------------------------------------

func TestHandleGossipMessage_StaleResourceScoreIgnored(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1", "node-2"}},
		map[string]string{
			"node-1": "127.0.0.1:8008",
			"node-2": "127.0.0.1:8009",
		},
		map[string]float64{"node-1": 1.0, "node-2": 1.0},
		"node-1", "default",
	)

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	// First update — high version, high score.
	g.IntraGossipsChan <- &pb.GossipMessage{
		SenderId: "node-2",
		States: []*pb.NodeState{
			{NodeId: "node-2", Version: 20, ResourceScore: 9.99, LastSeen: time.Now().Unix()},
		},
	}
	time.Sleep(80 * time.Millisecond)

	// Stale update — lower version.
	g.InterGossipsChan <- &pb.GossipMessage{
		SenderId: "node-2",
		States: []*pb.NodeState{
			{NodeId: "node-2", Version: 5, ResourceScore: 0.01, LastSeen: time.Now().Unix()},
		},
	}
	time.Sleep(80 * time.Millisecond)

	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "node-2" {
			if s.ResourceScore != 9.99 {
				t.Errorf("stale gossip overwrote score: got %.4f, want 9.99", s.ResourceScore)
			}
			return
		}
	}
	t.Error("node-2 not found in states")
}

// ---------------------------------------------------------------------------
// 7. OnResourceScoreUpdate callback is fired when score changes via gossip
// ---------------------------------------------------------------------------

func TestHandleGossipMessage_CallsCallback(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1", "node-2"}},
		map[string]string{
			"node-1": "127.0.0.1:8008",
			"node-2": "127.0.0.1:8009",
		},
		map[string]float64{"node-1": 1.0, "node-2": 1.0},
		"node-1", "default",
	)

	g := gossip.NewGossip(view)

	called := make(chan struct{ id string; score float64; version uint64 }, 1)
	g.OnResourceScoreUpdate = func(nodeID string, score float64, version uint64) {
		called <- struct {
			id      string
			score   float64
			version uint64
		}{nodeID, score, version}
	}

	go g.MonitorChannel()

	g.IntraGossipsChan <- &pb.GossipMessage{
		SenderId: "node-2",
		States: []*pb.NodeState{
			{NodeId: "node-2", Version: 5, ResourceScore: 4.56, LastSeen: time.Now().Unix()},
		},
	}

	select {
	case ev := <-called:
		if ev.id != "node-2" {
			t.Errorf("callback nodeID = %q, want node-2", ev.id)
		}
		if ev.score != 4.56 {
			t.Errorf("callback score = %.4f, want 4.56", ev.score)
		}
		if ev.version != 5 {
			t.Errorf("callback version = %d, want 5", ev.version)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("OnResourceScoreUpdate callback was not called within 500ms")
	}
}

// ---------------------------------------------------------------------------
// 8. Callback is NOT fired when score is unchanged (same value)
// ---------------------------------------------------------------------------

func TestHandleGossipMessage_NoCallbackOnSameScore(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1", "node-2"}},
		map[string]string{
			"node-1": "127.0.0.1:8008",
			"node-2": "127.0.0.1:8009",
		},
		map[string]float64{"node-1": 1.0, "node-2": 5.0},
		"node-1", "default",
	)

	g := gossip.NewGossip(view)

	called := make(chan struct{}, 1)
	g.OnResourceScoreUpdate = func(_ string, _ float64, _ uint64) {
		called <- struct{}{}
	}

	go g.MonitorChannel()

	// node-2 score is already 5.0 in the table; send same score with higher version.
	g.IntraGossipsChan <- &pb.GossipMessage{
		SenderId: "node-2",
		States: []*pb.NodeState{
			{NodeId: "node-2", Version: 1, ResourceScore: 5.0, LastSeen: time.Now().Unix()},
		},
	}

	select {
	case <-called:
		t.Error("OnResourceScoreUpdate should not be called when score is unchanged")
	case <-time.After(200 * time.Millisecond):
		// good — callback not triggered
	}
}

// ---------------------------------------------------------------------------
// 9. ToNodeStateProtobuf serialises ResourceScore
// ---------------------------------------------------------------------------

func TestToNodeStateProtobuf_IncludesResourceScore(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1"}},
		map[string]string{"node-1": "127.0.0.1:8008"},
		map[string]float64{"node-1": 6.28},
		"node-1", "default",
	)

	g := gossip.NewGossip(view)
	states := g.ToGossipStates()

	if len(states) != 1 {
		t.Fatalf("expected 1 state, got %d", len(states))
	}
	if states[0].ResourceScore != 6.28 {
		t.Errorf("proto ResourceScore = %.4f, want 6.28", states[0].ResourceScore)
	}
}

// ---------------------------------------------------------------------------
// 10. protoMessageToNodeState round-trip preserves ResourceScore (via new node)
// ---------------------------------------------------------------------------

func TestHandleGossipMessage_NewNode_IncludesResourceScore(t *testing.T) {
	view := newViewWithScores(
		map[string][]string{"default": {"node-1"}},
		map[string]string{"node-1": "127.0.0.1:8008"},
		nil,
		"node-1", "default",
	)

	g := gossip.NewGossip(view)
	go g.MonitorChannel()

	// "remote-node" is new — will be created via protoMessageToNodeState.
	g.InterGossipsChan <- &pb.GossipMessage{
		SenderId: "remote-node",
		States: []*pb.NodeState{
			{
				NodeId:        "remote-node",
				Group:         "eu-west",
				Health:        pb.NodeHealth(gossip.ALIVE),
				LastSeen:      time.Now().Unix(),
				Version:       3,
				ResourceScore: 2.22,
			},
		},
	}
	time.Sleep(100 * time.Millisecond)

	states := g.ToGossipStates()
	for _, s := range states {
		if s.NodeId == "remote-node" {
			if s.ResourceScore != 2.22 {
				t.Errorf("new node ResourceScore = %.4f, want 2.22", s.ResourceScore)
			}
			return
		}
	}
	t.Error("remote-node not found in gossip table")
}
