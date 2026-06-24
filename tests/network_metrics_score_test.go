package tests

import (
	"testing"
	"time"
)

func TestComputeNetworkFactor_NoTraffic(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	
	factor := s.ComputeNetworkFactor()
	if factor != 1.0 {
		t.Errorf("expected network factor with no traffic to be 1.0, got %f", factor)
	}
}

func TestComputeNetworkFactor_PerfectTraffic(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Net.RecordClient(true, 1*time.Microsecond)
	s.Net.RecordClient(true, 1*time.Microsecond)
	s.Net.RecordPeer(true, 1*time.Microsecond)
	factor := s.ComputeNetworkFactor()
	if factor < 0.99 || factor > 1.0 {
		t.Errorf("expected network factor for perfet traffic to be close to 1.0, got %f", factor)
	}
}

func TestComputeNetworkFactor_ScenarioFromPrompt(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	for i := 0; i < 995; i++ {
		s.Net.RecordClient(true, 500*time.Microsecond)
	}
	for i := 0; i < 5; i++ {
		s.Net.RecordClient(false, 0)
	}

	for i := 0; i < 499; i++ {
		s.Net.RecordPeer(true, 500*time.Microsecond)
	}
	for i := 0; i < 1; i++ {
		s.Net.RecordPeer(false, 0)
	}
	expected := 0.4*0.995 + 0.4*0.998 + 0.2*(1.0/1.5)
	factor := s.ComputeNetworkFactor()
	
	diff := factor - expected
	if diff < -0.001 || diff > 0.001 {
		t.Errorf("expected network factor to be close to %f, got %f (diff: %f)", expected, factor, diff)
	}
}

func TestComputeNetworkFactor_DegradedNetwork(t *testing.T) {
	s := NewTestServer("node-1", "127.0.0.1:8008")
	s.Net.RecordClient(true, 1*time.Millisecond)
	s.Net.RecordClient(false, 0)
	s.Net.RecordPeer(true, 100*time.Millisecond)
	s.Net.RecordPeer(false, 0)
	
	
	expected := 0.4*0.5 + 0.4*0.5 + 0.2*(1.0/26.25)
	factor := s.ComputeNetworkFactor()
	
	diff := factor - expected
	if diff < -0.001 || diff > 0.001 {
		t.Errorf("expected network factor to be close to %f, got %f (diff: %f)", expected, factor, diff)
	}
}
