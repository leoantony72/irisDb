package tests

import (
	"bufio"
	"fmt"
	"iris/config"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// ---------------------------------------------------------------------------
// Real TCP: Bus listener accepts connections
// ---------------------------------------------------------------------------

func TestBusListener_AcceptsConnections(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer lis.Close()

	// Accept a connection in the background.
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	// Connect.
	conn, err := net.DialTimeout("tcp", lis.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	select {
	case sconn := <-accepted:
		sconn.Close()
	case <-time.After(2 * time.Second):
		t.Error("server did not accept connection")
	}
}

// ---------------------------------------------------------------------------
// SendReplicaCMD — simulated ACK REP response
// ---------------------------------------------------------------------------

func TestSendReplicaCMD_Success(t *testing.T) {
	// Start a mock bus server that replies with "ACK REP\n".
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer lis.Close()

	go func() {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read the command.
		reader := bufio.NewReader(conn)
		_, _ = reader.ReadString('\n')

		// Reply with ACK.
		conn.Write([]byte("ACK REP\n"))
	}()

	// Build a server with a node pointing to the mock listener.
	// The bus addr is the actual addr (since SendReplicaCMD uses BumpPort).
	// We need to set up the node with an addr that bumps to the listener's addr.
	lisAddr := lis.Addr().String()
	host, portStr, _ := net.SplitHostPort(lisAddr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	// The node's main addr should be (bus port - 10000).
	mainPort := port - 10000
	if mainPort < 0 {
		mainPort = 0
	}
	mainAddr := fmt.Sprintf("%s:%d", host, mainPort)

	s := NewTestServer("master-1", "127.0.0.1:8008")
	AddNodeToServer(s, "replica-1", mainAddr, "default", 1.0)

	ok := s.SendReplicaCMD("REP testkey testvalue\n", "replica-1")
	if !ok {
		t.Error("SendReplicaCMD returned false, expected true")
	}
}

// ---------------------------------------------------------------------------
// SendReplicaCMD — unknown node
// ---------------------------------------------------------------------------

func TestSendReplicaCMD_UnknownNode(t *testing.T) {
	s := NewTestServer("master-1", "127.0.0.1:8008")

	ok := s.SendReplicaCMD("REP key val\n", "ghost")
	if ok {
		t.Error("expected false for unknown node")
	}
}

// ---------------------------------------------------------------------------
// HandleCommand — SET local (master node stores key)
// ---------------------------------------------------------------------------

func TestHandleCommand_SET_Local(t *testing.T) {
	e := NewTestEngine(t)
	s := NewTestServer("master-1", "127.0.0.1:8008")
	// Ensure all slots are owned by master-1 (already the case from NewTestServer).

	// Use net.Pipe for in-memory connection.
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// HandleCommand in background.
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.HandleCommand("SET mykey myvalue", server, s)
	}()

	// Read response.
	reader := bufio.NewReader(client)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response failed: %v", err)
	}

	<-done

	resp = strings.TrimSpace(resp)
	if resp != "OK" {
		t.Errorf("SET response = %q, want %q", resp, "OK")
	}

	// Verify key was stored.
	val, err := e.Get("mykey")
	if err != nil {
		t.Fatalf("Get(mykey) failed: %v", err)
	}
	if val != "myvalue" {
		t.Errorf("stored value = %q, want %q", val, "myvalue")
	}
}

// ---------------------------------------------------------------------------
// HandleCommand — GET local
// ---------------------------------------------------------------------------

func TestHandleCommand_GET_Local(t *testing.T) {
	e := NewTestEngine(t)
	s := NewTestServer("master-1", "127.0.0.1:8008")

	// Pre-store a value.
	_ = e.Db.Set([]byte("testkey"), []byte("testval"), pebble.Sync)

	// Make master-1 a replica in its own range so GET reads locally.
	// GET picks a random node from sr.Nodes; if empty, it panics.
	// We need at least one node in sr.Nodes.
	s.Metadata[0].Nodes = []string{"master-1"}

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.HandleCommand("GET testkey", server, s)
	}()

	reader := bufio.NewReader(client)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response failed: %v", err)
	}

	<-done

	resp = strings.TrimSpace(resp)
	if resp != "testval" {
		t.Errorf("GET response = %q, want %q", resp, "testval")
	}
}

// ---------------------------------------------------------------------------
// HandleCommand — GlobalPause
// ---------------------------------------------------------------------------

func TestHandleCommand_GlobalPause(t *testing.T) {
	e := NewTestEngine(t)
	s := NewTestServer("master-1", "127.0.0.1:8008")
	s.GlobalPause.Store(true)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.HandleCommand("SET key val", server, s)
	}()

	reader := bufio.NewReader(client)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response failed: %v", err)
	}

	<-done

	resp = strings.TrimSpace(resp)
	if !strings.HasPrefix(resp, "ERR") {
		t.Errorf("expected ERR response when paused, got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// HandleCommand — empty command
// ---------------------------------------------------------------------------

func TestHandleCommand_EmptyCommand(t *testing.T) {
	e := NewTestEngine(t)
	s := NewTestServer("master-1", "127.0.0.1:8008")

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.HandleCommand("", server, s)
	}()

	reader := bufio.NewReader(client)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response failed: %v", err)
	}

	<-done

	resp = strings.TrimSpace(resp)
	if !strings.HasPrefix(resp, "ERR") {
		t.Errorf("expected ERR for empty command, got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// HandleCommand — SET bad args
// ---------------------------------------------------------------------------

func TestHandleCommand_SET_BadArgs(t *testing.T) {
	e := NewTestEngine(t)
	s := NewTestServer("master-1", "127.0.0.1:8008")

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.HandleCommand("SET onlykey", server, s)
	}()

	reader := bufio.NewReader(client)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response failed: %v", err)
	}

	<-done

	resp = strings.TrimSpace(resp)
	if !strings.HasPrefix(resp, "ERR") {
		t.Errorf("expected ERR for missing value, got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// HandleCommand — GET not found
// ---------------------------------------------------------------------------

func TestHandleCommand_GET_NotFound(t *testing.T) {
	e := NewTestEngine(t)
	s := NewTestServer("master-1", "127.0.0.1:8008")
	s.Metadata[0].Nodes = []string{"master-1"}

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.HandleCommand("GET nonexistent", server, s)
	}()

	reader := bufio.NewReader(client)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response failed: %v", err)
	}

	<-done

	resp = strings.TrimSpace(resp)
	if resp != "NOTFOUND" {
		t.Errorf("expected NOTFOUND, got %q", resp)
	}
}

// ---------------------------------------------------------------------------
// HandleCommand — DEL
// ---------------------------------------------------------------------------

func TestHandleCommand_DEL(t *testing.T) {
	e := NewTestEngine(t)
	s := NewTestServer("master-1", "127.0.0.1:8008")

	_ = e.Db.Set([]byte("delkey"), []byte("val"), pebble.Sync)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.HandleCommand("DEL delkey", server, s)
	}()

	reader := bufio.NewReader(client)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response failed: %v", err)
	}

	<-done

	resp = strings.TrimSpace(resp)
	if resp != "OK" {
		t.Errorf("DEL response = %q, want OK", resp)
	}

	// Verify key is gone.
	_, err = e.Get("delkey")
	if err == nil {
		t.Error("key should be deleted")
	}
}

// Unused import guard
var _ = config.ALIVE
