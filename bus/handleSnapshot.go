package bus

import (
	"bufio"
	"encoding/gob"
	"iris/config"
	"iris/gossip"
	"log"
	"net"
	"time"
)

func (b *Bus) HandleClusterSnapshot(reader *bufio.Reader, conn net.Conn) {
	// The text "SNAPSHOT" command has already been read by the caller
	// Now read the binary gob-encoded snapshot from the reader/connection

	dec := gob.NewDecoder(reader)

	var snap config.ClusterSnapshot
	if err := dec.Decode(&snap); err != nil {
		log.Printf("Error decoding snapshot: %v", err)
		conn.Write([]byte("ERR: failed to decode snapshot\n"))
		return
	}

	log.Printf("Received cluster snapshot, applying...")
	b.server.ApplyClusterSnapshot(snap)
	// Update gossip table via JoinEvents for all nodes in snapshot
	if b.gossip != nil {
		for _, n := range snap.Nodes {
			b.gossip.JoinEvents <- gossip.NodeState{
				NodeID:   n.ServerID,
				Group:    n.Group,
				Health:   gossip.ALIVE,
				LastSeen: time.Now(),
				Version:  snap.ClusterVersion,
			}
		}
	}
	conn.Write([]byte("SNAPSHOT_OK\n"))
	log.Printf("Snapshot applied successfully")
}
