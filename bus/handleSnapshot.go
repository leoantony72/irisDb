package bus

import (
	"bufio"
	"encoding/gob"
	"iris/config"
	"log"
	"net"
)

func HandleClusterSnapshot(reader *bufio.Reader, conn net.Conn, s *config.Server) {
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
	s.ApplyClusterSnapshot(snap)
	conn.Write([]byte("SNAPSHOT_OK\n"))
	log.Printf("Snapshot applied successfully")
}
