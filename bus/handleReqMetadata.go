package bus

import (
	"net"
)

func (b *Bus) HandleReqMetadata(conn net.Conn, parts []string) {
	// log.Printf("Received REQ_METADATA from %s", conn.RemoteAddr().String())

	err := b.server.SendClusterSnapshot(conn)
	if err != nil {
		// log.Printf("Failed to send cluster snapshot to %s: %v", conn.RemoteAddr().String(), err)
		conn.Write([]byte("ERR failed to send cluster snapshot\n"))
		return
	}
}
