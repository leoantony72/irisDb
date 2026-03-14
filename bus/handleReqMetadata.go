package bus

import (
	"iris/config"
	"net"
)

func HandleReqMetadata(conn net.Conn, parts []string, s *config.Server) {
	// log.Printf("Received REQ_METADATA from %s", conn.RemoteAddr().String())

	err := s.SendClusterSnapshot(conn)
	if err != nil {
		// log.Printf("Failed to send cluster snapshot to %s: %v", conn.RemoteAddr().String(), err)
		conn.Write([]byte("ERR failed to send cluster snapshot\n"))
		return
	}
}
