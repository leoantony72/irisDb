package bus

import (
	"encoding/gob"
	"iris/config"
	"net"
)

func HandleClusterSnapshot(conn net.Conn, s *config.Server) {
	defer conn.Close()

	dec := gob.NewDecoder(conn)

	var snap config.ClusterSnapshot
	if err := dec.Decode(&snap); err != nil {
		conn.Write([]byte("ERR: failed to decode snapshot\n"))
		return
	}

	s.ApplyClusterSnapshot(snap)
	conn.Write([]byte("SNAPSHOT_OK\n"))
}
