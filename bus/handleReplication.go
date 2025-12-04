package bus

import (
	"fmt"
	"iris/config"
	"iris/engine"
	"net"
)

// Message Format: REP KEY VALUE
func HandleReplication(conn net.Conn, parts []string, s *config.Server, db *engine.Engine) {
	if len(parts) < 3 {
		conn.Write([]byte("ERR: Incorrect Format, INS KEY VALUE\n"))
		return
	}
	fmt.Printf("RECEIVED REPLICATION REQ: %s,%s, %s\n", parts[0], parts[1], parts[2])
	db.Set(parts[1], parts[2], conn)
}
