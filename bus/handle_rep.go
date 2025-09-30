package bus

import (
	"iris/config"
	"iris/engine"
	"net"
)

func HandleReplication(conn net.Conn, parts []string, s *config.Server, db *engine.Engine) {
	db.Set(parts[1], parts[2], conn)
}
