package bus

import (
	"fmt"
	"iris/config"
	"iris/engine"
	"net"
)

// MESSAGE FORMAT: INS KEY VALUE
// RESPONSE FORMAT: ACK KEY
func HandleINS(conn net.Conn, parts []string, server *config.Server, db *engine.Engine) {
	if len(parts) < 3 {
		conn.Write([]byte("Err: Incorrect Format: INS KEY VALUE"))
		return
	}
	fmt.Printf("RECEIVED FORWARD REQ: KEY//%s\n", parts[1])
	db.Set(parts[1], parts[2], conn)
}
