package bus

import (
	"fmt"
	"net"
)

// Message Format: REP KEY VALUE
func (b *Bus) HandleReplication(conn net.Conn, parts []string) {
	if len(parts) < 3 {
		conn.Write([]byte("ERR: Incorrect Format, REP KEY VALUE\n"))
		return
	}
	fmt.Printf("RECEIVED REPLICATION REQ: %s,%s, %s\n", parts[0], parts[1], parts[2])
	b.db.Set(parts[1], parts[2], conn)
	conn.Write([]byte("ACK REP\n"))
}
