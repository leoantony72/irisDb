package bus

import (
	"iris/config"
	"net"
	"strings"
)

func HandleClusterCommand(cmd string, conn net.Conn, s *config.Server) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		conn.Write([]byte("ERR empty command\n"))
		return
	}
	
}
