package bus

import (
	"iris/config"
	"iris/engine"
	"net"
	"strings"
)

func HandleClusterCommand(cmd string, conn net.Conn, s *config.Server, db *engine.Engine) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		conn.Write([]byte("ERR empty command\n"))
		return
	}

	switch strings.ToUpper(parts[0]) {
	case "JOIN":
		{
			HandleJoin(conn, parts, s, db)
			db.SaveServerMetadata(s)
		}

	case "PREPARE":
		{
			HandlePrepare(conn, parts, s)
			db.SaveServerMetadata(s)
		}
	case "REP":
		{
			HandleReplication(conn, parts, s, db)
		}

	case "COMMIT":
		{
			HandleCommit(conn, parts, s)
			db.SaveServerMetadata(s)
		}

	case "SHOW":
		{
			HandleShow(conn, s)
		}

	case "CLUSTER_METADATA_BEGIN":
		{
			HandleMetadata(conn, s)
		}

	case "INS":
		{
			HandleINS(conn, parts, s, db)
		}

	case "CMU":
		{
			HandleClusterMetdataUpdate(conn, parts, s)
			db.SaveServerMetadata(s)
		}
	case "LEAVE":
		{
			HandleLeave(conn, parts, s,db)
			db.SaveServerMetadata(s)
		}

	default:
		conn.Write([]byte("ERR unknown command\n"))
	}
}
