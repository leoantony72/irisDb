package bus

import (
	"net"
	"strings"
)

func (b *Bus) HandleClusterCommand(cmd string, conn net.Conn) {
	// log.Printf("HandleClusterCommand received: %s", cmd)
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		conn.Write([]byte("ERR empty command\n"))
		return
	}

	// log.Printf("Routing command: %s", strings.ToUpper(parts[0]))

	switch strings.ToUpper(parts[0]) {
	case "JOIN":
		{
			b.HandleJoin(conn, parts)
			b.db.SaveServerMetadata(b.server)
		}

	case "PREPARE":
		{
			HandlePrepare(conn, parts, b.server)
			b.db.SaveServerMetadata(b.server)
		}
	case "REP":
		{
			b.HandleReplication(conn, parts)
		}

	case "COMMIT":
		{
			b.HandleCommit(conn, parts)
			b.db.SaveServerMetadata(b.server)
		}

	case "SHOW":
		{
			b.HandleShow(conn)
		}

	case "CLUSTER_METADATA_BEGIN":
		{
			b.HandleMetadata(conn)
		}

	case "INS":
		{
			b.HandleINS(conn, parts)
		}

	case "CMU":
		{
			b.HandleClusterMetdataUpdate(conn, parts)
			b.db.SaveServerMetadata(b.server)
		}
	case "LEAVE":
		{
			b.HandleLeave(conn, parts)
			b.db.SaveServerMetadata(b.server)
		}

	case "HEARTBEAT":
		{
			b.HandleHeartbeat(conn, parts)
		}

	case "SUSPECT_LEADER":
		{
			b.HandleSuspectLeader(conn, parts)
		}

	case "REQ_METADATA":
		{
			b.HandleReqMetadata(conn, parts)
		}
	case "REQ_VOTE":
		{
			b.HandleReqVote(conn, parts)
		}
	case "GOSSIP":
		{
			b.HandleGossip(conn, parts)
		}
	default:
		conn.Write([]byte("ERR unknown command\n"))
	}
}
