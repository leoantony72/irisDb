package bus

import (
	"encoding/base64"
	"iris/config"
	"iris/serializer/pb"
	"net"

	"github.com/gogo/protobuf/proto"
)

func HandleGossip(conn net.Conn, parts []string, s *config.Server) {
	if len(parts) < 2 {
		conn.Write([]byte("ERR invalid gossip\n"))
		return
	}
	payload, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		conn.Write([]byte("ERR invalid base64\n"))
		return
	}

	var Message pb.Gossip
	err = proto.Unmarshal(payload, &Message)
	if err != nil {
		conn.Write([]byte("ERR invalid protobuf\n"))
		return
	}

}
