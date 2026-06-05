package bus

import (
	"encoding/base64"
	"iris/serializer/pb"
	"net"

	"github.com/gogo/protobuf/proto"
)

func (b *Bus) HandleGossip(conn net.Conn, parts []string) {
	if len(parts) < 2 {
		conn.Write([]byte("ERR invalid gossip\n"))
		return
	}
	payload, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		conn.Write([]byte("ERR invalid base64\n"))
		return
	}

	var Message pb.GossipMessage
	err = proto.Unmarshal(payload, &Message)
	if err != nil {
		conn.Write([]byte("ERR invalid protobuf\n"))
		return
	}

	//to future me: TASK
	//send this message to the gossip layer through the exposed channel
	//rest will be handled by the gossip go routines. (TASK COMPLETE)

	if Message.MessageType == 0 {
		//inter(comm to other groups)
		b.gossip.InterGossipsChan <- &Message
	} else {
		//intra (comm within the group)
		b.gossip.IntraGossipsChan <- &Message
	}

}
