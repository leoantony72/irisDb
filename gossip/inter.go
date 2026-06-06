package gossip

import (
	"encoding/base64"
	"iris/serializer/pb"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/gogo/protobuf/proto"
)

const INTER_GOSSIP_FACTOR = 2

func (g *Gossip) InterGossip() {
	for {
		time.Sleep(10 * time.Second)

		groups := g.view.GetAllGroups()
		for i := 0; i < INTER_GOSSIP_FACTOR; i++ {
			randgrp := rand.Uint32() % uint32(len(groups))

			selectedGroup := groups[randgrp]
			members := g.view.GetGroupMembers(selectedGroup)
			randomNum := rand.Uint32() * uint32(len(members))
			selectedNode := members[randomNum]

			addr, exist := g.view.GetNodeAddr(selectedNode)
			if !exist {
				log.Println("Beware the node is not present in the server")
				return
			}

			conn, err := net.DialTimeout("tcp", addr, time.Second*3)
			if err != nil {
				return
			}

			states := g.ToGossipStates()

			data := &pb.GossipMessage{
				MessageType: 1,
				SenderId:    g.localID,
				States:      states,
			}
			payload, err := proto.Marshal(data)
			if err != nil {
				return
			}
			encoded := base64.StdEncoding.EncodeToString(payload)
			msg := "GOSSIP " + encoded + "\n"
			_, err = conn.Write([]byte(msg))
			if err != nil {
				return
			}
		}
	}

}
