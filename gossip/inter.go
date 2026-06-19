package gossip

import (
	"encoding/base64"
	"fmt"
	"iris/serializer/pb"
	"iris/utils"
	"log"
	"math/rand"
	"net"
	"time"

	"google.golang.org/protobuf/proto"
)

const INTER_GOSSIP_FACTOR = 2

func (g *Gossip) InterGossip() {
	for {
		time.Sleep(10 * time.Second)

		groups := g.view.GetAllGroups()
		// debug
		log.Printf("[INTER] allGroups=%v localGroup=%s", groups, g.view.GetLocalGroup())
		if len(groups) < 2 {
			// nothing to gossip to across groups
			log.Println("InterGossip: not enough groups to gossip")
			continue
		}
		for i := 0; i < INTER_GOSSIP_FACTOR; i++ {
			// pick a group different from local group
			localGroup := g.view.GetLocalGroup()
			var selectedGroup string
			for attempts := 0; attempts < 3; attempts++ {
				randgrp := rand.Uint32() % uint32(len(groups))
				selectedGroup = groups[randgrp]
				if selectedGroup != localGroup {
					break
				}
			}
			if selectedGroup == localGroup {
				// couldn't pick another group
				continue
			}
			members := g.view.GetGroupMembers(selectedGroup)
			// exclude self from inter-group selection
			peers := make([]string, 0, len(members))
			for _, m := range members {
				if m == g.localID {
					continue
				}
				peers = append(peers, m)
			}
			if len(peers) < 1 {
				continue
			}
			randomNum := rand.Uint32() % uint32(len(peers))
			selectedNode := peers[randomNum]

			log.Printf("[INTER] selected group=%s peers=%v selected=%s", selectedGroup, peers, selectedNode)

			addr, exist := g.view.GetNodeAddr(selectedNode)
			if !exist {
				log.Println("Beware the node is not present in the server")
				continue
			}

			// connect to peer's bus port (main port + 10000)
			busAddr, err := utils.BumpPort(addr, 10000)
			if err != nil {
				log.Printf("failed to derive bus addr for %s: %v", addr, err)
				continue
			}

			conn, err := net.DialTimeout("tcp", busAddr, time.Second*3)
			if err != nil {
				continue
			}

			states := g.ToGossipStates()

			data := &pb.GossipMessage{
				MessageType: 0,
				SenderId:    g.localID,
				States:      states,
			}
			payload, err := proto.Marshal(data)
			if err != nil {
				conn.Close()
				continue
			}
			encoded := base64.StdEncoding.EncodeToString(payload)
			msg := "GOSSIP " + encoded + "\n"
			_, err = conn.Write([]byte(msg))
			if err != nil {
				conn.Close()
				continue
			}
			// record the sent gossip summary
			summary := fmt.Sprintf("%s SENT INTER to %s | States:%d", time.Now().Format(time.RFC3339), selectedNode, len(states))
			g.AddSent(summary)
			conn.Close()
		}
	}

}
