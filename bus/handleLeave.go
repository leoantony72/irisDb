package bus

import (
	"bufio"
	"fmt"
	"iris/config"
	"iris/engine"
	"iris/utils"
	"net"
	"strings"
	"time"
	"log"
)

// LEAVE SID
func HandleLeave(conn net.Conn, parts []string, s *config.Server, db *engine.Engine) {
	if len(parts) != 2 {
		// msg := fmt.Sprintf()
		conn.Write([]byte("ERR INVALID FORMAT, EXPECTED FORMAT: LEAVE SID\n"))
		return
	}
	log.Println("LEAVE REQ RECEIVED🤡🤡")

	serverId := parts[1]

	err := s.NodeExit(serverId)
	if err != nil {
		conn.Write([]byte("SHUTDOWN FAILED\n"))
		return
	}

	peers := s.GetCommitPeers()
	for _, p := range peers {
		busAddr, _ := utils.BumpPort(p.Addr, 10000)
		peerConn, err := net.DialTimeout("tcp", busAddr, 10*time.Second)
		if err != nil {
			log.Println("ERR: %s", err)
			continue
		}

		// send a small command to indicate snapshot mode
		peerConn.Write([]byte("SNAPSHOT \n"))
		log.Printf("Sent SNAPSHOT command to peer %s", p.ServerID)

		if err := s.SendClusterSnapshot(peerConn); err != nil {
			peerConn.Close()
			continue
		}

		expectedResponse := "SNAPSHOT_OK"

		reader := bufio.NewReader(peerConn)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(resp)

		if resp != expectedResponse {
			fmt.Printf("res: %s, %s\n", resp, expectedResponse)
			log.Printf("ERR: Err Response From peer Server for SNAPSHOT_OK")
			peerConn.Close()
			continue
		}
		
		log.Printf("Successfully updated peer %s with new cluster snapshot", p.ServerID)
		peerConn.Close()
	}
	
	// Send success response to the client
	conn.Write([]byte("SHUTDOWN SUCCESS\n"))
}
