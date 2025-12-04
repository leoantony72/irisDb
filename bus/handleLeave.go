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
		conn, err := net.DialTimeout("tcp", busAddr, 2*time.Second)
		if err != nil {
			log.Println("ERR: %s", err)
			continue
		}

		// send a small command to indicate snapshot mode
		conn.Write([]byte("SNAPSHOT \n"))

		if err := s.SendClusterSnapshot(conn); err != nil {
			conn.Close()
			continue
		}

		expectedResponse := "SNAPSHOT_OK"

		reader := bufio.NewReader(conn)
		resp, _ := reader.ReadString('\n')
		resp = strings.TrimSpace(resp)
		conn.Close()

		if resp != expectedResponse {
			fmt.Printf("res: %s, %s\n", resp, expectedResponse)
			errMsg := fmt.Sprintf("ERR write failed: %s\n", "Err Response From Master Server")
			conn.Write([]byte(errMsg))
			return
		} else {
			msg := fmt.Sprintf("LEAVE %s\n", serverId)
			conn.Write([]byte(msg))
		}
	}

}
