package engine

import (
	"bufio"
	"fmt"
	"iris/config"
	"iris/utils"
	"log"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

func (e *Engine) Set(key string, val string, conn net.Conn) {
	err := e.Db.Set([]byte(key), []byte(val), pebble.Sync)
	if err != nil {
		errMsg := fmt.Sprintf("ERR write failed: %s\n", err.Error())
		conn.Write([]byte(errMsg))

	} else {
		conn.Write([]byte("ACK REP\n"))
	}
}

func (e *Engine) HandleCommand(cmd string, conn net.Conn, server *config.Server) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		conn.Write([]byte("ERR empty command\n"))
		return
	}

	switch strings.ToUpper(parts[0]) {
	case "SET":
		{
			if len(parts) != 3 {
				conn.Write([]byte("ERR usage: SET KEY value\n"))
				return
			}
			hash := utils.CalculateCRC16([]byte(parts[1]))
			master_slot := FindNodeIDX(server, hash%server.N)
			g := server.Metadata[master_slot].MasterID

			//check if the server is the master node for this slot(hash)
			if server.Metadata[master_slot].MasterID == server.ServerID {
				err := e.Db.Set([]byte(parts[1]), []byte(parts[2]), pebble.Sync)
				if err != nil {
					errMsg := fmt.Sprintf("ERR write failed: %s\n", err.Error())
					conn.Write([]byte(errMsg))
				} else {
					conn.Write([]byte("OK\n"))
				}

				// @leoantony72 send the data to the replica nodes through the bus port
				replica_server := server.Metadata[master_slot].Nodes
				fmt.Println("Replication Nodes:", replica_server)
				success := false
				replication_cmd := fmt.Sprintf("REP %s %s\n", parts[1], parts[2])
				for _, id := range replica_server {
					success = server.SendReplicaCMD(replication_cmd, id)
					if !success {
						fmt.Println("rep failed")
						break
					}
				}
			} else {
				// @leoantony72 forward the req to the master node
				// (masternode = server.Metadata[master_slot].Nodes[0])
				fmt.Println("KEY FORWARD")
				busAddr, _ := utils.BumpPort(server.Nodes[g].Addr, 10000)
				fmt.Printf("SET FORWARD: ADDR: %s\n", busAddr)
				Sconn, err := net.DialTimeout("tcp", busAddr, 2*time.Second)
				if err != nil {
					errMsg := fmt.Sprintf("ERR write failed: %s\n", "Coudn't connect to Master Server")
					conn.Write([]byte(errMsg))
					return
				}

				//MESSAGE FORMAT: INS KEY VALUE
				//RESPONSE FORMAT: ACK KEY
				msg := fmt.Sprintf("INS %s %s\n", parts[1], parts[2])
				_, err = Sconn.Write([]byte(msg))
				if err != nil {
					errMsg := fmt.Sprintf("ERR write failed: %s\n", "Coudn't forward to Master Server")
					conn.Write([]byte(errMsg))
					Sconn.Close()
					return
				}

				expectedResponse := "ACK REP"

				reader := bufio.NewReader(Sconn)
				resp, _ := reader.ReadString('\n')
				resp = strings.TrimSpace(resp)
				Sconn.Close()

				if resp != expectedResponse {
					fmt.Printf("res: %s, %s\n", resp, expectedResponse)
					errMsg := fmt.Sprintf("ERR write failed: %s\n", "Err Response From Master Server")
					conn.Write([]byte(errMsg))
					return
				} else {
					conn.Write([]byte("OK\n"))
				}

			}

		}
	case "GET":
		{
			if len(parts) != 2 {
				conn.Write([]byte("ERR usage: GET KEY \n"))
				return
			}
			data, closer, err := e.Db.Get([]byte(parts[1]))
			if err != nil {
				if err == pebble.ErrNotFound {
					conn.Write([]byte("NOTFOUND\n"))
				} else {
					errMsg := fmt.Sprintf("ERR read failed: %s\n", err.Error())
					conn.Write([]byte(errMsg))
				}
				return
			}
			defer closer.Close()
			conn.Write(append(data, '\n'))
		}
	case "DEL":
		{
			if len(parts) != 2 {
				conn.Write([]byte("ERR usage: DEL KEY \n"))
				return
			}

			err := e.Db.Delete([]byte(parts[1]), pebble.Sync)
			if err != nil {
				errMsg := fmt.Sprintf("ERR delete failed: %s\n", err.Error())
				conn.Write([]byte(errMsg))
			} else {
				conn.Write([]byte("OK\n"))
			}
		}

	case "SHUTDOWN":
		{
			if len(parts) != 1 {
				errMsg := fmt.Sprintf("ERR Incorrect Format: %s\n", "SHUTDOWN")
				conn.Write([]byte(errMsg))
			}
			log.Println("SHUTDOWN requested❎")

			//send req to master server range[0-?]
			//master process the leave req by first promoting the first replica to master
			// metadataIdx := server.FindRangeIndexByServerID(server.ServerID)
			// for _, idx := range metadataIdx {
			// 	metadata, ok := server.GetSlotRangeByIndex(idx)
			// 	if !ok {
			// 		conn.Write([]byte("INTERNAL ERROR\n"))
			// 		return
			// 	}
			// forward the req to master server
			masterRange := server.GetSlotRangesByIndices([]int{0})

			masterNodeID := masterRange[0].MasterID
			masterNode, ok := server.GetConnectedNodeData(masterNodeID)
			if !ok {
				errMsg := fmt.Sprintf("INTERNAL ERROR:%s\n", "master node found")
				conn.Write([]byte(errMsg))
				return
			}

			busAddr, _ := utils.BumpPort(masterNode.Addr, 10000)
			fmt.Printf("MASTER ADDR: %s\n", busAddr)
			Sconn, err := net.DialTimeout("tcp", busAddr, 2*time.Second)
			if err != nil {
				errMsg := fmt.Sprintf("ERR SHUTDOWN failed: %s\n", "Coudn't connect to Master Server")
				conn.Write([]byte(errMsg))
				return
			}
			// format: LEAVE SID
			msg := fmt.Sprintf("LEAVE %s\n", server.ServerID)
			_, err = Sconn.Write([]byte(msg))
			if err != nil {
				errMsg := fmt.Sprintf("ERR write failed: %s\n", "Coudn't forward to Master Server")
				conn.Write([]byte(errMsg))
				return
			}

			// expected Response
			expectedResponse := fmt.Sprintf("LEAVE %s", server.ServerID)
			reader := bufio.NewReader(Sconn)
			resp, _ := reader.ReadString('\n')
			resp = strings.TrimSpace(resp)
			Sconn.Close()

			if resp != expectedResponse {
				fmt.Printf("res: %s, %s\n", resp, expectedResponse)
				errMsg := fmt.Sprintf("ERR SHUTDOWN failed: %s\n", "Err Response From Master Server")
				conn.Write([]byte(errMsg))
				return
			} else {
				conn.Write([]byte("SHUTDOWN successfull\n"))
			}

			//delete all the config data from pebble database

		}
	}

}
