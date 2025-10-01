package engine

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"time"

	"iris/config"
	"iris/utils"

	"github.com/cockroachdb/pebble"
)

type Engine struct {
	Db *pebble.DB
}

func NewEngine() (*Engine, error) {
	maxRetries := 5
	basePath := "irisdb"

	var db *pebble.DB
	var err error

	for i := 0; i <= maxRetries; i++ {
		dbPath := basePath
		if i > 0 {
			dbPath = fmt.Sprintf("%s_%d", basePath, i)
		}

		db, err = pebble.Open(dbPath, &pebble.Options{})
		if err == nil {
			log.Printf("✅ Using Pebble DB at path: %s\n", dbPath)
			return &Engine{Db: db}, nil
		}

		// Check if it's a locking error
		if strings.Contains(err.Error(), "lock") || strings.Contains(err.Error(), "resource temporarily unavailable") {
			log.Printf("⚠️DB at %s is locked, trying next...\n", dbPath)
			continue
		}

		// Unexpected error, exit early
		log.Printf("❌Failed to open Pebble DB at %s: %v", dbPath, err)
		return nil, err
	}

	return nil, fmt.Errorf("❌All fallback Pebble DB paths are locked or failed")
}

func (e *Engine) Close() {
	if e.Db != nil {
		e.Db.Close()
	}
}

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
	}
}

func FindNodeIDX(s *config.Server, hash uint16) int {
	fmt.Println("len:", len(s.Metadata))
	idx := sort.Search(len(s.Metadata), func(i int) bool {
		return s.Metadata[i].End >= hash
	})
	fmt.Println("hash:", hash, idx)

	return idx
}
