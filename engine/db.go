package engine

import (
	"fmt"
	"log"
	"net"
	"sort"
	"strings"

	"iris/config"
	"iris/utils"

	"github.com/cockroachdb/pebble"
)

type Engine struct {
	Db *pebble.DB
}

func NewEngine() (*Engine, error) {
	Db, err := pebble.Open("irisdb", &pebble.Options{})
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
		return nil, err
	}

	return &Engine{Db: Db}, nil
}

func (e *Engine) Close() {
	if e.Db != nil {
		e.Db.Close()
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
			master_slot := FindNodeIDX(*server, hash%server.N)

			//check if the server is the master node for this slot(hash)
			if server.Metadata[master_slot].Nodes[0].ServerID == server.ServerID {
				err := e.Db.Set([]byte(parts[1]), []byte(parts[2]), pebble.Sync)
				if err != nil {
					errMsg := fmt.Sprintf("ERR write failed: %s\n", err.Error())
					conn.Write([]byte(errMsg))
				} else {
					conn.Write([]byte("OK\n"))
				}

				// @leoantony72 send the data to the replica nodes through the bus port
				replica_server := server.Metadata[master_slot].Nodes[1:]
				fmt.Println("Replication Nodes:", replica_server)
			} else {
				// @leoantony72 forward the req to the master node
				// (masternode = server.Metadata[master_slot].Nodes[0])
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

func FindNodeIDX(s config.Server, hash uint16) int {

	idx := sort.Search(len(s.Metadata), func(i int) bool {
		return s.Metadata[i].End >= hash
	})

	return idx
}
