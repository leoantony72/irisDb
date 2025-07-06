package engine

import (
	"fmt"
	"log"
	"net"
	"strings"

	"iris/config"

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
			err := e.Db.Set([]byte(parts[1]), []byte(parts[2]), pebble.Sync)
			if err != nil {
				errMsg := fmt.Sprintf("ERR write failed: %s\n", err.Error())
				conn.Write([]byte(errMsg))
			} else {
				conn.Write([]byte("OK\n"))
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
