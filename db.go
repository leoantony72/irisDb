package main

import (
	"log"
	"net"
	"strings"

	"github.com/linxGnu/grocksdb"
)

func InitDb() {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	var err error
	Db, err := grocksdb.OpenDb(opts, "serpicodb")
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
	}
}

func handleCommand(cmd string, conn net.Conn) {
	parts := strings.Fields(cmd)
	if len(parts)==0{
		conn.Write([]byte("ERR empty command\n"))
		return
	}
}
