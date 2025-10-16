package main

import (
	"iris/config"
	"iris/engine"
	"log"

	"github.com/cockroachdb/pebble"
)

func CheckAndLoadMetadata(e *engine.Engine) bool {
	data, err := e.Get("config:server:metadata")
	if err != nil {
		if err == pebble.ErrNotFound {
			log.Printf("[INFO]: No Saved Metadata Found\n")
			// call config.NewServer() here if no saved metadata found!!
			return false
		}
	}

	print(data)

	return true
}

func SaveServerMetadata(s *config.Server, e *engine.Engine) {
	e.HSet("config:server:metadata","server_id",s.ServerID)
	// e.HSet("config:server:metadata","server_id",s.)
}
