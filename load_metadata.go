package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"iris/config"
	"iris/engine"
	"log"

	"github.com/cockroachdb/pebble"
)

func CheckAndLoadMetadata(e *engine.Engine) (*config.Server, error) {
	data, err := e.Get("config:server:metadata")
	if err != nil {
		if err == pebble.ErrNotFound {
			log.Printf("[INFO]: No Saved Metadata Found\n")
			// call config.NewServer() here if no saved metadata found!!
			return nil, errors.New("no saved Metadata")
		}
	}

	var server config.Server
	if err := gob.NewDecoder(bytes.NewReader([]byte(data))).Decode(&server); err != nil {
		return nil, err
	}
	return &server, nil
}
