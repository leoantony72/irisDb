package engine

import (
	"bytes"
	"encoding/gob"
	"iris/config"

	"github.com/cockroachdb/pebble"
)

func (e *Engine) SaveServerMetadata(s *config.Server) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return err
	}

	return e.Db.Set([]byte("config:server:metadata"), buf.Bytes(), pebble.Sync)
}
