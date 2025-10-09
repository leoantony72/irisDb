package engine

import (
	"fmt"
	"log"
	"strings"
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
