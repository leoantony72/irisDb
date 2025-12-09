package engine

import (
	"errors"
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

		errMsg := strings.ToLower(err.Error())

		// ✅ WINDOWS + LINUX + MAC FILE LOCK DETECTION
		if strings.Contains(errMsg, "lock") ||
			strings.Contains(errMsg, "resource temporarily unavailable") ||
			strings.Contains(errMsg, "being used by another process") ||
			strings.Contains(errMsg, "used by another process") ||
			strings.Contains(errMsg, "cannot access the file") {

			log.Printf("⚠️ DB at %s is locked, trying next...\n", dbPath)
			continue
		}

		// ❌ Any other error is real → exit immediately
		log.Printf("❌ Failed to open Pebble DB at %s: %v", dbPath, err)
		return nil, err
	}

	return nil, fmt.Errorf("❌ All fallback Pebble DB paths are locked or failed")
}

func (e *Engine) Close() {
	if e.Db != nil {
		e.Db.Close()
	}
}

// Get retrieves the value for a given key
// returns (string, error) returns an error if the key does not exist.
func (e *Engine) Get(key string) (string, error) {
	if e.Db == nil {
		return "", errors.New("database not initialized")
	}
	val, closer, err := e.Db.Get([]byte(key))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", errors.New("key not found")
		}
		return "", err
	}
	defer closer.Close()
	return string(val), nil
}

func (e *Engine) HSet(hash, field, value string) error {
	key := fmt.Sprintf("%s:%s", hash, field)
	return e.Db.Set([]byte(key), []byte(value), pebble.Sync)
}
