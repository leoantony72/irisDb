package engine

import (
	"errors"

	"github.com/cockroachdb/pebble"
)

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
