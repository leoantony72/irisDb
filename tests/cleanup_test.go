package tests

import (
	"fmt"
	"iris/config"
	"iris/utils"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestCleanUnownedKeys(t *testing.T) {
	e := NewTestEngine(t)
	s := NewTestServer("node-1", "127.0.0.1:8008")

	// Set up ranges: node-1 only owns [0 - 8191]. [8192 - 16383] is owned by node-2.
	s.Metadata = []*config.SlotRange{
		{
			Start:    0,
			End:      8191,
			MasterID: "node-1",
			Nodes:    []string{},
		},
		{
			Start:    8192,
			End:      16383,
			MasterID: "node-2",
			Nodes:    []string{},
		},
	}

	// Find keys hashing to specific ranges
	var keyOwned, keyUnowned string
	for i := 0; i < 10000; i++ {
		keyStr := fmt.Sprintf("key_%d", i)
		slot := utils.CalculateCRC16([]byte(keyStr)) % 16384
		if slot < 8192 && keyOwned == "" {
			keyOwned = keyStr
		} else if slot >= 8192 && keyUnowned == "" {
			keyUnowned = keyStr
		}
		if keyOwned != "" && keyUnowned != "" {
			break
		}
	}

	if keyOwned == "" || keyUnowned == "" {
		t.Fatalf("failed to find keys for both ranges")
	}

	// Insert both keys into Pebble DB
	err := e.Db.Set([]byte(keyOwned), []byte("val1"), pebble.Sync)
	if err != nil {
		t.Fatalf("failed to set owned key: %v", err)
	}
	err = e.Db.Set([]byte(keyUnowned), []byte("val2"), pebble.Sync)
	if err != nil {
		t.Fatalf("failed to set unowned key: %v", err)
	}

	// Run cleanup
	e.CleanUnownedKeys(s)

	// Verify keyOwned still exists
	_, err = e.Get(keyOwned)
	if err != nil {
		t.Errorf("owned key %s should still exist, but got err: %v", keyOwned, err)
	}

	// Verify keyUnowned was deleted
	_, err = e.Get(keyUnowned)
	if err == nil {
		t.Errorf("unowned key %s should have been deleted", keyUnowned)
	}
}
