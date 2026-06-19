package tests

import (
	"iris/engine"
	"testing"

	"github.com/cockroachdb/pebble"
)

// ---------------------------------------------------------------------------
// Engine: Set and Get
// ---------------------------------------------------------------------------

func TestEngine_SetAndGet(t *testing.T) {
	e := NewTestEngine(t)

	err := e.Db.Set([]byte("key1"), []byte("value1"), pebble.Sync)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err := e.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("Get(key1) = %q, want %q", val, "value1")
	}
}

// ---------------------------------------------------------------------------
// Engine: Get — not found
// ---------------------------------------------------------------------------

func TestEngine_Get_NotFound(t *testing.T) {
	e := NewTestEngine(t)

	_, err := e.Get("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
	if err.Error() != "key not found" {
		t.Errorf("error = %q, want 'key not found'", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Engine: Delete
// ---------------------------------------------------------------------------

func TestEngine_Delete(t *testing.T) {
	e := NewTestEngine(t)

	_ = e.Db.Set([]byte("del-me"), []byte("val"), pebble.Sync)
	err := e.Db.Delete([]byte("del-me"), pebble.Sync)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = e.Get("del-me")
	if err == nil {
		t.Error("expected error after delete")
	}
}

// ---------------------------------------------------------------------------
// Engine: HSet
// ---------------------------------------------------------------------------

func TestEngine_HSet(t *testing.T) {
	e := NewTestEngine(t)

	err := e.HSet("user", "name", "Alice")
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}

	val, err := e.Get("user:name")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "Alice" {
		t.Errorf("Get(user:name) = %q, want %q", val, "Alice")
	}
}

// ---------------------------------------------------------------------------
// Engine: Close
// ---------------------------------------------------------------------------

func TestEngine_Close(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.NewEngine(dir)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	// Close once — should not panic.
	e.Close()
	// After close, DB should be nil.
}

// ---------------------------------------------------------------------------
// ComputeRangeDigest — empty database
// ---------------------------------------------------------------------------

func TestComputeRangeDigest_Empty(t *testing.T) {
	e := NewTestEngine(t)

	d := e.ComputeRangeDigest(0, 16383, 16384)
	if d.KeyCount != 0 {
		t.Errorf("KeyCount = %d, want 0", d.KeyCount)
	}
	if d.Checksum != 0 {
		t.Errorf("Checksum = %08x, want 0", d.Checksum)
	}
}

// ---------------------------------------------------------------------------
// ComputeRangeDigest — with data
// ---------------------------------------------------------------------------

func TestComputeRangeDigest_WithData(t *testing.T) {
	e := NewTestEngine(t)

	// Insert some data.
	for _, kv := range []struct{ k, v string }{
		{"hello", "world"},
		{"foo", "bar"},
		{"test", "data"},
	} {
		if err := e.Db.Set([]byte(kv.k), []byte(kv.v), pebble.Sync); err != nil {
			t.Fatalf("Set(%q) failed: %v", kv.k, err)
		}
	}

	d := e.ComputeRangeDigest(0, 16383, 16384)
	if d.KeyCount != 3 {
		t.Errorf("KeyCount = %d, want 3", d.KeyCount)
	}
	if d.Checksum == 0 {
		t.Error("Checksum should be non-zero with data")
	}
}

// ---------------------------------------------------------------------------
// ComputeRangeDigest — skips metadata key
// ---------------------------------------------------------------------------

func TestComputeRangeDigest_SkipsMetadataKey(t *testing.T) {
	e := NewTestEngine(t)

	_ = e.Db.Set([]byte("config:server:metadata"), []byte("binary-data"), pebble.Sync)
	_ = e.Db.Set([]byte("user-key"), []byte("user-value"), pebble.Sync)

	d := e.ComputeRangeDigest(0, 16383, 16384)
	if d.KeyCount != 1 {
		t.Errorf("KeyCount = %d, want 1 (metadata key should be excluded)", d.KeyCount)
	}
}

// ---------------------------------------------------------------------------
// ComputeRangeDigest — slot filtering
// ---------------------------------------------------------------------------

func TestComputeRangeDigest_SlotFiltering(t *testing.T) {
	e := NewTestEngine(t)

	// Insert keys and check only those in slot range [0, 100] are counted.
	for i := 0; i < 100; i++ {
		key := []byte("key-" + string(rune('A'+i%26)) + string(rune('0'+i%10)))
		_ = e.Db.Set(key, []byte("val"), pebble.Sync)
	}

	// Small range: [0, 100] out of 16384 should capture a subset.
	d := e.ComputeRangeDigest(0, 100, 16384)
	if d.KeyCount >= 100 {
		t.Errorf("KeyCount = %d, expected fewer keys in narrow slot range", d.KeyCount)
	}
}

// ---------------------------------------------------------------------------
// ComputeRangeDigest — deterministic
// ---------------------------------------------------------------------------

func TestComputeRangeDigest_Deterministic(t *testing.T) {
	e := NewTestEngine(t)

	_ = e.Db.Set([]byte("a"), []byte("1"), pebble.Sync)
	_ = e.Db.Set([]byte("b"), []byte("2"), pebble.Sync)
	_ = e.Db.Set([]byte("c"), []byte("3"), pebble.Sync)

	d1 := e.ComputeRangeDigest(0, 16383, 16384)
	d2 := e.ComputeRangeDigest(0, 16383, 16384)

	if d1.KeyCount != d2.KeyCount || d1.Checksum != d2.Checksum {
		t.Errorf("digest not deterministic: d1=(keys=%d,crc=%08x) d2=(keys=%d,crc=%08x)",
			d1.KeyCount, d1.Checksum, d2.KeyCount, d2.Checksum)
	}
}

// ---------------------------------------------------------------------------
// NewEngine — default path
// ---------------------------------------------------------------------------

func TestNewEngine_DefaultPath(t *testing.T) {
	e := NewTestEngine(t)
	if e.Db == nil {
		t.Error("engine DB is nil")
	}
}

// ---------------------------------------------------------------------------
// Multiple Set/Get operations
// ---------------------------------------------------------------------------

func TestEngine_MultipleSetGet(t *testing.T) {
	e := NewTestEngine(t)

	keys := map[string]string{
		"name":    "IrisDb",
		"version": "1.0",
		"author":  "leoantony72",
	}

	for k, v := range keys {
		if err := e.Db.Set([]byte(k), []byte(v), pebble.Sync); err != nil {
			t.Fatalf("Set(%q) failed: %v", k, err)
		}
	}

	for k, want := range keys {
		got, err := e.Get(k)
		if err != nil {
			t.Errorf("Get(%q) failed: %v", k, err)
			continue
		}
		if got != want {
			t.Errorf("Get(%q) = %q, want %q", k, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Overwrite existing key
// ---------------------------------------------------------------------------

func TestEngine_OverwriteKey(t *testing.T) {
	e := NewTestEngine(t)

	_ = e.Db.Set([]byte("key"), []byte("v1"), pebble.Sync)
	_ = e.Db.Set([]byte("key"), []byte("v2"), pebble.Sync)

	val, err := e.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	if val != "v2" {
		t.Errorf("after overwrite: %q, want %q", val, "v2")
	}
}
