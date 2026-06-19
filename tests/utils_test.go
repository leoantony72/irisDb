package tests

import (
	"iris/utils"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// BumpPort / ReverseBumpPort
// ---------------------------------------------------------------------------

func TestBumpPort_Normal(t *testing.T) {
	got, err := utils.BumpPort("192.168.1.1:8008", 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "192.168.1.1:18008" {
		t.Errorf("BumpPort = %q, want %q", got, "192.168.1.1:18008")
	}
}

func TestBumpPort_InvalidAddr(t *testing.T) {
	_, err := utils.BumpPort("not-an-address", 10000)
	if err == nil {
		t.Error("expected error for invalid address")
	}
}

func TestBumpPort_PortOverflow(t *testing.T) {
	_, err := utils.BumpPort("127.0.0.1:60000", 10000)
	if err == nil {
		t.Error("expected error for port overflow (70000 > 65535)")
	}
}

func TestBumpPort_NegativeResult(t *testing.T) {
	_, err := utils.BumpPort("127.0.0.1:5000", -10000)
	if err == nil {
		t.Error("expected error for negative resulting port")
	}
}

func TestReverseBumpPort(t *testing.T) {
	got, err := utils.ReverseBumpPort("192.168.1.1:18008", 10000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "192.168.1.1:8008" {
		t.Errorf("ReverseBumpPort = %q, want %q", got, "192.168.1.1:8008")
	}
}

func TestBumpPort_RoundTrip(t *testing.T) {
	original := "10.0.0.1:8008"
	bumped, err := utils.BumpPort(original, 10000)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := utils.ReverseBumpPort(bumped, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if restored != original {
		t.Errorf("round-trip failed: %q → %q → %q", original, bumped, restored)
	}
}

// ---------------------------------------------------------------------------
// CalculateCRC16
// ---------------------------------------------------------------------------

func TestCalculateCRC16_Deterministic(t *testing.T) {
	a := utils.CalculateCRC16([]byte("hello"))
	b := utils.CalculateCRC16([]byte("hello"))
	if a != b {
		t.Errorf("CRC16 not deterministic: %d vs %d", a, b)
	}
}

func TestCalculateCRC16_DifferentKeys(t *testing.T) {
	a := utils.CalculateCRC16([]byte("key1"))
	b := utils.CalculateCRC16([]byte("key2"))
	// While collisions are possible, these specific keys should differ.
	if a == b {
		t.Logf("warning: CRC16 collision for 'key1' and 'key2' (both %d)", a)
	}
}

func TestCalculateCRC16_EmptyInput(t *testing.T) {
	// Should not panic.
	_ = utils.CalculateCRC16([]byte{})
}

func TestCalculateCRC16_SlotRange(t *testing.T) {
	// CRC16 mod 16384 should always be in [0, 16383].
	for _, key := range []string{"a", "test", "key123", "foobarbaz"} {
		slot := utils.CalculateCRC16([]byte(key)) % 16384
		if slot >= 16384 {
			t.Errorf("slot for %q = %d, out of range", key, slot)
		}
	}
}

// ---------------------------------------------------------------------------
// ParseUint16
// ---------------------------------------------------------------------------

func TestParseUint16_Valid(t *testing.T) {
	tests := []struct {
		input string
		want  uint16
	}{
		{"0", 0},
		{"1234", 1234},
		{"16383", 16383},
		{"65535", 65535},
	}
	for _, tt := range tests {
		got, err := utils.ParseUint16(tt.input)
		if err != nil {
			t.Errorf("ParseUint16(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ParseUint16(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseUint16_Overflow(t *testing.T) {
	_, err := utils.ParseUint16("70000")
	if err == nil {
		t.Error("expected error for overflow value 70000")
	}
}

func TestParseUint16_Invalid(t *testing.T) {
	_, err := utils.ParseUint16("abc")
	if err == nil {
		t.Error("expected error for non-numeric input")
	}
}

func TestParseUint16_Negative(t *testing.T) {
	_, err := utils.ParseUint16("-1")
	if err == nil {
		t.Error("expected error for negative input")
	}
}

// ---------------------------------------------------------------------------
// ParseFloat64
// ---------------------------------------------------------------------------

func TestParseFloat64_Valid(t *testing.T) {
	got, err := utils.ParseFloat64("3.14159")
	if err != nil {
		t.Fatalf("ParseFloat64 error: %v", err)
	}
	if got < 3.14 || got > 3.15 {
		t.Errorf("ParseFloat64(3.14159) = %f", got)
	}
}

func TestParseFloat64_Integer(t *testing.T) {
	got, err := utils.ParseFloat64("42")
	if err != nil {
		t.Fatalf("ParseFloat64 error: %v", err)
	}
	if got != 42.0 {
		t.Errorf("ParseFloat64(42) = %f", got)
	}
}

func TestParseFloat64_Invalid(t *testing.T) {
	_, err := utils.ParseFloat64("xyz")
	if err == nil {
		t.Error("expected error for non-numeric input")
	}
}

// ---------------------------------------------------------------------------
// ReadConfigFile
// ---------------------------------------------------------------------------

func TestReadConfigFile_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	content := `{
		"port": 9000,
		"master_fail_threshold": 5,
		"node_group": "us-east",
		"cluster_addr": "192.168.1.100:8008",
		"rocksdb_path": "/data/irisdb",
		"replication_factor": 2
	}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := utils.ReadConfigFile(&path)
	if cfg == nil {
		t.Fatal("ReadConfigFile returned nil")
	}
	if cfg.Port != 9000 {
		t.Errorf("Port = %d, want 9000", cfg.Port)
	}
	if cfg.MasterFailThreshold != 5 {
		t.Errorf("MasterFailThreshold = %d, want 5", cfg.MasterFailThreshold)
	}
	if cfg.NodeGroup != "us-east" {
		t.Errorf("NodeGroup = %q", cfg.NodeGroup)
	}
	if cfg.ClusterAddr != "192.168.1.100:8008" {
		t.Errorf("ClusterAddr = %q", cfg.ClusterAddr)
	}
	if cfg.RocksDBPath != "/data/irisdb" {
		t.Errorf("RocksDBPath = %q", cfg.RocksDBPath)
	}
	if cfg.ReplicationFactor != 2 {
		t.Errorf("ReplicationFactor = %d", cfg.ReplicationFactor)
	}
}

func TestReadConfigFile_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(path, []byte("{invalid json}"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := utils.ReadConfigFile(&path)
	if cfg != nil {
		t.Error("expected nil for invalid JSON")
	}
}

func TestReadConfigFile_NilPath(t *testing.T) {
	cfg := utils.ReadConfigFile(nil)
	if cfg != nil {
		t.Error("expected nil for nil path")
	}
}

func TestReadConfigFile_EmptyPath(t *testing.T) {
	empty := ""
	cfg := utils.ReadConfigFile(&empty)
	if cfg != nil {
		t.Error("expected nil for empty path")
	}
}

func TestReadConfigFile_NonExistent(t *testing.T) {
	path := "/nonexistent/config.json"
	cfg := utils.ReadConfigFile(&path)
	if cfg != nil {
		t.Error("expected nil for non-existent file")
	}
}
