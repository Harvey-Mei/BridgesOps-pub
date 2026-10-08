package runtimestatus

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

func TestWriteCreatesPrivateAtomicStatus(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "runtime", "status.json")
	startedAt := time.Date(2026, 7, 20, 1, 2, 3, 4, time.UTC)
	status, err := Write(path, "0.0.16", startedAt)
	if err != nil {
		t.Fatalf("write status: %v", err)
	}
	if status.SchemaVersion != 3 || status.PID != os.Getpid() || status.DatabaseDriver != "postgres" || status.BridgeOps != "0.0.16" {
		t.Fatalf("unexpected status: %#v", status)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(status.ProcessStartID) {
		t.Fatalf("process start identity = %q", status.ProcessStartID)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat status: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("status mode = %o, want 600", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read status: %v", err)
	}
	var decoded Status
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if decoded != status {
		t.Fatalf("decoded status = %#v, want %#v", decoded, status)
	}
}

func TestWriteRejectsUnsafeInputs(t *testing.T) {
	for _, test := range []struct {
		name    string
		path    string
		version string
	}{
		{name: "relative path", path: "runtime.json", version: "0.0.16"},
		{name: "empty version", path: filepath.Join(t.TempDir(), "runtime.json")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Write(test.path, test.version, time.Now()); err == nil {
				t.Fatal("unsafe runtime status input was accepted")
			}
		})
	}
}
