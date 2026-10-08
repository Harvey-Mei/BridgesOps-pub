package runtimestatus

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	EnvironmentPath = "BRIDGEOPS_RUNTIME_STATUS_FILE"
	RootPath        = "/run/bridgeops/runtime-status.json"
)

type Status struct {
	SchemaVersion  int    `json:"schema_version"`
	PID            int    `json:"pid"`
	ProcessStartID string `json:"process_start_id"`
	DatabaseDriver string `json:"database_driver"`
	BridgeOps      string `json:"bridgeops_version"`
	StartedAt      string `json:"started_at"`
}

func Path() string {
	if configured := os.Getenv(EnvironmentPath); configured != "" {
		return configured
	}
	if os.Geteuid() == 0 {
		return RootPath
	}
	return filepath.Join(os.TempDir(), fmt.Sprintf("bridgeops-runtime-%d", os.Geteuid()), "runtime-status.json")
}

func processStartIdentity(pid int) (string, error) {
	if pid < 1 {
		return "", errors.New("process PID must be positive")
	}
	statLine, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", fmt.Errorf("read process start marker: %w", err)
	}
	closingParen := strings.LastIndexByte(string(statLine), ')')
	if closingParen < 0 {
		return "", errors.New("process stat is malformed")
	}
	fields := strings.Fields(string(statLine[closingParen+1:]))
	if len(fields) <= 19 {
		return "", errors.New("process stat does not contain a start marker")
	}
	startTicks := fields[19]
	if _, err := strconv.ParseUint(startTicks, 10, 64); err != nil {
		return "", errors.New("process start marker is invalid")
	}
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fmt.Errorf("read boot identity: %w", err)
	}
	boot := strings.TrimSpace(string(bootID))
	if boot == "" {
		return "", errors.New("boot identity is empty")
	}
	sum := sha256.Sum256([]byte(boot + "\x00" + startTicks))
	return fmt.Sprintf("%x", sum[:]), nil
}

func Write(path, version string, startedAt time.Time) (Status, error) {
	if !filepath.IsAbs(path) {
		return Status{}, errors.New("runtime status path must be absolute")
	}
	if version == "" {
		return Status{}, errors.New("BridgeOps version is required")
	}
	processStartID, err := processStartIdentity(os.Getpid())
	if err != nil {
		return Status{}, fmt.Errorf("identify BridgeOps process start: %w", err)
	}
	status := Status{
		SchemaVersion:  3,
		PID:            os.Getpid(),
		ProcessStartID: processStartID,
		DatabaseDriver: "postgres",
		BridgeOps:      version,
		StartedAt:      startedAt.UTC().Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(status)
	if err != nil {
		return Status{}, fmt.Errorf("encode runtime status: %w", err)
	}
	data = append(data, '\n')

	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return Status{}, fmt.Errorf("create runtime status directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return Status{}, fmt.Errorf("secure runtime status directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".runtime-status-*.tmp")
	if err != nil {
		return Status{}, fmt.Errorf("create runtime status: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return Status{}, fmt.Errorf("secure runtime status: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return Status{}, fmt.Errorf("write runtime status: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return Status{}, fmt.Errorf("sync runtime status: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return Status{}, fmt.Errorf("close runtime status: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return Status{}, fmt.Errorf("install runtime status: %w", err)
	}
	return status, nil
}
