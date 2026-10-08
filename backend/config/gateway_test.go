package config

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGatewayRequestAdmissionDefaultsAreConservative(t *testing.T) {
	defaults := DefaultGatewayRequestAdmissionConfig()
	if err := ValidateGatewayRequestAdmissionConfig(defaults); err != nil {
		t.Fatalf("validate defaults: %v", err)
	}
	if defaults.MaxInFlightBodyBytes > 256<<20 {
		t.Fatalf("global body budget = %d, want at most 256 MiB", defaults.MaxInFlightBodyBytes)
	}
	if defaults.MaxInFlightBodyBytesPerKey > defaults.MaxInFlightBodyBytes/2 {
		t.Fatalf("per-key body budget = %d, global = %d", defaults.MaxInFlightBodyBytesPerKey, defaults.MaxInFlightBodyBytes)
	}
	if defaults.MaxInFlightRequestsPerKey >= defaults.MaxInFlightRequests || defaults.MaxQueuedRequestsPerKey >= defaults.MaxQueuedRequests {
		t.Fatalf("per-key request limits do not leave capacity for other keys: %+v", defaults)
	}
	if defaults.MaxQueueWaitMilliseconds != DefaultGatewayRequestAdmissionMaxQueueWaitMilliseconds {
		t.Fatalf("default queue wait = %d", defaults.MaxQueueWaitMilliseconds)
	}
	loaded, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	if loaded.Gateway.RequestAdmission != defaults {
		t.Fatalf("loaded defaults = %+v, want %+v", loaded.Gateway.RequestAdmission, defaults)
	}
	if err := ValidateGatewayRequestAdmissionConfigForMemory(defaults, 768<<20); err != nil {
		t.Fatalf("defaults must fit a 768 MiB effective memory limit: %v", err)
	}
}

func TestNormalizeGatewayRequestAdmissionConfigKeepsProgrammaticZeroQueueWaitInvalid(t *testing.T) {
	got := NormalizeGatewayRequestAdmissionConfig(GatewayRequestAdmissionConfig{})
	if got.MaxInFlightRequests != DefaultGatewayRequestAdmissionMaxInFlightRequests ||
		got.MaxInFlightBodyBytes != DefaultGatewayRequestAdmissionMaxInFlightBodyBytes ||
		got.MaxInFlightRequestsPerKey != DefaultGatewayRequestAdmissionMaxInFlightRequestsPerKey ||
		got.MaxInFlightBodyBytesPerKey != DefaultGatewayRequestAdmissionMaxInFlightBodyBytesPerKey {
		t.Fatalf("normalized required limits = %+v", got)
	}
	if got.MaxQueuedRequests != 0 || got.MaxQueuedRequestsPerKey != 0 {
		t.Fatalf("explicit queue disable was replaced: %+v", got)
	}
	if got.MaxQueueWaitMilliseconds != 0 {
		t.Fatalf("normalized queue wait = %d, want 0", got.MaxQueueWaitMilliseconds)
	}
	if err := ValidateGatewayRequestAdmissionConfig(got); err == nil || !strings.Contains(err.Error(), "max queue wait milliseconds") {
		t.Fatalf("validate programmatic zero queue wait error = %v", err)
	}

	disabledQueue := DefaultGatewayRequestAdmissionConfig()
	disabledQueue.MaxQueuedRequests = 0
	disabledQueue.MaxQueuedRequestsPerKey = 0
	if got := NormalizeGatewayRequestAdmissionConfig(disabledQueue); got != disabledQueue {
		t.Fatalf("queue-disabled config was changed: %+v", got)
	}
	if err := ValidateGatewayRequestAdmissionConfig(disabledQueue); err != nil {
		t.Fatalf("validate queue-disabled config: %v", err)
	}
}

func TestValidateGatewayRequestAdmissionConfigRejectsUnsafeRelationships(t *testing.T) {
	valid := DefaultGatewayRequestAdmissionConfig()
	tests := []struct {
		name   string
		mutate func(*GatewayRequestAdmissionConfig)
	}{
		{name: "negative requests", mutate: func(v *GatewayRequestAdmissionConfig) { v.MaxInFlightRequests = -1 }},
		{name: "excess requests", mutate: func(v *GatewayRequestAdmissionConfig) {
			v.MaxInFlightRequests = MaximumGatewayRequestAdmissionRequests + 1
		}},
		{name: "per-key requests exceed global", mutate: func(v *GatewayRequestAdmissionConfig) { v.MaxInFlightRequestsPerKey = v.MaxInFlightRequests + 1 }},
		{name: "body budget below minimum", mutate: func(v *GatewayRequestAdmissionConfig) {
			v.MaxInFlightBodyBytes = MinimumGatewayRequestAdmissionBodyBytes - 1
		}},
		{name: "body budget above maximum", mutate: func(v *GatewayRequestAdmissionConfig) {
			v.MaxInFlightBodyBytes = MaximumGatewayRequestAdmissionBodyBytes + 1
		}},
		{name: "per-key body exceeds global", mutate: func(v *GatewayRequestAdmissionConfig) { v.MaxInFlightBodyBytesPerKey = v.MaxInFlightBodyBytes + 1 }},
		{name: "negative queue", mutate: func(v *GatewayRequestAdmissionConfig) { v.MaxQueuedRequests = -1 }},
		{name: "excess queue", mutate: func(v *GatewayRequestAdmissionConfig) { v.MaxQueuedRequests = MaximumGatewayRequestAdmissionQueue + 1 }},
		{name: "per-key queue exceeds global", mutate: func(v *GatewayRequestAdmissionConfig) { v.MaxQueuedRequestsPerKey = v.MaxQueuedRequests + 1 }},
		{name: "queue wait below minimum", mutate: func(v *GatewayRequestAdmissionConfig) {
			v.MaxQueueWaitMilliseconds = MinimumGatewayRequestAdmissionQueueWaitMilliseconds - 1
		}},
		{name: "queue wait above maximum", mutate: func(v *GatewayRequestAdmissionConfig) {
			v.MaxQueueWaitMilliseconds = MaximumGatewayRequestAdmissionQueueWaitMilliseconds + 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			test.mutate(&value)
			if err := ValidateGatewayRequestAdmissionConfig(value); err == nil {
				t.Fatalf("unsafe config accepted: %+v", value)
			}
		})
	}
}

func TestLoadGatewayRequestAdmissionConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := []byte("gateway:\n  requestAdmission:\n    maxInFlightRequests: 9\n    maxInFlightBodyBytes: 134217728\n    maxInFlightRequestsPerKey: 2\n    maxInFlightBodyBytesPerKey: 33554432\n    maxQueuedRequests: 12\n    maxQueuedRequestsPerKey: 3\n    maxQueueWaitMilliseconds: 750\n")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load admission config: %v", err)
	}
	want := GatewayRequestAdmissionConfig{
		MaxInFlightRequests:        9,
		MaxInFlightBodyBytes:       128 << 20,
		MaxInFlightRequestsPerKey:  2,
		MaxInFlightBodyBytesPerKey: 32 << 20,
		MaxQueuedRequests:          12,
		MaxQueuedRequestsPerKey:    3,
		MaxQueueWaitMilliseconds:   750,
	}
	if loaded.Gateway.RequestAdmission != want {
		t.Fatalf("loaded admission = %+v, want %+v", loaded.Gateway.RequestAdmission, want)
	}

	if err := os.WriteFile(path, []byte("gateway:\n  requestAdmission:\n    maxInFlightRequests: 2\n    maxInFlightRequestsPerKey: 3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path); err == nil || !strings.Contains(err.Error(), "per-key in-flight requests") {
		t.Fatalf("invalid admission load error = %v", err)
	}
}

func TestLoadGatewayRequestAdmissionQueueOmittedAndExplicitZeroAreDistinct(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("gateway:\n  requestAdmission:\n    maxInFlightRequests: 8\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	omitted, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load omitted queue config: %v", err)
	}
	if omitted.Gateway.RequestAdmission.MaxQueuedRequests != DefaultGatewayRequestAdmissionMaxQueuedRequests ||
		omitted.Gateway.RequestAdmission.MaxQueuedRequestsPerKey != DefaultGatewayRequestAdmissionMaxQueuedRequestsPerKey {
		t.Fatalf("omitted queue limits = %+v", omitted.Gateway.RequestAdmission)
	}
	if omitted.Gateway.RequestAdmission.MaxQueueWaitMilliseconds != DefaultGatewayRequestAdmissionMaxQueueWaitMilliseconds {
		t.Fatalf("omitted queue wait = %+v", omitted.Gateway.RequestAdmission)
	}

	if err := os.WriteFile(path, []byte("gateway:\n  requestAdmission:\n    maxInFlightRequests: 8\n    maxQueuedRequests: 0\n    maxQueuedRequestsPerKey: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	disabled, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load queue-disabled config: %v", err)
	}
	if disabled.Gateway.RequestAdmission.MaxQueuedRequests != 0 || disabled.Gateway.RequestAdmission.MaxQueuedRequestsPerKey != 0 {
		t.Fatalf("explicit queue disable was replaced: %+v", disabled.Gateway.RequestAdmission)
	}

	if err := os.WriteFile(path, []byte("gateway:\n  requestAdmission:\n    maxQueueWaitMilliseconds: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path); err == nil || !strings.Contains(err.Error(), "max queue wait milliseconds") {
		t.Fatalf("explicit zero queue wait load error = %v", err)
	}
}

func TestSaveGatewayRequestAdmissionRejectsProgrammaticZeroQueueWait(t *testing.T) {
	admission := DefaultGatewayRequestAdmissionConfig()
	admission.MaxQueueWaitMilliseconds = 0
	path := filepath.Join(t.TempDir(), "config.yaml")
	const original = "existing configuration must not be replaced"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Save(path, &Config{Gateway: GatewayConfig{RequestAdmission: admission}}); err == nil || !strings.Contains(err.Error(), "max queue wait milliseconds") {
		t.Fatalf("save programmatic zero queue wait error = %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != original {
		t.Fatalf("invalid save replaced existing config: %q", body)
	}
}

func TestLoadGatewayRequestAdmissionRejectsZeroQueueWaitEnvironmentOverride(t *testing.T) {
	t.Setenv("GATEWAY_REQUESTADMISSION_MAXQUEUEWAITMILLISECONDS", "0")
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil || !strings.Contains(err.Error(), "max queue wait milliseconds") {
		t.Fatalf("zero queue wait environment override error = %v", err)
	}
}

func TestLoadGatewayRequestAdmissionQueueWaitYAMLBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	tests := []struct {
		name    string
		value   string
		want    int64
		wantErr bool
	}{
		{name: "minimum", value: "1", want: 1},
		{name: "maximum", value: "60000", want: 60000},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative", value: "-1", wantErr: true},
		{name: "above maximum", value: "60001", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := []byte("gateway:\n  requestAdmission:\n    maxQueueWaitMilliseconds: " + test.value + "\n")
			if err := os.WriteFile(path, body, 0o600); err != nil {
				t.Fatal(err)
			}
			loaded, err := LoadFile(path)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "max queue wait milliseconds") {
					t.Fatalf("load %s error = %v", test.value, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("load %s: %v", test.value, err)
			}
			if got := loaded.Gateway.RequestAdmission.MaxQueueWaitMilliseconds; got != test.want {
				t.Fatalf("loaded queue wait = %d, want %d", got, test.want)
			}
		})
	}
}

func TestValidateGatewayRequestAdmissionConfigForMemoryRejectsUnsafeBudgets(t *testing.T) {
	const effectiveLimit = int64(768 << 20)
	valid := DefaultGatewayRequestAdmissionConfig()
	if err := ValidateGatewayRequestAdmissionConfigForMemory(valid, effectiveLimit); err != nil {
		t.Fatalf("validate defaults: %v", err)
	}

	bodyAtBoundary := valid
	bodyAtBoundary.MaxInFlightBodyBytes = effectiveLimit / GatewayRequestAdmissionBodyMemoryDivisor
	if err := ValidateGatewayRequestAdmissionConfigForMemory(bodyAtBoundary, effectiveLimit); err != nil {
		t.Fatalf("validate body budget boundary: %v", err)
	}
	bodyOverBoundary := bodyAtBoundary
	bodyOverBoundary.MaxInFlightBodyBytes++
	if err := ValidateGatewayRequestAdmissionConfigForMemory(bodyOverBoundary, effectiveLimit); err == nil || !strings.Contains(err.Error(), "safe budget") {
		t.Fatalf("body budget overflow error = %v", err)
	}

	fourGiB := valid
	fourGiB.MaxInFlightBodyBytes = 4 << 30
	if err := ValidateGatewayRequestAdmissionConfigForMemory(fourGiB, 4<<30); err == nil || !strings.Contains(err.Error(), "safe budget") {
		t.Fatalf("4 GiB body budget error = %v", err)
	}

	extremeCounts := valid
	extremeCounts.MaxInFlightRequests = MaximumGatewayRequestAdmissionRequests
	extremeCounts.MaxQueuedRequests = MaximumGatewayRequestAdmissionQueue
	if err := ValidateGatewayRequestAdmissionConfigForMemory(extremeCounts, 4<<30); err == nil || !strings.Contains(err.Error(), "control budget") {
		t.Fatalf("extreme request and queue limits error = %v", err)
	}
}

func TestGatewayRequestAdmissionMemorySourceParsers(t *testing.T) {
	if got, err := parseCgroupMemoryLimit([]byte("max\n")); err != nil || got != 0 {
		t.Fatalf("parse unlimited memory.max = %d, %v", got, err)
	}
	if got, err := parseCgroupMemoryLimit([]byte("805306368\n")); err != nil || got != 768<<20 {
		t.Fatalf("parse finite memory.max = %d, %v", got, err)
	}
	if _, err := parseCgroupMemoryLimit([]byte("0\n")); err == nil {
		t.Fatal("zero memory.max was accepted")
	}

	meminfo := []byte("MemFree: 100 kB\nMemTotal:        4009512 kB\n")
	if got, err := parseProcMeminfoTotal(meminfo); err != nil || got != 4009512*1024 {
		t.Fatalf("parse MemTotal = %d, %v", got, err)
	}
	if _, err := parseProcMeminfoTotal([]byte("MemFree: 100 kB\n")); err == nil {
		t.Fatal("missing MemTotal was accepted")
	}

	if got := smallestFiniteMemoryLimit(math.MaxInt64, 1280<<20, 768<<20, 4<<30); got != 768<<20 {
		t.Fatalf("effective memory limit = %d, want 768 MiB", got)
	}
}

func TestReadCurrentCgroupMemoryLimitUsesSmallestAncestorBoundary(t *testing.T) {
	files := map[string][]byte{
		procSelfCgroupPath: []byte("0::/system.slice/bridgeops.service\n"),
		filepath.Join(cgroupRootPath, "system.slice", "bridgeops.service", "memory.max"): []byte("max\n"),
		filepath.Join(cgroupRootPath, "system.slice", "memory.max"):                      []byte("1342177280\n"),
		filepath.Join(cgroupRootPath, "memory.max"):                                      []byte("4294967296\n"),
	}
	readFile := func(path string) ([]byte, error) {
		body, ok := files[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		return body, nil
	}
	got, err := readCurrentCgroupMemoryLimit(readFile)
	if err != nil {
		t.Fatalf("read current cgroup memory: %v", err)
	}
	if got != 1280<<20 {
		t.Fatalf("effective cgroup limit = %d, want 1280 MiB", got)
	}
}
