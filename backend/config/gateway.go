package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	DefaultGatewayRequestAdmissionMaxInFlightRequests        int64 = 16
	DefaultGatewayRequestAdmissionMaxInFlightBodyBytes             = int64(256 << 20)
	DefaultGatewayRequestAdmissionMaxInFlightRequestsPerKey  int64 = 4
	DefaultGatewayRequestAdmissionMaxInFlightBodyBytesPerKey       = int64(96 << 20)
	DefaultGatewayRequestAdmissionMaxQueuedRequests          int64 = 32
	DefaultGatewayRequestAdmissionMaxQueuedRequestsPerKey    int64 = 8
	DefaultGatewayRequestAdmissionMaxQueueWaitMilliseconds   int64 = 1000

	MinimumGatewayRequestAdmissionBodyBytes             = int64(1 << 20)
	MaximumGatewayRequestAdmissionBodyBytes             = int64(4 << 30)
	MaximumGatewayRequestAdmissionRequests              = int64(1024)
	MaximumGatewayRequestAdmissionQueue                 = int64(8192)
	MinimumGatewayRequestAdmissionQueueWaitMilliseconds = int64(1)
	MaximumGatewayRequestAdmissionQueueWaitMilliseconds = int64(60000)

	// Request bodies may consume at most one third of the smallest effective
	// Go, cgroup, or host memory limit. Handler and queue slots receive a
	// separate one-sixth budget, leaving at least half for the rest of the
	// process even when both admission budgets are fully reserved.
	GatewayRequestAdmissionBodyMemoryDivisor    = int64(3)
	GatewayRequestAdmissionControlMemoryDivisor = int64(6)
	GatewayRequestAdmissionInFlightSlotBytes    = int64(2 << 20)
	GatewayRequestAdmissionQueuedSlotBytes      = int64(256 << 10)
)

const (
	procSelfCgroupPath = "/proc/self/cgroup"
	procMeminfoPath    = "/proc/meminfo"
	cgroupRootPath     = "/sys/fs/cgroup"
)

// GatewayConfig contains process-local data-plane resource limits. These
// limits are intentionally separate from routing policy and upstream account
// concurrency: they protect this process even when every upstream is healthy.
type GatewayConfig struct {
	RequestAdmission GatewayRequestAdmissionConfig `mapstructure:"requestAdmission" yaml:"requestAdmission" json:"requestAdmission"`
}

// GatewayRequestAdmissionConfig limits request bodies retained for the full
// downstream request lifetime. Body values are weighted peak-memory
// reservations, not raw Content-Length values: the API layer currently
// budgets a standard body at 4x + 64 KiB and a rewritten body at 8x + 256 KiB.
// MaxQueueWaitMilliseconds bounds only the time a request may wait after it
// has entered the finite queue; it is a startup-only data-plane setting.
type GatewayRequestAdmissionConfig struct {
	MaxInFlightRequests        int64 `mapstructure:"maxInFlightRequests" yaml:"maxInFlightRequests" json:"maxInFlightRequests"`
	MaxInFlightBodyBytes       int64 `mapstructure:"maxInFlightBodyBytes" yaml:"maxInFlightBodyBytes" json:"maxInFlightBodyBytes"`
	MaxInFlightRequestsPerKey  int64 `mapstructure:"maxInFlightRequestsPerKey" yaml:"maxInFlightRequestsPerKey" json:"maxInFlightRequestsPerKey"`
	MaxInFlightBodyBytesPerKey int64 `mapstructure:"maxInFlightBodyBytesPerKey" yaml:"maxInFlightBodyBytesPerKey" json:"maxInFlightBodyBytesPerKey"`
	MaxQueuedRequests          int64 `mapstructure:"maxQueuedRequests" yaml:"maxQueuedRequests" json:"maxQueuedRequests"`
	MaxQueuedRequestsPerKey    int64 `mapstructure:"maxQueuedRequestsPerKey" yaml:"maxQueuedRequestsPerKey" json:"maxQueuedRequestsPerKey"`
	MaxQueueWaitMilliseconds   int64 `mapstructure:"maxQueueWaitMilliseconds" yaml:"maxQueueWaitMilliseconds" json:"maxQueueWaitMilliseconds"`
}

// GatewayRequestAdmissionMemoryLimits records the finite memory boundaries
// used for startup validation. A zero source means that boundary is unlimited
// or unavailable; EffectiveLimitBytes is always the smallest finite source.
type GatewayRequestAdmissionMemoryLimits struct {
	GoMemoryLimitBytes  int64
	CgroupLimitBytes    int64
	HostMemoryBytes     int64
	EffectiveLimitBytes int64
}

func DefaultGatewayRequestAdmissionConfig() GatewayRequestAdmissionConfig {
	return GatewayRequestAdmissionConfig{
		MaxInFlightRequests:        DefaultGatewayRequestAdmissionMaxInFlightRequests,
		MaxInFlightBodyBytes:       DefaultGatewayRequestAdmissionMaxInFlightBodyBytes,
		MaxInFlightRequestsPerKey:  DefaultGatewayRequestAdmissionMaxInFlightRequestsPerKey,
		MaxInFlightBodyBytesPerKey: DefaultGatewayRequestAdmissionMaxInFlightBodyBytesPerKey,
		MaxQueuedRequests:          DefaultGatewayRequestAdmissionMaxQueuedRequests,
		MaxQueuedRequestsPerKey:    DefaultGatewayRequestAdmissionMaxQueuedRequestsPerKey,
		MaxQueueWaitMilliseconds:   DefaultGatewayRequestAdmissionMaxQueueWaitMilliseconds,
	}
}

// NormalizeGatewayRequestAdmissionConfig supplies legacy zero-value defaults
// for the required body and concurrency limits. It deliberately does not
// default MaxQueueWaitMilliseconds: unlike an omitted text-config field,
// programmatic values have no presence bit, so rewriting zero here would turn
// an invalid explicit value into a different policy. Programmatic callers
// that want defaults must start with DefaultGatewayRequestAdmissionConfig.
func NormalizeGatewayRequestAdmissionConfig(value GatewayRequestAdmissionConfig) GatewayRequestAdmissionConfig {
	defaults := DefaultGatewayRequestAdmissionConfig()
	if value.MaxInFlightRequests == 0 {
		value.MaxInFlightRequests = defaults.MaxInFlightRequests
	}
	if value.MaxInFlightBodyBytes == 0 {
		value.MaxInFlightBodyBytes = defaults.MaxInFlightBodyBytes
	}
	if value.MaxInFlightRequestsPerKey == 0 {
		value.MaxInFlightRequestsPerKey = defaults.MaxInFlightRequestsPerKey
	}
	if value.MaxInFlightBodyBytesPerKey == 0 {
		value.MaxInFlightBodyBytesPerKey = defaults.MaxInFlightBodyBytesPerKey
	}
	return value
}

func ValidateGatewayRequestAdmissionConfig(value GatewayRequestAdmissionConfig) error {
	if value.MaxInFlightRequests < 1 || value.MaxInFlightRequests > MaximumGatewayRequestAdmissionRequests {
		return fmt.Errorf("gateway request admission max in-flight requests must be between 1 and %d", MaximumGatewayRequestAdmissionRequests)
	}
	if value.MaxInFlightRequestsPerKey < 1 || value.MaxInFlightRequestsPerKey > value.MaxInFlightRequests {
		return fmt.Errorf("gateway request admission per-key in-flight requests must be between 1 and the global limit")
	}
	if value.MaxInFlightBodyBytes < MinimumGatewayRequestAdmissionBodyBytes || value.MaxInFlightBodyBytes > MaximumGatewayRequestAdmissionBodyBytes {
		return fmt.Errorf("gateway request admission max in-flight body bytes must be between %d and %d", MinimumGatewayRequestAdmissionBodyBytes, MaximumGatewayRequestAdmissionBodyBytes)
	}
	if value.MaxInFlightBodyBytesPerKey < MinimumGatewayRequestAdmissionBodyBytes || value.MaxInFlightBodyBytesPerKey > value.MaxInFlightBodyBytes {
		return fmt.Errorf("gateway request admission per-key body bytes must be between %d and the global limit", MinimumGatewayRequestAdmissionBodyBytes)
	}
	if value.MaxQueuedRequests < 0 || value.MaxQueuedRequests > MaximumGatewayRequestAdmissionQueue {
		return fmt.Errorf("gateway request admission max queued requests must be between 0 and %d", MaximumGatewayRequestAdmissionQueue)
	}
	if value.MaxQueuedRequestsPerKey < 0 || value.MaxQueuedRequestsPerKey > value.MaxQueuedRequests {
		return fmt.Errorf("gateway request admission per-key queued requests must be between 0 and the global limit")
	}
	if value.MaxQueueWaitMilliseconds < MinimumGatewayRequestAdmissionQueueWaitMilliseconds || value.MaxQueueWaitMilliseconds > MaximumGatewayRequestAdmissionQueueWaitMilliseconds {
		return fmt.Errorf(
			"gateway request admission max queue wait milliseconds must be between %d and %d",
			MinimumGatewayRequestAdmissionQueueWaitMilliseconds,
			MaximumGatewayRequestAdmissionQueueWaitMilliseconds,
		)
	}
	return nil
}

// DetectGatewayRequestAdmissionMemoryLimits reads the current Go soft memory
// target, the process's cgroup v2 hard limit, and platform host memory. The
// smallest finite value is the effective startup boundary.
func DetectGatewayRequestAdmissionMemoryLimits() (GatewayRequestAdmissionMemoryLimits, error) {
	goLimit := finiteMemoryLimit(debug.SetMemoryLimit(-1))
	cgroupLimit, err := readCurrentCgroupMemoryLimit(os.ReadFile)
	if err != nil {
		return GatewayRequestAdmissionMemoryLimits{}, err
	}
	hostLimit, err := detectPlatformHostMemoryLimit()
	if err != nil {
		return GatewayRequestAdmissionMemoryLimits{}, err
	}

	limits := GatewayRequestAdmissionMemoryLimits{
		GoMemoryLimitBytes: goLimit,
		CgroupLimitBytes:   cgroupLimit,
		HostMemoryBytes:    hostLimit,
	}
	limits.EffectiveLimitBytes = smallestFiniteMemoryLimit(goLimit, cgroupLimit, hostLimit)
	if limits.EffectiveLimitBytes <= 0 {
		return GatewayRequestAdmissionMemoryLimits{}, fmt.Errorf("gateway request admission cannot determine a finite process memory limit")
	}
	return limits, nil
}

// ValidateGatewayRequestAdmissionConfigForMemory applies startup-only memory
// safety checks after structural config validation. It deliberately does not
// mutate the Go memory limit or the process cgroup.
func ValidateGatewayRequestAdmissionConfigForMemory(value GatewayRequestAdmissionConfig, effectiveMemoryLimitBytes int64) error {
	if err := ValidateGatewayRequestAdmissionConfig(value); err != nil {
		return err
	}
	if effectiveMemoryLimitBytes <= 0 {
		return fmt.Errorf("gateway request admission effective memory limit must be positive")
	}

	bodyBudget := effectiveMemoryLimitBytes / GatewayRequestAdmissionBodyMemoryDivisor
	if value.MaxInFlightBodyBytes > bodyBudget {
		return fmt.Errorf(
			"gateway request admission max in-flight body bytes %d exceed the safe budget %d for effective memory limit %d",
			value.MaxInFlightBodyBytes,
			bodyBudget,
			effectiveMemoryLimitBytes,
		)
	}

	controlBudget := effectiveMemoryLimitBytes / GatewayRequestAdmissionControlMemoryDivisor
	controlReservation, ok := gatewayRequestAdmissionControlReservation(value.MaxInFlightRequests, value.MaxQueuedRequests)
	if !ok || controlReservation > controlBudget {
		return fmt.Errorf(
			"gateway request admission request and queue limits reserve more than the safe control budget %d for effective memory limit %d",
			controlBudget,
			effectiveMemoryLimitBytes,
		)
	}
	return nil
}

func gatewayRequestAdmissionControlReservation(inFlightRequests, queuedRequests int64) (int64, bool) {
	if inFlightRequests < 0 || queuedRequests < 0 ||
		inFlightRequests > math.MaxInt64/GatewayRequestAdmissionInFlightSlotBytes ||
		queuedRequests > math.MaxInt64/GatewayRequestAdmissionQueuedSlotBytes {
		return 0, false
	}
	inFlightBytes := inFlightRequests * GatewayRequestAdmissionInFlightSlotBytes
	queuedBytes := queuedRequests * GatewayRequestAdmissionQueuedSlotBytes
	if inFlightBytes > math.MaxInt64-queuedBytes {
		return 0, false
	}
	return inFlightBytes + queuedBytes, true
}

func finiteMemoryLimit(value int64) int64 {
	if value <= 0 || value == math.MaxInt64 {
		return 0
	}
	return value
}

func smallestFiniteMemoryLimit(values ...int64) int64 {
	var smallest int64
	for _, value := range values {
		value = finiteMemoryLimit(value)
		if value > 0 && (smallest == 0 || value < smallest) {
			smallest = value
		}
	}
	return smallest
}

func readCurrentCgroupMemoryLimit(readFile func(string) ([]byte, error)) (int64, error) {
	cgroupPath := ""
	body, err := readFile(procSelfCgroupPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("read current cgroup: %w", err)
	}
	if err == nil {
		for _, line := range strings.Split(string(body), "\n") {
			parts := strings.SplitN(line, ":", 3)
			if len(parts) == 3 && parts[1] == "" {
				cgroupPath = strings.TrimSpace(parts[2])
				break
			}
		}
	}

	candidates := make([]string, 0, 8)
	if cgroupPath != "" {
		cleanPath := strings.TrimPrefix(filepath.Clean("/"+cgroupPath), "/")
		for cleanPath != "" && cleanPath != "." {
			candidates = append(candidates, filepath.Join(cgroupRootPath, cleanPath, "memory.max"))
			parent := filepath.Dir(cleanPath)
			if parent == cleanPath || parent == "." {
				break
			}
			cleanPath = parent
		}
	}
	candidates = append(candidates, filepath.Join(cgroupRootPath, "memory.max"))
	var effective int64
	for _, path := range candidates {
		body, err := readFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("read cgroup memory limit: %w", err)
		}
		limit, err := parseCgroupMemoryLimit(body)
		if err != nil {
			return 0, fmt.Errorf("parse cgroup memory limit: %w", err)
		}
		effective = smallestFiniteMemoryLimit(effective, limit)
	}
	return effective, nil
}

func parseCgroupMemoryLimit(body []byte) (int64, error) {
	value := strings.TrimSpace(string(body))
	if value == "max" {
		return 0, nil
	}
	limit, err := strconv.ParseInt(value, 10, 64)
	if err != nil || limit <= 0 {
		return 0, fmt.Errorf("invalid memory.max value %q", value)
	}
	return limit, nil
}

func readHostMemoryLimit(readFile func(string) ([]byte, error)) (int64, error) {
	body, err := readFile(procMeminfoPath)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read host memory: %w", err)
	}
	limit, err := parseProcMeminfoTotal(body)
	if err != nil {
		return 0, fmt.Errorf("parse host memory: %w", err)
	}
	return limit, nil
}

func parseProcMeminfoTotal(body []byte) (int64, error) {
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "MemTotal:" && strings.EqualFold(fields[2], "kB") {
			kilobytes, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil || kilobytes <= 0 || kilobytes > math.MaxInt64/1024 {
				return 0, fmt.Errorf("invalid MemTotal value %q", fields[1])
			}
			return kilobytes * 1024, nil
		}
	}
	return 0, fmt.Errorf("MemTotal is missing")
}
