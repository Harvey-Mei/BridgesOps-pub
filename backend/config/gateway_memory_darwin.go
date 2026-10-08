//go:build darwin

package config

import (
	"fmt"
	"math"

	"golang.org/x/sys/unix"
)

func detectPlatformHostMemoryLimit() (int64, error) {
	value, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, fmt.Errorf("read Darwin host memory: %w", err)
	}
	if value == 0 || value > math.MaxInt64 {
		return 0, fmt.Errorf("Darwin hw.memsize is invalid")
	}
	return int64(value), nil
}
