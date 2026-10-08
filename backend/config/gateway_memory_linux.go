//go:build linux

package config

import "os"

func detectPlatformHostMemoryLimit() (int64, error) {
	return readHostMemoryLimit(os.ReadFile)
}
