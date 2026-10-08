//go:build !linux && !darwin

package config

func detectPlatformHostMemoryLimit() (int64, error) {
	return 0, nil
}
