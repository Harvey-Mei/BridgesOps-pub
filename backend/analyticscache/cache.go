// Package analyticscache provides a small, best-effort cache for management
// analytics snapshots. It only accepts JSON bytes and never owns business data.
package analyticscache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// KeyPrefix namespaces every analytics cache entry. Callers only provide the
	// logical suffix, typically a hash of their canonical filter.
	KeyPrefix = "bridgeops:analytics:v1:"

	// MaxPayloadBytes bounds both writes and accepted reads so a bad cache value
	// cannot turn a management-page request into an avoidable memory spike.
	MaxPayloadBytes = 128 * 1024

	// OperationTimeout bounds each Redis operation, including connection setup.
	OperationTimeout = 50 * time.Millisecond

	// MaxPoolSize is deliberately small because this cache is optional and
	// management-only.
	MaxPoolSize = 8
)

var (
	ErrCacheMiss        = errors.New("analytics cache: miss")
	ErrRedisUnavailable = errors.New("analytics cache: redis unavailable")
	ErrCorruptPayload   = errors.New("analytics cache: corrupt payload")
	ErrPayloadTooLarge  = errors.New("analytics cache: payload too large")
	ErrInvalidJSON      = errors.New("analytics cache: invalid JSON payload")
	ErrInvalidTTL       = errors.New("analytics cache: TTL must be positive")
	ErrInvalidKey       = errors.New("analytics cache: logical key is required")
	ErrInvalidNamespace = errors.New("analytics cache: namespace must be 1-64 ASCII characters")
	ErrInvalidOptions   = errors.New("analytics cache: invalid Redis options")
)

// JSONCache stores opaque JSON bytes. It intentionally has no route, ledger,
// key-material, or other business-object API.
type JSONCache interface {
	GetJSON(ctx context.Context, logicalKey string) ([]byte, error)
	SetJSON(ctx context.Context, logicalKey string, payload []byte, ttl time.Duration) error
	Close() error
}

type noopCache struct{}

// NewNoop returns a cache that always misses and never persists data. It keeps
// input validation so disabled-cache code follows the same contract as Redis.
func NewNoop() JSONCache {
	return noopCache{}
}

func (noopCache) GetJSON(_ context.Context, logicalKey string) ([]byte, error) {
	if _, err := cacheKey(KeyPrefix, logicalKey); err != nil {
		return nil, err
	}
	return nil, ErrCacheMiss
}

func (noopCache) SetJSON(_ context.Context, logicalKey string, payload []byte, ttl time.Duration) error {
	if _, err := cacheKey(KeyPrefix, logicalKey); err != nil {
		return err
	}
	if err := validatePayload(payload); err != nil {
		return err
	}
	return validateTTL(ttl)
}

func (noopCache) Close() error {
	return nil
}

func cacheKey(prefix, logicalKey string) (string, error) {
	if strings.TrimSpace(logicalKey) == "" {
		return "", ErrInvalidKey
	}
	return prefix + logicalKey, nil
}

func validateNamespace(namespace string) error {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" || len(namespace) > 64 {
		return ErrInvalidNamespace
	}
	for _, char := range namespace {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' && char != '_' && char != '.' {
			return ErrInvalidNamespace
		}
	}
	return nil
}

func validatePayload(payload []byte) error {
	if len(payload) > MaxPayloadBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrPayloadTooLarge, len(payload), MaxPayloadBytes)
	}
	if !json.Valid(payload) {
		return ErrInvalidJSON
	}
	return nil
}

func validateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return ErrInvalidTTL
	}
	return nil
}
