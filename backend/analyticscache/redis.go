package analyticscache

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisOptions contains only the connection information needed by this
// optional cache. It is deliberately independent from application config.
type RedisOptions struct {
	Addr      string
	Namespace string
	Username  string
	Password  string
	DB        int
	TLSConfig *tls.Config
}

type redisCommands interface {
	GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Close() error
}

// RedisCache is a best-effort JSON cache. NewRedis does not connect or ping;
// connection failures are reported by GetJSON or SetJSON for the caller to
// fall back to its authoritative data source.
type RedisCache struct {
	client redisCommands
	prefix string

	stateMu          sync.Mutex
	unavailableUntil time.Time
}

const redisCircuitCooldown = 5 * time.Second

// NewRedis creates a lazy Redis-backed cache. Construction performs no network
// I/O, so an unavailable Redis server cannot make startup or health checks fail.
func NewRedis(options RedisOptions) (*RedisCache, error) {
	addr := strings.TrimSpace(options.Addr)
	if addr == "" {
		return nil, fmt.Errorf("%w: address is required", ErrInvalidOptions)
	}
	if options.DB < 0 {
		return nil, fmt.Errorf("%w: database index must not be negative", ErrInvalidOptions)
	}
	namespace := strings.TrimSpace(options.Namespace)
	if err := validateNamespace(namespace); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}

	return &RedisCache{
		client: redis.NewClient(newRedisClientOptions(addr, options)),
		prefix: KeyPrefix + namespace + ":",
	}, nil
}

func newRedisClientOptions(addr string, options RedisOptions) *redis.Options {
	return &redis.Options{
		Addr:                  addr,
		Username:              options.Username,
		Password:              options.Password,
		DB:                    options.DB,
		TLSConfig:             options.TLSConfig,
		DialTimeout:           OperationTimeout,
		ReadTimeout:           OperationTimeout,
		WriteTimeout:          OperationTimeout,
		PoolTimeout:           OperationTimeout,
		PoolSize:              MaxPoolSize,
		MaxActiveConns:        MaxPoolSize,
		MaxConcurrentDials:    MaxPoolSize,
		MaxRetries:            -1,
		DialerRetries:         1,
		DialerRetryTimeout:    OperationTimeout,
		ContextTimeoutEnabled: true,
	}
}

// GetJSON returns an identified miss, Redis failure, or corrupted-value error
// so callers can safely fall back to the authoritative database query.
func (c *RedisCache) GetJSON(ctx context.Context, logicalKey string) ([]byte, error) {
	key, err := c.cacheKey(logicalKey)
	if err != nil {
		return nil, err
	}
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("%w: client is not initialized", ErrRedisUnavailable)
	}
	if c.circuitOpen() {
		return nil, ErrRedisUnavailable
	}

	operationCtx, cancel := boundedContext(ctx)
	defer cancel()
	// GETRANGE bounds the reply before it reaches this process. Request one
	// byte beyond the accepted payload limit so oversized cache entries remain
	// identifiable without allocating their complete value in a management read.
	value, err := c.client.GetRange(operationCtx, key, 0, int64(MaxPayloadBytes)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.markSuccess()
			return nil, ErrCacheMiss
		}
		c.markFailure()
		return nil, redisError("get", err)
	}
	c.markSuccess()
	if value == "" {
		return nil, ErrCacheMiss
	}

	payload := []byte(value)
	if len(payload) > MaxPayloadBytes {
		return nil, fmt.Errorf("%w: cached value is %d bytes", ErrPayloadTooLarge, len(payload))
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("%w: cached value is not valid JSON", ErrCorruptPayload)
	}
	return payload, nil
}

// SetJSON always requires a positive TTL. It never creates a persistent Redis
// key, and rejects malformed or oversized values before issuing a command.
func (c *RedisCache) SetJSON(ctx context.Context, logicalKey string, payload []byte, ttl time.Duration) error {
	key, err := c.cacheKey(logicalKey)
	if err != nil {
		return err
	}
	if err := validatePayload(payload); err != nil {
		return err
	}
	if err := validateTTL(ttl); err != nil {
		return err
	}
	if c == nil || c.client == nil {
		return fmt.Errorf("%w: client is not initialized", ErrRedisUnavailable)
	}
	if c.circuitOpen() {
		return ErrRedisUnavailable
	}

	operationCtx, cancel := boundedContext(ctx)
	defer cancel()
	if err := c.client.Set(operationCtx, key, string(payload), ttl).Err(); err != nil {
		c.markFailure()
		return redisError("set", err)
	}
	c.markSuccess()
	return nil
}

func (c *RedisCache) cacheKey(logicalKey string) (string, error) {
	prefix := KeyPrefix
	if c != nil && c.prefix != "" {
		prefix = c.prefix
	}
	return cacheKey(prefix, logicalKey)
}

func (c *RedisCache) circuitOpen() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return !c.unavailableUntil.IsZero() && time.Now().Before(c.unavailableUntil)
}

func (c *RedisCache) markFailure() {
	c.stateMu.Lock()
	c.unavailableUntil = time.Now().Add(redisCircuitCooldown)
	c.stateMu.Unlock()
}

func (c *RedisCache) markSuccess() {
	c.stateMu.Lock()
	c.unavailableUntil = time.Time{}
	c.stateMu.Unlock()
}

// Close releases the optional Redis client's resources. A close failure is
// non-authoritative and remains identifiable as a Redis availability failure.
func (c *RedisCache) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	if err := c.client.Close(); err != nil {
		return redisError("close", err)
	}
	return nil
}

func boundedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, OperationTimeout)
}

func redisError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrRedisUnavailable, operation, err)
}
