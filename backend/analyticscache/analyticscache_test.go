package analyticscache

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNoopCache(t *testing.T) {
	cache := NewNoop()

	if _, err := cache.GetJSON(context.Background(), "summary"); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("GetJSON error = %v, want cache miss", err)
	}
	if err := cache.SetJSON(context.Background(), "summary", []byte(`{"requests":1}`), time.Second); err != nil {
		t.Fatalf("SetJSON error = %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}
}

func TestRedisCacheGetJSON(t *testing.T) {
	t.Run("returns valid cached JSON with a bounded operation context", func(t *testing.T) {
		client := &fakeRedisClient{getValue: `{"requests":1}`}
		cache := &RedisCache{client: client}

		got, err := cache.GetJSON(context.Background(), "summary")
		if err != nil {
			t.Fatalf("GetJSON error = %v", err)
		}
		if string(got) != `{"requests":1}` {
			t.Fatalf("GetJSON = %q", got)
		}
		if client.getKey != KeyPrefix+"summary" {
			t.Fatalf("Redis key = %q", client.getKey)
		}
		if client.getStart != 0 || client.getEnd != int64(MaxPayloadBytes) {
			t.Fatalf("Redis range = %d..%d", client.getStart, client.getEnd)
		}
		assertBoundedDeadline(t, client.getRemaining)
	})

	t.Run("identifies a Redis miss", func(t *testing.T) {
		cache := &RedisCache{client: &fakeRedisClient{getErr: redis.Nil}}
		if _, err := cache.GetJSON(context.Background(), "summary"); !errors.Is(err, ErrCacheMiss) {
			t.Fatalf("GetJSON error = %v, want cache miss", err)
		}
	})

	t.Run("identifies Redis failures for fallback", func(t *testing.T) {
		upstreamErr := errors.New("dial failed")
		cache := &RedisCache{client: &fakeRedisClient{getErr: upstreamErr}}
		if _, err := cache.GetJSON(context.Background(), "summary"); !errors.Is(err, ErrRedisUnavailable) || !errors.Is(err, upstreamErr) {
			t.Fatalf("GetJSON error = %v, want wrapped Redis failure", err)
		}
	})

	t.Run("rejects corrupt or oversized cached values", func(t *testing.T) {
		cases := []struct {
			name  string
			value string
			want  error
		}{
			{name: "invalid JSON", value: "not-json", want: ErrCorruptPayload},
			{name: "oversized JSON", value: string(bytes.Repeat([]byte(" "), MaxPayloadBytes+1)), want: ErrPayloadTooLarge},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				cache := &RedisCache{client: &fakeRedisClient{getValue: tc.value}}
				if _, err := cache.GetJSON(context.Background(), "summary"); !errors.Is(err, tc.want) {
					t.Fatalf("GetJSON error = %v, want %v", err, tc.want)
				}
			})
		}
	})
}

func TestRedisCacheSetJSON(t *testing.T) {
	t.Run("uses the fixed prefix and caller TTL", func(t *testing.T) {
		client := &fakeRedisClient{}
		cache := &RedisCache{client: client}

		if err := cache.SetJSON(context.Background(), "analytics", []byte(`{"total":2}`), 10*time.Second); err != nil {
			t.Fatalf("SetJSON error = %v", err)
		}
		if client.setKey != KeyPrefix+"analytics" {
			t.Fatalf("Redis key = %q", client.setKey)
		}
		if client.setValue != `{"total":2}` {
			t.Fatalf("Redis value = %q", client.setValue)
		}
		if client.setTTL != 10*time.Second {
			t.Fatalf("Redis TTL = %s", client.setTTL)
		}
		assertBoundedDeadline(t, client.setRemaining)
	})

	t.Run("rejects invalid writes before Redis", func(t *testing.T) {
		cases := []struct {
			name    string
			payload []byte
			ttl     time.Duration
			want    error
		}{
			{name: "invalid JSON", payload: []byte("not-json"), ttl: time.Second, want: ErrInvalidJSON},
			{name: "persistent TTL", payload: []byte(`{}`), ttl: 0, want: ErrInvalidTTL},
			{name: "oversized payload", payload: bytes.Repeat([]byte(" "), MaxPayloadBytes+1), ttl: time.Second, want: ErrPayloadTooLarge},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				client := &fakeRedisClient{}
				cache := &RedisCache{client: client}
				if err := cache.SetJSON(context.Background(), "analytics", tc.payload, tc.ttl); !errors.Is(err, tc.want) {
					t.Fatalf("SetJSON error = %v, want %v", err, tc.want)
				}
				if client.setCalls != 0 {
					t.Fatalf("Redis Set called %d times for invalid input", client.setCalls)
				}
			})
		}
	})

	t.Run("identifies Redis write failures for fallback", func(t *testing.T) {
		upstreamErr := errors.New("write failed")
		cache := &RedisCache{client: &fakeRedisClient{setErr: upstreamErr}}
		if err := cache.SetJSON(context.Background(), "analytics", []byte(`{}`), time.Second); !errors.Is(err, ErrRedisUnavailable) || !errors.Is(err, upstreamErr) {
			t.Fatalf("SetJSON error = %v, want wrapped Redis failure", err)
		}
	})
}

func TestNewRedisUsesBoundedOptions(t *testing.T) {
	cache, err := NewRedis(RedisOptions{Addr: " 127.0.0.1:6379 ", Namespace: "prod-1"})
	if err != nil {
		t.Fatalf("NewRedis error = %v", err)
	}
	t.Cleanup(func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close error = %v", err)
		}
	})

	client, ok := cache.client.(*redis.Client)
	if !ok {
		t.Fatal("NewRedis did not create a Redis client")
	}
	options := client.Options()
	if options.Addr != "127.0.0.1:6379" {
		t.Fatalf("Addr = %q", options.Addr)
	}
	if options.DialTimeout != OperationTimeout || options.ReadTimeout != OperationTimeout || options.WriteTimeout != OperationTimeout || options.PoolTimeout != OperationTimeout {
		t.Fatalf("timeout options = dial:%s read:%s write:%s pool:%s", options.DialTimeout, options.ReadTimeout, options.WriteTimeout, options.PoolTimeout)
	}
	if options.PoolSize != MaxPoolSize || options.MaxActiveConns != MaxPoolSize || options.MaxConcurrentDials != MaxPoolSize {
		t.Fatalf("pool options = size:%d active:%d concurrent dials:%d", options.PoolSize, options.MaxActiveConns, options.MaxConcurrentDials)
	}
	if !options.ContextTimeoutEnabled {
		t.Fatal("ContextTimeoutEnabled = false")
	}
	if cache.prefix != KeyPrefix+"prod-1:" {
		t.Fatalf("cache prefix = %q", cache.prefix)
	}
}

func TestNewRedisRejectsInvalidOptions(t *testing.T) {
	if _, err := NewRedis(RedisOptions{Namespace: "test"}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("empty address error = %v", err)
	}
	if _, err := NewRedis(RedisOptions{Addr: "127.0.0.1:6379", Namespace: "test", DB: -1}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative database error = %v", err)
	}
	for _, namespace := range []string{"", "contains:colon", strings.Repeat("a", 65)} {
		if _, err := NewRedis(RedisOptions{Addr: "127.0.0.1:6379", Namespace: namespace}); !errors.Is(err, ErrInvalidOptions) || !errors.Is(err, ErrInvalidNamespace) {
			t.Fatalf("namespace %q error = %v", namespace, err)
		}
	}
}

func TestRedisCacheTemporarilyOpensCircuitAfterFailure(t *testing.T) {
	client := &fakeRedisClient{getErr: errors.New("dial failed")}
	cache := &RedisCache{client: client}
	if _, err := cache.GetJSON(context.Background(), "summary"); !errors.Is(err, ErrRedisUnavailable) {
		t.Fatalf("first GetJSON error = %v", err)
	}
	if _, err := cache.GetJSON(context.Background(), "summary"); !errors.Is(err, ErrRedisUnavailable) {
		t.Fatalf("circuit GetJSON error = %v", err)
	}
	if client.getCalls != 1 {
		t.Fatalf("Redis GET calls while circuit open = %d, want 1", client.getCalls)
	}
	if err := cache.SetJSON(context.Background(), "summary", []byte(`{}`), time.Second); !errors.Is(err, ErrRedisUnavailable) {
		t.Fatalf("circuit SetJSON error = %v", err)
	}
	if client.setCalls != 0 {
		t.Fatalf("Redis SET calls while circuit open = %d", client.setCalls)
	}
}

// fakeRedisClient is enough here because this package only needs command
// arguments and error propagation. A real Redis process would not improve the
// contract tests and would make the unit tests slower and less deterministic.
type fakeRedisClient struct {
	getValue string
	getErr   error
	getCalls int
	getKey   string
	getStart int64
	getEnd   int64

	setErr       error
	setCalls     int
	setKey       string
	setValue     string
	setTTL       time.Duration
	setRemaining time.Duration

	getRemaining time.Duration
	closeErr     error
}

func (c *fakeRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	c.getKey = key
	c.getRemaining = remainingDeadline(ctx)
	return redis.NewStringResult(c.getValue, c.getErr)
}

func (c *fakeRedisClient) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	c.getCalls++
	c.getKey = key
	c.getStart = start
	c.getEnd = end
	c.getRemaining = remainingDeadline(ctx)
	return redis.NewStringResult(c.getValue, c.getErr)
}

func (c *fakeRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	c.setCalls++
	c.setKey = key
	c.setValue, _ = value.(string)
	c.setTTL = expiration
	c.setRemaining = remainingDeadline(ctx)
	return redis.NewStatusResult("OK", c.setErr)
}

func (c *fakeRedisClient) Close() error {
	return c.closeErr
}

func remainingDeadline(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return time.Until(deadline)
}

func assertBoundedDeadline(t *testing.T, remaining time.Duration) {
	t.Helper()
	if remaining <= 0 || remaining > OperationTimeout {
		t.Fatalf("operation deadline remaining = %s, want (0, %s]", remaining, OperationTimeout)
	}
}
