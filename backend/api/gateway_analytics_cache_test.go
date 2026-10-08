package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/analyticscache"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestListGatewayRequestLogsUsesSummaryCacheAndLiveConcurrency(t *testing.T) {
	_, store, userTime := openGatewayAnalyticsCacheTestStore(t)
	filter := storage.GatewayRequestLogFilter{TimeZone: userTime.Name()}
	cache := &memoryAnalyticsCache{values: map[string][]byte{}}
	payload, err := marshalGatewayRequestAnalyticsCache("summary", &storage.GatewayRequestLogSummary{TotalRequests: 9, CurrentConcurrency: 999})
	if err != nil {
		t.Fatal(err)
	}
	cache.values[gatewayRequestAnalyticsCacheKey("summary", filter, 0)] = payload

	gatewayInFlightRequests.Store(4)
	t.Cleanup(func() { gatewayInFlightRequests.Store(0) })
	recorder := httptest.NewRecorder()
	ctx := ginTestContext(recorder, http.MethodGet, "/api/gateway/request-logs?page=1&page_size=20")
	listGatewayRequestLogs(ctx, &Deps{
		Gateway:        gateway.NewService(nil, nil, store, nil, nil, nil),
		UserTime:       userTime,
		AnalyticsCache: cache,
	})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data struct {
			Total   int64                            `json:"total"`
			Summary storage.GatewayRequestLogSummary `json:"summary"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.Total != 9 || response.Data.Summary.CurrentConcurrency != 4 || response.Data.Summary.CurrentConcurrencyScope != "instance" {
		t.Fatalf("cached summary/live concurrency = %+v", response.Data)
	}
	if cache.getCalls != 1 || cache.setCalls != 0 {
		t.Fatalf("cache calls get=%d set=%d", cache.getCalls, cache.setCalls)
	}
}

func TestListGatewayRequestLogsBypassesCacheWhileAggregateBackfillIsPending(t *testing.T) {
	db, store, userTime := openGatewayAnalyticsCacheTestStore(t)
	if err := db.Create(&storage.GatewayRequestLog{
		DownstreamKeyID: 1,
		ModelName:       "gpt-5",
		Method:          http.MethodPost,
		Path:            "/v1/responses",
		StatusCode:      http.StatusOK,
		CreatedAt:       time.Now().UTC(),
	}).Error; err != nil {
		t.Fatalf("create unaggregated log: %v", err)
	}
	filter := storage.GatewayRequestLogFilter{TimeZone: userTime.Name()}
	cache := &memoryAnalyticsCache{values: map[string][]byte{}}
	payload, err := marshalGatewayRequestAnalyticsCache("summary", &storage.GatewayRequestLogSummary{TotalRequests: 99})
	if err != nil {
		t.Fatal(err)
	}
	cache.values[gatewayRequestAnalyticsCacheKey("summary", filter, 0)] = payload

	recorder := httptest.NewRecorder()
	ctx := ginTestContext(recorder, http.MethodGet, "/api/gateway/request-logs?page=1&page_size=20")
	listGatewayRequestLogs(ctx, &Deps{
		Gateway:        gateway.NewService(nil, nil, store, nil, nil, nil),
		UserTime:       userTime,
		AnalyticsCache: cache,
	})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data struct {
			Total int64 `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.Total != 1 || cache.getCalls != 0 || cache.setCalls != 0 {
		t.Fatalf("backfill response/cache = total:%d get:%d set:%d", response.Data.Total, cache.getCalls, cache.setCalls)
	}
}

func TestGatewayRequestLogAnalyticsUsesOnlyCanonicalCachedResult(t *testing.T) {
	_, store, userTime := openGatewayAnalyticsCacheTestStore(t)
	filter := storage.GatewayRequestLogFilter{TimeZone: userTime.Name()}
	cache := &memoryAnalyticsCache{values: map[string][]byte{}}
	payload, err := marshalGatewayRequestAnalyticsCache("analytics", &storage.GatewayRequestLogAnalytics{
		Trend: []storage.GatewayRequestLogTrendPoint{{Bucket: "2026-07-19T00:00:00Z", Requests: 7}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cache.values[gatewayRequestAnalyticsCacheKey("analytics", filter, 8)] = payload

	recorder := httptest.NewRecorder()
	ctx := ginTestContext(recorder, http.MethodGet, "/api/gateway/request-log-analytics")
	gatewayRequestLogAnalytics(ctx, &Deps{
		Gateway:        gateway.NewService(nil, nil, store, nil, nil, nil),
		UserTime:       userTime,
		AnalyticsCache: cache,
	})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data storage.GatewayRequestLogAnalytics `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Data.Trend) != 1 || response.Data.Trend[0].Requests != 7 || cache.getCalls != 1 || cache.setCalls != 0 {
		t.Fatalf("analytics/cache = %+v / get:%d set:%d", response.Data, cache.getCalls, cache.setCalls)
	}
}

func TestGatewayRequestAnalyticsCacheKeyIncludesSystemTimeZoneAndUTCBounds(t *testing.T) {
	start := time.Date(2026, 7, 19, 12, 0, 0, 0, time.FixedZone("client", -4*60*60))
	base := storage.GatewayRequestLogFilter{Start: &start, TimeZone: "Asia/Shanghai", DownstreamKeyID: 3, ModelName: "gpt-5", ChannelID: 4, Result: "success", RouteReason: "fallback"}
	key := gatewayRequestAnalyticsCacheKey("summary", base, 0)
	utcStart := start.UTC()
	sameInstant := base
	sameInstant.Start = &utcStart
	if key != gatewayRequestAnalyticsCacheKey("summary", sameInstant, 0) {
		t.Fatal("same UTC instant produced different cache keys")
	}
	differentZone := base
	differentZone.TimeZone = "America/New_York"
	if key == gatewayRequestAnalyticsCacheKey("summary", differentZone, 0) {
		t.Fatal("system timezone was omitted from cache key")
	}
}

func TestGatewayRequestAnalyticsCacheKeyPreservesProviderRequestIDBytes(t *testing.T) {
	base := storage.GatewayRequestLogFilter{TimeZone: "UTC", UpstreamRequestID: "provider-id"}
	baseKey := gatewayRequestAnalyticsCacheKey("summary", base, 0)
	for _, value := range []string{" provider-id", "provider-id\u00a0", " "} {
		filter := base
		filter.UpstreamRequestID = value
		if baseKey == gatewayRequestAnalyticsCacheKey("summary", filter, 0) {
			t.Fatalf("provider request ID %q collapsed in analytics cache key", value)
		}
	}
}

func TestGatewayRequestAnalyticsCacheRejectsUnversionedPayload(t *testing.T) {
	_, store, userTime := openGatewayAnalyticsCacheTestStore(t)
	filter := storage.GatewayRequestLogFilter{TimeZone: userTime.Name()}
	cache := &memoryAnalyticsCache{values: map[string][]byte{
		gatewayRequestAnalyticsCacheKey("summary", filter, 0): []byte(`{}`),
	}}

	summary, err := cachedGatewayRequestLogSummary(context.Background(), &Deps{
		Gateway:        gateway.NewService(nil, nil, store, nil, nil, nil),
		UserTime:       userTime,
		AnalyticsCache: cache,
	}, filter, false)
	if err != nil {
		t.Fatalf("cached summary: %v", err)
	}
	if summary.TotalRequests != 0 || cache.setCalls != 1 {
		t.Fatalf("summary/cache writes = %+v / %d", summary, cache.setCalls)
	}
	var envelope gatewayRequestAnalyticsCacheEnvelope
	if err := json.Unmarshal(cache.values[gatewayRequestAnalyticsCacheKey("summary", filter, 0)], &envelope); err != nil || envelope.SchemaVersion != gatewayRequestAnalyticsCacheSchemaVersion {
		t.Fatalf("replacement envelope = %+v, err=%v", envelope, err)
	}
}

func TestGatewayRequestAnalyticsCacheBypassRefreshesCachedValue(t *testing.T) {
	_, store, userTime := openGatewayAnalyticsCacheTestStore(t)
	filter := storage.GatewayRequestLogFilter{TimeZone: userTime.Name()}
	stale, err := marshalGatewayRequestAnalyticsCache("summary", &storage.GatewayRequestLogSummary{TotalRequests: 99})
	if err != nil {
		t.Fatal(err)
	}
	cache := &memoryAnalyticsCache{values: map[string][]byte{
		gatewayRequestAnalyticsCacheKey("summary", filter, 0): stale,
	}}

	summary, err := cachedGatewayRequestLogSummary(context.Background(), &Deps{
		Gateway:        gateway.NewService(nil, nil, store, nil, nil, nil),
		UserTime:       userTime,
		AnalyticsCache: cache,
	}, filter, true)
	if err != nil {
		t.Fatalf("refresh summary: %v", err)
	}
	if summary.TotalRequests != 0 || cache.getCalls != 0 || cache.setCalls != 1 || cache.setTTL != 30*time.Second {
		t.Fatalf("refreshed summary/cache = %+v / get:%d set:%d ttl:%s", summary, cache.getCalls, cache.setCalls, cache.setTTL)
	}
}

func openGatewayAnalyticsCacheTestStore(t *testing.T) (*gorm.DB, *storage.Gateway, *usertime.Service) {
	t.Helper()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	userTime, err := usertime.New("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	return db, storage.NewGateway(db, userTime), userTime
}

func ginTestContext(recorder *httptest.ResponseRecorder, method string, target string) *gin.Context {
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, target, nil)
	return ctx
}

type memoryAnalyticsCache struct {
	values   map[string][]byte
	getCalls int
	setCalls int
	setTTL   time.Duration
	getErr   error
}

func (c *memoryAnalyticsCache) GetJSON(_ context.Context, key string) ([]byte, error) {
	c.getCalls++
	if c.getErr != nil {
		return nil, c.getErr
	}
	payload, ok := c.values[key]
	if !ok {
		return nil, analyticscache.ErrCacheMiss
	}
	return append([]byte(nil), payload...), nil
}

func (c *memoryAnalyticsCache) SetJSON(_ context.Context, key string, payload []byte, ttl time.Duration) error {
	c.setCalls++
	c.setTTL = ttl
	if c.values == nil {
		c.values = make(map[string][]byte)
	}
	c.values[key] = append([]byte(nil), payload...)
	return nil
}

func (c *memoryAnalyticsCache) Close() error { return nil }

var _ analyticscache.JSONCache = (*memoryAnalyticsCache)(nil)
