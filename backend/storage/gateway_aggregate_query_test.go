package storage

import (
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestGatewayRequestAggregateGroupedSQLAvoidsRawFallback(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	bucket := time.Date(2026, 7, 19, 4, 0, 0, 0, time.UTC)
	rows := []GatewayRequestAggregate{
		{BucketStart: bucket, DownstreamKeyID: 1, ChannelID: 1, ModelName: "gpt-5", CostSource: "reported", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess, Requests: 1, TotalTokens: 10, TotalCost: 0.1, ReportedCost: 0.1, ActualCost: 0.1, LatencyTotalMs: 100, LatencySamples: 1, FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion},
		{BucketStart: bucket.Add(15 * time.Minute), DownstreamKeyID: 2, ChannelID: 2, ModelName: "gpt-5-mini", CostSource: "estimated", RouteReason: "manual", Result: GatewayRequestResultFailed, Requests: 2, TotalTokens: 20, TotalCost: 0.2, EstimatedCost: 0.2, FirstRequestAt: bucket.Add(15 * time.Minute), LastRequestAt: bucket.Add(15 * time.Minute), Version: gatewayRequestAggregateVersion},
		{BucketStart: bucket.Add(30 * time.Minute), DownstreamKeyID: 3, ChannelID: 3, ModelName: "claude-sonnet", CostSource: "reconciled", RouteReason: "fallback", Result: GatewayRequestResultSuccess, Requests: 3, TotalTokens: 30, TotalCost: 0.3, ReconciledCost: 0.3, ActualCost: 0.3, LatencyTotalMs: 600, LatencySamples: 3, FirstRequestAt: bucket.Add(30 * time.Minute), LastRequestAt: bucket.Add(30 * time.Minute), Version: gatewayRequestAggregateVersion},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("create aggregate rows: %v", err)
	}
	previousLimit := gatewayRequestAggregateReadLimit
	previousBatchSize := gatewayRequestAggregateGroupedBatchSize
	gatewayRequestAggregateReadLimit = 2
	gatewayRequestAggregateGroupedBatchSize = 2
	t.Cleanup(func() {
		gatewayRequestAggregateReadLimit = previousLimit
		gatewayRequestAggregateGroupedBatchSize = previousBatchSize
	})

	summary, err := store.requestLogSummaryFilteredAt(GatewayRequestLogFilter{}, bucket.Add(time.Hour))
	if err != nil {
		t.Fatalf("grouped aggregate summary: %v", err)
	}
	if summary.TotalRequests != 6 || summary.TotalTokens != 60 || summary.Success != 4 || summary.Failed != 2 {
		t.Fatalf("grouped aggregate summary = %+v", summary)
	}
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 8)
	if err != nil {
		t.Fatalf("grouped aggregate analytics: %v", err)
	}
	if len(analytics.Trend) != 1 || analytics.Trend[0].Requests != 6 || analytics.Trend[0].TotalTokens != 60 || len(analytics.Models) != 3 {
		t.Fatalf("grouped aggregate analytics = %+v", analytics)
	}
}

func TestGatewayRequestChannelCostsUseGroupedAggregateSQL(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	bucket := time.Date(2026, 7, 19, 4, 0, 0, 0, time.UTC)
	rows := []GatewayRequestAggregate{
		{BucketStart: bucket, DownstreamKeyID: 1, ChannelID: 1, ModelName: "gpt-5", CostSource: "reported", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess, Requests: 1, TotalCost: 0.1, Version: gatewayRequestAggregateVersion},
		{BucketStart: bucket.Add(15 * time.Minute), DownstreamKeyID: 2, ChannelID: 1, ModelName: "gpt-5-mini", CostSource: "estimated", RouteReason: "manual", Result: GatewayRequestResultSuccess, Requests: 1, TotalCost: 0.2, Version: gatewayRequestAggregateVersion},
		{BucketStart: bucket.Add(30 * time.Minute), DownstreamKeyID: 3, ChannelID: 2, ModelName: "claude-sonnet", CostSource: "reconciled", RouteReason: "fallback", Result: GatewayRequestResultSuccess, Requests: 1, TotalCost: 0.3, Version: gatewayRequestAggregateVersion},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("create aggregate rows: %v", err)
	}
	previousLimit := gatewayRequestAggregateReadLimit
	gatewayRequestAggregateReadLimit = 1
	t.Cleanup(func() { gatewayRequestAggregateReadLimit = previousLimit })

	costs, err := store.RequestLogChannelCosts(GatewayRequestLogFilter{})
	if err != nil {
		t.Fatalf("grouped channel costs: %v", err)
	}
	if len(costs) != 2 || math.Abs(costs[1]-0.3) > 1e-9 || math.Abs(costs[2]-0.3) > 1e-9 {
		t.Fatalf("channel costs = %#v", costs)
	}
}

func TestGatewayRequestAggregateGroupedKeysetDoesNotShiftAfterEarlierInsert(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	bucket := time.Date(2026, 7, 19, 5, 0, 0, 0, time.UTC)
	for _, channelID := range []uint{1, 2, 3} {
		row := GatewayRequestAggregate{
			BucketStart: bucket, DownstreamKeyID: 1, ChannelID: channelID, ModelName: "gpt-5",
			CostSource: "reported", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
			Requests: 1, FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion,
		}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	previousBatchSize := gatewayRequestAggregateGroupedBatchSize
	gatewayRequestAggregateGroupedBatchSize = 2
	t.Cleanup(func() { gatewayRequestAggregateGroupedBatchSize = previousBatchSize })
	visited := make([]uint, 0, 3)
	err := store.visitRequestAggregateGroupedMetrics(GatewayRequestLogFilter{}, "channel_id AS group_id", "channel_id", func(row requestAggregateMetricsScan) any {
		return row.GroupID
	}, func(row requestAggregateMetricsScan) error {
		visited = append(visited, row.GroupID)
		if len(visited) == 2 {
			return db.Create(&GatewayRequestAggregate{
				BucketStart: bucket, DownstreamKeyID: 1, ChannelID: 0, ModelName: "gpt-5",
				CostSource: "reported", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
				Requests: 1, FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion,
			}).Error
		}
		return nil
	})
	if err != nil {
		t.Fatalf("visit grouped metrics: %v", err)
	}
	if len(visited) != 3 || visited[0] != 1 || visited[1] != 2 || visited[2] != 3 {
		t.Fatalf("visited groups = %v", visited)
	}
}

func TestGatewayRequestAggregateGroupedResultLimitFailsExplicitly(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	bucket := time.Date(2026, 7, 19, 6, 0, 0, 0, time.UTC)
	for _, model := range []string{"gpt-1", "gpt-2", "gpt-3"} {
		if err := db.Create(&GatewayRequestAggregate{
			BucketStart: bucket, DownstreamKeyID: 1, ChannelID: 1, ModelName: model,
			CostSource: "reported", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
			Requests: 1, FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	previousReadLimit := gatewayRequestAggregateReadLimit
	previousGroupedLimit := gatewayRequestAggregateGroupedReadLimit
	gatewayRequestAggregateReadLimit = 2
	gatewayRequestAggregateGroupedReadLimit = 2
	t.Cleanup(func() {
		gatewayRequestAggregateReadLimit = previousReadLimit
		gatewayRequestAggregateGroupedReadLimit = previousGroupedLimit
	})
	if _, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 8); !errors.Is(err, errGatewayRequestAggregateGroupedLimit) {
		t.Fatalf("grouped limit error = %v", err)
	}
}

func TestGatewayRequestAggregateEdgeResultLimitFailsExplicitly(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 19, 7, 1, 0, 0, time.UTC)
	end := time.Date(2026, 7, 19, 8, 0, 0, 0, time.UTC)
	for index, model := range []string{"gpt-edge-1", "gpt-edge-2", "gpt-edge-3"} {
		if err := store.AppendRequestLog(&GatewayRequestLog{
			DownstreamKeyID: uint(index + 1), ChannelID: 1, ModelName: model,
			Method: "POST", Path: "/v1/responses", StatusCode: 200,
			TotalTokens: 10, Cost: 0.1, CreatedAt: start.Add(time.Duration(index) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	for index, model := range []string{"gpt-main-1", "gpt-main-2", "gpt-main-3"} {
		bucket := time.Date(2026, 7, 19, 7, 15+index*15, 0, 0, time.UTC)
		if err := db.Create(&GatewayRequestAggregate{
			BucketStart: bucket, DownstreamKeyID: 10, ChannelID: 2, ModelName: model,
			CostSource: "reported", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
			Requests: 1, FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	previousReadLimit := gatewayRequestAggregateReadLimit
	previousGroupedLimit := gatewayRequestAggregateGroupedReadLimit
	gatewayRequestAggregateReadLimit = 2
	gatewayRequestAggregateGroupedReadLimit = 2
	t.Cleanup(func() {
		gatewayRequestAggregateReadLimit = previousReadLimit
		gatewayRequestAggregateGroupedReadLimit = previousGroupedLimit
	})
	if _, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &start, End: &end}, 8); !errors.Is(err, errGatewayRequestAggregateGroupedLimit) {
		t.Fatalf("edge grouped limit error = %v", err)
	}
}

func TestGatewayRequestRawUnitPriceLimitCountsRowsBeforeFamilyFiltering(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	for index, model := range []string{"grok-1", "grok-2", "grok-3"} {
		if err := store.AppendRequestLog(&GatewayRequestLog{
			DownstreamKeyID: uint(index + 1), ChannelID: 1, ModelName: model,
			Method: "POST", Path: "/v1/responses", StatusCode: 200,
			TotalTokens: 10, Cost: 0.1, CreatedAt: start.Add(time.Duration(index) * time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.requestLogModelUnitPriceTrend(GatewayRequestLogFilter{Start: &start, End: &end}, 3); !errors.Is(err, errGatewayRequestAggregateGroupedLimit) {
		t.Fatalf("raw unit price limit error = %v", err)
	}
}

func TestGatewayRequestAggregateMigrationRawAnalyticsBoundsUnitPriceRows(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 19, 9, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	for index, model := range []string{"grok-migration-1", "grok-migration-2", "grok-migration-3"} {
		if err := db.Create(&GatewayRequestLog{
			DownstreamKeyID: uint(index + 1), ChannelID: 1, ModelName: model,
			Method: "POST", Path: "/v1/responses", StatusCode: 200,
			TotalTokens: 10, Cost: 0.1, AggregateVersion: gatewayRequestAggregateVersion - 1,
			CreatedAt: start.Add(time.Duration(index) * time.Minute),
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	previousGroupedLimit := gatewayRequestAggregateGroupedReadLimit
	gatewayRequestAggregateGroupedReadLimit = 2
	t.Cleanup(func() { gatewayRequestAggregateGroupedReadLimit = previousGroupedLimit })

	if _, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &start, End: &end}, 8); !errors.Is(err, errGatewayRequestAggregateGroupedLimit) {
		t.Fatalf("migration raw analytics limit error = %v", err)
	}
}

func TestGatewayRequestAggregateFutureVersionFallsBackWithoutDoubleCounting(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "future-version", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", PasswordCipher: "x"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	bucket := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	logs := []GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: channel.ID, ModelName: "current", Method: "POST", Path: "/v1/responses", StatusCode: 200, AggregateVersion: gatewayRequestAggregateVersion, CreatedAt: bucket},
		{DownstreamKeyID: 1, ChannelID: channel.ID, ModelName: "future", Method: "POST", Path: "/v1/responses", StatusCode: 200, AggregateVersion: gatewayRequestAggregateVersion + 1, CreatedAt: bucket.Add(time.Minute)},
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatal(err)
	}
	rows := []GatewayRequestAggregate{
		{BucketStart: bucket, DownstreamKeyID: 1, ChannelID: channel.ID, ModelName: "current", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess, Requests: 1, FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion},
		{BucketStart: bucket, DownstreamKeyID: 1, ChannelID: channel.ID, ModelName: "future", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess, Requests: 50, FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion + 1},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	ready, err := requestAggregatesReady(db)
	if err != nil || ready {
		t.Fatalf("future-version aggregates ready=%t err=%v", ready, err)
	}
	summary, err := store.RequestLogSummary()
	if err != nil {
		t.Fatal(err)
	}
	if summary.TotalRequests != 2 {
		t.Fatalf("raw fallback requests = %d, want 2", summary.TotalRequests)
	}
	var currentRequests int64
	if err := store.applyRequestAggregateFilter(db.Model(&GatewayRequestAggregate{}), GatewayRequestLogFilter{}).
		Select("COALESCE(SUM(requests), 0)").Scan(&currentRequests).Error; err != nil {
		t.Fatal(err)
	}
	if currentRequests != 1 {
		t.Fatalf("current aggregate requests = %d, want 1", currentRequests)
	}
	items, err := channels.List()
	if err != nil || len(items) != 1 {
		t.Fatalf("channels = %+v, err=%v", items, err)
	}
	if items[0].RequestCount != 2 {
		t.Fatalf("channel request count = %d, want 2", items[0].RequestCount)
	}
}

func TestSummarizeUsageReadsIncrementalAggregateLedger(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	bucket := time.Date(2026, 7, 19, 11, 0, 0, 0, time.UTC)
	rows := []GatewayRequestAggregate{
		{
			BucketStart: bucket, DownstreamKeyID: 7, ChannelID: 1, ModelName: "gpt-5", CostSource: "reported",
			RouteReason: "smart_primary", Result: GatewayRequestResultSuccess, Requests: 2,
			PromptTokens: 60, CompletionTokens: 20, TotalTokens: 80,
			TotalCost: 0.4, ActualCost: 0.4, ReportedCost: 0.4,
			LatencyTotalMs: 400, LatencySamples: 2, FirstRequestAt: bucket, LastRequestAt: bucket.Add(5 * time.Minute),
			Version: gatewayRequestAggregateVersion,
		},
		{
			BucketStart: bucket, DownstreamKeyID: 7, ChannelID: 2, ModelName: "gpt-5", CostSource: "estimated",
			RouteReason: "fallback", Result: GatewayRequestResultFailed, Requests: 1,
			PromptTokens: 15, CompletionTokens: 5, TotalTokens: 20,
			TotalCost: 0.1, EstimatedCost: 0.1,
			FirstRequestAt: bucket.Add(6 * time.Minute), LastRequestAt: bucket.Add(6 * time.Minute),
			Version: gatewayRequestAggregateVersion,
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	usage, err := store.SummarizeUsage(7)
	if err != nil {
		t.Fatal(err)
	}
	if usage.Requests != 3 || usage.Success != 2 || usage.Failed != 1 || usage.AvgLatency != 200 {
		t.Fatalf("aggregate usage counts = %+v", usage)
	}
	if usage.TotalTokens != 100 || usage.PromptTokens != 75 || usage.CompletionTokens != 25 {
		t.Fatalf("aggregate usage tokens = %+v", usage)
	}
	if usage.TotalCost != 0.5 || usage.ActualCost != 0.4 || usage.ReportedCost != 0.4 || usage.EstimatedCost != 0.1 || usage.ReconciledCost != 0 {
		t.Fatalf("aggregate usage costs = %+v", usage)
	}
	wantLast := bucket.Add(6 * time.Minute)
	if usage.LastUsedAt == nil || !usage.LastUsedAt.Equal(wantLast) {
		t.Fatalf("aggregate usage last used = %v, want %v", usage.LastUsedAt, wantLast)
	}
}

func TestGatewayRequestAggregateLongRangeEdgesUseDailyBuckets(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 1, 0, 7, 0, 0, time.UTC)
	end := time.Date(2026, 7, 4, 0, 8, 0, 0, time.UTC)
	for _, item := range []struct {
		at   time.Time
		cost float64
	}{
		{at: start.Add(time.Minute), cost: 0.1},
		{at: time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC), cost: 0.2},
		{at: time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC), cost: 0.3},
		{at: end.Add(-time.Minute), cost: 0.4},
	} {
		if err := store.AppendRequestLog(&GatewayRequestLog{
			DownstreamKeyID: 1, ChannelID: 1, ModelName: "gpt-5",
			Method: "POST", Path: "/v1/responses", StatusCode: 200,
			PromptTokens: 80, CompletionTokens: 20, TotalTokens: 100, Cost: item.cost, CostSource: "reported", CreatedAt: item.at,
		}); err != nil {
			t.Fatal(err)
		}
	}
	previousReadLimit := gatewayRequestAggregateReadLimit
	gatewayRequestAggregateReadLimit = 1
	t.Cleanup(func() { gatewayRequestAggregateReadLimit = previousReadLimit })

	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &start, End: &end, TimeZone: "UTC"}, 8)
	if err != nil {
		t.Fatalf("long-range aggregate analytics: %v", err)
	}
	if len(analytics.Trend) != 4 || len(analytics.ModelUnitPrices) != 8 {
		t.Fatalf("daily edge buckets: trend=%+v unit_prices=%+v", analytics.Trend, analytics.ModelUnitPrices)
	}
	for _, row := range analytics.Trend {
		if len(row.Bucket) != len("2006-01-02") || strings.Contains(row.Bucket, "T") {
			t.Fatalf("trend mixed non-daily edge bucket %q", row.Bucket)
		}
	}
	for _, row := range analytics.ModelUnitPrices {
		if len(row.Bucket) != len("2006-01-02") || strings.Contains(row.Bucket, "T") {
			t.Fatalf("unit price mixed non-daily edge bucket %q", row.Bucket)
		}
	}
}

func TestGatewayRequestAggregateMigrationCacheAvoidsRepeatedRawSummary(t *testing.T) {
	if gatewayRequestAggregateMigrationCacheTTL != 2*time.Minute {
		t.Fatalf("migration cache TTL = %s, want 2m", gatewayRequestAggregateMigrationCacheTTL)
	}
	db := openTestDB(t)
	store := NewGateway(db)
	log := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, TotalTokens: 10, AggregateVersion: 1, CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(&log).Error; err != nil {
		t.Fatalf("create migrating request log: %v", err)
	}

	var rawSummaries atomic.Int64
	callbackName := "test:gateway-aggregate-migration-cache"
	if err := db.Callback().Row().After("gorm:row").Register(callbackName, func(tx *gorm.DB) {
		sql := tx.Statement.SQL.String()
		if strings.Contains(sql, `FROM "gateway_request_logs"`) && strings.Contains(sql, "AS total_requests") {
			rawSummaries.Add(1)
		}
	}); err != nil {
		t.Fatalf("register query counter: %v", err)
	}
	t.Cleanup(func() { _ = db.Callback().Row().Remove(callbackName) })

	for attempt := 0; attempt < 2; attempt++ {
		summary, err := store.RequestLogSummary()
		if err != nil || summary.TotalRequests != 1 {
			t.Fatalf("migration summary attempt %d = %+v, err=%v", attempt, summary, err)
		}
	}
	if rawSummaries.Load() != 1 {
		t.Fatalf("raw summary queries = %d, want 1", rawSummaries.Load())
	}
}
