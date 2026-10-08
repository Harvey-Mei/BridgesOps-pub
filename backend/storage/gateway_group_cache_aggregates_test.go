package storage

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestGatewayRequestGroupCacheFutureMarkerFailsClosed(t *testing.T) {
	db := testdb.Open(t)
	log := &GatewayRequestLog{
		DownstreamKeyID: 41, ChannelID: 42, ModelName: "future-marker", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, PromptTokens: 100, AggregateVersion: gatewayRequestAggregateVersion,
		GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion + 1, CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(log).Error; err != nil {
		t.Fatalf("create future group marker log: %v", err)
	}
	if err := db.Model(&GatewayRequestGroupCacheBackfillState{}).Where("id = ?", gatewayRequestGroupCacheBackfillStateID).
		Updates(map[string]any{"target_version": gatewayRequestGroupCacheAggregateVersion, "completed": true}).Error; err != nil {
		t.Fatalf("mark group cache backfill complete: %v", err)
	}
	ready, err := groupCacheRoutingStatsReady(db)
	if err != nil {
		t.Fatalf("check future group marker readiness: %v", err)
	}
	if ready {
		t.Fatal("future group marker was reported ready")
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); !errors.Is(err, errGatewayRequestGroupCacheAggregateVersionConflict) {
		t.Fatalf("future group marker backfill error = %v", err)
	}
}

func TestGatewayRequestGroupCacheFutureAggregateFailsClosed(t *testing.T) {
	db := testdb.Open(t)
	if err := db.Model(&GatewayRequestGroupCacheBackfillState{}).Where("id = ?", gatewayRequestGroupCacheBackfillStateID).
		Updates(map[string]any{"target_version": gatewayRequestGroupCacheAggregateVersion, "completed": true}).Error; err != nil {
		t.Fatalf("mark group cache backfill complete: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Minute)
	row := &GatewayRequestGroupCacheAggregate{
		BucketStart: now, DownstreamKeyID: 1, ChannelID: 2, GroupName: "future", ModelName: "gpt",
		PromptTokens: 100, CacheReadTokens: 50, ValidRequests: 1, LastNormalUseAt: now,
		Version: gatewayRequestGroupCacheAggregateVersion + 1,
	}
	if err := db.Create(row).Error; err != nil {
		t.Fatalf("create future group cache aggregate: %v", err)
	}
	ready, err := groupCacheRoutingStatsReady(db)
	if err != nil {
		t.Fatalf("check future group aggregate readiness: %v", err)
	}
	if ready {
		t.Fatal("future group aggregate was reported ready")
	}
}

func TestRequestGroupCacheAggregateFromLog(t *testing.T) {
	createdAt := time.Date(2026, 7, 23, 4, 5, 6, 0, time.FixedZone("test", 8*60*60))
	log := &GatewayRequestLog{
		DownstreamKeyID: 7, ChannelID: 9, ChannelNameSnapshot: " Alpha ", GroupName: " pro ",
		PromptTokens: 100, CacheCreationTokens: -3, CacheReadTokens: 25, CreatedAt: createdAt,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: "smart_primary",
	}
	row, ok := requestGroupCacheAggregateFromLog(log)
	if !ok {
		t.Fatal("normal request was excluded from group cache aggregate")
	}
	if !row.BucketStart.Equal(createdAt.UTC().Truncate(gatewayRequestAggregateBucket)) || !row.LastNormalUseAt.Equal(createdAt.UTC()) || row.DownstreamKeyID != 7 || row.ChannelID != 9 || row.ChannelNameSnapshot != "Alpha" || row.GroupName != "pro" || row.ModelName != "" || row.CacheCreationTokens != 0 || row.CacheReadTokens != 25 || row.PromptTokens != 100 || row.ValidRequests != 1 || row.Version != gatewayRequestGroupCacheAggregateVersion {
		t.Fatalf("group cache row = %+v", row)
	}
	missingPrompt := *log
	missingPrompt.PromptTokens = 0
	missingPrompt.CacheReadTokens = 900
	missingPrompt.CacheCreationTokens = 800
	missingRow, ok := requestGroupCacheAggregateFromLog(&missingPrompt)
	if !ok {
		t.Fatal("normal request with a missing prompt-token denominator was excluded")
	}
	if missingRow.PromptTokens != 0 || missingRow.CacheReadTokens != 0 || missingRow.CacheCreationTokens != 0 || missingRow.ValidRequests != 1 || !missingRow.LastNormalUseAt.Equal(missingRow.BucketStart) {
		t.Fatalf("missing-denominator group cache row = %+v", missingRow)
	}
	for _, excluded := range []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "pro", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, RouteReason: gatewayRouteReasonLatencyProbe},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "pro", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, RouteReason: gatewayRouteReasonKeyTest},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "pro", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, RouteReason: "model_check"},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "pro", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, RouteReason: "internal_refresh"},
		{DownstreamKeyID: 0, ChannelID: 2, GroupName: "pro", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, RouteReason: "smart_primary"},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "pro", Method: "POST", Path: "/v1/responses", StatusCode: 500, PromptTokens: 100, RouteReason: "smart_primary"},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "pro", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, RouteReason: "smart_primary", Error: "failed"},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "pro", Method: "GET", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, RouteReason: "smart_primary"},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "pro", Method: "POST", Path: "/v1/models", StatusCode: 200, PromptTokens: 100, RouteReason: "smart_primary"},
	} {
		if _, ok := requestGroupCacheAggregateFromLog(excluded); ok {
			t.Fatalf("excluded request produced group cache row: %+v", excluded)
		}
	}
}

func TestListGroupCacheRoutingStatsExcludesLatencyProbeTraffic(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Minute)
	for _, log := range []*GatewayRequestLog{
		{
			DownstreamKeyID: 1, ChannelID: 7, GroupName: "pro", ModelName: "gpt", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, PromptTokens: 100, CacheReadTokens: 40, RouteReason: "smart_primary", CreatedAt: now,
		},
		{
			DownstreamKeyID: 1, ChannelID: 7, GroupName: "pro", ModelName: "gpt", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, PromptTokens: 10_000, CacheReadTokens: 10_000, RouteReason: gatewayRouteReasonLatencyProbe, CreatedAt: now,
		},
	} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	if err := BackfillGatewayRequestGroupCacheAggregates(db); err != nil {
		t.Fatalf("backfill group cache aggregates: %v", err)
	}
	stats, err := store.ListGroupCacheRoutingStats(now.Add(time.Minute), time.Hour, 1_000_000, []GatewayRequestGroupCacheRoutingRoute{{ChannelID: 7, GroupName: "pro"}})
	if err != nil {
		t.Fatalf("list routing cache stats: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("routing cache stats = %+v, want one normal group", stats)
	}
	if stats[0].ChannelID != 7 || stats[0].GroupName != "pro" || stats[0].PromptTokens != 100 || stats[0].CacheReadTokens != 40 {
		t.Fatalf("latency probe polluted routing cache stats: %+v", stats[0])
	}
}

func TestListGroupCacheRoutingStatsUsesBoundedTokenWindowAndExactExpiry(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 23, 12, 7, 0, 0, time.UTC)
	if err := db.Model(&GatewayRequestGroupCacheBackfillState{}).Where("id = ?", gatewayRequestGroupCacheBackfillStateID).Updates(map[string]any{
		"target_version": gatewayRequestGroupCacheAggregateVersion,
		"completed":      true,
	}).Error; err != nil {
		t.Fatalf("mark group-cache ledger ready: %v", err)
	}
	rows := []GatewayRequestGroupCacheAggregate{
		{BucketStart: aggregateBucketStart(now.Add(-time.Minute)), DownstreamKeyID: 1, ChannelID: 77, GroupName: "pro", ModelName: "new", PromptTokens: 60, CacheReadTokens: 30, ValidRequests: 1, LastNormalUseAt: now.Add(-time.Minute), Version: gatewayRequestGroupCacheAggregateVersion},
		{BucketStart: aggregateBucketStart(now.Add(-20 * time.Minute)), DownstreamKeyID: 1, ChannelID: 77, GroupName: "pro", ModelName: "middle", PromptTokens: 80, CacheReadTokens: 40, ValidRequests: 1, LastNormalUseAt: now.Add(-20 * time.Minute), Version: gatewayRequestGroupCacheAggregateVersion},
		{BucketStart: aggregateBucketStart(now.Add(-40 * time.Minute)), DownstreamKeyID: 1, ChannelID: 77, GroupName: "pro", ModelName: "old", PromptTokens: 1000, CacheReadTokens: 1000, ValidRequests: 1, LastNormalUseAt: now.Add(-40 * time.Minute), Version: gatewayRequestGroupCacheAggregateVersion},
		{BucketStart: aggregateBucketStart(now.Add(-8 * 24 * time.Hour)), DownstreamKeyID: 1, ChannelID: 88, GroupName: "stale", ModelName: "old", PromptTokens: 100, CacheReadTokens: 100, ValidRequests: 1, LastNormalUseAt: now.Add(-8 * 24 * time.Hour), Version: gatewayRequestGroupCacheAggregateVersion},
		{BucketStart: aggregateBucketStart(now.Add(-time.Minute)), DownstreamKeyID: 1, ChannelID: 89, GroupName: "slow", ModelName: "new", PromptTokens: 10, CacheReadTokens: 5, ValidRequests: 1, LastNormalUseAt: now.Add(-time.Minute), Version: gatewayRequestGroupCacheAggregateVersion},
		{BucketStart: aggregateBucketStart(now.Add(-8 * 24 * time.Hour)), DownstreamKeyID: 1, ChannelID: 89, GroupName: "slow", ModelName: "old", PromptTokens: 90, CacheReadTokens: 45, ValidRequests: 1, LastNormalUseAt: now.Add(-8 * 24 * time.Hour), Version: gatewayRequestGroupCacheAggregateVersion},
		// No matching channels row exists for 999. Routing statistics are keyed
		// by persisted route identity and must not disappear after a rename or
		// channel deletion race.
		{BucketStart: aggregateBucketStart(now.Add(-2 * time.Minute)), DownstreamKeyID: 1, ChannelID: 999, GroupName: "unknown", ModelName: "new", PromptTokens: 50, CacheReadTokens: 10, ValidRequests: 1, LastNormalUseAt: now.Add(-2 * time.Minute), Version: gatewayRequestGroupCacheAggregateVersion},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("create group-cache routing aggregates: %v", err)
	}
	stats, err := store.ListGroupCacheRoutingStats(now, 7*24*time.Hour, 100, []GatewayRequestGroupCacheRoutingRoute{
		{ChannelID: 77, GroupName: " Pro "},
		{ChannelID: 88, GroupName: "stale"},
		{ChannelID: 89, GroupName: "slow"},
		{ChannelID: 999, GroupName: "unknown"},
		{ChannelID: 1000, GroupName: "never"},
	})
	if err != nil {
		t.Fatalf("list bounded group-cache routing stats: %v", err)
	}
	byRoute := make(map[gatewayRequestGroupCacheRoutingKey]GatewayRequestGroupCacheRoutingStat, len(stats))
	for _, stat := range stats {
		byRoute[gatewayRequestGroupCacheRoutingKey{ChannelID: stat.ChannelID, GroupName: stat.GroupName}] = stat
	}
	pro := byRoute[gatewayRequestGroupCacheRoutingKey{ChannelID: 77, GroupName: "pro"}]
	if pro.PromptTokens != 100 || pro.CacheReadTokens != 50 || pro.Expired || !pro.LastInputAt.Equal(now.Add(-time.Minute)) {
		t.Fatalf("rolling 100-token route stats = %+v", pro)
	}
	stale := byRoute[gatewayRequestGroupCacheRoutingKey{ChannelID: 88, GroupName: "stale"}]
	if !stale.Expired || stale.PromptTokens != 0 || stale.CacheReadTokens != 0 || !stale.LastInputAt.Equal(now.Add(-8*24*time.Hour)) {
		t.Fatalf("seven-day expired route stats = %+v", stale)
	}
	slow := byRoute[gatewayRequestGroupCacheRoutingKey{ChannelID: 89, GroupName: "slow"}]
	if slow.PromptTokens != 100 || slow.CacheReadTokens != 50 || slow.Expired || !slow.LastInputAt.Equal(now.Add(-time.Minute)) {
		t.Fatalf("active low-volume route did not accumulate across expiry horizon: %+v", slow)
	}
	unknown := byRoute[gatewayRequestGroupCacheRoutingKey{ChannelID: 999, GroupName: "unknown"}]
	if unknown.PromptTokens != 50 || unknown.CacheReadTokens != 10 || unknown.Expired {
		t.Fatalf("unknown-channel route stats = %+v", unknown)
	}
	if _, ok := byRoute[gatewayRequestGroupCacheRoutingKey{ChannelID: 1000, GroupName: "never"}]; ok {
		t.Fatalf("never-used route unexpectedly returned: %+v", stats)
	}
}

func TestListGroupCacheRoutingStatsFailsClosedAtBucketWorkBudgetWithoutDroppingCompleteRoutes(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Minute)
	if err := db.Model(&GatewayRequestGroupCacheBackfillState{}).Where("id = ?", gatewayRequestGroupCacheBackfillStateID).Updates(map[string]any{
		"target_version": gatewayRequestGroupCacheAggregateVersion,
		"completed":      true,
	}).Error; err != nil {
		t.Fatalf("mark group-cache ledger ready: %v", err)
	}
	perRouteBucketLimit := gatewayRequestGroupCacheRoutingBucketBudget / 3
	rows := make([]GatewayRequestGroupCacheAggregate, 0, perRouteBucketLimit*2+2)
	for index := 0; index <= perRouteBucketLimit; index++ {
		at := aggregateBucketStart(now.Add(-time.Duration(index) * gatewayRequestAggregateBucket))
		rows = append(rows, GatewayRequestGroupCacheAggregate{
			BucketStart: at, DownstreamKeyID: 1, ChannelID: 177, GroupName: "bounded", ModelName: "tiny",
			PromptTokens: 1, CacheReadTokens: 1, ValidRequests: 1, LastNormalUseAt: at, Version: gatewayRequestGroupCacheAggregateVersion,
		})
	}
	for index := 0; index < perRouteBucketLimit; index++ {
		at := aggregateBucketStart(now.Add(-time.Duration(index) * gatewayRequestAggregateBucket))
		rows = append(rows, GatewayRequestGroupCacheAggregate{
			BucketStart: at, DownstreamKeyID: 1, ChannelID: 178, GroupName: "exact-limit", ModelName: "tiny",
			PromptTokens: 1, CacheReadTokens: 1, ValidRequests: 1, LastNormalUseAt: at, Version: gatewayRequestGroupCacheAggregateVersion,
		})
	}
	rows = append(rows, GatewayRequestGroupCacheAggregate{
		BucketStart: aggregateBucketStart(now), DownstreamKeyID: 1, ChannelID: 179, GroupName: "complete", ModelName: "large",
		PromptTokens: 100_000_000, CacheReadTokens: 40_000_000, ValidRequests: 1, LastNormalUseAt: now, Version: gatewayRequestGroupCacheAggregateVersion,
	})
	if err := db.CreateInBatches(&rows, 500).Error; err != nil {
		t.Fatalf("create bounded-work history: %v", err)
	}
	stats, err := store.ListGroupCacheRoutingStats(now, 7*24*time.Hour, 100_000_000, []GatewayRequestGroupCacheRoutingRoute{
		{ChannelID: 177, GroupName: "bounded"},
		{ChannelID: 178, GroupName: "exact-limit"},
		{ChannelID: 179, GroupName: "complete"},
	})
	if err != nil {
		t.Fatalf("list bounded-work route: %v", err)
	}
	if len(stats) != 1 || stats[0].ChannelID != 179 || stats[0].PromptTokens != 100_000_000 || stats[0].CacheReadTokens != 40_000_000 {
		t.Fatalf("bucket-budget routes polluted or dropped complete route: %+v", stats)
	}
}

func TestGatewayRequestGroupCacheReconcileRecomputesLastNormalUse(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	bucket := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{
			DownstreamKeyID: 41, ChannelID: 42, ChannelNameSnapshot: "last-use", GroupName: "pro", ModelName: "gpt-5",
			Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CacheReadTokens: 40,
			CreatedAt: bucket.Add(time.Minute),
		},
		{
			DownstreamKeyID: 41, ChannelID: 42, ChannelNameSnapshot: "last-use", GroupName: "pro", ModelName: "gpt-5",
			Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CacheReadTokens: 60,
			CreatedAt: bucket.Add(10 * time.Minute),
		},
	}
	for _, log := range logs {
		if err := db.Create(log).Error; err != nil {
			t.Fatalf("create request log: %v", err)
		}
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); err != nil {
		t.Fatalf("backfill aggregates: %v", err)
	}
	var before GatewayRequestGroupCacheAggregate
	if err := db.Where("channel_id = ? AND group_name = ?", 42, "pro").First(&before).Error; err != nil {
		t.Fatalf("find initial aggregate: %v", err)
	}
	if before.ValidRequests != 2 || !before.LastNormalUseAt.Equal(logs[1].CreatedAt) {
		t.Fatalf("initial last use = %+v", before)
	}
	if err := store.UpdateRequestLogActualCostWithUsage(logs[1].ID, "usage-zero-prompt", 0.1, 0.1, "sub2api_actual_cost", GatewayRequestUsageMetrics{
		PromptTokens: 0, CompletionTokens: 1, TotalTokens: 1, HasTokenMetrics: true,
	}); err != nil {
		t.Fatalf("reconcile latest request: %v", err)
	}
	var after GatewayRequestGroupCacheAggregate
	if err := db.Where("channel_id = ? AND group_name = ?", 42, "pro").First(&after).Error; err != nil {
		t.Fatalf("find reconciled aggregate: %v", err)
	}
	if after.ValidRequests != 2 || after.PromptTokens != 100 || !after.LastNormalUseAt.Equal(logs[0].CreatedAt) {
		t.Fatalf("last normal use was not recomputed: %+v", after)
	}
}

func TestRequestLogGroupCacheRatiosRawExcludesLatencyProbeTraffic(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	for _, log := range []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 9, GroupName: "raw", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CacheReadTokens: 20, RouteReason: "smart_primary", CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 9, GroupName: "raw", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 900, CacheReadTokens: 900, RouteReason: gatewayRouteReasonLatencyProbe, CreatedAt: now.Add(time.Second)},
		{DownstreamKeyID: 1, ChannelID: 9, GroupName: "raw", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 0, CacheReadTokens: 700, RouteReason: "smart_primary", CreatedAt: now.Add(2 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 10, GroupName: "missing", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 0, CacheReadTokens: 800, RouteReason: "smart_primary", CreatedAt: now.Add(3 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 11, GroupName: "failed", Method: "POST", Path: "/v1/responses", StatusCode: 502, PromptTokens: 100, CacheReadTokens: 100, RouteReason: "smart_primary", CreatedAt: now.Add(4 * time.Second)},
	} {
		if err := db.Create(log).Error; err != nil {
			t.Fatalf("create raw request log: %v", err)
		}
	}
	rows, err := store.requestLogGroupCacheRatiosRaw(GatewayRequestLogFilter{}, 10)
	if err != nil {
		t.Fatalf("read raw group cache ratios: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("raw cache ratios = %+v, want two normal groups", rows)
	}
	byChannel := make(map[uint]GatewayRequestLogGroupCacheRatio, len(rows))
	for _, row := range rows {
		byChannel[row.ChannelID] = row
	}
	raw := byChannel[9]
	if raw.PromptTokens != 100 || raw.CacheReadTokens != 20 || raw.ValidRequests != 2 || raw.Ratio == nil || *raw.Ratio != 0.2 {
		t.Fatalf("probe or missing denominator polluted raw cache ratio: %+v", raw)
	}
	missing := byChannel[10]
	if missing.PromptTokens != 0 || missing.CacheReadTokens != 0 || missing.ValidRequests != 1 || missing.Ratio != nil {
		t.Fatalf("missing-denominator raw cache ratio = %+v", missing)
	}
}

func TestGatewayRequestGroupCacheBackfillRepairsCurrentRawLog(t *testing.T) {
	db := testdb.Open(t)
	createdAt := time.Date(2026, 7, 23, 5, 0, 0, 0, time.UTC)
	log := &GatewayRequestLog{
		DownstreamKeyID: 11, ChannelID: 12, ChannelNameSnapshot: "current-history", GroupName: "pro", ModelName: "gpt-5",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CacheReadTokens: 40,
		AggregateVersion: gatewayRequestAggregateVersion, CreatedAt: createdAt,
	}
	if err := db.Create(log).Error; err != nil {
		t.Fatalf("create current request log: %v", err)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); err != nil {
		t.Fatalf("repair current request log: %v", err)
	}
	var row GatewayRequestGroupCacheAggregate
	if err := db.Where("channel_id = ? AND group_name = ? AND model_name = ?", log.ChannelID, log.GroupName, log.ModelName).First(&row).Error; err != nil {
		t.Fatalf("find repaired group cache aggregate: %v", err)
	}
	if row.ValidRequests != 1 || row.PromptTokens != 100 || row.CacheReadTokens != 40 {
		t.Fatalf("repaired group cache aggregate = %+v", row)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.GroupCacheAggregateVersion != gatewayRequestGroupCacheAggregateVersion {
		t.Fatalf("group-cache marker = %d, want %d", stored.GroupCacheAggregateVersion, gatewayRequestGroupCacheAggregateVersion)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); err != nil {
		t.Fatalf("repeat group cache backfill: %v", err)
	}
	var count int64
	if err := db.Model(&GatewayRequestGroupCacheAggregate{}).Where("channel_id = ?", log.ChannelID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("repeat backfill produced %d group cache rows, want 1", count)
	}
}

func TestGatewayRequestGroupCacheBackfillDoesNotSplitCurrentMainSnapshot(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	channel := Channel{Name: "current-snapshot", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", PasswordCipher: "cipher"}
	if err := channels.Create(&channel); err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 7, 23, 5, 5, 0, 0, time.UTC)
	log := &GatewayRequestLog{
		DownstreamKeyID: 13, ChannelID: channel.ID, GroupName: "pro", ModelName: "gpt-5",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CacheReadTokens: 40,
		AggregateVersion: gatewayRequestAggregateVersion, CreatedAt: createdAt,
	}
	if err := db.Create(log).Error; err != nil {
		t.Fatalf("create current request log: %v", err)
	}
	mainAggregate := &GatewayRequestAggregate{
		BucketStart: aggregateBucketStart(createdAt), DownstreamKeyID: log.DownstreamKeyID, ChannelID: log.ChannelID,
		ModelName: log.ModelName, CostSource: "", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
		Requests: 1, PromptTokens: 100, FirstRequestAt: createdAt, LastRequestAt: createdAt,
		Version: gatewayRequestAggregateVersion,
	}
	if err := db.Create(mainAggregate).Error; err != nil {
		t.Fatalf("create current main aggregate: %v", err)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); err != nil {
		t.Fatalf("backfill current snapshot log: %v", err)
	}
	var storedLog GatewayRequestLog
	if err := db.First(&storedLog, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedLog.ChannelNameSnapshot != "" {
		t.Fatalf("current main aggregate snapshot was rewritten: %q", storedLog.ChannelNameSnapshot)
	}
	var storedMain GatewayRequestAggregate
	if err := db.First(&storedMain, mainAggregate.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedMain.ChannelNameSnapshot != "" {
		t.Fatalf("main aggregate snapshot changed: %q", storedMain.ChannelNameSnapshot)
	}
	var groupRow GatewayRequestGroupCacheAggregate
	if err := db.Where("channel_id = ? AND group_name = ? AND model_name = ?", log.ChannelID, log.GroupName, log.ModelName).First(&groupRow).Error; err != nil {
		t.Fatalf("find group aggregate: %v", err)
	}
	if groupRow.ChannelNameSnapshot != "" {
		t.Fatalf("group aggregate snapshot diverged from main key: %q", groupRow.ChannelNameSnapshot)
	}
}

func TestGatewayRequestGroupCacheConcurrentAppendAndBackfillCountsOnce(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	createdAt := time.Date(2026, 7, 23, 5, 15, 0, 0, time.UTC)
	legacy := &GatewayRequestLog{
		DownstreamKeyID: 21, ChannelID: 22, ChannelNameSnapshot: "concurrent-history", GroupName: "pro", ModelName: "gpt-5",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CacheReadTokens: 20,
		AggregateVersion: 0, CreatedAt: createdAt,
	}
	if err := db.Create(legacy).Error; err != nil {
		t.Fatalf("create legacy request log: %v", err)
	}
	live := &GatewayRequestLog{
		DownstreamKeyID: 21, ChannelID: 22, ChannelNameSnapshot: "concurrent-history", GroupName: "pro", ModelName: "gpt-5",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CacheReadTokens: 30,
		CreatedAt: createdAt.Add(time.Second),
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	errs := make(chan error, 2)
	go func() {
		defer workers.Done()
		<-start
		_, err := backfillGatewayRequestAggregates(context.Background(), db, 1, 0)
		errs <- err
	}()
	go func() {
		defer workers.Done()
		<-start
		errs <- store.AppendRequestLog(live)
	}()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent append/backfill: %v", err)
		}
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); err != nil {
		t.Fatalf("final group cache backfill: %v", err)
	}
	var row GatewayRequestGroupCacheAggregate
	if err := db.Where("channel_id = ? AND group_name = ? AND model_name = ?", legacy.ChannelID, legacy.GroupName, legacy.ModelName).First(&row).Error; err != nil {
		t.Fatalf("find concurrent group cache aggregate: %v", err)
	}
	if row.ValidRequests != 2 || row.PromptTokens != 200 || row.CacheReadTokens != 50 {
		t.Fatalf("concurrent group cache aggregate = %+v", row)
	}
}

func TestGatewayRequestGroupCacheReconcileDuringBackfillReplaysUpdatedRowOnce(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	createdAt := time.Date(2026, 7, 23, 5, 30, 0, 0, time.UTC)
	log := &GatewayRequestLog{
		DownstreamKeyID: 31, ChannelID: 32, ChannelNameSnapshot: "reconcile-history", GroupName: "pro", ModelName: "gpt-5",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CacheReadTokens: 25,
		CreatedAt: createdAt,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); err != nil {
		t.Fatalf("complete initial backfill: %v", err)
	}
	if err := db.Model(&GatewayRequestGroupCacheBackfillState{}).Where("id = ?", gatewayRequestGroupCacheBackfillStateID).
		Update("completed", false).Error; err != nil {
		t.Fatalf("pause group cache backfill: %v", err)
	}
	if err := store.UpdateRequestLogActualCostWithUsage(log.ID, "usage-reconcile", 0.2, 0.2, "sub2api_actual_cost", GatewayRequestUsageMetrics{
		PromptTokens: 200, CompletionTokens: 10, TotalTokens: 210, CacheReadTokens: 60, HasTokenMetrics: true, HasCacheTokenMetrics: true,
	}); err != nil {
		t.Fatalf("reconcile during backfill: %v", err)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); err != nil {
		t.Fatalf("rebuild reconciled row: %v", err)
	}
	var row GatewayRequestGroupCacheAggregate
	if err := db.Where("channel_id = ? AND group_name = ? AND model_name = ?", log.ChannelID, log.GroupName, log.ModelName).First(&row).Error; err != nil {
		t.Fatalf("find reconciled row: %v", err)
	}
	if row.ValidRequests != 1 || row.PromptTokens != 200 || row.CacheReadTokens != 60 {
		t.Fatalf("reconciled row was replayed incorrectly: %+v", row)
	}
}

func TestGatewayRequestGroupCacheAggregateBackfillAndReconcile(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	createdAt := time.Date(2026, 7, 23, 4, 5, 0, 0, time.UTC)
	log := &GatewayRequestLog{
		DownstreamKeyID: 7, ChannelID: 9, ChannelNameSnapshot: "cache-channel", GroupName: "pro",
		ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 100, CacheReadTokens: 25, CacheCreationTokens: 10, CompletionTokens: 5, TotalTokens: 115,
		Cost: 0.4, CostEstimated: true, CostSource: "gateway_estimate", CostStatus: GatewayCostStatusEstimated,
		AggregateVersion: 0, CreatedAt: createdAt,
	}
	if err := db.Create(log).Error; err != nil {
		t.Fatalf("create request log: %v", err)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 1, 0); err != nil {
		t.Fatalf("backfill aggregates: %v", err)
	}
	var row GatewayRequestGroupCacheAggregate
	if err := db.Where("channel_id = ? AND group_name = ?", log.ChannelID, log.GroupName).First(&row).Error; err != nil {
		t.Fatalf("find group cache aggregate: %v", err)
	}
	if row.Version != gatewayRequestGroupCacheAggregateVersion || row.ValidRequests != 1 || row.PromptTokens != 100 || row.CacheReadTokens != 25 || row.CacheCreationTokens != 10 {
		t.Fatalf("backfilled group cache aggregate = %+v", row)
	}
	if err := store.UpdateRequestLogActualCostWithUsage(log.ID, "usage-v2", 0.25, 0.25, "sub2api_actual_cost", GatewayRequestUsageMetrics{
		PromptTokens: 200, CompletionTokens: 8, TotalTokens: 208, CacheReadTokens: 60, CacheCreationTokens: 20,
		HasTokenMetrics: true, HasCacheTokenMetrics: true,
	}); err != nil {
		t.Fatalf("reconcile request log: %v", err)
	}
	if err := db.Where("channel_id = ? AND group_name = ?", log.ChannelID, log.GroupName).First(&row).Error; err != nil {
		t.Fatalf("find reconciled group cache aggregate: %v", err)
	}
	if row.ValidRequests != 1 || row.PromptTokens != 200 || row.CacheReadTokens != 60 || row.CacheCreationTokens != 20 {
		t.Fatalf("reconciled group cache aggregate = %+v", row)
	}
	var reconciled GatewayRequestLog
	if err := db.First(&reconciled, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reconciled.GroupCacheAggregateVersion != gatewayRequestGroupCacheAggregateVersion {
		t.Fatalf("reconciled group-cache marker = %d, want %d", reconciled.GroupCacheAggregateVersion, gatewayRequestGroupCacheAggregateVersion)
	}
	if ratio := float64(row.CacheReadTokens) / float64(row.PromptTokens); math.Abs(ratio-0.3) > 1e-12 {
		t.Fatalf("group cache ratio = %f, want 0.3", ratio)
	}
}
