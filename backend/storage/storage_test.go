package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	return testdb.Open(t)
}

func TestPostgresConnectionPoolRequiresCapacityBeyondAdvisoryLock(t *testing.T) {
	if err := validateDBConnectionPool(1); err == nil || !strings.Contains(err.Error(), "at least 2") {
		t.Fatalf("maxOpenConns=1 validation error = %v", err)
	}
	for _, maxOpenConns := range []int{0, 2, 20} {
		if err := validateDBConnectionPool(maxOpenConns); err != nil {
			t.Fatalf("maxOpenConns=%d validation error = %v", maxOpenConns, err)
		}
	}
}

func TestGatewayChannelLockSlotCountReservesQueryCapacity(t *testing.T) {
	tests := []struct {
		maxOpen int
		want    int
	}{
		{maxOpen: 0, want: gatewayChannelUnlimitedPoolLockSlots},
		{maxOpen: 1, want: 1},
		{maxOpen: 2, want: 1},
		{maxOpen: 3, want: 1},
		{maxOpen: 20, want: 10},
	}
	for _, tt := range tests {
		if got := gatewayChannelLockSlotCount(tt.maxOpen); got != tt.want {
			t.Fatalf("maxOpen=%d lock slots = %d, want %d", tt.maxOpen, got, tt.want)
		}
	}
}

func TestPostgresDSNUsesUTCAndValidatedTLSMode(t *testing.T) {
	dsn, err := (DBConfig{
		User: "bridgeops", Password: "secret value", Host: "db", Name: "bridgeops",
		TimeZone: "UTC", SSLMode: "verify-full",
	}).PostgresDSN()
	if err != nil {
		t.Fatalf("build Postgres DSN: %v", err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse Postgres DSN: %v", err)
	}
	if parsed.Scheme != "postgres" || parsed.Host != "db:5432" || parsed.Path != "/bridgeops" || parsed.User.Username() != "bridgeops" {
		t.Fatalf("Postgres DSN = %s", dsn)
	}
	if password, ok := parsed.User.Password(); !ok || password != "secret value" {
		t.Fatalf("Postgres DSN did not preserve password")
	}
	if got := parsed.Query().Get("TimeZone"); got != "UTC" {
		t.Fatalf("Postgres timezone = %q, want UTC", got)
	}
	if got := parsed.Query().Get("search_path"); got != "public" {
		t.Fatalf("Postgres search_path = %q, want public", got)
	}
	if got := parsed.Query().Get("sslmode"); got != "verify-full" {
		t.Fatalf("Postgres sslmode = %q", got)
	}
	for _, invalid := range []DBConfig{
		{User: "bridgeops", Host: "db", Name: "bridgeops", TimeZone: "Asia/Shanghai"},
		{User: "bridgeops", Host: "db", Name: "bridgeops", TimeZone: "UTC", SSLMode: "bad"},
	} {
		if _, err := invalid.PostgresDSN(); err == nil {
			t.Fatalf("invalid Postgres configuration was accepted: %+v", invalid)
		}
	}
}

func TestGatewayParsesDatabaseWallTimeWithConfiguredLocation(t *testing.T) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	store := &Gateway{databaseLocation: location}
	parsed, ok := store.parseRequestLogAggregateTime("2026-07-19 12:34:56")
	if !ok || !parsed.Equal(time.Date(2026, 7, 19, 4, 34, 56, 0, time.UTC)) {
		t.Fatalf("parsed database wall time = %v, ok=%t", parsed, ok)
	}
	bucket, ok := store.parseRequestLogHourBucket("2026-07-19T12:00:00")
	if !ok || !bucket.Equal(time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("parsed database hour bucket = %v, ok=%t", bucket, ok)
	}
}

func TestRequestAggregateUpsertQualifiesPostgresConflictColumns(t *testing.T) {
	sqlDB, err := sql.Open("pgx", "host=127.0.0.1 user=bridgeops database=bridgeops sslmode=disable")
	if err != nil {
		t.Fatalf("open lazy PostgreSQL connection: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB}), &gorm.Config{
		DisableAutomaticPing: true,
		DryRun:               true,
	})
	if err != nil {
		t.Fatalf("open dry-run PostgreSQL database: %v", err)
	}
	row := &GatewayRequestAggregate{
		BucketStart:     time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC),
		DownstreamKeyID: 1,
		ChannelID:       2,
		Requests:        1,
		FirstRequestAt:  time.Date(2026, 7, 21, 12, 1, 0, 0, time.UTC),
		LastRequestAt:   time.Date(2026, 7, 21, 12, 1, 0, 0, time.UTC),
		Version:         gatewayRequestAggregateVersion,
	}
	generated := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Clauses(requestAggregateOnConflict(row)).Create(row)
	})
	for _, column := range []string{"version", "requests", "first_request_at", "last_request_at"} {
		qualified := `"gateway_request_aggregates"."` + column + `"`
		if !strings.Contains(generated, qualified) {
			t.Fatalf("aggregate upsert does not qualify %s: %s", column, generated)
		}
	}
	for _, fragment := range []string{"LEAST(", "::numeric", strconv.FormatInt(math.MaxInt64, 10)} {
		if !strings.Contains(generated, fragment) {
			t.Fatalf("aggregate upsert is missing overflow guard %q: %s", fragment, generated)
		}
	}

	groupRow := &GatewayRequestGroupCacheAggregate{
		BucketStart:     row.BucketStart,
		DownstreamKeyID: row.DownstreamKeyID,
		ChannelID:       row.ChannelID,
		PromptTokens:    math.MaxInt64,
		ValidRequests:   1,
		LastNormalUseAt: row.LastRequestAt,
		Version:         gatewayRequestGroupCacheAggregateVersion,
	}
	groupGenerated := db.ToSQL(func(tx *gorm.DB) *gorm.DB {
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "bucket_start"}, {Name: "downstream_key_id"}, {Name: "channel_id"},
				{Name: "channel_name_snapshot"}, {Name: "group_name"}, {Name: "model_name"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"prompt_tokens":  groupCacheValueUpdate("prompt_tokens", groupRow.PromptTokens),
				"valid_requests": groupCacheValueUpdate("valid_requests", groupRow.ValidRequests),
			}),
		}).Create(groupRow)
	})
	for _, fragment := range []string{"LEAST(", "::numeric", strconv.FormatInt(math.MaxInt64, 10)} {
		if !strings.Contains(groupGenerated, fragment) {
			t.Fatalf("group-cache aggregate upsert is missing overflow guard %q: %s", fragment, groupGenerated)
		}
	}
}

func TestGatewayRequestAggregateBackfillAlsoPersistsCostStatuses(t *testing.T) {
	db := openTestDB(t)
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	usageID := "legacy-usage"
	logs := []GatewayRequestLog{
		{ChannelID: 1, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.1, CostEstimated: true, CostSource: "gateway_estimate", CreatedAt: now},
		{ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.2, CostSource: "cost", CreatedAt: now},
		{ChannelID: 3, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.3, CostSource: "newapi_quota", CreatedAt: now},
		{ChannelID: 4, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.4, CostSource: "custom", UpstreamUsageID: &usageID, CreatedAt: now},
		{ChannelID: 5, Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: now},
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatalf("create legacy request logs: %v", err)
	}
	var before []GatewayRequestLog
	if err := db.Order("id ASC").Find(&before).Error; err != nil {
		t.Fatalf("read request logs before backfill: %v", err)
	}
	for index := range before {
		if before[index].CostStatus != GatewayCostStatusNone {
			t.Fatalf("auto migrate rewrote log %d cost status to %q", index, before[index].CostStatus)
		}
	}
	summary, err := storeSummaryWithoutAggregates(db)
	if err != nil {
		t.Fatalf("summarize legacy costs before backfill: %v", err)
	}
	if math.Abs(summary.TotalCost-0.9) > 1e-12 || math.Abs(summary.EstimatedCost) > 1e-12 || math.Abs(summary.ReportedCost-0.2) > 1e-12 || math.Abs(summary.ReconciledCost-0.7) > 1e-12 || math.Abs(summary.ActualCost-0.9) > 1e-12 || summary.UnavailableRequests != 1 {
		t.Fatalf("legacy cost summary before backfill = %+v", summary)
	}
	store := NewGateway(db)
	items, total, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{})
	if err != nil || total != 5 || len(items) != 5 {
		t.Fatalf("legacy cost list before backfill: total=%d items=%+v err=%v", total, items, err)
	}
	seen := make(map[GatewayCostStatus]int)
	for _, item := range items {
		seen[item.CostStatus]++
	}
	if seen[GatewayCostStatusUnavailable] != 1 || seen[GatewayCostStatusReported] != 1 || seen[GatewayCostStatusReconciled] != 2 || seen[GatewayCostStatusNone] != 1 {
		t.Fatalf("legacy effective list statuses = %+v", seen)
	}
	processed, err := backfillGatewayRequestAggregates(context.Background(), db, 2, 0)
	if err != nil || processed != 5 {
		t.Fatalf("aggregate and cost status backfill: processed=%d err=%v", processed, err)
	}
	var got []GatewayRequestLog
	if err := db.Order("id ASC").Find(&got).Error; err != nil {
		t.Fatalf("read migrated request logs: %v", err)
	}
	want := []GatewayCostStatus{
		GatewayCostStatusUnavailable,
		GatewayCostStatusReported,
		GatewayCostStatusReconciled,
		GatewayCostStatusReconciled,
		GatewayCostStatusNone,
	}
	if len(got) != len(want) {
		t.Fatalf("migrated logs = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].CostStatus != want[index] {
			t.Fatalf("log %d cost status = %q, want %q", index, got[index].CostStatus, want[index])
		}
	}
	if got[0].CostEstimated || got[0].CostUnavailableReason != GatewayPricingUnavailableLegacyUnverifiedEstimate {
		t.Fatalf("legacy estimate persisted without unavailable provenance: %+v", got[0])
	}
}

func storeSummaryWithoutAggregates(db *gorm.DB) (*GatewayRequestLogSummary, error) {
	return NewGateway(db).requestLogSummaryFilteredAtRaw(GatewayRequestLogFilter{}, time.Now().UTC())
}

func TestCodexRadarStoreDeduplicatesChangedSnapshots(t *testing.T) {
	db := openTestDB(t)
	store := NewCodexRadarStore(db)
	now := time.Now().UTC()
	score := 75.0
	point := CodexRadarModelPoint{ConfigKey: "gpt", Score: &score, ObservedAt: now}
	if err := store.SaveSuccess("https://radar.test/current.json", `{"model_iq":{}}`, "payload-1", "model-1", now, []CodexRadarModelPoint{point}); err != nil {
		t.Fatalf("first SaveSuccess: %v", err)
	}
	if err := store.SaveSuccess("https://radar.test/current.json", `{"model_iq":{}}`, "payload-2", "model-1", now.Add(time.Minute), []CodexRadarModelPoint{point}); err != nil {
		t.Fatalf("duplicate SaveSuccess: %v", err)
	}
	if err := store.SaveSuccess("https://radar.test/current.json", `{"model_iq":{}}`, "payload-3", "model-2", now.Add(2*time.Minute), []CodexRadarModelPoint{{ConfigKey: "gpt", Score: &score, ObservedAt: now.Add(2 * time.Minute)}}); err != nil {
		t.Fatalf("changed SaveSuccess: %v", err)
	}
	points, err := store.ListModelPoints("https://radar.test/current.json", now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("ListModelPoints: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("history points = %d, want 2", len(points))
	}

	if err := store.SaveAttempt("https://radar.test/current.json", now.Add(3*time.Minute), "temporary failure"); err != nil {
		t.Fatalf("SaveAttempt: %v", err)
	}
	state, err := store.LoadState("https://radar.test/current.json")
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if state.PayloadHash != "payload-3" || state.LastError != "temporary failure" {
		t.Fatalf("state after attempt = %+v", state)
	}
	deleted, err := store.DeleteHistoryBefore(now.Add(90 * time.Second))
	if err != nil {
		t.Fatalf("DeleteHistoryBefore: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
}

func TestLastSuccessfulRouteIsIsolatedBySessionAndModel(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now()
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 2, GroupName: "group-a", ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: now.Add(-time.Minute)},
		{DownstreamKeyID: 1, SessionHash: "session-b", ChannelID: 3, GroupName: "group-b", ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: now},
		{DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 4, GroupName: "group-c", ModelName: "model-b", Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: now},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	route, err := store.LastSuccessfulRoute(1, "session-a", "model-a", now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("last route: %v", err)
	}
	if route == nil || route.ChannelID != 2 || route.GroupName != "group-a" {
		t.Fatalf("route = %#v, want session-a/model-a route", route)
	}
	withoutSession, err := store.LastSuccessfulRoute(1, "", "model-a", now.Add(-time.Hour))
	if err != nil || withoutSession != nil {
		t.Fatalf("route without session = %#v, err=%v", withoutSession, err)
	}
}

func TestLastSuccessfulRouteDoesNotRefreshFromAffinityLogs(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now()
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 2, GroupName: "fallback", ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: "fallback", CreatedAt: now.Add(-11 * time.Minute)},
		{DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 2, GroupName: "fallback", ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: "session_affinity", CreatedAt: now.Add(-time.Minute)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	route, err := store.LastSuccessfulRoute(1, "session-a", "model-a", now.Add(-10*time.Minute))
	if err != nil {
		t.Fatalf("last route: %v", err)
	}
	if route != nil {
		t.Fatalf("affinity log unexpectedly refreshed route: %#v", route)
	}
}

func TestLastSuccessfulRouteIsRevokedByLaterAcceptedFailure(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, SessionHash: "session-revoke", ChannelID: 2, GroupName: "primary", ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: "smart_primary", CreatedAt: now.Add(-time.Minute)},
		{DownstreamKeyID: 1, SessionHash: "session-revoke", ChannelID: 2, GroupName: "primary", ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 502, RouteReason: "smart_primary", UpstreamAccepted: true, Error: "stream stopped after request dispatch owner lease loss", CreatedAt: now},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	route, err := store.LastSuccessfulRoute(1, "session-revoke", "model-a", now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("last successful route: %v", err)
	}
	if route != nil {
		t.Fatalf("accepted stream failure did not revoke session affinity: %#v", route)
	}
}

func TestLastSuccessfulRouteIsRevokedByFailedAffinityFallbackBeforeAcceptance(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	origin := &GatewayRequestLog{
		DownstreamKeyID: 1, SessionHash: "session-affinity-fallback", ChannelID: 2, GroupName: "primary",
		ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 200,
		RouteReason: "smart_primary", CreatedAt: now.Add(-time.Minute),
	}
	if err := store.AppendRequestLog(origin); err != nil {
		t.Fatalf("append origin request log: %v", err)
	}
	if origin.ID == 0 {
		t.Fatal("origin request log did not receive an ID")
	}
	if err := store.AppendRequestLog(&GatewayRequestLog{
		DownstreamKeyID: 1, SessionHash: origin.SessionHash, ChannelID: 3, GroupName: "fallback",
		ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 502,
		RouteReason: "all_failed", SessionAffinityOriginRequestID: origin.ID,
		UpstreamAccepted: false, Error: "upstream transport failed", CreatedAt: now,
	}); err != nil {
		t.Fatalf("append failed affinity request log: %v", err)
	}
	route, err := store.LastSuccessfulRoute(1, origin.SessionHash, "model-a", now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("last successful route: %v", err)
	}
	if route != nil {
		t.Fatalf("failed pre-acceptance affinity fallback kept stale route: %#v", route)
	}
}

func TestCanceledGatewayRequestsDoNotBecomeAffinityRoutes(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now()
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 2, GroupName: "new-cancel", ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 499, RouteReason: "client_canceled", Error: "客户端取消", CreatedAt: now.Add(-time.Minute)},
		{DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 3, GroupName: "legacy-cancel", ModelName: "model-a", Method: "POST", Path: "/v1/responses", StatusCode: 200, Error: "context canceled", CreatedAt: now},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	route, err := store.LastSuccessfulRoute(1, "session-a", "model-a", now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("last route: %v", err)
	}
	if route != nil {
		t.Fatalf("canceled request unexpectedly became route: %#v", route)
	}
}

func TestCanceledGatewayRequestsDoNotAffectLatencyStats(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now()
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "fast", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 100, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "fast", Method: "POST", Path: "/v1/responses", StatusCode: 499, RouteReason: "client_canceled", Error: "客户端取消", LatencyMs: 60000, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "fast", Method: "POST", Path: "/v1/responses", StatusCode: 200, Error: "context canceled", LatencyMs: 70000, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "fast", Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "upstream context canceled while reading body", LatencyMs: 80000, CreatedAt: now},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	stats, err := store.ListSuccessfulLatencyStatsSince(now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("latency stats: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("stats len = %d, want 1: %#v", len(stats), stats)
	}
	if stats[0].AvgLatency != 100 || stats[0].Requests != 1 {
		t.Fatalf("latency stats = %#v, want one 100ms request", stats[0])
	}
	summary, err := store.RequestLogSummary()
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if summary.Success != 1 || summary.Failed != 1 || summary.Canceled != 2 || summary.AvgLatency != 100 {
		t.Fatalf("summary = %#v, want canceled requests excluded from success latency", summary)
	}
	failed, total, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{Result: GatewayRequestResultFailed})
	if err != nil || total != 1 || len(failed) != 1 || failed[0].Error != "upstream context canceled while reading body" {
		t.Fatalf("failed filter total=%d items=%+v err=%v", total, failed, err)
	}
}

func TestGatewayLatencySamplesPreferFirstByteAndFallBackToTotalLatency(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now()
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "fast", Method: "POST", Path: "/v1/responses", StatusCode: 200, FirstByteMs: 40, LatencyMs: 400, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, GroupName: "fast", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 200, CreatedAt: now.Add(time.Second)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	stats, err := store.ListSuccessfulLatencyStatsSince(now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("latency stats: %v", err)
	}
	if len(stats) != 1 || stats[0].AvgLatency != 120 || stats[0].Requests != 2 {
		t.Fatalf("latency stats = %#v, want average first-byte/fallback 120ms", stats)
	}
	recent, err := store.ListRecentSuccessfulLatencyLogsSince(now.Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("recent latency logs: %v", err)
	}
	if len(recent) != 2 || recent[0].LatencyMs != 200 || recent[1].LatencyMs != 40 {
		t.Fatalf("recent latency logs = %#v, want first-byte values with total-latency fallback", recent)
	}
}

func TestAggregateBalanceTrend(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, Balance: 20, SampledAt: day2.Add(12 * time.Hour)},
		{ChannelID: 2, Balance: 5, SampledAt: day2.Add(10 * time.Hour)},
		{ChannelID: 1, Balance: 7, SampledAt: day1.Add(11 * time.Hour)},
		{ChannelID: 2, Balance: 3, SampledAt: day1.Add(18 * time.Hour)},
		{ChannelID: 2, Balance: 9, SampledAt: day0.Add(8 * time.Hour)},
		{ChannelID: 2, Balance: 11, SampledAt: day0.Add(22 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	got, err := rates.AggregateBalanceTrend(3)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyAggregate{
		{Day: day2, Balance: trendFloat(25)},
		{Day: day1, Balance: trendFloat(10)},
		{Day: day0, Balance: trendFloat(11)},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if !trendFloatEqual(got[i].Balance, want[i].Balance) {
			t.Fatalf("balance %d mismatch: got %v want %v", i, got[i].Balance, want[i].Balance)
		}
	}
}

func TestChannelProxyEnabledPersists(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	ch := &Channel{
		Name:           "proxy-channel",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		ProxyEnabled:   true,
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	got, err := channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if !got.ProxyEnabled {
		t.Fatal("proxy_enabled = false, want true")
	}
}

func TestProxyEnabledPersistsForCaptchaAndNotification(t *testing.T) {
	db := openTestDB(t)

	captchas := NewCaptchas(db)
	cfg := &CaptchaConfig{
		Name:         "solver-proxy",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
		ProxyEnabled: true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("create captcha: %v", err)
	}
	gotCaptcha, err := captchas.FindByID(cfg.ID)
	if err != nil {
		t.Fatalf("find captcha: %v", err)
	}
	if !gotCaptcha.ProxyEnabled {
		t.Fatal("captcha proxy_enabled = false, want true")
	}

	notifies := NewNotifications(db)
	notify := &NotificationChannel{
		Name:         "notify-proxy",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
		ProxyEnabled: true,
	}
	if err := notifies.CreateChannel(notify); err != nil {
		t.Fatalf("create notification: %v", err)
	}
	gotNotify, err := notifies.FindChannel(notify.ID)
	if err != nil {
		t.Fatalf("find notification: %v", err)
	}
	if !gotNotify.ProxyEnabled {
		t.Fatal("notification proxy_enabled = false, want true")
	}
}

func TestAggregateBalanceTrendFillsMissingDays(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, Balance: 20, SampledAt: day0.Add(12 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	got, err := rates.AggregateBalanceTrend(3)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyAggregate{
		{Day: day2, Balance: trendFloat(10)},
		{Day: day1, Balance: nil},
		{Day: day0, Balance: trendFloat(20)},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if !trendFloatEqual(got[i].Balance, want[i].Balance) {
			t.Fatalf("balance %d mismatch: got %v want %v", i, got[i].Balance, want[i].Balance)
		}
	}
}

func TestAggregateCostTrend(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.1, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, TodayCost: 2.2, SampledAt: day2.Add(18 * time.Hour)},
		{ChannelID: 2, TodayCost: 0.8, SampledAt: day2.Add(10 * time.Hour)},
		{ChannelID: 1, TodayCost: 3.5, SampledAt: day1.Add(11 * time.Hour)},
		{ChannelID: 2, TodayCost: 1.2, SampledAt: day1.Add(13 * time.Hour)},
		{ChannelID: 2, TodayCost: 1.8, SampledAt: day1.Add(22 * time.Hour)},
		{ChannelID: 1, TodayCost: 4.0, SampledAt: day0.Add(8 * time.Hour)},
		{ChannelID: 2, TodayCost: 2.5, SampledAt: day0.Add(21 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	got, err := rates.AggregateCostTrend(3)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyCostAggregate{
		{Day: day2, Cost: trendFloat(3.0)},
		{Day: day1, Cost: trendFloat(5.3)},
		{Day: day0, Cost: trendFloat(6.5)},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if !trendFloatEqual(got[i].Cost, want[i].Cost) {
			t.Fatalf("cost %d mismatch: got %v want %v", i, got[i].Cost, want[i].Cost)
		}
	}
}

func TestAggregateCostTrendFillsMissingDays(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.5, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, TodayCost: 2.5, SampledAt: day0.Add(12 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	got, err := rates.AggregateCostTrend(3)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyCostAggregate{
		{Day: day2, Cost: trendFloat(1.5)},
		{Day: day1, Cost: nil},
		{Day: day0, Cost: trendFloat(2.5)},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if !trendFloatEqual(got[i].Cost, want[i].Cost) {
			t.Fatalf("cost %d mismatch: got %v want %v", i, got[i].Cost, want[i].Cost)
		}
	}
}

func TestTrendAggregatesEncodeMissingSamplesAsNull(t *testing.T) {
	payload, err := json.Marshal(struct {
		Balance DailyAggregate     `json:"balance"`
		Cost    DailyCostAggregate `json:"cost"`
	}{Balance: DailyAggregate{}, Cost: DailyCostAggregate{}})
	if err != nil {
		t.Fatalf("marshal trend aggregates: %v", err)
	}
	text := string(payload)
	if !strings.Contains(text, `"balance":null`) || !strings.Contains(text, `"cost":null`) {
		t.Fatalf("missing trend samples encoded as %s, want null values", text)
	}
}

func TestRateChangeAnalyticsAggregatesWindowAndChannel(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)
	now := time.Now().In(trendLocation)
	changes := []*RateChangeLog{
		{ChannelID: 1, ModelName: "gpt-a", OldRatio: trendFloat(1), NewRatio: 2, ChangedAt: now.Add(-time.Hour)},
		{ChannelID: 1, ModelName: "gpt-a", OldRatio: trendFloat(2), NewRatio: 1.5, ChangedAt: now.Add(-25 * time.Hour)},
		{ChannelID: 1, ModelName: "gpt-b", OldRatio: trendFloat(1), NewRatio: 1, OldCompletionRatio: trendFloat(1), NewCompletionRatio: 2, ChangedAt: now.Add(-2 * time.Hour)},
		{ChannelID: 1, ModelName: "gpt-d", OldRatio: trendFloat(1), NewRatio: 2, OldCompletionRatio: trendFloat(2), NewCompletionRatio: 1, ChangedAt: now.Add(-3 * time.Hour)},
		{ChannelID: 2, ModelName: "gpt-c", OldRatio: trendFloat(1), NewRatio: 3, ChangedAt: now.Add(-time.Hour)},
		{ChannelID: 1, ModelName: "expired", OldRatio: trendFloat(1), NewRatio: 3, ChangedAt: now.Add(-10 * 24 * time.Hour)},
	}
	for _, change := range changes {
		if err := rates.AppendChange(change); err != nil {
			t.Fatalf("append rate change: %v", err)
		}
	}

	got, err := rates.RateChangeAnalytics(1, now.Add(-7*24*time.Hour), 2)
	if err != nil {
		t.Fatalf("rate change analytics: %v", err)
	}
	if got.Total != 4 || got.Up != 2 || got.Down != 1 || got.Other != 1 {
		t.Fatalf("analytics totals = %+v", got)
	}
	if len(got.Recent) != 2 || len(got.Daily) != 2 || len(got.Models) != 3 {
		t.Fatalf("analytics collections = recent %d daily %d models %d", len(got.Recent), len(got.Daily), len(got.Models))
	}
	if got.Models[0].ModelName != "gpt-a" || got.Models[0].Changes != 2 {
		t.Fatalf("top model = %+v", got.Models[0])
	}
}

func TestRateChangeAnalyticsForDaysUsesShanghaiCalendarWindow(t *testing.T) {
	oldNow := trendNow
	trendNow = func() time.Time { return time.Date(2026, 7, 14, 12, 0, 0, 0, trendLocation) }
	t.Cleanup(func() { trendNow = oldNow })
	db := openTestDB(t)
	rates := NewRates(db)
	windowStart := time.Date(2026, 7, 8, 0, 0, 0, 0, trendLocation)
	for _, changedAt := range []time.Time{windowStart, windowStart.Add(-time.Second)} {
		if err := rates.AppendChange(&RateChangeLog{ChannelID: 1, ModelName: changedAt.String(), OldRatio: trendFloat(1), NewRatio: 2, ChangedAt: changedAt}); err != nil {
			t.Fatalf("append rate change: %v", err)
		}
	}
	got, err := rates.RateChangeAnalyticsForDays(1, 7, 20)
	if err != nil {
		t.Fatalf("rate change analytics: %v", err)
	}
	if got.Total != 1 || len(got.Recent) != 1 || !got.Recent[0].ChangedAt.Equal(windowStart) {
		t.Fatalf("calendar-window analytics = %+v", got)
	}
}

func TestAggregateTrendUsesShanghaiDayBoundary(t *testing.T) {
	oldNow := trendNow
	trendNow = func() time.Time {
		return time.Date(2026, 6, 19, 16, 30, 0, 0, time.UTC)
	}
	t.Cleanup(func() { trendNow = oldNow })

	db := openTestDB(t)
	rates := NewRates(db)

	day0 := time.Date(2026, 6, 20, 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)

	balanceSnapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: time.Date(2026, 6, 19, 15, 59, 0, 0, time.UTC)},
		{ChannelID: 1, Balance: 20, SampledAt: time.Date(2026, 6, 19, 16, 1, 0, 0, time.UTC)},
	}
	for _, snapshot := range balanceSnapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	costSnapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.5, SampledAt: time.Date(2026, 6, 19, 15, 59, 0, 0, time.UTC)},
		{ChannelID: 1, TodayCost: 2.5, SampledAt: time.Date(2026, 6, 19, 16, 1, 0, 0, time.UTC)},
	}
	for _, snapshot := range costSnapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	balances, err := rates.AggregateBalanceTrend(2)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(balances) != 2 {
		t.Fatalf("balance days = %d, want 2", len(balances))
	}
	if !balances[0].Day.Equal(day1) || balances[0].Balance == nil || *balances[0].Balance != 10 {
		t.Fatalf("previous shanghai day = %#v, want day %s balance 10", balances[0], day1)
	}
	if !balances[1].Day.Equal(day0) || balances[1].Balance == nil || *balances[1].Balance != 20 {
		t.Fatalf("current shanghai day = %#v, want day %s balance 20", balances[1], day0)
	}

	costs, err := rates.AggregateCostTrend(2)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(costs) != 2 {
		t.Fatalf("cost days = %d, want 2", len(costs))
	}
	if !costs[0].Day.Equal(day1) || costs[0].Cost == nil || *costs[0].Cost != 1.5 {
		t.Fatalf("previous shanghai day cost = %#v, want day %s cost 1.5", costs[0], day1)
	}
	if !costs[1].Day.Equal(day0) || costs[1].Cost == nil || *costs[1].Cost != 2.5 {
		t.Fatalf("current shanghai day cost = %#v, want day %s cost 2.5", costs[1], day0)
	}
}

func TestDeleteCostSnapshotsBefore(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now()
	oldSnapshot := CostSnapshot{ChannelID: 1, TodayCost: 1.2, SampledAt: now.AddDate(0, 0, -10)}
	newSnapshot := CostSnapshot{ChannelID: 1, TodayCost: 2.3, SampledAt: now.AddDate(0, 0, -2)}
	if err := rates.AppendCost(&oldSnapshot); err != nil {
		t.Fatalf("append old cost: %v", err)
	}
	if err := rates.AppendCost(&newSnapshot); err != nil {
		t.Fatalf("append new cost: %v", err)
	}

	deleted, err := rates.DeleteCostSnapshotsBefore(now.AddDate(0, 0, -5))
	if err != nil {
		t.Fatalf("delete cost snapshots: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	var count int64
	if err := db.Model(&CostSnapshot{}).Count(&count).Error; err != nil {
		t.Fatalf("count cost snapshots: %v", err)
	}
	if count != 1 {
		t.Fatalf("remaining count = %d, want 1", count)
	}
}

func TestTryClaimCooldown(t *testing.T) {
	db := openTestDB(t)
	notifications := NewNotifications(db)

	ok, err := notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !ok {
		t.Fatal("first claim should succeed")
	}

	ok, err = notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if ok {
		t.Fatal("second claim should be blocked by cooldown")
	}

	oldTime := time.Now().Add(-2 * time.Minute)
	if err := db.Model(&NotificationCooldown{}).
		Where("channel_id = ? AND event = ?", 1, EventBalanceLow).
		Updates(map[string]any{
			"last_sent_at": oldTime,
			"updated_at":   oldTime,
		}).Error; err != nil {
		t.Fatalf("age cooldown: %v", err)
	}

	ok, err = notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if !ok {
		t.Fatal("third claim should succeed after cooldown expires")
	}
}

func TestCooldownClaimReleaseRequiresExactToken(t *testing.T) {
	db := openTestDB(t)
	notifications := NewNotifications(db)
	ctx := context.Background()
	claimed, token, err := notifications.TryClaimCooldownContext(ctx, 11, EventSubscriptionDailyLow, time.Hour)
	if err != nil || !claimed || token.IsZero() {
		t.Fatalf("initial context claim = %t token=%v err=%v", claimed, token, err)
	}
	if err := notifications.ReleaseCooldownClaimContext(ctx, 11, EventSubscriptionDailyLow, token.Add(-time.Microsecond)); err != nil {
		t.Fatalf("release stale token: %v", err)
	}
	if claimed, _, err := notifications.TryClaimCooldownContext(ctx, 11, EventSubscriptionDailyLow, time.Hour); err != nil || claimed {
		t.Fatalf("stale release cleared current claim: claimed=%t err=%v", claimed, err)
	}
	if err := notifications.ReleaseCooldownClaimContext(ctx, 11, EventSubscriptionDailyLow, token); err != nil {
		t.Fatalf("release exact token: %v", err)
	}
	if claimed, nextToken, err := notifications.TryClaimCooldownContext(ctx, 11, EventSubscriptionDailyLow, time.Hour); err != nil || !claimed || nextToken.IsZero() {
		t.Fatalf("claim after exact release = %t token=%v err=%v", claimed, nextToken, err)
	}
}

func TestTryClaimCooldownConcurrent(t *testing.T) {
	db := openTestDB(t)
	notifications := NewNotifications(db)

	var claimed int32
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ok, err := notifications.TryClaimCooldown(2, EventBalanceLow, time.Minute)
			if err != nil {
				t.Errorf("concurrent claim: %v", err)
				return
			}
			if ok {
				atomic.AddInt32(&claimed, 1)
			}
		}()
	}
	wg.Wait()

	if claimed != 1 {
		t.Fatalf("expected exactly one successful claim, got %d", claimed)
	}
}

func TestUpstreamAnnouncementsSyncDedupes(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	items := []UpstreamAnnouncement{
		{SourceKey: "a", Title: "A", Content: "one", FirstSeenAt: now},
		{SourceKey: "a", Title: "A2", Content: "dup", FirstSeenAt: now.Add(time.Second)},
	}
	newItems, err := announcements.Sync(1, items)
	if err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if len(newItems) != 1 {
		t.Fatalf("new items = %d, want 1", len(newItems))
	}

	exists, err := announcements.Exists(1, "a")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("expected announcement to exist")
	}
}

func TestUpstreamAnnouncementsListLatest(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	publishedOld := now.Add(-3 * time.Hour)
	publishedNew := now.Add(-1 * time.Hour)
	items := []UpstreamAnnouncement{
		{ChannelID: 1, SourceKey: "a", Content: "body", PublishedAt: &publishedOld, FirstSeenAt: now.Add(3 * time.Minute)},
		{ChannelID: 1, SourceKey: "b", Content: "body", PublishedAt: &publishedNew, FirstSeenAt: now.Add(1 * time.Minute)},
		{ChannelID: 1, SourceKey: "c", Content: "body", FirstSeenAt: now.Add(4 * time.Minute)},
	}
	for _, item := range items {
		item := item
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("create announcement: %v", err)
		}
	}

	list, err := announcements.ListLatest(2)
	if err != nil {
		t.Fatalf("list latest: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("list len = %d, want 2", len(list))
	}
	if list[0].SourceKey != "b" || list[1].SourceKey != "a" {
		t.Fatalf("unexpected order: %#v", list)
	}
}

func TestUpstreamAnnouncementsDeleteByChannel(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "a",
		Content:     "one",
		FirstSeenAt: now,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if _, err := announcements.Sync(2, []UpstreamAnnouncement{{
		SourceKey:   "b",
		Content:     "two",
		FirstSeenAt: now,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}

	rows, err := announcements.DeleteByChannel(1)
	if err != nil {
		t.Fatalf("delete by channel: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].ChannelID != 2 {
		t.Fatalf("unexpected remaining announcements: total=%d list=%#v", total, list)
	}
}

func TestUpstreamAnnouncementsDeleteBefore(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	oldTime := time.Now().AddDate(0, 0, -10)
	newTime := time.Now()
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "old",
		Content:     "old",
		FirstSeenAt: oldTime,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "new",
		Content:     "new",
		FirstSeenAt: newTime,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}

	rows, err := announcements.DeleteBefore(time.Now().AddDate(0, 0, -5))
	if err != nil {
		t.Fatalf("delete before: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].SourceKey != "new" {
		t.Fatalf("unexpected remaining announcements: total=%d list=%#v", total, list)
	}
}

func TestUpdateCosts(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)

	c := &Channel{
		Name:           "test",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(c); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	sampledAt := time.Now().UTC().Truncate(time.Second)
	if err := channels.UpdateCosts(c.ID, 1.23, 9.87, sampledAt); err != nil {
		t.Fatalf("update costs: %v", err)
	}

	got, err := channels.FindByID(c.ID)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if got.TodayCost == nil || *got.TodayCost != 1.23 {
		t.Fatalf("today cost mismatch: %#v", got.TodayCost)
	}
	if got.TodayCostAt == nil || !got.TodayCostAt.Equal(sampledAt) {
		t.Fatalf("today cost sampled at mismatch: %#v", got.TodayCostAt)
	}
	if got.TotalCost == nil || *got.TotalCost != 9.87 {
		t.Fatalf("total cost mismatch: %#v", got.TotalCost)
	}
}

func TestChannelDeletePrepareBlocksNameReuseUntilRemoteCleanupCompletes(t *testing.T) {
	db := openTestDB(t)

	channels := NewChannels(db)
	ch := &Channel{
		Name:           "demo",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := channels.Delete(ch.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion: %v", err)
	}
	ch = &Channel{
		Name:           "demo",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err == nil {
		t.Fatal("pending channel name was reused before remote cleanup completed")
	}

	captchas := NewCaptchas(db)
	cfg := &CaptchaConfig{
		Name:         "solver",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("create captcha: %v", err)
	}
	if err := captchas.Delete(cfg.ID); err != nil {
		t.Fatalf("delete captcha: %v", err)
	}
	cfg = &CaptchaConfig{
		Name:         "solver",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("recreate captcha: %v", err)
	}

	notifications := NewNotifications(db)
	notify := &NotificationChannel{
		Name:         "telegram",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
	}
	if err := notifications.CreateChannel(notify); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}
	if err := notifications.DeleteChannel(notify.ID); err != nil {
		t.Fatalf("delete notification channel: %v", err)
	}
	notify = &NotificationChannel{
		Name:         "telegram",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
	}
	if err := notifications.CreateChannel(notify); err != nil {
		t.Fatalf("recreate notification channel: %v", err)
	}
}

func TestChannelDeletePrepareRetainsScopedState(t *testing.T) {
	db := openTestDB(t)

	channels := NewChannels(db)
	ch := &Channel{
		Name:           "demo",
		Type:           ChannelTypeSub2API,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	now := time.Now()
	if err := db.Create(&AuthSession{ChannelID: ch.ID}).Error; err != nil {
		t.Fatalf("create auth session: %v", err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: ch.ID, ModelName: "old", Ratio: 1, LastSeenAt: now}).Error; err != nil {
		t.Fatalf("create rate snapshot: %v", err)
	}
	if err := db.Create(&RateChangeLog{ChannelID: ch.ID, ModelName: "old", NewRatio: 1, ChangedAt: now}).Error; err != nil {
		t.Fatalf("create rate change: %v", err)
	}
	if err := db.Create(&BalanceSnapshot{ChannelID: ch.ID, Balance: 1, SampledAt: now}).Error; err != nil {
		t.Fatalf("create balance snapshot: %v", err)
	}
	if err := db.Create(&CostSnapshot{ChannelID: ch.ID, TodayCost: 1, SampledAt: now}).Error; err != nil {
		t.Fatalf("create cost snapshot: %v", err)
	}
	if err := db.Create(&MonitorLog{ChannelID: ch.ID, Job: MonitorJobBalance, Success: true, StartedAt: now, FinishedAt: now}).Error; err != nil {
		t.Fatalf("create monitor log: %v", err)
	}
	if err := db.Create(&NotificationCooldown{ChannelID: ch.ID, Event: EventBalanceLow, LastSentAt: now}).Error; err != nil {
		t.Fatalf("create cooldown: %v", err)
	}
	if err := db.Create(&NotificationLog{ChannelID: 99, UpstreamChannelID: ch.ID, Event: EventBalanceLow, Subject: "alert", Success: true, SentAt: now}).Error; err != nil {
		t.Fatalf("create notification log: %v", err)
	}
	if err := db.Create(&NotificationLog{ChannelID: 99, Event: EventBalanceLow, Subject: "demo 余额低于阈值", Success: true, SentAt: now}).Error; err != nil {
		t.Fatalf("create legacy notification log: %v", err)
	}
	if err := db.Create(&UpstreamAnnouncement{ChannelID: ch.ID, SourceKey: "a", Content: "deleted", FirstSeenAt: now}).Error; err != nil {
		t.Fatalf("create announcement: %v", err)
	}

	if err := channels.Delete(ch.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion: %v", err)
	}

	for _, tt := range []struct {
		name  string
		model any
	}{
		{"auth_sessions", &AuthSession{}},
		{"rate_change_logs", &RateChangeLog{}},
		{"balance_snapshots", &BalanceSnapshot{}},
		{"cost_snapshots", &CostSnapshot{}},
		{"monitor_logs", &MonitorLog{}},
		{"notification_cooldowns", &NotificationCooldown{}},
		{"upstream_announcements", &UpstreamAnnouncement{}},
		{"notification_logs", &NotificationLog{}},
	} {
		var count int64
		q := db.Model(tt.model).Where("channel_id = ?", ch.ID)
		if tt.name == "notification_logs" {
			q = db.Model(tt.model).Where("upstream_channel_id = ? OR subject LIKE ?", ch.ID, "%"+ch.Name+"%")
		}
		if err := q.Count(&count).Error; err != nil {
			t.Fatalf("count %s: %v", tt.name, err)
		}
		if count == 0 {
			t.Fatalf("delete prepare removed %s", tt.name)
		}
	}
	var rateCount, presentGroupCount int64
	if err := db.Model(&RateSnapshot{}).Where("channel_id = ?", ch.ID).Count(&rateCount).Error; err != nil {
		t.Fatalf("count rate snapshots: %v", err)
	}
	if err := db.Model(&GatewayUpstreamGroup{}).
		Where("channel_id = ? AND present = ?", ch.ID, true).
		Count(&presentGroupCount).Error; err != nil {
		t.Fatalf("count present upstream groups: %v", err)
	}
	if rateCount != 0 || presentGroupCount != 0 {
		t.Fatalf("delete prepare retained current presence: rates=%d present_groups=%d", rateCount, presentGroupCount)
	}
}

func seedGatewayModelSyncGeneration(t *testing.T, db *gorm.DB, store *Gateway, groupName string, at time.Time) *Channel {
	t.Helper()
	channel := &Channel{
		Name:           "model sync fixture",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://model-sync-fixture.example",
		MonitorEnabled: true,
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatalf("create model sync channel: %v", err)
	}
	if _, err := NewRates(db).Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: groupName, Ratio: 1, LastSeenAt: at,
	}); err != nil {
		t.Fatalf("create model sync rate: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, groupName, "added", 1, "managed", at); err != nil {
		t.Fatalf("record model sync generation: %v", err)
	}
	return channel
}

func TestGatewayUpstreamKeyKeepsFamiliesOnErrorAndDisable(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	channel := seedGatewayModelSyncGeneration(t, db, store, "group", now)
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID:    channel.ID,
		APIKeyCipher: "cipher",
		APIKeyPrefix: "sk-test",
		GroupName:    "group",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "group", "codex,gemini", "gpt-5.4,gemini-2.5-pro", now, ""); err != nil {
		t.Fatalf("write generation-bound model families: %v", err)
	}
	if err := store.SetUpstreamKeyModelsError(channel.ID, "group", now.Add(time.Minute), "temporary failure"); err != nil {
		t.Fatalf("set models error: %v", err)
	}
	if err := store.RecordUpstreamKeyModelsError(channel.ID, "group", now.Add(2*time.Minute), "temporary failure"); err != nil {
		t.Fatalf("record existing models error: %v", err)
	}
	if err := store.DisableUpstreamKey(channel.ID, "group", "invalidated"); err != nil {
		t.Fatalf("disable upstream key: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "group")
	if err != nil {
		t.Fatalf("find upstream key: %v", err)
	}
	if key.ModelFamilies != "codex,gemini" {
		t.Fatalf("model families = %q, want codex,gemini", key.ModelFamilies)
	}
	if key.Enabled {
		t.Fatal("key should be disabled")
	}
	if key.APIKeyCipher != "" || key.APIKeyPrefix != "" {
		t.Fatalf("key material not cleared: cipher=%q prefix=%q", key.APIKeyCipher, key.APIKeyPrefix)
	}
}

func TestGatewayUpstreamProbeChoiceSurvivesUpsertAndProvisioning(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	primary := &Channel{Name: "probe choice primary", Type: ChannelTypeNewAPI, SiteURL: "https://probe-choice-primary.example", MonitorEnabled: true}
	if err := channels.Create(primary); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: primary.ID, GroupName: "group", APIKeyCipher: "cipher", APIKeyPrefix: "sk", Enabled: true,
	}); err != nil {
		t.Fatalf("create upstream key: %v", err)
	}
	if err := store.UpdateLatencyProbeEnabled(primary.ID, "group", false); err != nil {
		t.Fatalf("disable group probe: %v", err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: primary.ID, GroupName: "group", APIKeyCipher: "replacement", APIKeyPrefix: "sk-new", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert replacement key: %v", err)
	}
	stored, err := store.FindUpstreamKeyAny(primary.ID, "group")
	if err != nil || stored.LatencyProbeEnabled {
		t.Fatalf("probe choice after upsert = %+v err=%v, want false", stored, err)
	}
	if err := store.DisableUpstreamKey(primary.ID, "group", "reprovision"); err != nil {
		t.Fatalf("disable upstream key: %v", err)
	}
	pending, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(primary.ID, "group", "managed-key", 1, time.Now(), time.Minute)
	if err != nil {
		t.Fatalf("claim provisioning: %v", err)
	}
	// A disabled row with only a previous error has no durable evidence that a
	// remote key was ever created. It must be claimable so synchronization can
	// recover a first key instead of leaving an empty group permanently stuck.
	if pending == nil || !claimed || !fresh || pending.LatencyProbeEnabled || !pending.Provisioning || pending.LastError != "" {
		t.Fatalf("error-only placeholder was not safely claimed: state=%+v claimed=%t fresh=%t", pending, claimed, fresh)
	}
	preProvision := &Channel{Name: "probe choice pre-provision", Type: ChannelTypeNewAPI, SiteURL: "https://probe-choice-pre-provision.example", MonitorEnabled: true}
	if err := channels.Create(preProvision); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: preProvision.ID, ModelName: "pre-provision", Ratio: 1}).Error; err != nil {
		t.Fatalf("create pre-provision rate: %v", err)
	}
	if err := store.UpdateLatencyProbeEnabled(preProvision.ID, "pre-provision", false); err != nil {
		t.Fatalf("save pre-provision probe choice: %v", err)
	}
	placeholder, err := store.FindUpstreamKeyAny(preProvision.ID, "pre-provision")
	if err != nil || placeholder.LatencyProbeEnabled || placeholder.Enabled || placeholder.APIKeyCipher != "" {
		t.Fatalf("pre-provision placeholder = %+v err=%v", placeholder, err)
	}
	pending, claimed, fresh, err = store.ClaimUpstreamKeyProvisioning(preProvision.ID, "pre-provision", "managed-key-2", 1, time.Now(), time.Minute)
	if err != nil || pending == nil || !claimed || !fresh || pending.LatencyProbeEnabled {
		t.Fatalf("blank placeholder provisioning = %+v claimed=%t fresh=%t err=%v", pending, claimed, fresh, err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: preProvision.ID, GroupName: "pre-provision", APIKeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatalf("provision placeholder: %v", err)
	}
	placeholder, err = store.FindUpstreamKeyAny(preProvision.ID, "pre-provision")
	if err != nil || placeholder.LatencyProbeEnabled || !placeholder.Enabled || placeholder.APIKeyCipher != "cipher" {
		t.Fatalf("probe choice after placeholder provisioning = %+v err=%v", placeholder, err)
	}
	if err := store.UpdateLatencyProbeEnabled(primary.ID, "missing", false); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("missing group update error = %v, want record not found", err)
	}
}

func TestGatewayProvisioningClaimDoesNotEraseConcurrentRecoveryEvidence(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channel := &Channel{Name: "provisioning recovery evidence", Type: ChannelTypeNewAPI, SiteURL: "https://provisioning-recovery-evidence.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now().Add(-time.Minute)
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 73,
		UpstreamKeyName: "BOG-p-0123456789abcdef", ProvisioningStartedAt: &startedAt,
		Enabled: false, LastError: "created remotely; reveal pending",
	}); err != nil {
		t.Fatal(err)
	}

	pending, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "promo", "BOG-p-fedcba9876543210", 2, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if pending == nil || claimed || fresh {
		t.Fatalf("evidenced claim = %+v claimed=%t fresh=%t", pending, claimed, fresh)
	}
	if pending.UpstreamKeyID != 73 || pending.UpstreamKeyName != "BOG-p-0123456789abcdef" || pending.LastError == "" || pending.GroupRatio != 0 {
		t.Fatalf("concurrent recovery evidence changed: %+v", pending)
	}
}

func TestGatewayUpstreamModelErrorHeartbeatUsesRetryInterval(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	first := time.Date(2026, 7, 18, 8, 0, 0, 0, time.UTC)
	channel := seedGatewayModelSyncGeneration(t, db, store, "group", first)
	if err := store.RecordUpstreamKeyModelsError(channel.ID, "group", first, "rate limited"); err != nil {
		t.Fatalf("record first model error: %v", err)
	}
	if err := store.RecordUpstreamKeyModelsError(channel.ID, "group", first.Add(4*time.Minute), "rate limited"); err != nil {
		t.Fatalf("record early model error: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "group")
	if err != nil {
		t.Fatalf("find early model error: %v", err)
	}
	if key.ModelsSyncedAt == nil || !key.ModelsSyncedAt.Equal(first) {
		t.Fatalf("early retry advanced heartbeat: %+v", key.ModelsSyncedAt)
	}

	retryAt := first.Add(6 * time.Minute)
	if err := store.RecordUpstreamKeyModelsError(channel.ID, "group", retryAt, "rate limited"); err != nil {
		t.Fatalf("record due model error: %v", err)
	}
	key, err = store.FindUpstreamKeyAny(channel.ID, "group")
	if err != nil {
		t.Fatalf("find due model error: %v", err)
	}
	if key.ModelsSyncedAt == nil || !key.ModelsSyncedAt.Equal(retryAt) {
		t.Fatalf("due retry did not advance heartbeat: %+v", key.ModelsSyncedAt)
	}
}

func TestGatewayListsUpstreamKeysInBatchAndRejectsMissingProbeModelUpdate(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	createdChannels := make([]*Channel, 3)
	for index := range createdChannels {
		createdChannels[index] = &Channel{
			Name: "batch upstream key channel " + strconv.Itoa(index+1), Type: ChannelTypeNewAPI,
			SiteURL: "https://batch-upstream-key-" + strconv.Itoa(index+1) + ".example", MonitorEnabled: true,
		}
		if err := channels.Create(createdChannels[index]); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []*GatewayUpstreamKey{
		{ChannelID: createdChannels[0].ID, GroupName: "beta", APIKeyCipher: "cipher", Enabled: true},
		{ChannelID: createdChannels[0].ID, GroupName: "alpha", APIKeyCipher: "cipher", Enabled: true},
		{ChannelID: createdChannels[1].ID, GroupName: "disabled", APIKeyCipher: "cipher", Enabled: true},
		{ChannelID: createdChannels[2].ID, GroupName: "excluded", APIKeyCipher: "cipher", Enabled: true},
	} {
		if err := store.UpsertUpstreamKey(key); err != nil {
			t.Fatalf("upsert key %d/%s: %v", key.ChannelID, key.GroupName, err)
		}
	}
	if err := store.DisableUpstreamKey(createdChannels[1].ID, "disabled", "disabled"); err != nil {
		t.Fatalf("disable key: %v", err)
	}

	list, err := store.ListUpstreamKeys([]uint{createdChannels[1].ID, createdChannels[0].ID})
	if err != nil {
		t.Fatalf("list upstream keys: %v", err)
	}
	if len(list) != 3 || list[0].ChannelID != createdChannels[0].ID || list[0].GroupName != "alpha" || list[1].GroupName != "beta" || list[2].ChannelID != createdChannels[1].ID || list[2].Enabled {
		t.Fatalf("batched upstream keys = %+v", list)
	}
	empty, err := store.ListUpstreamKeys(nil)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty upstream keys = %#v err=%v", empty, err)
	}
	if err := store.UpdateLatencyProbeModel(createdChannels[0].ID, "alpha", "gpt-mini"); err != nil {
		t.Fatalf("update existing probe model: %v", err)
	}
	if err := store.UpdateLatencyProbeModel(createdChannels[0].ID, "alpha", "gpt-mini"); err != nil {
		t.Fatalf("idempotent probe model update: %v", err)
	}
	stored, err := store.FindUpstreamKeyAny(createdChannels[0].ID, "alpha")
	if err != nil || stored.LatencyProbeModel != "gpt-mini" {
		t.Fatalf("stored probe model = %+v err=%v", stored, err)
	}
	if err := store.UpdateLatencyProbeModel(99, "missing", "gpt-mini"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("missing update error = %v, want record not found", err)
	}
}

func TestGatewayUpstreamKeyRoutingFailureHasDedicatedTimestampAndClearsOnSuccess(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	base := time.Now().UTC().Truncate(time.Microsecond)
	channel := seedGatewayModelSyncGeneration(t, db, store, "group", base)
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "group", APIKeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatalf("upsert key: %v", err)
	}
	failureAt := base.Add(time.Minute)
	if err := store.RecordUpstreamKeyRoutingFailure(channel.ID, "group", "http_status", 503, "gateway status 503", failureAt); err != nil {
		t.Fatalf("record routing failure: %v", err)
	}
	if err := store.SetUpstreamKeyError(channel.ID, "group", "key provisioning failed"); err != nil {
		t.Fatalf("set key error: %v", err)
	}
	if err := store.SetUpstreamKeyModelsError(channel.ID, "group", failureAt.Add(time.Minute), "model refresh failed"); err != nil {
		t.Fatalf("unrelated model update: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "group")
	if err != nil {
		t.Fatalf("find failed key: %v", err)
	}
	if key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(failureAt) || key.LastRoutingFailureKind != "http_status" || key.LastRoutingFailureStatus != 503 {
		t.Fatalf("routing failure changed after unrelated update: %+v", key)
	}
	if key.LastRoutingFailureError != "gateway status 503" || key.LastError != "key provisioning failed" {
		t.Fatalf("routing failure polluted key error: %+v", key)
	}
	if err := store.TouchUpstreamKey(channel.ID, "group", failureAt.Add(2*time.Minute)); err != nil {
		t.Fatalf("touch recovered key: %v", err)
	}
	key, err = store.FindUpstreamKeyAny(channel.ID, "group")
	if err != nil {
		t.Fatalf("find recovered key: %v", err)
	}
	if key.LastError != "key provisioning failed" || key.LastRoutingFailureError != "" || key.LastRoutingFailureAt != nil || key.LastRoutingFailureKind != "" || key.LastRoutingFailureStatus != 0 {
		t.Fatalf("success did not isolate key and routing state: %+v", key)
	}
}

func TestGatewayRoutingFailureUsesSlidingWindowIndefiniteIsolation(t *testing.T) {
	store, generation := routingEnableFixture(t, "group")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 8; i++ {
		at := base.Add(time.Duration(i) * time.Second)
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "transport", 0, "failed", at); err != nil {
			t.Fatalf("record failure %d: %v", i+1, err)
		}
		key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
		if err != nil || key.RoutingCooldownUntil != nil {
			t.Fatalf("failure %d key = %+v err=%v", i+1, key, err)
		}
		if key.RoutingFailureCount != i+1 {
			t.Fatalf("failure %d state = count %d, want %d", i+1, key.RoutingFailureCount, i+1)
		}
		if (key.RoutingIsolatedAt != nil) != (i+1 >= GatewayAdaptiveRoutingFailureThreshold) {
			t.Fatalf("failure %d isolation latch = %v", i+1, key.RoutingIsolatedAt)
		}
	}
	if err := store.TouchUpstreamKeyForRoutingHealth(fence, base.Add(8*time.Second)); err != nil {
		t.Fatalf("record in-flight real success: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingFailureCount != 8 || key.LastRoutingFailureAt == nil || key.RoutingCooldownUntil != nil || key.RoutingIsolatedAt == nil {
		t.Fatalf("real success released an isolated route: key=%+v err=%v", key, err)
	}
	if err := store.TouchUpstreamKeyForRoutingHealth(fence, base.Add(8*time.Second)); err != nil {
		t.Fatalf("replay in-flight real success: %v", err)
	}
	key, err = store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingFailureCount != 8 || key.LastRoutingFailureAt == nil || key.RoutingCooldownUntil != nil || key.RoutingIsolatedAt == nil {
		t.Fatalf("replayed real success released an isolated route: key=%+v err=%v", key, err)
	}
	var retainedFailureFacts int64
	if err := store.db.Model(&GatewayRoutingFailure{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", generation.ChannelID, generation.CanonicalGroupName).
		Count(&retainedFailureFacts).Error; err != nil || retainedFailureFacts != 8 {
		t.Fatalf("replayed real success removed isolation facts: count=%d err=%v", retainedFailureFacts, err)
	}
	probeRecoveryAt := base.Add(9 * time.Second)
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(fence, GatewayAppCodex, probeRecoveryAt); err != nil {
		t.Fatalf("recover isolated key from confirmed probes: %v", err)
	}
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "transport", 0, "failed again", base.Add(10*time.Second)); err != nil {
		t.Fatalf("record post-recovery failure: %v", err)
	}
	key, err = store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingFailureCount != 1 || key.RoutingCooldownUntil != nil || key.RoutingIsolatedAt != nil {
		t.Fatalf("post-recovery sliding state = %+v err=%v", key, err)
	}

	outsideWindow := base.Add(11 * time.Minute)
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "transport", 0, "new window", outsideWindow); err != nil {
		t.Fatalf("record new-window failure: %v", err)
	}
	key, err = store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingFailureCount != 1 || key.RoutingCooldownUntil != nil || key.RoutingIsolatedAt != nil {
		t.Fatalf("new window state = %+v err=%v", key, err)
	}

	batchAt := outsideWindow.Add(11 * time.Minute)
	if err := store.RecordUpstreamKeyRoutingFailuresForRoutingHealth(
		fence,
		"transport", 0, "confirmed batch", batchAt, 3,
	); err != nil {
		t.Fatalf("record confirmed routing failure batch: %v", err)
	}
	key, err = store.FindUpstreamKey(generation.ChannelID, "group")
	if err != nil || key.RoutingFailureCount != 3 || key.RoutingCooldownUntil != nil || key.RoutingIsolatedAt == nil {
		t.Fatalf("confirmed batch state = %+v err=%v", key, err)
	}
	if err := store.RecordUpstreamKeyRoutingFailuresForRoutingHealth(
		fence,
		"transport", 0, "invalid batch", batchAt.Add(time.Second), 0,
	); err == nil {
		t.Fatal("zero-sized routing failure batch must be rejected")
	}
	unchanged, err := store.FindUpstreamKey(generation.ChannelID, "group")
	if err != nil || unchanged.RoutingFailureCount != key.RoutingFailureCount || unchanged.RoutingCooldownUntil != nil {
		t.Fatalf("invalid batch changed routing health: before=%+v after=%+v err=%v", key, unchanged, err)
	}
}

func TestGatewayRoutingIsolationSurvivesFailureWindowExpiry(t *testing.T) {
	store, generation := routingEnableFixture(t, "isolation-window-expiry")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	for attempt := 0; attempt < GatewayAdaptiveRoutingFailureThreshold; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(
			fence, "transport", 0, "failed", base.Add(time.Duration(attempt)*time.Second),
		); err != nil {
			t.Fatal(err)
		}
	}
	lateFailureAt := base.Add(11 * time.Minute)
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(
		fence, "transport", 0, "failed later", lateFailureAt,
	); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingIsolatedAt == nil || key.RoutingFailureCount < GatewayAdaptiveRoutingFailureThreshold ||
		key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(lateFailureAt) {
		t.Fatalf("expired failure window released isolation: key=%+v err=%v", key, err)
	}
}

func TestGatewayRoutingIsolationSurvivesSuccessThenNewFailures(t *testing.T) {
	store, generation := routingEnableFixture(t, "isolation-success-failure-interleave")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	for attempt := 0; attempt < 8; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(
			fence, "transport", 0, "failed", base.Add(time.Duration(attempt)*time.Second),
		); err != nil {
			t.Fatal(err)
		}
	}
	successAt := base.Add(8 * time.Second)
	if err := store.TouchUpstreamKeyForRoutingHealth(fence, successAt); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(
			fence, "transport", 0, "failed after success", successAt.Add(time.Duration(attempt)*time.Second),
		); err != nil {
			t.Fatal(err)
		}
	}
	key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingIsolatedAt == nil || key.RoutingFailureCount < GatewayAdaptiveRoutingFailureThreshold {
		t.Fatalf("success/failure interleave released isolation: key=%+v err=%v", key, err)
	}
}

func TestGatewayRoutingIsolationUsesThirdFailureEventTime(t *testing.T) {
	store, generation := routingEnableFixture(t, "isolation-third-failure-time")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	for _, offset := range []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 5 * time.Second} {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(
			fence, "transport", 0, "failed", base.Add(offset),
		); err != nil {
			t.Fatal(err)
		}
	}
	key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	thirdFailureAt := base.Add(3 * time.Second)
	if err != nil || key.RoutingIsolatedAt == nil || !key.RoutingIsolatedAt.Equal(thirdFailureAt) {
		t.Fatalf("isolation threshold = %+v err=%v, want %s", key, err, thirdFailureAt)
	}

	// This success completed after the third failure but reached storage after
	// the fourth. It must not be treated as a pre-isolation success.
	if err := store.TouchUpstreamKeyForRoutingHealth(fence, base.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	key, err = store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingIsolatedAt == nil || !key.RoutingIsolatedAt.Equal(thirdFailureAt) ||
		key.RoutingFailureCount != 4 {
		t.Fatalf("late in-flight success released isolation: key=%+v err=%v", key, err)
	}
}

func TestGatewayRoutingSuccessWatermarkRejectsLateOlderFailure(t *testing.T) {
	store, generation := routingEnableFixture(t, "primary")
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "before success", base, generation); err != nil {
		t.Fatal(err)
	}
	successAt := base.Add(time.Second)
	if err := store.TouchUpstreamKey(generation.ChannelID, generation.CanonicalGroupName, successAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "equal-time late write", successAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "late older write", base.Add(500*time.Millisecond), generation); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastUsedAt == nil || !key.LastUsedAt.Equal(successAt) || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("late older failure crossed success watermark: key=%+v err=%v", key, err)
	}
	var failures int64
	if err := store.db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND group_name = ?", generation.ChannelID, generation.CanonicalGroupName).Count(&failures).Error; err != nil || failures != 0 {
		t.Fatalf("late older failure facts = %d err=%v", failures, err)
	}
	newFailureAt := successAt.Add(time.Second)
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "after success", newFailureAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err = store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(newFailureAt) || key.RoutingFailureCount != 1 || key.RoutingCooldownUntil != nil {
		t.Fatalf("new failure after success = %+v err=%v", key, err)
	}
}

func TestGatewayRoutingDelayedSuccessPreservesNewerFailure(t *testing.T) {
	store, generation := routingEnableFixture(t, "primary")
	base := time.Now().UTC().Truncate(time.Microsecond)
	oldFailureAt := base
	successAt := base.Add(time.Second)
	newFailureAt := base.Add(2 * time.Second)
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "old failure", oldFailureAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "http_status", 503, "newer failure", newFailureAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.TouchUpstreamKey(generation.ChannelID, generation.CanonicalGroupName, successAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastUsedAt == nil || !key.LastUsedAt.Equal(successAt) || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(newFailureAt) || key.LastRoutingFailureKind != "http_status" || key.LastRoutingFailureStatus != 503 || key.LastRoutingFailureError != "newer failure" || key.RoutingFailureCount != 1 || key.RoutingCooldownUntil != nil {
		t.Fatalf("delayed success lost newer failure: key=%+v err=%v", key, err)
	}
	var failures []GatewayRoutingFailure
	if err := store.db.Where("channel_id = ? AND group_name = ?", generation.ChannelID, generation.CanonicalGroupName).Order("occurred_at ASC").Find(&failures).Error; err != nil {
		t.Fatal(err)
	}
	if len(failures) != 1 || !failures[0].OccurredAt.Equal(newFailureAt) {
		t.Fatalf("remaining post-success failures = %+v", failures)
	}
}

func TestGatewayRoutingDelayedSuccessBeforeThirdFailureStartsNewEpoch(t *testing.T) {
	store, generation := routingEnableFixture(t, "delayed-before-threshold")
	base := time.Now().UTC().Truncate(time.Microsecond)
	for index, at := range []time.Time{base, base.Add(time.Second), base.Add(3 * time.Second)} {
		if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "failure", at, generation); err != nil {
			t.Fatalf("record failure %d: %v", index+1, err)
		}
	}

	// The success event happened between failure two and failure three, but its
	// writer reached the canonical lock after all three failures were visible.
	successAt := base.Add(2 * time.Second)
	if err := store.TouchUpstreamKey(generation.ChannelID, generation.CanonicalGroupName, successAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	thirdFailureAt := base.Add(3 * time.Second)
	if err != nil || key.LastUsedAt == nil || !key.LastUsedAt.Equal(successAt) ||
		key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(thirdFailureAt) ||
		key.RoutingFailureCount != 1 || key.RoutingCooldownUntil != nil || key.RoutingIsolatedAt != nil {
		t.Fatalf("event-time success did not start a new failure epoch: key=%+v err=%v", key, err)
	}
}

func TestGatewayRoutingSuccessClearsCanonicalGroupOnly(t *testing.T) {
	store, primaryGeneration := routingEnableFixture(t, "Primary")
	secondaryAt := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := NewRates(store.db).Upsert(&RateSnapshot{ChannelID: primaryGeneration.ChannelID, ModelName: "secondary", Ratio: 1, LastSeenAt: secondaryAt}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: primaryGeneration.ChannelID, GroupName: "secondary", APIKeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(primaryGeneration.ChannelID, "secondary", "added", 1, "secondary key", secondaryAt); err != nil {
		t.Fatal(err)
	}
	secondaryGeneration, err := store.CaptureUpstreamGroupGeneration(primaryGeneration.ChannelID, "secondary")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	for attempt := 0; attempt < 2; attempt++ {
		at := base.Add(time.Duration(attempt) * time.Second)
		if err := store.RecordUpstreamKeyRoutingFailure(primaryGeneration.ChannelID, "Primary", "transport", 0, "failed", at, primaryGeneration); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamKeyRoutingFailure(secondaryGeneration.ChannelID, "secondary", "transport", 0, "failed", at, secondaryGeneration); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.TouchUpstreamKey(primaryGeneration.ChannelID, " primary ", base.Add(2*time.Second), primaryGeneration); err != nil {
		t.Fatal(err)
	}
	primary, err := store.FindUpstreamKeyAny(primaryGeneration.ChannelID, "Primary")
	if err != nil || primary.LastRoutingFailureAt != nil || primary.RoutingFailureCount != 0 || primary.RoutingCooldownUntil != nil {
		t.Fatalf("canonical primary state = %+v err=%v", primary, err)
	}
	secondary, err := store.FindUpstreamKeyAny(secondaryGeneration.ChannelID, "secondary")
	if err != nil || secondary.LastRoutingFailureAt == nil || secondary.RoutingFailureCount != 2 {
		t.Fatalf("isolated secondary state = %+v err=%v", secondary, err)
	}
	var primaryFacts, secondaryFacts int64
	if err := store.db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", primaryGeneration.ChannelID, "primary").Count(&primaryFacts).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", secondaryGeneration.ChannelID, "secondary").Count(&secondaryFacts).Error; err != nil {
		t.Fatal(err)
	}
	if primaryFacts != 0 || secondaryFacts != 2 {
		t.Fatalf("group-isolated failure facts primary=%d secondary=%d", primaryFacts, secondaryFacts)
	}
}

func TestUpstreamKeyHealthWritesRejectStaleGroupGeneration(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "stale generation health", Type: ChannelTypeNewAPI, SiteURL: "https://stale-generation-health.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "old-cipher", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "old key", now); err != nil {
		t.Fatal(err)
	}
	staleGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 1, "old key", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteGroupRoutingState(channel.ID, "primary"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 2, "new key", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	failureAt := now.Add(3 * time.Second)
	cooldownUntil := failureAt.Add(2 * time.Minute)
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "new-cipher", APIKeyPrefix: "new",
		Enabled: true, LastRoutingFailureError: "new generation failure", LastRoutingFailureKind: "transport",
		LastRoutingFailureStatus: 503, LastRoutingFailureAt: &failureAt, RoutingFailureCount: 1, RoutingCooldownUntil: &cooldownUntil,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayRoutingFailure{ChannelID: channel.ID, GroupName: "primary", OccurredAt: failureAt}).Error; err != nil {
		t.Fatal(err)
	}

	if err := store.TouchUpstreamKey(channel.ID, "primary", now.Add(4*time.Second), staleGeneration); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale touch error = %v", err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(channel.ID, "primary", "http_status", 503, "stale failure", now.Add(4*time.Second), staleGeneration); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale routing failure error = %v", err)
	}
	if err := store.DisableUpstreamKey(channel.ID, "primary", "stale invalidation", staleGeneration); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale invalidation error = %v", err)
	}
	if err := store.SetUpstreamKeyModelsError(channel.ID, "primary", now.Add(4*time.Second), "stale model sync", staleGeneration); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale model sync error = %v", err)
	}

	current, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if !current.Enabled || current.APIKeyCipher != "new-cipher" || current.LastUsedAt != nil || current.ModelsError != "" {
		t.Fatalf("stale health writes changed new key: %+v", current)
	}
	if current.LastRoutingFailureError != "new generation failure" || current.LastRoutingFailureKind != "transport" || current.LastRoutingFailureStatus != 503 || current.LastRoutingFailureAt == nil || !current.LastRoutingFailureAt.Equal(failureAt) || current.RoutingFailureCount != 1 || current.RoutingCooldownUntil == nil || !current.RoutingCooldownUntil.Equal(cooldownUntil) {
		t.Fatalf("stale health writes changed new routing state: %+v", current)
	}
	var failures int64
	if err := db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND group_name = ?", channel.ID, "primary").Count(&failures).Error; err != nil {
		t.Fatal(err)
	}
	if failures != 1 {
		t.Fatalf("stale health writes changed current generation failure facts: %d", failures)
	}
}

func TestGatewayRoutingFailureDoesNotMoveCooldownBackwardsForLateOlderEvent(t *testing.T) {
	store, generation := routingEnableFixture(t, "reverse")
	base := time.Now().UTC().Truncate(time.Second)
	latest := base.Add(time.Minute)
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "http_status", 503, "latest", latest, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "older", base, generation); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKeyAny(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil {
		t.Fatal(err)
	}
	if key.RoutingFailureCount != 2 || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(latest) || key.RoutingCooldownUntil != nil || key.LastRoutingFailureStatus != 503 {
		t.Fatalf("reverse-time state = %+v", key)
	}
}

func TestDeleteGroupRoutingStatePreservesRequestHistory(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "delete routing state", Type: ChannelTypeNewAPI, SiteURL: "https://delete-routing-state.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "removed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "removed", APIKeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatalf("upsert key: %v", err)
	}
	if err := store.MarkModelCapability(channel.ID, "removed", "gpt-test", true, ""); err != nil {
		t.Fatalf("mark capability: %v", err)
	}
	log := &GatewayRequestLog{ChannelID: channel.ID, GroupName: "removed", Method: "POST", Path: "/v1/responses", StatusCode: 200}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request: %v", err)
	}
	if err := store.DeleteGroupRoutingState(channel.ID, "removed"); err != nil {
		t.Fatalf("delete routing state: %v", err)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "removed"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("upstream key still exists: %v", err)
	}
	var capabilities, logs int64
	if err := db.Model(&GatewayModelCapability{}).Where("channel_id = ? AND group_name = ?", channel.ID, "removed").Count(&capabilities).Error; err != nil {
		t.Fatalf("count capabilities: %v", err)
	}
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Count(&logs).Error; err != nil {
		t.Fatalf("count logs: %v", err)
	}
	if capabilities != 0 || logs != 1 {
		t.Fatalf("capabilities=%d logs=%d", capabilities, logs)
	}
}

func TestGatewayRequestLogsPageFiltered(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 10, GroupName: "cheap", ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 1000, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, Cost: 0.01, CostSource: "sub2api_actual_cost", UpstreamKeyID: 42, UpstreamKeyName: "BridgeOps Gateway - cheap", UpstreamRequestID: "req_exact_123", RouteReason: "smart_primary", CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 11, GroupName: "fast", ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 2000, PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30, Cost: 0.02, CostSource: "group_ratio_default_quota", RouteReason: "fallback", CreatedAt: now.Add(time.Hour)},
		{DownstreamKeyID: 2, ChannelID: 10, GroupName: "cheap", ModelName: "claude-haiku", Method: "POST", Path: "/v1/messages", StatusCode: 502, LatencyMs: 3000, Cost: 0.03, CostSource: "newapi_quota", RouteReason: "manual", CreatedAt: now.Add(2 * time.Hour)},
		{DownstreamKeyID: 1, ChannelID: 10, GroupName: "cheap", ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 500, RouteReason: "latency_probe", Cost: 0.04, CreatedAt: now.Add(3 * time.Hour)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	start := now.Add(-time.Minute)
	end := now.Add(90 * time.Minute)
	items, total, summary, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{
		Start:           &start,
		End:             &end,
		DownstreamKeyID: 1,
		ModelName:       "gpt-5-mini",
		ChannelID:       10,
	})
	if err != nil {
		t.Fatalf("list filtered logs: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].ModelName != "gpt-5-mini" {
		t.Fatalf("filtered items total=%d items=%+v", total, items)
	}
	if summary.TotalRequests != 1 || summary.TotalCost != 0.01 || summary.TotalTokens != 15 {
		t.Fatalf("summary = %+v, want filtered totals", summary)
	}
	if items[0].UpstreamKeyID != 42 || items[0].UpstreamKeyName != "BridgeOps Gateway - cheap" || items[0].UpstreamRequestID != "req_exact_123" {
		t.Fatalf("upstream trace fields = %+v", items[0])
	}
	traceItems, traceTotal, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{UpstreamRequestID: "req_exact_123", UpstreamKeyName: " BridgeOps Gateway - cheap "})
	if err != nil {
		t.Fatalf("filter upstream trace: %v", err)
	}
	if traceTotal != 1 || len(traceItems) != 1 || traceItems[0].ID != items[0].ID {
		t.Fatalf("trace filter total=%d items=%+v", traceTotal, traceItems)
	}
	_, whitespaceTotal, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{UpstreamRequestID: " req_exact_123 "})
	if err != nil || whitespaceTotal != 0 {
		t.Fatalf("whitespace-distinct upstream request ID filter should not collapse: total=%d err=%v", whitespaceTotal, err)
	}
	_, partialTotal, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{UpstreamRequestID: "req_exact"})
	if err != nil || partialTotal != 0 {
		t.Fatalf("upstream request id filter should be exact: total=%d err=%v", partialTotal, err)
	}
	models, err := store.ListRequestLogModels(GatewayRequestLogFilter{DownstreamKeyID: 1})
	if err != nil {
		t.Fatalf("list models: %v", err)
	}
	if strings.Join(models, ",") != "gpt-5,gpt-5-mini" {
		t.Fatalf("models = %v, want gpt-5,gpt-5-mini", models)
	}
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{DownstreamKeyID: 1}, 8)
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	if len(analytics.Trend) != 1 || analytics.Trend[0].Requests != 2 || analytics.Trend[0].TotalCost != 0.01 || analytics.Trend[0].UnavailableRequests != 1 {
		t.Fatalf("trend = %+v, want two visible downstream=1 requests", analytics.Trend)
	}
	if len(analytics.Channels) != 2 || analytics.Channels[0].TotalCost < analytics.Channels[1].TotalCost {
		t.Fatalf("channels breakdown = %+v", analytics.Channels)
	}
	if len(analytics.Models) != 2 {
		t.Fatalf("models breakdown = %+v", analytics.Models)
	}
	if len(analytics.Keys) != 1 || analytics.Keys[0].DownstreamKeyID != 1 || analytics.Keys[0].Requests != 2 {
		t.Fatalf("keys breakdown = %+v", analytics.Keys)
	}
	if len(analytics.CostSources) != 2 || analytics.CostSources[0].Name == "" {
		t.Fatalf("cost sources breakdown = %+v", analytics.CostSources)
	}
	if len(analytics.RouteReasons) != 2 || analytics.RouteReasons[0].Name == "" {
		t.Fatalf("route reasons breakdown = %+v", analytics.RouteReasons)
	}
}

func TestGatewayRequestLogFilterMatchesProviderRequestIDByteExact(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 23, 8, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "provider-id", CreatedAt: now},
		{Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: " provider-id ", CreatedAt: now.Add(time.Second)},
		{Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: " ", CreatedAt: now.Add(2 * time.Second)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append provider-ID log %q: %v", log.UpstreamRequestID, err)
		}
	}

	for _, want := range logs {
		items, total, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{UpstreamRequestID: want.UpstreamRequestID})
		if err != nil {
			t.Fatalf("filter provider ID %q: %v", want.UpstreamRequestID, err)
		}
		if total != 1 || len(items) != 1 || items[0].ID != want.ID || items[0].UpstreamRequestID != want.UpstreamRequestID {
			t.Fatalf("filter provider ID %q = total:%d items:%+v, want only ID %d", want.UpstreamRequestID, total, items, want.ID)
		}
	}
}

func TestGatewayAPIKeyTestRecordsAreVisibleInUsage(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	for _, log := range []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 10, GroupName: "default", ModelName: "gpt-4o-mini", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, TotalTokens: 24, Cost: 0.012, RouteReason: gatewayRouteReasonKeyTest, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 10, GroupName: "default", ModelName: "probe", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, TotalTokens: 8, Cost: 0.004, RouteReason: gatewayRouteReasonLatencyProbe, CreatedAt: now.Add(time.Minute)},
	} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	items, total, summary, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{DownstreamKeyID: 1})
	if err != nil {
		t.Fatalf("list request logs: %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].RouteReason != gatewayRouteReasonKeyTest {
		t.Fatalf("visible API test records total=%d items=%+v", total, items)
	}
	if summary.TotalRequests != 1 || summary.TotalTokens != 24 || math.Abs(summary.TotalCost-0.012) > 1e-9 {
		t.Fatalf("API test summary = %+v", summary)
	}
	usage, err := store.SummarizeUsage(1)
	if err != nil {
		t.Fatalf("summarize usage: %v", err)
	}
	if usage.Requests != 1 || usage.TotalTokens != 24 || usage.LastUsedAt == nil || math.Abs(usage.TotalCost-0.012) > 1e-9 {
		t.Fatalf("API test usage = %+v", usage)
	}
}

func TestGatewayRequestLogAnalyticsSeparatesModelCostAndTokenRankings(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 13, 8, 0, 0, 0, time.UTC)
	for _, log := range []*GatewayRequestLog{
		{ModelName: "high-cost", StatusCode: 200, TotalTokens: 10, Cost: 1, CreatedAt: now},
		{ModelName: "high-token", StatusCode: 200, TotalTokens: 1000, Cost: 0.01, CreatedAt: now.Add(time.Minute)},
	} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 1)
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	if len(analytics.Models) != 1 || analytics.Models[0].Name != "high-cost" {
		t.Fatalf("cost-ranked models = %+v", analytics.Models)
	}
	if len(analytics.ModelsByTokens) != 1 || analytics.ModelsByTokens[0].Name != "high-token" {
		t.Fatalf("token-ranked models = %+v", analytics.ModelsByTokens)
	}
}

func attachTrustedEstimatedTextPriceFact(t *testing.T, db *gorm.DB, log *GatewayRequestLog, inputUnit, outputUnit float64) {
	t.Helper()
	if log.ChannelID == 0 {
		log.ChannelID = log.DownstreamKeyID
		if log.ChannelID == 0 {
			log.ChannelID = 1
		}
	}
	zero := 0.0
	fact := GatewayRequestPriceFact{
		ChannelID: log.ChannelID, ChannelAccountIdentityRevision: 1, CostUnitEpoch: 1,
		UpstreamGroupID: log.ChannelID, CanonicalGroupName: "unit-price", UpstreamGroupPresenceGeneration: 1,
		GatewayUpstreamKeyID: log.ChannelID, UpstreamKeyGeneration: 1,
		ModelName: log.ModelName, Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
		InputUnitPrice: &inputUnit, OutputUnitPrice: &outputUnit, CacheReadUnitPrice: &zero, CacheWriteUnitPrice: &zero,
		Unit: GatewayPricingUnitPerMillionTokens, Currency: "USD", Source: "test_fixture",
		ObservedAt: log.CreatedAt.Add(-2 * time.Minute), ValidFrom: log.CreatedAt.Add(-time.Minute),
		FactVersion: 1, SchemaVersion: GatewayRequestPriceFactSchemaVersion,
	}
	if err := normalizeGatewayRequestPriceFact(&fact); err != nil {
		t.Fatalf("normalize unit-price fact: %v", err)
	}
	checksum, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		t.Fatalf("checksum unit-price fact: %v", err)
	}
	fact.Checksum = checksum
	if err := db.Create(&fact).Error; err != nil {
		t.Fatalf("create unit-price fact: %v", err)
	}

	factID := fact.ID
	observedAt := fact.ObservedAt
	validFrom := fact.ValidFrom
	log.ChannelIdentityRevision = fact.ChannelAccountIdentityRevision
	log.PriceCostUnitEpoch = fact.CostUnitEpoch
	log.UpstreamGroupID = fact.UpstreamGroupID
	log.UpstreamGroupGeneration = fact.UpstreamGroupPresenceGeneration
	log.UpstreamKeyLocalID = fact.GatewayUpstreamKeyID
	log.UpstreamKeyGeneration = fact.UpstreamKeyGeneration
	log.PriceFactID = &factID
	log.PriceFactVersion = fact.FactVersion
	log.PriceFactSchemaVersion = fact.SchemaVersion
	log.PriceFactChecksum = fact.Checksum
	log.PriceCanonicalGroupName = fact.CanonicalGroupName
	log.PriceModelName = fact.ModelName
	log.PriceOperation = fact.Operation
	log.PriceProtocol = fact.Protocol
	log.PriceInputUnit = fact.InputUnitPrice
	log.PriceOutputUnit = fact.OutputUnitPrice
	log.PriceCacheReadUnit = fact.CacheReadUnitPrice
	log.PriceCacheWriteUnit = fact.CacheWriteUnitPrice
	log.PriceCurrency = fact.Currency
	log.PriceUnit = fact.Unit
	log.PriceSource = fact.Source
	log.PriceObservedAt = &observedAt
	log.PriceValidFrom = &validFrom
	log.UsageSchema = GatewayPricingUsageSchemaTokensV1
	log.UsageSchemaVersion = 1
	log.UsageInputPresent = true
	log.UsageOutputPresent = true
	log.UsageCacheReadPresent = true
	log.UsageCacheWritePresent = true
	log.CostEstimated = true
	log.CostStatus = GatewayCostStatusEstimated
	log.CostSource = "price_fact"
}

func TestGatewayRequestLogAnalyticsUsesActualGPTAndClaudeModelsForUnitPriceTrends(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 13, 8, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ModelName: "gpt-5.4", RequestedModelName: "gpt-4.1", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 100, CompletionTokens: 20, CacheCreationTokens: 10, CacheReadTokens: 30, TotalTokens: 120, GroupRatio: 1, CompletionRatio: 2, Cost: 1, CostEstimated: true, CreatedAt: now},
		{DownstreamKeyID: 2, ModelName: "claude-sonnet-4-5", Method: "POST", Path: "/v1/messages", StatusCode: 200, PromptTokens: 40, CompletionTokens: 10, TotalTokens: 50, GroupRatio: 1.5, CompletionRatio: 3, Cost: 0.12, CreatedAt: now.Add(10 * time.Minute)},
		{DownstreamKeyID: 3, ModelName: "gpt-test", Method: "POST", Path: "/v1/responses", StatusCode: 200, TotalTokens: 10, Cost: 9, CreatedAt: now},
		{DownstreamKeyID: 4, ModelName: "grok-4", Method: "POST", Path: "/v1/responses", StatusCode: 200, TotalTokens: 10, Cost: 9, CreatedAt: now},
		{DownstreamKeyID: 0, ModelName: "gpt-5.4", Method: "POST", Path: "/v1/responses", StatusCode: 200, TotalTokens: 10, Cost: 9, CreatedAt: now},
		{DownstreamKeyID: 5, ModelName: "gpt-5.4", Method: "POST", Path: "/v1/responses", StatusCode: 200, TotalTokens: 10, Cost: 9, RouteReason: gatewayRouteReasonLatencyProbe, CreatedAt: now},
		{DownstreamKeyID: 6, ModelName: "gpt-5.4", Method: "POST", Path: "/v1/responses", StatusCode: 200, TotalTokens: 10, Cost: 9, RouteReason: gatewayRouteReasonKeyTest, CreatedAt: now},
		{DownstreamKeyID: 6, ModelName: "gpt-5.4", Method: "POST", Path: "/v1/responses", StatusCode: 502, TotalTokens: 10, Cost: 9, CreatedAt: now},
	}
	attachTrustedEstimatedTextPriceFact(t, db, logs[0], 10_000, 20_000)
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	end := now.Add(time.Hour)
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &now, End: &end}, 8)
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	if len(analytics.ModelUnitPrices) != 6 {
		t.Fatalf("model unit prices = %+v", analytics.ModelUnitPrices)
	}
	byModelAndType := make(map[string]GatewayRequestLogModelUnitPriceTrendPoint, len(analytics.ModelUnitPrices))
	for _, point := range analytics.ModelUnitPrices {
		byModelAndType[point.ModelName+"\x00"+point.TokenType] = point
	}
	if point := byModelAndType["gpt-5.4\x00input"]; point.ModelFamily != "gpt" || point.TotalTokens != 60 || point.TotalCost == nil || math.Abs(*point.TotalCost-0.6) > 1e-12 || point.CostPerMillionToken == nil || math.Abs(*point.CostPerMillionToken-10000) > 1e-9 {
		t.Fatalf("gpt unit price = %+v", point)
	}
	if point := byModelAndType["gpt-5.4\x00output"]; point.TotalTokens != 20 || point.TotalCost == nil || math.Abs(*point.TotalCost-0.4) > 1e-12 || point.CostPerMillionToken == nil || math.Abs(*point.CostPerMillionToken-20000) > 1e-9 {
		t.Fatalf("gpt output unit price = %+v", point)
	}
	for _, tokenType := range []string{GatewayTokenPriceCacheCreation, GatewayTokenPriceCacheRead} {
		point := byModelAndType["gpt-5.4\x00"+tokenType]
		if point.TotalTokens <= 0 || point.TotalCost != nil || point.CostPerMillionToken != nil {
			t.Fatalf("gpt cache unit price = %+v", point)
		}
	}
	if point := byModelAndType["claude-sonnet-4-5\x00input"]; point.ModelFamily != "claude" || point.TotalTokens != 40 || point.CostPerMillionToken == nil || math.Abs(*point.CostPerMillionToken-2000) > 1e-9 {
		t.Fatalf("claude unit price = %+v", point)
	}
	if point := byModelAndType["claude-sonnet-4-5\x00output"]; point.TotalTokens != 10 || point.CostPerMillionToken == nil || math.Abs(*point.CostPerMillionToken-4000) > 1e-9 {
		t.Fatalf("claude output unit price = %+v", point)
	}
}

func TestGatewayRequestLogUnitPricesKeepUnattributedComponentsNull(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 13, 9, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ModelName: "gpt-known-zero", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, GroupRatio: 1, CompletionRatio: 2, CostStatus: GatewayCostStatusReported, CreatedAt: now},
		{DownstreamKeyID: 1, ModelName: "gpt-mixed-unknown", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, GroupRatio: 1, CompletionRatio: 2, Cost: 0.1, CostStatus: GatewayCostStatusReported, CreatedAt: now},
		{DownstreamKeyID: 1, ModelName: "gpt-mixed-unknown", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, CostStatus: GatewayCostStatusNone, CreatedAt: now.Add(time.Minute)},
		{DownstreamKeyID: 1, ModelName: "gpt-cache-actual", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 20, CompletionTokens: 5, CacheReadTokens: 5, TotalTokens: 25, GroupRatio: 1, CompletionRatio: 2, Cost: 0.2, CostStatus: GatewayCostStatusReconciled, CreatedAt: now.Add(2 * time.Minute)},
		{DownstreamKeyID: 1, ModelName: "gpt-legacy-ratios", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, Cost: 0.2, CostStatus: GatewayCostStatusReported, CreatedAt: now.Add(3 * time.Minute)},
		{DownstreamKeyID: 1, ModelName: "gpt-estimated-cache", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 20, CompletionTokens: 5, CacheReadTokens: 5, TotalTokens: 25, GroupRatio: 1, CompletionRatio: 2, Cost: 0.1, CostStatus: GatewayCostStatusEstimated, CreatedAt: now.Add(4 * time.Minute)},
	}
	attachTrustedEstimatedTextPriceFact(t, db, logs[len(logs)-1], 4_000, 8_000)
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	end := now.Add(time.Hour)
	filter := GatewayRequestLogFilter{Start: &now, End: &end, TimeZone: "UTC"}
	raw, err := store.requestLogModelUnitPriceTrend(filter, 100)
	if err != nil {
		t.Fatalf("raw unit prices: %v", err)
	}
	analytics, err := store.RequestLogAnalytics(filter, 100)
	if err != nil {
		t.Fatalf("aggregate unit prices: %v", err)
	}

	assertPoints := func(t *testing.T, points []GatewayRequestLogModelUnitPriceTrendPoint) {
		t.Helper()
		byModelAndType := make(map[string]GatewayRequestLogModelUnitPriceTrendPoint, len(points))
		for _, point := range points {
			byModelAndType[point.ModelName+"\x00"+point.TokenType] = point
		}
		assertNull := func(model, tokenType string, tokens int64) {
			t.Helper()
			point, ok := byModelAndType[model+"\x00"+tokenType]
			if !ok || point.TotalTokens != tokens || point.TotalCost != nil || point.CostPerMillionToken != nil {
				t.Fatalf("%s %s point = %+v", model, tokenType, point)
			}
		}
		for _, tokenType := range []string{GatewayTokenPriceInput, GatewayTokenPriceOutput} {
			point := byModelAndType["gpt-known-zero\x00"+tokenType]
			if point.TotalCost == nil || *point.TotalCost != 0 || point.CostPerMillionToken == nil || *point.CostPerMillionToken != 0 {
				t.Fatalf("known zero %s point = %+v", tokenType, point)
			}
		}
		assertNull("gpt-mixed-unknown", GatewayTokenPriceInput, 20)
		assertNull("gpt-mixed-unknown", GatewayTokenPriceOutput, 10)
		assertNull("gpt-cache-actual", GatewayTokenPriceInput, 15)
		assertNull("gpt-cache-actual", GatewayTokenPriceOutput, 5)
		assertNull("gpt-cache-actual", GatewayTokenPriceCacheRead, 5)
		assertNull("gpt-legacy-ratios", GatewayTokenPriceInput, 10)
		assertNull("gpt-legacy-ratios", GatewayTokenPriceOutput, 5)
		if point := byModelAndType["gpt-estimated-cache\x00input"]; point.TotalTokens != 15 || point.TotalCost == nil || math.Abs(*point.TotalCost-0.06) > 1e-12 || point.CostPerMillionToken == nil || math.Abs(*point.CostPerMillionToken-4000) > 1e-9 {
			t.Fatalf("estimated cache input point = %+v", point)
		}
		if point := byModelAndType["gpt-estimated-cache\x00output"]; point.TotalTokens != 5 || point.TotalCost == nil || math.Abs(*point.TotalCost-0.04) > 1e-12 || point.CostPerMillionToken == nil || math.Abs(*point.CostPerMillionToken-8000) > 1e-9 {
			t.Fatalf("estimated cache output point = %+v", point)
		}
		assertNull("gpt-estimated-cache", GatewayTokenPriceCacheRead, 5)
	}
	t.Run("raw", func(t *testing.T) { assertPoints(t, raw) })
	t.Run("aggregate", func(t *testing.T) { assertPoints(t, analytics.ModelUnitPrices) })
}

func TestGatewayRequestLogResultRouteAndSortFilters(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "success-model", StatusCode: 200, LatencyMs: 300, TotalTokens: 20, Cost: 0.03, RouteReason: "smart_primary", CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 11, ModelName: "failed-model", StatusCode: 502, LatencyMs: 100, TotalTokens: 30, Cost: 0.01, RouteReason: "fallback", CreatedAt: now.Add(time.Minute)},
		{DownstreamKeyID: 1, ChannelID: 12, ModelName: "canceled-model", StatusCode: 499, LatencyMs: 200, TotalTokens: 10, Cost: 0.02, RouteReason: gatewayRouteReasonClientCanceled, Error: "客户端取消", CreatedAt: now.Add(2 * time.Minute)},
		{DownstreamKeyID: 1, ChannelID: 13, ModelName: "legacy-primary", StatusCode: 200, LatencyMs: 400, TotalTokens: 40, Cost: 0.04, RouteReason: "", CreatedAt: now.Add(3 * time.Minute)},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "probe-model", StatusCode: 200, LatencyMs: 1, TotalTokens: 1, Cost: 0.001, RouteReason: gatewayRouteReasonLatencyProbe, CreatedAt: now.Add(4 * time.Minute)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	for _, tt := range []struct {
		result string
		model  string
	}{
		{result: GatewayRequestResultSuccess, model: "legacy-primary"},
		{result: GatewayRequestResultFailed, model: "failed-model"},
		{result: GatewayRequestResultCanceled, model: "canceled-model"},
	} {
		items, total, summary, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{Result: tt.result})
		if err != nil {
			t.Fatalf("list %s logs: %v", tt.result, err)
		}
		if total == 0 || len(items) == 0 || items[0].ModelName != tt.model {
			t.Fatalf("%s filter total=%d items=%+v", tt.result, total, items)
		}
		if summary.TotalRequests != total {
			t.Fatalf("%s summary total=%d, want %d", tt.result, summary.TotalRequests, total)
		}
	}

	_, total, summary, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{RouteReason: "smart_primary"})
	if err != nil {
		t.Fatalf("list primary route logs: %v", err)
	}
	if total != 2 || summary.Success != 2 {
		t.Fatalf("smart_primary total=%d summary=%+v, want explicit and legacy primary logs", total, summary)
	}

	sorts := []struct {
		by    string
		order string
		model string
	}{
		{by: GatewayRequestLogSortCreatedAt, order: GatewayRequestLogSortAscending, model: "success-model"},
		{by: GatewayRequestLogSortModelName, order: GatewayRequestLogSortAscending, model: "canceled-model"},
		{by: GatewayRequestLogSortLatencyMs, order: GatewayRequestLogSortAscending, model: "failed-model"},
		{by: GatewayRequestLogSortTotalTokens, order: GatewayRequestLogSortDescending, model: "legacy-primary"},
		{by: GatewayRequestLogSortCost, order: GatewayRequestLogSortAscending, model: "failed-model"},
		{by: GatewayRequestLogSortStatusCode, order: GatewayRequestLogSortDescending, model: "failed-model"},
	}
	for _, tt := range sorts {
		items, _, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{SortBy: tt.by, SortOrder: tt.order})
		if err != nil {
			t.Fatalf("sort %s %s: %v", tt.by, tt.order, err)
		}
		if len(items) != 4 || items[0].ModelName != tt.model {
			t.Fatalf("sort %s %s models=%+v, want first %s", tt.by, tt.order, items, tt.model)
		}
	}

	exportItems, err := store.ListRequestLogsFiltered(GatewayRequestLogFilter{
		Result:    GatewayRequestResultFailed,
		SortBy:    GatewayRequestLogSortLatencyMs,
		SortOrder: GatewayRequestLogSortAscending,
	}, 100)
	if err != nil || len(exportItems) != 1 || exportItems[0].ModelName != "failed-model" {
		t.Fatalf("filtered export items=%+v err=%v", exportItems, err)
	}
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Result: GatewayRequestResultCanceled}, 8)
	if err != nil {
		t.Fatalf("canceled analytics: %v", err)
	}
	if len(analytics.Trend) != 1 || analytics.Trend[0].Requests != 1 || analytics.Trend[0].Canceled != 1 || analytics.Trend[0].Failed != 0 {
		t.Fatalf("canceled analytics trend=%+v", analytics.Trend)
	}

	items, total, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{SortBy: "created_at; DROP TABLE gateway_request_logs"})
	if err != nil || total != 4 || len(items) != 4 || items[0].ModelName != "legacy-primary" {
		t.Fatalf("unsafe sort fallback total=%d items=%+v err=%v", total, items, err)
	}
	_, total, _, err = store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{Result: "unknown"})
	if err != nil || total != 0 {
		t.Fatalf("unknown result should fail closed: total=%d err=%v", total, err)
	}
}

func TestGatewayRequestLogAnalyticsUsesHourlyBucketsForShortRanges(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", StatusCode: 200, CreatedAt: start.Add(15 * time.Minute)},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", StatusCode: 200, CreatedAt: start.Add(75 * time.Minute)},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", StatusCode: 200, CreatedAt: start.Add(25*time.Hour + 15*time.Minute)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	shortEnd := start.Add(48 * time.Hour)
	short, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &start, End: &shortEnd}, 8)
	if err != nil {
		t.Fatalf("short-range analytics: %v", err)
	}
	if len(short.Trend) != 3 {
		t.Fatalf("hourly trend = %+v, want three hourly buckets", short.Trend)
	}
	for index, want := range []string{"2026-07-13T10:00:00Z", "2026-07-13T11:00:00Z", "2026-07-14T11:00:00Z"} {
		if short.Trend[index].Bucket != want {
			t.Fatalf("hourly bucket[%d] = %q, want %q", index, short.Trend[index].Bucket, want)
		}
	}

	wideEnd := start.Add(72 * time.Hour)
	wide, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &start, End: &wideEnd}, 8)
	if err != nil {
		t.Fatalf("wide-range analytics: %v", err)
	}
	if len(wide.Trend) != 2 || wide.Trend[0].Bucket != "2026-07-13" || wide.Trend[1].Bucket != "2026-07-14" {
		t.Fatalf("daily trend = %+v, want two daily buckets", wide.Trend)
	}
}

func TestGatewayRequestLogAnalyticsUsesConfiguredTimeZoneForDailyBuckets(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)
	end := start.Add(72 * time.Hour)
	for _, createdAt := range []time.Time{
		time.Date(2026, 7, 13, 16, 30, 0, 0, time.UTC),
		time.Date(2026, 7, 14, 15, 30, 0, 0, time.UTC),
	} {
		if err := store.AppendRequestLog(&GatewayRequestLog{ModelName: "gpt-5-mini", StatusCode: 200, TotalTokens: 10, Cost: 0.01, CreatedAt: createdAt}); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &start, End: &end, TimeZone: "Asia/Shanghai"}, 8)
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	if len(analytics.Trend) != 1 || analytics.Trend[0].Bucket != "2026-07-14" || analytics.Trend[0].Requests != 2 {
		t.Fatalf("Shanghai daily trend = %+v, want one local-day bucket", analytics.Trend)
	}
}

func TestGatewayRequestLogAnalyticsUsesExactLocalDayBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		timeZone  string
		start     time.Time
		end       time.Time
		createdAt []time.Time
		wantDays  []string
	}{
		{
			name:     "half hour offset",
			timeZone: "Asia/Kolkata",
			start:    time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC),
			createdAt: []time.Time{
				time.Date(2026, 7, 13, 18, 10, 0, 0, time.UTC),
				time.Date(2026, 7, 13, 18, 50, 0, 0, time.UTC),
			},
			wantDays: []string{"2026-07-13", "2026-07-14"},
		},
		{
			name:     "quarter hour offset",
			timeZone: "Asia/Kathmandu",
			start:    time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC),
			createdAt: []time.Time{
				time.Date(2026, 7, 13, 18, 10, 0, 0, time.UTC),
				time.Date(2026, 7, 13, 18, 20, 0, 0, time.UTC),
			},
			wantDays: []string{"2026-07-13", "2026-07-14"},
		},
		{
			name:     "daylight saving day",
			timeZone: "America/New_York",
			start:    time.Date(2026, 3, 7, 0, 0, 0, 0, time.UTC),
			end:      time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC),
			createdAt: []time.Time{
				time.Date(2026, 3, 8, 5, 30, 0, 0, time.UTC),
				time.Date(2026, 3, 9, 3, 30, 0, 0, time.UTC),
			},
			wantDays: []string{"2026-03-08"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)
			store := NewGateway(db)
			for _, createdAt := range tt.createdAt {
				if err := store.AppendRequestLog(&GatewayRequestLog{ModelName: "gpt-5-mini", StatusCode: 200, CreatedAt: createdAt}); err != nil {
					t.Fatalf("append request log: %v", err)
				}
			}

			analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &tt.start, End: &tt.end, TimeZone: tt.timeZone}, 8)
			if err != nil {
				t.Fatalf("analytics: %v", err)
			}
			if len(analytics.Trend) != len(tt.wantDays) {
				t.Fatalf("trend = %+v, want days %v", analytics.Trend, tt.wantDays)
			}
			for index, wantDay := range tt.wantDays {
				if analytics.Trend[index].Bucket != wantDay {
					t.Fatalf("trend[%d].bucket = %q, want %q", index, analytics.Trend[index].Bucket, wantDay)
				}
			}
		})
	}
}

func TestGatewayRequestLogAnalyticsBatchesAndNarrowsLongDailyRanges(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, createdAt := range []time.Time{
		time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 31, 23, 59, 0, 0, time.UTC),
		time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 15, 12, 0, 0, 0, time.UTC),
	} {
		if err := store.AppendRequestLog(&GatewayRequestLog{ModelName: "gpt-5-mini", StatusCode: 200, CreatedAt: createdAt}); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &start, End: &end, TimeZone: "UTC"}, 8)
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	if len(analytics.Trend) != 4 || analytics.Trend[0].Bucket != "2026-01-01" || analytics.Trend[1].Bucket != "2026-01-31" || analytics.Trend[2].Bucket != "2026-02-01" || analytics.Trend[3].Bucket != "2026-02-15" {
		t.Fatalf("batched trend = %+v", analytics.Trend)
	}

	empty, err := store.RequestLogAnalytics(GatewayRequestLogFilter{Start: &start, End: &end, ModelName: "missing", TimeZone: "UTC"}, 8)
	if err != nil {
		t.Fatalf("empty analytics: %v", err)
	}
	if len(empty.Trend) != 0 {
		t.Fatalf("empty trend = %+v", empty.Trend)
	}

	bucketSQL, args := store.requestLogDailyBucketSQL(start, start.AddDate(0, 0, 31))
	if got := strings.Count(bucketSQL, " WHEN "); got != 31 || len(args) != 93 {
		t.Fatalf("daily batch has %d clauses and %d args, want 31 and 93", got, len(args))
	}
}

func TestGatewayRequestLogFilterNormalizesUTCBoundsForPostgres(t *testing.T) {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load Shanghai timezone: %v", err)
	}
	originalLocal := time.Local
	time.Local = shanghai
	t.Cleanup(func() { time.Local = originalLocal })

	db := openTestDB(t)
	store := NewGateway(db)
	newestAt := time.Date(2026, 7, 14, 14, 22, 59, 0, shanghai)
	for _, createdAt := range []time.Time{newestAt.Add(-30 * time.Second), newestAt} {
		if err := store.AppendRequestLog(&GatewayRequestLog{
			DownstreamKeyID: 1,
			ChannelID:       10,
			ModelName:       "gpt-5-mini",
			Method:          "POST",
			Path:            "/v1/responses",
			StatusCode:      200,
			CreatedAt:       createdAt,
		}); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	for _, createdAt := range []time.Time{
		time.Date(2026, 7, 13, 23, 30, 0, 0, shanghai),
		time.Date(2026, 7, 15, 0, 30, 0, 0, shanghai),
	} {
		if err := store.AppendRequestLog(&GatewayRequestLog{
			DownstreamKeyID: 1,
			ChannelID:       10,
			ModelName:       "gpt-5-mini",
			Method:          "POST",
			Path:            "/v1/responses",
			StatusCode:      200,
			CreatedAt:       createdAt,
		}); err != nil {
			t.Fatalf("append cross-day request log: %v", err)
		}
	}

	start := newestAt.Add(-time.Minute).UTC()
	end := newestAt.Add(time.Minute).UTC()
	items, total, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{Start: &start, End: &end})
	if err != nil {
		t.Fatalf("list logs in UTC window: %v", err)
	}
	if total != 2 || len(items) != 2 || !items[0].CreatedAt.Equal(newestAt) {
		t.Fatalf("UTC-window logs = total %d, items %+v; want newest Shanghai row first", total, items)
	}

	outsideStart := newestAt.Add(time.Minute).UTC()
	outsideEnd := newestAt.Add(2 * time.Minute).UTC()
	_, outsideTotal, _, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{Start: &outsideStart, End: &outsideEnd})
	if err != nil {
		t.Fatalf("list logs outside UTC window: %v", err)
	}
	if outsideTotal != 0 {
		t.Fatalf("outside UTC-window total = %d, want 0", outsideTotal)
	}

	dailyStart := time.Date(2026, 7, 13, 0, 0, 0, 0, shanghai).UTC()
	dailyEnd := time.Date(2026, 7, 16, 0, 0, 0, 0, shanghai).UTC()
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{
		Start:    &dailyStart,
		End:      &dailyEnd,
		TimeZone: "Asia/Shanghai",
	}, 8)
	if err != nil {
		t.Fatalf("daily analytics in UTC window: %v", err)
	}
	wantDaily := []struct {
		bucket   string
		requests int64
	}{{"2026-07-13", 1}, {"2026-07-14", 2}, {"2026-07-15", 1}}
	if len(analytics.Trend) != len(wantDaily) {
		t.Fatalf("daily trend = %+v, want %+v", analytics.Trend, wantDaily)
	}
	for index, want := range wantDaily {
		if analytics.Trend[index].Bucket != want.bucket || analytics.Trend[index].Requests != want.requests {
			t.Fatalf("daily trend[%d] = %+v, want bucket %q with %d requests", index, analytics.Trend[index], want.bucket, want.requests)
		}
	}
}

func TestGatewayRequestLogSummaryIncludesRateAndConcurrencyMetrics(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 1000, PromptTokens: 40, CompletionTokens: 20, TotalTokens: 60, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 2000, PromptTokens: 30, CompletionTokens: 30, TotalTokens: 60, CreatedAt: now.Add(time.Minute)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	summary, err := store.requestLogSummaryFilteredAt(GatewayRequestLogFilter{}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if summary.WindowSeconds != 60 {
		t.Fatalf("window seconds = %f, want 60", summary.WindowSeconds)
	}
	if summary.FirstRequestAt == nil || !summary.FirstRequestAt.Equal(now) {
		t.Fatalf("first request at = %v, want %v", summary.FirstRequestAt, now)
	}
	if summary.LastRequestAt == nil || !summary.LastRequestAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("last request at = %v, want %v", summary.LastRequestAt, now.Add(time.Minute))
	}
	if summary.PerformanceWindowSeconds != 300 {
		t.Fatalf("performance window seconds = %f, want 300", summary.PerformanceWindowSeconds)
	}
	if summary.PerformanceRequests != 2 || summary.PerformanceTokens != 120 {
		t.Fatalf("performance totals = (%d, %d), want (2, 120)", summary.PerformanceRequests, summary.PerformanceTokens)
	}
	assertFloatNear(t, summary.RPM, 0, 0.000001, "rpm")
	assertFloatNear(t, summary.TPM, 24, 0.000001, "tpm")
	assertFloatNear(t, summary.EstimatedConcurrency, 0.01, 0.000001, "estimated concurrency")

	start := now
	end := now.Add(2 * time.Minute)
	filtered, err := store.requestLogSummaryFilteredAt(GatewayRequestLogFilter{Start: &start, End: &end}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("filtered summary: %v", err)
	}
	if filtered.WindowSeconds != 120 {
		t.Fatalf("filtered window seconds = %f, want 120", filtered.WindowSeconds)
	}
	assertFloatNear(t, filtered.RPM, 0, 0.000001, "filtered rpm")
	assertFloatNear(t, filtered.TPM, 24, 0.000001, "filtered tpm")
}

func TestGatewayRequestLogPerformanceWindowExcludesNonModelRequests(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		log := &GatewayRequestLog{
			DownstreamKeyID:  1,
			ChannelID:        10,
			ModelName:        "gpt-5-mini",
			Method:           "POST",
			Path:             "/v1/responses",
			StatusCode:       200,
			LatencyMs:        1000,
			PromptTokens:     3,
			CompletionTokens: 2,
			TotalTokens:      5,
			CreatedAt:        now.Add(-time.Duration(i) * 20 * time.Second),
		}
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append successful log: %v", err)
		}
	}
	for _, log := range []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 1000, PromptTokens: 100, CompletionTokens: 10, CacheCreationTokens: 10, CacheReadTokens: 80, TotalTokens: 110, CreatedAt: now.Add(-5 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 1000, PromptTokens: 5, CompletionTokens: 2, CacheReadTokens: 10, TotalTokens: 7, CreatedAt: now.Add(-6 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 502, CreatedAt: now.Add(-time.Minute)},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: gatewayRouteReasonClientCanceled, CreatedAt: now.Add(-50 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: gatewayRouteReasonLatencyProbe, CreatedAt: now.Add(-40 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 10, Method: "GET", Path: "/v1/models", StatusCode: 200, CreatedAt: now.Add(-30 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 99, CompletionTokens: 99, CreatedAt: now.Add(-10 * time.Minute)},
	} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append excluded log: %v", err)
		}
	}

	summary, err := store.requestLogSummaryFilteredAt(GatewayRequestLogFilter{}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if summary.PerformanceRequests != 12 || summary.PerformanceTokens != 72 {
		t.Fatalf("performance totals = (%d, %d), want (12, 72)", summary.PerformanceRequests, summary.PerformanceTokens)
	}
	assertFloatNear(t, summary.RPM, 2, 0.000001, "rpm")
	assertFloatNear(t, summary.TPM, 14, 0.000001, "tpm")
	assertFloatNear(t, summary.PerformanceAvgLatency, 1000, 0.000001, "performance avg latency")
}

func assertFloatNear(t *testing.T, got float64, want float64, tolerance float64, name string) {
	t.Helper()
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %f, want %f", name, got, want)
	}
}

func trendFloat(value float64) *float64 {
	return &value
}

func trendFloatEqual(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func TestGatewayRequestLogAnalyticsNormalizesEmptyBreakdownLabels(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", StatusCode: 200, LatencyMs: 1000, Cost: 0.01, CostSource: "", RouteReason: "", CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 10, ModelName: "gpt-5-mini", StatusCode: 200, LatencyMs: 1000, Cost: 0.02, CostSource: " ", RouteReason: " ", CreatedAt: now.Add(time.Minute)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{DownstreamKeyID: 1}, 8)
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	if len(analytics.CostSources) != 1 {
		t.Fatalf("cost sources = %+v", analytics.CostSources)
	}
	if analytics.CostSources[0].Name != "未提供费用来源" {
		t.Fatalf("cost sources = %+v", analytics.CostSources)
	}
	if len(analytics.RouteReasons) != 1 {
		t.Fatalf("route reasons = %+v", analytics.RouteReasons)
	}
	if analytics.RouteReasons[0].Name != "smart_primary" {
		t.Fatalf("route reasons = %+v", analytics.RouteReasons)
	}
}

func TestGatewayEmptyListsAreNonNil(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)

	keys, err := store.ListDownstreamKeys()
	if err != nil {
		t.Fatalf("list downstream keys: %v", err)
	}
	if keys == nil {
		t.Fatal("downstream keys should be an empty slice, not nil")
	}

	items, total, summary, err := store.ListRequestLogsPageFiltered(1, 20, GatewayRequestLogFilter{})
	if err != nil {
		t.Fatalf("list request logs: %v", err)
	}
	if items == nil {
		t.Fatal("request log items should be an empty slice, not nil")
	}
	if total != 0 {
		t.Fatalf("total = %d, want 0", total)
	}
	if summary == nil || summary.TotalRequests != 0 {
		t.Fatalf("summary = %+v, want zero summary", summary)
	}

	exportItems, err := store.ListRequestLogsFiltered(GatewayRequestLogFilter{}, 100)
	if err != nil {
		t.Fatalf("list export request logs: %v", err)
	}
	if exportItems == nil {
		t.Fatal("export request log items should be an empty slice, not nil")
	}

	models, err := store.ListRequestLogModels(GatewayRequestLogFilter{})
	if err != nil {
		t.Fatalf("list request log models: %v", err)
	}
	if models == nil {
		t.Fatal("request log models should be an empty slice, not nil")
	}
}

func TestSummarizeDownstreamKeyUsageIncludesTodayAndTotal(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 13, 15, 30, 0, 0, time.UTC)
	dayStart := time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.0123, TotalTokens: 100, CreatedAt: dayStart.Add(-time.Hour)},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.0045, TotalTokens: 40, CreatedAt: dayStart.Add(time.Hour)},
		{DownstreamKeyID: 2, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.2, TotalTokens: 500, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "GET", Path: "/v1/models", StatusCode: 200, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: "latency_probe", Cost: 0.8, CreatedAt: now},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	usage, err := store.SummarizeDownstreamKeyUsage([]uint{1, 2, 3}, now)
	if err != nil {
		t.Fatalf("summarize key usage: %v", err)
	}
	got := usage[1]
	if got.TotalCost != 0.0168 || got.TodayCost != 0.0045 || got.TotalRequests != 3 || got.TodayRequests != 2 || got.TotalTokens != 140 || got.TodayTokens != 40 {
		t.Fatalf("key 1 usage = %+v", got)
	}
	if got := usage[2]; got.TotalCost != 0.2 || got.TodayCost != 0.2 || got.TotalRequests != 1 || got.TodayRequests != 1 {
		t.Fatalf("key 2 usage = %+v", got)
	}
	if _, ok := usage[3]; ok {
		t.Fatal("empty key should not have an aggregate row")
	}
}

func TestGatewayRequestAggregatesBackfillOnlyUnprocessedLogs(t *testing.T) {
	db := openTestDB(t)
	now := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	logs := []GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200, TotalTokens: 10, Cost: 0.1, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 502, TotalTokens: 20, Cost: 0.2, CreatedAt: now.Add(time.Minute)},
		{ChannelID: 2, ModelName: "probe", Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: gatewayRouteReasonLatencyProbe, CreatedAt: now},
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatalf("create legacy logs: %v", err)
	}
	if err := migrateGatewayRequestAggregates(db); err != nil {
		t.Fatalf("backfill aggregates: %v", err)
	}
	if err := migrateGatewayRequestAggregates(db); err != nil {
		t.Fatalf("repeat aggregate migration: %v", err)
	}
	var versionCount int64
	if err := db.Model(&GatewayRequestLog{}).Where("aggregate_version = ?", gatewayRequestAggregateVersion).Count(&versionCount).Error; err != nil || versionCount != 3 {
		t.Fatalf("versioned logs = %d, err=%v", versionCount, err)
	}
	var rows []GatewayRequestAggregate
	if err := db.Find(&rows).Error; err != nil {
		t.Fatalf("list aggregates: %v", err)
	}
	var requests, tokens int64
	var cost float64
	for _, row := range rows {
		requests += row.Requests
		tokens += row.TotalTokens
		cost += row.TotalCost
	}
	if requests != 2 || tokens != 30 || math.Abs(cost-0.1) > 1e-12 {
		t.Fatalf("aggregate totals = requests %d tokens %d cost %.6f", requests, tokens, cost)
	}
}

func TestGatewayRequestAggregateV2RebuildReplacesLegacyTotals(t *testing.T) {
	db := openTestDB(t)
	now := time.Date(2026, 7, 18, 15, 0, 0, 0, time.UTC)
	usageID := "legacy-custom-usage"
	logs := []GatewayRequestLog{
		{
			DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, LatencyMs: 100, Cost: 0.4, CostSource: "custom_actual", UpstreamUsageID: &usageID,
			AggregateVersion: 1, CreatedAt: now,
		},
		{
			DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, LatencyMs: 200, Cost: 0.2, CostSource: "custom_actual", CostStatus: GatewayCostStatusReported,
			AggregateVersion: 1, CreatedAt: now.Add(time.Minute),
		},
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatalf("create v1 request logs: %v", err)
	}
	legacy := &GatewayRequestAggregate{
		BucketStart: aggregateBucketStart(now), DownstreamKeyID: 1, ChannelID: 2,
		ModelName: "gpt-5", CostSource: "custom_actual", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
		Requests: 2, TotalCost: 0.6, ActualCost: 0.6, LatencyTotalMs: 300, LatencySamples: 2,
		FirstRequestAt: now, LastRequestAt: now.Add(time.Minute), Version: 1,
	}
	if err := db.Create(legacy).Error; err != nil {
		t.Fatalf("create v1 aggregate: %v", err)
	}

	processed, err := backfillGatewayRequestAggregates(context.Background(), db, 1, 0)
	if err != nil || processed != 2 {
		t.Fatalf("rebuild v2 aggregates: processed=%d err=%v", processed, err)
	}
	var rebuilt GatewayRequestAggregate
	if err := db.First(&rebuilt, legacy.ID).Error; err != nil {
		t.Fatalf("read rebuilt aggregate: %v", err)
	}
	if rebuilt.Version != gatewayRequestAggregateVersion || rebuilt.Requests != 2 {
		t.Fatalf("rebuilt aggregate version/requests = %+v", rebuilt)
	}
	assertFloatNear(t, rebuilt.TotalCost, 0.6, 1e-12, "rebuilt total")
	assertFloatNear(t, rebuilt.ActualCost, 0.6, 1e-12, "rebuilt actual")
	assertFloatNear(t, rebuilt.ReportedCost, 0.2, 1e-12, "rebuilt reported")
	assertFloatNear(t, rebuilt.ReconciledCost, 0.4, 1e-12, "rebuilt reconciled")
	ready, err := requestAggregatesReady(db)
	if err != nil || !ready {
		t.Fatalf("rebuilt aggregates ready=%t err=%v", ready, err)
	}
	var migrated []GatewayRequestLog
	if err := db.Order("id ASC").Find(&migrated).Error; err != nil {
		t.Fatalf("read migrated logs: %v", err)
	}
	if len(migrated) != 2 || migrated[0].CostStatus != GatewayCostStatusReconciled || migrated[1].CostStatus != GatewayCostStatusReported {
		t.Fatalf("migrated cost statuses = %+v", migrated)
	}
}

func TestGatewayRequestAggregateConcurrentBackfillCountsOnce(t *testing.T) {
	db := openTestDB(t)
	logs := make([]GatewayRequestLog, 100)
	for index := range logs {
		logs[index] = GatewayRequestLog{
			DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, TotalTokens: 10, Cost: 0.1, CreatedAt: time.Now().Add(time.Duration(index) * time.Second),
		}
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatalf("create legacy logs: %v", err)
	}
	errs := make(chan error, 2)
	for worker := 0; worker < 2; worker++ {
		go func() {
			_, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0)
			errs <- err
		}()
	}
	for worker := 0; worker < 2; worker++ {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent backfill: %v", err)
		}
	}
	var total struct {
		Requests int64
		Tokens   int64
		Cost     float64
	}
	if err := db.Model(&GatewayRequestAggregate{}).Select("SUM(requests) AS requests, SUM(total_tokens) AS tokens, SUM(total_cost) AS cost").Scan(&total).Error; err != nil {
		t.Fatalf("sum aggregates: %v", err)
	}
	if total.Requests != 100 || total.Tokens != 1000 || math.Abs(total.Cost-10) > 1e-9 {
		t.Fatalf("aggregate totals = %+v", total)
	}
}

func TestGatewayRequestAnalyticsReadsPersistedAggregates(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	for index := 0; index < 20; index++ {
		if err := store.AppendRequestLog(&GatewayRequestLog{
			DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, TotalTokens: 10, Cost: 0.01, CreatedAt: now.Add(time.Duration(index) * time.Minute),
		}); err != nil {
			t.Fatalf("append log: %v", err)
		}
	}
	if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&GatewayRequestLog{}).Error; err != nil {
		t.Fatalf("remove raw logs: %v", err)
	}
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 8)
	if err != nil {
		t.Fatalf("aggregate analytics: %v", err)
	}
	if len(analytics.Trend) != 1 || analytics.Trend[0].Requests != 20 || analytics.Trend[0].TotalTokens != 200 {
		t.Fatalf("aggregate analytics = %+v", analytics.Trend)
	}
}

func TestGatewayRequestCostReconcileMovesAggregateKeyByDelta(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	log := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30,
		Cost: 0.2, CostEstimated: true, CostSource: "gateway_estimate", CreatedAt: now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append estimated log: %v", err)
	}
	if err := store.UpdateRequestLogActualCostWithUsage(log.ID, "usage-1", 0.1, 0.1, "sub2api_actual_cost", GatewayRequestUsageMetrics{
		PromptTokens: 125, CompletionTokens: 30, TotalTokens: 155, HasTokenMetrics: true,
		CacheCreationTokens: 5, CacheReadTokens: 100, UpstreamModelName: "gpt-5.6mini", HasCacheTokenMetrics: true,
	}); err != nil {
		t.Fatalf("reconcile cost: %v", err)
	}
	summary, err := store.RequestLogSummary()
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if summary.TotalRequests != 1 || math.Abs(summary.TotalCost-0.1) > 1e-12 || summary.EstimatedCost != 0 || summary.ReportedCost != 0 || math.Abs(summary.ReconciledCost-0.1) > 1e-12 || math.Abs(summary.ActualCost-0.1) > 1e-12 || summary.PromptTokens != 125 || summary.CompletionTokens != 30 || summary.TotalTokens != 155 || summary.CacheCreationTokens != 5 || summary.CacheReadTokens != 100 {
		t.Fatalf("reconciled aggregate summary = %+v", summary)
	}
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 8)
	if err != nil {
		t.Fatalf("analytics: %v", err)
	}
	if len(analytics.CostSources) != 1 || analytics.CostSources[0].Name != "sub2api_actual_cost" || analytics.CostSources[0].Requests != 1 {
		t.Fatalf("reconciled cost sources = %+v", analytics.CostSources)
	}
	var reconciled GatewayRequestLog
	if err := db.First(&reconciled, log.ID).Error; err != nil {
		t.Fatalf("read reconciled log: %v", err)
	}
	if reconciled.UpstreamModelName != "gpt-5.6mini" {
		t.Fatalf("upstream model = %q", reconciled.UpstreamModelName)
	}
}

func TestGatewayRequestCostReconcileStateLifecycle(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	log := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		Cost: 0.2, CostEstimated: true, CostSource: "gateway_estimate", CreatedAt: now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append estimated request: %v", err)
	}
	if log.CostStatus != GatewayCostStatusUnavailable || log.CostEstimated || log.CostUnavailableReason != GatewayPricingUnavailableLegacyUnverifiedEstimate {
		t.Fatalf("appended cost state = status %q estimated %v", log.CostStatus, log.CostEstimated)
	}
	if err := store.MarkRequestCostReconcileRetry(log.ID, now.Add(time.Minute), " upstream record pending "); err != nil {
		t.Fatalf("mark retry with error: %v", err)
	}
	if err := store.MarkRequestCostReconcileRetry(log.ID, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("mark retry without error: %v", err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read retried request: %v", err)
	}
	if stored.CostReconcileTries != 2 || stored.CostReconcileError != "upstream record pending" || stored.CostReconcileAt == nil {
		t.Fatalf("retried state = %+v", stored)
	}
	if err := store.MarkRequestCostReconcileFailed(log.ID, " retry limit reached "); err != nil {
		t.Fatalf("mark terminal reconcile failure: %v", err)
	}
	stored = GatewayRequestLog{}
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read failed request: %v", err)
	}
	if stored.CostReconcileTries != 3 || stored.CostReconcileError != "retry limit reached" || stored.CostReconcileAt != nil {
		t.Fatalf("terminal state = %+v", stored)
	}
	if err := store.UpdateRequestLogActualCostWithRaw(log.ID, "usage-lifecycle", 0.1, 0.1, "sub2api_actual_cost", 0, 0, false); err != nil {
		t.Fatalf("complete reconcile: %v", err)
	}
	stored = GatewayRequestLog{}
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read reconciled request: %v", err)
	}
	if stored.CostStatus != GatewayCostStatusReconciled || stored.CostEstimated || stored.CostReconcileError != "" || stored.CostReconcileAt != nil {
		t.Fatalf("reconciled state = %+v", stored)
	}
}

func TestGatewayRequestCostStatusStatisticsMatchRawAndAggregates(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 18, 15, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.1, CostEstimated: true, CostSource: "gateway_estimate", CreatedAt: now.Add(time.Minute)},
		{DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.2, CostSource: "cost", CreatedAt: now.Add(2 * time.Minute)},
		{DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.3, CostSource: "sub2api_actual_cost", CreatedAt: now.Add(3 * time.Minute)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	wantStatuses := []GatewayCostStatus{
		GatewayCostStatusNone,
		GatewayCostStatusUnavailable,
		GatewayCostStatusReported,
		GatewayCostStatusReconciled,
	}
	for index := range logs {
		if logs[index].CostStatus != wantStatuses[index] {
			t.Fatalf("log %d status = %q, want %q", index, logs[index].CostStatus, wantStatuses[index])
		}
	}

	assertCosts := func(label string, total, estimated, reported, reconciled, actual float64) {
		t.Helper()
		assertFloatNear(t, total, 0.5, 1e-12, label+" total")
		assertFloatNear(t, estimated, 0, 1e-12, label+" estimated")
		assertFloatNear(t, reported, 0.2, 1e-12, label+" reported")
		assertFloatNear(t, reconciled, 0.3, 1e-12, label+" reconciled")
		assertFloatNear(t, actual, 0.5, 1e-12, label+" actual")
	}

	aggregateSummary, err := store.RequestLogSummary()
	if err != nil {
		t.Fatalf("aggregate summary: %v", err)
	}
	rawSummary, err := store.requestLogSummaryFilteredAtRaw(GatewayRequestLogFilter{}, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("raw summary: %v", err)
	}
	for label, summary := range map[string]*GatewayRequestLogSummary{"aggregate summary": aggregateSummary, "raw summary": rawSummary} {
		assertCosts(label, summary.TotalCost, summary.EstimatedCost, summary.ReportedCost, summary.ReconciledCost, summary.ActualCost)
	}

	aggregateAnalytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 8)
	if err != nil {
		t.Fatalf("aggregate analytics: %v", err)
	}
	rawAnalytics, err := store.requestLogAnalyticsRaw(GatewayRequestLogFilter{}, 8)
	if err != nil {
		t.Fatalf("raw analytics: %v", err)
	}
	for label, analytics := range map[string]*GatewayRequestLogAnalytics{"aggregate analytics": aggregateAnalytics, "raw analytics": rawAnalytics} {
		if len(analytics.Trend) != 1 || len(analytics.Channels) != 1 || len(analytics.Models) != 1 || len(analytics.Keys) != 1 {
			t.Fatalf("%s shape = %+v", label, analytics)
		}
		trend := analytics.Trend[0]
		assertCosts(label+" trend", trend.TotalCost, trend.EstimatedCost, trend.ReportedCost, trend.ReconciledCost, trend.ActualCost)
		channel := analytics.Channels[0]
		assertCosts(label+" breakdown", channel.TotalCost, channel.EstimatedCost, channel.ReportedCost, channel.ReconciledCost, channel.ActualCost)
	}

	aggregateKeyUsage, err := store.SummarizeDownstreamKeyUsage([]uint{1}, now)
	if err != nil {
		t.Fatalf("aggregate key usage: %v", err)
	}
	rawKeyUsage, err := store.summarizeDownstreamKeyUsageRaw([]uint{1}, now)
	if err != nil {
		t.Fatalf("raw key usage: %v", err)
	}
	for label, usage := range map[string]GatewayDownstreamKeyUsage{"aggregate key usage": aggregateKeyUsage[1], "raw key usage": rawKeyUsage[1]} {
		assertCosts(label, usage.TotalCost, usage.EstimatedCost, usage.ReportedCost, usage.ReconciledCost, usage.ActualCost)
	}

	usage, err := store.SummarizeUsage(1)
	if err != nil {
		t.Fatalf("usage summary: %v", err)
	}
	assertCosts("usage summary", usage.TotalCost, usage.EstimatedCost, usage.ReportedCost, usage.ReconciledCost, usage.ActualCost)

	items, _, _, err := store.ListRequestLogsPageFiltered(1, 10, GatewayRequestLogFilter{})
	if err != nil {
		t.Fatalf("list request logs: %v", err)
	}
	seen := make(map[GatewayCostStatus]bool, len(items))
	for _, item := range items {
		seen[item.CostStatus] = true
	}
	for _, status := range wantStatuses {
		if !seen[status] {
			t.Fatalf("request list missing cost status %q: %+v", status, items)
		}
	}
}

func TestGatewayRequestAggregatesPreserveCostStatusIndependentOfSource(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 18, 15, 0, 0, 0, time.UTC)
	usageID := "custom-upstream-usage"
	logs := []*GatewayRequestLog{
		{
			DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, Cost: 0.4, CostSource: "custom_actual", UpstreamUsageID: &usageID, CreatedAt: now,
		},
		{
			DownstreamKeyID: 1, ChannelID: 3, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, Cost: 0.2, CostSource: gatewayCostSourceNewAPIQuota, CostStatus: GatewayCostStatusReported, CreatedAt: now.Add(time.Minute),
		},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	if _, used, err := store.requestLogSummaryFromAggregates(GatewayRequestLogFilter{}, now.Add(time.Hour)); err != nil || !used {
		t.Fatalf("aggregate summary used=%t err=%v, want aggregate read", used, err)
	}
	if _, used, err := store.requestLogAnalyticsFromAggregates(GatewayRequestLogFilter{}, 8); err != nil || !used {
		t.Fatalf("aggregate analytics used=%t err=%v, want aggregate read", used, err)
	}
	if _, used, err := store.summarizeDownstreamKeyUsageFromAggregates([]uint{1}, now); err != nil || !used {
		t.Fatalf("aggregate key usage used=%t err=%v, want aggregate read", used, err)
	}

	assertCosts := func(label string, reported, reconciled, actual float64) {
		t.Helper()
		assertFloatNear(t, reported, 0.2, 1e-12, label+" reported")
		assertFloatNear(t, reconciled, 0.4, 1e-12, label+" reconciled")
		assertFloatNear(t, actual, 0.6, 1e-12, label+" actual")
	}
	summary, err := store.RequestLogSummary()
	if err != nil {
		t.Fatalf("request summary: %v", err)
	}
	assertCosts("summary", summary.ReportedCost, summary.ReconciledCost, summary.ActualCost)
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 8)
	if err != nil {
		t.Fatalf("request analytics: %v", err)
	}
	if len(analytics.Trend) != 1 {
		t.Fatalf("analytics trend = %+v", analytics.Trend)
	}
	assertCosts("analytics", analytics.Trend[0].ReportedCost, analytics.Trend[0].ReconciledCost, analytics.Trend[0].ActualCost)
	usage, err := store.SummarizeDownstreamKeyUsage([]uint{1}, now)
	if err != nil {
		t.Fatalf("key usage: %v", err)
	}
	assertCosts("key usage", usage[1].ReportedCost, usage[1].ReconciledCost, usage[1].ActualCost)
}

func TestRateUpsertSkipsUnchangedHeartbeatWrite(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)
	channel := &Channel{Name: "rate-heartbeat", Type: ChannelTypeNewAPI, SiteURL: "https://rate-heartbeat.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	firstSeen := time.Date(2026, 7, 13, 15, 0, 0, 0, time.UTC)
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1, CompletionRatio: 2, LastSeenAt: firstSeen}); err != nil {
		t.Fatalf("insert rate: %v", err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1, CompletionRatio: 2, LastSeenAt: firstSeen.Add(time.Minute)}); err != nil {
		t.Fatalf("repeat rate: %v", err)
	}
	stored, err := rates.FindByChannelAndModel(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find rate: %v", err)
	}
	if !stored.LastSeenAt.Equal(firstSeen) {
		t.Fatalf("unchanged heartbeat wrote last_seen_at = %v", stored.LastSeenAt)
	}
}

func TestRateUpsertLoadedRevalidatesDetachedPreviousAfterPresenceFence(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)
	channel := &Channel{Name: "rate-revalidation", Type: ChannelTypeNewAPI, SiteURL: "https://rate-revalidation.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	seenAt := time.Date(2026, 7, 18, 8, 0, 0, 0, time.UTC)
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1, CompletionRatio: 2, LastSeenAt: seenAt}); err != nil {
		t.Fatalf("insert rate: %v", err)
	}
	loaded, err := rates.FindByChannelAndModel(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("load previous rate: %v", err)
	}
	if err := db.Model(&RateSnapshot{}).Where("id = ?", loaded.ID).Updates(map[string]any{
		"ratio": 9.0, "completion_ratio": 8.0, "description": "concurrent",
	}).Error; err != nil {
		t.Fatalf("simulate concurrent update: %v", err)
	}

	next := &RateSnapshot{
		ChannelID: channel.ID, ModelName: "gpt", Description: "updated",
		Ratio: 1.5, CompletionRatio: 2.5, LastSeenAt: seenAt.Add(11 * time.Minute),
	}
	previous, err := rates.UpsertLoaded(next, loaded)
	if err != nil {
		t.Fatalf("upsert loaded rate: %v", err)
	}
	if previous == nil || previous.Ratio != 9 || previous.CompletionRatio != 8 || previous.Description != "concurrent" {
		t.Fatalf("previous rate = %+v", previous)
	}

	stored, err := rates.FindByChannelAndModel(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find updated rate: %v", err)
	}
	if stored.Ratio != 1.5 || stored.CompletionRatio != 2.5 || stored.Description != "updated" {
		t.Fatalf("updated rate = %+v", stored)
	}
}

func TestGatewayRequestCostReconcilePersistsAndPreventsUsageReuse(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now()
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.1, CostEstimated: true, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.2, CostEstimated: true, CreatedAt: now.Add(time.Second)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
		if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Second)); err != nil {
			t.Fatalf("schedule reconcile: %v", err)
		}
	}
	canceledLogs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 499, RouteReason: "client_canceled", Error: "客户端取消", CreatedAt: now.Add(2 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Error: "context canceled", CreatedAt: now.Add(3 * time.Second)},
	}
	for _, log := range canceledLogs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append canceled request log: %v", err)
		}
		if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Second)); err != nil {
			t.Fatalf("schedule canceled reconcile: %v", err)
		}
	}
	acceptedFailedLogs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 3, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "stream failed", UpstreamAccepted: true, CreatedAt: now.Add(4 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 4, Method: "POST", Path: "/v1/images/generations", StatusCode: 499, RouteReason: "client_canceled", BillingCommitted: true, CreatedAt: now.Add(5 * time.Second)},
	}
	for _, log := range acceptedFailedLogs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append accepted failed request log: %v", err)
		}
		if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Second)); err != nil {
			t.Fatalf("schedule accepted failed reconcile: %v", err)
		}
	}
	pending, err := store.ListPendingRequestCostReconciles(10, now)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 4 || pending[0].ID != logs[0].ID || pending[1].ID != logs[1].ID || pending[2].ID != acceptedFailedLogs[0].ID || pending[3].ID != acceptedFailedLogs[1].ID {
		t.Fatalf("pending = %+v, want success plus explicit accepted/committed failures", pending)
	}
	if err := store.UpdateRequestLogActualCost(logs[0].ID, "usage-1", 0.01234567, "sub2api_actual_cost", 5, 100, true); err != nil {
		t.Fatalf("update actual cost: %v", err)
	}
	var duplicateBefore GatewayRequestLog
	if err := db.First(&duplicateBefore, logs[1].ID).Error; err != nil {
		t.Fatalf("read duplicate before conflict: %v", err)
	}
	if err := store.UpdateRequestLogActualCost(logs[1].ID, "usage-1", 0.02, "sub2api_actual_cost", 0, 0, false); !errors.Is(err, ErrUpstreamUsageAlreadyAssigned) {
		t.Fatalf("reusing the same upstream usage error = %v, want %v", err, ErrUpstreamUsageAlreadyAssigned)
	}
	if abandoned, err := store.MarkRequestCostReconcileConflict(logs[1].ID, now.Add(time.Minute), 1); err != nil || !abandoned {
		t.Fatalf("abandon duplicate reconcile: abandoned=%v err=%v", abandoned, err)
	}
	var duplicate GatewayRequestLog
	if err := db.First(&duplicate, logs[1].ID).Error; err != nil {
		t.Fatalf("read duplicate log: %v", err)
	}
	if duplicate.CostReconcileAt != nil || duplicate.UpstreamUsageID != nil || duplicate.CostReconcileError != gatewayCostReconcileConflictError {
		t.Fatalf("duplicate reconcile was not terminal and preserved: %+v", duplicate)
	}
	sameOptionalFloat := func(left, right *float64) bool {
		return left == nil && right == nil || left != nil && right != nil && *left == *right
	}
	if duplicate.Cost != duplicateBefore.Cost || duplicate.CostEstimated != duplicateBefore.CostEstimated ||
		duplicate.CostStatus != duplicateBefore.CostStatus || duplicate.CostUnavailableReason != duplicateBefore.CostUnavailableReason ||
		!sameOptionalFloat(duplicate.RawCost, duplicateBefore.RawCost) || !sameOptionalFloat(duplicate.FundingFactor, duplicateBefore.FundingFactor) ||
		duplicate.CostSource != duplicateBefore.CostSource || duplicate.CacheCreationTokens != duplicateBefore.CacheCreationTokens ||
		duplicate.CacheReadTokens != duplicateBefore.CacheReadTokens {
		t.Fatalf("duplicate reconcile partially updated accounting fields: %+v", duplicate)
	}
	otherChannel := &GatewayRequestLog{DownstreamKeyID: 1, ChannelID: 3, Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: now}
	if err := store.AppendRequestLog(otherChannel); err != nil {
		t.Fatalf("append other-channel request log: %v", err)
	}
	if err := store.UpdateRequestLogActualCost(otherChannel.ID, "usage-1", 0.03, "sub2api_actual_cost", 0, 0, false); err != nil {
		t.Fatalf("same upstream usage id on another channel should be allowed: %v", err)
	}
	var got GatewayRequestLog
	if err := db.First(&got, logs[0].ID).Error; err != nil {
		t.Fatalf("read updated log: %v", err)
	}
	if got.Cost != 0.01234567 || got.CostEstimated || got.CostSource != "sub2api_actual_cost" || got.UpstreamUsageID == nil || *got.UpstreamUsageID != "usage-1" || got.CostReconcileAt != nil {
		t.Fatalf("updated log = %+v", got)
	}
	if got.CacheCreationTokens != 5 || got.CacheReadTokens != 100 {
		t.Fatalf("updated cache tokens = %d/%d, want 5/100", got.CacheCreationTokens, got.CacheReadTokens)
	}
}

func TestClearIneligibleRequestCostReconcilesUsesBoundedBatches(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	logs := []*GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Error: "stream failed", CreatedAt: now.Add(time.Second)},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 503, CreatedAt: now.Add(2 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 3, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "stream failed", UpstreamAccepted: true, CreatedAt: now.Add(3 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 4, Method: "POST", Path: "/v1/images/generations", StatusCode: 499, RouteReason: "client_canceled", BillingCommitted: true, CreatedAt: now.Add(4 * time.Second)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatal(err)
		}
		if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	cleared, err := store.ClearIneligibleRequestCostReconciles(1)
	if err != nil || cleared != 1 {
		t.Fatalf("first cleanup = %d err=%v", cleared, err)
	}
	var firstFailed GatewayRequestLog
	if err := db.First(&firstFailed, logs[1].ID).Error; err != nil {
		t.Fatal(err)
	}
	if firstFailed.CostReconcileAt != nil || firstFailed.CostReconcileError != gatewayCostReconcileIneligibleError {
		t.Fatalf("first ineligible cleanup = %+v", firstFailed)
	}
	var successful GatewayRequestLog
	if err := db.First(&successful, logs[0].ID).Error; err != nil {
		t.Fatal(err)
	}
	if successful.CostReconcileAt == nil || successful.CostReconcileError != "" {
		t.Fatalf("successful reconcile was cleared: %+v", successful)
	}
	cleared, err = store.ClearIneligibleRequestCostReconciles(10)
	if err != nil || cleared != 1 {
		t.Fatalf("second cleanup = %d err=%v", cleared, err)
	}
	var failedStatus GatewayRequestLog
	if err := db.First(&failedStatus, logs[2].ID).Error; err != nil {
		t.Fatal(err)
	}
	if failedStatus.CostReconcileAt != nil || failedStatus.CostReconcileError != gatewayCostReconcileIneligibleError {
		t.Fatalf("failed-status cleanup = %+v", failedStatus)
	}
	for _, index := range []int{3, 4} {
		var retained GatewayRequestLog
		if err := db.First(&retained, logs[index].ID).Error; err != nil {
			t.Fatal(err)
		}
		if retained.CostReconcileAt == nil || retained.CostReconcileError != "" {
			t.Fatalf("explicit billing fact was cleared: %+v", retained)
		}
	}
}

func TestRequeueHistoricalRequestCostReconcilesOnlyEligibleOldLookupFailures(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	oldNotFound := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 10, UpstreamRequestID: "old-not-found", CreatedAt: now.Add(-48 * time.Hour),
	}
	oldScanIncomplete := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 10, UpstreamRequestID: "old-scan-incomplete", CreatedAt: now.Add(-36 * time.Hour),
	}
	oldDuplicate := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 10, UpstreamRequestID: "old-duplicate", CreatedAt: now.Add(-48 * time.Hour),
	}
	recentNotFound := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 10, UpstreamRequestID: "recent-not-found", CreatedAt: now.Add(-2 * time.Hour),
	}
	for _, log := range []*GatewayRequestLog{oldNotFound, oldScanIncomplete, oldDuplicate, recentNotFound} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatal(err)
		}
		if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct {
		log *GatewayRequestLog
		err string
	}{
		{oldNotFound, "sub2api request cost: usage record not found"},
		{oldScanIncomplete, "sub2api request cost: usage scan incomplete"},
		{oldDuplicate, "sub2api request cost: duplicate usage id"},
		{recentNotFound, "sub2api request cost: usage record not found"},
	} {
		var current GatewayRequestLog
		if err := db.First(&current, item.log.ID).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.MarkRequestCostReconcileFailedTyped(current.ID, GatewayRequestCostReconcileReasonRetryLimit, item.err); err != nil {
			t.Fatal(err)
		}
	}
	requeued, err := store.RequeueHistoricalRequestCostReconciles(10, now.Add(-7*24*time.Hour), now.Add(-24*time.Hour))
	if err != nil || requeued != 2 {
		t.Fatalf("historical requeue = %d err=%v, want 2", requeued, err)
	}
	for _, item := range []*GatewayRequestLog{oldNotFound, oldScanIncomplete} {
		var got GatewayRequestLog
		if err := db.First(&got, item.ID).Error; err != nil {
			t.Fatal(err)
		}
		if got.CostReconcileState != GatewayRequestCostReconcilePending || got.CostReconcileAt == nil || got.CostReconcileTries != 0 || got.CostReconcileError != "" {
			t.Fatalf("requeued historical request = %+v", got)
		}
		var event GatewayRequestCostReconcileEvent
		if err := db.Where("request_log_id = ? AND reason_code = ?", item.ID, GatewayRequestCostReconcileReasonHistoricalBackfill).First(&event).Error; err != nil {
			t.Fatalf("historical backfill event: %v", err)
		}
	}
	var duplicate, recent GatewayRequestLog
	if err := db.First(&duplicate, oldDuplicate.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&recent, recentNotFound.ID).Error; err != nil {
		t.Fatal(err)
	}
	if duplicate.CostReconcileAt != nil || recent.CostReconcileAt != nil {
		t.Fatalf("ineligible historical rows were requeued: duplicate=%+v recent=%+v", duplicate, recent)
	}
	requeued, err = store.RequeueHistoricalRequestCostReconciles(10, now.Add(-7*24*time.Hour), now.Add(-24*time.Hour))
	if err != nil || requeued != 0 {
		t.Fatalf("historical requeue repeated = %d err=%v, want 0", requeued, err)
	}
}

func TestChannelsListPageSortsByRealGatewayRequestCount(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	items := []*Channel{
		{Name: "unused", Type: ChannelTypeNewAPI, SiteURL: "https://unused.example", PasswordCipher: "x", MonitorEnabled: true, SortOrder: 9},
		{Name: "busy", Type: ChannelTypeNewAPI, SiteURL: "https://busy.example", PasswordCipher: "x", MonitorEnabled: true, SortOrder: 1},
		{Name: "medium", Type: ChannelTypeNewAPI, SiteURL: "https://medium.example", PasswordCipher: "x", MonitorEnabled: true, SortOrder: 1},
	}
	for _, channel := range items {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel %s: %v", channel.Name, err)
		}
	}
	for _, channelID := range []uint{items[1].ID, items[1].ID, items[1].ID, items[2].ID} {
		if err := store.AppendRequestLog(&GatewayRequestLog{DownstreamKeyID: 1, ChannelID: channelID, Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: time.Now()}); err != nil {
			t.Fatalf("append gateway request: %v", err)
		}
	}
	if err := store.AppendRequestLog(&GatewayRequestLog{ChannelID: items[0].ID, Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: "latency_probe", CreatedAt: time.Now()}); err != nil {
		t.Fatalf("append latency probe: %v", err)
	}
	if err := db.Create(&GatewayRequestLog{DownstreamKeyID: 1, ChannelID: items[2].ID, Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: time.Now()}).Error; err != nil {
		t.Fatalf("append legacy gateway request: %v", err)
	}
	if err := db.Create(&GatewayRequestLog{ChannelID: items[0].ID, Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: "latency_probe", CreatedAt: time.Now()}).Error; err != nil {
		t.Fatalf("append legacy latency probe: %v", err)
	}
	list, total, err := channels.ListPage(1, -1)
	if err != nil {
		t.Fatalf("list channels: %v", err)
	}
	if total != 3 || len(list) != 3 {
		t.Fatalf("channel page total=%d len=%d", total, len(list))
	}
	if list[0].ID != items[1].ID || list[0].RequestCount != 3 || list[1].ID != items[2].ID || list[1].RequestCount != 2 || list[2].ID != items[0].ID || list[2].RequestCount != 0 {
		t.Fatalf("channel request ranking = %+v", list)
	}
	all, err := channels.List()
	if err != nil {
		t.Fatalf("list all channels: %v", err)
	}
	counts := make(map[uint]int64, len(all))
	for _, channel := range all {
		counts[channel.ID] = channel.RequestCount
	}
	if counts[items[0].ID] != 0 || counts[items[1].ID] != 3 || counts[items[2].ID] != 2 {
		t.Fatalf("channel request counts = %+v", counts)
	}
}

func TestChannelsListSkipsRawFallbackWhenRequestAggregatesAreReady(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "aggregate-ready", Type: ChannelTypeNewAPI, SiteURL: "https://aggregate-ready.example", PasswordCipher: "x"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendRequestLog(&GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	ready, err := requestAggregatesReady(db)
	if err != nil || !ready {
		t.Fatalf("request aggregates ready=%t err=%v", ready, err)
	}

	var rawFallbackQueries atomic.Int64
	const callbackName = "test:channel-list-raw-fallback"
	if err := db.Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == "channels" && strings.Contains(tx.Statement.SQL.String(), "gateway_request_logs") {
			rawFallbackQueries.Add(1)
		}
	}); err != nil {
		t.Fatalf("register query counter: %v", err)
	}
	t.Cleanup(func() { _ = db.Callback().Query().Remove(callbackName) })

	list, err := channels.List()
	if err != nil || len(list) != 1 || list[0].RequestCount != 1 {
		t.Fatalf("aggregate-ready channel list=%+v err=%v", list, err)
	}
	if rawFallbackQueries.Load() != 0 {
		t.Fatalf("aggregate-ready channel list used %d raw fallback queries", rawFallbackQueries.Load())
	}

	if err := db.Create(&GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		AggregateVersion: gatewayRequestAggregateVersion - 1, CreatedAt: time.Now(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	list, err = channels.List()
	if err != nil || len(list) != 1 || list[0].RequestCount != 2 {
		t.Fatalf("migrating channel list=%+v err=%v", list, err)
	}
	if rawFallbackQueries.Load() != 1 {
		t.Fatalf("migrating channel list raw fallback queries=%d, want 1", rawFallbackQueries.Load())
	}
}

func TestGatewayCandidateGroupCRUDAndReferencedDelete(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "candidate", Type: ChannelTypeNewAPI, SiteURL: "https://candidate.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "cheap", Ratio: 0.5}); err != nil {
		t.Fatalf("create rate: %v", err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "cheap")
	group := &GatewayCandidateGroup{Name: "low cost"}
	if err := store.SaveCandidateGroup(&GatewayCandidateGroup{Name: "empty"}, nil); err == nil {
		t.Fatal("empty candidate group was accepted")
	}
	members := []GatewayCandidateGroupMember{
		{ChannelID: channel.ID, GroupName: "cheap"},
		{ChannelID: channel.ID, GroupName: "cheap"},
	}
	if err := store.SaveCandidateGroup(group, members); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	got, err := store.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatalf("find candidate group: %v", err)
	}
	if got.Name != "low cost" || len(got.Members) != 1 || got.Members[0].GroupName != "cheap" {
		t.Fatalf("candidate group = %+v", got)
	}
	if err := store.DeleteGroupRoutingState(channel.ID, "cheap"); err != nil {
		t.Fatalf("delete group routing state: %v", err)
	}
	got, err = store.FindCandidateGroup(group.ID)
	if err != nil || len(got.Members) != 0 {
		t.Fatalf("removed upstream group left candidate members: group=%+v err=%v", got, err)
	}
	key := &GatewayDownstreamKey{Name: "client", App: GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID, KeyHash: "candidate-group-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	if err := store.DeleteCandidateGroup(group.ID); !errors.Is(err, ErrCandidateGroupInUse) {
		t.Fatalf("referenced delete error = %v", err)
	}
	key.CandidateGroupID = 0
	if err := store.UpdateDownstreamKey(key); err != nil {
		t.Fatalf("clear candidate group: %v", err)
	}
	if err := store.DeleteCandidateGroup(group.ID); err != nil {
		t.Fatalf("delete candidate group: %v", err)
	}
	if _, err := store.FindCandidateGroup(group.ID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted candidate group error = %v", err)
	}
	key.CandidateGroupID = group.ID
	if err := store.UpdateDownstreamKey(key); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("dangling candidate group assignment error = %v", err)
	}
}

func TestGatewayCandidateGroupAllowsProbeDisabledMembers(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "probe-groups", Type: ChannelTypeNewAPI, SiteURL: "https://candidate.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, groupName := range []string{"existing", "new-disabled"} {
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}); err != nil {
			t.Fatalf("create rate %s: %v", groupName, err)
		}
		if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: groupName, OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", Enabled: true}); err != nil {
			t.Fatalf("create upstream key %s: %v", groupName, err)
		}
	}
	group := &GatewayCandidateGroup{Name: "existing membership"}
	existingMember := GatewayCandidateGroupMember{ChannelID: channel.ID, GroupName: "existing"}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{existingMember}); err != nil {
		t.Fatalf("create candidate group: %v", err)
	}
	for _, groupName := range []string{"existing", "new-disabled"} {
		if err := store.UpdateLatencyProbeEnabled(channel.ID, groupName, false); err != nil {
			t.Fatalf("disable probe %s: %v", groupName, err)
		}
	}
	group.Name = "renamed with warning"
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{existingMember}); err != nil {
		t.Fatalf("retain existing disabled member: %v", err)
	}
	newDisabled := GatewayCandidateGroupMember{ChannelID: channel.ID, GroupName: "new-disabled"}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{existingMember, newDisabled}); err != nil {
		t.Fatalf("add disabled member: %v", err)
	}
	stored, err := store.FindCandidateGroup(group.ID)
	if err != nil || stored.Name != "renamed with warning" || len(stored.Members) != 2 {
		t.Fatalf("candidate group after disabled member update = %+v err=%v", stored, err)
	}
	if err := store.SaveCandidateGroup(&GatewayCandidateGroup{Name: "new disabled"}, []GatewayCandidateGroupMember{newDisabled}); err != nil {
		t.Fatalf("create candidate group with disabled member: %v", err)
	}
}

func TestGatewayStorageKeepsManualRouteInsideCandidateGroup(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "manual-route", Type: ChannelTypeNewAPI, SiteURL: "https://manual-route.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, groupName := range []string{"fixed", "other"} {
		if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}).Error; err != nil {
			t.Fatalf("create rate %s: %v", groupName, err)
		}
		createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, groupName)
	}
	primary := &GatewayCandidateGroup{Name: "primary"}
	primaryMembers := []GatewayCandidateGroupMember{
		{ChannelID: channel.ID, GroupName: "fixed"},
		{ChannelID: channel.ID, GroupName: "other"},
	}
	if err := store.SaveCandidateGroup(primary, primaryMembers); err != nil {
		t.Fatalf("create primary candidate group: %v", err)
	}
	secondary := &GatewayCandidateGroup{Name: "secondary"}
	if err := store.SaveCandidateGroup(secondary, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "other"}}); err != nil {
		t.Fatalf("create secondary candidate group: %v", err)
	}
	key := &GatewayDownstreamKey{
		Name: "manual", App: GatewayAppCodex, ModelFamily: "codex",
		CandidateGroupID: primary.ID, KeyHash: "manual-route-hash", APIKeyCipher: "cipher", KeyPrefix: "sk-test",
		SmartRouting: true, ManualChannelID: channel.ID, ManualGroupName: "fixed", Enabled: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create manual downstream key: %v", err)
	}
	key.SmartRouting = false
	key.ManualGroupName = " FIXED "
	if err := store.UpdateDownstreamKey(key); err != nil {
		t.Fatalf("enable canonical case-variant manual route: %v", err)
	}

	moved := *key
	moved.CandidateGroupID = secondary.ID
	if err := store.UpdateDownstreamKey(&moved); err == nil || !strings.Contains(err.Error(), "not in candidate group") {
		t.Fatalf("invalid candidate group move error = %v", err)
	}
	stored, err := store.FindDownstreamKey(key.ID)
	if err != nil || stored.CandidateGroupID != primary.ID {
		t.Fatalf("manual key changed after rejected move = %+v err=%v", stored, err)
	}

	primary.Name = "primary"
	if err := store.SaveCandidateGroup(primary, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "other"}}); err == nil || !strings.Contains(err.Error(), "must keep manual route") {
		t.Fatalf("manual member removal error = %v", err)
	}
	unchanged, err := store.FindCandidateGroup(primary.ID)
	if err != nil || len(unchanged.Members) != 2 {
		t.Fatalf("candidate members changed after rejected removal = %+v err=%v", unchanged, err)
	}
}

func TestGatewayManualRouteValidityIgnoresProbeAndSmartHealthState(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "manual-direct", Type: ChannelTypeNewAPI, SiteURL: "https://manual-direct.example",
		MonitorEnabled: false, LastError: "monitor failed",
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "fixed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	cooldownUntil := time.Now().Add(time.Hour)
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "fixed", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "cipher", Enabled: true,
		LatencyProbeEnabled: false, LastRoutingFailureKind: "http_status", RoutingCooldownUntil: &cooldownUntil,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "fixed", false); err != nil {
		t.Fatal(err)
	}
	group := &GatewayCandidateGroup{Name: "manual direct"}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "fixed"}}); err != nil {
		t.Fatal(err)
	}
	key := &GatewayDownstreamKey{
		Name: "manual direct", App: GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
		ManualChannelID: channel.ID, ManualGroupName: "fixed", KeyHash: "manual-direct-key", KeyPrefix: "sk",
		Enabled: true, SmartRouting: false,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatal(err)
	}
	list, err := store.ListDownstreamKeys()
	if err != nil || len(list) != 1 {
		t.Fatalf("manual downstream keys = %+v err=%v", list, err)
	}
	if list[0].ManualRouteInvalid {
		t.Fatalf("probe or smart health state invalidated fixed route: %+v", list[0])
	}
	if err := db.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND group_name = ?", channel.ID, "fixed").Update("provisioning", true).Error; err != nil {
		t.Fatal(err)
	}
	list, err = store.ListDownstreamKeys()
	if err != nil || len(list) != 1 || !list[0].ManualRouteInvalid {
		t.Fatalf("provisioning key remained a valid fixed route: %+v err=%v", list, err)
	}
}

func TestChannelDeletePrepareRetainsUpstreamGroupChangeHistory(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "removed", Type: ChannelTypeNewAPI, SiteURL: "https://removed.example", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "added-group", "added", 1, "BOG - added", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "removed-group", "removed", 2, "BOG - removed", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion: %v", err)
	}
	var count int64
	if err := db.Model(&GatewayUpstreamGroupChange{}).Where("channel_id = ?", channel.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("delete prepare changed upstream group history = %d", count)
	}
}

func TestRemovedUpstreamGroupChangeIsAutomaticallyAcknowledged(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "auto-cleanup", Type: ChannelTypeNewAPI, SiteURL: "https://auto-cleanup.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	detectedAt := time.Now().Add(-time.Minute).Round(time.Second)
	if err := store.RecordUpstreamGroupChange(channel.ID, "removed-group", "removed", 23, "BOG - removed-group", detectedAt); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListUpstreamGroupChanges(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("removed change still pending = %+v", pending)
	}
	history, total, err := store.ListUpstreamGroupChangesHistoryPage(1, 50)
	if err != nil || total != 1 || len(history) != 1 {
		t.Fatalf("removed history total=%d items=%+v err=%v", total, history, err)
	}
	if history[0].Status != "acknowledged" || history[0].ResolvedAt == nil || history[0].UpstreamKeyID != 23 {
		t.Fatalf("removed history = %+v", history[0])
	}
}

func TestLegacyManagedUpstreamKeyRemovalAPIsFailClosed(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "removal-claim", Type: ChannelTypeNewAPI, SiteURL: "https://removal.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if change, claimed, err := store.BeginManagedUpstreamKeyRemoval(
		channel.ID, "vip", 23, "BOG - vip", time.Now(), 10*time.Minute,
	); !errors.Is(err, ErrLegacyManagedUpstreamKeyRemovalUnsupported) || claimed || change != nil {
		t.Fatalf("legacy begin change=%+v claimed=%v err=%v", change, claimed, err)
	}
	if err := store.CompleteManagedUpstreamKeyRemoval(1, true, time.Now()); !errors.Is(err, ErrLegacyManagedUpstreamKeyRemovalUnsupported) {
		t.Fatalf("legacy complete error = %v", err)
	}
	if err := store.ReconcileManagedUpstreamKeyRemovals(channel.ID, map[int64]struct{}{}, time.Now()); !errors.Is(err, ErrLegacyManagedUpstreamKeyRemovalUnsupported) {
		t.Fatalf("legacy reconcile error = %v", err)
	}
	var changes int64
	if err := db.Model(&GatewayUpstreamGroupChange{}).Where("channel_id = ?", channel.ID).Count(&changes).Error; err != nil {
		t.Fatal(err)
	}
	if changes != 0 {
		t.Fatalf("legacy removal APIs persisted %d change rows", changes)
	}
}

func TestAssignStaleAddedUpstreamGroupChangeDoesNotRestoreRemovedMember(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "stale", Type: ChannelTypeNewAPI, SiteURL: "https://stale.example", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, groupName := range []string{"promo", "seed"} {
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: groupName, OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
			APIKeyCipher: "cipher", Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	group := &GatewayCandidateGroup{Name: "target"}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	changes, err := store.ListUpstreamGroupChanges(false)
	if err != nil || len(changes) != 1 {
		t.Fatalf("added changes=%+v err=%v", changes, err)
	}
	addedID := changes[0].ID
	if err := rates.DeleteSnapshot(channel.ID, "promo"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteGroupRoutingState(channel.ID, "promo"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "removed", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	var staleAdded GatewayUpstreamGroupChange
	if err := db.First(&staleAdded, addedID).Error; err != nil {
		t.Fatalf("load stale added change before assignment: %v", err)
	}
	if staleAdded.Status != "superseded" {
		t.Fatalf("stale added change status before assignment = %q", staleAdded.Status)
	}
	if err := store.AssignUpstreamGroupChange(addedID, []uint{group.ID}, time.Now()); !errors.Is(err, ErrUpstreamGroupChangeSuperseded) {
		t.Fatalf("assign stale added change error = %v", err)
	}
	updated, err := store.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(updated.Members) != 1 || updated.Members[0].GroupName != "seed" {
		t.Fatalf("stale change modified candidate members = %+v", updated.Members)
	}
	var added GatewayUpstreamGroupChange
	if err := db.First(&added, addedID).Error; err != nil {
		t.Fatal(err)
	}
	if added.Status != "superseded" {
		t.Fatalf("stale added change status = %q", added.Status)
	}
}

func TestAssignUpstreamGroupChangeAllowsProbeDisabledGroup(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "disabled-added", Type: ChannelTypeNewAPI, SiteURL: "https://disabled.example", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for index, groupName := range []string{"existing", "added"} {
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: groupName, UpstreamKeyID: int64(index + 1), UpstreamKeyName: "managed-" + groupName, OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", Enabled: true}); err != nil {
			t.Fatal(err)
		}
	}
	target := &GatewayCandidateGroup{Name: "target"}
	if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "existing"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "added", false); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "added", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 1 {
		t.Fatalf("pending changes = %+v err=%v", changes, err)
	}
	if err := store.AssignUpstreamGroupChange(changes[0].ID, []uint{target.ID}, time.Now()); err != nil {
		t.Fatalf("assign disabled group: %v", err)
	}
	stored, err := store.FindCandidateGroup(target.ID)
	if err != nil || len(stored.Members) != 2 {
		t.Fatalf("candidate group after disabled assign = %+v err=%v", stored, err)
	}
	change, err := store.FindUpstreamGroupChange(changes[0].ID)
	if err != nil || change.Status != "assigned" || change.AssignmentReasonCode != "manual_override_incomplete_facts" {
		t.Fatalf("change after disabled assign = %+v err=%v", change, err)
	}
}

func TestGatewayKeyGroupCRUDAndReferencedDelete(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	group := &GatewayKeyGroup{Name: "Dev"}
	if err := store.SaveKeyGroup(group); err != nil {
		t.Fatalf("create key group: %v", err)
	}
	group.Name = "Dev workstation"
	if err := store.SaveKeyGroup(group); err != nil {
		t.Fatalf("rename key group: %v", err)
	}
	key := &GatewayDownstreamKey{Name: "Claude", App: GatewayAppClaude, ModelFamily: "claude", KeyGroupID: group.ID, KeyHash: "key-group-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create grouped key: %v", err)
	}
	list, err := store.ListDownstreamKeys()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].KeyGroupName != "Dev workstation" {
		t.Fatalf("grouped keys = %+v", list)
	}
	if err := store.DeleteKeyGroup(group.ID); !errors.Is(err, ErrKeyGroupInUse) {
		t.Fatalf("referenced delete error = %v", err)
	}
	key.KeyGroupID = 0
	if err := store.UpdateDownstreamKey(key); err != nil {
		t.Fatalf("clear key group: %v", err)
	}
	if err := store.DeleteKeyGroup(group.ID); err != nil {
		t.Fatalf("delete key group: %v", err)
	}
	key.KeyGroupID = group.ID
	if err := store.UpdateDownstreamKey(key); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("dangling key group assignment error = %v", err)
	}
}

func TestGatewayKeyGroupAndKeyNamesAreUniqueWithinGroup(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	bee := &GatewayKeyGroup{Name: "Dev"}
	if err := store.SaveKeyGroup(bee); err != nil {
		t.Fatalf("create Dev group: %v", err)
	}
	if err := store.SaveKeyGroup(&GatewayKeyGroup{Name: " dev "}); err == nil {
		t.Fatal("case-insensitive duplicate key group was created")
	}
	owl := &GatewayKeyGroup{Name: "Owl"}
	if err := store.SaveKeyGroup(owl); err != nil {
		t.Fatalf("create Owl group: %v", err)
	}
	owl.Name = "DEV"
	if err := store.SaveKeyGroup(owl); err == nil {
		t.Fatal("key group was renamed to a duplicate name")
	}

	newKey := func(name string, groupID uint, hash string) *GatewayDownstreamKey {
		return &GatewayDownstreamKey{
			Name:         name,
			App:          GatewayAppCodex,
			ModelFamily:  "codex",
			KeyGroupID:   groupID,
			KeyHash:      hash,
			KeyPrefix:    "sk",
			Enabled:      true,
			SmartRouting: true,
		}
	}
	beeKey := newKey("Claude", bee.ID, "unique-key-bee")
	if err := store.CreateDownstreamKey(beeKey); err != nil {
		t.Fatalf("create Dev key: %v", err)
	}
	if err := store.CreateDownstreamKey(newKey("claude", bee.ID, "unique-key-bee-duplicate")); err == nil {
		t.Fatal("case-insensitive duplicate key was created in one group")
	}
	owlKey := newKey("CLAUDE", owl.ID, "unique-key-owl")
	if err := store.CreateDownstreamKey(owlKey); err != nil {
		t.Fatalf("same key name in another group should succeed: %v", err)
	}
	owlKey.KeyGroupID = bee.ID
	if err := store.UpdateDownstreamKey(owlKey); err == nil {
		t.Fatal("moving a key into a group with the same name succeeded")
	}
	if err := store.CreateDownstreamKey(newKey("default", 0, "unique-key-ungrouped")); err != nil {
		t.Fatalf("create ungrouped key: %v", err)
	}
	if err := store.CreateDownstreamKey(newKey("DEFAULT", 0, "unique-key-ungrouped-duplicate")); err == nil {
		t.Fatal("case-insensitive duplicate ungrouped key was created")
	}
}

func TestCandidateGroupMemberChangesDoNotRequireManualMappedModelFacts(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)

	createMember := func(name, modelIDs string) (*Channel, GatewayCandidateGroupMember) {
		t.Helper()
		channel := &Channel{Name: name, Type: ChannelTypeNewAPI, SiteURL: "https://" + name + ".example", PasswordCipher: "cipher", MonitorEnabled: true}
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: name, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: name, APIKeyCipher: "cipher", APIKeyPrefix: "sk",
			OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
			ModelFamilies:       "codex", ModelIDs: modelIDs, Enabled: true, LatencyProbeEnabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, name, "codex", modelIDs, time.Now(), ""); err != nil {
			t.Fatal(err)
		}
		return channel, GatewayCandidateGroupMember{ChannelID: channel.ID, GroupName: name}
	}
	_, supporting := createMember("supports-fixed", "gpt-5,GPT-5.4")
	unsupportedChannel, unsupported := createMember("other-model", "gpt-4.1")
	group := &GatewayCandidateGroup{Name: "manual-model", ModelMappingFamily: "codex"}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{supporting, unsupported}); err != nil {
		t.Fatal(err)
	}
	key := &GatewayDownstreamKey{
		Name: "manual-mapped", App: GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
		ModelMappingStrategy: "manual", ModelMappingModel: "gpt-5.4", ModelMappingReasoningEffort: "high",
		KeyHash: "manual-model-member-change", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{supporting}); err != nil {
		t.Fatalf("case-insensitive supporting member was rejected: %v", err)
	}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{supporting, unsupported}); err != nil {
		t.Fatalf("restore mixed members: %v", err)
	}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{unsupported}); err != nil {
		t.Fatalf("membership edit was blocked by mapped-model capability: %v", err)
	}
	if err := store.CandidateMembersSupportManualMappedModel([]GatewayCandidateGroupMember{unsupported}, "gpt-5.4"); err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("strict mapped-model validation error = %v", err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(unsupportedChannel.ID, unsupported.GroupName, "codex", "", time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	if err := store.CandidateMembersSupportManualMappedModel([]GatewayCandidateGroupMember{unsupported}, "gpt-5.4"); err == nil || !strings.Contains(err.Error(), "no synchronized model IDs") {
		t.Fatalf("strict empty mapped-model facts error = %v", err)
	}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{unsupported}); err != nil {
		t.Fatalf("unsynced membership edit was blocked: %v", err)
	}
	stored, err := store.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Members) != 1 || stored.Members[0].ChannelID != unsupported.ChannelID || stored.Members[0].GroupName != unsupported.GroupName {
		t.Fatalf("membership edit was not committed: %+v", stored.Members)
	}
}

func TestCandidateMembersSupportManualMappedModelUsesDirectModelsExactly(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)

	createMember := func(channel *Channel, groupName, modelIDs string) GatewayCandidateGroupMember {
		t.Helper()
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: groupName, APIKeyCipher: "cipher", APIKeyPrefix: "sk",
			OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
			ModelFamilies:       "codex", ModelIDs: modelIDs, Enabled: true, LatencyProbeEnabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, groupName, "codex", modelIDs, time.Now(), ""); err != nil {
			t.Fatal(err)
		}
		return GatewayCandidateGroupMember{ChannelID: channel.ID, GroupName: groupName}
	}

	direct := createMember(&Channel{
		Name: "direct-exact-models", Type: ChannelTypeNewAPI, SiteURL: "https://direct-exact-models.example",
		PasswordCipher: "cipher", CredentialMode: CredentialModeDirect,
		GatewayApp: GatewayAppClaude, DirectModels: EncodeDirectModels([]string{"gpt-5.4"}), MonitorEnabled: true,
	}, DirectChannelGroupName, "GPT-5.4,upstream-only")
	if err := store.CandidateMembersSupportManualMappedModel([]GatewayCandidateGroupMember{direct}, "gpt-5.4"); err != nil {
		t.Fatalf("direct exact selected model was rejected: %v", err)
	}
	for _, unsupported := range []string{"GPT-5.4", "upstream-only"} {
		if err := store.CandidateMembersSupportManualMappedModel([]GatewayCandidateGroupMember{direct}, unsupported); err == nil || !strings.Contains(err.Error(), "does not support") {
			t.Fatalf("direct model %q validation error = %v", unsupported, err)
		}
	}
	if err := store.CandidateMembersSupportMappingFamily([]GatewayCandidateGroupMember{direct}, "claude"); err != nil {
		t.Fatalf("current direct app family was rejected: %v", err)
	}
	if err := store.CandidateMembersSupportMappingFamily([]GatewayCandidateGroupMember{direct}, "codex"); err == nil {
		t.Fatal("stale direct model-family facts were accepted")
	}

	managed := createMember(&Channel{
		Name: "managed-case-folded-models", Type: ChannelTypeNewAPI, SiteURL: "https://managed-case-folded-models.example",
		PasswordCipher: "cipher", CredentialMode: CredentialModePassword, MonitorEnabled: true,
	}, "managed", "GPT-5.4")
	if err := store.CandidateMembersSupportManualMappedModel([]GatewayCandidateGroupMember{managed}, "gpt-5.4"); err != nil {
		t.Fatalf("managed case-insensitive model was rejected: %v", err)
	}
}

func TestCandidateMappingFactsStayStrictWhileMembershipAllowsCurrentUnsyncedGeneration(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "mapping-generation", Type: ChannelTypeNewAPI, SiteURL: "https://mapping-generation.example", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "premium", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "premium", UpstreamKeyID: 1, UpstreamKeyName: "managed", APIKeyCipher: "cipher", APIKeyPrefix: "sk",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		ModelFamilies:       "claude", ModelIDs: "claude-sonnet",
		Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	discoveredAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "premium", "added", 1, "managed", discoveredAt); err != nil {
		t.Fatal(err)
	}
	firstGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "premium")
	if err != nil {
		t.Fatal(err)
	}
	initialSyncedAt := firstGeneration.DiscoveredAt.Add(time.Second)
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "premium", "codex", "gpt-5.4", initialSyncedAt, ""); err != nil {
		t.Fatal(err)
	}
	members := []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "premium"}}
	group := &GatewayCandidateGroup{Name: "mapping-generation", ModelMappingFamily: "codex"}
	if err := store.SaveCandidateGroup(group, members); err != nil {
		t.Fatalf("save current-generation candidate group: %v", err)
	}
	if err := store.CandidateMembersSupportManualMappedModel(members, "gpt-5.4"); err != nil {
		t.Fatalf("current generation model support: %v", err)
	}

	removedAt := initialSyncedAt.Add(time.Second)
	if err := store.RecordUpstreamGroupChange(channel.ID, "premium", "removed", 1, "managed", removedAt); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "premium", "added", 2, "managed-2", removedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "premium", UpstreamKeyID: 2, UpstreamKeyName: "managed-2",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher-2", APIKeyPrefix: "sk", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "premium", "codex", "gpt-5.4", removedAt.Add(1500*time.Millisecond), "temporary model sync failure"); err != nil {
		t.Fatal(err)
	}
	if err := store.CandidateMembersSupportMappingFamily(members, "codex"); err == nil || !strings.Contains(err.Error(), "no member") {
		t.Fatalf("stale family facts survived failed current-generation sync: %v", err)
	}
	if err := store.CandidateMembersSupportManualMappedModel(members, "gpt-5.4"); err == nil || !strings.Contains(err.Error(), "no synchronized model IDs") {
		t.Fatalf("stale model facts survived failed current-generation sync: %v", err)
	}
	if err := store.SaveCandidateGroup(group, members); err != nil {
		t.Fatalf("membership edit was blocked by stale mapping facts: %v", err)
	}

	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "premium", "codex", "gpt-5.4", removedAt.Add(2*time.Second), ""); err != nil {
		t.Fatal(err)
	}
	if err := store.CandidateMembersSupportMappingFamily(members, "codex"); err != nil {
		t.Fatalf("current generation resync did not restore mapping facts: %v", err)
	}
}

func TestUpstreamModelFactsIgnoreOlderSameGenerationObservations(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "monotonic-model-facts", Type: ChannelTypeNewAPI, SiteURL: "https://monotonic-model-facts.example", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	discoveredAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "premium", Ratio: 1, LastSeenAt: discoveredAt}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "premium", APIKeyCipher: "cipher", APIKeyPrefix: "sk",
		Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "premium", "added", 1, "managed", discoveredAt); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "premium")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkModelCapabilityForApp(channel.ID, "premium", "claude-sonnet", GatewayAppClaude, true, "", generation); err != nil {
		t.Fatal(err)
	}
	var unsynchronizedRefresh GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "premium").First(&unsynchronizedRefresh).Error; err != nil {
		t.Fatal(err)
	}
	if unsynchronizedRefresh.ModelsSyncedAt != nil || unsynchronizedRefresh.FactsComplete || len(unsynchronizedRefresh.ModelIDs) != 0 {
		t.Fatalf("capability refresh promoted unsynchronized key model fields: %+v", unsynchronizedRefresh)
	}
	newer := discoveredAt.Add(20 * time.Second)
	older := discoveredAt.Add(10 * time.Second)
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "premium", "codex", "gpt-5.4", newer, "", generation); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkModelCapability(channel.ID, "premium", "gpt-5.4", true, "", generation); err != nil {
		t.Fatal(err)
	}
	var capabilityRefresh GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "premium").First(&capabilityRefresh).Error; err != nil {
		t.Fatal(err)
	}
	if capabilityRefresh.ModelsSyncedAt == nil || !capabilityRefresh.ModelsSyncedAt.Equal(newer) {
		t.Fatalf("capability refresh changed model observation time: got %v, want %v", capabilityRefresh.ModelsSyncedAt, newer)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "premium", "claude", "claude-sonnet", older, "late stale result", generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamKeyModelsError(channel.ID, "premium", older, "late stale failure", generation); err != nil {
		t.Fatal(err)
	}
	if err := store.SetUpstreamKeyModelsError(channel.ID, "premium", older, "late stale legacy failure"); err != nil {
		t.Fatal(err)
	}

	var key GatewayUpstreamKey
	if err := db.Where("channel_id = ? AND group_name = ?", channel.ID, "premium").First(&key).Error; err != nil {
		t.Fatal(err)
	}
	if key.ModelFamilies != "codex" || key.ModelIDs != "gpt-5.4" || key.ModelsError != "" {
		t.Fatalf("older observation overwrote upstream key facts: %+v", key)
	}
	if key.ModelsSyncedAt == nil || !key.ModelsSyncedAt.Equal(newer) {
		t.Fatalf("upstream key models_synced_at = %v, want %v", key.ModelsSyncedAt, newer)
	}

	var identity GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "premium").First(&identity).Error; err != nil {
		t.Fatal(err)
	}
	if strings.Join(identity.ModelFamilyCodes, ",") != "codex" || strings.Join(identity.ModelIDs, ",") != "gpt-5.4" || !identity.FactsComplete {
		t.Fatalf("older observation overwrote identity facts: %+v", identity)
	}
	if identity.ModelsSyncedAt == nil || !identity.ModelsSyncedAt.Equal(newer) {
		t.Fatalf("identity models_synced_at = %v, want %v", identity.ModelsSyncedAt, newer)
	}
}

func TestPatchDownstreamKeyReloadsAfterConcurrentMappingDisable(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	key := &GatewayDownstreamKey{
		Name: "client", App: GatewayAppCodex, ModelFamily: "codex",
		ModelMappingStrategy: "balanced", KeyHash: "concurrent-patch-key", KeyPrefix: "sk",
		Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	firstApply := make(chan struct{})
	releasePatch := make(chan struct{})
	patchDone := make(chan error, 1)
	var applyCalls atomic.Int32
	go func() {
		_, err := store.PatchDownstreamKey(key.ID, func(current *GatewayDownstreamKey) error {
			current.Name = "renamed"
			if applyCalls.Add(1) == 1 {
				close(firstApply)
				<-releasePatch
			}
			return nil
		}, nil)
		patchDone <- err
	}()

	<-firstApply
	if err := store.DisableDownstreamKeyModelMapping(key.ID); err != nil {
		t.Fatalf("disable mapping during patch: %v", err)
	}
	close(releasePatch)
	if err := <-patchDone; err != nil {
		t.Fatalf("patch downstream key: %v", err)
	}

	stored, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("find downstream key: %v", err)
	}
	if stored.Name != "renamed" || stored.ModelMappingStrategy != "off" {
		t.Fatalf("concurrent fields were overwritten: %+v", stored)
	}
	if applyCalls.Load() != 2 {
		t.Fatalf("patch apply calls = %d, want prospective and locked reload", applyCalls.Load())
	}
}

func TestUpdateDownstreamKeyRejectsStaleSnapshot(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	key := &GatewayDownstreamKey{
		Name: "client", App: GatewayAppCodex, ModelFamily: "codex",
		ModelMappingStrategy: "balanced", KeyHash: "stale-update-key", KeyPrefix: "sk",
		Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	stale, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("find stale snapshot: %v", err)
	}
	time.Sleep(time.Millisecond)
	if err := store.DisableDownstreamKeyModelMapping(key.ID); err != nil {
		t.Fatalf("disable mapping: %v", err)
	}
	stale.Name = "stale rename"
	if err := store.UpdateDownstreamKey(stale); !errors.Is(err, ErrDownstreamKeyUpdateConflict) {
		t.Fatalf("stale update error = %v", err)
	}
	stored, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("find downstream key: %v", err)
	}
	if stored.Name != "client" || stored.ModelMappingStrategy != "off" {
		t.Fatalf("stale update changed current fields: %+v", stored)
	}
}

func TestCandidateGroupFamilyWinsBeforeConcurrentMappingEnableAcrossConnections(t *testing.T) {
	base, group, key, members := candidateGroupMappingConcurrencyFixture(t, "off")
	dbA, err := testdb.ReopenWithError(t, base)
	if err != nil {
		t.Fatal(err)
	}
	dbB, err := testdb.ReopenWithError(t, base)
	if err != nil {
		t.Fatal(err)
	}
	storeA := NewGateway(dbA)
	storeB := NewGateway(dbB)

	updatedGroup := *group
	updatedGroup.ModelMappingFamily = "claude"
	groupLocked := make(chan struct{})
	releaseGroup := make(chan struct{})
	groupDone := make(chan error, 1)
	go func() {
		groupDone <- storeA.saveCandidateGroup(&updatedGroup, members, func() {
			close(groupLocked)
			<-releaseGroup
		})
	}()
	<-groupLocked

	patchStarted := make(chan struct{})
	patchDone := make(chan error, 1)
	go func() {
		close(patchStarted)
		_, err := storeB.PatchDownstreamKey(key.ID, func(current *GatewayDownstreamKey) error {
			current.ModelMappingStrategy = "balanced"
			return nil
		}, validateCandidateGroupFamilyForMappedKey)
		patchDone <- err
	}()
	<-patchStarted
	close(releaseGroup)

	if err := <-groupDone; err != nil {
		t.Fatalf("save Claude candidate group: %v", err)
	}
	if err := <-patchDone; err == nil {
		t.Fatal("Codex mapping enabled after candidate group changed to Claude")
	}
	storedGroup, err := storeA.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	storedKey, err := storeA.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedGroup.ModelMappingFamily != "claude" || storedKey.ModelMappingStrategy != "off" {
		t.Fatalf("serialized state is inconsistent: group=%+v key=%+v", storedGroup, storedKey)
	}
}

func TestMappedKeyWinsBeforeConcurrentCandidateFamilyChangeAcrossConnections(t *testing.T) {
	base, group, key, members := candidateGroupMappingConcurrencyFixture(t, "balanced")
	dbA, err := testdb.ReopenWithError(t, base)
	if err != nil {
		t.Fatal(err)
	}
	dbB, err := testdb.ReopenWithError(t, base)
	if err != nil {
		t.Fatal(err)
	}
	storeA := NewGateway(dbA)
	storeB := NewGateway(dbB)

	keyLocked := make(chan struct{})
	releaseKey := make(chan struct{})
	patchDone := make(chan error, 1)
	go func() {
		_, err := storeA.PatchDownstreamKey(key.ID, func(current *GatewayDownstreamKey) error {
			current.Name = "renamed"
			return nil
		}, func(_ *Gateway, _ *GatewayDownstreamKey) error {
			close(keyLocked)
			<-releaseKey
			return nil
		})
		patchDone <- err
	}()
	<-keyLocked

	updatedGroup := *group
	updatedGroup.ModelMappingFamily = "claude"
	groupStarted := make(chan struct{})
	groupDone := make(chan error, 1)
	go func() {
		close(groupStarted)
		groupDone <- storeB.SaveCandidateGroup(&updatedGroup, members)
	}()
	<-groupStarted
	close(releaseKey)

	if err := <-patchDone; err != nil {
		t.Fatalf("patch mapped key: %v", err)
	}
	if err := <-groupDone; err == nil || !strings.Contains(err.Error(), "mapped key") {
		t.Fatalf("candidate family change error = %v", err)
	}
	storedGroup, err := storeA.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	storedKey, err := storeA.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedGroup.ModelMappingFamily != "codex" || storedKey.Name != "renamed" || storedKey.ModelMappingStrategy != "balanced" {
		t.Fatalf("serialized state is inconsistent: group=%+v key=%+v", storedGroup, storedKey)
	}
}

func candidateGroupMappingConcurrencyFixture(t *testing.T, mappingStrategy string) (*gorm.DB, *GatewayCandidateGroup, *GatewayDownstreamKey, []GatewayCandidateGroupMember) {
	t.Helper()
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "mapping-race", Type: ChannelTypeNewAPI, SiteURL: "https://mapping-race.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "premium", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "premium", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "cipher", Enabled: true,
		LatencyProbeEnabled: true, ModelFamilies: "codex,claude", ModelIDs: "gpt-5,claude-sonnet",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "premium", "codex,claude", "gpt-5,claude-sonnet", time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	members := []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "premium"}}
	group := &GatewayCandidateGroup{Name: "mapping-race", ModelMappingFamily: "codex"}
	if err := store.SaveCandidateGroup(group, members); err != nil {
		t.Fatal(err)
	}
	key := &GatewayDownstreamKey{
		Name: "mapped", App: GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
		ModelMappingStrategy: mappingStrategy, KeyHash: "mapping-race-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatal(err)
	}
	return db, group, key, members
}

func validateCandidateGroupFamilyForMappedKey(store *Gateway, key *GatewayDownstreamKey) error {
	group, err := store.FindCandidateGroup(key.CandidateGroupID)
	if err != nil {
		return err
	}
	if !candidateGroupMappingFamilyMatchesKey(group.ModelMappingFamily, key) {
		return errors.New("model mapping key and candidate group must use the same model family")
	}
	return nil
}

func TestUpdateRequestLogActualCostUsesFundingSnapshot(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	factor := 0.5
	log := &GatewayRequestLog{
		ChannelID: 1, GroupName: "funded", FundingFactor: &factor,
		Method: "POST", Path: "/v1/responses", StatusCode: 200,
		Cost: 0.1, CostEstimated: true, CreatedAt: time.Now(),
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	if err := store.UpdateRequestLogActualCost(log.ID, "usage-funded", 0.04, "upstream", 0, 0, false); err != nil {
		t.Fatalf("update actual cost: %v", err)
	}
	var got GatewayRequestLog
	if err := db.First(&got, log.ID).Error; err != nil {
		t.Fatalf("read request log: %v", err)
	}
	if got.RawCost == nil || *got.RawCost != 0.04 || got.Cost != 0.02 || got.CostEstimated {
		t.Fatalf("cost snapshot = raw=%v actual=%v estimated=%v", got.RawCost, got.Cost, got.CostEstimated)
	}
}
