package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestRequestLogGroupCacheCursorPageStaysBoundedBeyondTenThousandGroups(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	if err := db.Model(&GatewayRequestGroupCacheBackfillState{}).Where("id = ?", gatewayRequestGroupCacheBackfillStateID).Updates(map[string]any{
		"target_version": gatewayRequestGroupCacheAggregateVersion,
		"completed":      true,
	}).Error; err != nil {
		t.Fatalf("mark group-cache ledger ready: %v", err)
	}

	bucket := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)
	rows := make([]GatewayRequestGroupCacheAggregate, 10_025)
	for index := range rows {
		channelID := uint(index + 1)
		rows[index] = GatewayRequestGroupCacheAggregate{
			BucketStart: bucket, DownstreamKeyID: 17, ChannelID: channelID,
			ChannelNameSnapshot: fmt.Sprintf("channel-%05d", channelID), GroupName: "primary", ModelName: "model-a",
			CacheReadTokens: int64(channelID), CacheCreationTokens: 2, PromptTokens: int64(channelID) * 2,
			ValidRequests: 1, LastNormalUseAt: bucket, Version: gatewayRequestGroupCacheAggregateVersion,
		}
	}
	if err := db.CreateInBatches(rows, 500).Error; err != nil {
		t.Fatalf("create large group-cache ledger: %v", err)
	}
	filter := GatewayRequestLogFilter{DownstreamKeyID: 17, ModelName: "model-a", TimeZone: "UTC"}

	first, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), filter, "", 100)
	if err != nil {
		t.Fatalf("first bounded page: %v", err)
	}
	if len(first.Items) != 100 || first.Items[0].ChannelID != 1 || first.Items[99].ChannelID != 100 || first.NextCursor == "" {
		t.Fatalf("first bounded page = %+v", first)
	}
	second, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), filter, first.NextCursor, 100)
	if err != nil {
		t.Fatalf("second bounded page: %v", err)
	}
	if len(second.Items) != 100 || second.Items[0].ChannelID != 101 || second.Items[99].ChannelID != 200 {
		t.Fatalf("second bounded page boundaries = first:%d last:%d count:%d", second.Items[0].ChannelID, second.Items[len(second.Items)-1].ChannelID, len(second.Items))
	}
	repeated, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), filter, first.NextCursor, 100)
	if err != nil {
		t.Fatalf("repeat second page: %v", err)
	}
	if len(repeated.Items) != len(second.Items) || repeated.NextCursor != second.NextCursor {
		t.Fatalf("repeated page changed: first=%+v repeated=%+v", second, repeated)
	}
	for index := range second.Items {
		if repeated.Items[index].ChannelID != second.Items[index].ChannelID {
			t.Fatalf("repeated page item %d = %d, want %d", index, repeated.Items[index].ChannelID, second.Items[index].ChannelID)
		}
	}

	beyondTenThousand, err := encodeGatewayRequestGroupCacheCursor(filter, 100, gatewayRequestGroupCachePageKey{ChannelID: 10_000, GroupName: "primary"})
	if err != nil {
		t.Fatalf("encode >10k boundary: %v", err)
	}
	last, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), filter, beyondTenThousand, 100)
	if err != nil {
		t.Fatalf("page beyond 10k groups: %v", err)
	}
	if len(last.Items) != 25 || last.Items[0].ChannelID != 10_001 || last.Items[24].ChannelID != 10_025 || last.NextCursor != "" {
		t.Fatalf("page beyond 10k = %+v", last)
	}
	if last.Items[0].Ratio == nil || *last.Items[0].Ratio != 0.5 {
		t.Fatalf("weighted ratio beyond 10k = %+v", last.Items[0])
	}

	filtered, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), GatewayRequestLogFilter{
		DownstreamKeyID: 17, ModelName: "model-a", ChannelID: 10_002, TimeZone: "UTC",
	}, "", 20)
	if err != nil || len(filtered.Items) != 1 || filtered.Items[0].ChannelID != 10_002 {
		t.Fatalf("filtered group-cache page = %+v err=%v", filtered, err)
	}
	if _, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), GatewayRequestLogFilter{
		DownstreamKeyID: 17, ModelName: "model-b", TimeZone: "UTC",
	}, first.NextCursor, 100); !errors.Is(err, ErrGatewayRequestGroupCacheCursorInvalid) {
		t.Fatalf("filter-mismatched cursor error = %v", err)
	}
	if _, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), filter, first.NextCursor, 50); !errors.Is(err, ErrGatewayRequestGroupCacheCursorInvalid) {
		t.Fatalf("page-size-mismatched cursor error = %v", err)
	}
}

func TestRequestLogGroupCacheCursorPageMergesAggregateAndRawEdges(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	if err := db.Model(&GatewayRequestGroupCacheBackfillState{}).Where("id = ?", gatewayRequestGroupCacheBackfillStateID).Updates(map[string]any{
		"target_version": gatewayRequestGroupCacheAggregateVersion,
		"completed":      true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	bucket := time.Date(2026, 7, 25, 10, 15, 0, 0, time.UTC)
	if err := db.Create(&GatewayRequestGroupCacheAggregate{
		BucketStart: bucket, DownstreamKeyID: 9, ChannelID: 77, ChannelNameSnapshot: "edge-channel",
		GroupName: "edge-group", ModelName: "model-a", CacheReadTokens: 30, CacheCreationTokens: 4,
		PromptTokens: 100, ValidRequests: 1, LastNormalUseAt: bucket, Version: gatewayRequestGroupCacheAggregateVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, log := range []GatewayRequestLog{
		{
			DownstreamKeyID: 9, ChannelID: 77, ChannelNameSnapshot: "edge-channel", GroupName: "edge-group", ModelName: "model-a",
			Method: "POST", Path: "/v1/responses", RouteReason: "smart_primary", StatusCode: 200,
			PromptTokens: 20, CacheReadTokens: 10, CacheCreationTokens: 1, CreatedAt: bucket.Add(-10 * time.Minute), GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion,
		},
		{
			DownstreamKeyID: 9, ChannelID: 77, ChannelNameSnapshot: "edge-channel", GroupName: "edge-group", ModelName: "model-a",
			Method: "POST", Path: "/v1/responses", RouteReason: "latency_probe", StatusCode: 200,
			PromptTokens: 500, CacheReadTokens: 500, CreatedAt: bucket.Add(-9 * time.Minute), GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion,
		},
		{
			DownstreamKeyID: 9, ChannelID: 77, ChannelNameSnapshot: "edge-channel", GroupName: "edge-group", ModelName: "model-a",
			Method: "POST", Path: "/v1/responses", RouteReason: "smart_primary", StatusCode: 200,
			PromptTokens: 30, CacheReadTokens: 5, CacheCreationTokens: 2, CreatedAt: bucket.Add(16 * time.Minute), GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion,
		},
	} {
		if err := db.Create(&log).Error; err != nil {
			t.Fatal(err)
		}
	}
	start, end := bucket.Add(-14*time.Minute), bucket.Add(17*time.Minute)
	page, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), GatewayRequestLogFilter{
		Start: &start, End: &end, DownstreamKeyID: 9, ModelName: "model-a", TimeZone: "UTC",
	}, "", 20)
	if err != nil {
		t.Fatalf("merge aggregate and raw edges: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("merged page = %+v", page)
	}
	item := page.Items[0]
	if item.PromptTokens != 150 || item.CacheReadTokens != 45 || item.CacheCreationTokens != 7 || item.ValidRequests != 3 || item.Ratio == nil || *item.Ratio != 0.3 {
		t.Fatalf("merged edge metrics = %+v", item)
	}
}

func TestRequestLogGroupCacheCursorPageKeepsMissingPromptGroupsAsNoData(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	bucket := time.Date(2026, 7, 25, 11, 0, 0, 0, time.UTC)
	logs := []*GatewayRequestLog{
		{
			DownstreamKeyID: 19, ChannelID: 10, ChannelNameSnapshot: "missing-channel", GroupName: "missing", ModelName: "model-a",
			Method: "POST", Path: "/v1/responses", RouteReason: "smart_primary", StatusCode: 200,
			PromptTokens: 0, CacheReadTokens: 700, CacheCreationTokens: 300, CreatedAt: bucket,
		},
		{
			DownstreamKeyID: 19, ChannelID: 11, ChannelNameSnapshot: "mixed-channel", GroupName: "mixed", ModelName: "model-a",
			Method: "POST", Path: "/v1/responses", RouteReason: "smart_primary", StatusCode: 200,
			PromptTokens: 100, CacheReadTokens: 25, CacheCreationTokens: 10, CreatedAt: bucket.Add(time.Minute),
		},
		{
			DownstreamKeyID: 19, ChannelID: 11, ChannelNameSnapshot: "mixed-channel", GroupName: "mixed", ModelName: "model-a",
			Method: "POST", Path: "/v1/responses", RouteReason: "smart_primary", StatusCode: 200,
			PromptTokens: 0, CacheReadTokens: 900, CacheCreationTokens: 800, CreatedAt: bucket.Add(2 * time.Minute),
		},
	}
	for _, log := range logs {
		if err := db.Create(log).Error; err != nil {
			t.Fatalf("create request log: %v", err)
		}
	}
	if err := BackfillGatewayRequestGroupCacheAggregates(db); err != nil {
		t.Fatalf("backfill nullable group-cache ledger: %v", err)
	}
	if err := db.Create(&GatewayRequestGroupCacheAggregate{
		BucketStart: bucket, DownstreamKeyID: 19, ChannelID: 1, ChannelNameSnapshot: "invalid-channel",
		GroupName: "invalid", ModelName: "model-a", PromptTokens: 100, CacheReadTokens: 100,
		ValidRequests: 0, LastNormalUseAt: bucket, Version: gatewayRequestGroupCacheAggregateVersion,
	}).Error; err != nil {
		t.Fatalf("create invalid aggregate row: %v", err)
	}

	filter := GatewayRequestLogFilter{DownstreamKeyID: 19, ModelName: "model-a", TimeZone: "UTC"}
	first, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), filter, "", 1)
	if err != nil {
		t.Fatalf("first nullable group-cache page: %v", err)
	}
	if len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("first nullable group-cache page = %+v", first)
	}
	missing := first.Items[0]
	if missing.ChannelID != 10 || missing.PromptTokens != 0 || missing.CacheReadTokens != 0 || missing.CacheCreationTokens != 0 || missing.ValidRequests != 1 || missing.Ratio != nil {
		t.Fatalf("missing-denominator group = %+v", missing)
	}

	second, err := store.ListRequestLogGroupCacheRatioCursorPage(context.Background(), filter, first.NextCursor, 1)
	if err != nil {
		t.Fatalf("second nullable group-cache page: %v", err)
	}
	if len(second.Items) != 1 || second.NextCursor != "" {
		t.Fatalf("second nullable group-cache page = %+v", second)
	}
	mixed := second.Items[0]
	if mixed.ChannelID != 11 || mixed.PromptTokens != 100 || mixed.CacheReadTokens != 25 || mixed.CacheCreationTokens != 10 || mixed.ValidRequests != 2 || mixed.Ratio == nil || *mixed.Ratio != 0.25 {
		t.Fatalf("mixed-denominator group = %+v", mixed)
	}
}
