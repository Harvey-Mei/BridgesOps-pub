package storage

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func createLegacyAggregateFixture(t *testing.T) (*Gateway, *GatewayRequestLog) {
	t.Helper()
	db := openTestDB(t)
	store := NewGateway(db)
	createdAt := time.Date(2026, 7, 19, 4, 5, 0, 0, time.UTC)
	log := &GatewayRequestLog{
		DownstreamKeyID: 7, ChannelID: 9, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15,
		Cost: 0.4, CostEstimated: true, CostSource: "gateway_estimate", CostStatus: GatewayCostStatusEstimated,
		AggregateVersion: 1, CreatedAt: createdAt,
	}
	if err := db.Create(log).Error; err != nil {
		t.Fatalf("create legacy request log: %v", err)
	}
	legacy := &GatewayRequestAggregate{
		BucketStart: aggregateBucketStart(createdAt), DownstreamKeyID: log.DownstreamKeyID, ChannelID: log.ChannelID,
		ModelName: log.ModelName, CostSource: log.CostSource, RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
		Requests: 1, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15,
		TotalCost: 0.4, EstimatedCost: 0.4, LatencySamples: 1,
		FirstRequestAt: createdAt, LastRequestAt: createdAt, Version: 1,
	}
	if err := db.Create(legacy).Error; err != nil {
		t.Fatalf("create legacy aggregate: %v", err)
	}
	return store, log
}

func reconcileAggregateFixture(store *Gateway, logID uint) error {
	return store.UpdateRequestLogActualCostWithUsage(logID, "usage-v2", 0.25, 0.25, "sub2api_actual_cost", GatewayRequestUsageMetrics{
		PromptTokens: 20, CompletionTokens: 8, TotalTokens: 28, HasTokenMetrics: true,
	})
}

func assertAggregateFixtureCountedOnce(t *testing.T, store *Gateway, logID uint) {
	t.Helper()
	var log GatewayRequestLog
	if err := store.db.First(&log, logID).Error; err != nil {
		t.Fatalf("request log was deleted: %v", err)
	}
	if log.AggregateVersion != gatewayRequestAggregateVersion || log.CostSource != "sub2api_actual_cost" {
		t.Fatalf("rebuilt request log = %+v", log)
	}
	var rows []GatewayRequestAggregate
	if err := store.db.Find(&rows).Error; err != nil {
		t.Fatalf("list rebuilt aggregates: %v", err)
	}
	if len(rows) != 1 || rows[0].Version != gatewayRequestAggregateVersion || rows[0].Requests != 1 || rows[0].TotalTokens != 28 || math.Abs(rows[0].TotalCost-0.25) > 1e-12 {
		t.Fatalf("rebuilt aggregates = %+v", rows)
	}
}

func TestGatewayRequestAggregateV2ReconcileAndBackfillOrderCountsOnce(t *testing.T) {
	for _, reconcileFirst := range []bool{true, false} {
		name := "backfill_before_reconcile"
		if reconcileFirst {
			name = "reconcile_before_backfill"
		}
		t.Run(name, func(t *testing.T) {
			store, log := createLegacyAggregateFixture(t)
			if reconcileFirst {
				if err := reconcileAggregateFixture(store, log.ID); err != nil {
					t.Fatalf("reconcile legacy request: %v", err)
				}
			}
			if _, err := backfillGatewayRequestAggregates(context.Background(), store.db, 1, 0); err != nil {
				t.Fatalf("backfill v2 aggregates: %v", err)
			}
			if !reconcileFirst {
				if err := reconcileAggregateFixture(store, log.ID); err != nil {
					t.Fatalf("reconcile rebuilt request: %v", err)
				}
			}
			assertAggregateFixtureCountedOnce(t, store, log.ID)
		})
	}
}

func TestGatewayRequestAggregateV2ConcurrentReconcileAndBackfillCountsOnce(t *testing.T) {
	store, log := createLegacyAggregateFixture(t)
	start := make(chan struct{})
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		_, err := backfillGatewayRequestAggregates(context.Background(), store.db, 1, 0)
		errs <- err
	}()
	go func() {
		defer workers.Done()
		<-start
		errs <- reconcileAggregateFixture(store, log.ID)
	}()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent reconcile/backfill: %v", err)
		}
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), store.db, 1, 0); err != nil {
		t.Fatalf("final aggregate cleanup: %v", err)
	}
	assertAggregateFixtureCountedOnce(t, store, log.ID)
}

func TestGatewayRequestAggregateUpsertDoesNotDowngradeFutureVersion(t *testing.T) {
	db := openTestDB(t)
	bucket := time.Date(2026, 7, 19, 14, 0, 0, 0, time.UTC)
	future := GatewayRequestAggregate{
		BucketStart: bucket, DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5",
		CostSource: "reported", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
		Requests: 9, TotalTokens: 90, TotalCost: 0.9,
		FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion + 1,
	}
	if err := db.Create(&future).Error; err != nil {
		t.Fatal(err)
	}
	incoming := future
	incoming.ID = 0
	incoming.Requests = 1
	incoming.TotalTokens = 10
	incoming.TotalCost = 0.1
	incoming.Version = gatewayRequestAggregateVersion
	if err := db.Transaction(func(tx *gorm.DB) error {
		return upsertRequestAggregateTx(tx, &incoming)
	}); err != nil {
		t.Fatal(err)
	}
	var stored GatewayRequestAggregate
	if err := db.First(&stored, future.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Version != gatewayRequestAggregateVersion+1 || stored.Requests != 9 || stored.TotalTokens != 90 || math.Abs(stored.TotalCost-0.9) > 1e-12 {
		t.Fatalf("future aggregate was mutated: %+v", stored)
	}
}

func TestGatewayRequestAggregateCostReconcileRejectsFutureVersion(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	createdAt := time.Date(2026, 7, 19, 14, 35, 0, 0, time.UTC)
	futureVersion := gatewayRequestAggregateVersion + 1
	log := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15,
		Cost: 0.4, CostEstimated: true, CostSource: "gateway_estimate", CostStatus: GatewayCostStatusEstimated,
		AggregateVersion: futureVersion, CreatedAt: createdAt,
	}
	if err := db.Create(&log).Error; err != nil {
		t.Fatal(err)
	}
	future := GatewayRequestAggregate{
		BucketStart: aggregateBucketStart(createdAt), DownstreamKeyID: log.DownstreamKeyID, ChannelID: log.ChannelID,
		ModelName: log.ModelName, CostSource: log.CostSource, RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
		Requests: 1, PromptTokens: log.PromptTokens, CompletionTokens: log.CompletionTokens, TotalTokens: log.TotalTokens,
		TotalCost: log.Cost, EstimatedCost: log.Cost, FirstRequestAt: createdAt, LastRequestAt: createdAt, Version: futureVersion,
	}
	if err := db.Create(&future).Error; err != nil {
		t.Fatal(err)
	}

	err := store.UpdateRequestLogActualCostWithUsage(log.ID, "usage-future", 0.25, 0.25, "sub2api_actual_cost", GatewayRequestUsageMetrics{
		PromptTokens: 20, CompletionTokens: 8, TotalTokens: 28, HasTokenMetrics: true,
	})
	if !errors.Is(err, errGatewayRequestAggregateVersionConflict) {
		t.Fatalf("future cost reconcile error = %v", err)
	}
	var storedLog GatewayRequestLog
	if err := db.First(&storedLog, log.ID).Error; err != nil {
		t.Fatalf("request log was deleted: %v", err)
	}
	if storedLog.AggregateVersion != futureVersion || storedLog.CostSource != log.CostSource || storedLog.UpstreamUsageID != nil || storedLog.TotalTokens != log.TotalTokens || math.Abs(storedLog.Cost-log.Cost) > 1e-12 {
		t.Fatalf("future request log reconcile was not rolled back: %+v", storedLog)
	}
	var storedAggregate GatewayRequestAggregate
	if err := db.First(&storedAggregate, future.ID).Error; err != nil {
		t.Fatalf("future aggregate was deleted: %v", err)
	}
	if storedAggregate.Version != futureVersion || storedAggregate.Requests != 1 || storedAggregate.TotalTokens != log.TotalTokens || math.Abs(storedAggregate.TotalCost-log.Cost) > 1e-12 {
		t.Fatalf("future aggregate changed: %+v", storedAggregate)
	}
}

func TestGatewayRequestAggregateHistoricalInvalidationRejectsFutureVersion(t *testing.T) {
	db := openTestDB(t)
	createdAt := time.Date(2026, 7, 19, 15, 5, 0, 0, time.UTC)
	futureVersion := gatewayRequestAggregateVersion + 1
	log := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, TotalTokens: 10, Cost: 0.4, CostSource: "reported",
		AggregateVersion: futureVersion, CreatedAt: createdAt,
	}
	if err := db.Create(&log).Error; err != nil {
		t.Fatal(err)
	}
	future := GatewayRequestAggregate{
		BucketStart: aggregateBucketStart(createdAt), DownstreamKeyID: log.DownstreamKeyID, ChannelID: log.ChannelID,
		ModelName: log.ModelName, CostSource: log.CostSource, RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
		Requests: 1, TotalTokens: log.TotalTokens, TotalCost: log.Cost,
		FirstRequestAt: createdAt, LastRequestAt: createdAt, Version: futureVersion,
	}
	if err := db.Create(&future).Error; err != nil {
		t.Fatal(err)
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Update("cost", 0.9).Error; err != nil {
			return err
		}
		return invalidateRequestAggregatesForRangeTx(tx, log.ChannelID, createdAt, createdAt.Add(time.Minute))
	})
	if !errors.Is(err, errGatewayRequestAggregateVersionConflict) {
		t.Fatalf("historical invalidation error = %v", err)
	}

	var storedLog GatewayRequestLog
	if err := db.First(&storedLog, log.ID).Error; err != nil {
		t.Fatalf("request log was deleted: %v", err)
	}
	if storedLog.AggregateVersion != futureVersion || math.Abs(storedLog.Cost-log.Cost) > 1e-12 {
		t.Fatalf("historical correction was not rolled back: %+v", storedLog)
	}
	var storedAggregate GatewayRequestAggregate
	if err := db.First(&storedAggregate, future.ID).Error; err != nil {
		t.Fatalf("future aggregate was deleted: %v", err)
	}
	if storedAggregate.Version != futureVersion || storedAggregate.Requests != 1 || storedAggregate.TotalTokens != 10 || math.Abs(storedAggregate.TotalCost-0.4) > 1e-12 {
		t.Fatalf("future aggregate changed: %+v", storedAggregate)
	}
	processed, err := backfillGatewayRequestAggregates(context.Background(), db, 1, 0)
	if err != nil || processed != 0 {
		t.Fatalf("rolled-back process backfill = %d, err=%v", processed, err)
	}
	var replayable int64
	if err := db.Model(&GatewayRequestLog{}).Where("id = ? AND aggregate_version < ?", log.ID, futureVersion).Count(&replayable).Error; err != nil {
		t.Fatal(err)
	}
	if replayable != 0 {
		t.Fatalf("future upgrade would replay %d already-counted logs", replayable)
	}
}

func TestGatewayRequestAggregateHistoricalInvalidationRejectsFutureAggregateWithoutFutureLog(t *testing.T) {
	db := openTestDB(t)
	bucket := time.Date(2026, 7, 19, 16, 0, 0, 0, time.UTC)
	future := GatewayRequestAggregate{
		BucketStart: bucket, DownstreamKeyID: 1, ChannelID: 2, ModelName: "gpt-5",
		CostSource: "reported", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
		Requests: 1, TotalTokens: 10, TotalCost: 0.4,
		FirstRequestAt: bucket, LastRequestAt: bucket, Version: gatewayRequestAggregateVersion + 1,
	}
	if err := db.Create(&future).Error; err != nil {
		t.Fatal(err)
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		return invalidateRequestAggregatesForRangeTx(tx, future.ChannelID, bucket, bucket.Add(time.Minute))
	})
	if !errors.Is(err, errGatewayRequestAggregateVersionConflict) {
		t.Fatalf("historical invalidation error = %v", err)
	}
	var stored GatewayRequestAggregate
	if err := db.First(&stored, future.ID).Error; err != nil {
		t.Fatalf("future aggregate was deleted: %v", err)
	}
	if stored.Version != future.Version || stored.Requests != future.Requests || stored.TotalTokens != future.TotalTokens || math.Abs(stored.TotalCost-future.TotalCost) > 1e-12 {
		t.Fatalf("future aggregate changed: %+v", stored)
	}
}

func TestGatewayRequestAggregateHistoricalInvalidationRejectsFutureGroupMarker(t *testing.T) {
	db := openTestDB(t)
	createdAt := time.Date(2026, 7, 19, 16, 30, 0, 0, time.UTC)
	log := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, ModelName: "future-group-marker", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, TotalTokens: 10, Cost: 0.4, CostSource: "reported",
		AggregateVersion:           gatewayRequestAggregateVersion,
		GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion + 1,
		CreatedAt:                  createdAt,
	}
	if err := db.Create(&log).Error; err != nil {
		t.Fatal(err)
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Update("cost", 0.9).Error; err != nil {
			return err
		}
		return invalidateRequestAggregatesForRangeTx(tx, log.ChannelID, createdAt, createdAt.Add(time.Minute))
	})
	if !errors.Is(err, errGatewayRequestGroupCacheAggregateVersionConflict) {
		t.Fatalf("future group marker invalidation error = %v", err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.GroupCacheAggregateVersion != gatewayRequestGroupCacheAggregateVersion+1 || math.Abs(stored.Cost-log.Cost) > 1e-12 {
		t.Fatalf("future group marker log changed: %+v", stored)
	}
}

func TestGatewayRequestAggregateChannelDeleteRejectsFutureVersion(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := Channel{Name: "future-history", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", PasswordCipher: "cipher"}
	if err := channels.Create(&channel); err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 7, 19, 17, 5, 0, 0, time.UTC)
	futureVersion := gatewayRequestAggregateVersion + 1
	log := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, AggregateVersion: futureVersion, CreatedAt: createdAt,
	}
	if err := db.Create(&log).Error; err != nil {
		t.Fatal(err)
	}
	future := GatewayRequestAggregate{
		BucketStart: aggregateBucketStart(createdAt), DownstreamKeyID: log.DownstreamKeyID, ChannelID: log.ChannelID,
		ModelName: log.ModelName, RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
		Requests: 1, FirstRequestAt: createdAt, LastRequestAt: createdAt, Version: futureVersion,
	}
	if err := db.Create(&future).Error; err != nil {
		t.Fatal(err)
	}

	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("channel delete prepare error = %v", err)
	}
	if prepared, err := channels.FindByID(channel.ID); err != nil || !prepared.DeletionPending {
		t.Fatalf("channel deletion was not prepared: channel=%+v err=%v", prepared, err)
	}
	var storedLog GatewayRequestLog
	if err := db.First(&storedLog, log.ID).Error; err != nil {
		t.Fatalf("request log was deleted: %v", err)
	}
	if storedLog.AggregateVersion != futureVersion || storedLog.ChannelNameSnapshot != "" {
		t.Fatalf("future request log changed: %+v", storedLog)
	}
	var storedAggregate GatewayRequestAggregate
	if err := db.First(&storedAggregate, future.ID).Error; err != nil {
		t.Fatalf("future aggregate was deleted: %v", err)
	}
	if storedAggregate.Version != futureVersion || storedAggregate.Requests != 1 {
		t.Fatalf("future aggregate changed: %+v", storedAggregate)
	}
}

func TestGatewayRequestAggregateChannelDeleteRejectsFutureGroupMarker(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := Channel{Name: "future-group-delete", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", PasswordCipher: "cipher"}
	if err := channels.Create(&channel); err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 7, 19, 17, 30, 0, 0, time.UTC)
	log := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, ModelName: "future-group-marker", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, AggregateVersion: gatewayRequestAggregateVersion,
		GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion + 1,
		CreatedAt:                  createdAt,
	}
	if err := db.Create(&log).Error; err != nil {
		t.Fatal(err)
	}
	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("future group marker channel delete prepare error = %v", err)
	}
	if prepared, err := channels.FindByID(channel.ID); err != nil || !prepared.DeletionPending {
		t.Fatalf("channel deletion was not prepared: channel=%+v err=%v", prepared, err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.GroupCacheAggregateVersion != gatewayRequestGroupCacheAggregateVersion+1 || stored.ChannelNameSnapshot != "" {
		t.Fatalf("future group marker request changed: %+v", stored)
	}
}
