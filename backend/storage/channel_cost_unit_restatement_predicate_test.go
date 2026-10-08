package storage

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestCostUnitRequestRestatementUsesFrozenPopulationPredicate(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{
		Name: "cost-unit-request-restatement-predicate", Type: ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "u", PasswordCipher: "x",
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	repo := NewCosting(db)
	next, audit, expectedVersion := prepareCostUnitRebase(t, db, repo, channel.ID)

	start := audit.RateTransitionAt.UTC().Truncate(time.Microsecond)
	end := start.Add(25 * time.Second)
	oldFactor := 0.1
	newFactor := 1.0
	fixtureIdentityRevision := channel.UpstreamAccountIdentityRevision
	createLog := func(name string, at time.Time, rawCost *float64, cost, rawRatio, ratio, rawCompletion, completion float64, factor float64, routeReason string) *GatewayRequestLog {
		t.Helper()
		log := &GatewayRequestLog{
			DownstreamKeyID: 1, ChannelID: channel.ID, ChannelIdentityRevision: fixtureIdentityRevision, GroupName: "PLUS", ModelName: name,
			UpstreamGroupID: 1, UpstreamGroupGeneration: 1, UpstreamKeyLocalID: 1, UpstreamKeyGeneration: 1,
			PriceCanonicalGroupName: "plus", PriceModelName: name,
			PriceOperation: GatewayPricingOperationTextGeneration, PriceProtocol: GatewayPricingProtocolOpenAIResponses,
			UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageSchemaVersion: 1,
			Method: "POST", Path: "/v1/responses", StatusCode: 200,
			RawCost: rawCost, Cost: cost, RawGroupRatio: restatementFloat(rawRatio), GroupRatio: ratio,
			RawCompletionRatio: restatementFloat(rawCompletion), CompletionRatio: completion,
			FundingFactor: restatementFloat(factor), CostBasisVersion: expectedVersion,
			CostBasisSource: "confirmed_redeem_code", CostSource: "reported", CostStatus: GatewayCostStatusReported,
			RouteReason: routeReason, AggregateVersion: gatewayRequestAggregateVersion,
			GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion, CreatedAt: at,
		}
		if rawCost == nil {
			log.CostSource = ""
			log.CostStatus = GatewayCostStatusNone
		}
		if err := db.Create(log).Error; err != nil {
			t.Fatalf("create %s request: %v", name, err)
		}
		return log
	}

	before := createLog("before-epoch", start.Add(-time.Microsecond), restatementFloat(1), 0.1, 0.1, 0.1, 0.2, 0.2, oldFactor, "")
	first := createLog("restate-first", start, restatementFloat(2), 0.2, 0.2, 0.2, 0.8, 0.4, oldFactor, "")
	first.PromptTokens = 10
	first.CompletionTokens = 5
	first.TotalTokens = 15
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", first.ID).Updates(map[string]any{
		"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
	}).Error; err != nil {
		t.Fatalf("add target usage fixture: %v", err)
	}
	costless := createLog("new-ratio-costless", start.Add(5*time.Second), nil, 0, 0.6, 0.6, 1.2, 1.2, oldFactor, "")
	oldRatio := createLog("old-ratio-correct", start.Add(10*time.Second), restatementFloat(4), 0.4, 10, 1, 20, 2, oldFactor, "")
	mismatch := createLog("ratio-mismatch", start.Add(15*time.Second), nil, 0, 2, 0.7, 4, 1.3, oldFactor, "")
	probe := createLog("latency-probe", start.Add(20*time.Second), restatementFloat(7), 0.7, 0.7, 0.7, 1.4, 1.4, oldFactor, gatewayRouteReasonLatencyProbe)
	last := createLog("restate-last", end, restatementFloat(3), 0.3, 0.3, 0.3, 1.2, 0.5, oldFactor, "")
	after := createLog("after-epoch", end.Add(time.Microsecond), restatementFloat(1), 0.1, 0.1, 0.1, 0.2, 0.2, oldFactor, "")
	otherFactor := createLog("other-factor", start.Add(12*time.Second), restatementFloat(1), 1, 1, 1, 2, 2, newFactor, "")
	fixtureIdentityRevision++
	otherIdentity := createLog("other-account-identity", start.Add(13*time.Second), restatementFloat(9), 0.9, 0.9, 0.9, 1.8, 1.8, oldFactor, "")
	fixtureIdentityRevision = channel.UpstreamAccountIdentityRevision
	factRawCost := 8.0
	factPriced := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision, GroupName: "PLUS", ModelName: first.ModelName,
		UpstreamGroupID: 1, UpstreamGroupGeneration: 1, UpstreamKeyLocalID: 1, UpstreamKeyGeneration: 1,
		PriceCanonicalGroupName: "plus", PriceModelName: first.ModelName,
		PriceOperation: GatewayPricingOperationTextGeneration, PriceProtocol: GatewayPricingProtocolOpenAIResponses,
		UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageSchemaVersion: 1,
		Method: "POST", Path: "/v1/responses", StatusCode: 200,
		RawCost: &factRawCost, Cost: 0.8, RawGroupRatio: restatementFloat(0.8), GroupRatio: 0.8,
		RawCompletionRatio: restatementFloat(1.6), CompletionRatio: 1.6,
		PromptTokens: 7, CompletionTokens: 3, TotalTokens: 10,
		FundingFactor: restatementFloat(oldFactor), CostBasisVersion: expectedVersion,
		CostBasisSource: "confirmed_redeem_code", RouteReason: "",
		AggregateVersion:           gatewayRequestAggregateVersion,
		GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion,
		CreatedAt:                  start.Add(18 * time.Second),
	}
	attachTrustedEstimatedTextPriceFact(t, db, factPriced, 1, 1)
	factPriced.CostEstimated = false
	factPriced.CostSource = "reported"
	factPriced.CostStatus = GatewayCostStatusReported
	if err := db.Create(factPriced).Error; err != nil {
		t.Fatalf("create fact-priced request: %v", err)
	}
	if err := db.Model(&GatewayRequestLog{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"aggregate_version":             0,
		"group_cache_aggregate_version": 0,
	}).Error; err != nil {
		t.Fatalf("reset aggregate fixture markers: %v", err)
	}
	if err := migrateGatewayRequestAggregates(db); err != nil {
		t.Fatalf("build aggregate fixture: %v", err)
	}

	var highWater uint64
	if err := db.Raw("SELECT max(id) FROM public.gateway_request_logs").Scan(&highWater).Error; err != nil {
		t.Fatal(err)
	}
	buckets := []time.Time{aggregateBucketStart(start)}
	digest, err := CostUnitRequestRestatementBucketDigest(buckets)
	if err != nil {
		t.Fatal(err)
	}
	audit.RequestRestatement = &CostUnitRequestRestatementEvidence{
		RequestLogHighWater: highWater, AffectedStartedAt: start, AffectedEndedAt: end,
		OldFundingFactor: "0.1", NewFundingFactor: "1",
		ExpectedPopulationRows: 6, ExpectedNewRatioRows: 4, ExpectedOldRatioRows: 1,
		ExpectedRatioMismatchRows: 1, ExpectedLatencyProbeRows: 1,
		ExpectedRestatementRows: 2, ExpectedAlreadyCorrectCostedRows: 1,
		ExpectedRawCostSum: "5", ExpectedRecordedCostSum: "0.5",
		ExpectedCorrectedCostSum: "5", ExpectedUndercountSum: "4.5",
		ExpectedAlreadyCorrectRawCostSum: "4", ExpectedAllCostedRawCostSum: "9",
		ExpectedBucketStarts: buckets, ExpectedBucketDigest: digest,
	}

	replayed, err := repo.ApplyCostUnitRebase(next, audit, expectedVersion)
	if err != nil {
		t.Fatalf("apply rebase with request restatement: %v", err)
	}
	if replayed {
		t.Fatal("first request restatement was reported as a replay")
	}

	assertLog := func(original *GatewayRequestLog, changed bool) GatewayRequestLog {
		t.Helper()
		var stored GatewayRequestLog
		if err := db.First(&stored, original.ID).Error; err != nil {
			t.Fatal(err)
		}
		if changed {
			if stored.FundingFactor == nil || *stored.FundingFactor != newFactor || stored.CostBasisVersion != expectedVersion+1 || stored.CostBasisSource != "cost_unit_rebase" {
				t.Fatalf("restated basis for %s = %+v", original.ModelName, stored)
			}
		} else if stored.FundingFactor == nil || *stored.FundingFactor != *original.FundingFactor || stored.Cost != original.Cost ||
			stored.GroupRatio != original.GroupRatio || stored.CompletionRatio != original.CompletionRatio ||
			stored.CostBasisVersion != original.CostBasisVersion || stored.CostBasisSource != original.CostBasisSource {
			t.Fatalf("non-target %s changed: before=%+v after=%+v", original.ModelName, *original, stored)
		}
		if stored.RawCost == nil != (original.RawCost == nil) || stored.RawGroupRatio == nil || original.RawGroupRatio == nil || *stored.RawGroupRatio != *original.RawGroupRatio ||
			stored.RawCompletionRatio == nil || original.RawCompletionRatio == nil || *stored.RawCompletionRatio != *original.RawCompletionRatio ||
			(stored.RawCost != nil && *stored.RawCost != *original.RawCost) || stored.CostSource != original.CostSource || stored.CostStatus != original.CostStatus {
			t.Fatalf("raw/accounting facts for %s changed: before=%+v after=%+v", original.ModelName, *original, stored)
		}
		return stored
	}
	storedFirst := assertLog(first, true)
	storedLast := assertLog(last, true)
	if storedFirst.Cost != 2 || storedFirst.GroupRatio != 0.2 || storedFirst.CompletionRatio != 0.8 ||
		storedLast.Cost != 3 || storedLast.GroupRatio != 0.3 || storedLast.CompletionRatio != 1.2 {
		t.Fatalf("restated amounts/ratios = first %+v, last %+v", storedFirst, storedLast)
	}
	for _, untouched := range []*GatewayRequestLog{before, costless, oldRatio, mismatch, probe, after, otherFactor, otherIdentity, factPriced} {
		assertLog(untouched, false)
	}
	storedFact := assertLog(factPriced, false)
	if storedFact.PriceFactID == nil || factPriced.PriceFactID == nil || *storedFact.PriceFactID != *factPriced.PriceFactID ||
		storedFact.PriceFactChecksum != factPriced.PriceFactChecksum || storedFact.PriceFactVersion != factPriced.PriceFactVersion ||
		storedFact.PriceFactSchemaVersion != factPriced.PriceFactSchemaVersion {
		t.Fatalf("fact-priced provenance changed: before=%+v after=%+v", *factPriced, storedFact)
	}

	var operation ChannelCostUnitRequestRestatement
	if err := db.Where("rebase_id = ?", audit.ID).First(&operation).Error; err != nil {
		t.Fatal(err)
	}
	if operation.AppliedRestatementRows != 2 || operation.AppliedRawCostSum != "5" ||
		operation.AppliedRecordedCostSum != "0.5" || operation.AppliedCorrectedCostSum != "5" || operation.AppliedUndercountSum != "4.5" {
		t.Fatalf("request restatement operation = %+v", operation)
	}
	var items []ChannelCostUnitRequestRestatementItem
	if err := db.Where("restatement_id = ?", operation.ID).Order("request_log_id ASC").Find(&items).Error; err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].RequestLogID != uint64(first.ID) || items[1].RequestLogID != uint64(last.ID) {
		t.Fatalf("request restatement items = %+v", items)
	}
	for _, item := range items {
		if item.CostSource != "reported" || item.CostStatus != string(GatewayCostStatusReported) || item.FundingFactorBefore != "0.1" || item.FundingFactorAfter != "1" {
			t.Fatalf("request restatement item lost audit facts: %+v", item)
		}
	}
	if err := db.Model(&ChannelCostUnitRequestRestatementItem{}).Where("id = ?", items[0].ID).
		Update("route_reason", "tampered").Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") {
		t.Fatalf("immutable request restatement item update error = %v", err)
	}
	if err := db.Where("id = ?", operation.ID).
		Delete(&ChannelCostUnitRequestRestatement{}).Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") {
		t.Fatalf("immutable request restatement operation delete error = %v", err)
	}

	for _, log := range []*GatewayRequestLog{before, first, costless, oldRatio, mismatch, probe, last, after, otherFactor, otherIdentity, factPriced} {
		var stored GatewayRequestLog
		if err := db.Select("aggregate_version", "group_cache_aggregate_version").First(&stored, log.ID).Error; err != nil {
			t.Fatal(err)
		}
		if stored.AggregateVersion != gatewayRequestAggregateVersion || stored.GroupCacheAggregateVersion != gatewayRequestGroupCacheAggregateVersion {
			t.Fatalf("mixed-bucket aggregate marker for %s = %d/%d", log.ModelName, stored.AggregateVersion, stored.GroupCacheAggregateVersion)
		}
	}
	var sharedAggregate GatewayRequestAggregate
	if err := db.Where("channel_id = ? AND model_name = ? AND cost_source = ?", channel.ID, first.ModelName, "reported").
		First(&sharedAggregate).Error; err != nil {
		var rows []GatewayRequestAggregate
		if listErr := db.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&rows).Error; listErr != nil {
			t.Fatalf("load shared request aggregate: %v (diagnostic query: %v)", err, listErr)
		}
		t.Fatalf("load shared request aggregate: %v; channel aggregates=%+v", err, rows)
	}
	if sharedAggregate.Requests != 2 || sharedAggregate.TotalTokens != 25 ||
		math.Abs(sharedAggregate.TotalCost-2.8) > 1e-12 || math.Abs(sharedAggregate.ActualCost-2.8) > 1e-12 ||
		math.Abs(sharedAggregate.ReportedCost-2.8) > 1e-12 ||
		sharedAggregate.Version != gatewayRequestAggregateVersion {
		t.Fatalf("rebuilt shared request aggregate = %+v", sharedAggregate)
	}
	var sharedGroupAggregate GatewayRequestGroupCacheAggregate
	if err := db.Where("channel_id = ? AND group_name = ? AND model_name = ?", channel.ID, first.GroupName, first.ModelName).
		First(&sharedGroupAggregate).Error; err != nil {
		t.Fatal(err)
	}
	if sharedGroupAggregate.ValidRequests != 2 || sharedGroupAggregate.PromptTokens != 17 ||
		sharedGroupAggregate.Version != gatewayRequestGroupCacheAggregateVersion {
		t.Fatalf("rebuilt shared group-cache aggregate = %+v", sharedGroupAggregate)
	}

	replayed, err = repo.ApplyCostUnitRebase(next, audit, expectedVersion)
	if err != nil || !replayed {
		t.Fatalf("idempotent rebase replay = %v, err=%v", replayed, err)
	}
	var operationCount, itemCount int64
	if err := db.Model(&ChannelCostUnitRequestRestatement{}).Where("rebase_id = ?", audit.ID).Count(&operationCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelCostUnitRequestRestatementItem{}).Where("restatement_id = ?", operation.ID).Count(&itemCount).Error; err != nil {
		t.Fatal(err)
	}
	if operationCount != 1 || itemCount != 2 {
		t.Fatalf("idempotent replay evidence counts = operation %d, items %d", operationCount, itemCount)
	}
}

func restatementFloat(value float64) *float64 { return &value }

func TestCostUnitRequestRestatementRejectsFloatEquivalentFactorEvidence(t *testing.T) {
	db := openTestDB(t)
	fixture := seedLiMaoRequestRestatementFixture(t, db, "restatement-exact-factor")
	mutated := *fixture.evidence
	mutated.ExpectedBucketStarts = append([]time.Time(nil), fixture.evidence.ExpectedBucketStarts...)
	// This differs as a PostgreSQL numeric but collapses to the same float64 as
	// the factor persisted by the rebase.
	mutated.NewFundingFactor = liMaoNewFundingFactor + "0000000000000001"
	fixture.audit.RequestRestatement = &mutated

	if _, err := fixture.repo.ApplyCostUnitRebase(fixture.next, fixture.audit, fixture.expectedVersion); !errors.Is(err, ErrCostUnitRequestRestatementInvalid) {
		t.Fatalf("float-equivalent factor evidence error = %v, want %v", err, ErrCostUnitRequestRestatementInvalid)
	}
	assertRestatementRollback(t, db, fixture)
}

func TestCostUnitRequestRestatementFutureAggregateMarkerRollsBackEverything(t *testing.T) {
	db := openTestDB(t)
	fixture := seedLiMaoRequestRestatementFixture(t, db, "restatement-future-aggregate")
	var log GatewayRequestLog
	if err := db.Where("channel_id = ? AND created_at >= ? AND created_at <= ?", fixture.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt).
		Order("id ASC").First(&log).Error; err != nil {
		t.Fatal(err)
	}
	futureVersion := gatewayRequestAggregateVersion + 1
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).
		Update("aggregate_version", futureVersion).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.repo.ApplyCostUnitRebase(fixture.next, fixture.audit, fixture.expectedVersion); !errors.Is(err, errGatewayRequestAggregateVersionConflict) {
		t.Fatalf("future aggregate marker error = %v, want %v", err, errGatewayRequestAggregateVersionConflict)
	}
	assertRestatementRollback(t, db, fixture)
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.AggregateVersion != futureVersion {
		t.Fatalf("future aggregate marker after rollback = %d, want %d", stored.AggregateVersion, futureVersion)
	}
}

func TestCostUnitRequestRestatementOffsettingRowResidualsRollBackEverything(t *testing.T) {
	db := openTestDB(t)
	fixture := seedLiMaoRequestRestatementFixture(t, db, "restatement-row-residual")
	var ids []uint64
	if err := db.Raw(`
SELECT id
FROM public.gateway_request_logs
WHERE channel_id = ?
  AND created_at >= ? AND created_at <= ?
  AND funding_factor = CAST(? AS numeric)
  AND raw_cost IS NOT NULL
  AND raw_group_ratio = group_ratio
ORDER BY id
LIMIT 2`, fixture.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt, liMaoOldFundingFactor).
		Scan(&ids).Error; err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("residual fixture target ids = %v", ids)
	}
	if err := db.Exec(`
UPDATE public.gateway_request_logs
SET cost = cost + CASE id WHEN ? THEN 0.000001::numeric ELSE -0.000001::numeric END
WHERE id IN (?, ?)`, ids[0], ids[0], ids[1]).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.repo.ApplyCostUnitRebase(fixture.next, fixture.audit, fixture.expectedVersion); !errors.Is(err, ErrCostUnitRequestRestatementConflict) {
		t.Fatalf("offsetting row residual error = %v, want %v", err, ErrCostUnitRequestRestatementConflict)
	}
	assertRestatementRollback(t, db, fixture)
}
