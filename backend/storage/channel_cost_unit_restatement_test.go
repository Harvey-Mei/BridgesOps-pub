package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

const (
	liMaoOldFundingFactor         = "0.10050346486909446"
	liMaoNewFundingFactor         = "1.00503464869094"
	liMaoPreviousProviderFactor   = "0.1"
	liMaoCurrentProviderFactor    = "1"
	liMaoRawCostSum               = "1.083942315"
	liMaoRecordedCostSum          = "0.1089399583757274"
	liMaoCorrectedCostSum         = "1.0893995837572692"
	liMaoUndercountSum            = "0.9804596253815418"
	liMaoAlreadyCorrectRawCostSum = "0.009882900"
	liMaoAllCostedRawCostSum      = "1.093825215"
)

var (
	liMaoAffectedStartedAt = time.Date(2026, 7, 26, 1, 46, 23, 337852000, time.UTC)
	liMaoAffectedEndedAt   = time.Date(2026, 7, 26, 2, 33, 4, 411928000, time.UTC)
)

func TestApplyCostUnitRebaseRestatesFrozen196RequestPopulation(t *testing.T) {
	db := openTestDB(t)
	fixture := seedLiMaoRequestRestatementFixture(t, db, "li-mao-196")
	beforeNonTarget := requestRestatementNonTargetDigest(t, db, fixture.channel.ID)
	var lowestPopulationIDCreatedAt time.Time
	if err := db.Raw(`
		SELECT created_at FROM public.gateway_request_logs
		WHERE channel_id = ? AND created_at >= ? AND created_at <= ?
		ORDER BY id LIMIT 1`, fixture.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt).
		Scan(&lowestPopulationIDCreatedAt).Error; err != nil {
		t.Fatal(err)
	}
	if !lowestPopulationIDCreatedAt.Equal(liMaoAffectedEndedAt) {
		t.Fatalf("fixture did not separate ID order from time order: lowest ID time = %v", lowestPopulationIDCreatedAt)
	}

	replayed, err := fixture.repo.ApplyCostUnitRebase(fixture.next, fixture.audit, fixture.expectedVersion)
	if err != nil {
		t.Fatalf("apply cost-unit request restatement: %v", err)
	}
	if replayed {
		t.Fatal("first request restatement was reported as a replay")
	}

	var targetRows int64
	if err := db.Raw(`
		SELECT count(*)
		FROM public.gateway_request_logs
		WHERE channel_id = ?
		  AND created_at >= ? AND created_at <= ?
		  AND funding_factor = CAST(? AS numeric)
		  AND cost_basis_version = ?
		  AND cost_basis_source = 'cost_unit_rebase'
		  AND cost = raw_cost * CAST(? AS numeric)
		  AND group_ratio = raw_group_ratio * CAST(? AS numeric)
		  AND completion_ratio = raw_completion_ratio * CAST(? AS numeric)`,
		fixture.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt, liMaoNewFundingFactor,
		fixture.expectedVersion+1, liMaoNewFundingFactor, liMaoNewFundingFactor, liMaoNewFundingFactor).
		Scan(&targetRows).Error; err != nil {
		t.Fatal(err)
	}
	if targetRows != 124 {
		t.Fatalf("restated rows = %d, want 124", targetRows)
	}

	var untouchedRows int64
	if err := db.Raw(`
		SELECT count(*)
		FROM public.gateway_request_logs
		WHERE channel_id = ?
		  AND created_at >= ? AND created_at <= ?
		  AND funding_factor = CAST(? AS numeric)`,
		fixture.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt, liMaoOldFundingFactor).
		Scan(&untouchedRows).Error; err != nil {
		t.Fatal(err)
	}
	if untouchedRows != 72 {
		t.Fatalf("untouched frozen-population rows = %d, want 72", untouchedRows)
	}
	if after := requestRestatementNonTargetDigest(t, db, fixture.channel.ID); after != beforeNonTarget {
		t.Fatalf("non-target request facts changed: before=%s after=%s", beforeNonTarget, after)
	}
	var outsideRows int64
	if err := db.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at IN ? AND funding_factor = CAST(? AS numeric) AND cost_basis_version = 7",
			fixture.channel.ID, []time.Time{liMaoAffectedStartedAt.Add(-time.Microsecond), liMaoAffectedEndedAt.Add(time.Microsecond)}, liMaoOldFundingFactor).
		Count(&outsideRows).Error; err != nil {
		t.Fatal(err)
	}
	if outsideRows != 2 {
		t.Fatalf("open/closed time-boundary rows preserved = %d, want 2", outsideRows)
	}

	var operation ChannelCostUnitRequestRestatement
	if err := db.Where("rebase_id = ?", fixture.audit.ID).First(&operation).Error; err != nil {
		t.Fatal(err)
	}
	if operation.ExpectedPopulationRows != 196 || operation.AppliedRestatementRows != 124 ||
		operation.ExpectedNewRatioRows != 187 || operation.ExpectedOldRatioRows != 6 ||
		operation.ExpectedRatioMismatchRows != 3 || operation.ExpectedAlreadyCorrectCostedRows != 5 ||
		operation.RequestLogHighWater != fixture.evidence.RequestLogHighWater {
		t.Fatalf("request restatement operation = %+v", operation)
	}
	var itemCount int64
	if err := db.Model(&ChannelCostUnitRequestRestatementItem{}).Where("restatement_id = ?", operation.ID).Count(&itemCount).Error; err != nil {
		t.Fatal(err)
	}
	if itemCount != 124 {
		t.Fatalf("request restatement item count = %d, want 124", itemCount)
	}
	assertLiMaoRestatementAmounts(t, db, operation.ID)

	var resetRows int64
	if err := db.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at < ? AND aggregate_version = 0 AND group_cache_aggregate_version = 0",
			fixture.channel.ID, fixture.bucketStarts[0], fixture.bucketStarts[len(fixture.bucketStarts)-1].Add(gatewayRequestAggregateBucket)).
		Count(&resetRows).Error; err != nil {
		t.Fatal(err)
	}
	if resetRows != 198 {
		t.Fatalf("full-bucket invalidated request rows = %d, want 198", resetRows)
	}
	assertAffectedAggregateRows(t, db, fixture.channel.ID, fixture.bucketStarts, 0)

	processed, err := BackfillGatewayRequestAggregates(context.Background(), db)
	if err != nil {
		t.Fatalf("rebuild invalidated request aggregates: %v", err)
	}
	if processed < 198 {
		t.Fatalf("rebuilt request rows = %d, want at least 198", processed)
	}
	assertAffectedAggregateRows(t, db, fixture.channel.ID, fixture.bucketStarts, -1)
	assertRestatementAggregatesMatchDetails(t, db, fixture.channel.ID, fixture.bucketStarts)

	beforeReplay := requestRestatementTargetDigest(t, db, operation.ID)
	replayAudit := &ChannelCostUnitRebase{
		ChannelID: fixture.channel.ID, IdempotencyKey: fixture.audit.IdempotencyKey,
		RequestFingerprint: fixture.audit.RequestFingerprint, Actor: fixture.audit.Actor,
		RequestRestatement: fixture.evidence,
	}
	replayed, err = fixture.repo.ApplyCostUnitRebase(&ChannelCostState{ChannelID: fixture.channel.ID}, replayAudit, fixture.expectedVersion)
	if err != nil || !replayed {
		t.Fatalf("idempotent request restatement replay = %t, err=%v", replayed, err)
	}
	if afterReplay := requestRestatementTargetDigest(t, db, operation.ID); afterReplay != beforeReplay {
		t.Fatalf("idempotent replay changed request money: before=%s after=%s", beforeReplay, afterReplay)
	}
	for _, model := range []any{&ChannelCostUnitRequestRestatement{}, &ChannelCostUnitRequestRestatementItem{}} {
		var count int64
		query := db.Model(model)
		if _, ok := model.(*ChannelCostUnitRequestRestatement); ok {
			query = query.Where("id = ?", operation.ID)
		} else {
			query = query.Where("restatement_id = ?", operation.ID)
		}
		if err := query.Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		want := int64(1)
		if _, ok := model.(*ChannelCostUnitRequestRestatementItem); ok {
			want = 124
		}
		if count != want {
			t.Fatalf("%T rows after replay = %d, want %d", model, count, want)
		}
	}

	if err := db.Model(&ChannelCostUnitRequestRestatement{}).Where("id = ?", operation.ID).
		Update("actor", "tampered").Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") {
		t.Fatalf("immutable operation update error = %v", err)
	}
	if err := db.Where("restatement_id = ?", operation.ID).
		Delete(&ChannelCostUnitRequestRestatementItem{}).Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") {
		t.Fatalf("immutable item delete error = %v", err)
	}
}

func TestApplyCostUnitRebaseRestatementEvidenceDriftRollsBackEverything(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*CostUnitRequestRestatementEvidence)
		want   error
	}{
		{
			name: "restatement row-count drift",
			mutate: func(evidence *CostUnitRequestRestatementEvidence) {
				evidence.ExpectedRestatementRows++
			},
			want: ErrCostUnitRequestRestatementConflict,
		},
		{
			name: "raw amount drift",
			mutate: func(evidence *CostUnitRequestRestatementEvidence) {
				evidence.ExpectedRawCostSum = "1.083942316"
				evidence.ExpectedAllCostedRawCostSum = "1.093825216"
			},
			want: ErrCostUnitRequestRestatementConflict,
		},
		{
			name: "amount identity drift",
			mutate: func(evidence *CostUnitRequestRestatementEvidence) {
				evidence.ExpectedRawCostSum = "1.083942316"
			},
			want: ErrCostUnitRequestRestatementInvalid,
		},
		{
			name: "funding factor drift",
			mutate: func(evidence *CostUnitRequestRestatementEvidence) {
				evidence.NewFundingFactor = "1.004"
			},
			want: ErrCostUnitRequestRestatementInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			fixture := seedLiMaoRequestRestatementFixture(t, db, "drift-"+strings.ReplaceAll(test.name, " ", "-"))
			mutated := *fixture.evidence
			mutated.ExpectedBucketStarts = append([]time.Time(nil), fixture.evidence.ExpectedBucketStarts...)
			test.mutate(&mutated)
			fixture.audit.RequestRestatement = &mutated

			if _, err := fixture.repo.ApplyCostUnitRebase(fixture.next, fixture.audit, fixture.expectedVersion); !errors.Is(err, test.want) {
				t.Fatalf("drift error = %v, want %v", err, test.want)
			}
			assertRestatementRollback(t, db, fixture)
		})
	}
}

func TestApplyCostUnitRebaseRestatementAllowsUnrelatedHighWaterGrowth(t *testing.T) {
	db := openTestDB(t)
	fixture := seedLiMaoRequestRestatementFixture(t, db, "unrelated-high-water-growth")
	fixture.evidence.RequestLogHighWater--
	fixture.audit.RequestRestatement = fixture.evidence

	if replayed, err := fixture.repo.ApplyCostUnitRebase(fixture.next, fixture.audit, fixture.expectedVersion); err != nil || replayed {
		t.Fatalf("apply with unrelated later request = replayed %t, err=%v", replayed, err)
	}
	var operation ChannelCostUnitRequestRestatement
	if err := db.Where("channel_id = ?", fixture.channel.ID).Take(&operation).Error; err != nil {
		t.Fatal(err)
	}
	if operation.RequestLogHighWater != fixture.evidence.RequestLogHighWater || operation.AppliedRestatementRows != 124 {
		t.Fatalf("restatement operation = %+v", operation)
	}
}

func TestApplyCostUnitRebaseRestatementRejectsLateTargetRowAboveHighWater(t *testing.T) {
	db := openTestDB(t)
	fixture := seedLiMaoRequestRestatementFixture(t, db, "late-target-row")
	insertCostedRestatementFixtureLog(t, db, fixture.channel.ID, liMaoAffectedStartedAt.Add(time.Second),
		"0.0001", liMaoOldFundingFactor, "0.06", "0.06", "0.12", "0.12")

	if _, err := fixture.repo.ApplyCostUnitRebase(fixture.next, fixture.audit, fixture.expectedVersion); !errors.Is(err, ErrCostUnitRequestRestatementConflict) {
		t.Fatalf("late target row error = %v, want %v", err, ErrCostUnitRequestRestatementConflict)
	}
	assertRestatementRollback(t, db, fixture)
}

func TestApplyCostUnitRebaseNeverRestatesLatencyProbe(t *testing.T) {
	db := openTestDB(t)
	fixture := seedLiMaoRequestRestatementFixture(t, db, "li-mao-probe")
	var probeID uint64
	if err := db.Raw(`
		SELECT id FROM public.gateway_request_logs
		WHERE channel_id = ? AND created_at >= ? AND created_at <= ?
		  AND raw_cost IS NULL AND raw_group_ratio = group_ratio
		ORDER BY id LIMIT 1`, fixture.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt).
		Scan(&probeID).Error; err != nil {
		t.Fatal(err)
	}
	if probeID == 0 {
		t.Fatal("fixture produced no costless new-ratio row for latency probe")
	}
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", probeID).Update("route_reason", "latency_probe").Error; err != nil {
		t.Fatal(err)
	}
	fixture.evidence.ExpectedLatencyProbeRows = 1
	fixture.audit.RequestRestatement = fixture.evidence
	if _, err := fixture.repo.ApplyCostUnitRebase(fixture.next, fixture.audit, fixture.expectedVersion); err != nil {
		t.Fatalf("apply restatement with explicit probe count: %v", err)
	}
	var preserved bool
	if err := db.Raw(`
		SELECT funding_factor = CAST(? AS numeric)
		   AND cost_basis_version = 7
		   AND cost_basis_source = 'confirmed_redeem_code'
		   AND raw_cost IS NULL
		   AND route_reason = 'latency_probe'
		FROM public.gateway_request_logs WHERE id = ?`, liMaoOldFundingFactor, probeID).
		Scan(&preserved).Error; err != nil {
		t.Fatal(err)
	}
	if !preserved {
		t.Fatal("latency probe was restated")
	}
	var itemCount int64
	if err := db.Model(&ChannelCostUnitRequestRestatementItem{}).Where("request_log_id = ?", probeID).Count(&itemCount).Error; err != nil {
		t.Fatal(err)
	}
	if itemCount != 0 {
		t.Fatalf("latency probe audit items = %d, want 0", itemCount)
	}
}

type liMaoRequestRestatementFixture struct {
	db              *gorm.DB
	repo            *Costing
	channel         *Channel
	next            *ChannelCostState
	audit           *ChannelCostUnitRebase
	evidence        *CostUnitRequestRestatementEvidence
	expectedVersion uint64
	bucketStarts    []time.Time
}

func seedLiMaoRequestRestatementFixture(t *testing.T, db *gorm.DB, name string) *liMaoRequestRestatementFixture {
	t.Helper()
	channel := &Channel{Name: name, Type: ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "x"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	repo := NewCosting(db)
	next, audit, expectedVersion := prepareCostUnitRebase(t, db, repo, channel.ID)
	oldFactor := mustParseFixtureFloat(t, liMaoOldFundingFactor)
	newFactor := mustParseFixtureFloat(t, liMaoNewFundingFactor)
	costSampledBefore := time.Date(2026, 7, 26, 1, 45, 0, 0, time.UTC)
	balanceSampledBefore := costSampledBefore.Add(-time.Minute)
	costSampledAfter := time.Date(2026, 7, 26, 2, 34, 0, 0, time.UTC)
	balanceSampledAfter := costSampledAfter.Add(-time.Minute)
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"funding_factor": oldFactor, "actual_inventory": 100 * oldFactor,
		"actual_today_cost": 20 * oldFactor, "actual_total_cost": 1000 * oldFactor,
		"balance_sampled_at": balanceSampledBefore, "cost_sampled_at": costSampledBefore,
		"observed_provider_factor_sampled_at": costSampledBefore,
	}).Error; err != nil {
		t.Fatal(err)
	}
	next.FundingFactor = newFactor
	next.ActualInventory = next.RawBalance * newFactor
	next.ActualTodayCost = next.RawTodayCost * newFactor
	next.ActualTotalCost = (1000-20)*oldFactor + next.ActualTodayCost
	next.BalanceSampledAt = &balanceSampledAfter
	next.CostSampledAt = &costSampledAfter
	next.ObservedProviderFactorSampledAt = &costSampledAfter
	audit.RateTransitionAt = liMaoAffectedStartedAt
	audit.AppliedAt = costSampledAfter

	span := liMaoAffectedEndedAt.Sub(liMaoAffectedStartedAt)
	createdAt := func(index int) time.Time {
		if index == 195 {
			return liMaoAffectedEndedAt
		}
		return liMaoAffectedStartedAt.Add(time.Duration(int64(span) * int64(index) / 195)).Truncate(time.Microsecond)
	}
	order := make([]int, 0, 196)
	order = append(order, 195)
	for index := 0; index < 195; index++ {
		order = append(order, index)
	}
	for _, index := range order {
		switch {
		case index < 124:
			raw := "0.008"
			if index == 123 {
				raw = "0.099942315"
			}
			insertCostedRestatementFixtureLog(t, db, channel.ID, createdAt(index), raw, liMaoOldFundingFactor, "0.06", "0.06", "0.12", "0.12")
		case index < 187:
			insertCostlessRestatementFixtureLog(t, db, channel.ID, createdAt(index), "0.06", "0.06", "0.12", "0.12")
		case index < 192:
			raw := "0.001"
			if index == 191 {
				raw = "0.005882900"
			}
			insertCostedRestatementFixtureLog(t, db, channel.ID, createdAt(index), raw, liMaoNewFundingFactor,
				"0.6", "0.060302078921456676", "1.2", "0.120604157842913352")
		case index == 192:
			insertCostlessRestatementFixtureLog(t, db, channel.ID, createdAt(index),
				"0.6", "0.060302078921456676", "1.2", "0.120604157842913352")
		default:
			insertCostlessRestatementFixtureLog(t, db, channel.ID, createdAt(index), "0.3", "0.07", "0.6", "0.14")
		}
	}
	insertCostedRestatementFixtureLog(t, db, channel.ID, liMaoAffectedStartedAt.Add(-time.Microsecond), "0.0001", liMaoOldFundingFactor, "0.06", "0.06", "0.12", "0.12")
	insertCostedRestatementFixtureLog(t, db, channel.ID, liMaoAffectedEndedAt.Add(time.Microsecond), "0.0001", liMaoOldFundingFactor, "0.06", "0.06", "0.12", "0.12")

	var highWater uint64
	if err := db.Raw("SELECT max(id) FROM public.gateway_request_logs").Scan(&highWater).Error; err != nil {
		t.Fatal(err)
	}
	buckets := []time.Time{
		time.Date(2026, 7, 26, 1, 45, 0, 0, time.UTC),
		time.Date(2026, 7, 26, 2, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 26, 2, 15, 0, 0, time.UTC),
		time.Date(2026, 7, 26, 2, 30, 0, 0, time.UTC),
	}
	digest, err := CostUnitRequestRestatementBucketDigest(buckets)
	if err != nil {
		t.Fatal(err)
	}
	evidence := &CostUnitRequestRestatementEvidence{
		RequestLogHighWater: highWater, AffectedStartedAt: liMaoAffectedStartedAt, AffectedEndedAt: liMaoAffectedEndedAt,
		OldFundingFactor: liMaoOldFundingFactor, NewFundingFactor: liMaoNewFundingFactor,
		ExpectedPopulationRows: 196, ExpectedNewRatioRows: 187, ExpectedOldRatioRows: 6, ExpectedRatioMismatchRows: 3,
		ExpectedLatencyProbeRows: 0, ExpectedRestatementRows: 124, ExpectedAlreadyCorrectCostedRows: 5,
		ExpectedRawCostSum: liMaoRawCostSum, ExpectedRecordedCostSum: liMaoRecordedCostSum,
		ExpectedCorrectedCostSum: liMaoCorrectedCostSum, ExpectedUndercountSum: liMaoUndercountSum,
		ExpectedAlreadyCorrectRawCostSum: liMaoAlreadyCorrectRawCostSum,
		ExpectedAllCostedRawCostSum:      liMaoAllCostedRawCostSum,
		ExpectedBucketStarts:             buckets, ExpectedBucketDigest: digest,
	}
	audit.RequestRestatement = evidence
	seedRestatementAggregateRows(t, db, channel.ID, buckets)
	return &liMaoRequestRestatementFixture{
		db: db, repo: repo, channel: channel, next: next, audit: audit, evidence: evidence,
		expectedVersion: expectedVersion, bucketStarts: buckets,
	}
}

func insertCostedRestatementFixtureLog(t *testing.T, db *gorm.DB, channelID uint, createdAt time.Time, rawCost, costFactor, rawGroup, group, rawCompletion, completion string) {
	t.Helper()
	if err := db.Exec(`
		INSERT INTO public.gateway_request_logs (
			downstream_key_id, channel_id, channel_identity_revision, channel_name_snapshot, group_name,
			upstream_group_id, upstream_group_generation, upstream_key_local_id, upstream_key_generation,
			price_canonical_group_name, price_model_name, price_operation, price_protocol, usage_schema, usage_schema_version,
			group_ratio, completion_ratio, raw_group_ratio, raw_completion_ratio,
			funding_factor, cost_basis_version, cost_basis_source,
			model_name, method, path, status_code, latency_ms, prompt_tokens, total_tokens,
			cost, raw_cost, cost_source, cost_status, route_reason,
			aggregate_version, group_cache_aggregate_version, created_at
		) VALUES (
			1, ?, (SELECT upstream_account_identity_revision FROM public.channels WHERE id = ?), 'li-mao', 'PLUS',
			1, 1, 1, 1, 'plus', 'gpt-5', 'text_generation', 'openai_responses', 'token_components_v1', 1,
			CAST(? AS numeric), CAST(? AS numeric), CAST(? AS numeric), CAST(? AS numeric),
			CAST(? AS numeric), 7, 'confirmed_redeem_code',
			'gpt-5', 'POST', '/v1/responses', 200, 1, 1, 1,
			CAST(? AS numeric) * CAST(? AS numeric), CAST(? AS numeric), 'sub2api_actual_cost', 'reported', 'smart_primary',
			?, ?, ?
		)`, channelID, channelID, group, completion, rawGroup, rawCompletion, liMaoOldFundingFactor,
		rawCost, costFactor, rawCost, gatewayRequestAggregateVersion, gatewayRequestGroupCacheAggregateVersion, createdAt).Error; err != nil {
		t.Fatal(err)
	}
}

func insertCostlessRestatementFixtureLog(t *testing.T, db *gorm.DB, channelID uint, createdAt time.Time, rawGroup, group, rawCompletion, completion string) {
	t.Helper()
	if err := db.Exec(`
		INSERT INTO public.gateway_request_logs (
			downstream_key_id, channel_id, channel_identity_revision, channel_name_snapshot, group_name,
			upstream_group_id, upstream_group_generation, upstream_key_local_id, upstream_key_generation,
			price_canonical_group_name, price_model_name, price_operation, price_protocol, usage_schema, usage_schema_version,
			group_ratio, completion_ratio, raw_group_ratio, raw_completion_ratio,
			funding_factor, cost_basis_version, cost_basis_source,
			model_name, method, path, status_code, latency_ms, prompt_tokens, total_tokens,
			cost, raw_cost, cost_source, cost_status, route_reason,
			aggregate_version, group_cache_aggregate_version, created_at
		) VALUES (
			1, ?, (SELECT upstream_account_identity_revision FROM public.channels WHERE id = ?), 'li-mao', 'PLUS',
			1, 1, 1, 1, 'plus', 'gpt-5', 'text_generation', 'openai_responses', 'token_components_v1', 1,
			CAST(? AS numeric), CAST(? AS numeric), CAST(? AS numeric), CAST(? AS numeric),
			CAST(? AS numeric), 7, 'confirmed_redeem_code',
			'gpt-5', 'POST', '/v1/responses', 200, 1, 1, 1,
			0, NULL, '', 'none', 'smart_primary', ?, ?, ?
		)`, channelID, channelID, group, completion, rawGroup, rawCompletion, liMaoOldFundingFactor,
		gatewayRequestAggregateVersion, gatewayRequestGroupCacheAggregateVersion, createdAt).Error; err != nil {
		t.Fatal(err)
	}
}

func seedRestatementAggregateRows(t *testing.T, db *gorm.DB, channelID uint, buckets []time.Time) {
	t.Helper()
	for _, bucket := range buckets {
		main := GatewayRequestAggregate{
			BucketStart: bucket, DownstreamKeyID: 1, ChannelID: channelID, ChannelNameSnapshot: "li-mao",
			ModelName: "gpt-5", CostSource: "sub2api_actual_cost", RouteReason: "smart_primary", Result: GatewayRequestResultSuccess,
			Requests: 1, TotalCost: 999, FirstRequestAt: bucket.Add(time.Minute), LastRequestAt: bucket.Add(time.Minute),
			Version: gatewayRequestAggregateVersion,
		}
		if err := db.Create(&main).Error; err != nil {
			t.Fatal(err)
		}
		group := GatewayRequestGroupCacheAggregate{
			BucketStart: bucket, DownstreamKeyID: 1, ChannelID: channelID, ChannelNameSnapshot: "li-mao",
			GroupName: "PLUS", ModelName: "gpt-5", PromptTokens: 1, ValidRequests: 1,
			LastNormalUseAt: bucket.Add(time.Minute), Version: gatewayRequestGroupCacheAggregateVersion,
		}
		if err := db.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func requestRestatementNonTargetDigest(t *testing.T, db *gorm.DB, channelID uint) string {
	t.Helper()
	var digest string
	if err := db.Raw(`
		SELECT md5(string_agg(concat_ws('|', id, created_at, cost, raw_cost, group_ratio, completion_ratio,
			raw_group_ratio, raw_completion_ratio, funding_factor, cost_basis_version, cost_basis_source,
			cost_source, cost_status, route_reason), ',' ORDER BY id))
		FROM public.gateway_request_logs
		WHERE channel_id = ?
		  AND created_at >= ? AND created_at <= ?
		  AND cost_basis_version = 7
		  AND NOT (funding_factor = CAST(? AS numeric) AND raw_cost IS NOT NULL
			AND COALESCE(route_reason, '') <> 'latency_probe' AND raw_group_ratio IS NOT NULL AND raw_group_ratio = group_ratio)`,
		channelID, liMaoAffectedStartedAt, liMaoAffectedEndedAt, liMaoOldFundingFactor).Scan(&digest).Error; err != nil {
		t.Fatal(err)
	}
	return digest
}

func requestRestatementTargetDigest(t *testing.T, db *gorm.DB, operationID uint64) string {
	t.Helper()
	var digest string
	if err := db.Raw(`
		SELECT md5(string_agg(concat_ws('|', logs.id, logs.cost, logs.group_ratio, logs.completion_ratio,
			logs.funding_factor, logs.cost_basis_version, logs.cost_basis_source), ',' ORDER BY logs.id))
		FROM public.channel_cost_unit_request_restatement_items AS item
		JOIN public.gateway_request_logs AS logs ON logs.id = item.request_log_id
		WHERE item.restatement_id = ?`, operationID).Scan(&digest).Error; err != nil {
		t.Fatal(err)
	}
	return digest
}

func assertLiMaoRestatementAmounts(t *testing.T, db *gorm.DB, operationID uint64) {
	t.Helper()
	var valid bool
	if err := db.Raw(`
		SELECT
			round(sum(item.raw_cost), 9) = 1.083942315::numeric
			AND round(sum(item.cost_before), 16) = 0.1089399583757274::numeric
			AND round(sum(item.cost_after), 16) = 1.0893995837572692::numeric
			AND round(sum(item.cost_after - item.cost_before), 16) = 0.9804596253815418::numeric
		FROM public.channel_cost_unit_request_restatement_items AS item
		WHERE item.restatement_id = ?`, operationID).Scan(&valid).Error; err != nil {
		t.Fatal(err)
	}
	if !valid {
		t.Fatal("request restatement amount conservation failed")
	}
}

func assertAffectedAggregateRows(t *testing.T, db *gorm.DB, channelID uint, buckets []time.Time, want int64) {
	t.Helper()
	start := buckets[0]
	end := buckets[len(buckets)-1].Add(gatewayRequestAggregateBucket)
	for _, model := range []any{&GatewayRequestAggregate{}, &GatewayRequestGroupCacheAggregate{}} {
		var count int64
		if err := db.Model(model).Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ?", channelID, start, end).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if want >= 0 && count != want {
			t.Fatalf("%T affected aggregate rows = %d, want %d", model, count, want)
		}
		if want < 0 && count == 0 {
			t.Fatalf("%T affected aggregates were not rebuilt", model)
		}
	}
}

func assertRestatementAggregatesMatchDetails(t *testing.T, db *gorm.DB, channelID uint, buckets []time.Time) {
	t.Helper()
	start := buckets[0]
	end := buckets[len(buckets)-1].Add(gatewayRequestAggregateBucket)
	var matching bool
	if err := db.Raw(`
		SELECT
			round((SELECT COALESCE(sum(total_cost), 0) FROM public.gateway_request_aggregates
				WHERE channel_id = ? AND bucket_start >= ? AND bucket_start < ?), 12)
			=
			round((SELECT COALESCE(sum(cost), 0) FROM public.gateway_request_logs
				WHERE channel_id = ? AND created_at >= ? AND created_at < ?), 12)`,
		channelID, start, end, channelID, start, end).Scan(&matching).Error; err != nil {
		t.Fatal(err)
	}
	if !matching {
		t.Fatal("rebuilt request aggregate money does not match request details")
	}
	var groupRequests int64
	if err := db.Model(&GatewayRequestGroupCacheAggregate{}).
		Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ?", channelID, start, end).
		Select("COALESCE(sum(valid_requests), 0)").Scan(&groupRequests).Error; err != nil {
		t.Fatal(err)
	}
	if groupRequests != 198 {
		t.Fatalf("rebuilt group-cache requests = %d, want 198", groupRequests)
	}
}

func assertRestatementRollback(t *testing.T, db *gorm.DB, fixture *liMaoRequestRestatementFixture) {
	t.Helper()
	state, err := fixture.repo.FindState(fixture.channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	oldFactor := mustParseFixtureFloat(t, liMaoOldFundingFactor)
	if state.Version != fixture.expectedVersion || state.FundingFactor != oldFactor || state.CostUnitEpoch != 1 {
		t.Fatalf("rejected request restatement changed cost state: %+v", state)
	}
	var changedRows int64
	if err := db.Model(&GatewayRequestLog{}).Where("channel_id = ? AND funding_factor = CAST(? AS numeric)", fixture.channel.ID, liMaoNewFundingFactor).
		Count(&changedRows).Error; err != nil {
		t.Fatal(err)
	}
	if changedRows != 0 {
		t.Fatalf("rejected request restatement changed %d request rows", changedRows)
	}
	for _, model := range []any{&ChannelCostUnitRebase{}, &ChannelCostUnitRequestRestatement{}, &ChannelCostUnitRequestRestatementItem{}} {
		var count int64
		if err := db.Model(model).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%T rows after rollback = %d", model, count)
		}
	}
	assertAffectedAggregateRows(t, db, fixture.channel.ID, fixture.bucketStarts, int64(len(fixture.bucketStarts)))
}

func mustParseFixtureFloat(t *testing.T, value string) float64 {
	t.Helper()
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestCostUnitRequestRestatementBucketDigestRejectsPartialOrDuplicateBuckets(t *testing.T) {
	bucket := aggregateBucketStart(time.Now().UTC())
	for _, buckets := range [][]time.Time{{bucket, bucket}, {bucket.Add(time.Minute)}} {
		if _, err := CostUnitRequestRestatementBucketDigest(buckets); !errors.Is(err, ErrCostUnitRequestRestatementInvalid) {
			t.Fatalf("bucket digest error = %v for %v", err, buckets)
		}
	}
	digest, err := CostUnitRequestRestatementBucketDigest([]time.Time{bucket, bucket.Add(gatewayRequestAggregateBucket)})
	if err != nil || len(digest) != 64 {
		t.Fatalf("valid bucket digest = %q, err=%v", digest, err)
	}
}

func ExampleCostUnitRequestRestatementEvidence_frozenBoundary() {
	buckets := []time.Time{
		time.Date(2026, 7, 26, 1, 45, 0, 0, time.UTC),
		time.Date(2026, 7, 26, 2, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 26, 2, 15, 0, 0, time.UTC),
		time.Date(2026, 7, 26, 2, 30, 0, 0, time.UTC),
	}
	digest, _ := CostUnitRequestRestatementBucketDigest(buckets)
	fmt.Println(len(digest), len(buckets))
	// Output: 64 4
}
