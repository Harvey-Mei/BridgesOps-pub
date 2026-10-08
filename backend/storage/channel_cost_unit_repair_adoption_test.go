package storage

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

const repairAdoptionStateVersion uint64 = 9

type costUnitRepairAdoptionFixture struct {
	db                 *gorm.DB
	request            *liMaoRequestRestatementFixture
	preview            *CostUnitRepairAdoptionPreview
	stateSnapshot      string
	projectionSnapshot string
}

func TestCostUnitRepairAdoptionPreviewAndApplyFrozen196RequestPopulation(t *testing.T) {
	db := openTestDB(t)
	fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-196")
	assertCostUnitRepairAdoptionPreview(t, db, fixture)
	insertCostlessRestatementFixtureLog(t, db, fixture.request.channel.ID,
		fixture.preview.Anchor.AffectedEndedAt.Add(time.Hour), "0.06", "0.06", "0.12", "0.12")

	adoption, replayed, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "adopt-events-59-60", "administrator",
	)
	if err != nil {
		t.Fatalf("apply repair adoption: %v", err)
	}
	if replayed || adoption == nil || adoption.ID == 0 {
		t.Fatalf("first repair adoption = %+v, replayed=%v", adoption, replayed)
	}
	if adoption.StateVersion != repairAdoptionStateVersion || adoption.CostUnitEpoch != fixture.preview.Anchor.ExpectedCostUnitEpoch ||
		adoption.ResetEventID != 59 || adoption.CorrectionEventID != 60 ||
		adoption.PreviewDigest != fixture.preview.Digest || adoption.SourceEventsDigest != fixture.preview.SourceEventsDigest ||
		adoption.CostStateDigest != fixture.preview.CostStateDigest || adoption.ChannelProjectionDigest != fixture.preview.ChannelProjectionDigest ||
		adoption.StateSourceAtAdoption != "manual_denomination_reset" || adoption.StateStatusAtAdoption != "ready" ||
		!decimalTextEqual(adoption.FundingFactorBefore, liMaoOldFundingFactor) ||
		!decimalTextEqual(adoption.FundingFactorAfter, liMaoNewFundingFactor) {
		t.Fatalf("stored repair adoption = %+v", adoption)
	}

	stateAfter, err := loadCostUnitRepairStateAnchorTx(db, fixture.request.channel.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	projectionAfter, err := loadCostUnitRepairProjectionAnchorTx(db, fixture.request.channel.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if stateAfter.Snapshot != fixture.stateSnapshot || projectionAfter.Snapshot != fixture.projectionSnapshot {
		t.Fatalf("repair adoption changed current money: state %t, projection %t",
			stateAfter.Snapshot != fixture.stateSnapshot, projectionAfter.Snapshot != fixture.projectionSnapshot)
	}
	if stateAfter.Version != repairAdoptionStateVersion || stateAfter.CostUnitEpoch != fixture.preview.Anchor.ExpectedCostUnitEpoch {
		t.Fatalf("repair adoption advanced state boundary: %+v", stateAfter)
	}

	var rebaseCount int64
	if err := db.Model(&ChannelCostUnitRebase{}).Where("channel_id = ?", fixture.request.channel.ID).Count(&rebaseCount).Error; err != nil {
		t.Fatal(err)
	}
	if rebaseCount != 0 {
		t.Fatalf("historical repair was recorded as %d canonical rebases", rebaseCount)
	}

	var operation ChannelCostUnitRequestRestatement
	if err := db.Where("repair_adoption_id = ?", adoption.ID).First(&operation).Error; err != nil {
		t.Fatal(err)
	}
	if operation.RebaseID != nil || operation.RepairAdoptionID == nil || *operation.RepairAdoptionID != adoption.ID ||
		operation.ExpectedPopulationRows != 196 || operation.AppliedRestatementRows != 124 ||
		operation.ExpectedNewRatioRows != 187 || operation.ExpectedOldRatioRows != 6 ||
		operation.ExpectedRatioMismatchRows != 3 || operation.ExpectedAlreadyCorrectCostedRows != 5 ||
		operation.AppliedCostBasisVersion != repairAdoptionStateVersion ||
		operation.AppliedCostBasisSource != costUnitRepairAdoptionBasisSource {
		t.Fatalf("repair adoption restatement = %+v", operation)
	}
	var itemCount int64
	if err := db.Model(&ChannelCostUnitRequestRestatementItem{}).Where("restatement_id = ?", operation.ID).Count(&itemCount).Error; err != nil {
		t.Fatal(err)
	}
	if itemCount != 124 {
		t.Fatalf("repair adoption restatement items = %d, want 124", itemCount)
	}
	assertLiMaoRestatementAmounts(t, db, operation.ID)

	var restatedRows int64
	if err := db.Raw(`
		SELECT count(*)
		FROM public.gateway_request_logs
		WHERE channel_id = ?
		  AND created_at >= ? AND created_at <= ?
		  AND funding_factor = CAST(? AS numeric)
		  AND cost_basis_version = ?
		  AND cost_basis_source = ?
		  AND raw_cost IS NOT NULL
		  AND cost = raw_cost * CAST(? AS numeric)
		  AND group_ratio = raw_group_ratio * CAST(? AS numeric)`,
		fixture.request.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt, liMaoNewFundingFactor,
		repairAdoptionStateVersion, costUnitRepairAdoptionBasisSource, liMaoNewFundingFactor, liMaoNewFundingFactor).
		Scan(&restatedRows).Error; err != nil {
		t.Fatal(err)
	}
	if restatedRows != 124 {
		t.Fatalf("repair adoption restated rows = %d, want 124", restatedRows)
	}
	var preservedRows int64
	if err := db.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at <= ? AND funding_factor = CAST(? AS numeric)",
			fixture.request.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt, liMaoOldFundingFactor).
		Count(&preservedRows).Error; err != nil {
		t.Fatal(err)
	}
	if preservedRows != 72 {
		t.Fatalf("non-target frozen-population rows = %d, want 72", preservedRows)
	}

	var invalidatedRows int64
	if err := db.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at < ? AND aggregate_version = 0 AND group_cache_aggregate_version = 0",
			fixture.request.channel.ID, fixture.request.bucketStarts[0],
			fixture.request.bucketStarts[len(fixture.request.bucketStarts)-1].Add(gatewayRequestAggregateBucket)).
		Count(&invalidatedRows).Error; err != nil {
		t.Fatal(err)
	}
	if invalidatedRows != 198 {
		t.Fatalf("repair adoption invalidated request markers = %d, want 198", invalidatedRows)
	}
	assertAffectedAggregateRows(t, db, fixture.request.channel.ID, fixture.request.bucketStarts, 0)

	beforeReplay := requestRestatementTargetDigest(t, db, operation.ID)
	replayedAdoption, replayed, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "adopt-events-59-60", "administrator",
	)
	if err != nil || !replayed || replayedAdoption == nil || replayedAdoption.ID != adoption.ID {
		t.Fatalf("idempotent repair adoption replay = %+v, replayed=%v, err=%v", replayedAdoption, replayed, err)
	}
	if afterReplay := requestRestatementTargetDigest(t, db, operation.ID); afterReplay != beforeReplay {
		t.Fatalf("repair adoption replay changed request money: before=%s after=%s", beforeReplay, afterReplay)
	}
	found, err := fixture.request.repo.FindCostUnitRepairAdoption(
		fixture.request.channel.ID, fixture.preview.Anchor.ExpectedIdentityRevision, " adopt-events-59-60 ",
	)
	if err != nil || found.ID != adoption.ID {
		t.Fatalf("find repair adoption = %+v, err=%v", found, err)
	}

	if _, _, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "adopt-events-59-60", "different-actor",
	); !errors.Is(err, ErrCostUnitRepairAdoptionIdempotencyConflict) {
		t.Fatalf("same-key conflicting repair adoption error = %v", err)
	}
	if _, _, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "different-key", "administrator",
	); !errors.Is(err, ErrCostUnitRepairAdoptionIdempotencyConflict) {
		t.Fatalf("same-source conflicting repair adoption error = %v", err)
	}

	var adoptionCount, operationCount int64
	if err := db.Model(&ChannelCostUnitRepairAdoption{}).Where("channel_id = ?", fixture.request.channel.ID).Count(&adoptionCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelCostUnitRequestRestatement{}).Where("repair_adoption_id = ?", adoption.ID).Count(&operationCount).Error; err != nil {
		t.Fatal(err)
	}
	if adoptionCount != 1 || operationCount != 1 || itemCount != 124 {
		t.Fatalf("idempotent evidence counts = adoption %d, operation %d, items %d", adoptionCount, operationCount, itemCount)
	}

	if err := db.Model(&ChannelCostUnitRepairAdoption{}).Where("id = ?", adoption.ID).
		Update("actor", "tampered").Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") {
		t.Fatalf("immutable repair adoption update error = %v", err)
	}
	if err := db.Where("id = ?", adoption.ID).Delete(&ChannelCostUnitRepairAdoption{}).Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") {
		t.Fatalf("immutable repair adoption delete error = %v", err)
	}
	if err := db.Model(&ChannelCostUnitRequestRestatement{}).Where("id = ?", operation.ID).
		Update("actor", "tampered").Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") {
		t.Fatalf("immutable repair restatement update error = %v", err)
	}
	if err := db.Where("restatement_id = ?", operation.ID).Delete(&ChannelCostUnitRequestRestatementItem{}).Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") {
		t.Fatalf("immutable repair restatement item delete error = %v", err)
	}
	if err := db.Where("id = ?", uint64(59)).Delete(&ChannelFundingEvent{}).Error; err == nil || !strings.Contains(err.Error(), "SQLSTATE 23503") {
		t.Fatalf("adopted source event delete error = %v", err)
	}
}

func TestCostUnitRepairAdoptionReplayRejectsRestatementItemDrift(t *testing.T) {
	db := openTestDB(t)
	fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-replay-item-drift")
	adoption, replayed, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "replay-item-drift", "administrator",
	)
	if err != nil || replayed || adoption == nil {
		t.Fatalf("apply repair adoption = %+v, replayed=%v, err=%v", adoption, replayed, err)
	}
	var operation ChannelCostUnitRequestRestatement
	if err := db.Where("repair_adoption_id = ?", adoption.ID).Take(&operation).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`ALTER TABLE public.channel_cost_unit_request_restatement_items
		DISABLE TRIGGER trg_channel_cost_unit_request_restatement_items_append_only`).Error; err != nil {
		t.Fatalf("disable append-only trigger: %v", err)
	}
	if err := db.Exec(`
		UPDATE public.channel_cost_unit_request_restatement_items
		SET cost_after = cost_after + 0.000001
		WHERE id = (
			SELECT min(id)
			FROM public.channel_cost_unit_request_restatement_items
			WHERE restatement_id = ?
		)`, operation.ID).Error; err != nil {
		t.Fatalf("tamper restatement item fixture: %v", err)
	}
	if err := db.Exec(`ALTER TABLE public.channel_cost_unit_request_restatement_items
		ENABLE TRIGGER trg_channel_cost_unit_request_restatement_items_append_only`).Error; err != nil {
		t.Fatalf("restore append-only trigger: %v", err)
	}

	if _, _, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "replay-item-drift", "administrator",
	); !errors.Is(err, ErrCostUnitRepairAdoptionConflict) {
		t.Fatalf("repair adoption replay with item drift error = %v", err)
	}
}

func TestCostUnitRepairAdoptionConcurrentSameKeyIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-concurrent-idempotency")
	type result struct {
		adoption *ChannelCostUnitRepairAdoption
		replayed bool
		err      error
	}

	const callers = 4
	start := make(chan struct{})
	results := make(chan result, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for range callers {
		go func() {
			ready.Done()
			<-start
			adoption, replayed, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
				fixture.preview.Anchor, fixture.preview.Digest, "concurrent-same-key", "administrator",
			)
			results <- result{adoption: adoption, replayed: replayed, err: err}
		}()
	}
	ready.Wait()
	close(start)

	var adoptionID uint64
	applied := 0
	replays := 0
	for range callers {
		got := <-results
		if got.err != nil || got.adoption == nil || got.adoption.ID == 0 {
			t.Fatalf("concurrent repair adoption = %+v, replayed=%v, err=%v", got.adoption, got.replayed, got.err)
		}
		if adoptionID == 0 {
			adoptionID = got.adoption.ID
		} else if got.adoption.ID != adoptionID {
			t.Fatalf("concurrent repair adoption ids = %d and %d", adoptionID, got.adoption.ID)
		}
		if got.replayed {
			replays++
		} else {
			applied++
		}
	}
	if applied != 1 || replays != callers-1 {
		t.Fatalf("concurrent repair adoption results = %d applied, %d replayed", applied, replays)
	}

	for _, model := range []any{&ChannelCostUnitRepairAdoption{}, &ChannelCostUnitRequestRestatement{}} {
		var count int64
		if err := db.Model(model).Where("channel_id = ?", fixture.request.channel.ID).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%T rows after concurrent repair adoption = %d, want 1", model, count)
		}
	}
}

func TestCostUnitRepairAdoptionReplaySurvivesDeletionPrepare(t *testing.T) {
	db := openTestDB(t)
	fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-deletion-replay")
	adoption, replayed, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "deletion-replay", "administrator",
	)
	if err != nil || replayed || adoption == nil {
		t.Fatalf("initial repair adoption = %+v, replayed=%v, err=%v", adoption, replayed, err)
	}
	prepareChannelDeletionForTest(t, db, NewChannels(db), fixture.request.channel.ID)
	replayedAdoption, replayed, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "deletion-replay", "administrator",
	)
	if err != nil || !replayed || replayedAdoption == nil || replayedAdoption.ID != adoption.ID {
		t.Fatalf("repair adoption replay after deletion prepare = %+v, replayed=%v, err=%v", replayedAdoption, replayed, err)
	}
}

func TestCostUnitRepairAdoptionRejectsProviderFactorScaleMismatch(t *testing.T) {
	db := openTestDB(t)
	fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-provider-scale")
	anchor := fixture.preview.Anchor
	anchor.PreviousProviderFactor = "0.2"
	if _, err := fixture.request.repo.PreviewCostUnitRepairAdoption(anchor); !errors.Is(err, ErrCostUnitRepairAdoptionInvalid) {
		t.Fatalf("provider factor scale error = %v, want %v", err, ErrCostUnitRepairAdoptionInvalid)
	}
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", fixture.request.channel.ID).
		Update("observed_provider_factor", 0.9).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.request.repo.PreviewCostUnitRepairAdoption(fixture.preview.Anchor); !errors.Is(err, ErrCostUnitRepairAdoptionInvalid) {
		t.Fatalf("provider observation mismatch error = %v, want %v", err, ErrCostUnitRepairAdoptionInvalid)
	}
	currentProviderFactor := mustParseFixtureFloat(t, liMaoCurrentProviderFactor)
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", fixture.request.channel.ID).Updates(map[string]any{
		"observed_provider_factor":            currentProviderFactor,
		"observed_provider_factor_sampled_at": fixture.preview.Anchor.CostSampledAtAfter.Add(-time.Microsecond),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.request.repo.PreviewCostUnitRepairAdoption(fixture.preview.Anchor); !errors.Is(err, ErrCostUnitRepairAdoptionInvalid) {
		t.Fatalf("pre-correction provider sample error = %v, want %v", err, ErrCostUnitRepairAdoptionInvalid)
	}
}

func TestCostUnitRepairAdoptionRejectsCostedRatioMismatchAtPreview(t *testing.T) {
	db := openTestDB(t)
	fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-costed-ratio-mismatch")
	result := db.Exec(`
UPDATE public.gateway_request_logs
SET raw_cost = 0.001::numeric,
    cost = 0.001::numeric * CAST(? AS numeric)
WHERE id = (
    SELECT id
    FROM public.gateway_request_logs
    WHERE channel_id = ?
      AND funding_factor = CAST(? AS numeric)
      AND raw_cost IS NULL
      AND raw_group_ratio IS NOT NULL
      AND raw_group_ratio <> group_ratio
      AND abs(group_ratio - raw_group_ratio * CAST(? AS numeric)) > 0.000000000001::numeric
    ORDER BY id
    LIMIT 1
)`, liMaoOldFundingFactor, fixture.request.channel.ID, liMaoOldFundingFactor, liMaoOldFundingFactor)
	if result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("seed costed ratio mismatch: rows=%d err=%v", result.RowsAffected, result.Error)
	}
	if _, err := fixture.request.repo.PreviewCostUnitRepairAdoption(fixture.preview.Anchor); !errors.Is(err, ErrCostUnitRequestRestatementInvalid) {
		t.Fatalf("costed ratio-mismatch preview error = %v, want %v", err, ErrCostUnitRequestRestatementInvalid)
	}
	assertCostUnitRepairAdoptionRollback(t, fixture, 196)
}

func TestCostUnitRepairAdoptionLateReconcileUsesRestatedBasis(t *testing.T) {
	db := openTestDB(t)
	fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-late-reconcile")
	adoption, replayed, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
		fixture.preview.Anchor, fixture.preview.Digest, "late-reconcile", "administrator",
	)
	if err != nil || replayed || adoption == nil {
		t.Fatalf("apply repair adoption = %+v, replayed=%v, err=%v", adoption, replayed, err)
	}

	var operation ChannelCostUnitRequestRestatement
	if err := db.Where("repair_adoption_id = ?", adoption.ID).Take(&operation).Error; err != nil {
		t.Fatal(err)
	}
	var item ChannelCostUnitRequestRestatementItem
	if err := db.Where("restatement_id = ?", operation.ID).Order("request_log_id ASC").Take(&item).Error; err != nil {
		t.Fatal(err)
	}
	rawCost := mustParseFixtureFloat(t, item.RawCost)
	staleActualCost := rawCost * mustParseFixtureFloat(t, liMaoOldFundingFactor)
	if err := NewGateway(db).UpdateRequestLogActualCostWithUsage(
		uint(item.RequestLogID), "late-usage", rawCost, staleActualCost, "sub2api_actual_cost", GatewayRequestUsageMetrics{},
	); err != nil {
		t.Fatalf("late request reconciliation: %v", err)
	}

	var stored GatewayRequestLog
	if err := db.First(&stored, item.RequestLogID).Error; err != nil {
		t.Fatal(err)
	}
	wantActualCost := rawCost * mustParseFixtureFloat(t, liMaoNewFundingFactor)
	if math.Abs(stored.Cost-wantActualCost) > 1e-12 || stored.Cost == staleActualCost ||
		stored.CostBasisVersion != repairAdoptionStateVersion || stored.CostBasisSource != costUnitRepairAdoptionBasisSource ||
		stored.CostStatus != GatewayCostStatusReconciled {
		t.Fatalf("late reconciliation used stale basis: %+v, want cost %.16f", stored, wantActualCost)
	}
	if _, err := BackfillGatewayRequestAggregates(context.Background(), db); err != nil {
		t.Fatalf("backfill after late reconciliation: %v", err)
	}
	assertRestatementAggregatesMatchDetails(t, db, fixture.request.channel.ID, fixture.request.bucketStarts)
}

func TestCostUnitRepairAdoptionAndReconcileUseCompatibleLockOrder(t *testing.T) {
	db := openTestDB(t)
	fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-reconcile-lock-order")
	if _, err := BackfillGatewayRequestAggregates(context.Background(), db); err != nil {
		t.Fatalf("complete aggregate backfill before lock-order test: %v", err)
	}
	var target GatewayRequestLog
	if err := db.Where(
		"channel_id = ? AND funding_factor = CAST(? AS numeric) AND raw_cost IS NOT NULL AND raw_group_ratio = group_ratio",
		fixture.request.channel.ID, liMaoOldFundingFactor,
	).Order("id ASC").Take(&target).Error; err != nil {
		t.Fatal(err)
	}
	if target.RawCost == nil {
		t.Fatal("lock-order target has no raw cost")
	}
	oldFactor := mustParseFixtureFloat(t, liMaoOldFundingFactor)
	if err := NewGateway(db).UpdateRequestLogActualCostWithUsage(
		target.ID, "normalize-concurrent-reconcile", *target.RawCost,
		*target.RawCost*oldFactor, "sub2api_actual_cost", GatewayRequestUsageMetrics{},
	); err != nil {
		t.Fatalf("normalize lock-order target: %v", err)
	}
	if err := db.First(&target, target.ID).Error; err != nil {
		t.Fatal(err)
	}
	refreshedPreview, err := fixture.request.repo.PreviewCostUnitRepairAdoption(fixture.preview.Anchor)
	if err != nil {
		t.Fatalf("refresh preview after target normalization: %v", err)
	}
	fixture.preview = refreshedPreview

	rowLocked := make(chan struct{})
	releaseRow := make(chan struct{})
	var blockOnce sync.Once
	var releaseOnce sync.Once
	const callbackName = "test:repair-adoption-reconcile-lock-order"
	if err := db.Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		request, ok := tx.Statement.Dest.(*GatewayRequestLog)
		if !ok || request.ID != target.ID {
			return
		}
		blockOnce.Do(func() {
			close(rowLocked)
			<-releaseRow
		})
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseRow) })
		_ = db.Callback().Query().Remove(callbackName)
	})

	reconcileErr := make(chan error, 1)
	go func() {
		rawCost := *target.RawCost
		reconcileErr <- NewGateway(db).UpdateRequestLogActualCostWithUsage(
			target.ID, "concurrent-reconcile", rawCost, target.Cost,
			"sub2api_actual_cost", GatewayRequestUsageMetrics{},
		)
	}()
	select {
	case <-rowLocked:
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler did not lock the target request")
	}

	type applyResult struct {
		adoption *ChannelCostUnitRepairAdoption
		err      error
	}
	applyDone := make(chan applyResult, 1)
	go func() {
		adoption, _, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
			fixture.preview.Anchor, fixture.preview.Digest, "concurrent-reconcile", "administrator",
		)
		applyDone <- applyResult{adoption: adoption, err: err}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case result := <-applyDone:
			t.Fatalf("repair adoption finished before requesting its table lock: adoption=%+v err=%v", result.adoption, result.err)
		default:
		}
		var exclusiveRequests int64
		if err := db.Raw(`
SELECT count(*)
FROM pg_locks
WHERE relation = 'public.gateway_request_logs'::regclass
	  AND mode = 'ExclusiveLock'`).Scan(&exclusiveRequests).Error; err != nil {
			t.Fatal(err)
		}
		if exclusiveRequests > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("repair adoption did not request its table lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	releaseOnce.Do(func() { close(releaseRow) })

	select {
	case err := <-reconcileErr:
		if err != nil {
			t.Fatalf("concurrent reconciliation: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent reconciliation timed out")
	}
	select {
	case result := <-applyDone:
		if result.err != nil || result.adoption == nil {
			t.Fatalf("concurrent repair adoption = %+v, err=%v", result.adoption, result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent repair adoption timed out")
	}

	var stored GatewayRequestLog
	if err := db.First(&stored, target.ID).Error; err != nil {
		t.Fatal(err)
	}
	wantCost := *target.RawCost * mustParseFixtureFloat(t, liMaoNewFundingFactor)
	if math.Abs(stored.Cost-wantCost) > 1e-12 || stored.CostBasisSource != costUnitRepairAdoptionBasisSource {
		t.Fatalf("concurrent repair stored request = %+v, want cost %.16f", stored, wantCost)
	}
}

func TestRequestLogActualCostForCurrentBasisUsesLockedFactor(t *testing.T) {
	factor := 2.0
	for _, source := range []string{"cost_unit_rebase", costUnitRepairAdoptionBasisSource} {
		actual, err := requestLogActualCostForCurrentBasis(&GatewayRequestLog{
			CostBasisSource: source, FundingFactor: &factor,
		}, 3, 0.3)
		if err != nil || actual != 6 {
			t.Fatalf("source %q actual = %v, err=%v", source, actual, err)
		}
	}
	actual, err := requestLogActualCostForCurrentBasis(&GatewayRequestLog{
		CostBasisSource: "confirmed_redeem_code", FundingFactor: &factor,
	}, 3, 0.3)
	if err != nil || actual != 6 {
		t.Fatalf("ordinary basis actual = %v, err=%v", actual, err)
	}
	actual, err = requestLogActualCostForCurrentBasis(&GatewayRequestLog{}, 3, 0.3)
	if err != nil || actual != 0.3 {
		t.Fatalf("unknown basis actual = %v, err=%v", actual, err)
	}
}

func TestRequestLogActualCostUsesLockedOrdinaryFundingBasis(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	oldFactor := 1.0
	log := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, ChannelNameSnapshot: "ordinary-funding-race",
		ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200,
		FundingFactor: &oldFactor, CostBasisVersion: 7, CostBasisSource: "confirmed_redeem_code",
		Cost: 0.25, CostEstimated: true, CostSource: "gateway_estimate", CreatedAt: time.Now().UTC(),
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Updates(map[string]any{
		"funding_factor": 2, "cost_basis_version": 8, "cost_basis_source": "confirmed_recharge",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateRequestLogActualCostWithUsage(
		log.ID, "ordinary-funding-race", 0.25, 0.25, "sub2api_actual_cost", GatewayRequestUsageMetrics{},
	); err != nil {
		t.Fatal(err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.RawCost == nil || *stored.RawCost != 0.25 || stored.Cost != 0.5 || stored.FundingFactor == nil || *stored.FundingFactor != 2 ||
		stored.CostBasisVersion != 8 || stored.CostBasisSource != "confirmed_recharge" {
		t.Fatalf("late ordinary reconciliation used stale basis: %+v", stored)
	}
}

func TestCostUnitRepairAdoptionRejectsPreviewDriftAndRollsBack(t *testing.T) {
	tests := []struct {
		name          string
		want          error
		previewDigest string
		population    int64
		mutate        func(*testing.T, *costUnitRepairAdoptionFixture)
	}{
		{
			name: "preview digest", want: ErrCostUnitRepairAdoptionConflict,
			previewDigest: strings.Repeat("f", 64),
		},
		{
			name: "cost state snapshot", want: ErrCostUnitRepairAdoptionConflict,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				if err := fixture.db.Model(&ChannelCostState{}).Where("channel_id = ?", fixture.request.channel.ID).
					Update("last_error", "state changed after preview").Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "channel projection snapshot", want: ErrCostUnitRepairAdoptionConflict,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				changedAt := fixture.preview.Anchor.CostSampledAtAfter.Add(time.Minute)
				if err := fixture.db.Model(&Channel{}).Where("id = ?", fixture.request.channel.ID).
					Update("rate_snapshot_observed_at", changedAt).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "channel projection financial mismatch", want: ErrCostUnitRepairAdoptionConflict,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				if err := fixture.db.Model(&Channel{}).Where("id = ?", fixture.request.channel.ID).
					Update("raw_total_cost", fixture.request.next.RawTotalCost+1).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "cost state not ready", want: ErrCostUnitRepairAdoptionConflict,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				if err := fixture.db.Model(&ChannelCostState{}).Where("channel_id = ?", fixture.request.channel.ID).
					Updates(map[string]any{"status": "needs_review", "last_error": "review required"}).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "account identity evidence", want: ErrCostUnitRepairAdoptionConflict,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				if err := fixture.db.Table("gateway_channel_upstream_account_bindings").
					Where("channel_id = ?", fixture.request.channel.ID).
					Updates(map[string]any{"source": "auth_me_refreshed", "observed_at": fixture.preview.Anchor.CostSampledAtAfter}).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "source event snapshot", want: ErrCostUnitRepairAdoptionConflict,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				if err := fixture.db.Model(&ChannelFundingEvent{}).Where("id = ?", uint64(60)).
					Update("note", "event changed after preview").Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "source event high water", want: ErrCostUnitRepairAdoptionInvalid,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				factor := mustParseFixtureFloat(t, liMaoNewFundingFactor)
				event := &ChannelFundingEvent{
					ChannelID: fixture.request.channel.ID, Kind: "recharge",
					FundingFactorBefore: factor, FundingFactorAfter: factor,
					EffectiveAt: fixture.preview.Anchor.CostSampledAtAfter.Add(time.Minute),
				}
				if err := fixture.db.Create(event).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "late affected request", want: ErrCostUnitRequestRestatementConflict, population: 197,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				insertCostlessRestatementFixtureLog(t, fixture.db, fixture.request.channel.ID,
					fixture.preview.Anchor.AffectedEndedAt, "0.06", "0.06", "0.12", "0.12")
			},
		},
		{
			name: "future aggregate marker", want: errGatewayRequestAggregateVersionConflict,
			mutate: func(t *testing.T, fixture *costUnitRepairAdoptionFixture) {
				t.Helper()
				var requestID uint64
				if err := fixture.db.Raw(`
					SELECT id FROM public.gateway_request_logs
					WHERE channel_id = ? AND created_at >= ? AND created_at <= ?
					ORDER BY id LIMIT 1`, fixture.request.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt).
					Scan(&requestID).Error; err != nil {
					t.Fatal(err)
				}
				if requestID == 0 {
					t.Fatal("repair adoption fixture has no request to mark")
				}
				if err := fixture.db.Model(&GatewayRequestLog{}).Where("id = ?", requestID).
					Update("aggregate_version", gatewayRequestAggregateVersion+1).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			fixture := seedCostUnitRepairAdoptionFixture(t, db, "repair-adoption-drift-"+strings.ReplaceAll(test.name, " ", "-"))
			if test.mutate != nil {
				test.mutate(t, fixture)
			}
			digest := test.previewDigest
			if digest == "" {
				digest = fixture.preview.Digest
			}
			if _, _, err := fixture.request.repo.ApplyCostUnitRepairAdoption(
				fixture.preview.Anchor, digest, "drift-key", "administrator",
			); !errors.Is(err, test.want) {
				t.Fatalf("repair adoption drift error = %v, want %v", err, test.want)
			}
			assertCostUnitRepairAdoptionRollback(t, fixture, test.population)
		})
	}
}

func seedCostUnitRepairAdoptionFixture(t *testing.T, db *gorm.DB, name string) *costUnitRepairAdoptionFixture {
	t.Helper()
	request := seedLiMaoRequestRestatementFixture(t, db, name)
	var stateBefore ChannelCostState
	if err := db.First(&stateBefore, "channel_id = ?", request.channel.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stateBefore.CostSampledAt == nil || request.next.CostSampledAt == nil || request.next.BalanceSampledAt == nil {
		t.Fatal("repair adoption fixture is missing sampling bounds")
	}
	currentProviderFactor := mustParseFixtureFloat(t, liMaoCurrentProviderFactor)
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", request.channel.ID).Updates(map[string]any{
		"raw_balance": request.next.RawBalance, "actual_inventory": request.next.ActualInventory,
		"funding_factor": request.next.FundingFactor,
		"raw_today_cost": request.next.RawTodayCost, "actual_today_cost": request.next.ActualTodayCost,
		"raw_total_cost": request.next.RawTotalCost, "actual_total_cost": request.next.ActualTotalCost,
		"balance_sampled_at": request.next.BalanceSampledAt, "cost_sampled_at": request.next.CostSampledAt,
		"observed_provider_factor": currentProviderFactor, "observed_provider_factor_source": "advertised",
		"observed_provider_factor_sampled_at": request.next.CostSampledAt.Add(time.Minute),
		"cost_unit_epoch":                     request.next.CostUnitEpoch, "version": repairAdoptionStateVersion,
		"source": "manual_denomination_reset", "status": "ready", "last_error": "",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&Channel{}).Where("id = ?", request.channel.ID).Updates(map[string]any{
		"raw_last_balance": request.next.RawBalance, "last_balance": request.next.ActualInventory,
		"last_balance_at": request.next.BalanceSampledAt, "funding_factor": request.next.FundingFactor,
		"funding_factor_source": "manual_denomination_reset",
		"raw_today_cost":        request.next.RawTodayCost, "today_cost": request.next.ActualTodayCost,
		"raw_total_cost": request.next.RawTotalCost, "total_cost": request.next.ActualTotalCost,
		"today_cost_at": request.next.CostSampledAt, "cost_unit_epoch": request.next.CostUnitEpoch,
	}).Error; err != nil {
		t.Fatal(err)
	}

	insertCostUnitRepairAdoptionEvent(t, db, 59, request.channel.ID, "denomination_reset",
		liMaoOldFundingFactor, liMaoNewFundingFactor, liMaoAffectedEndedAt, liMaoAffectedEndedAt)
	insertCostUnitRepairAdoptionEvent(t, db, 60, request.channel.ID, "denomination_cost_fix",
		liMaoNewFundingFactor, liMaoNewFundingFactor, request.next.CostSampledAt.UTC(), request.next.CostSampledAt.UTC())
	var sequenceValue uint64
	if err := db.Raw(`SELECT setval(pg_get_serial_sequence('public.channel_funding_events', 'id'), 60, true)`).
		Scan(&sequenceValue).Error; err != nil {
		t.Fatal(err)
	}

	preview, err := request.repo.PreviewCostUnitRepairAdoption(CostUnitRepairAdoptionAnchor{
		ChannelID: request.channel.ID, ResetEventID: 59, CorrectionEventID: 60,
		CostSampledAtBefore: stateBefore.CostSampledAt.UTC(), CostSampledAtAfter: request.next.CostSampledAt.UTC(),
		AffectedStartedAt: liMaoAffectedStartedAt, AffectedEndedAt: liMaoAffectedEndedAt,
		PreviousProviderFactor: liMaoPreviousProviderFactor, CurrentProviderFactor: liMaoCurrentProviderFactor,
		FactorSource: "advertised",
	})
	if err != nil {
		t.Fatalf("preview repair adoption: %v", err)
	}
	stateAnchor, err := loadCostUnitRepairStateAnchorTx(db, request.channel.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	projectionAnchor, err := loadCostUnitRepairProjectionAnchorTx(db, request.channel.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	return &costUnitRepairAdoptionFixture{
		db: db, request: request, preview: preview,
		stateSnapshot: stateAnchor.Snapshot, projectionSnapshot: projectionAnchor.Snapshot,
	}
}

func insertCostUnitRepairAdoptionEvent(t *testing.T, db *gorm.DB, id uint64, channelID uint, kind, factorBefore, factorAfter string, effectiveAt, createdAt time.Time) {
	t.Helper()
	var insertedID uint64
	if err := db.Raw(`
		INSERT INTO public.channel_funding_events (
			id, channel_id, kind, paid_amount, credited_amount,
			raw_balance_before, raw_balance_after,
			actual_inventory_before, actual_inventory_after,
			funding_factor_before, funding_factor_after,
			raw_today_cost_before, raw_total_cost_before, note,
			effective_at, created_at
		) VALUES (
			?, ?, ?, 0, 0, 0, 0, 0, 0,
			CAST(? AS numeric), CAST(? AS numeric), 0, 0, ?, ?, ?
		)
		RETURNING id`, id, channelID, kind, factorBefore, factorAfter, kind, effectiveAt, createdAt).
		Scan(&insertedID).Error; err != nil {
		t.Fatal(err)
	}
	if insertedID != id {
		t.Fatalf("inserted repair event id = %d, want %d", insertedID, id)
	}
}

func assertCostUnitRepairAdoptionPreview(t *testing.T, db *gorm.DB, fixture *costUnitRepairAdoptionFixture) {
	t.Helper()
	preview := fixture.preview
	evidence := preview.RequestRestatement
	if preview.Anchor.ExpectedIdentityRevision == 0 || preview.Anchor.ExpectedAccountRecordID == 0 ||
		preview.Anchor.ExpectedStateVersion != repairAdoptionStateVersion || preview.Anchor.ExpectedCostUnitEpoch == 0 ||
		preview.Anchor.SourceEventHighWater != 60 || preview.Anchor.RequestLogHighWater != fixture.request.evidence.RequestLogHighWater ||
		!decimalTextEqual(preview.Anchor.PreviousProviderFactor, liMaoPreviousProviderFactor) ||
		!decimalTextEqual(preview.Anchor.CurrentProviderFactor, liMaoCurrentProviderFactor) ||
		len(preview.Digest) != 64 || len(preview.SourceEventsDigest) != 64 || len(preview.CostStateDigest) != 64 ||
		len(preview.ChannelProjectionDigest) != 64 || evidence == nil {
		t.Fatalf("repair adoption preview = %+v", preview)
	}
	if !decimalTextEqual(preview.FundingFactorBefore, liMaoOldFundingFactor) ||
		!decimalTextEqual(preview.FundingFactorAfter, liMaoNewFundingFactor) ||
		evidence.ExpectedPopulationRows != 196 || evidence.ExpectedNewRatioRows != 187 || evidence.ExpectedOldRatioRows != 6 ||
		evidence.ExpectedRatioMismatchRows != 3 || evidence.ExpectedRestatementRows != 124 ||
		evidence.ExpectedAlreadyCorrectCostedRows != 5 || evidence.ExpectedLatencyProbeRows != 0 ||
		!sameTimeList(evidence.ExpectedBucketStarts, fixture.request.bucketStarts) ||
		evidence.ExpectedBucketDigest != fixture.request.evidence.ExpectedBucketDigest {
		t.Fatalf("derived repair restatement evidence = %+v", evidence)
	}
	var amountsMatch bool
	if err := db.Raw(`
		SELECT
			round(CAST(? AS numeric), 9) = CAST(? AS numeric)
			AND round(CAST(? AS numeric), 16) = CAST(? AS numeric)
			AND round(CAST(? AS numeric), 16) = CAST(? AS numeric)
			AND round(CAST(? AS numeric), 16) = CAST(? AS numeric)
			AND round(CAST(? AS numeric), 9) = CAST(? AS numeric)
			AND round(CAST(? AS numeric), 9) = CAST(? AS numeric)`,
		evidence.ExpectedRawCostSum, liMaoRawCostSum,
		evidence.ExpectedRecordedCostSum, liMaoRecordedCostSum,
		evidence.ExpectedCorrectedCostSum, liMaoCorrectedCostSum,
		evidence.ExpectedUndercountSum, liMaoUndercountSum,
		evidence.ExpectedAlreadyCorrectRawCostSum, liMaoAlreadyCorrectRawCostSum,
		evidence.ExpectedAllCostedRawCostSum, liMaoAllCostedRawCostSum).
		Scan(&amountsMatch).Error; err != nil {
		t.Fatal(err)
	}
	if !amountsMatch {
		t.Fatalf("derived repair restatement amounts = %+v", evidence)
	}
}

func assertCostUnitRepairAdoptionRollback(t *testing.T, fixture *costUnitRepairAdoptionFixture, wantPopulation int64) {
	t.Helper()
	if wantPopulation == 0 {
		wantPopulation = 196
	}
	for _, model := range []any{&ChannelCostUnitRepairAdoption{}, &ChannelCostUnitRequestRestatement{}, &ChannelCostUnitRequestRestatementItem{}, &ChannelCostUnitRebase{}} {
		var count int64
		if err := fixture.db.Model(model).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%T rows after rejected repair adoption = %d", model, count)
		}
	}
	var restatedRows int64
	if err := fixture.db.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND cost_basis_source = ?", fixture.request.channel.ID, costUnitRepairAdoptionBasisSource).
		Count(&restatedRows).Error; err != nil {
		t.Fatal(err)
	}
	if restatedRows != 0 {
		t.Fatalf("rejected repair adoption changed %d request rows", restatedRows)
	}
	var frozenPopulation int64
	if err := fixture.db.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at <= ? AND funding_factor = CAST(? AS numeric)",
			fixture.request.channel.ID, liMaoAffectedStartedAt, liMaoAffectedEndedAt, liMaoOldFundingFactor).
		Count(&frozenPopulation).Error; err != nil {
		t.Fatal(err)
	}
	if frozenPopulation != wantPopulation {
		t.Fatalf("rejected repair adoption left frozen population = %d, want %d", frozenPopulation, wantPopulation)
	}
	assertAffectedAggregateRows(t, fixture.db, fixture.request.channel.ID, fixture.request.bucketStarts, int64(len(fixture.request.bucketStarts)))
}
