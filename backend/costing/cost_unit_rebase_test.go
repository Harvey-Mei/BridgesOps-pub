package costing

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

const costUnitFloatTolerance = 1e-10

func TestCostUnitRebaseCounterMath(t *testing.T) {
	const (
		oldRawToday = 7.054747725
		oldRawTotal = 805.956025725
		newRawToday = 3.4234572092
		newRawTotal = 802.3247352092
	)
	oldPrior, err := nonNegativeDifference(oldRawTotal, oldRawToday)
	if err != nil {
		t.Fatal(err)
	}
	newPrior, err := nonNegativeDifference(newRawTotal, newRawToday)
	if err != nil {
		t.Fatal(err)
	}
	if !sameMonetaryCounter(newPrior, oldPrior) {
		t.Fatalf("production prior anchors differ: old=%0.12f new=%0.12f", oldPrior, newPrior)
	}
	if sameMonetaryCounter(500-5, oldPrior) || sameMonetaryCounter(500-5, oldPrior*0.1) {
		t.Fatal("ambiguous counter transition matched a supported epoch")
	}
	if !sameMonetaryCounter(101-3, (1000-20)*0.1) {
		t.Fatal("scaled prior anchor was not recognized")
	}
	mode, err := historicalMultiplierMode(10, 0.1, connector.RechargeMultiplierModeMultiply)
	if err != nil || mode != connector.RechargeMultiplierModeDivide {
		t.Fatalf("historical multiplier mode = %q, err=%v", mode, err)
	}
}

func TestCostUnitRebaseDivideTenToOnePreservesActualLedger(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := createCostUnitTestChannel(t, db, "divide-ten-to-one")

	beforeBalanceAt := time.Date(2026, 7, 26, 1, 45, 0, 0, time.UTC)
	beforeCostAt := beforeBalanceAt.Add(10 * time.Second)
	const (
		expectedVersion = uint64(11)
		oldFactor       = 0.10050346486909446
		oldRawBalance   = 594.14400491
		oldRawToday     = 7.054747725
		oldActualToday  = 0.709998186961003
		oldRawTotal     = 805.956025725
		oldActualTotal  = 82.2981229088619
	)
	seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
		Version: expectedVersion, RawBalance: oldRawBalance,
		ActualInventory: oldRawBalance * oldFactor, FundingFactor: oldFactor,
		RawTodayCost: oldRawToday, ActualTodayCost: oldActualToday,
		RawTotalCost: oldRawTotal, ActualTotalCost: oldActualTotal,
		BalanceSampledAt: beforeBalanceAt, CostSampledAt: beforeCostAt,
		ObservedProviderFactor: 0.1, ObservedProviderFactorSource: "advertised",
		Source: "confirmed_redeem_code",
	})

	preBalance := storage.BalanceSnapshot{
		ChannelID: channel.ID, Balance: oldRawBalance * oldFactor,
		RawBalance: floatPointer(oldRawBalance), FundingFactor: floatPointer(oldFactor),
		CostBasisVersion: expectedVersion, SampledAt: beforeBalanceAt,
	}
	if err := db.Create(&preBalance).Error; err != nil {
		t.Fatal(err)
	}
	preCost := storage.CostSnapshot{
		ChannelID: channel.ID, TodayCost: oldActualToday, TotalCost: oldActualTotal,
		RawTodayCost: floatPointer(oldRawToday), RawTotalCost: floatPointer(oldRawTotal),
		FundingFactor: floatPointer(oldFactor), CostBasisVersion: expectedVersion, SampledAt: beforeCostAt,
	}
	if err := db.Create(&preCost).Error; err != nil {
		t.Fatal(err)
	}
	preLog := storage.GatewayRequestLog{
		ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: "legacy-unit",
		Method: "POST", Path: "/v1/responses", StatusCode: 200,
		Cost: 0.25, RawCost: floatPointer(2.487476), FundingFactor: floatPointer(oldFactor),
		CostBasisVersion: expectedVersion, CostBasisSource: "confirmed_redeem_code",
		CreatedAt: beforeCostAt.Add(20 * time.Second),
	}
	if err := db.Create(&preLog).Error; err != nil {
		t.Fatal(err)
	}
	var preBalanceBefore storage.BalanceSnapshot
	if err := db.First(&preBalanceBefore, preBalance.ID).Error; err != nil {
		t.Fatal(err)
	}
	var preCostBefore storage.CostSnapshot
	if err := db.First(&preCostBefore, preCost.ID).Error; err != nil {
		t.Fatal(err)
	}
	var preLogBefore storage.GatewayRequestLog
	if err := db.First(&preLogBefore, preLog.ID).Error; err != nil {
		t.Fatal(err)
	}

	// The production incident did not rescale cumulative raw cost by 0.1.
	// Today's and cumulative counters each start a new anchor at the boundary.
	const (
		newRawBalance = 67.31368018
		newRawToday   = 3.4234572092
		newRawTotal   = 802.3247352092
	)
	assertCostUnitFloat(t, "unchanged prior raw anchor", newRawTotal-newRawToday, oldRawTotal-oldRawToday)
	afterBalanceAt := beforeBalanceAt.Add(time.Hour)
	afterCostAt := afterBalanceAt.Add(10 * time.Second)
	command := CostUnitRebaseCommand{
		ExpectedVersion: expectedVersion, PreviousMultiplier: 10, CurrentMultiplier: 1,
		IdempotencyKey: "demo-20260726-10-to-1", Actor: "administrator",
		Note: "provider changed its raw monetary unit",
	}
	result, err := NewService(storage.NewCosting(db)).RebaseCostUnit(command, withCostUnitIdentityFence(t, db, CostUnitRebaseObservation{
		// The persisted old factor, not the channel's current mode, defines the
		// previous side of the transition.
		ChannelID: channel.ID, MultiplierMode: connector.RechargeMultiplierModeMultiply,
		ObservedFactor: 1, FactorSource: "advertised", RawBalance: newRawBalance,
		RawTodayCost: newRawToday, RawTotalCost: newRawTotal,
		BalanceSampledAt: afterBalanceAt, CostSampledAt: afterCostAt,
		RateEvidence: validCostUnitRateEvidence(beforeCostAt),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.State == nil || result.Audit == nil || result.IdempotentReplay {
		t.Fatalf("rebase result = %+v", result)
	}

	newFactor := oldFactor * (1 / 0.1)
	priorActual := oldActualTotal - oldActualToday
	wantActualToday := newRawToday * newFactor
	wantActualTotal := priorActual + wantActualToday
	assertCostUnitFloat(t, "funding factor", result.State.FundingFactor, newFactor)
	assertCostUnitFloat(t, "raw today", result.State.RawTodayCost, newRawToday)
	assertCostUnitFloat(t, "actual today at epoch boundary", result.State.ActualTodayCost, wantActualToday)
	assertCostUnitFloat(t, "raw total", result.State.RawTotalCost, newRawTotal)
	assertCostUnitFloat(t, "actual total at epoch boundary", result.State.ActualTotalCost, wantActualTotal)
	assertCostUnitFloat(t, "actual inventory", result.State.ActualInventory, newRawBalance*newFactor)
	if result.State.Version != expectedVersion+1 || result.State.Source != "cost_unit_rebase" {
		t.Fatalf("rebased state version/source = %+v", result.State)
	}
	if result.State.ObservedProviderFactor == nil || *result.State.ObservedProviderFactor != 1 || result.State.ObservedProviderFactorSource != "advertised" {
		t.Fatalf("provider factor evidence = %+v", result.State)
	}

	audit := result.Audit
	assertCostUnitFloat(t, "audit funding factor before", audit.FundingFactorBefore, oldFactor)
	assertCostUnitFloat(t, "audit funding factor after", audit.FundingFactorAfter, newFactor)
	assertCostUnitFloat(t, "audit raw total before", audit.RawTotalCostBefore, oldRawTotal)
	assertCostUnitFloat(t, "audit raw total after", audit.RawTotalCostAfter, newRawTotal)
	assertCostUnitFloat(t, "audit actual total before", audit.ActualTotalCostBefore, oldActualTotal)
	assertCostUnitFloat(t, "audit actual total after", audit.ActualTotalCostAfter, wantActualTotal)
	if audit.ExpectedStateVersion != expectedVersion || audit.AppliedStateVersion != expectedVersion+1 ||
		audit.PreviousProviderFactor != 0.1 || audit.CurrentProviderFactor != 1 {
		t.Fatalf("rebase audit versions/factors = %+v", audit)
	}

	var storedChannel storage.Channel
	if err := db.First(&storedChannel, channel.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedChannel.LastBalance == nil || storedChannel.TotalCost == nil {
		t.Fatalf("channel projection is incomplete: %+v", storedChannel)
	}
	assertCostUnitFloat(t, "projected balance", *storedChannel.LastBalance, newRawBalance*newFactor)
	assertCostUnitFloat(t, "projected total", *storedChannel.TotalCost, wantActualTotal)

	var storedPreBalance storage.BalanceSnapshot
	if err := db.First(&storedPreBalance, preBalance.ID).Error; err != nil {
		t.Fatal(err)
	}
	var storedPreCost storage.CostSnapshot
	if err := db.First(&storedPreCost, preCost.ID).Error; err != nil {
		t.Fatal(err)
	}
	var storedPreLog storage.GatewayRequestLog
	if err := db.First(&storedPreLog, preLog.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(preBalanceBefore, storedPreBalance) {
		t.Fatalf("pre-rebase balance snapshot was rewritten: before=%+v after=%+v", preBalanceBefore, storedPreBalance)
	}
	if !reflect.DeepEqual(preCostBefore, storedPreCost) {
		t.Fatalf("pre-rebase cost snapshot was rewritten: before=%+v after=%+v", preCostBefore, storedPreCost)
	}
	if !reflect.DeepEqual(preLogBefore, storedPreLog) {
		t.Fatalf("pre-rebase request log was rewritten: before=%+v after=%+v", preLogBefore, storedPreLog)
	}

	var balanceSnapshots []storage.BalanceSnapshot
	if err := db.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&balanceSnapshots).Error; err != nil {
		t.Fatal(err)
	}
	var costSnapshots []storage.CostSnapshot
	if err := db.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&costSnapshots).Error; err != nil {
		t.Fatal(err)
	}
	if len(balanceSnapshots) != 2 || len(costSnapshots) != 2 {
		t.Fatalf("snapshot counts after rebase = balances %d, costs %d", len(balanceSnapshots), len(costSnapshots))
	}
	if balanceSnapshots[1].CostBasisVersion != expectedVersion+1 || balanceSnapshots[1].FundingFactor == nil {
		t.Fatalf("post-rebase balance snapshot = %+v", balanceSnapshots[1])
	}
	if costSnapshots[1].CostBasisVersion != expectedVersion+1 || costSnapshots[1].FundingFactor == nil {
		t.Fatalf("post-rebase cost snapshot = %+v", costSnapshots[1])
	}
	assertCostUnitFloat(t, "post-rebase balance snapshot factor", *balanceSnapshots[1].FundingFactor, newFactor)
	assertCostUnitFloat(t, "post-rebase cost snapshot factor", *costSnapshots[1].FundingFactor, newFactor)

	// Only observations after the new anchors may add cost at the new factor.
	const (
		nextTodayDelta = 0.25
		nextTotalDelta = 0.4
	)
	next, candidate, err := NewService(storage.NewCosting(db)).RecordObservation(ObservationInput{
		Balance: BalanceInput{
			ChannelID: channel.ID, ExpectedVersion: expectedVersion + 1,
			RawBalance: newRawBalance - nextTotalDelta, ObservedFactor: 1,
			Source: "advertised", SampledAt: afterBalanceAt.Add(time.Minute),
		},
		Costs: CostInput{
			ChannelID: channel.ID, ExpectedVersion: expectedVersion + 1,
			RawTodayCost: newRawToday + nextTodayDelta, RawTotalCost: newRawTotal + nextTotalDelta,
			SampledAt: afterCostAt.Add(time.Minute),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if candidate != nil {
		t.Fatalf("post-rebase debit created funding candidate: %+v", candidate)
	}
	assertCostUnitFloat(t, "post-rebase actual today delta", next.ActualTodayCost, wantActualToday+nextTodayDelta*newFactor)
	assertCostUnitFloat(t, "post-rebase actual total delta", next.ActualTotalCost, wantActualTotal+nextTotalDelta*newFactor)
	if next.Version != expectedVersion+2 || next.FundingFactor != newFactor {
		t.Fatalf("post-rebase observation state = %+v", next)
	}
}

func TestCostUnitRebaseRejectsAmbiguousOrMismatchedEvidenceWithoutWrites(t *testing.T) {
	tests := []struct {
		name              string
		oldObservedFactor float64
		observedFactor    float64
		observedToday     float64
		observedTotal     float64
		wantErr           error
	}{
		{name: "ambiguous prior raw anchor", oldObservedFactor: 0.1, observedFactor: 1, observedToday: 5, observedTotal: 500, wantErr: ErrCostUnitRebaseUnexplainedRegression},
		{name: "mismatched current factor evidence", oldObservedFactor: 0.1, observedFactor: 0.5, observedToday: 21, observedTotal: 1001, wantErr: ErrCostUnitRebaseEvidenceMismatch},
		{name: "missing persisted previous factor", observedFactor: 1, observedToday: 19, observedTotal: 999, wantErr: ErrCostUnitRebaseEvidenceMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := testdb.OpenWithError(t)
			if err != nil {
				t.Fatal(err)
			}
			channel := createCostUnitTestChannel(t, db, "rebase-rejected")
			beforeAt := time.Date(2026, 7, 26, 2, 0, 0, 0, time.UTC)
			seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
				Version: 7, RawBalance: 100, ActualInventory: 10, FundingFactor: 0.1,
				RawTodayCost: 20, ActualTodayCost: 2,
				RawTotalCost: 1000, ActualTotalCost: 100,
				BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
				ObservedProviderFactor: tc.oldObservedFactor, ObservedProviderFactorSource: "advertised",
				Source: "confirmed_redeem_code",
			})
			repo := storage.NewCosting(db)
			beforeState, err := repo.FindState(channel.ID)
			if err != nil {
				t.Fatal(err)
			}
			var beforeChannel storage.Channel
			if err := db.First(&beforeChannel, channel.ID).Error; err != nil {
				t.Fatal(err)
			}

			_, err = NewService(repo).RebaseCostUnit(CostUnitRebaseCommand{
				ExpectedVersion: 7, PreviousMultiplier: 10, CurrentMultiplier: 1,
				IdempotencyKey: "rejected-unit-change", Actor: "administrator",
			}, withCostUnitIdentityFence(t, db, CostUnitRebaseObservation{
				ChannelID: channel.ID, MultiplierMode: connector.RechargeMultiplierModeDivide,
				ObservedFactor: tc.observedFactor, FactorSource: "advertised", RawBalance: 9,
				RawTodayCost: tc.observedToday, RawTotalCost: tc.observedTotal,
				BalanceSampledAt: beforeAt.Add(time.Minute), CostSampledAt: beforeAt.Add(time.Minute),
				RateEvidence: validCostUnitRateEvidence(beforeAt),
			}))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("rebase error = %v, want %v", err, tc.wantErr)
			}
			assertCostUnitStateAndProjectionUnchanged(t, db, beforeState, &beforeChannel)
			assertNoCostUnitRebaseWrites(t, db, channel.ID)
		})
	}
}

func TestCostUnitRebaseRecognizesUnchangedAndScaledCounterEpochs(t *testing.T) {
	tests := []struct {
		name        string
		newRawToday float64
		newRawTotal float64
		wantToday   float64
		wantTotal   float64
	}{
		{
			name: "unchanged prior anchor without cumulative regression",
			// 1001 - 21 == 1000 - 20, so this is a valid epoch even
			// though the cumulative counter did not move backwards.
			newRawToday: 21, newRawTotal: 1001, wantToday: 21, wantTotal: 119,
		},
		{
			name: "scaled prior anchor",
			// (101 - 3) == (1000 - 20) * 0.1.
			newRawToday: 3, newRawTotal: 101, wantToday: 3, wantTotal: 101,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, err := testdb.OpenWithError(t)
			if err != nil {
				t.Fatal(err)
			}
			channel := createCostUnitTestChannel(t, db, "recognized-counter-epoch")
			beforeAt := time.Date(2026, 7, 26, 2, 15, 0, 0, time.UTC)
			seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
				Version: 7, RawBalance: 100, ActualInventory: 10, FundingFactor: 0.1,
				RawTodayCost: 20, ActualTodayCost: 2, RawTotalCost: 1000, ActualTotalCost: 100,
				BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
				ObservedProviderFactor: 0.1, ObservedProviderFactorSource: "advertised",
				Source: "confirmed_redeem_code",
			})
			result, err := NewService(storage.NewCosting(db)).RebaseCostUnit(
				costUnitCommand(7, "recognized-"+strings.ReplaceAll(tc.name, " ", "-")),
				withCostUnitIdentityFence(t, db, CostUnitRebaseObservation{
					ChannelID: channel.ID, MultiplierMode: connector.RechargeMultiplierModeDivide,
					ObservedFactor: 1, FactorSource: "advertised", RawBalance: 9,
					RawTodayCost: tc.newRawToday, RawTotalCost: tc.newRawTotal,
					BalanceSampledAt: beforeAt.Add(time.Minute), CostSampledAt: beforeAt.Add(time.Minute),
					RateEvidence: validCostUnitRateEvidence(beforeAt),
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			if result == nil || result.State == nil || result.Audit == nil {
				t.Fatalf("recognized counter epoch result = %+v", result)
			}
			assertCostUnitFloat(t, "recognized epoch factor", result.State.FundingFactor, 1)
			assertCostUnitFloat(t, "recognized epoch actual today", result.State.ActualTodayCost, tc.wantToday)
			assertCostUnitFloat(t, "recognized epoch actual total", result.State.ActualTotalCost, tc.wantTotal)
		})
	}
}

func TestCostUnitRebaseRejectsRepeatedManualDenominationReset(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := createCostUnitTestChannel(t, db, "manual-reset-already-applied")
	beforeAt := time.Date(2026, 7, 26, 2, 25, 0, 0, time.UTC)
	seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
		Version: 9, RawBalance: 67, ActualInventory: 67.337, FundingFactor: 1.0050346486909446,
		RawTodayCost: 3, ActualTodayCost: 3.015103946072834,
		RawTotalCost: 802, ActualTotalCost: 85,
		BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
		ObservedProviderFactor: 1, ObservedProviderFactorSource: "advertised",
		Source: "manual_denomination_reset",
	})
	repo := storage.NewCosting(db)
	beforeState, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	var beforeChannel storage.Channel
	if err := db.First(&beforeChannel, channel.ID).Error; err != nil {
		t.Fatal(err)
	}

	_, err = NewService(repo).RebaseCostUnit(costUnitCommand(9, "second-manual-reset"), withCostUnitIdentityFence(t, db, CostUnitRebaseObservation{
		ChannelID: channel.ID, MultiplierMode: connector.RechargeMultiplierModeDivide,
		ObservedFactor: 1, FactorSource: "advertised", RawBalance: 66.9,
		// The prior raw anchor is unchanged, so only the already-applied
		// provider-factor transition can reject this second command.
		RawTodayCost: 2.9, RawTotalCost: 801.9,
		BalanceSampledAt: beforeAt.Add(time.Minute), CostSampledAt: beforeAt.Add(time.Minute),
		RateEvidence: validCostUnitRateEvidence(beforeAt),
	}))
	if err == nil {
		t.Fatal("repeated manual denomination reset unexpectedly succeeded")
	}
	assertCostUnitStateAndProjectionUnchanged(t, db, beforeState, &beforeChannel)
	assertNoCostUnitRebaseWrites(t, db, channel.ID)
}

func TestCostUnitRebasePendingCandidateCASAndIdempotency(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	repo := storage.NewCosting(db)
	svc := NewService(repo)
	beforeAt := time.Date(2026, 7, 26, 2, 30, 0, 0, time.UTC)

	t.Run("stale CAS", func(t *testing.T) {
		channel := createCostUnitTestChannel(t, db, "rebase-stale-cas")
		seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
			Version: 7, RawBalance: 100, ActualInventory: 10, FundingFactor: 0.1,
			RawTodayCost: 20, ActualTodayCost: 2, RawTotalCost: 1000, ActualTotalCost: 100,
			BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
			ObservedProviderFactor: 0.1, ObservedProviderFactorSource: "advertised",
			Source: "confirmed_redeem_code",
		})
		beforeState, err := repo.FindState(channel.ID)
		if err != nil {
			t.Fatal(err)
		}
		var beforeChannel storage.Channel
		if err := db.First(&beforeChannel, channel.ID).Error; err != nil {
			t.Fatal(err)
		}
		_, err = svc.RebaseCostUnit(costUnitCommand(8, "stale-cas"), validCostUnitObservation(t, db, channel.ID, beforeAt))
		if !errors.Is(err, storage.ErrCostStateConflict) {
			t.Fatalf("stale rebase error = %v", err)
		}
		assertCostUnitStateAndProjectionUnchanged(t, db, beforeState, &beforeChannel)
		assertNoCostUnitRebaseWrites(t, db, channel.ID)
	})

	t.Run("pending funding candidate", func(t *testing.T) {
		channel := createCostUnitTestChannel(t, db, "rebase-pending-candidate")
		seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
			Version: 7, RawBalance: 100, ActualInventory: 10, FundingFactor: 0.1,
			RawTodayCost: 20, ActualTodayCost: 2, RawTotalCost: 1000, ActualTotalCost: 100,
			BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
			ObservedProviderFactor: 0.1, ObservedProviderFactorSource: "advertised",
			Source: "confirmed_redeem_code",
		})
		if _, err := repo.UpsertFundingCandidate(&storage.ChannelFundingCandidate{
			ChannelID: channel.ID, Source: "balance_increase", ExternalID: "pending-before-rebase",
			Status: storage.FundingCandidatePending, CreditedAmount: 10,
			RawBalanceBefore: 100, RawBalanceDetected: 110, FundingFactorBefore: 0.1,
			DetectedAt: beforeAt.Add(time.Minute),
		}); err != nil {
			t.Fatal(err)
		}
		beforeState, err := repo.FindState(channel.ID)
		if err != nil {
			t.Fatal(err)
		}
		var beforeChannel storage.Channel
		if err := db.First(&beforeChannel, channel.ID).Error; err != nil {
			t.Fatal(err)
		}
		_, err = svc.RebaseCostUnit(costUnitCommand(7, "pending-candidate"), validCostUnitObservation(t, db, channel.ID, beforeAt))
		if !errors.Is(err, storage.ErrPendingFundingCandidate) {
			t.Fatalf("pending-candidate rebase error = %v", err)
		}
		assertCostUnitStateAndProjectionUnchanged(t, db, beforeState, &beforeChannel)
		assertNoCostUnitRebaseWrites(t, db, channel.ID)
	})

	t.Run("idempotent replay and payload conflict", func(t *testing.T) {
		channel := createCostUnitTestChannel(t, db, "rebase-idempotent")
		seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
			Version: 7, RawBalance: 100, ActualInventory: 10, FundingFactor: 0.1,
			RawTodayCost: 20, ActualTodayCost: 2, RawTotalCost: 1000, ActualTotalCost: 100,
			BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
			ObservedProviderFactor: 0.1, ObservedProviderFactorSource: "advertised",
			Source: "confirmed_redeem_code",
		})
		command := costUnitCommand(7, "idempotent-rebase")
		applied, err := svc.RebaseCostUnit(command, validCostUnitObservation(t, db, channel.ID, beforeAt))
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := svc.ReplayCostUnitRebase(channel.ID, command)
		if err != nil {
			t.Fatal(err)
		}
		if replayed == nil || replayed.Audit == nil || !replayed.IdempotentReplay || replayed.State != nil ||
			replayed.Audit.ID != applied.Audit.ID {
			t.Fatalf("idempotent replay = %+v, applied = %+v", replayed, applied)
		}
		var auditCount, balanceCount, costCount int64
		if err := db.Model(&storage.ChannelCostUnitRebase{}).Where("channel_id = ?", channel.ID).Count(&auditCount).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Model(&storage.BalanceSnapshot{}).Where("channel_id = ?", channel.ID).Count(&balanceCount).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Model(&storage.CostSnapshot{}).Where("channel_id = ?", channel.ID).Count(&costCount).Error; err != nil {
			t.Fatal(err)
		}
		if auditCount != 1 || balanceCount != 1 || costCount != 1 {
			t.Fatalf("idempotent replay duplicated rows: audit=%d balance=%d cost=%d", auditCount, balanceCount, costCount)
		}

		conflicting := command
		conflicting.Note = "same key with different intent"
		if _, err := svc.ReplayCostUnitRebase(channel.ID, conflicting); !errors.Is(err, storage.ErrCostUnitRebaseIdempotencyConflict) {
			t.Fatalf("idempotency payload conflict error = %v", err)
		}
	})
}

func TestRecordObservationRebasesProviderCounterWithoutHalfCommit(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := createCostUnitTestChannel(t, db, "observation-regression")
	beforeAt := time.Date(2026, 7, 26, 3, 0, 0, 0, time.UTC)
	seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
		Version: 5, RawBalance: 100, ActualInventory: 100, FundingFactor: 1,
		RawTodayCost: 20, ActualTodayCost: 20, RawTotalCost: 1000, ActualTotalCost: 1000,
		BalanceSampledAt: beforeAt, CostSampledAt: beforeAt, Source: "default",
	})
	repo := storage.NewCosting(db)
	svc := NewService(repo)
	beforeState, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := svc.RecordCosts(CostInput{
		ChannelID: channel.ID, ExpectedVersion: beforeState.Version,
		RawTodayCost: 2, RawTotalCost: 100, SampledAt: beforeAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("default factor=1 counter reset: %v", err)
	}
	if first.ActualTotalCost != beforeState.ActualTotalCost || first.RawTotalCost != 100 || first.ActualTodayCost != 2 ||
		!IsUpstreamCostCounterResetWarning(first.LastError) {
		t.Fatalf("default counter reset state = %+v", first)
	}

	_, candidate, err := svc.RecordObservation(ObservationInput{
		Balance: BalanceInput{
			ChannelID: channel.ID, ExpectedVersion: first.Version,
			RawBalance: 9, ObservedFactor: 1, Source: "advertised", SampledAt: beforeAt.Add(2 * time.Minute),
		},
		Costs: CostInput{
			ChannelID: channel.ID, ExpectedVersion: first.Version,
			RawTodayCost: 3, RawTotalCost: 102, SampledAt: beforeAt.Add(2 * time.Minute),
		},
	})
	if err != nil {
		t.Fatalf("atomic observation after counter reset: %v", err)
	}
	if candidate != nil {
		t.Fatalf("counter reset observation created candidate: %+v", candidate)
	}
	after, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.ActualTotalCost != beforeState.ActualTotalCost+2 || after.RawTotalCost != 102 || after.LastError != "" {
		t.Fatalf("post-reset counter state = %+v", after)
	}
	var afterChannel storage.Channel
	if err := db.First(&afterChannel, channel.ID).Error; err != nil {
		t.Fatal(err)
	}
	if afterChannel.TotalCost == nil || *afterChannel.TotalCost != after.ActualTotalCost {
		t.Fatalf("post-reset channel projection = %+v", afterChannel)
	}
	assertNoCostUnitRebaseArtifacts(t, db, channel.ID)
}

func TestRecordObservationMarksAdvertisedFactorChangeForReviewWithoutRewritingAmounts(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := createCostUnitTestChannel(t, db, "observation-factor-change")
	beforeAt := time.Date(2026, 7, 26, 1, 46, 23, 337852000, time.UTC)
	seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
		Version: 17, RawBalance: 594.14400491, ActualInventory: 59.713536360606,
		FundingFactor: 0.10050346486909446,
		RawTodayCost:  7.054747725, ActualTodayCost: 0.709998186961003,
		RawTotalCost: 805.956025725, ActualTotalCost: 82.2981229088619,
		BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
		ObservedProviderFactor: 0.1, ObservedProviderFactorSource: "advertised",
		Source: "confirmed_redeem_code",
	})
	repo := storage.NewCosting(db)
	svc := NewService(repo)
	beforeState, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	var beforeChannel storage.Channel
	if err := db.First(&beforeChannel, channel.ID).Error; err != nil {
		t.Fatal(err)
	}

	observation := ObservationInput{
		Balance: BalanceInput{
			ChannelID: channel.ID, ExpectedVersion: beforeState.Version,
			RawBalance: 67.31368018, ObservedFactor: 1,
			Source: "advertised", SampledAt: beforeAt.Add(time.Minute),
		},
		Costs: CostInput{
			ChannelID: channel.ID, ExpectedVersion: beforeState.Version,
			RawTodayCost: 3.4234572092, RawTotalCost: 802.3247352092,
			SampledAt: beforeAt.Add(time.Minute),
		},
	}
	if _, _, err := svc.RecordObservation(observation); !errors.Is(err, ErrCostUnitChangeRequiresRebase) {
		t.Fatalf("factor change error = %v", err)
	}
	afterFirst, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterFirst.Status != StateStatusNeedsReview || afterFirst.Version != beforeState.Version+1 ||
		!strings.Contains(afterFirst.LastError, ErrCostUnitChangeRequiresRebase.Error()) {
		t.Fatalf("review state = %+v", afterFirst)
	}
	assertCostUnitFinancialAnchorsEqual(t, beforeState, afterFirst)
	var afterChannel storage.Channel
	if err := db.First(&afterChannel, channel.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&beforeChannel, &afterChannel) {
		t.Fatalf("factor review changed channel projection: before=%+v after=%+v", beforeChannel, afterChannel)
	}

	if err := svc.FlagCostUnitReview(channel.ID, afterFirst.Version, 1); !errors.Is(err, ErrCostUnitChangeRequiresRebase) {
		t.Fatalf("rate-path repeated factor change error = %v", err)
	}
	afterRatePath, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterFirst, afterRatePath) {
		t.Fatalf("rate-path repeated review changed state: before=%+v after=%+v", afterFirst, afterRatePath)
	}

	observation.Balance.ExpectedVersion = afterFirst.Version
	observation.Costs.ExpectedVersion = afterFirst.Version
	if _, _, err := svc.RecordObservation(observation); !errors.Is(err, ErrCostUnitChangeRequiresRebase) {
		t.Fatalf("repeated factor change error = %v", err)
	}
	afterRepeat, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterFirst, afterRepeat) {
		t.Fatalf("repeated review changed state: before=%+v after=%+v", afterFirst, afterRepeat)
	}
}

func TestRecordObservationRejectsStaleFactorChangeBeforeMarkingReview(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := createCostUnitTestChannel(t, db, "stale-observation-factor-change")
	beforeAt := time.Date(2026, 7, 26, 2, 33, 1, 301320000, time.UTC)
	seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
		Version: 18, RawBalance: 67.31368018, ActualInventory: 67.652601327,
		FundingFactor: 1.00503464869094,
		RawTodayCost:  3.4234572092, ActualTodayCost: 3.440693,
		RawTotalCost: 802.3247352092, ActualTotalCost: 83.287456,
		BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
		ObservedProviderFactor: 1, ObservedProviderFactorSource: "advertised",
		Source: "cost_unit_rebase",
	})
	repo := storage.NewCosting(db)
	beforeState, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = NewService(repo).RecordObservation(ObservationInput{
		Balance: BalanceInput{
			ChannelID: channel.ID, ExpectedVersion: beforeState.Version - 1,
			RawBalance: 594.14400491, ObservedFactor: 0.1,
			Source: "advertised", SampledAt: beforeAt.Add(-time.Minute),
		},
		Costs: CostInput{
			ChannelID: channel.ID, ExpectedVersion: beforeState.Version - 1,
			RawTodayCost: 7.054747725, RawTotalCost: 805.956025725,
			SampledAt: beforeAt.Add(-time.Minute),
		},
	})
	if !errors.Is(err, storage.ErrCostStateConflict) {
		t.Fatalf("stale factor observation error = %v", err)
	}
	afterState, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeState, afterState) {
		t.Fatalf("stale factor observation changed state: before=%+v after=%+v", beforeState, afterState)
	}
	if err := NewService(repo).FlagCostUnitReview(channel.ID, beforeState.Version-1, 0.1); !errors.Is(err, storage.ErrCostStateConflict) {
		t.Fatalf("stale rate-path factor change error = %v", err)
	}
}

func TestRecordObservationClearsReviewWhenProviderRevertsWithoutChangingFundingBasis(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := createCostUnitTestChannel(t, db, "provider-factor-reverted")
	beforeAt := time.Date(2026, 7, 26, 4, 0, 0, 0, time.UTC)
	const fundingFactor = 0.10050346486909446
	seedCostUnitState(t, db, channel.ID, costUnitStateSeed{
		Version: 21, RawBalance: 100, ActualInventory: 100 * fundingFactor,
		FundingFactor: fundingFactor,
		RawTodayCost:  7, ActualTodayCost: 7 * fundingFactor,
		RawTotalCost: 805, ActualTotalCost: 80.5,
		BalanceSampledAt: beforeAt, CostSampledAt: beforeAt,
		ObservedProviderFactor: 0.1, ObservedProviderFactorSource: "advertised",
		Source: "confirmed_redeem_code",
	})
	repo := storage.NewCosting(db)
	svc := NewService(repo)
	if err := svc.FlagCostUnitReview(channel.ID, 21, 1); !errors.Is(err, ErrCostUnitChangeRequiresRebase) {
		t.Fatalf("mark review: %v", err)
	}
	reviewed, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}

	next, candidate, err := svc.RecordObservation(ObservationInput{
		Balance: BalanceInput{
			ChannelID: channel.ID, ExpectedVersion: reviewed.Version,
			RawBalance: 99, ObservedFactor: 0.1,
			Source: "advertised", SampledAt: beforeAt.Add(time.Minute),
		},
		Costs: CostInput{
			ChannelID: channel.ID, ExpectedVersion: reviewed.Version,
			RawTodayCost: 7.25, RawTotalCost: 805.25,
			SampledAt: beforeAt.Add(time.Minute),
		},
	})
	if err != nil || candidate != nil {
		t.Fatalf("provider reversion observation = state=%+v candidate=%+v err=%v", next, candidate, err)
	}
	if next.Status != StateStatusReady || next.LastError != "" || next.FundingFactor != fundingFactor {
		t.Fatalf("provider reversion changed funding basis: %+v", next)
	}
	if next.ObservedProviderFactor == nil || *next.ObservedProviderFactor != 0.1 ||
		next.ObservedProviderFactorSource != "advertised" {
		t.Fatalf("provider reversion observation evidence = %+v", next)
	}
	assertCostUnitFloat(t, "provider reversion inventory", next.ActualInventory, 99*fundingFactor)
	assertCostUnitFloat(t, "provider reversion today cost", next.ActualTodayCost, 7.25*fundingFactor)
	assertCostUnitFloat(t, "provider reversion total cost", next.ActualTotalCost, 80.5+0.25*fundingFactor)
}

func assertCostUnitFinancialAnchorsEqual(t *testing.T, before, after *storage.ChannelCostState) {
	t.Helper()
	beforeCopy := *before
	afterCopy := *after
	beforeCopy.Version, afterCopy.Version = 0, 0
	beforeCopy.Status, afterCopy.Status = "", ""
	beforeCopy.LastError, afterCopy.LastError = "", ""
	beforeCopy.UpdatedAt, afterCopy.UpdatedAt = time.Time{}, time.Time{}
	if !reflect.DeepEqual(&beforeCopy, &afterCopy) {
		t.Fatalf("review rewrote financial anchors: before=%+v after=%+v", before, after)
	}
}

type costUnitStateSeed struct {
	Version                         uint64
	RawBalance, ActualInventory     float64
	FundingFactor                   float64
	RawTodayCost, ActualTodayCost   float64
	RawTotalCost, ActualTotalCost   float64
	BalanceSampledAt, CostSampledAt time.Time
	ObservedProviderFactor          float64
	ObservedProviderFactorSource    string
	Source                          string
}

func createCostUnitTestChannel(t *testing.T, db *gorm.DB, name string) *storage.Channel {
	t.Helper()
	channel := &storage.Channel{
		Name: name, Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com",
		Username: "user", PasswordCipher: "cipher",
	}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	bindCostUnitAccountIdentity(t, db, channel)
	return channel
}

// bindCostUnitAccountIdentity establishes the provider-authenticated account
// binding a rebase or review write is fenced against. Every cost-unit write
// refuses to touch money without it, so the fixture must observe it exactly as
// the monitor does.
func bindCostUnitAccountIdentity(t *testing.T, db *gorm.DB, channel *storage.Channel) *storage.UpstreamAccountIdentitySnapshot {
	t.Helper()
	snapshot, err := storage.NewChannels(db).ObserveUpstreamAccountIdentity(storage.UpstreamAccountIdentityObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channel.AdminRevision,
		Provider:              storage.ChannelTypeSub2API, APIOrigin: channel.SiteURL,
		ExternalAccountID: "account-" + channel.Name, Source: "auth_me",
		Scope:      storage.UpstreamConcurrencyScopeAccount,
		ObservedAt: time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("observe upstream account identity: %v", err)
	}
	return snapshot
}

// costUnitIdentityFence returns the locked-identity expectations a rebase
// observation must carry.
func costUnitIdentityFence(t *testing.T, db *gorm.DB, channelID uint) (uint64, uint64, string) {
	t.Helper()
	identity, err := storage.NewChannels(db).CurrentUpstreamAccountIdentity(channelID)
	if err != nil {
		t.Fatalf("load current upstream account identity: %v", err)
	}
	return identity.ChannelIdentityRevision, identity.AccountRecordID, identity.Fingerprint
}

// withCostUnitIdentityFence fills the identity expectations on an otherwise
// complete observation.
func withCostUnitIdentityFence(t *testing.T, db *gorm.DB, observation CostUnitRebaseObservation) CostUnitRebaseObservation {
	t.Helper()
	revision, recordID, fingerprint := costUnitIdentityFence(t, db, observation.ChannelID)
	observation.ExpectedIdentityRevision = revision
	observation.ExpectedAccountRecordID = recordID
	observation.ExpectedAccountFingerprint = fingerprint
	return observation
}

func seedCostUnitState(t *testing.T, db *gorm.DB, channelID uint, seed costUnitStateSeed) {
	t.Helper()
	updates := map[string]any{
		"raw_balance": seed.RawBalance, "actual_inventory": seed.ActualInventory,
		"funding_factor": seed.FundingFactor,
		"raw_today_cost": seed.RawTodayCost, "actual_today_cost": seed.ActualTodayCost,
		"raw_total_cost": seed.RawTotalCost, "actual_total_cost": seed.ActualTotalCost,
		"balance_sampled_at": seed.BalanceSampledAt, "cost_sampled_at": seed.CostSampledAt,
		"version": seed.Version, "source": seed.Source, "status": StateStatusReady, "last_error": "",
	}
	if seed.ObservedProviderFactor > 0 {
		updates["observed_provider_factor"] = seed.ObservedProviderFactor
		updates["observed_provider_factor_source"] = seed.ObservedProviderFactorSource
		updates["observed_provider_factor_sampled_at"] = seed.CostSampledAt
	}
	if err := db.Model(&storage.ChannelCostState{}).Where("channel_id = ?", channelID).Updates(updates).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&storage.Channel{}).Where("id = ?", channelID).Updates(map[string]any{
		"raw_last_balance": seed.RawBalance, "last_balance": seed.ActualInventory,
		"last_balance_at": seed.BalanceSampledAt, "funding_factor": seed.FundingFactor,
		"funding_factor_source": seed.Source,
		"raw_today_cost":        seed.RawTodayCost, "today_cost": seed.ActualTodayCost,
		"raw_total_cost": seed.RawTotalCost, "total_cost": seed.ActualTotalCost,
		"today_cost_at": seed.CostSampledAt,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: channelID, ModelName: "PLUS", Description: "plus", Ratio: 0.6, LastSeenAt: seed.CostSampledAt},
		{ChannelID: channelID, ModelName: "promo", Description: "promo", Ratio: 0.15, LastSeenAt: seed.CostSampledAt},
	} {
		if err := db.Create(rate).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func costUnitCommand(expectedVersion uint64, idempotencyKey string) CostUnitRebaseCommand {
	return CostUnitRebaseCommand{
		ExpectedVersion: expectedVersion, PreviousMultiplier: 10, CurrentMultiplier: 1,
		IdempotencyKey: idempotencyKey, Actor: "administrator", Note: "approved provider unit change",
	}
}

func validCostUnitObservation(t *testing.T, db *gorm.DB, channelID uint, beforeAt time.Time) CostUnitRebaseObservation {
	t.Helper()
	return withCostUnitIdentityFence(t, db, CostUnitRebaseObservation{
		ChannelID: channelID, MultiplierMode: connector.RechargeMultiplierModeDivide,
		ObservedFactor: 1, FactorSource: "advertised", RawBalance: 9,
		// 999 - 19 == 1000 - 20: valid unchanged prior anchor.
		RawTodayCost: 19, RawTotalCost: 999,
		BalanceSampledAt: beforeAt.Add(time.Minute), CostSampledAt: beforeAt.Add(time.Minute + time.Second),
		RateEvidence: validCostUnitRateEvidence(beforeAt),
	})
}

func validCostUnitRateEvidence(previousCostAt time.Time) CostUnitRateEvidence {
	items := []storage.CostUnitRateTransitionItem{
		{ModelName: "PLUS", DescriptionBefore: "plus", DescriptionAfter: "plus", RatioBefore: 0.6, RatioAfter: 0.06},
		{ModelName: "promo", DescriptionBefore: "promo", DescriptionAfter: "promo", RatioBefore: 0.15, RatioAfter: 0.015},
	}
	digest, err := storage.CostUnitRateEvidenceDigest(items)
	if err != nil {
		panic(err)
	}
	return CostUnitRateEvidence{
		TransitionAt: previousCostAt.Add(30 * time.Second), RawScale: 0.1,
		GroupCount: len(items), SnapshotDigest: digest, Items: items,
	}
}

func assertCostUnitStateAndProjectionUnchanged(t *testing.T, db *gorm.DB, beforeState *storage.ChannelCostState, beforeChannel *storage.Channel) {
	t.Helper()
	afterState, err := storage.NewCosting(db).FindState(beforeState.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeState, afterState) {
		t.Fatalf("rejected operation changed cost state: before=%+v after=%+v", beforeState, afterState)
	}
	var afterChannel storage.Channel
	if err := db.First(&afterChannel, beforeChannel.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beforeChannel, &afterChannel) {
		t.Fatalf("rejected operation changed channel projection: before=%+v after=%+v", beforeChannel, &afterChannel)
	}
}

func assertNoCostUnitRebaseWrites(t *testing.T, db *gorm.DB, channelID uint) {
	t.Helper()
	for _, model := range []any{
		&storage.ChannelCostUnitRebase{}, &storage.BalanceSnapshot{}, &storage.CostSnapshot{},
	} {
		var count int64
		if err := db.Model(model).Where("channel_id = ?", channelID).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%T rows after rejected operation = %d", model, count)
		}
	}
	var rateChanges int64
	if err := db.Model(&storage.RateChangeLog{}).Where("channel_id = ? AND change_reason = ?", channelID, "cost_unit_rebase").Count(&rateChanges).Error; err != nil {
		t.Fatal(err)
	}
	if rateChanges != 0 {
		t.Fatalf("cost-unit rate changes after rejected operation = %d", rateChanges)
	}
}

func assertNoCostUnitRebaseArtifacts(t *testing.T, db *gorm.DB, channelID uint) {
	t.Helper()
	var rebaseCount int64
	if err := db.Model(&storage.ChannelCostUnitRebase{}).Where("channel_id = ?", channelID).Count(&rebaseCount).Error; err != nil {
		t.Fatal(err)
	}
	if rebaseCount != 0 {
		t.Fatalf("cost-unit rebase rows = %d", rebaseCount)
	}
	var rateChanges int64
	if err := db.Model(&storage.RateChangeLog{}).Where("channel_id = ? AND change_reason = ?", channelID, "cost_unit_rebase").Count(&rateChanges).Error; err != nil {
		t.Fatal(err)
	}
	if rateChanges != 0 {
		t.Fatalf("cost-unit rate changes = %d", rateChanges)
	}
}

func assertCostUnitFloat(t *testing.T, name string, got, want float64) {
	t.Helper()
	tolerance := costUnitFloatTolerance * math.Max(1, math.Max(math.Abs(got), math.Abs(want)))
	if math.Abs(got-want) > tolerance {
		t.Fatalf("%s = %.15g, want %.15g", name, got, want)
	}
}

func floatPointer(value float64) *float64 { return &value }
