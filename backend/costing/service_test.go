package costing

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
)

func TestCanRecoverAdvertisedFactorEvidenceRequiresMatchingFundingBasis(t *testing.T) {
	state := &storage.ChannelCostState{Source: "advertised", FundingFactor: 0.6}
	if !CanRecoverAdvertisedFactorEvidence(state, 0.6, "advertised") {
		t.Fatal("matching advertised factor should recover incomplete evidence")
	}
	if CanRecoverAdvertisedFactorEvidence(state, 0.5, "advertised") {
		t.Fatal("changed advertised factor must remain fenced")
	}
	if CanRecoverAdvertisedFactorEvidence(state, 0.6, "default") {
		t.Fatal("default factor is not provider evidence")
	}
	observed := 0.6
	state.ObservedProviderFactor = &observed
	state.ObservedProviderFactorSource = "advertised"
	sampledAt := time.Now()
	state.ObservedProviderFactorSampledAt = &sampledAt
	if CanRecoverAdvertisedFactorEvidence(state, 0.6, "advertised") {
		t.Fatal("complete evidence does not need recovery")
	}
}

func TestRecordProviderFactorEvidenceDoesNotAdvanceFinancialAnchors(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	channel := &storage.Channel{Name: "factor-only-evidence", Type: storage.ChannelTypeSub2API, SiteURL: "https://factor-only.example", Username: "u", PasswordCipher: "cipher"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	costAt := time.Date(2026, 8, 8, 23, 55, 0, 0, time.UTC)
	seed := db.Model(&storage.ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"raw_balance": 12.5, "actual_inventory": 7.5, "funding_factor": 0.6,
		"raw_today_cost": 3.25, "actual_today_cost": 1.95,
		"raw_total_cost": 18.75, "actual_total_cost": 11.25,
		"cost_sampled_at": costAt, "source": "advertised", "status": StateStatusReady,
		"observed_provider_factor": nil, "observed_provider_factor_source": "", "observed_provider_factor_sampled_at": nil,
	})
	if seed.Error != nil || seed.RowsAffected != 1 {
		t.Fatalf("seed incomplete evidence: err=%v rows=%d", seed.Error, seed.RowsAffected)
	}
	svc := NewService(storage.NewCosting(db))
	before, err := svc.State(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	sampledAt := costAt.Add(24 * time.Hour)
	after, err := svc.RecordProviderFactorEvidence(ProviderFactorEvidenceInput{
		ChannelID: channel.ID, ExpectedVersion: before.Version,
		ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ObservedFactor:           0.6, Source: "advertised", SampledAt: sampledAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if after.Version != before.Version+1 || after.ObservedProviderFactor == nil || *after.ObservedProviderFactor != 0.6 ||
		after.ObservedProviderFactorSource != "advertised" || after.ObservedProviderFactorSampledAt == nil || !after.ObservedProviderFactorSampledAt.Equal(sampledAt) {
		t.Fatalf("provider factor evidence = %+v", after)
	}
	if after.RawBalance != before.RawBalance || after.ActualInventory != before.ActualInventory ||
		after.RawTodayCost != before.RawTodayCost || after.ActualTodayCost != before.ActualTodayCost ||
		after.RawTotalCost != before.RawTotalCost || after.ActualTotalCost != before.ActualTotalCost ||
		after.CostSampledAt == nil || !after.CostSampledAt.Equal(*before.CostSampledAt) {
		t.Fatalf("factor-only recovery changed financial anchors: before=%+v after=%+v", before, after)
	}
}

func TestActualAndWeightedFactor(t *testing.T) {
	actual, err := Actual(200, 0.25)
	if err != nil || actual != 50 {
		t.Fatalf("Actual(200, .25) = %v, %v", actual, err)
	}
	factor, err := WeightedFactor(100, 0.5, 20, 100)
	if err != nil {
		t.Fatalf("WeightedFactor: %v", err)
	}
	if math.Abs(factor-0.35) > 1e-12 {
		t.Fatalf("factor = %v, want .35", factor)
	}
	if zero, err := Actual(1, 0); err != nil || zero != 0 {
		t.Fatalf("zero-cost Actual = %v, %v", zero, err)
	}
	if freeFactor, err := WeightedFactor(1, 1, 0, 1); err != nil || freeFactor != 0.5 {
		t.Fatalf("free-credit factor = %v, %v", freeFactor, err)
	}
	for name, fn := range map[string]func() error{
		"negative raw":  func() error { _, err := Actual(-1, 1); return err },
		"nan factor":    func() error { _, err := Actual(1, math.NaN()); return err },
		"zero credited": func() error { _, err := WeightedFactor(1, 1, 1, 0); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := fn(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestActualBalanceAcceptsSignedBalancesWithoutRelaxingCosts(t *testing.T) {
	actual, err := ActualBalance(-20, 0.25)
	if err != nil || actual != -5 {
		t.Fatalf("ActualBalance(-20, .25) = %v, %v", actual, err)
	}
	if _, err := Actual(-20, 0.25); err == nil {
		t.Fatal("negative cost counter must remain invalid")
	}
	for name, fn := range map[string]func() error{
		"nan balance":      func() error { _, err := ActualBalance(math.NaN(), 1); return err },
		"infinite balance": func() error { _, err := ActualBalance(math.Inf(-1), 1); return err },
		"negative factor":  func() error { _, err := ActualBalance(-1, -1); return err },
		"overflow":         func() error { _, err := ActualBalance(-math.MaxFloat64, 2); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := fn(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestIsAccountingGateIncludesCostStateConflicts(t *testing.T) {
	for name, err := range map[string]error{
		"direct":  storage.ErrCostStateConflict,
		"wrapped": fmt.Errorf("stale observation: %w", storage.ErrCostStateConflict),
	} {
		t.Run(name, func(t *testing.T) {
			if !IsAccountingGate(err) {
				t.Fatalf("IsAccountingGate(%v) = false, want true", err)
			}
		})
	}
	if IsAccountingGate(errors.New("upstream unavailable")) {
		t.Fatal("connectivity failure must not be classified as an accounting gate")
	}
}

func TestFundingCandidateAmountUsesConfiguredHardFloor(t *testing.T) {
	svc := NewService(nil)
	for _, amount := range []float64{0, 0.98} {
		if svc.IsFundingCandidateAmount(amount) {
			t.Fatalf("amount %v unexpectedly became a funding candidate", amount)
		}
	}
	if !svc.IsFundingCandidateAmount(0.99) {
		t.Fatal("0.99 must become a funding candidate")
	}

	svc.SetFundingDetectionThreshold(2)
	if svc.IsFundingCandidateAmount(1.99) {
		t.Fatal("amount below user threshold unexpectedly became a funding candidate")
	}
	if !svc.IsFundingCandidateAmount(2) {
		t.Fatal("amount at user threshold must become a funding candidate")
	}

	svc.SetFundingDetectionThreshold(0.1)
	if got := svc.FundingDetectionThreshold(); got != 0.99 {
		t.Fatalf("lowered threshold = %v, want 0.99", got)
	}

	if _, err := svc.buildFundingCandidate(FundingCandidateInput{
		ChannelID: 1, Source: "balance_increase", ExternalID: "small-observed-credit",
		CreditedAmount: 0.98, RawBalanceBefore: 1, RawBalanceDetected: 1.98,
		FundingFactorBefore: 1, DetectedAt: time.Now(),
	}); !errors.Is(err, ErrFundingCandidateBelowThreshold) {
		t.Fatalf("small observed funding error = %v", err)
	}
	if _, err := svc.buildFundingCandidate(FundingCandidateInput{
		ChannelID: 1, Source: "manual_adjustment", ExternalID: "small-manual-credit",
		CreditedAmount: 0.5, RawBalanceBefore: 1, RawBalanceDetected: 1.5,
		FundingFactorBefore: 1, DetectedAt: time.Now(),
	}); err != nil {
		t.Fatalf("manual funding must not be limited by observed-credit threshold: %v", err)
	}
}

func TestServiceRecordsWeightedFundingAtomically(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	channel := &storage.Channel{Name: "costed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	sampledAt := time.Date(2026, 7, 15, 1, 2, 3, 0, time.UTC)
	state, err := svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: 1, RawBalance: 100, SampledAt: sampledAt, Source: "upstream"})
	if err != nil {
		t.Fatal(err)
	}
	if state.ActualInventory != 100 || state.Version != 2 {
		t.Fatalf("balance state = %+v", state)
	}
	state, event, err := svc.RecordFunding(FundingInput{
		ChannelID: channel.ID, ExpectedVersion: 2,
		PaidAmount: 50, CreditedAmount: 100,
		EffectiveAt: sampledAt.Add(time.Hour), Note: "tier recharge",
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.RawBalance != 200 || state.ActualInventory != 150 || state.FundingFactor != 0.75 || state.Version != 3 {
		t.Fatalf("funded state = %+v", state)
	}
	if event.RawBalanceBefore != 100 || event.ActualInventoryBefore != 100 || event.ActualInventoryAfter != 150 || event.FundingFactorAfter != 0.75 {
		t.Fatalf("funding event = %+v", event)
	}
	_, _, err = svc.RecordFunding(FundingInput{ChannelID: channel.ID, ExpectedVersion: 2, PaidAmount: 1, CreditedAmount: 1})
	if !errors.Is(err, storage.ErrCostStateConflict) {
		t.Fatalf("stale funding error = %v", err)
	}
	events, total, err := svc.FundingEvents(channel.ID, 50, 0)
	if err != nil || total != 1 || len(events) != 1 {
		t.Fatalf("events = %d/%d, err %v", len(events), total, err)
	}
}

func TestRecordBalanceDropsLegacyAdvertisedFactor(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	channel := &storage.Channel{Name: "legacy-advertised", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	state, err := svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: 1, RawBalance: 10, ObservedFactor: 1 / 7.3, Source: "advertised", SampledAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordCosts(CostInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawTodayCost: 2, RawTotalCost: 8, SampledAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawBalance: 10, ObservedFactor: 1, Source: "default", SampledAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if state.FundingFactor != 1 || state.Source != "default" || state.ActualInventory != 10 || state.ActualTodayCost != 2 || state.ActualTotalCost != 8 {
		t.Fatalf("migrated state = %+v", state)
	}
}

func TestServiceUsesCurrentRawBalanceForRecharge(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "current", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	current := 20.0
	state, _, err := svc.RecordFunding(FundingInput{ChannelID: channel.ID, ExpectedVersion: 1, CurrentRawBalance: &current, PaidAmount: 40, CreditedAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	// Existing 20 credits cost 20; the new 100 cost 40. The remaining 120
	// credits therefore carry 60 actual currency at an aggregate factor .5.
	if state.RawBalance != 120 || state.ActualInventory != 60 || state.FundingFactor != 0.5 {
		t.Fatalf("state = %+v", state)
	}
}

func TestConfirmFundingCandidateKeepsLatestObservedBalance(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "candidate", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	state, err := svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: 1, RawBalance: 50, SampledAt: time.Now(), Source: "upstream"})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := svc.DetectFunding(FundingCandidateInput{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "balance-1",
		CreditedAmount: 100, RawBalanceBefore: 50, RawBalanceDetected: 150,
		FundingFactorBefore: state.FundingFactor, DetectedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawBalance: 140, SampledAt: time.Now(), Source: "upstream"})
	if err != nil {
		t.Fatal(err)
	}
	state, event, err := svc.ConfirmFundingCandidate(channel.ID, candidate.ID, 50, state.Version, "redeem code")
	if err != nil {
		t.Fatal(err)
	}
	if state.RawBalance != 140 || state.ActualInventory != 140*(100.0/150.0) {
		t.Fatalf("confirmed state rewrote latest balance: %+v", state)
	}
	if event.RawBalanceAfter != 150 || event.ActualInventoryAfter != 100 {
		t.Fatalf("event does not preserve detection-time evidence: %+v", event)
	}
}

func TestRecordCostsAccumulatesDeltasAtTheirFundingFactor(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "deltas", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	state, err := svc.RecordCosts(CostInput{ChannelID: channel.ID, ExpectedVersion: 1, RawTodayCost: 10, RawTotalCost: 100, SampledAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err = svc.RecordFunding(FundingInput{ChannelID: channel.ID, ExpectedVersion: state.Version, PaidAmount: 50, CreditedAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordCosts(CostInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawTodayCost: 14, RawTotalCost: 104, SampledAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if state.FundingFactor != 0.5 || state.ActualTodayCost != 12 || state.ActualTotalCost != 102 {
		t.Fatalf("delta-accounted costs = %+v", state)
	}
}

func TestRecordCostsRebasesProviderCounterAtCurrentFundingFactor(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "provider-counter-reset", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	state, _, err := svc.RecordFunding(FundingInput{ChannelID: channel.ID, ExpectedVersion: 1, PaidAmount: 50, CreditedAmount: 100})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordCosts(CostInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawTodayCost: 10, RawTotalCost: 100, SampledAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordCosts(CostInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawTodayCost: 1, RawTotalCost: 5, SampledAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if state.RawTotalCost != 5 || state.ActualTotalCost != 50 || state.ActualTodayCost != 0.5 ||
		!IsUpstreamCostCounterResetWarning(state.LastError) {
		t.Fatalf("rebased provider counter state = %+v", state)
	}
	state, err = svc.RecordCosts(CostInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawTodayCost: 3, RawTotalCost: 9, SampledAt: time.Now().Add(2 * time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if state.ActualTotalCost != 52 || state.ActualTodayCost != 1.5 || state.LastError != "" {
		t.Fatalf("post-reset provider counter state = %+v", state)
	}
}

func TestRawTotalCostObservationIgnoresOneSampleRegression(t *testing.T) {
	actual, warning, err := applyRawTotalCostObservation(100, 100, 10, 1, true, "")
	if err != nil || actual != 100 || !IsUpstreamCostCounterResetWarning(warning) {
		t.Fatalf("pending counter reset = actual %.2f warning %q err=%v", actual, warning, err)
	}
	actual, warning, err = applyRawTotalCostObservation(10, actual, 101, 1, true, warning)
	if err != nil || actual != 101 || warning != "" {
		t.Fatalf("recovered counter regression = actual %.2f warning %q err=%v", actual, warning, err)
	}
}

func TestRawTotalCostObservationKeepsCumulativeLedgerAfterConfirmedReset(t *testing.T) {
	actual, warning, err := applyRawTotalCostObservation(1000, 1000, 100, 1, true, "")
	if err != nil || actual != 1000 || !IsUpstreamCostCounterResetWarning(warning) {
		t.Fatalf("pending counter reset = actual %.2f warning %q err=%v", actual, warning, err)
	}
	actual, warning, err = applyRawTotalCostObservation(100, actual, 102, 1, true, warning)
	if err != nil || actual != 1002 || warning != "" {
		t.Fatalf("confirmed counter reset = actual %.2f warning %q err=%v", actual, warning, err)
	}
	actual, warning, err = applyRawTotalCostObservation(102, actual, 103, 1, true, warning)
	if err != nil || actual != 1003 || warning != "" {
		t.Fatalf("post-reset cumulative ledger = actual %.2f warning %q err=%v", actual, warning, err)
	}
}

func TestFundingCandidatesConfirmOldestFirstAndRebaseLaterCandidate(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "ordered", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	t1 := time.Now().Add(-2 * time.Minute)
	t2 := t1.Add(time.Minute)
	state, err := svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: 1, RawBalance: 50, SampledAt: t1.Add(-time.Minute), Source: "upstream"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := svc.DetectFunding(FundingCandidateInput{ChannelID: channel.ID, Source: "balance_increase", ExternalID: "first", CreditedAmount: 100, RawBalanceBefore: 50, RawBalanceDetected: 150, FundingFactorBefore: 1, DetectedAt: t1})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawBalance: 150, SampledAt: t1, Source: "upstream"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.DetectFunding(FundingCandidateInput{ChannelID: channel.ID, Source: "balance_increase", ExternalID: "second", CreditedAmount: 100, RawBalanceBefore: 150, RawBalanceDetected: 250, FundingFactorBefore: 1, DetectedAt: t2})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawBalance: 250, SampledAt: t2, Source: "upstream"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.ConfirmFundingCandidate(channel.ID, second.ID, 25, state.Version, "second"); !errors.Is(err, storage.ErrFundingCandidateOrder) {
		t.Fatalf("out-of-order confirmation error = %v", err)
	}
	state, _, err = svc.ConfirmFundingCandidate(channel.ID, first.ID, 50, state.Version, "first")
	if err != nil {
		t.Fatal(err)
	}
	state, _, err = svc.ConfirmFundingCandidate(channel.ID, second.ID, 25, state.Version, "second")
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(state.FundingFactor-0.5) > 1e-12 || state.ActualInventory != 125 {
		t.Fatalf("rebased state = %+v", state)
	}
}

func TestFundingCandidatesAtSameTimeConfirmInIDOrder(t *testing.T) {
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "same-time-ordered", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	effectiveAt := time.Now().UTC().Add(-time.Minute)
	state, err := svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: 1, RawBalance: 50, SampledAt: effectiveAt.Add(-time.Minute), Source: "upstream"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := svc.DetectFunding(FundingCandidateInput{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "same-time-first",
		CreditedAmount: 100, RawBalanceBefore: 50, RawBalanceDetected: 150,
		FundingFactorBefore: state.FundingFactor, DetectedAt: effectiveAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.DetectFunding(FundingCandidateInput{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "same-time-second",
		CreditedAmount: 100, RawBalanceBefore: 150, RawBalanceDetected: 250,
		FundingFactorBefore: state.FundingFactor, DetectedAt: effectiveAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err = svc.ConfirmFundingCandidate(channel.ID, first.ID, 50, state.Version, "first")
	if err != nil {
		t.Fatalf("confirm first same-time candidate: %v", err)
	}
	state, _, err = svc.ConfirmFundingCandidate(channel.ID, second.ID, 25, state.Version, "second")
	if err != nil {
		t.Fatalf("confirm second same-time candidate: %v", err)
	}
	if math.Abs(state.FundingFactor-0.5) > 1e-12 || state.ActualInventory != 25 {
		t.Fatalf("same-time candidate state = %+v", state)
	}

	var corrections int64
	if err := db.Model(&storage.GatewayRequestFundingCorrection{}).Where("channel_id = ?", channel.ID).Count(&corrections).Error; err != nil {
		t.Fatal(err)
	}
	if corrections != 1 {
		t.Fatalf("same-time correction intervals = %d, want 1 non-empty interval", corrections)
	}
}

func TestRecordBalanceWithFundingRollsBackCandidateOnVersionConflict(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "atomic", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	_, _, err = svc.RecordBalanceWithFunding(BalanceInput{ChannelID: channel.ID, ExpectedVersion: 99, RawBalance: 10, SampledAt: time.Now(), Source: "upstream"}, FundingCandidateInput{ChannelID: channel.ID, Source: "balance_increase", ExternalID: "atomic", CreditedAmount: 10, RawBalanceDetected: 10, FundingFactorBefore: 1, DetectedAt: time.Now()})
	if !errors.Is(err, storage.ErrCostStateConflict) {
		t.Fatalf("error = %v", err)
	}
	items, total, err := svc.FundingCandidates(channel.ID, "pending", 10, 0)
	if err != nil || total != 0 || len(items) != 0 {
		t.Fatalf("rolled-back candidates = %d/%d, err=%v", len(items), total, err)
	}
}

func TestRecordHistoricalFundingUsesSeparateEventAndCurrentBalance(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "historical", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	state, err := svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: 1, RawBalance: 50, SampledAt: time.Now(), Source: "upstream"})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordCosts(CostInput{ChannelID: channel.ID, ExpectedVersion: state.Version, RawTodayCost: 20, RawTotalCost: 100, SampledAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	preRaw, projectedRaw, rawTodayBefore, rawTotalBefore := 40.0, 50.0, 10.0, 80.0
	effectiveAt := time.Now().Add(-time.Hour)
	state, event, err := svc.RecordFunding(FundingInput{ChannelID: channel.ID, ExpectedVersion: state.Version, PaidAmount: 10, CreditedAmount: 20, CurrentRawBalance: &preRaw, ProjectedRawBalance: &projectedRaw, RawTodayCostBefore: &rawTodayBefore, RawTotalCostBefore: &rawTotalBefore, EffectiveAt: effectiveAt})
	if err != nil {
		t.Fatal(err)
	}
	wantFactor := 50.0 / 60.0
	if math.Abs(state.FundingFactor-wantFactor) > 1e-12 || math.Abs(state.RawBalance-50) > 1e-12 || math.Abs(state.ActualInventory-50*wantFactor) > 1e-12 || math.Abs(state.ActualTotalCost-(100+20*(wantFactor-1))) > 1e-12 {
		t.Fatalf("historical state = %+v", state)
	}
	if event.RawBalanceBefore != 40 || event.RawBalanceAfter != 60 || event.RawTotalCostBefore != 80 {
		t.Fatalf("historical event = %+v", event)
	}
	_, _, err = svc.RecordFunding(FundingInput{ChannelID: channel.ID, ExpectedVersion: state.Version, PaidAmount: 1, CreditedAmount: 1, EffectiveAt: effectiveAt.Add(-time.Minute)})
	if err == nil {
		t.Fatal("backdated funding before confirmed event succeeded")
	}
}

func TestRecordFundingRejectsPendingCandidate(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "pending", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	detectedAt := time.Now().Add(-time.Minute)
	if _, err := svc.DetectFunding(FundingCandidateInput{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "pending-before-manual",
		CreditedAmount: 10, RawBalanceDetected: 10, FundingFactorBefore: 1, DetectedAt: detectedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.RecordFunding(FundingInput{
		ChannelID: channel.ID, ExpectedVersion: 1, PaidAmount: 5, CreditedAmount: 10,
		EffectiveAt: detectedAt.Add(time.Minute),
	}); !errors.Is(err, storage.ErrPendingFundingCandidate) {
		t.Fatalf("manual funding error = %v", err)
	}
	state, err := svc.State(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 1 || state.FundingFactor != 1 || state.RawBalance != 0 {
		t.Fatalf("rejected funding changed state: %+v", state)
	}
	events, total, err := svc.FundingEvents(channel.ID, 10, 0)
	if err != nil || len(events) != 0 || total != 0 {
		t.Fatalf("rejected funding events = %d/%d, err=%v", len(events), total, err)
	}
}

func TestConfirmStaleFundingCandidateArchivesWithoutDuplicateFunding(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "stale", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	effectiveAt := time.Now().Add(-time.Minute)
	state, event, err := svc.RecordFunding(FundingInput{
		ChannelID: channel.ID, ExpectedVersion: 1, PaidAmount: 5, CreditedAmount: 10, EffectiveAt: effectiveAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := &storage.ChannelFundingCandidate{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "legacy-stale", Status: storage.FundingCandidatePending,
		CreditedAmount: 10, RawBalanceDetected: 10, FundingFactorBefore: 1, DetectedAt: effectiveAt.Add(-time.Minute),
	}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.ConfirmFundingCandidate(channel.ID, candidate.ID, 5, state.Version, "must not be recorded"); !errors.Is(err, storage.ErrFundingCandidateSuperseded) {
		t.Fatalf("stale candidate error = %v", err)
	}
	stored, err := storage.NewCosting(db).FindFundingCandidate(channel.ID, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != storage.FundingCandidateSuperseded || stored.ResolvedEventID == nil || *stored.ResolvedEventID != event.ID {
		t.Fatalf("stale candidate = %+v", stored)
	}
	after, err := svc.State(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Version != state.Version || after.RawBalance != state.RawBalance || after.FundingFactor != state.FundingFactor || after.ActualInventory != state.ActualInventory {
		t.Fatalf("archiving changed cost state: before=%+v after=%+v", state, after)
	}
	events, total, err := svc.FundingEvents(channel.ID, 10, 0)
	if err != nil || len(events) != 1 || total != 1 || events[0].ID != event.ID {
		t.Fatalf("archive created duplicate event: %d/%d, err=%v", len(events), total, err)
	}
	counts, err := svc.PendingFundingCounts()
	if err != nil || counts[channel.ID] != 0 {
		t.Fatalf("pending count after archive = %v, err=%v", counts, err)
	}
}

func TestLateFundingCandidateIsArchivedOnDetection(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "late", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	effectiveAt := time.Now().Add(-time.Minute)
	_, event, err := svc.RecordFunding(FundingInput{
		ChannelID: channel.ID, ExpectedVersion: 1, PaidAmount: 5, CreditedAmount: 10, EffectiveAt: effectiveAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	occurredAt := effectiveAt.Add(-time.Minute)
	candidate, err := svc.DetectFunding(FundingCandidateInput{
		ChannelID: channel.ID, Source: "newapi_topup", ExternalID: "late-order",
		CreditedAmount: 10, RawBalanceDetected: 10, FundingFactorBefore: 1,
		OccurredAt: &occurredAt, DetectedAt: effectiveAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Status != storage.FundingCandidateSuperseded || candidate.ResolvedEventID == nil || *candidate.ResolvedEventID != event.ID {
		t.Fatalf("late candidate = %+v", candidate)
	}
}

func TestPendingFundingCandidateKeepsAutomaticBalanceUpdatesOnFrozenBasis(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "pending-basis", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	state, err := svc.RecordBalance(BalanceInput{
		ChannelID: channel.ID, ExpectedVersion: 1, RawBalance: 10,
		ObservedFactor: 0.5, SampledAt: time.Now().Add(-time.Minute), Source: "advertised",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordCosts(CostInput{
		ChannelID: channel.ID, ExpectedVersion: state.Version,
		RawTodayCost: 8, RawTotalCost: 20, SampledAt: time.Now().Add(-30 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := svc.DetectFunding(FundingCandidateInput{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "frozen-basis",
		CreditedAmount: 10, RawBalanceBefore: 10, RawBalanceDetected: 20,
		FundingFactorBefore: state.FundingFactor, DetectedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err = svc.RecordBalance(BalanceInput{
		ChannelID: channel.ID, ExpectedVersion: state.Version, RawBalance: 20,
		ObservedFactor: 0.25, SampledAt: time.Now(), Source: "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.FundingFactor != 0.5 || state.Source != "advertised" || state.ActualInventory != 10 || state.ActualTodayCost != 4 || state.ActualTotalCost != 10 {
		t.Fatalf("pending candidate basis drifted: %+v", state)
	}
	state, _, err = svc.ConfirmFundingCandidate(channel.ID, candidate.ID, 5, state.Version, "confirmed after refresh")
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(state.FundingFactor-(10.0/20.0)) > 1e-12 {
		t.Fatalf("confirmed funding factor = %v", state.FundingFactor)
	}
}

func TestConfirmHistoricalCandidateRecoversAutomaticBasisDriftWithoutLaterLedgerEvent(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "historical-basis-drift", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage.NewCosting(db))
	candidate, err := svc.DetectFunding(FundingCandidateInput{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "legacy-drift",
		CreditedAmount: 10, RawBalanceBefore: 0, RawBalanceDetected: 10,
		FundingFactorBefore: 1, DetectedAt: time.Now().Add(-time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&storage.ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"raw_balance": 10, "actual_inventory": 2.5, "funding_factor": 0.25,
		"source": "advertised", "version": 2,
	}).Error; err != nil {
		t.Fatal(err)
	}
	state, _, err := svc.ConfirmFundingCandidate(channel.ID, candidate.ID, 5, 2, "recover old drift")
	if err != nil {
		t.Fatal(err)
	}
	if state.FundingFactor != 0.5 || state.ActualInventory != 5 {
		t.Fatalf("recovered state = %+v", state)
	}
}
