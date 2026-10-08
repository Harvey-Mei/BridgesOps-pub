package storage

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestUpdateStateForChannelIdentityAppendsAtomicSnapshots(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "identity-cost-snapshots", Type: ChannelTypeNewAPI,
		SiteURL: "https://example.com", PasswordCipher: "credential",
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	repo := NewCosting(db)
	current, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	balanceAt := time.Date(2026, 7, 29, 9, 0, 0, 0, time.UTC)
	costAt := balanceAt.Add(time.Second)
	next := *current
	next.RawBalance = 20
	next.ActualInventory = 10
	next.FundingFactor = 0.5
	next.RawTodayCost = 4
	next.ActualTodayCost = 2
	next.RawTotalCost = 12
	next.ActualTotalCost = 6
	next.BalanceSampledAt = &balanceAt
	next.CostSampledAt = &costAt
	next.Source = "recharge_history"
	if err := repo.UpdateStateForChannelIdentity(
		&next, current.Version, channel.UpstreamAccountIdentityRevision, true,
	); err != nil {
		t.Fatalf("update identity-fenced cost state: %v", err)
	}

	var balances []BalanceSnapshot
	if err := db.Where("channel_id = ?", channel.ID).Find(&balances).Error; err != nil {
		t.Fatal(err)
	}
	var costs []CostSnapshot
	if err := db.Where("channel_id = ?", channel.ID).Find(&costs).Error; err != nil {
		t.Fatal(err)
	}
	if len(balances) != 1 || balances[0].CostBasisVersion != next.Version ||
		balances[0].CostUnitEpoch != next.CostUnitEpoch || balances[0].Balance != next.ActualInventory {
		t.Fatalf("identity-fenced balance snapshots = %+v, state = %+v", balances, next)
	}
	if len(costs) != 1 || costs[0].CostBasisVersion != next.Version ||
		costs[0].CostUnitEpoch != next.CostUnitEpoch || costs[0].TotalCost != next.ActualTotalCost {
		t.Fatalf("identity-fenced cost snapshots = %+v, state = %+v", costs, next)
	}
}

func TestFundingCandidateBalanceBoundaryRejectsNegativeEvidence(t *testing.T) {
	for name, candidate := range map[string]*ChannelFundingCandidate{
		"negative previous": {RawBalanceBefore: -1, RawBalanceDetected: 1},
		"negative detected": {RawBalanceBefore: 1, RawBalanceDetected: -1},
		"non-finite":        {RawBalanceBefore: math.NaN(), RawBalanceDetected: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateFundingCandidateBalanceBoundary(candidate); err == nil {
				t.Fatal("expected invalid funding candidate balance evidence")
			}
		})
	}
	if err := validateFundingCandidateBalanceBoundary(&ChannelFundingCandidate{RawBalanceBefore: 0, RawBalanceDetected: 1}); err != nil {
		t.Fatalf("valid candidate balance evidence: %v", err)
	}
}

func TestUpdateBalanceStatePreservesSignedInventoryWithPendingFunding(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{
		Name: "negative-balance-pending-funding", Type: ChannelTypeNewAPI,
		SiteURL: "https://example.com", PasswordCipher: "credential",
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"funding_factor": 0.5,
		"source":         "recharge_history",
	}).Error; err != nil {
		t.Fatal(err)
	}
	repo := NewCosting(db)
	current, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFundingCandidate{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "existing-pending",
		Status: FundingCandidatePending, CreditedAmount: 10,
		RawBalanceBefore: 10, RawBalanceDetected: 20, FundingFactorBefore: 0.5,
		DetectedAt: time.Now().UTC(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	next := *current
	next.RawBalance = -4
	next.ActualInventory = -4
	sampledAt := time.Now().UTC().Add(time.Second)
	next.BalanceSampledAt = &sampledAt
	if err := repo.UpdateBalanceState(&next, current.Version); err != nil {
		t.Fatalf("update signed balance with pending funding: %v", err)
	}
	stored, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RawBalance != -4 || stored.ActualInventory != -2 || stored.FundingFactor != 0.5 {
		t.Fatalf("pending funding did not preserve signed inventory: %+v", stored)
	}
	var projected Channel
	if err := db.First(&projected, channel.ID).Error; err != nil {
		t.Fatal(err)
	}
	if projected.RawLastBalance == nil || *projected.RawLastBalance != -4 || projected.LastBalance == nil || *projected.LastBalance != -2 {
		t.Fatalf("channel signed balance projection = %+v", projected)
	}
}

func TestChannelDeletePrepareRetainsCostStateAndFundingLedger(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{Name: "delete-cost", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "x"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&ChannelFundingEvent{ChannelID: channel.ID, Kind: "recharge", PaidAmount: 1, CreditedAmount: 1, FundingFactorBefore: 1, FundingFactorAfter: 1, EffectiveAt: time.Now()}).Error; err != nil {
		t.Fatal(err)
	}
	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatal(err)
	}
	for _, model := range []any{&ChannelCostState{}, &ChannelFundingEvent{}} {
		var count int64
		if err := db.Model(model).Where("channel_id = ?", channel.ID).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Fatalf("delete prepare removed %T rows", model)
		}
	}
}

func TestAppendRequestLogFreezesMissingFundingFactor(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "funded", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", PasswordCipher: "x"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"funding_factor": 0.5,
		"version":        7,
		"source":         "recharge_history",
	}).Error; err != nil {
		t.Fatal(err)
	}
	log := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "gpt", ModelName: "gpt-5",
		GroupRatio: 2, CompletionRatio: 4, Method: "POST", Path: "/v1/responses",
		StatusCode: 200, Cost: 1.5, CostSource: "reported", CreatedAt: time.Now().UTC(),
	}
	if err := NewGateway(db).AppendRequestLog(log); err != nil {
		t.Fatal(err)
	}
	if log.FundingFactor == nil || *log.FundingFactor != 0.5 || log.CostBasisVersion != 7 || log.CostBasisSource != "recharge_history" {
		t.Fatalf("frozen cost basis = %+v", log)
	}
	if log.RawCost == nil || *log.RawCost != 3 || log.RawGroupRatio == nil || *log.RawGroupRatio != 4 || log.RawCompletionRatio == nil || *log.RawCompletionRatio != 8 {
		t.Fatalf("derived raw values = %+v", log)
	}
	var aggregate GatewayRequestAggregate
	if err := db.Where("downstream_key_id = ? AND version = ?", log.DownstreamKeyID, gatewayRequestAggregateVersion).First(&aggregate).Error; err != nil {
		t.Fatal(err)
	}
	if aggregate.TotalCost != log.Cost || aggregate.Requests != 1 || log.AggregateVersion != gatewayRequestAggregateVersion {
		t.Fatalf("detail/aggregate mismatch: log=%+v aggregate=%+v", log, aggregate)
	}
}

func TestFundingCorrectionsDoNotRefillPricingFencedRequestLogs(t *testing.T) {
	corrections := []struct {
		name  string
		apply func(t *testing.T, db *gorm.DB, repo *Costing, state *ChannelCostState, next *ChannelCostState, effectiveAt time.Time, reason string) error
	}{
		{
			name: "append funding event",
			apply: func(_ *testing.T, _ *gorm.DB, repo *Costing, state *ChannelCostState, next *ChannelCostState, effectiveAt time.Time, _ string) error {
				return repo.AppendFundingEvent(next, &ChannelFundingEvent{
					ChannelID: state.ChannelID, Kind: "funding_correction",
					FundingFactorBefore: state.FundingFactor, FundingFactorAfter: next.FundingFactor,
					EffectiveAt: effectiveAt,
				}, state.Version)
			},
		},
		{
			name: "confirm funding candidate",
			apply: func(t *testing.T, db *gorm.DB, repo *Costing, state *ChannelCostState, next *ChannelCostState, effectiveAt time.Time, reason string) error {
				candidate := &ChannelFundingCandidate{
					ChannelID: state.ChannelID, Source: "balance_increase", ExternalID: "pricing-fence-" + reason,
					Status: FundingCandidatePending, CreditedAmount: 10, RawBalanceDetected: 10,
					FundingFactorBefore: state.FundingFactor, DetectedAt: effectiveAt.Add(-time.Second),
				}
				if err := db.Create(candidate).Error; err != nil {
					t.Fatalf("create funding candidate: %v", err)
				}
				return repo.ConfirmFundingCandidate(next, &ChannelFundingEvent{
					ChannelID: state.ChannelID, Kind: "funding_correction",
					FundingFactorBefore: state.FundingFactor, FundingFactorAfter: next.FundingFactor,
					EffectiveAt: effectiveAt,
				}, candidate.ID, state.Version)
			},
		},
	}

	for _, correction := range corrections {
		for _, reason := range []string{GatewayPricingUnavailableScopeIncomplete, GatewayPricingUnavailableIdentityChanged} {
			t.Run(correction.name+"/"+reason, func(t *testing.T) {
				db := openTestDB(t)
				channel := &Channel{
					Name: "pricing-fence-correction-" + strings.ReplaceAll(t.Name(), "/", "-"),
					Type: ChannelTypeNewAPI, SiteURL: "https://example.com", PasswordCipher: "x",
				}
				if err := NewChannels(db).Create(channel); err != nil {
					t.Fatalf("create channel: %v", err)
				}
				repo := NewCosting(db)
				state, err := repo.FindState(channel.ID)
				if err != nil {
					t.Fatalf("load cost state: %v", err)
				}
				next := *state
				next.FundingFactor = 2
				next.Source = "funding_correction"
				effectiveAt := time.Now().UTC().Add(-time.Minute)
				originalEvidenceFactor := state.FundingFactor
				dispatchEvidence := &GatewayRequestLog{
					RecordKind: GatewayRequestRecordKindDispatchCostEvidence,
					ChannelID:  channel.ID, UpstreamRequestID: "provider-pending-" + strings.ReplaceAll(t.Name(), "/", "-"),
					FundingFactor: &originalEvidenceFactor, CreatedAt: effectiveAt.Add(3 * time.Second),
				}
				if err := NewGateway(db).AppendRequestLog(dispatchEvidence); err != nil {
					t.Fatalf("append pending dispatch evidence: %v", err)
				}

				fenced := &GatewayRequestLog{
					DownstreamKeyID: 1, ChannelID: channel.ID, Method: "POST", Path: "/v1/responses", StatusCode: 200,
					CostStatus: GatewayCostStatusUnavailable, CostUnavailableReason: reason,
					CreatedAt: effectiveAt.Add(time.Second),
				}
				if err := NewGateway(db).AppendRequestLog(fenced); err != nil {
					t.Fatalf("append pricing-fenced request log: %v", err)
				}

				rawCost, rawGroupRatio, rawCompletionRatio, originalFactor := 5.0, 3.0, 4.0, 1.0
				ordinary := &GatewayRequestLog{
					DownstreamKeyID: 2, ChannelID: channel.ID, Method: "POST", Path: "/v1/responses", StatusCode: 200,
					Cost: rawCost, RawCost: &rawCost, CostEstimated: true, CostSource: "price_fact", CostStatus: GatewayCostStatusEstimated,
					GroupRatio: rawGroupRatio, RawGroupRatio: &rawGroupRatio,
					CompletionRatio: rawCompletionRatio, RawCompletionRatio: &rawCompletionRatio,
					FundingFactor: &originalFactor, CostBasisVersion: state.Version, CostBasisSource: state.Source,
					CreatedAt: effectiveAt.Add(2 * time.Second),
				}
				if err := NewGateway(db).AppendRequestLog(ordinary); err != nil {
					t.Fatalf("append ordinary request log: %v", err)
				}

				if err := correction.apply(t, db, repo, state, &next, effectiveAt, reason); err != nil {
					t.Fatalf("apply %s: %v", correction.name, err)
				}

				var storedFenced, storedOrdinary, storedDispatchEvidence GatewayRequestLog
				if err := db.First(&storedFenced, fenced.ID).Error; err != nil {
					t.Fatalf("load fenced request log: %v", err)
				}
				if err := db.First(&storedOrdinary, ordinary.ID).Error; err != nil {
					t.Fatalf("load ordinary request log: %v", err)
				}
				if err := db.First(&storedDispatchEvidence, dispatchEvidence.ID).Error; err != nil {
					t.Fatalf("load pending dispatch evidence: %v", err)
				}
				assertFundingCorrectionPreservedPricingFence(t, storedFenced, reason)
				if storedDispatchEvidence.RecordKind != GatewayRequestRecordKindDispatchCostEvidence ||
					storedDispatchEvidence.FundingFactor == nil || *storedDispatchEvidence.FundingFactor != originalEvidenceFactor ||
					storedDispatchEvidence.Cost != 0 || storedDispatchEvidence.RawCost != nil ||
					storedDispatchEvidence.CostStatus != GatewayCostStatusNone || storedDispatchEvidence.UpstreamUsageID != nil ||
					storedDispatchEvidence.DeferredFundingFactor == nil || *storedDispatchEvidence.DeferredFundingFactor != next.FundingFactor ||
					storedDispatchEvidence.DeferredCostBasisVersion == nil || *storedDispatchEvidence.DeferredCostBasisVersion != next.Version ||
					storedDispatchEvidence.DeferredCostBasisSource == nil || *storedDispatchEvidence.DeferredCostBasisSource != next.Source {
					t.Fatalf("funding correction did not preserve pending dispatch evidence with deferred basis: %+v", storedDispatchEvidence)
				}
				if storedOrdinary.FundingFactor == nil || *storedOrdinary.FundingFactor != next.FundingFactor ||
					storedOrdinary.CostBasisVersion != next.Version || storedOrdinary.CostBasisSource != next.Source ||
					storedOrdinary.Cost != rawCost*next.FundingFactor || storedOrdinary.GroupRatio != rawGroupRatio*next.FundingFactor ||
					storedOrdinary.CompletionRatio != rawCompletionRatio*next.FundingFactor {
					t.Fatalf("ordinary request log missed funding correction: %+v", storedOrdinary)
				}
			})
		}
	}
}

func assertFundingCorrectionPreservedPricingFence(t *testing.T, log GatewayRequestLog, reason string) {
	t.Helper()
	if log.CostStatus != GatewayCostStatusUnavailable || log.CostUnavailableReason != reason ||
		log.Cost != 0 || log.GroupRatio != 0 || log.CompletionRatio != 0 || log.RawCost != nil ||
		log.RawGroupRatio != nil || log.RawCompletionRatio != nil || log.FundingFactor != nil ||
		log.CostBasisVersion != 0 || log.CostBasisSource != "" {
		t.Fatalf("funding correction refilled hard-fenced request metadata: %+v", log)
	}
}

func TestCostingRepositoryRollsBackEventOnVersionConflict(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "conflict", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "x"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	repo := NewCosting(db)
	state, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	event := &ChannelFundingEvent{ChannelID: channel.ID, Kind: "recharge", PaidAmount: 1, CreditedAmount: 1, EffectiveAt: time.Now()}
	if err := repo.AppendFundingEvent(state, event, 99); !errors.Is(err, ErrCostStateConflict) {
		t.Fatalf("error = %v", err)
	}
	var count int64
	if err := db.Model(&ChannelFundingEvent{}).Count(&count).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("events after conflict = %d", count)
	}
}

func TestReconcileSupersededFundingCandidates(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "reconcile-stale", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "x"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	effectiveAt := time.Now().Add(-time.Minute)
	event := &ChannelFundingEvent{
		ChannelID: channel.ID, Kind: "recharge", PaidAmount: 5, CreditedAmount: 10,
		RawBalanceBefore: 0, RawBalanceAfter: 10,
		FundingFactorBefore: 1, FundingFactorAfter: 0.5, EffectiveAt: effectiveAt,
	}
	if err := db.Create(event).Error; err != nil {
		t.Fatal(err)
	}
	candidate := &ChannelFundingCandidate{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "stuck", Status: FundingCandidatePending,
		CreditedAmount: 10, RawBalanceDetected: 10, FundingFactorBefore: 1, DetectedAt: effectiveAt.Add(-time.Minute),
	}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}
	repo := NewCosting(db)
	resolved, err := repo.ReconcileSupersededFundingCandidates(100)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != 1 {
		t.Fatalf("resolved = %d, want 1", resolved)
	}
	stored, err := repo.FindFundingCandidate(channel.ID, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != FundingCandidateSuperseded || stored.ResolvedEventID == nil || *stored.ResolvedEventID != event.ID {
		t.Fatalf("reconciled candidate = %+v", stored)
	}
	counts, err := repo.PendingFundingCounts()
	if err != nil || counts[channel.ID] != 0 {
		t.Fatalf("pending counts = %v, err=%v", counts, err)
	}
	resolved, err = repo.ReconcileSupersededFundingCandidates(100)
	if err != nil || resolved != 0 {
		t.Fatalf("idempotent reconcile = %d, err=%v", resolved, err)
	}
}

func TestReconcileDoesNotSupersedeUnrelatedFundingCandidate(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "reconcile-unrelated", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "x"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	candidateTime := time.Now().Add(-2 * time.Minute)
	event := &ChannelFundingEvent{
		ChannelID: channel.ID, Kind: "recharge", PaidAmount: 5, CreditedAmount: 20,
		RawBalanceBefore: 10, RawBalanceAfter: 30,
		FundingFactorBefore: 1, FundingFactorAfter: 0.5, EffectiveAt: candidateTime.Add(time.Minute),
	}
	if err := db.Create(event).Error; err != nil {
		t.Fatal(err)
	}
	candidate := &ChannelFundingCandidate{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "different-recharge", Status: FundingCandidatePending,
		CreditedAmount: 10, RawBalanceBefore: 0, RawBalanceDetected: 10,
		FundingFactorBefore: 1, DetectedAt: candidateTime,
	}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}

	repo := NewCosting(db)
	resolved, err := repo.ReconcileSupersededFundingCandidates(100)
	if err != nil || resolved != 0 {
		t.Fatalf("unrelated reconcile = %d, err=%v", resolved, err)
	}
	stored, err := repo.FindFundingCandidate(channel.ID, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != FundingCandidatePending || stored.ResolvedEventID != nil {
		t.Fatalf("unrelated candidate was silently superseded: %+v", stored)
	}
}

func TestReconcileSupersedesImportedCandidateCoveredByLaterEvent(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "reconcile-imported", Type: ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "x"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	detectedAt := time.Now().Add(-time.Minute)
	event := &ChannelFundingEvent{
		ChannelID: channel.ID, Kind: "recharge", PaidAmount: 10, CreditedAmount: 100.000001,
		RawBalanceBefore: 0.099999, RawBalanceAfter: 100.1,
		FundingFactorBefore: 10, FundingFactorAfter: 0.10989, EffectiveAt: detectedAt.Add(23 * time.Second),
	}
	if err := db.Create(event).Error; err != nil {
		t.Fatal(err)
	}
	candidate := &ChannelFundingCandidate{
		ChannelID: channel.ID, Source: "redeem_code", ExternalID: "imported-redeem", Status: FundingCandidatePending,
		CreditedAmount: 100.1, RawBalanceBefore: 0, RawBalanceDetected: 100.1,
		FundingFactorBefore: 1, DetectedAt: detectedAt,
	}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}

	repo := NewCosting(db)
	resolved, err := repo.ReconcileSupersededFundingCandidates(100)
	if err != nil || resolved != 1 {
		t.Fatalf("imported candidate reconcile = %d, err=%v", resolved, err)
	}
	stored, err := repo.FindFundingCandidate(channel.ID, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != FundingCandidateSuperseded || stored.ResolvedEventID == nil || *stored.ResolvedEventID != event.ID {
		t.Fatalf("imported candidate = %+v", stored)
	}
}

func TestCoveredFundingEvidenceRemainsFailClosed(t *testing.T) {
	detectedAt := time.Now().Add(-time.Minute)
	baseCandidate := ChannelFundingCandidate{
		ChannelID: 7, Source: "redeem_code", CreditedAmount: 100.1,
		RawBalanceBefore: 0, RawBalanceDetected: 100.1, FundingFactorBefore: 1, DetectedAt: detectedAt,
	}
	baseEvent := ChannelFundingEvent{
		ChannelID: 7, Kind: "recharge", PaidAmount: 10, CreditedAmount: 100.000001,
		RawBalanceBefore: 0.099999, RawBalanceAfter: 100.1,
		FundingFactorBefore: 10, EffectiveAt: detectedAt.Add(time.Minute),
	}
	tests := []struct {
		name      string
		candidate ChannelFundingCandidate
		event     ChannelFundingEvent
	}{
		{name: "different final balance", candidate: baseCandidate, event: func() ChannelFundingEvent { value := baseEvent; value.RawBalanceAfter = 100.2; return value }()},
		{name: "independent small recharge", candidate: func() ChannelFundingCandidate {
			value := baseCandidate
			value.CreditedAmount = 100
			value.RawBalanceDetected = 100
			return value
		}(), event: func() ChannelFundingEvent {
			value := baseEvent
			value.CreditedAmount = 1
			value.RawBalanceBefore = 99
			value.RawBalanceAfter = 100
			return value
		}()},
		{name: "late event", candidate: baseCandidate, event: func() ChannelFundingEvent {
			value := baseEvent
			value.EffectiveAt = detectedAt.Add(6 * time.Minute)
			return value
		}()},
		{name: "known paid amount", candidate: func() ChannelFundingCandidate {
			value := baseCandidate
			paid := 10.0
			value.PaidAmount = &paid
			return value
		}(), event: baseEvent},
		{name: "authoritative occurred time", candidate: func() ChannelFundingCandidate { value := baseCandidate; value.OccurredAt = &detectedAt; return value }(), event: baseEvent},
		{name: "unsupported source", candidate: func() ChannelFundingCandidate { value := baseCandidate; value.Source = "newapi_topup"; return value }(), event: baseEvent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if fundingCandidateMatchesEvent(&tt.candidate, &tt.event) {
				t.Fatal("unrelated evidence was treated as covered")
			}
		})
	}
}

func TestReconcileBatchAdvancesPastUnmatchedCandidatePage(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "reconcile-pages", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: "x"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	detectedAt := time.Now().Add(-10 * time.Minute)
	candidates := []ChannelFundingCandidate{
		{ChannelID: channel.ID, Source: "redeem_code", ExternalID: "late", Status: FundingCandidatePending, CreditedAmount: 10, RawBalanceDetected: 10, FundingFactorBefore: 1, DetectedAt: detectedAt},
		{ChannelID: channel.ID, Source: "newapi_topup", ExternalID: "unsupported", Status: FundingCandidatePending, CreditedAmount: 20, RawBalanceDetected: 20, FundingFactorBefore: 1, DetectedAt: detectedAt},
		{ChannelID: channel.ID, Source: "balance_increase", ExternalID: "matched", Status: FundingCandidatePending, CreditedAmount: 30, RawBalanceDetected: 30, FundingFactorBefore: 1, DetectedAt: detectedAt},
	}
	for i := range candidates {
		if err := db.Create(&candidates[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	events := []ChannelFundingEvent{
		{ChannelID: channel.ID, Kind: "recharge", CreditedAmount: 10, RawBalanceAfter: 10, FundingFactorBefore: 2, EffectiveAt: detectedAt.Add(6 * time.Minute)},
		{ChannelID: channel.ID, Kind: "recharge", CreditedAmount: 20, RawBalanceAfter: 20, FundingFactorBefore: 2, EffectiveAt: detectedAt.Add(time.Minute)},
		{ChannelID: channel.ID, Kind: "recharge", CreditedAmount: 30, RawBalanceAfter: 30, FundingFactorBefore: 1, EffectiveAt: detectedAt.Add(time.Minute)},
	}
	for i := range events {
		if err := db.Create(&events[i]).Error; err != nil {
			t.Fatal(err)
		}
	}

	repo := NewCosting(db)
	first, err := repo.ReconcileSupersededFundingCandidatesBatchContext(context.Background(), 0, 0, 2)
	if err != nil {
		t.Fatalf("first reconcile page: %v", err)
	}
	if first.HighWaterID != candidates[2].ID || first.NextID != candidates[1].ID || first.Scanned != 2 || first.Resolved != 0 || first.Exhausted {
		t.Fatalf("first reconcile page = %+v", first)
	}
	insertedAfterSnapshot := ChannelFundingCandidate{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "after-high-water", Status: FundingCandidatePending,
		CreditedAmount: 40, RawBalanceDetected: 40, FundingFactorBefore: 1, DetectedAt: detectedAt,
	}
	if err := db.Create(&insertedAfterSnapshot).Error; err != nil {
		t.Fatalf("create candidate after reconcile snapshot: %v", err)
	}
	if err := db.Create(&ChannelFundingEvent{
		ChannelID: channel.ID, Kind: "recharge", CreditedAmount: 40, RawBalanceAfter: 40,
		FundingFactorBefore: 1, EffectiveAt: detectedAt.Add(time.Minute),
	}).Error; err != nil {
		t.Fatalf("create event after reconcile snapshot: %v", err)
	}
	second, err := repo.ReconcileSupersededFundingCandidatesBatchContext(context.Background(), first.NextID, first.HighWaterID, 2)
	if err != nil {
		t.Fatalf("second reconcile page: %v", err)
	}
	if second.HighWaterID != first.HighWaterID || second.NextID != candidates[2].ID || second.Scanned != 1 || second.Resolved != 1 || !second.Exhausted {
		t.Fatalf("second reconcile page = %+v", second)
	}
	matched, err := repo.FindFundingCandidate(channel.ID, candidates[2].ID)
	if err != nil {
		t.Fatal(err)
	}
	if matched.Status != FundingCandidateSuperseded || matched.ResolvedEventID == nil || *matched.ResolvedEventID != events[2].ID {
		t.Fatalf("matched candidate = %+v", matched)
	}
	outsideSnapshot, err := repo.FindFundingCandidate(channel.ID, insertedAfterSnapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	if outsideSnapshot.Status != FundingCandidatePending || outsideSnapshot.ResolvedEventID != nil {
		t.Fatalf("candidate above reconcile high-water was processed: %+v", outsideSnapshot)
	}
	resolved, err := repo.ReconcileSupersededFundingCandidates(2)
	if err != nil || resolved != 1 {
		t.Fatalf("legacy reconcile across unmatched page = %d, err=%v", resolved, err)
	}
	outsideSnapshot, err = repo.FindFundingCandidate(channel.ID, insertedAfterSnapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	if outsideSnapshot.Status != FundingCandidateSuperseded || outsideSnapshot.ResolvedEventID == nil {
		t.Fatalf("legacy reconcile did not scan past unmatched page: %+v", outsideSnapshot)
	}
}
