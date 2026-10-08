package api

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestChannelCostUnitRebaseReadinessUsesPersistedProviderEvidence(t *testing.T) {
	now := time.Date(2026, time.July, 27, 1, 2, 3, 0, time.UTC)
	factor := 0.1
	channel := &storage.Channel{
		ID: 41, Type: storage.ChannelTypeSub2API,
		RechargeMultiplierMode: "divide",
	}
	state := &storage.ChannelCostState{
		ChannelID: 41, Version: 18, CostUnitEpoch: 7,
		Status:                 costing.StateStatusNeedsReview,
		LastError:              costing.ErrCostUnitChangeRequiresRebase.Error() + ": persisted factor 0.1, observed factor 1",
		ObservedProviderFactor: &factor, ObservedProviderFactorSource: "advertised",
		BalanceSampledAt: &now, CostSampledAt: &now,
	}

	readiness := buildChannelCostUnitRebaseReadiness(channel, state, 0)
	if !readiness.Supported || !readiness.Eligible || len(readiness.Blockers) != 0 {
		t.Fatalf("readiness = %+v", readiness)
	}
	if readiness.ExpectedVersion != 18 || readiness.ExpectedCostUnitEpoch != 7 || readiness.MultiplierMode != "divide" {
		t.Fatalf("fences = %+v", readiness)
	}
	if readiness.PreviousMultiplier == nil || *readiness.PreviousMultiplier != 10 {
		t.Fatalf("previous multiplier = %v", readiness.PreviousMultiplier)
	}
	if readiness.PreviousProviderFactor == nil || *readiness.PreviousProviderFactor != factor {
		t.Fatalf("previous provider factor = %v", readiness.PreviousProviderFactor)
	}
}

func TestChannelCostUnitRebaseReadinessExplainsStableBlockers(t *testing.T) {
	now := time.Now().UTC()
	factor := 0.1
	manual := 10.0
	channel := &storage.Channel{
		ID: 42, Type: storage.ChannelTypeSub2API,
		RechargeMultiplier: &manual, RechargeMultiplierMode: "divide",
	}
	state := &storage.ChannelCostState{
		ChannelID: 42, Version: 3, CostUnitEpoch: 2,
		Status:                 costing.StateStatusNeedsReview,
		LastError:              costing.ErrCostUnitChangeRequiresRebase.Error(),
		ObservedProviderFactor: &factor, ObservedProviderFactorSource: "advertised",
		BalanceSampledAt: &now, CostSampledAt: &now,
	}

	readiness := buildChannelCostUnitRebaseReadiness(channel, state, 2)
	if readiness.Supported || readiness.Eligible {
		t.Fatalf("manual multiplier unexpectedly rebasable: %+v", readiness)
	}
	for _, blocker := range []string{"manual_multiplier_configured", "pending_funding_candidates"} {
		if !containsChannelAccountingBlocker(readiness.Blockers, blocker) {
			t.Fatalf("blockers = %v, missing %q", readiness.Blockers, blocker)
		}
	}
}

func TestChannelAccountingReviewReasonSeparatesCalibrationFromUnitChange(t *testing.T) {
	legacy := &storage.ChannelCostState{Status: costing.StateStatusNeedsReview, Source: "legacy_unknown"}
	if got := channelAccountingReviewReason(legacy); got != channelAccountingReviewFundingBasisMissing {
		t.Fatalf("legacy review reason = %q", got)
	}
	factor := 0.2
	changed := &storage.ChannelCostState{
		Status: costing.StateStatusNeedsReview, Source: "confirmed_recharge",
		ObservedProviderFactor: &factor,
		LastError:              costing.ErrCostUnitChangeRequiresRebase.Error() + ": evidence",
	}
	if got := channelAccountingReviewReason(changed); got != channelAccountingReviewProviderFactorChanged {
		t.Fatalf("factor-change review reason = %q", got)
	}
	if got := channelAccountingReviewReason(&storage.ChannelCostState{Status: costing.StateStatusReady}); got != "" {
		t.Fatalf("ready review reason = %q", got)
	}
}

func containsChannelAccountingBlocker(blockers []string, target string) bool {
	for _, blocker := range blockers {
		if blocker == target {
			return true
		}
	}
	return false
}
