package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func prepareChannelDeletionForTest(t *testing.T, db *gorm.DB, channels *Channels, channelID uint) (*Channel, *ChannelDeletionIntent) {
	t.Helper()
	if err := channels.Delete(channelID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion error = %v, want %v", err, ErrChannelDeletionPending)
	}
	var channel Channel
	if err := db.First(&channel, channelID).Error; err != nil {
		t.Fatalf("prepared channel was removed: %v", err)
	}
	if !channel.DeletionPending || channel.DeletionEpoch == 0 ||
		channel.DeletionIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		channel.DeletionState != string(ChannelDeletionIntentPendingRemoteCleanup) || channel.DeletionRequestedAt == nil ||
		channel.MonitorEnabled || channel.LatencyProbeEnabled {
		t.Fatalf("prepared channel fence = %+v", channel)
	}
	var intents []ChannelDeletionIntent
	if err := db.Where("channel_id = ?", channelID).Order("id ASC").Find(&intents).Error; err != nil {
		t.Fatalf("load channel deletion intent: %v", err)
	}
	if len(intents) != 1 {
		t.Fatalf("channel deletion intents = %+v, want exactly one", intents)
	}
	intent := &intents[0]
	if intent.DeletionEpoch != channel.DeletionEpoch ||
		intent.ChannelIdentityRevision != channel.DeletionIdentityRevision ||
		intent.ChannelNameSnapshot != channel.Name || intent.ChannelTypeSnapshot != channel.Type ||
		intent.Status != ChannelDeletionIntentPendingRemoteCleanup || intent.RequestedAt.IsZero() {
		t.Fatalf("channel deletion intent = %+v, channel = %+v", intent, channel)
	}
	return &channel, intent
}

func seedUpstreamAccountIdentityForCostUnitTest(t *testing.T, channels *Channels, channelID uint) {
	t.Helper()
	current, err := channels.FindByID(channelID)
	if err != nil {
		t.Fatalf("load channel for account identity: %v", err)
	}
	if _, err := channels.ObserveUpstreamAccountIdentity(UpstreamAccountIdentityObservation{
		ChannelID: channelID, ExpectedIdentityRevision: current.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: current.AdminRevision, Provider: ChannelTypeSub2API,
		APIOrigin: "https://example.com", AccountID: "upstream-account-1",
		Scope: UpstreamConcurrencyScopeAccount, Source: "auth_me",
		ObservedAt: time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Microsecond),
	}); err != nil {
		t.Fatalf("observe upstream account identity: %v", err)
	}
}

func prepareCostUnitRebase(t *testing.T, db *gorm.DB, repo *Costing, channelID uint) (*ChannelCostState, *ChannelCostUnitRebase, uint64) {
	t.Helper()
	seedUpstreamAccountIdentityForCostUnitTest(t, NewChannels(db), channelID)
	beforeBalanceAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Microsecond)
	beforeCostAt := beforeBalanceAt.Add(10 * time.Second)
	const expectedVersion uint64 = 7
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", channelID).Updates(map[string]any{
		"raw_balance": 100, "actual_inventory": 10, "funding_factor": 0.1,
		"raw_today_cost": 20, "actual_today_cost": 2,
		"raw_total_cost": 1000, "actual_total_cost": 100,
		"balance_sampled_at": beforeBalanceAt, "cost_sampled_at": beforeCostAt,
		"observed_provider_factor": 0.1, "observed_provider_factor_source": "advertised",
		"observed_provider_factor_sampled_at": beforeCostAt,
		"version":                             expectedVersion, "source": "confirmed_redeem_code", "status": "ready",
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, rate := range []*RateSnapshot{
		{ChannelID: channelID, ModelName: "PLUS", Description: "plus", Ratio: 0.6, LastSeenAt: beforeCostAt},
		{ChannelID: channelID, ModelName: "promo", Description: "promo", Ratio: 0.15, LastSeenAt: beforeCostAt},
	} {
		if err := db.Create(rate).Error; err != nil {
			t.Fatal(err)
		}
	}
	state, err := repo.FindState(channelID)
	if err != nil {
		t.Fatal(err)
	}
	afterBalanceAt := beforeBalanceAt.Add(time.Minute)
	afterCostAt := afterBalanceAt.Add(10 * time.Second)
	observedFactor := 1.0
	next := *state
	next.CostUnitEpoch = state.CostUnitEpoch + 1
	next.RawBalance = 9
	next.ActualInventory = 9
	next.FundingFactor = 1
	next.RawTodayCost = 2.5
	next.ActualTodayCost = 2.5
	next.RawTotalCost = 982.5
	next.ActualTotalCost = 100.5
	next.BalanceSampledAt = &afterBalanceAt
	next.CostSampledAt = &afterCostAt
	next.ObservedProviderFactor = &observedFactor
	next.ObservedProviderFactorSource = "advertised"
	next.ObservedProviderFactorSampledAt = &afterCostAt
	next.Source = "cost_unit_rebase"
	next.Status = "ready"
	next.LastError = ""
	rateItems := []CostUnitRateTransitionItem{
		{ModelName: "PLUS", DescriptionBefore: "plus", DescriptionAfter: "plus", RatioBefore: 0.6, RatioAfter: 0.06},
		{ModelName: "promo", DescriptionBefore: "promo", DescriptionAfter: "promo", RatioBefore: 0.15, RatioAfter: 0.015},
	}
	rateDigest, err := CostUnitRateEvidenceDigest(rateItems)
	if err != nil {
		t.Fatal(err)
	}
	audit := &ChannelCostUnitRebase{
		ChannelID: channelID, IdempotencyKey: "rebase-10-to-1",
		RequestFingerprint: strings.Repeat("a", 64), Actor: "administrator",
		PreviousMultiplier: 10, CurrentMultiplier: 1,
		PreviousMultiplierMode: "divide", CurrentMultiplierMode: "divide",
		PreviousProviderFactor: 0.1, CurrentProviderFactor: 1,
		RateTransitionAt: beforeCostAt.Add(20 * time.Second), RateRawScale: 0.1,
		RateEvidenceGroupCount: len(rateItems), RateEvidenceDigest: rateDigest,
		CounterTransition: "prior_unchanged", RateTransitionItems: rateItems,
		Note: "provider changed raw credit unit from 10 to 1", AppliedAt: afterCostAt,
	}
	return &next, audit, expectedVersion
}
