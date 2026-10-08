package storage

import (
	"testing"
	"time"
)

func TestCostBasisOverrideRateDigestIsOrderIndependent(t *testing.T) {
	left := []RateSnapshot{
		{ID: 2, ModelName: "promo", Description: "p", Ratio: 0.2, CompletionRatio: 0.3},
		{ID: 1, ModelName: "primary", Description: "p", Ratio: 0.5, CompletionRatio: 0.6},
	}
	right := []RateSnapshot{left[1], left[0]}
	first, err := costBasisOverrideRateDigest(left)
	if err != nil {
		t.Fatal(err)
	}
	second, err := costBasisOverrideRateDigest(right)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("rate digest changed with row order: %q != %q", first, second)
	}
}

func TestCostBasisOverrideFundingCandidateDigestIsOrderIndependent(t *testing.T) {
	detectedAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	left := []ChannelFundingCandidate{
		{ID: 2, ChannelID: 1, Source: "balance_increase", ExternalID: "second", Status: FundingCandidatePending, CreditedAmount: 2, RawBalanceBefore: 10, RawBalanceDetected: 12, FundingFactorBefore: 0.2, DetectedAt: detectedAt.Add(time.Minute)},
		{ID: 1, ChannelID: 1, Source: "redeem_code", ExternalID: "first", Status: FundingCandidatePending, CreditedAmount: 1, RawBalanceBefore: 9, RawBalanceDetected: 10, FundingFactorBefore: 0.2, DetectedAt: detectedAt},
	}
	right := []ChannelFundingCandidate{left[1], left[0]}
	first, err := costBasisOverrideFundingCandidateDigest(left)
	if err != nil {
		t.Fatal(err)
	}
	second, err := costBasisOverrideFundingCandidateDigest(right)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("candidate digest changed with row order: %q != %q", first, second)
	}
}

func TestCostBasisOverrideFundingCandidateDigestIncludesManualEvidence(t *testing.T) {
	detectedAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	paid := 3.5
	candidate := ChannelFundingCandidate{
		ID: 1, ChannelID: 1, Source: "newapi_topup", ExternalID: "payment-1", Status: FundingCandidatePending,
		PaidAmount: &paid, CreditedAmount: 10, RawBalanceBefore: 5, RawBalanceDetected: 15,
		FundingFactorBefore: 0.2, DetectedAt: detectedAt, Note: "provider payment evidence",
	}
	baseline, err := costBasisOverrideFundingCandidateDigest([]ChannelFundingCandidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	changedPaid := candidate
	otherPaid := 4.5
	changedPaid.PaidAmount = &otherPaid
	paidDigest, err := costBasisOverrideFundingCandidateDigest([]ChannelFundingCandidate{changedPaid})
	if err != nil {
		t.Fatal(err)
	}
	changedNote := candidate
	changedNote.Note = "different provider evidence"
	noteDigest, err := costBasisOverrideFundingCandidateDigest([]ChannelFundingCandidate{changedNote})
	if err != nil {
		t.Fatal(err)
	}
	if baseline == paidDigest || baseline == noteDigest {
		t.Fatalf("candidate digest omitted manual evidence: baseline=%q paid=%q note=%q", baseline, paidDigest, noteDigest)
	}
}

func TestCostBasisOverrideEffectiveCompletionUsesPromptFallback(t *testing.T) {
	if got := costBasisOverrideEffectiveCompletionRatio(2.5, 0); got != 2.5 {
		t.Fatalf("zero completion ratio fallback = %g, want 2.5", got)
	}
	if got := costBasisOverrideEffectiveCompletionRatio(2.5, 3.5); got != 3.5 {
		t.Fatalf("explicit completion ratio = %g, want 3.5", got)
	}
}

func TestValidateCostBasisOverrideStateRejectsHistoricalCostRewrite(t *testing.T) {
	now := time.Now().UTC()
	current := &ChannelCostState{
		ChannelID: 1, Version: 3, CostUnitEpoch: 4,
		RawBalance: 10, ActualInventory: 2, FundingFactor: 0.2,
		RawTodayCost: 4, ActualTodayCost: 0.8, RawTotalCost: 20, ActualTotalCost: 4,
		BalanceSampledAt: &now, CostSampledAt: &now,
		Source: "confirmed_recharge", Status: "ready",
	}
	next := *current
	next.FundingFactor = 0.5
	next.ActualInventory = 5
	next.CostUnitEpoch = 5
	next.Source = CostBasisOverrideSource
	next.Version = 4
	next.Status = "ready"
	audit := &ChannelCostBasisOverride{
		ChannelID: 1, ChannelIdentityRevision: 1, ChannelNameSnapshot: "channel", ChannelTypeSnapshot: ChannelTypeSub2API,
		IdempotencyKey: "override", RequestFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Actor: "admin", Note: "reason",
		AppliedAt: now, RateSnapshotDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	candidateDigest, err := costBasisOverrideFundingCandidateDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	populateCostBasisOverrideAudit(audit, &Channel{Name: "channel", Type: ChannelTypeSub2API}, current, &next, 3, 1, 4, 0, candidateDigest, 0, audit.RateSnapshotDigest)
	if err := validateCostBasisOverrideAudit(audit); err != nil {
		t.Fatal(err)
	}
	next.ActualTotalCost = 5
	if err := validateCostBasisOverrideState(current, &next, 3, 4); err == nil {
		t.Fatal("historical actual cost rewrite was accepted")
	}
}

func TestValidateCostBasisOverrideStateAllowsSameFactorReassertion(t *testing.T) {
	now := time.Now().UTC()
	current := &ChannelCostState{
		ChannelID: 2, Version: 7, CostUnitEpoch: 3,
		RawBalance: 10, ActualInventory: 2, FundingFactor: 0.2,
		RawTodayCost: 4, ActualTodayCost: 0.8, RawTotalCost: 20, ActualTotalCost: 4,
		BalanceSampledAt: &now, CostSampledAt: &now,
		Source: "confirmed_recharge", Status: "needs_review", LastError: "provider evidence unavailable",
	}
	next := *current
	next.CostUnitEpoch++
	next.Version++
	next.Source = CostBasisOverrideSource
	next.Status = "ready"
	next.LastError = ""
	if err := validateCostBasisOverrideState(current, &next, 7, 3); err != nil {
		t.Fatalf("same-factor administrator reassertion was rejected: %v", err)
	}
}

func TestValidateCostBasisOverrideStateAllowsZeroCostBasis(t *testing.T) {
	now := time.Now().UTC()
	current := &ChannelCostState{
		ChannelID: 3, Version: 2, CostUnitEpoch: 1,
		RawBalance: -10, ActualInventory: -2, FundingFactor: 0.2,
		RawTodayCost: 1, ActualTodayCost: 0.2, RawTotalCost: 4, ActualTotalCost: 0.8,
		BalanceSampledAt: &now, CostSampledAt: &now,
		Source: "confirmed_recharge", Status: "ready",
	}
	next := *current
	next.CostUnitEpoch++
	next.Version++
	next.FundingFactor = 0
	next.ActualInventory = 0
	next.Source = CostBasisOverrideSource
	if err := validateCostBasisOverrideState(current, &next, 2, 1); err != nil {
		t.Fatalf("zero-cost administrator basis was rejected: %v", err)
	}
	audit := &ChannelCostBasisOverride{
		ChannelID: 3, IdempotencyKey: "zero-basis", RequestFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Actor: "admin", Note: "free quota", AppliedAt: now, RateSnapshotDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	candidateDigest, err := costBasisOverrideFundingCandidateDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	populateCostBasisOverrideAudit(audit, &Channel{Name: "channel", Type: ChannelTypeNewAPI}, current, &next, 2, 1, 1, 0, candidateDigest, 0, audit.RateSnapshotDigest)
	if err := validateCostBasisOverrideAudit(audit); err != nil {
		t.Fatalf("zero-cost audit was rejected: %v", err)
	}
}

func TestApplyCostBasisOverrideArchivesPendingFundingCandidates(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "manual-override-pending", Type: ChannelTypeNewAPI, SiteURL: "https://example.com", PasswordCipher: "credential"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	repo := NewCosting(db)
	current, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	candidate := &ChannelFundingCandidate{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "stuck-candidate", Status: FundingCandidatePending,
		CreditedAmount: 10, RawBalanceBefore: 10, RawBalanceDetected: 20, FundingFactorBefore: current.FundingFactor,
		DetectedAt: time.Now().UTC(),
	}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}
	next := *current
	next.FundingFactor = 0.25
	next.ActualInventory = next.RawBalance * next.FundingFactor
	next.CostUnitEpoch++
	next.Source = CostBasisOverrideSource
	next.Status = "ready"
	next.LastError = ""
	audit := &ChannelCostBasisOverride{
		ChannelID: channel.ID, IdempotencyKey: "archive-stuck-candidate",
		RequestFingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Actor:              "admin", Note: "manual recovery",
	}
	replayed, err := repo.ApplyCostBasisOverride(
		&next, audit, current.Version, channel.UpstreamAccountIdentityRevision, current.CostUnitEpoch,
	)
	if err != nil {
		t.Fatalf("apply cost basis override: %v", err)
	}
	if replayed || audit.SupersededCandidateCount != 1 || audit.SupersededCandidateDigest == "" {
		t.Fatalf("override audit = %+v, replayed=%v", audit, replayed)
	}
	var stored ChannelFundingCandidate
	if err := db.First(&stored, candidate.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Status != FundingCandidateSuperseded || stored.ResolvedEventID != nil {
		t.Fatalf("candidate after override = %+v", stored)
	}
}
