package costing

import (
	"math"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

type manualCostBasisObservationStore struct {
	Store
	state   *storage.ChannelCostState
	updated *storage.ChannelCostState
}

type manualCostBasisOverrideStore struct {
	Store
	state                    *storage.ChannelCostState
	applied                  *storage.ChannelCostState
	appliedAudit             *storage.ChannelCostBasisOverride
	expectedVersion          uint64
	expectedIdentityRevision uint64
	expectedCostUnitEpoch    uint64
}

func (s *manualCostBasisObservationStore) FindState(uint) (*storage.ChannelCostState, error) {
	next := *s.state
	return &next, nil
}

func (s *manualCostBasisObservationStore) ListFundingCandidates(uint, string, int, int) ([]storage.ChannelFundingCandidate, int64, error) {
	return nil, 0, nil
}

func (s *manualCostBasisObservationStore) UpdateState(next *storage.ChannelCostState, expectedVersion uint64) error {
	copy := *next
	copy.Version = expectedVersion + 1
	s.updated = &copy
	*next = copy
	return nil
}

func (s *manualCostBasisOverrideStore) FindState(uint) (*storage.ChannelCostState, error) {
	next := *s.state
	return &next, nil
}

func (s *manualCostBasisOverrideStore) FindCostBasisOverride(uint, string) (*storage.ChannelCostBasisOverride, error) {
	return nil, gorm.ErrRecordNotFound
}

func (s *manualCostBasisOverrideStore) ApplyCostBasisOverride(next *storage.ChannelCostState, audit *storage.ChannelCostBasisOverride, expectedVersion, expectedIdentityRevision, expectedCostUnitEpoch uint64) (bool, error) {
	stateCopy := *next
	auditCopy := *audit
	s.applied = &stateCopy
	s.appliedAudit = &auditCopy
	s.expectedVersion = expectedVersion
	s.expectedIdentityRevision = expectedIdentityRevision
	s.expectedCostUnitEpoch = expectedCostUnitEpoch
	return false, nil
}

func TestOverrideCostBasisAllowsZeroAndPreservesHistoricalActualCosts(t *testing.T) {
	sampledAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	state := &storage.ChannelCostState{
		ChannelID: 9, Version: 7, CostUnitEpoch: 3,
		RawBalance: -10, ActualInventory: -2, FundingFactor: 0.2,
		RawTodayCost: 4, ActualTodayCost: 0.8, RawTotalCost: 20, ActualTotalCost: 4,
		BalanceSampledAt: &sampledAt, CostSampledAt: &sampledAt,
		Source: "recharge_history", Status: StateStatusNeedsReview, LastError: "provider evidence unavailable",
	}
	store := &manualCostBasisOverrideStore{state: state}
	result, err := NewService(store).OverrideCostBasis(state.ChannelID, CostBasisOverrideCommand{
		ExpectedVersion: state.Version, ExpectedIdentityRevision: 5, ExpectedCostUnitEpoch: state.CostUnitEpoch,
		FundingFactor: 0, IdempotencyKey: "zero-cost-basis", Actor: "admin", Note: "free provider quota",
	})
	if err != nil {
		t.Fatalf("OverrideCostBasis: %v", err)
	}
	if result == nil || result.State == nil || result.Audit == nil || result.IdempotentReplay {
		t.Fatalf("override result = %+v", result)
	}
	next := result.State
	if next.FundingFactor != 0 || next.ActualInventory != 0 || next.RawBalance != state.RawBalance ||
		next.RawTodayCost != state.RawTodayCost || next.ActualTodayCost != state.ActualTodayCost ||
		next.RawTotalCost != state.RawTotalCost || next.ActualTotalCost != state.ActualTotalCost {
		t.Fatalf("override financial state = %+v", next)
	}
	if next.CostUnitEpoch != state.CostUnitEpoch+1 || next.Source != storage.CostBasisOverrideSource ||
		next.Status != StateStatusReady || next.LastError != "" {
		t.Fatalf("override state transition = %+v", next)
	}
	if store.applied == nil || store.appliedAudit == nil || store.expectedVersion != state.Version ||
		store.expectedIdentityRevision != 5 || store.expectedCostUnitEpoch != state.CostUnitEpoch {
		t.Fatalf("persistence call = state=%+v audit=%+v version=%d identity=%d epoch=%d",
			store.applied, store.appliedAudit, store.expectedVersion, store.expectedIdentityRevision, store.expectedCostUnitEpoch)
	}
}

func TestManualCostBasisKeepsProviderFactorEvidenceFromOverwritingFactor(t *testing.T) {
	providerFactor := 0.1
	now := time.Now().UTC()
	state := &storage.ChannelCostState{
		ChannelID: 7, Version: 4, FundingFactor: 0.25, Source: storage.CostBasisOverrideSource,
		Status: StateStatusReady, ObservedProviderFactor: &providerFactor,
		ObservedProviderFactorSource: "recharge_history", ObservedProviderFactorSampledAt: &now,
	}
	next, err := nextBalanceStateFrom(state, BalanceInput{
		ChannelID: 7, ExpectedVersion: 4, RawBalance: -8,
		ObservedFactor: providerFactor, Source: "recharge_history", SampledAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.FundingFactor != 0.25 {
		t.Fatalf("manual funding factor was overwritten by provider evidence: %g", next.FundingFactor)
	}
	if next.ActualInventory != -2 {
		t.Fatalf("signed balance projection = %g, want -2", next.ActualInventory)
	}
}

func TestManualCostBasisOverrideActiveAllowsZeroFactor(t *testing.T) {
	state := &storage.ChannelCostState{Source: storage.CostBasisOverrideSource, FundingFactor: 0}
	if !ManualCostBasisOverrideActive(state) {
		t.Fatal("zero-cost manual basis was not treated as active")
	}
}

func TestManualCostBasisAcceptsNewProviderEvidenceWithoutReview(t *testing.T) {
	oldProviderFactor := 0.1
	sampledAt := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	state := &storage.ChannelCostState{
		ChannelID: 8, Version: 6, CostUnitEpoch: 3,
		RawBalance: 10, ActualInventory: 2, FundingFactor: 0.2,
		RawTodayCost: 2, ActualTodayCost: 0.4, RawTotalCost: 10, ActualTotalCost: 2,
		BalanceSampledAt: &sampledAt, CostSampledAt: &sampledAt,
		Source: storage.CostBasisOverrideSource, Status: StateStatusReady,
		ObservedProviderFactor: &oldProviderFactor, ObservedProviderFactorSource: "recharge_history",
		ObservedProviderFactorSampledAt: &sampledAt,
	}
	store := &manualCostBasisObservationStore{state: state}
	nextSample := sampledAt.Add(time.Minute)
	next, candidate, err := NewService(store).RecordObservation(ObservationInput{
		Balance: BalanceInput{
			ChannelID: state.ChannelID, ExpectedVersion: state.Version,
			RawBalance: 8, ObservedFactor: 0.25, Source: "recharge_history", SampledAt: nextSample,
		},
		Costs: CostInput{
			ChannelID: state.ChannelID, ExpectedVersion: state.Version,
			RawTodayCost: 3, RawTotalCost: 11, SampledAt: nextSample,
		},
	})
	if err != nil {
		t.Fatalf("RecordObservation: %v", err)
	}
	if candidate != nil || store.updated == nil {
		t.Fatalf("candidate=%+v updated=%+v", candidate, store.updated)
	}
	if next.FundingFactor != 0.2 || next.Source != storage.CostBasisOverrideSource || next.Status != StateStatusReady || next.LastError != "" {
		t.Fatalf("manual basis changed or entered review: %+v", next)
	}
	if math.Abs(next.ActualInventory-1.6) > 1e-12 || math.Abs(next.ActualTodayCost-0.6) > 1e-12 || math.Abs(next.ActualTotalCost-2.2) > 1e-12 {
		t.Fatalf("manual projections = balance %g today %g total %g", next.ActualInventory, next.ActualTodayCost, next.ActualTotalCost)
	}
	if next.ObservedProviderFactor == nil || *next.ObservedProviderFactor != 0.25 || next.ObservedProviderFactorSource != "recharge_history" ||
		next.ObservedProviderFactorSampledAt == nil || !next.ObservedProviderFactorSampledAt.Equal(nextSample) {
		t.Fatalf("provider evidence was not refreshed: %+v", next)
	}
}
