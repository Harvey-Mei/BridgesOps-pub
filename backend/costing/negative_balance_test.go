package costing

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type negativeBalanceStore struct {
	Store
	state                  *storage.ChannelCostState
	updated                *storage.ChannelCostState
	fundingCandidateWrites int
}

func (s *negativeBalanceStore) FindState(uint) (*storage.ChannelCostState, error) {
	copy := *s.state
	return &copy, nil
}

func (s *negativeBalanceStore) ListFundingCandidates(uint, string, int, int) ([]storage.ChannelFundingCandidate, int64, error) {
	return nil, 0, nil
}

func (s *negativeBalanceStore) UpdateState(next *storage.ChannelCostState, expectedVersion uint64) error {
	copy := *next
	copy.Version = expectedVersion + 1
	s.updated = &copy
	*next = copy
	return nil
}

func (s *negativeBalanceStore) UpdateBalanceState(next *storage.ChannelCostState, expectedVersion uint64) error {
	copy := *next
	copy.Version = expectedVersion + 1
	s.updated = &copy
	*next = copy
	return nil
}

func (s *negativeBalanceStore) UpdateStateWithFundingCandidate(next *storage.ChannelCostState, _ *storage.ChannelFundingCandidate, expectedVersion uint64) error {
	s.fundingCandidateWrites++
	return s.UpdateState(next, expectedVersion)
}

func TestRecordObservationPersistsNegativeBalanceWithoutAutomaticFundingCandidate(t *testing.T) {
	previousAt := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	store := &negativeBalanceStore{state: &storage.ChannelCostState{
		ChannelID: 7, Version: 4, CostUnitEpoch: 1,
		RawBalance: -10, ActualInventory: -5, FundingFactor: 0.5,
		BalanceSampledAt: &previousAt, CostSampledAt: &previousAt,
		Source: "recharge_history", Status: StateStatusReady,
	}}
	sampledAt := previousAt.Add(time.Minute)
	state, candidate, err := NewService(store).RecordObservation(ObservationInput{
		Balance: BalanceInput{
			ChannelID: 7, ExpectedVersion: 4, RawBalance: -5,
			SampledAt: sampledAt, Source: "default",
		},
		Costs: CostInput{
			ChannelID: 7, ExpectedVersion: 4,
			RawTodayCost: 2, RawTotalCost: 3, SampledAt: sampledAt,
		},
		Funding: &FundingCandidateInput{
			ChannelID: 7, Source: "balance_increase", ExternalID: "negative-recovery",
			CreditedAmount: 5, RawBalanceBefore: -10, RawBalanceDetected: -5,
			FundingFactorBefore: 0.5, DetectedAt: sampledAt,
		},
	})
	if err != nil {
		t.Fatalf("RecordObservation: %v", err)
	}
	if candidate != nil || store.fundingCandidateWrites != 0 {
		t.Fatalf("negative balance recovery created funding candidate: candidate=%+v writes=%d", candidate, store.fundingCandidateWrites)
	}
	if state.RawBalance != -5 || state.ActualInventory != -2.5 || state.RawTodayCost != 2 || state.ActualTodayCost != 1 ||
		state.RawTotalCost != 3 || state.ActualTotalCost != 1.5 || store.updated == nil {
		t.Fatalf("signed observation was not persisted with non-negative costs: %+v", state)
	}
}

func TestRecordBalanceWithFundingSkipsCandidateAcrossNegativeBoundary(t *testing.T) {
	store := &negativeBalanceStore{state: &storage.ChannelCostState{
		ChannelID: 9, Version: 2, CostUnitEpoch: 1,
		RawBalance: -2, ActualInventory: -1, FundingFactor: 0.5,
		Source: "recharge_history", Status: StateStatusReady,
	}}
	sampledAt := time.Date(2026, 8, 9, 11, 0, 0, 0, time.UTC)
	state, candidate, err := NewService(store).RecordBalanceWithFunding(BalanceInput{
		ChannelID: 9, ExpectedVersion: 2, RawBalance: 2, SampledAt: sampledAt, Source: "default",
	}, FundingCandidateInput{
		ChannelID: 9, Source: "redeem_code", ExternalID: "negative-crossing",
		CreditedAmount: 4, RawBalanceBefore: -2, RawBalanceDetected: 2,
		FundingFactorBefore: 0.5, DetectedAt: sampledAt,
	})
	if err != nil {
		t.Fatalf("RecordBalanceWithFunding: %v", err)
	}
	if candidate != nil || store.fundingCandidateWrites != 0 {
		t.Fatalf("negative-boundary balance created funding candidate: candidate=%+v writes=%d", candidate, store.fundingCandidateWrites)
	}
	if state.RawBalance != 2 || state.ActualInventory != 1 || store.updated == nil {
		t.Fatalf("balance crossing zero was not recorded: %+v", state)
	}
}
