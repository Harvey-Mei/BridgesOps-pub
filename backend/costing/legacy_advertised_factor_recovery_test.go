package costing

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type legacyAdvertisedRecoveryStore struct {
	Store
	state   *storage.ChannelCostState
	updated *storage.ChannelCostState
}

func (s *legacyAdvertisedRecoveryStore) FindState(uint) (*storage.ChannelCostState, error) {
	copy := *s.state
	return &copy, nil
}

func (s *legacyAdvertisedRecoveryStore) ListFundingCandidates(uint, string, int, int) ([]storage.ChannelFundingCandidate, int64, error) {
	return nil, 0, nil
}

func (s *legacyAdvertisedRecoveryStore) UpdateState(next *storage.ChannelCostState, expectedVersion uint64) error {
	copy := *next
	copy.Version = expectedVersion + 1
	s.updated = &copy
	*next = copy
	return nil
}

func TestRecordObservationRecoversOnlyMatchingLegacyAdvertisedEvidence(t *testing.T) {
	sampledBefore := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	base := storage.ChannelCostState{
		ChannelID: 7, RawBalance: 100, ActualInventory: 10, FundingFactor: 0.1,
		RawTodayCost: 2, ActualTodayCost: 0.2, RawTotalCost: 10, ActualTotalCost: 1,
		BalanceSampledAt: &sampledBefore, CostSampledAt: &sampledBefore,
		CostUnitEpoch: 1, Version: 4, Source: "advertised", Status: StateStatusNeedsReview,
		LastError: LegacyAdvertisedFactorEvidenceReviewReason,
	}
	now := sampledBefore.Add(time.Hour)

	for _, test := range []struct {
		name           string
		observedFactor float64
		lastError      string
		wantReady      bool
	}{
		{name: "matching current evidence", observedFactor: 0.1, wantReady: true},
		{name: "changed current evidence", observedFactor: 0.2},
		{name: "unrelated review remains blocked", observedFactor: 0.1, lastError: "operator evidence already required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := base
			if test.lastError != "" {
				fixture.LastError = test.lastError
			}
			store := &legacyAdvertisedRecoveryStore{state: &fixture}
			state, _, err := NewService(store).RecordObservation(ObservationInput{
				Balance: BalanceInput{
					ChannelID: 7, ExpectedVersion: 4, RawBalance: 90,
					ObservedFactor: test.observedFactor, Source: "advertised", SampledAt: now,
				},
				Costs: CostInput{
					ChannelID: 7, ExpectedVersion: 4, RawTodayCost: 3, RawTotalCost: 11, SampledAt: now,
				},
			})
			if test.wantReady {
				if err != nil {
					t.Fatalf("RecordObservation: %v", err)
				}
				if state.Status != StateStatusReady || state.LastError != "" || state.ObservedProviderFactor == nil ||
					*state.ObservedProviderFactor != test.observedFactor || state.ObservedProviderFactorSource != "advertised" ||
					state.ObservedProviderFactorSampledAt == nil || store.updated == nil {
					t.Fatalf("matching legacy observation was not recovered: %+v", state)
				}
				return
			}
			if err != nil {
				t.Fatalf("changed observation should remain reviewable without a storage failure: %v", err)
			}
			if state.Status != StateStatusNeedsReview || state.LastError != fixture.LastError || state.FundingFactor != base.FundingFactor ||
				state.ObservedProviderFactor != nil || state.ObservedProviderFactorSource != "" || state.ObservedProviderFactorSampledAt != nil {
				t.Fatalf("changed legacy observation rewrote or unlocked the basis: %+v", state)
			}

			store.state = state
			secondSampledAt := now.Add(time.Minute)
			state, _, err = NewService(store).RecordObservation(ObservationInput{
				Balance: BalanceInput{
					ChannelID: 7, ExpectedVersion: state.Version, RawBalance: 89,
					ObservedFactor: test.observedFactor, Source: "advertised", SampledAt: secondSampledAt,
				},
				Costs: CostInput{
					ChannelID: 7, ExpectedVersion: state.Version, RawTodayCost: 3.1, RawTotalCost: 11.1, SampledAt: secondSampledAt,
				},
			})
			if err != nil {
				t.Fatalf("second blocked observation: %v", err)
			}
			if state.Status != StateStatusNeedsReview || state.LastError != fixture.LastError ||
				state.ObservedProviderFactor != nil || state.ObservedProviderFactorSource != "" || state.ObservedProviderFactorSampledAt != nil {
				t.Fatalf("repeated observation unlocked or polluted blocked review: %+v", state)
			}
		})
	}
}

func TestLegacyAdvertisedFactorEvidenceMissingRequiresExactMigrationState(t *testing.T) {
	sampledAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	factor := 0.1
	valid := storage.ChannelCostState{
		Source: "advertised", Status: StateStatusNeedsReview,
		LastError: LegacyAdvertisedFactorEvidenceReviewReason,
	}
	for _, test := range []struct {
		name   string
		mutate func(*storage.ChannelCostState)
		want   bool
	}{
		{name: "exact migration state", want: true},
		{name: "ready status", mutate: func(state *storage.ChannelCostState) { state.Status = StateStatusReady }},
		{name: "unrelated review", mutate: func(state *storage.ChannelCostState) { state.LastError = "operator evidence already required" }},
		{name: "marker with whitespace", mutate: func(state *storage.ChannelCostState) { state.LastError += " " }},
		{name: "other source", mutate: func(state *storage.ChannelCostState) { state.Source = "manual" }},
		{name: "observed factor", mutate: func(state *storage.ChannelCostState) { state.ObservedProviderFactor = &factor }},
		{name: "observed source", mutate: func(state *storage.ChannelCostState) { state.ObservedProviderFactorSource = "advertised" }},
		{name: "whitespace observed source", mutate: func(state *storage.ChannelCostState) { state.ObservedProviderFactorSource = " " }},
		{name: "observed time", mutate: func(state *storage.ChannelCostState) { state.ObservedProviderFactorSampledAt = &sampledAt }},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := valid
			if test.mutate != nil {
				test.mutate(&state)
			}
			if got := LegacyAdvertisedFactorEvidenceMissing(&state); got != test.want {
				t.Fatalf("LegacyAdvertisedFactorEvidenceMissing(%+v) = %v, want %v", state, got, test.want)
			}
		})
	}
	if LegacyAdvertisedFactorEvidenceMissing(nil) {
		t.Fatal("nil cost state was treated as recoverable legacy evidence")
	}
}

func TestSingleBalanceObservationCannotRecoverLegacyAdvertisedEvidence(t *testing.T) {
	state := &storage.ChannelCostState{
		ChannelID: 8, FundingFactor: 0.1, CostUnitEpoch: 1, Version: 3,
		Source: "advertised", Status: StateStatusNeedsReview,
	}
	next, err := nextBalanceStateFrom(state, BalanceInput{
		ChannelID: 8, ExpectedVersion: 3, RawBalance: 10,
		ObservedFactor: 0.1, Source: "advertised", SampledAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.Status != StateStatusNeedsReview || next.ObservedProviderFactor != nil {
		t.Fatalf("single balance observation recovered legacy evidence: %+v", next)
	}
}

func TestProviderFactorReviewResolutionRequiresCanonicalSystemMarker(t *testing.T) {
	sampledAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	observedFactor := 0.1
	base := storage.ChannelCostState{
		ChannelID: 9, FundingFactor: 0.2, CostUnitEpoch: 1, Version: 3,
		Source: "confirmed_redeem_code", Status: StateStatusNeedsReview,
		ObservedProviderFactor: &observedFactor, ObservedProviderFactorSource: "advertised",
		ObservedProviderFactorSampledAt: &sampledAt,
		LastError: fmt.Sprintf("%s: persisted factor %.12g, observed factor %.12g",
			ErrCostUnitChangeRequiresRebase, observedFactor, 1.0),
	}
	for _, test := range []struct {
		name      string
		mutate    func(*storage.ChannelCostState)
		wantReady bool
	}{
		{name: "canonical factor-change review", wantReady: true},
		{name: "operator review", mutate: func(state *storage.ChannelCostState) { state.LastError = "operator evidence already required" }},
		{name: "approximate marker", mutate: func(state *storage.ChannelCostState) { state.LastError += " " }},
		{name: "marker without a change", mutate: func(state *storage.ChannelCostState) {
			state.LastError = fmt.Sprintf("%s: persisted factor %.12g, observed factor %.12g",
				ErrCostUnitChangeRequiresRebase, observedFactor, observedFactor)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := base
			if test.mutate != nil {
				test.mutate(&state)
			}
			next, err := nextBalanceStateFrom(&state, BalanceInput{
				ChannelID: state.ChannelID, ExpectedVersion: state.Version, RawBalance: 10,
				ObservedFactor: observedFactor, Source: "advertised", SampledAt: sampledAt.Add(time.Minute),
			})
			if err != nil {
				t.Fatal(err)
			}
			if test.wantReady {
				if next.Status != StateStatusReady || next.LastError != "" {
					t.Fatalf("canonical provider reversion remained blocked: %+v", next)
				}
				return
			}
			if next.Status != StateStatusNeedsReview || next.LastError != state.LastError {
				t.Fatalf("untrusted review marker was cleared: %+v", next)
			}
		})
	}
}

func TestUnrelatedReviewCannotBeReownedByProviderFactorChange(t *testing.T) {
	sampledAt := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	observedFactor := 0.1
	fixture := storage.ChannelCostState{
		ChannelID: 10, RawBalance: 10, ActualInventory: 2, FundingFactor: 0.2,
		CostUnitEpoch: 1, Version: 4, Source: "confirmed_redeem_code", Status: StateStatusNeedsReview,
		LastError:              "operator evidence already required",
		ObservedProviderFactor: &observedFactor, ObservedProviderFactorSource: "advertised",
		ObservedProviderFactorSampledAt: &sampledAt,
	}
	store := &legacyAdvertisedRecoveryStore{state: &fixture}
	service := NewService(store)
	changed := ObservationInput{
		Balance: BalanceInput{
			ChannelID: fixture.ChannelID, ExpectedVersion: fixture.Version, RawBalance: 9,
			ObservedFactor: 1, Source: "advertised", SampledAt: sampledAt.Add(time.Minute),
		},
		Costs: CostInput{
			ChannelID: fixture.ChannelID, ExpectedVersion: fixture.Version,
			RawTodayCost: 1, RawTotalCost: 2, SampledAt: sampledAt.Add(time.Minute),
		},
	}
	if state, candidate, err := service.RecordObservation(changed); !errors.Is(err, ErrCostUnitChangeRequiresRebase) || state != nil || candidate != nil {
		t.Fatalf("factor change over unrelated review = state=%+v candidate=%+v err=%v", state, candidate, err)
	}
	if store.updated != nil || fixture.LastError != "operator evidence already required" {
		t.Fatalf("factor change re-owned unrelated review: state=%+v updated=%+v", fixture, store.updated)
	}

	reverted := changed
	reverted.Balance.ObservedFactor = observedFactor
	state, candidate, err := service.RecordObservation(reverted)
	if err != nil || candidate != nil {
		t.Fatalf("provider reversion under unrelated review = state=%+v candidate=%+v err=%v", state, candidate, err)
	}
	if state.Status != StateStatusNeedsReview || state.LastError != fixture.LastError {
		t.Fatalf("provider reversion cleared unrelated review: %+v", state)
	}
}
