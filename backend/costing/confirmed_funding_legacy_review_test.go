package costing

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type confirmedFundingLegacyReviewStore struct {
	Store
	state   *storage.ChannelCostState
	updated *storage.ChannelCostState
}

func (s *confirmedFundingLegacyReviewStore) FindState(uint) (*storage.ChannelCostState, error) {
	copy := *s.state
	return &copy, nil
}

func (s *confirmedFundingLegacyReviewStore) UpdateStateForChannelIdentity(next *storage.ChannelCostState, expectedVersion, expectedIdentityRevision uint64, clearBalanceFact bool) error {
	if expectedVersion != s.state.Version {
		return storage.ErrCostStateConflict
	}
	if expectedIdentityRevision != 3 || clearBalanceFact {
		return errors.New("unexpected identity recovery fence")
	}
	copy := *next
	copy.Version = expectedVersion + 1
	s.updated = &copy
	return nil
}

func confirmedFundingLegacyReviewFixture() storage.ChannelCostState {
	balancedAt := time.Date(2026, 8, 9, 2, 37, 51, 0, time.UTC)
	costedAt := time.Date(2026, 7, 23, 16, 30, 47, 0, time.UTC)
	return storage.ChannelCostState{
		ChannelID: 36, Version: 177, CostUnitEpoch: 1,
		RawBalance: 9.32040524, ActualInventory: 9.22085584, FundingFactor: 0.9893191983141713,
		RawTodayCost: 3.68194317, ActualTodayCost: 3.68194317,
		RawTotalCost: 7.179144135, ActualTotalCost: 7.179144135,
		BalanceSampledAt: &balancedAt, CostSampledAt: &costedAt,
		Source: "confirmed_redeem_code", Status: StateStatusNeedsReview,
		LastError: LegacyAdvertisedFactorEvidenceReviewReason,
	}
}

func TestCanRecoverConfirmedFundingLegacyReviewRequiresExactSafeState(t *testing.T) {
	observedFactor := 1.0
	sampledAt := time.Now().UTC()
	for _, test := range []struct {
		name   string
		mutate func(*storage.ChannelCostState)
		want   bool
	}{
		{name: "confirmed redeem code", want: true},
		{name: "confirmed balance increase", mutate: func(state *storage.ChannelCostState) { state.Source = "confirmed_balance_increase" }, want: true},
		{name: "advertised source", mutate: func(state *storage.ChannelCostState) { state.Source = "advertised" }},
		{name: "untrusted confirmed prefix", mutate: func(state *storage.ChannelCostState) { state.Source = "confirmed_operator_override" }},
		{name: "ready state", mutate: func(state *storage.ChannelCostState) { state.Status = StateStatusReady }},
		{name: "different review", mutate: func(state *storage.ChannelCostState) { state.LastError = ErrCostUnitChangeRequiresRebase.Error() }},
		{name: "approximate migration marker", mutate: func(state *storage.ChannelCostState) { state.LastError += " " }},
		{name: "observed factor", mutate: func(state *storage.ChannelCostState) { state.ObservedProviderFactor = &observedFactor }},
		{name: "observed source", mutate: func(state *storage.ChannelCostState) { state.ObservedProviderFactorSource = "advertised" }},
		{name: "observed time", mutate: func(state *storage.ChannelCostState) { state.ObservedProviderFactorSampledAt = &sampledAt }},
		{name: "invalid confirmed factor", mutate: func(state *storage.ChannelCostState) { state.FundingFactor = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := confirmedFundingLegacyReviewFixture()
			if test.mutate != nil {
				test.mutate(&state)
			}
			if got := CanRecoverConfirmedFundingLegacyReview(&state); got != test.want {
				t.Fatalf("CanRecoverConfirmedFundingLegacyReview(%+v) = %v, want %v", state, got, test.want)
			}
		})
	}
}

func TestRecoverConfirmedFundingLegacyReviewPreservesFinancialFacts(t *testing.T) {
	fixture := confirmedFundingLegacyReviewFixture()
	store := &confirmedFundingLegacyReviewStore{state: &fixture}
	state, err := NewService(store).RecoverConfirmedFundingLegacyReview(fixture.ChannelID, fixture.Version, 3)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != StateStatusReady || state.LastError != "" || state.Version != fixture.Version+1 {
		t.Fatalf("recovered state = %+v", state)
	}
	want := fixture
	want.Status = StateStatusReady
	want.LastError = ""
	want.Version++
	if !reflect.DeepEqual(state, &want) || !reflect.DeepEqual(store.updated, &want) {
		t.Fatalf("recovery changed financial facts:\nstate=%+v\nupdated=%+v\nwant=%+v", state, store.updated, want)
	}
	if !ConfirmedFundingBasisWithoutProviderEvidence(state) {
		t.Fatalf("recovered confirmed basis was not recognized: %+v", state)
	}
}

func TestRecoverConfirmedFundingLegacyReviewRejectsOtherReview(t *testing.T) {
	fixture := confirmedFundingLegacyReviewFixture()
	fixture.LastError = ErrCostUnitChangeRequiresRebase.Error()
	store := &confirmedFundingLegacyReviewStore{state: &fixture}
	state, err := NewService(store).RecoverConfirmedFundingLegacyReview(fixture.ChannelID, fixture.Version, 3)
	if state != nil || !errors.Is(err, ErrCostUnitEvidenceUnavailable) || store.updated != nil {
		t.Fatalf("unrelated review recovery = state=%+v err=%v updated=%+v", state, err, store.updated)
	}
}
