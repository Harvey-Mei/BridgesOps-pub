package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestAccountConcurrencyTrackerSharesBoundedSlotsAcrossOneAccount(t *testing.T) {
	tracker := NewAccountConcurrencyTracker()
	limit := 2
	policy := DefaultAccountConcurrencyPolicy()
	first, firstDecision := tracker.TryAcquire(10, &limit, AccountConcurrencySmartData, policy)
	second, secondDecision := tracker.TryAcquire(10, &limit, AccountConcurrencySmartData, policy)
	third, thirdDecision := tracker.TryAcquire(10, &limit, AccountConcurrencySmartData, policy)
	other, otherDecision := tracker.TryAcquire(11, &limit, AccountConcurrencySmartData, policy)
	if !firstDecision.Admitted || !secondDecision.Admitted || third != nil || thirdDecision.Admitted || thirdDecision.Reason != "account_concurrency_limit_reached" {
		t.Fatalf("same-account decisions: first=%#v second=%#v third=%#v", firstDecision, secondDecision, thirdDecision)
	}
	if !otherDecision.Admitted || other == nil {
		t.Fatalf("different account was not isolated: %#v", otherDecision)
	}
	if got := tracker.Snapshot(10); got.Total != 2 || got.SmartData != 2 || got.Scope != AccountConcurrencyTrackerScopeInstance {
		t.Fatalf("account 10 snapshot = %#v", got)
	}
	first.Release()
	second.Release()
	other.Release()
}

func TestAccountConcurrencyFixedRouteBypassesAdmissionButRemainsObserved(t *testing.T) {
	tracker := NewAccountConcurrencyTracker()
	limit := 1
	policy := DefaultAccountConcurrencyPolicy()
	smart, smartDecision := tracker.TryAcquire(20, &limit, AccountConcurrencySmartData, policy)
	fixed, fixedDecision := tracker.TryAcquire(20, &limit, AccountConcurrencyFixedData, policy)
	if !smartDecision.Admitted || !fixedDecision.Admitted || fixed == nil {
		t.Fatalf("fixed bypass decisions: smart=%#v fixed=%#v", smartDecision, fixedDecision)
	}
	if got := tracker.Snapshot(20); got.Total != 2 || got.SmartData != 1 || got.FixedData != 1 {
		t.Fatalf("fixed bypass snapshot = %#v", got)
	}
	smart.Release()
	blocked, blockedDecision := tracker.TryAcquire(20, &limit, AccountConcurrencySmartData, policy)
	if blocked != nil || blockedDecision.Admitted || blockedDecision.Reason != "account_concurrency_limit_reached" {
		t.Fatalf("fixed observation did not keep bounded smart admission conservative: %#v", blockedDecision)
	}
	fixed.Release()
	admitted, admittedDecision := tracker.TryAcquire(20, &limit, AccountConcurrencySmartData, policy)
	if admitted == nil || !admittedDecision.Admitted {
		t.Fatalf("smart admission did not recover after fixed completion: %#v", admittedDecision)
	}
	admitted.Release()
}

func TestAccountConcurrencyProbeReserveAndUnknownUnlimitedSemantics(t *testing.T) {
	tracker := NewAccountConcurrencyTracker()
	policy := DefaultAccountConcurrencyPolicy()
	limitOne := 1
	probe, decision := tracker.TryAcquire(30, &limitOne, AccountConcurrencyLatencyProbe, policy)
	if probe != nil || decision.Admitted || decision.Reason != "reserved_for_data_plane" {
		t.Fatalf("limit=1 probe decision = %#v", decision)
	}

	limitTwo := 2
	probe, decision = tracker.TryAcquire(31, &limitTwo, AccountConcurrencyModelCheck, policy)
	if probe == nil || !decision.Admitted {
		t.Fatalf("limit=2 first low-priority decision = %#v", decision)
	}
	secondProbe, secondProbeDecision := tracker.TryAcquire(31, &limitTwo, AccountConcurrencyAPITest, policy)
	if secondProbe != nil || secondProbeDecision.Reason != "low_priority_budget_exhausted" {
		t.Fatalf("shared low-priority budget decision = %#v", secondProbeDecision)
	}
	smart, smartDecision := tracker.TryAcquire(31, &limitTwo, AccountConcurrencySmartData, policy)
	if smart == nil || !smartDecision.Admitted {
		t.Fatalf("reserved data-plane slot was not available: %#v", smartDecision)
	}
	probe.Release()
	smart.Release()

	unknownOne, unknownDecision := tracker.TryAcquire(32, nil, AccountConcurrencySmartData, policy)
	unknownTwo, unknownSecondDecision := tracker.TryAcquire(32, nil, AccountConcurrencySmartData, policy)
	unlimited := 0
	unlimitedOne, unlimitedDecision := tracker.TryAcquire(33, &unlimited, AccountConcurrencySmartData, policy)
	unlimitedTwo, unlimitedSecondDecision := tracker.TryAcquire(33, &unlimited, AccountConcurrencySmartData, policy)
	if unknownOne == nil || unknownTwo == nil || !unknownDecision.Admitted || !unknownSecondDecision.Admitted || unknownDecision.LimitState != "unknown" {
		t.Fatalf("unknown limit created a fake gate: first=%#v second=%#v", unknownDecision, unknownSecondDecision)
	}
	if unlimitedOne == nil || unlimitedTwo == nil || !unlimitedDecision.Admitted || !unlimitedSecondDecision.Admitted || unlimitedDecision.LimitState != "unlimited" {
		t.Fatalf("explicit unlimited created a fake gate: first=%#v second=%#v", unlimitedDecision, unlimitedSecondDecision)
	}
	unknownOne.Release()
	unknownTwo.Release()
	unlimitedOne.Release()
	unlimitedTwo.Release()

	unbound, unboundDecision := tracker.TryAcquire(0, nil, AccountConcurrencyFixedData, policy)
	if unbound == nil || !unboundDecision.Admitted || unboundDecision.Tracked || unboundDecision.Reason != "account_identity_unavailable" {
		t.Fatalf("unbound fixed route was blocked: %#v", unboundDecision)
	}
	unbound.Release()
}

func TestAccountConcurrencyManagementDoesNotConsumeInferenceSlots(t *testing.T) {
	tracker := NewAccountConcurrencyTracker()
	management, decision := tracker.TryAcquire(0, nil, AccountConcurrencyManagement, DefaultAccountConcurrencyPolicy())
	if management == nil || !decision.Admitted || decision.Tracked || decision.Reason != "management_outside_inference_slots" {
		t.Fatalf("management decision = %#v", decision)
	}
	management.Release()
	if got := tracker.Snapshot(0); got.Total != 0 {
		t.Fatalf("management work changed inference snapshot: %#v", got)
	}
}

func TestServiceCandidateAccountConcurrencyLazyInitSharesKnownAccountAndBypassesFixed(t *testing.T) {
	service := &Service{}
	limit := 1
	observedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	first := accountConcurrencyCandidate(401, 1, "Primary")
	second := accountConcurrencyCandidate(402, 1, "secondary")
	first.UpstreamAccountConcurrency = explicitAccountConcurrencyTestSnapshot(9001, 1, limit, observedAt)
	second.UpstreamAccountConcurrency = explicitAccountConcurrencyTestSnapshot(9001, 1, limit, observedAt)

	active, activeDecision := service.TryAcquireCandidateAccountConcurrency(first, AccountConcurrencySmartData)
	blocked, blockedDecision := service.TryAcquireCandidateAccountConcurrency(second, AccountConcurrencySmartData)
	fixed, fixedDecision := service.TryAcquireCandidateAccountConcurrency(second, AccountConcurrencyFixedData)
	if active == nil || !activeDecision.Admitted {
		t.Fatalf("first known-account decision = %#v", activeDecision)
	}
	if blocked != nil || blockedDecision.Admitted || blockedDecision.Reason != "account_concurrency_limit_reached" {
		t.Fatalf("second channel did not share known account limit: %#v", blockedDecision)
	}
	if fixed == nil || !fixedDecision.Admitted || !fixedDecision.Tracked {
		t.Fatalf("fixed candidate did not bypass known limit: %#v", fixedDecision)
	}
	active.Release()
	fixed.Release()
	if service.accountConcurrencyGovernor == nil {
		t.Fatal("zero-value service did not lazily initialize governor")
	}
}

func TestExplicitAccountConcurrencyPressureRequiresConcurrencyEvidence(t *testing.T) {
	for _, test := range []struct {
		status  int
		message string
		want    bool
	}{
		{status: http.StatusTooManyRequests, message: `{"error":"daily rate limit exceeded"}`},
		{status: http.StatusTooManyRequests, message: `{"error":"concurrency limit exceeded"}`, want: true},
		{status: http.StatusTooManyRequests, message: `{"error":"selected model is at capacity"}`},
		{status: http.StatusOK, message: `{"error":"selected model is at capacity"}`},
		{status: http.StatusServiceUnavailable, message: `{"error":"too many concurrent requests"}`, want: true},
		{status: http.StatusTooManyRequests, message: ""},
	} {
		if got := IsExplicitAccountConcurrencyPressure(test.status, test.message); got != test.want {
			t.Fatalf("pressure(%d, %q) = %t, want %t", test.status, test.message, got, test.want)
		}
	}
}

func TestAccountConcurrencyLeaseReleasesOnEveryHTTPBodyTerminalPath(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T, *AccountConcurrencyLease)
	}{
		{
			name: "body eof",
			run: func(t *testing.T, lease *AccountConcurrencyLease) {
				response := &http.Response{Body: io.NopCloser(strings.NewReader("complete"))}
				lease.BindHTTPResponse(response, nil)
				if _, err := io.ReadAll(response.Body); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "body close",
			run: func(t *testing.T, lease *AccountConcurrencyLease) {
				response := &http.Response{Body: io.NopCloser(strings.NewReader("not-read"))}
				lease.BindHTTPResponse(response, nil)
				if err := response.Body.Close(); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "transport error",
			run: func(_ *testing.T, lease *AccountConcurrencyLease) {
				lease.BindHTTPResponse(nil, errors.New("dial failed"))
			},
		},
		{
			name: "canceled read",
			run: func(t *testing.T, lease *AccountConcurrencyLease) {
				response := &http.Response{Body: &terminalReadCloser{err: context.Canceled}}
				lease.BindHTTPResponse(response, nil)
				buffer := make([]byte, 1)
				if _, err := response.Body.Read(buffer); !errors.Is(err, context.Canceled) {
					t.Fatalf("read error = %v, want context canceled", err)
				}
			},
		},
		{
			name: "response body missing",
			run: func(_ *testing.T, lease *AccountConcurrencyLease) {
				lease.BindHTTPResponse(&http.Response{}, nil)
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := NewAccountConcurrencyTracker()
			limit := 1
			lease, decision := tracker.TryAcquire(uint64(100+index), &limit, AccountConcurrencySmartData, DefaultAccountConcurrencyPolicy())
			if lease == nil || !decision.Admitted {
				t.Fatalf("acquire decision = %#v", decision)
			}
			test.run(t, lease)
			lease.Release()
			lease.Release()
			if got := tracker.Snapshot(uint64(100 + index)); got.Total != 0 || got.SmartData != 0 {
				t.Fatalf("terminal path leaked or double-released lease: %#v", got)
			}
		})
	}
}

func TestAccountConcurrencyLeaseReleasesWhenBoundContextIsCanceled(t *testing.T) {
	tracker := NewAccountConcurrencyTracker()
	limit := 1
	lease, decision := tracker.TryAcquire(210, &limit, AccountConcurrencySmartData, DefaultAccountConcurrencyPolicy())
	if lease == nil || !decision.Admitted {
		t.Fatalf("acquire decision = %#v", decision)
	}
	ctx, cancel := context.WithCancel(context.Background())
	lease.BindContext(ctx)
	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if snapshot := tracker.Snapshot(210); snapshot.Total == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("context cancellation leaked account slot: %#v", tracker.Snapshot(210))
}

func TestUnknownAccountConcurrencyPressureLearnsSoftLimitWithoutBlockingFixed(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	governor := NewAccountConcurrencyGovernor()
	governor.now = func() time.Time { return now }
	route := accountConcurrencyTestRoute(1)
	policy := DefaultAccountConcurrencyPolicy()

	first, firstDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	second, secondDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	third, thirdDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if first == nil || second == nil || third == nil || !firstDecision.Admitted || !secondDecision.Admitted || !thirdDecision.Admitted {
		t.Fatalf("unknown route created a fake initial limit: first=%#v second=%#v third=%#v", firstDecision, secondDecision, thirdDecision)
	}
	third.ObservePressure() // 429 at three in flight learns a local two-slot ceiling.
	first.Release()
	second.Release()
	third.Release()

	blocked, blockedDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if blocked != nil || blockedDecision.Reason != "unknown_concurrency_pressure_backoff" || blockedDecision.RetryAfter != policy.UnknownPressureBackoff {
		t.Fatalf("pressure backoff decision = %#v", blockedDecision)
	}
	fixed, fixedDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencyFixedData, policy)
	if fixed == nil || !fixedDecision.Admitted || !fixedDecision.Tracked {
		t.Fatalf("fixed route was blocked by learned pressure: %#v", fixedDecision)
	}
	fixed.ObserveUsefulSuccess()
	fixed.Release()

	blocked, blockedDecision = governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if blocked != nil || blockedDecision.Reason != "unknown_concurrency_pressure_backoff" {
		t.Fatalf("unsaturated fixed success cleared capacity backoff: %#v", blockedDecision)
	}
	now = now.Add(policy.UnknownPressureBackoff)
	first, firstDecision = governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	second, secondDecision = governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	third, thirdDecision = governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if first == nil || second == nil || third != nil || !firstDecision.Admitted || !secondDecision.Admitted || thirdDecision.Reason != "learned_account_concurrency_limit_reached" {
		t.Fatalf("learned limit decisions: first=%#v second=%#v third=%#v", firstDecision, secondDecision, thirdDecision)
	}
	first.Release()
	second.Release()
}

func TestWaitingProbeCanObserveBusyUnknownRouteWithinBudgets(t *testing.T) {
	governor := NewAccountConcurrencyGovernor()
	route := accountConcurrencyTestRoute(31)
	policy := DefaultAccountConcurrencyPolicy()
	main, decision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if !decision.Admitted {
		t.Fatal(decision)
	}
	defer main.Release()
	background, decision := governor.TryAcquire(route, 0, nil, AccountConcurrencyLatencyProbe, policy)
	if background != nil || decision.Reason != "data_plane_in_flight" {
		t.Fatal("ordinary probes must still defer", decision)
	}
	waiting, decision := governor.TryAcquire(route, 0, nil, AccountConcurrencyWaitingProbe, policy)
	if waiting == nil || !decision.Admitted {
		t.Fatal("waiting health probe was always blocked by its own main request", decision)
	}
	defer waiting.Release()
	limit := 1
	blocked, decision := governor.TryAcquire(accountConcurrencyTestRoute(32), 91, &limit, AccountConcurrencyWaitingProbe, policy)
	if blocked != nil || decision.Admitted {
		t.Fatal("waiting probe bypassed reserved account capacity", decision)
	}
}

func TestKnownAccountConcurrencyCombinesHardLimitWithRoutePressureOverlay(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	governor := NewAccountConcurrencyGovernor()
	governor.now = func() time.Time { return now }
	policy := DefaultAccountConcurrencyPolicy()
	route := accountConcurrencyTestRoute(21)
	const accountID = uint64(9021)
	limit := 4

	pressure, decision := governor.TryAcquire(route, accountID, &limit, AccountConcurrencySmartData, policy)
	if pressure == nil || !decision.Admitted || !decision.Tracked || decision.LimitState != "bounded" || decision.Limit == nil || *decision.Limit != limit {
		t.Fatalf("known pressure acquisition = %#v", decision)
	}
	pressure.ObservePressure()
	pressure.Release()

	blocked, blockedDecision := governor.TryAcquire(route, accountID, &limit, AccountConcurrencySmartData, policy)
	if blocked != nil || blockedDecision.Admitted || blockedDecision.Reason != "unknown_concurrency_pressure_backoff" || blockedDecision.LimitState != "bounded" {
		t.Fatalf("known smart pressure decision = %#v", blockedDecision)
	}
	probe, probeDecision := governor.TryAcquire(route, accountID, &limit, AccountConcurrencyLatencyProbe, policy)
	if probe != nil || probeDecision.Admitted {
		t.Fatalf("known probe bypassed route pressure: %#v", probeDecision)
	}
	if snapshot := governor.tracker.Snapshot(accountID); snapshot.Total != 0 {
		t.Fatalf("rejected overlay leaked the hard-limit lease: %#v", snapshot)
	}

	for attempt := 0; attempt < policy.UnknownIncreaseSuccesses; attempt++ {
		fixed, fixedDecision := governor.TryAcquire(route, accountID, &limit, AccountConcurrencyFixedData, policy)
		if fixed == nil || !fixedDecision.Admitted || !fixedDecision.Tracked {
			t.Fatalf("fixed recovery %d = %#v", attempt, fixedDecision)
		}
		if attempt == 0 {
			if hard := governor.tracker.Snapshot(accountID); hard.Total != 1 || hard.FixedData != 1 {
				t.Fatalf("fixed route was not account-observed: %#v", hard)
			}
			if overlay := governor.UnknownSnapshot(route); overlay.Current != 1 || overlay.FixedData != 1 {
				t.Fatalf("fixed route was not overlay-observed: %#v", overlay)
			}
		}
		fixed.ObserveUsefulSuccess()
		fixed.Release()
	}
	recovered := governor.UnknownSnapshot(route)
	if recovered.LearnedLimit == nil || *recovered.LearnedLimit != 2 || !recovered.BackoffUntil.IsZero() {
		t.Fatalf("saturated known-route success did not recover overlay: %#v", recovered)
	}

	first, firstDecision := governor.TryAcquire(route, accountID, &limit, AccountConcurrencySmartData, policy)
	second, secondDecision := governor.TryAcquire(route, accountID, &limit, AccountConcurrencySmartData, policy)
	if first == nil || second == nil || !firstDecision.Admitted || !secondDecision.Admitted {
		t.Fatalf("recovered known-route decisions: first=%#v second=%#v", firstDecision, secondDecision)
	}
	second.ObservePressure()
	first.Release()
	second.Release()
	now = now.Add(policy.UnknownLearningTTL + time.Second)
	postTTL, postTTLDecision := governor.TryAcquire(route, accountID, &limit, AccountConcurrencySmartData, policy)
	if postTTL == nil || !postTTLDecision.Admitted || postTTLDecision.LearnedLimit != nil {
		t.Fatalf("expired known-route overlay remained active: %#v", postTTLDecision)
	}
	postTTL.Release()
}

func TestFixedNoRateCandidateUsesModelFactGenerationForPressureIdentity(t *testing.T) {
	service := &Service{}
	candidate := accountConcurrencyCandidate(77, 700, "Fixed Group")
	candidate.ModelFactGeneration = candidate.UpstreamGeneration
	candidate.UpstreamGeneration = storage.UpstreamGroupGeneration{}

	route := accountConcurrencyRouteGenerationForCandidate(candidate)
	if !route.Valid() || route.UpstreamGroupID != candidate.ModelFactGeneration.ID || route.PresenceGeneration != candidate.ModelFactGeneration.PresenceGeneration {
		t.Fatalf("fixed no-rate route identity = %#v", route)
	}
	pressure, pressureDecision := service.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencySmartData)
	if pressure == nil || !pressureDecision.Admitted {
		t.Fatalf("fixed no-rate pressure acquisition = %#v", pressureDecision)
	}
	pressure.ObservePressure()
	pressure.Release()
	blocked, blockedDecision := service.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencySmartData)
	if blocked != nil || blockedDecision.Admitted || blockedDecision.Reason != "unknown_concurrency_pressure_backoff" {
		t.Fatalf("fixed no-rate candidate lost its pressure identity: %#v", blockedDecision)
	}
	fixed, fixedDecision := service.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencyFixedData)
	if fixed == nil || !fixedDecision.Admitted || !fixedDecision.Tracked {
		t.Fatalf("fixed no-rate route was blocked by its pressure overlay: %#v", fixedDecision)
	}
	fixed.Release()
}

func TestCandidateAccountConcurrencySnapshotRequiresCurrentIdentityAndExplicitProvenance(t *testing.T) {
	channel := storage.Channel{ID: 88, UpstreamAccountIdentityRevision: 3}
	observedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	limit := 1
	valid := explicitAccountConcurrencyTestSnapshot(9088, 3, limit, observedAt)
	accepted := validatedUpstreamAccountConcurrencySnapshot(channel, valid)
	if accepted == nil || accepted == valid || accepted.Limit == valid.Limit || accepted.ObservedAt == valid.ObservedAt {
		t.Fatalf("valid snapshot was not independently accepted: got=%#v input=%#v", accepted, valid)
	}
	unknown := &storage.UpstreamAccountConcurrencySnapshot{
		AccountRecordID: 9088, ChannelIdentityRevision: 3, Scope: storage.UpstreamConcurrencyScopeAccount,
	}
	if validatedUpstreamAccountConcurrencySnapshot(channel, unknown) == nil {
		t.Fatal("identity-confirmed unknown account snapshot was rejected")
	}

	invalid := []*storage.UpstreamAccountConcurrencySnapshot{
		explicitAccountConcurrencyTestSnapshot(9088, 2, limit, observedAt),
		explicitAccountConcurrencyTestSnapshot(0, 3, limit, observedAt),
		explicitAccountConcurrencyTestSnapshot(9088, 3, limit, time.Time{}),
	}
	missingSource := explicitAccountConcurrencyTestSnapshot(9088, 3, limit, observedAt)
	missingSource.Source = ""
	invalid = append(invalid, missingSource)
	wrongScope := explicitAccountConcurrencyTestSnapshot(9088, 3, limit, observedAt)
	wrongScope.Scope = "key"
	invalid = append(invalid, wrongScope)
	for _, snapshot := range invalid {
		if got := validatedUpstreamAccountConcurrencySnapshot(channel, snapshot); got != nil {
			t.Fatalf("invalid snapshot was trusted: %#v", snapshot)
		}
	}

	service := &Service{}
	candidate := accountConcurrencyCandidate(channel.ID, 880, "gpt-test")
	candidate.Channel = channel
	candidate.UpstreamAccountConcurrency = invalid[0]
	first, firstDecision := service.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencySmartData)
	second, secondDecision := service.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencySmartData)
	if first == nil || second == nil || !firstDecision.Admitted || !secondDecision.Admitted || firstDecision.LimitState != "unknown" || secondDecision.LimitState != "unknown" {
		t.Fatalf("stale snapshot became a hard limit: first=%#v second=%#v", firstDecision, secondDecision)
	}
	if tracked := service.accountConcurrencyGovernor.tracker.Snapshot(9088); tracked.Total != 0 {
		t.Fatalf("stale account identity reached hard tracker: %#v", tracked)
	}
	first.Release()
	second.Release()
}

func TestUnknownAccountConcurrencySuccessGraduallyRecoversAndExpires(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	governor := NewAccountConcurrencyGovernor()
	governor.now = func() time.Time { return now }
	route := accountConcurrencyTestRoute(2)
	policy := DefaultAccountConcurrencyPolicy()

	pressure, decision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if pressure == nil || !decision.Admitted {
		t.Fatalf("pressure acquisition = %#v", decision)
	}
	pressure.ObservePressure() // One concurrent rejected request learns one slot.
	pressure.Release()
	for attempt := 0; attempt < policy.UnknownIncreaseSuccesses; attempt++ {
		fixed, fixedDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencyFixedData, policy)
		if fixed == nil || !fixedDecision.Admitted {
			t.Fatalf("fixed recovery %d = %#v", attempt, fixedDecision)
		}
		fixed.ObserveUsefulSuccess()
		fixed.ObserveUsefulSuccess() // One physical response can only count once.
		fixed.Release()
	}
	snapshot := governor.UnknownSnapshot(route)
	if snapshot.LearnedLimit == nil || *snapshot.LearnedLimit != 2 || !snapshot.BackoffUntil.IsZero() {
		t.Fatalf("successful saturation did not recover learned limit: %#v", snapshot)
	}

	first, firstDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	second, secondDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if first == nil || second == nil || !firstDecision.Admitted || !secondDecision.Admitted {
		t.Fatalf("recovered two-slot admission: first=%#v second=%#v", firstDecision, secondDecision)
	}
	first.Release()
	second.Release()

	now = now.Add(policy.UnknownLearningTTL + time.Second)
	postTTL, postTTLDecision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if postTTL == nil || !postTTLDecision.Admitted || postTTLDecision.LearnedLimit != nil {
		t.Fatalf("expired pressure remained a synthetic limit: %#v", postTTLDecision)
	}
	postTTL.Release()
}

func TestUnknownAccountConcurrencySeparatesGenerationsAndReservesDataPlane(t *testing.T) {
	governor := NewAccountConcurrencyGovernor()
	route := accountConcurrencyTestRoute(3)
	nextGeneration := route
	nextGeneration.PresenceGeneration++
	policy := DefaultAccountConcurrencyPolicy()

	pressure, _ := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	pressure.ObservePressure()
	pressure.Release()
	next, nextDecision := governor.TryAcquire(nextGeneration, 0, nil, AccountConcurrencySmartData, policy)
	if next == nil || !nextDecision.Admitted || nextDecision.LearnedLimit != nil {
		t.Fatalf("new generation inherited old pressure: %#v", nextDecision)
	}
	next.Release()

	probe, probeDecision := governor.TryAcquire(nextGeneration, 0, nil, AccountConcurrencyLatencyProbe, policy)
	if probe == nil || !probeDecision.Admitted {
		t.Fatalf("first unknown low-priority request = %#v", probeDecision)
	}
	secondProbe, secondProbeDecision := governor.TryAcquire(nextGeneration, 0, nil, AccountConcurrencyModelCheck, policy)
	if secondProbe != nil || secondProbeDecision.Reason != "low_priority_budget_exhausted" {
		t.Fatalf("unknown low-priority budget = %#v", secondProbeDecision)
	}
	smart, smartDecision := governor.TryAcquire(nextGeneration, 0, nil, AccountConcurrencySmartData, policy)
	if smart == nil || !smartDecision.Admitted {
		t.Fatalf("unknown probe reserved no data-plane path: %#v", smartDecision)
	}
	probe.Release()
	blockedProbe, blockedProbeDecision := governor.TryAcquire(nextGeneration, 0, nil, AccountConcurrencyAPITest, policy)
	if blockedProbe != nil || blockedProbeDecision.Reason != "data_plane_in_flight" {
		t.Fatalf("probe stole active data-plane route: %#v", blockedProbeDecision)
	}
	smart.Release()
}

func TestUnknownAccountConcurrencySeparatesChannelIdentityRevisionAndCanonicalizesGroup(t *testing.T) {
	governor := NewAccountConcurrencyGovernor()
	policy := DefaultAccountConcurrencyPolicy()
	route := accountConcurrencyTestRoute(33)

	pressure, decision := governor.TryAcquire(route, 0, nil, AccountConcurrencySmartData, policy)
	if pressure == nil || !decision.Admitted {
		t.Fatalf("initial route decision = %#v", decision)
	}
	pressure.ObservePressure()
	pressure.Release()

	spellingVariant := route
	spellingVariant.CanonicalGroupName = "  GPT-TEST  "
	if snapshot := governor.UnknownSnapshot(spellingVariant); snapshot.LearnedLimit == nil {
		t.Fatalf("group spelling variant split unknown state: %#v", snapshot)
	}
	blocked, blockedDecision := governor.TryAcquire(spellingVariant, 0, nil, AccountConcurrencySmartData, policy)
	if blocked != nil || blockedDecision.Reason != "unknown_concurrency_pressure_backoff" {
		t.Fatalf("canonical route did not share backoff: %#v", blockedDecision)
	}

	nextIdentity := route
	nextIdentity.ChannelIdentityRevision++
	admitted, admittedDecision := governor.TryAcquire(nextIdentity, 0, nil, AccountConcurrencySmartData, policy)
	if admitted == nil || !admittedDecision.Admitted || admittedDecision.LearnedLimit != nil {
		t.Fatalf("new channel identity inherited old pressure: %#v", admittedDecision)
	}
	admitted.Release()

	nextAccount := route
	nextAccount.UpstreamAccountRecordID++
	accountAdmission, accountDecision := governor.TryAcquire(nextAccount, 0, nil, AccountConcurrencySmartData, policy)
	if accountAdmission == nil || !accountDecision.Admitted || accountDecision.LearnedLimit != nil {
		t.Fatalf("new authenticated account inherited old pressure: %#v", accountDecision)
	}
	accountAdmission.Release()
}

func TestUnknownAccountConcurrencyStateLimitDoesNotBlockDataPlane(t *testing.T) {
	governor := NewAccountConcurrencyGovernor()
	policy := DefaultAccountConcurrencyPolicy()
	policy.UnknownStateLimit = 1
	firstRoute := accountConcurrencyTestRoute(4)
	secondRoute := accountConcurrencyTestRoute(5)
	first, firstDecision := governor.TryAcquire(firstRoute, 0, nil, AccountConcurrencySmartData, policy)
	if first == nil || !firstDecision.Tracked {
		t.Fatalf("first tracked route = %#v", firstDecision)
	}
	second, secondDecision := governor.TryAcquire(secondRoute, 0, nil, AccountConcurrencySmartData, policy)
	if second == nil || secondDecision.Tracked || secondDecision.Reason != "unknown_tracking_capacity_exhausted" {
		t.Fatalf("state-capacity data-plane decision = %#v", secondDecision)
	}
	probe, probeDecision := governor.TryAcquire(secondRoute, 0, nil, AccountConcurrencyAPITest, policy)
	if probe != nil || probeDecision.Reason != "unknown_tracking_capacity_exhausted" {
		t.Fatalf("untracked low-priority work was admitted: %#v", probeDecision)
	}
	second.Release()
	first.Release()
	trackedAgain, trackedAgainDecision := governor.TryAcquire(secondRoute, 0, nil, AccountConcurrencySmartData, policy)
	if trackedAgain == nil || !trackedAgainDecision.Tracked {
		t.Fatalf("idle state did not become reusable: %#v", trackedAgainDecision)
	}
	trackedAgain.Release()
}

func accountConcurrencyTestRoute(id uint) AccountConcurrencyRouteGeneration {
	return AccountConcurrencyRouteGeneration{
		ChannelID:               id,
		ChannelIdentityRevision: 1,
		UpstreamAccountRecordID: uint64(id),
		CanonicalGroupName:      "gpt-test",
		UpstreamGroupID:         id,
		PresenceGeneration:      1,
	}
}

func accountConcurrencyCandidate(channelID, generationID uint, groupName string) Candidate {
	return Candidate{
		Channel: storage.Channel{ID: channelID, UpstreamAccountIdentityRevision: 1},
		Rate:    storage.RateSnapshot{ChannelID: channelID, ModelName: groupName},
		UpstreamGeneration: storage.UpstreamGroupGeneration{
			ID: generationID, ChannelID: channelID, CanonicalGroupName: canonicalGroupName(groupName), PresenceGeneration: 1,
		},
	}
}

func explicitAccountConcurrencyTestSnapshot(accountID, channelIdentityRevision uint64, limit int, observedAt time.Time) *storage.UpstreamAccountConcurrencySnapshot {
	return &storage.UpstreamAccountConcurrencySnapshot{
		AccountRecordID:         accountID,
		ChannelIdentityRevision: channelIdentityRevision,
		Limit:                   &limit,
		Scope:                   storage.UpstreamConcurrencyScopeAccount,
		Source:                  "sub2api.auth.me.concurrency",
		ObservedAt:              &observedAt,
	}
}

type terminalReadCloser struct {
	err error
}

func (r *terminalReadCloser) Read([]byte) (int, error) { return 0, r.err }
func (r *terminalReadCloser) Close() error             { return nil }
