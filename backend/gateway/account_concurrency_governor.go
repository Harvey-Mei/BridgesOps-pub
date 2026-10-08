package gateway

import (
	"strings"
	"sync"
	"time"
)

// AccountConcurrencyRouteGeneration isolates unknown-limit learning to one
// exact upstream group incarnation, one channel-account identity revision,
// and (when known) one authenticated upstream account record. A recreated
// group or changed account must not inherit pressure learned by the previous
// route identity.
type AccountConcurrencyRouteGeneration struct {
	ChannelID               uint
	ChannelIdentityRevision uint64
	UpstreamAccountRecordID uint64
	CanonicalGroupName      string
	UpstreamGroupID         uint
	PresenceGeneration      uint64
}

func (k AccountConcurrencyRouteGeneration) Valid() bool {
	return k.ChannelID > 0 && k.ChannelIdentityRevision > 0 && strings.TrimSpace(k.CanonicalGroupName) != "" && k.UpstreamGroupID > 0 && k.PresenceGeneration > 0
}

func normalizeAccountConcurrencyRouteGeneration(route AccountConcurrencyRouteGeneration) AccountConcurrencyRouteGeneration {
	route.CanonicalGroupName = canonicalGroupName(route.CanonicalGroupName)
	return route
}

type accountConcurrencyUnknownState struct {
	counts             accountConcurrencyCounts
	learnedLimit       int
	backoffUntil       time.Time
	learningExpiresAt  time.Time
	saturatedSuccesses int
	lastTouched        time.Time
}

// AccountConcurrencyUnknownSnapshot is process-local diagnostic state. It is
// never a provider-authenticated account limit and must not be persisted as
// one.
type AccountConcurrencyUnknownSnapshot struct {
	Current            int
	SmartData          int
	FixedData          int
	LowPriority        int
	LearnedLimit       *int
	BackoffUntil       time.Time
	LearningExpiresAt  time.Time
	SaturatedSuccesses int
}

// AccountConcurrencyGovernor combines provider-authenticated account limits
// with conservative, expiring route-generation learning for unknown limits.
// It never queues requests. Production evidence showed repeated 30-second
// first-byte waits turning successful fallbacks into 55-109 second requests;
// a rejected smart admission must immediately move to another candidate.
type AccountConcurrencyGovernor struct {
	mu      sync.Mutex
	tracker *AccountConcurrencyTracker
	unknown map[AccountConcurrencyRouteGeneration]*accountConcurrencyUnknownState
	now     func() time.Time
}

func NewAccountConcurrencyGovernor() *AccountConcurrencyGovernor {
	return &AccountConcurrencyGovernor{
		tracker: NewAccountConcurrencyTracker(),
		unknown: make(map[AccountConcurrencyRouteGeneration]*accountConcurrencyUnknownState),
		now:     time.Now,
	}
}

// TryAcquire uses a durable account record whenever an explicit bounded or
// unlimited fact exists. Known facts retain the account-wide hard gate while
// every data-plane route also participates in the exact-generation pressure
// overlay. A nil limit is governed only by that expiring local overlay.
func (g *AccountConcurrencyGovernor) TryAcquire(route AccountConcurrencyRouteGeneration, accountID uint64, limit *int, class AccountConcurrencyRequestClass, policy AccountConcurrencyPolicy) (*AccountConcurrencyLease, AccountConcurrencyDecision) {
	policy = normalizeAccountConcurrencyPolicy(policy)
	if class == AccountConcurrencyManagement {
		return g.accountTracker().TryAcquire(accountID, limit, class, policy)
	}
	if limit == nil {
		return g.tryAcquireRouteOverlay(route, class, policy)
	}

	accountLease, accountDecision := g.accountTracker().TryAcquire(accountID, limit, class, policy)
	if accountLease == nil || !accountDecision.Admitted {
		return nil, accountDecision
	}
	routeLease, routeDecision := g.tryAcquireRouteOverlay(route, class, policy)
	if routeLease == nil || !routeDecision.Admitted {
		accountLease.Release()
		routeDecision.LimitState = accountDecision.LimitState
		routeDecision.Limit = accountDecision.Limit
		routeDecision.Tracked = false
		return nil, routeDecision
	}

	accountDecision.Tracked = accountDecision.Tracked || routeDecision.Tracked
	accountDecision.LearnedLimit = routeDecision.LearnedLimit
	return combineAccountConcurrencyLeases(accountLease, routeLease), accountDecision
}

func (g *AccountConcurrencyGovernor) tryAcquireRouteOverlay(route AccountConcurrencyRouteGeneration, class AccountConcurrencyRequestClass, policy AccountConcurrencyPolicy) (*AccountConcurrencyLease, AccountConcurrencyDecision) {
	route = normalizeAccountConcurrencyRouteGeneration(route)
	decision := AccountConcurrencyDecision{
		Scope:      AccountConcurrencyTrackerScopeInstance,
		LimitState: "unknown",
	}
	if !validAccountConcurrencyClass(class) {
		decision.Reason = "invalid_account_concurrency_request"
		return nil, decision
	}
	if g == nil || !route.Valid() {
		if accountConcurrencyLowPriority(class) {
			decision.Reason = "unknown_route_identity_unavailable"
			return nil, decision
		}
		decision.Admitted = true
		decision.Reason = "unknown_route_identity_unavailable"
		return &AccountConcurrencyLease{}, decision
	}

	now := g.currentTime()
	g.mu.Lock()
	if g.unknown == nil {
		g.unknown = make(map[AccountConcurrencyRouteGeneration]*accountConcurrencyUnknownState)
	}
	state := g.unknown[route]
	if state == nil {
		state = g.allocateUnknownStateLocked(route, now, policy.UnknownStateLimit)
	}
	if state == nil {
		g.mu.Unlock()
		if accountConcurrencyLowPriority(class) {
			decision.Reason = "unknown_tracking_capacity_exhausted"
			return nil, decision
		}
		decision.Admitted = true
		decision.Reason = "unknown_tracking_capacity_exhausted"
		return &AccountConcurrencyLease{}, decision
	}
	g.expireUnknownLearningLocked(state, now)
	state.lastTouched = now
	decision.Current = state.counts.total
	if state.learnedLimit > 0 {
		learned := state.learnedLimit
		decision.LearnedLimit = &learned
	}
	if reason, retryAfter := unknownAccountConcurrencyRejection(state, class, policy, now); reason != "" {
		decision.Reason = reason
		decision.RetryAfter = retryAfter
		g.mu.Unlock()
		return nil, decision
	}

	state.counts.total++
	switch class {
	case AccountConcurrencySmartData:
		state.counts.smartData++
	case AccountConcurrencyFixedData:
		state.counts.fixedData++
	case AccountConcurrencyAPITest, AccountConcurrencyLatencyProbe, AccountConcurrencyWaitingProbe, AccountConcurrencyModelCheck:
		state.counts.lowPriority++
	}
	current := state.counts.total
	decision.Admitted = true
	decision.Tracked = true
	decision.Current = current
	decision.Reason = "admitted"
	g.mu.Unlock()

	lease := &AccountConcurrencyLease{class: class}
	lease.releaseFn = func() { g.releaseUnknown(route, class) }
	lease.pressureFn = func() { g.observeUnknownPressure(route, current, policy) }
	lease.successFn = func() { g.observeUnknownSuccess(route, current, policy) }
	return lease, decision
}

func combineAccountConcurrencyLeases(accountLease, routeLease *AccountConcurrencyLease) *AccountConcurrencyLease {
	if accountLease == nil {
		return routeLease
	}
	if routeLease == nil {
		return accountLease
	}
	return &AccountConcurrencyLease{
		releaseFn: func() {
			routeLease.Release()
			accountLease.Release()
		},
		pressureFn: routeLease.ObservePressure,
		successFn:  routeLease.ObserveUsefulSuccess,
	}
}

func (g *AccountConcurrencyGovernor) accountTracker() *AccountConcurrencyTracker {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	if g.tracker == nil {
		g.tracker = NewAccountConcurrencyTracker()
	}
	tracker := g.tracker
	g.mu.Unlock()
	return tracker
}

func (g *AccountConcurrencyGovernor) currentTime() time.Time {
	if g != nil && g.now != nil {
		return g.now()
	}
	return time.Now()
}

func (g *AccountConcurrencyGovernor) allocateUnknownStateLocked(route AccountConcurrencyRouteGeneration, now time.Time, limit int) *accountConcurrencyUnknownState {
	if state := g.unknown[route]; state != nil {
		return state
	}
	if len(g.unknown) >= limit {
		g.evictUnknownStateLocked(now)
	}
	if len(g.unknown) >= limit {
		return nil
	}
	state := &accountConcurrencyUnknownState{lastTouched: now}
	g.unknown[route] = state
	return state
}

func (g *AccountConcurrencyGovernor) evictUnknownStateLocked(now time.Time) {
	var oldestKey AccountConcurrencyRouteGeneration
	var oldestAt time.Time
	found := false
	for key, state := range g.unknown {
		if state == nil || state.counts.total != 0 {
			continue
		}
		g.expireUnknownLearningLocked(state, now)
		if state.learnedLimit == 0 && state.backoffUntil.IsZero() {
			delete(g.unknown, key)
			return
		}
		if !found || state.lastTouched.Before(oldestAt) {
			oldestKey = key
			oldestAt = state.lastTouched
			found = true
		}
	}
	if found {
		delete(g.unknown, oldestKey)
	}
}

func (g *AccountConcurrencyGovernor) expireUnknownLearningLocked(state *accountConcurrencyUnknownState, now time.Time) {
	if state == nil || state.learningExpiresAt.IsZero() || now.Before(state.learningExpiresAt) {
		return
	}
	state.learnedLimit = 0
	state.backoffUntil = time.Time{}
	state.learningExpiresAt = time.Time{}
	state.saturatedSuccesses = 0
}

func unknownAccountConcurrencyRejection(state *accountConcurrencyUnknownState, class AccountConcurrencyRequestClass, policy AccountConcurrencyPolicy, now time.Time) (string, time.Duration) {
	if state == nil || class == AccountConcurrencyFixedData {
		return "", 0
	}
	if accountConcurrencyLowPriority(class) {
		if class != AccountConcurrencyWaitingProbe && state.counts.smartData+state.counts.fixedData > 0 {
			return "data_plane_in_flight", 0
		}
		if state.counts.lowPriority >= policy.LowPriorityMaxInFlight {
			return "low_priority_budget_exhausted", 0
		}
		if state.learnedLimit > 0 {
			ceiling := state.learnedLimit - policy.ReservedDataPlaneSlots
			if ceiling < 0 {
				ceiling = 0
			}
			if state.counts.total >= ceiling {
				return "reserved_for_data_plane", 0
			}
		}
		return "", 0
	}
	if !state.backoffUntil.IsZero() && now.Before(state.backoffUntil) {
		return "unknown_concurrency_pressure_backoff", state.backoffUntil.Sub(now)
	}
	if state.learnedLimit > 0 && state.counts.total >= state.learnedLimit {
		return "learned_account_concurrency_limit_reached", 0
	}
	return "", 0
}

func accountConcurrencyLowPriority(class AccountConcurrencyRequestClass) bool {
	return class == AccountConcurrencyAPITest || class == AccountConcurrencyLatencyProbe || class == AccountConcurrencyWaitingProbe || class == AccountConcurrencyModelCheck
}

func (g *AccountConcurrencyGovernor) releaseUnknown(route AccountConcurrencyRouteGeneration, class AccountConcurrencyRequestClass) {
	if g == nil {
		return
	}
	now := g.currentTime()
	g.mu.Lock()
	state := g.unknown[route]
	if state == nil {
		g.mu.Unlock()
		return
	}
	if state.counts.total > 0 {
		state.counts.total--
	}
	switch class {
	case AccountConcurrencySmartData:
		if state.counts.smartData > 0 {
			state.counts.smartData--
		}
	case AccountConcurrencyFixedData:
		if state.counts.fixedData > 0 {
			state.counts.fixedData--
		}
	case AccountConcurrencyAPITest, AccountConcurrencyLatencyProbe, AccountConcurrencyWaitingProbe, AccountConcurrencyModelCheck:
		if state.counts.lowPriority > 0 {
			state.counts.lowPriority--
		}
	}
	state.lastTouched = now
	g.expireUnknownLearningLocked(state, now)
	if state.counts.total == 0 && state.learnedLimit == 0 && state.backoffUntil.IsZero() {
		delete(g.unknown, route)
	}
	g.mu.Unlock()
}

func (g *AccountConcurrencyGovernor) observeUnknownPressure(route AccountConcurrencyRouteGeneration, observedInFlight int, policy AccountConcurrencyPolicy) {
	route = normalizeAccountConcurrencyRouteGeneration(route)
	if g == nil || !route.Valid() {
		return
	}
	policy = normalizeAccountConcurrencyPolicy(policy)
	now := g.currentTime()
	g.mu.Lock()
	if g.unknown == nil {
		g.unknown = make(map[AccountConcurrencyRouteGeneration]*accountConcurrencyUnknownState)
	}
	state := g.allocateUnknownStateLocked(route, now, policy.UnknownStateLimit)
	if state == nil {
		g.mu.Unlock()
		return
	}
	g.expireUnknownLearningLocked(state, now)
	learned := observedInFlight - 1
	if learned < 1 {
		learned = 1
	}
	if state.learnedLimit == 0 || learned < state.learnedLimit {
		state.learnedLimit = learned
	}
	backoffUntil := now.Add(policy.UnknownPressureBackoff)
	if state.backoffUntil.Before(backoffUntil) {
		state.backoffUntil = backoffUntil
	}
	state.learningExpiresAt = now.Add(policy.UnknownLearningTTL)
	state.saturatedSuccesses = 0
	state.lastTouched = now
	g.mu.Unlock()
}

func (g *AccountConcurrencyGovernor) observeUnknownSuccess(route AccountConcurrencyRouteGeneration, observedInFlight int, policy AccountConcurrencyPolicy) {
	route = normalizeAccountConcurrencyRouteGeneration(route)
	if g == nil || !route.Valid() {
		return
	}
	policy = normalizeAccountConcurrencyPolicy(policy)
	now := g.currentTime()
	g.mu.Lock()
	state := g.unknown[route]
	if state == nil {
		g.mu.Unlock()
		return
	}
	g.expireUnknownLearningLocked(state, now)
	if state.learnedLimit > 0 {
		if observedInFlight >= state.learnedLimit {
			// A success below the learned ceiling proves health, but it does not
			// prove that another concurrency slot is available. Only saturated
			// success may relax capacity backoff or grow the soft ceiling.
			state.backoffUntil = time.Time{}
			state.saturatedSuccesses++
			if state.saturatedSuccesses >= policy.UnknownIncreaseSuccesses {
				state.learnedLimit++
				state.saturatedSuccesses = 0
			}
		} else {
			state.saturatedSuccesses = 0
		}
		state.learningExpiresAt = now.Add(policy.UnknownLearningTTL)
	}
	state.lastTouched = now
	if state.counts.total == 0 && state.learnedLimit == 0 {
		delete(g.unknown, route)
	}
	g.mu.Unlock()
}

func (g *AccountConcurrencyGovernor) UnknownSnapshot(route AccountConcurrencyRouteGeneration) AccountConcurrencyUnknownSnapshot {
	route = normalizeAccountConcurrencyRouteGeneration(route)
	if g == nil || !route.Valid() {
		return AccountConcurrencyUnknownSnapshot{}
	}
	now := g.currentTime()
	g.mu.Lock()
	state := g.unknown[route]
	if state == nil {
		g.mu.Unlock()
		return AccountConcurrencyUnknownSnapshot{}
	}
	g.expireUnknownLearningLocked(state, now)
	snapshot := AccountConcurrencyUnknownSnapshot{
		Current:            state.counts.total,
		SmartData:          state.counts.smartData,
		FixedData:          state.counts.fixedData,
		LowPriority:        state.counts.lowPriority,
		BackoffUntil:       state.backoffUntil,
		LearningExpiresAt:  state.learningExpiresAt,
		SaturatedSuccesses: state.saturatedSuccesses,
	}
	if state.learnedLimit > 0 {
		learned := state.learnedLimit
		snapshot.LearnedLimit = &learned
	}
	g.mu.Unlock()
	return snapshot
}
