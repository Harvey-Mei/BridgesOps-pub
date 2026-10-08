package gateway

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const AccountConcurrencyTrackerScopeInstance = "instance"

type AccountConcurrencyRequestClass string

const (
	AccountConcurrencySmartData    AccountConcurrencyRequestClass = "smart_data"
	AccountConcurrencyFixedData    AccountConcurrencyRequestClass = "fixed_data"
	AccountConcurrencyAPITest      AccountConcurrencyRequestClass = "api_test"
	AccountConcurrencyLatencyProbe AccountConcurrencyRequestClass = "latency_probe"
	AccountConcurrencyWaitingProbe AccountConcurrencyRequestClass = "waiting_probe"
	AccountConcurrencyModelCheck   AccountConcurrencyRequestClass = "model_check"
	AccountConcurrencyManagement   AccountConcurrencyRequestClass = "management"
)

type AccountConcurrencyPolicy struct {
	// LowPriorityMaxInFlight is an instance-local budget shared by api_test,
	// latency_probe, and model_check for one upstream account.
	LowPriorityMaxInFlight int
	// ReservedDataPlaneSlots keeps at least this many known bounded account
	// slots unavailable to low-priority work. Values below one normalize to one.
	ReservedDataPlaneSlots int
	// UnknownPressureBackoff is a short, non-blocking suppression window after
	// an explicit concurrency-pressure response. Smart routing should choose a
	// different candidate instead of queueing behind this window.
	UnknownPressureBackoff time.Duration
	// UnknownLearningTTL prevents one old pressure response from becoming a
	// permanent synthetic provider limit.
	UnknownLearningTTL time.Duration
	// UnknownIncreaseSuccesses is the number of useful successes observed at
	// the learned ceiling before local admission grows by one slot.
	UnknownIncreaseSuccesses int
	// UnknownStateLimit bounds retained route-generation learning facts.
	UnknownStateLimit int
}

func DefaultAccountConcurrencyPolicy() AccountConcurrencyPolicy {
	return AccountConcurrencyPolicy{
		LowPriorityMaxInFlight:   1,
		ReservedDataPlaneSlots:   1,
		UnknownPressureBackoff:   5 * time.Second,
		UnknownLearningTTL:       10 * time.Minute,
		UnknownIncreaseSuccesses: 3,
		UnknownStateLimit:        4096,
	}
}

type AccountConcurrencyDecision struct {
	Admitted     bool
	Tracked      bool
	Reason       string
	Scope        string
	LimitState   string
	Current      int
	Limit        *int
	LearnedLimit *int
	RetryAfter   time.Duration
}

type AccountConcurrencySnapshot struct {
	Scope       string
	Total       int
	SmartData   int
	FixedData   int
	LowPriority int
}

type accountConcurrencyCounts struct {
	total       int
	smartData   int
	fixedData   int
	lowPriority int
}

// AccountConcurrencyTracker is deliberately instance-local. It is an exact
// observer for work dispatched by this process, not a distributed lease and
// not evidence of a provider-wide current_concurrency value.
type AccountConcurrencyTracker struct {
	mu       sync.Mutex
	accounts map[uint64]accountConcurrencyCounts
}

func NewAccountConcurrencyTracker() *AccountConcurrencyTracker {
	return &AccountConcurrencyTracker{accounts: make(map[uint64]accountConcurrencyCounts)}
}

// TryAcquire applies only provider-authenticated bounded limits. nil means
// unknown and 0 means explicitly unlimited; neither creates a fake hard gate.
// Fixed routing always acquires an observational lease even when the known
// limit is full. Management work is intentionally outside inference slots.
func (t *AccountConcurrencyTracker) TryAcquire(accountID uint64, limit *int, class AccountConcurrencyRequestClass, policy AccountConcurrencyPolicy) (*AccountConcurrencyLease, AccountConcurrencyDecision) {
	decision := AccountConcurrencyDecision{Scope: AccountConcurrencyTrackerScopeInstance, LimitState: accountConcurrencyLimitState(limit)}
	if limit != nil {
		value := *limit
		if value <= 0 {
			value = 0
		}
		decision.Limit = &value
	}
	if class == AccountConcurrencyManagement {
		decision.Admitted = true
		decision.Reason = "management_outside_inference_slots"
		return &AccountConcurrencyLease{}, decision
	}
	if !validAccountConcurrencyClass(class) {
		decision.Reason = "invalid_account_concurrency_request"
		return nil, decision
	}
	if t == nil || accountID == 0 {
		// An unavailable account identity is unknown, never a synthetic zero
		// slot limit. Future dispatch integration can still emit this diagnostic
		// while allowing fixed and smart traffic to continue normally.
		decision.Admitted = true
		decision.Reason = "account_identity_unavailable"
		return &AccountConcurrencyLease{}, decision
	}
	policy = normalizeAccountConcurrencyPolicy(policy)

	t.mu.Lock()
	if t.accounts == nil {
		t.accounts = make(map[uint64]accountConcurrencyCounts)
	}
	counts := t.accounts[accountID]
	decision.Current = counts.total
	if reason := accountConcurrencyRejection(counts, limit, class, policy); reason != "" {
		decision.Reason = reason
		t.mu.Unlock()
		return nil, decision
	}
	counts.total++
	switch class {
	case AccountConcurrencySmartData:
		counts.smartData++
	case AccountConcurrencyFixedData:
		counts.fixedData++
	case AccountConcurrencyAPITest, AccountConcurrencyLatencyProbe, AccountConcurrencyWaitingProbe, AccountConcurrencyModelCheck:
		counts.lowPriority++
	}
	t.accounts[accountID] = counts
	decision.Admitted = true
	decision.Tracked = true
	decision.Current = counts.total
	decision.Reason = "admitted"
	t.mu.Unlock()
	return &AccountConcurrencyLease{tracker: t, accountID: accountID, class: class}, decision
}

func (t *AccountConcurrencyTracker) Snapshot(accountID uint64) AccountConcurrencySnapshot {
	snapshot := AccountConcurrencySnapshot{Scope: AccountConcurrencyTrackerScopeInstance}
	if t == nil || accountID == 0 {
		return snapshot
	}
	t.mu.Lock()
	counts := t.accounts[accountID]
	t.mu.Unlock()
	snapshot.Total = counts.total
	snapshot.SmartData = counts.smartData
	snapshot.FixedData = counts.fixedData
	snapshot.LowPriority = counts.lowPriority
	return snapshot
}

func normalizeAccountConcurrencyPolicy(policy AccountConcurrencyPolicy) AccountConcurrencyPolicy {
	defaults := DefaultAccountConcurrencyPolicy()
	if policy.LowPriorityMaxInFlight < 1 {
		policy.LowPriorityMaxInFlight = defaults.LowPriorityMaxInFlight
	}
	if policy.ReservedDataPlaneSlots < 1 {
		policy.ReservedDataPlaneSlots = defaults.ReservedDataPlaneSlots
	}
	if policy.UnknownPressureBackoff <= 0 {
		policy.UnknownPressureBackoff = defaults.UnknownPressureBackoff
	}
	if policy.UnknownLearningTTL <= 0 {
		policy.UnknownLearningTTL = defaults.UnknownLearningTTL
	}
	if policy.UnknownIncreaseSuccesses < 1 {
		policy.UnknownIncreaseSuccesses = defaults.UnknownIncreaseSuccesses
	}
	if policy.UnknownStateLimit < 1 {
		policy.UnknownStateLimit = defaults.UnknownStateLimit
	}
	return policy
}

func accountConcurrencyLimitState(limit *int) string {
	if limit == nil {
		return "unknown"
	}
	if *limit <= 0 {
		return "unlimited"
	}
	return "bounded"
}

// IsExplicitAccountConcurrencyPressure intentionally rejects a bare 429 as
// evidence. Providers also use 429 for token, daily, and billing quotas; only
// an explicit concurrency/capacity message may teach the unknown-limit
// governor a local ceiling.
func IsExplicitAccountConcurrencyPressure(status int, message string) bool {
	normalized := strings.ToLower(strings.TrimSpace(message))
	if normalized == "" {
		return false
	}
	for _, phrase := range []string{
		"concurrency limit",
		"concurrent request limit",
		"too many concurrent",
		"maximum concurrent",
		"max concurrent",
		"concurrency exceeded",
		"concurrency is full",
		"account is at capacity",
		"account capacity exhausted",
		"并发限制",
		"并发上限",
		"并发已满",
		"并发数过多",
	} {
		if strings.Contains(normalized, phrase) {
			return true
		}
	}
	return false
}

func validAccountConcurrencyClass(class AccountConcurrencyRequestClass) bool {
	switch class {
	case AccountConcurrencySmartData, AccountConcurrencyFixedData,
		AccountConcurrencyAPITest, AccountConcurrencyLatencyProbe, AccountConcurrencyWaitingProbe, AccountConcurrencyModelCheck:
		return true
	default:
		return false
	}
}

func accountConcurrencyRejection(counts accountConcurrencyCounts, limit *int, class AccountConcurrencyRequestClass, policy AccountConcurrencyPolicy) string {
	if class == AccountConcurrencyFixedData {
		return ""
	}
	lowPriority := class == AccountConcurrencyAPITest || class == AccountConcurrencyLatencyProbe || class == AccountConcurrencyWaitingProbe || class == AccountConcurrencyModelCheck
	if lowPriority && counts.lowPriority >= policy.LowPriorityMaxInFlight {
		return "low_priority_budget_exhausted"
	}
	if limit == nil || *limit <= 0 {
		return ""
	}
	boundedLimit := *limit
	if lowPriority {
		lowPriorityCeiling := boundedLimit - policy.ReservedDataPlaneSlots
		if lowPriorityCeiling < 0 {
			lowPriorityCeiling = 0
		}
		if counts.total >= lowPriorityCeiling {
			return "reserved_for_data_plane"
		}
		return ""
	}
	if counts.total >= boundedLimit {
		return "account_concurrency_limit_reached"
	}
	return ""
}

type AccountConcurrencyLease struct {
	tracker     *AccountConcurrencyTracker
	accountID   uint64
	class       AccountConcurrencyRequestClass
	releaseFn   func()
	pressureFn  func()
	successFn   func()
	once        sync.Once
	outcomeOnce sync.Once
	contextMu   sync.Mutex
	contextStop func() bool
	released    bool
}

// ObservePressure records one explicit provider concurrency-pressure outcome.
// It is safe after EOF has already released the in-flight slot.
func (l *AccountConcurrencyLease) ObservePressure() {
	if l == nil {
		return
	}
	l.outcomeOnce.Do(func() {
		if l.pressureFn != nil {
			l.pressureFn()
		}
	})
}

// ObserveUsefulSuccess clears short pressure backoff and, only after repeated
// successes at the learned ceiling, increases the local unknown-limit budget.
func (l *AccountConcurrencyLease) ObserveUsefulSuccess() {
	if l == nil {
		return
	}
	l.outcomeOnce.Do(func() {
		if l.successFn != nil {
			l.successFn()
		}
	})
}

// BindContext releases the slot if the downstream request is canceled even
// when a caller forgets to read or close the upstream response body. Bind the
// original downstream context, not a first-byte-only child context.
func (l *AccountConcurrencyLease) BindContext(ctx context.Context) {
	if l == nil || ctx == nil || ctx.Done() == nil {
		return
	}
	stop := context.AfterFunc(ctx, l.Release)
	l.contextMu.Lock()
	if l.released {
		l.contextMu.Unlock()
		stop()
		return
	}
	previous := l.contextStop
	l.contextStop = stop
	l.contextMu.Unlock()
	if previous != nil {
		previous()
	}
}

// Release is safe from every terminal path and can be called repeatedly.
func (l *AccountConcurrencyLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.contextMu.Lock()
		l.released = true
		stop := l.contextStop
		l.contextStop = nil
		l.contextMu.Unlock()
		if stop != nil {
			stop()
		}
		if l.releaseFn != nil {
			l.releaseFn()
			return
		}
		if l.tracker == nil || l.accountID == 0 {
			return
		}
		l.tracker.mu.Lock()
		counts := l.tracker.accounts[l.accountID]
		if counts.total > 0 {
			counts.total--
		}
		switch l.class {
		case AccountConcurrencySmartData:
			if counts.smartData > 0 {
				counts.smartData--
			}
		case AccountConcurrencyFixedData:
			if counts.fixedData > 0 {
				counts.fixedData--
			}
		case AccountConcurrencyAPITest, AccountConcurrencyLatencyProbe, AccountConcurrencyWaitingProbe, AccountConcurrencyModelCheck:
			if counts.lowPriority > 0 {
				counts.lowPriority--
			}
		}
		if counts.total == 0 {
			delete(l.tracker.accounts, l.accountID)
		} else {
			l.tracker.accounts[l.accountID] = counts
		}
		l.tracker.mu.Unlock()
	})
}

// BindHTTPResponse keeps the lease until the response body reaches EOF, read
// failure, or Close. A transport error or missing body releases immediately.
func (l *AccountConcurrencyLease) BindHTTPResponse(response *http.Response, transportErr error) {
	if l == nil {
		return
	}
	if transportErr != nil || response == nil || response.Body == nil {
		l.Release()
		return
	}
	response.Body = l.WrapResponseBody(response.Body)
}

func (l *AccountConcurrencyLease) WrapResponseBody(body io.ReadCloser) io.ReadCloser {
	if l == nil {
		return body
	}
	if body == nil {
		l.Release()
		return nil
	}
	return &accountConcurrencyLeaseBody{ReadCloser: body, lease: l}
}

type accountConcurrencyLeaseBody struct {
	io.ReadCloser
	lease *AccountConcurrencyLease
}

func (b *accountConcurrencyLeaseBody) Read(buffer []byte) (int, error) {
	read, err := b.ReadCloser.Read(buffer)
	if err != nil {
		b.lease.Release()
	}
	return read, err
}

func (b *accountConcurrencyLeaseBody) Close() error {
	defer b.lease.Release()
	return b.ReadCloser.Close()
}
