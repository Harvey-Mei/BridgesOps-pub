package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const (
	requestDispatchRecoveryInterval       = 10 * time.Second
	requestDispatchRecoveryHandoffLimit   = 4096
	requestDispatchRecoveryHandoffBatch   = 100
	requestDispatchOwnerHeartbeatInterval = 10 * time.Second
	requestDispatchOwnerRetryInterval     = 2 * time.Second
	requestDispatchOwnerOperationTimeout  = 5 * time.Second

	// requestDispatchOwnerDrainMaximum bounds how long a fenced owner may keep
	// refusing every new binding. The lease itself is 45 seconds and all bound
	// contexts are canceled as soon as it is lost, so a transport that honors
	// cancellation always drains well inside this window. Twice the lease
	// duration is still two orders of magnitude below the one-hour orphan
	// recovery grace, which leaves an abandoned exact handoff ample time to
	// land before any neutral reconstruction can be attempted.
	requestDispatchOwnerDrainMaximum = 90 * time.Second

	// requestDispatchTerminalHandoffRetryLimit bounds how many exact terminal
	// snapshots this process keeps alive while their durable write is failing.
	requestDispatchTerminalHandoffRetryLimit = 512
	// requestDispatchTerminalHandoffRetryBudget stops retrying an exact snapshot
	// well before requestDispatchRecoveryOrphanGrace, so the two paths can never
	// compete for the same dispatch.
	requestDispatchTerminalHandoffRetryBudget = 15 * time.Minute
)

type requestDispatchRecoveryHandoff struct {
	job RequestDispatchRecoveryJob
}

// RequestDispatchRecoveryJob is a bounded in-memory retry record for one
// physical dispatch. It carries the exact terminal log plus lifecycle facts
// whose initial database write failed, rather than reconstructing a generic
// error while this process is still alive.
type RequestDispatchRecoveryJob struct {
	DispatchID      uint64
	OwnerInstanceID string
	OwnerFenceToken uint64
	TerminalKind    string
	RequestedAt     time.Time
	StartedAt       time.Time
	FinishedAt      time.Time
	Log             *storage.GatewayRequestLog
	Facts           storage.GatewayRequestDispatchFinalizationFacts
	// TelemetryVersion preserves the legacy-record boundary when a physical
	// dispatch needs a synthesized terminal request log.
	TelemetryVersion int
	// PhysicallyDispatched prevents a nil-log recovery job from silently
	// completing an attempt that entered a real RoundTripper.
	PhysicallyDispatched bool
}

var ErrRequestDispatchRecoveryDurablyHandedOff = errors.New("gateway request dispatch recovery was durably handed off")

var ErrRequestDispatchOwnerLeaseLost = errors.New("gateway request dispatch owner lease lost")

type requestDispatchOwnerStore interface {
	RegisterRequestDispatchOwner(context.Context, time.Time, time.Time) error
	RenewRequestDispatchOwner(context.Context, time.Time, time.Time) error
	StopRequestDispatchOwner(context.Context, time.Time) error
	RotateRequestDispatchOwner(context.Context, time.Time, time.Time) error
}

type requestDispatchOwnerLease struct {
	store requestDispatchOwnerStore
	log   *slog.Logger

	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	retryInterval     time.Duration
	operationTimeout  time.Duration
	drainMaximum      time.Duration
	context           context.Context
	cancel            context.CancelCauseFunc
	loopContext       context.Context
	stopLoop          context.CancelFunc
	done              chan struct{}
	stopOnce          sync.Once
	lossOnce          sync.Once
	mu                sync.RWMutex
	leaseUntil        time.Time
	bindingsMu        sync.Mutex
	activeBindings    int
	pendingHandoffs   int
	draining          bool
	drainClosed       bool
	drainStartedAt    time.Time
	bindingsDrained   chan struct{}
}

func newRequestDispatchOwnerLease(store requestDispatchOwnerStore, log *slog.Logger) *requestDispatchOwnerLease {
	ownerContext, cancel := context.WithCancelCause(context.Background())
	loopContext, stopLoop := context.WithCancel(context.Background())
	return &requestDispatchOwnerLease{
		store:             store,
		log:               log,
		leaseDuration:     storage.GatewayRequestDispatchOwnerLeaseDuration,
		heartbeatInterval: requestDispatchOwnerHeartbeatInterval,
		retryInterval:     requestDispatchOwnerRetryInterval,
		operationTimeout:  requestDispatchOwnerOperationTimeout,
		drainMaximum:      requestDispatchOwnerDrainMaximum,
		context:           ownerContext,
		cancel:            cancel,
		loopContext:       loopContext,
		stopLoop:          stopLoop,
		done:              make(chan struct{}),
		bindingsDrained:   make(chan struct{}),
	}
}

func (o *requestDispatchOwnerLease) start(now time.Time) error {
	if o == nil || o.store == nil {
		return errors.New("gateway request dispatch owner store is unavailable")
	}
	if now.IsZero() {
		now = time.Now()
	}
	// Keep the process-local deadline on Go's monotonic clock. UTC conversion is
	// only for the durable database timestamps; using it for local cancellation
	// would make an NTP wall-clock jump extend or prematurely end this lease.
	localLeaseUntil := now.Add(o.leaseDuration)
	databaseNow := now.UTC()
	databaseLeaseUntil := databaseNow.Add(o.leaseDuration)
	ctx, cancel := context.WithTimeout(o.loopContext, o.operationTimeout)
	err := o.store.RegisterRequestDispatchOwner(ctx, databaseNow, databaseLeaseUntil)
	cancel()
	if err != nil {
		o.stopLoop()
		o.lose(err)
		return err
	}
	o.setLeaseUntil(localLeaseUntil)
	go o.run()
	return nil
}

func (o *requestDispatchOwnerLease) run() {
	defer close(o.done)
	heartbeat := time.NewTimer(o.heartbeatInterval)
	expiry := time.NewTimer(o.timeUntilExpiry(time.Now()))
	defer heartbeat.Stop()
	defer expiry.Stop()

	failures := 0
	for {
		select {
		case <-o.loopContext.Done():
			return
		case <-expiry.C:
			o.logTerminal(errors.New("local owner lease deadline elapsed"))
			o.lose(errors.New("local owner lease deadline elapsed"))
			return
		case <-heartbeat.C:
			err := o.renew()
			now := time.Now()
			if err == nil {
				if failures > 0 && o.log != nil {
					o.log.Info("gateway request dispatch owner heartbeat recovered", "failures", failures)
				}
				failures = 0
				resetRequestDispatchTimer(expiry, o.timeUntilExpiry(now))
				resetRequestDispatchTimer(heartbeat, o.heartbeatInterval)
				continue
			}

			failures++
			if errors.Is(err, storage.ErrGatewayRequestDispatchOwnerFenced) {
				o.logTerminal(err)
				o.lose(err)
				return
			}
			remaining := o.timeUntilExpiry(now)
			if remaining <= 0 {
				o.logTerminal(err)
				o.lose(err)
				return
			}
			if failures == 1 && o.log != nil {
				o.log.Warn("gateway request dispatch owner heartbeat failed; retrying within current lease", "err", err, "lease_remaining", remaining)
			}
			delay := o.retryInterval
			if remaining < delay {
				delay = remaining
			}
			resetRequestDispatchTimer(heartbeat, delay)
		}
	}
}

func (o *requestDispatchOwnerLease) renew() error {
	now := time.Now()
	leaseUntil := o.currentLeaseUntil()
	if !now.Before(leaseUntil) {
		return ErrRequestDispatchOwnerLeaseLost
	}
	deadline := now.Add(o.operationTimeout)
	if leaseUntil.Before(deadline) {
		deadline = leaseUntil
	}
	ctx, cancel := context.WithDeadline(o.loopContext, deadline)
	defer cancel()
	nextLocalLeaseUntil := now.Add(o.leaseDuration)
	databaseNow := now.UTC()
	databaseLeaseUntil := databaseNow.Add(o.leaseDuration)
	if err := o.store.RenewRequestDispatchOwner(ctx, databaseNow, databaseLeaseUntil); err != nil {
		return err
	}
	o.setLeaseUntil(nextLocalLeaseUntil)
	return nil
}

func (o *requestDispatchOwnerLease) bind(parent context.Context) (context.Context, context.CancelFunc, error) {
	if o == nil {
		return nil, nil, errors.New("gateway request dispatch owner is unavailable")
	}
	if parent == nil {
		parent = context.Background()
	}
	leaseUntil := o.currentLeaseUntil()
	if !time.Now().Before(leaseUntil) {
		o.lose(errors.New("local owner lease deadline elapsed"))
		return nil, nil, ErrRequestDispatchOwnerLeaseLost
	}
	o.bindingsMu.Lock()
	if cause := context.Cause(o.context); cause != nil {
		o.bindingsMu.Unlock()
		return nil, nil, cause
	}
	if o.draining {
		o.bindingsMu.Unlock()
		return nil, nil, ErrRequestDispatchOwnerLeaseLost
	}
	o.activeBindings++
	o.bindingsMu.Unlock()
	bound, cancel := context.WithCancelCause(parent)
	stopOwnerCancellation := context.AfterFunc(o.context, func() {
		cancel(context.Cause(o.context))
	})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			stopOwnerCancellation()
			cancel(context.Canceled)
			o.bindingsMu.Lock()
			o.activeBindings--
			o.closeDrainLocked()
			o.bindingsMu.Unlock()
		})
	}
	return bound, release, nil
}

// closeDrainLocked opens the rotation gate only once this incarnation has no
// live binding left and no exact terminal handoff is still waiting to become
// durable. Releasing the request alone is not an acknowledgement: a surrendered
// audit whose durable handoff has not been persisted must keep the old identity
// gated so the exact snapshot stays retryable.
func (o *requestDispatchOwnerLease) closeDrainLocked() {
	if !o.draining || o.activeBindings > 0 || o.pendingHandoffs > 0 {
		return
	}
	o.markDrainedLocked()
}

func (o *requestDispatchOwnerLease) markDrainedLocked() {
	if o.drainClosed {
		return
	}
	o.drainClosed = true
	close(o.bindingsDrained)
}

// retainDurableHandoff registers one exact terminal snapshot that has not yet
// been acknowledged by the database. It is taken while the request still holds
// its binding, so the drain gate can never close in the window between
// surrendering an audit and durably persisting it.
func (o *requestDispatchOwnerLease) retainDurableHandoff() func() {
	if o == nil {
		return func() {}
	}
	o.bindingsMu.Lock()
	o.pendingHandoffs++
	o.bindingsMu.Unlock()
	var releaseOnce sync.Once
	return func() {
		releaseOnce.Do(func() {
			o.bindingsMu.Lock()
			if o.pendingHandoffs > 0 {
				o.pendingHandoffs--
			}
			o.closeDrainLocked()
			o.bindingsMu.Unlock()
		})
	}
}

func (o *requestDispatchOwnerLease) beginDrain() <-chan struct{} {
	if o == nil {
		drained := make(chan struct{})
		close(drained)
		return drained
	}
	o.bindingsMu.Lock()
	if !o.draining {
		o.draining = true
		o.drainStartedAt = time.Now()
		o.closeDrainLocked()
	}
	drained := o.bindingsDrained
	o.bindingsMu.Unlock()
	return drained
}

// forceDrain abandons the bounded wait. It never fabricates or completes any
// dispatch outcome: it only stops gating rotation and reports what was still
// outstanding so the degrade can be audited. Outstanding exact handoffs remain
// authorized by the immutable audit-bound owner identity and keep retrying.
func (o *requestDispatchOwnerLease) forceDrain() storage.GatewayRequestDispatchForcedDrain {
	forced := storage.GatewayRequestDispatchForcedDrain{DrainMaximumWait: requestDispatchOwnerDrainMaximum}
	if o == nil {
		return forced
	}
	if o.drainMaximum > 0 {
		forced.DrainMaximumWait = o.drainMaximum
	}
	o.bindingsMu.Lock()
	o.draining = true
	forced.ActiveBindings = o.activeBindings
	forced.PendingHandoffs = o.pendingHandoffs
	if !o.drainStartedAt.IsZero() {
		forced.DrainWaitedFor = time.Since(o.drainStartedAt)
	}
	o.markDrainedLocked()
	o.bindingsMu.Unlock()
	return forced
}

func (o *requestDispatchOwnerLease) stop(ctx context.Context, now time.Time) error {
	if o == nil || o.store == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	o.stopOnce.Do(o.stopLoop)
	select {
	case <-o.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	o.lose(errors.New("gateway request dispatch owner stopped"))
	return o.store.StopRequestDispatchOwner(ctx, normalizeRequestDispatchOwnerTime(now))
}

func (o *requestDispatchOwnerLease) lose(reason error) {
	o.lossOnce.Do(func() {
		if reason == nil {
			reason = ErrRequestDispatchOwnerLeaseLost
		}
		o.cancel(fmt.Errorf("%w: %v", ErrRequestDispatchOwnerLeaseLost, reason))
	})
}

func (o *requestDispatchOwnerLease) logTerminal(err error) {
	if o != nil && o.log != nil {
		o.log.Error("gateway request dispatch owner lease lost", "err", err)
	}
}

func (o *requestDispatchOwnerLease) currentLeaseUntil() time.Time {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.leaseUntil
}

func (o *requestDispatchOwnerLease) setLeaseUntil(value time.Time) {
	o.mu.Lock()
	o.leaseUntil = value
	o.mu.Unlock()
}

func (o *requestDispatchOwnerLease) timeUntilExpiry(now time.Time) time.Duration {
	return o.currentLeaseUntil().Sub(now)
}

func normalizeRequestDispatchOwnerTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

func resetRequestDispatchTimer(timer *time.Timer, delay time.Duration) {
	if delay <= 0 {
		delay = time.Nanosecond
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

func (s *Service) StartRequestDispatchOwner() error {
	if s == nil || s.Store == nil {
		return errors.New("gateway request dispatch owner store is unavailable")
	}
	s.requestDispatchOwnerStartOnce.Do(func() {
		owner := newRequestDispatchOwnerLease(s.Store, s.Log)
		if err := owner.start(time.Now()); err != nil {
			s.requestDispatchOwnerStartErr = err
			return
		}
		s.requestDispatchOwnerMu.Lock()
		s.requestDispatchOwner = owner
		s.requestDispatchOwnerMu.Unlock()
	})
	return s.requestDispatchOwnerStartErr
}

func (s *Service) BindRequestDispatchOwnerContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	if s == nil {
		return nil, nil, errors.New("gateway service is unavailable")
	}
	s.requestDispatchOwnerMu.RLock()
	owner := s.requestDispatchOwner
	s.requestDispatchOwnerMu.RUnlock()
	if owner == nil {
		return nil, nil, errors.New("gateway request dispatch owner is not started")
	}
	return owner.bind(parent)
}

func IsRequestDispatchOwnerLeaseLost(err error) bool {
	return errors.Is(err, ErrRequestDispatchOwnerLeaseLost) || errors.Is(err, storage.ErrGatewayRequestDispatchOwnerFenced)
}

// ScheduleRequestDispatchRecovery queues the exact terminal outcome after the
// live handler has stopped using an attempt. A queue entry is first-wins: once
// an audit has surrendered, defer cleanup must not enqueue a conflicting
// second terminal classification.
func (s *Service) ScheduleRequestDispatchRecovery(job RequestDispatchRecoveryJob) error {
	if s == nil || s.Store == nil || job.DispatchID == 0 {
		return errors.New("gateway request dispatch recovery handoff is unavailable")
	}
	job, err := normalizeRequestDispatchRecoveryJob(job)
	if err != nil {
		s.failRequestDispatchOwner(err)
		return err
	}
	if job.PhysicallyDispatched && job.Log == nil {
		job.Log = requestDispatchRecoveryTerminalLog(job)
	}
	// Retain the drain obligation before the write is attempted. The caller has
	// already surrendered this audit, so releasing its binding must not be read
	// as a durable handoff acknowledgement.
	releaseDrain := s.retainRequestDispatchTerminalHandoff()
	persisted, err := s.persistRequestDispatchTerminalHandoff(job)
	if err != nil {
		// The live handler has already surrendered this audit. If the exact
		// terminal handoff is not durable, fence the owner so orphan recovery
		// can eventually take over instead of leaving an active-owner row stuck,
		// and keep the exact snapshot retryable until it becomes durable.
		s.failRequestDispatchOwner(err)
		s.retainFailedRequestDispatchTerminalHandoff(&requestDispatchTerminalHandoffRetry{
			job:          job,
			releaseDrain: releaseDrain,
		}, err)
		return err
	}
	releaseDrain()
	if !persisted {
		return nil
	}

	s.enqueuePersistedRequestDispatchRecoveryJob(job)
	return nil
}

func (s *Service) persistRequestDispatchTerminalHandoff(job RequestDispatchRecoveryJob) (bool, error) {
	return s.Store.PersistRequestDispatchTerminalHandoff(
		job.DispatchID,
		job.OwnerInstanceID,
		job.OwnerFenceToken,
		job.TerminalKind,
		job.RequestedAt,
		job.StartedAt,
		job.FinishedAt,
		job.Log,
		job.Facts,
		job.TelemetryVersion,
		job.PhysicallyDispatched,
	)
}

// ScheduleLinkedRequestDispatchRecovery persists both rollover outcomes before
// either becomes recoverable. Failure fences the current owner and deliberately
// leaves it stopped; callers may rotate only after this method succeeds.
func (s *Service) ScheduleLinkedRequestDispatchRecovery(previousJob, nextJob RequestDispatchRecoveryJob, knownUnsentSuccessor bool) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway request dispatch recovery handoff is unavailable")
	}
	var err error
	previousJob, err = normalizeRequestDispatchRecoveryJob(previousJob)
	if err != nil {
		s.failRequestDispatchOwner(err)
		return err
	}
	nextJob, err = normalizeRequestDispatchRecoveryJob(nextJob)
	if err != nil {
		s.failRequestDispatchOwner(err)
		return err
	}
	if previousJob.PhysicallyDispatched && previousJob.Log == nil {
		previousJob.Log = requestDispatchRecoveryTerminalLog(previousJob)
	}
	if nextJob.PhysicallyDispatched && nextJob.Log == nil {
		nextJob.Log = requestDispatchRecoveryTerminalLog(nextJob)
	}
	releaseDrain := s.retainRequestDispatchTerminalHandoff()
	err = s.persistLinkedRequestDispatchTerminalHandoffs(previousJob, nextJob, knownUnsentSuccessor)
	if err != nil {
		s.failRequestDispatchOwner(err)
		s.retainFailedRequestDispatchTerminalHandoff(&requestDispatchTerminalHandoffRetry{
			job:                  previousJob,
			linkedJob:            nextJob,
			linked:               true,
			knownUnsentSuccessor: knownUnsentSuccessor,
			releaseDrain:         releaseDrain,
		}, err)
		return err
	}
	releaseDrain()
	s.enqueuePersistedRequestDispatchRecoveryJob(previousJob)
	s.enqueuePersistedRequestDispatchRecoveryJob(nextJob)
	return nil
}

func (s *Service) persistLinkedRequestDispatchTerminalHandoffs(previousJob, nextJob RequestDispatchRecoveryJob, knownUnsentSuccessor bool) error {
	return s.Store.PersistLinkedRequestDispatchTerminalHandoffs(
		requestDispatchTerminalHandoffInput(previousJob),
		requestDispatchTerminalHandoffInput(nextJob),
		knownUnsentSuccessor,
	)
}

func requestDispatchTerminalHandoffInput(job RequestDispatchRecoveryJob) storage.GatewayRequestDispatchTerminalHandoffInput {
	return storage.GatewayRequestDispatchTerminalHandoffInput{
		DispatchID: job.DispatchID, OwnerInstanceID: job.OwnerInstanceID, OwnerFenceToken: job.OwnerFenceToken,
		TerminalKind: job.TerminalKind, RequestedAt: job.RequestedAt,
		StartedAt: job.StartedAt, FinishedAt: job.FinishedAt, Log: job.Log, Facts: job.Facts,
		TelemetryVersion: job.TelemetryVersion, PhysicallyDispatched: job.PhysicallyDispatched,
	}
}

// requestDispatchTerminalHandoffRetry keeps one exact terminal snapshot whose
// durable write failed alive inside this process. The audit row, not the live
// owner lease, authorizes that write, so the retry stays valid after the owner
// rotates; the retained drain obligation is released only once the snapshot is
// durable or the bounded budget ends.
type requestDispatchTerminalHandoffRetry struct {
	job                  RequestDispatchRecoveryJob
	linkedJob            RequestDispatchRecoveryJob
	linked               bool
	knownUnsentSuccessor bool
	releaseDrain         func()
	firstFailureAt       time.Time
	attempts             int
}

func (s *Service) currentRequestDispatchOwner() *requestDispatchOwnerLease {
	if s == nil {
		return nil
	}
	s.requestDispatchOwnerMu.RLock()
	owner := s.requestDispatchOwner
	s.requestDispatchOwnerMu.RUnlock()
	return owner
}

// retainRequestDispatchTerminalHandoff blocks the owner drain gate until the
// caller acknowledges that the exact terminal snapshot is durable.
func (s *Service) retainRequestDispatchTerminalHandoff() func() {
	owner := s.currentRequestDispatchOwner()
	if owner == nil {
		return func() {}
	}
	return owner.retainDurableHandoff()
}

// retainFailedRequestDispatchTerminalHandoff queues a bounded in-process retry
// for an exact terminal snapshot that could not be persisted. It never
// downgrades the snapshot: on abandonment the dispatch simply stays open for
// ordinary orphan recovery, which writes a neutral reconstruction instead of an
// exact fact.
func (s *Service) retainFailedRequestDispatchTerminalHandoff(retry *requestDispatchTerminalHandoffRetry, cause error) {
	if s == nil || retry == nil || retry.job.DispatchID == 0 {
		return
	}
	if retry.releaseDrain == nil {
		retry.releaseDrain = func() {}
	}
	retry.firstFailureAt = time.Now()
	s.requestDispatchHandoffRetryMu.Lock()
	if s.requestDispatchHandoffRetryQueue == nil {
		s.requestDispatchHandoffRetryQueue = make(map[uint64]*requestDispatchTerminalHandoffRetry)
	}
	_, existing := s.requestDispatchHandoffRetryQueue[retry.job.DispatchID]
	accepted := !existing && len(s.requestDispatchHandoffRetryQueue) < requestDispatchTerminalHandoffRetryLimit
	if accepted {
		s.requestDispatchHandoffRetryQueue[retry.job.DispatchID] = retry
	}
	s.requestDispatchHandoffRetryMu.Unlock()
	if !accepted {
		retry.releaseDrain()
		if s.Log != nil && !existing {
			s.Log.Error("gateway request dispatch exact terminal handoff dropped because the retry queue is full",
				"dispatch_id", retry.job.DispatchID, "retry_limit", requestDispatchTerminalHandoffRetryLimit, "cause", cause)
		}
		return
	}
	if s.Log != nil {
		s.Log.Warn("gateway request dispatch exact terminal handoff is not durable yet; retrying with the old owner identity",
			"dispatch_id", retry.job.DispatchID, "linked", retry.linked, "err", cause)
	}
	select {
	case s.requestDispatchRecoveryWake <- struct{}{}:
	default:
	}
}

// retryFailedRequestDispatchTerminalHandoffs re-attempts every exact terminal
// snapshot whose durable write failed. Each attempt targets the immutable owner
// identity captured on the dispatch audit, so rotation does not invalidate it.
func (s *Service) retryFailedRequestDispatchTerminalHandoffs() {
	if s == nil || s.Store == nil {
		return
	}
	s.requestDispatchHandoffRetryMu.Lock()
	pending := make([]*requestDispatchTerminalHandoffRetry, 0, len(s.requestDispatchHandoffRetryQueue))
	for _, retry := range s.requestDispatchHandoffRetryQueue {
		pending = append(pending, retry)
	}
	s.requestDispatchHandoffRetryMu.Unlock()

	for _, retry := range pending {
		retry.attempts++
		var err error
		persisted := true
		if retry.linked {
			err = s.persistLinkedRequestDispatchTerminalHandoffs(retry.job, retry.linkedJob, retry.knownUnsentSuccessor)
		} else {
			persisted, err = s.persistRequestDispatchTerminalHandoff(retry.job)
		}
		if err != nil {
			if time.Since(retry.firstFailureAt) < requestDispatchTerminalHandoffRetryBudget {
				continue
			}
			// Abandon the in-process retry only. Nothing is written: the dispatch
			// stays open and ordinary orphan recovery reconstructs a neutral,
			// clearly non-exact terminal record after its own grace period.
			s.forgetFailedRequestDispatchTerminalHandoff(retry)
			if s.Log != nil {
				s.Log.Error("gateway request dispatch exact terminal handoff abandoned after its bounded retry budget",
					"dispatch_id", retry.job.DispatchID, "linked", retry.linked, "attempts", retry.attempts,
					"retry_budget", requestDispatchTerminalHandoffRetryBudget.String(), "err", err)
			}
			continue
		}
		s.forgetFailedRequestDispatchTerminalHandoff(retry)
		if s.Log != nil {
			s.Log.Info("gateway request dispatch exact terminal handoff became durable after retry",
				"dispatch_id", retry.job.DispatchID, "linked", retry.linked, "attempts", retry.attempts)
		}
		if retry.linked {
			s.enqueuePersistedRequestDispatchRecoveryJob(retry.job)
			s.enqueuePersistedRequestDispatchRecoveryJob(retry.linkedJob)
			continue
		}
		if persisted {
			s.enqueuePersistedRequestDispatchRecoveryJob(retry.job)
		}
	}
}

func (s *Service) forgetFailedRequestDispatchTerminalHandoff(retry *requestDispatchTerminalHandoffRetry) {
	s.requestDispatchHandoffRetryMu.Lock()
	if current, ok := s.requestDispatchHandoffRetryQueue[retry.job.DispatchID]; ok && current == retry {
		delete(s.requestDispatchHandoffRetryQueue, retry.job.DispatchID)
	}
	s.requestDispatchHandoffRetryMu.Unlock()
	retry.releaseDrain()
}

func (s *Service) enqueuePersistedRequestDispatchRecoveryJob(job RequestDispatchRecoveryJob) {
	s.requestDispatchRecoveryMu.Lock()
	if s.requestDispatchRecoveryQueue == nil {
		s.requestDispatchRecoveryQueue = make(map[uint64]*requestDispatchRecoveryHandoff)
	}
	if _, ok := s.requestDispatchRecoveryQueue[job.DispatchID]; !ok && len(s.requestDispatchRecoveryQueue) < requestDispatchRecoveryHandoffLimit {
		s.requestDispatchRecoveryQueue[job.DispatchID] = &requestDispatchRecoveryHandoff{job: job}
	}
	s.requestDispatchRecoveryMu.Unlock()
	select {
	case s.requestDispatchRecoveryWake <- struct{}{}:
	default:
	}
}

// normalizeRequestDispatchRecoveryJob validates a live handler's surrender. The
// owner identity is mandatory here because it is what authorizes creating the
// durable handoff row for that exact dispatch.
func normalizeRequestDispatchRecoveryJob(job RequestDispatchRecoveryJob) (RequestDispatchRecoveryJob, error) {
	job.OwnerInstanceID = strings.TrimSpace(job.OwnerInstanceID)
	if job.OwnerInstanceID == "" || job.OwnerFenceToken == 0 {
		return job, errors.New("gateway request dispatch recovery owner identity is invalid")
	}
	return normalizeDurableRequestDispatchRecoveryJob(job)
}

// normalizeDurableRequestDispatchRecoveryJob normalizes an outcome that is
// already durable. Consuming a persisted handoff is authorized by the row and
// its dispatch audit, not by a live owner identity, so the worker must not
// require an identity the durable record never carried.
func normalizeDurableRequestDispatchRecoveryJob(job RequestDispatchRecoveryJob) (RequestDispatchRecoveryJob, error) {
	job.TerminalKind = strings.TrimSpace(job.TerminalKind)
	job.OwnerInstanceID = strings.TrimSpace(job.OwnerInstanceID)
	if job.DispatchID == 0 || job.TerminalKind == "" || len(job.TerminalKind) > 64 {
		return job, errors.New("gateway request dispatch recovery terminal kind is invalid")
	}
	if job.RequestedAt.IsZero() {
		job.RequestedAt = time.Now().UTC()
	} else {
		job.RequestedAt = job.RequestedAt.UTC()
	}
	if job.FinishedAt.IsZero() {
		job.FinishedAt = job.RequestedAt
	} else {
		job.FinishedAt = job.FinishedAt.UTC()
	}
	if job.StartedAt.IsZero() {
		job.StartedAt = job.FinishedAt
	} else {
		job.StartedAt = job.StartedAt.UTC()
	}
	job.Log = cloneRequestDispatchRecoveryLog(job.Log)
	return job, nil
}

func cloneRequestDispatchRecoveryLog(log *storage.GatewayRequestLog) *storage.GatewayRequestLog {
	if log == nil {
		return nil
	}
	copy := *log
	if log.RawGroupRatio != nil {
		value := *log.RawGroupRatio
		copy.RawGroupRatio = &value
	}
	if log.RawCompletionRatio != nil {
		value := *log.RawCompletionRatio
		copy.RawCompletionRatio = &value
	}
	if log.FundingFactor != nil {
		value := *log.FundingFactor
		copy.FundingFactor = &value
	}
	if log.RawCost != nil {
		value := *log.RawCost
		copy.RawCost = &value
	}
	if log.UpstreamUsageID != nil {
		value := *log.UpstreamUsageID
		copy.UpstreamUsageID = &value
	}
	if log.CostReconcileAt != nil {
		value := *log.CostReconcileAt
		copy.CostReconcileAt = &value
	}
	if log.OperationUnits != nil {
		value := *log.OperationUnits
		copy.OperationUnits = &value
	}
	return &copy
}

func (s *Service) processRequestDispatchRecoveryJob(job RequestDispatchRecoveryJob) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway request dispatch recovery handoff is unavailable")
	}
	job, err := normalizeDurableRequestDispatchRecoveryJob(job)
	if err != nil {
		return err
	}
	if job.Log == nil {
		if job.PhysicallyDispatched {
			job.Log = requestDispatchRecoveryTerminalLog(job)
		} else {
			err = s.Store.CompleteRequestDispatch(job.DispatchID, job.TerminalKind, job.FinishedAt)
			if errors.Is(err, storage.ErrGatewayRequestDispatchOwnerFenced) || errors.Is(err, storage.ErrGatewayRequestDispatchRecoveryRequested) {
				err = s.Store.CompleteRecoveredRequestDispatch(job.DispatchID, job.TerminalKind, job.RequestedAt, job.FinishedAt)
			}
			return err
		}
	}

	log := cloneRequestDispatchRecoveryLog(job.Log)
	created, err := s.Store.FinalizeRequestDispatchWithFactsResult(job.DispatchID, log, job.TerminalKind, job.Facts, job.FinishedAt)
	if errors.Is(err, storage.ErrGatewayRequestDispatchOwnerFenced) || errors.Is(err, storage.ErrGatewayRequestDispatchRecoveryRequested) {
		created, err = s.Store.FinalizeRecoveredRequestDispatchWithFactsResult(job.DispatchID, log, job.TerminalKind, job.Facts, job.RequestedAt, job.FinishedAt)
	}
	if err != nil {
		return err
	}
	if created {
		s.recordRoutingLatency(log)
	}
	s.ReconcileRequestCost(log, job.StartedAt, job.FinishedAt)
	return nil
}

func (s *Service) processDurableRequestDispatchRecoveryHandoff(handoff storage.GatewayRequestDispatchTerminalHandoff) error {
	decoded, err := storage.DecodeGatewayRequestDispatchTerminalHandoff(handoff)
	if err != nil {
		return err
	}
	job := RequestDispatchRecoveryJob{
		DispatchID:           decoded.DispatchID,
		TerminalKind:         decoded.TerminalKind,
		RequestedAt:          decoded.RequestedAt,
		StartedAt:            decoded.StartedAt,
		FinishedAt:           decoded.FinishedAt,
		Log:                  decoded.Log,
		Facts:                decoded.Facts,
		TelemetryVersion:     decoded.TelemetryVersion,
		PhysicallyDispatched: decoded.PhysicallyDispatched,
	}
	job, err = normalizeDurableRequestDispatchRecoveryJob(job)
	if err != nil {
		return err
	}
	resolved, err := s.Store.ResolveUnconfirmedRequestDispatchTerminalHandoff(job.DispatchID, job.TerminalKind, job.RequestedAt, job.FinishedAt)
	if err != nil {
		return err
	}
	if !resolved {
		if job.Log == nil {
			err = s.Store.CompleteDurableRequestDispatchHandoff(job.DispatchID, job.TerminalKind, job.RequestedAt, job.FinishedAt)
		} else {
			log := cloneRequestDispatchRecoveryLog(job.Log)
			created := false
			created, err = s.Store.FinalizeDurableRequestDispatchHandoffResult(job.DispatchID, log, job.TerminalKind, job.Facts, job.RequestedAt, job.FinishedAt)
			if err == nil {
				if created {
					s.recordRoutingLatency(log)
				}
				s.ReconcileRequestCost(log, job.StartedAt, job.FinishedAt)
			}
		}
		if err != nil {
			return err
		}
	}
	err = s.Store.DeleteRequestDispatchTerminalHandoff(job.DispatchID)
	if errors.Is(err, storage.ErrGatewayRequestDispatchManualReviewRequired) {
		return nil
	}
	return err
}

func requestDispatchRecoveryTerminalLog(job RequestDispatchRecoveryJob) *storage.GatewayRequestLog {
	statusCode := http.StatusBadGateway
	errorText := "request completion could not be confirmed"
	terminalFailureCode := ""
	if job.TelemetryVersion != 0 {
		terminalFailureCode = requestDispatchRecoveryRouteReason
	}
	if job.TerminalKind == "client_canceled" {
		statusCode = 499
		errorText = "客户端取消"
		if job.TelemetryVersion != 0 {
			terminalFailureCode = "client_canceled"
		}
	}
	latencyMs := requestDispatchRecoveryLatency(job.StartedAt, job.FinishedAt)
	reconcileAt := InitialRequestCostReconcileAt(job.FinishedAt)
	return &storage.GatewayRequestLog{
		StatusCode:          statusCode,
		LatencyMs:           latencyMs,
		CostReconcileAt:     &reconcileAt,
		RouteReason:         requestDispatchRecoveryRouteReason,
		TerminalFailureCode: terminalFailureCode,
		Error:               errorText,
		CreatedAt:           job.FinishedAt,
	}
}

func (s *Service) failRequestDispatchOwner(reason error) {
	if s == nil {
		return
	}
	s.requestDispatchOwnerMu.RLock()
	owner := s.requestDispatchOwner
	s.requestDispatchOwnerMu.RUnlock()
	if owner == nil {
		return
	}
	owner.beginDrain()
	owner.stopLoop()
	owner.lose(reason)
}

func (s *Service) restartFailedRequestDispatchOwner() {
	if s == nil {
		return
	}
	s.requestDispatchOwnerMu.RLock()
	owner := s.requestDispatchOwner
	s.requestDispatchOwnerMu.RUnlock()
	if owner == nil || context.Cause(owner.context) == nil {
		return
	}
	s.restartRequestDispatchOwner(owner)
}

// RestartFailedRequestDispatchOwner starts replacement only after the API has
// durably surrendered every linked audit involved in the failed handler.
func (s *Service) RestartFailedRequestDispatchOwner() {
	s.restartFailedRequestDispatchOwner()
}

func (s *Service) restartRequestDispatchOwner(failed *requestDispatchOwnerLease) {
	if s == nil || s.Store == nil || failed == nil {
		return
	}
	s.requestDispatchOwnerRecoveryMu.Lock()
	if s.requestDispatchOwnerStopping || s.requestDispatchOwnerRecovering {
		s.requestDispatchOwnerRecoveryMu.Unlock()
		return
	}
	s.requestDispatchOwnerRecovering = true
	s.requestDispatchOwnerRecoveryMu.Unlock()

	go func() {
		defer func() {
			s.requestDispatchOwnerRecoveryMu.Lock()
			s.requestDispatchOwnerRecovering = false
			s.requestDispatchOwnerRecoveryMu.Unlock()
		}()
		drained := failed.beginDrain()
		drainMaximum := failed.drainMaximum
		if drainMaximum <= 0 {
			drainMaximum = requestDispatchOwnerDrainMaximum
		}
		drainDeadline := time.NewTimer(drainMaximum)
		defer drainDeadline.Stop()
		var forcedDrain *storage.GatewayRequestDispatchForcedDrain
		for {
			s.requestDispatchOwnerRecoveryMu.Lock()
			stopping := s.requestDispatchOwnerStopping
			s.requestDispatchOwnerRecoveryMu.Unlock()
			if stopping {
				return
			}
			select {
			case <-drained:
			case <-drainDeadline.C:
				// A transport or body that ignores cancellation must not keep this
				// process from ever taking a new incarnation. Degrade in a bounded,
				// audited way: stop gating rotation, but never synthesize a terminal
				// outcome for what is still outstanding.
				forced := failed.forceDrain()
				forcedDrain = &forced
				if s.Log != nil {
					s.Log.Error("gateway request dispatch owner drain exceeded its bounded deadline; rotating without full acknowledgement",
						"drain_maximum", drainMaximum.String(),
						"active_bindings", forced.ActiveBindings,
						"pending_durable_handoffs", forced.PendingHandoffs)
				}
			case <-time.After(requestDispatchOwnerRetryInterval):
				continue
			}
			now := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), requestDispatchOwnerOperationTimeout)
			var err error
			if forcedDrain != nil {
				err = s.Store.RotateForcedDrainRequestDispatchOwner(ctx, now.UTC(), now.UTC().Add(storage.GatewayRequestDispatchOwnerLeaseDuration), *forcedDrain)
			} else {
				err = s.Store.RotateRequestDispatchOwner(ctx, now.UTC(), now.UTC().Add(storage.GatewayRequestDispatchOwnerLeaseDuration))
			}
			cancel()
			if err == nil {
				replacement := newRequestDispatchOwnerLease(s.Store, s.Log)
				err = replacement.start(time.Now())
				if err == nil {
					s.requestDispatchOwnerRecoveryMu.Lock()
					stopping = s.requestDispatchOwnerStopping
					s.requestDispatchOwnerRecoveryMu.Unlock()
					if stopping {
						stopCtx, stopCancel := context.WithTimeout(context.Background(), requestDispatchOwnerOperationTimeout)
						_ = replacement.stop(stopCtx, time.Now())
						stopCancel()
						return
					}
					s.requestDispatchOwnerMu.Lock()
					if s.requestDispatchOwner == failed {
						s.requestDispatchOwner = replacement
						replacement = nil
					}
					s.requestDispatchOwnerMu.Unlock()
					if replacement != nil {
						stopCtx, stopCancel := context.WithTimeout(context.Background(), requestDispatchOwnerOperationTimeout)
						_ = replacement.stop(stopCtx, time.Now())
						stopCancel()
					}
					return
				}
			}
			if s.Log != nil {
				s.Log.Error("replace gateway request dispatch owner after fence failed; will retry", "err", err)
			}
			timer := time.NewTimer(requestDispatchOwnerRetryInterval)
			<-timer.C
		}
	}()
}

func (s *Service) flushRequestDispatchRecoveryHandoffs() {
	if s == nil || s.Store == nil {
		return
	}
	handoffs, err := s.Store.ListRequestDispatchTerminalHandoffs(requestDispatchRecoveryHandoffBatch)
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("list gateway request dispatch terminal handoffs failed; will retry", "err", err)
		}
		return
	}
	for index := range handoffs {
		handoff := handoffs[index]
		if err := s.processDurableRequestDispatchRecoveryHandoff(handoff); err != nil {
			if s.Log != nil {
				s.Log.Warn("gateway request dispatch finalization retry failed; will retry", "dispatch_id", handoff.DispatchID, "err", err)
			}
			continue
		}
		s.requestDispatchRecoveryMu.Lock()
		delete(s.requestDispatchRecoveryQueue, handoff.DispatchID)
		s.requestDispatchRecoveryMu.Unlock()
	}
}

// startRequestDispatchRecoveryWorker runs one bounded pass during startup and
// keeps retrying every ten seconds. Owner renewal runs independently, so a
// slow bounded recovery query cannot consume the heartbeat safety margin.
func (s *Service) startRequestDispatchRecoveryWorker() {
	if s == nil || s.Store == nil {
		return
	}
	if err := s.StartRequestDispatchOwner(); err != nil {
		if s.Log != nil {
			s.Log.Error("start gateway request dispatch owner failed", "err", err)
		}
		return
	}
	s.requestDispatchRecoveryOnce.Do(func() {
		startupAt := time.Now().UTC()
		s.runRequestDispatchRecovery(startupAt, startupAt)
		go func() {
			ticker := time.NewTicker(requestDispatchRecoveryInterval)
			defer ticker.Stop()
			for {
				select {
				case now := <-ticker.C:
					s.runRequestDispatchRecovery(now, startupAt)
				case <-s.requestDispatchRecoveryWake:
					s.runRequestDispatchRecovery(time.Now().UTC(), startupAt)
				}
			}
		}()
	})
}

// StopRequestDispatchOwner permanently fences this process incarnation after
// the HTTP server has stopped accepting and draining requests.
func (s *Service) StopRequestDispatchOwner(ctx context.Context, now time.Time) error {
	if s == nil {
		return nil
	}
	s.requestDispatchOwnerRecoveryMu.Lock()
	s.requestDispatchOwnerStopping = true
	s.requestDispatchOwnerRecoveryMu.Unlock()
	s.requestDispatchOwnerMu.RLock()
	owner := s.requestDispatchOwner
	s.requestDispatchOwnerMu.RUnlock()
	if owner == nil {
		return nil
	}
	return owner.stop(ctx, now)
}

func (s *Service) runRequestDispatchRecovery(now, maximumDispatchedAt time.Time) {
	s.retryFailedRequestDispatchTerminalHandoffs()
	// A durable-handoff failure fences this incarnation from anywhere, including
	// paths that have no live handler left to ask for a replacement. Without a
	// periodic attempt the process would keep refusing every new binding.
	s.restartFailedRequestDispatchOwner()
	s.flushRequestDispatchRecoveryHandoffs()
	s.flushRoutingHealthSuccessOutbox()
	if err := s.recoverRequestDispatchOrphansWithMaximumDispatchedAt(now, maximumDispatchedAt); err != nil && s.Log != nil {
		s.Log.Warn("gateway request dispatch recovery failed", "err", err)
	}
}
