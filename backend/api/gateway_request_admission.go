package api

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	errGatewayRequestAdmissionInvalid   = errors.New("invalid gateway request admission")
	errGatewayRequestBodyTooLarge       = errors.New("gateway request body exceeds configured limit")
	errGatewayRequestAdmissionBusy      = errors.New("gateway request admission is temporarily busy")
	errGatewayRequestAdmissionQueueFull = errors.New("gateway request admission queue is full")
	// Queue wait expiry is a capacity signal rather than a caller cancellation.
	// It deliberately wraps Busy so the data-plane handler preserves its bounded
	// 503 + Retry-After response without treating an internal deadline as 499.
	errGatewayRequestAdmissionQueueWaitExceeded = fmt.Errorf("%w: maximum queue wait exceeded", errGatewayRequestAdmissionBusy)
)

const (
	// A normal request retains its raw body while several bounded protocol
	// parsers inspect it. A mapping request additionally builds a decoded JSON
	// object graph and a rewritten body before the original can be dropped.
	// These weights deliberately budget peak memory rather than wire bytes.
	gatewayRequestBodyStandardMultiplier int64 = 4
	gatewayRequestBodyRewriteMultiplier  int64 = 8
	gatewayRequestBodyStandardOverhead   int64 = 64 << 10
	gatewayRequestBodyRewriteOverhead    int64 = 256 << 10
	gatewayRequestAdmissionMaxInt64            = int64(^uint64(0) >> 1)

	// The fallback exists for isolated tests and direct handler construction.
	// cmd/server always injects the normalized runtime configuration instead.
	gatewayRequestAdmissionFallbackMaxInFlightRequests        int64 = 16
	gatewayRequestAdmissionFallbackMaxInFlightBodyBytes             = int64(256 << 20)
	gatewayRequestAdmissionFallbackMaxInFlightRequestsPerKey  int64 = 4
	gatewayRequestAdmissionFallbackMaxInFlightBodyBytesPerKey       = int64(96 << 20)
	gatewayRequestAdmissionFallbackMaxQueuedRequests          int64 = 32
	gatewayRequestAdmissionFallbackMaxQueuedRequestsPerKey    int64 = 8
	gatewayRequestAdmissionFallbackMaxQueueWait                     = time.Second
)

// GatewayRequestAdmissionLimits bounds both retained request bodies and the
// number of handlers allowed to keep them alive while an upstream response is
// streaming. Queue bounds are separate so authenticated callers waiting for a
// lease cannot grow per-process or per-key state without limit.
type GatewayRequestAdmissionLimits struct {
	MaxInFlightRequests        int64
	MaxInFlightBodyBytes       int64
	MaxInFlightRequestsPerKey  int64
	MaxInFlightBodyBytesPerKey int64
	MaxQueuedRequests          int64
	MaxQueuedRequestsPerKey    int64
	MaxQueueWait               time.Duration
}

type gatewayRequestAdmissionLimits = GatewayRequestAdmissionLimits

type gatewayRequestAdmissionBodyProfile struct {
	BodyMultiplier int64
	FixedOverhead  int64
}

var (
	gatewayRequestAdmissionStandardProfile = gatewayRequestAdmissionBodyProfile{
		BodyMultiplier: gatewayRequestBodyStandardMultiplier,
		FixedOverhead:  gatewayRequestBodyStandardOverhead,
	}
	gatewayRequestAdmissionRewriteProfile = gatewayRequestAdmissionBodyProfile{
		BodyMultiplier: gatewayRequestBodyRewriteMultiplier,
		FixedOverhead:  gatewayRequestBodyRewriteOverhead,
	}
)

func gatewayRequestAdmissionFallbackLimits() gatewayRequestAdmissionLimits {
	return gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        gatewayRequestAdmissionFallbackMaxInFlightRequests,
		MaxInFlightBodyBytes:       gatewayRequestAdmissionFallbackMaxInFlightBodyBytes,
		MaxInFlightRequestsPerKey:  gatewayRequestAdmissionFallbackMaxInFlightRequestsPerKey,
		MaxInFlightBodyBytesPerKey: gatewayRequestAdmissionFallbackMaxInFlightBodyBytesPerKey,
		MaxQueuedRequests:          gatewayRequestAdmissionFallbackMaxQueuedRequests,
		MaxQueuedRequestsPerKey:    gatewayRequestAdmissionFallbackMaxQueuedRequestsPerKey,
		MaxQueueWait:               gatewayRequestAdmissionFallbackMaxQueueWait,
	}
}

type gatewayRequestAdmission struct {
	mu sync.Mutex

	limits gatewayRequestAdmissionLimits

	inFlightRequests  int64
	inFlightBodyBytes int64
	queuedRequests    int64
	keys              map[uint]*gatewayRequestAdmissionKeyState
	waiters           list.List
}

type gatewayRequestAdmissionKeyState struct {
	inFlightRequests  int64
	inFlightBodyBytes int64
	queuedRequests    int64
}

type gatewayRequestAdmissionWaiter struct {
	ctx       context.Context
	keyID     uint
	bodyBytes int64
	deadline  time.Time
	ready     chan struct{}
	element   *list.Element
	granted   bool
	grantedAt time.Time
}

// gatewayRequestAdmissionLease contains accounting facts only. It never owns
// or references the request body. The caller must defer Release immediately
// after Acquire succeeds and keep the lease through the complete response
// lifetime, including long streams.
type gatewayRequestAdmissionLease struct {
	mu sync.Mutex

	admission *gatewayRequestAdmission
	keyID     uint
	bodyBytes int64
	released  bool
}

type gatewayRequestAdmissionSnapshot struct {
	InFlightRequests     int64
	InFlightBodyBytes    int64
	QueuedRequests       int64
	KeyInFlightRequests  int64
	KeyInFlightBodyBytes int64
	KeyQueuedRequests    int64
	TrackedKeys          int
}

func newGatewayRequestAdmission(limits gatewayRequestAdmissionLimits) (*gatewayRequestAdmission, error) {
	if limits.MaxInFlightRequests <= 0 ||
		limits.MaxInFlightBodyBytes <= 0 ||
		limits.MaxInFlightRequestsPerKey <= 0 ||
		limits.MaxInFlightBodyBytesPerKey <= 0 ||
		limits.MaxQueuedRequests < 0 ||
		limits.MaxQueuedRequestsPerKey < 0 ||
		limits.MaxQueueWait <= 0 {
		return nil, fmt.Errorf("%w: limits and maximum queue wait must be positive and queue limits must be non-negative", errGatewayRequestAdmissionInvalid)
	}
	if limits.MaxInFlightRequestsPerKey > limits.MaxInFlightRequests ||
		limits.MaxInFlightBodyBytesPerKey > limits.MaxInFlightBodyBytes ||
		limits.MaxQueuedRequestsPerKey > limits.MaxQueuedRequests {
		return nil, fmt.Errorf("%w: per-key limits cannot exceed process limits", errGatewayRequestAdmissionInvalid)
	}
	return &gatewayRequestAdmission{
		limits: limits,
		keys:   make(map[uint]*gatewayRequestAdmissionKeyState),
	}, nil
}

func mustNewGatewayRequestAdmission(limits gatewayRequestAdmissionLimits) *gatewayRequestAdmission {
	admission, err := newGatewayRequestAdmission(limits)
	if err != nil {
		panic(err)
	}
	return admission
}

// ConfigureGatewayRequestAdmission installs one process-level controller on
// the dependency graph. It must be called before handlers begin serving.
func (d *Deps) ConfigureGatewayRequestAdmission(limits GatewayRequestAdmissionLimits) error {
	if d == nil {
		return fmt.Errorf("%w: dependencies are required", errGatewayRequestAdmissionInvalid)
	}
	admission, err := newGatewayRequestAdmission(limits)
	if err != nil {
		return err
	}
	installed := false
	d.gatewayRequestAdmissionOnce.Do(func() {
		if d.gatewayRequestAdmission == nil {
			d.gatewayRequestAdmission = admission
			installed = true
		}
	})
	if !installed {
		return fmt.Errorf("%w: admission is already configured", errGatewayRequestAdmissionInvalid)
	}
	return nil
}

// Acquire reserves one request slot plus a conservative peak-memory weight for
// the body. A known Content-Length is scaled for parsing, and a model rewrite
// uses the larger raw + decoded + rewritten profile. An unknown/chunked body
// starts with only the profile's fixed scratch overhead and grows its lease as
// bytes are read.
func (a *gatewayRequestAdmission) Acquire(ctx context.Context, keyID uint, contentLength, maxBodyBytes int64, profile gatewayRequestAdmissionBodyProfile) (*gatewayRequestAdmissionLease, error) {
	if a == nil || ctx == nil || keyID == 0 {
		return nil, fmt.Errorf("%w: admission, context, and downstream key are required", errGatewayRequestAdmissionInvalid)
	}
	bodyBytes, err := gatewayRequestBodyReservationBytes(contentLength, maxBodyBytes, profile)
	if err != nil {
		return nil, err
	}
	if bodyBytes > a.limits.MaxInFlightBodyBytes || bodyBytes > a.limits.MaxInFlightBodyBytesPerKey {
		return nil, fmt.Errorf("%w: weighted body reservation %d exceeds process or downstream-key limit", errGatewayRequestBodyTooLarge, bodyBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, gatewayRequestAdmissionContextError(ctx)
	}

	a.mu.Lock()
	if err := ctx.Err(); err != nil {
		a.mu.Unlock()
		return nil, gatewayRequestAdmissionContextError(ctx)
	}
	// Remove abandoned waiters before deciding whether this request can use a
	// now-free direct slot. This preserves FIFO for live waiters without letting
	// an expired or canceled head force a new caller into a full queue.
	a.drainLocked()
	if err := ctx.Err(); err != nil {
		a.mu.Unlock()
		return nil, gatewayRequestAdmissionContextError(ctx)
	}
	if a.waiters.Len() == 0 && a.canGrantProcessLocked(bodyBytes) && a.canGrantKeyLocked(keyID, bodyBytes) {
		a.reserveLocked(keyID, bodyBytes)
		// Cancellation can race the pre-lock check. Recheck while the grant is
		// still private so a canceled caller never receives a lease.
		if err := ctx.Err(); err != nil {
			a.releaseReservationLocked(keyID, bodyBytes)
			a.drainLocked()
			a.mu.Unlock()
			return nil, gatewayRequestAdmissionContextError(ctx)
		}
		a.mu.Unlock()
		return newGatewayRequestAdmissionLease(a, keyID, bodyBytes), nil
	}

	state := a.keys[keyID]
	if a.limits.MaxQueuedRequests == 0 ||
		a.limits.MaxQueuedRequestsPerKey == 0 ||
		a.queuedRequests >= a.limits.MaxQueuedRequests ||
		(state != nil && state.queuedRequests >= a.limits.MaxQueuedRequestsPerKey) {
		if err := ctx.Err(); err != nil {
			a.mu.Unlock()
			return nil, gatewayRequestAdmissionContextError(ctx)
		}
		a.mu.Unlock()
		return nil, errGatewayRequestAdmissionQueueFull
	}
	if err := ctx.Err(); err != nil {
		a.mu.Unlock()
		return nil, gatewayRequestAdmissionContextError(ctx)
	}
	if state == nil {
		state = &gatewayRequestAdmissionKeyState{}
		a.keys[keyID] = state
	}
	waiter := &gatewayRequestAdmissionWaiter{
		ctx:       ctx,
		keyID:     keyID,
		bodyBytes: bodyBytes,
		// Queue time starts at the actual enqueue boundary, not after an
		// opportunistic drain or timer allocation while holding this mutex.
		deadline: time.Now().Add(a.limits.MaxQueueWait),
		ready:    make(chan struct{}),
	}
	waiter.element = a.waiters.PushBack(waiter)
	a.queuedRequests++
	state.queuedRequests++
	a.drainLocked()
	if granted, waitErr := a.resolveWaiterLocked(waiter); waitErr != nil {
		a.mu.Unlock()
		return nil, waitErr
	} else if granted {
		a.mu.Unlock()
		return newGatewayRequestAdmissionLease(a, keyID, bodyBytes), nil
	}
	queueWait := time.NewTimer(time.Until(waiter.deadline))
	a.mu.Unlock()
	defer queueWait.Stop()

	for {
		select {
		case <-waiter.ready:
		case <-ctx.Done():
		case <-queueWait.C:
		}

		a.mu.Lock()
		granted, waitErr := a.resolveWaiterLocked(waiter)
		a.mu.Unlock()
		if waitErr != nil {
			return nil, waitErr
		}
		if granted {
			// The deadline is checked when drainLocked grants the lease, not
			// when this goroutine gets scheduled after ready is closed. A valid
			// grant must survive delayed scheduling.
			return newGatewayRequestAdmissionLease(a, keyID, bodyBytes), nil
		}
	}
}

func gatewayRequestBodyReservationBytes(contentLength, maxBodyBytes int64, profile gatewayRequestAdmissionBodyProfile) (int64, error) {
	if maxBodyBytes <= 0 {
		return 0, fmt.Errorf("%w: body limit must be positive", errGatewayRequestAdmissionInvalid)
	}
	if profile.BodyMultiplier <= 0 || profile.FixedOverhead < 0 {
		return 0, fmt.Errorf("%w: body profile is invalid", errGatewayRequestAdmissionInvalid)
	}
	bodyBytes := contentLength
	if contentLength < 0 {
		bodyBytes = 0
	}
	if bodyBytes > maxBodyBytes {
		return 0, fmt.Errorf("%w: content length %d exceeds %d", errGatewayRequestBodyTooLarge, contentLength, maxBodyBytes)
	}
	multiplier := profile.BodyMultiplier
	overhead := profile.FixedOverhead
	if bodyBytes > (gatewayRequestAdmissionMaxInt64-overhead)/multiplier {
		return 0, fmt.Errorf("%w: weighted body reservation overflows", errGatewayRequestBodyTooLarge)
	}
	return bodyBytes*multiplier + overhead, nil
}

func newGatewayRequestAdmissionLease(admission *gatewayRequestAdmission, keyID uint, bodyBytes int64) *gatewayRequestAdmissionLease {
	return &gatewayRequestAdmissionLease{admission: admission, keyID: keyID, bodyBytes: bodyBytes}
}

// GrowTo atomically raises this lease's weighted body reservation. A target
// that can never fit the configured process or per-key limit is a permanent
// request-size failure. Capacity occupied by other live requests is transient
// and fails promptly rather than blocking a handler that already read bytes.
func (l *gatewayRequestAdmissionLease) GrowTo(bodyBytes int64) error {
	if l == nil || l.admission == nil || bodyBytes < 0 {
		return fmt.Errorf("%w: lease and non-negative body reservation are required", errGatewayRequestAdmissionInvalid)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return fmt.Errorf("%w: lease is already released", errGatewayRequestAdmissionInvalid)
	}
	if bodyBytes <= l.bodyBytes {
		return nil
	}
	if err := l.admission.grow(l.keyID, l.bodyBytes, bodyBytes); err != nil {
		return err
	}
	l.bodyBytes = bodyBytes
	return nil
}

// Release is idempotent so cleanup paths can safely converge on the same
// lease without underflowing global or per-key accounting. It releases the
// final amount after any successful GrowTo calls.
func (l *gatewayRequestAdmissionLease) Release() {
	if l == nil || l.admission == nil {
		return
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	bodyBytes := l.bodyBytes
	l.mu.Unlock()
	l.admission.release(l.keyID, bodyBytes)
}

func (a *gatewayRequestAdmission) grow(keyID uint, currentBodyBytes, targetBodyBytes int64) error {
	if targetBodyBytes > a.limits.MaxInFlightBodyBytes || targetBodyBytes > a.limits.MaxInFlightBodyBytesPerKey {
		return fmt.Errorf("%w: weighted body reservation %d exceeds process or downstream-key limit", errGatewayRequestBodyTooLarge, targetBodyBytes)
	}
	delta := targetBodyBytes - currentBodyBytes
	if delta <= 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.keys[keyID]
	if state == nil || state.inFlightRequests <= 0 || state.inFlightBodyBytes < currentBodyBytes || a.inFlightRequests <= 0 || a.inFlightBodyBytes < currentBodyBytes {
		return fmt.Errorf("%w: lease accounting is no longer active", errGatewayRequestAdmissionInvalid)
	}
	if delta > a.limits.MaxInFlightBodyBytes-a.inFlightBodyBytes || delta > a.limits.MaxInFlightBodyBytesPerKey-state.inFlightBodyBytes {
		return errGatewayRequestAdmissionBusy
	}
	a.inFlightBodyBytes += delta
	state.inFlightBodyBytes += delta
	return nil
}

func (a *gatewayRequestAdmission) release(keyID uint, bodyBytes int64) {
	a.mu.Lock()
	a.releaseReservationLocked(keyID, bodyBytes)
	a.drainLocked()
	a.mu.Unlock()
}

func (a *gatewayRequestAdmission) cancelWaiter(waiter *gatewayRequestAdmissionWaiter) {
	a.mu.Lock()
	a.cancelWaiterLocked(waiter)
	a.drainLocked()
	a.mu.Unlock()
}

// resolveWaiterLocked atomically settles the one waiter owned by an Acquire
// call. Cancellation wins every race. A queue deadline only governs when a
// lease may be granted; once drainLocked has recorded an in-time grant, later
// goroutine scheduling must not revoke it.
func (a *gatewayRequestAdmission) resolveWaiterLocked(waiter *gatewayRequestAdmissionWaiter) (bool, error) {
	if waiter == nil || waiter.ctx == nil {
		return false, fmt.Errorf("%w: queued waiter is invalid", errGatewayRequestAdmissionInvalid)
	}
	if err := waiter.ctx.Err(); err != nil {
		a.cancelWaiterLocked(waiter)
		a.drainLocked()
		return false, gatewayRequestAdmissionContextError(waiter.ctx)
	}
	if waiter.granted {
		if waiter.grantedAt.IsZero() || !waiter.grantedAt.Before(waiter.deadline) {
			a.cancelWaiterLocked(waiter)
			a.drainLocked()
			return false, errGatewayRequestAdmissionQueueWaitExceeded
		}
		return true, nil
	}
	if !time.Now().Before(waiter.deadline) {
		// Cancellation can arrive after the initial check but before this
		// timeout decision. The caller's context still wins over controller
		// pressure whenever both become ready together.
		if err := waiter.ctx.Err(); err != nil {
			a.cancelWaiterLocked(waiter)
			a.drainLocked()
			return false, gatewayRequestAdmissionContextError(waiter.ctx)
		}
		a.cancelWaiterLocked(waiter)
		a.drainLocked()
		return false, errGatewayRequestAdmissionQueueWaitExceeded
	}
	return false, nil
}

func (a *gatewayRequestAdmission) cancelWaiterLocked(waiter *gatewayRequestAdmissionWaiter) {
	if waiter == nil {
		return
	}
	if waiter.granted {
		// Grant and cancellation can become ready together. If cancellation wins
		// before Acquire transfers the lease to its caller, undo the reservation.
		waiter.granted = false
		waiter.grantedAt = time.Time{}
		a.releaseReservationLocked(waiter.keyID, waiter.bodyBytes)
		return
	}
	if waiter.element != nil {
		a.removeQueuedWaiterLocked(waiter)
	}
}

func (a *gatewayRequestAdmission) canGrantProcessLocked(bodyBytes int64) bool {
	return a.inFlightRequests < a.limits.MaxInFlightRequests &&
		bodyBytes <= a.limits.MaxInFlightBodyBytes-a.inFlightBodyBytes
}

func (a *gatewayRequestAdmission) canGrantKeyLocked(keyID uint, bodyBytes int64) bool {
	state := a.keys[keyID]
	if state == nil {
		return true
	}
	return state.inFlightRequests < a.limits.MaxInFlightRequestsPerKey &&
		bodyBytes <= a.limits.MaxInFlightBodyBytesPerKey-state.inFlightBodyBytes
}

func (a *gatewayRequestAdmission) reserveLocked(keyID uint, bodyBytes int64) {
	state := a.keys[keyID]
	if state == nil {
		state = &gatewayRequestAdmissionKeyState{}
		a.keys[keyID] = state
	}
	a.inFlightRequests++
	a.inFlightBodyBytes += bodyBytes
	state.inFlightRequests++
	state.inFlightBodyBytes += bodyBytes
}

func (a *gatewayRequestAdmission) releaseReservationLocked(keyID uint, bodyBytes int64) {
	state := a.keys[keyID]
	if state == nil || state.inFlightRequests <= 0 || state.inFlightBodyBytes < bodyBytes || a.inFlightRequests <= 0 || a.inFlightBodyBytes < bodyBytes {
		// The lease serializes GrowTo and Release. Keep accounting fail-closed if
		// an internal invariant is ever violated.
		return
	}
	a.inFlightRequests--
	a.inFlightBodyBytes -= bodyBytes
	state.inFlightRequests--
	state.inFlightBodyBytes -= bodyBytes
	a.deleteIdleKeyLocked(keyID, state)
}

// drainLocked first removes canceled and expired waiters. It then preserves
// FIFO whenever process capacity is the blocker. A waiter blocked only by its
// own per-key cap may be bypassed by another key; otherwise one saturated key
// could idle process capacity and starve every other authenticated key.
func (a *gatewayRequestAdmission) drainLocked() {
	for {
		a.removeUnavailableWaitersLocked()
		granted := false
		for element := a.waiters.Front(); element != nil; {
			next := element.Next()
			waiter := element.Value.(*gatewayRequestAdmissionWaiter)
			if !a.canGrantProcessLocked(waiter.bodyBytes) {
				return
			}
			if !a.canGrantKeyLocked(waiter.keyID, waiter.bodyBytes) {
				element = next
				continue
			}
			// Recheck at the exact grant boundary. The queue may have waited
			// behind a long critical section after the initial sweep.
			if waiter.ctx.Err() != nil || !time.Now().Before(waiter.deadline) {
				a.removeQueuedWaiterLocked(waiter)
				element = next
				continue
			}
			a.removeQueuedWaiterLocked(waiter)
			a.reserveLocked(waiter.keyID, waiter.bodyBytes)
			grantAt := time.Now()
			// A cancellation or deadline that races the reservation still wins
			// while this lock makes the grant private.
			if waiter.ctx.Err() != nil || !grantAt.Before(waiter.deadline) {
				a.releaseReservationLocked(waiter.keyID, waiter.bodyBytes)
				element = next
				continue
			}
			waiter.granted = true
			waiter.grantedAt = grantAt
			close(waiter.ready)
			granted = true
			break
		}
		if !granted {
			return
		}
	}
}

func (a *gatewayRequestAdmission) removeUnavailableWaitersLocked() {
	now := time.Now()
	for element := a.waiters.Front(); element != nil; {
		next := element.Next()
		waiter := element.Value.(*gatewayRequestAdmissionWaiter)
		if waiter.ctx.Err() != nil || !now.Before(waiter.deadline) {
			a.removeQueuedWaiterLocked(waiter)
		}
		element = next
	}
}

func (a *gatewayRequestAdmission) removeQueuedWaiterLocked(waiter *gatewayRequestAdmissionWaiter) {
	if waiter == nil || waiter.element == nil {
		return
	}
	a.waiters.Remove(waiter.element)
	waiter.element = nil
	a.queuedRequests--
	state := a.keys[waiter.keyID]
	if state == nil {
		return
	}
	state.queuedRequests--
	a.deleteIdleKeyLocked(waiter.keyID, state)
}

func (a *gatewayRequestAdmission) deleteIdleKeyLocked(keyID uint, state *gatewayRequestAdmissionKeyState) {
	if state != nil && state.inFlightRequests == 0 && state.inFlightBodyBytes == 0 && state.queuedRequests == 0 {
		delete(a.keys, keyID)
	}
}

func (a *gatewayRequestAdmission) snapshot(keyID uint) gatewayRequestAdmissionSnapshot {
	if a == nil {
		return gatewayRequestAdmissionSnapshot{}
	}
	a.mu.Lock()
	snapshot := gatewayRequestAdmissionSnapshot{
		InFlightRequests:  a.inFlightRequests,
		InFlightBodyBytes: a.inFlightBodyBytes,
		QueuedRequests:    a.queuedRequests,
		TrackedKeys:       len(a.keys),
	}
	if state := a.keys[keyID]; state != nil {
		snapshot.KeyInFlightRequests = state.inFlightRequests
		snapshot.KeyInFlightBodyBytes = state.inFlightBodyBytes
		snapshot.KeyQueuedRequests = state.queuedRequests
	}
	a.mu.Unlock()
	return snapshot
}

func gatewayRequestAdmissionContextError(ctx context.Context) error {
	contextErr := ctx.Err()
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, contextErr) {
		// Preserve a diagnostic cancellation cause without losing the canonical
		// context error. The gateway maps the latter to 499, so a parent cancel
		// can never be mistaken for this controller's queue deadline.
		return errors.Join(contextErr, cause)
	}
	return contextErr
}
