package api

import (
	"context"
	"errors"
	"testing"
	"time"
)

var gatewayRequestAdmissionTestProfile = gatewayRequestAdmissionBodyProfile{BodyMultiplier: 1}

func TestGatewayRequestAdmissionFallbackIsIsolatedAndRejectsMaximumRewrite(t *testing.T) {
	firstDeps := &Deps{}
	secondDeps := &Deps{}
	admission := gatewayRequestAdmissionForDeps(firstDeps)
	if admission == gatewayRequestAdmissionForDeps(secondDeps) {
		t.Fatal("fallback admission controller is shared across dependency graphs")
	}
	limits := admission.limits
	if limits.MaxInFlightRequests != 16 || limits.MaxInFlightRequestsPerKey != 4 ||
		limits.MaxInFlightBodyBytes != 256<<20 || limits.MaxInFlightBodyBytesPerKey != 96<<20 ||
		limits.MaxQueuedRequests != 32 || limits.MaxQueuedRequestsPerKey != 8 || limits.MaxQueueWait != time.Second {
		t.Fatalf("fallback request and queue limits = %+v", limits)
	}
	maximumRewrite, err := gatewayRequestBodyReservationBytes(64<<20, 64<<20, gatewayRequestAdmissionRewriteProfile)
	if err != nil {
		t.Fatalf("maximum rewrite reservation: %v", err)
	}
	if maximumRewrite <= limits.MaxInFlightBodyBytes || maximumRewrite <= limits.MaxInFlightBodyBytesPerKey {
		t.Fatalf("64 MiB rewrite unexpectedly fits conservative limits: reservation=%d limits=%+v", maximumRewrite, limits)
	}
	if _, err := admission.Acquire(context.Background(), 1, 64<<20, 64<<20, gatewayRequestAdmissionRewriteProfile); !errors.Is(err, errGatewayRequestBodyTooLarge) {
		t.Fatalf("64 MiB rewrite admission error = %v, want body too large", err)
	}

	ordinaryRewrite, err := admission.Acquire(context.Background(), 1, 4<<20, 64<<20, gatewayRequestAdmissionRewriteProfile)
	if err != nil {
		t.Fatalf("ordinary rewrite rejected: %v", err)
	}
	ordinaryRewrite.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestConfigureGatewayRequestAdmissionBuildsOneExplicitController(t *testing.T) {
	deps := &Deps{}
	limits := GatewayRequestAdmissionLimits{
		MaxInFlightRequests:        3,
		MaxInFlightBodyBytes:       12 << 20,
		MaxInFlightRequestsPerKey:  1,
		MaxInFlightBodyBytesPerKey: 4 << 20,
		MaxQueuedRequests:          2,
		MaxQueuedRequestsPerKey:    1,
		MaxQueueWait:               time.Second,
	}
	if err := deps.ConfigureGatewayRequestAdmission(limits); err != nil {
		t.Fatalf("configure admission: %v", err)
	}
	if got := gatewayRequestAdmissionForDeps(deps); got == nil || got.limits != limits {
		t.Fatalf("configured admission = %#v", got)
	}
	if err := deps.ConfigureGatewayRequestAdmission(limits); !errors.Is(err, errGatewayRequestAdmissionInvalid) {
		t.Fatalf("duplicate configuration error = %v", err)
	}
}

func TestGatewayRequestAdmissionReservesKnownAndUnknownBodies(t *testing.T) {
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        4,
		MaxInFlightBodyBytes:       200,
		MaxInFlightRequestsPerKey:  2,
		MaxInFlightBodyBytesPerKey: 100,
		MaxQueuedRequests:          4,
		MaxQueuedRequestsPerKey:    2,
	})

	known, err := admission.Acquire(context.Background(), 1, 37, 100, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire known body: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightRequests != 1 || snapshot.InFlightBodyBytes != 37 || snapshot.KeyInFlightRequests != 1 || snapshot.KeyInFlightBodyBytes != 37 {
		t.Fatalf("known body reservation = %+v", snapshot)
	}
	known.Release()
	known.Release()
	assertGatewayRequestAdmissionIdle(t, admission)

	unknown, err := admission.Acquire(context.Background(), 1, -1, 100, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire unknown body: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightBodyBytes != 0 || snapshot.KeyInFlightBodyBytes != 0 {
		t.Fatalf("unknown body did not start from fixed overhead only: %+v", snapshot)
	}
	if err := unknown.GrowTo(73); err != nil {
		t.Fatalf("grow unknown body: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightBodyBytes != 73 || snapshot.KeyInFlightBodyBytes != 73 {
		t.Fatalf("grown unknown body reservation = %+v", snapshot)
	}
	unknown.Release()
	assertGatewayRequestAdmissionIdle(t, admission)

	if _, err := admission.Acquire(context.Background(), 2, 101, 100, gatewayRequestAdmissionTestProfile); !errors.Is(err, errGatewayRequestBodyTooLarge) {
		t.Fatalf("oversized Content-Length error = %v", err)
	}
	unreservable, err := admission.Acquire(context.Background(), 2, -1, 1<<20, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire chunked body before growth: %v", err)
	}
	if err := unreservable.GrowTo(101); !errors.Is(err, errGatewayRequestBodyTooLarge) {
		t.Fatalf("unreservable chunked body growth error = %v", err)
	}
	unreservable.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionBudgetsMappingPeakForKnownAndChunkedBodies(t *testing.T) {
	rawBytes := int64(64 << 20)
	standard, err := gatewayRequestBodyReservationBytes(rawBytes, rawBytes, gatewayRequestAdmissionStandardProfile)
	if err != nil {
		t.Fatalf("standard peak reservation: %v", err)
	}
	rewrite, err := gatewayRequestBodyReservationBytes(rawBytes, rawBytes, gatewayRequestAdmissionRewriteProfile)
	if err != nil {
		t.Fatalf("rewrite peak reservation: %v", err)
	}
	if standard <= rawBytes || rewrite <= standard {
		t.Fatalf("body profiles did not add distinct peak headroom: standard=%d rewrite=%d raw=%d", standard, rewrite, rawBytes)
	}
	unknownRewrite, err := gatewayRequestBodyReservationBytes(-1, rawBytes, gatewayRequestAdmissionRewriteProfile)
	if err != nil || unknownRewrite != gatewayRequestBodyRewriteOverhead {
		t.Fatalf("initial chunked rewrite reservation = %d/%v, want %d", unknownRewrite, err, gatewayRequestBodyRewriteOverhead)
	}

	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        2,
		MaxInFlightBodyBytes:       rewrite * 2,
		MaxInFlightRequestsPerKey:  2,
		MaxInFlightBodyBytesPerKey: rewrite,
		MaxQueuedRequests:          1,
		MaxQueuedRequestsPerKey:    1,
	})
	known, err := admission.Acquire(context.Background(), 1, rawBytes, rawBytes, gatewayRequestAdmissionRewriteProfile)
	if err != nil {
		t.Fatalf("known mapping body admission: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightBodyBytes != rewrite || snapshot.KeyInFlightBodyBytes != rewrite {
		t.Fatalf("known mapping peak reservation = %+v, want %d", snapshot, rewrite)
	}
	known.Release()
	unknown, err := admission.Acquire(context.Background(), 1, -1, rawBytes, gatewayRequestAdmissionRewriteProfile)
	if err != nil {
		t.Fatalf("chunked mapping body admission: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightBodyBytes != unknownRewrite || snapshot.KeyInFlightBodyBytes != unknownRewrite {
		t.Fatalf("initial chunked mapping peak reservation = %+v, want %d", snapshot, unknownRewrite)
	}
	if err := unknown.GrowTo(rewrite); err != nil {
		t.Fatalf("grow chunked mapping reservation: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightBodyBytes != rewrite || snapshot.KeyInFlightBodyBytes != rewrite {
		t.Fatalf("grown chunked mapping peak reservation = %+v, want %d", snapshot, rewrite)
	}
	unknown.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionGrowToDistinguishesStaticAndTransientPressure(t *testing.T) {
	profile := gatewayRequestAdmissionBodyProfile{BodyMultiplier: 1, FixedOverhead: 10}
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        3,
		MaxInFlightBodyBytes:       100,
		MaxInFlightRequestsPerKey:  2,
		MaxInFlightBodyBytesPerKey: 100,
		MaxQueuedRequests:          1,
		MaxQueuedRequestsPerKey:    1,
	})
	holder, err := admission.Acquire(context.Background(), 2, 60, 90, profile)
	if err != nil {
		t.Fatalf("acquire pressure holder: %v", err)
	}
	unknown, err := admission.Acquire(context.Background(), 1, -1, 90, profile)
	if err != nil {
		t.Fatalf("acquire unknown body: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightBodyBytes != 80 || snapshot.KeyInFlightBodyBytes != 10 {
		t.Fatalf("initial pressure accounting = %+v", snapshot)
	}
	if err := unknown.GrowTo(40); !errors.Is(err, errGatewayRequestAdmissionBusy) {
		t.Fatalf("transient growth error = %v, want busy", err)
	}
	if err := unknown.GrowTo(101); !errors.Is(err, errGatewayRequestBodyTooLarge) {
		t.Fatalf("static growth error = %v, want body too large", err)
	}
	holder.Release()
	if err := unknown.GrowTo(40); err != nil {
		t.Fatalf("growth after pressure release: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightRequests != 1 || snapshot.InFlightBodyBytes != 40 || snapshot.KeyInFlightBodyBytes != 40 {
		t.Fatalf("grown accounting = %+v", snapshot)
	}
	unknown.Release()
	unknown.Release()
	if err := unknown.GrowTo(50); !errors.Is(err, errGatewayRequestAdmissionInvalid) {
		t.Fatalf("growth after release error = %v", err)
	}
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionGrowReleaseRaceDoesNotLeak(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       100,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 100,
			MaxQueuedRequests:          0,
			MaxQueuedRequestsPerKey:    0,
		})
		lease, err := admission.Acquire(context.Background(), 1, -1, 100, gatewayRequestAdmissionTestProfile)
		if err != nil {
			t.Fatalf("attempt %d acquire unknown body: %v", attempt, err)
		}
		start := make(chan struct{})
		growResult := make(chan error, 1)
		releaseDone := make(chan struct{}, 1)
		go func() {
			<-start
			growResult <- lease.GrowTo(50)
		}()
		go func() {
			<-start
			lease.Release()
			releaseDone <- struct{}{}
		}()
		close(start)
		growErr := <-growResult
		<-releaseDone
		if growErr != nil && !errors.Is(growErr, errGatewayRequestAdmissionInvalid) {
			t.Fatalf("attempt %d grow/release result = %v", attempt, growErr)
		}
		lease.Release()
		assertGatewayRequestAdmissionIdle(t, admission)
	}
}

func TestGatewayRequestAdmissionIsFIFOAcrossProcessCapacity(t *testing.T) {
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        1,
		MaxInFlightBodyBytes:       100,
		MaxInFlightRequestsPerKey:  1,
		MaxInFlightBodyBytesPerKey: 100,
		MaxQueuedRequests:          4,
		MaxQueuedRequestsPerKey:    2,
	})
	holder, err := admission.Acquire(context.Background(), 99, 1, 100, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan gatewayRequestAdmissionResult, 1)
	second := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(ctx, admission, 1, 10, first)
	waitForGatewayRequestAdmission(t, admission, 1, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.KeyQueuedRequests == 1
	})
	go acquireGatewayRequestAdmission(ctx, admission, 2, 10, second)
	waitForGatewayRequestAdmission(t, admission, 2, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.QueuedRequests == 2 && snapshot.KeyQueuedRequests == 1
	})

	holder.Release()
	firstResult := receiveGatewayRequestAdmission(t, first)
	if firstResult.err != nil || firstResult.lease == nil {
		t.Fatalf("first waiter = %+v", firstResult)
	}
	select {
	case secondResult := <-second:
		if secondResult.lease != nil {
			secondResult.lease.Release()
		}
		t.Fatalf("second waiter bypassed FIFO process capacity: %+v", secondResult)
	case <-time.After(20 * time.Millisecond):
	}

	firstResult.lease.Release()
	secondResult := receiveGatewayRequestAdmission(t, second)
	if secondResult.err != nil || secondResult.lease == nil {
		t.Fatalf("second waiter = %+v", secondResult)
	}
	secondResult.lease.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionLetsOtherKeysBypassPerKeySaturation(t *testing.T) {
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        2,
		MaxInFlightBodyBytes:       100,
		MaxInFlightRequestsPerKey:  1,
		MaxInFlightBodyBytesPerKey: 60,
		MaxQueuedRequests:          4,
		MaxQueuedRequestsPerKey:    2,
	})
	holder, err := admission.Acquire(context.Background(), 1, 30, 60, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire key holder: %v", err)
	}

	waitCtx, cancelWait := context.WithCancel(context.Background())
	waiting := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(waitCtx, admission, 1, 20, waiting)
	waitForGatewayRequestAdmission(t, admission, 1, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.KeyQueuedRequests == 1
	})

	other, err := admission.Acquire(context.Background(), 2, 40, 60, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("another key was blocked by the saturated key: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightRequests != 2 || snapshot.InFlightBodyBytes != 70 || snapshot.KeyQueuedRequests != 1 {
		t.Fatalf("per-key bypass accounting = %+v", snapshot)
	}
	other.Release()

	cancelWait()
	waitResult := receiveGatewayRequestAdmission(t, waiting)
	if !errors.Is(waitResult.err, context.Canceled) || waitResult.lease != nil {
		t.Fatalf("canceled per-key waiter = %+v", waitResult)
	}
	holder.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionEnforcesProcessAndPerKeyByteBudgets(t *testing.T) {
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        4,
		MaxInFlightBodyBytes:       70,
		MaxInFlightRequestsPerKey:  3,
		MaxInFlightBodyBytesPerKey: 50,
		MaxQueuedRequests:          4,
		MaxQueuedRequestsPerKey:    2,
	})
	holder, err := admission.Acquire(context.Background(), 1, 40, 50, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire byte holder: %v", err)
	}

	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	perKeyWaiter := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(waitCtx, admission, 1, 20, perKeyWaiter)
	waitForGatewayRequestAdmission(t, admission, 1, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.KeyQueuedRequests == 1
	})

	other, err := admission.Acquire(context.Background(), 2, 30, 50, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("another key did not use remaining process bytes: %v", err)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightBodyBytes != 70 || snapshot.KeyInFlightBodyBytes != 40 || snapshot.KeyQueuedRequests != 1 {
		t.Fatalf("byte saturation accounting = %+v", snapshot)
	}

	globalWaiter := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(waitCtx, admission, 3, 1, globalWaiter)
	waitForGatewayRequestAdmission(t, admission, 3, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.QueuedRequests == 2 && snapshot.KeyQueuedRequests == 1
	})
	other.Release()
	globalResult := receiveGatewayRequestAdmission(t, globalWaiter)
	if globalResult.err != nil || globalResult.lease == nil {
		t.Fatalf("small request did not proceed after global bytes were released: %+v", globalResult)
	}
	globalResult.lease.Release()

	select {
	case result := <-perKeyWaiter:
		if result.lease != nil {
			result.lease.Release()
		}
		t.Fatalf("per-key byte waiter was admitted before its key released bytes: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	holder.Release()
	perKeyResult := receiveGatewayRequestAdmission(t, perKeyWaiter)
	if perKeyResult.err != nil || perKeyResult.lease == nil {
		t.Fatalf("per-key byte waiter = %+v", perKeyResult)
	}
	perKeyResult.lease.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionBoundsQueueAndTrackedKeys(t *testing.T) {
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        1,
		MaxInFlightBodyBytes:       100,
		MaxInFlightRequestsPerKey:  1,
		MaxInFlightBodyBytesPerKey: 100,
		MaxQueuedRequests:          2,
		MaxQueuedRequestsPerKey:    1,
	})
	holder, err := admission.Acquire(context.Background(), 10, 1, 100, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	first := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(firstCtx, admission, 1, 1, first)
	waitForGatewayRequestAdmission(t, admission, 1, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.KeyQueuedRequests == 1
	})
	if _, err := admission.Acquire(context.Background(), 1, 1, 100, gatewayRequestAdmissionTestProfile); !errors.Is(err, errGatewayRequestAdmissionQueueFull) {
		t.Fatalf("per-key queue overflow error = %v", err)
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	second := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(secondCtx, admission, 2, 1, second)
	waitForGatewayRequestAdmission(t, admission, 2, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.QueuedRequests == 2 && snapshot.KeyQueuedRequests == 1
	})
	if _, err := admission.Acquire(context.Background(), 3, 1, 100, gatewayRequestAdmissionTestProfile); !errors.Is(err, errGatewayRequestAdmissionQueueFull) {
		t.Fatalf("process queue overflow error = %v", err)
	}
	if snapshot := admission.snapshot(3); snapshot.TrackedKeys != 3 || snapshot.KeyQueuedRequests != 0 {
		t.Fatalf("rejected key created unbounded state: %+v", snapshot)
	}

	cancelFirst()
	cancelSecond()
	for name, resultChannel := range map[string]<-chan gatewayRequestAdmissionResult{"first": first, "second": second} {
		result := receiveGatewayRequestAdmission(t, resultChannel)
		if !errors.Is(result.err, context.Canceled) || result.lease != nil {
			t.Fatalf("%s canceled waiter = %+v", name, result)
		}
	}
	holder.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionQueueWaitTimeoutCleansState(t *testing.T) {
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        1,
		MaxInFlightBodyBytes:       100,
		MaxInFlightRequestsPerKey:  1,
		MaxInFlightBodyBytesPerKey: 100,
		MaxQueuedRequests:          1,
		MaxQueuedRequestsPerKey:    1,
		MaxQueueWait:               20 * time.Millisecond,
	})
	holder, err := admission.Acquire(context.Background(), 9, 1, 100, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	result := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(context.Background(), admission, 1, 1, result)
	waitForGatewayRequestAdmission(t, admission, 1, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.QueuedRequests == 1 && snapshot.KeyQueuedRequests == 1
	})

	outcome := receiveGatewayRequestAdmission(t, result)
	if outcome.lease != nil || !errors.Is(outcome.err, errGatewayRequestAdmissionQueueWaitExceeded) || !errors.Is(outcome.err, errGatewayRequestAdmissionBusy) {
		t.Fatalf("queue wait outcome = %+v", outcome)
	}
	if snapshot := admission.snapshot(1); snapshot.InFlightRequests != 1 || snapshot.QueuedRequests != 0 || snapshot.KeyInFlightRequests != 0 || snapshot.KeyQueuedRequests != 0 || snapshot.TrackedKeys != 1 {
		t.Fatalf("queue wait timeout accounting = %+v", snapshot)
	}
	holder.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionQueueWaitPreservesParentCancellation(t *testing.T) {
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        1,
		MaxInFlightBodyBytes:       100,
		MaxInFlightRequestsPerKey:  1,
		MaxInFlightBodyBytesPerKey: 100,
		MaxQueuedRequests:          1,
		MaxQueuedRequestsPerKey:    1,
		MaxQueueWait:               200 * time.Millisecond,
	})
	holder, err := admission.Acquire(context.Background(), 9, 1, 100, gatewayRequestAdmissionTestProfile)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}
	parent, cancel := context.WithCancelCause(context.Background())
	result := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(parent, admission, 1, 1, result)
	waitForGatewayRequestAdmission(t, admission, 1, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.QueuedRequests == 1 && snapshot.KeyQueuedRequests == 1
	})
	cause := errors.New("caller closed request")
	cancel(cause)
	outcome := receiveGatewayRequestAdmission(t, result)
	if outcome.lease != nil || !errors.Is(outcome.err, cause) || !errors.Is(outcome.err, context.Canceled) || errors.Is(outcome.err, errGatewayRequestAdmissionQueueWaitExceeded) {
		t.Fatalf("parent cancellation outcome = %+v", outcome)
	}
	holder.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}

func TestGatewayRequestAdmissionQueueWaitGrantRaceDoesNotLeak(t *testing.T) {
	const (
		attempts    = 10
		maximumWait = 100 * time.Millisecond
	)
	for attempt := 0; attempt < attempts; attempt++ {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       100,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 100,
			MaxQueuedRequests:          1,
			MaxQueuedRequestsPerKey:    1,
			MaxQueueWait:               maximumWait,
		})
		holder, err := admission.Acquire(context.Background(), 9, 1, 100, gatewayRequestAdmissionTestProfile)
		if err != nil {
			t.Fatalf("attempt %d acquire holder: %v", attempt, err)
		}
		result := make(chan gatewayRequestAdmissionResult, 1)
		go acquireGatewayRequestAdmission(context.Background(), admission, 1, 1, result)
		waitForGatewayRequestAdmission(t, admission, 1, func(snapshot gatewayRequestAdmissionSnapshot) bool {
			return snapshot.QueuedRequests == 1 && snapshot.KeyQueuedRequests == 1
		})
		deadline := gatewayRequestAdmissionQueuedDeadline(t, admission, 1)
		remaining := time.Until(deadline)
		if remaining <= 0 {
			holder.Release()
			t.Fatalf("attempt %d observed an already-expired queued waiter", attempt)
		}
		releaseDone := make(chan struct{})
		time.AfterFunc(remaining, func() {
			holder.Release()
			close(releaseDone)
		})
		outcome := receiveGatewayRequestAdmission(t, result)
		if outcome.lease != nil {
			outcome.lease.Release()
		} else if !errors.Is(outcome.err, errGatewayRequestAdmissionQueueWaitExceeded) {
			t.Fatalf("attempt %d grant/timeout outcome = %+v", attempt, outcome)
		}
		<-releaseDone
		assertGatewayRequestAdmissionIdle(t, admission)
	}
}

func TestGatewayRequestAdmissionGrantCancelRaceDoesNotLeak(t *testing.T) {
	for attempt := 0; attempt < 100; attempt++ {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       100,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 100,
			MaxQueuedRequests:          1,
			MaxQueuedRequestsPerKey:    1,
		})
		holder, err := admission.Acquire(context.Background(), 1, 1, 100, gatewayRequestAdmissionTestProfile)
		if err != nil {
			t.Fatalf("attempt %d acquire holder: %v", attempt, err)
		}
		waitCtx, cancel := context.WithCancel(context.Background())
		result := make(chan gatewayRequestAdmissionResult, 1)
		go acquireGatewayRequestAdmission(waitCtx, admission, 2, 1, result)
		waitForGatewayRequestAdmission(t, admission, 2, func(snapshot gatewayRequestAdmissionSnapshot) bool {
			return snapshot.KeyQueuedRequests == 1
		})

		start := make(chan struct{})
		actionsDone := make(chan struct{}, 2)
		go func() {
			<-start
			holder.Release()
			actionsDone <- struct{}{}
		}()
		go func() {
			<-start
			cancel()
			actionsDone <- struct{}{}
		}()
		close(start)
		<-actionsDone
		<-actionsDone
		outcome := receiveGatewayRequestAdmission(t, result)
		if outcome.lease != nil {
			outcome.lease.Release()
		} else if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("attempt %d unexpected grant/cancel result: %+v", attempt, outcome)
		}
		assertGatewayRequestAdmissionIdle(t, admission)
	}
}

func TestGatewayRequestAdmissionRejectsInvalidInputsWithoutState(t *testing.T) {
	if _, err := newGatewayRequestAdmission(gatewayRequestAdmissionLimits{}); !errors.Is(err, errGatewayRequestAdmissionInvalid) {
		t.Fatalf("zero limits error = %v", err)
	}
	if _, err := newGatewayRequestAdmission(gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        1,
		MaxInFlightBodyBytes:       10,
		MaxInFlightRequestsPerKey:  2,
		MaxInFlightBodyBytesPerKey: 10,
		MaxQueuedRequests:          1,
		MaxQueuedRequestsPerKey:    1,
		MaxQueueWait:               time.Second,
	}); !errors.Is(err, errGatewayRequestAdmissionInvalid) {
		t.Fatalf("per-key limit relationship error = %v", err)
	}
	admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
		MaxInFlightRequests:        1,
		MaxInFlightBodyBytes:       100,
		MaxInFlightRequestsPerKey:  1,
		MaxInFlightBodyBytesPerKey: 100,
		MaxQueuedRequests:          0,
		MaxQueuedRequestsPerKey:    0,
	})
	if _, err := admission.Acquire(nil, 1, 1, 100, gatewayRequestAdmissionTestProfile); !errors.Is(err, errGatewayRequestAdmissionInvalid) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := admission.Acquire(context.Background(), 0, 1, 100, gatewayRequestAdmissionTestProfile); !errors.Is(err, errGatewayRequestAdmissionInvalid) {
		t.Fatalf("zero key error = %v", err)
	}
	if _, err := admission.Acquire(context.Background(), 1, 1, 100, gatewayRequestAdmissionBodyProfile{}); !errors.Is(err, errGatewayRequestAdmissionInvalid) {
		t.Fatalf("invalid body profile error = %v", err)
	}
	if _, err := gatewayRequestBodyReservationBytes(gatewayRequestAdmissionMaxInt64, gatewayRequestAdmissionMaxInt64, gatewayRequestAdmissionRewriteProfile); !errors.Is(err, errGatewayRequestBodyTooLarge) {
		t.Fatalf("overflowing body profile error = %v", err)
	}
	canceled, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("request already canceled")
	cancel(cause)
	if _, err := admission.Acquire(canceled, 1, 1, 100, gatewayRequestAdmissionTestProfile); !errors.Is(err, cause) {
		t.Fatalf("canceled context error = %v", err)
	}
	assertGatewayRequestAdmissionIdle(t, admission)
}

type gatewayRequestAdmissionResult struct {
	lease *gatewayRequestAdmissionLease
	err   error
}

func acquireGatewayRequestAdmission(ctx context.Context, admission *gatewayRequestAdmission, keyID uint, bodyBytes int64, result chan<- gatewayRequestAdmissionResult) {
	lease, err := admission.Acquire(ctx, keyID, bodyBytes, 100, gatewayRequestAdmissionTestProfile)
	result <- gatewayRequestAdmissionResult{lease: lease, err: err}
}

func mustGatewayRequestAdmission(t *testing.T, limits gatewayRequestAdmissionLimits) *gatewayRequestAdmission {
	t.Helper()
	if limits.MaxQueueWait == 0 {
		limits.MaxQueueWait = gatewayRequestAdmissionFallbackMaxQueueWait
	}
	admission, err := newGatewayRequestAdmission(limits)
	if err != nil {
		t.Fatalf("new admission: %v", err)
	}
	return admission
}

func waitForGatewayRequestAdmission(t *testing.T, admission *gatewayRequestAdmission, keyID uint, ready func(gatewayRequestAdmissionSnapshot) bool) gatewayRequestAdmissionSnapshot {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := admission.snapshot(keyID)
		if ready(snapshot) {
			return snapshot
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for admission state: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
}

func gatewayRequestAdmissionQueuedDeadline(t *testing.T, admission *gatewayRequestAdmission, keyID uint) time.Time {
	t.Helper()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	for element := admission.waiters.Front(); element != nil; element = element.Next() {
		waiter, _ := element.Value.(*gatewayRequestAdmissionWaiter)
		if waiter != nil && waiter.keyID == keyID && !waiter.deadline.IsZero() {
			return waiter.deadline
		}
	}
	t.Fatalf("queued waiter for downstream key %d disappeared before its deadline could be read", keyID)
	return time.Time{}
}

func receiveGatewayRequestAdmission(t *testing.T, result <-chan gatewayRequestAdmissionResult) gatewayRequestAdmissionResult {
	t.Helper()
	select {
	case received := <-result:
		return received
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for admission result")
		return gatewayRequestAdmissionResult{}
	}
}

func assertGatewayRequestAdmissionIdle(t *testing.T, admission *gatewayRequestAdmission) {
	t.Helper()
	if snapshot := admission.snapshot(0); snapshot.InFlightRequests != 0 || snapshot.InFlightBodyBytes != 0 || snapshot.QueuedRequests != 0 || snapshot.TrackedKeys != 0 {
		t.Fatalf("admission retained state: %+v", snapshot)
	}
}
