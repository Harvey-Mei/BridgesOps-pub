package api

import (
	"context"
	"testing"
	"time"
)

// TestGatewayRequestAdmissionInTimeGrantSurvivesDelayedConsumer verifies the
// contract at the hand-off boundary: a queue deadline controls whether a
// waiter may be granted, not when the goroutine consuming that grant happens
// to be scheduled. Holding the controller mutex after drainLocked records an
// in-time grant prevents the waiting Acquire call from resolving it until
// after the deadline.
func TestGatewayRequestAdmissionInTimeGrantSurvivesDelayedConsumer(t *testing.T) {
	const maximumWait = 250 * time.Millisecond
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
		t.Fatalf("acquire holder: %v", err)
	}

	result := make(chan gatewayRequestAdmissionResult, 1)
	go acquireGatewayRequestAdmission(context.Background(), admission, 1, 1, result)
	waitForGatewayRequestAdmission(t, admission, 1, func(snapshot gatewayRequestAdmissionSnapshot) bool {
		return snapshot.QueuedRequests == 1 && snapshot.KeyQueuedRequests == 1
	})

	admission.mu.Lock()
	var waiter *gatewayRequestAdmissionWaiter
	for element := admission.waiters.Front(); element != nil; element = element.Next() {
		candidate, _ := element.Value.(*gatewayRequestAdmissionWaiter)
		if candidate != nil && candidate.keyID == 1 {
			waiter = candidate
			break
		}
	}
	if waiter == nil {
		admission.mu.Unlock()
		holder.Release()
		t.Fatal("queued waiter disappeared before controlled grant")
	}

	// This is the locked portion of holder.Release. Marking the holder released
	// prevents a later cleanup path from underflowing the manually released
	// reservation, while drainLocked performs the ordinary queue grant.
	holder.mu.Lock()
	holder.released = true
	holder.mu.Unlock()
	admission.releaseReservationLocked(holder.keyID, holder.bodyBytes)
	admission.drainLocked()
	granted := waiter.granted
	grantedAt := waiter.grantedAt
	deadline := waiter.deadline
	if !granted || grantedAt.IsZero() || !grantedAt.Before(deadline) {
		admission.mu.Unlock()
		t.Fatalf("waiter was not granted before deadline: granted=%t granted_at=%s deadline=%s", granted, grantedAt, deadline)
	}

	// The ready channel is closed, but Acquire cannot re-lock admission.mu to
	// resolve its grant. Keep it blocked until the queue deadline has passed.
	for time.Now().Before(deadline) {
		time.Sleep(time.Until(deadline) + time.Millisecond)
	}
	admission.mu.Unlock()
	if !deadline.Before(time.Now()) {
		t.Fatalf("consumer was not delayed past the queue deadline: deadline=%s now=%s", deadline, time.Now())
	}

	outcome := receiveGatewayRequestAdmission(t, result)
	if outcome.err != nil || outcome.lease == nil {
		t.Fatalf("in-time grant lost after delayed consumer scheduling: %+v", outcome)
	}
	outcome.lease.Release()
	assertGatewayRequestAdmissionIdle(t, admission)
}
