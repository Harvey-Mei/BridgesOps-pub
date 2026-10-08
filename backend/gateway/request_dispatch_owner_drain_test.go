package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func testOwnerDrainService(owner *requestDispatchOwnerLease) *Service {
	return &Service{
		Store:                        &storage.Gateway{},
		requestDispatchOwner:         owner,
		requestDispatchRecoveryQueue: make(map[uint64]*requestDispatchRecoveryHandoff),
		requestDispatchRecoveryWake:  make(chan struct{}, 1),
	}
}

func testOwnerDrainRecoveryJob(dispatchID uint64) RequestDispatchRecoveryJob {
	now := time.Now().UTC()
	return RequestDispatchRecoveryJob{
		DispatchID:      dispatchID,
		OwnerInstanceID: "00000000000000000000000000000001",
		OwnerFenceToken: 7,
		TerminalKind:    "audit_failure",
		RequestedAt:     now,
		StartedAt:       now.Add(-time.Second),
		FinishedAt:      now,
		Log: &storage.GatewayRequestLog{
			StatusCode: 200, PromptTokens: 11, CompletionTokens: 19, TotalTokens: 30, CreatedAt: now,
		},
		TelemetryVersion:     storage.GatewayRequestTelemetryVersion,
		PhysicallyDispatched: true,
	}
}

// TestRequestDispatchOwnerDrainWaitsForDurableHandoffAcknowledgement pins the
// gate that rotation depends on: releasing the request is not an
// acknowledgement, only a durable handoff is.
func TestRequestDispatchOwnerDrainWaitsForDurableHandoffAcknowledgement(t *testing.T) {
	owner := testRequestDispatchOwnerLease(&fakeRequestDispatchOwnerStore{}, time.Second, 250*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond)
	owner.setLeaseUntil(time.Now().Add(time.Minute))
	_, release, err := owner.bind(context.Background())
	if err != nil {
		t.Fatalf("bind owner: %v", err)
	}
	acknowledge := owner.retainDurableHandoff()
	drained := owner.beginDrain()

	release()
	select {
	case <-drained:
		t.Fatal("owner drained while an exact terminal handoff was still unacknowledged")
	case <-time.After(20 * time.Millisecond):
	}

	acknowledge()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("owner did not drain after the durable handoff was acknowledged")
	}
	// Acknowledging twice must stay idempotent.
	acknowledge()
}

// TestFailedDurableHandoffKeepsOwnerDrainGatedAndRetriesExactSnapshot proves the
// live handler cannot surrender an audit into nothing: a failed durable write
// keeps the old identity gated and keeps the exact snapshot retryable.
func TestFailedDurableHandoffKeepsOwnerDrainGatedAndRetriesExactSnapshot(t *testing.T) {
	owner := testRequestDispatchOwnerLease(&fakeRequestDispatchOwnerStore{}, time.Second, 250*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond)
	owner.setLeaseUntil(time.Now().Add(time.Minute))
	_, release, err := owner.bind(context.Background())
	if err != nil {
		t.Fatalf("bind owner: %v", err)
	}
	service := testOwnerDrainService(owner)
	service.requestDispatchOwnerRecoveryMu.Lock()
	service.requestDispatchOwnerStopping = true
	service.requestDispatchOwnerRecoveryMu.Unlock()

	job := testOwnerDrainRecoveryJob(801)
	if err := service.ScheduleRequestDispatchRecovery(job); err == nil {
		t.Fatal("unavailable durable handoff store silently accepted an exact outcome")
	}
	drained := owner.beginDrain()
	release()
	select {
	case <-drained:
		t.Fatal("owner drained after a failed durable handoff; the exact snapshot could no longer be retried safely")
	case <-time.After(20 * time.Millisecond):
	}

	service.requestDispatchHandoffRetryMu.Lock()
	retry := service.requestDispatchHandoffRetryQueue[job.DispatchID]
	service.requestDispatchHandoffRetryMu.Unlock()
	if retry == nil || retry.job.Log == nil {
		t.Fatal("failed durable handoff did not retain the exact terminal snapshot")
	}
	if retry.job.Log.TotalTokens != 30 || retry.job.OwnerInstanceID != job.OwnerInstanceID || retry.job.OwnerFenceToken != job.OwnerFenceToken {
		t.Fatalf("retained snapshot lost its exact outcome or audit-bound owner identity: %+v", retry.job)
	}

	// A still-failing retry inside the budget must not release the gate.
	service.retryFailedRequestDispatchTerminalHandoffs()
	select {
	case <-drained:
		t.Fatal("owner drained while the retry budget was still open")
	case <-time.After(20 * time.Millisecond):
	}
	if retry.attempts != 1 {
		t.Fatalf("retry attempts = %d, want 1", retry.attempts)
	}

	// Once the bounded budget is exhausted the process stops holding the gate,
	// but it must not have written any substitute outcome.
	retry.firstFailureAt = time.Now().Add(-2 * requestDispatchTerminalHandoffRetryBudget)
	service.retryFailedRequestDispatchTerminalHandoffs()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("owner never drained after the bounded retry budget expired")
	}
	service.requestDispatchHandoffRetryMu.Lock()
	remaining := len(service.requestDispatchHandoffRetryQueue)
	service.requestDispatchHandoffRetryMu.Unlock()
	if remaining != 0 {
		t.Fatalf("abandoned retries = %d, want 0", remaining)
	}
	if len(service.requestDispatchRecoveryQueue) != 0 {
		t.Fatalf("abandoned exact snapshot entered the durable flush queue: %d", len(service.requestDispatchRecoveryQueue))
	}
}

// TestRequestDispatchOwnerForcedDrainReportsOutstandingWorkWithoutSynthesizing
// covers the bounded degrade: a transport that never returns must not freeze
// this process forever, and the degrade may only report counters.
func TestRequestDispatchOwnerForcedDrainReportsOutstandingWorkWithoutSynthesizing(t *testing.T) {
	owner := testRequestDispatchOwnerLease(&fakeRequestDispatchOwnerStore{}, time.Second, 250*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond)
	owner.drainMaximum = 30 * time.Millisecond
	owner.setLeaseUntil(time.Now().Add(time.Minute))
	_, release, err := owner.bind(context.Background())
	if err != nil {
		t.Fatalf("bind owner: %v", err)
	}
	acknowledge := owner.retainDurableHandoff()
	drained := owner.beginDrain()
	select {
	case <-drained:
		t.Fatal("owner drained while a stuck binding and an unacknowledged handoff remained")
	case <-time.After(20 * time.Millisecond):
	}

	forced := owner.forceDrain()
	if forced.ActiveBindings != 1 || forced.PendingHandoffs != 1 {
		t.Fatalf("forced drain counters = %+v, want one binding and one handoff", forced)
	}
	if forced.DrainMaximumWait != owner.drainMaximum || forced.DrainWaitedFor <= 0 {
		t.Fatalf("forced drain audit window = %+v", forced)
	}
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("bounded forced drain did not open the rotation gate")
	}
	// The straggler still finishes normally; neither path may panic or close the
	// already-closed gate a second time.
	acknowledge()
	release()
}

// TestFailedDurableHandoffRetryQueueStaysBounded keeps the degrade honest: a
// database outage must not grow unbounded in-memory state, and a dropped entry
// must release the gate instead of blocking rotation forever.
func TestFailedDurableHandoffRetryQueueStaysBounded(t *testing.T) {
	owner := testRequestDispatchOwnerLease(&fakeRequestDispatchOwnerStore{}, time.Second, 250*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond)
	owner.setLeaseUntil(time.Now().Add(time.Minute))
	service := testOwnerDrainService(owner)
	service.requestDispatchOwnerRecoveryMu.Lock()
	service.requestDispatchOwnerStopping = true
	service.requestDispatchOwnerRecoveryMu.Unlock()

	for id := 1; id <= requestDispatchTerminalHandoffRetryLimit+3; id++ {
		if err := service.ScheduleRequestDispatchRecovery(testOwnerDrainRecoveryJob(uint64(id))); err == nil {
			t.Fatalf("dispatch %d silently accepted an exact outcome", id)
		}
	}
	service.requestDispatchHandoffRetryMu.Lock()
	queued := len(service.requestDispatchHandoffRetryQueue)
	service.requestDispatchHandoffRetryMu.Unlock()
	if queued != requestDispatchTerminalHandoffRetryLimit {
		t.Fatalf("retry queue size = %d, want the %d bound", queued, requestDispatchTerminalHandoffRetryLimit)
	}
	owner.bindingsMu.Lock()
	pending := owner.pendingHandoffs
	owner.bindingsMu.Unlock()
	if pending != requestDispatchTerminalHandoffRetryLimit {
		t.Fatalf("gated handoffs = %d, want %d; a dropped snapshot must not keep the gate closed", pending, requestDispatchTerminalHandoffRetryLimit)
	}
}
