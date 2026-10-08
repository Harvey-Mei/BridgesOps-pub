package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

type fakeRequestDispatchOwnerStore struct {
	mu            sync.Mutex
	registerErr   error
	renew         func(context.Context, time.Time, time.Time) error
	stopErr       error
	registerCalls int
	renewCalls    int
	stopCalls     int
	rotateCalls   int
}

func (f *fakeRequestDispatchOwnerStore) RegisterRequestDispatchOwner(_ context.Context, _, _ time.Time) error {
	f.mu.Lock()
	f.registerCalls++
	err := f.registerErr
	f.mu.Unlock()
	return err
}

func (f *fakeRequestDispatchOwnerStore) RenewRequestDispatchOwner(ctx context.Context, now, leaseUntil time.Time) error {
	f.mu.Lock()
	f.renewCalls++
	renew := f.renew
	f.mu.Unlock()
	if renew == nil {
		return nil
	}
	return renew(ctx, now, leaseUntil)
}

func (f *fakeRequestDispatchOwnerStore) StopRequestDispatchOwner(_ context.Context, _ time.Time) error {
	f.mu.Lock()
	f.stopCalls++
	err := f.stopErr
	f.mu.Unlock()
	return err
}

func (f *fakeRequestDispatchOwnerStore) RotateRequestDispatchOwner(_ context.Context, _, _ time.Time) error {
	f.mu.Lock()
	f.rotateCalls++
	f.mu.Unlock()
	return nil
}

func (f *fakeRequestDispatchOwnerStore) counts() (register, renew, stop int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registerCalls, f.renewCalls, f.stopCalls
}

func testRequestDispatchOwnerLease(store requestDispatchOwnerStore, lease, heartbeat, retry, operation time.Duration) *requestDispatchOwnerLease {
	owner := newRequestDispatchOwnerLease(store, nil)
	owner.leaseDuration = lease
	owner.heartbeatInterval = heartbeat
	owner.retryInterval = retry
	owner.operationTimeout = operation
	return owner
}

func TestRequestDispatchOwnerRetriesTransientHeartbeatBeforeLeaseExpiry(t *testing.T) {
	recovered := make(chan struct{})
	var once sync.Once
	store := &fakeRequestDispatchOwnerStore{}
	store.renew = func(context.Context, time.Time, time.Time) error {
		_, renewCalls, _ := store.counts()
		if renewCalls == 1 {
			return errors.New("temporary database interruption")
		}
		once.Do(func() { close(recovered) })
		return nil
	}
	owner := testRequestDispatchOwnerLease(store, 250*time.Millisecond, 20*time.Millisecond, 10*time.Millisecond, 40*time.Millisecond)
	if err := owner.start(time.Now()); err != nil {
		t.Fatalf("start owner lease: %v", err)
	}
	bound, release, err := owner.bind(context.Background())
	if err != nil {
		t.Fatalf("bind owner context: %v", err)
	}
	defer release()

	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("owner heartbeat did not recover before lease expiry")
	}
	if cause := context.Cause(bound); cause != nil {
		t.Fatalf("transient heartbeat canceled owner context: %v", cause)
	}
	register, renew, _ := store.counts()
	if register != 1 || renew < 2 {
		t.Fatalf("owner calls = register:%d renew:%d", register, renew)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.stop(stopCtx, time.Now()); err != nil {
		t.Fatalf("stop owner lease: %v", err)
	}
}

func TestRequestDispatchOwnerCancelsAtLocalExpiryAfterPersistentFailure(t *testing.T) {
	store := &fakeRequestDispatchOwnerStore{
		renew: func(ctx context.Context, _ time.Time, _ time.Time) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	owner := testRequestDispatchOwnerLease(store, 90*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond, 250*time.Millisecond)
	if err := owner.start(time.Now()); err != nil {
		t.Fatalf("start owner lease: %v", err)
	}
	bound, release, err := owner.bind(context.Background())
	if err != nil {
		t.Fatalf("bind owner context: %v", err)
	}
	defer release()

	select {
	case <-bound.Done():
	case <-time.After(time.Second):
		t.Fatal("persistent heartbeat failure did not cancel at lease expiry")
	}
	if cause := context.Cause(bound); !IsRequestDispatchOwnerLeaseLost(cause) {
		t.Fatalf("owner expiry cause = %v", cause)
	}
	_, renew, _ := store.counts()
	if renew == 0 {
		t.Fatal("owner lease expired without attempting heartbeat renewal")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.stop(stopCtx, time.Now()); err != nil {
		t.Fatalf("stop expired owner lease: %v", err)
	}
}

func TestRequestDispatchOwnerCancelsImmediatelyWhenFenced(t *testing.T) {
	store := &fakeRequestDispatchOwnerStore{
		renew: func(context.Context, time.Time, time.Time) error {
			return storage.ErrGatewayRequestDispatchOwnerFenced
		},
	}
	owner := testRequestDispatchOwnerLease(store, time.Second, 10*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
	if err := owner.start(time.Now()); err != nil {
		t.Fatalf("start owner lease: %v", err)
	}
	bound, release, err := owner.bind(context.Background())
	if err != nil {
		t.Fatalf("bind owner context: %v", err)
	}
	defer release()

	select {
	case <-bound.Done():
	case <-time.After(time.Second):
		t.Fatal("fenced owner did not cancel bound context")
	}
	if cause := context.Cause(bound); !IsRequestDispatchOwnerLeaseLost(cause) {
		t.Fatalf("fenced owner cause = %v", cause)
	}
	_, renew, _ := store.counts()
	if renew != 1 {
		t.Fatalf("fenced owner renew calls = %d, want 1", renew)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.stop(stopCtx, time.Now()); err != nil {
		t.Fatalf("stop fenced owner lease: %v", err)
	}
}

func TestRequestDispatchOwnerPreservesParentCancellationCause(t *testing.T) {
	store := &fakeRequestDispatchOwnerStore{}
	owner := testRequestDispatchOwnerLease(store, time.Second, 250*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond)
	if err := owner.start(time.Now()); err != nil {
		t.Fatalf("start owner lease: %v", err)
	}
	parent, cancelParent := context.WithCancel(context.Background())
	bound, release, err := owner.bind(parent)
	if err != nil {
		t.Fatalf("bind owner context: %v", err)
	}
	cancelParent()
	select {
	case <-bound.Done():
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not reach bound context")
	}
	if cause := context.Cause(bound); !errors.Is(cause, context.Canceled) || IsRequestDispatchOwnerLeaseLost(cause) {
		t.Fatalf("parent cancellation cause = %v", cause)
	}
	release()
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.stop(stopCtx, time.Now()); err != nil {
		t.Fatalf("stop owner lease: %v", err)
	}
}

func TestRequestDispatchOwnerLeaseDeadlinePreservesMonotonicTime(t *testing.T) {
	owner := newRequestDispatchOwnerLease(&fakeRequestDispatchOwnerStore{}, nil)
	deadline := time.Now().Add(time.Second)
	owner.setLeaseUntil(deadline)
	if got := owner.currentLeaseUntil(); got != deadline {
		t.Fatalf("local lease deadline changed during storage: got %v want %v", got, deadline)
	}
}

func TestScheduleRequestDispatchRecoveryKeepsExactFirstOutcomeAndFacts(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	now := time.Now().UTC()
	routeSnapshot, err := storage.NewGatewayRouteExclusionSnapshot(now, []storage.GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("create route exclusion snapshot: %v", err)
	}
	dispatchSnapshot := postgresDispatchRecoverySnapshot(now.Add(-time.Second), 701, "/v1/responses")
	dispatchSnapshot.LogicalRequestTraceID = "cccccccccccccccccccccccccccccccc"
	dispatchSnapshot.TelemetryVersion = storage.GatewayRequestTelemetryVersion
	dispatchSnapshot.RoutingMode = storage.GatewayRoutingModeSmart
	dispatchSnapshot.GatewayApp = string(storage.GatewayAppCodex)
	dispatchSnapshot.RouteExclusionSnapshotVersion = storage.GatewayRouteExclusionSnapshotVersion
	dispatchSnapshot.RouteExclusionsSnapshot = routeSnapshot
	audit, err := store.BeginRequestDispatch(dispatchSnapshot, now.Add(-900*time.Millisecond))
	if err != nil {
		t.Fatalf("begin dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(-800*time.Millisecond)); err != nil {
		t.Fatalf("arm dispatch: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(audit.ID, now.Add(-700*time.Millisecond)); err != nil {
		t.Fatalf("confirm dispatch: %v", err)
	}
	rawCost := 1.25
	operationUnits := int64(2)
	reconcileAt := now.Add(time.Minute)
	log := &storage.GatewayRequestLog{
		StatusCode:        200,
		PromptTokens:      11,
		CompletionTokens:  19,
		TotalTokens:       30,
		Cost:              1.5,
		RawCost:           &rawCost,
		CostSource:        "provider_reported",
		UpstreamRequestID: "provider-exact-outcome",
		UpstreamAccepted:  true,
		CostReconcileAt:   &reconcileAt,
		OperationUnits:    &operationUnits,
		CreatedAt:         now,
	}
	job := RequestDispatchRecoveryJob{
		DispatchID:           audit.ID,
		OwnerInstanceID:      audit.OwnerInstanceID,
		OwnerFenceToken:      audit.OwnerFenceToken,
		TerminalKind:         "success",
		RequestedAt:          now,
		StartedAt:            now.Add(-time.Second),
		FinishedAt:           now,
		Log:                  log,
		TelemetryVersion:     storage.GatewayRequestTelemetryVersion,
		PhysicallyDispatched: true,
		Facts: storage.GatewayRequestDispatchFinalizationFacts{
			ProviderRequestID:  "provider-exact-outcome",
			ResponseObservedAt: now.Add(-500 * time.Millisecond),
			Accepted:           true,
			AcceptedEventType:  "response.created",
			AcceptedObjectID:   "response-object",
			AcceptedAt:         now.Add(-400 * time.Millisecond),
		},
	}
	svc := &Service{
		Store:                        store,
		requestDispatchRecoveryQueue: make(map[uint64]*requestDispatchRecoveryHandoff),
		requestDispatchRecoveryWake:  make(chan struct{}, 1),
	}
	if err := svc.ScheduleRequestDispatchRecovery(job); err != nil {
		t.Fatalf("schedule exact outcome: %v", err)
	}
	wantReconcileAt := *log.CostReconcileAt
	wantOperationUnits := *log.OperationUnits
	log.StatusCode = 502
	log.TotalTokens = 0
	*log.RawCost = 99
	*log.CostReconcileAt = now.Add(24 * time.Hour)
	*log.OperationUnits = 99
	if err := svc.ScheduleRequestDispatchRecovery(RequestDispatchRecoveryJob{
		DispatchID: audit.ID, OwnerInstanceID: audit.OwnerInstanceID, OwnerFenceToken: audit.OwnerFenceToken,
		TerminalKind: "client_canceled", RequestedAt: now, TelemetryVersion: storage.GatewayRequestTelemetryVersion,
	}); err != nil {
		t.Fatalf("repeat schedule must be first-wins idempotent: %v", err)
	}

	handoff := svc.requestDispatchRecoveryQueue[audit.ID]
	if handoff == nil || handoff.job.Log == nil {
		t.Fatal("exact recovery job was not retained")
	}
	got := handoff.job
	if got.TerminalKind != "success" || got.Log.StatusCode != 200 || got.Log.TotalTokens != 30 || got.Log.RawCost == nil || *got.Log.RawCost != 1.25 || got.Log.CostReconcileAt == nil || !got.Log.CostReconcileAt.Equal(wantReconcileAt) || got.Log.OperationUnits == nil || *got.Log.OperationUnits != wantOperationUnits || got.Log.OperationUnits == log.OperationUnits {
		t.Fatalf("queued exact outcome changed with caller mutation: kind=%q status=%d tokens=%d raw=%v reconcile=%v operation_units=%v", got.TerminalKind, got.Log.StatusCode, got.Log.TotalTokens, got.Log.RawCost, got.Log.CostReconcileAt, got.Log.OperationUnits)
	}
	if !got.Facts.Accepted || got.Facts.ProviderRequestID != "provider-exact-outcome" || got.Facts.AcceptedEventType != "response.created" || got.Facts.AcceptedObjectID != "response-object" {
		t.Fatalf("queued lifecycle facts = %+v", got.Facts)
	}
}

func TestRequestDispatchRecoveryTerminalLogRespectsTelemetryVersion(t *testing.T) {
	now := time.Date(2026, time.July, 25, 1, 2, 3, 0, time.UTC)
	tests := []struct {
		name                string
		telemetryVersion    int
		terminalKind        string
		terminalFailureCode string
	}{
		{
			name:                "legacy audit omits terminal code",
			terminalKind:        requestDispatchRecoveryTerminalKind,
			terminalFailureCode: "",
		},
		{
			name:                "current audit has recovery terminal code",
			telemetryVersion:    storage.GatewayRequestTelemetryVersion,
			terminalKind:        requestDispatchRecoveryTerminalKind,
			terminalFailureCode: requestDispatchRecoveryRouteReason,
		},
		{
			name:                "current canceled audit has cancellation terminal code",
			telemetryVersion:    storage.GatewayRequestTelemetryVersion,
			terminalKind:        "client_canceled",
			terminalFailureCode: "client_canceled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := requestDispatchRecoveryTerminalLog(RequestDispatchRecoveryJob{
				TerminalKind:     tt.terminalKind,
				StartedAt:        now.Add(-time.Second),
				FinishedAt:       now,
				TelemetryVersion: tt.telemetryVersion,
			})
			if log.TerminalFailureCode != tt.terminalFailureCode {
				t.Fatalf("terminal failure code = %q, want %q", log.TerminalFailureCode, tt.terminalFailureCode)
			}
		})
	}
}

func TestScheduleRequestDispatchRecoveryDurableHandoffFailureFencesOwner(t *testing.T) {
	owner := newRequestDispatchOwnerLease(&fakeRequestDispatchOwnerStore{}, nil)
	owner.setLeaseUntil(time.Now().Add(time.Minute))
	bound, release, err := owner.bind(context.Background())
	if err != nil {
		t.Fatalf("bind owner: %v", err)
	}
	defer release()
	svc := &Service{
		Store:                        &storage.Gateway{},
		requestDispatchOwner:         owner,
		requestDispatchRecoveryQueue: make(map[uint64]*requestDispatchRecoveryHandoff),
		requestDispatchRecoveryWake:  make(chan struct{}, 1),
	}
	err = svc.ScheduleRequestDispatchRecovery(RequestDispatchRecoveryJob{
		DispatchID:   702,
		TerminalKind: "success",
		RequestedAt:  time.Now(),
		Log:          &storage.GatewayRequestLog{StatusCode: 200},
	})
	if err == nil {
		t.Fatal("unavailable durable handoff store silently accepted an outcome")
	}
	select {
	case <-bound.Done():
	case <-time.After(time.Second):
		t.Fatal("durable handoff failure did not fence the process owner")
	}
	if cause := context.Cause(bound); !IsRequestDispatchOwnerLeaseLost(cause) {
		t.Fatalf("durable handoff failure owner cause = %v", cause)
	}
	if len(svc.requestDispatchRecoveryQueue) != 0 {
		t.Fatalf("failed durable handoff entered local queue: size=%d", len(svc.requestDispatchRecoveryQueue))
	}
	svc.requestDispatchOwnerRecoveryMu.Lock()
	svc.requestDispatchOwnerStopping = true
	svc.requestDispatchOwnerRecoveryMu.Unlock()
}

func TestFailedRequestDispatchOwnerAutomaticallyRotatesAndRestarts(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	svc := &Service{Store: store, requestDispatchRecoveryWake: make(chan struct{}, 1)}
	if err := svc.StartRequestDispatchOwner(); err != nil {
		t.Fatalf("start request dispatch owner: %v", err)
	}
	oldID, oldToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	svc.requestDispatchOwnerMu.RLock()
	oldOwner := svc.requestDispatchOwner
	svc.requestDispatchOwnerMu.RUnlock()
	bound, release, err := oldOwner.bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	svc.failRequestDispatchOwner(errors.New("durable handoff failed"))
	svc.restartFailedRequestDispatchOwner()
	select {
	case <-bound.Done():
	case <-time.After(time.Second):
		t.Fatal("failed owner context was not canceled")
	}
	release()
	deadline := time.Now().Add(2 * time.Second)
	for {
		newID, newToken, identityErr := store.RequestDispatchOwnerIdentity()
		svc.requestDispatchOwnerMu.RLock()
		newOwner := svc.requestDispatchOwner
		svc.requestDispatchOwnerMu.RUnlock()
		if identityErr == nil && (newID != oldID || newToken != oldToken) && newOwner != nil && newOwner != oldOwner {
			newContext, newRelease, bindErr := svc.BindRequestDispatchOwnerContext(context.Background())
			if bindErr != nil {
				t.Fatalf("bind replacement owner: %v", bindErr)
			}
			newRelease()
			if cause := context.Cause(newContext); cause != nil && !errors.Is(cause, context.Canceled) {
				t.Fatalf("replacement owner context cause = %v", cause)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("owner was not automatically replaced: old=%s/%d current=%s/%d err=%v", oldID, oldToken, newID, newToken, identityErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := svc.StopRequestDispatchOwner(stopCtx, time.Now()); err != nil {
		t.Fatalf("stop replacement owner: %v", err)
	}
}

func TestRequestDispatchOwnerRotationWaitsForConcurrentBindings(t *testing.T) {
	store := &fakeRequestDispatchOwnerStore{}
	owner := testRequestDispatchOwnerLease(store, time.Second, 250*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond)
	if err := owner.start(time.Now()); err != nil {
		t.Fatal(err)
	}
	_, releaseA, err := owner.bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	contextB, releaseB, err := owner.bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	drained := owner.beginDrain()
	owner.lose(errors.New("request A forced owner replacement"))
	select {
	case <-contextB.Done():
	case <-time.After(time.Second):
		t.Fatal("request B did not observe owner loss")
	}
	rotated := make(chan struct{})
	go func() {
		<-drained
		_ = store.RotateRequestDispatchOwner(context.Background(), time.Now(), time.Now().Add(time.Second))
		close(rotated)
	}()

	releaseA()
	select {
	case <-rotated:
		t.Fatal("owner rotated while request B still retained its old-owner exact handoff capability")
	case <-time.After(20 * time.Millisecond):
	}
	releaseB()
	select {
	case <-rotated:
	case <-time.After(time.Second):
		t.Fatal("owner did not rotate after both old-owner requests drained")
	}
	store.mu.Lock()
	rotateCalls := store.rotateCalls
	store.mu.Unlock()
	if rotateCalls != 1 {
		t.Fatalf("owner rotations = %d, want 1", rotateCalls)
	}
}

func TestRequestDispatchOwnerRotationPreservesConcurrentObservedOutcome(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	svc := &Service{Store: store, requestDispatchRecoveryWake: make(chan struct{}, 1)}
	if err := svc.StartRequestDispatchOwner(); err != nil {
		t.Fatal(err)
	}
	oldOwnerID, oldOwnerToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	_, releaseA, err := svc.BindRequestDispatchOwnerContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	contextB, releaseB, err := svc.BindRequestDispatchOwnerContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	concurrentRouteSnapshot, err := storage.NewGatewayRouteExclusionSnapshot(now, []storage.GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("create route exclusion snapshot: %v", err)
	}
	begin := func(channelID uint) *storage.GatewayRequestDispatchAudit {
		snapshot := postgresDispatchRecoverySnapshot(now.Add(-time.Second), channelID, "/v1/responses")
		snapshot.LogicalRequestTraceID = strings.Repeat("d", 32)
		snapshot.TelemetryVersion = storage.GatewayRequestTelemetryVersion
		snapshot.RoutingMode = storage.GatewayRoutingModeSmart
		snapshot.GatewayApp = string(storage.GatewayAppCodex)
		snapshot.RouteExclusionSnapshotVersion = storage.GatewayRouteExclusionSnapshotVersion
		snapshot.RouteExclusionsSnapshot = concurrentRouteSnapshot
		audit, beginErr := store.BeginRequestDispatch(snapshot, now.Add(-900*time.Millisecond))
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		if markErr := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(-800*time.Millisecond)); markErr != nil {
			t.Fatal(markErr)
		}
		if confirmErr := store.ConfirmRequestDispatchTransportInvocation(audit.ID, now.Add(-700*time.Millisecond)); confirmErr != nil {
			t.Fatal(confirmErr)
		}
		return audit
	}
	auditA := begin(711)
	auditB := begin(712)
	svc.failRequestDispatchOwner(errors.New("request A confirmation persistence failed"))
	select {
	case <-contextB.Done():
	case <-time.After(time.Second):
		t.Fatal("concurrent request B did not observe owner loss")
	}
	job := func(audit *storage.GatewayRequestDispatchAudit, providerID string, tokens int64) RequestDispatchRecoveryJob {
		finishedAt := now
		return RequestDispatchRecoveryJob{
			DispatchID: audit.ID, OwnerInstanceID: audit.OwnerInstanceID, OwnerFenceToken: audit.OwnerFenceToken,
			TerminalKind: "audit_failure", RequestedAt: finishedAt,
			StartedAt: now.Add(-time.Second), FinishedAt: finishedAt,
			Log: &storage.GatewayRequestLog{StatusCode: 200, UpstreamRequestID: providerID, TotalTokens: tokens, CreatedAt: finishedAt},
			Facts: storage.GatewayRequestDispatchFinalizationFacts{
				ProviderRequestID: providerID, ResponseObservedAt: now.Add(-500 * time.Millisecond),
			},
			TelemetryVersion: storage.GatewayRequestTelemetryVersion, PhysicallyDispatched: true,
		}
	}
	if err := svc.ScheduleRequestDispatchRecovery(job(auditA, "provider-a", 11)); err != nil {
		t.Fatalf("persist request A handoff: %v", err)
	}
	svc.RestartFailedRequestDispatchOwner()
	releaseA()
	currentOwnerID, currentOwnerToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if currentOwnerID != oldOwnerID || currentOwnerToken != oldOwnerToken {
		t.Fatal("owner rotated before concurrent request B persisted its exact outcome")
	}
	if err := svc.ScheduleRequestDispatchRecovery(job(auditB, "provider-b", 37)); err != nil {
		t.Fatalf("request B lost old-owner exact handoff capability: %v", err)
	}
	releaseB()

	deadline := time.Now().Add(2 * time.Second)
	for {
		currentOwnerID, currentOwnerToken, identityErr := store.RequestDispatchOwnerIdentity()
		if identityErr != nil {
			t.Fatal(identityErr)
		}
		handoffs, listErr := store.ListRequestDispatchTerminalHandoffs(10)
		if listErr != nil {
			t.Fatal(listErr)
		}
		for _, handoff := range handoffs {
			if handoff.DispatchID != auditB.ID {
				continue
			}
			decoded, decodeErr := storage.DecodeGatewayRequestDispatchTerminalHandoff(handoff)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if decoded.Log == nil || decoded.Log.UpstreamRequestID != "provider-b" || decoded.Log.TotalTokens != 37 || decoded.Facts.ProviderRequestID != "provider-b" || decoded.Facts.ResponseObservedAt.IsZero() {
				t.Fatalf("request B exact observed outcome = %+v log=%+v", decoded.Facts, decoded.Log)
			}
			if currentOwnerID == oldOwnerID && currentOwnerToken == oldOwnerToken {
				break
			}
			stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if stopErr := svc.StopRequestDispatchOwner(stopCtx, time.Now()); stopErr != nil {
				t.Fatal(stopErr)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("request B durable handoff was not retained")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestManualReviewRequestDispatchRetainsDurableHandoff(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	now := time.Now().UTC()
	manualReviewSnapshot := postgresDispatchRecoverySnapshot(now, 703, "/v1/responses")
	manualReviewSnapshot.LogicalRequestTraceID = strings.Repeat("e", 32)
	manualReviewSnapshot.TelemetryVersion = storage.GatewayRequestTelemetryVersion
	manualReviewSnapshot.RoutingMode = storage.GatewayRoutingModeSmart
	manualReviewSnapshot.GatewayApp = string(storage.GatewayAppCodex)
	manualReviewSnapshot.RouteExclusionSnapshotVersion = storage.GatewayRouteExclusionSnapshotVersion
	manualReviewRouteSnapshot, err := storage.NewGatewayRouteExclusionSnapshot(now, []storage.GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("create route exclusion snapshot: %v", err)
	}
	manualReviewSnapshot.RouteExclusionsSnapshot = manualReviewRouteSnapshot
	audit, err := store.BeginRequestDispatch(manualReviewSnapshot, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.StopRequestDispatchOwner(nil, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	// The armed transport is deliberately left unattested. A live handler that
	// knows base.RoundTrip returned would confirm it through
	// PersistRequestDispatchTerminalHandoff; this row simulates the handoff of an
	// interrupted incarnation that never produced that attestation, which is the
	// only state the quarantine exists for.
	if err := db.Create(&storage.GatewayRequestDispatchTerminalHandoff{
		DispatchID: audit.ID, Version: 1, TerminalKind: "dispatch_audit_failure",
		RequestedAt: now.Add(3 * time.Millisecond), StartedAt: now, FinishedAt: now.Add(3 * time.Millisecond),
		FactsSnapshotJSON: "{}", TelemetryVersion: audit.TelemetryVersion, PhysicallyDispatched: true,
		CreatedAt: now.Add(3 * time.Millisecond),
	}).Error; err != nil {
		t.Fatalf("persist unattested armed handoff: %v", err)
	}
	handoffs, err := store.ListRequestDispatchTerminalHandoffs(10)
	if err != nil || len(handoffs) != 1 {
		t.Fatalf("initial durable handoffs = %+v err=%v", handoffs, err)
	}
	svc := &Service{Store: store}
	if err := svc.processDurableRequestDispatchRecoveryHandoff(handoffs[0]); err != nil {
		t.Fatalf("process manual-review handoff: %v", err)
	}
	stored, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.TransportState != storage.GatewayRequestDispatchTransportStateManualReview {
		t.Fatalf("unconfirmed dispatch was not quarantined: %+v", stored)
	}
	var retained int64
	if err := db.Model(&storage.GatewayRequestDispatchTerminalHandoff{}).Where("dispatch_id = ?", audit.ID).Count(&retained).Error; err != nil || retained != 1 {
		t.Fatalf("manual-review handoff retained=%d err=%v", retained, err)
	}
	handoffs, err = store.ListRequestDispatchTerminalHandoffs(10)
	if err != nil || len(handoffs) != 0 {
		t.Fatalf("manual-review handoff remained on automatic queue: %+v err=%v", handoffs, err)
	}
	if err := store.DeleteRequestDispatchTerminalHandoff(audit.ID); !errors.Is(err, storage.ErrGatewayRequestDispatchManualReviewRequired) {
		t.Fatalf("manual-review handoff deletion error = %v", err)
	}
}
