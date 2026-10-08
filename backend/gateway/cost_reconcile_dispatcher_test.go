package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestReconcileRequestCostSchedulesExplicitlyAcceptedFailure(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	finishedAt := time.Now().UTC()
	accepted := &storage.GatewayRequestLog{
		ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 502,
		Error: "stream failed after accepted", UpstreamAccepted: true, UpstreamRequestID: "accepted-request",
		CreatedAt: finishedAt,
	}
	ordinary := &storage.GatewayRequestLog{
		ChannelID: 3, Method: "POST", Path: "/v1/responses", StatusCode: 502,
		Error: "upstream failed before acceptance", CreatedAt: finishedAt,
	}
	evidence := &storage.GatewayRequestLog{
		RecordKind: storage.GatewayRequestRecordKindDispatchCostEvidence,
		ChannelID:  4, Method: "POST", Path: "/v1/responses",
		UpstreamRequestID: "provider-evidence", CreatedAt: finishedAt,
	}
	for _, log := range []*storage.GatewayRequestLog{accepted, ordinary, evidence} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, &fakeKeyManager{}, nil)
	svc.ReconcileRequestCost(accepted, finishedAt.Add(-time.Second), finishedAt)
	svc.ReconcileRequestCost(ordinary, finishedAt.Add(-time.Second), finishedAt)
	svc.ReconcileRequestCost(evidence, finishedAt.Add(-time.Second), finishedAt)

	var scheduled storage.GatewayRequestLog
	if err := db.First(&scheduled, accepted.ID).Error; err != nil {
		t.Fatalf("read accepted request: %v", err)
	}
	if scheduled.CostReconcileAt == nil {
		t.Fatalf("accepted failed request was not scheduled: %+v", scheduled)
	}
	want := InitialRequestCostReconcileAt(finishedAt)
	if delta := scheduled.CostReconcileAt.Sub(want); delta < -time.Second || delta > time.Second {
		t.Fatalf("accepted request due at %v, want near %v", scheduled.CostReconcileAt, want)
	}
	var unscheduled storage.GatewayRequestLog
	if err := db.First(&unscheduled, ordinary.ID).Error; err != nil {
		t.Fatalf("read ordinary request: %v", err)
	}
	if unscheduled.CostReconcileAt != nil {
		t.Fatalf("ordinary failed request was scheduled: %+v", unscheduled)
	}
	var scheduledEvidence storage.GatewayRequestLog
	if err := db.First(&scheduledEvidence, evidence.ID).Error; err != nil {
		t.Fatalf("read dispatch evidence: %v", err)
	}
	if scheduledEvidence.CostReconcileAt == nil {
		t.Fatalf("provider-ID dispatch evidence was not scheduled: %+v", scheduledEvidence)
	}
	if delta := scheduledEvidence.CostReconcileAt.Sub(want); delta < -time.Second || delta > time.Second {
		t.Fatalf("dispatch evidence due at %v, want near %v", scheduledEvidence.CostReconcileAt, want)
	}
}

type fairnessBlockingCostManager struct {
	fakeKeyManager
	calls   chan string
	release <-chan struct{}
}

func (m *fairnessBlockingCostManager) LookupRequestCost(ctx context.Context, _ uint, query connector.RequestCostQuery) (*connector.RequestCostResult, error) {
	select {
	case m.calls <- query.RequestID:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-m.release:
		return nil, errors.New("lookup pending")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCostDispatcherAdmitsOnePerBillingIdentity(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now().UTC()
	logs := []*storage.GatewayRequestLog{
		{ChannelID: 10, UpstreamKeyID: 100, UpstreamKeyName: "key-100", Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "a-oldest", CreatedAt: now.Add(-4 * time.Second)},
		{ChannelID: 10, UpstreamKeyID: 100, UpstreamKeyName: "key-100", Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "a-next", CreatedAt: now.Add(-3 * time.Second)},
		{ChannelID: 11, UpstreamKeyID: 200, UpstreamKeyName: "key-200", Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "b", CreatedAt: now.Add(-2 * time.Second)},
		{ChannelID: 12, UpstreamKeyID: 300, UpstreamKeyName: "key-300", Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "c", CreatedAt: now.Add(-time.Second)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
		if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Second)); err != nil {
			t.Fatalf("schedule request log: %v", err)
		}
	}
	release := make(chan struct{})
	manager := &fairnessBlockingCostManager{calls: make(chan string, 4), release: release}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, manager, nil)
	svc.costLookupSlots = make(chan struct{}, 4)
	svc.dispatchPendingCosts()

	started := make(map[string]struct{}, 3)
	for len(started) < 3 {
		select {
		case requestID := <-manager.calls:
			started[requestID] = struct{}{}
		case <-time.After(time.Second):
			t.Fatalf("started identities = %v, want a-oldest, b, c", started)
		}
	}
	for _, requestID := range []string{"a-oldest", "b", "c"} {
		if _, ok := started[requestID]; !ok {
			t.Fatalf("started identities = %v, missing %q", started, requestID)
		}
	}
	select {
	case requestID := <-manager.calls:
		t.Fatalf("second request for busy identity started: %q", requestID)
	default:
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for len(svc.costLookupSlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(svc.costLookupSlots) != 0 {
		t.Fatal("dispatcher workers did not release their slots")
	}
}

func TestCostDispatcherRecoversExpiredLoneDurableClaim(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now().UTC()
	log := &storage.GatewayRequestLog{
		ChannelID: 13, UpstreamKeyID: 400, UpstreamKeyName: "key-400",
		Method: "POST", Path: "/v1/responses", StatusCode: 200,
		UpstreamRequestID: "expired-lone-claim", CreatedAt: now.Add(-time.Minute),
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Second)); err != nil {
		t.Fatalf("schedule request log: %v", err)
	}
	first, acquired, err := store.ClaimRequestCostReconcileDurable(log.ID, time.Minute)
	if err != nil || !acquired || first == nil {
		t.Fatalf("initial durable claim = %+v, acquired=%v, err=%v", first, acquired, err)
	}
	if err := db.Exec(`
		WITH expired AS (
			SELECT clock_timestamp() - interval '2 seconds' AS claimed_at,
			       clock_timestamp() - interval '1 second' AS lease_expires_at
		)
		UPDATE gateway_request_logs
		   SET cost_reconcile_claimed_at = expired.claimed_at,
		       cost_reconcile_lease_expires_at = expired.lease_expires_at,
		       cost_reconcile_at = expired.lease_expires_at
		  FROM expired
		 WHERE id = ?
	`, log.ID).Error; err != nil {
		t.Fatalf("expire request claim: %v", err)
	}
	if err := db.Exec(`
		UPDATE gateway_request_cost_reconcile_identity_leases AS leases
		   SET claimed_at = logs.cost_reconcile_claimed_at,
		       lease_expires_at = logs.cost_reconcile_lease_expires_at
		  FROM gateway_request_logs AS logs
		 WHERE leases.id = ?
		   AND logs.id = ?
	`, first.Fence.IdentityLeaseID, log.ID).Error; err != nil {
		t.Fatalf("expire identity claim: %v", err)
	}

	manager := &fakeKeyManager{requestCost: &connector.RequestCostResult{
		UsageID: "expired-lone-usage", Cost: 0.25, Source: "upstream",
	}}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, manager, nil)
	svc.costLookupSlots = make(chan struct{}, 1)
	svc.dispatchPendingCosts()
	select {
	case <-svc.costWorkerWake:
	case <-time.After(2 * time.Second):
		t.Fatal("expired lone claim was not recovered by the dispatcher")
	}

	var recovered storage.GatewayRequestLog
	if err := db.First(&recovered, log.ID).Error; err != nil {
		t.Fatalf("read recovered request: %v", err)
	}
	if recovered.CostReconcileState != storage.GatewayRequestCostReconcileSucceeded ||
		recovered.CostReconcileOutcome != storage.GatewayRequestCostReconcileOutcomeSucceeded ||
		recovered.CostReconcileTries != 2 || recovered.Cost != 0.25 || recovered.UpstreamUsageID == nil ||
		*recovered.UpstreamUsageID != "expired-lone-usage" || recovered.CostReconcileIdentityLeaseID != nil ||
		recovered.CostReconcileClaimToken != "" || recovered.CostReconcileLeaseExpiresAt != nil {
		t.Fatalf("recovered request = %+v", recovered)
	}
	var lease storage.GatewayRequestCostReconcileIdentityLease
	if err := db.First(&lease, first.Fence.IdentityLeaseID).Error; err != nil {
		t.Fatalf("read released identity lease: %v", err)
	}
	if lease.RequestLogID != nil || lease.ClaimToken != "" || lease.LeaseExpiresAt != nil || lease.ClaimEpoch != first.Fence.ClaimEpoch+1 {
		t.Fatalf("released identity lease = %+v", lease)
	}
}
