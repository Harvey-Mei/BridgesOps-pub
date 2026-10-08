package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func appendDueCostReconcileLog(t *testing.T, store *Gateway, channelID uint, upstreamKeyID int64, requestID string, createdAt time.Time) GatewayRequestLog {
	t.Helper()
	dueAt := createdAt.Add(time.Second)
	log := GatewayRequestLog{
		DownstreamKeyID:   1,
		ChannelID:         channelID,
		UpstreamKeyID:     upstreamKeyID,
		Method:            "POST",
		Path:              "/v1/responses",
		StatusCode:        200,
		UpstreamRequestID: requestID,
		CostReconcileAt:   &dueAt,
		CreatedAt:         createdAt,
	}
	if err := store.AppendRequestLog(&log); err != nil {
		t.Fatalf("append due cost reconcile log: %v", err)
	}
	return log
}

func expireDurableCostReconcileClaim(t *testing.T, db *gorm.DB, fence GatewayRequestCostReconcileFence) {
	t.Helper()
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
	`, fence.RequestLogID).Error; err != nil {
		t.Fatalf("expire request claim: %v", err)
	}
	if err := db.Exec(`
		UPDATE gateway_request_cost_reconcile_identity_leases AS leases
		   SET claimed_at = logs.cost_reconcile_claimed_at,
		       lease_expires_at = logs.cost_reconcile_lease_expires_at
		  FROM gateway_request_logs AS logs
		 WHERE leases.id = ?
		   AND logs.id = ?
	`, fence.IdentityLeaseID, fence.RequestLogID).Error; err != nil {
		t.Fatalf("expire identity claim: %v", err)
	}
}

func TestRequestCostReconcileDurableIdentityLeaseSerializesInstances(t *testing.T) {
	db := testdb.Open(t)
	firstStore := NewGateway(db.Session(&gorm.Session{NewDB: true}))
	secondStore := NewGateway(db.Session(&gorm.Session{NewDB: true}))
	createdAt := time.Now().UTC().Add(-time.Minute)
	first := appendDueCostReconcileLog(t, firstStore, 4101, 77, "identity-first", createdAt)
	second := appendDueCostReconcileLog(t, firstStore, 4101, 77, "identity-second", createdAt.Add(time.Microsecond))

	start := make(chan struct{})
	type result struct {
		claim    *GatewayRequestCostReconcileClaim
		acquired bool
		err      error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for index, candidate := range []struct {
		store *Gateway
		id    uint
	}{{firstStore, first.ID}, {secondStore, second.ID}} {
		_ = index
		wg.Add(1)
		go func(candidate struct {
			store *Gateway
			id    uint
		}) {
			defer wg.Done()
			<-start
			claim, acquired, err := candidate.store.ClaimRequestCostReconcileDurable(candidate.id, time.Minute)
			results <- result{claim: claim, acquired: acquired, err: err}
		}(candidate)
	}
	close(start)
	wg.Wait()
	close(results)

	var winner *GatewayRequestCostReconcileClaim
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent identity claim: %v", result.err)
		}
		if result.acquired {
			if result.claim == nil || winner != nil {
				t.Fatalf("same billing identity acquired more than once: previous=%+v current=%+v", winner, result.claim)
			}
			winner = result.claim
		}
	}
	if winner == nil {
		t.Fatal("same billing identity had no winning claimant")
	}
	if err := firstStore.ReleaseRequestCostReconcileDurableClaim(winner.Fence, GatewayRequestCostReconcileReasonDispatcherSlotUnavailable); err != nil {
		t.Fatalf("release winning identity claim: %v", err)
	}
}

func TestRequestCostReconcileFairListExcludesRemoteIdentityLease(t *testing.T) {
	db := testdb.Open(t)
	firstStore := NewGateway(db.Session(&gorm.Session{NewDB: true}))
	secondStore := NewGateway(db.Session(&gorm.Session{NewDB: true}))
	createdAt := time.Now().UTC().Add(-time.Minute)
	first := appendDueCostReconcileLog(t, firstStore, 4201, 88, "busy-head", createdAt)
	busyFollower := appendDueCostReconcileLog(t, firstStore, 4201, 88, "busy-follower", createdAt.Add(time.Microsecond))
	available := appendDueCostReconcileLog(t, firstStore, 4202, 88, "available-head", createdAt.Add(2*time.Microsecond))

	claim, acquired, err := firstStore.ClaimRequestCostReconcileDurable(first.ID, time.Minute)
	if err != nil || !acquired || claim == nil {
		t.Fatalf("claim busy identity = %+v, acquired=%v, err=%v", claim, acquired, err)
	}
	pending, err := secondStore.ListFairDueRequestCostReconciles(10)
	if err != nil {
		t.Fatalf("list fair cross-instance work: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != available.ID {
		t.Fatalf("fair work while identity is remotely leased = %+v, want only %d (not %d)", pending, available.ID, busyFollower.ID)
	}
	if err := firstStore.ReleaseRequestCostReconcileDurableClaim(claim.Fence, GatewayRequestCostReconcileReasonDispatcherSlotUnavailable); err != nil {
		t.Fatalf("release busy identity: %v", err)
	}
}

func TestRequestCostReconcileFairListRecoversExpiredClaimWithoutFollower(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	createdAt := time.Now().UTC().Add(-time.Minute)
	log := appendDueCostReconcileLog(t, store, 4251, 89, "expired-only", createdAt)

	first, acquired, err := store.ClaimRequestCostReconcileDurable(log.ID, time.Minute)
	if err != nil || !acquired || first == nil {
		t.Fatalf("claim only request = %+v, acquired=%v, err=%v", first, acquired, err)
	}
	expireDurableCostReconcileClaim(t, db, first.Fence)

	pending, err := store.ListFairDueRequestCostReconciles(10)
	if err != nil {
		t.Fatalf("list expired only claim: %v", err)
	}
	if len(pending) != 1 || pending[0].ID != log.ID || pending[0].CostReconcileState != GatewayRequestCostReconcileClaimed {
		t.Fatalf("expired only claim did not re-enter fair queue: %+v", pending)
	}

	second, acquired, err := store.ClaimRequestCostReconcileDurable(log.ID, time.Minute)
	if err != nil || !acquired || second == nil {
		t.Fatalf("recover expired only claim = %+v, acquired=%v, err=%v", second, acquired, err)
	}
	if second.Fence.ClaimEpoch != first.Fence.ClaimEpoch+1 || second.Fence.ClaimToken == first.Fence.ClaimToken {
		t.Fatalf("recovered only-claim fence = %+v, first = %+v", second.Fence, first.Fence)
	}
	if err := store.ReleaseRequestCostReconcileDurableClaim(second.Fence, GatewayRequestCostReconcileReasonDispatcherSlotUnavailable); err != nil {
		t.Fatalf("release recovered only claim: %v", err)
	}
}

func TestRequestCostReconcileDurableClaimRejectsStaleEpochAndToken(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	createdAt := time.Now().UTC().Add(-time.Minute)
	log := appendDueCostReconcileLog(t, store, 4301, 99, "stale-fence", createdAt)

	var databaseBefore time.Time
	if err := db.Raw("SELECT clock_timestamp()").Scan(&databaseBefore).Error; err != nil {
		t.Fatalf("read database clock before claim: %v", err)
	}
	first, acquired, err := store.ClaimRequestCostReconcileDurable(log.ID, time.Minute)
	if err != nil || !acquired || first == nil {
		t.Fatalf("first durable claim = %+v, acquired=%v, err=%v", first, acquired, err)
	}
	var databaseAfter time.Time
	if err := db.Raw("SELECT clock_timestamp()").Scan(&databaseAfter).Error; err != nil {
		t.Fatalf("read database clock after claim: %v", err)
	}
	if first.Log.CostReconcileClaimedAt == nil || first.Log.CostReconcileClaimedAt.Before(databaseBefore) || first.Log.CostReconcileClaimedAt.After(databaseAfter) {
		t.Fatalf("claim time %v is not bounded by database clock [%v, %v]", first.Log.CostReconcileClaimedAt, databaseBefore, databaseAfter)
	}
	if got := first.Fence.LeaseExpiresAt.Sub(*first.Log.CostReconcileClaimedAt); got != time.Minute {
		t.Fatalf("database lease duration = %v, want 1m", got)
	}

	expireDurableCostReconcileClaim(t, db, first.Fence)

	second, acquired, err := store.ClaimRequestCostReconcileDurable(log.ID, time.Minute)
	if err != nil || !acquired || second == nil {
		t.Fatalf("take over expired claim = %+v, acquired=%v, err=%v", second, acquired, err)
	}
	if second.Fence.ClaimEpoch != first.Fence.ClaimEpoch+1 || second.Fence.ClaimToken == first.Fence.ClaimToken {
		t.Fatalf("takeover fence = %+v, first = %+v", second.Fence, first.Fence)
	}
	eventsBefore, err := store.CountRequestCostReconcileEvents(log.ID)
	if err != nil {
		t.Fatalf("count events before stale write: %v", err)
	}
	if err := store.MarkClaimedRequestCostReconcileRetryDurable(first.Fence, time.Now().Add(time.Minute), GatewayRequestCostReconcileReasonLookupFailed, "stale retry"); !errors.Is(err, ErrRequestCostReconcileLeaseLost) {
		t.Fatalf("stale durable retry error = %v, want lease lost", err)
	}
	eventsAfter, err := store.CountRequestCostReconcileEvents(log.ID)
	if err != nil {
		t.Fatalf("count events after stale write: %v", err)
	}
	if eventsAfter != eventsBefore {
		t.Fatalf("stale worker appended audit evidence: before=%d after=%d", eventsBefore, eventsAfter)
	}
	if err := store.MarkClaimedRequestCostReconcileRetryDurable(second.Fence, time.Now().Add(time.Minute), GatewayRequestCostReconcileReasonLookupFailed, "retry later"); err != nil {
		t.Fatalf("current durable retry: %v", err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read retried request: %v", err)
	}
	if stored.CostReconcileState != GatewayRequestCostReconcileRetryScheduled || stored.CostReconcileOutcome != GatewayRequestCostReconcileOutcomeRetry ||
		stored.CostReconcileIdentityLeaseID != nil || stored.CostReconcileClaimEpoch != 0 || stored.CostReconcileClaimToken != "" {
		t.Fatalf("durable retry state = %+v", stored)
	}
}

func TestRequestCostReconcileExpiredIdentityLeaseRequeuesOldRequestBeforeCrossRequestTakeover(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	createdAt := time.Now().UTC().Add(-time.Minute)
	oldLog := appendDueCostReconcileLog(t, store, 4351, 100, "expired-old", createdAt)
	newLog := appendDueCostReconcileLog(t, store, 4351, 100, "takeover-new", createdAt.Add(time.Microsecond))

	oldClaim, acquired, err := store.ClaimRequestCostReconcileDurable(oldLog.ID, time.Minute)
	if err != nil || !acquired || oldClaim == nil {
		t.Fatalf("claim old request = %+v, acquired=%v, err=%v", oldClaim, acquired, err)
	}
	expireDurableCostReconcileClaim(t, db, oldClaim.Fence)

	newClaim, acquired, err := store.ClaimRequestCostReconcileDurable(newLog.ID, time.Minute)
	if err != nil || !acquired || newClaim == nil {
		t.Fatalf("cross-request takeover = %+v, acquired=%v, err=%v", newClaim, acquired, err)
	}
	if newClaim.Fence.ClaimEpoch != oldClaim.Fence.ClaimEpoch+1 || newClaim.Fence.RequestLogID != newLog.ID {
		t.Fatalf("cross-request takeover fence = %+v, old = %+v", newClaim.Fence, oldClaim.Fence)
	}

	var retired GatewayRequestLog
	if err := db.First(&retired, oldLog.ID).Error; err != nil {
		t.Fatalf("read retired old request: %v", err)
	}
	if retired.CostReconcileState != GatewayRequestCostReconcileRetryScheduled || retired.CostReconcileOutcome != GatewayRequestCostReconcileOutcomeRetry ||
		retired.CostReconcileAt == nil || retired.CostReconcileIdentityLeaseID != nil || retired.CostReconcileClaimEpoch != 0 || retired.CostReconcileClaimToken != "" ||
		retired.CostReconcileTries != 1 || retired.CostReconcileError != "billing identity lease expired before a durable outcome" {
		t.Fatalf("expired old request was stranded or incompletely retired: %+v", retired)
	}
	var events []GatewayRequestCostReconcileEvent
	if err := db.Where("request_log_id = ?", oldLog.ID).Order("id ASC").Find(&events).Error; err != nil {
		t.Fatalf("list old request audit events: %v", err)
	}
	if len(events) != 2 || events[1].EventKind != "released" || events[1].Outcome != GatewayRequestCostReconcileOutcomeRetry ||
		events[1].ReasonCode != GatewayRequestCostReconcileReasonIdentityLeaseExpired || events[1].ClaimEpoch != oldClaim.Fence.ClaimEpoch ||
		events[1].ClaimToken != oldClaim.Fence.ClaimToken {
		t.Fatalf("expired old request audit events = %+v", events)
	}
	if err := store.ReleaseRequestCostReconcileDurableClaim(newClaim.Fence, GatewayRequestCostReconcileReasonDispatcherSlotUnavailable); err != nil {
		t.Fatalf("release takeover request: %v", err)
	}
	pending, err := store.ListFairDueRequestCostReconciles(10)
	if err != nil {
		t.Fatalf("list work after takeover release: %v", err)
	}
	if len(pending) == 0 || pending[0].ID != oldLog.ID {
		t.Fatalf("retired old request did not re-enter fair queue: %+v", pending)
	}
}

func TestRequestCostReconcileCrossRequestTakeoverAndStaleCompletionDoNotDeadlock(t *testing.T) {
	db := testdb.Open(t)
	stores := []*Gateway{
		NewGateway(db.Session(&gorm.Session{NewDB: true})),
		NewGateway(db.Session(&gorm.Session{NewDB: true})),
		NewGateway(db.Session(&gorm.Session{NewDB: true})),
	}
	createdAt := time.Now().UTC().Add(-time.Minute)
	oldLog := appendDueCostReconcileLog(t, stores[0], 4361, 101, "deadlock-old", createdAt)
	firstNew := appendDueCostReconcileLog(t, stores[0], 4361, 101, "deadlock-new-a", createdAt.Add(time.Microsecond))
	secondNew := appendDueCostReconcileLog(t, stores[0], 4361, 101, "deadlock-new-b", createdAt.Add(2*time.Microsecond))
	oldClaim, acquired, err := stores[0].ClaimRequestCostReconcileDurable(oldLog.ID, time.Minute)
	if err != nil || !acquired || oldClaim == nil {
		t.Fatalf("claim old request = %+v, acquired=%v, err=%v", oldClaim, acquired, err)
	}
	expireDurableCostReconcileClaim(t, db, oldClaim.Fence)

	type concurrentResult struct {
		kind     string
		claim    *GatewayRequestCostReconcileClaim
		acquired bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan concurrentResult, 3)
	go func() {
		<-start
		err := stores[0].MarkClaimedRequestCostReconcileRetryDurable(oldClaim.Fence, time.Now().Add(time.Minute), GatewayRequestCostReconcileReasonLookupFailed, "stale completion")
		results <- concurrentResult{kind: "stale", err: err}
	}()
	for index, requestID := range []uint{firstNew.ID, secondNew.ID} {
		store := stores[index+1]
		go func(kind string, store *Gateway, requestID uint) {
			<-start
			claim, acquired, err := store.ClaimRequestCostReconcileDurable(requestID, time.Minute)
			results <- concurrentResult{kind: kind, claim: claim, acquired: acquired, err: err}
		}("claim", store, requestID)
	}
	close(start)

	var winner *GatewayRequestCostReconcileClaim
	for completed := 0; completed < 3; completed++ {
		select {
		case result := <-results:
			switch result.kind {
			case "stale":
				if !errors.Is(result.err, ErrRequestCostReconcileLeaseLost) {
					t.Fatalf("stale completion error = %v, want lease lost", result.err)
				}
			case "claim":
				if result.err != nil {
					t.Fatalf("concurrent takeover claim: %v", result.err)
				}
				if result.acquired {
					if winner != nil || result.claim == nil {
						t.Fatalf("multiple concurrent takeover winners: previous=%+v current=%+v", winner, result.claim)
					}
					winner = result.claim
				}
			}
		case <-time.After(5 * time.Second):
			t.Fatal("identity-first takeover and stale completion deadlocked")
		}
	}
	if winner == nil {
		t.Fatal("concurrent cross-request takeover had no winner")
	}
	if err := stores[0].ReleaseRequestCostReconcileDurableClaim(winner.Fence, GatewayRequestCostReconcileReasonDispatcherSlotUnavailable); err != nil {
		t.Fatalf("release concurrent takeover winner: %v", err)
	}
}

func TestRequestCostReconcileDurableTypedTerminalAndSuccessOutcomes(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	createdAt := time.Now().UTC().Add(-time.Minute)
	successLog := appendDueCostReconcileLog(t, store, 4401, 101, "typed-success", createdAt)
	conflictLog := appendDueCostReconcileLog(t, store, 4402, 102, "typed-conflict", createdAt)

	successClaim, acquired, err := store.ClaimRequestCostReconcileDurable(successLog.ID, time.Minute)
	if err != nil || !acquired || successClaim == nil {
		t.Fatalf("claim success request = %+v, acquired=%v, err=%v", successClaim, acquired, err)
	}
	if err := store.UpdateClaimedRequestLogActualCostWithUsageDurable(successClaim.Fence, "typed-usage", 0.25, 0.2, gatewayCostSourceSub2APIActualCost, GatewayRequestUsageMetrics{}); err != nil {
		t.Fatalf("complete durable success: %v", err)
	}
	var succeeded GatewayRequestLog
	if err := db.First(&succeeded, successLog.ID).Error; err != nil {
		t.Fatalf("read durable success: %v", err)
	}
	if succeeded.CostReconcileState != GatewayRequestCostReconcileSucceeded || succeeded.CostReconcileOutcome != GatewayRequestCostReconcileOutcomeSucceeded ||
		succeeded.CostStatus != GatewayCostStatusReconciled || succeeded.Cost != 0.25 {
		t.Fatalf("durable success state = %+v", succeeded)
	}

	conflictClaim, acquired, err := store.ClaimRequestCostReconcileDurable(conflictLog.ID, time.Minute)
	if err != nil || !acquired || conflictClaim == nil {
		t.Fatalf("claim conflict request = %+v, acquired=%v, err=%v", conflictClaim, acquired, err)
	}
	terminal, err := store.MarkClaimedRequestCostReconcileConflictDurable(conflictClaim.Fence, time.Now().Add(time.Minute), 1, GatewayRequestCostReconcileReasonUsageConflict)
	if err != nil || !terminal {
		t.Fatalf("terminal conflict = %v, err=%v", terminal, err)
	}
	var conflicted GatewayRequestLog
	if err := db.First(&conflicted, conflictLog.ID).Error; err != nil {
		t.Fatalf("read terminal conflict: %v", err)
	}
	if conflicted.CostReconcileState != GatewayRequestCostReconcileTerminal || conflicted.CostReconcileOutcome != GatewayRequestCostReconcileOutcomeTerminalConflict ||
		conflicted.CostReconcileAt != nil || conflicted.CostReconcileError != gatewayCostReconcileConflictError {
		t.Fatalf("durable terminal conflict state = %+v", conflicted)
	}
}

func TestRequestCostReconcileDurablePreservesPricingFence(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	dueAt := time.Now().UTC().Add(-time.Minute)
	log := GatewayRequestLog{
		DownstreamKeyID:       1,
		ChannelID:             4501,
		Method:                "POST",
		Path:                  "/v1/responses",
		StatusCode:            200,
		UpstreamRequestID:     "pricing-fence",
		CostStatus:            GatewayCostStatusUnavailable,
		CostUnavailableReason: GatewayPricingUnavailableScopeIncomplete,
		CostReconcileAt:       &dueAt,
		CreatedAt:             dueAt,
	}
	if err := store.AppendRequestLog(&log); err != nil {
		t.Fatalf("append pricing-fenced request: %v", err)
	}
	if claim, acquired, err := store.ClaimRequestCostReconcileDurable(log.ID, time.Minute); err != nil || acquired || claim != nil {
		t.Fatalf("pricing-fenced claim = %+v, acquired=%v, err=%v", claim, acquired, err)
	}
	cleared, err := store.ClearIneligibleRequestCostReconciles(10)
	if err != nil || cleared != 1 {
		t.Fatalf("clear pricing-fenced request = %d, err=%v", cleared, err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read pricing-fenced request: %v", err)
	}
	if stored.CostStatus != GatewayCostStatusUnavailable || stored.CostUnavailableReason != GatewayPricingUnavailableScopeIncomplete ||
		stored.Cost != 0 || stored.RawCost != nil || stored.UpstreamUsageID != nil ||
		stored.CostReconcileState != GatewayRequestCostReconcileTerminal || stored.CostReconcileOutcome != GatewayRequestCostReconcileOutcomePricingUnavailable {
		t.Fatalf("pricing fence changed during durable reconcile cleanup: %+v", stored)
	}
}
