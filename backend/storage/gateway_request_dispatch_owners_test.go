package storage

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestRequestDispatchOwnerIdentityIsOpaqueAndSignedSafe(t *testing.T) {
	first := NewGateway(nil)
	second := NewGateway(nil)
	firstID, firstToken, err := first.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	secondID, secondToken, err := second.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateGatewayRequestDispatchOwnerIdentity(firstID, firstToken); err != nil {
		t.Fatalf("validate first owner identity: %v", err)
	}
	if err := validateGatewayRequestDispatchOwnerIdentity(secondID, secondToken); err != nil {
		t.Fatalf("validate second owner identity: %v", err)
	}
	if firstID == secondID {
		t.Fatalf("request dispatch owner identities collided: %s/%d", firstID, firstToken)
	}
	if firstToken > uint64(^uint64(0)>>1) || secondToken > uint64(^uint64(0)>>1) {
		t.Fatal("request dispatch owner fence token exceeds PostgreSQL BIGINT")
	}
}

func TestRequestDispatchOwnerLeaseUsesDatabaseClockAndNeverRevives(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	applicationNow := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.RegisterRequestDispatchOwner(nil, applicationNow, applicationNow.Add(30*time.Second)); err != nil {
		t.Fatalf("register skewed-clock owner: %v", err)
	}
	instanceID, fenceToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}

	times := readRequestDispatchOwnerTimes(t, db, instanceID, fenceToken)
	assertRequestDispatchOwnerLeaseDuration(t, times, 30*time.Second)

	applicationNow = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := store.RenewRequestDispatchOwner(nil, applicationNow, applicationNow.Add(40*time.Second)); err != nil {
		t.Fatalf("renew skewed-clock owner: %v", err)
	}
	times = readRequestDispatchOwnerTimes(t, db, instanceID, fenceToken)
	assertRequestDispatchOwnerLeaseDuration(t, times, 40*time.Second)

	expireRequestDispatchOwnerForTest(t, db, store)
	if err := store.RenewRequestDispatchOwner(nil, applicationNow, applicationNow.Add(40*time.Second)); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("expired owner renewal error = %v", err)
	}
	if err := store.RegisterRequestDispatchOwner(nil, applicationNow, applicationNow.Add(40*time.Second)); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("expired owner registration error = %v", err)
	}
}

func TestStoppedRequestDispatchOwnerIsImmediatelyRecoverableAndFenced(t *testing.T) {
	db := openTestDB(t)
	ownerStore := NewGateway(db)
	now := time.Now().UTC()
	audit, err := ownerStore.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 64), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.ConfirmRequestDispatchTransportInvocation(audit.ID, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.RecordRequestDispatchResponse(audit.ID, "provider-stopped-owner", now.Add(3*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.MarkRequestDispatchAccepted(audit.ID, "response.created", "response-stopped-owner", now.Add(4*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := ownerStore.StopRequestDispatchOwner(nil, now); err != nil {
		t.Fatalf("stop request dispatch owner: %v", err)
	}

	recoveryStore := NewGateway(db)
	recoverable, err := recoveryStore.ListRecoverableRequestDispatches(now.Add(-time.Hour), now, 10)
	if err != nil || len(recoverable) != 1 || recoverable[0].ID != audit.ID {
		t.Fatalf("stopped owner recoverable dispatches = %+v err=%v", recoverable, err)
	}
	recoveredLog := &GatewayRequestLog{StatusCode: 502, CreatedAt: now}
	created, err := recoveryStore.FinalizeRecoveredRequestDispatchResult(audit.ID, recoveredLog, "orphaned_after_service_interruption", now.Add(-time.Hour), now)
	if err != nil || !created || recoveredLog.ID == 0 {
		t.Fatalf("finalize stopped-owner dispatch: created=%v log=%+v err=%v", created, recoveredLog, err)
	}
	if _, err := ownerStore.FinalizeRequestDispatchResult(audit.ID, &GatewayRequestLog{}, "stale_stopped_owner", now); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("stopped original owner finalize error = %v", err)
	}
}

func TestRotateRequestDispatchOwnerStopsOldFenceAndAcceptsNewDispatches(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	oldAudit, err := store.BeginRequestDispatch(requestDispatchTestSnapshot(now, 65), now)
	if err != nil {
		t.Fatal(err)
	}
	oldID, oldToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RotateRequestDispatchOwner(nil, now, now.Add(GatewayRequestDispatchOwnerLeaseDuration)); err != nil {
		t.Fatalf("rotate request dispatch owner: %v", err)
	}
	newID, newToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if newID == oldID || newToken == oldToken {
		t.Fatalf("owner fence was not rotated: old=%s/%d new=%s/%d", oldID, oldToken, newID, newToken)
	}
	var oldOwner GatewayRequestDispatchOwner
	if err := db.First(&oldOwner, "instance_id = ? AND fence_token = ?", oldID, oldToken).Error; err != nil {
		t.Fatal(err)
	}
	if oldOwner.StoppedAt == nil {
		t.Fatalf("rotated owner remained active: %+v", oldOwner)
	}
	if err := store.RecordRequestDispatchResponse(oldAudit.ID, "stale-owner-response", now); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("old dispatch accepted new owner write: %v", err)
	}
	newAudit, err := store.BeginRequestDispatch(requestDispatchTestSnapshot(now, 66), now)
	if err != nil {
		t.Fatalf("begin dispatch with replacement owner: %v", err)
	}
	if newAudit.OwnerInstanceID != newID || newAudit.OwnerFenceToken != newToken {
		t.Fatalf("replacement dispatch owner = %s/%d, want %s/%d", newAudit.OwnerInstanceID, newAudit.OwnerFenceToken, newID, newToken)
	}
}

type requestDispatchOwnerTimes struct {
	StartedAt      time.Time `gorm:"column:started_at"`
	HeartbeatAt    time.Time `gorm:"column:heartbeat_at"`
	LeaseExpiresAt time.Time `gorm:"column:lease_expires_at"`
	DatabaseNow    time.Time `gorm:"column:database_now"`
}

func readRequestDispatchOwnerTimes(t *testing.T, db *gorm.DB, instanceID string, fenceToken uint64) requestDispatchOwnerTimes {
	t.Helper()
	var times requestDispatchOwnerTimes
	if err := db.Raw(`SELECT started_at, heartbeat_at, lease_expires_at, clock_timestamp() AS database_now
		FROM gateway_request_dispatch_owners
		WHERE instance_id = ? AND fence_token = ?`, instanceID, fenceToken).Scan(&times).Error; err != nil {
		t.Fatalf("read request dispatch owner times: %v", err)
	}
	return times
}

func assertRequestDispatchOwnerLeaseDuration(t *testing.T, times requestDispatchOwnerTimes, want time.Duration) {
	t.Helper()
	if times.StartedAt.IsZero() || times.HeartbeatAt.IsZero() || times.LeaseExpiresAt.IsZero() || times.DatabaseNow.IsZero() {
		t.Fatalf("incomplete request dispatch owner times: %+v", times)
	}
	remaining := times.LeaseExpiresAt.Sub(times.DatabaseNow)
	if remaining < want-2*time.Second || remaining > want+time.Second {
		t.Fatalf("database-clock lease remaining = %v, want about %v", remaining, want)
	}
}

// TestRotateForcedDrainRequestDispatchOwnerRecordsImmutableDegradeAudit covers
// the bounded drain degrade: the abandoned incarnation must carry a traceable,
// append-only record of what was still outstanding, the record must never be
// rewritten, and the old identity must remain able to persist its exact
// terminal handoff afterwards.
func TestRotateForcedDrainRequestDispatchOwnerRecordsImmutableDegradeAudit(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 67), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(audit.ID, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	oldID, oldToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}

	forced := GatewayRequestDispatchForcedDrain{ActiveBindings: 2, PendingHandoffs: 1, DrainWaitedFor: 90 * time.Second, DrainMaximumWait: 90 * time.Second}
	if err := store.RotateForcedDrainRequestDispatchOwner(nil, now, now.Add(GatewayRequestDispatchOwnerLeaseDuration), forced); err != nil {
		t.Fatalf("rotate after forced drain: %v", err)
	}
	var abandoned GatewayRequestDispatchOwner
	if err := db.First(&abandoned, "instance_id = ? AND fence_token = ?", oldID, oldToken).Error; err != nil {
		t.Fatal(err)
	}
	if abandoned.StoppedAt == nil || abandoned.DrainForcedAt == nil {
		t.Fatalf("forced drain was not recorded on the abandoned incarnation: %+v", abandoned)
	}
	if abandoned.DrainForcedBindings != 2 || abandoned.DrainForcedHandoffs != 1 {
		t.Fatalf("forced drain counters = %d/%d, want 2/1", abandoned.DrainForcedBindings, abandoned.DrainForcedHandoffs)
	}

	// Point the in-memory owner back at the abandoned incarnation to model a
	// stale concurrent rotator. The drain_forced_at guard must keep the first
	// degrade audit append-only.
	store.requestDispatchOwnerMu.Lock()
	store.requestDispatchOwnerID = oldID
	store.requestDispatchOwnerFenceToken = oldToken
	store.requestDispatchOwnerRegistered = true
	store.requestDispatchOwnerStopped = false
	store.requestDispatchOwnerMu.Unlock()
	if err := store.RotateForcedDrainRequestDispatchOwner(nil, now, now.Add(GatewayRequestDispatchOwnerLeaseDuration), GatewayRequestDispatchForcedDrain{ActiveBindings: 9, PendingHandoffs: 9}); err != nil {
		t.Fatalf("second rotation: %v", err)
	}
	var rechecked GatewayRequestDispatchOwner
	if err := db.First(&rechecked, "instance_id = ? AND fence_token = ?", oldID, oldToken).Error; err != nil {
		t.Fatal(err)
	}
	if rechecked.DrainForcedBindings != 2 || rechecked.DrainForcedHandoffs != 1 || !rechecked.DrainForcedAt.Equal(*abandoned.DrainForcedAt) {
		t.Fatalf("forced drain audit was rewritten: %+v", rechecked)
	}

	// The bounded degrade must not cost the straggler its exact snapshot: the
	// dispatch audit, not the live lease, authorizes the durable handoff.
	log := &GatewayRequestLog{StatusCode: 200, PromptTokens: 11, CompletionTokens: 19, TotalTokens: 30, CreatedAt: now.Add(3 * time.Millisecond)}
	persisted, err := store.PersistRequestDispatchTerminalHandoff(
		audit.ID, oldID, oldToken, GatewayRequestDispatchTerminalOwnerLeaseLost,
		now.Add(3*time.Millisecond), now, now.Add(3*time.Millisecond),
		log, GatewayRequestDispatchFinalizationFacts{}, audit.TelemetryVersion, true,
	)
	if err != nil || !persisted {
		t.Fatalf("exact handoff after forced rotation: persisted=%t err=%v", persisted, err)
	}
	handoffs, err := store.ListRequestDispatchTerminalHandoffs(10)
	if err != nil || len(handoffs) != 1 {
		t.Fatalf("durable handoffs after forced rotation = %+v err=%v", handoffs, err)
	}
	decoded, err := DecodeGatewayRequestDispatchTerminalHandoff(handoffs[0])
	if err != nil || decoded.Log == nil || decoded.Log.TotalTokens != 30 {
		t.Fatalf("forced rotation lost the exact terminal snapshot: %+v err=%v", decoded.Log, err)
	}
}
