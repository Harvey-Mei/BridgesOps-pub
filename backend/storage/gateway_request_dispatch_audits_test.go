package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

type requestDispatchCommitUnknownPool struct {
	gorm.ConnPool
	injected atomic.Bool
}

func (p *requestDispatchCommitUnknownPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	beginner, ok := p.ConnPool.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	})
	if !ok {
		return nil, gorm.ErrInvalidTransaction
	}
	tx, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &requestDispatchCommitUnknownTx{Tx: tx, pool: p}, nil
}

type requestDispatchCommitUnknownTx struct {
	*sql.Tx
	pool *requestDispatchCommitUnknownPool
}

func (tx *requestDispatchCommitUnknownTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.pool.injected.CompareAndSwap(false, true) {
		return driver.ErrBadConn
	}
	return nil
}

func requestDispatchTestNumber(value float64) *float64 {
	return &value
}

func requestDispatchTestSnapshot(now time.Time, channelID uint) GatewayRequestDispatchSnapshot {
	affinityExpiresAt := now.Add(9 * time.Minute)
	return GatewayRequestDispatchSnapshot{
		DownstreamKeyID:                     7,
		ChannelID:                           channelID,
		ChannelNameSnapshot:                 "provider",
		GroupName:                           "primary",
		UpstreamKeyID:                       88,
		UpstreamKeyName:                     "bridgeops-primary",
		ModelName:                           "gpt-5",
		UpstreamModelName:                   "gpt-5-upstream",
		ReasoningEffort:                     "high",
		RequestedModelName:                  "gpt-5-requested",
		RequestedReasoningEffort:            "medium",
		ModelMappingStrategy:                "balanced",
		ModelMappingDetails:                 "综合平衡：gpt-5-requested -> gpt-5",
		Method:                              "POST",
		Path:                                "/v1/responses",
		ClientIP:                            "203.0.113.7",
		ClientIPSource:                      "trusted_proxy",
		SessionHash:                         strings.Repeat("a", 64),
		RouteReason:                         "smart_primary",
		RouteSelectionRank:                  1,
		SessionAffinityOriginRequestID:      321,
		SessionAffinityExpiresAt:            &affinityExpiresAt,
		SessionAffinityOverriddenCandidates: `[{"channel_id":14,"channel_name":"higher","group_name":"priority","eligible_rank":1}]`,
		RouteDetails:                        "provider/primary selected",
		RouteAttemptedChannels:              `[{"channel_id":41,"reason":"selected"}]`,
		GroupRatio:                          0.5,
		CompletionRatio:                     2,
		RawGroupRatio:                       requestDispatchTestNumber(0.5),
		RawCompletionRatio:                  requestDispatchTestNumber(2),
		FundingFactor:                       requestDispatchTestNumber(1.25),
		CostBasisVersion:                    3,
		CostBasisSource:                     "channel_balance",
		RequestStartedAt:                    now.Add(-time.Second),
	}
}

func requestDispatchImageTestSnapshot(t *testing.T, now time.Time, channelID uint) GatewayRequestDispatchSnapshot {
	t.Helper()
	snapshot := requestDispatchTestSnapshot(now, channelID)
	snapshot.Path = "/v1/images/generations"
	snapshot.PriceOperation = GatewayImageOperationGeneration
	snapshot.PriceProtocol = GatewayImageProtocolImages
	snapshot.UsageSchema = GatewayPricingUsageSchemaOperationV1
	snapshot.UsageSchemaVersion = 1
	snapshot.UsageOperationPresent = true
	operationUnits := int64(1)
	snapshot.OperationUnits = &operationUnits
	return requestDispatchSnapshotWithCurrentTelemetry(t, snapshot, now)
}

func requestDispatchCurrentTelemetryTestSnapshot(t *testing.T, now time.Time, channelID uint) GatewayRequestDispatchSnapshot {
	t.Helper()
	return requestDispatchSnapshotWithCurrentTelemetry(t, requestDispatchTestSnapshot(now, channelID), now)
}

func requestDispatchSnapshotWithCurrentTelemetry(t *testing.T, snapshot GatewayRequestDispatchSnapshot, now time.Time) GatewayRequestDispatchSnapshot {
	t.Helper()
	routeSnapshot, err := NewGatewayRouteExclusionSnapshot(now, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("create route exclusion snapshot: %v", err)
	}
	snapshot.LogicalRequestTraceID = strings.Repeat("c", 32)
	snapshot.TelemetryVersion = GatewayRequestTelemetryVersion
	snapshot.RoutingMode = GatewayRoutingModeSmart
	snapshot.GatewayApp = string(GatewayAppCodex)
	snapshot.RouteExclusionSnapshotVersion = GatewayRouteExclusionSnapshotVersion
	snapshot.RouteExclusionsSnapshot = routeSnapshot
	return snapshot
}

func expireRequestDispatchOwnerForTest(t *testing.T, db *gorm.DB, store *Gateway) {
	t.Helper()
	instanceID, fenceToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatalf("read request dispatch owner identity: %v", err)
	}
	result := db.Exec(`UPDATE gateway_request_dispatch_owners
		SET started_at = LEAST(started_at, clock_timestamp() - interval '3 seconds'),
			heartbeat_at = clock_timestamp() - interval '2 seconds',
			lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE instance_id = ? AND fence_token = ? AND stopped_at IS NULL`, instanceID, fenceToken)
	if result.Error != nil {
		t.Fatalf("expire request dispatch owner: %v", result.Error)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("expired request dispatch owners = %d, want 1", result.RowsAffected)
	}
}

func confirmPlainRequestDispatchForTest(t *testing.T, store *Gateway, dispatchID uint64, at time.Time) {
	t.Helper()
	if err := store.MarkRequestDispatchPhysicallyDispatched(dispatchID, at); err != nil {
		t.Fatalf("mark request dispatch physical: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(dispatchID, at.Add(time.Millisecond)); err != nil {
		t.Fatalf("confirm request dispatch transport: %v", err)
	}
}

func confirmImageRequestDispatchForTest(t *testing.T, store *Gateway, dispatchID uint64, at time.Time) {
	t.Helper()
	if err := store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(dispatchID, at); err != nil {
		t.Fatalf("mark image request dispatch physical: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(dispatchID, at.Add(time.Millisecond)); err != nil {
		t.Fatalf("confirm image request dispatch transport: %v", err)
	}
}

func TestRequestDispatchSnapshotRejectsUnsafeShape(t *testing.T) {
	typeOfSnapshot := reflect.TypeOf(GatewayRequestDispatchSnapshot{})
	for _, forbidden := range []string{"Body", "APIKey", "Credential", "Authorization", "SessionID", "UpstreamResponse"} {
		if _, ok := typeOfSnapshot.FieldByName(forbidden); ok {
			t.Fatalf("dispatch snapshot exposes forbidden field %s", forbidden)
		}
	}
	now := time.Now().UTC()
	snapshot := requestDispatchTestSnapshot(now, 40)
	snapshot.SessionHash = "raw-session-id"
	if _, err := normalizeGatewayRequestDispatchSnapshot(snapshot, now); err == nil || !strings.Contains(err.Error(), "SHA-256") {
		t.Fatalf("raw session id error = %v", err)
	}
	snapshot = requestDispatchTestSnapshot(now, 40)
	snapshot.RouteDetails = strings.Repeat("x", 4097)
	if _, err := normalizeGatewayRequestDispatchSnapshot(snapshot, now); err == nil || !strings.Contains(err.Error(), "safe length") {
		t.Fatalf("oversized route details error = %v", err)
	}
}

func TestGatewayRequestDispatchIdentityConflictComparesRouteSnapshotAsJSON(t *testing.T) {
	audit := &GatewayRequestDispatchAudit{
		RouteExclusionSnapshotVersion: GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:       `{"entries": [], "version": 1, "captured_at": "2026-07-25T00:00:00Z"}`,
	}
	log := &GatewayRequestLog{
		RouteExclusionSnapshotVersion: GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:       `{"version":1,"captured_at":"2026-07-25T00:00:00Z","entries":[]}`,
	}
	if gatewayRequestDispatchIdentityConflict(audit, log) {
		t.Fatal("equivalent JSONB route snapshots were treated as different identities")
	}
	log.RouteExclusionsSnapshot = `{"version":1,"captured_at":"2026-07-25T00:00:01Z","entries":[]}`
	if !gatewayRequestDispatchIdentityConflict(audit, log) {
		t.Fatal("different route snapshot facts were accepted as the same identity")
	}
	audit.RouteExclusionsSnapshot = `{"version":1,"captured_at":"2026-07-25T00:00:00Z","total_entries":1,"truncated":false,"entries":[{"channel_id":1,"group_name":"primary","reason":"model_not_listed","upstream_group_id":1,"presence_generation":9007199254740992}]}`
	log.RouteExclusionsSnapshot = `{"version":1,"captured_at":"2026-07-25T00:00:00Z","total_entries":1,"truncated":false,"entries":[{"channel_id":1,"group_name":"primary","reason":"model_not_listed","upstream_group_id":1,"presence_generation":9007199254740993}]}`
	if !gatewayRequestDispatchIdentityConflict(audit, log) {
		t.Fatal("adjacent route generations above the JSON float precision boundary were treated as equal")
	}
}

func TestRequestDispatchPreSendBillingInvariantIsPureAndImageScoped(t *testing.T) {
	now := time.Now().UTC()
	imageSnapshot, err := normalizeGatewayRequestDispatchSnapshot(requestDispatchImageTestSnapshot(t, now, 401), now)
	if err != nil {
		t.Fatalf("normalize image snapshot: %v", err)
	}
	imageAudit := gatewayRequestDispatchAuditFromSnapshot(imageSnapshot, strings.Repeat("0", 32), 1, now)
	if imageAudit.BillingCommitted || imageAudit.PhysicallyDispatched || imageAudit.PhysicalBillingInvariantVersion != GatewayRequestDispatchPhysicalBillingInvariantVersion {
		t.Fatalf("pre-send image audit invariant = %+v", imageAudit)
	}
	if !gatewayRequestDispatchRequiresBillingCommitAtPhysicalDispatch(imageAudit) {
		t.Fatalf("frozen image audit was not recognized: %+v", imageAudit)
	}
	if gatewayRequestDispatchHasTrustedBillingCommitment(imageAudit) || gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(imageAudit) {
		t.Fatalf("fresh image audit unexpectedly has billing evidence: %+v", imageAudit)
	}

	textSnapshot, err := normalizeGatewayRequestDispatchSnapshot(requestDispatchTestSnapshot(now, 402), now)
	if err != nil {
		t.Fatalf("normalize text snapshot: %v", err)
	}
	textAudit := gatewayRequestDispatchAuditFromSnapshot(textSnapshot, strings.Repeat("1", 32), 2, now)
	if gatewayRequestDispatchRequiresBillingCommitAtPhysicalDispatch(textAudit) {
		t.Fatalf("text audit was classified as image billing: %+v", textAudit)
	}
	textAudit.PhysicallyDispatched = true
	if gatewayRequestDispatchHasTrustedBillingCommitment(textAudit) || gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(textAudit) {
		t.Fatalf("physical text audit was treated as billing commitment: %+v", textAudit)
	}
	imageAudit.PhysicallyDispatched = true
	imageAudit.BillingCommitted = true
	imageAudit.TransportState = GatewayRequestDispatchTransportStateConfirmed
	if !gatewayRequestDispatchHasTrustedBillingCommitment(imageAudit) || gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(imageAudit) {
		t.Fatalf("trusted image billing classification = %+v", imageAudit)
	}
	imageAudit.PhysicallyDispatched = false
	if gatewayRequestDispatchHasTrustedBillingCommitment(imageAudit) || !gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(imageAudit) {
		t.Fatalf("prephysical v1 billing classification = %+v", imageAudit)
	}
	imageAudit.PhysicalBillingInvariantVersion = 0
	if gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(imageAudit) {
		t.Fatalf("legacy billing was reclassified from a physical default: %+v", imageAudit)
	}
}

func TestConfirmRequestDispatchTransportInvocationAllowsTerminalWriteAfterTransportError(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 403), now)
	if err != nil {
		t.Fatalf("begin dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatalf("arm dispatch before transport: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(audit.ID, now.Add(2*time.Millisecond)); err != nil {
		t.Fatalf("confirm returned transport: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(audit.ID, now.Add(3*time.Millisecond)); err != nil {
		t.Fatalf("repeat transport confirmation: %v", err)
	}

	log := &GatewayRequestLog{StatusCode: 502, Error: "transport returned an error", CreatedAt: now.Add(4 * time.Millisecond)}
	created, err := store.FinalizeRequestDispatchResult(audit.ID, log, "transport_error", log.CreatedAt)
	if err != nil || !created || log.ID == 0 {
		t.Fatalf("finalize confirmed transport error: created=%t log=%+v err=%v", created, log, err)
	}
	persisted, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.TransportState != GatewayRequestDispatchTransportStateConfirmed || persisted.TransportConfirmationVersion != 1 || !persisted.TransportReturnConfirmed || persisted.TransportReturnConfirmedAt == nil || !persisted.PhysicallyDispatched || persisted.RequestLogID == nil || *persisted.RequestLogID != log.ID {
		t.Fatalf("confirmed terminal dispatch = %+v", persisted)
	}
}

func TestConfirmRequestDispatchTransportInvocationCommitUnknownNeverRevertsToPrepared(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 404), now)
	if err != nil {
		t.Fatalf("begin dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatalf("arm dispatch before transport: %v", err)
	}

	faultDB := db.Session(&gorm.Session{NewDB: true})
	pool := &requestDispatchCommitUnknownPool{ConnPool: faultDB.Statement.ConnPool}
	faultDB.Statement.ConnPool = pool
	store.db = faultDB
	err = store.ConfirmRequestDispatchTransportInvocation(audit.ID, now.Add(2*time.Millisecond))
	if !errors.Is(err, ErrGatewayRequestDispatchCommitUnknown) || !pool.injected.Load() {
		t.Fatalf("confirm commit-unknown error=%v injected=%t", err, pool.injected.Load())
	}

	persisted, err := NewGateway(db).FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.TransportState != GatewayRequestDispatchTransportStateConfirmed || persisted.TransportConfirmationVersion != 1 || !persisted.TransportReturnConfirmed || persisted.TransportReturnConfirmedAt == nil || !persisted.PhysicallyDispatched {
		t.Fatalf("ambiguous confirmation lost physical invocation: %+v", persisted)
	}
}

func TestBeginNextRequestDispatchDefersPredecessorCloseUntilSuccessorConfirmed(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	previous, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 405), now)
	if err != nil {
		t.Fatalf("begin previous dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(previous.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatalf("arm previous dispatch: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(previous.ID, now.Add(2*time.Millisecond)); err != nil {
		t.Fatalf("confirm previous dispatch: %v", err)
	}

	nextSnapshot := requestDispatchCurrentTelemetryTestSnapshot(t, now, 406)
	nextSnapshot.PriorPhysicallyDispatchedFailureCount = 1
	next, err := store.BeginNextRequestDispatch(previous.ID, nextSnapshot, now.Add(3*time.Millisecond))
	if err != nil {
		t.Fatalf("begin linked successor: %v", err)
	}
	persistedPrevious, err := store.FindRequestDispatchAudit(previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persistedPrevious.TerminalAt != nil || persistedPrevious.RequestLogID != nil || persistedPrevious.Stage != GatewayRequestDispatchStageDispatched {
		t.Fatalf("successor preparation closed predecessor: %+v", persistedPrevious)
	}
	if next.TransportState != GatewayRequestDispatchTransportStatePrepared || next.PhysicallyDispatched || next.RolloverPredecessorID == nil || *next.RolloverPredecessorID != previous.ID || !next.RealFallback {
		t.Fatalf("prepared linked successor = %+v", next)
	}

	if err := store.MarkRequestDispatchPhysicallyDispatched(next.ID, now.Add(4*time.Millisecond)); err != nil {
		t.Fatalf("arm successor: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(next.ID, now.Add(5*time.Millisecond)); err != nil {
		t.Fatalf("confirm successor: %v", err)
	}
	if _, err := store.ClosePreviousRequestDispatchAfterNextPhysical(
		previous.ID,
		next.ID,
		nil,
		"failed_before_acceptance",
		GatewayRequestDispatchFinalizationFacts{},
		now.Add(2*time.Millisecond),
	); err != nil {
		t.Fatalf("close predecessor after confirmed successor: %v", err)
	}
	persistedPrevious, err = store.FindRequestDispatchAudit(previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	persistedNext, err := store.FindRequestDispatchAudit(next.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persistedPrevious.Stage != GatewayRequestDispatchStageCompleted || persistedPrevious.TerminalAt == nil || persistedPrevious.RequestLogID != nil {
		t.Fatalf("confirmed successor did not close predecessor: %+v", persistedPrevious)
	}
	if persistedNext.TransportState != GatewayRequestDispatchTransportStateConfirmed || !persistedNext.PhysicallyDispatched || persistedNext.TerminalAt != nil {
		t.Fatalf("successor changed while closing predecessor: %+v", persistedNext)
	}
}

func TestRequestDispatchFinalizeResultValidationPreservesLogID(t *testing.T) {
	store := &Gateway{db: &gorm.DB{}}
	requestLog := &GatewayRequestLog{ID: 73}

	created, err := store.FinalizeRequestDispatchResult(1, requestLog, strings.Repeat("x", 65), time.Now())
	if err == nil {
		t.Fatal("invalid terminal kind unexpectedly finalized dispatch")
	}
	if created {
		t.Fatal("invalid terminal kind reported a newly created request log")
	}
	if requestLog.ID != 73 {
		t.Fatalf("validation error changed log id to %d, want 73", requestLog.ID)
	}
}

func TestBeginNextRequestDispatchCommitUnknownReturnsPossiblePersistedNext(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	previous, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 140), now)
	if err != nil {
		t.Fatalf("begin previous dispatch: %v", err)
	}
	confirmPlainRequestDispatchForTest(t, store, previous.ID, now.Add(time.Millisecond))
	responseAt := now.Add(time.Second)
	if err := store.RecordRequestDispatchResponse(previous.ID, "provider-rollover-commit-unknown", responseAt); err != nil {
		t.Fatalf("record previous response: %v", err)
	}
	finishedAt := now.Add(2 * time.Second)
	reconcileAt := finishedAt.Add(time.Minute)
	evidence := &GatewayRequestLog{
		RecordKind:        GatewayRequestRecordKindDispatchCostEvidence,
		UpstreamRequestID: "provider-rollover-commit-unknown",
		CostReconcileAt:   &reconcileAt,
		CreatedAt:         finishedAt,
	}

	faultDB := db.Session(&gorm.Session{NewDB: true})
	pool := &requestDispatchCommitUnknownPool{ConnPool: faultDB.Statement.ConnPool}
	faultDB.Statement.ConnPool = pool
	store.db = faultDB
	next, previousLogCreated, err := store.BeginNextRequestDispatchAndClosePrevious(
		previous.ID,
		evidence,
		"failed_before_acceptance",
		GatewayRequestDispatchFinalizationFacts{
			ProviderRequestID:  "provider-rollover-commit-unknown",
			ResponseObservedAt: responseAt,
		},
		finishedAt,
		func() GatewayRequestDispatchSnapshot {
			snapshot := requestDispatchCurrentTelemetryTestSnapshot(t, now, 141)
			snapshot.PriorPhysicallyDispatchedFailureCount = 1
			return snapshot
		}(),
		finishedAt.Add(time.Second),
	)
	store.db = db

	if !pool.injected.Load() {
		t.Fatal("commit-unknown connection fault was not injected")
	}
	if !errors.Is(err, ErrGatewayRequestDispatchCommitUnknown) {
		t.Fatalf("rollover error = %v, want commit-unknown", err)
	}
	if previousLogCreated {
		t.Fatal("commit-unknown rollover reported a confirmed previous log")
	}
	if next == nil || next.ID == 0 {
		t.Fatalf("commit-unknown rollover lost possible next audit identity: %+v", next)
	}
	if err := store.HandoffLinkedRequestDispatchRecovery(
		previous.ID,
		next.ID,
		GatewayRequestDispatchTerminalOwnerLeaseLost,
		GatewayRequestDispatchFinalizationFacts{
			ProviderRequestID:  "provider-rollover-commit-unknown",
			ResponseObservedAt: responseAt,
		},
		true,
		finishedAt.Add(3*time.Second),
	); err != nil {
		t.Fatalf("handoff possible linked dispatches: %v", err)
	}

	persistedPrevious, err := store.FindRequestDispatchAudit(previous.ID)
	if err != nil {
		t.Fatalf("read previous dispatch: %v", err)
	}
	if persistedPrevious.Stage != GatewayRequestDispatchStageFinalized || persistedPrevious.RequestLogID == nil || persistedPrevious.TerminalAt == nil {
		t.Fatalf("previous dispatch was not atomically finalized: %+v", persistedPrevious)
	}
	persistedNext, err := store.FindRequestDispatchAudit(next.ID)
	if err != nil {
		t.Fatalf("read possible next dispatch: %v", err)
	}
	if persistedNext.Stage != GatewayRequestDispatchStageDispatched || persistedNext.TerminalAt != nil || persistedNext.ResponseObservedAt != nil || persistedNext.BillingCommitted || persistedNext.RecoveryRequestedAt == nil {
		t.Fatalf("possible next dispatch should remain unsent for recovery: %+v", persistedNext)
	}
	var persistedEvidence GatewayRequestLog
	if err := db.First(&persistedEvidence, *persistedPrevious.RequestLogID).Error; err != nil {
		t.Fatalf("read previous dispatch evidence: %v", err)
	}
	if persistedEvidence.RecordKind != GatewayRequestRecordKindDispatchCostEvidence || persistedEvidence.UpstreamRequestID != "provider-rollover-commit-unknown" {
		t.Fatalf("previous exact provider evidence = %+v", persistedEvidence)
	}
}

func TestBeginRequestDispatchCommitUnknownHandoffKeepsUnsentImageNonBillable(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	if err := store.RegisterRequestDispatchOwner(context.Background(), now, now.Add(GatewayRequestDispatchOwnerLeaseDuration)); err != nil {
		t.Fatalf("initialize request dispatch owner: %v", err)
	}

	faultDB := db.Session(&gorm.Session{NewDB: true})
	pool := &requestDispatchCommitUnknownPool{ConnPool: faultDB.Statement.ConnPool}
	faultDB.Statement.ConnPool = pool
	store.db = faultDB
	audit, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now, 142), now)
	store.db = db

	if !pool.injected.Load() || !errors.Is(err, ErrGatewayRequestDispatchCommitUnknown) || audit == nil || audit.ID == 0 {
		t.Fatalf("initial commit-unknown audit=%+v injected=%t err=%v", audit, pool.injected.Load(), err)
	}
	if err := store.HandoffUnsentRequestDispatchRecovery(audit.ID, GatewayRequestDispatchTerminalOwnerLeaseLost, now.Add(time.Second)); err != nil {
		t.Fatalf("handoff initial unsent dispatch: %v", err)
	}
	persisted, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatalf("read handed-off initial dispatch: %v", err)
	}
	if persisted.Stage != GatewayRequestDispatchStageDispatched || persisted.BillingCommitted || persisted.PhysicallyDispatched || persisted.ResponseObservedAt != nil || persisted.RecoveryRequestedAt == nil || persisted.RecoveryTerminalKind != GatewayRequestDispatchTerminalOwnerLeaseLost || persisted.PhysicalBillingInvariantVersion != GatewayRequestDispatchPhysicalBillingInvariantVersion {
		t.Fatalf("initial unsent dispatch handoff = %+v", persisted)
	}
}

func TestRequestDispatchFinalizePersistsFactsAndIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 41), now)
	if err != nil {
		t.Fatalf("begin dispatch: %v", err)
	}
	confirmPlainRequestDispatchForTest(t, store, audit.ID, now.Add(time.Millisecond))
	if err := store.RecordRequestDispatchResponse(audit.ID, "provider-request-1", now.Add(time.Second)); err != nil {
		t.Fatalf("record response: %v", err)
	}
	if err := store.RecordRequestDispatchResponse(audit.ID, "provider-request-1", now.Add(2*time.Second)); err != nil {
		t.Fatalf("repeat response: %v", err)
	}
	if err := store.MarkRequestDispatchAccepted(audit.ID, "response.created", "resp-object-1", now.Add(2*time.Second)); err != nil {
		t.Fatalf("mark accepted: %v", err)
	}
	if err := store.MarkRequestDispatchAccepted(audit.ID, "response.created", "resp-object-1", now.Add(3*time.Second)); err != nil {
		t.Fatalf("repeat accepted: %v", err)
	}

	finishedAt := now.Add(4 * time.Second)
	reconcileAt := finishedAt.Add(time.Minute)
	requestLog := &GatewayRequestLog{
		StatusCode:      502,
		LatencyMs:       finishedAt.Sub(now.Add(-time.Second)).Milliseconds(),
		CostReconcileAt: &reconcileAt,
		Error:           "upstream stream disconnected after acceptance",
		CreatedAt:       finishedAt,
	}
	created, err := store.FinalizeRequestDispatchResult(audit.ID, requestLog, "stream_transport_error", finishedAt)
	if err != nil {
		t.Fatalf("finalize dispatch: %v", err)
	}
	if !created {
		t.Fatal("first finalize did not report a newly created request log")
	}
	if requestLog.ID == 0 {
		t.Fatal("finalized request log has no id")
	}
	if requestLog.UpstreamRequestID != "provider-request-1" || !requestLog.UpstreamAccepted || requestLog.BillingCommitted {
		t.Fatalf("finalized billing facts = %+v", requestLog)
	}
	if requestLog.ChannelID != 41 || requestLog.GroupName != "primary" || requestLog.ModelName != "gpt-5" || requestLog.Method != "POST" || requestLog.Path != "/v1/responses" {
		t.Fatalf("finalized route snapshot = %+v", requestLog)
	}
	if requestLog.ClientIP != "203.0.113.7" || requestLog.ClientIPSource != "trusted_proxy" || requestLog.SessionHash != strings.Repeat("a", 64) || requestLog.SessionAffinityOriginRequestID != 321 {
		t.Fatalf("finalized client/session snapshot = %+v", requestLog)
	}
	if requestLog.SessionAffinityExpiresAt == nil || !requestLog.SessionAffinityExpiresAt.Equal(now.Add(9*time.Minute)) || !strings.Contains(requestLog.SessionAffinityOverriddenCandidates, `"channel_name":"higher"`) {
		t.Fatalf("finalized affinity audit snapshot = %+v", requestLog)
	}
	if requestLog.UpstreamModelName != "gpt-5-upstream" || requestLog.ReasoningEffort != "high" || requestLog.RequestedModelName != "gpt-5-requested" || requestLog.ModelMappingStrategy != "balanced" || requestLog.ModelMappingDetails == "" {
		t.Fatalf("finalized model mapping snapshot = %+v", requestLog)
	}
	if requestLog.RouteDetails != "provider/primary selected" || requestLog.RouteAttemptedChannels == "" {
		t.Fatalf("finalized route audit snapshot = %+v", requestLog)
	}

	storedAudit, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatalf("find dispatch audit: %v", err)
	}
	if storedAudit.Stage != GatewayRequestDispatchStageFinalized || storedAudit.TerminalAt == nil || storedAudit.RequestLogID == nil || *storedAudit.RequestLogID != requestLog.ID {
		t.Fatalf("finalized audit = %+v", storedAudit)
	}
	if storedAudit.ProviderRequestID != "provider-request-1" || storedAudit.AcceptedEventType != "response.created" || storedAudit.AcceptedObjectID != "resp-object-1" {
		t.Fatalf("separate provider/object ids were not preserved: %+v", storedAudit)
	}

	created, err = store.FinalizeRequestDispatchResult(audit.ID, requestLog, "retry_same_object", finishedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("idempotent finalize with same log: %v", err)
	}
	if created {
		t.Fatal("idempotent finalize with same log reported a new request log")
	}
	retry := &GatewayRequestLog{}
	created, err = store.FinalizeRequestDispatchResult(audit.ID, retry, "retry_after_ambiguous_commit", finishedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("idempotent finalize: %v", err)
	}
	if created {
		t.Fatal("idempotent finalize after ambiguous commit reported a new request log")
	}
	if retry.ID != requestLog.ID {
		t.Fatalf("idempotent finalize id = %d, want %d", retry.ID, requestLog.ID)
	}
	if retry.ChannelID != 41 || retry.UpstreamRequestID != "provider-request-1" || !retry.UpstreamAccepted || retry.ClientIP != "203.0.113.7" || retry.ModelMappingDetails == "" || retry.RouteDetails == "" {
		t.Fatalf("idempotent finalize did not return complete persisted log: %+v", retry)
	}
	conflictingID := requestLog.ID + 100
	conflicting := &GatewayRequestLog{ID: conflictingID}
	created, err = store.FinalizeRequestDispatchResult(audit.ID, conflicting, "conflicting_retry", finishedAt.Add(2*time.Second))
	if !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("conflicting retry error = %v", err)
	}
	if created {
		t.Fatal("conflicting retry reported a new request log")
	}
	if conflicting.ID != conflictingID {
		t.Fatalf("conflicting retry changed log id to %d, want %d", conflicting.ID, conflictingID)
	}
	if err := store.FinalizeRequestDispatch(audit.ID, &GatewayRequestLog{}, "compatibility_wrapper", finishedAt.Add(3*time.Second)); err != nil {
		t.Fatalf("compatibility wrapper retry: %v", err)
	}
	var requestLogCount int64
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", requestLog.ID).Count(&requestLogCount).Error; err != nil || requestLogCount != 1 {
		t.Fatalf("request log count = %d err=%v", requestLogCount, err)
	}
	var aggregateRequests int64
	if err := db.Model(&GatewayRequestAggregate{}).Where("channel_id = ?", 41).Select("COALESCE(SUM(requests), 0)").Scan(&aggregateRequests).Error; err != nil || aggregateRequests != 1 {
		t.Fatalf("aggregate requests = %d err=%v", aggregateRequests, err)
	}
}

func TestRequestDispatchFinalizeAtomicallyPersistsQueuedFacts(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 411), now)
	if err != nil {
		t.Fatalf("begin dispatch: %v", err)
	}
	confirmPlainRequestDispatchForTest(t, store, audit.ID, now.Add(time.Millisecond))
	responseAt := now.Add(time.Second)
	acceptedAt := now.Add(2 * time.Second)
	finishedAt := now.Add(3 * time.Second)
	log := &GatewayRequestLog{
		StatusCode:       200,
		PromptTokens:     17,
		CompletionTokens: 23,
		TotalTokens:      40,
		UpstreamAccepted: true,
		CreatedAt:        finishedAt,
	}
	facts := GatewayRequestDispatchFinalizationFacts{
		ProviderRequestID:  "provider-from-retry-queue",
		ResponseObservedAt: responseAt,
		Accepted:           true,
		AcceptedEventType:  "response.created",
		AcceptedObjectID:   "response-object-from-memory",
		AcceptedAt:         acceptedAt,
	}
	created, err := store.FinalizeRequestDispatchWithFactsResult(audit.ID, log, "success", facts, finishedAt)
	if err != nil || !created {
		t.Fatalf("finalize queued facts: created=%v err=%v", created, err)
	}
	stored, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ProviderRequestID != facts.ProviderRequestID || stored.ResponseObservedAt == nil || !stored.ResponseObservedAt.Equal(responseAt) || stored.AcceptedAt == nil || !stored.AcceptedAt.Equal(acceptedAt) || stored.AcceptedEventType != facts.AcceptedEventType || stored.AcceptedObjectID != facts.AcceptedObjectID {
		t.Fatalf("queued lifecycle facts = %+v", stored)
	}
	if log.ID == 0 || log.UpstreamRequestID != facts.ProviderRequestID || !log.UpstreamAccepted || log.StatusCode != 200 || log.TotalTokens != 40 {
		t.Fatalf("exact queued final log = %+v", log)
	}
	var count int64
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("queued final logs = %d err=%v", count, err)
	}
}

func TestRequestDispatchLifecycleCASAndBillableCompletionGate(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()

	plain, err := store.BeginRequestDispatch(requestDispatchTestSnapshot(now, 42), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteRequestDispatch(plain.ID, "transport_before_response", now.Add(time.Second)); err != nil {
		t.Fatalf("complete plain dispatch: %v", err)
	}
	if err := store.CompleteRequestDispatch(plain.ID, "transport_before_response", now.Add(2*time.Second)); err != nil {
		t.Fatalf("repeat complete plain dispatch: %v", err)
	}
	if err := store.RecordRequestDispatchResponse(plain.ID, "late-provider-id", now.Add(2*time.Second)); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("late response error = %v", err)
	}
	if err := store.FinalizeRequestDispatch(plain.ID, &GatewayRequestLog{StatusCode: 502}, "late_finalize", now.Add(2*time.Second)); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("late finalize error = %v", err)
	}

	physical, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 421), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(physical.ID, now); err != nil {
		t.Fatalf("mark physical text dispatch: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(physical.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatalf("confirm physical text dispatch: %v", err)
	}
	if err := store.CompleteRequestDispatch(physical.ID, "transport", now.Add(time.Second)); !errors.Is(err, ErrGatewayRequestDispatchRequiresFinalization) {
		t.Fatalf("physical text completion error = %v", err)
	}
	storedPhysical, err := store.FindRequestDispatchAudit(physical.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !storedPhysical.PhysicallyDispatched || storedPhysical.Stage != GatewayRequestDispatchStageDispatched || storedPhysical.TerminalAt != nil || storedPhysical.RequestLogID != nil {
		t.Fatalf("physical text audit was altered by completion: %+v", storedPhysical)
	}

	committed, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now, 43), now)
	if err != nil {
		t.Fatal(err)
	}
	if committed.BillingCommitted || committed.PhysicallyDispatched {
		t.Fatalf("pre-send image dispatch was pre-billed: %+v", committed)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(committed.ID, now); err != nil {
		t.Fatalf("atomically mark committed image dispatch: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(committed.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatalf("confirm committed image dispatch: %v", err)
	}
	if err := store.CompleteRequestDispatch(committed.ID, "transport", now.Add(time.Second)); !errors.Is(err, ErrGatewayRequestDispatchRequiresFinalization) {
		t.Fatalf("committed completion error = %v", err)
	}
	if err := store.RecordRequestDispatchResponse(committed.ID, "image-provider-id", now.Add(time.Second)); err != nil {
		t.Fatalf("record committed response: %v", err)
	}
	if err := store.RecordRequestDispatchResponse(committed.ID, "different-provider-id", now.Add(time.Second)); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("provider id replacement error = %v", err)
	}

	accepted, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 44), now)
	if err != nil {
		t.Fatal(err)
	}
	confirmPlainRequestDispatchForTest(t, store, accepted.ID, now.Add(time.Millisecond))
	if err := store.MarkRequestDispatchAccepted(accepted.ID, "message_delta", "", now.Add(time.Second)); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("acceptance before response error = %v", err)
	}
	if err := store.RecordRequestDispatchResponse(accepted.ID, "", now.Add(time.Second)); err != nil {
		t.Fatalf("record response without provider id: %v", err)
	}
	if err := store.MarkRequestDispatchAccepted(accepted.ID, "message_delta", "", now.Add(2*time.Second)); err != nil {
		t.Fatalf("mark Claude fallback accepted: %v", err)
	}
	if err := store.MarkRequestDispatchAccepted(accepted.ID, "content_block_start", "", now.Add(2*time.Second)); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("replace acceptance fact error = %v", err)
	}
	if err := store.CompleteRequestDispatch(accepted.ID, "stream_error", now.Add(3*time.Second)); !errors.Is(err, ErrGatewayRequestDispatchRequiresFinalization) {
		t.Fatalf("accepted completion error = %v", err)
	}

	responseOnly, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 45), now)
	if err != nil {
		t.Fatal(err)
	}
	confirmPlainRequestDispatchForTest(t, store, responseOnly.ID, now.Add(time.Millisecond))
	if err := store.RecordRequestDispatchResponse(responseOnly.ID, "", now.Add(time.Second)); err != nil {
		t.Fatalf("record response-only boundary: %v", err)
	}
	if err := store.CompleteRequestDispatch(responseOnly.ID, "response_without_provider_id", now.Add(2*time.Second)); !errors.Is(err, ErrGatewayRequestDispatchRequiresFinalization) {
		t.Fatalf("response-only physical completion error = %v", err)
	}

	providerOnly, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 46), now)
	if err != nil {
		t.Fatal(err)
	}
	confirmPlainRequestDispatchForTest(t, store, providerOnly.ID, now.Add(time.Millisecond))
	if err := store.RecordRequestDispatchResponse(providerOnly.ID, "provider-cost-evidence", now.Add(time.Second)); err != nil {
		t.Fatalf("record provider-only evidence: %v", err)
	}
	if err := store.CompleteRequestDispatch(providerOnly.ID, "response_with_provider_id", now.Add(2*time.Second)); !errors.Is(err, ErrGatewayRequestDispatchRequiresFinalization) {
		t.Fatalf("provider-only completion error = %v", err)
	}
}

func TestClosePreviousRequestDispatchAfterNextPhysicalRequiresPhysicalSuccessor(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	previous, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 431), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(previous.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatalf("mark prior physical dispatch: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(previous.ID, now.Add(2*time.Millisecond)); err != nil {
		t.Fatalf("confirm prior physical dispatch: %v", err)
	}
	nextSnapshot := requestDispatchCurrentTelemetryTestSnapshot(t, now, 432)
	nextSnapshot.PriorPhysicallyDispatchedFailureCount = 1
	next, err := store.BeginNextRequestDispatch(previous.ID, nextSnapshot, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	finishedAt := now.Add(2 * time.Second)
	if _, err := store.ClosePreviousRequestDispatchAfterNextPhysical(
		previous.ID,
		next.ID,
		nil,
		"failed_before_acceptance",
		GatewayRequestDispatchFinalizationFacts{},
		finishedAt,
	); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("close before successor physical marker error = %v", err)
	}
	storedPrevious, err := store.FindRequestDispatchAudit(previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedPrevious.Stage != GatewayRequestDispatchStageDispatched || storedPrevious.TerminalAt != nil || storedPrevious.RequestLogID != nil {
		t.Fatalf("prior dispatch closed before successor was physical: %+v", storedPrevious)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(next.ID, now.Add(3*time.Second)); err != nil {
		t.Fatalf("mark successor physical dispatch: %v", err)
	}
	if _, err := store.ClosePreviousRequestDispatchAfterNextPhysical(
		previous.ID,
		next.ID,
		nil,
		"failed_before_acceptance",
		GatewayRequestDispatchFinalizationFacts{},
		finishedAt,
	); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("close before successor transport confirmation error = %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(next.ID, now.Add(4*time.Second)); err != nil {
		t.Fatalf("confirm successor physical dispatch: %v", err)
	}
	closed, err := store.ClosePreviousRequestDispatchAfterNextPhysical(
		previous.ID,
		next.ID,
		nil,
		"failed_before_acceptance",
		GatewayRequestDispatchFinalizationFacts{},
		finishedAt,
	)
	if err != nil || closed {
		t.Fatalf("close after successor transport confirmation: evidence_created=%t err=%v", closed, err)
	}
	storedPrevious, err = store.FindRequestDispatchAudit(previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedPrevious.Stage != GatewayRequestDispatchStageCompleted || storedPrevious.TerminalAt == nil || storedPrevious.RequestLogID != nil {
		t.Fatalf("prior dispatch was not normally closed after successor physical marker: %+v", storedPrevious)
	}
}

func TestHandoffUnknownPhysicalDispatchMarkRecoveryClearsOnlyPlainPreDoMarker(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 433), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now); err != nil {
		t.Fatalf("mark physical text dispatch: %v", err)
	}
	if err := store.HandoffUnknownPhysicalDispatchMarkRecovery(audit.ID, GatewayRequestDispatchTerminalOwnerLeaseLost, now.Add(time.Second)); err != nil {
		t.Fatalf("handoff unknown plain physical marker: %v", err)
	}
	stored, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.PhysicallyDispatched || stored.BillingCommitted || stored.RecoveryRequestedAt == nil || stored.RecoveryTerminalKind != GatewayRequestDispatchTerminalOwnerLeaseLost || stored.TerminalAt != nil {
		t.Fatalf("plain pre-Do marker handoff = %+v", stored)
	}

	committed, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now, 434), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(committed.ID, now); err != nil {
		t.Fatalf("mark paired image dispatch: %v", err)
	}
	if err := store.HandoffUnknownPhysicalDispatchMarkRecovery(committed.ID, GatewayRequestDispatchTerminalOwnerLeaseLost, now.Add(time.Second)); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("plain marker handoff accepted paired billing audit: %v", err)
	}
}

func TestRequestDispatchImagePhysicalBillingCommitIsAtomicAndOrdinaryUnsentHandoffPreservesPhysicalMarker(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()

	image, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now, 471), now)
	if err != nil {
		t.Fatalf("begin image dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(image.ID, now); !errors.Is(err, ErrGatewayRequestDispatchBillingCommitRequired) {
		t.Fatalf("ordinary physical mark for image error = %v", err)
	}
	persisted, err := store.FindRequestDispatchAudit(image.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.PhysicallyDispatched || persisted.BillingCommitted {
		t.Fatalf("failed ordinary image physical mark changed evidence: %+v", persisted)
	}
	other := NewGateway(db)
	if err := other.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(image.ID, now); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("wrong owner image mark error = %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(image.ID, now); err != nil {
		t.Fatalf("atomic image physical/billing mark: %v", err)
	}
	persisted, err = store.FindRequestDispatchAudit(image.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !persisted.PhysicallyDispatched || !persisted.BillingCommitted || persisted.PhysicalBillingInvariantVersion != GatewayRequestDispatchPhysicalBillingInvariantVersion {
		t.Fatalf("atomic image physical/billing state = %+v", persisted)
	}
	terminalHandoffPersisted, err := store.PersistRequestDispatchTerminalHandoff(
		image.ID,
		image.OwnerInstanceID,
		image.OwnerFenceToken,
		GatewayRequestDispatchTerminalOwnerLeaseLost,
		now.Add(time.Second),
		now,
		now.Add(time.Second),
		nil,
		GatewayRequestDispatchFinalizationFacts{},
		image.TelemetryVersion,
		false,
	)
	if terminalHandoffPersisted || !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("generic nonphysical terminal handoff accepted armed image dispatch: persisted=%t err=%v", terminalHandoffPersisted, err)
	}
	if err := store.HandoffUnsentRequestDispatchRecovery(image.ID, GatewayRequestDispatchTerminalOwnerLeaseLost, now.Add(time.Second)); !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("ordinary unsent handoff erased physical image dispatch: %v", err)
	}
	persisted, err = store.FindRequestDispatchAudit(image.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !persisted.PhysicallyDispatched || !persisted.BillingCommitted {
		t.Fatalf("ordinary unsent handoff changed physical image evidence: %+v", persisted)
	}
	unsent, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now, 472), now)
	if err != nil {
		t.Fatalf("begin unsent image dispatch: %v", err)
	}
	if err := store.HandoffUnsentRequestDispatchRecovery(unsent.ID, GatewayRequestDispatchTerminalOwnerLeaseLost, now.Add(time.Second)); err != nil {
		t.Fatalf("handoff unsent dispatch: %v", err)
	}
	unsentPersisted, err := store.FindRequestDispatchAudit(unsent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unsentPersisted.PhysicallyDispatched || unsentPersisted.BillingCommitted || unsentPersisted.RecoveryRequestedAt == nil || unsentPersisted.RecoveryTerminalKind != GatewayRequestDispatchTerminalOwnerLeaseLost {
		t.Fatalf("unsent handoff did not force nonphysical/nonbillable state: %+v", unsentPersisted)
	}

	var defaultVersion int
	if err := db.Raw(`
		INSERT INTO gateway_request_dispatch_audits (
			channel_id, method, path, request_started_at, dispatched_at, legacy_ownerless
		) VALUES (
			9991, 'POST', '/v1/responses', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE
		)
		RETURNING physical_billing_invariant_version
	`).Scan(&defaultVersion).Error; err != nil {
		t.Fatalf("raw post-migration dispatch insert with default invariant version: %v", err)
	}
	if defaultVersion != GatewayRequestDispatchPhysicalBillingInvariantVersion {
		t.Fatalf("raw post-migration dispatch default invariant version = %d, want %d", defaultVersion, GatewayRequestDispatchPhysicalBillingInvariantVersion)
	}
	err = db.Exec(`
		INSERT INTO gateway_request_dispatch_audits (
			channel_id, method, path, request_started_at, dispatched_at,
			legacy_ownerless, physical_billing_invariant_version
		) VALUES (
			9992, 'POST', '/v1/responses', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
			TRUE, 0
		)
	`).Error
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "23514" || !strings.Contains(pgErr.Message, "new gateway request dispatch must use physical billing invariant version 1") {
		t.Fatalf("explicit version-0 post-migration insert error = %v, want invariant trigger violation", err)
	}
}

func TestRequestDispatchUnknownPhysicalBillingCommitIsForcedBackToUnsent(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	audit, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now, 473), now)
	if err != nil {
		t.Fatalf("begin image dispatch: %v", err)
	}

	faultDB := db.Session(&gorm.Session{NewDB: true})
	pool := &requestDispatchCommitUnknownPool{ConnPool: faultDB.Statement.ConnPool}
	faultDB.Statement.ConnPool = pool
	store.db = faultDB
	err = store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(audit.ID, now.Add(time.Second))
	store.db = db
	if !pool.injected.Load() || !errors.Is(err, ErrGatewayRequestDispatchCommitUnknown) {
		t.Fatalf("unknown paired physical/billing commit injected=%t err=%v", pool.injected.Load(), err)
	}
	if err := store.HandoffUnknownPairedPhysicalBillingCommitRecovery(audit.ID, GatewayRequestDispatchTerminalOwnerLeaseLost, now.Add(2*time.Second)); err != nil {
		t.Fatalf("handoff unknown paired physical/billing commit: %v", err)
	}
	persisted, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.PhysicallyDispatched || persisted.BillingCommitted || persisted.RecoveryRequestedAt == nil || persisted.RecoveryTerminalKind != GatewayRequestDispatchTerminalOwnerLeaseLost {
		t.Fatalf("unknown paired physical/billing commit was not forced unsent: %+v", persisted)
	}
}

func TestRequestDispatchLiveWritesRequireTheExactLocalOwner(t *testing.T) {
	db := openTestDB(t)
	owner := NewGateway(db)
	other := NewGateway(db)
	now := time.Now().UTC()
	audit, err := owner.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 47), now)
	if err != nil {
		t.Fatalf("begin dispatch: %v", err)
	}
	confirmPlainRequestDispatchForTest(t, owner, audit.ID, now.Add(time.Millisecond))

	if err := other.RecordRequestDispatchResponse(audit.ID, "wrong-owner-provider-id", now.Add(time.Second)); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("wrong owner response write error = %v", err)
	}
	if err := other.HandoffRequestDispatchRecovery(audit.ID, "wrong_owner_handoff", now.Add(time.Second)); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("wrong owner handoff error = %v", err)
	}
	if err := owner.RecordRequestDispatchResponse(audit.ID, "right-owner-provider-id", now.Add(2*time.Second)); err != nil {
		t.Fatalf("exact owner response write: %v", err)
	}
	stored, err := owner.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ProviderRequestID != "right-owner-provider-id" || stored.RecoveryRequestedAt != nil {
		t.Fatalf("wrong owner changed dispatch: %+v", stored)
	}
}

func TestRequestDispatchCrashWindowsRecoveryAndRetention(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()

	dispatchOnly, err := store.BeginRequestDispatch(requestDispatchTestSnapshot(now.Add(-12*time.Minute), 51), now.Add(-12*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	responseOnly, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now.Add(-11*time.Minute), 52), now.Add(-11*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	responseOnlyAt := now.Add(-11 * time.Minute)
	confirmPlainRequestDispatchForTest(t, store, responseOnly.ID, responseOnlyAt)
	if err := store.RecordRequestDispatchResponse(responseOnly.ID, "", responseOnlyAt.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	providerOnly, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now.Add(-10*time.Minute), 53), now.Add(-10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	providerOnlyAt := now.Add(-10 * time.Minute)
	confirmPlainRequestDispatchForTest(t, store, providerOnly.ID, providerOnlyAt)
	if err := store.RecordRequestDispatchResponse(providerOnly.ID, "provider-only-cost-evidence", providerOnlyAt.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	committed, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now.Add(-9*time.Minute), 54), now.Add(-9*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	confirmImageRequestDispatchForTest(t, store, committed.ID, now.Add(-9*time.Minute))
	accepted, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now.Add(-8*time.Minute), 55), now.Add(-8*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	acceptedAt := now.Add(-8 * time.Minute)
	confirmPlainRequestDispatchForTest(t, store, accepted.ID, acceptedAt)
	if err := store.RecordRequestDispatchResponse(accepted.ID, "provider-accepted", acceptedAt.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchAccepted(accepted.ID, "message_delta", "", acceptedAt.Add(3*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	finalized, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now.Add(-7*time.Minute), 56), now.Add(-7*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	finalizedAt := now.Add(-7 * time.Minute)
	confirmPlainRequestDispatchForTest(t, store, finalized.ID, finalizedAt)
	if err := store.RecordRequestDispatchResponse(finalized.ID, "provider-success", finalizedAt.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	finalLog := &GatewayRequestLog{StatusCode: 200, CreatedAt: finalizedAt.Add(3 * time.Millisecond)}
	if err := store.FinalizeRequestDispatch(finalized.ID, finalLog, "success", finalizedAt.Add(3*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	recoverable, err := store.ListRecoverableRequestDispatches(now.Add(time.Minute), now, 10)
	if err != nil {
		t.Fatalf("list active-owner recoverable: %v", err)
	}
	if len(recoverable) != 0 {
		t.Fatalf("active owner exposed recoverable dispatches = %+v", recoverable)
	}
	completed, err := store.CompleteStaleRequestDispatches(now.Add(time.Minute), now, 10)
	if err != nil || completed != 0 {
		t.Fatalf("active owner stale completions = %d err=%v", completed, err)
	}

	expireRequestDispatchOwnerForTest(t, db, store)
	if err := store.RenewRequestDispatchOwner(nil, now, now.Add(GatewayRequestDispatchOwnerLeaseDuration)); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("expired owner renewal error = %v", err)
	}
	if err := store.RecordRequestDispatchResponse(providerOnly.ID, "provider-only-cost-evidence", now); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("expired owner response write error = %v", err)
	}
	if _, err := store.FinalizeRequestDispatchResult(accepted.ID, &GatewayRequestLog{StatusCode: 502}, "stale_owner_finalize", now); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("expired owner finalize error = %v", err)
	}

	recoveryStore := NewGateway(db)
	recoverable, err = recoveryStore.ListRecoverableRequestDispatches(now.Add(-time.Hour), now, 10)
	if err != nil {
		t.Fatalf("list expired-owner recoverable: %v", err)
	}
	if len(recoverable) != 4 || recoverable[0].ID != responseOnly.ID || recoverable[1].ID != providerOnly.ID || recoverable[2].ID != committed.ID || recoverable[3].ID != accepted.ID {
		t.Fatalf("expired-owner recoverable dispatches = %+v", recoverable)
	}
	completed, err = recoveryStore.CompleteStaleRequestDispatches(now.Add(-time.Hour), now, 10)
	if err != nil || completed != 1 {
		t.Fatalf("complete expired-owner stale dispatches = %d err=%v", completed, err)
	}
	for _, completedID := range []uint64{dispatchOnly.ID} {
		stale, err := recoveryStore.FindRequestDispatchAudit(completedID)
		if err != nil {
			t.Fatal(err)
		}
		if stale.Stage != GatewayRequestDispatchStageCompleted || stale.TerminalKind != GatewayRequestDispatchTerminalOwnerLeaseLost {
			t.Fatalf("stale dispatch %d = %+v", completedID, stale)
		}
	}
	providerEvidence, err := recoveryStore.FindRequestDispatchAudit(providerOnly.ID)
	if err != nil {
		t.Fatal(err)
	}
	if providerEvidence.Stage != GatewayRequestDispatchStageResponded || providerEvidence.TerminalAt != nil || providerEvidence.ProviderRequestID == "" {
		t.Fatalf("provider-only evidence was changed: %+v", providerEvidence)
	}

	pruned, err := recoveryStore.PruneRequestDispatchAudits(now.Add(time.Minute), now.Add(time.Minute), 10)
	if err != nil || pruned != 2 {
		t.Fatalf("prune dispatch audits = %d err=%v", pruned, err)
	}
	for _, retainedID := range []uint64{responseOnly.ID, providerOnly.ID, committed.ID, accepted.ID} {
		if _, err := recoveryStore.FindRequestDispatchAudit(retainedID); err != nil {
			t.Fatalf("cost-evidence orphan %d was pruned: %v", retainedID, err)
		}
	}
	for _, deletedID := range []uint64{dispatchOnly.ID, finalized.ID} {
		if _, err := recoveryStore.FindRequestDispatchAudit(deletedID); !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Fatalf("retained prunable audit %d: %v", deletedID, err)
		}
	}
	var finalLogCount int64
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", finalLog.ID).Count(&finalLogCount).Error; err != nil || finalLogCount != 1 {
		t.Fatalf("pruning audit changed linked request log: count=%d err=%v", finalLogCount, err)
	}
}

func TestRequestDispatchFinalizeRollbackLeavesAcceptedOrphanRecoverable(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	usageID := "duplicate-upstream-usage"
	existing := &GatewayRequestLog{
		ChannelID:       61,
		Method:          "POST",
		Path:            "/v1/responses",
		StatusCode:      200,
		UpstreamUsageID: &usageID,
		CreatedAt:       now,
	}
	if err := store.AppendRequestLog(existing); err != nil {
		t.Fatalf("append existing request log: %v", err)
	}

	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 61), now)
	if err != nil {
		t.Fatal(err)
	}
	confirmPlainRequestDispatchForTest(t, store, audit.ID, now.Add(time.Millisecond))
	if err := store.RecordRequestDispatchResponse(audit.ID, "provider-rollback", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchAccepted(audit.ID, "response.created", "resp-rollback", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	failedLog := &GatewayRequestLog{
		StatusCode:      502,
		Error:           "stream failed",
		UpstreamUsageID: &usageID,
		CreatedAt:       now.Add(2 * time.Second),
	}
	if err := store.FinalizeRequestDispatch(audit.ID, failedLog, "stream_error", now.Add(2*time.Second)); err == nil {
		t.Fatal("finalize unexpectedly accepted duplicate upstream usage")
	}
	if failedLog.ID != 0 {
		t.Fatalf("rolled-back request log retained id %d", failedLog.ID)
	}
	stored, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Stage != GatewayRequestDispatchStageAccepted || stored.TerminalAt != nil || stored.RequestLogID != nil {
		t.Fatalf("rollback changed accepted audit: %+v", stored)
	}
	recoverable, err := store.ListRecoverableRequestDispatches(now.Add(time.Minute), now.Add(time.Minute), 10)
	if err != nil || len(recoverable) != 0 {
		t.Fatalf("active owner rollback recoverable = %+v err=%v", recoverable, err)
	}
	expireRequestDispatchOwnerForTest(t, db, store)
	recoverable, err = store.ListRecoverableRequestDispatches(now.Add(time.Minute), now.Add(time.Minute), 10)
	if err != nil || len(recoverable) != 1 || recoverable[0].ID != audit.ID {
		t.Fatalf("recoverable after rollback = %+v err=%v", recoverable, err)
	}
	var logCount int64
	if err := db.Model(&GatewayRequestLog{}).Where("channel_id = ?", 61).Count(&logCount).Error; err != nil || logCount != 1 {
		t.Fatalf("request logs after rollback = %d err=%v", logCount, err)
	}

	recoveryStore := NewGateway(db)
	recoveredLog := &GatewayRequestLog{StatusCode: 502, Error: "recovered after rollback", CreatedAt: now.Add(time.Minute)}
	created, err := recoveryStore.FinalizeRecoveredRequestDispatchResult(audit.ID, recoveredLog, "orphaned_after_service_interruption", now.Add(time.Minute), now.Add(time.Minute))
	if err != nil || !created || recoveredLog.ID == 0 {
		t.Fatalf("finalize recovered rollback orphan: created=%v log=%+v err=%v", created, recoveredLog, err)
	}
	retryLog := &GatewayRequestLog{}
	created, err = recoveryStore.FinalizeRecoveredRequestDispatchResult(audit.ID, retryLog, "orphaned_after_service_interruption", now.Add(time.Minute), now.Add(2*time.Minute))
	if err != nil || created || retryLog.ID != recoveredLog.ID {
		t.Fatalf("idempotent recovered finalize: created=%v log=%+v err=%v", created, retryLog, err)
	}
	if _, err := store.FinalizeRequestDispatchResult(audit.ID, &GatewayRequestLog{}, "stale_original_owner", now.Add(2*time.Minute)); !errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
		t.Fatalf("stale original owner idempotent finalize error = %v", err)
	}
}

func TestRequestDispatchLegacyOwnerlessGraceAppliesOnlyToMarkedRows(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 62), now)
	if err != nil {
		t.Fatal(err)
	}
	confirmPlainRequestDispatchForTest(t, store, audit.ID, now.Add(time.Millisecond))
	if audit.LegacyOwnerless || audit.OwnerInstanceID == "" || audit.OwnerFenceToken == 0 {
		t.Fatalf("new dispatch owner metadata = %+v", audit)
	}
	if err := store.RecordRequestDispatchResponse(audit.ID, "legacy-provider-id", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchAccepted(audit.ID, "response.created", "legacy-response", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	result := db.Exec(`UPDATE gateway_request_dispatch_audits
		SET legacy_ownerless = TRUE, owner_instance_id = NULL, owner_fence_token = NULL
		WHERE id = ?`, audit.ID)
	if result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("mark simulated pre-migration dispatch ownerless: rows=%d err=%v", result.RowsAffected, result.Error)
	}

	recoverable, err := store.ListRecoverableRequestDispatches(now.Add(-time.Second), now, 10)
	if err != nil || len(recoverable) != 0 {
		t.Fatalf("young legacy ownerless recovery = %+v err=%v", recoverable, err)
	}
	recoverable, err = store.ListRecoverableRequestDispatches(now.Add(time.Second), now, 10)
	if err != nil || len(recoverable) != 1 || recoverable[0].ID != audit.ID {
		t.Fatalf("eligible legacy ownerless recovery = %+v err=%v", recoverable, err)
	}

	invalid := db.Exec(`UPDATE gateway_request_dispatch_audits
		SET legacy_ownerless = FALSE
		WHERE id = ?`, audit.ID)
	if invalid.Error == nil {
		t.Fatal("ownerless non-legacy dispatch unexpectedly passed the database constraint")
	}
}

func TestRequestDispatchOwnerRetentionDeletesOnlyAfterLastAudit(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	audit, err := store.BeginRequestDispatch(requestDispatchTestSnapshot(now, 63), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteRequestDispatch(audit.ID, "transport_before_response", now); err != nil {
		t.Fatal(err)
	}
	instanceID, fenceToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StopRequestDispatchOwner(nil, now); err != nil {
		t.Fatal(err)
	}

	pruned, err := store.PruneRequestDispatchAudits(now.Add(time.Hour), now.Add(time.Hour), 10)
	if err != nil || pruned != 1 {
		t.Fatalf("prune final owner audit = %d err=%v", pruned, err)
	}
	var ownerCount int64
	if err := db.Model(&GatewayRequestDispatchOwner{}).Where("instance_id = ? AND fence_token = ?", instanceID, fenceToken).Count(&ownerCount).Error; err != nil {
		t.Fatalf("count retained owner: %v", err)
	}
	if ownerCount != 0 {
		t.Fatalf("unreferenced stopped owners = %d, want 0", ownerCount)
	}
}

func TestHandoffLinkedRequestDispatchRecoveryUpdatesBothAuditsAtomically(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	previous, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 671), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(previous.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(previous.ID, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	nextSnapshot := requestDispatchCurrentTelemetryTestSnapshot(t, now, 672)
	nextSnapshot.PriorPhysicallyDispatchedFailureCount = 1
	next, err := store.BeginNextRequestDispatch(previous.ID, nextSnapshot, now.Add(3*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(next.ID, now.Add(4*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(next.ID, now.Add(5*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	providerID := "linked-previous-provider"
	facts := GatewayRequestDispatchFinalizationFacts{ProviderRequestID: providerID, ResponseObservedAt: now.Add(6 * time.Millisecond)}
	if err := store.HandoffLinkedRequestDispatchRecovery(previous.ID, next.ID, GatewayRequestDispatchTerminalOwnerLeaseLost, facts, false, now.Add(7*time.Millisecond)); err != nil {
		t.Fatalf("handoff linked dispatches: %v", err)
	}
	for _, id := range []uint64{previous.ID, next.ID} {
		stored, err := store.FindRequestDispatchAudit(id)
		if err != nil {
			t.Fatal(err)
		}
		if stored.RecoveryRequestedAt == nil || stored.RecoveryTerminalKind != GatewayRequestDispatchTerminalOwnerLeaseLost {
			t.Fatalf("linked dispatch %d was not surrendered: %+v", id, stored)
		}
	}
	storedPrevious, _ := store.FindRequestDispatchAudit(previous.ID)
	if storedPrevious.ProviderRequestID != providerID {
		t.Fatalf("previous provider outcome = %q, want %q", storedPrevious.ProviderRequestID, providerID)
	}
}

func TestHandoffLinkedRequestDispatchRecoveryRollsBackFirstSideOnInvalidLink(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	previous, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 673), now)
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 674), now)
	if err != nil {
		t.Fatal(err)
	}
	err = store.HandoffLinkedRequestDispatchRecovery(previous.ID, unrelated.ID, GatewayRequestDispatchTerminalOwnerLeaseLost, GatewayRequestDispatchFinalizationFacts{}, false, now)
	if !errors.Is(err, ErrGatewayRequestDispatchConflict) {
		t.Fatalf("invalid linked handoff error = %v", err)
	}
	stored, err := store.FindRequestDispatchAudit(previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RecoveryRequestedAt != nil {
		t.Fatalf("invalid linked handoff partially updated predecessor: %+v", stored)
	}
}

func TestPersistLinkedRequestDispatchTerminalHandoffsIsAtomic(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	previous, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 675), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(previous.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(previous.ID, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	nextSnapshot := requestDispatchCurrentTelemetryTestSnapshot(t, now, 676)
	nextSnapshot.PriorPhysicallyDispatchedFailureCount = 1
	next, err := store.BeginNextRequestDispatch(previous.ID, nextSnapshot, now.Add(3*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(next.ID, now.Add(4*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(next.ID, now.Add(5*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	const callbackName = "request_dispatch_handoff:fail_second_linked_insert"
	injectedErr := errors.New("inject second linked handoff insert failure")
	var inserts atomic.Int32
	if err := db.Callback().Create().Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
		if tx == nil || tx.Error != nil || tx.Statement == nil || tx.Statement.Table != "gateway_request_dispatch_terminal_handoffs" {
			return
		}
		if inserts.Add(1) == 2 {
			tx.AddError(injectedErr)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Create().Remove(callbackName) })
	terminalLog := func(status int) *GatewayRequestLog {
		return &GatewayRequestLog{StatusCode: status, CreatedAt: now.Add(5 * time.Millisecond)}
	}
	err = store.PersistLinkedRequestDispatchTerminalHandoffs(
		GatewayRequestDispatchTerminalHandoffInput{
			DispatchID: previous.ID, OwnerInstanceID: previous.OwnerInstanceID, OwnerFenceToken: previous.OwnerFenceToken,
			TerminalKind: "audit_failure", RequestedAt: now.Add(6 * time.Millisecond),
			StartedAt: now, FinishedAt: now.Add(6 * time.Millisecond), Log: terminalLog(502),
			TelemetryVersion: previous.TelemetryVersion, PhysicallyDispatched: true,
		},
		GatewayRequestDispatchTerminalHandoffInput{
			DispatchID: next.ID, OwnerInstanceID: next.OwnerInstanceID, OwnerFenceToken: next.OwnerFenceToken,
			TerminalKind: "audit_failure", RequestedAt: now.Add(6 * time.Millisecond),
			StartedAt: now, FinishedAt: now.Add(6 * time.Millisecond), Log: terminalLog(503),
			Facts:            GatewayRequestDispatchFinalizationFacts{ResponseObservedAt: now.Add(6 * time.Millisecond)},
			TelemetryVersion: next.TelemetryVersion, PhysicallyDispatched: true,
		},
		false,
	)
	if !errors.Is(err, injectedErr) {
		t.Fatalf("linked handoff error = %v, want injected failure", err)
	}
	var handoffCount int64
	if err := db.Model(&GatewayRequestDispatchTerminalHandoff{}).Where("dispatch_id IN ?", []uint64{previous.ID, next.ID}).Count(&handoffCount).Error; err != nil {
		t.Fatal(err)
	}
	if handoffCount != 0 {
		t.Fatalf("partial linked terminal snapshots = %d, want 0", handoffCount)
	}
	for _, id := range []uint64{previous.ID, next.ID} {
		stored, findErr := store.FindRequestDispatchAudit(id)
		if findErr != nil {
			t.Fatal(findErr)
		}
		if stored.RecoveryRequestedAt != nil || stored.ResponseObservedAt != nil {
			t.Fatalf("linked handoff failure partially fenced dispatch %d: %+v", id, stored)
		}
	}
}
