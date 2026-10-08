package gateway

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

var postgresDispatchRecoveryTraceSequence uint64

func postgresDispatchRecoveryNumber(value float64) *float64 {
	return &value
}

func postgresDispatchRecoveryNumbersEqual(left, right *float64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func postgresDispatchRecoverySnapshot(startedAt time.Time, channelID uint, path string) storage.GatewayRequestDispatchSnapshot {
	return storage.GatewayRequestDispatchSnapshot{
		DownstreamKeyID:                701,
		ChannelID:                      channelID,
		ChannelNameSnapshot:            "recovery provider",
		GroupName:                      "recovery-group",
		UpstreamKeyID:                  8801,
		UpstreamKeyName:                "recovery-key",
		ModelName:                      "gpt-recovery",
		UpstreamModelName:              "gpt-recovery-upstream",
		ReasoningEffort:                "high",
		RequestedModelName:             "gpt-recovery-requested",
		RequestedReasoningEffort:       "medium",
		ModelMappingStrategy:           "balanced",
		ModelMappingDetails:            "recovery mapping snapshot",
		Method:                         "POST",
		Path:                           path,
		ClientIP:                       "203.0.113.71",
		ClientIPSource:                 "trusted_proxy",
		SessionHash:                    strings.Repeat("b", 64),
		RouteReason:                    "smart_primary",
		RouteSelectionRank:             2,
		SessionAffinityOriginRequestID: 902,
		RouteDetails:                   "recovery route snapshot",
		RouteAttemptedChannels:         `[{"channel_id":700,"reason":"transport"}]`,
		GroupRatio:                     0.75,
		CompletionRatio:                1.5,
		RawGroupRatio:                  postgresDispatchRecoveryNumber(0.75),
		RawCompletionRatio:             postgresDispatchRecoveryNumber(1.5),
		FundingFactor:                  postgresDispatchRecoveryNumber(1.25),
		CostBasisVersion:               4,
		CostBasisSource:                "channel_balance",
		RequestStartedAt:               startedAt,
	}
}

func postgresDispatchRecoveryCurrentTelemetrySnapshot(t *testing.T, startedAt time.Time, channelID uint, path string) storage.GatewayRequestDispatchSnapshot {
	t.Helper()
	routeSnapshot, err := storage.NewGatewayRouteExclusionSnapshot(startedAt, []storage.GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("create route exclusion snapshot: %v", err)
	}
	snapshot := postgresDispatchRecoverySnapshot(startedAt, channelID, path)
	snapshot.LogicalRequestTraceID = fmt.Sprintf("%032x", atomic.AddUint64(&postgresDispatchRecoveryTraceSequence, 1))
	snapshot.RouteAttemptedChannels = ""
	snapshot.TelemetryVersion = storage.GatewayRequestTelemetryVersion
	snapshot.RoutingMode = storage.GatewayRoutingModeSmart
	snapshot.GatewayApp = string(storage.GatewayAppCodex)
	snapshot.RouteExclusionSnapshotVersion = storage.GatewayRouteExclusionSnapshotVersion
	snapshot.RouteExclusionsSnapshot = routeSnapshot
	return snapshot
}

func expirePostgresDispatchOwner(t *testing.T, db *gorm.DB, store *storage.Gateway) {
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

type postgresDispatchRecoveryBarrierStore struct {
	*storage.Gateway
	arrived chan<- struct{}
	release <-chan struct{}
	once    sync.Once
}

func (s *postgresDispatchRecoveryBarrierStore) ListRecoverableRequestDispatches(legacyBefore, at time.Time, limit int) ([]storage.GatewayRequestDispatchAudit, error) {
	audits, err := s.Gateway.ListRecoverableRequestDispatches(legacyBefore, at, limit)
	s.once.Do(func() {
		s.arrived <- struct{}{}
		<-s.release
	})
	return audits, err
}

func TestRecoverRequestDispatchOrphansPostgresFinalizesAcceptedAndCommittedOnce(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	secondDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatalf("open recovery PostgreSQL connection: %v", err)
	}
	recoveryStore := storage.NewGateway(secondDB)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	acceptedStartedAt := now.Add(-30 * time.Second)
	acceptedSnapshot := postgresDispatchRecoveryCurrentTelemetrySnapshot(t, acceptedStartedAt, 7101, "/v1/responses")
	accepted, err := store.BeginRequestDispatch(acceptedSnapshot, acceptedStartedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("begin accepted dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(accepted.ID, acceptedStartedAt.Add(1250*time.Millisecond)); err != nil {
		t.Fatalf("mark accepted dispatch physically dispatched: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(accepted.ID, acceptedStartedAt.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("confirm accepted dispatch transport invocation: %v", err)
	}
	if err := store.RecordRequestDispatchResponse(accepted.ID, "provider-request-accepted", acceptedStartedAt.Add(2*time.Second)); err != nil {
		t.Fatalf("record accepted response: %v", err)
	}
	if err := store.MarkRequestDispatchAccepted(accepted.ID, "response.created", "protocol-object-not-billing-id", acceptedStartedAt.Add(3*time.Second)); err != nil {
		t.Fatalf("mark accepted dispatch: %v", err)
	}

	committedStartedAt := now.Add(-20 * time.Second)
	committedSnapshot := postgresDispatchRecoveryCurrentTelemetrySnapshot(t, committedStartedAt, 7102, "/v1/images/generations")
	committed, err := store.BeginRequestDispatch(committedSnapshot, committedStartedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("begin billing-committed dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(committed.ID, committedStartedAt.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("mark billing-committed dispatch: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(committed.ID, committedStartedAt.Add(1750*time.Millisecond)); err != nil {
		t.Fatalf("confirm billing-committed transport invocation: %v", err)
	}
	if err := store.RecordRequestDispatchResponse(committed.ID, "provider-request-committed", committedStartedAt.Add(2*time.Second)); err != nil {
		t.Fatalf("record committed response: %v", err)
	}
	if err := store.RenewRequestDispatchOwner(nil, now, now.Add(storage.GatewayRequestDispatchOwnerLeaseDuration)); err != nil {
		t.Fatalf("renew active dispatch owner: %v", err)
	}

	service := NewService(nil, nil, recoveryStore, nil, nil, nil)
	if err := service.RecoverRequestDispatchOrphans(now); err != nil {
		t.Fatalf("run active-owner recovery pass: %v", err)
	}
	assertPostgresRecoveryCounts(t, db, []uint{acceptedSnapshot.ChannelID, committedSnapshot.ChannelID}, 0, 0)

	expirePostgresDispatchOwner(t, db, store)
	if err := service.RecoverRequestDispatchOrphans(now); err != nil {
		t.Fatalf("recover expired-owner accepted and billing-committed dispatches: %v", err)
	}

	assertPostgresRecoveredDispatch(t, db, accepted.ID, acceptedSnapshot, now, "provider-request-accepted", true, false)
	assertPostgresRecoveredDispatch(t, db, committed.ID, committedSnapshot, now, "provider-request-committed", false, true)
	assertPostgresRecoveryCounts(t, db, []uint{acceptedSnapshot.ChannelID, committedSnapshot.ChannelID}, 2, 2)

	if err := service.RecoverRequestDispatchOrphans(now.Add(time.Minute)); err != nil {
		t.Fatalf("repeat orphan recovery: %v", err)
	}
	assertPostgresRecoveryCounts(t, db, []uint{acceptedSnapshot.ChannelID, committedSnapshot.ChannelID}, 2, 2)
}

func TestRecoverRequestDispatchOrphansPostgresConcurrentInstancesFinalizeOnce(t *testing.T) {
	db := testdb.Open(t)
	secondDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatalf("open second PostgreSQL connection: %v", err)
	}
	thirdDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatalf("open third PostgreSQL connection: %v", err)
	}
	firstStore := storage.NewGateway(db)
	secondStore := storage.NewGateway(secondDB)
	thirdStore := storage.NewGateway(thirdDB)
	now := time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC)
	startedAt := now.Add(-15 * time.Second)
	snapshot := postgresDispatchRecoveryCurrentTelemetrySnapshot(t, startedAt, 7201, "/v1/responses")
	audit, err := firstStore.BeginRequestDispatch(snapshot, startedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("begin concurrent dispatch: %v", err)
	}
	if err := firstStore.MarkRequestDispatchPhysicallyDispatched(audit.ID, startedAt.Add(1250*time.Millisecond)); err != nil {
		t.Fatalf("mark concurrent dispatch physically dispatched: %v", err)
	}
	if err := firstStore.ConfirmRequestDispatchTransportInvocation(audit.ID, startedAt.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("confirm concurrent dispatch transport invocation: %v", err)
	}
	if err := firstStore.RecordRequestDispatchResponse(audit.ID, "provider-request-concurrent", startedAt.Add(2*time.Second)); err != nil {
		t.Fatalf("record concurrent response: %v", err)
	}
	if err := firstStore.MarkRequestDispatchAccepted(audit.ID, "response.created", "protocol-object-concurrent", startedAt.Add(3*time.Second)); err != nil {
		t.Fatalf("mark concurrent dispatch accepted: %v", err)
	}
	expirePostgresDispatchOwner(t, db, firstStore)

	secondService := NewService(nil, nil, secondStore, nil, nil, nil)
	thirdService := NewService(nil, nil, thirdStore, nil, nil, nil)
	start := make(chan struct{})
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan error, 2)
	workers := []struct {
		store     requestDispatchRecoveryStore
		reconcile func(*storage.GatewayRequestLog, time.Time, time.Time)
	}{
		{store: &postgresDispatchRecoveryBarrierStore{Gateway: secondStore, arrived: arrived, release: release}, reconcile: secondService.ReconcileRequestCost},
		{store: &postgresDispatchRecoveryBarrierStore{Gateway: thirdStore, arrived: arrived, release: release}, reconcile: thirdService.ReconcileRequestCost},
	}
	for _, worker := range workers {
		go func(worker struct {
			store     requestDispatchRecoveryStore
			reconcile func(*storage.GatewayRequestLog, time.Time, time.Time)
		}) {
			<-start
			results <- recoverRequestDispatchOrphans(worker.store, worker.reconcile, now)
		}(worker)
	}
	close(start)
	for index := 0; index < 2; index++ {
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatalf("concurrent recovery worker %d did not list the orphan", index+1)
		}
	}
	close(release)
	resultTimeout := time.NewTimer(5 * time.Second)
	defer resultTimeout.Stop()
	for index := 0; index < 2; index++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("concurrent orphan recovery %d: %v", index+1, err)
			}
		case <-resultTimeout.C:
			t.Fatalf("concurrent orphan recovery %d did not finish", index+1)
		}
	}

	assertPostgresRecoveredDispatch(t, db, audit.ID, snapshot, now, "provider-request-concurrent", true, false)
	assertPostgresRecoveryCounts(t, db, []uint{snapshot.ChannelID}, 1, 1)
}

func TestRecoverRequestDispatchOrphansPostgresCompletesDispatchOnlyWithoutLog(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	secondDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatalf("open recovery PostgreSQL connection: %v", err)
	}
	recoveryStore := storage.NewGateway(secondDB)
	now := time.Date(2026, 7, 23, 14, 0, 0, 0, time.UTC)
	startedAt := now.Add(-15 * time.Second)
	snapshot := postgresDispatchRecoverySnapshot(startedAt, 7301, "/v1/responses")
	audit, err := store.BeginRequestDispatch(snapshot, startedAt)
	if err != nil {
		t.Fatalf("begin dispatch-only attempt: %v", err)
	}

	service := NewService(nil, nil, recoveryStore, nil, nil, nil)
	if err := service.RecoverRequestDispatchOrphans(now); err != nil {
		t.Fatalf("run active-owner recovery pass: %v", err)
	}
	activeAudit, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatalf("read active dispatch-only audit: %v", err)
	}
	if activeAudit.Stage != storage.GatewayRequestDispatchStageDispatched || activeAudit.TerminalAt != nil {
		t.Fatalf("active owner dispatch-only state = %+v", activeAudit)
	}

	expirePostgresDispatchOwner(t, db, store)
	if err := service.RecoverRequestDispatchOrphans(now); err != nil {
		t.Fatalf("complete dispatch-only orphan: %v", err)
	}

	storedAudit, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatalf("read completed dispatch-only audit: %v", err)
	}
	if storedAudit.Stage != storage.GatewayRequestDispatchStageCompleted || storedAudit.TerminalKind != storage.GatewayRequestDispatchTerminalOwnerLeaseLost || storedAudit.TerminalAt == nil || !storedAudit.TerminalAt.Equal(now) {
		t.Fatalf("dispatch-only terminal state = %+v", storedAudit)
	}
	if storedAudit.RequestLogID != nil {
		t.Fatalf("dispatch-only audit unexpectedly linked request log %d", *storedAudit.RequestLogID)
	}
	assertPostgresRecoveryCounts(t, db, []uint{snapshot.ChannelID}, 0, 0)
}

func TestRecoverRequestDispatchOrphansPostgresPreservesProviderOnlyEvidence(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	secondDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatalf("open recovery PostgreSQL connection: %v", err)
	}
	recoveryStore := storage.NewGateway(secondDB)
	now := time.Date(2026, 7, 23, 15, 0, 0, 0, time.UTC)

	responseOnlyStartedAt := now.Add(-20 * time.Second)
	responseOnly, err := store.BeginRequestDispatch(postgresDispatchRecoveryCurrentTelemetrySnapshot(t, responseOnlyStartedAt, 7401, "/v1/responses"), responseOnlyStartedAt)
	if err != nil {
		t.Fatalf("begin response-only dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(responseOnly.ID, responseOnlyStartedAt.Add(250*time.Millisecond)); err != nil {
		t.Fatalf("mark response-only dispatch physically dispatched: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(responseOnly.ID, responseOnlyStartedAt.Add(500*time.Millisecond)); err != nil {
		t.Fatalf("confirm response-only transport invocation: %v", err)
	}
	if err := store.RecordRequestDispatchResponse(responseOnly.ID, "", responseOnlyStartedAt.Add(time.Second)); err != nil {
		t.Fatalf("record response-only boundary: %v", err)
	}

	providerOnlyStartedAt := now.Add(-10 * time.Second)
	providerOnly, err := store.BeginRequestDispatch(postgresDispatchRecoveryCurrentTelemetrySnapshot(t, providerOnlyStartedAt, 7402, "/v1/responses"), providerOnlyStartedAt)
	if err != nil {
		t.Fatalf("begin provider-only dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(providerOnly.ID, providerOnlyStartedAt.Add(250*time.Millisecond)); err != nil {
		t.Fatalf("mark provider-only dispatch physically dispatched: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(providerOnly.ID, providerOnlyStartedAt.Add(500*time.Millisecond)); err != nil {
		t.Fatalf("confirm provider-only transport invocation: %v", err)
	}
	if err := store.RecordRequestDispatchResponse(providerOnly.ID, "provider-only-cost-evidence", providerOnlyStartedAt.Add(time.Second)); err != nil {
		t.Fatalf("record provider-only evidence: %v", err)
	}
	if err := store.CompleteRequestDispatch(providerOnly.ID, "must_not_complete", now); !errors.Is(err, storage.ErrGatewayRequestDispatchRequiresFinalization) {
		t.Fatalf("complete provider-only evidence error = %v", err)
	}

	service := NewService(nil, nil, recoveryStore, nil, nil, nil)
	if err := service.RecoverRequestDispatchOrphans(now); err != nil {
		t.Fatalf("run active-owner evidence recovery pass: %v", err)
	}
	for _, id := range []uint64{responseOnly.ID, providerOnly.ID} {
		audit, err := recoveryStore.FindRequestDispatchAudit(id)
		if err != nil {
			t.Fatalf("read active evidence audit %d: %v", id, err)
		}
		if audit.TerminalAt != nil {
			t.Fatalf("active owner evidence audit %d was finalized: %+v", id, audit)
		}
	}

	expirePostgresDispatchOwner(t, db, store)
	if err := service.RecoverRequestDispatchOrphans(now); err != nil {
		t.Fatalf("recover response-only and provider-only dispatches: %v", err)
	}

	responseAudit, err := recoveryStore.FindRequestDispatchAudit(responseOnly.ID)
	if err != nil {
		t.Fatalf("read completed response-only audit: %v", err)
	}
	if responseAudit.Stage != storage.GatewayRequestDispatchStageFinalized || responseAudit.TerminalKind != storage.GatewayRequestDispatchTerminalOwnerLeaseLost || responseAudit.TerminalAt == nil || responseAudit.ProviderRequestID != "" || responseAudit.RequestLogID == nil {
		t.Fatalf("response-only recovery state = %+v", responseAudit)
	}
	var responseLog storage.GatewayRequestLog
	if err := db.First(&responseLog, "id = ?", *responseAudit.RequestLogID).Error; err != nil {
		t.Fatalf("read response-only recovery log: %v", err)
	}
	if responseLog.RecordKind != storage.GatewayRequestRecordKindRequest || responseLog.StatusCode != 502 || responseLog.UpstreamRequestID != "" || responseLog.UpstreamAccepted || responseLog.BillingCommitted {
		t.Fatalf("response-only recovery log = %+v", responseLog)
	}
	providerAudit, err := recoveryStore.FindRequestDispatchAudit(providerOnly.ID)
	if err != nil {
		t.Fatalf("read retained provider-only audit: %v", err)
	}
	if providerAudit.Stage != storage.GatewayRequestDispatchStageFinalized || providerAudit.TerminalKind != storage.GatewayRequestDispatchTerminalOwnerLeaseLost || providerAudit.TerminalAt == nil || providerAudit.ProviderRequestID != "provider-only-cost-evidence" || providerAudit.RequestLogID == nil {
		t.Fatalf("provider-only evidence was not finalized: %+v", providerAudit)
	}
	var evidence storage.GatewayRequestLog
	if err := db.First(&evidence, "id = ?", *providerAudit.RequestLogID).Error; err != nil {
		t.Fatalf("read provider-only evidence log: %v", err)
	}
	wantReconcileAt := InitialRequestCostReconcileAt(now)
	if evidence.RecordKind != storage.GatewayRequestRecordKindDispatchCostEvidence || evidence.UpstreamRequestID != "provider-only-cost-evidence" || evidence.UpstreamAccepted || evidence.BillingCommitted || evidence.StatusCode != 0 || evidence.LatencyMs != 0 || evidence.TotalTokens != 0 || evidence.Error != "" || !evidence.CreatedAt.Equal(now) || evidence.CostReconcileAt == nil || !evidence.CostReconcileAt.Equal(wantReconcileAt) {
		t.Fatalf("provider-only hidden evidence = %+v, want reconcile at %v", evidence, wantReconcileAt)
	}
	var ordinaryLogs int64
	if err := db.Model(&storage.GatewayRequestLog{}).Where("channel_id = ? AND record_kind = ?", 7402, storage.GatewayRequestRecordKindRequest).Count(&ordinaryLogs).Error; err != nil || ordinaryLogs != 0 {
		t.Fatalf("provider-only ordinary logs = %d err=%v", ordinaryLogs, err)
	}
	var aggregateRequests int64
	if err := db.Model(&storage.GatewayRequestAggregate{}).Where("channel_id = ?", 7402).Select("COALESCE(SUM(requests), 0)").Scan(&aggregateRequests).Error; err != nil || aggregateRequests != 0 {
		t.Fatalf("provider-only aggregate requests = %d err=%v", aggregateRequests, err)
	}
	if err := service.RecoverRequestDispatchOrphans(now.Add(time.Minute)); err != nil {
		t.Fatalf("repeat provider-only recovery: %v", err)
	}
	var evidenceCount int64
	if err := db.Model(&storage.GatewayRequestLog{}).Where("channel_id = ? AND record_kind = ?", 7402, storage.GatewayRequestRecordKindDispatchCostEvidence).Count(&evidenceCount).Error; err != nil || evidenceCount != 1 {
		t.Fatalf("provider-only evidence logs = %d err=%v", evidenceCount, err)
	}
}

func assertPostgresRecoveredDispatch(t *testing.T, db *gorm.DB, auditID uint64, snapshot storage.GatewayRequestDispatchSnapshot, recoveredAt time.Time, providerRequestID string, wantAccepted, wantCommitted bool) {
	t.Helper()
	store := storage.NewGateway(db)
	audit, err := store.FindRequestDispatchAudit(auditID)
	if err != nil {
		t.Fatalf("read recovered dispatch %d: %v", auditID, err)
	}
	if audit.Stage != storage.GatewayRequestDispatchStageFinalized || audit.TerminalKind != storage.GatewayRequestDispatchTerminalOwnerLeaseLost || audit.TerminalAt == nil || !audit.TerminalAt.Equal(recoveredAt) || audit.RequestLogID == nil {
		t.Fatalf("recovered audit %d terminal state = %+v", auditID, audit)
	}
	if audit.ProviderRequestID != providerRequestID {
		t.Fatalf("recovered audit %d provider request id = %q, want %q", auditID, audit.ProviderRequestID, providerRequestID)
	}

	var log storage.GatewayRequestLog
	if err := db.First(&log, "id = ?", *audit.RequestLogID).Error; err != nil {
		t.Fatalf("read recovered request log for dispatch %d: %v", auditID, err)
	}
	if log.StatusCode != 502 || log.Error != "request completion could not be confirmed after a service interruption" || log.RouteReason != "dispatch_recovery" {
		t.Fatalf("recovered request outcome = %+v", log)
	}
	if log.UpstreamRequestID != providerRequestID || log.UpstreamAccepted != wantAccepted || log.BillingCommitted != wantCommitted {
		t.Fatalf("recovered billing facts = %+v", log)
	}
	if log.UpstreamRequestID == audit.AcceptedObjectID && audit.AcceptedObjectID != "" {
		t.Fatalf("protocol object id %q was substituted for provider request id", audit.AcceptedObjectID)
	}
	if log.DownstreamKeyID != snapshot.DownstreamKeyID || log.ChannelID != snapshot.ChannelID || log.ChannelNameSnapshot != snapshot.ChannelNameSnapshot || log.GroupName != snapshot.GroupName || log.UpstreamKeyID != snapshot.UpstreamKeyID || log.UpstreamKeyName != snapshot.UpstreamKeyName {
		t.Fatalf("recovered route identity snapshot = %+v", log)
	}
	if log.ModelName != snapshot.ModelName || log.UpstreamModelName != snapshot.UpstreamModelName || log.ReasoningEffort != snapshot.ReasoningEffort || log.RequestedModelName != snapshot.RequestedModelName || log.RequestedReasoningEffort != snapshot.RequestedReasoningEffort || log.ModelMappingStrategy != snapshot.ModelMappingStrategy || log.ModelMappingDetails != snapshot.ModelMappingDetails {
		t.Fatalf("recovered model snapshot = %+v", log)
	}
	if log.Method != snapshot.Method || log.Path != snapshot.Path || log.ClientIP != snapshot.ClientIP || log.ClientIPSource != snapshot.ClientIPSource || log.SessionHash != snapshot.SessionHash {
		t.Fatalf("recovered request/client snapshot = %+v", log)
	}
	if log.RouteSelectionRank != snapshot.RouteSelectionRank || log.SessionAffinityOriginRequestID != snapshot.SessionAffinityOriginRequestID || log.RouteDetails != snapshot.RouteDetails || log.RouteAttemptedChannels != snapshot.RouteAttemptedChannels {
		t.Fatalf("recovered route audit snapshot = %+v", log)
	}
	if log.GroupRatio != audit.GroupRatio || log.CompletionRatio != audit.CompletionRatio ||
		!postgresDispatchRecoveryNumbersEqual(log.RawGroupRatio, audit.RawGroupRatio) ||
		!postgresDispatchRecoveryNumbersEqual(log.RawCompletionRatio, audit.RawCompletionRatio) ||
		!postgresDispatchRecoveryNumbersEqual(log.FundingFactor, audit.FundingFactor) ||
		log.CostBasisVersion != audit.CostBasisVersion || log.CostBasisSource != audit.CostBasisSource {
		t.Fatalf("recovered cost basis snapshot = %+v", log)
	}
	if log.LatencyMs != recoveredAt.Sub(snapshot.RequestStartedAt).Milliseconds() || !log.CreatedAt.Equal(recoveredAt) {
		t.Fatalf("recovered timing = latency %d created %s", log.LatencyMs, log.CreatedAt)
	}
	wantReconcileAt := InitialRequestCostReconcileAt(recoveredAt)
	wantCostStatus := storage.GatewayCostStatusNone
	pricingFence := storage.IsGatewayRequestPricingFenceUnavailable(&storage.GatewayRequestLog{
		CostStatus:            storage.GatewayCostStatusUnavailable,
		CostUnavailableReason: audit.PricingUnavailableReason,
	})
	if pricingFence {
		wantCostStatus = storage.GatewayCostStatusUnavailable
	}
	if log.CostReconcileAt == nil || !log.CostReconcileAt.Equal(wantReconcileAt) || log.CostStatus != wantCostStatus || log.CostUnavailableReason != audit.PricingUnavailableReason || log.Cost != 0 || log.CostEstimated || log.CostSource != "" || log.CostReconcileTries != 0 || log.CostReconcileError != "" {
		t.Fatalf("recovered cost reconciliation state = %+v, want due %s", log, wantReconcileAt)
	}
	if eligible := storage.IsGatewayRequestCostReconcileEligible(&log); eligible != !pricingFence {
		t.Fatalf("recovered request reconciliation eligibility = %v with pricing reason %q: %+v", eligible, audit.PricingUnavailableReason, log)
	}
}

func assertPostgresRecoveryCounts(t *testing.T, db *gorm.DB, channelIDs []uint, wantLogs, wantAggregateRequests int64) {
	t.Helper()
	var logCount int64
	if err := db.Model(&storage.GatewayRequestLog{}).Where("channel_id IN ?", channelIDs).Count(&logCount).Error; err != nil {
		t.Fatalf("count recovered request logs: %v", err)
	}
	if logCount != wantLogs {
		t.Fatalf("recovered request logs = %d, want %d", logCount, wantLogs)
	}
	var aggregateRequests int64
	if err := db.Model(&storage.GatewayRequestAggregate{}).Where("channel_id IN ?", channelIDs).Select("COALESCE(SUM(requests), 0)").Scan(&aggregateRequests).Error; err != nil {
		t.Fatalf("sum recovered aggregate requests: %v", err)
	}
	if aggregateRequests != wantAggregateRequests {
		t.Fatalf("recovered aggregate requests = %d, want %d", aggregateRequests, wantAggregateRequests)
	}
}
