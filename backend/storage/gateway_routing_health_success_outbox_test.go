package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

type routingHealthSuccessCommitUnknownPool struct {
	gorm.ConnPool
	injected atomic.Bool
}

func (p *routingHealthSuccessCommitUnknownPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
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
	return &routingHealthSuccessCommitUnknownTx{Tx: tx, pool: p}, nil
}

type routingHealthSuccessCommitUnknownTx struct {
	*sql.Tx
	pool *routingHealthSuccessCommitUnknownPool
}

func (tx *routingHealthSuccessCommitUnknownTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.pool.injected.CompareAndSwap(false, true) {
		return driver.ErrBadConn
	}
	return nil
}

type routingHealthSuccessOutboxFixture struct {
	db         *gorm.DB
	store      *Gateway
	channel    Channel
	generation UpstreamGroupGeneration
	key        GatewayUpstreamKey
	audit      GatewayRequestDispatchAudit
	base       time.Time
}

func newRoutingHealthSuccessOutboxFixture(t *testing.T) routingHealthSuccessOutboxFixture {
	t.Helper()
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: "routing-health-success-outbox", Type: ChannelTypeNewAPI,
		SiteURL: "https://routing-health-success-outbox.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create routing health success channel: %v", err)
	}
	if _, err := rates.Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create routing health success group: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture routing health success generation: %v", err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 91, UpstreamKeyName: "bridgeops-primary",
		APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", Enabled: true,
	}); err != nil {
		t.Fatalf("create routing health success upstream key: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil {
		t.Fatalf("load routing health success upstream key: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	routeExclusions, err := NewGatewayRouteExclusionSnapshot(base, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("create routing health success exclusion snapshot: %v", err)
	}
	snapshot := GatewayRequestDispatchSnapshot{
		LogicalRequestTraceID:         "cccccccccccccccccccccccccccccccc",
		TelemetryVersion:              GatewayRequestTelemetryVersion,
		RoutingMode:                   GatewayRoutingModeSmart,
		GatewayApp:                    string(GatewayAppCodex),
		FirstByteBudgetMs:             30_000,
		RouteExclusionSnapshotVersion: GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:       routeExclusions,
		DownstreamKeyID:               7001,
		ChannelID:                     channel.ID,
		ChannelNameSnapshot:           channel.Name,
		GroupName:                     generation.CanonicalGroupName,
		UpstreamKeyID:                 key.UpstreamKeyID,
		UpstreamKeyName:               key.UpstreamKeyName,
		ModelName:                     "model-exact",
		UpstreamModelName:             "model-exact",
		Method:                        "POST",
		Path:                          "/v1/chat/completions",
		RouteReason:                   "smart_primary",
		GroupRatio:                    1,
		CompletionRatio:               1,
		CostBasisVersion:              1,
		CostBasisSource:               "channel_balance",
		RequestStartedAt:              base,
		ChannelIdentityRevision:       channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:               generation.ID,
		UpstreamGroupGeneration:       generation.PresenceGeneration,
		UpstreamKeyLocalID:            key.ID,
		UpstreamKeyGeneration:         key.KeyGeneration,
	}
	audit, err := store.BeginRequestDispatch(snapshot, base)
	if err != nil {
		t.Fatalf("begin routing health success dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, base.Add(time.Millisecond)); err != nil {
		t.Fatalf("mark routing health success dispatch physical: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(audit.ID, base.Add(2*time.Millisecond)); err != nil {
		t.Fatalf("confirm routing health success dispatch transport: %v", err)
	}
	return routingHealthSuccessOutboxFixture{
		db: db, store: store, channel: *channel, generation: generation, key: *key, audit: *audit, base: base,
	}
}

func finalizeRoutingHealthSuccessOutboxFixture(t *testing.T, fixture routingHealthSuccessOutboxFixture, routeReason, terminalKind string, at time.Time) *GatewayRequestLog {
	t.Helper()
	log := &GatewayRequestLog{StatusCode: 200, RouteReason: routeReason, CreatedAt: at}
	created, err := fixture.store.FinalizeRequestDispatchResult(fixture.audit.ID, log, terminalKind, at)
	if err != nil || !created || log.ID == 0 {
		t.Fatalf("finalize routing health success: created=%t log=%+v err=%v", created, log, err)
	}
	return log
}

func TestGatewayRoutingHealthSuccessOutboxDeliversFinalizedInference(t *testing.T) {
	fixture := newRoutingHealthSuccessOutboxFixture(t)
	fence := NewGatewayRoutingHealthFence(fixture.generation, fixture.channel.UpstreamAccountIdentityRevision)
	failureAt := fixture.base.Add(2 * time.Millisecond)
	if err := fixture.store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "http_status", 503, "temporary failure", failureAt); err != nil {
		t.Fatalf("record pre-success routing failure: %v", err)
	}
	successAt := fixture.base.Add(4 * time.Millisecond)
	log := finalizeRoutingHealthSuccessOutboxFixture(t, fixture, "smart_primary", "success", successAt)

	var row GatewayRoutingHealthSuccessOutbox
	if err := fixture.db.First(&row, "dispatch_id = ?", fixture.audit.ID).Error; err != nil {
		t.Fatalf("load durable routing health success: %v", err)
	}
	if row.RequestLogID != log.ID || row.ChannelID != fixture.channel.ID ||
		row.ChannelIdentityRevision != fence.ChannelIdentityRevision || row.UpstreamGroupID != fence.UpstreamGroupID ||
		row.PresenceGeneration != fence.PresenceGeneration || row.CanonicalGroupName != fence.CanonicalGroupName ||
		!row.ObservedAt.Equal(successAt) || row.AttemptCount != 0 || row.LastError != "" {
		t.Fatalf("durable routing health success row = %+v", row)
	}
	before, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.generation.CanonicalGroupName)
	if err != nil || before.LastUsedAt != nil || before.RoutingFailureCount != 1 || before.LastRoutingFailureAt == nil {
		t.Fatalf("health changed before outbox delivery: key=%+v err=%v", before, err)
	}

	delivered, err := fixture.store.DeliverRoutingHealthSuccessOutbox(10)
	if err != nil || delivered != 1 {
		t.Fatalf("deliver routing health success outbox: delivered=%d err=%v", delivered, err)
	}
	var remaining int64
	if err := fixture.db.Model(&GatewayRoutingHealthSuccessOutbox{}).Where("dispatch_id = ?", fixture.audit.ID).Count(&remaining).Error; err != nil || remaining != 0 {
		t.Fatalf("remaining routing health success rows = %d err=%v", remaining, err)
	}
	after, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.generation.CanonicalGroupName)
	if err != nil || after.LastUsedAt == nil || !after.LastUsedAt.Equal(successAt) || after.LastRoutingFailureAt != nil ||
		after.RoutingFailureCount != 0 || after.RoutingCooldownUntil != nil || !fence.MatchesUpstreamKey(after) {
		t.Fatalf("delivered routing health state = %+v err=%v", after, err)
	}

	// Replaying a completed batch is harmless and does not synthesize work.
	delivered, err = fixture.store.DeliverRoutingHealthSuccessOutbox(10)
	if err != nil || delivered != 0 {
		t.Fatalf("replay empty routing health success outbox: delivered=%d err=%v", delivered, err)
	}
}

func TestGatewayRoutingHealthSuccessOutboxDiscardsPriorAccountSuccess(t *testing.T) {
	fixture := newRoutingHealthSuccessOutboxFixture(t)
	oldFence := NewGatewayRoutingHealthFence(fixture.generation, fixture.channel.UpstreamAccountIdentityRevision)
	newRevision := fixture.channel.UpstreamAccountIdentityRevision + 1
	if err := fixture.db.Session(&gorm.Session{SkipHooks: true}).Model(&Channel{}).
		Where("id = ?", fixture.channel.ID).
		UpdateColumn("upstream_account_identity_revision", newRevision).Error; err != nil {
		t.Fatalf("switch channel account identity: %v", err)
	}
	newFence := NewGatewayRoutingHealthFence(fixture.generation, newRevision)
	newFailureAt := fixture.base.Add(2 * time.Millisecond)
	if err := fixture.store.RecordUpstreamKeyRoutingFailureForRoutingHealth(newFence, "transport", 0, "new account failure", newFailureAt); err != nil {
		t.Fatalf("record new-account routing failure: %v", err)
	}
	oldSuccessAt := fixture.base.Add(4 * time.Millisecond)
	finalizeRoutingHealthSuccessOutboxFixture(t, fixture, "smart_primary", GatewayRequestDispatchTerminalOwnerLeaseLost, oldSuccessAt)

	var queued int64
	if err := fixture.db.Model(&GatewayRoutingHealthSuccessOutbox{}).Where("dispatch_id = ?", fixture.audit.ID).Count(&queued).Error; err != nil || queued != 1 {
		t.Fatalf("queued old-account routing health successes = %d err=%v", queued, err)
	}
	delivered, err := fixture.store.DeliverRoutingHealthSuccessOutbox(10)
	if err != nil || delivered != 1 {
		t.Fatalf("discard stale routing health success: delivered=%d err=%v", delivered, err)
	}
	current, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.generation.CanonicalGroupName)
	if err != nil || current.LastUsedAt != nil || current.LastRoutingFailureAt == nil || !current.LastRoutingFailureAt.Equal(newFailureAt) ||
		current.RoutingFailureCount != 1 || !newFence.MatchesUpstreamKey(current) || oldFence.MatchesUpstreamKey(current) {
		t.Fatalf("stale success changed replacement-account health: key=%+v err=%v", current, err)
	}
	if err := fixture.db.Model(&GatewayRoutingHealthSuccessOutbox{}).Where("dispatch_id = ?", fixture.audit.ID).Count(&queued).Error; err != nil || queued != 0 {
		t.Fatalf("stale routing health success remained queued: count=%d err=%v", queued, err)
	}
}

func TestGatewayRoutingHealthSuccessOutboxRetainsTransientDeliveryFailure(t *testing.T) {
	fixture := newRoutingHealthSuccessOutboxFixture(t)
	successAt := fixture.base.Add(4 * time.Millisecond)
	finalizeRoutingHealthSuccessOutboxFixture(t, fixture, "smart_primary", "success", successAt)

	injectedErr := errors.New("injected routing health write failure")
	faultDB := fixture.db.Session(&gorm.Session{NewDB: true})
	const callbackName = "test:routing_health_success_outbox_failure"
	if err := faultDB.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		table := tx.Statement.Table
		if table == "" && tx.Statement.Schema != nil {
			table = tx.Statement.Schema.Table
		}
		if table == "gateway_upstream_keys" {
			tx.AddError(injectedErr)
		}
	}); err != nil {
		t.Fatalf("register routing health failure callback: %v", err)
	}
	callbackInstalled := true
	t.Cleanup(func() {
		if callbackInstalled {
			_ = faultDB.Callback().Update().Remove(callbackName)
		}
	})
	delivered, err := NewGateway(faultDB).DeliverRoutingHealthSuccessOutbox(10)
	if delivered != 0 || !errors.Is(err, injectedErr) {
		t.Fatalf("transient routing health delivery: delivered=%d err=%v", delivered, err)
	}
	if err := faultDB.Callback().Update().Remove(callbackName); err != nil {
		t.Fatalf("remove routing health failure callback: %v", err)
	}
	callbackInstalled = false

	var retained GatewayRoutingHealthSuccessOutbox
	if err := fixture.db.First(&retained, "dispatch_id = ?", fixture.audit.ID).Error; err != nil {
		t.Fatalf("load retained routing health success: %v", err)
	}
	if retained.AttemptCount != 1 || retained.LastError == "" {
		t.Fatalf("retained routing health success retry state = %+v", retained)
	}
	delivered, err = fixture.store.DeliverRoutingHealthSuccessOutbox(10)
	if err != nil || delivered != 1 {
		t.Fatalf("retry routing health success delivery: delivered=%d err=%v", delivered, err)
	}
	key, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.generation.CanonicalGroupName)
	if err != nil || key.LastUsedAt == nil || !key.LastUsedAt.Equal(successAt) {
		t.Fatalf("retried routing health success state = %+v err=%v", key, err)
	}
}

func TestGatewayRoutingHealthSuccessOutboxSurvivesFinalizationCommitUnknown(t *testing.T) {
	fixture := newRoutingHealthSuccessOutboxFixture(t)
	successAt := fixture.base.Add(4 * time.Millisecond)
	faultDB := fixture.db.Session(&gorm.Session{NewDB: true})
	pool := &routingHealthSuccessCommitUnknownPool{ConnPool: faultDB.Statement.ConnPool}
	faultDB.Statement.ConnPool = pool
	fixture.store.db = faultDB

	log := &GatewayRequestLog{StatusCode: 200, RouteReason: "smart_primary", CreatedAt: successAt}
	created, err := fixture.store.FinalizeRequestDispatchResult(fixture.audit.ID, log, "success", successAt)
	if created || err == nil || !pool.injected.Load() || log.ID != 0 {
		t.Fatalf("commit-unknown routing health finalization: created=%t log=%+v injected=%t err=%v", created, log, pool.injected.Load(), err)
	}

	fixture.store.db = fixture.db
	created, err = fixture.store.FinalizeRequestDispatchResult(fixture.audit.ID, log, "success", successAt)
	if err != nil || created || log.ID == 0 {
		t.Fatalf("retry committed routing health finalization: created=%t log=%+v err=%v", created, log, err)
	}
	var rows []GatewayRoutingHealthSuccessOutbox
	if err := fixture.db.Where("dispatch_id = ?", fixture.audit.ID).Find(&rows).Error; err != nil {
		t.Fatalf("load commit-unknown routing health success: %v", err)
	}
	if len(rows) != 1 || rows[0].RequestLogID != log.ID || !rows[0].ObservedAt.Equal(successAt) {
		t.Fatalf("commit-unknown routing health successes = %+v", rows)
	}
	delivered, err := fixture.store.DeliverRoutingHealthSuccessOutbox(10)
	if err != nil || delivered != 1 {
		t.Fatalf("deliver commit-unknown routing health success: delivered=%d err=%v", delivered, err)
	}
}

func TestGatewayRoutingHealthSuccessOutboxUsesIndependentFinalizationFence(t *testing.T) {
	fixture := newRoutingHealthSuccessOutboxFixture(t)
	routeExclusions, err := NewGatewayRouteExclusionSnapshot(fixture.base, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("create independent-fence exclusion snapshot: %v", err)
	}
	snapshot := GatewayRequestDispatchSnapshot{
		LogicalRequestTraceID:         "dddddddddddddddddddddddddddddddd",
		TelemetryVersion:              GatewayRequestTelemetryVersion,
		RoutingMode:                   GatewayRoutingModeSmart,
		GatewayApp:                    string(GatewayAppCodex),
		FirstByteBudgetMs:             30_000,
		RouteExclusionSnapshotVersion: GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:       routeExclusions,
		DownstreamKeyID:               7002,
		ChannelID:                     fixture.channel.ID,
		ChannelNameSnapshot:           fixture.channel.Name,
		GroupName:                     fixture.generation.CanonicalGroupName,
		UpstreamKeyID:                 fixture.key.UpstreamKeyID,
		UpstreamKeyName:               fixture.key.UpstreamKeyName,
		ModelName:                     "model-exact",
		UpstreamModelName:             "model-exact",
		Method:                        "POST",
		Path:                          "/v1/chat/completions",
		RouteReason:                   "smart_primary",
		RequestStartedAt:              fixture.base,
	}
	audit, err := fixture.store.BeginRequestDispatch(snapshot, fixture.base)
	if err != nil {
		t.Fatalf("begin independent-fence dispatch: %v", err)
	}
	if audit.ChannelIdentityRevision != 0 || audit.UpstreamGroupID != 0 || audit.UpstreamGroupGeneration != 0 {
		t.Fatalf("independent-fence audit unexpectedly retained pricing identity: %+v", audit)
	}
	if err := fixture.store.MarkRequestDispatchPhysicallyDispatched(audit.ID, fixture.base.Add(time.Millisecond)); err != nil {
		t.Fatalf("mark independent-fence dispatch physical: %v", err)
	}
	if err := fixture.store.ConfirmRequestDispatchTransportInvocation(audit.ID, fixture.base.Add(2*time.Millisecond)); err != nil {
		t.Fatalf("confirm independent-fence dispatch transport: %v", err)
	}
	fence := NewGatewayRoutingHealthFence(fixture.generation, fixture.channel.UpstreamAccountIdentityRevision)
	successAt := fixture.base.Add(4 * time.Millisecond)
	log := &GatewayRequestLog{StatusCode: 200, RouteReason: "smart_primary", CreatedAt: successAt}
	created, err := fixture.store.FinalizeRequestDispatchWithFactsResult(
		audit.ID,
		log,
		"success",
		GatewayRequestDispatchFinalizationFacts{RoutingHealthFence: &fence},
		successAt,
	)
	if err != nil || !created || log.ID == 0 {
		t.Fatalf("finalize independent-fence success: created=%t log=%+v err=%v", created, log, err)
	}
	var row GatewayRoutingHealthSuccessOutbox
	if err := fixture.db.First(&row, "dispatch_id = ?", audit.ID).Error; err != nil {
		t.Fatalf("load independent-fence routing health success: %v", err)
	}
	if row.ChannelIdentityRevision != fence.ChannelIdentityRevision || row.UpstreamGroupID != fence.UpstreamGroupID ||
		row.PresenceGeneration != fence.PresenceGeneration || row.CanonicalGroupName != fence.CanonicalGroupName {
		t.Fatalf("independent routing health fence was not persisted: %+v", row)
	}
}

func TestGatewayRoutingHealthFenceSurvivesDurableHandoffFactsJSON(t *testing.T) {
	fence := GatewayRoutingHealthFence{
		ChannelID: 7, ChannelIdentityRevision: 11, UpstreamGroupID: 13,
		PresenceGeneration: 17, CanonicalGroupName: "primary",
	}
	encoded, err := encodeGatewayRequestDispatchTerminalFacts(GatewayRequestDispatchFinalizationFacts{RoutingHealthFence: &fence})
	if err != nil {
		t.Fatalf("encode routing health success handoff facts: %v", err)
	}
	decoded, err := decodeGatewayRequestDispatchTerminalFacts(encoded)
	if err != nil {
		t.Fatalf("decode routing health success handoff facts: %v", err)
	}
	if decoded.RoutingHealthFence == nil || *decoded.RoutingHealthFence != fence {
		t.Fatalf("decoded routing health success fence = %+v, want %+v", decoded.RoutingHealthFence, fence)
	}
	if _, err := normalizeGatewayRequestDispatchFinalizationFacts(GatewayRequestDispatchFinalizationFacts{
		RoutingHealthFence: &GatewayRoutingHealthFence{ChannelID: 7},
	}, time.Now().UTC()); err == nil {
		t.Fatal("incomplete routing health success fence was accepted")
	}
}

func TestGatewayRequestCarriesRoutingHealthSuccessRejectsInternalTraffic(t *testing.T) {
	base := GatewayRequestLog{
		ID: 1, DownstreamKeyID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
	}
	tests := []struct {
		name   string
		mutate func(*GatewayRequestLog)
		want   bool
	}{
		{name: "real inference", want: true},
		{name: "api test", mutate: func(log *GatewayRequestLog) { log.RouteReason = gatewayRouteReasonKeyTest }},
		{name: "latency probe", mutate: func(log *GatewayRequestLog) { log.RouteReason = gatewayRouteReasonLatencyProbe }},
		{name: "model check", mutate: func(log *GatewayRequestLog) { log.RouteReason = "model_check" }},
		{name: "internal route", mutate: func(log *GatewayRequestLog) { log.RouteReason = "internal_probe" }},
		{name: "count tokens", mutate: func(log *GatewayRequestLog) { log.Path = "/v1/messages/count_tokens" }},
		{name: "redirect", mutate: func(log *GatewayRequestLog) { log.StatusCode = 302 }},
		{name: "error payload", mutate: func(log *GatewayRequestLog) { log.Error = "protocol error" }},
		{name: "no downstream key", mutate: func(log *GatewayRequestLog) { log.DownstreamKeyID = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := base
			if test.mutate != nil {
				test.mutate(&log)
			}
			if got := gatewayRequestCarriesRoutingHealthSuccess(&log); got != test.want {
				t.Fatalf("routing health success eligibility = %t, want %t: %+v", got, test.want, log)
			}
		})
	}
}
