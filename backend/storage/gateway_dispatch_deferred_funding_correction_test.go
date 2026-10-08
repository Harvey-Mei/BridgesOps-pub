package storage

import (
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testpostgres"
	"gorm.io/gorm"
)

func TestDispatchEvidenceDeferredFundingCorrectionFundingThenReconcile(t *testing.T) {
	requireDeferredFundingPostgres(t)
	db := openTestDB(t)
	channel, state, evidence, effectiveAt := createPendingDispatchEvidenceForFundingTest(t, db, "funding-then-reconcile")

	next := *state
	next.FundingFactor = 2
	next.Source = "funding_correction"
	candidate := &ChannelFundingCandidate{
		ChannelID: channel.ID, Source: "balance_increase", ExternalID: "deferred-funding-candidate",
		Status: FundingCandidatePending, CreditedAmount: 10, RawBalanceDetected: 10,
		FundingFactorBefore: state.FundingFactor, DetectedAt: effectiveAt.Add(-time.Second),
	}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatalf("create funding candidate: %v", err)
	}
	event := &ChannelFundingEvent{
		ChannelID: channel.ID, Kind: "funding_correction",
		FundingFactorBefore: state.FundingFactor, FundingFactorAfter: next.FundingFactor,
		EffectiveAt: effectiveAt,
	}
	if err := NewCosting(db).ConfirmFundingCandidate(&next, event, candidate.ID, state.Version); err != nil {
		t.Fatalf("confirm funding before exact reconciliation: %v", err)
	}

	var pending GatewayRequestLog
	if err := db.First(&pending, evidence.ID).Error; err != nil {
		t.Fatalf("read deferred dispatch evidence: %v", err)
	}
	assertPendingDispatchDeferredFunding(t, pending, state.FundingFactor, next.FundingFactor, next.Version, next.Source)

	if err := NewGateway(db).UpdateRequestLogActualCostWithRaw(
		evidence.ID, "usage-funding-then-reconcile", 0.25, 0.25,
		gatewayCostSourceSub2APIActualCost, 0, 0, false,
	); err != nil {
		t.Fatalf("reconcile dispatch evidence after funding: %v", err)
	}
	assertReconciledDispatchFundingBasis(t, db, evidence.ID, 0.25, next.FundingFactor, next.Version, next.Source)
}

func TestDispatchEvidenceDeferredFundingCorrectionReconcileThenFunding(t *testing.T) {
	requireDeferredFundingPostgres(t)
	db := openTestDB(t)
	channel, state, evidence, effectiveAt := createPendingDispatchEvidenceForFundingTest(t, db, "reconcile-then-funding")

	if err := NewGateway(db).UpdateRequestLogActualCostWithRaw(
		evidence.ID, "usage-reconcile-then-funding", 0.25, 0.25,
		gatewayCostSourceSub2APIActualCost, 0, 0, false,
	); err != nil {
		t.Fatalf("reconcile dispatch evidence before funding: %v", err)
	}

	next := *state
	next.FundingFactor = 2
	next.Source = "funding_correction"
	event := &ChannelFundingEvent{
		ChannelID: channel.ID, Kind: "funding_correction",
		FundingFactorBefore: state.FundingFactor, FundingFactorAfter: next.FundingFactor,
		EffectiveAt: effectiveAt,
	}
	if err := NewCosting(db).AppendFundingEvent(&next, event, state.Version); err != nil {
		t.Fatalf("apply funding after exact reconciliation: %v", err)
	}
	assertReconciledDispatchFundingBasis(t, db, evidence.ID, 0.25, next.FundingFactor, next.Version, next.Source)
}

func TestDispatchEvidenceFundingCorrectionBeforeLateMaterialization(t *testing.T) {
	requireDeferredFundingPostgres(t)
	db := openTestDB(t)
	channel := &Channel{
		Name: "deferred dispatch funding late materialization", Type: ChannelTypeNewAPI,
		SiteURL: "https://deferred-funding.example", PasswordCipher: "test",
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatalf("create late-materialization channel: %v", err)
	}
	state, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatalf("load late-materialization cost state: %v", err)
	}
	effectiveAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	dispatchedAt := effectiveAt.Add(time.Minute)
	next := *state
	next.FundingFactor = 2
	next.Source = "funding_correction"
	event := &ChannelFundingEvent{
		ChannelID: channel.ID, Kind: "funding_correction",
		FundingFactorBefore: state.FundingFactor, FundingFactorAfter: next.FundingFactor,
		EffectiveAt: effectiveAt,
	}
	if err := NewCosting(db).AppendFundingEvent(&next, event, state.Version); err != nil {
		t.Fatalf("apply funding before late materialization: %v", err)
	}
	var correction GatewayRequestFundingCorrection
	if err := db.Where("funding_event_id = ?", event.ID).First(&correction).Error; err != nil {
		t.Fatalf("load open funding correction: %v", err)
	}
	if correction.IntervalEnd != nil {
		t.Fatalf("latest funding correction unexpectedly ended at %v", *correction.IntervalEnd)
	}
	postCommitAt := time.Now().UTC().Add(time.Minute)
	matched, err := gatewayRequestFundingCorrectionAtTx(db, channel.ID, postCommitAt, state.Version)
	if err != nil || matched == nil || matched.ID != correction.ID {
		t.Fatalf("open funding correction did not cover stale post-commit basis: correction=%+v err=%v", matched, err)
	}
	matched, err = gatewayRequestFundingCorrectionAtTx(db, channel.ID, postCommitAt, next.Version)
	if err != nil || matched != nil {
		t.Fatalf("open funding correction overrode an equal-or-newer basis: correction=%+v err=%v", matched, err)
	}

	frozenFactor := state.FundingFactor
	audit := &GatewayRequestDispatchAudit{
		ChannelID: channel.ID, ChannelNameSnapshot: channel.Name,
		GroupName: "primary", FundingFactor: &frozenFactor,
		CostBasisVersion: state.Version, CostBasisSource: state.Source,
		DispatchedAt: dispatchedAt,
	}
	evidence := &GatewayRequestLog{
		RecordKind:        GatewayRequestRecordKindDispatchCostEvidence,
		UpstreamRequestID: "provider-late-materialization",
		CreatedAt:         time.Now().UTC(),
	}
	applyGatewayRequestDispatchSnapshotToLog(audit, evidence)
	if err := NewGateway(db).AppendRequestLog(evidence); err != nil {
		t.Fatalf("materialize dispatch evidence after funding: %v", err)
	}
	if evidence.FundingBasisAt == nil || !evidence.FundingBasisAt.Equal(dispatchedAt) ||
		evidence.FundingFactor == nil || math.Abs(*evidence.FundingFactor-state.FundingFactor) > 1e-12 ||
		evidence.CostBasisVersion != state.Version || evidence.CostBasisSource != state.Source ||
		evidence.DeferredFundingFactor == nil || math.Abs(*evidence.DeferredFundingFactor-next.FundingFactor) > 1e-12 ||
		evidence.DeferredCostBasisVersion == nil || *evidence.DeferredCostBasisVersion != next.Version ||
		evidence.DeferredCostBasisSource == nil || *evidence.DeferredCostBasisSource != next.Source {
		t.Fatalf("late materialized dispatch funding facts = %+v", evidence)
	}
	if err := NewGateway(db).UpdateRequestLogActualCostWithRaw(
		evidence.ID, "usage-late-materialization", 0.25, 0.25,
		gatewayCostSourceSub2APIActualCost, 0, 0, false,
	); err != nil {
		t.Fatalf("reconcile late materialized dispatch evidence: %v", err)
	}
	assertReconciledDispatchFundingBasis(t, db, evidence.ID, 0.25, next.FundingFactor, next.Version, next.Source)
}

func TestDispatchEvidenceDeferredFundingCorrectionConcurrentWithReconcile(t *testing.T) {
	requireDeferredFundingPostgres(t)
	db := openTestDB(t)
	channel, state, evidence, effectiveAt := createPendingDispatchEvidenceForFundingTest(t, db, "concurrent")

	next := *state
	next.FundingFactor = 2
	next.Source = "funding_correction"
	event := &ChannelFundingEvent{
		ChannelID: channel.ID, Kind: "funding_correction",
		FundingFactorBefore: state.FundingFactor, FundingFactorAfter: next.FundingFactor,
		EffectiveAt: effectiveAt,
	}

	start := make(chan struct{})
	errorsByOperation := make(chan struct {
		operation string
		err       error
	}, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		errorsByOperation <- struct {
			operation string
			err       error
		}{"funding", NewCosting(db).AppendFundingEvent(&next, event, state.Version)}
	}()
	go func() {
		defer workers.Done()
		<-start
		errorsByOperation <- struct {
			operation string
			err       error
		}{"reconcile", NewGateway(db).UpdateRequestLogActualCostWithRaw(
			evidence.ID, "usage-concurrent-funding-reconcile", 0.25, 0.25,
			gatewayCostSourceSub2APIActualCost, 0, 0, false,
		)}
	}()
	close(start)
	workers.Wait()
	close(errorsByOperation)
	for result := range errorsByOperation {
		if result.err != nil {
			t.Fatalf("concurrent %s: %v", result.operation, result.err)
		}
	}
	assertReconciledDispatchFundingBasis(t, db, evidence.ID, 0.25, next.FundingFactor, next.Version, next.Source)
}

func requireDeferredFundingPostgres(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv(testpostgres.EnvironmentDSN)) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
}

func createPendingDispatchEvidenceForFundingTest(t *testing.T, db *gorm.DB, name string) (*Channel, *ChannelCostState, *GatewayRequestLog, time.Time) {
	t.Helper()
	channel := &Channel{
		Name: "deferred dispatch funding " + name, Type: ChannelTypeNewAPI,
		SiteURL: "https://deferred-funding.example", PasswordCipher: "test",
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatalf("create deferred funding channel: %v", err)
	}
	state, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatalf("load deferred funding cost state: %v", err)
	}
	effectiveAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	evidence := &GatewayRequestLog{
		RecordKind: GatewayRequestRecordKindDispatchCostEvidence,
		ChannelID:  channel.ID, UpstreamRequestID: "provider-deferred-" + name,
		CreatedAt: effectiveAt.Add(time.Minute),
	}
	if err := NewGateway(db).AppendRequestLog(evidence); err != nil {
		t.Fatalf("append pending dispatch evidence: %v", err)
	}
	if evidence.FundingFactor == nil || math.Abs(*evidence.FundingFactor-state.FundingFactor) > 1e-12 {
		t.Fatalf("dispatch evidence did not freeze current factor: log=%+v state=%+v", evidence, state)
	}
	return channel, state, evidence, effectiveAt
}

func assertPendingDispatchDeferredFunding(t *testing.T, log GatewayRequestLog, frozenFactor, deferredFactor float64, version uint64, source string) {
	t.Helper()
	if log.RecordKind != GatewayRequestRecordKindDispatchCostEvidence || log.CostStatus != GatewayCostStatusNone ||
		log.RawCost != nil || log.UpstreamUsageID != nil || log.FundingFactor == nil ||
		math.Abs(*log.FundingFactor-frozenFactor) > 1e-12 || log.DeferredFundingFactor == nil ||
		math.Abs(*log.DeferredFundingFactor-deferredFactor) > 1e-12 || log.DeferredCostBasisVersion == nil ||
		*log.DeferredCostBasisVersion != version || log.DeferredCostBasisSource == nil ||
		*log.DeferredCostBasisSource != source {
		t.Fatalf("pending dispatch deferred funding facts = %+v", log)
	}
}

func assertReconciledDispatchFundingBasis(t *testing.T, db *gorm.DB, id uint, rawCost, factor float64, version uint64, source string) {
	t.Helper()
	var got GatewayRequestLog
	if err := db.First(&got, id).Error; err != nil {
		t.Fatalf("read reconciled dispatch evidence: %v", err)
	}
	if got.RecordKind != GatewayRequestRecordKindDispatchCostEvidence || got.CostStatus != GatewayCostStatusReconciled ||
		got.RawCost == nil || math.Abs(*got.RawCost-rawCost) > 1e-12 || math.Abs(got.Cost-rawCost*factor) > 1e-12 ||
		got.FundingFactor == nil || math.Abs(*got.FundingFactor-factor) > 1e-12 || got.CostBasisVersion != version ||
		got.CostBasisSource != source || got.DeferredFundingFactor != nil || got.DeferredCostBasisVersion != nil ||
		got.DeferredCostBasisSource != nil {
		t.Fatalf("reconciled dispatch funding facts = %+v", got)
	}
}
