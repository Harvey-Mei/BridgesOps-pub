package storage

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNormalizeDispatchCostEvidenceRejectsInvalidIdentityAndAcceptance(t *testing.T) {
	for _, test := range []struct {
		name string
		log  *GatewayRequestLog
	}{
		{name: "nil"},
		{name: "unknown kind", log: &GatewayRequestLog{RecordKind: "unknown"}},
		{name: "missing provider ID", log: &GatewayRequestLog{RecordKind: GatewayRequestRecordKindDispatchCostEvidence}},
		{name: "claims accepted", log: &GatewayRequestLog{RecordKind: GatewayRequestRecordKindDispatchCostEvidence, UpstreamRequestID: "provider-accepted", UpstreamAccepted: true}},
		{name: "claims billing committed", log: &GatewayRequestLog{RecordKind: GatewayRequestRecordKindDispatchCostEvidence, UpstreamRequestID: "provider-committed", BillingCommitted: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := normalizeGatewayRequestRecord(test.log); !errors.Is(err, ErrGatewayRequestRecordInvalid) {
				t.Fatalf("normalize error = %v, want ErrGatewayRequestRecordInvalid", err)
			}
		})
	}
}

func TestDispatchCostEvidenceUsageMetricsUpdateIsNoOp(t *testing.T) {
	usage := GatewayRequestUsageMetrics{
		PromptTokens:         9,
		CompletionTokens:     8,
		TotalTokens:          17,
		CacheCreationTokens:  7,
		CacheReadTokens:      6,
		UpstreamModelName:    "provider-model",
		HasTokenMetrics:      true,
		HasCacheTokenMetrics: true,
	}
	evidence := GatewayRequestLog{RecordKind: GatewayRequestRecordKindDispatchCostEvidence, UpstreamModelName: "attempt-model"}
	evidenceAfter := evidence
	evidenceUpdates := map[string]any{}
	applyGatewayRequestUsageMetricsUpdate(&evidence, &evidenceAfter, evidenceUpdates, usage)
	if len(evidenceUpdates) != 0 || evidenceAfter.PromptTokens != 0 || evidenceAfter.CompletionTokens != 0 ||
		evidenceAfter.TotalTokens != 0 || evidenceAfter.CacheCreationTokens != 0 || evidenceAfter.CacheReadTokens != 0 ||
		evidenceAfter.UpstreamModelName != "attempt-model" {
		t.Fatalf("dispatch evidence accepted provider usage metrics: updates=%+v after=%+v", evidenceUpdates, evidenceAfter)
	}

	request := GatewayRequestLog{RecordKind: GatewayRequestRecordKindRequest}
	requestAfter := request
	requestUpdates := map[string]any{}
	applyGatewayRequestUsageMetricsUpdate(&request, &requestAfter, requestUpdates, usage)
	if requestAfter.PromptTokens != 9 || requestAfter.CompletionTokens != 8 || requestAfter.TotalTokens != 17 ||
		requestAfter.CacheCreationTokens != 7 || requestAfter.CacheReadTokens != 6 || requestAfter.UpstreamModelName != "provider-model" ||
		requestAfter.UsageInputPresent || requestAfter.UsageOutputPresent || requestAfter.UsageCacheReadPresent || requestAfter.UsageCacheWritePresent ||
		len(requestUpdates) != 6 {
		t.Fatalf("ordinary request lost provider usage metrics: updates=%+v after=%+v", requestUpdates, requestAfter)
	}

	v1Request := GatewayRequestLog{
		RecordKind:     GatewayRequestRecordKindRequest,
		PriceOperation: GatewayPricingOperationTextGeneration, PriceProtocol: GatewayPricingProtocolOpenAIResponses,
		UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageSchemaVersion: 1,
	}
	v1After := v1Request
	v1Updates := map[string]any{}
	applyGatewayRequestUsageMetricsUpdate(&v1Request, &v1After, v1Updates, usage)
	if !v1After.UsageInputPresent || !v1After.UsageOutputPresent || !v1After.UsageCacheReadPresent || !v1After.UsageCacheWritePresent ||
		len(v1Updates) != 10 {
		t.Fatalf("v1 request lost explicit provider usage presence: updates=%+v after=%+v", v1Updates, v1After)
	}
}

func TestDispatchCostEvidenceReconcileKeepsUsageMetricsHidden(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	reconcileAt := now.Add(6 * time.Second)
	if err := db.Create(&Channel{
		ID: 72, Name: "evidence channel", Type: ChannelTypeNewAPI,
		SiteURL: "https://evidence-channel.example", PasswordCipher: "test",
	}).Error; err != nil {
		t.Fatalf("create evidence channel: %v", err)
	}
	if err := db.Create(&ChannelCostState{
		ChannelID: 72, FundingFactor: 0.8, Version: 7, Source: "recharge_history", Status: "ready",
	}).Error; err != nil {
		t.Fatalf("create evidence funding snapshot: %v", err)
	}
	log := &GatewayRequestLog{
		RecordKind:          GatewayRequestRecordKindDispatchCostEvidence,
		DownstreamKeyID:     71,
		ChannelID:           72,
		ChannelNameSnapshot: "evidence channel",
		GroupName:           "evidence group",
		ModelName:           "gpt-5.6",
		UpstreamModelName:   "attempt-model",
		Method:              "POST",
		Path:                "/v1/responses",
		StatusCode:          502,
		FirstByteMs:         31_000,
		LatencyMs:           32_000,
		PromptTokens:        100,
		CompletionTokens:    20,
		CacheCreationTokens: 3,
		CacheReadTokens:     7,
		TotalTokens:         120,
		Error:               "ordinary failure must stay hidden",
		UpstreamRequestID:   "provider-evidence-reconcile",
		CostReconcileAt:     &reconcileAt,
		CreatedAt:           now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append dispatch evidence: %v", err)
	}
	if log.StatusCode != 0 || log.FirstByteMs != 0 || log.LatencyMs != 0 || log.PromptTokens != 0 ||
		log.CompletionTokens != 0 || log.CacheCreationTokens != 0 || log.CacheReadTokens != 0 ||
		log.TotalTokens != 0 || log.Error != "" {
		t.Fatalf("dispatch evidence retained ordinary request metrics: %+v", log)
	}
	if log.FundingFactor == nil || math.Abs(*log.FundingFactor-0.8) > 1e-12 {
		t.Fatalf("dispatch evidence did not freeze its funding factor: %+v", log)
	}

	if err := store.UpdateRequestLogActualCostWithUsage(log.ID, "usage-evidence-reconcile", 0.25, 0.2, gatewayCostSourceSub2APIActualCost, GatewayRequestUsageMetrics{
		PromptTokens:         900,
		CompletionTokens:     800,
		TotalTokens:          1700,
		CacheCreationTokens:  70,
		CacheReadTokens:      60,
		UpstreamModelName:    "provider-model",
		HasTokenMetrics:      true,
		HasCacheTokenMetrics: true,
	}); err != nil {
		t.Fatalf("reconcile dispatch evidence: %v", err)
	}

	var got GatewayRequestLog
	if err := db.First(&got, log.ID).Error; err != nil {
		t.Fatalf("read reconciled dispatch evidence: %v", err)
	}
	if got.RecordKind != GatewayRequestRecordKindDispatchCostEvidence || got.CostStatus != GatewayCostStatusReconciled ||
		got.CostSource != gatewayCostSourceSub2APIActualCost || got.UpstreamUsageID == nil || *got.UpstreamUsageID != "usage-evidence-reconcile" ||
		math.Abs(got.Cost-0.2) > 1e-12 || got.RawCost == nil || math.Abs(*got.RawCost-0.25) > 1e-12 {
		t.Fatalf("reconciled dispatch evidence cost facts = %+v", got)
	}
	if got.PromptTokens != 0 || got.CompletionTokens != 0 || got.TotalTokens != 0 ||
		got.CacheCreationTokens != 0 || got.CacheReadTokens != 0 || got.UpstreamModelName != "attempt-model" {
		t.Fatalf("provider usage metrics leaked into dispatch evidence: %+v", got)
	}

	var aggregates []GatewayRequestAggregate
	if err := db.Where("result = ?", gatewayRequestResultDispatchEvidence).Find(&aggregates).Error; err != nil {
		t.Fatalf("read dispatch evidence aggregate: %v", err)
	}
	if len(aggregates) != 1 {
		t.Fatalf("dispatch evidence aggregates = %d, want 1: %+v", len(aggregates), aggregates)
	}
	aggregate := aggregates[0]
	if aggregate.Requests != 0 || aggregate.TotalTokens != 0 || aggregate.TotalCost != 0 || aggregate.LatencySamples != 0 ||
		aggregate.PerformanceRequests != 0 || aggregate.DispatchEvidencePending != 0 ||
		math.Abs(aggregate.DispatchEvidenceActual-0.2) > 1e-12 || math.Abs(aggregate.DispatchEvidenceReconciled-0.2) > 1e-12 {
		t.Fatalf("dispatch evidence aggregate leaked ordinary metrics: %+v", aggregate)
	}

	summary, err := store.requestLogSummaryFilteredAtRaw(GatewayRequestLogFilter{}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("raw evidence summary: %v", err)
	}
	if summary.TotalRequests != 0 || summary.TotalTokens != 0 || summary.TotalCost != 0 || summary.FirstRequestAt != nil || summary.LastRequestAt != nil ||
		math.Abs(summary.DispatchEvidenceActual-0.2) > 1e-12 || math.Abs(summary.DispatchEvidenceReconciled-0.2) > 1e-12 {
		t.Fatalf("raw evidence summary = %+v", summary)
	}
	filtered, err := store.requestLogSummaryFilteredAtRaw(GatewayRequestLogFilter{Result: GatewayRequestResultFailed}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("filtered evidence summary: %v", err)
	}
	if filtered.TotalRequests != 0 || filtered.DispatchEvidenceActual != 0 || filtered.DispatchEvidenceReconciled != 0 {
		t.Fatalf("result filter included dispatch evidence: %+v", filtered)
	}

	trend, err := store.requestLogTrend(GatewayRequestLogFilter{})
	if err != nil {
		t.Fatalf("raw evidence trend: %v", err)
	}
	if len(trend) != 1 || trend[0].Requests != 0 || math.Abs(trend[0].DispatchEvidenceActual-0.2) > 1e-12 ||
		math.Abs(trend[0].DispatchEvidenceReconciled-0.2) > 1e-12 {
		t.Fatalf("raw evidence trend = %+v", trend)
	}
	analytics, err := store.requestLogAnalyticsRaw(GatewayRequestLogFilter{}, 8)
	if err != nil {
		t.Fatalf("raw evidence analytics: %v", err)
	}
	if len(analytics.Channels) != 0 || len(analytics.Models) != 0 || len(analytics.Keys) != 0 ||
		len(analytics.CostSources) != 0 || len(analytics.RouteReasons) != 0 {
		t.Fatalf("evidence-only dimensions leaked into breakdowns: %+v", analytics)
	}
	items, err := store.ListRequestLogsFiltered(GatewayRequestLogFilter{}, 10)
	if err != nil {
		t.Fatalf("list request logs: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("dispatch evidence leaked into request list: %+v", items)
	}

	nextState := &ChannelCostState{
		ChannelID: log.ChannelID, FundingFactor: 2, Source: "funding_correction", Status: "ready", CostUnitEpoch: 1,
	}
	fundingEvent := &ChannelFundingEvent{
		ChannelID: log.ChannelID, Kind: "funding_correction",
		FundingFactorBefore: 0.8, FundingFactorAfter: 2, EffectiveAt: now.Add(-time.Second),
	}
	if err := NewCosting(db).AppendFundingEvent(nextState, fundingEvent, 7); err != nil {
		t.Fatalf("apply evidence funding correction and invalidate aggregates: %v", err)
	}
	var invalidated GatewayRequestLog
	if err := db.First(&invalidated, log.ID).Error; err != nil {
		t.Fatalf("read invalidated evidence: %v", err)
	}
	if nextState.Version != 8 || invalidated.AggregateVersion != 0 || invalidated.FundingFactor == nil || math.Abs(*invalidated.FundingFactor-2) > 1e-12 ||
		invalidated.RawCost == nil || math.Abs(*invalidated.RawCost-0.25) > 1e-12 || math.Abs(invalidated.Cost-0.5) > 1e-12 ||
		invalidated.CostBasisVersion != 8 || invalidated.CostBasisSource != "funding_correction" {
		t.Fatalf("funding-corrected evidence facts = %+v", invalidated)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 10, 0); err != nil {
		t.Fatalf("rebuild funding-corrected evidence aggregate: %v", err)
	}
	aggregates = nil
	if err := db.Where("result = ?", gatewayRequestResultDispatchEvidence).Find(&aggregates).Error; err != nil {
		t.Fatalf("read rebuilt evidence aggregate: %v", err)
	}
	if len(aggregates) != 1 || aggregates[0].Version != gatewayRequestAggregateVersion || aggregates[0].Requests != 0 ||
		aggregates[0].TotalCost != 0 || math.Abs(aggregates[0].DispatchEvidenceActual-0.5) > 1e-12 ||
		math.Abs(aggregates[0].DispatchEvidenceReconciled-0.5) > 1e-12 {
		t.Fatalf("rebuilt funding-corrected evidence aggregate = %+v", aggregates)
	}
}
