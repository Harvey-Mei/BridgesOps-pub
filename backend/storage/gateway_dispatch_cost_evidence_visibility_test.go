package storage

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestRequestLogVisibilityScopesKeepEvidenceOutOfRawRows(t *testing.T) {
	sqlDB, err := sql.Open("pgx", "host=127.0.0.1 user=bridgeops database=bridgeops sslmode=disable")
	if err != nil {
		t.Fatalf("open lazy PostgreSQL connection: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB}), &gorm.Config{
		DisableAutomaticPing: true,
		DryRun:               true,
	})
	if err != nil {
		t.Fatalf("open dry-run database: %v", err)
	}
	store := NewGateway(db)

	visible := store.applyRequestLogFilter(db.Model(&GatewayRequestLog{}), "", GatewayRequestLogFilter{}).
		Find(&[]GatewayRequestLog{}).Statement.SQL.String()
	visible = strings.ToLower(visible)
	if !strings.Contains(visible, "record_kind") || !strings.Contains(visible, "= 'request'") ||
		strings.Contains(visible, "dispatch_cost_evidence") {
		t.Fatalf("visible request-log scope did not fail closed to normal requests: %s", visible)
	}

	ledger := store.applyRequestLedgerFilter(db.Model(&GatewayRequestLog{}), "", GatewayRequestLogFilter{}).
		Find(&[]GatewayRequestLog{}).Statement.SQL.String()
	ledger = strings.ToLower(ledger)
	if !strings.Contains(ledger, "= 'request'") || !strings.Contains(ledger, "dispatch_cost_evidence") {
		t.Fatalf("internal ledger scope did not keep the two record domains explicit: %s", ledger)
	}

	failedLedger := store.applyRequestLedgerFilter(db.Model(&GatewayRequestLog{}), "", GatewayRequestLogFilter{Result: GatewayRequestResultFailed}).
		Find(&[]GatewayRequestLog{}).Statement.SQL.String()
	failedLedger = strings.ToLower(failedLedger)
	if !strings.Contains(failedLedger, "dispatch_cost_evidence") ||
		strings.Count(failedLedger, "= 'request'") < 2 {
		t.Fatalf("failed-result ledger filter could classify evidence as a request failure: %s", failedLedger)
	}
}

func TestDispatchCostEvidenceNormalizationAndAggregateIsolation(t *testing.T) {
	rawCost := 8.5
	usageID := "untrusted-local-usage"
	reconcileAt := time.Now().UTC()
	log := &GatewayRequestLog{
		RecordKind:         GatewayRequestRecordKindDispatchCostEvidence,
		DownstreamKeyID:    7,
		ChannelID:          9,
		ModelName:          "hidden-evidence-model",
		Method:             "POST",
		Path:               "/v1/responses",
		StatusCode:         502,
		PromptTokens:       100,
		CompletionTokens:   20,
		TotalTokens:        120,
		Cost:               8.5,
		RawCost:            &rawCost,
		CostEstimated:      true,
		CostSource:         "gateway_estimate",
		CostStatus:         GatewayCostStatusEstimated,
		UpstreamRequestID:  "provider-request-id",
		UpstreamUsageID:    &usageID,
		CostReconcileAt:    &reconcileAt,
		CostReconcileTries: 3,
		CreatedAt:          time.Now().UTC(),
	}

	if err := normalizeGatewayRequestRecord(log); err != nil {
		t.Fatalf("normalize dispatch evidence: %v", err)
	}
	if log.Cost != 0 || log.RawCost != nil || log.CostEstimated || log.CostSource != "" ||
		log.CostStatus != GatewayCostStatusNone || log.UpstreamUsageID != nil {
		t.Fatalf("dispatch evidence retained local monetary claims: %+v", log)
	}
	if IsSuccessfulGatewayRequest(log) || IsGatewayRequestCostBillable(log) {
		t.Fatalf("dispatch evidence entered normal request/billing predicates: %+v", log)
	}
	if !IsGatewayRequestCostReconcileEligible(log) {
		t.Fatal("exact provider-ID evidence was not eligible for reconciliation")
	}
	if _, ok := requestGroupCacheAggregateFromLog(log); ok {
		t.Fatal("dispatch evidence entered the cache-ratio ledger")
	}
	row, ok := requestAggregateFromLog(log)
	if !ok {
		t.Fatal("dispatch evidence was not represented in its isolated aggregate ledger")
	}
	if row.Requests != 0 || row.TotalTokens != 0 || row.TotalCost != 0 || row.EstimatedCost != 0 ||
		row.ActualCost != 0 || row.PerformanceRequests != 0 || row.LatencySamples != 0 ||
		row.DispatchEvidencePending != 1 {
		t.Fatalf("dispatch evidence aggregate was not isolated: %+v", row)
	}
}

func TestAggregateBreakdownEdgeMergePreservesDispatchEvidence(t *testing.T) {
	metrics := aggregateMetricsFromBreakdown(GatewayRequestLogBreakdownItem{
		Requests:                   2,
		DispatchEvidencePending:    3,
		DispatchEvidenceActual:     0.45,
		DispatchEvidenceReported:   0.15,
		DispatchEvidenceReconciled: 0.30,
	})
	if metrics.requests != 2 || metrics.dispatchEvidencePending != 3 ||
		metrics.dispatchEvidenceActual != 0.45 || metrics.dispatchEvidenceReported != 0.15 ||
		metrics.dispatchEvidenceReconciled != 0.30 {
		t.Fatalf("partial-edge breakdown lost dispatch evidence: %+v", metrics)
	}
}

func TestAggregateEdgeBreakdownsMergeEvidenceAfterAllRequests(t *testing.T) {
	channelMap := map[gatewayRequestAggregateChannelKey]*aggregateMetrics{
		requestAggregateChannelKey(1, "interior"): {},
	}
	modelMap := map[string]*aggregateMetrics{"interior": {}}
	keyMap := map[uint]*aggregateMetrics{1: {}}
	costSourceMap := map[string]*aggregateMetrics{"interior": {}}
	routeReasonMap := map[string]*aggregateMetrics{"interior": {}}
	evidence := func(name string, id uint, pending int64) GatewayRequestLogBreakdownItem {
		return GatewayRequestLogBreakdownItem{
			Name: name, ChannelID: id, DownstreamKeyID: id,
			DispatchEvidencePending: pending, DispatchEvidenceActual: float64(pending) / 10,
		}
	}
	normal := func(name string, id uint) GatewayRequestLogBreakdownItem {
		return GatewayRequestLogBreakdownItem{Name: name, ChannelID: id, DownstreamKeyID: id, Requests: 1}
	}
	edges := []*GatewayRequestLogAnalytics{
		{
			Channels:     []GatewayRequestLogBreakdownItem{evidence("interior", 1, 1), evidence("cross-edge", 2, 2), evidence("hidden", 3, 3)},
			Models:       []GatewayRequestLogBreakdownItem{evidence("interior", 1, 1), evidence("cross-edge", 2, 2), evidence("hidden", 3, 3)},
			Keys:         []GatewayRequestLogBreakdownItem{evidence("interior", 1, 1), evidence("cross-edge", 2, 2), evidence("hidden", 3, 3)},
			CostSources:  []GatewayRequestLogBreakdownItem{evidence("interior", 1, 1), evidence("cross-edge", 2, 2), evidence("hidden", 3, 3)},
			RouteReasons: []GatewayRequestLogBreakdownItem{evidence("interior", 1, 1), evidence("cross-edge", 2, 2), evidence("hidden", 3, 3)},
		},
		{
			Channels:     []GatewayRequestLogBreakdownItem{normal("cross-edge", 2)},
			Models:       []GatewayRequestLogBreakdownItem{normal("cross-edge", 2)},
			Keys:         []GatewayRequestLogBreakdownItem{normal("cross-edge", 2)},
			CostSources:  []GatewayRequestLogBreakdownItem{normal("cross-edge", 2)},
			RouteReasons: []GatewayRequestLogBreakdownItem{normal("cross-edge", 2)},
		},
	}
	if err := mergeRequestAggregateEdgeBreakdowns(edges, channelMap, modelMap, keyMap, costSourceMap, routeReasonMap); err != nil {
		t.Fatalf("merge partial-edge evidence: %v", err)
	}
	assertMetrics := func(label string, interior, cross *aggregateMetrics, hidden bool) {
		t.Helper()
		if interior == nil || interior.dispatchEvidencePending != 1 || interior.dispatchEvidenceActual != 0.1 {
			t.Fatalf("%s interior evidence = %+v", label, interior)
		}
		if cross == nil || cross.requests != 1 || cross.dispatchEvidencePending != 2 || cross.dispatchEvidenceActual != 0.2 {
			t.Fatalf("%s cross-edge evidence = %+v", label, cross)
		}
		if hidden {
			t.Fatalf("%s evidence-only dimension became visible", label)
		}
	}
	assertMetrics("channel", channelMap[requestAggregateChannelKey(1, "interior")], channelMap[requestAggregateChannelKey(2, "cross-edge")], channelMap[requestAggregateChannelKey(3, "hidden")] != nil)
	assertMetrics("model", modelMap["interior"], modelMap["cross-edge"], modelMap["hidden"] != nil)
	assertMetrics("key", keyMap[1], keyMap[2], keyMap[3] != nil)
	assertMetrics("cost source", costSourceMap["interior"], costSourceMap["cross-edge"], costSourceMap["hidden"] != nil)
	assertMetrics("route reason", routeReasonMap["interior"], routeReasonMap["cross-edge"], routeReasonMap["hidden"] != nil)
}

func TestEvidenceOnlyAggregateRowsStayOutOfUsageViews(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	normalAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	evidenceAt := normalAt.Add(6 * time.Hour)
	rows := []GatewayRequestAggregate{
		{
			BucketStart: normalAt, DownstreamKeyID: 1, ChannelID: 1,
			ModelName: "visible-model", CostSource: "reported", RouteReason: "smart_primary",
			Result: GatewayRequestResultSuccess, Requests: 1, TotalTokens: 3, TotalCost: 0.1,
			ReportedCost: 0.1, ActualCost: 0.1, FirstRequestAt: normalAt, LastRequestAt: normalAt,
			Version: gatewayRequestAggregateVersion,
		},
		{
			BucketStart: evidenceAt, DownstreamKeyID: 2, ChannelID: 2,
			ModelName: "hidden-evidence-model", CostSource: "", RouteReason: "smart_primary",
			Result: gatewayRequestResultDispatchEvidence, DispatchEvidencePending: 1,
			FirstRequestAt: evidenceAt, LastRequestAt: evidenceAt, Version: gatewayRequestAggregateVersion,
		},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("create aggregate fixtures: %v", err)
	}

	summary, used, err := store.requestLogSummaryFromAggregates(GatewayRequestLogFilter{}, evidenceAt.Add(time.Hour))
	if err != nil || !used {
		t.Fatalf("aggregate summary used=%t err=%v", used, err)
	}
	if summary.TotalRequests != 1 || summary.TotalTokens != 3 || summary.FirstRequestAt == nil ||
		summary.LastRequestAt == nil || !summary.FirstRequestAt.Equal(normalAt) || !summary.LastRequestAt.Equal(normalAt) ||
		summary.DispatchEvidencePending != 1 || summary.DispatchEvidenceActual != 0 {
		t.Fatalf("hidden evidence polluted request summary: %+v", summary)
	}

	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 8)
	if err != nil {
		t.Fatalf("aggregate analytics: %v", err)
	}
	if len(analytics.Models) != 1 || analytics.Models[0].Name != "visible-model" ||
		len(analytics.Channels) != 1 || analytics.Channels[0].ChannelID != 1 ||
		len(analytics.Keys) != 1 || analytics.Keys[0].DownstreamKeyID != 1 {
		t.Fatalf("hidden evidence leaked into aggregate breakdowns: %+v", analytics)
	}

	usage, used, err := store.summarizeDownstreamKeyUsageFromAggregates([]uint{1, 2}, evidenceAt)
	if err != nil || !used {
		t.Fatalf("aggregate downstream usage used=%t err=%v", used, err)
	}
	if usage[1].TotalRequests != 1 {
		t.Fatalf("visible downstream usage = %+v", usage[1])
	}
	if _, exists := usage[2]; exists {
		t.Fatalf("evidence-only downstream key leaked into usage: %+v", usage[2])
	}
}
