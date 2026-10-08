package storage

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestDashboardMultiplierTrendUsesFrozenDispatchFacts(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(4 * time.Hour)
	routeSnapshot, err := NewGatewayRouteExclusionSnapshot(start, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("route exclusion snapshot: %v", err)
	}

	logs := []GatewayRequestLog{
		// The model says Claude, but the persisted dispatch app is Codex. This
		// proves the series does not classify history from a model name.
		{
			DownstreamKeyID: 1, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "claude-looking-model",
			GroupRatio: 2, CompletionRatio: 4, PromptTokens: 100, CacheCreationTokens: 10, CacheReadTokens: 20, CompletionTokens: 30,
			Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(30 * time.Minute),
		},
		// A frozen 0x output rate is valid (for example, a free-credit grant):
		// its tokens remain covered and must not inherit the input multiplier.
		{
			DownstreamKeyID: 2, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
			GroupRatio: 4, CompletionRatio: 0, PromptTokens: 10, CompletionTokens: 10,
			Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
		},
		// Input has its own valid 0x multiplier. It exercises the independent
		// input coverage and keeps the following bucket's smoothing contiguous.
		{
			DownstreamKeyID: 10, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
			GroupRatio: 0, CompletionRatio: 1, PromptTokens: 10, CompletionTokens: 0,
			Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(90 * time.Minute),
		},
		{
			DownstreamKeyID: 3, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "other-model",
			GroupRatio: 6, CompletionRatio: 8, PromptTokens: 20, CompletionTokens: 20,
			Method: "POST", Path: "/v1/messages", StatusCode: 200, CreatedAt: start.Add(150 * time.Minute),
		},
		// This row is frozen as Claude even though its model looks like Codex.
		{
			DownstreamKeyID: 4, ChannelID: 1, GatewayApp: string(GatewayAppClaude), ModelName: "gpt-looking-model",
			GroupRatio: 1.5, CompletionRatio: 2, PromptTokens: 100, CompletionTokens: 50,
			Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, CreatedAt: start.Add(30 * time.Minute),
		},
		// Neither an image operation nor missing/invalid frozen app facts may be
		// inferred from model names or affect the two user data-plane series.
		{
			DownstreamKeyID: 5, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-image",
			GroupRatio: 999, CompletionRatio: 999, PromptTokens: 999, CompletionTokens: 999,
			Method: "POST", Path: "/v1/images/generations", StatusCode: 200, CreatedAt: start.Add(30 * time.Minute),
		},
		{
			DownstreamKeyID: 6, ChannelID: 1, GatewayApp: "", ModelName: "gpt-5.4",
			GroupRatio: 999, CompletionRatio: 999, PromptTokens: 999, CompletionTokens: 999,
			Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(30 * time.Minute),
		},
	}
	for _, reason := range []string{gatewayRouteReasonLatencyProbe, gatewayRouteReasonKeyTest, "model_check", "internal", "internal_reconcile"} {
		logs = append(logs, GatewayRequestLog{
			DownstreamKeyID: 7, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4", RouteReason: reason,
			GroupRatio: 999, CompletionRatio: 999, PromptTokens: 999, CompletionTokens: 999,
			Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(30 * time.Minute),
		})
	}
	logs = append(logs, GatewayRequestLog{
		DownstreamKeyID: 8, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
		GroupRatio: 999, CompletionRatio: 999, PromptTokens: 999, CompletionTokens: 999,
		Method: "POST", Path: "/v1/responses", StatusCode: 500, Error: "upstream failure", CreatedAt: start.Add(30 * time.Minute),
	})
	// A 2xx canceled record is intentionally present: canonical successful-user
	// filtering must reject it even though a simplistic status-only query would
	// include it.
	logs = append(logs, GatewayRequestLog{
		DownstreamKeyID: 9, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4", RouteReason: gatewayRouteReasonClientCanceled,
		GroupRatio: 999, CompletionRatio: 999, PromptTokens: 999, CompletionTokens: 999,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(30 * time.Minute),
	})
	for index := range logs {
		appendDashboardMultiplierTrendLog(t, store, logs[index], routeSnapshot, index+1)
	}

	trend, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("dashboard multiplier trend: %v", err)
	}
	if trend.BucketSeconds != int64((2*time.Hour)/time.Second) || trend.SmoothingBuckets != 3 {
		t.Fatalf("trend shape = %+v", trend)
	}
	codex := dashboardMultiplierTrendAppForTest(t, trend, GatewayAppCodex)
	claude := dashboardMultiplierTrendAppForTest(t, trend, GatewayAppClaude)
	if len(codex.Points) != 2 || len(claude.Points) != 2 {
		t.Fatalf("bucket count codex=%d claude=%d, want 2", len(codex.Points), len(claude.Points))
	}

	first := codex.Points[0]
	if first.SuccessfulRequests != 3 || first.InputTokens != 90 || first.InputCoveredTokens != 90 || first.OutputTokens != 40 || first.OutputCoveredTokens != 40 {
		t.Fatalf("first codex coverage = %+v", first)
	}
	assertDashboardMultiplierNear(t, first.InputMultiplier, 2)
	assertDashboardMultiplierNear(t, first.OutputMultiplier, 3)
	assertDashboardMultiplierNear(t, first.SmoothedInputMultiplier, 2)
	assertDashboardMultiplierNear(t, first.SmoothedOutputMultiplier, 3)

	second := codex.Points[1]
	if second.SuccessfulRequests != 1 || second.InputTokens != 20 || second.InputCoveredTokens != 20 || second.OutputTokens != 20 || second.OutputCoveredTokens != 20 {
		t.Fatalf("second codex coverage = %+v", second)
	}
	assertDashboardMultiplierNear(t, second.InputMultiplier, 6)
	assertDashboardMultiplierNear(t, second.OutputMultiplier, 8)
	assertDashboardMultiplierNear(t, second.SmoothedInputMultiplier, 300.0/110.0)
	assertDashboardMultiplierNear(t, second.SmoothedOutputMultiplier, 280.0/60.0)
	if second.SmoothedInputCoveredTokens != 110 || second.SmoothedOutputCoveredTokens != 60 {
		t.Fatalf("second smoothing coverage = %+v", second)
	}

	if claude.Points[0].SuccessfulRequests != 1 || claude.Points[0].InputTokens != 100 || claude.Points[0].OutputTokens != 50 {
		t.Fatalf("claude was not classified from frozen app: %+v", claude.Points[0])
	}
	assertDashboardMultiplierNear(t, claude.Points[0].InputMultiplier, 1.5)
	assertDashboardMultiplierNear(t, claude.Points[0].OutputMultiplier, 2)
}

func TestDashboardMultiplierTrendIgnoresFundingCorrectedRequestLogRatios(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	routeSnapshot, err := NewGatewayRouteExclusionSnapshot(start, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("route exclusion snapshot: %v", err)
	}
	audit := appendDashboardMultiplierTrendLog(t, store, GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
		GroupRatio: 2, CompletionRatio: 4, PromptTokens: 100, CompletionTokens: 50,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
	}, routeSnapshot, 1)
	if audit == nil || audit.RequestLogID == nil {
		t.Fatal("missing final dispatch audit fixture")
	}

	before, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("trend before correction: %v", err)
	}
	beforePoint := dashboardMultiplierTrendAppForTest(t, before, GatewayAppCodex).Points[0]
	assertDashboardMultiplierNear(t, beforePoint.InputMultiplier, 2)
	assertDashboardMultiplierNear(t, beforePoint.OutputMultiplier, 4)

	// Funding reconciliation intentionally changes the mutable request-log
	// accounting projection. It must never rewrite the dispatched multiplier
	// shown in an already-completed overview window.
	if err := db.Model(&GatewayRequestLog{}).Where("id = ?", *audit.RequestLogID).Updates(map[string]any{
		"group_ratio":       17.0,
		"completion_ratio":  19.0,
		"funding_factor":    3.0,
		"cost_basis_source": "funding_correction",
	}).Error; err != nil {
		t.Fatalf("apply funding correction projection: %v", err)
	}

	after, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("trend after correction: %v", err)
	}
	afterPoint := dashboardMultiplierTrendAppForTest(t, after, GatewayAppCodex).Points[0]
	assertDashboardMultiplierNear(t, afterPoint.InputMultiplier, 2)
	assertDashboardMultiplierNear(t, afterPoint.OutputMultiplier, 4)
}

func TestDashboardMultiplierTrendFailsClosedWithoutExactFinalPhysicalAuditFacts(t *testing.T) {
	db := openTestDB(t)
	// 000045 enforces complete pricing identity on every new audit.  This test
	// deliberately exercises damaged rows that can only predate that NOT VALID
	// constraint, so remove just that write-time guard in this disposable clone
	// before constructing the legacy fixtures.  The production read must still
	// fail closed when such rows remain in an upgraded database.
	if err := db.Exec("ALTER TABLE gateway_request_dispatch_audits DROP CONSTRAINT chk_gateway_request_dispatch_price_fact_shape").Error; err != nil {
		t.Fatalf("allow legacy incomplete pricing audit fixtures: %v", err)
	}
	if err := db.Exec("DROP TRIGGER trg_gateway_request_dispatch_final_price_snapshot_consistency ON gateway_request_dispatch_audits").Error; err != nil {
		t.Fatalf("allow legacy final price mismatch fixtures: %v", err)
	}
	store := NewGateway(db)
	start := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	routeSnapshot, err := NewGatewayRouteExclusionSnapshot(start, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("route exclusion snapshot: %v", err)
	}
	newLog := func(sequence int) GatewayRequestLog {
		return GatewayRequestLog{
			DownstreamKeyID: uint(sequence), ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
			GroupRatio: 2, CompletionRatio: 4, PromptTokens: 100, CompletionTokens: 50,
			Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
		}
	}

	appendDashboardMultiplierTrendLog(t, store, newLog(1), routeSnapshot, 1, func(audit *GatewayRequestDispatchAudit) {
		audit.ChannelIdentityRevision = 0
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(2), routeSnapshot, 2, func(audit *GatewayRequestDispatchAudit) {
		audit.UpstreamGroupID = 0
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(3), routeSnapshot, 3, func(audit *GatewayRequestDispatchAudit) {
		audit.UpstreamGroupGeneration = 0
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(4), routeSnapshot, 4, func(audit *GatewayRequestDispatchAudit) {
		audit.UpstreamKeyLocalID = 0
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(5), routeSnapshot, 5, func(audit *GatewayRequestDispatchAudit) {
		audit.UpstreamKeyGeneration = 0
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(6), routeSnapshot, 6, func(audit *GatewayRequestDispatchAudit) {
		audit.PhysicallyDispatched = false
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(7), routeSnapshot, 7, func(audit *GatewayRequestDispatchAudit) {
		audit.LogicalRequestTraceID = fmt.Sprintf("%032x", 99)
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(8), routeSnapshot, 8, func(audit *GatewayRequestDispatchAudit) {
		audit.UpstreamGroupID = 2 // complete but not the linked log's identity
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(9), routeSnapshot, 9, func(audit *GatewayRequestDispatchAudit) {
		audit.TerminalKind = "failed_before_acceptance"
	})
	appendDashboardMultiplierTrendLog(t, store, newLog(10), routeSnapshot, 10, func(audit *GatewayRequestDispatchAudit) {
		audit.Stage = GatewayRequestDispatchStageCompleted
		audit.RequestLogID = nil
	})
	// A legacy request has no v1 trace/app facts and therefore cannot be
	// safely attributed to either overview series.
	if err := store.AppendRequestLog(&GatewayRequestLog{
		DownstreamKeyID: 8, ChannelID: 1, PromptTokens: 100, CompletionTokens: 50,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
	}); err != nil {
		t.Fatalf("append legacy request log: %v", err)
	}

	// The failed audit has the target trace but belongs to another request log.
	// A trace-only join would falsely attribute it to the successful target.
	target := newLog(11)
	target.TelemetryVersion = GatewayRequestTelemetryVersion
	target.LogicalRequestTraceID = fmt.Sprintf("%032x", 99)
	target.RoutingMode = GatewayRoutingModeSmart
	target.RouteExclusionSnapshotVersion = GatewayRouteExclusionSnapshotVersion
	target.RouteExclusionsSnapshot = routeSnapshot
	target.ChannelIdentityRevision = 1
	target.UpstreamGroupID = 1
	target.UpstreamGroupGeneration = 1
	target.UpstreamKeyLocalID = 1
	target.UpstreamKeyGeneration = 1
	completeDashboardMultiplierPricingFixture(&target)
	if err := store.AppendRequestLog(&target); err != nil {
		t.Fatalf("append unmatched target request log: %v", err)
	}
	shadow := newLog(12)
	shadow.StatusCode = 500
	appendDashboardMultiplierTrendLog(t, store, shadow, routeSnapshot, 12, func(audit *GatewayRequestDispatchAudit) {
		audit.LogicalRequestTraceID = target.LogicalRequestTraceID
	})

	trend, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("dashboard multiplier trend: %v", err)
	}
	point := dashboardMultiplierTrendAppForTest(t, trend, GatewayAppCodex).Points[0]
	if point.SuccessfulRequests != 11 || point.InputTokens != 1100 || point.OutputTokens != 550 || point.InputCoveredTokens != 0 || point.OutputCoveredTokens != 0 || point.InputMultiplier != nil || point.OutputMultiplier != nil {
		t.Fatalf("incomplete or non-exact audit facts did not remain an uncovered denominator: %+v", point)
	}
}

func TestDashboardMultiplierTrendHardFencedAuditsRemainUncovered(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	routeSnapshot, err := NewGatewayRouteExclusionSnapshot(start, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("route exclusion snapshot: %v", err)
	}

	for index, reason := range []string{GatewayPricingUnavailableScopeIncomplete, GatewayPricingUnavailableIdentityChanged} {
		audit := appendDashboardMultiplierTrendLog(t, store, GatewayRequestLog{
			DownstreamKeyID: uint(index + 1), ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
			// Default-zero values are valid only when they have complete,
			// unfenced provenance. These audits intentionally retain complete
			// IDs to model a damaged historical row.
			GroupRatio: 0, CompletionRatio: 0, PromptTokens: 100, CompletionTokens: 50,
			Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
		}, routeSnapshot, index+1, func(audit *GatewayRequestDispatchAudit) {
			audit.PricingUnavailableReason = reason
		})
		if audit == nil || audit.ChannelIdentityRevision == 0 || audit.UpstreamGroupID == 0 ||
			audit.UpstreamGroupGeneration == 0 || audit.UpstreamKeyLocalID == 0 || audit.UpstreamKeyGeneration == 0 {
			t.Fatalf("hard-fence fixture lost complete IDs: %+v", audit)
		}
	}

	// A genuine frozen free-credit fact remains covered. This distinguishes it
	// from the same numeric multiplier on an explicitly hard-fenced audit.
	control := appendDashboardMultiplierTrendLog(t, store, GatewayRequestLog{
		DownstreamKeyID: 3, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
		GroupRatio: 0, CompletionRatio: 0, PromptTokens: 100, CompletionTokens: 50,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
	}, routeSnapshot, 3)
	if control == nil {
		t.Fatal("missing unfenced zero-multiplier control")
	}

	trend, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("dashboard multiplier trend: %v", err)
	}
	point := dashboardMultiplierTrendAppForTest(t, trend, GatewayAppCodex).Points[0]
	if point.SuccessfulRequests != 3 || point.InputTokens != 300 || point.OutputTokens != 150 ||
		point.InputCoveredTokens != 100 || point.OutputCoveredTokens != 50 ||
		point.InputSampleRequests != 1 || point.OutputSampleRequests != 1 {
		t.Fatalf("hard-fenced audits covered tokens or disappeared from the denominator: %+v", point)
	}
	assertDashboardMultiplierNear(t, point.InputMultiplier, 0)
	assertDashboardMultiplierNear(t, point.OutputMultiplier, 0)
}

func TestDashboardMultiplierTrendFinalDispatchAuditJoinExplicitlyExcludesHardFences(t *testing.T) {
	join := dashboardMultiplierTrendFinalDispatchAuditJoinSQL()
	for _, required := range []string{
		"dispatch_audit.cost_unavailable_reason",
		GatewayPricingUnavailableScopeIncomplete,
		GatewayPricingUnavailableIdentityChanged,
	} {
		if !strings.Contains(join, required) {
			t.Fatalf("final dispatch audit join missing %q: %s", required, join)
		}
	}
}

func TestDashboardMultiplierTrendRejectsFullyPopulatedLegacyProvenanceVersion(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	routeSnapshot, err := NewGatewayRouteExclusionSnapshot(start, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("route exclusion snapshot: %v", err)
	}
	audit := appendDashboardMultiplierTrendLog(t, store, GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
		GroupRatio: 2, CompletionRatio: 4, PromptTokens: 100, CompletionTokens: 50,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
	}, routeSnapshot, 1)
	if audit == nil {
		t.Fatal("missing complete audit fixture")
	}

	// The migration rejects newly-written v0 audits. Disable only its trigger
	// in this disposable cloned test database to model a complete-looking row
	// that existed before migration 000051. Such legacy data must still be a
	// visible coverage gap, not a guessed historical multiplier.
	if err := db.Exec("ALTER TABLE gateway_request_dispatch_audits DISABLE TRIGGER trg_gateway_request_dispatch_multiplier_provenance").Error; err != nil {
		t.Fatalf("disable provenance trigger for legacy fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Exec("ALTER TABLE gateway_request_dispatch_audits ENABLE TRIGGER trg_gateway_request_dispatch_multiplier_provenance").Error; err != nil {
			t.Errorf("restore provenance trigger: %v", err)
		}
	})
	if err := db.Exec("UPDATE gateway_request_dispatch_audits SET multiplier_provenance_version = 0 WHERE id = ?", audit.ID).Error; err != nil {
		t.Fatalf("downgrade complete audit to legacy provenance: %v", err)
	}

	trend, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("dashboard multiplier trend: %v", err)
	}
	point := dashboardMultiplierTrendAppForTest(t, trend, GatewayAppCodex).Points[0]
	if point.SuccessfulRequests != 1 || point.InputTokens != 100 || point.OutputTokens != 50 || point.InputCoveredTokens != 0 || point.OutputCoveredTokens != 0 || point.InputMultiplier != nil || point.OutputMultiplier != nil {
		t.Fatalf("legacy provenance did not remain an uncovered denominator: %+v", point)
	}
}

func TestDashboardMultiplierTrendKeepsV1KnownAppLogWithoutTraceUncovered(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	routeSnapshot, err := NewGatewayRouteExclusionSnapshot(start, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("route exclusion snapshot: %v", err)
	}
	legacyAudit := appendDashboardMultiplierTrendLog(t, store, GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
		GroupRatio: 2, CompletionRatio: 4, PromptTokens: 100, CompletionTokens: 50,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
	}, routeSnapshot, 1)
	missingTraceAudit := appendDashboardMultiplierTrendLog(t, store, GatewayRequestLog{
		DownstreamKeyID: 2, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
		GroupRatio: 2, CompletionRatio: 4, PromptTokens: 100, CompletionTokens: 50,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
	}, routeSnapshot, 2)
	if legacyAudit == nil || legacyAudit.RequestLogID == nil || missingTraceAudit == nil || missingTraceAudit.RequestLogID == nil {
		t.Fatal("missing telemetry regression fixtures")
	}

	// This isolated fixture represents a v0 legacy row plus a damaged v1 trace.
	// The v0 row has no trustworthy app classification and must stay out of the
	// two app series; the v1 row remains in its known-app denominator but cannot
	// receive audit coverage without an exact trace. The production constraint
	// is intentionally removed only in this cloned test database to construct
	// the historical shape.
	if err := db.Exec("ALTER TABLE gateway_request_logs DROP CONSTRAINT chk_gateway_request_logs_incident_telemetry").Error; err != nil {
		t.Fatalf("remove isolated-test telemetry constraint: %v", err)
	}
	if err := db.Exec("UPDATE gateway_request_logs SET telemetry_version = 0, logical_request_trace_id = '', routing_mode = '' WHERE id = ?", *legacyAudit.RequestLogID).Error; err != nil {
		t.Fatalf("create legacy known-app request log: %v", err)
	}
	if err := db.Exec("UPDATE gateway_request_logs SET logical_request_trace_id = '' WHERE id = ?", *missingTraceAudit.RequestLogID).Error; err != nil {
		t.Fatalf("create missing-trace request log: %v", err)
	}

	trend, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("dashboard multiplier trend: %v", err)
	}
	point := dashboardMultiplierTrendAppForTest(t, trend, GatewayAppCodex).Points[0]
	if point.SuccessfulRequests != 1 || point.InputTokens != 100 || point.OutputTokens != 50 || point.InputCoveredTokens != 0 || point.OutputCoveredTokens != 0 || point.InputMultiplier != nil || point.OutputMultiplier != nil {
		t.Fatalf("v1 missing-trace log did not remain the sole uncovered known-app denominator: %+v", point)
	}
}

func TestDashboardMultiplierTrendRejectsDuplicateAuditLinks(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	start := time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	routeSnapshot, err := NewGatewayRouteExclusionSnapshot(start, []GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("route exclusion snapshot: %v", err)
	}
	audit := appendDashboardMultiplierTrendLog(t, store, GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 1, GatewayApp: string(GatewayAppCodex), ModelName: "gpt-5.4",
		GroupRatio: 2, CompletionRatio: 4, PromptTokens: 100, CompletionTokens: 50,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: start.Add(time.Hour),
	}, routeSnapshot, 1)
	if audit == nil || audit.RequestLogID == nil {
		t.Fatal("missing complete audit fixture")
	}

	// Production has a unique constraint, but a defensive read must still not
	// double-count if an old manual repair or damaged restore contains two final
	// audit rows for one request log.
	if err := db.Exec("ALTER TABLE gateway_request_dispatch_audits DROP CONSTRAINT uq_gateway_request_dispatch_log").Error; err != nil {
		t.Fatalf("remove isolated-test uniqueness fence: %v", err)
	}
	duplicate := *audit
	duplicate.ID = 0
	duplicate.CreatedAt = duplicate.CreatedAt.Add(time.Nanosecond)
	duplicate.DispatchedAt = duplicate.DispatchedAt.Add(time.Nanosecond)
	persistDashboardMultiplierTrendAudit(t, store, &duplicate, 2)

	trend, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("dashboard multiplier trend: %v", err)
	}
	point := dashboardMultiplierTrendAppForTest(t, trend, GatewayAppCodex).Points[0]
	if point.SuccessfulRequests != 1 || point.InputTokens != 100 || point.OutputTokens != 50 || point.InputCoveredTokens != 0 || point.OutputCoveredTokens != 0 || point.InputMultiplier != nil || point.OutputMultiplier != nil {
		t.Fatalf("duplicate audit links did not remain an uncovered denominator: %+v", point)
	}
}

func TestDashboardMultiplierTrendResetsSmoothingAfterAnEmptyBucket(t *testing.T) {
	points := []DashboardMultiplierTrendPoint{
		{InputCoveredTokens: 100, inputWeightedMultiplierTotal: 200, OutputCoveredTokens: 20, outputWeightedMultiplierTotal: 100},
		{},
		{InputCoveredTokens: 100, inputWeightedMultiplierTotal: 500, OutputCoveredTokens: 20, outputWeightedMultiplierTotal: 40},
	}
	dashboardMultiplierTrendFinalizePoints(points)
	assertDashboardMultiplierNear(t, points[0].SmoothedInputMultiplier, 2)
	assertDashboardMultiplierNear(t, points[2].SmoothedInputMultiplier, 5)
	assertDashboardMultiplierNear(t, points[0].SmoothedOutputMultiplier, 5)
	assertDashboardMultiplierNear(t, points[2].SmoothedOutputMultiplier, 2)
	if points[2].SmoothedInputCoveredTokens != 100 || points[2].SmoothedOutputCoveredTokens != 20 {
		t.Fatalf("post-gap smoothing retained old coverage: %+v", points[2])
	}
	if points[1].SmoothedInputMultiplier != nil || points[1].SmoothedOutputMultiplier != nil {
		t.Fatalf("empty current bucket received carried-forward multiplier: %+v", points[1])
	}
}

func TestFiniteDashboardMultiplierBounds(t *testing.T) {
	for _, test := range []struct {
		name  string
		value float64
		want  bool
	}{
		{name: "zero is a valid free-credit multiplier", value: 0, want: true},
		{name: "maximum is valid", value: dashboardMultiplierTrendMaximumRatio, want: true},
		{name: "negative", value: -0.01, want: false},
		{name: "nan", value: math.NaN(), want: false},
		{name: "positive infinity", value: math.Inf(1), want: false},
		{name: "negative infinity", value: math.Inf(-1), want: false},
		{name: "over display bound", value: dashboardMultiplierTrendMaximumRatio + 1, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := finiteDashboardMultiplier(test.value); got != test.want {
				t.Fatalf("finiteDashboardMultiplier(%v) = %t, want %t", test.value, got, test.want)
			}
		})
	}
	if !finiteDashboardMultiplierTotal(dashboardMultiplierTrendMaximumRatio * 2) {
		t.Fatal("finite weighted total above display bound was rejected")
	}
}

func TestDashboardMultiplierTrendLocalDailyBucketsRespectDST(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	start := time.Date(2026, 3, 7, 23, 30, 0, 0, location).UTC()
	end := time.Date(2026, 3, 10, 1, 30, 0, 0, location).UTC()
	buckets := dashboardMultiplierTrendBuckets(start, end, 24*time.Hour, location)
	if len(buckets) != 4 {
		t.Fatalf("daily bucket count = %d, want 4: %+v", len(buckets), buckets)
	}
	labels := []string{"2026-03-07", "2026-03-08", "2026-03-09", "2026-03-10"}
	for index, bucket := range buckets {
		if bucket.label != labels[index] {
			t.Fatalf("bucket %d label = %q, want %q", index, bucket.label, labels[index])
		}
	}
	// Local midnight-to-midnight on the spring-forward day is 23 physical
	// hours, which proves labels and SQL boundaries follow the configured zone
	// rather than UTC dates.
	if got := buckets[1].end.Sub(buckets[1].start); got != 23*time.Hour {
		t.Fatalf("DST local day duration = %s, want 23h", got)
	}
}

func TestDashboardMultiplierTrendLocalSubDailyBucketsRespectFallBackDST(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	start := time.Date(2026, 11, 1, 0, 30, 0, 0, location).UTC()

	twoHourEnd := time.Date(2026, 11, 1, 6, 0, 0, 0, location).UTC()
	twoHourBuckets := dashboardMultiplierTrendBuckets(start, twoHourEnd, 2*time.Hour, location)
	if len(twoHourBuckets) != 3 {
		t.Fatalf("2h bucket count = %d, want 3: %+v", len(twoHourBuckets), twoHourBuckets)
	}
	if got := twoHourBuckets[0].start.In(location).Format(time.RFC3339); got != "2026-11-01T00:00:00-04:00" {
		t.Fatalf("2h first local start = %s", got)
	}
	if got := twoHourBuckets[0].end.In(location).Format(time.RFC3339); got != "2026-11-01T02:00:00-05:00" {
		t.Fatalf("2h fall-back local end = %s", got)
	}
	if got := twoHourBuckets[0].end.Sub(twoHourBuckets[0].start); got != 3*time.Hour {
		t.Fatalf("2h fall-back physical duration = %s, want 3h", got)
	}
	if twoHourBuckets[1].start != twoHourBuckets[0].end || twoHourBuckets[2].start != twoHourBuckets[1].end {
		t.Fatalf("2h buckets overlap or leave a gap: %+v", twoHourBuckets)
	}
	if twoHourBuckets[0].label != "2026-11-01T04:00:00Z" || twoHourBuckets[1].label != "2026-11-01T07:00:00Z" {
		t.Fatalf("2h labels did not preserve distinct instants: %+v", twoHourBuckets)
	}

	twelveHourEnd := time.Date(2026, 11, 2, 0, 0, 0, 0, location).UTC()
	twelveHourBuckets := dashboardMultiplierTrendBuckets(start, twelveHourEnd, 12*time.Hour, location)
	if len(twelveHourBuckets) != 2 {
		t.Fatalf("12h bucket count = %d, want 2: %+v", len(twelveHourBuckets), twelveHourBuckets)
	}
	if got := twelveHourBuckets[0].start.In(location).Format(time.RFC3339); got != "2026-11-01T00:00:00-04:00" {
		t.Fatalf("12h first local start = %s", got)
	}
	if got := twelveHourBuckets[0].end.In(location).Format(time.RFC3339); got != "2026-11-01T12:00:00-05:00" {
		t.Fatalf("12h fall-back local end = %s", got)
	}
	if got := twelveHourBuckets[0].end.Sub(twelveHourBuckets[0].start); got != 13*time.Hour {
		t.Fatalf("12h fall-back physical duration = %s, want 13h", got)
	}
	if twelveHourBuckets[1].start != twelveHourBuckets[0].end {
		t.Fatalf("12h buckets overlap or leave a gap: %+v", twelveHourBuckets)
	}
}

func TestDashboardMultiplierSuccessfulUserRequestSQLBuildsOnCanonicalSuccess(t *testing.T) {
	sql := dashboardMultiplierSuccessfulUserRequestSQL("")
	if !strings.Contains(sql, successfulGatewayRequestSQL("")) {
		t.Fatalf("multiplier predicate bypassed canonical success predicate: %s", sql)
	}
	for _, required := range []string{"client_canceled", "latency_probe", "api_test", "model_check", "internal_", "downstream_key_id > 0", "/v1/responses"} {
		if !strings.Contains(sql, required) {
			t.Fatalf("multiplier predicate missing %q: %s", required, sql)
		}
	}
}

func appendDashboardMultiplierTrendLog(t *testing.T, store *Gateway, log GatewayRequestLog, routeSnapshot string, sequence int, mutateAudit ...func(*GatewayRequestDispatchAudit)) *GatewayRequestDispatchAudit {
	t.Helper()
	if log.GatewayApp != "" {
		log.TelemetryVersion = GatewayRequestTelemetryVersion
		log.LogicalRequestTraceID = fmt.Sprintf("%032x", sequence)
		log.RoutingMode = GatewayRoutingModeSmart
		log.RouteExclusionSnapshotVersion = GatewayRouteExclusionSnapshotVersion
		log.RouteExclusionsSnapshot = routeSnapshot
		log.ChannelIdentityRevision = 1
		log.UpstreamGroupID = 1
		log.UpstreamGroupGeneration = 1
		log.UpstreamKeyLocalID = 1
		log.UpstreamKeyGeneration = 1
		completeDashboardMultiplierPricingFixture(&log)
	}
	if err := store.AppendRequestLog(&log); err != nil {
		t.Fatalf("append valid telemetered request log %d: %v", sequence, err)
	}
	if log.GatewayApp == "" {
		return nil
	}

	terminalAt := log.CreatedAt
	audit := &GatewayRequestDispatchAudit{
		LogicalRequestTraceID:           log.LogicalRequestTraceID,
		TelemetryVersion:                GatewayRequestTelemetryVersion,
		RoutingMode:                     log.RoutingMode,
		GatewayApp:                      log.GatewayApp,
		PhysicallyDispatched:            true,
		PhysicalBillingInvariantVersion: GatewayRequestDispatchPhysicalBillingInvariantVersion,
		RouteExclusionSnapshotVersion:   GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:         routeSnapshot,
		DownstreamKeyID:                 log.DownstreamKeyID,
		ChannelID:                       log.ChannelID,
		ChannelNameSnapshot:             log.ChannelNameSnapshot,
		GroupName:                       log.GroupName,
		UpstreamKeyID:                   log.UpstreamKeyID,
		UpstreamKeyName:                 log.UpstreamKeyName,
		ModelName:                       log.ModelName,
		UpstreamModelName:               log.UpstreamModelName,
		Method:                          log.Method,
		Path:                            log.Path,
		RouteReason:                     log.RouteReason,
		GroupRatio:                      log.GroupRatio,
		CompletionRatio:                 log.CompletionRatio,
		RawGroupRatio:                   log.RawGroupRatio,
		RawCompletionRatio:              log.RawCompletionRatio,
		FundingFactor:                   log.FundingFactor,
		CostBasisVersion:                log.CostBasisVersion,
		CostBasisSource:                 log.CostBasisSource,
		ChannelIdentityRevision:         log.ChannelIdentityRevision,
		UpstreamGroupID:                 log.UpstreamGroupID,
		UpstreamGroupGeneration:         log.UpstreamGroupGeneration,
		UpstreamKeyLocalID:              log.UpstreamKeyLocalID,
		UpstreamKeyGeneration:           log.UpstreamKeyGeneration,
		PriceCanonicalGroupName:         log.PriceCanonicalGroupName,
		PriceModelName:                  log.PriceModelName,
		PriceOperation:                  log.PriceOperation,
		PriceProtocol:                   log.PriceProtocol,
		UsageSchema:                     log.UsageSchema,
		UsageSchemaVersion:              log.UsageSchemaVersion,
		UsageOperationPresent:           log.UsageOperationPresent,
		OperationUnits:                  cloneGatewayRequestPricingInt64(log.OperationUnits),
		RequestStartedAt:                log.CreatedAt.Add(-time.Second),
		Stage:                           GatewayRequestDispatchStageFinalized,
		TerminalAt:                      &terminalAt,
		TerminalKind:                    "success",
		RequestLogID:                    &log.ID,
		DispatchedAt:                    log.CreatedAt.Add(-time.Second),
		CreatedAt:                       log.CreatedAt.Add(-time.Second),
	}
	if log.PriceOperation == GatewayImageOperationGeneration {
		audit.BillingCommitted = true
	}
	for _, mutate := range mutateAudit {
		if mutate != nil {
			mutate(audit)
		}
	}
	return persistDashboardMultiplierTrendAudit(t, store, audit, sequence)
}

func completeDashboardMultiplierPricingFixture(log *GatewayRequestLog) {
	if log == nil {
		return
	}
	log.PriceCanonicalGroupName = "default"
	log.PriceModelName = log.ModelName
	log.UsageSchemaVersion = 1
	switch log.Path {
	case "/v1/images/generations":
		units := int64(1)
		log.PriceOperation = GatewayImageOperationGeneration
		log.PriceProtocol = GatewayImageProtocolImages
		log.UsageSchema = GatewayPricingUsageSchemaOperationV1
		log.UsageOperationPresent = true
		log.OperationUnits = &units
	case "/v1/messages":
		log.PriceOperation = GatewayPricingOperationTextGeneration
		log.PriceProtocol = GatewayPricingProtocolAnthropicMessages
		log.UsageSchema = GatewayPricingUsageSchemaTokensV1
	case "/v1/chat/completions":
		log.PriceOperation = GatewayPricingOperationTextGeneration
		log.PriceProtocol = GatewayPricingProtocolOpenAIChatCompletions
		log.UsageSchema = GatewayPricingUsageSchemaTokensV1
	default:
		log.PriceOperation = GatewayPricingOperationTextGeneration
		log.PriceProtocol = GatewayPricingProtocolOpenAIResponses
		log.UsageSchema = GatewayPricingUsageSchemaTokensV1
	}
}

func persistDashboardMultiplierTrendAudit(t *testing.T, store *Gateway, desired *GatewayRequestDispatchAudit, sequence int) *GatewayRequestDispatchAudit {
	t.Helper()
	if desired == nil {
		t.Fatal("dashboard multiplier audit fixture is required")
	}
	ownerNow := time.Now().UTC()
	if err := store.RegisterRequestDispatchOwner(context.Background(), ownerNow, ownerNow.Add(30*time.Second)); err != nil {
		t.Fatalf("register dashboard multiplier dispatch owner %d: %v", sequence, err)
	}
	ownerInstanceID, ownerFenceToken, err := store.RequestDispatchOwnerIdentity()
	if err != nil {
		t.Fatalf("load dashboard multiplier dispatch owner %d: %v", sequence, err)
	}
	final := *desired
	staged := final
	staged.ID = 0
	staged.PhysicallyDispatched = false
	staged.BillingCommitted = false
	staged.TransportFenceVersion = GatewayRequestDispatchTransportFenceVersion
	staged.TransportState = GatewayRequestDispatchTransportStatePrepared
	staged.TransportConfirmationVersion = 1
	staged.TransportReturnConfirmed = false
	staged.TransportReturnConfirmedAt = nil
	staged.Stage = GatewayRequestDispatchStageDispatched
	staged.TerminalAt = nil
	staged.TerminalKind = ""
	staged.RequestLogID = nil
	staged.OwnerInstanceID = ownerInstanceID
	staged.OwnerFenceToken = ownerFenceToken
	staged.LegacyOwnerless = false
	if err := store.db.Create(&staged).Error; err != nil {
		t.Fatalf("append prepared frozen dispatch audit %d: %v", sequence, err)
	}

	if final.PhysicallyDispatched {
		if err := store.db.Model(&GatewayRequestDispatchAudit{}).Where("id = ?", staged.ID).Updates(map[string]any{
			"transport_state":       GatewayRequestDispatchTransportStateArmed,
			"physically_dispatched": true,
			"billing_committed":     final.BillingCommitted,
		}).Error; err != nil {
			t.Fatalf("arm frozen dispatch audit %d: %v", sequence, err)
		}
		if err := store.db.Model(&GatewayRequestDispatchAudit{}).Where("id = ?", staged.ID).Updates(map[string]any{
			"transport_state":               GatewayRequestDispatchTransportStateConfirmed,
			"transport_return_confirmed":    true,
			"transport_return_confirmed_at": ownerNow,
		}).Error; err != nil {
			t.Fatalf("confirm frozen dispatch audit %d: %v", sequence, err)
		}
	} else if err := store.db.Model(&GatewayRequestDispatchAudit{}).Where("id = ?", staged.ID).
		Update("transport_state", GatewayRequestDispatchTransportStateNotDispatched).Error; err != nil {
		t.Fatalf("mark frozen dispatch audit %d not dispatched: %v", sequence, err)
	}

	if err := store.db.Model(&GatewayRequestDispatchAudit{}).Where("id = ?", staged.ID).Updates(map[string]any{
		"stage":          final.Stage,
		"terminal_at":    final.TerminalAt,
		"terminal_kind":  final.TerminalKind,
		"request_log_id": final.RequestLogID,
	}).Error; err != nil {
		t.Fatalf("finalize frozen dispatch audit %d: %v", sequence, err)
	}
	var persisted GatewayRequestDispatchAudit
	if err := store.db.First(&persisted, staged.ID).Error; err != nil {
		t.Fatalf("reload frozen dispatch audit %d: %v", sequence, err)
	}
	return &persisted
}

func dashboardMultiplierTrendAppForTest(t *testing.T, trend *DashboardMultiplierTrend, app GatewayApp) DashboardMultiplierTrendApp {
	t.Helper()
	for _, series := range trend.Apps {
		if series.App == string(app) {
			return series
		}
	}
	t.Fatalf("missing %s series", app)
	return DashboardMultiplierTrendApp{}
}

func assertDashboardMultiplierNear(t *testing.T, actual *float64, want float64) {
	t.Helper()
	if actual == nil || math.Abs(*actual-want) > 0.0000001 {
		if actual == nil {
			t.Fatalf("multiplier = nil, want %v", want)
		}
		t.Fatalf("multiplier = %v, want %v", *actual, want)
	}
}
