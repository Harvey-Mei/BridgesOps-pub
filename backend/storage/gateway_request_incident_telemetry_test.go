package storage

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func testGatewayRouteExclusionSnapshot(t *testing.T) string {
	t.Helper()
	snapshot, err := NewGatewayRouteExclusionSnapshot(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC), []GatewayRouteExclusionSnapshotEntry{{
		ChannelID:          17,
		GroupName:          "standard",
		Reason:             "upstream_temporarily_unavailable",
		UpstreamGroupID:    91,
		PresenceGeneration: 4,
		FailureCount:       3,
	}})
	if err != nil {
		t.Fatalf("build route exclusion snapshot: %v", err)
	}
	return snapshot
}

func TestNormalizeGatewayRequestTelemetrySeparatesHistoricalAndCurrentFacts(t *testing.T) {
	if version, traceID, mode, app, budget, err := normalizeGatewayRequestTelemetry(0, "", "", "", 0); err != nil || version != 0 || traceID != "" || mode != "" || app != "" || budget != 0 {
		t.Fatalf("historical telemetry = %d %q %q %q %d err=%v", version, traceID, mode, app, budget, err)
	}

	trace := strings.Repeat("A", 32)
	version, traceID, mode, app, budget, err := normalizeGatewayRequestTelemetry(
		GatewayRequestTelemetryVersion,
		trace,
		" SMART ",
		" CODEX ",
		30_000,
	)
	if err != nil {
		t.Fatalf("normalize current telemetry: %v", err)
	}
	if version != GatewayRequestTelemetryVersion || traceID != strings.ToLower(trace) || mode != GatewayRoutingModeSmart || app != string(GatewayAppCodex) || budget != 30_000 {
		t.Fatalf("current telemetry = %d %q %q %q %d", version, traceID, mode, app, budget)
	}

	for _, test := range []struct {
		name    string
		version int
		trace   string
		mode    string
		app     string
		budget  int64
	}{
		{name: "unversioned trace", trace: strings.Repeat("a", 32)},
		{name: "short trace", version: GatewayRequestTelemetryVersion, trace: "abc", mode: GatewayRoutingModeSmart, app: string(GatewayAppCodex)},
		{name: "unknown mode", version: GatewayRequestTelemetryVersion, trace: strings.Repeat("a", 32), mode: "automatic", app: string(GatewayAppCodex)},
		{name: "unknown app", version: GatewayRequestTelemetryVersion, trace: strings.Repeat("a", 32), mode: GatewayRoutingModeSmart, app: "other"},
		{name: "negative budget", version: GatewayRequestTelemetryVersion, trace: strings.Repeat("a", 32), mode: GatewayRoutingModeSmart, app: string(GatewayAppCodex), budget: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, _, _, _, err := normalizeGatewayRequestTelemetry(test.version, test.trace, test.mode, test.app, test.budget); err == nil {
				t.Fatal("invalid telemetry unexpectedly normalized")
			}
		})
	}
}

func TestNormalizeGatewayRequestLogTelemetryValidatesTerminalFacts(t *testing.T) {
	visible := int64(42)
	routeSnapshot := testGatewayRouteExclusionSnapshot(t)
	log := &GatewayRequestLog{
		LogicalRequestTraceID:                 strings.Repeat("b", 32),
		TelemetryVersion:                      GatewayRequestTelemetryVersion,
		RoutingMode:                           GatewayRoutingModeSmart,
		GatewayApp:                            string(GatewayAppClaude),
		DownstreamFirstVisibleMs:              &visible,
		TerminalFailureCode:                   " UPSTREAM_5XX ",
		PriorPhysicallyDispatchedFailureCount: 2,
		RealFallback:                          true,
		RouteExclusionSnapshotVersion:         GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:               routeSnapshot,
	}
	if err := normalizeGatewayRequestLogTelemetry(log); err != nil {
		t.Fatalf("normalize request telemetry log: %v", err)
	}
	if log.TerminalFailureCode != "upstream_5xx" || log.DownstreamFirstVisibleMs == nil || *log.DownstreamFirstVisibleMs != visible {
		t.Fatalf("normalized request telemetry log = %+v", log)
	}

	for _, test := range []struct {
		name string
		log  GatewayRequestLog
	}{
		{name: "historical visible fact", log: GatewayRequestLog{DownstreamFirstVisibleMs: &visible}},
		{name: "current telemetry missing route snapshot", log: GatewayRequestLog{LogicalRequestTraceID: strings.Repeat("a", 32), TelemetryVersion: 1, RoutingMode: GatewayRoutingModeSmart, GatewayApp: string(GatewayAppCodex)}},
		{name: "invalid terminal code", log: GatewayRequestLog{LogicalRequestTraceID: strings.Repeat("c", 32), TelemetryVersion: 1, RoutingMode: GatewayRoutingModeSmart, GatewayApp: string(GatewayAppGemini), RouteExclusionSnapshotVersion: 1, RouteExclusionsSnapshot: routeSnapshot, TerminalFailureCode: "upstream-error"}},
		{name: "fixed real fallback", log: GatewayRequestLog{LogicalRequestTraceID: strings.Repeat("d", 32), TelemetryVersion: 1, RoutingMode: GatewayRoutingModeFixed, GatewayApp: string(GatewayAppCodex), RouteExclusionSnapshotVersion: 1, RouteExclusionsSnapshot: routeSnapshot, RealFallback: true}},
		{name: "negative prior failures", log: GatewayRequestLog{LogicalRequestTraceID: strings.Repeat("e", 32), TelemetryVersion: 1, RoutingMode: GatewayRoutingModeSmart, GatewayApp: string(GatewayAppCodex), RouteExclusionSnapshotVersion: 1, RouteExclusionsSnapshot: routeSnapshot, PriorPhysicallyDispatchedFailureCount: -1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := test.log
			if err := normalizeGatewayRequestLogTelemetry(&candidate); err == nil {
				t.Fatal("invalid request telemetry log unexpectedly normalized")
			}
		})
	}
}

func TestUpdateRequestLogDownstreamFirstVisibleIsTraceFencedAndImmutable(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	traceID := strings.Repeat("f", 32)
	log := &GatewayRequestLog{
		DownstreamKeyID:               1,
		Method:                        http.MethodPost,
		Path:                          "/v1/responses",
		StatusCode:                    http.StatusBadGateway,
		LogicalRequestTraceID:         traceID,
		TelemetryVersion:              GatewayRequestTelemetryVersion,
		RoutingMode:                   GatewayRoutingModeSmart,
		GatewayApp:                    string(GatewayAppCodex),
		TerminalFailureCode:           "upstream_5xx",
		RouteExclusionSnapshotVersion: GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:       testGatewayRouteExclusionSnapshot(t),
		Error:                         "upstream failed",
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append telemetry request log: %v", err)
	}
	if err := store.UpdateRequestLogDownstreamFirstVisible(log.ID, traceID, 37); err != nil {
		t.Fatalf("update downstream visibility: %v", err)
	}
	var persisted GatewayRequestLog
	if err := db.First(&persisted, log.ID).Error; err != nil {
		t.Fatalf("read telemetry request log: %v", err)
	}
	if persisted.DownstreamFirstVisibleMs == nil || *persisted.DownstreamFirstVisibleMs != 37 {
		t.Fatalf("persisted downstream visibility = %v", persisted.DownstreamFirstVisibleMs)
	}
	if err := store.UpdateRequestLogDownstreamFirstVisible(log.ID, traceID, 99); !errors.Is(err, ErrGatewayRequestRecordInvalid) {
		t.Fatalf("overwrite downstream visibility error = %v, want immutable conflict", err)
	}
	if err := store.UpdateRequestLogDownstreamFirstVisible(log.ID, strings.Repeat("e", 32), 11); !errors.Is(err, ErrGatewayRequestRecordInvalid) {
		t.Fatalf("wrong-trace visibility error = %v, want trace fence", err)
	}
}

func TestGatewayRouteExclusionSnapshotIsVersionedBoundedAndRedacted(t *testing.T) {
	raw := testGatewayRouteExclusionSnapshot(t)
	snapshot, ok := DecodeGatewayRouteExclusionSnapshot(GatewayRouteExclusionSnapshotVersion, raw)
	if !ok || snapshot.Version != GatewayRouteExclusionSnapshotVersion || snapshot.TotalEntries != 1 || snapshot.Truncated || len(snapshot.Entries) != 1 {
		t.Fatalf("decoded route exclusion snapshot = %+v ok=%t", snapshot, ok)
	}
	entry := snapshot.Entries[0]
	if entry.UpstreamGroupID != 91 || entry.PresenceGeneration != 4 || entry.FailureCount != 3 || entry.CooldownUntil != nil {
		t.Fatalf("generation-bound isolation entry = %+v", entry)
	}
	if _, ok := DecodeGatewayRouteExclusionSnapshot(0, raw); ok {
		t.Fatal("historical version zero unexpectedly exposed a route snapshot")
	}
	withDetail := strings.Replace(raw, `"reason":"upstream_temporarily_unavailable"`, `"reason":"upstream_temporarily_unavailable","detail":"Bearer sk-secret raw error"`, 1)
	if _, _, err := normalizeGatewayRouteExclusionSnapshotText(withDetail); err == nil {
		t.Fatal("route exclusion snapshot accepted forbidden diagnostic detail")
	}
}

func TestGatewayRouteExclusionSnapshotMarksBoundedCoverage(t *testing.T) {
	entries := make([]GatewayRouteExclusionSnapshotEntry, 0, gatewayRouteExclusionSnapshotMaxRows+1)
	for index := 0; index <= gatewayRouteExclusionSnapshotMaxRows; index++ {
		entries = append(entries, GatewayRouteExclusionSnapshotEntry{
			ChannelID:          uint(index + 1),
			GroupName:          "standard",
			Reason:             "model_not_listed",
			UpstreamGroupID:    uint(index + 100),
			PresenceGeneration: 1,
		})
	}
	raw, err := NewGatewayRouteExclusionSnapshot(time.Now().UTC(), entries)
	if err != nil {
		t.Fatalf("build bounded route exclusion snapshot: %v", err)
	}
	snapshot, ok := DecodeGatewayRouteExclusionSnapshot(GatewayRouteExclusionSnapshotVersion, raw)
	if !ok || !snapshot.Truncated || snapshot.TotalEntries != gatewayRouteExclusionSnapshotMaxRows+1 || len(snapshot.Entries) != gatewayRouteExclusionSnapshotMaxRows {
		t.Fatalf("bounded snapshot coverage = %+v ok=%t", snapshot, ok)
	}
}
