package api

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayRequestTelemetryRecordsFirstVisibleBodyWrite(t *testing.T) {
	startedAt := time.Now().UTC().Add(-25 * time.Millisecond)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	downstream := &storage.GatewayDownstreamKey{
		App:          storage.GatewayAppCodex,
		SmartRouting: true,
	}

	telemetry := gatewayRequestTelemetryFromContext(context, startedAt, downstream)
	if _, ok := context.Writer.(*gatewayTelemetryResponseWriter); !ok {
		t.Fatalf("response writer = %T, want gateway telemetry wrapper", context.Writer)
	}
	if got := gatewayRequestTelemetryFromContext(context, time.Now(), downstream); got != telemetry {
		t.Fatal("logical request created more than one telemetry writer")
	}
	if _, err := context.Writer.WriteString("visible"); err != nil {
		t.Fatalf("write downstream response: %v", err)
	}

	log := &storage.GatewayRequestLog{StatusCode: http.StatusOK}
	telemetry.applyToLog(log, "")
	if log.TelemetryVersion != storage.GatewayRequestTelemetryVersion || log.RoutingMode != storage.GatewayRoutingModeSmart || log.GatewayApp != string(storage.GatewayAppCodex) {
		t.Fatalf("telemetry metadata = version %d mode %q app %q", log.TelemetryVersion, log.RoutingMode, log.GatewayApp)
	}
	decoded, err := hex.DecodeString(log.LogicalRequestTraceID)
	if err != nil || len(decoded) != 16 || len(log.LogicalRequestTraceID) != 32 {
		t.Fatalf("logical trace id = %q err=%v, want 128-bit lowercase hex", log.LogicalRequestTraceID, err)
	}
	if log.DownstreamFirstVisibleMs == nil || *log.DownstreamFirstVisibleMs < 1 {
		t.Fatalf("downstream first visible latency = %v, want positive duration", log.DownstreamFirstVisibleMs)
	}
	if log.TerminalFailureCode != "" {
		t.Fatalf("successful request terminal failure code = %q", log.TerminalFailureCode)
	}
	if snapshot, ok := storage.DecodeGatewayRouteExclusionSnapshot(log.RouteExclusionSnapshotVersion, log.RouteExclusionsSnapshot); !ok || snapshot.TotalEntries != 0 || snapshot.Truncated {
		t.Fatalf("empty frozen route exclusion snapshot = %+v ok=%t", snapshot, ok)
	}
	if !strings.Contains(log.RouteExclusionsSnapshot, `"entries":[]`) {
		t.Fatalf("empty route exclusion entries were not encoded as an array: %s", log.RouteExclusionsSnapshot)
	}
}

func TestGatewayRequestTelemetryFreezesGenerationBoundIsolationExclusions(t *testing.T) {
	capturedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	telemetry := newGatewayRequestTelemetry(capturedAt, storage.GatewayAppCodex, true)
	decision := &gatewayRouteDecision{Exclusions: []gateway.CandidateExclusion{{
		ChannelID:          17,
		GroupName:          "Standard",
		Reason:             "upstream_temporarily_unavailable",
		UpstreamGroupID:    91,
		PresenceGeneration: 4,
		FailureCount:       3,
		Detail:             "Authorization: Bearer sk-secret raw provider error",
	}}}
	telemetry.captureRouteExclusions(decision, capturedAt)

	log := &storage.GatewayRequestLog{StatusCode: http.StatusBadGateway, Error: "upstream failed"}
	telemetry.applyToLog(log, "upstream_5xx")
	snapshot, ok := storage.DecodeGatewayRouteExclusionSnapshot(log.RouteExclusionSnapshotVersion, log.RouteExclusionsSnapshot)
	if !ok || len(snapshot.Entries) != 1 {
		t.Fatalf("route exclusion snapshot = %+v ok=%t", snapshot, ok)
	}
	entry := snapshot.Entries[0]
	if entry.GroupName != "standard" || entry.UpstreamGroupID != 91 || entry.PresenceGeneration != 4 || entry.FailureCount != 3 {
		t.Fatalf("route exclusion entry = %+v", entry)
	}
	if strings.Contains(log.RouteExclusionsSnapshot, "sk-secret") || strings.Contains(log.RouteExclusionsSnapshot, "Authorization") || strings.Contains(log.RouteExclusionsSnapshot, "raw provider error") {
		t.Fatalf("route exclusion snapshot leaked diagnostic detail: %s", log.RouteExclusionsSnapshot)
	}
}

func TestGatewayRequestTelemetryCountsEachPhysicalFailureOnce(t *testing.T) {
	telemetry := newGatewayRequestTelemetry(time.Now(), storage.GatewayAppClaude, true)
	preparation := &gatewayDispatchAuditState{id: 1, physicallyDispatched: false}
	preparation.recordTelemetryPhysicalFailure(telemetry)
	physical := &gatewayDispatchAuditState{id: 2, physicallyDispatched: true}
	physical.recordTelemetryPhysicalFailure(telemetry)
	physical.recordTelemetryPhysicalFailure(telemetry)

	log := &storage.GatewayRequestLog{StatusCode: http.StatusBadGateway, Error: "upstream failed"}
	telemetry.applyToLog(log, "upstream_5xx")
	if log.PriorPhysicallyDispatchedFailureCount != 1 {
		t.Fatalf("physical failure count = %d, want 1", log.PriorPhysicallyDispatchedFailureCount)
	}
	if log.RealFallback {
		t.Fatal("failure count alone must not assert a successful real fallback")
	}
}

func TestGatewayTelemetryTerminalFailureUsesStructuredPhysicalReason(t *testing.T) {
	for _, tc := range []struct {
		name string
		log  *storage.GatewayRequestLog
		want string
	}{
		{
			name: "structured transport",
			log: &storage.GatewayRequestLog{
				StatusCode:             http.StatusBadGateway,
				RouteAttemptedChannels: `[{"channel_id":17,"channel_name":"primary","reason":"transport","physically_dispatched":true,"first_byte_budget_ms":30000}]`,
				Error:                  "upstream failed",
			},
			want: "transport",
		},
		{
			name: "fixed body started",
			log: &storage.GatewayRequestLog{
				StatusCode: http.StatusBadGateway,
				Error:      "read response body after upstream body started: unexpected EOF",
			},
			want: "response_body_started",
		},
		{
			name: "fixed local spool",
			log: &storage.GatewayRequestLog{
				StatusCode: http.StatusBadGateway,
				Error:      "local response spool unavailable after physical dispatch",
			},
			want: "response_spool_local",
		},
		{
			name: "malformed terminal frame",
			log:  &storage.GatewayRequestLog{StatusCode: http.StatusBadGateway, Error: "upstream returned a malformed protocol terminal frame"},
			want: "malformed_terminal_frame",
		},
		{
			name: "truncated stream",
			log:  &storage.GatewayRequestLog{StatusCode: http.StatusBadGateway, Error: "stream read failed after upstream acceptance: unexpected EOF"},
			want: "upstream_stream_truncated",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := gatewayTelemetryTerminalFailureCode(tc.log); got != tc.want {
				t.Fatalf("terminal failure code = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGatewayRouteAttemptTelemetryUsesPersistedDispatchFacts(t *testing.T) {
	candidate := testGatewayTelemetryCandidate()
	attempt := newGatewayRouteAttemptFromDispatch(&gatewayDispatchAuditState{
		id:                   7,
		physicallyDispatched: true,
		firstByteBudgetMs:    30_000,
	}, candidate, "response_timeout", http.StatusGatewayTimeout)
	if !attempt.Physical || attempt.FirstByteBudgetMs != 30_000 {
		t.Fatalf("route attempt telemetry = physical %t budget %d", attempt.Physical, attempt.FirstByteBudgetMs)
	}

	preparation := newGatewayRouteAttempt(candidate, "upstream_key", 0)
	raw := gatewayRouteAttemptedChannels(&gatewayRouteDecision{SmartRouting: true}, []gatewayRouteAttempt{preparation, attempt}, candidate)
	traces, ok := gatewayRouteAttemptTraces(raw)
	if !ok || len(traces) != 1 {
		t.Fatalf("route attempt traces = %#v ok=%t, want only physical failure", traces, ok)
	}
	if !traces[0].Physical || traces[0].FirstByteBudgetMs != 30_000 {
		t.Fatalf("structured route attempt = %#v", traces[0])
	}
}

func testGatewayTelemetryCandidate() gateway.Candidate {
	return gateway.Candidate{
		Channel: storage.Channel{ID: 17, Name: "primary"},
		Rate:    storage.RateSnapshot{ModelName: "standard"},
	}
}
