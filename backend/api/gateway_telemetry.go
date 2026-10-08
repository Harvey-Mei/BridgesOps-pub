package api

import (
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

const gatewayRequestTelemetryContextKey = "bridgeops.gateway.request.telemetry"

var gatewayTelemetryFallbackSequence atomic.Uint64

// gatewayRequestTelemetry is the immutable logical-request writer shared by
// every physical dispatch and the final ordinary request log. It deliberately
// stores no request body, credential, session identifier, or upstream payload.
type gatewayRequestTelemetry struct {
	startedAt   time.Time
	traceID     string
	telemetry   int
	routingMode string
	gatewayApp  string

	mu                                sync.Mutex
	firstVisibleAt                    time.Time
	priorPhysicalDispatchFailureCount int
	physicallyDispatchedCount         int
	realFallback                      bool
	routeSnapshotCaptured             bool
	routeExclusionSnapshotVersion     int
	routeExclusionsSnapshot           string
}

func newGatewayRequestTelemetry(start time.Time, app storage.GatewayApp, smartRouting bool) *gatewayRequestTelemetry {
	if start.IsZero() {
		start = time.Now().UTC()
	}
	traceID := newGatewayLogicalRequestTraceID()
	mode := storage.GatewayRoutingModeFixed
	if smartRouting {
		mode = storage.GatewayRoutingModeSmart
	}
	return &gatewayRequestTelemetry{
		startedAt:   start.UTC(),
		traceID:     traceID,
		telemetry:   storage.GatewayRequestTelemetryVersion,
		routingMode: mode,
		gatewayApp:  strings.ToLower(strings.TrimSpace(string(app))),
	}
}

func newGatewayLogicalRequestTraceID() string {
	var raw [16]byte
	if _, err := cryptorand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	// crypto/rand failure is exceptionally rare, but dropping the join key would
	// make the request impossible to audit. The fallback remains irreversible and
	// contains no client or upstream material; its monotonic input prevents reuse.
	sequence := gatewayTelemetryFallbackSequence.Add(1)
	digest := sha256.Sum256([]byte(fmt.Sprintf("bridgeops-telemetry:%d:%d", time.Now().UTC().UnixNano(), sequence)))
	return hex.EncodeToString(digest[:16])
}

func gatewayRequestTelemetryFromContext(c *gin.Context, start time.Time, downstream *storage.GatewayDownstreamKey) *gatewayRequestTelemetry {
	if c != nil {
		if value, ok := c.Get(gatewayRequestTelemetryContextKey); ok {
			if telemetry, ok := value.(*gatewayRequestTelemetry); ok && telemetry != nil {
				wrapGatewayTelemetryResponseWriter(c, telemetry)
				return telemetry
			}
		}
	}
	var app storage.GatewayApp
	var smart bool
	if downstream != nil {
		app = downstream.App
		smart = downstream.SmartRouting
	}
	telemetry := newGatewayRequestTelemetry(start, app, smart)
	if c != nil {
		c.Set(gatewayRequestTelemetryContextKey, telemetry)
		wrapGatewayTelemetryResponseWriter(c, telemetry)
	}
	return telemetry
}

func wrapGatewayTelemetryResponseWriter(c *gin.Context, telemetry *gatewayRequestTelemetry) {
	if c == nil || c.Writer == nil || telemetry == nil {
		return
	}
	if _, wrapped := c.Writer.(*gatewayTelemetryResponseWriter); !wrapped {
		c.Writer = &gatewayTelemetryResponseWriter{ResponseWriter: c.Writer, telemetry: telemetry}
	}
}

func (t *gatewayRequestTelemetry) markDownstreamFirstVisible(at time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.firstVisibleAt.IsZero() {
		if at.IsZero() {
			at = time.Now().UTC()
		}
		t.firstVisibleAt = at.UTC()
	}
}

func (t *gatewayRequestTelemetry) markPhysicalDispatch() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.physicallyDispatchedCount++
	t.mu.Unlock()
}

func (t *gatewayRequestTelemetry) markPhysicalDispatchFailure() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.priorPhysicalDispatchFailureCount++
	t.mu.Unlock()
}

func (t *gatewayRequestTelemetry) setRealFallback(value bool) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.realFallback = value
	t.mu.Unlock()
}

func (t *gatewayRequestTelemetry) captureRouteExclusions(decision *gatewayRouteDecision, at time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.routeSnapshotCaptured {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	entries := make([]storage.GatewayRouteExclusionSnapshotEntry, 0)
	seen := make(map[string]struct{})
	totalExclusions := 0
	if decision != nil {
		totalExclusions = len(decision.Exclusions)
		entries = make([]storage.GatewayRouteExclusionSnapshotEntry, 0, len(decision.Exclusions))
		seen = make(map[string]struct{}, len(decision.Exclusions))
		for _, exclusion := range decision.Exclusions {
			reason := strings.TrimSpace(exclusion.Reason)
			switch reason {
			case "upstream_generation_unavailable", "upstream_temporarily_unavailable", "model_capability_unknown", "model_not_listed", "model_not_supported":
			default:
				continue
			}
			groupName := strings.ToLower(strings.TrimSpace(exclusion.GroupName))
			if exclusion.ChannelID == 0 || groupName == "" {
				continue
			}
			key := fmt.Sprintf("%d\x00%s\x00%s", exclusion.ChannelID, groupName, reason)
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			entry := storage.GatewayRouteExclusionSnapshotEntry{
				ChannelID:          exclusion.ChannelID,
				GroupName:          groupName,
				Reason:             reason,
				UpstreamGroupID:    exclusion.UpstreamGroupID,
				PresenceGeneration: exclusion.PresenceGeneration,
				FailureCount:       exclusion.FailureCount,
			}
			if exclusion.CooldownUntil != nil {
				until := exclusion.CooldownUntil.UTC()
				entry.CooldownUntil = &until
			}
			entries = append(entries, entry)
		}
	}
	encoded, err := storage.NewGatewayRouteExclusionSnapshot(at.UTC(), entries, totalExclusions)
	if err != nil {
		// Preserve the incomplete-coverage fact without allowing malformed internal
		// diagnostic data into the durable audit record.
		if totalExclusions == 0 {
			totalExclusions = 1
		}
		encoded, _ = storage.NewGatewayRouteExclusionSnapshot(at.UTC(), nil, totalExclusions)
	}
	t.routeSnapshotCaptured = true
	t.routeExclusionSnapshotVersion = storage.GatewayRouteExclusionSnapshotVersion
	t.routeExclusionsSnapshot = encoded
}

func (t *gatewayRequestTelemetry) routeExclusionSnapshot() (version int, snapshot string) {
	if t == nil {
		return 0, ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.routeExclusionSnapshotVersion, t.routeExclusionsSnapshot
}

func (t *gatewayRequestTelemetry) snapshot() (traceID string, version int, mode, app string, firstVisible *int64, priorFailures int, realFallback bool) {
	if t == nil {
		return "", 0, "", "", nil, 0, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	traceID, version, mode, app = t.traceID, t.telemetry, t.routingMode, t.gatewayApp
	priorFailures, realFallback = t.priorPhysicalDispatchFailureCount, t.realFallback
	if !t.firstVisibleAt.IsZero() && !t.startedAt.IsZero() {
		value := t.firstVisibleAt.Sub(t.startedAt).Milliseconds()
		if value < 1 {
			value = 1
		}
		firstVisible = &value
	}
	return
}

func (t *gatewayRequestTelemetry) applyToLog(log *storage.GatewayRequestLog, terminalCode string) {
	if t == nil || log == nil {
		return
	}
	t.captureRouteExclusions(nil, time.Now().UTC())
	traceID, version, mode, app, firstVisible, priorFailures, realFallback := t.snapshot()
	if traceID == "" || version == 0 {
		return
	}
	log.LogicalRequestTraceID = traceID
	log.TelemetryVersion = version
	log.RoutingMode = mode
	log.GatewayApp = app
	log.DownstreamFirstVisibleMs = firstVisible
	log.PriorPhysicallyDispatchedFailureCount = priorFailures
	log.RealFallback = realFallback
	log.RouteExclusionSnapshotVersion, log.RouteExclusionsSnapshot = t.routeExclusionSnapshot()
	if log.RecordKind == storage.GatewayRequestRecordKindDispatchCostEvidence {
		log.TerminalFailureCode = "dispatch_cost_evidence"
		log.PriorPhysicallyDispatchedFailureCount = 0
		log.RealFallback = false
		return
	}
	if strings.TrimSpace(terminalCode) == "" {
		terminalCode = gatewayTelemetryTerminalFailureCode(log)
	}
	log.TerminalFailureCode = strings.ToLower(strings.TrimSpace(terminalCode))
}

func gatewayTelemetryTerminalFailureCode(log *storage.GatewayRequestLog) string {
	if log == nil {
		return "gateway_failure"
	}
	if strings.EqualFold(strings.TrimSpace(log.RouteReason), gatewayRouteReasonClientCanceled) || strings.TrimSpace(log.Error) == "客户端取消" {
		return "client_canceled"
	}
	if log.StatusCode >= http.StatusOK && log.StatusCode < http.StatusMultipleChoices && strings.TrimSpace(log.Error) == "" {
		return ""
	}
	diagnostic := strings.ToLower(strings.Join([]string{log.Error, log.RouteDetails, log.RouteAttemptedChannels}, " "))
	for _, item := range []struct {
		needle string
		code   string
	}{
		{"malformed protocol terminal frame", "malformed_terminal_frame"},
		{"malformed json frame", "malformed_json_frame"},
		{"response body after upstream body started", "response_body_started"},
		{"truncated json frame", "upstream_stream_truncated"},
		{"unexpected eof", "upstream_stream_truncated"},
		{"stream read failed after upstream acceptance", "upstream_stream_truncated"},
		{"stream disconnected before completion", "upstream_stream_truncated"},
		{"response_timeout", "response_timeout"},
		{"context_limit", "context_limit"},
		{"model_unsupported", "model_unsupported"},
		{"stream_error", "stream_error"},
		{"response_read", "response_read"},
		{"response_body_started", "response_body_started"},
		{"response_spool_local", "response_spool_local"},
		{"local response spool unavailable after physical dispatch", "response_spool_local"},
		{"target_resolution", "target_resolution"},
		{"upstream_key", "upstream_key"},
	} {
		if strings.Contains(diagnostic, item.needle) {
			return item.code
		}
	}
	if traces, ok := gatewayRouteAttemptTraces(log.RouteAttemptedChannels); ok {
		for index := len(traces) - 1; index >= 0; index-- {
			if !traces[index].Physical {
				continue
			}
			if code := gatewayTelemetryRouteAttemptFailureCode(traces[index].Reason); code != "" {
				return code
			}
		}
	}
	switch log.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication_or_permission"
	case http.StatusPaymentRequired:
		return "payment_required"
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusTooEarly, http.StatusTooManyRequests:
		return "retryable_http_status"
	}
	if log.StatusCode >= 500 && log.StatusCode < 600 {
		return "upstream_5xx"
	}
	if log.StatusCode >= 400 && log.StatusCode < 500 {
		return "other_4xx"
	}
	return "gateway_failure"
}

func gatewayTelemetryRouteAttemptFailureCode(reason string) string {
	switch strings.TrimSpace(reason) {
	case "upstream_key", "target_resolution", "model_family", "transport", "model_unsupported", "context_limit", "response_read", "response_body_started", "response_spool_local", "response_timeout", "stream_prelude", "stream_error", "stream_empty", "stream_account_limit", "upstream_account_unavailable", "upstream_account_concurrency", "upstream_key_capacity", "dispatch_audit", "mapping_runtime_fallback":
		return strings.TrimSpace(reason)
	default:
		return ""
	}
}

func persistGatewayDownstreamFirstVisible(d *Deps, log *storage.GatewayRequestLog, telemetry *gatewayRequestTelemetry) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil || log == nil || log.ID == 0 || telemetry == nil || log.DownstreamFirstVisibleMs != nil {
		return
	}
	traceID, version, _, _, firstVisible, _, _ := telemetry.snapshot()
	if version != storage.GatewayRequestTelemetryVersion || traceID == "" || firstVisible == nil {
		return
	}
	if err := d.Gateway.Store.UpdateRequestLogDownstreamFirstVisible(log.ID, traceID, *firstVisible); err != nil {
		if d.Log != nil {
			d.Log.Warn("persist gateway downstream first visible telemetry failed", "request_log_id", log.ID, "err", err)
		}
		return
	}
	value := *firstVisible
	log.DownstreamFirstVisibleMs = &value
}

type gatewayTelemetryResponseWriter struct {
	gin.ResponseWriter
	telemetry *gatewayRequestTelemetry
}

func (w *gatewayTelemetryResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if n > 0 {
		w.telemetry.markDownstreamFirstVisible(time.Now().UTC())
	}
	return n, err
}

func (w *gatewayTelemetryResponseWriter) WriteString(value string) (int, error) {
	n, err := w.ResponseWriter.WriteString(value)
	if n > 0 {
		w.telemetry.markDownstreamFirstVisible(time.Now().UTC())
	}
	return n, err
}

func (w *gatewayTelemetryResponseWriter) Unwrap() http.ResponseWriter {
	if w == nil || w.ResponseWriter == nil {
		return nil
	}
	return w.ResponseWriter
}
