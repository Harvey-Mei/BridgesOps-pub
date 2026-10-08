package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestGatewayRequestLogOutputWhitelistsModelCheckKind(t *testing.T) {
	for _, tc := range []struct{ reason, raw, want string }{
		{"model_check", `{"kind":"pelican_svg","private":"must-not-leak"}`, "pelican_svg"},
		{"model_check", `{"kind":"candy_shapes","funding_trusted":true}`, "candy_shapes"},
		{"model_check", "pelican_svg", "pelican_svg"},
		{"model_check", "candy_shapes", "candy_shapes"},
		{"model_check", `{"kind":"must-not-leak"}`, ""},
		{"model_check", `{"kind":"pelican_svg"`, ""},
		{"smart_primary", `{"kind":"pelican_svg"}`, ""},
	} {
		output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{RouteReason: tc.reason, RouteDetails: tc.raw})
		if output.ModelCheckKind != tc.want || output.RouteDetails != "" {
			t.Fatalf("invalid kind projection: %+v", output)
		}
		raw, err := json.Marshal(output)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "must-not-leak") || strings.Contains(string(raw), "funding_trusted") {
			t.Fatal("raw audit leaked")
		}
		if tc.want != "" && !strings.Contains(string(raw), `"model_check_kind":"`+tc.want+`"`) {
			t.Fatal("missing frontend detection kind")
		}
	}
}

func TestGatewayRequestLogOutputExposesOnlySanitizedAttemptSummary(t *testing.T) {
	output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		RouteDetails: `raw route details Bearer sk-route-details-secret; response warning: detected <thinking> markup`,
		RouteAttemptedChannels: `[{
			"channel_id":7,
			"channel_name":"primary",
			"group_name":"standard",
				"reason":"upstream_status",
				"summary":"quota exhausted; Authorization: Bearer sk-attempt-secret",
				"status_code":429,
				"physically_dispatched":true,
				"first_byte_budget_ms":30000
			}]`,
	})
	if len(output.RouteAttempts) != 1 {
		t.Fatalf("route attempts = %#v, want one", output.RouteAttempts)
	}
	if !output.ThinkingLeakWarning {
		t.Fatal("thinking warning metadata was not preserved")
	}
	if got := output.RouteAttempts[0].Summary; !strings.Contains(got, "quota exhausted") || strings.Contains(got, "sk-attempt-secret") {
		t.Fatalf("sanitized summary = %q", got)
	}
	if !output.RouteAttempts[0].Physical || output.RouteAttempts[0].FirstByteBudgetMs != 30_000 {
		t.Fatalf("physical attempt telemetry = %#v", output.RouteAttempts[0])
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	body := string(encoded)
	for _, want := range []string{`"physically_dispatched":true`, `"first_byte_budget_ms":30000`} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing physical dispatch telemetry %s: %s", want, body)
		}
	}
	for _, forbidden := range []string{"raw route details", "sk-route-details-secret", "sk-attempt-secret", "route_attempted_channels"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("response leaked %q: %s", forbidden, body)
		}
	}
}

func TestGatewayRequestLogOutputIncludesCostComparisonValues(t *testing.T) {
	calculated := 1.0
	upstream := 1.5
	difference := upstream - calculated
	ratio := difference / calculated
	output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		CalculatedCost:         &calculated,
		CalculatedCostStatus:   storage.GatewayCostStatusEstimated,
		UpstreamCost:           &upstream,
		UpstreamCostStatus:     storage.GatewayCostStatusReported,
		DisplayCost:            &upstream,
		CostDifference:         &difference,
		CostDifferenceRatio:    &ratio,
		CostDifferenceMaterial: true,
	})
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	body := string(encoded)
	for _, want := range []string{
		`"calculated_cost":1`,
		`"upstream_cost":1.5`,
		`"cost_difference":0.5`,
		`"cost_difference_material":true`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %s: %s", want, body)
		}
	}
}

func TestGatewayRequestLogOutputSanitizesSessionAffinityCandidates(t *testing.T) {
	output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		SessionAffinityOverriddenCandidates: `[{"channel_id":14,"channel_name":"高优先渠道 Authorization: Bearer sk-secret","group_name":"standard","eligible_rank":1}]`,
	})
	if len(output.SessionAffinityOverriddenCandidates) != 1 {
		t.Fatalf("affinity candidates = %#v", output.SessionAffinityOverriddenCandidates)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	body := string(encoded)
	if strings.Contains(body, "sk-secret") || strings.Contains(body, "SessionAffinityOverriddenCandidates") {
		t.Fatalf("response exposed unsanitized affinity audit: %s", body)
	}
	if !strings.Contains(body, `"session_affinity_overridden_candidates":[{"channel_id":14`) {
		t.Fatalf("response omitted affinity candidates: %s", body)
	}
}

func TestGatewayRequestLogOutputClassifiesConnectionEvents(t *testing.T) {
	tests := []struct {
		name string
		item storage.GatewayRequestLogItem
		want string
	}{
		{
			name: "client disconnect",
			item: storage.GatewayRequestLogItem{StatusCode: 499, RouteReason: "client_canceled"},
			want: gatewayConnectionEventClientDisconnect,
		},
		{
			name: "upstream interruption after acceptance",
			item: storage.GatewayRequestLogItem{
				StatusCode:          http.StatusBadGateway,
				UpstreamAccepted:    true,
				TerminalFailureCode: "upstream_5xx",
			},
			want: gatewayConnectionEventUpstreamInterrupted,
		},
		{
			name: "dispatch recovery",
			item: storage.GatewayRequestLogItem{StatusCode: http.StatusBadGateway, RouteReason: "dispatch_recovery"},
			want: gatewayConnectionEventDispatchRecovery,
		},
		{
			name: "pre acceptance failure is not a connection event",
			item: storage.GatewayRequestLogItem{StatusCode: http.StatusBadGateway, TerminalFailureCode: "retry_prompt_after_exhaustion"},
			want: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := gatewayRequestLogOutputFromItem(test.item)
			if output.ConnectionEvent != test.want {
				t.Fatalf("connection event = %q, want %q", output.ConnectionEvent, test.want)
			}
		})
	}
}

func TestGatewayRequestLogOutputRejectsLegacyAttemptsWithoutPhysicalEvidence(t *testing.T) {
	output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		RouteAttemptedChannels: `[
			{"channel_id":7,"channel_name":"legacy","group_name":"standard","reason":"upstream_status","status_code":503},
			{"channel_id":8,"channel_name":"prepared","group_name":"standard","reason":"upstream_key","physically_dispatched":false}
		]`,
	})
	if len(output.RouteAttempts) != 0 {
		t.Fatalf("legacy or preparation-only attempts were exposed as physical failures: %#v", output.RouteAttempts)
	}
}

func TestGatewayRequestLogOutputSanitizesUnavailablePricingReasonAndKeepsFactMetadata(t *testing.T) {
	factID := uint64(91)
	observedAt := time.Date(2026, 7, 29, 6, 7, 8, 0, time.UTC)
	output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		CostStatus:             storage.GatewayCostStatusUnavailable,
		CostUnavailableReason:  storage.GatewayPricingUnavailableFactMissing,
		PriceFactID:            &factID,
		PriceFactVersion:       4,
		PriceFactSchemaVersion: 1,
		PriceCostUnitEpoch:     12,
		CostBasisSource:        "confirmed_recharge",
		PriceModelName:         "gpt-test",
		PriceOperation:         "text_generation",
		PriceProtocol:          "openai_chat_completions",
		PriceCurrency:          "USD",
		PriceUnit:              "per_million_tokens",
		PriceSource:            "provider_catalog",
		PriceObservedAt:        &observedAt,
		UsageSchema:            "token_components_v1",
		UsageSchemaVersion:     1,
	})
	if output.CostUnavailableReason != storage.GatewayPricingUnavailableFactMissing {
		t.Fatalf("unavailable reason = %q", output.CostUnavailableReason)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	body := string(encoded)
	for _, want := range []string{
		`"cost_status":"unavailable"`,
		`"cost_unavailable_reason":"fact_missing"`,
		`"price_fact_id":91`,
		`"price_fact_version":4`,
		`"price_cost_unit_epoch":12`,
		`"cost_basis_source":"confirmed_recharge"`,
		`"price_model_name":"gpt-test"`,
		`"price_source":"provider_catalog"`,
		`"price_observed_at":"2026-07-29T06:07:08Z"`,
		`"usage_schema":"token_components_v1"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %s: %s", want, body)
		}
	}

	unknown := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		CostStatus:            storage.GatewayCostStatusUnavailable,
		CostUnavailableReason: "Bearer should-never-reach-browser",
	})
	if unknown.CostUnavailableReason != "unknown" {
		t.Fatalf("unknown unavailable reason = %q", unknown.CostUnavailableReason)
	}
}

func TestGatewayRequestLogOutputSanitizesCalculatedCostUnavailableReason(t *testing.T) {
	output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		CalculatedCostStatus:            storage.GatewayCostStatusUnavailable,
		CalculatedCostUnavailableReason: "Bearer should-never-reach-browser",
	})
	if output.CalculatedCostUnavailableReason != "unknown" || output.GatewayRequestLogItem.CalculatedCostUnavailableReason != "" {
		t.Fatalf("calculated unavailable reason was not sanitized: %+v", output)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	body := string(encoded)
	if strings.Contains(body, "should-never-reach-browser") || !strings.Contains(body, `"calculated_cost_unavailable_reason":"unknown"`) {
		t.Fatalf("calculated unavailable reason output = %s", body)
	}

	known := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		CalculatedCostStatus:            storage.GatewayCostStatusUnavailable,
		CalculatedCostUnavailableReason: storage.GatewayPricingUnavailableUsageIncomplete,
	})
	if known.CalculatedCostUnavailableReason != storage.GatewayPricingUnavailableUsageIncomplete {
		t.Fatalf("known calculated unavailable reason = %q", known.CalculatedCostUnavailableReason)
	}
}

func TestGatewayRequestLogOutputExposesOnlyValidatedRouteExclusionSnapshot(t *testing.T) {
	capturedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	cooldownUntil := capturedAt.Add(10 * time.Minute)
	raw, err := storage.NewGatewayRouteExclusionSnapshot(capturedAt, []storage.GatewayRouteExclusionSnapshotEntry{{
		ChannelID:          17,
		GroupName:          "standard",
		Reason:             "upstream_temporarily_unavailable",
		UpstreamGroupID:    91,
		PresenceGeneration: 4,
		FailureCount:       3,
		CooldownUntil:      &cooldownUntil,
	}})
	if err != nil {
		t.Fatalf("build route exclusion snapshot: %v", err)
	}
	output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		RouteExclusionSnapshotVersion: storage.GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:       raw,
	})
	if output.RouteExclusionSnapshot == nil || len(output.RouteExclusionSnapshot.Entries) != 1 {
		t.Fatalf("safe route exclusion output = %+v", output.RouteExclusionSnapshot)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal route exclusion output: %v", err)
	}
	body := string(encoded)
	for _, forbidden := range []string{"route_exclusions_snapshot", `"detail":`, "raw error", "Bearer", "sk-secret"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("route exclusion output leaked %q: %s", forbidden, body)
		}
	}

	malformed := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		RouteExclusionSnapshotVersion: storage.GatewayRouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:       strings.Replace(raw, `"entries":[`, `"secret":"Bearer sk-secret","entries":[`, 1),
	})
	if malformed.RouteExclusionSnapshot != nil {
		t.Fatalf("malformed route exclusion snapshot was exposed: %+v", malformed.RouteExclusionSnapshot)
	}
	historical := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{RouteExclusionsSnapshot: raw})
	if historical.RouteExclusionSnapshot != nil {
		t.Fatalf("historical version zero snapshot was inferred: %+v", historical.RouteExclusionSnapshot)
	}
}

func TestGatewayRequestLogOutputKeepsConcreteFailureSummaries(t *testing.T) {
	candidate := gateway.Candidate{
		Channel: storage.Channel{ID: 7, Name: "primary"},
		Rate:    storage.RateSnapshot{ModelName: "standard"},
	}
	decision := &gatewayRouteDecision{
		SmartRouting:     true,
		Candidates:       []gateway.Candidate{candidate},
		RankedCandidates: []gateway.Candidate{candidate},
	}
	tests := []struct {
		name       string
		reason     string
		statusCode int
		detail     string
		want       string
	}{
		{name: "timeout", reason: "response_timeout", statusCode: http.StatusGatewayTimeout, detail: "upstream response exceeded 30s first-byte timeout", want: "30s first-byte timeout"},
		{name: "transport", reason: "transport", detail: "dial tcp: connection reset by peer; Authorization: Bearer sk-super-secret-token; https://user:pass@example.test/v1?token=query-secret", want: "connection reset by peer"},
		{name: "key", reason: "upstream_key", detail: "managed upstream key is still provisioning", want: "still provisioning"},
		{name: "target", reason: "target_resolution", detail: "upstream target URL is not configured", want: "not configured"},
		{name: "401", reason: "upstream_status", statusCode: http.StatusUnauthorized, detail: "authentication failed: invalid API key", want: "authentication failed"},
		{name: "403", reason: "upstream_status", statusCode: http.StatusForbidden, detail: "permission denied for requested workspace", want: "permission denied"},
		{name: "429", reason: "upstream_status", statusCode: http.StatusTooManyRequests, detail: "rate limit exceeded; retry later", want: "rate limit exceeded"},
		{name: "5xx", reason: "upstream_status", statusCode: http.StatusServiceUnavailable, detail: "upstream service overloaded", want: "service overloaded"},
		{name: "json", reason: "stream_error", statusCode: http.StatusOK, detail: "JSON error: overloaded_error", want: "overloaded_error"},
		{name: "sse", reason: "stream_error", statusCode: http.StatusServiceUnavailable, detail: "SSE api_error before useful output", want: "SSE api_error"},
		{name: "context", reason: "context_limit", statusCode: http.StatusBadRequest, detail: "request input 120000 exceeds learned safe budget 100000", want: "safe budget 100000"},
		{name: "model", reason: "model_unsupported", statusCode: http.StatusNotFound, detail: "requested model is not supported by this group", want: "not supported"},
		{name: "read", reason: "response_read", detail: "unexpected EOF while reading response body", want: "unexpected EOF"},
		{name: "body started", reason: "response_body_started", detail: "upstream response body started before a projection failure", want: "body started"},
		{name: "local spool", reason: "response_spool_local", detail: "local response spool unavailable after physical dispatch", want: "local response spool"},
		{name: "stream prelude", reason: "stream_prelude", detail: "unexpected EOF before the first SSE frame", want: "first SSE frame"},
		{name: "bounded", reason: "transport", detail: strings.Repeat("bounded failure detail ", 40), want: "bounded failure detail"},
		{name: "default fallback", reason: "stream_empty", statusCode: http.StatusBadGateway, want: "stream ended before useful output"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempt := newGatewayRouteAttemptFromDispatch(&gatewayDispatchAuditState{id: 1, physicallyDispatched: true, firstByteBudgetMs: 30_000}, candidate, test.reason, test.statusCode, test.detail)
			rawAttempts := gatewayRouteAttemptedChannels(decision, []gatewayRouteAttempt{attempt}, gateway.Candidate{})
			output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
				RouteDetails:           "legacy raw details must be cleared",
				RouteAttemptedChannels: rawAttempts,
			})
			if output.RouteDetails != "" || output.RouteAttemptedChannels != "" {
				t.Fatalf("raw route fields were exposed: %+v", output.GatewayRequestLogItem)
			}
			if len(output.RouteAttempts) != 1 {
				t.Fatalf("route attempts = %#v, want one", output.RouteAttempts)
			}
			if !output.RouteAttempts[0].Physical || output.RouteAttempts[0].FirstByteBudgetMs != 30_000 {
				t.Fatalf("route attempt telemetry = %#v", output.RouteAttempts[0])
			}
			summary := output.RouteAttempts[0].Summary
			if !strings.Contains(summary, test.want) {
				t.Fatalf("summary = %q, want concrete detail %q", summary, test.want)
			}
			if len([]rune(summary)) > 240 {
				t.Fatalf("summary length = %d, want <= 240", len([]rune(summary)))
			}
			for _, secret := range []string{"sk-super-secret-token", "Authorization", "Bearer", "user:pass", "query-secret"} {
				if strings.Contains(summary, secret) {
					t.Fatalf("summary leaked %q: %q", secret, summary)
				}
			}
		})
	}
}
