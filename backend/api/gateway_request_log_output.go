package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// gatewayRequestLogOutput keeps the raw route-attempt audit JSON on the
// server. The browser receives only a sanitized, structured trace so a legacy
// or malformed record cannot turn an admin detail dialog into an error dump.
type gatewayRequestLogOutput struct {
	storage.GatewayRequestLogItem
	UpstreamModelMismatch               *bool                                   `json:"upstream_model_mismatch,omitempty"`
	ModelCheckKind                      string                                  `json:"model_check_kind,omitempty"`
	CostUnavailableReason               string                                  `json:"cost_unavailable_reason,omitempty"`
	CalculatedCostUnavailableReason     string                                  `json:"calculated_cost_unavailable_reason,omitempty"`
	RouteAttempts                       []gatewayRouteAttemptOutput             `json:"route_attempts,omitempty"`
	SessionAffinityOverriddenCandidates []gatewaySessionAffinityCandidateOutput `json:"session_affinity_overridden_candidates,omitempty"`
	RouteExclusionSnapshot              *storage.GatewayRouteExclusionSnapshot  `json:"route_exclusion_snapshot,omitempty"`
	ThinkingLeakWarning                 bool                                    `json:"thinking_leak_warning,omitempty"`
	ConnectionEvent                     string                                  `json:"connection_event,omitempty"`
}

type gatewayRouteAttemptOutput struct {
	ChannelID         uint   `json:"channel_id,omitempty"`
	ChannelName       string `json:"channel_name"`
	GroupName         string `json:"group_name,omitempty"`
	Reason            string `json:"reason"`
	Summary           string `json:"summary,omitempty"`
	StatusCode        int    `json:"status_code,omitempty"`
	EligibleRank      uint   `json:"eligible_rank,omitempty"`
	Physical          bool   `json:"physically_dispatched"`
	FirstByteBudgetMs int64  `json:"first_byte_budget_ms"`
}

type gatewaySessionAffinityCandidateOutput struct {
	ChannelID    uint   `json:"channel_id"`
	ChannelName  string `json:"channel_name"`
	GroupName    string `json:"group_name"`
	EligibleRank uint   `json:"eligible_rank"`
}

func gatewayRequestLogOutputs(items []storage.GatewayRequestLogItem) []gatewayRequestLogOutput {
	outputs := make([]gatewayRequestLogOutput, 0, len(items))
	for _, item := range items {
		outputs = append(outputs, gatewayRequestLogOutputFromItem(item))
	}
	return outputs
}

func gatewayRequestLogOutputFromItem(item storage.GatewayRequestLogItem) gatewayRequestLogOutput {
	item.UpstreamResponseModel = gatewayResponseModel(item.UpstreamResponseModel)
	var modelMismatch *bool
	if item.UpstreamResponseModel != "" && strings.TrimSpace(item.ModelName) != "" {
		mismatch := !strings.EqualFold(strings.TrimSpace(item.ModelName), item.UpstreamResponseModel)
		modelMismatch = &mismatch
	}
	rawAttempts := item.RouteAttemptedChannels
	rawAffinityCandidates := item.SessionAffinityOverriddenCandidates
	rawExclusions := item.RouteExclusionsSnapshot
	thinkingLeakWarning := gatewayRouteDetailsHasThinkingWarning(item.RouteDetails)
	modelCheckKind := gatewayModelCheckKindOutput(item.RouteReason, item.RouteDetails)
	costUnavailableReason := gatewayCostUnavailableReasonOutput(item.CostUnavailableReason)
	calculatedCostUnavailableReason := gatewayCostUnavailableReasonOutput(item.CalculatedCostUnavailableReason)
	// Do not serialize legacy raw audit text alongside the safe structured form.
	// New failure summaries are sanitized when they enter each structured trace.
	item.RouteDetails = ""
	item.RouteAttemptedChannels = ""
	item.SessionAffinityOverriddenCandidates = ""
	item.CostUnavailableReason = ""
	item.CalculatedCostUnavailableReason = ""
	output := gatewayRequestLogOutput{
		ModelCheckKind:                      modelCheckKind,
		UpstreamModelMismatch:               modelMismatch,
		GatewayRequestLogItem:               item,
		CostUnavailableReason:               costUnavailableReason,
		CalculatedCostUnavailableReason:     calculatedCostUnavailableReason,
		RouteAttempts:                       gatewayRouteAttemptOutputs(rawAttempts),
		SessionAffinityOverriddenCandidates: gatewaySessionAffinityCandidateOutputs(rawAffinityCandidates),
		ThinkingLeakWarning:                 thinkingLeakWarning,
		ConnectionEvent:                     gatewayRequestConnectionEvent(item),
	}
	if snapshot, ok := storage.DecodeGatewayRouteExclusionSnapshot(item.RouteExclusionSnapshotVersion, rawExclusions); ok {
		output.RouteExclusionSnapshot = &snapshot
	}
	return output
}

func gatewayModelCheckKindOutput(reason, raw string) string {
	if reason != "model_check" || len(raw) > 4096 {
		return ""
	}
	kind := strings.TrimSpace(raw)
	var details struct {
		Kind string `json:"kind"`
	}
	if json.Unmarshal([]byte(raw), &details) == nil {
		kind = details.Kind
	}
	switch kind {
	case "pelican_svg", "candy_shapes":
		return kind
	default:
		return ""
	}
}

func gatewaySessionAffinityCandidateOutputs(raw string) []gatewaySessionAffinityCandidateOutput {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var traces []gatewaySessionAffinityCandidateOutput
	if err := json.Unmarshal([]byte(raw), &traces); err != nil || len(traces) == 0 || len(traces) > 128 {
		return nil
	}
	seen := make(map[string]struct{}, len(traces))
	for index := range traces {
		trace := &traces[index]
		trace.ChannelName = gatewayRouteTraceDisplayText(trace.ChannelName)
		trace.GroupName = gatewayRouteTraceDisplayText(trace.GroupName)
		if trace.ChannelID == 0 || trace.ChannelName == "" || trace.GroupName == "" || trace.EligibleRank == 0 || trace.EligibleRank > 100000 {
			return nil
		}
		key := fmt.Sprintf("%d:%s", trace.ChannelID, strings.ToLower(trace.GroupName))
		if _, duplicate := seen[key]; duplicate {
			return nil
		}
		seen[key] = struct{}{}
	}
	return traces
}

func gatewayCostUnavailableReasonOutput(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case storage.GatewayPricingUnavailableScopeIncomplete,
		storage.GatewayPricingUnavailableOperationAmbiguous,
		storage.GatewayPricingUnavailableOperationUnsupported,
		storage.GatewayPricingUnavailableIdentityChanged,
		storage.GatewayPricingUnavailableFactMissing,
		storage.GatewayPricingUnavailableFactExpired,
		storage.GatewayPricingUnavailableFactNotYetValid,
		storage.GatewayPricingUnavailableSchemaUnsupported,
		storage.GatewayPricingUnavailableChecksumMismatch,
		storage.GatewayPricingUnavailableUsageIncomplete,
		storage.GatewayPricingUnavailableUsageInvalid,
		storage.GatewayPricingUnavailablePriceMissing,
		storage.GatewayPricingUnavailableCurrencyUnsupported,
		storage.GatewayPricingUnavailableLegacyUnverifiedEstimate:
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "unknown"
	}
}

func gatewayRouteDetailsHasThinkingWarning(value string) bool {
	text := strings.ToLower(value)
	return strings.Contains(text, "<thinking>") || strings.Contains(text, `\u003cthinking\u003e`)
}

func gatewayRouteAttemptOutputs(raw string) []gatewayRouteAttemptOutput {
	traces, ok := gatewayRouteAttemptTraces(raw)
	if !ok {
		return nil
	}
	outputs := make([]gatewayRouteAttemptOutput, 0, len(traces))
	for _, trace := range traces {
		if !trace.Physical {
			continue
		}
		channelName := gatewayRouteTraceDisplayText(trace.ChannelName)
		if channelName == "" && trace.ChannelID == 0 {
			continue
		}
		outputs = append(outputs, gatewayRouteAttemptOutput{
			ChannelID:         trace.ChannelID,
			ChannelName:       channelName,
			GroupName:         gatewayRouteTraceDisplayText(trace.GroupName),
			Reason:            gatewayRouteAttemptOutputReason(trace.Reason),
			Summary:           gatewayRouteAttemptSummaryOutput(trace.Summary),
			StatusCode:        gatewayRouteAttemptOutputStatus(trace.StatusCode),
			EligibleRank:      gatewayRouteAttemptOutputRank(trace.EligibleRank),
			Physical:          trace.Physical,
			FirstByteBudgetMs: gatewayRouteAttemptOutputFirstByteBudget(trace.FirstByteBudgetMs),
		})
	}
	if len(outputs) == 0 {
		return nil
	}
	return outputs
}

func gatewayRouteTraceDisplayText(value string) string {
	return truncateGatewayText(sanitizeGatewayProbeError([]byte(value), ""), 160)
}

func gatewayRouteAttemptSummaryOutput(value string) string {
	return truncateGatewayText(sanitizeGatewayProbeError([]byte(value), ""), 240)
}

func gatewayRouteAttemptOutputReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case "candidate_resolution", "upstream_key", "target_resolution", "model_family", "transport", "model_unsupported", "context_limit", "response_read", "response_body_started", "response_spool_local", "response_timeout", "stream_prelude", "stream_error", "stream_empty", "stream_account_limit", "upstream_account_unavailable", "upstream_account_concurrency", "upstream_key_capacity", "upstream_status", "dispatch_audit", "mapping_runtime_fallback":
		return strings.TrimSpace(reason)
	default:
		return "unknown"
	}
}

func gatewayRouteAttemptOutputStatus(statusCode int) int {
	if statusCode < http.StatusContinue || statusCode > 599 {
		return 0
	}
	return statusCode
}

func gatewayRouteAttemptOutputRank(rank uint) uint {
	if rank > 100000 {
		return 0
	}
	return rank
}

func gatewayRouteAttemptOutputFirstByteBudget(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
