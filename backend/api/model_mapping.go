package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/clauderadar"
	"github.com/Harvey-Mei/BridgeOps/backend/codexradar"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

var errClaudeRadarModelBindingRequired = errors.New("claude radar configuration is not bound to an API model")

type gatewayModelMappingResult struct {
	Strategy                 gateway.ModelMappingStrategy
	RequestedModel           string
	RequestedReasoningEffort string
	Model                    string
	ReasoningEffort          string
	Body                     []byte
	RequestedBody            []byte
	Details                  string
	Applied                  bool
}

func newGatewayModelMappingResult(model, reasoningEffort string, body []byte) gatewayModelMappingResult {
	return gatewayModelMappingResult{
		Strategy:                 gateway.ModelMappingStrategyDisabled,
		RequestedModel:           strings.TrimSpace(model),
		RequestedReasoningEffort: strings.TrimSpace(reasoningEffort),
		Model:                    strings.TrimSpace(model),
		ReasoningEffort:          strings.TrimSpace(reasoningEffort),
		// The gateway owns body for the handler lifetime and treats it as
		// immutable. Borrow it here instead of retaining two full copies. A
		// separate requested body is retained only after a rewrite actually
		// creates a second backing array.
		Body: body,
	}
}

func (result gatewayModelMappingResult) enabled() bool {
	return result.Strategy != gateway.ModelMappingStrategyDisabled
}

func (result gatewayModelMappingResult) applyToLog(log *storage.GatewayRequestLog) {
	if log == nil || !result.enabled() {
		return
	}
	log.RequestedModelName = result.RequestedModel
	log.RequestedReasoningEffort = result.RequestedReasoningEffort
	log.ModelMappingStrategy = string(result.Strategy)
	log.ModelMappingDetails = result.Details
}

func resolveGatewayModelMapping(d *Deps, downstream *storage.GatewayDownstreamKey, result gatewayModelMappingResult, path string, candidateGroups ...*storage.GatewayCandidateGroup) (gatewayModelMappingResult, error) {
	if downstream == nil {
		return result, errors.New("missing downstream key")
	}
	strategy, err := gateway.NormalizeModelMappingStrategy(downstream.ModelMappingStrategy)
	if err != nil {
		return result, err
	}
	result.Strategy = strategy
	if strategy == gateway.ModelMappingStrategyDisabled {
		return result, nil
	}
	var candidateGroup *storage.GatewayCandidateGroup
	if len(candidateGroups) > 0 {
		candidateGroup = candidateGroups[0]
	}
	selected, source, err := gatewayModelMappingSelection(d, downstream, strategy, candidateGroup)
	if err != nil {
		return result, err
	}
	if !gatewayMappedModelAllowedForCandidateGroup(d, downstream, candidateGroup, selected.Model) {
		return result, fmt.Errorf("mapped model %s is not allowed for this downstream key", selected.Model)
	}
	requestedBody := result.Body
	body, err := rewriteGatewayRequestForMapping(requestedBody, path, downstream.App, selected.Model, selected.ReasoningEffort)
	if err != nil {
		return result, err
	}
	actualReasoningEffort := gatewayRequestReasoningEffort(body)
	if !strings.EqualFold(actualReasoningEffort, selected.ReasoningEffort) {
		return result, errors.New("failed to enforce mapped reasoning effort")
	}
	result.Model = strings.TrimSpace(selected.Model)
	result.ReasoningEffort = actualReasoningEffort
	result.RequestedBody = requestedBody
	result.Body = body
	result.Applied = true
	result.Details = formatGatewayModelMappingDetails(strategy, source, result.RequestedModel, result.RequestedReasoningEffort, result.Model, selected.ReasoningEffort, result.ReasoningEffort)
	return result, nil
}

// gatewayDownstreamAllowsMappedModel is the shared admission check for every
// server-selected mapping. Keeping preview and forwarding on this one signal
// chain prevents a radar result from looking resolved in management while the
// data plane must reject it.
func gatewayDownstreamAllowsMappedModel(downstream *storage.GatewayDownstreamKey, model string) bool {
	if downstream == nil {
		return false
	}
	return gatewayAppAllowsModel(downstream.App, model) &&
		gatewayModelFamilyAllowsModel(downstream.ModelFamily, downstream.App, model)
}

func gatewayMappedModelAllowedForCandidateGroup(d *Deps, downstream *storage.GatewayDownstreamKey, group *storage.GatewayCandidateGroup, model string) bool {
	if gatewayDownstreamAllowsMappedModel(downstream, model) {
		return true
	}
	if d == nil || d.Channels == nil || d.Gateway == nil || d.Gateway.Store == nil || downstream == nil {
		return false
	}
	if group == nil {
		if !downstream.SmartRouting {
			if downstream.ManualChannelID == 0 {
				return false
			}
			channel, err := d.Channels.FindByID(downstream.ManualChannelID)
			return err == nil && directGatewayProjectedRouteAllowsModel(d, *channel, downstream.ManualGroupName, downstream.App, model)
		}
		if downstream.CandidateGroupID > 0 {
			var err error
			group, err = d.Gateway.Store.FindCandidateGroup(downstream.CandidateGroupID)
			if err != nil {
				return false
			}
		} else {
			return directGatewayModelConfigured(d, downstream, model)
		}
	}
	for _, member := range group.Members {
		channel, err := d.Channels.FindByID(member.ChannelID)
		if err == nil && directGatewayProjectedRouteAllowsModel(d, *channel, member.GroupName, downstream.App, model) {
			return true
		}
	}
	return false
}

func gatewayModelMappingSelection(d *Deps, downstream *storage.GatewayDownstreamKey, strategy gateway.ModelMappingStrategy, candidateGroup *storage.GatewayCandidateGroup) (gateway.ModelMappingOption, string, error) {
	if downstream == nil {
		return gateway.ModelMappingOption{}, "", errors.New("missing downstream key")
	}
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		return gateway.ModelMappingOption{}, "", errors.New("gateway store is unavailable")
	}
	if downstream.CandidateGroupID == 0 {
		return gateway.ModelMappingOption{}, "", errors.New("model mapping requires a candidate group")
	}
	group := candidateGroup
	if group == nil {
		loaded, err := d.Gateway.Store.FindCandidateGroup(downstream.CandidateGroupID)
		if err != nil {
			return gateway.ModelMappingOption{}, "", err
		}
		group = loaded
	}
	family, err := gateway.NormalizeModelMappingFamily(group.ModelMappingFamily)
	if err != nil || family == "" {
		return gateway.ModelMappingOption{}, "", errors.New("candidate group has no model mapping radar family")
	}
	if downstream.App != storage.GatewayApp(family) || !strings.EqualFold(strings.TrimSpace(downstream.ModelFamily), family) {
		return gateway.ModelMappingOption{}, "", errors.New("candidate group radar family does not match downstream key")
	}
	if strategy == gateway.ModelMappingStrategyManual {
		selected, err := gateway.SelectModelMappingOption(strategy, nil, downstream.ModelMappingModel, downstream.ModelMappingReasoningEffort)
		if err != nil {
			return gateway.ModelMappingOption{}, "手动设置", err
		}
		selected, err = forceGatewayModelMappingReasoningEffort(downstream, strategy, selected)
		return selected, "手动设置", err
	}
	options, source, err := gatewayModelMappingOptions(d, family)
	if err != nil {
		return gateway.ModelMappingOption{}, source, err
	}
	selected, err := gateway.SelectModelMappingOption(strategy, options, downstream.ModelMappingModel, downstream.ModelMappingReasoningEffort)
	if err != nil {
		return gateway.ModelMappingOption{}, source, err
	}
	// Claude Radar exposes opaque comparison keys and display labels, not API
	// model IDs. Keep the selected reference for the preview, but never send the
	// opaque key upstream until an explicit binding exists.
	if family == string(storage.GatewayAppClaude) {
		return selected, source, errClaudeRadarModelBindingRequired
	}
	selected, err = forceGatewayModelMappingReasoningEffort(downstream, strategy, selected)
	return selected, source, err
}

// forceGatewayModelMappingReasoningEffort makes every enabled mapping carry a
// concrete, protocol-valid reasoning setting. Automatic policies first use the
// selected radar configuration and then a conservative family default.
func forceGatewayModelMappingReasoningEffort(downstream *storage.GatewayDownstreamKey, strategy gateway.ModelMappingStrategy, selected gateway.ModelMappingOption) (gateway.ModelMappingOption, error) {
	if downstream == nil {
		return gateway.ModelMappingOption{}, errors.New("missing downstream key")
	}
	effort := strings.TrimSpace(selected.ReasoningEffort)
	if effort == "" {
		if strategy == gateway.ModelMappingStrategyManual {
			return gateway.ModelMappingOption{}, errors.New("manual model mapping requires a reasoning effort")
		}
		effort = gateway.DefaultModelMappingReasoningEffort(downstream.App)
	}
	effort, err := gateway.NormalizeModelMappingReasoningEffort(downstream.App, effort)
	if err != nil {
		return gateway.ModelMappingOption{}, err
	}
	selected.ReasoningEffort = effort
	return selected, nil
}

func gatewayModelMappingPreview(d *Deps, downstream *storage.GatewayDownstreamKey, candidateGroup *storage.GatewayCandidateGroup) *storage.GatewayModelMappingPreview {
	if downstream == nil {
		return nil
	}
	strategy, err := gateway.NormalizeModelMappingStrategy(downstream.ModelMappingStrategy)
	if err != nil || strategy == gateway.ModelMappingStrategyDisabled {
		return nil
	}
	preview := &storage.GatewayModelMappingPreview{Strategy: string(strategy)}
	selected, source, err := gatewayModelMappingSelection(d, downstream, strategy, candidateGroup)
	preview.ConfigKey = strings.TrimSpace(selected.ConfigKey)
	preview.Label = strings.TrimSpace(selected.Label)
	preview.Source = source
	if err != nil {
		preview.Error = err.Error()
		return preview
	}
	preview.Model = strings.TrimSpace(selected.Model)
	preview.ReasoningEffort = strings.TrimSpace(selected.ReasoningEffort)
	return preview
}

// gatewayCandidatesWithModelMapping deliberately resolves ordinary candidate
// order before selecting a radar model. Mapping can only remove incompatible
// candidates from that order; it never recalculates price or latency scores.
func gatewayCandidatesWithModelMapping(d *Deps, downstream *storage.GatewayDownstreamKey, model, reasoningEffort string, body []byte, path, sessionHash string) (*gatewayRouteDecision, gatewayModelMappingResult, error) {
	result := newGatewayModelMappingResult(model, reasoningEffort, body)
	if downstream == nil {
		return nil, result, errors.New("missing downstream key")
	}
	if !gatewayModelMappingAllowedPath(path) {
		decision, candidateErr := gatewayCandidatesForDownstream(d, downstream, model, sessionHash)
		return decision, result, candidateErr
	}
	strategy, err := gateway.NormalizeModelMappingStrategy(downstream.ModelMappingStrategy)
	if err != nil {
		return nil, result, err
	}
	result.Strategy = strategy
	if strategy == gateway.ModelMappingStrategyDisabled {
		decision, candidateErr := gatewayCandidatesForDownstream(d, downstream, model, sessionHash)
		return decision, result, candidateErr
	}
	decision, candidateGroup, candidateErr := gatewayCandidatesForStableCandidateGroup(d, downstream, sessionHash)
	if candidateErr != nil {
		return decision, gatewayModelMappingFailure(result, candidateErr), candidateErr
	}
	decision.PreMappingCandidates = append([]gateway.Candidate(nil), decision.RankedCandidates...)
	mapped, mappingErr := resolveGatewayModelMapping(d, downstream, result, path, candidateGroup)
	if mappingErr != nil {
		return decision, gatewayModelMappingFailure(mapped, mappingErr), mappingErr
	}
	if !downstream.SmartRouting {
		// A fixed route remains a single-target passthrough, but an explicit
		// mapping changes that target request's model. Re-resolve the exact route
		// against the mapped model so confirmed protocol/model incompatibilities
		// cannot be bypassed through the earlier requested-model snapshot. The
		// service deliberately still permits unknown or stale facts here.
		candidates, candidateErr := d.Gateway.ManualCandidateForApp(
			downstream.ID, downstream.App, downstream.ManualChannelID, downstream.ManualGroupName, mapped.Model,
		)
		if candidateErr != nil {
			return decision, gatewayModelMappingFailure(mapped, candidateErr), candidateErr
		}
		decision.Candidates = candidates
		decision.RankedCandidates = append([]gateway.Candidate(nil), candidates...)
		decision.RequestedModel = mapped.Model
		return decision, gatewayModelMappingFinalized(mapped), nil
	}
	compatible, exclusions, filterErr := d.Gateway.FilterCandidatesForMappedModel(decision.Candidates, mapped.Model)
	if filterErr != nil {
		return decision, gatewayModelMappingFailure(mapped, filterErr), filterErr
	}
	if len(compatible) == 0 {
		cause := fmt.Errorf("mapped model %q is not supported by any selected candidate", mapped.Model)
		if !gatewayMappedModelIsConfirmedUnsupported(exclusions) {
			return decision, gatewayModelMappingFailure(mapped, cause), cause
		}
		if err := d.Gateway.Store.DisableDownstreamKeyModelMappingIfCurrent(downstream, candidateGroup); err != nil {
			return decision, gatewayModelMappingFailure(mapped, fmt.Errorf("disable unsupported model mapping: %w", err)), err
		}
		downstream.ModelMappingStrategy = string(gateway.ModelMappingStrategyDisabled)
		fallback, err := gatewayCandidatesForDownstream(d, downstream, result.RequestedModel, sessionHash)
		if err != nil {
			return decision, gatewayModelMappingAutoDisabled(mapped, cause), err
		}
		return fallback, gatewayModelMappingAutoDisabled(mapped, cause), nil
	}
	decision.Candidates = compatible
	decision.RankedCandidates = append([]gateway.Candidate(nil), compatible...)
	decision.Exclusions = append(decision.Exclusions, exclusions...)
	decision.RequestedModel = mapped.Model
	ordered, origin := gatewayCacheAffinityCandidatesWithOrigin(d, downstream, mapped.Model, sessionHash, decision.Candidates)
	decision.Candidates = ordered
	gatewaySetSessionAffinityDecision(d, decision, origin)
	return decision, gatewayModelMappingFinalized(mapped), nil
}

const gatewayCandidateGroupSnapshotAttempts = 3

// gatewayCandidatesForStableCandidateGroup keeps the candidate list and the
// group version used for mapping decisions on one membership snapshot. The
// candidate service reads the group internally, so a before/after version
// check is required to detect a membership write racing with that read.
func gatewayCandidatesForStableCandidateGroup(d *Deps, downstream *storage.GatewayDownstreamKey, sessionHash string) (*gatewayRouteDecision, *storage.GatewayCandidateGroup, error) {
	if downstream == nil || downstream.CandidateGroupID == 0 {
		return nil, nil, errors.New("model mapping requires a candidate group")
	}
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		return nil, nil, errors.New("gateway store is unavailable")
	}
	for attempt := 0; attempt < gatewayCandidateGroupSnapshotAttempts; attempt++ {
		before, err := d.Gateway.Store.FindCandidateGroup(downstream.CandidateGroupID)
		if err != nil {
			return nil, nil, err
		}
		decision, err := gatewayCandidatesForDownstream(d, downstream, "", sessionHash)
		if err != nil {
			return decision, before, err
		}
		after, err := d.Gateway.Store.FindCandidateGroup(downstream.CandidateGroupID)
		if err != nil {
			return decision, before, err
		}
		if before.ID == after.ID && before.MembershipVersion == after.MembershipVersion {
			return decision, before, nil
		}
	}
	return nil, nil, errors.New("candidate group changed while building model mapping")
}

// Only disable a mapping after synchronized capability data positively rejects
// the mapped model. A temporary outage or missing capability sync should not
// silently change the configured routing policy.
func gatewayMappedModelIsConfirmedUnsupported(exclusions []gateway.CandidateExclusion) bool {
	if len(exclusions) == 0 {
		return false
	}
	for _, exclusion := range exclusions {
		if exclusion.Reason != "model_not_listed" && exclusion.Reason != "model_not_supported" {
			return false
		}
	}
	return true
}

func gatewayModelMappingAutoDisabled(result gatewayModelMappingResult, cause error) gatewayModelMappingResult {
	result.Applied = false
	result.Model = result.RequestedModel
	result.ReasoningEffort = result.RequestedReasoningEffort
	if result.RequestedBody != nil {
		result.Body = result.RequestedBody
	}
	result.RequestedBody = nil
	result.Details = fmt.Sprintf("%s 自动停用：%s；本次沿用请求模型", modelMappingStrategyLabel(result.Strategy), strings.TrimSpace(cause.Error()))
	return result
}

func gatewayModelMappingFailure(result gatewayModelMappingResult, cause error) gatewayModelMappingResult {
	result.Applied = false
	result.Details = modelMappingFailureDetails(result, cause)
	return gatewayModelMappingFinalized(result)
}

// gatewayModelMappingFinalized drops the original body once no later branch
// can fall back from an executable mapping to the requested payload. Keeping
// only the body that will be forwarded prevents a long response stream from
// pinning both large backing arrays.
func gatewayModelMappingFinalized(result gatewayModelMappingResult) gatewayModelMappingResult {
	result.RequestedBody = nil
	return result
}

func gatewayModelMappingOptions(d *Deps, family string) ([]gateway.ModelMappingOption, string, error) {
	switch family {
	case string(storage.GatewayAppCodex):
		if d == nil || d.CodexRadar == nil {
			return nil, "", errors.New("codex radar is unavailable")
		}
		options := codexRadarMappingOptions(d.CodexRadar.Snapshot())
		if len(options) == 0 {
			return nil, "", errors.New("codex radar has no model configurations")
		}
		return options, "Codex Radar", nil
	case string(storage.GatewayAppClaude):
		if d == nil || d.ClaudeRadar == nil {
			return nil, "", errors.New("claude radar is unavailable")
		}
		options := claudeRadarMappingOptions(d.ClaudeRadar.Snapshot())
		if len(options) == 0 {
			return nil, "", errors.New("claude radar has no model configurations")
		}
		return options, "Claude Radar", nil
	default:
		return nil, "", errors.New("unsupported model mapping radar family")
	}
}

func codexRadarMappingOptions(snapshot codexradar.Snapshot) []gateway.ModelMappingOption {
	var payload struct {
		ModelIQ struct {
			Comparisons map[string]struct {
				Label  string `json:"label"`
				Model  string `json:"model"`
				Latest *struct {
					Model           string   `json:"model"`
					ReasoningEffort string   `json:"reasoning_effort"`
					Score           *float64 `json:"score"`
					CostUSD         *float64 `json:"cost_usd"`
					WallSeconds     *float64 `json:"wall_seconds"`
				} `json:"latest"`
			} `json:"comparisons"`
		} `json:"model_iq"`
	}
	if len(snapshot.Payload) > 0 && json.Unmarshal(snapshot.Payload, &payload) == nil {
		options := make([]gateway.ModelMappingOption, 0, len(payload.ModelIQ.Comparisons))
		for key, comparison := range payload.ModelIQ.Comparisons {
			if comparison.Latest == nil {
				continue
			}
			model := strings.TrimSpace(comparison.Latest.Model)
			if model == "" {
				model = strings.TrimSpace(comparison.Model)
			}
			options = append(options, gateway.ModelMappingOption{
				ConfigKey:       key,
				Label:           comparison.Label,
				Model:           model,
				ReasoningEffort: comparison.Latest.ReasoningEffort,
				Score:           comparison.Latest.Score,
				CostUSD:         comparison.Latest.CostUSD,
				WallSeconds:     comparison.Latest.WallSeconds,
			})
		}
		if len(options) > 0 {
			return sortGatewayModelMappingOptions(options)
		}
	}
	latest := make(map[string]codexradar.HistoryPoint)
	for _, point := range snapshot.History {
		key := strings.TrimSpace(point.ConfigKey)
		if key == "" {
			key = strings.TrimSpace(point.Model) + "\x00" + strings.TrimSpace(point.ReasoningEffort)
		}
		if previous, ok := latest[key]; !ok || point.ObservedAt.After(previous.ObservedAt) {
			latest[key] = point
		}
	}
	options := make([]gateway.ModelMappingOption, 0, len(latest))
	for key, point := range latest {
		options = append(options, gateway.ModelMappingOption{ConfigKey: key, Label: point.Label, Model: point.Model, ReasoningEffort: point.ReasoningEffort, Score: point.Score, CostUSD: point.CostUSD, WallSeconds: point.WallSeconds})
	}
	return sortGatewayModelMappingOptions(options)
}

func claudeRadarMappingOptions(snapshot clauderadar.Snapshot) []gateway.ModelMappingOption {
	var payload struct {
		IQ struct {
			Models []struct {
				Key            string     `json:"key"`
				Name           string     `json:"name"`
				HistoricalOnly bool       `json:"historical_only"`
				Score          *float64   `json:"score"`
				Cost           []*float64 `json:"cost"`
				Time           []*float64 `json:"time"`
			} `json:"models"`
		} `json:"iq"`
	}
	if len(snapshot.Payload) > 0 && json.Unmarshal(snapshot.Payload, &payload) == nil {
		options := make([]gateway.ModelMappingOption, 0, len(payload.IQ.Models))
		for _, model := range payload.IQ.Models {
			if model.HistoricalOnly {
				continue
			}
			key := strings.TrimSpace(model.Key)
			if key == "" {
				continue
			}
			var wallSeconds *float64
			if hours := lastRadarNumber(model.Time); hours != nil {
				seconds := *hours * 3600
				wallSeconds = &seconds
			}
			options = append(options, gateway.ModelMappingOption{
				ConfigKey:   key,
				Label:       model.Name,
				Model:       key,
				Score:       model.Score,
				CostUSD:     lastRadarNumber(model.Cost),
				WallSeconds: wallSeconds,
			})
		}
		if len(options) > 0 {
			return sortGatewayModelMappingOptions(options)
		}
	}
	return nil
}

func lastRadarNumber(values []*float64) *float64 {
	for index := len(values) - 1; index >= 0; index-- {
		if values[index] == nil {
			continue
		}
		value := *values[index]
		return &value
	}
	return nil
}

func sortGatewayModelMappingOptions(options []gateway.ModelMappingOption) []gateway.ModelMappingOption {
	filtered := make([]gateway.ModelMappingOption, 0, len(options))
	seen := make(map[string]struct{}, len(options))
	for _, option := range options {
		option.Model = strings.TrimSpace(option.Model)
		if option.Model == "" {
			continue
		}
		key := strings.TrimSpace(option.ConfigKey)
		if key == "" {
			key = option.Model + "\x00" + strings.TrimSpace(option.ReasoningEffort)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		filtered = append(filtered, option)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return strings.ToLower(filtered[i].ConfigKey+"\x00"+filtered[i].Model+"\x00"+filtered[i].ReasoningEffort) < strings.ToLower(filtered[j].ConfigKey+"\x00"+filtered[j].Model+"\x00"+filtered[j].ReasoningEffort)
	})
	return filtered
}

func rewriteGatewayRequestForMapping(body []byte, path string, app storage.GatewayApp, model, reasoningEffort string) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		return nil, errors.New("request body must be a JSON object")
	}
	payload["model"] = strings.TrimSpace(model)
	effort := strings.TrimSpace(reasoningEffort)
	if effort == "" {
		return nil, errors.New("mapped reasoning effort is required")
	}
	switch app {
	case storage.GatewayAppClaude:
		budget, ok := gateway.ClaudeThinkingBudget(effort)
		if !ok {
			return nil, errors.New("claude mapped thinking budget must be a positive integer")
		}
		thinking, ok := objectValue(payload, "thinking")
		if !ok {
			thinking = map[string]any{}
		}
		thinking["type"] = "enabled"
		thinking["budget_tokens"] = budget
		payload["thinking"] = thinking
	case storage.GatewayAppCodex:
		// Preserve a client-provided representation when possible, but make all
		// present reasoning controls agree with the selected mapping. For a
		// bare request, write the canonical field for the target endpoint.
		if _, ok := payload["reasoning_effort"]; ok || !strings.HasPrefix(path, "/v1/responses") {
			payload["reasoning_effort"] = effort
		}
		if _, ok := payload["reasoningEffort"]; ok {
			payload["reasoningEffort"] = effort
		}
		if reasoning, ok := objectValue(payload, "reasoning"); ok {
			reasoning["effort"] = effort
			payload["reasoning"] = reasoning
		} else if strings.HasPrefix(path, "/v1/responses") {
			payload["reasoning"] = map[string]any{"effort": effort}
		}
		if thinking, ok := objectValue(payload, "thinking"); ok {
			thinking["effort"] = effort
			if thinking["level"] != nil {
				thinking["level"] = effort
			}
			if thinking["type"] != nil {
				thinking["type"] = "enabled"
			}
			if thinking["enabled"] != nil {
				thinking["enabled"] = true
			}
			delete(thinking, "budget_tokens")
			payload["thinking"] = thinking
		}
	default:
		return nil, errors.New("model mapping reasoning is unsupported for this gateway app")
	}
	return json.Marshal(payload)
}

func gatewayModelMappingAllowedPath(path string) bool {
	switch path {
	case "/v1/chat/completions", "/v1/completions", "/v1/responses", "/v1/messages":
		return true
	default:
		return false
	}
}

func formatGatewayModelMappingDetails(strategy gateway.ModelMappingStrategy, source, requestedModel, requestedReasoningEffort, selectedModel, selectedReasoningEffort, actualReasoningEffort string) string {
	label := map[gateway.ModelMappingStrategy]string{
		gateway.ModelMappingStrategyIQ:       "智商优先",
		gateway.ModelMappingStrategyCost:     "费用优先",
		gateway.ModelMappingStrategyTime:     "耗时优先",
		gateway.ModelMappingStrategyBalanced: "综合平衡",
		gateway.ModelMappingStrategyManual:   "手动固定",
	}[strategy]
	requested := strings.TrimSpace(requestedModel)
	if requestedReasoningEffort != "" {
		requested += " / " + requestedReasoningEffort
	}
	selected := strings.TrimSpace(selectedModel)
	if actualReasoningEffort != "" {
		selected += " / " + actualReasoningEffort
	}
	detail := fmt.Sprintf("%s（%s）：%s -> %s", label, source, requested, selected)
	if selectedReasoningEffort != "" && actualReasoningEffort != selectedReasoningEffort {
		detail += "；已按协议归一化思考档位"
	}
	return detail
}

func modelMappingFailureDetails(result gatewayModelMappingResult, cause error) string {
	if cause == nil {
		return result.Details
	}
	return fmt.Sprintf("%s 映射未完成：%s；请求未转发", modelMappingStrategyLabel(result.Strategy), strings.TrimSpace(cause.Error()))
}

func modelMappingStrategyLabel(strategy gateway.ModelMappingStrategy) string {
	labels := map[gateway.ModelMappingStrategy]string{
		gateway.ModelMappingStrategyIQ:       "智商优先",
		gateway.ModelMappingStrategyCost:     "费用优先",
		gateway.ModelMappingStrategyTime:     "耗时优先",
		gateway.ModelMappingStrategyBalanced: "综合平衡",
		gateway.ModelMappingStrategyManual:   "手动固定",
	}
	return labels[strategy]
}

func newestRadarTime(left, right time.Time) bool {
	return left.After(right)
}
