package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

const (
	gatewayManualMappedTargetMappingStateNotApplicable = "not_applicable"
	gatewayManualMappedTargetMappingStateResolved      = "resolved"
	gatewayManualMappedTargetMappingStateUnresolved    = "unresolved"

	// This provenance means the server, rather than a requested/recent browser
	// model, evaluated a mapped model against an exact fixed-route target. It
	// applies to both persisted manual mappings and server-resolved automatic
	// mappings; the name describes the manual-route gate, not the mapping
	// strategy.
	gatewayManualMappedTargetGateProvenance       = "manual_mapped_model_gate"
	gatewayManualMappedTargetUnresolvedProvenance = "mapping_unresolved"
)

// gatewayManualMappedTargetPreviewOutput is deliberately a narrow management
// projection. In particular, it never serializes the mapped model, upstream
// endpoint, key, identity scope, model-sync error, or provider response.
//
// The browser supplies only downstream_key_id. The service enumerates the
// current persisted candidate-group members and resolves mapping state itself,
// so recent/requested preview models can never affect a fixed-route gate.
type gatewayManualMappedTargetPreviewOutput struct {
	MappingState   string                                `json:"mapping_state"`
	GateProvenance string                                `json:"gate_provenance"`
	Targets        []gatewayManualMappedTargetGateOutput `json:"targets"`
}

type gatewayManualMappedTargetGateOutput struct {
	CandidateMemberID     uint   `json:"candidate_member_id"`
	ChannelID             uint   `json:"channel_id"`
	GroupName             string `json:"group_name"`
	StructuralBlocked     bool   `json:"structural_blocked"`
	ManualMappedModelGate bool   `json:"manual_mapped_model_gate"`
	GateProvenance        string `json:"gate_provenance"`
	Reason                string `json:"reason,omitempty"`
}

func gatewayManualMappedTargetPreview(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("manual mapped target preview is unavailable"))
		return
	}
	downstreamKeyID, ok := gatewayManualMappedTargetPreviewDownstreamKeyID(c)
	if !ok {
		fail(c, http.StatusBadRequest, errors.New("downstream_key_id is required"))
		return
	}
	key, err := d.Gateway.Store.FindDownstreamKey(downstreamKeyID)
	if err != nil {
		// Do not turn a storage/provider error into a browser-visible diagnostic
		// that could reveal an account, endpoint, or mapping implementation.
		fail(c, http.StatusBadRequest, errors.New("downstream key is unavailable"))
		return
	}
	projections, err := d.Gateway.ManualMappedTargetGateProjectionsForDownstreamKey(downstreamKeyID)
	if err != nil {
		// A candidate group can disappear between key loading and projection
		// enumeration. The browser cannot safely distinguish that from another
		// incomplete projection read, and must not receive its implementation
		// detail. This is a management preview, so make the unknown state
		// explicit instead of pretending the preview service itself is down.
		gatewayManualMappedTargetWriteUnresolved(c)
		return
	}

	mappingState := gatewayManualMappedTargetMappingStateNotApplicable
	gateProvenance := ""
	strategy, strategyErr := gateway.NormalizeModelMappingStrategy(key.ModelMappingStrategy)
	switch {
	case strategyErr != nil:
		mappingState = gatewayManualMappedTargetMappingStateUnresolved
		gateProvenance = gatewayManualMappedTargetUnresolvedProvenance
	case strategy == gateway.ModelMappingStrategyDisabled:
		// No mapping is configured, so this endpoint only contributes baseline
		// target facts. It never invents a model gate from a client preview.
	case strategy == gateway.ModelMappingStrategyManual:
		if len(projections) == 0 || gatewayManualMappedTargetProjectionUnresolved(projections) {
			mappingState = gatewayManualMappedTargetMappingStateUnresolved
			gateProvenance = gatewayManualMappedTargetUnresolvedProvenance
		} else {
			mappingState = gatewayManualMappedTargetMappingStateResolved
			gateProvenance = gatewayManualMappedTargetGateProvenance
		}
	default:
		// Automatic modes have no fixed model stored on the key. Resolve the
		// current server-owned radar choice once for this response. A failed or
		// opaque resolver is deliberately non-model-blocking; real forwarding
		// re-resolves and remains fail-closed.
		group, groupErr := d.Gateway.Store.FindCandidateGroup(key.CandidateGroupID)
		if groupErr != nil {
			// Do not retain a prior baseline member list after the group has
			// disappeared (or can no longer be read). It could otherwise be a
			// stale target inventory; return the same explicit safe unknown
			// contract used for other enumeration races.
			gatewayManualMappedTargetWriteUnresolved(c)
			return
		}
		model, resolved := gatewayAutomaticMappedModelForManagement(d, key, group, strategy)
		if !resolved {
			mappingState = gatewayManualMappedTargetMappingStateUnresolved
			gateProvenance = gatewayManualMappedTargetUnresolvedProvenance
			break
		}
		mapped, mappedErr := d.Gateway.ManualMappedTargetGateProjectionsForDownstreamKeyWithServerModel(key.ID, model)
		if mappedErr != nil {
			// The group may change after the baseline batch projection. Do not
			// publish a stale resolved result, a partial target list, or any raw
			// storage/provider error.
			gatewayManualMappedTargetWriteUnresolved(c)
			return
		}
		projections = mapped
		mappingState = gatewayManualMappedTargetMappingStateResolved
		gateProvenance = gatewayManualMappedTargetGateProvenance
	}

	targets := make([]gatewayManualMappedTargetGateOutput, 0, len(projections))
	for _, projection := range projections {
		targets = append(targets, gatewayManualMappedTargetGateOutputFromProjection(projection, mappingState, gateProvenance))
	}
	c.JSON(http.StatusOK, gin.H{"data": gatewayManualMappedTargetPreviewOutput{
		MappingState:   mappingState,
		GateProvenance: gateProvenance,
		Targets:        targets,
	}})
}

func gatewayAutomaticMappedModelForManagement(d *Deps, key *storage.GatewayDownstreamKey, group *storage.GatewayCandidateGroup, strategy gateway.ModelMappingStrategy) (string, bool) {
	if strategy == gateway.ModelMappingStrategyDisabled || strategy == gateway.ModelMappingStrategyManual {
		return "", false
	}
	selected, _, err := gatewayModelMappingSelection(d, key, strategy, group)
	model := strings.TrimSpace(selected.Model)
	if err != nil || model == "" || !gatewayMappedModelAllowedForCandidateGroup(d, key, group, model) {
		return "", false
	}
	return model, true
}

// gatewayManualMappedTargetWriteUnresolved is the stable, allowlisted result
// for a key that exists but whose candidate members or exact mapping cannot be
// safely determined. Keep targets as a concrete empty slice so clients never
// have to infer whether null means an omitted or unavailable projection.
func gatewayManualMappedTargetWriteUnresolved(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"data": gatewayManualMappedTargetPreviewOutput{
		MappingState:   gatewayManualMappedTargetMappingStateUnresolved,
		GateProvenance: gatewayManualMappedTargetUnresolvedProvenance,
		Targets:        []gatewayManualMappedTargetGateOutput{},
	}})
}

func gatewayManualMappedTargetPreviewDownstreamKeyID(c *gin.Context) (uint, bool) {
	if c == nil {
		return 0, false
	}
	raw := strings.TrimSpace(c.Query("downstream_key_id"))
	if raw == "" {
		return 0, false
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || parsed == 0 {
		return 0, false
	}
	return uint(parsed), true
}

func gatewayManualMappedTargetProjectionUnresolved(projections []gateway.ManualMappedTargetGateProjection) bool {
	for _, projection := range projections {
		if projection.MappingUnresolved {
			return true
		}
	}
	return false
}

func gatewayManualMappedTargetGateOutputFromProjection(projection gateway.ManualMappedTargetGateProjection, mappingState, gateProvenance string) gatewayManualMappedTargetGateOutput {
	out := gatewayManualMappedTargetGateOutput{
		CandidateMemberID:     projection.CandidateMemberID,
		ChannelID:             projection.ChannelID,
		GroupName:             projection.CanonicalGroupName,
		StructuralBlocked:     projection.StructuralBlocked,
		ManualMappedModelGate: projection.ManualMappedModelGate && mappingState == gatewayManualMappedTargetMappingStateResolved,
		GateProvenance:        gateProvenance,
	}
	if reason, ok := gatewayManualMappedTargetSafeReason(projection.Reason); ok {
		out.Reason = string(reason)
	}
	return out
}

func gatewayManualMappedTargetSafeReason(reason gateway.ManualRouteGateReason) (gateway.ManualRouteGateReason, bool) {
	switch reason {
	case gateway.ManualRouteGateReasonTargetRequired,
		gateway.ManualRouteGateReasonCandidateGroupMissing,
		gateway.ManualRouteGateReasonCandidateGroupMismatch,
		gateway.ManualRouteGateReasonChannelMissing,
		gateway.ManualRouteGateReasonUpstreamKeyMissing,
		gateway.ManualRouteGateReasonUpstreamKeyDisabled,
		gateway.ManualRouteGateReasonUpstreamKeyUnavailable,
		gateway.ManualRouteGateReasonProtocolMismatch,
		gateway.ManualRouteGateReasonModelNotListed,
		gateway.ManualRouteGateReasonModelNotSupported,
		gateway.ManualRouteGateReasonGenerationUnavailable:
		return reason, true
	default:
		return "", false
	}
}
