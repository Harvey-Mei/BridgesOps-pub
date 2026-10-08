package api

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const (
	gatewayDispatchTerminalSuccess                  = "success"
	gatewayDispatchTerminalFailedBeforeAcceptance   = "failed_before_acceptance"
	gatewayDispatchTerminalFailedAfterAcceptance    = "failed_after_acceptance"
	gatewayDispatchTerminalClientCanceled           = "client_canceled"
	gatewayDispatchTerminalImageFailedAfterDispatch = "image_failed_after_dispatch"
	gatewayDispatchTerminalAuditFailure             = "dispatch_audit_failure"

	// gatewayDispatchVisibleOutputAcceptanceEvent records acceptance proven by
	// output the downstream client can see rather than by a named protocol
	// lifecycle event. Unknown frame shapes must never be relabelled as a real
	// provider event, but they still make the attempt non-replayable.
	gatewayDispatchVisibleOutputAcceptanceEvent = "visible_output"
)

var (
	errGatewayRequestDispatchBegin        = errors.New("persist request dispatch before upstream send")
	errGatewayRequestPhysicalDispatchMark = errors.New("persist transport-entry request dispatch evidence")
	errGatewayRequestTransportConfirm     = errors.New("persist completed transport invocation evidence")
)

// gatewayDispatchAuditState belongs to one client.Do invocation. It never
// contains a request body, credential, raw session identifier, or private
// upstream response.
type gatewayDispatchAuditState struct {
	store                    *storage.Gateway
	id                       uint64
	ownerInstanceID          string
	ownerFenceToken          uint64
	route                    gatewayRouteSnapshot
	billingCommitted         bool
	providerRequestID        string
	responseObservedAt       time.Time
	acceptanceObserved       bool
	acceptanceObservedAt     time.Time
	acceptancePersisted      bool
	acceptanceEvidence       gatewayStreamAcceptanceEvidence
	acceptancePersistErr     error
	physicallyDispatched     bool
	firstByteBudgetMs        int64
	telemetryVersion         int
	routeSnapshotVersion     int
	routeExclusionsSnapshot  string
	telemetryFailureRecorded bool
	closed                   bool
	surrendered              bool
}

func beginGatewayRequestDispatch(
	d *Deps,
	c *gin.Context,
	downstreamKeyID uint,
	decision *gatewayRouteDecision,
	route gatewayRouteSnapshot,
	model string,
	reasoningEffort string,
	mapping gatewayModelMappingResult,
	start time.Time,
	attempts []gatewayRouteAttempt,
	errorsSeen []string,
	telemetry *gatewayRequestTelemetry,
	firstByteBudget time.Duration,
	at time.Time,
) (*gatewayDispatchAuditState, error) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		return nil, fmt.Errorf("%w: gateway store is unavailable", errGatewayRequestDispatchBegin)
	}
	snapshot, err := gatewayRequestDispatchSnapshot(c, downstreamKeyID, decision, route, model, reasoningEffort, mapping, start, attempts, errorsSeen, telemetry, firstByteBudget)
	if err != nil {
		return nil, err
	}
	audit, err := d.Gateway.BeginRequestDispatch(snapshot, at)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errGatewayRequestDispatchBegin, err)
	}
	return gatewayDispatchAuditStateFromPersisted(d.Gateway.Store, audit, route), nil
}

func beginGatewayRequestDispatchAfter(
	d *Deps,
	c *gin.Context,
	previous *gatewayDispatchAuditState,
	_ time.Time,
	downstreamKeyID uint,
	decision *gatewayRouteDecision,
	route gatewayRouteSnapshot,
	model string,
	reasoningEffort string,
	mapping gatewayModelMappingResult,
	start time.Time,
	attempts []gatewayRouteAttempt,
	errorsSeen []string,
	telemetry *gatewayRequestTelemetry,
	firstByteBudget time.Duration,
	at time.Time,
) (*gatewayDispatchAuditState, error) {
	if previous == nil {
		return beginGatewayRequestDispatch(d, c, downstreamKeyID, decision, route, model, reasoningEffort, mapping, start, attempts, errorsSeen, telemetry, firstByteBudget, at)
	}
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		return nil, fmt.Errorf("%w: gateway store is unavailable", errGatewayRequestDispatchBegin)
	}
	snapshot, err := gatewayRequestDispatchSnapshot(c, downstreamKeyID, decision, route, model, reasoningEffort, mapping, start, attempts, errorsSeen, telemetry, firstByteBudget)
	if err != nil {
		return nil, err
	}
	audit, err := d.Gateway.BeginNextRequestDispatch(previous.id, snapshot, at)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errGatewayRequestDispatchBegin, err)
	}
	return gatewayDispatchAuditStateFromPersisted(d.Gateway.Store, audit, route), nil
}

func gatewayRequestDispatchSnapshot(
	c *gin.Context,
	downstreamKeyID uint,
	decision *gatewayRouteDecision,
	route gatewayRouteSnapshot,
	model string,
	reasoningEffort string,
	mapping gatewayModelMappingResult,
	start time.Time,
	attempts []gatewayRouteAttempt,
	errorsSeen []string,
	telemetry *gatewayRequestTelemetry,
	firstByteBudget time.Duration,
) (storage.GatewayRequestDispatchSnapshot, error) {
	if c == nil || c.Request == nil {
		return storage.GatewayRequestDispatchSnapshot{}, fmt.Errorf("%w: downstream request is unavailable", errGatewayRequestDispatchBegin)
	}
	candidate := route.Candidate
	provenance, _ := route.multiplierProvenance()
	traceID, telemetryVersion, routingMode, gatewayApp := "", 0, "", ""
	if telemetry != nil {
		telemetry.captureRouteExclusions(decision, time.Now().UTC())
		traceID, telemetryVersion, routingMode, gatewayApp, _, _, _ = telemetry.snapshot()
	}
	routeSnapshotVersion, routeExclusionsSnapshot := 0, ""
	if telemetry != nil {
		routeSnapshotVersion, routeExclusionsSnapshot = telemetry.routeExclusionSnapshot()
	}
	priorPhysicalFailures := 0
	if telemetry != nil {
		_, _, _, _, _, priorPhysicalFailures, _ = telemetry.snapshot()
	}
	firstByteBudgetMs := firstByteBudget.Milliseconds()
	if firstByteBudgetMs < 0 {
		firstByteBudgetMs = 0
	}
	snapshot := storage.GatewayRequestDispatchSnapshot{
		LogicalRequestTraceID:                 traceID,
		TelemetryVersion:                      telemetryVersion,
		RoutingMode:                           routingMode,
		GatewayApp:                            gatewayApp,
		FirstByteBudgetMs:                     firstByteBudgetMs,
		RouteExclusionSnapshotVersion:         routeSnapshotVersion,
		RouteExclusionsSnapshot:               routeExclusionsSnapshot,
		DownstreamKeyID:                       downstreamKeyID,
		ChannelID:                             route.ChannelID,
		ChannelNameSnapshot:                   route.ChannelName,
		GroupName:                             route.GroupName,
		UpstreamKeyID:                         route.UpstreamKeyID,
		UpstreamKeyName:                       route.UpstreamKeyName,
		ModelName:                             strings.TrimSpace(model),
		UpstreamModelName:                     strings.TrimSpace(model),
		ReasoningEffort:                       strings.TrimSpace(reasoningEffort),
		Method:                                c.Request.Method,
		Path:                                  c.Request.URL.Path,
		ClientIP:                              gatewayClientIP(c),
		ClientIPSource:                        gatewayClientIPSource(c),
		SessionHash:                           decisionSessionHash(decision),
		RouteReason:                           gatewayRequestRouteReason(c, gatewayRouteReason(decision, attempts, candidate)),
		RouteSelectionRank:                    gatewayRouteSelectionRank(decision, candidate),
		SessionAffinityOriginRequestID:        gatewaySessionAffinityOriginRequestID(decision),
		SessionAffinityExpiresAt:              gatewaySessionAffinityExpiresAt(decision),
		SessionAffinityOverriddenCandidates:   gatewaySessionAffinityOverriddenCandidates(decision),
		RouteDetails:                          gatewayRouteDetailsWithErrors(decision, attempts, candidate, errorsSeen),
		RouteAttemptedChannels:                gatewayRouteAttemptedChannels(decision, attempts, candidate),
		PriorPhysicallyDispatchedFailureCount: priorPhysicalFailures,
		GroupRatio:                            provenance.GroupRatio,
		CompletionRatio:                       provenance.CompletionRatio,
		RawGroupRatio:                         provenance.RawGroupRatio,
		RawCompletionRatio:                    provenance.RawCompletionRatio,
		FundingFactor:                         provenance.FundingFactor,
		CostBasisVersion:                      provenance.CostBasisVersion,
		CostBasisSource:                       provenance.CostBasisSource,
		ChannelIdentityRevision:               provenance.ChannelIdentityRevision,
		UpstreamGroupID:                       provenance.UpstreamGroupID,
		UpstreamGroupGeneration:               provenance.UpstreamGroupGeneration,
		UpstreamKeyLocalID:                    provenance.UpstreamKeyLocalID,
		UpstreamKeyGeneration:                 provenance.UpstreamKeyGeneration,
		RequestStartedAt:                      start,
	}
	populateGatewayRequestDispatchPricingSnapshot(&snapshot, mapping.Body)
	if mapping.enabled() {
		snapshot.RequestedModelName = mapping.RequestedModel
		snapshot.RequestedReasoningEffort = mapping.RequestedReasoningEffort
		snapshot.ModelMappingStrategy = string(mapping.Strategy)
		snapshot.ModelMappingDetails = mapping.Details
	}
	return snapshot, nil
}

func populateGatewayRequestDispatchPricingSnapshot(snapshot *storage.GatewayRequestDispatchSnapshot, body []byte) {
	if snapshot == nil || !strings.EqualFold(strings.TrimSpace(snapshot.Method), "POST") {
		return
	}

	setTokenPricing := func(protocol string) {
		snapshot.PriceOperation = storage.GatewayPricingOperationTextGeneration
		snapshot.PriceProtocol = protocol
		snapshot.UsageSchema = storage.GatewayPricingUsageSchemaTokensV1
		snapshot.UsageSchemaVersion = 1
		snapshot.UsageOperationPresent = false
		snapshot.OperationUnits = nil
	}
	setImagePricing := func(protocol string) {
		operationUnits := int64(1)
		snapshot.PriceOperation = storage.GatewayImageOperationGeneration
		snapshot.PriceProtocol = protocol
		snapshot.UsageSchema = storage.GatewayPricingUsageSchemaOperationV1
		snapshot.UsageSchemaVersion = 1
		snapshot.UsageOperationPresent = true
		snapshot.OperationUnits = &operationUnits
	}

	switch snapshot.Path {
	case "/v1/images/generations":
		setImagePricing(storage.GatewayImageProtocolImages)
	case "/v1/responses":
		if isGatewayImageGenerationRequest(snapshot.Path, body) {
			setImagePricing(storage.GatewayImageProtocolResponses)
		} else {
			setTokenPricing(storage.GatewayPricingProtocolOpenAIResponses)
		}
	case "/v1/chat/completions":
		setTokenPricing(storage.GatewayPricingProtocolOpenAIChatCompletions)
	case "/v1/completions":
		setTokenPricing(storage.GatewayPricingProtocolOpenAICompletions)
	case "/v1/messages":
		setTokenPricing(storage.GatewayPricingProtocolAnthropicMessages)
	case "/v1/embeddings":
		snapshot.PriceOperation = storage.GatewayPricingOperationEmbedding
		snapshot.PriceProtocol = storage.GatewayPricingProtocolOpenAIEmbeddings
		snapshot.UsageSchema = storage.GatewayPricingUsageSchemaInputTokensV1
		snapshot.UsageSchemaVersion = 1
		snapshot.UsageOperationPresent = false
		snapshot.OperationUnits = nil
	}
}

func gatewayDispatchAuditStateFromPersisted(store *storage.Gateway, audit *storage.GatewayRequestDispatchAudit, route gatewayRouteSnapshot) *gatewayDispatchAuditState {
	if store == nil || audit == nil {
		return nil
	}
	return &gatewayDispatchAuditState{
		store:                   store,
		id:                      audit.ID,
		ownerInstanceID:         audit.OwnerInstanceID,
		ownerFenceToken:         audit.OwnerFenceToken,
		route:                   route,
		billingCommitted:        audit.BillingCommitted && audit.TransportState == storage.GatewayRequestDispatchTransportStateConfirmed,
		physicallyDispatched:    audit.PhysicallyDispatched && (audit.TransportFenceVersion == 0 || audit.TransportState == storage.GatewayRequestDispatchTransportStateConfirmed),
		firstByteBudgetMs:       audit.FirstByteBudgetMs,
		telemetryVersion:        audit.TelemetryVersion,
		routeSnapshotVersion:    audit.RouteExclusionSnapshotVersion,
		routeExclusionsSnapshot: audit.RouteExclusionsSnapshot,
	}
}

func (s *gatewayDispatchAuditState) recordResponse(providerRequestID string, at time.Time) error {
	if s == nil || s.store == nil || s.id == 0 {
		return errors.New("request dispatch audit is unavailable")
	}
	s.providerRequestID = providerRequestID
	if s.responseObservedAt.IsZero() {
		s.responseObservedAt = at
	}
	return s.store.RecordRequestDispatchResponse(s.id, s.providerRequestID, at)
}

func (s *gatewayDispatchAuditState) markAccepted(evidence gatewayStreamAcceptanceEvidence, at time.Time) error {
	if s == nil || s.store == nil || s.id == 0 {
		return errors.New("request dispatch audit is unavailable")
	}
	// Visible output with no recognized protocol lifecycle event still fences
	// the dispatch. Normalize the observed fact itself rather than only the
	// store argument: finalization and the durable owner-loss handoff both
	// project acceptanceEvidence, and both reject an empty event type.
	if strings.TrimSpace(evidence.EventType) == "" {
		evidence.EventType = gatewayDispatchVisibleOutputAcceptanceEvent
	}
	if !s.acceptanceObserved {
		s.acceptanceObserved = true
		s.acceptanceEvidence = evidence
		s.acceptanceObservedAt = at
	}
	if s.acceptancePersisted {
		return nil
	}
	evidence = s.acceptanceEvidence
	if strings.TrimSpace(evidence.EventType) == "" {
		evidence.EventType = gatewayDispatchVisibleOutputAcceptanceEvent
		s.acceptanceEvidence = evidence
	}
	err := s.store.MarkRequestDispatchAccepted(s.id, evidence.EventType, evidence.ObjectID, at)
	if err != nil {
		s.acceptancePersistErr = err
		return err
	}
	s.acceptancePersisted = true
	s.acceptancePersistErr = nil
	return nil
}

func (s *gatewayDispatchAuditState) complete(terminalKind string, at time.Time) error {
	if s == nil || s.id == 0 {
		return nil
	}
	if s.closed || s.surrendered {
		return nil
	}
	if s.physicallyDispatched || s.billingCommitted || s.acceptanceObserved {
		return storage.ErrGatewayRequestDispatchRequiresFinalization
	}
	if s.store == nil {
		return errors.New("request dispatch audit store is unavailable")
	}
	if err := s.store.CompleteRequestDispatch(s.id, terminalKind, at); err != nil {
		return err
	}
	s.closed = true
	return nil
}

func (s *gatewayDispatchAuditState) finalize(service *gateway.Service, log *storage.GatewayRequestLog, terminalKind string, startedAt, finishedAt time.Time) (bool, error) {
	if s == nil || service == nil || log == nil || s.id == 0 {
		return false, errors.New("request dispatch finalization is unavailable")
	}
	if s.closed || s.surrendered {
		return false, storage.ErrGatewayRequestDispatchConflict
	}
	if s.acceptanceObserved && !s.acceptancePersisted {
		if err := s.markAccepted(s.acceptanceEvidence, finishedAt); err != nil {
			return false, fmt.Errorf("persist accepted dispatch before finalization: %w", err)
		}
	}
	s.applyTerminalFactsToLog(log)
	created, err := service.FinalizeRequestDispatchWithFacts(s.id, log, terminalKind, s.recoveryFacts(), startedAt, finishedAt)
	if err != nil {
		return false, err
	}
	s.closed = true
	return created, nil
}

// applyTerminalFactsToLog stamps the durable dispatch identity onto an exact
// terminal log. Atomic finalization and the durable owner-loss handoff must
// produce the same record, so both paths use this one projection. Acceptance
// mirrors the observed lifecycle fact because finalization persists it before
// committing, and the durable handoff writes the same fact in the transaction
// that makes the snapshot recoverable.
func (s *gatewayDispatchAuditState) applyTerminalFactsToLog(log *storage.GatewayRequestLog) {
	if s == nil || log == nil {
		return
	}
	if s.providerRequestID != "" && log.UpstreamRequestID == "" {
		log.UpstreamRequestID = s.providerRequestID
	}
	if s.routeSnapshotVersion != 0 {
		log.RouteExclusionSnapshotVersion = s.routeSnapshotVersion
		log.RouteExclusionsSnapshot = s.routeExclusionsSnapshot
	}
	log.UpstreamAccepted = s.acceptanceObserved
	log.BillingCommitted = s.billingCommitted
}

func (s *gatewayDispatchAuditState) recoveryFacts() storage.GatewayRequestDispatchFinalizationFacts {
	if s == nil {
		return storage.GatewayRequestDispatchFinalizationFacts{}
	}
	facts := storage.GatewayRequestDispatchFinalizationFacts{
		ProviderRequestID:  s.providerRequestID,
		ResponseObservedAt: s.responseObservedAt,
		Accepted:           s.acceptanceObserved,
		AcceptedEventType:  strings.TrimSpace(s.acceptanceEvidence.EventType),
		AcceptedObjectID:   strings.TrimSpace(s.acceptanceEvidence.ObjectID),
		AcceptedAt:         s.acceptanceObservedAt,
	}
	if fence, ok := gateway.RoutingHealthFenceForCandidate(s.route.Candidate); ok {
		facts.RoutingHealthFence = &fence
	}
	return facts
}

func (s *gatewayDispatchAuditState) recordTelemetryPhysicalFailure(telemetry *gatewayRequestTelemetry) {
	if s == nil || telemetry == nil || !s.physicallyDispatched || s.telemetryFailureRecorded {
		return
	}
	telemetry.markPhysicalDispatchFailure()
	s.telemetryFailureRecorded = true
}

func gatewayDispatchAuditFailure(prefix string, err error) string {
	prefix = strings.TrimSpace(prefix)
	if err == nil {
		return prefix
	}
	detail := sanitizeGatewayProbeError([]byte(err.Error()), "")
	if detail == "" {
		detail = "durable audit failed"
	}
	if prefix == "" {
		return detail
	}
	return prefix + ": " + detail
}
