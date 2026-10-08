package storage

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/providerid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	GatewayRequestDispatchStageDispatched = "dispatched"
	GatewayRequestDispatchStageResponded  = "responded"
	GatewayRequestDispatchStageAccepted   = "accepted"
	GatewayRequestDispatchStageCompleted  = "completed"
	GatewayRequestDispatchStageFinalized  = "finalized"

	GatewayRequestDispatchTerminalOrphanedWithoutAcceptance = "orphaned_without_acceptance_evidence"
	GatewayRequestDispatchTerminalOwnerLeaseLost            = "owner_lease_lost"

	// GatewayRequestDispatchPhysicalBillingInvariantVersion distinguishes
	// audits written after the physical-send/billing coupling was introduced
	// from historical rows. Legacy rows remain recoverable under their old
	// evidence semantics; every newly-created audit must use this version.
	GatewayRequestDispatchPhysicalBillingInvariantVersion = 1
	// GatewayRequestDispatchTransportFenceVersion distinguishes new audits that
	// record the pre-RoundTripper armed state from legacy physical markers.
	GatewayRequestDispatchTransportFenceVersion = 1

	GatewayRequestDispatchTransportStateLegacy        = "legacy"
	GatewayRequestDispatchTransportStatePrepared      = "prepared"
	GatewayRequestDispatchTransportStateArmed         = "armed"
	GatewayRequestDispatchTransportStateConfirmed     = "confirmed"
	GatewayRequestDispatchTransportStateNotDispatched = "not_dispatched"
	GatewayRequestDispatchTransportStateManualReview  = "manual_review"
	// GatewayRequestDispatchMultiplierProvenanceVersion marks audits whose
	// multiplier and exact routing identity were frozen at dispatch time.
	// Version zero is reserved for rows that predate the dispatch-time fence
	// and must never be treated as equivalent historical evidence.
	GatewayRequestDispatchMultiplierProvenanceVersion = 1
)

var (
	ErrGatewayRequestDispatchConflict              = errors.New("gateway request dispatch lifecycle conflict")
	ErrGatewayRequestDispatchRequiresFinalization  = errors.New("accepted or billing-committed dispatch requires request-log finalization")
	ErrGatewayRequestDispatchHasCostEvidence       = errors.New("request dispatch has provider billing evidence")
	ErrGatewayRequestDispatchRecoveryRequested     = errors.New("gateway request dispatch was handed off for recovery")
	ErrGatewayRequestDispatchBillingCommitRequired = errors.New("image request dispatch must commit billing with physical dispatch")
	ErrGatewayRequestDispatchBillingCommitInvalid  = errors.New("request dispatch billing commitment is not eligible for physical dispatch")
	// ErrGatewayRequestDispatchCommitUnknown means the database accepted the
	// transaction body, but its final commit result was not observable. The
	// caller must fence this owner and let recovery reconcile both the previous
	// and possible next physical audit; it must never issue another upstream Do.
	ErrGatewayRequestDispatchCommitUnknown = errors.New("gateway request dispatch transaction commit outcome is unknown")
)

// GatewayRequestDispatchSnapshot contains only the minimum non-secret routing
// facts needed to audit one physical upstream HTTP dispatch and reconstruct a
// failed request log after a process restart. ClientIP must already have the
// same normalized semantics as GatewayRequestLog, and SessionHash must be the
// existing one-way hash. The snapshot intentionally excludes request bodies,
// credentials, raw session identifiers, and private upstream responses.
type GatewayRequestDispatchSnapshot struct {
	LogicalRequestTraceID                 string
	TelemetryVersion                      int
	RoutingMode                           string
	GatewayApp                            string
	FirstByteBudgetMs                     int64
	RouteExclusionSnapshotVersion         int
	RouteExclusionsSnapshot               string
	DownstreamKeyID                       uint
	ChannelID                             uint
	ChannelNameSnapshot                   string
	GroupName                             string
	UpstreamKeyID                         int64
	UpstreamKeyName                       string
	ModelName                             string
	UpstreamModelName                     string
	ReasoningEffort                       string
	RequestedModelName                    string
	RequestedReasoningEffort              string
	ModelMappingStrategy                  string
	ModelMappingDetails                   string
	Method                                string
	Path                                  string
	ClientIP                              string
	ClientIPSource                        string
	SessionHash                           string
	RouteReason                           string
	RouteSelectionRank                    uint
	SessionAffinityOriginRequestID        uint
	SessionAffinityExpiresAt              *time.Time
	SessionAffinityOverriddenCandidates   string
	RouteDetails                          string
	RouteAttemptedChannels                string
	RolloverPredecessorID                 uint64
	PriorPhysicallyDispatchedFailureCount int
	RealFallback                          bool
	GroupRatio                            float64
	CompletionRatio                       float64
	RawGroupRatio                         *float64
	RawCompletionRatio                    *float64
	FundingFactor                         *float64
	CostBasisVersion                      uint64
	CostBasisSource                       string
	ChannelIdentityRevision               uint64
	UpstreamGroupID                       uint
	UpstreamGroupGeneration               uint64
	UpstreamKeyLocalID                    uint
	UpstreamKeyGeneration                 uint64
	PriceFactID                           *uint64
	PriceCostUnitEpoch                    uint64
	PriceFactVersion                      uint64
	PriceFactSchemaVersion                int
	PriceFactChecksum                     string
	PriceCanonicalGroupName               string
	PriceModelName                        string
	PriceOperation                        string
	PriceProtocol                         string
	PriceInputUnit                        *float64
	PriceOutputUnit                       *float64
	PriceCacheReadUnit                    *float64
	PriceCacheWriteUnit                   *float64
	PriceLongContextThresholdTokens       *int64
	PriceLongContextInputUnit             *float64
	PriceLongContextOutputUnit            *float64
	PriceLongContextCacheReadUnit         *float64
	PriceLongContextCacheWriteUnit        *float64
	PriceOperationUnit                    *float64
	PriceCurrency                         string
	PriceUnit                             string
	PriceSource                           string
	PriceObservedAt                       *time.Time
	PriceValidFrom                        *time.Time
	PriceValidUntil                       *time.Time
	UsageSchema                           string
	UsageSchemaVersion                    int
	UsageOperationPresent                 bool
	OperationUnits                        *int64
	PricingUnavailableReason              string
	RequestStartedAt                      time.Time
}

// GatewayRequestDispatchFinalizationFacts carries lifecycle evidence that was
// observed by the live handler but whose first persistence attempt failed.
// Recovery-queue retries apply these facts in the same transaction as the
// final request log, so an authoritative provider ID or accepted event cannot
// be dropped between separate writes.
type GatewayRequestDispatchFinalizationFacts struct {
	ProviderRequestID  string
	ResponseObservedAt time.Time
	Accepted           bool
	AcceptedEventType  string
	AcceptedObjectID   string
	AcceptedAt         time.Time
	RoutingHealthFence *GatewayRoutingHealthFence `json:"routing_health_fence,omitempty"`
}

// GatewayRequestDispatchAudit records lifecycle facts for exactly one
// physical upstream dispatch. ProviderRequestID is the provider-specific
// authoritative billing header. AcceptedObjectID is a protocol object ID and
// lives in a separate namespace; it must never be substituted for the billing
// ID unless a connector explicitly declares that capability.
type GatewayRequestDispatchAudit struct {
	ID                                    uint64 `gorm:"primaryKey;index:idx_gateway_request_dispatch_trace,priority:3"`
	LogicalRequestTraceID                 string `gorm:"size:32;index:idx_gateway_request_dispatch_trace,priority:1;not null;default:''"`
	TelemetryVersion                      int    `gorm:"not null;default:0"`
	RoutingMode                           string `gorm:"size:16;not null;default:''"`
	GatewayApp                            string `gorm:"size:32;not null;default:''"`
	PhysicallyDispatched                  bool   `gorm:"not null;default:false"`
	PhysicalBillingInvariantVersion       int    `gorm:"not null;default:1"`
	TransportFenceVersion                 int    `gorm:"not null;default:1"`
	TransportState                        string `gorm:"size:24;not null;default:'prepared'"`
	TransportConfirmationVersion          int    `gorm:"not null;default:1"`
	TransportReturnConfirmed              bool   `gorm:"not null;default:false"`
	TransportReturnConfirmedAt            *time.Time
	RolloverPredecessorID                 *uint64
	PriorPhysicallyDispatchedFailureCount int    `gorm:"not null;default:0"`
	RealFallback                          bool   `gorm:"not null;default:false"`
	FirstByteBudgetMs                     int64  `gorm:"not null;default:0"`
	RouteExclusionSnapshotVersion         int    `gorm:"not null;default:0"`
	RouteExclusionsSnapshot               string `gorm:"type:jsonb;not null;default:'{}'"`
	DownstreamKeyID                       uint   `gorm:"not null;default:0"`
	ChannelID                             uint   `gorm:"not null"`
	ChannelNameSnapshot                   string `gorm:"size:256;not null;default:''"`
	GroupName                             string `gorm:"size:128;not null;default:''"`
	UpstreamKeyID                         int64  `gorm:"not null;default:0"`
	UpstreamKeyName                       string `gorm:"size:128;not null;default:''"`
	ModelName                             string `gorm:"size:256;not null;default:''"`
	UpstreamModelName                     string `gorm:"size:256;not null;default:''"`
	ReasoningEffort                       string `gorm:"size:64;not null;default:''"`
	RequestedModelName                    string `gorm:"size:256;not null;default:''"`
	RequestedReasoningEffort              string `gorm:"size:64;not null;default:''"`
	ModelMappingStrategy                  string `gorm:"size:32;not null;default:''"`
	ModelMappingDetails                   string `gorm:"size:1024;not null;default:''"`
	Method                                string `gorm:"size:16;not null"`
	Path                                  string `gorm:"size:512;not null"`
	ClientIP                              string `gorm:"size:64;not null;default:''"`
	ClientIPSource                        string `gorm:"size:32;not null;default:''"`
	SessionHash                           string `gorm:"size:64;not null;default:''"`
	RouteReason                           string `gorm:"size:32;not null;default:''"`
	RouteSelectionRank                    uint   `gorm:"not null;default:0"`
	SessionAffinityOriginRequestID        uint   `gorm:"not null;default:0"`
	SessionAffinityExpiresAt              *time.Time
	SessionAffinityOverriddenCandidates   string  `gorm:"type:text;not null;default:''"`
	RouteDetails                          string  `gorm:"size:4096;not null;default:''"`
	RouteAttemptedChannels                string  `gorm:"size:16384;not null;default:''"`
	GroupRatio                            float64 `gorm:"not null;default:0"`
	CompletionRatio                       float64 `gorm:"not null;default:0"`
	RawGroupRatio                         *float64
	RawCompletionRatio                    *float64
	FundingFactor                         *float64
	CostBasisVersion                      uint64 `gorm:"not null;default:0"`
	CostBasisSource                       string `gorm:"size:32;not null;default:''"`
	MultiplierProvenanceVersion           int    `gorm:"not null;default:1"`
	ChannelIdentityRevision               uint64 `gorm:"not null;default:0"`
	UpstreamGroupID                       uint   `gorm:"not null;default:0"`
	UpstreamGroupGeneration               uint64 `gorm:"not null;default:0"`
	UpstreamKeyLocalID                    uint   `gorm:"not null;default:0"`
	UpstreamKeyGeneration                 uint64 `gorm:"not null;default:0"`
	PriceFactID                           *uint64
	PriceCostUnitEpoch                    uint64 `gorm:"not null;default:0"`
	PriceFactVersion                      uint64 `gorm:"not null;default:0"`
	PriceFactSchemaVersion                int    `gorm:"not null;default:0"`
	PriceFactChecksum                     string `gorm:"size:64;not null;default:''"`
	PriceCanonicalGroupName               string `gorm:"size:128;not null;default:''"`
	PriceModelName                        string `gorm:"size:256;not null;default:''"`
	PriceOperation                        string `gorm:"size:32;not null;default:''"`
	PriceProtocol                         string `gorm:"size:32;not null;default:''"`
	PriceInputUnit                        *float64
	PriceOutputUnit                       *float64
	PriceCacheReadUnit                    *float64
	PriceCacheWriteUnit                   *float64
	PriceLongContextThresholdTokens       *int64
	PriceLongContextInputUnit             *float64
	PriceLongContextOutputUnit            *float64
	PriceLongContextCacheReadUnit         *float64
	PriceLongContextCacheWriteUnit        *float64
	PriceOperationUnit                    *float64
	PriceCurrency                         string `gorm:"size:3;not null;default:''"`
	PriceUnit                             string `gorm:"size:32;not null;default:''"`
	PriceSource                           string `gorm:"size:64;not null;default:''"`
	PriceObservedAt                       *time.Time
	PriceValidFrom                        *time.Time
	PriceValidUntil                       *time.Time
	UsageSchema                           string `gorm:"size:32;not null;default:''"`
	UsageSchemaVersion                    int    `gorm:"not null;default:0"`
	UsageOperationPresent                 bool   `gorm:"not null;default:false"`
	OperationUnits                        *int64
	PricingUnavailableReason              string    `gorm:"column:cost_unavailable_reason;size:64;not null;default:''"`
	RequestStartedAt                      time.Time `gorm:"not null"`
	OwnerInstanceID                       string    `gorm:"size:32"`
	OwnerFenceToken                       uint64
	LegacyOwnerless                       bool `gorm:"not null;default:false"`
	RecoveryRequestedAt                   *time.Time
	RecoveryTerminalKind                  string `gorm:"size:64;not null;default:''"`
	Stage                                 string `gorm:"size:16;not null;default:'dispatched'"`
	BillingCommitted                      bool   `gorm:"not null;default:false"`
	ResponseObservedAt                    *time.Time
	ProviderRequestID                     string `gorm:"size:512;not null;default:''"`
	AcceptedAt                            *time.Time
	AcceptedEventType                     string `gorm:"size:64;not null;default:''"`
	AcceptedObjectID                      string `gorm:"size:128;not null;default:''"`
	TerminalAt                            *time.Time
	TerminalKind                          string    `gorm:"size:64;not null;default:''"`
	RequestLogID                          *uint     `gorm:"uniqueIndex"`
	DispatchedAt                          time.Time `gorm:"not null;index:idx_gateway_request_dispatch_trace,priority:2"`
	CreatedAt                             time.Time `gorm:"not null"`
}

func (GatewayRequestDispatchAudit) TableName() string {
	return "gateway_request_dispatch_audits"
}

func normalizeGatewayRequestDispatchTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Now().UTC()
	}
	return value.UTC()
}

func cloneGatewayRequestDispatchNumber(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneGatewayRequestDispatchTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func gatewayRequestDispatchTimesEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.UTC().Equal(right.UTC())
}

type gatewayRequestDispatchAffinityCandidate struct {
	ChannelID    uint   `json:"channel_id"`
	ChannelName  string `json:"channel_name"`
	GroupName    string `json:"group_name"`
	EligibleRank uint   `json:"eligible_rank"`
}

func normalizeGatewayRequestDispatchAffinityCandidates(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	var candidates []gatewayRequestDispatchAffinityCandidate
	if err := json.Unmarshal([]byte(raw), &candidates); err != nil || len(candidates) == 0 || len(candidates) > 128 {
		return "", errors.New("request dispatch session affinity candidates are invalid")
	}
	seen := make(map[string]struct{}, len(candidates))
	for index := range candidates {
		candidate := &candidates[index]
		candidate.ChannelName = strings.TrimSpace(candidate.ChannelName)
		candidate.GroupName = strings.TrimSpace(candidate.GroupName)
		if candidate.ChannelID == 0 || candidate.ChannelName == "" || candidate.GroupName == "" || candidate.EligibleRank == 0 || candidate.EligibleRank > 100000 {
			return "", errors.New("request dispatch session affinity candidate is incomplete")
		}
		key := fmt.Sprintf("%d:%s", candidate.ChannelID, strings.ToLower(candidate.GroupName))
		if _, duplicate := seen[key]; duplicate {
			return "", errors.New("request dispatch session affinity candidates contain a duplicate")
		}
		seen[key] = struct{}{}
	}
	encoded, err := json.Marshal(candidates)
	if err != nil {
		return "", errors.New("request dispatch session affinity candidates could not be encoded")
	}
	return string(encoded), nil
}

func validateGatewayRequestDispatchText(field, value string, maximum int, required bool) (string, error) {
	value = strings.TrimSpace(value)
	if required && value == "" {
		return "", fmt.Errorf("%s is required", field)
	}
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > maximum {
		return "", fmt.Errorf("%s exceeds its safe length", field)
	}
	return value, nil
}

func validateGatewayRequestDispatchSessionHash(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if len(value) != 64 {
		return "", errors.New("request dispatch session hash must be a SHA-256 hex digest")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return "", errors.New("request dispatch session hash must be a SHA-256 hex digest")
	}
	return strings.ToLower(value), nil
}

func validateGatewayLogicalRequestTraceID(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "", nil
	}
	// A 128-bit cryptographically random value is encoded as 32 lowercase hex
	// characters. Keeping this strict prevents this diagnostic join key from
	// becoming an accidental carrier for client-controlled request metadata.
	if len(value) != 32 {
		return "", errors.New("logical request trace id must be a 128-bit hex value")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		return "", errors.New("logical request trace id must be a 128-bit hex value")
	}
	return value, nil
}

func normalizeGatewayRequestTelemetry(version int, traceID, routingMode, gatewayApp string, firstByteBudgetMs int64) (int, string, string, string, int64, error) {
	traceID, err := validateGatewayLogicalRequestTraceID(traceID)
	if err != nil {
		return 0, "", "", "", 0, err
	}
	routingMode = strings.ToLower(strings.TrimSpace(routingMode))
	gatewayApp = strings.ToLower(strings.TrimSpace(gatewayApp))
	if firstByteBudgetMs < 0 {
		return 0, "", "", "", 0, errors.New("first byte budget must not be negative")
	}
	if version == 0 {
		if traceID != "" || routingMode != "" || gatewayApp != "" || firstByteBudgetMs != 0 {
			return 0, "", "", "", 0, errors.New("request telemetry facts require a telemetry version")
		}
		return 0, "", "", "", 0, nil
	}
	if version != GatewayRequestTelemetryVersion || traceID == "" {
		return 0, "", "", "", 0, errors.New("request telemetry version or trace id is invalid")
	}
	if routingMode != GatewayRoutingModeSmart && routingMode != GatewayRoutingModeFixed {
		return 0, "", "", "", 0, errors.New("request telemetry routing mode is invalid")
	}
	switch GatewayApp(gatewayApp) {
	case GatewayAppCodex, GatewayAppClaude, GatewayAppGemini:
	default:
		return 0, "", "", "", 0, errors.New("request telemetry gateway app is invalid")
	}
	return version, traceID, routingMode, gatewayApp, firstByteBudgetMs, nil
}

func normalizeGatewayRequestDispatchFinalizationFacts(facts GatewayRequestDispatchFinalizationFacts, at time.Time) (GatewayRequestDispatchFinalizationFacts, error) {
	var err error
	if facts.ProviderRequestID, err = providerid.Normalize(facts.ProviderRequestID); err != nil {
		return facts, fmt.Errorf("provider request id: %w", err)
	}
	if facts.RoutingHealthFence != nil {
		fence := *facts.RoutingHealthFence
		if fence.CanonicalGroupName, err = validateGatewayRequestDispatchText("routing health success group", fence.CanonicalGroupName, 128, true); err != nil {
			return facts, err
		}
		if !fence.Valid() {
			return facts, errors.New("routing health success fence is incomplete")
		}
		facts.RoutingHealthFence = &fence
	}
	if !facts.ResponseObservedAt.IsZero() {
		facts.ResponseObservedAt = facts.ResponseObservedAt.UTC()
	}
	if !facts.Accepted {
		if strings.TrimSpace(facts.AcceptedEventType) != "" || strings.TrimSpace(facts.AcceptedObjectID) != "" || !facts.AcceptedAt.IsZero() {
			return facts, errors.New("request dispatch acceptance fields require accepted evidence")
		}
		if facts.ProviderRequestID != "" && facts.ResponseObservedAt.IsZero() {
			facts.ResponseObservedAt = at
		}
		return facts, nil
	}
	if facts.AcceptedEventType, err = validateGatewayRequestDispatchText("accepted event type", facts.AcceptedEventType, 64, true); err != nil {
		return facts, err
	}
	if facts.AcceptedObjectID, err = validateGatewayRequestDispatchText("accepted object id", facts.AcceptedObjectID, 128, false); err != nil {
		return facts, err
	}
	if facts.AcceptedAt.IsZero() {
		facts.AcceptedAt = at
	} else {
		facts.AcceptedAt = facts.AcceptedAt.UTC()
	}
	if facts.ResponseObservedAt.IsZero() {
		facts.ResponseObservedAt = facts.AcceptedAt
	}
	return facts, nil
}

func normalizeGatewayRequestDispatchSnapshot(snapshot GatewayRequestDispatchSnapshot, at time.Time) (GatewayRequestDispatchSnapshot, error) {
	if snapshot.ChannelID == 0 {
		return snapshot, errors.New("request dispatch channel is required")
	}
	var err error
	if snapshot.TelemetryVersion, snapshot.LogicalRequestTraceID, snapshot.RoutingMode, snapshot.GatewayApp, snapshot.FirstByteBudgetMs, err = normalizeGatewayRequestTelemetry(
		snapshot.TelemetryVersion,
		snapshot.LogicalRequestTraceID,
		snapshot.RoutingMode,
		snapshot.GatewayApp,
		snapshot.FirstByteBudgetMs,
	); err != nil {
		return snapshot, err
	}
	if snapshot.RouteExclusionSnapshotVersion, snapshot.RouteExclusionsSnapshot, err = normalizeGatewayRouteExclusionSnapshot(
		snapshot.RouteExclusionSnapshotVersion,
		snapshot.RouteExclusionsSnapshot,
	); err != nil {
		return snapshot, err
	}
	if snapshot.TelemetryVersion == 0 && snapshot.RouteExclusionSnapshotVersion != 0 ||
		snapshot.TelemetryVersion != 0 && snapshot.RouteExclusionSnapshotVersion != GatewayRouteExclusionSnapshotVersion {
		return snapshot, errors.New("route exclusion snapshot coverage does not match request telemetry")
	}
	if snapshot.PriorPhysicallyDispatchedFailureCount < 0 {
		return snapshot, errors.New("request dispatch prior physical failure count must not be negative")
	}
	if snapshot.TelemetryVersion == 0 && snapshot.PriorPhysicallyDispatchedFailureCount != 0 {
		return snapshot, errors.New("request dispatch prior physical failure count requires telemetry")
	}
	if snapshot.TelemetryVersion == 0 && snapshot.RealFallback {
		return snapshot, errors.New("request dispatch real fallback requires telemetry")
	}
	if snapshot.ChannelNameSnapshot, err = validateGatewayRequestDispatchText("request dispatch channel name", snapshot.ChannelNameSnapshot, 256, false); err != nil {
		return snapshot, err
	}
	if snapshot.GroupName, err = validateGatewayRequestDispatchText("request dispatch group", snapshot.GroupName, 128, false); err != nil {
		return snapshot, err
	}
	if snapshot.UpstreamKeyName, err = validateGatewayRequestDispatchText("request dispatch upstream key name", snapshot.UpstreamKeyName, 128, false); err != nil {
		return snapshot, err
	}
	if snapshot.ModelName, err = validateGatewayRequestDispatchText("request dispatch model", snapshot.ModelName, 256, false); err != nil {
		return snapshot, err
	}
	if snapshot.UpstreamModelName, err = validateGatewayRequestDispatchText("request dispatch upstream model", snapshot.UpstreamModelName, 256, false); err != nil {
		return snapshot, err
	}
	if snapshot.ReasoningEffort, err = validateGatewayRequestDispatchText("request dispatch reasoning effort", snapshot.ReasoningEffort, 64, false); err != nil {
		return snapshot, err
	}
	if snapshot.RequestedModelName, err = validateGatewayRequestDispatchText("request dispatch requested model", snapshot.RequestedModelName, 256, false); err != nil {
		return snapshot, err
	}
	if snapshot.RequestedReasoningEffort, err = validateGatewayRequestDispatchText("request dispatch requested reasoning effort", snapshot.RequestedReasoningEffort, 64, false); err != nil {
		return snapshot, err
	}
	if snapshot.ModelMappingStrategy, err = validateGatewayRequestDispatchText("request dispatch model mapping strategy", snapshot.ModelMappingStrategy, 32, false); err != nil {
		return snapshot, err
	}
	if snapshot.ModelMappingDetails, err = validateGatewayRequestDispatchText("request dispatch model mapping details", snapshot.ModelMappingDetails, 1024, false); err != nil {
		return snapshot, err
	}
	if snapshot.Method, err = validateGatewayRequestDispatchText("request dispatch method", snapshot.Method, 16, true); err != nil {
		return snapshot, err
	}
	if snapshot.Path, err = validateGatewayRequestDispatchText("request dispatch path", snapshot.Path, 512, true); err != nil {
		return snapshot, err
	}
	if snapshot.ClientIP, err = validateGatewayRequestDispatchText("request dispatch client ip", snapshot.ClientIP, 64, false); err != nil {
		return snapshot, err
	}
	if snapshot.ClientIP != "" && net.ParseIP(snapshot.ClientIP) == nil {
		return snapshot, errors.New("request dispatch client ip is invalid")
	}
	if snapshot.ClientIPSource, err = validateGatewayRequestDispatchText("request dispatch client ip source", snapshot.ClientIPSource, 32, false); err != nil {
		return snapshot, err
	}
	if snapshot.SessionHash, err = validateGatewayRequestDispatchSessionHash(snapshot.SessionHash); err != nil {
		return snapshot, err
	}
	if snapshot.RouteReason, err = validateGatewayRequestDispatchText("request dispatch route reason", snapshot.RouteReason, 32, false); err != nil {
		return snapshot, err
	}
	if snapshot.SessionAffinityExpiresAt != nil {
		expiresAt := snapshot.SessionAffinityExpiresAt.UTC()
		if expiresAt.IsZero() {
			return snapshot, errors.New("request dispatch session affinity expiry is invalid")
		}
		snapshot.SessionAffinityExpiresAt = &expiresAt
	}
	if snapshot.SessionAffinityOverriddenCandidates, err = validateGatewayRequestDispatchText("request dispatch session affinity candidates", snapshot.SessionAffinityOverriddenCandidates, 65536, false); err != nil {
		return snapshot, err
	}
	if snapshot.SessionAffinityOverriddenCandidates, err = normalizeGatewayRequestDispatchAffinityCandidates(snapshot.SessionAffinityOverriddenCandidates); err != nil {
		return snapshot, err
	}
	if snapshot.SessionAffinityOriginRequestID == 0 && (snapshot.SessionAffinityExpiresAt != nil || snapshot.SessionAffinityOverriddenCandidates != "") {
		return snapshot, errors.New("request dispatch session affinity audit requires an origin request")
	}
	if snapshot.RouteDetails, err = validateGatewayRequestDispatchText("request dispatch route details", snapshot.RouteDetails, 4096, false); err != nil {
		return snapshot, err
	}
	if snapshot.RouteAttemptedChannels, err = validateGatewayRequestDispatchText("request dispatch attempted channels", snapshot.RouteAttemptedChannels, 16384, false); err != nil {
		return snapshot, err
	}
	if snapshot.CostBasisSource, err = validateGatewayRequestDispatchText("request dispatch cost basis source", snapshot.CostBasisSource, 32, false); err != nil {
		return snapshot, err
	}
	if snapshot.PriceOperation, err = validateGatewayRequestDispatchText("request dispatch price operation", strings.ToLower(strings.TrimSpace(snapshot.PriceOperation)), 32, false); err != nil {
		return snapshot, err
	}
	if snapshot.PriceProtocol, err = validateGatewayRequestDispatchText("request dispatch price protocol", strings.ToLower(strings.TrimSpace(snapshot.PriceProtocol)), 32, false); err != nil {
		return snapshot, err
	}
	if snapshot.UsageSchema, err = validateGatewayRequestDispatchText("request dispatch usage schema", strings.ToLower(strings.TrimSpace(snapshot.UsageSchema)), 32, false); err != nil {
		return snapshot, err
	}
	if snapshot.UsageSchemaVersion != 0 && snapshot.UsageSchemaVersion != 1 {
		return snapshot, errors.New("request dispatch usage schema version is unsupported")
	}
	if snapshot.UsageOperationPresent != (snapshot.OperationUnits != nil) || snapshot.OperationUnits != nil && *snapshot.OperationUnits < 0 {
		return snapshot, errors.New("request dispatch operation usage is invalid")
	}
	snapshot.OperationUnits = cloneGatewayRequestPricingInt64(snapshot.OperationUnits)
	selectionProvenanceProvided := snapshot.ChannelIdentityRevision != 0 || snapshot.UpstreamGroupID != 0 || snapshot.UpstreamGroupGeneration != 0 ||
		snapshot.UpstreamKeyLocalID != 0 || snapshot.UpstreamKeyGeneration != 0
	if selectionProvenanceProvided && (snapshot.ChannelIdentityRevision == 0 || snapshot.UpstreamGroupID == 0 || snapshot.UpstreamGroupGeneration == 0 ||
		snapshot.UpstreamKeyLocalID == 0 || snapshot.UpstreamKeyGeneration == 0) {
		return snapshot, errors.New("request dispatch selection provenance is incomplete")
	}
	if snapshot.PriceFactID != nil ||
		snapshot.PriceFactVersion != 0 || snapshot.PriceFactSchemaVersion != 0 || snapshot.PriceFactChecksum != "" ||
		snapshot.PriceCanonicalGroupName != "" || snapshot.PriceModelName != "" || snapshot.PriceInputUnit != nil || snapshot.PriceOutputUnit != nil ||
		snapshot.PriceCacheReadUnit != nil || snapshot.PriceCacheWriteUnit != nil || snapshot.PriceOperationUnit != nil ||
		snapshot.PriceLongContextThresholdTokens != nil || snapshot.PriceLongContextInputUnit != nil ||
		snapshot.PriceLongContextOutputUnit != nil || snapshot.PriceLongContextCacheReadUnit != nil ||
		snapshot.PriceLongContextCacheWriteUnit != nil ||
		snapshot.PriceCurrency != "" || snapshot.PriceUnit != "" || snapshot.PriceSource != "" ||
		snapshot.PriceValidFrom != nil || snapshot.PriceValidUntil != nil || snapshot.PricingUnavailableReason != "" {
		return snapshot, errors.New("request dispatch price fact snapshot must be resolved by storage")
	}
	snapshot.RawGroupRatio = cloneGatewayRequestDispatchNumber(snapshot.RawGroupRatio)
	snapshot.RawCompletionRatio = cloneGatewayRequestDispatchNumber(snapshot.RawCompletionRatio)
	snapshot.FundingFactor = cloneGatewayRequestDispatchNumber(snapshot.FundingFactor)
	if snapshot.RequestStartedAt.IsZero() {
		snapshot.RequestStartedAt = at
	} else {
		snapshot.RequestStartedAt = snapshot.RequestStartedAt.UTC()
	}
	return snapshot, nil
}

func gatewayRequestDispatchAuditFromSnapshot(normalized GatewayRequestDispatchSnapshot, ownerInstanceID string, ownerFenceToken uint64, at time.Time) *GatewayRequestDispatchAudit {
	audit := &GatewayRequestDispatchAudit{
		LogicalRequestTraceID:                 normalized.LogicalRequestTraceID,
		TelemetryVersion:                      normalized.TelemetryVersion,
		RoutingMode:                           normalized.RoutingMode,
		GatewayApp:                            normalized.GatewayApp,
		PhysicalBillingInvariantVersion:       GatewayRequestDispatchPhysicalBillingInvariantVersion,
		TransportFenceVersion:                 GatewayRequestDispatchTransportFenceVersion,
		TransportState:                        GatewayRequestDispatchTransportStatePrepared,
		TransportConfirmationVersion:          1,
		PriorPhysicallyDispatchedFailureCount: normalized.PriorPhysicallyDispatchedFailureCount,
		RealFallback:                          normalized.RealFallback,
		FirstByteBudgetMs:                     normalized.FirstByteBudgetMs,
		RouteExclusionSnapshotVersion:         normalized.RouteExclusionSnapshotVersion,
		RouteExclusionsSnapshot:               normalized.RouteExclusionsSnapshot,
		DownstreamKeyID:                       normalized.DownstreamKeyID,
		ChannelID:                             normalized.ChannelID,
		ChannelNameSnapshot:                   normalized.ChannelNameSnapshot,
		GroupName:                             normalized.GroupName,
		UpstreamKeyID:                         normalized.UpstreamKeyID,
		UpstreamKeyName:                       normalized.UpstreamKeyName,
		ModelName:                             normalized.ModelName,
		UpstreamModelName:                     normalized.UpstreamModelName,
		ReasoningEffort:                       normalized.ReasoningEffort,
		RequestedModelName:                    normalized.RequestedModelName,
		RequestedReasoningEffort:              normalized.RequestedReasoningEffort,
		ModelMappingStrategy:                  normalized.ModelMappingStrategy,
		ModelMappingDetails:                   normalized.ModelMappingDetails,
		Method:                                normalized.Method,
		Path:                                  normalized.Path,
		ClientIP:                              normalized.ClientIP,
		ClientIPSource:                        normalized.ClientIPSource,
		SessionHash:                           normalized.SessionHash,
		RouteReason:                           normalized.RouteReason,
		RouteSelectionRank:                    normalized.RouteSelectionRank,
		SessionAffinityOriginRequestID:        normalized.SessionAffinityOriginRequestID,
		SessionAffinityExpiresAt:              cloneGatewayRequestDispatchTime(normalized.SessionAffinityExpiresAt),
		SessionAffinityOverriddenCandidates:   normalized.SessionAffinityOverriddenCandidates,
		RouteDetails:                          normalized.RouteDetails,
		RouteAttemptedChannels:                normalized.RouteAttemptedChannels,
		GroupRatio:                            normalized.GroupRatio,
		CompletionRatio:                       normalized.CompletionRatio,
		RawGroupRatio:                         normalized.RawGroupRatio,
		RawCompletionRatio:                    normalized.RawCompletionRatio,
		FundingFactor:                         normalized.FundingFactor,
		CostBasisVersion:                      normalized.CostBasisVersion,
		CostBasisSource:                       normalized.CostBasisSource,
		MultiplierProvenanceVersion:           GatewayRequestDispatchMultiplierProvenanceVersion,
		ChannelIdentityRevision:               normalized.ChannelIdentityRevision,
		UpstreamGroupID:                       normalized.UpstreamGroupID,
		UpstreamGroupGeneration:               normalized.UpstreamGroupGeneration,
		UpstreamKeyLocalID:                    normalized.UpstreamKeyLocalID,
		UpstreamKeyGeneration:                 normalized.UpstreamKeyGeneration,
		PriceOperation:                        normalized.PriceOperation,
		PriceProtocol:                         normalized.PriceProtocol,
		UsageSchema:                           normalized.UsageSchema,
		UsageSchemaVersion:                    normalized.UsageSchemaVersion,
		UsageOperationPresent:                 normalized.UsageOperationPresent,
		OperationUnits:                        cloneGatewayRequestPricingInt64(normalized.OperationUnits),
		RequestStartedAt:                      normalized.RequestStartedAt,
		OwnerInstanceID:                       ownerInstanceID,
		OwnerFenceToken:                       ownerFenceToken,
		Stage:                                 GatewayRequestDispatchStageDispatched,
		// A preparation audit is deliberately never billed. For image
		// operations the billing commitment is set only by the single
		// owner-fenced transaction that records transport entry below.
		BillingCommitted: false,
		DispatchedAt:     at,
		CreatedAt:        at,
	}
	if normalized.RolloverPredecessorID != 0 {
		predecessorID := normalized.RolloverPredecessorID
		audit.RolloverPredecessorID = &predecessorID
	}
	return audit
}

// gatewayRequestDispatchRequiresBillingCommitAtPhysicalDispatch identifies a
// frozen, potentially chargeable image operation. It intentionally relies on
// the dispatch snapshot instead of the mutable route or a request path alone:
// /v1/responses can carry either ordinary text or an image_generation tool.
func gatewayRequestDispatchRequiresBillingCommitAtPhysicalDispatch(audit *GatewayRequestDispatchAudit) bool {
	if audit == nil || audit.PriceOperation != GatewayImageOperationGeneration ||
		audit.UsageSchema != GatewayPricingUsageSchemaOperationV1 ||
		!audit.UsageOperationPresent || audit.OperationUnits == nil || *audit.OperationUnits != 1 {
		return false
	}
	switch audit.Path {
	case "/v1/images/generations":
		return audit.PriceProtocol == GatewayImageProtocolImages
	case "/v1/responses":
		return audit.PriceProtocol == GatewayImageProtocolResponses
	default:
		return false
	}
}

func gatewayRequestDispatchHasTrustedBillingCommitment(audit *GatewayRequestDispatchAudit) bool {
	if audit == nil || !audit.BillingCommitted {
		return false
	}
	// Historical rows predate the transport fence and retain their original
	// billing-evidence semantics. Only v1 can distinguish armed from confirmed.
	if audit.TransportFenceVersion == 0 {
		return true
	}
	return gatewayRequestDispatchHasConfirmedPhysicalTransport(audit)
}

func gatewayRequestDispatchHasConfirmedPhysicalTransport(audit *GatewayRequestDispatchAudit) bool {
	if audit == nil || !audit.PhysicallyDispatched {
		return false
	}
	if audit.TransportFenceVersion == 0 {
		return true
	}
	return audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && audit.TransportState == GatewayRequestDispatchTransportStateConfirmed
}

func gatewayRequestDispatchHasArmedPhysicalTransport(audit *GatewayRequestDispatchAudit) bool {
	return audit != nil && audit.PhysicallyDispatched && audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && audit.TransportState == GatewayRequestDispatchTransportStateArmed
}

func gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(audit *GatewayRequestDispatchAudit) bool {
	// Migration 000043 introduced physically_dispatched with a false default.
	// Pre-000048 billing rows therefore cannot be reclassified from that field:
	// version 0 keeps the historical commitment semantics. Only a v1 audit is
	// known to have been created under the coupled physical/billing contract.
	return audit != nil && audit.PhysicalBillingInvariantVersion == GatewayRequestDispatchPhysicalBillingInvariantVersion && audit.BillingCommitted && !audit.PhysicallyDispatched
}

// BeginRequestDispatch durably records the physical attempt before the HTTP
// transport is invoked. A caller must fail closed when this insert fails. The
// row is always non-billable at this point: pre-Do work is not evidence that a
// non-idempotent image request reached an upstream.
func (g *Gateway) BeginRequestDispatch(snapshot GatewayRequestDispatchSnapshot, at time.Time) (*GatewayRequestDispatchAudit, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	at = normalizeGatewayRequestDispatchTime(at)
	normalized, err := normalizeGatewayRequestDispatchSnapshot(snapshot, at)
	if err != nil {
		return nil, err
	}
	if normalized.RolloverPredecessorID != 0 || normalized.PriorPhysicallyDispatchedFailureCount != 0 || normalized.RealFallback {
		return nil, errors.New("initial request dispatch cannot carry rollover provenance")
	}
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return nil, err
	}
	audit := gatewayRequestDispatchAuditFromSnapshot(normalized, ownerInstanceID, ownerFenceToken, at)
	auditCreated := false
	if err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, audit, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if err := freezeGatewayRequestPricingTx(tx, audit, at); err != nil {
			return err
		}
		if err := validateGatewayRequestDispatchPriceFactSnapshotTx(tx, audit); err != nil {
			return err
		}
		if err := tx.Create(audit).Error; err != nil {
			return err
		}
		auditCreated = true
		return nil
	}); err != nil {
		if auditCreated {
			return audit, fmt.Errorf("%w: %v", ErrGatewayRequestDispatchCommitUnknown, err)
		}
		return nil, err
	}
	return audit, nil
}

// BeginNextRequestDispatch creates a linked successor without closing its
// predecessor. The caller must arm the successor immediately before RoundTrip,
// confirm it after RoundTrip returns, and only then close the predecessor with
// ClosePreviousRequestDispatchAfterNextPhysical.
func (g *Gateway) BeginNextRequestDispatch(previousID uint64, nextSnapshot GatewayRequestDispatchSnapshot, at time.Time) (*GatewayRequestDispatchAudit, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	at = normalizeGatewayRequestDispatchTime(at)
	normalizedNext, err := normalizeGatewayRequestDispatchSnapshot(nextSnapshot, at)
	if err != nil {
		return nil, err
	}
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return nil, err
	}
	var next *GatewayRequestDispatchAudit
	nextCreated := false
	err = g.db.Transaction(func(tx *gorm.DB) error {
		previous, err := lockGatewayRequestDispatchAuditTx(tx, previousID)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, previous, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if previous.TerminalAt != nil || previous.RequestLogID != nil ||
			(previous.Stage != GatewayRequestDispatchStageDispatched && previous.Stage != GatewayRequestDispatchStageResponded) ||
			!gatewayRequestDispatchHasConfirmedPhysicalTransport(previous) {
			return ErrGatewayRequestDispatchConflict
		}
		if previous.AcceptedAt != nil || previous.BillingCommitted {
			return ErrGatewayRequestDispatchRequiresFinalization
		}
		if previous.TelemetryVersion != normalizedNext.TelemetryVersion ||
			(previous.TelemetryVersion != 0 && previous.LogicalRequestTraceID != normalizedNext.LogicalRequestTraceID) {
			return ErrGatewayRequestDispatchConflict
		}
		expectedPriorFailures := 0
		if normalizedNext.TelemetryVersion != 0 {
			expectedPriorFailures = previous.PriorPhysicallyDispatchedFailureCount + 1
		}
		if normalizedNext.PriorPhysicallyDispatchedFailureCount != expectedPriorFailures {
			return ErrGatewayRequestDispatchConflict
		}
		if normalizedNext.RolloverPredecessorID != 0 && normalizedNext.RolloverPredecessorID != previous.ID {
			return ErrGatewayRequestDispatchConflict
		}
		normalizedNext.RolloverPredecessorID = previous.ID
		normalizedNext.RealFallback = previous.RealFallback || previous.ChannelID != normalizedNext.ChannelID ||
			!strings.EqualFold(strings.TrimSpace(previous.GroupName), strings.TrimSpace(normalizedNext.GroupName))
		next = gatewayRequestDispatchAuditFromSnapshot(normalizedNext, ownerInstanceID, ownerFenceToken, at)
		if err := freezeGatewayRequestPricingTx(tx, next, at); err != nil {
			return err
		}
		if err := validateGatewayRequestDispatchPriceFactSnapshotTx(tx, next); err != nil {
			return err
		}
		if err := tx.Create(next).Error; err != nil {
			return err
		}
		nextCreated = true
		return nil
	})
	if err != nil {
		if nextCreated {
			return next, fmt.Errorf("%w: %v", ErrGatewayRequestDispatchCommitUnknown, err)
		}
		return nil, err
	}
	return next, nil
}

// MarkRequestDispatchPhysicallyDispatched records that net/http.Client and the
// dispatch wrapper passed request validation and entered the configured
// RoundTripper. It does not assert socket flush, remote receipt, or upstream
// acceptance. Image operations must use
// MarkRequestDispatchPhysicallyDispatchedAndCommitBilling instead, so an
// image can never become physically dispatched without the paired billing
// commitment in the same owner-fenced transaction.
func (g *Gateway) MarkRequestDispatchPhysicallyDispatched(id uint64, at time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	at = normalizeGatewayRequestDispatchTime(at)
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	markApplied := false
	transactionErr := g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, audit, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if audit.PhysicallyDispatched {
			if audit.BillingCommitted {
				return ErrGatewayRequestDispatchBillingCommitInvalid
			}
			if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion &&
				audit.TransportState != GatewayRequestDispatchTransportStateArmed &&
				audit.TransportState != GatewayRequestDispatchTransportStateConfirmed {
				return ErrGatewayRequestDispatchConflict
			}
			return nil
		}
		if audit.BillingCommitted || gatewayRequestDispatchRequiresBillingCommitAtPhysicalDispatch(audit) {
			return ErrGatewayRequestDispatchBillingCommitRequired
		}
		if audit.TerminalAt != nil || audit.RequestLogID != nil || audit.Stage != GatewayRequestDispatchStageDispatched {
			return ErrGatewayRequestDispatchConflict
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND physically_dispatched = FALSE AND billing_committed = FALSE AND transport_fence_version = ? AND transport_state = ? AND stage = ? AND terminal_at IS NULL AND request_log_id IS NULL", audit.ID, GatewayRequestDispatchTransportFenceVersion, GatewayRequestDispatchTransportStatePrepared, GatewayRequestDispatchStageDispatched).
			Updates(map[string]any{
				"physically_dispatched": true,
				"transport_state":       GatewayRequestDispatchTransportStateArmed,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		// The marker runs before base.RoundTrip. If COMMIT becomes ambiguous after
		// this update, the caller can still prove that no transport attempt ran and
		// must hand off the possible marker through the narrow pre-Do recovery path.
		markApplied = true
		return nil
	})
	if transactionErr != nil && markApplied {
		return fmt.Errorf("%w: %v", ErrGatewayRequestDispatchCommitUnknown, transactionErr)
	}
	return transactionErr
}

// MarkRequestDispatchPhysicallyDispatchedAndCommitBilling records the exact
// pre-Do boundary for a non-idempotent image operation. Both facts are written
// by one transaction after locking the audit and confirming the exact active
// owner fence; a process cannot persist only one side of this boundary.
func (g *Gateway) MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(id uint64, at time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	at = normalizeGatewayRequestDispatchTime(at)
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	markApplied := false
	transactionErr := g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, audit, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if audit.PhysicallyDispatched || audit.BillingCommitted {
			if audit.PhysicallyDispatched && audit.BillingCommitted && gatewayRequestDispatchRequiresBillingCommitAtPhysicalDispatch(audit) &&
				audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion &&
				(audit.TransportState == GatewayRequestDispatchTransportStateArmed || audit.TransportState == GatewayRequestDispatchTransportStateConfirmed) {
				return nil
			}
			return ErrGatewayRequestDispatchConflict
		}
		if !gatewayRequestDispatchRequiresBillingCommitAtPhysicalDispatch(audit) {
			return ErrGatewayRequestDispatchBillingCommitInvalid
		}
		if audit.PhysicalBillingInvariantVersion != GatewayRequestDispatchPhysicalBillingInvariantVersion {
			return ErrGatewayRequestDispatchBillingCommitInvalid
		}
		if audit.TransportFenceVersion != GatewayRequestDispatchTransportFenceVersion {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.TerminalAt != nil || audit.RequestLogID != nil || audit.Stage != GatewayRequestDispatchStageDispatched {
			return ErrGatewayRequestDispatchConflict
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND physically_dispatched = FALSE AND billing_committed = FALSE AND transport_fence_version = ? AND transport_state = ? AND stage = ? AND terminal_at IS NULL AND request_log_id IS NULL", audit.ID, GatewayRequestDispatchTransportFenceVersion, GatewayRequestDispatchTransportStatePrepared, GatewayRequestDispatchStageDispatched).
			Updates(map[string]any{
				"physically_dispatched": true,
				"billing_committed":     true,
				"transport_state":       GatewayRequestDispatchTransportStateArmed,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		// GORM has assigned the update result before Transaction commits. If
		// COMMIT then returns an ambiguous transport error, the caller must
		// treat this as unsent because the base RoundTripper was not invoked.
		markApplied = true
		return nil
	})
	if transactionErr != nil && markApplied {
		return fmt.Errorf("%w: %v", ErrGatewayRequestDispatchCommitUnknown, transactionErr)
	}
	return transactionErr
}

// ConfirmRequestDispatchTransportInvocation promotes the durable pre-call
// marker only after the configured RoundTripper returned control. Until this
// owner-fenced transition succeeds, startup recovery treats the attempt as
// nonbillable/manual-review evidence rather than an automatic request charge.
func (g *Gateway) ConfirmRequestDispatchTransportInvocation(id uint64, at time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	at = normalizeGatewayRequestDispatchTime(at)
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	confirmationApplied := false
	transactionErr := g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, audit, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if audit.TransportFenceVersion != GatewayRequestDispatchTransportFenceVersion || !audit.PhysicallyDispatched ||
			audit.Stage != GatewayRequestDispatchStageDispatched || audit.ResponseObservedAt != nil || audit.AcceptedAt != nil ||
			audit.TerminalAt != nil || audit.RequestLogID != nil {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.TransportState == GatewayRequestDispatchTransportStateConfirmed {
			if audit.TransportConfirmationVersion != 1 || !audit.TransportReturnConfirmed || audit.TransportReturnConfirmedAt == nil {
				return ErrGatewayRequestDispatchConflict
			}
			return nil
		}
		if audit.TransportState != GatewayRequestDispatchTransportStateArmed || audit.TransportConfirmationVersion != 1 || audit.TransportReturnConfirmed || audit.TransportReturnConfirmedAt != nil {
			return ErrGatewayRequestDispatchConflict
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND transport_fence_version = ? AND transport_state = ? AND transport_confirmation_version = 1 AND transport_return_confirmed = FALSE AND transport_return_confirmed_at IS NULL AND physically_dispatched = TRUE AND terminal_at IS NULL AND request_log_id IS NULL", audit.ID, GatewayRequestDispatchTransportFenceVersion, GatewayRequestDispatchTransportStateArmed).
			Updates(map[string]any{
				"transport_state":               GatewayRequestDispatchTransportStateConfirmed,
				"transport_return_confirmed":    true,
				"transport_return_confirmed_at": at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		confirmationApplied = true
		return nil
	})
	if transactionErr != nil && confirmationApplied {
		return fmt.Errorf("%w: %v", ErrGatewayRequestDispatchCommitUnknown, transactionErr)
	}
	return transactionErr
}

// BeginNextRequestDispatchAndClosePrevious atomically transfers ownership of
// a logical request from one failed physical attempt to the next. The previous
// attempt is completed without a visible log when it has no billing evidence,
// or finalized with a hidden exact-ID evidence row when it does. The next
// audit is created in the same transaction and must exist before its HTTP Do.
func (g *Gateway) BeginNextRequestDispatchAndClosePrevious(
	previousID uint64,
	previousEvidence *GatewayRequestLog,
	previousTerminalKind string,
	previousFacts GatewayRequestDispatchFinalizationFacts,
	previousFinishedAt time.Time,
	nextSnapshot GatewayRequestDispatchSnapshot,
	nextDispatchedAt time.Time,
) (next *GatewayRequestDispatchAudit, previousLogCreated bool, err error) {
	if g == nil || g.db == nil {
		return nil, false, errors.New("gateway store is unavailable")
	}
	previousTerminalKind, err = validateGatewayRequestDispatchText("previous request dispatch terminal kind", previousTerminalKind, 64, true)
	if err != nil {
		return nil, false, err
	}
	previousFinishedAt = normalizeGatewayRequestDispatchTime(previousFinishedAt)
	nextDispatchedAt = normalizeGatewayRequestDispatchTime(nextDispatchedAt)
	previousFacts, err = normalizeGatewayRequestDispatchFinalizationFacts(previousFacts, previousFinishedAt)
	if err != nil {
		return nil, false, err
	}
	if previousFacts.Accepted {
		return nil, false, ErrGatewayRequestDispatchRequiresFinalization
	}
	normalizedNext, err := normalizeGatewayRequestDispatchSnapshot(nextSnapshot, nextDispatchedAt)
	if err != nil {
		return nil, false, err
	}
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return nil, false, err
	}
	originalEvidenceID := uint(0)
	if previousEvidence != nil {
		originalEvidenceID = previousEvidence.ID
		if previousEvidence.ID != 0 || previousEvidence.RecordKind != GatewayRequestRecordKindDispatchCostEvidence {
			return nil, false, errors.New("previous request dispatch evidence log is invalid")
		}
		if previousEvidence.CreatedAt.IsZero() {
			previousEvidence.CreatedAt = previousFinishedAt
		} else {
			previousEvidence.CreatedAt = previousEvidence.CreatedAt.UTC()
		}
		if previousEvidence.CostReconcileAt != nil {
			value := previousEvidence.CostReconcileAt.UTC()
			previousEvidence.CostReconcileAt = &value
		}
	}
	transactionBodyApplied := false
	err = g.db.Transaction(func(tx *gorm.DB) error {
		previous, err := lockGatewayRequestDispatchAuditTx(tx, previousID)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, previous, ownerInstanceID, ownerFenceToken, previousFinishedAt); err != nil {
			return err
		}
		if previous.TerminalAt != nil || previous.RequestLogID != nil ||
			(previous.Stage != GatewayRequestDispatchStageDispatched && previous.Stage != GatewayRequestDispatchStageResponded) ||
			!gatewayRequestDispatchHasConfirmedPhysicalTransport(previous) {
			return ErrGatewayRequestDispatchConflict
		}
		if previous.AcceptedAt != nil || previous.BillingCommitted {
			return ErrGatewayRequestDispatchRequiresFinalization
		}
		if previous.TelemetryVersion != normalizedNext.TelemetryVersion ||
			(previous.TelemetryVersion != 0 && previous.LogicalRequestTraceID != normalizedNext.LogicalRequestTraceID) {
			return ErrGatewayRequestDispatchConflict
		}
		expectedPriorFailures := 0
		if normalizedNext.TelemetryVersion != 0 {
			expectedPriorFailures = previous.PriorPhysicallyDispatchedFailureCount + 1
		}
		if normalizedNext.PriorPhysicallyDispatchedFailureCount != expectedPriorFailures {
			return ErrGatewayRequestDispatchConflict
		}
		if normalizedNext.RolloverPredecessorID != 0 && normalizedNext.RolloverPredecessorID != previous.ID {
			return ErrGatewayRequestDispatchConflict
		}
		normalizedNext.RolloverPredecessorID = previous.ID
		normalizedNext.RealFallback = previous.RealFallback || previous.ChannelID != normalizedNext.ChannelID ||
			!strings.EqualFold(strings.TrimSpace(previous.GroupName), strings.TrimSpace(normalizedNext.GroupName))
		next = gatewayRequestDispatchAuditFromSnapshot(normalizedNext, ownerInstanceID, ownerFenceToken, nextDispatchedAt)
		if err := freezeGatewayRequestPricingTx(tx, next, nextDispatchedAt); err != nil {
			return err
		}
		if err := validateGatewayRequestDispatchPriceFactSnapshotTx(tx, next); err != nil {
			return err
		}
		if err := tx.Create(next).Error; err != nil {
			return err
		}
		nextUpdates := map[string]any{
			"physically_dispatched": true,
			"transport_state":       GatewayRequestDispatchTransportStateArmed,
		}
		if gatewayRequestDispatchRequiresBillingCommitAtPhysicalDispatch(next) {
			nextUpdates["billing_committed"] = true
			next.BillingCommitted = true
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND transport_fence_version = ? AND transport_state = ? AND physically_dispatched = FALSE AND billing_committed = FALSE AND terminal_at IS NULL AND request_log_id IS NULL", next.ID, GatewayRequestDispatchTransportFenceVersion, GatewayRequestDispatchTransportStatePrepared).
			Updates(nextUpdates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		next.PhysicallyDispatched = true
		next.TransportState = GatewayRequestDispatchTransportStateArmed
		providerRequestID := previous.ProviderRequestID
		if previousFacts.ProviderRequestID != "" {
			if providerRequestID != "" && providerRequestID != previousFacts.ProviderRequestID {
				return ErrGatewayRequestDispatchConflict
			}
			providerRequestID = previousFacts.ProviderRequestID
		}
		updates := map[string]any{
			"terminal_at":   previousFinishedAt,
			"terminal_kind": previousTerminalKind,
		}
		if previous.ResponseObservedAt == nil && !previousFacts.ResponseObservedAt.IsZero() {
			updates["response_observed_at"] = previousFacts.ResponseObservedAt
		}
		if previous.ProviderRequestID == "" && providerRequestID != "" {
			updates["provider_request_id"] = providerRequestID
		}
		if providerRequestID == "" {
			if previousEvidence != nil {
				return ErrGatewayRequestDispatchConflict
			}
			updates["stage"] = GatewayRequestDispatchStageCompleted
		} else {
			if previousEvidence == nil {
				return ErrGatewayRequestDispatchHasCostEvidence
			}
			if value := previousEvidence.UpstreamRequestID; value != "" && value != providerRequestID {
				return ErrGatewayRequestDispatchConflict
			}
			previous.ProviderRequestID = providerRequestID
			previousEvidence.UpstreamRequestID = providerRequestID
			applyGatewayRequestDispatchSnapshotToLog(previous, previousEvidence)
			finalizeGatewayRequestPricing(previous, previousEvidence)
			if err := g.appendRequestLogTx(tx, previousEvidence, true); err != nil {
				return err
			}
			updates["stage"] = GatewayRequestDispatchStageFinalized
			updates["request_log_id"] = previousEvidence.ID
		}
		result = tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND stage = ? AND terminal_at IS NULL AND request_log_id IS NULL", previous.ID, previous.Stage).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		previousLogCreated = previousEvidence != nil
		transactionBodyApplied = true
		return nil
	})
	if err != nil {
		if previousEvidence != nil {
			previousEvidence.ID = originalEvidenceID
		}
		if transactionBodyApplied {
			// The full transaction body was applied and only COMMIT was unobservable.
			// Return the assigned successor so the service can fence both audits.
			return next, false, fmt.Errorf("%w: %v", ErrGatewayRequestDispatchCommitUnknown, err)
		}
		return nil, false, err
	}
	return next, previousLogCreated, nil
}

// ClosePreviousRequestDispatchAfterNextPhysical closes a failed physical
// attempt only after its successor has entered the transport boundary. The API
// calls it after the successor's RoundTripper has returned, so a canceled or
// failed pre-Do preparation cannot turn the prior physical attempt into hidden
// completion/evidence. A provider-ID-only predecessor remains hidden only for
// that proven intermediate rollover.
func (g *Gateway) ClosePreviousRequestDispatchAfterNextPhysical(
	previousID uint64,
	nextID uint64,
	previousEvidence *GatewayRequestLog,
	previousTerminalKind string,
	previousFacts GatewayRequestDispatchFinalizationFacts,
	previousFinishedAt time.Time,
) (previousLogCreated bool, err error) {
	if g == nil || g.db == nil {
		return false, errors.New("gateway store is unavailable")
	}
	previousTerminalKind, err = validateGatewayRequestDispatchText("previous request dispatch terminal kind", previousTerminalKind, 64, true)
	if err != nil {
		return false, err
	}
	previousFinishedAt = normalizeGatewayRequestDispatchTime(previousFinishedAt)
	previousFacts, err = normalizeGatewayRequestDispatchFinalizationFacts(previousFacts, previousFinishedAt)
	if err != nil {
		return false, err
	}
	if previousFacts.Accepted {
		return false, ErrGatewayRequestDispatchRequiresFinalization
	}
	originalEvidenceID := uint(0)
	if previousEvidence != nil {
		originalEvidenceID = previousEvidence.ID
		if previousEvidence.ID != 0 || previousEvidence.RecordKind != GatewayRequestRecordKindDispatchCostEvidence {
			return false, errors.New("previous request dispatch evidence log is invalid")
		}
		if previousEvidence.CreatedAt.IsZero() {
			previousEvidence.CreatedAt = previousFinishedAt
		} else {
			previousEvidence.CreatedAt = previousEvidence.CreatedAt.UTC()
		}
		if previousEvidence.CostReconcileAt != nil {
			value := previousEvidence.CostReconcileAt.UTC()
			previousEvidence.CostReconcileAt = &value
		}
	}
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return false, err
	}
	previousClosed := false
	err = g.db.Transaction(func(tx *gorm.DB) error {
		previous, err := lockGatewayRequestDispatchAuditTx(tx, previousID)
		if err != nil {
			return err
		}
		next, err := lockGatewayRequestDispatchAuditTx(tx, nextID)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, previous, ownerInstanceID, ownerFenceToken, previousFinishedAt); err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, next, ownerInstanceID, ownerFenceToken, previousFinishedAt); err != nil {
			return err
		}
		if !gatewayRequestDispatchHasConfirmedPhysicalTransport(next) || next.TerminalAt != nil || next.RequestLogID != nil ||
			next.RolloverPredecessorID == nil || *next.RolloverPredecessorID != previous.ID ||
			next.TelemetryVersion != previous.TelemetryVersion ||
			(next.TelemetryVersion != 0 && (next.LogicalRequestTraceID != previous.LogicalRequestTraceID || next.PriorPhysicallyDispatchedFailureCount != previous.PriorPhysicallyDispatchedFailureCount+1)) {
			return ErrGatewayRequestDispatchConflict
		}
		if !gatewayRequestDispatchHasConfirmedPhysicalTransport(previous) || previous.TerminalAt != nil || previous.RequestLogID != nil || (previous.Stage != GatewayRequestDispatchStageDispatched && previous.Stage != GatewayRequestDispatchStageResponded) {
			return ErrGatewayRequestDispatchConflict
		}
		if previous.AcceptedAt != nil || previous.BillingCommitted {
			return ErrGatewayRequestDispatchRequiresFinalization
		}
		providerRequestID := previous.ProviderRequestID
		if previousFacts.ProviderRequestID != "" {
			if providerRequestID != "" && providerRequestID != previousFacts.ProviderRequestID {
				return ErrGatewayRequestDispatchConflict
			}
			providerRequestID = previousFacts.ProviderRequestID
		}
		updates := map[string]any{
			"terminal_at":   previousFinishedAt,
			"terminal_kind": previousTerminalKind,
		}
		if previous.ResponseObservedAt == nil && !previousFacts.ResponseObservedAt.IsZero() {
			updates["response_observed_at"] = previousFacts.ResponseObservedAt
		}
		if previous.ProviderRequestID == "" && providerRequestID != "" {
			updates["provider_request_id"] = providerRequestID
		}
		if providerRequestID == "" {
			if previousEvidence != nil {
				return ErrGatewayRequestDispatchConflict
			}
			updates["stage"] = GatewayRequestDispatchStageCompleted
		} else {
			if previousEvidence == nil {
				return ErrGatewayRequestDispatchHasCostEvidence
			}
			if value := previousEvidence.UpstreamRequestID; value != "" && value != providerRequestID {
				return ErrGatewayRequestDispatchConflict
			}
			previous.ProviderRequestID = providerRequestID
			previousEvidence.UpstreamRequestID = providerRequestID
			applyGatewayRequestDispatchSnapshotToLog(previous, previousEvidence)
			finalizeGatewayRequestPricing(previous, previousEvidence)
			if err := g.appendRequestLogTx(tx, previousEvidence, true); err != nil {
				return err
			}
			updates["stage"] = GatewayRequestDispatchStageFinalized
			updates["request_log_id"] = previousEvidence.ID
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND stage = ? AND physically_dispatched = TRUE AND terminal_at IS NULL AND request_log_id IS NULL", previous.ID, previous.Stage).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		previousClosed = true
		previousLogCreated = previousEvidence != nil
		return nil
	})
	if err != nil {
		if previousEvidence != nil {
			previousEvidence.ID = originalEvidenceID
		}
		if previousClosed {
			return false, fmt.Errorf("%w: %v", ErrGatewayRequestDispatchCommitUnknown, err)
		}
		return false, err
	}
	return previousLogCreated, nil
}

func lockGatewayRequestDispatchAuditTx(tx *gorm.DB, id uint64) (*GatewayRequestDispatchAudit, error) {
	if id == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	var audit GatewayRequestDispatchAudit
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&audit, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &audit, nil
}

// RecordRequestDispatchResponse persists the provider's authoritative billing
// request ID as soon as response headers exist. An empty ID still records the
// response boundary without inventing a match key.
func (g *Gateway) RecordRequestDispatchResponse(id uint64, providerRequestID string, at time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	providerRequestID, err := providerid.Normalize(providerRequestID)
	if err != nil {
		return fmt.Errorf("provider request id: %w", err)
	}
	at = normalizeGatewayRequestDispatchTime(at)
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, audit, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && !gatewayRequestDispatchHasConfirmedPhysicalTransport(audit) {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.ProviderRequestID != "" && providerRequestID != "" && audit.ProviderRequestID != providerRequestID {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.TerminalAt != nil {
			if audit.ResponseObservedAt != nil && (providerRequestID == "" || audit.ProviderRequestID == providerRequestID) {
				return nil
			}
			return ErrGatewayRequestDispatchConflict
		}
		if audit.Stage != GatewayRequestDispatchStageDispatched && audit.Stage != GatewayRequestDispatchStageResponded && audit.Stage != GatewayRequestDispatchStageAccepted {
			return ErrGatewayRequestDispatchConflict
		}
		updates := map[string]any{}
		if audit.ResponseObservedAt == nil {
			updates["response_observed_at"] = at
		}
		if audit.ProviderRequestID == "" && providerRequestID != "" {
			updates["provider_request_id"] = providerRequestID
		}
		if audit.Stage == GatewayRequestDispatchStageDispatched {
			updates["stage"] = GatewayRequestDispatchStageResponded
		}
		if len(updates) == 0 {
			return nil
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND stage = ? AND terminal_at IS NULL", audit.ID, audit.Stage).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		return nil
	})
}

// MarkRequestDispatchAccepted persists protocol-whitelisted acceptance before
// any accepted frame is released downstream. acceptedObjectID is optional at
// the storage layer because some validated Claude fallback events carry no
// message ID; the API parser owns event-specific ID requirements.
func (g *Gateway) MarkRequestDispatchAccepted(id uint64, acceptedEventType, acceptedObjectID string, at time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	var err error
	if acceptedEventType, err = validateGatewayRequestDispatchText("accepted event type", acceptedEventType, 64, true); err != nil {
		return err
	}
	if acceptedObjectID, err = validateGatewayRequestDispatchText("accepted object id", acceptedObjectID, 128, false); err != nil {
		return err
	}
	at = normalizeGatewayRequestDispatchTime(at)
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, audit, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && !gatewayRequestDispatchHasConfirmedPhysicalTransport(audit) {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.AcceptedAt != nil {
			if audit.AcceptedEventType == acceptedEventType && audit.AcceptedObjectID == acceptedObjectID {
				return nil
			}
			return ErrGatewayRequestDispatchConflict
		}
		if audit.TerminalAt != nil || audit.Stage != GatewayRequestDispatchStageResponded || audit.ResponseObservedAt == nil {
			return ErrGatewayRequestDispatchConflict
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND stage = ? AND response_observed_at IS NOT NULL AND accepted_at IS NULL AND terminal_at IS NULL", audit.ID, audit.Stage).
			Updates(map[string]any{
				"stage":               GatewayRequestDispatchStageAccepted,
				"accepted_at":         at,
				"accepted_event_type": acceptedEventType,
				"accepted_object_id":  acceptedObjectID,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		return nil
	})
}

// CompleteRequestDispatch closes only a preparation audit that has not entered
// the physical transport boundary and has no durable acceptance or possible
// provider billing evidence. A physically dispatched terminal attempt must be
// finalized with a normal request log; only the rollover-specific primitive
// above may hide a predecessor after a proven next physical attempt.
func (g *Gateway) CompleteRequestDispatch(id uint64, terminalKind string, at time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	terminalKind, err := validateGatewayRequestDispatchText("request dispatch terminal kind", terminalKind, 64, true)
	if err != nil {
		return err
	}
	at = normalizeGatewayRequestDispatchTime(at)
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireActiveGatewayRequestDispatchOwnerTx(tx, audit, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if audit.Stage == GatewayRequestDispatchStageCompleted && audit.TerminalAt != nil && audit.TerminalKind == terminalKind {
			return nil
		}
		untrustedPrePhysicalBilling := gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(audit)
		if audit.PhysicallyDispatched || audit.AcceptedAt != nil || (audit.BillingCommitted && !untrustedPrePhysicalBilling) {
			return ErrGatewayRequestDispatchRequiresFinalization
		}
		if audit.ProviderRequestID != "" {
			return ErrGatewayRequestDispatchHasCostEvidence
		}
		if audit.TerminalAt != nil || (audit.Stage != GatewayRequestDispatchStageDispatched && audit.Stage != GatewayRequestDispatchStageResponded) {
			return ErrGatewayRequestDispatchConflict
		}
		updates := map[string]any{
			"stage":         GatewayRequestDispatchStageCompleted,
			"terminal_at":   at,
			"terminal_kind": terminalKind,
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion {
			if audit.TransportState != GatewayRequestDispatchTransportStatePrepared && audit.TransportState != GatewayRequestDispatchTransportStateNotDispatched {
				return ErrGatewayRequestDispatchConflict
			}
			updates["transport_state"] = GatewayRequestDispatchTransportStateNotDispatched
		}
		if untrustedPrePhysicalBilling {
			updates["billing_committed"] = false
			updates["physically_dispatched"] = false
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND stage = ? AND physically_dispatched = FALSE AND terminal_at IS NULL AND accepted_at IS NULL AND provider_request_id = '' AND (billing_committed = FALSE OR (physical_billing_invariant_version = 1 AND physically_dispatched = FALSE))", audit.ID, audit.Stage).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		return nil
	})
}

// CompleteRecoveredRequestDispatch closes one queued nonphysical,
// evidence-free attempt after its original owner was stopped/expired or
// explicitly handed it off.
func (g *Gateway) CompleteRecoveredRequestDispatch(id uint64, terminalKind string, legacyBefore, at time.Time) error {
	return g.completeRecoveredRequestDispatch(id, terminalKind, legacyBefore, at, false)
}

func (g *Gateway) CompleteDurableRequestDispatchHandoff(id uint64, terminalKind string, legacyBefore, at time.Time) error {
	return g.completeRecoveredRequestDispatch(id, terminalKind, legacyBefore, at, true)
}

func (g *Gateway) completeRecoveredRequestDispatch(id uint64, terminalKind string, legacyBefore, at time.Time, durableHandoff bool) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	terminalKind, err := validateGatewayRequestDispatchText("request dispatch terminal kind", terminalKind, 64, true)
	if err != nil {
		return err
	}
	legacyBefore = normalizeGatewayRequestDispatchTime(legacyBefore)
	at = normalizeGatewayRequestDispatchTime(at)
	return g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireRecoverableGatewayRequestDispatchOwnerTx(tx, audit, legacyBefore, at); err != nil {
			return err
		}
		handoffExists, err := gatewayRequestDispatchTerminalHandoffExistsTx(tx, audit.ID)
		if err != nil {
			return err
		}
		if handoffExists && !durableHandoff {
			return ErrGatewayRequestDispatchRecoveryRequested
		}
		if !handoffExists && durableHandoff && audit.TerminalAt == nil {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.Stage == GatewayRequestDispatchStageCompleted && audit.TerminalAt != nil && audit.TerminalKind == terminalKind {
			return nil
		}
		untrustedPrePhysicalBilling := gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(audit)
		if audit.TerminalAt != nil || audit.PhysicallyDispatched || audit.AcceptedAt != nil || (audit.BillingCommitted && !untrustedPrePhysicalBilling) || audit.ProviderRequestID != "" {
			return ErrGatewayRequestDispatchConflict
		}
		updates := map[string]any{
			"stage":         GatewayRequestDispatchStageCompleted,
			"terminal_at":   at,
			"terminal_kind": terminalKind,
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion {
			if audit.TransportState != GatewayRequestDispatchTransportStatePrepared && audit.TransportState != GatewayRequestDispatchTransportStateNotDispatched {
				return ErrGatewayRequestDispatchConflict
			}
			updates["transport_state"] = GatewayRequestDispatchTransportStateNotDispatched
		}
		if untrustedPrePhysicalBilling {
			updates["billing_committed"] = false
			updates["physically_dispatched"] = false
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND stage = ? AND physically_dispatched = FALSE AND terminal_at IS NULL AND accepted_at IS NULL AND provider_request_id = '' AND (billing_committed = FALSE OR (physical_billing_invariant_version = 1 AND physically_dispatched = FALSE))", audit.ID, audit.Stage).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		return nil
	})
}

// HandoffRequestDispatchRecovery durably declares that the live handler has
// stopped using this physical attempt. The exact active owner is the only
// writer allowed to request the handoff; after it commits, ordinary live
// lifecycle writes are fenced and a recovery worker may finalize the row even
// while the process-level owner lease remains active.
func (g *Gateway) HandoffRequestDispatchRecovery(id uint64, terminalKind string, at time.Time) error {
	return g.HandoffRequestDispatchRecoveryWithFacts(id, terminalKind, GatewayRequestDispatchFinalizationFacts{}, at)
}

// HandoffUnsentRequestDispatchRecovery is used only when the caller knows it
// returned from the pre-Do audit hook without invoking the upstream client.
// Unlike the paired-marker commit-unknown path below, this ordinary handoff
// accepts only an audit which is already nonphysical and nonbillable. It must
// never erase a real physical dispatch marker.
func (g *Gateway) HandoffUnsentRequestDispatchRecovery(id uint64, terminalKind string, at time.Time) error {
	return g.handoffUnsentRequestDispatchRecovery(id, terminalKind, at, false, false)
}

// HandoffUnknownPairedPhysicalBillingCommitRecovery is the deliberately
// narrow recovery path for ErrGatewayRequestDispatchCommitUnknown returned by
// MarkRequestDispatchPhysicallyDispatchedAndCommitBilling. The paired marker
// is invoked before base.RoundTrip, so its caller can prove that no upstream
// transport attempt has started. An ambiguous COMMIT may have persisted the
// v1 physical/billing pair; this method permits clearing exactly that pair
// (or the rolled-back false/false state) before the owner is fenced. No
// ordinary unsent, rollover, or post-RoundTrip path may call this method.
func (g *Gateway) HandoffUnknownPairedPhysicalBillingCommitRecovery(id uint64, terminalKind string, at time.Time) error {
	return g.handoffUnsentRequestDispatchRecovery(id, terminalKind, at, true, false)
}

// HandoffUnknownPhysicalDispatchMarkRecovery is the narrow pre-RoundTripper
// recovery path for an ambiguous plain-text physical marker. The marker runs
// before base.RoundTrip, so its caller can prove that the possible true marker
// did not correspond to an upstream attempt and may clear it before fencing
// this owner. It must never be used after the transport hook returns.
func (g *Gateway) HandoffUnknownPhysicalDispatchMarkRecovery(id uint64, terminalKind string, at time.Time) error {
	return g.handoffUnsentRequestDispatchRecovery(id, terminalKind, at, false, true)
}

func (g *Gateway) handoffUnsentRequestDispatchRecovery(id uint64, terminalKind string, at time.Time, allowUnknownPairedPhysicalBillingCommit, allowUnknownPhysicalDispatchMark bool) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	terminalKind, err := validateGatewayRequestDispatchText("request dispatch recovery terminal kind", terminalKind, 64, true)
	if err != nil {
		return err
	}
	at = normalizeGatewayRequestDispatchTime(at)
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireGatewayRequestDispatchOwnerLeaseActiveTx(tx, audit, ownerInstanceID, ownerFenceToken); err != nil {
			return err
		}
		if audit.PhysicalBillingInvariantVersion != GatewayRequestDispatchPhysicalBillingInvariantVersion {
			return ErrGatewayRequestDispatchBillingCommitInvalid
		}
		if audit.TerminalAt != nil {
			return nil
		}
		if audit.Stage != GatewayRequestDispatchStageDispatched || audit.ResponseObservedAt != nil || audit.AcceptedAt != nil || audit.ProviderRequestID != "" {
			return ErrGatewayRequestDispatchConflict
		}
		if !allowUnknownPairedPhysicalBillingCommit && !allowUnknownPhysicalDispatchMark && (audit.PhysicallyDispatched || audit.BillingCommitted) {
			return ErrGatewayRequestDispatchConflict
		}
		if allowUnknownPairedPhysicalBillingCommit && audit.PhysicallyDispatched != audit.BillingCommitted {
			return ErrGatewayRequestDispatchConflict
		}
		if allowUnknownPhysicalDispatchMark && audit.BillingCommitted {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.RecoveryRequestedAt != nil && audit.RecoveryTerminalKind == terminalKind && !audit.BillingCommitted && !audit.PhysicallyDispatched && audit.TransportState == GatewayRequestDispatchTransportStateNotDispatched {
			return nil
		}
		if audit.TransportState != GatewayRequestDispatchTransportStatePrepared && audit.TransportState != GatewayRequestDispatchTransportStateArmed {
			return ErrGatewayRequestDispatchConflict
		}
		updates := map[string]any{
			"billing_committed":      false,
			"physically_dispatched":  false,
			"transport_state":        GatewayRequestDispatchTransportStateNotDispatched,
			"recovery_requested_at":  at,
			"recovery_terminal_kind": terminalKind,
		}
		query := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND physical_billing_invariant_version = ? AND transport_fence_version = ? AND transport_state IN ? AND stage = ? AND terminal_at IS NULL AND request_log_id IS NULL AND response_observed_at IS NULL AND accepted_at IS NULL AND provider_request_id = ''", audit.ID, GatewayRequestDispatchPhysicalBillingInvariantVersion, GatewayRequestDispatchTransportFenceVersion, []string{GatewayRequestDispatchTransportStatePrepared, GatewayRequestDispatchTransportStateArmed}, audit.Stage)
		if allowUnknownPairedPhysicalBillingCommit {
			query = query.Where("(physically_dispatched = FALSE AND billing_committed = FALSE) OR (physically_dispatched = TRUE AND billing_committed = TRUE)")
		} else if allowUnknownPhysicalDispatchMark {
			query = query.Where("(physically_dispatched = FALSE AND billing_committed = FALSE) OR (physically_dispatched = TRUE AND billing_committed = FALSE)")
		} else {
			query = query.Where("physically_dispatched = FALSE AND billing_committed = FALSE")
		}
		result := query.Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		return nil
	})
}

// HandoffRequestDispatchRecoveryWithFacts is the queue-saturation fail-safe.
// It durably fences live writes and preserves any response/acceptance evidence
// in one transaction, even though the exact in-memory terminal log could not
// be retained in the bounded queue.
func (g *Gateway) HandoffRequestDispatchRecoveryWithFacts(id uint64, terminalKind string, facts GatewayRequestDispatchFinalizationFacts, at time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	terminalKind, err := validateGatewayRequestDispatchText("request dispatch recovery terminal kind", terminalKind, 64, true)
	if err != nil {
		return err
	}
	at = normalizeGatewayRequestDispatchTime(at)
	facts, err = normalizeGatewayRequestDispatchFinalizationFacts(facts, at)
	if err != nil {
		return err
	}
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if err := requireGatewayRequestDispatchOwnerLeaseActiveTx(tx, audit, ownerInstanceID, ownerFenceToken); err != nil {
			return err
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && !gatewayRequestDispatchHasConfirmedPhysicalTransport(audit) {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.TerminalAt != nil {
			return nil
		}
		if audit.RecoveryRequestedAt != nil && audit.RecoveryTerminalKind != terminalKind {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.Stage != GatewayRequestDispatchStageDispatched && audit.Stage != GatewayRequestDispatchStageResponded && audit.Stage != GatewayRequestDispatchStageAccepted {
			return ErrGatewayRequestDispatchConflict
		}
		if facts.ProviderRequestID != "" && audit.ProviderRequestID != "" && facts.ProviderRequestID != audit.ProviderRequestID {
			return ErrGatewayRequestDispatchConflict
		}
		if facts.Accepted && audit.AcceptedAt != nil && (audit.AcceptedEventType != facts.AcceptedEventType || audit.AcceptedObjectID != facts.AcceptedObjectID) {
			return ErrGatewayRequestDispatchConflict
		}
		updates := map[string]any{}
		if audit.RecoveryRequestedAt == nil {
			updates["recovery_requested_at"] = at
			updates["recovery_terminal_kind"] = terminalKind
		}
		if audit.ResponseObservedAt == nil && !facts.ResponseObservedAt.IsZero() {
			updates["response_observed_at"] = facts.ResponseObservedAt
		}
		if audit.ProviderRequestID == "" && facts.ProviderRequestID != "" {
			updates["provider_request_id"] = facts.ProviderRequestID
		}
		if audit.AcceptedAt == nil && facts.Accepted {
			updates["stage"] = GatewayRequestDispatchStageAccepted
			updates["accepted_at"] = facts.AcceptedAt
			updates["accepted_event_type"] = facts.AcceptedEventType
			updates["accepted_object_id"] = facts.AcceptedObjectID
		} else if audit.Stage == GatewayRequestDispatchStageDispatched && (!facts.ResponseObservedAt.IsZero() || facts.ProviderRequestID != "") {
			updates["stage"] = GatewayRequestDispatchStageResponded
		}
		if len(updates) == 0 {
			return nil
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND stage = ? AND terminal_at IS NULL", audit.ID, audit.Stage).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		return nil
	})
}

// HandoffLinkedRequestDispatchRecovery atomically surrenders both sides of a
// rollover after its commit result becomes unknown. The predecessor may
// already be terminal when COMMIT actually succeeded; the successor is still
// fenced in the same transaction so a retry can never leave just one side
// recoverable. knownUnsentSuccessor is restricted to the legacy pre-Do rollover
// path and clears only its armed marker.
func (g *Gateway) HandoffLinkedRequestDispatchRecovery(previousID, nextID uint64, terminalKind string, previousFacts GatewayRequestDispatchFinalizationFacts, knownUnsentSuccessor bool, at time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	if previousID == 0 || nextID == 0 || previousID == nextID {
		return errors.New("linked request dispatch ids are invalid")
	}
	terminalKind, err := validateGatewayRequestDispatchText("request dispatch recovery terminal kind", terminalKind, 64, true)
	if err != nil {
		return err
	}
	at = normalizeGatewayRequestDispatchTime(at)
	previousFacts, err = normalizeGatewayRequestDispatchFinalizationFacts(previousFacts, at)
	if err != nil {
		return err
	}
	ownerInstanceID, ownerFenceToken, err := g.ensureRequestDispatchOwner()
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		firstID, secondID := previousID, nextID
		if secondID < firstID {
			firstID, secondID = secondID, firstID
		}
		first, err := lockGatewayRequestDispatchAuditTx(tx, firstID)
		if err != nil {
			return err
		}
		second, err := lockGatewayRequestDispatchAuditTx(tx, secondID)
		if err != nil {
			return err
		}
		previous, next := first, second
		if previous.ID != previousID {
			previous, next = second, first
		}
		if next.RolloverPredecessorID == nil || *next.RolloverPredecessorID != previous.ID ||
			previous.OwnerInstanceID != next.OwnerInstanceID || previous.OwnerFenceToken != next.OwnerFenceToken ||
			previous.OwnerInstanceID != ownerInstanceID || previous.OwnerFenceToken != ownerFenceToken {
			return ErrGatewayRequestDispatchConflict
		}
		if previousFacts.ProviderRequestID != "" && previous.ProviderRequestID != "" && previousFacts.ProviderRequestID != previous.ProviderRequestID {
			return ErrGatewayRequestDispatchConflict
		}
		if previousFacts.Accepted && previous.AcceptedAt != nil && (previousFacts.AcceptedEventType != previous.AcceptedEventType || previousFacts.AcceptedObjectID != previous.AcceptedObjectID) {
			return ErrGatewayRequestDispatchConflict
		}
		for _, audit := range []*GatewayRequestDispatchAudit{previous, next} {
			if audit.RecoveryRequestedAt != nil && audit.RecoveryTerminalKind != terminalKind {
				return ErrGatewayRequestDispatchConflict
			}
		}
		if err := requireGatewayRequestDispatchOwnerLeaseActiveTx(tx, next, ownerInstanceID, ownerFenceToken); err != nil {
			return err
		}
		if previous.TerminalAt == nil {
			updates := map[string]any{
				"recovery_requested_at":  at,
				"recovery_terminal_kind": terminalKind,
			}
			if previous.ProviderRequestID == "" && previousFacts.ProviderRequestID != "" {
				updates["provider_request_id"] = previousFacts.ProviderRequestID
			}
			if previous.ResponseObservedAt == nil && !previousFacts.ResponseObservedAt.IsZero() {
				updates["response_observed_at"] = previousFacts.ResponseObservedAt
			}
			if previous.AcceptedAt == nil && previousFacts.Accepted {
				updates["stage"] = GatewayRequestDispatchStageAccepted
				updates["accepted_at"] = previousFacts.AcceptedAt
				updates["accepted_event_type"] = previousFacts.AcceptedEventType
				updates["accepted_object_id"] = previousFacts.AcceptedObjectID
			}
			if result := tx.Model(&GatewayRequestDispatchAudit{}).Where("id = ? AND terminal_at IS NULL", previous.ID).Updates(updates); result.Error != nil {
				return result.Error
			} else if result.RowsAffected != 1 {
				return ErrGatewayRequestDispatchConflict
			}
		}
		if next.TerminalAt != nil {
			return nil
		}
		nextUpdates := map[string]any{
			"recovery_requested_at":  at,
			"recovery_terminal_kind": terminalKind,
		}
		if knownUnsentSuccessor {
			if next.TransportFenceVersion != GatewayRequestDispatchTransportFenceVersion || next.TransportState != GatewayRequestDispatchTransportStateArmed || next.ResponseObservedAt != nil || next.AcceptedAt != nil || next.ProviderRequestID != "" {
				return ErrGatewayRequestDispatchConflict
			}
			nextUpdates["transport_state"] = GatewayRequestDispatchTransportStateNotDispatched
			nextUpdates["physically_dispatched"] = false
			nextUpdates["billing_committed"] = false
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).Where("id = ? AND terminal_at IS NULL", next.ID).Updates(nextUpdates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		return nil
	})
}

func gatewayRequestDispatchIdentityConflict(audit *GatewayRequestDispatchAudit, log *GatewayRequestLog) bool {
	if audit == nil || log == nil {
		return true
	}
	if log.DownstreamKeyID != 0 && log.DownstreamKeyID != audit.DownstreamKeyID {
		return true
	}
	if log.ChannelID != 0 && log.ChannelID != audit.ChannelID {
		return true
	}
	if value := strings.TrimSpace(log.GroupName); value != "" && !strings.EqualFold(value, audit.GroupName) {
		return true
	}
	if value := strings.TrimSpace(log.Method); value != "" && !strings.EqualFold(value, audit.Method) {
		return true
	}
	if value := strings.TrimSpace(log.Path); value != "" && value != audit.Path {
		return true
	}
	if value := strings.TrimSpace(log.ModelName); value != "" && audit.ModelName != "" && value != audit.ModelName {
		return true
	}
	if value := strings.TrimSpace(log.ClientIP); value != "" && audit.ClientIP != "" && value != audit.ClientIP {
		return true
	}
	if value := strings.TrimSpace(log.SessionHash); value != "" && audit.SessionHash != "" && value != audit.SessionHash {
		return true
	}
	if value := strings.TrimSpace(log.RequestedModelName); value != "" && audit.RequestedModelName != "" && value != audit.RequestedModelName {
		return true
	}
	if log.SessionAffinityOriginRequestID != 0 && audit.SessionAffinityOriginRequestID != 0 && log.SessionAffinityOriginRequestID != audit.SessionAffinityOriginRequestID {
		return true
	}
	if log.SessionAffinityExpiresAt != nil && audit.SessionAffinityExpiresAt != nil && !gatewayRequestDispatchTimesEqual(log.SessionAffinityExpiresAt, audit.SessionAffinityExpiresAt) {
		return true
	}
	if value := strings.TrimSpace(log.SessionAffinityOverriddenCandidates); value != "" && strings.TrimSpace(audit.SessionAffinityOverriddenCandidates) != "" && value != audit.SessionAffinityOverriddenCandidates {
		return true
	}
	if value := strings.TrimSpace(log.LogicalRequestTraceID); value != "" && value != audit.LogicalRequestTraceID {
		return true
	}
	if log.TelemetryVersion != 0 && log.TelemetryVersion != audit.TelemetryVersion {
		return true
	}
	if value := strings.TrimSpace(log.RoutingMode); value != "" && audit.RoutingMode != "" && value != audit.RoutingMode {
		return true
	}
	if value := strings.TrimSpace(log.GatewayApp); value != "" && audit.GatewayApp != "" && value != audit.GatewayApp {
		return true
	}
	if log.RouteExclusionSnapshotVersion != 0 && audit.RouteExclusionSnapshotVersion != 0 && log.RouteExclusionSnapshotVersion != audit.RouteExclusionSnapshotVersion {
		return true
	}
	if value := strings.TrimSpace(log.RouteExclusionsSnapshot); value != "" && value != "{}" && audit.RouteExclusionsSnapshot != "" && !gatewayRequestDispatchJSONEquivalent(value, audit.RouteExclusionsSnapshot) {
		return true
	}
	return false
}

func gatewayRequestDispatchJSONEquivalent(left, right string) bool {
	_, normalizedLeft, err := normalizeGatewayRouteExclusionSnapshotText(left)
	if err != nil {
		return false
	}
	_, normalizedRight, err := normalizeGatewayRouteExclusionSnapshotText(right)
	if err != nil {
		return false
	}
	return normalizedLeft == normalizedRight
}

func applyGatewayRequestDispatchSnapshotToLog(audit *GatewayRequestDispatchAudit, log *GatewayRequestLog) {
	if audit.TelemetryVersion != 0 {
		log.TelemetryVersion = audit.TelemetryVersion
		log.LogicalRequestTraceID = audit.LogicalRequestTraceID
		log.RoutingMode = audit.RoutingMode
		log.GatewayApp = audit.GatewayApp
		log.PriorPhysicallyDispatchedFailureCount = audit.PriorPhysicallyDispatchedFailureCount
		log.RealFallback = audit.RealFallback
	}
	if audit.RouteExclusionSnapshotVersion != 0 {
		log.RouteExclusionSnapshotVersion = audit.RouteExclusionSnapshotVersion
		log.RouteExclusionsSnapshot = audit.RouteExclusionsSnapshot
	}
	log.DownstreamKeyID = audit.DownstreamKeyID
	log.ChannelID = audit.ChannelID
	log.ChannelNameSnapshot = audit.ChannelNameSnapshot
	log.GroupName = audit.GroupName
	log.UpstreamKeyID = audit.UpstreamKeyID
	log.UpstreamKeyName = audit.UpstreamKeyName
	if strings.TrimSpace(log.ModelName) == "" {
		log.ModelName = audit.ModelName
	}
	if strings.TrimSpace(log.UpstreamModelName) == "" {
		log.UpstreamModelName = audit.UpstreamModelName
	}
	if strings.TrimSpace(log.ReasoningEffort) == "" {
		log.ReasoningEffort = audit.ReasoningEffort
	}
	if strings.TrimSpace(log.RequestedModelName) == "" {
		log.RequestedModelName = audit.RequestedModelName
	}
	if strings.TrimSpace(log.RequestedReasoningEffort) == "" {
		log.RequestedReasoningEffort = audit.RequestedReasoningEffort
	}
	if strings.TrimSpace(log.ModelMappingStrategy) == "" {
		log.ModelMappingStrategy = audit.ModelMappingStrategy
	}
	if strings.TrimSpace(log.ModelMappingDetails) == "" {
		log.ModelMappingDetails = audit.ModelMappingDetails
	}
	log.Method = audit.Method
	log.Path = audit.Path
	log.ClientIP = audit.ClientIP
	log.ClientIPSource = audit.ClientIPSource
	log.SessionHash = audit.SessionHash
	if strings.TrimSpace(log.RouteReason) == "" {
		log.RouteReason = audit.RouteReason
	}
	if log.RouteSelectionRank == 0 {
		log.RouteSelectionRank = audit.RouteSelectionRank
	}
	if log.SessionAffinityOriginRequestID == 0 {
		log.SessionAffinityOriginRequestID = audit.SessionAffinityOriginRequestID
	}
	if log.SessionAffinityExpiresAt == nil {
		log.SessionAffinityExpiresAt = cloneGatewayRequestDispatchTime(audit.SessionAffinityExpiresAt)
	}
	if strings.TrimSpace(log.SessionAffinityOverriddenCandidates) == "" {
		log.SessionAffinityOverriddenCandidates = audit.SessionAffinityOverriddenCandidates
	}
	if strings.TrimSpace(log.RouteDetails) == "" {
		log.RouteDetails = audit.RouteDetails
	}
	if strings.TrimSpace(log.RouteAttemptedChannels) == "" {
		log.RouteAttemptedChannels = audit.RouteAttemptedChannels
	}
	log.GroupRatio = audit.GroupRatio
	log.CompletionRatio = audit.CompletionRatio
	log.RawGroupRatio = cloneGatewayRequestDispatchNumber(audit.RawGroupRatio)
	log.RawCompletionRatio = cloneGatewayRequestDispatchNumber(audit.RawCompletionRatio)
	log.FundingFactor = cloneGatewayRequestDispatchNumber(audit.FundingFactor)
	log.CostBasisVersion = audit.CostBasisVersion
	log.CostBasisSource = audit.CostBasisSource
	basisAt := audit.DispatchedAt.UTC()
	log.FundingBasisAt = &basisAt
	log.ChannelIdentityRevision = audit.ChannelIdentityRevision
	log.UpstreamGroupID = audit.UpstreamGroupID
	log.UpstreamGroupGeneration = audit.UpstreamGroupGeneration
	log.UpstreamKeyLocalID = audit.UpstreamKeyLocalID
	log.UpstreamKeyGeneration = audit.UpstreamKeyGeneration
	log.PriceFactID = cloneGatewayRequestDispatchUint64(audit.PriceFactID)
	log.PriceCostUnitEpoch = audit.PriceCostUnitEpoch
	log.PriceFactVersion = audit.PriceFactVersion
	log.PriceFactSchemaVersion = audit.PriceFactSchemaVersion
	log.PriceFactChecksum = audit.PriceFactChecksum
	log.PriceCanonicalGroupName = audit.PriceCanonicalGroupName
	log.PriceModelName = audit.PriceModelName
	log.PriceOperation = audit.PriceOperation
	log.PriceProtocol = audit.PriceProtocol
	log.PriceInputUnit = cloneGatewayRequestDispatchNumber(audit.PriceInputUnit)
	log.PriceOutputUnit = cloneGatewayRequestDispatchNumber(audit.PriceOutputUnit)
	log.PriceCacheReadUnit = cloneGatewayRequestDispatchNumber(audit.PriceCacheReadUnit)
	log.PriceCacheWriteUnit = cloneGatewayRequestDispatchNumber(audit.PriceCacheWriteUnit)
	log.PriceLongContextThresholdTokens = cloneGatewayRequestPricingInt64(audit.PriceLongContextThresholdTokens)
	log.PriceLongContextInputUnit = cloneGatewayRequestDispatchNumber(audit.PriceLongContextInputUnit)
	log.PriceLongContextOutputUnit = cloneGatewayRequestDispatchNumber(audit.PriceLongContextOutputUnit)
	log.PriceLongContextCacheReadUnit = cloneGatewayRequestDispatchNumber(audit.PriceLongContextCacheReadUnit)
	log.PriceLongContextCacheWriteUnit = cloneGatewayRequestDispatchNumber(audit.PriceLongContextCacheWriteUnit)
	log.PriceOperationUnit = cloneGatewayRequestDispatchNumber(audit.PriceOperationUnit)
	log.PriceCurrency = audit.PriceCurrency
	log.PriceUnit = audit.PriceUnit
	log.PriceSource = audit.PriceSource
	log.PriceObservedAt = cloneGatewayRequestPricingTime(audit.PriceObservedAt)
	log.PriceValidFrom = cloneGatewayRequestPricingTime(audit.PriceValidFrom)
	log.PriceValidUntil = cloneGatewayRequestPricingTime(audit.PriceValidUntil)
	log.UsageSchema = audit.UsageSchema
	log.UsageSchemaVersion = audit.UsageSchemaVersion
	if audit.UsageOperationPresent {
		log.UsageOperationPresent = true
		log.OperationUnits = cloneGatewayRequestPricingInt64(audit.OperationUnits)
	}
	log.CostUnavailableReason = audit.PricingUnavailableReason
	log.UpstreamAccepted = audit.AcceptedAt != nil
	log.BillingCommitted = gatewayRequestDispatchHasTrustedBillingCommitment(audit)
	if log.UpstreamRequestID == "" {
		log.UpstreamRequestID = audit.ProviderRequestID
	}
}

func cloneGatewayRequestDispatchUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// FinalizeRequestDispatchResult atomically appends the final request log
// (including aggregate updates) and links it to the dispatch audit. Created is
// true only when this call inserted the request log. Retrying after an
// ambiguous commit loads the already-linked request log and returns false
// without creating a duplicate log or aggregate contribution.
func (g *Gateway) FinalizeRequestDispatchResult(id uint64, log *GatewayRequestLog, terminalKind string, at time.Time) (created bool, err error) {
	return g.finalizeRequestDispatchResult(id, log, terminalKind, GatewayRequestDispatchFinalizationFacts{}, time.Time{}, at, false, false)
}

// FinalizeRequestDispatchWithFactsResult is the live-owner retry path used by
// the bounded recovery queue. Facts and the exact final log commit together.
func (g *Gateway) FinalizeRequestDispatchWithFactsResult(id uint64, log *GatewayRequestLog, terminalKind string, facts GatewayRequestDispatchFinalizationFacts, at time.Time) (created bool, err error) {
	return g.finalizeRequestDispatchResult(id, log, terminalKind, facts, time.Time{}, at, false, false)
}

// FinalizeRecoveredRequestDispatchResult revalidates and fences an inactive
// owner in the same transaction that creates the synthetic recovery log.
func (g *Gateway) FinalizeRecoveredRequestDispatchResult(id uint64, log *GatewayRequestLog, terminalKind string, legacyBefore, at time.Time) (created bool, err error) {
	return g.finalizeRequestDispatchResult(id, log, terminalKind, GatewayRequestDispatchFinalizationFacts{}, legacyBefore, at, true, false)
}

// FinalizeRecoveredRequestDispatchWithFactsResult preserves a queued exact
// outcome after the original process owner has stopped or expired.
func (g *Gateway) FinalizeRecoveredRequestDispatchWithFactsResult(id uint64, log *GatewayRequestLog, terminalKind string, facts GatewayRequestDispatchFinalizationFacts, legacyBefore, at time.Time) (created bool, err error) {
	return g.finalizeRequestDispatchResult(id, log, terminalKind, facts, legacyBefore, at, true, false)
}

func (g *Gateway) FinalizeDurableRequestDispatchHandoffResult(id uint64, log *GatewayRequestLog, terminalKind string, facts GatewayRequestDispatchFinalizationFacts, legacyBefore, at time.Time) (created bool, err error) {
	return g.finalizeRequestDispatchResult(id, log, terminalKind, facts, legacyBefore, at, true, true)
}

func (g *Gateway) finalizeRequestDispatchResult(id uint64, log *GatewayRequestLog, terminalKind string, facts GatewayRequestDispatchFinalizationFacts, legacyBefore, at time.Time, recovery, durableHandoff bool) (created bool, err error) {
	if g == nil || g.db == nil {
		return false, errors.New("gateway store is unavailable")
	}
	if log == nil {
		return false, errors.New("request dispatch final log is required")
	}
	originalLogID := log.ID
	terminalKind, err = validateGatewayRequestDispatchText("request dispatch terminal kind", terminalKind, 64, true)
	if err != nil {
		return false, err
	}
	at = normalizeGatewayRequestDispatchTime(at)
	facts, err = normalizeGatewayRequestDispatchFinalizationFacts(facts, at)
	if err != nil {
		return false, err
	}
	if recovery {
		legacyBefore = normalizeGatewayRequestDispatchTime(legacyBefore)
	}
	var ownerInstanceID string
	var ownerFenceToken uint64
	if !recovery {
		ownerInstanceID, ownerFenceToken, err = g.ensureRequestDispatchOwner()
		if err != nil {
			return false, err
		}
	}
	if log.CreatedAt.IsZero() {
		log.CreatedAt = at
	} else {
		log.CreatedAt = log.CreatedAt.UTC()
	}
	if log.CostReconcileAt != nil {
		value := log.CostReconcileAt.UTC()
		log.CostReconcileAt = &value
	}
	err = g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
		if err != nil {
			return err
		}
		if recovery {
			if err := requireRecoverableGatewayRequestDispatchOwnerTx(tx, audit, legacyBefore, at); err != nil {
				return err
			}
		} else if err := requireActiveGatewayRequestDispatchOwnerTx(tx, audit, ownerInstanceID, ownerFenceToken, at); err != nil {
			return err
		}
		if recovery {
			handoffExists, err := gatewayRequestDispatchTerminalHandoffExistsTx(tx, audit.ID)
			if err != nil {
				return err
			}
			if handoffExists && !durableHandoff {
				return ErrGatewayRequestDispatchRecoveryRequested
			}
			if !handoffExists && durableHandoff && audit.TerminalAt == nil {
				return ErrGatewayRequestDispatchConflict
			}
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && !gatewayRequestDispatchHasConfirmedPhysicalTransport(audit) {
			return ErrGatewayRequestDispatchConflict
		}
		if facts.ProviderRequestID != "" && audit.ProviderRequestID != "" && facts.ProviderRequestID != audit.ProviderRequestID {
			return ErrGatewayRequestDispatchConflict
		}
		if facts.Accepted && audit.AcceptedAt != nil && (audit.AcceptedEventType != facts.AcceptedEventType || audit.AcceptedObjectID != facts.AcceptedObjectID) {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.Stage == GatewayRequestDispatchStageFinalized && audit.TerminalAt != nil && audit.RequestLogID != nil {
			if (facts.ProviderRequestID != "" && audit.ProviderRequestID != facts.ProviderRequestID) || (facts.Accepted && audit.AcceptedAt == nil) {
				return ErrGatewayRequestDispatchConflict
			}
			if originalLogID != 0 && originalLogID != *audit.RequestLogID {
				return ErrGatewayRequestDispatchConflict
			}
			var persisted GatewayRequestLog
			if err := tx.First(&persisted, "id = ?", *audit.RequestLogID).Error; err != nil {
				return err
			}
			*log = persisted
			return nil
		}
		if originalLogID != 0 {
			return errors.New("request dispatch final log must not already be persisted")
		}
		if audit.TerminalAt != nil || audit.Stage == GatewayRequestDispatchStageCompleted {
			return ErrGatewayRequestDispatchConflict
		}
		if gatewayRequestDispatchIdentityConflict(audit, log) {
			return ErrGatewayRequestDispatchConflict
		}
		trustedBillingCommitted := gatewayRequestDispatchHasTrustedBillingCommitment(audit)
		untrustedBillingCommitted := audit.BillingCommitted && !trustedBillingCommitted
		if log.BillingCommitted && !trustedBillingCommitted {
			return ErrGatewayRequestDispatchConflict
		}
		// Do not propagate the old pre-Do marker into an ordinary request log.
		// A physically-unconfirmed legacy marker is intentionally downgraded to
		// no billing commitment; acceptance/provider-ID evidence, if any, still
		// follows its own exact reconciliation path.
		if untrustedBillingCommitted {
			audit.BillingCommitted = false
		}
		providerRequestID := log.UpstreamRequestID
		if providerRequestID == "" {
			providerRequestID = facts.ProviderRequestID
		} else if facts.ProviderRequestID != "" && providerRequestID != facts.ProviderRequestID {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.ProviderRequestID != "" && providerRequestID != "" && audit.ProviderRequestID != providerRequestID {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.ProviderRequestID == "" && providerRequestID != "" {
			audit.ProviderRequestID = providerRequestID
		}
		if audit.ResponseObservedAt == nil && !facts.ResponseObservedAt.IsZero() {
			observedAt := facts.ResponseObservedAt
			audit.ResponseObservedAt = &observedAt
		}
		if audit.AcceptedAt == nil && facts.Accepted {
			acceptedAt := facts.AcceptedAt
			audit.AcceptedAt = &acceptedAt
			audit.AcceptedEventType = facts.AcceptedEventType
			audit.AcceptedObjectID = facts.AcceptedObjectID
		}
		if log.UpstreamAccepted && audit.AcceptedAt == nil {
			return ErrGatewayRequestDispatchConflict
		}
		if facts.Accepted {
			log.UpstreamAccepted = true
		}
		applyGatewayRequestDispatchSnapshotToLog(audit, log)
		finalizeGatewayRequestPricing(audit, log)
		if err := g.appendRequestLogTx(tx, log, true); err != nil {
			return err
		}
		if err := enqueueGatewayRoutingHealthSuccessTx(tx, audit, log, facts, at); err != nil {
			return err
		}
		updates := map[string]any{
			"stage":          GatewayRequestDispatchStageFinalized,
			"terminal_at":    at,
			"terminal_kind":  terminalKind,
			"request_log_id": log.ID,
		}
		if untrustedBillingCommitted {
			updates["billing_committed"] = false
			updates["physically_dispatched"] = false
		}
		if audit.ProviderRequestID == providerRequestID && providerRequestID != "" {
			updates["provider_request_id"] = providerRequestID
		}
		if audit.ResponseObservedAt != nil && (facts.ResponseObservedAt.IsZero() == false) {
			updates["response_observed_at"] = *audit.ResponseObservedAt
		}
		if facts.Accepted && audit.AcceptedAt != nil {
			updates["accepted_at"] = *audit.AcceptedAt
			updates["accepted_event_type"] = audit.AcceptedEventType
			updates["accepted_object_id"] = audit.AcceptedObjectID
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND stage = ? AND terminal_at IS NULL AND request_log_id IS NULL", audit.ID, audit.Stage).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		created = true
		return nil
	})
	if err != nil {
		log.ID = originalLogID
		return false, err
	}
	return created, nil
}

// FinalizeRequestDispatch retains the original error-only API for callers that
// do not need to distinguish a new request log from an idempotent retry.
func (g *Gateway) FinalizeRequestDispatch(id uint64, log *GatewayRequestLog, terminalKind string, at time.Time) error {
	_, err := g.FinalizeRequestDispatchResult(id, log, terminalKind, at)
	return err
}

func (g *Gateway) FindRequestDispatchAudit(id uint64) (*GatewayRequestDispatchAudit, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	var audit GatewayRequestDispatchAudit
	if err := g.db.First(&audit, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return &audit, nil
}

// ListRecoverableRequestDispatches returns physical attempts and attempts with
// durable acceptance, physically-confirmed billing, or exact provider-ID
// evidence whose owner is stopped/expired, plus attempts explicitly handed off
// by their still-active owner. Ownerless pre-migration rows alone use the
// conservative dispatched-at grace cutoff.
func (g *Gateway) ListRecoverableRequestDispatches(legacyBefore, _ time.Time, limit int) ([]GatewayRequestDispatchAudit, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	legacyBefore = normalizeGatewayRequestDispatchTime(legacyBefore)
	var audits []GatewayRequestDispatchAudit
	err := g.db.
		Where(`terminal_at IS NULL
			AND (
				(
					transport_fence_version = 0
					AND (physically_dispatched = TRUE OR accepted_at IS NOT NULL OR billing_committed = TRUE OR COALESCE(provider_request_id, '') <> '')
				)
				OR (
					transport_fence_version = 1
					AND transport_state = 'confirmed'
					AND physically_dispatched = TRUE
				)
			)
			AND NOT EXISTS (
				SELECT 1 FROM gateway_request_dispatch_terminal_handoffs AS handoff
				WHERE handoff.dispatch_id = gateway_request_dispatch_audits.id
			)
			AND (
				recovery_requested_at IS NOT NULL
				OR (legacy_ownerless = TRUE AND (owner_instance_id IS NULL OR owner_instance_id = '') AND (owner_fence_token IS NULL OR owner_fence_token = 0) AND dispatched_at <= ?)
				OR EXISTS (
					SELECT 1 FROM gateway_request_dispatch_owners AS owner
					WHERE owner.instance_id = gateway_request_dispatch_audits.owner_instance_id
						AND owner.fence_token = gateway_request_dispatch_audits.owner_fence_token
						AND (owner.stopped_at IS NOT NULL OR owner.lease_expires_at <= clock_timestamp())
				)
			)`, legacyBefore).
		Order("dispatched_at ASC, id ASC").
		Limit(limit).
		Find(&audits).Error
	return audits, err
}

// CompleteStaleRequestDispatches bounds non-billable rows after a crash. It
// records that no durable acceptance or provider-ID evidence survived; it does
// not claim the upstream definitively rejected the request and never touches
// rows with possible billable evidence.
func (g *Gateway) CompleteStaleRequestDispatches(legacyBefore, at time.Time, limit int) (int64, error) {
	if g == nil || g.db == nil {
		return 0, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	legacyBefore = normalizeGatewayRequestDispatchTime(legacyBefore)
	at = normalizeGatewayRequestDispatchTime(at)
	var affected int64
	err := g.db.Transaction(func(tx *gorm.DB) error {
		var ids []uint64
		if err := tx.Model(&GatewayRequestDispatchAudit{}).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where(`terminal_at IS NULL AND accepted_at IS NULL AND provider_request_id = ''
				AND (
					(
						transport_fence_version = 1
						AND transport_state IN ('prepared', 'armed', 'not_dispatched')
					)
					OR (
						transport_fence_version = 0
						AND physically_dispatched = FALSE
						AND (billing_committed = FALSE OR physical_billing_invariant_version = 1)
					)
				)
				AND NOT EXISTS (
					SELECT 1 FROM gateway_request_dispatch_terminal_handoffs AS handoff
					WHERE handoff.dispatch_id = gateway_request_dispatch_audits.id
				)
				AND (
					recovery_requested_at IS NOT NULL
					OR (legacy_ownerless = TRUE AND (owner_instance_id IS NULL OR owner_instance_id = '') AND (owner_fence_token IS NULL OR owner_fence_token = 0) AND dispatched_at <= ?)
					OR EXISTS (
						SELECT 1 FROM gateway_request_dispatch_owners AS owner
						WHERE owner.instance_id = gateway_request_dispatch_audits.owner_instance_id
							AND owner.fence_token = gateway_request_dispatch_audits.owner_fence_token
							AND (owner.stopped_at IS NOT NULL OR owner.lease_expires_at <= clock_timestamp())
					)
				)`, legacyBefore).
			Order("dispatched_at ASC, id ASC").
			Limit(limit).
			Pluck("id", &ids).Error; err != nil || len(ids) == 0 {
			return err
		}
		for _, id := range ids {
			audit, err := lockGatewayRequestDispatchAuditTx(tx, id)
			if err != nil {
				return err
			}
			handoffExists, err := gatewayRequestDispatchTerminalHandoffExistsTx(tx, audit.ID)
			if err != nil {
				return err
			}
			if handoffExists {
				continue
			}
			untrustedPrePhysicalBilling := gatewayRequestDispatchHasUntrustedPrePhysicalBillingCommitment(audit)
			armed := gatewayRequestDispatchHasArmedPhysicalTransport(audit)
			if audit.TerminalAt != nil || audit.AcceptedAt != nil || audit.ProviderRequestID != "" ||
				(audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && audit.TransportState == GatewayRequestDispatchTransportStateManualReview) ||
				(!armed && audit.PhysicallyDispatched) || (audit.BillingCommitted && !armed && !untrustedPrePhysicalBilling) {
				continue
			}
			if err := requireRecoverableGatewayRequestDispatchOwnerTx(tx, audit, legacyBefore, at); err != nil {
				if errors.Is(err, ErrGatewayRequestDispatchOwnerActive) {
					continue
				}
				return err
			}
			terminalKind := GatewayRequestDispatchTerminalOrphanedWithoutAcceptance
			if strings.TrimSpace(audit.RecoveryTerminalKind) != "" {
				terminalKind = audit.RecoveryTerminalKind
			} else if !audit.LegacyOwnerless {
				terminalKind = GatewayRequestDispatchTerminalOwnerLeaseLost
			}
			updates := map[string]any{
				"stage":         GatewayRequestDispatchStageCompleted,
				"terminal_at":   at,
				"terminal_kind": terminalKind,
			}
			if armed {
				updates["transport_state"] = GatewayRequestDispatchTransportStateManualReview
				updates["billing_committed"] = false
				updates["physically_dispatched"] = false
			} else if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion {
				updates["transport_state"] = GatewayRequestDispatchTransportStateNotDispatched
			} else if untrustedPrePhysicalBilling {
				updates["billing_committed"] = false
				updates["physically_dispatched"] = false
			}
			result := tx.Model(&GatewayRequestDispatchAudit{}).
				Where("id = ? AND stage = ? AND terminal_at IS NULL AND accepted_at IS NULL AND provider_request_id = ''", audit.ID, audit.Stage).
				Updates(updates)
			if result.Error != nil {
				return result.Error
			}
			affected += result.RowsAffected
		}
		return nil
	})
	return affected, err
}

// PruneRequestDispatchAudits provides bounded retention. Finalized audits may
// be deleted after their retention window (and also cascade when the linked
// request log is deleted). Pre-accept completed rows use a separate window.
// Accepted, billing-committed, or provider-ID rows without a final request log
// are never eligible for deletion.
func (g *Gateway) PruneRequestDispatchAudits(finalizedBefore, completedBefore time.Time, limit int) (int64, error) {
	if g == nil || g.db == nil {
		return 0, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	finalizedBefore = normalizeGatewayRequestDispatchTime(finalizedBefore)
	completedBefore = normalizeGatewayRequestDispatchTime(completedBefore)
	var affected int64
	err := g.db.Transaction(func(tx *gorm.DB) error {
		var ids []uint64
		if err := tx.Model(&GatewayRequestDispatchAudit{}).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where(`terminal_at IS NOT NULL AND (
					(request_log_id IS NOT NULL AND terminal_at < ?)
					OR
					(request_log_id IS NULL AND accepted_at IS NULL AND billing_committed = FALSE AND provider_request_id = '' AND terminal_at < ?)
				)
				AND NOT (transport_fence_version = 1 AND transport_state = 'manual_review')
				AND NOT EXISTS (
					SELECT 1 FROM gateway_request_dispatch_audits AS successor
					WHERE successor.rollover_predecessor_id = gateway_request_dispatch_audits.id
				)`, finalizedBefore, completedBefore).
			Order("terminal_at ASC, id ASC").
			Limit(limit).
			Pluck("id", &ids).Error; err != nil {
			return err
		}
		if len(ids) > 0 {
			result := tx.Where("id IN ?", ids).Delete(&GatewayRequestDispatchAudit{})
			if result.Error != nil {
				return result.Error
			}
			affected = result.RowsAffected
		}

		// Owner rows carry no request contents and are retained long enough for
		// audit correlation. Delete them only after every referencing dispatch
		// audit is gone; the foreign key remains a final fail-closed guard.
		var ownerIDs []string
		if err := tx.Model(&GatewayRequestDispatchOwner{}).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where(`COALESCE(stopped_at, lease_expires_at) < ?
				AND NOT EXISTS (
					SELECT 1 FROM gateway_request_dispatch_audits AS audit
					WHERE audit.owner_instance_id = gateway_request_dispatch_owners.instance_id
						AND audit.owner_fence_token = gateway_request_dispatch_owners.fence_token
				)`, finalizedBefore).
			Order("COALESCE(stopped_at, lease_expires_at) ASC, instance_id ASC").
			Limit(limit).
			Pluck("instance_id", &ownerIDs).Error; err != nil {
			return err
		}
		if len(ownerIDs) == 0 {
			return nil
		}
		return tx.Where("instance_id IN ?", ownerIDs).Delete(&GatewayRequestDispatchOwner{}).Error
	})
	return affected, err
}
