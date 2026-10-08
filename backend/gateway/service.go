package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

const (
	upstreamKeyName               = "BOG"
	directUpstreamKeyName         = storage.DirectChannelUpstreamKeyName
	upstreamProvisioningKeyPrefix = "BOG-p-"
	legacyUpstreamKeyName         = "BridgeOps Gateway"
	historicUpstreamKeyName       = "UpstreamOps Gateway"
)
const modelCapabilityMaxAge = 24 * time.Hour
const modelSyncErrorRetryInterval = 5 * time.Minute
const upstreamKeyProvisioningStaleAfter = 10 * time.Minute
const defaultRoutingLatencyWindow = 7 * 24 * time.Hour
const defaultRoutingPriceScoreMax = 70
const defaultRoutingLatencyScoreMax = 30
const defaultRoutingLatencySampleSize = 10
const routingLatencyFullScoreMs = 10_000
const routingLatencyZeroScoreMs = 80_000
const defaultRoutingUnknownLatencyScoreFactor = 0.8
const defaultRoutingResponseTimeout = 30 * time.Second
const defaultRoutingCacheInputPriceMultiplier = 1
const defaultRoutingCachePriceMinInputTokens int64 = 100_000_000
const defaultRoutingCachePriceWindowInputTokens int64 = 100_000_000
const defaultRoutingCacheStatsMaxAge = 7 * 24 * time.Hour
const routingCacheStatsRefreshInterval = time.Minute
const routingCacheStatsFailureBackoff = 15 * time.Second
const routingCacheStatsQueryTimeout = 750 * time.Millisecond
const maximumRoutingCachePriceInputTokens int64 = 1_000_000_000_000
const maximumRoutingCacheStatsMaxAge = 365 * 24 * time.Hour
const defaultManualRoutingCooldown = 10 * time.Minute
const upstreamKeyLookupPageSize = 50
const upstreamKeyLookupMaxPages = 10
const latencyProbeRouteReason = "latency_probe"
const maxUsageConflictRetries = 3
const maxCostReconcileAttempts = 96
const maxDeterministicCostReconcileAttempts = 3
const maxCostReconcileAge = 7 * 24 * time.Hour
const costReconcileLease = time.Minute
const costReconcilePollInterval = time.Second
const costReconcileCleanupBatch = 100
const costReconcileCleanupInterval = time.Minute
const historicalCostReconcileBackfillBatch = 100
const downstreamKeyTestTokenTTL = time.Minute
const maxPendingDownstreamKeyTestTokens = 64
const gatewayStateTouchInterval = time.Minute
const upstreamIdentityRepairRetryInterval = 5 * time.Minute
const managedUpstreamKeyRemovalStaleAfter = 10 * time.Minute
const modelProbeClientCacheLimit = 32
const gatewayProbeResponseMaxBytes = 1 << 20
const gatewayCapacityNoticeMaxBytes = 256
const latencyProbeStateCleanupInterval = time.Hour
const managedUpstreamKeyRebuildPendingError = storage.GatewayManagedKeyRebuildPendingError

const gatewayUnmarkedCapacityNotice = "our servers are currently overloaded please try again later"

var gatewayCapacityNotices = []string{
	"selected model is at capacity",
	"selected model is at capacity please try a different model",
	"selected model is at capacity please select a different model",
	"selected model is at capacity please try another model",
	"the selected model is at capacity",
	"the selected model is at capacity please try a different model",
	"the selected model is at capacity please select a different model",
	"model is at capacity",
	"model is currently at capacity",
	"model is at capacity please try a different model",
	"model is currently at capacity please try a different model",
	gatewayUnmarkedCapacityNotice,
}

var errAccountConcurrencyProbeDeferred = errors.New("upstream account concurrency probe deferred")

var modelProbeClients = struct {
	sync.Mutex
	values map[string]*http.Client
}{values: make(map[string]*http.Client)}

var upstreamProvisioningKeyNamePattern = regexp.MustCompile(`^BOG-p-[0-9a-f]{16}$`)

type KeyManager interface {
	ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error)
	ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error)
	CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error)
	RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error)
	PrepareManagedAPIKeyDeletion(ctx context.Context, channelID uint, expected connector.APIKey, expectedIdentityRevision uint64) (func() error, error)
}

type requestCostManager interface {
	LookupRequestCost(ctx context.Context, channelID uint, query connector.RequestCostQuery) (*connector.RequestCostResult, error)
}

type RoutingConfig struct {
	WaitingProbeInterval         time.Duration
	WaitingProbeTimeout          time.Duration
	PriceScoreMax                float64
	LatencyScoreMax              float64
	LatencySamples               int
	LatencyWindow                time.Duration
	LatencyFullScore             time.Duration
	LatencyZeroScore             time.Duration
	UnknownLatencyScoreFactor    float64
	FailurePolicy                RoutingFailurePolicy
	SessionAffinityMaxAge        time.Duration
	SessionAffinityStableSamples int
	CacheInputPriceMultiplier    float64
	CachePriceMinInputTokens     int64
	CachePriceWindowInputTokens  int64
	CacheStatsMaxAge             time.Duration
}

// UpstreamKeyLease freezes the exact secret and remote identity used for one
// upstream dispatch. It is request-scoped and must never be persisted or
// exposed outside the gateway transport path.
type UpstreamKeyLease struct {
	Key             string
	UpstreamKeyID   int64
	UpstreamKeyName string
	LocalKeyID      uint
	KeyGeneration   uint64

	// ChannelSnapshot is populated when preparing the key required a
	// channel-locked slow path. In particular, Sub2API discovery may persist a
	// dedicated API origin and advance the account identity revision while the
	// key is being prepared. The caller must use this snapshot instead of the
	// pre-discovery routing candidate so the first real request is not sent to
	// the homepage with a key created for the newly discovered API origin.
	ChannelSnapshot            *storage.Channel
	UpstreamAccountConcurrency *storage.UpstreamAccountConcurrencySnapshot
}

// RoutingFailurePolicy holds thresholds for business-defined failures that
// should feed the existing adaptive-routing cooldown.
type RoutingFailurePolicy struct {
	ResponseTimeout time.Duration
	ManualCooldown  time.Duration
}

type Service struct {
	Channels *storage.Channels
	Rates    *storage.Rates
	Store    *storage.Gateway
	Cipher   *appcrypto.Cipher
	Keys     KeyManager
	Log      *slog.Logger
	Costing  *costing.Service

	costLookupSlots                chan struct{}
	costWorkerOnce                 sync.Once
	costWorkerWake                 chan struct{}
	costIdentityLimiter            *costReconcileIdentityLimiter
	costCleanupMu                  sync.Mutex
	costCleanupAt                  time.Time
	requestDispatchOwnerStartOnce  sync.Once
	requestDispatchOwnerStartErr   error
	requestDispatchOwnerMu         sync.RWMutex
	requestDispatchOwner           *requestDispatchOwnerLease
	requestDispatchOwnerRecoveryMu sync.Mutex
	requestDispatchOwnerRecovering bool
	requestDispatchOwnerStopping   bool
	requestDispatchRecoveryOnce    sync.Once
	requestDispatchRecoveryMu      sync.Mutex
	requestDispatchRecoveryQueue   map[uint64]*requestDispatchRecoveryHandoff
	requestDispatchRecoveryWake    chan struct{}
	// requestDispatchHandoffRetryQueue holds exact terminal snapshots whose
	// durable handoff failed. They keep the old owner's drain gate closed until
	// the database acknowledges them or their bounded budget expires.
	requestDispatchHandoffRetryMu    sync.Mutex
	requestDispatchHandoffRetryQueue map[uint64]*requestDispatchTerminalHandoffRetry
	modelSyncMu                      sync.Mutex
	modelSyncLocks                   map[uint]chan struct{}
	routingMu                        sync.RWMutex
	routing                          RoutingConfig
	cacheStatsMu                     sync.RWMutex
	cacheStatsAt                     time.Time
	cacheStatsConfig                 cacheStatsConfigKey
	cacheStats                       map[candidateLatencyKey]cacheRatioStat
	cacheStatsAverage                float64
	cacheStatsHasAverage             bool
	cacheStatsFailureAt              time.Time
	cacheStatsFailureConfig          cacheStatsConfigKey
	cacheStatsRefreshing             bool
	cacheStatsActiveConfig           cacheStatsConfigKey
	cacheStatsQueued                 *cacheStatsRefreshRequest
	cacheStatsLoader                 cacheStatsLoader
	latencyCache                     routingLatencyCache
	modelPriceCache                  routingModelPriceCache
	testTokenMu                      sync.Mutex
	testTokens                       map[string]downstreamKeyTestToken
	touchMu                          sync.Mutex
	upstreamTouchAt                  map[upstreamTouchKey]time.Time
	upstreamIdentityRepairAt         map[upstreamTouchKey]time.Time
	probeStateCleanupMu              sync.Mutex
	probeStateCleanupAt              time.Time
	accountConcurrencyMu             sync.Mutex
	accountConcurrencyGovernor       *AccountConcurrencyGovernor
	accountConcurrencyPolicy         AccountConcurrencyPolicy
}

type upstreamTouchKey struct {
	channelID               uint
	channelIdentityRevision uint64
	groupName               string
	upstreamGroupID         uint
	presenceGeneration      uint64
}

type downstreamKeyTestToken struct {
	DownstreamKeyID uint
	ExpiresAt       time.Time
}

type cacheStatsConfigKey struct {
	Multiplier   float64
	MinTokens    int64
	WindowTokens int64
	MaxAge       time.Duration
	RoutesKey    string
}

type cacheStatsRefreshRequest struct {
	config  cacheStatsConfigKey
	routing RoutingConfig
	routes  []storage.GatewayRequestGroupCacheRoutingRoute
}

type cacheStatsLoader func(context.Context, time.Time, time.Duration, int64, []storage.GatewayRequestGroupCacheRoutingRoute) ([]storage.GatewayRequestGroupCacheRoutingStat, error)

type cacheRatioStat struct {
	Ratio        float64
	PromptTokens int64
	CacheTokens  int64
	LastInputAt  time.Time
	Expired      bool
}

type cacheRatioSnapshot struct {
	Ratios     map[candidateLatencyKey]float64
	Average    float64
	HasAverage bool
	Expired    map[candidateLatencyKey]struct{}
}

func cacheRatioForCandidate(snapshot cacheRatioSnapshot, key candidateLatencyKey) (float64, bool) {
	if _, expired := snapshot.Expired[key]; expired {
		return 0, true
	}
	if ratio, ok := snapshot.Ratios[key]; ok {
		return clamp01(ratio), true
	}
	if snapshot.HasAverage {
		return clamp01(snapshot.Average), true
	}
	return 0, false
}

func cacheRatioSnapshotFromStats(values map[candidateLatencyKey]cacheRatioStat, configKey cacheStatsConfigKey, now time.Time) cacheRatioSnapshot {
	ratios := make(map[candidateLatencyKey]float64, len(values))
	expired := make(map[candidateLatencyKey]struct{})
	cutoff := now.Add(-configKey.MaxAge)
	var validPrompt, validCache int64
	for _, value := range values {
		if value.Expired || value.LastInputAt.IsZero() || value.PromptTokens < configKey.MinTokens || value.PromptTokens <= 0 {
			continue
		}
		if value.LastInputAt.Before(cutoff) {
			continue
		}
		validPrompt += value.PromptTokens
		validCache += max(value.CacheTokens, 0)
	}
	average := 0.0
	if validPrompt > 0 {
		average = clamp01(float64(validCache) / float64(validPrompt))
	}
	for key, value := range values {
		isExpired := value.Expired || value.LastInputAt.IsZero() || value.LastInputAt.Before(cutoff)
		if isExpired {
			expired[key] = struct{}{}
			continue
		}
		ratio := value.Ratio
		if value.PromptTokens >= configKey.MinTokens && value.PromptTokens > 0 {
			ratio = clamp01(float64(max(value.CacheTokens, 0)) / float64(value.PromptTokens))
		} else if validPrompt > 0 {
			ratio = average
		}
		ratios[key] = ratio
	}
	return cacheRatioSnapshot{Ratios: ratios, Average: average, HasAverage: validPrompt > 0, Expired: expired}
}

func (s *Service) cachedRoutingCacheSnapshotLocked(configKey cacheStatsConfigKey, now time.Time) cacheRatioSnapshot {
	if s == nil || s.cacheStatsConfig != configKey {
		return cacheRatioSnapshot{}
	}
	return cacheRatioSnapshotFromStats(s.cacheStats, configKey, now)
}

type DownstreamKeyResult struct {
	Key    storage.GatewayDownstreamKey `json:"key"`
	Plain  string                       `json:"plain"`
	Notice string                       `json:"notice"`
}

type Candidate struct {
	Channel            storage.Channel
	Rate               storage.RateSnapshot
	UpstreamGeneration storage.UpstreamGroupGeneration
	// ModelFactGeneration fences observations about model families,
	// capabilities and context limits. Fixed no-rate routes retain this
	// generation even though they intentionally omit UpstreamGeneration so
	// they cannot mutate shared routing health or pricing state.
	ModelFactGeneration storage.UpstreamGroupGeneration `json:"-"`
	// KeyStateGeneration fences a direct 401/403 key invalidation. It is
	// intentionally separate from routing health and model-fact evidence: a
	// no-rate fixed route may invalidate the exact key it used, but must never
	// disable a replacement group that appeared while its request was in flight.
	KeyStateGeneration         storage.UpstreamGroupGeneration             `json:"-"`
	UpstreamAccountConcurrency *storage.UpstreamAccountConcurrencySnapshot `json:"-"`
	App                        storage.GatewayApp
	ModelFamilies              []string
	ModelIDs                   []string
	DirectBaseURL              string
	LatencyProbeModel          string
	ModelsSyncedAt             *time.Time
	ModelsError                string
	EffectivePrice             float64
	EffectivePriceBasis        string
	PriceInputShare            float64
	CacheRatio                 float64
	CacheRatioKnown            bool
	AvgLatencyMs               float64
	LatencySamples             int64
	Score                      float64
	PricePriorityScore         float64
	LatencyPriorityScore       float64
	ProbeEstimatedCostUSD      float64
	RawRatio                   float64
	RawCompletionRatio         float64
	FundingFactor              float64
	CostBasisVersion           uint64
	CostBasisSource            string
	DisabledForKey             bool
	ProbeDisabled              bool
	CoolingDown                bool
	FailureCount               int
	LastRoutingFailureAt       *time.Time
	CooldownUntil              *time.Time
	CooldownRemaining          time.Duration
}

func candidateUpstreamGenerationArgs(candidate Candidate) []storage.UpstreamGroupGeneration {
	if !candidate.UpstreamGeneration.Valid() {
		return nil
	}
	return []storage.UpstreamGroupGeneration{candidate.UpstreamGeneration}
}

// RoutingHealthFenceForCandidate returns the immutable route identity captured
// before dispatch. Data-plane health writers must use this full fence instead
// of reconstructing the current channel identity after an observation returns.
func RoutingHealthFenceForCandidate(candidate Candidate) (storage.GatewayRoutingHealthFence, bool) {
	generation := candidate.UpstreamGeneration
	canonicalGroup := canonicalGroupName(candidate.Rate.ModelName)
	generationCanonicalGroup := strings.TrimSpace(generation.CanonicalGroupName)
	if !generation.Valid() || candidate.Channel.ID == 0 || candidate.Channel.UpstreamAccountIdentityRevision == 0 ||
		generation.ChannelID != candidate.Channel.ID || canonicalGroup == "" ||
		generationCanonicalGroup != canonicalGroup {
		return storage.GatewayRoutingHealthFence{}, false
	}
	fence := storage.NewGatewayRoutingHealthFence(generation, candidate.Channel.UpstreamAccountIdentityRevision)
	return fence, fence.Valid()
}

// ModelFactFenceForCandidate captures the account revision independently from
// routing-health eligibility. Fixed no-rate routes still learn model facts, but
// only for the exact account/group identity that issued the observation.
func ModelFactFenceForCandidate(candidate Candidate) (storage.GatewayModelFactFence, bool) {
	generation := candidate.ModelFactGeneration
	if !generation.Valid() {
		generation = candidate.UpstreamGeneration
	}
	canonicalGroup := canonicalGroupName(candidate.Rate.ModelName)
	if !generation.Valid() || candidate.Channel.ID == 0 || candidate.Channel.UpstreamAccountIdentityRevision == 0 ||
		generation.ChannelID != candidate.Channel.ID || canonicalGroup == "" ||
		strings.TrimSpace(generation.CanonicalGroupName) != canonicalGroup {
		return storage.GatewayModelFactFence{}, false
	}
	fence := storage.NewGatewayModelFactFence(generation, candidate.Channel.UpstreamAccountIdentityRevision)
	return fence, fence.Valid()
}

// UpstreamKeyInvalidationFenceForCandidate binds delayed authentication
// evidence to the exact local secret that was put on the wire. The group
// generation alone does not change on an account switch or key rotation.
func UpstreamKeyInvalidationFenceForCandidate(candidate Candidate, lease UpstreamKeyLease) (storage.GatewayUpstreamKeyInvalidationFence, bool) {
	generation := candidate.KeyStateGeneration
	if !generation.Valid() {
		generation = candidate.UpstreamGeneration
	}
	canonicalGroup := canonicalGroupName(candidate.Rate.ModelName)
	if !generation.Valid() || candidate.Channel.ID == 0 || candidate.Channel.UpstreamAccountIdentityRevision == 0 ||
		generation.ChannelID != candidate.Channel.ID || canonicalGroup == "" ||
		strings.TrimSpace(generation.CanonicalGroupName) != canonicalGroup ||
		lease.LocalKeyID == 0 || lease.KeyGeneration == 0 {
		return storage.GatewayUpstreamKeyInvalidationFence{}, false
	}
	fence := storage.NewGatewayUpstreamKeyInvalidationFence(
		generation,
		candidate.Channel.UpstreamAccountIdentityRevision,
		lease.LocalKeyID,
		lease.KeyGeneration,
	)
	return fence, fence.Valid()
}

func accountConcurrencyRouteGenerationForCandidate(candidate Candidate) AccountConcurrencyRouteGeneration {
	generation := candidate.UpstreamGeneration
	if !generation.Valid() {
		generation = candidate.ModelFactGeneration
	}
	if !generation.Valid() {
		generation = candidate.KeyStateGeneration
	}
	if !generation.Valid() || generation.ChannelID != candidate.Channel.ID ||
		canonicalGroupName(generation.CanonicalGroupName) != canonicalGroupName(candidate.Rate.ModelName) {
		return AccountConcurrencyRouteGeneration{}
	}
	var upstreamAccountRecordID uint64
	if snapshot := validatedUpstreamAccountConcurrencySnapshot(candidate.Channel, candidate.UpstreamAccountConcurrency); snapshot != nil {
		upstreamAccountRecordID = snapshot.AccountRecordID
	}
	return normalizeAccountConcurrencyRouteGeneration(AccountConcurrencyRouteGeneration{
		ChannelID:               candidate.Channel.ID,
		ChannelIdentityRevision: candidate.Channel.UpstreamAccountIdentityRevision,
		UpstreamAccountRecordID: upstreamAccountRecordID,
		CanonicalGroupName:      generation.CanonicalGroupName,
		UpstreamGroupID:         generation.ID,
		PresenceGeneration:      generation.PresenceGeneration,
	})
}

func validatedUpstreamAccountConcurrencySnapshot(channel storage.Channel, snapshot *storage.UpstreamAccountConcurrencySnapshot) *storage.UpstreamAccountConcurrencySnapshot {
	if snapshot == nil || channel.UpstreamAccountIdentityRevision == 0 ||
		snapshot.AccountRecordID == 0 ||
		snapshot.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		snapshot.Scope != storage.UpstreamConcurrencyScopeAccount {
		return nil
	}
	if snapshot.Limit != nil && (*snapshot.Limit < 0 || strings.TrimSpace(snapshot.Source) == "" || snapshot.ObservedAt == nil || snapshot.ObservedAt.IsZero()) {
		return nil
	}
	clone := *snapshot
	if snapshot.Limit != nil {
		limit := *snapshot.Limit
		clone.Limit = &limit
	}
	if snapshot.ObservedAt != nil {
		observedAt := *snapshot.ObservedAt
		clone.ObservedAt = &observedAt
	}
	return &clone
}

// CandidateGroupMemberPreview keeps candidate-group management separate from
// request routing. Routing only sees eligible candidates; this projection
// intentionally retains every persisted group member so operators can account
// for disabled, cooling, and otherwise unavailable entries.
type CandidateGroupMemberPreview struct {
	Candidate
	CandidateMemberID       uint
	RoutingAvailable        bool
	AvailabilityReasons     []string
	ProbeEligible           bool
	ProbeBlockers           []string
	LatencyQualified        bool
	LatencySource           string
	UpstreamKeyConfigured   bool
	UpstreamKeyEnabled      bool
	UpstreamKeyProvisioning bool
	UpstreamKeyAvailable    bool
}

type CandidateExclusion struct {
	ChannelID          uint
	ChannelName        string
	GroupName          string
	Reason             string
	UpstreamGroupID    uint
	PresenceGeneration uint64
	FailureCount       int
	CooldownUntil      *time.Time
	// Detail remains internal diagnostic prose. Structured request telemetry must
	// use only the fields above and must never serialize this value.
	Detail    string
	candidate Candidate
}

// ManualRouteGateReason is a stable, non-sensitive reason code for a fixed
// route that cannot be sent to its one configured target.  It is deliberately
// separate from adaptive-routing exclusions: callers may expose the code to
// an administrator, but a fixed request must never turn it into a fallback or
// a cooldown decision.
type ManualRouteGateReason string

const (
	ManualRouteGateReasonTargetRequired         ManualRouteGateReason = "manual_target_required"
	ManualRouteGateReasonCandidateGroupMissing  ManualRouteGateReason = "candidate_group_missing"
	ManualRouteGateReasonCandidateGroupMismatch ManualRouteGateReason = "manual_target_not_in_candidate_group"
	ManualRouteGateReasonChannelMissing         ManualRouteGateReason = "channel_missing"
	ManualRouteGateReasonUpstreamKeyMissing     ManualRouteGateReason = "upstream_key_missing"
	ManualRouteGateReasonUpstreamKeyDisabled    ManualRouteGateReason = "upstream_key_disabled"
	ManualRouteGateReasonUpstreamKeyUnavailable ManualRouteGateReason = "upstream_key_unavailable"
	ManualRouteGateReasonProtocolMismatch       ManualRouteGateReason = "gateway_app_mismatch"
	ManualRouteGateReasonModelNotListed         ManualRouteGateReason = "model_not_listed"
	ManualRouteGateReasonModelNotSupported      ManualRouteGateReason = "model_not_supported"
	ManualRouteGateReasonGenerationUnavailable  ManualRouteGateReason = "upstream_generation_unavailable"
)

// ManualRouteGateError gives the API and the management UI a machine-readable
// reason without leaking endpoint, credential, or provider response details.
// The public data-plane response remains deliberately generic.
type ManualRouteGateError struct {
	Reason  ManualRouteGateReason `json:"reason"`
	Message string                `json:"message"`
}

// ManualMappedTargetGateProjection is the non-sensitive, server-owned view of
// a configured fixed target. It deliberately does not include the persisted
// mapped model, any upstream endpoint, identity scope, or upstream error
// detail. Callers must resolve it from the downstream-key ID; they must not
// supply a model or target from the browser.
//
// ManualMappedModelGate means the service evaluated this target using a
// server-owned exact mapped model. For persisted manual mappings, the model is
// read from the downstream key. Automatic strategies can set it only through
// the trusted server-resolver entry point; HTTP must never pass a model here.
// It remains true when that exact model is currently allowed, so callers can
// distinguish this server-side provenance from a client-side requested/recent
// model guess. Disabled or unresolved mappings leave it false.
//
// MappingUnresolved is reserved for legacy or invalid persisted manual mapping
// state. In that case the member still receives a baseline target gate, but no
// client may invent an exact model. The API layer may report the same state
// for an unresolved automatic resolver without accepting a browser model.
type ManualMappedTargetGateProjection struct {
	CandidateMemberID     uint                  `json:"candidate_member_id,omitempty"`
	ChannelID             uint                  `json:"channel_id"`
	CanonicalGroupName    string                `json:"canonical_group_name"`
	Reason                ManualRouteGateReason `json:"reason,omitempty"`
	StructuralBlocked     bool                  `json:"structural_blocked"`
	ManualMappedModelGate bool                  `json:"manual_mapped_model_gate"`
	MappingUnresolved     bool                  `json:"mapping_unresolved"`
}

// AutomaticMappedModelResolver is a trusted management-plane callback used
// only while validating a prospective downstream-key PATCH. The API layer
// resolves its own cached Radar facts; HTTP input must never provide model.
// resolved=false means the automatic mapping is currently opaque/unavailable,
// so the service keeps the baseline target gate and the data plane remains the
// final fail-closed authority.
type AutomaticMappedModelResolver func(key *storage.GatewayDownstreamKey, group *storage.GatewayCandidateGroup, strategy ModelMappingStrategy) (model string, resolved bool)

func (e *ManualRouteGateError) Error() string {
	if e == nil {
		return ""
	}
	if message := strings.TrimSpace(e.Message); message != "" {
		return message
	}
	return string(e.Reason)
}

// ManualRouteGateReasonOf extracts the stable gate reason from a wrapped
// fixed-route validation failure.
func ManualRouteGateReasonOf(err error) (ManualRouteGateReason, bool) {
	var gate *ManualRouteGateError
	if !errors.As(err, &gate) || gate == nil || strings.TrimSpace(string(gate.Reason)) == "" {
		return "", false
	}
	return gate.Reason, true
}

func manualRouteGateError(reason ManualRouteGateReason, message string) error {
	return &ManualRouteGateError{Reason: reason, Message: message}
}

const (
	candidateExclusionReasonUnknown                        = "unknown"
	candidateExclusionReasonUpstreamGenerationUnavailable  = "upstream_generation_unavailable"
	candidateExclusionReasonUpstreamTemporarilyUnavailable = "upstream_temporarily_unavailable"
	candidateExclusionReasonModelNotListed                 = "model_not_listed"
	candidateExclusionReasonModelNotSupported              = "model_not_supported"
	candidateExclusionReasonModelCapabilityUnknown         = "model_capability_unknown"
)

func newCandidateExclusion(candidate Candidate, reason, detail string) CandidateExclusion {
	exclusion := CandidateExclusion{
		ChannelID:   candidate.Channel.ID,
		ChannelName: strings.TrimSpace(candidate.Channel.Name),
		GroupName:   strings.TrimSpace(candidate.Rate.ModelName),
		Reason:      normalizeCandidateExclusionReason(reason),
		Detail:      detail,
		candidate:   candidate,
	}
	generation := candidate.UpstreamGeneration
	if generation.Valid() && generation.ChannelID == candidate.Channel.ID &&
		canonicalGroupName(generation.CanonicalGroupName) == canonicalGroupName(candidate.Rate.ModelName) {
		exclusion.UpstreamGroupID = generation.ID
		exclusion.PresenceGeneration = generation.PresenceGeneration
	}
	if candidate.FailureCount > 0 {
		exclusion.FailureCount = candidate.FailureCount
	}
	if candidate.CooldownUntil != nil {
		cooldownUntil := candidate.CooldownUntil.UTC()
		exclusion.CooldownUntil = &cooldownUntil
	}
	return exclusion
}

func normalizeCandidateExclusionReason(reason string) string {
	switch strings.TrimSpace(reason) {
	case candidateExclusionReasonUpstreamGenerationUnavailable,
		candidateExclusionReasonUpstreamTemporarilyUnavailable,
		candidateExclusionReasonModelNotListed,
		candidateExclusionReasonModelNotSupported,
		candidateExclusionReasonModelCapabilityUnknown:
		return strings.TrimSpace(reason)
	default:
		return candidateExclusionReasonUnknown
	}
}

type ModelFamilySyncItem struct {
	ChannelID   uint     `json:"channel_id"`
	ChannelName string   `json:"channel_name"`
	GroupName   string   `json:"group_name"`
	Families    []string `json:"families,omitempty"`
	ModelIDs    []string `json:"model_ids,omitempty"`
	Skipped     bool     `json:"skipped,omitempty"`
	Error       string   `json:"error,omitempty"`
}

type managedUpstreamKeyReconcileFailure struct {
	GroupName string
	Err       error
}

// sanitizedGatewayFailure keeps a safe error message for persistence and UI
// while retaining the original cause for local retry classification.
type sanitizedGatewayFailure struct {
	message string
	cause   error
}

func (e *sanitizedGatewayFailure) Error() string { return e.message }

func (e *sanitizedGatewayFailure) Unwrap() error { return e.cause }

// ModelFamilySyncPartialError reports group-scoped synchronization failures.
// Each failure is already persisted on its upstream-key row, so callers must
// not promote this error to a channel login or rate-collection failure.
type ModelFamilySyncPartialError struct {
	err error
}

func (e *ModelFamilySyncPartialError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *ModelFamilySyncPartialError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (*ModelFamilySyncPartialError) ModelFamilySyncPartial() bool { return true }

type ManagedUpstreamKeyRebuildResult struct {
	Matched   int                   `json:"matched"`
	Deleted   int                   `json:"deleted"`
	Recreated int                   `json:"recreated"`
	Items     []ModelFamilySyncItem `json:"items"`
}

type LatencyProbeOptions struct {
	MaxCandidates int
	Timeout       time.Duration
	// Interval is the scheduler's base cooldown-recovery cadence.
	Interval time.Duration
	// MaxConcurrent bounds probe rounds in one scheduler pass. Zero keeps the
	// conservative single-worker behavior used by direct callers.
	MaxConcurrent int
	// MaxEstimatedCostUSD bounds the estimated cost of all three-attempt probe
	// confirmations in this pass. Zero disables the optional monetary cap while
	// MaxCandidates still bounds total upstream requests.
	MaxEstimatedCostUSD float64
	// Now is injectable for scheduler/storage tests. A zero value uses the
	// current UTC time.
	Now time.Time
}

type LatencyProbeResult struct {
	App         storage.GatewayApp
	ChannelID   uint
	ChannelName string
	GroupName   string
	ModelName   string
	LatencyMs   int64
	StatusCode  int
	Error       string

	// Kept private so retries can defer adaptive-routing state changes without
	// exposing confirmation internals through the API response.
	failureKind         string
	failureStatus       int
	failureMessage      string
	retryable           bool
	confirmation        string
	confirmationMixed   bool
	confirmationNeutral bool
	deferred            bool
	// completedAt is the wall-clock time at which the confirmation round
	// finished. It is intentionally private: the scheduler uses it to anchor
	// short retry schedules without exposing confirmation internals.
	completedAt time.Time
}

type gatewayProbeTarget struct {
	SiteURL       string
	ProxyURL      string
	ProxyCacheKey string
}

type cachedGatewayAppInfo struct {
	App                      storage.GatewayApp
	ModelFamilies            []string
	ModelIDs                 []string
	DirectBaseURL            string
	LatencyProbeModel        string
	ProbeEnabled             bool
	ModelsSyncedAt           *time.Time
	ModelsError              string
	LastRoutingFailureKind   string
	LastRoutingFailureStatus int
	LastRoutingFailureAt     *time.Time
	RoutingFailureCount      int
	RoutingCooldownUntil     *time.Time
	RoutingIsolatedAt        *time.Time
}

const (
	RoutingFailurePreparation     = "preparation"
	RoutingFailureTransport       = "transport"
	RoutingFailureHTTPStatus      = "http_status"
	RoutingFailureResponseTimeout = "response_timeout"
	RoutingFailureManualCooldown  = storage.GatewayRoutingFailureManualCooldown
)

func NewService(channels *storage.Channels, rates *storage.Rates, store *storage.Gateway, cipher *appcrypto.Cipher, keys KeyManager, log *slog.Logger) *Service {
	return &Service{
		Channels:                         channels,
		Rates:                            rates,
		Store:                            store,
		Cipher:                           cipher,
		Keys:                             keys,
		Log:                              log,
		costLookupSlots:                  make(chan struct{}, 4),
		costWorkerWake:                   make(chan struct{}, 1),
		costIdentityLimiter:              newCostReconcileIdentityLimiter(),
		requestDispatchRecoveryQueue:     make(map[uint64]*requestDispatchRecoveryHandoff),
		requestDispatchRecoveryWake:      make(chan struct{}, 1),
		requestDispatchHandoffRetryQueue: make(map[uint64]*requestDispatchTerminalHandoffRetry),
		modelSyncLocks:                   make(map[uint]chan struct{}),
		testTokens:                       make(map[string]downstreamKeyTestToken),
		upstreamTouchAt:                  make(map[upstreamTouchKey]time.Time),
		upstreamIdentityRepairAt:         make(map[upstreamTouchKey]time.Time),
		cacheStats:                       make(map[candidateLatencyKey]cacheRatioStat),
		routing:                          DefaultRoutingConfig(),
	}
}

func (s *Service) accountConcurrencyControl() (*AccountConcurrencyGovernor, AccountConcurrencyPolicy) {
	if s == nil {
		return nil, DefaultAccountConcurrencyPolicy()
	}
	s.accountConcurrencyMu.Lock()
	if s.accountConcurrencyGovernor == nil {
		s.accountConcurrencyGovernor = NewAccountConcurrencyGovernor()
	}
	governor := s.accountConcurrencyGovernor
	policy := normalizeAccountConcurrencyPolicy(s.accountConcurrencyPolicy)
	s.accountConcurrencyMu.Unlock()
	return governor, policy
}

// TryAcquireCandidateAccountConcurrency is the single admission entry point
// for data-plane dispatch, API tests, model checks, and latency probes. The
// returned decision is retained even on rejection so callers can expose a
// precise route-attempt diagnostic without another storage query.
func (s *Service) TryAcquireCandidateAccountConcurrency(candidate Candidate, class AccountConcurrencyRequestClass) (*AccountConcurrencyLease, AccountConcurrencyDecision) {
	governor, policy := s.accountConcurrencyControl()
	if governor == nil {
		return nil, AccountConcurrencyDecision{
			Reason:     "account_concurrency_governor_unavailable",
			Scope:      AccountConcurrencyTrackerScopeInstance,
			LimitState: "unknown",
		}
	}
	var accountID uint64
	var limit *int
	candidate.UpstreamAccountConcurrency = validatedUpstreamAccountConcurrencySnapshot(candidate.Channel, candidate.UpstreamAccountConcurrency)
	if snapshot := candidate.UpstreamAccountConcurrency; snapshot != nil {
		accountID = snapshot.AccountRecordID
		if snapshot.Limit != nil {
			value := *snapshot.Limit
			limit = &value
		}
	}
	return governor.TryAcquire(accountConcurrencyRouteGenerationForCandidate(candidate), accountID, limit, class, policy)
}

// SetCosting injects the canonical channel cost conversion service without
// changing the gateway constructor contract. A nil service preserves the
// legacy 1:1 cost basis.
func (s *Service) SetCosting(service *costing.Service) {
	if s == nil {
		return
	}
	s.Costing = service
}

func DefaultRoutingConfig() RoutingConfig {
	return RoutingConfig{
		WaitingProbeInterval:         30 * time.Second,
		WaitingProbeTimeout:          2 * time.Minute,
		PriceScoreMax:                defaultRoutingPriceScoreMax,
		LatencyScoreMax:              defaultRoutingLatencyScoreMax,
		LatencySamples:               defaultRoutingLatencySampleSize,
		LatencyWindow:                defaultRoutingLatencyWindow,
		LatencyFullScore:             time.Duration(routingLatencyFullScoreMs) * time.Millisecond,
		LatencyZeroScore:             time.Duration(routingLatencyZeroScoreMs) * time.Millisecond,
		UnknownLatencyScoreFactor:    defaultRoutingUnknownLatencyScoreFactor,
		FailurePolicy:                RoutingFailurePolicy{ResponseTimeout: defaultRoutingResponseTimeout, ManualCooldown: defaultManualRoutingCooldown},
		SessionAffinityMaxAge:        10 * time.Minute,
		SessionAffinityStableSamples: defaultRoutingLatencySampleSize,
		CacheInputPriceMultiplier:    defaultRoutingCacheInputPriceMultiplier,
		CachePriceMinInputTokens:     defaultRoutingCachePriceMinInputTokens,
		CachePriceWindowInputTokens:  defaultRoutingCachePriceWindowInputTokens,
		CacheStatsMaxAge:             defaultRoutingCacheStatsMaxAge,
	}
}

func NormalizeRoutingConfig(cfg RoutingConfig) RoutingConfig {
	out := DefaultRoutingConfig()
	if cfg.WaitingProbeInterval > 0 {
		out.WaitingProbeInterval = cfg.WaitingProbeInterval
	}
	if cfg.WaitingProbeTimeout > 0 {
		out.WaitingProbeTimeout = cfg.WaitingProbeTimeout
	}
	if cfg.PriceScoreMax > 0 {
		out.PriceScoreMax = cfg.PriceScoreMax
	}
	if cfg.LatencyScoreMax > 0 {
		out.LatencyScoreMax = cfg.LatencyScoreMax
	}
	if cfg.LatencySamples > 0 {
		out.LatencySamples = cfg.LatencySamples
	}
	if cfg.LatencyWindow > 0 {
		out.LatencyWindow = cfg.LatencyWindow
	}
	if cfg.LatencyFullScore > 0 {
		out.LatencyFullScore = cfg.LatencyFullScore
	}
	if cfg.LatencyZeroScore > 0 {
		out.LatencyZeroScore = cfg.LatencyZeroScore
	}
	if cfg.UnknownLatencyScoreFactor > 0 {
		out.UnknownLatencyScoreFactor = clamp01(cfg.UnknownLatencyScoreFactor)
	}
	if cfg.FailurePolicy.ResponseTimeout > 0 {
		out.FailurePolicy.ResponseTimeout = cfg.FailurePolicy.ResponseTimeout
	}
	if cfg.FailurePolicy.ManualCooldown > 0 {
		out.FailurePolicy.ManualCooldown = cfg.FailurePolicy.ManualCooldown
	}
	if out.LatencyZeroScore <= out.LatencyFullScore {
		out.LatencyFullScore = time.Duration(routingLatencyFullScoreMs) * time.Millisecond
		out.LatencyZeroScore = time.Duration(routingLatencyZeroScoreMs) * time.Millisecond
	}
	if cfg.SessionAffinityMaxAge > 0 {
		out.SessionAffinityMaxAge = cfg.SessionAffinityMaxAge
	}
	if cfg.SessionAffinityStableSamples > 0 {
		out.SessionAffinityStableSamples = cfg.SessionAffinityStableSamples
	}
	if !math.IsNaN(cfg.CacheInputPriceMultiplier) && !math.IsInf(cfg.CacheInputPriceMultiplier, 0) && cfg.CacheInputPriceMultiplier >= 0 && cfg.CacheInputPriceMultiplier <= 1 {
		out.CacheInputPriceMultiplier = cfg.CacheInputPriceMultiplier
	}
	if cfg.CachePriceMinInputTokens > 0 && cfg.CachePriceMinInputTokens <= maximumRoutingCachePriceInputTokens {
		out.CachePriceMinInputTokens = cfg.CachePriceMinInputTokens
	}
	if cfg.CachePriceWindowInputTokens > 0 && cfg.CachePriceWindowInputTokens <= maximumRoutingCachePriceInputTokens {
		out.CachePriceWindowInputTokens = cfg.CachePriceWindowInputTokens
	}
	if cfg.CacheStatsMaxAge > 0 && cfg.CacheStatsMaxAge <= maximumRoutingCacheStatsMaxAge {
		out.CacheStatsMaxAge = cfg.CacheStatsMaxAge
	}
	total := out.PriceScoreMax + out.LatencyScoreMax
	if total <= 0 {
		out.PriceScoreMax = defaultRoutingPriceScoreMax
		out.LatencyScoreMax = defaultRoutingLatencyScoreMax
	}
	return out
}

func (s *Service) UpdateRoutingConfig(cfg RoutingConfig) {
	if s == nil {
		return
	}
	normalized := NormalizeRoutingConfig(cfg)
	s.routingMu.Lock()
	changed := s.routing.LatencySamples != normalized.LatencySamples || s.routing.LatencyWindow != normalized.LatencyWindow
	s.routing = normalized
	s.routingMu.Unlock()
	s.cacheStatsMu.Lock()
	s.cacheStatsAt = time.Time{}
	s.cacheStatsConfig = cacheStatsConfigKey{}
	s.cacheStatsFailureAt = time.Time{}
	s.cacheStatsFailureConfig = cacheStatsConfigKey{}
	s.cacheStatsQueued = nil
	s.cacheStatsMu.Unlock()
	if changed {
		s.resetRoutingLatencyCache()
	}
}

func (s *Service) routingConfig() RoutingConfig {
	if s == nil {
		return DefaultRoutingConfig()
	}
	s.routingMu.RLock()
	cfg := s.routing
	s.routingMu.RUnlock()
	return NormalizeRoutingConfig(cfg)
}

func (s *Service) CurrentRoutingConfig() RoutingConfig {
	return s.routingConfig()
}

func (s *Service) SyncChannelModelFamilies(ctx context.Context, channelID uint, force bool) error {
	items, err := s.syncModelFamilies(ctx, channelID, force)
	if err != nil {
		return err
	}
	var errs []error
	for _, item := range items {
		if item.Error != "" && !item.Skipped {
			errs = append(errs, fmt.Errorf("%s/%s: %s", item.ChannelName, item.GroupName, item.Error))
		}
	}
	if joined := errors.Join(errs...); joined != nil {
		return &ModelFamilySyncPartialError{err: joined}
	}
	return nil
}

// RefreshUpstreamGroupFacts publishes the current rate snapshot into the
// stable group-generation projection without touching remote keys or model
// probes. It is the lightweight write-preflight bridge used after the monitor
// fact collector commits a fresh upstream snapshot.
func (s *Service) RefreshUpstreamGroupFacts(ctx context.Context, channelID uint) error {
	if s == nil || s.Channels == nil || s.Rates == nil || s.Store == nil || channelID == 0 {
		return errors.New("upstream group fact projection is unavailable")
	}
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return err
	}
	defer unlock()
	channels := s.Channels.WithContext(ctx)
	rateRepo := s.Rates.WithContext(ctx)
	store := s.Store.WithContext(ctx)
	channel, err := channels.FindByID(channelID)
	if err != nil {
		return err
	}
	if channel.CredentialMode == storage.CredentialModeDirect {
		_, err := s.syncDirectChannelModelFamilies(ctx, channel)
		return err
	}
	rates, err := rateRepo.ListByChannel(channelID)
	if err != nil {
		return err
	}
	for _, rate := range canonicalRateSnapshots(rates) {
		groupName := strings.TrimSpace(rate.ModelName)
		if groupName == "" {
			continue
		}
		if _, err := store.CaptureUpstreamGroupGeneration(channelID, groupName); errors.Is(err, gorm.ErrRecordNotFound) {
			if err := store.RecordUpstreamGroupChange(channelID, groupName, "added", 0, upstreamKeyNameForGroup(groupName), time.Now()); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return store.RequireUpstreamAccountIdentityRevision(channel.ID, channel.UpstreamAccountIdentityRevision)
}

// ApplyUpstreamGroupFacts is the control-plane consumer for a committed
// upstream group snapshot. It reconciles only managed Keys and removed routing
// groups; model probing remains owned by SyncChannelModelFamilies.
func (s *Service) ApplyUpstreamGroupFacts(ctx context.Context, channelID uint) error {
	if s == nil || s.Channels == nil || s.Rates == nil || s.Store == nil || channelID == 0 {
		return errors.New("upstream group fact consumer is unavailable")
	}
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return err
	}
	defer unlock()
	channels := s.Channels.WithContext(ctx)
	rateRepo := s.Rates.WithContext(ctx)
	store := s.Store.WithContext(ctx)
	channel, err := channels.FindByID(channelID)
	if err != nil {
		return err
	}
	if channel.CredentialMode == storage.CredentialModeDirect {
		_, err := s.syncDirectChannelModelFamilies(ctx, channel)
		return err
	}
	rates, err := rateRepo.ListByChannel(channelID)
	if err != nil {
		return err
	}
	active := make(map[string]string, len(rates))
	activeGroups := make(map[string]struct{}, len(rates))
	activeRatios := make(map[string]float64, len(rates))
	activeNames := make([]string, 0, len(rates))
	for _, rate := range canonicalRateSnapshots(rates) {
		if name := strings.TrimSpace(rate.ModelName); name != "" {
			groupKey := canonicalGroupName(name)
			active[groupKey] = name
			activeGroups[groupKey] = struct{}{}
			activeRatios[groupKey] = rate.Ratio
			activeNames = append(activeNames, name)
			if _, captureErr := store.CaptureUpstreamGroupGeneration(channelID, name); errors.Is(captureErr, gorm.ErrRecordNotFound) {
				if changeErr := store.RecordUpstreamGroupChange(channelID, name, "added", 0, upstreamKeyNameForGroup(name), time.Now().UTC()); changeErr != nil {
					return changeErr
				}
			} else if captureErr != nil {
				return captureErr
			}
		}
	}
	// Reconcile the complete remote inventory after the authoritative snapshot
	// is committed. The managed prefix is the only deletion authority here;
	// manual keys are intentionally ignored. A failed group is isolated below,
	// while an inventory/list failure remains fail-closed for this pass.
	deletedRemoteIDs := make(map[int64]struct{})
	ctx = withUpstreamInventoryPassCache(ctx, channel.ID, channel.UpstreamAccountIdentityRevision, deletedRemoteIDs)
	reconcileFailures, reconcileDeferred, err := s.reconcileManagedUpstreamKeysWithBackoff(
		ctx, channel.ID, channel.UpstreamAccountIdentityRevision, activeGroups, active, deletedRemoteIDs,
	)
	if err != nil {
		return fmt.Errorf("reconcile managed upstream keys: %w", err)
	}
	reconcileFailureByGroup := make(map[string]error, len(reconcileFailures))
	var failures []error
	for _, failure := range reconcileFailures {
		groupKey := canonicalGroupName(failure.GroupName)
		if groupKey != "" {
			reconcileFailureByGroup[groupKey] = failure.Err
		}
		failures = append(failures, fmt.Errorf("maintain managed upstream key %q: %w", failure.GroupName, failure.Err))
	}
	// Once this pass has been admitted, let unrelated groups finish even if one
	// provider mutation records a transient maintenance backoff. The backoff
	// still gates later passes.
	if !reconcileDeferred {
		ctx = withManagedKeyMaintenanceProbe(ctx)
	}
	routingGroups, err := s.Store.ListRoutingGroupNames(channelID)
	if err != nil {
		return err
	}
	for _, groupName := range routingGroups {
		groupName = strings.TrimSpace(groupName)
		groupKey := canonicalGroupName(groupName)
		if groupName == "" || active[groupKey] != "" || reconcileFailureByGroup[groupKey] != nil {
			continue
		}
		groupCtx, cancelGroup := managedKeyRemoteOperationContext(ctx)
		_, _, _, removeErr := s.removeStaleUpstreamGroupWithBackoff(groupCtx, channelID, groupName, channel.UpstreamAccountIdentityRevision, false)
		cancelGroup()
		if removeErr != nil {
			if ctx.Err() != nil || errors.Is(removeErr, storage.ErrStaleChannelAccountIdentityRevision) {
				return errors.Join(append(failures, removeErr)...)
			}
			failures = append(failures, removeErr)
			continue
		}
	}
	for _, groupName := range activeNames {
		groupKey := canonicalGroupName(groupName)
		if reconcileFailureByGroup[groupKey] != nil {
			continue
		}
		// ensureUpstreamAPIKeyForGroup still returns an already usable local
		// secret during backoff, while missing or broken Keys remain deferred.
		groupCtx, cancelGroup := managedKeyRemoteOperationContext(ctx)
		_, ensureErr := s.ensureUpstreamAPIKeyForGroup(groupCtx, channelID, groupName, nil, activeRatios[canonicalGroupName(groupName)], false, false)
		cancelGroup()
		if ensureErr != nil {
			var deferredErr *ManagedKeyMaintenanceDeferredError
			if errors.As(ensureErr, &deferredErr) {
				continue
			}
			if ctx.Err() != nil || errors.Is(ensureErr, storage.ErrStaleChannelAccountIdentityRevision) {
				return errors.Join(append(failures, ensureErr)...)
			}
			failures = append(failures, fmt.Errorf("maintain managed upstream key %q: %w", groupName, ensureErr))
		}
	}
	if err := s.Channels.RequireUpstreamAccountIdentityRevision(channelID, channel.UpstreamAccountIdentityRevision); err != nil {
		return err
	}
	for _, failure := range failures {
		if _, _, transient := managedKeyMaintenanceFailure(ctx, failure); !transient {
			continue
		}
		deferred, stateErr := s.managedKeyMaintenanceDeferred(channelID, channel.UpstreamAccountIdentityRevision, time.Now().UTC())
		if stateErr != nil {
			return errors.Join(errors.Join(failures...), stateErr)
		}
		if deferred == nil {
			if persistErr := s.persistManagedKeyMaintenanceFailure(ctx, channelID, channel.UpstreamAccountIdentityRevision, failure); persistErr != nil {
				return errors.Join(errors.Join(failures...), persistErr)
			}
		}
		break
	}
	return errors.Join(failures...)
}

// MaintainManagedUpstreamKeyForGroup is the control-plane consumer for a
// freshly committed group fact. It provisions or recovers only the requested
// managed key; model probes and financial evidence are intentionally outside
// this stage.
func (s *Service) MaintainManagedUpstreamKeyForGroup(ctx context.Context, channelID uint, groupName string) error {
	groupName = strings.TrimSpace(groupName)
	if s == nil || s.Channels == nil || s.Rates == nil || s.Store == nil || channelID == 0 || groupName == "" {
		return errors.New("managed upstream key maintenance is unavailable")
	}
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return err
	}
	defer unlock()
	channels := s.Channels.WithContext(ctx)
	rateRepo := s.Rates.WithContext(ctx)
	store := s.Store.WithContext(ctx)
	channel, err := channels.FindByID(channelID)
	if err != nil {
		return err
	}
	if channel.CredentialMode == storage.CredentialModeDirect {
		_, err := s.syncDirectChannelModelFamilies(ctx, channel)
		return err
	}
	rates, err := rateRepo.ListByChannel(channelID)
	if err != nil {
		return err
	}
	var current *storage.RateSnapshot
	for _, rate := range canonicalRateSnapshots(rates) {
		if canonicalGroupName(rate.ModelName) == canonicalGroupName(groupName) {
			rate := rate
			current = &rate
			break
		}
	}
	if current == nil {
		return gorm.ErrRecordNotFound
	}
	if _, err := store.CaptureUpstreamGroupGeneration(channelID, current.ModelName); errors.Is(err, gorm.ErrRecordNotFound) {
		if err := store.RecordUpstreamGroupChange(channelID, current.ModelName, "added", 0, upstreamKeyNameForGroup(current.ModelName), time.Now().UTC()); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if _, err := s.maintainManagedUpstreamKeyForGroup(ctx, channelID, current.ModelName, current.Ratio, false); err != nil {
		return err
	}
	latest, err := channels.FindByID(channelID)
	if err != nil {
		return err
	}
	if latest.UpstreamAccountIdentityRevision != channel.UpstreamAccountIdentityRevision {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

func (s *Service) SyncModelFamilies(ctx context.Context, channelID uint) ([]ModelFamilySyncItem, error) {
	return s.syncModelFamilies(ctx, channelID, true)
}

func (s *Service) syncModelFamilies(ctx context.Context, channelID uint, force bool) ([]ModelFamilySyncItem, error) {
	var channels []storage.Channel
	if channelID > 0 {
		channel, err := s.Channels.FindByID(channelID)
		if err != nil {
			return nil, err
		}
		channels = []storage.Channel{*channel}
	} else {
		var err error
		channels, err = s.Channels.List()
		if err != nil {
			return nil, err
		}
	}
	items := make([]ModelFamilySyncItem, 0)
	for _, channel := range channels {
		blockers := managedKeySyncChannelBlockers(channel, force)
		if len(blockers) > 0 {
			if force {
				items = append(items, ModelFamilySyncItem{
					ChannelID:   channel.ID,
					ChannelName: channel.Name,
					Skipped:     true,
					Error:       "model sync skipped: " + strings.Join(blockers, ", "),
				})
			}
			continue
		}
		channelItems, err := s.syncChannelModelFamilies(ctx, &channel, force)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return items, err
			}
			items = append(items, ModelFamilySyncItem{
				ChannelID:   channel.ID,
				ChannelName: channel.Name,
				Error:       sanitizeGatewayServiceError(err.Error()),
			})
			continue
		}
		items = append(items, channelItems...)
	}
	return items, nil
}

// managedKeySyncChannelBlockers contains only conditions that must block the
// whole channel control-plane pass. Balance facts gate paid data-plane work and
// model probes, but they must not strand a present upstream group without its
// managed key.
func managedKeySyncChannelBlockers(channel storage.Channel, force bool) []string {
	if channel.CredentialMode == storage.CredentialModeDirect {
		return nil
	}
	reasons := ChannelAvailabilityReasons(channel)
	blockers := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		switch reason {
		case availabilityReasonLowBalance, availabilityReasonAccountBalance, availabilityReasonMonitorDisabled:
			continue
		case availabilityReasonChannelError:
			if force {
				continue
			}
		}
		blockers = append(blockers, reason)
	}
	return blockers
}

func (s *Service) syncChannelModelFamilies(ctx context.Context, channel *storage.Channel, force bool) ([]ModelFamilySyncItem, error) {
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channel.ID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	current, err := s.Channels.FindByID(channel.ID)
	if err != nil {
		return nil, err
	}
	return s.syncChannelModelFamiliesLocked(ctx, current, force, false)
}

func (s *Service) syncChannelModelFamiliesLocked(ctx context.Context, channel *storage.Channel, force bool, rebuildingManagedKeys bool) ([]ModelFamilySyncItem, error) {
	if channel != nil && channel.CredentialMode == storage.CredentialModeDirect {
		return s.syncDirectChannelModelFamilies(ctx, channel)
	}
	accountConcurrency, err := s.Channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		return nil, err
	}
	rates, err := s.Rates.ListByChannel(channel.ID)
	if err != nil {
		return nil, err
	}
	rates = canonicalRateSnapshots(rates)
	activeGroups := make(map[string]struct{}, len(rates))
	activeGroupNames := make(map[string]string, len(rates))
	for _, rate := range rates {
		if groupName := strings.TrimSpace(rate.ModelName); groupName != "" {
			groupKey := canonicalGroupName(groupName)
			activeGroups[groupKey] = struct{}{}
			activeGroupNames[groupKey] = groupName
		}
	}
	routingGroups, err := s.Store.ListRoutingGroupNames(channel.ID)
	if err != nil {
		return nil, err
	}
	cachedKeys, err := s.Store.ListUpstreamKeys([]uint{channel.ID})
	if err != nil {
		return nil, err
	}
	cachedByGroup := make(map[string]storage.GatewayUpstreamKey, len(cachedKeys))
	for _, cached := range cachedKeys {
		if groupName := strings.TrimSpace(cached.GroupName); groupName != "" {
			cachedByGroup[canonicalGroupName(groupName)] = cached
		}
	}
	knownGroups := make(map[string]struct{}, len(routingGroups))
	deletedRemoteIDs := make(map[int64]struct{})
	staleRemovalFailures := make(map[string]error)
	maintenanceDeferred, err := s.managedKeyMaintenanceDeferred(channel.ID, channel.UpstreamAccountIdentityRevision, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	for _, groupName := range routingGroups {
		groupName = strings.TrimSpace(groupName)
		if _, ok := activeGroups[canonicalGroupName(groupName)]; ok {
			continue
		}
		if maintenanceDeferred != nil {
			continue
		}
		keyID, _, err := s.removeStaleUpstreamGroup(ctx, channel.ID, groupName, channel.UpstreamAccountIdentityRevision, rebuildingManagedKeys)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
				return nil, fmt.Errorf("delete stale routing group %q: %w", groupName, err)
			}
			staleRemovalFailures[canonicalGroupName(groupName)] = fmt.Errorf("delete stale routing group %q: %w", groupName, err)
			if _, _, transient := managedKeyMaintenanceFailure(ctx, err); transient && maintenanceDeferred == nil {
				// Stop additional provider-management requests in this pass even if
				// persisting the cross-process backoff itself fails.
				maintenanceDeferred = &ManagedKeyMaintenanceDeferredError{}
				if persistErr := s.persistManagedKeyMaintenanceFailure(ctx, channel.ID, channel.UpstreamAccountIdentityRevision, err); persistErr != nil {
					if errors.Is(persistErr, storage.ErrStaleChannelAccountIdentityRevision) {
						return nil, persistErr
					}
					staleRemovalFailures[canonicalGroupName(groupName)] = errors.Join(
						staleRemovalFailures[canonicalGroupName(groupName)],
						fmt.Errorf("record managed upstream key maintenance backoff: %w", persistErr),
					)
				}
			}
			continue
		}
		if keyID > 0 {
			deletedRemoteIDs[keyID] = struct{}{}
		}
	}
	reconcileFailures := make([]managedUpstreamKeyReconcileFailure, 0)
	reconcileFailureByGroup := make(map[string]error)
	if force {
		var reconcileDeferred bool
		reconcileFailures, reconcileDeferred, err = s.reconcileManagedUpstreamKeysWithBackoff(ctx, channel.ID, channel.UpstreamAccountIdentityRevision, activeGroups, activeGroupNames, deletedRemoteIDs)
		if err != nil {
			return nil, fmt.Errorf("reconcile managed upstream keys: %w", err)
		}
		if !reconcileDeferred {
			for _, failure := range reconcileFailures {
				groupKey := canonicalGroupName(failure.GroupName)
				if groupKey != "" {
					reconcileFailureByGroup[groupKey] = failure.Err
				}
			}
		}
	}
	for groupKey := range activeGroups {
		cached, ok := cachedByGroup[groupKey]
		if !ok {
			continue
		}
		// A failed first probe creates a routing-cache row. It must not make a
		// subsequently discovered upstream group look like an old one.
		if strings.TrimSpace(cached.ModelFamilies) != "" && strings.TrimSpace(cached.ModelIDs) != "" {
			knownGroups[groupKey] = struct{}{}
		}
	}

	items := make([]ModelFamilySyncItem, 0, len(rates)+len(reconcileFailures)+len(staleRemovalFailures))
	for groupKey, failure := range staleRemovalFailures {
		items = append(items, ModelFamilySyncItem{
			ChannelID: channel.ID, ChannelName: channel.Name,
			GroupName: groupKey, Error: sanitizeGatewayServiceError(failure.Error()),
		})
	}
	for _, failure := range reconcileFailures {
		if _, active := activeGroups[canonicalGroupName(failure.GroupName)]; active {
			continue
		}
		items = append(items, ModelFamilySyncItem{
			ChannelID: channel.ID, ChannelName: channel.Name,
			GroupName: failure.GroupName, Error: sanitizeGatewayServiceError(failure.Err.Error()),
		})
	}
	balanceProbeStopped := false
	for _, rate := range rates {
		groupName := strings.TrimSpace(rate.ModelName)
		if groupName == "" {
			continue
		}
		groupKey := canonicalGroupName(groupName)
		cached, cachedExists := cachedByGroup[groupKey]
		cacheGroupName := groupName
		if cachedExists && strings.TrimSpace(cached.GroupName) != "" {
			cacheGroupName = strings.TrimSpace(cached.GroupName)
		}
		if reconcileErr := reconcileFailureByGroup[groupKey]; reconcileErr != nil {
			items = append(items, ModelFamilySyncItem{
				ChannelID: channel.ID, ChannelName: channel.Name,
				GroupName: groupName, Error: sanitizeGatewayServiceError(reconcileErr.Error()),
			})
			continue
		}
		if !balanceProbeStopped && !force && !channelBalanceBlocksModelErrorBackoff(*channel) && cachedExists && cached.Enabled && !cached.Provisioning && strings.TrimSpace(cached.APIKeyCipher) != "" {
			if strings.TrimSpace(cached.ModelFamilies) != "" &&
				strings.TrimSpace(cached.ModelIDs) != "" &&
				strings.TrimSpace(cached.ModelsError) == "" {
				continue
			}
			if cached.ModelsSyncedAt != nil &&
				strings.TrimSpace(cached.ModelsError) != "" &&
				cached.ModelsSyncedAt.Add(modelSyncErrorRetryInterval).After(time.Now()) {
				items = append(items, ModelFamilySyncItem{
					ChannelID: channel.ID, ChannelName: channel.Name,
					GroupName: groupName, Error: cached.ModelsError,
				})
				continue
			}
		}
		item := ModelFamilySyncItem{ChannelID: channel.ID, ChannelName: channel.Name, GroupName: groupName}
		generation, generationErr := s.Store.CaptureUpstreamGroupGeneration(channel.ID, groupName)
		if errors.Is(generationErr, gorm.ErrRecordNotFound) {
			discoveredAt := time.Now()
			generationErr = s.Store.RecordUpstreamGroupChange(channel.ID, groupName, "added", 0, upstreamKeyNameForGroup(groupName), discoveredAt)
			if generationErr == nil {
				generation, generationErr = s.Store.CaptureUpstreamGroupGeneration(channel.ID, groupName)
			}
		}
		if generationErr != nil {
			item.Error = sanitizeGatewayServiceError(fmt.Errorf("capture upstream group generation: %w", generationErr).Error())
			items = append(items, item)
			continue
		}
		probeCandidate := Candidate{
			Channel:                    *channel,
			Rate:                       rate,
			UpstreamGeneration:         generation,
			ModelFactGeneration:        generation,
			KeyStateGeneration:         generation,
			UpstreamAccountConcurrency: accountConcurrency,
		}
		upstreamKey, err := s.maintainManagedUpstreamKeyForGroup(ctx, channel.ID, cacheGroupName, rate.Ratio, rebuildingManagedKeys)
		if err == nil {
			// Preparing the first Sub2API key may discover and persist a
			// dedicated API origin. That operation advances the account identity
			// revision and invalidates every pre-discovery key snapshot. Restart
			// the locked sync from the freshly persisted channel instead of
			// probing the homepage or continuing with stale cached-key state.
			currentChannel, refreshErr := s.Channels.FindByID(channel.ID)
			if refreshErr != nil {
				err = refreshErr
			} else if currentChannel.UpstreamAccountIdentityRevision != channel.UpstreamAccountIdentityRevision {
				return s.syncChannelModelFamiliesLocked(ctx, currentChannel, force, rebuildingManagedKeys)
			} else {
				channel = currentChannel
				probeCandidate.Channel = *currentChannel
			}
		}
		if err == nil {
			if !channel.MonitorEnabled {
				item.Skipped = true
				item.Error = "model probe skipped: " + availabilityReasonMonitorDisabled
				items = append(items, item)
				continue
			} else if balanceProbeStopped {
				// A confirmed account-balance rejection applies to every key on
				// the account. Maintain later groups' keys, but avoid repeating
				// the same paid model probe throughout this control-plane pass.
				item.Skipped = true
				items = append(items, item)
				continue
			} else {
				probeKeyLease, leaseErr := s.upstreamKeyLeaseForPlain(channel.ID, cacheGroupName, upstreamKey)
				if leaseErr != nil {
					err = leaseErr
				} else {
					var target gatewayProbeTarget
					target, err = s.resolveGatewayProbeTarget(ctx, channel)
					if err == nil {
						item.Families, item.ModelIDs, err = s.probeModelFamilies(ctx, target, upstreamKey, probeCandidate, probeKeyLease)
					}
					if err != nil && isModelProbeAuthError(err) {
						fence, ok := UpstreamKeyInvalidationFenceForCandidate(probeCandidate, probeKeyLease)
						if !ok {
							err = errors.Join(err, errors.New("upstream key invalidation identity is unavailable"))
						} else if invalidateErr := s.InvalidateUpstreamGroupKeyForIdentityContext(ctx, fence); invalidateErr != nil {
							err = errors.Join(err, fmt.Errorf("invalidate upstream key: %w", invalidateErr))
						} else if refreshedKey, refreshErr := s.ensureUpstreamAPIKeyForGroup(ctx, channel.ID, cacheGroupName, nil, rate.Ratio, false, rebuildingManagedKeys); refreshErr == nil {
							if refreshedLease, leaseErr := s.upstreamKeyLeaseForPlain(channel.ID, cacheGroupName, refreshedKey); leaseErr == nil {
								item.Families, item.ModelIDs, err = s.probeModelFamilies(ctx, target, refreshedKey, probeCandidate, refreshedLease)
							} else {
								err = leaseErr
							}
						} else {
							err = refreshErr
						}
					}
				}
			}
		}
		accountBalanceError := connector.IsAccountBalanceHTTPError(err)
		if accountBalanceError {
			balanceProbeStopped = true
			_ = s.MarkUpstreamAccountBalanceInsufficient(probeCandidate, time.Now().UTC())
		}
		var maintenanceDeferred *ManagedKeyMaintenanceDeferredError
		if errors.As(err, &maintenanceDeferred) {
			item.Skipped = true
			item.Error = maintenanceDeferred.Error()
		} else if errors.Is(err, errAccountConcurrencyProbeDeferred) {
			item.Skipped = true
		} else if accountBalanceError {
			// A 402 is a channel-level balance fact, not a group model
			// capability failure. Keep the existing model evidence intact; later
			// groups still maintain their keys but skip redundant probes this pass.
			item.Skipped = true
		} else if err != nil {
			item.Error = sanitizeGatewayServiceError(err.Error())
			modelFence, fenced := ModelFactFenceForCandidate(probeCandidate)
			if !fenced {
				item.Error = sanitizeGatewayServiceError(errors.Join(err, storage.ErrUpstreamGroupGenerationChanged).Error())
			} else if saveErr := s.Store.RecordUpstreamKeyModelsErrorForIdentity(modelFence, time.Now(), item.Error); saveErr != nil {
				item.Error = sanitizeGatewayServiceError(errors.Join(err, fmt.Errorf("save model sync error: %w", saveErr)).Error())
			}
		} else {
			modelFence, fenced := ModelFactFenceForCandidate(probeCandidate)
			if !fenced {
				item.Error = sanitizeGatewayServiceError(storage.ErrUpstreamGroupGenerationChanged.Error())
			} else if saveErr := s.Store.UpdateUpstreamKeyModelFamiliesForIdentity(modelFence, strings.Join(item.Families, ","), strings.Join(item.ModelIDs, ","), time.Now(), ""); saveErr != nil {
				item.Error = sanitizeGatewayServiceError(fmt.Errorf("save model families: %w", saveErr).Error())
			} else if _, known := knownGroups[groupKey]; !known {
				var upstreamKeyID int64
				upstreamKeyName := upstreamKeyNameForGroup(groupName)
				if record, findErr := s.Store.FindUpstreamKeyAny(channel.ID, cacheGroupName); findErr == nil {
					upstreamKeyID = record.UpstreamKeyID
					upstreamKeyName = record.UpstreamKeyName
				}
				if changeErr := s.Store.RecordUpstreamGroupChange(channel.ID, groupName, "added", upstreamKeyID, upstreamKeyName, time.Now()); changeErr != nil {
					return nil, changeErr
				}
			}
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Service) syncDirectChannelModelFamilies(ctx context.Context, channel *storage.Channel) ([]ModelFamilySyncItem, error) {
	if s == nil || channel == nil || s.Store == nil || s.Cipher == nil || s.Channels == nil {
		return nil, errors.New("direct channel gateway sync is unavailable")
	}
	if err := s.Channels.WithContext(ctx).RequireOrdinaryChannelWrite(ctx, channel.ID); err != nil {
		return nil, err
	}
	credentials, err := s.Channels.WithContext(ctx).ListDirectCredentials(channel.ID)
	if err != nil {
		return nil, err
	}
	if len(credentials) == 0 {
		credentials, err = s.Channels.WithContext(ctx).EnsureLegacyDirectCredential(channel)
		if err != nil {
			return nil, err
		}
	}
	activeGroups := make(map[string]struct{}, len(credentials))
	items := make([]ModelFamilySyncItem, 0, len(credentials))
	for _, credential := range credentials {
		groupName := strings.TrimSpace(credential.GroupName)
		if !credential.Enabled || groupName == "" {
			continue
		}
		models, parseErr := storage.ParseDirectModels(credential.Models)
		if parseErr != nil {
			return nil, fmt.Errorf("direct group %q models: %w", groupName, parseErr)
		}
		plain, decryptErr := s.Cipher.Decrypt(credential.APIKeyCipher)
		if decryptErr != nil {
			return nil, fmt.Errorf("decrypt direct group %q API key: %w", groupName, decryptErr)
		}
		plain = strings.TrimSpace(plain)
		if plain == "" {
			return nil, fmt.Errorf("direct group %q API key is empty", groupName)
		}
		if saveErr := s.saveDirectUpstreamKeyContext(ctx, channel.ID, channel.UpstreamAccountIdentityRevision, credential, plain); saveErr != nil {
			return nil, fmt.Errorf("save direct group %q gateway key: %w", groupName, saveErr)
		}
		generation, generationErr := s.Store.WithContext(ctx).CaptureUpstreamGroupGeneration(channel.ID, groupName)
		if errors.Is(generationErr, gorm.ErrRecordNotFound) {
			if generationErr = s.Store.WithContext(ctx).RecordUpstreamGroupChange(channel.ID, groupName, "added", 0, directUpstreamKeyName, time.Now().UTC()); generationErr == nil {
				generation, generationErr = s.Store.WithContext(ctx).CaptureUpstreamGroupGeneration(channel.ID, groupName)
			}
		}
		if generationErr != nil {
			return nil, fmt.Errorf("capture direct group %q generation: %w", groupName, generationErr)
		}
		families := directChannelModelFamilies(channel, models)
		fence := storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision)
		if saveErr := s.Store.WithContext(ctx).UpdateUpstreamKeyModelFamiliesForIdentity(fence, strings.Join(families, ","), strings.Join(models, ","), time.Now().UTC(), ""); saveErr != nil {
			return nil, fmt.Errorf("save direct group %q model families: %w", groupName, saveErr)
		}
		activeGroups[canonicalGroupName(groupName)] = struct{}{}
		items = append(items, ModelFamilySyncItem{ChannelID: channel.ID, ChannelName: channel.Name, GroupName: groupName, Families: families, ModelIDs: models})
	}
	routingGroups, err := s.Store.WithContext(ctx).ListRoutingGroupNames(channel.ID)
	if err != nil {
		return nil, err
	}
	for _, groupName := range routingGroups {
		if _, active := activeGroups[canonicalGroupName(groupName)]; active {
			continue
		}
		cached, findErr := s.Store.WithContext(ctx).FindUpstreamKeyAny(channel.ID, groupName)
		keyID, keyName := int64(0), upstreamKeyNameForGroup(groupName)
		if findErr == nil {
			keyID, keyName = cached.UpstreamKeyID, cached.UpstreamKeyName
		}
		if err := s.Store.WithContext(ctx).RecordUpstreamGroupChange(channel.ID, groupName, "removed", keyID, keyName, time.Now().UTC()); err != nil {
			return nil, fmt.Errorf("record stale direct routing group %q: %w", groupName, err)
		}
		if err := s.Store.WithContext(ctx).DeleteGroupUpstreamState(channel.ID, groupName, true); err != nil {
			return nil, fmt.Errorf("delete stale direct routing group %q: %w", groupName, err)
		}
	}
	return items, nil
}

// directChannelModelFamilies records the selected transport protocol. Model
// IDs are an explicit administrator allowlist and must not be reclassified by
// vendor names (a Codex-compatible endpoint may expose claude-* aliases).
func directChannelModelFamilies(channel *storage.Channel, models []string) []string {
	if channel == nil {
		return nil
	}
	family := string(normalizeGatewayApp(channel.GatewayApp))
	if family == "" {
		return nil
	}
	return []string{family}
}

func channelBalanceBlocksModelErrorBackoff(channel storage.Channel) bool {
	for _, reason := range ChannelAvailabilityReasons(channel) {
		if reason == availabilityReasonLowBalance || reason == availabilityReasonAccountBalance {
			return true
		}
	}
	return false
}

// maintainManagedUpstreamKeyForGroup is the control-plane stage of model
// synchronization. Keep it separate from model probing so routing/balance
// admission can skip probes without preventing key provisioning or recovery.
func (s *Service) maintainManagedUpstreamKeyForGroup(ctx context.Context, channelID uint, groupName string, groupRatio float64, rebuildingManagedKeys bool) (string, error) {
	return s.ensureUpstreamAPIKeyForGroup(ctx, channelID, groupName, nil, groupRatio, false, rebuildingManagedKeys)
}

// reconcileManagedUpstreamKeysForSync is the management-plane repair path for
// remote API keys. A managed name is the ownership proof: local IDs, names and
// secrets are observations that may be stale after an upstream administrator
// edits a key. Manual keys never enter this path.
func (s *Service) reconcileManagedUpstreamKeysForSync(ctx context.Context, channelID uint, expectedIdentityRevision uint64, activeGroups map[string]struct{}, activeGroupNames map[string]string, alreadyDeletedIDs map[int64]struct{}) ([]managedUpstreamKeyReconcileFailure, error) {
	remote, err := s.listAllUpstreamKeysAllowingDuplicateNames(ctx, channelID, expectedIdentityRevision)
	if err != nil {
		return nil, err
	}
	remoteByID := make(map[int64]connector.APIKey, len(remote))
	presentRemote := make(map[int64]string, len(remote))
	for _, item := range remote {
		if item.ID > 0 {
			remoteByID[item.ID] = item
			presentRemote[item.ID] = strings.TrimSpace(item.Name)
		}
	}
	// A complete inventory can finish an inventory deletion whose provider
	// mutation succeeded before the acknowledgement transaction committed. It
	// also repairs exact stale local bindings left by releases that acknowledged
	// the deletion before clearing the local secret.
	if err := s.Store.ReconcileGenerationSafeManagedUpstreamKeyInventoryRemovals(
		channelID, expectedIdentityRevision, presentRemote, time.Now().UTC(),
	); err != nil {
		return nil, fmt.Errorf("reconcile managed upstream key inventory removals: %w", err)
	}
	localKeys, err := s.Store.ListUpstreamKeys([]uint{channelID})
	if err != nil {
		return nil, fmt.Errorf("list local upstream keys: %w", err)
	}
	// Sub2API can retain only a numeric group ID after a group is removed. A
	// unique, trusted local binding supplies the old name only when the
	// connector proves that it successfully read the complete group directory
	// and that ID was absent. Incomplete provider responses fail closed.
	localByRemoteID := uniqueManagedLocalKeysByRemoteID(localKeys)
	groupForRemote := func(item connector.APIKey) string {
		return managedUpstreamKeyGroupName(item, localByRemoteID)
	}
	// Once both inventories are complete, failures are isolated to their
	// canonical group. The failed group is not ensured in this pass, while
	// unrelated active groups may continue through provisioning. Account
	// identity changes remain channel-wide because they invalidate the remote
	// snapshot itself.
	failuresByGroup := make(map[string]managedUpstreamKeyReconcileFailure)
	recordFailure := func(groupName string, failure error) {
		groupName = strings.TrimSpace(groupName)
		groupKey := canonicalGroupName(groupName)
		if failure == nil || groupKey == "" {
			return
		}
		failure = &sanitizedGatewayFailure{
			message: sanitizeGatewayServiceError(failure.Error()),
			cause:   failure,
		}
		if saveErr := s.Store.SetUpstreamKeyError(channelID, groupName, failure.Error()); saveErr != nil {
			failure = errors.Join(failure, fmt.Errorf("record managed key reconcile error: %w", saveErr))
		}
		if previous, exists := failuresByGroup[groupKey]; exists {
			previous.Err = errors.Join(previous.Err, failure)
			failuresByGroup[groupKey] = previous
			return
		}
		failuresByGroup[groupKey] = managedUpstreamKeyReconcileFailure{GroupName: groupName, Err: failure}
	}
	groupFailed := func(groupKey string) bool {
		_, failed := failuresByGroup[groupKey]
		return failed
	}
	fatalFailure := func(failure error) bool {
		// Only the caller's canceled/deadline context or a changed account
		// identity invalidates the channel-wide inventory. Provider timeouts,
		// throttling and 5xx responses from one mutation stay group-scoped.
		return ctx != nil && ctx.Err() != nil ||
			errors.Is(failure, storage.ErrStaleChannelAccountIdentityRevision)
	}
	deleteRemote := func(item connector.APIKey, generation storage.UpstreamGroupGeneration, groupPresent bool, groupName string) (bool, error) {
		operationCtx, cancelOperation := managedKeyRemoteOperationContext(ctx)
		defer cancelOperation()
		return s.deleteManagedRemoteKeyForSync(operationCtx, channelID, expectedIdentityRevision, item, generation, groupPresent, groupName)
	}
	revealRemote := func(keyID int64) (string, error) {
		operationCtx, cancelOperation := managedKeyRemoteOperationContext(ctx)
		defer cancelOperation()
		return s.revealAPIKeyForIdentityRevision(operationCtx, channelID, expectedIdentityRevision, keyID)
	}
	result := func() []managedUpstreamKeyReconcileFailure {
		failures := make([]managedUpstreamKeyReconcileFailure, 0, len(failuresByGroup))
		for _, failure := range failuresByGroup {
			failures = append(failures, failure)
		}
		sort.Slice(failures, func(i, j int) bool {
			return canonicalGroupName(failures[i].GroupName) < canonicalGroupName(failures[j].GroupName)
		})
		return failures
	}
	pendingRemovalFailure := func(keyID int64) error {
		return fmt.Errorf("managed upstream key %d removal is pending revalidation or manual review", keyID)
	}
	deletedInReconcile := make(map[int64]struct{})
	groupGenerations := make(map[string]storage.UpstreamGroupGeneration)
	getGeneration := func(groupKey, groupName string, present bool) (storage.UpstreamGroupGeneration, error) {
		if generation, ok := groupGenerations[groupKey]; ok {
			return generation, nil
		}
		var generation storage.UpstreamGroupGeneration
		if present {
			generation, err = s.Store.CaptureUpstreamGroupGeneration(channelID, groupName)
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if recordErr := s.Store.RecordUpstreamGroupChange(channelID, groupName, "added", 0, upstreamKeyNameForGroup(groupName), time.Now()); recordErr != nil {
					return storage.UpstreamGroupGeneration{}, recordErr
				}
				generation, err = s.Store.CaptureUpstreamGroupGeneration(channelID, groupName)
			}
		} else {
			generation, err = s.Store.RetireUpstreamGroupForKeyRemoval(channelID, expectedIdentityRevision, groupName, time.Now(), true)
		}
		if err != nil {
			return storage.UpstreamGroupGeneration{}, err
		}
		groupGenerations[groupKey] = generation
		return generation, nil
	}
	for _, local := range localKeys {
		groupKey := canonicalGroupName(local.GroupName)
		if _, active := activeGroups[groupKey]; !active || local.UpstreamKeyID <= 0 || !isStronglyManagedUpstreamKeyName(local.UpstreamKeyName) {
			continue
		}
		if groupFailed(groupKey) {
			continue
		}
		remoteItem, present := remoteByID[local.UpstreamKeyID]
		if !present {
			groupName := strings.TrimSpace(activeGroupNames[groupKey])
			if groupName == "" {
				groupName = strings.TrimSpace(local.GroupName)
			}
			if err := s.Store.ResetUpstreamKeyAfterManagedReconcile(channelID, expectedIdentityRevision, groupName, "managed upstream key disappeared"); err != nil {
				failure := fmt.Errorf("reset disappeared managed upstream key %q: %w", groupName, err)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(groupName, failure)
			}
			continue
		}
		remoteGroupKey := canonicalGroupName(groupForRemote(remoteItem))
		if !isStronglyManagedUpstreamKeyName(remoteItem.Name) {
			groupName := strings.TrimSpace(activeGroupNames[groupKey])
			if groupName == "" {
				groupName = strings.TrimSpace(local.GroupName)
			}
			if err := s.Store.ResetUpstreamKeyAfterManagedReconcile(channelID, expectedIdentityRevision, groupName, "managed upstream key ownership could not be revalidated"); err != nil {
				failure := fmt.Errorf("reset unverified managed upstream key %q: %w", groupName, err)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(groupName, failure)
			}
			continue
		}
		if remoteGroupKey != groupKey {
			groupName := strings.TrimSpace(activeGroupNames[groupKey])
			if groupName == "" {
				groupName = strings.TrimSpace(local.GroupName)
			}
			generation, generationErr := getGeneration(groupKey, groupName, true)
			if generationErr != nil {
				failure := fmt.Errorf("capture changed managed upstream key generation %q: %w", groupName, generationErr)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(groupName, failure)
				continue
			}
			deleted, err := deleteRemote(remoteItem, generation, true, groupName)
			if err != nil {
				failure := fmt.Errorf("replace changed managed upstream key %d: %w", remoteItem.ID, err)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(groupName, failure)
				continue
			}
			if !deleted {
				recordFailure(groupName, pendingRemovalFailure(remoteItem.ID))
				continue
			}
			deletedInReconcile[remoteItem.ID] = struct{}{}
			groupName = strings.TrimSpace(activeGroupNames[groupKey])
			if groupName == "" {
				groupName = strings.TrimSpace(local.GroupName)
			}
			if err := s.Store.ResetUpstreamKeyAfterManagedReconcile(channelID, expectedIdentityRevision, groupName, "managed upstream key changed"); err != nil {
				failure := fmt.Errorf("reset changed managed upstream key %q: %w", groupName, err)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(groupName, failure)
			}
		}
	}
	byGroup := make(map[string][]connector.APIKey)
	inventoryGroupNames := make(map[string]string)
	for _, item := range remote {
		if item.ID <= 0 || !isStronglyManagedUpstreamKeyName(item.Name) {
			continue
		}
		if _, deleted := deletedInReconcile[item.ID]; deleted {
			continue
		}
		groupName := groupForRemote(item)
		if groupName == "" {
			orphanName, orphanKey, orphaned := deletedSub2APIManagedKeyInventoryGroup(item)
			if !orphaned {
				continue
			}
			groupName = orphanName
			inventoryGroupNames[orphanKey] = orphanName
			byGroup[orphanKey] = append(byGroup[orphanKey], item)
			continue
		}
		groupKey := canonicalGroupName(groupName)
		inventoryGroupNames[groupKey] = groupName
		byGroup[groupKey] = append(byGroup[groupKey], item)
	}
groupInventoryLoop:
	for groupKey, items := range byGroup {
		expectedGroupName := strings.TrimSpace(activeGroupNames[groupKey])
		if expectedGroupName == "" {
			expectedGroupName = strings.TrimSpace(inventoryGroupNames[groupKey])
		}
		if expectedGroupName == "" && len(items) > 0 {
			expectedGroupName = strings.TrimSpace(groupForRemote(items[0]))
		}
		if groupFailed(groupKey) {
			continue
		}
		if _, active := activeGroups[groupKey]; !active {
			generation, generationErr := getGeneration(groupKey, expectedGroupName, false)
			if generationErr != nil {
				failure := fmt.Errorf("retire orphaned managed upstream group %q: %w", expectedGroupName, generationErr)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(expectedGroupName, failure)
				continue
			}
			for _, item := range items {
				deleted, err := deleteRemote(item, generation, false, expectedGroupName)
				if err != nil {
					failure := fmt.Errorf("delete orphaned managed upstream key %d: %w", item.ID, err)
					if fatalFailure(failure) {
						return result(), failure
					}
					recordFailure(expectedGroupName, failure)
					continue groupInventoryLoop
				}
				if !deleted {
					recordFailure(expectedGroupName, pendingRemovalFailure(item.ID))
					continue groupInventoryLoop
				}
				if alreadyDeletedIDs != nil {
					alreadyDeletedIDs[item.ID] = struct{}{}
				}
			}
			continue
		}

		generation, generationErr := getGeneration(groupKey, expectedGroupName, true)
		if generationErr != nil {
			failure := fmt.Errorf("capture active managed upstream group %q: %w", expectedGroupName, generationErr)
			if fatalFailure(failure) {
				return result(), failure
			}
			recordFailure(expectedGroupName, failure)
			continue
		}
		var selected *connector.APIKey
		if expectedGroupName == "" {
			expectedGroupName = groupKey
		}
		for index := range items {
			item := items[index]
			if !managedUpstreamKeyExpectedAttributes(item, expectedGroupName) {
				continue
			}
			if selected == nil || item.ID < selected.ID {
				copyItem := item
				selected = &copyItem
			}
		}

		deletedAny := false
		resetLocal := false
		for _, item := range items {
			if selected != nil && item.ID == selected.ID {
				continue
			}
			deleted, err := deleteRemote(item, generation, true, expectedGroupName)
			if err != nil {
				failure := fmt.Errorf("replace managed upstream key %d: %w", item.ID, err)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(expectedGroupName, failure)
				continue groupInventoryLoop
			}
			if !deleted {
				recordFailure(expectedGroupName, pendingRemovalFailure(item.ID))
				continue groupInventoryLoop
			}
			deletedAny = true
			for _, local := range localKeys {
				if local.UpstreamKeyID == item.ID && canonicalGroupName(local.GroupName) == groupKey {
					resetLocal = true
					break
				}
			}
			if alreadyDeletedIDs != nil {
				alreadyDeletedIDs[item.ID] = struct{}{}
			}
		}
		if deletedAny && resetLocal {
			// The encrypted local secret and remote identity are no longer
			// authoritative after a managed key was removed. Keep group-level
			// settings, but force the normal provisioning path to create a key.
			if err := s.Store.ResetUpstreamKeyAfterManagedReconcile(channelID, expectedIdentityRevision, expectedGroupName, "managed upstream key reconciled"); err != nil {
				failure := fmt.Errorf("reset local managed upstream key %q: %w", groupKey, err)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(expectedGroupName, failure)
				continue
			}
		}
		if selected == nil {
			continue
		}
		if local := s.usableLocalManagedKeyMatchingRemote(localKeys, *selected, expectedGroupName); local != nil {
			// Managed provisioning keys are immutable at supported providers. When
			// the complete inventory still reports the exact ID, name, group and
			// creation fingerprint and the persisted secret decrypts locally, a
			// reveal adds no identity evidence and can throttle the whole scan.
			if (selected.GroupRatio > 0 && local.GroupRatio != selected.GroupRatio) || strings.TrimSpace(local.LastError) != "" {
				if selected.GroupRatio > 0 {
					local.GroupRatio = selected.GroupRatio
				}
				local.LastError = ""
				if err := s.Store.UpsertUpstreamKey(local); err != nil {
					failure := fmt.Errorf("refresh local managed upstream key metadata %q: %w", expectedGroupName, err)
					if fatalFailure(failure) {
						return result(), failure
					}
					recordFailure(expectedGroupName, failure)
				}
			}
			continue
		}
		plain, revealErr := revealRemote(selected.ID)
		if fatalFailure(revealErr) {
			return result(), revealErr
		}
		if revealErr != nil || strings.TrimSpace(plain) == "" {
			deleted, err := deleteRemote(*selected, generation, true, expectedGroupName)
			if err != nil {
				failure := fmt.Errorf("replace unrevealable managed upstream key %d: %w", selected.ID, err)
				if revealErr != nil {
					failure = errors.Join(fmt.Errorf("reveal managed upstream key %d: %w", selected.ID, revealErr), failure)
				}
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(expectedGroupName, failure)
				continue
			}
			if !deleted {
				recordFailure(expectedGroupName, pendingRemovalFailure(selected.ID))
				continue
			}
			if alreadyDeletedIDs != nil {
				alreadyDeletedIDs[selected.ID] = struct{}{}
			}
			resetLocal = true
			if err := s.Store.ResetUpstreamKeyAfterManagedReconcile(channelID, expectedIdentityRevision, expectedGroupName, "managed upstream key could not be revealed"); err != nil {
				failure := fmt.Errorf("reset unrevealed managed upstream key %q: %w", groupKey, err)
				if fatalFailure(failure) {
					return result(), failure
				}
				recordFailure(expectedGroupName, failure)
			}
			continue
		}
		if local, findErr := s.Store.FindUpstreamKeyAny(channelID, expectedGroupName); findErr == nil {
			// A human/adopted local key remains authoritative unless its own
			// recorded remote name carries the strong provisioning prefix. Do not
			// silently replace its secret merely because another managed key exists
			// in the same upstream group.
			if !isStronglyManagedUpstreamKeyName(local.UpstreamKeyName) && strings.TrimSpace(local.UpstreamKeyName) != "" {
				continue
			}
		} else if !errors.Is(findErr, gorm.ErrRecordNotFound) {
			failure := fmt.Errorf("read local managed upstream key %q: %w", expectedGroupName, findErr)
			if fatalFailure(failure) {
				return result(), failure
			}
			recordFailure(expectedGroupName, failure)
			continue
		}
		if err := s.saveUpstreamKey(channelID, strings.TrimSpace(plain), expectedGroupName, selected.GroupRatio, selected, storage.GatewayUpstreamKeyProvenanceCreated); err != nil {
			failure := fmt.Errorf("refresh local managed upstream key %q: %w", expectedGroupName, err)
			if fatalFailure(failure) {
				return result(), failure
			}
			recordFailure(expectedGroupName, failure)
		}
	}
	return result(), nil
}

func (s *Service) usableLocalManagedKeyMatchingRemote(localKeys []storage.GatewayUpstreamKey, remote connector.APIKey, groupName string) *storage.GatewayUpstreamKey {
	if s == nil || s.Cipher == nil || remote.ID <= 0 || !managedUpstreamKeyExpectedAttributes(remote, groupName) {
		return nil
	}
	groupKey := canonicalGroupName(groupName)
	for _, local := range localKeys {
		if canonicalGroupName(local.GroupName) != groupKey || local.UpstreamKeyID != remote.ID ||
			strings.TrimSpace(local.UpstreamKeyName) != strings.TrimSpace(remote.Name) ||
			strings.TrimSpace(local.OwnershipProvenance) != storage.GatewayUpstreamKeyProvenanceCreated ||
			!local.Enabled || local.Provisioning || strings.TrimSpace(local.APIKeyCipher) == "" {
			continue
		}
		plain, err := s.Cipher.Decrypt(local.APIKeyCipher)
		if err != nil || strings.TrimSpace(plain) == "" {
			return nil
		}
		if prefix := strings.TrimSpace(local.APIKeyPrefix); prefix != "" && prefix != keyPrefix(plain) {
			return nil
		}
		matched := local
		return &matched
	}
	return nil
}

func (s *Service) deleteManagedRemoteKeyForSync(ctx context.Context, channelID uint, expectedIdentityRevision uint64, item connector.APIKey, generation storage.UpstreamGroupGeneration, groupPresent bool, groupName string) (bool, error) {
	if item.ID <= 0 || !isStronglyManagedUpstreamKeyName(item.Name) {
		return false, errors.New("remote key is not a deletable managed key")
	}
	groupName = strings.TrimSpace(groupName)
	if groupName == "" {
		return false, errors.New("managed remote key group is required")
	}
	evidenceDigest := managedInventoryRemovalEvidenceDigest(generation, item)
	ownershipClaim := HashKey("managed-prefix:" + strings.TrimSpace(item.Name))
	store := s.Store.WithContext(ctx)
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyInventoryRemoval(
		generation, expectedIdentityRevision, groupName, item.ID, item.Name,
		groupPresent, evidenceDigest, ownershipClaim, time.Now(), managedUpstreamKeyRemovalStaleAfter,
	)
	if err != nil {
		return false, err
	}
	if !claimed {
		if intent != nil {
			switch intent.Status {
			case "processing", "ignored":
				// Leave an in-flight or manually quarantined intent alone. A later
				// inventory pass can reclaim a stale processing claim safely. The
				// caller must not clear local state or treat this as a deletion.
				return false, nil
			case "acknowledged":
				if intent.RemovalClaimEpoch == nil || *intent.RemovalClaimEpoch == 0 {
					return false, storage.ErrManagedUpstreamKeyRemovalIntentChanged
				}
				// Re-run the fenced finalizer without a provider mutation. Besides
				// repairing rows acknowledged by older releases, this makes a retry
				// after a commit response loss idempotently finish the exact local
				// binding cleanup.
				if err := store.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalKeepLocal(
					generation, expectedIdentityRevision, *intent.RemovalClaimEpoch, intent.ID, item.ID, item.Name,
					groupPresent, evidenceDigest, ownershipClaim, time.Now(), nil,
				); err != nil {
					return false, err
				}
				markUpstreamInventoryPassKeyDeleted(ctx, channelID, item.ID)
				return true, nil
			}
		}
		return false, storage.ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if intent == nil || intent.RemovalClaimEpoch == nil || *intent.RemovalClaimEpoch == 0 {
		return false, storage.ErrManagedUpstreamKeyRemovalIntentChanged
	}
	claimEpoch := *intent.RemovalClaimEpoch
	deleteRemote, err := s.Keys.PrepareManagedAPIKeyDeletion(ctx, channelID, item, expectedIdentityRevision)
	if err != nil {
		return false, err
	}
	preserveLocal := false
	if !groupPresent {
		localKeys, listErr := store.ListUpstreamKeys([]uint{channelID})
		if listErr != nil {
			return false, listErr
		}
		itemGroupKey := canonicalGroupName(groupName)
		for _, local := range localKeys {
			if canonicalGroupName(local.GroupName) != itemGroupKey {
				continue
			}
			if local.UpstreamKeyID != item.ID || !isStronglyManagedUpstreamKeyName(local.UpstreamKeyName) {
				preserveLocal = true
				break
			}
		}
	}
	remoteDeleteAttempted := false
	remoteDeleteCompleted := false
	trackedDeleteRemote := func() error {
		remoteDeleteAttempted = true
		deleteErr := deleteRemote()
		remoteDeleteCompleted = deleteErr == nil
		return deleteErr
	}
	var finalizeErr error
	if preserveLocal {
		finalizeErr = store.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalKeepLocal(
			generation, expectedIdentityRevision, claimEpoch, intent.ID, item.ID, item.Name,
			groupPresent, evidenceDigest, ownershipClaim, time.Now(), trackedDeleteRemote,
		)
	} else {
		finalizeErr = store.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(
			generation, expectedIdentityRevision, claimEpoch, intent.ID, item.ID, item.Name,
			groupPresent, evidenceDigest, ownershipClaim, time.Now(), trackedDeleteRemote,
		)
	}
	if finalizeErr != nil {
		if remoteDeleteCompleted {
			cleanupErr := s.withManagedKeyRemovalCleanupStore(func(cleanupStore *storage.Gateway) error {
				if preserveLocal {
					return cleanupStore.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalKeepLocal(
						generation, expectedIdentityRevision, claimEpoch, intent.ID, item.ID, item.Name,
						groupPresent, evidenceDigest, ownershipClaim, time.Now(), nil,
					)
				}
				return cleanupStore.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(
					generation, expectedIdentityRevision, claimEpoch, intent.ID, item.ID, item.Name,
					groupPresent, evidenceDigest, ownershipClaim, time.Now(), nil,
				)
			})
			if cleanupErr == nil {
				markUpstreamInventoryPassKeyDeleted(ctx, channelID, item.ID)
				return true, nil
			}
			finalizeErr = errors.Join(finalizeErr, fmt.Errorf("finalize completed managed upstream key inventory removal: %w", cleanupErr))
		}
		if errors.Is(finalizeErr, connector.ErrManagedAPIKeyDeletionIdentityChanged) {
			reviewErr := s.withManagedKeyRemovalCleanupStore(func(cleanupStore *storage.Gateway) error {
				return cleanupStore.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalManualReview(
					generation, expectedIdentityRevision, claimEpoch, intent.ID, item.ID, item.Name,
					groupPresent, evidenceDigest, ownershipClaim, "remote_identity_changed", time.Now(),
				)
			})
			if reviewErr != nil {
				return false, errors.Join(finalizeErr, fmt.Errorf("record managed upstream key identity drift: %w", reviewErr))
			}
			return false, nil
		}
		if remoteDeleteAttempted {
			quarantineErr := s.withManagedKeyRemovalCleanupStore(func(cleanupStore *storage.Gateway) error {
				return cleanupStore.QuarantineGenerationSafeManagedUpstreamKeyInventoryRemoval(
					generation, expectedIdentityRevision, claimEpoch, intent.ID, item.ID, item.Name,
					groupPresent, evidenceDigest, ownershipClaim, time.Now(),
				)
			})
			if quarantineErr != nil {
				return false, errors.Join(finalizeErr, fmt.Errorf("quarantine uncertain managed upstream key inventory removal: %w", quarantineErr))
			}
		}
		return false, finalizeErr
	}
	markUpstreamInventoryPassKeyDeleted(ctx, channelID, item.ID)
	return true, nil
}

func managedInventoryRemovalEvidenceDigest(generation storage.UpstreamGroupGeneration, item connector.APIKey) string {
	return HashKey(fmt.Sprintf("managed-key-inventory-removal:v1:%d:%d:%d:%s:%d:%s", generation.ChannelID, generation.ID, generation.PresenceGeneration, generation.CanonicalGroupName, item.ID, strings.TrimSpace(item.Name)))
}

func managedUpstreamKeyExpectedAttributes(key connector.APIKey, groupName string) bool {
	groupName = strings.TrimSpace(groupName)
	if !isStronglyManagedUpstreamKeyName(key.Name) {
		return false
	}
	if strings.TrimSpace(key.GroupName) != "" || strings.TrimSpace(key.Group) != "" {
		if !apiKeyGroupMatches(key, groupName, nil) {
			return false
		}
	}
	return apiKeyReusable(key) && managedUpstreamKeyCreationFingerprintMatches(key, true)
}

// managedKeyRebuildManualReviewWriteError classifies expected generation and
// claim races. Those races are already durable state transitions (or belong to
// a newer claimant); real persistence failures must remain visible to callers.
func managedKeyRebuildManualReviewWriteError(err error) error {
	if err == nil || errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) ||
		errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) ||
		errors.Is(err, storage.ErrManagedUpstreamKeyRebuildClaimLost) {
		return nil
	}
	return err
}

// RebuildManagedUpstreamKeys is an explicit destructive recovery operation.
// It removes only BridgeOps-managed remote keys, preserves local routing
// configuration and then provisions one fresh key for every current group.
func (s *Service) RebuildManagedUpstreamKeys(ctx context.Context, channelID uint) (*ManagedUpstreamKeyRebuildResult, error) {
	if s == nil || s.Channels == nil || s.Rates == nil || s.Store == nil || s.Keys == nil {
		return nil, errors.New("gateway key management is unavailable")
	}
	if channelID == 0 {
		return nil, errors.New("channel id is required")
	}
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return nil, err
	}
	defer unlock()

	// Take the operation lock before capturing the account identity. An
	// administrator edit must not leave this rebuild using a pre-lock snapshot.
	channel, err := s.Channels.FindByID(channelID)
	if err != nil {
		return nil, err
	}
	if channel.CredentialMode == storage.CredentialModeDirect {
		return nil, errors.New("direct channels do not have managed upstream keys to rebuild")
	}
	channelIdentityRevision := channel.UpstreamAccountIdentityRevision
	if channelIdentityRevision == 0 {
		return nil, errors.New("channel account identity revision is required")
	}

	remoteInventory, err := s.listAllUpstreamKeysForIdentityRevision(ctx, channelID, channel.UpstreamAccountIdentityRevision)
	if err != nil {
		return nil, fmt.Errorf("list remote upstream keys for rebuild: %w", err)
	}
	if err := s.Channels.RequireUpstreamAccountIdentityRevision(channelID, channelIdentityRevision); err != nil {
		return nil, fmt.Errorf("verify channel identity after rebuilding inventory: %w", err)
	}
	presentRemote := make(map[int64]string, len(remoteInventory))
	for _, item := range remoteInventory {
		if item.ID > 0 {
			presentRemote[item.ID] = strings.TrimSpace(item.Name)
		}
	}
	if err := s.Store.ReconcileGenerationSafeManagedUpstreamKeyRebuilds(channelID, channelIdentityRevision, presentRemote, time.Now()); err != nil {
		return nil, fmt.Errorf("reconcile managed upstream key rebuild intents: %w", err)
	}
	localKeys, err := s.Store.ListUpstreamKeys([]uint{channelID})
	if err != nil {
		return nil, fmt.Errorf("list local upstream keys: %w", err)
	}
	managed, err := s.strictManagedKeysForRebuildInventory(ctx, channelID, channelIdentityRevision, localKeys, remoteInventory)
	if err != nil {
		return nil, fmt.Errorf("list managed upstream keys: %w", err)
	}
	result := &ManagedUpstreamKeyRebuildResult{Matched: len(managed), Items: make([]ModelFamilySyncItem, 0)}
	if err := s.Store.PrepareChannelUpstreamKeysForRebuild(channelID, channelIdentityRevision, managedUpstreamKeyRebuildPendingError); err != nil {
		return result, fmt.Errorf("prepare local upstream key rebuild: %w", err)
	}
	s.forgetChannelUpstreamKeyCaches(channelID)
	for _, target := range managed {
		if target.Absent {
			removed, removeErr := s.removeManagedKeyForAbsentGeneration(
				ctx, target.Generation, target.Local.GroupName, &target.Local, target.Remote, channelIdentityRevision, true,
			)
			if removeErr != nil {
				return result, fmt.Errorf("remove obsolete managed upstream key %d after deleting %d of %d: %w", target.Remote.ID, result.Deleted, result.Matched, removeErr)
			}
			if !removed {
				return result, fmt.Errorf("obsolete managed upstream key %d requires recovery or manual review", target.Remote.ID)
			}
			result.Deleted++
			continue
		}
		intent, claimed, beginErr := s.Store.BeginGenerationSafeManagedUpstreamKeyRebuild(
			target.Generation, target.ChannelIdentityRevision, target.Local, target.Remote.ID, target.Remote.Name,
			target.EvidenceDigest, target.OwnershipClaim, time.Now(), managedUpstreamKeyRemovalStaleAfter,
		)
		if beginErr != nil {
			return result, fmt.Errorf("begin managed upstream key %d rebuild after deleting %d of %d: %w", target.Remote.ID, result.Deleted, result.Matched, beginErr)
		}
		if intent == nil {
			return result, fmt.Errorf("managed upstream key %d rebuild requires recovery or manual review", target.Remote.ID)
		}
		var current *connector.APIKey
		if !claimed {
			if intent.Status != "manual_review" {
				return result, fmt.Errorf("managed upstream key %d rebuild requires recovery or manual review", target.Remote.ID)
			}
			current, err = s.revalidateManagedKeyRebuildTarget(ctx, channelID, target)
			remoteAbsent := errors.Is(err, gorm.ErrRecordNotFound)
			resolution := "facts_revalidated"
			if remoteAbsent {
				resolution = "remote_absence_revalidated"
			} else if err != nil {
				return result, fmt.Errorf("revalidate manual-review managed upstream key %d: %w", target.Remote.ID, err)
			}
			resolved, resolveErr := s.Store.ResolveGenerationSafeManagedUpstreamKeyRebuildManualReview(
				target.Generation, target.ChannelIdentityRevision, intent.ID, target.Local, target.Remote.ID, target.Remote.Name,
				target.EvidenceDigest, target.OwnershipClaim, "administrator_rebuild", resolution, time.Now(),
			)
			if resolveErr != nil {
				return result, fmt.Errorf("resolve manual-review managed upstream key %d: %w", target.Remote.ID, resolveErr)
			}
			intent = resolved
			claimed = true
			if remoteAbsent {
				if finalizeErr := s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
					target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, target.Local, target.Remote.ID, target.Remote.Name,
					target.EvidenceDigest, target.OwnershipClaim, time.Now(), nil,
				); finalizeErr != nil {
					if errors.Is(finalizeErr, storage.ErrUpstreamGroupGenerationChanged) || errors.Is(finalizeErr, storage.ErrStaleChannelAccountIdentityRevision) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRemovalIdentityConflict) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRemovalIntentChanged) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRebuildClaimLost) {
						if reviewErr := managedKeyRebuildManualReviewWriteError(s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, "finalize_absence_revalidation_failed", "administrator_rebuild", time.Now())); reviewErr != nil {
							finalizeErr = errors.Join(finalizeErr, fmt.Errorf("record managed upstream key rebuild manual review: %w", reviewErr))
						}
					}
					return result, fmt.Errorf("finalize absent managed upstream key %d rebuild: %w", target.Remote.ID, finalizeErr)
				}
				result.Deleted++
				continue
			}
		}
		if current == nil {
			current, err = s.revalidateManagedKeyRebuildTarget(ctx, channelID, target)
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if finalizeErr := s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
				target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, target.Local, target.Remote.ID, target.Remote.Name,
				target.EvidenceDigest, target.OwnershipClaim, time.Now(), nil,
			); finalizeErr != nil {
				if errors.Is(finalizeErr, storage.ErrUpstreamGroupGenerationChanged) || errors.Is(finalizeErr, storage.ErrStaleChannelAccountIdentityRevision) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRemovalIdentityConflict) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRemovalIntentChanged) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRebuildClaimLost) {
					if reviewErr := managedKeyRebuildManualReviewWriteError(s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, "finalize_absence_revalidation_failed", "administrator_rebuild", time.Now())); reviewErr != nil {
						finalizeErr = errors.Join(finalizeErr, fmt.Errorf("record managed upstream key rebuild manual review: %w", reviewErr))
					}
				}
				return result, fmt.Errorf("finalize absent managed upstream key %d rebuild: %w", target.Remote.ID, finalizeErr)
			}
			result.Deleted++
			continue
		}
		if err != nil {
			if reviewErr := managedKeyRebuildManualReviewWriteError(s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, "remote_revalidation_failed", "administrator_rebuild", time.Now())); reviewErr != nil {
				err = errors.Join(err, fmt.Errorf("record managed upstream key rebuild manual review: %w", reviewErr))
			}
			return result, fmt.Errorf("revalidate managed upstream key %d after deleting %d of %d: %w", target.Remote.ID, result.Deleted, result.Matched, err)
		}
		remoteDelete, prepareErr := s.Keys.PrepareManagedAPIKeyDeletion(ctx, channelID, *current, target.ChannelIdentityRevision)
		if prepareErr != nil {
			if reviewErr := managedKeyRebuildManualReviewWriteError(s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, "prepared_delete_failed", "administrator_rebuild", time.Now())); reviewErr != nil {
				prepareErr = errors.Join(prepareErr, fmt.Errorf("record managed upstream key rebuild manual review: %w", reviewErr))
			}
			return result, fmt.Errorf("prepare managed upstream key %d deletion after deleting %d of %d: %w", current.ID, result.Deleted, result.Matched, prepareErr)
		}
		finalizeErr := s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
			target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, target.Local, current.ID, current.Name,
			target.EvidenceDigest, target.OwnershipClaim, time.Now(), remoteDelete,
		)
		if finalizeErr != nil {
			if errors.Is(finalizeErr, connector.ErrManagedAPIKeyDeletionIdentityChanged) {
				if reviewErr := managedKeyRebuildManualReviewWriteError(s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, "remote_identity_changed", "administrator_rebuild", time.Now())); reviewErr != nil {
					finalizeErr = errors.Join(finalizeErr, fmt.Errorf("record managed upstream key rebuild identity drift: %w", reviewErr))
				}
				return result, fmt.Errorf("delete managed upstream key %d after deleting %d of %d: %w", current.ID, result.Deleted, result.Matched, finalizeErr)
			}
			if errors.Is(finalizeErr, storage.ErrUpstreamGroupGenerationChanged) || errors.Is(finalizeErr, storage.ErrStaleChannelAccountIdentityRevision) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRemovalIdentityConflict) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRemovalIntentChanged) || errors.Is(finalizeErr, storage.ErrManagedUpstreamKeyRebuildClaimLost) {
				if reviewErr := managedKeyRebuildManualReviewWriteError(s.Store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(target.Generation, target.ChannelIdentityRevision, intent.ID, intent.ClaimEpoch, "finalize_revalidation_failed", "administrator_rebuild", time.Now())); reviewErr != nil {
					finalizeErr = errors.Join(finalizeErr, fmt.Errorf("record managed upstream key rebuild manual review: %w", reviewErr))
				}
			}
			return result, fmt.Errorf("delete managed upstream key %d after deleting %d of %d: %w", current.ID, result.Deleted, result.Matched, finalizeErr)
		}
		result.Deleted++
	}
	rates, err := s.Rates.ListByChannel(channelID)
	if err != nil {
		return result, fmt.Errorf("list rebuild groups: %w", err)
	}
	rates = canonicalRateSnapshots(rates)
	preferredGroupNames := make([]string, 0, len(rates))
	for _, rate := range rates {
		preferredGroupNames = append(preferredGroupNames, rate.ModelName)
	}
	if err := s.Store.CanonicalizeChannelUpstreamGroupsForRebuild(channelID, channelIdentityRevision, preferredGroupNames); err != nil {
		return result, fmt.Errorf("canonicalize local upstream groups: %w", err)
	}
	if err := s.Store.ResetChannelUpstreamKeysForRebuild(channelID, channelIdentityRevision); err != nil {
		return result, fmt.Errorf("finalize local upstream key rebuild after deleting %d keys: %w", result.Deleted, err)
	}
	if err := s.Channels.RequireUpstreamAccountIdentityRevision(channelID, channelIdentityRevision); err != nil {
		return result, fmt.Errorf("verify channel identity before rebuilding keys: %w", err)
	}
	items, err := s.syncChannelModelFamiliesLocked(ctx, channel, true, true)
	if err != nil {
		return result, err
	}
	result.Items = items
	for _, item := range items {
		if !item.Skipped && strings.TrimSpace(item.Error) == "" {
			result.Recreated++
		}
	}
	return result, nil
}

func (s *Service) listAllUpstreamKeys(ctx context.Context, channelID uint) ([]connector.APIKey, error) {
	return s.listAllUpstreamKeysForIdentityRevision(ctx, channelID, 0)
}

func (s *Service) listAllUpstreamKeysForIdentityRevision(ctx context.Context, channelID uint, channelIdentityRevision uint64) ([]connector.APIKey, error) {
	return s.listAllUpstreamKeysForIdentityRevisionWithOptions(ctx, channelID, channelIdentityRevision, false)
}

// listAllUpstreamKeysAllowingDuplicateNames keeps a complete remote snapshot
// when an upstream provider contains duplicate names. Callers must resolve
// ownership by remote ID and perform their own name-reuse checks; this helper
// must not be used by code that auto-adopts or destructively rebuilds keys.
func (s *Service) listAllUpstreamKeysAllowingDuplicateNames(ctx context.Context, channelID uint, channelIdentityRevision uint64) ([]connector.APIKey, error) {
	return s.listAllUpstreamKeysForIdentityRevisionWithOptions(ctx, channelID, channelIdentityRevision, true)
}

func (s *Service) listAllUpstreamKeysForIdentityRevisionWithOptions(ctx context.Context, channelID uint, channelIdentityRevision uint64, allowDuplicateNames bool) ([]connector.APIKey, error) {
	if cache := upstreamInventoryCacheFromContext(ctx, channelID); cache != nil {
		cache.bindIdentityRevision(channelIdentityRevision)
		if remote, ok := cache.currentKeys(); ok {
			if err := s.requireUpstreamAccountIdentityRevision(channelID, channelIdentityRevision); err != nil {
				return nil, err
			}
			return validateCompleteUpstreamKeyInventory(remote, allowDuplicateNames)
		}
	}
	remote := make([]connector.APIKey, 0)
	for pageNum := 1; pageNum <= upstreamKeyLookupMaxPages; pageNum++ {
		if err := s.requireUpstreamAccountIdentityRevision(channelID, channelIdentityRevision); err != nil {
			return nil, err
		}
		page, err := s.Keys.ListAPIKeys(ctx, channelID, connector.APIKeyQuery{
			Page: pageNum, PageSize: upstreamKeyLookupPageSize, RequireCompleteGroupIdentity: true,
		})
		if err != nil {
			return nil, err
		}
		if err := s.requireUpstreamAccountIdentityRevision(channelID, channelIdentityRevision); err != nil {
			return nil, err
		}
		if page != nil {
			remote = append(remote, page.Items...)
		}
		if apiKeyPageComplete(page, pageNum, upstreamKeyLookupPageSize) {
			if cache := upstreamInventoryCacheFromContext(ctx, channelID); cache != nil {
				cache.bindIdentityRevision(channelIdentityRevision)
				cache.storeKeys(remote)
			}
			return validateCompleteUpstreamKeyInventory(remote, allowDuplicateNames)
		}
	}
	return nil, errors.New("upstream api key list exceeds safe lookup limit")
}

func validateCompleteUpstreamKeyInventory(remote []connector.APIKey, allowDuplicateNames bool) ([]connector.APIKey, error) {
	validated := append([]connector.APIKey(nil), remote...)
	seen := make(map[int64]struct{})
	seenNames := make(map[string]int64)
	for _, item := range validated {
		if item.ID > 0 {
			if _, ok := seen[item.ID]; ok {
				return nil, fmt.Errorf("upstream api key id %d is ambiguous", item.ID)
			}
			seen[item.ID] = struct{}{}
		}
		name := strings.TrimSpace(item.Name)
		if previousID, ok := seenNames[name]; name != "" && ok && !allowDuplicateNames {
			return nil, fmt.Errorf("upstream api key name %q is ambiguous between ids %d and %d", name, previousID, item.ID)
		}
		if name != "" {
			seenNames[name] = item.ID
		}
	}
	sort.Slice(validated, func(i, j int) bool { return validated[i].ID < validated[j].ID })
	return validated, nil
}

func (s *Service) revealAPIKeyForIdentityRevision(ctx context.Context, channelID uint, channelIdentityRevision uint64, keyID int64) (string, error) {
	if err := s.requireUpstreamAccountIdentityRevisionContext(ctx, channelID, channelIdentityRevision); err != nil {
		return "", err
	}
	plain, err := s.Keys.RevealAPIKey(ctx, channelID, keyID)
	if err != nil {
		return "", err
	}
	if err := s.requireUpstreamAccountIdentityRevisionContext(ctx, channelID, channelIdentityRevision); err != nil {
		return "", err
	}
	return plain, nil
}

func (s *Service) listAPIKeyGroupsForIdentityRevision(ctx context.Context, channelID uint, channelIdentityRevision uint64) ([]connector.APIKeyGroup, error) {
	if cache := upstreamInventoryCacheFromContext(ctx, channelID); cache != nil {
		cache.bindIdentityRevision(channelIdentityRevision)
	}
	if err := s.requireUpstreamAccountIdentityRevisionContext(ctx, channelID, channelIdentityRevision); err != nil {
		return nil, err
	}
	groups, err := s.listAPIKeyGroupsUsingPassCache(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if err := s.requireUpstreamAccountIdentityRevisionContext(ctx, channelID, channelIdentityRevision); err != nil {
		return nil, err
	}
	return groups, nil
}

func (s *Service) requireUpstreamAccountIdentityRevision(channelID uint, channelIdentityRevision uint64) error {
	return s.requireUpstreamAccountIdentityRevisionContext(context.Background(), channelID, channelIdentityRevision)
}

func (s *Service) requireUpstreamAccountIdentityRevisionContext(ctx context.Context, channelID uint, channelIdentityRevision uint64) error {
	if channelIdentityRevision == 0 {
		return nil
	}
	if s == nil || s.Channels == nil {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	return s.Channels.WithContext(ctx).RequireUpstreamAccountIdentityRevision(channelID, channelIdentityRevision)
}

func (s *Service) deleteOrphanedManagedUpstreamKeys(ctx context.Context, channelID uint, expectedIdentityRevision uint64, activeGroups map[string]struct{}, localKeys []storage.GatewayUpstreamKey, alreadyDeletedIDs map[int64]struct{}) error {
	localByRemoteID := uniqueManagedLocalKeysByRemoteID(localKeys)
	remote, err := s.listAllUpstreamKeysAllowingDuplicateNames(ctx, channelID, expectedIdentityRevision)
	if err != nil {
		return err
	}
	generations := make(map[string]storage.UpstreamGroupGeneration)
	for _, item := range remote {
		if item.ID <= 0 || !isStronglyManagedUpstreamKeyName(item.Name) {
			continue
		}
		if alreadyDeletedIDs != nil {
			if _, alreadyDeleted := alreadyDeletedIDs[item.ID]; alreadyDeleted {
				continue
			}
		}
		groupName := managedUpstreamKeyGroupName(item, localByRemoteID)
		forcedOrphan := false
		if groupName == "" {
			groupName, _, forcedOrphan = deletedSub2APIManagedKeyInventoryGroup(item)
		}
		if groupName == "" || (!forcedOrphan && groupNameMatchesAny(groupName, activeGroups)) {
			continue
		}
		groupKey := canonicalGroupName(groupName)
		generation, ok := generations[groupKey]
		if !ok {
			generation, err = s.Store.RetireUpstreamGroupForKeyRemoval(channelID, expectedIdentityRevision, groupName, time.Now(), true)
			if err != nil {
				return err
			}
			generations[groupKey] = generation
		}
		deleted, err := s.deleteManagedRemoteKeyForSync(ctx, channelID, expectedIdentityRevision, item, generation, false, groupName)
		if err != nil {
			return fmt.Errorf("delete orphaned managed upstream key %d: %w", item.ID, err)
		}
		if !deleted {
			continue
		}
		if alreadyDeletedIDs != nil {
			alreadyDeletedIDs[item.ID] = struct{}{}
		}
	}
	return nil
}

func managedUpstreamKeyGroupName(item connector.APIKey, localByID map[int64]storage.GatewayUpstreamKey) string {
	name := strings.TrimSpace(item.Name)
	for _, prefix := range []string{upstreamKeyName + " - ", legacyUpstreamKeyName + " - ", historicUpstreamKeyName + " - "} {
		if strings.HasPrefix(name, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(name, prefix))
		}
	}
	if upstreamProvisioningKeyNamePattern.MatchString(name) {
		groupName := strings.TrimSpace(item.GroupName)
		group := strings.TrimSpace(item.Group)
		if groupName != "" && group != "" && !strings.EqualFold(groupName, group) {
			return ""
		}
		if groupName != "" {
			return groupName
		}
		if group != "" {
			return group
		}
		// Random provisioning names deliberately do not encode the group. Only a
		// deleted Sub2API group has enough provider evidence to recover the old
		// name from a trusted local remote-ID binding.
		if item.Source != connector.APIKeySourceSub2API || item.GroupID == nil || *item.GroupID <= 0 || !item.GroupDirectoryComplete {
			return ""
		}
		if local, ok := localByID[item.ID]; ok && trustedManagedLocalGroupFallback(local) && strings.TrimSpace(local.UpstreamKeyName) == name {
			return strings.TrimSpace(local.GroupName)
		}
		return ""
	}
	if name == upstreamKeyName || name == legacyUpstreamKeyName || name == historicUpstreamKeyName {
		groupName := strings.TrimSpace(item.GroupName)
		if groupName != "" {
			return groupName
		}
		return strings.TrimSpace(item.Group)
	}
	return ""
}

func uniqueManagedLocalKeysByRemoteID(localKeys []storage.GatewayUpstreamKey) map[int64]storage.GatewayUpstreamKey {
	unique := make(map[int64]storage.GatewayUpstreamKey)
	ambiguous := make(map[int64]struct{})
	for _, local := range localKeys {
		if local.UpstreamKeyID <= 0 || !trustedManagedLocalGroupFallback(local) || strings.TrimSpace(local.GroupName) == "" {
			continue
		}
		if previous, exists := unique[local.UpstreamKeyID]; exists && previous.ID != local.ID {
			delete(unique, local.UpstreamKeyID)
			ambiguous[local.UpstreamKeyID] = struct{}{}
			continue
		}
		if _, conflict := ambiguous[local.UpstreamKeyID]; !conflict {
			unique[local.UpstreamKeyID] = local
		}
	}
	return unique
}

func trustedManagedLocalGroupFallback(local storage.GatewayUpstreamKey) bool {
	if !isStronglyManagedUpstreamKeyName(local.UpstreamKeyName) {
		return false
	}
	switch strings.TrimSpace(local.OwnershipProvenance) {
	case storage.GatewayUpstreamKeyProvenanceCreated:
		return true
	case storage.GatewayUpstreamKeyProvenanceProvisioningClaim:
		return local.Provisioning && local.ProvisioningStartedAt != nil
	default:
		return false
	}
}

func deletedSub2APIManagedKeyInventoryGroup(item connector.APIKey) (groupName, inventoryKey string, ok bool) {
	if item.ID <= 0 || !isStronglyManagedUpstreamKeyName(item.Name) || item.Source != connector.APIKeySourceSub2API ||
		item.GroupID == nil || *item.GroupID <= 0 || !item.GroupDirectoryComplete ||
		strings.TrimSpace(item.GroupName) != "" || strings.TrimSpace(item.Group) != "" {
		return "", "", false
	}
	groupName = fmt.Sprintf("%sdeleted_sub2api_group_id__:%d", storage.ReservedInternalUpstreamGroupPrefix, *item.GroupID)
	// Keep the in-memory inventory key outside the provider-name domain too.
	inventoryKey = "\x00" + groupName
	return groupName, inventoryKey, true
}

func groupNameMatchesAny(groupName string, groups map[string]struct{}) bool {
	groupKey := canonicalGroupName(groupName)
	for candidate := range groups {
		if groupKey == canonicalGroupName(candidate) {
			return true
		}
	}
	return false
}

func canonicalGroupName(groupName string) string {
	return strings.ToLower(strings.TrimSpace(groupName))
}

func canonicalRateSnapshots(rates []storage.RateSnapshot) []storage.RateSnapshot {
	selected := make([]storage.RateSnapshot, 0, len(rates))
	indexByGroup := make(map[candidateLatencyKey]int, len(rates))
	for _, rate := range rates {
		groupKey := canonicalCandidateKey(rate.ChannelID, rate.ModelName)
		if groupKey.ChannelID == 0 || groupKey.GroupName == "" {
			continue
		}
		if index, exists := indexByGroup[groupKey]; exists {
			current := selected[index]
			if rate.LastSeenAt.After(current.LastSeenAt) || (rate.LastSeenAt.Equal(current.LastSeenAt) && rate.ID > current.ID) {
				selected[index] = rate
			}
			continue
		}
		indexByGroup[groupKey] = len(selected)
		selected = append(selected, rate)
	}
	return selected
}

func (s *Service) forgetChannelUpstreamKeyCaches(channelID uint) {
	if s == nil || channelID == 0 {
		return
	}
	s.touchMu.Lock()
	for key := range s.upstreamTouchAt {
		if key.channelID == channelID {
			delete(s.upstreamTouchAt, key)
		}
	}
	for key := range s.upstreamIdentityRepairAt {
		if key.channelID == channelID {
			delete(s.upstreamIdentityRepairAt, key)
		}
	}
	s.touchMu.Unlock()
}

func (s *Service) removeStaleUpstreamGroup(ctx context.Context, channelID uint, groupName string, expectedIdentityRevision uint64, preserveCandidates bool) (int64, string, error) {
	store := s.Store.WithContext(ctx)
	generation, err := store.RetireUpstreamGroupForKeyRemoval(channelID, expectedIdentityRevision, groupName, time.Now(), preserveCandidates)
	if err != nil {
		return 0, "", err
	}
	cached, err := store.FindUpstreamKeyAny(channelID, groupName)
	var managedRemote []connector.APIKey
	if s.Keys != nil {
		var managedErr error
		managedRemote, managedErr = s.findManagedUpstreamKeysForGroup(ctx, channelID, expectedIdentityRevision, groupName)
		if managedErr != nil {
			// An unavailable inventory provides no remote identity to review.
			// Preserve the provider error so the shared maintenance backoff can
			// retry safely.
			return 0, "", managedErr
		}
	}
	if len(managedRemote) > 0 {
		primary := managedRemote[0]
		if err == nil && cached != nil && cached.UpstreamKeyID > 0 {
			for _, item := range managedRemote {
				if item.ID == cached.UpstreamKeyID {
					primary = item
					break
				}
			}
		}
		if err == nil && cached != nil && !isStronglyManagedUpstreamKeyName(cached.UpstreamKeyName) {
			// A separate manual local key must never be purged just because the
			// same group also has a managed remote key. Remove only the proven
			// managed identities and leave the manual record for explicit review.
			for _, item := range managedRemote {
				deleted, err := s.deleteManagedRemoteKeyForSync(ctx, channelID, expectedIdentityRevision, item, generation, false, groupName)
				if err != nil {
					return 0, "", fmt.Errorf("remove managed upstream key %d: %w", item.ID, err)
				}
				if !deleted {
					return primary.ID, primary.Name, nil
				}
			}
			return primary.ID, primary.Name, nil
		}
		if err == nil && cached != nil {
			// Rebind the mutable remote identity before entering the durable
			// removal intent. The strong provisioning prefix remains the ownership
			// proof even when the upstream changed the ID or name recorded locally.
			cached.UpstreamKeyID = primary.ID
			cached.UpstreamKeyName = primary.Name
			if cached.OwnershipProvenance == storage.GatewayUpstreamKeyProvenanceAdopted || cached.OwnershipProvenance == storage.GatewayUpstreamKeyProvenanceUnknown || strings.TrimSpace(cached.OwnershipProvenance) == "" {
				cached.OwnershipProvenance = storage.GatewayUpstreamKeyProvenanceCreated
			}
			if err := store.UpsertUpstreamKey(cached); err != nil {
				return 0, "", fmt.Errorf("refresh local managed upstream key identity: %w", err)
			}
			removed, removeErr := s.removeManagedKeyForAbsentGeneration(ctx, generation, groupName, cached, primary, expectedIdentityRevision, preserveCandidates)
			if removeErr != nil {
				return 0, "", fmt.Errorf("remove managed upstream key %d: %w", primary.ID, removeErr)
			}
			if !removed {
				return 0, "", nil
			}
			for _, item := range managedRemote {
				if item.ID == primary.ID {
					continue
				}
				deleted, err := s.deleteManagedRemoteKeyForSync(ctx, channelID, expectedIdentityRevision, item, generation, false, groupName)
				if err != nil {
					return 0, "", fmt.Errorf("remove duplicate managed upstream key %d: %w", item.ID, err)
				}
				if !deleted {
					return 0, "", nil
				}
			}
			return primary.ID, primary.Name, nil
		}
		for _, item := range managedRemote {
			deleted, err := s.deleteManagedRemoteKeyForSync(ctx, channelID, expectedIdentityRevision, item, generation, false, groupName)
			if err != nil {
				return 0, "", fmt.Errorf("remove managed upstream key %d: %w", item.ID, err)
			}
			if !deleted {
				return primary.ID, primary.Name, nil
			}
		}
		keyID, keyName := primary.ID, primary.Name
		if err := store.FinalizeAbsentUpstreamGroupState(generation, expectedIdentityRevision, groupName, keyID, keyName, time.Now()); err != nil {
			return keyID, keyName, err
		}
		return keyID, keyName, nil
	}
	var keyID int64
	var keyName string
	if err == nil {
		keyID, keyName = cached.UpstreamKeyID, cached.UpstreamKeyName
	}
	if err == nil {
		if s.Keys == nil {
			if keyID > 0 || strings.TrimSpace(keyName) != "" {
				if reviewErr := store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, 0, groupName, keyID, keyName, 0, preserveCandidates, time.Now()); reviewErr != nil {
					return 0, "", reviewErr
				}
				return 0, "", nil
			}
		} else {
			remote, ambiguous, findErr := s.findRemovedGroupRemoteKey(ctx, channelID, expectedIdentityRevision, cached)
			if findErr != nil || ambiguous {
				if reviewErr := store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, 0, groupName, keyID, keyName, 0, preserveCandidates, time.Now()); reviewErr != nil {
					return 0, "", reviewErr
				}
				return 0, "", nil
			}
			if remote != nil {
				removed, removeErr := s.removeManagedKeyForAbsentGeneration(ctx, generation, groupName, cached, *remote, expectedIdentityRevision, preserveCandidates)
				if removeErr != nil {
					return 0, "", fmt.Errorf("remove managed upstream key %d: %w", remote.ID, removeErr)
				}
				if !removed {
					return 0, "", nil
				}
				keyID, keyName = remote.ID, remote.Name
				return keyID, keyName, nil
			}
			if keyID > 0 {
				ownershipClaim, claimErr := managedUpstreamKeyRemovalOwnershipClaim(*cached)
				if claimErr != nil {
					if reviewErr := store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, 0, groupName, keyID, keyName, 0, preserveCandidates, time.Now()); reviewErr != nil {
						return 0, "", reviewErr
					}
					return 0, "", nil
				}
				if acknowledgeErr := store.AcknowledgeGenerationSafeManagedUpstreamKeyRemoval(generation, expectedIdentityRevision, keyID, keyName, cached.ID, ownershipClaim, time.Now()); acknowledgeErr != nil {
					if errors.Is(acknowledgeErr, storage.ErrUpstreamGroupGenerationChanged) || errors.Is(acknowledgeErr, storage.ErrManagedUpstreamKeyRemovalIdentityConflict) || errors.Is(acknowledgeErr, storage.ErrManagedUpstreamKeyRemovalIntentChanged) || errors.Is(acknowledgeErr, storage.ErrStaleChannelAccountIdentityRevision) {
						if reviewErr := store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, 0, groupName, keyID, keyName, 0, preserveCandidates, time.Now()); reviewErr != nil && !errors.Is(reviewErr, storage.ErrStaleChannelAccountIdentityRevision) {
							return 0, "", reviewErr
						}
						return 0, "", nil
					}
					return 0, "", fmt.Errorf("acknowledge absent managed upstream key %d: %w", keyID, acknowledgeErr)
				}
				return keyID, keyName, nil
			}
		}
	} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, "", err
	}
	if err := store.FinalizeAbsentUpstreamGroupState(generation, expectedIdentityRevision, groupName, keyID, keyName, time.Now()); err != nil {
		return keyID, keyName, err
	}
	return keyID, keyName, nil
}

func (s *Service) DeleteGroupRoutingState(ctx context.Context, channelID uint, groupName string) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway store is unavailable")
	}
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return err
	}
	defer unlock()
	channel, err := s.Channels.WithContext(ctx).FindByID(channelID)
	if err != nil {
		return err
	}
	_, _, deferred, err := s.removeStaleUpstreamGroupWithBackoff(ctx, channelID, groupName, channel.UpstreamAccountIdentityRevision, false)
	if err != nil {
		return err
	}
	if deferred {
		state, stateErr := s.managedKeyMaintenanceDeferredContext(ctx, channelID, channel.UpstreamAccountIdentityRevision, time.Now().UTC())
		if stateErr != nil {
			return stateErr
		}
		if state != nil {
			return state
		}
		return &ManagedKeyMaintenanceDeferredError{}
	}
	return nil
}

// RetireManagedChannelRouting removes provider-side BridgeOps keys while the
// old management credential is still available. Every owned remote key is
// mapped before the first mutation so a partial or ambiguous inventory cannot
// silently strand a key when the channel switches to direct credentials.
func (s *Service) RetireManagedChannelRouting(ctx context.Context, channelID uint) error {
	return s.RetireManagedChannelRoutingAndThen(ctx, channelID, nil, nil)
}

// RetireManagedChannelRoutingAndThen keeps model synchronization, remote-key
// retirement, validation, and the local mode update inside one channel lock.
func (s *Service) RetireManagedChannelRoutingAndThen(
	ctx context.Context,
	channelID uint,
	validate func(context.Context) error,
	update func(context.Context) error,
) error {
	if s == nil || s.Store == nil || s.Channels == nil || s.Rates == nil || s.Keys == nil {
		return errors.New("managed gateway key cleanup is unavailable")
	}
	if channelID == 0 {
		return errors.New("channel id is required")
	}
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return err
	}
	defer unlock()
	if validate != nil {
		if err := validate(ctx); err != nil {
			return err
		}
	}
	channel, err := s.Channels.WithContext(ctx).FindByID(channelID)
	if err != nil {
		return err
	}
	if channel.CredentialMode == storage.CredentialModeDirect {
		return errors.New("channel is already direct")
	}
	revision := channel.UpstreamAccountIdentityRevision
	if revision == 0 {
		return errors.New("channel account identity revision is required")
	}
	localKeys, err := s.Store.WithContext(ctx).ListUpstreamKeys([]uint{channelID})
	if err != nil {
		return fmt.Errorf("list local upstream keys: %w", err)
	}
	deletedRemoteIDs := make(map[int64]struct{})
	ctx = withUpstreamInventoryPassCache(ctx, channelID, revision, deletedRemoteIDs)
	remoteKeys, err := s.listAllUpstreamKeysAllowingDuplicateNames(ctx, channelID, revision)
	if err != nil {
		return fmt.Errorf("list remote upstream keys: %w", err)
	}
	localByRemoteID := uniqueManagedLocalKeysByRemoteID(localKeys)
	groupNames := make(map[string]string)
	activeGroups := make(map[string]struct{})
	remoteByGroup := make(map[string][]connector.APIKey)
	addGroup := func(groupName string) {
		groupName = strings.TrimSpace(groupName)
		if groupName != "" && !strings.EqualFold(groupName, storage.DirectChannelGroupName) {
			groupNames[canonicalGroupName(groupName)] = groupName
		}
	}
	for _, key := range localKeys {
		addGroup(key.GroupName)
	}
	rates, err := s.Rates.WithContext(ctx).ListByChannel(channelID)
	if err != nil {
		return fmt.Errorf("list channel rate groups: %w", err)
	}
	for _, rate := range rates {
		addGroup(rate.ModelName)
		if groupKey := canonicalGroupName(rate.ModelName); groupKey != "" {
			activeGroups[groupKey] = struct{}{}
		}
	}
	routingGroups, err := s.Store.WithContext(ctx).ListRoutingGroupNames(channelID)
	if err != nil {
		return fmt.Errorf("list routing groups: %w", err)
	}
	for _, groupName := range routingGroups {
		addGroup(groupName)
	}
	for _, key := range remoteKeys {
		if key.ID <= 0 || !isManagedUpstreamKeyName(key.Name) {
			continue
		}
		if !isStronglyManagedUpstreamKeyName(key.Name) {
			return fmt.Errorf("managed upstream key %q requires manual review before switching to direct mode", strings.TrimSpace(key.Name))
		}
		groupName := strings.TrimSpace(managedUpstreamKeyGroupName(key, localByRemoteID))
		if groupName == "" {
			return fmt.Errorf("managed upstream key %q cannot be safely mapped to one group", strings.TrimSpace(key.Name))
		}
		addGroup(groupName)
		groupKey := canonicalGroupName(groupName)
		remoteByGroup[groupKey] = append(remoteByGroup[groupKey], key)
	}
	orderedGroups := make([]string, 0, len(groupNames))
	for _, groupName := range groupNames {
		orderedGroups = append(orderedGroups, groupName)
	}
	sort.Slice(orderedGroups, func(i, j int) bool {
		return canonicalGroupName(orderedGroups[i]) < canonicalGroupName(orderedGroups[j])
	})
	for _, groupName := range orderedGroups {
		groupKey := canonicalGroupName(groupName)
		if _, active := activeGroups[groupKey]; active {
			generation, captureErr := s.Store.WithContext(ctx).CaptureUpstreamGroupGeneration(channelID, groupName)
			if captureErr != nil {
				return fmt.Errorf("capture managed routing group %q: %w", groupName, captureErr)
			}
			for _, remote := range remoteByGroup[groupKey] {
				deleted, deleteErr := s.deleteManagedRemoteKeyForSync(ctx, channelID, revision, remote, generation, true, groupName)
				if deleteErr != nil {
					return fmt.Errorf("retire managed routing group %q key %d: %w", groupName, remote.ID, deleteErr)
				}
				if !deleted {
					return fmt.Errorf("managed upstream key %q requires manual review before switching to direct mode", strings.TrimSpace(remote.Name))
				}
			}
			continue
		}
		if _, _, removeErr := s.removeStaleUpstreamGroup(ctx, channelID, groupName, revision, true); removeErr != nil {
			return fmt.Errorf("retire managed routing group %q: %w", groupName, removeErr)
		}
	}
	remaining, err := s.listAllUpstreamKeysAllowingDuplicateNames(ctx, channelID, revision)
	if err != nil {
		return fmt.Errorf("verify retired upstream keys: %w", err)
	}
	for _, key := range remaining {
		if key.ID > 0 && isManagedUpstreamKeyName(key.Name) {
			return fmt.Errorf("managed upstream key %q requires manual review before switching to direct mode", strings.TrimSpace(key.Name))
		}
	}
	if err := s.Channels.WithContext(ctx).RequireUpstreamAccountIdentityRevision(channelID, revision); err != nil {
		return err
	}
	for _, groupName := range orderedGroups {
		if err := s.Store.WithContext(ctx).DeleteGroupUpstreamState(channelID, groupName, true); err != nil {
			return fmt.Errorf("clear retired local routing group %q: %w", groupName, err)
		}
	}
	s.forgetChannelUpstreamKeyCaches(channelID)
	if update != nil {
		if updateErr := update(ctx); updateErr != nil {
			// The provider mutations cannot be rolled back. If the local mode
			// update failed, rebuild the still-managed channel before releasing
			// the operation lock so it does not remain without usable keys.
			recoveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			recoveryCtx = storage.WithChannelOperationLockHeld(recoveryCtx, channelID)
			current, findErr := s.Channels.WithContext(recoveryCtx).FindByID(channelID)
			if findErr != nil {
				return errors.Join(updateErr, fmt.Errorf("reload managed channel for routing recovery: %w", findErr))
			}
			if current.CredentialMode != storage.CredentialModeDirect {
				if _, restoreErr := s.syncChannelModelFamiliesLocked(recoveryCtx, current, true, true); restoreErr != nil {
					return errors.Join(updateErr, fmt.Errorf("restore managed routing after failed direct-mode update: %w", restoreErr))
				}
			}
			return updateErr
		}
	}
	return nil
}

// InvalidateDirectRoutingState removes the local direct secret before a
// credential replacement or mode change, while retaining candidate-group
// membership configured by administrators.
func (s *Service) InvalidateDirectRoutingState(ctx context.Context, channelID uint) error {
	return s.InvalidateDirectRoutingStateAndThen(ctx, channelID, false, nil)
}

// UpdateDirectRoutingConfigurationAndThen serializes a direct-channel policy
// edit with model synchronization while preserving durable authentication
// invalidation evidence for the unchanged API key identity.
func (s *Service) UpdateDirectRoutingConfigurationAndThen(
	ctx context.Context,
	channelID uint,
	update func(context.Context) error,
) error {
	if update == nil {
		return errors.New("direct routing configuration update is unavailable")
	}
	return s.WithChannelOperationLockAndThen(ctx, channelID, update)
}

// WithChannelOperationLockAndThen runs update while holding the shared channel
// operation lock. Callers composing several channel operations may pass the
// marked context through without re-entering the non-reentrant lock.
func (s *Service) WithChannelOperationLockAndThen(
	ctx context.Context,
	channelID uint,
	update func(context.Context) error,
) error {
	if s == nil || s.Store == nil || channelID == 0 || update == nil {
		return errors.New("channel operation is unavailable")
	}
	if storage.ChannelOperationLockHeld(ctx, channelID) {
		return update(ctx)
	}
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return err
	}
	defer unlock()
	return update(ctx)
}

// InvalidateDirectRoutingStateAndThen keeps the channel mutation and subsequent
// direct routing cleanup under the same cross-process channel lock.
func (s *Service) InvalidateDirectRoutingStateAndThen(
	ctx context.Context,
	channelID uint,
	removeRate bool,
	update func(context.Context) error,
) error {
	if s == nil || s.Store == nil || channelID == 0 {
		return errors.New("gateway store is unavailable")
	}
	return s.WithChannelOperationLockAndThen(ctx, channelID, func(lockedCtx context.Context) error {
		if update != nil {
			if updateErr := update(lockedCtx); updateErr != nil {
				return updateErr
			}
		}
		groups, err := s.Store.WithContext(lockedCtx).ListRoutingGroupNames(channelID)
		if err != nil {
			return err
		}
		groupsByCanonical := make(map[string]string, len(groups))
		for _, groupName := range groups {
			if canonical := canonicalGroupName(groupName); canonical != "" {
				groupsByCanonical[canonical] = strings.TrimSpace(groupName)
			}
		}
		if removeRate {
			if s.Rates == nil {
				return errors.New("direct rate cleanup is unavailable")
			}
			rates, rateErr := s.Rates.WithContext(lockedCtx).ListByChannel(channelID)
			if rateErr != nil {
				return rateErr
			}
			for _, rate := range rates {
				if canonical := canonicalGroupName(rate.ModelName); canonical != "" {
					groupsByCanonical[canonical] = strings.TrimSpace(rate.ModelName)
				}
			}
		}
		orderedGroups := make([]string, 0, len(groupsByCanonical))
		for _, groupName := range groupsByCanonical {
			orderedGroups = append(orderedGroups, groupName)
		}
		sort.Slice(orderedGroups, func(i, j int) bool {
			return canonicalGroupName(orderedGroups[i]) < canonicalGroupName(orderedGroups[j])
		})
		for _, groupName := range orderedGroups {
			if err := s.Store.WithContext(lockedCtx).DeleteGroupUpstreamState(channelID, groupName, true); err != nil {
				return err
			}
			if removeRate {
				if err := s.Rates.WithContext(lockedCtx).DeleteSnapshot(channelID, groupName); err != nil {
					return err
				}
			}
		}
		s.forgetChannelUpstreamKeyCaches(channelID)
		return nil
	})
}

func (s *Service) lockModelSyncChannel(ctx context.Context, channelID uint) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.modelSyncMu.Lock()
	lock := s.modelSyncLocks[channelID]
	if lock == nil {
		lock = make(chan struct{}, 1)
		lock <- struct{}{}
		s.modelSyncLocks[channelID] = lock
	}
	s.modelSyncMu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lock:
		if s.Store == nil {
			return func() { lock <- struct{}{} }, nil
		}
		databaseUnlock, err := s.Store.LockChannelOperation(ctx, channelID)
		if err != nil {
			lock <- struct{}{}
			return nil, fmt.Errorf("lock gateway channel operation: %w", err)
		}
		return func() {
			databaseUnlock()
			lock <- struct{}{}
		}, nil
	}
}

func (s *Service) lockModelSyncChannelContext(ctx context.Context, channelID uint) (context.Context, func(), error) {
	unlock, err := s.lockModelSyncChannel(ctx, channelID)
	if err != nil {
		return ctx, nil, err
	}
	if s.Store == nil {
		return ctx, unlock, nil
	}
	return storage.WithChannelOperationLockHeld(ctx, channelID), unlock, nil
}

func (s *Service) resolveGatewayProbeTarget(ctx context.Context, channel *storage.Channel) (gatewayProbeTarget, error) {
	target := gatewayProbeTarget{SiteURL: channel.SiteURL}
	resolver, ok := s.Keys.(interface {
		Resolve(context.Context, *storage.Channel) (*connector.Channel, error)
	})
	if !ok {
		return target, nil
	}
	resolved, err := resolver.Resolve(ctx, channel)
	if err != nil {
		return target, err
	}
	if strings.TrimSpace(resolved.SiteURL) != "" {
		target.SiteURL = resolved.SiteURL
	}
	target.ProxyURL = strings.TrimSpace(resolved.ProxyURL)
	target.ProxyCacheKey = resolved.ProxyCacheKey
	return target, nil
}

func (s *Service) probeModelFamilies(ctx context.Context, target gatewayProbeTarget, upstreamKey string, candidate Candidate, keyLeases ...UpstreamKeyLease) ([]string, []string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(target.SiteURL, "/")+"/v1/models", nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(upstreamKey))
	req.Header.Set("x-api-key", strings.TrimSpace(upstreamKey))
	client, err := modelProbeClientWithCacheKey(target.ProxyURL, target.ProxyCacheKey, s.modelProbeTimeout())
	if err != nil {
		return nil, nil, err
	}
	if len(keyLeases) > 1 {
		return nil, nil, errors.New("model probe accepts at most one upstream key lease")
	}
	if len(keyLeases) == 1 && s != nil && s.Store != nil && s.Channels != nil {
		dispatchFence, err := UpstreamDispatchFenceForCandidate(candidate, keyLeases[0], candidate.App, "")
		if err != nil {
			return nil, nil, err
		}
		client = clientWithUpstreamDispatchFence(client, s, dispatchFence)
	}
	lease, decision := s.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencyAPITest)
	if !decision.Admitted || lease == nil {
		return nil, nil, errAccountConcurrencyProbeDeferred
	}
	lease.BindContext(ctx)
	resp, err := netproxy.DoWithoutRedirects(client, req)
	lease.BindHTTPResponse(resp, err)
	if err != nil {
		return nil, nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, gatewayProbeResponseMaxBytes+1))
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, nil, readErr
	}
	if len(raw) > gatewayProbeResponseMaxBytes {
		return nil, nil, errors.New("models response exceeded 1 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		text := strings.ReplaceAll(strings.TrimSpace(string(raw)), strings.TrimSpace(upstreamKey), "[redacted]")
		if IsExplicitAccountConcurrencyPressure(resp.StatusCode, text) {
			lease.ObservePressure()
		}
		return nil, nil, fmt.Errorf("models probe: %w", connector.HTTPStatusError(resp.StatusCode, []byte(text)))
	}
	var parsed struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, nil, err
	}
	lease.ObserveUsefulSuccess()
	return modelFamiliesFromItems(parsed.Data), modelIDsFromItems(parsed.Data), nil
}

func (s *Service) modelProbeTimeout() time.Duration {
	timeout := s.routingConfig().FailurePolicy.ResponseTimeout
	if timeout <= 0 {
		return defaultRoutingResponseTimeout
	}
	return timeout
}

func modelProbeClient(rawProxyURL string, timeout time.Duration) (*http.Client, error) {
	return modelProbeClientWithCacheKey(rawProxyURL, "", timeout)
}

func modelProbeClientWithCacheKey(rawProxyURL, profileCacheKey string, timeout time.Duration) (*http.Client, error) {
	rawProxyURL = strings.TrimSpace(rawProxyURL)
	if timeout <= 0 {
		timeout = defaultRoutingResponseTimeout
	}
	cacheKey := strings.TrimSpace(profileCacheKey)
	if cacheKey == "" {
		cacheKey = netproxy.CacheKey(rawProxyURL)
	}
	cacheKey += "\x00" + timeout.String()
	modelProbeClients.Lock()
	if cached := modelProbeClients.values[cacheKey]; cached != nil {
		modelProbeClients.Unlock()
		return cached, nil
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		modelProbeClients.Unlock()
		return nil, errors.New("default transport is not http transport")
	}
	transport := base.Clone()
	transport.TLSHandshakeTimeout = timeout
	client, err := netproxy.NewClient(&http.Client{Transport: transport, Timeout: timeout}, rawProxyURL)
	if err != nil {
		modelProbeClients.Unlock()
		return nil, err
	}
	if len(modelProbeClients.values) < modelProbeClientCacheLimit {
		modelProbeClients.values[cacheKey] = client
	}
	modelProbeClients.Unlock()
	return client, nil
}

func isModelProbeAuthError(err error) bool {
	return connector.IsAuthHTTPError(err) && !connector.IsUpstreamChallengeHTTPError(err) && !connector.IsAccountBalanceHTTPError(err)
}

func modelFamiliesFromItems(items []map[string]any) []string {
	families := make(map[string]struct{})
	for _, item := range items {
		id, _ := item["id"].(string)
		if family := modelFamilyFromName(id); family != "" {
			families[family] = struct{}{}
		}
	}
	out := make([]string, 0, len(families))
	for _, family := range modelFamilyOrder {
		if _, ok := families[family]; ok {
			out = append(out, family)
		}
	}
	return out
}

func modelIDsFromItems(items []map[string]any) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		id, _ := item["id"].(string)
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func modelFamilyFromName(name string) string {
	text := strings.ToLower(strings.TrimSpace(name))
	if text == "" {
		return ""
	}
	if strings.Contains(text, "grok") || strings.Contains(text, "xai") {
		return "grok"
	}
	if strings.Contains(text, "deepseek") {
		return "deepseek"
	}
	for _, family := range []string{"qwen", "mistral", "llama", "glm", "minimax", "moonshot", "kimi", "yi-", "ernie"} {
		if strings.Contains(text, family) {
			return strings.TrimSuffix(family, "-")
		}
	}
	if strings.Contains(text, "claude") || strings.Contains(text, "anthropic") || strings.Contains(text, "sonnet") || strings.Contains(text, "opus") || strings.Contains(text, "haiku") || strings.Contains(text, "kiro") || strings.Contains(text, "krio") {
		return string(storage.GatewayAppClaude)
	}
	if strings.Contains(text, "gemini") || strings.Contains(text, "palm") {
		return string(storage.GatewayAppGemini)
	}
	if strings.Contains(text, "gpt") || strings.Contains(text, "chatgpt") || strings.Contains(text, "codex") || strings.Contains(text, "dall-e") || strings.Contains(text, "dalle") || strings.Contains(text, "text-embedding") || containsModelToken(text, []string{"o1", "o3", "o4"}) {
		return string(storage.GatewayAppCodex)
	}
	return "other"
}

var modelFamilyOrder = []string{
	string(storage.GatewayAppCodex),
	"grok",
	"deepseek",
	string(storage.GatewayAppClaude),
	string(storage.GatewayAppGemini),
	"qwen",
	"mistral",
	"llama",
	"glm",
	"minimax",
	"moonshot",
	"kimi",
	"yi",
	"ernie",
	"other",
}

func familySupportsGatewayApp(family string, app storage.GatewayApp) bool {
	family = strings.ToLower(strings.TrimSpace(family))
	app = normalizeGatewayApp(app)
	if family == "" {
		return true
	}
	if family == "other" {
		return app == storage.GatewayAppCodex
	}
	switch app {
	case storage.GatewayAppClaude:
		return family == string(storage.GatewayAppClaude)
	case storage.GatewayAppGemini:
		return family == string(storage.GatewayAppGemini)
	case storage.GatewayAppCodex:
		switch family {
		case string(storage.GatewayAppCodex), "grok", "deepseek", "qwen", "mistral", "llama", "glm", "minimax", "moonshot", "kimi", "yi", "ernie":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func familySupportsGatewayCandidate(family string, app storage.GatewayApp) bool {
	if strings.EqualFold(strings.TrimSpace(family), "other") {
		return normalizeGatewayApp(app) == storage.GatewayAppCodex
	}
	return familySupportsGatewayApp(family, app)
}

func containsModelToken(text string, tokens []string) bool {
	seen := make(map[string]struct{})
	for _, token := range strings.FieldsFunc(text, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9')
	}) {
		seen[token] = struct{}{}
	}
	for _, token := range tokens {
		if _, ok := seen[token]; ok {
			return true
		}
	}
	return false
}

func (s *Service) StartCostReconciler() {
	if s == nil || s.Store == nil {
		return
	}
	s.startRequestDispatchRecoveryWorker()
	if _, ok := s.Keys.(requestCostManager); !ok {
		return
	}
	s.startCostWorker()
	s.wakeCostWorker()
}

func (s *Service) ReconcileRequestCost(log *storage.GatewayRequestLog, startedAt, finishedAt time.Time) {
	if s == nil || s.Store == nil || log == nil || log.ID == 0 || log.ChannelID == 0 || !storage.IsGatewayRequestCostReconcileEligible(log) {
		return
	}
	if requestUsesDirectCostBasis(*log) {
		s.failRequestCostReconcile(log.ID, storage.GatewayRequestCostReconcileReasonPricingUnavailable, "direct channel has no management-plane usage lookup")
		log.CostReconcileAt = nil
		return
	}
	if requestCostCannotBeMatched(*log) {
		s.failRequestCostReconcile(log.ID, storage.GatewayRequestCostReconcileReasonUnmatchable, "upstream usage cannot be matched without a request ID or token metrics")
		log.CostReconcileAt = nil
		return
	}
	if _, ok := s.Keys.(requestCostManager); !ok {
		return
	}
	if log.CreatedAt.IsZero() {
		log.CreatedAt = finishedAt
	}
	if log.CostReconcileAt == nil {
		log.CostReconcileAt = timePtr(InitialRequestCostReconcileAt(finishedAt))
		if err := s.Store.ScheduleRequestCostReconcile(log.ID, *log.CostReconcileAt); err != nil {
			return
		}
	}
	// All lookups enter one due-time ordered dispatcher.  An incoming request
	// must not claim a slot ahead of older persisted work, otherwise sustained
	// traffic can starve the backlog indefinitely.
	s.startCostWorker()
	s.wakeCostWorker()
}

func (s *Service) startCostWorker() {
	s.costWorkerOnce.Do(func() {
		if s.costWorkerWake == nil {
			s.costWorkerWake = make(chan struct{}, 1)
		}
		if s.costLookupSlots == nil {
			s.costLookupSlots = make(chan struct{}, 4)
		}
		if s.costIdentityLimiter == nil {
			s.costIdentityLimiter = newCostReconcileIdentityLimiter()
		}
		go func() {
			ticker := time.NewTicker(costReconcilePollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
				case <-s.costWorkerWake:
				}
				s.dispatchPendingCosts()
			}
		}()
	})
}

func (s *Service) wakeCostWorker() {
	if s == nil || s.costWorkerWake == nil {
		return
	}
	select {
	case s.costWorkerWake <- struct{}{}:
	default:
	}
}

func (s *Service) dispatchPendingCosts() {
	if s == nil || s.Store == nil || s.costLookupSlots == nil {
		return
	}
	available := cap(s.costLookupSlots) - len(s.costLookupSlots)
	if available <= 0 {
		return
	}
	// Repair a small bounded tranche left by older writers. Cleanup is throttled
	// independently from dispatch because its anti-eligibility predicate can
	// otherwise rescan a large historical prefix on every worker tick.
	s.maybeClearIneligibleRequestCostReconciles()

	busy := s.costIdentityLimiter.snapshot()
	pending, err := s.Store.ListFairDueRequestCostReconciles(available + len(busy))
	if err != nil {
		return
	}
	pending = selectFairRequestCostReconciles(pending, available, busy)
	for _, item := range pending {
		identity := requestCostReconcileIdentity(item)
		if !s.costIdentityLimiter.tryAcquire(identity) {
			continue
		}
		claim, claimed, err := s.Store.ClaimRequestCostReconcileDurable(item.ID, costReconcileLease)
		if err != nil || !claimed || claim == nil {
			s.costIdentityLimiter.release(identity)
			continue
		}
		select {
		case s.costLookupSlots <- struct{}{}:
			go func(claim storage.GatewayRequestCostReconcileClaim, identity costReconcileIdentity) {
				defer func() {
					<-s.costLookupSlots
					s.costIdentityLimiter.release(identity)
					s.wakeCostWorker()
				}()
				log := claim.Log
				s.reconcileDurablePendingCost(claim, log.CreatedAt.Add(-time.Duration(log.LatencyMs)*time.Millisecond), log.CreatedAt)
			}(*claim, identity)
		default:
			// The available count is only a snapshot. If another dispatcher won
			// the last slot, return this claim to the due queue promptly.
			_ = s.Store.ReleaseRequestCostReconcileDurableClaim(claim.Fence, storage.GatewayRequestCostReconcileReasonDispatcherSlotUnavailable)
			s.costIdentityLimiter.release(identity)
		}
	}
}

func (s *Service) maybeClearIneligibleRequestCostReconciles() {
	if s == nil || s.Store == nil {
		return
	}
	now := time.Now()
	s.costCleanupMu.Lock()
	if !s.costCleanupAt.IsZero() && now.Before(s.costCleanupAt.Add(costReconcileCleanupInterval)) {
		s.costCleanupMu.Unlock()
		return
	}
	s.costCleanupAt = now
	s.costCleanupMu.Unlock()
	_, _ = s.Store.ClearIneligibleRequestCostReconciles(costReconcileCleanupBatch)
	_, _ = s.Store.RequeueHistoricalRequestCostReconciles(historicalCostReconcileBackfillBatch, now.Add(-maxCostReconcileAge), now.Add(-24*time.Hour))
}

type requestCostReconcileWork struct {
	log   storage.GatewayRequestLog
	fence *storage.GatewayRequestCostReconcileFence
}

func (s *Service) reconcilePendingCost(log storage.GatewayRequestLog, startedAt, finishedAt time.Time) {
	s.reconcilePendingCostWork(requestCostReconcileWork{log: log}, startedAt, finishedAt)
}

func (s *Service) reconcileDurablePendingCost(claim storage.GatewayRequestCostReconcileClaim, startedAt, finishedAt time.Time) {
	fence := claim.Fence
	s.reconcilePendingCostWork(requestCostReconcileWork{log: claim.Log, fence: &fence}, startedAt, finishedAt)
}

func (s *Service) reconcilePendingCostWork(work requestCostReconcileWork, startedAt, finishedAt time.Time) {
	log := work.log
	if log.RouteReason == "model_check" || (log.RouteReason == latencyProbeRouteReason && storage.IsGatewayWaitingProbeDetails(log.RouteDetails)) {
		current, err := s.internalProbeCostIdentityCurrent(log)
		if err != nil {
			s.retryRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonLookupFailed, "internal probe cost identity lookup failed", time.Now(), costReconcileRetryDelay(log.CostReconcileTries))
			return
		}
		if !current {
			s.failClaimedRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonPricingUnavailable, "internal probe cost identity or funding basis is unavailable")
			return
		}
	}
	if requestUsesDirectCostBasis(log) {
		s.failClaimedRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonPricingUnavailable, "direct channel has no management-plane usage lookup")
		return
	}
	if requestCostCannotBeMatched(log) {
		s.failClaimedRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonUnmatchable, "upstream usage cannot be matched without a request ID or token metrics")
		return
	}
	now := time.Now()
	if terminalReason := requestCostReconcileTerminalReason(log, now, false); terminalReason != "" {
		message := "request cost reconciliation retry limit reached"
		if terminalReason == storage.GatewayRequestCostReconcileReasonExpired {
			message = fmt.Sprintf("request cost reconciliation expired after %d days", int(maxCostReconcileAge/(24*time.Hour)))
		}
		s.failClaimedRequestCostReconcile(work, terminalReason, message)
		return
	}
	manager, ok := s.Keys.(requestCostManager)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	upstreamKeyID := log.UpstreamKeyID
	upstreamKeyName := strings.TrimSpace(log.UpstreamKeyName)
	if (upstreamKeyID <= 0 || upstreamKeyName == "") && s.Store != nil {
		if cached, err := s.Store.FindUpstreamKey(log.ChannelID, log.GroupName); err == nil {
			if cached.UpstreamKeyID <= 0 && s.Cipher != nil {
				if plain, decErr := s.Cipher.Decrypt(cached.APIKeyCipher); decErr == nil {
					if identity, repairErr := s.findCachedUpstreamIdentity(ctx, log.ChannelID, log.GroupName, nil, plain); repairErr == nil {
						if saveErr := s.saveUpstreamKey(log.ChannelID, plain, log.GroupName, cached.GroupRatio, identity); saveErr == nil {
							cached.UpstreamKeyID = identity.ID
							cached.UpstreamKeyName = strings.TrimSpace(identity.Name)
						}
					}
				}
			}
			if upstreamKeyID <= 0 {
				upstreamKeyID = cached.UpstreamKeyID
			}
			if upstreamKeyName == "" {
				upstreamKeyName = strings.TrimSpace(cached.UpstreamKeyName)
			}
		}
	}
	if upstreamKeyName == "" {
		upstreamKeyName = upstreamKeyNameForGroup(log.GroupName)
	}
	result, err := manager.LookupRequestCost(ctx, log.ChannelID, connector.RequestCostQuery{
		APIKeyID:         upstreamKeyID,
		APIKeyName:       upstreamKeyName,
		GroupName:        log.GroupName,
		ModelName:        log.ModelName,
		PromptTokens:     log.PromptTokens,
		CompletionTokens: log.CompletionTokens,
		HasTokenMetrics:  log.PromptTokens > 0 || log.CompletionTokens > 0 || log.TotalTokens > 0,
		RequestID:        log.UpstreamRequestID,
		StartedAt:        startedAt,
		FinishedAt:       finishedAt,
	})
	if log.RouteReason == "model_check" || (log.RouteReason == latencyProbeRouteReason && storage.IsGatewayWaitingProbeDetails(log.RouteDetails)) {
		current, identityErr := s.internalProbeCostIdentityCurrent(log)
		if identityErr != nil {
			s.retryRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonLookupFailed, "internal probe cost identity lookup failed", time.Now(), costReconcileRetryDelay(log.CostReconcileTries))
			return
		}
		if !current {
			s.failClaimedRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonPricingUnavailable, "internal probe cost identity changed during lookup")
			return
		}
	}
	if err != nil {
		reason := fmt.Sprintf("upstream usage lookup failed: %v", err)
		if reasonCode, ok := deterministicCostReconcileFailureReason(reason); ok && log.CostReconcileTries+1 >= maxDeterministicCostReconcileAttempts {
			s.failClaimedRequestCostReconcile(work, reasonCode, reason)
			return
		}
		reasonCode := storage.GatewayRequestCostReconcileReasonLookupFailed
		if deterministicReason, ok := deterministicCostReconcileFailureReason(reason); ok {
			reasonCode = deterministicReason
		}
		s.retryRequestCostReconcile(work, reasonCode, reason, time.Now(), costReconcileRetryDelay(log.CostReconcileTries))
		return
	}
	if result == nil {
		s.retryRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonLookupNoResult, "upstream usage lookup returned no result", time.Now(), costReconcileRetryDelay(log.CostReconcileTries))
		return
	}
	if result.Cost < 0 {
		s.retryRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonNegativeCost, "upstream usage lookup returned a negative cost", time.Now(), costReconcileRetryDelay(log.CostReconcileTries))
		return
	}
	if strings.TrimSpace(result.Source) == "" {
		s.retryRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonEmptyCostSource, "upstream usage lookup returned an empty cost source", time.Now(), costReconcileRetryDelay(log.CostReconcileTries))
		return
	}
	rawCost := result.Cost
	fundingFactor := 1.0
	if log.FundingFactor != nil && *log.FundingFactor >= 0 {
		fundingFactor = *log.FundingFactor
	}
	actualCost, conversionErr := costing.Actual(rawCost, fundingFactor)
	if conversionErr != nil {
		s.retryRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonCostConversionFailed, fmt.Sprintf("request cost conversion failed: %v", conversionErr), time.Now(), time.Minute)
		return
	}
	if log.CostReconcileAt == nil {
		return
	}
	usage := storage.GatewayRequestUsageMetrics{
		PromptTokens:         result.PromptTokens,
		CompletionTokens:     result.CompletionTokens,
		TotalTokens:          result.TotalTokens,
		CacheCreationTokens:  result.CacheCreationTokens,
		CacheReadTokens:      result.CacheReadTokens,
		UpstreamModelName:    result.ModelName,
		HasTokenMetrics:      result.HasTokenMetrics,
		HasCacheTokenMetrics: result.HasCacheTokenMetrics,
	}
	var updateErr error
	if work.fence != nil {
		updateErr = s.Store.UpdateClaimedRequestLogActualCostWithUsageDurable(*work.fence, result.UsageID, rawCost, actualCost, result.Source, usage)
	} else {
		updateErr = s.Store.UpdateClaimedRequestLogActualCostWithUsage(log.ID, *log.CostReconcileAt, result.UsageID, rawCost, actualCost, result.Source, usage)
	}
	if updateErr != nil {
		err := updateErr
		if errors.Is(err, storage.ErrRequestCostReconcileLeaseLost) {
			return
		}
		if errors.Is(err, storage.ErrUpstreamUsageAlreadyAssigned) {
			var abandoned bool
			var markErr error
			if work.fence != nil {
				abandoned, markErr = s.Store.MarkClaimedRequestCostReconcileConflictDurable(*work.fence, time.Now().Add(10*time.Second), maxUsageConflictRetries, storage.GatewayRequestCostReconcileReasonUsageConflict)
			} else {
				abandoned, markErr = s.Store.MarkClaimedRequestCostReconcileConflict(log.ID, *log.CostReconcileAt, time.Now().Add(10*time.Second), maxUsageConflictRetries)
			}
			if markErr != nil {
				if errors.Is(markErr, storage.ErrRequestCostReconcileLeaseLost) {
					return
				}
				s.retryRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonConflictUpdateFailed, fmt.Sprintf("request cost conflict update failed: %v", markErr), time.Now(), time.Minute)
				if s.Log != nil {
					s.Log.Warn("gateway actual cost conflict update failed", "requestLogID", log.ID, "err", markErr)
				}
				return
			}
			if abandoned && s.Log != nil {
				s.Log.Warn("gateway actual cost usage already assigned", "requestLogID", log.ID, "usageID", strings.TrimSpace(result.UsageID))
			}
			return
		}
		s.retryRequestCostReconcile(work, storage.GatewayRequestCostReconcileReasonCostUpdateFailed, fmt.Sprintf("request cost update failed: %v", err), time.Now(), time.Minute)
		if s.Log != nil {
			s.Log.Warn("gateway actual cost update failed", "requestLogID", log.ID, "err", err)
		}
	}
}

func deterministicCostReconcileFailureReason(reason string) (string, bool) {
	reason = strings.ToLower(strings.TrimSpace(reason))
	switch {
	case strings.Contains(reason, "duplicate usage id"):
		return storage.GatewayRequestCostReconcileReasonDuplicateUsageID, true
	default:
		return "", false
	}
}

func requestUsesDirectCostBasis(log storage.GatewayRequestLog) bool {
	return strings.EqualFold(strings.TrimSpace(log.CostBasisSource), "direct")
}

func requestCostCannotBeMatched(log storage.GatewayRequestLog) bool {
	return log.UpstreamRequestID == "" && log.PromptTokens == 0 && log.CompletionTokens == 0
}

func requestCostReconcileExpired(log storage.GatewayRequestLog, now time.Time) bool {
	return !log.CreatedAt.IsZero() && !now.Before(log.CreatedAt.Add(maxCostReconcileAge))
}

func requestCostReconcileTerminalReason(log storage.GatewayRequestLog, now time.Time, includeCurrentFailure bool) string {
	if requestCostReconcileExpired(log, now) {
		return storage.GatewayRequestCostReconcileReasonExpired
	}
	tries := log.CostReconcileTries
	if includeCurrentFailure {
		tries++
	}
	if tries >= maxCostReconcileAttempts {
		return storage.GatewayRequestCostReconcileReasonRetryLimit
	}
	return ""
}

func costReconcileRetryDelay(tries int) time.Duration {
	delay := time.Duration(1<<min(tries, 6)) * 5 * time.Second
	if tries >= 12 {
		return time.Hour
	}
	return delay
}

func (s *Service) retryRequestCostReconcile(work requestCostReconcileWork, reasonCode, reason string, now time.Time, delay time.Duration) {
	log := work.log
	reason = sanitizeGatewayServiceError(reason)
	if reason == "" {
		reason = "request cost reconciliation failed"
	}
	if terminalCode := requestCostReconcileTerminalReason(log, now, true); terminalCode != "" {
		s.failClaimedRequestCostReconcile(work, terminalCode, reason)
		return
	}
	retryAt := now.Add(delay)
	if !log.CreatedAt.IsZero() {
		deadline := log.CreatedAt.Add(maxCostReconcileAge)
		if retryAt.After(deadline) {
			retryAt = deadline
		}
	}
	if log.CostReconcileAt == nil {
		return
	}
	var err error
	if work.fence != nil {
		err = s.Store.MarkClaimedRequestCostReconcileRetryDurable(*work.fence, retryAt, reasonCode, reason)
	} else {
		err = s.Store.MarkClaimedRequestCostReconcileRetry(log.ID, *log.CostReconcileAt, retryAt, reason)
	}
	if err != nil && !errors.Is(err, storage.ErrRequestCostReconcileLeaseLost) && s.Log != nil {
		s.Log.Warn("gateway cost reconcile retry update failed", "requestLogID", log.ID, "err", err)
	}
}

func (s *Service) failRequestCostReconcile(id uint, reasonCode, reason string) {
	reason = sanitizeGatewayServiceError(reason)
	if reason == "" {
		reason = "request cost reconciliation failed"
	}
	if err := s.Store.MarkRequestCostReconcileFailedTyped(id, reasonCode, reason); err != nil && s.Log != nil {
		s.Log.Warn("gateway cost reconcile terminal update failed", "requestLogID", id, "reasonCode", reasonCode, "err", err)
	}
}

func (s *Service) failClaimedRequestCostReconcile(work requestCostReconcileWork, reasonCode, reason string) {
	log := work.log
	if log.CostReconcileAt == nil {
		return
	}
	reason = sanitizeGatewayServiceError(reason)
	if reason == "" {
		reason = "request cost reconciliation failed"
	}
	var err error
	if work.fence != nil {
		err = s.Store.MarkClaimedRequestCostReconcileFailedDurable(*work.fence, reasonCode, reason)
	} else {
		err = s.Store.MarkClaimedRequestCostReconcileFailedTyped(log.ID, *log.CostReconcileAt, reasonCode, reason)
	}
	if err != nil && !errors.Is(err, storage.ErrRequestCostReconcileLeaseLost) && s.Log != nil {
		s.Log.Warn("gateway cost reconcile terminal update failed", "requestLogID", log.ID, "err", err)
	}
}

func timePtr(v time.Time) *time.Time { return &v }

func (s *Service) CreateDownstreamKey(name string, app storage.GatewayApp) (*DownstreamKeyResult, error) {
	return s.CreateDownstreamKeyWithFamily(name, app, "")
}

func (s *Service) CreateDownstreamKeyWithFamily(name string, app storage.GatewayApp, modelFamily string) (*DownstreamKeyResult, error) {
	return s.CreateDownstreamKeyWithFamilyAndGroup(name, app, modelFamily, 0)
}

func (s *Service) CreateDownstreamKeyWithFamilyAndGroup(name string, app storage.GatewayApp, modelFamily string, candidateGroupID uint) (*DownstreamKeyResult, error) {
	return s.CreateDownstreamKeyWithFamilyAndGroups(name, app, modelFamily, candidateGroupID, 0)
}

func (s *Service) CreateDownstreamKeyWithFamilyAndGroups(name string, app storage.GatewayApp, modelFamily string, candidateGroupID, keyGroupID uint) (*DownstreamKeyResult, error) {
	targetMode := "all_candidates"
	if candidateGroupID > 0 {
		targetMode = "candidate_group"
	}
	return s.CreateDownstreamKeyWithCandidateTarget(name, app, modelFamily, candidateGroupID, keyGroupID, targetMode)
}

func (s *Service) CreateDownstreamKeyWithCandidateTarget(name string, app storage.GatewayApp, modelFamily string, candidateGroupID, keyGroupID uint, candidateTargetMode string) (*DownstreamKeyResult, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, errors.New("key name is required")
	}
	app = normalizeGatewayApp(app)
	family, err := normalizeGatewayModelFamily(modelFamily, app)
	if err != nil {
		return nil, err
	}
	if err := s.validateCandidateGroup(candidateGroupID); err != nil {
		return nil, err
	}
	if err := s.validateKeyGroup(keyGroupID); err != nil {
		return nil, err
	}
	plain, err := randomKey("sk-briops-")
	if err != nil {
		return nil, err
	}
	cipherText, err := s.Cipher.Encrypt(plain)
	if err != nil {
		return nil, err
	}
	record := storage.GatewayDownstreamKey{
		Name:                 name,
		App:                  app,
		ModelFamily:          family,
		CandidateGroupID:     candidateGroupID,
		KeyGroupID:           keyGroupID,
		ModelMappingStrategy: string(ModelMappingStrategyDisabled),
		KeyHash:              HashKey(plain),
		APIKeyCipher:         cipherText,
		KeyPrefix:            keyPrefix(plain),
		APIKey:               plain,
		SmartRouting:         true,
		Enabled:              true,
	}
	switch strings.ToLower(strings.TrimSpace(candidateTargetMode)) {
	case "candidate_group":
		if candidateGroupID == 0 {
			return nil, errors.New("candidate_group target requires a candidate group")
		}
		record.CandidateTargetMode = "candidate_group"
		record.CandidateTargetSource = "manual"
	case "all_candidates":
		if candidateGroupID != 0 {
			return nil, errors.New("all_candidates target cannot include a candidate group")
		}
		record.CandidateTargetMode = "all_candidates"
		record.CandidateTargetSource = "manual"
	case "key_group_policy":
		if candidateGroupID != 0 {
			return nil, errors.New("key_group_policy target cannot include a candidate group")
		}
		if keyGroupID == 0 {
			return nil, errors.New("key_group_policy target requires a key group")
		}
		applied, err := s.Store.ApplyKeyGroupRoutePolicy(&record)
		if err != nil {
			return nil, err
		}
		if !applied {
			return nil, fmt.Errorf("no default route policy matches key group %d, app %s, model family %s", keyGroupID, app, family)
		}
		if err := s.validateCandidateGroup(record.CandidateGroupID); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("candidate_target_mode must be candidate_group, all_candidates, or key_group_policy")
	}
	if err := s.Store.CreateDownstreamKey(&record); err != nil {
		return nil, err
	}
	return &DownstreamKeyResult{
		Key:    record,
		Plain:  plain,
		Notice: "api key is encrypted at rest and visible to admins",
	}, nil
}

func (s *Service) ListDownstreamKeys() ([]storage.GatewayDownstreamKey, error) {
	keys, err := s.Store.ListDownstreamKeys()
	if err != nil {
		return nil, err
	}
	for i := range keys {
		if strings.TrimSpace(keys[i].ModelFamily) == "" {
			keys[i].ModelFamily = defaultGatewayModelFamily(keys[i].App)
		}
		if strings.TrimSpace(keys[i].ModelMappingStrategy) == "" {
			keys[i].ModelMappingStrategy = string(ModelMappingStrategyDisabled)
		}
		if strings.TrimSpace(keys[i].APIKeyCipher) == "" {
			continue
		}
		plain, err := s.Cipher.Decrypt(keys[i].APIKeyCipher)
		if err != nil {
			return nil, err
		}
		keys[i].APIKey = strings.TrimSpace(plain)
	}
	return keys, nil
}

func (s *Service) UpdateDownstreamKey(id uint, name *string, enabled *bool, app *storage.GatewayApp, smartRouting *bool, manualChannelID *uint, manualGroupName *string) (*storage.GatewayDownstreamKey, error) {
	return s.UpdateDownstreamKeyWithFamily(id, name, enabled, app, nil, smartRouting, manualChannelID, manualGroupName)
}

func (s *Service) UpdateDownstreamKeyWithFamily(id uint, name *string, enabled *bool, app *storage.GatewayApp, modelFamily *string, smartRouting *bool, manualChannelID *uint, manualGroupName *string) (*storage.GatewayDownstreamKey, error) {
	return s.UpdateDownstreamKeyWithFamilyAndGroup(id, name, enabled, app, modelFamily, nil, smartRouting, manualChannelID, manualGroupName)
}

func (s *Service) UpdateDownstreamKeyWithFamilyAndGroup(id uint, name *string, enabled *bool, app *storage.GatewayApp, modelFamily *string, candidateGroupID *uint, smartRouting *bool, manualChannelID *uint, manualGroupName *string) (*storage.GatewayDownstreamKey, error) {
	return s.UpdateDownstreamKeyWithFamilyAndGroups(id, name, enabled, app, modelFamily, candidateGroupID, nil, smartRouting, manualChannelID, manualGroupName)
}

func (s *Service) UpdateDownstreamKeyWithFamilyAndGroups(id uint, name *string, enabled *bool, app *storage.GatewayApp, modelFamily *string, candidateGroupID, keyGroupID *uint, smartRouting *bool, manualChannelID *uint, manualGroupName *string) (*storage.GatewayDownstreamKey, error) {
	return s.UpdateDownstreamKeyWithFamilyGroupsAndMapping(id, name, enabled, app, modelFamily, candidateGroupID, keyGroupID, nil, nil, nil, smartRouting, manualChannelID, manualGroupName)
}

func (s *Service) DisableDownstreamKeyModelMapping(id uint) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway store is unavailable")
	}
	return s.Store.DisableDownstreamKeyModelMapping(id)
}

func (s *Service) UpdateDownstreamKeyWithFamilyGroupsAndMapping(id uint, name *string, enabled *bool, app *storage.GatewayApp, modelFamily *string, candidateGroupID, keyGroupID *uint, modelMappingStrategy, modelMappingModel, modelMappingReasoningEffort *string, smartRouting *bool, manualChannelID *uint, manualGroupName *string) (*storage.GatewayDownstreamKey, error) {
	return s.UpdateDownstreamKeyWithCandidateTargetAndMapping(id, name, enabled, app, modelFamily, candidateGroupID, nil, keyGroupID, modelMappingStrategy, modelMappingModel, modelMappingReasoningEffort, smartRouting, manualChannelID, manualGroupName)
}

func (s *Service) UpdateDownstreamKeyWithCandidateTargetAndMapping(id uint, name *string, enabled *bool, app *storage.GatewayApp, modelFamily *string, candidateGroupID *uint, candidateTargetMode *string, keyGroupID *uint, modelMappingStrategy, modelMappingModel, modelMappingReasoningEffort *string, smartRouting *bool, manualChannelID *uint, manualGroupName *string) (*storage.GatewayDownstreamKey, error) {
	return s.updateDownstreamKeyWithCandidateTargetAndMapping(id, name, enabled, app, modelFamily, candidateGroupID, candidateTargetMode, keyGroupID, modelMappingStrategy, modelMappingModel, modelMappingReasoningEffort, smartRouting, manualChannelID, manualGroupName, nil)
}

func (s *Service) UpdateDownstreamKeyWithCandidateTargetMappingAndAutomaticResolver(id uint, name *string, enabled *bool, app *storage.GatewayApp, modelFamily *string, candidateGroupID *uint, candidateTargetMode *string, keyGroupID *uint, modelMappingStrategy, modelMappingModel, modelMappingReasoningEffort *string, smartRouting *bool, manualChannelID *uint, manualGroupName *string, automaticResolver AutomaticMappedModelResolver) (*storage.GatewayDownstreamKey, error) {
	return s.updateDownstreamKeyWithCandidateTargetAndMapping(id, name, enabled, app, modelFamily, candidateGroupID, candidateTargetMode, keyGroupID, modelMappingStrategy, modelMappingModel, modelMappingReasoningEffort, smartRouting, manualChannelID, manualGroupName, automaticResolver)
}

func (s *Service) updateDownstreamKeyWithCandidateTargetAndMapping(id uint, name *string, enabled *bool, app *storage.GatewayApp, modelFamily *string, candidateGroupID *uint, candidateTargetMode *string, keyGroupID *uint, modelMappingStrategy, modelMappingModel, modelMappingReasoningEffort *string, smartRouting *bool, manualChannelID *uint, manualGroupName *string, automaticResolver AutomaticMappedModelResolver) (*storage.GatewayDownstreamKey, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	type updateChanges struct {
		candidateGroup bool
		keyGroup       bool
		mapping        bool
		mappingContext bool
		route          bool
	}
	var changes updateChanges
	key, err := s.Store.PatchDownstreamKeyWithStore(id, func(transactionStore *storage.Gateway, key *storage.GatewayDownstreamKey) error {
		currentChanges := updateChanges{}
		if name != nil {
			trimmed := strings.TrimSpace(*name)
			if trimmed == "" {
				return errors.New("key name is required")
			}
			key.Name = trimmed
		}
		if enabled != nil {
			key.Enabled = *enabled
		}
		if app != nil {
			normalized := normalizeGatewayApp(*app)
			currentChanges.route = true
			if key.App != normalized {
				currentChanges.mappingContext = true
			}
			key.App = normalized
		}
		if strings.TrimSpace(key.ModelFamily) == "" {
			key.ModelFamily = defaultGatewayModelFamily(key.App)
		}
		if modelFamily != nil {
			family, err := normalizeGatewayModelFamily(*modelFamily, key.App)
			if err != nil {
				return err
			}
			currentChanges.route = true
			if key.ModelFamily != family {
				currentChanges.mappingContext = true
			}
			key.ModelFamily = family
		} else if app != nil {
			family, err := normalizeGatewayModelFamily(key.ModelFamily, key.App)
			if err != nil {
				return err
			}
			if key.ModelFamily != family {
				currentChanges.mappingContext = true
			}
			key.ModelFamily = family
		}
		if candidateGroupID != nil && candidateTargetMode == nil {
			currentChanges.candidateGroup = key.CandidateGroupID != *candidateGroupID
			currentChanges.mappingContext = currentChanges.mappingContext || currentChanges.candidateGroup
			currentChanges.route = currentChanges.route || currentChanges.candidateGroup
			key.CandidateGroupID = *candidateGroupID
		}
		if keyGroupID != nil {
			currentChanges.keyGroup = key.KeyGroupID != *keyGroupID
			key.KeyGroupID = *keyGroupID
		}
		if candidateTargetMode != nil {
			mode := strings.ToLower(strings.TrimSpace(*candidateTargetMode))
			switch mode {
			case "candidate_group":
				if candidateGroupID == nil || *candidateGroupID == 0 {
					return errors.New("candidate_group target requires a candidate group")
				}
				candidateGroupChanged := key.CandidateGroupID != *candidateGroupID
				currentChanges.candidateGroup = currentChanges.candidateGroup || candidateGroupChanged
				currentChanges.mappingContext = currentChanges.mappingContext || candidateGroupChanged
				currentChanges.route = currentChanges.route || candidateGroupChanged
				key.CandidateGroupID = *candidateGroupID
				key.CandidateTargetMode = mode
				key.CandidateTargetSource = "manual"
				key.RoutePolicyID = nil
				key.RoutePolicyVersion = nil
			case "all_candidates":
				if candidateGroupID != nil && *candidateGroupID != 0 {
					return errors.New("all_candidates target cannot include a candidate group")
				}
				candidateGroupChanged := key.CandidateGroupID != 0
				currentChanges.candidateGroup = currentChanges.candidateGroup || candidateGroupChanged
				currentChanges.mappingContext = currentChanges.mappingContext || candidateGroupChanged
				currentChanges.route = currentChanges.route || candidateGroupChanged
				key.CandidateGroupID = 0
				key.CandidateTargetMode = mode
				key.CandidateTargetSource = "manual"
				key.RoutePolicyID = nil
				key.RoutePolicyVersion = nil
			case "key_group_policy":
				if candidateGroupID != nil && *candidateGroupID != 0 {
					return errors.New("key_group_policy target cannot include a candidate group")
				}
				if key.KeyGroupID == 0 {
					return errors.New("key_group_policy target requires a key group")
				}
				previousGroupID := key.CandidateGroupID
				applied, err := transactionStore.ApplyKeyGroupRoutePolicy(key)
				if err != nil {
					return err
				}
				if !applied {
					return fmt.Errorf("no default route policy matches key group %d, app %s, model family %s", key.KeyGroupID, key.App, key.ModelFamily)
				}
				candidateGroupChanged := previousGroupID != key.CandidateGroupID
				currentChanges.candidateGroup = currentChanges.candidateGroup || candidateGroupChanged
				currentChanges.mappingContext = currentChanges.mappingContext || candidateGroupChanged
				currentChanges.route = true
			default:
				return errors.New("candidate_target_mode must be candidate_group, all_candidates, or key_group_policy")
			}
		} else if currentChanges.keyGroup && candidateGroupID == nil && key.CandidateTargetMode == "unset" {
			previousGroupID := key.CandidateGroupID
			applied, err := transactionStore.ApplyKeyGroupRoutePolicy(key)
			if err != nil {
				return err
			}
			if applied {
				candidateGroupChanged := previousGroupID != key.CandidateGroupID
				currentChanges.candidateGroup = currentChanges.candidateGroup || candidateGroupChanged
				currentChanges.mappingContext = currentChanges.mappingContext || candidateGroupChanged
				currentChanges.route = true
			}
		}
		if modelMappingStrategy != nil {
			strategy, err := NormalizeModelMappingStrategy(*modelMappingStrategy)
			if err != nil {
				return err
			}
			normalized := string(strategy)
			currentChanges.mapping = currentChanges.mapping || key.ModelMappingStrategy != normalized
			key.ModelMappingStrategy = normalized
		}
		if modelMappingModel != nil {
			normalized := strings.TrimSpace(*modelMappingModel)
			currentChanges.mapping = currentChanges.mapping || key.ModelMappingModel != normalized
			key.ModelMappingModel = normalized
		}
		if modelMappingReasoningEffort != nil {
			normalized := strings.TrimSpace(*modelMappingReasoningEffort)
			currentChanges.mapping = currentChanges.mapping || key.ModelMappingReasoningEffort != normalized
			key.ModelMappingReasoningEffort = normalized
		}
		if strings.TrimSpace(key.ModelMappingStrategy) == "" {
			key.ModelMappingStrategy = string(ModelMappingStrategyDisabled)
		}
		if smartRouting != nil {
			currentChanges.route = true
			key.SmartRouting = *smartRouting
		}
		if manualChannelID != nil {
			currentChanges.route = true
			key.ManualChannelID = *manualChannelID
		}
		if manualGroupName != nil {
			normalized := strings.TrimSpace(*manualGroupName)
			currentChanges.route = true
			key.ManualGroupName = normalized
		}
		// PatchDownstreamKey applies twice: once to choose lock targets and once
		// to the locked row. Only the second pass controls validation.
		changes = currentChanges
		return nil
	}, func(transactionStore *storage.Gateway, key *storage.GatewayDownstreamKey) error {
		if changes.candidateGroup {
			if err := s.validateCandidateGroupWithStore(transactionStore, key.CandidateGroupID); err != nil {
				return err
			}
		}
		if changes.keyGroup {
			if err := s.validateKeyGroupWithStore(transactionStore, key.KeyGroupID); err != nil {
				return err
			}
		}
		// A changed fixed target, mapping, or mapping context needs an exact
		// target validation. The persisted manual model is intentionally read
		// from key after this patch has been applied; browser input must not
		// decide the gate. Run it separately from the candidate-group mapping
		// configuration validation below: that configuration is group-wide and
		// strict, while this is the single fixed target's current-fact gate.
		if !key.SmartRouting && (changes.route || changes.mapping || changes.mappingContext) {
			if err := s.validateManualMappedTargetWithAutomaticResolver(transactionStore, key, automaticResolver); err != nil {
				if changes.candidateGroup {
					return fmt.Errorf("candidate group change would invalidate manual route %d/%s for downstream key %q: %w", key.ManualChannelID, key.ManualGroupName, key.Name, err)
				}
				return err
			}
		}
		if changes.mapping || changes.mappingContext {
			if err := s.validateDownstreamKeyModelMappingWithStore(transactionStore, key); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(key.APIKeyCipher) != "" {
		plain, err := s.Cipher.Decrypt(key.APIKeyCipher)
		if err != nil {
			return nil, err
		}
		key.APIKey = strings.TrimSpace(plain)
	}
	return key, nil
}

func (s *Service) validateCandidateGroup(id uint) error {
	return s.validateCandidateGroupWithStore(s.Store, id)
}

func (s *Service) validateCandidateGroupWithStore(store *storage.Gateway, id uint) error {
	if id == 0 {
		return nil
	}
	if store == nil {
		return errors.New("gateway store is unavailable")
	}
	if _, err := store.FindCandidateGroup(id); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("candidate group %d does not exist", id)
		}
		return err
	}
	return nil
}

func (s *Service) validateDownstreamKeyModelMapping(key *storage.GatewayDownstreamKey) error {
	return s.validateDownstreamKeyModelMappingWithStore(s.Store, key)
}

func (s *Service) validateDownstreamKeyModelMappingWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey) error {
	if key == nil {
		return errors.New("downstream key is required")
	}
	strategy, err := NormalizeModelMappingStrategy(key.ModelMappingStrategy)
	if err != nil {
		return err
	}
	key.ModelMappingStrategy = string(strategy)
	if strategy == ModelMappingStrategyDisabled {
		return nil
	}
	key.ModelMappingModel = strings.TrimSpace(key.ModelMappingModel)
	key.ModelMappingReasoningEffort = strings.TrimSpace(key.ModelMappingReasoningEffort)
	if strategy == ModelMappingStrategyManual && strings.TrimSpace(key.ModelMappingModel) == "" {
		return errors.New("manual model mapping requires a model")
	}
	if strategy == ModelMappingStrategyManual && key.ModelMappingReasoningEffort == "" {
		return errors.New("manual model mapping requires a reasoning effort")
	}
	if key.ModelMappingReasoningEffort != "" {
		effort, err := NormalizeModelMappingReasoningEffort(key.App, key.ModelMappingReasoningEffort)
		if err != nil {
			return err
		}
		key.ModelMappingReasoningEffort = effort
	}
	if key.CandidateGroupID == 0 {
		return errors.New("model mapping requires a candidate group with a radar family")
	}
	if s == nil || store == nil {
		return errors.New("gateway store is unavailable")
	}
	group, err := store.FindCandidateGroup(key.CandidateGroupID)
	if err != nil {
		return err
	}
	family, err := NormalizeModelMappingFamily(group.ModelMappingFamily)
	if err != nil || family == "" {
		return errors.New("candidate group must select codex or claude radar")
	}
	if key.App != storage.GatewayApp(family) || !strings.EqualFold(strings.TrimSpace(key.ModelFamily), family) {
		return errors.New("model mapping key and candidate group must use the same model family")
	}
	if err := s.validateCandidateGroupMappingUpdateWithStore(store, group.ID, family, group.Members); err != nil {
		return err
	}
	if err := store.CandidateMembersSupportMappingFamily(group.Members, family); err != nil {
		return err
	}
	if strategy == ModelMappingStrategyManual {
		if err := validateManualMappedModelWithStore(store, group.Members, key.ModelMappingModel); err != nil {
			return err
		}
	}
	return nil
}

func validateManualMappedModelWithStore(store *storage.Gateway, members []storage.GatewayCandidateGroupMember, model string) error {
	model = strings.TrimSpace(model)
	if store == nil {
		return errors.New("gateway store is unavailable")
	}
	return store.CandidateMembersSupportManualMappedModel(members, model)
}

func (s *Service) validateKeyGroup(id uint) error {
	return s.validateKeyGroupWithStore(s.Store, id)
}

func (s *Service) validateKeyGroupWithStore(store *storage.Gateway, id uint) error {
	if id == 0 {
		return nil
	}
	if store == nil {
		return errors.New("gateway store is unavailable")
	}
	groups, err := store.ListKeyGroups()
	if err != nil {
		return err
	}
	for _, group := range groups {
		if group.ID == id {
			return nil
		}
	}
	return gorm.ErrRecordNotFound
}

func (s *Service) ListKeyGroups() ([]storage.GatewayKeyGroup, error) {
	return s.Store.ListKeyGroups()
}

func (s *Service) SaveKeyGroup(id uint, name string) (*storage.GatewayKeyGroup, error) {
	group := &storage.GatewayKeyGroup{ID: id, Name: strings.TrimSpace(name)}
	if err := s.Store.SaveKeyGroup(group); err != nil {
		return nil, err
	}
	return group, nil
}

func (s *Service) DeleteKeyGroup(id uint) error {
	return s.Store.DeleteKeyGroup(id)
}

func (s *Service) ListUpstreamGroupChanges(pendingOnly bool) ([]storage.GatewayUpstreamGroupChange, error) {
	return s.Store.ListUpstreamGroupChanges(pendingOnly)
}

func (s *Service) ReconcileReservedManagedUpstreamKeyRemovalReviews(ctx context.Context) (resolved int, failed int, err error) {
	if s == nil || s.Store == nil {
		return 0, 0, errors.New("managed upstream key review reconciliation is unavailable")
	}
	changes, err := s.Store.ListPendingUpstreamGroupChangeManualReviews()
	if err != nil {
		return 0, 0, err
	}
	for _, change := range changes {
		if !isStronglyManagedUpstreamKeyName(change.UpstreamKeyName) {
			continue
		}
		if err := s.ResumeManagedUpstreamKeyRemovalManualReview(ctx, change.ID, "system_reserved_key_reconciler", "reserved_name_absence_revalidated"); err != nil {
			failed++
			continue
		}
		resolved++
	}
	return resolved, failed, nil
}

// ResumeManagedUpstreamKeyRemovalManualReview performs the administrator
// recovery path for one quarantined removal intent. The remote inventory and
// ownership are revalidated while the channel operation lock is held; only
// then is the claim epoch advanced and the exact remote identity finalized.
// An ignored intent is never converted back to processing without these fresh
// facts.
func (s *Service) ResumeManagedUpstreamKeyRemovalManualReview(ctx context.Context, intentID uint, actor, resolution string) error {
	if s == nil || s.Store == nil || s.Channels == nil || s.Keys == nil {
		return errors.New("managed upstream key removal recovery is unavailable")
	}
	if intentID == 0 {
		return errors.New("managed upstream key removal intent id is required")
	}
	change, err := s.Store.FindUpstreamGroupChange(intentID)
	if err != nil {
		return err
	}
	if change.Kind != "removed" || change.Status != "ignored" || change.AssignmentReasonCode != "manual_review_required" ||
		change.RemovalChannelIdentityRevision == nil || *change.RemovalChannelIdentityRevision == 0 ||
		change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch == 0 || change.UpstreamGroupID == nil ||
		change.PresenceGeneration == nil || change.UpstreamKeyID <= 0 || strings.TrimSpace(change.UpstreamKeyName) == "" {
		return storage.ErrManagedUpstreamKeyRemovalIntentChanged
	}

	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, change.ChannelID)
	if err != nil {
		return err
	}
	defer unlock()

	channel, err := s.Channels.FindByID(change.ChannelID)
	if err != nil {
		return err
	}
	identityRevision := *change.RemovalChannelIdentityRevision
	if channel.UpstreamAccountIdentityRevision != identityRevision {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	canonicalGroupName, err := s.Store.CanonicalUpstreamGroupName(change.GroupName)
	if err != nil {
		return err
	}
	expected := storage.UpstreamGroupGeneration{
		ID: *change.UpstreamGroupID, ChannelID: change.ChannelID,
		CanonicalGroupName: canonicalGroupName, PresenceGeneration: *change.PresenceGeneration,
	}
	if !expected.Valid() {
		return storage.ErrUpstreamGroupGenerationChanged
	}
	if err := s.Store.RequireAbsentUpstreamGroupGeneration(expected); err != nil {
		return err
	}

	local, err := s.Store.FindUpstreamKeyAny(change.ChannelID, change.GroupName)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return storage.ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	if err != nil {
		return err
	}
	if local == nil || local.ID == 0 || local.Enabled || local.UpstreamKeyID != change.UpstreamKeyID ||
		strings.TrimSpace(local.UpstreamKeyName) != strings.TrimSpace(change.UpstreamKeyName) {
		return storage.ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	ownershipClaim, err := managedUpstreamKeyRemovalOwnershipClaim(*local)
	if err != nil {
		return err
	}
	remote, nameReused, err := s.findRemovedGroupRemoteKey(ctx, change.ChannelID, identityRevision, local)
	if err != nil {
		return fmt.Errorf("revalidate managed upstream key %d: %w", change.UpstreamKeyID, err)
	}
	if nameReused {
		return storage.ErrManagedUpstreamKeyRemovalIdentityConflict
	}

	evidenceDigest := ""
	if remote == nil {
		evidenceDigest = storage.ManagedUpstreamKeyAbsenceEvidenceDigest(expected, identityRevision, change.UpstreamKeyID, change.UpstreamKeyName, ownershipClaim)
	} else {
		var reviewReason string
		evidenceDigest, reviewReason, err = s.managedKeyRemovalEvidenceDigest(ctx, expected, change.GroupName, local, *remote, identityRevision)
		if err != nil {
			return fmt.Errorf("revalidate managed upstream key %d evidence: %w", change.UpstreamKeyID, err)
		}
		if reviewReason != "" || evidenceDigest == "" {
			return fmt.Errorf("managed upstream key %d still requires manual review: %s", change.UpstreamKeyID, reviewReason)
		}
	}
	resumed, err := s.Store.ResumeGenerationSafeManagedUpstreamKeyRemovalManualReview(
		expected, identityRevision, *change.RemovalClaimEpoch, change.ID, local.ID,
		change.UpstreamKeyID, change.UpstreamKeyName, evidenceDigest, ownershipClaim, actor, resolution, time.Now(),
	)
	if err != nil {
		return err
	}
	if resumed == nil || resumed.RemovalClaimEpoch == nil {
		return storage.ErrManagedUpstreamKeyRemovalIntentChanged
	}
	claimEpoch := *resumed.RemovalClaimEpoch
	if remote == nil {
		return s.Store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
			expected, identityRevision, claimEpoch, resumed.ID, local.ID, change.UpstreamKeyID,
			change.UpstreamKeyName, evidenceDigest, ownershipClaim, time.Now(), nil,
		)
	}
	remoteDelete, err := s.Keys.PrepareManagedAPIKeyDeletion(ctx, change.ChannelID, *remote, identityRevision)
	if err != nil {
		if reviewErr := s.Store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected, identityRevision, claimEpoch, change.GroupName, change.UpstreamKeyID, change.UpstreamKeyName, resumed.ID, false, time.Now()); reviewErr != nil {
			err = errors.Join(err, fmt.Errorf("record managed upstream key removal manual review: %w", reviewErr))
		}
		return fmt.Errorf("prepare managed upstream key %d deletion: %w", remote.ID, err)
	}
	finalizeErr := s.Store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		expected, identityRevision, claimEpoch, resumed.ID, local.ID, remote.ID, remote.Name,
		evidenceDigest, ownershipClaim, time.Now(), remoteDelete,
	)
	if errors.Is(finalizeErr, connector.ErrManagedAPIKeyDeletionIdentityChanged) {
		if reviewErr := s.Store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected, identityRevision, claimEpoch, change.GroupName, change.UpstreamKeyID, change.UpstreamKeyName, resumed.ID, false, time.Now()); reviewErr != nil {
			finalizeErr = errors.Join(finalizeErr, fmt.Errorf("record managed upstream key identity drift: %w", reviewErr))
		}
	}
	return finalizeErr
}

func (s *Service) ResolveUpstreamGroupChange(id uint, candidateGroupIDs []uint, ignore bool) error {
	return s.ResolveUpstreamGroupChangeAs(id, candidateGroupIDs, ignore, "administrator")
}

func (s *Service) ResolveUpstreamGroupChangeAs(id uint, candidateGroupIDs []uint, ignore bool, actor string) error {
	return s.ResolveUpstreamGroupChangeAsWithIdentityRevision(id, candidateGroupIDs, ignore, actor, 0)
}

func (s *Service) ResolveUpstreamGroupChangeAsWithIdentityRevision(id uint, candidateGroupIDs []uint, ignore bool, actor string, expectedIdentityRevision uint64) error {
	return s.ResolveUpstreamGroupChangeAsWithIdentityRevisionContext(context.Background(), id, candidateGroupIDs, ignore, actor, expectedIdentityRevision)
}

func (s *Service) ResolveUpstreamGroupChangeAsWithIdentityRevisionContext(ctx context.Context, id uint, candidateGroupIDs []uint, ignore bool, actor string, expectedIdentityRevision uint64) error {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "administrator"
	}
	store := s.Store.WithContext(ctx)
	change, err := store.FindUpstreamGroupChange(id)
	if err != nil {
		return err
	}
	if change.Status != "pending" {
		return errors.New("upstream group change is already resolved")
	}
	if change.Kind == "removed" {
		return store.ResolveUpstreamGroupChangeWithContext(ctx, id, "acknowledged", time.Now())
	}
	if ignore {
		return store.IgnoreUpstreamGroupChangeWithContext(ctx, id, "Ignored from upstream change review", actor, time.Now())
	}
	if len(candidateGroupIDs) == 0 {
		return errors.New("select at least one candidate group or ignore this change")
	}
	seen := make(map[uint]struct{}, len(candidateGroupIDs))
	for _, candidateGroupID := range candidateGroupIDs {
		if candidateGroupID == 0 {
			return errors.New("candidate group is required")
		}
		if _, ok := seen[candidateGroupID]; ok {
			continue
		}
		seen[candidateGroupID] = struct{}{}
		if _, err := store.FindCandidateGroup(candidateGroupID); err != nil {
			return err
		}
	}
	return store.AssignUpstreamGroupChangeAsWithIdentityRevisionContext(ctx, id, candidateGroupIDs, actor, time.Now(), expectedIdentityRevision)
}

func (s *Service) ResolveUpstreamGroupChanges(ids []uint, candidateGroupIDs []uint, ignore bool) (int, map[uint]string) {
	return s.ResolveUpstreamGroupChangesAs(ids, candidateGroupIDs, ignore, "administrator")
}

func (s *Service) ResolveUpstreamGroupChangesAs(ids []uint, candidateGroupIDs []uint, ignore bool, actor string) (int, map[uint]string) {
	return s.ResolveUpstreamGroupChangesAsWithIdentityRevisions(ids, candidateGroupIDs, ignore, actor, nil)
}

func (s *Service) ResolveUpstreamGroupChangesAsWithIdentityRevisions(ids []uint, candidateGroupIDs []uint, ignore bool, actor string, expectedRevisions map[uint]uint64) (int, map[uint]string) {
	return s.ResolveUpstreamGroupChangesAsWithIdentityRevisionsContext(context.Background(), ids, candidateGroupIDs, ignore, actor, expectedRevisions)
}

func (s *Service) ResolveUpstreamGroupChangesAsWithIdentityRevisionsContext(ctx context.Context, ids []uint, candidateGroupIDs []uint, ignore bool, actor string, expectedRevisions map[uint]uint64) (int, map[uint]string) {
	store := s.Store.WithContext(ctx)
	failures := make(map[uint]string)
	seen := make(map[uint]struct{}, len(ids))
	resolved := 0
	for _, id := range ids {
		if id == 0 {
			failures[id] = "upstream group change id is required"
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		var expectedRevision uint64
		if expectedRevisions != nil {
			change, findErr := store.FindUpstreamGroupChange(id)
			if findErr != nil {
				failures[id] = findErr.Error()
				continue
			}
			var ok bool
			expectedRevision, ok = expectedRevisions[change.ChannelID]
			if !ok || expectedRevision == 0 {
				failures[id] = storage.ErrStaleChannelAccountIdentityRevision.Error()
				continue
			}
		}
		if err := s.ResolveUpstreamGroupChangeAsWithIdentityRevisionContext(ctx, id, candidateGroupIDs, ignore, actor, expectedRevision); err != nil {
			if errors.Is(err, storage.ErrUpstreamGroupChangeSuperseded) {
				resolved++
				continue
			}
			failures[id] = err.Error()
			continue
		}
		resolved++
	}
	return resolved, failures
}

func (s *Service) ListCandidateGroups() ([]storage.GatewayCandidateGroup, error) {
	return s.Store.ListCandidateGroups()
}

func (s *Service) SaveCandidateGroup(id uint, name string, members []storage.GatewayCandidateGroupMember) (*storage.GatewayCandidateGroup, error) {
	return s.SaveCandidateGroupWithMappingFamily(id, name, "", members)
}

func (s *Service) SaveCandidateGroupWithMappingFamily(id uint, name, mappingFamily string, members []storage.GatewayCandidateGroupMember) (*storage.GatewayCandidateGroup, error) {
	return s.saveCandidateGroupWithMappingFamily(id, name, mappingFamily, members, "administrator", false, nil)
}

func (s *Service) SaveCandidateGroupWithMappingFamilyAs(id uint, name, mappingFamily string, members []storage.GatewayCandidateGroupMember, actor string) (*storage.GatewayCandidateGroup, error) {
	return s.saveCandidateGroupWithMappingFamily(id, name, mappingFamily, members, actor, true, nil)
}

func (s *Service) SaveCandidateGroupWithMappingFamilyVersionAs(id uint, name, mappingFamily string, members []storage.GatewayCandidateGroupMember, expectedMembershipVersion uint64, actor string) (*storage.GatewayCandidateGroup, error) {
	return s.saveCandidateGroupWithMappingFamily(id, name, mappingFamily, members, actor, true, &expectedMembershipVersion)
}

func (s *Service) SaveCandidateGroupAs(id uint, name string, members []storage.GatewayCandidateGroupMember, actor string) (*storage.GatewayCandidateGroup, error) {
	return s.SaveCandidateGroupWithMappingFamilyAs(id, name, "", members, actor)
}

func (s *Service) saveCandidateGroupWithMappingFamily(id uint, name, mappingFamily string, members []storage.GatewayCandidateGroupMember, actor string, requireCurrentGeneration bool, expectedMembershipVersion *uint64) (*storage.GatewayCandidateGroup, error) {
	family, err := NormalizeModelMappingFamily(mappingFamily)
	if err != nil {
		return nil, err
	}
	if err := s.validateCandidateGroupMappingUpdate(id, family, members); err != nil {
		return nil, err
	}
	group := &storage.GatewayCandidateGroup{ID: id, Name: strings.TrimSpace(name), ModelMappingFamily: family}
	if requireCurrentGeneration {
		if expectedMembershipVersion != nil {
			err = s.Store.SaveCandidateGroupCurrentGenerationVersionAs(group, members, *expectedMembershipVersion, actor)
		} else {
			err = s.Store.SaveCandidateGroupCurrentGenerationAs(group, members, actor)
		}
	} else {
		err = s.Store.SaveCandidateGroup(group, members)
	}
	if err != nil {
		return nil, err
	}
	return s.Store.FindCandidateGroup(group.ID)
}

func (s *Service) validateCandidateGroupMappingUpdate(id uint, mappingFamily string, members []storage.GatewayCandidateGroupMember) error {
	return s.validateCandidateGroupMappingUpdateWithStore(s.Store, id, mappingFamily, members)
}

// validateCandidateGroupMappingUpdateWithStore protects configuration
// invariants only. A candidate membership edit is not a model-mapping change,
// so incomplete probe/model facts must not block it; the downstream-key
// mapping validator performs the strict capability checks when a mapping is
// actually enabled or modified.
func (s *Service) validateCandidateGroupMappingUpdateWithStore(store *storage.Gateway, id uint, mappingFamily string, members []storage.GatewayCandidateGroupMember) error {
	if s == nil || store == nil {
		return errors.New("gateway store is unavailable")
	}
	family, err := NormalizeModelMappingFamily(mappingFamily)
	if err != nil {
		return err
	}
	for _, member := range members {
		channelID := member.ChannelID
		groupName := strings.TrimSpace(member.GroupName)
		if channelID == 0 || groupName == "" {
			return errors.New("candidate group member requires channel and upstream group")
		}
	}
	if id == 0 {
		return nil
	}
	keys, err := store.ListDownstreamKeysByCandidateGroup(id)
	if err != nil {
		return err
	}
	memberRoutes := make(map[candidateLatencyKey]struct{}, len(members))
	for _, member := range members {
		memberRoutes[canonicalCandidateKey(member.ChannelID, member.GroupName)] = struct{}{}
	}
	for _, key := range keys {
		if !key.SmartRouting {
			manualRoute := canonicalCandidateKey(key.ManualChannelID, key.ManualGroupName)
			if _, ok := memberRoutes[manualRoute]; !ok {
				return fmt.Errorf("candidate group %d must keep manual route %d/%s used by downstream key %q; enable smart routing or choose another manual route first", id, key.ManualChannelID, key.ManualGroupName, key.Name)
			}
		}
		strategy, err := NormalizeModelMappingStrategy(key.ModelMappingStrategy)
		if err != nil {
			return err
		}
		if strategy == ModelMappingStrategyDisabled {
			continue
		}
		if family == "" || key.App != storage.GatewayApp(family) || !strings.EqualFold(strings.TrimSpace(key.ModelFamily), family) {
			return fmt.Errorf("candidate group is used by mapped key %q and must remain %s", key.Name, key.ModelFamily)
		}
	}
	return nil
}

func (s *Service) DeleteCandidateGroup(id uint) error {
	return s.Store.DeleteCandidateGroup(id)
}

func (s *Service) DeleteDownstreamKey(id uint) error {
	return s.Store.DeleteDownstreamKey(id)
}

func (s *Service) SetRouteExclusion(downstreamKeyID uint, channelID uint, groupName string, disabled bool) error {
	if _, err := s.Store.FindDownstreamKey(downstreamKeyID); err != nil {
		return err
	}
	return s.Store.SetRouteExclusion(downstreamKeyID, channelID, groupName, disabled)
}

func (s *Service) Authenticate(raw string) (*storage.GatewayDownstreamKey, error) {
	token := bearerToken(raw)
	if token == "" {
		return nil, errors.New("missing bearer token")
	}
	key, err := s.Store.FindDownstreamKeyByHash(HashKey(token))
	if err != nil {
		return nil, errors.New("invalid bearer token")
	}
	now := time.Now()
	if key.LastUsedAt == nil || !key.LastUsedAt.Add(gatewayStateTouchInterval).After(now) {
		_ = s.Store.TouchDownstreamKey(key.ID, now)
	}
	return key, nil
}

// IssueDownstreamKeyTestToken creates a short-lived one-time marker for an
// API connectivity test. The marker is not derived from the downstream key,
// so key holders cannot hide normal requests as tests.
func (s *Service) IssueDownstreamKeyTestToken(downstreamKeyID uint) (string, error) {
	if s == nil || s.Store == nil {
		return "", errors.New("gateway service is unavailable")
	}
	key, err := s.Store.FindDownstreamKey(downstreamKeyID)
	if err != nil {
		return "", err
	}
	if !key.Enabled {
		return "", errors.New("downstream key is disabled")
	}
	token, err := randomKey("test-")
	if err != nil {
		return "", err
	}
	now := time.Now()
	s.testTokenMu.Lock()
	defer s.testTokenMu.Unlock()
	if s.testTokens == nil {
		s.testTokens = make(map[string]downstreamKeyTestToken)
	}
	for value, pending := range s.testTokens {
		if !pending.ExpiresAt.After(now) {
			delete(s.testTokens, value)
		}
	}
	if len(s.testTokens) >= maxPendingDownstreamKeyTestTokens {
		return "", errors.New("too many pending downstream key tests")
	}
	s.testTokens[token] = downstreamKeyTestToken{DownstreamKeyID: downstreamKeyID, ExpiresAt: now.Add(downstreamKeyTestTokenTTL)}
	return token, nil
}

// ConsumeDownstreamKeyTestToken atomically verifies and removes a pending
// marker before the matching downstream key is proxied.
func (s *Service) ConsumeDownstreamKeyTestToken(token string, downstreamKeyID uint) bool {
	if s == nil || downstreamKeyID == 0 {
		return false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	s.testTokenMu.Lock()
	defer s.testTokenMu.Unlock()
	pending, ok := s.testTokens[token]
	if !ok {
		return false
	}
	delete(s.testTokens, token)
	return pending.DownstreamKeyID == downstreamKeyID && pending.ExpiresAt.After(time.Now())
}

func normalizeGatewayApp(app storage.GatewayApp) storage.GatewayApp {
	switch storage.GatewayApp(strings.ToLower(strings.TrimSpace(string(app)))) {
	case storage.GatewayAppClaude:
		return storage.GatewayAppClaude
	case storage.GatewayAppGemini:
		return storage.GatewayAppGemini
	default:
		return storage.GatewayAppCodex
	}
}

var gatewayOpenAIModelFamilies = map[string]struct{}{
	string(storage.GatewayAppCodex): {},
	"grok":                          {},
	"deepseek":                      {},
	"qwen":                          {},
	"mistral":                       {},
	"llama":                         {},
	"glm":                           {},
	"minimax":                       {},
	"moonshot":                      {},
	"kimi":                          {},
	"yi":                            {},
	"ernie":                         {},
}

func defaultGatewayModelFamily(app storage.GatewayApp) string {
	switch normalizeGatewayApp(app) {
	case storage.GatewayAppClaude:
		return string(storage.GatewayAppClaude)
	case storage.GatewayAppGemini:
		return string(storage.GatewayAppGemini)
	default:
		return string(storage.GatewayAppCodex)
	}
}

func normalizeGatewayModelFamily(family string, app storage.GatewayApp) (string, error) {
	app = normalizeGatewayApp(app)
	family = strings.ToLower(strings.TrimSpace(family))
	if family == "" || family == "openai" || family == "openai-compatible" || family == "gpt" {
		family = defaultGatewayModelFamily(app)
	}
	if app == storage.GatewayAppClaude {
		if family != string(storage.GatewayAppClaude) {
			return "", fmt.Errorf("claude key must use claude model family")
		}
		return family, nil
	}
	if app == storage.GatewayAppGemini {
		if family != string(storage.GatewayAppGemini) {
			return "", fmt.Errorf("gemini key must use gemini model family")
		}
		return family, nil
	}
	if _, ok := gatewayOpenAIModelFamilies[family]; !ok {
		return "", fmt.Errorf("unsupported OpenAI-compatible model family %q", family)
	}
	return family, nil
}

func (s *Service) Candidates(model string) ([]Candidate, error) {
	return s.CandidatesForApp(storage.GatewayAppCodex, model)
}

func (s *Service) CandidatesForApp(app storage.GatewayApp, model string) ([]Candidate, error) {
	candidates, _, err := s.CandidatesForAppWithExclusions(app, model)
	return candidates, err
}

func (s *Service) CandidatesForAppWithExclusions(app storage.GatewayApp, model string) ([]Candidate, []CandidateExclusion, error) {
	return s.candidatesForApp(app, model, 0, false, false)
}

func (s *Service) CandidatesForDownstreamKeyWithExclusions(downstreamKeyID uint, app storage.GatewayApp, model string, includeDisabled bool) ([]Candidate, []CandidateExclusion, error) {
	modelFamily := ""
	var allowed map[candidateLatencyKey]struct{}
	candidateGroupName := ""
	if downstreamKeyID > 0 {
		if s.Store == nil {
			return nil, nil, errors.New("gateway store is unavailable")
		}
		key, err := s.Store.FindDownstreamKey(downstreamKeyID)
		if err != nil {
			return nil, nil, fmt.Errorf("find downstream key %d: %w", downstreamKeyID, err)
		}
		modelFamily = key.ModelFamily
		app = key.App
		if key.CandidateGroupID > 0 {
			group, err := s.Store.FindCandidateGroup(key.CandidateGroupID)
			if err != nil {
				return nil, nil, fmt.Errorf("find candidate group %d: %w", key.CandidateGroupID, err)
			}
			candidateGroupName = group.Name
			allowed = make(map[candidateLatencyKey]struct{}, len(group.Members))
			for _, member := range group.Members {
				allowed[canonicalCandidateKey(member.ChannelID, member.GroupName)] = struct{}{}
			}
		}
	}
	candidates, exclusions, err := s.candidatesForAppFamilyAllowed(app, model, downstreamKeyID, includeDisabled, modelFamily, false, false, false, allowed)
	if err != nil && allowed != nil {
		return nil, exclusions, fmt.Errorf("candidate group %q has no available candidates: %w", candidateGroupName, err)
	}
	return candidates, exclusions, err
}

// CandidateGroupMemberPreviewForDownstreamKey returns every persisted member
// of one downstream key's candidate group. The selected model is mandatory:
// an unqualified list cannot truthfully represent the request-time order.
//
// When mappedModel is true, it mirrors the gateway mapping sequence: ordinary
// routing is scored first and the mapped model then filters that order. For an
// unmapped request, model support participates in the initial candidate
// calculation just as it does on the request path.
func (s *Service) CandidateGroupMemberPreviewForDownstreamKey(downstreamKeyID uint, model string, mappedModel bool) ([]CandidateGroupMemberPreview, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, errors.New("model is required for candidate group preview")
	}
	return s.candidateMemberPreviewForDownstreamKey(downstreamKeyID, model, mappedModel, false)
}

// ManualRouteMemberPreviewForDownstreamKey retains every configured member,
// including temporarily unavailable channels. It is intentionally unscored
// and model-agnostic because the operator is choosing an exact route, not
// asking the smart router for a request-time ranking.
func (s *Service) ManualRouteMemberPreviewForDownstreamKey(downstreamKeyID uint) ([]CandidateGroupMemberPreview, error) {
	return s.candidateMemberPreviewForDownstreamKey(downstreamKeyID, "", false, true)
}

func (s *Service) candidateMemberPreviewForDownstreamKey(downstreamKeyID uint, model string, mappedModel bool, manualSelection bool) ([]CandidateGroupMemberPreview, error) {
	if s == nil || s.Store == nil || s.Channels == nil || s.Rates == nil {
		return nil, errors.New("candidate group preview is unavailable")
	}
	if downstreamKeyID == 0 {
		return nil, errors.New("downstream key is required")
	}
	downstream, err := s.Store.FindDownstreamKey(downstreamKeyID)
	if err != nil {
		return nil, fmt.Errorf("find downstream key %d: %w", downstreamKeyID, err)
	}
	if downstream.CandidateGroupID == 0 && !manualSelection {
		return nil, errors.New("downstream key has no candidate group")
	}

	config, err := s.ProbeConfig()
	if err != nil {
		return nil, err
	}
	groups := make(map[candidateLatencyKey]ProbeConfigGroup)
	for _, channel := range config {
		for _, configuredGroup := range channel.Groups {
			groups[canonicalCandidateKey(channel.ChannelID, configuredGroup.GroupName)] = configuredGroup
		}
	}
	storedChannels, err := s.Channels.List()
	if err != nil {
		return nil, err
	}
	storedChannelsByID := make(map[uint]storage.Channel, len(storedChannels))
	for _, channel := range storedChannels {
		storedChannelsByID[channel.ID] = channel
	}
	members := make([]storage.GatewayCandidateGroupMember, 0)
	if downstream.CandidateGroupID > 0 {
		group, err := s.Store.FindCandidateGroup(downstream.CandidateGroupID)
		if err != nil {
			return nil, fmt.Errorf("find candidate group %d: %w", downstream.CandidateGroupID, err)
		}
		members = append(members, group.Members...)
	} else {
		for _, channel := range config {
			for _, configuredGroup := range channel.Groups {
				members = append(members, storage.GatewayCandidateGroupMember{ChannelID: channel.ChannelID, GroupName: strings.TrimSpace(configuredGroup.GroupName)})
			}
		}
	}
	memberChannelIDs := make([]uint, 0, len(members))
	seenMemberChannelIDs := make(map[uint]struct{}, len(members))
	for _, member := range members {
		if member.ChannelID == 0 {
			continue
		}
		if _, seen := seenMemberChannelIDs[member.ChannelID]; seen {
			continue
		}
		seenMemberChannelIDs[member.ChannelID] = struct{}{}
		memberChannelIDs = append(memberChannelIDs, member.ChannelID)
	}
	upstreamKeys, err := s.Store.ListUpstreamKeys(memberChannelIDs)
	if err != nil {
		return nil, err
	}
	upstreamByRoute := make(map[candidateLatencyKey]storage.GatewayUpstreamKey, len(upstreamKeys))
	for _, upstream := range upstreamKeys {
		routeKey := canonicalCandidateKey(upstream.ChannelID, upstream.GroupName)
		current, exists := upstreamByRoute[routeKey]
		if !exists || upstreamKeyRoutePreference(upstream, current) {
			upstreamByRoute[routeKey] = upstream
		}
	}
	generationByRoute := make(map[candidateLatencyKey]storage.UpstreamGroupGeneration)
	if manualSelection {
		currentGenerations, err := s.Store.ListUpstreamGroupGenerations(memberChannelIDs)
		if err != nil {
			return nil, err
		}
		generationByRoute = make(map[candidateLatencyKey]storage.UpstreamGroupGeneration, len(currentGenerations))
		for _, generation := range currentGenerations {
			generationByRoute[canonicalCandidateKey(generation.ChannelID, generation.CanonicalGroupName)] = generation
		}
	}

	disabled, err := s.routeExclusionSet(downstreamKeyID)
	if err != nil {
		return nil, err
	}
	unsupported := make([]storage.GatewayModelCapability, 0)
	if model != "" {
		unsupported, err = s.Store.ListUnsupportedModelCapabilities(model, modelCapabilityMaxAge)
		if err != nil {
			return nil, err
		}
	}
	unsupportedRoutes := make(map[candidateLatencyKey]struct{}, len(unsupported))
	for _, capability := range unsupported {
		unsupportedRoutes[canonicalCandidateKey(capability.ChannelID, capability.GroupName)] = struct{}{}
	}

	modelFamily := strings.TrimSpace(downstream.ModelFamily)
	if modelFamily == "" {
		modelFamily = defaultGatewayModelFamily(downstream.App)
	}
	previews := make([]CandidateGroupMemberPreview, 0, len(members))
	scoreable := make([]Candidate, 0, len(members))
	for _, member := range members {
		groupName := strings.TrimSpace(member.GroupName)
		routeKey := canonicalCandidateKey(member.ChannelID, groupName)
		candidate := Candidate{
			Channel:        storage.Channel{ID: member.ChannelID, Name: fmt.Sprintf("渠道 #%d", member.ChannelID)},
			Rate:           storage.RateSnapshot{ChannelID: member.ChannelID, ModelName: groupName},
			App:            storage.GatewayAppCodex,
			DisabledForKey: routeExcluded(disabled, member.ChannelID, groupName),
		}
		preview := CandidateGroupMemberPreview{
			Candidate:           candidate,
			CandidateMemberID:   member.ID,
			AvailabilityReasons: make([]string, 0, 5),
			ProbeBlockers:       make([]string, 0, 5),
			LatencySource:       latencySourceNone,
		}

		channel, channelExists := storedChannelsByID[member.ChannelID]
		configuredGroup, groupExists := groups[routeKey]
		basicAvailable := true
		if manualSelection {
			if generation, generationExists := generationByRoute[routeKey]; !generationExists || !generation.Valid() {
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, string(ManualRouteGateReasonGenerationUnavailable))
				basicAvailable = false
			}
		}
		if !channelExists {
			preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "channel_missing")
			preview.ProbeBlockers = appendReason(preview.ProbeBlockers, "channel_missing")
			basicAvailable = false
		} else {
			preview.Channel = channel
		}
		if !groupExists {
			preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, availabilityReasonNoRateSnapshot)
			preview.ProbeBlockers = appendReason(preview.ProbeBlockers, availabilityReasonNoRateSnapshot)
			if !manualSelection {
				basicAvailable = false
			}
			if upstream, upstreamExists := upstreamByRoute[routeKey]; !upstreamExists {
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, probeBlockerKeyMissing)
				preview.ProbeBlockers = appendReason(preview.ProbeBlockers, probeBlockerKeyMissing)
				basicAvailable = false
			} else {
				preview.ModelFamilies = splitCSV(upstream.ModelFamilies)
				preview.ModelIDs = splitCSV(upstream.ModelIDs)
				preview.LatencyProbeModel = strings.TrimSpace(upstream.LatencyProbeModel)
				preview.ModelsSyncedAt = upstream.ModelsSyncedAt
				preview.ModelsError = upstream.ModelsError
				preview.ProbeDisabled = !upstream.LatencyProbeEnabled
				preview.UpstreamKeyConfigured = strings.TrimSpace(upstream.APIKeyCipher) != ""
				preview.UpstreamKeyEnabled = upstream.Enabled
				preview.UpstreamKeyProvisioning = upstream.Provisioning
				preview.UpstreamKeyAvailable = preview.UpstreamKeyConfigured && preview.UpstreamKeyEnabled && !preview.UpstreamKeyProvisioning
				switch {
				case !upstream.Enabled:
					preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, probeBlockerKeyDisabled)
					preview.ProbeBlockers = appendReason(preview.ProbeBlockers, probeBlockerKeyDisabled)
					basicAvailable = false
				case upstream.Provisioning:
					preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, probeBlockerKeyUnavailable)
					preview.ProbeBlockers = appendReason(preview.ProbeBlockers, probeBlockerKeyUnavailable)
					basicAvailable = false
				case !preview.UpstreamKeyConfigured:
					preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, probeBlockerKeyMissing)
					preview.ProbeBlockers = appendReason(preview.ProbeBlockers, probeBlockerKeyMissing)
					basicAvailable = false
				}
				if manualSelection {
					if preview.Channel.CredentialMode == storage.CredentialModeDirect {
						preview.App = normalizeGatewayApp(preview.Channel.GatewayApp)
						if preview.App != normalizeGatewayApp(downstream.App) {
							preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "gateway_app_mismatch")
							basicAvailable = false
						}
					} else {
						if generation, generationExists := generationByRoute[routeKey]; generationExists {
							if families, _, trusted := manualRouteCurrentSynchronizedFacts(&upstream, generation); trusted {
								if known, compatible := manualRouteProtocolCompatibility(families, downstream.App); known && !compatible {
									preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "gateway_app_mismatch")
									basicAvailable = false
								}
							}
						}
						preview.App = downstream.App
					}
				}
			}
		} else {
			preview.Rate.Ratio = configuredGroup.Ratio
			preview.Rate.CompletionRatio = configuredGroup.CompletionRatio
			preview.RawRatio = configuredGroup.RawRatio
			preview.RawCompletionRatio = configuredGroup.RawCompletionRatio
			preview.FundingFactor = configuredGroup.FundingFactor
			preview.CostBasisVersion = configuredGroup.CostBasisVersion
			preview.EffectivePrice = configuredGroup.EffectivePrice
			preview.ModelFamilies = append([]string(nil), configuredGroup.ModelFamilies...)
			preview.ModelIDs = append([]string(nil), configuredGroup.ModelIDs...)
			preview.LatencyProbeModel = configuredGroup.LatencyProbeModel
			preview.ModelsSyncedAt = configuredGroup.ModelsSyncedAt
			preview.ModelsError = configuredGroup.ModelsError
			preview.AvgLatencyMs = configuredGroup.AvgLatencyMs
			preview.LatencySamples = configuredGroup.LatencySamples
			preview.ProbeEstimatedCostUSD = configuredGroup.ProbeEstimatedCostUSD
			preview.ProbeDisabled = !configuredGroup.ProbeEnabled
			preview.CoolingDown = configuredGroup.CoolingDown
			preview.FailureCount = configuredGroup.FailureCount
			preview.CooldownUntil = configuredGroup.CooldownUntil
			preview.CooldownRemaining = time.Duration(configuredGroup.CooldownRemaining) * time.Second
			preview.ProbeEligible = configuredGroup.ProbeEligible
			preview.ProbeBlockers = append(preview.ProbeBlockers, configuredGroup.ProbeBlockers...)
			preview.LatencyQualified = configuredGroup.LatencyQualified
			preview.LatencySource = configuredGroup.LatencySource
			preview.UpstreamKeyConfigured = configuredGroup.UpstreamKeyConfigured
			preview.UpstreamKeyEnabled = configuredGroup.UpstreamKeyEnabled
			preview.UpstreamKeyProvisioning = configuredGroup.UpstreamKeyProvisioning
			preview.UpstreamKeyAvailable = configuredGroup.UpstreamKeyAvailable
			directChannel := preview.Channel.CredentialMode == storage.CredentialModeDirect
			hasRoutingBlocker := false
			for _, blocker := range configuredGroup.ProbeBlockers {
				if blocker == probeBlockerProbeDisabled {
					// Probe switches only control scheduled latency probes. Real
					// requests continue to use the normal routing availability
					// gates and still contribute passive latency samples.
					continue
				}
				if (manualSelection || directChannel) && (blocker == probeBlockerUnsupportedApp || blocker == probeBlockerModelsNotSynced) {
					// These describe background probe evidence. A fixed route may
					// use an otherwise ready exact channel/group target while a
					// model sync is pending or has failed. Direct channels likewise
					// use their administrator-owned protocol and model allowlist.
					continue
				}
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, blocker)
				hasRoutingBlocker = true
			}
			appSupported := candidatePreviewSupportsApp(configuredGroup.GatewayApps, downstream.App)
			if directChannel {
				appSupported = normalizeGatewayApp(preview.Channel.GatewayApp) == normalizeGatewayApp(downstream.App)
			}
			if directChannel && !appSupported {
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "gateway_app_mismatch")
				basicAvailable = false
			} else if manualSelection {
				if upstream, upstreamExists := upstreamByRoute[routeKey]; upstreamExists {
					if generation, generationExists := generationByRoute[routeKey]; generationExists {
						if families, _, trusted := manualRouteCurrentSynchronizedFacts(&upstream, generation); trusted {
							if known, compatible := manualRouteProtocolCompatibility(families, downstream.App); known && !compatible {
								preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "gateway_app_mismatch")
								basicAvailable = false
							}
						}
					}
				}
			} else if !appSupported {
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "gateway_app_mismatch")
				basicAvailable = false
			}
			if !manualSelection && !directChannel && !cachedGatewayFamilyMatches(preview.ModelFamilies, modelFamily) {
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "model_family_mismatch")
				basicAvailable = false
			}
			if directChannel {
				preview.App = normalizeGatewayApp(preview.Channel.GatewayApp)
			} else if manualSelection || appSupported {
				preview.App = downstream.App
			} else if len(configuredGroup.GatewayApps) > 0 {
				preview.App = storage.GatewayApp(configuredGroup.GatewayApps[0])
			}
			if hasRoutingBlocker || preview.CoolingDown {
				basicAvailable = false
			}
		}
		if channelExists && preview.Channel.CredentialMode == storage.CredentialModeDirect {
			preview.App = normalizeGatewayApp(preview.Channel.GatewayApp)
			if len(preview.ModelIDs) == 0 {
				preview.ModelFamilies = nil
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "model_capability_unknown")
				basicAvailable = false
			} else {
				preview.ModelFamilies = directChannelModelFamilies(&preview.Channel, preview.ModelIDs)
			}
		}

		modelAvailable := true
		if groupExists && model != "" {
			if mappedModel && len(preview.ModelIDs) == 0 {
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "model_capability_unknown")
				modelAvailable = false
			} else if (preview.Channel.CredentialMode == storage.CredentialModeDirect || len(preview.ModelIDs) > 0) && !modelAvailableForChannel(preview.Channel, preview.ModelIDs, model, downstream.App) {
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "model_not_listed")
				modelAvailable = false
			} else if _, markedUnsupported := unsupportedRoutes[routeKey]; markedUnsupported {
				preview.AvailabilityReasons = appendReason(preview.AvailabilityReasons, "model_not_supported")
				modelAvailable = false
			}
		} else if !groupExists && !manualSelection {
			modelAvailable = false
		}

		preview.RoutingAvailable = basicAvailable && modelAvailable && !preview.DisabledForKey
		if !manualSelection && basicAvailable && !preview.DisabledForKey && (mappedModel || modelAvailable) {
			scoreable = append(scoreable, preview.Candidate)
		}
		previews = append(previews, preview)
	}

	if manualSelection {
		return previews, nil
	}
	if !mappedModel {
		// Reuse the request path's comparison set, generation/cache evidence and
		// unit decision. The inventory projection above deliberately includes
		// unavailable members and is not itself sufficient for price scoring.
		if actual, _, priceErr := s.CandidatesForDownstreamKeyWithExclusions(downstreamKeyID, downstream.App, model, true); priceErr == nil {
			projectRoutingModelPricesToPreview(scoreable, actual)
		}
	}
	scoreCandidatesWithConfig(scoreable, s.routingConfig())
	sortCandidates(scoreable)
	byRoute := make(map[candidateLatencyKey]Candidate, len(scoreable))
	for _, candidate := range scoreable {
		byRoute[canonicalCandidateKey(candidate.Channel.ID, candidate.Rate.ModelName)] = candidate
	}
	ordered := make([]CandidateGroupMemberPreview, 0, len(previews))
	used := make(map[candidateLatencyKey]struct{}, len(previews))
	for _, candidate := range scoreable {
		routeKey := canonicalCandidateKey(candidate.Channel.ID, candidate.Rate.ModelName)
		for index := range previews {
			if previews[index].Channel.ID != routeKey.ChannelID || canonicalGroupName(previews[index].Rate.ModelName) != routeKey.GroupName || !previews[index].RoutingAvailable {
				continue
			}
			previews[index].Candidate = candidate
			ordered = append(ordered, previews[index])
			used[routeKey] = struct{}{}
			break
		}
	}
	for index := range previews {
		routeKey := canonicalCandidateKey(previews[index].Channel.ID, previews[index].Rate.ModelName)
		if scored, ok := byRoute[routeKey]; ok {
			previews[index].Candidate.Score = scored.Score
			previews[index].Candidate.PricePriorityScore = scored.PricePriorityScore
			previews[index].Candidate.LatencyPriorityScore = scored.LatencyPriorityScore
		}
		if _, ok := used[routeKey]; ok {
			continue
		}
		ordered = append(ordered, previews[index])
	}
	return ordered, nil
}

func candidatePreviewSupportsApp(apps []string, app storage.GatewayApp) bool {
	app = normalizeGatewayApp(app)
	for _, item := range apps {
		if normalizeGatewayApp(storage.GatewayApp(item)) == app {
			return true
		}
	}
	return false
}

func (s *Service) candidatesForApp(app storage.GatewayApp, model string, downstreamKeyID uint, includeDisabled bool, includeRecentlyFailed bool) ([]Candidate, []CandidateExclusion, error) {
	return s.candidatesForAppFamily(app, model, downstreamKeyID, includeDisabled, "", includeRecentlyFailed)
}

func (s *Service) candidatesForAppFamily(app storage.GatewayApp, model string, downstreamKeyID uint, includeDisabled bool, modelFamily string, includeRecentlyFailed bool) ([]Candidate, []CandidateExclusion, error) {
	return s.candidatesForAppFamilyAllowed(app, model, downstreamKeyID, includeDisabled, modelFamily, includeRecentlyFailed, false, false, nil)
}

func (s *Service) candidatesForAppFamilyAllowed(app storage.GatewayApp, model string, downstreamKeyID uint, includeDisabled bool, modelFamily string, includeRecentlyFailed bool, allowUntrustedCost bool, allowBalanceBlocked bool, allowed map[candidateLatencyKey]struct{}) ([]Candidate, []CandidateExclusion, error) {
	model = strings.TrimSpace(model)
	app = normalizeGatewayApp(app)
	modelFamily = strings.ToLower(strings.TrimSpace(modelFamily))
	if modelFamily == "" && downstreamKeyID > 0 {
		modelFamily = defaultGatewayModelFamily(app)
	}
	channels, err := s.Channels.List()
	if err != nil {
		return nil, nil, err
	}
	channelIDs := make([]uint, 0, len(channels))
	for _, channel := range channels {
		channelIDs = append(channelIDs, channel.ID)
	}
	accountConcurrencyByChannel, err := s.Channels.UpstreamAccountConcurrencyForChannels(channelIDs)
	if err != nil {
		return nil, nil, err
	}
	pairedRates, err := s.Rates.ListModelsWithCostBasis()
	if err != nil {
		return nil, nil, err
	}
	allRates, fundingFactors := splitRateCostBasisSnapshots(pairedRates)
	allRates = canonicalRateSnapshots(allRates)
	ratesByChannel := make(map[uint][]storage.RateSnapshot, len(channels))
	for _, rate := range allRates {
		ratesByChannel[rate.ChannelID] = append(ratesByChannel[rate.ChannelID], rate)
	}
	upstreamKeys, err := s.Store.ListUpstreamKeys(channelIDs)
	if err != nil {
		return nil, nil, err
	}
	upstreamGenerations, err := s.Store.ListUpstreamGroupGenerations(channelIDs)
	if err != nil {
		return nil, nil, err
	}
	generationByRoute := make(map[candidateLatencyKey]storage.UpstreamGroupGeneration, len(upstreamGenerations))
	for _, generation := range upstreamGenerations {
		generationByRoute[canonicalCandidateKey(generation.ChannelID, generation.CanonicalGroupName)] = generation
	}
	upstreamByRoute := make(map[candidateLatencyKey]storage.GatewayUpstreamKey, len(upstreamKeys))
	for _, key := range upstreamKeys {
		routeKey := canonicalCandidateKey(key.ChannelID, key.GroupName)
		current, exists := upstreamByRoute[routeKey]
		if !exists || upstreamKeyRoutePreference(key, current) {
			upstreamByRoute[routeKey] = key
		}
	}
	unsupportedRoutes := make(map[candidateLatencyKey]struct{})
	if model != "" {
		capabilities, capabilityErr := s.Store.ListUnsupportedModelCapabilities(model, modelCapabilityMaxAge)
		if capabilityErr != nil {
			return nil, nil, capabilityErr
		}
		for _, capability := range capabilities {
			unsupportedRoutes[canonicalCandidateKey(capability.ChannelID, capability.GroupName)] = struct{}{}
		}
	}
	latencies, err := s.candidateLatencies()
	if err != nil {
		return nil, nil, err
	}
	disabled, err := s.routeExclusionSet(downstreamKeyID)
	if err != nil {
		return nil, nil, err
	}
	routing := s.routingConfig()
	cacheRoutes := cacheRoutingStatsRoutes(allRates, channels, upstreamByRoute)
	cacheRatios := s.routingCacheRatios(routing, cacheRoutes)
	out := make([]Candidate, 0, len(channels))
	exclusions := make([]CandidateExclusion, 0)
	for _, ch := range channels {
		if !channelAvailable(ch) && !(allowBalanceBlocked && !modelProbeChannelBlocked(ch)) {
			continue
		}
		rates := ratesByChannel[ch.ID]
		for _, rate := range rates {
			if strings.TrimSpace(rate.ModelName) == "" {
				continue
			}
			routeKey := canonicalCandidateKey(ch.ID, rate.ModelName)
			upstreamKey := upstreamByRoute[routeKey]
			upstreamGeneration, generationExists := generationByRoute[routeKey]
			if !generationExists || !upstreamGeneration.Valid() {
				exclusions = append(exclusions, newCandidateExclusion(Candidate{Channel: ch, Rate: rate},
					candidateExclusionReasonUpstreamGenerationUnavailable,
					"current upstream group generation is unavailable"))
				continue
			}
			healthFence, hasHealthFence := RoutingHealthFenceForCandidate(Candidate{
				Channel: ch, Rate: rate, UpstreamGeneration: upstreamGeneration,
			})
			if !hasHealthFence {
				exclusions = append(exclusions, newCandidateExclusion(Candidate{Channel: ch, Rate: rate},
					candidateExclusionReasonUpstreamGenerationUnavailable,
					"current upstream routing identity is unavailable"))
				continue
			}
			modelFence := storage.NewGatewayModelFactFence(upstreamGeneration, ch.UpstreamAccountIdentityRevision)
			var rateInfo cachedGatewayAppInfo
			var ok bool
			if ch.CredentialMode == storage.CredentialModeDirect {
				rateInfo, ok = cachedDirectGatewayAppFromKey(&upstreamKey, ch, app, healthFence)
			} else {
				rateInfo, ok = cachedGatewayAppFromKeyForIdentity(&upstreamKey, app, modelFence, healthFence)
			}
			if !ok {
				continue
			}
			if rateInfo.App != app {
				continue
			}
			if ch.CredentialMode != storage.CredentialModeDirect && modelFamily != "" && !cachedGatewayFamilyMatches(rateInfo.ModelFamilies, modelFamily) {
				continue
			}
			if allowed != nil {
				if _, ok := allowed[routeKey]; !ok {
					continue
				}
			}
			latency := latencyForCandidate(latencies, ch.ID, strings.TrimSpace(rate.ModelName), routing.LatencySamples)
			rawRatio := rate.Ratio
			rawCompletionRatio := rate.CompletionRatio
			funding := fundingFactors[ch.ID]
			if ch.CredentialMode == storage.CredentialModeDirect {
				funding = channelFundingSnapshot{factor: 1, source: "direct", trusted: true}
			}
			if !funding.trusted && !allowUntrustedCost {
				exclusions = append(exclusions, CandidateExclusion{
					ChannelID:   ch.ID,
					ChannelName: ch.Name,
					GroupName:   strings.TrimSpace(rate.ModelName),
					Reason:      "cost_basis_needs_review",
				})
				continue
			}
			fundingFactor, costBasisVersion := funding.factor, funding.version
			if fundingFactor < 0 {
				fundingFactor = 1
			}
			rate.Ratio = rawRatio * fundingFactor
			if rawCompletionRatio > 0 {
				rate.CompletionRatio = rawCompletionRatio * fundingFactor
			}
			cacheRatio, cacheRatioKnown := cacheRatioForCandidate(cacheRatios, routeKey)
			var accountConcurrency *storage.UpstreamAccountConcurrencySnapshot
			if snapshot, exists := accountConcurrencyByChannel[ch.ID]; exists {
				snapshot := snapshot
				accountConcurrency = validatedUpstreamAccountConcurrencySnapshot(ch, &snapshot)
			}
			candidate := Candidate{
				Channel:                    ch,
				Rate:                       rate,
				UpstreamGeneration:         upstreamGeneration,
				ModelFactGeneration:        upstreamGeneration,
				KeyStateGeneration:         upstreamGeneration,
				UpstreamAccountConcurrency: accountConcurrency,
				App:                        rateInfo.App,
				ModelFamilies:              append([]string(nil), rateInfo.ModelFamilies...),
				ModelIDs:                   rateInfo.ModelIDs,
				DirectBaseURL:              rateInfo.DirectBaseURL,
				LatencyProbeModel:          rateInfo.LatencyProbeModel,
				ModelsSyncedAt:             rateInfo.ModelsSyncedAt,
				ModelsError:                rateInfo.ModelsError,
				EffectivePrice:             effectiveCandidatePriceWithCache(rate, cacheRatio, routing.CacheInputPriceMultiplier),
				CacheRatio:                 cacheRatio,
				CacheRatioKnown:            cacheRatioKnown,
				AvgLatencyMs:               latency.AvgMs,
				LatencySamples:             latency.Samples,
				ProbeEstimatedCostUSD:      estimateProbeCost(rate, rateInfo.ModelIDs),
				RawRatio:                   rawRatio,
				RawCompletionRatio:         rawCompletionRatio,
				FundingFactor:              fundingFactor,
				CostBasisVersion:           costBasisVersion,
				CostBasisSource:            funding.source,
				DisabledForKey:             routeExcluded(disabled, ch.ID, strings.TrimSpace(rate.ModelName)),
				ProbeDisabled:              !rateInfo.ProbeEnabled,
			}
			if _, unavailable := gatewayCandidateTransientFailure(rateInfo, time.Now()); unavailable {
				candidate.CoolingDown = true
				candidate.FailureCount = rateInfo.RoutingFailureCount
				candidate.LastRoutingFailureAt = rateInfo.LastRoutingFailureAt
				candidate.CooldownUntil = gatewayCandidateTransientCooldownUntil(rateInfo)
				if candidate.CooldownUntil != nil {
					candidate.CooldownRemaining = time.Until(*candidate.CooldownUntil)
				}
			}
			// Disabled routes are deliberately outside model capability diagnostics.
			if candidate.DisabledForKey {
				if includeDisabled {
					out = append(out, candidate)
				}
				continue
			}
			if detail, unavailable := gatewayCandidateTransientFailure(rateInfo, time.Now()); unavailable && !includeRecentlyFailed {
				exclusions = append(exclusions, newCandidateExclusion(candidate,
					candidateExclusionReasonUpstreamTemporarilyUnavailable, detail))
				if includeDisabled {
					out = append(out, candidate)
				}
				continue
			}
			modelListed := modelAvailableForChannel(ch, rateInfo.ModelIDs, model, app)
			if model != "" && (len(rateInfo.ModelIDs) > 0 || ch.CredentialMode == storage.CredentialModeDirect) && !modelListed {
				exclusions = append(exclusions, newCandidateExclusion(candidate,
					candidateExclusionReasonModelNotListed, model))
				continue
			}
			if model != "" && s.Store != nil {
				if _, unsupported := unsupportedRoutes[routeKey]; unsupported {
					exclusions = append(exclusions, newCandidateExclusion(candidate,
						candidateExclusionReasonModelNotSupported, model))
					if includeDisabled {
						out = append(out, candidate)
					}
					continue
				}
			}
			out = append(out, candidate)
		}
	}
	s.applyRoutingModelPrices(out, exclusions, model)
	scoreCandidatesWithConfig(out, routing)
	sortCandidates(out)
	exclusions = rankedModelExclusions(out, exclusions, routing)
	if len(out) == 0 {
		if model != "" {
			return nil, exclusions, fmt.Errorf("no available %s upstream channel has rate snapshots for model %q", app, model)
		}
		return nil, exclusions, fmt.Errorf("no available %s upstream channel has rate snapshots", app)
	}
	return out, exclusions, nil
}

func (s *Service) ManualCandidateForApp(downstreamKeyID uint, app storage.GatewayApp, channelID uint, groupName string, model string) ([]Candidate, error) {
	var key *storage.GatewayDownstreamKey
	if downstreamKeyID > 0 {
		if s.Store == nil {
			return nil, errors.New("gateway store is unavailable")
		}
		var err error
		key, err = s.Store.FindDownstreamKey(downstreamKeyID)
		if err != nil {
			return nil, fmt.Errorf("find downstream key %d: %w", downstreamKeyID, err)
		}
	}
	return s.manualCandidateForKey(key, app, channelID, groupName, model)
}

func (s *Service) validateManualRouteReference(key *storage.GatewayDownstreamKey) error {
	return s.validateManualRouteReferenceWithStore(s.Store, key)
}

func (s *Service) validateManualRouteReferenceWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey) error {
	if key == nil {
		return manualRouteGateError(ManualRouteGateReasonTargetRequired, "manual route requires downstream key")
	}
	_, err := s.manualRouteCandidateWithStore(store, key, key.App, key.ManualChannelID, key.ManualGroupName)
	return err
}

// ManualMappedTargetGateProjectionForDownstreamKey resolves the configured
// fixed target and, only for a persisted manual mapping, evaluates its exact
// persisted model. It is intentionally key-only: the target and model are
// always loaded from storage, never accepted from a browser preview request.
//
// Non-manual strategies still receive baseline fixed-target facts (for
// example a missing target or a known protocol mismatch), but this service
// cannot safely infer an IQ/cost/time/balanced model without the API layer's
// server-side resolver, so ManualMappedModelGate remains false for them.
func (s *Service) ManualMappedTargetGateProjectionForDownstreamKey(downstreamKeyID uint) (*ManualMappedTargetGateProjection, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if downstreamKeyID == 0 {
		return nil, errors.New("downstream key is required")
	}
	key, err := s.Store.FindDownstreamKey(downstreamKeyID)
	if err != nil {
		return nil, fmt.Errorf("find downstream key %d: %w", downstreamKeyID, err)
	}
	return s.manualMappedTargetGateProjectionWithStore(s.Store, key)
}

func (s *Service) validateManualMappedTargetWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey) error {
	projection, err := s.manualMappedTargetGateProjectionWithStore(store, key)
	if err != nil {
		return err
	}
	if !projection.StructuralBlocked {
		return nil
	}
	return manualRouteGateError(projection.Reason, "manual mapped route target is structurally blocked")
}

func (s *Service) validateManualMappedTargetWithAutomaticResolver(store *storage.Gateway, key *storage.GatewayDownstreamKey, resolver AutomaticMappedModelResolver) error {
	if key == nil {
		return errors.New("downstream key is required")
	}
	strategy, err := NormalizeModelMappingStrategy(key.ModelMappingStrategy)
	if err != nil || strategy == ModelMappingStrategyDisabled || strategy == ModelMappingStrategyManual || resolver == nil {
		return s.validateManualMappedTargetWithStore(store, key)
	}
	if store == nil || key.CandidateGroupID == 0 {
		return s.validateManualMappedTargetWithStore(store, key)
	}
	group, err := store.FindCandidateGroup(key.CandidateGroupID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return manualRouteGateError(ManualRouteGateReasonCandidateGroupMissing, "manual mapped target candidate group no longer exists")
		}
		return fmt.Errorf("find candidate group %d: %w", key.CandidateGroupID, err)
	}
	mappedModel, resolved := resolver(key, group, strategy)
	mappedModel = strings.TrimSpace(mappedModel)
	if !resolved || mappedModel == "" {
		return s.validateManualMappedTargetWithStore(store, key)
	}
	projection, err := s.manualMappedTargetGateProjectionForResolvedModelWithStore(
		store,
		key,
		key.ManualChannelID,
		key.ManualGroupName,
		mappedModel,
		true,
	)
	if err != nil {
		return err
	}
	if !projection.StructuralBlocked {
		return nil
	}
	return manualRouteGateError(projection.Reason, "automatic mapped route target is structurally blocked")
}

func (s *Service) manualMappedTargetGateProjectionWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey) (*ManualMappedTargetGateProjection, error) {
	if key == nil {
		return nil, errors.New("downstream key is required")
	}
	if key.SmartRouting {
		return &ManualMappedTargetGateProjection{}, nil
	}
	return s.manualMappedTargetGateProjectionForTargetWithStore(store, key, key.ManualChannelID, key.ManualGroupName)
}

// ManualMappedTargetGateProjectionsForDownstreamKey resolves the current
// persisted candidate-group members for one key. Each result is keyed by its
// stable channel ID and canonical group name; callers do not submit a target,
// model, or identity scope. This lets every fixed-route entry point consume
// the same server-side exact-model gate without deriving it from a recent or
// requested browser model.
func (s *Service) ManualMappedTargetGateProjectionsForDownstreamKey(downstreamKeyID uint) ([]ManualMappedTargetGateProjection, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if downstreamKeyID == 0 {
		return nil, errors.New("downstream key is required")
	}
	key, err := s.Store.FindDownstreamKey(downstreamKeyID)
	if err != nil {
		return nil, fmt.Errorf("find downstream key %d: %w", downstreamKeyID, err)
	}
	return s.manualMappedTargetGateProjectionsWithStore(s.Store, key)
}

// ManualMappedTargetGateProjectionsForDownstreamKeyWithServerModel applies a
// model chosen by trusted server-side mapping resolution to every persisted
// candidate member. It exists for automatic mapping strategies only: HTTP
// handlers must derive model from their own server resolver and must never
// forward a browser-supplied model into this method.
//
// The caller is responsible for enforcing downstream app/model-family model
// admission before calling this method. A non-empty model here therefore has
// server-owned exact-model provenance and sets ManualMappedModelGate on every
// returned projection, including targets where it is currently allowed.
func (s *Service) ManualMappedTargetGateProjectionsForDownstreamKeyWithServerModel(downstreamKeyID uint, model string) ([]ManualMappedTargetGateProjection, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if downstreamKeyID == 0 {
		return nil, errors.New("downstream key is required")
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, errors.New("server mapped model is required")
	}
	key, err := s.Store.FindDownstreamKey(downstreamKeyID)
	if err != nil {
		return nil, fmt.Errorf("find downstream key %d: %w", downstreamKeyID, err)
	}
	return s.manualMappedTargetGateProjectionsForModelWithStore(s.Store, key, model, true, false)
}

func (s *Service) manualMappedTargetGateProjectionsWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey) ([]ManualMappedTargetGateProjection, error) {
	if key == nil {
		return nil, errors.New("downstream key is required")
	}
	if store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if key.CandidateGroupID == 0 {
		return []ManualMappedTargetGateProjection{}, nil
	}
	group, err := store.FindCandidateGroup(key.CandidateGroupID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, manualRouteGateError(ManualRouteGateReasonCandidateGroupMissing, "manual mapped target candidate group no longer exists")
		}
		return nil, fmt.Errorf("find candidate group %d: %w", key.CandidateGroupID, err)
	}
	model, manualMapping, mappingErr := persistedManualMappedModelForCandidateGroup(key, group)
	mappingUnresolved := mappingErr != nil
	if mappingUnresolved {
		// A legacy/incomplete manual mapping must not turn an administrative
		// preview into an internal-error response, nor may it cause the client
		// to guess a model. Preserve baseline target facts and make the
		// unresolved state explicit for the API contract.
		model = ""
		manualMapping = false
	}
	return s.manualMappedTargetGateProjectionsForLoadedGroupWithStore(store, key, group, model, manualMapping, mappingUnresolved)
}

func (s *Service) manualMappedTargetGateProjectionsForModelWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey, model string, mappedModel bool, mappingUnresolved bool) ([]ManualMappedTargetGateProjection, error) {
	if key == nil {
		return nil, errors.New("downstream key is required")
	}
	if store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if key.CandidateGroupID == 0 {
		return []ManualMappedTargetGateProjection{}, nil
	}
	group, err := store.FindCandidateGroup(key.CandidateGroupID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, manualRouteGateError(ManualRouteGateReasonCandidateGroupMissing, "manual mapped target candidate group no longer exists")
		}
		return nil, fmt.Errorf("find candidate group %d: %w", key.CandidateGroupID, err)
	}
	return s.manualMappedTargetGateProjectionsForLoadedGroupWithStore(store, key, group, strings.TrimSpace(model), mappedModel, mappingUnresolved)
}

// manualMappedTargetGateProjectionsForLoadedGroupWithStore is the bounded
// member projection path: one candidate-group read has already occurred, then
// it performs at most four further bulk reads (channels, upstream keys,
// current generations, and—when an exact mapped model exists—capabilities).
// It intentionally excludes rates, account concurrency, probe state, and
// adaptive health because none are structural fixed-target gates.
func (s *Service) manualMappedTargetGateProjectionsForLoadedGroupWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey, group *storage.GatewayCandidateGroup, model string, mappedModel, mappingUnresolved bool) ([]ManualMappedTargetGateProjection, error) {
	if key == nil || group == nil {
		return nil, errors.New("downstream key candidate group is required")
	}
	facts, err := s.loadManualMappedTargetGateBatchFacts(store, group.Members, model, mappedModel)
	if err != nil {
		return nil, err
	}
	app := normalizeGatewayApp(key.App)
	projections := make([]ManualMappedTargetGateProjection, 0, len(group.Members))
	for _, member := range group.Members {
		projection := manualMappedTargetGateProjectionFromBatchFacts(member, app, model, mappedModel, mappingUnresolved, facts)
		projections = append(projections, projection)
	}
	return projections, nil
}

type manualMappedTargetGateBatchFacts struct {
	channels          map[uint]storage.Channel
	upstreamByRoute   map[candidateLatencyKey]storage.GatewayUpstreamKey
	generationByRoute map[candidateLatencyKey]storage.UpstreamGroupGeneration
	capabilityByRoute map[candidateLatencyKey]storage.GatewayModelCapability
}

func (s *Service) loadManualMappedTargetGateBatchFacts(store *storage.Gateway, members []storage.GatewayCandidateGroupMember, model string, mappedModel bool) (manualMappedTargetGateBatchFacts, error) {
	var facts manualMappedTargetGateBatchFacts
	if s == nil || s.Channels == nil || store == nil {
		return facts, errors.New("gateway stores are unavailable")
	}
	channelIDs := make([]uint, 0, len(members))
	seenChannels := make(map[uint]struct{}, len(members))
	for _, member := range members {
		if member.ChannelID == 0 {
			continue
		}
		if _, seen := seenChannels[member.ChannelID]; seen {
			continue
		}
		seenChannels[member.ChannelID] = struct{}{}
		channelIDs = append(channelIDs, member.ChannelID)
	}
	channels, err := s.Channels.ListByIDs(channelIDs)
	if err != nil {
		return facts, fmt.Errorf("list mapped target channels: %w", err)
	}
	facts.channels = make(map[uint]storage.Channel, len(channels))
	for _, channel := range channels {
		facts.channels[channel.ID] = channel
	}
	upstreamKeys, err := store.ListUpstreamKeys(channelIDs)
	if err != nil {
		return facts, fmt.Errorf("list mapped target upstream keys: %w", err)
	}
	facts.upstreamByRoute = make(map[candidateLatencyKey]storage.GatewayUpstreamKey, len(upstreamKeys))
	for _, upstream := range upstreamKeys {
		route := canonicalCandidateKey(upstream.ChannelID, upstream.GroupName)
		current, exists := facts.upstreamByRoute[route]
		if !exists || upstreamKeyRoutePreference(upstream, current) {
			facts.upstreamByRoute[route] = upstream
		}
	}
	generations, err := store.ListUpstreamGroupGenerations(channelIDs)
	if err != nil {
		return facts, fmt.Errorf("list mapped target upstream generations: %w", err)
	}
	facts.generationByRoute = make(map[candidateLatencyKey]storage.UpstreamGroupGeneration, len(generations))
	for _, generation := range generations {
		facts.generationByRoute[canonicalCandidateKey(generation.ChannelID, generation.CanonicalGroupName)] = generation
	}
	facts.capabilityByRoute = make(map[candidateLatencyKey]storage.GatewayModelCapability)
	if !mappedModel || strings.TrimSpace(model) == "" {
		return facts, nil
	}
	capabilityRoutes := make([]storage.GatewayModelCapabilityRoute, 0, len(members))
	for _, member := range members {
		capabilityRoutes = append(capabilityRoutes, storage.GatewayModelCapabilityRoute{
			ChannelID: member.ChannelID,
			GroupName: member.GroupName,
		})
	}
	capabilities, err := store.ListModelCapabilitiesForRoutes(model, capabilityRoutes, modelCapabilityMaxAge)
	if err != nil {
		return facts, fmt.Errorf("list mapped target model capabilities: %w", err)
	}
	for _, capability := range capabilities {
		route := canonicalCandidateKey(capability.ChannelID, capability.GroupName)
		current, exists := facts.capabilityByRoute[route]
		if !exists || capability.LastCheckedAt.After(current.LastCheckedAt) || (capability.LastCheckedAt.Equal(current.LastCheckedAt) && capability.ID > current.ID) {
			facts.capabilityByRoute[route] = capability
		}
	}
	return facts, nil
}

func manualMappedTargetGateProjectionFromBatchFacts(member storage.GatewayCandidateGroupMember, app storage.GatewayApp, model string, mappedModel, mappingUnresolved bool, facts manualMappedTargetGateBatchFacts) ManualMappedTargetGateProjection {
	projection := ManualMappedTargetGateProjection{
		CandidateMemberID:     member.ID,
		ChannelID:             member.ChannelID,
		CanonicalGroupName:    canonicalGroupName(member.GroupName),
		ManualMappedModelGate: mappedModel,
		MappingUnresolved:     mappingUnresolved,
	}
	block := func(reason ManualRouteGateReason) ManualMappedTargetGateProjection {
		projection.Reason = reason
		projection.StructuralBlocked = true
		return projection
	}
	if projection.ChannelID == 0 || projection.CanonicalGroupName == "" {
		return block(ManualRouteGateReasonTargetRequired)
	}
	channel, exists := facts.channels[projection.ChannelID]
	if !exists || channel.UpstreamAccountIdentityRevision == 0 {
		return block(ManualRouteGateReasonChannelMissing)
	}
	route := canonicalCandidateKey(projection.ChannelID, projection.CanonicalGroupName)
	upstream, exists := facts.upstreamByRoute[route]
	if !exists {
		return block(ManualRouteGateReasonUpstreamKeyMissing)
	}
	if !upstream.Enabled {
		return block(ManualRouteGateReasonUpstreamKeyDisabled)
	}
	if upstream.Provisioning {
		return block(ManualRouteGateReasonUpstreamKeyUnavailable)
	}
	if strings.TrimSpace(upstream.APIKeyCipher) == "" {
		return block(ManualRouteGateReasonUpstreamKeyMissing)
	}
	generation, exists := facts.generationByRoute[route]
	if !exists || !generation.Valid() {
		return block(ManualRouteGateReasonGenerationUnavailable)
	}
	modelFence := storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision)
	families, modelIDs, trusted := manualRouteCurrentSynchronizedFactsForIdentity(&upstream, generation, modelFence)
	if channel.CredentialMode == storage.CredentialModeDirect {
		if normalizeGatewayApp(channel.GatewayApp) != normalizeGatewayApp(app) {
			return block(ManualRouteGateReasonProtocolMismatch)
		}
		if !trusted || len(modelIDs) == 0 {
			return block(ManualRouteGateReasonModelNotListed)
		}
		if mappedModel && strings.TrimSpace(model) != "" && !directModelIDSupportsRequest(modelIDs, model) {
			return block(ManualRouteGateReasonModelNotListed)
		}
	} else if trusted {
		if known, compatible := manualRouteProtocolCompatibility(families, app); known && !compatible {
			return block(ManualRouteGateReasonProtocolMismatch)
		}
		if mappedModel && strings.TrimSpace(model) != "" && len(modelIDs) > 0 && !modelIDSupportsRequest(modelIDs, model, app) {
			return block(ManualRouteGateReasonModelNotListed)
		}
	}
	if !mappedModel || strings.TrimSpace(model) == "" {
		return projection
	}
	if capability, exists := facts.capabilityByRoute[route]; exists && modelFence.MatchesCapability(&capability) &&
		!capability.Supported && !capability.LastCheckedAt.Before(generation.DiscoveredAt) {
		return block(ManualRouteGateReasonModelNotSupported)
	}
	return projection
}

func (s *Service) manualMappedTargetGateProjectionForTargetWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey, channelID uint, groupName string) (*ManualMappedTargetGateProjection, error) {
	if key == nil {
		return nil, errors.New("downstream key is required")
	}
	model, manualMapping, err := s.persistedManualMappedModelWithStore(store, key)
	if err != nil {
		return nil, err
	}
	return s.manualMappedTargetGateProjectionForResolvedModelWithStore(store, key, channelID, groupName, model, manualMapping)
}

func (s *Service) manualMappedTargetGateProjectionForResolvedModelWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey, channelID uint, groupName, model string, manualMapping bool) (*ManualMappedTargetGateProjection, error) {
	return s.manualMappedTargetGateProjectionForResolvedModelWithMembership(store, key, channelID, groupName, model, manualMapping, nil)
}

func (s *Service) manualMappedTargetGateProjectionForResolvedModelWithMembership(store *storage.Gateway, key *storage.GatewayDownstreamKey, channelID uint, groupName, model string, manualMapping bool, membership *manualRouteCandidateMembership) (*ManualMappedTargetGateProjection, error) {
	if key == nil {
		return nil, errors.New("downstream key is required")
	}
	projection := &ManualMappedTargetGateProjection{
		ChannelID:             channelID,
		CanonicalGroupName:    canonicalGroupName(groupName),
		ManualMappedModelGate: manualMapping,
	}
	requestedModels := make([]string, 0, 1)
	if manualMapping {
		requestedModels = append(requestedModels, model)
	}
	_, err := s.manualRouteCandidateWithStoreAndMembership(store, key, key.App, channelID, groupName, membership, requestedModels...)
	if err == nil {
		return projection, nil
	}
	if reason, ok := ManualRouteGateReasonOf(err); ok {
		projection.Reason = reason
		projection.StructuralBlocked = true
		return projection, nil
	}
	return nil, err
}

func persistedManualMappedModel(key *storage.GatewayDownstreamKey) (model string, manual bool, err error) {
	if key == nil {
		return "", false, errors.New("downstream key is required")
	}
	strategy, err := NormalizeModelMappingStrategy(key.ModelMappingStrategy)
	if err != nil {
		return "", false, err
	}
	if strategy != ModelMappingStrategyManual {
		return "", false, nil
	}
	model = strings.TrimSpace(key.ModelMappingModel)
	if model == "" {
		return "", true, errors.New("manual model mapping requires a model")
	}
	if strings.TrimSpace(key.ModelMappingReasoningEffort) == "" {
		return "", true, errors.New("manual model mapping requires a reasoning effort")
	}
	if _, err := NormalizeModelMappingReasoningEffort(key.App, key.ModelMappingReasoningEffort); err != nil {
		return "", true, err
	}
	return model, true, nil
}

func persistedManualMappedModelForCandidateGroup(key *storage.GatewayDownstreamKey, group *storage.GatewayCandidateGroup) (model string, manual bool, err error) {
	model, manual, err = persistedManualMappedModel(key)
	if err != nil || !manual {
		return model, manual, err
	}
	if group == nil || key.CandidateGroupID == 0 || group.ID != key.CandidateGroupID {
		return "", true, errors.New("manual model mapping requires a candidate group")
	}
	family, familyErr := NormalizeModelMappingFamily(group.ModelMappingFamily)
	if familyErr != nil || family == "" {
		return "", true, errors.New("candidate group must select codex or claude radar")
	}
	if key.App != storage.GatewayApp(family) || !strings.EqualFold(strings.TrimSpace(key.ModelFamily), family) {
		return "", true, errors.New("model mapping key and candidate group must use the same model family")
	}
	return model, true, nil
}

func (s *Service) persistedManualMappedModelWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey) (model string, manual bool, err error) {
	model, manual, err = persistedManualMappedModel(key)
	if err != nil || !manual {
		return model, manual, err
	}
	if store == nil {
		return "", true, errors.New("gateway store is unavailable")
	}
	if key.CandidateGroupID == 0 {
		return "", true, errors.New("manual model mapping requires a candidate group")
	}
	group, findErr := store.FindCandidateGroup(key.CandidateGroupID)
	if findErr != nil {
		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			return "", true, manualRouteGateError(ManualRouteGateReasonCandidateGroupMissing, "manual mapped target candidate group no longer exists")
		}
		return "", true, fmt.Errorf("find candidate group %d: %w", key.CandidateGroupID, findErr)
	}
	return persistedManualMappedModelForCandidateGroup(key, group)
}

func (s *Service) manualCandidateForKey(key *storage.GatewayDownstreamKey, app storage.GatewayApp, channelID uint, groupName string, model string) ([]Candidate, error) {
	candidate, err := s.manualRouteCandidateWithStore(s.Store, key, app, channelID, groupName, model)
	if err != nil {
		return nil, err
	}
	return []Candidate{candidate}, nil
}

// manualRouteCandidateMembership is a service-owned snapshot of an already
// loaded candidate group. It is only used by read-only member projections to
// avoid re-reading that same group once per member. Fixed-route writes and the
// data plane deliberately pass nil and revalidate current membership.
type manualRouteCandidateMembership struct {
	candidateGroupID uint
	members          map[candidateLatencyKey]struct{}
}

func manualRouteCandidateMembershipForGroup(group *storage.GatewayCandidateGroup) *manualRouteCandidateMembership {
	if group == nil || group.ID == 0 {
		return nil
	}
	membership := &manualRouteCandidateMembership{
		candidateGroupID: group.ID,
		members:          make(map[candidateLatencyKey]struct{}, len(group.Members)),
	}
	for _, member := range group.Members {
		membership.members[canonicalCandidateKey(member.ChannelID, member.GroupName)] = struct{}{}
	}
	return membership
}

// manualRouteCandidateWithStore resolves one exact operator-selected route.
// It deliberately bypasses probe switches, route exclusions, latency/price
// scores and adaptive-routing health. It does retain known protocol and exact
// model incompatibilities: those are structural target errors, not smart
// routing policy.
//
// A rate snapshot is deliberately not structural. It is only a pricing and
// smart-scoring observation, so a missing snapshot must not make a configured
// fixed target disappear. In that degraded case the returned candidate has a
// zero ratio and no upstream generation: it may be forwarded once, but cannot
// fabricate pricing or an unfenced shared-health success.
func (s *Service) manualRouteCandidateWithStore(store *storage.Gateway, key *storage.GatewayDownstreamKey, app storage.GatewayApp, channelID uint, groupName string, requestedModels ...string) (Candidate, error) {
	return s.manualRouteCandidateWithStoreAndMembership(store, key, app, channelID, groupName, nil, requestedModels...)
}

func (s *Service) manualRouteCandidateWithStoreAndMembership(store *storage.Gateway, key *storage.GatewayDownstreamKey, app storage.GatewayApp, channelID uint, groupName string, membership *manualRouteCandidateMembership, requestedModels ...string) (Candidate, error) {
	var candidate Candidate
	groupName = strings.TrimSpace(groupName)
	requestedModel := ""
	if len(requestedModels) > 0 {
		requestedModel = strings.TrimSpace(requestedModels[0])
	}
	if channelID == 0 || groupName == "" {
		return candidate, manualRouteGateError(ManualRouteGateReasonTargetRequired, "manual route requires channel and group")
	}
	if store == nil || s == nil || s.Channels == nil || s.Rates == nil {
		return candidate, errors.New("gateway stores are unavailable")
	}
	app = normalizeGatewayApp(app)
	if key != nil {
		app = normalizeGatewayApp(key.App)
		if key.CandidateGroupID > 0 {
			memberFound := false
			if membership != nil && membership.candidateGroupID == key.CandidateGroupID {
				_, memberFound = membership.members[canonicalCandidateKey(channelID, groupName)]
			} else {
				group, err := store.FindCandidateGroup(key.CandidateGroupID)
				if err != nil {
					if errors.Is(err, gorm.ErrRecordNotFound) {
						return candidate, manualRouteGateError(ManualRouteGateReasonCandidateGroupMissing, "manual route candidate group no longer exists")
					}
					return candidate, fmt.Errorf("find candidate group %d: %w", key.CandidateGroupID, err)
				}
				for _, member := range group.Members {
					if member.ChannelID == channelID && canonicalGroupName(member.GroupName) == canonicalGroupName(groupName) {
						memberFound = true
						break
					}
				}
			}
			if !memberFound {
				return candidate, manualRouteGateError(ManualRouteGateReasonCandidateGroupMismatch, "manual route target is not in the downstream key candidate group")
			}
		}
	}

	channel, err := s.Channels.FindByID(channelID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return candidate, manualRouteGateError(ManualRouteGateReasonChannelMissing, "manual route channel no longer exists")
		}
		return candidate, fmt.Errorf("find manual route channel %d: %w", channelID, err)
	}
	rate := &storage.RateSnapshot{ChannelID: channelID, ModelName: groupName}
	hasRateSnapshot := false
	if currentRate, rateErr := s.Rates.FindByChannelAndModel(channelID, groupName); rateErr == nil {
		rate = currentRate
		hasRateSnapshot = true
	} else if !errors.Is(rateErr, gorm.ErrRecordNotFound) {
		return candidate, fmt.Errorf("find manual route rate snapshot %d/%s: %w", channelID, groupName, rateErr)
	}
	upstreamKey, err := store.FindUpstreamKeyAny(channelID, groupName)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return candidate, manualRouteGateError(ManualRouteGateReasonUpstreamKeyMissing, "manual route target has no upstream key")
		}
		return candidate, fmt.Errorf("find manual route upstream key %d/%s: %w", channelID, groupName, err)
	}
	if !upstreamKey.Enabled {
		return candidate, manualRouteGateError(ManualRouteGateReasonUpstreamKeyDisabled, "manual route upstream key is disabled")
	}
	if upstreamKey.Provisioning {
		return candidate, manualRouteGateError(ManualRouteGateReasonUpstreamKeyUnavailable, "manual route upstream key is still provisioning")
	}
	if strings.TrimSpace(upstreamKey.APIKeyCipher) == "" {
		return candidate, manualRouteGateError(ManualRouteGateReasonUpstreamKeyMissing, "manual route upstream key material is missing")
	}
	generation, generationErr := store.CaptureUpstreamGroupGeneration(channelID, groupName)
	if generationErr != nil {
		if errors.Is(generationErr, gorm.ErrRecordNotFound) {
			return Candidate{}, manualRouteGateError(ManualRouteGateReasonGenerationUnavailable, "manual route group has no current generation")
		}
		return Candidate{}, fmt.Errorf("capture current manual route group generation %d/%s: %w", channelID, groupName, generationErr)
	}
	if !generation.Valid() {
		return Candidate{}, manualRouteGateError(ManualRouteGateReasonGenerationUnavailable, "manual route group has no valid current generation")
	}
	modelFence := storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision)
	modelFamilies, modelIDs, factsTrusted := manualRouteCurrentSynchronizedFactsForIdentity(upstreamKey, generation, modelFence)
	if channel.CredentialMode == storage.CredentialModeDirect {
		if normalizeGatewayApp(channel.GatewayApp) != app {
			return candidate, manualRouteGateError(ManualRouteGateReasonProtocolMismatch, "manual route target does not support the downstream protocol")
		}
		// Direct routing is administrator-owned. The current credential
		// projection, rather than a potentially stale model-sync snapshot, is
		// authoritative for the exact allowlist and endpoint identity.
		projectedModels, projected := directProjectedModels(*channel, *upstreamKey)
		if !projected || len(projectedModels) == 0 {
			return candidate, manualRouteGateError(ManualRouteGateReasonModelNotListed, "manual route target has an invalid direct model allowlist")
		}
		modelIDs = projectedModels
		modelFamilies = directChannelModelFamilies(channel, projectedModels)
		candidate.DirectBaseURL = strings.TrimSpace(upstreamKey.DirectBaseURL)
		if requestedModel != "" && !directModelIDSupportsRequest(modelIDs, requestedModel) {
			return candidate, manualRouteGateError(ManualRouteGateReasonModelNotListed, "manual route target does not allow the requested model")
		}
	} else if factsTrusted {
		if known, compatible := manualRouteProtocolCompatibility(modelFamilies, app); known && !compatible {
			return candidate, manualRouteGateError(ManualRouteGateReasonProtocolMismatch, "manual route target does not support the downstream protocol")
		}
		if requestedModel != "" && len(modelIDs) > 0 && !modelIDSupportsRequest(modelIDs, requestedModel, app) {
			return candidate, manualRouteGateError(ManualRouteGateReasonModelNotListed, "manual route target does not advertise the requested model")
		}
	}
	accountConcurrency, err := s.Channels.UpstreamAccountConcurrencyForChannel(channelID)
	if err != nil {
		return candidate, fmt.Errorf("load manual route account concurrency %d/%s: %w", channelID, groupName, err)
	}
	accountConcurrency = validatedUpstreamAccountConcurrencySnapshot(*channel, accountConcurrency)

	// Preserve no-rate as a pricing-unavailable fact. Reusing the key's old
	// group ratio would turn a missing current snapshot into a false estimate.
	if !hasRateSnapshot {
		rate = &storage.RateSnapshot{ChannelID: channelID, ModelName: strings.TrimSpace(upstreamKey.GroupName)}
		if strings.TrimSpace(rate.ModelName) == "" {
			rate.ModelName = groupName
		}
	}
	rawRatio := rate.Ratio
	rawCompletionRatio := rate.CompletionRatio
	funding := s.channelFundingFactors([]storage.Channel{*channel})[channel.ID]
	fundingFactor := funding.factor
	if fundingFactor < 0 {
		fundingFactor = 1
	}
	rate.Ratio = rawRatio * fundingFactor
	if rawCompletionRatio > 0 {
		rate.CompletionRatio = rawCompletionRatio * fundingFactor
	}
	candidate = Candidate{
		Channel:                    *channel,
		Rate:                       *rate,
		UpstreamAccountConcurrency: accountConcurrency,
		App:                        app,
		ModelFactGeneration:        generation,
		KeyStateGeneration:         generation,
		ModelFamilies:              modelFamilies,
		ModelIDs:                   modelIDs,
		LatencyProbeModel:          strings.TrimSpace(upstreamKey.LatencyProbeModel),
		ModelsSyncedAt:             upstreamKey.ModelsSyncedAt,
		ModelsError:                upstreamKey.ModelsError,
		EffectivePrice:             effectiveCandidatePriceWithCache(*rate, 0, s.routingConfig().CacheInputPriceMultiplier),
		ProbeEstimatedCostUSD:      estimateProbeCost(*rate, nil),
		RawRatio:                   rawRatio,
		RawCompletionRatio:         rawCompletionRatio,
		FundingFactor:              fundingFactor,
		CostBasisVersion:           funding.version,
		CostBasisSource:            funding.source,
		ProbeDisabled:              !upstreamKey.LatencyProbeEnabled,
	}
	if hasRateSnapshot {
		candidate.UpstreamGeneration = generation
	}
	if requestedModel != "" {
		capability, capabilityErr := store.FindModelCapabilityForIdentity(modelFence, requestedModel)
		if capabilityErr != nil {
			return Candidate{}, fmt.Errorf("load manual route model capability: %w", capabilityErr)
		}
		if capability != nil && !capability.Supported &&
			time.Since(capability.LastCheckedAt) <= modelCapabilityMaxAge &&
			!capability.LastCheckedAt.Before(generation.DiscoveredAt) {
			return Candidate{}, manualRouteGateError(ManualRouteGateReasonModelNotSupported, "manual route target is confirmed not to support the requested model")
		}
	}
	healthInfo := cachedGatewayAppInfo{}
	if fence, ok := RoutingHealthFenceForCandidate(candidate); ok && fence.MatchesUpstreamKey(upstreamKey) {
		healthInfo.LastRoutingFailureKind = upstreamKey.LastRoutingFailureKind
		healthInfo.LastRoutingFailureStatus = upstreamKey.LastRoutingFailureStatus
		healthInfo.LastRoutingFailureAt = upstreamKey.LastRoutingFailureAt
		healthInfo.RoutingFailureCount = upstreamKey.RoutingFailureCount
		healthInfo.RoutingCooldownUntil = upstreamKey.RoutingCooldownUntil
		healthInfo.RoutingIsolatedAt = upstreamKey.RoutingIsolatedAt
	}
	if _, unavailable := gatewayCandidateTransientFailure(healthInfo, time.Now()); unavailable {
		candidate.CoolingDown = true
		candidate.FailureCount = upstreamKey.RoutingFailureCount
		candidate.LastRoutingFailureAt = upstreamKey.LastRoutingFailureAt
		candidate.CooldownUntil = gatewayCandidateTransientCooldownUntil(healthInfo)
		if candidate.CooldownUntil != nil {
			candidate.CooldownRemaining = time.Until(*candidate.CooldownUntil)
		}
	}
	return candidate, nil
}

// manualRouteCurrentSynchronizedFacts returns model facts that are safe to
// use as fixed-route gates. A group can disappear and later reappear with the
// same name, and a failed sync deliberately preserves its prior observations
// for diagnostics. Neither old-generation nor failed-sync observations can
// prove that a currently fixed target rejects a protocol or model.
func manualRouteCurrentSynchronizedFacts(key *storage.GatewayUpstreamKey, generation storage.UpstreamGroupGeneration) (families []string, modelIDs []string, trusted bool) {
	if key == nil || !generation.Valid() || key.ModelsSyncedAt == nil ||
		key.ModelsSyncedAt.Before(generation.DiscoveredAt) || strings.TrimSpace(key.ModelsError) != "" {
		return nil, nil, false
	}
	return splitCSV(key.ModelFamilies), splitCSV(key.ModelIDs), true
}

func manualRouteCurrentSynchronizedFactsForIdentity(key *storage.GatewayUpstreamKey, generation storage.UpstreamGroupGeneration, fence storage.GatewayModelFactFence) (families []string, modelIDs []string, trusted bool) {
	if !fence.MatchesUpstreamKey(key) {
		return nil, nil, false
	}
	return manualRouteCurrentSynchronizedFacts(key, generation)
}

// manualRouteProtocolCompatibility only rejects a fact that is already known
// to be incompatible. Missing model-sync data is not a reason to add a probe
// or a second upstream request to a fixed dispatch.
func manualRouteProtocolCompatibility(families []string, app storage.GatewayApp) (known bool, compatible bool) {
	if len(families) == 0 {
		return false, true
	}
	for _, family := range families {
		if familySupportsGatewayCandidate(family, app) {
			return true, true
		}
	}
	return true, false
}

func (s *Service) cachedGatewayApp(channelID uint, groupName string, requestedApp storage.GatewayApp) (cachedGatewayAppInfo, bool) {
	var out cachedGatewayAppInfo
	if s == nil || s.Store == nil {
		return out, false
	}
	key, err := s.Store.FindUpstreamKey(channelID, strings.TrimSpace(groupName))
	if err != nil {
		return out, false
	}
	return cachedGatewayAppFromKey(key, requestedApp)
}

func cachedGatewayAppFromKey(key *storage.GatewayUpstreamKey, requestedApp storage.GatewayApp, healthFences ...storage.GatewayRoutingHealthFence) (cachedGatewayAppInfo, bool) {
	var out cachedGatewayAppInfo
	if key == nil || key.ID == 0 || !key.Enabled || key.Provisioning || strings.TrimSpace(key.ModelFamilies) == "" || strings.TrimSpace(key.APIKeyCipher) == "" {
		return out, false
	}
	out.ModelIDs = splitCSV(key.ModelIDs)
	out.ModelFamilies = splitCSV(key.ModelFamilies)
	out.LatencyProbeModel = strings.TrimSpace(key.LatencyProbeModel)
	out.ProbeEnabled = key.LatencyProbeEnabled
	out.ModelsSyncedAt = key.ModelsSyncedAt
	out.ModelsError = key.ModelsError
	includeRoutingHealth := len(healthFences) == 0
	if len(healthFences) == 1 {
		includeRoutingHealth = healthFences[0].MatchesUpstreamKey(key)
	}
	if includeRoutingHealth {
		out.LastRoutingFailureKind = strings.TrimSpace(key.LastRoutingFailureKind)
		out.LastRoutingFailureStatus = key.LastRoutingFailureStatus
		out.LastRoutingFailureAt = key.LastRoutingFailureAt
		out.RoutingFailureCount = key.RoutingFailureCount
		out.RoutingCooldownUntil = key.RoutingCooldownUntil
		out.RoutingIsolatedAt = key.RoutingIsolatedAt
	}
	families := modelFamilySet(key.ModelFamilies)
	for family := range families {
		if familySupportsGatewayCandidate(family, requestedApp) {
			out.App = requestedApp
			return out, true
		}
	}
	return out, true
}

func cachedGatewayAppFromKeyForIdentity(key *storage.GatewayUpstreamKey, requestedApp storage.GatewayApp, modelFence storage.GatewayModelFactFence, healthFences ...storage.GatewayRoutingHealthFence) (cachedGatewayAppInfo, bool) {
	if !modelFence.MatchesUpstreamKey(key) {
		return cachedGatewayAppInfo{}, false
	}
	return cachedGatewayAppFromKey(key, requestedApp, healthFences...)
}

// cachedDirectGatewayAppFromKey keeps only operational key/health evidence
// from the synchronized row. Direct protocol and model policy come from the
// current channel row, so stale model-fact fields cannot admit or reject it.
func cachedDirectGatewayAppFromKey(key *storage.GatewayUpstreamKey, channel storage.Channel, requestedApp storage.GatewayApp, healthFences ...storage.GatewayRoutingHealthFence) (cachedGatewayAppInfo, bool) {
	var out cachedGatewayAppInfo
	requestedApp = normalizeGatewayApp(requestedApp)
	if channel.CredentialMode != storage.CredentialModeDirect || normalizeGatewayApp(channel.GatewayApp) != requestedApp ||
		key == nil || key.ID == 0 || !key.Enabled || key.Provisioning || strings.TrimSpace(key.APIKeyCipher) == "" {
		return out, false
	}
	models, available := directProjectedModels(channel, *key)
	if !available {
		return out, false
	}
	baseURL := strings.TrimSpace(key.DirectBaseURL)
	if legacyDirectProjectedKey(*key) {
		baseURL = strings.TrimSpace(channel.SiteURL)
	}
	if len(models) == 0 || baseURL == "" {
		return out, false
	}
	out.App = requestedApp
	out.ModelFamilies = directChannelModelFamilies(&channel, models)
	out.ModelIDs = models
	out.DirectBaseURL = baseURL
	out.LatencyProbeModel = strings.TrimSpace(key.LatencyProbeModel)
	out.ProbeEnabled = key.LatencyProbeEnabled
	out.ModelsSyncedAt = key.ModelsSyncedAt
	out.ModelsError = key.ModelsError
	includeRoutingHealth := len(healthFences) == 0
	if len(healthFences) == 1 {
		includeRoutingHealth = healthFences[0].MatchesUpstreamKey(key)
	}
	if includeRoutingHealth {
		out.LastRoutingFailureKind = strings.TrimSpace(key.LastRoutingFailureKind)
		out.LastRoutingFailureStatus = key.LastRoutingFailureStatus
		out.LastRoutingFailureAt = key.LastRoutingFailureAt
		out.RoutingFailureCount = key.RoutingFailureCount
		out.RoutingCooldownUntil = key.RoutingCooldownUntil
		out.RoutingIsolatedAt = key.RoutingIsolatedAt
	}
	return out, true
}

func gatewayCandidateTransientFailure(info cachedGatewayAppInfo, now time.Time) (string, bool) {
	if info.LastRoutingFailureKind == RoutingFailureManualCooldown {
		if info.RoutingCooldownUntil != nil && now.Before(*info.RoutingCooldownUntil) {
			return RoutingFailureManualCooldown, true
		}
	}
	detail := ""
	switch info.LastRoutingFailureKind {
	case RoutingFailurePreparation, RoutingFailureTransport, RoutingFailureResponseTimeout:
		detail = info.LastRoutingFailureKind
	case RoutingFailureHTTPStatus:
		status := info.LastRoutingFailureStatus
		if status == http.StatusRequestTimeout || status == http.StatusConflict || status == http.StatusTooEarly || status == http.StatusTooManyRequests || status >= 500 && status <= 599 {
			detail = fmt.Sprintf("HTTP %d", status)
		}
	}
	if info.RoutingIsolatedAt != nil {
		if detail == "" {
			detail = "adaptive routing failure"
		}
		return detail, true
	}
	return "", false
}

func gatewayCandidateTransientCooldownUntil(info cachedGatewayAppInfo) *time.Time {
	if info.LastRoutingFailureKind != RoutingFailureManualCooldown || info.RoutingCooldownUntil == nil {
		return nil
	}
	until := info.RoutingCooldownUntil.UTC()
	return &until
}

// cachedGatewayFamilyMatches requires a group to belong to exactly one model
// family when a downstream key is family-pinned. A mixed GPT/Grok group is
// intentionally excluded because it could violate the key's model contract.
func cachedGatewayFamilyMatches(families []string, requested string) bool {
	requested = strings.ToLower(strings.TrimSpace(requested))
	if requested == "" || len(families) != 1 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(families[0]), requested)
}

func modelFamilySet(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(strings.ToLower(item))
		if item != "" {
			out[item] = struct{}{}
		}
	}
	return out
}

func splitCSV(raw string) []string {
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func modelIDSupportsRequest(modelIDs []string, requested string, app storage.GatewayApp) bool {
	requested = strings.TrimSpace(requested)
	if requested == "" || len(modelIDs) == 0 {
		return true
	}
	requestedFamily := modelFamilyFromName(requested)
	if requestedFamily == "" || !familySupportsGatewayApp(requestedFamily, app) {
		return false
	}
	for _, model := range modelIDs {
		if strings.EqualFold(strings.TrimSpace(model), requested) {
			return true
		}
	}
	// The upstream /v1/models list is authoritative for a synced group. Do
	// not fall back to a broad family match here: gpt-4 and gpt-5, or two
	// DeepSeek variants, may have different availability and pricing.
	return false
}

func directModelIDSupportsRequest(modelIDs []string, requested string) bool {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return true
	}
	if len(modelIDs) == 0 {
		return false
	}
	for _, model := range modelIDs {
		if strings.TrimSpace(model) == requested {
			return true
		}
	}
	return false
}

// DirectChannelAllowsModel evaluates the administrator-owned direct protocol
// and exact model allowlist without relying on asynchronously synchronized
// gateway model facts.
func DirectChannelAllowsModel(channel storage.Channel, app storage.GatewayApp, requested string) bool {
	if channel.CredentialMode != storage.CredentialModeDirect || normalizeGatewayApp(channel.GatewayApp) != normalizeGatewayApp(app) {
		return false
	}
	models, err := storage.ParseDirectModels(channel.DirectModels)
	return err == nil && directModelIDSupportsRequest(models, requested)
}

func modelAvailableForChannel(channel storage.Channel, modelIDs []string, requested string, app storage.GatewayApp) bool {
	if channel.CredentialMode == storage.CredentialModeDirect {
		return normalizeGatewayApp(channel.GatewayApp) == normalizeGatewayApp(app) && directModelIDSupportsRequest(modelIDs, requested)
	}
	return modelIDSupportsRequest(modelIDs, requested, app)
}

type candidateLatencyKey struct {
	ChannelID uint
	GroupName string
}

func canonicalCandidateKey(channelID uint, groupName string) candidateLatencyKey {
	return candidateLatencyKey{ChannelID: channelID, GroupName: canonicalGroupName(groupName)}
}

func cacheRoutingStatsRoutes(rates []storage.RateSnapshot, channels []storage.Channel, upstreamByRoute map[candidateLatencyKey]storage.GatewayUpstreamKey) []storage.GatewayRequestGroupCacheRoutingRoute {
	availableChannels := make(map[uint]struct{}, len(channels))
	for _, channel := range channels {
		if channelAvailable(channel) {
			availableChannels[channel.ID] = struct{}{}
		}
	}
	values := make([]storage.GatewayRequestGroupCacheRoutingRoute, 0, len(rates))
	seen := make(map[candidateLatencyKey]struct{}, len(rates))
	for _, rate := range rates {
		key := canonicalCandidateKey(rate.ChannelID, rate.ModelName)
		if key.ChannelID == 0 || key.GroupName == "" {
			continue
		}
		if _, ok := availableChannels[key.ChannelID]; !ok {
			continue
		}
		upstreamKey := upstreamByRoute[key]
		if upstreamKey.ID == 0 || !upstreamKey.Enabled || strings.TrimSpace(upstreamKey.APIKeyCipher) == "" || strings.TrimSpace(upstreamKey.ModelFamilies) == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, storage.GatewayRequestGroupCacheRoutingRoute{ChannelID: key.ChannelID, GroupName: key.GroupName})
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].ChannelID != values[j].ChannelID {
			return values[i].ChannelID < values[j].ChannelID
		}
		return values[i].GroupName < values[j].GroupName
	})
	return values
}

func cacheRoutingStatsRoutesKey(routes []storage.GatewayRequestGroupCacheRoutingRoute) string {
	if len(routes) == 0 {
		return ""
	}
	values := append([]storage.GatewayRequestGroupCacheRoutingRoute(nil), routes...)
	sort.Slice(values, func(i, j int) bool {
		if values[i].ChannelID != values[j].ChannelID {
			return values[i].ChannelID < values[j].ChannelID
		}
		return canonicalGroupName(values[i].GroupName) < canonicalGroupName(values[j].GroupName)
	})
	var key strings.Builder
	for _, route := range values {
		if route.ChannelID == 0 {
			continue
		}
		key.WriteString(fmt.Sprintf("%d", route.ChannelID))
		key.WriteByte('\x00')
		key.WriteString(canonicalGroupName(route.GroupName))
		key.WriteByte('\x01')
	}
	return key.String()
}

func upstreamKeyRoutePreference(candidate, current storage.GatewayUpstreamKey) bool {
	if candidate.Enabled != current.Enabled {
		return candidate.Enabled
	}
	if candidate.Provisioning != current.Provisioning {
		return !candidate.Provisioning
	}
	candidateReady := strings.TrimSpace(candidate.APIKeyCipher) != ""
	currentReady := strings.TrimSpace(current.APIKeyCipher) != ""
	if candidateReady != currentReady {
		return candidateReady
	}
	if candidate.ModelsSyncedAt != nil || current.ModelsSyncedAt != nil {
		if candidate.ModelsSyncedAt == nil {
			return false
		}
		if current.ModelsSyncedAt == nil || !candidate.ModelsSyncedAt.Equal(*current.ModelsSyncedAt) {
			return current.ModelsSyncedAt == nil || candidate.ModelsSyncedAt.After(*current.ModelsSyncedAt)
		}
	}
	return candidate.ID > current.ID
}

type candidateLatencyStat struct {
	AvgMs   float64
	Samples int64
}

type channelFundingSnapshot struct {
	factor  float64
	version uint64
	source  string
	trusted bool
}

func splitRateCostBasisSnapshots(paired []storage.RateCostBasisSnapshot) ([]storage.RateSnapshot, map[uint]channelFundingSnapshot) {
	rates := make([]storage.RateSnapshot, 0, len(paired))
	funding := make(map[uint]channelFundingSnapshot)
	for _, item := range paired {
		rates = append(rates, item.RateSnapshot())
		if _, exists := funding[item.ChannelID]; exists {
			continue
		}
		funding[item.ChannelID] = channelFundingSnapshot{
			factor: item.FundingFactor, version: item.CostBasisVersion,
			source:  item.CostBasisSource,
			trusted: trustedCostBasisStatus(item.CostBasisStatus, item.CostBasisStateFound),
		}
	}
	return rates, funding
}

func trustedCostBasisStatus(status string, stateFound bool) bool {
	if !stateFound {
		return true
	}
	switch strings.TrimSpace(status) {
	case "", costing.StateStatusReady, costing.StateStatusError:
		return true
	default:
		return false
	}
}

// channelFundingFactors loads the canonical conversion state once per
// candidate calculation. Missing or legacy state intentionally falls back to
// the channel's denormalized factor and finally to 1:1.
func (s *Service) channelFundingFactors(channels []storage.Channel) map[uint]channelFundingSnapshot {
	out := make(map[uint]channelFundingSnapshot, len(channels))
	states := make(map[uint]storage.ChannelCostState, len(channels))
	if s != nil && s.Costing != nil {
		if list, err := s.Costing.ListStates(); err == nil {
			for _, state := range list {
				states[state.ChannelID] = state
			}
		}
	}
	for _, ch := range channels {
		if ch.CredentialMode == storage.CredentialModeDirect {
			out[ch.ID] = channelFundingSnapshot{factor: 1, source: "direct", trusted: true}
			continue
		}
		factor := ch.FundingFactor
		if factor < 0 {
			factor = 1
		}
		snapshot := channelFundingSnapshot{factor: factor, source: ch.FundingFactorSource, trusted: true}
		if state, ok := states[ch.ID]; ok {
			if state.FundingFactor >= 0 {
				snapshot.factor = state.FundingFactor
			}
			snapshot.version = state.Version
			snapshot.source = state.Source
			snapshot.trusted = trustedCostBasisStatus(state.Status, true)
		}
		out[ch.ID] = snapshot
	}
	return out
}

const candidateLatencyTrimRatio = 0.2

func (s *Service) routeExclusionSet(downstreamKeyID uint) (map[candidateLatencyKey]struct{}, error) {
	out := make(map[candidateLatencyKey]struct{})
	if downstreamKeyID == 0 || s == nil || s.Store == nil {
		return out, nil
	}
	list, err := s.Store.ListRouteExclusions(downstreamKeyID)
	if err != nil {
		return nil, err
	}
	for _, item := range list {
		groupName := strings.TrimSpace(item.GroupName)
		if item.ChannelID == 0 || groupName == "" {
			continue
		}
		out[canonicalCandidateKey(item.ChannelID, groupName)] = struct{}{}
	}
	return out, nil
}

func routeExcluded(disabled map[candidateLatencyKey]struct{}, channelID uint, groupName string) bool {
	if len(disabled) == 0 || channelID == 0 {
		return false
	}
	_, ok := disabled[canonicalCandidateKey(channelID, groupName)]
	return ok
}

func (s *Service) candidateLatencies() (map[candidateLatencyKey]candidateLatencyStat, error) {
	out := make(map[candidateLatencyKey]candidateLatencyStat)
	samples, err := s.loadCandidateLatencySamples()
	if err != nil {
		return nil, err
	}
	for key, values := range samples.groups {
		if len(values) >= samples.minimum {
			avg, kept := trimmedMean(values, candidateLatencyTrimRatio)
			if kept > 0 {
				out[key] = candidateLatencyStat{AvgMs: avg, Samples: int64(len(values))}
			}
		}
	}
	for channelID, values := range samples.channels {
		if len(values) >= samples.minimum {
			avg, kept := trimmedMean(values, candidateLatencyTrimRatio)
			if kept > 0 {
				out[candidateLatencyKey{ChannelID: channelID}] = candidateLatencyStat{AvgMs: avg, Samples: int64(len(values))}
			}
		}
	}
	return out, nil
}

func trimmedMean(samples []float64, trimRatio float64) (float64, int) {
	if len(samples) == 0 {
		return 0, 0
	}
	values := append([]float64(nil), samples...)
	sort.Float64s(values)
	trim := int(float64(len(values)) * trimRatio)
	if trim*2 >= len(values) {
		trim = 0
	}
	kept := values[trim : len(values)-trim]
	if len(kept) == 0 {
		return 0, 0
	}
	sum := 0.0
	for _, value := range kept {
		sum += value
	}
	return sum / float64(len(kept)), len(kept)
}

func latencyForCandidate(latencies map[candidateLatencyKey]candidateLatencyStat, channelID uint, groupName string, minSamples int) candidateLatencyStat {
	if latencies == nil {
		return candidateLatencyStat{}
	}
	if minSamples <= 0 {
		minSamples = defaultRoutingLatencySampleSize
	}
	if value := latencies[canonicalCandidateKey(channelID, groupName)]; value.AvgMs > 0 && value.Samples >= int64(minSamples) {
		return value
	}
	if value := latencies[candidateLatencyKey{ChannelID: channelID}]; value.AvgMs > 0 && value.Samples >= int64(minSamples) {
		return value
	}
	return candidateLatencyStat{}
}

func effectiveCandidatePrice(rate storage.RateSnapshot) float64 {
	if rate.CompletionRatio > 0 {
		return (rate.Ratio + rate.CompletionRatio) / 2
	}
	return rate.Ratio
}

func cacheAdjustedInputRatio(inputRatio, cacheRatio, cacheMultiplier float64) float64 {
	if inputRatio <= 0 || math.IsNaN(inputRatio) || math.IsInf(inputRatio, 0) {
		return inputRatio
	}
	if math.IsNaN(cacheRatio) || math.IsInf(cacheRatio, 0) {
		cacheRatio = 0
	}
	cacheRatio = clamp01(cacheRatio)
	if math.IsNaN(cacheMultiplier) || math.IsInf(cacheMultiplier, 0) || cacheMultiplier < 0 {
		cacheMultiplier = defaultRoutingCacheInputPriceMultiplier
	}
	return inputRatio * ((1 - cacheRatio) + cacheRatio*cacheMultiplier)
}

func effectiveCandidatePriceWithCache(rate storage.RateSnapshot, cacheRatio, cacheMultiplier float64) float64 {
	inputRatio := cacheAdjustedInputRatio(rate.Ratio, cacheRatio, cacheMultiplier)
	if rate.CompletionRatio > 0 {
		return (inputRatio + rate.CompletionRatio) / 2
	}
	return inputRatio
}

// routingCacheRatios is intentionally memory-only. Candidate construction can
// happen for every downstream request, so stale or missing snapshots merely
// enqueue one background refresh and return immediately. The aggregate ledger
// itself contains only successful real downstream data-plane traffic.
func (s *Service) routingCacheRatios(routing RoutingConfig, routes []storage.GatewayRequestGroupCacheRoutingRoute) cacheRatioSnapshot {
	if s == nil || (s.Store == nil && s.cacheStatsLoader == nil) {
		return cacheRatioSnapshot{}
	}
	routing = NormalizeRoutingConfig(routing)
	configKey := cacheStatsConfigKey{
		Multiplier:   routing.CacheInputPriceMultiplier,
		MinTokens:    routing.CachePriceMinInputTokens,
		WindowTokens: routing.CachePriceWindowInputTokens,
		MaxAge:       routing.CacheStatsMaxAge,
		RoutesKey:    cacheRoutingStatsRoutesKey(routes),
	}
	now := time.Now().UTC()
	request := cacheStatsRefreshRequest{
		config:  configKey,
		routing: routing,
		routes:  append([]storage.GatewayRequestGroupCacheRoutingRoute(nil), routes...),
	}
	startRefresh := false
	s.cacheStatsMu.Lock()
	snapshot := s.cachedRoutingCacheSnapshotLocked(configKey, now)
	fresh := s.cacheStatsConfig == configKey && s.cacheStatsAt.After(now.Add(-routingCacheStatsRefreshInterval))
	backingOff := s.cacheStatsFailureConfig == configKey && s.cacheStatsFailureAt.After(now.Add(-routingCacheStatsFailureBackoff))
	if !fresh && !backingOff {
		if !s.cacheStatsRefreshing || s.cacheStatsActiveConfig != configKey {
			queued := request
			s.cacheStatsQueued = &queued
			if !s.cacheStatsRefreshing {
				s.cacheStatsRefreshing = true
				startRefresh = true
			}
		}
	}
	s.cacheStatsMu.Unlock()
	if startRefresh {
		go s.runRoutingCacheStatsRefresh()
	}
	return snapshot
}

func (s *Service) runRoutingCacheStatsRefresh() {
	for s != nil {
		s.cacheStatsMu.Lock()
		request := s.cacheStatsQueued
		s.cacheStatsQueued = nil
		if request == nil {
			s.cacheStatsRefreshing = false
			s.cacheStatsActiveConfig = cacheStatsConfigKey{}
			s.cacheStatsMu.Unlock()
			return
		}
		s.cacheStatsActiveConfig = request.config
		s.cacheStatsMu.Unlock()

		now := time.Now().UTC()
		queryCtx, cancel := context.WithTimeout(context.Background(), routingCacheStatsQueryTimeout)
		stats, err := s.loadRoutingCacheStats(queryCtx, now, *request)
		cancel()
		if err != nil {
			if s.Log != nil {
				s.Log.Warn("load gateway cache ratios for routing failed", "err", err)
			}
			s.cacheStatsMu.Lock()
			// A failed refresh must fail closed. Retaining a previously published
			// snapshot would let stale cache evidence discount prices indefinitely
			// during a persistent database outage.
			if s.cacheStatsConfig == request.config {
				s.cacheStatsAt = time.Time{}
				s.cacheStatsConfig = cacheStatsConfigKey{}
				s.cacheStats = make(map[candidateLatencyKey]cacheRatioStat)
				s.cacheStatsAverage = 0
				s.cacheStatsHasAverage = false
			}
			// A newer configuration may already be queued. Do not let an old
			// failure suppress that refresh.
			if s.cacheStatsQueued == nil || s.cacheStatsQueued.config == request.config {
				s.cacheStatsFailureAt = now
				s.cacheStatsFailureConfig = request.config
				if s.cacheStatsQueued != nil && s.cacheStatsQueued.config == request.config {
					s.cacheStatsQueued = nil
				}
			}
			s.cacheStatsMu.Unlock()
			continue
		}
		s.publishRoutingCacheStats(now, *request, stats)
	}
}

func (s *Service) loadRoutingCacheStats(ctx context.Context, now time.Time, request cacheStatsRefreshRequest) ([]storage.GatewayRequestGroupCacheRoutingStat, error) {
	if s == nil {
		return nil, errors.New("gateway service is unavailable")
	}
	if s.cacheStatsLoader != nil {
		return s.cacheStatsLoader(ctx, now, request.routing.CacheStatsMaxAge, request.routing.CachePriceWindowInputTokens, request.routes)
	}
	if s.Store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	return s.Store.ListGroupCacheRoutingStatsContext(ctx, now, request.routing.CacheStatsMaxAge, request.routing.CachePriceWindowInputTokens, request.routes)
}

func (s *Service) publishRoutingCacheStats(now time.Time, request cacheStatsRefreshRequest, stats []storage.GatewayRequestGroupCacheRoutingStat) {
	routing := request.routing
	// Only groups with enough recent input establish the average. Groups with
	// too little evidence inherit that average; groups absent from the expiry
	// window are intentionally omitted and therefore use the zero-cache default.
	var validPrompt, validCache int64
	for _, stat := range stats {
		if stat.Expired || stat.LastInputAt.IsZero() || stat.LastInputAt.Before(now.Add(-routing.CacheStatsMaxAge)) || stat.PromptTokens < routing.CachePriceMinInputTokens || stat.PromptTokens <= 0 {
			continue
		}
		validPrompt += stat.PromptTokens
		validCache += max(stat.CacheReadTokens, 0)
	}
	average := 0.0
	if validPrompt > 0 {
		average = clamp01(float64(validCache) / float64(validPrompt))
	}
	values := make(map[candidateLatencyKey]cacheRatioStat, len(stats))
	for _, stat := range stats {
		if stat.ChannelID == 0 {
			continue
		}
		key := canonicalCandidateKey(stat.ChannelID, stat.GroupName)
		if stat.Expired || stat.LastInputAt.IsZero() || stat.LastInputAt.Before(now.Add(-routing.CacheStatsMaxAge)) {
			values[key] = cacheRatioStat{LastInputAt: stat.LastInputAt, Expired: true}
			continue
		}
		if stat.PromptTokens <= 0 {
			continue
		}
		if stat.PromptTokens < routing.CachePriceMinInputTokens && validPrompt <= 0 {
			continue
		}
		ratio := average
		if stat.PromptTokens >= routing.CachePriceMinInputTokens {
			ratio = clamp01(float64(max(stat.CacheReadTokens, 0)) / float64(stat.PromptTokens))
		}
		values[key] = cacheRatioStat{Ratio: ratio, PromptTokens: stat.PromptTokens, CacheTokens: max(stat.CacheReadTokens, 0), LastInputAt: stat.LastInputAt}
	}

	s.cacheStatsMu.Lock()
	// If a newer route/configuration set arrived while the database query was
	// running, discard this result; the worker loop will consume the queued one.
	if s.cacheStatsQueued != nil && s.cacheStatsQueued.config != request.config {
		s.cacheStatsMu.Unlock()
		return
	}
	s.cacheStatsAt = now
	s.cacheStatsConfig = request.config
	s.cacheStats = values
	s.cacheStatsAverage = average
	s.cacheStatsHasAverage = validPrompt > 0
	s.cacheStatsFailureAt = time.Time{}
	s.cacheStatsFailureConfig = cacheStatsConfigKey{}
	s.cacheStatsMu.Unlock()
}

// estimateProbeCostUSD uses the same default quota basis as gateway request
// cost estimation. A minimal probe is modeled as one input and one output
// token; upstream responses may later provide an actual cost in request logs.
func estimateProbeCost(rate storage.RateSnapshot, _ []string) float64 {
	promptRatio := rate.Ratio
	completionRatio := rate.CompletionRatio
	if promptRatio <= 0 {
		return 0
	}
	if completionRatio <= 0 {
		completionRatio = promptRatio
	}
	return (promptRatio + completionRatio) / 500000
}

func scoreCandidates(candidates []Candidate) {
	scoreCandidatesWithConfig(candidates, DefaultRoutingConfig())
}

func sortCandidates(candidates []Candidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		leftUnavailable := candidates[i].DisabledForKey || candidates[i].CoolingDown
		rightUnavailable := candidates[j].DisabledForKey || candidates[j].CoolingDown
		if leftUnavailable != rightUnavailable {
			return !leftUnavailable
		}
		if candidates[i].Score != candidates[j].Score {
			return candidates[i].Score < candidates[j].Score
		}
		if candidates[i].EffectivePrice != candidates[j].EffectivePrice {
			return candidates[i].EffectivePrice < candidates[j].EffectivePrice
		}
		if candidates[i].AvgLatencyMs != candidates[j].AvgLatencyMs {
			if candidates[i].AvgLatencyMs == 0 {
				return false
			}
			if candidates[j].AvgLatencyMs == 0 {
				return true
			}
			return candidates[i].AvgLatencyMs < candidates[j].AvgLatencyMs
		}
		if candidates[i].Channel.SortOrder != candidates[j].Channel.SortOrder {
			return candidates[i].Channel.SortOrder > candidates[j].Channel.SortOrder
		}
		return candidates[i].Channel.ID < candidates[j].Channel.ID
	})
}

func rankedModelExclusions(routable []Candidate, exclusions []CandidateExclusion, routing RoutingConfig) []CandidateExclusion {
	modelExclusions := make(map[string]CandidateExclusion)
	result := make([]CandidateExclusion, 0, len(exclusions))
	hypothetical := append([]Candidate(nil), routable...)
	for _, exclusion := range exclusions {
		if exclusion.Reason != "model_not_listed" && exclusion.Reason != "model_not_supported" {
			result = append(result, exclusion)
			continue
		}
		key := candidateRouteKey(exclusion.candidate)
		modelExclusions[key] = exclusion
		hypothetical = append(hypothetical, exclusion.candidate)
	}
	if len(modelExclusions) == 0 {
		return result
	}
	scoreCandidatesWithConfig(hypothetical, routing)
	sortCandidates(hypothetical)
	routableSet := make(map[string]struct{}, len(routable))
	for _, candidate := range routable {
		routableSet[candidateRouteKey(candidate)] = struct{}{}
	}
	for _, candidate := range hypothetical {
		key := candidateRouteKey(candidate)
		if _, ok := routableSet[key]; ok {
			break
		}
		if exclusion, ok := modelExclusions[key]; ok {
			result = append(result, exclusion)
		}
	}
	return result
}

func candidateRouteKey(candidate Candidate) string {
	return fmt.Sprintf("%d\x00%s", candidate.Channel.ID, strings.TrimSpace(candidate.Rate.ModelName))
}

func scoreCandidatesWithConfig(candidates []Candidate, routing RoutingConfig) {
	if len(candidates) == 0 {
		return
	}
	routing = NormalizeRoutingConfig(routing)
	totalScore := routing.PriceScoreMax + routing.LatencyScoreMax
	if totalScore <= 0 {
		totalScore = 100
	}
	priceWeight := routing.PriceScoreMax / totalScore
	latencyWeight := routing.LatencyScoreMax / totalScore
	priceBenefits := pricePriorityBenefits(candidates)
	for i := range candidates {
		if candidates[i].DisabledForKey || candidates[i].CoolingDown {
			candidates[i].Score = 1
			candidates[i].PricePriorityScore = 0
			candidates[i].LatencyPriorityScore = 0
			continue
		}
		priceScore := 1 - priceBenefits[i]
		latencyScore := latencyPenaltyScore(candidates[i].AvgLatencyMs, routing)
		candidates[i].Score = priceWeight*priceScore + latencyWeight*latencyScore
		candidates[i].PricePriorityScore = priceBenefits[i] * routing.PriceScoreMax
		candidates[i].LatencyPriorityScore = (1 - latencyScore) * routing.LatencyScoreMax
	}
}

func pricePriorityBenefits(candidates []Candidate) []float64 {
	out := make([]float64, len(candidates))
	if len(candidates) == 0 {
		return out
	}
	for _, candidate := range candidates {
		if !candidate.DisabledForKey && !candidate.CoolingDown && candidate.EffectivePriceBasis == RoutingPriceBasisModel && candidate.EffectivePrice == 0 {
			for index, other := range candidates {
				if !other.DisabledForKey && !other.CoolingDown && other.EffectivePriceBasis == RoutingPriceBasisModel && other.EffectivePrice == 0 {
					out[index] = 1
				}
			}
			return out
		}
	}
	minPrice := 0.0
	for _, candidate := range candidates {
		if candidate.DisabledForKey || candidate.CoolingDown {
			continue
		}
		price := candidate.EffectivePrice
		if price <= 0 {
			continue
		}
		if minPrice == 0 || price < minPrice {
			minPrice = price
		}
	}
	if minPrice <= 0 {
		return out
	}
	for idx, candidate := range candidates {
		if candidate.DisabledForKey || candidate.CoolingDown {
			continue
		}
		price := candidate.EffectivePrice
		if price <= 0 {
			out[idx] = 1
			continue
		}
		out[idx] = clamp01(minPrice / price)
	}
	return out
}

func latencyPenaltyScore(latencyMs float64, routing RoutingConfig) float64 {
	routing = NormalizeRoutingConfig(routing)
	fullScoreMs := float64(routing.LatencyFullScore.Milliseconds())
	zeroScoreMs := float64(routing.LatencyZeroScore.Milliseconds())
	if latencyMs <= 0 {
		return 1 - routing.UnknownLatencyScoreFactor
	}
	if latencyMs <= fullScoreMs {
		return 0
	}
	if latencyMs >= zeroScoreMs {
		return 1
	}
	return (latencyMs - fullScoreMs) / (zeroScoreMs - fullScoreMs)
}

func normalizedLowerIsBetter(value, minValue, maxValue, fallback float64) float64 {
	if maxValue <= minValue {
		return fallback
	}
	return (value - minValue) / (maxValue - minValue)
}

func clamp01(value float64) float64 {
	if value <= 0 {
		return 0
	}
	if value >= 1 {
		return 1
	}
	return value
}

var (
	gatewayServiceSecretPattern             = regexp.MustCompile(`(?i)(bearer\s+)?(sk-[a-z0-9._=-]{8,}|sk-upops-[a-z0-9._=-]{8,}|[a-z0-9_-]{32,})`)
	gatewayServiceHeaderCredentialPattern   = regexp.MustCompile(`(?i)\b(?:authorization|proxy-authorization|x-api-key|api-key|apikey|cookie|set-cookie)\b\s*[:=]\s*(?:(?:bearer|basic|token)\s+)?[^\s,;]+`)
	gatewayServiceSchemeCredentialPattern   = regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+[a-z0-9._~+/=-]+`)
	gatewayServiceAssignedCredentialPattern = regexp.MustCompile(`(?i)["']?(?:access[_-]?token|auth[_-]?token|refresh[_-]?token|api[_-]?key|apikey|token|secret|password|passwd|session(?:[_-]?(?:id|token))?)["']?\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^&\s,;}\]]+)`)
)

func sanitizeGatewayServiceError(text string, sensitive ...string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	for _, value := range sensitive {
		value = strings.TrimSpace(value)
		if value != "" {
			text = strings.ReplaceAll(text, value, "[redacted]")
		}
	}
	text = gatewayServiceHeaderCredentialPattern.ReplaceAllString(text, "[redacted-header]")
	text = gatewayServiceSchemeCredentialPattern.ReplaceAllString(text, "[redacted-credential]")
	text = gatewayServiceAssignedCredentialPattern.ReplaceAllString(text, "[redacted-credential]")
	text = gatewayServiceSecretPattern.ReplaceAllString(text, "[redacted]")
	for _, marker := range []string{"Authorization", "authorization", "x-api-key", "X-Api-Key", "X-API-Key"} {
		if strings.Contains(text, marker) {
			text = strings.ReplaceAll(text, marker, "[redacted-header]")
		}
	}
	if len(text) > 512 {
		text = text[:512]
	}
	return text
}

// IsSessionScopedCapacityError identifies account-capacity failures that can
// be retried by a different candidate without cooling the whole channel.
func IsSessionScopedCapacityError(err error) bool {
	if err == nil {
		return false
	}
	var httpErr *connector.HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusTooManyRequests {
		return true
	}
	return IsSessionScopedCapacityMessage(err.Error())
}

func IsSessionScopedCapacityMessage(message string) bool {
	message = strings.ToLower(strings.TrimSpace(message))
	if message == "" {
		return false
	}
	for _, phrase := range []string{
		"selected model is at capacity",
		"model is at capacity",
		"model is overloaded",
		"model is currently overloaded",
		"server overloaded",
		"service overloaded",
		"upstream overloaded",
		"concurrency limit exceeded",
		"concurrent request limit",
		"upstream rate limit exceeded",
		"rate limit exceeded",
		"rate limit reached",
		"rate_limit_exceeded",
		"too many requests",
		"temporarily unavailable",
		"please retry later",
		"try again later",
	} {
		if strings.Contains(message, phrase) {
			return true
		}
	}
	return false
}

// IsSessionScopedCapacityNotice recognizes a standalone capacity notification
// before a stream has emitted useful output. It deliberately accepts a much
// narrower set than IsSessionScopedCapacityMessage so ordinary output that
// merely mentions capacity cannot trigger a transparent retry.
func IsSessionScopedCapacityNotice(message string) bool {
	if len(message) == 0 || len(message) > gatewayCapacityNoticeMaxBytes {
		return false
	}
	normalized := normalizeGatewayCapacityNotice(message)
	for _, notice := range gatewayCapacityNotices {
		if normalized == notice {
			return true
		}
	}
	return false
}

// IsSessionScopedCapacityNoticePrefix reports whether a bounded, unfinished
// provider warning can still become one of the canonical capacity notices.
// Callers must use it only while staging a response that has not reached the
// downstream client yet.
func IsSessionScopedCapacityNoticePrefix(message string) bool {
	if len(message) == 0 || len(message) > gatewayCapacityNoticeMaxBytes {
		return false
	}
	normalized := normalizeGatewayCapacityNotice(message)
	if normalized == "" {
		return false
	}
	for _, notice := range gatewayCapacityNotices {
		if strings.HasPrefix(notice, normalized) {
			return true
		}
	}
	return false
}

// IsSessionScopedUnmarkedCapacityNoticePrefix recognizes the one provider
// capacity notice that is emitted as ordinary Responses text without the
// warning marker used by the other canonical notices. Keep this narrower than
// IsSessionScopedCapacityNoticePrefix so an unmarked model answer mentioning a
// generic capacity phrase is never replayed.
func IsSessionScopedUnmarkedCapacityNoticePrefix(message string) bool {
	if len(message) == 0 || len(message) > gatewayCapacityNoticeMaxBytes {
		return false
	}
	normalized := normalizeGatewayCapacityNotice(message)
	return normalized != "" && strings.HasPrefix(gatewayUnmarkedCapacityNotice, normalized)
}

func normalizeGatewayCapacityNotice(message string) string {
	var normalized strings.Builder
	normalized.Grow(len(message))
	spacePending := false
	for _, r := range strings.ToLower(strings.TrimSpace(message)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if spacePending && normalized.Len() > 0 {
				normalized.WriteByte(' ')
			}
			normalized.WriteRune(r)
			spacePending = false
			continue
		}
		spacePending = normalized.Len() > 0
	}
	return normalized.String()
}

func (s *Service) EnsureUpstreamAPIKey(ctx context.Context, channelID uint) (string, error) {
	groupName, groupID, groupRatio, err := s.cheapestUpstreamGroup(ctx, channelID)
	if err != nil {
		if !IsSessionScopedCapacityError(err) {
			_ = s.Store.SetUpstreamKeyError(channelID, "", sanitizeGatewayServiceError(err.Error()))
		}
		return "", err
	}
	return s.EnsureUpstreamAPIKeyForGroup(ctx, channelID, groupName, groupID, groupRatio)
}

func (s *Service) EnsureUpstreamAPIKeyForGroup(ctx context.Context, channelID uint, requestedGroup string, requestedGroupID *int64, requestedGroupRatio float64) (string, error) {
	if plain, ok, err := s.cachedUsableUpstreamKey(channelID, requestedGroup, false); err != nil {
		return "", err
	} else if ok {
		return plain, nil
	}
	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return "", err
	}
	defer unlock()
	return s.ensureUpstreamAPIKeyForGroup(ctx, channelID, requestedGroup, requestedGroupID, requestedGroupRatio, true, false)
}

// EnsureUpstreamAPIKeyLeaseForGroup returns the key material and the remote
// identity from the same local-record snapshot. A request must use this value
// for both the Authorization header and its immutable request log, otherwise a
// concurrent key rotation could make the log describe a different key.
func (s *Service) EnsureUpstreamAPIKeyLeaseForGroup(ctx context.Context, channelID uint, requestedGroup string, requestedGroupID *int64, requestedGroupRatio float64) (UpstreamKeyLease, error) {
	if lease, ok, err := s.cachedUsableUpstreamKeyLease(channelID, requestedGroup, false); err != nil {
		return UpstreamKeyLease{}, err
	} else if ok {
		return lease, nil
	}

	ctx, unlock, err := s.lockModelSyncChannelContext(ctx, channelID)
	if err != nil {
		return UpstreamKeyLease{}, err
	}
	defer unlock()
	plain, err := s.ensureUpstreamAPIKeyForGroup(ctx, channelID, requestedGroup, requestedGroupID, requestedGroupRatio, true, false)
	if err != nil {
		return UpstreamKeyLease{}, err
	}
	lease, err := s.upstreamKeyLeaseForPlain(channelID, requestedGroup, plain)
	if err != nil {
		return UpstreamKeyLease{}, err
	}
	// lockModelSyncChannelContext still owns the channel operation lock here,
	// so this snapshot and the key lease belong to the same account identity.
	// Returning it also closes the API-origin discovery race without requiring
	// the data plane to hold the advisory lock for the duration of the request.
	channel, err := s.Channels.FindByID(channelID)
	if err != nil {
		return UpstreamKeyLease{}, err
	}
	accountConcurrency, err := s.Channels.UpstreamAccountConcurrencyForChannel(channelID)
	if err != nil {
		return UpstreamKeyLease{}, err
	}
	lease.ChannelSnapshot = channel
	lease.UpstreamAccountConcurrency = validatedUpstreamAccountConcurrencySnapshot(*channel, accountConcurrency)
	return lease, nil
}

// FixedUpstreamAPIKeyLeaseForGroup reads the exact local key selected by a
// fixed route. Fixed dispatch must not turn a valid local secret into a
// control-plane identity repair or remote provisioning operation merely
// because its historical upstream ID/name is missing.
func (s *Service) FixedUpstreamAPIKeyLeaseForGroup(channelID uint, requestedGroup string) (UpstreamKeyLease, error) {
	requestedGroup = strings.TrimSpace(requestedGroup)
	if s == nil || s.Store == nil || s.Cipher == nil || channelID == 0 || requestedGroup == "" {
		return UpstreamKeyLease{}, errors.New("fixed upstream key lease is unavailable")
	}
	cached, err := s.Store.FindUpstreamKeyAny(channelID, requestedGroup)
	if err != nil {
		return UpstreamKeyLease{}, err
	}
	if !cached.Enabled || cached.Provisioning || strings.TrimSpace(cached.APIKeyCipher) == "" {
		return UpstreamKeyLease{}, errors.New("fixed upstream key is not ready")
	}
	if s.Channels == nil {
		return UpstreamKeyLease{}, errors.New("channel write admission is unavailable")
	}
	if err := s.Channels.RequireOrdinaryChannelWrite(context.Background(), channelID); err != nil {
		return UpstreamKeyLease{}, err
	}
	plain, err := s.Cipher.Decrypt(cached.APIKeyCipher)
	if err != nil {
		return UpstreamKeyLease{}, err
	}
	plain = strings.TrimSpace(plain)
	if plain == "" {
		return UpstreamKeyLease{}, errors.New("fixed upstream key is empty")
	}
	return UpstreamKeyLease{
		Key:             plain,
		UpstreamKeyID:   cached.UpstreamKeyID,
		UpstreamKeyName: strings.TrimSpace(cached.UpstreamKeyName),
		LocalKeyID:      cached.ID,
		KeyGeneration:   cached.KeyGeneration,
	}, nil
}

func (s *Service) ensureUpstreamAPIKeyForGroupRemote(ctx context.Context, channelID uint, requestedGroup string, requestedGroupID *int64, requestedGroupRatio float64, expectedIdentityRevision uint64, rebuildingManagedKeys bool) (string, error) {
	cacheGroupName := strings.TrimSpace(requestedGroup)
	store := s.Store.WithContext(ctx)
	upstreamGroupName, groupID, groupRatio, err := s.resolveUpstreamGroup(ctx, channelID, requestedGroup, requestedGroupID, requestedGroupRatio, expectedIdentityRevision)
	if err != nil {
		if !IsSessionScopedCapacityError(err) {
			_ = store.SetUpstreamKeyError(channelID, cacheGroupName, sanitizeGatewayServiceError(err.Error()))
		}
		return "", err
	}
	if cacheGroupName == "" {
		cacheGroupName = upstreamGroupName
	}
	if err := s.requireUpstreamAccountIdentityRevisionContext(ctx, channelID, expectedIdentityRevision); err != nil {
		return "", err
	}
	if cached, err := store.FindUpstreamKeyAny(channelID, cacheGroupName); err == nil {
		rebuildPending := !cached.Enabled && strings.TrimSpace(cached.LastError) == managedUpstreamKeyRebuildPendingError
		if rebuildPending && !rebuildingManagedKeys {
			return "", errors.New("managed upstream key rebuild is incomplete; retry the administrator rebuild action")
		}
		if rebuildPending {
			// The rebuild-specific provisioning claim below atomically replaces
			// this marker. Ordinary callers cannot bypass it.
		} else if cached.Provisioning {
			// The durable claim below decides whether this caller owns a stale
			// retry before it performs recovery. Keeping that decision ahead of the
			// remote absence checks prevents an unclaimed caller from posting.
		} else {
			plain, decErr := s.Cipher.Decrypt(cached.APIKeyCipher)
			if decErr != nil {
				return "", decErr
			}
			if strings.TrimSpace(plain) != "" {
				if cached.UpstreamKeyID <= 0 || strings.TrimSpace(cached.UpstreamKeyName) == "" {
					if identity, repairErr := s.findCachedUpstreamIdentity(ctx, channelID, upstreamGroupName, groupID, plain); repairErr == nil {
						if saveErr := s.saveUpstreamKeyContext(ctx, channelID, plain, cacheGroupName, groupRatio, identity); saveErr != nil {
							return "", saveErr
						}
						s.clearUpstreamIdentityRepair(channelID, cacheGroupName)
					}
				}
				return strings.TrimSpace(plain), nil
			}
			if hasUpstreamKeyRecoveryEvidence(cached) {
				if cached.UpstreamKeyID > 0 && isManagedUpstreamKeyName(cached.UpstreamKeyName) {
					plain, present, recoverErr := s.recoverCachedManagedUpstreamKey(ctx, channelID, cacheGroupName, upstreamGroupName, groupID, groupRatio, cached)
					if recoverErr != nil {
						_ = store.SetUpstreamKeyError(channelID, cacheGroupName, sanitizeGatewayServiceError(recoverErr.Error()))
						return "", recoverErr
					}
					if present {
						return plain, nil
					}
				}
				plain, adoptErr := s.adoptExistingUpstreamKey(ctx, channelID, cacheGroupName, upstreamGroupName, groupID, groupRatio)
				if adoptErr == nil {
					return plain, nil
				}
				if !errors.Is(adoptErr, gorm.ErrRecordNotFound) {
					_ = store.SetUpstreamKeyError(channelID, cacheGroupName, sanitizeGatewayServiceError(adoptErr.Error()))
					return "", adoptErr
				}
				err := errors.New("existing upstream key could not be recovered; refusing automatic replacement")
				_ = store.SetUpstreamKeyError(channelID, cacheGroupName, err.Error())
				return "", err
			} else if hasUpstreamKeyErrorState(cached) {
				// A model/login error can leave an empty placeholder row before
				// any remote create was attempted. Try the guarded adoption path
				// first, then allow a fresh claim when no compatible key exists.
				plain, adoptErr := s.adoptExistingUpstreamKey(ctx, channelID, cacheGroupName, upstreamGroupName, groupID, groupRatio)
				if adoptErr == nil {
					return plain, nil
				}
				if !errors.Is(adoptErr, gorm.ErrRecordNotFound) {
					_ = store.SetUpstreamKeyError(channelID, cacheGroupName, sanitizeGatewayServiceError(adoptErr.Error()))
					return "", adoptErr
				}
			}
			// Claim the existing empty row so group-level settings survive provisioning.
		}
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", err
	}
	generation, generationErr := store.CaptureUpstreamGroupGeneration(channelID, upstreamGroupName)
	if generationErr != nil {
		return "", fmt.Errorf("capture managed key recovery generation for group %q: %w", upstreamGroupName, generationErr)
	}
	if !generation.Valid() {
		return "", fmt.Errorf("capture managed key recovery generation for group %q: invalid generation", upstreamGroupName)
	}
	provisioningName, err := newUpstreamKeyProvisioningName()
	if err != nil {
		return "", fmt.Errorf("generate upstream key provisioning name: %w", err)
	}
	var pending *storage.GatewayUpstreamKey
	var claimed, fresh bool
	if rebuildingManagedKeys {
		pending, claimed, fresh, err = store.ClaimUpstreamKeyProvisioningForRebuild(
			channelID, cacheGroupName, provisioningName, groupRatio, time.Now(), upstreamKeyProvisioningStaleAfter, managedUpstreamKeyRebuildPendingError,
		)
	} else {
		pending, claimed, fresh, err = store.ClaimUpstreamKeyProvisioning(
			channelID, cacheGroupName, provisioningName, groupRatio, time.Now(), upstreamKeyProvisioningStaleAfter,
		)
	}
	if err != nil {
		return "", err
	}
	if pending != nil && pending.Enabled && strings.TrimSpace(pending.APIKeyCipher) != "" {
		plain, decryptErr := s.Cipher.Decrypt(pending.APIKeyCipher)
		if decryptErr != nil {
			return "", decryptErr
		}
		if strings.TrimSpace(plain) != "" {
			return strings.TrimSpace(plain), nil
		}
	}
	if pending == nil || strings.TrimSpace(pending.UpstreamKeyName) == "" {
		return "", errors.New("upstream key provisioning claim is incomplete")
	}
	if !fresh {
		identity, plain, recoverErr := s.revealProvisionedUpstreamKey(ctx, channelID, upstreamGroupName, groupID, pending)
		if recoverErr == nil {
			if err := s.saveUpstreamKeyContext(ctx, channelID, plain, cacheGroupName, groupRatio, identity, recoveredProvisioningOwnershipProvenance(pending)); err != nil {
				return "", err
			}
			return plain, nil
		}
		if !errors.Is(recoverErr, gorm.ErrRecordNotFound) {
			_ = store.SetUpstreamKeyError(channelID, cacheGroupName, sanitizeGatewayServiceError(recoverErr.Error()))
			return "", recoverErr
		}
		plain, adoptErr := s.adoptExistingUpstreamKey(ctx, channelID, cacheGroupName, upstreamGroupName, groupID, groupRatio)
		if adoptErr == nil {
			return plain, nil
		}
		if !errors.Is(adoptErr, gorm.ErrRecordNotFound) {
			_ = store.SetUpstreamKeyError(channelID, cacheGroupName, sanitizeGatewayServiceError(adoptErr.Error()))
			return "", adoptErr
		}
		if !claimed || !pending.Provisioning {
			if pending.Provisioning {
				return "", errors.New("upstream key provisioning recovery is already in progress")
			}
			err := errors.New("previous upstream key provisioning could not be recovered; refusing duplicate create")
			_ = store.SetUpstreamKeyError(channelID, cacheGroupName, err.Error())
			return "", err
		}
		// This caller atomically reclaimed a stale persisted nonce. Both the exact
		// nonce lookup and the complete adoption scan proved absence, so it alone
		// may retry the POST with that same nonce. Other callers observe the renewed
		// claim timestamp and cannot reach this point.
	}

	unlimited := true
	expired := int64(-1)
	// Keep the upstream credential pinned to its selected group. Some NewAPI
	// deployments also normalize an explicitly enabled cross-group retry back to
	// false, which would otherwise make the durable creation fingerprint reject
	// the key that the same request just created.
	crossGroupRetry := false
	modelLimitsEnabled := false
	createRequest := connector.APIKeyCreateRequest{
		Name:               pending.UpstreamKeyName,
		Group:              upstreamGroupName,
		GroupID:            groupID,
		UnlimitedQuota:     &unlimited,
		ExpiredTime:        &expired,
		CrossGroupRetry:    &crossGroupRetry,
		ModelLimitsEnabled: &modelLimitsEnabled,
	}
	createRequest.RecoveryFence = &connector.APIKeyCreateRecoveryFence{
		ChannelIdentityRevision: expectedIdentityRevision,
		UpstreamGroupID:         generation.ID,
		PresenceGeneration:      generation.PresenceGeneration,
		CanonicalGroupName:      generation.CanonicalGroupName,
	}
	created, err := s.Keys.CreateAPIKey(ctx, channelID, createRequest)
	if err != nil {
		if isUpstreamAPIKeyCountLimitError(err) {
			plain, adoptErr := s.adoptExistingUpstreamKey(ctx, channelID, cacheGroupName, upstreamGroupName, groupID, groupRatio)
			if adoptErr == nil {
				return plain, nil
			}
			if !errors.Is(adoptErr, gorm.ErrRecordNotFound) {
				err = errors.Join(err, fmt.Errorf("recover existing managed upstream key: %w", adoptErr))
			}
		}
		pending.LastError = sanitizeGatewayServiceError(err.Error())
		if created != nil && created.ID > 0 {
			pending.UpstreamKeyID = created.ID
		}
		pending.Provisioning = true
		pending.Enabled = false
		pending.APIKeyCipher = ""
		pending.APIKeyPrefix = ""
		if saveErr := store.UpsertUpstreamKey(pending); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save upstream key provisioning state: %w", saveErr))
		}
		if !IsSessionScopedCapacityError(err) {
			_ = store.SetUpstreamKeyError(channelID, cacheGroupName, sanitizeGatewayServiceError(err.Error()))
		}
		return "", err
	}
	if created == nil || strings.TrimSpace(created.Key) == "" {
		err := errors.New("upstream created api key did not return its actual key")
		if created != nil && created.ID > 0 {
			pending.UpstreamKeyID = created.ID
		}
		pending.Provisioning = true
		pending.Enabled = false
		pending.APIKeyCipher = ""
		pending.APIKeyPrefix = ""
		pending.LastError = err.Error()
		_ = store.UpsertUpstreamKey(pending)
		return "", err
	}
	key := strings.TrimSpace(created.Key)
	if created != nil {
		if strings.TrimSpace(created.Name) == "" {
			created.Name = pending.UpstreamKeyName
		}
		if strings.TrimSpace(created.GroupName) == "" {
			created.GroupName = upstreamGroupName
		}
		if strings.TrimSpace(created.Group) == "" {
			created.Group = upstreamGroupName
		}
		if created.GroupRatio > 0 {
			groupRatio = created.GroupRatio
		}
	}
	if err := s.saveUpstreamKeyContext(ctx, channelID, key, cacheGroupName, groupRatio, created, storage.GatewayUpstreamKeyProvenanceCreated); err != nil {
		return "", err
	}
	return key, nil
}

// cachedUsableUpstreamKey is deliberately limited to an already-enabled local
// record. It is the gateway hot path: resolving the upstream group or listing
// remote management data is only needed when the local key is missing or is in
// provisioning/recovery state.
func (s *Service) cachedUsableUpstreamKey(channelID uint, groupName string, identityRepairClaimed bool) (string, bool, error) {
	return s.cachedUsableUpstreamKeyContext(context.Background(), channelID, groupName, identityRepairClaimed)
}

func (s *Service) cachedUsableUpstreamKeyContext(ctx context.Context, channelID uint, groupName string, identityRepairClaimed bool) (string, bool, error) {
	lease, ok, err := s.cachedUsableUpstreamKeyLeaseContext(ctx, channelID, groupName, identityRepairClaimed)
	return lease.Key, ok, err
}

func (s *Service) cachedUsableUpstreamKeyLease(channelID uint, groupName string, identityRepairClaimed bool) (UpstreamKeyLease, bool, error) {
	return s.cachedUsableUpstreamKeyLeaseContext(context.Background(), channelID, groupName, identityRepairClaimed)
}

func (s *Service) cachedUsableUpstreamKeyLeaseContext(ctx context.Context, channelID uint, groupName string, identityRepairClaimed bool) (UpstreamKeyLease, bool, error) {
	groupName = strings.TrimSpace(groupName)
	if s == nil || s.Store == nil || s.Cipher == nil || channelID == 0 || groupName == "" {
		return UpstreamKeyLease{}, false, nil
	}
	cached, err := s.Store.WithContext(ctx).FindUpstreamKeyAny(channelID, groupName)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return UpstreamKeyLease{}, false, nil
	}
	if err != nil {
		return UpstreamKeyLease{}, false, err
	}
	if !cached.Enabled || cached.Provisioning || strings.TrimSpace(cached.APIKeyCipher) == "" {
		return UpstreamKeyLease{}, false, nil
	}
	if (cached.UpstreamKeyID <= 0 || strings.TrimSpace(cached.UpstreamKeyName) == "") && !isDirectUpstreamKeyRecord(cached) {
		if identityRepairClaimed || s.claimUpstreamIdentityRepair(channelID, groupName) {
			return UpstreamKeyLease{}, false, nil
		}
	}
	plain, err := s.Cipher.Decrypt(cached.APIKeyCipher)
	if err != nil {
		return UpstreamKeyLease{}, false, err
	}
	plain = strings.TrimSpace(plain)
	if plain == "" {
		return UpstreamKeyLease{}, false, nil
	}
	if s.Channels == nil {
		return UpstreamKeyLease{}, false, errors.New("channel write admission is unavailable")
	}
	if err := s.Channels.WithContext(ctx).RequireOrdinaryChannelWrite(ctx, channelID); err != nil {
		return UpstreamKeyLease{}, false, err
	}
	return UpstreamKeyLease{
		Key:             plain,
		UpstreamKeyID:   cached.UpstreamKeyID,
		UpstreamKeyName: strings.TrimSpace(cached.UpstreamKeyName),
		LocalKeyID:      cached.ID,
		KeyGeneration:   cached.KeyGeneration,
	}, true, nil
}

func isDirectUpstreamKeyRecord(key *storage.GatewayUpstreamKey) bool {
	return key != nil && key.UpstreamKeyID == 0 &&
		(key.DirectCredentialID != nil || (strings.EqualFold(strings.TrimSpace(key.GroupName), storage.DirectChannelGroupName) &&
			strings.TrimSpace(key.UpstreamKeyName) == directUpstreamKeyName))
}

func (s *Service) upstreamKeyLeaseForPlain(channelID uint, groupName, plain string) (UpstreamKeyLease, error) {
	plain = strings.TrimSpace(plain)
	if s == nil || s.Store == nil || s.Cipher == nil || channelID == 0 || strings.TrimSpace(groupName) == "" || plain == "" {
		return UpstreamKeyLease{}, errors.New("upstream key lease is incomplete")
	}
	record, err := s.Store.FindUpstreamKeyAny(channelID, strings.TrimSpace(groupName))
	if err != nil {
		return UpstreamKeyLease{}, err
	}
	if strings.TrimSpace(record.APIKeyCipher) != "" {
		storedPlain, decryptErr := s.Cipher.Decrypt(record.APIKeyCipher)
		if decryptErr != nil {
			return UpstreamKeyLease{}, decryptErr
		}
		if HashKey(storedPlain) != HashKey(plain) {
			return UpstreamKeyLease{}, errors.New("upstream key changed during identity snapshot")
		}
	}
	if s.Channels == nil {
		return UpstreamKeyLease{}, errors.New("channel write admission is unavailable")
	}
	if err := s.Channels.RequireOrdinaryChannelWrite(context.Background(), channelID); err != nil {
		return UpstreamKeyLease{}, err
	}
	return UpstreamKeyLease{
		Key:             plain,
		UpstreamKeyID:   record.UpstreamKeyID,
		UpstreamKeyName: strings.TrimSpace(record.UpstreamKeyName),
		LocalKeyID:      record.ID,
		KeyGeneration:   record.KeyGeneration,
	}, nil
}

func newUpstreamKeyProvisioningName() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return upstreamProvisioningKeyPrefix + hex.EncodeToString(raw), nil
}

func (s *Service) revealProvisionedUpstreamKey(ctx context.Context, channelID uint, groupName string, groupID *int64, pending *storage.GatewayUpstreamKey) (*connector.APIKey, string, error) {
	if pending == nil || strings.TrimSpace(pending.UpstreamKeyName) == "" {
		return nil, "", gorm.ErrRecordNotFound
	}
	expectedName := strings.TrimSpace(pending.UpstreamKeyName)
	if !isExpectedManagedUpstreamKeyName(expectedName, groupName) {
		return nil, "", errors.New("provisioned upstream key has an unexpected managed name")
	}
	if pending.UpstreamKeyID > 0 {
		remote, err := s.findUpstreamKeyByID(ctx, channelID, pending.UpstreamKeyID)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if conflictErr := s.rejectManagedUpstreamKeyNameConflict(ctx, channelID, expectedName, groupName, groupID); conflictErr != nil {
				return nil, "", conflictErr
			}
			return nil, "", gorm.ErrRecordNotFound
		}
		if err != nil {
			return nil, "", fmt.Errorf("verify provisioned upstream key %d: %w", pending.UpstreamKeyID, err)
		}
		if remote.ID != pending.UpstreamKeyID || !recoverableManagedUpstreamKeyForName(*remote, expectedName, groupName, groupID, true) {
			return nil, "", fmt.Errorf("provisioned upstream key %d identity or creation fingerprint changed", pending.UpstreamKeyID)
		}
		plain, err := s.Keys.RevealAPIKey(ctx, channelID, pending.UpstreamKeyID)
		if err != nil {
			return nil, "", fmt.Errorf("reveal provisioned upstream key %d: %w", pending.UpstreamKeyID, err)
		}
		if strings.TrimSpace(plain) == "" {
			return nil, "", fmt.Errorf("reveal provisioned upstream key %d returned an empty key", pending.UpstreamKeyID)
		}
		return remote, strings.TrimSpace(plain), nil
	}
	allowUnobserved := upstreamProvisioningKeyNamePattern.MatchString(expectedName)
	matches := make(map[int64]connector.APIKey)
	incompatible := 0
	for pageNum := 1; pageNum <= upstreamKeyLookupMaxPages; pageNum++ {
		query := connector.APIKeyQuery{Page: pageNum, PageSize: upstreamKeyLookupPageSize, Search: expectedName}
		if groupID != nil && *groupID > 0 {
			query.GroupID = strconv.FormatInt(*groupID, 10)
		}
		page, err := s.listAPIKeysUsingPassCache(ctx, channelID, query)
		if err != nil {
			return nil, "", fmt.Errorf("list provisioned upstream key: %w", err)
		}
		for _, item := range page.Items {
			if strings.TrimSpace(item.Name) != expectedName {
				continue
			}
			if item.ID <= 0 || !recoverableManagedUpstreamKeyForName(item, expectedName, groupName, groupID, allowUnobserved) {
				incompatible++
				continue
			}
			matches[item.ID] = item
		}
		if apiKeyPageComplete(page, pageNum, upstreamKeyLookupPageSize) {
			break
		}
		if pageNum == upstreamKeyLookupMaxPages {
			return nil, "", errors.New("provisioned upstream key lookup exceeds safe limit")
		}
	}
	if incompatible > 0 {
		return nil, "", fmt.Errorf("provisioned upstream key name %q has incompatible remote records", expectedName)
	}
	if len(matches) == 0 {
		return nil, "", gorm.ErrRecordNotFound
	}
	if len(matches) != 1 {
		return nil, "", fmt.Errorf("provisioned upstream key name %q matched %d remote keys", expectedName, len(matches))
	}
	var selected connector.APIKey
	for _, item := range matches {
		selected = item
	}
	plain, err := s.Keys.RevealAPIKey(ctx, channelID, selected.ID)
	if err != nil {
		return nil, "", fmt.Errorf("reveal provisioned upstream key %d: %w", selected.ID, err)
	}
	if strings.TrimSpace(plain) == "" {
		return nil, "", fmt.Errorf("reveal provisioned upstream key %d returned an empty key", selected.ID)
	}
	return &selected, strings.TrimSpace(plain), nil
}

func (s *Service) ListModels() ([]storage.RateSnapshot, error) {
	channels, err := s.Channels.List()
	if err != nil {
		return nil, err
	}
	out := make([]storage.RateSnapshot, 0)
	for _, ch := range channels {
		if !channelAvailable(ch) {
			continue
		}
		rates, err := s.Rates.ListByChannel(ch.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, rates...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ModelName != out[j].ModelName {
			return out[i].ModelName < out[j].ModelName
		}
		if out[i].Ratio != out[j].Ratio {
			return out[i].Ratio < out[j].Ratio
		}
		return out[i].ChannelID < out[j].ChannelID
	})
	return out, nil
}

func (s *Service) RunLatencyProbes(ctx context.Context, opts LatencyProbeOptions) ([]LatencyProbeResult, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if opts.MaxCandidates <= 0 {
		opts.MaxCandidates = DefaultLatencyProbeMaxCandidates
	}
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultLatencyProbeTimeout
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Minute
	}
	useCompletionClock := opts.Now.IsZero()
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	// The latency scheduler is only a cooldown-recovery scanner. Model-list
	// synchronization has its own lifecycle; doing it here would turn an empty
	// recovery pass into active upstream traffic.
	s.cleanupLatencyProbeStates()
	results := make([]LatencyProbeResult, 0)
	type scheduledProbe struct {
		app       storage.GatewayApp
		candidate Candidate
		interval  time.Duration
	}
	work := make([]scheduledProbe, 0)
	remainingBudget := opts.MaxEstimatedCostUSD
	for _, app := range []storage.GatewayApp{storage.GatewayAppCodex, storage.GatewayAppClaude, storage.GatewayAppGemini} {
		// Latency probes are control-plane observations. Include channels that
		// are currently below a routing balance threshold so a probe can refresh
		// the balance fact; monitor-disabled and channel-error states remain hard
		// blockers.
		candidates, _, err := s.candidatesForAppFamilyAllowed(app, "", 0, false, "", true, true, true, nil)
		if err != nil {
			if isNoAvailableLatencyProbeCandidateError(err) {
				continue
			}
			results = append(results, LatencyProbeResult{App: app, Error: sanitizeGatewayServiceError(err.Error())})
			continue
		}
		candidates = filterProbeGenerationCandidates(candidates)
		candidates = s.filterCurrentDirectLatencyProbeCandidates(ctx, candidates)
		states, stateErr := s.Store.ListLatencyProbeStates()
		if stateErr != nil {
			return nil, stateErr
		}
		selected, intervals, usedBudget, err := selectDueLatencyProbeCandidatesWithBudget(candidates, states, app, opts.MaxCandidates, opts.Interval, now, remainingBudget)
		if err != nil {
			return nil, err
		}
		if remainingBudget > 0 {
			remainingBudget -= usedBudget
			if remainingBudget < 0 {
				remainingBudget = 0
			}
		}
		for i, candidate := range selected {
			work = append(work, scheduledProbe{app: app, candidate: candidate, interval: intervals[i]})
		}
	}
	maxConcurrent := opts.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	if maxConcurrent > len(work) {
		maxConcurrent = len(work)
	}
	if maxConcurrent == 0 {
		return results, nil
	}
	sem := make(chan struct{}, maxConcurrent)
	probeResults := make([]LatencyProbeResult, len(work))
	var wg sync.WaitGroup
	for index, item := range work {
		index, item := index, item
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			result := s.runLatencyProbe(ctx, item.app, item.candidate, opts.Timeout)
			probeResults[index] = result
			if result.deferred {
				// Local admission deferral is not a probe outcome, but it still
				// needs a durable next-due time so a saturated account cannot
				// be selected again on every scheduler tick. The persistence
				// path preserves the previous confirmation fields for this case.
				if err := s.persistLatencyProbeSchedule(item.candidate, item.app, result, item.interval, now); err != nil && s.Log != nil {
					s.Log.Warn("gateway deferred latency probe schedule state update failed", "channel_id", item.candidate.Channel.ID, "group", strings.TrimSpace(item.candidate.Rate.ModelName), "err", err)
				}
				return
			}
			scheduleNow := now
			// Mixed confirmations and cooldown recovery need a short, precise
			// retry. Anchor them to this worker's completion time instead of the
			// timestamp captured before the three upstream requests started.
			if useCompletionClock && !result.completedAt.IsZero() && (result.confirmationMixed || result.confirmation == "failure" || item.candidate.CoolingDown) {
				scheduleNow = result.completedAt
			}
			if err := s.persistLatencyProbeSchedule(item.candidate, item.app, result, item.interval, scheduleNow); err != nil && s.Log != nil {
				s.Log.Warn("gateway latency probe schedule state update failed", "channel_id", item.candidate.Channel.ID, "group", strings.TrimSpace(item.candidate.Rate.ModelName), "err", err)
			}
		}()
	}
	wg.Wait()
	for _, result := range probeResults {
		if !result.deferred {
			results = append(results, result)
		}
	}
	return results, nil
}

func isNoAvailableLatencyProbeCandidateError(err error) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return strings.HasPrefix(message, "no available ") && strings.Contains(message, "upstream channel has rate snapshots")
}

// filterProbeEnabledCandidates is retained for compatibility projections and
// legacy callers. Cooldown recovery deliberately does not call it: historical
// hidden switch values must not strand a route in cooldown.
func filterProbeEnabledCandidates(candidates []Candidate) []Candidate {
	if len(candidates) == 0 {
		return candidates
	}
	out := make([]Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Channel.LatencyProbeEnabled && !candidate.ProbeDisabled {
			out = append(out, candidate)
		}
	}
	return out
}

// cleanupLatencyProbeStates is intentionally much less frequent than probe
// scheduling. The state list already scopes queries to current generations;
// this periodic pass only bounds historical rows without adding a correlated
// DELETE to every application-specific probe pass.
func (s *Service) cleanupLatencyProbeStates() {
	if s == nil || s.Store == nil {
		return
	}
	now := time.Now().UTC()
	s.probeStateCleanupMu.Lock()
	if !s.probeStateCleanupAt.IsZero() && now.Sub(s.probeStateCleanupAt) < latencyProbeStateCleanupInterval {
		s.probeStateCleanupMu.Unlock()
		return
	}
	s.probeStateCleanupAt = now
	s.probeStateCleanupMu.Unlock()
	if _, err := s.Store.PruneLatencyProbeStates(0); err != nil && s.Log != nil {
		s.Log.Warn("gateway latency probe state cleanup skipped", "err", err)
	}
}

func filterProbeGenerationCandidates(candidates []Candidate) []Candidate {
	if len(candidates) == 0 {
		return candidates
	}
	out := make([]Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.UpstreamGeneration.Valid() {
			out = append(out, candidate)
		}
	}
	return out
}

func (s *Service) filterCurrentDirectLatencyProbeCandidates(ctx context.Context, candidates []Candidate) []Candidate {
	if len(candidates) == 0 {
		return candidates
	}
	type directSource struct {
		channel     storage.Channel
		credentials []storage.ChannelDirectCredential
	}
	sources := make(map[uint]directSource)
	failed := make(map[uint]struct{})
	out := make([]Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Channel.CredentialMode != storage.CredentialModeDirect {
			out = append(out, candidate)
			continue
		}
		if s == nil || s.Store == nil || s.Channels == nil {
			continue
		}
		channelID := candidate.Channel.ID
		if _, unavailable := failed[channelID]; unavailable {
			continue
		}
		source, loaded := sources[channelID]
		if !loaded {
			current, err := s.Channels.WithContext(ctx).FindByID(channelID)
			if err != nil || current == nil {
				failed[channelID] = struct{}{}
				continue
			}
			credentials, err := s.Channels.WithContext(ctx).ListDirectCredentials(channelID)
			if err != nil {
				failed[channelID] = struct{}{}
				continue
			}
			source = directSource{channel: *current, credentials: credentials}
			sources[channelID] = source
		}
		key, err := s.Store.WithContext(ctx).FindUpstreamKey(channelID, strings.TrimSpace(candidate.Rate.ModelName))
		if err != nil || !directLatencyProbeCandidateMatchesCurrentProjection(candidate, source.channel, *key, source.credentials) {
			continue
		}
		out = append(out, candidate)
	}
	return out
}

func directLatencyProbeCandidateMatchesCurrentProjection(candidate Candidate, current storage.Channel, key storage.GatewayUpstreamKey, credentials []storage.ChannelDirectCredential) bool {
	if candidate.Channel.ID == 0 || candidate.Channel.ID != current.ID || key.ChannelID != current.ID ||
		candidate.Channel.CredentialMode != storage.CredentialModeDirect || current.CredentialMode != storage.CredentialModeDirect ||
		candidate.Channel.UpstreamAccountIdentityRevision == 0 || candidate.Channel.UpstreamAccountIdentityRevision != current.UpstreamAccountIdentityRevision ||
		candidate.Channel.AdminRevision == 0 || candidate.Channel.AdminRevision != current.AdminRevision ||
		normalizeGatewayApp(candidate.App) != normalizeGatewayApp(current.GatewayApp) || !key.Enabled || key.Provisioning ||
		canonicalGroupName(candidate.Rate.ModelName) != canonicalGroupName(key.GroupName) ||
		strings.TrimRight(strings.TrimSpace(candidate.DirectBaseURL), "/") != strings.TrimRight(strings.TrimSpace(key.DirectBaseURL), "/") {
		return false
	}
	models, available := directProjectedModels(current, key)
	if !available || len(models) != len(candidate.ModelIDs) {
		return false
	}
	for index := range models {
		if models[index] != candidate.ModelIDs[index] {
			return false
		}
	}
	for _, credential := range credentials {
		if credential.ChannelID == current.ID && directProjectedCredentialMatches(credential, key) {
			return true
		}
	}
	return false
}

const (
	// DefaultLatencyProbeMaxCandidates is shared with the scheduler so its
	// run-level deadline covers the same number of candidates as the gateway.
	DefaultLatencyProbeMaxCandidates = 5
	DefaultLatencyProbeTimeout       = 30 * time.Second

	// LatencyProbeConfirmationAttempts is the number of consecutive transient
	// probe failures required before adaptive routing records one failure.
	LatencyProbeConfirmationAttempts = 3
	// DefaultLatencyProbeBudgetUSD is used by the scheduler. Direct callers can
	// set zero to retain the legacy candidate-count-only limit.
	DefaultLatencyProbeBudgetUSD = 0.05
)

const gatewayLatencyProbeAttempts = LatencyProbeConfirmationAttempts

const (
	latencyProbeMaxInterval       = 30 * time.Minute
	latencyProbeMixedRetryDelay   = 5 * time.Second
	latencyProbeCoolingNearWindow = time.Minute
)

type latencyProbeSelection struct {
	candidate Candidate
	interval  time.Duration
	order     int
	unprobed  bool
	overdue   time.Duration
}

// selectDueLatencyProbeCandidates consumes candidates in the exact order
// returned by smart routing and excludes every healthy route. Isolated routes
// stay eligible until a confirmed probe recovery clears their failure chain.
func selectDueLatencyProbeCandidates(candidates []Candidate, states []storage.GatewayLatencyProbeState, app storage.GatewayApp, maxCandidates int, base time.Duration, now time.Time) ([]Candidate, []time.Duration, error) {
	selected, intervals, _, err := selectDueLatencyProbeCandidatesWithBudget(candidates, states, app, maxCandidates, base, now, 0)
	return selected, intervals, err
}

func selectDueLatencyProbeCandidatesWithBudget(candidates []Candidate, states []storage.GatewayLatencyProbeState, app storage.GatewayApp, maxCandidates int, base time.Duration, now time.Time, maxCostUSD float64) ([]Candidate, []time.Duration, float64, error) {
	if maxCandidates <= 0 {
		maxCandidates = DefaultLatencyProbeMaxCandidates
	}
	if base <= 0 {
		base = DefaultLatencyProbeTimeout
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	// The caller passes a probe timeout as base only for backwards-compatible
	// direct calls. Scheduler callers provide their configured interval below;
	// clamp an accidental huge timeout to a useful cadence.
	if base > latencyProbeMaxInterval {
		base = latencyProbeMaxInterval
	}
	stateByKey := make(map[string]storage.GatewayLatencyProbeState, len(states))
	for _, state := range states {
		if state.GatewayApp != app {
			continue
		}
		stateByKey[latencyProbeStateKey(state.ChannelID, state.ChannelIdentityRevision, state.GroupName, state.UpstreamGroupID, state.PresenceGeneration)] = state
	}
	due := make([]latencyProbeSelection, 0, len(candidates))
	for rank, candidate := range candidates {
		remaining := latencyProbeCooldownRemaining(candidate, now)
		// Healthy candidates collect latency passively from real requests. A
		// scheduled active probe is allowed only while the exact route is isolated
		// and has a current model that can be used for
		// this protocol. Missing managed facts or an empty direct projection must
		// fail closed before a worker or durable schedule entry is created.
		if !candidate.CoolingDown || !latencyProbeCandidateHasCurrentModel(candidate) {
			continue
		}
		key := latencyProbeCandidateStateKey(candidate)
		state, exists := stateByKey[key]
		interval := latencyProbeInterval(base, rank, candidate.CoolingDown, remaining)
		dueAt := latencyProbeEffectiveDueAt(candidate, state, exists, interval, now)
		if dueAt.After(now) {
			continue
		}
		unprobed := !exists || state.LastProbeAt == nil
		overdue := time.Duration(0)
		if exists && state.NextProbeAt != nil && !state.NextProbeAt.After(now) {
			overdue = now.Sub(*state.NextProbeAt)
		}
		due = append(due, latencyProbeSelection{candidate: candidate, interval: interval, order: rank, unprobed: unprobed, overdue: overdue})
	}
	sort.SliceStable(due, func(i, j int) bool {
		left, right := due[i], due[j]
		// Bootstrap a newly isolated route before one that already has recovery
		// evidence, then prefer the most overdue persisted schedule.
		if left.unprobed != right.unprobed {
			return left.unprobed
		}
		if left.overdue != right.overdue {
			return left.overdue > right.overdue
		}
		return left.order < right.order
	})
	selected := make([]Candidate, 0, len(due))
	intervals := make([]time.Duration, 0, len(due))
	usedCost := 0.0
	for _, item := range due {
		if len(selected) >= maxCandidates {
			break
		}
		candidateCost := item.candidate.ProbeEstimatedCostUSD * float64(LatencyProbeConfirmationAttempts)
		if maxCostUSD > 0 && candidateCost > 0 && usedCost+candidateCost > maxCostUSD {
			continue
		}
		selected = append(selected, item.candidate)
		intervals = append(intervals, item.interval)
		usedCost += candidateCost
	}
	return selected, intervals, usedCost, nil
}

func latencyProbeCandidateHasCurrentModel(candidate Candidate) bool {
	if latencyProbeModelForCandidate(candidate) == "" {
		return false
	}
	if candidate.Channel.CredentialMode == storage.CredentialModeDirect {
		// Direct ModelIDs are an administrator-owned runtime projection. Their
		// exact source identity is revalidated at the dispatch fence instead of
		// being tied to the managed model-synchronization clock.
		return true
	}
	return candidate.ModelsSyncedAt != nil &&
		strings.TrimSpace(candidate.ModelsError) == "" &&
		!candidate.ModelsSyncedAt.Before(candidate.UpstreamGeneration.DiscoveredAt)
}

func latencyProbeCooldownRemaining(candidate Candidate, now time.Time) time.Duration {
	if candidate.CooldownUntil != nil {
		return candidate.CooldownUntil.Sub(now)
	}
	return candidate.CooldownRemaining
}

// latencyProbeEffectiveDueAt lets a new cooldown pull a historical far-future
// state forward. The caller has already excluded healthy candidates.
func latencyProbeEffectiveDueAt(candidate Candidate, state storage.GatewayLatencyProbeState, exists bool, interval time.Duration, now time.Time) time.Time {
	// Every eligible candidate gets one bootstrap probe. A state row without a
	// completed probe is also treated as bootstrap state so a partially written
	// schedule cannot starve it.
	if !exists || state.NextProbeAt == nil || state.LastProbeAt == nil {
		return now
	}
	dueAt := *state.NextProbeAt
	// The rank supplied by smart routing can change between scheduler passes.
	// A persisted next time was calculated using the old rank, so use the
	// current rank interval from the last completed probe as an earlier bound.
	// This accelerates a promoted candidate without postponing an already due
	// or otherwise shorter schedule.
	if state.LastProbeAt != nil {
		rankDueAt := state.LastProbeAt.Add(interval)
		if rankDueAt.Before(dueAt) {
			dueAt = rankDueAt
		}
	}
	// A new data-plane failure can isolate a route long after a historical probe
	// scheduled a distant next run. Pull it forward exactly once. Subsequent
	// failures from that probe precede the persisted LastProbeAt and therefore
	// cannot create a hot loop.
	if candidate.CoolingDown && candidate.LastRoutingFailureAt != nil &&
		state.LastProbeAt != nil && state.LastProbeAt.Before(*candidate.LastRoutingFailureAt) {
		return now
	}
	return dueAt
}

func latencyProbeStateKey(channelID uint, channelIdentityRevision uint64, groupName string, groupID, generation uint64) string {
	return fmt.Sprintf("%d\x00%d\x00%s\x00%d\x00%d", channelID, channelIdentityRevision, canonicalGroupName(groupName), groupID, generation)
}

func latencyProbeCandidateStateKey(candidate Candidate) string {
	groupName := candidate.Rate.ModelName
	generation := candidate.UpstreamGeneration
	if generation.Valid() {
		// The identity was canonicalized by PostgreSQL. Reuse it instead of
		// relying on Go's Unicode case folding for a non-ASCII group name.
		groupName = generation.CanonicalGroupName
	}
	return latencyProbeStateKey(candidate.Channel.ID, candidate.Channel.UpstreamAccountIdentityRevision, groupName, uint64(generation.ID), generation.PresenceGeneration)
}

func latencyProbeInterval(base time.Duration, rank int, cooling bool, remaining time.Duration) time.Duration {
	if base <= 0 {
		base = time.Minute
	}
	if base > latencyProbeMaxInterval {
		base = latencyProbeMaxInterval
	}
	if cooling {
		if remaining <= latencyProbeCoolingNearWindow {
			return base
		}
		// A long way from recovery is deliberately sampled less often, while
		// every additional distance bucket doubles the interval.
		buckets := int(remaining / latencyProbeCoolingNearWindow)
		if buckets < 1 {
			buckets = 1
		}
		if buckets > 5 {
			buckets = 5
		}
		for i := 1; i < buckets; i++ {
			if base >= latencyProbeMaxInterval/2 {
				return latencyProbeMaxInterval
			}
			base *= 2
		}
		if base > latencyProbeMaxInterval {
			return latencyProbeMaxInterval
		}
		return base
	}
	if rank < 0 {
		rank = 0
	}
	for i := 0; i < rank; i++ {
		if base >= latencyProbeMaxInterval/2 {
			return latencyProbeMaxInterval
		}
		base *= 2
	}
	if base > latencyProbeMaxInterval {
		return latencyProbeMaxInterval
	}
	return base
}

func (s *Service) persistLatencyProbeSchedule(candidate Candidate, app storage.GatewayApp, result LatencyProbeResult, interval time.Duration, now time.Time) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway store is unavailable")
	}
	healthFence, hasHealthFence := RoutingHealthFenceForCandidate(candidate)
	if !hasHealthFence {
		return storage.ErrUpstreamGroupGenerationChanged
	}
	if interval <= 0 {
		interval = time.Minute
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	next := now.Add(interval)
	if result.confirmationMixed {
		next = now.Add(latencyProbeMixedRetryDelay)
	}
	state := &storage.GatewayLatencyProbeState{
		ChannelID: healthFence.ChannelID, ChannelIdentityRevision: healthFence.ChannelIdentityRevision,
		GroupName: healthFence.CanonicalGroupName, GatewayApp: app,
		UpstreamGroupID: uint64(healthFence.UpstreamGroupID), PresenceGeneration: healthFence.PresenceGeneration,
		NextProbeAt: &next, LastProbeAt: &now,
	}
	if result.deferred {
		// A local admission deferral must not erase a prior confirmed outcome.
		// It is scheduler backpressure, not upstream evidence, so preserve the
		// existing confirmation fields while advancing only the next due time.
		if previous, findErr := s.Store.FindLatencyProbeStateForRoutingHealth(healthFence, app); findErr != nil {
			return findErr
		} else if previous != nil {
			state.ConfirmationOutcome = previous.ConfirmationOutcome
			state.ConfirmationCount = previous.ConfirmationCount
			state.LastConfirmedAt = previous.LastConfirmedAt
		}
	}
	switch result.confirmation {
	case "success", "failure":
		confirmedAt := now
		state.ConfirmationOutcome = result.confirmation
		state.ConfirmationCount = LatencyProbeConfirmationAttempts
		state.LastConfirmedAt = &confirmedAt
	}
	return s.Store.UpsertLatencyProbeStateForRoutingHealth(healthFence, state)
}

// runLatencyProbe confirms transient probe failures before changing routing
// state. A failed confirmation round records all three physical failures.
func (s *Service) runLatencyProbe(ctx context.Context, app storage.GatewayApp, candidate Candidate, timeout time.Duration) (result LatencyProbeResult) {
	defer func() {
		result.completedAt = time.Now().UTC()
	}()
	ctx = withManagedKeyMaintenanceProbe(ctx)
	outcomes := make([]string, 0, gatewayLatencyProbeAttempts)
	var failure LatencyProbeResult
	for attempt := 1; attempt <= gatewayLatencyProbeAttempts; attempt++ {
		result = s.runLatencyProbeAttempt(ctx, app, candidate, timeout)
		if result.deferred {
			return result
		}
		if result.Error == "" {
			outcomes = append(outcomes, "success")
		} else if result.retryable && result.failureKind != "" {
			outcomes = append(outcomes, "failure")
			failure = result
		} else if result.retryable {
			// 429 is retryable for probe evidence but intentionally neutral for
			// routing health, so it can never become a cooldown.
			outcomes = append(outcomes, "neutral")
		} else {
			return result
		}
		if latencyProbeCallerCanceled(ctx, nil) {
			return result
		}
	}
	if allProbeOutcomes(outcomes, "success") {
		result.confirmation = "success"
		successAt := time.Now().UTC()
		if healthFence, ok := RoutingHealthFenceForCandidate(candidate); ok {
			s.RecoverUpstreamGroupRoutingFromConfirmedLatencyProbeForRoutingHealthAt(healthFence, app, successAt)
		}
		if modelFence, ok := ModelFactFenceForCandidate(candidate); ok {
			_ = s.Store.MarkModelCapabilityForIdentity(modelFence, result.ModelName, true, "")
		}
		result.Error = ""
		return result
	}
	if allProbeOutcomes(outcomes, "failure") {
		result = failure
		result.confirmation = "failure"
		if healthFence, ok := RoutingHealthFenceForCandidate(candidate); ok {
			s.MarkUpstreamGroupRoutingFailureForRoutingHealth(healthFence, failure.failureKind, failure.failureStatus, failure.failureMessage, gatewayLatencyProbeAttempts)
		}
		return result
	}
	if allProbeOutcomes(outcomes, "neutral") {
		// Repeating a 429 at the fast mixed-result cadence would amplify an
		// upstream limit. It stays non-health-changing and returns at the
		// normal adaptive cadence instead.
		result.confirmationNeutral = true
		return result
	}
	result.confirmationMixed = true
	result.Error = "latency probe confirmation inconclusive"
	return result
}

func allProbeOutcomes(outcomes []string, want string) bool {
	if len(outcomes) != gatewayLatencyProbeAttempts {
		return false
	}
	for _, outcome := range outcomes {
		if outcome != want {
			return false
		}
	}
	return true
}

func (s *Service) runLatencyProbeAttempt(ctx context.Context, app storage.GatewayApp, candidate Candidate, timeout time.Duration) LatencyProbeResult {
	groupName := strings.TrimSpace(candidate.Rate.ModelName)
	result := LatencyProbeResult{
		App:         app,
		ChannelID:   candidate.Channel.ID,
		ChannelName: candidate.Channel.Name,
		GroupName:   groupName,
		ModelName:   latencyProbeModelForCandidate(candidate),
	}
	start := time.Now()
	directCandidateIdentityRevision := candidate.Channel.UpstreamAccountIdentityRevision
	if result.ModelName == "" {
		result.Error = "missing latency probe model"
		s.logLatencyProbe(result, candidate, start, time.Now())
		return result
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	keyLease, err := s.EnsureUpstreamAPIKeyLeaseForGroup(probeCtx, candidate.Channel.ID, groupName, nil, candidate.RawRatio)
	if err != nil {
		result.Error = sanitizeGatewayServiceError(err.Error())
		if !IsSessionScopedCapacityError(err) && !latencyProbeCallerCanceled(ctx, err) {
			result.retryable = isRetryableLatencyProbeError(ctx, err)
			if result.retryable {
				result.failureKind = RoutingFailurePreparation
				result.failureMessage = result.Error
			}
		}
		s.logLatencyProbe(result, candidate, start, time.Now())
		return result
	}
	if keyLease.ChannelSnapshot != nil {
		if keyLease.ChannelSnapshot.ID != candidate.Channel.ID ||
			keyLease.ChannelSnapshot.UpstreamAccountIdentityRevision != candidate.Channel.UpstreamAccountIdentityRevision {
			// Key preparation discovered a different account/API origin. Re-select
			// the route so health, scheduler and concurrency state all use the new
			// identity instead of mixing it with this stale candidate.
			result.deferred = true
			return result
		}
		candidate.Channel = *keyLease.ChannelSnapshot
		candidate.UpstreamAccountConcurrency = keyLease.UpstreamAccountConcurrency
	}
	if candidate.Channel.CredentialMode == storage.CredentialModeDirect {
		current, currentErr := s.Channels.WithContext(probeCtx).FindByID(candidate.Channel.ID)
		if currentErr != nil {
			result.Error = sanitizeGatewayServiceError(currentErr.Error())
			s.logLatencyProbe(result, candidate, start, time.Now())
			return result
		}
		if current.UpstreamAccountIdentityRevision != directCandidateIdentityRevision ||
			normalizeGatewayApp(current.GatewayApp) != normalizeGatewayApp(app) ||
			!directModelIDSupportsRequest(candidate.ModelIDs, result.ModelName) {
			result.deferred = true
			return result
		}
		candidate.Channel = *current
	}
	accountLease, decision := s.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencyLatencyProbe)
	if !decision.Admitted || accountLease == nil {
		result.deferred = true
		return result
	}
	defer accountLease.Release()
	accountLease.BindContext(probeCtx)
	upstreamKey := keyLease.Key
	dispatchFence, fenceErr := UpstreamDispatchFenceForCandidate(candidate, keyLease, app, result.ModelName)
	if fenceErr != nil {
		result.deferred = true
		return result
	}
	target, err := s.resolveGatewayProbeTarget(probeCtx, &candidate.Channel)
	if err == nil && candidate.Channel.CredentialMode == storage.CredentialModeDirect && strings.TrimSpace(candidate.DirectBaseURL) != "" {
		target.SiteURL = strings.TrimRight(strings.TrimSpace(candidate.DirectBaseURL), "/")
	}
	if err != nil {
		result.Error = sanitizeGatewayServiceError(err.Error())
		if !latencyProbeCallerCanceled(ctx, err) {
			result.retryable = isRetryableLatencyProbeError(ctx, err)
			if result.retryable {
				result.failureKind = RoutingFailurePreparation
				result.failureMessage = result.Error
			}
		}
		s.logLatencyProbe(result, candidate, start, time.Now())
		return result
	}
	resp, err := sendLatencyProbeRequestWithDispatchFence(probeCtx, target, upstreamKey, app, result.ModelName, timeout, accountLease, s, dispatchFence)
	finishedAt := time.Now()
	result.LatencyMs = finishedAt.Sub(start).Milliseconds()
	if err != nil {
		if errors.Is(err, ErrUpstreamDispatchIdentityChanged) {
			result.deferred = true
			return result
		}
		result.Error = sanitizeGatewayServiceError(err.Error())
		if !latencyProbeCallerCanceled(ctx, err) {
			result.retryable = isRetryableLatencyProbeError(ctx, err)
			if result.retryable {
				result.failureKind = RoutingFailureTransport
				result.failureMessage = result.Error
			}
		}
		s.logLatencyProbe(result, candidate, start, finishedAt)
		return result
	}
	result.StatusCode = resp.StatusCode
	var raw []byte
	var readErr error
	if resp.Body == nil {
		readErr = errors.New("latency probe response body is nil")
	} else {
		raw, readErr = io.ReadAll(io.LimitReader(resp.Body, gatewayProbeResponseMaxBytes+1))
		_ = resp.Body.Close()
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = sanitizeGatewayServiceError(strings.ReplaceAll(strings.TrimSpace(string(raw)), strings.TrimSpace(upstreamKey), "[redacted]"))
		if result.Error == "" && readErr != nil {
			result.Error = sanitizeGatewayServiceError(readErr.Error())
		}
		if result.Error == "" {
			result.Error = fmt.Sprintf("latency probe status %d", resp.StatusCode)
		}
		if IsExplicitAccountConcurrencyPressure(resp.StatusCode, string(raw)) {
			accountLease.ObservePressure()
		}
		if gatewayHTTPFailureInvalidatesKey(resp.StatusCode, raw) {
			// The status remains authoritative when its response body is
			// truncated unless it contains an explicit account-balance rejection.
			if fence, ok := UpstreamKeyInvalidationFenceForCandidate(candidate, keyLease); ok {
				_ = s.InvalidateUpstreamGroupKeyForIdentityContext(probeCtx, fence)
			}
		} else if connector.IsAccountBalanceFailure(resp.StatusCode, raw) {
			_ = s.MarkUpstreamAccountBalanceInsufficient(candidate, finishedAt)
		} else if !latencyProbeCallerCanceled(ctx, readErr) && resp.StatusCode == http.StatusTooManyRequests {
			// A probe may hit one upstream account's quota. It belongs in the
			// probe log only and must not cool down the entire channel group. It
			// is retried only to confirm whether the probe recovered.
			result.retryable = true
		} else if !latencyProbeCallerCanceled(ctx, readErr) && isRetryableLatencyProbeResponse(resp.StatusCode, raw) {
			result.failureKind = RoutingFailureHTTPStatus
			result.failureStatus = resp.StatusCode
			result.failureMessage = fmt.Sprintf("latency probe status %d", resp.StatusCode)
			result.retryable = true
		}
		s.logLatencyProbe(result, candidate, start, finishedAt)
		return result
	}
	if readErr != nil {
		result.Error = sanitizeGatewayServiceError(readErr.Error())
		if !latencyProbeCallerCanceled(ctx, readErr) {
			result.retryable = isRetryableLatencyProbeError(ctx, readErr)
			if result.retryable {
				result.failureKind = RoutingFailureTransport
				result.failureMessage = result.Error
			}
		}
		s.logLatencyProbe(result, candidate, start, finishedAt)
		return result
	}
	if len(raw) > gatewayProbeResponseMaxBytes {
		result.Error = "latency probe response exceeded 1 MiB"
		s.logLatencyProbe(result, candidate, start, finishedAt)
		return result
	}
	accountLease.ObserveUsefulSuccess()
	s.logLatencyProbe(result, candidate, start, finishedAt)
	return result
}

func gatewayHTTPFailureInvalidatesKey(status int, raw []byte) bool {
	return (status == http.StatusUnauthorized || status == http.StatusForbidden) && !connector.IsUpstreamChallengeResponse(status, raw) && !connector.IsAccountBalanceFailure(status, raw)
}

func (s *Service) MarkUpstreamAccountBalanceInsufficient(candidate Candidate, observedAt time.Time) error {
	if s == nil || s.Channels == nil {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	return s.Channels.MarkUpstreamAccountBalanceInsufficient(candidate.Channel.ID, candidate.Channel.UpstreamAccountIdentityRevision, observedAt)
}

func (s *Service) ClearUpstreamAccountBalanceInsufficient(candidate Candidate) error {
	if s == nil || s.Channels == nil {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	return s.Channels.ClearUpstreamAccountBalanceInsufficient(candidate.Channel.ID, candidate.Channel.UpstreamAccountIdentityRevision)
}

func latencyProbeCallerCanceled(ctx context.Context, err error) bool {
	if ctx != nil && ctx.Err() != nil {
		return true
	}
	return errors.Is(err, context.Canceled)
}

func isRetryableLatencyProbeTransportError(ctx context.Context, err error) bool {
	if err == nil || latencyProbeCallerCanceled(ctx, err) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.Timeout() || dnsErr.Temporary()
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && (networkErr.Timeout() || networkErr.Temporary()) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

func isRetryableLatencyProbeError(ctx context.Context, err error) bool {
	if err == nil || latencyProbeCallerCanceled(ctx, err) {
		return false
	}
	var httpErr *connector.HTTPError
	if errors.As(err, &httpErr) {
		return connector.IsUpstreamChallengeHTTPError(err) || isRetryableLatencyProbeStatus(httpErr.StatusCode)
	}
	return isRetryableLatencyProbeTransportError(ctx, err)
}

func isRetryableLatencyProbeStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusConflict || status == http.StatusTooEarly || status == http.StatusTooManyRequests || status >= 500 && status <= 599
}

func isRetryableLatencyProbeResponse(status int, raw []byte) bool {
	return isRetryableLatencyProbeStatus(status) || connector.IsUpstreamChallengeResponse(status, raw)
}

func (s *Service) logLatencyProbe(result LatencyProbeResult, candidate Candidate, startedAt, finishedAt time.Time) {
	if s == nil || s.Store == nil {
		return
	}
	statusCode := result.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusBadGateway
	}
	latencyMs := result.LatencyMs
	if latencyMs <= 0 {
		latencyMs = finishedAt.Sub(startedAt).Milliseconds()
	}
	log := &storage.GatewayRequestLog{
		ChannelID:          candidate.Channel.ID,
		GroupName:          strings.TrimSpace(candidate.Rate.ModelName),
		GroupRatio:         candidate.Rate.Ratio,
		CompletionRatio:    candidate.Rate.CompletionRatio,
		RawGroupRatio:      float64PtrOrNil(candidate.RawRatio),
		RawCompletionRatio: float64PtrOrNil(candidate.RawCompletionRatio),
		FundingFactor:      float64PtrOrNil(candidate.FundingFactor),
		CostBasisVersion:   candidate.CostBasisVersion,
		CostBasisSource:    candidate.CostBasisSource,
		ModelName:          result.ModelName,
		Method:             http.MethodPost,
		Path:               latencyProbePath(result.App),
		StatusCode:         statusCode,
		LatencyMs:          latencyMs,
		RouteReason:        latencyProbeRouteReason,
		Error:              result.Error,
		CreatedAt:          finishedAt,
	}
	s.LogRequest(log)
}

func float64PtrOrNil(value float64) *float64 {
	if value <= 0 {
		return nil
	}
	copy := value
	return &copy
}

func sendLatencyProbeRequest(ctx context.Context, target gatewayProbeTarget, upstreamKey string, app storage.GatewayApp, modelName string, timeout time.Duration) (*http.Response, error) {
	return sendLatencyProbeRequestWithLease(ctx, target, upstreamKey, app, modelName, timeout, nil)
}

func sendLatencyProbeRequestWithLease(ctx context.Context, target gatewayProbeTarget, upstreamKey string, app storage.GatewayApp, modelName string, timeout time.Duration, lease *AccountConcurrencyLease) (*http.Response, error) {
	path := latencyProbePath(app)
	body := latencyProbeBody(app, modelName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(target.SiteURL, "/")+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(upstreamKey))
	req.Header.Set("x-api-key", strings.TrimSpace(upstreamKey))
	req.Header.Set("Content-Type", "application/json")
	if app == storage.GatewayAppClaude {
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	client, err := modelProbeClientWithCacheKey(target.ProxyURL, target.ProxyCacheKey, timeout)
	if err != nil {
		return nil, err
	}
	response, err := netproxy.DoWithoutRedirects(client, req)
	lease.BindHTTPResponse(response, err)
	return response, err
}

func latencyProbePath(app storage.GatewayApp) string {
	if app == storage.GatewayAppClaude {
		return "/v1/messages"
	}
	return "/v1/chat/completions"
}

func latencyProbeBody(app storage.GatewayApp, modelName string) string {
	escapedModel, _ := json.Marshal(strings.TrimSpace(modelName))
	if app == storage.GatewayAppClaude {
		return fmt.Sprintf(`{"model":%s,"max_tokens":1,"messages":[{"role":"user","content":"ping"}]}`, escapedModel)
	}
	return fmt.Sprintf(`{"model":%s,"messages":[{"role":"user","content":"ping"}],"max_tokens":1,"stream":false}`, escapedModel)
}

func latencyProbeModelForCandidate(candidate Candidate) string {
	models := candidate.ModelIDs
	if len(models) == 0 {
		return ""
	}
	best := ""
	bestScore := int(^uint(0) >> 1)
	for _, model := range models {
		if candidate.Channel.CredentialMode != storage.CredentialModeDirect {
			if family := modelFamilyFromName(model); family != "" && !familySupportsGatewayApp(family, candidate.App) {
				continue
			}
		}
		score := latencyProbeModelRank(model)
		if best == "" || score < bestScore {
			best = strings.TrimSpace(model)
			bestScore = score
		}
	}
	if best != "" {
		return best
	}
	return ""
}

func latencyProbeModelRank(model string) int {
	name := strings.ToLower(strings.TrimSpace(model))
	score := 100
	for _, item := range []struct {
		token string
		score int
	}{
		{"flash-lite", 0},
		{"nano", 1},
		{"haiku", 2},
		{"mini", 3},
		{"flash", 4},
		{"lite", 5},
	} {
		if strings.Contains(name, item.token) && item.score < score {
			score = item.score
		}
	}
	return score
}

func (s *Service) LogRequest(log *storage.GatewayRequestLog) {
	if s == nil || s.Store == nil || log == nil {
		return
	}
	// Complete the monetary snapshot at write time for callers that construct
	// logs without carrying the selected Candidate through their stack.
	if log.ChannelID > 0 && log.FundingFactor == nil && !storage.IsGatewayRequestPricingFenceUnavailable(log) {
		factor := 1.0
		var version uint64
		if s.Costing != nil {
			if state, err := s.Costing.State(log.ChannelID); err == nil && state != nil {
				if state.FundingFactor >= 0 {
					factor = state.FundingFactor
				}
				version = state.Version
			}
		}
		log.FundingFactor = &factor
		if log.CostBasisSource == "" {
			log.CostBasisSource = "default"
		}
		if log.CostBasisVersion == 0 {
			log.CostBasisVersion = version
		}
		if factor > 0 && log.RawGroupRatio == nil && log.GroupRatio > 0 {
			raw := log.GroupRatio / factor
			log.RawGroupRatio = &raw
		}
		if factor > 0 && log.RawCompletionRatio == nil && log.CompletionRatio > 0 {
			raw := log.CompletionRatio / factor
			log.RawCompletionRatio = &raw
		}
		if factor > 0 && log.RawCost == nil && log.Cost > 0 {
			raw := log.Cost / factor
			log.RawCost = &raw
		}
	}
	if err := s.Store.AppendRequestLog(log); err != nil {
		if s.Log != nil {
			s.Log.Warn("gateway request log failed", "err", err)
		}
		return
	}
	s.recordRoutingLatency(log)
}

func (s *Service) TouchUpstream(channelID uint) {
	s.touchUpstreamIfDueAt(channelID, "", time.Now())
}

func (s *Service) TouchUpstreamGroup(channelID uint, groupName string, generations ...storage.UpstreamGroupGeneration) {
	s.TouchUpstreamGroupAt(channelID, groupName, time.Now(), generations...)

}

func (s *Service) TouchUpstreamGroupAt(channelID uint, groupName string, at time.Time, generations ...storage.UpstreamGroupGeneration) {
	s.touchUpstreamIfDueAt(channelID, strings.TrimSpace(groupName), at, generations...)
}

// TouchUpstreamGroupForRoutingHealthAt is the data-plane success bridge. The
// fence is captured with the candidate before dispatch, so a late success can
// never clear health for a replacement account or group generation.
func (s *Service) TouchUpstreamGroupForRoutingHealthAt(fence storage.GatewayRoutingHealthFence, at time.Time) {
	if s == nil || s.Store == nil || !fence.Valid() {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	at = at.UTC()
	if err := s.Store.TouchUpstreamKeyForRoutingHealth(fence, at); err != nil {
		return
	}
	s.recordUpstreamTouch(upstreamTouchCacheKeyForRoutingHealth(fence), at)
}

func (s *Service) MarkUpstreamGroupRoutingFailure(channelID uint, groupName, kind string, status int, msg string, generations ...storage.UpstreamGroupGeneration) {
	if s == nil || s.Store == nil {
		return
	}
	groupName = strings.TrimSpace(groupName)
	s.forgetUpstreamTouch(channelID, groupName, generations...)
	err := s.Store.RecordUpstreamKeyRoutingFailure(
		channelID,
		groupName,
		strings.TrimSpace(kind),
		status,
		sanitizeGatewayServiceError(msg),
		time.Now(),
		generations...,
	)
	// Invalidate again after the database write. A delayed success may have
	// populated the touch throttle while this failure was waiting on storage;
	// the next real success must still be allowed to clear the retained failure.
	s.forgetUpstreamTouch(channelID, groupName, generations...)
	if err != nil && !errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) && s.Log != nil {
		s.Log.Warn("record adaptive routing failure failed", "channel_id", channelID, "group", groupName, "err", err)
	}
}

// MarkUpstreamGroupRoutingFailureForRoutingHealth is the data-plane failure
// bridge. It refuses an incomplete identity rather than falling back to the
// same-named group that happens to be current when persistence runs.
func (s *Service) MarkUpstreamGroupRoutingFailureForRoutingHealth(fence storage.GatewayRoutingHealthFence, kind string, status int, msg string, occurrences ...int) {
	if s == nil || s.Store == nil || !fence.Valid() {
		return
	}
	failureCount := 1
	if len(occurrences) > 0 && occurrences[0] > 0 {
		failureCount = occurrences[0]
	}
	key := upstreamTouchCacheKeyForRoutingHealth(fence)
	s.forgetUpstreamTouchKey(key)
	err := s.Store.RecordUpstreamKeyRoutingFailuresForRoutingHealth(
		fence,
		strings.TrimSpace(kind),
		status,
		sanitizeGatewayServiceError(msg),
		time.Now(),
		failureCount,
	)
	// A success can populate the local watermark while this write waits on the
	// canonical database lock. Ensure the next success still reaches storage.
	s.forgetUpstreamTouchKey(key)
	if err != nil && !errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) &&
		!errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) && s.Log != nil {
		s.Log.Warn("record identity-fenced adaptive routing failure failed", "channel_id", fence.ChannelID, "group", fence.CanonicalGroupName, "err", err)
	}
}

func (s *Service) ApplyRequestLogManualRoutingCooldown(requestLogID uint, actor, idempotencyKey string) (*storage.GatewayManualRoutingCooldownResult, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	duration := s.CurrentRoutingConfig().FailurePolicy.ManualCooldown
	if duration <= 0 {
		return nil, errors.New("manual routing cooldown is not configured")
	}
	result, err := s.Store.ApplyRequestLogManualRoutingCooldown(requestLogID, duration, actor, idempotencyKey, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	s.forgetUpstreamTouch(result.ChannelID, result.GroupName)
	return result, nil
}

func (s *Service) InvalidateUpstreamKey(channelID uint) {
	s.forgetUpstreamTouch(channelID, "")
	_ = s.Store.DisableUpstreamKey(channelID, "", "upstream key invalidated")
}

func (s *Service) InvalidateUpstreamGroupKey(channelID uint, groupName string, generations ...storage.UpstreamGroupGeneration) {
	groupName = strings.TrimSpace(groupName)
	s.forgetUpstreamTouch(channelID, groupName, generations...)
	_ = s.Store.DisableUpstreamKey(channelID, groupName, "upstream key invalidated", generations...)
}

// InvalidateUpstreamGroupKeyForGeneration is the request-path entry point for
// explicit authentication evidence. It never falls back to the legacy
// unfenced key lookup: a group removed and recreated while a request is in
// flight must not have its replacement key disabled by the old response.
func (s *Service) InvalidateUpstreamGroupKeyForGeneration(channelID uint, groupName string, generation storage.UpstreamGroupGeneration) {
	if !generation.Valid() || generation.ChannelID != channelID ||
		canonicalGroupName(generation.CanonicalGroupName) != canonicalGroupName(groupName) {
		return
	}
	s.InvalidateUpstreamGroupKey(channelID, groupName, generation)
}

// InvalidateUpstreamGroupKeyForIdentity is the data-plane invalidation path.
// It refuses incomplete evidence and cannot disable a key installed after the
// observed request acquired its lease.
func (s *Service) InvalidateUpstreamGroupKeyForIdentity(fence storage.GatewayUpstreamKeyInvalidationFence) {
	_ = s.InvalidateUpstreamGroupKeyForIdentityContext(context.Background(), fence)
}

// InvalidateUpstreamGroupKeyForIdentityContext orders an exact 401/403
// invalidation with the final dispatch boundary. Cancellation of the
// downstream request must not discard authentication evidence already
// received from upstream, while context values still carry lock ownership for
// composed operations.
func (s *Service) InvalidateUpstreamGroupKeyForIdentityContext(ctx context.Context, fence storage.GatewayUpstreamKeyInvalidationFence) error {
	if s == nil || s.Store == nil || !fence.Valid() {
		return storage.ErrGatewayUpstreamKeyInvalidationFenceChanged
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithoutCancel(ctx)
	return s.WithChannelOperationLockAndThen(ctx, fence.ChannelID, func(lockedCtx context.Context) error {
		if err := s.Store.DisableUpstreamKeyForIdentityContext(lockedCtx, fence, "upstream key invalidated"); err != nil {
			return err
		}
		s.forgetUpstreamTouch(fence.ChannelID, fence.CanonicalGroupName, fence.UpstreamGeneration())
		return nil
	})
}

func (s *Service) touchUpstreamIfDueAt(channelID uint, groupName string, at time.Time, generations ...storage.UpstreamGroupGeneration) {
	if s == nil || s.Store == nil || channelID == 0 {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	at = at.UTC()
	key := upstreamTouchCacheKey(channelID, groupName, generations...)
	// A local success cache cannot observe a failure written by another service
	// instance. Every real success must reach storage, where the canonical
	// routing-health lock can compare it with the current failure facts.
	if err := s.Store.TouchUpstreamKey(channelID, key.groupName, at, generations...); err != nil {
		return
	}
	s.recordUpstreamTouch(key, at)
}

func (s *Service) recordUpstreamTouch(key upstreamTouchKey, at time.Time) {
	if s == nil {
		return
	}
	s.touchMu.Lock()
	if s.upstreamTouchAt == nil {
		s.upstreamTouchAt = make(map[upstreamTouchKey]time.Time)
	}
	if last, ok := s.upstreamTouchAt[key]; !ok || at.After(last) {
		s.upstreamTouchAt[key] = at
	}
	s.touchMu.Unlock()
}

func (s *Service) forgetUpstreamTouch(channelID uint, groupName string, generations ...storage.UpstreamGroupGeneration) {
	if s == nil || channelID == 0 {
		return
	}
	s.forgetUpstreamTouchKey(upstreamTouchCacheKey(channelID, groupName, generations...))
}

func (s *Service) forgetUpstreamTouchKey(key upstreamTouchKey) {
	if s == nil || key.channelID == 0 {
		return
	}
	s.touchMu.Lock()
	delete(s.upstreamTouchAt, key)
	s.touchMu.Unlock()
}

func upstreamTouchCacheKey(channelID uint, groupName string, generations ...storage.UpstreamGroupGeneration) upstreamTouchKey {
	key := upstreamTouchKey{channelID: channelID, groupName: strings.TrimSpace(groupName)}
	if len(generations) == 0 || !generations[0].Valid() {
		return key
	}
	generation := generations[0]
	key.channelID = generation.ChannelID
	key.groupName = generation.CanonicalGroupName
	key.upstreamGroupID = generation.ID
	key.presenceGeneration = generation.PresenceGeneration
	return key
}

func upstreamTouchCacheKeyForRoutingHealth(fence storage.GatewayRoutingHealthFence) upstreamTouchKey {
	if !fence.Valid() {
		return upstreamTouchKey{}
	}
	return upstreamTouchKey{
		channelID: fence.ChannelID, channelIdentityRevision: fence.ChannelIdentityRevision,
		groupName: fence.CanonicalGroupName, upstreamGroupID: fence.UpstreamGroupID,
		presenceGeneration: fence.PresenceGeneration,
	}
}

// claimUpstreamIdentityRepair lets an old, otherwise usable key take the
// slower identity-repair path once. Failed repairs are rate-limited so the
// gateway hot path never repeatedly scans the upstream management API.
func (s *Service) claimUpstreamIdentityRepair(channelID uint, groupName string) bool {
	if s == nil || channelID == 0 || strings.TrimSpace(groupName) == "" {
		return false
	}
	key := upstreamTouchKey{channelID: channelID, groupName: strings.TrimSpace(groupName)}
	now := time.Now()
	s.touchMu.Lock()
	defer s.touchMu.Unlock()
	if s.upstreamIdentityRepairAt == nil {
		s.upstreamIdentityRepairAt = make(map[upstreamTouchKey]time.Time)
	}
	if last, ok := s.upstreamIdentityRepairAt[key]; ok && last.Add(upstreamIdentityRepairRetryInterval).After(now) {
		return false
	}
	s.upstreamIdentityRepairAt[key] = now
	return true
}

func (s *Service) clearUpstreamIdentityRepair(channelID uint, groupName string) {
	if s == nil || channelID == 0 {
		return
	}
	s.touchMu.Lock()
	delete(s.upstreamIdentityRepairAt, upstreamTouchKey{channelID: channelID, groupName: strings.TrimSpace(groupName)})
	s.touchMu.Unlock()
}

func channelAvailable(ch storage.Channel) bool {
	return len(ChannelAvailabilityReasons(ch)) == 0
}

func (s *Service) revealExistingUpstreamKey(ctx context.Context, channelID uint, groupName string, groupID *int64) (*connector.APIKey, string, error) {
	selected, err := s.findReusableUpstreamKey(ctx, channelID, groupName, groupID)
	if err != nil {
		return nil, "", err
	}
	key, err := s.Keys.RevealAPIKey(ctx, channelID, selected.ID)
	if err != nil {
		return nil, "", err
	}
	return selected, strings.TrimSpace(key), nil
}

func (s *Service) adoptExistingUpstreamKey(ctx context.Context, channelID uint, cacheGroupName, upstreamGroupName string, groupID *int64, groupRatio float64) (string, error) {
	identity, plain, err := s.revealExistingUpstreamKey(ctx, channelID, upstreamGroupName, groupID)
	if err != nil {
		return "", err
	}
	plain = strings.TrimSpace(plain)
	if plain == "" {
		return "", errors.New("existing managed upstream key revealed an empty key")
	}
	if err := s.saveUpstreamKeyContext(ctx, channelID, plain, cacheGroupName, groupRatio, identity, storage.GatewayUpstreamKeyProvenanceAdopted); err != nil {
		return "", err
	}
	return plain, nil
}

func (s *Service) recoverCachedManagedUpstreamKey(ctx context.Context, channelID uint, cacheGroupName, upstreamGroupName string, groupID *int64, groupRatio float64, cached *storage.GatewayUpstreamKey) (string, bool, error) {
	if cached == nil || cached.UpstreamKeyID <= 0 || !isManagedUpstreamKeyName(cached.UpstreamKeyName) {
		return "", false, nil
	}
	expectedName := strings.TrimSpace(cached.UpstreamKeyName)
	remote, err := s.findUpstreamKeyByID(ctx, channelID, cached.UpstreamKeyID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if conflictErr := s.rejectManagedUpstreamKeyNameConflict(ctx, channelID, expectedName, upstreamGroupName, groupID); conflictErr != nil {
			return "", true, conflictErr
		}
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("verify cached upstream key %d: %w", cached.UpstreamKeyID, err)
	}
	if remote.ID != cached.UpstreamKeyID || !recoverableManagedUpstreamKeyForName(*remote, expectedName, upstreamGroupName, groupID, true) {
		return "", true, fmt.Errorf("cached upstream key %d identity or creation fingerprint changed; refusing automatic replacement", cached.UpstreamKeyID)
	}
	plain, err := s.Keys.RevealAPIKey(ctx, channelID, remote.ID)
	if err != nil {
		return "", true, fmt.Errorf("reveal cached upstream key %d: %w", remote.ID, err)
	}
	plain = strings.TrimSpace(plain)
	if plain == "" {
		return "", true, fmt.Errorf("reveal cached upstream key %d returned an empty key", remote.ID)
	}
	if err := s.saveUpstreamKeyContext(ctx, channelID, plain, cacheGroupName, groupRatio, remote); err != nil {
		return "", true, err
	}
	return plain, true, nil
}

func (s *Service) cachedUpstreamKeyMatchesRemote(ctx context.Context, channelID uint, cached *storage.GatewayUpstreamKey) (bool, error) {
	if s == nil || s.Keys == nil || cached == nil || cached.UpstreamKeyID <= 0 || strings.TrimSpace(cached.UpstreamKeyName) == "" || strings.TrimSpace(cached.APIKeyCipher) == "" {
		return false, nil
	}
	remote, err := s.findUpstreamKeyByID(ctx, channelID, cached.UpstreamKeyID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if remote.ID != cached.UpstreamKeyID || !recoverableManagedUpstreamKeyForName(*remote, cached.UpstreamKeyName, cached.GroupName, nil, true) {
		return false, nil
	}
	plain, err := s.Cipher.Decrypt(cached.APIKeyCipher)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(plain) == "" {
		return false, nil
	}
	revealed, err := s.Keys.RevealAPIKey(ctx, channelID, cached.UpstreamKeyID)
	if err != nil || strings.TrimSpace(revealed) == "" {
		return false, nil
	}
	return HashKey(revealed) == HashKey(plain), nil
}

func (s *Service) findReusableUpstreamKey(ctx context.Context, channelID uint, groupName string, groupID *int64) (*connector.APIKey, error) {
	var selected *connector.APIKey
	seenByName := make(map[string]int64)
	for _, search := range managedUpstreamKeySearchesForGroup(groupID) {
		for pageNum := 1; pageNum <= upstreamKeyLookupMaxPages; pageNum++ {
			query := connector.APIKeyQuery{Page: pageNum, PageSize: upstreamKeyLookupPageSize, Search: search}
			if groupID != nil && *groupID > 0 {
				query.GroupID = strconv.FormatInt(*groupID, 10)
			}
			page, err := s.listAPIKeysUsingPassCache(ctx, channelID, query)
			if err != nil {
				if search != "" {
					break
				}
				return nil, err
			}
			for _, item := range page.Items {
				if !isRecoverableManagedUpstreamKeyName(item.Name, groupName) {
					continue
				}
				if item.ID <= 0 || !recoverableManagedUpstreamKey(item, groupName, groupID, false) {
					return nil, fmt.Errorf("managed upstream key name %q has an incompatible creation fingerprint", strings.TrimSpace(item.Name))
				}
				name := strings.TrimSpace(item.Name)
				if previousID, exists := seenByName[name]; exists && previousID != item.ID {
					return nil, fmt.Errorf("managed upstream key name %q is ambiguous", name)
				}
				seenByName[name] = item.ID
				item := item
				if selected == nil || item.ID < selected.ID {
					selected = &item
				}
			}
			complete := apiKeyPageComplete(page, pageNum, upstreamKeyLookupPageSize)
			if pageNum == upstreamKeyLookupMaxPages && !complete {
				return nil, errors.New("upstream api key list exceeds safe lookup limit")
			}
			if complete {
				break
			}
		}
	}
	if selected == nil {
		return nil, gorm.ErrRecordNotFound
	}
	return selected, nil
}

func (s *Service) findUpstreamKeyByID(ctx context.Context, channelID uint, keyID int64) (*connector.APIKey, error) {
	if keyID <= 0 {
		return nil, gorm.ErrRecordNotFound
	}
	var selected *connector.APIKey
	nameIDs := make(map[string]map[int64]struct{})
	for pageNum := 1; pageNum <= upstreamKeyLookupMaxPages; pageNum++ {
		page, err := s.listAPIKeysUsingPassCache(ctx, channelID, connector.APIKeyQuery{Page: pageNum, PageSize: upstreamKeyLookupPageSize})
		if err != nil {
			return nil, err
		}
		if page != nil {
			for _, item := range page.Items {
				name := strings.TrimSpace(item.Name)
				if name != "" {
					ids := nameIDs[name]
					if ids == nil {
						ids = make(map[int64]struct{})
						nameIDs[name] = ids
					}
					ids[item.ID] = struct{}{}
				}
				if item.ID == keyID {
					if selected != nil {
						return nil, fmt.Errorf("upstream api key id %d is ambiguous", keyID)
					}
					item := item
					selected = &item
				}
			}
		}
		if apiKeyPageComplete(page, pageNum, upstreamKeyLookupPageSize) {
			if selected == nil {
				return nil, gorm.ErrRecordNotFound
			}
			name := strings.TrimSpace(selected.Name)
			if name != "" && len(nameIDs[name]) != 1 {
				return nil, fmt.Errorf("upstream api key name %q is ambiguous", name)
			}
			return selected, nil
		}
	}
	return nil, errors.New("upstream api key list exceeds safe lookup limit")
}

func (s *Service) rejectManagedUpstreamKeyNameConflict(ctx context.Context, channelID uint, expectedName, groupName string, groupID *int64) error {
	expectedName = strings.TrimSpace(expectedName)
	if !isExpectedManagedUpstreamKeyName(expectedName, groupName) {
		return errors.New("managed upstream key has an unexpected name")
	}
	matches := make(map[int64]struct{})
	incompatible := 0
	for pageNum := 1; pageNum <= upstreamKeyLookupMaxPages; pageNum++ {
		page, err := s.listAPIKeysUsingPassCache(ctx, channelID, connector.APIKeyQuery{Page: pageNum, PageSize: upstreamKeyLookupPageSize, Search: expectedName})
		if err != nil {
			return fmt.Errorf("list managed upstream key %q: %w", expectedName, err)
		}
		for _, item := range page.Items {
			if strings.TrimSpace(item.Name) != expectedName {
				continue
			}
			if item.ID <= 0 || !recoverableManagedUpstreamKeyForName(item, expectedName, groupName, groupID, true) {
				incompatible++
				continue
			}
			matches[item.ID] = struct{}{}
		}
		if apiKeyPageComplete(page, pageNum, upstreamKeyLookupPageSize) {
			break
		}
		if pageNum == upstreamKeyLookupMaxPages {
			return errors.New("managed upstream key lookup exceeds safe limit")
		}
	}
	if incompatible > 0 {
		return fmt.Errorf("managed upstream key name %q has incompatible remote records", expectedName)
	}
	if len(matches) > 0 {
		return fmt.Errorf("managed upstream key name %q exists with an unexpected remote id", expectedName)
	}
	return nil
}

func (s *Service) findCachedUpstreamIdentity(ctx context.Context, channelID uint, groupName string, groupID *int64, cachedPlain string) (*connector.APIKey, error) {
	var selected *connector.APIKey
	cachedHash := HashKey(cachedPlain)
	for _, search := range managedUpstreamKeySearchesForGroup(groupID) {
		for pageNum := 1; pageNum <= upstreamKeyLookupMaxPages; pageNum++ {
			query := connector.APIKeyQuery{Page: pageNum, PageSize: upstreamKeyLookupPageSize, Search: search}
			if groupID != nil && *groupID > 0 {
				query.GroupID = strconv.FormatInt(*groupID, 10)
			}
			page, err := s.listAPIKeysUsingPassCache(ctx, channelID, query)
			if err != nil {
				if search != "" {
					break
				}
				return nil, err
			}
			for _, item := range page.Items {
				if item.ID <= 0 || !recoverableManagedUpstreamKey(item, groupName, groupID, true) {
					continue
				}
				revealed, err := s.Keys.RevealAPIKey(ctx, channelID, item.ID)
				if err != nil || HashKey(revealed) != cachedHash {
					continue
				}
				item := item
				if selected == nil || item.ID < selected.ID {
					selected = &item
				}
			}
			complete := apiKeyPageComplete(page, pageNum, upstreamKeyLookupPageSize)
			if search == "" && pageNum == upstreamKeyLookupMaxPages && selected == nil && !complete {
				return nil, errors.New("upstream api key list exceeds safe lookup limit")
			}
			if complete {
				break
			}
		}
		if selected != nil {
			break
		}
	}
	if selected == nil {
		return nil, gorm.ErrRecordNotFound
	}
	return selected, nil
}

func apiKeyReusable(key connector.APIKey) bool {
	status := strings.ToLower(strings.TrimSpace(key.Status))
	if status != "" && status != "active" && status != "enabled" {
		return false
	}
	now := time.Now()
	switch key.Source {
	case connector.APIKeySourceNewAPI:
		if key.ObservedFields.ExpiredTime && key.ExpiredTime > 0 && key.ExpiredTime <= now.Unix() {
			return false
		}
		if key.ObservedFields.ExpiresAt && key.ExpiresAt != nil && !key.ExpiresAt.After(now) {
			return false
		}
		if key.ObservedFields.UnlimitedQuota {
			return key.UnlimitedQuota || (key.ObservedFields.Quota && key.Quota > 0)
		}
		return !key.ObservedFields.Quota || key.Quota >= 0
	case connector.APIKeySourceSub2API:
		if status != "active" {
			return false
		}
		return !key.ObservedFields.ExpiresAt || key.ExpiresAt == nil || key.ExpiresAt.After(now)
	default:
		if key.ExpiredTime > 0 && key.ExpiredTime <= now.Unix() {
			return false
		}
		if key.ExpiresAt != nil && !key.ExpiresAt.After(now) {
			return false
		}
		return key.UnlimitedQuota || key.Quota > 0
	}
}

func apiKeyPageComplete(page *connector.APIKeyPage, requestedPage, requestedPageSize int) bool {
	if page == nil {
		return true
	}
	currentPage := page.Page
	if currentPage <= 0 {
		currentPage = requestedPage
	}
	if currentPage < requestedPage {
		return false
	}
	metadataPresent := false
	if page.Pages > 0 {
		metadataPresent = true
		if currentPage < page.Pages || currentPage > page.Pages {
			return false
		}
	}
	if page.Total > 0 {
		metadataPresent = true
		pageSize := page.PageSize
		if pageSize <= 0 {
			pageSize = requestedPageSize
		}
		if pageSize <= 0 || int64(currentPage*pageSize) < page.Total {
			return false
		}
		if page.Pages > 0 {
			expectedPages := int((page.Total + int64(pageSize) - 1) / int64(pageSize))
			if page.Pages < expectedPages {
				return false
			}
		}
	}
	if metadataPresent {
		return true
	}
	return len(page.Items) < requestedPageSize
}

func (s *Service) cheapestUpstreamGroup(ctx context.Context, channelID uint) (string, *int64, float64, error) {
	if s == nil || s.Keys == nil {
		return "", nil, 0, errors.New("upstream key manager is unavailable")
	}
	groups, err := s.Keys.ListAPIKeyGroups(ctx, channelID)
	if err != nil {
		return "", nil, 0, err
	}
	if len(groups) == 0 {
		return "", nil, 0, errors.New("upstream has no api key groups")
	}
	sort.SliceStable(groups, func(i, j int) bool {
		return groups[i].Ratio < groups[j].Ratio
	})
	return strings.TrimSpace(groups[0].Name), groups[0].ID, groups[0].Ratio, nil
}

func (s *Service) resolveUpstreamGroup(ctx context.Context, channelID uint, requestedName string, requestedID *int64, requestedRatio float64, expectedIdentityRevision uint64) (string, *int64, float64, error) {
	requestedName = strings.TrimSpace(requestedName)
	if s == nil || s.Keys == nil {
		return "", nil, 0, errors.New("upstream key manager is unavailable")
	}
	groups, err := s.listAPIKeyGroupsForIdentityRevision(ctx, channelID, expectedIdentityRevision)
	if err != nil {
		return "", nil, 0, err
	}
	for _, group := range groups {
		if requestedID != nil && group.ID != nil && *requestedID == *group.ID {
			ratio := group.Ratio
			if ratio <= 0 {
				ratio = requestedRatio
			}
			return strings.TrimSpace(group.Name), group.ID, ratio, nil
		}
		if requestedName != "" && strings.EqualFold(strings.TrimSpace(group.Name), requestedName) {
			ratio := group.Ratio
			if ratio <= 0 {
				ratio = requestedRatio
			}
			return strings.TrimSpace(group.Name), group.ID, ratio, nil
		}
	}
	if requestedName == "" {
		return "", nil, 0, errors.New("upstream group is required")
	}
	return "", nil, 0, fmt.Errorf("upstream group %q not found", requestedName)
}

func (s *Service) saveUpstreamKey(channelID uint, key string, groupName string, groupRatio float64, upstream *connector.APIKey, provenances ...string) error {
	return s.saveUpstreamKeyContext(context.Background(), channelID, key, groupName, groupRatio, upstream, provenances...)
}

func (s *Service) saveUpstreamKeyContext(ctx context.Context, channelID uint, key string, groupName string, groupRatio float64, upstream *connector.APIKey, provenances ...string) error {
	cipherText, err := s.Cipher.Encrypt(strings.TrimSpace(key))
	if err != nil {
		return err
	}
	record := &storage.GatewayUpstreamKey{
		ChannelID:             channelID,
		APIKeyCipher:          cipherText,
		APIKeyPrefix:          keyPrefix(key),
		GroupName:             strings.TrimSpace(groupName),
		GroupRatio:            groupRatio,
		Enabled:               true,
		Provisioning:          false,
		ProvisioningStartedAt: nil,
		LastError:             "",
	}
	if len(provenances) > 0 {
		record.OwnershipProvenance = strings.TrimSpace(provenances[0])
	}
	if upstream != nil {
		record.UpstreamKeyID = upstream.ID
		record.UpstreamKeyName = strings.TrimSpace(upstream.Name)
	}
	if record.UpstreamKeyName == "" {
		record.UpstreamKeyName = upstreamKeyNameForGroup(groupName)
	}
	return s.Store.WithContext(ctx).UpsertUpstreamKey(record)
}

func (s *Service) saveDirectUpstreamKeyContext(ctx context.Context, channelID uint, channelIdentityRevision uint64, credential storage.ChannelDirectCredential, key string) error {
	keyCipher := strings.TrimSpace(credential.APIKeyCipher)
	key = strings.TrimSpace(key)
	if s == nil || s.Store == nil || channelID == 0 || channelIdentityRevision == 0 || credential.ID == 0 || credential.Revision == 0 || keyCipher == "" || key == "" {
		return errors.New("direct upstream key is incomplete")
	}
	return s.Store.WithContext(ctx).UpsertDirectUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channelID, UpstreamKeyName: directUpstreamKeyName,
		APIKeyCipher: keyCipher, APIKeyPrefix: keyPrefix(key), GroupName: credential.GroupName,
		GroupRatio: credential.Ratio, DirectCredentialID: &credential.ID,
		DirectCredentialRevision: credential.Revision, DirectBaseURL: credential.BaseURL,
		Enabled: true, LatencyProbeEnabled: true,
	}, channelIdentityRevision)
}

func recoveredProvisioningOwnershipProvenance(pending *storage.GatewayUpstreamKey) string {
	if pending == nil {
		return storage.GatewayUpstreamKeyProvenanceUnknown
	}
	provenance := strings.TrimSpace(pending.OwnershipProvenance)
	if provenance == storage.GatewayUpstreamKeyProvenanceProvisioningClaim && pending.Provisioning && pending.ProvisioningStartedAt != nil &&
		upstreamProvisioningKeyNamePattern.MatchString(strings.TrimSpace(pending.UpstreamKeyName)) {
		return storage.GatewayUpstreamKeyProvenanceCreated
	}
	if provenance == "" {
		return storage.GatewayUpstreamKeyProvenanceUnknown
	}
	return provenance
}

func HashKey(key string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(key)))
	return hex.EncodeToString(sum[:])
}

func apiKeyGroupRatio(key connector.APIKey) float64 {
	if key.GroupRatio > 0 {
		return key.GroupRatio
	}
	return 1
}

func apiKeyGroupMatches(key connector.APIKey, groupName string, groupID *int64) bool {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" {
		return false
	}
	names := make([]string, 0, 2)
	for _, name := range []string{key.GroupName, key.Group} {
		name = strings.TrimSpace(name)
		duplicate := false
		for _, existing := range names {
			if strings.EqualFold(existing, name) {
				duplicate = true
				break
			}
		}
		if name == "" || duplicate {
			continue
		}
		names = append(names, name)
	}
	idsMatch := groupID != nil && key.GroupID != nil && *key.GroupID == *groupID
	if len(names) == 0 {
		return idsMatch
	}
	for _, name := range names {
		if !strings.EqualFold(name, groupName) {
			return false
		}
	}
	if groupID != nil {
		return idsMatch
	}
	return true
}

func upstreamKeyNameForGroup(groupName string) string {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" {
		return upstreamKeyName
	}
	return upstreamKeyName + " - " + groupName
}

func managedUpstreamKeySearches() []string {
	return []string{upstreamKeyName, legacyUpstreamKeyName, historicUpstreamKeyName, ""}
}

func managedUpstreamKeySearchesForGroup(groupID *int64) []string {
	if groupID != nil && *groupID > 0 {
		// Only Sub2API exposes numeric API-key group identities. Its group_id
		// filter is already a complete, bounded inventory for that one group, so
		// repeating the same request with legacy name searches only multiplies a
		// slow provider's latency without adding recovery evidence.
		return []string{""}
	}
	return managedUpstreamKeySearches()
}

func isManagedUpstreamKeyName(name string) bool {
	name = strings.TrimSpace(name)
	return name == upstreamKeyName || strings.HasPrefix(name, upstreamKeyName+" - ") || upstreamProvisioningKeyNamePattern.MatchString(name) || name == legacyUpstreamKeyName || strings.HasPrefix(name, legacyUpstreamKeyName+" - ") || name == historicUpstreamKeyName || strings.HasPrefix(name, historicUpstreamKeyName+" - ")
}

// isStronglyManagedUpstreamKeyName is the ownership proof for automatic
// deletion/replacement. Only the random provisioning prefix is reserved for
// this purpose; historical human-readable names can also be chosen manually.
func isStronglyManagedUpstreamKeyName(name string) bool {
	name = strings.TrimSpace(name)
	return upstreamProvisioningKeyNamePattern.MatchString(name)
}

func recoverableManagedUpstreamKey(key connector.APIKey, groupName string, groupID *int64, allowUnobserved bool) bool {
	for _, expectedName := range recoverableManagedUpstreamKeyNames(groupName) {
		if recoverableManagedUpstreamKeyForName(key, expectedName, groupName, groupID, allowUnobserved) {
			return true
		}
	}
	return false
}

func recoverableManagedUpstreamKeyForName(key connector.APIKey, expectedName, groupName string, groupID *int64, allowUnobserved bool) bool {
	return apiKeyReusable(key) && managedUpstreamKeyFingerprintMatchesForName(key, expectedName, groupName, groupID, allowUnobserved)
}

func managedUpstreamKeyFingerprintMatches(key connector.APIKey, groupName string, groupID *int64, allowUnobserved bool) bool {
	for _, expectedName := range recoverableManagedUpstreamKeyNames(groupName) {
		if managedUpstreamKeyFingerprintMatchesForName(key, expectedName, groupName, groupID, allowUnobserved) {
			return true
		}
	}
	return false
}

func managedUpstreamKeyFingerprintMatchesForName(key connector.APIKey, expectedName, groupName string, groupID *int64, allowUnobserved bool) bool {
	expectedName = strings.TrimSpace(expectedName)
	if expectedName == "" || strings.TrimSpace(key.Name) != expectedName || !isExpectedManagedUpstreamKeyName(expectedName, groupName) {
		return false
	}
	return apiKeyGroupMatches(key, groupName, groupID) && managedUpstreamKeyCreationFingerprintMatches(key, allowUnobserved)
}

func managedUpstreamKeyDeletionFingerprintMatches(key connector.APIKey, groupName string, groupID *int64, allowUnobserved bool) bool {
	name := strings.TrimSpace(key.Name)
	nameMatches := false
	for _, expectedName := range recoverableManagedUpstreamKeyNames(groupName) {
		if strings.EqualFold(name, strings.TrimSpace(expectedName)) {
			nameMatches = true
			break
		}
	}
	if !nameMatches || !apiKeyGroupMatches(key, groupName, groupID) {
		return false
	}
	return managedUpstreamKeyCreationFingerprintMatches(key, allowUnobserved)
}

func managedUpstreamKeyCreationFingerprintMatches(key connector.APIKey, allowUnobserved bool) bool {
	switch key.Source {
	case connector.APIKeySourceNewAPI:
		return recoverableNewAPIKeyFingerprint(key, allowUnobserved)
	case connector.APIKeySourceSub2API:
		return recoverableSub2APIKeyFingerprint(key, allowUnobserved)
	default:
		return key.UnlimitedQuota && key.ExpiredTime == -1 && key.ExpiresAt == nil &&
			!key.ModelLimitsEnabled && strings.TrimSpace(key.ModelLimits) == "" &&
			strings.TrimSpace(key.AllowIPs) == "" && len(key.IPWhitelist) == 0 && len(key.IPBlacklist) == 0 &&
			key.CrossGroupRetry && key.RateLimit5h == 0 && key.RateLimit1d == 0 && key.RateLimit7d == 0
	}
}

func recoverableNewAPIKeyFingerprint(key connector.APIKey, allowUnobserved bool) bool {
	observed := key.ObservedFields
	nullFields := key.NullFields
	if nullFields.Quota || nullFields.UnlimitedQuota || nullFields.ExpiredTime || nullFields.ModelLimitsEnabled || nullFields.ModelLimits || nullFields.AllowIPs || nullFields.CrossGroupRetry {
		return false
	}
	if !allowUnobserved && (!observed.Quota || !observed.UnlimitedQuota || !observed.ExpiredTime || !observed.ModelLimitsEnabled || !observed.ModelLimits || !observed.AllowIPs || !observed.CrossGroupRetry) {
		return false
	}
	return (!observed.Quota || key.Quota == 0) &&
		(!observed.UnlimitedQuota || key.UnlimitedQuota) &&
		(!observed.ExpiredTime || key.ExpiredTime == -1) &&
		(!observed.ExpiresAt || key.ExpiresAt == nil) &&
		(!observed.ModelLimitsEnabled || !key.ModelLimitsEnabled) &&
		(!observed.ModelLimits || strings.TrimSpace(key.ModelLimits) == "") &&
		(!observed.AllowIPs || strings.TrimSpace(key.AllowIPs) == "") &&
		(!observed.IPWhitelist || len(key.IPWhitelist) == 0) &&
		(!observed.IPBlacklist || len(key.IPBlacklist) == 0) &&
		// Both values are valid ownership evidence across the policy transition:
		// historical managed keys used true, while new keys are group-pinned with
		// false. Presence/null checks above remain strict.
		(!observed.RateLimit5h || key.RateLimit5h == 0) &&
		(!observed.RateLimit1d || key.RateLimit1d == 0) &&
		(!observed.RateLimit7d || key.RateLimit7d == 0)
}

func recoverableSub2APIKeyFingerprint(key connector.APIKey, allowUnobserved bool) bool {
	observed := key.ObservedFields
	nullFields := key.NullFields
	if nullFields.Quota || nullFields.RateLimit5h || nullFields.RateLimit1d || nullFields.RateLimit7d {
		return false
	}
	if !allowUnobserved && (!observed.Quota || !observed.ExpiresAt || !observed.IPWhitelist || !observed.IPBlacklist || !observed.RateLimit5h || !observed.RateLimit1d || !observed.RateLimit7d) {
		return false
	}
	return (!observed.Quota || key.Quota == 0) &&
		(!observed.ExpiresAt || key.ExpiresAt == nil) &&
		(!observed.IPWhitelist || len(key.IPWhitelist) == 0) &&
		(!observed.IPBlacklist || len(key.IPBlacklist) == 0) &&
		(!observed.RateLimit5h || key.RateLimit5h == 0) &&
		(!observed.RateLimit1d || key.RateLimit1d == 0) &&
		(!observed.RateLimit7d || key.RateLimit7d == 0)
}

func recoverableManagedUpstreamKeyNames(groupName string) []string {
	groupName = strings.TrimSpace(groupName)
	legacyName := legacyUpstreamKeyName
	if groupName != "" {
		legacyName += " - " + groupName
	}
	historicName := historicUpstreamKeyName
	if groupName != "" {
		historicName += " - " + groupName
	}
	return []string{upstreamKeyNameForGroup(groupName), legacyName, historicName}
}

func isRecoverableManagedUpstreamKeyName(name, groupName string) bool {
	name = strings.TrimSpace(name)
	for _, expectedName := range recoverableManagedUpstreamKeyNames(groupName) {
		if name == expectedName {
			return true
		}
	}
	return false
}

func isExpectedManagedUpstreamKeyName(name, groupName string) bool {
	if isRecoverableManagedUpstreamKeyName(name, groupName) {
		return true
	}
	return upstreamProvisioningKeyNamePattern.MatchString(strings.TrimSpace(name))
}

func hasUpstreamKeyRecoveryEvidence(key *storage.GatewayUpstreamKey) bool {
	if key == nil {
		return false
	}
	return key.Provisioning || key.ProvisioningStartedAt != nil || key.UpstreamKeyID > 0 ||
		strings.TrimSpace(key.UpstreamKeyName) != "" || strings.TrimSpace(key.APIKeyCipher) != "" ||
		strings.TrimSpace(key.APIKeyPrefix) != ""
}

func hasUpstreamKeyErrorState(key *storage.GatewayUpstreamKey) bool {
	return key != nil && (strings.TrimSpace(key.ModelsError) != "" || strings.TrimSpace(key.LastError) != "")
}

func isUpstreamAPIKeyCountLimitError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, phrase := range []string{
		"最大令牌数量限制",
		"maximum token count",
		"maximum number of tokens",
		"max token count",
		"maximum api key",
		"api key limit reached",
	} {
		if strings.Contains(message, phrase) {
			return true
		}
	}
	return false
}

func bearerToken(header string) string {
	header = strings.TrimSpace(header)
	if header == "" {
		return ""
	}
	const prefix = "Bearer "
	if strings.HasPrefix(strings.ToLower(header), strings.ToLower(prefix)) {
		return strings.TrimSpace(header[len(prefix):])
	}
	return ""
}

func keyPrefix(key string) string {
	key = strings.TrimSpace(key)
	if len(key) <= 12 {
		return key
	}
	return key[:12]
}

func randomKey(prefix string) (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(raw), nil
}
