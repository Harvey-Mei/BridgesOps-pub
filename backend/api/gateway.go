package api

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/geoip"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"golang.org/x/net/http/httpguts"
	"gorm.io/gorm"
)

const gatewayBodyLimit = 64 << 20
const gatewayStreamUsageCaptureLimit = 2 << 20
const gatewayStreamPreludeLimit = 64 << 10
const gatewayCapacityNoticePreludeLimit = 4 << 10
const gatewayStreamFrameLimit = 256 << 10
const gatewayResponsesRetryPromptTextLimit = 2 << 20
const gatewayCandidateRetryAttempts = 3
const gatewayRouteDetailsLimit = 4096
const gatewayRouteDetailPartLimit = 768
const gatewayDefaultQuotaPerUSD = 500000
const gatewayFallbackCacheInputPriceMultiplier = 0.1
const gatewayModelProbeResponseLimit = 1 << 20
const gatewaySessionHeader = "X-BridgeOps-Session-ID"
const legacyGatewaySessionHeader = "X-Upstream-Ops-Session-ID"
const gatewayDownstreamKeyTestHeader = "X-BridgeOps-Key-Test"
const gatewayDownstreamKeyTestContextKey = "bridgeops.downstream_key_test"
const gatewayManagedKeyRebuildConfirmation = "DELETE_AND_REBUILD_MANAGED_KEYS"
const gatewayManualCooldownIdempotencyHeader = "Idempotency-Key"

// A candidate-group assignment is an interactive control-plane operation.
// Bound the remote fact refresh independently of the HTTP server timeout so a
// provider or an in-progress channel sync cannot leave the UI waiting
// indefinitely. The final storage transaction still fences the observed
// identity/generation and reports a retryable failure when this budget ends.
const gatewayUpstreamGroupAssignmentPreflightTimeout = 10 * time.Second

const (
	gatewayRouteReasonManual          = "manual"
	gatewayRouteReasonSmartPrimary    = "smart_primary"
	gatewayRouteReasonSessionAffinity = "session_affinity"
	gatewayRouteReasonModelFiltered   = "model_filtered"
	gatewayRouteReasonFallback        = "fallback"
	gatewayRouteReasonAllFailed       = "all_failed"
	gatewayRouteReasonClientCanceled  = "client_canceled"
	gatewayRouteReasonKeyTest         = "api_test"
)

const gatewayClientClosedStatus = 499
const gatewayPublicFailureMessage = "request failed; retry later"
const gatewayPublicStreamFailureMessage = "request failed before completion"
const gatewayPublicRetryPromptMessage = "The response was interrupted. Please retry."
const gatewayConfirmedUpstreamInterruptionMessage = "Our servers are currently overloaded. Please try again later."
const gatewayPublicContextLimitMessage = "prompt is too long for the selected model; run /compact or reduce conversation history, system prompt, tools, or requested output tokens"

const gatewayContextPreflightBodyRatioNumerator = 5
const gatewayContextPreflightBodyRatioDenominator = 4
const gatewayContextCapacityMaxAge = 24 * time.Hour
const gatewayContextCountTimeout = 5 * time.Second
const gatewayModelsProxyTimeout = 30 * time.Second
const gatewaySessionAffinityFirstByteTimeout = 90 * time.Second
const gatewayUpstreamRequestIDMaxLength = storage.GatewayProviderRequestIDMaxLength

var gatewayHTTPClient = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

var gatewayInFlightRequests atomic.Int64

var gatewayManualCooldownIdempotencyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

const gatewayProxyClientCacheLimit = 32

var gatewayProxyClients = struct {
	sync.Mutex
	values map[string]*http.Client
}{values: make(map[string]*http.Client)}

var gatewayKeyInFlight = struct {
	sync.RWMutex
	values map[uint]int64
}{values: make(map[uint]int64)}

func addGatewayKeyInFlight(keyID uint, delta int64) {
	if keyID == 0 || delta == 0 {
		return
	}
	gatewayKeyInFlight.Lock()
	next := gatewayKeyInFlight.values[keyID] + delta
	if next <= 0 {
		delete(gatewayKeyInFlight.values, keyID)
	} else {
		gatewayKeyInFlight.values[keyID] = next
	}
	gatewayKeyInFlight.Unlock()
}

func gatewayKeyInFlightCount(keyID uint) int64 {
	gatewayKeyInFlight.RLock()
	value := gatewayKeyInFlight.values[keyID]
	gatewayKeyInFlight.RUnlock()
	return value
}

func registerGateway(g *gin.RouterGroup, d *Deps) {
	gp := g.Group("/gateway")
	gp.GET("/downstream-keys", func(c *gin.Context) { listGatewayDownstreamKeys(c, d) })
	gp.POST("/downstream-keys", func(c *gin.Context) { createGatewayDownstreamKey(c, d) })
	gp.PATCH("/downstream-keys/:id", func(c *gin.Context) { updateGatewayDownstreamKey(c, d) })
	gp.PATCH("/downstream-keys/:id/route-exclusions", func(c *gin.Context) { updateGatewayRouteExclusion(c, d) })
	gp.DELETE("/downstream-keys/:id", func(c *gin.Context) { deleteGatewayDownstreamKey(c, d) })
	gp.POST("/downstream-keys/:id/test-token", func(c *gin.Context) { issueGatewayDownstreamKeyTestToken(c, d) })
	gp.GET("/key-groups", func(c *gin.Context) { listGatewayKeyGroups(c, d) })
	gp.POST("/key-groups", func(c *gin.Context) { createGatewayKeyGroup(c, d) })
	gp.PATCH("/key-groups/:id", func(c *gin.Context) { updateGatewayKeyGroup(c, d) })
	gp.DELETE("/key-groups/:id", func(c *gin.Context) { deleteGatewayKeyGroup(c, d) })
	gp.GET("/upstream-group-changes", func(c *gin.Context) { listGatewayUpstreamGroupChanges(c, d) })
	gp.GET("/upstream-group-changes/review-summary", func(c *gin.Context) { gatewayUpstreamGroupChangeReviewSummary(c, d) })
	gp.POST("/upstream-group-changes/reconcile-reserved", func(c *gin.Context) { reconcileGatewayReservedUpstreamGroupChanges(c, d) })
	gp.GET("/upstream-group-changes/history", func(c *gin.Context) { listGatewayUpstreamGroupChangeHistory(c, d) })
	gp.POST("/upstream-group-changes/resolve-batch", func(c *gin.Context) { resolveGatewayUpstreamGroupChanges(c, d) })
	gp.POST("/upstream-group-changes/:id/resolve", func(c *gin.Context) { resolveGatewayUpstreamGroupChange(c, d) })
	gp.POST("/upstream-group-changes/:id/manual-review/resume", func(c *gin.Context) { resumeGatewayUpstreamGroupChangeManualReview(c, d) })
	gp.GET("/candidate-groups", func(c *gin.Context) { listGatewayCandidateGroups(c, d) })
	gp.POST("/candidate-groups", func(c *gin.Context) { createGatewayCandidateGroup(c, d) })
	gp.PATCH("/candidate-groups/:id", func(c *gin.Context) { updateGatewayCandidateGroup(c, d) })
	gp.DELETE("/candidate-groups/:id", func(c *gin.Context) { deleteGatewayCandidateGroup(c, d) })
	gp.GET("/model-catalog", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"data": storage.ModelFamilyCatalog()}) })
	gp.GET("/automatic-assignment", func(c *gin.Context) { gatewayAutomaticAssignmentConfig(c, d) })
	gp.PATCH("/automatic-assignment/state", func(c *gin.Context) { updateGatewayAutomaticAssignmentState(c, d) })
	gp.POST("/automatic-assignment/rules", func(c *gin.Context) { saveGatewayAutomaticAssignmentRule(c, d, 0) })
	gp.PATCH("/automatic-assignment/rules/:id", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		saveGatewayAutomaticAssignmentRule(c, d, id)
	})
	gp.DELETE("/automatic-assignment/rules/:id", func(c *gin.Context) { deleteGatewayAutomaticAssignmentRule(c, d) })
	gp.POST("/automatic-assignment/preview", func(c *gin.Context) { previewGatewayAutomaticAssignments(c, d, false) })
	gp.POST("/automatic-assignment/preview-draft", func(c *gin.Context) { previewGatewayAutomaticAssignments(c, d, true) })
	gp.POST("/automatic-assignment/execute", func(c *gin.Context) { executeGatewayAutomaticAssignment(c, d) })
	gp.POST("/automatic-assignment/recheck", func(c *gin.Context) { recheckGatewayAutomaticAssignments(c, d) })
	gp.PATCH("/upstream-groups/:id/service-tier", func(c *gin.Context) { updateGatewayUpstreamGroupServiceTier(c, d) })
	gp.GET("/key-group-route-policies", func(c *gin.Context) { listGatewayKeyGroupRoutePolicies(c, d) })
	gp.POST("/key-group-route-policies", func(c *gin.Context) { saveGatewayKeyGroupRoutePolicy(c, d, 0) })
	gp.PATCH("/key-group-route-policies/:id", func(c *gin.Context) {
		id, err := uintParam(c, "id")
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		saveGatewayKeyGroupRoutePolicy(c, d, id)
	})
	gp.DELETE("/key-group-route-policies/:id", func(c *gin.Context) { deleteGatewayKeyGroupRoutePolicy(c, d) })
	gp.GET("/route-preview", func(c *gin.Context) { gatewayRoutePreview(c, d) })
	gp.GET("/manual-mapped-target-preview", func(c *gin.Context) { gatewayManualMappedTargetPreview(c, d) })
	gp.GET("/request-logs", func(c *gin.Context) { listGatewayRequestLogs(c, d) })
	gp.GET("/request-logs/export", func(c *gin.Context) { exportGatewayRequestLogs(c, d) })
	gp.POST("/request-logs/:id/client-ip-geo", func(c *gin.Context) { gatewayRequestLogClientIPGeo(c, d) })
	gp.POST("/request-logs/:id/routing-cooldown", func(c *gin.Context) { gatewayRequestLogRoutingCooldown(c, d) })
	gp.POST("/upstream-groups/:id/routing-enable", func(c *gin.Context) { enableGatewayUpstreamGroupRouting(c, d) })
	gp.GET("/request-log-filters", func(c *gin.Context) { gatewayRequestLogFilters(c, d) })
	gp.GET("/request-log-analytics", func(c *gin.Context) { gatewayRequestLogAnalytics(c, d) })
	gp.GET("/request-log-group-cache-ratios", func(c *gin.Context) { gatewayRequestLogGroupCacheRatios(c, d) })
	registerGatewayModelPriceRoutes(gp, d)
	gp.POST("/request-price-facts/import", func(c *gin.Context) { importGatewayRequestPriceFacts(c, d) })
	gp.GET("/request-price-facts/import-target", func(c *gin.Context) { getGatewayRequestPriceImportTarget(c, d) })
	gp.GET("/probe-config", func(c *gin.Context) { gatewayProbeConfig(c, d) })
	gp.GET("/image-operation-targets", func(c *gin.Context) { listGatewayImageOperationTargets(c, d) })
	gp.GET("/image-operation-capabilities", func(c *gin.Context) { listGatewayImageOperationCapabilities(c, d) })
	gp.PATCH("/image-operation-capabilities", func(c *gin.Context) { updateGatewayImageOperationCapability(c, d) })
	gp.GET("/image-operation-capabilities/:id/audit", func(c *gin.Context) { listGatewayImageOperationCapabilityAudits(c, d) })
	gp.POST("/upstream-keys/:channel_id/ensure", func(c *gin.Context) { ensureGatewayUpstreamKey(c, d) })
	gp.POST("/upstream-keys/:channel_id/rebuild-managed", func(c *gin.Context) { rebuildGatewayManagedUpstreamKeys(c, d) })
	gp.POST("/model-families/sync", func(c *gin.Context) { syncGatewayModelFamilies(c, d) })
	gp.PATCH("/latency-probe-model", func(c *gin.Context) { updateGatewayLatencyProbeModel(c, d) })
	gp.PATCH("/probe-channel", func(c *gin.Context) { updateGatewayProbeChannel(c, d) })
	gp.PATCH("/probe-group", func(c *gin.Context) { updateGatewayProbeGroup(c, d) })
}

func registerGatewayProxy(r *gin.Engine, d *Deps) {
	r.Any("/v1/*path", func(c *gin.Context) { gatewayProxy(c, d) })
}

type createGatewayDownstreamKeyInput struct {
	Name                string `json:"name"`
	App                 string `json:"app"`
	KeyGroupID          uint   `json:"key_group_id"`
	ModelFamily         string `json:"model_family"`
	CandidateGroupID    uint   `json:"candidate_group_id"`
	CandidateTargetMode string `json:"candidate_target_mode"`
}

type updateGatewayDownstreamKeyInput struct {
	Name                        *string `json:"name"`
	Enabled                     *bool   `json:"enabled"`
	App                         *string `json:"app"`
	ModelFamily                 *string `json:"model_family"`
	CandidateGroupID            *uint   `json:"candidate_group_id"`
	CandidateTargetMode         *string `json:"candidate_target_mode"`
	KeyGroupID                  *uint   `json:"key_group_id"`
	ModelMappingStrategy        *string `json:"model_mapping_strategy"`
	ModelMappingModel           *string `json:"model_mapping_model"`
	ModelMappingReasoningEffort *string `json:"model_mapping_reasoning_effort"`
	SmartRouting                *bool   `json:"smart_routing"`
	ManualChannelID             *uint   `json:"manual_channel_id"`
	ManualGroupName             *string `json:"manual_group_name"`
}

type updateGatewayRouteExclusionInput struct {
	ChannelID uint   `json:"channel_id"`
	GroupName string `json:"group_name"`
	Disabled  bool   `json:"disabled"`
}

type gatewayCandidateGroupInput struct {
	Name                      string  `json:"name"`
	ModelMappingFamily        string  `json:"model_mapping_family"`
	ExpectedMembershipVersion *uint64 `json:"expected_membership_version"`
	Members                   []struct {
		ChannelID          uint   `json:"channel_id"`
		GroupName          string `json:"group_name"`
		UpstreamGroupID    uint   `json:"upstream_group_id"`
		PresenceGeneration uint64 `json:"presence_generation"`
	} `json:"members"`
}

type gatewayAutomaticAssignmentStateInput struct {
	RulesetVersion     uint64 `json:"ruleset_version"`
	BackfillComplete   bool   `json:"backfill_complete"`
	AutoExecuteEnabled bool   `json:"auto_execute_enabled"`
}

type gatewayAutomaticAssignmentRuleInput struct {
	RulesetVersion uint64                                `json:"ruleset_version"`
	Rule           storage.GatewayCandidateGroupAutoRule `json:"rule"`
}

type gatewayAutomaticAssignmentPreviewInput struct {
	ChangeIDs []uint                                 `json:"change_ids"`
	Rule      *storage.GatewayCandidateGroupAutoRule `json:"rule,omitempty"`
}

type gatewayAutomaticAssignmentExecuteInput struct {
	ChangeID        uint   `json:"change_id"`
	EvaluationToken string `json:"evaluation_token"`
}

type gatewayUpstreamGroupServiceTierInput struct {
	ServiceTier string `json:"service_tier"`
	Source      string `json:"source"`
}

type gatewayKeyGroupRoutePolicyInput struct {
	ExpectedVersion uint64                             `json:"expected_version"`
	Policy          storage.GatewayKeyGroupRoutePolicy `json:"policy"`
}

type updateGatewayProbeModelInput struct {
	ChannelID uint   `json:"channel_id"`
	GroupName string `json:"group_name"`
	ModelName string `json:"model_name"`
}

type updateGatewayProbeChannelInput struct {
	ChannelID uint `json:"channel_id"`
	Enabled   bool `json:"enabled"`
}

type updateGatewayProbeGroupInput struct {
	ChannelID uint   `json:"channel_id"`
	GroupName string `json:"group_name"`
	Enabled   bool   `json:"enabled"`
}

type gatewayRouteCandidateOutput struct {
	UpstreamGroupID         uint       `json:"upstream_group_id,omitempty"`
	PresenceGeneration      uint64     `json:"presence_generation,omitempty"`
	ChannelIdentityRevision uint64     `json:"channel_identity_revision,omitempty"`
	ChannelID               uint       `json:"channel_id"`
	ChannelName             string     `json:"channel_name"`
	ChannelType             string     `json:"channel_type"`
	GatewayApp              string     `json:"gateway_app"`
	GroupName               string     `json:"group_name"`
	Ratio                   float64    `json:"ratio"`
	CompletionRatio         float64    `json:"completion_ratio"`
	RawRatio                float64    `json:"raw_ratio"`
	RawCompletionRatio      float64    `json:"raw_completion_ratio"`
	FundingFactor           float64    `json:"funding_factor"`
	CostBasisVersion        uint64     `json:"cost_basis_version"`
	CostBasisSource         string     `json:"cost_basis_source,omitempty"`
	EffectivePrice          float64    `json:"effective_price"`
	EffectivePriceBasis     string     `json:"effective_price_basis,omitempty"`
	PriceInputShare         float64    `json:"price_input_share"`
	CacheRatio              float64    `json:"cache_ratio"`
	CacheRatioKnown         bool       `json:"cache_ratio_known"`
	ModelIDs                []string   `json:"model_ids"`
	ModelFamilies           []string   `json:"model_families"`
	ModelMappingFamilies    []string   `json:"model_mapping_families"`
	LatencyProbeModel       string     `json:"latency_probe_model"`
	ModelsSyncedAt          *time.Time `json:"models_synced_at,omitempty"`
	ModelsError             string     `json:"models_error,omitempty"`
	AvgLatencyMs            float64    `json:"avg_latency_ms"`
	Score                   float64    `json:"score"`
	PriceScore              float64    `json:"price_score"`
	LatencyScore            float64    `json:"latency_score"`
	LatencySamples          int64      `json:"latency_samples"`
	ProbeEnabled            bool       `json:"probe_enabled"`
	ProbeDisabled           bool       `json:"probe_disabled"`
	ProbeEstimatedCostUSD   float64    `json:"probe_estimated_cost_usd"`
	DisabledForKey          bool       `json:"disabled_for_key"`
	CoolingDown             bool       `json:"cooling_down"`
	FailureCount            int        `json:"failure_count"`
	CooldownUntil           *time.Time `json:"cooldown_until,omitempty"`
	CooldownRemaining       int64      `json:"cooldown_remaining"`
	RoutingAvailable        *bool      `json:"routing_available,omitempty"`
	AvailabilityReasons     []string   `json:"availability_reasons,omitempty"`
	ProbeEligible           *bool      `json:"probe_eligible,omitempty"`
	ProbeBlockers           []string   `json:"probe_blockers,omitempty"`
	LatencyQualified        *bool      `json:"latency_qualified,omitempty"`
	LatencySource           string     `json:"latency_source,omitempty"`
	UpstreamKeyConfigured   *bool      `json:"upstream_key_configured,omitempty"`
	UpstreamKeyEnabled      *bool      `json:"upstream_key_enabled,omitempty"`
	UpstreamKeyProvisioning *bool      `json:"upstream_key_provisioning,omitempty"`
	UpstreamKeyAvailable    *bool      `json:"upstream_key_available,omitempty"`
}

func gatewayRouteCandidateOutputFromCandidate(candidate gateway.Candidate) gatewayRouteCandidateOutput {
	return gatewayRouteCandidateOutput{
		UpstreamGroupID:         candidate.UpstreamGeneration.ID,
		PresenceGeneration:      candidate.UpstreamGeneration.PresenceGeneration,
		ChannelIdentityRevision: candidate.Channel.UpstreamAccountIdentityRevision,
		ChannelID:               candidate.Channel.ID,
		ChannelName:             candidate.Channel.Name,
		ChannelType:             string(candidate.Channel.Type),
		GatewayApp:              string(candidate.App),
		GroupName:               candidate.Rate.ModelName,
		Ratio:                   candidate.Rate.Ratio,
		CompletionRatio:         candidate.Rate.CompletionRatio,
		RawRatio:                candidate.RawRatio,
		RawCompletionRatio:      candidate.RawCompletionRatio,
		FundingFactor:           candidate.FundingFactor,
		CostBasisVersion:        candidate.CostBasisVersion,
		CostBasisSource:         candidate.CostBasisSource,
		EffectivePrice:          candidate.EffectivePrice,
		EffectivePriceBasis:     candidate.EffectivePriceBasis,
		PriceInputShare:         candidate.PriceInputShare,
		CacheRatio:              candidate.CacheRatio,
		CacheRatioKnown:         candidate.CacheRatioKnown,
		ModelFamilies:           candidate.ModelFamilies,
		ModelMappingFamilies:    gateway.ModelMappingFamilies(candidate.ModelFamilies),
		ModelIDs:                candidate.ModelIDs,
		LatencyProbeModel:       candidate.LatencyProbeModel,
		ModelsSyncedAt:          candidate.ModelsSyncedAt,
		ModelsError:             candidate.ModelsError,
		AvgLatencyMs:            candidate.AvgLatencyMs,
		Score:                   gatewayPriorityScore(candidate.Score),
		PriceScore:              candidate.PricePriorityScore,
		LatencyScore:            candidate.LatencyPriorityScore,
		LatencySamples:          candidate.LatencySamples,
		ProbeEnabled:            candidate.Channel.LatencyProbeEnabled && !candidate.ProbeDisabled,
		ProbeDisabled:           candidate.ProbeDisabled,
		ProbeEstimatedCostUSD:   candidate.ProbeEstimatedCostUSD,
		DisabledForKey:          candidate.DisabledForKey,
		CoolingDown:             candidate.CoolingDown,
		FailureCount:            candidate.FailureCount,
		CooldownUntil:           candidate.CooldownUntil,
		CooldownRemaining:       max(0, int64(candidate.CooldownRemaining.Seconds())),
	}
}

func gatewayRouteCandidateOutputFromMemberPreview(preview gateway.CandidateGroupMemberPreview) gatewayRouteCandidateOutput {
	out := gatewayRouteCandidateOutputFromCandidate(preview.Candidate)
	routingAvailable := preview.RoutingAvailable
	probeEligible := preview.ProbeEligible
	latencyQualified := preview.LatencyQualified
	upstreamKeyConfigured := preview.UpstreamKeyConfigured
	upstreamKeyEnabled := preview.UpstreamKeyEnabled
	upstreamKeyProvisioning := preview.UpstreamKeyProvisioning
	upstreamKeyAvailable := preview.UpstreamKeyAvailable
	out.RoutingAvailable = &routingAvailable
	out.AvailabilityReasons = append([]string(nil), preview.AvailabilityReasons...)
	out.ProbeEligible = &probeEligible
	out.ProbeBlockers = append([]string(nil), preview.ProbeBlockers...)
	out.LatencyQualified = &latencyQualified
	out.LatencySource = preview.LatencySource
	out.UpstreamKeyConfigured = &upstreamKeyConfigured
	out.UpstreamKeyEnabled = &upstreamKeyEnabled
	out.UpstreamKeyProvisioning = &upstreamKeyProvisioning
	out.UpstreamKeyAvailable = &upstreamKeyAvailable
	return out
}

type gatewayResolvedTarget struct {
	SiteURL       string
	ProxyURL      string
	ProxyCacheKey string
}

func listGatewayDownstreamKeys(c *gin.Context, d *Deps) {
	keys, err := d.Gateway.ListDownstreamKeys()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	needsMappingPreview := false
	for i := range keys {
		keys[i].CurrentConcurrency = gatewayKeyInFlightCount(keys[i].ID)
		strategy, normalizeErr := gateway.NormalizeModelMappingStrategy(keys[i].ModelMappingStrategy)
		if normalizeErr == nil && strategy != gateway.ModelMappingStrategyDisabled {
			needsMappingPreview = true
			break
		}
	}
	if needsMappingPreview && d.Gateway != nil && d.Gateway.Store != nil {
		groups, groupsErr := d.Gateway.Store.ListCandidateGroups()
		if groupsErr != nil {
			for i := range keys {
				strategy, normalizeErr := gateway.NormalizeModelMappingStrategy(keys[i].ModelMappingStrategy)
				if normalizeErr == nil && strategy != gateway.ModelMappingStrategyDisabled {
					keys[i].ModelMappingPreview = &storage.GatewayModelMappingPreview{Strategy: string(strategy), Error: groupsErr.Error()}
				}
			}
		} else {
			groupsByID := make(map[uint]*storage.GatewayCandidateGroup, len(groups))
			for i := range groups {
				groupsByID[groups[i].ID] = &groups[i]
			}
			for i := range keys {
				keys[i].ModelMappingPreview = gatewayModelMappingPreview(d, &keys[i], groupsByID[keys[i].CandidateGroupID])
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"data": keys})
}

func createGatewayDownstreamKey(c *gin.Context, d *Deps) {
	var in createGatewayDownstreamKeyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	res, err := d.Gateway.CreateDownstreamKeyWithCandidateTarget(in.Name, storage.GatewayApp(in.App), in.ModelFamily, in.CandidateGroupID, in.KeyGroupID, in.CandidateTargetMode)
	if err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": res})
}

func updateGatewayDownstreamKey(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in updateGatewayDownstreamKeyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var app *storage.GatewayApp
	if in.App != nil {
		value := storage.GatewayApp(*in.App)
		app = &value
	}
	if in.CandidateGroupID != nil && in.CandidateTargetMode == nil {
		fail(c, http.StatusBadRequest, errors.New("candidate_target_mode is required when candidate_group_id is provided"))
		return
	}
	automaticResolver := gateway.AutomaticMappedModelResolver(func(key *storage.GatewayDownstreamKey, group *storage.GatewayCandidateGroup, strategy gateway.ModelMappingStrategy) (string, bool) {
		return gatewayAutomaticMappedModelForManagement(d, key, group, strategy)
	})
	key, err := d.Gateway.UpdateDownstreamKeyWithCandidateTargetMappingAndAutomaticResolver(id, in.Name, in.Enabled, app, in.ModelFamily, in.CandidateGroupID, in.CandidateTargetMode, in.KeyGroupID, in.ModelMappingStrategy, in.ModelMappingModel, in.ModelMappingReasoningEffort, in.SmartRouting, in.ManualChannelID, in.ManualGroupName, automaticResolver)
	if err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": key})
}

type gatewayKeyGroupInput struct {
	Name string `json:"name"`
}

func listGatewayKeyGroups(c *gin.Context, d *Deps) {
	groups, err := d.Gateway.ListKeyGroups()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": groups})
}

func createGatewayKeyGroup(c *gin.Context, d *Deps) {
	var in gatewayKeyGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	group, err := d.Gateway.SaveKeyGroup(0, in.Name)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": group})
}

func updateGatewayKeyGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in gatewayKeyGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	group, err := d.Gateway.SaveKeyGroup(id, in.Name)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": group})
}

func deleteGatewayKeyGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.Gateway.DeleteKeyGroup(id); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, storage.ErrKeyGroupInUse) {
			status = http.StatusConflict
		} else if errors.Is(err, gorm.ErrRecordNotFound) {
			status = http.StatusNotFound
		}
		fail(c, status, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func listGatewayUpstreamGroupChanges(c *gin.Context, d *Deps) {
	changes, err := d.Gateway.ListUpstreamGroupChanges(true)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	if len(changes) == 0 {
		c.JSON(http.StatusOK, gin.H{"data": changes})
		return
	}
	rates, err := d.Gateway.EffectiveGroupRates()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gatewayUpstreamGroupChangeItems(changes, rates)})
}

func gatewayUpstreamGroupChangeReviewSummary(c *gin.Context, d *Deps) {
	count, err := d.Gateway.Store.CountPendingUpstreamGroupChangeManualReviews()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"count": count}})
}

func reconcileGatewayReservedUpstreamGroupChanges(c *gin.Context, d *Deps) {
	if _, err := gatewayAdministratorActor(c, d); err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	reconcileContext, cancel := context.WithTimeout(c.Request.Context(), 2*time.Minute)
	defer cancel()
	resolved, failures, err := d.Gateway.ReconcileReservedManagedUpstreamKeyRemovalReviews(reconcileContext)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"resolved": resolved, "failures": failures}})
}

type gatewayUpstreamGroupChangeItem struct {
	storage.GatewayUpstreamGroupChange
	Ratio           *float64 `json:"ratio,omitempty"`
	CompletionRatio *float64 `json:"completion_ratio,omitempty"`
}

type gatewayUpstreamGroupChangeRateKey struct {
	channelID uint
	groupName string
}

func gatewayUpstreamGroupChangeItems(changes []storage.GatewayUpstreamGroupChange, effectiveRates []gateway.EffectiveGroupRate) []gatewayUpstreamGroupChangeItem {
	rates := make(map[gatewayUpstreamGroupChangeRateKey]gateway.EffectiveGroupRate, len(effectiveRates))
	for _, rate := range effectiveRates {
		rates[gatewayUpstreamGroupChangeRateKey{channelID: rate.ChannelID, groupName: strings.TrimSpace(rate.GroupName)}] = rate
	}

	items := make([]gatewayUpstreamGroupChangeItem, 0, len(changes))
	for _, change := range changes {
		item := gatewayUpstreamGroupChangeItem{GatewayUpstreamGroupChange: change}
		if rate, ok := rates[gatewayUpstreamGroupChangeRateKey{channelID: change.ChannelID, groupName: strings.TrimSpace(change.GroupName)}]; ok {
			ratio := rate.Ratio
			item.Ratio = &ratio
			completionRatio := rate.CompletionRatio
			item.CompletionRatio = &completionRatio
		}
		items = append(items, item)
	}
	return items
}

func listGatewayUpstreamGroupChangeHistory(c *gin.Context, d *Deps) {
	pageSize := 50
	if raw := strings.TrimSpace(c.Query("page_size")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			fail(c, http.StatusBadRequest, fmt.Errorf("page_size invalid"))
			return
		}
		pageSize = parsed
	}
	filter := storage.GatewayUpstreamGroupChangeHistoryFilter{
		View:    c.Query("view"),
		Search:  c.Query("search"),
		Channel: c.Query("channel"),
		Group:   c.Query("group"),
		Source:  c.Query("source"),
	}
	result, err := d.Gateway.Store.ListUpstreamGroupChangesHistoryFilteredCursorPage(filter, c.Query("cursor"), pageSize)
	if err != nil {
		if errors.Is(err, storage.ErrGatewayUpstreamGroupChangeHistoryFilterInvalid) || errors.Is(err, storage.ErrGatewayUpstreamGroupChangeHistoryCursorInvalid) {
			fail(c, http.StatusBadRequest, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}

type resolveGatewayUpstreamGroupChangeInput struct {
	CandidateGroupIDs []uint `json:"candidate_group_ids"`
	Ignore            bool   `json:"ignore"`
}

type resolveGatewayUpstreamGroupChangesInput struct {
	IDs               []uint `json:"ids"`
	CandidateGroupIDs []uint `json:"candidate_group_ids"`
	Ignore            bool   `json:"ignore"`
}

type resumeGatewayUpstreamGroupChangeManualReviewInput struct {
	Resolution string `json:"resolution"`
}

func resolveGatewayUpstreamGroupChanges(c *gin.Context, d *Deps) {
	var in resolveGatewayUpstreamGroupChangesInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	ids := in.IDs
	resolutionContext, cancelResolution := context.WithTimeout(c.Request.Context(), gatewayUpstreamGroupAssignmentPreflightTimeout)
	defer cancelResolution()
	preflightFailures := map[uint]string{}
	preflightRevisions := map[uint]uint64{}
	if !in.Ignore {
		ids, preflightFailures, preflightRevisions = refreshGatewayUpstreamGroupAssignmentChannelsWithRevisions(resolutionContext, d, in.IDs)
	}
	// Ignoring an already-captured change does not write provider-derived
	// membership. It deliberately has no refresh revision to fence, so do not
	// turn the empty preflight map into a stale-identity failure.
	var expectedRevisions map[uint]uint64
	if !in.Ignore {
		expectedRevisions = preflightRevisions
	}
	resolved, failures := d.Gateway.ResolveUpstreamGroupChangesAsWithIdentityRevisionsContext(resolutionContext, ids, in.CandidateGroupIDs, in.Ignore, actor, expectedRevisions)
	for id, reason := range preflightFailures {
		failures[id] = reason
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"resolved": resolved, "failures": failures}})
}

func resolveGatewayUpstreamGroupChange(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in resolveGatewayUpstreamGroupChangeInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	resolutionContext, cancelResolution := context.WithTimeout(c.Request.Context(), gatewayUpstreamGroupAssignmentPreflightTimeout)
	defer cancelResolution()
	if !in.Ignore {
		_, failures, revisions := refreshGatewayUpstreamGroupAssignmentChannelsWithRevisions(resolutionContext, d, []uint{id})
		if reason, failed := failures[id]; failed {
			status := http.StatusConflict
			if reason == gorm.ErrRecordNotFound.Error() {
				status = http.StatusNotFound
			}
			fail(c, status, errors.New(reason))
			return
		}
		change, findErr := d.Gateway.Store.WithContext(resolutionContext).FindUpstreamGroupChange(id)
		if findErr != nil {
			fail(c, http.StatusConflict, findErr)
			return
		}
		expectedRevision := revisions[change.ChannelID]
		if expectedRevision == 0 {
			fail(c, http.StatusConflict, storage.ErrStaleChannelAccountIdentityRevision)
			return
		}
		if err := d.Gateway.ResolveUpstreamGroupChangeAsWithIdentityRevisionContext(resolutionContext, id, in.CandidateGroupIDs, in.Ignore, actor, expectedRevision); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
				status = http.StatusConflict
			}
			fail(c, status, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}
	if err := d.Gateway.ResolveUpstreamGroupChangeAsWithIdentityRevisionContext(resolutionContext, id, in.CandidateGroupIDs, in.Ignore, actor, 0); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, gorm.ErrRecordNotFound) {
			status = http.StatusNotFound
		}
		fail(c, status, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// refreshGatewayUpstreamGroupAssignmentChannels refreshes the authoritative
// upstream group snapshot immediately before a membership write. A batch can
// still make progress for healthy channels when another channel cannot be
// refreshed; every change from the failed channel receives the same reason.
func refreshGatewayUpstreamGroupAssignmentChannels(ctx context.Context, d *Deps, ids []uint) ([]uint, map[uint]string) {
	ready, failures, _ := refreshGatewayUpstreamGroupAssignmentChannelsWithRevisions(ctx, d, ids)
	return ready, failures
}

func refreshGatewayUpstreamGroupAssignmentChannelsWithRevisions(ctx context.Context, d *Deps, ids []uint) ([]uint, map[uint]string, map[uint]uint64) {
	failures := make(map[uint]string)
	revisions := make(map[uint]uint64)
	if len(ids) == 0 {
		return ids, failures, revisions
	}
	if d == nil || d.Monitor == nil {
		for _, id := range ids {
			failures[id] = "上游分组实时刷新服务不可用"
		}
		return nil, failures, revisions
	}
	if d.Gateway == nil || d.Gateway.Store == nil {
		for _, id := range ids {
			failures[id] = "上游分组状态存储不可用"
		}
		return nil, failures, revisions
	}
	channels := d.Channels
	if channels == nil {
		channels = d.Gateway.Channels
	}
	if channels == nil {
		for _, id := range ids {
			failures[id] = "渠道状态存储不可用"
		}
		return nil, failures, revisions
	}
	channels = channels.WithContext(ctx)
	store := d.Gateway.Store.WithContext(ctx)

	type channelChanges struct {
		channel              *storage.Channel
		ids                  []uint
		groupsNeedingKeySync map[string][]uint
		groupsNeedingRemoval map[string][]uint
	}
	byChannel := make(map[uint]*channelChanges)
	channelOrder := make([]uint, 0)
	seenIDs := make(map[uint]struct{}, len(ids))
	for _, id := range ids {
		if _, seen := seenIDs[id]; seen {
			continue
		}
		seenIDs[id] = struct{}{}
		change, err := store.FindUpstreamGroupChange(id)
		if err != nil {
			failures[id] = err.Error()
			continue
		}
		if change.ChannelID == 0 {
			failures[id] = "上游分组变更缺少渠道"
			continue
		}
		group := byChannel[change.ChannelID]
		if group == nil {
			channel, findErr := channels.FindByID(change.ChannelID)
			if findErr != nil {
				failures[id] = findErr.Error()
				continue
			}
			group = &channelChanges{
				channel:              channel,
				groupsNeedingKeySync: make(map[string][]uint),
				groupsNeedingRemoval: make(map[string][]uint),
			}
			byChannel[change.ChannelID] = group
			channelOrder = append(channelOrder, change.ChannelID)
		}
		group.ids = append(group.ids, id)
		groupName := strings.ToLower(strings.TrimSpace(change.GroupName))
		if strings.EqualFold(strings.TrimSpace(change.Kind), "removed") {
			group.groupsNeedingRemoval[groupName] = append(group.groupsNeedingRemoval[groupName], id)
		} else {
			group.groupsNeedingKeySync[groupName] = append(group.groupsNeedingKeySync[groupName], id)
		}
	}

	ready := make([]uint, 0, len(ids))
	for _, channelID := range channelOrder {
		group := byChannel[channelID]
		refreshCtx, cancelRefresh := gatewayUpstreamGroupAssignmentPreflightContext(ctx)
		refreshErr := refreshUpstreamGroupFactsForAssignment(refreshCtx, d.Monitor, group.channel)
		if refreshErr != nil {
			cancelRefresh()
			err := refreshErr
			if errors.Is(err, context.DeadlineExceeded) {
				err = errors.New("上游分组事实刷新超时，请稍后重试")
			}
			reason := fmt.Sprintf("刷新渠道 %s 的当前上游分组状态失败：%v", group.channel.Name, err)
			for _, id := range group.ids {
				failures[id] = reason
			}
			continue
		}
		if err := d.Gateway.RefreshUpstreamGroupFacts(refreshCtx, group.channel.ID); err != nil {
			cancelRefresh()
			reason := fmt.Sprintf("发布渠道 %s 的当前上游分组状态失败：%v", group.channel.Name, err)
			for _, id := range group.ids {
				failures[id] = reason
			}
			continue
		}
		failedChanges := make(map[uint]struct{})
		identityChanged := false
		for groupName, groupIDs := range group.groupsNeedingRemoval {
			if groupName == "" {
				continue
			}
			if removeErr := d.Gateway.DeleteGroupRoutingState(refreshCtx, group.channel.ID, groupName); removeErr != nil {
				if errors.Is(removeErr, storage.ErrStaleChannelAccountIdentityRevision) {
					identityChanged = true
					break
				}
				for _, id := range groupIDs {
					failedChanges[id] = struct{}{}
					failures[id] = fmt.Sprintf("维护渠道 %s 的已移除上游分组 %s 失败：%v", group.channel.Name, groupName, removeErr)
				}
			}
		}
		if identityChanged {
			cancelRefresh()
			reason := fmt.Sprintf("渠道 %s 的上游账户在刷新期间发生变化", group.channel.Name)
			for _, id := range group.ids {
				failures[id] = reason
			}
			continue
		}
		for groupName, groupIDs := range group.groupsNeedingKeySync {
			if groupName == "" {
				continue
			}
			// Removed changes are resolved against their captured generation;
			// only the added IDs in this group need a managed-key check.
			key, keyErr := store.FindUpstreamKeyAny(group.channel.ID, groupName)
			keyReady := keyErr == nil && key.Enabled && !key.Provisioning && strings.TrimSpace(key.APIKeyCipher) != ""
			if keyReady && d.Gateway.Cipher != nil {
				plain, decryptErr := d.Gateway.Cipher.Decrypt(key.APIKeyCipher)
				keyReady = decryptErr == nil && strings.TrimSpace(plain) != ""
			}
			if keyReady {
				continue
			}
			if maintainErr := d.Gateway.MaintainManagedUpstreamKeyForGroup(refreshCtx, group.channel.ID, groupName); maintainErr != nil {
				if errors.Is(maintainErr, storage.ErrStaleChannelAccountIdentityRevision) {
					identityChanged = true
					break
				}
				for _, id := range groupIDs {
					failedChanges[id] = struct{}{}
				}
				reason := fmt.Sprintf("维护渠道 %s 的上游分组 %s 托管密钥失败：%v", group.channel.Name, groupName, maintainErr)
				for _, id := range groupIDs {
					failures[id] = reason
				}
			}
		}
		if identityChanged {
			cancelRefresh()
			reason := fmt.Sprintf("渠道 %s 的上游账户在刷新期间发生变化", group.channel.Name)
			for _, id := range group.ids {
				failures[id] = reason
			}
			continue
		}
		current, err := channels.FindByID(group.channel.ID)
		if err != nil {
			cancelRefresh()
			reason := fmt.Sprintf("读取渠道 %s 的最新账户版本失败：%v", group.channel.Name, err)
			for _, id := range group.ids {
				failures[id] = reason
			}
			continue
		}
		revisions[group.channel.ID] = current.UpstreamAccountIdentityRevision
		for _, id := range group.ids {
			change, findErr := store.FindUpstreamGroupChange(id)
			if findErr != nil || change == nil || strings.TrimSpace(change.GroupName) == "" {
				if findErr != nil {
					failures[id] = findErr.Error()
				}
				continue
			}
			if _, failed := failedChanges[id]; failed {
				continue
			}
			ready = append(ready, id)
		}
		cancelRefresh()
	}
	return ready, failures, revisions
}

func refreshUpstreamGroupFactsForAssignment(ctx context.Context, monitor monitorService, channel *storage.Channel) error {
	if monitor == nil {
		return errors.New("上游分组实时刷新服务不可用")
	}
	if fast, ok := monitor.(assignmentPreflightMonitor); ok {
		return fast.RefreshUpstreamGroupFactsForAssignment(ctx, channel)
	}
	// Keep compatibility with isolated test doubles and older monitor
	// implementations. Production's monitor.Service implements the fast path.
	return monitor.RefreshUpstreamGroupFacts(ctx, channel)
}

func gatewayUpstreamGroupAssignmentPreflightContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, gatewayUpstreamGroupAssignmentPreflightTimeout)
}

func resumeGatewayUpstreamGroupChangeManualReview(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in resumeGatewayUpstreamGroupChangeManualReviewInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	if err := d.Gateway.ResumeManagedUpstreamKeyRemovalManualReview(c.Request.Context(), id, actor, in.Resolution); err != nil {
		status := http.StatusBadRequest
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			status = http.StatusNotFound
		case errors.Is(err, storage.ErrManagedUpstreamKeyRemovalIdentityConflict),
			errors.Is(err, storage.ErrManagedUpstreamKeyRemovalIntentChanged),
			errors.Is(err, storage.ErrUpstreamGroupGenerationChanged),
			errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision):
			status = http.StatusConflict
		}
		fail(c, status, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func listGatewayCandidateGroups(c *gin.Context, d *Deps) {
	groups, err := d.Gateway.ListCandidateGroups()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": groups})
}

func createGatewayCandidateGroup(c *gin.Context, d *Deps) {
	var in gatewayCandidateGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	group, err := d.Gateway.SaveCandidateGroupWithMappingFamilyAs(0, in.Name, in.ModelMappingFamily, candidateGroupMembers(in), actor)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) {
			status = http.StatusConflict
		}
		fail(c, status, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": group})
}

func updateGatewayCandidateGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in gatewayCandidateGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	if in.ExpectedMembershipVersion == nil || *in.ExpectedMembershipVersion == 0 {
		fail(c, http.StatusConflict, storage.ErrCandidateGroupMembershipChanged)
		return
	}
	group, err := d.Gateway.SaveCandidateGroupWithMappingFamilyVersionAs(id, in.Name, in.ModelMappingFamily, candidateGroupMembers(in), *in.ExpectedMembershipVersion, actor)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, gorm.ErrRecordNotFound) {
			status = http.StatusNotFound
		} else if errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) || errors.Is(err, storage.ErrCandidateGroupMembershipChanged) {
			status = http.StatusConflict
		}
		fail(c, status, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": group})
}

func deleteGatewayCandidateGroup(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.Gateway.DeleteCandidateGroup(id); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, storage.ErrCandidateGroupInUse) {
			status = http.StatusConflict
		} else if errors.Is(err, gorm.ErrRecordNotFound) {
			status = http.StatusNotFound
		}
		fail(c, status, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func candidateGroupMembers(in gatewayCandidateGroupInput) []storage.GatewayCandidateGroupMember {
	members := make([]storage.GatewayCandidateGroupMember, 0, len(in.Members))
	for _, member := range in.Members {
		members = append(members, storage.GatewayCandidateGroupMember{
			ChannelID: member.ChannelID, GroupName: strings.TrimSpace(member.GroupName),
			UpstreamGroupID: member.UpstreamGroupID, PresenceGeneration: member.PresenceGeneration,
		})
	}
	return members
}

func gatewayAutomaticAssignmentConfig(c *gin.Context, d *Deps) {
	config, err := d.Gateway.Store.AutomaticAssignmentConfig()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": config})
}

func updateGatewayAutomaticAssignmentState(c *gin.Context, d *Deps) {
	var in gatewayAutomaticAssignmentStateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	state, err := d.Gateway.Store.UpdateAutomaticAssignmentState(in.RulesetVersion, in.BackfillComplete, in.AutoExecuteEnabled)
	if err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": state})
}

func saveGatewayAutomaticAssignmentRule(c *gin.Context, d *Deps, id uint) {
	var in gatewayAutomaticAssignmentRuleInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	in.Rule.ID = id
	state, err := d.Gateway.Store.SaveAutomaticAssignmentRule(&in.Rule, in.RulesetVersion)
	if err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"rule": in.Rule, "state": state}})
}

func deleteGatewayAutomaticAssignmentRule(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	expected, _ := strconv.ParseUint(c.Query("ruleset_version"), 10, 64)
	revoke := c.Query("revoke_applied") == "1"
	state, err := d.Gateway.Store.DeleteAutomaticAssignmentRule(id, expected, revoke)
	if err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": state})
}

func previewGatewayAutomaticAssignments(c *gin.Context, d *Deps, draft bool) {
	var in gatewayAutomaticAssignmentPreviewInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var (
		previews []storage.GatewayAutomaticAssignmentPreview
		err      error
	)
	if draft {
		if in.Rule == nil {
			fail(c, http.StatusBadRequest, errors.New("draft rule is required"))
			return
		}
		previews, err = d.Gateway.Store.PreviewAutomaticAssignmentDraft(*in.Rule, in.ChangeIDs)
	} else {
		previews, err = d.Gateway.Store.PreviewAutomaticAssignments(in.ChangeIDs, true)
	}
	if err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": previews})
}

func executeGatewayAutomaticAssignment(c *gin.Context, d *Deps) {
	var in gatewayAutomaticAssignmentExecuteInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	preview, err := d.Gateway.Store.ExecuteAutomaticAssignment(in.ChangeID, in.EvaluationToken, actor, storage.GatewayAssignmentTriggerManual)
	if err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": preview})
}

func recheckGatewayAutomaticAssignments(c *gin.Context, d *Deps) {
	assigned, failures := d.Gateway.Store.ReevaluatePendingAutomaticAssignments(false)
	if message, ok := failures[0]; ok {
		fail(c, http.StatusInternalServerError, errors.New(message))
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"assigned": assigned, "failures": failures}})
}

func updateGatewayUpstreamGroupServiceTier(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in gatewayUpstreamGroupServiceTierInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if source := strings.ToLower(strings.TrimSpace(in.Source)); source != "" && source != "manual" {
		fail(c, http.StatusBadRequest, errors.New("administrator service tier updates must use source manual"))
		return
	}
	group, err := d.Gateway.Store.SetUpstreamGroupServiceTier(id, in.ServiceTier, in.Source)
	if err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": group})
}

func listGatewayKeyGroupRoutePolicies(c *gin.Context, d *Deps) {
	keyGroupID, _ := strconv.ParseUint(c.Query("key_group_id"), 10, 64)
	policies, err := d.Gateway.Store.ListKeyGroupRoutePolicies(uint(keyGroupID))
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": policies})
}

func saveGatewayKeyGroupRoutePolicy(c *gin.Context, d *Deps, id uint) {
	var in gatewayKeyGroupRoutePolicyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	in.Policy.ID = id
	if err := d.Gateway.Store.SaveKeyGroupRoutePolicy(&in.Policy, in.ExpectedVersion); err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": in.Policy})
}

func deleteGatewayKeyGroupRoutePolicy(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	expected, _ := strconv.ParseUint(c.Query("expected_version"), 10, 64)
	if err := d.Gateway.Store.DeleteKeyGroupRoutePolicy(id, expected); err != nil {
		gatewayAutomaticAssignmentFail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func gatewayAutomaticAssignmentFail(c *gin.Context, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, storage.ErrGatewayRulesetChanged) || errors.Is(err, storage.ErrGatewayEvaluationStale) {
		status = http.StatusConflict
	} else if errors.Is(err, gorm.ErrRecordNotFound) {
		status = http.StatusNotFound
	}
	fail(c, status, err)
}

func updateGatewayRouteExclusion(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in updateGatewayRouteExclusionInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.Gateway.SetRouteExclusion(id, in.ChannelID, strings.TrimSpace(in.GroupName), in.Disabled); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func deleteGatewayDownstreamKey(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := d.Gateway.DeleteDownstreamKey(id); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func issueGatewayDownstreamKeyTestToken(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	token, err := d.Gateway.IssueDownstreamKeyTestToken(id)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"token": token}})
}

func gatewayRoutePreview(c *gin.Context, d *Deps) {
	downstreamKeyID := uint(0)
	if raw := strings.TrimSpace(c.Query("downstream_key_id")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			fail(c, http.StatusBadRequest, fmt.Errorf("downstream_key_id invalid"))
			return
		}
		downstreamKeyID = uint(parsed)
	}
	model := strings.TrimSpace(c.Query("model"))
	if c.Query("manual_selection") == "1" {
		if downstreamKeyID == 0 {
			fail(c, http.StatusBadRequest, fmt.Errorf("downstream_key_id is required for manual route preview"))
			return
		}
		members, err := d.Gateway.ManualRouteMemberPreviewForDownstreamKey(downstreamKeyID)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		out := make([]gatewayRouteCandidateOutput, 0, len(members))
		for _, member := range members {
			out = append(out, gatewayRouteCandidateOutputFromMemberPreview(member))
		}
		c.JSON(http.StatusOK, gin.H{"data": out})
		return
	}
	includeCandidateGroupMembers := c.Query("candidate_group_members") == "1"
	if includeCandidateGroupMembers {
		if downstreamKeyID == 0 {
			fail(c, http.StatusBadRequest, fmt.Errorf("downstream_key_id is required for candidate group preview"))
			return
		}
		if model == "" {
			fail(c, http.StatusBadRequest, fmt.Errorf("model is required for candidate group preview"))
			return
		}
		mappedModel := false
		switch strings.TrimSpace(c.Query("model_context")) {
		case "", "requested":
		case "mapped":
			mappedModel = true
		default:
			fail(c, http.StatusBadRequest, fmt.Errorf("model_context must be requested or mapped"))
			return
		}
		members, err := d.Gateway.CandidateGroupMemberPreviewForDownstreamKey(downstreamKeyID, model, mappedModel)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		out := make([]gatewayRouteCandidateOutput, 0, len(members))
		for _, member := range members {
			out = append(out, gatewayRouteCandidateOutputFromMemberPreview(member))
		}
		c.JSON(http.StatusOK, gin.H{"data": out})
		return
	}
	candidates, _, err := d.Gateway.CandidatesForDownstreamKeyWithExclusions(downstreamKeyID, storage.GatewayApp(c.Query("app")), model, true)
	if err != nil {
		fail(c, http.StatusBadGateway, err)
		return
	}
	out := make([]gatewayRouteCandidateOutput, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, gatewayRouteCandidateOutputFromCandidate(candidate))
	}
	c.JSON(http.StatusOK, gin.H{"data": out})
}

func gatewayPriorityScore(routingScore float64) float64 {
	if routingScore <= 0 {
		return 100
	}
	if routingScore >= 1 {
		return 0
	}
	return (1 - routingScore) * 100
}

func listGatewayRequestLogs(c *gin.Context, d *Deps) {
	page, pageSize, err := parsePageQuery(c)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	filter, err := parseGatewayRequestLogFilter(c, d)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	summary, err := cachedGatewayRequestLogSummary(c.Request.Context(), d, filter, c.Query("refresh") == "1")
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	list, err := d.Gateway.Store.ListRequestLogPageItemsFiltered(page, pageSize, filter)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	total := summary.TotalRequests
	if summary != nil {
		summary.CurrentConcurrency = gatewayInFlightRequests.Load()
		summary.CurrentConcurrencyScope = "instance"
	}
	pages := 1
	if total > 0 {
		pages = int((total + int64(pageSize) - 1) / int64(pageSize))
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"items":     gatewayRequestLogOutputs(list),
		"total":     total,
		"page":      page,
		"page_size": pageSize,
		"pages":     pages,
		"summary":   summary,
	}})
}

// gatewayRequestLogClientIPGeo resolves a stored request-log IP with the
// server's local database. The browser supplies only the log ID: it never
// places the client IP in a URL, body, or third-party geolocation request.
func gatewayRequestLogClientIPGeo(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil || d.GeoIP == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("local IP geolocation is unavailable"))
		return
	}
	id, err := strconv.ParseUint(strings.TrimSpace(c.Param("id")), 10, 64)
	if err != nil || id == 0 || id > uint64(^uint(0)) {
		fail(c, http.StatusBadRequest, errors.New("request log id is invalid"))
		return
	}
	requestLog, err := d.Gateway.Store.FindRequestLogClientIP(uint(id))
	if errors.Is(err, gorm.ErrRecordNotFound) {
		fail(c, http.StatusNotFound, errors.New("request log was not found"))
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, errors.New("request log lookup failed"))
		return
	}

	result, err := d.GeoIP.Lookup(requestLog.ClientIP)
	switch {
	case err == nil:
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"ip":                 result.IP,
			"provider":           result.Provider,
			"country":            result.Country,
			"country_code":       result.CountryCode,
			"region":             result.Region,
			"city":               result.City,
			"postal":             result.Postal,
			"latitude":           result.Latitude,
			"longitude":          result.Longitude,
			"accuracy_radius_km": result.AccuracyRadiusKM,
			"asn":                result.ASN,
			"organization":       result.Organization,
			"timezone":           result.Timezone,
			"client_ip_source":   requestLog.ClientIPSource,
		}})
	case errors.Is(err, geoip.ErrUnavailable):
		fail(c, http.StatusServiceUnavailable, errors.New("local IP geolocation is unavailable"))
	case errors.Is(err, geoip.ErrInvalidIP):
		fail(c, http.StatusUnprocessableEntity, errors.New("recorded IP is not a public unicast address"))
	case errors.Is(err, geoip.ErrNotFound):
		fail(c, http.StatusNotFound, errors.New("recorded IP is not present in local geolocation data"))
	default:
		fail(c, http.StatusServiceUnavailable, errors.New("local IP geolocation is unavailable"))
	}
}

func gatewayRequestLogRoutingCooldown(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("gateway routing cooldown is unavailable"))
		return
	}
	id, err := uintParam(c, "id")
	if err != nil || id == 0 {
		fail(c, http.StatusBadRequest, errors.New("request log id is invalid"))
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader(gatewayManualCooldownIdempotencyHeader))
	if !gatewayManualCooldownIdempotencyPattern.MatchString(idempotencyKey) {
		fail(c, http.StatusBadRequest, errors.New("a valid Idempotency-Key header is required"))
		return
	}
	result, err := d.Gateway.ApplyRequestLogManualRoutingCooldown(id, actor, idempotencyKey)
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		fail(c, http.StatusNotFound, errors.New("request log was not found"))
		return
	case errors.Is(err, storage.ErrGatewayRequestLogRouteUnavailable):
		fail(c, http.StatusUnprocessableEntity, errors.New("request log has no routed upstream group"))
		return
	case errors.Is(err, storage.ErrGatewayRequestLogManualCooldownIneligible):
		fail(c, http.StatusUnprocessableEntity, errors.New("request log is not eligible for manual cooldown"))
		return
	case errors.Is(err, storage.ErrGatewayRequestLogUpstreamGroupUnavailable):
		fail(c, http.StatusConflict, errors.New("request log upstream group is no longer available"))
		return
	case errors.Is(err, storage.ErrGatewayManualCooldownIdempotencyConflict):
		fail(c, http.StatusConflict, errors.New("Idempotency-Key was already used by another administrator"))
		return
	case err != nil:
		if d.Log != nil {
			d.Log.Warn("apply administrator routing cooldown failed", "request_log_id", id, "err", err)
		}
		fail(c, http.StatusInternalServerError, errors.New("failed to apply routing cooldown"))
		return
	}
	if d.Log != nil {
		d.Log.Info("administrator applied request-log routing cooldown",
			"actor", actor,
			"request_log_id", result.RequestLogID,
			"channel_id", result.ChannelID,
			"group", result.GroupName,
			"cooldown_until", result.CooldownUntil,
			"idempotent_replay", result.IdempotentReplay,
		)
	}
	durationMinutes := int(time.Duration(result.DurationSeconds) * time.Second / time.Minute)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"request_log_id":    result.RequestLogID,
		"channel_id":        result.ChannelID,
		"channel_name":      result.ChannelName,
		"group_name":        result.GroupName,
		"duration_minutes":  durationMinutes,
		"cooldown_until":    result.CooldownUntil,
		"idempotent_replay": result.IdempotentReplay,
	}})
}

func gatewayAdministratorActor(c *gin.Context, d *Deps) (string, error) {
	if raw, ok := c.Get("authSubject"); ok {
		if actor := strings.TrimSpace(fmt.Sprint(raw)); actor != "" {
			return actor, nil
		}
	}
	if d != nil && d.Runtime != nil && d.Runtime.CurrentAuth() != nil {
		return "", errors.New("administrator authentication is required")
	}
	return "administrator", nil
}

func exportGatewayRequestLogs(c *gin.Context, d *Deps) {
	filter, err := parseGatewayRequestLogFilter(c, d)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	limit := 10000
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			fail(c, http.StatusBadRequest, fmt.Errorf("limit invalid"))
			return
		}
		if parsed < uint64(limit) {
			limit = int(parsed)
		}
	}
	list, err := d.Gateway.Store.ListRequestLogsFiltered(filter, limit)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	location, err := time.LoadLocation(filter.TimeZone)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	var buf bytes.Buffer
	buf.WriteString("\ufeff")
	writer := csv.NewWriter(&buf)
	_ = writer.Write([]string{
		fmt.Sprintf("时间 (%s)", filter.TimeZone),
		"下游 Key",
		"上游渠道",
		"分组",
		"模型",
		"上游计费模型",
		"思考程度",
		"请求模型",
		"请求思考程度",
		"映射策略",
		"映射详情",
		"方法",
		"路径",
		"状态",
		"首字时间(ms)",
		"总耗时(ms)",
		"输入 Token",
		"输出 Token",
		"缓存写入 Token",
		"缓存读取 Token",
		"总 Token",
		"费用",
		"费用类型",
		"费用来源",
		"上游 Key ID",
		"上游 Key 名称",
		"上游请求 ID",
		"输入倍率",
		"输出倍率",
		"路由原因",
		"路由详情",
		"错误",
	})
	for _, item := range list {
		_ = writer.Write([]string{
			item.CreatedAt.In(location).Format(time.RFC3339),
			safeGatewayCSVText(gatewayExportKeyLabel(item)),
			safeGatewayCSVText(item.ChannelName),
			safeGatewayCSVText(item.GroupName),
			safeGatewayCSVText(item.ModelName),
			safeGatewayCSVText(item.UpstreamModelName),
			safeGatewayCSVText(item.ReasoningEffort),
			safeGatewayCSVText(item.RequestedModelName),
			safeGatewayCSVText(item.RequestedReasoningEffort),
			safeGatewayCSVText(item.ModelMappingStrategy),
			safeGatewayCSVText(item.ModelMappingDetails),
			safeGatewayCSVText(item.Method),
			safeGatewayCSVText(item.Path),
			strconv.Itoa(item.StatusCode),
			strconv.FormatInt(item.FirstByteMs, 10),
			strconv.FormatInt(item.LatencyMs, 10),
			strconv.FormatInt(item.PromptTokens, 10),
			strconv.FormatInt(item.CompletionTokens, 10),
			strconv.FormatInt(item.CacheCreationTokens, 10),
			strconv.FormatInt(item.CacheReadTokens, 10),
			strconv.FormatInt(item.TotalTokens, 10),
			gatewayExportCost(item),
			gatewayExportCostType(item),
			safeGatewayCSVText(gatewayExportCostSource(item)),
			gatewayExportUpstreamKeyID(item.UpstreamKeyID),
			safeGatewayCSVText(item.UpstreamKeyName),
			safeGatewayCSVText(item.UpstreamRequestID),
			strconv.FormatFloat(item.GroupRatio, 'f', 6, 64),
			strconv.FormatFloat(item.CompletionRatio, 'f', 6, 64),
			safeGatewayCSVText(item.RouteReason),
			safeGatewayCSVText(item.RouteDetails),
			safeGatewayCSVText(item.Error),
		})
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	filename := "gateway-request-logs-" + time.Now().In(location).Format("20060102-150405") + ".csv"
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	c.Data(http.StatusOK, "text/csv; charset=utf-8", buf.Bytes())
}

func gatewayExportKeyLabel(item storage.GatewayRequestLogItem) string {
	if strings.TrimSpace(item.DownstreamKeyName) != "" {
		return item.DownstreamKeyName
	}
	if item.DownstreamKeyID == 0 {
		return ""
	}
	return strconv.FormatUint(uint64(item.DownstreamKeyID), 10)
}

func gatewayExportUpstreamKeyID(id int64) string {
	if id <= 0 {
		return ""
	}
	return strconv.FormatInt(id, 10)
}

func gatewayExportCostType(item storage.GatewayRequestLogItem) string {
	if strings.TrimSpace(item.CostReconcileError) != "" && item.CostReconcileAt == nil {
		return "reconcile_failed"
	}
	if item.UpstreamCost != nil {
		return string(item.UpstreamCostStatus)
	}
	if item.CalculatedCost != nil {
		return string(storage.GatewayCostStatusEstimated)
	}
	if item.PriceFactID != nil {
		return string(storage.GatewayCostStatusUnavailable)
	}
	switch item.CostStatus {
	case storage.GatewayCostStatusEstimated:
		return "estimated"
	case storage.GatewayCostStatusReported:
		return "reported"
	case storage.GatewayCostStatusReconciled:
		return "reconciled"
	default:
		return "none"
	}
}

func gatewayExportCost(item storage.GatewayRequestLogItem) string {
	if item.UpstreamCost != nil {
		return strconv.FormatFloat(*item.UpstreamCost, 'f', 8, 64)
	}
	if item.CalculatedCost != nil {
		return strconv.FormatFloat(*item.CalculatedCost, 'f', 8, 64)
	}
	if item.PriceFactID != nil || item.CostStatus == storage.GatewayCostStatusUnavailable || item.CostStatus == storage.GatewayCostStatusNone {
		return ""
	}
	return strconv.FormatFloat(item.Cost, 'f', 8, 64)
}

func gatewayExportCostSource(item storage.GatewayRequestLogItem) string {
	if item.UpstreamCost != nil {
		return item.CostSource
	}
	if item.CalculatedCost != nil || item.PriceFactID != nil {
		return "price_fact"
	}
	return item.CostSource
}

func safeGatewayCSVText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	switch value[0] {
	case '=', '+', '-', '@', '\t', '\r', '\n':
		return "'" + value
	default:
		return value
	}
}

func gatewayRequestLogFilters(c *gin.Context, d *Deps) {
	filter, err := parseGatewayRequestLogFilter(c, d)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	filter.ModelName = ""
	models, err := d.Gateway.Store.ListRequestLogModels(filter)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	groupsFilter := filter
	groupsFilter.GroupName = ""
	groupsFilter.GroupUnassigned = false
	groups, err := d.Gateway.Store.ListRequestLogGroups(groupsFilter)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"models": models,
		"groups": groups,
	}})
}

func gatewayRequestLogAnalytics(c *gin.Context, d *Deps) {
	filter, err := parseGatewayRequestLogFilter(c, d)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	analytics, err := cachedGatewayRequestLogAnalytics(c.Request.Context(), d, filter, 8, c.Query("refresh") == "1")
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": analytics})
}

func parseGatewayRequestLogFilter(c *gin.Context, d *Deps) (storage.GatewayRequestLogFilter, error) {
	filter := storage.GatewayRequestLogFilter{TimeZone: configuredUserTime(d).Name()}
	location, err := time.LoadLocation(filter.TimeZone)
	if err != nil {
		return filter, fmt.Errorf("time_zone invalid")
	}
	if raw := strings.TrimSpace(c.Query("start")); raw != "" {
		value, err := parseGatewayRequestLogTime(raw, location)
		if err != nil {
			return filter, fmt.Errorf("start invalid")
		}
		filter.Start = &value
	}
	if raw := strings.TrimSpace(c.Query("end")); raw != "" {
		value, err := parseGatewayRequestLogTime(raw, location)
		if err != nil {
			return filter, fmt.Errorf("end invalid")
		}
		filter.End = &value
	}
	if filter.Start != nil && filter.End != nil && !filter.End.After(*filter.Start) {
		return filter, fmt.Errorf("end must be after start")
	}
	if raw := strings.TrimSpace(c.Query("downstream_key_id")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return filter, fmt.Errorf("downstream_key_id invalid")
		}
		filter.DownstreamKeyID = uint(parsed)
	}
	filter.ModelName = strings.TrimSpace(c.Query("model"))
	// Provider request IDs are opaque billing identities. Preserve their bytes
	// exactly: whitespace is valid evidence and must not collapse filters.
	filter.UpstreamRequestID = c.Query("upstream_request_id")
	if !utf8.ValidString(filter.UpstreamRequestID) || len(filter.UpstreamRequestID) > gatewayUpstreamRequestIDMaxLength {
		return filter, fmt.Errorf("upstream_request_id invalid")
	}
	filter.UpstreamKeyName = strings.TrimSpace(c.Query("upstream_key_name"))
	if len(filter.UpstreamKeyName) > 128 {
		return filter, fmt.Errorf("upstream_key_name invalid")
	}
	if raw := strings.TrimSpace(c.Query("channel_id")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return filter, fmt.Errorf("channel_id invalid")
		}
		filter.ChannelID = uint(parsed)
	}
	filter.GroupName = strings.TrimSpace(c.Query("group_name"))
	if len(filter.GroupName) > 128 {
		return filter, fmt.Errorf("group_name invalid")
	}
	if raw, exists := c.GetQuery("group_unassigned"); exists {
		switch strings.TrimSpace(raw) {
		case "", "0":
		case "1":
			filter.GroupUnassigned = true
		default:
			return filter, fmt.Errorf("group_unassigned invalid")
		}
	}
	if filter.GroupUnassigned && filter.GroupName != "" {
		return filter, fmt.Errorf("group_name and group_unassigned cannot be combined")
	}
	filter.RouteReason = strings.TrimSpace(c.Query("route_reason"))
	if raw := strings.ToLower(strings.TrimSpace(c.Query("result"))); raw != "" {
		switch raw {
		case storage.GatewayRequestResultSuccess, storage.GatewayRequestResultFailed, storage.GatewayRequestResultCanceled:
			filter.Result = raw
		default:
			return filter, fmt.Errorf("result invalid")
		}
	}
	if raw := strings.ToLower(strings.TrimSpace(c.Query("sort_by"))); raw != "" {
		switch raw {
		case storage.GatewayRequestLogSortCreatedAt,
			storage.GatewayRequestLogSortModelName,
			storage.GatewayRequestLogSortLatencyMs,
			storage.GatewayRequestLogSortTotalTokens,
			storage.GatewayRequestLogSortCost,
			storage.GatewayRequestLogSortStatusCode:
			filter.SortBy = raw
		default:
			return filter, fmt.Errorf("sort_by invalid")
		}
	}
	if raw := strings.ToLower(strings.TrimSpace(c.Query("sort_order"))); raw != "" {
		switch raw {
		case storage.GatewayRequestLogSortAscending, storage.GatewayRequestLogSortDescending:
			filter.SortOrder = raw
		default:
			return filter, fmt.Errorf("sort_order invalid")
		}
	}
	return filter, nil
}

func parseGatewayRequestLogTime(raw string, location *time.Location) (time.Time, error) {
	if value, err := time.Parse(time.RFC3339, raw); err == nil {
		// An RFC3339 offset identifies the same absolute instant regardless of
		// its spelling. Normalize it to UTC so the configured app timezone
		// remains the only display/business-day timezone.
		return value.UTC(), nil
	}
	if location == nil {
		location = time.UTC
	}
	if value, err := time.ParseInLocation("2006-01-02", raw, location); err == nil {
		return value, nil
	}
	return time.Time{}, fmt.Errorf("invalid time")
}

func ensureGatewayUpstreamKey(c *gin.Context, d *Deps) {
	raw := strings.TrimSpace(c.Param("channel_id"))
	id64, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id64 == 0 {
		fail(c, http.StatusBadRequest, fmt.Errorf("channel_id invalid"))
		return
	}
	key, err := d.Gateway.EnsureUpstreamAPIKey(c.Request.Context(), uint(id64))
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"key_prefix": gatewayKeyPrefix(key)}})
}

func rebuildGatewayManagedUpstreamKeys(c *gin.Context, d *Deps) {
	raw := strings.TrimSpace(c.Param("channel_id"))
	id64, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id64 == 0 || id64 > uint64(^uint(0)) {
		fail(c, http.StatusBadRequest, fmt.Errorf("channel_id invalid"))
		return
	}
	var in struct {
		Confirm string `json:"confirm"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || strings.TrimSpace(in.Confirm) != gatewayManagedKeyRebuildConfirmation {
		fail(c, http.StatusBadRequest, errors.New("managed key rebuild confirmation is required"))
		return
	}
	result, err := d.Gateway.RebuildManagedUpstreamKeys(c.Request.Context(), uint(id64))
	if err != nil {
		if result != nil {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "data": result})
			return
		}
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}

func syncGatewayModelFamilies(c *gin.Context, d *Deps) {
	channelID := uint(0)
	if raw := strings.TrimSpace(c.Query("channel_id")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			fail(c, http.StatusBadRequest, fmt.Errorf("channel_id invalid"))
			return
		}
		channelID = uint(parsed)
	}
	items, err := d.Gateway.SyncModelFamilies(c.Request.Context(), channelID)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": items, "total": len(items)}})
}

func updateGatewayLatencyProbeModel(c *gin.Context, d *Deps) {
	var in updateGatewayProbeModelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	groupName := strings.TrimSpace(in.GroupName)
	modelName := strings.TrimSpace(in.ModelName)
	if in.ChannelID == 0 || groupName == "" {
		fail(c, http.StatusBadRequest, fmt.Errorf("channel_id and group_name are required"))
		return
	}
	key, err := d.Gateway.Store.FindUpstreamKeyAny(in.ChannelID, groupName)
	if err != nil {
		fail(c, http.StatusBadRequest, fmt.Errorf("upstream group does not exist"))
		return
	}
	if modelName != "" {
		if !csvContains(key.ModelIDs, modelName) {
			fail(c, http.StatusBadRequest, fmt.Errorf("probe model is not in synced upstream model list"))
			return
		}
	}
	if err := d.Gateway.Store.UpdateLatencyProbeModel(in.ChannelID, groupName, modelName); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func gatewayProbeConfig(c *gin.Context, d *Deps) {
	list, err := d.Gateway.ProbeConfig()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func updateGatewayProbeChannel(c *gin.Context, d *Deps) {
	var in updateGatewayProbeChannelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.ChannelID == 0 || d == nil || d.Channels == nil {
		fail(c, http.StatusBadRequest, fmt.Errorf("channel_id is required"))
		return
	}
	if _, err := d.Channels.FindByID(in.ChannelID); err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	if err := d.Channels.SetLatencyProbeEnabled(in.ChannelID, in.Enabled); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "channel_id": in.ChannelID, "enabled": in.Enabled})
}

func updateGatewayProbeGroup(c *gin.Context, d *Deps) {
	var in updateGatewayProbeGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	groupName := strings.TrimSpace(in.GroupName)
	if in.ChannelID == 0 || groupName == "" || d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusBadRequest, fmt.Errorf("channel_id and group_name are required"))
		return
	}
	if err := d.Gateway.Store.UpdateLatencyProbeEnabled(in.ChannelID, groupName, in.Enabled); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			fail(c, http.StatusNotFound, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "channel_id": in.ChannelID, "group_name": groupName, "enabled": in.Enabled})
}

func gatewayProxy(c *gin.Context, d *Deps) {
	downstream, err := gatewayProxyAuthenticate(d, gatewayAuthHeader(c))
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	if d.Gateway.ConsumeDownstreamKeyTestToken(c.GetHeader(gatewayDownstreamKeyTestHeader), downstream.ID) {
		c.Set(gatewayDownstreamKeyTestContextKey, true)
	}

	if c.Request.Method == http.MethodGet && c.Request.URL.Path == "/v1/models" {
		if downstream.App == storage.GatewayAppClaude {
			c.JSON(http.StatusForbidden, gin.H{"error": fmt.Sprintf("%s key cannot access %s", downstream.App, c.Request.URL.Path)})
			return
		}
		gatewayProxyModels(c, d, downstream)
		return
	}
	if c.Request.Method == http.MethodGet && c.Request.URL.Path == "/v1/usage" {
		gatewayUsage(c, d, downstream)
		return
	}
	if !supportedGatewayPath(c.Request.URL.Path) {
		c.JSON(http.StatusNotFound, gin.H{"error": "unsupported gateway path"})
		return
	}
	if !gatewayAppAllowsPath(downstream.App, c.Request.URL.Path) {
		c.JSON(http.StatusForbidden, gin.H{"error": fmt.Sprintf("%s key cannot access %s", downstream.App, c.Request.URL.Path)})
		return
	}
	// Generate one logical trace after downstream authentication and keep the
	// response writer wrapped for the entire data-plane request. The trace is
	// shared by candidate resolution, every physical dispatch and its final log.
	telemetry := gatewayRequestTelemetryFromContext(c, time.Now().UTC(), downstream)

	mappingStrategy, mappingErr := gateway.NormalizeModelMappingStrategy(downstream.ModelMappingStrategy)
	if mappingErr != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "downstream key has an invalid model mapping strategy"})
		return
	}
	bodyLimit := gatewayRequestBodyLimitForPath(c.Request.URL.Path)
	admissionProfile := gatewayRequestAdmissionStandardProfile
	if mappingStrategy != gateway.ModelMappingStrategyDisabled && gatewayModelMappingAllowedPath(c.Request.URL.Path) {
		admissionProfile = gatewayRequestAdmissionRewriteProfile
	}
	lease, err := gatewayRequestAdmissionForDeps(d).Acquire(
		c.Request.Context(), downstream.ID, c.Request.ContentLength, bodyLimit, admissionProfile,
	)
	if err != nil {
		gatewayWriteRequestAdmissionFailure(c, err)
		return
	}
	defer lease.Release()

	body, err := readGatewayBody(c.Writer, c.Request, bodyLimit, lease, admissionProfile)
	if err != nil {
		if gatewayRequestBodyReadExceededLimit(err) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body exceeds configured limit"})
		} else if errors.Is(err, errGatewayRequestBodyTooLarge) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body exceeds configured limit"})
		} else if errors.Is(err, errGatewayRequestAdmissionBusy) {
			c.Header("Retry-After", "1")
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": gatewayPublicFailureMessage})
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		return
	}
	model := gatewayRequestModel(body)
	reasoningEffort := gatewayRequestReasoningEffort(body)
	if model == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must include model"})
		return
	}
	if mappingStrategy == gateway.ModelMappingStrategyDisabled || !gatewayModelMappingAllowedPath(c.Request.URL.Path) {
		appAllowed := gatewayAppAllowsModel(downstream.App, model)
		directModelConfigured := directGatewayModelConfigured(d, downstream, model)
		if !appAllowed && !directModelConfigured {
			c.JSON(http.StatusForbidden, gin.H{"error": fmt.Sprintf("%s key cannot access model %s", downstream.App, model)})
			return
		}
		if !gatewayModelFamilyAllowsModel(downstream.ModelFamily, downstream.App, model) && !directModelConfigured {
			c.JSON(http.StatusForbidden, gin.H{"error": fmt.Sprintf("model family %s key cannot access model %s", downstream.ModelFamily, model)})
			return
		}
	}

	gatewayInFlightRequests.Add(1)
	defer gatewayInFlightRequests.Add(-1)
	addGatewayKeyInFlight(downstream.ID, 1)
	defer addGatewayKeyInFlight(downstream.ID, -1)

	start := time.Now()
	sessionHash := gatewayRequestSessionHash(c.Request, body)
	decision, mapping, err := gatewayCandidatesWithModelMapping(d, downstream, model, reasoningEffort, body, c.Request.URL.Path, sessionHash)
	body = nil
	if decision == nil && err != nil {
		decision = &gatewayRouteDecision{SessionHash: sessionHash, SmartRouting: downstream.SmartRouting}
	}
	telemetry.captureRouteExclusions(decision, time.Now().UTC())
	if err != nil {
		errText := sanitizeGatewayProbeError([]byte(err.Error()), "")
		resolutionAttempts := []gatewayRouteAttempt{{Reason: "candidate_resolution"}}
		details := gatewayRouteDetails(decision, resolutionAttempts, gateway.Candidate{})
		routeReason := gatewayRequestRouteReason(c, gatewayRouteReasonAllFailed)
		attemptedChannels := gatewayRouteAttemptedChannels(decision, resolutionAttempts, gateway.Candidate{})
		if !decision.SmartRouting {
			// A fixed target failure is not a candidate fallback. Keep the
			// sanitized resolution error for administrators, but do not emit
			// a synthetic candidate-resolution attempt or an all-failed label.
			routeReason = gatewayRequestRouteReason(c, gatewayRouteReasonManual)
			details = gatewayRouteDetailsWithErrors(decision, nil, gateway.Candidate{}, []string{errText})
			attemptedChannels = ""
		}
		requestLog := &storage.GatewayRequestLog{
			DownstreamKeyID:                downstream.ID,
			ClientIP:                       gatewayClientIP(c),
			ClientIPSource:                 gatewayClientIPSource(c),
			ModelName:                      mapping.Model,
			ReasoningEffort:                mapping.ReasoningEffort,
			Method:                         c.Request.Method,
			Path:                           c.Request.URL.Path,
			StatusCode:                     http.StatusBadGateway,
			LatencyMs:                      time.Since(start).Milliseconds(),
			SessionHash:                    sessionHash,
			RouteReason:                    routeReason,
			SessionAffinityOriginRequestID: gatewaySessionAffinityOriginRequestID(decision),
			RouteDetails:                   details,
			RouteAttemptedChannels:         attemptedChannels,
			Error:                          errText,
		}
		gatewayApplySessionAffinityAudit(requestLog, decision)
		mapping.applyToLog(requestLog)
		telemetry.applyToLog(requestLog, "candidate_resolution")
		d.Gateway.LogRequest(requestLog)
		c.JSON(http.StatusBadGateway, gin.H{"error": gatewayPublicFailureMessage})
		persistGatewayDownstreamFirstVisible(d, requestLog, telemetry)
		return
	}

	if gatewayForward(c, d, downstream, decision, mapping.Model, mapping.ReasoningEffort, mapping.Body, mapping, start) {
		return
	}
}

func gatewayProxyAuthenticate(d *Deps, raw string) (*storage.GatewayDownstreamKey, error) {
	if d.gatewayAuthenticate != nil {
		return d.gatewayAuthenticate(raw)
	}
	return d.Gateway.Authenticate(raw)
}

// gatewayWriteRequestAdmissionFailure keeps request cancellation distinct from
// controller pressure. gatewayRequestAdmissionContextError intentionally
// preserves a caller-supplied cancellation cause, which may itself wrap a
// pressure sentinel; the context state still wins because the client has
// already abandoned the request.
func gatewayWriteRequestAdmissionFailure(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errGatewayRequestBodyTooLarge):
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "request body exceeds configured limit"})
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		c.Status(gatewayClientClosedStatus)
	case errors.Is(err, errGatewayRequestAdmissionBusy), errors.Is(err, errGatewayRequestAdmissionQueueFull):
		c.Header("Retry-After", "1")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": gatewayPublicFailureMessage})
	default:
		c.Header("Retry-After", "1")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": gatewayPublicFailureMessage})
	}
}

func gatewayProxyModels(c *gin.Context, d *Deps, downstream *storage.GatewayDownstreamKey) {
	gatewayProxyModelsWithTimeout(c, d, downstream, gatewayModelsProxyTimeout)
}

func gatewayProxyModelsWithTimeout(c *gin.Context, d *Deps, downstream *storage.GatewayDownstreamKey, timeout time.Duration) {
	releaseOwner, err := bindGatewayRequestDispatchOwner(c, d)
	if err != nil {
		abortGatewayRequestDispatch(c, d, "bind request dispatch owner for models proxy", err)
		return
	}
	defer releaseOwner()
	responseSpool, err := gatewayUpstreamResponseSpoolControllerForDeps(d)
	if err != nil {
		abortGatewayRequestDispatch(c, d, "load models response limits", err)
		return
	}

	requestContext, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()

	decision, err := gatewayCandidatesForDownstream(d, downstream, "", "")
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": gatewayPublicFailureMessage})
		return
	}
	seen := make(map[string]struct{})
	data := make([]map[string]any, 0)
	encodedDataBytes := 0
	var errorsSeen []string
	for _, candidate := range decision.Candidates {
		if requestContext.Err() != nil {
			if gatewayRequestDispatchOwnerLeaseLost(c.Request, requestContext.Err()) {
				abortGatewayRequestDispatch(c, d, "models proxy request dispatch owner lease lost", context.Cause(requestContext))
				return
			}
			break
		}
		groupName := strings.TrimSpace(candidate.Rate.ModelName)
		var lease gateway.UpstreamKeyLease
		var err error
		if decision.SmartRouting {
			lease, err = d.Gateway.EnsureUpstreamAPIKeyLeaseForGroup(requestContext, candidate.Channel.ID, groupName, nil, candidate.RawRatio)
		} else {
			lease, err = d.Gateway.FixedUpstreamAPIKeyLeaseForGroup(candidate.Channel.ID, groupName)
		}
		if err != nil {
			if gatewayRequestDispatchOwnerLeaseLost(c.Request, err) {
				abortGatewayRequestDispatch(c, d, "models proxy request dispatch owner lease lost", err)
				return
			}
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %v", candidate.Channel.Name, groupName, err))
			continue
		}
		if lease.ChannelSnapshot != nil {
			candidate.Channel = *lease.ChannelSnapshot
			candidate.UpstreamAccountConcurrency = lease.UpstreamAccountConcurrency
		}
		upstreamKey := lease.Key
		target, err := resolveGatewayCandidateTarget(requestContext, d, candidate)
		if err != nil {
			if gatewayRequestDispatchOwnerLeaseLost(c.Request, err) {
				abortGatewayRequestDispatch(c, d, "models proxy request dispatch owner lease lost", err)
				return
			}
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %v", candidate.Channel.Name, groupName, err))
			continue
		}
		dispatchFence, err := gateway.UpstreamDispatchFenceForCandidate(candidate, lease, downstream.App, "")
		if err != nil {
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %v", candidate.Channel.Name, groupName, err))
			continue
		}
		resp, err := forwardGatewayRequestWithDispatchFence(
			c.Request.WithContext(requestContext), target, upstreamKey, nil,
			&gatewayTransportDispatchFence{service: d.Gateway, fence: dispatchFence}, nil,
		)
		if err != nil {
			if gatewayRequestDispatchOwnerLeaseLost(c.Request, err) {
				abortGatewayRequestDispatch(c, d, "models proxy request dispatch owner lease lost", err)
				return
			}
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %v", candidate.Channel.Name, groupName, err))
			continue
		}
		raw, readErr := readGatewayUpstreamResponseLimited(resp.Body, responseSpool.modelsResponseBytes)
		_ = resp.Body.Close()
		if gatewayRequestDispatchOwnerLeaseLost(c.Request, readErr) {
			abortGatewayRequestDispatch(c, d, "models proxy request dispatch owner lease lost", readErr)
			return
		}
		if readErr != nil {
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: read response body: %v", candidate.Channel.Name, groupName, readErr))
			continue
		}
		if requestContext.Err() != nil {
			if gatewayRequestDispatchOwnerLeaseLost(c.Request, context.Cause(requestContext)) {
				abortGatewayRequestDispatch(c, d, "models proxy request dispatch owner lease lost after response read", context.Cause(requestContext))
				return
			}
			break
		}
		if shouldFallbackGatewayStatus(resp.StatusCode) || resp.StatusCode < 200 || resp.StatusCode >= 300 {
			msg := sanitizeGatewayProbeError(raw, upstreamKey)
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: status %d %s", candidate.Channel.Name, groupName, resp.StatusCode, msg))
			observeGatewayHTTPAuthorizationEvidence(requestContext, d, candidate, lease, groupName, resp.StatusCode, raw)
			continue
		}
		items, err := parseGatewayBoundedModelItems(raw)
		if err != nil {
			errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: decode models: %v", candidate.Channel.Name, groupName, err))
			continue
		}
		if requestContext.Err() != nil {
			if gatewayRequestDispatchOwnerLeaseLost(c.Request, context.Cause(requestContext)) {
				abortGatewayRequestDispatch(c, d, "models proxy request dispatch owner lease lost before model facts update", context.Cause(requestContext))
				return
			}
			break
		}
		if candidate.Channel.CredentialMode == storage.CredentialModeDirect {
			// The response may arrive after an administrator edits or removes this
			// group. Revalidate the exact source projection before exposing models
			// from the old key/configuration snapshot.
			if err := d.Gateway.WithUpstreamDispatchFenceAndThen(requestContext, dispatchFence, nil); err != nil {
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: direct configuration changed after models response: %v", candidate.Channel.Name, groupName, err))
				continue
			}
		}
		if candidate.Channel.CredentialMode != storage.CredentialModeDirect {
			if modelFence, ok := gateway.ModelFactFenceForCandidate(candidate); ok {
				updateGatewayModelFamiliesForIdentity(d, modelFence, gatewayModelFamiliesFromItems(items), gatewayModelIDsFromItems(items), "")
			}
		}
		for _, item := range items {
			id, _ := item["id"].(string)
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			if candidate.Channel.CredentialMode == storage.CredentialModeDirect {
				if normalizeDirectGatewayApp(candidate.Channel.GatewayApp) != normalizeDirectGatewayApp(downstream.App) ||
					!gatewayModelIDExact(candidate.ModelIDs, id) {
					continue
				}
			} else if !gatewayFamilySupportsApp(gatewayModelFamilyFromName(id), downstream.App) {
				continue
			}
			data, encodedDataBytes, err = appendGatewayBoundedUniqueModelItem(data, seen, encodedDataBytes, item)
			if err != nil {
				abortGatewayRequestDispatch(c, d, "aggregate bounded models response", err)
				return
			}
		}
	}
	if gatewayRequestDispatchOwnerLeaseLost(c.Request, requestContext.Err()) {
		abortGatewayRequestDispatch(c, d, "models proxy request dispatch owner lease lost", context.Cause(requestContext))
		return
	}
	if len(data) == 0 {
		errText := sanitizeGatewayProbeError([]byte(strings.Join(errorsSeen, "; ")), "")
		if errText == "" {
			errText = "all upstream channels returned no models"
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": gatewayPublicFailureMessage})
		return
	}
	c.JSON(http.StatusOK, gin.H{"object": "list", "data": data})
}

func gatewayUsage(c *gin.Context, d *Deps, downstream *storage.GatewayDownstreamKey) {
	usage, err := d.Gateway.Store.SummarizeUsage(downstream.ID)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, usage)
}

type gatewayRouteDecision struct {
	Candidates           []gateway.Candidate
	RankedCandidates     []gateway.Candidate
	PreMappingCandidates []gateway.Candidate
	SessionHash          string
	SmartRouting         bool
	AffinityApplied      bool
	AffinityOrigin       *storage.GatewayRecentRoute
	AffinityExpiresAt    *time.Time
	Exclusions           []gateway.CandidateExclusion
	RequestedModel       string
}

func gatewayCandidatesForDownstream(d *Deps, downstream *storage.GatewayDownstreamKey, model string, sessionHash string) (*gatewayRouteDecision, error) {
	if downstream == nil {
		return nil, fmt.Errorf("missing downstream key")
	}
	decision := &gatewayRouteDecision{
		SessionHash:    strings.TrimSpace(sessionHash),
		SmartRouting:   downstream.SmartRouting,
		RequestedModel: strings.TrimSpace(model),
	}
	if downstream.SmartRouting {
		candidates, exclusions, err := d.Gateway.CandidatesForDownstreamKeyWithExclusions(downstream.ID, downstream.App, model, false)
		decision.Candidates = candidates
		decision.RankedCandidates = append([]gateway.Candidate(nil), candidates...)
		decision.Exclusions = exclusions
		if err != nil {
			return decision, err
		}
		ordered, origin := gatewayCacheAffinityCandidatesWithOrigin(d, downstream, model, decision.SessionHash, candidates)
		decision.Candidates = ordered
		gatewaySetSessionAffinityDecision(d, decision, origin)
		return decision, nil
	}
	candidates, err := d.Gateway.ManualCandidateForApp(downstream.ID, downstream.App, downstream.ManualChannelID, downstream.ManualGroupName, model)
	if err != nil {
		return decision, err
	}
	decision.Candidates = candidates
	decision.RankedCandidates = append([]gateway.Candidate(nil), candidates...)
	return decision, nil
}

func gatewaySetSessionAffinityDecision(d *Deps, decision *gatewayRouteDecision, origin *storage.GatewayRecentRoute) {
	if decision == nil {
		return
	}
	decision.AffinityOrigin = origin
	decision.AffinityApplied = origin != nil
	decision.AffinityExpiresAt = nil
	if d == nil || d.Gateway == nil || origin == nil || origin.CreatedAt.IsZero() {
		return
	}
	maxAge := d.Gateway.CurrentRoutingConfig().SessionAffinityMaxAge
	if maxAge <= 0 {
		return
	}
	expiresAt := origin.CreatedAt.UTC().Add(maxAge)
	decision.AffinityExpiresAt = &expiresAt
}

func gatewayCacheAffinityCandidates(d *Deps, downstream *storage.GatewayDownstreamKey, model string, sessionHash string, candidates []gateway.Candidate) ([]gateway.Candidate, bool) {
	ordered, origin := gatewayCacheAffinityCandidatesWithOrigin(d, downstream, model, sessionHash, candidates)
	return ordered, origin != nil
}

func gatewayCacheAffinityCandidatesWithOrigin(d *Deps, downstream *storage.GatewayDownstreamKey, model string, sessionHash string, candidates []gateway.Candidate) ([]gateway.Candidate, *storage.GatewayRecentRoute) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil || downstream == nil || model == "" || sessionHash == "" || len(candidates) == 0 {
		return candidates, nil
	}
	routing := d.Gateway.CurrentRoutingConfig()
	route, err := d.Gateway.Store.LastSuccessfulRoute(downstream.ID, sessionHash, model, time.Now().Add(-routing.SessionAffinityMaxAge))
	if err != nil || !gatewaySessionAffinityOriginAuditable(route) {
		return candidates, nil
	}
	if candidates[0].Channel.ID == route.ChannelID && strings.TrimSpace(candidates[0].Rate.ModelName) == strings.TrimSpace(route.GroupName) {
		// The previous route is already the normal winner. Keep the origin so
		// this request receives the session-affinity first-byte budget even
		// though no candidate reorder was needed.
		return candidates, route
	}
	if !gatewaySessionAffinityCostSafe(candidates, route.ChannelID, route.GroupName) {
		return candidates, nil
	}
	if route.RouteReason == gatewayRouteReasonFallback {
		ordered, applied := reorderGatewayCandidatesForCacheAffinity(candidates, route.ChannelID, route.GroupName)
		if applied {
			return ordered, route
		}
		return candidates, nil
	}
	if candidates[0].LatencySamples >= int64(routing.SessionAffinityStableSamples) {
		return candidates, nil
	}
	ordered, applied := reorderGatewayCandidatesForCacheAffinity(candidates, route.ChannelID, route.GroupName)
	if applied {
		return ordered, route
	}
	return candidates, nil
}

// gatewaySessionAffinityOriginAuditable refuses to let a legacy or incomplete
// request record change current routing. A session route may only inherit an
// origin that exposes the original request-time selection rank and cause.
func gatewaySessionAffinityOriginAuditable(route *storage.GatewayRecentRoute) bool {
	if route == nil || route.ID == 0 || route.SelectionRank == 0 {
		return false
	}
	switch route.RouteReason {
	case gatewayRouteReasonSmartPrimary:
		return route.SelectionRank == 1
	case gatewayRouteReasonFallback:
		return gatewayFallbackAffinityEvidenceComplete(route)
	default:
		return false
	}
}

// gatewayFallbackAffinityEvidenceComplete proves why a fallback route was
// selected before it is allowed to affect a later request. A route at normal
// priority N must include one structured, failed attempt for every preceding
// priority 1..N-1. Old prose details are intentionally not accepted: they
// cannot prove which candidate was actually tried at the time.
func gatewayFallbackAffinityEvidenceComplete(route *storage.GatewayRecentRoute) bool {
	if route == nil || route.SelectionRank < 2 {
		return false
	}
	traces, ok := gatewayRouteAttemptTraces(route.RouteAttemptedChannels)
	if !ok || len(traces) != int(route.SelectionRank-1) {
		return false
	}
	seenRanks := make(map[uint]struct{}, len(traces))
	for _, trace := range traces {
		if !trace.Physical || trace.EligibleRank == 0 || trace.EligibleRank >= route.SelectionRank || trace.ChannelID == 0 || strings.TrimSpace(trace.GroupName) == "" || !gatewayRouteAttemptIsFailure(trace.Reason) {
			return false
		}
		if _, exists := seenRanks[trace.EligibleRank]; exists {
			return false
		}
		seenRanks[trace.EligibleRank] = struct{}{}
	}
	for rank := uint(1); rank < route.SelectionRank; rank++ {
		if _, exists := seenRanks[rank]; !exists {
			return false
		}
	}
	return true
}

// gatewaySessionAffinityCostSafe keeps a prior route only when it is not more
// expensive than every current eligible candidate. The score winner may be
// faster but more expensive than another eligible route, so comparing only to
// position one would let affinity hide a higher-cost choice.
func gatewaySessionAffinityCostSafe(candidates []gateway.Candidate, channelID uint, groupName string) bool {
	if len(candidates) == 0 || channelID == 0 || strings.TrimSpace(groupName) == "" {
		return false
	}
	lowestPrice := 0.0
	affinityPrice := 0.0
	lowestFound, affinityFound := false, false
	for _, candidate := range candidates {
		if candidate.DisabledForKey || candidate.CoolingDown || candidate.EffectivePrice < 0 || (candidate.EffectivePrice == 0 && candidate.EffectivePriceBasis != gateway.RoutingPriceBasisModel) {
			continue
		}
		if !lowestFound || candidate.EffectivePrice < lowestPrice {
			lowestPrice = candidate.EffectivePrice
			lowestFound = true
		}
		if candidate.Channel.ID == channelID && strings.EqualFold(strings.TrimSpace(candidate.Rate.ModelName), strings.TrimSpace(groupName)) {
			affinityPrice = candidate.EffectivePrice
			affinityFound = true
		}
	}
	return affinityFound && lowestFound && affinityPrice <= lowestPrice
}

func reorderGatewayCandidatesForCacheAffinity(candidates []gateway.Candidate, channelID uint, groupName string) ([]gateway.Candidate, bool) {
	if len(candidates) < 2 || channelID == 0 || strings.TrimSpace(groupName) == "" {
		return candidates, false
	}
	for i := 1; i < len(candidates); i++ {
		candidateGroup := strings.TrimSpace(candidates[i].Rate.ModelName)
		if candidates[i].Channel.ID != channelID || !strings.EqualFold(candidateGroup, groupName) {
			continue
		}
		out := make([]gateway.Candidate, 0, len(candidates))
		out = append(out, candidates[i])
		out = append(out, candidates[:i]...)
		out = append(out, candidates[i+1:]...)
		return out, true
	}
	return candidates, false
}

func gatewayForward(c *gin.Context, d *Deps, downstream *storage.GatewayDownstreamKey, decision *gatewayRouteDecision, model string, reasoningEffort string, body []byte, mapping gatewayModelMappingResult, start time.Time) bool {
	return gatewayForwardWithHistory(c, d, downstream, decision, model, reasoningEffort, body, mapping, start, nil, nil)
}

// gatewayRouteSnapshot freezes the route facts before an upstream request is
// dispatched. The selected candidate and key may change while a stream is in
// progress, but the request record and later reconciliation must refer to the
// exact route that was used for this request.
type gatewayRouteSnapshot struct {
	Candidate               gateway.Candidate
	ChannelID               uint
	ChannelName             string
	GroupName               string
	ChannelIdentityRevision uint64
	UpstreamGroupID         uint
	UpstreamGroupGeneration uint64
	UpstreamKeyID           int64
	UpstreamKeyName         string
	UpstreamKeyLocalID      uint
	UpstreamKeyGeneration   uint64
	// MultiplierProvenanceInvalidated is deliberately narrower than route
	// validity. A lease may return a newly discovered API origin/key that is
	// safe to send to, while the candidate's rate/funding facts were selected
	// under the preceding channel identity. In that case the request still
	// needs its physical-dispatch audit, but it must contribute no trusted
	// multiplier or local-price evidence.
	MultiplierProvenanceInvalidated bool
}

func gatewayRouteSnapshotForCandidate(candidate gateway.Candidate) gatewayRouteSnapshot {
	selectionGeneration := gatewayRouteSelectionGeneration(candidate)
	return gatewayRouteSnapshot{
		Candidate:               candidate,
		ChannelID:               candidate.Channel.ID,
		ChannelName:             strings.TrimSpace(candidate.Channel.Name),
		GroupName:               strings.TrimSpace(candidate.Rate.ModelName),
		ChannelIdentityRevision: candidate.Channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         selectionGeneration.ID,
		UpstreamGroupGeneration: selectionGeneration.PresenceGeneration,
	}
}

// gatewayRouteSnapshotAfterLeaseChannel keeps the actual channel snapshot
// returned by a slow key-preparation path for transport and health handling.
// It never retargets the selection-time multiplier provenance to a newly
// discovered account identity: the candidate's Rate/raw/funding values were
// computed before the lease and therefore no longer belong to that identity.
func gatewayRouteSnapshotAfterLeaseChannel(route gatewayRouteSnapshot, candidate gateway.Candidate) gatewayRouteSnapshot {
	selectedRevision := route.ChannelIdentityRevision
	leasedRevision := candidate.Channel.UpstreamAccountIdentityRevision
	if route.ChannelID == 0 || candidate.Channel.ID == 0 ||
		route.ChannelID != candidate.Channel.ID || selectedRevision == 0 || leasedRevision == 0 ||
		selectedRevision != leasedRevision {
		route.Candidate = candidate
		route.ChannelID = candidate.Channel.ID
		route.ChannelName = strings.TrimSpace(candidate.Channel.Name)
		route.MultiplierProvenanceInvalidated = true
		return route
	}
	refreshed := gatewayRouteSnapshotForCandidate(candidate)
	// A retry may begin from the same post-discovery identity. Preserve the
	// caller's sticky fence instead of treating the old selection facts as if
	// they had been selected under that new identity.
	refreshed.MultiplierProvenanceInvalidated = route.MultiplierProvenanceInvalidated
	return refreshed
}

// gatewayRouteSelectionGeneration returns only the group-generation fact that
// accompanied a real persisted rate snapshot. ModelFactGeneration and
// KeyStateGeneration fence their own side effects; neither is multiplier
// provenance for a fixed route whose current rate snapshot is absent.
func gatewayRouteSelectionGeneration(candidate gateway.Candidate) storage.UpstreamGroupGeneration {
	rate := candidate.Rate
	generation := candidate.UpstreamGeneration
	if rate.ID == 0 || rate.ChannelID == 0 || rate.ChannelID != candidate.Channel.ID ||
		!generation.Valid() || generation.ChannelID != rate.ChannelID ||
		!strings.EqualFold(strings.TrimSpace(generation.CanonicalGroupName), strings.TrimSpace(rate.ModelName)) {
		return storage.UpstreamGroupGeneration{}
	}
	return generation
}

// gatewayRouteMultiplierProvenance is the single projection used by both the
// pre-send dispatch audit and direct terminal logs. A numeric 0x rate remains
// valid when it came from a persisted RateSnapshot; absence is represented by
// an incomplete tuple, never inferred from the multiplier value.
type gatewayRouteMultiplierProvenance struct {
	GroupRatio              float64
	CompletionRatio         float64
	RawGroupRatio           *float64
	RawCompletionRatio      *float64
	FundingFactor           *float64
	CostBasisVersion        uint64
	CostBasisSource         string
	ChannelIdentityRevision uint64
	UpstreamGroupID         uint
	UpstreamGroupGeneration uint64
	UpstreamKeyLocalID      uint
	UpstreamKeyGeneration   uint64
}

func (snapshot gatewayRouteSnapshot) multiplierProvenance() (gatewayRouteMultiplierProvenance, bool) {
	candidate := snapshot.Candidate
	generation := gatewayRouteSelectionGeneration(candidate)
	if snapshot.MultiplierProvenanceInvalidated || !generation.Valid() ||
		snapshot.ChannelID == 0 || candidate.Channel.ID != snapshot.ChannelID ||
		candidate.Rate.ChannelID != snapshot.ChannelID ||
		!strings.EqualFold(strings.TrimSpace(candidate.Rate.ModelName), strings.TrimSpace(snapshot.GroupName)) ||
		snapshot.ChannelIdentityRevision == 0 || candidate.Channel.UpstreamAccountIdentityRevision != snapshot.ChannelIdentityRevision ||
		snapshot.UpstreamGroupID == 0 || snapshot.UpstreamGroupID != generation.ID ||
		snapshot.UpstreamGroupGeneration == 0 || snapshot.UpstreamGroupGeneration != generation.PresenceGeneration ||
		snapshot.UpstreamKeyLocalID == 0 || snapshot.UpstreamKeyGeneration == 0 {
		return gatewayRouteMultiplierProvenance{}, false
	}
	return gatewayRouteMultiplierProvenance{
		GroupRatio:              candidate.Rate.Ratio,
		CompletionRatio:         candidate.Rate.CompletionRatio,
		RawGroupRatio:           numberPointer(candidate.RawRatio),
		RawCompletionRatio:      numberPointer(candidate.RawCompletionRatio),
		FundingFactor:           numberPointer(candidate.FundingFactor),
		CostBasisVersion:        candidate.CostBasisVersion,
		CostBasisSource:         candidate.CostBasisSource,
		ChannelIdentityRevision: snapshot.ChannelIdentityRevision,
		UpstreamGroupID:         snapshot.UpstreamGroupID,
		UpstreamGroupGeneration: snapshot.UpstreamGroupGeneration,
		UpstreamKeyLocalID:      snapshot.UpstreamKeyLocalID,
		UpstreamKeyGeneration:   snapshot.UpstreamKeyGeneration,
	}, true
}

func (snapshot gatewayRouteSnapshot) applyToLog(log *storage.GatewayRequestLog) {
	if log == nil {
		return
	}
	log.ChannelID = snapshot.ChannelID
	log.ChannelNameSnapshot = snapshot.ChannelName
	log.GroupName = snapshot.GroupName
	log.UpstreamKeyID = snapshot.UpstreamKeyID
	log.UpstreamKeyName = snapshot.UpstreamKeyName
	provenance, complete := snapshot.multiplierProvenance()
	log.GroupRatio = provenance.GroupRatio
	log.CompletionRatio = provenance.CompletionRatio
	log.RawGroupRatio = provenance.RawGroupRatio
	log.RawCompletionRatio = provenance.RawCompletionRatio
	log.FundingFactor = provenance.FundingFactor
	log.CostBasisVersion = provenance.CostBasisVersion
	log.CostBasisSource = provenance.CostBasisSource
	log.ChannelIdentityRevision = provenance.ChannelIdentityRevision
	log.UpstreamGroupID = provenance.UpstreamGroupID
	log.UpstreamGroupGeneration = provenance.UpstreamGroupGeneration
	log.UpstreamKeyLocalID = provenance.UpstreamKeyLocalID
	log.UpstreamKeyGeneration = provenance.UpstreamKeyGeneration
	if !complete {
		// Some branches legitimately finish before an audit or key lease exists.
		// Mark their direct logs now so append-time funding completion and later
		// funding corrections cannot refill an unproven multiplier tuple.
		log.GroupRatio = 0
		log.CompletionRatio = 0
		log.RawGroupRatio = nil
		log.RawCompletionRatio = nil
		log.FundingFactor = nil
		log.CostBasisVersion = 0
		log.CostBasisSource = ""
		log.ChannelIdentityRevision = 0
		log.UpstreamGroupID = 0
		log.UpstreamGroupGeneration = 0
		log.UpstreamKeyLocalID = 0
		log.UpstreamKeyGeneration = 0
		log.Cost = 0
		log.RawCost = nil
		log.CostEstimated = false
		log.CostSource = ""
		log.CostStatus = storage.GatewayCostStatusUnavailable
		log.CostUnavailableReason = storage.GatewayPricingUnavailableScopeIncomplete
		return
	}
}

func gatewayForwardWithHistory(c *gin.Context, d *Deps, downstream *storage.GatewayDownstreamKey, decision *gatewayRouteDecision, model string, reasoningEffort string, body []byte, mapping gatewayModelMappingResult, start time.Time, priorAttempts []gatewayRouteAttempt, priorErrors []string) bool {
	if downstream == nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "missing downstream key"})
		return true
	}
	telemetry := gatewayRequestTelemetryFromContext(c, start, downstream)
	releaseOwner, err := bindGatewayRequestDispatchOwner(c, d)
	if err != nil {
		abortGatewayRequestDispatch(c, d, "bind request dispatch owner for gateway request", err)
		return true
	}
	defer releaseOwner()
	responseSpool, err := gatewayUpstreamResponseSpoolControllerForDeps(d)
	if err != nil {
		abortGatewayRequestDispatch(c, d, "load upstream response spool controller", err)
		return true
	}

	downstreamKeyID := downstream.ID
	app := downstream.App
	if decision == nil {
		decision = &gatewayRouteDecision{}
	}
	telemetry.captureRouteExclusions(decision, time.Now().UTC())
	smartRouting := decision.SmartRouting
	if start.IsZero() {
		start = time.Now()
	}
	errorsSeen := append([]string(nil), priorErrors...)
	attempts := append([]gatewayRouteAttempt(nil), priorAttempts...)
	configuredFirstByteBudget := d.Gateway.CurrentRoutingConfig().FailurePolicy.ResponseTimeout
	contextLimitAttempts := 0
	directImageRequest := c.Request.URL.Path == "/v1/images/generations"
	imageRequest := isGatewayImageGenerationRequest(c.Request.URL.Path, body)
	streamingRequest := isGatewayStreamingRequest(body)
	imageRequestCommitted := false
	imageRequestAccepted := false
	committedImageUpstreamRequestID := ""
	committedImageMetrics := gatewayUsageMetrics{}
	var committedImageRoute gatewayRouteSnapshot
	var committedImageDispatch *gatewayDispatchAuditState
	var fixedRoute gatewayRouteSnapshot
	var pendingFinalAudit *gatewayDispatchAuditState
	var pendingFinalAuditAt time.Time
	var currentAttemptAudit *gatewayDispatchAuditState
	var currentAttemptUsage gatewayUsageMetrics
	var pendingFinalUsage gatewayUsageMetrics
	dispatchAudits := make([]*gatewayDispatchAuditState, 0, len(decision.Candidates))
	dispatchRecoveryRequired := make(map[*gatewayDispatchAuditState]struct{})
	closeFallbackAudit := func(audit *gatewayDispatchAuditState, terminalKind string, finishedAt time.Time) bool {
		if audit == nil || audit.closed || audit.surrendered {
			return true
		}
		if requestCause := context.Cause(c.Request.Context()); gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
			abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost before retaining fallback attempt", requestCause)
			return false
		}
		if audit.billingCommitted || audit.acceptanceObserved {
			err := storage.ErrGatewayRequestDispatchRequiresFinalization
			scheduleGatewayDispatchRecovery(d, audit, nil, gatewayDispatchTerminalAuditFailure, start, finishedAt, err)
			abortGatewayRequestDispatch(c, d, "retain billable request dispatch before retry or fallback", err)
			return false
		}
		audit.recordTelemetryPhysicalFailure(telemetry)
		if pendingFinalAudit != nil && pendingFinalAudit != audit {
			err := storage.ErrGatewayRequestDispatchConflict
			scheduleGatewayDispatchRecovery(d, audit, nil, gatewayDispatchTerminalAuditFailure, start, finishedAt, err)
			abortGatewayRequestDispatch(c, d, "retain more than one pending request dispatch", err)
			return false
		}
		_ = terminalKind
		pendingFinalAudit = audit
		pendingFinalAuditAt = finishedAt
		pendingFinalUsage = currentAttemptUsage
		return true
	}
	finalizeClientCanceledAudit := func(audit *gatewayDispatchAuditState, route gatewayRouteSnapshot, finishedAt time.Time) bool {
		if audit == nil && pendingFinalAudit != nil {
			audit = pendingFinalAudit
			route = audit.route
		}
		requestLog := gatewayClientCanceledRequestLog(
			downstreamKeyID, decision, route, model, reasoningEffort, mapping,
			gatewayRequestIsKeyTest(c), gatewayClientIP(c), gatewayClientIPSource(c),
			c.Request.Method, c.Request.URL.Path, start, finishedAt,
		)
		usage := currentAttemptUsage
		if audit != nil && audit == pendingFinalAudit {
			usage = pendingFinalUsage
		}
		applyGatewayRequestUsageAndCost(requestLog, usage, imageRequest, route.Candidate.FundingFactor)
		telemetry.applyToLog(requestLog, "client_canceled")
		if audit == nil {
			d.Gateway.LogRequest(requestLog)
			return true
		}
		if _, err := audit.finalize(d.Gateway, requestLog, gatewayDispatchTerminalClientCanceled, start, finishedAt); err != nil {
			scheduleGatewayDispatchRecovery(d, audit, requestLog, gatewayDispatchTerminalClientCanceled, start, finishedAt, err)
			abortGatewayRequestDispatch(c, d, "finalize client-canceled request dispatch", err)
			return false
		}
		if pendingFinalAudit == audit {
			pendingFinalAudit = nil
			pendingFinalAuditAt = time.Time{}
			pendingFinalUsage = gatewayUsageMetrics{}
		}
		return true
	}
	defer func() {
		if gatewayRequestDispatchOwnerLeaseLost(c.Request, context.Cause(c.Request.Context())) {
			// Rotation waits for this immutable owner binding to drain, so every
			// physical attempt can persist its exact in-memory response/provider
			// facts before the store switches to the replacement identity.
			for _, audit := range dispatchAudits {
				if audit == nil || audit.closed || audit.surrendered {
					continue
				}
				if _, recoveryRequired := dispatchRecoveryRequired[audit]; recoveryRequired {
					continue
				}
				finishedAt := time.Now()
				if audit == pendingFinalAudit && !pendingFinalAuditAt.IsZero() {
					finishedAt = pendingFinalAuditAt
				}
				recoveryLog := gatewayDispatchOwnerLossRecoveryLog(
					audit, pendingFinalAudit, currentAttemptAudit,
					pendingFinalUsage, currentAttemptUsage,
					imageRequest, c.Request, start, finishedAt,
				)
				scheduleGatewayDispatchRecovery(d, audit, recoveryLog, storage.GatewayRequestDispatchTerminalOwnerLeaseLost, start, finishedAt, context.Cause(c.Request.Context()))
			}
			return
		}
		// Abnormal early returns still surrender every open physical attempt. The
		// ordinary all-failed and cancellation paths finalize the retained last
		// attempt before reaching this cleanup.
		for _, audit := range dispatchAudits {
			if _, recoveryRequired := dispatchRecoveryRequired[audit]; recoveryRequired {
				continue
			}
			finishedAt := time.Now()
			if audit == pendingFinalAudit && !pendingFinalAuditAt.IsZero() {
				finishedAt = pendingFinalAuditAt
			}
			if err := closeGatewayDispatchAuditForFallback(d.Gateway, audit, gatewayDispatchTerminalFailedBeforeAcceptance, start, finishedAt); err != nil {
				scheduleGatewayDispatchRecovery(d, audit, nil, gatewayDispatchTerminalAuditFailure, start, finishedAt, err)
				abortGatewayRequestDispatch(c, d, "deferred request dispatch cleanup", err)
			}
		}
	}()
	failCommittedImageRequest := func(route gatewayRouteSnapshot, attempt gatewayRouteAttempt, failure, routingFailureKind string, routingFailureStatus int) bool {
		finishedAt := time.Now()
		attempt.Physical = committedImageDispatch != nil && committedImageDispatch.physicallyDispatched
		if committedImageDispatch != nil {
			if attempt.Physical && committedImageDispatch.firstByteBudgetMs > 0 {
				attempt.FirstByteBudgetMs = committedImageDispatch.firstByteBudgetMs
			}
			committedImageDispatch.recordTelemetryPhysicalFailure(telemetry)
		}
		requestCause := context.Cause(c.Request.Context())
		if gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
			abortGatewayRequestDispatch(c, d, "image request dispatch owner lease lost", requestCause)
			return true
		}
		clientCanceled := isGatewayClientCanceled(c.Request, requestCause)
		failure = sanitizeGatewayProbeError([]byte(failure), "")
		if clientCanceled {
			failure = "客户端取消"
		}
		terminalAttempts := append(append([]gatewayRouteAttempt(nil), attempts...), attempt)
		terminalErrors := append(append([]string(nil), errorsSeen...), fmt.Sprintf("%s/%s: %s", route.ChannelName, route.GroupName, failure))
		routeDetailsAttempts := terminalAttempts
		selectedRoute := gateway.Candidate{}
		if !smartRouting {
			// Fixed routing has one explicit target, so expose only that target
			// snapshot and the sanitized error, never a fallback attempt trace.
			routeDetailsAttempts = nil
			selectedRoute = route.Candidate
		}
		metrics := estimateGatewayCost(committedImageMetrics, route.Candidate.RawRatio, route.Candidate.RawCompletionRatio)
		rawCost := metrics.Cost
		if metrics.CostObserved {
			if actualCost, conversionErr := costing.Actual(rawCost, route.Candidate.FundingFactor); conversionErr == nil {
				metrics.Cost = actualCost
			}
		}
		statusCode := http.StatusBadGateway
		routeReason := gatewayRequestRouteReason(c, gatewayRouteFailureReason(decision))
		if clientCanceled {
			statusCode = gatewayClientClosedStatus
			routeReason = gatewayClientCanceledRouteReason(gatewayRequestIsKeyTest(c))
		}
		requestLog := &storage.GatewayRequestLog{
			DownstreamKeyID:                downstreamKeyID,
			ClientIP:                       gatewayClientIP(c),
			ClientIPSource:                 gatewayClientIPSource(c),
			ModelName:                      model,
			ReasoningEffort:                reasoningEffort,
			Method:                         c.Request.Method,
			Path:                           c.Request.URL.Path,
			StatusCode:                     statusCode,
			UpstreamAccepted:               imageRequestAccepted,
			BillingCommitted:               imageRequestCommitted,
			LatencyMs:                      finishedAt.Sub(start).Milliseconds(),
			PromptTokens:                   metrics.PromptTokens,
			CompletionTokens:               metrics.CompletionTokens,
			CacheCreationTokens:            metrics.CacheCreationTokens,
			CacheReadTokens:                metrics.CacheReadTokens,
			TotalTokens:                    metrics.TotalTokens,
			Cost:                           metrics.Cost,
			RawCost:                        optionalCostPointer(rawCost, metrics.CostObserved),
			CostEstimated:                  metrics.CostEstimated,
			CostSource:                     metrics.CostSource,
			UpstreamRequestID:              committedImageUpstreamRequestID,
			SessionHash:                    decision.SessionHash,
			RouteReason:                    routeReason,
			RouteSelectionRank:             gatewayRouteSelectionRank(decision, route.Candidate),
			SessionAffinityOriginRequestID: gatewaySessionAffinityOriginRequestID(decision),
			RouteDetails:                   gatewayRouteDetailsWithErrors(decision, routeDetailsAttempts, selectedRoute, terminalErrors),
			RouteAttemptedChannels:         gatewayRouteAttemptedChannels(decision, terminalAttempts, gateway.Candidate{}),
			Error:                          failure,
		}
		gatewayApplySessionAffinityAudit(requestLog, decision)
		route.applyToLog(requestLog)
		mapping.applyToLog(requestLog)
		telemetry.applyToLog(requestLog, "")
		if storage.IsGatewayRequestCostReconcileEligible(requestLog) {
			costReconcileAt := gateway.InitialRequestCostReconcileAt(finishedAt)
			requestLog.CostReconcileAt = &costReconcileAt
		}
		if committedImageDispatch != nil {
			terminalKind := gatewayDispatchTerminalImageFailedAfterDispatch
			if clientCanceled {
				terminalKind = gatewayDispatchTerminalClientCanceled
			}
			if _, finalizeErr := committedImageDispatch.finalize(d.Gateway, requestLog, terminalKind, start, finishedAt); finalizeErr != nil {
				scheduleGatewayDispatchRecovery(d, committedImageDispatch, requestLog, terminalKind, start, finishedAt, finalizeErr)
				abortGatewayRequestDispatch(c, d, "finalize committed image request dispatch", finalizeErr)
				return true
			}
		} else {
			d.Gateway.LogRequest(requestLog)
			d.Gateway.ReconcileRequestCost(requestLog, start, finishedAt)
		}
		if !clientCanceled && smartRouting && routingFailureKind != "" {
			if requestCause := context.Cause(c.Request.Context()); gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
				abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost before image routing failure update", requestCause)
				return true
			}
			markGatewayCandidateRoutingFailure(d, route.Candidate, route.GroupName, routingFailureKind, routingFailureStatus, failure)
		}
		if !clientCanceled {
			c.JSON(http.StatusBadGateway, gin.H{"error": gatewayPublicFailureMessage})
			persistGatewayDownstreamFirstVisible(d, requestLog, telemetry)
		}
		return true
	}
	candidates := decision.Candidates
	if !smartRouting && len(candidates) > 1 {
		candidates = candidates[:1]
	}
candidateLoop:
	for _, candidate := range candidates {
		if requestCause := context.Cause(c.Request.Context()); gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
			abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost before candidate selection", requestCause)
			return true
		}
		if imageRequest && imageRequestCommitted {
			break
		}
		groupName := strings.TrimSpace(candidate.Rate.ModelName)
		candidateErrorStart := len(errorsSeen)
		candidateAttemptStart := len(attempts)
		candidateBudgetStartedAt := time.Now()
		var candidateFirstByteStartedAt time.Time
		candidateFirstByteBudget := gatewayCandidateFirstByteBudget(decision, configuredFirstByteBudget, imageRequest)
		waitingProbe := smartRouting && !imageRequest && gatewayWaitingProbeSupported(c.Request.URL.Path)
		if waitingProbe {
			candidateFirstByteBudget = 0
		}
		// A slow lease can update the local channel snapshot in-place. Keep a
		// provenance invalidation sticky across same-candidate confirmation
		// retries, otherwise retry two would treat that post-discovery identity
		// as if it had selected the old rate/funding basis itself.
		candidateMultiplierProvenanceInvalidated := false
		// Confirm fast transient failures on the same route. Once the bounded
		// confirmation fails, cool that route and continue with the next eligible
		// candidate. Retries share this candidate's first-byte budget, while image
		// requests are never replayed because an accepted generation may already be
		// billed.
		candidateAttempts := 1
		if smartRouting {
			candidateAttempts = gatewayCandidateRetryAttempts
		}
		for candidateAttempt := 1; candidateAttempt <= candidateAttempts; candidateAttempt++ {
			var waitingDeadline *gatewayFirstByteDeadline
			currentAttemptAudit = nil
			currentAttemptUsage = gatewayUsageMetrics{}
			if requestCause := context.Cause(c.Request.Context()); gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
				abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost before candidate attempt", requestCause)
				return true
			}
			retryCandidate := func() bool {
				if waitingDeadline != nil && waitingDeadline.WaitingProbeFailed() {
					return false
				}
				if waitingProbe && candidateFirstByteStartedAt.IsZero() && time.Since(candidateBudgetStartedAt) >= configuredFirstByteBudget {
					return false
				}
				if !smartRouting || imageRequest || candidateAttempt >= candidateAttempts {
					return false
				}
				budgetStartedAt := candidateBudgetStartedAt
				if !candidateFirstByteStartedAt.IsZero() {
					budgetStartedAt = candidateFirstByteStartedAt
				}
				if candidateFirstByteBudget > 0 && time.Since(budgetStartedAt) >= candidateFirstByteBudget {
					return false
				}
				errorsSeen = errorsSeen[:candidateErrorStart]
				attempts = attempts[:candidateAttemptStart]
				return true
			}
			confirmCandidateFailure := func(kind string, status int, message string) bool {
				if !smartRouting {
					return true
				}
				if requestCause := context.Cause(c.Request.Context()); gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost before routing failure update", requestCause)
					return false
				}
				budgetStartedAt := candidateBudgetStartedAt
				if !candidateFirstByteStartedAt.IsZero() {
					budgetStartedAt = candidateFirstByteStartedAt
				}
				budgetExhausted := candidateFirstByteBudget > 0 && time.Since(budgetStartedAt) >= candidateFirstByteBudget
				budgetExhausted = budgetExhausted || (waitingDeadline != nil && waitingDeadline.WaitingProbeFailed())
				if candidateAttempt < candidateAttempts && !budgetExhausted {
					return true
				}
				markGatewayCandidateRoutingFailure(d, candidate, groupName, kind, status, message, candidateAttempt)
				return true
			}
			route := gatewayRouteSnapshotForCandidate(candidate)
			route.MultiplierProvenanceInvalidated = candidateMultiplierProvenanceInvalidated
			if !smartRouting {
				fixedRoute = route
			}
			attemptTimeout := candidateFirstByteBudget
			if candidateFirstByteBudget > 0 && candidateAttempt > 1 && !candidateFirstByteStartedAt.IsZero() {
				remainingBudget := candidateFirstByteBudget - time.Since(candidateFirstByteStartedAt)
				if remainingBudget <= 0 {
					// Earlier retries consumed this candidate's budget. Record only the
					// physical failures that actually ran, then continue without granting
					// this route a fresh window.
					msg := gatewayFirstByteTimeoutMessage(candidateFirstByteBudget)
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttempt(candidate, "response_timeout", http.StatusGatewayTimeout, msg))
					if requestCause := context.Cause(c.Request.Context()); gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
						abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost before timeout health update", requestCause)
						return true
					}
					markGatewayCandidateRoutingFailure(d, candidate, groupName, gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout, msg, candidateAttempt-1)
					continue candidateLoop
				}
				attemptTimeout = remainingBudget
			}
			preparationTimeout := attemptTimeout
			if waitingProbe {
				preparationTimeout = configuredFirstByteBudget
			}
			if preparationTimeout > 0 {
				// Durable lease, capability, and dispatch-audit preparation is bounded
				// independently from the physical response budget. Keep a small fixed
				// handoff margin for scheduler and database latency without allowing a
				// cold candidate probe to run unbounded.
				preparationTimeout += 500 * time.Millisecond
			}
			firstByteDeadline := newGatewayFirstByteDeadline(c.Request.Context(), preparationTimeout)
			firstByteDeadline.waiting = waitingProbe
			waitingDeadline = firstByteDeadline
			var lease gateway.UpstreamKeyLease
			var err error
			if smartRouting {
				lease, err = d.Gateway.EnsureUpstreamAPIKeyLeaseForGroup(firstByteDeadline.PreparationContext(), candidate.Channel.ID, groupName, nil, candidate.RawRatio)
			} else {
				lease, err = d.Gateway.FixedUpstreamAPIKeyLeaseForGroup(candidate.Channel.ID, groupName)
			}
			if err != nil {
				timedOut := firstByteDeadline.TimedOut()
				firstByteDeadline.Close()
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, err) {
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while preparing upstream key", err)
					return true
				}
				if isGatewayClientCanceled(c.Request, err) && !imageRequestCommitted {
					if !finalizeClientCanceledAudit(nil, route, time.Now()) {
						return true
					}
					return true
				}
				if timedOut {
					msg := firstByteDeadline.TimeoutMessage(candidateFirstByteBudget)
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttempt(candidate, "response_timeout", http.StatusGatewayTimeout, msg))
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout, msg) {
						return true
					}
					continue candidateLoop
				}
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %v", candidate.Channel.Name, groupName, err))
				if gateway.IsSessionScopedCapacityError(err) {
					attempts = append(attempts, newGatewayRouteAttempt(candidate, "upstream_key_capacity", http.StatusTooManyRequests, err.Error()))
					continue candidateLoop
				}
				retryable := isRetryableGatewayPreparationError(c.Request.Context(), err)
				attempt := newGatewayRouteAttempt(candidate, "upstream_key", 0, err.Error())
				attempt.Retryable = retryable
				attempts = append(attempts, attempt)
				if retryable {
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailurePreparation, 0, err.Error()) {
						return true
					}
				}
				continue candidateLoop
			}
			upstreamKey := lease.Key
			directCandidate := candidate.Channel.CredentialMode == storage.CredentialModeDirect
			directCandidateIdentityRevision := candidate.Channel.UpstreamAccountIdentityRevision
			if lease.ChannelSnapshot != nil {
				candidate.Channel = *lease.ChannelSnapshot
				candidate.UpstreamAccountConcurrency = lease.UpstreamAccountConcurrency
			}
			if directCandidate {
				var current *storage.Channel
				var currentErr error
				if d.Channels == nil {
					currentErr = errors.New("channel store is unavailable")
				} else {
					current, currentErr = d.Channels.WithContext(firstByteDeadline.PreparationContext()).FindByID(candidate.Channel.ID)
				}
				if currentErr != nil || current == nil || current.UpstreamAccountIdentityRevision != directCandidateIdentityRevision ||
					!directRouteModelAllowed(candidate, app, model) {
					firstByteDeadline.Close()
					msg := "direct route no longer allows the requested model"
					if currentErr != nil {
						msg = "direct route configuration is unavailable"
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttempt(candidate, "model_not_listed", 0, msg))
					continue candidateLoop
				}
				candidate.Channel = *current
			}
			if lease.ChannelSnapshot != nil || directCandidate {
				route = gatewayRouteSnapshotAfterLeaseChannel(route, candidate)
				candidateMultiplierProvenanceInvalidated = candidateMultiplierProvenanceInvalidated || route.MultiplierProvenanceInvalidated
			}
			route.UpstreamKeyID = lease.UpstreamKeyID
			route.UpstreamKeyName = lease.UpstreamKeyName
			route.UpstreamKeyLocalID = lease.LocalKeyID
			route.UpstreamKeyGeneration = lease.KeyGeneration
			if !smartRouting {
				fixedRoute = route
			}
			target, err := resolveGatewayCandidateTarget(firstByteDeadline.PreparationContext(), d, candidate)
			if err != nil {
				timedOut := firstByteDeadline.TimedOut()
				firstByteDeadline.Close()
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, err) {
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while resolving target", err)
					return true
				}
				if isGatewayClientCanceled(c.Request, err) && !imageRequestCommitted {
					if !finalizeClientCanceledAudit(nil, route, time.Now()) {
						return true
					}
					return true
				}
				if timedOut {
					msg := firstByteDeadline.TimeoutMessage(candidateFirstByteBudget)
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttempt(candidate, "response_timeout", http.StatusGatewayTimeout, msg))
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout, msg) {
						return true
					}
					continue candidateLoop
				}
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %v", candidate.Channel.Name, groupName, err))
				retryable := isRetryableGatewayPreparationError(c.Request.Context(), err)
				attempt := newGatewayRouteAttempt(candidate, "target_resolution", 0, err.Error())
				attempt.Retryable = retryable
				attempts = append(attempts, attempt)
				if retryable {
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailurePreparation, 0, err.Error()) {
						return true
					}
				}
				continue candidateLoop
			}
			dispatchFence, fenceErr := gateway.UpstreamDispatchFenceForCandidate(candidate, lease, app, model)
			if fenceErr != nil {
				firstByteDeadline.Close()
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %v", candidate.Channel.Name, groupName, fenceErr))
				attempts = append(attempts, newGatewayRouteAttempt(candidate, "upstream_key", 0, fenceErr.Error()))
				continue candidateLoop
			}
			if smartRouting {
				probeContext := firstByteDeadline.PreparationContext()
				cancelProbe := func() {}
				if candidateFirstByteBudget > 0 {
					remainingPreparation := candidateFirstByteBudget - time.Since(candidateBudgetStartedAt)
					if remainingPreparation <= 0 {
						firstByteDeadline.cancel(errGatewayFirstByteTimeout)
					} else {
						probeContext, cancelProbe = context.WithTimeout(probeContext, remainingPreparation)
					}
				}
				known, allowed := gatewayCandidateAllowsApp(
					probeContext, d, candidate, lease, target, upstreamKey, app,
					gatewayCandidateModelFactGenerationArgs(candidate), dispatchFence,
				)
				probeTimedOut := probeContext.Err() != nil
				cancelProbe()
				if probeTimedOut {
					firstByteDeadline.cancel(errGatewayFirstByteTimeout)
				}
				if known && !allowed {
					firstByteDeadline.Close()
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: model family does not allow %s", candidate.Channel.Name, groupName, app))
					attempts = append(attempts, newGatewayRouteAttempt(candidate, "model_family", 0, fmt.Sprintf("model family does not allow %s", app)))
					continue candidateLoop
				}
			}
			if smartRouting && c.Request.URL.Path == "/v1/messages" {
				budget := gatewayKnownSafeInputBudget(d, candidate, model, body)
				if budget > 0 && shouldPreflightGatewayContext(body, budget) {
					// The count request is preparation work for this candidate. Derive
					// it from the candidate deadline so a slow tokenizer call cannot
					// escape this candidate's first-byte budget.
					countContext, cancelCount := context.WithTimeout(firstByteDeadline.PreparationContext(), gatewayContextCountTimeout)
					inputTokens, counted, exceeded, countErr := countGatewayCandidateInputTokens(
						c.Request.WithContext(countContext), target, upstreamKey, body,
						&gatewayTransportDispatchFence{service: d.Gateway, fence: dispatchFence},
						func(status int, raw []byte) {
							observeGatewayHTTPAuthorizationEvidence(countContext, d, candidate, lease, groupName, status, raw)
						},
					)
					cancelCount()
					if gatewayRequestDispatchOwnerLeaseLost(c.Request, countErr) {
						firstByteDeadline.Close()
						abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost during context preflight", countErr)
						return true
					}
					if countErr == nil && (exceeded || (counted && inputTokens > budget)) {
						firstByteDeadline.Close()
						errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: request input %d exceeds learned safe budget %d", candidate.Channel.Name, groupName, inputTokens, budget))
						attempts = append(attempts, newGatewayRouteAttempt(candidate, "context_limit", http.StatusBadRequest, fmt.Sprintf("request input %d exceeds learned safe budget %d", inputTokens, budget)))
						contextLimitAttempts++
						continue candidateLoop
					}
				}
			}
			// Account concurrency is admitted immediately before the physical data
			// plane request. Smart routing must move on without waiting when a
			// shared upstream account is full; fixed routing keeps its explicit
			// target and only observes the in-flight slot.
			accountClass := gateway.AccountConcurrencySmartData
			if !smartRouting {
				accountClass = gateway.AccountConcurrencyFixedData
			}
			accountLease, accountDecision := d.Gateway.TryAcquireCandidateAccountConcurrency(candidate, accountClass)
			if accountLease != nil {
				accountLease.BindContext(c.Request.Context())
			}
			if !accountDecision.Admitted || (smartRouting && accountLease == nil) {
				if smartRouting {
					firstByteDeadline.Close()
					summary := "upstream account concurrency admission deferred"
					if strings.TrimSpace(accountDecision.Reason) != "" {
						summary += ": " + accountDecision.Reason
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, summary))
					attempts = append(attempts, newGatewayRouteAttempt(candidate, "upstream_account_concurrency", 0, summary))
					continue candidateLoop
				}
				// A fixed target is never blocked by the local governor. The branch
				// is defensive for an unavailable governor; continue transparently.
				accountLease = nil
			}
			var dispatchAudit *gatewayDispatchAuditState
			var rolloverPrevious *gatewayDispatchAuditState
			var rolloverPreviousFinishedAt time.Time
			resp, err := forwardGatewayRequestWithDispatchFence(
				c.Request.WithContext(firstByteDeadline.Context()), target, upstreamKey, body,
				&gatewayTransportDispatchFence{service: d.Gateway, fence: dispatchFence, beforeDispatch: func(context.Context) error {
					if candidateFirstByteBudget > 0 && !candidateFirstByteStartedAt.IsZero() {
						attemptTimeout = time.Until(candidateFirstByteStartedAt.Add(candidateFirstByteBudget))
						if attemptTimeout <= 0 {
							firstByteDeadline.cancel(errGatewayFirstByteTimeout)
							return errGatewayFirstByteTimeout
						}
					}
					rolloverPrevious = pendingFinalAudit
					rolloverPreviousFinishedAt = pendingFinalAuditAt
					if rolloverPrevious != nil && rolloverPreviousFinishedAt.IsZero() {
						rolloverPreviousFinishedAt = time.Now()
					}
					var beginErr error
					dispatchAudit, beginErr = beginGatewayRequestDispatchAfter(
						d, c, rolloverPrevious, rolloverPreviousFinishedAt, downstreamKeyID, decision, route, model, reasoningEffort,
						mapping, start, attempts, errorsSeen, telemetry, candidateFirstByteBudget, time.Now(),
					)
					if beginErr != nil {
						return beginErr
					}
					dispatchAudits = append(dispatchAudits, dispatchAudit)
					if dispatchAudit == nil || dispatchAudit.id == 0 {
						return fmt.Errorf("%w: persisted dispatch audit is unavailable", errGatewayRequestDispatchBegin)
					}
					currentAttemptAudit = dispatchAudit
					markedAt := time.Now().UTC()
					if imageRequest {
						if markErr := d.Gateway.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(dispatchAudit.id, markedAt); markErr != nil {
							// The service has already attempted the only handoff that may
							// classify this pre-RoundTripper ambiguity as known-unsent. A
							// generic defer must neither overwrite a successful handoff's
							// terminal kind nor erase an armed marker after a failed one.
							if errors.Is(markErr, storage.ErrGatewayRequestDispatchCommitUnknown) {
								dispatchRecoveryRequired[dispatchAudit] = struct{}{}
							}
							return fmt.Errorf("%w: %w", errGatewayRequestPhysicalDispatchMark, markErr)
						}
						dispatchAudit.billingCommitted = true
						imageRequestCommitted = true
						committedImageRoute = route
						committedImageDispatch = dispatchAudit
						firstByteDeadline.MarkFirstByte()
					} else if markErr := d.Gateway.MarkRequestDispatchPhysicallyDispatched(dispatchAudit.id, markedAt); markErr != nil {
						return fmt.Errorf("%w: %w", errGatewayRequestPhysicalDispatchMark, markErr)
					}
					dispatchAudit.physicallyDispatched = true
					telemetry.markPhysicalDispatch()
					if candidateFirstByteStartedAt.IsZero() {
						candidateFirstByteStartedAt = time.Now()
						attemptTimeout = candidateFirstByteBudget
					} else if candidateFirstByteBudget > 0 {
						attemptTimeout = time.Until(candidateFirstByteStartedAt.Add(candidateFirstByteBudget))
					}
					if candidateFirstByteBudget > 0 {
						firstByteDeadline.Start(attemptTimeout)
					}
					if waitingProbe {
						firstByteDeadline.preparationCancel()
						firstByteDeadline.preparationCancel = func() {}
						startGatewayWaitingProbe(d, firstByteDeadline, candidate, target, lease, dispatchFence, c.Request.URL.Path, model)
					}
					if imageRequest {
						firstByteDeadline.MarkFirstByte()
					}
					return nil
				}}, func() error {
					if dispatchAudit == nil || dispatchAudit.id == 0 {
						return errors.New("persisted dispatch audit is unavailable")
					}
					return d.Gateway.ConfirmRequestDispatchTransportInvocation(dispatchAudit.id, time.Now().UTC())
				},
			)
			if accountLease != nil {
				accountLease.BindHTTPResponse(resp, err)
			}
			if errors.Is(err, errGatewayRequestTransportConfirm) {
				finishedAt := time.Now()
				confirmationLog := gatewayDispatchConfirmationFailureLog(dispatchAudit, err, candidate.Channel.Type, upstreamKey, start, finishedAt)
				if rolloverPrevious != nil && dispatchAudit != nil {
					previousFinishedAt := rolloverPreviousFinishedAt
					if previousFinishedAt.IsZero() {
						previousFinishedAt = finishedAt
					}
					previousLog := gatewayDispatchRecoveryFallbackLog(rolloverPrevious, gatewayDispatchTerminalAuditFailure, start, previousFinishedAt)
					handoffErr := scheduleGatewayLinkedDispatchRecovery(
						d, rolloverPrevious, previousLog, previousFinishedAt,
						dispatchAudit, confirmationLog, finishedAt,
						gatewayDispatchTerminalAuditFailure, start, false,
					)
					if handoffErr != nil && d.Log != nil {
						d.Log.Error("atomically handoff linked dispatches after transport confirmation failure", "previous_dispatch_id", rolloverPrevious.id, "next_dispatch_id", dispatchAudit.id, "err", handoffErr)
					}
					dispatchRecoveryRequired[rolloverPrevious] = struct{}{}
					dispatchRecoveryRequired[dispatchAudit] = struct{}{}
					if handoffErr == nil {
						d.Gateway.RestartFailedRequestDispatchOwner()
					}
				} else {
					dispatchRecoveryRequired[dispatchAudit] = struct{}{}
					if handoffErr := scheduleGatewayDispatchRecovery(d, dispatchAudit, confirmationLog, gatewayDispatchTerminalAuditFailure, start, finishedAt, err); handoffErr == nil {
						d.Gateway.RestartFailedRequestDispatchOwner()
					}
				}
				abortGatewayRequestDispatch(c, d, "confirm completed upstream transport invocation", err)
				return true
			}
			if rolloverPrevious != nil && dispatchAudit != nil && dispatchAudit.physicallyDispatched {
				previousFinishedAt := rolloverPreviousFinishedAt
				if previousFinishedAt.IsZero() {
					previousFinishedAt = time.Now()
				}
				rolloverClosedAt := time.Now()
				previousEvidence := gatewayDispatchIntermediateRolloverEvidenceLog(rolloverPrevious, previousFinishedAt)
				if closeErr := d.Gateway.ClosePreviousRequestDispatchAfterNextPhysical(
					rolloverPrevious.id,
					dispatchAudit.id,
					previousEvidence,
					gatewayDispatchTerminalFailedBeforeAcceptance,
					rolloverPrevious.recoveryFacts(),
					start,
					previousFinishedAt,
				); closeErr != nil {
					if resp != nil && resp.Body != nil {
						_ = resp.Body.Close()
					}
					firstByteDeadline.Close()
					if gatewayRequestDispatchOwnerLeaseLost(c.Request, closeErr) {
						abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while closing proven rollover", closeErr)
						return true
					}
					dispatchRecoveryRequired[rolloverPrevious] = struct{}{}
					dispatchRecoveryRequired[dispatchAudit] = struct{}{}
					if !errors.Is(closeErr, storage.ErrGatewayRequestDispatchCommitUnknown) {
						handoffErr := scheduleGatewayLinkedDispatchRecovery(
							d, rolloverPrevious, previousEvidence, previousFinishedAt,
							dispatchAudit, nil, rolloverClosedAt,
							gatewayDispatchTerminalAuditFailure, start, false,
						)
						if handoffErr != nil {
							if d.Log != nil {
								d.Log.Error("atomically handoff linked dispatches after rollover close failure", "previous_dispatch_id", rolloverPrevious.id, "next_dispatch_id", dispatchAudit.id, "err", handoffErr)
							}
						} else {
							d.Gateway.RestartFailedRequestDispatchOwner()
						}
					}
					abortGatewayRequestDispatch(c, d, "close prior request dispatch after physical rollover", closeErr)
					return true
				}
				rolloverPrevious.closed = true
				if pendingFinalAudit == rolloverPrevious {
					pendingFinalAudit = nil
					pendingFinalAuditAt = time.Time{}
					pendingFinalUsage = gatewayUsageMetrics{}
				}
			}
			if err != nil {
				timedOut := firstByteDeadline.TimedOut()
				firstByteDeadline.Close()
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, err) {
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost during upstream send", err)
					return true
				}
				// Owner cancellation is forwarded to the request context through an
				// asynchronous context.AfterFunc callback. Classify the durable commit
				// ambiguity directly so it cannot race that propagation and fall through
				// to an ordinary 502 terminal response.
				if errors.Is(err, storage.ErrGatewayRequestDispatchCommitUnknown) {
					abortGatewayRequestDispatch(c, d, "request dispatch commit outcome is unknown", err)
					return true
				}
				if errors.Is(err, errGatewayRequestDispatchBegin) || errors.Is(err, errGatewayRequestPhysicalDispatchMark) {
					message := "request was not sent because durable dispatch audit failed"
					if errors.Is(err, errGatewayRequestPhysicalDispatchMark) {
						message = "request was not sent because transport-entry dispatch evidence could not be persisted"
					}
					failure := gatewayDispatchAuditFailure(message, err)
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, failure))
					attempts = append(attempts, newGatewayRouteAttempt(candidate, "dispatch_audit", 0, failure))
					continue candidateLoop
				}
				if isGatewayClientCanceled(c.Request, err) && !imageRequestCommitted {
					if !finalizeClientCanceledAudit(dispatchAudit, route, time.Now()) {
						return true
					}
					return true
				}
				if imageRequestCommitted {
					if timedOut {
						msg := firstByteDeadline.TimeoutMessage(candidateFirstByteBudget)
						return failCommittedImageRequest(route, newGatewayRouteAttempt(candidate, "response_timeout", http.StatusGatewayTimeout, msg), msg, gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout)
					}
					return failCommittedImageRequest(route, newGatewayRouteAttempt(candidate, "transport", 0, sanitizeGatewayProbeError([]byte(err.Error()), upstreamKey)), err.Error(), gateway.RoutingFailureTransport, 0)
				}
				if timedOut {
					msg := firstByteDeadline.TimeoutMessage(candidateFirstByteBudget)
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "response_timeout", http.StatusGatewayTimeout, msg))
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout, msg) {
						return true
					}
					continue candidateLoop
				}
				if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
					return true
				}
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %v", candidate.Channel.Name, groupName, err))
				retryable := isRetryableGatewayPreparationError(c.Request.Context(), err)
				attempt := newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "transport", 0, sanitizeGatewayProbeError([]byte(err.Error()), upstreamKey))
				attempt.Retryable = retryable
				attempts = append(attempts, attempt)
				if retryable {
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureTransport, 0, err.Error()) {
						return true
					}
				}
				continue candidateLoop
			}
			providerRequestID, providerRequestIDErr := gatewayUpstreamRequestID(resp.Header, candidate.Channel.Type)
			if providerRequestIDErr != nil {
				if resp.Body != nil {
					_ = resp.Body.Close()
				}
				firstByteDeadline.Close()
				dispatchRecoveryRequired[dispatchAudit] = struct{}{}
				scheduleGatewayDispatchRecovery(d, dispatchAudit, nil, gatewayDispatchTerminalAuditFailure, start, time.Now(), providerRequestIDErr)
				abortGatewayRequestDispatch(c, d, "validate authoritative upstream request identity", providerRequestIDErr)
				return true
			}
			if imageRequest {
				committedImageUpstreamRequestID = providerRequestID
			}
			if recordErr := dispatchAudit.recordResponse(providerRequestID, time.Now()); recordErr != nil {
				if resp.Body != nil {
					_ = resp.Body.Close()
				}
				firstByteDeadline.Close()
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, recordErr) {
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while persisting upstream response identity", recordErr)
					return true
				}
				failure := gatewayDispatchAuditFailure("persist upstream response identity", recordErr)
				if imageRequest {
					return failCommittedImageRequest(route, newGatewayRouteAttempt(candidate, "dispatch_audit", resp.StatusCode, failure), failure, "", resp.StatusCode)
				}
				finishedAt := time.Now()
				terminalLog := gatewayDispatchRecoveryFallbackLog(dispatchAudit, gatewayDispatchTerminalAuditFailure, start, finishedAt)
				if terminalLog == nil {
					dispatchRecoveryRequired[dispatchAudit] = struct{}{}
					scheduleGatewayDispatchRecovery(d, dispatchAudit, nil, gatewayDispatchTerminalAuditFailure, start, finishedAt, recordErr)
					abortGatewayRequestDispatch(c, d, "build terminal fallback after upstream response identity failure", recordErr)
					return true
				}
				if _, finalizeErr := dispatchAudit.finalize(d.Gateway, terminalLog, gatewayDispatchTerminalAuditFailure, start, finishedAt); finalizeErr != nil {
					scheduleGatewayDispatchRecovery(d, dispatchAudit, terminalLog, gatewayDispatchTerminalAuditFailure, start, finishedAt, finalizeErr)
					abortGatewayRequestDispatch(c, d, "finalize terminal fallback after upstream response identity failure", finalizeErr)
					return true
				}
				abortGatewayRequestDispatch(c, d, "persist upstream response identity", recordErr)
				return true
			}
			// LatencyMs remains end-to-end, while FirstByteMs belongs to the
			// candidate that ultimately served the request. Failed candidates must
			// not poison latency scoring or the manual-cooldown action for the
			// healthy fallback route.
			firstByte := gatewayFirstByteTracker{startedAt: candidateFirstByteStartedAt}
			upstreamAccepted := imageRequestAccepted
			billingCommitted := imageRequestCommitted
			preludeAccepted := false
			bufferResponse := shouldParseGatewayUsage(resp) || directImageRequest
			if imageRequest && (resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices) {
				raw := []byte(nil)
				var readErr error
				if resp.Body != nil {
					raw, readErr = readGatewayUpstreamResponseLimited(resp.Body, responseSpool.errorResponseBytes)
					_ = resp.Body.Close()
					if readErr == nil {
						committedImageMetrics = parseGatewayUsageMetricsForPath(raw, c.Request.URL.Path, upstreamKey)
					}
				}
				observeGatewayHTTPAuthorizationEvidence(c.Request.Context(), d, candidate, lease, groupName, resp.StatusCode, raw)
				// Image requests are committed before their response status is
				// known, so account-capacity evidence must still feed the local
				// governor before the terminal failure is recorded. The helper
				// deliberately ignores bare 429s and unrelated status bodies.
				observeGatewayAccountConcurrencyPressure(accountLease, resp.StatusCode, string(raw))
				firstByteDeadline.Close()
				failure := fmt.Sprintf("gateway status %d", resp.StatusCode)
				reason := gatewayStatusAttemptReason(resp.StatusCode)
				if msg := sanitizeGatewayProbeError(raw, upstreamKey); msg != "" {
					failure += ": " + msg
				} else if readErr != nil {
					failure += fmt.Sprintf(": read response body: %v", readErr)
				}
				routingFailureKind := ""
				if isTransientGatewayResponse(resp.StatusCode, raw) {
					routingFailureKind = gateway.RoutingFailureHTTPStatus
				}
				return failCommittedImageRequest(route, newGatewayRouteAttempt(candidate, reason, resp.StatusCode, failure), failure, routingFailureKind, resp.StatusCode)
			}
			if imageRequest && resp.Body == nil {
				firstByteDeadline.Close()
				return failCommittedImageRequest(
					route,
					newGatewayRouteAttempt(candidate, "response_read", resp.StatusCode, "upstream response body is empty"),
					"upstream response body is empty",
					gateway.RoutingFailureTransport,
					resp.StatusCode,
				)
			}
			if shouldFallbackGatewayStatus(resp.StatusCode) {
				raw, readErr := readGatewayUpstreamResponseLimited(resp.Body, responseSpool.errorResponseBytes)
				transientStatus := isTransientGatewayResponse(resp.StatusCode, raw)
				_ = resp.Body.Close()
				if readErr == nil {
					currentAttemptUsage = parseGatewayUsageMetricsForPath(raw, c.Request.URL.Path, upstreamKey)
				}
				timedOut := firstByteDeadline.TimedOut()
				firstByteDeadline.Close()
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, readErr) {
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while reading fallback status", readErr)
					return true
				}
				if readErr != nil && isGatewayClientCanceled(c.Request, readErr) {
					if !finalizeClientCanceledAudit(dispatchAudit, route, time.Now()) {
						return true
					}
					observeGatewayHTTPAuthorizationEvidence(c.Request.Context(), d, candidate, lease, groupName, resp.StatusCode, raw)
					return true
				}
				if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
					return true
				}
				observeGatewayHTTPAuthorizationEvidence(c.Request.Context(), d, candidate, lease, groupName, resp.StatusCode, raw)
				if readErr != nil {
					if !transientStatus {
						errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: status %d read error response: %v", candidate.Channel.Name, groupName, resp.StatusCode, readErr))
						attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, gatewayStatusAttemptReason(resp.StatusCode), resp.StatusCode, fmt.Sprintf("status %d read error response: %v", resp.StatusCode, readErr)))
						continue candidateLoop
					}
					if timedOut {
						msg := firstByteDeadline.TimeoutMessage(candidateFirstByteBudget)
						errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
						attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "response_timeout", http.StatusGatewayTimeout, msg))
						if retryCandidate() {
							continue
						}
						if !confirmCandidateFailure(gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout, msg) {
							return true
						}
						continue candidateLoop
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: read error response: %v", candidate.Channel.Name, groupName, readErr))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "response_read", resp.StatusCode, fmt.Sprintf("read error response: %v", readErr)))
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureTransport, resp.StatusCode, fmt.Sprintf("read error response: %v", readErr)) {
						return true
					}
					continue candidateLoop
				}
				msg := sanitizeGatewayProbeError(raw, upstreamKey)
				observeGatewayAccountConcurrencyPressure(accountLease, resp.StatusCode, string(raw))
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: status %d %s", candidate.Channel.Name, groupName, resp.StatusCode, msg))
				attempt := newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, gatewayStatusAttemptReason(resp.StatusCode), resp.StatusCode, msg)
				attempt.Retryable = transientStatus
				attempts = append(attempts, attempt)
				if transientStatus {
					failure := fmt.Sprintf("gateway status %d", resp.StatusCode)
					if msg != "" {
						failure += ": " + msg
					}
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureHTTPStatus, resp.StatusCode, failure) {
						return true
					}
				}
				continue candidateLoop
			}
			if mayBeModelUnsupportedStatus(resp.StatusCode) || resp.StatusCode == http.StatusRequestEntityTooLarge {
				raw, readErr := readGatewayUpstreamResponseLimited(resp.Body, responseSpool.errorResponseBytes)
				_ = resp.Body.Close()
				if readErr == nil {
					currentAttemptUsage = parseGatewayUsageMetricsForPath(raw, c.Request.URL.Path, upstreamKey)
				}
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, readErr) {
					firstByteDeadline.Close()
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while reading deterministic status", readErr)
					return true
				}
				if imageRequest {
					committedImageMetrics = parseGatewayUsageMetricsForPath(raw, c.Request.URL.Path, upstreamKey)
				}
				if readErr != nil {
					firstByteDeadline.Close()
					if isGatewayClientCanceled(c.Request, readErr) {
						if !finalizeClientCanceledAudit(dispatchAudit, route, time.Now()) {
							return true
						}
						return true
					}
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					// A 400/404/413 status is a permanent request/model outcome.
					// A broken body must not turn it into a replayable transport
					// failure or contaminate the channel's routing health.
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: status %d read error response: %v", candidate.Channel.Name, groupName, resp.StatusCode, readErr))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, gatewayStatusAttemptReason(resp.StatusCode), resp.StatusCode, fmt.Sprintf("status %d read error response: %v", resp.StatusCode, readErr)))
					continue candidateLoop
				}
				msg := sanitizeGatewayProbeError(raw, upstreamKey)
				if isGatewayContextLimitError(resp.StatusCode, raw) {
					firstByteDeadline.Close()
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					observeGatewayContextLimit(d, candidate, model, body, raw)
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: upstream context limit exceeded: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "context_limit", resp.StatusCode, msg))
					contextLimitAttempts++
					continue candidateLoop
				}
				if isGatewayModelUnsupported(msg) {
					firstByteDeadline.Close()
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: status %d %s", candidate.Channel.Name, groupName, resp.StatusCode, msg))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "model_unsupported", resp.StatusCode, msg))
					markGatewayCandidateModelCapability(d.Gateway.Store, candidate, groupName, model, false, fmt.Sprintf("status %d: upstream reported model unsupported", resp.StatusCode))
					continue candidateLoop
				}
				resp.Body = io.NopCloser(bytes.NewReader(raw))
			}
			if (!bufferResponse || streamingRequest) && isNonTransientGatewayResponseStatus(resp.StatusCode) {
				raw, readErr := readGatewayUpstreamResponseLimited(resp.Body, responseSpool.errorResponseBytes)
				_ = resp.Body.Close()
				if readErr == nil {
					currentAttemptUsage = parseGatewayUsageMetricsForPath(raw, c.Request.URL.Path, upstreamKey)
				}
				firstByteDeadline.Close()
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, readErr) {
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while reading non-transient status", readErr)
					return true
				}
				if readErr != nil && isGatewayClientCanceled(c.Request, readErr) {
					if !finalizeClientCanceledAudit(dispatchAudit, route, time.Now()) {
						return true
					}
					return true
				}
				if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
					return true
				}
				failure := fmt.Sprintf("status %d", resp.StatusCode)
				if msg := sanitizeGatewayProbeError(raw, upstreamKey); msg != "" {
					failure += " " + msg
				} else if readErr != nil {
					failure += fmt.Sprintf(" read error response: %v", readErr)
				}
				errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, failure))
				attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, gatewayStatusAttemptReason(resp.StatusCode), resp.StatusCode, failure))
				continue candidateLoop
			}
			var nonStreamingFacts gatewayBufferedResponseFacts
			var nonStreamingPrefix []byte
			var nonStreamingSize int64
			bufferReadFailure := ""
			bufferReadClientCanceled := false
			if bufferResponse {
				onBufferedFirstByte := func() {
					firstByte.record()
					if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
						firstByteDeadline.MarkFirstByte()
					}
				}
				buffered, readErr := bufferGatewayUpstreamResponse(firstByteDeadline.Context(), responseSpool, resp.Body, c.Request.URL.Path, onBufferedFirstByte, configuredFirstByteBudget)
				_ = resp.Body.Close()
				if readErr == nil {
					buffered.Facts.Usage = sanitizeGatewayUsageModel(buffered.Facts.Usage, upstreamKey)
					currentAttemptUsage = buffered.Facts.Usage
				}
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, readErr) {
					firstByteDeadline.Close()
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while buffering upstream response", readErr)
					return true
				}
				// A partial non-streaming response proves that its body has started, so
				// replaying this request could duplicate billed work. It does not,
				// however, prove protocol-level upstream acceptance. Keep that
				// non-replayable boundary separate from UpstreamAccepted: only a
				// complete parsable response or trusted SSE lifecycle evidence may set
				// the latter.
				nonStreamingBodyStarted := readErr != nil && buffered.FirstBodyObserved && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices && !imageRequest
				localResponseSpoolFailure := readErr != nil && gatewayResponseSpoolLocalFailure(readErr)
				if !imageRequest && isGatewayClientCanceled(c.Request, readErr) {
					firstByteDeadline.Close()
					if !finalizeClientCanceledAudit(dispatchAudit, route, time.Now()) {
						return true
					}
					return true
				} else if localResponseSpoolFailure && !imageRequest {
					// A local spool may fail after bytes have arrived (for example while
					// extending its reservation). It remains a local resource failure,
					// not a response-body or upstream-timeout signal.
					firstByteDeadline.Close()
					dispatchAudit.recordTelemetryPhysicalFailure(telemetry)
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					failure := "local response spool unavailable after physical dispatch"
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, failure))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "response_spool_local", resp.StatusCode, failure))
					break candidateLoop
				} else if nonStreamingBodyStarted {
					firstByteDeadline.MarkFirstByte()
					firstByteDeadline.Close()
					bufferReadClientCanceled = isGatewayClientCanceled(c.Request, readErr)
					if bufferReadClientCanceled {
						bufferReadFailure = "客户端取消"
					} else if bufferReadFailure == "" {
						bufferReadFailure = sanitizeGatewayProbeError([]byte("read response body after upstream body started: "+readErr.Error()), upstreamKey)
						dispatchAudit.recordTelemetryPhysicalFailure(telemetry)
						attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "response_body_started", resp.StatusCode, bufferReadFailure))
					}
				} else if readErr != nil {
					timedOut := firstByteDeadline.TimedOut()
					firstByteDeadline.Close()
					if imageRequest {
						reason := "response_read"
						routingFailureKind := gateway.RoutingFailureTransport
						switch {
						case errors.Is(readErr, ErrGatewayUpstreamResponseTooLarge):
							reason = "response_too_large"
						case isGatewayClientCanceled(c.Request, readErr):
							routingFailureKind = ""
						case errors.Is(readErr, errGatewayResponseSpoolCapacity), errors.Is(readErr, errGatewayResponseSpoolQueueFull):
							reason = "response_spool_capacity"
							routingFailureKind = ""
						case gatewayResponseSpoolLocalFailure(readErr):
							reason = "response_spool_local"
							routingFailureKind = ""
						}
						failure := sanitizeGatewayProbeError([]byte("buffer upstream response: "+readErr.Error()), upstreamKey)
						return failCommittedImageRequest(route, newGatewayRouteAttempt(candidate, reason, resp.StatusCode, failure), failure, routingFailureKind, resp.StatusCode)
					}
					if isGatewayClientCanceled(c.Request, readErr) && !imageRequest {
						if !finalizeClientCanceledAudit(dispatchAudit, route, time.Now()) {
							return true
						}
						return true
					}
					if !imageRequest {
						if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
							return true
						}
					}
					if isNonTransientGatewayClientErrorStatus(resp.StatusCode) {
						// Once the upstream has supplied a deterministic client-error
						// status, a truncated body cannot turn it into a replayable
						// transport failure or contaminate route health.
						errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: status %d read error response: %v", candidate.Channel.Name, groupName, resp.StatusCode, readErr))
						attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, gatewayStatusAttemptReason(resp.StatusCode), resp.StatusCode, fmt.Sprintf("status %d read error response: %v", resp.StatusCode, readErr)))
						continue candidateLoop
					}
					if timedOut {
						msg := firstByteDeadline.TimeoutMessage(candidateFirstByteBudget)
						errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
						attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "response_timeout", http.StatusGatewayTimeout, msg))
						if retryCandidate() {
							continue
						}
						if !confirmCandidateFailure(gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout, msg) {
							return true
						}
						continue candidateLoop
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: read response body: %v", candidate.Channel.Name, groupName, readErr))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "response_read", 0, fmt.Sprintf("read response body: %v", readErr)))
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureTransport, 0, fmt.Sprintf("read response body: %v", readErr)) {
						return true
					}
					continue candidateLoop
				}
				if readErr == nil {
					nonStreamingFacts = buffered.Facts
					nonStreamingPrefix = buffered.Prefix
					nonStreamingSize = buffered.Size
					if imageRequest {
						committedImageMetrics = buffered.Facts.Usage
					}
				}
				classificationRaw := nonStreamingFacts.ClassificationJSON
				if !nonStreamingBodyStarted && isNonTransientGatewayResponseStatus(resp.StatusCode) {
					// The outer permanent status is authoritative. A provider may
					// include a transient-looking word or JSON error type in its
					// body, but that must not turn a deterministic request failure
					// into a replayable transport failure.
					firstByteDeadline.Close()
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					failure := fmt.Sprintf("status %d", resp.StatusCode)
					if msg := sanitizeGatewayProbeError(classificationRaw, upstreamKey); msg != "" {
						failure += " " + msg
					}
					if buffered.Spool != nil {
						_ = buffered.Spool.Close()
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, failure))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, gatewayStatusAttemptReason(resp.StatusCode), resp.StatusCode, failure))
					continue candidateLoop
				}
				if !nonStreamingBodyStarted && nonStreamingFacts.StructuredErrorFound && isGatewayContextLimitError(http.StatusBadRequest, classificationRaw) {
					msg := sanitizeGatewayProbeError(classificationRaw, upstreamKey)
					firstByteDeadline.Close()
					observeGatewayContextLimit(d, candidate, model, body, classificationRaw)
					if buffered.Spool != nil {
						_ = buffered.Spool.Close()
					}
					if imageRequest {
						return failCommittedImageRequest(
							route,
							newGatewayRouteAttempt(candidate, "context_limit", http.StatusBadRequest, msg),
							msg,
							"",
							http.StatusBadRequest,
						)
					}
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: upstream context limit exceeded: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "context_limit", http.StatusBadRequest, msg))
					contextLimitAttempts++
					continue candidateLoop
				}
				// Some upstreams return an HTTP 200 envelope for a structured error.
				// Treat it exactly like an SSE error before marking the route healthy;
				// otherwise an overloaded route is passed through as a success and can
				// never be confirmed, cooled, or retried.
				if !nonStreamingBodyStarted && resp.StatusCode == http.StatusOK {
					classification := classifyGatewayStreamFailure(classificationRaw)
					if classification.Found {
						failureStatus := classification.Status
						if failureStatus == 0 {
							failureStatus = resp.StatusCode
						}
						msg := gatewayStreamError(classificationRaw, upstreamKey)
						if msg == "" {
							msg = "upstream returned a structured error before useful output"
						}
						observeGatewayAccountConcurrencyPressure(accountLease, resp.StatusCode, string(classificationRaw))
						firstByteDeadline.Close()
						if buffered.Spool != nil {
							_ = buffered.Spool.Close()
						}
						if !imageRequest {
							if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
								return true
							}
						}
						observeGatewayStructuredAuthorizationEvidence(c.Request.Context(), d, candidate, lease, groupName, classificationRaw, classification)
						attemptReason := "stream_error"
						if gatewayStructuredFailureMarksModelUnsupported(classificationRaw) || isGatewayModelUnsupported(msg) {
							attemptReason = "model_unsupported"
							markGatewayCandidateModelCapability(d.Gateway.Store, candidate, groupName, model, false, "upstream reported model unsupported")
						}
						failureAttempt := newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, attemptReason, failureStatus, msg)
						failureAttempt.Retryable = classification.Disposition == gatewayStreamFailureTransient
						if imageRequest {
							routingFailureKind := ""
							if classification.Disposition == gatewayStreamFailureTransient {
								routingFailureKind = gateway.RoutingFailureTransport
							}
							return failCommittedImageRequest(route, failureAttempt, msg, routingFailureKind, failureStatus)
						}
						errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
						attempts = append(attempts, failureAttempt)
						if classification.Disposition == gatewayStreamFailureTransient {
							if retryCandidate() {
								continue
							}
							if !confirmCandidateFailure(gateway.RoutingFailureTransport, failureStatus, msg) {
								return true
							}
							continue candidateLoop
						}
						continue candidateLoop
					}
				}
				if !nonStreamingBodyStarted && !imageRequest && nonStreamingSize > 0 && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
					upstreamAccepted = true
					if markErr := dispatchAudit.markAccepted(gatewayStreamAcceptanceEvidence{EventType: "non_streaming_body"}, time.Now()); markErr != nil {
						if gatewayRequestDispatchOwnerLeaseLost(c.Request, markErr) {
							firstByteDeadline.Close()
							abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while persisting non-streaming acceptance", markErr)
							return true
						}
						bufferReadFailure = gatewayDispatchAuditFailure("persist non-streaming acceptance", markErr)
						nonStreamingBodyStarted = true
						firstByteDeadline.Close()
					} else {
						firstByteDeadline.MarkFirstByte()
					}
				}
				if nonStreamingBodyStarted {
					if buffered.Spool != nil {
						_ = buffered.Spool.Close()
					}
					if bufferReadClientCanceled {
						resp.Body = io.NopCloser(bytes.NewReader(nil))
					} else {
						resp.StatusCode = http.StatusBadGateway
						resp.Header.Del("Content-Length")
						resp.Header.Set("Content-Type", "application/json")
						resp.Body = io.NopCloser(bytes.NewReader(gatewayPublicBufferedFailureBody(c.Request.URL.Path)))
					}
				} else if buffered.Spool != nil {
					resp.Body = buffered.Spool
				}
			}
			forcedStreamFailure := ""
			if !bufferResponse {
				preparedBody, prelude, preludeHasUsefulOutput, accepted, prepareErr := prepareGatewayStreamPreludeWithAcceptance(resp.Body, c.Request.URL.Path, func(evidence gatewayStreamAcceptanceEvidence) error {
					upstreamAccepted = true
					if markErr := dispatchAudit.markAccepted(evidence, time.Now()); markErr != nil {
						return markErr
					}
					firstByteDeadline.MarkAccepted()
					return nil
				}, firstByteDeadline.MarkFirstByte)
				currentAttemptUsage = parseGatewayStreamUsageMetricsForPath(prelude, c.Request.URL.Path, upstreamKey)
				preludeAccepted = accepted
				resp.Body = preparedBody
				if gatewayRequestDispatchOwnerLeaseLost(c.Request, prepareErr) {
					_ = resp.Body.Close()
					firstByteDeadline.Close()
					abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while preparing stream", prepareErr)
					return true
				}
				var acceptanceWriteErr *gatewayStreamAcceptancePersistenceError
				if preludeAccepted && prepareErr != nil && (errors.As(prepareErr, &acceptanceWriteErr) || firstByteDeadline.WaitingProbeFailed()) {
					// The provider has already supplied trusted acceptance/useful
					// evidence. A durable mark failure is terminal for this route: do
					// not replay the request and do not expose the original frame.
					forcedStreamFailure = gatewayDispatchAuditFailure("persist upstream acceptance before downstream output", prepareErr)
					if firstByteDeadline.WaitingProbeFailed() {
						forcedStreamFailure = errGatewayWaitingProbeTimeout.Error()
					}
					_ = resp.Body.Close()
					// The prelude has already been consumed from the upstream body. Keep
					// its Responses sequence number on the replacement terminal frame so
					// clients never observe a sequence reset to zero after an audit fence.
					resp.Body = io.NopCloser(bytes.NewReader(gatewayPublicStreamErrorFrame(prelude, c.Request.URL.Path)))
					resp.Header.Del("Content-Length")
					resp.Header.Set("Content-Type", "text/event-stream")
					prelude = nil
					preludeHasUsefulOutput = false
					prepareErr = nil
					firstByteDeadline.Close()
				}
				if preludeAccepted && prepareErr != nil {
					if isGatewayClientCanceled(c.Request, prepareErr) && !imageRequest {
						_ = resp.Body.Close()
						firstByteDeadline.Close()
						finalizeClientCanceledAudit(dispatchAudit, route, time.Now())
						return true
					}
					// The replay reader retains consumed frames and the original read
					// error. Let the sanitizer preserve the response identity and apply
					// the accepted-stream terminal policy, without another dispatch.
					prepareErr = nil
				}
				if prepareErr != nil {
					_ = resp.Body.Close()
					timedOut := firstByteDeadline.TimedOut()
					firstByteDeadline.Close()
					if isGatewayClientCanceled(c.Request, prepareErr) && !imageRequest {
						if !finalizeClientCanceledAudit(dispatchAudit, route, time.Now()) {
							return true
						}
						return true
					}
					if !imageRequest {
						if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
							return true
						}
					}
					if imageRequest {
						if timedOut {
							msg := firstByteDeadline.TimeoutMessage(candidateFirstByteBudget)
							return failCommittedImageRequest(route, newGatewayRouteAttempt(candidate, "response_timeout", http.StatusGatewayTimeout, msg), msg, gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout)
						}
						return failCommittedImageRequest(route, newGatewayRouteAttempt(candidate, "stream_prelude", 0, sanitizeGatewayProbeError([]byte(prepareErr.Error()), upstreamKey)), prepareErr.Error(), gateway.RoutingFailureTransport, 0)
					}
					if timedOut {
						msg := firstByteDeadline.TimeoutMessage(candidateFirstByteBudget)
						errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
						attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "response_timeout", http.StatusGatewayTimeout, msg))
						if retryCandidate() {
							continue
						}
						if !confirmCandidateFailure(gateway.RoutingFailureResponseTimeout, http.StatusGatewayTimeout, msg) {
							return true
						}
						continue candidateLoop
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: stream prelude: %v", candidate.Channel.Name, groupName, prepareErr))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "stream_prelude", 0, sanitizeGatewayProbeError([]byte(prepareErr.Error()), upstreamKey)))
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureTransport, 0, prepareErr.Error()) {
						return true
					}
					continue candidateLoop
				}
				if !preludeAccepted && gatewayStreamFrameHasError(prelude) && isGatewayContextLimitError(http.StatusBadRequest, prelude) {
					_ = resp.Body.Close()
					firstByteDeadline.Close()
					msg := sanitizeGatewayProbeError(prelude, upstreamKey)
					observeGatewayContextLimit(d, candidate, model, body, prelude)
					if imageRequest {
						return failCommittedImageRequest(
							route,
							newGatewayRouteAttempt(candidate, "context_limit", http.StatusBadRequest, msg),
							msg,
							"",
							http.StatusBadRequest,
						)
					}
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: upstream context limit exceeded: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "context_limit", http.StatusBadRequest, msg))
					contextLimitAttempts++
					continue candidateLoop
				}
				capacityNotice := smartRouting && !preludeAccepted && resp.StatusCode == http.StatusOK && downstream.App == storage.GatewayAppCodex && c.Request.URL.Path == "/v1/responses" && (isGatewaySSECapacityNotification(prelude) || isGatewaySSEUpstreamInterruptionNotification(prelude))
				if capacityNotice {
					_ = resp.Body.Close()
					firstByteDeadline.Close()
					msg := sanitizeGatewayProbeError(prelude, upstreamKey)
					if msg == "" && capacityNotice {
						msg = "selected model capacity notice"
					}
					observeGatewayAccountConcurrencyPressure(accountLease, resp.StatusCode, string(prelude))
					if imageRequest {
						return failCommittedImageRequest(
							route,
							newGatewayRouteAttempt(candidate, "stream_account_limit", resp.StatusCode, msg),
							msg,
							"",
							resp.StatusCode,
						)
					}
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: upstream capacity %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "stream_account_limit", resp.StatusCode, msg))
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureTransport, resp.StatusCode, msg) {
						return true
					}
					continue candidateLoop
				}
				if !preludeAccepted && isGatewayModelUnsupported(string(prelude)) {
					_ = resp.Body.Close()
					firstByteDeadline.Close()
					msg := sanitizeGatewayProbeError(prelude, upstreamKey)
					markGatewayCandidateModelCapability(d.Gateway.Store, candidate, groupName, model, false, "stream reported model unsupported")
					if imageRequest {
						return failCommittedImageRequest(
							route,
							newGatewayRouteAttempt(candidate, "model_unsupported", http.StatusOK, msg),
							msg,
							"",
							http.StatusOK,
						)
					}
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: stream reported unsupported model %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "model_unsupported", http.StatusOK, msg))
					continue candidateLoop
				}
				if !preludeAccepted && gatewayStreamFrameHasError(prelude) {
					_ = resp.Body.Close()
					firstByteDeadline.Close()
					if !imageRequest {
						if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
							return true
						}
					}
					classification := classifyGatewayStreamFailure(prelude)
					msg := gatewayStreamError(prelude, upstreamKey)
					if msg == "" {
						msg = "upstream stream failed before useful output"
					}
					observeGatewayAccountConcurrencyPressure(accountLease, resp.StatusCode, string(prelude))
					failureAttempt := newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "stream_error", classification.Status, msg)
					failureAttempt.Retryable = classification.Disposition == gatewayStreamFailureTransient
					observeGatewayStructuredAuthorizationEvidence(c.Request.Context(), d, candidate, lease, groupName, prelude, classification)
					if imageRequest {
						routingFailureKind := ""
						if classification.Disposition == gatewayStreamFailureTransient {
							routingFailureKind = gateway.RoutingFailureTransport
						}
						return failCommittedImageRequest(route, failureAttempt, msg, routingFailureKind, classification.Status)
					}
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, failureAttempt)
					if classification.Disposition == gatewayStreamFailureTransient {
						if retryCandidate() {
							continue
						}
						if !confirmCandidateFailure(gateway.RoutingFailureTransport, classification.Status, msg) {
							return true
						}
						continue candidateLoop
					}
					continue candidateLoop
				}
				if !preludeAccepted && !preludeHasUsefulOutput && resp.StatusCode != http.StatusOK {
					_ = resp.Body.Close()
					firstByteDeadline.Close()
					if !closeFallbackAudit(dispatchAudit, gatewayDispatchTerminalFailedBeforeAcceptance, time.Now()) {
						return true
					}
					msg := "upstream stream ended before useful output"
					errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s", candidate.Channel.Name, groupName, msg))
					attempts = append(attempts, newGatewayRouteAttemptFromDispatch(dispatchAudit, candidate, "stream_empty", resp.StatusCode, msg))
					if retryCandidate() {
						continue
					}
					if !confirmCandidateFailure(gateway.RoutingFailureTransport, resp.StatusCode, msg) {
						return true
					}
					continue candidateLoop
				}
				if preludeHasUsefulOutput {
					firstByte.record()
					upstreamAccepted = true
				}
				if preludeHasUsefulOutput {
					firstByteDeadline.MarkFirstByte()
				}
			}

			metrics := currentAttemptUsage
			streamedRaw := &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}
			var streamSanitizer *gatewaySSEStreamSanitizer
			if bufferResponse {
				metrics = nonStreamingFacts.Usage
			} else {
				streamSanitizer = newGatewaySSEStreamSanitizer(resp.Body, streamedRaw, upstreamKey, c.Request.URL.Path, func() {
					firstByte.record()
					upstreamAccepted = true
					firstByteDeadline.MarkFirstByte()
				})
				streamSanitizer.accepted = preludeAccepted
				persistStreamAcceptance := func(evidence gatewayStreamAcceptanceEvidence) error {
					upstreamAccepted = true
					if markErr := dispatchAudit.markAccepted(evidence, time.Now()); markErr != nil {
						return markErr
					}
					firstByteDeadline.MarkAccepted()
					return nil
				}
				streamSanitizer.onAcceptedEvidence = persistStreamAcceptance
				streamSanitizer.onUsefulEvidence = func(evidence gatewayStreamAcceptanceEvidence) error {
					firstByteDeadline.MarkFirstByte()
					return persistStreamAcceptance(evidence)
				}
				streamSanitizer.waitingProbeFailed = firstByteDeadline.WaitingProbeFailed
				streamSanitizer.retryPromptEnabled = forcedStreamFailure == "" && !imageRequest && downstream.App == storage.GatewayAppCodex && c.Request.URL.Path == "/v1/responses"
				if forcedStreamFailure != "" {
					streamSanitizer.errorText = forcedStreamFailure
				}
				streamSanitizer.detectCapacityNotice = smartRouting && downstream.App == storage.GatewayAppCodex && c.Request.URL.Path == "/v1/responses"
				streamSanitizer.detectInterruption = downstream.App == storage.GatewayAppCodex && c.Request.URL.Path == "/v1/responses"
				streamSanitizer.clientCanceled = func() bool {
					cause := context.Cause(c.Request.Context())
					return isGatewayClientCanceled(c.Request, cause)
				}
				streamSanitizer.ownerLeaseLost = func() bool {
					return gatewayRequestDispatchOwnerLeaseLost(c.Request, context.Cause(c.Request.Context()))
				}
				resp.Body = streamSanitizer
			}
			copyGatewayHeaders(c.Writer.Header(), resp.Header)
			c.Status(resp.StatusCode)
			_, copyErr := copyGatewayBody(c.Writer, resp.Body)
			_ = resp.Body.Close()
			firstByteDeadline.Close()
			// Owner loss must not short-circuit the exact terminal outcome. The tail
			// buffer can already hold the upstream usage block, so the tail usage is
			// parsed and the complete terminal log is built first; only then is that
			// exact snapshot handed to the durable owner-loss handoff below.
			// Returning here would leave the deferred cleanup with a fallback log
			// that has no token usage and no cost basis.
			ownerLeaseLost := gatewayRequestDispatchOwnerLeaseLost(c.Request, copyErr) ||
				(streamSanitizer != nil && streamSanitizer.OwnerLeaseLost())
			errText := bufferReadFailure
			streamWarning := ""
			clientCanceled := bufferReadClientCanceled
			if streamSanitizer != nil {
				if streamFailure := streamSanitizer.ErrorText(); streamFailure != "" {
					errText = streamFailure
				}
				streamWarning = streamSanitizer.WarningText()
				evidence := streamSanitizer.FailureEvidence()
				// A fenced owner keeps the exact terminal snapshot but must not push
				// shared channel/key/model state through a database this process has
				// already been told it can no longer own.
				if !ownerLeaseLost {
					if evidence.AccountBalanceInsufficient {
						markGatewayAccountBalanceInsufficient(d, candidate, time.Now().UTC())
					} else if evidence.InvalidatesKey {
						invalidateGatewayCandidateUpstreamKey(c.Request.Context(), d, candidate, lease, groupName)
					}
					if evidence.ModelUnsupported && model != "" {
						markGatewayCandidateModelCapability(d.Gateway.Store, candidate, groupName, model, false, "upstream reported model unsupported after output started")
					}
				}
			} else if gatewayFrameHasVisibleThinkingMarkup(nonStreamingPrefix) {
				streamWarning = "检测到响应正文包含 <thinking> 思考标签"
			}
			if errText == "" {
				if streamFailure := gatewayStreamError(streamedRaw.Bytes(), upstreamKey); streamFailure != "" {
					errText = streamFailure
				}
			}
			if errText == "" && copyErr != nil {
				if isGatewayClientCanceled(c.Request, copyErr) {
					clientCanceled = true
					errText = "客户端取消"
				} else {
					errText = sanitizeGatewayProbeError([]byte(copyErr.Error()), "")
				}
			}
			if streamedRaw.Len() > 0 {
				metrics = mergeGatewayUsageMetrics(metrics, parseGatewayStreamUsageMetricsForPath(streamedRaw.Bytes(), c.Request.URL.Path, upstreamKey))
			}
			if streamSanitizer != nil && streamSanitizer.responseModel != "" {
				metrics.ResponseModel = streamSanitizer.responseModel
			}
			metrics = estimateGatewayCost(metrics, candidate.RawRatio, candidate.RawCompletionRatio)
			rawCost := metrics.Cost
			if metrics.CostObserved {
				if actualCost, conversionErr := costing.Actual(rawCost, candidate.FundingFactor); conversionErr == nil {
					metrics.Cost = actualCost
				}
			}
			finishedAt := time.Now()
			inferenceSucceeded := !clientCanceled && errText == "" && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices &&
				((bufferResponse && nonStreamingSize > 0) || (streamSanitizer != nil && streamSanitizer.useful))
			if inferenceSucceeded {
				telemetry.setRealFallback(gatewayRouteHasRealFallback(decision, attempts, candidate))
			} else {
				telemetry.setRealFallback(false)
			}
			if ownerLeaseLost {
				// An owner-fenced stream says nothing about upstream account
				// capacity, so it must not move the shared pressure signal.
			} else if inferenceSucceeded {
				observeGatewayAccountConcurrencySuccess(accountLease, true)
			} else {
				observeGatewayAccountConcurrencyPressure(accountLease, resp.StatusCode, errText+"\n"+string(streamedRaw.Bytes()))
			}
			if bufferResponse && inferenceSucceeded {
				upstreamAccepted = true
			}
			statusCode := resp.StatusCode
			routeReason := gatewayRequestRouteReason(c, gatewayRouteReason(decision, attempts, candidate))
			if clientCanceled {
				statusCode = gatewayClientClosedStatus
				routeReason = gatewayClientCanceledRouteReason(gatewayRequestIsKeyTest(c))
			} else if errText != "" && statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
				// The downstream status is already committed for an SSE response,
				// but its durable audit must remain a failed request rather than a
				// healthy 200 after a protocol terminal error.
				statusCode = http.StatusBadGateway
			}
			routeDetails := gatewayRouteDetailsWithErrors(decision, attempts, candidate, errorsSeen)
			if streamWarning != "" {
				routeDetails = appendGatewayRouteWarning(routeDetails, streamWarning)
			}
			requestLog := &storage.GatewayRequestLog{
				DownstreamKeyID:                downstreamKeyID,
				ClientIP:                       gatewayClientIP(c),
				ClientIPSource:                 gatewayClientIPSource(c),
				ModelName:                      model,
				ReasoningEffort:                reasoningEffort,
				Method:                         c.Request.Method,
				Path:                           c.Request.URL.Path,
				StatusCode:                     statusCode,
				FirstByteMs:                    firstByte.LatencyMs(),
				LatencyMs:                      finishedAt.Sub(start).Milliseconds(),
				PromptTokens:                   metrics.PromptTokens,
				CompletionTokens:               metrics.CompletionTokens,
				CacheCreationTokens:            metrics.CacheCreationTokens,
				CacheReadTokens:                metrics.CacheReadTokens,
				TotalTokens:                    metrics.TotalTokens,
				Cost:                           metrics.Cost,
				RawCost:                        optionalCostPointer(rawCost, metrics.CostObserved),
				CostEstimated:                  metrics.CostEstimated,
				CostSource:                     metrics.CostSource,
				UpstreamRequestID:              providerRequestID,
				UpstreamAccepted:               upstreamAccepted,
				BillingCommitted:               billingCommitted,
				SessionHash:                    decision.SessionHash,
				RouteReason:                    routeReason,
				RouteSelectionRank:             gatewayRouteSelectionRank(decision, candidate),
				SessionAffinityOriginRequestID: gatewaySessionAffinityOriginRequestID(decision),
				RouteDetails:                   routeDetails,
				RouteAttemptedChannels:         gatewayRouteAttemptedChannels(decision, attempts, candidate),
				Error:                          errText,
			}
			applyGatewayRequestPricingUsage(requestLog, metrics, imageRequest)
			gatewayApplySessionAffinityAudit(requestLog, decision)
			route.applyToLog(requestLog)
			mapping.applyToLog(requestLog)
			terminalFailureCode := ""
			if streamSanitizer != nil && streamSanitizer.RetryPrompted() {
				terminalFailureCode = "transient_after_acceptance"
			}
			telemetry.applyToLog(requestLog, terminalFailureCode)
			if storage.IsGatewayRequestCostReconcileEligible(requestLog) {
				costReconcileAt := gateway.InitialRequestCostReconcileAt(finishedAt)
				requestLog.CostReconcileAt = &costReconcileAt
			}
			terminalKind := gatewayDispatchTerminalFailedBeforeAcceptance
			switch {
			case clientCanceled:
				terminalKind = gatewayDispatchTerminalClientCanceled
			case inferenceSucceeded:
				terminalKind = gatewayDispatchTerminalSuccess
			case upstreamAccepted:
				terminalKind = gatewayDispatchTerminalFailedAfterAcceptance
			}
			if ownerLeaseLost {
				// The complete terminal log, including any tail usage, is now
				// available. Surrender it through the durable handoff instead of
				// finalizing against an owner this process no longer holds; the
				// handoff is authorized by the immutable owner identity recorded on
				// the dispatch audit, and rotation waits for it to become durable.
				terminalKind = storage.GatewayRequestDispatchTerminalOwnerLeaseLost
				dispatchRecoveryRequired[dispatchAudit] = struct{}{}
				scheduleGatewayDispatchRecovery(d, dispatchAudit, requestLog, terminalKind, start, finishedAt, gateway.ErrRequestDispatchOwnerLeaseLost)
				abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost while streaming upstream response", gateway.ErrRequestDispatchOwnerLeaseLost)
				return true
			}
			_, finalizeErr := dispatchAudit.finalize(d.Gateway, requestLog, terminalKind, start, finishedAt)
			if finalizeErr != nil {
				scheduleGatewayDispatchRecovery(d, dispatchAudit, requestLog, terminalKind, start, finishedAt, finalizeErr)
				abortGatewayRequestDispatch(c, d, "finalize terminal gateway request dispatch", finalizeErr)
				return true
			}
			if requestCause := context.Cause(c.Request.Context()); gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
				abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost after terminal finalization", requestCause)
				return true
			}
			if finalizeErr == nil && !gatewayRequestIsKeyTest(c) && inferenceSucceeded {
				if bufferResponse {
					observeGatewayImageOperationSuccessFromFacts(d, c.Request, candidate, model, body, nonStreamingFacts.Image, resp.Header, requestLog.ID)
				} else {
					observeGatewayImageOperationSuccess(d, c.Request, candidate, model, body, nil, streamedRaw.Bytes(), resp.Header, requestLog.ID)
				}
			}
			if finalizeErr == nil && inferenceSucceeded {
				if !gatewayRequestIsKeyTest(c) {
					touchGatewayCandidateRoutingHealth(d, candidate, groupName, finishedAt)
					_ = d.Gateway.ClearUpstreamAccountBalanceInsufficient(candidate)
				}
				if model != "" {
					markGatewayCandidateModelCapability(d.Gateway.Store, candidate, groupName, model, true, "")
				}
			}
			return true
		}
	}

	if requestCause := context.Cause(c.Request.Context()); gatewayRequestDispatchOwnerLeaseLost(c.Request, requestCause) {
		abortGatewayRequestDispatch(c, d, "request dispatch owner lease lost before terminal failure log", requestCause)
		return true
	}
	errText := sanitizeGatewayProbeError([]byte(strings.Join(errorsSeen, "; ")), "")
	if errText == "" {
		if smartRouting {
			errText = "all upstream channels failed"
		} else {
			errText = "fixed upstream target failed"
		}
	}
	finalStatus := http.StatusBadGateway
	publicResponse := gin.H{"error": gatewayPublicFailureMessage}
	if contextLimitAttempts > 0 && contextLimitAttempts == len(attempts) {
		finalStatus = http.StatusBadRequest
		publicResponse = gatewayPublicContextLimitResponse(c.Request.URL.Path)
	}
	routeDetailsAttempts := attempts
	selectedRoute := gateway.Candidate{}
	if !smartRouting {
		routeDetailsAttempts = nil
		if imageRequestCommitted {
			selectedRoute = committedImageRoute.Candidate
		} else {
			selectedRoute = fixedRoute.Candidate
		}
	}
	finishedAt := time.Now()
	telemetry.setRealFallback(false)
	requestLog := &storage.GatewayRequestLog{
		DownstreamKeyID:                downstreamKeyID,
		ClientIP:                       gatewayClientIP(c),
		ClientIPSource:                 gatewayClientIPSource(c),
		ModelName:                      model,
		ReasoningEffort:                reasoningEffort,
		Method:                         c.Request.Method,
		Path:                           c.Request.URL.Path,
		StatusCode:                     finalStatus,
		UpstreamAccepted:               imageRequestAccepted,
		BillingCommitted:               imageRequestCommitted,
		LatencyMs:                      finishedAt.Sub(start).Milliseconds(),
		SessionHash:                    decision.SessionHash,
		RouteReason:                    gatewayRequestRouteReason(c, gatewayRouteFailureReason(decision)),
		SessionAffinityOriginRequestID: gatewaySessionAffinityOriginRequestID(decision),
		RouteDetails:                   gatewayRouteDetailsWithErrors(decision, routeDetailsAttempts, selectedRoute, errorsSeen),
		RouteAttemptedChannels:         gatewayRouteAttemptedChannels(decision, attempts, gateway.Candidate{}),
		Error:                          errText,
	}
	gatewayApplySessionAffinityAudit(requestLog, decision)
	mapping.applyToLog(requestLog)
	if imageRequestCommitted {
		committedImageRoute.applyToLog(requestLog)
		metrics := estimateGatewayCost(committedImageMetrics, committedImageRoute.Candidate.RawRatio, committedImageRoute.Candidate.RawCompletionRatio)
		rawCost := metrics.Cost
		if metrics.CostObserved {
			if actualCost, conversionErr := costing.Actual(rawCost, committedImageRoute.Candidate.FundingFactor); conversionErr == nil {
				metrics.Cost = actualCost
			}
		}
		requestLog.PromptTokens = metrics.PromptTokens
		requestLog.CompletionTokens = metrics.CompletionTokens
		requestLog.CacheCreationTokens = metrics.CacheCreationTokens
		requestLog.CacheReadTokens = metrics.CacheReadTokens
		requestLog.TotalTokens = metrics.TotalTokens
		requestLog.Cost = metrics.Cost
		requestLog.RawCost = optionalCostPointer(rawCost, metrics.CostObserved)
		requestLog.CostEstimated = metrics.CostEstimated
		requestLog.CostSource = metrics.CostSource
		requestLog.UpstreamRequestID = committedImageUpstreamRequestID
		requestLog.RouteSelectionRank = gatewayRouteSelectionRank(decision, committedImageRoute.Candidate)
	} else if !smartRouting && fixedRoute.ChannelID > 0 {
		fixedRoute.applyToLog(requestLog)
		requestLog.RouteSelectionRank = gatewayRouteSelectionRank(decision, fixedRoute.Candidate)
	}
	retryPromptAfterExhaustion := gatewayRetryPromptAfterExhaustionEligible(smartRouting, app, c.Request.URL.Path, imageRequest, finalStatus, attempts)
	terminalFailureCode := ""
	if retryPromptAfterExhaustion {
		terminalFailureCode = "retry_prompt_after_exhaustion"
	}
	telemetry.applyToLog(requestLog, terminalFailureCode)
	if pendingFinalAudit != nil {
		if smartRouting {
			pendingFinalAudit.route.applyToLog(requestLog)
			requestLog.RouteSelectionRank = gatewayRouteSelectionRank(decision, pendingFinalAudit.route.Candidate)
		}
		requestLog.UpstreamRequestID = pendingFinalAudit.providerRequestID
		requestLog.UpstreamAccepted = pendingFinalAudit.acceptanceObserved
		requestLog.BillingCommitted = pendingFinalAudit.billingCommitted
	}
	pendingFinalFundingFactor := float64(0)
	if pendingFinalAudit != nil {
		pendingFinalFundingFactor = pendingFinalAudit.route.Candidate.FundingFactor
	}
	applyGatewayRequestUsageAndCost(requestLog, pendingFinalUsage, imageRequest, pendingFinalFundingFactor)
	if storage.IsGatewayRequestCostReconcileEligible(requestLog) {
		costReconcileAt := gateway.InitialRequestCostReconcileAt(finishedAt)
		requestLog.CostReconcileAt = &costReconcileAt
	}
	if imageRequestCommitted && committedImageDispatch != nil {
		if _, finalizeErr := committedImageDispatch.finalize(d.Gateway, requestLog, gatewayDispatchTerminalImageFailedAfterDispatch, start, finishedAt); finalizeErr != nil {
			scheduleGatewayDispatchRecovery(d, committedImageDispatch, requestLog, gatewayDispatchTerminalImageFailedAfterDispatch, start, finishedAt, finalizeErr)
			abortGatewayRequestDispatch(c, d, "finalize committed image fallback request", finalizeErr)
			return true
		}
	} else if pendingFinalAudit != nil {
		if _, finalizeErr := pendingFinalAudit.finalize(d.Gateway, requestLog, gatewayDispatchTerminalFailedBeforeAcceptance, start, finishedAt); finalizeErr != nil {
			scheduleGatewayDispatchRecovery(d, pendingFinalAudit, requestLog, gatewayDispatchTerminalFailedBeforeAcceptance, start, finishedAt, finalizeErr)
			abortGatewayRequestDispatch(c, d, "finalize last physical gateway request dispatch", finalizeErr)
			return true
		}
		pendingFinalAudit = nil
		pendingFinalAuditAt = time.Time{}
	} else {
		if d.Gateway.Store == nil {
			abortGatewayRequestDispatch(c, d, "append terminal gateway failure log", errors.New("gateway store is unavailable"))
			return true
		}
		if logErr := d.Gateway.Store.AppendRequestLog(requestLog); logErr != nil {
			abortGatewayRequestDispatch(c, d, "append terminal gateway failure log", logErr)
			return true
		}
		d.Gateway.ReconcileRequestCost(requestLog, start, finishedAt)
	}
	if retryPromptAfterExhaustion {
		writeGatewayRetryPrompt(c, streamingRequest, gatewayRetryPromptResponseID(telemetry))
	} else {
		c.JSON(finalStatus, publicResponse)
	}
	persistGatewayDownstreamFirstVisible(d, requestLog, telemetry)
	return true
}

func directRouteModelAllowed(candidate gateway.Candidate, app storage.GatewayApp, model string) bool {
	return candidate.Channel.CredentialMode == storage.CredentialModeDirect &&
		strings.EqualFold(strings.TrimSpace(string(candidate.Channel.GatewayApp)), strings.TrimSpace(string(app))) &&
		containsExactModel(candidate.ModelIDs, model)
}

func containsExactModel(models []string, requested string) bool {
	requested = strings.TrimSpace(requested)
	for _, item := range models {
		if strings.TrimSpace(item) == requested {
			return true
		}
	}
	return requested == ""
}

func bindGatewayRequestDispatchOwner(c *gin.Context, d *Deps) (func(), error) {
	if c == nil || c.Request == nil {
		return nil, errors.New("gateway request is unavailable")
	}
	if d == nil || d.Gateway == nil {
		return nil, errors.New("gateway service is unavailable")
	}
	if err := d.Gateway.StartRequestDispatchOwner(); err != nil {
		return nil, err
	}
	bound, release, err := d.Gateway.BindRequestDispatchOwnerContext(c.Request.Context())
	if err != nil {
		return nil, err
	}
	original := c.Request
	c.Request = original.WithContext(bound)
	return func() {
		release()
		c.Request = original
	}, nil
}

func gatewayRequestDispatchOwnerLeaseLost(request *http.Request, err error) bool {
	if gateway.IsRequestDispatchOwnerLeaseLost(err) {
		return true
	}
	return request != nil && gateway.IsRequestDispatchOwnerLeaseLost(context.Cause(request.Context()))
}

// closeGatewayDispatchAuditForFallback is retained for abnormal deferred
// cleanup only. A physical attempt here has no proven successor and therefore
// must atomically finalize a normal terminal log rather than being hidden.
func closeGatewayDispatchAuditForFallback(service *gateway.Service, audit *gatewayDispatchAuditState, terminalKind string, startedAt, finishedAt time.Time) error {
	if audit == nil || audit.closed || audit.surrendered {
		return nil
	}
	if audit.physicallyDispatched {
		terminalLog := gatewayDispatchRecoveryFallbackLog(audit, terminalKind, startedAt, finishedAt)
		if terminalLog == nil {
			return storage.ErrGatewayRequestDispatchRequiresFinalization
		}
		_, err := audit.finalize(service, terminalLog, terminalKind, startedAt, finishedAt)
		return err
	}
	if audit.billingCommitted || audit.acceptanceObserved {
		return storage.ErrGatewayRequestDispatchRequiresFinalization
	}
	providerRequestID := audit.providerRequestID
	if providerRequestID == "" {
		return audit.complete(terminalKind, finishedAt)
	}
	reconcileAt := gateway.InitialRequestCostReconcileAt(finishedAt)
	evidence := &storage.GatewayRequestLog{
		RecordKind:        storage.GatewayRequestRecordKindDispatchCostEvidence,
		UpstreamRequestID: providerRequestID,
		CostReconcileAt:   &reconcileAt,
		CreatedAt:         finishedAt,
	}
	_, err := audit.finalize(service, evidence, terminalKind, startedAt, finishedAt)
	return err
}

func gatewayDispatchIntermediateRolloverEvidenceLog(audit *gatewayDispatchAuditState, finishedAt time.Time) *storage.GatewayRequestLog {
	if audit == nil || audit.providerRequestID == "" {
		return nil
	}
	reconcileAt := gateway.InitialRequestCostReconcileAt(finishedAt)
	return &storage.GatewayRequestLog{
		RecordKind:        storage.GatewayRequestRecordKindDispatchCostEvidence,
		UpstreamRequestID: audit.providerRequestID,
		CostReconcileAt:   &reconcileAt,
		CreatedAt:         finishedAt,
	}
}

func scheduleGatewayDispatchRecovery(d *Deps, audit *gatewayDispatchAuditState, requestLog *storage.GatewayRequestLog, terminalKind string, startedAt, at time.Time, cause error) error {
	if d == nil || d.Gateway == nil || audit == nil || audit.closed || audit.id == 0 {
		return errors.New("request dispatch recovery handoff is unavailable")
	}
	audit.surrendered = true
	if requestLog == nil {
		requestLog = gatewayDispatchRecoveryFallbackLog(audit, terminalKind, startedAt, at)
	}
	audit.applyTerminalFactsToLog(requestLog)
	err := d.Gateway.ScheduleRequestDispatchRecovery(gateway.RequestDispatchRecoveryJob{
		DispatchID:           audit.id,
		OwnerInstanceID:      audit.ownerInstanceID,
		OwnerFenceToken:      audit.ownerFenceToken,
		TerminalKind:         terminalKind,
		RequestedAt:          at,
		StartedAt:            startedAt,
		FinishedAt:           at,
		Log:                  requestLog,
		Facts:                audit.recoveryFacts(),
		TelemetryVersion:     audit.telemetryVersion,
		PhysicallyDispatched: audit.physicallyDispatched,
	})
	if err != nil && d.Log != nil {
		d.Log.Error("queue gateway request dispatch recovery failed", "dispatch_id", audit.id, "cause", cause, "err", err)
	}
	return err
}

func scheduleGatewayLinkedDispatchRecovery(
	d *Deps,
	previous *gatewayDispatchAuditState,
	previousLog *storage.GatewayRequestLog,
	previousFinishedAt time.Time,
	next *gatewayDispatchAuditState,
	nextLog *storage.GatewayRequestLog,
	nextFinishedAt time.Time,
	terminalKind string,
	startedAt time.Time,
	knownUnsentSuccessor bool,
) error {
	if d == nil || d.Gateway == nil || previous == nil || next == nil || previous.closed || next.closed || previous.id == 0 || next.id == 0 {
		return errors.New("linked request dispatch recovery handoff is unavailable")
	}
	previous.surrendered = true
	next.surrendered = true
	if previousLog == nil {
		previousLog = gatewayDispatchRecoveryFallbackLog(previous, terminalKind, startedAt, previousFinishedAt)
	}
	if nextLog == nil {
		nextLog = gatewayDispatchRecoveryFallbackLog(next, terminalKind, startedAt, nextFinishedAt)
	}
	previous.applyTerminalFactsToLog(previousLog)
	next.applyTerminalFactsToLog(nextLog)
	return d.Gateway.ScheduleLinkedRequestDispatchRecovery(
		gateway.RequestDispatchRecoveryJob{
			DispatchID: previous.id, OwnerInstanceID: previous.ownerInstanceID, OwnerFenceToken: previous.ownerFenceToken,
			TerminalKind: terminalKind, RequestedAt: nextFinishedAt,
			StartedAt: startedAt, FinishedAt: previousFinishedAt, Log: previousLog, Facts: previous.recoveryFacts(),
			TelemetryVersion: previous.telemetryVersion, PhysicallyDispatched: previous.physicallyDispatched,
		},
		gateway.RequestDispatchRecoveryJob{
			DispatchID: next.id, OwnerInstanceID: next.ownerInstanceID, OwnerFenceToken: next.ownerFenceToken,
			TerminalKind: terminalKind, RequestedAt: nextFinishedAt,
			StartedAt: startedAt, FinishedAt: nextFinishedAt, Log: nextLog, Facts: next.recoveryFacts(),
			TelemetryVersion: next.telemetryVersion, PhysicallyDispatched: next.physicallyDispatched,
		},
		knownUnsentSuccessor,
	)
}

func gatewayDispatchRecoveryFallbackLog(audit *gatewayDispatchAuditState, terminalKind string, startedAt, finishedAt time.Time) *storage.GatewayRequestLog {
	if audit == nil {
		return nil
	}
	facts := audit.recoveryFacts()
	providerRequestID := facts.ProviderRequestID
	if !audit.physicallyDispatched && !facts.Accepted && !audit.billingCommitted && providerRequestID == "" {
		return nil
	}
	reconcileAt := gateway.InitialRequestCostReconcileAt(finishedAt)
	if !audit.physicallyDispatched && !facts.Accepted && !audit.billingCommitted {
		return &storage.GatewayRequestLog{
			RecordKind:        storage.GatewayRequestRecordKindDispatchCostEvidence,
			UpstreamRequestID: providerRequestID,
			CostReconcileAt:   &reconcileAt,
			CreatedAt:         finishedAt,
		}
	}
	statusCode := http.StatusBadGateway
	errorText := "request completion could not be confirmed"
	terminalFailureCode := ""
	if audit.telemetryVersion != 0 {
		terminalFailureCode = "dispatch_recovery"
	}
	if terminalKind == gatewayDispatchTerminalClientCanceled {
		statusCode = gatewayClientClosedStatus
		errorText = "客户端取消"
		if audit.telemetryVersion != 0 {
			terminalFailureCode = "client_canceled"
		}
	}
	latencyMs := int64(0)
	if !startedAt.IsZero() && startedAt.Before(finishedAt) {
		latencyMs = finishedAt.Sub(startedAt).Milliseconds()
	}
	return &storage.GatewayRequestLog{
		StatusCode:          statusCode,
		LatencyMs:           latencyMs,
		UpstreamRequestID:   providerRequestID,
		UpstreamAccepted:    facts.Accepted,
		BillingCommitted:    audit.billingCommitted,
		CostReconcileAt:     &reconcileAt,
		TerminalFailureCode: terminalFailureCode,
		Error:               errorText,
		CreatedAt:           finishedAt,
	}
}

func applyGatewayRecoveryRequestIdentity(log *storage.GatewayRequestLog, request *http.Request) {
	if log == nil || request == nil {
		return
	}
	log.Method = request.Method
	log.Path = request.URL.Path
}

func gatewayDispatchOwnerLossRecoveryLog(
	audit *gatewayDispatchAuditState,
	pendingAudit *gatewayDispatchAuditState,
	currentAudit *gatewayDispatchAuditState,
	pendingUsage gatewayUsageMetrics,
	currentUsage gatewayUsageMetrics,
	imageRequest bool,
	request *http.Request,
	startedAt time.Time,
	finishedAt time.Time,
) *storage.GatewayRequestLog {
	if audit == nil {
		return nil
	}
	var usage gatewayUsageMetrics
	switch audit {
	case pendingAudit:
		usage = pendingUsage
	case currentAudit:
		usage = currentUsage
	default:
		return nil
	}
	log := gatewayDispatchRecoveryFallbackLog(audit, storage.GatewayRequestDispatchTerminalOwnerLeaseLost, startedAt, finishedAt)
	applyGatewayRecoveryRequestIdentity(log, request)
	applyGatewayRequestUsageAndCost(log, usage, imageRequest, audit.route.Candidate.FundingFactor)
	return log
}

func gatewayDispatchConfirmationFailureLog(audit *gatewayDispatchAuditState, confirmFailure error, provider storage.ChannelType, upstreamKey string, startedAt, finishedAt time.Time) *storage.GatewayRequestLog {
	log := gatewayDispatchRecoveryFallbackLog(audit, gatewayDispatchTerminalAuditFailure, startedAt, finishedAt)
	if log == nil {
		return nil
	}
	var outcome *gatewayTransportConfirmationFailure
	if !errors.As(confirmFailure, &outcome) || outcome == nil {
		return log
	}
	if outcome.StatusCode > 0 {
		log.StatusCode = outcome.StatusCode
	}
	if outcome.ResponseObserved && audit != nil && audit.responseObservedAt.IsZero() {
		audit.responseObservedAt = finishedAt
	}
	if providerID, err := gatewayUpstreamRequestID(outcome.Header, provider); err == nil && providerID != "" {
		log.UpstreamRequestID = providerID
		if audit != nil {
			audit.providerRequestID = providerID
		}
	}
	if outcome.TransportErr != nil {
		if detail := sanitizeGatewayProbeError([]byte(outcome.TransportErr.Error()), upstreamKey); detail != "" {
			log.Error = detail
		}
	} else if outcome.StatusCode > 0 {
		log.Error = fmt.Sprintf("upstream transport returned HTTP %d before durable confirmation failed", outcome.StatusCode)
	}
	return log
}

func abortGatewayRequestDispatch(c *gin.Context, d *Deps, operation string, err error) {
	if d != nil && d.Log != nil {
		d.Log.Warn("gateway request stopped because durable dispatch state is unavailable", "operation", strings.TrimSpace(operation), "err", err)
	}
	if c == nil {
		return
	}
	c.Abort()
	if !c.Writer.Written() {
		c.Header("Retry-After", "1")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": gatewayPublicFailureMessage})
	}
}

func isGatewayClientCanceled(request *http.Request, err error) bool {
	if err == nil {
		return false
	}
	if gatewayRequestDispatchOwnerLeaseLost(request, err) {
		return false
	}
	if request == nil {
		return errors.Is(err, context.Canceled)
	}
	return errors.Is(context.Cause(request.Context()), context.Canceled)
}

func logGatewayClientCanceled(c *gin.Context, d *Deps, downstreamKeyID uint, decision *gatewayRouteDecision, route gatewayRouteSnapshot, model string, reasoningEffort string, mapping gatewayModelMappingResult, start time.Time) {
	if d == nil || d.Gateway == nil {
		return
	}
	finishedAt := time.Now()
	d.Gateway.LogRequest(gatewayClientCanceledRequestLog(downstreamKeyID, decision, route, model, reasoningEffort, mapping, gatewayRequestIsKeyTest(c), gatewayClientIP(c), gatewayClientIPSource(c), c.Request.Method, c.Request.URL.Path, start, finishedAt))
}

func gatewayClientCanceledRequestLog(downstreamKeyID uint, decision *gatewayRouteDecision, route gatewayRouteSnapshot, model string, reasoningEffort string, mapping gatewayModelMappingResult, keyTest bool, clientIP string, clientIPSource string, method string, path string, start time.Time, finishedAt time.Time) *storage.GatewayRequestLog {
	requestLog := &storage.GatewayRequestLog{
		DownstreamKeyID:                downstreamKeyID,
		ClientIP:                       clientIP,
		ClientIPSource:                 clientIPSource,
		ModelName:                      model,
		ReasoningEffort:                reasoningEffort,
		Method:                         method,
		Path:                           path,
		StatusCode:                     gatewayClientClosedStatus,
		LatencyMs:                      finishedAt.Sub(start).Milliseconds(),
		SessionHash:                    decisionSessionHash(decision),
		RouteReason:                    gatewayClientCanceledRouteReason(keyTest),
		RouteSelectionRank:             gatewayRouteSelectionRank(decision, route.Candidate),
		SessionAffinityOriginRequestID: gatewaySessionAffinityOriginRequestID(decision),
		RouteDetails:                   gatewayRouteDetails(decision, nil, route.Candidate),
		RouteAttemptedChannels:         gatewayRouteAttemptedChannels(decision, nil, route.Candidate),
		CostReconcileAt:                nil,
		Error:                          "客户端取消",
	}
	gatewayApplySessionAffinityAudit(requestLog, decision)
	route.applyToLog(requestLog)
	mapping.applyToLog(requestLog)
	return requestLog
}

func gatewayClientIP(c *gin.Context) string {
	if c == nil {
		return ""
	}
	return strings.TrimSpace(c.ClientIP())
}

func gatewayRequestIsKeyTest(c *gin.Context) bool {
	if c == nil {
		return false
	}
	value, ok := c.Get(gatewayDownstreamKeyTestContextKey)
	keyTest, _ := value.(bool)
	return ok && keyTest
}

func gatewayRequestRouteReason(c *gin.Context, routeReason string) string {
	if gatewayRequestIsKeyTest(c) {
		return gatewayRouteReasonKeyTest
	}
	return routeReason
}

func gatewayClientCanceledRouteReason(keyTest bool) string {
	if keyTest {
		return gatewayRouteReasonKeyTest
	}
	return gatewayRouteReasonClientCanceled
}

func numberPointer(value float64) *float64 { return &value }

func optionalCostPointer(value float64, available bool) *float64 {
	if !available {
		return nil
	}
	return &value
}

func decisionSessionHash(decision *gatewayRouteDecision) string {
	if decision == nil {
		return ""
	}
	return decision.SessionHash
}

func gatewayRequestSessionHash(request *http.Request, body []byte) string {
	var value string
	if request != nil {
		for _, header := range []string{
			gatewaySessionHeader,
			legacyGatewaySessionHeader,
			"X-Session-ID",
			"Session-ID",
			"X-Conversation-ID",
			"Conversation-ID",
		} {
			if value = strings.TrimSpace(request.Header.Get(header)); value != "" {
				break
			}
		}
	}
	if value == "" && len(bytes.TrimSpace(body)) > 0 {
		var payload map[string]any
		if json.Unmarshal(body, &payload) == nil {
			value = gatewaySessionValue(payload)
		}
	}
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func gatewaySessionValue(payload map[string]any) string {
	for _, key := range []string{"session_id", "conversation_id"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	switch conversation := payload["conversation"].(type) {
	case string:
		if strings.TrimSpace(conversation) != "" {
			return strings.TrimSpace(conversation)
		}
	case map[string]any:
		if value, ok := conversation["id"].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	metadata, _ := payload["metadata"].(map[string]any)
	for _, key := range []string{"session_id", "conversation_id"} {
		if value, ok := metadata[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

type gatewayRouteAttempt struct {
	ChannelID         uint
	ChannelName       string
	GroupName         string
	Reason            string
	Summary           string
	StatusCode        int
	EligibleRank      uint
	Physical          bool
	FirstByteBudgetMs int64
	Retryable         bool
}

func newGatewayRouteAttempt(candidate gateway.Candidate, reason string, statusCode int, summary ...string) gatewayRouteAttempt {
	attempt := gatewayRouteAttempt{
		ChannelID:   candidate.Channel.ID,
		ChannelName: strings.TrimSpace(candidate.Channel.Name),
		GroupName:   strings.TrimSpace(candidate.Rate.ModelName),
		Reason:      strings.TrimSpace(reason),
		StatusCode:  statusCode,
		Retryable:   gatewayRouteAttemptRetryable(reason, statusCode),
	}
	attempt.Summary = gatewayRouteAttemptSummary(gatewayRouteAttemptLabel(attempt.Reason))
	if len(summary) > 0 {
		if concrete := gatewayRouteAttemptSummary(summary[0]); concrete != "" {
			attempt.Summary = concrete
		}
	}
	return attempt
}

func gatewayRouteAttemptRetryable(reason string, statusCode int) bool {
	if isTransientGatewayStatus(statusCode) {
		return true
	}
	switch strings.TrimSpace(reason) {
	case "transport", "response_read", "response_timeout", "stream_prelude", "stream_empty", "stream_account_limit", "upstream_account_concurrency", "upstream_key_capacity":
		return true
	default:
		return false
	}
}

func gatewayRetryPromptAfterExhaustionEligible(smartRouting bool, app storage.GatewayApp, requestPath string, imageRequest bool, finalStatus int, attempts []gatewayRouteAttempt) bool {
	if !smartRouting || imageRequest || finalStatus != http.StatusBadGateway || app != storage.GatewayAppCodex || requestPath != "/v1/responses" {
		return false
	}
	for _, attempt := range attempts {
		if attempt.Retryable {
			return true
		}
	}
	return false
}

func newGatewayRouteAttemptFromDispatch(dispatch *gatewayDispatchAuditState, candidate gateway.Candidate, reason string, statusCode int, summary ...string) gatewayRouteAttempt {
	attempt := newGatewayRouteAttempt(candidate, reason, statusCode, summary...)
	if dispatch == nil {
		return attempt
	}
	attempt.Physical = dispatch.physicallyDispatched
	if dispatch.firstByteBudgetMs > 0 {
		attempt.FirstByteBudgetMs = dispatch.firstByteBudgetMs
	}
	return attempt
}

func gatewayRouteAttemptSummary(value string) string {
	return truncateGatewayText(sanitizeGatewayProbeError([]byte(value), ""), 240)
}

func gatewayRouteReason(decision *gatewayRouteDecision, attempts []gatewayRouteAttempt, selected ...gateway.Candidate) string {
	if decision == nil || !decision.SmartRouting {
		return gatewayRouteReasonManual
	}
	var selectedCandidate gateway.Candidate
	if len(selected) > 0 {
		selectedCandidate = selected[0]
	}
	if gatewayRouteHasRealFallback(decision, attempts, selectedCandidate) {
		return gatewayRouteReasonFallback
	}
	for _, exclusion := range decision.Exclusions {
		if exclusion.Reason == "model_not_listed" || exclusion.Reason == "model_not_supported" || exclusion.Reason == "model_capability_unknown" {
			return gatewayRouteReasonModelFiltered
		}
	}
	if decision.AffinityApplied {
		return gatewayRouteReasonSessionAffinity
	}
	return gatewayRouteReasonSmartPrimary
}

func gatewayRouteHasRealFallback(decision *gatewayRouteDecision, attempts []gatewayRouteAttempt, selected gateway.Candidate) bool {
	if decision == nil || !decision.SmartRouting {
		return false
	}
	for _, attempt := range attempts {
		if !attempt.Physical || !gatewayRouteAttemptIsFailure(attempt.Reason) {
			continue
		}
		if selected.Channel.ID != 0 && attempt.ChannelID == selected.Channel.ID &&
			strings.EqualFold(strings.TrimSpace(attempt.GroupName), strings.TrimSpace(selected.Rate.ModelName)) {
			continue
		}
		return true
	}
	return false
}

func gatewayRouteFailureReason(decision *gatewayRouteDecision) string {
	if decision == nil || !decision.SmartRouting {
		return gatewayRouteReasonManual
	}
	return gatewayRouteReasonAllFailed
}

func gatewayRouteDetails(decision *gatewayRouteDecision, attempts []gatewayRouteAttempt, selected gateway.Candidate) string {
	return joinGatewayRouteDetails(gatewayRouteDetailParts(decision, attempts, selected), gatewaySelectedRouteDetail(decision, selected))
}

func gatewayRouteDetailParts(decision *gatewayRouteDecision, attempts []gatewayRouteAttempt, selected gateway.Candidate) []string {
	parts := make([]string, 0, len(attempts)+2)
	if decision != nil {
		if source := gatewaySessionAffinityOriginDetail(decision.AffinityOrigin); source != "" {
			parts = append(parts, source)
		}
		selectedPreMappingRank := gatewayPreMappingCandidateRank(decision, selected)
		for _, exclusion := range decision.Exclusions {
			if exclusion.Reason != "model_not_listed" && exclusion.Reason != "model_not_supported" && exclusion.Reason != "model_capability_unknown" && exclusion.Reason != "upstream_temporarily_unavailable" {
				continue
			}
			prefix := "higher-ranked"
			if selectedPreMappingRank > 0 && exclusion.Reason != "upstream_temporarily_unavailable" {
				rank := gatewayCandidateRank(decision.PreMappingCandidates, exclusion.ChannelID, exclusion.GroupName)
				if rank == 0 || rank >= selectedPreMappingRank {
					continue
				}
				prefix = fmt.Sprintf("higher pre-mapping priority #%d", rank)
			}
			model := strings.TrimSpace(exclusion.Detail)
			if model == "" {
				model = strings.TrimSpace(decision.RequestedModel)
			}
			reason := "is not listed by the upstream group"
			switch exclusion.Reason {
			case "model_not_supported":
				reason = "was rejected as unsupported"
			case "model_capability_unknown":
				reason = "has no synchronized model capability"
			case "upstream_temporarily_unavailable":
				reason = "is temporarily unavailable"
			}
			if exclusion.Reason == "upstream_temporarily_unavailable" {
				parts = append(parts, fmt.Sprintf("%s %s/%s filtered: %s", prefix, exclusion.ChannelName, exclusion.GroupName, reason))
				continue
			}
			parts = append(parts, fmt.Sprintf("%s %s/%s filtered: model %s %s", prefix, exclusion.ChannelName, exclusion.GroupName, model, reason))
		}
	}
	for _, attempt := range attempts {
		location := ""
		if attempt.Reason != "candidate_resolution" {
			location = strings.Trim(strings.TrimSpace(attempt.ChannelName)+"/"+strings.TrimSpace(attempt.GroupName), "/")
		}
		if location == "" {
			location = "candidate selection"
		}
		detail := fmt.Sprintf("attempt failed %s: %s", location, gatewayRouteAttemptLabel(attempt.Reason))
		if attempt.StatusCode > 0 {
			detail += fmt.Sprintf(" (status %d)", attempt.StatusCode)
		}
		parts = append(parts, detail)
	}
	return parts
}

func gatewaySelectedRouteDetail(decision *gatewayRouteDecision, selected gateway.Candidate) string {
	if selected.Channel.ID == 0 {
		return ""
	}
	detail := fmt.Sprintf("selected %s/%s", strings.TrimSpace(selected.Channel.Name), strings.TrimSpace(selected.Rate.ModelName))
	annotations := make([]string, 0, 3)
	if decision != nil {
		if rank := gatewayEligibleCandidateRank(decision, selected); rank > 0 {
			if decision.AffinityApplied {
				annotations = append(annotations, fmt.Sprintf("normal eligible priority #%d", rank))
			} else if preMappingRank := gatewayPreMappingCandidateRank(decision, selected); preMappingRank > 0 {
				annotations = append(annotations, fmt.Sprintf("mapped-model eligible priority #%d", rank))
				annotations = append(annotations, fmt.Sprintf("pre-mapping priority #%d", preMappingRank))
			} else {
				annotations = append(annotations, fmt.Sprintf("eligible priority #%d", rank))
			}
		}
	}
	if decision != nil && decision.AffinityApplied {
		annotations = append(annotations, "session affinity")
	}
	if len(annotations) == 0 {
		return detail
	}
	return detail + " (" + strings.Join(annotations, ", ") + ")"
}

// gatewayEligibleCandidateRank reports the request-time eligible ordering.
// The candidate list has already excluded unsupported models, cooldowns,
// disabled routes, and per-key exclusions.
func gatewayEligibleCandidateRank(decision *gatewayRouteDecision, selected gateway.Candidate) int {
	if decision == nil || selected.Channel.ID == 0 {
		return 0
	}
	candidates := decision.RankedCandidates
	if len(candidates) == 0 {
		candidates = decision.Candidates
	}
	return gatewayCandidateRank(candidates, selected.Channel.ID, selected.Rate.ModelName)
}

// gatewayPreMappingCandidateRank preserves the original candidate order for a
// mapped request. The mapped-model filter intentionally runs after ordinary
// price/latency scoring, so this is the missing evidence when a channel that
// was tenth before filtering becomes the first compatible route.
func gatewayPreMappingCandidateRank(decision *gatewayRouteDecision, selected gateway.Candidate) int {
	if decision == nil || selected.Channel.ID == 0 || len(decision.PreMappingCandidates) == 0 {
		return 0
	}
	return gatewayCandidateRank(decision.PreMappingCandidates, selected.Channel.ID, selected.Rate.ModelName)
}

func gatewayCandidateRank(candidates []gateway.Candidate, channelID uint, groupName string) int {
	if channelID == 0 {
		return 0
	}
	groupName = strings.TrimSpace(groupName)
	for index, candidate := range candidates {
		if candidate.Channel.ID == channelID && strings.TrimSpace(candidate.Rate.ModelName) == groupName {
			return index + 1
		}
	}
	return 0
}

func gatewayRouteSelectionRank(decision *gatewayRouteDecision, selected gateway.Candidate) uint {
	rank := gatewayEligibleCandidateRank(decision, selected)
	if rank <= 0 {
		return 0
	}
	return uint(rank)
}

func gatewaySessionAffinityOriginRequestID(decision *gatewayRouteDecision) uint {
	if decision == nil || decision.AffinityOrigin == nil {
		return 0
	}
	return decision.AffinityOrigin.ID
}

func gatewaySessionAffinityExpiresAt(decision *gatewayRouteDecision) *time.Time {
	if decision == nil || !decision.AffinityApplied || decision.AffinityExpiresAt == nil {
		return nil
	}
	expiresAt := decision.AffinityExpiresAt.UTC()
	return &expiresAt
}

func gatewayApplySessionAffinityAudit(log *storage.GatewayRequestLog, decision *gatewayRouteDecision) {
	if log == nil {
		return
	}
	log.SessionAffinityExpiresAt = gatewaySessionAffinityExpiresAt(decision)
	log.SessionAffinityOverriddenCandidates = gatewaySessionAffinityOverriddenCandidates(decision)
}

type gatewaySessionAffinityOverriddenCandidate struct {
	ChannelID    uint   `json:"channel_id"`
	ChannelName  string `json:"channel_name"`
	GroupName    string `json:"group_name"`
	EligibleRank uint   `json:"eligible_rank"`
}

func gatewaySessionAffinityOverriddenCandidates(decision *gatewayRouteDecision) string {
	if decision == nil || !decision.AffinityApplied || len(decision.RankedCandidates) == 0 {
		return ""
	}
	if len(decision.Candidates) == 0 {
		return ""
	}
	affinityTarget := decision.Candidates[0]
	rank := gatewayEligibleCandidateRank(decision, affinityTarget)
	if rank <= 1 {
		return ""
	}
	traces := make([]gatewaySessionAffinityOverriddenCandidate, 0, rank-1)
	for index, candidate := range decision.RankedCandidates[:rank-1] {
		channelName := strings.TrimSpace(candidate.Channel.Name)
		groupName := strings.TrimSpace(candidate.Rate.ModelName)
		if candidate.Channel.ID == 0 || channelName == "" || groupName == "" {
			return ""
		}
		traces = append(traces, gatewaySessionAffinityOverriddenCandidate{
			ChannelID:    candidate.Channel.ID,
			ChannelName:  channelName,
			GroupName:    groupName,
			EligibleRank: uint(index + 1),
		})
	}
	if len(traces) == 0 || len(traces) > 128 {
		return ""
	}
	encoded, err := json.Marshal(traces)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func gatewaySessionAffinityOriginDetail(origin *storage.GatewayRecentRoute) string {
	if origin == nil || origin.ID == 0 {
		return ""
	}
	detail := fmt.Sprintf("session affinity source request #%d (%s, normal eligible priority #%d)", origin.ID, strings.TrimSpace(origin.RouteReason), origin.SelectionRank)
	if origin.RouteReason != gatewayRouteReasonFallback {
		return detail
	}
	traces, ok := gatewayRouteAttemptTraces(origin.RouteAttemptedChannels)
	if !ok {
		return detail
	}
	parts := make([]string, 0, len(traces))
	for _, trace := range traces {
		location := strings.Trim(strings.TrimSpace(trace.ChannelName)+"/"+strings.TrimSpace(trace.GroupName), "/")
		if location == "" {
			location = fmt.Sprintf("channel #%d", trace.ChannelID)
		}
		part := fmt.Sprintf("#%d %s: %s", trace.EligibleRank, location, gatewayRouteAttemptLabel(trace.Reason))
		if trace.StatusCode > 0 {
			part += fmt.Sprintf(" (status %d)", trace.StatusCode)
		}
		parts = append(parts, part)
	}
	if len(parts) > 0 {
		detail += ": source request exhausted " + strings.Join(parts, "; ")
	}
	return detail
}

func gatewayRouteDetailsWithErrors(decision *gatewayRouteDecision, attempts []gatewayRouteAttempt, selected gateway.Candidate, errorsSeen []string) string {
	parts := make([]string, 0, len(errorsSeen)+len(attempts)+1)
	for _, failure := range errorsSeen {
		failure = sanitizeGatewayProbeError([]byte(failure), "")
		if failure != "" {
			parts = append(parts, "upstream error: "+failure)
		}
	}
	parts = append(parts, gatewayRouteDetailParts(decision, attempts, selected)...)
	return joinGatewayRouteDetails(parts, gatewaySelectedRouteDetail(decision, selected))
}

func joinGatewayRouteDetails(parts []string, selected string) string {
	selected = truncateGatewayText(sanitizeGatewayProbeError([]byte(selected), ""), gatewayRouteDetailPartLimit)
	limit := gatewayRouteDetailsLimit
	selectedRunes := len([]rune(selected))
	if selectedRunes >= limit {
		return truncateGatewayText(selected, limit)
	}
	available := limit
	if selected != "" {
		available -= selectedRunes + 2
	}
	included := make([]string, 0, len(parts)+1)
	used := 0
	omitted := 0
	for _, part := range parts {
		part = truncateGatewayText(sanitizeGatewayProbeError([]byte(part), ""), gatewayRouteDetailPartLimit)
		if part == "" {
			continue
		}
		separator := 0
		if used > 0 || selected != "" {
			separator = 2
		}
		partRunes := len([]rune(part))
		if used+separator+partRunes > available {
			omitted++
			continue
		}
		included = append(included, part)
		used += separator + partRunes
	}
	if omitted > 0 {
		marker := fmt.Sprintf("%d more route details omitted", omitted)
		separator := 0
		if used > 0 || selected != "" {
			separator = 2
		}
		if used+separator+len([]rune(marker)) <= available {
			included = append(included, marker)
		}
	}
	if selected != "" {
		included = append(included, selected)
	}
	return strings.Join(included, "; ")
}

type gatewayRouteAttemptTrace struct {
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

func gatewayRouteAttemptedChannels(decision *gatewayRouteDecision, attempts []gatewayRouteAttempt, _ gateway.Candidate) string {
	if decision == nil || !decision.SmartRouting {
		return ""
	}
	traces := make([]gatewayRouteAttemptTrace, 0, len(attempts))
	for _, attempt := range attempts {
		if !attempt.Physical {
			continue
		}
		if strings.TrimSpace(attempt.ChannelName) == "" && attempt.ChannelID == 0 {
			continue
		}
		traces = append(traces, gatewayRouteAttemptTrace{
			ChannelID:         attempt.ChannelID,
			ChannelName:       strings.TrimSpace(attempt.ChannelName),
			GroupName:         strings.TrimSpace(attempt.GroupName),
			Reason:            strings.TrimSpace(attempt.Reason),
			Summary:           gatewayRouteAttemptSummary(attempt.Summary),
			StatusCode:        attempt.StatusCode,
			EligibleRank:      gatewayRouteAttemptRank(decision, attempt),
			Physical:          attempt.Physical,
			FirstByteBudgetMs: gatewayRouteAttemptFirstByteBudget(attempt.FirstByteBudgetMs),
		})
	}
	if len(traces) == 0 {
		return ""
	}
	encoded, err := json.Marshal(traces)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func gatewayRouteAttemptFirstByteBudget(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func gatewayRouteAttemptRank(decision *gatewayRouteDecision, attempt gatewayRouteAttempt) uint {
	if attempt.EligibleRank > 0 {
		return attempt.EligibleRank
	}
	if decision == nil {
		return 0
	}
	candidates := decision.RankedCandidates
	if len(candidates) == 0 {
		candidates = decision.Candidates
	}
	return uint(gatewayCandidateRank(candidates, attempt.ChannelID, attempt.GroupName))
}

func gatewayRouteAttemptTraces(raw string) ([]gatewayRouteAttemptTrace, bool) {
	if strings.TrimSpace(raw) == "" {
		return nil, false
	}
	var traces []gatewayRouteAttemptTrace
	if err := json.Unmarshal([]byte(raw), &traces); err != nil || len(traces) == 0 {
		return nil, false
	}
	return traces, true
}

func gatewayRouteAttemptIsFailure(reason string) bool {
	switch strings.TrimSpace(reason) {
	case "upstream_key", "target_resolution", "model_family", "transport", "model_unsupported", "context_limit", "response_read", "response_body_started", "response_spool_local", "response_timeout", "stream_prelude", "stream_error", "stream_empty", "stream_account_limit", "upstream_account_unavailable", "upstream_account_concurrency", "upstream_key_capacity", "upstream_status":
		return true
	default:
		return false
	}
}

func gatewayRouteAttemptLabel(reason string) string {
	switch strings.TrimSpace(reason) {
	case "candidate_resolution":
		return "no eligible upstream candidate"
	case "upstream_key":
		return "upstream key unavailable"
	case "target_resolution":
		return "upstream target unavailable"
	case "model_family":
		return "model family is not allowed"
	case "transport":
		return "upstream connection failed"
	case "model_unsupported":
		return "requested model is not supported"
	case "context_limit":
		return "request exceeds upstream model context limit"
	case "response_read":
		return "upstream response could not be read"
	case "response_body_started":
		return "upstream response started but did not complete"
	case "response_spool_local":
		return "local response buffer was unavailable after upstream dispatch"
	case "response_timeout":
		return "upstream response timed out before first output"
	case "stream_prelude":
		return "upstream stream could not be started"
	case "stream_error":
		return "upstream stream failed before useful output"
	case "stream_empty":
		return "upstream stream ended before useful output"
	case "stream_account_limit":
		return "upstream capacity, concurrency, or rate limit exceeded"
	case "upstream_account_unavailable":
		return "upstream account or workspace is unavailable"
	case "upstream_account_concurrency":
		return "upstream account concurrency admission is currently full"
	case "upstream_key_capacity":
		return "upstream key creation hit capacity, concurrency, or rate limit"
	case "dispatch_audit":
		return "durable request audit could not be persisted"
	case "mapping_runtime_fallback":
		return "mapped model rejected; retried requested model"
	default:
		return "upstream returned a fallback status"
	}
}

func gatewayUpstreamRequestID(headers http.Header, provider storage.ChannelType) (string, error) {
	headerName := ""
	switch provider {
	case storage.ChannelTypeSub2API:
		headerName = "X-Client-Request-ID"
	case storage.ChannelTypeNewAPI:
		headerName = "X-Oneapi-Request-Id"
	default:
		return "", nil
	}
	values := headers.Values(headerName)
	if len(values) == 0 {
		return "", nil
	}
	value := values[0]
	// A proxy can repeat a header while preserving the same value. Any
	// disagreement means there is no single authoritative provider ID, so do
	// not select an arbitrary value and risk reconciling the wrong request.
	for _, candidate := range values[1:] {
		if candidate != value {
			return "", errors.New("conflicting authoritative upstream request ID headers")
		}
	}
	if !utf8.ValidString(value) || len(value) > gatewayUpstreamRequestIDMaxLength {
		return "", fmt.Errorf("authoritative upstream request ID exceeds %d bytes", gatewayUpstreamRequestIDMaxLength)
	}
	return value, nil
}

// gatewayRequestBodyLimitForPath is the authoritative raw-wire ceiling used
// both for admission reservations and for the read limiter. Paths currently
// share the same ceiling, but keeping the choice here prevents a future
// path-specific limit from drifting away from admission accounting.
func gatewayRequestBodyLimitForPath(_ string) int64 {
	return gatewayBodyLimit
}

func gatewayRequestBodyReadExceededLimit(err error) bool {
	var maxBytesErr *http.MaxBytesError
	return errors.As(err, &maxBytesErr)
}

func readGatewayBody(w http.ResponseWriter, r *http.Request, maxBodyBytes int64, lease *gatewayRequestAdmissionLease, profile gatewayRequestAdmissionBodyProfile) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if r.ContentLength >= 0 {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		return body, nil
	}

	const chunkSize = 32 << 10
	var body bytes.Buffer
	chunk := make([]byte, chunkSize)
	for {
		n, err := r.Body.Read(chunk)
		if n > 0 {
			if int64(body.Len()) > maxBodyBytes-int64(n) {
				return nil, errGatewayRequestBodyTooLarge
			}
			weightedBytes, reservationErr := gatewayRequestBodyReservationBytes(int64(body.Len())+int64(n), maxBodyBytes, profile)
			if reservationErr != nil {
				return nil, reservationErr
			}
			if growErr := lease.GrowTo(weightedBytes); growErr != nil {
				return nil, growErr
			}
			_, _ = body.Write(chunk[:n])
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return body.Bytes(), nil
			}
			return nil, err
		}
	}
}

type gatewayTailBuffer struct {
	limit int
	data  []byte
}

func (b *gatewayTailBuffer) Write(p []byte) (int, error) {
	if b.limit <= 0 {
		return len(p), nil
	}
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return len(p), nil
	}
	overflow := len(b.data) + len(p) - b.limit
	if overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *gatewayTailBuffer) Len() int {
	return len(b.data)
}

func (b *gatewayTailBuffer) Bytes() []byte {
	return b.data
}

func gatewayRequestModel(body []byte) string {
	var req struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}
	return strings.TrimSpace(req.Model)
}

func gatewayRequestReasoningEffort(body []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	return normalizeGatewayReasoningEffort(gatewayReasoningEffortValue(payload))
}

func gatewayReasoningEffortValue(payload map[string]any) string {
	for _, key := range []string{"reasoning_effort", "reasoningEffort"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	if reasoning, ok := objectValue(payload, "reasoning"); ok {
		for _, key := range []string{"effort", "reasoning_effort"} {
			if value, ok := reasoning[key].(string); ok && strings.TrimSpace(value) != "" {
				return value
			}
		}
	}
	if thinking, ok := objectValue(payload, "thinking"); ok {
		for _, key := range []string{"effort", "level", "budget_tokens", "type"} {
			if value := gatewayReasoningValueToString(thinking[key]); value != "" {
				return value
			}
		}
	}
	return ""
}

func gatewayReasoningValueToString(raw any) string {
	switch v := raw.(type) {
	case string:
		return strings.TrimSpace(v)
	case float64:
		if v > 0 {
			return strconv.FormatInt(int64(v), 10)
		}
	case json.Number:
		return strings.TrimSpace(v.String())
	}
	return ""
}

func normalizeGatewayReasoningEffort(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.Trim(strings.ReplaceAll(value, "\x00", ""), " \t\r\n")
	if len(value) > 64 {
		value = value[:64]
	}
	return value
}

type gatewayUsageMetrics struct {
	ResponseModel              string
	PromptTokens               int64
	CompletionTokens           int64
	CacheCreationTokens        int64
	CacheReadTokens            int64
	InputTokensPresent         bool
	OutputTokensPresent        bool
	CacheCreationTokensPresent bool
	CacheReadTokensPresent     bool
	TotalTokens                int64
	TotalTokensPresent         bool
	UsageInvalid               bool
	Cost                       float64
	CostObserved               bool
	CostEstimated              bool
	CostSource                 string
}

type gatewayTokenComponent struct {
	value   int64
	present bool
	invalid bool
}

func mergeGatewayTokenComponent(left, right gatewayTokenComponent) gatewayTokenComponent {
	return gatewayTokenComponent{
		value:   max(left.value, right.value),
		present: left.present || right.present,
		invalid: left.invalid || right.invalid,
	}
}

func gatewayAuthHeader(c *gin.Context) string {
	auth := strings.TrimSpace(c.GetHeader("Authorization"))
	if auth != "" {
		return auth
	}
	apiKey := strings.TrimSpace(c.GetHeader("x-api-key"))
	if apiKey == "" {
		return ""
	}
	return "Bearer " + apiKey
}

func shouldParseGatewayUsage(resp *http.Response) bool {
	if resp == nil || resp.Body == nil {
		return false
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	return !strings.Contains(ct, "text/event-stream")
}

func isGatewayStreamingRequest(body []byte) bool {
	if len(bytes.TrimSpace(body)) == 0 {
		return false
	}
	var request struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &request) == nil && request.Stream
}

func isGatewayImageGenerationRequest(path string, body []byte) bool {
	if path == "/v1/images/generations" {
		return true
	}
	if path != "/v1/responses" || len(bytes.TrimSpace(body)) == 0 {
		return false
	}
	var request struct {
		Tools []struct {
			Type string `json:"type"`
		} `json:"tools"`
		// Responses API accepts either a tool-choice object or a string such
		// as "auto". Keep this field raw so a valid string does not make the
		// entire request fail to decode and hide an image_generation tool.
		ToolChoice json.RawMessage `json:"tool_choice"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return false
	}
	if len(request.ToolChoice) > 0 {
		var toolChoice struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(request.ToolChoice, &toolChoice); err == nil && strings.EqualFold(strings.TrimSpace(toolChoice.Type), "image_generation") {
			return true
		}
	}
	for _, tool := range request.Tools {
		if strings.EqualFold(strings.TrimSpace(tool.Type), "image_generation") {
			return true
		}
	}
	return false
}

func parseGatewayUsageMetrics(raw []byte) gatewayUsageMetrics {
	return parseGatewayUsageMetricsForPath(raw, "")
}

func parseGatewayUsageMetricsForPath(raw []byte, requestPath string, secrets ...string) gatewayUsageMetrics {
	var out gatewayUsageMetrics
	if len(bytes.TrimSpace(raw)) == 0 {
		return out
	}
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return out
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return out
	}

	usage := gatewayUsageObject(root)
	out.ResponseModel = gatewayResponseModelFromObject(root)
	out = sanitizeGatewayUsageModel(out, secrets...)
	input := gatewayTokenComponentFields(usage, "prompt_tokens", "input_tokens")
	cacheCreation := gatewayTokenComponentFields(usage,
		"cache_creation_input_tokens",
		"cache_creation_tokens",
		"cache_write_input_tokens",
		"cache_write_tokens",
		"cached_creation_tokens",
	)
	cacheRead := gatewayTokenComponentFields(usage,
		"cache_read_input_tokens",
		"cache_read_tokens",
		"cached_tokens",
	)
	inputTokensIncludeCache := gatewayInputTokensIncludeCache(root, usage, requestPath)
	for _, field := range []string{"input_tokens_details", "prompt_tokens_details"} {
		if details, ok := objectValue(usage, field); ok {
			cacheCreation = mergeGatewayTokenComponent(cacheCreation, gatewayTokenComponentFields(details,
				"cache_creation_input_tokens",
				"cache_creation_tokens",
				"cache_write_input_tokens",
				"cache_write_tokens",
				"cached_creation_tokens",
			))
			cacheRead = mergeGatewayTokenComponent(cacheRead, gatewayTokenComponentFields(details,
				"cache_read_input_tokens",
				"cache_read_tokens",
				"cached_tokens",
			))
		}
	}
	output := gatewayTokenComponentFields(usage, "completion_tokens", "output_tokens")
	total := gatewayTokenComponentFields(usage, "total_tokens")
	out.PromptTokens = input.value
	out.CompletionTokens = output.value
	out.CacheCreationTokens = cacheCreation.value
	out.CacheReadTokens = cacheRead.value
	out.InputTokensPresent = input.present
	out.OutputTokensPresent = output.present
	out.CacheCreationTokensPresent = cacheCreation.present
	out.CacheReadTokensPresent = cacheRead.present
	out.TotalTokens = total.value
	out.TotalTokensPresent = total.present
	out.UsageInvalid = input.invalid || output.invalid || cacheCreation.invalid || cacheRead.invalid || total.invalid
	// Anthropic reports uncached input separately; OpenAI input/prompt tokens already include cache details.
	if !inputTokensIncludeCache {
		if promptTokens, ok := addGatewayTokenCounts(out.PromptTokens, out.CacheCreationTokens, out.CacheReadTokens); ok {
			out.PromptTokens = promptTokens
		} else {
			out.UsageInvalid = true
		}
	}
	if !out.TotalTokensPresent && !total.invalid {
		if totalTokens, ok := addGatewayTokenCounts(out.PromptTokens, out.CompletionTokens); ok {
			out.TotalTokens = totalTokens
		} else {
			out.UsageInvalid = true
		}
	}

	for _, source := range []struct {
		fields []string
		data   map[string]any
	}{
		{fields: []string{"cost", "total_cost", "cost_usd", "actual_cost"}, data: usage},
		{fields: []string{"cost", "total_cost", "cost_usd", "actual_cost"}, data: root},
	} {
		for _, field := range source.fields {
			if cost, ok := float64Value(source.data, field); ok && cost >= 0 {
				out.Cost = cost
				out.CostObserved = true
				out.CostSource = field
				return out
			}
		}
	}
	for _, source := range []map[string]any{usage, root} {
		if cost, ok := float64Value(source, "estimated_cost"); ok && cost >= 0 {
			out.Cost = cost
			out.CostObserved = true
			out.CostEstimated = true
			out.CostSource = "estimated_cost"
			return out
		}
	}
	return out
}

func parseGatewayStreamUsageMetrics(raw []byte) gatewayUsageMetrics {
	return parseGatewayStreamUsageMetricsForPath(raw, "")
}

func parseGatewayStreamUsageMetricsForPath(raw []byte, requestPath string, secrets ...string) gatewayUsageMetrics {
	var out gatewayUsageMetrics
	var eventData [][]byte
	flush := func() {
		if len(eventData) == 0 {
			return
		}
		payload := bytes.TrimSpace(bytes.Join(eventData, []byte("\n")))
		eventData = eventData[:0]
		if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
			return
		}
		out = mergeGatewayUsageMetrics(out, parseGatewayUsageMetricsForPath(payload, requestPath, secrets...))
	}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			flush()
			continue
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		eventData = append(eventData, append([]byte(nil), payload...))
	}
	flush()
	return out
}

func gatewayInputTokensIncludeCache(root map[string]any, usage map[string]any, requestPath string) bool {
	if requestPath != "" {
		return requestPath != "/v1/messages" && requestPath != "/v1/messages/count_tokens"
	}
	if _, ok := usage["prompt_tokens"]; ok {
		return true
	}
	for _, field := range []string{"input_tokens_details", "prompt_tokens_details"} {
		if _, ok := objectValue(usage, field); ok {
			return true
		}
	}
	_, nestedResponse := objectValue(root, "response")
	return nestedResponse
}

func mergeGatewayUsageMetrics(a gatewayUsageMetrics, b gatewayUsageMetrics) gatewayUsageMetrics {
	if b.ResponseModel != "" {
		a.ResponseModel = b.ResponseModel
	}
	if b.PromptTokens > a.PromptTokens {
		a.PromptTokens = b.PromptTokens
	}
	if b.CompletionTokens > a.CompletionTokens {
		a.CompletionTokens = b.CompletionTokens
	}
	if b.CacheCreationTokens > a.CacheCreationTokens {
		a.CacheCreationTokens = b.CacheCreationTokens
	}
	if b.CacheReadTokens > a.CacheReadTokens {
		a.CacheReadTokens = b.CacheReadTokens
	}
	a.InputTokensPresent = a.InputTokensPresent || b.InputTokensPresent
	a.OutputTokensPresent = a.OutputTokensPresent || b.OutputTokensPresent
	a.CacheCreationTokensPresent = a.CacheCreationTokensPresent || b.CacheCreationTokensPresent
	a.CacheReadTokensPresent = a.CacheReadTokensPresent || b.CacheReadTokensPresent
	a.TotalTokensPresent = a.TotalTokensPresent || b.TotalTokensPresent
	a.UsageInvalid = a.UsageInvalid || b.UsageInvalid
	if b.TotalTokens > a.TotalTokens {
		a.TotalTokens = b.TotalTokens
	}
	if !a.TotalTokensPresent {
		if totalTokens, ok := addGatewayTokenCounts(a.PromptTokens, a.CompletionTokens); ok {
			a.TotalTokens = totalTokens
		} else {
			a.UsageInvalid = true
		}
	}
	if b.CostObserved && (!a.CostObserved || (a.CostEstimated && !b.CostEstimated) || a.CostEstimated == b.CostEstimated) {
		a.Cost = b.Cost
		a.CostObserved = b.CostObserved
		a.CostEstimated = b.CostEstimated
		a.CostSource = b.CostSource
	}
	return a
}

func applyGatewayRequestPricingUsage(log *storage.GatewayRequestLog, metrics gatewayUsageMetrics, imageRequest bool) {
	if log == nil {
		return
	}
	if model := gatewayResponseModel(metrics.ResponseModel); model != "" {
		log.UpstreamResponseModel = model
	}
	if imageRequest {
		return
	}

	usage := storage.GatewayRequestPricingUsage{}
	switch log.Path {
	case "/v1/embeddings":
		log.PromptTokens = metrics.PromptTokens
		log.CompletionTokens = 0
		log.CacheCreationTokens = 0
		log.CacheReadTokens = 0
		log.TotalTokens = metrics.TotalTokens
		if metrics.InputTokensPresent {
			inputTokens := metrics.PromptTokens
			usage.InputTokens = &inputTokens
		}
	case "/v1/responses", "/v1/chat/completions", "/v1/completions", "/v1/messages":
		log.PromptTokens = metrics.PromptTokens
		log.CompletionTokens = metrics.CompletionTokens
		log.CacheCreationTokens = metrics.CacheCreationTokens
		log.CacheReadTokens = metrics.CacheReadTokens
		log.TotalTokens = metrics.TotalTokens
		if metrics.InputTokensPresent {
			if inputTokens, ok := subtractGatewayTokenCounts(metrics.PromptTokens, metrics.CacheCreationTokens, metrics.CacheReadTokens); ok {
				usage.InputTokens = &inputTokens
			} else {
				metrics.UsageInvalid = true
			}
		}
		if metrics.OutputTokensPresent {
			outputTokens := metrics.CompletionTokens
			usage.OutputTokens = &outputTokens
		}
		if metrics.CacheReadTokensPresent {
			cacheReadTokens := metrics.CacheReadTokens
			usage.CacheReadTokens = &cacheReadTokens
		}
		if metrics.CacheCreationTokensPresent {
			cacheWriteTokens := metrics.CacheCreationTokens
			usage.CacheWriteTokens = &cacheWriteTokens
		}
	default:
		return
	}

	if metrics.UsageInvalid {
		markGatewayRequestUsageInvalid(log, usage, metrics)
		return
	}
	if err := storage.SetGatewayRequestPricingUsage(log, usage); err == nil {
		return
	}
	markGatewayRequestUsageInvalid(log, usage, metrics)
}

func markGatewayRequestUsageInvalid(log *storage.GatewayRequestLog, usage storage.GatewayRequestPricingUsage, metrics gatewayUsageMetrics) {
	if log == nil {
		return
	}
	if log.Path == "/v1/embeddings" {
		log.UsageInputPresent = metrics.InputTokensPresent
		log.UsageOutputPresent = false
		log.UsageCacheReadPresent = false
		log.UsageCacheWritePresent = false
	} else {
		log.UsageInputPresent = metrics.InputTokensPresent || usage.InputTokens != nil
		log.UsageOutputPresent = metrics.OutputTokensPresent || usage.OutputTokens != nil
		log.UsageCacheReadPresent = metrics.CacheReadTokensPresent || usage.CacheReadTokens != nil
		log.UsageCacheWritePresent = metrics.CacheCreationTokensPresent || usage.CacheWriteTokens != nil
	}
	if log.CostStatus == storage.GatewayCostStatusUnavailable && log.CostUnavailableReason != "" &&
		log.CostUnavailableReason != storage.GatewayPricingUnavailableUsageInvalid {
		return
	}
	if metrics.CostObserved && !metrics.CostEstimated {
		log.CostUnavailableReason = ""
		return
	}
	log.Cost = 0
	log.RawCost = nil
	log.CostEstimated = false
	log.CostSource = ""
	log.CostStatus = storage.GatewayCostStatusUnavailable
	log.CostUnavailableReason = storage.GatewayPricingUnavailableUsageInvalid
}

func applyGatewayRequestUsageAndCost(log *storage.GatewayRequestLog, metrics gatewayUsageMetrics, imageRequest bool, fundingFactor float64) {
	if log == nil {
		return
	}
	if metrics.CostObserved && log.CostStatus != storage.GatewayCostStatusUnavailable {
		rawCost := metrics.Cost
		actualCost := rawCost
		if converted, err := costing.Actual(rawCost, fundingFactor); err == nil {
			actualCost = converted
		}
		log.Cost = actualCost
		log.RawCost = optionalCostPointer(rawCost, true)
		log.CostEstimated = metrics.CostEstimated
		log.CostSource = metrics.CostSource
	}
	applyGatewayRequestPricingUsage(log, metrics, imageRequest)
}

func subtractGatewayTokenCounts(total int64, values ...int64) (int64, bool) {
	if total < 0 {
		return 0, false
	}
	for _, value := range values {
		if value < 0 || value > total {
			return 0, false
		}
		total -= value
	}
	return total, true
}

func estimateGatewayCost(metrics gatewayUsageMetrics, promptRatio float64, completionRatio float64) gatewayUsageMetrics {
	if metrics.CostObserved || metrics.UsageInvalid || promptRatio <= 0 || math.IsNaN(promptRatio) || math.IsInf(promptRatio, 0) {
		return metrics
	}
	if completionRatio <= 0 || math.IsNaN(completionRatio) || math.IsInf(completionRatio, 0) {
		completionRatio = promptRatio
	}
	// The ratio fallback is used only when an immutable price fact is not
	// available. Cache read/write prices cannot be recovered from the mixed
	// request total, so use the industry-default 0.1x multiplier for cache
	// tokens. The price-fact path remains authoritative when independent cache
	// unit prices are available.
	promptTokens := metrics.PromptTokens
	if promptTokens < 0 {
		promptTokens = 0
	}
	cacheCreationTokens := metrics.CacheCreationTokens
	if cacheCreationTokens < 0 {
		cacheCreationTokens = 0
	}
	cacheReadTokens := metrics.CacheReadTokens
	if cacheReadTokens < 0 {
		cacheReadTokens = 0
	}
	completionTokens := metrics.CompletionTokens
	if completionTokens < 0 {
		completionTokens = 0
	}
	totalTokens := metrics.TotalTokens
	if totalTokens < 0 {
		totalTokens = 0
	}
	uncachedPromptTokens := promptTokens
	if cacheCreationTokens >= uncachedPromptTokens {
		uncachedPromptTokens = 0
	} else {
		uncachedPromptTokens -= cacheCreationTokens
	}
	if cacheReadTokens >= uncachedPromptTokens {
		uncachedPromptTokens = 0
	} else {
		uncachedPromptTokens -= cacheReadTokens
	}
	var quota float64
	if promptTokens == 0 && completionTokens == 0 && cacheCreationTokens == 0 && cacheReadTokens == 0 {
		quota = float64(totalTokens) * promptRatio
	} else {
		quota = float64(uncachedPromptTokens)*promptRatio +
			(float64(cacheCreationTokens)+float64(cacheReadTokens))*promptRatio*gatewayFallbackCacheInputPriceMultiplier +
			float64(completionTokens)*completionRatio
	}
	if math.IsNaN(quota) || math.IsInf(quota, 0) {
		return metrics
	}
	if quota <= 0 {
		return metrics
	}
	cost := quota / gatewayDefaultQuotaPerUSD
	if cost <= 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return metrics
	}
	metrics.Cost = cost
	metrics.CostObserved = true
	metrics.CostEstimated = true
	metrics.CostSource = "group_ratio_default_quota"
	return metrics
}

func gatewayUsageObject(root map[string]any) map[string]any {
	if nested, ok := objectValue(root, "usage"); ok {
		return nested
	}
	for _, key := range []string{"response", "message"} {
		if nested, ok := objectValue(root, key); ok {
			if usage, ok := objectValue(nested, "usage"); ok {
				return usage
			}
			return nested
		}
	}
	return root
}

func objectValue(data map[string]any, key string) (map[string]any, bool) {
	raw, ok := data[key]
	if !ok {
		return nil, false
	}
	value, ok := raw.(map[string]any)
	return value, ok
}

func int64Value(data map[string]any, key string) int64 {
	value, ok := nonNegativeInt64Field(data, key)
	if !ok {
		return 0
	}
	return value
}

func maxInt64Fields(data map[string]any, fields ...string) int64 {
	var result int64
	for _, field := range fields {
		if value := int64Value(data, field); value > result {
			result = value
		}
	}
	return result
}

func nonNegativeInt64FieldPresent(data map[string]any, fields ...string) bool {
	for _, field := range fields {
		if _, ok := nonNegativeInt64Field(data, field); ok {
			return true
		}
	}
	return false
}

func gatewayTokenComponentFields(data map[string]any, fields ...string) gatewayTokenComponent {
	component := gatewayTokenComponent{}
	for _, field := range fields {
		if _, ok := data[field]; !ok {
			continue
		}
		value, ok := nonNegativeInt64Field(data, field)
		if !ok {
			component.invalid = true
			continue
		}
		component.present = true
		component.value = max(component.value, value)
	}
	return component
}

func addGatewayTokenCounts(values ...int64) (int64, bool) {
	var total int64
	for _, value := range values {
		if value < 0 || total > math.MaxInt64-value {
			return 0, false
		}
		total += value
	}
	return total, true
}

func nonNegativeInt64Field(data map[string]any, field string) (int64, bool) {
	raw, ok := data[field]
	if !ok {
		return 0, false
	}
	switch value := raw.(type) {
	case json.Number:
		parsed, err := strconv.ParseInt(strings.TrimSpace(value.String()), 10, 64)
		return parsed, err == nil && parsed >= 0
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		return parsed, err == nil && parsed >= 0
	case int:
		return int64(value), value >= 0
	case int64:
		return value, value >= 0
	case float64:
		const maxExactlyRepresentedInteger = float64(1 << 53)
		if value < 0 || value > maxExactlyRepresentedInteger || math.Trunc(value) != value || math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, false
		}
		return int64(value), true
	case float32:
		const maxExactlyRepresentedInteger = float32(1 << 24)
		if value < 0 || value > maxExactlyRepresentedInteger || float32(math.Trunc(float64(value))) != value || math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}

func float64Value(data map[string]any, key string) (float64, bool) {
	raw, ok := data[key]
	if !ok {
		return 0, false
	}
	var value float64
	var err error
	switch v := raw.(type) {
	case float64:
		value = v
	case float32:
		value = float64(v)
	case int:
		value = float64(v)
	case int64:
		value = float64(v)
	case json.Number:
		value, err = v.Float64()
	case string:
		value, err = strconv.ParseFloat(strings.TrimSpace(v), 64)
	default:
		return 0, false
	}
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return value, true
}

func supportedGatewayPath(path string) bool {
	switch path {
	case "/v1/chat/completions",
		"/v1/completions",
		"/v1/embeddings",
		"/v1/responses",
		"/v1/messages",
		"/v1/messages/count_tokens",
		"/v1/images/generations":
		return true
	default:
		return false
	}
}

func gatewayAppAllowsPath(app storage.GatewayApp, path string) bool {
	switch app {
	case storage.GatewayAppClaude:
		return path == "/v1/messages" || path == "/v1/messages/count_tokens"
	case storage.GatewayAppCodex, storage.GatewayAppGemini:
		return path == "/v1/chat/completions" ||
			path == "/v1/completions" ||
			path == "/v1/embeddings" ||
			path == "/v1/responses" ||
			path == "/v1/images/generations"
	default:
		return false
	}
}

func gatewayAppAllowsModel(app storage.GatewayApp, model string) bool {
	family := gatewayModelFamilyFromName(model)
	return gatewayFamilySupportsApp(family, app)
}

func gatewayModelFamilyAllowsModel(modelFamily string, app storage.GatewayApp, model string) bool {
	modelFamily = strings.ToLower(strings.TrimSpace(modelFamily))
	if modelFamily == "" {
		return gatewayAppAllowsModel(app, model)
	}
	requestedFamily := strings.ToLower(strings.TrimSpace(gatewayModelFamilyFromName(model)))
	if requestedFamily == "" {
		return true
	}
	return requestedFamily == modelFamily
}

func resolveGatewayTarget(ctx context.Context, d *Deps, channel storage.Channel) (gatewayResolvedTarget, error) {
	target := gatewayResolvedTarget{SiteURL: channel.SiteURL}
	type resolver interface {
		Resolve(context.Context, *storage.Channel) (*connector.Channel, error)
	}
	if d != nil {
		if svc, ok := d.ChannelSvc.(resolver); ok {
			resolved, err := svc.Resolve(ctx, &channel)
			if err != nil {
				return target, err
			}
			if strings.TrimSpace(resolved.SiteURL) != "" {
				target.SiteURL = resolved.SiteURL
			}
			target.ProxyURL = strings.TrimSpace(resolved.ProxyURL)
			target.ProxyCacheKey = resolved.ProxyCacheKey
		}
	}
	return target, nil
}

func resolveGatewayCandidateTarget(ctx context.Context, d *Deps, candidate gateway.Candidate) (gatewayResolvedTarget, error) {
	target, err := resolveGatewayTarget(ctx, d, candidate.Channel)
	if err != nil {
		return target, err
	}
	if candidate.Channel.CredentialMode == storage.CredentialModeDirect && strings.TrimSpace(candidate.DirectBaseURL) != "" {
		target.SiteURL = strings.TrimRight(strings.TrimSpace(candidate.DirectBaseURL), "/")
	}
	return target, nil
}

func forwardGatewayRequest(in *http.Request, target gatewayResolvedTarget, upstreamKey string, body []byte) (*http.Response, error) {
	return forwardGatewayRequestOnDispatch(in, target, upstreamKey, body, nil, nil, nil)
}

// gatewayDispatchRoundTripper records the deterministic transport-entry
// boundary: net/http.Client has accepted the request and this wrapper has
// completed method, URL, header and context validation immediately before the
// configured RoundTripper. This does not assert that socket bytes were flushed
// or that the upstream received or accepted the request.
type gatewayDispatchRoundTripper struct {
	base                 http.RoundTripper
	dispatch             *gatewayTransportDispatchFence
	onDispatch           func() error
	onTransportCompleted func() error
}

type gatewayTransportDispatchFence struct {
	service        *gateway.Service
	fence          gateway.UpstreamDispatchFence
	beforeDispatch func(context.Context) error
}

type gatewayTransportConfirmationFailure struct {
	StatusCode       int
	Header           http.Header
	ResponseObserved bool
	TransportErr     error
	ConfirmErr       error
}

func (e *gatewayTransportConfirmationFailure) Error() string {
	if e == nil {
		return errGatewayRequestTransportConfirm.Error()
	}
	return fmt.Sprintf("%s: %v", errGatewayRequestTransportConfirm, e.ConfirmErr)
}

func (e *gatewayTransportConfirmationFailure) Unwrap() []error {
	if e == nil {
		return []error{errGatewayRequestTransportConfirm}
	}
	errs := []error{errGatewayRequestTransportConfirm}
	if e.ConfirmErr != nil {
		errs = append(errs, e.ConfirmErr)
	}
	if e.TransportErr != nil {
		errs = append(errs, e.TransportErr)
	}
	return errs
}

func (t gatewayDispatchRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if err := validateGatewayTransportEntryRequest(request); err != nil {
		closeGatewayTransportRequestBody(request)
		return nil, err
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	var response *http.Response
	var transportErr error
	transportStarted := false
	if t.dispatch != nil {
		if t.dispatch.service == nil {
			closeGatewayTransportRequestBody(request)
			return nil, errors.New("upstream dispatch fence is unavailable")
		}
		beforeDispatch := func(ctx context.Context) error {
			if t.dispatch.beforeDispatch != nil {
				if err := t.dispatch.beforeDispatch(ctx); err != nil {
					return err
				}
			}
			transportStarted = true
			return nil
		}
		response, transportErr = t.dispatch.service.RoundTripWithUpstreamDispatchFence(
			request.Context(), t.dispatch.fence, beforeDispatch, request, base,
		)
	} else {
		if t.onDispatch != nil {
			if err := t.onDispatch(); err != nil {
				closeGatewayTransportRequestBody(request)
				return nil, err
			}
		}
		transportStarted = true
		response, transportErr = base.RoundTrip(request)
	}
	if !transportStarted || t.onTransportCompleted == nil {
		return response, transportErr
	}
	if confirmErr := t.onTransportCompleted(); confirmErr != nil {
		failure := &gatewayTransportConfirmationFailure{TransportErr: transportErr, ConfirmErr: confirmErr}
		if response != nil {
			failure.StatusCode = response.StatusCode
			failure.Header = response.Header.Clone()
			failure.ResponseObserved = true
		}
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, failure
	}
	return response, transportErr
}

func closeGatewayTransportRequestBody(request *http.Request) {
	if request != nil && request.Body != nil {
		_ = request.Body.Close()
	}
}

func validateGatewayTransportEntryRequest(request *http.Request) error {
	if request == nil || request.URL == nil {
		return errors.New("gateway transport request is unavailable")
	}
	if err := request.Context().Err(); err != nil {
		return err
	}
	if request.RequestURI != "" {
		return errors.New("upstream client request URI must be empty")
	}
	if request.Method != "" && !httpguts.ValidHeaderFieldName(request.Method) {
		return fmt.Errorf("invalid upstream HTTP method %q", request.Method)
	}
	if request.Header == nil {
		return errors.New("upstream request headers are unavailable")
	}
	if request.URL.Scheme != "http" && request.URL.Scheme != "https" {
		return fmt.Errorf("unsupported upstream URL scheme %q", request.URL.Scheme)
	}
	if strings.TrimSpace(request.URL.Host) == "" {
		return errors.New("upstream URL host is empty")
	}
	host := request.Host
	if host == "" {
		host = request.URL.Host
	}
	if !httpguts.ValidHostHeader(host) {
		return errors.New("invalid upstream Host header")
	}
	for name, values := range request.Header {
		if !httpguts.ValidHeaderFieldName(name) {
			return fmt.Errorf("invalid upstream header field name %q", name)
		}
		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return fmt.Errorf("invalid upstream header field value for %q", name)
			}
		}
	}
	for name, values := range request.Trailer {
		if !httpguts.ValidHeaderFieldName(name) {
			return fmt.Errorf("invalid upstream trailer field name %q", name)
		}
		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return fmt.Errorf("invalid upstream trailer field value for %q", name)
			}
		}
	}
	return nil
}

func forwardGatewayRequestOnDispatch(in *http.Request, target gatewayResolvedTarget, upstreamKey string, body []byte, prepareDispatch, markPhysicalDispatch, confirmTransportCompleted func() error) (*http.Response, error) {
	return forwardGatewayRequestWithTransport(
		in, target, upstreamKey, body, prepareDispatch, nil, markPhysicalDispatch, confirmTransportCompleted,
	)
}

func forwardGatewayRequestWithDispatchFence(in *http.Request, target gatewayResolvedTarget, upstreamKey string, body []byte, dispatch *gatewayTransportDispatchFence, confirmTransportCompleted func() error) (*http.Response, error) {
	return forwardGatewayRequestWithTransport(
		in, target, upstreamKey, body, nil, dispatch, nil, confirmTransportCompleted,
	)
}

func forwardGatewayRequestWithTransport(in *http.Request, target gatewayResolvedTarget, upstreamKey string, body []byte, prepareDispatch func() error, dispatch *gatewayTransportDispatchFence, markPhysicalDispatch, confirmTransportCompleted func() error) (*http.Response, error) {
	targetURL := strings.TrimRight(target.SiteURL, "/") + in.URL.Path
	if in.URL.RawQuery != "" {
		targetURL += "?" + in.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(in.Context(), in.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	// A POST carrying Idempotency-Key is considered replayable by Go's transport
	// when GetBody is available. Every physical send needs its own durable
	// dispatch audit, so implicit transport replay is forbidden for every route.
	req.GetBody = nil
	copyGatewayRequestHeaders(req.Header, in.Header)
	req.Header.Del(gatewaySessionHeader)
	req.Header.Del(legacyGatewaySessionHeader)
	req.Header.Del(gatewayDownstreamKeyTestHeader)
	// Administrator cookies are scoped to BridgeOps and must never cross the
	// proxy boundary to an upstream channel.
	req.Header.Del("Cookie")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(upstreamKey))
	req.Header.Set("x-api-key", strings.TrimSpace(upstreamKey))
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	client, err := gatewayClientForProxyWithCacheKey(target.ProxyURL, target.ProxyCacheKey)
	if err != nil {
		return nil, err
	}
	if err := in.Context().Err(); err != nil {
		return nil, err
	}
	if prepareDispatch != nil {
		if err := prepareDispatch(); err != nil {
			return nil, err
		}
	}
	if err := in.Context().Err(); err != nil {
		return nil, err
	}
	clientCopy := *client
	baseTransport := gatewaySingleAttemptTransportForRequest(clientCopy.Transport, req)
	clientCopy.Transport = baseTransport
	if dispatch != nil || markPhysicalDispatch != nil {
		clientCopy.Transport = gatewayDispatchRoundTripper{
			base:                 baseTransport,
			dispatch:             dispatch,
			onDispatch:           markPhysicalDispatch,
			onTransportCompleted: confirmTransportCompleted,
		}
	}
	return netproxy.DoWithoutRedirects(&clientCopy, req)
}

func gatewaySingleAttemptTransportForRequest(base http.RoundTripper, request *http.Request) http.RoundTripper {
	if request != nil && (request.Body == nil || request.Body == http.NoBody) {
		return gatewaySingleAttemptEmptyBodyTransport(base)
	}
	return base
}

// gatewaySingleAttemptEmptyBodyTransport preserves the ordinary zero-length
// wire representation while preventing net/http from replaying a Body==NoBody
// request on a reused connection. A fresh, keepalive-disabled HTTP/1 transport
// cannot inherit an idle connection and therefore does not need the previous
// protocol-changing unknown-length/chunked empty body workaround.
func gatewaySingleAttemptEmptyBodyTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	transport, ok := base.(*http.Transport)
	if !ok {
		// A custom RoundTripper is invoked exactly once by our wrapper. Its own
		// internal retry contract is outside net/http.Transport's replay rules.
		return base
	}
	clone := transport.Clone()
	clone.DisableKeepAlives = true
	clone.ForceAttemptHTTP2 = false
	clone.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	return clone
}

// gatewayCandidateAllowsApp learns model facts and processes explicit
// authentication evidence from one capability probe. Those effects have
// different lifecycles: model facts are fenced by ModelFactGeneration, while a
// 401/403 can only mutate the exact KeyStateGeneration that sent the request.
func gatewayCandidateAllowsApp(ctx context.Context, d *Deps, candidate gateway.Candidate, keyLease gateway.UpstreamKeyLease, target gatewayResolvedTarget, upstreamKey string, app storage.GatewayApp, modelFactGenerations []storage.UpstreamGroupGeneration, dispatchFence gateway.UpstreamDispatchFence) (bool, bool) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		return false, true
	}
	if candidate.Channel.CredentialMode == storage.CredentialModeDirect {
		// Direct protocol and models are administrator-owned per-group policy.
		// Never replace that allowlist with a provider-wide /v1/models response.
		return true, normalizeDirectGatewayApp(candidate.Channel.GatewayApp) == normalizeDirectGatewayApp(app) && len(candidate.ModelIDs) > 0
	}
	channelID := candidate.Channel.ID
	expectedIdentityRevision := candidate.Channel.UpstreamAccountIdentityRevision
	groupName := candidate.Rate.ModelName
	var modelFence storage.GatewayModelFactFence
	if generation, ok := gatewaySingleModelFactGeneration(modelFactGenerations); ok {
		modelFence = storage.NewGatewayModelFactFence(generation, expectedIdentityRevision)
	}
	if cached, err := d.Gateway.Store.FindUpstreamKey(channelID, groupName); err == nil && modelFence.MatchesUpstreamKey(cached) {
		if families := gatewayFamilySet(cached.ModelFamilies); len(families) > 0 {
			ok := false
			for family := range families {
				if gatewayFamilySupportsCandidate(family, app) {
					ok = true
					break
				}
			}
			return true, ok
		}
	}
	families, modelIDs, err := probeGatewayModelFamilies(
		ctx, target, upstreamKey,
		&gatewayTransportDispatchFence{service: d.Gateway, fence: dispatchFence},
	)
	if err != nil {
		if connector.IsAccountBalanceHTTPError(err) {
			if d.Channels != nil {
				_ = d.Channels.MarkUpstreamAccountBalanceInsufficient(channelID, expectedIdentityRevision, time.Now().UTC())
			}
		} else if isGatewayProbeAuthError(err) {
			invalidateGatewayCandidateUpstreamKey(ctx, d, candidate, keyLease, groupName)
		}
		if modelFence.Valid() {
			updateGatewayModelFamiliesForIdentity(d, modelFence, nil, nil, err.Error())
		}
		return false, true
	}
	if modelFence.Valid() {
		updateGatewayModelFamiliesForIdentity(d, modelFence, families, modelIDs, "")
	}
	if len(families) == 0 {
		return false, true
	}
	ok := false
	for family := range gatewayFamilySet(strings.Join(families, ",")) {
		if gatewayFamilySupportsCandidate(family, app) {
			ok = true
			break
		}
	}
	return true, ok
}

func probeGatewayModelFamilies(ctx context.Context, target gatewayResolvedTarget, upstreamKey string, dispatch *gatewayTransportDispatchFence) ([]string, []string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/v1/models", nil)
	if err != nil {
		return nil, nil, err
	}
	// Model capability probing is authenticated traffic. Keep credentials and
	// the probe request on the configured origin; a redirect target is not an
	// approved upstream and must never receive the upstream key.
	resp, err := forwardGatewayRequestWithDispatchFence(req, target, upstreamKey, nil, dispatch, nil)
	if err != nil {
		return nil, nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, gatewayModelProbeResponseLimit+1))
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, nil, readErr
	}
	if len(raw) > gatewayModelProbeResponseLimit {
		return nil, nil, errors.New("models response exceeded 1 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		redacted := []byte(strings.ReplaceAll(string(raw), strings.TrimSpace(upstreamKey), "[redacted]"))
		return nil, nil, fmt.Errorf("models probe: %w", connector.HTTPStatusError(resp.StatusCode, redacted))
	}
	var parsed struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, nil, err
	}
	return gatewayModelFamiliesFromItems(parsed.Data), gatewayModelIDsFromItems(parsed.Data), nil
}

func isGatewayProbeAuthError(err error) bool {
	return connector.IsAuthHTTPError(err) && !connector.IsUpstreamChallengeHTTPError(err) && !connector.IsAccountBalanceHTTPError(err)
}

func sanitizeGatewayProbeError(raw []byte, upstreamKey string) string {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return ""
	}
	text = strings.ToValidUTF8(text, "�")
	if upstreamKey != "" {
		text = strings.ReplaceAll(text, upstreamKey, "[redacted]")
	}
	text = gatewayCredentialURLPattern.ReplaceAllString(text, `${1}[redacted]@`)
	text = gatewayBearerPattern.ReplaceAllString(text, `${1}[redacted]`)
	text = gatewayCredentialQueryPattern.ReplaceAllString(text, `${1}[redacted]`)
	text = gatewayCredentialHeaderPattern.ReplaceAllString(text, "[redacted-header]")
	text = gatewaySecretPattern.ReplaceAllString(text, "[redacted]")
	return truncateGatewayText(text, 512)
}

func truncateGatewayText(text string, limit int) string {
	text = strings.ToValidUTF8(strings.TrimSpace(text), "�")
	if limit <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	if limit <= 3 {
		return string(runes[:limit])
	}
	return string(runes[:limit-3]) + "..."
}

var (
	gatewayCredentialURLPattern    = regexp.MustCompile(`(?i)(https?://)[^/@\s]+:[^/@\s]+@`)
	gatewayCredentialQueryPattern  = regexp.MustCompile(`(?i)([?&](?:api[_-]?key|token|password|secret|key)=)[^&\s]+`)
	gatewayCredentialHeaderPattern = regexp.MustCompile(`(?i)\b(authorization|x-api-key|api-key|api_key|cookie|token|password|secret)\s*[:=]\s*["']?[^\s,;"'}]+`)
	gatewayBearerPattern           = regexp.MustCompile(`(?i)\b(Bearer\s+|Basic\s+)[^\s,;"'}]+`)
	gatewaySecretPattern           = regexp.MustCompile(`(?i)(sk-[a-z0-9._=-]{8,}|sk-upops-[a-z0-9._=-]{8,}|[a-z0-9_-]{32,})`)
)

func updateGatewayModelFamilies(d *Deps, channelID uint, groupName string, families []string, modelIDs []string, syncErr string, generations ...storage.UpstreamGroupGeneration) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		return
	}
	generation, ok := gatewaySingleModelFactGeneration(generations)
	if !ok {
		return
	}
	if strings.TrimSpace(syncErr) != "" {
		_ = d.Gateway.Store.SetUpstreamKeyModelsError(channelID, strings.TrimSpace(groupName), time.Now(), sanitizeGatewayProbeError([]byte(syncErr), ""), generation)
		return
	}
	_ = d.Gateway.Store.UpdateUpstreamKeyModelFamilies(channelID, strings.TrimSpace(groupName), strings.Join(families, ","), strings.Join(modelIDs, ","), time.Now(), "", generation)
}

func updateGatewayModelFamiliesForIdentity(d *Deps, fence storage.GatewayModelFactFence, families []string, modelIDs []string, syncErr string) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil || !fence.Valid() {
		return
	}
	if strings.TrimSpace(syncErr) != "" {
		_ = d.Gateway.Store.SetUpstreamKeyModelsErrorForIdentity(fence, time.Now(), sanitizeGatewayProbeError([]byte(syncErr), ""))
		return
	}
	_ = d.Gateway.Store.UpdateUpstreamKeyModelFamiliesForIdentity(fence, strings.Join(families, ","), strings.Join(modelIDs, ","), time.Now(), "")
}

func directGatewayModelConfigured(d *Deps, downstream *storage.GatewayDownstreamKey, model string) bool {
	if d == nil || d.Channels == nil || downstream == nil {
		return false
	}
	app := normalizeDirectGatewayApp(downstream.App)
	model = strings.TrimSpace(model)
	if model == "" {
		return false
	}
	if !downstream.SmartRouting {
		if downstream.ManualChannelID == 0 {
			return false
		}
		channel, err := d.Channels.FindByID(downstream.ManualChannelID)
		return err == nil && directGatewayProjectedRouteAllowsModel(d, *channel, downstream.ManualGroupName, app, model)
	}

	var allowedRoutes map[string]struct{}
	var channelIDs []uint
	if downstream.CandidateGroupID > 0 {
		if d.Gateway == nil || d.Gateway.Store == nil {
			return false
		}
		group, err := d.Gateway.Store.FindCandidateGroup(downstream.CandidateGroupID)
		if err != nil {
			return false
		}
		allowedRoutes = make(map[string]struct{}, len(group.Members))
		seenChannels := make(map[uint]struct{}, len(group.Members))
		for _, member := range group.Members {
			allowedRoutes[directGatewayRouteScopeKey(member.ChannelID, member.GroupName)] = struct{}{}
			if _, seen := seenChannels[member.ChannelID]; !seen {
				seenChannels[member.ChannelID] = struct{}{}
				channelIDs = append(channelIDs, member.ChannelID)
			}
		}
	}

	var channels []storage.Channel
	var err error
	if allowedRoutes == nil {
		channels, err = d.Channels.List()
	} else {
		channels, err = d.Channels.ListByIDs(channelIDs)
	}
	if err != nil {
		return false
	}
	if d.Gateway == nil || d.Gateway.Store == nil {
		return false
	}
	channelByID := make(map[uint]storage.Channel, len(channels))
	ids := make([]uint, 0, len(channels))
	for _, channel := range channels {
		channelByID[channel.ID] = channel
		ids = append(ids, channel.ID)
	}
	keys, err := d.Gateway.Store.ListUpstreamKeys(ids)
	if err != nil {
		return false
	}
	for _, key := range keys {
		channel, ok := channelByID[key.ChannelID]
		if !ok {
			continue
		}
		if allowedRoutes != nil {
			if _, allowed := allowedRoutes[directGatewayRouteScopeKey(channel.ID, key.GroupName)]; !allowed {
				continue
			}
		}
		if directGatewayProjectedKeyAllowsModel(channel, key, app, model) {
			return true
		}
	}
	return false
}

func directGatewayProjectedRouteAllowsModel(d *Deps, channel storage.Channel, groupName string, app storage.GatewayApp, model string) bool {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		return false
	}
	key, err := d.Gateway.Store.FindUpstreamKeyAny(channel.ID, groupName)
	return err == nil && directGatewayProjectedKeyAllowsModel(channel, *key, app, model)
}

func directGatewayProjectedKeyAllowsModel(channel storage.Channel, key storage.GatewayUpstreamKey, app storage.GatewayApp, model string) bool {
	return channel.CredentialMode == storage.CredentialModeDirect &&
		normalizeDirectGatewayApp(channel.GatewayApp) == normalizeDirectGatewayApp(app) &&
		key.ID > 0 && key.Enabled && !key.Provisioning && strings.TrimSpace(key.APIKeyCipher) != "" &&
		key.DirectCredentialID != nil && *key.DirectCredentialID > 0 && key.DirectCredentialRevision > 0 &&
		strings.TrimSpace(key.DirectBaseURL) != "" &&
		gatewayModelIDExact(strings.Split(key.ModelIDs, ","), model)
}

func directGatewayChannelAllowsModel(channel storage.Channel, groupName string, app storage.GatewayApp, model string) bool {
	if channel.CredentialMode != storage.CredentialModeDirect ||
		!strings.EqualFold(strings.TrimSpace(groupName), storage.DirectChannelGroupName) ||
		normalizeDirectGatewayApp(channel.GatewayApp) != normalizeDirectGatewayApp(app) {
		return false
	}
	models, err := storage.ParseDirectModels(channel.DirectModels)
	return err == nil && gatewayModelIDExact(models, model)
}

func directGatewayRouteScopeKey(channelID uint, groupName string) string {
	return fmt.Sprintf("%d\x00%s", channelID, strings.ToLower(strings.TrimSpace(groupName)))
}

func normalizeDirectGatewayApp(app storage.GatewayApp) storage.GatewayApp {
	switch storage.GatewayApp(strings.ToLower(strings.TrimSpace(string(app)))) {
	case storage.GatewayAppClaude:
		return storage.GatewayAppClaude
	case storage.GatewayAppGemini:
		return storage.GatewayAppGemini
	default:
		return storage.GatewayAppCodex
	}
}

func gatewayModelIDExact(models []string, requested string) bool {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return false
	}
	for _, model := range models {
		if strings.TrimSpace(model) == requested {
			return true
		}
	}
	return false
}

func gatewayModelFamiliesFromItems(items []map[string]any) []string {
	families := make(map[string]struct{})
	for _, item := range items {
		id, _ := item["id"].(string)
		if family := gatewayModelFamilyFromName(id); family != "" {
			families[family] = struct{}{}
		}
	}
	out := make([]string, 0, len(families))
	for _, family := range gatewayModelFamilyOrder {
		if _, ok := families[family]; ok {
			out = append(out, family)
		}
	}
	return out
}

func gatewayModelIDsFromItems(items []map[string]any) []string {
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

func csvContains(raw string, value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, item := range strings.Split(raw, ",") {
		if strings.TrimSpace(item) == value {
			return true
		}
	}
	return false
}

func gatewayModelFamilyFromName(name string) string {
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
	if strings.Contains(text, "claude") ||
		strings.Contains(text, "anthropic") ||
		strings.Contains(text, "sonnet") ||
		strings.Contains(text, "opus") ||
		strings.Contains(text, "haiku") ||
		strings.Contains(text, "kiro") ||
		strings.Contains(text, "krio") {
		return string(storage.GatewayAppClaude)
	}
	if strings.Contains(text, "gemini") || strings.Contains(text, "palm") {
		return string(storage.GatewayAppGemini)
	}
	if strings.Contains(text, "gpt") ||
		strings.Contains(text, "chatgpt") ||
		strings.Contains(text, "codex") ||
		strings.Contains(text, "dall-e") ||
		strings.Contains(text, "dalle") ||
		strings.Contains(text, "text-embedding") ||
		gatewayContainsModelToken(text, []string{"o1", "o3", "o4"}) {
		return string(storage.GatewayAppCodex)
	}
	return "other"
}

var gatewayModelFamilyOrder = []string{
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

func gatewayFamilySupportsApp(family string, app storage.GatewayApp) bool {
	family = strings.ToLower(strings.TrimSpace(family))
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

func gatewayFamilySupportsCandidate(family string, app storage.GatewayApp) bool {
	if strings.EqualFold(strings.TrimSpace(family), "other") {
		return app == storage.GatewayAppCodex
	}
	return gatewayFamilySupportsApp(family, app)
}

func gatewayFamilySet(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(strings.ToLower(item))
		if item != "" {
			out[item] = struct{}{}
		}
	}
	return out
}

func gatewayContainsModelToken(text string, tokens []string) bool {
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

func gatewayClientForProxy(rawProxyURL string) (*http.Client, error) {
	return gatewayClientForProxyWithCacheKey(rawProxyURL, "")
}

func gatewayClientForProxyWithCacheKey(rawProxyURL, profileCacheKey string) (*http.Client, error) {
	rawProxyURL = strings.TrimSpace(rawProxyURL)
	if rawProxyURL == "" {
		return gatewayHTTPClient, nil
	}
	cacheKey := strings.TrimSpace(profileCacheKey)
	if cacheKey == "" {
		cacheKey = netproxy.CacheKey(rawProxyURL)
	}
	gatewayProxyClients.Lock()
	if cached := gatewayProxyClients.values[cacheKey]; cached != nil {
		gatewayProxyClients.Unlock()
		return cached, nil
	}
	client, err := netproxy.NewClient(gatewayHTTPClient, rawProxyURL)
	if err != nil {
		gatewayProxyClients.Unlock()
		return nil, err
	}
	if len(gatewayProxyClients.values) < gatewayProxyClientCacheLimit {
		gatewayProxyClients.values[cacheKey] = client
	}
	gatewayProxyClients.Unlock()
	return client, nil
}

func shouldFallbackGatewayStatus(status int) bool {
	return status == http.StatusUnauthorized ||
		status == http.StatusForbidden ||
		status == http.StatusPaymentRequired ||
		status == http.StatusRequestTimeout ||
		status == http.StatusConflict ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		isGatewayServerErrorStatus(status)
}

func gatewayHTTPFailureInvalidatesKey(status int, raw []byte) bool {
	return (status == http.StatusUnauthorized || status == http.StatusForbidden) && !connector.IsUpstreamChallengeResponse(status, raw) && !connector.IsAccountBalanceFailure(status, raw)
}

func markGatewayAccountBalanceInsufficient(d *Deps, candidate gateway.Candidate, observedAt time.Time) {
	if d == nil || d.Channels == nil {
		return
	}
	_ = d.Channels.MarkUpstreamAccountBalanceInsufficient(
		candidate.Channel.ID,
		candidate.Channel.UpstreamAccountIdentityRevision,
		observedAt,
	)
}

func observeGatewayHTTPAuthorizationEvidence(ctx context.Context, d *Deps, candidate gateway.Candidate, lease gateway.UpstreamKeyLease, groupName string, status int, raw []byte) {
	if connector.IsAccountBalanceFailure(status, raw) {
		markGatewayAccountBalanceInsufficient(d, candidate, time.Now().UTC())
		return
	}
	if d != nil && d.Gateway != nil && gatewayHTTPFailureInvalidatesKey(status, raw) {
		invalidateGatewayCandidateUpstreamKey(ctx, d, candidate, lease, groupName)
	}
}

func observeGatewayStructuredAuthorizationEvidence(ctx context.Context, d *Deps, candidate gateway.Candidate, lease gateway.UpstreamKeyLease, groupName string, raw []byte, classification gatewayStreamFailureClassification) {
	if gatewayStructuredFailureIsAccountBalance(raw, classification) {
		markGatewayAccountBalanceInsufficient(d, candidate, time.Now().UTC())
		return
	}
	if d != nil && d.Gateway != nil && gatewayStructuredFailureInvalidatesKey(raw, classification) {
		invalidateGatewayCandidateUpstreamKey(ctx, d, candidate, lease, groupName)
	}
}

func isTransientGatewayStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusConflict || status == http.StatusTooEarly || status == http.StatusTooManyRequests || isGatewayServerErrorStatus(status)
}

func isTransientGatewayResponse(status int, raw []byte) bool {
	return isTransientGatewayStatus(status) || connector.IsUpstreamChallengeResponse(status, raw)
}

func isGatewayServerErrorStatus(status int) bool {
	return status >= http.StatusInternalServerError && status <= 599
}

func isNonTransientGatewayClientErrorStatus(status int) bool {
	return status >= http.StatusBadRequest && status <= 499 && !isTransientGatewayStatus(status)
}

func isNonTransientGatewayResponseStatus(status int) bool {
	return status >= http.StatusMultipleChoices && !isTransientGatewayStatus(status)
}

func isRetryableGatewayPreparationError(ctx context.Context, err error) bool {
	if err == nil || ctx != nil && ctx.Err() != nil {
		return false
	}
	// A provider-side cancellation is transient when the downstream request is
	// still alive. Client cancellation is rejected above through the request
	// context and must never be replayed.
	if errors.Is(err, context.Canceled) {
		return true
	}
	if connector.IsUpstreamChallengeHTTPError(err) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var httpErr *connector.HTTPError
	if errors.As(err, &httpErr) {
		return isTransientGatewayStatus(httpErr.StatusCode)
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.Timeout() || dnsErr.Temporary()
	}
	var addrErr *net.AddrError
	if errors.As(err, &addrErr) {
		return false
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && (networkErr.Timeout() || networkErr.Temporary()) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr)
}

func gatewayStatusAttemptReason(status int) string {
	if status == http.StatusPaymentRequired {
		return "upstream_account_unavailable"
	}
	return "upstream_status"
}

func mayBeModelUnsupportedStatus(status int) bool {
	return status == http.StatusBadRequest || status == http.StatusNotFound
}

type gatewayContextCapacity struct {
	ContextWindowTokens   int64
	EstimatedInputTokens  int64
	SafeInputTokens       int64
	RequestedOutputTokens int64
	SafetyTokens          int64
}

var (
	gatewayContextWindowPattern  = regexp.MustCompile(`(?i)([0-9][0-9,_]*)\s+token\s+context`)
	gatewayEstimatedInputPattern = regexp.MustCompile(`(?i)estimated\s+input\s+([0-9][0-9,_]*)\s+tokens?`)
	gatewaySafeInputPattern      = regexp.MustCompile(`(?i)safe\s+input\s+budget(?:\s+of)?\s+([0-9][0-9,_]*)`)
	gatewayReservedOutputPattern = regexp.MustCompile(`(?i)reserv(?:e|ing|ed)\s+([0-9][0-9,_]*)\s+output\s+tokens?`)
	gatewaySafetyTokensPattern   = regexp.MustCompile(`(?i)([0-9][0-9,_]*)\s+safety\s+tokens?`)
	gatewayPromptTooLongPattern  = regexp.MustCompile(`(?i)prompt\s+is\s+too\s+long\s*:\s*([0-9][0-9,_]*)\s+tokens?\s*>\s*([0-9][0-9,_]*)\s+maximum`)
)

func parseGatewayContextCapacity(body []byte, requestedOutputTokens int64) gatewayContextCapacity {
	text := string(body)
	out := gatewayContextCapacity{
		ContextWindowTokens:   gatewayContextNumber(text, gatewayContextWindowPattern, 1),
		EstimatedInputTokens:  gatewayContextNumber(text, gatewayEstimatedInputPattern, 1),
		SafeInputTokens:       gatewayContextNumber(text, gatewaySafeInputPattern, 1),
		RequestedOutputTokens: gatewayContextNumber(text, gatewayReservedOutputPattern, 1),
		SafetyTokens:          gatewayContextNumber(text, gatewaySafetyTokensPattern, 1),
	}
	if matches := gatewayPromptTooLongPattern.FindStringSubmatch(text); len(matches) == 3 {
		if out.EstimatedInputTokens == 0 {
			out.EstimatedInputTokens = parseGatewayContextNumber(matches[1])
		}
		if out.SafeInputTokens == 0 {
			out.SafeInputTokens = parseGatewayContextNumber(matches[2])
		}
	}
	if out.RequestedOutputTokens == 0 {
		out.RequestedOutputTokens = max(requestedOutputTokens, 0)
	}
	if out.SafetyTokens == 0 && out.ContextWindowTokens > 0 && out.SafeInputTokens > 0 {
		remaining := out.ContextWindowTokens - out.SafeInputTokens - out.RequestedOutputTokens
		if remaining > 0 {
			out.SafetyTokens = remaining
		}
	}
	return out
}

func gatewayContextNumber(text string, pattern *regexp.Regexp, group int) int64 {
	matches := pattern.FindStringSubmatch(text)
	if len(matches) <= group {
		return 0
	}
	return parseGatewayContextNumber(matches[group])
}

func parseGatewayContextNumber(value string) int64 {
	value = strings.NewReplacer(",", "", "_", "").Replace(strings.TrimSpace(value))
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func gatewayRequestMaxTokens(body []byte) int64 {
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return 0
	}
	return int64Value(payload, "max_tokens")
}

func observeGatewayContextLimit(d *Deps, candidate gateway.Candidate, model string, requestBody, responseBody []byte) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		return
	}
	modelFence, ok := gateway.ModelFactFenceForCandidate(candidate)
	if !ok {
		return
	}
	capacity := parseGatewayContextCapacity(responseBody, gatewayRequestMaxTokens(requestBody))
	if capacity.SafeInputTokens <= 0 {
		return
	}
	_ = d.Gateway.Store.ObserveModelContextLimitForIdentity(
		modelFence,
		strings.TrimSpace(model),
		capacity.ContextWindowTokens,
		capacity.SafeInputTokens,
		capacity.SafetyTokens,
		time.Now(),
	)
}

// gatewayCandidateGenerationArgs carries the route snapshot's upstream-group
// generation into persistence. A group removed or replaced while an upstream
// request is in flight must not receive stale capability evidence afterward.
func gatewayCandidateGenerationArgs(candidate gateway.Candidate) []storage.UpstreamGroupGeneration {
	if !candidate.UpstreamGeneration.Valid() {
		return nil
	}
	return []storage.UpstreamGroupGeneration{candidate.UpstreamGeneration}
}

// gatewayCandidateModelFactGenerationArgs carries the generation that fenced
// model facts when the candidate was selected. Fixed routes without a rate
// snapshot intentionally have no health generation, but a late /v1/models or
// model-capability observation must still never land on a replacement group
// with the same name. Older candidates fall back to their health generation.
func gatewayCandidateModelFactGenerationArgs(candidate gateway.Candidate) []storage.UpstreamGroupGeneration {
	generation := candidate.ModelFactGeneration
	if !generation.Valid() {
		generation = candidate.UpstreamGeneration
	}
	if !generation.Valid() {
		return nil
	}
	return []storage.UpstreamGroupGeneration{generation}
}

// gatewaySingleModelFactGeneration rejects the legacy unfenced storage path.
// Model facts are scoped to precisely one upstream-group presence generation;
// a missing, invalid, or ambiguous fence must never mutate a same-named group
// that was removed and later recreated while a request was in flight.
func gatewaySingleModelFactGeneration(generations []storage.UpstreamGroupGeneration) (storage.UpstreamGroupGeneration, bool) {
	if len(generations) != 1 || !generations[0].Valid() {
		return storage.UpstreamGroupGeneration{}, false
	}
	return generations[0], true
}

// gatewayCandidateKeyStateGenerationArgs is deliberately independent from
// model facts and routing health. A fixed no-rate request may receive an
// explicit invalid-key response, but it can only invalidate the exact group
// generation captured before dispatch.
func gatewayCandidateKeyStateGenerationArgs(candidate gateway.Candidate) []storage.UpstreamGroupGeneration {
	generation := candidate.KeyStateGeneration
	if !generation.Valid() {
		generation = candidate.UpstreamGeneration
	}
	if !generation.Valid() {
		return nil
	}
	return []storage.UpstreamGroupGeneration{generation}
}

// invalidateGatewayCandidateUpstreamKey is the sole data-plane key
// invalidation bridge. Authentication evidence must carry the exact key lease
// in addition to the channel and group identity captured by the candidate.
func invalidateGatewayCandidateUpstreamKey(ctx context.Context, d *Deps, candidate gateway.Candidate, lease gateway.UpstreamKeyLease, _ string) {
	if d == nil || d.Gateway == nil {
		return
	}
	fence, ok := gateway.UpstreamKeyInvalidationFenceForCandidate(candidate, lease)
	if !ok {
		return
	}
	_ = d.Gateway.InvalidateUpstreamGroupKeyForIdentityContext(ctx, fence)
}

// touchGatewayCandidateRoutingHealth preserves the fixed-route rule that a
// no-rate candidate cannot emit an unfenced shared-health success. Model
// observations use ModelFactGeneration separately and do not imply this
// routing-health mutation.
func touchGatewayCandidateRoutingHealth(d *Deps, candidate gateway.Candidate, _ string, at time.Time) {
	if d == nil || d.Gateway == nil {
		return
	}
	fence, ok := gateway.RoutingHealthFenceForCandidate(candidate)
	if !ok {
		return
	}
	d.Gateway.TouchUpstreamGroupForRoutingHealthAt(fence, at)
}

func markGatewayCandidateRoutingFailure(d *Deps, candidate gateway.Candidate, _ string, kind string, status int, message string, occurrences ...int) {
	if d == nil || d.Gateway == nil {
		return
	}
	fence, ok := gateway.RoutingHealthFenceForCandidate(candidate)
	if !ok {
		return
	}
	d.Gateway.MarkUpstreamGroupRoutingFailureForRoutingHealth(fence, kind, status, message, occurrences...)
}

func markGatewayCandidateModelCapability(store *storage.Gateway, candidate gateway.Candidate, groupName, modelName string, supported bool, lastError string) {
	if store == nil {
		return
	}
	modelFence, ok := gateway.ModelFactFenceForCandidate(candidate)
	if !ok {
		return
	}
	_ = store.MarkModelCapabilityForIdentity(
		modelFence,
		modelName,
		supported,
		lastError,
	)
}

func gatewayKnownSafeInputBudget(d *Deps, candidate gateway.Candidate, model string, body []byte) int64 {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil || strings.TrimSpace(model) == "" {
		return 0
	}
	modelFence, ok := gateway.ModelFactFenceForCandidate(candidate)
	if !ok {
		return 0
	}
	capability, err := d.Gateway.Store.FindModelCapabilityForIdentity(modelFence, strings.TrimSpace(model))
	if err != nil || capability == nil || capability.SafeInputTokens <= 0 || capability.ContextLimitObserved == nil || capability.ContextLimitObserved.Before(time.Now().Add(-gatewayContextCapacityMaxAge)) {
		return 0
	}
	budget := capability.SafeInputTokens
	requestedOutput := gatewayRequestMaxTokens(body)
	if capability.ContextWindowTokens > 0 && capability.ContextSafetyTokens >= 0 && requestedOutput > 0 {
		if dynamic := capability.ContextWindowTokens - capability.ContextSafetyTokens - requestedOutput; dynamic > 0 {
			budget = dynamic
		}
	}
	return max(budget, 0)
}

func shouldPreflightGatewayContext(body []byte, safeInputTokens int64) bool {
	if safeInputTokens <= 0 || len(body) == 0 {
		return false
	}
	threshold := safeInputTokens * gatewayContextPreflightBodyRatioNumerator / gatewayContextPreflightBodyRatioDenominator
	return int64(len(body)) >= threshold
}

func countGatewayCandidateInputTokens(
	in *http.Request,
	target gatewayResolvedTarget,
	upstreamKey string,
	body []byte,
	dispatch *gatewayTransportDispatchFence,
	observeAuthorization func(status int, raw []byte),
) (int64, bool, bool, error) {
	countBody, err := gatewayCountTokensBody(body)
	if err != nil {
		return 0, false, false, err
	}
	request := in.Clone(in.Context())
	request.Method = http.MethodPost
	request.URL.Path = "/v1/messages/count_tokens"
	request.URL.RawPath = ""
	request.URL.RawQuery = ""
	resp, err := forwardGatewayRequestWithDispatchFence(request, target, upstreamKey, countBody, dispatch, nil)
	if err != nil {
		return 0, false, false, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if readErr != nil {
		return 0, false, false, readErr
	}
	if isGatewayContextLimitError(resp.StatusCode, raw) {
		return 0, false, true, nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		if observeAuthorization != nil {
			observeAuthorization(resp.StatusCode, raw)
		}
		return 0, false, false, fmt.Errorf("count tokens status %d", resp.StatusCode)
	}
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil {
		return 0, false, false, errors.New("invalid count tokens response")
	}
	inputTokens := maxInt64Fields(payload, "input_tokens", "prompt_tokens")
	if inputTokens <= 0 {
		return 0, false, false, errors.New("count tokens response omitted input_tokens")
	}
	return inputTokens, true, false, nil
}

func gatewayCountTokensBody(body []byte) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	for _, field := range []string{"max_tokens", "stream", "temperature", "top_k", "top_p", "stop_sequences", "metadata", "service_tier"} {
		delete(payload, field)
	}
	return json.Marshal(payload)
}

func isGatewayContextLimitError(status int, body []byte) bool {
	if status != http.StatusBadRequest && status != http.StatusRequestEntityTooLarge {
		return false
	}
	text := strings.ToLower(string(body))
	if strings.Contains(text, "prompt cannot fit") || strings.Contains(text, "safe input budget") {
		return true
	}
	if strings.Contains(text, "prompt is too long") && (strings.Contains(text, "token") || strings.Contains(text, "maximum")) {
		return true
	}
	for _, marker := range []string{"context length", "context window", "context limit", "context_length_exceeded", "maximum context"} {
		if strings.Contains(text, marker) && (strings.Contains(text, "token") || strings.Contains(text, "exceed") || strings.Contains(text, "too long")) {
			return true
		}
	}
	return false
}

func gatewayPublicContextLimitResponse(requestPath string) gin.H {
	if requestPath == "/v1/messages" || requestPath == "/v1/messages/count_tokens" {
		return gin.H{
			"type": "error",
			"error": gin.H{
				"type":    "invalid_request_error",
				"message": gatewayPublicContextLimitMessage,
			},
		}
	}
	return gin.H{
		"error": gin.H{
			"type":    "invalid_request_error",
			"code":    "context_length_exceeded",
			"message": gatewayPublicContextLimitMessage,
		},
	}
}

func isGatewayModelUnsupported(body string) bool {
	body = strings.ToLower(body)
	return strings.Contains(body, "model_not_found") ||
		strings.Contains(body, "model not found") ||
		strings.Contains(body, "not supported by any configured account") ||
		strings.Contains(body, "is not supported")
}

func gatewayResponseSnippet(body io.Reader, upstreamKey string) string {
	raw, _ := io.ReadAll(io.LimitReader(body, 4096))
	return sanitizeGatewayProbeError(raw, upstreamKey)
}

type gatewayReplayReadCloser struct {
	io.Reader
	io.Closer
}

type gatewayFirstByteTracker struct {
	startedAt time.Time
	firstAt   time.Time
}

func (t *gatewayFirstByteTracker) record() {
	if t != nil && t.firstAt.IsZero() {
		t.firstAt = time.Now()
	}
}

func (t *gatewayFirstByteTracker) LatencyMs() int64 {
	if t == nil || t.startedAt.IsZero() || t.firstAt.IsZero() {
		return 0
	}
	latency := t.firstAt.Sub(t.startedAt).Milliseconds()
	if latency < 1 {
		return 1
	}
	return latency
}

type gatewayFirstByteReadCloser struct {
	io.ReadCloser
	tracker     *gatewayFirstByteTracker
	onFirstByte func()
}

func (r *gatewayFirstByteReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		r.tracker.record()
		if r.onFirstByte != nil {
			r.onFirstByte()
		}
	}
	return n, err
}

var (
	errGatewayFirstByteTimeout        = errors.New("upstream response timed out before first body byte")
	errGatewayMalformedResponsesFrame = errors.New("upstream Responses stream contained a malformed JSON frame")
)

type gatewayFirstByteDeadline struct {
	waiting           bool
	waitStop          func()
	ctx               context.Context
	cancel            context.CancelCauseFunc
	preparationCtx    context.Context
	preparationCancel context.CancelFunc
	firstByte         atomic.Bool
	firstByteTimer    *time.Timer
}

func newGatewayFirstByteDeadline(parent context.Context, preparationTimeout time.Duration) *gatewayFirstByteDeadline {
	ctx, cancel := context.WithCancelCause(parent)
	preparationCtx := ctx
	preparationCancel := func() {}
	if preparationTimeout > 0 {
		preparationCtx, preparationCancel = context.WithTimeoutCause(ctx, preparationTimeout, errGatewayFirstByteTimeout)
	}
	return &gatewayFirstByteDeadline{ctx: ctx, cancel: cancel, preparationCtx: preparationCtx, preparationCancel: preparationCancel}
}

func (d *gatewayFirstByteDeadline) Start(timeout time.Duration) {
	if d == nil || d.firstByte.Load() || d.firstByteTimer != nil {
		return
	}
	d.preparationCancel()
	d.preparationCancel = func() {}
	if timeout <= 0 {
		if d.firstByte.CompareAndSwap(false, true) {
			d.cancel(errGatewayFirstByteTimeout)
		}
		return
	}
	d.firstByteTimer = time.AfterFunc(timeout, func() {
		if d.firstByte.CompareAndSwap(false, true) {
			d.cancel(errGatewayFirstByteTimeout)
		}
	})
}

func (d *gatewayFirstByteDeadline) Context() context.Context {
	if d == nil || d.ctx == nil {
		return context.Background()
	}
	return d.ctx
}

func (d *gatewayFirstByteDeadline) PreparationContext() context.Context {
	if d == nil || d.preparationCtx == nil {
		return context.Background()
	}
	return d.preparationCtx
}

func (d *gatewayFirstByteDeadline) MarkFirstByte() {
	if d == nil {
		return
	}
	if d.firstByte.CompareAndSwap(false, true) && d.firstByteTimer != nil {
		d.firstByteTimer.Stop()
	}
	if d.waitStop != nil {
		d.waitStop()
	}
}

func (d *gatewayFirstByteDeadline) MarkAccepted() {
	if d != nil && !d.waiting {
		d.MarkFirstByte()
	}
}

func (d *gatewayFirstByteDeadline) failWaitingProbe() {
	if d.firstByte.CompareAndSwap(false, true) {
		d.cancel(errGatewayWaitingProbeTimeout)
	}
}

func (d *gatewayFirstByteDeadline) WaitingProbeFailed() bool {
	return d != nil && errors.Is(context.Cause(d.ctx), errGatewayWaitingProbeTimeout)
}

func (d *gatewayFirstByteDeadline) TimeoutMessage(timeout time.Duration) string {
	if d.WaitingProbeFailed() {
		return errGatewayWaitingProbeTimeout.Error()
	}
	return gatewayFirstByteTimeoutMessage(timeout)
}

func (d *gatewayFirstByteDeadline) TimedOut() bool {
	return d != nil && (d.WaitingProbeFailed() || errors.Is(context.Cause(d.ctx), errGatewayFirstByteTimeout) || errors.Is(context.Cause(d.preparationCtx), errGatewayFirstByteTimeout))
}

func (d *gatewayFirstByteDeadline) Close() {
	if d == nil {
		return
	}
	d.MarkFirstByte()
	d.preparationCancel()
	d.preparationCancel = func() {}
	if d.firstByteTimer != nil {
		d.firstByteTimer.Stop()
	}
	d.cancel(nil)
}

func gatewayCandidateFirstByteBudget(decision *gatewayRouteDecision, configured time.Duration, imageRequest bool) time.Duration {
	if decision != nil && !decision.SmartRouting {
		return 0
	}
	if imageRequest {
		return configured
	}
	if decision != nil && decision.AffinityApplied && gatewaySessionAffinityOriginAuditable(decision.AffinityOrigin) && configured < gatewaySessionAffinityFirstByteTimeout {
		return gatewaySessionAffinityFirstByteTimeout
	}
	return configured
}

func gatewayFirstByteTimeoutMessage(timeout time.Duration) string {
	if timeout <= 0 {
		return "upstream response timed out before first output"
	}
	return fmt.Sprintf("upstream response exceeded %s first-byte timeout", timeout.Round(time.Millisecond))
}

// gatewayResponseSpoolLocalFailure identifies failures caused by the local
// response-spool boundary rather than an upstream response. A physical data
// plane dispatch has already occurred before this code buffers the body, so
// these failures must never be converted into a retry, fallback, or routing
// health signal. The body-started boundary is handled separately because an
// upstream read failure after bytes arrive is non-replayable for the same
// reason, but it is not evidence of protocol-level acceptance.
func gatewayResponseSpoolLocalFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errGatewayResponseSpoolCapacity) ||
		errors.Is(err, errGatewayResponseSpoolQueueFull) ||
		errors.Is(err, errGatewayResponseSpoolQueueWaitCanceled) ||
		errors.Is(err, errGatewayResponseSpoolReleased) ||
		errors.Is(err, errGatewayResponseBufferInvalid) ||
		errors.Is(err, io.ErrShortWrite) {
		return true
	}
	var pathErr *os.PathError
	return errors.As(err, &pathErr)
}

func prepareGatewayStreamPrelude(body io.ReadCloser, requestPath string) (io.ReadCloser, []byte, bool, bool, error) {
	return prepareGatewayStreamPreludeWithAcceptance(body, requestPath, nil)
}

type gatewayStreamAcceptancePersistenceError struct{ cause error }

func (e *gatewayStreamAcceptancePersistenceError) Error() string { return e.cause.Error() }
func (e *gatewayStreamAcceptancePersistenceError) Unwrap() error { return e.cause }

type gatewayStreamPreludeReadError struct{ err error }

func (r gatewayStreamPreludeReadError) Read([]byte) (int, error) { return 0, r.err }

func prepareGatewayStreamPreludeWithAcceptance(body io.ReadCloser, requestPath string, onAccepted func(gatewayStreamAcceptanceEvidence) error, onUseful ...func()) (io.ReadCloser, []byte, bool, bool, error) {
	if body == nil {
		return io.NopCloser(bytes.NewReader(nil)), nil, false, false, nil
	}
	reader := bufio.NewReaderSize(body, 16*1024)
	var prelude bytes.Buffer
	var frame bytes.Buffer
	capacityNoticeBytes := 0
	interruptionNoticeBytes := 0
	upstreamInterruptionComplete := false
	deferredAcceptance := gatewayStreamAcceptanceEvidence{}
	deferredAcceptanceSet := false
	acceptanceMarked := false
	var acceptanceError error
	replay := func(useful, accepted bool, err error) (io.ReadCloser, []byte, bool, bool, error) {
		raw := append([]byte(nil), prelude.Bytes()...)
		var remainder io.Reader = reader
		if err != nil && acceptanceMarked {
			// ReadSlice may consume a one-shot read error. Preserve it after the
			// buffered frames instead of turning the interrupted stream into EOF.
			remainder = gatewayStreamPreludeReadError{err: err}
		}
		return &gatewayReplayReadCloser{Reader: io.MultiReader(bytes.NewReader(raw), remainder), Closer: body}, raw, useful, accepted || acceptanceMarked, err
	}
	markAccepted := func(evidence gatewayStreamAcceptanceEvidence) (bool, error) {
		if acceptanceMarked {
			return true, acceptanceError
		}
		acceptanceMarked = true
		if deferredAcceptanceSet {
			evidence = deferredAcceptance
		}
		if onAccepted != nil {
			if err := onAccepted(evidence); err != nil {
				acceptanceError = &gatewayStreamAcceptancePersistenceError{cause: err}
			}
		}
		return true, acceptanceError
	}
	markUseful := func(raw []byte) (bool, error) {
		// Stop health observation as soon as semantic output is recognized,
		// before a durable acceptance write can wait on the local database.
		for _, notify := range onUseful {
			if notify != nil {
				notify()
			}
		}
		if deferredAcceptanceSet {
			return markAccepted(deferredAcceptance)
		}
		if onAccepted == nil {
			return false, nil
		}
		// The caller only reaches this for a visible, non-error, non-metadata
		// frame: real output that is about to be written downstream, so this
		// attempt is already non-replayable. A frame shape we do not recognize
		// must therefore still fence the dispatch as accepted. Refusing to mark
		// it would turn a served upstream generation into a routing failure and
		// replay the same prompt against the same route, double-billing it.
		// Guessing a protocol lifecycle event stays forbidden: an unrecognized
		// frame carries no evidence, and the durable mark records it as generic
		// visible output rather than as a named acceptance event.
		evidence, _ := gatewayStreamUsefulAcceptanceEvidence(raw, requestPath)
		return markAccepted(evidence)
	}
	for prelude.Len() < gatewayStreamPreludeLimit {
		line, err := reader.ReadSlice('\n')
		_, _ = prelude.Write(line)
		_, _ = frame.Write(line)
		if errors.Is(err, bufio.ErrBufferFull) {
			if gatewayStreamFrameHasError(frame.Bytes()) {
				if deferredAcceptanceSet {
					accepted, markErr := markAccepted(deferredAcceptance)
					return replay(false, accepted, markErr)
				}
				return replay(false, false, nil)
			}
			// response.created can carry large instructions and tool schemas. Keep
			// collecting bounded fragments so a lifecycle frame is not mistaken
			// for useful output merely because it crossed the reader buffer.
			if prelude.Len() < gatewayStreamPreludeLimit {
				continue
			}
			if upstreamInterruptionComplete {
				return replay(true, false, nil)
			}
			if deferredAcceptanceSet {
				accepted, markErr := markAccepted(deferredAcceptance)
				return replay(false, accepted, markErr)
			}
			useful := !gatewayStreamFrameIsMetadata(frame.Bytes())
			if useful {
				accepted, markErr := markUseful(frame.Bytes())
				return replay(markErr == nil, accepted, markErr)
			}
			return replay(false, false, nil)
		}
		if err == nil && len(bytes.TrimSpace(line)) == 0 && frame.Len() > 0 {
			if gatewayResponsesFrameHasMalformedJSON(frame.Bytes(), requestPath) {
				if deferredAcceptanceSet {
					accepted, markErr := markAccepted(deferredAcceptance)
					if markErr != nil {
						return replay(false, accepted, markErr)
					}
					return replay(false, accepted, errGatewayMalformedResponsesFrame)
				}
				return replay(false, false, errGatewayMalformedResponsesFrame)
			}
			if gatewayStreamFrameHasError(frame.Bytes()) {
				if deferredAcceptanceSet {
					accepted, markErr := markAccepted(deferredAcceptance)
					return replay(false, accepted, markErr)
				}
				return replay(false, false, nil)
			}
			metadata := gatewayStreamFrameIsMetadata(frame.Bytes())
			if upstreamInterruptionComplete {
				if gatewayStreamFrameTerminatesResponse(frame.Bytes()) {
					return replay(true, false, nil)
				}
				if !metadata && !gatewayResponsesFrameClosesConfirmedInterruption(frame.Bytes()) {
					accepted, markErr := markUseful(frame.Bytes())
					return replay(markErr == nil, accepted, markErr)
				}
				frame.Reset()
				continue
			}
			if !metadata {
				if isGatewaySSEUpstreamInterruptionNotification(prelude.Bytes()) && interruptionNoticeBytes+frame.Len() <= gatewayCapacityNoticePreludeLimit {
					upstreamInterruptionComplete = true
					interruptionNoticeBytes += frame.Len()
					frame.Reset()
					continue
				}
				if gatewaySSEUpstreamInterruptionNotificationPending(prelude.Bytes()) && interruptionNoticeBytes+frame.Len() <= gatewayCapacityNoticePreludeLimit {
					interruptionNoticeBytes += frame.Len()
					frame.Reset()
					continue
				}
			}
			if evidence, accepted := gatewayStreamFrameAcceptanceEvidence(frame.Bytes(), requestPath); accepted {
				accepted, markErr := markAccepted(evidence)
				if markErr != nil {
					return replay(false, accepted, markErr)
				}
				if requestPath == "/v1/responses" && metadata && !gatewayStreamFrameTerminatesResponse(frame.Bytes()) {
					// Buffer lifecycle frames separately from the durable acceptance
					// boundary. A later capacity notice must never undo this fence.
					if !deferredAcceptanceSet {
						deferredAcceptance = evidence
						deferredAcceptanceSet = true
					}
					frame.Reset()
					continue
				}
				return replay(false, accepted, markErr)
			}
			if !metadata {
				if deferredAcceptanceSet {
					accepted, markErr := markUseful(frame.Bytes())
					return replay(markErr == nil, accepted, markErr)
				}
				if isGatewaySSECapacityNotification(prelude.Bytes()) {
					return replay(true, false, nil)
				}
				if gatewaySSECapacityNotificationPending(prelude.Bytes()) && capacityNoticeBytes+frame.Len() <= gatewayCapacityNoticePreludeLimit {
					capacityNoticeBytes += frame.Len()
					frame.Reset()
					continue
				}
				accepted, markErr := markUseful(frame.Bytes())
				return replay(markErr == nil, accepted, markErr)
			}
			frame.Reset()
			if prelude.Len() >= gatewayStreamPreludeLimit {
				return replay(false, false, nil)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if frame.Len() > 0 && gatewayResponsesFrameHasMalformedJSON(frame.Bytes(), requestPath) {
					if deferredAcceptanceSet {
						accepted, markErr := markAccepted(deferredAcceptance)
						if markErr != nil {
							return replay(false, accepted, markErr)
						}
						return replay(false, accepted, errGatewayMalformedResponsesFrame)
					}
					return replay(false, false, errGatewayMalformedResponsesFrame)
				}
				if upstreamInterruptionComplete || isGatewaySSEUpstreamInterruptionNotification(prelude.Bytes()) {
					return replay(true, false, nil)
				}
				if evidence, accepted := gatewayStreamFrameAcceptanceEvidence(frame.Bytes(), requestPath); frame.Len() > 0 && accepted {
					accepted, markErr := markAccepted(evidence)
					return replay(false, accepted, markErr)
				}
				if deferredAcceptanceSet {
					accepted, markErr := markAccepted(deferredAcceptance)
					return replay(false, accepted, markErr)
				}
				useful := frame.Len() > 0 && !gatewayStreamFrameHasError(frame.Bytes()) && !gatewayStreamFrameIsMetadata(frame.Bytes())
				if !useful && gatewaySSECapacityNotificationPending(prelude.Bytes()) {
					useful = true
				}
				if useful {
					accepted, markErr := markUseful(frame.Bytes())
					return replay(markErr == nil, accepted, markErr)
				}
				return replay(false, false, nil)
			}
			if upstreamInterruptionComplete {
				return replay(true, false, nil)
			}
			if deferredAcceptanceSet {
				accepted, markErr := markAccepted(deferredAcceptance)
				if markErr != nil {
					return replay(false, accepted, markErr)
				}
				return replay(false, accepted, err)
			}
			return replay(false, false, err)
		}
		if prelude.Len() >= gatewayStreamPreludeLimit {
			if upstreamInterruptionComplete {
				return replay(true, false, nil)
			}
			if deferredAcceptanceSet {
				accepted, markErr := markAccepted(deferredAcceptance)
				return replay(false, accepted, markErr)
			}
			useful := frame.Len() > 0 && !gatewayStreamFrameHasError(frame.Bytes()) && !gatewayStreamFrameIsMetadata(frame.Bytes())
			if useful {
				accepted, markErr := markUseful(frame.Bytes())
				return replay(markErr == nil, accepted, markErr)
			}
			return replay(false, false, nil)
		}
	}
	if upstreamInterruptionComplete {
		return replay(true, false, nil)
	}
	if deferredAcceptanceSet {
		accepted, markErr := markAccepted(deferredAcceptance)
		return replay(false, accepted, markErr)
	}
	return replay(false, false, nil)
}

// Metadata frames establish a stream but do not contain an answer or a tool
// delta. Buffering only these frames lets a following capacity error fail over
// before a client sees a stream it would treat as terminal.
func gatewayStreamFrameIsMetadata(frame []byte) bool {
	eventName := ""
	data := make([]string, 0, 1)
	scanner := bufio.NewScanner(bytes.NewReader(frame))
	scanner.Buffer(make([]byte, 1024), 64*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "id:") || strings.HasPrefix(line, "retry:"):
			continue
		case strings.HasPrefix(line, "event:"):
			eventName = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "event:")))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		default:
			return false
		}
	}
	if scanner.Err() != nil {
		// An oversized data-only frame may be a real text/tool delta. Only keep
		// staging it when the preceding SSE event name proves it is metadata.
		return eventName != "" && gatewayStreamMetadataEvent(eventName)
	}
	if len(data) == 0 {
		return gatewayStreamMetadataEvent(eventName)
	}
	payload := strings.Join(data, "\n")
	if payload == "[DONE]" {
		return true
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(payload), &value); err != nil {
		return eventName != "" && gatewayStreamMetadataEvent(eventName)
	}
	if kind, ok := value["type"].(string); ok {
		kind = strings.ToLower(strings.TrimSpace(kind))
		if gatewayStreamMetadataEvent(kind) || gatewayResponsesStructuralEventIsMetadata(kind, value) {
			return true
		}
	}
	if choices, ok := value["choices"].([]any); ok {
		return gatewayChatCompletionMetadata(choices)
	}
	return eventName != "" && (gatewayStreamMetadataEvent(eventName) || gatewayResponsesStructuralEventIsMetadata(eventName, value))
}

type gatewayStreamAcceptanceEvidence struct {
	EventType string
	ObjectID  string
}

// gatewayStreamFrameAcceptanceEvidence recognizes only protocol-defined
// lifecycle frames that prove the provider accepted the request. This is
// deliberately narrower than generic metadata detection: unknown keepalives
// and arbitrary empty frames must not disable smart fallback.
func gatewayStreamFrameAcceptanceEvidence(frame []byte, requestPath string) (gatewayStreamAcceptanceEvidence, bool) {
	if len(bytes.TrimSpace(frame)) == 0 || gatewayStreamFrameHasError(frame) {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	if requestPath == "/v1/responses" {
		if evidence, completed := gatewayResponsesCompletedFrameEvidence(frame); completed {
			return evidence, true
		}
	}
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(frame)))
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["type"])))
		if eventName != "" && kind != "" && eventName != kind {
			continue
		}
		switch requestPath {
		case "/v1/responses":
			switch kind {
			case "response.created", "response.queued", "response.in_progress":
				response, ok := payload["response"].(map[string]any)
				if ok && gatewayAcceptedStreamID(response["id"]) {
					return gatewayStreamAcceptanceEvidence{EventType: kind, ObjectID: strings.TrimSpace(gatewayStreamFailureString(response["id"]))}, true
				}
			case "response.output_item.added":
				item, ok := payload["item"].(map[string]any)
				if ok && gatewayAcceptedStreamID(item["id"]) && gatewayAcceptedStreamType(item["type"]) {
					return gatewayStreamAcceptanceEvidence{EventType: kind, ObjectID: strings.TrimSpace(gatewayStreamFailureString(item["id"]))}, true
				}
			case "response.content_part.added", "response.reasoning_summary_part.added":
				part, ok := payload["part"].(map[string]any)
				if ok && gatewayAcceptedStreamID(payload["item_id"]) && gatewayAcceptedStreamType(part["type"]) {
					return gatewayStreamAcceptanceEvidence{EventType: kind, ObjectID: strings.TrimSpace(gatewayStreamFailureString(payload["item_id"]))}, true
				}
			}
		case "/v1/messages":
			switch kind {
			case "message_start":
				message, ok := payload["message"].(map[string]any)
				if !ok {
					continue
				}
				role := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(message["role"])))
				if role == "assistant" && gatewayAcceptedStreamID(message["id"]) {
					return gatewayStreamAcceptanceEvidence{EventType: kind, ObjectID: strings.TrimSpace(gatewayStreamFailureString(message["id"]))}, true
				}
			case "content_block_start":
				block, ok := payload["content_block"].(map[string]any)
				if ok && gatewayAcceptedStreamIndex(payload["index"]) && gatewayAcceptedStreamType(block["type"]) {
					return gatewayStreamAcceptanceEvidence{EventType: kind}, true
				}
			case "message_delta":
				delta, ok := payload["delta"].(map[string]any)
				if ok && len(delta) > 0 {
					return gatewayStreamAcceptanceEvidence{EventType: kind}, true
				}
			}
		case "/v1/chat/completions":
			if object := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["object"]))); object != "chat.completion.chunk" || !gatewayAcceptedStreamID(payload["id"]) {
				continue
			}
			if _, ok := payload["choices"].([]any); ok {
				return gatewayStreamAcceptanceEvidence{EventType: "chat.completion.chunk", ObjectID: strings.TrimSpace(gatewayStreamFailureString(payload["id"]))}, true
			}
		case "/v1/completions":
			// The legacy OpenAI Completions stream has no event-name field for a
			// text_completion delta. Do not let an arbitrary SSE event wrapper turn
			// a superficially JSON-shaped payload into trusted acceptance evidence.
			if eventName == "" {
				if evidence, accepted := gatewayLegacyCompletionDeltaAcceptanceEvidence(payload); accepted {
					return evidence, true
				}
			}
		}
	}
	return gatewayStreamAcceptanceEvidence{}, false
}

func gatewayStreamFrameAccepted(frame []byte, requestPath string) bool {
	_, accepted := gatewayStreamFrameAcceptanceEvidence(frame, requestPath)
	return accepted
}

// gatewayStreamUsefulAcceptanceEvidence is used only for a protocol frame
// that is about to become visible output. It remains a strict whitelist so an
// unknown event cannot silently turn off fallback before durable acceptance is
// recorded.
func gatewayStreamUsefulAcceptanceEvidence(frame []byte, requestPath string) (gatewayStreamAcceptanceEvidence, bool) {
	if evidence, accepted := gatewayStreamFrameAcceptanceEvidence(frame, requestPath); accepted {
		return evidence, true
	}
	if len(bytes.TrimSpace(frame)) == 0 || gatewayStreamFrameHasError(frame) {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(frame)))
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["type"])))
		if eventName != "" && kind != "" && eventName != kind {
			continue
		}
		switch requestPath {
		case "/v1/responses":
			switch kind {
			case "response.output_text.delta", "response.refusal.delta",
				"response.function_call_arguments.delta", "response.custom_tool_call_input.delta",
				"response.reasoning_text.delta", "response.reasoning_summary_text.delta":
				if _, ok := payload["delta"].(string); ok {
					return gatewayStreamAcceptanceEvidence{EventType: kind, ObjectID: strings.TrimSpace(gatewayStreamFailureString(payload["item_id"]))}, true
				}
			case "response.image_generation_call.partial_image":
				if gatewayAcceptedStreamID(payload["item_id"]) {
					return gatewayStreamAcceptanceEvidence{EventType: kind, ObjectID: strings.TrimSpace(gatewayStreamFailureString(payload["item_id"]))}, true
				}
			}
		case "/v1/messages":
			if kind != "content_block_delta" || !gatewayAcceptedStreamIndex(payload["index"]) {
				continue
			}
			if delta, ok := payload["delta"].(map[string]any); ok && len(delta) > 0 {
				return gatewayStreamAcceptanceEvidence{EventType: kind}, true
			}
		case "/v1/chat/completions":
			if object := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["object"]))); object == "chat.completion.chunk" && gatewayAcceptedStreamID(payload["id"]) {
				if _, ok := payload["choices"].([]any); ok {
					return gatewayStreamAcceptanceEvidence{EventType: "chat.completion.chunk", ObjectID: strings.TrimSpace(gatewayStreamFailureString(payload["id"]))}, true
				}
			}
		}
	}
	return gatewayStreamAcceptanceEvidence{}, false
}

func gatewayAcceptedStreamID(value any) bool {
	id, ok := value.(string)
	return ok && strings.TrimSpace(id) != ""
}

func gatewayAcceptedStreamType(value any) bool {
	kind, ok := value.(string)
	return ok && strings.TrimSpace(kind) != ""
}

func gatewayAcceptedStreamIndex(value any) bool {
	index, ok := value.(float64)
	return ok && index >= 0 && index == float64(int64(index))
}

// gatewayLegacyCompletionDeltaAcceptanceEvidence recognizes the only legacy
// completions stream frame that can establish a non-replayable response: an
// OpenAI text_completion delta with an authoritative completion ID and at
// least one well-formed choice. Unlike a generic data frame, this is concrete
// protocol evidence that a generation has started; it must therefore stop
// smart-route retry accounting before the delta is passed downstream.
func gatewayLegacyCompletionDeltaAcceptanceEvidence(payload map[string]any) (gatewayStreamAcceptanceEvidence, bool) {
	if strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["object"]))) != "text_completion" || !gatewayAcceptedStreamID(payload["id"]) {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	choices, ok := payload["choices"].([]any)
	if !ok || len(choices) == 0 {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	for _, rawChoice := range choices {
		choice, ok := rawChoice.(map[string]any)
		if !ok || !gatewayAcceptedStreamIndex(choice["index"]) {
			return gatewayStreamAcceptanceEvidence{}, false
		}
		if _, ok := choice["text"].(string); !ok {
			return gatewayStreamAcceptanceEvidence{}, false
		}
	}
	return gatewayStreamAcceptanceEvidence{
		EventType: "text_completion.delta",
		ObjectID:  strings.TrimSpace(gatewayStreamFailureString(payload["id"])),
	}, true
}

func gatewayStreamFrameHasError(frame []byte) bool {
	if gatewayErrorPayloadText(frame) != "" {
		return true
	}
	if classifyGatewayStreamFailure(frame).Found {
		return true
	}
	scanner := bufio.NewScanner(bytes.NewReader(frame))
	scanner.Buffer(make([]byte, 1024), 64*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "event:") {
			continue
		}
		eventName := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "event:")))
		if eventName == "error" || strings.HasSuffix(eventName, ".failed") {
			return true
		}
	}
	return false
}

func gatewayStreamFrameTerminatesResponse(frame []byte) bool {
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(frame)))
	if eventName == "response.completed" || eventName == "response.done" {
		return true
	}
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["type"])))
		if kind == "response.completed" || kind == "response.done" {
			return true
		}
	}
	scanner := bufio.NewScanner(bytes.NewReader(frame))
	scanner.Buffer(make([]byte, 256), 8*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "data:") && strings.TrimSpace(strings.TrimPrefix(line, "data:")) == "[DONE]" {
			return true
		}
	}
	return false
}

func gatewayResponsesFrameClosesConfirmedInterruption(frame []byte) bool {
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(frame)))
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["type"])))
		if eventName != "" && kind != "" && eventName != kind {
			return false
		}
		if kind == "" {
			kind = eventName
		}
		switch kind {
		case "response.output_text.done":
			text, ok := payload["text"].(string)
			return ok && gatewayConfirmedUpstreamInterruptionText(text)
		case "response.content_part.done":
			part, ok := payload["part"].(map[string]any)
			if !ok || !strings.EqualFold(strings.TrimSpace(gatewayStreamFailureString(part["type"])), "output_text") {
				return false
			}
			text, ok := part["text"].(string)
			return ok && gatewayConfirmedUpstreamInterruptionText(text)
		case "response.output_item.done":
			item, ok := payload["item"].(map[string]any)
			if !ok {
				return false
			}
			content, ok := item["content"].([]any)
			if !ok || len(content) != 1 {
				return false
			}
			part, ok := content[0].(map[string]any)
			if !ok || !strings.EqualFold(strings.TrimSpace(gatewayStreamFailureString(part["type"])), "output_text") {
				return false
			}
			text, ok := part["text"].(string)
			return ok && gatewayConfirmedUpstreamInterruptionText(text)
		}
	}
	return false
}

type gatewayStreamFailureDisposition uint8

const (
	gatewayStreamFailureUnknown gatewayStreamFailureDisposition = iota
	gatewayStreamFailurePermanent
	gatewayStreamFailureTransient
)

type gatewayStreamFailureClassification struct {
	Disposition gatewayStreamFailureDisposition
	Status      int
	Found       bool
}

func gatewayStructuredFailureIsAccountBalance(raw []byte, _ gatewayStreamFailureClassification) bool {
	for _, payload := range gatewayStreamJSONPayloads(raw) {
		if !gatewayStreamPayloadHasError(payload) {
			continue
		}
		payloadRaw, err := json.Marshal(payload)
		if err != nil {
			continue
		}
		for _, value := range gatewayStreamFailureValues(payload) {
			for _, key := range []string{"status_code", "status", "code"} {
				status := gatewayStreamHTTPStatus(value[key])
				if connector.IsAccountBalanceFailure(status, payloadRaw) {
					return true
				}
			}
		}
	}
	return false
}

// gatewayStructuredFailureInvalidatesKey recognizes explicit invalid-key
// signals only. Generic permission or model errors must not disable a usable
// key merely because their provider type happens to contain "forbidden".
func gatewayStructuredFailureInvalidatesKey(raw []byte, classification gatewayStreamFailureClassification) bool {
	if connector.IsUpstreamChallengeResponse(classification.Status, raw) {
		return false
	}
	if gatewayStructuredFailureIsAccountBalance(raw, classification) {
		return false
	}
	if classification.Status == http.StatusUnauthorized || classification.Status == http.StatusForbidden {
		return true
	}
	for _, payload := range gatewayStreamJSONPayloads(raw) {
		for _, value := range gatewayStreamFailureValues(payload) {
			for _, key := range []string{"status_code", "status", "code"} {
				status := gatewayStreamHTTPStatus(value[key])
				if status == http.StatusUnauthorized || status == http.StatusForbidden {
					return true
				}
			}
			for _, key := range []string{"type", "code", "error"} {
				if gatewayStructuredFailureInvalidKeyIdentifier(gatewayStreamFailureString(value[key])) {
					return true
				}
			}
			for _, key := range []string{"message", "detail", "error"} {
				if gatewayStructuredFailureInvalidKeyMessage(gatewayStreamFailureString(value[key])) {
					return true
				}
			}
		}
	}
	return false
}

// gatewayStructuredFailureMarksModelUnsupported only accepts a complete
// structured error payload with the provider's explicit model_not_found
// identifier. Ordinary generated text mentioning the phrase is not evidence
// and must not poison the model capability cache.
func gatewayStructuredFailureMarksModelUnsupported(raw []byte) bool {
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(raw)))
	eventDeclaresFailure := eventName == "error" || strings.HasSuffix(eventName, ".failed")
	for _, payload := range gatewayStreamJSONPayloads(raw) {
		if !eventDeclaresFailure && !gatewayStreamPayloadHasError(payload) {
			continue
		}
		for _, value := range gatewayStreamFailureValues(payload) {
			for _, key := range []string{"type", "code", "error"} {
				identifier := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(value[key])))
				identifier = strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(identifier)
				if identifier == "model_not_found" {
					return true
				}
			}
		}
	}
	return false
}

func gatewayStructuredFailureInvalidKeyIdentifier(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(value)
	switch value {
	case "authentication", "authentication_error", "authentication_failed", "invalid_api_key", "invalid_api_key_error", "api_key_invalid", "key_invalid", "invalid_key", "invalid_token", "invalid_token_error", "expired_api_key", "revoked_api_key", "api_key_expired", "api_key_revoked", "token_expired", "token_revoked", "unauthorized":
		return true
	default:
		return false
	}
}

func gatewayStructuredFailureInvalidKeyMessage(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, marker := range []string{
		"invalid api key", "api key is invalid", "api key has expired", "api key expired", "expired api key", "api key has been revoked", "api key revoked", "revoked api key",
		"invalid token", "token is invalid", "token has expired", "token expired", "expired token", "token has been revoked", "token revoked", "revoked token",
		"invalid credential", "credential is invalid", "credentials are invalid",
	} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func gatewayStreamDeclaresFailure(raw []byte) bool {
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(raw)))
	if eventName == "error" || strings.HasSuffix(eventName, ".failed") {
		return true
	}
	for _, payload := range gatewayStreamJSONPayloads(raw) {
		if gatewayStreamPayloadHasError(payload) {
			return true
		}
	}
	return false
}

// classifyGatewayStreamFailure uses protocol fields before message text. An
// unknown SSE error is handled conservatively: replaying an ambiguous request
// can duplicate side effects and must not cool a channel.
func classifyGatewayStreamFailure(raw []byte) gatewayStreamFailureClassification {
	classification := gatewayStreamFailureClassification{}
	// Challenge markers can also appear in ordinary generated text. Require a
	// protocol-level failure signal before treating the frame as a synthetic
	// provider 403 so useful output is never suppressed or replayed.
	if gatewayStreamDeclaresFailure(raw) && connector.IsUpstreamChallengeResponse(http.StatusForbidden, raw) {
		return gatewayStreamFailureClassification{
			Disposition: gatewayStreamFailureTransient,
			Status:      http.StatusForbidden,
			Found:       true,
		}
	}
	for _, payload := range gatewayStreamJSONPayloads(raw) {
		if !gatewayStreamPayloadHasError(payload) {
			continue
		}
		classification.Found = true
		values := gatewayStreamFailureValues(payload)
		for _, value := range values {
			for _, key := range []string{"status_code", "status", "code"} {
				status := gatewayStreamHTTPStatus(value[key])
				if status == 0 {
					continue
				}
				if isTransientGatewayStatus(status) {
					classification.Status = status
					classification.Disposition = gatewayStreamFailureTransient
				} else if status >= http.StatusMultipleChoices {
					classification.Status = status
					classification.Disposition = gatewayStreamFailurePermanent
				} else {
					continue
				}
				return classification
			}
		}
		for _, value := range values {
			for _, key := range []string{"type", "code", "status"} {
				disposition := gatewayStreamFailureIdentifierDisposition(gatewayStreamFailureString(value[key]))
				if disposition != gatewayStreamFailureUnknown {
					classification.Disposition = disposition
					return classification
				}
			}
		}
		for _, value := range values {
			for _, key := range []string{"message", "detail", "error"} {
				disposition := gatewayStreamFailureMessageDisposition(gatewayStreamFailureString(value[key]))
				if disposition != gatewayStreamFailureUnknown {
					classification.Disposition = disposition
					return classification
				}
			}
		}
	}
	if !classification.Found {
		if message := gatewayErrorPayloadText(raw); message != "" {
			classification.Found = true
			classification.Disposition = gatewayStreamFailureMessageDisposition(message)
		}
	}
	return classification
}

func gatewayStreamJSONPayloads(raw []byte) []map[string]any {
	payloads := make([]map[string]any, 0, 2)
	appendPayload := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || value == "[DONE]" {
			return
		}
		var payload map[string]any
		if json.Unmarshal([]byte(value), &payload) == nil && payload != nil {
			payloads = append(payloads, payload)
		}
	}
	appendPayload(string(bytes.TrimSpace(raw)))

	var data []string
	flush := func() {
		if len(data) > 0 {
			appendPayload(strings.Join(data, "\n"))
			data = data[:0]
		}
	}
	scanner := bufio.NewScanner(bytes.NewReader(bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))))
	scanner.Buffer(make([]byte, 1024), gatewayStreamFrameLimit)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	flush()
	return payloads
}

func gatewayStreamPayloadHasError(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	if value, ok := payload["error"]; ok && gatewayStreamErrorValuePresent(value) {
		return true
	}
	if kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["type"]))); kind == "error" || strings.HasSuffix(kind, ".failed") {
		return true
	}
	response, ok := payload["response"].(map[string]any)
	if !ok {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(gatewayStreamFailureString(response["status"])), "failed") {
		return true
	}
	value, ok := response["error"]
	return ok && gatewayStreamErrorValuePresent(value)
}

func gatewayStreamErrorValuePresent(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case map[string]any:
		return len(typed) > 0
	case []any:
		return len(typed) > 0
	case bool:
		return typed
	default:
		return true
	}
}

func gatewayStreamFailureValues(payload map[string]any) []map[string]any {
	values := make([]map[string]any, 0, 4)
	if value, ok := payload["error"].(map[string]any); ok {
		values = append(values, value)
	}
	if response, ok := payload["response"].(map[string]any); ok {
		if value, ok := response["error"].(map[string]any); ok {
			values = append(values, value)
		}
		values = append(values, response)
	}
	return append(values, payload)
}

func gatewayStreamHTTPStatus(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case json.Number:
		status, _ := strconv.Atoi(typed.String())
		return status
	case string:
		status, _ := strconv.Atoi(strings.TrimSpace(typed))
		return status
	default:
		return 0
	}
}

func gatewayStreamFailureString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	default:
		return ""
	}
}

func gatewayStreamFailureIdentifierDisposition(value string) gatewayStreamFailureDisposition {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(value)
	if value == "" || value == "error" || value == "failed" {
		return gatewayStreamFailureUnknown
	}
	for _, marker := range []string{
		"invalid_request", "bad_request", "request_too_large", "context_length", "authentication", "authorization", "unauthorized", "permission", "forbidden",
		"not_found", "unsupported", "content_policy", "content_filter", "policy_violation", "tool_not_allowed", "tool_policy", "invalid_tool", "safety",
		"billing_error", "insufficient_quota",
	} {
		if strings.Contains(value, marker) {
			return gatewayStreamFailurePermanent
		}
	}
	if value == "api_error" {
		return gatewayStreamFailureTransient
	}
	for _, marker := range []string{"overload", "rate_limit", "too_many_requests", "timeout", "service_unavailable", "server_error", "internal_error", "stream_read_error"} {
		if strings.Contains(value, marker) {
			return gatewayStreamFailureTransient
		}
	}
	return gatewayStreamFailureUnknown
}

func gatewayStreamFailureMessageDisposition(value string) gatewayStreamFailureDisposition {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return gatewayStreamFailureUnknown
	}
	for _, marker := range []string{"invalid request", "invalid api key", "authentication", "unauthorized", "permission denied", "forbidden", "not found", "not supported", "unsupported", "content policy", "content filter", "policy violation"} {
		if strings.Contains(value, marker) {
			return gatewayStreamFailurePermanent
		}
	}
	if strings.Contains(value, "tool") && (strings.Contains(value, "not allowed") || strings.Contains(value, "policy") || strings.Contains(value, "unsupported")) {
		return gatewayStreamFailurePermanent
	}
	for _, marker := range []string{"timed out", "timeout", "temporarily unavailable", "service unavailable", "server error", "internal server error", "internal error", "overloaded", "rate limit", "too many requests", "stream_read_error", "stream read failed"} {
		if strings.Contains(value, marker) {
			return gatewayStreamFailureTransient
		}
	}
	return gatewayStreamFailureUnknown
}

func gatewayStreamMetadataEvent(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "response.created", "response.queued", "response.in_progress",
		"message_start", "message_delta", "content_block_start", "ping", "heartbeat", "keep-alive":
		return true
	default:
		return false
	}
}

func gatewayResponsesStructuralEventIsMetadata(name string, payload map[string]any) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "response.output_item.added":
		item, ok := payload["item"].(map[string]any)
		if !ok {
			return false
		}
		itemType, _ := item["type"].(string)
		switch strings.ToLower(strings.TrimSpace(itemType)) {
		case "message":
			return !gatewayStreamValueHasContent(item["content"])
		case "reasoning":
			return !gatewayStreamValueHasContent(item["content"]) &&
				!gatewayStreamValueHasContent(item["summary"]) &&
				!gatewayStreamValueHasContent(item["encrypted_content"])
		case "function_call":
			return !gatewayStreamValueHasContent(item["arguments"])
		case "custom_tool_call":
			return !gatewayStreamValueHasContent(item["input"])
		default:
			return false
		}
	case "response.content_part.added", "response.reasoning_summary_part.added":
		part, ok := payload["part"].(map[string]any)
		if !ok {
			return false
		}
		partType, _ := part["type"].(string)
		switch strings.ToLower(strings.TrimSpace(partType)) {
		case "output_text", "reasoning_text", "summary_text":
			return !gatewayStreamValueHasContent(part["text"])
		case "refusal":
			return !gatewayStreamValueHasContent(part["refusal"])
		default:
			return false
		}
	default:
		return false
	}
}

func gatewayChatCompletionMetadata(choices []any) bool {
	if len(choices) == 0 {
		return true
	}
	for _, item := range choices {
		choice, ok := item.(map[string]any)
		if !ok {
			return false
		}
		if choice["finish_reason"] != nil {
			return false
		}
		delta, ok := choice["delta"].(map[string]any)
		if !ok {
			return false
		}
		for _, key := range []string{"content", "reasoning_content", "tool_calls", "function_call", "audio", "refusal"} {
			if gatewayStreamValueHasContent(delta[key]) {
				return false
			}
		}
	}
	return true
}

func gatewayStreamValueHasContent(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

// isGatewaySSECapacityNotification recognizes canonical Responses capacity
// notices before useful output. Most providers mark the notice with ⚠; one
// known provider emits the exact overload sentence as an unmarked delta.
// Non-streaming bodies and other ordinary text deltas are never retry signals.
func isGatewaySSECapacityNotification(raw []byte) bool {
	matched, _ := gatewaySSECapacityNotificationState(raw)
	return matched
}

func isGatewaySSEUpstreamInterruptionNotification(raw []byte) bool {
	matched, _ := gatewaySSEUpstreamInterruptionNotificationState(raw)
	return matched
}

func gatewaySSEUpstreamInterruptionNotificationPending(raw []byte) bool {
	_, pending := gatewaySSEUpstreamInterruptionNotificationState(raw)
	return pending
}

func gatewaySSEUpstreamInterruptionNotificationState(raw []byte) (matched bool, pending bool) {
	normalized := bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
	var notice strings.Builder
	seen := false
	complete := false
	for _, frame := range bytes.Split(normalized, []byte("\n\n")) {
		if len(bytes.TrimSpace(frame)) == 0 {
			continue
		}
		if complete {
			if gatewayStreamFrameIsMetadata(frame) || gatewayStreamFrameTerminatesResponse(frame) || gatewayResponsesFrameClosesConfirmedInterruption(frame) {
				continue
			}
			return false, false
		}
		if gatewayStreamFrameIsMetadata(frame) {
			continue
		}
		delta, ok := gatewaySSECapacityNoticeDelta(frame)
		if !ok {
			return false, false
		}
		seen = true
		notice.WriteString(delta)
		if notice.Len() > gatewayCapacityNoticePreludeLimit {
			return false, false
		}
		if strings.TrimSpace(notice.String()) != "" && !gateway.IsSessionScopedUnmarkedCapacityNoticePrefix(notice.String()) {
			return false, false
		}
		if gateway.IsSessionScopedCapacityNotice(notice.String()) {
			complete = true
		}
	}
	if complete {
		return true, false
	}
	if !seen || strings.TrimSpace(notice.String()) == "" {
		return false, seen
	}
	return false, gateway.IsSessionScopedUnmarkedCapacityNoticePrefix(notice.String())
}

func gatewayConfirmedUpstreamInterruptionText(text string) bool {
	return gateway.IsSessionScopedCapacityNotice(text) && gateway.IsSessionScopedUnmarkedCapacityNoticePrefix(text)
}

func gatewaySSECapacityNotificationPending(raw []byte) bool {
	_, pending := gatewaySSECapacityNotificationState(raw)
	return pending
}

func gatewaySSECapacityNotificationState(raw []byte) (matched bool, pending bool) {
	normalized := bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
	var notice strings.Builder
	marked := false
	unmarked := false
	for _, frame := range bytes.Split(normalized, []byte("\n\n")) {
		if len(bytes.TrimSpace(frame)) == 0 || gatewayStreamFrameIsMetadata(frame) {
			continue
		}
		delta, ok := gatewaySSECapacityNoticeDelta(frame)
		if !ok {
			return false, false
		}
		if !marked && !unmarked {
			originalDelta := delta
			var hasMarker bool
			delta, hasMarker = trimGatewayCapacityNoticeMarker(delta)
			if hasMarker {
				marked = true
			} else {
				delta = originalDelta
				// One provider emits this exact overload notice as ordinary
				// Responses text rather than a structured error or warning-marked
				// frame. Keep accepting only a prefix of a canonical notice here;
				// arbitrary generated text that merely mentions overload remains
				// ordinary output and is never retried.
				if !gateway.IsSessionScopedUnmarkedCapacityNoticePrefix(delta) {
					return false, false
				}
				unmarked = true
			}
		}
		notice.WriteString(delta)
		if notice.Len() > gatewayCapacityNoticePreludeLimit {
			return false, false
		}
		if gateway.IsSessionScopedCapacityNotice(notice.String()) {
			return true, false
		}
		if strings.TrimSpace(notice.String()) == "" {
			continue
		}
		if !gateway.IsSessionScopedCapacityNoticePrefix(notice.String()) {
			return false, false
		}
	}
	if (!marked && !unmarked) || strings.TrimSpace(notice.String()) == "" {
		return false, marked || unmarked
	}
	if unmarked {
		return false, gateway.IsSessionScopedUnmarkedCapacityNoticePrefix(notice.String())
	}
	return false, gateway.IsSessionScopedCapacityNoticePrefix(notice.String())
}

func gatewaySSECapacityNoticeDelta(frame []byte) (string, bool) {
	eventName := ""
	data := make([]string, 0, 1)
	scanner := bufio.NewScanner(bytes.NewReader(frame))
	scanner.Buffer(make([]byte, 1024), 64*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "event:"):
			eventName = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "event:")))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
	}
	if scanner.Err() != nil || len(data) != 1 {
		return "", false
	}
	var payload map[string]any
	if json.Unmarshal([]byte(data[0]), &payload) != nil {
		return "", false
	}
	kind, _ := payload["type"].(string)
	kind = strings.ToLower(strings.TrimSpace(kind))
	if kind == "" {
		kind = eventName
	}
	if kind != "response.output_text.delta" {
		return "", false
	}
	delta, ok := payload["delta"].(string)
	return delta, ok
}

func trimGatewayCapacityNoticeMarker(text string) (string, bool) {
	text = strings.TrimLeft(text, " \t\r\n")
	if !strings.HasPrefix(text, "⚠") {
		return "", false
	}
	text = strings.TrimPrefix(text, "⚠")
	text = strings.TrimPrefix(text, "\ufe0f")
	return strings.TrimLeft(text, " \t\r\n"), true
}

func gatewayStreamError(raw []byte, upstreamKey string) string {
	text := gatewayErrorPayloadText(raw)
	if text == "" {
		return ""
	}
	return sanitizeGatewayProbeError([]byte(text), upstreamKey)
}

func gatewayErrorPayloadText(raw []byte) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	if text := gatewayErrorJSONText(trimmed); text != "" {
		return text
	}
	scanner := bufio.NewScanner(bytes.NewReader(trimmed))
	scanner.Buffer(make([]byte, 1024), 64*1024)
	errorEvent := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.HasPrefix(line, "event:"):
			errorEvent = strings.EqualFold(strings.TrimSpace(strings.TrimPrefix(line, "event:")), "error")
		case strings.HasPrefix(line, "data:"):
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if text := gatewayErrorJSONText([]byte(payload)); text != "" {
				return text
			}
			if errorEvent && payload != "" && payload != "[DONE]" {
				return payload
			}
		}
	}
	return ""
}

func gatewayErrorJSONText(raw []byte) string {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	if value, ok := payload["error"]; ok {
		return gatewayErrorValueText(value)
	}
	if response, ok := payload["response"].(map[string]any); ok {
		if value, ok := response["error"]; ok {
			return gatewayErrorValueText(value)
		}
		if status, _ := response["status"].(string); strings.EqualFold(strings.TrimSpace(status), "failed") {
			return "response.failed"
		}
	}
	if kind, _ := payload["type"].(string); strings.EqualFold(strings.TrimSpace(kind), "error") {
		return gatewayErrorValueText(payload)
	} else if strings.HasSuffix(strings.ToLower(strings.TrimSpace(kind)), ".failed") {
		return strings.TrimSpace(kind)
	}
	return ""
}

func gatewayErrorValueText(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case map[string]any:
		for _, key := range []string{"message", "detail", "error", "code"} {
			if text := gatewayErrorValueText(typed[key]); text != "" {
				return text
			}
		}
	}
	return ""
}

// gatewaySSEStreamSanitizer keeps upstream error frames out of the downstream
// protocol. A stream with trusted acceptance or useful output cannot be
// replayed safely, so it is terminated with a protocol-shaped generic error
// while the sanitized upstream detail remains available to the administrator
// log.
type gatewaySSEStreamSanitizer struct {
	waitingProbeFailed             func() bool
	responseModel                  string
	source                         *bufio.Reader
	closer                         io.Closer
	raw                            *gatewayTailBuffer
	upstreamKey                    string
	requestPath                    string
	onUseful                       func()
	onAccepted                     func()
	onUsefulEvidence               func(gatewayStreamAcceptanceEvidence) error
	onAcceptedEvidence             func(gatewayStreamAcceptanceEvidence) error
	clientCanceled                 func() bool
	ownerLeaseLost                 func() bool
	accepted                       bool
	useful                         bool
	detectCapacityNotice           bool
	detectInterruption             bool
	capacityPending                []byte
	interruptionPending            []byte
	interruptionComplete           bool
	frame                          []byte
	pending                        []byte
	suppressed                     bool
	overflowed                     bool
	errorText                      string
	warningText                    string
	evidence                       gatewayStreamFailureEvidence
	sourceEOF                      bool
	sourceErr                      error
	ownerLossObserved              bool
	protocolTerminal               bool
	syntheticTerminal              bool
	syntheticSequence              int64
	responseSequenceSeen           bool
	responseNextSequence           int64
	responsePublishedNextSequence  int64
	responseID                     string
	pendingResponseID              string
	responseSnapshot               map[string]any
	pendingResponse                map[string]any
	pendingResponseBytes           int
	pendingSequenceNext            int64
	pendingSequenceBytes           int
	pendingOutputStarted           bool
	downstreamOutputSeen           bool
	syntheticPrefixBytes           int
	syntheticOutputSeen            bool
	retryPromptEnabled             bool
	retryPromptEligible            bool
	retryPrompted                  bool
	retryPromptStructureCompatible bool
	retryPromptResponseCreated     bool
	retryPromptOutputItemSeen      bool
	retryPromptItemStarted         bool
	retryPromptPartStarted         bool
	retryPromptMessageID           string
	retryPromptResponseID          string
	retryPromptText                string
	retryPromptVisibleText         string
	retryPromptTextDeltaSeen       bool
	retryPromptTransportFailure    bool
	retryPromptLastSequence        int64
	retryPromptSequenceSeen        bool
	retryPromptSequenceComplete    bool
}

type gatewayStreamFailureEvidence struct {
	InvalidatesKey             bool
	ModelUnsupported           bool
	AccountBalanceInsufficient bool
}

func newGatewaySSEStreamSanitizer(body io.ReadCloser, raw *gatewayTailBuffer, upstreamKey, requestPath string, onUseful ...func()) *gatewaySSEStreamSanitizer {
	sanitizer := &gatewaySSEStreamSanitizer{
		source:                         bufio.NewReaderSize(body, 32*1024),
		closer:                         body,
		raw:                            raw,
		upstreamKey:                    upstreamKey,
		requestPath:                    requestPath,
		retryPromptStructureCompatible: true,
		retryPromptSequenceComplete:    true,
	}
	if len(onUseful) > 0 {
		sanitizer.onUseful = onUseful[0]
	}
	return sanitizer
}

func (s *gatewaySSEStreamSanitizer) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.handleOwnerLeaseLossBeforeReadOrOutput()
	for len(s.pending) == 0 && !s.sourceEOF {
		if err := s.readFrame(); err != nil && !errors.Is(err, io.EOF) {
			if !s.protocolTerminal {
				s.finishSourceError(err)
			}
			s.sourceEOF = true
		}
	}
	s.handleOwnerLeaseLossBeforeReadOrOutput()
	if len(s.pending) > 0 {
		n := copy(p, s.pending)
		if n > 0 {
			s.downstreamOutputSeen = true
		}
		if s.pendingResponseBytes > 0 {
			if n >= s.pendingResponseBytes {
				s.responseID = s.pendingResponseID
				if s.pendingResponse != nil {
					s.responseSnapshot = cloneGatewayResponsesObject(s.pendingResponse)
				}
				s.pendingResponseID = ""
				s.pendingResponse = nil
				s.pendingResponseBytes = 0
			} else {
				s.pendingResponseBytes -= n
			}
		}
		if s.pendingSequenceBytes > 0 {
			if n >= s.pendingSequenceBytes {
				if s.pendingSequenceNext > s.responsePublishedNextSequence {
					s.responsePublishedNextSequence = s.pendingSequenceNext
				}
				s.pendingSequenceNext = 0
				s.pendingSequenceBytes = 0
			} else {
				s.pendingSequenceBytes -= n
			}
		}
		if s.syntheticTerminal && !s.syntheticOutputSeen {
			if n > s.syntheticPrefixBytes {
				s.syntheticOutputSeen = true
			}
			if n >= s.syntheticPrefixBytes {
				s.syntheticPrefixBytes = 0
			} else {
				s.syntheticPrefixBytes -= n
			}
		}
		if n == len(s.pending) {
			s.pending = nil
			s.pendingOutputStarted = false
			s.pendingResponseID = ""
			s.pendingResponse = nil
			s.pendingResponseBytes = 0
			s.pendingSequenceNext = 0
			s.pendingSequenceBytes = 0
		} else {
			s.pending = s.pending[n:]
			s.pendingOutputStarted = true
		}
		return n, nil
	}
	if s.sourceErr != nil {
		err := s.sourceErr
		s.sourceErr = nil
		return 0, err
	}
	return 0, io.EOF
}

func (s *gatewaySSEStreamSanitizer) Close() error {
	if s == nil || s.closer == nil {
		return nil
	}
	return s.closer.Close()
}

func (s *gatewaySSEStreamSanitizer) ErrorText() string {
	if s == nil {
		return ""
	}
	return s.errorText
}

func (s *gatewaySSEStreamSanitizer) WarningText() string {
	if s == nil {
		return ""
	}
	return s.warningText
}

func (s *gatewaySSEStreamSanitizer) FailureEvidence() gatewayStreamFailureEvidence {
	if s == nil {
		return gatewayStreamFailureEvidence{}
	}
	return s.evidence
}

func (s *gatewaySSEStreamSanitizer) RetryPrompted() bool {
	return s != nil && s.retryPrompted
}

func (s *gatewaySSEStreamSanitizer) OwnerLeaseLost() bool {
	return s != nil && s.ownerLossObserved
}

func (s *gatewaySSEStreamSanitizer) handleOwnerLeaseLossBeforeReadOrOutput() bool {
	if s == nil || s.ownerLeaseLost == nil || !s.ownerLeaseLost() {
		return false
	}
	if s.ownerLossObserved && s.syntheticTerminal {
		s.sourceEOF = true
		return true
	}
	s.ownerLossObserved = true
	s.capacityPending = nil
	s.interruptionPending = nil
	s.interruptionComplete = false
	if s.protocolTerminal && s.pendingOutputStarted && len(s.pending) > 0 {
		// A legal terminal that has reached the caller cannot be replaced without
		// producing two conflicting terminal results. Finish its remaining bytes.
		s.sourceEOF = true
		return true
	}
	if s.protocolTerminal && len(s.pending) == 0 {
		// The terminal has already reached the caller. A late fence stops further
		// reads without creating a second protocol terminal.
		s.sourceEOF = true
		return true
	}
	if !s.accepted && !s.useful {
		if s.downstreamOutputSeen {
			if s.errorText == "" {
				s.errorText = "stream stopped after request dispatch owner lease loss"
			}
			if s.syntheticTerminal {
				if s.pendingOutputStarted || s.syntheticOutputSeen {
					s.sourceEOF = true
					return true
				}
				s.replacePendingWithSyntheticTerminal()
				s.sourceEOF = true
				return true
			}
			if !s.pendingOutputStarted {
				s.pending = nil
				s.pendingResponseID = ""
				s.pendingResponse = nil
				s.pendingResponseBytes = 0
				s.pendingSequenceNext = 0
				s.pendingSequenceBytes = 0
			}
			s.appendPublicStreamErrorFrameWithMinimumSequence(nil, s.responseNextSequence)
			s.sourceEOF = true
			return true
		}
		s.pending = nil
		s.pendingOutputStarted = false
		s.pendingResponseID = ""
		s.pendingResponse = nil
		s.pendingResponseBytes = 0
		s.pendingSequenceNext = 0
		s.pendingSequenceBytes = 0
		s.resetFrame()
		if s.sourceErr == nil {
			s.sourceErr = gateway.ErrRequestDispatchOwnerLeaseLost
		}
		s.sourceEOF = true
		return true
	}
	if s.errorText == "" {
		s.errorText = "stream stopped after request dispatch owner lease loss"
	}
	if s.syntheticTerminal {
		if s.pendingOutputStarted || s.syntheticOutputSeen {
			s.sourceEOF = true
			return true
		}
		// EOF can stage an upstream frame and its synthetic terminal together.
		// Discard every unsent upstream byte while preserving the terminal's
		// already assigned Responses sequence number.
		s.replacePendingWithSyntheticTerminal()
	} else {
		minimumSequence := s.responseNextSequence
		if !s.pendingOutputStarted {
			s.pending = nil
			s.pendingResponseID = ""
			s.pendingResponse = nil
			s.pendingResponseBytes = 0
			s.pendingSequenceNext = 0
			s.pendingSequenceBytes = 0
		}
		s.appendPublicStreamErrorFrameWithMinimumSequence(nil, minimumSequence)
	}
	s.sourceEOF = true
	return true
}

func (s *gatewaySSEStreamSanitizer) replacePendingWithSyntheticTerminal() {
	s.pendingResponseID = ""
	s.pendingResponse = nil
	s.pendingResponseBytes = 0
	s.pendingSequenceNext = 0
	s.pendingSequenceBytes = 0
	if s.retryPrompted && !s.useful {
		s.pending = s.retryPromptFrames(s.syntheticSequence, s.responseID, s.responseSnapshot)
	} else {
		s.pending = gatewayPublicStreamErrorFrameWithResponse(nil, s.requestPath, s.syntheticSequence, s.responseID, s.responseSnapshot)
		s.retryPrompted = false
	}
	s.pendingOutputStarted = false
	s.syntheticPrefixBytes = 0
	s.syntheticOutputSeen = false
}

func (s *gatewaySSEStreamSanitizer) readFrame() error {
	for {
		piece, err := s.source.ReadSlice('\n')
		if len(piece) > 0 {
			if s.raw != nil {
				_, _ = s.raw.Write(piece)
			}
			s.appendFramePiece(piece)
			if s.overflowed {
				s.finishFrame()
				return nil
			}
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(s.frame) > 0 || s.suppressed {
					if gatewayResponsesFrameHasMalformedJSON(s.frame, s.requestPath) {
						s.suppressed = true
						if s.errorText == "" {
							s.errorText = "upstream Responses stream ended with a truncated JSON frame"
						}
					}
					s.finishFrame()
				}
				if len(s.interruptionPending) > 0 && !s.sourceEOF {
					if s.interruptionComplete {
						s.finishConfirmedUpstreamInterruption(nil)
					} else {
						s.emitNormalFrame(s.interruptionPending)
						s.interruptionPending = nil
					}
				}
				if len(s.capacityPending) > 0 && !s.sourceEOF {
					s.emitNormalFrame(s.capacityPending)
					s.capacityPending = nil
				}
				if !s.sourceEOF {
					s.finishSourceEOF()
				}
				return io.EOF
			}
			if len(s.interruptionPending) > 0 {
				if s.interruptionComplete {
					s.finishConfirmedUpstreamInterruption(nil)
					return nil
				}
				s.emitNormalFrame(s.interruptionPending)
				s.interruptionPending = nil
			}
			return err
		}
		if len(bytes.TrimSpace(piece)) == 0 {
			s.finishFrame()
			return nil
		}
	}
}

func gatewayResponsesFrameHasMalformedJSON(frame []byte, requestPath string) bool {
	if !strings.Contains(strings.ToLower(requestPath), "/responses") {
		return false
	}
	data := make([]string, 0, 1)
	eventName := ""
	scanner := bufio.NewScanner(bytes.NewReader(bytes.ReplaceAll(frame, []byte("\r\n"), []byte("\n"))))
	scanner.Buffer(make([]byte, 256), gatewayStreamFrameLimit)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event:") {
			eventName = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(line, "event:")))
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
	}
	if scanner.Err() != nil {
		return true
	}
	if len(data) == 0 {
		switch eventName {
		case "", "ping", "heartbeat", "keep-alive":
			return false
		default:
			return true
		}
	}
	payload := strings.TrimSpace(strings.Join(data, "\n"))
	if payload == "" {
		return true
	}
	if payload == "[DONE]" {
		return false
	}
	var object map[string]any
	return json.Unmarshal([]byte(payload), &object) != nil || object == nil
}

func (s *gatewaySSEStreamSanitizer) appendFramePiece(piece []byte) {
	if len(s.frame)+len(piece) > gatewayStreamFrameLimit {
		remaining := gatewayStreamFrameLimit - len(s.frame)
		if remaining > 0 {
			s.frame = append(s.frame, piece[:remaining]...)
		}
		s.suppressed = true
		s.overflowed = true
		return
	}
	s.frame = append(s.frame, piece...)
	if gatewayStreamFrameHasError(s.frame) {
		s.suppressed = true
	}
}

func gatewayFrameHasVisibleThinkingMarkup(frame []byte) bool {
	text := strings.ToLower(string(frame))
	return strings.Contains(text, "<thinking>") ||
		strings.Contains(text, "</thinking>") ||
		strings.Contains(text, `\u003cthinking\u003e`) ||
		strings.Contains(text, `\u003c/thinking\u003e`)
}

func appendGatewayRouteWarning(details, warning string) string {
	warning = truncateGatewayText(sanitizeGatewayProbeError([]byte(warning), ""), gatewayRouteDetailPartLimit)
	if warning == "" {
		return truncateGatewayText(details, gatewayRouteDetailsLimit)
	}
	if strings.TrimSpace(details) == "" {
		return truncateGatewayText("response warning: "+warning, gatewayRouteDetailsLimit)
	}
	return truncateGatewayText(details+"; response warning: "+warning, gatewayRouteDetailsLimit)
}

func (s *gatewaySSEStreamSanitizer) finishFrame() {
	if !s.overflowed {
		if model := parseGatewayStreamUsageMetricsForPath(s.frame, s.requestPath, s.upstreamKey).ResponseModel; model != "" {
			s.responseModel = model
		}
	}
	malformedResponsesFrame := !s.overflowed && gatewayResponsesFrameHasMalformedJSON(s.frame, s.requestPath)
	if malformedResponsesFrame {
		s.suppressed = true
		if s.errorText == "" {
			s.errorText = "upstream Responses stream contained a malformed JSON frame"
		}
		// A malformed frame after trusted acceptance is non-replayable. If the
		// published lifecycle is still one reconstructable assistant text part,
		// complete that part with the fixed retry suffix; otherwise retain the
		// normal failed terminal. The durable request record retains the real
		// malformed-frame error in either case.
		if s.accepted && strings.Contains(strings.ToLower(s.requestPath), "/responses") {
			s.retryPromptEligible = true
		}
	}
	s.frame = gatewayNormalizeResponsesCompletedFrame(s.frame, s.responseNextSequence, s.responseSequenceSeen)
	frameHasError := gatewayStreamFrameHasError(s.frame)
	invalidTerminal := gatewayStreamFrameHasInvalidProtocolTerminal(s.frame, s.requestPath)
	if invalidTerminal {
		s.suppressed = true
		if s.errorText == "" {
			s.errorText = "upstream returned a malformed protocol terminal frame"
		}
	}
	if s.suppressed || frameHasError {
		if frameHasError && !s.overflowed {
			s.observeResponsesRetryPromptSuppressedFrame(s.frame)
			classification := classifyGatewayStreamFailure(s.frame)
			s.retryPromptEligible = classification.Disposition == gatewayStreamFailureTransient
			s.evidence.AccountBalanceInsufficient = s.evidence.AccountBalanceInsufficient || gatewayStructuredFailureIsAccountBalance(s.frame, classification)
			s.evidence.InvalidatesKey = s.evidence.InvalidatesKey || gatewayStructuredFailureInvalidatesKey(s.frame, classification)
			s.evidence.ModelUnsupported = s.evidence.ModelUnsupported || gatewayStructuredFailureMarksModelUnsupported(s.frame)
		}
		if failure := gatewayStreamError(s.frame, s.upstreamKey); failure != "" && s.errorText == "" {
			s.errorText = failure
		}
		if s.overflowed && s.errorText == "" {
			s.errorText = fmt.Sprintf("upstream SSE frame exceeded %d-byte safety limit", gatewayStreamFrameLimit)
		}
		if s.errorText == "" {
			s.errorText = gatewayPublicStreamFailureMessage
		}
		s.appendPublicStreamErrorFrame(s.frame)
		s.sourceEOF = true
	} else {
		s.observeProtocolFrame(s.frame)
		if evidence, accepted := gatewayStreamFrameAcceptanceEvidence(s.frame, s.requestPath); !s.accepted && accepted {
			s.accepted = true
			if s.onAcceptedEvidence != nil {
				if err := s.onAcceptedEvidence(evidence); err != nil {
					s.finishAcceptanceAuditError(err)
					s.resetFrame()
					return
				}
			}
			if s.onAccepted != nil {
				s.onAccepted()
			}
		}
		if s.handleUpstreamInterruptionFrame() {
			s.resetFrame()
			return
		}
		if s.handleCapacityFrame() {
			s.resetFrame()
			return
		}
		s.emitNormalFrame(s.frame)
	}
	s.resetFrame()
}

func (s *gatewaySSEStreamSanitizer) handleUpstreamInterruptionFrame() bool {
	if !s.detectInterruption && !s.detectCapacityNotice {
		return false
	}
	if s.interruptionComplete {
		if gatewayStreamFrameTerminatesResponse(s.frame) {
			s.finishConfirmedUpstreamInterruption(s.frame)
			return true
		}
		if gatewayStreamFrameIsMetadata(s.frame) || gatewayResponsesFrameClosesConfirmedInterruption(s.frame) {
			s.interruptionPending = append(s.interruptionPending, s.frame...)
			return true
		}
		combined := append([]byte(nil), s.interruptionPending...)
		combined = append(combined, s.frame...)
		s.interruptionPending = nil
		s.interruptionComplete = false
		s.emitNormalFrame(combined)
		return true
	}
	combined := append([]byte(nil), s.interruptionPending...)
	combined = append(combined, s.frame...)
	matched, pending := gatewaySSEUpstreamInterruptionNotificationState(combined)
	if matched {
		s.interruptionPending = combined
		s.interruptionComplete = true
		return true
	}
	if pending {
		s.interruptionPending = combined
		return true
	}
	if len(s.interruptionPending) > 0 {
		s.interruptionPending = nil
		s.emitNormalFrame(combined)
		return true
	}
	return false
}

func (s *gatewaySSEStreamSanitizer) finishConfirmedUpstreamInterruption(frame []byte) {
	s.interruptionPending = nil
	s.interruptionComplete = false
	s.retryPromptEligible = true
	if s.errorText == "" {
		s.errorText = gatewayConfirmedUpstreamInterruptionMessage
	}
	s.appendPublicStreamErrorFrame(frame)
	s.sourceEOF = true
}

func (s *gatewaySSEStreamSanitizer) handleCapacityFrame() bool {
	if !s.detectCapacityNotice || s.useful {
		return false
	}
	combined := append([]byte(nil), s.capacityPending...)
	combined = append(combined, s.frame...)
	matched, pending := gatewaySSECapacityNotificationState(combined)
	if matched {
		s.capacityPending = nil
		s.retryPromptEligible = true
		if s.errorText == "" {
			s.errorText = "selected model capacity notice"
		}
		s.appendPublicStreamErrorFrame(s.frame)
		s.sourceEOF = true
		return true
	}
	if pending {
		s.capacityPending = combined
		return true
	}
	if len(s.capacityPending) > 0 {
		s.emitNormalFrame(s.capacityPending)
		s.capacityPending = nil
		if s.sourceEOF {
			return true
		}
	}
	return false
}

func (s *gatewaySSEStreamSanitizer) emitNormalFrame(frame []byte) {
	if strings.Contains(strings.ToLower(s.requestPath), "/responses") {
		s.pendingResponseID = gatewayResponsesStreamResponseID(frame)
		s.pendingResponse = gatewayResponsesStreamResponse(frame)
	}
	metadata := gatewayStreamFrameIsMetadata(frame)
	if !metadata {
		if !s.accepted && s.onUsefulEvidence != nil {
			// Same rule as the prelude: a visible frame is output, so it fences
			// the dispatch even when its shape matches no known protocol event.
			// Only a durable persistence failure may terminate the stream here.
			evidence, _ := gatewayStreamUsefulAcceptanceEvidence(frame, s.requestPath)
			s.accepted = true
			if err := s.onUsefulEvidence(evidence); err != nil {
				s.finishAcceptanceAuditError(err)
				return
			}
		}
		s.useful = true
		if s.onUseful != nil {
			s.onUseful()
		}
	}
	if s.warningText == "" && !metadata && gatewayFrameHasVisibleThinkingMarkup(frame) {
		s.warningText = "检测到响应正文包含 <thinking> 思考标签"
	}
	// Capacity/interruption frames are inspected for protocol structure but
	// withheld from the caller. Synthetic terminals may include only text that
	// passed the normal-output path, never a withheld provider error message.
	s.retryPromptVisibleText = s.retryPromptText
	s.pending = append(s.pending, frame...)
	if s.pendingResponseID != "" || s.pendingResponse != nil {
		s.pendingResponseBytes = len(s.pending)
	}
	if sequence, ok := gatewayResponsesStreamSequenceNumber(frame); ok {
		s.pendingSequenceNext = sequence + 1
		s.pendingSequenceBytes = len(s.pending)
	}
}

func (s *gatewaySSEStreamSanitizer) finishAcceptanceAuditError(err error) {
	s.pendingResponseID = ""
	s.pendingResponse = nil
	s.pendingResponseBytes = 0
	s.pendingSequenceNext = 0
	s.pendingSequenceBytes = 0
	if gateway.IsRequestDispatchOwnerLeaseLost(err) {
		s.ownerLossObserved = true
		if s.errorText == "" {
			s.errorText = "stream stopped after request dispatch owner lease loss"
		}
		s.capacityPending = nil
		s.appendPublicStreamErrorFrame(s.frame)
		s.sourceEOF = true
		return
	}
	if s.errorText == "" {
		s.errorText = gatewayDispatchAuditFailure("persist upstream acceptance before downstream output", err)
	}
	s.capacityPending = nil
	s.appendPublicStreamErrorFrame(s.frame)
	s.sourceEOF = true
}

func (s *gatewaySSEStreamSanitizer) finishSourceEOF() {
	if s.protocolTerminal || (!s.accepted && !s.useful) {
		s.sourceEOF = true
		return
	}
	s.finishSourceError(io.ErrUnexpectedEOF)
	s.sourceEOF = true
}

func (s *gatewaySSEStreamSanitizer) finishSourceError(err error) {
	if err == nil {
		return
	}
	if s.ownerLeaseLost != nil && s.ownerLeaseLost() {
		s.ownerLossObserved = true
		if !s.accepted && !s.useful {
			s.sourceErr = err
			return
		}
		if s.errorText == "" {
			s.errorText = "stream stopped after request dispatch owner lease loss"
		}
		s.capacityPending = nil
		s.appendPublicStreamErrorFrame(nil)
		return
	}
	if s.clientCanceled != nil && s.clientCanceled() {
		s.sourceErr = err
		return
	}
	if !s.accepted && !s.useful {
		s.sourceErr = err
		return
	}
	if s.errorText == "" {
		s.errorText = "stream read failed after upstream acceptance: " + sanitizeGatewayProbeError([]byte(err.Error()), s.upstreamKey)
	}
	s.capacityPending = nil
	s.retryPromptEligible = true
	s.retryPromptTransportFailure = true
	s.appendPublicStreamErrorFrame(nil)
}

func (s *gatewaySSEStreamSanitizer) observeProtocolFrame(frame []byte) {
	path := strings.ToLower(s.requestPath)
	if strings.Contains(path, "/responses") {
		s.observeResponsesRetryPromptStructure(frame)
		if sequence, ok := gatewayResponsesStreamSequenceNumber(frame); ok {
			next := sequence + 1
			if !s.responseSequenceSeen || next > s.responseNextSequence {
				s.responseSequenceSeen = true
				s.responseNextSequence = next
			}
		}
		if gatewayResponsesCompletedFrameIsTerminal(frame) {
			s.protocolTerminal = true
		}
		return
	}
	if strings.Contains(path, "/messages") {
		if gatewayClaudeMessageStopFrameIsTerminal(frame) {
			s.protocolTerminal = true
		}
		return
	}
	if (path == "/v1/chat/completions" || path == "/v1/completions") && gatewaySSEFrameHasDone(frame) {
		s.protocolTerminal = true
	}
}

func (s *gatewaySSEStreamSanitizer) observeResponsesRetryPromptStructure(frame []byte) {
	if s == nil || !s.retryPromptStructureCompatible {
		return
	}
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(frame)))
	payloads := gatewayStreamJSONPayloads(frame)
	if len(payloads) == 0 {
		switch eventName {
		case "", "ping", "heartbeat", "keep-alive":
			return
		default:
			s.retryPromptStructureCompatible = false
			return
		}
	}
	if len(payloads) != 1 {
		s.retryPromptStructureCompatible = false
		return
	}
	for _, payload := range payloads {
		kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["type"])))
		if kind == "" || (eventName != "" && eventName != kind) || !s.observeResponsesRetryPromptSequence(payload) {
			s.retryPromptStructureCompatible = false
			return
		}
		switch kind {
		case "response.created":
			if s.retryPromptResponseCreated {
				s.retryPromptStructureCompatible = false
				return
			}
			response, ok := payload["response"].(map[string]any)
			responseID := strings.TrimSpace(gatewayStreamFailureString(response["id"]))
			status := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(response["status"])))
			if !ok || !gatewayAcceptedStreamID(response["id"]) || (status != "" && status != "queued" && status != "in_progress") || gatewayStreamErrorValuePresent(response["error"]) || gatewayStreamValueHasContent(response["output"]) {
				s.retryPromptStructureCompatible = false
				return
			}
			s.retryPromptResponseCreated = true
			s.retryPromptResponseID = responseID
		case "response.queued", "response.in_progress":
			response, ok := payload["response"].(map[string]any)
			responseID := strings.TrimSpace(gatewayStreamFailureString(response["id"]))
			status := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(response["status"])))
			expectedStatus := strings.TrimPrefix(kind, "response.")
			if !s.retryPromptResponseCreated || s.retryPromptItemStarted || !ok || responseID == "" || responseID != s.retryPromptResponseID || (status != "" && status != expectedStatus) || gatewayStreamErrorValuePresent(response["error"]) || gatewayStreamValueHasContent(response["output"]) {
				s.retryPromptStructureCompatible = false
				return
			}
		case "response.output_item.added":
			if !s.retryPromptResponseCreated || s.retryPromptOutputItemSeen {
				s.retryPromptStructureCompatible = false
				return
			}
			outputIndex, ok := gatewayResponsesStreamIndex(payload["output_index"])
			item, itemOK := payload["item"].(map[string]any)
			itemID := strings.TrimSpace(gatewayStreamFailureString(item["id"]))
			itemType := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(item["type"])))
			itemStatus := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(item["status"])))
			itemRole := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(item["role"])))
			responseID := strings.TrimSpace(gatewayStreamFailureString(payload["response_id"]))
			if !ok || outputIndex != 0 || !itemOK || itemType != "message" || itemID == "" || itemRole != "assistant" || (itemStatus != "" && itemStatus != "in_progress") || (responseID != "" && responseID != s.retryPromptResponseID) || gatewayStreamValueHasContent(item["content"]) {
				s.retryPromptStructureCompatible = false
				return
			}
			s.retryPromptOutputItemSeen = true
			s.retryPromptItemStarted = true
			s.retryPromptMessageID = itemID
		case "response.content_part.added":
			outputIndex, outputOK := gatewayResponsesStreamIndex(payload["output_index"])
			contentIndex, contentOK := gatewayResponsesStreamIndex(payload["content_index"])
			part, partOK := payload["part"].(map[string]any)
			partType := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(part["type"])))
			itemID := strings.TrimSpace(gatewayStreamFailureString(payload["item_id"]))
			responseID := strings.TrimSpace(gatewayStreamFailureString(payload["response_id"]))
			partText, partTextPresent := part["text"]
			annotations, annotationsPresent := part["annotations"]
			logprobs, logprobsPresent := part["logprobs"]
			if !s.retryPromptItemStarted || s.retryPromptPartStarted || !outputOK || outputIndex != 0 || !contentOK || contentIndex != 0 || !partOK || partType != "output_text" || itemID != s.retryPromptMessageID || (responseID != "" && responseID != s.retryPromptResponseID) || (partTextPresent && partText != "") || (annotationsPresent && gatewayStreamValueHasContent(annotations)) || (logprobsPresent && gatewayStreamValueHasContent(logprobs)) {
				s.retryPromptStructureCompatible = false
				return
			}
			s.retryPromptPartStarted = true
		case "response.output_text.delta":
			outputIndex, outputOK := gatewayResponsesStreamIndex(payload["output_index"])
			contentIndex, contentOK := gatewayResponsesStreamIndex(payload["content_index"])
			itemID := strings.TrimSpace(gatewayStreamFailureString(payload["item_id"]))
			responseID := strings.TrimSpace(gatewayStreamFailureString(payload["response_id"]))
			delta, deltaOK := payload["delta"].(string)
			logprobs, logprobsPresent := payload["logprobs"]
			if !s.retryPromptPartStarted || !outputOK || outputIndex != 0 || !contentOK || contentIndex != 0 || itemID != s.retryPromptMessageID || (responseID != "" && responseID != s.retryPromptResponseID) || !deltaOK || (logprobsPresent && gatewayStreamValueHasContent(logprobs)) || len(s.retryPromptText)+len(delta) > gatewayResponsesRetryPromptTextLimit {
				s.retryPromptStructureCompatible = false
				return
			}
			s.retryPromptText += delta
			s.retryPromptTextDeltaSeen = true
		case "response.output_text.done", "response.output_item.done", "response.content_part.done":
			s.retryPromptStructureCompatible = false
			return
		case "response.failed":
			response, _ := payload["response"].(map[string]any)
			responseID := strings.TrimSpace(gatewayStreamFailureString(response["id"]))
			if responseID != "" && responseID != s.retryPromptResponseID {
				s.retryPromptStructureCompatible = false
				return
			}
		default:
			s.retryPromptStructureCompatible = false
			return
		}
	}
}

func (s *gatewaySSEStreamSanitizer) observeResponsesRetryPromptSequence(payload map[string]any) bool {
	value, present := payload["sequence_number"]
	if !present {
		s.retryPromptSequenceComplete = false
		// A provider may omit sequence numbers from a pre-output lifecycle. Keep
		// the structure usable while no answer bytes have been published; a
		// partially visible answer still fails the completeness check below.
		return !s.useful
	}
	sequenceValue, ok := value.(float64)
	if !ok || sequenceValue < 0 || sequenceValue > 9007199254740991 {
		s.retryPromptSequenceComplete = false
		return !s.useful
	}
	sequence := int64(sequenceValue)
	if float64(sequence) != sequenceValue || (s.retryPromptSequenceSeen && sequence <= s.retryPromptLastSequence) {
		s.retryPromptSequenceComplete = false
		return !s.useful
	}
	s.retryPromptSequenceSeen = true
	s.retryPromptLastSequence = sequence
	return true
}

func (s *gatewaySSEStreamSanitizer) observeResponsesRetryPromptSuppressedFrame(frame []byte) {
	if s == nil || !s.retryPromptStructureCompatible || !strings.Contains(strings.ToLower(s.requestPath), "/responses") {
		return
	}
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["type"])))
		switch kind {
		case "response.failed":
			response, _ := payload["response"].(map[string]any)
			responseID := strings.TrimSpace(gatewayStreamFailureString(response["id"]))
			if responseID != "" && responseID != s.retryPromptResponseID {
				s.retryPromptStructureCompatible = false
				return
			}
		case "response.created", "response.queued", "response.in_progress", "response.output_item.added", "response.content_part.added":
			// Suppressed bytes never reach the caller and cannot advance the
			// lifecycle that the synthetic completion must continue.
			s.retryPromptStructureCompatible = false
			return
		}
	}
}

func gatewayResponsesStreamIndex(value any) (int, bool) {
	number, ok := value.(float64)
	if !ok || number < 0 || number > float64(^uint(0)>>1) {
		return 0, false
	}
	index := int(number)
	return index, float64(index) == number
}

func gatewayResponsesCompletedFrameIsTerminal(frame []byte) bool {
	_, completed := gatewayResponsesCompletedFrameEvidence(frame)
	return completed
}

func gatewayNormalizeResponsesCompletedFrame(frame []byte, fallbackSequence int64, sequenceSeen bool) []byte {
	if _, completed := gatewayResponsesCompletedFrameEvidence(frame); !completed {
		return frame
	}
	payloads := gatewayStreamJSONPayloads(frame)
	if len(payloads) != 1 {
		return frame
	}
	payload := payloads[0]
	changed := false
	sequence, sequencePresent := payload["sequence_number"].(float64)
	if _, present := payload["sequence_number"]; !present ||
		(sequenceSeen && sequencePresent && sequence < float64(fallbackSequence)) {
		if fallbackSequence < 0 {
			fallbackSequence = 0
		}
		payload["sequence_number"] = fallbackSequence
		changed = true
	}
	response, _ := payload["response"].(map[string]any)
	if _, present := response["status"]; !present {
		response["status"] = "completed"
		changed = true
	}
	if !changed {
		return frame
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return frame
	}
	return append([]byte("event: response.completed\ndata: "), append(encoded, '\n', '\n')...)
}

func gatewayResponsesCompletedFrameEvidence(frame []byte) (gatewayStreamAcceptanceEvidence, bool) {
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(frame)))
	payloads := gatewayStreamJSONPayloads(frame)
	if len(payloads) != 1 {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	payload := payloads[0]
	kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payload["type"])))
	if kind != "response.completed" || (eventName != "" && eventName != kind) {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	if sequenceValue, present := payload["sequence_number"]; present {
		sequence, ok := sequenceValue.(float64)
		if !ok || sequence < 0 || sequence > 9007199254740991 || sequence != float64(int64(sequence)) {
			return gatewayStreamAcceptanceEvidence{}, false
		}
	}
	response, ok := payload["response"].(map[string]any)
	if !ok || !gatewayAcceptedStreamID(response["id"]) {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	if status, present := response["status"]; present &&
		!strings.EqualFold(strings.TrimSpace(gatewayStreamFailureString(status)), "completed") {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	if value, present := response["error"]; present && gatewayStreamErrorValuePresent(value) {
		return gatewayStreamAcceptanceEvidence{}, false
	}
	return gatewayStreamAcceptanceEvidence{
		EventType: kind,
		ObjectID:  strings.TrimSpace(gatewayStreamFailureString(response["id"])),
	}, true
}

func gatewayClaudeMessageStopFrameIsTerminal(frame []byte) bool {
	eventName := strings.ToLower(strings.TrimSpace(gatewaySSEEventName(frame)))
	payloads := gatewayStreamJSONPayloads(frame)
	if len(payloads) != 1 {
		return false
	}
	kind := strings.ToLower(strings.TrimSpace(gatewayStreamFailureString(payloads[0]["type"])))
	return kind == "message_stop" && (eventName == "" || eventName == kind)
}

func gatewayStreamFrameHasInvalidProtocolTerminal(frame []byte, requestPath string) bool {
	path := strings.ToLower(strings.TrimSpace(requestPath))
	switch {
	case strings.Contains(path, "/responses"):
		return gatewayStreamFrameHasEventType(frame, "response.completed") && !gatewayResponsesCompletedFrameIsTerminal(frame)
	case strings.Contains(path, "/messages"):
		return gatewayStreamFrameHasEventType(frame, "message_stop") && !gatewayClaudeMessageStopFrameIsTerminal(frame)
	case path == "/v1/chat/completions", path == "/v1/completions":
		return gatewaySSEFrameContainsDone(frame) && !gatewaySSEFrameHasDone(frame)
	default:
		return false
	}
}

func (s *gatewaySSEStreamSanitizer) appendPublicStreamErrorFrame(frame []byte) {
	s.appendPublicStreamErrorFrameWithMinimumSequence(frame, 0)
}

func (s *gatewaySSEStreamSanitizer) appendPublicStreamErrorFrameWithMinimumSequence(frame []byte, minimumSequence int64) {
	if s.syntheticTerminal {
		return
	}
	sequence := int64(0)
	if strings.Contains(strings.ToLower(s.requestPath), "/responses") {
		if frameSequence, ok := gatewayResponsesStreamSequenceNumber(frame); ok {
			sequence = frameSequence
		}
		publishedNext := s.responsePublishedNextSequence
		if s.pendingSequenceBytes > 0 && s.pendingSequenceNext > publishedNext {
			publishedNext = s.pendingSequenceNext
		}
		if sequence < publishedNext {
			sequence = publishedNext
		}
		if sequence < minimumSequence {
			sequence = minimumSequence
		}
	}
	responseID := s.responseID
	responseSnapshot := s.responseSnapshot
	if len(s.pending) > 0 && s.pendingResponseID != "" {
		responseID = s.pendingResponseID
	}
	if len(s.pending) > 0 && s.pendingResponse != nil {
		responseSnapshot = s.pendingResponse
	}
	if len(s.pending) > 0 && !bytes.HasSuffix(s.pending, []byte("\n\n")) {
		if !bytes.HasSuffix(s.pending, []byte("\n")) {
			s.pending = append(s.pending, '\n')
		}
		s.pending = append(s.pending, '\n')
	}
	if s.pendingResponseID != "" || s.pendingResponse != nil {
		s.pendingResponseBytes = len(s.pending)
	}
	s.syntheticPrefixBytes = len(s.pending)
	s.syntheticOutputSeen = false
	terminal := gatewayPublicStreamErrorFrameWithResponse(frame, s.requestPath, sequence, responseID, responseSnapshot)
	if s.retryPromptEnabled && s.retryPromptEligible && s.retryPromptStructureCompatible && s.retryPromptCanComplete() && strings.Contains(strings.ToLower(s.requestPath), "/responses") {
		terminal = s.retryPromptFrames(sequence, responseID, responseSnapshot)
		s.retryPrompted = true
	}
	s.pending = append(s.pending, terminal...)
	s.syntheticTerminal = true
	s.syntheticSequence = sequence
	s.protocolTerminal = true
}

func (s *gatewaySSEStreamSanitizer) retryPromptCanComplete() bool {
	if s != nil && s.waitingProbeFailed != nil && s.waitingProbeFailed() {
		return false
	}
	if s == nil || !s.useful {
		return true
	}
	return s.retryPromptResponseCreated && s.retryPromptItemStarted && s.retryPromptPartStarted && s.retryPromptTextDeltaSeen && s.retryPromptSequenceSeen && s.retryPromptSequenceComplete &&
		(s.retryPromptTransportFailure || strings.HasPrefix(s.errorText, "upstream Responses stream contained a malformed JSON frame"))
}

func (s *gatewaySSEStreamSanitizer) retryPromptFrames(sequence int64, responseID string, responseSnapshot map[string]any) []byte {
	return gatewayPublicStreamRetryFrames(gatewayResponsesRetryPromptStreamState{
		Sequence:         sequence,
		ResponseID:       responseID,
		ResponseSnapshot: responseSnapshot,
		ResponseCreated:  s.retryPromptResponseCreated,
		ItemStarted:      s.retryPromptItemStarted,
		PartStarted:      s.retryPromptPartStarted,
		MessageID:        s.retryPromptMessageID,
		TextPrefix:       s.retryPromptVisibleText,
	})
}

func gatewayStreamFrameHasEventType(frame []byte, expected string) bool {
	expected = strings.ToLower(strings.TrimSpace(expected))
	if expected == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(gatewaySSEEventName(frame)), expected) {
		return true
	}
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		kind, _ := payload["type"].(string)
		if strings.EqualFold(strings.TrimSpace(kind), expected) {
			return true
		}
	}
	return false
}

func gatewaySSEFrameHasDone(frame []byte) bool {
	found := false
	scanner := bufio.NewScanner(bytes.NewReader(frame))
	scanner.Buffer(make([]byte, 256), gatewayStreamFrameLimit)
	for scanner.Scan() {
		// Scanner's ScanLines removes only the trailing newline and optional CR.
		// Keep the rest raw: a leading space, a second data line, an event field,
		// or trailing payload whitespace is not the one exact protocol terminal.
		line := scanner.Text()
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "data:") || found {
			return false
		}
		payload := strings.TrimPrefix(line, "data:")
		// SSE permits one optional space immediately after the colon; no other
		// trimming is allowed for a terminal marker.
		payload = strings.TrimPrefix(payload, " ")
		if payload != "[DONE]" {
			return false
		}
		found = true
	}
	return scanner.Err() == nil && found
}

func gatewaySSEFrameContainsDone(frame []byte) bool {
	scanner := bufio.NewScanner(bytes.NewReader(frame))
	scanner.Buffer(make([]byte, 256), gatewayStreamFrameLimit)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "[DONE]" {
			return true
		}
		if strings.HasPrefix(line, "data:") && strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(line, "data:")), "[DONE]") {
			return true
		}
	}
	return false
}

func gatewayResponsesStreamSequenceNumber(frame []byte) (int64, bool) {
	var highest int64
	found := false
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		value, ok := payload["sequence_number"].(float64)
		if !ok || value < 0 || value > 9007199254740991 {
			continue
		}
		sequence := int64(value)
		if float64(sequence) != value {
			continue
		}
		if !found || sequence > highest {
			highest = sequence
			found = true
		}
	}
	return highest, found
}

func gatewayResponsesStreamResponseID(frame []byte) string {
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		if response, ok := payload["response"].(map[string]any); ok {
			if id := strings.TrimSpace(gatewayStreamFailureString(response["id"])); id != "" {
				return id
			}
		}
		if id := strings.TrimSpace(gatewayStreamFailureString(payload["response_id"])); id != "" {
			return id
		}
	}
	return ""
}

func gatewayResponsesStreamResponse(frame []byte) map[string]any {
	for _, payload := range gatewayStreamJSONPayloads(frame) {
		response, ok := payload["response"].(map[string]any)
		if !ok || !gatewayAcceptedStreamID(response["id"]) {
			continue
		}
		return cloneGatewayResponsesObject(response)
	}
	return nil
}

func cloneGatewayResponsesObject(response map[string]any) map[string]any {
	if response == nil {
		return nil
	}
	cloned := make(map[string]any, len(response))
	for key, value := range response {
		cloned[key] = value
	}
	return cloned
}

func (s *gatewaySSEStreamSanitizer) resetFrame() {
	if cap(s.frame) > gatewayStreamPreludeLimit {
		s.frame = nil
	} else {
		s.frame = s.frame[:0]
	}
	s.suppressed = false
	s.overflowed = false
}

func gatewaySSEEventName(frame []byte) string {
	scanner := bufio.NewScanner(bytes.NewReader(frame))
	scanner.Buffer(make([]byte, 256), 8*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "event:") {
			name := strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			if len(name) <= 128 {
				return name
			}
			return ""
		}
	}
	return ""
}

func gatewayPublicStreamErrorFrame(frame []byte, requestPath string) []byte {
	sequence, _ := gatewayResponsesStreamSequenceNumber(frame)
	return gatewayPublicStreamErrorFrameWithSequenceAndID(frame, requestPath, sequence, gatewayResponsesStreamResponseID(frame))
}

func gatewayPublicStreamErrorFrameWithSequence(frame []byte, requestPath string, responseSequence int64) []byte {
	return gatewayPublicStreamErrorFrameWithSequenceAndID(frame, requestPath, responseSequence, gatewayResponsesStreamResponseID(frame))
}

func gatewayPublicStreamErrorFrameWithSequenceAndID(frame []byte, requestPath string, responseSequence int64, responseID string) []byte {
	return gatewayPublicStreamErrorFrameWithResponse(frame, requestPath, responseSequence, responseID, nil)
}

func gatewayPublicStreamErrorFrameWithResponse(frame []byte, requestPath string, responseSequence int64, responseID string, responseSnapshot map[string]any) []byte {
	eventName := gatewaySSEEventName(frame)
	payload := `{"error":{"message":"request failed before completion","type":"server_error","code":"server_error"}}`
	if strings.EqualFold(eventName, "error") || strings.Contains(strings.ToLower(requestPath), "/messages") {
		eventName = "error"
		payload = `{"type":"error","error":{"type":"api_error","message":"request failed before completion"}}`
	}
	if strings.Contains(strings.ToLower(requestPath), "/responses") {
		if responseSequence < 0 {
			responseSequence = 0
		}
		responseID = strings.TrimSpace(responseID)
		if responseID == "" {
			responseID = "resp_failed"
		}
		eventName = "response.failed"
		failurePayload := map[string]any{
			"type":            "response.failed",
			"sequence_number": responseSequence,
			"response":        gatewayResponsesFailureObject(responseID, responseSnapshot),
		}
		if encoded, err := json.Marshal(failurePayload); err == nil {
			payload = string(encoded)
		}
	} else if eventName != "error" {
		eventName = ""
	}
	var out strings.Builder
	if eventName != "" {
		out.WriteString("event: ")
		out.WriteString(eventName)
		out.WriteString("\n")
	}
	out.WriteString("data: ")
	out.WriteString(payload)
	out.WriteString("\n\n")
	return []byte(out.String())
}

func gatewayResponsesFailureObject(responseID string, responseSnapshot map[string]any) map[string]any {
	response := cloneGatewayResponsesObject(responseSnapshot)
	if response == nil {
		response = make(map[string]any)
	}
	defaults := map[string]any{
		"created_at":           time.Now().Unix(),
		"completed_at":         nil,
		"incomplete_details":   nil,
		"instructions":         nil,
		"max_output_tokens":    nil,
		"model":                "",
		"parallel_tool_calls":  true,
		"previous_response_id": nil,
		"reasoning":            map[string]any{"effort": nil, "summary": nil},
		"store":                false,
		"temperature":          1.0,
		"text":                 map[string]any{"format": map[string]any{"type": "text"}},
		"tool_choice":          "auto",
		"tools":                []any{},
		"top_p":                1.0,
		"truncation":           "disabled",
		"usage":                nil,
		"user":                 nil,
		"metadata":             map[string]any{},
	}
	for key, value := range defaults {
		if _, present := response[key]; !present {
			response[key] = value
		}
	}
	response["id"] = responseID
	response["object"] = "response"
	response["status"] = "failed"
	response["completed_at"] = nil
	response["error"] = map[string]any{
		"code":    "server_error",
		"message": gatewayPublicStreamFailureMessage,
	}
	response["incomplete_details"] = nil
	response["output"] = []any{}
	response["usage"] = nil
	return response
}

func gatewayResponsesRetryPromptObject(responseID string, responseSnapshot map[string]any, messageID, text string) map[string]any {
	response := gatewayResponsesFailureObject(responseID, responseSnapshot)
	messageID = strings.TrimSpace(messageID)
	if messageID == "" {
		messageID = gatewayRetryPromptMessageID(responseID)
	}
	content := map[string]any{
		"type":        "output_text",
		"text":        text,
		"annotations": []any{},
		"logprobs":    []any{},
	}
	message := map[string]any{
		"id":      messageID,
		"type":    "message",
		"status":  "completed",
		"role":    "assistant",
		"content": []any{content},
	}
	response["status"] = "completed"
	response["completed_at"] = time.Now().Unix()
	response["error"] = nil
	response["incomplete_details"] = nil
	response["output"] = []any{message}
	response["usage"] = nil
	return response
}

func gatewayRetryPromptMessageID(responseID string) string {
	digest := sha256.Sum256([]byte(responseID))
	return "msg_retry_" + hex.EncodeToString(digest[:8])
}

type gatewayResponsesRetryPromptStreamState struct {
	Sequence         int64
	ResponseID       string
	ResponseSnapshot map[string]any
	ResponseCreated  bool
	ItemStarted      bool
	PartStarted      bool
	MessageID        string
	TextPrefix       string
}

func gatewayPublicStreamRetryFrameWithResponse(responseSequence int64, responseID string, responseSnapshot map[string]any) []byte {
	return gatewayPublicStreamRetryFrames(gatewayResponsesRetryPromptStreamState{
		Sequence:         responseSequence,
		ResponseID:       responseID,
		ResponseSnapshot: responseSnapshot,
	})
}

func gatewayPublicStreamRetryFrames(state gatewayResponsesRetryPromptStreamState) []byte {
	if state.Sequence < 0 {
		state.Sequence = 0
	}
	state.ResponseID = strings.TrimSpace(state.ResponseID)
	if state.ResponseID == "" {
		state.ResponseID = "resp_retry"
	}
	state.MessageID = strings.TrimSpace(state.MessageID)
	if state.MessageID == "" {
		state.MessageID = gatewayRetryPromptMessageID(state.ResponseID)
	}
	sequence := state.Sequence
	finalText := state.TextPrefix + gatewayPublicRetryPromptMessage
	type retryEvent struct {
		name    string
		payload map[string]any
	}
	events := make([]retryEvent, 0, 8)
	if !state.ResponseCreated {
		response := gatewayResponsesFailureObject(state.ResponseID, state.ResponseSnapshot)
		response["status"] = "in_progress"
		response["completed_at"] = nil
		response["error"] = nil
		response["output"] = []any{}
		events = append(events, retryEvent{name: "response.created", payload: map[string]any{
			"type": "response.created", "sequence_number": sequence, "response": response,
		}})
		sequence++
	}
	if !state.ItemStarted {
		events = append(events, retryEvent{name: "response.output_item.added", payload: map[string]any{
			"type": "response.output_item.added", "sequence_number": sequence, "output_index": 0,
			"item": map[string]any{"id": state.MessageID, "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}},
		}})
		sequence++
	}
	if !state.PartStarted {
		events = append(events, retryEvent{name: "response.content_part.added", payload: map[string]any{
			"type": "response.content_part.added", "sequence_number": sequence, "item_id": state.MessageID, "output_index": 0, "content_index": 0,
			"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}, "logprobs": []any{}},
		}})
		sequence++
	}
	events = append(events,
		retryEvent{name: "response.output_text.delta", payload: map[string]any{
			"type": "response.output_text.delta", "sequence_number": sequence, "item_id": state.MessageID, "output_index": 0, "content_index": 0,
			"delta": gatewayPublicRetryPromptMessage, "logprobs": []any{},
		}},
	)
	sequence++
	events = append(events, retryEvent{name: "response.output_text.done", payload: map[string]any{
		"type": "response.output_text.done", "sequence_number": sequence, "item_id": state.MessageID, "output_index": 0, "content_index": 0,
		"text": finalText, "logprobs": []any{},
	}})
	sequence++
	part := map[string]any{"type": "output_text", "text": finalText, "annotations": []any{}, "logprobs": []any{}}
	events = append(events, retryEvent{name: "response.content_part.done", payload: map[string]any{
		"type": "response.content_part.done", "sequence_number": sequence, "item_id": state.MessageID, "output_index": 0, "content_index": 0, "part": part,
	}})
	sequence++
	message := map[string]any{"id": state.MessageID, "type": "message", "status": "completed", "role": "assistant", "content": []any{part}}
	events = append(events, retryEvent{name: "response.output_item.done", payload: map[string]any{
		"type": "response.output_item.done", "sequence_number": sequence, "output_index": 0, "item": message,
	}})
	sequence++
	events = append(events, retryEvent{name: "response.completed", payload: map[string]any{
		"type": "response.completed", "sequence_number": sequence,
		"response": gatewayResponsesRetryPromptObject(state.ResponseID, state.ResponseSnapshot, state.MessageID, finalText),
	}})

	var out bytes.Buffer
	for _, event := range events {
		encoded, err := json.Marshal(event.payload)
		if err != nil {
			return []byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":0,\"response\":{\"id\":\"resp_retry\",\"object\":\"response\",\"status\":\"completed\",\"error\":null}}\n\n")
		}
		out.WriteString("event: ")
		out.WriteString(event.name)
		out.WriteString("\ndata: ")
		out.Write(encoded)
		out.WriteString("\n\n")
	}
	return out.Bytes()
}

func gatewayRetryPromptResponseID(telemetry *gatewayRequestTelemetry) string {
	traceID := ""
	if telemetry != nil {
		traceID, _, _, _, _, _, _ = telemetry.snapshot()
	}
	if traceID == "" {
		traceID = newGatewayLogicalRequestTraceID()
	}
	return "resp_retry_" + traceID
}

func writeGatewayRetryPrompt(c *gin.Context, streaming bool, responseID string) {
	if c == nil {
		return
	}
	if streaming {
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "text/event-stream; charset=utf-8", gatewayPublicStreamRetryFrameWithResponse(0, responseID, nil))
		return
	}
	encoded, err := json.Marshal(gatewayResponsesRetryPromptObject(responseID, nil, "", gatewayPublicRetryPromptMessage))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"id":     responseID,
			"object": "response",
			"status": "completed",
			"output": []any{},
		})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", encoded)
}

func gatewayPublicBufferedFailureBody(requestPath string) []byte {
	if requestPath == "/v1/messages" {
		return []byte(`{"type":"error","error":{"type":"api_error","message":"request failed before completion"}}`)
	}
	return []byte(`{"error":{"message":"request failed before completion","type":"server_error","code":"server_error"}}`)
}

func copyGatewayBody(dst http.ResponseWriter, src io.Reader) (int64, error) {
	buf := make([]byte, 32*1024)
	var written int64
	flusher, canFlush := dst.(http.Flusher)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			written += int64(nw)
			if canFlush {
				flusher.Flush()
			}
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				return written, nil
			}
			return written, er
		}
	}
}

func copyGatewayRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		if skipGatewayHeader(key) ||
			strings.HasPrefix(strings.ToLower(key), "x-bridgeops-") ||
			strings.HasPrefix(strings.ToLower(key), "x-upstream-ops-") ||
			strings.EqualFold(key, "Authorization") ||
			strings.EqualFold(key, "x-api-key") ||
			strings.EqualFold(key, "Cookie") ||
			strings.EqualFold(key, "Accept-Encoding") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyGatewayHeaders(dst, src http.Header) {
	for key, values := range src {
		// Downstream callers must not learn the upstream implementation,
		// gateway topology, or receive upstream-managed browser state.
		if !strings.EqualFold(key, "Content-Type") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func skipGatewayHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade", "content-length":
		return true
	default:
		return false
	}
}

func gatewayKeyPrefix(key string) string {
	key = strings.TrimSpace(key)
	if len(key) <= 12 {
		return key
	}
	return key[:12]
}
