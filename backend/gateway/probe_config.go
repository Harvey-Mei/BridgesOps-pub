package gateway

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const (
	availabilityReasonMonitorDisabled = "monitor_disabled"
	availabilityReasonChannelError    = "channel_error"
	availabilityReasonLowBalance      = "low_balance"
	availabilityReasonAccountBalance  = "account_balance_unavailable"
	availabilityReasonNoRateSnapshot  = "no_rate_snapshot"
	availabilityReasonNoRoutableGroup = "no_routable_group"

	probeBlockerProbeDisabled   = "probe_disabled"
	probeBlockerKeyMissing      = "upstream_key_missing"
	probeBlockerKeyDisabled     = "upstream_key_disabled"
	probeBlockerKeyUnavailable  = "upstream_key_unavailable"
	probeBlockerModelsNotSynced = "models_not_synced"
	probeBlockerUnsupportedApp  = "no_supported_gateway_app"
	probeStatusReady            = "ready"
	latencySourceGroup          = "group"
	latencySourceChannel        = "channel"
	latencySourceNone           = "none"
)

// ProbeConfigChannel is an explicit response DTO. It intentionally excludes
// channel endpoints, usernames, login material, API key identifiers and key
// prefixes.
type ProbeConfigChannel struct {
	ChannelID                    uint               `json:"channel_id"`
	ChannelName                  string             `json:"channel_name"`
	ChannelType                  string             `json:"channel_type"`
	GatewayApp                   string             `json:"gateway_app"`
	CredentialMode               string             `json:"credential_mode"`
	MonitorEnabled               bool               `json:"monitor_enabled"`
	ProbeEnabled                 bool               `json:"probe_enabled"`
	RoutingAvailable             bool               `json:"routing_available"`
	AvailabilityReasons          []string           `json:"availability_reasons"`
	LastError                    string             `json:"last_error"`
	BalanceError                 string             `json:"balance_error"`
	BalanceErrorAt               *time.Time         `json:"balance_error_at"`
	AccountBalanceInsufficient   bool               `json:"upstream_account_balance_insufficient"`
	AccountBalanceInsufficientAt *time.Time         `json:"upstream_account_balance_insufficient_at"`
	LastBalance                  *float64           `json:"last_balance"`
	BalanceThreshold             float64            `json:"balance_threshold"`
	LastBalanceAt                *time.Time         `json:"last_balance_at"`
	Groups                       []ProbeConfigGroup `json:"groups"`
}

type ProbeConfigGroup struct {
	UpstreamGroupID          uint       `json:"upstream_group_id"`
	PresenceGeneration       uint64     `json:"presence_generation"`
	ChannelIdentityRevision  uint64     `json:"channel_identity_revision"`
	GroupName                string     `json:"group_name"`
	ProbeEnabled             bool       `json:"probe_enabled"`
	Description              string     `json:"description"`
	Ratio                    float64    `json:"ratio"`
	CompletionRatio          float64    `json:"completion_ratio"`
	RawRatio                 float64    `json:"raw_ratio"`
	RawCompletionRatio       float64    `json:"raw_completion_ratio"`
	FundingFactor            float64    `json:"funding_factor"`
	CostBasisVersion         uint64     `json:"cost_basis_version"`
	EffectivePrice           float64    `json:"effective_price"`
	ModelFamilies            []string   `json:"model_families"`
	ModelMappingFamilies     []string   `json:"model_mapping_families"`
	ModelIDs                 []string   `json:"model_ids"`
	GatewayApps              []string   `json:"gateway_apps"`
	LatencyProbeModel        string     `json:"latency_probe_model"`
	ModelsSyncedAt           *time.Time `json:"models_synced_at"`
	ModelsError              string     `json:"models_error"`
	UpstreamKeyConfigured    bool       `json:"upstream_key_configured"`
	UpstreamKeyEnabled       bool       `json:"upstream_key_enabled"`
	UpstreamKeyProvisioning  bool       `json:"upstream_key_provisioning"`
	UpstreamKeyAvailable     bool       `json:"upstream_key_available"`
	LastError                string     `json:"last_error"`
	LastRoutingFailureError  string     `json:"last_routing_failure_error"`
	LastRoutingFailureKind   string     `json:"last_routing_failure_kind"`
	LastRoutingFailureStatus int        `json:"last_routing_failure_status"`
	LastRoutingFailureAt     *time.Time `json:"last_routing_failure_at"`
	CoolingDown              bool       `json:"cooling_down"`
	FailureCount             int        `json:"failure_count"`
	CooldownUntil            *time.Time `json:"cooldown_until"`
	CooldownRemaining        int64      `json:"cooldown_remaining"`
	Status                   string     `json:"status"`
	At                       *time.Time `json:"at"`
	AvgLatencyMs             float64    `json:"avg_latency_ms"`
	LatencySamples           int64      `json:"latency_samples"`
	LatencyRawSamples        []int64    `json:"latency_raw_samples"`
	LatencyQualified         bool       `json:"latency_qualified"`
	LatencySource            string     `json:"latency_source"`
	ProbeEligible            bool       `json:"probe_eligible"`
	ProbeBlockers            []string   `json:"probe_blockers"`
	ProbeEstimatedCostUSD    float64    `json:"probe_estimated_cost_usd"`
}

// EffectiveGroupRate is the small canonical rate projection needed by
// change notifications. It deliberately excludes routing diagnostics so a
// temporary probe or latency read cannot block the change list.
type EffectiveGroupRate struct {
	ChannelID       uint
	GroupName       string
	Ratio           float64
	CompletionRatio float64
}

type candidateLatencySamples struct {
	groups   map[candidateLatencyKey][]float64
	channels map[uint][]float64
	minimum  int
}

type probeConfigLatency struct {
	avgMs     float64
	raw       []int64
	qualified bool
	source    string
}

// ChannelAvailabilityReasons is the canonical channel-level routing health
// check. Candidate selection and the probe configuration endpoint both use
// this helper so they cannot disagree about monitor, error or balance state.
func ChannelAvailabilityReasons(ch storage.Channel) []string {
	reasons := make([]string, 0, 3)
	if !ch.MonitorEnabled {
		reasons = append(reasons, availabilityReasonMonitorDisabled)
	}
	if strings.TrimSpace(ch.LastError) != "" {
		reasons = append(reasons, availabilityReasonChannelError)
	}
	if ch.CredentialMode != storage.CredentialModeDirect && ch.HasLowBalance() {
		reasons = append(reasons, availabilityReasonLowBalance)
	}
	if ch.CredentialMode != storage.CredentialModeDirect && ch.HasCurrentUpstreamAccountBalanceInsufficient() {
		reasons = append(reasons, availabilityReasonAccountBalance)
	}
	return reasons
}

// modelProbeChannelBlocked reports only conditions that make the control-plane
// probe target itself unsafe. Balance facts intentionally remain visible to
// probes so a 402 can refresh the balance fact while routing stays gated.
func modelProbeChannelBlocked(ch storage.Channel) bool {
	for _, reason := range ChannelAvailabilityReasons(ch) {
		switch reason {
		case availabilityReasonLowBalance, availabilityReasonAccountBalance:
			continue
		default:
			return true
		}
	}
	return false
}

// EffectiveGroupRates returns current rates after the canonical channel
// funding conversion. Missing output rates use the same input rate as the
// gateway cost path.
func (s *Service) EffectiveGroupRates() ([]EffectiveGroupRate, error) {
	if s == nil || s.Rates == nil {
		return nil, errors.New("gateway effective rates are unavailable")
	}
	pairedRates, err := s.Rates.ListModelsWithCostBasis()
	if err != nil {
		return nil, err
	}
	rates, fundingFactors := splitRateCostBasisSnapshots(pairedRates)
	out := make([]EffectiveGroupRate, 0, len(rates))
	for _, rate := range rates {
		funding, ok := fundingFactors[rate.ChannelID]
		if !ok {
			continue
		}
		factor := funding.factor
		if factor < 0 {
			factor = 1
		}
		completion := rate.CompletionRatio
		if completion <= 0 {
			completion = rate.Ratio
		}
		out = append(out, EffectiveGroupRate{
			ChannelID:       rate.ChannelID,
			GroupName:       strings.TrimSpace(rate.ModelName),
			Ratio:           rate.Ratio * factor,
			CompletionRatio: completion * factor,
		})
	}
	return out, nil
}

// ProbeConfig returns every configured channel and every current rate group,
// including entries which routing would normally filter out.
func (s *Service) ProbeConfig() ([]ProbeConfigChannel, error) {
	if s == nil {
		return nil, errors.New("gateway probe configuration is unavailable")
	}
	return s.probeConfigWithStore(s.Store)
}

func (s *Service) probeConfigWithStore(store *storage.Gateway) ([]ProbeConfigChannel, error) {
	if s == nil || s.Channels == nil || s.Rates == nil || store == nil {
		return nil, errors.New("gateway probe configuration is unavailable")
	}
	channels, err := s.Channels.List()
	if err != nil {
		return nil, err
	}
	out := make([]ProbeConfigChannel, 0, len(channels))
	if len(channels) == 0 {
		return out, nil
	}

	channelIDs := make([]uint, 0, len(channels))
	for _, ch := range channels {
		channelIDs = append(channelIDs, ch.ID)
	}
	pairedRates, err := s.Rates.ListModelsWithCostBasis()
	if err != nil {
		return nil, err
	}
	rates, fundingFactors := splitRateCostBasisSnapshots(pairedRates)
	keys, err := store.ListUpstreamKeys(channelIDs)
	if err != nil {
		return nil, err
	}
	generations, err := store.ListUpstreamGroupGenerations(channelIDs)
	if err != nil {
		return nil, err
	}
	latencies, err := s.loadCandidateLatencySamplesWithStore(store)
	if err != nil {
		return nil, err
	}

	ratesByChannel := make(map[uint][]storage.RateSnapshot, len(channels))
	for _, rate := range rates {
		ratesByChannel[rate.ChannelID] = append(ratesByChannel[rate.ChannelID], rate)
	}
	keysByGroup := make(map[candidateLatencyKey]storage.GatewayUpstreamKey, len(keys))
	for _, key := range keys {
		keysByGroup[candidateLatencyKey{ChannelID: key.ChannelID, GroupName: strings.TrimSpace(key.GroupName)}] = key
	}
	generationsByGroup := make(map[candidateLatencyKey]storage.UpstreamGroupGeneration, len(generations))
	for _, generation := range generations {
		generationsByGroup[canonicalCandidateKey(generation.ChannelID, generation.CanonicalGroupName)] = generation
	}

	for _, ch := range channels {
		channelReasons := ChannelAvailabilityReasons(ch)
		accountBalanceInsufficient := ch.HasCurrentUpstreamAccountBalanceInsufficient()
		var accountBalanceInsufficientAt *time.Time
		if accountBalanceInsufficient {
			accountBalanceInsufficientAt = ch.UpstreamAccountBalanceInsufficientAt
		}
		channelRates := ratesByChannel[ch.ID]
		item := ProbeConfigChannel{
			ChannelID:                    ch.ID,
			ChannelName:                  ch.Name,
			ChannelType:                  string(ch.Type),
			GatewayApp:                   string(ch.GatewayApp),
			CredentialMode:               string(ch.CredentialMode),
			MonitorEnabled:               ch.MonitorEnabled,
			ProbeEnabled:                 ch.LatencyProbeEnabled,
			AvailabilityReasons:          append(make([]string, 0, len(channelReasons)), channelReasons...),
			LastError:                    sanitizeProbeConfigError(ch.LastError, ch.SiteURL, ch.Username),
			BalanceError:                 sanitizeProbeConfigError(ch.BalanceError, ch.SiteURL, ch.Username),
			BalanceErrorAt:               ch.BalanceErrorAt,
			AccountBalanceInsufficient:   accountBalanceInsufficient,
			AccountBalanceInsufficientAt: accountBalanceInsufficientAt,
			LastBalance:                  ch.LastBalance,
			BalanceThreshold:             ch.EffectiveBalanceThreshold(),
			LastBalanceAt:                ch.LastBalanceAt,
			Groups:                       make([]ProbeConfigGroup, 0, len(channelRates)),
		}
		if len(channelRates) == 0 {
			item.AvailabilityReasons = appendReason(item.AvailabilityReasons, availabilityReasonNoRateSnapshot)
		}

		anyRoutableGroup := false
		for _, rate := range channelRates {
			rawRatio := rate.Ratio
			rawCompletionRatio := rate.CompletionRatio
			funding := fundingFactors[ch.ID]
			if funding.factor < 0 {
				funding.factor = 1
			}
			rate.Ratio = rawRatio * funding.factor
			if rawCompletionRatio > 0 {
				rate.CompletionRatio = rawCompletionRatio * funding.factor
			}
			groupName := strings.TrimSpace(rate.ModelName)
			keyValue, keyExists := keysByGroup[candidateLatencyKey{ChannelID: ch.ID, GroupName: groupName}]
			var key *storage.GatewayUpstreamKey
			if keyExists {
				key = &keyValue
			}
			generation := generationsByGroup[canonicalCandidateKey(ch.ID, groupName)]
			group, routable := s.probeConfigGroup(ch, rate, key, generation, channelReasons, latencies)
			if generation.Valid() {
				group.UpstreamGroupID = generation.ID
				group.PresenceGeneration = generation.PresenceGeneration
			}
			group.RawRatio = rawRatio
			group.RawCompletionRatio = rawCompletionRatio
			group.FundingFactor = funding.factor
			group.CostBasisVersion = funding.version
			if routable {
				anyRoutableGroup = true
			}
			item.Groups = append(item.Groups, group)
		}
		item.RoutingAvailable = len(channelReasons) == 0 && anyRoutableGroup
		if len(channelRates) > 0 && len(channelReasons) == 0 && !anyRoutableGroup {
			item.AvailabilityReasons = appendReason(item.AvailabilityReasons, availabilityReasonNoRoutableGroup)
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *Service) probeConfigGroup(ch storage.Channel, rate storage.RateSnapshot, key *storage.GatewayUpstreamKey, generation storage.UpstreamGroupGeneration, channelReasons []string, latencies candidateLatencySamples) (ProbeConfigGroup, bool) {
	groupName := strings.TrimSpace(rate.ModelName)
	modelFamilies := make([]string, 0)
	modelIDs := make([]string, 0)
	gatewayApps := make([]string, 0)
	blockers := append(make([]string, 0, len(channelReasons)+4), channelReasons...)
	keyConfigured := key != nil && strings.TrimSpace(key.APIKeyCipher) != ""
	keyEnabled := key != nil && key.Enabled
	keyProvisioning := key != nil && key.Provisioning
	keyAvailable := keyConfigured && keyEnabled && !keyProvisioning
	probeEnabled := key == nil || key.LatencyProbeEnabled
	var routingIsolatedAt *time.Time

	group := ProbeConfigGroup{
		GroupName:               groupName,
		ProbeEnabled:            probeEnabled,
		Description:             rate.Description,
		Ratio:                   rate.Ratio,
		CompletionRatio:         rate.CompletionRatio,
		EffectivePrice:          effectiveCandidatePrice(rate),
		ModelFamilies:           modelFamilies,
		ModelIDs:                modelIDs,
		GatewayApps:             gatewayApps,
		UpstreamKeyConfigured:   keyConfigured,
		UpstreamKeyEnabled:      keyEnabled,
		UpstreamKeyProvisioning: keyProvisioning,
		UpstreamKeyAvailable:    keyAvailable,
		Status:                  probeStatusReady,
		LatencySource:           latencySourceNone,
		ProbeBlockers:           make([]string, 0),
		ProbeEstimatedCostUSD:   estimateProbeCost(rate, nil),
		ChannelIdentityRevision: ch.UpstreamAccountIdentityRevision,
	}
	if !ch.LatencyProbeEnabled || !probeEnabled {
		blockers = appendReason(blockers, probeBlockerProbeDisabled)
	}
	if key == nil {
		blockers = appendReason(blockers, probeBlockerKeyMissing)
	} else {
		healthCurrent := false
		if fence, ok := RoutingHealthFenceForCandidate(Candidate{Channel: ch, Rate: rate, UpstreamGeneration: generation}); ok {
			healthCurrent = fence.MatchesUpstreamKey(key)
		}
		if ch.CredentialMode == storage.CredentialModeDirect {
			directModels, directModelsAvailable := directProjectedModels(ch, *key)
			if directModelsAvailable {
				group.ModelIDs = directModels
				group.ModelFamilies = directChannelModelFamilies(&ch, directModels)
				group.GatewayApps = []string{string(normalizeGatewayApp(ch.GatewayApp))}
				probeModel := strings.TrimSpace(key.LatencyProbeModel)
				if !directModelIDSupportsRequest(directModels, probeModel) {
					probeModel = ""
				}
				group.LatencyProbeModel = probeModel
			} else {
				group.ModelFamilies = nil
				group.ModelIDs = nil
				group.GatewayApps = nil
				group.LatencyProbeModel = ""
			}
		} else {
			group.ModelFamilies = splitCSV(key.ModelFamilies)
			group.ModelIDs = splitCSV(key.ModelIDs)
			group.GatewayApps = gatewayAppsForFamilies(group.ModelFamilies)
			group.LatencyProbeModel = strings.TrimSpace(key.LatencyProbeModel)
		}
		group.ModelMappingFamilies = ModelMappingFamilies(group.ModelFamilies)
		group.ModelsSyncedAt = key.ModelsSyncedAt
		group.ModelsError = sanitizeProbeConfigError(key.ModelsError, ch.SiteURL, ch.Username, key.APIKeyPrefix)
		group.LastError = sanitizeProbeConfigError(key.LastError, ch.SiteURL, ch.Username, key.APIKeyPrefix)
		if healthCurrent {
			routingIsolatedAt = key.RoutingIsolatedAt
			group.LastRoutingFailureError = sanitizeProbeConfigError(key.LastRoutingFailureError, ch.SiteURL, ch.Username, key.APIKeyPrefix)
			group.LastRoutingFailureKind = strings.TrimSpace(key.LastRoutingFailureKind)
			group.LastRoutingFailureStatus = key.LastRoutingFailureStatus
			group.LastRoutingFailureAt = key.LastRoutingFailureAt
			group.FailureCount = key.RoutingFailureCount
			healthInfo := cachedGatewayAppInfo{
				LastRoutingFailureKind: group.LastRoutingFailureKind, LastRoutingFailureStatus: group.LastRoutingFailureStatus,
				LastRoutingFailureAt: group.LastRoutingFailureAt, RoutingFailureCount: group.FailureCount,
				RoutingCooldownUntil: key.RoutingCooldownUntil, RoutingIsolatedAt: key.RoutingIsolatedAt,
			}
			if _, unavailable := gatewayCandidateTransientFailure(healthInfo, time.Now()); unavailable {
				group.CoolingDown = true
				group.CooldownUntil = gatewayCandidateTransientCooldownUntil(healthInfo)
				if group.CooldownUntil != nil {
					group.CooldownRemaining = max(0, int64(time.Until(*group.CooldownUntil).Seconds()))
				}
			}
		}
		group.At = latestProbeConfigStateAt(key, healthCurrent)
		if key.Provisioning {
			blockers = appendReason(blockers, probeBlockerKeyUnavailable)
		} else if !key.Enabled {
			blockers = appendReason(blockers, probeBlockerKeyDisabled)
		} else if !keyConfigured {
			blockers = appendReason(blockers, probeBlockerKeyMissing)
		}
		if len(group.ModelFamilies) == 0 {
			blockers = appendReason(blockers, probeBlockerModelsNotSynced)
		} else if len(group.GatewayApps) == 0 {
			blockers = appendReason(blockers, probeBlockerUnsupportedApp)
		}
	}

	latency := latencyForProbeConfig(latencies, ch.ID, groupName)
	group.AvgLatencyMs = latency.avgMs
	group.LatencySamples = int64(len(latency.raw))
	group.LatencyRawSamples = latency.raw
	group.LatencyQualified = latency.qualified
	group.LatencySource = latency.source
	group.ProbeBlockers = blockers
	group.ProbeEligible = len(blockers) == 0
	if len(blockers) > 0 {
		group.Status = blockers[0]
	}

	// Probe switches only suppress scheduled latency probes. They are not a
	// real-request availability gate, so a locally ready group remains
	// routable while either probe switch is paused.
	routable := len(channelReasons) == 0 && keyAvailable && len(group.GatewayApps) > 0
	if routable {
		_, temporarilyUnavailable := gatewayCandidateTransientFailure(cachedGatewayAppInfo{
			LastRoutingFailureKind:   group.LastRoutingFailureKind,
			LastRoutingFailureStatus: group.LastRoutingFailureStatus,
			LastRoutingFailureAt:     group.LastRoutingFailureAt,
			RoutingFailureCount:      group.FailureCount,
			RoutingCooldownUntil:     group.CooldownUntil,
			RoutingIsolatedAt:        routingIsolatedAt,
		}, time.Now())
		routable = !temporarilyUnavailable
	}
	return group, routable
}

func (s *Service) loadCandidateLatencySamples() (candidateLatencySamples, error) {
	return s.loadCandidateLatencySamplesWithStore(s.Store)
}

func (s *Service) loadCandidateLatencySamplesWithStore(store *storage.Gateway) (candidateLatencySamples, error) {
	routing := s.routingConfig()
	minimum := routing.LatencySamples
	if minimum <= 0 {
		minimum = defaultRoutingLatencySampleSize
	}
	return s.routingLatencySnapshotWithStore(store, routing.LatencyWindow, minimum)
}

func latencyForProbeConfig(samples candidateLatencySamples, channelID uint, groupName string) probeConfigLatency {
	group := samples.groups[canonicalCandidateKey(channelID, groupName)]
	channel := samples.channels[channelID]
	selected := group
	source := latencySourceGroup
	qualified := len(group) >= samples.minimum
	if !qualified && len(channel) >= samples.minimum {
		selected = channel
		source = latencySourceChannel
		qualified = true
	} else if len(group) == 0 && len(channel) > 0 {
		selected = channel
		source = latencySourceChannel
	}
	if len(selected) == 0 {
		return probeConfigLatency{raw: make([]int64, 0), source: latencySourceNone}
	}
	avg, _ := trimmedMean(selected, candidateLatencyTrimRatio)
	raw := make([]int64, 0, len(selected))
	for _, value := range selected {
		raw = append(raw, int64(value))
	}
	return probeConfigLatency{avgMs: avg, raw: raw, qualified: qualified, source: source}
}

func gatewayAppsForFamilies(families []string) []string {
	out := make([]string, 0, 3)
	for _, app := range []storage.GatewayApp{storage.GatewayAppCodex, storage.GatewayAppClaude, storage.GatewayAppGemini} {
		for _, family := range families {
			if familySupportsGatewayCandidate(family, app) {
				out = append(out, string(app))
				break
			}
		}
	}
	return out
}

func latestProbeConfigStateAt(key *storage.GatewayUpstreamKey, includeRoutingHealth bool) *time.Time {
	if key == nil {
		return nil
	}
	latest := key.UpdatedAt
	candidates := []*time.Time{key.ModelsSyncedAt}
	if includeRoutingHealth {
		candidates = append(candidates, key.LastRoutingFailureAt)
	}
	for _, candidate := range candidates {
		if candidate != nil && (latest.IsZero() || candidate.After(latest)) {
			latest = *candidate
		}
	}
	if latest.IsZero() {
		return nil
	}
	return &latest
}

func appendReason(reasons []string, reason string) []string {
	for _, existing := range reasons {
		if existing == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

var probeConfigURLPattern = regexp.MustCompile(`(?i)https?://[^\s"'<>]+`)
var probeConfigCredentialPattern = regexp.MustCompile(`(?i)\b(password|passwd|token|secret|api[_-]?key|cookie)\s*[:=]\s*[^\s,;]+`)
var probeConfigHTTPStatusPattern = regexp.MustCompile(`(?i)\b(?:http|status)\s*[:=]?\s*([45][0-9]{2})\b`)

func sanitizeProbeConfigError(text string, sensitive ...string) string {
	text = sanitizeGatewayServiceError(text)
	for _, value := range sensitive {
		value = strings.TrimSpace(value)
		if value != "" {
			text = strings.ReplaceAll(text, value, "[redacted]")
		}
	}
	text = probeConfigURLPattern.ReplaceAllString(text, "[redacted-url]")
	text = probeConfigCredentialPattern.ReplaceAllString(text, "$1=[redacted]")
	return probeConfigErrorSummary(text)
}

// probeConfigErrorSummary deliberately returns a short category instead of
// raw upstream error text. Upstreams can echo credentials in arbitrary forms,
// so a best-effort regex is not a sufficient boundary for this API response.
func probeConfigErrorSummary(text string) string {
	text = strings.ToLower(strings.TrimSpace(text))
	if text == "" {
		return ""
	}
	if match := probeConfigHTTPStatusPattern.FindStringSubmatch(text); len(match) == 2 {
		return "上游请求失败 HTTP " + match[1]
	}
	switch {
	case strings.Contains(text, "timeout"), strings.Contains(text, "deadline"):
		return "上游请求超时"
	case strings.Contains(text, "unauthorized"), strings.Contains(text, "authentication"), strings.Contains(text, "login"), strings.Contains(text, "auth"):
		return "上游鉴权失败"
	case strings.Contains(text, "forbidden"):
		return "上游拒绝访问"
	case strings.Contains(text, "rate limit"), strings.Contains(text, "too many requests"):
		return "上游限流"
	case strings.Contains(text, "dial"), strings.Contains(text, "connection"), strings.Contains(text, "network"), strings.Contains(text, "tls"):
		return "上游连接失败"
	default:
		return "上游请求失败"
	}
}
