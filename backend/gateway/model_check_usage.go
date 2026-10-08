package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

type modelCheckUsageDetails struct {
	Kind                    string `json:"kind"`
	CostUnitEpoch           uint64 `json:"cost_unit_epoch"`
	FundingTrusted          bool   `json:"funding_trusted"`
	ChannelIdentityRevision uint64 `json:"channel_identity_revision,omitempty"`
}

func (s *Service) modelCheckUsageSnapshot(ctx context.Context, candidate Candidate, key UpstreamKeyLease, request modelcheck.ProbeRequest, model, path string) *storage.GatewayRequestLog {
	factor := candidate.Channel.FundingFactor
	source, version := candidate.Channel.FundingFactorSource, uint64(0)
	trusted := factor >= 0
	if factor < 0 {
		factor = 1
	}
	if source == "" {
		source = "default"
	}
	if candidate.Channel.CredentialMode == storage.CredentialModeDirect {
		factor, source = 1, "direct"
	} else if s.Costing != nil {
		if state, err := s.Costing.State(candidate.Channel.ID); err == nil && state != nil {
			factor, source, version = state.FundingFactor, state.Source, state.Version
			trusted = trustedCostBasisStatus(state.Status, true)
		} else {
			trusted = false
		}
	}
	log := &storage.GatewayRequestLog{ChannelID: candidate.Channel.ID, ChannelNameSnapshot: candidate.Channel.Name, ChannelIdentityRevision: candidate.Channel.UpstreamAccountIdentityRevision, GroupName: candidate.Rate.ModelName, ModelName: model, RequestedModelName: model, Method: http.MethodPost, Path: path, RouteReason: "model_check", RouteDetails: request.Kind, FundingFactor: &factor, CostBasisSource: source, CostBasisVersion: version, CostStatus: storage.GatewayCostStatusUnavailable, CostUnavailableReason: storage.GatewayPricingUnavailableFactMissing, UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName, UpstreamKeyLocalID: key.LocalKeyID, UpstreamKeyGeneration: key.KeyGeneration, UpstreamGroupID: candidate.UpstreamGeneration.ID, UpstreamGroupGeneration: candidate.UpstreamGeneration.PresenceGeneration}

	log.PriceCanonicalGroupName = canonicalGroupName(candidate.Rate.ModelName)
	log.PriceModelName = model
	log.PriceOperation = storage.GatewayPricingOperationTextGeneration
	log.PriceProtocol = storage.GatewayPricingProtocolOpenAIChatCompletions
	if candidate.App == storage.GatewayAppClaude {
		log.PriceProtocol = storage.GatewayPricingProtocolAnthropicMessages
	}
	log.UsageSchema, log.UsageSchemaVersion = storage.GatewayPricingUsageSchemaTokensV1, 1
	details, _ := json.Marshal(modelCheckUsageDetails{Kind: request.Kind, CostUnitEpoch: candidate.Channel.CostUnitEpoch, FundingTrusted: trusted})
	log.RouteDetails = string(details)
	if s.Rates != nil {
		if rate, err := s.Rates.WithContext(ctx).FindByChannelAndModel(log.ChannelID, log.GroupName); err == nil && rate != nil {
			raw, completion := rate.Ratio, rate.CompletionRatio
			log.RawGroupRatio, log.RawCompletionRatio = &raw, &completion
			log.GroupRatio, log.CompletionRatio = raw*factor, completion*factor
		}
	}
	if !trusted {
		log.FundingFactor = nil
		log.GroupRatio = 0
		log.CompletionRatio = 0
		log.CostUnavailableReason = storage.GatewayPricingUnavailableCostBasisChanged
	}
	return log
}

func (s *Service) internalProbeCostIdentityCurrent(log storage.GatewayRequestLog) (bool, error) {
	raw := log.RouteDetails
	revision := log.ChannelIdentityRevision
	var details modelCheckUsageDetails
	if log.RouteReason == latencyProbeRouteReason && storage.IsGatewayWaitingProbeDetails(raw) {
		raw = strings.TrimPrefix(raw, storage.GatewayWaitingProbeDetail+"\n")
		if json.Unmarshal([]byte(raw), &details) != nil {
			return false, nil
		}
		revision = details.ChannelIdentityRevision
	} else if json.Unmarshal([]byte(raw), &details) != nil {
		return false, nil
	}
	if !details.FundingTrusted || details.CostUnitEpoch == 0 || revision == 0 {
		return false, nil
	}
	channel, err := s.Channels.FindByID(log.ChannelID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if channel.UpstreamAccountIdentityRevision != revision || channel.CostUnitEpoch != details.CostUnitEpoch {
		return false, nil
	}
	if s.Costing != nil {
		state, err := s.Costing.State(log.ChannelID)
		if err != nil {
			return false, err
		}
		if state == nil || !trustedCostBasisStatus(state.Status, true) {
			return false, nil
		}
	}
	return true, nil
}

func (s *Service) finishModelCheckUsage(log *storage.GatewayRequestLog, result modelcheck.ProbeResponse, err error, started time.Time) error {
	finished := time.Now().UTC()
	log.CreatedAt = finished
	log.LatencyMs = finished.Sub(started).Milliseconds()
	log.FirstByteMs = result.FirstByteMS
	log.PromptTokens, log.CompletionTokens, log.TotalTokens = result.PromptTokens, result.CompletionTokens, result.TotalTokens
	log.CacheReadTokens, log.CacheCreationTokens = result.CacheReadTokens, result.CacheCreationTokens
	log.StatusCode = result.StatusCode
	if err != nil {
		log.Error = modelcheck.ProbeErrorDiagnostic(err, result.StatusCode)
		if log.StatusCode < 400 {
			log.StatusCode = http.StatusBadGateway
		}
		if strings.HasPrefix(log.Error, modelcheck.ProbeErrorTimeout) {
			log.StatusCode = http.StatusGatewayTimeout
		}
	}
	if log.StatusCode == 0 {
		log.StatusCode = http.StatusBadGateway
	}
	if len(result.ActualModel) <= 256 && utf8.ValidString(result.ActualModel) {
		log.UpstreamResponseModel = result.ActualModel
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	current, identityErr := s.internalProbeCostIdentityCurrent(*log)
	if !current && identityErr == nil && log.CostUnavailableReason != storage.GatewayPricingUnavailableCostBasisChanged {
		log.CostUnavailableReason = storage.GatewayPricingUnavailableIdentityChanged
	}
	if _, supported := s.Keys.(requestCostManager); supported && storage.IsGatewayRequestCostReconcileEligible(log) && !requestUsesDirectCostBasis(*log) && !requestCostCannotBeMatched(*log) {
		if current || identityErr != nil {
			// A transient identity lookup failure must retain durable retry work.
			at := InitialRequestCostReconcileAt(finished)
			log.CostReconcileAt = &at
		}
	}
	// Append directly: model checks are visible usage, but never routing latency
	// samples, cache workload samples, session affinity or recovery evidence.
	if err := s.Store.WithContext(ctx).AppendRequestLog(log); err != nil {
		return err
	}
	if log.CostReconcileAt != nil {
		s.startCostWorker()
		s.wakeCostWorker()
	}
	return nil
}

func modelCheckRequestID(headers http.Header, provider storage.ChannelType, upstreamKey string) string {
	name := ""
	switch provider {
	case storage.ChannelTypeSub2API:
		name = "X-Client-Request-ID"
	case storage.ChannelTypeNewAPI:
		name = "X-Oneapi-Request-Id"
	default:
		return ""
	}
	values := headers.Values(name)
	if len(values) == 0 {
		return ""
	}
	value := values[0]
	if len(value) > 512 || !utf8.ValidString(value) {
		return ""
	}
	if secret := strings.TrimSpace(upstreamKey); secret != "" && strings.Contains(value, secret) {
		return ""
	}
	for _, next := range values[1:] {
		if next != value {
			return ""
		}
	}
	return value
}
