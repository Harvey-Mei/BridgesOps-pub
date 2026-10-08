package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const RoutingPriceBasisModel = "model_price_usd_per_million"

func projectRoutingModelPricesToPreview(preview, actual []Candidate) {
	byRoute := make(map[candidateLatencyKey]Candidate, len(actual))
	for _, c := range actual {
		byRoute[canonicalCandidateKey(c.Channel.ID, c.Rate.ModelName)] = c
	}
	for _, c := range preview {
		if _, ok := byRoute[canonicalCandidateKey(c.Channel.ID, c.Rate.ModelName)]; !ok {
			return
		}
	}
	for i := range preview {
		c := byRoute[canonicalCandidateKey(preview[i].Channel.ID, preview[i].Rate.ModelName)]
		preview[i].EffectivePrice = c.EffectivePrice
		preview[i].EffectivePriceBasis = c.EffectivePriceBasis
		preview[i].PriceInputShare = c.PriceInputShare
		preview[i].CacheRatio = c.CacheRatio
		preview[i].CacheRatioKnown = c.CacheRatioKnown
	}
}

type routingModelPriceEntry struct {
	snapshot storage.RoutingModelPriceSnapshot
	at       time.Time
	loading  bool
}

type routingModelPriceCache struct {
	mu      sync.Mutex
	entries map[string]*routingModelPriceEntry
	active  int
}

// The request path only looks in bounded memory. At most two background loads
// and eight snapshots (each <= 2000 prices) can exist in one service instance.
func (s *Service) routingModelPrices(model string, candidates []Candidate) (storage.RoutingModelPriceSnapshot, bool) {
	if s == nil || s.Rates == nil || model == "" || len(model) > 256 || len(candidates) == 0 || len(candidates) > 2000 {
		return storage.RoutingModelPriceSnapshot{}, false
	}
	identities := make([]string, 0, len(candidates))
	routes := make([]storage.GatewayRequestGroupCacheRoutingRoute, 0, len(candidates))
	seen := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		if c.DisabledForKey || c.CoolingDown {
			continue
		}
		group := strings.ToLower(strings.TrimSpace(c.Rate.ModelName))
		key := fmt.Sprintf("%d:%s", c.Channel.ID, group)
		if seen[key] {
			continue
		}
		seen[key] = true
		observed := ""
		if c.Channel.ModelPriceObservedAt != nil {
			observed = c.Channel.ModelPriceObservedAt.UTC().Format(time.RFC3339Nano)
		}
		identities = append(identities, fmt.Sprintf("%s:%d:%d:%d:%d:%s", key, c.Channel.UpstreamAccountIdentityRevision, c.Channel.CostUnitEpoch, c.UpstreamGeneration.ID, c.UpstreamGeneration.PresenceGeneration, observed))
		routes = append(routes, storage.GatewayRequestGroupCacheRoutingRoute{ChannelID: c.Channel.ID, GroupName: group})
	}
	if len(routes) == 0 {
		return storage.RoutingModelPriceSnapshot{}, false
	}
	sort.Strings(identities)
	hash := sha256.Sum256([]byte(model + "\x00" + strings.Join(identities, "\x00")))
	key := hex.EncodeToString(hash[:])
	cache := &s.modelPriceCache
	cache.mu.Lock()
	defer cache.mu.Unlock()
	now := time.Now().UTC()
	entry := cache.entries[key]
	if entry != nil && !entry.loading && now.Sub(entry.at) < time.Minute {
		return entry.snapshot, len(entry.snapshot.Prices) > 0
	}
	if entry != nil && entry.loading || cache.active >= 2 {
		return storage.RoutingModelPriceSnapshot{}, false
	}
	if cache.entries == nil {
		cache.entries = make(map[string]*routingModelPriceEntry)
	}
	if entry == nil && len(cache.entries) >= 8 {
		oldestKey := ""
		var oldest time.Time
		for k, v := range cache.entries {
			if !v.loading && (oldestKey == "" || v.at.Before(oldest)) {
				oldestKey, oldest = k, v.at
			}
		}
		if oldestKey == "" {
			return storage.RoutingModelPriceSnapshot{}, false
		}
		delete(cache.entries, oldestKey)
	}
	entry = &routingModelPriceEntry{loading: true}
	cache.entries[key] = entry
	cache.active++
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), routingCacheStatsQueryTimeout)
		defer cancel()
		result, err := s.Rates.RoutingModelPrices(ctx, model, routes, now)
		cache.mu.Lock()
		defer cache.mu.Unlock()
		cache.active--
		if err == nil {
			entry.snapshot = result
		}
		entry.at = time.Now().UTC()
		entry.loading = false
	}()
	return storage.RoutingModelPriceSnapshot{}, false
}

// Every candidate in a comparison uses the same unit and workload. A missing
// or dynamic/length-dependent price keeps the whole comparison on its existing
// multiplier basis, including hypothetical model-filter audit ranks.
func (s *Service) applyRoutingModelPrices(candidates []Candidate, exclusions []CandidateExclusion, model string) {
	all := append([]Candidate(nil), candidates...)
	for _, e := range exclusions {
		if e.Reason == "model_not_listed" || e.Reason == "model_not_supported" {
			all = append(all, e.candidate)
		}
	}
	snapshot, ok := s.routingModelPrices(model, all)
	if !ok {
		return
	}
	prices, ok := routingModelPriceScores(all, snapshot)
	if !ok {
		return
	}
	for i := range candidates {
		if price, exists := prices[canonicalCandidateKey(candidates[i].Channel.ID, candidates[i].Rate.ModelName)]; exists {
			candidates[i].EffectivePrice = price
			candidates[i].EffectivePriceBasis = RoutingPriceBasisModel
			candidates[i].PriceInputShare = snapshot.InputShare
		}
	}
	for i := range exclusions {
		c := &exclusions[i].candidate
		if price, exists := prices[canonicalCandidateKey(c.Channel.ID, c.Rate.ModelName)]; exists {
			c.EffectivePrice = price
			c.EffectivePriceBasis = RoutingPriceBasisModel
			c.PriceInputShare = snapshot.InputShare
		}
	}
}

func routingModelPriceScores(candidates []Candidate, snapshot storage.RoutingModelPriceSnapshot) (map[candidateLatencyKey]float64, bool) {
	prices := make(map[candidateLatencyKey]storage.GatewayChannelModelPrice, len(snapshot.Prices))
	for _, price := range snapshot.Prices {
		prices[canonicalCandidateKey(price.ChannelID, price.UpstreamGroupName)] = price
	}
	out := make(map[candidateLatencyKey]float64, len(candidates))
	inputShare := snapshot.InputShare
	if !snapshot.WorkloadKnown {
		inputShare = 0.5
	}
	if inputShare < 0 || inputShare > 1 || math.IsNaN(inputShare) {
		return nil, false
	}
	for _, c := range candidates {
		if c.DisabledForKey || c.CoolingDown {
			continue
		}
		key := canonicalCandidateKey(c.Channel.ID, c.Rate.ModelName)
		price, ok := prices[key]
		if !ok || !c.UpstreamGeneration.Valid() || price.ChannelIdentityRevision != c.Channel.UpstreamAccountIdentityRevision || price.CostUnitEpoch != c.Channel.CostUnitEpoch || price.GatewayUpstreamGroupID != c.UpstreamGeneration.ID || price.UpstreamGroupPresenceGeneration != c.UpstreamGeneration.PresenceGeneration || math.Abs(price.GroupRatio-c.RawRatio) > 1e-9*math.Max(1, math.Abs(c.RawRatio)) || price.LongContextThresholdTokens != nil || price.LongContextBasis != "" {
			return nil, false
		}
		// Until write TTLs can be separated, use a conservative uncached price:
		// the higher of ordinary input and the provider's single write price.
		if price.CacheWriteUnitPriceUSD == nil || price.CacheReadUnitPriceUSD == nil {
			return nil, false
		}
		uncached := math.Max(price.InputUnitPriceUSD, *price.CacheWriteUnitPriceUSD)
		ratio := 0.0
		if c.CacheRatioKnown {
			ratio = clamp01(c.CacheRatio)
		}
		value := (inputShare*((1-ratio)*uncached+ratio*(*price.CacheReadUnitPriceUSD)) + (1-inputShare)*price.OutputUnitPriceUSD) * price.GroupRatio * c.FundingFactor
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, false
		}
		out[key] = value
	}
	return out, true
}
