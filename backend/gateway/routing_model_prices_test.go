package gateway

import (
	"math"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func routingPriceFixture() ([]Candidate, storage.RoutingModelPriceSnapshot) {
	read, write := 0.2, 2.0
	candidates := []Candidate{}
	snapshot := storage.RoutingModelPriceSnapshot{InputShare: 0.98, WorkloadKnown: true}
	for id := uint(1); id <= 2; id++ {
		ratio, cache := 1.0, 0.9
		if id == 2 {
			ratio, cache = 0.7, 0.05
		}
		candidates = append(candidates, Candidate{Channel: storage.Channel{ID: id, UpstreamAccountIdentityRevision: 1, CostUnitEpoch: 1}, Rate: storage.RateSnapshot{ModelName: "primary", Ratio: ratio}, UpstreamGeneration: storage.UpstreamGroupGeneration{ID: id, ChannelID: id, CanonicalGroupName: "primary", PresenceGeneration: 1}, RawRatio: ratio, FundingFactor: 1, CacheRatio: cache, CacheRatioKnown: true})
		snapshot.Prices = append(snapshot.Prices, storage.GatewayChannelModelPrice{ChannelID: id, ChannelIdentityRevision: 1, CostUnitEpoch: 1, GatewayUpstreamGroupID: id, UpstreamGroupPresenceGeneration: 1, UpstreamGroupName: "primary", ModelName: "gpt-test", GroupRatio: ratio, InputUnitPriceUSD: 2, OutputUnitPriceUSD: 8, CacheReadUnitPriceUSD: &read, CacheWriteUnitPriceUSD: &write})
	}
	return candidates, snapshot
}

func TestRoutingModelPricesPreferEffectiveCachedCost(t *testing.T) {
	candidates, snapshot := routingPriceFixture()
	prices, ok := routingModelPriceScores(candidates, snapshot)
	first, second := prices[canonicalCandidateKey(1, "primary")], prices[canonicalCandidateKey(2, "primary")]
	if !ok || math.Abs(first-0.5324) > 1e-9 || first >= second {
		t.Fatalf("cache-adjusted prices = %v, valid=%v", prices, ok)
	}
	for i := range candidates {
		candidates[i].EffectivePrice = prices[canonicalCandidateKey(candidates[i].Channel.ID, "primary")]
		candidates[i].EffectivePriceBasis = RoutingPriceBasisModel
	}
	scoreCandidatesWithConfig(candidates, RoutingConfig{PriceScoreMax: 70, LatencyScoreMax: 30})
	sortCandidates(candidates)
	if candidates[0].Channel.ID != 1 {
		t.Fatal("lower multiplier with poor cache incorrectly won")
	}
	candidates, snapshot = routingPriceFixture()
	candidates[0].FundingFactor = 0.5
	prices, ok = routingModelPriceScores(candidates, snapshot)
	if !ok || math.Abs(prices[canonicalCandidateKey(1, "primary")]-first*0.5) > 1e-9 {
		t.Fatal("funding factor was not applied exactly once")
	}
}

func TestRoutingModelPricesRejectIncompleteComparison(t *testing.T) {
	for _, kind := range []string{"missing", "generation", "account", "cost-unit", "ratio", "tier", "cache-price"} {
		t.Run(kind, func(t *testing.T) {
			candidates, snapshot := routingPriceFixture()
			switch kind {
			case "missing":
				snapshot.Prices = snapshot.Prices[:1]
			case "generation":
				snapshot.Prices[1].UpstreamGroupPresenceGeneration++
			case "account":
				snapshot.Prices[1].ChannelIdentityRevision++
			case "cost-unit":
				snapshot.Prices[1].CostUnitEpoch++
			case "ratio":
				snapshot.Prices[1].GroupRatio = 99
			case "tier":
				threshold := int64(200000)
				snapshot.Prices[1].LongContextThresholdTokens = &threshold
			case "cache-price":
				snapshot.Prices[1].CacheWriteUnitPriceUSD = nil
			}
			if _, ok := routingModelPriceScores(candidates, snapshot); ok {
				t.Fatal("incomparable prices accepted")
			}
		})
	}
}

func TestRoutingModelPriceZeroAndPreviewUnits(t *testing.T) {
	candidates, _ := routingPriceFixture()
	candidates[0].EffectivePriceBasis = RoutingPriceBasisModel
	candidates[0].EffectivePrice = 0
	candidates[1].EffectivePriceBasis = RoutingPriceBasisModel
	candidates[1].EffectivePrice = 1
	benefits := pricePriorityBenefits(candidates)
	if benefits[0] != 1 || benefits[1] != 0 {
		t.Fatalf("free price benefits=%v", benefits)
	}
	preview := append([]Candidate(nil), candidates...)
	preview[0].CacheRatio = 0
	preview[0].EffectivePrice = 99
	preview[0].EffectivePriceBasis = ""
	projectRoutingModelPricesToPreview(preview, candidates)
	if preview[0].EffectivePrice != 0 || preview[0].EffectivePriceBasis != RoutingPriceBasisModel || preview[0].CacheRatio != 0.9 {
		t.Fatal("preview diverged from request price evidence")
	}
	preview[0].EffectivePrice = 99
	projectRoutingModelPricesToPreview(preview, candidates[:1])
	if preview[0].EffectivePrice != 99 {
		t.Fatal("preview accepted a partial comparison")
	}
}
