package modelprice

import (
	"context"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/modelpricing"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestAutomaticInputMapsStrictCandidate(t *testing.T) {
	observedAt := time.Date(2026, 8, 30, 12, 34, 56, 0, time.UTC)
	candidate := modelpricing.Candidate{
		ModelName:                "gpt-test",
		Short:                    priceTier(4, 20, 0.4, 5),
		Above272K:                priceTier(8, 30, 0.8, 10),
		Above272KThresholdTokens: modelpricing.Above272KThresholdTokens,
		HasAnyAbove272KPrice:     true,
	}
	input, ok := automaticInput(candidate, modelpricing.LiteLLMSourceURL, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", observedAt)
	if !ok {
		t.Fatal("complete strict candidate was rejected")
	}
	if input.ModelName != "gpt-test" || input.InputUnitPriceUSD != 4 || input.OutputUnitPriceUSD != 20 ||
		input.CacheReadUnitPriceUSD == nil || *input.CacheReadUnitPriceUSD != 0.4 ||
		input.CacheWriteUnitPriceUSD == nil || *input.CacheWriteUnitPriceUSD != 5 ||
		input.Source != storage.GatewayModelPriceSourceLiteLLM || input.SourceURL != modelpricing.LiteLLMSourceURL ||
		input.SourceObservedAt != observedAt {
		t.Fatalf("automatic input = %+v", input)
	}
	if input.LongContextThresholdTokens == nil || *input.LongContextThresholdTokens != modelpricing.Above272KThresholdTokens ||
		input.LongContextInputUnitPriceUSD == nil || *input.LongContextInputUnitPriceUSD != 8 ||
		input.LongContextOutputUnitPriceUSD == nil || *input.LongContextOutputUnitPriceUSD != 30 ||
		input.LongContextCacheReadUnitPriceUSD == nil || *input.LongContextCacheReadUnitPriceUSD != 0.8 ||
		input.LongContextCacheWriteUnitPriceUSD == nil || *input.LongContextCacheWriteUnitPriceUSD != 10 {
		t.Fatalf("long-context automatic input = %+v", input)
	}
}

func TestAutomaticInputPreservesShortTierWhenOptionalPricesAreMissing(t *testing.T) {
	completeShort := priceTier(4, 20, 0.4, 5)

	t.Run("missing short cache price", func(t *testing.T) {
		candidate := modelpricing.Candidate{ModelName: "model", Short: completeShort}
		candidate.Short.CacheWriteUSDPerMillion = nil
		input, ok := automaticInput(candidate, "source", "checksum", time.Now())
		if !ok || input.CacheReadUnitPriceUSD == nil || *input.CacheReadUnitPriceUSD != 0.4 ||
			input.CacheWriteUnitPriceUSD != nil {
			t.Fatalf("partial short cache prices = %+v, accepted=%v", input, ok)
		}
	})

	t.Run("partial required long tier", func(t *testing.T) {
		candidate := modelpricing.Candidate{
			ModelName: "model", Short: completeShort,
			Above272K:                modelpricing.Tier{InputUSDPerMillion: pricePointer(8)},
			Above272KThresholdTokens: modelpricing.Above272KThresholdTokens,
			HasAnyAbove272KPrice:     true,
		}
		if input, ok := automaticInput(candidate, "source", "checksum", time.Now()); ok {
			t.Fatalf("partial required long tier was accepted: %+v", input)
		}
	})

	t.Run("wrong long threshold", func(t *testing.T) {
		candidate := modelpricing.Candidate{
			ModelName: "model", Short: completeShort, Above272K: priceTier(8, 30, 0.8, 10),
			Above272KThresholdTokens: 128_000, HasAnyAbove272KPrice: true,
		}
		if input, ok := automaticInput(candidate, "source", "checksum", time.Now()); ok {
			t.Fatalf("unsupported long threshold was accepted: %+v", input)
		}
	})

	t.Run("missing optional long cache prices", func(t *testing.T) {
		candidate := modelpricing.Candidate{
			ModelName: "model", Short: completeShort,
			Above272K: modelpricing.Tier{
				InputUSDPerMillion:  pricePointer(8),
				OutputUSDPerMillion: pricePointer(30),
			},
			Above272KThresholdTokens: modelpricing.Above272KThresholdTokens,
			HasAnyAbove272KPrice:     true,
		}
		input, ok := automaticInput(candidate, "source", "checksum", time.Now())
		if !ok || input.LongContextThresholdTokens == nil ||
			input.LongContextInputUnitPriceUSD == nil || *input.LongContextInputUnitPriceUSD != 8 ||
			input.LongContextOutputUnitPriceUSD == nil || *input.LongContextOutputUnitPriceUSD != 30 ||
			input.LongContextCacheReadUnitPriceUSD != nil || input.LongContextCacheWriteUnitPriceUSD != nil {
			t.Fatalf("long tier with unpublished cache prices = %+v, accepted=%v", input, ok)
		}
	})

	t.Run("no long tier", func(t *testing.T) {
		candidate := modelpricing.Candidate{ModelName: "model", Short: completeShort}
		input, ok := automaticInput(candidate, "source", "checksum", time.Now())
		if !ok || input.LongContextThresholdTokens != nil || input.LongContextInputUnitPriceUSD != nil ||
			input.LongContextOutputUnitPriceUSD != nil || input.LongContextCacheReadUnitPriceUSD != nil ||
			input.LongContextCacheWriteUnitPriceUSD != nil {
			t.Fatalf("short-only candidate = %+v, accepted=%v", input, ok)
		}
	})

	t.Run("missing required short price", func(t *testing.T) {
		candidate := modelpricing.Candidate{ModelName: "model", Short: completeShort}
		candidate.Short.OutputUSDPerMillion = nil
		if _, ok := automaticInput(candidate, "source", "checksum", time.Now()); ok {
			t.Fatal("candidate without a short output price was accepted")
		}
	})
}

func TestSyncNowSkipsImagePricingAndAppliesTokenModel(t *testing.T) {
	observedAt := time.Date(2026, 8, 31, 1, 2, 3, 0, time.UTC)
	store := &modelPriceStoreStub{used: []storage.GatewayUsedModel{
		{ModelName: "gpt-image-1"},
		{ModelName: "gpt-5.6-sol"},
	}}
	service := &Service{
		store: store,
		client: &http.Client{Transport: modelPriceRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			body := `{
				"gpt-image-1":{"input_cost_per_token":0.000005,"output_cost_per_image_token":0.000040},
				"gpt-5.6-sol":{"input_cost_per_token":0.000004,"output_cost_per_token":0.000020,"cache_read_input_token_cost":0.0000004,"cache_creation_input_token_cost":0.000005}
			}`
			return &http.Response{
				StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)),
				ContentLength: int64(len(body)), Request: request,
			}, nil
		})},
		now: func() time.Time { return observedAt },
	}

	result, err := service.SyncNow(context.Background())
	if err != nil {
		t.Fatalf("sync mixed catalog: %v", err)
	}
	if result.DiscoveredModels != 2 || result.MatchedModels != 1 || len(result.UnsupportedModels) != 1 ||
		result.UnsupportedModels[0] != "gpt-image-1" || result.Applied.Created != 1 {
		t.Fatalf("sync result = %+v", result)
	}
	if store.actor != "automatic-litellm-sync" || len(store.applied) != 1 {
		t.Fatalf("apply call actor=%q inputs=%+v", store.actor, store.applied)
	}
	input := store.applied[0]
	if input.ModelName != "gpt-5.6-sol" || input.InputUnitPriceUSD != 4 || input.OutputUnitPriceUSD != 20 ||
		input.CacheReadUnitPriceUSD == nil || math.Abs(*input.CacheReadUnitPriceUSD-0.4) > 1e-12 ||
		input.CacheWriteUnitPriceUSD == nil || *input.CacheWriteUnitPriceUSD != 5 {
		t.Fatalf("applied model price = %+v", input)
	}
}

type modelPriceStoreStub struct {
	used    []storage.GatewayUsedModel
	applied []storage.GatewayAutomaticModelPriceInput
	actor   string
}

func (s *modelPriceStoreStub) ListUsedModels(context.Context, time.Time) ([]storage.GatewayUsedModel, error) {
	return s.used, nil
}

func (s *modelPriceStoreStub) ApplyAutomaticModelPrices(_ context.Context, inputs []storage.GatewayAutomaticModelPriceInput, actor string) (storage.GatewayAutomaticModelPriceResult, error) {
	s.applied = append([]storage.GatewayAutomaticModelPriceInput(nil), inputs...)
	s.actor = actor
	return storage.GatewayAutomaticModelPriceResult{Created: len(inputs)}, nil
}

type modelPriceRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn modelPriceRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func priceTier(input, output, cacheRead, cacheWrite float64) modelpricing.Tier {
	return modelpricing.Tier{
		InputUSDPerMillion:      pricePointer(input),
		OutputUSDPerMillion:     pricePointer(output),
		CacheReadUSDPerMillion:  pricePointer(cacheRead),
		CacheWriteUSDPerMillion: pricePointer(cacheWrite),
	}
}

func pricePointer(value float64) *float64 { return &value }
