package storage

import (
	"math"
	"strings"
	"testing"
)

func TestGatewayRequestPricingUsageFromLogItemRejectsUnsafeArithmetic(t *testing.T) {
	tests := []struct {
		name string
		item GatewayRequestLogItem
	}{
		{
			name: "cache exceeds prompt",
			item: GatewayRequestLogItem{
				UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageInputPresent: true,
				PromptTokens: math.MaxInt64, CacheCreationTokens: math.MaxInt64, CacheReadTokens: 1,
			},
		},
		{
			name: "negative prompt",
			item: GatewayRequestLogItem{
				UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageInputPresent: true,
				PromptTokens: math.MinInt64,
			},
		},
		{
			name: "negative cache without presence",
			item: GatewayRequestLogItem{
				UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageInputPresent: true,
				PromptTokens: 10, CacheReadTokens: -1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			usage, reason := gatewayRequestPricingUsageFromLogItem(&test.item)
			if reason != GatewayPricingUnavailableUsageInvalid || usage.PresenceMask() != 0 {
				t.Fatalf("unsafe log item usage = %+v reason %q", usage, reason)
			}
		})
	}
}

func TestGatewayRequestPricingUsageFromEmbeddingLogItemDoesNotSubtractCache(t *testing.T) {
	item := GatewayRequestLogItem{
		UsageSchema: GatewayPricingUsageSchemaInputTokensV1, UsageInputPresent: true,
		PromptTokens: 10, CacheCreationTokens: 20, CacheReadTokens: 30,
	}
	usage, reason := gatewayRequestPricingUsageFromLogItem(&item)
	if reason != "" || usage.InputTokens == nil || *usage.InputTokens != 10 {
		t.Fatalf("embedding log item usage = %+v reason %q", usage, reason)
	}
}

func TestGatewayRequestPricingUsageFromLogItemRejectsNegativeReportedComponents(t *testing.T) {
	tests := []struct {
		name string
		item GatewayRequestLogItem
	}{
		{name: "output", item: GatewayRequestLogItem{UsageOutputPresent: true, CompletionTokens: -1}},
		{name: "cache read", item: GatewayRequestLogItem{UsageCacheReadPresent: true, CacheReadTokens: -1}},
		{name: "cache write", item: GatewayRequestLogItem{UsageCacheWritePresent: true, CacheCreationTokens: -1}},
		{name: "operation", item: GatewayRequestLogItem{UsageOperationPresent: true, OperationUnits: int64Pointer(-1)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			usage, reason := gatewayRequestPricingUsageFromLogItem(&test.item)
			if reason != GatewayPricingUnavailableUsageInvalid || usage.PresenceMask() != 0 {
				t.Fatalf("negative log item usage = %+v reason %q", usage, reason)
			}
		})
	}
}

func int64Pointer(value int64) *int64 { return &value }

func TestGatewayRequestCalculatedCostSQLCastsTokensBeforeArithmetic(t *testing.T) {
	query := gatewayRequestCalculatedCostSQL("logs")
	for _, fragment := range []string{
		"COALESCE(logs.prompt_tokens, 0)::numeric - COALESCE(logs.cache_creation_tokens, 0)::numeric",
		"THEN COALESCE(logs.prompt_tokens, 0)::numeric * logs.price_input_unit::numeric",
		"COALESCE(logs.prompt_tokens, 0)::numeric > logs.price_long_context_threshold_tokens::numeric",
		"logs.price_fact_schema_version = 2",
		"logs.price_long_context_output_unit",
		"logs.completion_ratio::numeric",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("calculated-cost SQL is missing %q:\n%s", fragment, query)
		}
	}
	if strings.Contains(query, "COALESCE(logs.prompt_tokens, 0) - COALESCE(logs.cache_creation_tokens, 0)") {
		t.Fatalf("calculated-cost SQL performs bigint subtraction before numeric cast:\n%s", query)
	}
}

func TestGatewayRequestCalculatedCostSQLAllowsUnknownUnusedTieredCachePrices(t *testing.T) {
	query := gatewayRequestCalculatedCostSQL("logs")
	for _, fragment := range []string{
		"COALESCE(CASE WHEN COALESCE(logs.prompt_tokens, 0)::numeric > logs.price_long_context_threshold_tokens::numeric THEN logs.price_long_context_cache_read_unit ELSE logs.price_cache_read_unit END, 0)::numeric",
		"COALESCE(logs.cache_read_tokens, 0)::numeric = 0 OR CASE WHEN COALESCE(logs.prompt_tokens, 0)::numeric > logs.price_long_context_threshold_tokens::numeric THEN logs.price_long_context_cache_read_unit ELSE logs.price_cache_read_unit END IS NOT NULL",
		"COALESCE(logs.cache_creation_tokens, 0)::numeric = 0 OR CASE WHEN COALESCE(logs.prompt_tokens, 0)::numeric > logs.price_long_context_threshold_tokens::numeric THEN logs.price_long_context_cache_write_unit ELSE logs.price_cache_write_unit END IS NOT NULL",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("calculated-cost SQL is missing nullable tiered-cache guard %q:\n%s", fragment, query)
		}
	}
	for _, fragment := range []string{
		"logs.price_cache_read_unit IS NOT NULL AND logs.price_cache_write_unit IS NOT NULL",
		"logs.price_long_context_cache_read_unit IS NOT NULL AND logs.price_long_context_cache_write_unit IS NOT NULL",
	} {
		if strings.Contains(query, fragment) {
			t.Fatalf("calculated-cost SQL still requires unused tiered cache prices via %q:\n%s", fragment, query)
		}
	}
}

func TestApplyGatewayRequestUsageMetricsUpdateSkipsOverflowingComponents(t *testing.T) {
	current := GatewayRequestLog{
		PriceOperation: GatewayPricingOperationTextGeneration,
		PriceProtocol:  GatewayPricingProtocolOpenAIResponses,
		UsageSchema:    GatewayPricingUsageSchemaTokensV1, UsageSchemaVersion: 1,
		PromptTokens: 7, CompletionTokens: 8, TotalTokens: 15,
		CacheCreationTokens: 2, CacheReadTokens: 3,
	}
	after := current
	updates := map[string]any{}
	applyGatewayRequestUsageMetricsUpdate(&current, &after, updates, GatewayRequestUsageMetrics{
		PromptTokens: math.MaxInt64, CompletionTokens: 1, HasTokenMetrics: true,
		CacheCreationTokens: -1, CacheReadTokens: 4, HasCacheTokenMetrics: true,
	})
	for _, field := range []string{
		"prompt_tokens", "completion_tokens", "total_tokens", "cache_creation_tokens", "cache_read_tokens",
		"usage_input_present", "usage_output_present", "usage_cache_write_present", "usage_cache_read_present",
	} {
		if _, exists := updates[field]; exists {
			t.Fatalf("invalid reconciliation usage updated %q: %+v", field, updates)
		}
	}
	if after.PromptTokens != current.PromptTokens || after.CompletionTokens != current.CompletionTokens || after.TotalTokens != current.TotalTokens ||
		after.CacheCreationTokens != current.CacheCreationTokens || after.CacheReadTokens != current.CacheReadTokens {
		t.Fatalf("invalid reconciliation usage changed the snapshot: before=%+v after=%+v", current, after)
	}
}

func TestFinalizeGatewayRequestPricingKeepsUsageInvalidTerminal(t *testing.T) {
	log := GatewayRequestLog{
		RecordKind:            GatewayRequestRecordKindRequest,
		StatusCode:            200,
		CostStatus:            GatewayCostStatusUnavailable,
		CostUnavailableReason: GatewayPricingUnavailableUsageInvalid,
	}
	finalizeGatewayRequestPricing(&GatewayRequestDispatchAudit{}, &log)
	if log.CostStatus != GatewayCostStatusUnavailable || log.CostUnavailableReason != GatewayPricingUnavailableUsageInvalid ||
		log.Cost != 0 || log.CostEstimated || log.CostSource != "" {
		t.Fatalf("usage-invalid pricing terminal was downgraded: %+v", log)
	}
}

func TestAggregateCounterMergesSaturateInsteadOfWrapping(t *testing.T) {
	request := GatewayRequestAggregate{Requests: math.MaxInt64, PromptTokens: math.MaxInt64}
	mergeRequestAggregate(&request, &GatewayRequestAggregate{Requests: 1, PromptTokens: 1})
	if request.Requests != math.MaxInt64 || request.PromptTokens != math.MaxInt64 {
		t.Fatalf("request aggregate counters wrapped: %+v", request)
	}

	cache := GatewayRequestGroupCacheAggregate{ValidRequests: math.MaxInt64, PromptTokens: math.MaxInt64}
	mergeGatewayRequestGroupCacheAggregate(&cache, &GatewayRequestGroupCacheAggregate{ValidRequests: 1, PromptTokens: 1})
	if cache.ValidRequests != math.MaxInt64 || cache.PromptTokens != math.MaxInt64 {
		t.Fatalf("group-cache aggregate counters wrapped: %+v", cache)
	}
}
