package storage

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func gatewayRequestPricingTestFact() GatewayRequestPriceFact {
	input := 1.25
	output := 2.5
	cacheRead := 0.25
	cacheWrite := 1.5
	return GatewayRequestPriceFact{
		ID: 1, ChannelID: 1, ChannelAccountIdentityRevision: 2,
		UpstreamGroupID: 3, CanonicalGroupName: "primary", UpstreamGroupPresenceGeneration: 4,
		GatewayUpstreamKeyID: 5, UpstreamKeyGeneration: 6,
		ModelName: "model-exact", Operation: GatewayPricingOperationTextGeneration,
		Protocol:       GatewayPricingProtocolOpenAIChatCompletions,
		InputUnitPrice: &input, OutputUnitPrice: &output,
		CacheReadUnitPrice: &cacheRead, CacheWriteUnitPrice: &cacheWrite,
		Unit: GatewayPricingUnitPerMillionTokens, Currency: "USD", Source: "provider_catalog",
		CostUnitEpoch: 1,
		ObservedAt:    time.Date(2026, 7, 24, 1, 2, 3, 456789123, time.UTC),
		ValidFrom:     time.Date(2026, 7, 24, 1, 2, 3, 456789123, time.UTC),
		FactVersion:   1, SchemaVersion: GatewayRequestPriceFactSchemaVersion,
	}
}

func gatewayRequestTieredPricingTestFact() GatewayRequestPriceFact {
	fact := gatewayRequestPricingTestFact()
	shortInput, shortOutput := 5.0, 30.0
	shortCacheRead, shortCacheWrite := 0.5, 6.25
	longInput, longOutput := 10.0, 45.0
	longCacheRead, longCacheWrite := 1.0, 12.5
	threshold := int64(272_000)
	groupRatio, completionRatio := 0.01, 0.02
	fact.SchemaVersion = GatewayRequestPriceFactTieredSchemaVersion
	fact.InputUnitPrice = &shortInput
	fact.OutputUnitPrice = &shortOutput
	fact.CacheReadUnitPrice = &shortCacheRead
	fact.CacheWriteUnitPrice = &shortCacheWrite
	fact.LongContextThresholdTokens = &threshold
	fact.LongContextInputUnitPrice = &longInput
	fact.LongContextOutputUnitPrice = &longOutput
	fact.LongContextCacheReadUnitPrice = &longCacheRead
	fact.LongContextCacheWriteUnitPrice = &longCacheWrite
	fact.GroupRatio = &groupRatio
	fact.CompletionRatio = &completionRatio
	return fact
}

func TestPopulateGatewayRequestLogCostComparisonPrefersUpstreamAndUsesFrozenUSDPrices(t *testing.T) {
	inputPrice := 2.0
	outputPrice := 4.0
	cacheReadPrice := 0.5
	cacheWritePrice := 1.0
	factID := uint64(17)
	fundingFactor := 9.0
	item := GatewayRequestLogItem{
		PromptTokens:           1_500_000,
		CompletionTokens:       500_000,
		CacheReadTokens:        250_000,
		CacheCreationTokens:    250_000,
		Cost:                   5.5,
		CostStatus:             GatewayCostStatusReconciled,
		FundingFactor:          &fundingFactor,
		PriceFactID:            &factID,
		PriceFactSchemaVersion: GatewayRequestPriceFactSchemaVersion,
		PriceOperation:         GatewayPricingOperationTextGeneration,
		PriceProtocol:          GatewayPricingProtocolOpenAIResponses,
		PriceInputUnit:         &inputPrice,
		PriceOutputUnit:        &outputPrice,
		PriceCacheReadUnit:     &cacheReadPrice,
		PriceCacheWriteUnit:    &cacheWritePrice,
		PriceCurrency:          "USD",
		PriceUnit:              GatewayPricingUnitPerMillionTokens,
		UsageInputPresent:      true,
		UsageOutputPresent:     true,
		UsageCacheReadPresent:  true,
		UsageCacheWritePresent: true,
	}

	populateGatewayRequestLogCostComparison(&item)
	wantCalculated := 4.375
	if item.CalculatedCost == nil || math.Abs(*item.CalculatedCost-wantCalculated) > 1e-12 {
		t.Fatalf("calculated cost = %v, want %v", item.CalculatedCost, wantCalculated)
	}
	if item.UpstreamCost == nil || *item.UpstreamCost != 5.5 || item.DisplayCost == nil || *item.DisplayCost != 5.5 {
		t.Fatalf("upstream/display cost = %v/%v", item.UpstreamCost, item.DisplayCost)
	}
	if item.CalculatedCostStatus != GatewayCostStatusEstimated || item.CalculatedCostUnavailableReason != "" ||
		item.UpstreamCostStatus != GatewayCostStatusReconciled {
		t.Fatalf("cost statuses = calculated %q reason %q upstream %q", item.CalculatedCostStatus, item.CalculatedCostUnavailableReason, item.UpstreamCostStatus)
	}
	if item.CostDifference == nil || math.Abs(*item.CostDifference-1.125) > 1e-12 ||
		item.CostDifferenceRatio == nil || math.Abs(*item.CostDifferenceRatio-(1.125/4.375)) > 1e-12 || !item.CostDifferenceMaterial {
		t.Fatalf("cost difference = %v ratio=%v material=%t", item.CostDifference, item.CostDifferenceRatio, item.CostDifferenceMaterial)
	}
}

func TestPopulateGatewayRequestLogCostComparisonFallsBackToCalculatedAndFailsClosed(t *testing.T) {
	inputPrice := 2.0
	factID := uint64(18)
	item := GatewayRequestLogItem{
		PromptTokens:           500_000,
		Cost:                   99,
		CostStatus:             GatewayCostStatusEstimated,
		PriceFactID:            &factID,
		PriceFactSchemaVersion: GatewayRequestPriceFactSchemaVersion,
		PriceOperation:         GatewayPricingOperationEmbedding,
		PriceProtocol:          GatewayPricingProtocolOpenAIEmbeddings,
		PriceInputUnit:         &inputPrice,
		PriceCurrency:          "USD",
		PriceUnit:              GatewayPricingUnitPerMillionTokens,
		UsageInputPresent:      true,
	}
	populateGatewayRequestLogCostComparison(&item)
	if item.CalculatedCost == nil || *item.CalculatedCost != 1 || item.DisplayCost == nil || *item.DisplayCost != 1 || item.UpstreamCost != nil {
		t.Fatalf("calculated fallback = calculated %v display %v upstream %v", item.CalculatedCost, item.DisplayCost, item.UpstreamCost)
	}

	incomplete := item
	incomplete.UsageInputPresent = false
	populateGatewayRequestLogCostComparison(&incomplete)
	if incomplete.CalculatedCost != nil || incomplete.DisplayCost != nil || incomplete.CalculatedCostUnavailableReason != GatewayPricingUnavailableUsageIncomplete {
		t.Fatalf("incomplete usage did not fail closed: %+v", incomplete)
	}
}

func TestPopulateGatewayRequestLogCostComparisonMaterialThresholdUsesRelativeDifference(t *testing.T) {
	tests := []struct {
		name       string
		calculated float64
		upstream   float64
		material   bool
	}{
		{name: "small absolute difference over five percent", calculated: 0.01, upstream: 0.011, material: true},
		{name: "one cent and over five percent", calculated: 0.10, upstream: 0.11, material: true},
		{name: "explicit zero upstream", calculated: 0.10, upstream: 0, material: true},
		{name: "exactly five percent", calculated: 1, upstream: 1 * 1.05, material: false},
		{name: "fractionally over five percent", calculated: 1, upstream: 1.0500000000005, material: true},
		{name: "over five percent", calculated: 1, upstream: 1.051, material: true},
		{name: "negative over five percent", calculated: 1, upstream: 0.94, material: true},
		{name: "relative noise", calculated: 10, upstream: 10.02, material: false},
		{name: "small decimal exactly five percent", calculated: 0.000065, upstream: 0.00006825, material: false},
		{name: "zero calculated and zero upstream", calculated: 0, upstream: 0, material: false},
		{name: "zero calculated with positive upstream", calculated: 0, upstream: 1, material: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := gatewayRequestLogCostComparisonBoundaryItem(test.calculated, test.upstream)
			populateGatewayRequestLogCostComparison(&item)
			if item.CostDifferenceMaterial != test.material {
				t.Fatalf("material=%t, want %t (calculated=%v upstream=%v difference=%v ratio=%v)", item.CostDifferenceMaterial, test.material, item.CalculatedCost, item.UpstreamCost, item.CostDifference, item.CostDifferenceRatio)
			}
			if test.calculated == 0 && test.upstream > 0 && (item.CostDifference == nil || item.CostDifferenceRatio != nil) {
				t.Fatalf("zero-base comparison must keep the difference without inventing a finite ratio: %+v", item)
			}
		})
	}
}

func gatewayRequestLogCostComparisonBoundaryItem(calculated, upstream float64) GatewayRequestLogItem {
	price := calculated
	operationUnits := int64(1)
	factID := uint64(19)
	return GatewayRequestLogItem{
		Cost:                   upstream,
		CostStatus:             GatewayCostStatusReported,
		PriceFactID:            &factID,
		PriceFactSchemaVersion: GatewayRequestPriceFactSchemaVersion,
		PriceOperation:         GatewayImageOperationGeneration,
		PriceProtocol:          GatewayImageProtocolImages,
		PriceOperationUnit:     &price,
		PriceCurrency:          "USD",
		PriceUnit:              GatewayPricingUnitPerOperation,
		UsageOperationPresent:  true,
		OperationUnits:         &operationUnits,
	}
}

func TestGatewayRequestPriceFactChecksumUsesPostgresTimePrecision(t *testing.T) {
	fact := gatewayRequestPricingTestFact()
	validUntil := fact.ValidFrom.Add(time.Hour + 777*time.Nanosecond)
	fact.ValidUntil = &validUntil
	if err := normalizeGatewayRequestPriceFact(&fact); err != nil {
		t.Fatalf("normalize fact: %v", err)
	}
	if fact.ValidFrom.Nanosecond()%int(time.Microsecond) != 0 || fact.ValidUntil.Nanosecond()%int(time.Microsecond) != 0 {
		t.Fatalf("fact validity was not reduced to PostgreSQL microsecond precision: %s - %s", fact.ValidFrom, *fact.ValidUntil)
	}
	first, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		t.Fatalf("first checksum: %v", err)
	}
	roundTrip := fact
	roundTrip.ValidFrom = roundTrip.ValidFrom.Add(999 * time.Nanosecond)
	until := roundTrip.ValidUntil.Add(999 * time.Nanosecond)
	roundTrip.ValidUntil = &until
	second, err := gatewayRequestPriceFactChecksum(&roundTrip)
	if err != nil {
		t.Fatalf("round-trip checksum: %v", err)
	}
	if first != second {
		t.Fatalf("checksum changed across sub-microsecond database precision: %s != %s", first, second)
	}
}

func TestGatewayRequestPriceFactV1ChecksumIgnoresV2Fields(t *testing.T) {
	fact := gatewayRequestPricingTestFact()
	flatChecksum, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		t.Fatal(err)
	}
	threshold := int64(272_000)
	longPrice, ratio := 99.0, 7.0
	fact.LongContextThresholdTokens = &threshold
	fact.LongContextInputUnitPrice = &longPrice
	fact.LongContextOutputUnitPrice = &longPrice
	fact.LongContextCacheReadUnitPrice = &longPrice
	fact.LongContextCacheWriteUnitPrice = &longPrice
	fact.GroupRatio = &ratio
	fact.CompletionRatio = &ratio
	checksumWithV2Fields, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		t.Fatal(err)
	}
	if checksumWithV2Fields != flatChecksum {
		t.Fatalf("schema-v1 checksum changed after adding v2-only fields: %s != %s", checksumWithV2Fields, flatChecksum)
	}
}

func TestFindCurrentGatewayRequestPriceFactDoesNotResurrectPriorVersion(t *testing.T) {
	db := openTestDB(t)
	base := time.Date(2026, 7, 24, 1, 0, 0, 0, time.UTC)

	v1 := gatewayRequestPricingTestFact()
	v1.ID = 0
	v1.ObservedAt = base
	v1.ValidFrom = base
	v1.FactVersion = 1
	if err := normalizeGatewayRequestPriceFact(&v1); err != nil {
		t.Fatalf("normalize v1: %v", err)
	}
	checksum, err := gatewayRequestPriceFactChecksum(&v1)
	if err != nil {
		t.Fatalf("checksum v1: %v", err)
	}
	v1.Checksum = checksum
	if err := db.Create(&v1).Error; err != nil {
		t.Fatalf("insert v1: %v", err)
	}

	v2 := gatewayRequestPricingTestFact()
	v2.ID = 0
	v2.ObservedAt = base
	v2.ValidFrom = base.Add(time.Hour)
	validUntil := base.Add(2 * time.Hour)
	v2.ValidUntil = &validUntil
	v2.FactVersion = 2
	if err := normalizeGatewayRequestPriceFact(&v2); err != nil {
		t.Fatalf("normalize v2: %v", err)
	}
	checksum, err = gatewayRequestPriceFactChecksum(&v2)
	if err != nil {
		t.Fatalf("checksum v2: %v", err)
	}
	v2.Checksum = checksum
	if err := db.Create(&v2).Error; err != nil {
		t.Fatalf("insert v2: %v", err)
	}

	scope := gatewayRequestPriceScopeFromFact(&v1)
	lookup, err := findCurrentGatewayRequestPriceFactTx(db, scope, base.Add(90*time.Minute))
	if err != nil {
		t.Fatalf("lookup active v2: %v", err)
	}
	if lookup.Fact == nil || lookup.Fact.FactVersion != 2 || lookup.UnavailableReason != "" {
		t.Fatalf("active lookup = %+v", lookup)
	}

	lookup, err = findCurrentGatewayRequestPriceFactTx(db, scope, base.Add(3*time.Hour))
	if err != nil {
		t.Fatalf("lookup expired v2: %v", err)
	}
	if lookup.Fact != nil || lookup.UnavailableReason != GatewayPricingUnavailableFactExpired {
		t.Fatalf("expired lookup resurrected v1: %+v", lookup)
	}
}

func TestEvaluateGatewayRequestPriceFactRequiresCompleteComponentPresence(t *testing.T) {
	fact := gatewayRequestPricingTestFact()
	input, output, cacheRead, cacheWrite := int64(1_000_000), int64(2_000_000), int64(3_000_000), int64(4_000_000)
	estimate := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{
		InputTokens: &input, OutputTokens: &output, CacheReadTokens: &cacheRead, CacheWriteTokens: &cacheWrite,
	})
	if estimate.Status != GatewayCostStatusEstimated || estimate.Cost == nil {
		t.Fatalf("complete estimate = %+v", estimate)
	}
	if want := 1.25 + 5 + 0.75 + 6; math.Abs(*estimate.Cost-want) > 1e-12 {
		t.Fatalf("cost = %.12f, want %.12f", *estimate.Cost, want)
	}

	incomplete := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{
		InputTokens: &input, OutputTokens: &output, CacheReadTokens: &cacheRead,
	})
	if incomplete.Status != GatewayCostStatusUnavailable || incomplete.UnavailableReason != GatewayPricingUnavailableUsageIncomplete || incomplete.Cost != nil {
		t.Fatalf("incomplete estimate = %+v", incomplete)
	}
}

func TestEvaluateGatewayRequestTieredPriceFactUsesWholeLongTierAbovePromptThreshold(t *testing.T) {
	fact := gatewayRequestTieredPricingTestFact()
	output, zero := int64(1_000_000), int64(0)

	inputAtThreshold := int64(272_000)
	atThreshold := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{
		InputTokens: &inputAtThreshold, OutputTokens: &output, CacheReadTokens: &zero, CacheWriteTokens: &zero,
	})
	if atThreshold.Cost == nil || math.Abs(*atThreshold.Cost-0.6136) > 1e-12 {
		t.Fatalf("cost at threshold = %+v, want 0.6136", atThreshold)
	}

	inputAboveThreshold := int64(272_001)
	aboveThreshold := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{
		InputTokens: &inputAboveThreshold, OutputTokens: &output, CacheReadTokens: &zero, CacheWriteTokens: &zero,
	})
	if aboveThreshold.Cost == nil || math.Abs(*aboveThreshold.Cost-0.9272001) > 1e-12 {
		t.Fatalf("cost above threshold = %+v, want 0.9272001", aboveThreshold)
	}
}

func TestEvaluateGatewayRequestTieredPriceFactCountsCachedTokensTowardThreshold(t *testing.T) {
	fact := gatewayRequestTieredPricingTestFact()
	input, output, cacheRead, cacheWrite := int64(1), int64(1_000_000), int64(272_000), int64(0)
	estimate := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{
		InputTokens: &input, OutputTokens: &output, CacheReadTokens: &cacheRead, CacheWriteTokens: &cacheWrite,
	})
	want := float64(input)*10*0.01/1_000_000 + 45*0.02 + float64(cacheRead)*1*0.01/1_000_000
	if estimate.Cost == nil || math.Abs(*estimate.Cost-want) > 1e-12 {
		t.Fatalf("cached-token tiered cost = %+v, want %.12f", estimate, want)
	}
}

func TestGatewayRequestTieredPriceFactRequiresCompleteFrozenTierAndRatios(t *testing.T) {
	fact := gatewayRequestTieredPricingTestFact()
	fact.LongContextOutputUnitPrice = nil
	if err := normalizeGatewayRequestPriceFact(&fact); err == nil {
		t.Fatal("schema-v2 fact accepted a missing long-context output price")
	}
	fact = gatewayRequestTieredPricingTestFact()
	fact.CompletionRatio = nil
	if err := normalizeGatewayRequestPriceFact(&fact); err == nil {
		t.Fatal("schema-v2 fact accepted a missing completion ratio")
	}

	fact = gatewayRequestTieredPricingTestFact()
	providerThreshold := int64(128_000)
	fact.LongContextThresholdTokens = &providerThreshold
	if err := normalizeGatewayRequestPriceFact(&fact); err != nil {
		t.Fatalf("schema-v2 fact rejected a reachable provider threshold: %v", err)
	}

	fact = gatewayRequestTieredPricingTestFact()
	unreachableThreshold := int64(math.MaxInt64)
	fact.LongContextThresholdTokens = &unreachableThreshold
	if err := normalizeGatewayRequestPriceFact(&fact); err == nil {
		t.Fatal("schema-v2 fact accepted an unreachable provider threshold")
	}
}

func TestEvaluateGatewayRequestPriceFactDistinguishesZeroFromMissingPrice(t *testing.T) {
	fact := gatewayRequestPricingTestFact()
	fact.CacheReadUnitPrice = nil
	zero := int64(0)
	one := int64(1)
	usage := GatewayRequestPricingUsage{
		InputTokens: &zero, OutputTokens: &zero, CacheReadTokens: &zero, CacheWriteTokens: &zero,
	}
	estimate := EvaluateGatewayRequestPriceFact(&fact, usage)
	if estimate.Status != GatewayCostStatusEstimated || estimate.Cost == nil || *estimate.Cost != 0 {
		t.Fatalf("explicit zero usage estimate = %+v", estimate)
	}
	usage.CacheReadTokens = &one
	estimate = EvaluateGatewayRequestPriceFact(&fact, usage)
	if estimate.Status != GatewayCostStatusUnavailable || estimate.UnavailableReason != GatewayPricingUnavailablePriceMissing || estimate.Cost != nil {
		t.Fatalf("missing used component price estimate = %+v", estimate)
	}
}

func TestEvaluateGatewayRequestEmbeddingPriceRequiresOnlyInputPresence(t *testing.T) {
	inputPrice := 0.75
	fact := gatewayRequestPricingTestFact()
	fact.Operation = GatewayPricingOperationEmbedding
	fact.Protocol = GatewayPricingProtocolOpenAIEmbeddings
	fact.InputUnitPrice = &inputPrice
	fact.OutputUnitPrice = nil
	fact.CacheReadUnitPrice = nil
	fact.CacheWriteUnitPrice = nil
	if err := normalizeGatewayRequestPriceFact(&fact); err != nil {
		t.Fatalf("normalize embedding fact: %v", err)
	}

	zero := int64(0)
	estimate := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{InputTokens: &zero})
	if estimate.Status != GatewayCostStatusEstimated || estimate.Cost == nil || *estimate.Cost != 0 {
		t.Fatalf("explicit zero embedding usage estimate = %+v", estimate)
	}

	missing := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{})
	if missing.Status != GatewayCostStatusUnavailable || missing.UnavailableReason != GatewayPricingUnavailableUsageIncomplete || missing.Cost != nil {
		t.Fatalf("missing embedding input usage estimate = %+v", missing)
	}

	oneMillion := int64(1_000_000)
	estimate = EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{InputTokens: &oneMillion})
	if estimate.Status != GatewayCostStatusEstimated || estimate.Cost == nil || math.Abs(*estimate.Cost-inputPrice) > 1e-12 {
		t.Fatalf("embedding estimate = %+v, want %.2f", estimate, inputPrice)
	}
}

func TestSetGatewayRequestPricingUsageIsAtomicOnInputTotalOverflow(t *testing.T) {
	originalUnits := int64(7)
	log := GatewayRequestLog{
		PromptTokens: 11, CompletionTokens: 12, CacheReadTokens: 13, CacheCreationTokens: 14,
		UsageInputPresent: true, UsageOutputPresent: true, UsageCacheReadPresent: true,
		UsageCacheWritePresent: true, UsageOperationPresent: true, OperationUnits: &originalUnits,
	}
	original := log
	input, cacheRead := int64(math.MaxInt64), int64(1)
	err := SetGatewayRequestPricingUsage(&log, GatewayRequestPricingUsage{
		InputTokens: &input, CacheReadTokens: &cacheRead,
	})
	if err == nil || !strings.Contains(err.Error(), GatewayPricingUnavailableUsageInvalid) {
		t.Fatalf("overflow error = %v, want %q classification", err, GatewayPricingUnavailableUsageInvalid)
	}
	if !reflect.DeepEqual(log, original) {
		t.Fatalf("overflow mutated request log: got %+v, want %+v", log, original)
	}
}

func TestSetGatewayRequestPricingUsageDoesNotAddStoredCacheToEmbeddingInput(t *testing.T) {
	log := GatewayRequestLog{
		Path: "/v1/embeddings", UsageSchema: GatewayPricingUsageSchemaInputTokensV1,
		PromptTokens: 99, CacheReadTokens: 20, CacheCreationTokens: 30,
		UsageCacheReadPresent: true, UsageCacheWritePresent: true,
	}
	input := int64(10)
	if err := SetGatewayRequestPricingUsage(&log, GatewayRequestPricingUsage{InputTokens: &input}); err != nil {
		t.Fatalf("set embedding usage: %v", err)
	}
	if log.PromptTokens != input || !log.UsageInputPresent || log.UsageCacheReadPresent || log.UsageCacheWritePresent {
		t.Fatalf("embedding usage retained cache in input total: %+v", log)
	}
	usage, reason := gatewayRequestPricingUsageFromLog(&log)
	if reason != "" || usage.InputTokens == nil || *usage.InputTokens != input {
		t.Fatalf("embedding usage round trip = %+v reason %q, want input %d", usage, reason, input)
	}
}

func TestGatewayRequestPricingUsageFromLogRejectsInvalidPromptCacheArithmetic(t *testing.T) {
	tests := []struct {
		name          string
		prompt        int64
		cacheCreation int64
		cacheRead     int64
	}{
		{name: "negative prompt", prompt: math.MinInt64},
		{name: "negative cache creation", cacheCreation: -1},
		{name: "negative cache read", cacheRead: -1},
		{name: "first subtraction overflows", cacheCreation: math.MinInt64},
		{name: "cache exceeds prompt", prompt: math.MaxInt64, cacheCreation: math.MaxInt64, cacheRead: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := GatewayRequestLog{
				UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageInputPresent: true,
				PromptTokens: test.prompt, CacheCreationTokens: test.cacheCreation, CacheReadTokens: test.cacheRead,
			}
			usage, reason := gatewayRequestPricingUsageFromLog(&log)
			if reason != GatewayPricingUnavailableUsageInvalid || usage.PresenceMask() != 0 {
				t.Fatalf("invalid prompt/cache arithmetic = %+v reason %q", usage, reason)
			}
		})
	}
}

func TestGatewayRequestPricingUsageFromLogRejectsNegativeReportedComponents(t *testing.T) {
	tests := []struct {
		name string
		log  GatewayRequestLog
	}{
		{name: "output", log: GatewayRequestLog{UsageOutputPresent: true, CompletionTokens: -1}},
		{name: "cache read", log: GatewayRequestLog{UsageCacheReadPresent: true, CacheReadTokens: -1}},
		{name: "cache write", log: GatewayRequestLog{UsageCacheWritePresent: true, CacheCreationTokens: -1}},
		{name: "missing operation", log: GatewayRequestLog{UsageOperationPresent: true}},
		{name: "negative operation", log: GatewayRequestLog{UsageOperationPresent: true, OperationUnits: int64Pointer(-1)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			usage, reason := gatewayRequestPricingUsageFromLog(&test.log)
			if reason != GatewayPricingUnavailableUsageInvalid || usage.PresenceMask() != 0 {
				t.Fatalf("negative request log usage = %+v reason %q", usage, reason)
			}
		})
	}
}

func TestGatewayRequestPriceFactNormalizationRejectsMismatchedPricingShape(t *testing.T) {
	completions := gatewayRequestPricingTestFact()
	completions.Protocol = GatewayPricingProtocolOpenAICompletions
	if err := normalizeGatewayRequestPriceFact(&completions); err != nil {
		t.Fatalf("normalize completions fact: %v", err)
	}

	wrongProtocol := gatewayRequestPricingTestFact()
	wrongProtocol.Protocol = GatewayPricingProtocolOpenAIEmbeddings
	if err := normalizeGatewayRequestPriceFact(&wrongProtocol); err == nil {
		t.Fatal("text price fact accepted the embeddings protocol")
	}

	embeddingWithOutputPrice := gatewayRequestPricingTestFact()
	embeddingWithOutputPrice.Operation = GatewayPricingOperationEmbedding
	embeddingWithOutputPrice.Protocol = GatewayPricingProtocolOpenAIEmbeddings
	embeddingWithOutputPrice.CacheReadUnitPrice = nil
	embeddingWithOutputPrice.CacheWriteUnitPrice = nil
	if err := normalizeGatewayRequestPriceFact(&embeddingWithOutputPrice); err == nil {
		t.Fatal("embedding price fact accepted an output unit price")
	}
}

func TestGatewayPricingIdentityForRequestCoversSupportedProtocols(t *testing.T) {
	tests := []struct {
		path        string
		operation   string
		protocol    string
		schema      string
		unavailable string
	}{
		{path: "/v1/chat/completions", operation: GatewayPricingOperationTextGeneration, protocol: GatewayPricingProtocolOpenAIChatCompletions, schema: GatewayPricingUsageSchemaTokensV1},
		{path: "/v1/completions", operation: GatewayPricingOperationTextGeneration, protocol: GatewayPricingProtocolOpenAICompletions, schema: GatewayPricingUsageSchemaTokensV1},
		{path: "/v1/messages", operation: GatewayPricingOperationTextGeneration, protocol: GatewayPricingProtocolAnthropicMessages, schema: GatewayPricingUsageSchemaTokensV1},
		{path: "/v1/embeddings", operation: GatewayPricingOperationEmbedding, protocol: GatewayPricingProtocolOpenAIEmbeddings, schema: GatewayPricingUsageSchemaInputTokensV1},
		{path: "/v1/images/generations", operation: GatewayImageOperationGeneration, protocol: GatewayImageProtocolImages, schema: GatewayPricingUsageSchemaOperationV1},
		{path: "/v1/responses", protocol: GatewayPricingProtocolOpenAIResponses, unavailable: GatewayPricingUnavailableOperationAmbiguous},
	}
	for _, test := range tests {
		operation, protocol, schema, unavailable := gatewayPricingIdentityForRequest("POST", test.path)
		if operation != test.operation || protocol != test.protocol || schema != test.schema || unavailable != test.unavailable {
			t.Errorf("identity for %s = %q/%q/%q/%q, want %q/%q/%q/%q", test.path, operation, protocol, schema, unavailable, test.operation, test.protocol, test.schema, test.unavailable)
		}
	}
}

func TestGatewayPricingResponsesAllowsExplicitTextOrImageOperation(t *testing.T) {
	if err := validateGatewayRequestPricingContract(
		GatewayPricingOperationTextGeneration,
		GatewayPricingProtocolOpenAIResponses,
		GatewayPricingUsageSchemaTokensV1,
	); err != nil {
		t.Fatalf("responses text pricing contract: %v", err)
	}
	if err := validateGatewayRequestPricingContract(
		GatewayImageOperationGeneration,
		GatewayImageProtocolResponses,
		GatewayPricingUsageSchemaOperationV1,
	); err != nil {
		t.Fatalf("responses image pricing contract: %v", err)
	}
	if err := validateGatewayRequestPricingContract(
		GatewayPricingOperationEmbedding,
		GatewayPricingProtocolOpenAIResponses,
		GatewayPricingUsageSchemaInputTokensV1,
	); err == nil {
		t.Fatal("responses pricing accepted an embeddings operation")
	}
}

func TestGatewayRequestPricingSnapshotRejectsUnknownSchemaOrVersion(t *testing.T) {
	tests := []GatewayRequestLog{
		{UsageSchema: "unknown_v1", UsageSchemaVersion: 1},
		{
			PriceOperation:     GatewayPricingOperationTextGeneration,
			PriceProtocol:      GatewayPricingProtocolOpenAIChatCompletions,
			UsageSchema:        GatewayPricingUsageSchemaTokensV1,
			UsageSchemaVersion: 2,
		},
		{UsageSchemaVersion: 1},
	}
	for index := range tests {
		if err := normalizeGatewayRequestPricingSnapshot(&tests[index]); err == nil {
			t.Errorf("invalid pricing usage schema case %d was accepted", index)
		}
	}

	valid := GatewayRequestLog{
		Path:               "/v1/embeddings",
		PriceOperation:     GatewayPricingOperationEmbedding,
		PriceProtocol:      GatewayPricingProtocolOpenAIEmbeddings,
		UsageSchema:        GatewayPricingUsageSchemaInputTokensV1,
		UsageSchemaVersion: 1,
		UsageInputPresent:  true,
	}
	if err := normalizeGatewayRequestPricingSnapshot(&valid); err != nil {
		t.Fatalf("valid embedding pricing usage schema: %v", err)
	}
}

func TestGatewayRequestPricingSnapshotAcceptsCompleteScopeWithoutPriceFact(t *testing.T) {
	log := GatewayRequestLog{
		ChannelID:               1,
		ChannelIdentityRevision: 2,
		UpstreamGroupID:         3,
		UpstreamGroupGeneration: 4,
		UpstreamKeyLocalID:      5,
		UpstreamKeyGeneration:   6,
		PriceCanonicalGroupName: "primary",
		PriceModelName:          "model-without-fact",
		PriceOperation:          GatewayPricingOperationTextGeneration,
		PriceProtocol:           GatewayPricingProtocolOpenAIChatCompletions,
		UsageSchema:             GatewayPricingUsageSchemaTokensV1,
		UsageSchemaVersion:      1,
		CostStatus:              GatewayCostStatusUnavailable,
		CostUnavailableReason:   GatewayPricingUnavailableFactMissing,
	}
	if err := normalizeGatewayRequestPricingSnapshot(&log); err != nil {
		t.Fatalf("complete scope without price fact: %v", err)
	}
	// PostgreSQL is the canonicalizer. Under LC_CTYPE=C this is already a
	// canonical name even though Go's Unicode tables would lowercase it.
	log.PriceCanonicalGroupName = "\u00c4"
	if err := normalizeGatewayRequestPricingSnapshot(&log); err != nil {
		t.Fatalf("PostgreSQL-canonical Unicode scope without price fact: %v", err)
	}

	log.UpstreamKeyGeneration = 0
	if err := normalizeGatewayRequestPricingSnapshot(&log); err == nil {
		t.Fatal("partial named pricing scope without a price fact was accepted")
	}
	log = GatewayRequestLog{
		ChannelID:               1,
		ChannelIdentityRevision: 2,
		PriceOperation:          GatewayPricingOperationTextGeneration,
		PriceProtocol:           GatewayPricingProtocolOpenAIChatCompletions,
		UsageSchema:             GatewayPricingUsageSchemaTokensV1,
		UsageSchemaVersion:      1,
	}
	if err := normalizeGatewayRequestPricingSnapshot(&log); err == nil {
		t.Fatal("partial unnamed pricing identity without a price fact was accepted")
	}
}

func TestGatewayRequestPriceFactGroupIdentityUsesPostgresCanonicalName(t *testing.T) {
	fact := gatewayRequestPricingTestFact()
	fact.CanonicalGroupName = "\u00c4"
	group := GatewayUpstreamGroup{
		ChannelID: fact.ChannelID, CanonicalGroupName: fact.CanonicalGroupName,
		PresenceGeneration: fact.UpstreamGroupPresenceGeneration, Present: true,
	}
	if !gatewayRequestPriceFactMatchesGroupIdentity(&fact, group) {
		t.Fatal("matching PostgreSQL-canonical Unicode group identity was rejected")
	}
	group.CanonicalGroupName = "\u00e4"
	if gatewayRequestPriceFactMatchesGroupIdentity(&fact, group) {
		t.Fatal("distinct PostgreSQL-canonical group identities were treated as equal")
	}
}

func TestGatewayRequestPricingSnapshotRejectsFullFactWithoutChannel(t *testing.T) {
	fact := gatewayRequestPricingTestFact()
	if err := normalizeGatewayRequestPriceFact(&fact); err != nil {
		t.Fatalf("normalize fact: %v", err)
	}
	checksum, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		t.Fatalf("checksum fact: %v", err)
	}
	factID := fact.ID
	validFrom := fact.ValidFrom
	log := GatewayRequestLog{
		ChannelIdentityRevision: fact.ChannelAccountIdentityRevision,
		UpstreamGroupID:         fact.UpstreamGroupID,
		UpstreamGroupGeneration: fact.UpstreamGroupPresenceGeneration,
		UpstreamKeyLocalID:      fact.GatewayUpstreamKeyID,
		UpstreamKeyGeneration:   fact.UpstreamKeyGeneration,
		PriceFactID:             &factID,
		PriceCostUnitEpoch:      fact.CostUnitEpoch,
		PriceFactVersion:        fact.FactVersion,
		PriceFactSchemaVersion:  fact.SchemaVersion,
		PriceFactChecksum:       checksum,
		PriceCanonicalGroupName: fact.CanonicalGroupName,
		PriceModelName:          fact.ModelName,
		PriceOperation:          fact.Operation,
		PriceProtocol:           fact.Protocol,
		PriceInputUnit:          fact.InputUnitPrice,
		PriceOutputUnit:         fact.OutputUnitPrice,
		PriceCacheReadUnit:      fact.CacheReadUnitPrice,
		PriceCacheWriteUnit:     fact.CacheWriteUnitPrice,
		PriceCurrency:           fact.Currency,
		PriceUnit:               fact.Unit,
		PriceSource:             fact.Source,
		PriceObservedAt:         &fact.ObservedAt,
		PriceValidFrom:          &validFrom,
		UsageSchema:             GatewayPricingUsageSchemaTokensV1,
		UsageSchemaVersion:      1,
	}
	if err := normalizeGatewayRequestPricingSnapshot(&log); err == nil {
		t.Fatal("full price-fact snapshot without a channel was accepted")
	}
	log.ChannelID = fact.ChannelID
	if err := normalizeGatewayRequestPricingSnapshot(&log); err != nil {
		t.Fatalf("complete full price-fact snapshot: %v", err)
	}
}

func TestNormalizeGatewayRequestDispatchPricingUsageDefaultsDirectImageUnit(t *testing.T) {
	audit := GatewayRequestDispatchAudit{
		Method:             "POST",
		Path:               "/v1/images/generations",
		PriceOperation:     GatewayImageOperationGeneration,
		PriceProtocol:      GatewayImageProtocolImages,
		UsageSchema:        GatewayPricingUsageSchemaOperationV1,
		UsageSchemaVersion: 1,
	}
	if err := normalizeGatewayRequestDispatchPricingUsage(&audit); err != nil {
		t.Fatalf("normalize direct image usage: %v", err)
	}
	if !audit.UsageOperationPresent || audit.OperationUnits == nil || *audit.OperationUnits != 1 {
		t.Fatalf("direct image operation usage = present:%t units:%v", audit.UsageOperationPresent, audit.OperationUnits)
	}
}

func TestNormalizeGatewayRequestDispatchPricingUsageRequiresOneResponsesImageUnit(t *testing.T) {
	one, two := int64(1), int64(2)
	for _, test := range []struct {
		name    string
		present bool
		units   *int64
		wantErr bool
	}{
		{name: "one", present: true, units: &one},
		{name: "missing", wantErr: true},
		{name: "multiple", present: true, units: &two, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			audit := GatewayRequestDispatchAudit{
				Method:                "POST",
				Path:                  "/v1/responses",
				PriceOperation:        GatewayImageOperationGeneration,
				PriceProtocol:         GatewayImageProtocolResponses,
				UsageSchema:           GatewayPricingUsageSchemaOperationV1,
				UsageSchemaVersion:    1,
				UsageOperationPresent: test.present,
				OperationUnits:        test.units,
			}
			err := normalizeGatewayRequestDispatchPricingUsage(&audit)
			if (err != nil) != test.wantErr {
				t.Fatalf("normalize responses image usage error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestGatewayRequestPricingSnapshotRequiresOneResponsesImageUnit(t *testing.T) {
	one, two := int64(1), int64(2)
	for _, test := range []struct {
		name    string
		present bool
		units   *int64
		wantErr bool
	}{
		{name: "one", present: true, units: &one},
		{name: "missing", wantErr: true},
		{name: "multiple", present: true, units: &two, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			log := GatewayRequestLog{
				Path:                  "/v1/responses",
				PriceOperation:        GatewayImageOperationGeneration,
				PriceProtocol:         GatewayImageProtocolResponses,
				UsageSchema:           GatewayPricingUsageSchemaOperationV1,
				UsageSchemaVersion:    1,
				UsageOperationPresent: test.present,
				OperationUnits:        test.units,
			}
			err := normalizeGatewayRequestPricingSnapshot(&log)
			if (err != nil) != test.wantErr {
				t.Fatalf("normalize responses image snapshot error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestEvaluateGatewayRequestOperationPriceDoesNotUseTokenFormula(t *testing.T) {
	price := 0.08
	fact := gatewayRequestPricingTestFact()
	fact.Operation = GatewayImageOperationGeneration
	fact.Protocol = GatewayImageProtocolImages
	fact.InputUnitPrice = nil
	fact.OutputUnitPrice = nil
	fact.CacheReadUnitPrice = nil
	fact.CacheWriteUnitPrice = nil
	fact.OperationUnitPrice = &price
	fact.Unit = GatewayPricingUnitPerOperation
	units := int64(3)
	estimate := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{OperationUnits: &units})
	if estimate.Status != GatewayCostStatusEstimated || estimate.Cost == nil || math.Abs(*estimate.Cost-0.24) > 1e-12 {
		t.Fatalf("operation estimate = %+v", estimate)
	}
}

func TestFinalizeGatewayRequestPricingFailsClosedWithoutUsagePresence(t *testing.T) {
	fact := gatewayRequestPricingTestFact()
	if err := normalizeGatewayRequestPriceFact(&fact); err != nil {
		t.Fatalf("normalize fact: %v", err)
	}
	checksum, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	fact.Checksum = checksum
	audit := GatewayRequestDispatchAudit{
		ChannelID:               fact.ChannelID,
		ChannelIdentityRevision: fact.ChannelAccountIdentityRevision,
		UpstreamGroupID:         fact.UpstreamGroupID,
		UpstreamGroupGeneration: fact.UpstreamGroupPresenceGeneration,
		UpstreamKeyLocalID:      fact.GatewayUpstreamKeyID,
		UpstreamKeyGeneration:   fact.UpstreamKeyGeneration,
	}
	copyGatewayRequestPriceFactToAudit(&fact, &audit)
	log := GatewayRequestLog{
		RecordKind: GatewayRequestRecordKindRequest, StatusCode: 200,
		PromptTokens: 100, CompletionTokens: 50, Cost: 99,
		CostEstimated: true, CostStatus: GatewayCostStatusEstimated, CostSource: "group_ratio_default_quota",
	}
	finalizeGatewayRequestPricing(&audit, &log)
	if log.CostStatus != GatewayCostStatusUnavailable || log.CostUnavailableReason != GatewayPricingUnavailableUsageIncomplete {
		t.Fatalf("final pricing status = %+v", log)
	}
	if log.Cost != 0 || log.CostEstimated || log.CostSource != "" || log.RawCost != nil {
		t.Fatalf("unavailable pricing retained a numeric estimate: %+v", log)
	}
}

func TestFinalizeGatewayRequestPricingFenceOverridesIncomingReportedAndReconciledCost(t *testing.T) {
	for _, unavailableReason := range []string{
		GatewayPricingUnavailableScopeIncomplete,
		GatewayPricingUnavailableIdentityChanged,
	} {
		for _, status := range []GatewayCostStatus{GatewayCostStatusReported, GatewayCostStatusReconciled} {
			t.Run(unavailableReason+"/"+string(status), func(t *testing.T) {
				raw := 2.5
				log := GatewayRequestLog{
					RecordKind: GatewayRequestRecordKindRequest,
					StatusCode: 200,
					Cost:       1.25, RawCost: &raw, CostSource: "sub2api_actual_cost", CostStatus: status,
				}
				finalizeGatewayRequestPricing(&GatewayRequestDispatchAudit{PricingUnavailableReason: unavailableReason}, &log)
				if log.CostStatus != GatewayCostStatusUnavailable || log.CostUnavailableReason != unavailableReason {
					t.Fatalf("pricing fence status = %+v", log)
				}
				if log.Cost != 0 || log.RawCost != nil || log.CostEstimated || log.CostSource != "" {
					t.Fatalf("pricing fence retained monetary claim: %+v", log)
				}
			})
		}
	}
}

func TestFinalizeGatewayRequestPricingPreservesReliableCostForOrdinaryFactUnavailability(t *testing.T) {
	for _, unavailableReason := range []string{
		GatewayPricingUnavailableFactMissing,
		GatewayPricingUnavailableFactExpired,
		GatewayPricingUnavailableOperationAmbiguous,
		GatewayPricingUnavailableOperationUnsupported,
	} {
		for _, status := range []GatewayCostStatus{GatewayCostStatusReported, GatewayCostStatusReconciled} {
			t.Run(unavailableReason+"/"+string(status), func(t *testing.T) {
				raw := 2.5
				log := GatewayRequestLog{
					RecordKind: GatewayRequestRecordKindRequest,
					StatusCode: 200,
					Cost:       1.25, RawCost: &raw, CostSource: "sub2api_actual_cost", CostStatus: status,
				}
				finalizeGatewayRequestPricing(&GatewayRequestDispatchAudit{PricingUnavailableReason: unavailableReason}, &log)
				if log.CostStatus != status || log.CostUnavailableReason != "" || log.Cost != 1.25 || log.RawCost == nil || *log.RawCost != raw || log.CostSource != "sub2api_actual_cost" {
					t.Fatalf("ordinary pricing unavailability erased reliable cost: %+v", log)
				}
			})
		}
	}
}

func TestAppendRequestLogDoesNotRefillPricingFenceCostBasis(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	log := &GatewayRequestLog{
		ChannelID:               99,
		RecordKind:              GatewayRequestRecordKindRequest,
		Method:                  "POST",
		Path:                    "/v1/responses",
		StatusCode:              200,
		CostStatus:              GatewayCostStatusUnavailable,
		CostUnavailableReason:   GatewayPricingUnavailableScopeIncomplete,
		UpstreamRequestID:       "provider-pricing-fence",
		ChannelIdentityRevision: 0,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append pricing-fenced log: %v", err)
	}
	if log.FundingFactor != nil || log.RawGroupRatio != nil || log.RawCompletionRatio != nil || log.RawCost != nil ||
		log.CostBasisVersion != 0 || log.CostBasisSource != "" {
		t.Fatalf("append refilled pricing-fenced cost basis: %+v", log)
	}
}

func TestClaimedCostReconcileDoesNotOverwritePricingFence(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	log := &GatewayRequestLog{
		ChannelID:             99,
		RecordKind:            GatewayRequestRecordKindRequest,
		Method:                "POST",
		Path:                  "/v1/responses",
		StatusCode:            200,
		CostStatus:            GatewayCostStatusUnavailable,
		CostUnavailableReason: GatewayPricingUnavailableScopeIncomplete,
		UpstreamRequestID:     "provider-pricing-fence-reconcile",
		CostReconcileAt:       &now,
		CreatedAt:             now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append pricing-fenced reconcile log: %v", err)
	}
	if err := store.UpdateClaimedRequestLogActualCostWithUsage(log.ID, now, "usage-pricing-fence", 2.5, 1.25, "sub2api_actual_cost", GatewayRequestUsageMetrics{}); err != nil {
		t.Fatalf("update pricing-fenced reconcile log: %v", err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read pricing-fenced reconcile log: %v", err)
	}
	if stored.CostStatus != GatewayCostStatusUnavailable || stored.CostUnavailableReason != GatewayPricingUnavailableScopeIncomplete ||
		stored.Cost != 0 || stored.RawCost != nil || stored.CostSource != "" || stored.UpstreamUsageID != nil {
		t.Fatalf("pricing-fenced reconciliation retained a monetary claim: %+v", stored)
	}
	if stored.CostReconcileAt != nil || stored.CostReconcileError != gatewayCostReconcilePricingUnavailableError {
		t.Fatalf("pricing-fenced reconciliation was not safely stopped: %+v", stored)
	}
}

func TestFreezeGatewayRequestPricingUsesOnlySelectionTimeIdentity(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(t *testing.T, db *gorm.DB, channel *Channel, group UpstreamGroupGeneration, key GatewayUpstreamKey)
		mutateAudit func(*GatewayRequestDispatchAudit)
		wantOK      bool
		wantReason  string
	}{
		{name: "unchanged selection", wantOK: true},
		{
			name: "same name group recreated before writer",
			mutate: func(t *testing.T, db *gorm.DB, _ *Channel, group UpstreamGroupGeneration, _ GatewayUpstreamKey) {
				t.Helper()
				if err := db.Model(&GatewayUpstreamGroup{}).Where("id = ?", group.ID).
					Update("presence_generation", group.PresenceGeneration+1).Error; err != nil {
					t.Fatalf("advance same-name group generation: %v", err)
				}
			},
			wantReason: GatewayPricingUnavailableIdentityChanged,
		},
		{
			name: "same name key rebuilt before writer",
			mutate: func(t *testing.T, db *gorm.DB, _ *Channel, _ UpstreamGroupGeneration, key GatewayUpstreamKey) {
				t.Helper()
				if err := db.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).
					Update("api_key_cipher", "cipher-rebuilt").Error; err != nil {
					t.Fatalf("rebuild same-name key identity: %v", err)
				}
			},
			wantReason: GatewayPricingUnavailableIdentityChanged,
		},
		{
			name:        "incomplete selected scope",
			mutateAudit: func(audit *GatewayRequestDispatchAudit) { audit.UpstreamKeyGeneration = 0 },
			wantReason:  GatewayPricingUnavailableScopeIncomplete,
		},
		{
			// The identity tuple is restored before the price scope is normalized.
			// This reaches the hard fence after all IDs are nonzero.
			name: "scope becomes incomplete after identity restoration",
			mutateAudit: func(audit *GatewayRequestDispatchAudit) {
				audit.ModelName = ""
				audit.UpstreamModelName = ""
			},
			wantReason: GatewayPricingUnavailableScopeIncomplete,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, _, channel, group, key, audit := gatewayRequestPricingSelectionFixture(t)
			if test.mutate != nil {
				test.mutate(t, db, channel, group, key)
			}
			if test.mutateAudit != nil {
				test.mutateAudit(&audit)
			}
			if err := db.Transaction(func(tx *gorm.DB) error {
				return freezeGatewayRequestPricingTx(tx, &audit, time.Now().UTC())
			}); err != nil {
				t.Fatalf("freeze request pricing: %v", err)
			}
			if test.wantOK {
				if audit.PricingUnavailableReason != "" || audit.PriceFactID == nil ||
					audit.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision ||
					audit.UpstreamGroupID != group.ID || audit.UpstreamGroupGeneration != group.PresenceGeneration ||
					audit.UpstreamKeyLocalID != key.ID || audit.UpstreamKeyGeneration != key.KeyGeneration {
					t.Fatalf("selection-time pricing provenance = %+v", audit)
				}
				return
			}
			if audit.PricingUnavailableReason != test.wantReason || audit.PriceFactID != nil ||
				audit.ChannelIdentityRevision != 0 || audit.UpstreamGroupID != 0 || audit.UpstreamGroupGeneration != 0 ||
				audit.UpstreamKeyLocalID != 0 || audit.UpstreamKeyGeneration != 0 {
				t.Fatalf("replaced identity must have no pricing provenance: %+v", audit)
			}
			if audit.PriceOperation != GatewayPricingOperationTextGeneration ||
				audit.PriceProtocol != GatewayPricingProtocolOpenAIChatCompletions ||
				audit.UsageSchema != GatewayPricingUsageSchemaTokensV1 || audit.UsageSchemaVersion != 1 ||
				audit.UsageOperationPresent || audit.OperationUnits != nil {
				t.Fatalf("pricing hard fence lost or changed the request contract: %+v", audit)
			}
			assertGatewayRequestPricingHardFenceClear(t, audit)
		})
	}
}

func TestFreezeGatewayRequestPricingRejectsPriorCostUnitEpochFact(t *testing.T) {
	db, _, channel, group, key, audit := gatewayRequestPricingSelectionFixture(t)
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).
		Update("cost_unit_epoch", channel.CostUnitEpoch+1).Error; err != nil {
		t.Fatalf("advance channel cost-unit epoch: %v", err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return freezeGatewayRequestPricingTx(tx, &audit, time.Now().UTC())
	}); err != nil {
		t.Fatalf("freeze request pricing after cost-unit rebase: %v", err)
	}
	if audit.PricingUnavailableReason != GatewayPricingUnavailableCostBasisChanged || audit.PriceFactID != nil || audit.PriceCostUnitEpoch != 0 {
		t.Fatalf("prior-epoch price fact was frozen: %+v", audit)
	}
	if audit.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		audit.UpstreamGroupID != group.ID || audit.UpstreamGroupGeneration != group.PresenceGeneration ||
		audit.UpstreamKeyLocalID != key.ID || audit.UpstreamKeyGeneration != key.KeyGeneration ||
		audit.PriceCanonicalGroupName != "primary" || audit.PriceModelName != "model-exact" {
		t.Fatalf("cost-basis change lost immutable selection scope: %+v", audit)
	}
}

func TestGatewayRequestLogListProjectionIncludesPricingFenceMetadata(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	log := &GatewayRequestLog{
		DownstreamKeyID: 1,
		ModelName:       "projection-model",
		Method:          "POST",
		Path:            "/v1/responses",
		StatusCode:      200,
		CostBasisSource: "confirmed_recharge",
		CreatedAt:       createdAt,
	}
	attachTrustedEstimatedTextPriceFact(t, db, log, 1, 2)
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append priced request log: %v", err)
	}

	assertProjection := func(name string, items []GatewayRequestLogItem, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(items) != 1 {
			t.Fatalf("%s returned %d items", name, len(items))
		}
		item := items[0]
		if item.CostBasisSource != log.CostBasisSource || item.PriceCostUnitEpoch != log.PriceCostUnitEpoch ||
			item.PriceObservedAt == nil || log.PriceObservedAt == nil || !item.PriceObservedAt.Equal(*log.PriceObservedAt) {
			t.Fatalf("%s omitted pricing fence metadata: %+v", name, item)
		}
	}

	items, err := store.ListRequestLogPageItemsFiltered(1, 10, GatewayRequestLogFilter{})
	assertProjection("paged list", items, err)
	items, err = store.ListRequestLogsFiltered(GatewayRequestLogFilter{}, 10)
	assertProjection("export list", items, err)
}

func TestGatewayRequestLogCostSortUsesDisplayedAmountBeforePagination(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 8, 13, 1, 0, 0, 0, time.UTC)
	calculatedLow := &GatewayRequestLog{
		DownstreamKeyID: 1, ModelName: "calculated-low", Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 1_000_000, Cost: 99, CreatedAt: now,
	}
	attachTrustedEstimatedTextPriceFact(t, db, calculatedLow, 1, 0)
	calculatedHigh := &GatewayRequestLog{
		DownstreamKeyID: 1, ModelName: "calculated-high", Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 1_000_000, Cost: 0.01, CreatedAt: now.Add(time.Minute),
	}
	attachTrustedEstimatedTextPriceFact(t, db, calculatedHigh, 2, 0)
	unavailable := &GatewayRequestLog{
		DownstreamKeyID: 1, ModelName: "unavailable", Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 1_000_000, Cost: 1000, CreatedAt: now.Add(2 * time.Minute),
	}
	attachTrustedEstimatedTextPriceFact(t, db, unavailable, 1000, 0)
	unavailable.UsageOutputPresent = false
	logs := []*GatewayRequestLog{
		calculatedLow,
		calculatedHigh,
		unavailable,
		{DownstreamKeyID: 1, ModelName: "reported-middle", Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 1.5, CostStatus: GatewayCostStatusReported, CostSource: "cost", CreatedAt: now.Add(3 * time.Minute)},
		{DownstreamKeyID: 1, ModelName: "reconciled-high", Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 3, CostStatus: GatewayCostStatusReconciled, CostSource: gatewayCostSourceSub2APIActualCost, CreatedAt: now.Add(4 * time.Minute)},
	}
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append %s: %v", log.ModelName, err)
		}
	}

	assertPage := func(order string, want ...string) {
		t.Helper()
		items, err := store.ListRequestLogPageItemsFiltered(1, len(want), GatewayRequestLogFilter{SortBy: GatewayRequestLogSortCost, SortOrder: order})
		if err != nil {
			t.Fatalf("sort cost %s: %v", order, err)
		}
		if len(items) != len(want) {
			t.Fatalf("sort cost %s returned %d items, want %d", order, len(items), len(want))
		}
		for index, model := range want {
			if items[index].ModelName != model {
				t.Fatalf("sort cost %s item %d = %s, want %s", order, index, items[index].ModelName, model)
			}
		}
	}
	assertPage(GatewayRequestLogSortAscending, "calculated-low", "reported-middle")
	assertPage(GatewayRequestLogSortDescending, "reconciled-high", "calculated-high")

	for _, order := range []string{GatewayRequestLogSortAscending, GatewayRequestLogSortDescending} {
		items, err := store.ListRequestLogsFiltered(GatewayRequestLogFilter{SortBy: GatewayRequestLogSortCost, SortOrder: order}, 10)
		if err != nil {
			t.Fatalf("export sort cost %s: %v", order, err)
		}
		if len(items) != len(logs) || items[len(items)-1].ModelName != "unavailable" || items[len(items)-1].DisplayCost != nil {
			t.Fatalf("sort cost %s did not keep unavailable amount last: %+v", order, items)
		}
	}
}

func TestBeginRequestDispatchClearsPricingIdentityForUnresolvedOperation(t *testing.T) {
	tests := []struct {
		name         string
		method       string
		path         string
		wantReason   string
		wantProtocol string
	}{
		{
			name:         "responses operation ambiguous",
			method:       "POST",
			path:         "/v1/responses",
			wantReason:   GatewayPricingUnavailableOperationAmbiguous,
			wantProtocol: GatewayPricingProtocolOpenAIResponses,
		},
		{
			name:       "operation unsupported",
			method:     "POST",
			path:       "/v1/unsupported",
			wantReason: GatewayPricingUnavailableOperationUnsupported,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, store, channel, group, key, selected := gatewayRequestPricingSelectionFixture(t)
			now := time.Now().UTC()
			snapshot := GatewayRequestDispatchSnapshot{
				ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: selected.GroupName,
				UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
				ModelName: selected.ModelName, UpstreamModelName: selected.UpstreamModelName,
				Method: test.method, Path: test.path, RequestStartedAt: now,
				GroupRatio: selected.GroupRatio, CompletionRatio: selected.CompletionRatio,
				RawGroupRatio: selected.RawGroupRatio, RawCompletionRatio: selected.RawCompletionRatio,
				FundingFactor: selected.FundingFactor, CostBasisVersion: selected.CostBasisVersion, CostBasisSource: selected.CostBasisSource,
				ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
				UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
				UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
			}
			snapshot = requestDispatchSnapshotWithCurrentTelemetry(t, snapshot, now)
			persisted, err := store.BeginRequestDispatch(snapshot, now)
			if err != nil {
				t.Fatalf("begin unresolved-operation dispatch: %v", err)
			}
			var stored GatewayRequestDispatchAudit
			if err := db.First(&stored, persisted.ID).Error; err != nil {
				t.Fatalf("read unresolved-operation dispatch: %v", err)
			}
			if stored.PricingUnavailableReason != test.wantReason || stored.PriceOperation != "" ||
				stored.PriceProtocol != test.wantProtocol || stored.UsageSchema != "" || stored.UsageSchemaVersion != 0 ||
				stored.UsageOperationPresent || stored.OperationUnits != nil {
				t.Fatalf("unresolved-operation contract = %+v", stored)
			}
			assertGatewayRequestPricingIdentityClear(t, stored)
			if stored.ChannelID != snapshot.ChannelID || stored.GroupName != snapshot.GroupName ||
				stored.UpstreamKeyID != snapshot.UpstreamKeyID || stored.UpstreamKeyName != snapshot.UpstreamKeyName ||
				stored.ModelName != snapshot.ModelName || stored.UpstreamModelName != snapshot.UpstreamModelName ||
				stored.GroupRatio != snapshot.GroupRatio || stored.CompletionRatio != snapshot.CompletionRatio ||
				stored.RawGroupRatio == nil || snapshot.RawGroupRatio == nil || *stored.RawGroupRatio != *snapshot.RawGroupRatio ||
				stored.RawCompletionRatio == nil || snapshot.RawCompletionRatio == nil || *stored.RawCompletionRatio != *snapshot.RawCompletionRatio ||
				stored.FundingFactor == nil || snapshot.FundingFactor == nil || *stored.FundingFactor != *snapshot.FundingFactor ||
				stored.CostBasisVersion != snapshot.CostBasisVersion || stored.CostBasisSource != snapshot.CostBasisSource {
				t.Fatalf("unresolved operation changed retained route/accounting scalar values: %+v", stored)
			}

			markedAt := now.Add(time.Millisecond)
			if err := store.MarkRequestDispatchPhysicallyDispatched(stored.ID, markedAt); err != nil {
				t.Fatalf("mark unresolved-operation dispatch physical: %v", err)
			}
			if err := store.ConfirmRequestDispatchTransportInvocation(stored.ID, markedAt.Add(time.Millisecond)); err != nil {
				t.Fatalf("confirm unresolved-operation transport invocation: %v", err)
			}
			log := &GatewayRequestLog{StatusCode: 200, CreatedAt: markedAt.Add(2 * time.Millisecond)}
			created, err := store.FinalizeRequestDispatchResult(stored.ID, log, "response_recorded", log.CreatedAt)
			if err != nil || !created || log.ID == 0 {
				t.Fatalf("finalize unresolved-operation dispatch: created=%t log=%+v err=%v", created, log, err)
			}
			if log.CostStatus != GatewayCostStatusUnavailable || log.CostUnavailableReason != test.wantReason ||
				log.PriceOperation != "" || log.PriceProtocol != test.wantProtocol || log.UsageSchema != "" ||
				log.UsageSchemaVersion != 0 || log.UsageOperationPresent || log.OperationUnits != nil {
				t.Fatalf("finalized unresolved-operation contract = %+v", log)
			}
			if log.ChannelIdentityRevision != 0 || log.UpstreamGroupID != 0 || log.UpstreamGroupGeneration != 0 ||
				log.UpstreamKeyLocalID != 0 || log.UpstreamKeyGeneration != 0 ||
				log.PriceCanonicalGroupName != "" || log.PriceModelName != "" || log.PriceFactID != nil {
				t.Fatalf("finalized unresolved operation retained pricing attribution: %+v", log)
			}
			if log.GroupRatio != snapshot.GroupRatio || log.CompletionRatio != snapshot.CompletionRatio ||
				log.RawGroupRatio == nil || snapshot.RawGroupRatio == nil || *log.RawGroupRatio != *snapshot.RawGroupRatio ||
				log.RawCompletionRatio == nil || snapshot.RawCompletionRatio == nil || *log.RawCompletionRatio != *snapshot.RawCompletionRatio ||
				log.FundingFactor == nil || snapshot.FundingFactor == nil || *log.FundingFactor != *snapshot.FundingFactor ||
				log.CostBasisVersion != snapshot.CostBasisVersion || log.CostBasisSource != snapshot.CostBasisSource {
				t.Fatalf("finalized unresolved operation changed retained accounting scalar values: %+v", log)
			}
		})
	}
}

func TestBeginRequestDispatchRejectsPricingContractForUnsupportedOrMismatchedEndpoint(t *testing.T) {
	operationUnit := int64(1)
	tests := []struct {
		name                  string
		method                string
		path                  string
		operation             string
		protocol              string
		usageSchema           string
		usageOperationPresent bool
		operationUnits        *int64
	}{
		{
			name: "unsupported method with text contract", method: "GET", path: "/v1/responses",
			operation: GatewayPricingOperationTextGeneration, protocol: GatewayPricingProtocolOpenAIResponses,
			usageSchema: GatewayPricingUsageSchemaTokensV1,
		},
		{
			name: "unsupported path with text contract", method: "POST", path: "/v1/unsupported",
			operation: GatewayPricingOperationTextGeneration, protocol: GatewayPricingProtocolOpenAIResponses,
			usageSchema: GatewayPricingUsageSchemaTokensV1,
		},
		{
			name: "chat endpoint with image contract", method: "POST", path: "/v1/chat/completions",
			operation: GatewayImageOperationGeneration, protocol: GatewayImageProtocolResponses,
			usageSchema: GatewayPricingUsageSchemaOperationV1, usageOperationPresent: true, operationUnits: &operationUnit,
		},
		{
			name: "responses endpoint with embeddings contract", method: "POST", path: "/v1/responses",
			operation: GatewayPricingOperationEmbedding, protocol: GatewayPricingProtocolOpenAIEmbeddings,
			usageSchema: GatewayPricingUsageSchemaInputTokensV1,
		},
		{
			name: "responses endpoint with chat completions contract", method: "POST", path: "/v1/responses",
			operation: GatewayPricingOperationTextGeneration, protocol: GatewayPricingProtocolOpenAIChatCompletions,
			usageSchema: GatewayPricingUsageSchemaTokensV1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, store, channel, group, key, selected := gatewayRequestPricingSelectionFixture(t)
			now := time.Now().UTC()
			snapshot := GatewayRequestDispatchSnapshot{
				ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: selected.GroupName,
				UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
				ModelName: selected.ModelName, UpstreamModelName: selected.UpstreamModelName,
				Method: test.method, Path: test.path, RequestStartedAt: now,
				GroupRatio: selected.GroupRatio, CompletionRatio: selected.CompletionRatio,
				RawGroupRatio: selected.RawGroupRatio, RawCompletionRatio: selected.RawCompletionRatio,
				FundingFactor: selected.FundingFactor, CostBasisVersion: selected.CostBasisVersion, CostBasisSource: selected.CostBasisSource,
				ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
				UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
				UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
				PriceOperation: test.operation, PriceProtocol: test.protocol,
				UsageSchema: test.usageSchema, UsageSchemaVersion: 1,
				UsageOperationPresent: test.usageOperationPresent, OperationUnits: test.operationUnits,
			}
			snapshot = requestDispatchSnapshotWithCurrentTelemetry(t, snapshot, now)
			if _, err := store.BeginRequestDispatch(snapshot, now); err == nil {
				t.Fatal("unsupported or mismatched pricing contract was accepted")
			}
			var auditCount int64
			if err := db.Model(&GatewayRequestDispatchAudit{}).Count(&auditCount).Error; err != nil {
				t.Fatalf("count dispatch audits: %v", err)
			}
			if auditCount != 0 {
				t.Fatalf("rejected pricing contract persisted %d dispatch audits", auditCount)
			}
		})
	}
}

func TestBeginRequestDispatchAcceptsResponsesParserContract(t *testing.T) {
	operationUnit := int64(1)
	tests := []struct {
		name                  string
		operation             string
		protocol              string
		usageSchema           string
		usageOperationPresent bool
		operationUnits        *int64
	}{
		{
			name: "text", operation: GatewayPricingOperationTextGeneration,
			protocol: GatewayPricingProtocolOpenAIResponses, usageSchema: GatewayPricingUsageSchemaTokensV1,
		},
		{
			name: "image", operation: GatewayImageOperationGeneration,
			protocol: GatewayImageProtocolResponses, usageSchema: GatewayPricingUsageSchemaOperationV1,
			usageOperationPresent: true, operationUnits: &operationUnit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, store, channel, group, key, selected := gatewayRequestPricingSelectionFixture(t)
			now := time.Now().UTC()
			snapshot := GatewayRequestDispatchSnapshot{
				ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: selected.GroupName,
				UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
				ModelName: selected.ModelName, UpstreamModelName: selected.UpstreamModelName,
				Method: "POST", Path: "/v1/responses", RequestStartedAt: now,
				GroupRatio: selected.GroupRatio, CompletionRatio: selected.CompletionRatio,
				RawGroupRatio: selected.RawGroupRatio, RawCompletionRatio: selected.RawCompletionRatio,
				FundingFactor: selected.FundingFactor, CostBasisVersion: selected.CostBasisVersion, CostBasisSource: selected.CostBasisSource,
				ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
				UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
				UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
				PriceOperation: test.operation, PriceProtocol: test.protocol,
				UsageSchema: test.usageSchema, UsageSchemaVersion: 1,
				UsageOperationPresent: test.usageOperationPresent, OperationUnits: test.operationUnits,
			}
			snapshot = requestDispatchSnapshotWithCurrentTelemetry(t, snapshot, now)
			persisted, err := store.BeginRequestDispatch(snapshot, now)
			if err != nil {
				t.Fatalf("begin responses %s dispatch: %v", test.name, err)
			}
			if persisted.PriceOperation != test.operation || persisted.PriceProtocol != test.protocol ||
				persisted.UsageSchema != test.usageSchema || persisted.UsageSchemaVersion != 1 ||
				persisted.UsageOperationPresent != test.usageOperationPresent ||
				(persisted.OperationUnits == nil) != (test.operationUnits == nil) ||
				persisted.OperationUnits != nil && *persisted.OperationUnits != *test.operationUnits {
				t.Fatalf("responses %s contract = %+v", test.name, persisted)
			}
		})
	}
}

func TestClearGatewayRequestPricingHardFenceClearsRestoredProvenance(t *testing.T) {
	priceFactID := uint64(17)
	price := 2.5
	validFrom := time.Now().UTC()
	operationUnits := int64(1)
	for _, reason := range []string{GatewayPricingUnavailableScopeIncomplete, GatewayPricingUnavailableIdentityChanged} {
		t.Run(reason, func(t *testing.T) {
			audit := GatewayRequestDispatchAudit{
				GroupRatio: 3, CompletionRatio: 4, RawGroupRatio: &price, RawCompletionRatio: &price,
				FundingFactor: &price, CostBasisVersion: 9, CostBasisSource: "funding_correction",
				ChannelIdentityRevision: 7, UpstreamGroupID: 8, UpstreamGroupGeneration: 9,
				UpstreamKeyLocalID: 10, UpstreamKeyGeneration: 11,
				PriceFactID: &priceFactID, PriceFactVersion: 12, PriceFactSchemaVersion: GatewayRequestPriceFactSchemaVersion,
				PriceFactChecksum: "checksum", PriceCanonicalGroupName: "primary", PriceModelName: "model",
				PriceInputUnit: &price, PriceOutputUnit: &price, PriceCacheReadUnit: &price, PriceCacheWriteUnit: &price,
				PriceOperationUnit: &price, PriceCurrency: "USD", PriceUnit: GatewayPricingUnitPerOperation,
				PriceSource: "snapshot", PriceValidFrom: &validFrom, PriceValidUntil: &validFrom,
				PriceOperation: GatewayImageOperationGeneration, PriceProtocol: GatewayImageProtocolImages,
				UsageSchema: GatewayPricingUsageSchemaOperationV1, UsageSchemaVersion: 1,
				UsageOperationPresent: true, OperationUnits: &operationUnits,
			}

			clearGatewayRequestPricingHardFence(&audit, reason)

			if audit.PricingUnavailableReason != reason {
				t.Fatalf("hard-fence reason = %q, want %q", audit.PricingUnavailableReason, reason)
			}
			if audit.PriceOperation != GatewayImageOperationGeneration || audit.PriceProtocol != GatewayImageProtocolImages ||
				audit.UsageSchema != GatewayPricingUsageSchemaOperationV1 || audit.UsageSchemaVersion != 1 ||
				!audit.UsageOperationPresent || audit.OperationUnits == nil || *audit.OperationUnits != 1 {
				t.Fatalf("hard fence lost or changed the image operation contract: %+v", audit)
			}
			assertGatewayRequestPricingHardFenceClear(t, audit)
		})
	}
}

func TestBeginRequestDispatchDoesNotAttributeLateWriterToReplacementGeneration(t *testing.T) {
	db, store, channel, group, key, audit := gatewayRequestPricingSelectionFixture(t)
	if err := db.Model(&GatewayUpstreamGroup{}).Where("id = ?", group.ID).
		Update("presence_generation", group.PresenceGeneration+1).Error; err != nil {
		t.Fatalf("recreate group before delayed audit insert: %v", err)
	}
	snapshot := GatewayRequestDispatchSnapshot{
		ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: "primary",
		UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
		ModelName: "model-exact", UpstreamModelName: "model-exact",
		Method: "POST", Path: "/v1/chat/completions", RequestStartedAt: time.Now().UTC(),
		GroupRatio: audit.GroupRatio, CompletionRatio: audit.CompletionRatio,
		RawGroupRatio: audit.RawGroupRatio, RawCompletionRatio: audit.RawCompletionRatio,
		FundingFactor: audit.FundingFactor, CostBasisVersion: audit.CostBasisVersion, CostBasisSource: audit.CostBasisSource,
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
		UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
	}
	snapshot = requestDispatchSnapshotWithCurrentTelemetry(t, snapshot, snapshot.RequestStartedAt)
	persisted, err := store.BeginRequestDispatch(snapshot, time.Now().UTC())
	if err != nil {
		t.Fatalf("late dispatch writer: %v", err)
	}
	if persisted.PricingUnavailableReason != GatewayPricingUnavailableIdentityChanged || persisted.PriceFactID != nil ||
		persisted.ChannelIdentityRevision != 0 || persisted.UpstreamGroupID != 0 || persisted.UpstreamGroupGeneration != 0 ||
		persisted.UpstreamKeyLocalID != 0 || persisted.UpstreamKeyGeneration != 0 {
		t.Fatalf("late dispatch was attributed to replacement generation: %+v", persisted)
	}
	if persisted.PriceOperation != GatewayPricingOperationTextGeneration ||
		persisted.PriceProtocol != GatewayPricingProtocolOpenAIChatCompletions ||
		persisted.UsageSchema != GatewayPricingUsageSchemaTokensV1 || persisted.UsageSchemaVersion != 1 {
		t.Fatalf("late dispatch lost its immutable request contract: %+v", persisted)
	}
	assertGatewayRequestPricingHardFenceClear(t, *persisted)
	projected := GatewayRequestLog{}
	applyGatewayRequestDispatchSnapshotToLog(persisted, &projected)
	if projected.GroupRatio != 0 || projected.CompletionRatio != 0 || projected.RawGroupRatio != nil ||
		projected.RawCompletionRatio != nil || projected.FundingFactor != nil || projected.CostBasisVersion != 0 || projected.CostBasisSource != "" {
		t.Fatalf("late dispatch projected stale multiplier metadata into request log: %+v", projected)
	}
}

func TestBeginRequestDispatchPersistsCompleteScopeWithoutPriceFact(t *testing.T) {
	_, store, channel, group, key, audit := gatewayRequestPricingSelectionFixture(t)
	const modelWithoutFact = "model-without-price-fact"
	snapshot := GatewayRequestDispatchSnapshot{
		ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: "primary",
		UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
		ModelName: modelWithoutFact, UpstreamModelName: modelWithoutFact,
		Method: "POST", Path: "/v1/chat/completions", RequestStartedAt: time.Now().UTC(),
		GroupRatio: audit.GroupRatio, CompletionRatio: audit.CompletionRatio,
		RawGroupRatio: audit.RawGroupRatio, RawCompletionRatio: audit.RawCompletionRatio,
		FundingFactor: audit.FundingFactor, CostBasisVersion: audit.CostBasisVersion, CostBasisSource: audit.CostBasisSource,
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
		UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
	}
	snapshot = requestDispatchSnapshotWithCurrentTelemetry(t, snapshot, snapshot.RequestStartedAt)
	persisted, err := store.BeginRequestDispatch(snapshot, time.Now().UTC())
	if err != nil {
		t.Fatalf("begin dispatch without a price fact: %v", err)
	}
	if persisted.PricingUnavailableReason != GatewayPricingUnavailableFactMissing || persisted.PriceFactID != nil ||
		persisted.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		persisted.UpstreamGroupID != group.ID || persisted.UpstreamGroupGeneration != group.PresenceGeneration ||
		persisted.UpstreamKeyLocalID != key.ID || persisted.UpstreamKeyGeneration != key.KeyGeneration ||
		persisted.PriceCanonicalGroupName != "primary" || persisted.PriceModelName != modelWithoutFact {
		t.Fatalf("complete fact-missing scope = %+v", persisted)
	}
	if persisted.PriceOperation != GatewayPricingOperationTextGeneration ||
		persisted.PriceProtocol != GatewayPricingProtocolOpenAIChatCompletions ||
		persisted.UsageSchema != GatewayPricingUsageSchemaTokensV1 || persisted.UsageSchemaVersion != 1 {
		t.Fatalf("fact-missing dispatch contract = %+v", persisted)
	}
	markedAt := time.Now().UTC()
	if err := store.MarkRequestDispatchPhysicallyDispatched(persisted.ID, markedAt); err != nil {
		t.Fatalf("mark fact-missing dispatch physical: %v", err)
	}
	if err := store.ConfirmRequestDispatchTransportInvocation(persisted.ID, markedAt.Add(time.Millisecond)); err != nil {
		t.Fatalf("confirm fact-missing transport invocation: %v", err)
	}
	log := &GatewayRequestLog{StatusCode: 200, CreatedAt: markedAt.Add(2 * time.Millisecond)}
	created, err := store.FinalizeRequestDispatchResult(persisted.ID, log, "response_recorded", log.CreatedAt)
	if err != nil || !created || log.ID == 0 {
		t.Fatalf("finalize fact-missing dispatch: created=%t log=%+v err=%v", created, log, err)
	}
	if log.PriceFactID != nil || log.PriceCanonicalGroupName != "primary" || log.PriceModelName != modelWithoutFact ||
		log.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		log.UpstreamGroupID != group.ID || log.UpstreamGroupGeneration != group.PresenceGeneration ||
		log.UpstreamKeyLocalID != key.ID || log.UpstreamKeyGeneration != key.KeyGeneration ||
		log.CostStatus != GatewayCostStatusUnavailable || log.CostUnavailableReason != GatewayPricingUnavailableFactMissing {
		t.Fatalf("finalized fact-missing pricing scope = %+v", log)
	}
}

func TestHardFencedImageDispatchRetainsAtomicBillingContractThroughFinalization(t *testing.T) {
	for _, test := range []struct {
		name     string
		path     string
		protocol string
	}{
		{name: "images endpoint", path: "/v1/images/generations", protocol: GatewayImageProtocolImages},
		{name: "responses image tool", path: "/v1/responses", protocol: GatewayImageProtocolResponses},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			store := NewGateway(db)
			now := time.Now().UTC()
			snapshot := requestDispatchImageTestSnapshot(t, now, 700)
			snapshot.Path = test.path
			snapshot.PriceProtocol = test.protocol

			audit, err := store.BeginRequestDispatch(snapshot, now)
			if err != nil {
				t.Fatalf("begin hard-fenced image dispatch: %v", err)
			}
			if audit.PricingUnavailableReason != GatewayPricingUnavailableScopeIncomplete || audit.PriceFactID != nil ||
				audit.ChannelIdentityRevision != 0 || audit.UpstreamGroupID != 0 || audit.UpstreamGroupGeneration != 0 ||
				audit.UpstreamKeyLocalID != 0 || audit.UpstreamKeyGeneration != 0 ||
				audit.PriceCanonicalGroupName != "" || audit.PriceModelName != "" ||
				audit.PriceOperation != GatewayImageOperationGeneration || audit.PriceProtocol != test.protocol ||
				audit.UsageSchema != GatewayPricingUsageSchemaOperationV1 || audit.UsageSchemaVersion != 1 ||
				!audit.UsageOperationPresent || audit.OperationUnits == nil || *audit.OperationUnits != 1 {
				t.Fatalf("hard-fenced image audit = %+v", audit)
			}
			assertGatewayRequestPricingHardFenceClear(t, *audit)
			if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(time.Millisecond)); !errors.Is(err, ErrGatewayRequestDispatchBillingCommitRequired) {
				t.Fatalf("plain image physical mark error = %v", err)
			}
			if err := store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(audit.ID, now.Add(2*time.Millisecond)); err != nil {
				t.Fatalf("paired image physical/billing mark: %v", err)
			}
			if err := store.ConfirmRequestDispatchTransportInvocation(audit.ID, now.Add(3*time.Millisecond)); err != nil {
				t.Fatalf("confirm image transport invocation: %v", err)
			}
			log := &GatewayRequestLog{StatusCode: 502, Error: "upstream failed", CreatedAt: now.Add(4 * time.Millisecond)}
			created, err := store.FinalizeRequestDispatchResult(audit.ID, log, "transport_error", log.CreatedAt)
			if err != nil || !created || log.ID == 0 {
				t.Fatalf("finalize hard-fenced image dispatch: created=%t log=%+v err=%v", created, log, err)
			}
			if !log.BillingCommitted || log.CostStatus != GatewayCostStatusUnavailable ||
				log.CostUnavailableReason != GatewayPricingUnavailableScopeIncomplete || log.Cost != 0 || log.CostSource != "" ||
				log.PriceOperation != GatewayImageOperationGeneration || log.PriceProtocol != test.protocol ||
				log.UsageSchema != GatewayPricingUsageSchemaOperationV1 || log.UsageSchemaVersion != 1 ||
				!log.UsageOperationPresent || log.OperationUnits == nil || *log.OperationUnits != 1 {
				t.Fatalf("finalized hard-fenced image log = %+v", log)
			}
		})
	}
}

func assertGatewayRequestPricingHardFenceClear(t *testing.T, audit GatewayRequestDispatchAudit) {
	t.Helper()
	if audit.GroupRatio != 0 || audit.CompletionRatio != 0 || audit.RawGroupRatio != nil ||
		audit.RawCompletionRatio != nil || audit.FundingFactor != nil || audit.CostBasisVersion != 0 || audit.CostBasisSource != "" {
		t.Fatalf("pricing hard fence retained mutable multiplier metadata: %+v", audit)
	}
	assertGatewayRequestPricingIdentityClear(t, audit)
}

func assertGatewayRequestPricingIdentityClear(t *testing.T, audit GatewayRequestDispatchAudit) {
	t.Helper()
	if audit.ChannelIdentityRevision != 0 || audit.UpstreamGroupID != 0 || audit.UpstreamGroupGeneration != 0 ||
		audit.UpstreamKeyLocalID != 0 || audit.UpstreamKeyGeneration != 0 || audit.PriceFactID != nil ||
		audit.PriceFactVersion != 0 || audit.PriceFactSchemaVersion != 0 || audit.PriceFactChecksum != "" ||
		audit.PriceCanonicalGroupName != "" || audit.PriceModelName != "" || audit.PriceInputUnit != nil ||
		audit.PriceOutputUnit != nil || audit.PriceCacheReadUnit != nil || audit.PriceCacheWriteUnit != nil ||
		audit.PriceOperationUnit != nil || audit.PriceCurrency != "" || audit.PriceUnit != "" ||
		audit.PriceSource != "" || audit.PriceValidFrom != nil || audit.PriceValidUntil != nil {
		t.Fatalf("pricing hard fence retained selection or price-fact provenance: %+v", audit)
	}
}

func gatewayRequestPricingSelectionFixture(t *testing.T) (*gorm.DB, *Gateway, *Channel, UpstreamGroupGeneration, GatewayUpstreamKey, GatewayRequestDispatchAudit) {
	t.Helper()
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: "dispatch-pricing-selection",
		Type: ChannelTypeSub2API, SiteURL: "https://dispatch-pricing-selection.example",
		PasswordCipher: "cipher", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	identityObservedAt := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := channels.ObserveUpstreamAccountIdentity(UpstreamAccountIdentityObservation{
		ChannelID: channel.ID, ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channel.AdminRevision, Provider: channel.Type, APIOrigin: channel.SiteURL,
		AccountID: "dispatch-pricing-selection-account", Scope: UpstreamConcurrencyScopeAccount,
		Source: "pricing-test.authenticated-account", ObservedAt: identityObservedAt,
	}); err != nil {
		t.Fatalf("bind pricing account identity: %v", err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatalf("create group rate: %v", err)
	}
	group, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture group generation: %v", err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 91, UpstreamKeyName: "bridgeops-primary",
		APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", Enabled: true,
	}); err != nil {
		t.Fatalf("create upstream key: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil {
		t.Fatalf("load upstream key: %v", err)
	}
	fact := gatewayRequestPricingTestFact()
	fact.ID = 0
	fact.ChannelID = channel.ID
	fact.ChannelAccountIdentityRevision = channel.UpstreamAccountIdentityRevision
	fact.UpstreamGroupID = group.ID
	fact.UpstreamGroupPresenceGeneration = group.PresenceGeneration
	fact.GatewayUpstreamKeyID = key.ID
	fact.UpstreamKeyGeneration = key.KeyGeneration
	fact.FactVersion = 0
	fact.SchemaVersion = 0
	fact.Checksum = ""
	fact.ObservedAt = identityObservedAt.Add(time.Minute)
	fact.ValidFrom = time.Now().UTC().Add(-time.Minute)
	if err := store.AppendRequestPriceFact(&fact); err != nil {
		t.Fatalf("append current price fact: %v", err)
	}
	rawGroupRatio, rawCompletionRatio, fundingFactor := 2.5, 3.5, 0.5
	audit := GatewayRequestDispatchAudit{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
		ModelName: "model-exact", UpstreamModelName: "model-exact", Method: "POST", Path: "/v1/chat/completions",
		GroupRatio: 1, CompletionRatio: 1, RawGroupRatio: &rawGroupRatio, RawCompletionRatio: &rawCompletionRatio,
		FundingFactor: &fundingFactor, CostBasisVersion: 7, CostBasisSource: "recharge_history",
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
		UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
	}
	return db, store, channel, group, *key, audit
}
