package storage

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestGatewayRequestPriceFactSnapshotsCompareNullableFieldsAndEpoch(t *testing.T) {
	fact := gatewayRequestPricingTestFact()
	fact.ID = 7
	fact.CostUnitEpoch = 3
	fact.FactVersion = 4
	fact.SchemaVersion = GatewayRequestPriceFactSchemaVersion
	fact.Checksum = strings.Repeat("a", 64)
	base := gatewayRequestPriceFactSnapshotFromFact(&fact)

	for _, test := range []struct {
		name   string
		mutate func(*gatewayRequestPriceFactSnapshot)
	}{
		{name: "cost unit epoch", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) { snapshot.CostUnitEpoch++ }},
		{name: "canonical group", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) { snapshot.CanonicalGroupName = "other" }},
		{name: "nullable operation price", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) { value := 0.0; snapshot.OperationUnitPrice = &value }},
		{name: "observation time", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := snapshot.ObservedAt.Add(time.Second)
			snapshot.ObservedAt = &value
		}},
		{name: "validity", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := snapshot.ValidFrom.Add(time.Second)
			snapshot.ValidFrom = &value
		}},
		{name: "checksum", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) { snapshot.Checksum = strings.Repeat("b", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.mutate(&changed)
			if gatewayRequestPriceFactSnapshotsEqual(base, changed) {
				t.Fatalf("snapshot mutation %q was not detected", test.name)
			}
		})
	}
}

func TestGatewayRequestTieredPriceFactSnapshotsCompareLongTierAndRatios(t *testing.T) {
	fact := gatewayRequestTieredPricingTestFact()
	fact.ID = 8
	fact.FactVersion = 5
	fact.Checksum = strings.Repeat("c", 64)
	base := gatewayRequestPriceFactSnapshotFromFact(&fact)

	for _, test := range []struct {
		name   string
		mutate func(*gatewayRequestPriceFactSnapshot)
	}{
		{name: "threshold", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := *snapshot.LongContextThresholdTokens + 1
			snapshot.LongContextThresholdTokens = &value
		}},
		{name: "long input", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := *snapshot.LongContextInputUnitPrice + 1
			snapshot.LongContextInputUnitPrice = &value
		}},
		{name: "long output", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := *snapshot.LongContextOutputUnitPrice + 1
			snapshot.LongContextOutputUnitPrice = &value
		}},
		{name: "long cache read", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := *snapshot.LongContextCacheReadUnitPrice + 1
			snapshot.LongContextCacheReadUnitPrice = &value
		}},
		{name: "long cache write", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := *snapshot.LongContextCacheWriteUnitPrice + 1
			snapshot.LongContextCacheWriteUnitPrice = &value
		}},
		{name: "group ratio", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := *snapshot.GroupRatio + 1
			snapshot.GroupRatio = &value
		}},
		{name: "completion ratio", mutate: func(snapshot *gatewayRequestPriceFactSnapshot) {
			value := *snapshot.CompletionRatio + 1
			snapshot.CompletionRatio = &value
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			test.mutate(&changed)
			if gatewayRequestPriceFactSnapshotsEqual(base, changed) {
				t.Fatalf("tiered snapshot mutation %q was not detected", test.name)
			}
		})
	}
}

func TestAppendRequestLogRejectsMismatchedPriceFactSnapshotBeforeInsert(t *testing.T) {
	db, store, fact := gatewayRequestPriceSnapshotValidationFixture(t, "price-snapshot-log-mismatch")
	log := gatewayRequestPriceSnapshotValidationLog(fact)
	mutated := *log.PriceInputUnit + 0.5
	log.PriceInputUnit = &mutated

	err := store.AppendRequestLog(log)
	if !errors.Is(err, ErrGatewayRequestPriceSnapshotIntegrity) {
		t.Fatalf("append mismatched price snapshot error = %v", err)
	}
	var count int64
	if countErr := db.Model(&GatewayRequestLog{}).Where("downstream_key_id = ?", log.DownstreamKeyID).Count(&count).Error; countErr != nil || count != 0 {
		t.Fatalf("mismatched price snapshot rows = %d, err=%v", count, countErr)
	}
}

func TestAppendRequestLogRejectsReferencedFactWithInvalidChecksum(t *testing.T) {
	db, store, fact := gatewayRequestPriceSnapshotValidationFixture(t, "price-snapshot-fact-checksum")
	badFact := *fact
	badFact.ID = 0
	badFact.FactVersion++
	badFact.ValidFrom = normalizeGatewayRequestPricingTime(time.Now().UTC())
	badFact.Checksum = strings.Repeat("b", 64)
	badFact.CreatedAt = badFact.ValidFrom
	if err := db.Create(&badFact).Error; err != nil {
		t.Fatalf("insert legacy fact with invalid checksum: %v", err)
	}
	log := gatewayRequestPriceSnapshotValidationLog(&badFact)

	err := store.AppendRequestLog(log)
	if !errors.Is(err, ErrGatewayRequestPriceSnapshotIntegrity) || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("append invalid fact checksum error = %v", err)
	}
	var count int64
	if countErr := db.Model(&GatewayRequestLog{}).Where("downstream_key_id = ?", log.DownstreamKeyID).Count(&count).Error; countErr != nil || count != 0 {
		t.Fatalf("invalid fact checksum rows = %d, err=%v", count, countErr)
	}
}

func TestFactPricedRequestLogRejectsLegacyCostBasisRestatement(t *testing.T) {
	db, store, fact := gatewayRequestPriceSnapshotValidationFixture(t, "price-snapshot-basis-restatement")
	log := gatewayRequestPriceSnapshotValidationLog(fact)
	log.GroupRatio = 1
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append fact-priced request log: %v", err)
	}
	before := *log
	err := db.Model(&GatewayRequestLog{}).Where("id = ?", log.ID).Updates(map[string]any{
		"cost": 99.0, "group_ratio": 2.0, "funding_factor": 2.0,
	}).Error
	if err == nil || !strings.Contains(err.Error(), "price snapshot is immutable") {
		t.Fatalf("fact-priced legacy restatement error = %v", err)
	}
	var after GatewayRequestLog
	if err := db.First(&after, log.ID).Error; err != nil {
		t.Fatalf("reload fact-priced request log: %v", err)
	}
	if after.Cost != before.Cost || after.GroupRatio != before.GroupRatio || !equalGatewayRequestPriceFloat(after.FundingFactor, before.FundingFactor) {
		t.Fatalf("fact-priced request changed after rejected restatement: before=%+v after=%+v", before, after)
	}
}

func gatewayRequestPriceSnapshotValidationFixture(t *testing.T, name string) (*gorm.DB, *Gateway, *GatewayRequestPriceFact) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, name)
	fact := gatewayRequestPricingTestFact()
	fact.ID = 0
	fact.ChannelID = channel.ID
	fact.ChannelAccountIdentityRevision = channel.UpstreamAccountIdentityRevision
	fact.UpstreamGroupID = group.ID
	fact.CanonicalGroupName = group.CanonicalGroupName
	fact.UpstreamGroupPresenceGeneration = group.PresenceGeneration
	fact.GatewayUpstreamKeyID = key.ID
	fact.UpstreamKeyGeneration = key.KeyGeneration
	fact.FactVersion = 0
	fact.SchemaVersion = 0
	fact.Checksum = ""
	fact.ValidFrom = time.Now().UTC().Add(-time.Minute)
	fact.ObservedAt = fact.ValidFrom
	if err := store.AppendRequestPriceFact(&fact); err != nil {
		t.Fatalf("append validation price fact: %v", err)
	}
	return db, store, &fact
}

func gatewayRequestPriceSnapshotValidationLog(fact *GatewayRequestPriceFact) *GatewayRequestLog {
	input, output, cacheRead, cacheWrite := int64(10), int64(2), int64(0), int64(0)
	log := &GatewayRequestLog{
		DownstreamKeyID:  9901,
		ChannelID:        fact.ChannelID,
		GroupName:        fact.CanonicalGroupName,
		ModelName:        fact.ModelName,
		Method:           "POST",
		Path:             "/v1/responses",
		StatusCode:       200,
		LatencyMs:        1,
		CreatedAt:        time.Now().UTC(),
		PromptTokens:     input + cacheRead + cacheWrite,
		CompletionTokens: output,
	}
	log.ChannelIdentityRevision = fact.ChannelAccountIdentityRevision
	log.UpstreamGroupID = fact.UpstreamGroupID
	log.UpstreamGroupGeneration = fact.UpstreamGroupPresenceGeneration
	log.UpstreamKeyLocalID = fact.GatewayUpstreamKeyID
	log.UpstreamKeyGeneration = fact.UpstreamKeyGeneration
	log.PriceFactID = &fact.ID
	log.PriceCostUnitEpoch = fact.CostUnitEpoch
	log.PriceFactVersion = fact.FactVersion
	log.PriceFactSchemaVersion = fact.SchemaVersion
	log.PriceFactChecksum = fact.Checksum
	log.PriceCanonicalGroupName = fact.CanonicalGroupName
	log.PriceModelName = fact.ModelName
	log.PriceOperation = fact.Operation
	log.PriceProtocol = fact.Protocol
	log.PriceInputUnit = fact.InputUnitPrice
	log.PriceOutputUnit = fact.OutputUnitPrice
	log.PriceCacheReadUnit = fact.CacheReadUnitPrice
	log.PriceCacheWriteUnit = fact.CacheWriteUnitPrice
	log.PriceLongContextThresholdTokens = fact.LongContextThresholdTokens
	log.PriceLongContextInputUnit = fact.LongContextInputUnitPrice
	log.PriceLongContextOutputUnit = fact.LongContextOutputUnitPrice
	log.PriceLongContextCacheReadUnit = fact.LongContextCacheReadUnitPrice
	log.PriceLongContextCacheWriteUnit = fact.LongContextCacheWriteUnitPrice
	if fact.SchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
		log.GroupRatio = *fact.GroupRatio
		log.CompletionRatio = *fact.CompletionRatio
	}
	log.PriceOperationUnit = fact.OperationUnitPrice
	log.PriceCurrency = fact.Currency
	log.PriceUnit = fact.Unit
	log.PriceSource = fact.Source
	observedAt := fact.ObservedAt
	log.PriceObservedAt = &observedAt
	validFrom := fact.ValidFrom
	log.PriceValidFrom = &validFrom
	log.PriceValidUntil = fact.ValidUntil
	log.UsageSchema = GatewayPricingUsageSchemaTokensV1
	log.UsageSchemaVersion = 1
	log.UsageInputPresent = true
	log.UsageOutputPresent = true
	log.UsageCacheReadPresent = true
	log.UsageCacheWritePresent = true
	estimate := EvaluateGatewayRequestPriceFact(fact, GatewayRequestPricingUsage{
		InputTokens: &input, OutputTokens: &output,
		CacheReadTokens: &cacheRead, CacheWriteTokens: &cacheWrite,
	})
	log.CostStatus = estimate.Status
	if estimate.Cost != nil {
		log.Cost = *estimate.Cost
		log.CostSource = "price_fact"
	}
	return log
}
