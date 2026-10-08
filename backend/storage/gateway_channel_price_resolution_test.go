package storage

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestFreezeGatewayRequestPricingPrefersChannelPriceOverGlobalModelPrice(t *testing.T) {
	db, store, channel, audit := gatewayChannelPriceResolutionFixture(t, "channel-priority-model")
	rawGroupRatio, rawCompletionRatio, fundingFactor := 2.0, 7.0, 0.25
	audit.RawGroupRatio = &rawGroupRatio
	audit.RawCompletionRatio = &rawCompletionRatio
	audit.GroupRatio = 0.5
	audit.CompletionRatio = 1.75
	audit.FundingFactor = &fundingFactor
	if err := db.Model(&RateSnapshot{}).
		Where("channel_id = ? AND model_name = ?", channel.ID, "primary").
		Updates(map[string]any{"ratio": rawGroupRatio, "completion_ratio": rawCompletionRatio}).Error; err != nil {
		t.Fatalf("align primary group rate for channel price: %v", err)
	}
	createGatewayChannelResolutionGlobalPrice(t, store, audit.ModelName, 90, 100, 9, 10)
	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, time.Now().UTC(), "a",
		gatewayChannelPriceResolutionItemWithRatio("primary", audit.ModelName, rawGroupRatio, 3, 7, gatewayChannelPriceResolutionFloat(0.3), gatewayChannelPriceResolutionFloat(0.7)),
	)

	frozen := freezeGatewayModelPriceForTest(t, db, audit, time.Now().UTC())
	assertGatewayChannelResolutionFlatFact(t, frozen, gatewayChannelModelPriceSource, 1.5, 3.5, 0.15, 0.35)
}

func TestFreezeGatewayRequestPricingKeepsAdministratorPriceAboveChannelPrice(t *testing.T) {
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "channel-price-admin-priority")
	now := time.Now().UTC()
	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, now, "4",
		gatewayChannelPriceResolutionItem("primary", "model-a", 3, 7, gatewayChannelPriceResolutionFloat(0.3), gatewayChannelPriceResolutionFloat(0.7)),
	)
	input, output, cacheRead, cacheWrite := 70.0, 80.0, 7.0, 8.0
	validUntil := now.Add(time.Hour)
	if _, err := store.ImportAdministratorRequestPriceFacts(
		context.Background(),
		"channel-price-resolution-admin",
		"channel-price-resolution-admin-priority",
		[]GatewayAdministratorPriceObservation{
			gatewayAdministratorPriceObservationForTest(channel, group, key, now.Add(-time.Minute), GatewayAdministratorPriceObservation{
				ChannelID: channel.ID, GroupName: "primary", ModelName: "model-a",
				Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
				InputUnitPriceUSD: &input, OutputUnitPriceUSD: &output,
				CacheReadUnitPriceUSD: &cacheRead, CacheWriteUnitPriceUSD: &cacheWrite,
				ValidUntil: validUntil,
			}),
		},
	); err != nil {
		t.Fatalf("import administrator price above channel catalog: %v", err)
	}
	rawGroupRatio, rawCompletionRatio, fundingFactor := 1.0, 1.0, 1.0
	audit := GatewayRequestDispatchAudit{
		ChannelID: channel.ID, GroupName: "primary",
		UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
		ModelName: "model-a", UpstreamModelName: "model-a", Method: "POST", Path: "/v1/responses",
		GroupRatio: 1, CompletionRatio: 1, RawGroupRatio: &rawGroupRatio, RawCompletionRatio: &rawCompletionRatio,
		FundingFactor: &fundingFactor, CostBasisVersion: 1, CostBasisSource: "channel-price-resolution-test",
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
		UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
		PriceOperation: GatewayPricingOperationTextGeneration, PriceProtocol: GatewayPricingProtocolOpenAIResponses,
		UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageSchemaVersion: 1,
	}

	frozen := freezeGatewayModelPriceForTest(t, db, audit, time.Now().UTC())
	assertGatewayChannelResolutionFlatFact(t, frozen, GatewayRequestPriceSourceAdministrator, input, output, cacheRead, cacheWrite)
}

func TestFreezeGatewayRequestPricingSelectsChannelPriceByCanonicalGroup(t *testing.T) {
	db, store, channel, primary := gatewayChannelPriceResolutionFixture(t, "channel-group-model")
	rates := NewRates(db)
	now := time.Now().UTC()
	if _, err := rates.Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "secondary", Ratio: 1, CompletionRatio: 1, LastSeenAt: now,
	}); err != nil {
		t.Fatalf("create secondary group rate: %v", err)
	}
	secondaryGroup, err := store.CaptureUpstreamGroupGeneration(channel.ID, "secondary")
	if err != nil {
		t.Fatalf("capture secondary group generation: %v", err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "secondary", UpstreamKeyID: 92,
		UpstreamKeyName: "bridgeops-secondary", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", Enabled: true,
	}); err != nil {
		t.Fatalf("create secondary upstream key: %v", err)
	}
	secondaryKey, err := store.FindUpstreamKeyAny(channel.ID, "secondary")
	if err != nil {
		t.Fatalf("load secondary upstream key: %v", err)
	}
	secondary := primary
	secondary.GroupName = "secondary"
	secondary.UpstreamKeyID = secondaryKey.UpstreamKeyID
	secondary.UpstreamKeyName = secondaryKey.UpstreamKeyName
	secondary.UpstreamGroupID = secondaryGroup.ID
	secondary.UpstreamGroupGeneration = secondaryGroup.PresenceGeneration
	secondary.UpstreamKeyLocalID = secondaryKey.ID
	secondary.UpstreamKeyGeneration = secondaryKey.KeyGeneration

	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, time.Now().UTC(), "b",
		gatewayChannelPriceResolutionItem("primary", primary.ModelName, 2, 3, gatewayChannelPriceResolutionFloat(0.2), gatewayChannelPriceResolutionFloat(0.3)),
		gatewayChannelPriceResolutionItem("secondary", primary.ModelName, 20, 30, gatewayChannelPriceResolutionFloat(2), gatewayChannelPriceResolutionFloat(3)),
	)

	freezeAt := time.Now().UTC()
	primaryFrozen := freezeGatewayModelPriceForTest(t, db, primary, freezeAt)
	secondaryFrozen := freezeGatewayModelPriceForTest(t, db, secondary, freezeAt)
	assertGatewayChannelResolutionFlatFact(t, primaryFrozen, gatewayChannelModelPriceSource, 2, 3, 0.2, 0.3)
	assertGatewayChannelResolutionFlatFact(t, secondaryFrozen, gatewayChannelModelPriceSource, 20, 30, 2, 3)
}

func TestFreezeGatewayRequestPricingAppendsFactAfterChannelSnapshotPriceChange(t *testing.T) {
	db, _, channel, audit := gatewayChannelPriceResolutionFixture(t, "channel-versioned-model")
	now := time.Now().UTC()
	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, now, "c",
		gatewayChannelPriceResolutionItem("primary", audit.ModelName, 4, 8, gatewayChannelPriceResolutionFloat(0.4), gatewayChannelPriceResolutionFloat(0.8)),
	)
	first := freezeGatewayModelPriceForTest(t, db, audit, now)
	assertGatewayChannelResolutionFlatFact(t, first, gatewayChannelModelPriceSource, 4, 8, 0.4, 0.8)

	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, now.Add(time.Second), "d",
		gatewayChannelPriceResolutionItem("primary", audit.ModelName, 5, 9, gatewayChannelPriceResolutionFloat(0.5), gatewayChannelPriceResolutionFloat(0.9)),
	)
	second := freezeGatewayModelPriceForTest(t, db, audit, now.Add(time.Second))
	assertGatewayChannelResolutionFlatFact(t, second, gatewayChannelModelPriceSource, 5, 9, 0.5, 0.9)
	if first.PriceFactID == nil || second.PriceFactID == nil || *first.PriceFactID == *second.PriceFactID ||
		first.PriceFactVersion != 1 || second.PriceFactVersion != 2 {
		t.Fatalf("changed channel price did not append a new fact: first=%+v second=%+v", first, second)
	}
	var count int64
	if err := db.Model(&GatewayRequestPriceFact{}).
		Where("channel_id = ? AND model_name = ? AND source = ?", channel.ID, audit.ModelName, gatewayChannelModelPriceSource).
		Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("channel price fact count = %d, err=%v", count, err)
	}
}

func TestFreezeGatewayRequestPricingDoesNotReuseChannelPriceAcrossGroupGeneration(t *testing.T) {
	db, store, channel, audit := gatewayChannelPriceResolutionFixture(t, "channel-generation-model")
	createGatewayChannelResolutionGlobalPrice(t, store, audit.ModelName, 90, 100, 9, 10)
	observedAt := time.Now().UTC()
	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, observedAt, "6",
		gatewayChannelPriceResolutionItem("primary", audit.ModelName, 3, 7, gatewayChannelPriceResolutionFloat(0.3), gatewayChannelPriceResolutionFloat(0.7)),
	)

	first := freezeGatewayModelPriceForTest(t, db, audit, observedAt)
	assertGatewayChannelResolutionFlatFact(t, first, gatewayChannelModelPriceSource, 3, 7, 0.3, 0.7)

	var group GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "primary").Take(&group).Error; err != nil {
		t.Fatalf("load channel price group identity: %v", err)
	}
	var stored GatewayChannelModelPrice
	if err := db.Where("channel_id = ? AND upstream_group_name = ? AND model_name = ?", channel.ID, "primary", audit.ModelName).Take(&stored).Error; err != nil {
		t.Fatalf("load channel price generation fence: %v", err)
	}
	if stored.GatewayUpstreamGroupID != group.ID || stored.UpstreamGroupPresenceGeneration != group.PresenceGeneration {
		t.Fatalf("channel price generation fence = %+v, group=%+v", stored, group)
	}

	newGeneration := group.PresenceGeneration + 1
	if err := db.Model(&GatewayUpstreamGroup{}).Where("id = ?", group.ID).
		Update("presence_generation", newGeneration).Error; err != nil {
		t.Fatalf("advance same-name group generation: %v", err)
	}
	audit.UpstreamGroupGeneration = newGeneration
	second := freezeGatewayModelPriceForTest(t, db, audit, observedAt.Add(time.Second))
	assertGatewayChannelResolutionFlatFact(t, second, gatewayRequestPriceSourceModelConfig, 90, 100, 9, 10)
}

func TestFreezeGatewayRequestPricingPreservesUnknownFlatCachePrice(t *testing.T) {
	db, store, channel, audit := gatewayChannelPriceResolutionFixture(t, "channel-cache-unknown-model")
	createGatewayChannelResolutionGlobalPrice(t, store, audit.ModelName, 90, 100, 9, 10)
	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, time.Now().UTC(), "e",
		gatewayChannelPriceResolutionItem("primary", audit.ModelName, 3, 7, nil, nil),
	)

	frozen := freezeGatewayModelPriceForTest(t, db, audit, time.Now().UTC())
	if frozen.PriceFactID == nil || frozen.PriceSource != gatewayChannelModelPriceSource ||
		frozen.PriceCacheReadUnit != nil || frozen.PriceCacheWriteUnit != nil {
		t.Fatalf("unknown channel cache prices were not frozen as unknown: %+v", frozen)
	}
	fact := gatewayChannelPriceResolutionFact(t, db, frozen)
	zero, one := int64(0), int64(1)
	usage := GatewayRequestPricingUsage{
		InputTokens: &one, OutputTokens: &zero, CacheReadTokens: &zero, CacheWriteTokens: &zero,
	}
	withoutCacheUsage := EvaluateGatewayRequestPriceFact(&fact, usage)
	if withoutCacheUsage.Status != GatewayCostStatusEstimated || withoutCacheUsage.Cost == nil ||
		*withoutCacheUsage.Cost != 3.0/1_000_000 || withoutCacheUsage.UnavailableReason != "" {
		t.Fatalf("zero cache usage with unknown cache price = %+v", withoutCacheUsage)
	}
	usage.CacheReadTokens = &one
	withCacheUsage := EvaluateGatewayRequestPriceFact(&fact, usage)
	if withCacheUsage.Status != GatewayCostStatusUnavailable || withCacheUsage.Cost != nil ||
		withCacheUsage.UnavailableReason != GatewayPricingUnavailablePriceMissing {
		t.Fatalf("positive cache usage with unknown cache price = %+v", withCacheUsage)
	}
}

func TestFreezeGatewayRequestPricingPreservesCompleteProviderWholeRequestThreshold(t *testing.T) {
	threshold := GatewayRequestPriceLongContextThreshold
	providerThreshold := int64(128_000)
	baseCacheRead, baseCacheWrite := 0.5, 6.25
	longInput, longOutput, longCacheRead, longCacheWrite := 10.0, 45.0, 1.0, 12.5
	tests := []struct {
		name           string
		checksum       string
		threshold      int64
		baseCacheRead  *float64
		baseCacheWrite *float64
		longCacheRead  *float64
		longCacheWrite *float64
		wantSource     string
		wantSchema     int
	}{
		{
			name: "complete 272k whole request", checksum: "f", threshold: threshold,
			baseCacheRead: &baseCacheRead, baseCacheWrite: &baseCacheWrite,
			longCacheRead: &longCacheRead, longCacheWrite: &longCacheWrite,
			wantSource: gatewayChannelModelPriceSource, wantSchema: GatewayRequestPriceFactTieredSchemaVersion,
		},
		{
			name: "complete provider 128k whole request", checksum: "1", threshold: providerThreshold,
			baseCacheRead: &baseCacheRead, baseCacheWrite: &baseCacheWrite,
			longCacheRead: &longCacheRead, longCacheWrite: &longCacheWrite,
			wantSource: gatewayChannelModelPriceSource, wantSchema: GatewayRequestPriceFactTieredSchemaVersion,
		},
		{
			name: "missing base cache price", checksum: "2", threshold: threshold,
			baseCacheRead: nil, baseCacheWrite: &baseCacheWrite,
			longCacheRead: &longCacheRead, longCacheWrite: &longCacheWrite,
			wantSource: gatewayChannelModelPriceSource, wantSchema: GatewayRequestPriceFactTieredSchemaVersion,
		},
		{
			name: "missing long cache price", checksum: "3", threshold: threshold,
			baseCacheRead: &baseCacheRead, baseCacheWrite: &baseCacheWrite,
			longCacheRead: nil, longCacheWrite: &longCacheWrite,
			wantSource: gatewayChannelModelPriceSource, wantSchema: GatewayRequestPriceFactTieredSchemaVersion,
		},
		{
			name: "missing base cache write price", checksum: "4", threshold: threshold,
			baseCacheRead: &baseCacheRead, baseCacheWrite: nil,
			longCacheRead: &longCacheRead, longCacheWrite: &longCacheWrite,
			wantSource: gatewayChannelModelPriceSource, wantSchema: GatewayRequestPriceFactTieredSchemaVersion,
		},
		{
			name: "missing long cache write price", checksum: "5", threshold: threshold,
			baseCacheRead: &baseCacheRead, baseCacheWrite: &baseCacheWrite,
			longCacheRead: &longCacheRead, longCacheWrite: nil,
			wantSource: gatewayChannelModelPriceSource, wantSchema: GatewayRequestPriceFactTieredSchemaVersion,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, store, channel, audit := gatewayChannelPriceResolutionFixture(t, "channel-tiered-model")
			createGatewayChannelResolutionGlobalPrice(t, store, audit.ModelName, 90, 100, 9, 10)
			item := gatewayChannelPriceResolutionItem(
				"primary", audit.ModelName, 5, 30, test.baseCacheRead, test.baseCacheWrite,
			)
			item.LongContextBasis = "whole_request"
			item.LongContextThresholdTokens = &test.threshold
			item.LongContextInputUnitPriceUSD = &longInput
			item.LongContextOutputUnitPriceUSD = &longOutput
			item.LongContextCacheReadUnitPriceUSD = test.longCacheRead
			item.LongContextCacheWriteUnitPriceUSD = test.longCacheWrite
			if test.wantSchema == GatewayRequestPriceFactTieredSchemaVersion {
				rawGroupRatio, rawCompletionRatio, fundingFactor := 2.0, 7.0, 0.25
				audit.RawGroupRatio = &rawGroupRatio
				audit.RawCompletionRatio = &rawCompletionRatio
				audit.GroupRatio = 0.5
				audit.CompletionRatio = 1.75
				audit.FundingFactor = &fundingFactor
				item.GroupRatio = &rawGroupRatio
				if err := db.Model(&RateSnapshot{}).
					Where("channel_id = ? AND model_name = ?", channel.ID, "primary").
					Updates(map[string]any{"ratio": rawGroupRatio, "completion_ratio": rawCompletionRatio}).Error; err != nil {
					t.Fatalf("align tiered group rate for channel price: %v", err)
				}
			}
			replaceGatewayChannelPriceResolutionSnapshot(
				t, db, channel, time.Now().UTC(), test.checksum, item,
			)

			frozen := freezeGatewayModelPriceForTest(t, db, audit, time.Now().UTC())
			if frozen.PriceFactID == nil || frozen.PriceSource != test.wantSource ||
				frozen.PriceFactSchemaVersion != test.wantSchema {
				t.Fatalf("whole-request channel price selection = %+v", frozen)
			}
			if test.wantSchema == GatewayRequestPriceFactTieredSchemaVersion {
				if frozen.PriceLongContextThresholdTokens == nil || *frozen.PriceLongContextThresholdTokens != test.threshold ||
					frozen.PriceInputUnit == nil || *frozen.PriceInputUnit != 5 ||
					frozen.PriceOutputUnit == nil || *frozen.PriceOutputUnit != 30 ||
					frozen.PriceLongContextInputUnit == nil || *frozen.PriceLongContextInputUnit != longInput ||
					frozen.PriceLongContextOutputUnit == nil || *frozen.PriceLongContextOutputUnit != longOutput {
					t.Fatalf("complete whole-request tier was not frozen: %+v", frozen)
				}
				assertOptionalPrice := func(field string, got, want *float64) {
					t.Helper()
					if got == nil || want == nil {
						if got != nil || want != nil {
							t.Fatalf("%s = %v, want %v", field, got, want)
						}
						return
					}
					if *got != *want {
						t.Fatalf("%s = %v, want %v", field, *got, *want)
					}
				}
				assertOptionalPrice("base cache read unit", frozen.PriceCacheReadUnit, test.baseCacheRead)
				assertOptionalPrice("base cache write unit", frozen.PriceCacheWriteUnit, test.baseCacheWrite)
				assertOptionalPrice("long cache read unit", frozen.PriceLongContextCacheReadUnit, test.longCacheRead)
				assertOptionalPrice("long cache write unit", frozen.PriceLongContextCacheWriteUnit, test.longCacheWrite)
				fact := gatewayChannelPriceResolutionFact(t, db, frozen)
				if fact.GroupRatio == nil || *fact.GroupRatio != 0.5 ||
					fact.CompletionRatio == nil || *fact.CompletionRatio != 0.5 {
					t.Fatalf("complete whole-request tier froze wrong ratios: %+v", fact)
				}
				zero := int64(0)
				atThreshold := test.threshold
				baseEstimate := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{
					InputTokens: &atThreshold, OutputTokens: &zero, CacheReadTokens: &zero, CacheWriteTokens: &zero,
				})
				wantBase := float64(atThreshold) * 5 * 0.5 / 1_000_000
				if baseEstimate.Status != GatewayCostStatusEstimated || baseEstimate.UnavailableReason != "" ||
					baseEstimate.Cost == nil || math.Abs(*baseEstimate.Cost-wantBase) > 1e-12 {
					t.Fatalf("whole-request cost at threshold = %+v, want %.12f", baseEstimate, wantBase)
				}
				aboveThreshold := test.threshold + 1
				longEstimate := EvaluateGatewayRequestPriceFact(&fact, GatewayRequestPricingUsage{
					InputTokens: &aboveThreshold, OutputTokens: &zero, CacheReadTokens: &zero, CacheWriteTokens: &zero,
				})
				wantLong := float64(aboveThreshold) * longInput * 0.5 / 1_000_000
				if longEstimate.Status != GatewayCostStatusEstimated || longEstimate.UnavailableReason != "" ||
					longEstimate.Cost == nil || math.Abs(*longEstimate.Cost-wantLong) > 1e-12 {
					t.Fatalf("whole-request cost above threshold = %+v, want %.12f", longEstimate, wantLong)
				}

				one := int64(1)
				missingCacheUsage := GatewayRequestPricingUsage{
					InputTokens: &zero, OutputTokens: &zero, CacheReadTokens: &zero, CacheWriteTokens: &zero,
				}
				switch {
				case test.baseCacheRead == nil:
					missingCacheUsage.CacheReadTokens = &one
				case test.baseCacheWrite == nil:
					missingCacheUsage.CacheWriteTokens = &one
				case test.longCacheRead == nil:
					missingCacheUsage.InputTokens = &aboveThreshold
					missingCacheUsage.CacheReadTokens = &one
				case test.longCacheWrite == nil:
					missingCacheUsage.InputTokens = &aboveThreshold
					missingCacheUsage.CacheWriteTokens = &one
				default:
					return
				}
				missingCacheEstimate := EvaluateGatewayRequestPriceFact(&fact, missingCacheUsage)
				if missingCacheEstimate.Status != GatewayCostStatusUnavailable || missingCacheEstimate.Cost != nil ||
					missingCacheEstimate.UnavailableReason != GatewayPricingUnavailablePriceMissing {
					t.Fatalf("positive usage with missing tier cache price = %+v", missingCacheEstimate)
				}
			} else {
				assertGatewayChannelResolutionFlatFact(t, frozen, gatewayRequestPriceSourceModelConfig, 90, 100, 9, 10)
				if frozen.PriceLongContextThresholdTokens != nil || frozen.PriceLongContextInputUnit != nil ||
					frozen.PriceLongContextOutputUnit != nil || frozen.PriceLongContextCacheReadUnit != nil ||
					frozen.PriceLongContextCacheWriteUnit != nil {
					t.Fatalf("unsupported whole-request tier leaked into global fallback: %+v", frozen)
				}
			}
		})
	}
}

func TestFreezeGatewayRequestPricingFallsBackWhenChannelGroupRatioDoesNotMatchRequest(t *testing.T) {
	db, store, channel, audit := gatewayChannelPriceResolutionFixture(t, "channel-ratio-mismatch-model")
	createGatewayChannelResolutionGlobalPrice(t, store, audit.ModelName, 90, 100, 9, 10)
	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, time.Now().UTC(), "0",
		gatewayChannelPriceResolutionItem("primary", audit.ModelName, 3, 7, gatewayChannelPriceResolutionFloat(0.3), gatewayChannelPriceResolutionFloat(0.7)),
	)
	rawGroupRatio, rawCompletionRatio, fundingFactor := 2.0, 2.0, 0.5
	audit.RawGroupRatio = &rawGroupRatio
	audit.RawCompletionRatio = &rawCompletionRatio
	audit.GroupRatio = 1
	audit.CompletionRatio = 1
	audit.FundingFactor = &fundingFactor

	frozen := freezeGatewayModelPriceForTest(t, db, audit, time.Now().UTC())
	assertGatewayChannelResolutionFlatFact(t, frozen, gatewayRequestPriceSourceModelConfig, 90, 100, 9, 10)
}

func gatewayChannelPriceResolutionFixture(t *testing.T, modelName string) (*gorm.DB, *Gateway, *Channel, GatewayRequestDispatchAudit) {
	t.Helper()
	db, store, channel, _, _, audit := gatewayRequestPricingSelectionFixture(t)
	var current Channel
	if err := db.First(&current, channel.ID).Error; err != nil {
		t.Fatalf("reload channel price resolution fixture channel: %v", err)
	}
	rawGroupRatio, rawCompletionRatio, fundingFactor := 1.0, 1.0, 1.0
	audit.ModelName = modelName
	audit.UpstreamModelName = modelName
	audit.GroupRatio = 1
	audit.CompletionRatio = 1
	audit.RawGroupRatio = &rawGroupRatio
	audit.RawCompletionRatio = &rawCompletionRatio
	audit.FundingFactor = &fundingFactor
	audit.ChannelIdentityRevision = current.UpstreamAccountIdentityRevision
	return db, store, &current, audit
}

func createGatewayChannelResolutionGlobalPrice(
	t *testing.T,
	store *Gateway,
	modelName string,
	input, output, cacheRead, cacheWrite float64,
) {
	t.Helper()
	if _, err := store.CreateModelPrice(GatewayModelPriceInput{
		ModelName: modelName, InputUnitPriceUSD: input, OutputUnitPriceUSD: output,
		CacheReadUnitPriceUSD: &cacheRead, CacheWriteUnitPriceUSD: &cacheWrite, Enabled: true,
	}, "channel-price-resolution-test"); err != nil {
		t.Fatalf("create global fallback price for %q: %v", modelName, err)
	}
}

func replaceGatewayChannelPriceResolutionSnapshot(
	t *testing.T,
	db *gorm.DB,
	channel *Channel,
	observedAt time.Time,
	checksumRune string,
	items ...GatewayChannelModelPriceSnapshotItem,
) {
	t.Helper()
	if err := NewRates(db).ReplaceChannelModelPriceSnapshot(context.Background(), GatewayChannelModelPriceSnapshotInput{
		ChannelID: channel.ID, ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ExpectedCostUnitEpoch: channel.CostUnitEpoch, SourceVersion: "channel-price-resolution-test-" + checksumRune,
		SourceChecksum: strings.Repeat(checksumRune, 64), ObservedAt: observedAt, Items: items,
	}); err != nil {
		t.Fatalf("replace channel model price snapshot: %v", err)
	}
}

func gatewayChannelPriceResolutionItem(
	groupName, modelName string,
	input, output float64,
	cacheRead, cacheWrite *float64,
) GatewayChannelModelPriceSnapshotItem {
	return gatewayChannelPriceResolutionItemWithRatio(groupName, modelName, 1, input, output, cacheRead, cacheWrite)
}

func gatewayChannelPriceResolutionItemWithRatio(
	groupName, modelName string,
	groupRatio, input, output float64,
	cacheRead, cacheWrite *float64,
) GatewayChannelModelPriceSnapshotItem {
	return GatewayChannelModelPriceSnapshotItem{
		UpstreamGroupID: 1, UpstreamGroupName: groupName, GroupRatio: gatewayChannelPriceResolutionFloat(groupRatio),
		ModelName: modelName, InputUnitPriceUSD: input, OutputUnitPriceUSD: output,
		CacheReadUnitPriceUSD: cacheRead, CacheWriteUnitPriceUSD: cacheWrite,
	}
}

func assertGatewayChannelResolutionFlatFact(
	t *testing.T,
	audit GatewayRequestDispatchAudit,
	source string,
	input, output, cacheRead, cacheWrite float64,
) {
	t.Helper()
	if audit.PriceFactID == nil || audit.PriceSource != source ||
		audit.PriceFactSchemaVersion != GatewayRequestPriceFactSchemaVersion ||
		audit.PriceInputUnit == nil || *audit.PriceInputUnit != input ||
		audit.PriceOutputUnit == nil || *audit.PriceOutputUnit != output ||
		audit.PriceCacheReadUnit == nil || *audit.PriceCacheReadUnit != cacheRead ||
		audit.PriceCacheWriteUnit == nil || *audit.PriceCacheWriteUnit != cacheWrite {
		t.Fatalf("frozen flat price fact = %+v", audit)
	}
}

func gatewayChannelPriceResolutionFact(t *testing.T, db *gorm.DB, audit GatewayRequestDispatchAudit) GatewayRequestPriceFact {
	t.Helper()
	if audit.PriceFactID == nil {
		t.Fatal("frozen audit has no price fact ID")
	}
	var fact GatewayRequestPriceFact
	if err := db.First(&fact, *audit.PriceFactID).Error; err != nil {
		t.Fatalf("load frozen channel price fact: %v", err)
	}
	return fact
}

func gatewayChannelPriceResolutionFloat(value float64) *float64 {
	return &value
}
