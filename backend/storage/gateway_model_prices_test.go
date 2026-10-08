package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestGatewayModelPriceCatalogAuditedCRUD(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)

	input := GatewayModelPriceInput{
		ModelName: " model-price-crud ", InputUnitPriceUSD: 1, OutputUnitPriceUSD: 2,
		CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.1), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.2), Enabled: false,
	}
	created, err := store.CreateModelPrice(input, " price-admin ")
	if err != nil {
		t.Fatalf("create model price: %v", err)
	}
	if created.ModelName != "model-price-crud" || created.Enabled || created.Revision != 1 {
		t.Fatalf("created model price = %+v", created)
	}
	if _, err := store.CreateModelPrice(input, "price-admin"); !errors.Is(err, ErrGatewayModelPriceExists) {
		t.Fatalf("duplicate model price error = %v", err)
	}
	if _, err := store.UpdateModelPrice(created.ID, 2, input, "price-admin"); !errors.Is(err, ErrGatewayModelPriceConflict) {
		t.Fatalf("stale update error = %v", err)
	}

	input.OutputUnitPriceUSD = 3
	input.Enabled = true
	updated, err := store.UpdateModelPrice(created.ID, created.Revision, input, "price-admin")
	if err != nil {
		t.Fatalf("update model price: %v", err)
	}
	if updated.Revision != 2 || updated.OutputUnitPriceUSD != 3 || !updated.Enabled {
		t.Fatalf("updated model price = %+v", updated)
	}
	if err := store.DeleteModelPrice(updated.ID, updated.Revision, "price-admin"); err != nil {
		t.Fatalf("delete model price: %v", err)
	}
	if err := db.First(&GatewayModelPrice{}, updated.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted model price lookup error = %v", err)
	}

	var audits []GatewayModelPriceAudit
	if err := db.Where("model_price_id = ?", updated.ID).Order("revision ASC").Find(&audits).Error; err != nil {
		t.Fatalf("list model price audits: %v", err)
	}
	if len(audits) != 3 || audits[0].Action != "create" || audits[0].Actor != "price-admin" ||
		audits[1].Action != "update" || audits[1].Revision != 2 ||
		audits[2].Action != "delete" || audits[2].Revision != 3 {
		t.Fatalf("model price audits = %+v", audits)
	}
	if err := db.Model(&GatewayModelPriceAudit{}).Where("id = ?", audits[0].ID).Update("actor", "tampered").Error; err == nil {
		t.Fatal("append-only model price audit accepted an update")
	}
}

func TestGatewayModelPriceCatalogRejectsUnauditedMutations(t *testing.T) {
	db := openTestDB(t)
	createGPT56ModelPriceForTest(t, NewGateway(db))
	assertRejected := func(name string, mutate func(*gorm.DB) error) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			err := db.Transaction(func(tx *gorm.DB) error {
				if err := mutate(tx); err != nil {
					return err
				}
				if err := tx.Exec("SET CONSTRAINTS ALL IMMEDIATE").Error; err != nil {
					return err
				}
				return errors.New("unaudited mutation reached transaction commit")
			})
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
				t.Fatalf("unaudited mutation was not rejected by the database: %v", err)
			}
		})
	}

	assertRejected("insert", func(tx *gorm.DB) error {
		return tx.Exec(`
			INSERT INTO gateway_model_prices (
				model_name, input_unit_price_usd, output_unit_price_usd,
				cache_read_unit_price_usd, cache_write_unit_price_usd, enabled, revision
			) VALUES (?, ?, ?, ?, ?, ?, ?)
		`, "unaudited-model-price", 1, 2, 0.1, 0.2, true, 1).Error
	})
	assertRejected("update", func(tx *gorm.DB) error {
		return tx.Exec(`UPDATE gateway_model_prices SET output_unit_price_usd = ? WHERE model_name = ?`, 31, "gpt-5.6-sol").Error
	})
	assertRejected("delete", func(tx *gorm.DB) error {
		return tx.Exec(`DELETE FROM gateway_model_prices WHERE model_name = ?`, "gpt-5.6-sol").Error
	})
	assertRejected("truncate", func(tx *gorm.DB) error {
		return tx.Exec(`TRUNCATE TABLE gateway_model_prices`).Error
	})
}

func TestGatewayModelPriceCatalogRejectsHistoricalAuditReplayAndInvalidTransitions(t *testing.T) {
	t.Run("deleted id cannot reuse its create audit", func(t *testing.T) {
		db := openTestDB(t)
		store := NewGateway(db)
		created, err := store.CreateModelPrice(GatewayModelPriceInput{
			ModelName: "deleted-model-price", InputUnitPriceUSD: 1, OutputUnitPriceUSD: 2,
			CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.1), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.2), Enabled: true,
		}, "price-admin")
		if err != nil {
			t.Fatalf("create model price: %v", err)
		}
		if err := store.DeleteModelPrice(created.ID, created.Revision, "price-admin"); err != nil {
			t.Fatalf("delete model price: %v", err)
		}
		assertGatewayModelPriceCatalogMutationRejected(t, db, func(tx *gorm.DB) error {
			return tx.Exec(`
				INSERT INTO gateway_model_prices (
					id, model_name, input_unit_price_usd, output_unit_price_usd,
					cache_read_unit_price_usd, cache_write_unit_price_usd, enabled,
					revision, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, created.ID, created.ModelName, created.InputUnitPriceUSD, created.OutputUnitPriceUSD,
				created.CacheReadUnitPriceUSD, created.CacheWriteUnitPriceUSD, created.Enabled,
				created.Revision, created.CreatedAt, created.UpdatedAt).Error
		})
	})

	t.Run("orphan audit is rejected", func(t *testing.T) {
		db := openTestDB(t)
		store := NewGateway(db)
		created, err := store.CreateModelPrice(GatewayModelPriceInput{
			ModelName: "historical-update-audit", InputUnitPriceUSD: 1, OutputUnitPriceUSD: 2,
			CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.1), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.2), Enabled: true,
		}, "price-admin")
		if err != nil {
			t.Fatalf("create model price: %v", err)
		}
		orphan := gatewayModelPriceAudit(*created, "update", "orphan-audit", time.Now().UTC())
		orphan.Revision = created.Revision + 1
		orphan.OutputUnitPriceUSD = 9
		err = db.Create(&orphan).Error
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
			t.Fatalf("orphan audit was not rejected by the database: %v", err)
		}
	})

	t.Run("historical update audit cannot be reused", func(t *testing.T) {
		db := openTestDB(t)
		store := NewGateway(db)
		created, err := store.CreateModelPrice(GatewayModelPriceInput{
			ModelName: "historical-update-audit", InputUnitPriceUSD: 1, OutputUnitPriceUSD: 2,
			CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.1), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.2), Enabled: true,
		}, "price-admin")
		if err != nil {
			t.Fatalf("create model price: %v", err)
		}
		input := gatewayModelPriceInputForTest(*created)
		input.OutputUnitPriceUSD = 9
		updated, err := store.UpdateModelPrice(created.ID, created.Revision, input, "price-admin")
		if err != nil {
			t.Fatalf("create historical update audit: %v", err)
		}
		assertGatewayModelPriceCatalogMutationRejected(t, db, func(tx *gorm.DB) error {
			return tx.Model(&GatewayModelPrice{}).Where("id = ?", updated.ID).Updates(map[string]any{
				"output_unit_price_usd": updated.OutputUnitPriceUSD,
				"revision":              updated.Revision,
				"updated_at":            updated.UpdatedAt.Add(time.Second),
			}).Error
		})
	})

	for _, transition := range []struct {
		name   string
		mutate func(*gorm.DB, GatewayModelPrice) error
	}{
		{
			name: "revision cannot skip",
			mutate: func(tx *gorm.DB, created GatewayModelPrice) error {
				next := created
				next.Revision += 2
				next.OutputUnitPriceUSD = 7
				audit := gatewayModelPriceAudit(next, "update", "invalid-transition", time.Now().UTC())
				if err := tx.Create(&audit).Error; err != nil {
					return err
				}
				return tx.Model(&GatewayModelPrice{}).Where("id = ?", created.ID).Updates(map[string]any{
					"output_unit_price_usd": next.OutputUnitPriceUSD,
					"revision":              next.Revision,
					"updated_at":            time.Now().UTC(),
				}).Error
			},
		},
		{
			name: "primary key is immutable",
			mutate: func(tx *gorm.DB, created GatewayModelPrice) error {
				next := created
				next.ID += 1_000_000
				next.Revision++
				audit := gatewayModelPriceAudit(next, "update", "invalid-transition", time.Now().UTC())
				if err := tx.Create(&audit).Error; err != nil {
					return err
				}
				return tx.Model(&GatewayModelPrice{}).Where("id = ?", created.ID).Updates(map[string]any{
					"id":         next.ID,
					"revision":   next.Revision,
					"updated_at": time.Now().UTC(),
				}).Error
			},
		},
		{
			name: "creation time is immutable",
			mutate: func(tx *gorm.DB, created GatewayModelPrice) error {
				next := created
				next.Revision++
				audit := gatewayModelPriceAudit(next, "update", "invalid-transition", time.Now().UTC())
				if err := tx.Create(&audit).Error; err != nil {
					return err
				}
				return tx.Model(&GatewayModelPrice{}).Where("id = ?", created.ID).Updates(map[string]any{
					"created_at": created.CreatedAt.Add(time.Second),
					"updated_at": created.UpdatedAt.Add(time.Second),
					"revision":   next.Revision,
				}).Error
			},
		},
	} {
		t.Run(transition.name, func(t *testing.T) {
			db := openTestDB(t)
			store := NewGateway(db)
			created, err := store.CreateModelPrice(GatewayModelPriceInput{
				ModelName:         "invalid-transition-" + strings.ReplaceAll(transition.name, " ", "-"),
				InputUnitPriceUSD: 1, OutputUnitPriceUSD: 2,
				CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.1), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.2), Enabled: true,
			}, "price-admin")
			if err != nil {
				t.Fatalf("create model price: %v", err)
			}
			assertGatewayModelPriceCatalogMutationRejected(t, db, func(tx *gorm.DB) error {
				return transition.mutate(tx, *created)
			})
		})
	}
}

func assertGatewayModelPriceCatalogMutationRejected(t *testing.T, db *gorm.DB, mutate func(*gorm.DB) error) {
	t.Helper()
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := mutate(tx); err != nil {
			return err
		}
		if err := tx.Exec("SET CONSTRAINTS ALL IMMEDIATE").Error; err != nil {
			return err
		}
		return errors.New("invalid catalog mutation reached transaction commit")
	})
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23514" {
		t.Fatalf("invalid catalog mutation was not rejected by the database: %v", err)
	}
}

func TestFreezeGatewayRequestPricingDerivesAndVersionsModelPrices(t *testing.T) {
	db, store, _, _, _, base := gatewayRequestPricingSelectionFixture(t)
	createGPT56ModelPriceForTest(t, store)
	base.ModelName = "gpt-5.6-sol"
	base.UpstreamModelName = "gpt-5.6-sol"
	base.GroupRatio = 0.01
	base.CompletionRatio = 0
	now := time.Now().UTC()

	first := freezeGatewayModelPriceForTest(t, db, base, now)
	assertFrozenGatewayModelPrice(t, first, 0.05, 0.30, 0.005, 0.0625, 1)
	second := freezeGatewayModelPriceForTest(t, db, base, now)
	assertFrozenGatewayModelPrice(t, second, 0.05, 0.30, 0.005, 0.0625, 1)
	if first.PriceFactID == nil || second.PriceFactID == nil || *first.PriceFactID != *second.PriceFactID {
		t.Fatalf("unchanged model price did not reuse fact: first=%v second=%v", first.PriceFactID, second.PriceFactID)
	}

	seeded := loadGatewayModelPriceForTest(t, db, "gpt-5.6-sol")
	input := gatewayModelPriceInputForTest(seeded)
	input.OutputUnitPriceUSD = 40
	updated, err := store.UpdateModelPrice(seeded.ID, seeded.Revision, input, "price-admin")
	if err != nil {
		t.Fatalf("update seeded model price: %v", err)
	}
	third := freezeGatewayModelPriceForTest(t, db, base, now.Add(time.Second))
	assertFrozenGatewayModelPrice(t, third, 0.05, 0.40, 0.005, 0.0625, 2)
	if third.PriceFactID == nil || first.PriceFactID == nil || *third.PriceFactID == *first.PriceFactID {
		t.Fatalf("changed model price reused old fact: first=%v third=%v", first.PriceFactID, third.PriceFactID)
	}

	disabled := gatewayModelPriceInputForTest(*updated)
	disabled.Enabled = false
	if _, err := store.UpdateModelPrice(updated.ID, updated.Revision, disabled, "price-admin"); err != nil {
		t.Fatalf("disable model price: %v", err)
	}
	unavailable := freezeGatewayModelPriceForTest(t, db, base, now.Add(2*time.Second))
	if unavailable.PriceFactID != nil || unavailable.PricingUnavailableReason != GatewayPricingUnavailableFactMissing {
		t.Fatalf("disabled model price remained active: %+v", unavailable)
	}
}

func TestBeginRequestDispatchDerivesModelPriceForNewAPIWithoutAccountBinding(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	createGPT56ModelPriceForTest(t, store)
	channel := &Channel{
		Name: "newapi-model-price", Type: ChannelTypeNewAPI,
		SiteURL: "https://newapi-model-price.example", PasswordCipher: "cipher", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create NewAPI channel: %v", err)
	}
	now := time.Now().UTC()
	if _, err := rates.Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "primary", Ratio: 0.01, CompletionRatio: 0.035, LastSeenAt: now,
	}); err != nil {
		t.Fatalf("create NewAPI group rate: %v", err)
	}
	group, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture NewAPI group generation: %v", err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 92,
		UpstreamKeyName: "bridgeops-newapi-primary", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", Enabled: true,
	}); err != nil {
		t.Fatalf("create NewAPI upstream key: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil {
		t.Fatalf("load NewAPI upstream key: %v", err)
	}
	var bindingCount int64
	if err := db.Model(&GatewayChannelUpstreamAccountBinding{}).Where("channel_id = ?", channel.ID).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count NewAPI account bindings: %v", err)
	}
	if bindingCount != 0 {
		t.Fatalf("NewAPI test unexpectedly has %d durable account bindings", bindingCount)
	}

	rawGroupRatio, rawCompletionRatio, fundingFactor := 0.01, 0.035, 1.0
	snapshot := GatewayRequestDispatchSnapshot{
		ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: "primary",
		UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
		ModelName: "gpt-5.6-sol", UpstreamModelName: "gpt-5.6-sol",
		Method: "POST", Path: "/v1/responses", RequestStartedAt: now,
		GroupRatio: 0.01, CompletionRatio: 0.035,
		RawGroupRatio: &rawGroupRatio, RawCompletionRatio: &rawCompletionRatio,
		FundingFactor: &fundingFactor, CostBasisVersion: 1, CostBasisSource: "newapi_rates",
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
		UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
		PriceOperation: GatewayPricingOperationTextGeneration,
		PriceProtocol:  GatewayPricingProtocolOpenAIResponses,
		UsageSchema:    GatewayPricingUsageSchemaTokensV1, UsageSchemaVersion: 1,
	}
	snapshot = requestDispatchSnapshotWithCurrentTelemetry(t, snapshot, now)
	persisted, err := store.BeginRequestDispatch(snapshot, now)
	if err != nil {
		t.Fatalf("begin NewAPI dispatch with model catalog price: %v", err)
	}
	assertFrozenGatewayModelPrice(t, *persisted, 0.05, 1.05, 0.005, 0.0625, 1)

	providerFact := gatewayRequestPricingTestFact()
	providerFact.ID = 0
	providerFact.ChannelID = channel.ID
	providerFact.ChannelAccountIdentityRevision = channel.UpstreamAccountIdentityRevision
	providerFact.CostUnitEpoch = channel.CostUnitEpoch
	providerFact.UpstreamGroupID = group.ID
	providerFact.UpstreamGroupPresenceGeneration = group.PresenceGeneration
	providerFact.GatewayUpstreamKeyID = key.ID
	providerFact.UpstreamKeyGeneration = key.KeyGeneration
	providerFact.ModelName = "provider-priced-model"
	providerFact.ObservedAt = now
	providerFact.ValidFrom = now
	providerFact.FactVersion = 0
	providerFact.Checksum = ""
	if err := store.AppendRequestPriceFact(&providerFact); !errors.Is(err, ErrGatewayRequestPriceIdentityChanged) {
		t.Fatalf("NewAPI provider fact bypassed durable account binding: %v", err)
	}
}

func TestGatewayRequestPriceFactFromModelConfigUsesCompletionRatio(t *testing.T) {
	input := 5.0
	output := 30.0
	cacheRead := 0.5
	cacheWrite := 6.25
	price := &GatewayModelPrice{
		ModelName: "gpt-5.6-sol", InputUnitPriceUSD: input, OutputUnitPriceUSD: output,
		CacheReadUnitPriceUSD: &cacheRead, CacheWriteUnitPriceUSD: &cacheWrite, Enabled: true,
	}
	scope := GatewayRequestPriceScope{
		ChannelID: 1, ChannelAccountIdentityRevision: 1, UpstreamGroupID: 1,
		CanonicalGroupName: "plus", UpstreamGroupPresenceGeneration: 1,
		GatewayUpstreamKeyID: 1, UpstreamKeyGeneration: 1, ModelName: price.ModelName,
		Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
	}

	fact, ok := gatewayRequestPriceFactFromModelConfig(scope, 1, 0.01, 0.02, price, time.Now().UTC())
	if !ok {
		t.Fatal("derive model price fact failed")
	}
	if fact.SchemaVersion != GatewayRequestPriceFactSchemaVersion ||
		fact.InputUnitPrice == nil || *fact.InputUnitPrice != 0.05 ||
		fact.OutputUnitPrice == nil || *fact.OutputUnitPrice != 0.60 ||
		fact.CacheReadUnitPrice == nil || *fact.CacheReadUnitPrice != 0.005 ||
		fact.CacheWriteUnitPrice == nil || *fact.CacheWriteUnitPrice != 0.0625 {
		t.Fatalf("derived model price fact = %+v", fact)
	}

	fallback, ok := gatewayRequestPriceFactFromModelConfig(scope, 1, 0.01, 0, price, time.Now().UTC())
	if !ok || fallback.OutputUnitPrice == nil || *fallback.OutputUnitPrice != 0.30 {
		t.Fatalf("zero completion ratio fallback = %+v, ok=%v", fallback, ok)
	}
}

func TestGatewayRequestPriceFactFromTieredModelConfigFreezesRawPricesAndRatios(t *testing.T) {
	threshold := GatewayRequestPriceLongContextThreshold
	longInput, longOutput, longCacheRead, longCacheWrite := 10.0, 45.0, 1.0, 12.5
	price := &GatewayModelPrice{
		ModelName: "tiered-model", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
		CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.5), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(6.25),
		LongContextThresholdTokens: &threshold, LongContextInputUnitPriceUSD: &longInput,
		LongContextOutputUnitPriceUSD: &longOutput, LongContextCacheReadUnitPriceUSD: &longCacheRead,
		LongContextCacheWriteUnitPriceUSD: &longCacheWrite, Enabled: true,
	}
	scope := GatewayRequestPriceScope{
		ChannelID: 1, ChannelAccountIdentityRevision: 1, UpstreamGroupID: 1,
		CanonicalGroupName: "plus", UpstreamGroupPresenceGeneration: 1,
		GatewayUpstreamKeyID: 1, UpstreamKeyGeneration: 1, ModelName: price.ModelName,
		Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
	}

	fact, ok := gatewayRequestPriceFactFromModelConfig(scope, 1, 0.01, 0.02, price, time.Now().UTC())
	if !ok {
		t.Fatal("derive tiered model price fact failed")
	}
	if fact.SchemaVersion != GatewayRequestPriceFactTieredSchemaVersion ||
		fact.InputUnitPrice == nil || *fact.InputUnitPrice != 5 ||
		fact.OutputUnitPrice == nil || *fact.OutputUnitPrice != 30 ||
		fact.CacheReadUnitPrice == nil || *fact.CacheReadUnitPrice != 0.5 ||
		fact.CacheWriteUnitPrice == nil || *fact.CacheWriteUnitPrice != 6.25 ||
		fact.LongContextThresholdTokens == nil || *fact.LongContextThresholdTokens != threshold ||
		fact.LongContextInputUnitPrice == nil || *fact.LongContextInputUnitPrice != longInput ||
		fact.LongContextOutputUnitPrice == nil || *fact.LongContextOutputUnitPrice != longOutput ||
		fact.LongContextCacheReadUnitPrice == nil || *fact.LongContextCacheReadUnitPrice != longCacheRead ||
		fact.LongContextCacheWriteUnitPrice == nil || *fact.LongContextCacheWriteUnitPrice != longCacheWrite ||
		fact.GroupRatio == nil || *fact.GroupRatio != 0.01 ||
		fact.CompletionRatio == nil || *fact.CompletionRatio != 0.02 {
		t.Fatalf("derived tiered model price fact = %+v", fact)
	}

	zero := int64(0)
	input := threshold + 1
	output := int64(1)
	estimate := EvaluateGatewayRequestPriceFact(fact, GatewayRequestPricingUsage{
		InputTokens: &input, OutputTokens: &output, CacheReadTokens: &zero, CacheWriteTokens: &zero,
	})
	want := (float64(input)*longInput*0.01 + float64(output)*longOutput*0.02) / 1_000_000
	if estimate.Cost == nil || math.Abs(*estimate.Cost-want) > 1e-12 {
		t.Fatalf("tiered model price estimate = %+v, want %.12f", estimate, want)
	}

	changed := *fact
	changed.GroupRatio = floatPointerForGatewayModelPriceTest(0.02)
	if gatewayRequestModelConfigFactMatches(fact, &changed) {
		t.Fatal("model-config fact match ignored the frozen group ratio")
	}
}

func TestNormalizeGatewayModelPriceInputRejectsUnsupportedLongContextThreshold(t *testing.T) {
	threshold := int64(128_000)
	longPrice := 1.0
	input := GatewayModelPriceInput{
		ModelName: "tiered-model", InputUnitPriceUSD: 1, OutputUnitPriceUSD: 1,
		CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.1), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(1),
		LongContextThresholdTokens: &threshold, LongContextInputUnitPriceUSD: &longPrice,
		LongContextOutputUnitPriceUSD: &longPrice, LongContextCacheReadUnitPriceUSD: &longPrice,
		LongContextCacheWriteUnitPriceUSD: &longPrice,
	}
	if err := normalizeGatewayModelPriceInput(&input); !errors.Is(err, ErrGatewayModelPriceInvalid) {
		t.Fatalf("unsupported long-context threshold error = %v", err)
	}
}

func floatPointerForGatewayModelPriceTest(value float64) *float64 {
	return &value
}

func TestFreezeGatewayRequestPricingKeepsExplicitPriceFactPriority(t *testing.T) {
	db, store, channel, _, _, base := gatewayRequestPricingSelectionFixture(t)
	if _, err := store.CreateModelPrice(GatewayModelPriceInput{
		ModelName: "model-exact", InputUnitPriceUSD: 9, OutputUnitPriceUSD: 10,
		CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(8), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(11), Enabled: true,
	}, "price-admin"); err != nil {
		t.Fatalf("create competing model price: %v", err)
	}
	if err := db.Model(&RateSnapshot{}).
		Where("channel_id = ? AND model_name = ?", channel.ID, "primary").
		Updates(map[string]any{
			"ratio": *base.RawGroupRatio, "completion_ratio": *base.RawCompletionRatio,
		}).Error; err != nil {
		t.Fatalf("align group rate for competing channel price: %v", err)
	}
	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, time.Now().UTC(), "f",
		gatewayChannelPriceResolutionItemWithRatio(
			"primary", base.ModelName, *base.RawGroupRatio, 12, 13,
			gatewayChannelPriceResolutionFloat(1.2), gatewayChannelPriceResolutionFloat(1.3),
		),
	)
	frozen := freezeGatewayModelPriceForTest(t, db, base, time.Now().UTC())
	if frozen.PriceSource != "provider_catalog" || frozen.PriceFactVersion != 1 ||
		frozen.PriceInputUnit == nil || *frozen.PriceInputUnit != 1.25 {
		t.Fatalf("model config displaced explicit price fact: %+v", frozen)
	}
}

func TestAdministratorPriceWithEarlierObservationOverridesGeneratedPrice(t *testing.T) {
	db := openTestDB(t)
	store, channel, group, key := gatewayAdministratorPriceImportFixture(t, db, "administrator-after-generated-price")
	if _, err := store.CreateModelPrice(GatewayModelPriceInput{
		ModelName: "model-a", InputUnitPriceUSD: 9, OutputUnitPriceUSD: 10,
		CacheReadUnitPriceUSD:  floatPointerForGatewayModelPriceTest(0.9),
		CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(11),
		Enabled:                true,
	}, "price-admin"); err != nil {
		t.Fatalf("create generated model price source: %v", err)
	}

	ratio, fundingFactor := 1.0, 1.0
	audit := GatewayRequestDispatchAudit{
		ChannelID: channel.ID, GroupName: group.CanonicalGroupName,
		UpstreamKeyID: key.UpstreamKeyID, UpstreamKeyName: key.UpstreamKeyName,
		ModelName: "model-a", UpstreamModelName: "model-a", Method: "POST", Path: "/v1/responses",
		GroupRatio: ratio, CompletionRatio: ratio, RawGroupRatio: &ratio, RawCompletionRatio: &ratio,
		FundingFactor: &fundingFactor, CostBasisVersion: 1, CostBasisSource: "test",
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         group.ID, UpstreamGroupGeneration: group.PresenceGeneration,
		UpstreamKeyLocalID: key.ID, UpstreamKeyGeneration: key.KeyGeneration,
		PriceOperation: GatewayPricingOperationTextGeneration, PriceProtocol: GatewayPricingProtocolOpenAIResponses,
		UsageSchema: GatewayPricingUsageSchemaTokensV1, UsageSchemaVersion: 1,
	}
	generated := freezeGatewayModelPriceForTest(t, db, audit, time.Now().UTC())
	if generated.PriceSource != gatewayRequestPriceSourceModelConfig || generated.PriceFactVersion != 1 {
		t.Fatalf("generated price fact = %+v", generated)
	}

	input, output := 1.25, 2.5
	now := time.Now().UTC()
	result, err := store.ImportAdministratorRequestPriceFacts(
		context.Background(),
		"admin-user",
		"administrator-after-generated-price",
		[]GatewayAdministratorPriceObservation{
			gatewayAdministratorPriceObservationForTest(channel, group, key, now.Add(-2*time.Minute), GatewayAdministratorPriceObservation{
				ChannelID: channel.ID, GroupName: group.CanonicalGroupName, ModelName: "model-a",
				Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
				InputUnitPriceUSD: &input, OutputUnitPriceUSD: &output, ValidUntil: now.Add(time.Hour),
			}),
		},
	)
	if err != nil {
		t.Fatalf("import earlier administrator observation after generated price: %v", err)
	}
	if len(result.Items) != 1 || result.Items[0].FactVersion != 2 || result.Items[0].Source != GatewayRequestPriceSourceAdministrator {
		t.Fatalf("administrator price fact = %+v", result.Items)
	}

	frozen := freezeGatewayModelPriceForTest(t, db, audit, time.Now().UTC())
	if frozen.PriceSource != GatewayRequestPriceSourceAdministrator || frozen.PriceFactVersion != 2 ||
		frozen.PriceInputUnit == nil || *frozen.PriceInputUnit != input ||
		frozen.PriceOutputUnit == nil || *frozen.PriceOutputUnit != output {
		t.Fatalf("administrator price did not override generated price: %+v", frozen)
	}
}

func TestFreezeGatewayRequestPricingCreatesOneModelFactConcurrently(t *testing.T) {
	db, store, _, _, _, base := gatewayRequestPricingSelectionFixture(t)
	createGPT56ModelPriceForTest(t, store)
	base.ModelName = "gpt-5.6-sol"
	base.UpstreamModelName = "gpt-5.6-sol"
	base.GroupRatio = 0.01
	base.CompletionRatio = 0
	now := time.Now().UTC()

	type result struct {
		id  uint64
		err error
	}
	results := make(chan result, 8)
	var start sync.WaitGroup
	start.Add(1)
	for worker := 0; worker < 8; worker++ {
		go func() {
			start.Wait()
			audit := base
			err := db.Transaction(func(tx *gorm.DB) error {
				return freezeGatewayRequestPricingTx(tx, &audit, now)
			})
			var id uint64
			if audit.PriceFactID != nil {
				id = *audit.PriceFactID
			}
			results <- result{id: id, err: err}
		}()
	}
	start.Done()
	var factID uint64
	for worker := 0; worker < 8; worker++ {
		got := <-results
		if got.err != nil || got.id == 0 {
			t.Fatalf("concurrent freeze result = %+v", got)
		}
		if factID == 0 {
			factID = got.id
		} else if got.id != factID {
			t.Fatalf("concurrent freezes used fact IDs %d and %d", factID, got.id)
		}
	}
	var count int64
	if err := db.Model(&GatewayRequestPriceFact{}).
		Where("model_name = ? AND source = ?", "gpt-5.6-sol", gatewayRequestPriceSourceModelConfig).
		Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("derived price fact count = %d, err=%v", count, err)
	}
}

func freezeGatewayModelPriceForTest(t *testing.T, db *gorm.DB, base GatewayRequestDispatchAudit, at time.Time) GatewayRequestDispatchAudit {
	t.Helper()
	audit := base
	if err := db.Transaction(func(tx *gorm.DB) error {
		return freezeGatewayRequestPricingTx(tx, &audit, at)
	}); err != nil {
		t.Fatalf("freeze gateway model price: %v", err)
	}
	return audit
}

func assertFrozenGatewayModelPrice(t *testing.T, audit GatewayRequestDispatchAudit, input, output, cacheRead, cacheWrite float64, version uint64) {
	t.Helper()
	if audit.PriceFactID == nil || audit.PriceSource != gatewayRequestPriceSourceModelConfig ||
		audit.PriceFactVersion != version || audit.PriceInputUnit == nil || audit.PriceOutputUnit == nil ||
		audit.PriceCacheReadUnit == nil || audit.PriceCacheWriteUnit == nil ||
		math.Abs(*audit.PriceInputUnit-input) > 1e-12 || math.Abs(*audit.PriceOutputUnit-output) > 1e-12 ||
		math.Abs(*audit.PriceCacheReadUnit-cacheRead) > 1e-12 || math.Abs(*audit.PriceCacheWriteUnit-cacheWrite) > 1e-12 {
		t.Fatalf("frozen model price = %+v", audit)
	}
}

func loadGatewayModelPriceForTest(t *testing.T, db *gorm.DB, modelName string) GatewayModelPrice {
	t.Helper()
	var price GatewayModelPrice
	if err := db.Where("model_name = ?", modelName).Take(&price).Error; err != nil {
		t.Fatalf("load model price %q: %v", modelName, err)
	}
	return price
}

func gatewayModelPriceInputForTest(price GatewayModelPrice) GatewayModelPriceInput {
	return GatewayModelPriceInput{
		ModelName: price.ModelName, InputUnitPriceUSD: price.InputUnitPriceUSD,
		OutputUnitPriceUSD: price.OutputUnitPriceUSD, CacheReadUnitPriceUSD: price.CacheReadUnitPriceUSD,
		CacheWriteUnitPriceUSD:            price.CacheWriteUnitPriceUSD,
		LongContextThresholdTokens:        price.LongContextThresholdTokens,
		LongContextInputUnitPriceUSD:      price.LongContextInputUnitPriceUSD,
		LongContextOutputUnitPriceUSD:     price.LongContextOutputUnitPriceUSD,
		LongContextCacheReadUnitPriceUSD:  price.LongContextCacheReadUnitPriceUSD,
		LongContextCacheWriteUnitPriceUSD: price.LongContextCacheWriteUnitPriceUSD,
		AutoUpdate:                        price.AutoUpdate,
		Enabled:                           price.Enabled,
	}
}

func createGPT56ModelPriceForTest(t *testing.T, store *Gateway) *GatewayModelPrice {
	t.Helper()
	price, err := store.CreateModelPrice(GatewayModelPriceInput{
		ModelName: "gpt-5.6-sol", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
		CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.5), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(6.25), Enabled: true,
	}, "price-test")
	if err != nil {
		t.Fatalf("create GPT-5.6 Sol model price: %v", err)
	}
	return price
}

func TestGatewayModelPriceSourceMetadataOnlyChangesWithPriceValues(t *testing.T) {
	observedAt := time.Now().UTC()
	price := GatewayModelPrice{
		ModelName: "gpt-5.6-sol", InputUnitPriceUSD: 4, OutputUnitPriceUSD: 20,
		CacheReadUnitPriceUSD: floatPointerForGatewayModelPriceTest(0.4), CacheWriteUnitPriceUSD: floatPointerForGatewayModelPriceTest(5),
		Source: GatewayModelPriceSourceLiteLLM, SourceURL: "https://catalog.example/prices.json",
		SourceChecksum: strings.Repeat("a", 64), SourceObservedAt: &observedAt,
		AutoUpdate: true, Enabled: true,
	}
	input := gatewayModelPriceInputForTest(price)
	input.Enabled = false
	input.AutoUpdate = false
	if !gatewayModelPriceValuesMatchInput(&price, input) {
		t.Fatal("flag-only update was treated as a manual price edit")
	}
	input.OutputUnitPriceUSD++
	if gatewayModelPriceValuesMatchInput(&price, input) {
		t.Fatal("price edit preserved automatic source metadata")
	}

	desired := price
	desired.CacheReadUnitPriceUSD = floatPointerForGatewayModelPriceTest(*price.CacheReadUnitPriceUSD)
	desired.CacheWriteUnitPriceUSD = floatPointerForGatewayModelPriceTest(*price.CacheWriteUnitPriceUSD)
	desired.SourceChecksum = strings.Repeat("b", 64)
	later := observedAt.Add(24 * time.Hour)
	desired.SourceObservedAt = &later
	if !gatewayModelPriceAutomaticValuesEqual(&price, &desired) {
		t.Fatal("unchanged model price created catalog-wide checksum audit churn")
	}
	desired.InputUnitPriceUSD++
	if gatewayModelPriceAutomaticValuesEqual(&price, &desired) {
		t.Fatal("changed automatic price was treated as unchanged")
	}
}

func TestApplyAutomaticModelPricesDoesNotOverwriteNewerObservation(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	newerAt := time.Now().UTC().Truncate(time.Microsecond)
	newer := GatewayAutomaticModelPriceInput{
		ModelName: "automatic-observation-order", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
		Source: GatewayModelPriceSourceLiteLLM, SourceURL: "https://catalog.example/prices.json",
		SourceChecksum: strings.Repeat("a", 64), SourceObservedAt: newerAt,
	}
	created, err := store.ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{newer}, "automatic-price-test")
	if err != nil || created.Created != 1 {
		t.Fatalf("create newer automatic price: result=%+v err=%v", created, err)
	}

	older := newer
	older.InputUnitPriceUSD = 1
	older.SourceChecksum = strings.Repeat("b", 64)
	older.SourceObservedAt = newerAt.Add(-time.Hour)
	ignored, err := store.ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{older}, "automatic-price-test")
	if err != nil || ignored.Unchanged != 1 || ignored.Updated != 0 {
		t.Fatalf("ignore older automatic price: result=%+v err=%v", ignored, err)
	}
	stored := loadGatewayModelPriceForTest(t, db, newer.ModelName)
	if stored.InputUnitPriceUSD != newer.InputUnitPriceUSD || stored.SourceObservedAt == nil || !stored.SourceObservedAt.Equal(newerAt) || stored.Revision != 1 {
		t.Fatalf("older observation overwrote newer price: %+v", stored)
	}

	conflicting := newer
	conflicting.OutputUnitPriceUSD++
	conflicting.SourceChecksum = strings.Repeat("c", 64)
	if _, err := store.ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{conflicting}, "automatic-price-test"); !errors.Is(err, ErrGatewayModelPriceConflict) {
		t.Fatalf("same-time conflicting observation error = %v", err)
	}
}

func TestApplyAutomaticModelPricesAdvancesUnchangedObservationWatermark(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	firstAt := time.Now().UTC().Truncate(time.Microsecond).Add(-2 * time.Hour)
	first := GatewayAutomaticModelPriceInput{
		ModelName: "automatic-unchanged-watermark", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
		Source: GatewayModelPriceSourceLiteLLM, SourceURL: "https://catalog.example/prices.json",
		SourceChecksum: strings.Repeat("a", 64), SourceObservedAt: firstAt,
	}
	created, err := store.ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{first}, "automatic-price-test")
	if err != nil || created.Created != 1 {
		t.Fatalf("create initial automatic price: result=%+v err=%v", created, err)
	}

	latest := first
	latest.SourceChecksum = strings.Repeat("b", 64)
	latest.SourceObservedAt = firstAt.Add(2 * time.Hour)
	unchanged, err := store.ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{latest}, "automatic-price-test")
	if err != nil || unchanged.Unchanged != 1 || unchanged.Updated != 0 {
		t.Fatalf("observe unchanged latest price: result=%+v err=%v", unchanged, err)
	}

	delayed := first
	delayed.InputUnitPriceUSD = 1
	delayed.SourceChecksum = strings.Repeat("c", 64)
	delayed.SourceObservedAt = firstAt.Add(time.Hour)
	ignored, err := store.ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{delayed}, "automatic-price-test")
	if err != nil || ignored.Unchanged != 1 || ignored.Updated != 0 {
		t.Fatalf("ignore delayed changed price: result=%+v err=%v", ignored, err)
	}

	stored := loadGatewayModelPriceForTest(t, db, first.ModelName)
	if stored.InputUnitPriceUSD != first.InputUnitPriceUSD || stored.Revision != 1 ||
		stored.SourceObservedAt == nil || !stored.SourceObservedAt.Equal(firstAt) {
		t.Fatalf("unchanged observations created catalog churn or restored stale values: %+v", stored)
	}
	var watermark gatewayModelPriceSourceWatermark
	if err := db.Where("model_name = ? AND source = ?", first.ModelName, GatewayModelPriceSourceLiteLLM).
		Take(&watermark).Error; err != nil {
		t.Fatalf("load automatic price watermark: %v", err)
	}
	if !watermark.SourceObservedAt.Equal(latest.SourceObservedAt) || watermark.SourceChecksum != latest.SourceChecksum {
		t.Fatalf("automatic price watermark = %+v, want observed_at=%v checksum=%s",
			watermark, latest.SourceObservedAt, latest.SourceChecksum)
	}
	var auditCount int64
	if err := db.Model(&GatewayModelPriceAudit{}).Where("model_price_id = ?", stored.ID).Count(&auditCount).Error; err != nil {
		t.Fatalf("count automatic price audits: %v", err)
	}
	if auditCount != 1 {
		t.Fatalf("unchanged automatic observations created %d audits, want 1", auditCount)
	}
}

func TestApplyAutomaticModelPricesSerializesFirstCreateBySourceWatermark(t *testing.T) {
	db := openTestDB(t)
	base := time.Now().UTC().Truncate(time.Microsecond).Add(-3 * time.Hour)
	modelName := "automatic-concurrent-first-create"
	seed := gatewayModelPriceSourceWatermark{
		ModelName: modelName, Source: GatewayModelPriceSourceLiteLLM,
		SourceURL: "https://catalog.example/prices.json", SourceChecksum: strings.Repeat("a", 64),
		SourceObservedAt: base, CreatedAt: base, UpdatedAt: base,
	}
	if err := db.Create(&seed).Error; err != nil {
		t.Fatalf("seed automatic price watermark: %v", err)
	}

	blockerPID, release, blockerDone := holdGatewayModelPriceWatermarkForTest(t, db, modelName)
	released := false
	defer func() {
		if !released {
			close(release)
			if err := <-blockerDone; err != nil {
				t.Errorf("release automatic price watermark: %v", err)
			}
		}
	}()

	olderDB, olderPID := reopenGatewayModelPriceWorkerForTest(t, db)
	newerDB, newerPID := reopenGatewayModelPriceWorkerForTest(t, db)
	older := GatewayAutomaticModelPriceInput{
		ModelName: modelName, InputUnitPriceUSD: 1, OutputUnitPriceUSD: 2,
		Source: GatewayModelPriceSourceLiteLLM, SourceURL: seed.SourceURL,
		SourceChecksum: strings.Repeat("b", 64), SourceObservedAt: base.Add(time.Hour),
	}
	newer := older
	newer.InputUnitPriceUSD = 5
	newer.OutputUnitPriceUSD = 30
	newer.SourceChecksum = strings.Repeat("c", 64)
	newer.SourceObservedAt = base.Add(2 * time.Hour)

	type applyResult struct {
		result GatewayAutomaticModelPriceResult
		err    error
	}
	results := make(chan applyResult, 2)
	go func() {
		result, err := NewGateway(olderDB).ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{older}, "automatic-price-test")
		results <- applyResult{result: result, err: err}
	}()
	if err := waitForGatewayModelPriceWatermarkWaiters(db, blockerPID, [2]int{olderPID, olderPID}, 1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	go func() {
		result, err := NewGateway(newerDB).ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{newer}, "automatic-price-test")
		results <- applyResult{result: result, err: err}
	}()
	if err := waitForGatewayModelPriceWatermarkWaiters(db, blockerPID, [2]int{olderPID, newerPID}, 2, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	close(release)
	released = true
	if err := <-blockerDone; err != nil {
		t.Fatalf("release automatic price watermark: %v", err)
	}
	for index := 0; index < 2; index++ {
		select {
		case applied := <-results:
			if applied.err != nil {
				t.Fatalf("concurrent automatic price apply %d: result=%+v err=%v", index+1, applied.result, applied.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent automatic price applies did not finish")
		}
	}

	stored := loadGatewayModelPriceForTest(t, db, modelName)
	if stored.InputUnitPriceUSD != newer.InputUnitPriceUSD || stored.OutputUnitPriceUSD != newer.OutputUnitPriceUSD ||
		stored.SourceObservedAt == nil || !stored.SourceObservedAt.Equal(newer.SourceObservedAt) || stored.Revision != 2 {
		t.Fatalf("serialized automatic price = %+v, want newer observation", stored)
	}
	var watermark gatewayModelPriceSourceWatermark
	if err := db.Where("model_name = ? AND source = ?", modelName, GatewayModelPriceSourceLiteLLM).Take(&watermark).Error; err != nil {
		t.Fatalf("load serialized automatic price watermark: %v", err)
	}
	if !watermark.SourceObservedAt.Equal(newer.SourceObservedAt) || watermark.SourceChecksum != newer.SourceChecksum {
		t.Fatalf("serialized automatic price watermark = %+v, want newer observation", watermark)
	}
}

func TestApplyAutomaticModelPricesClearsResultWhenBatchRollsBack(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	observedAt := time.Now().UTC().Truncate(time.Microsecond)
	existing := GatewayAutomaticModelPriceInput{
		ModelName: "z-automatic-batch-conflict", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
		Source: GatewayModelPriceSourceLiteLLM, SourceURL: "https://catalog.example/prices.json",
		SourceChecksum: strings.Repeat("a", 64), SourceObservedAt: observedAt,
	}
	if result, err := store.ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{existing}, "automatic-price-test"); err != nil || result.Created != 1 {
		t.Fatalf("seed conflicting automatic price: result=%+v err=%v", result, err)
	}
	createdThenRolledBack := existing
	createdThenRolledBack.ModelName = "a-automatic-batch-rollback"
	conflicting := existing
	conflicting.OutputUnitPriceUSD++
	conflicting.SourceChecksum = strings.Repeat("b", 64)

	result, err := store.ApplyAutomaticModelPrices(context.Background(), []GatewayAutomaticModelPriceInput{createdThenRolledBack, conflicting}, "automatic-price-test")
	if !errors.Is(err, ErrGatewayModelPriceConflict) {
		t.Fatalf("automatic price batch conflict error = %v", err)
	}
	if result.Created != 0 || result.Updated != 0 || result.Unchanged != 0 || result.ManualLocked != 0 ||
		len(result.CreatedModels) != 0 || len(result.UpdatedModels) != 0 {
		t.Fatalf("rolled-back automatic price result retained counts: %+v", result)
	}
	var count int64
	if err := db.Model(&GatewayModelPrice{}).Where("model_name = ?", createdThenRolledBack.ModelName).Count(&count).Error; err != nil {
		t.Fatalf("count rolled-back automatic price: %v", err)
	}
	if count != 0 {
		t.Fatalf("rolled-back automatic price count = %d, want 0", count)
	}
	if err := db.Model(&gatewayModelPriceSourceWatermark{}).Where("model_name = ?", createdThenRolledBack.ModelName).Count(&count).Error; err != nil {
		t.Fatalf("count rolled-back automatic price watermark: %v", err)
	}
	if count != 0 {
		t.Fatalf("rolled-back automatic price watermark count = %d, want 0", count)
	}
}

type gatewayModelPriceWatermarkHoldReady struct {
	pid int
	err error
}

func holdGatewayModelPriceWatermarkForTest(t *testing.T, db *gorm.DB, modelName string) (int, chan struct{}, <-chan error) {
	t.Helper()
	ready := make(chan gatewayModelPriceWatermarkHoldReady, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.Transaction(func(tx *gorm.DB) error {
			var watermark gatewayModelPriceSourceWatermark
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("model_name = ? AND source = ?", modelName, GatewayModelPriceSourceLiteLLM).
				Take(&watermark).Error; err != nil {
				ready <- gatewayModelPriceWatermarkHoldReady{err: err}
				return err
			}
			var pid int
			if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				ready <- gatewayModelPriceWatermarkHoldReady{err: err}
				return err
			}
			ready <- gatewayModelPriceWatermarkHoldReady{pid: pid}
			<-release
			return nil
		})
	}()
	select {
	case result := <-ready:
		if result.err != nil {
			close(release)
			t.Fatalf("hold automatic price watermark: %v", result.err)
		}
		return result.pid, release, done
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("timed out holding automatic price watermark")
		return 0, release, done
	}
}

func reopenGatewayModelPriceWorkerForTest(t *testing.T, db *gorm.DB) (*gorm.DB, int) {
	t.Helper()
	workerDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatalf("reopen automatic price worker database: %v", err)
	}
	sqlDB, err := workerDB.DB()
	if err != nil {
		t.Fatalf("open automatic price worker connection pool: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	var pid int
	if err := workerDB.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
		t.Fatalf("read automatic price worker backend pid: %v", err)
	}
	return workerDB, pid
}

func waitForGatewayModelPriceWatermarkWaiters(db *gorm.DB, blockerPID int, waiterPIDs [2]int, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var waiters int
		if err := db.Raw(`
			WITH RECURSIVE blocker_chain(waiter_pid, blocker_pid) AS (
				SELECT waiting.pid, blocker.pid
				FROM pg_stat_activity waiting
				CROSS JOIN LATERAL unnest(pg_blocking_pids(waiting.pid)) AS blocker(pid)
				WHERE waiting.datname = current_database()
				  AND waiting.pid IN (?, ?)
				  AND waiting.wait_event_type = 'Lock'
				UNION
				SELECT chain.waiter_pid, blocker.pid
				FROM blocker_chain chain
				CROSS JOIN LATERAL unnest(pg_blocking_pids(chain.blocker_pid)) AS blocker(pid)
				WHERE chain.blocker_pid <> ?
			)
			SELECT COUNT(DISTINCT waiter_pid)
			FROM blocker_chain
			WHERE blocker_pid = ?`, waiterPIDs[0], waiterPIDs[1], blockerPID, blockerPID).Scan(&waiters).Error; err != nil {
			return err
		}
		if waiters == expected {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for automatic price workers %v behind watermark blocker %d (got %d, want %d)", waiterPIDs, blockerPID, waiters, expected)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
