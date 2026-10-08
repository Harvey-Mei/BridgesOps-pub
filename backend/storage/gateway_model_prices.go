package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrGatewayModelPriceInvalid  = errors.New("gateway model price is invalid")
	ErrGatewayModelPriceConflict = errors.New("gateway model price changed")
	ErrGatewayModelPriceExists   = errors.New("gateway model price already exists")
)

type GatewayModelPrice struct {
	ID                                uint64     `gorm:"primaryKey" json:"id"`
	ModelName                         string     `gorm:"size:256;not null;uniqueIndex" json:"model_name"`
	InputUnitPriceUSD                 float64    `gorm:"not null" json:"input_unit_price_usd"`
	OutputUnitPriceUSD                float64    `gorm:"not null" json:"output_unit_price_usd"`
	CacheReadUnitPriceUSD             *float64   `json:"cache_read_unit_price_usd"`
	CacheWriteUnitPriceUSD            *float64   `json:"cache_write_unit_price_usd"`
	LongContextThresholdTokens        *int64     `json:"long_context_threshold_tokens,omitempty"`
	LongContextInputUnitPriceUSD      *float64   `json:"long_context_input_unit_price_usd,omitempty"`
	LongContextOutputUnitPriceUSD     *float64   `json:"long_context_output_unit_price_usd,omitempty"`
	LongContextCacheReadUnitPriceUSD  *float64   `json:"long_context_cache_read_unit_price_usd,omitempty"`
	LongContextCacheWriteUnitPriceUSD *float64   `json:"long_context_cache_write_unit_price_usd,omitempty"`
	Source                            string     `gorm:"size:32;not null;default:'manual'" json:"source"`
	SourceURL                         string     `gorm:"size:512;not null;default:''" json:"source_url,omitempty"`
	SourceChecksum                    string     `gorm:"size:64;not null;default:''" json:"source_checksum,omitempty"`
	SourceObservedAt                  *time.Time `json:"source_observed_at,omitempty"`
	AutoUpdate                        bool       `gorm:"not null;default:false" json:"auto_update"`
	Enabled                           bool       `gorm:"not null" json:"enabled"`
	Revision                          uint64     `gorm:"not null;default:1" json:"revision"`
	CreatedAt                         time.Time  `gorm:"not null" json:"created_at"`
	UpdatedAt                         time.Time  `gorm:"not null" json:"updated_at"`
}

func (GatewayModelPrice) TableName() string { return "gateway_model_prices" }

type GatewayModelPriceAudit struct {
	ID                                uint64  `gorm:"primaryKey"`
	ModelPriceID                      uint64  `gorm:"not null"`
	Action                            string  `gorm:"size:16;not null"`
	Actor                             string  `gorm:"size:128;not null"`
	Revision                          uint64  `gorm:"not null"`
	ModelName                         string  `gorm:"size:256;not null"`
	InputUnitPriceUSD                 float64 `gorm:"not null"`
	OutputUnitPriceUSD                float64 `gorm:"not null"`
	CacheReadUnitPriceUSD             *float64
	CacheWriteUnitPriceUSD            *float64
	LongContextThresholdTokens        *int64
	LongContextInputUnitPriceUSD      *float64
	LongContextOutputUnitPriceUSD     *float64
	LongContextCacheReadUnitPriceUSD  *float64
	LongContextCacheWriteUnitPriceUSD *float64
	Source                            string `gorm:"size:32;not null"`
	SourceURL                         string `gorm:"size:512;not null"`
	SourceChecksum                    string `gorm:"size:64;not null"`
	SourceObservedAt                  *time.Time
	AutoUpdate                        bool      `gorm:"not null"`
	Enabled                           bool      `gorm:"not null"`
	CreatedAt                         time.Time `gorm:"not null"`
}

func (GatewayModelPriceAudit) TableName() string { return "gateway_model_price_audits" }

type GatewayModelPriceInput struct {
	ModelName                         string
	InputUnitPriceUSD                 float64
	OutputUnitPriceUSD                float64
	CacheReadUnitPriceUSD             *float64
	CacheWriteUnitPriceUSD            *float64
	LongContextThresholdTokens        *int64
	LongContextInputUnitPriceUSD      *float64
	LongContextOutputUnitPriceUSD     *float64
	LongContextCacheReadUnitPriceUSD  *float64
	LongContextCacheWriteUnitPriceUSD *float64
	AutoUpdate                        bool
	Enabled                           bool
}

func normalizeGatewayModelPriceInput(input *GatewayModelPriceInput) error {
	if input == nil {
		return ErrGatewayModelPriceInvalid
	}
	input.ModelName = strings.TrimSpace(input.ModelName)
	if input.ModelName == "" || strings.ContainsRune(input.ModelName, '\x00') ||
		!utf8.ValidString(input.ModelName) || utf8.RuneCountInString(input.ModelName) > 256 {
		return ErrGatewayModelPriceInvalid
	}
	for _, value := range []float64{input.InputUnitPriceUSD, input.OutputUnitPriceUSD} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return ErrGatewayModelPriceInvalid
		}
	}
	for _, value := range []*float64{input.CacheReadUnitPriceUSD, input.CacheWriteUnitPriceUSD} {
		if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return ErrGatewayModelPriceInvalid
		}
	}
	longConfigured := input.LongContextThresholdTokens != nil
	if (input.LongContextInputUnitPriceUSD != nil) != longConfigured ||
		(input.LongContextOutputUnitPriceUSD != nil) != longConfigured ||
		(!longConfigured && (input.LongContextCacheReadUnitPriceUSD != nil || input.LongContextCacheWriteUnitPriceUSD != nil)) {
		return ErrGatewayModelPriceInvalid
	}
	for _, value := range []*float64{
		input.LongContextInputUnitPriceUSD, input.LongContextOutputUnitPriceUSD,
		input.LongContextCacheReadUnitPriceUSD, input.LongContextCacheWriteUnitPriceUSD,
	} {
		if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return ErrGatewayModelPriceInvalid
		}
	}
	if longConfigured && *input.LongContextThresholdTokens != GatewayRequestPriceLongContextThreshold {
		return ErrGatewayModelPriceInvalid
	}
	return nil
}

func normalizeGatewayModelPriceActor(actor string) (string, error) {
	actor = strings.TrimSpace(actor)
	if actor == "" || strings.ContainsRune(actor, '\x00') || !utf8.ValidString(actor) || utf8.RuneCountInString(actor) > 128 {
		return "", ErrGatewayModelPriceInvalid
	}
	return actor, nil
}

func gatewayModelPriceAudit(price GatewayModelPrice, action, actor string, at time.Time) GatewayModelPriceAudit {
	return GatewayModelPriceAudit{
		ModelPriceID: price.ID, Action: action, Actor: actor, Revision: price.Revision,
		ModelName: price.ModelName, InputUnitPriceUSD: price.InputUnitPriceUSD,
		OutputUnitPriceUSD: price.OutputUnitPriceUSD, CacheReadUnitPriceUSD: price.CacheReadUnitPriceUSD,
		CacheWriteUnitPriceUSD:            price.CacheWriteUnitPriceUSD,
		LongContextThresholdTokens:        price.LongContextThresholdTokens,
		LongContextInputUnitPriceUSD:      price.LongContextInputUnitPriceUSD,
		LongContextOutputUnitPriceUSD:     price.LongContextOutputUnitPriceUSD,
		LongContextCacheReadUnitPriceUSD:  price.LongContextCacheReadUnitPriceUSD,
		LongContextCacheWriteUnitPriceUSD: price.LongContextCacheWriteUnitPriceUSD,
		Source:                            price.Source, SourceURL: price.SourceURL, SourceChecksum: price.SourceChecksum,
		SourceObservedAt: price.SourceObservedAt, AutoUpdate: price.AutoUpdate,
		Enabled: price.Enabled, CreatedAt: at,
	}
}

func gatewayModelPriceValuesMatchInput(price *GatewayModelPrice, input GatewayModelPriceInput) bool {
	if price == nil {
		return false
	}
	return price.ModelName == input.ModelName &&
		price.InputUnitPriceUSD == input.InputUnitPriceUSD &&
		price.OutputUnitPriceUSD == input.OutputUnitPriceUSD &&
		equalGatewayModelPriceFloat(price.CacheReadUnitPriceUSD, input.CacheReadUnitPriceUSD) &&
		equalGatewayModelPriceFloat(price.CacheWriteUnitPriceUSD, input.CacheWriteUnitPriceUSD) &&
		equalGatewayModelPriceInt64(price.LongContextThresholdTokens, input.LongContextThresholdTokens) &&
		equalGatewayModelPriceFloat(price.LongContextInputUnitPriceUSD, input.LongContextInputUnitPriceUSD) &&
		equalGatewayModelPriceFloat(price.LongContextOutputUnitPriceUSD, input.LongContextOutputUnitPriceUSD) &&
		equalGatewayModelPriceFloat(price.LongContextCacheReadUnitPriceUSD, input.LongContextCacheReadUnitPriceUSD) &&
		equalGatewayModelPriceFloat(price.LongContextCacheWriteUnitPriceUSD, input.LongContextCacheWriteUnitPriceUSD)
}

func (g *Gateway) ListModelPrices() ([]GatewayModelPrice, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	var prices []GatewayModelPrice
	if err := g.db.Order("model_name ASC, id ASC").Find(&prices).Error; err != nil {
		return nil, err
	}
	return prices, nil
}

func (g *Gateway) CreateModelPrice(input GatewayModelPriceInput, actor string) (*GatewayModelPrice, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	if err := normalizeGatewayModelPriceInput(&input); err != nil {
		return nil, err
	}
	actor, err := normalizeGatewayModelPriceActor(actor)
	if err != nil {
		return nil, err
	}
	now := normalizeGatewayRequestPricingTime(time.Now())
	price := &GatewayModelPrice{
		ModelName: input.ModelName, InputUnitPriceUSD: input.InputUnitPriceUSD,
		OutputUnitPriceUSD: input.OutputUnitPriceUSD, CacheReadUnitPriceUSD: input.CacheReadUnitPriceUSD,
		CacheWriteUnitPriceUSD:            input.CacheWriteUnitPriceUSD,
		LongContextThresholdTokens:        input.LongContextThresholdTokens,
		LongContextInputUnitPriceUSD:      input.LongContextInputUnitPriceUSD,
		LongContextOutputUnitPriceUSD:     input.LongContextOutputUnitPriceUSD,
		LongContextCacheReadUnitPriceUSD:  input.LongContextCacheReadUnitPriceUSD,
		LongContextCacheWriteUnitPriceUSD: input.LongContextCacheWriteUnitPriceUSD,
		Source:                            "manual", AutoUpdate: input.AutoUpdate, Enabled: input.Enabled,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	err = g.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Select(
			"model_name", "input_unit_price_usd", "output_unit_price_usd",
			"cache_read_unit_price_usd", "cache_write_unit_price_usd",
			"long_context_threshold_tokens", "long_context_input_unit_price_usd",
			"long_context_output_unit_price_usd", "long_context_cache_read_unit_price_usd",
			"long_context_cache_write_unit_price_usd", "source", "source_url", "source_checksum",
			"source_observed_at", "auto_update", "enabled",
			"revision", "created_at", "updated_at",
		).Create(price).Error; err != nil {
			if gatewayModelPriceDuplicateError(err) {
				return ErrGatewayModelPriceExists
			}
			return err
		}
		audit := gatewayModelPriceAudit(*price, "create", actor, now)
		return tx.Create(&audit).Error
	})
	return price, err
}

func (g *Gateway) UpdateModelPrice(id, expectedRevision uint64, input GatewayModelPriceInput, actor string) (*GatewayModelPrice, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	if id == 0 || expectedRevision == 0 || expectedRevision == math.MaxUint64 {
		return nil, ErrGatewayModelPriceInvalid
	}
	if err := normalizeGatewayModelPriceInput(&input); err != nil {
		return nil, err
	}
	actor, err := normalizeGatewayModelPriceActor(actor)
	if err != nil {
		return nil, err
	}
	var updated GatewayModelPrice
	err = g.db.Transaction(func(tx *gorm.DB) error {
		var current GatewayModelPrice
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, id).Error; err != nil {
			return err
		}
		if current.Revision != expectedRevision {
			return ErrGatewayModelPriceConflict
		}
		now := normalizeGatewayRequestPricingTime(time.Now())
		updated = current
		updated.ModelName = input.ModelName
		updated.InputUnitPriceUSD = input.InputUnitPriceUSD
		updated.OutputUnitPriceUSD = input.OutputUnitPriceUSD
		updated.CacheReadUnitPriceUSD = input.CacheReadUnitPriceUSD
		updated.CacheWriteUnitPriceUSD = input.CacheWriteUnitPriceUSD
		updated.LongContextThresholdTokens = input.LongContextThresholdTokens
		updated.LongContextInputUnitPriceUSD = input.LongContextInputUnitPriceUSD
		updated.LongContextOutputUnitPriceUSD = input.LongContextOutputUnitPriceUSD
		updated.LongContextCacheReadUnitPriceUSD = input.LongContextCacheReadUnitPriceUSD
		updated.LongContextCacheWriteUnitPriceUSD = input.LongContextCacheWriteUnitPriceUSD
		if !gatewayModelPriceValuesMatchInput(&current, input) {
			updated.Source = GatewayModelPriceSourceManual
			updated.SourceURL = ""
			updated.SourceChecksum = ""
			updated.SourceObservedAt = nil
		}
		updated.AutoUpdate = input.AutoUpdate
		updated.Enabled = input.Enabled
		updated.Revision++
		updated.UpdatedAt = now
		result := tx.Model(&GatewayModelPrice{}).Where("id = ? AND revision = ?", id, expectedRevision).Updates(map[string]any{
			"model_name": input.ModelName, "input_unit_price_usd": input.InputUnitPriceUSD,
			"output_unit_price_usd": input.OutputUnitPriceUSD, "cache_read_unit_price_usd": input.CacheReadUnitPriceUSD,
			"cache_write_unit_price_usd":              input.CacheWriteUnitPriceUSD,
			"long_context_threshold_tokens":           input.LongContextThresholdTokens,
			"long_context_input_unit_price_usd":       input.LongContextInputUnitPriceUSD,
			"long_context_output_unit_price_usd":      input.LongContextOutputUnitPriceUSD,
			"long_context_cache_read_unit_price_usd":  input.LongContextCacheReadUnitPriceUSD,
			"long_context_cache_write_unit_price_usd": input.LongContextCacheWriteUnitPriceUSD,
			"source": updated.Source, "source_url": updated.SourceURL,
			"source_checksum": updated.SourceChecksum, "source_observed_at": updated.SourceObservedAt,
			"auto_update": input.AutoUpdate, "enabled": input.Enabled,
			"revision": updated.Revision, "updated_at": now,
		})
		if result.Error != nil {
			if gatewayModelPriceDuplicateError(result.Error) {
				return ErrGatewayModelPriceExists
			}
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayModelPriceConflict
		}
		audit := gatewayModelPriceAudit(updated, "update", actor, now)
		return tx.Create(&audit).Error
	})
	return &updated, err
}

func (g *Gateway) DeleteModelPrice(id, expectedRevision uint64, actor string) error {
	if g == nil || g.db == nil {
		return errors.New("gateway storage is required")
	}
	if id == 0 || expectedRevision == 0 || expectedRevision == math.MaxUint64 {
		return ErrGatewayModelPriceInvalid
	}
	actor, err := normalizeGatewayModelPriceActor(actor)
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		var current GatewayModelPrice
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, id).Error; err != nil {
			return err
		}
		if current.Revision != expectedRevision || current.Revision == math.MaxUint64 {
			return ErrGatewayModelPriceConflict
		}
		now := normalizeGatewayRequestPricingTime(time.Now())
		current.Revision++
		audit := gatewayModelPriceAudit(current, "delete", actor, now)
		if err := tx.Create(&audit).Error; err != nil {
			return err
		}
		result := tx.Where("id = ? AND revision = ?", id, expectedRevision).Delete(&GatewayModelPrice{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayModelPriceConflict
		}
		return nil
	})
}

func findEnabledGatewayModelPriceTx(tx *gorm.DB, modelName string) (*GatewayModelPrice, error) {
	modelName = strings.TrimSpace(modelName)
	if tx == nil || modelName == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var price GatewayModelPrice
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where("model_name = ? AND enabled = ?", modelName, true).Take(&price).Error; err != nil {
		return nil, err
	}
	return &price, nil
}

func gatewayModelPriceDuplicateError(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var state interface{ SQLState() string }
	return errors.As(err, &state) && state.SQLState() == "23505"
}

func resolveGatewayRequestPriceFactWithModelConfigTx(
	tx *gorm.DB,
	scope GatewayRequestPriceScope,
	costUnitEpoch uint64,
	groupRatio float64,
	completionRatio float64,
	rawGroupRatio *float64,
	at time.Time,
) (GatewayRequestPriceFactLookup, error) {
	priority, found, err := findPriorityGatewayRequestPriceFactTx(tx, scope, costUnitEpoch, at)
	if err != nil || found {
		return priority, err
	}
	if scope.Operation != GatewayPricingOperationTextGeneration {
		return findCurrentGatewayRequestPriceFactForEpochTx(tx, scope, costUnitEpoch, at)
	}
	if groupRatio < 0 || math.IsNaN(groupRatio) || math.IsInf(groupRatio, 0) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactMissing}, nil
	}

	desired, err := desiredGatewayRequestPriceFactTx(
		tx, scope, costUnitEpoch, groupRatio, completionRatio, rawGroupRatio, at,
	)
	if err != nil {
		return GatewayRequestPriceFactLookup{}, err
	}
	if desired == nil {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactMissing}, nil
	}
	if err := lockGatewayRequestModelPriceScopeTx(tx, scope, costUnitEpoch); err != nil {
		return GatewayRequestPriceFactLookup{}, err
	}
	resolveAt, err := gatewayRequestPriceImportServerTimeTx(tx)
	if err != nil {
		return GatewayRequestPriceFactLookup{}, err
	}

	// The administrator importer does not take this advisory lock. Recheck its
	// higher-priority facts after waiting, then use conflict-safe inserts below
	// to cover an import that commits between this lookup and our INSERT.
	priority, found, err = findPriorityGatewayRequestPriceFactTx(tx, scope, costUnitEpoch, resolveAt)
	if err != nil || found {
		return priority, err
	}
	for attempt := 0; attempt < 4; attempt++ {
		lookup, lookupErr := findCurrentGatewayRequestPriceFactForEpochTx(tx, scope, costUnitEpoch, resolveAt)
		if lookupErr != nil {
			return GatewayRequestPriceFactLookup{}, lookupErr
		}
		if lookup.Fact != nil && gatewayRequestGeneratedFactMatches(lookup.Fact, desired) {
			return lookup, nil
		}
		if lookup.Fact == nil && lookup.UnavailableReason != GatewayPricingUnavailableFactMissing &&
			lookup.UnavailableReason != GatewayPricingUnavailableFactExpired {
			return lookup, nil
		}

		effectiveAt, timeErr := gatewayRequestPriceImportServerTimeTx(tx)
		if timeErr != nil {
			return GatewayRequestPriceFactLookup{}, timeErr
		}
		candidate := *desired
		candidate.ID = 0
		candidate.FactVersion = 0
		candidate.Checksum = ""
		candidate.CreatedAt = time.Time{}
		candidate.ObservedAt = effectiveAt
		candidate.ValidFrom = effectiveAt
		appendErr := appendGeneratedRequestPriceFactTx(tx, &candidate, effectiveAt)
		if appendErr == nil {
			priority, found, err = findPriorityGatewayRequestPriceFactTx(tx, scope, costUnitEpoch, effectiveAt)
			if err != nil || found {
				return priority, err
			}
			return GatewayRequestPriceFactLookup{Fact: &candidate}, nil
		}
		if !errors.Is(appendErr, errGatewayRequestPriceAppendConflict) &&
			!errors.Is(appendErr, ErrGatewayRequestPriceObservationStale) &&
			!errors.Is(appendErr, ErrGatewayRequestPriceObservationConflict) {
			return GatewayRequestPriceFactLookup{}, appendErr
		}
		resolveAt, err = gatewayRequestPriceImportServerTimeTx(tx)
		if err != nil {
			return GatewayRequestPriceFactLookup{}, err
		}
		priority, found, err = findPriorityGatewayRequestPriceFactTx(tx, scope, costUnitEpoch, resolveAt)
		if err != nil || found {
			return priority, err
		}
	}
	return GatewayRequestPriceFactLookup{}, errors.New("gateway model price fact append did not converge")
}

func findPriorityGatewayRequestPriceFactTx(
	tx *gorm.DB,
	scope GatewayRequestPriceScope,
	costUnitEpoch uint64,
	at time.Time,
) (GatewayRequestPriceFactLookup, bool, error) {
	at = normalizeGatewayRequestPricingTime(at)
	if at.IsZero() {
		at = time.Now().UTC()
	}
	var fact GatewayRequestPriceFact
	err := gatewayRequestPriceScopeEpochQuery(tx.Model(&GatewayRequestPriceFact{}), scope, costUnitEpoch).
		Where("valid_from <= ?", at).
		Where("source = ?", GatewayRequestPriceSourceAdministrator).
		Order("fact_version DESC, valid_from DESC, id DESC").
		First(&fact).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = gatewayRequestPriceScopeEpochQuery(tx.Model(&GatewayRequestPriceFact{}), scope, costUnitEpoch).
			Where("valid_from <= ?", at).
			Where("source NOT IN ?", []string{
				GatewayRequestPriceSourceAdministrator,
				gatewayRequestPriceSourceModelConfig,
				gatewayChannelModelPriceSource,
			}).
			Order("fact_version DESC, valid_from DESC, id DESC").
			First(&fact).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return GatewayRequestPriceFactLookup{}, false, nil
		}
	}
	if err != nil {
		return GatewayRequestPriceFactLookup{}, false, err
	}
	if fact.ValidUntil != nil && !fact.ValidUntil.After(at) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactExpired}, true, nil
	}
	if !gatewayRequestPriceFactSchemaSupported(fact.SchemaVersion) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableSchemaUnsupported}, true, nil
	}
	checksum, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		return GatewayRequestPriceFactLookup{}, true, err
	}
	if !strings.EqualFold(checksum, fact.Checksum) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableChecksumMismatch}, true, nil
	}
	return GatewayRequestPriceFactLookup{Fact: &fact}, true, nil
}

func gatewayRequestPriceFactFromModelConfig(
	scope GatewayRequestPriceScope,
	costUnitEpoch uint64,
	groupRatio float64,
	completionRatio float64,
	price *GatewayModelPrice,
	at time.Time,
) (*GatewayRequestPriceFact, bool) {
	if price == nil || !price.Enabled || price.ModelName != scope.ModelName || costUnitEpoch == 0 {
		return nil, false
	}
	if groupRatio < 0 || math.IsNaN(groupRatio) || math.IsInf(groupRatio, 0) {
		return nil, false
	}
	if completionRatio <= 0 || math.IsNaN(completionRatio) || math.IsInf(completionRatio, 0) {
		completionRatio = groupRatio
	}
	basePrices := []float64{price.InputUnitPriceUSD, price.OutputUnitPriceUSD}
	for _, value := range basePrices {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, false
		}
	}
	for _, value := range []*float64{price.CacheReadUnitPriceUSD, price.CacheWriteUnitPriceUSD} {
		if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return nil, false
		}
	}
	at = normalizeGatewayRequestPricingTime(at)
	fact := &GatewayRequestPriceFact{
		ChannelID: scope.ChannelID, ChannelAccountIdentityRevision: scope.ChannelAccountIdentityRevision,
		CostUnitEpoch: costUnitEpoch, UpstreamGroupID: scope.UpstreamGroupID,
		CanonicalGroupName: scope.CanonicalGroupName, UpstreamGroupPresenceGeneration: scope.UpstreamGroupPresenceGeneration,
		GatewayUpstreamKeyID: scope.GatewayUpstreamKeyID, UpstreamKeyGeneration: scope.UpstreamKeyGeneration,
		ModelName: scope.ModelName, Operation: scope.Operation, Protocol: scope.Protocol,
		Unit: GatewayPricingUnitPerMillionTokens, Currency: gatewayRequestPriceSupportedCurrency,
		Source: gatewayRequestPriceSourceModelConfig, ObservedAt: at, ValidFrom: at,
	}

	if price.LongContextThresholdTokens == nil {
		effectivePrices := []float64{price.InputUnitPriceUSD * groupRatio, price.OutputUnitPriceUSD * completionRatio}
		for _, value := range effectivePrices {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, false
			}
		}
		fact.InputUnitPrice = &effectivePrices[0]
		fact.OutputUnitPrice = &effectivePrices[1]
		if price.CacheReadUnitPriceUSD != nil {
			cacheRead := *price.CacheReadUnitPriceUSD * groupRatio
			if math.IsNaN(cacheRead) || math.IsInf(cacheRead, 0) {
				return nil, false
			}
			fact.CacheReadUnitPrice = &cacheRead
		}
		if price.CacheWriteUnitPriceUSD != nil {
			cacheWrite := *price.CacheWriteUnitPriceUSD * groupRatio
			if math.IsNaN(cacheWrite) || math.IsInf(cacheWrite, 0) {
				return nil, false
			}
			fact.CacheWriteUnitPrice = &cacheWrite
		}
		fact.SchemaVersion = GatewayRequestPriceFactSchemaVersion
		return fact, true
	}

	if *price.LongContextThresholdTokens != GatewayRequestPriceLongContextThreshold {
		return nil, false
	}
	if price.LongContextInputUnitPriceUSD == nil || price.LongContextOutputUnitPriceUSD == nil {
		return nil, false
	}
	for _, value := range []*float64{
		price.LongContextInputUnitPriceUSD, price.LongContextOutputUnitPriceUSD,
		price.LongContextCacheReadUnitPriceUSD, price.LongContextCacheWriteUnitPriceUSD,
	} {
		if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return nil, false
		}
	}
	threshold := *price.LongContextThresholdTokens
	fact.InputUnitPrice = &basePrices[0]
	fact.OutputUnitPrice = &basePrices[1]
	fact.CacheReadUnitPrice = cloneGatewayRequestDispatchNumber(price.CacheReadUnitPriceUSD)
	fact.CacheWriteUnitPrice = cloneGatewayRequestDispatchNumber(price.CacheWriteUnitPriceUSD)
	fact.LongContextThresholdTokens = &threshold
	fact.LongContextInputUnitPrice = cloneGatewayRequestDispatchNumber(price.LongContextInputUnitPriceUSD)
	fact.LongContextOutputUnitPrice = cloneGatewayRequestDispatchNumber(price.LongContextOutputUnitPriceUSD)
	fact.LongContextCacheReadUnitPrice = cloneGatewayRequestDispatchNumber(price.LongContextCacheReadUnitPriceUSD)
	fact.LongContextCacheWriteUnitPrice = cloneGatewayRequestDispatchNumber(price.LongContextCacheWriteUnitPriceUSD)
	fact.GroupRatio = &groupRatio
	fact.CompletionRatio = &completionRatio
	fact.SchemaVersion = GatewayRequestPriceFactTieredSchemaVersion
	return fact, true
}

func desiredGatewayRequestPriceFactTx(
	tx *gorm.DB,
	scope GatewayRequestPriceScope,
	costUnitEpoch uint64,
	groupRatio float64,
	completionRatio float64,
	rawGroupRatio *float64,
	at time.Time,
) (*GatewayRequestPriceFact, error) {
	channelPrice, err := findGatewayChannelModelPriceTx(tx, scope, costUnitEpoch)
	if err == nil {
		if desired, ok := gatewayRequestPriceFactFromChannelPrice(
			scope, costUnitEpoch, groupRatio, completionRatio, rawGroupRatio, channelPrice, at,
		); ok {
			return desired, nil
		}
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	price, err := findEnabledGatewayModelPriceTx(tx, scope.ModelName)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	desired, ok := gatewayRequestPriceFactFromModelConfig(
		scope, costUnitEpoch, groupRatio, completionRatio, price, at,
	)
	if !ok {
		return nil, nil
	}
	return desired, nil
}

func gatewayRequestPriceFactFromChannelPrice(
	scope GatewayRequestPriceScope,
	costUnitEpoch uint64,
	groupRatio float64,
	completionRatio float64,
	rawGroupRatio *float64,
	price *GatewayChannelModelPrice,
	at time.Time,
) (*GatewayRequestPriceFact, bool) {
	if price == nil || costUnitEpoch == 0 || price.ChannelID != scope.ChannelID ||
		price.ChannelIdentityRevision != scope.ChannelAccountIdentityRevision ||
		price.CostUnitEpoch != costUnitEpoch || price.GatewayUpstreamGroupID != scope.UpstreamGroupID ||
		price.UpstreamGroupPresenceGeneration != scope.UpstreamGroupPresenceGeneration || price.ModelName != scope.ModelName ||
		!strings.EqualFold(strings.TrimSpace(price.UpstreamGroupName), strings.TrimSpace(scope.CanonicalGroupName)) ||
		rawGroupRatio == nil || !financiallyEqual(price.GroupRatio, *rawGroupRatio) {
		return nil, false
	}
	if groupRatio < 0 || math.IsNaN(groupRatio) || math.IsInf(groupRatio, 0) {
		return nil, false
	}
	// Channel catalogs already publish independent input and output base prices.
	// The group's effective multiplier applies to both; applying the legacy
	// completion multiplier again would double-count the model output ratio.
	completionRatio = groupRatio
	for _, value := range []float64{price.InputUnitPriceUSD, price.OutputUnitPriceUSD} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, false
		}
	}
	for _, value := range []*float64{
		price.CacheReadUnitPriceUSD, price.CacheWriteUnitPriceUSD,
		price.LongContextInputUnitPriceUSD, price.LongContextOutputUnitPriceUSD,
		price.LongContextCacheReadUnitPriceUSD, price.LongContextCacheWriteUnitPriceUSD,
	} {
		if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
			return nil, false
		}
	}

	at = normalizeGatewayRequestPricingTime(at)
	fact := &GatewayRequestPriceFact{
		ChannelID: scope.ChannelID, ChannelAccountIdentityRevision: scope.ChannelAccountIdentityRevision,
		CostUnitEpoch: costUnitEpoch, UpstreamGroupID: scope.UpstreamGroupID,
		CanonicalGroupName: scope.CanonicalGroupName, UpstreamGroupPresenceGeneration: scope.UpstreamGroupPresenceGeneration,
		GatewayUpstreamKeyID: scope.GatewayUpstreamKeyID, UpstreamKeyGeneration: scope.UpstreamKeyGeneration,
		ModelName: scope.ModelName, Operation: scope.Operation, Protocol: scope.Protocol,
		Unit: GatewayPricingUnitPerMillionTokens, Currency: gatewayRequestPriceSupportedCurrency,
		Source: gatewayChannelModelPriceSource, ObservedAt: at, ValidFrom: at,
	}

	longContextConfigured := price.LongContextBasis != "" || price.LongContextThresholdTokens != nil ||
		price.LongContextInputUnitPriceUSD != nil || price.LongContextOutputUnitPriceUSD != nil ||
		price.LongContextCacheReadUnitPriceUSD != nil || price.LongContextCacheWriteUnitPriceUSD != nil
	if !longContextConfigured {
		input := price.InputUnitPriceUSD * groupRatio
		output := price.OutputUnitPriceUSD * completionRatio
		if math.IsNaN(input) || math.IsInf(input, 0) || math.IsNaN(output) || math.IsInf(output, 0) {
			return nil, false
		}
		fact.InputUnitPrice = &input
		fact.OutputUnitPrice = &output
		if price.CacheReadUnitPriceUSD != nil {
			cacheRead := *price.CacheReadUnitPriceUSD * groupRatio
			if math.IsNaN(cacheRead) || math.IsInf(cacheRead, 0) {
				return nil, false
			}
			fact.CacheReadUnitPrice = &cacheRead
		}
		if price.CacheWriteUnitPriceUSD != nil {
			cacheWrite := *price.CacheWriteUnitPriceUSD * groupRatio
			if math.IsNaN(cacheWrite) || math.IsInf(cacheWrite, 0) {
				return nil, false
			}
			fact.CacheWriteUnitPrice = &cacheWrite
		}
		fact.SchemaVersion = GatewayRequestPriceFactSchemaVersion
		return fact, true
	}

	if price.LongContextBasis != "whole_request" || price.LongContextThresholdTokens == nil ||
		!validGatewayRequestPriceLongContextThreshold(*price.LongContextThresholdTokens) ||
		price.LongContextInputUnitPriceUSD == nil || price.LongContextOutputUnitPriceUSD == nil {
		return nil, false
	}
	threshold := *price.LongContextThresholdTokens
	fact.InputUnitPrice = cloneGatewayRequestDispatchNumber(&price.InputUnitPriceUSD)
	fact.OutputUnitPrice = cloneGatewayRequestDispatchNumber(&price.OutputUnitPriceUSD)
	fact.CacheReadUnitPrice = cloneGatewayRequestDispatchNumber(price.CacheReadUnitPriceUSD)
	fact.CacheWriteUnitPrice = cloneGatewayRequestDispatchNumber(price.CacheWriteUnitPriceUSD)
	fact.LongContextThresholdTokens = &threshold
	fact.LongContextInputUnitPrice = cloneGatewayRequestDispatchNumber(price.LongContextInputUnitPriceUSD)
	fact.LongContextOutputUnitPrice = cloneGatewayRequestDispatchNumber(price.LongContextOutputUnitPriceUSD)
	fact.LongContextCacheReadUnitPrice = cloneGatewayRequestDispatchNumber(price.LongContextCacheReadUnitPriceUSD)
	fact.LongContextCacheWriteUnitPrice = cloneGatewayRequestDispatchNumber(price.LongContextCacheWriteUnitPriceUSD)
	fact.GroupRatio = cloneGatewayRequestDispatchNumber(&groupRatio)
	fact.CompletionRatio = cloneGatewayRequestDispatchNumber(&completionRatio)
	fact.SchemaVersion = GatewayRequestPriceFactTieredSchemaVersion
	return fact, true
}

func gatewayRequestModelConfigFactMatches(current, desired *GatewayRequestPriceFact) bool {
	return current != nil && current.Source == gatewayRequestPriceSourceModelConfig &&
		desired != nil && desired.Source == gatewayRequestPriceSourceModelConfig &&
		gatewayRequestGeneratedFactMatches(current, desired)
}

func gatewayRequestGeneratedFactMatches(current, desired *GatewayRequestPriceFact) bool {
	if current == nil || desired == nil || current.Source != desired.Source ||
		current.SchemaVersion != desired.SchemaVersion || current.Unit != desired.Unit || current.Currency != desired.Currency {
		return false
	}
	return gatewayRequestPriceNumberEqual(current.InputUnitPrice, desired.InputUnitPrice) &&
		gatewayRequestPriceNumberEqual(current.OutputUnitPrice, desired.OutputUnitPrice) &&
		gatewayRequestPriceNumberEqual(current.CacheReadUnitPrice, desired.CacheReadUnitPrice) &&
		gatewayRequestPriceNumberEqual(current.CacheWriteUnitPrice, desired.CacheWriteUnitPrice) &&
		equalGatewayModelPriceInt64(current.LongContextThresholdTokens, desired.LongContextThresholdTokens) &&
		gatewayRequestPriceNumberEqual(current.LongContextInputUnitPrice, desired.LongContextInputUnitPrice) &&
		gatewayRequestPriceNumberEqual(current.LongContextOutputUnitPrice, desired.LongContextOutputUnitPrice) &&
		gatewayRequestPriceNumberEqual(current.LongContextCacheReadUnitPrice, desired.LongContextCacheReadUnitPrice) &&
		gatewayRequestPriceNumberEqual(current.LongContextCacheWriteUnitPrice, desired.LongContextCacheWriteUnitPrice) &&
		gatewayRequestPriceNumberEqual(current.GroupRatio, desired.GroupRatio) &&
		gatewayRequestPriceNumberEqual(current.CompletionRatio, desired.CompletionRatio) &&
		gatewayRequestPriceNumberEqual(current.OperationUnitPrice, desired.OperationUnitPrice)
}

func gatewayRequestPriceNumberEqual(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func lockGatewayRequestModelPriceScopeTx(tx *gorm.DB, scope GatewayRequestPriceScope, costUnitEpoch uint64) error {
	if tx == nil {
		return errors.New("gateway storage transaction is required")
	}
	if tx.Dialector == nil || tx.Dialector.Name() != string(DBDriverPostgres) {
		return nil
	}
	payload := fmt.Sprintf("%d\x00%d\x00%d\x00%s\x00%d\x00%d\x00%d\x00%s\x00%s\x00%s\x00%d",
		scope.ChannelID, scope.ChannelAccountIdentityRevision, scope.UpstreamGroupID, scope.CanonicalGroupName,
		scope.UpstreamGroupPresenceGeneration, scope.GatewayUpstreamKeyID, scope.UpstreamKeyGeneration,
		scope.ModelName, scope.Operation, scope.Protocol, costUnitEpoch)
	digest := sha256.Sum256([]byte(payload))
	lockKey := int64(binary.BigEndian.Uint64(digest[:8]))
	return tx.Exec("SELECT pg_advisory_xact_lock(?)", lockKey).Error
}
