package storage

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	GatewayModelPriceSourceManual  = "manual"
	GatewayModelPriceSourceLiteLLM = "litellm"
)

type GatewayAutomaticModelPriceInput struct {
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
	Source                            string
	SourceURL                         string
	SourceChecksum                    string
	SourceObservedAt                  time.Time
}

type GatewayAutomaticModelPriceResult struct {
	Created       int      `json:"created"`
	Updated       int      `json:"updated"`
	Unchanged     int      `json:"unchanged"`
	ManualLocked  int      `json:"manual_locked"`
	CreatedModels []string `json:"created_models"`
	UpdatedModels []string `json:"updated_models"`
}

type gatewayModelPriceSourceWatermark struct {
	ModelName        string    `gorm:"size:256;primaryKey"`
	Source           string    `gorm:"size:32;primaryKey"`
	SourceURL        string    `gorm:"size:512;not null"`
	SourceChecksum   string    `gorm:"size:64;not null"`
	SourceObservedAt time.Time `gorm:"not null"`
	CreatedAt        time.Time `gorm:"not null"`
	UpdatedAt        time.Time `gorm:"not null"`
}

func (gatewayModelPriceSourceWatermark) TableName() string {
	return "gateway_model_price_source_watermarks"
}

type GatewayUsedModel struct {
	ModelName                 string             `json:"model_name"`
	RequestCount              int64              `json:"request_count"`
	LastUsedAt                *time.Time         `json:"last_used_at,omitempty"`
	UpstreamDiscovered        bool               `json:"upstream_discovered"`
	LastDiscoveredAt          *time.Time         `json:"last_discovered_at,omitempty"`
	ChannelAutomaticPriceRows int64              `json:"channel_automatic_price_rows"`
	ChannelPriceObservedAt    *time.Time         `json:"channel_price_observed_at,omitempty"`
	Price                     *GatewayModelPrice `json:"price,omitempty"`
}

func normalizeGatewayAutomaticModelPriceInput(input *GatewayAutomaticModelPriceInput) error {
	if input == nil {
		return ErrGatewayModelPriceInvalid
	}
	manual := GatewayModelPriceInput{
		ModelName:                         input.ModelName,
		InputUnitPriceUSD:                 input.InputUnitPriceUSD,
		OutputUnitPriceUSD:                input.OutputUnitPriceUSD,
		CacheReadUnitPriceUSD:             input.CacheReadUnitPriceUSD,
		CacheWriteUnitPriceUSD:            input.CacheWriteUnitPriceUSD,
		LongContextThresholdTokens:        input.LongContextThresholdTokens,
		LongContextInputUnitPriceUSD:      input.LongContextInputUnitPriceUSD,
		LongContextOutputUnitPriceUSD:     input.LongContextOutputUnitPriceUSD,
		LongContextCacheReadUnitPriceUSD:  input.LongContextCacheReadUnitPriceUSD,
		LongContextCacheWriteUnitPriceUSD: input.LongContextCacheWriteUnitPriceUSD,
	}
	if err := normalizeGatewayModelPriceInput(&manual); err != nil {
		return err
	}
	input.ModelName = manual.ModelName
	input.Source = strings.ToLower(strings.TrimSpace(input.Source))
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.SourceChecksum = strings.ToLower(strings.TrimSpace(input.SourceChecksum))
	input.SourceObservedAt = normalizeGatewayRequestPricingTime(input.SourceObservedAt)
	if input.Source != GatewayModelPriceSourceLiteLLM || input.SourceURL == "" ||
		!gatewayPricingChecksumPattern.MatchString(input.SourceChecksum) || input.SourceObservedAt.IsZero() {
		return ErrGatewayModelPriceInvalid
	}
	return nil
}

func automaticGatewayModelPrice(input GatewayAutomaticModelPriceInput, now time.Time) GatewayModelPrice {
	observedAt := input.SourceObservedAt
	return GatewayModelPrice{
		ModelName:                         input.ModelName,
		InputUnitPriceUSD:                 input.InputUnitPriceUSD,
		OutputUnitPriceUSD:                input.OutputUnitPriceUSD,
		CacheReadUnitPriceUSD:             input.CacheReadUnitPriceUSD,
		CacheWriteUnitPriceUSD:            input.CacheWriteUnitPriceUSD,
		LongContextThresholdTokens:        input.LongContextThresholdTokens,
		LongContextInputUnitPriceUSD:      input.LongContextInputUnitPriceUSD,
		LongContextOutputUnitPriceUSD:     input.LongContextOutputUnitPriceUSD,
		LongContextCacheReadUnitPriceUSD:  input.LongContextCacheReadUnitPriceUSD,
		LongContextCacheWriteUnitPriceUSD: input.LongContextCacheWriteUnitPriceUSD,
		Source:                            input.Source, SourceURL: input.SourceURL, SourceChecksum: input.SourceChecksum,
		SourceObservedAt: &observedAt, AutoUpdate: true, Enabled: true,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func gatewayModelPriceAutomaticValuesEqual(current, desired *GatewayModelPrice) bool {
	if current == nil || desired == nil {
		return false
	}
	return current.ModelName == desired.ModelName &&
		current.InputUnitPriceUSD == desired.InputUnitPriceUSD &&
		current.OutputUnitPriceUSD == desired.OutputUnitPriceUSD &&
		equalGatewayModelPriceFloat(current.CacheReadUnitPriceUSD, desired.CacheReadUnitPriceUSD) &&
		equalGatewayModelPriceFloat(current.CacheWriteUnitPriceUSD, desired.CacheWriteUnitPriceUSD) &&
		equalGatewayModelPriceInt64(current.LongContextThresholdTokens, desired.LongContextThresholdTokens) &&
		equalGatewayModelPriceFloat(current.LongContextInputUnitPriceUSD, desired.LongContextInputUnitPriceUSD) &&
		equalGatewayModelPriceFloat(current.LongContextOutputUnitPriceUSD, desired.LongContextOutputUnitPriceUSD) &&
		equalGatewayModelPriceFloat(current.LongContextCacheReadUnitPriceUSD, desired.LongContextCacheReadUnitPriceUSD) &&
		equalGatewayModelPriceFloat(current.LongContextCacheWriteUnitPriceUSD, desired.LongContextCacheWriteUnitPriceUSD) &&
		current.Source == desired.Source && current.SourceURL == desired.SourceURL
}

func equalGatewayModelPriceFloat(left, right *float64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func equalGatewayModelPriceInt64(left, right *int64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func observeGatewayAutomaticModelPriceSourceTx(
	tx *gorm.DB,
	input GatewayAutomaticModelPriceInput,
	now time.Time,
) (bool, error) {
	if tx == nil {
		return false, errors.New("gateway storage transaction is required")
	}
	load := func() (gatewayModelPriceSourceWatermark, error) {
		var watermark gatewayModelPriceSourceWatermark
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("model_name = ? AND source = ?", input.ModelName, input.Source).
			Take(&watermark).Error
		return watermark, err
	}

	watermark, err := load()
	if errors.Is(err, gorm.ErrRecordNotFound) {
		watermark = gatewayModelPriceSourceWatermark{
			ModelName: input.ModelName, Source: input.Source, SourceURL: input.SourceURL,
			SourceChecksum: input.SourceChecksum, SourceObservedAt: input.SourceObservedAt,
			CreatedAt: now, UpdatedAt: now,
		}
		created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&watermark)
		if created.Error != nil {
			return false, created.Error
		}
		if created.RowsAffected == 1 {
			return false, nil
		}
		watermark, err = load()
	}
	if err != nil {
		return false, err
	}

	switch {
	case input.SourceObservedAt.Before(watermark.SourceObservedAt):
		return true, nil
	case input.SourceObservedAt.Equal(watermark.SourceObservedAt):
		if input.SourceChecksum != watermark.SourceChecksum || input.SourceURL != watermark.SourceURL {
			return false, ErrGatewayModelPriceConflict
		}
		return false, nil
	}

	updated := tx.Model(&gatewayModelPriceSourceWatermark{}).
		Where("model_name = ? AND source = ? AND source_observed_at = ?",
			watermark.ModelName, watermark.Source, watermark.SourceObservedAt).
		Updates(map[string]any{
			"source_url": input.SourceURL, "source_checksum": input.SourceChecksum,
			"source_observed_at": input.SourceObservedAt, "updated_at": now,
		})
	if updated.Error != nil {
		return false, updated.Error
	}
	if updated.RowsAffected != 1 {
		return false, ErrGatewayModelPriceConflict
	}
	return false, nil
}

func (g *Gateway) ApplyAutomaticModelPrices(ctx context.Context, inputs []GatewayAutomaticModelPriceInput, actor string) (GatewayAutomaticModelPriceResult, error) {
	result := GatewayAutomaticModelPriceResult{CreatedModels: []string{}, UpdatedModels: []string{}}
	if g == nil || g.db == nil {
		return result, errors.New("gateway storage is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	actor, err := normalizeGatewayModelPriceActor(actor)
	if err != nil {
		return result, err
	}
	byModel := make(map[string]GatewayAutomaticModelPriceInput, len(inputs))
	for index := range inputs {
		input := inputs[index]
		if err := normalizeGatewayAutomaticModelPriceInput(&input); err != nil {
			return result, err
		}
		if _, exists := byModel[input.ModelName]; exists {
			return result, ErrGatewayModelPriceInvalid
		}
		byModel[input.ModelName] = input
	}
	modelNames := make([]string, 0, len(byModel))
	for modelName := range byModel {
		modelNames = append(modelNames, modelName)
	}
	sort.Strings(modelNames)

	err = g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, modelName := range modelNames {
			if err := ctx.Err(); err != nil {
				return err
			}
			input := byModel[modelName]
			now := normalizeGatewayRequestPricingTime(time.Now())
			stale, err := observeGatewayAutomaticModelPriceSourceTx(tx, input, now)
			if err != nil {
				return err
			}
			if stale {
				result.Unchanged++
				continue
			}
			desired := automaticGatewayModelPrice(input, now)
			var current GatewayModelPrice
			err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("model_name = ?", modelName).Take(&current).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if err := tx.Create(&desired).Error; err != nil {
					if gatewayModelPriceDuplicateError(err) {
						return ErrGatewayModelPriceConflict
					}
					return err
				}
				audit := gatewayModelPriceAudit(desired, "create", actor, now)
				if err := tx.Create(&audit).Error; err != nil {
					return err
				}
				result.Created++
				result.CreatedModels = append(result.CreatedModels, modelName)
				continue
			}
			if err != nil {
				return err
			}
			if !current.AutoUpdate {
				result.ManualLocked++
				continue
			}
			desired.ID = current.ID
			desired.Enabled = current.Enabled
			desired.CreatedAt = current.CreatedAt
			desired.Revision = current.Revision
			if current.SourceObservedAt != nil {
				switch {
				case input.SourceObservedAt.Before(*current.SourceObservedAt):
					result.Unchanged++
					continue
				case input.SourceObservedAt.Equal(*current.SourceObservedAt) &&
					!gatewayModelPriceAutomaticValuesEqual(&current, &desired):
					return ErrGatewayModelPriceConflict
				}
			}
			if gatewayModelPriceAutomaticValuesEqual(&current, &desired) {
				result.Unchanged++
				continue
			}
			if current.Revision == math.MaxUint64 || current.Revision >= math.MaxInt64 {
				return ErrGatewayModelPriceConflict
			}
			desired.Revision++
			desired.UpdatedAt = now
			updates := map[string]any{
				"input_unit_price_usd":                    desired.InputUnitPriceUSD,
				"output_unit_price_usd":                   desired.OutputUnitPriceUSD,
				"cache_read_unit_price_usd":               desired.CacheReadUnitPriceUSD,
				"cache_write_unit_price_usd":              desired.CacheWriteUnitPriceUSD,
				"long_context_threshold_tokens":           desired.LongContextThresholdTokens,
				"long_context_input_unit_price_usd":       desired.LongContextInputUnitPriceUSD,
				"long_context_output_unit_price_usd":      desired.LongContextOutputUnitPriceUSD,
				"long_context_cache_read_unit_price_usd":  desired.LongContextCacheReadUnitPriceUSD,
				"long_context_cache_write_unit_price_usd": desired.LongContextCacheWriteUnitPriceUSD,
				"source": desired.Source, "source_url": desired.SourceURL,
				"source_checksum": desired.SourceChecksum, "source_observed_at": desired.SourceObservedAt,
				"revision": desired.Revision, "updated_at": now,
			}
			update := tx.Model(&GatewayModelPrice{}).Where("id = ? AND revision = ?", current.ID, current.Revision).Updates(updates)
			if update.Error != nil {
				return update.Error
			}
			if update.RowsAffected != 1 {
				return ErrGatewayModelPriceConflict
			}
			audit := gatewayModelPriceAudit(desired, "update", actor, now)
			if err := tx.Create(&audit).Error; err != nil {
				return err
			}
			result.Updated++
			result.UpdatedModels = append(result.UpdatedModels, modelName)
		}
		return nil
	})
	if err != nil {
		return GatewayAutomaticModelPriceResult{CreatedModels: []string{}, UpdatedModels: []string{}}, err
	}
	return result, err
}

func (g *Gateway) ListUsedModels(ctx context.Context, since time.Time) ([]GatewayUsedModel, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if since.IsZero() {
		since = time.Now().UTC().Add(-30 * 24 * time.Hour)
	}
	type usageRow struct {
		ModelName    string
		RequestCount int64
		LastUsedAt   *time.Time
	}
	var rows []usageRow
	// price_model_name is the exact upstream model frozen by dispatch and is the
	// same key used by price resolution. Older rows fall back to their upstream
	// and downstream display names in that order.
	effectiveModel := "COALESCE(NULLIF(TRIM(price_model_name), ''), NULLIF(TRIM(upstream_model_name), ''), TRIM(model_name))"
	err := g.db.WithContext(ctx).Model(&GatewayRequestLog{}).
		Select(effectiveModel+" AS model_name, COUNT(*) AS request_count, MAX(created_at) AS last_used_at").
		Where("created_at >= ?", normalizeGatewayRequestPricingTime(since)).
		Where(effectiveModel+" <> ''").
		Where(gatewayRequestRecordSQL("")).
		Where("COALESCE(route_reason, '') <> ?", gatewayRouteReasonLatencyProbe).
		Group(effectiveModel).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	byModel := make(map[string]*GatewayUsedModel, len(rows))
	for _, row := range rows {
		name := strings.TrimSpace(row.ModelName)
		if name == "" {
			continue
		}
		byModel[name] = &GatewayUsedModel{ModelName: name, RequestCount: row.RequestCount, LastUsedAt: row.LastUsedAt}
	}
	var upstreamKeys []GatewayUpstreamKey
	if err := g.db.WithContext(ctx).Select("model_ids", "models_synced_at").
		Where("TRIM(COALESCE(model_ids, '')) <> ''").Find(&upstreamKeys).Error; err != nil {
		return nil, err
	}
	for _, key := range upstreamKeys {
		for _, rawName := range strings.Split(key.ModelIDs, ",") {
			name := strings.TrimSpace(rawName)
			if name == "" {
				continue
			}
			item := byModel[name]
			if item == nil {
				item = &GatewayUsedModel{ModelName: name}
				byModel[name] = item
			}
			item.UpstreamDiscovered = true
			if key.ModelsSyncedAt != nil && (item.LastDiscoveredAt == nil || key.ModelsSyncedAt.After(*item.LastDiscoveredAt)) {
				observed := *key.ModelsSyncedAt
				item.LastDiscoveredAt = &observed
			}
		}
	}
	type channelPriceCoverageRow struct {
		ModelName      string
		PriceRows      int64
		LastObservedAt *time.Time
	}
	var channelCoverage []channelPriceCoverageRow
	if err := g.db.WithContext(ctx).Table("gateway_channel_model_prices AS prices").
		Select("prices.model_name AS model_name, COUNT(*) AS price_rows, MAX(prices.source_observed_at) AS last_observed_at").
		Joins("JOIN channels ON channels.id = prices.channel_id AND channels.upstream_account_identity_revision = prices.channel_identity_revision AND channels.cost_unit_epoch = prices.cost_unit_epoch").
		Joins(`JOIN gateway_upstream_groups AS groups ON groups.id = prices.gateway_upstream_group_id
			AND groups.channel_id = prices.channel_id
			AND groups.present = TRUE
			AND groups.presence_generation = prices.upstream_group_presence_generation`).
		Joins(`JOIN rate_snapshots AS rates ON rates.channel_id = prices.channel_id
			AND rates.cost_unit_epoch = prices.cost_unit_epoch
			AND lower(btrim(rates.model_name)) = prices.upstream_group_name
			AND abs(rates.ratio - prices.group_ratio) <= greatest(
				1e-9::double precision,
				greatest(abs(rates.ratio), abs(prices.group_ratio)) * 1e-9::double precision
			)`).
		Group("prices.model_name").Scan(&channelCoverage).Error; err != nil {
		return nil, err
	}
	for _, row := range channelCoverage {
		name := strings.TrimSpace(row.ModelName)
		if name == "" || row.PriceRows <= 0 {
			continue
		}
		item := byModel[name]
		if item == nil {
			item = &GatewayUsedModel{ModelName: name}
			byModel[name] = item
		}
		item.ChannelAutomaticPriceRows = row.PriceRows
		item.ChannelPriceObservedAt = row.LastObservedAt
	}
	prices, err := g.ListModelPrices()
	if err != nil {
		return nil, err
	}
	for index := range prices {
		price := prices[index]
		item := byModel[price.ModelName]
		if item == nil {
			item = &GatewayUsedModel{ModelName: price.ModelName}
			byModel[price.ModelName] = item
		}
		item.Price = &price
	}
	items := make([]GatewayUsedModel, 0, len(byModel))
	for _, item := range byModel {
		items = append(items, *item)
	}
	sort.Slice(items, func(i, j int) bool {
		leftMissing, rightMissing := items[i].Price == nil, items[j].Price == nil
		if leftMissing != rightMissing {
			return leftMissing
		}
		if items[i].RequestCount != items[j].RequestCount {
			return items[i].RequestCount > items[j].RequestCount
		}
		return items[i].ModelName < items[j].ModelName
	})
	return items, nil
}
