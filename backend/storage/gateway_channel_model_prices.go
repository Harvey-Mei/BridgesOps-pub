package storage

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	gatewayChannelModelPriceSource      = "channel_pricing_api"
	gatewayChannelModelPriceMaximumRows = 4096
)

var ErrStaleGatewayChannelModelPriceSnapshot = errors.New("channel model price snapshot is older than the committed observation")

// GatewayChannelModelPrice is the last complete, provider-published price for
// one exact channel account, upstream group, and model. Request price facts
// copy these values into an immutable dispatch scope; this mutable catalog is
// never itself used to reprice historical requests.
type GatewayChannelModelPrice struct {
	ID                                uint64  `gorm:"primaryKey"`
	ChannelID                         uint    `gorm:"not null;uniqueIndex:uq_gateway_channel_model_prices,priority:1"`
	ChannelIdentityRevision           uint64  `gorm:"not null"`
	CostUnitEpoch                     uint64  `gorm:"not null"`
	UpstreamGroupID                   int64   `gorm:"not null"`
	GatewayUpstreamGroupID            uint    `gorm:"not null"`
	UpstreamGroupPresenceGeneration   uint64  `gorm:"not null"`
	UpstreamGroupName                 string  `gorm:"size:128;not null;uniqueIndex:uq_gateway_channel_model_prices,priority:2"`
	GroupRatio                        float64 `gorm:"not null"`
	ModelName                         string  `gorm:"size:256;not null;uniqueIndex:uq_gateway_channel_model_prices,priority:3"`
	InputUnitPriceUSD                 float64 `gorm:"not null"`
	OutputUnitPriceUSD                float64 `gorm:"not null"`
	CacheReadUnitPriceUSD             *float64
	CacheWriteUnitPriceUSD            *float64
	LongContextBasis                  string `gorm:"size:32;not null"`
	LongContextThresholdTokens        *int64
	LongContextInputUnitPriceUSD      *float64
	LongContextOutputUnitPriceUSD     *float64
	LongContextCacheReadUnitPriceUSD  *float64
	LongContextCacheWriteUnitPriceUSD *float64
	SourceVersion                     string    `gorm:"size:128;not null"`
	SourceChecksum                    string    `gorm:"size:64;not null"`
	SourceObservedAt                  time.Time `gorm:"not null"`
	CreatedAt                         time.Time `gorm:"not null"`
	UpdatedAt                         time.Time `gorm:"not null"`
}

func (GatewayChannelModelPrice) TableName() string { return "gateway_channel_model_prices" }

type GatewayChannelModelPriceSnapshotItem struct {
	UpstreamGroupID                   int64
	UpstreamGroupName                 string
	GroupRatio                        *float64
	ModelName                         string
	InputUnitPriceUSD                 float64
	OutputUnitPriceUSD                float64
	CacheReadUnitPriceUSD             *float64
	CacheWriteUnitPriceUSD            *float64
	LongContextBasis                  string
	LongContextThresholdTokens        *int64
	LongContextInputUnitPriceUSD      *float64
	LongContextOutputUnitPriceUSD     *float64
	LongContextCacheReadUnitPriceUSD  *float64
	LongContextCacheWriteUnitPriceUSD *float64
}

type GatewayChannelModelPriceSnapshotInput struct {
	ChannelID                uint
	ExpectedIdentityRevision uint64
	ExpectedCostUnitEpoch    uint64
	SourceVersion            string
	SourceChecksum           string
	ObservedAt               time.Time
	Items                    []GatewayChannelModelPriceSnapshotItem
}

func normalizeGatewayChannelModelPriceSnapshot(input *GatewayChannelModelPriceSnapshotInput) error {
	if input == nil || input.ChannelID == 0 || input.ExpectedIdentityRevision == 0 || input.ExpectedCostUnitEpoch == 0 ||
		input.ObservedAt.IsZero() || len(input.Items) > gatewayChannelModelPriceMaximumRows {
		return ErrGatewayModelPriceInvalid
	}
	input.ObservedAt = normalizeGatewayRequestPricingTime(input.ObservedAt)
	input.SourceVersion = strings.TrimSpace(input.SourceVersion)
	input.SourceChecksum = strings.ToLower(strings.TrimSpace(input.SourceChecksum))
	if strings.ContainsRune(input.SourceVersion, '\x00') || !utf8.ValidString(input.SourceVersion) ||
		utf8.RuneCountInString(input.SourceVersion) > 128 || !gatewayPricingChecksumPattern.MatchString(input.SourceChecksum) {
		return ErrGatewayModelPriceInvalid
	}
	seen := make(map[string]struct{}, len(input.Items))
	for index := range input.Items {
		item := &input.Items[index]
		item.UpstreamGroupName = strings.ToLower(strings.TrimSpace(item.UpstreamGroupName))
		item.ModelName = strings.TrimSpace(item.ModelName)
		if item.UpstreamGroupID < 0 || item.UpstreamGroupName == "" || strings.ContainsRune(item.UpstreamGroupName, '\x00') ||
			!utf8.ValidString(item.UpstreamGroupName) || utf8.RuneCountInString(item.UpstreamGroupName) > 128 ||
			item.ModelName == "" || strings.ContainsRune(item.ModelName, '\x00') || !utf8.ValidString(item.ModelName) ||
			utf8.RuneCountInString(item.ModelName) > 256 {
			return ErrGatewayModelPriceInvalid
		}
		key := item.UpstreamGroupName + "\x00" + item.ModelName
		if _, duplicate := seen[key]; duplicate {
			return ErrGatewayModelPriceInvalid
		}
		seen[key] = struct{}{}
		if item.GroupRatio == nil || *item.GroupRatio < 0 || math.IsNaN(*item.GroupRatio) || math.IsInf(*item.GroupRatio, 0) {
			return ErrGatewayModelPriceInvalid
		}
		for _, value := range []float64{item.InputUnitPriceUSD, item.OutputUnitPriceUSD} {
			if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
				return ErrGatewayModelPriceInvalid
			}
		}
		for _, value := range []*float64{
			item.CacheReadUnitPriceUSD, item.CacheWriteUnitPriceUSD,
			item.LongContextInputUnitPriceUSD, item.LongContextOutputUnitPriceUSD,
			item.LongContextCacheReadUnitPriceUSD, item.LongContextCacheWriteUnitPriceUSD,
		} {
			if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
				return ErrGatewayModelPriceInvalid
			}
		}
		item.LongContextBasis = strings.ToLower(strings.TrimSpace(item.LongContextBasis))
		if strings.ContainsRune(item.LongContextBasis, '\x00') || !utf8.ValidString(item.LongContextBasis) ||
			utf8.RuneCountInString(item.LongContextBasis) > 32 {
			return ErrGatewayModelPriceInvalid
		}
		longContext := item.LongContextBasis != "" || item.LongContextThresholdTokens != nil ||
			item.LongContextInputUnitPriceUSD != nil || item.LongContextOutputUnitPriceUSD != nil ||
			item.LongContextCacheReadUnitPriceUSD != nil || item.LongContextCacheWriteUnitPriceUSD != nil
		if !longContext {
			continue
		}
		if item.LongContextBasis != "whole_request" || item.LongContextThresholdTokens == nil ||
			!validGatewayRequestPriceLongContextThreshold(*item.LongContextThresholdTokens) ||
			item.LongContextInputUnitPriceUSD == nil ||
			item.LongContextOutputUnitPriceUSD == nil {
			return ErrGatewayModelPriceInvalid
		}
	}
	sort.Slice(input.Items, func(i, j int) bool {
		if input.Items[i].UpstreamGroupName != input.Items[j].UpstreamGroupName {
			return input.Items[i].UpstreamGroupName < input.Items[j].UpstreamGroupName
		}
		return input.Items[i].ModelName < input.Items[j].ModelName
	})
	return nil
}

func (r *Rates) ReplaceChannelModelPriceSnapshot(ctx context.Context, input GatewayChannelModelPriceSnapshotInput) error {
	if r == nil || r.db == nil {
		return errors.New("rate snapshot store is unavailable")
	}
	if err := normalizeGatewayChannelModelPriceSnapshot(&input); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		channel, state, err := lockChannelAndCostStateTx(tx, input.ChannelID)
		if err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != input.ExpectedIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		if state.CostUnitEpoch != input.ExpectedCostUnitEpoch || channel.CostUnitEpoch != input.ExpectedCostUnitEpoch {
			return ErrCostUnitEpochConflict
		}
		if channel.ModelPriceObservedAt != nil && !input.ObservedAt.After(*channel.ModelPriceObservedAt) {
			return ErrStaleGatewayChannelModelPriceSnapshot
		}
		var rates []RateSnapshot
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
			Where("channel_id = ? AND cost_unit_epoch = ?", input.ChannelID, input.ExpectedCostUnitEpoch).
			Find(&rates).Error; err != nil {
			return err
		}
		rateByGroup := make(map[string]float64, len(rates))
		for _, rate := range rates {
			groupName := strings.ToLower(strings.TrimSpace(rate.ModelName))
			if groupName == "" {
				return ErrGatewayModelPriceInvalid
			}
			if _, duplicate := rateByGroup[groupName]; duplicate {
				return ErrGatewayModelPriceInvalid
			}
			rateByGroup[groupName] = rate.Ratio
		}
		var groups []GatewayUpstreamGroup
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
			Select("id", "channel_id", "canonical_group_name", "presence_generation", "present").
			Where("channel_id = ? AND present = ?", input.ChannelID, true).
			Find(&groups).Error; err != nil {
			return err
		}
		groupByName := make(map[string]GatewayUpstreamGroup, len(groups))
		for _, group := range groups {
			if !group.Present || group.ID == 0 || group.PresenceGeneration == 0 || group.CanonicalGroupName == "" {
				return ErrGatewayModelPriceInvalid
			}
			if _, duplicate := groupByName[group.CanonicalGroupName]; duplicate {
				return ErrGatewayModelPriceInvalid
			}
			groupByName[group.CanonicalGroupName] = group
		}
		activeItems := make([]GatewayChannelModelPriceSnapshotItem, 0, len(input.Items))
		for _, item := range input.Items {
			rate, active := rateByGroup[item.UpstreamGroupName]
			if !active {
				continue
			}
			if _, active = groupByName[item.UpstreamGroupName]; !active {
				continue
			}
			if item.GroupRatio == nil || !financiallyEqual(rate, *item.GroupRatio) {
				return ErrGatewayModelPriceInvalid
			}
			activeItems = append(activeItems, item)
		}

		var stored []GatewayChannelModelPrice
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ?", input.ChannelID).Order("upstream_group_name ASC, model_name ASC").Find(&stored).Error; err != nil {
			return err
		}
		current := make(map[string]GatewayChannelModelPrice, len(stored))
		for _, row := range stored {
			current[gatewayChannelModelPriceKey(row.UpstreamGroupName, row.ModelName)] = row
		}
		now := input.ObservedAt
		for _, item := range activeItems {
			group := groupByName[item.UpstreamGroupName]
			row := GatewayChannelModelPrice{
				ChannelID: input.ChannelID, ChannelIdentityRevision: input.ExpectedIdentityRevision,
				CostUnitEpoch: input.ExpectedCostUnitEpoch, UpstreamGroupID: item.UpstreamGroupID,
				GatewayUpstreamGroupID:          group.ID,
				UpstreamGroupPresenceGeneration: group.PresenceGeneration,
				UpstreamGroupName:               item.UpstreamGroupName, GroupRatio: *item.GroupRatio, ModelName: item.ModelName,
				InputUnitPriceUSD:  item.InputUnitPriceUSD,
				OutputUnitPriceUSD: item.OutputUnitPriceUSD, CacheReadUnitPriceUSD: item.CacheReadUnitPriceUSD,
				CacheWriteUnitPriceUSD: item.CacheWriteUnitPriceUSD, LongContextBasis: item.LongContextBasis,
				LongContextThresholdTokens:        item.LongContextThresholdTokens,
				LongContextInputUnitPriceUSD:      item.LongContextInputUnitPriceUSD,
				LongContextOutputUnitPriceUSD:     item.LongContextOutputUnitPriceUSD,
				LongContextCacheReadUnitPriceUSD:  item.LongContextCacheReadUnitPriceUSD,
				LongContextCacheWriteUnitPriceUSD: item.LongContextCacheWriteUnitPriceUSD,
				SourceVersion:                     input.SourceVersion,
				SourceChecksum:                    input.SourceChecksum, SourceObservedAt: input.ObservedAt,
				CreatedAt: now, UpdatedAt: now,
			}
			key := gatewayChannelModelPriceKey(item.UpstreamGroupName, item.ModelName)
			previous, exists := current[key]
			if !exists {
				if err := tx.Create(&row).Error; err != nil {
					return err
				}
				continue
			}
			delete(current, key)
			updated := tx.Model(&GatewayChannelModelPrice{}).
				Where("id = ? AND channel_id = ?", previous.ID, input.ChannelID).
				Updates(map[string]any{
					"channel_identity_revision":               input.ExpectedIdentityRevision,
					"cost_unit_epoch":                         input.ExpectedCostUnitEpoch,
					"upstream_group_id":                       item.UpstreamGroupID,
					"gateway_upstream_group_id":               group.ID,
					"upstream_group_presence_generation":      group.PresenceGeneration,
					"group_ratio":                             *item.GroupRatio,
					"input_unit_price_usd":                    item.InputUnitPriceUSD,
					"output_unit_price_usd":                   item.OutputUnitPriceUSD,
					"cache_read_unit_price_usd":               item.CacheReadUnitPriceUSD,
					"cache_write_unit_price_usd":              item.CacheWriteUnitPriceUSD,
					"long_context_basis":                      item.LongContextBasis,
					"long_context_threshold_tokens":           item.LongContextThresholdTokens,
					"long_context_input_unit_price_usd":       item.LongContextInputUnitPriceUSD,
					"long_context_output_unit_price_usd":      item.LongContextOutputUnitPriceUSD,
					"long_context_cache_read_unit_price_usd":  item.LongContextCacheReadUnitPriceUSD,
					"long_context_cache_write_unit_price_usd": item.LongContextCacheWriteUnitPriceUSD,
					"source_version":                          input.SourceVersion,
					"source_checksum":                         input.SourceChecksum,
					"source_observed_at":                      input.ObservedAt,
					"updated_at":                              now,
				})
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != 1 {
				return ErrStaleChannelAccountIdentityRevision
			}
		}
		staleIDs := make([]uint64, 0, len(current))
		for _, row := range current {
			staleIDs = append(staleIDs, row.ID)
		}
		if len(staleIDs) > 0 {
			deleted := tx.Where("channel_id = ? AND id IN ?", input.ChannelID, staleIDs).Delete(&GatewayChannelModelPrice{})
			if deleted.Error != nil {
				return deleted.Error
			}
			if deleted.RowsAffected != int64(len(staleIDs)) {
				return ErrStaleChannelAccountIdentityRevision
			}
		}
		watermark := tx.Model(&Channel{}).
			Where("id = ? AND upstream_account_identity_revision = ? AND cost_unit_epoch = ?",
				input.ChannelID, input.ExpectedIdentityRevision, input.ExpectedCostUnitEpoch).
			Update("model_price_observed_at", input.ObservedAt)
		if watermark.Error != nil {
			return watermark.Error
		}
		if watermark.RowsAffected != 1 {
			return ErrCostUnitEpochConflict
		}
		return nil
	})
}

func findGatewayChannelModelPriceTx(tx *gorm.DB, scope GatewayRequestPriceScope, costUnitEpoch uint64) (*GatewayChannelModelPrice, error) {
	if tx == nil || costUnitEpoch == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	var price GatewayChannelModelPrice
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Where("channel_id = ? AND channel_identity_revision = ? AND cost_unit_epoch = ? AND gateway_upstream_group_id = ? AND upstream_group_presence_generation = ? AND upstream_group_name = ? AND model_name = ?",
			scope.ChannelID, scope.ChannelAccountIdentityRevision, costUnitEpoch,
			scope.UpstreamGroupID, scope.UpstreamGroupPresenceGeneration,
			strings.ToLower(strings.TrimSpace(scope.CanonicalGroupName)), scope.ModelName).
		Take(&price).Error; err != nil {
		return nil, err
	}
	return &price, nil
}

func gatewayChannelModelPriceKey(groupName, modelName string) string {
	return strings.ToLower(strings.TrimSpace(groupName)) + "\x00" + modelName
}
