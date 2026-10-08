package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// GatewayRequestPriceFactSchemaVersion remains the legacy flat-price
	// schema identifier. Existing callers that omit a schema continue to write
	// v1 facts, whose checksum payload must never change.
	GatewayRequestPriceFactSchemaVersion       = 1
	GatewayRequestPriceFactTieredSchemaVersion = 2
	GatewayRequestPriceLongContextThreshold    = int64(272_000)

	GatewayPricingOperationTextGeneration  = "text_generation"
	GatewayPricingOperationEmbedding       = "embedding"
	GatewayPricingUnitPerMillionTokens     = "per_million_tokens"
	GatewayPricingUnitPerOperation         = "per_operation"
	GatewayPricingUsageSchemaTokensV1      = "token_components_v1"
	GatewayPricingUsageSchemaInputTokensV1 = "input_tokens_v1"
	GatewayPricingUsageSchemaOperationV1   = "operation_units_v1"

	GatewayPricingProtocolOpenAIChatCompletions = "openai_chat_completions"
	GatewayPricingProtocolOpenAICompletions     = "openai_completions"
	GatewayPricingProtocolOpenAIEmbeddings      = "openai_embeddings"
	GatewayPricingProtocolOpenAIResponses       = GatewayImageProtocolResponses
	GatewayPricingProtocolAnthropicMessages     = "anthropic_messages"

	GatewayPricingUnavailableScopeIncomplete          = "scope_incomplete"
	GatewayPricingUnavailableOperationAmbiguous       = "operation_ambiguous"
	GatewayPricingUnavailableOperationUnsupported     = "operation_unsupported"
	GatewayPricingUnavailableIdentityChanged          = "identity_changed"
	GatewayPricingUnavailableFactMissing              = "fact_missing"
	GatewayPricingUnavailableFactExpired              = "fact_expired"
	GatewayPricingUnavailableFactNotYetValid          = "fact_not_yet_valid"
	GatewayPricingUnavailableSchemaUnsupported        = "schema_unsupported"
	GatewayPricingUnavailableChecksumMismatch         = "checksum_mismatch"
	GatewayPricingUnavailableUsageIncomplete          = "usage_incomplete"
	GatewayPricingUnavailableUsageInvalid             = "usage_invalid"
	GatewayPricingUnavailablePriceMissing             = "price_missing"
	GatewayPricingUnavailableCurrencyUnsupported      = "currency_unsupported"
	GatewayPricingUnavailableCostBasisChanged         = "cost_basis_changed"
	GatewayPricingUnavailableLegacyUnverifiedEstimate = "legacy_unverified_estimate"

	gatewayRequestPriceSupportedCurrency = "USD"
	gatewayRequestPriceSourceModelConfig = "model_price_config"
)

const (
	GatewayPricingUsageInputPresent uint16 = 1 << iota
	GatewayPricingUsageOutputPresent
	GatewayPricingUsageCacheReadPresent
	GatewayPricingUsageCacheWritePresent
	GatewayPricingUsageOperationUnitsPresent
)

const gatewayPricingTokenUsagePresence = GatewayPricingUsageInputPresent |
	GatewayPricingUsageOutputPresent |
	GatewayPricingUsageCacheReadPresent |
	GatewayPricingUsageCacheWritePresent

var (
	ErrGatewayRequestPriceIdentityChanged     = errors.New("gateway request price identity changed")
	ErrGatewayRequestPriceObservationStale    = errors.New("gateway request price observation is stale")
	ErrGatewayRequestPriceObservationConflict = errors.New("gateway request price observation conflicts at the same time")
	errGatewayRequestPriceAppendConflict      = errors.New("gateway request price fact append conflicted")

	gatewayPricingCurrencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
	gatewayPricingSourcePattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)
	gatewayPricingChecksumPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// PostgreSQL timestamptz stores microsecond precision. Price-fact checksums
// must be calculated from the exact representation that survives a database
// round trip, otherwise a nanosecond-valued fact would invalidate itself after
// it is inserted and read back.
func normalizeGatewayRequestPricingTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

// GatewayRequestPriceFact is an append-only price observation for one exact
// upstream identity. Mutable display names and ratios are deliberately not
// consulted when an historical request is evaluated.
type GatewayRequestPriceFact struct {
	ID                              uint64     `gorm:"primaryKey" json:"id"`
	ChannelID                       uint       `gorm:"not null;index:idx_gateway_price_fact_scope,priority:1" json:"channel_id"`
	ChannelAccountIdentityRevision  uint64     `gorm:"not null;index:idx_gateway_price_fact_scope,priority:2" json:"channel_account_identity_revision"`
	CostUnitEpoch                   uint64     `gorm:"not null;default:1;index:idx_gateway_price_fact_scope,priority:3" json:"cost_unit_epoch"`
	UpstreamGroupID                 uint       `gorm:"not null;index:idx_gateway_price_fact_scope,priority:4" json:"upstream_group_id"`
	CanonicalGroupName              string     `gorm:"size:128;not null" json:"canonical_group_name"`
	UpstreamGroupPresenceGeneration uint64     `gorm:"not null;index:idx_gateway_price_fact_scope,priority:4" json:"upstream_group_presence_generation"`
	GatewayUpstreamKeyID            uint       `gorm:"not null;index:idx_gateway_price_fact_scope,priority:5" json:"gateway_upstream_key_id"`
	UpstreamKeyGeneration           uint64     `gorm:"not null;index:idx_gateway_price_fact_scope,priority:6" json:"upstream_key_generation"`
	ModelName                       string     `gorm:"size:256;not null;index:idx_gateway_price_fact_scope,priority:7" json:"model_name"`
	Operation                       string     `gorm:"size:32;not null;index:idx_gateway_price_fact_scope,priority:8" json:"operation"`
	Protocol                        string     `gorm:"size:32;not null;index:idx_gateway_price_fact_scope,priority:9" json:"protocol"`
	InputUnitPrice                  *float64   `json:"input_unit_price,omitempty"`
	OutputUnitPrice                 *float64   `json:"output_unit_price,omitempty"`
	CacheReadUnitPrice              *float64   `json:"cache_read_unit_price,omitempty"`
	CacheWriteUnitPrice             *float64   `json:"cache_write_unit_price,omitempty"`
	LongContextThresholdTokens      *int64     `json:"long_context_threshold_tokens,omitempty"`
	LongContextInputUnitPrice       *float64   `json:"long_context_input_unit_price,omitempty"`
	LongContextOutputUnitPrice      *float64   `json:"long_context_output_unit_price,omitempty"`
	LongContextCacheReadUnitPrice   *float64   `json:"long_context_cache_read_unit_price,omitempty"`
	LongContextCacheWriteUnitPrice  *float64   `json:"long_context_cache_write_unit_price,omitempty"`
	GroupRatio                      *float64   `json:"group_ratio,omitempty"`
	CompletionRatio                 *float64   `json:"completion_ratio,omitempty"`
	OperationUnitPrice              *float64   `json:"operation_unit_price,omitempty"`
	Unit                            string     `gorm:"size:32;not null" json:"unit"`
	Currency                        string     `gorm:"size:3;not null" json:"currency"`
	Source                          string     `gorm:"size:64;not null" json:"source"`
	ObservedAt                      time.Time  `gorm:"not null;index:idx_gateway_price_fact_observation" json:"observed_at"`
	ValidFrom                       time.Time  `gorm:"not null;index:idx_gateway_price_fact_validity,priority:1" json:"valid_from"`
	ValidUntil                      *time.Time `gorm:"index:idx_gateway_price_fact_validity,priority:2" json:"valid_until,omitempty"`
	FactVersion                     uint64     `gorm:"not null" json:"fact_version"`
	SchemaVersion                   int        `gorm:"not null" json:"schema_version"`
	Checksum                        string     `gorm:"size:64;not null" json:"checksum"`
	CreatedAt                       time.Time  `gorm:"not null" json:"created_at"`
}

func (GatewayRequestPriceFact) TableName() string { return "gateway_request_price_facts" }

type GatewayRequestPriceScope struct {
	ChannelID                       uint
	ChannelAccountIdentityRevision  uint64
	UpstreamGroupID                 uint
	CanonicalGroupName              string
	UpstreamGroupPresenceGeneration uint64
	GatewayUpstreamKeyID            uint
	UpstreamKeyGeneration           uint64
	ModelName                       string
	Operation                       string
	Protocol                        string
}

type GatewayRequestPriceFactLookup struct {
	Fact              *GatewayRequestPriceFact
	UnavailableReason string
}

// GatewayRequestPricingUsage uses pointers so a connector can distinguish an
// explicitly reported zero from a component that was not present at all.
type GatewayRequestPricingUsage struct {
	InputTokens      *int64
	OutputTokens     *int64
	CacheReadTokens  *int64
	CacheWriteTokens *int64
	OperationUnits   *int64
}

type GatewayRequestPriceEstimate struct {
	Cost              *float64
	Currency          string
	Status            GatewayCostStatus
	UnavailableReason string
	UsagePresence     uint16
}

func (usage GatewayRequestPricingUsage) PresenceMask() uint16 {
	var mask uint16
	if usage.InputTokens != nil {
		mask |= GatewayPricingUsageInputPresent
	}
	if usage.OutputTokens != nil {
		mask |= GatewayPricingUsageOutputPresent
	}
	if usage.CacheReadTokens != nil {
		mask |= GatewayPricingUsageCacheReadPresent
	}
	if usage.CacheWriteTokens != nil {
		mask |= GatewayPricingUsageCacheWritePresent
	}
	if usage.OperationUnits != nil {
		mask |= GatewayPricingUsageOperationUnitsPresent
	}
	return mask
}

func gatewayRequestPricingProtocolSupported(operation, protocol string) bool {
	switch operation {
	case GatewayPricingOperationTextGeneration:
		switch protocol {
		case GatewayPricingProtocolOpenAIChatCompletions,
			GatewayPricingProtocolOpenAICompletions,
			GatewayPricingProtocolOpenAIResponses,
			GatewayPricingProtocolAnthropicMessages:
			return true
		}
	case GatewayPricingOperationEmbedding:
		return protocol == GatewayPricingProtocolOpenAIEmbeddings
	case GatewayImageOperationGeneration:
		return protocol == GatewayImageProtocolImages || protocol == GatewayImageProtocolResponses
	}
	return false
}

func validateGatewayRequestPriceFactShape(fact *GatewayRequestPriceFact) error {
	if fact == nil {
		return errors.New("request price fact is required")
	}
	if !gatewayRequestPricingProtocolSupported(fact.Operation, fact.Protocol) {
		return errors.New("request price fact operation or protocol is unsupported")
	}
	if fact.SchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
		if fact.Operation != GatewayPricingOperationTextGeneration ||
			fact.Unit != GatewayPricingUnitPerMillionTokens || fact.OperationUnitPrice != nil ||
			fact.InputUnitPrice == nil || fact.OutputUnitPrice == nil ||
			fact.GroupRatio == nil || fact.CompletionRatio == nil {
			return errors.New("tiered text price fact has an invalid base shape")
		}
		if *fact.GroupRatio < 0 || math.IsNaN(*fact.GroupRatio) || math.IsInf(*fact.GroupRatio, 0) ||
			*fact.CompletionRatio < 0 || math.IsNaN(*fact.CompletionRatio) || math.IsInf(*fact.CompletionRatio, 0) {
			return errors.New("tiered text price fact ratios are invalid")
		}
		if fact.LongContextThresholdTokens == nil ||
			!validGatewayRequestPriceLongContextThreshold(*fact.LongContextThresholdTokens) {
			return errors.New("tiered text price fact threshold is invalid")
		}
		if fact.LongContextInputUnitPrice == nil || fact.LongContextOutputUnitPrice == nil {
			return errors.New("tiered text price fact long-context prices are incomplete")
		}
		return nil
	}
	if fact.SchemaVersion != 0 && fact.SchemaVersion != GatewayRequestPriceFactSchemaVersion {
		return errors.New("request price fact schema is unsupported")
	}
	if fact.LongContextThresholdTokens != nil || fact.LongContextInputUnitPrice != nil ||
		fact.LongContextOutputUnitPrice != nil || fact.LongContextCacheReadUnitPrice != nil ||
		fact.LongContextCacheWriteUnitPrice != nil || fact.GroupRatio != nil || fact.CompletionRatio != nil {
		return errors.New("flat price fact contains tiered pricing fields")
	}
	switch fact.Operation {
	case GatewayPricingOperationTextGeneration:
		if fact.Unit != GatewayPricingUnitPerMillionTokens || fact.OperationUnitPrice != nil ||
			fact.InputUnitPrice == nil && fact.OutputUnitPrice == nil && fact.CacheReadUnitPrice == nil && fact.CacheWriteUnitPrice == nil {
			return errors.New("text price fact must contain token prices only")
		}
	case GatewayPricingOperationEmbedding:
		if fact.Unit != GatewayPricingUnitPerMillionTokens || fact.InputUnitPrice == nil ||
			fact.OutputUnitPrice != nil || fact.CacheReadUnitPrice != nil || fact.CacheWriteUnitPrice != nil || fact.OperationUnitPrice != nil {
			return errors.New("embedding price fact must contain exactly one input token price")
		}
	case GatewayImageOperationGeneration:
		if fact.Unit != GatewayPricingUnitPerOperation || fact.OperationUnitPrice == nil ||
			fact.InputUnitPrice != nil || fact.OutputUnitPrice != nil || fact.CacheReadUnitPrice != nil || fact.CacheWriteUnitPrice != nil {
			return errors.New("image price fact must contain exactly one operation price")
		}
	default:
		return errors.New("request price fact operation is unsupported")
	}
	return nil
}

func normalizeGatewayPriceScope(scope *GatewayRequestPriceScope) error {
	if scope == nil {
		return errors.New("request price scope is required")
	}
	scope.CanonicalGroupName = strings.TrimSpace(scope.CanonicalGroupName)
	scope.ModelName = strings.TrimSpace(scope.ModelName)
	scope.Operation = strings.ToLower(strings.TrimSpace(scope.Operation))
	scope.Protocol = strings.ToLower(strings.TrimSpace(scope.Protocol))
	if scope.ChannelID == 0 || scope.ChannelAccountIdentityRevision == 0 ||
		scope.UpstreamGroupID == 0 || scope.CanonicalGroupName == "" ||
		scope.UpstreamGroupPresenceGeneration == 0 || scope.GatewayUpstreamKeyID == 0 ||
		scope.UpstreamKeyGeneration == 0 || scope.ModelName == "" ||
		scope.Operation == "" || scope.Protocol == "" {
		return errors.New("request price scope is incomplete")
	}
	if !utf8.ValidString(scope.CanonicalGroupName) || utf8.RuneCountInString(scope.CanonicalGroupName) > 128 ||
		!utf8.ValidString(scope.ModelName) || utf8.RuneCountInString(scope.ModelName) > 256 ||
		!utf8.ValidString(scope.Operation) || utf8.RuneCountInString(scope.Operation) > 32 ||
		!utf8.ValidString(scope.Protocol) || utf8.RuneCountInString(scope.Protocol) > 32 {
		return errors.New("request price scope text is invalid")
	}
	return nil
}

func gatewayRequestPriceScopeFromFact(fact *GatewayRequestPriceFact) GatewayRequestPriceScope {
	if fact == nil {
		return GatewayRequestPriceScope{}
	}
	return GatewayRequestPriceScope{
		ChannelID: fact.ChannelID, ChannelAccountIdentityRevision: fact.ChannelAccountIdentityRevision,
		UpstreamGroupID: fact.UpstreamGroupID, CanonicalGroupName: fact.CanonicalGroupName,
		UpstreamGroupPresenceGeneration: fact.UpstreamGroupPresenceGeneration,
		GatewayUpstreamKeyID:            fact.GatewayUpstreamKeyID, UpstreamKeyGeneration: fact.UpstreamKeyGeneration,
		ModelName: fact.ModelName, Operation: fact.Operation, Protocol: fact.Protocol,
	}
}

func normalizeGatewayRequestPriceFact(fact *GatewayRequestPriceFact) error {
	if fact == nil {
		return errors.New("request price fact is required")
	}
	scope := gatewayRequestPriceScopeFromFact(fact)
	if err := normalizeGatewayPriceScope(&scope); err != nil {
		return err
	}
	fact.ChannelID = scope.ChannelID
	fact.ChannelAccountIdentityRevision = scope.ChannelAccountIdentityRevision
	fact.UpstreamGroupID = scope.UpstreamGroupID
	fact.CanonicalGroupName = scope.CanonicalGroupName
	fact.UpstreamGroupPresenceGeneration = scope.UpstreamGroupPresenceGeneration
	fact.GatewayUpstreamKeyID = scope.GatewayUpstreamKeyID
	fact.UpstreamKeyGeneration = scope.UpstreamKeyGeneration
	fact.ModelName = scope.ModelName
	fact.Operation = scope.Operation
	fact.Protocol = scope.Protocol
	fact.Unit = strings.ToLower(strings.TrimSpace(fact.Unit))
	fact.Currency = strings.ToUpper(strings.TrimSpace(fact.Currency))
	fact.Source = strings.ToLower(strings.TrimSpace(fact.Source))
	if !gatewayPricingCurrencyPattern.MatchString(fact.Currency) || !gatewayPricingSourcePattern.MatchString(fact.Source) {
		return errors.New("request price fact currency or source is invalid")
	}
	if fact.Currency != gatewayRequestPriceSupportedCurrency {
		return fmt.Errorf("request price fact currency %s is unsupported: only %s is billable", fact.Currency, gatewayRequestPriceSupportedCurrency)
	}
	if fact.ValidFrom.IsZero() {
		return errors.New("request price fact valid_from is required")
	}
	fact.ValidFrom = normalizeGatewayRequestPricingTime(fact.ValidFrom)
	if fact.ObservedAt.IsZero() {
		return errors.New("request price fact observed_at is required")
	}
	fact.ObservedAt = normalizeGatewayRequestPricingTime(fact.ObservedAt)
	if fact.ObservedAt.After(fact.ValidFrom) {
		return errors.New("request price fact observed_at must not be after valid_from")
	}
	if fact.ValidUntil != nil {
		value := normalizeGatewayRequestPricingTime(*fact.ValidUntil)
		if !value.After(fact.ValidFrom) {
			return errors.New("request price fact validity window is invalid")
		}
		fact.ValidUntil = &value
	}
	if fact.LongContextThresholdTokens != nil &&
		(*fact.LongContextThresholdTokens <= 0 || *fact.LongContextThresholdTokens == math.MaxInt64) {
		return errors.New("request price fact long-context threshold is unreachable")
	}
	for _, price := range []*float64{
		fact.InputUnitPrice, fact.OutputUnitPrice, fact.CacheReadUnitPrice, fact.CacheWriteUnitPrice,
		fact.LongContextInputUnitPrice, fact.LongContextOutputUnitPrice,
		fact.LongContextCacheReadUnitPrice, fact.LongContextCacheWriteUnitPrice,
		fact.GroupRatio, fact.CompletionRatio, fact.OperationUnitPrice,
	} {
		if price != nil && (*price < 0 || math.IsNaN(*price) || math.IsInf(*price, 0)) {
			return errors.New("request price fact contains an invalid unit price")
		}
	}
	if err := validateGatewayRequestPriceFactShape(fact); err != nil {
		return err
	}
	if fact.SchemaVersion == 0 {
		fact.SchemaVersion = GatewayRequestPriceFactSchemaVersion
	}
	if !gatewayRequestPriceFactSchemaSupported(fact.SchemaVersion) {
		return errors.New("request price fact schema is unsupported")
	}
	return nil
}

func gatewayRequestPriceFactSchemaSupported(schemaVersion int) bool {
	return schemaVersion == GatewayRequestPriceFactSchemaVersion ||
		schemaVersion == GatewayRequestPriceFactTieredSchemaVersion
}

func validGatewayRequestPriceLongContextThreshold(threshold int64) bool {
	return threshold > 0 && threshold < math.MaxInt64
}

type gatewayRequestPriceChecksumPayload struct {
	ChannelID                       uint     `json:"channel_id"`
	ChannelAccountIdentityRevision  uint64   `json:"channel_account_identity_revision"`
	CostUnitEpoch                   uint64   `json:"cost_unit_epoch"`
	UpstreamGroupID                 uint     `json:"upstream_group_id"`
	CanonicalGroupName              string   `json:"canonical_group_name"`
	UpstreamGroupPresenceGeneration uint64   `json:"upstream_group_presence_generation"`
	GatewayUpstreamKeyID            uint     `json:"gateway_upstream_key_id"`
	UpstreamKeyGeneration           uint64   `json:"upstream_key_generation"`
	ModelName                       string   `json:"model_name"`
	Operation                       string   `json:"operation"`
	Protocol                        string   `json:"protocol"`
	InputUnitPrice                  *float64 `json:"input_unit_price"`
	OutputUnitPrice                 *float64 `json:"output_unit_price"`
	CacheReadUnitPrice              *float64 `json:"cache_read_unit_price"`
	CacheWriteUnitPrice             *float64 `json:"cache_write_unit_price"`
	OperationUnitPrice              *float64 `json:"operation_unit_price"`
	Unit                            string   `json:"unit"`
	Currency                        string   `json:"currency"`
	Source                          string   `json:"source"`
	ObservedAt                      string   `json:"observed_at"`
	ValidFrom                       string   `json:"valid_from"`
	ValidUntil                      *string  `json:"valid_until"`
	FactVersion                     uint64   `json:"fact_version"`
	SchemaVersion                   int      `json:"schema_version"`
}

type gatewayRequestTieredPriceChecksumPayload struct {
	gatewayRequestPriceChecksumPayload
	LongContextThresholdTokens     *int64   `json:"long_context_threshold_tokens"`
	LongContextInputUnitPrice      *float64 `json:"long_context_input_unit_price"`
	LongContextOutputUnitPrice     *float64 `json:"long_context_output_unit_price"`
	LongContextCacheReadUnitPrice  *float64 `json:"long_context_cache_read_unit_price"`
	LongContextCacheWriteUnitPrice *float64 `json:"long_context_cache_write_unit_price"`
	GroupRatio                     *float64 `json:"group_ratio"`
	CompletionRatio                *float64 `json:"completion_ratio"`
}

func gatewayRequestPriceFactChecksum(fact *GatewayRequestPriceFact) (string, error) {
	if fact == nil {
		return "", errors.New("request price fact is required")
	}
	var validUntil *string
	if fact.ValidUntil != nil {
		value := normalizeGatewayRequestPricingTime(*fact.ValidUntil).Format(time.RFC3339Nano)
		validUntil = &value
	}
	payload := gatewayRequestPriceChecksumPayload{
		ChannelID: fact.ChannelID, ChannelAccountIdentityRevision: fact.ChannelAccountIdentityRevision,
		CostUnitEpoch:   fact.CostUnitEpoch,
		UpstreamGroupID: fact.UpstreamGroupID, CanonicalGroupName: fact.CanonicalGroupName,
		UpstreamGroupPresenceGeneration: fact.UpstreamGroupPresenceGeneration,
		GatewayUpstreamKeyID:            fact.GatewayUpstreamKeyID, UpstreamKeyGeneration: fact.UpstreamKeyGeneration,
		ModelName: fact.ModelName, Operation: fact.Operation, Protocol: fact.Protocol,
		InputUnitPrice: fact.InputUnitPrice, OutputUnitPrice: fact.OutputUnitPrice,
		CacheReadUnitPrice: fact.CacheReadUnitPrice, CacheWriteUnitPrice: fact.CacheWriteUnitPrice,
		OperationUnitPrice: fact.OperationUnitPrice, Unit: fact.Unit, Currency: fact.Currency, Source: fact.Source,
		ObservedAt: normalizeGatewayRequestPricingTime(fact.ObservedAt).Format(time.RFC3339Nano),
		ValidFrom:  normalizeGatewayRequestPricingTime(fact.ValidFrom).Format(time.RFC3339Nano), ValidUntil: validUntil,
		FactVersion: fact.FactVersion, SchemaVersion: fact.SchemaVersion,
	}
	var checksumPayload any = payload
	if fact.SchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
		checksumPayload = gatewayRequestTieredPriceChecksumPayload{
			gatewayRequestPriceChecksumPayload: payload,
			LongContextThresholdTokens:         fact.LongContextThresholdTokens,
			LongContextInputUnitPrice:          fact.LongContextInputUnitPrice,
			LongContextOutputUnitPrice:         fact.LongContextOutputUnitPrice,
			LongContextCacheReadUnitPrice:      fact.LongContextCacheReadUnitPrice,
			LongContextCacheWriteUnitPrice:     fact.LongContextCacheWriteUnitPrice,
			GroupRatio:                         fact.GroupRatio,
			CompletionRatio:                    fact.CompletionRatio,
		}
	}
	encoded, err := json.Marshal(checksumPayload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func gatewayRequestPriceScopeQuery(tx *gorm.DB, scope GatewayRequestPriceScope) *gorm.DB {
	return tx.Where(
		"channel_id = ? AND channel_account_identity_revision = ? AND upstream_group_id = ? AND canonical_group_name = ? AND upstream_group_presence_generation = ? AND gateway_upstream_key_id = ? AND upstream_key_generation = ? AND model_name = ? AND operation = ? AND protocol = ?",
		scope.ChannelID, scope.ChannelAccountIdentityRevision, scope.UpstreamGroupID, scope.CanonicalGroupName,
		scope.UpstreamGroupPresenceGeneration, scope.GatewayUpstreamKeyID, scope.UpstreamKeyGeneration,
		scope.ModelName, scope.Operation, scope.Protocol,
	)
}

func gatewayRequestPriceScopeEpochQuery(tx *gorm.DB, scope GatewayRequestPriceScope, costUnitEpoch uint64) *gorm.DB {
	return gatewayRequestPriceScopeQuery(tx, scope).Where("cost_unit_epoch = ?", costUnitEpoch)
}

// AppendRequestPriceFact appends a new immutable fact. Storage allocates the
// version while holding the table append lock.
func (g *Gateway) AppendRequestPriceFact(fact *GatewayRequestPriceFact) error {
	if g == nil || g.db == nil {
		return errors.New("gateway storage is required")
	}
	if fact != nil && fact.FactVersion != 0 {
		return errors.New("request price fact version is assigned by storage")
	}
	if err := normalizeGatewayRequestPriceFact(fact); err != nil {
		return err
	}
	if fact.Source == GatewayRequestPriceSourceAdministrator {
		return errors.New("administrator request price facts must use the authenticated import path")
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if tx.Dialector != nil && tx.Dialector.Name() == string(DBDriverPostgres) {
			if err := tx.Exec("LOCK TABLE public.gateway_request_price_facts IN SHARE ROW EXCLUSIVE MODE").Error; err != nil {
				return err
			}
		}
		return appendRequestPriceFactTx(tx, fact, time.Now())
	})
}

func appendRequestPriceFactTx(tx *gorm.DB, fact *GatewayRequestPriceFact, createdAt time.Time) error {
	return appendRequestPriceFactWithIdentityPolicyTx(tx, fact, createdAt, false)
}

// Generated catalog prices still freeze the exact channel, group and key
// generations, but do not require the provider to expose a durable account ID.
// Their source is restricted here so callers cannot use this path to bypass
// account binding for arbitrary externally supplied facts.
func appendGeneratedRequestPriceFactTx(tx *gorm.DB, fact *GatewayRequestPriceFact, createdAt time.Time) error {
	return appendRequestPriceFactWithIdentityPolicyTx(tx, fact, createdAt, true)
}

func appendModelConfigRequestPriceFactTx(tx *gorm.DB, fact *GatewayRequestPriceFact, createdAt time.Time) error {
	return appendGeneratedRequestPriceFactTx(tx, fact, createdAt)
}

func appendRequestPriceFactWithIdentityPolicyTx(tx *gorm.DB, fact *GatewayRequestPriceFact, createdAt time.Time, allowModelConfigWithoutAccountBinding bool) error {
	if tx == nil {
		return errors.New("gateway storage transaction is required")
	}
	if fact == nil {
		return errors.New("request price fact is required")
	}
	if fact.FactVersion > math.MaxInt64 {
		return errors.New("request price fact version exceeds PostgreSQL bigint")
	}
	fact.ID = 0
	canonicalGroup, err := canonicalUpstreamGroupName(tx, fact.CanonicalGroupName)
	if err != nil {
		return err
	}
	fact.CanonicalGroupName = canonicalGroup
	if err := normalizeGatewayRequestPriceFact(fact); err != nil {
		return err
	}
	if allowModelConfigWithoutAccountBinding && fact.Source != gatewayRequestPriceSourceModelConfig &&
		fact.Source != gatewayChannelModelPriceSource {
		return errors.New("account-binding exemption is restricted to generated model prices")
	}
	if err := requireCurrentGatewayRequestPriceIdentityTx(tx, fact, fact.ObservedAt, !allowModelConfigWithoutAccountBinding); err != nil {
		return err
	}
	if fact.FactVersion == 0 {
		scope := gatewayRequestPriceScopeFromFact(fact)
		var latestVersion struct {
			Version uint64
		}
		if err := gatewayRequestPriceScopeEpochQuery(tx.Model(&GatewayRequestPriceFact{}), scope, fact.CostUnitEpoch).
			Select("COALESCE(MAX(fact_version), 0) AS version").Scan(&latestVersion).Error; err != nil {
			return err
		}
		var latestSourceObservation struct {
			ObservedAt *time.Time
		}
		if err := gatewayRequestPriceScopeEpochQuery(tx.Model(&GatewayRequestPriceFact{}), scope, fact.CostUnitEpoch).
			Where("source = ?", fact.Source).
			Select("MAX(observed_at) AS observed_at").Scan(&latestSourceObservation).Error; err != nil {
			return err
		}
		if latestSourceObservation.ObservedAt != nil {
			currentObservedAt := normalizeGatewayRequestPricingTime(*latestSourceObservation.ObservedAt)
			switch {
			case fact.ObservedAt.Before(currentObservedAt):
				return ErrGatewayRequestPriceObservationStale
			case fact.ObservedAt.Equal(currentObservedAt):
				return ErrGatewayRequestPriceObservationConflict
			}
		}
		if latestVersion.Version >= math.MaxInt64 {
			return errors.New("request price fact version is exhausted")
		}
		fact.FactVersion = latestVersion.Version + 1
	}
	checksum, err := gatewayRequestPriceFactChecksum(fact)
	if err != nil {
		return err
	}
	fact.Checksum = checksum
	fact.CreatedAt = normalizeGatewayRequestPricingTime(createdAt)
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(fact)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errGatewayRequestPriceAppendConflict
	}
	return nil
}

type gatewayRequestPriceChannelIdentity struct {
	ID                              uint
	Provider                        ChannelType
	SiteURL                         string
	APIURL                          string
	UpstreamAccountIdentityRevision uint64
	AdminRevision                   uint64
	CostUnitEpoch                   uint64
}

func loadGatewayRequestPriceChannelIdentityTx(tx *gorm.DB, channelID uint) (gatewayRequestPriceChannelIdentity, error) {
	var channel gatewayRequestPriceChannelIdentity
	result := tx.Table("channels AS channel_row").Clauses(clause.Locking{Strength: "SHARE"}).
		Select(`channel_row.id, channel_row.type AS provider, channel_row.site_url, channel_row.api_url,
			channel_row.upstream_account_identity_revision, channel_row.admin_revision,
			COALESCE((to_jsonb(channel_row)->>'cost_unit_epoch')::bigint, 0) AS cost_unit_epoch`).
		Where("channel_row.id = ?", channelID).Take(&channel)
	return channel, result.Error
}

func requireGatewayRequestPriceAccountBindingTx(tx *gorm.DB, channel gatewayRequestPriceChannelIdentity, observedAt time.Time) error {
	if tx == nil || channel.ID == 0 || channel.Provider == "" || channel.UpstreamAccountIdentityRevision == 0 || observedAt.IsZero() {
		return fmt.Errorf("%w: channel account binding is incomplete", ErrGatewayRequestPriceIdentityChanged)
	}
	switch channel.Provider {
	case ChannelTypeNewAPI:
		return fmt.Errorf("%w: provider does not expose a durable authenticated account binding", ErrGatewayRequestPriceIdentityChanged)
	case ChannelTypeSub2API:
	default:
		return fmt.Errorf("%w: channel provider is unsupported", ErrGatewayRequestPriceIdentityChanged)
	}
	observedAt = normalizeGatewayRequestPricingTime(observedAt)
	var binding GatewayChannelUpstreamAccountBinding
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Where("channel_id = ? AND channel_identity_revision = ?", channel.ID, channel.UpstreamAccountIdentityRevision).
		Take(&binding).Error; err != nil {
		return fmt.Errorf("%w: current authenticated account binding is unavailable: %v", ErrGatewayRequestPriceIdentityChanged, err)
	}
	if binding.UpstreamAccountID == 0 || binding.Provider != channel.Provider || strings.TrimSpace(binding.Source) == "" || binding.ObservedAt.IsZero() {
		return fmt.Errorf("%w: current authenticated account binding is stale", ErrGatewayRequestPriceIdentityChanged)
	}
	if normalizeGatewayRequestPricingTime(binding.ObservedAt).After(observedAt) {
		var priorBindingCount int64
		if err := tx.Model(&GatewayChannelUpstreamAccountBindingAudit{}).
			Where("channel_id = ? AND provider = ? AND channel_identity_revision = ? AND upstream_account_id = ? AND observed_at <= ?",
				channel.ID, channel.Provider, channel.UpstreamAccountIdentityRevision, binding.UpstreamAccountID, observedAt).
			Count(&priorBindingCount).Error; err != nil {
			return fmt.Errorf("%w: authenticated account binding history is unavailable: %v", ErrGatewayRequestPriceIdentityChanged, err)
		}
		if priorBindingCount == 0 {
			return fmt.Errorf("%w: authenticated account was not bound at observed_at", ErrGatewayRequestPriceIdentityChanged)
		}
	}
	var account GatewayUpstreamAccount
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).First(&account, binding.UpstreamAccountID).Error; err != nil {
		return fmt.Errorf("%w: current authenticated account is unavailable: %v", ErrGatewayRequestPriceIdentityChanged, err)
	}
	configuredOrigin := strings.TrimSpace(channel.APIURL)
	if configuredOrigin == "" {
		configuredOrigin = channel.SiteURL
	}
	configuredOrigin, err := normalizeUpstreamAccountAPIOrigin(configuredOrigin)
	if err != nil || account.Provider != channel.Provider ||
		account.APIOriginHash != hashUpstreamAccountIdentity(configuredOrigin) ||
		!gatewayPricingChecksumPattern.MatchString(account.ExternalAccountIDHash) {
		return fmt.Errorf("%w: current authenticated account API origin changed", ErrGatewayRequestPriceIdentityChanged)
	}
	return nil
}

func requireCurrentGatewayRequestPriceIdentityTx(tx *gorm.DB, fact *GatewayRequestPriceFact, observedAt time.Time, requireAccountBinding bool) error {
	channel, err := loadGatewayRequestPriceChannelIdentityTx(tx, fact.ChannelID)
	if err != nil {
		return err
	}
	if channel.UpstreamAccountIdentityRevision != fact.ChannelAccountIdentityRevision {
		return fmt.Errorf("%w: channel account revision changed", ErrGatewayRequestPriceIdentityChanged)
	}
	if fact.CostUnitEpoch == 0 {
		fact.CostUnitEpoch = channel.CostUnitEpoch
	}
	if channel.CostUnitEpoch == 0 || channel.CostUnitEpoch != fact.CostUnitEpoch {
		return fmt.Errorf("%w: channel cost-unit epoch changed", ErrGatewayRequestPriceIdentityChanged)
	}
	if requireAccountBinding {
		if err := requireGatewayRequestPriceAccountBindingTx(tx, channel, observedAt); err != nil {
			return err
		}
	}
	var group GatewayUpstreamGroup
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Select("id", "channel_id", "canonical_group_name", "presence_generation", "present").First(&group, fact.UpstreamGroupID).Error; err != nil {
		return err
	}
	if !gatewayRequestPriceFactMatchesGroupIdentity(fact, group) {
		return fmt.Errorf("%w: upstream group generation changed", ErrGatewayRequestPriceIdentityChanged)
	}
	var key GatewayUpstreamKey
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Select("id", "channel_id", "group_name", "key_generation").First(&key, fact.GatewayUpstreamKeyID).Error; err != nil {
		return err
	}
	keyCanonicalGroup, err := canonicalUpstreamGroupName(tx, key.GroupName)
	if err != nil {
		return err
	}
	if key.ChannelID != fact.ChannelID || key.KeyGeneration != fact.UpstreamKeyGeneration || keyCanonicalGroup != fact.CanonicalGroupName {
		return fmt.Errorf("%w: upstream key generation changed", ErrGatewayRequestPriceIdentityChanged)
	}
	return nil
}

func gatewayRequestPriceFactMatchesGroupIdentity(fact *GatewayRequestPriceFact, group GatewayUpstreamGroup) bool {
	if fact == nil {
		return false
	}
	// Both names were canonicalized by PostgreSQL. Comparing them byte-for-byte
	// avoids introducing Go's different Unicode case-folding rules.
	return group.ChannelID == fact.ChannelID && group.Present &&
		group.PresenceGeneration == fact.UpstreamGroupPresenceGeneration &&
		group.CanonicalGroupName == fact.CanonicalGroupName
}

func (g *Gateway) FindCurrentRequestPriceFact(scope GatewayRequestPriceScope, at time.Time) (GatewayRequestPriceFactLookup, error) {
	if g == nil || g.db == nil {
		return GatewayRequestPriceFactLookup{}, errors.New("gateway storage is required")
	}
	canonicalGroup, err := canonicalUpstreamGroupName(g.db, scope.CanonicalGroupName)
	if err != nil {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableScopeIncomplete}, nil
	}
	scope.CanonicalGroupName = canonicalGroup
	if err := normalizeGatewayPriceScope(&scope); err != nil {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableScopeIncomplete}, nil
	}
	channel, err := loadGatewayRequestPriceChannelIdentityTx(g.db, scope.ChannelID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableIdentityChanged}, nil
		}
		return GatewayRequestPriceFactLookup{}, err
	}
	if channel.UpstreamAccountIdentityRevision != scope.ChannelAccountIdentityRevision {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableIdentityChanged}, nil
	}
	lookup, err := findCurrentGatewayRequestPriceFactForEpochTx(g.db, scope, channel.CostUnitEpoch, at)
	if err != nil || lookup.Fact != nil || lookup.UnavailableReason != GatewayPricingUnavailableFactMissing {
		return lookup, err
	}
	var priorEpochTotal int64
	if err := gatewayRequestPriceScopeQuery(g.db.Model(&GatewayRequestPriceFact{}), scope).Count(&priorEpochTotal).Error; err != nil {
		return GatewayRequestPriceFactLookup{}, err
	}
	if priorEpochTotal > 0 {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableCostBasisChanged}, nil
	}
	return lookup, nil
}

func findCurrentGatewayRequestPriceFactForEpochTx(tx *gorm.DB, scope GatewayRequestPriceScope, costUnitEpoch uint64, at time.Time) (GatewayRequestPriceFactLookup, error) {
	at = normalizeGatewayRequestPricingTime(at)
	if at.IsZero() {
		at = time.Now().UTC()
	}
	var fact GatewayRequestPriceFact
	query := gatewayRequestPriceScopeEpochQuery(tx.Model(&GatewayRequestPriceFact{}), scope, costUnitEpoch).
		Where("valid_from <= ?", at).
		Order("fact_version DESC, valid_from DESC, id DESC")
	if err := query.First(&fact).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return GatewayRequestPriceFactLookup{}, err
		}
		var total int64
		if err := gatewayRequestPriceScopeEpochQuery(tx.Model(&GatewayRequestPriceFact{}), scope, costUnitEpoch).Count(&total).Error; err != nil {
			return GatewayRequestPriceFactLookup{}, err
		}
		if total == 0 {
			return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactMissing}, nil
		}
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactNotYetValid}, nil
	}
	if fact.ValidUntil != nil && !fact.ValidUntil.After(at) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactExpired}, nil
	}
	if !gatewayRequestPriceFactSchemaSupported(fact.SchemaVersion) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableSchemaUnsupported}, nil
	}
	checksum, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		return GatewayRequestPriceFactLookup{}, err
	}
	if !strings.EqualFold(checksum, fact.Checksum) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableChecksumMismatch}, nil
	}
	return GatewayRequestPriceFactLookup{Fact: &fact}, nil
}

func findCurrentGatewayRequestPriceFactTx(tx *gorm.DB, scope GatewayRequestPriceScope, at time.Time) (GatewayRequestPriceFactLookup, error) {
	at = normalizeGatewayRequestPricingTime(at)
	if at.IsZero() {
		at = time.Now().UTC()
	}
	var fact GatewayRequestPriceFact
	query := gatewayRequestPriceScopeQuery(tx.Model(&GatewayRequestPriceFact{}), scope).
		Where("valid_from <= ?", at).
		Order("fact_version DESC, valid_from DESC, id DESC")
	if err := query.First(&fact).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return GatewayRequestPriceFactLookup{}, err
		}
		var total int64
		base := gatewayRequestPriceScopeQuery(tx.Model(&GatewayRequestPriceFact{}), scope)
		if err := base.Count(&total).Error; err != nil {
			return GatewayRequestPriceFactLookup{}, err
		}
		if total == 0 {
			return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactMissing}, nil
		}
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactNotYetValid}, nil
	}
	// Version is authoritative once it has started. If that version expires,
	// fail closed instead of reviving an older open-ended observation.
	if fact.ValidUntil != nil && !fact.ValidUntil.After(at) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableFactExpired}, nil
	}
	if !gatewayRequestPriceFactSchemaSupported(fact.SchemaVersion) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableSchemaUnsupported}, nil
	}
	checksum, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		return GatewayRequestPriceFactLookup{}, err
	}
	if !strings.EqualFold(checksum, fact.Checksum) {
		return GatewayRequestPriceFactLookup{UnavailableReason: GatewayPricingUnavailableChecksumMismatch}, nil
	}
	return GatewayRequestPriceFactLookup{Fact: &fact}, nil
}

func EvaluateGatewayRequestPriceFact(fact *GatewayRequestPriceFact, usage GatewayRequestPricingUsage) GatewayRequestPriceEstimate {
	result := GatewayRequestPriceEstimate{Status: GatewayCostStatusUnavailable, UsagePresence: usage.PresenceMask()}
	if fact == nil {
		result.UnavailableReason = GatewayPricingUnavailableFactMissing
		return result
	}
	if !gatewayRequestPriceFactSchemaSupported(fact.SchemaVersion) {
		result.UnavailableReason = GatewayPricingUnavailableSchemaUnsupported
		return result
	}
	if err := validateGatewayRequestPriceFactShape(fact); err != nil {
		result.UnavailableReason = GatewayPricingUnavailableSchemaUnsupported
		return result
	}
	var cost float64
	switch fact.Operation {
	case GatewayPricingOperationTextGeneration:
		if result.UsagePresence&gatewayPricingTokenUsagePresence != gatewayPricingTokenUsagePresence {
			result.UnavailableReason = GatewayPricingUnavailableUsageIncomplete
			return result
		}
		inputPrice := fact.InputUnitPrice
		outputPrice := fact.OutputUnitPrice
		cacheReadPrice := fact.CacheReadUnitPrice
		cacheWritePrice := fact.CacheWriteUnitPrice
		inputRatio := 1.0
		outputRatio := 1.0
		if fact.SchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
			promptTokens, ok := checkedGatewayRequestPricingAdd(*usage.InputTokens, *usage.CacheReadTokens)
			if ok {
				promptTokens, ok = checkedGatewayRequestPricingAdd(promptTokens, *usage.CacheWriteTokens)
			}
			if !ok {
				result.UnavailableReason = GatewayPricingUnavailableUsageInvalid
				return result
			}
			if fact.LongContextThresholdTokens != nil && promptTokens > *fact.LongContextThresholdTokens {
				inputPrice = fact.LongContextInputUnitPrice
				outputPrice = fact.LongContextOutputUnitPrice
				cacheReadPrice = fact.LongContextCacheReadUnitPrice
				cacheWritePrice = fact.LongContextCacheWriteUnitPrice
			}
			inputRatio = *fact.GroupRatio
			outputRatio = *fact.CompletionRatio
		}
		components := []struct {
			usage *int64
			price *float64
			ratio float64
		}{
			{usage.InputTokens, inputPrice, inputRatio},
			{usage.OutputTokens, outputPrice, outputRatio},
			{usage.CacheReadTokens, cacheReadPrice, inputRatio},
			{usage.CacheWriteTokens, cacheWritePrice, inputRatio},
		}
		for _, component := range components {
			if component.usage == nil || *component.usage < 0 {
				result.UnavailableReason = GatewayPricingUnavailableUsageInvalid
				return result
			}
			if *component.usage == 0 {
				continue
			}
			if component.price == nil {
				result.UnavailableReason = GatewayPricingUnavailablePriceMissing
				return result
			}
			cost += float64(*component.usage) * *component.price * component.ratio / 1_000_000
		}
	case GatewayPricingOperationEmbedding:
		if usage.InputTokens == nil {
			result.UnavailableReason = GatewayPricingUnavailableUsageIncomplete
			return result
		}
		if *usage.InputTokens < 0 {
			result.UnavailableReason = GatewayPricingUnavailableUsageInvalid
			return result
		}
		cost = float64(*usage.InputTokens) * *fact.InputUnitPrice / 1_000_000
	case GatewayImageOperationGeneration:
		if usage.OperationUnits == nil {
			result.UnavailableReason = GatewayPricingUnavailableUsageIncomplete
			return result
		}
		if *usage.OperationUnits < 0 {
			result.UnavailableReason = GatewayPricingUnavailableUsageInvalid
			return result
		}
		if *usage.OperationUnits > 0 && fact.OperationUnitPrice == nil {
			result.UnavailableReason = GatewayPricingUnavailablePriceMissing
			return result
		}
		if fact.OperationUnitPrice != nil {
			cost = float64(*usage.OperationUnits) * *fact.OperationUnitPrice
		}
	default:
		result.UnavailableReason = GatewayPricingUnavailableSchemaUnsupported
		return result
	}
	if cost < 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		result.UnavailableReason = GatewayPricingUnavailableUsageInvalid
		return result
	}
	result.Cost = &cost
	result.Currency = fact.Currency
	result.Status = GatewayCostStatusEstimated
	return result
}

func gatewayPricingIdentityForRequest(method, path string) (operation, protocol, usageSchema, unavailableReason string) {
	if !strings.EqualFold(strings.TrimSpace(method), "POST") {
		return "", "", "", GatewayPricingUnavailableOperationUnsupported
	}
	switch strings.TrimSpace(path) {
	case "/v1/images/generations":
		return GatewayImageOperationGeneration, GatewayImageProtocolImages, GatewayPricingUsageSchemaOperationV1, ""
	case "/v1/chat/completions":
		return GatewayPricingOperationTextGeneration, GatewayPricingProtocolOpenAIChatCompletions, GatewayPricingUsageSchemaTokensV1, ""
	case "/v1/completions":
		return GatewayPricingOperationTextGeneration, GatewayPricingProtocolOpenAICompletions, GatewayPricingUsageSchemaTokensV1, ""
	case "/v1/messages":
		return GatewayPricingOperationTextGeneration, GatewayPricingProtocolAnthropicMessages, GatewayPricingUsageSchemaTokensV1, ""
	case "/v1/embeddings":
		return GatewayPricingOperationEmbedding, GatewayPricingProtocolOpenAIEmbeddings, GatewayPricingUsageSchemaInputTokensV1, ""
	case "/v1/responses":
		// The path alone cannot distinguish a text response from the paid
		// image_generation tool. The request parser must supply the operation.
		return "", GatewayPricingProtocolOpenAIResponses, "", GatewayPricingUnavailableOperationAmbiguous
	default:
		return "", "", "", GatewayPricingUnavailableOperationUnsupported
	}
}

func gatewayRequestPricingTimePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = normalizeGatewayRequestPricingTime(value)
	return &value
}

func cloneGatewayRequestPricingTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := normalizeGatewayRequestPricingTime(*value)
	return &clone
}

func gatewayRequestPricingUsageSchemaForOperation(operation string) string {
	switch strings.ToLower(strings.TrimSpace(operation)) {
	case GatewayImageOperationGeneration:
		return GatewayPricingUsageSchemaOperationV1
	case GatewayPricingOperationTextGeneration:
		return GatewayPricingUsageSchemaTokensV1
	case GatewayPricingOperationEmbedding:
		return GatewayPricingUsageSchemaInputTokensV1
	default:
		return ""
	}
}

func normalizeGatewayRequestPricingUsageSchema(schema string, version int) (string, error) {
	schema = strings.ToLower(strings.TrimSpace(schema))
	if schema == "" {
		if version != 0 {
			return "", errors.New("empty request pricing usage schema must use version zero")
		}
		return "", nil
	}
	if version != 1 {
		return "", errors.New("request pricing usage schema version is unsupported")
	}
	switch schema {
	case GatewayPricingUsageSchemaTokensV1, GatewayPricingUsageSchemaInputTokensV1, GatewayPricingUsageSchemaOperationV1:
		return schema, nil
	default:
		return "", errors.New("request pricing usage schema is unsupported")
	}
}

func validateGatewayRequestPricingContract(operation, protocol, usageSchema string) error {
	if operation == "" {
		if usageSchema != "" || protocol != "" && protocol != GatewayPricingProtocolOpenAIResponses {
			return errors.New("request pricing operation, protocol, and usage schema do not match")
		}
		return nil
	}
	if !gatewayRequestPricingProtocolSupported(operation, protocol) ||
		usageSchema != gatewayRequestPricingUsageSchemaForOperation(operation) {
		return errors.New("request pricing operation, protocol, and usage schema do not match")
	}
	return nil
}

func normalizeGatewayRequestDispatchPricingUsage(audit *GatewayRequestDispatchAudit) error {
	if audit == nil {
		return errors.New("request dispatch pricing usage is required")
	}
	usageSchema, err := normalizeGatewayRequestPricingUsageSchema(audit.UsageSchema, audit.UsageSchemaVersion)
	if err != nil {
		return err
	}
	audit.UsageSchema = usageSchema
	if err := validateGatewayRequestPricingContract(audit.PriceOperation, audit.PriceProtocol, audit.UsageSchema); err != nil {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(audit.Method), "POST") && strings.TrimSpace(audit.Path) == "/v1/images/generations" &&
		audit.PriceOperation == GatewayImageOperationGeneration && audit.PriceProtocol == GatewayImageProtocolImages &&
		audit.UsageSchema == GatewayPricingUsageSchemaOperationV1 {
		operationUnits := int64(1)
		audit.UsageOperationPresent = true
		audit.OperationUnits = &operationUnits
	}
	if audit.UsageOperationPresent != (audit.OperationUnits != nil) || audit.OperationUnits != nil && *audit.OperationUnits < 0 {
		return errors.New("request dispatch pricing operation usage is invalid")
	}
	if audit.UsageSchema != GatewayPricingUsageSchemaOperationV1 && (audit.UsageOperationPresent || audit.OperationUnits != nil) {
		return errors.New("request dispatch pricing operation usage does not match its schema")
	}
	if audit.PriceOperation == GatewayImageOperationGeneration && audit.UsageSchema == GatewayPricingUsageSchemaOperationV1 &&
		(!audit.UsageOperationPresent || audit.OperationUnits == nil || *audit.OperationUnits != 1) {
		return errors.New("image request pricing must carry one operation unit")
	}
	return nil
}

// freezeGatewayRequestPricingTx resolves mutable route identities and the
// current fact in the same transaction that appends the pre-send audit. A
// missing or stale fact never blocks dispatch; it freezes an unavailable reason
// so finalization cannot silently fall back to group-ratio pricing.
func freezeGatewayRequestPricingTx(tx *gorm.DB, audit *GatewayRequestDispatchAudit, at time.Time) error {
	if tx == nil || audit == nil {
		return errors.New("request pricing snapshot is unavailable")
	}
	// These are selection-time facts, not lookup hints. A same-name group or
	// key can be deleted and recreated while a request is waiting to persist;
	// resolving the current row by name would then silently attribute this
	// dispatch to the replacement generation. Preserve the selected tuple
	// across the derived-price reset and accept it only after the locked rows
	// below match exactly.
	expectedChannelIdentityRevision := audit.ChannelIdentityRevision
	expectedUpstreamGroupID := audit.UpstreamGroupID
	expectedUpstreamGroupGeneration := audit.UpstreamGroupGeneration
	expectedUpstreamKeyLocalID := audit.UpstreamKeyLocalID
	expectedUpstreamKeyGeneration := audit.UpstreamKeyGeneration
	requestedOperation := strings.ToLower(strings.TrimSpace(audit.PriceOperation))
	requestedProtocol := strings.ToLower(strings.TrimSpace(audit.PriceProtocol))
	requestedUsageSchema := strings.ToLower(strings.TrimSpace(audit.UsageSchema))
	requestedUsageSchemaVersion := audit.UsageSchemaVersion
	*audit = clearGatewayRequestPriceFactSnapshot(*audit)

	operation, protocol, usageSchema, unavailableReason := gatewayPricingIdentityForRequest(audit.Method, audit.Path)
	// A caller-supplied contract may resolve only the operation ambiguity of a
	// POST /v1/responses request; its parser-derived protocol remains fixed. It
	// also must not retarget an otherwise exact endpoint contract. Unsupported
	// methods and paths retain their unavailable reason even when no parser-derived
	// contract is present.
	requestedContractPresent := requestedOperation != "" || requestedProtocol != "" || requestedUsageSchema != "" ||
		requestedUsageSchemaVersion != 0 || audit.UsageOperationPresent || audit.OperationUnits != nil
	if (unavailableReason == GatewayPricingUnavailableOperationUnsupported && requestedContractPresent) ||
		(unavailableReason == GatewayPricingUnavailableOperationAmbiguous &&
			requestedProtocol != "" && requestedProtocol != protocol) ||
		(unavailableReason == "" &&
			(requestedOperation != "" && requestedOperation != operation ||
				requestedProtocol != "" && requestedProtocol != protocol ||
				requestedUsageSchema != "" && requestedUsageSchema != usageSchema)) {
		return errors.New("request dispatch pricing contract does not match method and path")
	}
	if requestedOperation != "" {
		operation = requestedOperation
		if unavailableReason == GatewayPricingUnavailableOperationAmbiguous {
			unavailableReason = ""
		}
	}
	if requestedProtocol != "" {
		protocol = requestedProtocol
	}
	if requestedUsageSchema != "" {
		if requestedUsageSchemaVersion != 1 {
			return errors.New("request dispatch pricing usage schema version is unsupported")
		}
		usageSchema = requestedUsageSchema
	} else if requestedUsageSchemaVersion != 0 {
		return errors.New("empty request dispatch pricing usage schema must use version zero")
	} else if requestedOperation != "" {
		usageSchema = gatewayRequestPricingUsageSchemaForOperation(requestedOperation)
	}
	audit.PriceOperation = operation
	audit.PriceProtocol = protocol
	audit.UsageSchema = usageSchema
	if usageSchema != "" {
		audit.UsageSchemaVersion = 1
	}
	if err := normalizeGatewayRequestDispatchPricingUsage(audit); err != nil {
		return err
	}
	if expectedChannelIdentityRevision == 0 || expectedUpstreamGroupID == 0 || expectedUpstreamGroupGeneration == 0 ||
		expectedUpstreamKeyLocalID == 0 || expectedUpstreamKeyGeneration == 0 {
		clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableScopeIncomplete)
		return nil
	}
	var channel Channel
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Select("id", "upstream_account_identity_revision", "cost_unit_epoch").First(&channel, audit.ChannelID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableIdentityChanged)
			return nil
		}
		return err
	}
	if channel.UpstreamAccountIdentityRevision == 0 || channel.UpstreamAccountIdentityRevision != expectedChannelIdentityRevision {
		clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableIdentityChanged)
		return nil
	}
	if channel.CostUnitEpoch == 0 {
		clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableCostBasisChanged)
		return nil
	}

	canonicalGroup, err := canonicalUpstreamGroupName(tx, audit.GroupName)
	if err != nil {
		clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableScopeIncomplete)
		return nil
	}
	var group GatewayUpstreamGroup
	err = tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Select("id", "channel_id", "canonical_group_name", "presence_generation", "present").
		First(&group, expectedUpstreamGroupID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableIdentityChanged)
			return nil
		}
		return err
	}
	if group.ChannelID != audit.ChannelID || !group.Present || group.PresenceGeneration != expectedUpstreamGroupGeneration ||
		strings.TrimSpace(group.CanonicalGroupName) != canonicalGroup {
		clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableIdentityChanged)
		return nil
	}

	var key GatewayUpstreamKey
	err = tx.Clauses(clause.Locking{Strength: "SHARE"}).
		Select("id", "channel_id", "group_name", "upstream_key_id", "upstream_key_name", "key_generation").
		First(&key, expectedUpstreamKeyLocalID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableIdentityChanged)
			return nil
		}
		return err
	}
	keyCanonicalGroup, err := canonicalUpstreamGroupName(tx, key.GroupName)
	if err != nil {
		clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableScopeIncomplete)
		return nil
	}
	if key.ChannelID != audit.ChannelID || keyCanonicalGroup != canonicalGroup || key.KeyGeneration != expectedUpstreamKeyGeneration ||
		(audit.UpstreamKeyID != 0 && key.UpstreamKeyID != audit.UpstreamKeyID) ||
		(strings.TrimSpace(audit.UpstreamKeyName) != "" && strings.TrimSpace(key.UpstreamKeyName) != strings.TrimSpace(audit.UpstreamKeyName)) {
		clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableIdentityChanged)
		return nil
	}
	audit.ChannelIdentityRevision = expectedChannelIdentityRevision
	audit.UpstreamGroupID = expectedUpstreamGroupID
	audit.UpstreamGroupGeneration = expectedUpstreamGroupGeneration
	audit.UpstreamKeyLocalID = expectedUpstreamKeyLocalID
	audit.UpstreamKeyGeneration = expectedUpstreamKeyGeneration
	if unavailableReason != "" {
		clearGatewayRequestPricingIdentity(audit, unavailableReason)
		return nil
	}
	modelName := strings.TrimSpace(audit.UpstreamModelName)
	if modelName == "" {
		modelName = strings.TrimSpace(audit.ModelName)
	}
	scope := GatewayRequestPriceScope{
		ChannelID: audit.ChannelID, ChannelAccountIdentityRevision: audit.ChannelIdentityRevision,
		UpstreamGroupID: audit.UpstreamGroupID, CanonicalGroupName: canonicalGroup,
		UpstreamGroupPresenceGeneration: audit.UpstreamGroupGeneration,
		GatewayUpstreamKeyID:            audit.UpstreamKeyLocalID, UpstreamKeyGeneration: audit.UpstreamKeyGeneration,
		ModelName: modelName, Operation: operation, Protocol: protocol,
	}
	if err := normalizeGatewayPriceScope(&scope); err != nil {
		clearGatewayRequestPricingHardFence(audit, GatewayPricingUnavailableScopeIncomplete)
		return nil
	}
	// The resolved scope is immutable request provenance even when no matching
	// price fact exists. Persist it before lookup so fact-missing and expired
	// states remain distinguishable from an incomplete identity fence.
	audit.PriceCanonicalGroupName = scope.CanonicalGroupName
	audit.PriceModelName = scope.ModelName
	lookup, err := resolveGatewayRequestPriceFactWithModelConfigTx(
		tx,
		scope,
		channel.CostUnitEpoch,
		audit.GroupRatio,
		audit.CompletionRatio,
		audit.RawGroupRatio,
		at,
	)
	if err != nil {
		return err
	}
	if lookup.Fact == nil && lookup.UnavailableReason == GatewayPricingUnavailableFactMissing {
		var priorEpochTotal int64
		if err := gatewayRequestPriceScopeQuery(tx.Model(&GatewayRequestPriceFact{}), scope).
			Where("cost_unit_epoch <> ?", channel.CostUnitEpoch).
			Count(&priorEpochTotal).Error; err != nil {
			return err
		}
		if priorEpochTotal > 0 {
			lookup.UnavailableReason = GatewayPricingUnavailableCostBasisChanged
		}
	}
	if lookup.Fact == nil {
		audit.PricingUnavailableReason = lookup.UnavailableReason
		return nil
	}
	copyGatewayRequestPriceFactToAudit(lookup.Fact, audit)
	return nil
}

func clearGatewayRequestPriceFactSnapshot(audit GatewayRequestDispatchAudit) GatewayRequestDispatchAudit {
	audit.ChannelIdentityRevision = 0
	audit.UpstreamGroupID = 0
	audit.UpstreamGroupGeneration = 0
	audit.UpstreamKeyLocalID = 0
	audit.UpstreamKeyGeneration = 0
	audit.PriceFactID = nil
	audit.PriceCostUnitEpoch = 0
	audit.PriceFactVersion = 0
	audit.PriceFactSchemaVersion = 0
	audit.PriceFactChecksum = ""
	audit.PriceCanonicalGroupName = ""
	audit.PriceModelName = ""
	audit.PriceInputUnit = nil
	audit.PriceOutputUnit = nil
	audit.PriceCacheReadUnit = nil
	audit.PriceCacheWriteUnit = nil
	audit.PriceLongContextThresholdTokens = nil
	audit.PriceLongContextInputUnit = nil
	audit.PriceLongContextOutputUnit = nil
	audit.PriceLongContextCacheReadUnit = nil
	audit.PriceLongContextCacheWriteUnit = nil
	audit.PriceOperationUnit = nil
	audit.PriceCurrency = ""
	audit.PriceUnit = ""
	audit.PriceSource = ""
	audit.PriceObservedAt = nil
	audit.PriceValidFrom = nil
	audit.PriceValidUntil = nil
	audit.UsageSchemaVersion = 0
	audit.PricingUnavailableReason = ""
	return audit
}

// clearGatewayRequestPricingIdentity removes attribution to a selected pricing
// scope while retaining the parser-derived request contract and ordinary route
// telemetry. It is used when the request operation cannot select a price scope.
// Unlike a hard fence, that condition retains captured multiplier and cost-basis
// scalar values; clearing the selection tuple still prevents provenance-qualified
// aggregates from treating those values as attributable multiplier evidence.
func clearGatewayRequestPricingIdentity(audit *GatewayRequestDispatchAudit, reason string) {
	if audit == nil {
		return
	}
	operation := audit.PriceOperation
	protocol := audit.PriceProtocol
	usageSchema := audit.UsageSchema
	usageSchemaVersion := audit.UsageSchemaVersion
	usageOperationPresent := audit.UsageOperationPresent
	operationUnits := cloneGatewayRequestPricingInt64(audit.OperationUnits)
	*audit = clearGatewayRequestPriceFactSnapshot(*audit)
	audit.PriceOperation = operation
	audit.PriceProtocol = protocol
	audit.UsageSchema = usageSchema
	audit.UsageSchemaVersion = usageSchemaVersion
	audit.UsageOperationPresent = usageOperationPresent
	audit.OperationUnits = operationUnits
	audit.PricingUnavailableReason = reason
}

// clearGatewayRequestPricingHardFence removes mutable accounting projections
// after the selected channel/group/key generation can no longer be proven. The
// parser-derived operation, protocol, and usage contract remain immutable
// request facts: image dispatch uses them to atomically cross the physical and
// billing boundary even when monetary pricing is unavailable.
func clearGatewayRequestPricingHardFence(audit *GatewayRequestDispatchAudit, reason string) {
	if audit == nil {
		return
	}
	// Everything that claims the selected pricing identity or its derived
	// monetary basis must be removed, even when this helper is reached after
	// freezeGatewayRequestPricingTx restored the selection IDs.
	clearGatewayRequestPricingIdentity(audit, reason)
	audit.GroupRatio = 0
	audit.CompletionRatio = 0
	audit.RawGroupRatio = nil
	audit.RawCompletionRatio = nil
	audit.FundingFactor = nil
	audit.CostBasisVersion = 0
	audit.CostBasisSource = ""
}

func copyGatewayRequestPriceFactToAudit(fact *GatewayRequestPriceFact, audit *GatewayRequestDispatchAudit) {
	if fact == nil || audit == nil {
		return
	}
	id := fact.ID
	audit.PriceFactID = &id
	audit.PriceCostUnitEpoch = fact.CostUnitEpoch
	audit.PriceFactVersion = fact.FactVersion
	audit.PriceFactSchemaVersion = fact.SchemaVersion
	audit.PriceFactChecksum = fact.Checksum
	audit.PriceCanonicalGroupName = fact.CanonicalGroupName
	audit.PriceModelName = fact.ModelName
	audit.PriceOperation = fact.Operation
	audit.PriceProtocol = fact.Protocol
	audit.PriceInputUnit = cloneGatewayRequestDispatchNumber(fact.InputUnitPrice)
	audit.PriceOutputUnit = cloneGatewayRequestDispatchNumber(fact.OutputUnitPrice)
	audit.PriceCacheReadUnit = cloneGatewayRequestDispatchNumber(fact.CacheReadUnitPrice)
	audit.PriceCacheWriteUnit = cloneGatewayRequestDispatchNumber(fact.CacheWriteUnitPrice)
	audit.PriceLongContextThresholdTokens = cloneGatewayRequestPricingInt64(fact.LongContextThresholdTokens)
	audit.PriceLongContextInputUnit = cloneGatewayRequestDispatchNumber(fact.LongContextInputUnitPrice)
	audit.PriceLongContextOutputUnit = cloneGatewayRequestDispatchNumber(fact.LongContextOutputUnitPrice)
	audit.PriceLongContextCacheReadUnit = cloneGatewayRequestDispatchNumber(fact.LongContextCacheReadUnitPrice)
	audit.PriceLongContextCacheWriteUnit = cloneGatewayRequestDispatchNumber(fact.LongContextCacheWriteUnitPrice)
	if fact.SchemaVersion == GatewayRequestPriceFactTieredSchemaVersion && fact.GroupRatio != nil && fact.CompletionRatio != nil {
		audit.GroupRatio = *fact.GroupRatio
		audit.CompletionRatio = *fact.CompletionRatio
	}
	audit.PriceOperationUnit = cloneGatewayRequestDispatchNumber(fact.OperationUnitPrice)
	audit.PriceCurrency = fact.Currency
	audit.PriceUnit = fact.Unit
	audit.PriceSource = fact.Source
	audit.PriceObservedAt = gatewayRequestPricingTimePointer(fact.ObservedAt)
	audit.PriceValidFrom = gatewayRequestPricingTimePointer(fact.ValidFrom)
	audit.PriceValidUntil = cloneGatewayRequestPricingTime(fact.ValidUntil)
	audit.PricingUnavailableReason = ""
}

func gatewayRequestPriceFactFromAudit(audit *GatewayRequestDispatchAudit) *GatewayRequestPriceFact {
	if audit == nil || audit.PriceFactID == nil || *audit.PriceFactID == 0 || audit.PriceObservedAt == nil || audit.PriceValidFrom == nil {
		return nil
	}
	fact := &GatewayRequestPriceFact{
		ID:            *audit.PriceFactID,
		CostUnitEpoch: audit.PriceCostUnitEpoch,
		ChannelID:     audit.ChannelID, ChannelAccountIdentityRevision: audit.ChannelIdentityRevision,
		UpstreamGroupID: audit.UpstreamGroupID, CanonicalGroupName: audit.PriceCanonicalGroupName,
		UpstreamGroupPresenceGeneration: audit.UpstreamGroupGeneration,
		GatewayUpstreamKeyID:            audit.UpstreamKeyLocalID, UpstreamKeyGeneration: audit.UpstreamKeyGeneration,
		ModelName: audit.PriceModelName, Operation: audit.PriceOperation, Protocol: audit.PriceProtocol,
		InputUnitPrice:                 cloneGatewayRequestDispatchNumber(audit.PriceInputUnit),
		OutputUnitPrice:                cloneGatewayRequestDispatchNumber(audit.PriceOutputUnit),
		CacheReadUnitPrice:             cloneGatewayRequestDispatchNumber(audit.PriceCacheReadUnit),
		CacheWriteUnitPrice:            cloneGatewayRequestDispatchNumber(audit.PriceCacheWriteUnit),
		LongContextThresholdTokens:     cloneGatewayRequestPricingInt64(audit.PriceLongContextThresholdTokens),
		LongContextInputUnitPrice:      cloneGatewayRequestDispatchNumber(audit.PriceLongContextInputUnit),
		LongContextOutputUnitPrice:     cloneGatewayRequestDispatchNumber(audit.PriceLongContextOutputUnit),
		LongContextCacheReadUnitPrice:  cloneGatewayRequestDispatchNumber(audit.PriceLongContextCacheReadUnit),
		LongContextCacheWriteUnitPrice: cloneGatewayRequestDispatchNumber(audit.PriceLongContextCacheWriteUnit),
		OperationUnitPrice:             cloneGatewayRequestDispatchNumber(audit.PriceOperationUnit),
		Unit:                           audit.PriceUnit, Currency: audit.PriceCurrency, Source: audit.PriceSource,
		ObservedAt: normalizeGatewayRequestPricingTime(*audit.PriceObservedAt),
		ValidFrom:  normalizeGatewayRequestPricingTime(*audit.PriceValidFrom), ValidUntil: cloneGatewayRequestPricingTime(audit.PriceValidUntil),
		FactVersion: audit.PriceFactVersion, SchemaVersion: audit.PriceFactSchemaVersion, Checksum: audit.PriceFactChecksum,
	}
	if fact.SchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
		groupRatio := audit.GroupRatio
		completionRatio := audit.CompletionRatio
		fact.GroupRatio = &groupRatio
		fact.CompletionRatio = &completionRatio
	}
	return fact
}

func gatewayRequestNeedsPriceEstimate(log *GatewayRequestLog) bool {
	if log == nil || !isGatewayRequestRecord(log) || isGatewayRequestCountTokens(log) {
		return false
	}
	status := gatewayRequestCostStatus(log)
	if status == GatewayCostStatusReported || status == GatewayCostStatusReconciled {
		return false
	}
	return IsSuccessfulGatewayRequest(log) || log.UpstreamAccepted || log.BillingCommitted ||
		log.CostEstimated || status == GatewayCostStatusEstimated || log.TotalTokens > 0 ||
		log.PromptTokens > 0 || log.CompletionTokens > 0 || log.CacheCreationTokens > 0 ||
		log.CacheReadTokens > 0 || log.OperationUnits != nil
}

func gatewayRequestPricingUsageFromLog(log *GatewayRequestLog) (GatewayRequestPricingUsage, string) {
	if log == nil {
		return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageIncomplete
	}
	usage := GatewayRequestPricingUsage{}
	if log.UsageInputPresent {
		if log.PromptTokens < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		input := log.PromptTokens
		if log.UsageSchema != GatewayPricingUsageSchemaInputTokensV1 {
			if log.CacheCreationTokens < 0 || log.CacheReadTokens < 0 {
				return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
			}
			var ok bool
			input, ok = checkedGatewayRequestPricingSub(input, log.CacheCreationTokens)
			if !ok {
				return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
			}
			input, ok = checkedGatewayRequestPricingSub(input, log.CacheReadTokens)
			if !ok || input < 0 {
				return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
			}
		}
		usage.InputTokens = &input
	}
	if log.UsageOutputPresent {
		value := log.CompletionTokens
		if value < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		usage.OutputTokens = &value
	}
	if log.UsageCacheReadPresent {
		value := log.CacheReadTokens
		if value < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		usage.CacheReadTokens = &value
	}
	if log.UsageCacheWritePresent {
		value := log.CacheCreationTokens
		if value < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		usage.CacheWriteTokens = &value
	}
	if log.UsageOperationPresent {
		if log.OperationUnits == nil || *log.OperationUnits < 0 {
			return GatewayRequestPricingUsage{}, GatewayPricingUnavailableUsageInvalid
		}
		usage.OperationUnits = cloneGatewayRequestPricingInt64(log.OperationUnits)
	}
	return usage, ""
}

func cloneGatewayRequestPricingInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func checkedGatewayRequestPricingAdd(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right || right < 0 && left < math.MinInt64-right {
		return 0, false
	}
	return left + right, true
}

func checkedGatewayRequestPricingSub(left, right int64) (int64, bool) {
	if right > 0 && left < math.MinInt64+right || right < 0 && left > math.MaxInt64+right {
		return 0, false
	}
	return left - right, true
}

func markGatewayRequestPricingUnavailable(log *GatewayRequestLog, reason string) {
	if log == nil {
		return
	}
	reason = strings.ToLower(strings.TrimSpace(reason))
	if reason == "" {
		reason = GatewayPricingUnavailableFactMissing
	}
	log.Cost = 0
	log.RawCost = nil
	log.CostEstimated = false
	log.CostSource = ""
	log.CostStatus = GatewayCostStatusUnavailable
	log.CostUnavailableReason = reason
}

// finalizeGatewayRequestPricing applies only the fact frozen before the HTTP
// send. It never performs a current-price lookup and never falls back to group
// ratios. A frozen pricing-provenance fence wins even over an incoming provider
// amount: that amount may have been converted with an old account's funding
// basis, so persisting it as reported/reconciled would be a false local cost.
func finalizeGatewayRequestPricing(audit *GatewayRequestDispatchAudit, log *GatewayRequestLog) {
	if audit == nil || log == nil {
		return
	}
	if gatewayRequestPricingFenceUnavailableReason(audit.PricingUnavailableReason) {
		markGatewayRequestPricingUnavailable(log, audit.PricingUnavailableReason)
		return
	}
	status := gatewayRequestCostStatus(log)
	if status == GatewayCostStatusReported || status == GatewayCostStatusReconciled {
		log.CostUnavailableReason = ""
		return
	}
	if status == GatewayCostStatusUnavailable &&
		strings.EqualFold(strings.TrimSpace(log.CostUnavailableReason), GatewayPricingUnavailableUsageInvalid) {
		// Invalid usage is a terminal observation, not a temporarily incomplete
		// snapshot. A later finalization pass must not reinterpret the missing
		// components as a complete zero-valued usage tuple.
		return
	}
	if !gatewayRequestNeedsPriceEstimate(log) {
		return
	}
	if audit.PricingUnavailableReason != "" {
		markGatewayRequestPricingUnavailable(log, audit.PricingUnavailableReason)
		return
	}
	fact := gatewayRequestPriceFactFromAudit(audit)
	if fact == nil {
		markGatewayRequestPricingUnavailable(log, GatewayPricingUnavailableFactMissing)
		return
	}
	checksum, err := gatewayRequestPriceFactChecksum(fact)
	if err != nil || !strings.EqualFold(checksum, fact.Checksum) {
		markGatewayRequestPricingUnavailable(log, GatewayPricingUnavailableChecksumMismatch)
		return
	}
	if !strings.EqualFold(fact.Currency, "USD") {
		markGatewayRequestPricingUnavailable(log, GatewayPricingUnavailableCurrencyUnsupported)
		return
	}
	usage, usageReason := gatewayRequestPricingUsageFromLog(log)
	if usageReason != "" {
		markGatewayRequestPricingUnavailable(log, usageReason)
		return
	}
	estimate := EvaluateGatewayRequestPriceFact(fact, usage)
	if estimate.Cost == nil || estimate.Status != GatewayCostStatusEstimated {
		markGatewayRequestPricingUnavailable(log, estimate.UnavailableReason)
		return
	}
	log.Cost = *estimate.Cost
	log.RawCost = nil
	log.CostEstimated = true
	log.CostSource = "price_fact"
	log.CostStatus = GatewayCostStatusEstimated
	log.CostUnavailableReason = ""
}

func gatewayRequestPricingFenceUnavailableReason(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case GatewayPricingUnavailableScopeIncomplete, GatewayPricingUnavailableIdentityChanged:
		return true
	default:
		return false
	}
}

// SetGatewayRequestPricingUsage copies explicit component presence into a
// final request log. InputTokens is the uncached input component; PromptTokens
// remains the compatibility total including cache read/write components.
func SetGatewayRequestPricingUsage(log *GatewayRequestLog, usage GatewayRequestPricingUsage) error {
	if log == nil {
		return errors.New("request log is required")
	}
	for _, value := range []*int64{usage.InputTokens, usage.OutputTokens, usage.CacheReadTokens, usage.CacheWriteTokens, usage.OperationUnits} {
		if value != nil && *value < 0 {
			return fmt.Errorf("%s: request pricing usage must not be negative", GatewayPricingUnavailableUsageInvalid)
		}
	}

	next := *log
	next.UsageInputPresent = usage.InputTokens != nil
	next.UsageOutputPresent = usage.OutputTokens != nil
	next.UsageCacheReadPresent = usage.CacheReadTokens != nil
	next.UsageCacheWritePresent = usage.CacheWriteTokens != nil
	next.UsageOperationPresent = usage.OperationUnits != nil
	if usage.CacheReadTokens != nil {
		next.CacheReadTokens = *usage.CacheReadTokens
	}
	if usage.CacheWriteTokens != nil {
		next.CacheCreationTokens = *usage.CacheWriteTokens
	}
	if usage.OutputTokens != nil {
		next.CompletionTokens = *usage.OutputTokens
	}
	if usage.InputTokens != nil {
		promptTokens := *usage.InputTokens
		for _, cacheTokens := range []*int64{usage.CacheReadTokens, usage.CacheWriteTokens} {
			if cacheTokens == nil {
				continue
			}
			var ok bool
			promptTokens, ok = checkedGatewayRequestPricingAdd(promptTokens, *cacheTokens)
			if !ok {
				return fmt.Errorf("%s: request pricing input token total overflows int64", GatewayPricingUnavailableUsageInvalid)
			}
		}
		next.PromptTokens = promptTokens
	}
	next.OperationUnits = cloneGatewayRequestPricingInt64(usage.OperationUnits)
	*log = next
	return nil
}

func normalizeGatewayRequestPricingSnapshot(log *GatewayRequestLog) error {
	if log == nil {
		return errors.New("request pricing log is required")
	}
	log.PriceCanonicalGroupName = strings.TrimSpace(log.PriceCanonicalGroupName)
	log.PriceModelName = strings.TrimSpace(log.PriceModelName)
	log.PriceOperation = strings.ToLower(strings.TrimSpace(log.PriceOperation))
	log.PriceProtocol = strings.ToLower(strings.TrimSpace(log.PriceProtocol))
	log.PriceCurrency = strings.ToUpper(strings.TrimSpace(log.PriceCurrency))
	log.PriceUnit = strings.ToLower(strings.TrimSpace(log.PriceUnit))
	log.PriceSource = strings.ToLower(strings.TrimSpace(log.PriceSource))
	log.UsageSchema = strings.ToLower(strings.TrimSpace(log.UsageSchema))
	log.CostUnavailableReason = strings.ToLower(strings.TrimSpace(log.CostUnavailableReason))
	usageSchema, err := normalizeGatewayRequestPricingUsageSchema(log.UsageSchema, log.UsageSchemaVersion)
	if err != nil {
		return err
	}
	log.UsageSchema = usageSchema
	if err := validateGatewayRequestPricingContract(log.PriceOperation, log.PriceProtocol, log.UsageSchema); err != nil {
		return err
	}
	if log.PriceValidFrom != nil {
		value := normalizeGatewayRequestPricingTime(*log.PriceValidFrom)
		log.PriceValidFrom = &value
	}
	if log.PriceObservedAt != nil {
		value := normalizeGatewayRequestPricingTime(*log.PriceObservedAt)
		log.PriceObservedAt = &value
	}
	if log.PriceValidUntil != nil {
		value := normalizeGatewayRequestPricingTime(*log.PriceValidUntil)
		log.PriceValidUntil = &value
	}
	if log.PriceFactID == nil {
		if log.PriceCostUnitEpoch != 0 || log.PriceFactVersion != 0 || log.PriceFactSchemaVersion != 0 || log.PriceFactChecksum != "" ||
			log.PriceInputUnit != nil || log.PriceOutputUnit != nil || log.PriceCacheReadUnit != nil ||
			log.PriceCacheWriteUnit != nil || log.PriceLongContextThresholdTokens != nil ||
			log.PriceLongContextInputUnit != nil || log.PriceLongContextOutputUnit != nil ||
			log.PriceLongContextCacheReadUnit != nil || log.PriceLongContextCacheWriteUnit != nil ||
			log.PriceOperationUnit != nil ||
			log.PriceCurrency != "" || log.PriceUnit != "" || log.PriceSource != "" || log.PriceObservedAt != nil ||
			log.PriceValidFrom != nil || log.PriceValidUntil != nil {
			return errors.New("request pricing fact snapshot is incomplete")
		}
		namedScopePresent := log.PriceCanonicalGroupName != "" || log.PriceModelName != ""
		if !namedScopePresent && (log.ChannelIdentityRevision != 0 || log.UpstreamGroupID != 0 || log.UpstreamGroupGeneration != 0 ||
			log.UpstreamKeyLocalID != 0 || log.UpstreamKeyGeneration != 0) {
			return errors.New("request pricing identity snapshot is partial")
		}
		if namedScopePresent && (log.ChannelID == 0 || log.ChannelIdentityRevision == 0 || log.UpstreamGroupID == 0 ||
			log.UpstreamGroupGeneration == 0 || log.UpstreamKeyLocalID == 0 || log.UpstreamKeyGeneration == 0 ||
			log.PriceCanonicalGroupName == "" ||
			log.PriceModelName == "" || log.PriceOperation == "" || log.PriceProtocol == "") {
			return errors.New("request pricing scope snapshot is incomplete")
		}
	} else {
		if *log.PriceFactID == 0 || log.ChannelID == 0 || log.ChannelIdentityRevision == 0 || log.UpstreamGroupID == 0 ||
			log.UpstreamGroupGeneration == 0 || log.UpstreamKeyLocalID == 0 || log.UpstreamKeyGeneration == 0 ||
			log.PriceCostUnitEpoch == 0 || log.PriceFactVersion == 0 || !gatewayRequestPriceFactSchemaSupported(log.PriceFactSchemaVersion) ||
			!gatewayPricingChecksumPattern.MatchString(log.PriceFactChecksum) || log.PriceCanonicalGroupName == "" || log.PriceModelName == "" ||
			log.PriceOperation == "" || log.PriceProtocol == "" || log.PriceCurrency == "" || log.PriceUnit == "" ||
			log.PriceSource == "" || log.PriceObservedAt == nil || log.PriceValidFrom == nil {
			return errors.New("request pricing fact snapshot is incomplete")
		}
		if log.PriceObservedAt.After(*log.PriceValidFrom) {
			return errors.New("request pricing fact observation is after valid_from")
		}
		if log.PriceValidUntil != nil && !log.PriceValidUntil.After(*log.PriceValidFrom) {
			return errors.New("request pricing fact validity is invalid")
		}
		fact := GatewayRequestPriceFact{
			SchemaVersion: log.PriceFactSchemaVersion,
			Operation:     log.PriceOperation, Protocol: log.PriceProtocol,
			InputUnitPrice: log.PriceInputUnit, OutputUnitPrice: log.PriceOutputUnit,
			CacheReadUnitPrice: log.PriceCacheReadUnit, CacheWriteUnitPrice: log.PriceCacheWriteUnit,
			LongContextThresholdTokens:     log.PriceLongContextThresholdTokens,
			LongContextInputUnitPrice:      log.PriceLongContextInputUnit,
			LongContextOutputUnitPrice:     log.PriceLongContextOutputUnit,
			LongContextCacheReadUnitPrice:  log.PriceLongContextCacheReadUnit,
			LongContextCacheWriteUnitPrice: log.PriceLongContextCacheWriteUnit,
			OperationUnitPrice:             log.PriceOperationUnit, Unit: log.PriceUnit,
		}
		if fact.SchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
			groupRatio := log.GroupRatio
			completionRatio := log.CompletionRatio
			fact.GroupRatio = &groupRatio
			fact.CompletionRatio = &completionRatio
		}
		if err := validateGatewayRequestPriceFactShape(&fact); err != nil {
			return err
		}
	}
	if log.UsageSchema == "" {
		if log.UsageSchemaVersion != 0 || log.UsageInputPresent || log.UsageOutputPresent ||
			log.UsageCacheReadPresent || log.UsageCacheWritePresent || log.UsageOperationPresent || log.OperationUnits != nil {
			return errors.New("request pricing usage schema is incomplete")
		}
	} else {
		switch log.UsageSchema {
		case GatewayPricingUsageSchemaTokensV1:
			if log.UsageOperationPresent || log.OperationUnits != nil {
				return errors.New("token component usage cannot carry operation units")
			}
		case GatewayPricingUsageSchemaInputTokensV1:
			if log.UsageOutputPresent || log.UsageCacheReadPresent || log.UsageCacheWritePresent ||
				log.UsageOperationPresent || log.OperationUnits != nil {
				return errors.New("input token usage contains unsupported components")
			}
		case GatewayPricingUsageSchemaOperationV1:
			if log.UsageInputPresent || log.UsageOutputPresent || log.UsageCacheReadPresent || log.UsageCacheWritePresent {
				return errors.New("operation usage cannot carry token components")
			}
		}
	}
	if log.UsageOperationPresent != (log.OperationUnits != nil) {
		return errors.New("request pricing operation usage presence is inconsistent")
	}
	if log.OperationUnits != nil && *log.OperationUnits < 0 {
		return errors.New("request pricing operation units must not be negative")
	}
	if log.PriceOperation == GatewayImageOperationGeneration && log.UsageSchema == GatewayPricingUsageSchemaOperationV1 &&
		(!log.UsageOperationPresent || log.OperationUnits == nil || *log.OperationUnits != 1) {
		return errors.New("image request pricing must carry one operation unit")
	}
	if len(log.CostUnavailableReason) > 64 ||
		log.CostStatus == GatewayCostStatusUnavailable && log.CostUnavailableReason == "" {
		return errors.New("request pricing unavailable reason is invalid")
	}
	if log.CostStatus == GatewayCostStatusUnavailable && !isLegacyUnverifiedGatewayCostEstimate(log) &&
		(log.Cost != 0 || log.CostEstimated || strings.TrimSpace(log.CostSource) != "") {
		return errors.New("unavailable request pricing cannot carry a cost")
	}
	return nil
}
