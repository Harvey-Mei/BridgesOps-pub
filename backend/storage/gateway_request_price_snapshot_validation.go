package storage

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrGatewayRequestPriceSnapshotIntegrity = errors.New("gateway request price snapshot integrity violation")

// gatewayRequestPriceFactSnapshot is the common immutable subset copied from a
// price fact into request logs and dispatch audits.
type gatewayRequestPriceFactSnapshot struct {
	HasFact                        bool
	FactID                         uint64
	ChannelID                      uint
	ChannelIdentityRevision        uint64
	CostUnitEpoch                  uint64
	UpstreamGroupID                uint
	CanonicalGroupName             string
	UpstreamGroupGeneration        uint64
	UpstreamKeyLocalID             uint
	UpstreamKeyGeneration          uint64
	ModelName                      string
	Operation                      string
	Protocol                       string
	InputUnitPrice                 *float64
	OutputUnitPrice                *float64
	CacheReadUnitPrice             *float64
	CacheWriteUnitPrice            *float64
	LongContextThresholdTokens     *int64
	LongContextInputUnitPrice      *float64
	LongContextOutputUnitPrice     *float64
	LongContextCacheReadUnitPrice  *float64
	LongContextCacheWriteUnitPrice *float64
	GroupRatio                     *float64
	CompletionRatio                *float64
	OperationUnitPrice             *float64
	Unit                           string
	Currency                       string
	Source                         string
	ObservedAt                     *time.Time
	ValidFrom                      *time.Time
	ValidUntil                     *time.Time
	FactVersion                    uint64
	SchemaVersion                  int
	Checksum                       string
}

func validateGatewayRequestLogPriceFactSnapshotTx(tx *gorm.DB, log *GatewayRequestLog) error {
	if log == nil {
		return fmt.Errorf("%w: request log is required", ErrGatewayRequestPriceSnapshotIntegrity)
	}
	return validateGatewayRequestPriceFactSnapshotTx(tx, gatewayRequestPriceFactSnapshotFromLog(log))
}

func validateGatewayRequestDispatchPriceFactSnapshotTx(tx *gorm.DB, audit *GatewayRequestDispatchAudit) error {
	if audit == nil {
		return fmt.Errorf("%w: dispatch audit is required", ErrGatewayRequestPriceSnapshotIntegrity)
	}
	return validateGatewayRequestPriceFactSnapshotTx(tx, gatewayRequestPriceFactSnapshotFromDispatchAudit(audit))
}

func validateGatewayRequestPriceFactSnapshotTx(tx *gorm.DB, snapshot gatewayRequestPriceFactSnapshot) error {
	if tx == nil {
		return fmt.Errorf("%w: storage transaction is required", ErrGatewayRequestPriceSnapshotIntegrity)
	}
	if !snapshot.HasFact {
		return nil
	}
	var fact GatewayRequestPriceFact
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).First(&fact, snapshot.FactID).Error; err != nil {
		return fmt.Errorf("%w: load referenced price fact %d: %v", ErrGatewayRequestPriceSnapshotIntegrity, snapshot.FactID, err)
	}
	checksum, err := gatewayRequestPriceFactChecksum(&fact)
	if err != nil {
		return fmt.Errorf("%w: checksum referenced price fact %d: %v", ErrGatewayRequestPriceSnapshotIntegrity, snapshot.FactID, err)
	}
	if checksum != fact.Checksum {
		return fmt.Errorf("%w: referenced price fact %d checksum does not match its contents", ErrGatewayRequestPriceSnapshotIntegrity, snapshot.FactID)
	}
	if !gatewayRequestPriceFactSnapshotsEqual(snapshot, gatewayRequestPriceFactSnapshotFromFact(&fact)) {
		return fmt.Errorf("%w: copied fields do not match referenced price fact %d", ErrGatewayRequestPriceSnapshotIntegrity, snapshot.FactID)
	}
	return nil
}

func gatewayRequestPriceFactSnapshotFromFact(fact *GatewayRequestPriceFact) gatewayRequestPriceFactSnapshot {
	if fact == nil {
		return gatewayRequestPriceFactSnapshot{}
	}
	observedAt := fact.ObservedAt
	validFrom := fact.ValidFrom
	return gatewayRequestPriceFactSnapshot{
		HasFact: true, FactID: fact.ID,
		ChannelID: fact.ChannelID, ChannelIdentityRevision: fact.ChannelAccountIdentityRevision,
		CostUnitEpoch: fact.CostUnitEpoch, UpstreamGroupID: fact.UpstreamGroupID,
		CanonicalGroupName: fact.CanonicalGroupName, UpstreamGroupGeneration: fact.UpstreamGroupPresenceGeneration,
		UpstreamKeyLocalID: fact.GatewayUpstreamKeyID, UpstreamKeyGeneration: fact.UpstreamKeyGeneration,
		ModelName: fact.ModelName, Operation: fact.Operation, Protocol: fact.Protocol,
		InputUnitPrice: fact.InputUnitPrice, OutputUnitPrice: fact.OutputUnitPrice,
		CacheReadUnitPrice: fact.CacheReadUnitPrice, CacheWriteUnitPrice: fact.CacheWriteUnitPrice,
		LongContextThresholdTokens:     fact.LongContextThresholdTokens,
		LongContextInputUnitPrice:      fact.LongContextInputUnitPrice,
		LongContextOutputUnitPrice:     fact.LongContextOutputUnitPrice,
		LongContextCacheReadUnitPrice:  fact.LongContextCacheReadUnitPrice,
		LongContextCacheWriteUnitPrice: fact.LongContextCacheWriteUnitPrice,
		GroupRatio:                     fact.GroupRatio, CompletionRatio: fact.CompletionRatio,
		OperationUnitPrice: fact.OperationUnitPrice, Unit: fact.Unit, Currency: fact.Currency, Source: fact.Source,
		ObservedAt: &observedAt,
		ValidFrom:  &validFrom, ValidUntil: fact.ValidUntil,
		FactVersion: fact.FactVersion, SchemaVersion: fact.SchemaVersion, Checksum: fact.Checksum,
	}
}

func gatewayRequestPriceFactSnapshotFromLog(log *GatewayRequestLog) gatewayRequestPriceFactSnapshot {
	if log == nil {
		return gatewayRequestPriceFactSnapshot{}
	}
	snapshot := gatewayRequestPriceFactSnapshot{
		ChannelID: log.ChannelID, ChannelIdentityRevision: log.ChannelIdentityRevision,
		CostUnitEpoch: log.PriceCostUnitEpoch, UpstreamGroupID: log.UpstreamGroupID,
		CanonicalGroupName: log.PriceCanonicalGroupName, UpstreamGroupGeneration: log.UpstreamGroupGeneration,
		UpstreamKeyLocalID: log.UpstreamKeyLocalID, UpstreamKeyGeneration: log.UpstreamKeyGeneration,
		ModelName: log.PriceModelName, Operation: log.PriceOperation, Protocol: log.PriceProtocol,
		InputUnitPrice: log.PriceInputUnit, OutputUnitPrice: log.PriceOutputUnit,
		CacheReadUnitPrice: log.PriceCacheReadUnit, CacheWriteUnitPrice: log.PriceCacheWriteUnit,
		LongContextThresholdTokens:     log.PriceLongContextThresholdTokens,
		LongContextInputUnitPrice:      log.PriceLongContextInputUnit,
		LongContextOutputUnitPrice:     log.PriceLongContextOutputUnit,
		LongContextCacheReadUnitPrice:  log.PriceLongContextCacheReadUnit,
		LongContextCacheWriteUnitPrice: log.PriceLongContextCacheWriteUnit,
		OperationUnitPrice:             log.PriceOperationUnit, Unit: log.PriceUnit, Currency: log.PriceCurrency, Source: log.PriceSource,
		ObservedAt: log.PriceObservedAt,
		ValidFrom:  log.PriceValidFrom, ValidUntil: log.PriceValidUntil,
		FactVersion: log.PriceFactVersion, SchemaVersion: log.PriceFactSchemaVersion, Checksum: log.PriceFactChecksum,
	}
	if log.PriceFactID != nil {
		snapshot.HasFact = true
		snapshot.FactID = *log.PriceFactID
		if log.PriceFactSchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
			groupRatio := log.GroupRatio
			completionRatio := log.CompletionRatio
			snapshot.GroupRatio = &groupRatio
			snapshot.CompletionRatio = &completionRatio
		}
	}
	return snapshot
}

func gatewayRequestPriceFactSnapshotFromDispatchAudit(audit *GatewayRequestDispatchAudit) gatewayRequestPriceFactSnapshot {
	if audit == nil {
		return gatewayRequestPriceFactSnapshot{}
	}
	snapshot := gatewayRequestPriceFactSnapshot{
		ChannelID: audit.ChannelID, ChannelIdentityRevision: audit.ChannelIdentityRevision,
		CostUnitEpoch: audit.PriceCostUnitEpoch, UpstreamGroupID: audit.UpstreamGroupID,
		CanonicalGroupName: audit.PriceCanonicalGroupName, UpstreamGroupGeneration: audit.UpstreamGroupGeneration,
		UpstreamKeyLocalID: audit.UpstreamKeyLocalID, UpstreamKeyGeneration: audit.UpstreamKeyGeneration,
		ModelName: audit.PriceModelName, Operation: audit.PriceOperation, Protocol: audit.PriceProtocol,
		InputUnitPrice: audit.PriceInputUnit, OutputUnitPrice: audit.PriceOutputUnit,
		CacheReadUnitPrice: audit.PriceCacheReadUnit, CacheWriteUnitPrice: audit.PriceCacheWriteUnit,
		LongContextThresholdTokens:     audit.PriceLongContextThresholdTokens,
		LongContextInputUnitPrice:      audit.PriceLongContextInputUnit,
		LongContextOutputUnitPrice:     audit.PriceLongContextOutputUnit,
		LongContextCacheReadUnitPrice:  audit.PriceLongContextCacheReadUnit,
		LongContextCacheWriteUnitPrice: audit.PriceLongContextCacheWriteUnit,
		OperationUnitPrice:             audit.PriceOperationUnit, Unit: audit.PriceUnit, Currency: audit.PriceCurrency, Source: audit.PriceSource,
		ObservedAt: audit.PriceObservedAt,
		ValidFrom:  audit.PriceValidFrom, ValidUntil: audit.PriceValidUntil,
		FactVersion: audit.PriceFactVersion, SchemaVersion: audit.PriceFactSchemaVersion, Checksum: audit.PriceFactChecksum,
	}
	if audit.PriceFactID != nil {
		snapshot.HasFact = true
		snapshot.FactID = *audit.PriceFactID
		if audit.PriceFactSchemaVersion == GatewayRequestPriceFactTieredSchemaVersion {
			groupRatio := audit.GroupRatio
			completionRatio := audit.CompletionRatio
			snapshot.GroupRatio = &groupRatio
			snapshot.CompletionRatio = &completionRatio
		}
	}
	return snapshot
}

func gatewayRequestPriceFactSnapshotsEqual(left, right gatewayRequestPriceFactSnapshot) bool {
	return left.HasFact == right.HasFact && left.FactID == right.FactID &&
		left.ChannelID == right.ChannelID && left.ChannelIdentityRevision == right.ChannelIdentityRevision &&
		left.CostUnitEpoch == right.CostUnitEpoch && left.UpstreamGroupID == right.UpstreamGroupID &&
		left.CanonicalGroupName == right.CanonicalGroupName && left.UpstreamGroupGeneration == right.UpstreamGroupGeneration &&
		left.UpstreamKeyLocalID == right.UpstreamKeyLocalID && left.UpstreamKeyGeneration == right.UpstreamKeyGeneration &&
		left.ModelName == right.ModelName && left.Operation == right.Operation && left.Protocol == right.Protocol &&
		equalGatewayRequestPriceFloat(left.InputUnitPrice, right.InputUnitPrice) &&
		equalGatewayRequestPriceFloat(left.OutputUnitPrice, right.OutputUnitPrice) &&
		equalGatewayRequestPriceFloat(left.CacheReadUnitPrice, right.CacheReadUnitPrice) &&
		equalGatewayRequestPriceFloat(left.CacheWriteUnitPrice, right.CacheWriteUnitPrice) &&
		equalGatewayRequestPriceInt64(left.LongContextThresholdTokens, right.LongContextThresholdTokens) &&
		equalGatewayRequestPriceFloat(left.LongContextInputUnitPrice, right.LongContextInputUnitPrice) &&
		equalGatewayRequestPriceFloat(left.LongContextOutputUnitPrice, right.LongContextOutputUnitPrice) &&
		equalGatewayRequestPriceFloat(left.LongContextCacheReadUnitPrice, right.LongContextCacheReadUnitPrice) &&
		equalGatewayRequestPriceFloat(left.LongContextCacheWriteUnitPrice, right.LongContextCacheWriteUnitPrice) &&
		equalGatewayRequestPriceFloat(left.GroupRatio, right.GroupRatio) &&
		equalGatewayRequestPriceFloat(left.CompletionRatio, right.CompletionRatio) &&
		equalGatewayRequestPriceFloat(left.OperationUnitPrice, right.OperationUnitPrice) &&
		left.Unit == right.Unit && left.Currency == right.Currency && left.Source == right.Source &&
		equalGatewayRequestPriceTime(left.ObservedAt, right.ObservedAt) &&
		equalGatewayRequestPriceTime(left.ValidFrom, right.ValidFrom) &&
		equalGatewayRequestPriceTime(left.ValidUntil, right.ValidUntil) &&
		left.FactVersion == right.FactVersion && left.SchemaVersion == right.SchemaVersion && left.Checksum == right.Checksum
}

func equalGatewayRequestPriceFloat(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalGatewayRequestPriceTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func equalGatewayRequestPriceInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
