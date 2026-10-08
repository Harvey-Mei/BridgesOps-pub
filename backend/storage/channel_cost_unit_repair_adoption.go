package storage

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"gorm.io/gorm"
)

const costUnitRepairAdoptionBasisSource = "cost_unit_repair_adoption"

var (
	ErrCostUnitRepairAdoptionInvalid             = errors.New("cost unit repair adoption anchor is invalid")
	ErrCostUnitRepairAdoptionConflict            = errors.New("cost unit repair adoption evidence changed after preview")
	ErrCostUnitRepairAdoptionIdempotencyConflict = errors.New("cost unit repair adoption idempotency key conflicts with another request")
)

// CostUnitRepairAdoptionAnchor identifies immutable server-side evidence. A
// preview fills the expected revisions and high-water marks; apply accepts that
// returned anchor plus its digest and never accepts copied monetary evidence.
type CostUnitRepairAdoptionAnchor struct {
	ChannelID                uint      `json:"channel_id"`
	ExpectedIdentityRevision uint64    `json:"expected_identity_revision"`
	ExpectedAccountRecordID  uint64    `json:"expected_account_record_id"`
	ExpectedStateVersion     uint64    `json:"expected_state_version"`
	ExpectedCostUnitEpoch    uint64    `json:"expected_cost_unit_epoch"`
	SourceEventHighWater     uint64    `json:"source_event_high_water"`
	ResetEventID             uint64    `json:"reset_event_id"`
	CorrectionEventID        uint64    `json:"correction_event_id"`
	RequestLogHighWater      uint64    `json:"request_log_high_water"`
	CostSampledAtBefore      time.Time `json:"cost_sampled_at_before"`
	CostSampledAtAfter       time.Time `json:"cost_sampled_at_after"`
	AffectedStartedAt        time.Time `json:"affected_started_at"`
	AffectedEndedAt          time.Time `json:"affected_ended_at"`
	// Provider factors use the normalized cost-factor convention: divide-mode
	// advertised multiplier 10 is represented as 0.1.
	PreviousProviderFactor string `json:"previous_provider_factor"`
	CurrentProviderFactor  string `json:"current_provider_factor"`
	FactorSource           string `json:"factor_source"`
}

type CostUnitRepairAdoptionPreview struct {
	Anchor                  CostUnitRepairAdoptionAnchor        `json:"anchor"`
	Digest                  string                              `json:"digest"`
	SourceEventsDigest      string                              `json:"source_events_digest"`
	CostStateDigest         string                              `json:"cost_state_digest"`
	ChannelProjectionDigest string                              `json:"channel_projection_digest"`
	FundingFactorBefore     string                              `json:"funding_factor_before"`
	FundingFactorAfter      string                              `json:"funding_factor_after"`
	RequestRestatement      *CostUnitRequestRestatementEvidence `json:"request_restatement"`
}

// ChannelCostUnitRepairAdoption is the append-only copy of an already-applied
// emergency repair and the exact state against which it was adopted.
type ChannelCostUnitRepairAdoption struct {
	ID                                   uint64      `gorm:"primaryKey" json:"id"`
	ChannelID                            uint        `gorm:"not null" json:"channel_id"`
	ChannelIdentityRevision              uint64      `gorm:"not null" json:"-"`
	ChannelNameSnapshot                  string      `gorm:"size:128;not null" json:"channel_name_snapshot"`
	ChannelTypeSnapshot                  ChannelType `gorm:"size:32;not null" json:"channel_type_snapshot"`
	AccountIdentityFingerprint           string      `gorm:"size:64;not null" json:"-"`
	AccountIdentityFingerprintVersion    uint64      `gorm:"not null" json:"-"`
	AccountIdentityFingerprintSource     string      `gorm:"size:128;not null" json:"account_identity_fingerprint_source"`
	AccountIdentityFingerprintObservedAt time.Time   `gorm:"not null" json:"account_identity_fingerprint_observed_at"`
	IdempotencyKey                       string      `gorm:"size:128;not null" json:"idempotency_key"`
	RequestFingerprint                   string      `gorm:"size:64;not null" json:"-"`
	PreviewDigest                        string      `gorm:"size:64;not null" json:"preview_digest"`
	Actor                                string      `gorm:"size:128;not null" json:"actor"`
	SourceEventHighWater                 uint64      `gorm:"not null" json:"source_event_high_water"`
	ResetEventID                         uint64      `gorm:"not null" json:"reset_event_id"`
	CorrectionEventID                    uint64      `gorm:"not null" json:"correction_event_id"`
	ResetEventSnapshot                   string      `gorm:"type:jsonb;not null" json:"-"`
	CorrectionEventSnapshot              string      `gorm:"type:jsonb;not null" json:"-"`
	SourceEventsDigest                   string      `gorm:"size:64;not null" json:"source_events_digest"`
	StateVersion                         uint64      `gorm:"not null" json:"state_version"`
	CostUnitEpoch                        uint64      `gorm:"not null" json:"cost_unit_epoch"`
	StateSourceAtAdoption                string      `gorm:"size:32;not null" json:"state_source_at_adoption"`
	StateStatusAtAdoption                string      `gorm:"size:32;not null" json:"state_status_at_adoption"`
	CostStateSnapshot                    string      `gorm:"type:jsonb;not null" json:"-"`
	CostStateDigest                      string      `gorm:"size:64;not null" json:"cost_state_digest"`
	ChannelProjectionSnapshot            string      `gorm:"type:jsonb;not null" json:"-"`
	ChannelProjectionDigest              string      `gorm:"size:64;not null" json:"channel_projection_digest"`
	CostSampledAtBefore                  time.Time   `gorm:"not null" json:"cost_sampled_at_before"`
	CostSampledAtAfter                   time.Time   `gorm:"not null" json:"cost_sampled_at_after"`
	AffectedStartedAt                    time.Time   `gorm:"not null" json:"affected_started_at"`
	AffectedEndedAt                      time.Time   `gorm:"not null" json:"affected_ended_at"`
	RequestLogHighWater                  uint64      `gorm:"not null" json:"request_log_high_water"`
	PreviousProviderFactor               string      `gorm:"type:numeric;not null" json:"previous_provider_factor"`
	CurrentProviderFactor                string      `gorm:"type:numeric;not null" json:"current_provider_factor"`
	FactorSource                         string      `gorm:"size:32;not null" json:"factor_source"`
	FundingFactorBefore                  string      `gorm:"type:numeric;not null" json:"funding_factor_before"`
	FundingFactorAfter                   string      `gorm:"type:numeric;not null" json:"funding_factor_after"`
	CreatedAt                            time.Time   `gorm:"not null" json:"created_at"`
}

func (ChannelCostUnitRepairAdoption) TableName() string {
	return "channel_cost_unit_repair_adoptions"
}

type costUnitRepairEventAnchor struct {
	ID                  uint64
	ChannelID           uint
	Kind                string
	FundingFactorBefore string
	FundingFactorAfter  string
	EffectiveAt         time.Time
	CreatedAt           time.Time
	Snapshot            string
}

type costUnitRepairStateAnchor struct {
	Version                         uint64
	CostUnitEpoch                   uint64
	Source                          string
	Status                          string
	FundingFactor                   string
	ObservedProviderFactor          string
	ObservedProviderFactorSource    string
	ObservedProviderFactorSampledAt *time.Time
	Snapshot                        string
}

type costUnitRepairProjectionAnchor struct {
	FundingFactor string
	Snapshot      string
	MatchesState  bool
}

type derivedCostUnitRepairAdoption struct {
	Preview            CostUnitRepairAdoptionPreview
	Channel            Channel
	Identity           UpstreamAccountIdentitySnapshot
	ResetEventSnapshot string
	CorrectionSnapshot string
	CostStateSnapshot  string
	ProjectionSnapshot string
	StateSource        string
	StateStatus        string
}

func (r *Costing) PreviewCostUnitRepairAdoption(anchor CostUnitRepairAdoptionAnchor) (*CostUnitRepairAdoptionPreview, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("costing store is unavailable")
	}
	anchor, err := normalizeCostUnitRepairAdoptionAnchor(anchor, false)
	if err != nil {
		return nil, err
	}
	var derived *derivedCostUnitRepairAdoption
	err = r.db.Transaction(func(tx *gorm.DB) error {
		var deriveErr error
		derived, deriveErr = deriveCostUnitRepairAdoptionTx(tx, anchor, false)
		return deriveErr
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	preview := derived.Preview
	return &preview, nil
}

func (r *Costing) FindCostUnitRepairAdoption(channelID uint, channelIdentityRevision uint64, idempotencyKey string) (*ChannelCostUnitRepairAdoption, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if r == nil || r.db == nil || channelID == 0 || channelIdentityRevision == 0 || idempotencyKey == "" {
		return nil, ErrCostUnitRepairAdoptionInvalid
	}
	var adoption ChannelCostUnitRepairAdoption
	if err := r.db.Where("channel_id = ? AND channel_identity_revision = ? AND idempotency_key = ?",
		channelID, channelIdentityRevision, idempotencyKey).Take(&adoption).Error; err != nil {
		return nil, err
	}
	return &adoption, nil
}

func (r *Costing) ApplyCostUnitRepairAdoption(anchor CostUnitRepairAdoptionAnchor, previewDigest, idempotencyKey, actor string) (*ChannelCostUnitRepairAdoption, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("costing store is unavailable")
	}
	var err error
	anchor, err = normalizeCostUnitRepairAdoptionAnchor(anchor, true)
	previewDigest = strings.ToLower(strings.TrimSpace(previewDigest))
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	actor = strings.TrimSpace(actor)
	if err != nil || !isLowerHexFingerprint(previewDigest) || idempotencyKey == "" || len(idempotencyKey) > 128 || actor == "" || len(actor) > 128 {
		return nil, false, ErrCostUnitRepairAdoptionInvalid
	}
	requestFingerprint, err := costUnitRepairAdoptionRequestFingerprint(anchor, previewDigest, idempotencyKey, actor)
	if err != nil {
		return nil, false, err
	}

	var committed ChannelCostUnitRepairAdoption
	replayed := false
	err = r.db.Transaction(func(tx *gorm.DB) error {
		// Serialize idempotency and source-event ownership without making an
		// exact replay depend on the channel still being writable (or present).
		// A new adoption enters the ordinary channel lock domain in derive below.
		if err := lockCostUnitRepairAdoptionTx(tx, anchor.ChannelID); err != nil {
			return err
		}
		var existing ChannelCostUnitRepairAdoption
		existingErr := tx.Where("channel_id = ? AND channel_identity_revision = ? AND idempotency_key = ?",
			anchor.ChannelID, anchor.ExpectedIdentityRevision, idempotencyKey).Take(&existing).Error
		if existingErr == nil {
			if existing.RequestFingerprint != requestFingerprint || existing.Actor != actor || existing.PreviewDigest != previewDigest {
				return ErrCostUnitRepairAdoptionIdempotencyConflict
			}
			if err := verifyCostUnitRepairAdoptionRestatementTx(tx, &existing); err != nil {
				return err
			}
			committed = existing
			replayed = true
			return nil
		}
		if !errors.Is(existingErr, gorm.ErrRecordNotFound) {
			return existingErr
		}
		var sourceExisting ChannelCostUnitRepairAdoption
		sourceErr := tx.Where("channel_id = ? AND reset_event_id = ? AND correction_event_id = ?",
			anchor.ChannelID, anchor.ResetEventID, anchor.CorrectionEventID).Take(&sourceExisting).Error
		if sourceErr == nil {
			return ErrCostUnitRepairAdoptionIdempotencyConflict
		}
		if !errors.Is(sourceErr, gorm.ErrRecordNotFound) {
			return sourceErr
		}

		derived, err := deriveCostUnitRepairAdoptionTx(tx, anchor, true)
		if err != nil {
			return err
		}
		if derived.Preview.Digest != previewDigest {
			return ErrCostUnitRepairAdoptionConflict
		}
		adoption := ChannelCostUnitRepairAdoption{
			ChannelID: derived.Channel.ID, ChannelIdentityRevision: derived.Identity.ChannelIdentityRevision,
			ChannelNameSnapshot: strings.TrimSpace(derived.Channel.Name), ChannelTypeSnapshot: derived.Channel.Type,
			AccountIdentityFingerprint:           derived.Identity.Fingerprint,
			AccountIdentityFingerprintVersion:    derived.Identity.FingerprintVersion,
			AccountIdentityFingerprintSource:     strings.TrimSpace(derived.Identity.Source),
			AccountIdentityFingerprintObservedAt: derived.Identity.ObservedAt.UTC(),
			IdempotencyKey:                       idempotencyKey, RequestFingerprint: requestFingerprint, PreviewDigest: previewDigest, Actor: actor,
			SourceEventHighWater: anchor.SourceEventHighWater, ResetEventID: anchor.ResetEventID, CorrectionEventID: anchor.CorrectionEventID,
			ResetEventSnapshot: derived.ResetEventSnapshot, CorrectionEventSnapshot: derived.CorrectionSnapshot,
			SourceEventsDigest: derived.Preview.SourceEventsDigest,
			StateVersion:       anchor.ExpectedStateVersion, CostUnitEpoch: anchor.ExpectedCostUnitEpoch,
			StateSourceAtAdoption: derived.StateSource, StateStatusAtAdoption: derived.StateStatus,
			CostStateSnapshot: derived.CostStateSnapshot, CostStateDigest: derived.Preview.CostStateDigest,
			ChannelProjectionSnapshot: derived.ProjectionSnapshot, ChannelProjectionDigest: derived.Preview.ChannelProjectionDigest,
			CostSampledAtBefore: anchor.CostSampledAtBefore, CostSampledAtAfter: anchor.CostSampledAtAfter,
			AffectedStartedAt: anchor.AffectedStartedAt, AffectedEndedAt: anchor.AffectedEndedAt,
			RequestLogHighWater:    anchor.RequestLogHighWater,
			PreviousProviderFactor: anchor.PreviousProviderFactor, CurrentProviderFactor: anchor.CurrentProviderFactor,
			FactorSource: anchor.FactorSource, FundingFactorBefore: derived.Preview.FundingFactorBefore,
			FundingFactorAfter: derived.Preview.FundingFactorAfter,
		}
		if err := tx.Create(&adoption).Error; err != nil {
			return fmt.Errorf("append channel cost unit repair adoption: %w", err)
		}
		adoptionID := adoption.ID
		if err := applyCostUnitRequestRestatementForParentTx(tx, costUnitRequestRestatementParent{
			RepairAdoptionID: &adoptionID, ChannelID: adoption.ChannelID,
			ChannelIdentityRevision: adoption.ChannelIdentityRevision,
			IdempotencyKey:          adoption.IdempotencyKey, RequestFingerprint: adoption.RequestFingerprint, Actor: adoption.Actor,
			CostSampledAtBefore: adoption.CostSampledAtBefore, CostSampledAtAfter: adoption.CostSampledAtAfter,
			OldFundingFactor: adoption.FundingFactorBefore, NewFundingFactor: adoption.FundingFactorAfter,
			AppliedCostBasisVersion: adoption.StateVersion, AppliedCostBasisSource: costUnitRepairAdoptionBasisSource,
		}, derived.Preview.RequestRestatement); err != nil {
			return err
		}
		committed = adoption
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return &committed, replayed, nil
}

func verifyCostUnitRepairAdoptionRestatementTx(tx *gorm.DB, adoption *ChannelCostUnitRepairAdoption) error {
	if tx == nil || adoption == nil || adoption.ID == 0 {
		return ErrCostUnitRepairAdoptionConflict
	}
	var operation ChannelCostUnitRequestRestatement
	if err := tx.Where("repair_adoption_id = ?", adoption.ID).Take(&operation).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrCostUnitRepairAdoptionConflict
		}
		return err
	}
	if operation.RebaseID != nil || operation.RepairAdoptionID == nil || *operation.RepairAdoptionID != adoption.ID ||
		operation.ChannelID != adoption.ChannelID || operation.ChannelIdentityRevision != adoption.ChannelIdentityRevision ||
		operation.IdempotencyKey != adoption.IdempotencyKey || operation.RequestFingerprint != adoption.RequestFingerprint ||
		operation.Actor != adoption.Actor || operation.RequestLogHighWater != adoption.RequestLogHighWater ||
		!operation.CostSampledAtBefore.Equal(adoption.CostSampledAtBefore) ||
		!operation.CostSampledAtAfter.Equal(adoption.CostSampledAtAfter) ||
		!operation.AffectedStartedAt.Equal(adoption.AffectedStartedAt) ||
		!operation.AffectedEndedAt.Equal(adoption.AffectedEndedAt) ||
		!decimalTextEqual(operation.OldFundingFactor, adoption.FundingFactorBefore) ||
		!decimalTextEqual(operation.NewFundingFactor, adoption.FundingFactorAfter) ||
		operation.AppliedCostBasisVersion != adoption.StateVersion ||
		operation.AppliedCostBasisSource != costUnitRepairAdoptionBasisSource {
		return ErrCostUnitRepairAdoptionConflict
	}

	var bucketStarts []time.Time
	if err := json.Unmarshal([]byte(operation.ExpectedBucketStartsJSON), &bucketStarts); err != nil {
		return ErrCostUnitRepairAdoptionConflict
	}
	evidence, err := NormalizeCostUnitRequestRestatementEvidence(&CostUnitRequestRestatementEvidence{
		RequestLogHighWater:              operation.RequestLogHighWater,
		AffectedStartedAt:                operation.AffectedStartedAt,
		AffectedEndedAt:                  operation.AffectedEndedAt,
		OldFundingFactor:                 operation.OldFundingFactor,
		NewFundingFactor:                 operation.NewFundingFactor,
		ExpectedPopulationRows:           operation.ExpectedPopulationRows,
		ExpectedNewRatioRows:             operation.ExpectedNewRatioRows,
		ExpectedOldRatioRows:             operation.ExpectedOldRatioRows,
		ExpectedRatioMismatchRows:        operation.ExpectedRatioMismatchRows,
		ExpectedLatencyProbeRows:         operation.ExpectedLatencyProbeRows,
		ExpectedRestatementRows:          operation.ExpectedRestatementRows,
		ExpectedAlreadyCorrectCostedRows: operation.ExpectedAlreadyCorrectCostedRows,
		ExpectedRawCostSum:               operation.ExpectedRawCostSum,
		ExpectedRecordedCostSum:          operation.ExpectedRecordedCostSum,
		ExpectedCorrectedCostSum:         operation.ExpectedCorrectedCostSum,
		ExpectedUndercountSum:            operation.ExpectedUndercountSum,
		ExpectedAlreadyCorrectRawCostSum: operation.ExpectedAlreadyCorrectRawCostSum,
		ExpectedAllCostedRawCostSum:      operation.ExpectedAllCostedRawCostSum,
		ExpectedBucketStarts:             bucketStarts,
		ExpectedBucketDigest:             operation.ExpectedBucketDigest,
	})
	if err != nil || evidence == nil || len(bucketStarts) != operation.ExpectedBucketCount ||
		operation.AppliedRestatementRows != operation.ExpectedRestatementRows ||
		!decimalTextEqual(operation.AppliedRawCostSum, operation.ExpectedRawCostSum) ||
		!decimalTextEqual(operation.AppliedRecordedCostSum, operation.ExpectedRecordedCostSum) ||
		!decimalTextEqual(operation.AppliedCorrectedCostSum, operation.ExpectedCorrectedCostSum) ||
		!decimalTextEqual(operation.AppliedUndercountSum, operation.ExpectedUndercountSum) {
		return ErrCostUnitRepairAdoptionConflict
	}
	err = verifyAppliedCostUnitRequestRestatementTx(tx, operation.ID, costUnitRequestRestatementParent{
		RepairAdoptionID:        operation.RepairAdoptionID,
		ChannelID:               adoption.ChannelID,
		ChannelIdentityRevision: adoption.ChannelIdentityRevision,
	}, evidence)
	if errors.Is(err, ErrCostUnitRequestRestatementConflict) || errors.Is(err, ErrCostUnitRequestRestatementInvalid) {
		return ErrCostUnitRepairAdoptionConflict
	}
	return err
}

func lockCostUnitRepairAdoptionTx(tx *gorm.DB, channelID uint) error {
	if tx == nil || channelID == 0 {
		return ErrCostUnitRepairAdoptionInvalid
	}
	key := fmt.Sprintf("bridgeops:cost-unit-repair-adoption:%d", channelID)
	return tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", key).Error
}

func normalizeCostUnitRepairAdoptionAnchor(anchor CostUnitRepairAdoptionAnchor, requireFrozen bool) (CostUnitRepairAdoptionAnchor, error) {
	anchor.PreviousProviderFactor = strings.TrimSpace(anchor.PreviousProviderFactor)
	anchor.CurrentProviderFactor = strings.TrimSpace(anchor.CurrentProviderFactor)
	anchor.FactorSource = strings.TrimSpace(anchor.FactorSource)
	anchor.CostSampledAtBefore = anchor.CostSampledAtBefore.UTC()
	anchor.CostSampledAtAfter = anchor.CostSampledAtAfter.UTC()
	anchor.AffectedStartedAt = anchor.AffectedStartedAt.UTC()
	anchor.AffectedEndedAt = anchor.AffectedEndedAt.UTC()
	if anchor.ChannelID == 0 || anchor.ResetEventID == 0 || anchor.CorrectionEventID <= anchor.ResetEventID ||
		anchor.CostSampledAtBefore.IsZero() || anchor.CostSampledAtAfter.IsZero() ||
		anchor.AffectedStartedAt.IsZero() || anchor.AffectedEndedAt.IsZero() ||
		!anchor.CostSampledAtBefore.Before(anchor.AffectedStartedAt) || anchor.AffectedStartedAt.After(anchor.AffectedEndedAt) ||
		anchor.AffectedEndedAt.After(anchor.CostSampledAtAfter) ||
		anchor.CostSampledAtBefore.Nanosecond()%1000 != 0 || anchor.CostSampledAtAfter.Nanosecond()%1000 != 0 ||
		anchor.AffectedStartedAt.Nanosecond()%1000 != 0 || anchor.AffectedEndedAt.Nanosecond()%1000 != 0 ||
		anchor.FactorSource != "advertised" || !validPositiveDecimal(anchor.PreviousProviderFactor) ||
		!validPositiveDecimal(anchor.CurrentProviderFactor) || decimalTextEqual(anchor.PreviousProviderFactor, anchor.CurrentProviderFactor) {
		return anchor, ErrCostUnitRepairAdoptionInvalid
	}
	if requireFrozen && (anchor.ExpectedIdentityRevision == 0 || anchor.ExpectedAccountRecordID == 0 ||
		anchor.ExpectedStateVersion == 0 || anchor.ExpectedCostUnitEpoch == 0 ||
		anchor.SourceEventHighWater == 0 || anchor.RequestLogHighWater == 0 ||
		anchor.SourceEventHighWater > math.MaxInt64 || anchor.RequestLogHighWater > math.MaxInt64) {
		return anchor, ErrCostUnitRepairAdoptionInvalid
	}
	return anchor, nil
}

func deriveCostUnitRepairAdoptionTx(tx *gorm.DB, anchor CostUnitRepairAdoptionAnchor, lock bool) (*derivedCostUnitRepairAdoption, error) {
	if tx == nil {
		return nil, ErrCostUnitRepairAdoptionInvalid
	}
	var channel *Channel
	var state *ChannelCostState
	var err error
	if lock {
		channel, state, err = lockChannelAndCostStateTx(tx, anchor.ChannelID)
	} else {
		var loadedChannel Channel
		if err = tx.First(&loadedChannel, anchor.ChannelID).Error; err == nil {
			if loadedChannel.DeletionPending {
				err = ErrChannelDeletionPending
			} else {
				var loadedState ChannelCostState
				err = tx.First(&loadedState, "channel_id = ?", anchor.ChannelID).Error
				if err == nil && (loadedChannel.CostUnitEpoch == 0 || loadedChannel.CostUnitEpoch != loadedState.CostUnitEpoch) {
					err = ErrCostUnitEpochConflict
				}
				channel, state = &loadedChannel, &loadedState
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if channel.Type != ChannelTypeSub2API || state.Version == 0 || state.CostUnitEpoch == 0 ||
		strings.TrimSpace(state.Source) != "manual_denomination_reset" {
		return nil, ErrCostUnitRepairAdoptionInvalid
	}
	identity, err := loadCurrentUpstreamAccountIdentity(tx, anchor.ChannelID, lock)
	if err != nil {
		return nil, err
	}
	if anchor.ExpectedIdentityRevision == 0 {
		anchor.ExpectedIdentityRevision = identity.ChannelIdentityRevision
	}
	if anchor.ExpectedAccountRecordID == 0 {
		anchor.ExpectedAccountRecordID = identity.AccountRecordID
	}
	if anchor.ExpectedStateVersion == 0 {
		anchor.ExpectedStateVersion = state.Version
	}
	if anchor.ExpectedCostUnitEpoch == 0 {
		anchor.ExpectedCostUnitEpoch = state.CostUnitEpoch
	}
	if anchor.ExpectedIdentityRevision != identity.ChannelIdentityRevision || anchor.ExpectedIdentityRevision != channel.UpstreamAccountIdentityRevision {
		return nil, ErrStaleChannelAccountIdentityRevision
	}
	if anchor.ExpectedAccountRecordID != identity.AccountRecordID {
		return nil, ErrStaleUpstreamAccountObservation
	}
	if anchor.ExpectedStateVersion != state.Version {
		return nil, ErrCostStateConflict
	}
	if anchor.ExpectedCostUnitEpoch != state.CostUnitEpoch {
		return nil, ErrCostUnitEpochConflict
	}

	reset, err := loadCostUnitRepairEventAnchorTx(tx, anchor.ChannelID, anchor.ResetEventID, lock)
	if err != nil {
		return nil, err
	}
	correction, err := loadCostUnitRepairEventAnchorTx(tx, anchor.ChannelID, anchor.CorrectionEventID, lock)
	if err != nil {
		return nil, err
	}
	var sourceHighWater uint64
	if err := tx.Raw("SELECT COALESCE(max(id), 0) FROM public.channel_funding_events WHERE channel_id = ?", anchor.ChannelID).Scan(&sourceHighWater).Error; err != nil {
		return nil, err
	}
	if anchor.SourceEventHighWater == 0 {
		anchor.SourceEventHighWater = sourceHighWater
	}
	if sourceHighWater != anchor.SourceEventHighWater || correction.ID > sourceHighWater ||
		reset.Kind != "denomination_reset" || correction.Kind != "denomination_cost_fix" ||
		!correction.EffectiveAt.After(reset.EffectiveAt) || !anchor.AffectedEndedAt.Equal(reset.EffectiveAt.UTC()) ||
		!anchor.CostSampledAtAfter.Equal(correction.EffectiveAt.UTC()) ||
		!decimalTextEqual(reset.FundingFactorAfter, correction.FundingFactorBefore) ||
		!decimalTextEqual(reset.FundingFactorAfter, correction.FundingFactorAfter) ||
		decimalTextEqual(reset.FundingFactorBefore, reset.FundingFactorAfter) ||
		!decimalScaleMatches(reset.FundingFactorBefore, correction.FundingFactorAfter, anchor.PreviousProviderFactor, anchor.CurrentProviderFactor) {
		return nil, ErrCostUnitRepairAdoptionInvalid
	}

	stateAnchor, err := loadCostUnitRepairStateAnchorTx(tx, anchor.ChannelID, lock)
	if err != nil {
		return nil, err
	}
	projection, err := loadCostUnitRepairProjectionAnchorTx(tx, anchor.ChannelID, lock)
	if err != nil {
		return nil, err
	}
	if stateAnchor.Version != anchor.ExpectedStateVersion || stateAnchor.CostUnitEpoch != anchor.ExpectedCostUnitEpoch ||
		stateAnchor.Source != "manual_denomination_reset" || stateAnchor.Status != "ready" ||
		!decimalTextEqual(stateAnchor.FundingFactor, correction.FundingFactorAfter) ||
		!projection.MatchesState || !decimalTextEqual(projection.FundingFactor, stateAnchor.FundingFactor) {
		return nil, ErrCostUnitRepairAdoptionConflict
	}
	providerEvidenceMatches := validPositiveDecimal(stateAnchor.ObservedProviderFactor) &&
		decimalValuesClose(stateAnchor.ObservedProviderFactor, anchor.CurrentProviderFactor) &&
		strings.TrimSpace(stateAnchor.ObservedProviderFactorSource) == anchor.FactorSource &&
		stateAnchor.ObservedProviderFactorSampledAt != nil &&
		!stateAnchor.ObservedProviderFactorSampledAt.UTC().Before(anchor.CostSampledAtAfter)
	if !providerEvidenceMatches {
		if lock {
			return nil, ErrCostUnitRepairAdoptionConflict
		}
		return nil, ErrCostUnitRepairAdoptionInvalid
	}
	var pending int64
	if err := tx.Model(&ChannelFundingCandidate{}).Where("channel_id = ? AND status = ?", anchor.ChannelID, FundingCandidatePending).Count(&pending).Error; err != nil {
		return nil, err
	}
	if pending != 0 {
		return nil, ErrPendingFundingCandidate
	}

	var requestHighWater uint64
	if err := tx.Raw("SELECT COALESCE(max(id), 0) FROM public.gateway_request_logs").Scan(&requestHighWater).Error; err != nil {
		return nil, err
	}
	if anchor.RequestLogHighWater == 0 {
		anchor.RequestLogHighWater = requestHighWater
	}
	// Requests outside the frozen channel/time/factor population may continue
	// while an administrator reviews the preview. The restatement transaction
	// separately rejects any target row that arrives above this bound.
	if requestHighWater < anchor.RequestLogHighWater {
		return nil, ErrCostUnitRepairAdoptionConflict
	}
	restatement, err := deriveCostUnitRequestRestatementEvidenceTx(tx, anchor, reset.FundingFactorBefore, correction.FundingFactorAfter)
	if err != nil {
		return nil, err
	}

	sourceDigest := sha256Hex(reset.Snapshot + "\n" + correction.Snapshot)
	stateDigest := sha256Hex(stateAnchor.Snapshot)
	projectionDigest := sha256Hex(projection.Snapshot)
	previewPayload := struct {
		SchemaVersion         int                                 `json:"schema_version"`
		Anchor                CostUnitRepairAdoptionAnchor        `json:"anchor"`
		ChannelName           string                              `json:"channel_name"`
		ChannelType           ChannelType                         `json:"channel_type"`
		AccountFingerprint    string                              `json:"account_fingerprint"`
		AccountFingerprintVer uint64                              `json:"account_fingerprint_version"`
		AccountFingerprintSrc string                              `json:"account_fingerprint_source"`
		AccountFingerprintAt  time.Time                           `json:"account_fingerprint_observed_at"`
		ResetEvent            json.RawMessage                     `json:"reset_event"`
		CorrectionEvent       json.RawMessage                     `json:"correction_event"`
		CostState             json.RawMessage                     `json:"cost_state"`
		ChannelProjection     json.RawMessage                     `json:"channel_projection"`
		RequestRestatement    *CostUnitRequestRestatementEvidence `json:"request_restatement"`
	}{
		SchemaVersion: 1, Anchor: anchor, ChannelName: strings.TrimSpace(channel.Name), ChannelType: channel.Type,
		AccountFingerprint: identity.Fingerprint, AccountFingerprintVer: identity.FingerprintVersion,
		AccountFingerprintSrc: strings.TrimSpace(identity.Source), AccountFingerprintAt: identity.ObservedAt.UTC(),
		ResetEvent: json.RawMessage(reset.Snapshot), CorrectionEvent: json.RawMessage(correction.Snapshot),
		CostState: json.RawMessage(stateAnchor.Snapshot), ChannelProjection: json.RawMessage(projection.Snapshot),
		RequestRestatement: restatement,
	}
	encoded, err := json.Marshal(previewPayload)
	if err != nil {
		return nil, err
	}
	preview := CostUnitRepairAdoptionPreview{
		Anchor: anchor, Digest: sha256Hex(string(encoded)), SourceEventsDigest: sourceDigest,
		CostStateDigest: stateDigest, ChannelProjectionDigest: projectionDigest,
		FundingFactorBefore: reset.FundingFactorBefore, FundingFactorAfter: correction.FundingFactorAfter,
		RequestRestatement: restatement,
	}
	return &derivedCostUnitRepairAdoption{
		Preview: preview, Channel: *channel, Identity: *identity,
		ResetEventSnapshot: reset.Snapshot, CorrectionSnapshot: correction.Snapshot,
		CostStateSnapshot: stateAnchor.Snapshot, ProjectionSnapshot: projection.Snapshot,
		StateSource: stateAnchor.Source, StateStatus: stateAnchor.Status,
	}, nil
}

func loadCostUnitRepairEventAnchorTx(tx *gorm.DB, channelID uint, eventID uint64, lock bool) (costUnitRepairEventAnchor, error) {
	var event costUnitRepairEventAnchor
	query := `
SELECT id, channel_id, kind,
       funding_factor_before::text AS funding_factor_before,
       funding_factor_after::text AS funding_factor_after,
       effective_at, created_at, to_jsonb(events)::text AS snapshot
FROM public.channel_funding_events AS events
WHERE id = ? AND channel_id = ?`
	if lock {
		query += " FOR UPDATE"
	}
	err := tx.Raw(query, eventID, channelID).Scan(&event).Error
	if err == nil && event.ID == 0 {
		err = gorm.ErrRecordNotFound
	}
	return event, err
}

func loadCostUnitRepairStateAnchorTx(tx *gorm.DB, channelID uint, lock bool) (costUnitRepairStateAnchor, error) {
	var state costUnitRepairStateAnchor
	query := `
SELECT version, cost_unit_epoch, source, status,
       funding_factor::text AS funding_factor,
       observed_provider_factor::text AS observed_provider_factor,
       observed_provider_factor_source,
       observed_provider_factor_sampled_at,
       to_jsonb(states)::text AS snapshot
FROM public.channel_cost_states AS states
WHERE channel_id = ?`
	if lock {
		query += " FOR UPDATE"
	}
	err := tx.Raw(query, channelID).Scan(&state).Error
	if err == nil && state.Version == 0 {
		err = gorm.ErrRecordNotFound
	}
	return state, err
}

func loadCostUnitRepairProjectionAnchorTx(tx *gorm.DB, channelID uint, lock bool) (costUnitRepairProjectionAnchor, error) {
	var projection costUnitRepairProjectionAnchor
	query := `
SELECT channel.funding_factor::text AS funding_factor,
	       (channel.raw_last_balance IS NOT DISTINCT FROM state.raw_balance
	        AND channel.last_balance IS NOT DISTINCT FROM state.actual_inventory
	        AND channel.last_balance_at IS NOT DISTINCT FROM state.balance_sampled_at
	        AND channel.funding_factor = state.funding_factor
	        AND channel.funding_factor_source = state.source
	        AND channel.raw_today_cost IS NOT DISTINCT FROM state.raw_today_cost
	        AND channel.today_cost IS NOT DISTINCT FROM state.actual_today_cost
	        AND channel.raw_total_cost IS NOT DISTINCT FROM state.raw_total_cost
	        AND channel.total_cost IS NOT DISTINCT FROM state.actual_total_cost
	        AND channel.today_cost_at IS NOT DISTINCT FROM state.cost_sampled_at
	        AND channel.cost_unit_epoch = state.cost_unit_epoch) AS matches_state,
	       jsonb_build_object(
           'id', channel.id,
           'upstream_account_identity_revision', channel.upstream_account_identity_revision,
           'cost_unit_epoch', channel.cost_unit_epoch,
	           'raw_last_balance', channel.raw_last_balance,
	           'last_balance', channel.last_balance,
	           'last_balance_at', channel.last_balance_at,
	           'funding_factor', channel.funding_factor,
	           'funding_factor_source', channel.funding_factor_source,
	           'raw_today_cost', channel.raw_today_cost,
	           'today_cost', channel.today_cost,
	           'raw_total_cost', channel.raw_total_cost,
	           'total_cost', channel.total_cost,
	           'today_cost_at', channel.today_cost_at,
	           'rate_snapshot_observed_at', channel.rate_snapshot_observed_at
       )::text AS snapshot
FROM public.channels AS channel
JOIN public.channel_cost_states AS state ON state.channel_id = channel.id
WHERE channel.id = ?`
	if lock {
		query += " FOR UPDATE OF channel"
	}
	err := tx.Raw(query, channelID).Scan(&projection).Error
	if err == nil && projection.Snapshot == "" {
		err = gorm.ErrRecordNotFound
	}
	return projection, err
}

func deriveCostUnitRequestRestatementEvidenceTx(tx *gorm.DB, anchor CostUnitRepairAdoptionAnchor, oldFactor, newFactor string) (*CostUnitRequestRestatementEvidence, error) {
	var derived struct {
		PopulationRows           int64
		NewRatioRows             int64
		OldRatioRows             int64
		RatioMismatchRows        int64
		LatencyProbeRows         int64
		RestatementRows          int64
		AlreadyCorrectCostedRows int64
		ResidualViolationRows    int64
		BasisVersionViolations   int64
		RawCostSum               string
		RecordedCostSum          string
		CorrectedCostSum         string
		UndercountSum            string
		AlreadyCorrectRawCostSum string
		AllCostedRawCostSum      string
	}
	err := tx.Raw(`
WITH scoped AS (
    SELECT *,
        raw_group_ratio IS NOT NULL AND raw_group_ratio = group_ratio AS is_new_ratio,
        raw_group_ratio IS NOT NULL
            AND raw_group_ratio <> group_ratio
            AND abs(group_ratio - raw_group_ratio * CAST(? AS numeric)) <= 0.000000000001::numeric AS is_old_ratio
    FROM public.gateway_request_logs
    WHERE channel_id = ?
      AND channel_identity_revision = ?
      AND id <= ?
      AND created_at >= ?
      AND created_at <= ?
      AND price_fact_id IS NULL
      AND funding_factor = CAST(? AS numeric)
), classified AS (
    SELECT *,
        raw_cost IS NOT NULL
            AND COALESCE(route_reason, '') <> 'latency_probe'
            AND is_new_ratio AS should_restate
    FROM scoped
)
SELECT
    count(*) AS population_rows,
    count(*) FILTER (WHERE is_new_ratio) AS new_ratio_rows,
    count(*) FILTER (WHERE is_old_ratio) AS old_ratio_rows,
    count(*) FILTER (WHERE NOT is_new_ratio AND NOT is_old_ratio) AS ratio_mismatch_rows,
    count(*) FILTER (WHERE COALESCE(route_reason, '') = 'latency_probe') AS latency_probe_rows,
    count(*) FILTER (WHERE should_restate) AS restatement_rows,
    count(*) FILTER (WHERE raw_cost IS NOT NULL AND COALESCE(route_reason, '') <> 'latency_probe' AND is_old_ratio) AS already_correct_costed_rows,
    count(*) FILTER (WHERE should_restate AND abs(cost - raw_cost * CAST(? AS numeric)) > 0.000000000001::numeric) AS residual_violation_rows,
    count(*) FILTER (WHERE should_restate AND cost_basis_version >= ?) AS basis_version_violations,
    COALESCE(sum(raw_cost) FILTER (WHERE should_restate), 0)::text AS raw_cost_sum,
    COALESCE(sum(cost) FILTER (WHERE should_restate), 0)::text AS recorded_cost_sum,
    COALESCE(sum(raw_cost * CAST(? AS numeric)) FILTER (WHERE should_restate), 0)::text AS corrected_cost_sum,
    COALESCE(sum(raw_cost * CAST(? AS numeric) - cost) FILTER (WHERE should_restate), 0)::text AS undercount_sum,
    COALESCE(sum(raw_cost) FILTER (WHERE raw_cost IS NOT NULL AND COALESCE(route_reason, '') <> 'latency_probe' AND is_old_ratio), 0)::text AS already_correct_raw_cost_sum,
    COALESCE(sum(raw_cost) FILTER (WHERE raw_cost IS NOT NULL AND COALESCE(route_reason, '') <> 'latency_probe'), 0)::text AS all_costed_raw_cost_sum
FROM classified`, oldFactor, anchor.ChannelID, anchor.ExpectedIdentityRevision, anchor.RequestLogHighWater, anchor.AffectedStartedAt, anchor.AffectedEndedAt, oldFactor,
		oldFactor, anchor.ExpectedStateVersion, newFactor, newFactor).Scan(&derived).Error
	if err != nil {
		return nil, err
	}
	if derived.ResidualViolationRows != 0 || derived.BasisVersionViolations != 0 {
		return nil, ErrCostUnitRepairAdoptionInvalid
	}
	evidence := &CostUnitRequestRestatementEvidence{
		RequestLogHighWater: anchor.RequestLogHighWater,
		AffectedStartedAt:   anchor.AffectedStartedAt, AffectedEndedAt: anchor.AffectedEndedAt,
		OldFundingFactor: oldFactor, NewFundingFactor: newFactor,
		ExpectedPopulationRows: derived.PopulationRows, ExpectedNewRatioRows: derived.NewRatioRows,
		ExpectedOldRatioRows: derived.OldRatioRows, ExpectedRatioMismatchRows: derived.RatioMismatchRows,
		ExpectedLatencyProbeRows: derived.LatencyProbeRows, ExpectedRestatementRows: derived.RestatementRows,
		ExpectedAlreadyCorrectCostedRows: derived.AlreadyCorrectCostedRows,
		ExpectedRawCostSum:               derived.RawCostSum, ExpectedRecordedCostSum: derived.RecordedCostSum,
		ExpectedCorrectedCostSum: derived.CorrectedCostSum, ExpectedUndercountSum: derived.UndercountSum,
		ExpectedAlreadyCorrectRawCostSum: derived.AlreadyCorrectRawCostSum,
		ExpectedAllCostedRawCostSum:      derived.AllCostedRawCostSum,
	}
	buckets, err := loadCostUnitRequestRestatementBucketsTx(tx, anchor.ChannelID, anchor.ExpectedIdentityRevision, evidence)
	if err != nil {
		return nil, err
	}
	evidence.ExpectedBucketStarts = buckets
	evidence.ExpectedBucketDigest, err = CostUnitRequestRestatementBucketDigest(buckets)
	if err != nil {
		return nil, err
	}
	return NormalizeCostUnitRequestRestatementEvidence(evidence)
}

func costUnitRepairAdoptionRequestFingerprint(anchor CostUnitRepairAdoptionAnchor, previewDigest, idempotencyKey, actor string) (string, error) {
	payload := struct {
		Anchor         CostUnitRepairAdoptionAnchor `json:"anchor"`
		PreviewDigest  string                       `json:"preview_digest"`
		IdempotencyKey string                       `json:"idempotency_key"`
		Actor          string                       `json:"actor"`
	}{anchor, previewDigest, idempotencyKey, actor}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return sha256Hex(string(encoded)), nil
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func validPositiveDecimal(value string) bool {
	if len(value) == 0 || len(value) > 128 || !unsignedDecimalPattern.MatchString(value) {
		return false
	}
	number, ok := new(big.Rat).SetString(value)
	return ok && number.Sign() > 0
}

func decimalTextEqual(left, right string) bool {
	leftNumber, leftOK := new(big.Rat).SetString(left)
	rightNumber, rightOK := new(big.Rat).SetString(right)
	return leftOK && rightOK && leftNumber.Cmp(rightNumber) == 0
}

func decimalScaleMatches(fundingBefore, fundingAfter, providerBefore, providerAfter string) bool {
	beforeFunding, beforeFundingOK := new(big.Rat).SetString(fundingBefore)
	afterFunding, afterFundingOK := new(big.Rat).SetString(fundingAfter)
	beforeProvider, beforeProviderOK := new(big.Rat).SetString(providerBefore)
	afterProvider, afterProviderOK := new(big.Rat).SetString(providerAfter)
	if !beforeFundingOK || !afterFundingOK || !beforeProviderOK || !afterProviderOK {
		return false
	}
	left := new(big.Rat).Mul(afterFunding, beforeProvider)
	right := new(big.Rat).Mul(beforeFunding, afterProvider)
	return decimalRationalsClose(left, right)
}

func decimalValuesClose(leftText, rightText string) bool {
	left, leftOK := new(big.Rat).SetString(leftText)
	right, rightOK := new(big.Rat).SetString(rightText)
	return leftOK && rightOK && decimalRationalsClose(left, right)
}

func decimalRationalsClose(left, right *big.Rat) bool {
	if left == nil || right == nil {
		return false
	}
	difference := new(big.Rat).Abs(new(big.Rat).Sub(left, right))
	scale := new(big.Rat).Abs(new(big.Rat).Set(left))
	if rightScale := new(big.Rat).Abs(new(big.Rat).Set(right)); rightScale.Cmp(scale) > 0 {
		scale = rightScale
	}
	if scale.Cmp(big.NewRat(1, 1)) < 0 {
		scale = big.NewRat(1, 1)
	}
	tolerance := new(big.Rat).Quo(scale, big.NewRat(1_000_000_000_000, 1))
	return difference.Cmp(tolerance) <= 0
}
