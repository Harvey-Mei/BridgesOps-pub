package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	FundingCandidatePending    = "pending"
	FundingCandidateConfirmed  = "confirmed"
	FundingCandidateSuperseded = "superseded"
)

var ErrCostStateConflict = errors.New("channel cost state version conflict")
var ErrFundingCandidateOrder = errors.New("older funding candidate must be confirmed first")
var ErrPendingFundingCandidate = errors.New("pending funding candidate must be resolved first")
var ErrFundingCandidateSuperseded = errors.New("funding candidate was superseded by a later confirmed funding event")
var ErrFundingCandidateResolved = errors.New("funding candidate is no longer pending")
var ErrFundingCandidateCostBasis = errors.New("funding candidate cost basis changed")
var ErrCostUnitRebaseIdempotencyConflict = errors.New("cost unit rebase idempotency key conflicts with another request")
var ErrCostUnitRebaseInvalid = errors.New("cost unit rebase evidence is invalid")
var ErrCostStateProjectionMissing = errors.New("channel cost projection target is missing")
var ErrCostUnitEpochConflict = errors.New("channel cost-unit epoch conflict")

// Costing owns persistence for normalized channel costs and the append-only
// funding ledger.
type Costing struct{ db *gorm.DB }

// CostUnitRateTransitionItem freezes one group's raw rate on both sides of a
// provider monetary-unit change. It is carried into ApplyCostUnitRebase and is
// persisted through the immutable audit summary plus ordinary rate-change
// rows; it is never accepted from an HTTP client.
type CostUnitRateTransitionItem struct {
	ModelName             string
	DescriptionBefore     string
	DescriptionAfter      string
	RatioBefore           float64
	RatioAfter            float64
	CompletionRatioBefore float64
	CompletionRatioAfter  float64
}

func NewCosting(db *gorm.DB) *Costing { return &Costing{db: db} }

// CostUnitRateEvidenceDigest returns a stable digest for the complete old/new
// raw-rate transition. Callers must pass every current group exactly once.
func CostUnitRateEvidenceDigest(items []CostUnitRateTransitionItem) (string, error) {
	if len(items) == 0 {
		return "", ErrCostUnitRebaseInvalid
	}
	ordered := append([]CostUnitRateTransitionItem(nil), items...)
	sort.Slice(ordered, func(i, j int) bool {
		return strings.ToLower(strings.TrimSpace(ordered[i].ModelName)) < strings.ToLower(strings.TrimSpace(ordered[j].ModelName))
	})
	seen := make(map[string]struct{}, len(ordered))
	var payload strings.Builder
	for _, item := range ordered {
		name := strings.ToLower(strings.TrimSpace(item.ModelName))
		if name == "" {
			return "", ErrCostUnitRebaseInvalid
		}
		if _, exists := seen[name]; exists {
			return "", ErrCostUnitRebaseInvalid
		}
		seen[name] = struct{}{}
		for _, value := range []float64{item.RatioBefore, item.RatioAfter, item.CompletionRatioBefore, item.CompletionRatioAfter} {
			if !finiteNonNegative(value) {
				return "", ErrCostUnitRebaseInvalid
			}
		}
		fmt.Fprintf(&payload, "%s\x00%.17g\x00%.17g\x00%.17g\x00%.17g\x00", name,
			item.RatioBefore, item.RatioAfter, item.CompletionRatioBefore, item.CompletionRatioAfter)
	}
	digest := sha256.Sum256([]byte(payload.String()))
	return hex.EncodeToString(digest[:]), nil
}

func (r *Costing) FindState(channelID uint) (*ChannelCostState, error) {
	var state ChannelCostState
	if err := r.db.First(&state, "channel_id = ?", channelID).Error; err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *Costing) ListStates() ([]ChannelCostState, error) {
	var states []ChannelCostState
	if err := r.db.Order("channel_id ASC").Find(&states).Error; err != nil {
		return nil, err
	}
	return states, nil
}

func (r *Costing) FindCostUnitRebase(channelID uint, idempotencyKey string) (*ChannelCostUnitRebase, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if channelID == 0 || idempotencyKey == "" {
		return nil, errors.New("channel and idempotency key are required")
	}
	var rebase ChannelCostUnitRebase
	if err := r.db.Table("channel_cost_unit_rebases AS rebase").Select("rebase.*").
		Joins("JOIN channels ON channels.id = rebase.channel_id AND channels.upstream_account_identity_revision = rebase.channel_identity_revision").
		Joins("JOIN gateway_channel_upstream_account_bindings AS binding ON binding.channel_id = channels.id AND binding.channel_identity_revision = channels.upstream_account_identity_revision AND binding.provider = channels.type").
		Joins("JOIN gateway_upstream_accounts AS account ON account.id = binding.upstream_account_id AND account.provider = binding.provider AND account.external_account_id_hash = rebase.account_identity_fingerprint").
		Where("rebase.channel_id = ? AND rebase.idempotency_key = ?", channelID, idempotencyKey).
		Order("rebase.id DESC").Take(&rebase).Error; err != nil {
		return nil, err
	}
	return &rebase, nil
}

// MarkCostUnitReview pauses consumers of the channel's price basis without
// changing any monetary anchor or the channel-level connectivity projection.
//
// The account-identity fence is resolved here rather than demanded from the
// caller. Only providers that authenticate an account expose that evidence, so
// a provider without it is fenced on the channel identity revision alone. The
// alternative — refusing the mark — is strictly worse than a slightly weaker
// fence: it would leave an untrustworthy price basis live on the routing path
// for every channel type that cannot produce an account binding.
func (r *Costing) MarkCostUnitReview(channelID uint, expectedVersion uint64, message string) error {
	if r == nil || r.db == nil || channelID == 0 {
		return errors.New("cost unit review state is invalid")
	}
	var channel Channel
	if err := r.db.Select("id", "type", "upstream_account_identity_revision").First(&channel, channelID).Error; err != nil {
		return err
	}
	if !providerExposesUpstreamAccountIdentity(channel.Type) {
		return r.markCostUnitReview(channelID, expectedVersion, channel.UpstreamAccountIdentityRevision, 0, message)
	}
	identity, err := loadCurrentUpstreamAccountIdentity(r.db, channelID, false)
	if err != nil {
		return err
	}
	return r.MarkCostUnitReviewForIdentity(channelID, expectedVersion, identity.ChannelIdentityRevision, identity.AccountRecordID, message)
}

// MarkCostUnitReviewForIdentity is the strict variant used by collection paths
// that already froze a provider-authenticated account before sampling. It
// always requires the full account fence.
func (r *Costing) MarkCostUnitReviewForIdentity(channelID uint, expectedVersion, expectedIdentityRevision, expectedAccountRecordID uint64, message string) error {
	if expectedAccountRecordID == 0 {
		return errors.New("cost unit review state is invalid")
	}
	return r.markCostUnitReview(channelID, expectedVersion, expectedIdentityRevision, expectedAccountRecordID, message)
}

// markCostUnitReview applies the review pause. expectedAccountRecordID may be
// zero only for a provider that cannot produce account identity evidence at
// all; the provider is re-checked under the channel row lock so this can never
// be used to bypass the fence on a Sub2API channel.
func (r *Costing) markCostUnitReview(channelID uint, expectedVersion, expectedIdentityRevision, expectedAccountRecordID uint64, message string) error {
	message = strings.TrimSpace(message)
	if r == nil || r.db == nil || channelID == 0 || expectedVersion == 0 || expectedVersion == math.MaxUint64 ||
		expectedIdentityRevision == 0 || message == "" || len(message) > 2048 {
		return errors.New("cost unit review state is invalid")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		channel, current, err := lockChannelAndCostStateTx(tx, channelID)
		if err != nil {
			return err
		}
		if current.Version != expectedVersion || channel.UpstreamAccountIdentityRevision != expectedIdentityRevision {
			return ErrCostStateConflict
		}
		if providerExposesUpstreamAccountIdentity(channel.Type) {
			if expectedAccountRecordID == 0 {
				return ErrStaleUpstreamAccountObservation
			}
			identity, err := loadCurrentUpstreamAccountIdentity(tx, channelID, true)
			if err != nil {
				return err
			}
			if identity.ChannelIdentityRevision != expectedIdentityRevision || identity.AccountRecordID != expectedAccountRecordID {
				return ErrStaleUpstreamAccountObservation
			}
		} else if expectedAccountRecordID != 0 {
			return ErrStaleUpstreamAccountObservation
		}
		result := tx.Model(&ChannelCostState{}).
			Where("channel_id = ? AND version = ? AND cost_unit_epoch = ?", channelID, expectedVersion, current.CostUnitEpoch).
			Updates(map[string]any{
				"status": "needs_review", "last_error": message,
				"version": expectedVersion + 1,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrCostStateConflict
		}
		return nil
	})
}

// ApplyCostUnitRebase atomically advances the monetary state, updates the
// channel projection, appends post-rebase snapshots, and records immutable
// audit evidence. On an idempotent replay audit is replaced with the first
// committed row and no state is changed.
func (r *Costing) ApplyCostUnitRebase(next *ChannelCostState, audit *ChannelCostUnitRebase, expectedVersion uint64) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("costing store is unavailable")
	}
	if next == nil || audit == nil || next.ChannelID == 0 || audit.ChannelID != next.ChannelID {
		return false, errors.New("matching cost state and unit rebase evidence are required")
	}
	identity, err := loadCurrentUpstreamAccountIdentity(r.db, next.ChannelID, false)
	if err != nil {
		return false, err
	}
	return r.ApplyCostUnitRebaseForIdentity(next, audit, expectedVersion, identity.ChannelIdentityRevision, identity.AccountRecordID)
}

func (r *Costing) ApplyCostUnitRebaseForIdentity(next *ChannelCostState, audit *ChannelCostUnitRebase, expectedVersion, expectedIdentityRevision, expectedAccountRecordID uint64) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("costing store is unavailable")
	}
	if next == nil || audit == nil || next.ChannelID == 0 || audit.ChannelID != next.ChannelID {
		return false, errors.New("matching cost state and unit rebase evidence are required")
	}
	if expectedVersion == math.MaxUint64 || expectedIdentityRevision == 0 || expectedAccountRecordID == 0 {
		return false, errors.New("channel cost state version is exhausted")
	}
	audit.IdempotencyKey = strings.TrimSpace(audit.IdempotencyKey)
	audit.RequestFingerprint = strings.TrimSpace(audit.RequestFingerprint)
	audit.Actor = strings.TrimSpace(audit.Actor)
	audit.PreviousMultiplierMode = strings.TrimSpace(audit.PreviousMultiplierMode)
	audit.CurrentMultiplierMode = strings.TrimSpace(audit.CurrentMultiplierMode)
	audit.RateEvidenceDigest = strings.TrimSpace(audit.RateEvidenceDigest)
	audit.CounterTransition = strings.TrimSpace(audit.CounterTransition)
	if audit.IdempotencyKey == "" || len(audit.IdempotencyKey) > 128 || !isLowerHexFingerprint(audit.RequestFingerprint) || audit.Actor == "" || len(audit.Actor) > 128 {
		return false, ErrCostUnitRebaseInvalid
	}

	var committedState ChannelCostState
	var committedAudit ChannelCostUnitRebase
	idempotentReplay := false
	err := r.db.Transaction(func(tx *gorm.DB) error {
		channel, current, err := lockChannelAndCostStateTx(tx, next.ChannelID)
		if err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != expectedIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		identity, err := loadCurrentUpstreamAccountIdentity(tx, next.ChannelID, true)
		if err != nil {
			return err
		}
		if identity.ChannelIdentityRevision != expectedIdentityRevision || identity.AccountRecordID != expectedAccountRecordID {
			return ErrStaleUpstreamAccountObservation
		}

		var existing ChannelCostUnitRebase
		err = tx.Where("channel_id = ? AND channel_identity_revision = ? AND idempotency_key = ?", audit.ChannelID, expectedIdentityRevision, audit.IdempotencyKey).First(&existing).Error
		if err == nil {
			if existing.RequestFingerprint != audit.RequestFingerprint || existing.Actor != audit.Actor {
				return ErrCostUnitRebaseIdempotencyConflict
			}
			if err := verifyCostUnitRebaseRateItemsTx(tx, &existing); err != nil {
				return err
			}
			committedAudit = existing
			idempotentReplay = true
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if current.Version != expectedVersion {
			return ErrCostStateConflict
		}

		var pending int64
		if err := tx.Model(&ChannelFundingCandidate{}).
			Where("channel_id = ? AND status = ?", next.ChannelID, FundingCandidatePending).
			Count(&pending).Error; err != nil {
			return err
		}
		if pending > 0 {
			return ErrPendingFundingCandidate
		}

		nextCopy := *next
		auditCopy := *audit
		populateCostUnitRebaseIdentityEvidence(&auditCopy, channel, identity)
		populateCostUnitRebaseEvidence(&auditCopy, current, &nextCopy, expectedVersion)
		if err := validateCostUnitRebase(current, &nextCopy, &auditCopy); err != nil {
			return err
		}
		if err := tx.Create(&auditCopy).Error; err != nil {
			return fmt.Errorf("append channel cost unit rebase: %w", err)
		}
		if err := appendCostUnitRebaseRateItemsTx(tx, &auditCopy); err != nil {
			return err
		}
		if err := applyCostUnitRateTransitionTx(tx, &auditCopy); err != nil {
			return err
		}
		watermark := tx.Model(&Channel{}).
			Where("id = ? AND cost_unit_epoch = ?", nextCopy.ChannelID, auditCopy.ExpectedCostUnitEpoch).
			Update("rate_snapshot_observed_at", auditCopy.RateTransitionAt)
		if watermark.Error != nil {
			return watermark.Error
		}
		if watermark.RowsAffected != 1 {
			return ErrCostUnitEpochConflict
		}
		if err := updateCostStateAndProjection(tx, &nextCopy, expectedVersion); err != nil {
			return err
		}
		if err := clearChannelSyncFailureTx(tx, nextCopy.ChannelID); err != nil {
			return err
		}
		if err := appendCostStateSnapshotsTx(tx, &nextCopy); err != nil {
			return err
		}
		if err := applyCostUnitRequestRestatementTx(tx, &auditCopy, auditCopy.RequestRestatement); err != nil {
			return err
		}
		committedState = nextCopy
		committedAudit = auditCopy
		return nil
	})
	if err != nil {
		return false, err
	}
	*audit = committedAudit
	if !idempotentReplay {
		*next = committedState
	}
	return idempotentReplay, nil
}

func clearChannelSyncFailureTx(tx *gorm.DB, channelID uint) error {
	if tx == nil || channelID == 0 {
		return ErrCostStateProjectionMissing
	}
	result := tx.Model(&Channel{}).Where("id = ?", channelID).Updates(map[string]any{
		"last_error":         "",
		"sync_failure_count": 0,
		"next_sync_retry_at": nil,
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrCostStateProjectionMissing
	}
	return nil
}

// UpdateState applies a precomputed state with an optimistic version check.
func (r *Costing) UpdateState(next *ChannelCostState, expectedVersion uint64) error {
	if next == nil || next.ChannelID == 0 {
		return errors.New("channel cost state is required")
	}
	if expectedVersion == math.MaxUint64 {
		return errors.New("channel cost state version is exhausted")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		current, err := lockChannelCostStateTx(tx, next.ChannelID)
		if err != nil {
			return err
		}
		if err := validateOrdinaryCostStateTransition(current, next, expectedVersion); err != nil {
			return err
		}
		if err := updateCostStateAndProjection(tx, next, expectedVersion); err != nil {
			return err
		}
		return appendChangedCostStateSnapshotsTx(tx, current, next)
	})
}

// UpdateStateForChannelIdentity applies an observation while fencing both the
// cost-state version and the channel account identity. When clearBalanceFact
// is true the same transaction also clears a previously observed account
// balance-insufficient fact.
func (r *Costing) UpdateStateForChannelIdentity(next *ChannelCostState, expectedVersion, expectedIdentityRevision uint64, clearBalanceFact bool) error {
	if next == nil || next.ChannelID == 0 || expectedIdentityRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if expectedVersion == math.MaxUint64 {
		return errors.New("channel cost state version is exhausted")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		current, err := lockChannelCostStateTx(tx, next.ChannelID)
		if err != nil {
			return err
		}
		if err := validateOrdinaryCostStateTransition(current, next, expectedVersion); err != nil {
			return err
		}
		if err := updateCostStateAndProjectionForChannelIdentity(tx, next, expectedVersion, expectedIdentityRevision, clearBalanceFact); err != nil {
			return err
		}
		return appendChangedCostStateSnapshotsTx(tx, current, next)
	})
}

// UpdateBalanceState atomically decides whether a pending funding candidate
// requires the current conversion basis to remain frozen.
func (r *Costing) UpdateBalanceState(next *ChannelCostState, expectedVersion uint64) error {
	if next == nil || next.ChannelID == 0 {
		return errors.New("channel cost state is required")
	}
	if expectedVersion == math.MaxUint64 {
		return errors.New("channel cost state version is exhausted")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		return updateBalanceStateAndProjectionForChannelIdentity(tx, next, expectedVersion, 0)
	})
}

func (r *Costing) UpdateBalanceStateForChannelIdentity(next *ChannelCostState, expectedVersion, expectedIdentityRevision uint64) error {
	if next == nil || next.ChannelID == 0 || expectedIdentityRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if expectedVersion == math.MaxUint64 {
		return errors.New("channel cost state version is exhausted")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		return updateBalanceStateAndProjectionForChannelIdentity(tx, next, expectedVersion, expectedIdentityRevision)
	})
}

func (r *Costing) UpdateStateWithFundingCandidate(next *ChannelCostState, candidate *ChannelFundingCandidate, expectedVersion uint64) error {
	if next == nil || candidate == nil || next.ChannelID == 0 || candidate.ChannelID != next.ChannelID {
		return errors.New("matching cost state and funding candidate are required")
	}
	if err := validateFundingCandidateBalanceBoundary(candidate); err != nil {
		return err
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		current, err := lockChannelCostStateTx(tx, next.ChannelID)
		if err != nil {
			return err
		}
		if err := validateOrdinaryCostStateTransition(current, next, expectedVersion); err != nil {
			return err
		}
		if candidate.DetectedAt.IsZero() {
			candidate.DetectedAt = time.Now()
		}
		if strings.TrimSpace(candidate.Status) == "" {
			candidate.Status = FundingCandidatePending
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(candidate).Error; err != nil {
			return err
		}
		var stored ChannelFundingCandidate
		if err := tx.Where("channel_id = ? AND source = ? AND external_id = ?", candidate.ChannelID, candidate.Source, candidate.ExternalID).First(&stored).Error; err != nil {
			return err
		}
		if _, err := supersedeFundingCandidateIfNeeded(tx, &stored); err != nil {
			return err
		}
		if stored.Status == FundingCandidatePending && math.Abs(stored.FundingFactorBefore-current.FundingFactor) > fundingEvidenceTolerance {
			return ErrFundingCandidateCostBasis
		}
		if err := updateCostStateAndProjection(tx, next, expectedVersion); err != nil {
			return err
		}
		if err := appendChangedCostStateSnapshotsTx(tx, current, next); err != nil {
			return err
		}
		*candidate = stored
		return nil
	})
}

func (r *Costing) UpdateStateWithFundingCandidateForChannelIdentity(next *ChannelCostState, candidate *ChannelFundingCandidate, expectedVersion, expectedIdentityRevision uint64) error {
	if next == nil || candidate == nil || next.ChannelID == 0 || candidate.ChannelID != next.ChannelID || expectedIdentityRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	if err := validateFundingCandidateBalanceBoundary(candidate); err != nil {
		return err
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		current, err := lockChannelCostStateTx(tx, next.ChannelID)
		if err != nil {
			return err
		}
		if err := validateOrdinaryCostStateTransition(current, next, expectedVersion); err != nil {
			return err
		}
		if err := updateCostStateAndProjectionForChannelIdentity(tx, next, expectedVersion, expectedIdentityRevision, true); err != nil {
			return err
		}
		if candidate.DetectedAt.IsZero() {
			candidate.DetectedAt = time.Now()
		}
		if strings.TrimSpace(candidate.Status) == "" {
			candidate.Status = FundingCandidatePending
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(candidate).Error; err != nil {
			return err
		}
		var stored ChannelFundingCandidate
		if err := tx.Where("channel_id = ? AND source = ? AND external_id = ?", candidate.ChannelID, candidate.Source, candidate.ExternalID).First(&stored).Error; err != nil {
			return err
		}
		if _, err := supersedeFundingCandidateIfNeeded(tx, &stored); err != nil {
			return err
		}
		if err := appendChangedCostStateSnapshotsTx(tx, current, next); err != nil {
			return err
		}
		*candidate = stored
		return nil
	})
}

// AppendFundingEvent atomically advances the state and appends its audit row.
// There is intentionally no event update/delete method: corrections are new
// ledger events, never mutations of history.
func (r *Costing) AppendFundingEvent(next *ChannelCostState, event *ChannelFundingEvent, expectedVersion uint64) error {
	if next == nil || event == nil || next.ChannelID == 0 || event.ChannelID != next.ChannelID {
		return errors.New("matching channel cost state and funding event are required")
	}
	if expectedVersion == math.MaxUint64 {
		return errors.New("channel cost state version is exhausted")
	}
	if event.EffectiveAt.IsZero() {
		event.EffectiveAt = time.Now()
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		current, err := lockChannelCostStateTx(tx, next.ChannelID)
		if err != nil {
			return err
		}
		if err := validateOrdinaryCostStateTransition(current, next, expectedVersion); err != nil {
			return err
		}
		var pending int64
		if err := tx.Model(&ChannelFundingCandidate{}).
			Where("channel_id = ? AND status = ?", next.ChannelID, FundingCandidatePending).
			Count(&pending).Error; err != nil {
			return err
		}
		if pending > 0 {
			return ErrPendingFundingCandidate
		}
		if err := updateCostStateAndProjection(tx, next, expectedVersion); err != nil {
			return err
		}
		if err := appendFundingFactorRateChangesTx(tx, current, next, time.Now().UTC()); err != nil {
			return err
		}
		intervalEnd := time.Now()
		if err := tx.Create(event).Error; err != nil {
			return fmt.Errorf("append channel funding event: %w", err)
		}
		if event.EffectiveAt.Before(intervalEnd) && event.FundingFactorAfter != event.FundingFactorBefore {
			if err := applyGatewayRequestFundingCorrectionTx(tx, next.ChannelID, event.ID, event.EffectiveAt, nil,
				event.FundingFactorAfter, next.Version, next.Source); err != nil {
				return err
			}
			if err := tx.Model(&BalanceSnapshot{}).Where("channel_id = ? AND cost_unit_epoch = ? AND sampled_at >= ? AND sampled_at < ?", next.ChannelID, next.CostUnitEpoch, event.EffectiveAt, intervalEnd).Updates(map[string]any{
				"balance":        gorm.Expr("CASE WHEN raw_balance IS NOT NULL THEN raw_balance * ? ELSE balance END", event.FundingFactorAfter),
				"funding_factor": event.FundingFactorAfter, "cost_basis_version": next.Version,
			}).Error; err != nil {
				return err
			}
			if err := tx.Model(&CostSnapshot{}).Where("channel_id = ? AND cost_unit_epoch = ? AND sampled_at >= ? AND sampled_at < ?", next.ChannelID, next.CostUnitEpoch, event.EffectiveAt, intervalEnd).Updates(map[string]any{
				"today_cost":     gorm.Expr("CASE WHEN raw_today_cost IS NULL THEN today_cost WHEN raw_today_cost >= ? THEN today_cost + (raw_today_cost - ?) * ? ELSE raw_today_cost * ? END", event.RawTodayCostBefore, event.RawTodayCostBefore, event.FundingFactorAfter-event.FundingFactorBefore, event.FundingFactorAfter),
				"total_cost":     gorm.Expr("CASE WHEN raw_total_cost IS NULL THEN total_cost WHEN raw_total_cost >= ? THEN total_cost + (raw_total_cost - ?) * ? ELSE total_cost END", event.RawTotalCostBefore, event.RawTotalCostBefore, event.FundingFactorAfter-event.FundingFactorBefore),
				"funding_factor": event.FundingFactorAfter, "cost_basis_version": next.Version,
			}).Error; err != nil {
				return err
			}
		}
		return appendChangedCostStateSnapshotsTx(tx, current, next)
	})
}

func (r *Costing) ListFundingEvents(channelID uint, limit, offset int) ([]ChannelFundingEvent, int64, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	query := r.db.Model(&ChannelFundingEvent{}).Where("channel_id = ?", channelID)
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var events []ChannelFundingEvent
	if err := query.Order("effective_at DESC").Order("id DESC").Limit(limit).Offset(offset).Find(&events).Error; err != nil {
		return nil, 0, err
	}
	return events, total, nil
}

func (r *Costing) UpsertFundingCandidate(candidate *ChannelFundingCandidate) (*ChannelFundingCandidate, error) {
	if candidate == nil || candidate.ChannelID == 0 || strings.TrimSpace(candidate.Source) == "" || strings.TrimSpace(candidate.ExternalID) == "" {
		return nil, errors.New("channel, source and external id are required")
	}
	if err := validateFundingCandidateBalanceBoundary(candidate); err != nil {
		return nil, err
	}
	if candidate.DetectedAt.IsZero() {
		candidate.DetectedAt = time.Now()
	}
	if strings.TrimSpace(candidate.Status) == "" {
		candidate.Status = FundingCandidatePending
	}
	var stored ChannelFundingCandidate
	if err := r.db.Transaction(func(tx *gorm.DB) error {
		if err := lockChannelCostProjection(tx, candidate.ChannelID, 0); err != nil {
			return err
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(candidate).Error; err != nil {
			return err
		}
		if err := tx.Where("channel_id = ? AND source = ? AND external_id = ?", candidate.ChannelID, candidate.Source, candidate.ExternalID).First(&stored).Error; err != nil {
			return err
		}
		_, err := supersedeFundingCandidateIfNeeded(tx, &stored)
		return err
	}); err != nil {
		return nil, err
	}
	return &stored, nil
}

// SupersededFundingCandidateReconcileBatch describes one bounded keyset page.
// HighWaterID freezes the candidate set at the first page so concurrent inserts
// cannot keep startup reconciliation running indefinitely.
type SupersededFundingCandidateReconcileBatch struct {
	HighWaterID uint64
	NextID      uint64
	Scanned     int
	Resolved    int64
	Exhausted   bool
}

// ReconcileSupersededFundingCandidates preserves the legacy resolved-row cap
// while scanning historical pending rows in bounded keyset transactions.
func (r *Costing) ReconcileSupersededFundingCandidates(limit int) (int64, error) {
	return r.ReconcileSupersededFundingCandidatesContext(context.Background(), limit)
}

// ReconcileSupersededFundingCandidatesContext preserves the legacy resolved-row
// limit across bounded keyset pages. Startup cleanup consumes each batch result
// directly so it can pause between transactions and report scan exhaustion.
func (r *Costing) ReconcileSupersededFundingCandidatesContext(ctx context.Context, limit int) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var total int64
	var afterID, highWaterID uint64
	for total < int64(limit) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		remaining := limit - int(total)
		batch, err := r.ReconcileSupersededFundingCandidatesBatchContext(ctx, afterID, highWaterID, remaining)
		if err != nil {
			return total, err
		}
		total += batch.Resolved
		if batch.Exhausted {
			return total, nil
		}
		if batch.NextID <= afterID {
			return total, errors.New("superseded funding candidate reconciliation made no cursor progress")
		}
		afterID = batch.NextID
		highWaterID = batch.HighWaterID
	}
	return total, nil
}

// ReconcileSupersededFundingCandidatesBatchContext scans and locks at most one
// keyset page. Pass zero highWaterID only for the first page; subsequent calls
// must preserve the returned high-water and advance afterID to NextID.
func (r *Costing) ReconcileSupersededFundingCandidatesBatchContext(ctx context.Context, afterID, highWaterID uint64, limit int) (SupersededFundingCandidateReconcileBatch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SupersededFundingCandidateReconcileBatch{}, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if highWaterID == 0 && afterID != 0 {
		return SupersededFundingCandidateReconcileBatch{}, errors.New("superseded funding candidate high-water is required after the first page")
	}
	result := SupersededFundingCandidateReconcileBatch{HighWaterID: highWaterID, NextID: afterID}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if result.HighWaterID == 0 {
			if err := tx.Model(&ChannelFundingCandidate{}).
				Where("status = ?", FundingCandidatePending).
				Select("COALESCE(MAX(id), 0)").
				Scan(&result.HighWaterID).Error; err != nil {
				return err
			}
		}
		if result.HighWaterID == 0 || afterID >= result.HighWaterID {
			result.Exhausted = true
			return ctx.Err()
		}

		var candidates []ChannelFundingCandidate
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("status = ? AND id > ? AND id <= ?", FundingCandidatePending, afterID, result.HighWaterID).
			Where(`EXISTS (
				SELECT 1 FROM channel_funding_events
				WHERE channel_funding_events.channel_id = channel_funding_candidates.channel_id
				AND channel_funding_events.effective_at >= COALESCE(channel_funding_candidates.occurred_at, channel_funding_candidates.detected_at)
				AND ABS(channel_funding_events.raw_balance_after - channel_funding_candidates.raw_balance_detected) <= ?
			)`, fundingEvidenceTolerance).
			Order("id ASC").Limit(limit).Find(&candidates).Error; err != nil {
			return err
		}
		result.Scanned = len(candidates)
		if len(candidates) == 0 {
			result.Exhausted = true
			return ctx.Err()
		}
		result.NextID = candidates[len(candidates)-1].ID
		for i := range candidates {
			if err := ctx.Err(); err != nil {
				return err
			}
			changed, err := supersedeFundingCandidateIfNeeded(tx, &candidates[i])
			if err != nil {
				return err
			}
			if changed {
				result.Resolved++
			}
		}
		result.Exhausted = len(candidates) < limit || result.NextID >= result.HighWaterID
		return ctx.Err()
	})
	if err != nil {
		return SupersededFundingCandidateReconcileBatch{}, err
	}
	return result, nil
}

func (r *Costing) FindFundingCandidate(channelID uint, candidateID uint64) (*ChannelFundingCandidate, error) {
	var candidate ChannelFundingCandidate
	if err := r.db.Where("id = ? AND channel_id = ?", candidateID, channelID).First(&candidate).Error; err != nil {
		return nil, err
	}
	return &candidate, nil
}

func (r *Costing) ListFundingCandidates(channelID uint, status string, limit, offset int) ([]ChannelFundingCandidate, int64, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	query := r.db.Model(&ChannelFundingCandidate{}).Where("channel_id = ?", channelID)
	if status = strings.TrimSpace(status); status != "" {
		query = query.Where("status = ?", status)
	}
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var items []ChannelFundingCandidate
	if status == FundingCandidatePending {
		// Confirmation is oldest-first by event time. Apply the same order before
		// pagination so the actionable candidate is never hidden on a later page.
		query = query.Order("COALESCE(occurred_at, detected_at) ASC").Order("id ASC")
	} else {
		query = query.Order("detected_at DESC").Order("id DESC")
	}
	if err := query.Limit(limit).Offset(offset).Find(&items).Error; err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (r *Costing) PendingFundingCounts() (map[uint]int64, error) {
	type row struct {
		ChannelID uint
		Count     int64
	}
	var rows []row
	if err := r.db.Model(&ChannelFundingCandidate{}).Select("channel_id, COUNT(*) AS count").Where("status = ?", FundingCandidatePending).Group("channel_id").Scan(&rows).Error; err != nil {
		return nil, err
	}
	counts := make(map[uint]int64, len(rows))
	for _, item := range rows {
		counts[item.ChannelID] = item.Count
	}
	return counts, nil
}

// ConfirmFundingCandidate atomically advances the cost state, appends the
// immutable ledger event, and resolves the pending reminder.
func (r *Costing) ConfirmFundingCandidate(next *ChannelCostState, event *ChannelFundingEvent, candidateID uint64, expectedVersion uint64) error {
	if next == nil || event == nil || candidateID == 0 || next.ChannelID == 0 || event.ChannelID != next.ChannelID {
		return errors.New("matching cost state, event and candidate are required")
	}
	superseded := false
	err := r.db.Transaction(func(tx *gorm.DB) error {
		currentState, err := lockChannelCostStateTx(tx, next.ChannelID)
		if err != nil {
			return err
		}
		if err := validateOrdinaryCostStateTransition(currentState, next, expectedVersion); err != nil {
			return err
		}
		var candidate ChannelFundingCandidate
		if err := tx.Where("id = ? AND channel_id = ? AND status = ?", candidateID, next.ChannelID, FundingCandidatePending).First(&candidate).Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if err := tx.Where("id = ? AND channel_id = ?", candidateID, next.ChannelID).First(&candidate).Error; err != nil {
				return err
			}
			if candidate.Status == FundingCandidateSuperseded {
				superseded = true
				return nil
			}
			return ErrFundingCandidateResolved
		}
		changed, err := supersedeFundingCandidateIfNeeded(tx, &candidate)
		if err != nil {
			return err
		}
		if changed {
			superseded = true
			return nil
		}
		candidateTime := candidate.DetectedAt
		if candidate.OccurredAt != nil && !candidate.OccurredAt.IsZero() {
			candidateTime = *candidate.OccurredAt
		}
		var conflictingLaterEvents int64
		if err := tx.Model(&ChannelFundingEvent{}).
			Where("channel_id = ?", next.ChannelID).
			Where(`
					effective_at > ? OR (
						effective_at = ? AND NOT EXISTS (
							SELECT 1
							FROM channel_funding_candidates AS earlier_candidate
							WHERE earlier_candidate.channel_id = channel_funding_events.channel_id
							  AND earlier_candidate.resolved_event_id = channel_funding_events.id
							  AND earlier_candidate.status = ?
							  AND COALESCE(earlier_candidate.occurred_at, earlier_candidate.detected_at) = ?
							  AND earlier_candidate.id < ?
						)
					)
				`, candidateTime, candidateTime, FundingCandidateConfirmed, candidateTime, candidate.ID).
			Count(&conflictingLaterEvents).Error; err != nil {
			return err
		}
		if conflictingLaterEvents > 0 {
			return ErrFundingCandidateCostBasis
		}
		var previousEvent ChannelFundingEvent
		if err := tx.Where("channel_id = ?", next.ChannelID).
			Where(`
					effective_at < ? OR (
						effective_at = ? AND EXISTS (
							SELECT 1
							FROM channel_funding_candidates AS earlier_candidate
							WHERE earlier_candidate.channel_id = channel_funding_events.channel_id
							  AND earlier_candidate.resolved_event_id = channel_funding_events.id
							  AND earlier_candidate.status = ?
							  AND COALESCE(earlier_candidate.occurred_at, earlier_candidate.detected_at) = ?
							  AND earlier_candidate.id < ?
						)
					)
				`, candidateTime, candidateTime, FundingCandidateConfirmed, candidateTime, candidate.ID).
			Order("effective_at DESC").Order("id DESC").First(&previousEvent).Error; err == nil {
			if math.Abs(previousEvent.FundingFactorAfter-candidate.FundingFactorBefore) > fundingEvidenceTolerance {
				return ErrFundingCandidateCostBasis
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var oldest ChannelFundingCandidate
		if err := tx.Where("channel_id = ? AND status = ?", next.ChannelID, FundingCandidatePending).Order("COALESCE(occurred_at, detected_at) ASC").Order("id ASC").First(&oldest).Error; err != nil {
			return err
		}
		if oldest.ID != candidateID {
			return ErrFundingCandidateOrder
		}
		if err := updateCostStateAndProjection(tx, next, expectedVersion); err != nil {
			return err
		}
		if err := appendFundingFactorRateChangesTx(tx, currentState, next, time.Now().UTC()); err != nil {
			return err
		}
		if event.EffectiveAt.IsZero() {
			event.EffectiveAt = time.Now()
		}
		intervalEnd := time.Now()
		var correctionIntervalEnd *time.Time
		var later ChannelFundingCandidate
		laterErr := tx.Where("channel_id = ? AND status = ? AND (COALESCE(occurred_at, detected_at) > ? OR (COALESCE(occurred_at, detected_at) = ? AND id > ?))", next.ChannelID, FundingCandidatePending, candidateTime, candidateTime, candidate.ID).
			Order("COALESCE(occurred_at, detected_at) ASC").Order("id ASC").First(&later).Error
		if laterErr == nil {
			intervalEnd = later.DetectedAt
			if later.OccurredAt != nil && !later.OccurredAt.IsZero() {
				intervalEnd = *later.OccurredAt
			}
			correctionEnd := intervalEnd.UTC()
			correctionIntervalEnd = &correctionEnd
		} else if !errors.Is(laterErr, gorm.ErrRecordNotFound) {
			return laterErr
		}
		if err := tx.Create(event).Error; err != nil {
			return fmt.Errorf("append candidate funding event: %w", err)
		}
		if event.EffectiveAt.After(intervalEnd) {
			return ErrCostUnitRequestRestatementConflict
		}
		if err := applyGatewayRequestFundingCorrectionTx(tx, next.ChannelID, event.ID, event.EffectiveAt, correctionIntervalEnd,
			next.FundingFactor, next.Version, next.Source); err != nil {
			return err
		}
		if err := tx.Model(&BalanceSnapshot{}).
			Where("channel_id = ? AND cost_unit_epoch = ? AND sampled_at >= ? AND sampled_at < ?", next.ChannelID, next.CostUnitEpoch, event.EffectiveAt, intervalEnd).
			Updates(map[string]any{
				"balance":            gorm.Expr("CASE WHEN raw_balance IS NOT NULL THEN raw_balance * ? ELSE balance END", next.FundingFactor),
				"funding_factor":     next.FundingFactor,
				"cost_basis_version": next.Version,
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&CostSnapshot{}).
			Where("channel_id = ? AND cost_unit_epoch = ? AND sampled_at >= ? AND sampled_at < ?", next.ChannelID, next.CostUnitEpoch, event.EffectiveAt, intervalEnd).
			Updates(map[string]any{
				"today_cost":         gorm.Expr("CASE WHEN raw_today_cost IS NULL THEN today_cost WHEN raw_today_cost >= ? THEN today_cost + (raw_today_cost - ?) * ? ELSE raw_today_cost * ? END", candidate.RawTodayCostBefore, candidate.RawTodayCostBefore, next.FundingFactor-candidate.FundingFactorBefore, next.FundingFactor),
				"total_cost":         gorm.Expr("CASE WHEN raw_total_cost IS NULL THEN total_cost WHEN raw_total_cost >= ? THEN total_cost + (raw_total_cost - ?) * ? ELSE total_cost END", candidate.RawTotalCostBefore, candidate.RawTotalCostBefore, next.FundingFactor-candidate.FundingFactorBefore),
				"funding_factor":     next.FundingFactor,
				"cost_basis_version": next.Version,
			}).Error; err != nil {
			return err
		}
		result := tx.Model(&ChannelFundingCandidate{}).
			Where("id = ? AND channel_id = ? AND status = ?", candidate.ID, next.ChannelID, FundingCandidatePending).
			Updates(map[string]any{
				"status": FundingCandidateConfirmed, "paid_amount": event.PaidAmount, "resolved_event_id": event.ID,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrFundingCandidateResolved
		}
		if err := tx.Model(&ChannelFundingCandidate{}).
			Where("channel_id = ? AND status = ? AND (COALESCE(occurred_at, detected_at) > ? OR (COALESCE(occurred_at, detected_at) = ? AND id > ?))", next.ChannelID, FundingCandidatePending, candidateTime, candidateTime, candidate.ID).
			Update("funding_factor_before", next.FundingFactor).Error; err != nil {
			return err
		}
		return appendChangedCostStateSnapshotsTx(tx, currentState, next)
	})
	if err != nil {
		return err
	}
	if superseded {
		return ErrFundingCandidateSuperseded
	}
	return nil
}

// applyGatewayRequestFundingCorrectionTx records the immutable correction
// interval before updating request rows that already exist. Late materializers
// query the interval while holding the same channel and cost-state locks.
func applyGatewayRequestFundingCorrectionTx(tx *gorm.DB, channelID uint, fundingEventID uint64, intervalStart time.Time, intervalEnd *time.Time, factor float64, version uint64, source string) error {
	if tx == nil || channelID == 0 || fundingEventID == 0 || intervalStart.IsZero() ||
		factor < 0 || math.IsNaN(factor) || math.IsInf(factor, 0) || version == 0 || strings.TrimSpace(source) == "" {
		return ErrCostUnitRequestRestatementConflict
	}
	intervalStart = intervalStart.UTC()
	var normalizedIntervalEnd *time.Time
	if intervalEnd != nil {
		if intervalEnd.IsZero() || intervalEnd.Before(intervalStart) {
			return ErrCostUnitRequestRestatementConflict
		}
		value := intervalEnd.UTC()
		if intervalStart.Equal(value) {
			return nil
		}
		normalizedIntervalEnd = &value
	}
	source = strings.TrimSpace(source)
	if len(source) > 32 {
		return ErrCostUnitRequestRestatementConflict
	}
	// Aggregate backfill owns this singleton before it locks request rows. Take
	// the same fence before the correction population so funding can never hold
	// a log or aggregate row while waiting for a backfill-owned log.
	if _, err := lockGatewayRequestGroupCacheBackfillStateTx(tx); err != nil {
		return err
	}
	correction := &GatewayRequestFundingCorrection{
		ChannelID: channelID, FundingEventID: fundingEventID,
		IntervalStart: intervalStart, IntervalEnd: normalizedIntervalEnd,
		FundingFactor: factor, CostBasisVersion: version, CostBasisSource: source,
	}
	if err := tx.Create(correction).Error; err != nil {
		return err
	}
	scope := func(db *gorm.DB) *gorm.DB {
		basisAtSQL := "COALESCE(funding_basis_at, (SELECT dispatched_at FROM gateway_request_dispatch_audits WHERE request_log_id = gateway_request_logs.id), created_at)"
		scoped := db.Where("channel_id = ? AND "+basisAtSQL+" >= ? AND cost_basis_version < ?", channelID, intervalStart, version)
		if normalizedIntervalEnd != nil {
			scoped = scoped.Where(basisAtSQL+" < ?", *normalizedIntervalEnd)
		}
		return scoped.Where("price_fact_id IS NULL").
			Where("NOT " + gatewayRequestCostReconcileBlockedByPricingFenceSQL(""))
	}
	type affectedGatewayRequestLog struct {
		ID        uint
		CreatedAt time.Time
	}
	var affected []affectedGatewayRequestLog
	// Backfill locks the mixed request population in ID order. Freeze the same
	// complete set before either category update so the two writers cannot each
	// hold one class of row while waiting on the other.
	if err := scope(tx.Model(&GatewayRequestLog{})).
		Where("("+gatewayPendingDispatchCostEvidenceSQL("")+" OR "+gatewayRequestFundingCorrectionEligibleSQL("")+")").
		Select("id", "created_at").Order("id ASC").
		Clauses(clause.Locking{Strength: "UPDATE"}).Find(&affected).Error; err != nil {
		return err
	}
	if err := scope(tx.Model(&GatewayRequestLog{})).
		Where(gatewayPendingDispatchCostEvidenceSQL("")).
		Updates(map[string]any{
			"deferred_funding_factor":     factor,
			"deferred_cost_basis_version": version,
			"deferred_cost_basis_source":  source,
		}).Error; err != nil {
		return err
	}
	if err := scope(tx.Model(&GatewayRequestLog{})).
		Where(gatewayRequestFundingCorrectionEligibleSQL("")).
		Updates(map[string]any{
			"cost":               gorm.Expr("CASE WHEN raw_cost IS NOT NULL THEN raw_cost * ? ELSE cost END", factor),
			"group_ratio":        gorm.Expr("CASE WHEN raw_group_ratio IS NOT NULL THEN raw_group_ratio * ? ELSE group_ratio END", factor),
			"completion_ratio":   gorm.Expr("CASE WHEN raw_completion_ratio IS NOT NULL THEN raw_completion_ratio * ? ELSE completion_ratio END", factor),
			"funding_factor":     factor,
			"cost_basis_version": version,
			"cost_basis_source":  source,
		}).Error; err != nil {
		return err
	}
	if len(affected) == 0 {
		return nil
	}
	firstCreatedAt := affected[0].CreatedAt.UTC()
	lastCreatedAt := firstCreatedAt
	for _, row := range affected[1:] {
		createdAt := row.CreatedAt.UTC()
		if createdAt.Before(firstCreatedAt) {
			firstCreatedAt = createdAt
		}
		if createdAt.After(lastCreatedAt) {
			lastCreatedAt = createdAt
		}
	}
	createdEnd := lastCreatedAt.Add(time.Nanosecond)
	if !createdEnd.After(lastCreatedAt) {
		return ErrCostUnitRequestRestatementConflict
	}
	return invalidateRequestAggregatesForRangeTx(tx, channelID, firstCreatedAt, createdEnd)
}

func supersedeFundingCandidateIfNeeded(tx *gorm.DB, candidate *ChannelFundingCandidate) (bool, error) {
	if candidate == nil || candidate.ID == 0 {
		return false, errors.New("funding candidate is required")
	}
	if candidate.Status == FundingCandidateSuperseded {
		return true, nil
	}
	if candidate.Status != FundingCandidatePending {
		return false, nil
	}
	candidateTime := candidate.DetectedAt
	if candidate.OccurredAt != nil && !candidate.OccurredAt.IsZero() {
		candidateTime = *candidate.OccurredAt
	}
	var events []ChannelFundingEvent
	if err := tx.Where("channel_id = ? AND effective_at >= ?", candidate.ChannelID, candidateTime).
		Order("effective_at ASC").Order("id ASC").Find(&events).Error; err != nil {
		return false, err
	}
	var event *ChannelFundingEvent
	for i := range events {
		if fundingCandidateMatchesEvent(candidate, &events[i]) {
			event = &events[i]
			break
		}
	}
	if event == nil {
		return false, nil
	}
	result := tx.Model(&ChannelFundingCandidate{}).
		Where("id = ? AND channel_id = ? AND status = ?", candidate.ID, candidate.ChannelID, FundingCandidatePending).
		Updates(map[string]any{"status": FundingCandidateSuperseded, "resolved_event_id": event.ID})
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected != 1 {
		var current ChannelFundingCandidate
		if err := tx.Where("id = ? AND channel_id = ?", candidate.ID, candidate.ChannelID).First(&current).Error; err != nil {
			return false, err
		}
		*candidate = current
		if current.Status == FundingCandidateSuperseded {
			return true, nil
		}
		return false, ErrFundingCandidateResolved
	}
	eventID := event.ID
	candidate.Status = FundingCandidateSuperseded
	candidate.ResolvedEventID = &eventID
	return true, nil
}

const fundingEvidenceTolerance = 1e-9
const fundingCoveredEventMaxDelay = 5 * time.Minute
const fundingCoveredBalanceDriftRatio = 0.001

func fundingCandidateMatchesEvent(candidate *ChannelFundingCandidate, event *ChannelFundingEvent) bool {
	if candidate == nil || event == nil || candidate.ChannelID == 0 || candidate.ChannelID != event.ChannelID {
		return false
	}
	candidateTime := candidate.DetectedAt
	if candidate.OccurredAt != nil && !candidate.OccurredAt.IsZero() {
		candidateTime = *candidate.OccurredAt
	}
	if event.EffectiveAt.Before(candidateTime) {
		return false
	}
	matches := func(a, b float64) bool { return math.Abs(a-b) <= fundingEvidenceTolerance }
	exactEvidence := matches(event.CreditedAmount, candidate.CreditedAmount) &&
		matches(event.RawBalanceBefore, candidate.RawBalanceBefore) &&
		matches(event.RawBalanceAfter, candidate.RawBalanceDetected) &&
		matches(event.FundingFactorBefore, candidate.FundingFactorBefore) &&
		(candidate.PaidAmount == nil || matches(event.PaidAmount, *candidate.PaidAmount))
	if exactEvidence {
		return true
	}
	if candidate.PaidAmount != nil || candidate.OccurredAt != nil || event.Kind != "recharge" || event.EffectiveAt.After(candidateTime.Add(fundingCoveredEventMaxDelay)) {
		return false
	}
	switch strings.TrimSpace(candidate.Source) {
	case "redeem_code", "balance_increase":
	default:
		return false
	}
	candidateDelta := candidate.RawBalanceDetected - candidate.RawBalanceBefore
	eventDelta := event.RawBalanceAfter - event.RawBalanceBefore
	balanceDrift := event.RawBalanceBefore - candidate.RawBalanceBefore
	return candidateDelta > fundingEvidenceTolerance &&
		eventDelta > fundingEvidenceTolerance &&
		balanceDrift >= -fundingEvidenceTolerance &&
		balanceDrift <= candidate.CreditedAmount*fundingCoveredBalanceDriftRatio+fundingEvidenceTolerance &&
		matches(candidateDelta, candidate.CreditedAmount) &&
		matches(eventDelta, event.CreditedAmount) &&
		matches(event.RawBalanceAfter, candidate.RawBalanceDetected) &&
		event.RawBalanceBefore >= candidate.RawBalanceBefore-fundingEvidenceTolerance &&
		event.RawBalanceBefore <= candidate.RawBalanceDetected+fundingEvidenceTolerance &&
		event.CreditedAmount <= candidate.CreditedAmount+fundingEvidenceTolerance &&
		matches(candidate.CreditedAmount-event.CreditedAmount, balanceDrift)
}

func updateCostStateAndProjection(tx *gorm.DB, next *ChannelCostState, expectedVersion uint64) error {
	return updateCostStateAndProjectionForChannelIdentity(tx, next, expectedVersion, 0, false)
}

// lockChannelCostProjection keeps every state mutation in the same lock order
// as an account-identity change: channel first, then cost state, then funding
// evidence. Otherwise a monitor write could hold the cost row while an admin
// edit holds the channel row, producing a PostgreSQL deadlock.
func lockChannelCostProjection(tx *gorm.DB, channelID uint, expectedIdentityRevision uint64) error {
	if tx == nil || channelID == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	var channel Channel
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "upstream_account_identity_revision").
		First(&channel, channelID).Error; err != nil {
		return err
	}
	if expectedIdentityRevision != 0 && channel.UpstreamAccountIdentityRevision != expectedIdentityRevision {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

func updateBalanceStateAndProjectionForChannelIdentity(tx *gorm.DB, next *ChannelCostState, expectedVersion, expectedIdentityRevision uint64) error {
	if err := lockChannelCostProjection(tx, next.ChannelID, expectedIdentityRevision); err != nil {
		return err
	}
	var current ChannelCostState
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND version = ?", next.ChannelID, expectedVersion).
		First(&current).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrCostStateConflict
		}
		return err
	}
	if err := validateOrdinaryCostStateTransition(&current, next, expectedVersion); err != nil {
		return err
	}
	var pending int64
	if err := tx.Model(&ChannelFundingCandidate{}).
		Where("channel_id = ? AND status = ?", next.ChannelID, FundingCandidatePending).
		Count(&pending).Error; err != nil {
		return err
	}
	if pending > 0 {
		if math.IsNaN(current.FundingFactor) || math.IsInf(current.FundingFactor, 0) || current.FundingFactor < 0 {
			return errors.New("pending funding basis has an invalid factor")
		}
		actualInventory := next.RawBalance * current.FundingFactor
		if math.IsNaN(next.RawBalance) || math.IsInf(next.RawBalance, 0) || math.IsNaN(actualInventory) || math.IsInf(actualInventory, 0) {
			return errors.New("pending funding basis produced an invalid balance")
		}
		next.ActualInventory = actualInventory
		next.FundingFactor = current.FundingFactor
		next.ActualTodayCost = current.ActualTodayCost
		next.ActualTotalCost = current.ActualTotalCost
		next.Source = current.Source
		next.Status = current.Status
	}
	if err := updateCostStateAndProjectionForChannelIdentity(tx, next, expectedVersion, expectedIdentityRevision, expectedIdentityRevision != 0); err != nil {
		return err
	}
	return appendChangedCostStateSnapshotsTx(tx, &current, next)
}

func validateFundingCandidateBalanceBoundary(candidate *ChannelFundingCandidate) error {
	if candidate == nil || math.IsNaN(candidate.RawBalanceBefore) || math.IsInf(candidate.RawBalanceBefore, 0) || candidate.RawBalanceBefore < 0 ||
		math.IsNaN(candidate.RawBalanceDetected) || math.IsInf(candidate.RawBalanceDetected, 0) || candidate.RawBalanceDetected < 0 {
		return errors.New("funding candidate balances must be finite and non-negative")
	}
	return nil
}

func updateCostStateAndProjectionForChannelIdentity(tx *gorm.DB, next *ChannelCostState, expectedVersion, expectedIdentityRevision uint64, clearBalanceFact bool) error {
	if err := lockChannelCostProjection(tx, next.ChannelID, expectedIdentityRevision); err != nil {
		return err
	}
	result := tx.Model(&ChannelCostState{}).
		Where("channel_id = ? AND version = ?", next.ChannelID, expectedVersion).
		Updates(map[string]any{
			"raw_balance": next.RawBalance, "actual_inventory": next.ActualInventory,
			"raw_today_cost": next.RawTodayCost, "actual_today_cost": next.ActualTodayCost,
			"raw_total_cost": next.RawTotalCost, "actual_total_cost": next.ActualTotalCost,
			"cost_sampled_at": next.CostSampledAt,
			"funding_factor":  next.FundingFactor, "balance_sampled_at": next.BalanceSampledAt,
			"observed_provider_factor":            next.ObservedProviderFactor,
			"observed_provider_factor_source":     strings.TrimSpace(next.ObservedProviderFactorSource),
			"observed_provider_factor_sampled_at": next.ObservedProviderFactorSampledAt,
			"cost_unit_epoch":                     next.CostUnitEpoch,
			"version":                             expectedVersion + 1, "source": strings.TrimSpace(next.Source),
			"status": strings.TrimSpace(next.Status), "last_error": next.LastError,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrCostStateConflict
	}
	updates := map[string]any{
		"raw_last_balance": next.RawBalance, "last_balance": next.ActualInventory,
		"last_balance_at": next.BalanceSampledAt, "funding_factor": next.FundingFactor,
		"funding_factor_source": strings.TrimSpace(next.Source),
		"cost_unit_epoch":       next.CostUnitEpoch,
		"raw_today_cost":        next.RawTodayCost, "today_cost": next.ActualTodayCost,
		"raw_total_cost": next.RawTotalCost, "total_cost": next.ActualTotalCost,
		"today_cost_at": next.CostSampledAt,
	}
	if clearBalanceFact {
		updates["balance_error"] = ""
		updates["balance_error_at"] = nil
		updates["upstream_account_balance_insufficient"] = false
		updates["upstream_account_balance_insufficient_at"] = nil
		updates["upstream_account_balance_insufficient_revision"] = 0
	}
	query := tx.Model(&Channel{}).Where("id = ?", next.ChannelID)
	if expectedIdentityRevision != 0 {
		query = query.Where("upstream_account_identity_revision = ?", expectedIdentityRevision)
	}
	projection := query.Updates(updates)
	if projection.Error != nil {
		return projection.Error
	}
	if expectedIdentityRevision != 0 && projection.RowsAffected != 1 {
		return ErrStaleChannelAccountIdentityRevision
	}
	next.Version = expectedVersion + 1
	return nil
}
func lockCostStateTx(tx *gorm.DB, channelID uint) (*ChannelCostState, error) {
	if tx == nil || channelID == 0 {
		return nil, errors.New("channel cost state is required")
	}
	var state ChannelCostState
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("channel_id = ?", channelID).First(&state).Error; err != nil {
		return nil, err
	}
	return &state, nil
}

func lockChannelCostStateTx(tx *gorm.DB, channelID uint) (*ChannelCostState, error) {
	_, state, err := lockChannelAndCostStateTx(tx, channelID)
	return state, err
}

func lockChannelAndCostStateTx(tx *gorm.DB, channelID uint) (*Channel, *ChannelCostState, error) {
	if tx == nil || channelID == 0 {
		return nil, nil, errors.New("channel cost state is required")
	}
	channel, err := requireOrdinaryChannelWriteTx(tx, channelID)
	if err != nil {
		return nil, nil, err
	}
	state, err := lockCostStateTx(tx, channelID)
	if err != nil {
		return nil, nil, err
	}
	if channel.CostUnitEpoch == 0 || state.CostUnitEpoch == 0 || channel.CostUnitEpoch != state.CostUnitEpoch {
		return nil, nil, ErrCostUnitEpochConflict
	}
	return &channel, state, nil
}

func populateCostUnitRebaseIdentityEvidence(audit *ChannelCostUnitRebase, channel *Channel, identity *UpstreamAccountIdentitySnapshot) {
	if audit == nil || channel == nil || identity == nil {
		return
	}
	audit.ChannelIdentityRevision = identity.ChannelIdentityRevision
	audit.ChannelNameSnapshot = strings.TrimSpace(channel.Name)
	audit.ChannelTypeSnapshot = channel.Type
	audit.AccountIdentityFingerprint = identity.Fingerprint
	audit.AccountIdentityFingerprintVersion = identity.FingerprintVersion
	audit.AccountIdentityFingerprintSource = strings.TrimSpace(identity.Source)
	audit.AccountIdentityFingerprintObservedAt = identity.ObservedAt
}

func populateCostUnitRebaseEvidence(audit *ChannelCostUnitRebase, current, next *ChannelCostState, expectedVersion uint64) {
	if audit.AppliedAt.IsZero() {
		audit.AppliedAt = time.Now().UTC()
	}
	audit.ExpectedStateVersion = expectedVersion
	audit.AppliedStateVersion = expectedVersion + 1
	audit.ExpectedCostUnitEpoch = current.CostUnitEpoch
	audit.AppliedCostUnitEpoch = next.CostUnitEpoch
	audit.FundingFactorBefore = current.FundingFactor
	audit.FundingFactorAfter = next.FundingFactor
	audit.RawBalanceBefore = current.RawBalance
	audit.ActualInventoryBefore = current.ActualInventory
	audit.RawBalanceAfter = next.RawBalance
	audit.ActualInventoryAfter = next.ActualInventory
	audit.RawTodayCostBefore = current.RawTodayCost
	audit.ActualTodayCostBefore = current.ActualTodayCost
	audit.RawTodayCostAfter = next.RawTodayCost
	audit.ActualTodayCostAfter = next.ActualTodayCost
	audit.RawTotalCostBefore = current.RawTotalCost
	audit.ActualTotalCostBefore = current.ActualTotalCost
	audit.RawTotalCostAfter = next.RawTotalCost
	audit.ActualTotalCostAfter = next.ActualTotalCost
	audit.BalanceSampledAtBefore = current.BalanceSampledAt
	audit.CostSampledAtBefore = current.CostSampledAt
	if next.BalanceSampledAt != nil {
		audit.BalanceSampledAtAfter = *next.BalanceSampledAt
	}
	if next.CostSampledAt != nil {
		audit.CostSampledAtAfter = *next.CostSampledAt
	}
	audit.FactorSource = strings.TrimSpace(next.ObservedProviderFactorSource)
	audit.StateSourceBefore = strings.TrimSpace(current.Source)
	audit.StateSourceAfter = strings.TrimSpace(next.Source)
	audit.Note = strings.TrimSpace(audit.Note)
}

func validateCostUnitRebase(current, next *ChannelCostState, audit *ChannelCostUnitRebase) error {
	if current == nil || next == nil || audit == nil || audit.ID != 0 || current.ChannelID == 0 ||
		current.ChannelID != next.ChannelID || audit.ChannelID != next.ChannelID ||
		audit.ChannelIdentityRevision == 0 || strings.TrimSpace(audit.ChannelNameSnapshot) == "" ||
		audit.ChannelTypeSnapshot != ChannelTypeSub2API || !isLowerHexFingerprint(audit.AccountIdentityFingerprint) ||
		audit.AccountIdentityFingerprintVersion != 1 || strings.TrimSpace(audit.AccountIdentityFingerprintSource) == "" ||
		audit.AccountIdentityFingerprintObservedAt.IsZero() ||
		current.CostUnitEpoch == 0 || current.CostUnitEpoch == math.MaxUint64 || next.CostUnitEpoch != current.CostUnitEpoch+1 ||
		audit.ExpectedCostUnitEpoch != current.CostUnitEpoch || audit.AppliedCostUnitEpoch != next.CostUnitEpoch ||
		next.BalanceSampledAt == nil || next.BalanceSampledAt.IsZero() || next.CostSampledAt == nil || next.CostSampledAt.IsZero() ||
		next.ObservedProviderFactorSampledAt == nil || next.ObservedProviderFactorSampledAt.IsZero() ||
		audit.AppliedAt.IsZero() || audit.RateTransitionAt.IsZero() || len(audit.Note) > 1024 ||
		len(audit.FactorSource) > 32 || len(audit.StateSourceBefore) > 32 || len(audit.StateSourceAfter) > 32 ||
		audit.FactorSource != "advertised" || audit.StateSourceBefore == "" || audit.StateSourceAfter == "" ||
		audit.RateEvidenceGroupCount < 1 || !isLowerHexFingerprint(audit.RateEvidenceDigest) {
		return ErrCostUnitRebaseInvalid
	}
	if !validMultiplierMode(audit.PreviousMultiplierMode) || !validMultiplierMode(audit.CurrentMultiplierMode) {
		return ErrCostUnitRebaseInvalid
	}
	for _, value := range []float64{
		audit.PreviousMultiplier, audit.CurrentMultiplier,
		audit.PreviousProviderFactor, audit.CurrentProviderFactor,
		audit.RateRawScale,
	} {
		if !finitePositive(value) {
			return ErrCostUnitRebaseInvalid
		}
	}
	if !finiteNonNegative(audit.FundingFactorBefore) || !finiteNonNegative(audit.FundingFactorAfter) {
		return ErrCostUnitRebaseInvalid
	}
	for _, value := range []float64{
		audit.RawBalanceBefore, audit.ActualInventoryBefore, audit.RawBalanceAfter, audit.ActualInventoryAfter,
	} {
		if !finiteNumber(value) {
			return ErrCostUnitRebaseInvalid
		}
	}
	for _, value := range []float64{
		audit.RawTodayCostBefore, audit.ActualTodayCostBefore, audit.RawTodayCostAfter, audit.ActualTodayCostAfter,
		audit.RawTotalCostBefore, audit.ActualTotalCostBefore, audit.RawTotalCostAfter, audit.ActualTotalCostAfter,
	} {
		if !finiteNonNegative(value) {
			return ErrCostUnitRebaseInvalid
		}
	}
	if next.ObservedProviderFactor == nil || !finitePositive(*next.ObservedProviderFactor) ||
		math.Abs(*next.ObservedProviderFactor-audit.CurrentProviderFactor) > fundingEvidenceTolerance {
		return ErrCostUnitRebaseInvalid
	}
	if current.ObservedProviderFactor == nil || current.ObservedProviderFactorSampledAt == nil || current.ObservedProviderFactorSampledAt.IsZero() ||
		strings.TrimSpace(current.ObservedProviderFactorSource) != "advertised" ||
		!financiallyEqual(*current.ObservedProviderFactor, audit.PreviousProviderFactor) {
		return ErrCostUnitRebaseInvalid
	}
	if next.ObservedProviderFactorSampledAt.Before(*current.ObservedProviderFactorSampledAt) {
		return ErrCostUnitRebaseInvalid
	}
	previousProviderFactor, ok := providerFactorForMultiplier(audit.PreviousMultiplier, audit.PreviousMultiplierMode)
	if !ok || !financiallyEqual(audit.PreviousProviderFactor, previousProviderFactor) {
		return ErrCostUnitRebaseInvalid
	}
	currentProviderFactor, ok := providerFactorForMultiplier(audit.CurrentMultiplier, audit.CurrentMultiplierMode)
	if !ok || !financiallyEqual(audit.CurrentProviderFactor, currentProviderFactor) {
		return ErrCostUnitRebaseInvalid
	}
	expectedFundingFactor := audit.FundingFactorBefore * currentProviderFactor / previousProviderFactor
	if !finiteNonNegative(expectedFundingFactor) || !financiallyEqual(audit.FundingFactorAfter, expectedFundingFactor) {
		return ErrCostUnitRebaseInvalid
	}
	expectedInventory := audit.RawBalanceAfter * audit.FundingFactorAfter
	if !finiteNumber(expectedInventory) || !financiallyEqual(audit.ActualInventoryAfter, expectedInventory) {
		return ErrCostUnitRebaseInvalid
	}
	oldPriorRaw, ok := nonNegativeFinancialDifference(audit.RawTotalCostBefore, audit.RawTodayCostBefore)
	if !ok {
		return ErrCostUnitRebaseInvalid
	}
	newPriorRaw, ok := nonNegativeFinancialDifference(audit.RawTotalCostAfter, audit.RawTodayCostAfter)
	if !ok {
		return ErrCostUnitRebaseInvalid
	}
	rawScale := audit.PreviousProviderFactor / audit.CurrentProviderFactor
	if !financiallyEqual(audit.RateRawScale, rawScale) {
		return ErrCostUnitRebaseInvalid
	}
	unchangedPrior := financiallyEqual(newPriorRaw, oldPriorRaw)
	scaledPrior := financiallyEqual(newPriorRaw, oldPriorRaw*rawScale)
	if (!unchangedPrior && !scaledPrior) ||
		(audit.CounterTransition == "prior_unchanged" && !unchangedPrior) ||
		(audit.CounterTransition == "prior_scaled" && !scaledPrior) ||
		(audit.CounterTransition != "prior_unchanged" && audit.CounterTransition != "prior_scaled") {
		return ErrCostUnitRebaseInvalid
	}
	priorActual, ok := nonNegativeFinancialDifference(audit.ActualTotalCostBefore, audit.ActualTodayCostBefore)
	if !ok {
		return ErrCostUnitRebaseInvalid
	}
	expectedToday := audit.RawTodayCostAfter * audit.FundingFactorAfter
	expectedTotal := priorActual + expectedToday
	if !finiteNonNegative(expectedToday) || !finiteNonNegative(expectedTotal) ||
		!financiallyEqual(audit.ActualTodayCostAfter, expectedToday) ||
		!financiallyEqual(audit.ActualTotalCostAfter, expectedTotal) {
		return ErrCostUnitRebaseInvalid
	}
	if audit.BalanceSampledAtBefore != nil && !audit.BalanceSampledAtAfter.After(*audit.BalanceSampledAtBefore) {
		return ErrCostUnitRebaseInvalid
	}
	if audit.CostSampledAtBefore != nil && !audit.CostSampledAtAfter.After(*audit.CostSampledAtBefore) {
		return ErrCostUnitRebaseInvalid
	}
	if audit.CostSampledAtBefore != nil && !audit.RateTransitionAt.After(*audit.CostSampledAtBefore) {
		return ErrCostUnitRebaseInvalid
	}
	if audit.RateTransitionAt.After(audit.CostSampledAtAfter) {
		return ErrCostUnitRebaseInvalid
	}
	return nil
}

func applyCostUnitRateTransitionTx(tx *gorm.DB, audit *ChannelCostUnitRebase) error {
	if tx == nil || audit == nil || audit.ChannelID == 0 || len(audit.RateTransitionItems) != audit.RateEvidenceGroupCount {
		return ErrCostUnitRebaseInvalid
	}
	digest, err := CostUnitRateEvidenceDigest(audit.RateTransitionItems)
	if err != nil || digest != audit.RateEvidenceDigest {
		return ErrCostUnitRebaseInvalid
	}

	var current []RateSnapshot
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND cost_unit_epoch = ?", audit.ChannelID, audit.ExpectedCostUnitEpoch).
		Order("model_name ASC").
		Find(&current).Error; err != nil {
		return err
	}
	if len(current) != len(audit.RateTransitionItems) {
		return ErrCostUnitRebaseInvalid
	}
	transitionByName := make(map[string]CostUnitRateTransitionItem, len(audit.RateTransitionItems))
	for _, item := range audit.RateTransitionItems {
		name := strings.ToLower(strings.TrimSpace(item.ModelName))
		if name == "" {
			return ErrCostUnitRebaseInvalid
		}
		if _, exists := transitionByName[name]; exists {
			return ErrCostUnitRebaseInvalid
		}
		transitionByName[name] = item
	}

	for i := range current {
		rate := &current[i]
		item, ok := transitionByName[strings.ToLower(strings.TrimSpace(rate.ModelName))]
		if !ok || !financiallyEqual(rate.Ratio, item.RatioBefore) ||
			!financiallyEqual(rate.CompletionRatio, item.CompletionRatioBefore) ||
			!financiallyEqual(item.RatioAfter, item.RatioBefore*audit.RateRawScale) ||
			!financiallyEqual(item.CompletionRatioAfter, item.CompletionRatioBefore*audit.RateRawScale) {
			return ErrCostUnitRebaseInvalid
		}

		oldRatio := rate.Ratio
		oldCompletion := rate.CompletionRatio
		oldEffective := oldRatio * audit.FundingFactorBefore
		newEffective := item.RatioAfter * audit.FundingFactorAfter
		oldEffectiveCompletion := oldCompletion * audit.FundingFactorBefore
		newEffectiveCompletion := item.CompletionRatioAfter * audit.FundingFactorAfter
		if !financiallyEqual(oldEffective, newEffective) || !financiallyEqual(oldEffectiveCompletion, newEffectiveCompletion) {
			return ErrCostUnitRebaseInvalid
		}

		result := tx.Model(&RateSnapshot{}).
			Where("id = ? AND channel_id = ? AND cost_unit_epoch = ? AND ratio = ? AND completion_ratio = ?", rate.ID, audit.ChannelID, audit.ExpectedCostUnitEpoch, oldRatio, oldCompletion).
			Updates(map[string]any{
				"ratio":            item.RatioAfter,
				"completion_ratio": item.CompletionRatioAfter,
				"description":      item.DescriptionAfter,
				"last_seen_at":     audit.RateTransitionAt,
				"cost_unit_epoch":  audit.AppliedCostUnitEpoch,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrCostUnitRebaseInvalid
		}
		if err := tx.Create(&RateChangeLog{
			ChannelID: audit.ChannelID, ModelName: rate.ModelName,
			OldRatio: &oldRatio, NewRatio: item.RatioAfter,
			OldCompletionRatio: &oldCompletion, NewCompletionRatio: item.CompletionRatioAfter,
			FundingFactorBefore: audit.FundingFactorBefore, FundingFactorAfter: audit.FundingFactorAfter,
			OldEffectiveRatio: &oldEffective, NewEffectiveRatio: newEffective,
			OldEffectiveCompletionRatio: &oldEffectiveCompletion, NewEffectiveCompletionRatio: newEffectiveCompletion,
			ChangeReason: "cost_unit_rebase", CostUnitEpochBefore: audit.ExpectedCostUnitEpoch,
			CostUnitEpochAfter: audit.AppliedCostUnitEpoch, ChangedAt: audit.RateTransitionAt,
		}).Error; err != nil {
			return fmt.Errorf("append cost-unit rate transition: %w", err)
		}
	}
	return nil
}

func appendCostUnitRebaseRateItemsTx(tx *gorm.DB, audit *ChannelCostUnitRebase) error {
	if tx == nil || audit == nil || audit.ID == 0 || len(audit.RateTransitionItems) != audit.RateEvidenceGroupCount {
		return ErrCostUnitRebaseInvalid
	}
	ordered := append([]CostUnitRateTransitionItem(nil), audit.RateTransitionItems...)
	sort.Slice(ordered, func(i, j int) bool {
		return strings.ToLower(strings.TrimSpace(ordered[i].ModelName)) < strings.ToLower(strings.TrimSpace(ordered[j].ModelName))
	})
	rows := make([]ChannelCostUnitRebaseRateItem, len(ordered))
	for index, item := range ordered {
		rows[index] = ChannelCostUnitRebaseRateItem{
			RebaseID: audit.ID, Ordinal: index, ModelName: strings.TrimSpace(item.ModelName),
			DescriptionBefore: strings.TrimSpace(item.DescriptionBefore), DescriptionAfter: strings.TrimSpace(item.DescriptionAfter),
			RatioBefore: item.RatioBefore, RatioAfter: item.RatioAfter,
			CompletionRatioBefore: item.CompletionRatioBefore, CompletionRatioAfter: item.CompletionRatioAfter,
			CreatedAt: audit.AppliedAt,
		}
	}
	if err := tx.Create(&rows).Error; err != nil {
		return fmt.Errorf("append cost-unit rate transition evidence: %w", err)
	}
	return verifyCostUnitRebaseRateItemsTx(tx, audit)
}

func verifyCostUnitRebaseRateItemsTx(tx *gorm.DB, audit *ChannelCostUnitRebase) error {
	if tx == nil || audit == nil || audit.ID == 0 || audit.RateEvidenceGroupCount < 1 || !isLowerHexFingerprint(audit.RateEvidenceDigest) {
		return ErrCostUnitRebaseInvalid
	}
	var rows []ChannelCostUnitRebaseRateItem
	if err := tx.Where("rebase_id = ?", audit.ID).Order("ordinal ASC, id ASC").Find(&rows).Error; err != nil {
		return err
	}
	if len(rows) != audit.RateEvidenceGroupCount {
		return ErrCostUnitRebaseInvalid
	}
	items := make([]CostUnitRateTransitionItem, len(rows))
	for index, row := range rows {
		if row.Ordinal != index {
			return ErrCostUnitRebaseInvalid
		}
		items[index] = CostUnitRateTransitionItem{
			ModelName: row.ModelName, DescriptionBefore: row.DescriptionBefore, DescriptionAfter: row.DescriptionAfter,
			RatioBefore: row.RatioBefore, RatioAfter: row.RatioAfter,
			CompletionRatioBefore: row.CompletionRatioBefore, CompletionRatioAfter: row.CompletionRatioAfter,
		}
	}
	digest, err := CostUnitRateEvidenceDigest(items)
	if err != nil || digest != audit.RateEvidenceDigest {
		return ErrCostUnitRebaseInvalid
	}
	return nil
}

func nonNegativeFinancialDifference(total, part float64) (float64, bool) {
	if !finiteNonNegative(total) || !finiteNonNegative(part) {
		return 0, false
	}
	difference := total - part
	if difference < -financialTolerance(total, part) {
		return 0, false
	}
	if difference < 0 {
		return 0, true
	}
	return difference, true
}

func appendCostStateSnapshotsTx(tx *gorm.DB, state *ChannelCostState) error {
	if tx == nil || state == nil || state.BalanceSampledAt == nil || state.CostSampledAt == nil {
		return ErrCostUnitRebaseInvalid
	}
	rawBalance := state.RawBalance
	rawTodayCost := state.RawTodayCost
	rawTotalCost := state.RawTotalCost
	fundingFactor := state.FundingFactor
	if err := tx.Create(&BalanceSnapshot{
		ChannelID: state.ChannelID, Balance: state.ActualInventory,
		RawBalance: &rawBalance, FundingFactor: &fundingFactor,
		CostBasisVersion: state.Version, CostUnitEpoch: state.CostUnitEpoch, SampledAt: *state.BalanceSampledAt,
	}).Error; err != nil {
		return fmt.Errorf("append post-rebase balance snapshot: %w", err)
	}
	if err := tx.Create(&CostSnapshot{
		ChannelID: state.ChannelID, TodayCost: state.ActualTodayCost, TotalCost: state.ActualTotalCost,
		RawTodayCost: &rawTodayCost, RawTotalCost: &rawTotalCost, FundingFactor: &fundingFactor,
		CostBasisVersion: state.Version, CostUnitEpoch: state.CostUnitEpoch, SampledAt: *state.CostSampledAt,
	}).Error; err != nil {
		return fmt.Errorf("append post-rebase cost snapshot: %w", err)
	}
	return nil
}

func validateOrdinaryCostStateTransition(current, next *ChannelCostState, expectedVersion uint64) error {
	if current == nil || next == nil || current.ChannelID == 0 || current.ChannelID != next.ChannelID {
		return errors.New("matching channel cost states are required")
	}
	if current.Version != expectedVersion {
		return ErrCostStateConflict
	}
	if current.CostUnitEpoch == 0 || next.CostUnitEpoch != current.CostUnitEpoch {
		return ErrCostUnitEpochConflict
	}
	if current.BalanceSampledAt != nil && next.BalanceSampledAt != nil && next.BalanceSampledAt.Before(*current.BalanceSampledAt) ||
		current.CostSampledAt != nil && next.CostSampledAt != nil && next.CostSampledAt.Before(*current.CostSampledAt) {
		return ErrCostUnitEpochConflict
	}
	return nil
}

func appendChangedCostStateSnapshotsTx(tx *gorm.DB, current, next *ChannelCostState) error {
	if tx == nil || current == nil || next == nil || current.ChannelID == 0 || current.ChannelID != next.ChannelID ||
		current.CostUnitEpoch == 0 || next.CostUnitEpoch != current.CostUnitEpoch || next.Version == 0 {
		return ErrCostUnitEpochConflict
	}
	balanceChanged := next.BalanceSampledAt != nil && !next.BalanceSampledAt.IsZero() &&
		(current.BalanceSampledAt == nil || !next.BalanceSampledAt.Equal(*current.BalanceSampledAt))
	costChanged := next.CostSampledAt != nil && !next.CostSampledAt.IsZero() &&
		(current.CostSampledAt == nil || !next.CostSampledAt.Equal(*current.CostSampledAt))
	if balanceChanged {
		rawBalance, fundingFactor := next.RawBalance, next.FundingFactor
		if err := tx.Create(&BalanceSnapshot{
			ChannelID: next.ChannelID, Balance: next.ActualInventory,
			RawBalance: &rawBalance, FundingFactor: &fundingFactor,
			CostBasisVersion: next.Version, CostUnitEpoch: next.CostUnitEpoch, SampledAt: *next.BalanceSampledAt,
		}).Error; err != nil {
			return fmt.Errorf("append balance snapshot: %w", err)
		}
	}
	if costChanged {
		rawTodayCost, rawTotalCost, fundingFactor := next.RawTodayCost, next.RawTotalCost, next.FundingFactor
		if err := tx.Create(&CostSnapshot{
			ChannelID: next.ChannelID, TodayCost: next.ActualTodayCost, TotalCost: next.ActualTotalCost,
			RawTodayCost: &rawTodayCost, RawTotalCost: &rawTotalCost, FundingFactor: &fundingFactor,
			CostBasisVersion: next.Version, CostUnitEpoch: next.CostUnitEpoch, SampledAt: *next.CostSampledAt,
		}).Error; err != nil {
			return fmt.Errorf("append cost snapshot: %w", err)
		}
	}
	return nil
}

func appendFundingFactorRateChangesTx(tx *gorm.DB, current, next *ChannelCostState, changedAt time.Time) error {
	if tx == nil || current == nil || next == nil || current.ChannelID == 0 || current.ChannelID != next.ChannelID ||
		current.CostUnitEpoch == 0 || current.CostUnitEpoch != next.CostUnitEpoch {
		return ErrCostUnitEpochConflict
	}
	if current.FundingFactor == next.FundingFactor {
		return nil
	}
	if changedAt.IsZero() {
		changedAt = time.Now().UTC()
	}
	var rates []RateSnapshot
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND cost_unit_epoch = ?", next.ChannelID, next.CostUnitEpoch).
		Order("model_name ASC").Find(&rates).Error; err != nil {
		return err
	}
	changes := make([]RateChangeLog, 0, len(rates))
	for _, rate := range rates {
		rawRatio, rawCompletion := rate.Ratio, rate.CompletionRatio
		oldEffective := rawRatio * current.FundingFactor
		newEffective := rawRatio * next.FundingFactor
		oldEffectiveCompletion := rawCompletion * current.FundingFactor
		newEffectiveCompletion := rawCompletion * next.FundingFactor
		changes = append(changes, RateChangeLog{
			ChannelID: next.ChannelID, ModelName: rate.ModelName,
			OldRatio: &rawRatio, NewRatio: rawRatio,
			OldCompletionRatio: &rawCompletion, NewCompletionRatio: rawCompletion,
			FundingFactorBefore: current.FundingFactor, FundingFactorAfter: next.FundingFactor,
			OldEffectiveRatio: &oldEffective, NewEffectiveRatio: newEffective,
			OldEffectiveCompletionRatio: &oldEffectiveCompletion, NewEffectiveCompletionRatio: newEffectiveCompletion,
			ChangeReason: "funding_factor", CostUnitEpochBefore: next.CostUnitEpoch,
			CostUnitEpochAfter: next.CostUnitEpoch, ChangedAt: changedAt,
		})
	}
	if len(changes) == 0 {
		return nil
	}
	if err := tx.Create(&changes).Error; err != nil {
		return fmt.Errorf("append funding-factor rate changes: %w", err)
	}
	return nil
}

func isLowerHexFingerprint(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func validMultiplierMode(value string) bool {
	switch strings.TrimSpace(value) {
	case "divide", "multiply":
		return true
	default:
		return false
	}
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func finiteNonNegative(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func finiteNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func providerFactorForMultiplier(multiplier float64, mode string) (float64, bool) {
	if !finitePositive(multiplier) {
		return 0, false
	}
	switch strings.TrimSpace(mode) {
	case "divide":
		return 1 / multiplier, true
	case "multiply":
		return multiplier, true
	default:
		return 0, false
	}
}

func financiallyEqual(left, right float64) bool {
	return finiteNonNegative(left) && finiteNonNegative(right) && math.Abs(left-right) <= financialTolerance(left, right)
}

func financialTolerance(left, right float64) float64 {
	return math.Max(1e-9, math.Max(math.Abs(left), math.Abs(right))*1e-9)
}
