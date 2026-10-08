package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const CostBasisOverrideSource = "manual_override"
const CostBasisOverrideRateChangeReason = "manual_cost_basis_override"

var ErrCostBasisOverrideInvalid = errors.New("cost basis override evidence is invalid")
var ErrCostBasisOverrideIdempotencyConflict = errors.New("cost basis override idempotency key conflicts with another request")

func (r *Costing) FindCostBasisOverride(channelID uint, idempotencyKey string) (*ChannelCostBasisOverride, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if r == nil || r.db == nil || channelID == 0 || idempotencyKey == "" {
		return nil, ErrCostBasisOverrideInvalid
	}
	var audit ChannelCostBasisOverride
	err := r.db.Table("channel_cost_basis_overrides AS audit").Select("audit.*").
		Joins("JOIN channels ON channels.id = audit.channel_id AND channels.upstream_account_identity_revision = audit.channel_identity_revision").
		Where("audit.channel_id = ? AND audit.idempotency_key = ?", channelID, idempotencyKey).
		Order("audit.id DESC").Take(&audit).Error
	if err != nil {
		return nil, err
	}
	return &audit, nil
}

// ApplyCostBasisOverride atomically replaces the current funding factor while
// preserving raw rates and already-recorded actual costs. The cost-unit epoch
// advances so every downstream price consumer must observe the new basis.
func (r *Costing) ApplyCostBasisOverride(
	next *ChannelCostState,
	audit *ChannelCostBasisOverride,
	expectedVersion, expectedIdentityRevision, expectedCostUnitEpoch uint64,
) (bool, error) {
	if r == nil || r.db == nil || next == nil || audit == nil || next.ChannelID == 0 || audit.ChannelID != next.ChannelID ||
		expectedVersion == 0 || expectedIdentityRevision == 0 || expectedCostUnitEpoch == 0 {
		return false, ErrCostBasisOverrideInvalid
	}
	audit.IdempotencyKey = strings.TrimSpace(audit.IdempotencyKey)
	audit.RequestFingerprint = strings.TrimSpace(audit.RequestFingerprint)
	audit.Actor = strings.TrimSpace(audit.Actor)
	audit.Note = strings.TrimSpace(audit.Note)
	if audit.IdempotencyKey == "" || len(audit.IdempotencyKey) > 128 || !isLowerHexFingerprint(audit.RequestFingerprint) ||
		audit.Actor == "" || len(audit.Actor) > 128 || audit.Note == "" || len(audit.Note) > 1024 {
		return false, ErrCostBasisOverrideInvalid
	}

	var committedAudit ChannelCostBasisOverride
	var committedState ChannelCostState
	idempotentReplay := false
	err := r.db.Transaction(func(tx *gorm.DB) error {
		channel, current, err := lockChannelAndCostStateTx(tx, next.ChannelID)
		if err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != expectedIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}

		var existing ChannelCostBasisOverride
		err = tx.Where("channel_id = ? AND channel_identity_revision = ? AND idempotency_key = ?",
			audit.ChannelID, expectedIdentityRevision, audit.IdempotencyKey).First(&existing).Error
		if err == nil {
			if existing.RequestFingerprint != audit.RequestFingerprint || existing.Actor != audit.Actor {
				return ErrCostBasisOverrideIdempotencyConflict
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
		if current.CostUnitEpoch != expectedCostUnitEpoch || channel.CostUnitEpoch != expectedCostUnitEpoch {
			return ErrCostUnitEpochConflict
		}

		var pending []ChannelFundingCandidate
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ? AND status = ?", next.ChannelID, FundingCandidatePending).
			Order("id ASC").Find(&pending).Error; err != nil {
			return err
		}
		candidateDigest, err := costBasisOverrideFundingCandidateDigest(pending)
		if err != nil {
			return err
		}

		nextCopy := *next
		if err := validateCostBasisOverrideState(current, &nextCopy, expectedVersion, expectedCostUnitEpoch); err != nil {
			return err
		}
		var rates []RateSnapshot
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ?", next.ChannelID).
			Order("model_name ASC, id ASC").Find(&rates).Error; err != nil {
			return err
		}
		for _, rate := range rates {
			if rate.CostUnitEpoch != expectedCostUnitEpoch {
				return ErrCostUnitEpochConflict
			}
		}
		rateDigest, err := costBasisOverrideRateDigest(rates)
		if err != nil {
			return err
		}

		auditCopy := *audit
		populateCostBasisOverrideAudit(
			&auditCopy, channel, current, &nextCopy,
			expectedVersion, expectedIdentityRevision, expectedCostUnitEpoch,
			len(pending), candidateDigest, len(rates), rateDigest,
		)
		if err := validateCostBasisOverrideAudit(&auditCopy); err != nil {
			return err
		}
		if err := tx.Create(&auditCopy).Error; err != nil {
			return err
		}
		if len(pending) > 0 {
			candidateIDs := make([]uint64, len(pending))
			for index := range pending {
				candidateIDs[index] = pending[index].ID
			}
			updated := tx.Model(&ChannelFundingCandidate{}).
				Where("channel_id = ? AND status = ? AND id IN ?", next.ChannelID, FundingCandidatePending, candidateIDs).
				Update("status", FundingCandidateSuperseded)
			if updated.Error != nil {
				return updated.Error
			}
			if updated.RowsAffected != int64(len(pending)) {
				return ErrCostStateConflict
			}
		}
		if err := applyCostBasisOverrideRatesTx(tx, rates, current, &nextCopy, auditCopy.AppliedAt); err != nil {
			return err
		}
		if err := updateCostStateAndProjection(tx, &nextCopy, expectedVersion); err != nil {
			return err
		}
		committedAudit = auditCopy
		committedState = nextCopy
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

func validateCostBasisOverrideState(current, next *ChannelCostState, expectedVersion, expectedCostUnitEpoch uint64) error {
	if current == nil || next == nil || current.ChannelID == 0 || current.ChannelID != next.ChannelID ||
		current.Version != expectedVersion || current.CostUnitEpoch != expectedCostUnitEpoch || expectedCostUnitEpoch == math.MaxUint64 ||
		next.CostUnitEpoch != expectedCostUnitEpoch+1 || !finiteNonNegative(current.FundingFactor) ||
		!finiteNonNegative(next.FundingFactor) ||
		!finiteNumber(current.RawBalance) || !finiteNumber(current.ActualInventory) ||
		!finiteNumber(next.RawBalance) || !finiteNumber(next.ActualInventory) ||
		!finiteNonNegative(current.RawTodayCost) || !finiteNonNegative(current.RawTotalCost) ||
		!finiteNonNegative(current.ActualTodayCost) || !finiteNonNegative(current.ActualTotalCost) ||
		!signedFinanciallyEqual(next.RawBalance, current.RawBalance) ||
		!signedFinanciallyEqual(next.ActualInventory, next.RawBalance*next.FundingFactor) ||
		!financiallyEqual(next.RawTodayCost, current.RawTodayCost) ||
		!financiallyEqual(next.RawTotalCost, current.RawTotalCost) ||
		!financiallyEqual(next.ActualTodayCost, current.ActualTodayCost) ||
		!financiallyEqual(next.ActualTotalCost, current.ActualTotalCost) ||
		!timesEqual(next.BalanceSampledAt, current.BalanceSampledAt) || !timesEqual(next.CostSampledAt, current.CostSampledAt) ||
		strings.TrimSpace(next.Source) != CostBasisOverrideSource || strings.TrimSpace(next.Status) != "ready" || next.LastError != "" ||
		!sameProviderFactorEvidence(current, next) {
		return ErrCostBasisOverrideInvalid
	}
	return nil
}

func sameProviderFactorEvidence(current, next *ChannelCostState) bool {
	if current.ObservedProviderFactor == nil || next.ObservedProviderFactor == nil {
		return current.ObservedProviderFactor == nil && next.ObservedProviderFactor == nil &&
			current.ObservedProviderFactorSampledAt == nil && next.ObservedProviderFactorSampledAt == nil &&
			current.ObservedProviderFactorSource == next.ObservedProviderFactorSource
	}
	return financiallyEqual(*current.ObservedProviderFactor, *next.ObservedProviderFactor) &&
		current.ObservedProviderFactorSource == next.ObservedProviderFactorSource &&
		timesEqual(current.ObservedProviderFactorSampledAt, next.ObservedProviderFactorSampledAt)
}

func timesEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func populateCostBasisOverrideAudit(
	audit *ChannelCostBasisOverride,
	channel *Channel,
	current, next *ChannelCostState,
	expectedVersion, expectedIdentityRevision, expectedCostUnitEpoch uint64,
	supersededCandidateCount int,
	supersededCandidateDigest string,
	rateCount int,
	rateDigest string,
) {
	if audit.AppliedAt.IsZero() {
		audit.AppliedAt = time.Now().UTC()
	}
	audit.ChannelIdentityRevision = expectedIdentityRevision
	audit.ChannelNameSnapshot = strings.TrimSpace(channel.Name)
	audit.ChannelTypeSnapshot = channel.Type
	audit.ExpectedStateVersion = expectedVersion
	audit.AppliedStateVersion = expectedVersion + 1
	audit.ExpectedCostUnitEpoch = expectedCostUnitEpoch
	audit.AppliedCostUnitEpoch = expectedCostUnitEpoch + 1
	audit.FundingFactorBefore = current.FundingFactor
	audit.FundingFactorAfter = next.FundingFactor
	audit.RawBalance = current.RawBalance
	audit.ActualInventoryBefore = current.ActualInventory
	audit.ActualInventoryAfter = next.ActualInventory
	audit.RawTodayCost = current.RawTodayCost
	audit.ActualTodayCostBefore = current.ActualTodayCost
	audit.ActualTodayCostAfter = next.ActualTodayCost
	audit.RawTotalCost = current.RawTotalCost
	audit.ActualTotalCostBefore = current.ActualTotalCost
	audit.ActualTotalCostAfter = next.ActualTotalCost
	audit.StateSourceBefore = strings.TrimSpace(current.Source)
	audit.StateSourceAfter = strings.TrimSpace(next.Source)
	audit.StateStatusBefore = strings.TrimSpace(current.Status)
	audit.StateStatusAfter = strings.TrimSpace(next.Status)
	audit.StateLastErrorBefore = current.LastError
	audit.SupersededCandidateCount = supersededCandidateCount
	audit.SupersededCandidateDigest = supersededCandidateDigest
	audit.RateSnapshotCount = rateCount
	audit.RateSnapshotDigest = rateDigest
}

func validateCostBasisOverrideAudit(audit *ChannelCostBasisOverride) error {
	if audit == nil || audit.ID != 0 || audit.ChannelID == 0 || audit.ChannelIdentityRevision == 0 ||
		strings.TrimSpace(audit.ChannelNameSnapshot) == "" ||
		(audit.ChannelTypeSnapshot != ChannelTypeNewAPI && audit.ChannelTypeSnapshot != ChannelTypeSub2API) ||
		audit.ExpectedStateVersion == 0 || audit.AppliedStateVersion != audit.ExpectedStateVersion+1 ||
		audit.ExpectedCostUnitEpoch == 0 || audit.AppliedCostUnitEpoch != audit.ExpectedCostUnitEpoch+1 ||
		!finiteNonNegative(audit.FundingFactorBefore) || !finiteNonNegative(audit.FundingFactorAfter) ||
		!finiteNumber(audit.RawBalance) || !finiteNumber(audit.ActualInventoryBefore) || !finiteNumber(audit.ActualInventoryAfter) ||
		!finiteNonNegative(audit.RawTodayCost) || !finiteNonNegative(audit.ActualTodayCostBefore) ||
		!financiallyEqual(audit.ActualTodayCostBefore, audit.ActualTodayCostAfter) ||
		!finiteNonNegative(audit.RawTotalCost) || !finiteNonNegative(audit.ActualTotalCostBefore) ||
		!financiallyEqual(audit.ActualTotalCostBefore, audit.ActualTotalCostAfter) ||
		strings.TrimSpace(audit.StateSourceBefore) == "" || audit.StateSourceAfter != CostBasisOverrideSource ||
		strings.TrimSpace(audit.StateStatusBefore) == "" || audit.StateStatusAfter != "ready" ||
		audit.SupersededCandidateCount < 0 || !isLowerHexFingerprint(audit.SupersededCandidateDigest) ||
		audit.RateSnapshotCount < 0 || !isLowerHexFingerprint(audit.RateSnapshotDigest) || audit.AppliedAt.IsZero() {
		return ErrCostBasisOverrideInvalid
	}
	return nil
}

func costBasisOverrideFundingCandidateDigest(candidates []ChannelFundingCandidate) (string, error) {
	ordered := append([]ChannelFundingCandidate(nil), candidates...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	type digestItem struct {
		ID                  uint64     `json:"id"`
		ChannelID           uint       `json:"channel_id"`
		Source              string     `json:"source"`
		ExternalID          string     `json:"external_id"`
		Status              string     `json:"status"`
		PaidAmount          *float64   `json:"paid_amount"`
		CreditedAmount      float64    `json:"credited_amount"`
		RawBalanceBefore    float64    `json:"raw_balance_before"`
		RawBalanceDetected  float64    `json:"raw_balance_detected"`
		FundingFactorBefore float64    `json:"funding_factor_before"`
		RawTodayCostBefore  float64    `json:"raw_today_cost_before"`
		RawTotalCostBefore  float64    `json:"raw_total_cost_before"`
		OccurredAt          *time.Time `json:"occurred_at,omitempty"`
		DetectedAt          time.Time  `json:"detected_at"`
		ResolvedEventID     *uint64    `json:"resolved_event_id"`
		Note                string     `json:"note"`
	}
	items := make([]digestItem, len(ordered))
	seen := make(map[uint64]struct{}, len(ordered))
	for index, candidate := range ordered {
		if candidate.ID == 0 || candidate.ChannelID == 0 || strings.TrimSpace(candidate.Source) == "" ||
			strings.TrimSpace(candidate.ExternalID) == "" || strings.TrimSpace(candidate.Status) != FundingCandidatePending ||
			!finiteNonNegative(candidate.CreditedAmount) || !finiteNonNegative(candidate.RawBalanceBefore) ||
			!finiteNonNegative(candidate.RawBalanceDetected) || !finiteNonNegative(candidate.FundingFactorBefore) ||
			!finiteNonNegative(candidate.RawTodayCostBefore) || !finiteNonNegative(candidate.RawTotalCostBefore) ||
			(candidate.PaidAmount != nil && !finiteNonNegative(*candidate.PaidAmount)) ||
			(candidate.ResolvedEventID != nil && *candidate.ResolvedEventID == 0) || candidate.DetectedAt.IsZero() {
			return "", ErrCostBasisOverrideInvalid
		}
		if _, exists := seen[candidate.ID]; exists {
			return "", ErrCostBasisOverrideInvalid
		}
		seen[candidate.ID] = struct{}{}
		items[index] = digestItem{
			ID: candidate.ID, ChannelID: candidate.ChannelID, Source: candidate.Source, ExternalID: candidate.ExternalID,
			Status: candidate.Status, PaidAmount: candidate.PaidAmount, CreditedAmount: candidate.CreditedAmount, RawBalanceBefore: candidate.RawBalanceBefore,
			RawBalanceDetected: candidate.RawBalanceDetected, FundingFactorBefore: candidate.FundingFactorBefore,
			RawTodayCostBefore: candidate.RawTodayCostBefore, RawTotalCostBefore: candidate.RawTotalCostBefore,
			OccurredAt: candidate.OccurredAt, DetectedAt: candidate.DetectedAt,
			ResolvedEventID: candidate.ResolvedEventID, Note: candidate.Note,
		}
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func applyCostBasisOverrideRatesTx(tx *gorm.DB, rates []RateSnapshot, current, next *ChannelCostState, changedAt time.Time) error {
	changes := make([]RateChangeLog, 0, len(rates))
	for _, rate := range rates {
		updated := tx.Model(&RateSnapshot{}).
			Where("id = ? AND channel_id = ? AND cost_unit_epoch = ?", rate.ID, next.ChannelID, current.CostUnitEpoch).
			Update("cost_unit_epoch", next.CostUnitEpoch)
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected != 1 {
			return ErrCostUnitEpochConflict
		}
		rawRatio, rawCompletion := rate.Ratio, rate.CompletionRatio
		oldEffective := rawRatio * current.FundingFactor
		newEffective := rawRatio * next.FundingFactor
		effectiveRawCompletion := costBasisOverrideEffectiveCompletionRatio(rawRatio, rawCompletion)
		oldCompletion := effectiveRawCompletion * current.FundingFactor
		newCompletion := effectiveRawCompletion * next.FundingFactor
		changes = append(changes, RateChangeLog{
			ChannelID: next.ChannelID, ModelName: rate.ModelName,
			OldRatio: &rawRatio, NewRatio: rawRatio,
			OldCompletionRatio: &rawCompletion, NewCompletionRatio: rawCompletion,
			FundingFactorBefore: current.FundingFactor, FundingFactorAfter: next.FundingFactor,
			OldEffectiveRatio: &oldEffective, NewEffectiveRatio: newEffective,
			OldEffectiveCompletionRatio: &oldCompletion, NewEffectiveCompletionRatio: newCompletion,
			ChangeReason:        CostBasisOverrideRateChangeReason,
			CostUnitEpochBefore: current.CostUnitEpoch, CostUnitEpochAfter: next.CostUnitEpoch,
			ChangedAt: changedAt,
		})
	}
	if len(changes) > 0 {
		if err := tx.Create(&changes).Error; err != nil {
			return err
		}
	}
	return nil
}

func costBasisOverrideEffectiveCompletionRatio(rawRatio, rawCompletion float64) float64 {
	if rawCompletion <= 0 {
		return rawRatio
	}
	return rawCompletion
}

func costBasisOverrideRateDigest(rates []RateSnapshot) (string, error) {
	ordered := append([]RateSnapshot(nil), rates...)
	sort.Slice(ordered, func(i, j int) bool {
		left, right := strings.ToLower(strings.TrimSpace(ordered[i].ModelName)), strings.ToLower(strings.TrimSpace(ordered[j].ModelName))
		if left != right {
			return left < right
		}
		return ordered[i].ID < ordered[j].ID
	})
	type digestItem struct {
		ID              uint    `json:"id"`
		ModelName       string  `json:"model_name"`
		Description     string  `json:"description"`
		Ratio           float64 `json:"ratio"`
		CompletionRatio float64 `json:"completion_ratio"`
	}
	items := make([]digestItem, len(ordered))
	for index, rate := range ordered {
		if rate.ID == 0 || strings.TrimSpace(rate.ModelName) == "" || !finiteNonNegative(rate.Ratio) || !finiteNonNegative(rate.CompletionRatio) {
			return "", ErrCostBasisOverrideInvalid
		}
		items[index] = digestItem{rate.ID, strings.TrimSpace(rate.ModelName), strings.TrimSpace(rate.Description), rate.Ratio, rate.CompletionRatio}
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func signedFinanciallyEqual(left, right float64) bool {
	return finiteNumber(left) && finiteNumber(right) && math.Abs(left-right) <= financialTolerance(left, right)
}
