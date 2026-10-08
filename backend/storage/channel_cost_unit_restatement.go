package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

var (
	ErrCostUnitRequestRestatementInvalid  = errors.New("cost unit request restatement evidence is invalid")
	ErrCostUnitRequestRestatementConflict = errors.New("cost unit request restatement evidence conflicts with stored requests")
)

var unsignedDecimalPattern = regexp.MustCompile(`^(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$`)

// CostUnitRequestRestatementEvidence is administrator-frozen evidence for a
// bounded historical correction. Decimal strings are intentionally retained
// until PostgreSQL evaluates them as numeric values.
type CostUnitRequestRestatementEvidence struct {
	RequestLogHighWater              uint64      `json:"request_log_high_water"`
	AffectedStartedAt                time.Time   `json:"affected_started_at"`
	AffectedEndedAt                  time.Time   `json:"affected_ended_at"`
	OldFundingFactor                 string      `json:"old_funding_factor"`
	NewFundingFactor                 string      `json:"new_funding_factor"`
	ExpectedPopulationRows           int64       `json:"expected_population_rows"`
	ExpectedNewRatioRows             int64       `json:"expected_new_ratio_rows"`
	ExpectedOldRatioRows             int64       `json:"expected_old_ratio_rows"`
	ExpectedRatioMismatchRows        int64       `json:"expected_ratio_mismatch_rows"`
	ExpectedLatencyProbeRows         int64       `json:"expected_latency_probe_rows"`
	ExpectedRestatementRows          int64       `json:"expected_restatement_rows"`
	ExpectedAlreadyCorrectCostedRows int64       `json:"expected_already_correct_costed_rows"`
	ExpectedRawCostSum               string      `json:"expected_raw_cost_sum"`
	ExpectedRecordedCostSum          string      `json:"expected_recorded_cost_sum"`
	ExpectedCorrectedCostSum         string      `json:"expected_corrected_cost_sum"`
	ExpectedUndercountSum            string      `json:"expected_undercount_sum"`
	ExpectedAlreadyCorrectRawCostSum string      `json:"expected_already_correct_raw_cost_sum"`
	ExpectedAllCostedRawCostSum      string      `json:"expected_all_costed_raw_cost_sum"`
	ExpectedBucketStarts             []time.Time `json:"expected_bucket_starts"`
	ExpectedBucketDigest             string      `json:"expected_bucket_digest"`
}

// ChannelCostUnitRequestRestatement is the immutable operation summary.
type ChannelCostUnitRequestRestatement struct {
	ID                               uint64    `gorm:"primaryKey" json:"id"`
	RebaseID                         *uint64   `json:"rebase_id,omitempty"`
	RepairAdoptionID                 *uint64   `json:"repair_adoption_id,omitempty"`
	ChannelID                        uint      `gorm:"not null" json:"channel_id"`
	ChannelIdentityRevision          uint64    `gorm:"not null" json:"-"`
	IdempotencyKey                   string    `gorm:"size:128;not null" json:"idempotency_key"`
	RequestFingerprint               string    `gorm:"size:64;not null" json:"-"`
	Actor                            string    `gorm:"size:128;not null" json:"actor"`
	RequestLogHighWater              uint64    `gorm:"not null" json:"request_log_high_water"`
	CostSampledAtBefore              time.Time `gorm:"not null" json:"cost_sampled_at_before"`
	CostSampledAtAfter               time.Time `gorm:"not null" json:"cost_sampled_at_after"`
	AffectedStartedAt                time.Time `gorm:"not null" json:"affected_started_at"`
	AffectedEndedAt                  time.Time `gorm:"not null" json:"affected_ended_at"`
	OldFundingFactor                 string    `gorm:"type:numeric;not null" json:"old_funding_factor"`
	NewFundingFactor                 string    `gorm:"type:numeric;not null" json:"new_funding_factor"`
	ExpectedPopulationRows           int64     `gorm:"not null" json:"expected_population_rows"`
	ExpectedNewRatioRows             int64     `gorm:"not null" json:"expected_new_ratio_rows"`
	ExpectedOldRatioRows             int64     `gorm:"not null" json:"expected_old_ratio_rows"`
	ExpectedRatioMismatchRows        int64     `gorm:"not null" json:"expected_ratio_mismatch_rows"`
	ExpectedLatencyProbeRows         int64     `gorm:"not null" json:"expected_latency_probe_rows"`
	ExpectedRestatementRows          int64     `gorm:"not null" json:"expected_restatement_rows"`
	ExpectedAlreadyCorrectCostedRows int64     `gorm:"not null" json:"expected_already_correct_costed_rows"`
	ExpectedRawCostSum               string    `gorm:"type:numeric;not null" json:"expected_raw_cost_sum"`
	ExpectedRecordedCostSum          string    `gorm:"type:numeric;not null" json:"expected_recorded_cost_sum"`
	ExpectedCorrectedCostSum         string    `gorm:"type:numeric;not null" json:"expected_corrected_cost_sum"`
	ExpectedUndercountSum            string    `gorm:"type:numeric;not null" json:"expected_undercount_sum"`
	ExpectedAlreadyCorrectRawCostSum string    `gorm:"type:numeric;not null" json:"expected_already_correct_raw_cost_sum"`
	ExpectedAllCostedRawCostSum      string    `gorm:"type:numeric;not null" json:"expected_all_costed_raw_cost_sum"`
	ExpectedBucketCount              int       `gorm:"not null" json:"expected_bucket_count"`
	ExpectedBucketStartsJSON         string    `gorm:"column:expected_bucket_starts;type:jsonb;not null" json:"-"`
	ExpectedBucketDigest             string    `gorm:"size:64;not null" json:"expected_bucket_digest"`
	AppliedRestatementRows           int64     `gorm:"not null" json:"applied_restatement_rows"`
	AppliedRawCostSum                string    `gorm:"type:numeric;not null" json:"applied_raw_cost_sum"`
	AppliedRecordedCostSum           string    `gorm:"type:numeric;not null" json:"applied_recorded_cost_sum"`
	AppliedCorrectedCostSum          string    `gorm:"type:numeric;not null" json:"applied_corrected_cost_sum"`
	AppliedUndercountSum             string    `gorm:"type:numeric;not null" json:"applied_undercount_sum"`
	AppliedCostBasisVersion          uint64    `gorm:"not null" json:"applied_cost_basis_version"`
	AppliedCostBasisSource           string    `gorm:"size:32;not null" json:"applied_cost_basis_source"`
	CreatedAt                        time.Time `gorm:"not null" json:"created_at"`
}

func (ChannelCostUnitRequestRestatement) TableName() string {
	return "channel_cost_unit_request_restatements"
}

type ChannelCostUnitRequestRestatementItem struct {
	ID                     uint64    `gorm:"primaryKey" json:"id"`
	RestatementID          uint64    `gorm:"not null" json:"restatement_id"`
	RequestLogID           uint64    `gorm:"not null" json:"request_log_id"`
	RequestCreatedAt       time.Time `gorm:"not null" json:"request_created_at"`
	RawCost                string    `gorm:"type:numeric;not null" json:"raw_cost"`
	RawGroupRatio          string    `gorm:"type:numeric;not null" json:"raw_group_ratio"`
	RawCompletionRatio     *string   `gorm:"type:numeric" json:"raw_completion_ratio,omitempty"`
	CostBefore             string    `gorm:"type:numeric;not null" json:"cost_before"`
	CostAfter              string    `gorm:"type:numeric;not null" json:"cost_after"`
	GroupRatioBefore       string    `gorm:"type:numeric;not null" json:"group_ratio_before"`
	GroupRatioAfter        string    `gorm:"type:numeric;not null" json:"group_ratio_after"`
	CompletionRatioBefore  string    `gorm:"type:numeric;not null" json:"completion_ratio_before"`
	CompletionRatioAfter   string    `gorm:"type:numeric;not null" json:"completion_ratio_after"`
	FundingFactorBefore    string    `gorm:"type:numeric;not null" json:"funding_factor_before"`
	FundingFactorAfter     string    `gorm:"type:numeric;not null" json:"funding_factor_after"`
	CostBasisVersionBefore uint64    `gorm:"not null" json:"cost_basis_version_before"`
	CostBasisVersionAfter  uint64    `gorm:"not null" json:"cost_basis_version_after"`
	CostBasisSourceBefore  string    `gorm:"size:32;not null" json:"cost_basis_source_before"`
	CostBasisSourceAfter   string    `gorm:"size:32;not null" json:"cost_basis_source_after"`
	CostSource             string    `gorm:"size:32;not null" json:"cost_source"`
	CostStatus             string    `gorm:"size:16;not null" json:"cost_status"`
	RouteReason            string    `gorm:"size:32;not null" json:"route_reason"`
	CreatedAt              time.Time `gorm:"not null" json:"created_at"`
}

func (ChannelCostUnitRequestRestatementItem) TableName() string {
	return "channel_cost_unit_request_restatement_items"
}

func NormalizeCostUnitRequestRestatementEvidence(evidence *CostUnitRequestRestatementEvidence) (*CostUnitRequestRestatementEvidence, error) {
	if evidence == nil {
		return nil, nil
	}
	normalized := *evidence
	normalized.OldFundingFactor = strings.TrimSpace(normalized.OldFundingFactor)
	normalized.NewFundingFactor = strings.TrimSpace(normalized.NewFundingFactor)
	normalized.ExpectedRawCostSum = strings.TrimSpace(normalized.ExpectedRawCostSum)
	normalized.ExpectedRecordedCostSum = strings.TrimSpace(normalized.ExpectedRecordedCostSum)
	normalized.ExpectedCorrectedCostSum = strings.TrimSpace(normalized.ExpectedCorrectedCostSum)
	normalized.ExpectedUndercountSum = strings.TrimSpace(normalized.ExpectedUndercountSum)
	normalized.ExpectedAlreadyCorrectRawCostSum = strings.TrimSpace(normalized.ExpectedAlreadyCorrectRawCostSum)
	normalized.ExpectedAllCostedRawCostSum = strings.TrimSpace(normalized.ExpectedAllCostedRawCostSum)
	normalized.ExpectedBucketDigest = strings.ToLower(strings.TrimSpace(normalized.ExpectedBucketDigest))
	normalized.AffectedStartedAt = normalized.AffectedStartedAt.UTC()
	normalized.AffectedEndedAt = normalized.AffectedEndedAt.UTC()
	normalized.ExpectedBucketStarts = append([]time.Time(nil), normalized.ExpectedBucketStarts...)
	for i := range normalized.ExpectedBucketStarts {
		normalized.ExpectedBucketStarts[i] = normalized.ExpectedBucketStarts[i].UTC()
	}
	sort.Slice(normalized.ExpectedBucketStarts, func(i, j int) bool {
		return normalized.ExpectedBucketStarts[i].Before(normalized.ExpectedBucketStarts[j])
	})
	if err := validateCostUnitRequestRestatementEvidenceShape(&normalized); err != nil {
		return nil, err
	}
	return &normalized, nil
}

func CostUnitRequestRestatementBucketDigest(bucketStarts []time.Time) (string, error) {
	if len(bucketStarts) == 0 {
		return "", ErrCostUnitRequestRestatementInvalid
	}
	var payload strings.Builder
	previous := time.Time{}
	for _, bucket := range bucketStarts {
		bucket = bucket.UTC()
		if bucket.IsZero() || bucket.Nanosecond() != 0 || bucket.Unix()%int64(gatewayRequestAggregateBucket/time.Second) != 0 ||
			(!previous.IsZero() && !bucket.After(previous)) {
			return "", ErrCostUnitRequestRestatementInvalid
		}
		payload.WriteString(bucket.Format(time.RFC3339Nano))
		payload.WriteByte('\n')
		previous = bucket
	}
	digest := sha256.Sum256([]byte(payload.String()))
	return hex.EncodeToString(digest[:]), nil
}

func validateCostUnitRequestRestatementEvidenceShape(evidence *CostUnitRequestRestatementEvidence) error {
	if evidence == nil || evidence.RequestLogHighWater == 0 || evidence.RequestLogHighWater > math.MaxInt64 ||
		evidence.AffectedStartedAt.IsZero() || evidence.AffectedEndedAt.IsZero() || evidence.AffectedStartedAt.After(evidence.AffectedEndedAt) ||
		evidence.AffectedStartedAt.Nanosecond()%1000 != 0 || evidence.AffectedEndedAt.Nanosecond()%1000 != 0 ||
		evidence.ExpectedPopulationRows <= 0 || evidence.ExpectedNewRatioRows < 0 || evidence.ExpectedOldRatioRows < 0 ||
		evidence.ExpectedRatioMismatchRows < 0 || evidence.ExpectedLatencyProbeRows < 0 || evidence.ExpectedRestatementRows <= 0 ||
		evidence.ExpectedAlreadyCorrectCostedRows < 0 ||
		evidence.ExpectedNewRatioRows+evidence.ExpectedOldRatioRows+evidence.ExpectedRatioMismatchRows != evidence.ExpectedPopulationRows ||
		evidence.ExpectedLatencyProbeRows > evidence.ExpectedPopulationRows || evidence.ExpectedRestatementRows > evidence.ExpectedNewRatioRows ||
		evidence.ExpectedAlreadyCorrectCostedRows > evidence.ExpectedOldRatioRows {
		return ErrCostUnitRequestRestatementInvalid
	}
	for _, decimal := range []string{
		evidence.OldFundingFactor, evidence.NewFundingFactor,
		evidence.ExpectedRawCostSum, evidence.ExpectedRecordedCostSum, evidence.ExpectedCorrectedCostSum,
		evidence.ExpectedUndercountSum, evidence.ExpectedAlreadyCorrectRawCostSum, evidence.ExpectedAllCostedRawCostSum,
	} {
		if len(decimal) == 0 || len(decimal) > 128 || !unsignedDecimalPattern.MatchString(decimal) {
			return ErrCostUnitRequestRestatementInvalid
		}
	}
	oldFactor, oldOK := new(big.Rat).SetString(evidence.OldFundingFactor)
	newFactor, newOK := new(big.Rat).SetString(evidence.NewFundingFactor)
	if !oldOK || !newOK || oldFactor.Sign() <= 0 || newFactor.Sign() <= 0 || oldFactor.Cmp(newFactor) == 0 {
		return ErrCostUnitRequestRestatementInvalid
	}
	rawCost, rawOK := new(big.Rat).SetString(evidence.ExpectedRawCostSum)
	recordedCost, recordedOK := new(big.Rat).SetString(evidence.ExpectedRecordedCostSum)
	correctedCost, correctedOK := new(big.Rat).SetString(evidence.ExpectedCorrectedCostSum)
	undercount, undercountOK := new(big.Rat).SetString(evidence.ExpectedUndercountSum)
	alreadyCorrectRaw, alreadyCorrectOK := new(big.Rat).SetString(evidence.ExpectedAlreadyCorrectRawCostSum)
	allCostedRaw, allCostedOK := new(big.Rat).SetString(evidence.ExpectedAllCostedRawCostSum)
	if !rawOK || !recordedOK || !correctedOK || !undercountOK || !alreadyCorrectOK || !allCostedOK ||
		new(big.Rat).Sub(correctedCost, recordedCost).Cmp(undercount) != 0 ||
		new(big.Rat).Add(rawCost, alreadyCorrectRaw).Cmp(allCostedRaw) != 0 {
		return ErrCostUnitRequestRestatementInvalid
	}
	digest, err := CostUnitRequestRestatementBucketDigest(evidence.ExpectedBucketStarts)
	if err != nil || digest != evidence.ExpectedBucketDigest {
		return ErrCostUnitRequestRestatementInvalid
	}
	firstBucket := aggregateBucketStart(evidence.AffectedStartedAt)
	lastBucket := aggregateBucketStart(evidence.AffectedEndedAt)
	if !evidence.ExpectedBucketStarts[0].Equal(firstBucket) ||
		!evidence.ExpectedBucketStarts[len(evidence.ExpectedBucketStarts)-1].Equal(lastBucket) {
		return ErrCostUnitRequestRestatementInvalid
	}
	return nil
}

type costUnitRequestRestatementPopulation struct {
	PopulationRows           int64
	NewRatioRows             int64
	OldRatioRows             int64
	RatioMismatchRows        int64
	LatencyProbeRows         int64
	RestatementRows          int64
	AlreadyCorrectCostedRows int64
	ResidualViolationRows    int64
	RawCostMatches           bool
	RecordedCostMatches      bool
	CorrectedCostMatches     bool
	UndercountMatches        bool
	AlreadyCorrectRawMatches bool
	AllCostedRawMatches      bool
	RawCostSum               string
	RecordedCostSum          string
	CorrectedCostSum         string
	UndercountSum            string
}

type costUnitRequestRestatementParent struct {
	RebaseID                *uint64
	RepairAdoptionID        *uint64
	ChannelID               uint
	ChannelIdentityRevision uint64
	IdempotencyKey          string
	RequestFingerprint      string
	Actor                   string
	CostSampledAtBefore     time.Time
	CostSampledAtAfter      time.Time
	OldFundingFactor        string
	NewFundingFactor        string
	AppliedCostBasisVersion uint64
	AppliedCostBasisSource  string
}

func applyCostUnitRequestRestatementTx(tx *gorm.DB, audit *ChannelCostUnitRebase, evidence *CostUnitRequestRestatementEvidence) error {
	if evidence == nil {
		return nil
	}
	if tx == nil || audit == nil || audit.ID == 0 || audit.ChannelID == 0 || audit.ChannelIdentityRevision == 0 ||
		audit.CostSampledAtBefore == nil || audit.CostSampledAtBefore.IsZero() || audit.CostSampledAtAfter.IsZero() ||
		!evidence.AffectedStartedAt.After(audit.CostSampledAtBefore.UTC()) || evidence.AffectedEndedAt.After(audit.CostSampledAtAfter.UTC()) ||
		strings.TrimSpace(audit.StateSourceAfter) == "" || audit.AppliedStateVersion == 0 {
		return ErrCostUnitRequestRestatementInvalid
	}
	normalized, err := NormalizeCostUnitRequestRestatementEvidence(evidence)
	if err != nil {
		return err
	}
	var factorsMatch bool
	if err := tx.Raw(`
SELECT funding_factor_before = CAST(? AS numeric)
   AND funding_factor_after = CAST(? AS numeric)
FROM public.channel_cost_unit_rebases
WHERE id = ? AND channel_id = ?`,
		normalized.OldFundingFactor, normalized.NewFundingFactor, audit.ID, audit.ChannelID).
		Row().Scan(&factorsMatch); err != nil {
		return err
	}
	if !factorsMatch {
		return ErrCostUnitRequestRestatementInvalid
	}
	rebaseID := audit.ID
	return applyCostUnitRequestRestatementForParentTx(tx, costUnitRequestRestatementParent{
		RebaseID: &rebaseID, ChannelID: audit.ChannelID,
		ChannelIdentityRevision: audit.ChannelIdentityRevision,
		IdempotencyKey:          audit.IdempotencyKey, RequestFingerprint: audit.RequestFingerprint, Actor: audit.Actor,
		CostSampledAtBefore: audit.CostSampledAtBefore.UTC(), CostSampledAtAfter: audit.CostSampledAtAfter.UTC(),
		OldFundingFactor: normalized.OldFundingFactor, NewFundingFactor: normalized.NewFundingFactor,
		AppliedCostBasisVersion: audit.AppliedStateVersion, AppliedCostBasisSource: audit.StateSourceAfter,
	}, normalized)
}

func applyCostUnitRequestRestatementForParentTx(tx *gorm.DB, parent costUnitRequestRestatementParent, evidence *CostUnitRequestRestatementEvidence) error {
	if tx == nil || (parent.RebaseID == nil) == (parent.RepairAdoptionID == nil) ||
		parent.ChannelID == 0 || parent.ChannelIdentityRevision == 0 ||
		strings.TrimSpace(parent.IdempotencyKey) == "" || !isLowerHexFingerprint(parent.RequestFingerprint) ||
		strings.TrimSpace(parent.Actor) == "" || parent.CostSampledAtBefore.IsZero() || parent.CostSampledAtAfter.IsZero() ||
		parent.AppliedCostBasisVersion == 0 || strings.TrimSpace(parent.AppliedCostBasisSource) == "" {
		return ErrCostUnitRequestRestatementInvalid
	}
	normalized, err := NormalizeCostUnitRequestRestatementEvidence(evidence)
	if err != nil || normalized == nil || normalized.OldFundingFactor != parent.OldFundingFactor || normalized.NewFundingFactor != parent.NewFundingFactor ||
		!normalized.AffectedStartedAt.After(parent.CostSampledAtBefore.UTC()) || normalized.AffectedEndedAt.After(parent.CostSampledAtAfter.UTC()) {
		return ErrCostUnitRequestRestatementInvalid
	}

	// The aggregate backfill owns the singleton before locking request rows.
	// Following that order prevents the rare correction path from deadlocking
	// its bounded rebuild worker.
	if _, err := lockGatewayRequestGroupCacheBackfillStateTx(tx); err != nil {
		return err
	}
	// EXCLUSIVE also blocks new SELECT FOR UPDATE holders. SHARE would allow a
	// reconciler to lock a request row and then deadlock while upgrading its
	// table lock behind this transaction.
	if err := tx.Exec("LOCK TABLE public.gateway_request_logs IN EXCLUSIVE MODE").Error; err != nil {
		return err
	}
	var currentHighWater uint64
	if err := tx.Raw("SELECT COALESCE(max(id), 0) FROM public.gateway_request_logs").Scan(&currentHighWater).Error; err != nil {
		return err
	}
	if currentHighWater < normalized.RequestLogHighWater {
		return ErrCostUnitRequestRestatementConflict
	}
	var latePopulationRows int64
	if err := tx.Raw(`
SELECT count(*)
FROM public.gateway_request_logs
WHERE id > ?
  AND channel_id = ?
  AND channel_identity_revision = ?
  AND created_at >= ?
  AND created_at <= ?
  AND price_fact_id IS NULL
  AND funding_factor = CAST(? AS numeric)`,
		normalized.RequestLogHighWater, parent.ChannelID, parent.ChannelIdentityRevision,
		normalized.AffectedStartedAt, normalized.AffectedEndedAt,
		normalized.OldFundingFactor).Scan(&latePopulationRows).Error; err != nil {
		return err
	}
	if latePopulationRows != 0 {
		return ErrCostUnitRequestRestatementConflict
	}

	population, err := loadCostUnitRequestRestatementPopulationTx(tx, parent.ChannelID, parent.ChannelIdentityRevision, normalized)
	if err != nil {
		return err
	}
	if !costUnitRequestRestatementPopulationMatches(population, normalized) {
		return ErrCostUnitRequestRestatementConflict
	}
	buckets, err := loadCostUnitRequestRestatementBucketsTx(tx, parent.ChannelID, parent.ChannelIdentityRevision, normalized)
	if err != nil {
		return err
	}
	if !sameTimeList(buckets, normalized.ExpectedBucketStarts) {
		return ErrCostUnitRequestRestatementConflict
	}

	bucketJSON, err := json.Marshal(normalized.ExpectedBucketStarts)
	if err != nil {
		return err
	}
	operation := ChannelCostUnitRequestRestatement{
		RebaseID: parent.RebaseID, RepairAdoptionID: parent.RepairAdoptionID,
		ChannelID: parent.ChannelID, ChannelIdentityRevision: parent.ChannelIdentityRevision,
		IdempotencyKey: parent.IdempotencyKey, RequestFingerprint: parent.RequestFingerprint, Actor: parent.Actor,
		RequestLogHighWater: normalized.RequestLogHighWater,
		CostSampledAtBefore: parent.CostSampledAtBefore.UTC(), CostSampledAtAfter: parent.CostSampledAtAfter.UTC(),
		AffectedStartedAt: normalized.AffectedStartedAt, AffectedEndedAt: normalized.AffectedEndedAt,
		OldFundingFactor: normalized.OldFundingFactor, NewFundingFactor: normalized.NewFundingFactor,
		ExpectedPopulationRows: normalized.ExpectedPopulationRows,
		ExpectedNewRatioRows:   normalized.ExpectedNewRatioRows, ExpectedOldRatioRows: normalized.ExpectedOldRatioRows,
		ExpectedRatioMismatchRows: normalized.ExpectedRatioMismatchRows, ExpectedLatencyProbeRows: normalized.ExpectedLatencyProbeRows,
		ExpectedRestatementRows:          normalized.ExpectedRestatementRows,
		ExpectedAlreadyCorrectCostedRows: normalized.ExpectedAlreadyCorrectCostedRows,
		ExpectedRawCostSum:               normalized.ExpectedRawCostSum, ExpectedRecordedCostSum: normalized.ExpectedRecordedCostSum,
		ExpectedCorrectedCostSum: normalized.ExpectedCorrectedCostSum, ExpectedUndercountSum: normalized.ExpectedUndercountSum,
		ExpectedAlreadyCorrectRawCostSum: normalized.ExpectedAlreadyCorrectRawCostSum,
		ExpectedAllCostedRawCostSum:      normalized.ExpectedAllCostedRawCostSum,
		ExpectedBucketCount:              len(normalized.ExpectedBucketStarts), ExpectedBucketStartsJSON: string(bucketJSON),
		ExpectedBucketDigest:   normalized.ExpectedBucketDigest,
		AppliedRestatementRows: normalized.ExpectedRestatementRows,
		AppliedRawCostSum:      population.RawCostSum, AppliedRecordedCostSum: population.RecordedCostSum,
		AppliedCorrectedCostSum: population.CorrectedCostSum, AppliedUndercountSum: population.UndercountSum,
		AppliedCostBasisVersion: parent.AppliedCostBasisVersion, AppliedCostBasisSource: parent.AppliedCostBasisSource,
	}
	if err := tx.Create(&operation).Error; err != nil {
		return fmt.Errorf("append request restatement operation: %w", err)
	}
	if err := appendCostUnitRequestRestatementItemsTx(tx, operation.ID, parent, normalized); err != nil {
		return err
	}
	preserveAggregateMarkers, err := costUnitRequestRestatementSharesBucketWithPriceFactsTx(tx, parent.ChannelID, normalized)
	if err != nil {
		return err
	}
	if err := updateCostUnitRequestRestatementRowsTx(tx, operation.ID, parent, normalized); err != nil {
		return err
	}
	if err := verifyAppliedCostUnitRequestRestatementTx(tx, operation.ID, parent, normalized); err != nil {
		return err
	}
	if preserveAggregateMarkers {
		return rebuildCostUnitRequestRestatementAggregatesTx(tx, parent.ChannelID, normalized)
	}
	endExclusive := normalized.AffectedEndedAt.Add(time.Nanosecond)
	if !endExclusive.After(normalized.AffectedEndedAt) {
		return ErrCostUnitRequestRestatementInvalid
	}
	return invalidateRequestAggregatesForRangeTx(tx, parent.ChannelID, normalized.AffectedStartedAt, endExclusive)
}

func costUnitRequestRestatementSharesBucketWithPriceFactsTx(tx *gorm.DB, channelID uint, evidence *CostUnitRequestRestatementEvidence) (bool, error) {
	if tx == nil || channelID == 0 || evidence == nil {
		return false, ErrCostUnitRequestRestatementInvalid
	}
	endExclusive := evidence.AffectedEndedAt.Add(time.Nanosecond)
	if !endExclusive.After(evidence.AffectedEndedAt) {
		return false, ErrCostUnitRequestRestatementInvalid
	}
	bucketStart := aggregateBucketStart(evidence.AffectedStartedAt)
	bucketEnd := aggregateBucketCeil(endExclusive)
	var exists bool
	err := tx.Raw(`
SELECT EXISTS (
    SELECT 1
    FROM public.gateway_request_logs
    WHERE channel_id = ?
      AND created_at >= ?
      AND created_at < ?
      AND price_fact_id IS NOT NULL
)`, channelID, bucketStart, bucketEnd).Scan(&exists).Error
	return exists, err
}

func rebuildCostUnitRequestRestatementAggregatesTx(tx *gorm.DB, channelID uint, evidence *CostUnitRequestRestatementEvidence) error {
	if tx == nil || channelID == 0 || evidence == nil {
		return ErrCostUnitRequestRestatementInvalid
	}
	endExclusive := evidence.AffectedEndedAt.Add(time.Nanosecond)
	if !endExclusive.After(evidence.AffectedEndedAt) {
		return ErrCostUnitRequestRestatementInvalid
	}
	bucketStart := aggregateBucketStart(evidence.AffectedStartedAt)
	bucketEnd := aggregateBucketCeil(endExclusive)
	logScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at < ?", channelID, bucketStart, bucketEnd)
	aggregateScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestAggregate{}).
		Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ?", channelID, bucketStart, bucketEnd)
	if err := ensureRequestAggregateResetVersionCompatible(logScope, aggregateScope); err != nil {
		return err
	}
	groupLogScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at < ?", channelID, bucketStart, bucketEnd)
	if err := ensureGatewayRequestGroupCacheLogResetVersionCompatible(groupLogScope); err != nil {
		return err
	}
	groupScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestGroupCacheAggregate{}).
		Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ?", channelID, bucketStart, bucketEnd)
	if err := ensureGatewayRequestGroupCacheResetVersionCompatible(groupScope); err != nil {
		return err
	}
	state, err := lockGatewayRequestGroupCacheBackfillStateTx(tx)
	if err != nil {
		return err
	}
	if state.TargetVersion > gatewayRequestGroupCacheAggregateVersion {
		return errGatewayRequestGroupCacheAggregateVersionConflict
	}
	if state.TargetVersion != gatewayRequestGroupCacheAggregateVersion || !state.Completed {
		return ErrCostUnitRequestRestatementConflict
	}
	var pendingRows int64
	if err := tx.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at < ?", channelID, bucketStart, bucketEnd).
		Where("(aggregate_version <> ? OR group_cache_aggregate_version <> ?)", gatewayRequestAggregateVersion, gatewayRequestGroupCacheAggregateVersion).
		Count(&pendingRows).Error; err != nil {
		return err
	}
	if pendingRows != 0 {
		return ErrCostUnitRequestRestatementConflict
	}
	var logs []GatewayRequestLog
	if err := tx.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at < ?", channelID, bucketStart, bucketEnd).
		Order("id ASC").Find(&logs).Error; err != nil {
		return err
	}
	if err := tx.Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ? AND version <= ?", channelID, bucketStart, bucketEnd, gatewayRequestAggregateVersion).
		Delete(&GatewayRequestAggregate{}).Error; err != nil {
		return err
	}
	if err := tx.Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ? AND version <= ?", channelID, bucketStart, bucketEnd, gatewayRequestGroupCacheAggregateVersion).
		Delete(&GatewayRequestGroupCacheAggregate{}).Error; err != nil {
		return err
	}
	for i := range logs {
		if row, ok := requestAggregateFromLog(&logs[i]); ok {
			if err := upsertRequestAggregateTx(tx, row); err != nil {
				return err
			}
		}
		if row, ok := requestGroupCacheAggregateFromLog(&logs[i]); ok {
			if err := upsertGatewayRequestGroupCacheAggregateTx(tx, row); err != nil {
				return err
			}
		}
	}
	return nil
}

func loadCostUnitRequestRestatementPopulationTx(tx *gorm.DB, channelID uint, channelIdentityRevision uint64, evidence *CostUnitRequestRestatementEvidence) (costUnitRequestRestatementPopulation, error) {
	var population costUnitRequestRestatementPopulation
	query := `
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
    round(COALESCE(sum(raw_cost) FILTER (WHERE should_restate), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS raw_cost_matches,
    round(COALESCE(sum(cost) FILTER (WHERE should_restate), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS recorded_cost_matches,
    round(COALESCE(sum(raw_cost * CAST(? AS numeric)) FILTER (WHERE should_restate), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS corrected_cost_matches,
    round(COALESCE(sum(raw_cost * CAST(? AS numeric) - cost) FILTER (WHERE should_restate), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS undercount_matches,
    round(COALESCE(sum(raw_cost) FILTER (WHERE raw_cost IS NOT NULL AND COALESCE(route_reason, '') <> 'latency_probe' AND is_old_ratio), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS already_correct_raw_matches,
    round(COALESCE(sum(raw_cost) FILTER (WHERE raw_cost IS NOT NULL AND COALESCE(route_reason, '') <> 'latency_probe'), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS all_costed_raw_matches,
    COALESCE(sum(raw_cost) FILTER (WHERE should_restate), 0)::text AS raw_cost_sum,
    COALESCE(sum(cost) FILTER (WHERE should_restate), 0)::text AS recorded_cost_sum,
    COALESCE(sum(raw_cost * CAST(? AS numeric)) FILTER (WHERE should_restate), 0)::text AS corrected_cost_sum,
    COALESCE(sum(raw_cost * CAST(? AS numeric) - cost) FILTER (WHERE should_restate), 0)::text AS undercount_sum
FROM classified`
	err := tx.Raw(query,
		evidence.OldFundingFactor, channelID, channelIdentityRevision, evidence.RequestLogHighWater, evidence.AffectedStartedAt, evidence.AffectedEndedAt, evidence.OldFundingFactor,
		evidence.OldFundingFactor,
		evidence.ExpectedRawCostSum, evidence.ExpectedRawCostSum,
		evidence.ExpectedRecordedCostSum, evidence.ExpectedRecordedCostSum,
		evidence.NewFundingFactor, evidence.ExpectedCorrectedCostSum, evidence.ExpectedCorrectedCostSum,
		evidence.NewFundingFactor, evidence.ExpectedUndercountSum, evidence.ExpectedUndercountSum,
		evidence.ExpectedAlreadyCorrectRawCostSum, evidence.ExpectedAlreadyCorrectRawCostSum,
		evidence.ExpectedAllCostedRawCostSum, evidence.ExpectedAllCostedRawCostSum,
		evidence.NewFundingFactor, evidence.NewFundingFactor,
	).Scan(&population).Error
	return population, err
}

func costUnitRequestRestatementPopulationMatches(population costUnitRequestRestatementPopulation, evidence *CostUnitRequestRestatementEvidence) bool {
	return evidence != nil && population.PopulationRows == evidence.ExpectedPopulationRows &&
		population.NewRatioRows == evidence.ExpectedNewRatioRows && population.OldRatioRows == evidence.ExpectedOldRatioRows &&
		population.RatioMismatchRows == evidence.ExpectedRatioMismatchRows && population.LatencyProbeRows == evidence.ExpectedLatencyProbeRows &&
		population.RestatementRows == evidence.ExpectedRestatementRows &&
		population.AlreadyCorrectCostedRows == evidence.ExpectedAlreadyCorrectCostedRows && population.ResidualViolationRows == 0 &&
		population.RawCostMatches && population.RecordedCostMatches && population.CorrectedCostMatches &&
		population.UndercountMatches && population.AlreadyCorrectRawMatches && population.AllCostedRawMatches
}

func loadCostUnitRequestRestatementBucketsTx(tx *gorm.DB, channelID uint, channelIdentityRevision uint64, evidence *CostUnitRequestRestatementEvidence) ([]time.Time, error) {
	var rows []struct{ BucketStart time.Time }
	err := tx.Raw(`
SELECT DISTINCT to_timestamp(floor(extract(epoch FROM created_at) / 900) * 900) AS bucket_start
FROM public.gateway_request_logs
WHERE channel_id = ?
  AND channel_identity_revision = ?
  AND id <= ?
  AND created_at >= ?
  AND created_at <= ?
  AND price_fact_id IS NULL
  AND funding_factor = CAST(? AS numeric)
ORDER BY bucket_start`, channelID, channelIdentityRevision, evidence.RequestLogHighWater, evidence.AffectedStartedAt, evidence.AffectedEndedAt, evidence.OldFundingFactor).
		Scan(&rows).Error
	buckets := make([]time.Time, len(rows))
	for i := range rows {
		buckets[i] = rows[i].BucketStart.UTC()
	}
	return buckets, err
}

func sameTimeList(left, right []time.Time) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !left[i].Equal(right[i]) {
			return false
		}
	}
	return true
}

func appendCostUnitRequestRestatementItemsTx(tx *gorm.DB, operationID uint64, parent costUnitRequestRestatementParent, evidence *CostUnitRequestRestatementEvidence) error {
	result := tx.Exec(`
INSERT INTO public.channel_cost_unit_request_restatement_items (
    restatement_id, request_log_id, request_created_at,
    raw_cost, raw_group_ratio, raw_completion_ratio,
    cost_before, cost_after, group_ratio_before, group_ratio_after,
    completion_ratio_before, completion_ratio_after,
    funding_factor_before, funding_factor_after,
    cost_basis_version_before, cost_basis_version_after,
    cost_basis_source_before, cost_basis_source_after,
    cost_source, cost_status, route_reason
)
SELECT
    ?, logs.id, logs.created_at,
    logs.raw_cost, logs.raw_group_ratio, logs.raw_completion_ratio,
    logs.cost, logs.raw_cost * CAST(? AS numeric),
    logs.group_ratio, logs.raw_group_ratio * CAST(? AS numeric),
    logs.completion_ratio,
    CASE WHEN logs.raw_completion_ratio IS NULL THEN logs.completion_ratio ELSE logs.raw_completion_ratio * CAST(? AS numeric) END,
    logs.funding_factor, CAST(? AS numeric),
    logs.cost_basis_version, ?,
    COALESCE(logs.cost_basis_source, ''), ?,
    COALESCE(logs.cost_source, ''), logs.cost_status, COALESCE(logs.route_reason, '')
FROM public.gateway_request_logs AS logs
WHERE logs.channel_id = ?
  AND logs.channel_identity_revision = ?
  AND logs.id <= ?
  AND logs.created_at >= ?
  AND logs.created_at <= ?
  AND logs.price_fact_id IS NULL
  AND logs.funding_factor = CAST(? AS numeric)
  AND logs.raw_cost IS NOT NULL
  AND COALESCE(logs.route_reason, '') <> 'latency_probe'
  AND logs.raw_group_ratio IS NOT NULL
  AND logs.raw_group_ratio = logs.group_ratio
ORDER BY logs.id
FOR UPDATE`, operationID,
		evidence.NewFundingFactor, evidence.NewFundingFactor, evidence.NewFundingFactor, evidence.NewFundingFactor,
		parent.AppliedCostBasisVersion, parent.AppliedCostBasisSource,
		parent.ChannelID, parent.ChannelIdentityRevision, evidence.RequestLogHighWater, evidence.AffectedStartedAt, evidence.AffectedEndedAt, evidence.OldFundingFactor)
	if result.Error != nil {
		return fmt.Errorf("freeze request restatement rows: %w", result.Error)
	}
	if result.RowsAffected != evidence.ExpectedRestatementRows {
		return ErrCostUnitRequestRestatementConflict
	}
	return nil
}

func updateCostUnitRequestRestatementRowsTx(tx *gorm.DB, operationID uint64, parent costUnitRequestRestatementParent, evidence *CostUnitRequestRestatementEvidence) error {
	result := tx.Exec(`
UPDATE public.gateway_request_logs AS logs
SET cost = item.cost_after,
    group_ratio = item.group_ratio_after,
    completion_ratio = item.completion_ratio_after,
    funding_factor = item.funding_factor_after,
    cost_basis_version = item.cost_basis_version_after,
    cost_basis_source = item.cost_basis_source_after
FROM public.channel_cost_unit_request_restatement_items AS item
WHERE item.restatement_id = ?
  AND logs.id = item.request_log_id
  AND logs.channel_id = ?
  AND logs.channel_identity_revision = ?
  AND logs.id <= ?
  AND logs.price_fact_id IS NULL
  AND logs.created_at = item.request_created_at
  AND logs.raw_cost = item.raw_cost
  AND logs.raw_group_ratio = item.raw_group_ratio
  AND logs.raw_completion_ratio IS NOT DISTINCT FROM item.raw_completion_ratio
  AND logs.cost = item.cost_before
  AND logs.group_ratio = item.group_ratio_before
  AND logs.completion_ratio = item.completion_ratio_before
  AND logs.funding_factor = item.funding_factor_before
  AND logs.cost_basis_version = item.cost_basis_version_before
  AND COALESCE(logs.cost_basis_source, '') = item.cost_basis_source_before
  AND COALESCE(logs.cost_source, '') = item.cost_source
  AND logs.cost_status::text = item.cost_status
	  AND COALESCE(logs.route_reason, '') = item.route_reason`, operationID, parent.ChannelID, parent.ChannelIdentityRevision, evidence.RequestLogHighWater)
	if result.Error != nil {
		return fmt.Errorf("apply request restatement rows: %w", result.Error)
	}
	if result.RowsAffected != evidence.ExpectedRestatementRows {
		return ErrCostUnitRequestRestatementConflict
	}
	return nil
}

func verifyAppliedCostUnitRequestRestatementTx(tx *gorm.DB, operationID uint64, parent costUnitRequestRestatementParent, evidence *CostUnitRequestRestatementEvidence) error {
	var verified struct {
		Rows              int64
		InvariantErrors   int64
		RawCostMatches    bool
		CostMatches       bool
		UndercountMatches bool
	}
	err := tx.Raw(`
SELECT
    count(*) AS rows,
    count(*) FILTER (WHERE
        logs.raw_cost IS DISTINCT FROM item.raw_cost
        OR logs.raw_group_ratio IS DISTINCT FROM item.raw_group_ratio
        OR logs.raw_completion_ratio IS DISTINCT FROM item.raw_completion_ratio
        OR logs.cost IS DISTINCT FROM item.cost_after
        OR logs.group_ratio IS DISTINCT FROM item.group_ratio_after
        OR logs.completion_ratio IS DISTINCT FROM item.completion_ratio_after
        OR logs.funding_factor IS DISTINCT FROM item.funding_factor_after
        OR logs.cost_basis_version <> item.cost_basis_version_after
        OR COALESCE(logs.cost_basis_source, '') <> item.cost_basis_source_after
        OR COALESCE(logs.cost_source, '') <> item.cost_source
        OR logs.cost_status::text <> item.cost_status
        OR COALESCE(logs.route_reason, '') <> item.route_reason
    ) AS invariant_errors,
    round(COALESCE(sum(logs.raw_cost), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS raw_cost_matches,
    round(COALESCE(sum(logs.cost), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS cost_matches,
    round(COALESCE(sum(item.cost_after - item.cost_before), 0), scale(CAST(? AS numeric))) = CAST(? AS numeric) AS undercount_matches
FROM public.channel_cost_unit_request_restatement_items AS item
JOIN public.gateway_request_logs AS logs ON logs.id = item.request_log_id
WHERE item.restatement_id = ?
  AND logs.channel_id = ?
  AND logs.channel_identity_revision = ?
  AND logs.price_fact_id IS NULL`,
		evidence.ExpectedRawCostSum, evidence.ExpectedRawCostSum,
		evidence.ExpectedCorrectedCostSum, evidence.ExpectedCorrectedCostSum,
		evidence.ExpectedUndercountSum, evidence.ExpectedUndercountSum, operationID, parent.ChannelID, parent.ChannelIdentityRevision).
		Scan(&verified).Error
	if err != nil {
		return err
	}
	if verified.Rows != evidence.ExpectedRestatementRows || verified.InvariantErrors != 0 ||
		!verified.RawCostMatches || !verified.CostMatches || !verified.UndercountMatches {
		return ErrCostUnitRequestRestatementConflict
	}
	return nil
}
