package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var channelCostUnitRebaseIdempotencyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

const channelCostUnitRebaseBodyLimit = 16 << 10

type channelCostRate struct {
	GroupName                string  `json:"group_name"`
	RawRatio                 float64 `json:"raw_ratio"`
	RawCompletionRatio       float64 `json:"raw_completion_ratio"`
	EffectiveRatio           float64 `json:"effective_ratio"`
	EffectiveCompletionRatio float64 `json:"effective_completion_ratio"`
}

type channelCostProfileResponse struct {
	State             *storage.ChannelCostState         `json:"state"`
	Events            []storage.ChannelFundingEvent     `json:"events"`
	Total             int64                             `json:"total"`
	Pending           []storage.ChannelFundingCandidate `json:"pending"`
	PendingTotal      int64                             `json:"pending_total"`
	Rates             []channelCostRate                 `json:"rates"`
	CostUnitRebase    channelCostUnitRebaseReadiness    `json:"cost_unit_rebase"`
	CostBasisOverride channelCostBasisOverrideReadiness `json:"cost_basis_override"`
	Resolution        string                            `json:"candidate_resolution,omitempty"`
}

const (
	channelAccountingReviewProviderFactorChanged = "provider_factor_changed"
	channelAccountingReviewFundingBasisMissing   = "funding_basis_missing"
	channelAccountingReviewUnclassified          = "accounting_review"
)

type channelCostUnitRebaseReadiness struct {
	Supported              bool     `json:"supported"`
	Eligible               bool     `json:"eligible"`
	Blockers               []string `json:"blockers"`
	ExpectedVersion        uint64   `json:"expected_version"`
	ExpectedCostUnitEpoch  uint64   `json:"expected_cost_unit_epoch"`
	MultiplierMode         string   `json:"multiplier_mode"`
	PreviousMultiplier     *float64 `json:"previous_multiplier,omitempty"`
	PreviousProviderFactor *float64 `json:"previous_provider_factor,omitempty"`
}

type channelCostBasisOverrideReadiness struct {
	Eligible                 bool     `json:"eligible"`
	Blockers                 []string `json:"blockers"`
	ExpectedVersion          uint64   `json:"expected_version"`
	ExpectedIdentityRevision uint64   `json:"expected_identity_revision"`
	ExpectedCostUnitEpoch    uint64   `json:"expected_cost_unit_epoch"`
	CurrentFundingFactor     float64  `json:"current_funding_factor"`
	PendingFundingCandidates int64    `json:"pending_funding_candidates"`
}

func buildChannelCostBasisOverrideReadiness(channel *storage.Channel, state *storage.ChannelCostState, pendingTotal int64) channelCostBasisOverrideReadiness {
	readiness := channelCostBasisOverrideReadiness{Blockers: []string{}}
	if channel == nil || state == nil || channel.ID == 0 || state.ChannelID != channel.ID {
		readiness.Blockers = append(readiness.Blockers, "accounting_state_unavailable")
		return readiness
	}
	readiness.ExpectedVersion = state.Version
	readiness.ExpectedIdentityRevision = channel.UpstreamAccountIdentityRevision
	readiness.ExpectedCostUnitEpoch = state.CostUnitEpoch
	readiness.CurrentFundingFactor = state.FundingFactor
	readiness.PendingFundingCandidates = pendingTotal
	if channel.DeletionPending {
		readiness.Blockers = append(readiness.Blockers, "channel_deletion_pending")
	}
	if state.Version == 0 || channel.UpstreamAccountIdentityRevision == 0 || state.CostUnitEpoch == 0 || channel.CostUnitEpoch != state.CostUnitEpoch {
		readiness.Blockers = append(readiness.Blockers, "accounting_fence_unavailable")
	}
	if !finiteNonNegativeAPIValue(state.FundingFactor) {
		readiness.Blockers = append(readiness.Blockers, "current_funding_factor_invalid")
	}
	readiness.Eligible = len(readiness.Blockers) == 0
	return readiness
}

func finiteNonNegativeAPIValue(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func channelAccountingReviewReason(state *storage.ChannelCostState) string {
	if state == nil || strings.TrimSpace(state.Status) != costing.StateStatusNeedsReview {
		return ""
	}
	if strings.Contains(state.LastError, costing.ErrCostUnitChangeRequiresRebase.Error()) {
		return channelAccountingReviewProviderFactorChanged
	}
	if state.ObservedProviderFactor == nil || strings.TrimSpace(state.Source) == "legacy_unknown" {
		return channelAccountingReviewFundingBasisMissing
	}
	return channelAccountingReviewUnclassified
}

func buildChannelCostUnitRebaseReadiness(channel *storage.Channel, state *storage.ChannelCostState, pendingTotal int64) channelCostUnitRebaseReadiness {
	readiness := channelCostUnitRebaseReadiness{Blockers: []string{}}
	if channel == nil || state == nil || channel.ID == 0 || state.ChannelID != channel.ID {
		readiness.Blockers = append(readiness.Blockers, "accounting_state_unavailable")
		return readiness
	}

	readiness.ExpectedVersion = state.Version
	readiness.ExpectedCostUnitEpoch = state.CostUnitEpoch
	readiness.MultiplierMode = connector.NormalizeRechargeMultiplierMode(channel.RechargeMultiplierMode)
	readiness.Supported = channel.Type == storage.ChannelTypeSub2API && channel.RechargeMultiplier == nil

	if channel.Type != storage.ChannelTypeSub2API {
		readiness.Blockers = append(readiness.Blockers, "provider_not_supported")
	}
	if channel.RechargeMultiplier != nil {
		readiness.Blockers = append(readiness.Blockers, "manual_multiplier_configured")
	}
	if strings.TrimSpace(state.Status) != costing.StateStatusNeedsReview {
		readiness.Blockers = append(readiness.Blockers, "accounting_review_not_active")
	} else if channelAccountingReviewReason(state) != channelAccountingReviewProviderFactorChanged {
		readiness.Blockers = append(readiness.Blockers, "review_reason_not_rebasable")
	}
	if pendingTotal > 0 {
		readiness.Blockers = append(readiness.Blockers, "pending_funding_candidates")
	}
	if state.Version == 0 || state.CostUnitEpoch == 0 {
		readiness.Blockers = append(readiness.Blockers, "accounting_fence_unavailable")
	}
	if state.BalanceSampledAt == nil || state.CostSampledAt == nil {
		readiness.Blockers = append(readiness.Blockers, "financial_anchors_incomplete")
	}
	if state.ObservedProviderFactor == nil || !finitePositiveAPIValue(*state.ObservedProviderFactor) || strings.TrimSpace(state.ObservedProviderFactorSource) != "advertised" {
		readiness.Blockers = append(readiness.Blockers, "previous_provider_factor_unavailable")
	} else {
		providerFactor := *state.ObservedProviderFactor
		readiness.PreviousProviderFactor = &providerFactor
		previousMultiplier := providerFactor
		if readiness.MultiplierMode == connector.RechargeMultiplierModeDivide {
			previousMultiplier = 1 / providerFactor
		}
		if finitePositiveAPIValue(previousMultiplier) {
			readiness.PreviousMultiplier = &previousMultiplier
		} else {
			readiness.Blockers = append(readiness.Blockers, "previous_multiplier_unavailable")
		}
	}

	readiness.Eligible = readiness.Supported && len(readiness.Blockers) == 0
	return readiness
}

func finitePositiveAPIValue(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func channelCostProfile(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}
	profile, err := loadChannelCostProfile(d, id, pageSize, (page-1)*pageSize)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": profile})
}

type channelCostUnitRebaseInput struct {
	ExpectedVersion    uint64                                      `json:"expected_version"`
	PreviousMultiplier float64                                     `json:"previous_multiplier"`
	CurrentMultiplier  float64                                     `json:"current_multiplier"`
	Note               string                                      `json:"note"`
	RequestRestatement *storage.CostUnitRequestRestatementEvidence `json:"request_restatement,omitempty"`
}

func rebaseChannelCostUnit(c *gin.Context, d *Deps) {
	if d == nil || d.CostUnitRebaser == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("cost unit rebase is unavailable"))
		return
	}
	channelID, err := uintParam(c, "id")
	if err != nil || channelID == 0 {
		fail(c, http.StatusBadRequest, errors.New("channel id is invalid"))
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if !channelCostUnitRebaseIdempotencyPattern.MatchString(idempotencyKey) {
		fail(c, http.StatusBadRequest, errors.New("a valid Idempotency-Key header is required"))
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}

	var input channelCostUnitRebaseInput
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, channelCostUnitRebaseBodyLimit)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		failChannelCostUnitRebaseDecode(c, err)
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		failChannelCostUnitRebaseDecode(c, err)
		return
	}
	input.Note = strings.TrimSpace(input.Note)
	if input.ExpectedVersion == 0 || input.PreviousMultiplier <= 0 || input.CurrentMultiplier <= 0 || input.PreviousMultiplier == input.CurrentMultiplier ||
		actor == "" || len(actor) > 128 || len(input.Note) > 1024 {
		fail(c, http.StatusBadRequest, costing.ErrCostUnitRebaseCommandInvalid)
		return
	}

	result, err := d.CostUnitRebaser.RebaseCostUnit(c.Request.Context(), channelID, costing.CostUnitRebaseCommand{
		ExpectedVersion:    input.ExpectedVersion,
		PreviousMultiplier: input.PreviousMultiplier, CurrentMultiplier: input.CurrentMultiplier,
		IdempotencyKey: idempotencyKey, Actor: actor, Note: input.Note,
		RequestRestatement: input.RequestRestatement,
	})
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		fail(c, http.StatusNotFound, errors.New("channel or cost state was not found"))
		return
	case errors.Is(err, storage.ErrCostStateConflict),
		errors.Is(err, storage.ErrCostUnitEpochConflict),
		errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision),
		errors.Is(err, storage.ErrStaleUpstreamAccountObservation),
		errors.Is(err, storage.ErrPendingFundingCandidate),
		errors.Is(err, storage.ErrCostUnitRebaseIdempotencyConflict),
		errors.Is(err, storage.ErrCostUnitRequestRestatementConflict):
		fail(c, http.StatusConflict, stableCostUnitRebaseError(err))
		return
	case errors.Is(err, costing.ErrCostUnitRebaseCommandInvalid):
		fail(c, http.StatusBadRequest, costing.ErrCostUnitRebaseCommandInvalid)
		return
	case errors.Is(err, costing.ErrCostUnitRebaseEvidenceMismatch),
		errors.Is(err, costing.ErrCostUnitRebaseNotRequired),
		errors.Is(err, costing.ErrCostUnitRebaseUnexplainedRegression),
		errors.Is(err, storage.ErrCostUnitRebaseInvalid),
		errors.Is(err, storage.ErrCostUnitRequestRestatementInvalid):
		fail(c, http.StatusUnprocessableEntity, stableCostUnitRebaseError(err))
		return
	case errors.Is(err, costing.ErrCostUnitRebaseUpstream):
		fail(c, http.StatusBadGateway, errors.New("failed to collect trusted upstream monetary evidence"))
		return
	case err != nil:
		if d.Log != nil {
			d.Log.Error("apply channel cost unit rebase failed", "channel_id", channelID, "actor", actor, "err", err)
		}
		fail(c, http.StatusInternalServerError, errors.New("failed to apply channel cost unit rebase"))
		return
	}
	if result == nil || result.Audit == nil {
		fail(c, http.StatusInternalServerError, errors.New("cost unit rebase returned no audit evidence"))
		return
	}
	if d.Log != nil {
		d.Log.Info("administrator applied channel cost unit rebase",
			"channel_id", channelID, "actor", actor,
			"previous_multiplier", result.Audit.PreviousMultiplier,
			"current_multiplier", result.Audit.CurrentMultiplier,
			"applied_state_version", result.Audit.AppliedStateVersion,
			"idempotent_replay", result.IdempotentReplay,
		)
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}

type channelCostBasisOverrideInput struct {
	ExpectedVersion          uint64   `json:"expected_version"`
	ExpectedIdentityRevision uint64   `json:"expected_identity_revision"`
	ExpectedCostUnitEpoch    uint64   `json:"expected_cost_unit_epoch"`
	FundingFactor            *float64 `json:"funding_factor"`
	Note                     string   `json:"note"`
}

func overrideChannelCostBasis(c *gin.Context, d *Deps) {
	if d == nil || d.CostBasisOverride == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("cost basis override is unavailable"))
		return
	}
	channelID, err := uintParam(c, "id")
	if err != nil || channelID == 0 {
		fail(c, http.StatusBadRequest, errors.New("channel id is invalid"))
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if !channelCostUnitRebaseIdempotencyPattern.MatchString(idempotencyKey) {
		fail(c, http.StatusBadRequest, errors.New("a valid Idempotency-Key header is required"))
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	var input channelCostBasisOverrideInput
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, channelCostUnitRebaseBodyLimit)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		failChannelCostUnitRebaseDecode(c, err)
		return
	}
	if err := ensureJSONEOF(decoder); err != nil {
		failChannelCostUnitRebaseDecode(c, err)
		return
	}
	input.Note = strings.TrimSpace(input.Note)
	if input.ExpectedVersion == 0 || input.ExpectedIdentityRevision == 0 || input.ExpectedCostUnitEpoch == 0 ||
		input.FundingFactor == nil || !finiteNonNegativeAPIValue(*input.FundingFactor) || input.Note == "" || len(input.Note) > 1024 {
		fail(c, http.StatusBadRequest, costing.ErrCostBasisOverrideCommandInvalid)
		return
	}
	result, err := d.CostBasisOverride.OverrideCostBasis(channelID, costing.CostBasisOverrideCommand{
		ExpectedVersion: input.ExpectedVersion, ExpectedIdentityRevision: input.ExpectedIdentityRevision,
		ExpectedCostUnitEpoch: input.ExpectedCostUnitEpoch, FundingFactor: *input.FundingFactor,
		IdempotencyKey: idempotencyKey, Actor: actor, Note: input.Note,
	})
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		fail(c, http.StatusNotFound, errors.New("channel or cost state was not found"))
		return
	case errors.Is(err, storage.ErrCostStateConflict), errors.Is(err, storage.ErrCostUnitEpochConflict),
		errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision),
		errors.Is(err, storage.ErrCostBasisOverrideIdempotencyConflict), errors.Is(err, storage.ErrChannelDeletionPending):
		fail(c, http.StatusConflict, stableCostBasisOverrideError(err))
		return
	case errors.Is(err, costing.ErrCostBasisOverrideCommandInvalid), errors.Is(err, storage.ErrCostBasisOverrideInvalid):
		fail(c, http.StatusUnprocessableEntity, stableCostBasisOverrideError(err))
		return
	case err != nil:
		if d.Log != nil {
			d.Log.Error("apply channel cost basis override failed", "channel_id", channelID, "actor", actor, "err", err)
		}
		fail(c, http.StatusInternalServerError, errors.New("failed to apply channel cost basis override"))
		return
	}
	if result == nil || result.Audit == nil {
		fail(c, http.StatusInternalServerError, errors.New("cost basis override returned no audit evidence"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}

func stableCostBasisOverrideError(err error) error {
	for _, known := range []error{
		storage.ErrCostStateConflict, storage.ErrCostUnitEpochConflict, storage.ErrStaleChannelAccountIdentityRevision,
		storage.ErrCostBasisOverrideIdempotencyConflict,
		storage.ErrChannelDeletionPending,
		costing.ErrCostBasisOverrideCommandInvalid, storage.ErrCostBasisOverrideInvalid,
	} {
		if errors.Is(err, known) {
			return known
		}
	}
	return errors.New("cost basis override failed")
}

func failChannelCostUnitRebaseDecode(c *gin.Context, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		fail(c, http.StatusRequestEntityTooLarge, errors.New("request body exceeds 16 KiB"))
		return
	}
	fail(c, http.StatusBadRequest, err)
}

func stableCostUnitRebaseError(err error) error {
	for _, known := range []error{
		storage.ErrCostStateConflict,
		storage.ErrCostUnitEpochConflict,
		storage.ErrStaleChannelAccountIdentityRevision,
		storage.ErrStaleUpstreamAccountObservation,
		storage.ErrPendingFundingCandidate,
		storage.ErrCostUnitRebaseIdempotencyConflict,
		storage.ErrCostUnitRequestRestatementConflict,
		costing.ErrCostUnitRebaseEvidenceMismatch,
		costing.ErrCostUnitRebaseNotRequired,
		costing.ErrCostUnitRebaseUnexplainedRegression,
		storage.ErrCostUnitRebaseInvalid,
		storage.ErrCostUnitRequestRestatementInvalid,
	} {
		if errors.Is(err, known) {
			return errors.New(known.Error())
		}
	}
	return errors.New("cost unit rebase failed")
}

type channelCostUnitRepairAdoptionPreviewInput struct {
	ResetEventID           uint64    `json:"reset_event_id"`
	CorrectionEventID      uint64    `json:"correction_event_id"`
	CostSampledAtBefore    time.Time `json:"cost_sampled_at_before"`
	CostSampledAtAfter     time.Time `json:"cost_sampled_at_after"`
	AffectedStartedAt      time.Time `json:"affected_started_at"`
	AffectedEndedAt        time.Time `json:"affected_ended_at"`
	PreviousProviderFactor string    `json:"previous_provider_factor"`
	CurrentProviderFactor  string    `json:"current_provider_factor"`
}

type channelCostUnitRepairAdoptionApplyInput struct {
	Anchor        storage.CostUnitRepairAdoptionAnchor `json:"anchor"`
	PreviewDigest string                               `json:"preview_digest"`
}

type channelCostUnitRepairAdoptionResult struct {
	Adoption         *storage.ChannelCostUnitRepairAdoption `json:"adoption"`
	IdempotentReplay bool                                   `json:"idempotent_replay"`
}

func previewChannelCostUnitRepairAdoption(c *gin.Context, d *Deps) {
	if d == nil || d.CostUnitRepair == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("cost unit repair adoption is unavailable"))
		return
	}
	channelID, err := uintParam(c, "id")
	if err != nil || channelID == 0 {
		fail(c, http.StatusBadRequest, errors.New("channel id is invalid"))
		return
	}
	var input channelCostUnitRepairAdoptionPreviewInput
	if err := decodeChannelCostUnitRepairAdoptionBody(c, &input); err != nil {
		failChannelCostUnitRebaseDecode(c, err)
		return
	}
	preview, err := d.CostUnitRepair.PreviewCostUnitRepairAdoption(storage.CostUnitRepairAdoptionAnchor{
		ChannelID: channelID, ResetEventID: input.ResetEventID, CorrectionEventID: input.CorrectionEventID,
		CostSampledAtBefore: input.CostSampledAtBefore, CostSampledAtAfter: input.CostSampledAtAfter,
		AffectedStartedAt: input.AffectedStartedAt, AffectedEndedAt: input.AffectedEndedAt,
		PreviousProviderFactor: input.PreviousProviderFactor, CurrentProviderFactor: input.CurrentProviderFactor,
		FactorSource: "advertised",
	})
	if handleChannelCostUnitRepairAdoptionError(c, d, channelID, "preview", err) {
		return
	}
	if preview == nil || preview.Digest == "" || preview.RequestRestatement == nil {
		fail(c, http.StatusInternalServerError, errors.New("cost unit repair adoption returned no preview evidence"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": preview})
}

func applyChannelCostUnitRepairAdoption(c *gin.Context, d *Deps) {
	if d == nil || d.CostUnitRepair == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("cost unit repair adoption is unavailable"))
		return
	}
	channelID, err := uintParam(c, "id")
	if err != nil || channelID == 0 {
		fail(c, http.StatusBadRequest, errors.New("channel id is invalid"))
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if !channelCostUnitRebaseIdempotencyPattern.MatchString(idempotencyKey) {
		fail(c, http.StatusBadRequest, errors.New("a valid Idempotency-Key header is required"))
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	var input channelCostUnitRepairAdoptionApplyInput
	if err := decodeChannelCostUnitRepairAdoptionBody(c, &input); err != nil {
		failChannelCostUnitRebaseDecode(c, err)
		return
	}
	if input.Anchor.ChannelID != channelID {
		fail(c, http.StatusBadRequest, storage.ErrCostUnitRepairAdoptionInvalid)
		return
	}
	adoption, replayed, err := d.CostUnitRepair.ApplyCostUnitRepairAdoption(
		input.Anchor, input.PreviewDigest, idempotencyKey, actor,
	)
	if handleChannelCostUnitRepairAdoptionError(c, d, channelID, actor, err) {
		return
	}
	if adoption == nil || adoption.ID == 0 {
		fail(c, http.StatusInternalServerError, errors.New("cost unit repair adoption returned no audit evidence"))
		return
	}
	if d.Log != nil {
		d.Log.Info("administrator adopted channel cost unit repair",
			"channel_id", channelID, "actor", actor, "adoption_id", adoption.ID,
			"reset_event_id", adoption.ResetEventID, "correction_event_id", adoption.CorrectionEventID,
			"idempotent_replay", replayed)
	}
	c.JSON(http.StatusOK, gin.H{"data": channelCostUnitRepairAdoptionResult{Adoption: adoption, IdempotentReplay: replayed}})
}

func decodeChannelCostUnitRepairAdoptionBody(c *gin.Context, target any) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, channelCostUnitRebaseBodyLimit)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func handleChannelCostUnitRepairAdoptionError(c *gin.Context, d *Deps, channelID uint, actor string, err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		fail(c, http.StatusNotFound, errors.New("channel or repair evidence was not found"))
	case errors.Is(err, storage.ErrCostUnitRepairAdoptionInvalid),
		errors.Is(err, storage.ErrCostUnitRequestRestatementInvalid):
		fail(c, http.StatusUnprocessableEntity, stableCostUnitRepairAdoptionError(err))
	case errors.Is(err, storage.ErrCostUnitRepairAdoptionConflict),
		errors.Is(err, storage.ErrCostUnitRepairAdoptionIdempotencyConflict),
		errors.Is(err, storage.ErrCostUnitRequestRestatementConflict),
		errors.Is(err, storage.ErrCostStateConflict),
		errors.Is(err, storage.ErrCostUnitEpochConflict),
		errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision),
		errors.Is(err, storage.ErrStaleUpstreamAccountObservation),
		errors.Is(err, storage.ErrUpstreamAccountIdentityUnbound),
		errors.Is(err, storage.ErrPendingFundingCandidate),
		errors.Is(err, storage.ErrChannelDeletionPending):
		fail(c, http.StatusConflict, stableCostUnitRepairAdoptionError(err))
	default:
		if d != nil && d.Log != nil {
			d.Log.Error("channel cost unit repair adoption failed", "channel_id", channelID, "actor", actor, "err", err)
		}
		fail(c, http.StatusInternalServerError, errors.New("cost unit repair adoption failed"))
	}
	return true
}

func stableCostUnitRepairAdoptionError(err error) error {
	for _, known := range []error{
		storage.ErrCostUnitRepairAdoptionInvalid,
		storage.ErrCostUnitRepairAdoptionConflict,
		storage.ErrCostUnitRepairAdoptionIdempotencyConflict,
		storage.ErrCostUnitRequestRestatementInvalid,
		storage.ErrCostUnitRequestRestatementConflict,
		storage.ErrCostStateConflict,
		storage.ErrCostUnitEpochConflict,
		storage.ErrStaleChannelAccountIdentityRevision,
		storage.ErrStaleUpstreamAccountObservation,
		storage.ErrUpstreamAccountIdentityUnbound,
		storage.ErrPendingFundingCandidate,
		storage.ErrChannelDeletionPending,
	} {
		if errors.Is(err, known) {
			return errors.New(known.Error())
		}
	}
	return errors.New("cost unit repair adoption failed")
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain exactly one JSON object")
		}
		return err
	}
	return nil
}

type channelFundingInput struct {
	PaidAmount            *float64   `json:"paid_amount"`
	CreditedAmount        float64    `json:"credited_amount" binding:"required"`
	BalanceIncludesCredit bool       `json:"balance_includes_credit"`
	ExpectedVersion       uint64     `json:"expected_version" binding:"required"`
	Note                  string     `json:"note"`
	EffectiveAt           *time.Time `json:"effective_at"`
}

func createChannelFundingEvent(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if d.Costing == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("costing service is unavailable"))
		return
	}
	var in channelFundingInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.PaidAmount == nil {
		fail(c, http.StatusBadRequest, errors.New("paid_amount is required"))
		return
	}
	state, err := d.Costing.State(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	preRaw := state.RawBalance
	effectiveAt := time.Now()
	if in.EffectiveAt != nil && !in.EffectiveAt.IsZero() {
		effectiveAt = *in.EffectiveAt
	}
	historical := effectiveAt.Before(time.Now().Add(-time.Second))
	rawTodayBefore, rawTotalBefore := state.RawTodayCost, state.RawTotalCost
	if historical {
		if d.Rates == nil {
			fail(c, http.StatusBadRequest, errors.New("没有历史快照，无法安全补录该充值时间"))
			return
		}
		balanceSnapshot, snapshotErr := d.Rates.BalanceSnapshotAtOrBefore(id, effectiveAt)
		if snapshotErr != nil || balanceSnapshot == nil || balanceSnapshot.RawBalance == nil {
			fail(c, http.StatusBadRequest, errors.New("充值时间之前没有原始余额快照，无法安全计算成本"))
			return
		}
		preRaw = *balanceSnapshot.RawBalance
		costSnapshot, snapshotErr := d.Rates.CostSnapshotAtOrBefore(id, effectiveAt)
		if snapshotErr != nil || costSnapshot == nil || costSnapshot.RawTodayCost == nil || costSnapshot.RawTotalCost == nil {
			fail(c, http.StatusBadRequest, errors.New("充值时间之前没有完整消费快照，无法安全回填费用"))
			return
		}
		rawTodayBefore, rawTotalBefore = *costSnapshot.RawTodayCost, *costSnapshot.RawTotalCost
	} else if in.BalanceIncludesCredit {
		if in.CreditedAmount > state.RawBalance {
			fail(c, http.StatusBadRequest, fmt.Errorf("到账金额 %.6f 大于当前上游余额 %.6f，无法反推出充值前余额", in.CreditedAmount, state.RawBalance))
			return
		}
		preRaw = state.RawBalance - in.CreditedAmount
	}
	projectedRaw := state.RawBalance
	if !in.BalanceIncludesCredit {
		projectedRaw += in.CreditedAmount
	}
	_, _, err = d.Costing.RecordFunding(costing.FundingInput{
		ChannelID: id, ExpectedVersion: in.ExpectedVersion,
		PaidAmount: *in.PaidAmount, CreditedAmount: in.CreditedAmount,
		CurrentRawBalance: &preRaw, ProjectedRawBalance: &projectedRaw,
		RawTodayCostBefore: &rawTodayBefore, RawTotalCostBefore: &rawTotalBefore,
		Kind: costing.FundingKindRecharge, Note: in.Note, EffectiveAt: effectiveAt, Source: "manual_recharge",
	})
	if errors.Is(err, storage.ErrCostStateConflict) {
		fail(c, http.StatusConflict, errors.New("余额成本状态已更新，请刷新后重新确认"))
		return
	}
	if errors.Is(err, storage.ErrPendingFundingCandidate) {
		fail(c, http.StatusConflict, errors.New("存在待确认充值，请先在上方确认后再手工记录新充值"))
		return
	}
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	profile, err := loadChannelCostProfile(d, id, 20, 0)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": profile})
}

type confirmFundingCandidateInput struct {
	PaidAmount      *float64 `json:"paid_amount"`
	ExpectedVersion uint64   `json:"expected_version" binding:"required"`
	Note            string   `json:"note"`
}

func confirmChannelFundingCandidate(c *gin.Context, d *Deps) {
	channelID, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	candidateID, err := strconv.ParseUint(c.Param("candidate_id"), 10, 64)
	if err != nil || candidateID == 0 {
		fail(c, http.StatusBadRequest, errors.New("candidate_id invalid"))
		return
	}
	var in confirmFundingCandidateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.PaidAmount == nil {
		fail(c, http.StatusBadRequest, errors.New("paid_amount is required"))
		return
	}
	_, _, err = d.Costing.ConfirmFundingCandidate(channelID, candidateID, *in.PaidAmount, in.ExpectedVersion, in.Note)
	if errors.Is(err, storage.ErrCostStateConflict) {
		fail(c, http.StatusConflict, errors.New("余额成本状态已更新，请刷新后重新确认"))
		return
	}
	if errors.Is(err, storage.ErrFundingCandidateOrder) {
		fail(c, http.StatusConflict, errors.New("请先确认时间更早的待处理充值"))
		return
	}
	if errors.Is(err, storage.ErrFundingCandidateCostBasis) {
		fail(c, http.StatusConflict, errors.New("该充值证据与现有成本账本冲突，请核对充值历史后再处理"))
		return
	}
	if errors.Is(err, storage.ErrFundingCandidateSuperseded) {
		profile, profileErr := loadChannelCostProfile(d, channelID, 20, 0)
		if profileErr != nil {
			fail(c, http.StatusInternalServerError, profileErr)
			return
		}
		profile.Resolution = storage.FundingCandidateSuperseded
		c.JSON(http.StatusOK, gin.H{"data": profile})
		return
	}
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	profile, err := loadChannelCostProfile(d, channelID, 20, 0)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": profile})
}

func loadChannelCostProfile(d *Deps, channelID uint, limit, offset int) (*channelCostProfileResponse, error) {
	if d == nil || d.Costing == nil || d.Channels == nil || d.Rates == nil {
		return nil, errors.New("costing service is unavailable")
	}
	channelRecord, err := d.Channels.FindByID(channelID)
	if err != nil {
		return nil, err
	}
	state, err := d.Costing.State(channelID)
	if err != nil {
		return nil, err
	}
	events, total, err := d.Costing.FundingEvents(channelID, limit, offset)
	if err != nil {
		return nil, err
	}
	pending, pendingTotal, err := d.Costing.FundingCandidates(channelID, "pending", 100, 0)
	if err != nil {
		return nil, err
	}
	rates, err := d.Rates.ListByChannel(channelID)
	if err != nil {
		return nil, err
	}
	items := make([]channelCostRate, 0, len(rates))
	for _, rate := range rates {
		completion := rate.CompletionRatio
		if completion <= 0 {
			completion = rate.Ratio
		}
		items = append(items, channelCostRate{
			GroupName: rate.ModelName,
			RawRatio:  rate.Ratio, RawCompletionRatio: rate.CompletionRatio,
			EffectiveRatio:           rate.Ratio * state.FundingFactor,
			EffectiveCompletionRatio: completion * state.FundingFactor,
		})
	}
	return &channelCostProfileResponse{
		State: state, Events: events, Total: total, Pending: pending, PendingTotal: pendingTotal, Rates: items,
		CostUnitRebase:    buildChannelCostUnitRebaseReadiness(channelRecord, state, pendingTotal),
		CostBasisOverride: buildChannelCostBasisOverrideReadiness(channelRecord, state, pendingTotal),
	}, nil
}
