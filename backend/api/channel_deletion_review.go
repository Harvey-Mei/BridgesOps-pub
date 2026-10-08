package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type channelDeletionReviewClaimOutput struct {
	ID                    uint64                                `json:"id,string"`
	ClaimEpoch            uint64                                `json:"claim_epoch,string"`
	LocalKeyID            uint                                  `json:"local_key_id,string"`
	KeyGeneration         uint64                                `json:"key_generation,string"`
	UpstreamGroupID       *uint                                 `json:"upstream_group_id,omitempty,string"`
	PresenceGeneration    *uint64                               `json:"presence_generation,omitempty,string"`
	CanonicalGroupName    string                                `json:"canonical_group_name,omitempty"`
	UpstreamKeyID         int64                                 `json:"upstream_key_id,omitempty,string"`
	ObservedRemoteID      int64                                 `json:"observed_remote_id,omitempty,string"`
	UpstreamKeyName       string                                `json:"upstream_key_name,omitempty"`
	OwnershipProvenance   string                                `json:"ownership_provenance"`
	Provisioning          bool                                  `json:"provisioning"`
	ProvisioningStartedAt *time.Time                            `json:"provisioning_started_at,omitempty"`
	Disposition           storage.ChannelDeletionKeyDisposition `json:"disposition"`
	Status                storage.ChannelDeletionKeyClaimStatus `json:"status"`
	OutcomeReason         string                                `json:"outcome_reason,omitempty"`
	OutcomeActor          string                                `json:"outcome_actor,omitempty"`
	OutcomeAt             *time.Time                            `json:"outcome_at,omitempty"`
	LastError             string                                `json:"last_error,omitempty"`
	LeaseExpiresAt        *time.Time                            `json:"lease_expires_at,omitempty"`
	UpdatedAt             time.Time                             `json:"updated_at"`
}

type channelDeletionReviewOutput struct {
	IntentID                uint64                              `json:"intent_id,string"`
	ChannelID               uint                                `json:"channel_id,string"`
	DeletionEpoch           uint64                              `json:"deletion_epoch,string"`
	ChannelIdentityRevision uint64                              `json:"channel_identity_revision,string"`
	ChannelName             string                              `json:"channel_name"`
	ChannelType             storage.ChannelType                 `json:"channel_type"`
	Status                  storage.ChannelDeletionIntentStatus `json:"status"`
	LastError               string                              `json:"last_error,omitempty"`
	AttemptCount            uint64                              `json:"attempt_count,string"`
	LastAttemptAt           *time.Time                          `json:"last_attempt_at,omitempty"`
	NextAttemptAt           *time.Time                          `json:"next_attempt_at,omitempty"`
	FinalizedAt             *time.Time                          `json:"finalized_at,omitempty"`
	RequestedAt             time.Time                           `json:"requested_at"`
	UpdatedAt               time.Time                           `json:"updated_at"`
	Claims                  []channelDeletionReviewClaimOutput  `json:"claims"`
}

type channelDeletionReviewActionInput struct {
	ExpectedIntentID         uint64  `json:"expected_intent_id,string"`
	ExpectedDeletionEpoch    uint64  `json:"expected_deletion_epoch,string"`
	ExpectedIdentityRevision uint64  `json:"expected_identity_revision,string"`
	ExpectedClaimEpoch       *uint64 `json:"expected_claim_epoch,string"`
	Resolution               string  `json:"resolution"`
}

func channelDeletionReviewOutputFor(snapshot storage.ChannelDeletionReviewSnapshot) channelDeletionReviewOutput {
	intent := snapshot.Intent
	out := channelDeletionReviewOutput{
		IntentID: intent.ID, ChannelID: intent.ChannelID, DeletionEpoch: intent.DeletionEpoch,
		ChannelIdentityRevision: intent.ChannelIdentityRevision,
		ChannelName:             intent.ChannelNameSnapshot, ChannelType: intent.ChannelTypeSnapshot,
		Status: intent.Status, LastError: intent.LastError, AttemptCount: intent.AttemptCount,
		LastAttemptAt: intent.LastAttemptAt, NextAttemptAt: intent.NextAttemptAt, FinalizedAt: intent.FinalizedAt,
		RequestedAt: intent.RequestedAt, UpdatedAt: intent.UpdatedAt,
		Claims: make([]channelDeletionReviewClaimOutput, 0, len(snapshot.Claims)),
	}
	for _, claim := range snapshot.Claims {
		out.Claims = append(out.Claims, channelDeletionReviewClaimOutput{
			ID: claim.ID, ClaimEpoch: claim.ClaimEpoch, LocalKeyID: claim.LocalKeyID,
			KeyGeneration: claim.KeyGeneration, UpstreamGroupID: claim.UpstreamGroupID,
			PresenceGeneration: claim.PresenceGeneration, CanonicalGroupName: claim.CanonicalGroupName,
			UpstreamKeyID: claim.UpstreamKeyID, ObservedRemoteID: claim.ObservedRemoteID,
			UpstreamKeyName:     claim.UpstreamKeyName,
			OwnershipProvenance: claim.OwnershipProvenance, Provisioning: claim.Provisioning,
			ProvisioningStartedAt: claim.ProvisioningStartedAt, Disposition: claim.Disposition,
			Status: claim.Status, OutcomeReason: claim.OutcomeReason, OutcomeActor: claim.OutcomeActor,
			OutcomeAt: claim.OutcomeAt, LastError: claim.LastError, LeaseExpiresAt: claim.LeaseExpiresAt,
			UpdatedAt: claim.UpdatedAt,
		})
	}
	return out
}

func listChannelDeletionReviews(c *gin.Context, d *Deps) {
	if d == nil || d.Channels == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("channel deletion review is unavailable"))
		return
	}
	page, pageSize, err := parsePageQuery(c)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	statuses, err := channelDeletionReviewStatuses(c.Query("status"))
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	snapshots, total, err := d.Channels.ListChannelDeletionReviews(c.Request.Context(), statuses, page, pageSize)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	items := make([]channelDeletionReviewOutput, 0, len(snapshots))
	for _, snapshot := range snapshots {
		items = append(items, channelDeletionReviewOutputFor(snapshot))
	}
	pages := 1
	if total > 0 {
		pages = int((total + int64(pageSize) - 1) / int64(pageSize))
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"items": items, "total": total, "page": page, "page_size": pageSize, "pages": pages,
	}})
}

func channelDeletionReviewStatuses(raw string) ([]storage.ChannelDeletionIntentStatus, error) {
	switch strings.TrimSpace(raw) {
	case "", "active":
		return []storage.ChannelDeletionIntentStatus{
			storage.ChannelDeletionIntentPendingRemoteCleanup,
			storage.ChannelDeletionIntentManualReviewRequired,
			storage.ChannelDeletionIntentReadyForFinalization,
		}, nil
	case "all":
		return nil, nil
	case string(storage.ChannelDeletionIntentPendingRemoteCleanup):
		return []storage.ChannelDeletionIntentStatus{storage.ChannelDeletionIntentPendingRemoteCleanup}, nil
	case string(storage.ChannelDeletionIntentManualReviewRequired):
		return []storage.ChannelDeletionIntentStatus{storage.ChannelDeletionIntentManualReviewRequired}, nil
	case string(storage.ChannelDeletionIntentReadyForFinalization):
		return []storage.ChannelDeletionIntentStatus{storage.ChannelDeletionIntentReadyForFinalization}, nil
	case string(storage.ChannelDeletionIntentFinalized):
		return []storage.ChannelDeletionIntentStatus{storage.ChannelDeletionIntentFinalized}, nil
	default:
		return nil, errors.New("invalid channel deletion status")
	}
}

func getChannelDeletionReview(c *gin.Context, d *Deps) {
	channelID, err := uintParam(c, "id")
	if err != nil || channelID == 0 {
		fail(c, http.StatusBadRequest, errors.New("channel_id is invalid"))
		return
	}
	writeChannelDeletionReview(c, d, channelID)
}

func writeChannelDeletionReview(c *gin.Context, d *Deps, channelID uint) {
	if d == nil || d.Channels == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("channel deletion review is unavailable"))
		return
	}
	snapshot, err := d.Channels.ChannelDeletionReview(c.Request.Context(), channelID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		fail(c, http.StatusNotFound, errors.New("channel deletion review was not found"))
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": channelDeletionReviewOutputFor(*snapshot)})
}

func requeueChannelDeletionReviewClaim(c *gin.Context, d *Deps) {
	resolveChannelDeletionReviewClaim(c, d, false)
}

func retainChannelDeletionReviewClaim(c *gin.Context, d *Deps) {
	resolveChannelDeletionReviewClaim(c, d, true)
}

func resolveChannelDeletionReviewClaim(c *gin.Context, d *Deps, retain bool) {
	channelID, err := uintParam(c, "id")
	if err != nil || channelID == 0 {
		fail(c, http.StatusBadRequest, errors.New("channel_id is invalid"))
		return
	}
	claimID, err := strconv.ParseUint(c.Param("claim_id"), 10, 64)
	if err != nil || claimID == 0 {
		fail(c, http.StatusBadRequest, errors.New("claim_id is invalid"))
		return
	}
	if d == nil || d.Channels == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("channel deletion review is unavailable"))
		return
	}
	var input channelDeletionReviewActionInput
	if err := c.ShouldBindJSON(&input); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	input.Resolution = strings.TrimSpace(input.Resolution)
	if input.ExpectedIntentID == 0 || input.ExpectedDeletionEpoch == 0 || input.ExpectedIdentityRevision == 0 ||
		input.ExpectedClaimEpoch == nil || input.Resolution == "" || utf8.RuneCountInString(input.Resolution) > 64 {
		fail(c, http.StatusBadRequest, errors.New("channel deletion review fence or resolution is invalid"))
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusUnauthorized, err)
		return
	}
	fence := storage.ChannelDeletionKeyReviewFence{
		ChannelDeletionFence: storage.ChannelDeletionFence{
			ChannelID: channelID, IntentID: input.ExpectedIntentID,
			DeletionEpoch: input.ExpectedDeletionEpoch, IdentityRevision: input.ExpectedIdentityRevision,
		},
		KeyClaimID: claimID, ClaimEpoch: *input.ExpectedClaimEpoch,
	}
	if retain {
		err = d.Channels.RetainChannelDeletionKeyAfterReview(c.Request.Context(), fence, input.Resolution, actor, time.Now().UTC())
	} else {
		err = d.Channels.RequeueChannelDeletionKeyManualReview(c.Request.Context(), fence, input.Resolution, actor, time.Now().UTC())
	}
	if err != nil {
		if errors.Is(err, storage.ErrStaleChannelDeletionFence) ||
			errors.Is(err, storage.ErrChannelDeletionIntentMissing) ||
			errors.Is(err, storage.ErrChannelDeletionKeyClaimLost) ||
			errors.Is(err, storage.ErrChannelDeletionKeyFreezeChanged) {
			fail(c, http.StatusConflict, errors.New("channel deletion review changed; refresh and try again"))
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	writeChannelDeletionReview(c, d, channelID)
}

func channelDeletionPendingOutput(review *storage.ChannelDeletionReviewSnapshot, channelID uint) gin.H {
	status := storage.ChannelDeletionIntentPendingRemoteCleanup
	message := "本地删除已准备，远端清理尚未完成；渠道和登录凭据会暂时保留。"
	data := gin.H{"channel_id": channelID}
	if review != nil {
		status = review.Intent.Status
		data["intent_id"] = strconv.FormatUint(review.Intent.ID, 10)
		data["deletion_epoch"] = strconv.FormatUint(review.Intent.DeletionEpoch, 10)
		data["channel_identity_revision"] = strconv.FormatUint(review.Intent.ChannelIdentityRevision, 10)
	}
	switch status {
	case storage.ChannelDeletionIntentManualReviewRequired:
		message = "远端密钥正在按固定策略自动核验；渠道、登录凭据和删除证据会暂时保留。"
	case storage.ChannelDeletionIntentReadyForFinalization:
		message = "远端密钥清理已确认，正在等待本地删除收尾。"
	}
	data["status"] = string(status)
	data["message"] = message
	return data
}
