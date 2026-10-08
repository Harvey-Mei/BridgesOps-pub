package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type gatewayRoutingEnableInput struct {
	PresenceGeneration      uint64 `json:"presence_generation"`
	ChannelIdentityRevision uint64 `json:"channel_identity_revision"`
	Reason                  string `json:"reason"`
}

func enableGatewayUpstreamGroupRouting(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("gateway routing health is unavailable"))
		return
	}
	id, err := uintParam(c, "id")
	if err != nil || id == 0 {
		fail(c, http.StatusBadRequest, errors.New("upstream group id is invalid"))
		return
	}
	var in gatewayRoutingEnableInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, errors.New("channel_identity_revision, presence_generation, and reason are required"))
		return
	}
	reason := strings.TrimSpace(in.Reason)
	if in.ChannelIdentityRevision == 0 || in.PresenceGeneration == 0 || reason == "" || len(reason) > 256 {
		fail(c, http.StatusBadRequest, errors.New("channel_identity_revision, presence_generation, and reason are required"))
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader(gatewayManualCooldownIdempotencyHeader))
	if !gatewayManualCooldownIdempotencyPattern.MatchString(idempotencyKey) {
		fail(c, http.StatusBadRequest, errors.New("a valid Idempotency-Key header is required"))
		return
	}
	result, err := d.Gateway.EnableUpstreamGroupRouting(id, in.PresenceGeneration, in.ChannelIdentityRevision, actor, idempotencyKey, reason)
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		fail(c, http.StatusNotFound, errors.New("upstream group was not found"))
		return
	case errors.Is(err, storage.ErrUpstreamGroupGenerationChanged):
		fail(c, http.StatusConflict, errors.New("upstream group generation changed; reload and try again"))
		return
	case errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision):
		fail(c, http.StatusConflict, errors.New("channel account identity changed; reload and try again"))
		return
	case errors.Is(err, storage.ErrGatewayRoutingEnableUpstreamKeyUnavailable):
		fail(c, http.StatusConflict, errors.New("upstream key is missing, disabled, or still provisioning"))
		return
	case errors.Is(err, storage.ErrGatewayRoutingEnableIdempotencyConflict):
		fail(c, http.StatusConflict, errors.New("Idempotency-Key was already used by another administrator"))
		return
	case err != nil:
		if d.Log != nil {
			d.Log.Warn("administrator routing enable failed", "upstream_group_id", id, "err", err)
		}
		fail(c, http.StatusInternalServerError, errors.New("failed to enable upstream group routing"))
		return
	}
	if d.Log != nil {
		d.Log.Info("administrator enabled upstream group routing",
			"actor", actor,
			"upstream_group_id", result.UpstreamGroupID,
			"presence_generation", result.PresenceGeneration,
			"channel_identity_revision", result.ChannelIdentityRevision,
			"channel_id", result.ChannelID,
			"group", result.CanonicalGroupName,
			"changed", result.Changed,
			"idempotent_replay", result.IdempotentReplay,
		)
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}
