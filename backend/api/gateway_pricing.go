package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

const gatewayRequestPriceImportBodyLimit = 1 << 20

func getGatewayRequestPriceImportTarget(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errors.New("gateway storage is unavailable"))
		return
	}
	if _, err := gatewayAuthenticatedAdministratorActor(c); err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	channelIDValue, err := strconv.ParseUint(strings.TrimSpace(c.Query("channel_id")), 10, 64)
	if err != nil || channelIDValue == 0 || channelIDValue > uint64(^uint(0)) {
		fail(c, http.StatusBadRequest, errors.New("a valid channel_id is required"))
		return
	}
	target, err := d.Gateway.Store.AdministratorRequestPriceTarget(
		c.Request.Context(), uint(channelIDValue), c.Query("group_name"), c.Query("model_name"),
	)
	if err != nil {
		status, responseErr := gatewayRequestPriceImportFailure(err)
		fail(c, status, responseErr)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": target})
}

type gatewayRequestPriceImportInput struct {
	Items []gatewayRequestPriceImportItem `json:"items"`
}

// Prices accepted by this administrator-only endpoint are final actual USD
// costs. The caller attests when and against which current identity it observed
// them; storage verifies that fence under lock. Currency, unit, source,
// cost-unit epoch, fact version, schema version, and checksum are server-owned.
type gatewayRequestPriceImportItem struct {
	ChannelID                       uint       `json:"channel_id"`
	GroupName                       string     `json:"group_name"`
	ModelName                       string     `json:"model_name"`
	Operation                       string     `json:"operation"`
	Protocol                        string     `json:"protocol"`
	ExpectedIdentityRevision        uint64     `json:"expected_identity_revision"`
	ExpectedAdminRevision           uint64     `json:"expected_admin_revision"`
	ExpectedProvider                string     `json:"expected_provider"`
	ExpectedAPIOrigin               string     `json:"expected_api_origin"`
	ExpectedCostUnitEpoch           uint64     `json:"expected_cost_unit_epoch"`
	ExpectedGroupID                 uint       `json:"expected_group_id"`
	ExpectedGroupPresenceGeneration uint64     `json:"expected_group_presence_generation"`
	ExpectedUpstreamKeyID           uint       `json:"expected_upstream_key_id"`
	ExpectedUpstreamKeyGeneration   uint64     `json:"expected_upstream_key_generation"`
	ObservedAt                      time.Time  `json:"observed_at"`
	InputUnitPriceUSD               *float64   `json:"input_unit_price_usd"`
	OutputUnitPriceUSD              *float64   `json:"output_unit_price_usd"`
	CacheReadUnitPriceUSD           *float64   `json:"cache_read_unit_price_usd"`
	CacheWriteUnitPriceUSD          *float64   `json:"cache_write_unit_price_usd"`
	OperationUnitPriceUSD           *float64   `json:"operation_unit_price_usd"`
	ValidUntil                      *time.Time `json:"valid_until"`
}

func importGatewayRequestPriceFacts(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errors.New("gateway storage is unavailable"))
		return
	}
	actor, err := gatewayAuthenticatedAdministratorActor(c)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader(gatewayManualCooldownIdempotencyHeader))
	if !gatewayManualCooldownIdempotencyPattern.MatchString(idempotencyKey) {
		fail(c, http.StatusBadRequest, errors.New("a valid Idempotency-Key header is required"))
		return
	}
	var in gatewayRequestPriceImportInput
	if err := decodeGatewayRequestPriceImport(c, &in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	observations := make([]storage.GatewayAdministratorPriceObservation, len(in.Items))
	for index, item := range in.Items {
		observations[index] = storage.GatewayAdministratorPriceObservation{
			ChannelID:                       item.ChannelID,
			GroupName:                       item.GroupName,
			ModelName:                       item.ModelName,
			Operation:                       item.Operation,
			Protocol:                        item.Protocol,
			ExpectedIdentityRevision:        item.ExpectedIdentityRevision,
			ExpectedAdminRevision:           item.ExpectedAdminRevision,
			ExpectedProvider:                storage.ChannelType(item.ExpectedProvider),
			ExpectedAPIOrigin:               item.ExpectedAPIOrigin,
			ExpectedCostUnitEpoch:           item.ExpectedCostUnitEpoch,
			ExpectedGroupID:                 item.ExpectedGroupID,
			ExpectedGroupPresenceGeneration: item.ExpectedGroupPresenceGeneration,
			ExpectedUpstreamKeyID:           item.ExpectedUpstreamKeyID,
			ExpectedUpstreamKeyGeneration:   item.ExpectedUpstreamKeyGeneration,
			ObservedAt:                      item.ObservedAt,
			InputUnitPriceUSD:               item.InputUnitPriceUSD,
			OutputUnitPriceUSD:              item.OutputUnitPriceUSD,
			CacheReadUnitPriceUSD:           item.CacheReadUnitPriceUSD,
			CacheWriteUnitPriceUSD:          item.CacheWriteUnitPriceUSD,
			OperationUnitPriceUSD:           item.OperationUnitPriceUSD,
		}
		if item.ValidUntil != nil {
			observations[index].ValidUntil = *item.ValidUntil
		}
	}
	result, err := d.Gateway.Store.ImportAdministratorRequestPriceFacts(c.Request.Context(), actor, idempotencyKey, observations)
	if err != nil {
		if retryAfter := gatewayRequestPriceImportRetryAfter(err); retryAfter != "" {
			c.Header("Retry-After", retryAfter)
		}
		status, responseErr := gatewayRequestPriceImportFailure(err)
		fail(c, status, responseErr)
		return
	}
	if result == nil {
		fail(c, http.StatusInternalServerError, errors.New("request price import returned no result"))
		return
	}
	status := http.StatusCreated
	if result.IdempotentReplay {
		status = http.StatusOK
	}
	c.JSON(status, gin.H{"data": gin.H{
		"import_id":         result.CommandID,
		"items":             result.Items,
		"total":             len(result.Items),
		"idempotent_replay": result.IdempotentReplay,
	}})
}

func gatewayAuthenticatedAdministratorActor(c *gin.Context) (string, error) {
	if c != nil {
		if raw, ok := c.Get("authSubject"); ok {
			if actor, ok := raw.(string); ok && strings.TrimSpace(actor) != "" {
				actor = strings.TrimSpace(actor)
				return actor, nil
			}
		}
	}
	return "", errors.New("administrator authentication is required")
}

func gatewayRequestPriceImportRetryAfter(err error) string {
	if errors.Is(err, storage.ErrGatewayRequestPriceImportCommitUnknown) {
		return "1"
	}
	return ""
}

func gatewayRequestPriceImportFailure(err error) (int, error) {
	switch {
	case errors.Is(err, storage.ErrGatewayRequestPriceImportInvalid):
		return http.StatusBadRequest, err
	case errors.Is(err, storage.ErrGatewayRequestPriceImportUnavailable),
		errors.Is(err, storage.ErrGatewayRequestPriceIdentityChanged):
		return http.StatusConflict, err
	case errors.Is(err, storage.ErrGatewayRequestPriceImportIdempotencyConflict):
		return http.StatusConflict, errors.New("Idempotency-Key was already used with a different administrator or payload")
	case errors.Is(err, storage.ErrGatewayRequestPriceImportCommitUnknown):
		return http.StatusServiceUnavailable, errors.New("request price import outcome is unknown; retry with the same Idempotency-Key")
	default:
		return http.StatusInternalServerError, errors.New("request price import failed")
	}
}

func decodeGatewayRequestPriceImport(c *gin.Context, target *gatewayRequestPriceImportInput) error {
	if c == nil || c.Request == nil || target == nil {
		return errors.New("request body is required")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, gatewayRequestPriceImportBodyLimit)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain exactly one JSON object")
		}
		return err
	}
	return nil
}
