package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const gatewayModelPriceBodyLimit = 64 << 10

var (
	errGatewayModelPriceInvalidRequest = errors.New("model price request is invalid")
	errGatewayModelPriceAlreadyExists  = errors.New("model price already exists")
	errGatewayModelPriceChanged        = errors.New("model price changed; refresh and try again")
	errGatewayModelPriceNotFound       = errors.New("model price was not found")
	errGatewayModelPriceOperation      = errors.New("model price operation failed")
)

type gatewayModelPriceInput struct {
	ModelName                         string   `json:"model_name"`
	InputUnitPriceUSD                 *float64 `json:"input_unit_price_usd"`
	OutputUnitPriceUSD                *float64 `json:"output_unit_price_usd"`
	CacheReadUnitPriceUSD             *float64 `json:"cache_read_unit_price_usd"`
	CacheWriteUnitPriceUSD            *float64 `json:"cache_write_unit_price_usd"`
	LongContextThresholdTokens        *int64   `json:"long_context_threshold_tokens"`
	LongContextInputUnitPriceUSD      *float64 `json:"long_context_input_unit_price_usd"`
	LongContextOutputUnitPriceUSD     *float64 `json:"long_context_output_unit_price_usd"`
	LongContextCacheReadUnitPriceUSD  *float64 `json:"long_context_cache_read_unit_price_usd"`
	LongContextCacheWriteUnitPriceUSD *float64 `json:"long_context_cache_write_unit_price_usd"`
	AutoUpdate                        bool     `json:"auto_update"`
	Enabled                           *bool    `json:"enabled"`
	ExpectedRevision                  *uint64  `json:"expected_revision"`
}

func registerGatewayModelPriceRoutes(gp *gin.RouterGroup, d *Deps) {
	gp.GET("/model-prices", func(c *gin.Context) { listGatewayModelPrices(c, d) })
	gp.GET("/model-prices/used-models", func(c *gin.Context) { listGatewayUsedModels(c, d) })
	gp.POST("/model-prices/sync", func(c *gin.Context) { syncGatewayModelPrices(c, d) })
	gp.POST("/model-prices", func(c *gin.Context) { createGatewayModelPrice(c, d) })
	gp.PATCH("/model-prices/:id", func(c *gin.Context) { updateGatewayModelPrice(c, d) })
	gp.DELETE("/model-prices/:id", func(c *gin.Context) { deleteGatewayModelPrice(c, d) })
}

func listGatewayUsedModels(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errGatewayModelPriceOperation)
		return
	}
	days, err := strconv.Atoi(strings.TrimSpace(c.DefaultQuery("days", "30")))
	if err != nil || days < 1 || days > 90 {
		fail(c, http.StatusBadRequest, errors.New("days must be between 1 and 90"))
		return
	}
	items, err := d.Gateway.Store.ListUsedModels(c.Request.Context(), time.Now().UTC().Add(-time.Duration(days)*24*time.Hour))
	if err != nil {
		fail(c, http.StatusInternalServerError, errGatewayModelPriceOperation)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": items})
}

func syncGatewayModelPrices(c *gin.Context, d *Deps) {
	if d == nil || d.ModelPriceSync == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("automatic model price sync is unavailable"))
		return
	}
	if _, err := gatewayModelPriceActor(c, d); err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	result, err := d.ModelPriceSync.SyncNow(c.Request.Context())
	if err != nil {
		fail(c, http.StatusBadGateway, errors.New("automatic model price sync failed"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}

func listGatewayModelPrices(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errGatewayModelPriceOperation)
		return
	}
	prices, err := d.Gateway.Store.ListModelPrices()
	if err != nil {
		fail(c, http.StatusInternalServerError, errGatewayModelPriceOperation)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": prices})
}

func gatewayModelPriceActor(c *gin.Context, d *Deps) (string, error) {
	return gatewayAdministratorActor(c, d)
}

func decodeGatewayModelPriceInput(c *gin.Context, requireRevision bool) (storage.GatewayModelPriceInput, *uint64, error) {
	if c == nil || c.Request == nil || c.Request.Body == nil {
		return storage.GatewayModelPriceInput{}, nil, errors.New("request body is required")
	}
	var in gatewayModelPriceInput
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, gatewayModelPriceBodyLimit)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		return storage.GatewayModelPriceInput{}, nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return storage.GatewayModelPriceInput{}, nil, errors.New("request body must contain exactly one JSON object")
		}
		return storage.GatewayModelPriceInput{}, nil, err
	}
	if in.Enabled == nil {
		return storage.GatewayModelPriceInput{}, nil, errors.New("enabled is required")
	}
	for _, required := range []struct {
		name  string
		value *float64
	}{
		{name: "input_unit_price_usd", value: in.InputUnitPriceUSD},
		{name: "output_unit_price_usd", value: in.OutputUnitPriceUSD},
	} {
		if required.value == nil {
			return storage.GatewayModelPriceInput{}, nil, fmt.Errorf("%s is required", required.name)
		}
	}
	if requireRevision && (in.ExpectedRevision == nil || *in.ExpectedRevision == 0 || *in.ExpectedRevision > math.MaxInt64) {
		return storage.GatewayModelPriceInput{}, nil, errors.New("expected_revision is required")
	}
	if !requireRevision && in.ExpectedRevision != nil {
		return storage.GatewayModelPriceInput{}, nil, errors.New("expected_revision is not allowed when creating a model price")
	}
	return storage.GatewayModelPriceInput{
		ModelName: in.ModelName, InputUnitPriceUSD: *in.InputUnitPriceUSD,
		OutputUnitPriceUSD: *in.OutputUnitPriceUSD, CacheReadUnitPriceUSD: in.CacheReadUnitPriceUSD,
		CacheWriteUnitPriceUSD:            in.CacheWriteUnitPriceUSD,
		LongContextThresholdTokens:        in.LongContextThresholdTokens,
		LongContextInputUnitPriceUSD:      in.LongContextInputUnitPriceUSD,
		LongContextOutputUnitPriceUSD:     in.LongContextOutputUnitPriceUSD,
		LongContextCacheReadUnitPriceUSD:  in.LongContextCacheReadUnitPriceUSD,
		LongContextCacheWriteUnitPriceUSD: in.LongContextCacheWriteUnitPriceUSD,
		AutoUpdate:                        in.AutoUpdate, Enabled: *in.Enabled,
	}, in.ExpectedRevision, nil
}

func gatewayModelPriceFailure(err error) (int, error) {
	switch {
	case errors.Is(err, storage.ErrGatewayModelPriceInvalid):
		return http.StatusBadRequest, errGatewayModelPriceInvalidRequest
	case errors.Is(err, storage.ErrGatewayModelPriceExists):
		return http.StatusConflict, errGatewayModelPriceAlreadyExists
	case errors.Is(err, storage.ErrGatewayModelPriceConflict):
		return http.StatusConflict, errGatewayModelPriceChanged
	case errors.Is(err, gorm.ErrRecordNotFound):
		return http.StatusNotFound, errGatewayModelPriceNotFound
	default:
		return http.StatusInternalServerError, errGatewayModelPriceOperation
	}
}

func gatewayModelPriceID(c *gin.Context) (uint64, error) {
	id, err := strconv.ParseUint(strings.TrimSpace(c.Param("id")), 10, 64)
	if err != nil || id == 0 || id > math.MaxInt64 {
		return 0, errors.New("a valid model price id is required")
	}
	return id, nil
}

func createGatewayModelPrice(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errGatewayModelPriceOperation)
		return
	}
	actor, err := gatewayModelPriceActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	in, _, err := decodeGatewayModelPriceInput(c, false)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	price, err := d.Gateway.Store.CreateModelPrice(in, actor)
	if err != nil {
		status, responseErr := gatewayModelPriceFailure(err)
		fail(c, status, responseErr)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"data": price})
}

func updateGatewayModelPrice(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errGatewayModelPriceOperation)
		return
	}
	id, err := gatewayModelPriceID(c)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	actor, err := gatewayModelPriceActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	in, revision, err := decodeGatewayModelPriceInput(c, true)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	price, err := d.Gateway.Store.UpdateModelPrice(id, *revision, in, actor)
	if err != nil {
		status, responseErr := gatewayModelPriceFailure(err)
		fail(c, status, responseErr)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": price})
}

func deleteGatewayModelPrice(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errGatewayModelPriceOperation)
		return
	}
	id, err := gatewayModelPriceID(c)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	revision, err := strconv.ParseUint(strings.TrimSpace(c.Query("expected_revision")), 10, 64)
	if err != nil || revision == 0 || revision > math.MaxInt64 {
		fail(c, http.StatusBadRequest, errors.New("expected_revision is required"))
		return
	}
	actor, err := gatewayModelPriceActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	err = d.Gateway.Store.DeleteModelPrice(id, revision, actor)
	if err != nil {
		status, responseErr := gatewayModelPriceFailure(err)
		fail(c, status, responseErr)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
