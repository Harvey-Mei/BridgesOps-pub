package api

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/auth"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

func decodeGatewayModelPriceInputForTest(t *testing.T, body string, requireRevision bool) (storage.GatewayModelPriceInput, *uint64, error) {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return decodeGatewayModelPriceInput(ctx, requireRevision)
}

func TestDecodeGatewayModelPriceInputIsStrictAndPreservesDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	valid := `{"model_name":"model-a","input_unit_price_usd":5,"output_unit_price_usd":30,"cache_read_unit_price_usd":0.5,"cache_write_unit_price_usd":6.25,"enabled":false}`
	input, revision, err := decodeGatewayModelPriceInputForTest(t, valid, false)
	if err != nil {
		t.Fatalf("decode valid create input: %v", err)
	}
	if input.Enabled || revision != nil {
		t.Fatalf("decoded enabled=%t revision=%v, want disabled without revision", input.Enabled, revision)
	}

	updateInput, revision, err := decodeGatewayModelPriceInputForTest(t, strings.TrimSuffix(valid, "}")+`,"expected_revision":7}`, true)
	if err != nil {
		t.Fatalf("decode valid update input: %v", err)
	}
	if updateInput.Enabled || revision == nil || *revision != 7 {
		t.Fatalf("decoded update enabled=%t revision=%v", updateInput.Enabled, revision)
	}

	for _, test := range []struct {
		name            string
		body            string
		requireRevision bool
		wantText        string
	}{
		{name: "unknown field", body: strings.TrimSuffix(valid, "}") + `,"private_price":1}`, wantText: "unknown field"},
		{name: "trailing object", body: valid + `{}`, wantText: "exactly one JSON object"},
		{name: "missing enabled", body: `{"model_name":"model-a"}`, wantText: "enabled is required"},
		{name: "null enabled", body: `{"model_name":"model-a","enabled":null}`, wantText: "enabled is required"},
		{name: "missing input price", body: `{"model_name":"model-a","output_unit_price_usd":30,"cache_read_unit_price_usd":0.5,"cache_write_unit_price_usd":6.25,"enabled":true}`, wantText: "input_unit_price_usd is required"},
		{name: "null input price", body: `{"model_name":"model-a","input_unit_price_usd":null,"output_unit_price_usd":30,"cache_read_unit_price_usd":0.5,"cache_write_unit_price_usd":6.25,"enabled":true}`, wantText: "input_unit_price_usd is required"},
		{name: "missing output price", body: `{"model_name":"model-a","input_unit_price_usd":5,"cache_read_unit_price_usd":0.5,"cache_write_unit_price_usd":6.25,"enabled":true}`, wantText: "output_unit_price_usd is required"},
		{name: "null output price", body: `{"model_name":"model-a","input_unit_price_usd":5,"output_unit_price_usd":null,"cache_read_unit_price_usd":0.5,"cache_write_unit_price_usd":6.25,"enabled":true}`, wantText: "output_unit_price_usd is required"},
		{name: "partial update", body: `{"model_name":"model-a","input_unit_price_usd":5,"enabled":true,"expected_revision":1}`, requireRevision: true, wantText: "output_unit_price_usd is required"},
		{name: "create revision", body: strings.TrimSuffix(valid, "}") + `,"expected_revision":1}`, wantText: "not allowed"},
		{name: "missing update revision", body: valid, requireRevision: true, wantText: "expected_revision is required"},
		{name: "oversized update revision", body: strings.TrimSuffix(valid, "}") + fmt.Sprintf(`,"expected_revision":%d}`, uint64(math.MaxInt64)+1), requireRevision: true, wantText: "expected_revision is required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, decodeErr := decodeGatewayModelPriceInputForTest(t, test.body, test.requireRevision)
			if decodeErr == nil || !strings.Contains(decodeErr.Error(), test.wantText) {
				t.Fatalf("decode error = %v, want text %q", decodeErr, test.wantText)
			}
		})
	}
}

func TestGatewayModelPriceAPIPreservesMissingAndNullCachePrices(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "model-price-admin")
		c.Next()
	})
	registerGatewayModelPriceRoutes(router.Group("/api/gateway"), &Deps{
		Gateway: &gateway.Service{Store: storage.NewGateway(db)},
	})

	for _, test := range []struct {
		name      string
		modelName string
		body      string
	}{
		{
			name:      "missing cache prices",
			modelName: "api-model-price-cache-missing",
			body:      `{"model_name":"api-model-price-cache-missing","input_unit_price_usd":5,"output_unit_price_usd":30,"enabled":true}`,
		},
		{
			name:      "null cache prices",
			modelName: "api-model-price-cache-null",
			body:      `{"model_name":"api-model-price-cache-null","input_unit_price_usd":5,"output_unit_price_usd":30,"cache_read_unit_price_usd":null,"cache_write_unit_price_usd":null,"enabled":true}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/gateway/model-prices", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusCreated {
				t.Fatalf("create response=%d %s", response.Code, response.Body.String())
			}
			for _, field := range []string{`"cache_read_unit_price_usd":null`, `"cache_write_unit_price_usd":null`} {
				if !strings.Contains(response.Body.String(), field) {
					t.Fatalf("create response did not preserve %s: %s", field, response.Body.String())
				}
			}

			var price storage.GatewayModelPrice
			if err := db.Where("model_name = ?", test.modelName).Take(&price).Error; err != nil {
				t.Fatalf("load created model price: %v", err)
			}
			if price.CacheReadUnitPriceUSD != nil || price.CacheWriteUnitPriceUSD != nil {
				t.Fatalf("stored cache prices = (%v, %v), want (nil, nil)", price.CacheReadUnitPriceUSD, price.CacheWriteUnitPriceUSD)
			}
		})
	}
}

func TestGatewayModelPriceAPIRejectsInvalidCachePrices(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "model-price-admin")
		c.Next()
	})
	registerGatewayModelPriceRoutes(router.Group("/api/gateway"), &Deps{
		Gateway: &gateway.Service{Store: storage.NewGateway(db)},
	})

	for _, test := range []struct {
		name string
		body string
	}{
		{name: "negative cache read", body: `{"model_name":"invalid-negative-cache-read","input_unit_price_usd":5,"output_unit_price_usd":30,"cache_read_unit_price_usd":-0.1,"enabled":true}`},
		{name: "negative cache write", body: `{"model_name":"invalid-negative-cache-write","input_unit_price_usd":5,"output_unit_price_usd":30,"cache_write_unit_price_usd":-0.1,"enabled":true}`},
		{name: "non finite cache read", body: `{"model_name":"invalid-non-finite-cache-read","input_unit_price_usd":5,"output_unit_price_usd":30,"cache_read_unit_price_usd":1e309,"enabled":true}`},
		{name: "non finite cache write", body: `{"model_name":"invalid-non-finite-cache-write","input_unit_price_usd":5,"output_unit_price_usd":30,"cache_write_unit_price_usd":-1e309,"enabled":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/gateway/model-prices", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("create response=%d %s, want 400", response.Code, response.Body.String())
			}
		})
	}

	var count int64
	if err := db.Model(&storage.GatewayModelPrice{}).Where("model_name LIKE ?", "invalid-%").Count(&count).Error; err != nil {
		t.Fatalf("count invalid model prices: %v", err)
	}
	if count != 0 {
		t.Fatalf("stored %d invalid model prices, want 0", count)
	}
}

func TestGatewayModelPriceActorMatchesRuntimeAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	newContext := func() *gin.Context {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		return ctx
	}

	ctx := newContext()
	actor, err := gatewayModelPriceActor(ctx, nil)
	if err != nil || actor != "administrator" {
		t.Fatalf("missing runtime actor = %q, err=%v; want administrator", actor, err)
	}

	disabledRuntime := runtimeconfig.New("", "", nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil)
	actor, err = gatewayModelPriceActor(ctx, &Deps{Runtime: disabledRuntime})
	if err != nil || actor != "administrator" {
		t.Fatalf("disabled auth actor = %q, err=%v; want administrator", actor, err)
	}

	authService, err := auth.New("admin", "password", "gateway-model-price-secret", time.Hour)
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	enabledRuntime := runtimeconfig.New("", "", nil, nil, nil, authService, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil)
	if actor, actorErr := gatewayModelPriceActor(newContext(), &Deps{Runtime: enabledRuntime}); actorErr == nil || actor != "" {
		t.Fatalf("enabled auth without subject actor = %q, err=%v; want rejection", actor, actorErr)
	}

	authenticated := newContext()
	authenticated.Set("authSubject", " model-price-admin ")
	actor, err = gatewayModelPriceActor(authenticated, &Deps{Runtime: enabledRuntime})
	if err != nil || actor != "model-price-admin" {
		t.Fatalf("authenticated actor = %q, err=%v; want model-price-admin", actor, err)
	}
}

func TestGatewayModelPriceFailureMappingIsStableAndRedacted(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		wantStatus int
		wantText   string
	}{
		{name: "invalid", err: fmt.Errorf("private validation detail: %w", storage.ErrGatewayModelPriceInvalid), wantStatus: http.StatusBadRequest, wantText: "model price request is invalid"},
		{name: "exists", err: fmt.Errorf("private duplicate detail: %w", storage.ErrGatewayModelPriceExists), wantStatus: http.StatusConflict, wantText: "model price already exists"},
		{name: "untyped gorm duplicate", err: gorm.ErrDuplicatedKey, wantStatus: http.StatusInternalServerError, wantText: "model price operation failed"},
		{name: "untyped postgres unique violation", err: &pgconn.PgError{Code: "23505", Detail: "private row and constraint detail"}, wantStatus: http.StatusInternalServerError, wantText: "model price operation failed"},
		{name: "conflict", err: fmt.Errorf("private revision detail: %w", storage.ErrGatewayModelPriceConflict), wantStatus: http.StatusConflict, wantText: "model price changed; refresh and try again"},
		{name: "not found", err: gorm.ErrRecordNotFound, wantStatus: http.StatusNotFound, wantText: "model price was not found"},
		{name: "internal", err: errors.New("private database failure"), wantStatus: http.StatusInternalServerError, wantText: "model price operation failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			status, responseErr := gatewayModelPriceFailure(test.err)
			if status != test.wantStatus || responseErr == nil || responseErr.Error() != test.wantText {
				t.Fatalf("mapping status=%d err=%v, want %d %q", status, responseErr, test.wantStatus, test.wantText)
			}
			for _, privateText := range []string{"private", "constraint", "SQLSTATE", "duplicate key"} {
				if strings.Contains(responseErr.Error(), privateText) {
					t.Fatalf("mapping leaked %q in %q", privateText, responseErr.Error())
				}
			}
		})
	}
}

func TestGatewayModelPriceAPIHonorsRevisionAndDisabledState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "model-price-admin")
		c.Next()
	})
	registerGatewayModelPriceRoutes(router.Group("/api/gateway"), &Deps{
		Gateway: &gateway.Service{Store: storage.NewGateway(db)},
	})

	do := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		return recorder
	}

	createBody := `{"model_name":"api-model-price-test","input_unit_price_usd":5,"output_unit_price_usd":30,"cache_read_unit_price_usd":0.5,"cache_write_unit_price_usd":6.25,"enabled":false}`
	created := do(http.MethodPost, "/api/gateway/model-prices", createBody)
	if created.Code != http.StatusCreated || !strings.Contains(created.Body.String(), `"enabled":false`) || !strings.Contains(created.Body.String(), `"revision":1`) {
		t.Fatalf("create response=%d %s", created.Code, created.Body.String())
	}
	if strings.Contains(created.Body.String(), `"enabled":true`) {
		t.Fatalf("create silently re-enabled disabled price: %s", created.Body.String())
	}

	duplicate := do(http.MethodPost, "/api/gateway/model-prices", createBody)
	if duplicate.Code != http.StatusConflict || !strings.Contains(duplicate.Body.String(), errGatewayModelPriceAlreadyExists.Error()) {
		t.Fatalf("duplicate response=%d %s", duplicate.Code, duplicate.Body.String())
	}
	for _, privateText := range []string{"SQLSTATE", "duplicate key", "uq_gateway_model_prices"} {
		if strings.Contains(duplicate.Body.String(), privateText) {
			t.Fatalf("duplicate response leaked %q: %s", privateText, duplicate.Body.String())
		}
	}

	var price storage.GatewayModelPrice
	if err := db.Where("model_name = ?", "api-model-price-test").Take(&price).Error; err != nil {
		t.Fatalf("load created model price: %v", err)
	}
	updateBody := `{"model_name":"api-model-price-test","input_unit_price_usd":5,"output_unit_price_usd":30,"cache_read_unit_price_usd":0.5,"cache_write_unit_price_usd":6.25,"enabled":true,"expected_revision":1}`
	updated := do(http.MethodPatch, fmt.Sprintf("/api/gateway/model-prices/%d", price.ID), updateBody)
	if updated.Code != http.StatusOK || !strings.Contains(updated.Body.String(), `"enabled":true`) || !strings.Contains(updated.Body.String(), `"revision":2`) {
		t.Fatalf("update response=%d %s", updated.Code, updated.Body.String())
	}

	stale := do(http.MethodPatch, fmt.Sprintf("/api/gateway/model-prices/%d", price.ID), updateBody)
	if stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), errGatewayModelPriceChanged.Error()) {
		t.Fatalf("stale update response=%d %s", stale.Code, stale.Body.String())
	}

	missing := do(http.MethodPatch, fmt.Sprintf("/api/gateway/model-prices/%d", uint64(math.MaxInt64)), updateBody)
	if missing.Code != http.StatusNotFound || !strings.Contains(missing.Body.String(), errGatewayModelPriceNotFound.Error()) {
		t.Fatalf("missing update response=%d %s", missing.Code, missing.Body.String())
	}

	deleted := do(http.MethodDelete, fmt.Sprintf("/api/gateway/model-prices/%d?expected_revision=2", price.ID), "")
	if deleted.Code != http.StatusOK || !strings.Contains(deleted.Body.String(), `"ok":true`) {
		t.Fatalf("delete response=%d %s", deleted.Code, deleted.Body.String())
	}
}
