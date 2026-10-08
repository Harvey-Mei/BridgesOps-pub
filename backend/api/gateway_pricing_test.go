package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/auth"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

const gatewayRequestPriceImportTestKey = "request-price-import-test"

func TestGatewayRequestPriceImportRequiresAdministratorActorAndIdempotencyKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	authService, err := auth.New("admin", "password", "gateway-price-import-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	runtime := runtimeconfig.New("", "", nil, nil, nil, authService, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil)
	protected := gin.New()
	registerGateway(protected.Group("/api"), &Deps{
		Runtime: runtime,
		Gateway: &gateway.Service{Store: storage.NewGateway(nil)},
	})
	unauthenticated := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(`{"items":[]}`))
	unauthenticated.Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey)
	unauthenticatedRecorder := httptest.NewRecorder()
	protected.ServeHTTP(unauthenticatedRecorder, unauthenticated)
	if unauthenticatedRecorder.Code != http.StatusForbidden {
		t.Fatalf("missing administrator actor status = %d, body=%s", unauthenticatedRecorder.Code, unauthenticatedRecorder.Body.String())
	}
	unauthenticatedTarget := httptest.NewRequest(http.MethodGet, "/api/gateway/request-price-facts/import-target?channel_id=1&group_name=primary&model_name=model-a", nil)
	unauthenticatedTargetRecorder := httptest.NewRecorder()
	protected.ServeHTTP(unauthenticatedTargetRecorder, unauthenticatedTarget)
	if unauthenticatedTargetRecorder.Code != http.StatusForbidden {
		t.Fatalf("unauthenticated price target status = %d, body=%s", unauthenticatedTargetRecorder.Code, unauthenticatedTargetRecorder.Body.String())
	}

	unprotected := gin.New()
	unprotected.Use(func(c *gin.Context) {
		c.Set("authSubject", "price-admin")
		c.Next()
	})
	registerGateway(unprotected.Group("/api"), &Deps{Gateway: &gateway.Service{Store: storage.NewGateway(nil)}})
	for _, test := range []struct {
		name string
		key  string
	}{
		{name: "missing"},
		{name: "invalid characters", key: "not valid/for import"},
		{name: "too long", key: strings.Repeat("a", 129)},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(`{"items":[]}`))
			request.Header.Set(gatewayManualCooldownIdempotencyHeader, test.key)
			recorder := httptest.NewRecorder()
			unprotected.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "Idempotency-Key") {
				t.Fatalf("idempotency key %q response = %d %s", test.name, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestGatewayRequestPriceImportEndpointsRequireStringAdministratorActor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name  string
		actor any
	}{
		{name: "blank string", actor: " \t\n "},
		{name: "integer", actor: 42},
		{name: "object", actor: map[string]any{"subject": "price-admin"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			router := gin.New()
			router.Use(func(c *gin.Context) {
				c.Set("authSubject", test.actor)
				c.Next()
			})
			registerGateway(router.Group("/api"), &Deps{Gateway: &gateway.Service{Store: storage.NewGateway(nil)}})

			requests := []*http.Request{
				httptest.NewRequest(http.MethodGet, "/api/gateway/request-price-facts/import-target?channel_id=1&group_name=primary&model_name=model-a", nil),
				httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(`{"items":[]}`)),
			}
			requests[1].Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey)
			for _, request := range requests {
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusForbidden || !strings.Contains(recorder.Body.String(), "administrator authentication is required") {
					t.Fatalf("%s response = %d %s", request.Method, recorder.Code, recorder.Body.String())
				}
			}
		})
	}
}

func TestGatewayRequestPriceImportRejectsClientControlledFactFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "price-admin")
		c.Next()
	})
	registerGateway(router.Group("/api"), &Deps{Gateway: &gateway.Service{Store: storage.NewGateway(nil)}})
	for _, field := range []string{
		"source", "currency", "unit", "valid_from", "channel_account_identity_revision",
		"cost_unit_epoch", "upstream_group_id", "upstream_group_presence_generation", "gateway_upstream_key_id",
		"upstream_key_generation", "fact_version", "schema_version", "checksum", "created_at",
	} {
		t.Run(field, func(t *testing.T) {
			body := fmt.Sprintf(`{"items":[{"channel_id":1,"group_name":"primary","model_name":"model-a","operation":"embedding","protocol":"openai_embeddings","input_unit_price_usd":1,"valid_until":"2030-01-01T00:00:00Z",%q:1}]}`, field)
			request := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey+"-"+field)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "unknown field") {
				t.Fatalf("client field %q response = %d %s", field, recorder.Code, recorder.Body.String())
			}
		})
	}
	for _, field := range []string{"actor", "idempotency_key", "payload_digest", "payload_digest_version", "import_id"} {
		t.Run("top-level "+field, func(t *testing.T) {
			body := fmt.Sprintf(`{"items":[],%q:true}`, field)
			request := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey+"-top-"+field)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "unknown field") {
				t.Fatalf("client command field %q response = %d %s", field, recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestGatewayRequestPriceImportCreatesAdministratorFact(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	service, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
		channelID: 171, name: "administrator-price", groupName: "primary", siteURL: "https://price.invalid", ratio: 1,
	}})
	if err := db.Model(&storage.Channel{}).Where("id = ?", 171).Update("type", storage.ChannelTypeSub2API).Error; err != nil {
		t.Fatalf("set Sub2API price fixture provider: %v", err)
	}
	// Item 58 owns the channel cost-unit epoch migration. This isolated Item 41
	// worktree reserves but does not duplicate 000059-000061, so install the
	// upstream schema prerequisite explicitly for this integration test.
	if err := db.Exec("ALTER TABLE public.channels ADD COLUMN IF NOT EXISTS cost_unit_epoch bigint NOT NULL DEFAULT 1").Error; err != nil {
		t.Fatalf("install cost-unit epoch API fixture: %v", err)
	}
	var channel storage.Channel
	if err := db.First(&channel, 171).Error; err != nil {
		t.Fatalf("reload channel before account binding: %v", err)
	}
	if _, err := storage.NewChannels(db).ObserveUpstreamAccountIdentity(storage.UpstreamAccountIdentityObservation{
		ChannelID: channel.ID, ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channel.AdminRevision, Provider: channel.Type, APIOrigin: channel.SiteURL,
		AccountID: "administrator-price-account", Scope: storage.UpstreamConcurrencyScopeAccount,
		Source: "pricing-api-test.authenticated-account", ObservedAt: time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("bind authenticated pricing account: %v", err)
	}
	generation := decision.Candidates[0].UpstreamGeneration
	syncedAt := time.Now().UTC()
	if err := store.UpdateUpstreamKeyModelFamilies(171, "primary", "codex", "model-a", syncedAt, "", generation); err != nil {
		t.Fatalf("synchronize model identity: %v", err)
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "price-admin")
		c.Next()
	})
	registerGateway(router.Group("/api"), &Deps{Gateway: service})
	if err := db.First(&channel, 171).Error; err != nil {
		t.Fatalf("reload bound channel identity: %v", err)
	}
	key, err := store.FindUpstreamKey(171, "primary")
	if err != nil {
		t.Fatalf("reload upstream key: %v", err)
	}
	observedAt := time.Now().UTC().Truncate(time.Microsecond)
	validUntil := observedAt.Add(2 * time.Hour)
	if channel.UpstreamAccountIdentityRevision == 0 || channel.AdminRevision == 0 || generation.ID == 0 ||
		generation.PresenceGeneration == 0 || key.ID == 0 || key.KeyGeneration == 0 {
		t.Fatalf("incomplete price observation fixture: channel=%+v generation=%+v key=%+v", channel, generation, key)
	}
	targetRequest := httptest.NewRequest(http.MethodGet, "/api/gateway/request-price-facts/import-target?channel_id=171&group_name=primary&model_name=model-a", nil)
	targetRecorder := httptest.NewRecorder()
	router.ServeHTTP(targetRecorder, targetRequest)
	if targetRecorder.Code != http.StatusOK {
		t.Fatalf("administrator price target = %d %s", targetRecorder.Code, targetRecorder.Body.String())
	}
	var targetResponse struct {
		Data storage.GatewayAdministratorPriceTarget `json:"data"`
	}
	if err := json.Unmarshal(targetRecorder.Body.Bytes(), &targetResponse); err != nil {
		t.Fatalf("decode administrator price target: %v", err)
	}
	target := targetResponse.Data
	if target.ChannelID != channel.ID || target.ExpectedCostUnitEpoch != 1 || target.ExpectedUpstreamKeyID != key.ID || target.CapturedAt.IsZero() {
		t.Fatalf("administrator price target = %+v", target)
	}
	body := fmt.Sprintf(`{"items":[{"channel_id":171,"group_name":"primary","model_name":"model-a","operation":"text_generation","protocol":"openai_responses","expected_identity_revision":%d,"expected_admin_revision":%d,"expected_provider":%q,"expected_api_origin":%q,"expected_cost_unit_epoch":%d,"expected_group_id":%d,"expected_group_presence_generation":%d,"expected_upstream_key_id":%d,"expected_upstream_key_generation":%d,"observed_at":%q,"input_unit_price_usd":1.25,"output_unit_price_usd":2.5,"cache_read_unit_price_usd":0,"cache_write_unit_price_usd":1.5,"valid_until":%q}]}`,
		target.ExpectedIdentityRevision, target.ExpectedAdminRevision, target.ExpectedProvider, target.ExpectedAPIOrigin,
		target.ExpectedCostUnitEpoch, target.ExpectedGroupID, target.ExpectedGroupPresenceGeneration,
		target.ExpectedUpstreamKeyID, target.ExpectedUpstreamKeyGeneration,
		observedAt.Format(time.RFC3339Nano), validUntil.Format(time.RFC3339Nano))
	request := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("administrator import = %d %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data struct {
			ImportID         uint64                            `json:"import_id"`
			Items            []storage.GatewayRequestPriceFact `json:"items"`
			Total            int                               `json:"total"`
			IdempotentReplay bool                              `json:"idempotent_replay"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode administrator import: %v", err)
	}
	if response.Data.ImportID == 0 || response.Data.Total != 1 || len(response.Data.Items) != 1 || response.Data.IdempotentReplay {
		t.Fatalf("administrator import response = %+v", response)
	}
	fact := response.Data.Items[0]
	if err := db.Select("id", "upstream_account_identity_revision").First(&channel, 171).Error; err != nil {
		t.Fatalf("reload channel identity: %v", err)
	}
	if fact.ID == 0 || fact.ChannelAccountIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		fact.UpstreamGroupID != generation.ID || fact.UpstreamGroupPresenceGeneration != generation.PresenceGeneration ||
		fact.GatewayUpstreamKeyID != key.ID || fact.UpstreamKeyGeneration != key.KeyGeneration ||
		fact.Source != storage.GatewayRequestPriceSourceAdministrator || fact.Currency != "USD" ||
		!fact.ObservedAt.Equal(observedAt) ||
		fact.Unit != storage.GatewayPricingUnitPerMillionTokens || fact.ValidUntil == nil || !fact.ValidUntil.Equal(validUntil) ||
		fact.CacheReadUnitPrice == nil || *fact.CacheReadUnitPrice != 0 {
		t.Fatalf("administrator fact = %+v", fact)
	}
	lookup, err := store.FindCurrentRequestPriceFact(storage.GatewayRequestPriceScope{
		ChannelID: 171, ChannelAccountIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID: generation.ID, CanonicalGroupName: generation.CanonicalGroupName,
		UpstreamGroupPresenceGeneration: generation.PresenceGeneration,
		GatewayUpstreamKeyID:            key.ID, UpstreamKeyGeneration: key.KeyGeneration,
		ModelName: "model-a", Operation: storage.GatewayPricingOperationTextGeneration,
		Protocol: storage.GatewayPricingProtocolOpenAIResponses,
	}, time.Now().UTC())
	if err != nil || lookup.Fact == nil || lookup.Fact.ID != fact.ID {
		t.Fatalf("current administrator fact = %+v, err=%v", lookup, err)
	}

	replayRequest := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(body))
	replayRequest.Header.Set("Content-Type", "application/json")
	replayRequest.Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey)
	replayRecorder := httptest.NewRecorder()
	router.ServeHTTP(replayRecorder, replayRequest)
	if replayRecorder.Code != http.StatusOK {
		t.Fatalf("administrator import replay = %d %s", replayRecorder.Code, replayRecorder.Body.String())
	}
	var replayResponse struct {
		Data struct {
			ImportID         uint64                            `json:"import_id"`
			Items            []storage.GatewayRequestPriceFact `json:"items"`
			Total            int                               `json:"total"`
			IdempotentReplay bool                              `json:"idempotent_replay"`
		} `json:"data"`
	}
	if err := json.Unmarshal(replayRecorder.Body.Bytes(), &replayResponse); err != nil {
		t.Fatalf("decode administrator import replay: %v", err)
	}
	if !replayResponse.Data.IdempotentReplay || replayResponse.Data.ImportID != response.Data.ImportID || replayResponse.Data.Total != 1 || len(replayResponse.Data.Items) != 1 || replayResponse.Data.Items[0].ID != fact.ID {
		t.Fatalf("administrator import replay response = %+v", replayResponse)
	}

	payloadConflict := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(strings.Replace(body, `"input_unit_price_usd":1.25`, `"input_unit_price_usd":1.26`, 1)))
	payloadConflict.Header.Set("Content-Type", "application/json")
	payloadConflict.Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey)
	payloadConflictRecorder := httptest.NewRecorder()
	router.ServeHTTP(payloadConflictRecorder, payloadConflict)
	if payloadConflictRecorder.Code != http.StatusConflict || !strings.Contains(payloadConflictRecorder.Body.String(), "Idempotency-Key") {
		t.Fatalf("administrator import payload conflict = %d %s", payloadConflictRecorder.Code, payloadConflictRecorder.Body.String())
	}

	otherActorRouter := gin.New()
	otherActorRouter.Use(func(c *gin.Context) {
		c.Set("authSubject", "other-price-admin")
		c.Next()
	})
	registerGateway(otherActorRouter.Group("/api"), &Deps{Gateway: service})
	actorConflict := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(body))
	actorConflict.Header.Set("Content-Type", "application/json")
	actorConflict.Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey)
	actorConflictRecorder := httptest.NewRecorder()
	otherActorRouter.ServeHTTP(actorConflictRecorder, actorConflict)
	if actorConflictRecorder.Code != http.StatusConflict || !strings.Contains(actorConflictRecorder.Body.String(), "Idempotency-Key") {
		t.Fatalf("administrator import actor conflict = %d %s", actorConflictRecorder.Code, actorConflictRecorder.Body.String())
	}

	var factCount int64
	if err := db.Model(&storage.GatewayRequestPriceFact{}).
		Where("channel_id = ? AND model_name = ? AND operation = ? AND protocol = ?", 171, "model-a", storage.GatewayPricingOperationTextGeneration, storage.GatewayPricingProtocolOpenAIResponses).
		Count(&factCount).Error; err != nil || factCount != 1 {
		t.Fatalf("idempotent import fact count = %d, err=%v", factCount, err)
	}
	var command struct {
		Actor string
	}
	if err := db.Table("gateway_request_price_import_commands").Select("actor").Where("idempotency_key = ?", gatewayRequestPriceImportTestKey).Take(&command).Error; err != nil || command.Actor != "price-admin" {
		t.Fatalf("server-derived import actor = %q, err=%v", command.Actor, err)
	}
}

func TestGatewayRequestPriceImportNewAPIFailsClosedWithoutAccountBinding(t *testing.T) {
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("dedicated PostgreSQL test DSN is not configured")
	}
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	service, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
		channelID: 172, name: "administrator-price-newapi", groupName: "primary",
		siteURL: "https://price-newapi.invalid", ratio: 1,
	}})
	if err := db.Exec("ALTER TABLE public.channels ADD COLUMN IF NOT EXISTS cost_unit_epoch bigint NOT NULL DEFAULT 1").Error; err != nil {
		t.Fatalf("install cost-unit epoch API fixture: %v", err)
	}
	generation := decision.Candidates[0].UpstreamGeneration
	if err := store.UpdateUpstreamKeyModelFamilies(172, "primary", "codex", "model-a", time.Now().UTC(), "", generation); err != nil {
		t.Fatalf("synchronize NewAPI model identity: %v", err)
	}
	var channel storage.Channel
	if err := db.First(&channel, 172).Error; err != nil {
		t.Fatalf("reload NewAPI channel: %v", err)
	}
	key, err := store.FindUpstreamKey(172, "primary")
	if err != nil {
		t.Fatalf("reload NewAPI upstream key: %v", err)
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "price-admin")
		c.Next()
	})
	registerGateway(router.Group("/api"), &Deps{Gateway: service})

	targetRequest := httptest.NewRequest(http.MethodGet, "/api/gateway/request-price-facts/import-target?channel_id=172&group_name=primary&model_name=model-a", nil)
	targetRecorder := httptest.NewRecorder()
	router.ServeHTTP(targetRecorder, targetRequest)
	if targetRecorder.Code != http.StatusConflict || !strings.Contains(targetRecorder.Body.String(), "durable authenticated account binding") {
		t.Fatalf("NewAPI administrator price target = %d %s", targetRecorder.Code, targetRecorder.Body.String())
	}

	observedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	validUntil := observedAt.Add(time.Hour)
	body := fmt.Sprintf(`{"items":[{"channel_id":172,"group_name":"primary","model_name":"model-a","operation":"embedding","protocol":"openai_embeddings","expected_identity_revision":%d,"expected_admin_revision":%d,"expected_provider":%q,"expected_api_origin":%q,"expected_cost_unit_epoch":1,"expected_group_id":%d,"expected_group_presence_generation":%d,"expected_upstream_key_id":%d,"expected_upstream_key_generation":%d,"observed_at":%q,"input_unit_price_usd":1.25,"valid_until":%q}]}`,
		channel.UpstreamAccountIdentityRevision, channel.AdminRevision, channel.Type, channel.SiteURL,
		generation.ID, generation.PresenceGeneration, key.ID, key.KeyGeneration,
		observedAt.Format(time.RFC3339Nano), validUntil.Format(time.RFC3339Nano))
	request := httptest.NewRequest(http.MethodPost, "/api/gateway/request-price-facts/import", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(gatewayManualCooldownIdempotencyHeader, gatewayRequestPriceImportTestKey+"-newapi")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "durable authenticated account binding") {
		t.Fatalf("NewAPI administrator price import = %d %s", recorder.Code, recorder.Body.String())
	}
	var factCount, commandCount int64
	if err := db.Model(&storage.GatewayRequestPriceFact{}).Count(&factCount).Error; err != nil {
		t.Fatalf("count NewAPI price facts: %v", err)
	}
	if err := db.Table("gateway_request_price_import_commands").Count(&commandCount).Error; err != nil {
		t.Fatalf("count NewAPI price import commands: %v", err)
	}
	if factCount != 0 || commandCount != 0 {
		t.Fatalf("NewAPI failed import persisted facts=%d commands=%d", factCount, commandCount)
	}
}

func TestGatewayRequestPriceImportFailureMapping(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		wantStatus int
		wantText   string
		forbidden  string
	}{
		{name: "invalid", err: fmt.Errorf("wrapped: %w", storage.ErrGatewayRequestPriceImportInvalid), wantStatus: http.StatusBadRequest, wantText: "invalid"},
		{name: "target unavailable", err: storage.ErrGatewayRequestPriceImportUnavailable, wantStatus: http.StatusConflict, wantText: "unavailable"},
		{name: "identity changed", err: storage.ErrGatewayRequestPriceIdentityChanged, wantStatus: http.StatusConflict, wantText: "identity"},
		{name: "idempotency conflict", err: fmt.Errorf("private payload digest: %w", storage.ErrGatewayRequestPriceImportIdempotencyConflict), wantStatus: http.StatusConflict, wantText: "Idempotency-Key", forbidden: "private payload digest"},
		{name: "commit unknown", err: fmt.Errorf("private database detail: %w", storage.ErrGatewayRequestPriceImportCommitUnknown), wantStatus: http.StatusServiceUnavailable, wantText: "retry with the same Idempotency-Key", forbidden: "private database detail"},
		{name: "internal", err: errors.New("private database failure"), wantStatus: http.StatusInternalServerError, wantText: "request price import failed", forbidden: "private database failure"},
	} {
		t.Run(test.name, func(t *testing.T) {
			status, responseErr := gatewayRequestPriceImportFailure(test.err)
			if status != test.wantStatus || responseErr == nil || !strings.Contains(responseErr.Error(), test.wantText) {
				t.Fatalf("failure mapping status=%d err=%v", status, responseErr)
			}
			if test.forbidden != "" && strings.Contains(responseErr.Error(), test.forbidden) {
				t.Fatalf("failure mapping leaked %q in %q", test.forbidden, responseErr.Error())
			}
			wantRetryAfter := ""
			if errors.Is(test.err, storage.ErrGatewayRequestPriceImportCommitUnknown) {
				wantRetryAfter = "1"
			}
			if got := gatewayRequestPriceImportRetryAfter(test.err); got != wantRetryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, wantRetryAfter)
			}
		})
	}
}
