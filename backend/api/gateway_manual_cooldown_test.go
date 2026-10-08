package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestGatewayRequestLogRoutingCooldownRequiresAdminAndUsesStoredRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	store := storage.NewGateway(db)
	channel := storage.Channel{Name: "stored-route", Type: storage.ChannelTypeNewAPI, SiteURL: "https://stored.example", PasswordCipher: "test"}
	if err := db.Create(&channel).Error; err != nil {
		t.Fatal(err)
	}
	key := storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "stored-group", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", Enabled: true,
	}
	if err := db.Create(&key).Error; err != nil {
		t.Fatal(err)
	}
	requestLog := storage.GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: "stored-group",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, FirstByteMs: 45_000, LatencyMs: 46_000,
		RouteReason: gatewayRouteReasonSmartPrimary, CreatedAt: time.Now().UTC(),
	}
	if _, err := storage.NewRates(db).Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: requestLog.GroupName, Ratio: 1, LastSeenAt: requestLog.CreatedAt,
	}); err != nil {
		t.Fatalf("create routing generation rate: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, requestLog.GroupName)
	if err != nil {
		t.Fatalf("capture routing generation: %v", err)
	}
	requestLog.ChannelIdentityRevision = channel.UpstreamAccountIdentityRevision
	requestLog.UpstreamGroupID = generation.ID
	requestLog.UpstreamGroupGeneration = generation.PresenceGeneration
	requestLog.UpstreamKeyLocalID = key.ID
	requestLog.UpstreamKeyGeneration = key.KeyGeneration
	requestLog.ModelName = "gpt-test"
	requestLog.PriceCanonicalGroupName = generation.CanonicalGroupName
	requestLog.PriceModelName = requestLog.ModelName
	requestLog.PriceOperation = storage.GatewayPricingOperationTextGeneration
	requestLog.PriceProtocol = storage.GatewayPricingProtocolOpenAIResponses
	requestLog.UsageSchema = storage.GatewayPricingUsageSchemaTokensV1
	requestLog.UsageSchemaVersion = 1
	if err := db.Create(&requestLog).Error; err != nil {
		t.Fatal(err)
	}

	authService, err := auth.New("admin", "password", "gateway-manual-cooldown-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	runtime := runtimeconfig.New("", "", nil, nil, nil, authService, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil)
	gatewayService := gateway.NewService(nil, nil, store, nil, nil, nil)
	gatewayService.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{
		ResponseTimeout: time.Second,
		ManualCooldown:  7 * time.Minute,
	}})
	router := gin.New()
	api := router.Group("/api")
	api.Use(runtime.AuthMiddleware())
	registerGateway(api, &Deps{Runtime: runtime, Gateway: gatewayService})
	path := "/api/gateway/request-logs/" + strconv.FormatUint(uint64(requestLog.ID), 10) + "/routing-cooldown"

	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, path, nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, body=%s", unauthorized.Code, unauthorized.Body.String())
	}

	token, _, err := authService.Login("admin", "password")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"channel_id":999999,"group_name":"forged"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(gatewayManualCooldownIdempotencyHeader, "request-log-cooldown-test")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("manual cooldown status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data storage.GatewayManualRoutingCooldownResult `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Data.ChannelID != channel.ID || response.Data.GroupName != "stored-group" {
		t.Fatalf("manual cooldown trusted forged route: %+v", response.Data)
	}
	stored, err := store.FindUpstreamKey(channel.ID, "stored-group")
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastRoutingFailureKind != gateway.RoutingFailureManualCooldown || stored.RoutingCooldownUntil == nil || time.Until(*stored.RoutingCooldownUntil) < 6*time.Minute {
		t.Fatalf("stored cooldown = %+v", stored)
	}
	var audit storage.GatewayManualRoutingCooldownAudit
	if err := db.Where("request_log_id = ?", requestLog.ID).First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	if audit.Actor != "admin" || audit.ChannelID != channel.ID || audit.GroupName != "stored-group" || audit.DurationSeconds != 7*60 {
		t.Fatalf("manual cooldown audit = %+v", audit)
	}

	replay := httptest.NewRecorder()
	replayRequest := httptest.NewRequest(http.MethodPost, path, nil)
	replayRequest.Header.Set("Authorization", "Bearer "+token)
	replayRequest.Header.Set(gatewayManualCooldownIdempotencyHeader, "request-log-cooldown-test")
	router.ServeHTTP(replay, replayRequest)
	if replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), `"idempotent_replay":true`) {
		t.Fatalf("idempotent replay = %d %s", replay.Code, replay.Body.String())
	}
	var auditCount int64
	if err := db.Model(&storage.GatewayManualRoutingCooldownAudit{}).Where("request_log_id = ?", requestLog.ID).Count(&auditCount).Error; err != nil || auditCount != 1 {
		t.Fatalf("manual cooldown audit count = %d, err=%v", auditCount, err)
	}
}

func TestGatewayRequestLogRoutingCooldownRequiresIdempotencyKeyAndSlowRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	store := storage.NewGateway(db)
	channel := storage.Channel{Name: "stored-route", Type: storage.ChannelTypeNewAPI, SiteURL: "https://stored.example", PasswordCipher: "test"}
	if err := db.Create(&channel).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "stored-group", APIKeyCipher: "cipher", Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	requestLog := storage.GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: "stored-group",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, FirstByteMs: 30_000, LatencyMs: 31_000, CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(&requestLog).Error; err != nil {
		t.Fatal(err)
	}

	authService, err := auth.New("admin", "password", "gateway-manual-cooldown-secret", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	runtime := runtimeconfig.New("", "", nil, nil, nil, authService, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil)
	gatewayService := gateway.NewService(nil, nil, store, nil, nil, nil)
	router := gin.New()
	api := router.Group("/api")
	api.Use(runtime.AuthMiddleware())
	registerGateway(api, &Deps{Runtime: runtime, Gateway: gatewayService})
	path := "/api/gateway/request-logs/" + strconv.FormatUint(uint64(requestLog.ID), 10) + "/routing-cooldown"
	token, _, err := authService.Login("admin", "password")
	if err != nil {
		t.Fatal(err)
	}

	missingKey := httptest.NewRequest(http.MethodPost, path, nil)
	missingKey.Header.Set("Authorization", "Bearer "+token)
	missingRecorder := httptest.NewRecorder()
	router.ServeHTTP(missingRecorder, missingKey)
	if missingRecorder.Code != http.StatusBadRequest {
		t.Fatalf("missing idempotency status = %d, body=%s", missingRecorder.Code, missingRecorder.Body.String())
	}

	ineligible := httptest.NewRequest(http.MethodPost, path, nil)
	ineligible.Header.Set("Authorization", "Bearer "+token)
	ineligible.Header.Set(gatewayManualCooldownIdempotencyHeader, "request-log-not-slow")
	ineligibleRecorder := httptest.NewRecorder()
	router.ServeHTTP(ineligibleRecorder, ineligible)
	if ineligibleRecorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("ineligible status = %d, body=%s", ineligibleRecorder.Code, ineligibleRecorder.Body.String())
	}
}
