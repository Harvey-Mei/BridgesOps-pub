package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayRoutingEnableRequiresCurrentChannelIdentityAndReplaysExactAudit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "routing enable identity", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://routing-enable.invalid", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: "identity-group", Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "identity-group", APIKeyCipher: "cipher", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "identity-group")
	if err != nil {
		t.Fatalf("capture generation: %v", err)
	}
	identityRevision := channel.UpstreamAccountIdentityRevision
	if identityRevision == 0 {
		t.Fatal("channel identity revision was not initialized")
	}

	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)})
	path := fmt.Sprintf("/api/gateway/upstream-groups/%d/routing-enable", generation.ID)
	request := func(idempotencyKey, body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(gatewayManualCooldownIdempotencyHeader, idempotencyKey)
		router.ServeHTTP(recorder, req)
		return recorder
	}

	missingRevision := request("routing-enable-missing-revision", fmt.Sprintf(
		`{"presence_generation":%d,"reason":"operator_one_click_enable"}`,
		generation.PresenceGeneration,
	))
	if missingRevision.Code != http.StatusBadRequest {
		t.Fatalf("missing revision status=%d body=%s", missingRevision.Code, missingRevision.Body.String())
	}

	body := fmt.Sprintf(
		`{"presence_generation":%d,"channel_identity_revision":%d,"reason":"operator_one_click_enable"}`,
		generation.PresenceGeneration, identityRevision,
	)
	initial := request("routing-enable-response-loss", body)
	if initial.Code != http.StatusOK || strings.Contains(initial.Body.String(), `"idempotent_replay":true`) {
		t.Fatalf("initial enable status=%d body=%s", initial.Code, initial.Body.String())
	}

	newIdentityRevision := identityRevision + 1
	updated := db.Model(&storage.Channel{}).
		Where("id = ? AND upstream_account_identity_revision = ?", channel.ID, identityRevision).
		Update("upstream_account_identity_revision", newIdentityRevision)
	if updated.Error != nil || updated.RowsAffected != 1 {
		t.Fatalf("replace channel identity rows=%d err=%v", updated.RowsAffected, updated.Error)
	}
	stale := request("routing-enable-stale-revision", body)
	if stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), "channel account identity changed") {
		t.Fatalf("stale revision status=%d body=%s", stale.Code, stale.Body.String())
	}

	replayAfterIdentityChange := request("routing-enable-response-loss", body)
	if replayAfterIdentityChange.Code != http.StatusOK || !strings.Contains(replayAfterIdentityChange.Body.String(), `"idempotent_replay":true`) {
		t.Fatalf("identity-change replay status=%d body=%s", replayAfterIdentityChange.Code, replayAfterIdentityChange.Body.String())
	}
	if err := db.Model(&storage.GatewayUpstreamGroup{}).Where("id = ?", generation.ID).
		Update("presence_generation", generation.PresenceGeneration+1).Error; err != nil {
		t.Fatalf("replace group generation: %v", err)
	}
	replayAfterBothChanges := request("routing-enable-response-loss", body)
	if replayAfterBothChanges.Code != http.StatusOK || !strings.Contains(replayAfterBothChanges.Body.String(), `"idempotent_replay":true`) {
		t.Fatalf("identity-and-generation replay status=%d body=%s", replayAfterBothChanges.Code, replayAfterBothChanges.Body.String())
	}
	var auditCount int64
	if err := db.Model(&storage.GatewayRoutingEnableAudit{}).Count(&auditCount).Error; err != nil || auditCount != 1 {
		t.Fatalf("routing enable audit count=%d err=%v", auditCount, err)
	}
}

func TestGatewayProbeConfigJSONIncludesChannelIdentityRevision(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "probe identity", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: "https://probe-identity.invalid", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: "probe-group", Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "probe-group", APIKeyCipher: "cipher", Enabled: true,
		ModelFamilies: "codex", ModelIDs: "gpt-test", LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}

	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/gateway/probe-config", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("probe config status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Data []struct {
			Groups []struct {
				UpstreamGroupID         uint   `json:"upstream_group_id"`
				PresenceGeneration      uint64 `json:"presence_generation"`
				ChannelIdentityRevision uint64 `json:"channel_identity_revision"`
			} `json:"groups"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode probe config: %v", err)
	}
	if len(payload.Data) != 1 || len(payload.Data[0].Groups) != 1 {
		t.Fatalf("probe config shape=%+v", payload.Data)
	}
	group := payload.Data[0].Groups[0]
	if group.UpstreamGroupID == 0 || group.PresenceGeneration == 0 || group.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision {
		t.Fatalf("probe config identity=%+v want channel revision=%d", group, channel.UpstreamAccountIdentityRevision)
	}
}

func TestGatewayRouteCandidateJSONIncludesChannelIdentityRevision(t *testing.T) {
	candidate := gateway.Candidate{
		Channel: storage.Channel{
			ID: 7, Name: "candidate identity", Type: storage.ChannelTypeNewAPI,
			UpstreamAccountIdentityRevision: 41,
		},
		Rate: storage.RateSnapshot{ChannelID: 7, ModelName: "candidate-group", Ratio: 1},
		App:  storage.GatewayAppCodex,
		UpstreamGeneration: storage.UpstreamGroupGeneration{
			ID: 19, ChannelID: 7, CanonicalGroupName: "candidate-group", PresenceGeneration: 5,
		},
	}
	body, err := json.Marshal(gatewayRouteCandidateOutputFromCandidate(candidate))
	if err != nil {
		t.Fatalf("marshal candidate: %v", err)
	}
	for _, field := range []string{
		`"upstream_group_id":19`,
		`"presence_generation":5`,
		`"channel_identity_revision":41`,
	} {
		if !strings.Contains(string(body), field) {
			t.Fatalf("candidate JSON missing %s: %s", field, body)
		}
	}
}
