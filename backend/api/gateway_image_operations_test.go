package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestParseGatewayImageCapabilityEvidence(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		request      string
		response     string
		wantMode     storage.GatewayImageTaskMode
		wantIDPath   string
		wantStatus   string
		wantProtocol string
		wantFound    bool
	}{
		{name: "images sync", path: "/v1/images/generations", request: `{}`, response: `{"data":[{"b64_json":"image"}]}`, wantMode: storage.GatewayImageTaskModeSync, wantProtocol: storage.GatewayImageProtocolImages, wantFound: true},
		{name: "images async", path: "/v1/images/generations", request: `{}`, response: `{"task":{"id":"task-secret","status":"queued"}}`, wantMode: storage.GatewayImageTaskModeAsync, wantIDPath: "task.id", wantStatus: "task.status", wantProtocol: storage.GatewayImageProtocolImages, wantFound: true},
		{name: "responses sync", path: "/v1/responses", request: `{"tools":[{"type":"image_generation"}]}`, response: `{"status":"completed","output":[{"type":"image_generation_call","result":"image"}]}`, wantMode: storage.GatewayImageTaskModeSync, wantProtocol: storage.GatewayImageProtocolResponses, wantFound: true},
		{name: "responses image without result", path: "/v1/responses", request: `{"tools":[{"type":"image_generation"}]}`, response: `{"status":"completed","output":[{"type":"image_generation_call"}]}`, wantFound: false},
		{name: "responses failed image item", path: "/v1/responses", request: `{"tools":[{"type":"image_generation"}]}`, response: `{"status":"completed","output":[{"type":"image_generation_call","status":"failed","result":"image"}]}`, wantFound: false},
		{name: "responses similar output type", path: "/v1/responses", request: `{"tools":[{"type":"image_generation"}]}`, response: `{"status":"completed","output":[{"type":"custom_image_generation_notice","result":"image"}]}`, wantFound: false},
		{name: "responses missing completion status", path: "/v1/responses", request: `{"tools":[{"type":"image_generation"}]}`, response: `{"output":[{"type":"image_generation_call","result":"image"}]}`, wantFound: false},
		{name: "responses lifecycle is not async", path: "/v1/responses", request: `{"tool_choice":{"type":"image_generation"}}`, response: "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp-secret\",\"status\":\"in_progress\"}}\n\n", wantFound: false},
		{name: "responses background async", path: "/v1/responses", request: `{"background":true,"tool_choice":{"type":"image_generation"}}`, response: "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp-secret\",\"status\":\"in_progress\"}}\n\n", wantMode: storage.GatewayImageTaskModeAsync, wantIDPath: "response.id", wantStatus: "response.status", wantProtocol: storage.GatewayImageProtocolResponses, wantFound: true},
		{name: "responses completed sse", path: "/v1/responses", request: `{"tools":[{"type":"image_generation"}]}`, response: "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-secret\",\"status\":\"in_progress\"}}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-secret\",\"status\":\"completed\",\"output\":[{\"type\":\"image_generation_call\",\"result\":\"image\"}]}}\n\n", wantMode: storage.GatewayImageTaskModeSync, wantProtocol: storage.GatewayImageProtocolResponses, wantFound: true},
		{name: "unstructured success", path: "/v1/images/generations", request: `{}`, response: `{"ok":true}`, wantFound: false},
		{name: "ordinary responses", path: "/v1/responses", request: `{"tools":[{"type":"web_search"}]}`, response: `{"status":"completed"}`, wantFound: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			protocol, _, _, mode, idPath, statusPath, found := parseGatewayImageCapabilityEvidence(test.path, []byte(test.request), []byte(test.response), nil)
			if found != test.wantFound || mode != test.wantMode || idPath != test.wantIDPath || statusPath != test.wantStatus || protocol != test.wantProtocol {
				t.Fatalf("evidence = protocol=%q mode=%q id=%q status=%q found=%t", protocol, mode, idPath, statusPath, found)
			}
		})
	}
}

func TestGatewayKeyTestDoesNotRecordImageOperationEvidence(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"b64_json":"private-image-payload"}]}`))
	}))
	defer upstream.Close()
	service, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 71, name: "image", groupName: "primary", siteURL: upstream.URL, ratio: 1,
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"gpt-image-test","prompt":"draw"}`)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Set(gatewayDownstreamKeyTestContextKey, true)
	gatewayForward(ctx, &Deps{Gateway: service}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-image-test", "", body, gatewayModelMappingResult{Model: "gpt-image-test"}, time.Now())
	if recorder.Code != http.StatusOK {
		t.Fatalf("gateway response = %d %s", recorder.Code, recorder.Body.String())
	}
	items, err := store.ListCurrentImageOperationCapabilities(storage.GatewayImageOperationCapabilityFilter{ChannelID: 71})
	if err != nil || len(items) != 0 {
		t.Fatalf("key test recorded image capability: %+v, %v", items, err)
	}
}

func TestObservedImageIdempotencyHeaderRequiresStructuredConfirmation(t *testing.T) {
	requestHeaders := http.Header{"Idempotency-Key": []string{"private-value"}}
	if _, ok := observedImageIdempotencyHeader(requestHeaders, http.Header{}); ok {
		t.Fatal("request header alone confirmed idempotency support")
	}
	if _, ok := observedImageIdempotencyHeader(requestHeaders, http.Header{"Idempotency-Key": []string{"private-value"}}); ok {
		t.Fatal("echoed idempotency key confirmed semantic support")
	}
	responseHeaders := http.Header{"Idempotency-Status": []string{"created"}}
	if header, ok := observedImageIdempotencyHeader(requestHeaders, responseHeaders); !ok || header != "Idempotency-Key" {
		t.Fatalf("confirmed idempotency header = %q, %t", header, ok)
	}
	if _, ok := observedImageIdempotencyHeader(http.Header{}, responseHeaders); ok {
		t.Fatal("response metadata without a real idempotent request confirmed support")
	}
}

func TestGatewayImageCapabilityAdminConfigurationDoesNotProbeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var upstreamCalls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusPaymentRequired)
	}))
	defer upstream.Close()
	service, store, _ := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "image", groupName: "primary", siteURL: upstream.URL, ratio: 1,
	}})
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "operator:image-capability")
		c.Next()
	})
	registerGateway(router.Group("/api"), &Deps{Gateway: service})
	generation, err := store.CaptureUpstreamGroupGeneration(1, "primary")
	if err != nil {
		t.Fatalf("capture image capability target generation: %v", err)
	}
	body := gatewayImageCapabilityAdminRequestBody(t, generation, 0)
	request := httptest.NewRequest(http.MethodPatch, "/api/gateway/image-operation-capabilities", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("admin configuration = %d %s", recorder.Code, recorder.Body.String())
	}
	if upstreamCalls.Load() != 0 {
		t.Fatalf("admin configuration issued %d upstream requests", upstreamCalls.Load())
	}
	capability, err := store.FindCurrentImageOperationCapability(1, "primary", "gpt-image-test", storage.GatewayImageProtocolImages)
	if err != nil || capability == nil || capability.EvidenceSource != storage.GatewayImageEvidenceAdminConfig || capability.RecoveryComplete || capability.AutomaticReplayAllowed {
		t.Fatalf("stored admin capability = %+v, %v", capability, err)
	}
	listRequest := httptest.NewRequest(http.MethodGet, "/api/gateway/image-operation-capabilities?channel_id=1&protocol=openai_images", nil)
	listRecorder := httptest.NewRecorder()
	router.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK || !strings.Contains(listRecorder.Body.String(), `"automatic_replay_allowed":false`) || !strings.Contains(listRecorder.Body.String(), `"model_name":"gpt-image-test"`) {
		t.Fatalf("capability list = %d %s", listRecorder.Code, listRecorder.Body.String())
	}
	auditRequest := httptest.NewRequest(http.MethodGet, "/api/gateway/image-operation-capabilities/"+strconv.FormatUint(uint64(capability.ID), 10)+"/audit", nil)
	auditRecorder := httptest.NewRecorder()
	router.ServeHTTP(auditRecorder, auditRequest)
	if auditRecorder.Code != http.StatusOK || !strings.Contains(auditRecorder.Body.String(), `"action":"configured"`) || !strings.Contains(auditRecorder.Body.String(), `"actor":"operator:image-capability"`) || strings.Contains(auditRecorder.Body.String(), "private") {
		t.Fatalf("capability audit = %d %s", auditRecorder.Code, auditRecorder.Body.String())
	}
	zeroAuditRequest := httptest.NewRequest(http.MethodGet, "/api/gateway/image-operation-capabilities/0/audit", nil)
	zeroAuditRecorder := httptest.NewRecorder()
	router.ServeHTTP(zeroAuditRecorder, zeroAuditRequest)
	if zeroAuditRecorder.Code != http.StatusBadRequest {
		t.Fatalf("zero capability audit = %d %s", zeroAuditRecorder.Code, zeroAuditRecorder.Body.String())
	}
	missingAuditRequest := httptest.NewRequest(http.MethodGet, "/api/gateway/image-operation-capabilities/1000000/audit", nil)
	missingAuditRecorder := httptest.NewRecorder()
	router.ServeHTTP(missingAuditRecorder, missingAuditRequest)
	if missingAuditRecorder.Code != http.StatusNotFound {
		t.Fatalf("missing capability audit = %d %s", missingAuditRecorder.Code, missingAuditRecorder.Body.String())
	}
}

func TestGatewayImageCapabilityAdminConfigurationRejectsStaleOrMissingGeneration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service, store, _ := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 81, name: "generation-bound-image", groupName: "primary", siteURL: "https://generation-bound.invalid", ratio: 1,
	}})
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "operator:image-capability")
		c.Next()
	})
	registerGateway(router.Group("/api"), &Deps{Gateway: service})

	stale, err := store.CaptureUpstreamGroupGeneration(81, "primary")
	if err != nil {
		t.Fatalf("capture stale image target: %v", err)
	}
	missingExpected := stale
	missingExpected.ID = 0
	missingExpected.PresenceGeneration = 0
	missingGeneration := gatewayImageCapabilityAdminRequestBody(t, missingExpected, 0)
	missingRequest := httptest.NewRequest(http.MethodPatch, "/api/gateway/image-operation-capabilities", strings.NewReader(missingGeneration))
	missingRequest.Header.Set("Content-Type", "application/json")
	missingRecorder := httptest.NewRecorder()
	router.ServeHTTP(missingRecorder, missingRequest)
	if missingRecorder.Code != http.StatusBadRequest {
		t.Fatalf("missing expected generation status=%d body=%s", missingRecorder.Code, missingRecorder.Body.String())
	}

	changedAt := time.Now().UTC().Add(time.Minute)
	if err := store.RecordUpstreamGroupChange(81, "primary", "removed", 0, "", changedAt); err != nil {
		t.Fatalf("remove stale image target: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(81, "primary", "added", 82, "BOG generation-bound", changedAt.Add(time.Second)); err != nil {
		t.Fatalf("recreate image target: %v", err)
	}
	current, err := store.CaptureUpstreamGroupGeneration(81, "primary")
	if err != nil || current.ID != stale.ID || current.PresenceGeneration == stale.PresenceGeneration {
		t.Fatalf("reborn image target current=%+v stale=%+v err=%v", current, stale, err)
	}

	request := httptest.NewRequest(http.MethodPatch, "/api/gateway/image-operation-capabilities", strings.NewReader(gatewayImageCapabilityAdminRequestBody(t, stale, 0)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "upstream group changed") {
		t.Fatalf("stale expected generation status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if capability, err := store.FindCurrentImageOperationCapability(81, "primary", "gpt-image-test", storage.GatewayImageProtocolImages); err != nil || capability != nil {
		t.Fatalf("stale create lookup = %+v, %v; want no current capability", capability, err)
	}
}

func TestGatewayImageOperationTargetInventoryEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	service, store, _ := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
		channelID: 82, name: "independent-image-inventory", groupName: "primary", siteURL: "https://image-inventory.invalid", ratio: 1,
	}})
	generation, err := store.CaptureUpstreamGroupGeneration(82, "primary")
	if err != nil {
		t.Fatalf("capture image inventory generation: %v", err)
	}
	syncedAt := generation.DiscoveredAt.Add(time.Second)
	if err := store.UpdateUpstreamKeyModelFamilies(82, "primary", "codex", "image-alpha,image-beta", syncedAt, "", generation); err != nil {
		t.Fatalf("synchronize image inventory models: %v", err)
	}
	if err := db.Where("channel_id = ?", 82).Delete(&storage.RateSnapshot{}).Error; err != nil {
		t.Fatalf("remove rate projection from image inventory fixture: %v", err)
	}
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", 82, "primary").
		Updates(map[string]any{"enabled": false, "latency_probe_enabled": false}).Error; err != nil {
		t.Fatalf("disable routing and probe state in image inventory fixture: %v", err)
	}

	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: service})
	serve := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder
	}

	firstRecorder := serve("/api/gateway/image-operation-targets?page_size=1")
	if firstRecorder.Code != http.StatusOK {
		t.Fatalf("first image inventory page = %d %s", firstRecorder.Code, firstRecorder.Body.String())
	}
	var first struct {
		Data storage.GatewayImageOperationTargetInventoryPage `json:"data"`
	}
	if err := json.Unmarshal(firstRecorder.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first image inventory page: %v", err)
	}
	if len(first.Data.Items) != 1 || first.Data.Items[0].ModelName != "image-alpha" ||
		first.Data.Items[0].UpstreamGroupID != generation.ID ||
		first.Data.Items[0].PresenceGeneration != generation.PresenceGeneration ||
		first.Data.NextCursor == "" || first.Data.Snapshot == "" {
		t.Fatalf("first image inventory page = %+v", first.Data)
	}

	secondRecorder := serve("/api/gateway/image-operation-targets?page_size=1&cursor=" + first.Data.NextCursor)
	if secondRecorder.Code != http.StatusOK {
		t.Fatalf("second image inventory page = %d %s", secondRecorder.Code, secondRecorder.Body.String())
	}
	var second struct {
		Data storage.GatewayImageOperationTargetInventoryPage `json:"data"`
	}
	if err := json.Unmarshal(secondRecorder.Body.Bytes(), &second); err != nil {
		t.Fatalf("decode second image inventory page: %v", err)
	}
	if len(second.Data.Items) != 1 || second.Data.Items[0].ModelName != "image-beta" || second.Data.NextCursor != "" || second.Data.Snapshot != first.Data.Snapshot {
		t.Fatalf("second image inventory page = %+v", second.Data)
	}
	if recorder := serve("/api/gateway/image-operation-targets?page_size=501"); recorder.Code != http.StatusBadRequest {
		t.Fatalf("oversized image inventory page = %d %s", recorder.Code, recorder.Body.String())
	}
	if recorder := serve("/api/gateway/image-operation-targets?cursor=not-a-cursor"); recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid image inventory cursor = %d %s", recorder.Code, recorder.Body.String())
	}

	if err := store.UpdateUpstreamKeyModelFamilies(82, "primary", "codex", "image-alpha,image-beta,image-gamma", syncedAt.Add(time.Minute), "", generation); err != nil {
		t.Fatalf("change image inventory between pages: %v", err)
	}
	if recorder := serve("/api/gateway/image-operation-targets?page_size=1&cursor=" + first.Data.NextCursor); recorder.Code != http.StatusConflict {
		t.Fatalf("changed image inventory snapshot = %d %s", recorder.Code, recorder.Body.String())
	}
}

func gatewayImageCapabilityAdminRequestBody(t *testing.T, generation storage.UpstreamGroupGeneration, expectedVersion uint64) string {
	t.Helper()
	payload := map[string]any{
		"channel_id": generation.ChannelID, "expected_upstream_group_id": generation.ID,
		"expected_presence_generation": generation.PresenceGeneration,
		"group_name":                   generation.CanonicalGroupName, "model_name": "gpt-image-test",
		"operation": "image_generation", "protocol": "openai_images", "endpoint_path": "/v1/images/generations",
		"task_mode": "async", "idempotency_key_supported": true, "idempotency_header": "Idempotency-Key",
		"task_id_returned": true, "task_id_json_path": "id", "task_query_supported": true,
		"task_query_method": "GET", "task_query_path_template": "/v1/tasks/{task_id}", "task_status_json_path": "status",
		"task_success_values": []string{"completed"}, "task_failure_values": []string{"failed"}, "expected_version": expectedVersion,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal image capability admin request: %v", err)
	}
	return string(raw)
}

func TestObserveGatewayImageOperationSuccessLogsStorageFailure(t *testing.T) {
	service, _, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 72, name: "image", groupName: "primary", siteURL: "https://image.invalid", ratio: 1,
	}})
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-test","prompt":"draw"}`))
	requestBody := []byte(`{"model":"gpt-image-test","prompt":"draw"}`)
	responseBody := []byte(`{"data":[{"b64_json":"private-image-payload"}]}`)

	observeGatewayImageOperationSuccess(
		&Deps{Gateway: service, Log: logger},
		request,
		decision.Candidates[0],
		"gpt-image-test",
		requestBody,
		responseBody,
		nil,
		nil,
		999999,
	)

	output := logs.String()
	if !strings.Contains(output, "record gateway image operation capability evidence failed") ||
		!strings.Contains(output, "request_log_id=999999") ||
		!strings.Contains(output, "channel_id=72") {
		t.Fatalf("missing structured observation failure log: %q", output)
	}
	for _, forbidden := range []string{"private-image-payload", `\"prompt\"`, "draw"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("observation failure log leaked %q: %s", forbidden, output)
		}
	}
}

func TestGatewaySuccessfulImageRequestsRecordStructuredEvidenceOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name       string
		response   string
		wantMode   storage.GatewayImageTaskMode
		wantIDPath string
	}{
		{name: "sync", response: `{"created":1,"data":[{"b64_json":"private-image-payload"}]}`, wantMode: storage.GatewayImageTaskModeSync},
		{name: "async", response: `{"id":"task-secret-123","status":"queued"}`, wantMode: storage.GatewayImageTaskModeAsync, wantIDPath: "id"},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.Header.Get("Idempotency-Key") != "private-idempotency-value" {
					t.Errorf("upstream idempotency header = %q", request.Header.Get("Idempotency-Key"))
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Idempotency-Status", "created")
				_, _ = w.Write([]byte(test.response))
			}))
			defer upstream.Close()
			channelID := uint(index + 10)
			service, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
				channelID: channelID, name: "image", groupName: "primary", siteURL: upstream.URL, ratio: 1,
			}})
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			body := []byte(`{"model":"gpt-image-test","prompt":"draw"}`)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			ctx.Request.Header.Set("Idempotency-Key", "private-idempotency-value")
			gatewayForward(ctx, &Deps{Gateway: service}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-image-test", "", body, gatewayModelMappingResult{Model: "gpt-image-test"}, time.Now())
			if recorder.Code != http.StatusOK {
				t.Fatalf("gateway response = %d %s", recorder.Code, recorder.Body.String())
			}
			if recorder.Header().Get("Idempotency-Status") != "" {
				t.Fatal("upstream idempotency metadata leaked to downstream")
			}
			capability, err := store.FindCurrentImageOperationCapability(channelID, "primary", "gpt-image-test", storage.GatewayImageProtocolImages)
			if err != nil || capability == nil || capability.TaskMode != test.wantMode || capability.TaskIDJSONPath != test.wantIDPath || !capability.IdempotencyKeySupported || capability.EvidenceSource != storage.GatewayImageEvidenceObservedSuccess {
				t.Fatalf("observed capability = %+v, %v", capability, err)
			}
			audits, err := store.ListImageOperationCapabilityAudits(capability.ID)
			if err != nil || len(audits) != 1 || audits[0].EvidenceReferenceKind != "request_log" || audits[0].EvidenceReferenceID == 0 {
				t.Fatalf("observation audits = %+v, %v", audits, err)
			}
			raw, _ := json.Marshal(audits)
			for _, forbidden := range []string{"task-secret-123", "private-image-payload", "private-idempotency-value"} {
				if strings.Contains(string(raw), forbidden) {
					t.Fatalf("capability audit retained %q: %s", forbidden, raw)
				}
			}
		})
	}
}

func TestGatewayConfiguredAsyncImageCapabilityDoesNotEnableReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var firstCalls atomic.Int64
	var secondCalls atomic.Int64
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"type":"server_error","message":"temporary"}}`))
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"b64_json":"must-not-run"}]}`))
	}))
	defer second.Close()
	service, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 31, name: "first", groupName: "primary", siteURL: first.URL, ratio: 1},
		{channelID: 32, name: "second", groupName: "secondary", siteURL: second.URL, ratio: 2},
	})
	firstCandidate := decision.Candidates[0]
	configured := storage.GatewayImageOperationCapability{
		ChannelID: firstCandidate.Channel.ID, ModelName: "gpt-image-test", Operation: storage.GatewayImageOperationGeneration,
		Protocol: storage.GatewayImageProtocolImages, EndpointPath: "/v1/images/generations", TaskMode: storage.GatewayImageTaskModeAsync,
		IdempotencyKeySupported: true, IdempotencyHeader: "Idempotency-Key", TaskIDReturned: true, TaskIDJSONPath: "id",
		TaskQuerySupported: true, TaskQueryMethod: "GET", TaskQueryPathTemplate: "/v1/tasks/{task_id}", TaskStatusJSONPath: "status",
		TaskSuccessValues: []string{"completed"}, TaskFailureValues: []string{"failed"},
	}
	capability, err := store.SaveImageOperationCapabilityConfig(configured, firstCandidate.UpstreamGeneration, 0, "administrator")
	if err != nil {
		t.Fatal(err)
	}
	if capability.RecoveryComplete || capability.AutomaticReplayAllowed {
		t.Fatalf("configured capability enabled recovery: %+v", capability)
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"gpt-image-test","prompt":"draw"}`)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	gatewayForward(ctx, &Deps{Gateway: service}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-image-test", "", body, gatewayModelMappingResult{Model: "gpt-image-test"}, time.Now())
	if recorder.Code != http.StatusBadGateway || firstCalls.Load() != 1 || secondCalls.Load() != 0 {
		t.Fatalf("configured async failure response=%d first=%d second=%d body=%s", recorder.Code, firstCalls.Load(), secondCalls.Load(), recorder.Body.String())
	}
}
