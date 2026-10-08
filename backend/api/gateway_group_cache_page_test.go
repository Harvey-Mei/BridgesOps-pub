package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayRequestLogGroupCacheCursorEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	store := storage.NewGateway(db)
	if err := storage.BackfillGatewayRequestGroupCacheAggregates(db); err != nil {
		t.Fatalf("prepare group-cache ledger: %v", err)
	}
	bucket := time.Date(2026, 7, 25, 10, 0, 0, 0, time.UTC)
	for index := 1; index <= 3; index++ {
		log := &storage.GatewayRequestLog{
			DownstreamKeyID: 5, ChannelID: uint(index), ChannelNameSnapshot: fmt.Sprintf("channel-%d", index),
			GroupName: "primary", ModelName: "model-a", Method: "POST", Path: "/v1/responses",
			RouteReason: "smart_primary", StatusCode: 200, PromptTokens: 100, CacheReadTokens: int64(index * 10), CreatedAt: bucket,
		}
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append group-cache request %d: %v", index, err)
		}
	}

	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(nil, nil, store, nil, nil, nil)})
	request := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder
	}
	decode := func(recorder *httptest.ResponseRecorder) storage.GatewayRequestLogGroupCacheRatioCursorPage {
		t.Helper()
		var payload struct {
			Data storage.GatewayRequestLogGroupCacheRatioCursorPage `json:"data"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode group-cache page: %v; body=%s", err, recorder.Body.String())
		}
		return payload.Data
	}

	firstResponse := request("/api/gateway/request-log-group-cache-ratios?page_size=2&downstream_key_id=5&model=model-a")
	if firstResponse.Code != http.StatusOK {
		t.Fatalf("first group-cache page status=%d body=%s", firstResponse.Code, firstResponse.Body.String())
	}
	first := decode(firstResponse)
	if len(first.Items) != 2 || first.NextCursor == "" || first.PageSize != 2 {
		t.Fatalf("first group-cache page = %+v", first)
	}
	secondResponse := request("/api/gateway/request-log-group-cache-ratios?page_size=2&downstream_key_id=5&model=model-a&cursor=" + first.NextCursor)
	if secondResponse.Code != http.StatusOK {
		t.Fatalf("second group-cache page status=%d body=%s", secondResponse.Code, secondResponse.Body.String())
	}
	second := decode(secondResponse)
	if len(second.Items) != 1 || second.Items[0].ChannelID != 3 || second.NextCursor != "" {
		t.Fatalf("second group-cache page = %+v", second)
	}

	for _, path := range []string{
		"/api/gateway/request-log-group-cache-ratios?page_size=101",
		"/api/gateway/request-log-group-cache-ratios?cursor=not-a-cursor",
		"/api/gateway/request-log-group-cache-ratios?page_size=2&downstream_key_id=5&model=model-b&cursor=" + first.NextCursor,
	} {
		response := request(path)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid group-cache request %q status=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}
