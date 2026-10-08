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

// TestGatewayUpstreamGroupChangeHistoryCursorEndpoint keeps the public handler
// contract aligned with the storage cursor: clients may advance exactly one
// stable page at a time, but cannot reuse a cursor under another filter scope.
func TestGatewayUpstreamGroupChangeHistoryCursorEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "history cursor endpoint", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://history-cursor.example", PasswordCipher: "test", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, time.July, 24, 9, 0, 0, 0, time.UTC)
	for i, name := range []string{"first", "second", "third"} {
		change := &storage.GatewayUpstreamGroupChange{
			ChannelID: channel.ID, GroupName: name, Kind: "added", Status: "ignored",
			UpstreamKeyID: int64(i + 1), UpstreamKeyName: "managed " + name,
			DetectedAt: base.Add(time.Duration(i) * time.Minute), ResolvedAt: ptrHistoryCursorTime(base.Add(time.Duration(i+1) * time.Minute)),
		}
		if err := db.Create(change).Error; err != nil {
			t.Fatalf("create %s history change: %v", name, err)
		}
	}

	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)})
	request := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder
	}

	first := request(fmt.Sprintf("/api/gateway/upstream-group-changes/history?view=ignored&page_size=2&channel=%d", channel.ID))
	if first.Code != http.StatusOK {
		t.Fatalf("first cursor page status=%d body=%s", first.Code, first.Body.String())
	}
	firstPage := decodeGatewayHistoryCursorPage(t, first)
	if len(firstPage.Items) != 2 || strings.TrimSpace(firstPage.NextCursor) == "" {
		t.Fatalf("first cursor page=%+v", firstPage)
	}
	assertGatewayHistoryNoLegacyPagination(t, first)

	second := request(fmt.Sprintf("/api/gateway/upstream-group-changes/history?view=ignored&page_size=2&channel=%d&cursor=%s", channel.ID, firstPage.NextCursor))
	if second.Code != http.StatusOK {
		t.Fatalf("second cursor page status=%d body=%s", second.Code, second.Body.String())
	}
	secondPage := decodeGatewayHistoryCursorPage(t, second)
	if len(secondPage.Items) != 1 || secondPage.NextCursor != "" {
		t.Fatalf("second cursor page=%+v", secondPage)
	}
	for _, firstItem := range firstPage.Items {
		if firstItem.ID == secondPage.Items[0].ID {
			t.Fatalf("cursor page repeated history id %d", firstItem.ID)
		}
	}

	filterMismatch := request(fmt.Sprintf("/api/gateway/upstream-group-changes/history?view=ignored&page_size=2&channel=%d&search=third&cursor=%s", channel.ID, firstPage.NextCursor))
	if filterMismatch.Code != http.StatusBadRequest {
		t.Fatalf("filter-mismatched cursor status=%d body=%s", filterMismatch.Code, filterMismatch.Body.String())
	}
	invalid := request("/api/gateway/upstream-group-changes/history?view=ignored&cursor=not-a-cursor")
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid cursor status=%d body=%s", invalid.Code, invalid.Body.String())
	}
	invalidFilter := request("/api/gateway/upstream-group-changes/history?view=not-a-history-view")
	if invalidFilter.Code != http.StatusBadRequest {
		t.Fatalf("invalid history filter status=%d body=%s", invalidFilter.Code, invalidFilter.Body.String())
	}
	invalidPageSize := request("/api/gateway/upstream-group-changes/history?page_size=101")
	if invalidPageSize.Code != http.StatusBadRequest {
		t.Fatalf("invalid history page size status=%d body=%s", invalidPageSize.Code, invalidPageSize.Body.String())
	}
}

func ptrHistoryCursorTime(value time.Time) *time.Time { return &value }

func decodeGatewayHistoryCursorPage(t *testing.T, recorder *httptest.ResponseRecorder) storage.GatewayUpstreamGroupChangeHistoryCursorPage {
	t.Helper()
	var payload struct {
		Data storage.GatewayUpstreamGroupChangeHistoryCursorPage `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode history cursor response: %v; body=%s", err, recorder.Body.String())
	}
	return payload.Data
}

func assertGatewayHistoryNoLegacyPagination(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	var payload struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode history cursor wire contract: %v", err)
	}
	for _, field := range []string{"total", "page", "page_size", "pages"} {
		if _, found := payload.Data[field]; found {
			t.Fatalf("history cursor endpoint returned legacy %s field: %s", field, recorder.Body.String())
		}
	}
}
