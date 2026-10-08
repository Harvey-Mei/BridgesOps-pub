package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayClientIPSourceUsesForwardedAddressOnlyForTrustedPeer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		wantIP     string
		wantSource string
	}{
		{
			name:       "trusted loopback proxy",
			remoteAddr: "127.0.0.1:43120",
			xff:        "8.8.8.8",
			wantIP:     "8.8.8.8",
			wantSource: gatewayClientIPSourceTrustedProxy,
		},
		{
			name:       "untrusted peer cannot spoof forwarded address",
			remoteAddr: "8.8.4.4:43120",
			xff:        "1.1.1.1",
			wantIP:     "8.8.4.4",
			wantSource: gatewayClientIPSourceDirect,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			if err := router.SetTrustedProxies([]string{"127.0.0.1", "::1"}); err != nil {
				t.Fatalf("set trusted proxies: %v", err)
			}
			var gotIP, gotSource string
			router.GET("/", func(c *gin.Context) {
				gotIP = gatewayClientIP(c)
				gotSource = gatewayClientIPSource(c)
				c.Status(http.StatusNoContent)
			})

			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.RemoteAddr = tt.remoteAddr
			request.Header.Set("X-Forwarded-For", tt.xff)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusNoContent {
				t.Fatalf("status = %d", recorder.Code)
			}
			if gotIP != tt.wantIP || gotSource != tt.wantSource {
				t.Fatalf("client identity = %q/%q, want %q/%q", gotIP, gotSource, tt.wantIP, tt.wantSource)
			}
		})
	}
}

func TestGatewayRequestLogListKeepsClientIPSource(t *testing.T) {
	db := openTestDB(t)
	if !db.Migrator().HasColumn(&storage.GatewayRequestLog{}, "client_ip_source") {
		t.Fatal("gateway request logs are missing client_ip_source")
	}
	store := storage.NewGateway(db)
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{
		DownstreamKeyID: 1,
		ChannelID:       1,
		ModelName:       "gpt-test",
		Method:          http.MethodPost,
		Path:            "/v1/responses",
		StatusCode:      http.StatusOK,
		ClientIP:        "8.8.8.8",
		ClientIPSource:  gatewayClientIPSourceTrustedProxy,
		CreatedAt:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	items, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("list request logs = %#v, %v", items, err)
	}
	if items[0].ClientIP != "8.8.8.8" || items[0].ClientIPSource != gatewayClientIPSourceTrustedProxy {
		t.Fatalf("request log client IP provenance = %#v", items[0])
	}
	paged, total, _, err := store.ListRequestLogsPageFiltered(1, 1, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(paged) != 1 {
		t.Fatalf("page request logs = %#v, total=%d, err=%v", paged, total, err)
	}
	if paged[0].ClientIPSource != gatewayClientIPSourceTrustedProxy {
		t.Fatalf("paged request log client IP provenance = %#v", paged[0])
	}
}
