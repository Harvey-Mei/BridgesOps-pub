package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/geoip"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

type gatewayClientIPGeoLookupFunc func(string) (geoip.Result, error)

func (f gatewayClientIPGeoLookupFunc) Lookup(ip string) (geoip.Result, error) {
	return f(ip)
}

func TestGatewayRequestLogClientIPGeoUsesStoredIP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	store := storage.NewGateway(db)
	requestLog := &storage.GatewayRequestLog{
		DownstreamKeyID: 1,
		ChannelID:       1,
		ModelName:       "gpt-test",
		Method:          http.MethodPost,
		Path:            "/v1/responses",
		StatusCode:      http.StatusOK,
		ClientIP:        "8.8.8.8",
		ClientIPSource:  gatewayClientIPSourceTrustedProxy,
		CreatedAt:       time.Now().UTC(),
	}
	if err := store.AppendRequestLog(requestLog); err != nil {
		t.Fatalf("append request log: %v", err)
	}

	var lookupIP string
	lookup := gatewayClientIPGeoLookupFunc(func(ip string) (geoip.Result, error) {
		lookupIP = ip
		return geoip.Result{
			IP: "8.8.8.8", Provider: "local_maxmind", Country: "United States", CountryCode: "US",
			Region: "California", City: "Mountain View", AccuracyRadiusKM: 20, ASN: "AS64500", Organization: "Example ISP",
		}, nil
	})
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: &gateway.Service{Store: store}, GeoIP: lookup})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/gateway/request-logs/"+strconv.FormatUint(uint64(requestLog.ID), 10)+"/client-ip-geo?ip=1.1.1.1", nil)
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if lookupIP != "8.8.8.8" {
		t.Fatalf("local lookup IP = %q, want stored request-log IP", lookupIP)
	}
	if strings.Contains(recorder.Body.String(), "1.1.1.1") {
		t.Fatalf("response unexpectedly contains caller-supplied query IP: %s", recorder.Body.String())
	}
	var response struct {
		Data struct {
			IP       string `json:"ip"`
			Provider string `json:"provider"`
			Source   string `json:"client_ip_source"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.IP != "8.8.8.8" || response.Data.Provider != "local_maxmind" || response.Data.Source != gatewayClientIPSourceTrustedProxy {
		t.Fatalf("response data = %#v", response.Data)
	}
}

func TestGatewayRequestLogClientIPGeoUsesSafeStatusResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	store := storage.NewGateway(db)
	requestLog := &storage.GatewayRequestLog{
		DownstreamKeyID: 1,
		ChannelID:       1,
		ModelName:       "gpt-test",
		Method:          http.MethodPost,
		Path:            "/v1/responses",
		StatusCode:      http.StatusOK,
		ClientIP:        "8.8.8.8",
		CreatedAt:       time.Now().UTC(),
	}
	if err := store.AppendRequestLog(requestLog); err != nil {
		t.Fatalf("append request log: %v", err)
	}

	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "local database unavailable", err: geoip.ErrUnavailable, want: http.StatusServiceUnavailable},
		{name: "private recorded address", err: geoip.ErrInvalidIP, want: http.StatusUnprocessableEntity},
		{name: "not in local database", err: geoip.ErrNotFound, want: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			registerGateway(router.Group("/api"), &Deps{
				Gateway: &gateway.Service{Store: store},
				GeoIP: gatewayClientIPGeoLookupFunc(func(string) (geoip.Result, error) {
					return geoip.Result{}, tt.err
				}),
			})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/gateway/request-logs/"+strconv.FormatUint(uint64(requestLog.ID), 10)+"/client-ip-geo", nil))
			if recorder.Code != tt.want {
				t.Fatalf("status = %d, want %d, body = %s", recorder.Code, tt.want, recorder.Body.String())
			}
			if strings.Contains(recorder.Body.String(), "8.8.8.8") {
				t.Fatalf("error response leaks recorded IP: %s", recorder.Body.String())
			}
		})
	}
}
