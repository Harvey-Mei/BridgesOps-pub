package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/codexradar"
	"github.com/gin-gonic/gin"
)

func TestCodexRadarEndpointsExposeCacheAndManualRefresh(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"community_confirmed"}`))
	}))
	t.Cleanup(server.Close)

	radar := codexradar.New(codexradar.Options{URL: server.URL, Client: server.Client()})
	r := gin.New()
	registerCodexRadar(r.Group("/api"), &Deps{CodexRadar: radar})

	getReq := httptest.NewRequest(http.MethodGet, "/api/codex-radar", nil)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, body = %s", getRec.Code, getRec.Body.String())
	}

	refreshReq := httptest.NewRequest(http.MethodPost, "/api/codex-radar/refresh", nil)
	refreshRec := httptest.NewRecorder()
	r.ServeHTTP(refreshRec, refreshReq)
	if refreshRec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, body = %s", refreshRec.Code, refreshRec.Body.String())
	}
	var body struct {
		Data codexradar.Snapshot `json:"data"`
	}
	if err := json.Unmarshal(refreshRec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode POST response: %v", err)
	}
	if string(body.Data.Payload) != `{"status":"community_confirmed"}` {
		t.Fatalf("payload = %s", body.Data.Payload)
	}

	cachedReq := httptest.NewRequest(http.MethodGet, "/api/codex-radar", nil)
	cachedRec := httptest.NewRecorder()
	r.ServeHTTP(cachedRec, cachedReq)
	if cachedRec.Code != http.StatusOK || !json.Valid(cachedRec.Body.Bytes()) {
		t.Fatalf("cached GET status/body = %d/%s", cachedRec.Code, cachedRec.Body.String())
	}
}

func TestCodexRadarRefreshReturnsStaleDataOnUpstreamFailure(t *testing.T) {
	var failed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failed.Load() {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"status":"community_confirmed"}`))
	}))
	t.Cleanup(server.Close)

	radar := codexradar.New(codexradar.Options{URL: server.URL, Client: server.Client()})
	if _, err := radar.Refresh(context.Background()); err != nil {
		t.Fatalf("seed refresh: %v", err)
	}
	failed.Store(true)

	r := gin.New()
	registerCodexRadar(r.Group("/api"), &Deps{CodexRadar: radar})
	req := httptest.NewRequest(http.MethodPost, "/api/codex-radar/refresh", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Data         codexradar.Snapshot `json:"data"`
		RefreshError string              `json:"refresh_error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode failure response: %v", err)
	}
	if body.RefreshError == "" || !body.Data.Stale {
		t.Fatalf("failure response = %+v", body)
	}
}
