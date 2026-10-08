package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/auth"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/gin-gonic/gin"
)

func TestLoginSetsHttpOnlyCookieWithoutTokenPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	authSvc, err := auth.New("admin", "password", "test-token-secret", 0)
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	runtime := runtimeconfig.New("", "test-secret", nil, nil, nil, authSvc, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil)
	router := gin.New()
	api := router.Group("/api")
	api.Use(runtime.AuthMiddleware())
	registerAuth(api, &Deps{Runtime: runtime})

	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewBufferString(`{"username":"admin","password":"password"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	if _, exists := response.Data["token"]; exists {
		t.Fatalf("login response exposes bearer token: %s", rec.Body.String())
	}
	var session *http.Cookie
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == authSessionCookieName {
			session = cookie
			break
		}
	}
	if session == nil || !session.HttpOnly || session.Value == "" {
		t.Fatalf("login session cookie = %#v", session)
	}
}

func TestLogoutClearsSessionWithoutCurrentCredential(t *testing.T) {
	gin.SetMode(gin.TestMode)
	authSvc, err := auth.New("admin", "password", "test-token-secret", 0)
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	router := gin.New()
	api := router.Group("/api")
	api.Use(authSvc.Middleware())
	registerAuth(api, &Deps{})

	req := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	req.Header.Set("Authorization", "Bearer stale-legacy-token")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("logout status = %d, want %d", rec.Code, http.StatusOK)
	}
	var cleared *http.Cookie
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == authSessionCookieName {
			cleared = cookie
			break
		}
	}
	if cleared == nil || cleared.MaxAge >= 0 || !cleared.HttpOnly || cleared.Path != "/" {
		t.Fatalf("logout cookie = %#v", cleared)
	}
}
