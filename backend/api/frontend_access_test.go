package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Harvey-Mei/BridgeOps/backend/auth"
	"github.com/gin-gonic/gin"
)

type frontendAuthProviderStub struct {
	service *auth.Service
}

func (s frontendAuthProviderStub) CurrentAuth() *auth.Service {
	return s.service
}

func TestFrontendRequiresAuthenticationBeforeServingAssets(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router, service := newProtectedFrontendRouter(t)

	root := httptest.NewRecorder()
	router.ServeHTTP(root, httptest.NewRequest(http.MethodGet, "/", nil))
	if root.Code != http.StatusFound {
		t.Fatalf("anonymous root status = %d, want %d", root.Code, http.StatusFound)
	}
	if location := root.Header().Get("Location"); location != "/login?next=%2F" {
		t.Fatalf("anonymous root location = %q", location)
	}
	if strings.Contains(root.Body.String(), "private-admin-html") {
		t.Fatal("anonymous root exposed embedded frontend")
	}

	login := httptest.NewRecorder()
	router.ServeHTTP(login, httptest.NewRequest(http.MethodGet, "/login", nil))
	if login.Code != http.StatusOK {
		t.Fatalf("anonymous login status = %d, want %d", login.Code, http.StatusOK)
	}
	for _, forbidden := range []string{"private-admin-html", "NewAPI", "Sub2API", "upstream"} {
		if strings.Contains(login.Body.String(), forbidden) {
			t.Fatalf("anonymous login exposed %q", forbidden)
		}
	}
	if strings.Contains(login.Body.String(), "%!") {
		t.Fatal("anonymous login contains an invalid format placeholder")
	}
	if !strings.Contains(login.Body.String(), "BridgeOps 登录") {
		t.Fatal("anonymous login is missing its user-facing service content")
	}
	for _, directive := range []string{"default-src 'none'", "form-action 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(login.Header().Get("Content-Security-Policy"), directive) {
			t.Fatalf("anonymous login is missing CSP directive %q", directive)
		}
	}

	asset := httptest.NewRecorder()
	router.ServeHTTP(asset, httptest.NewRequest(http.MethodGet, "/assets/app.js", nil))
	if asset.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous asset status = %d, want %d", asset.Code, http.StatusUnauthorized)
	}
	if asset.Body.Len() != 0 {
		t.Fatal("anonymous asset response must not include content")
	}

	gateway := httptest.NewRecorder()
	router.ServeHTTP(gateway, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if gateway.Code != http.StatusNoContent {
		t.Fatalf("gateway path status = %d, want %d", gateway.Code, http.StatusNoContent)
	}

	token, _, err := service.Login("admin", "test-password")
	if err != nil {
		t.Fatalf("create session token: %v", err)
	}
	authenticatedRoot := httptest.NewRecorder()
	authenticatedRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	authenticatedRequest.Header.Set("Authorization", "Bearer stale-legacy-token")
	authenticatedRequest.AddCookie(&http.Cookie{Name: authSessionCookieName, Value: token})
	router.ServeHTTP(authenticatedRoot, authenticatedRequest)
	if authenticatedRoot.Code != http.StatusOK {
		t.Fatalf("authenticated root status = %d, want %d", authenticatedRoot.Code, http.StatusOK)
	}
	if !strings.Contains(authenticatedRoot.Body.String(), "private-admin-html") {
		t.Fatal("authenticated root did not receive embedded frontend")
	}
	if cacheControl := authenticatedRoot.Header().Get("Cache-Control"); cacheControl != "no-cache, no-store, must-revalidate" {
		t.Fatalf("authenticated index cache-control = %q", cacheControl)
	}

	authenticatedAsset := httptest.NewRecorder()
	authenticatedAssetRequest := httptest.NewRequest(http.MethodGet, "/assets/app.js", nil)
	authenticatedAssetRequest.Header.Set("Authorization", "Bearer stale-legacy-token")
	authenticatedAssetRequest.AddCookie(&http.Cookie{Name: authSessionCookieName, Value: token})
	router.ServeHTTP(authenticatedAsset, authenticatedAssetRequest)
	if authenticatedAsset.Code != http.StatusOK {
		t.Fatalf("authenticated asset status = %d, want %d", authenticatedAsset.Code, http.StatusOK)
	}
	if cacheControl := authenticatedAsset.Header().Get("Cache-Control"); cacheControl != "no-cache, no-store, must-revalidate" {
		t.Fatalf("authenticated asset cache-control = %q", cacheControl)
	}
	if vary := authenticatedAsset.Header().Get("Vary"); vary != "Cookie, Authorization" {
		t.Fatalf("authenticated asset vary = %q", vary)
	}
}

func TestFrontendLoginSetsHttpOnlySessionCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router, _ := newProtectedFrontendRouter(t)
	form := url.Values{
		"username": {"admin"},
		"password": {"test-password"},
		"next":     {"/gateway/keys"},
	}
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("login status = %d, want %d", rec.Code, http.StatusSeeOther)
	}
	if location := rec.Header().Get("Location"); location != "/gateway/keys" {
		t.Fatalf("login location = %q", location)
	}
	var session *http.Cookie
	for _, cookie := range rec.Result().Cookies() {
		if cookie.Name == authSessionCookieName {
			session = cookie
			break
		}
	}
	if session == nil || session.Value == "" || !session.HttpOnly || session.Path != "/" {
		t.Fatalf("unexpected session cookie: %#v", session)
	}

	externalTarget := url.Values{"next": {"//example.invalid"}}
	externalReq := httptest.NewRequest(http.MethodGet, "/login?"+externalTarget.Encode(), nil)
	externalReq.AddCookie(session)
	externalRec := httptest.NewRecorder()
	router.ServeHTTP(externalRec, externalReq)
	if location := externalRec.Header().Get("Location"); location != "/" {
		t.Fatalf("unsafe login target location = %q", location)
	}
}

func TestFrontendLoginFailureKeepsTargetEscapedAndPasswordPrivate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router, _ := newProtectedFrontendRouter(t)
	form := url.Values{
		"username": {"admin"},
		"password": {"private-invalid-password"},
		"next":     {`/gateway/keys?filter="><img src=x onerror=alert(1)>`},
	}
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), `role="alert">账号或密码错误</p>`) {
		t.Fatal("invalid password must render a fixed accessible login error")
	}
	for _, forbidden := range []string{"private-invalid-password", "<img src=x", "%!"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("login failure exposed unsafe content %q", forbidden)
		}
	}
	if !strings.Contains(rec.Body.String(), `name="next" value="/gateway/keys?filter=&#34;&gt;&lt;img src=x onerror=alert(1)&gt;"`) {
		t.Fatal("login failure must preserve an escaped local return target")
	}
}

func newProtectedFrontendRouter(t *testing.T) (*gin.Engine, *auth.Service) {
	t.Helper()
	service, err := auth.New("admin", "test-password", "test-token-secret", 0)
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	router := gin.New()
	router.Any("/v1/*path", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})
	registerFrontend(router, fstest.MapFS{
		"index.html":    &fstest.MapFile{Data: []byte("private-admin-html")},
		"assets/app.js": &fstest.MapFile{Data: []byte("private-admin-javascript")},
		"icon.svg":      &fstest.MapFile{Data: []byte("<svg></svg>")},
	}, frontendAuthProviderStub{service: service})
	return router, service
}
