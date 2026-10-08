package sub2api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
)

func TestSetHTTPConfigAppliesUserAgentAndTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "custom-agent" {
			t.Fatalf("user agent = %q", got)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{}}`))
	}))
	defer srv.Close()

	c := New()
	c.SetHTTPConfig(connector.HTTPConfig{
		Timeout:   45 * time.Second,
		UserAgent: "custom-agent",
	})
	if c.http.GetClient().Timeout != 45*time.Second {
		t.Fatalf("timeout = %s", c.http.GetClient().Timeout)
	}
	transport, ok := c.http.GetClient().Transport.(*http.Transport)
	if !ok || transport.TLSHandshakeTimeout != 45*time.Second {
		t.Fatalf("TLS handshake timeout = %#v", c.http.GetClient().Transport)
	}
	if _, err := c.getJSON(context.Background(), srv.URL, nil); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
}

func TestGetJSONClassifiesHTMLAsAPIOriginMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!doctype html><html><body><script>secret-upstream-page</script></body></html>`))
	}))
	defer srv.Close()

	_, err := New().getJSON(context.Background(), srv.URL+"/api/v1/groups/available", &connector.AuthSession{AccessToken: "token"})
	if err == nil {
		t.Fatal("getJSON succeeded, want protocol mismatch")
	}
	if !connector.IsAPIOriginMismatch(err) {
		t.Fatalf("getJSON error = %v, want API origin mismatch classification", err)
	}
	if !strings.Contains(err.Error(), "网页 HTML") {
		t.Fatalf("getJSON error = %q, want fixed HTML hint", err)
	}
	if strings.Contains(err.Error(), "secret-upstream-page") {
		t.Fatalf("getJSON error leaked upstream HTML body: %q", err)
	}
}

func TestGetJSONClassifiesHTMLDocumentWithoutContentType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<html><body>frontend shell</body></html>`))
	}))
	defer srv.Close()

	_, err := New().getJSON(context.Background(), srv.URL+"/api/v1/groups/rates", nil)
	if err == nil || !connector.IsAPIOriginMismatch(err) {
		t.Fatalf("getJSON error = %v, want API origin mismatch classification", err)
	}
}

func TestGetJSONAcceptsJSONWithIncorrectHTMLContentType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"ok":true}}`))
	}))
	defer srv.Close()

	body, err := New().getJSON(context.Background(), srv.URL+"/api/v1/groups/available", nil)
	if err != nil || string(body) != `{"ok":true}` {
		t.Fatalf("getJSON body=%s err=%v, want valid JSON response", body, err)
	}
}

func TestGetJSONClassifiesMissingSub2APIEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	_, err := New().getJSON(context.Background(), srv.URL+"/api/v1/groups/available", nil)
	if err == nil || !connector.IsAPIOriginMismatch(err) {
		t.Fatalf("getJSON error = %v, want API origin mismatch classification", err)
	}
	var httpErr *connector.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound {
		t.Fatalf("getJSON error = %v, want wrapped HTTP 404", err)
	}
}

func TestLoginAddsExtraParams(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["email"] != "u" || body["password"] != "p" || body["device_id"] != "d1" {
			t.Fatalf("body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"access_token":"token","refresh_token":"refresh","expires_in":3600}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session, err := c.Login(context.Background(), &connector.Channel{
		SiteURL:          srv.URL,
		Username:         "u",
		Password:         "p",
		LoginExtraParams: map[string]any{"device_id": "d1"},
	})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.AccessToken != "token" {
		t.Fatalf("session = %#v", session)
	}
	if session.RefreshToken != "refresh" {
		t.Fatalf("refresh token = %q, want refresh", session.RefreshToken)
	}
}

func TestLoginCapturesExplicitAccountConcurrency(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"access_token":"token","expires_in":3600,"user":{"id":42,"concurrency":3,"current_concurrency":99}}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	session, err := New().Login(context.Background(), &connector.Channel{SiteURL: srv.URL, Username: "u", Password: "p"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.UserID != "" || session.AccountConcurrency == nil || session.AccountConcurrency.Limit == nil || session.AccountConcurrency.AccountID != "42" {
		t.Fatalf("session account fact = %#v", session)
	}
	if got := *session.AccountConcurrency.Limit; got != 3 {
		t.Fatalf("concurrency limit = %d, want 3", got)
	}
	if session.AccountConcurrency.Provider != connector.TypeSub2API ||
		session.AccountConcurrency.Scope != connector.AccountConcurrencyScopeAccount ||
		session.AccountConcurrency.Source != sub2APIConcurrencySourceLogin ||
		session.AccountConcurrency.ObservedAt.IsZero() {
		t.Fatalf("account concurrency provenance = %#v", session.AccountConcurrency)
	}
}

func TestSub2APIAccountProfileRequiresCanonicalExplicitFields(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		payload   string
		nested    bool
		wantID    string
		wantLimit *int
	}{
		{name: "bounded", payload: `{"id":7,"concurrency":4}`, wantID: "7", wantLimit: intPointer(4)},
		{name: "zero is explicit unlimited", payload: `{"id":7,"concurrency":0}`, wantID: "7", wantLimit: intPointer(0)},
		{name: "negative is explicit unlimited", payload: `{"id":7,"concurrency":-1}`, wantID: "7", wantLimit: intPointer(0)},
		{name: "nested login user", payload: `{"user":{"id":"account-7","concurrency":2}}`, nested: true, wantID: "account-7", wantLimit: intPointer(2)},
		{name: "missing limit is unknown", payload: `{"id":7}`, wantID: "7"},
		{name: "null limit is unknown", payload: `{"id":7,"concurrency":null}`, wantID: "7"},
		{name: "key current counter is not a limit", payload: `{"id":7,"current_concurrency":12}`, wantID: "7"},
		{name: "malformed limit is unknown", payload: `{"id":7,"concurrency":"4"}`, wantID: "7"},
		{name: "missing account id cannot form fact", payload: `{"concurrency":4}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accountID, fact := sub2APIAccountProfile(json.RawMessage(test.payload), test.nested, sub2APIConcurrencySourceAuthMe, now)
			if accountID != test.wantID {
				t.Fatalf("account id = %q, want %q", accountID, test.wantID)
			}
			if test.wantLimit == nil {
				if test.wantID == "" {
					if fact != nil {
						t.Fatalf("fact = %#v, want no account identity", fact)
					}
					return
				}
				if fact == nil || fact.AccountID != test.wantID || fact.Limit != nil {
					t.Fatalf("fact = %#v, want authenticated account with unknown limit", fact)
				}
				if !fact.ObservedAt.Equal(now) || fact.Source != sub2APIConcurrencySourceAuthMe {
					t.Fatalf("unknown fact provenance = %#v", fact)
				}
				return
			}
			if fact == nil || fact.Limit == nil || *fact.Limit != *test.wantLimit {
				t.Fatalf("fact = %#v, want limit %d", fact, *test.wantLimit)
			}
			if !fact.ObservedAt.Equal(now) || fact.Source != sub2APIConcurrencySourceAuthMe {
				t.Fatalf("fact provenance = %#v", fact)
			}
		})
	}
}

func intPointer(value int) *int { return &value }

func TestLoginExplainsFrontendOrigin404(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	_, err := New().Login(context.Background(), &connector.Channel{
		SiteURL:  srv.URL,
		Username: "u",
		Password: "p",
	})
	if err == nil {
		t.Fatal("Login succeeded, want 404 error")
	}
	for _, want := range []string{"status 404", "API 服务域名", "网页前端域名"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Login error = %q, want %q", err, want)
		}
	}
	var httpErr *connector.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusNotFound {
		t.Fatalf("Login error = %v, want wrapped HTTP 404", err)
	}
}

func TestDiscoversAPIOriginFromFrontendBundleBeforeLogin(t *testing.T) {
	var publicRequests atomic.Int32
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		publicRequests.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	apiMux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"access_token":"token","refresh_token":"refresh","expires_in":3600}}`))
	})
	apiMux.HandleFunc("/api/v1/auth/me", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"balance":1}}`))
	})
	apiServer := httptest.NewServer(apiMux)
	defer apiServer.Close()

	pageMux := http.NewServeMux()
	pageMux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<html><body><script src="/assets/app.js"></script></body></html>`))
	})
	pageMux.HandleFunc("/assets/app.js", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `const apiOrigin=%q;const apiBase=apiOrigin+"/api/v1";`, apiServer.URL)
	})
	pageServer := httptest.NewServer(pageMux)
	defer pageServer.Close()

	ch := &connector.Channel{SiteURL: pageServer.URL, Username: "u", Password: "p"}
	client := New()
	siteKey, err := client.GetTurnstileSiteKey(context.Background(), ch)
	if err != nil {
		t.Fatalf("GetTurnstileSiteKey: %v", err)
	}
	if siteKey != "" || ch.SiteURL != apiServer.URL {
		t.Fatalf("discovery result site_key=%q site_url=%q, want %q", siteKey, ch.SiteURL, apiServer.URL)
	}
	session, err := client.Login(context.Background(), ch)
	if err != nil {
		t.Fatalf("Login after discovery: %v", err)
	}
	if session.AccessToken != "token" {
		t.Fatalf("access token = %q", session.AccessToken)
	}
	if got := publicRequests.Load(); got != 1 {
		t.Fatalf("public settings requests after login = %d, want 1; discovery settings should be reused", got)
	}
	tokenChannel := &connector.Channel{SiteURL: pageServer.URL}
	if err := client.CheckAuth(context.Background(), tokenChannel, &connector.AuthSession{AccessToken: "token"}); err != nil {
		t.Fatalf("CheckAuth with discovery: %v", err)
	}
	if tokenChannel.SiteURL != apiServer.URL {
		t.Fatalf("token discovery site URL = %q, want %q", tokenChannel.SiteURL, apiServer.URL)
	}
}

func TestLoginDiscoversBeforeSendingCredentials(t *testing.T) {
	var homepageLoginRequests atomic.Int32
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	apiMux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode API login body: %v", err)
		}
		if body["password"] != "secret" {
			t.Fatalf("API login body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"access_token":"token","expires_in":3600}}`))
	})
	apiServer := httptest.NewServer(apiMux)
	defer apiServer.Close()

	pageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			homepageLoginRequests.Add(1)
		}
		_, _ = fmt.Fprintf(w, `<script>const apiOrigin=%q;</script>`, apiServer.URL)
	}))
	defer pageServer.Close()

	ch := &connector.Channel{SiteURL: pageServer.URL, Username: "user", Password: "secret"}
	session, err := New().Login(context.Background(), ch)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.AccessToken != "token" || ch.SiteURL != apiServer.URL {
		t.Fatalf("session=%#v site_url=%q", session, ch.SiteURL)
	}
	if got := homepageLoginRequests.Load(); got != 0 {
		t.Fatalf("credential-bearing homepage login requests = %d, want 0", got)
	}
}

func TestLoginRediscoversFromHomepageWhenStoredAPIURLIsStale(t *testing.T) {
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	apiMux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"access_token":"token","expires_in":3600}}`))
	})
	apiServer := httptest.NewServer(apiMux)
	defer apiServer.Close()

	pageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `<script>const apiOrigin=%q;</script>`, apiServer.URL)
	}))
	defer pageServer.Close()

	var staleLoginRequests atomic.Int32
	staleServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/auth/login" {
			staleLoginRequests.Add(1)
		}
		http.NotFound(w, r)
	}))
	defer staleServer.Close()

	channel := &connector.Channel{
		SiteURL:        staleServer.URL,
		HomepageURL:    pageServer.URL,
		APIOriginKnown: true,
		Username:       "user",
		Password:       "secret",
	}
	session, err := New().Login(context.Background(), channel)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.AccessToken != "token" || channel.SiteURL != apiServer.URL || channel.HomepageURL != pageServer.URL {
		t.Fatalf("session=%#v site=%q homepage=%q", session, channel.SiteURL, channel.HomepageURL)
	}
	if got := staleLoginRequests.Load(); got != 1 {
		t.Fatalf("credential requests sent to persisted stale API URL = %d, want 1", got)
	}
}

func TestKnownAPIOriginSkipsPublicSettingsBeforeAuthenticatedOperations(t *testing.T) {
	var settingsRequests atomic.Int32
	var loginRequests atomic.Int32
	var authRequests atomic.Int32
	var homepageRequests atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		settingsRequests.Add(1)
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		loginRequests.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"access_token":"token","expires_in":3600}}`))
	})
	mux.HandleFunc("/api/v1/auth/me", func(w http.ResponseWriter, _ *http.Request) {
		authRequests.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"balance":1}}`))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		homepageRequests.Add(1)
		http.Error(w, "not found", http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	channel := &connector.Channel{
		SiteURL:        strings.TrimRight(server.URL, "/"),
		HomepageURL:    strings.TrimRight(server.URL, "/") + "/",
		APIOriginKnown: true,
		Username:       "user",
		Password:       "secret",
	}
	session, err := New().Login(context.Background(), channel)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.AccessToken != "token" {
		t.Fatalf("access token = %q, want token", session.AccessToken)
	}
	if err := New().CheckAuth(context.Background(), channel, session); err != nil {
		t.Fatalf("CheckAuth: %v", err)
	}
	if got := settingsRequests.Load(); got != 0 {
		t.Fatalf("public settings requests = %d, want 0", got)
	}
	if got := homepageRequests.Load(); got != 0 {
		t.Fatalf("homepage requests = %d, want 0", got)
	}
	if got := loginRequests.Load(); got != 1 {
		t.Fatalf("login requests = %d, want 1", got)
	}
	if got := authRequests.Load(); got != 1 {
		t.Fatalf("auth requests = %d, want 1", got)
	}
}

func TestCheckAuthAndBalanceCaptureAuthMeAccountConcurrency(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/me", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"id":51,"balance":2.5,"concurrency":0,"current_concurrency":8}}`))
	})
	mux.HandleFunc("/api/v1/settings/recharge-multiplier", func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	channel := &connector.Channel{SiteURL: server.URL, APIOriginKnown: true}
	session := &connector.AuthSession{AccessToken: "token"}
	client := New()
	if err := client.CheckAuth(context.Background(), channel, session); err != nil {
		t.Fatalf("CheckAuth: %v", err)
	}
	if session.UserID != "" || session.AccountConcurrency == nil || session.AccountConcurrency.AccountID != "51" || session.AccountConcurrency.Limit == nil || *session.AccountConcurrency.Limit != 0 {
		t.Fatalf("auth/me session fact = %#v", session)
	}
	if session.AccountConcurrency.Source != sub2APIConcurrencySourceAuthMe {
		t.Fatalf("auth/me fact source = %q", session.AccountConcurrency.Source)
	}

	balance, err := client.GetBalance(context.Background(), channel, session)
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if balance.Balance != 2.5 || balance.AccountConcurrency == nil || balance.AccountConcurrency.Limit == nil || *balance.AccountConcurrency.Limit != 0 {
		t.Fatalf("balance account fact = %#v", balance)
	}
}

func TestCheckAuthReplacesOldAccountLimitWithAuthenticatedUnknownAccount(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/me", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"id":52,"balance":2.5,"current_concurrency":8}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	oldLimit := 3
	session := &connector.AuthSession{
		AccessToken: "token",
		AccountConcurrency: &connector.AccountConcurrencyFact{
			Provider: connector.TypeSub2API, AccountID: "old-account", Limit: &oldLimit,
			Scope: connector.AccountConcurrencyScopeAccount, Source: sub2APIConcurrencySourceAuthMe,
			ObservedAt: time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC),
		},
	}
	if err := New().CheckAuth(context.Background(), &connector.Channel{SiteURL: server.URL, APIOriginKnown: true}, session); err != nil {
		t.Fatalf("CheckAuth: %v", err)
	}
	if session.AccountConcurrency == nil || session.AccountConcurrency.AccountID != "52" || session.AccountConcurrency.Limit != nil {
		t.Fatalf("authenticated unknown account fact = %#v", session.AccountConcurrency)
	}
}

func TestKnownAPIOriginSettingsFailureDoesNotBlockLogin(t *testing.T) {
	var settingsRequests atomic.Int32
	var loginRequests atomic.Int32
	var homepageRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/settings/public" {
			settingsRequests.Add(1)
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/api/v1/auth/login" {
			loginRequests.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"access_token":"token","expires_in":3600}}`))
			return
		}
		homepageRequests.Add(1)
		_, _ = w.Write([]byte("homepage"))
	}))
	defer server.Close()

	client := New()
	channel := &connector.Channel{
		SiteURL:        server.URL,
		HomepageURL:    server.URL + "/",
		APIOriginKnown: true,
		Username:       "user",
		Password:       "secret",
	}
	_, turnstileErr := client.GetTurnstileSiteKey(context.Background(), channel)
	if turnstileErr == nil {
		t.Fatal("GetTurnstileSiteKey succeeded, want temporary settings error")
	}
	if delay, ok := connector.RetryDelay(turnstileErr); !ok || delay != transientDiscoveryRetry {
		t.Fatalf("turnstile retry delay = %s/%v, want %s", delay, ok, transientDiscoveryRetry)
	}
	session, loginErr := client.Login(context.Background(), channel)
	if loginErr != nil || session.AccessToken != "token" {
		t.Fatalf("Login after settings failure: session=%#v err=%v", session, loginErr)
	}
	if got := settingsRequests.Load(); got != 1 {
		t.Fatalf("settings requests = %d, want 1", got)
	}
	if got := loginRequests.Load(); got != 1 {
		t.Fatalf("login requests = %d, want 1", got)
	}
	if got := homepageRequests.Load(); got != 0 {
		t.Fatalf("homepage requests = %d, want 0", got)
	}
}

func TestUnknownAPIOriginTransientSettingsFailureUsesShortRetryWithoutHomepageScan(t *testing.T) {
	var settingsRequests atomic.Int32
	var homepageRequests atomic.Int32
	var loginRequests atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		settingsRequests.Add(1)
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		loginRequests.Add(1)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		homepageRequests.Add(1)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := New().Login(context.Background(), &connector.Channel{
		SiteURL:     server.URL,
		HomepageURL: server.URL + "/",
		Username:    "user",
		Password:    "secret",
	})
	if err == nil {
		t.Fatal("Login succeeded, want transient settings failure")
	}
	if delay, ok := connector.RetryDelay(err); !ok || delay != transientDiscoveryRetry {
		t.Fatalf("retry delay = %s/%v, want %s", delay, ok, transientDiscoveryRetry)
	}
	if settingsRequests.Load() != 1 || homepageRequests.Load() != 0 || loginRequests.Load() != 0 {
		t.Fatalf("requests: settings=%d homepage=%d login=%d", settingsRequests.Load(), homepageRequests.Load(), loginRequests.Load())
	}
}

func TestKnownAPIOriginCredentialFailureDoesNotUseDiscoveryRetry(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("known API origin must not preflight public settings")
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := New().Login(context.Background(), &connector.Channel{
		SiteURL:        server.URL,
		APIOriginKnown: true,
		Username:       "user",
		Password:       "wrong",
	})
	if err == nil {
		t.Fatal("Login succeeded, want credential failure")
	}
	if delay, ok := connector.RetryDelay(err); ok {
		t.Fatalf("credential failure received discovery retry hint %s", delay)
	}
	var httpErr *connector.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Login error = %v, want wrapped HTTP 401", err)
	}
}

type knownAPIAuthOperation struct {
	name string
	run  func(context.Context, *Client, *connector.Channel) error
}

func knownAPIAuthOperations() []knownAPIAuthOperation {
	return []knownAPIAuthOperation{
		{
			name: "login",
			run: func(ctx context.Context, client *Client, channel *connector.Channel) error {
				_, err := client.Login(ctx, channel)
				return err
			},
		},
		{
			name: "refresh",
			run: func(ctx context.Context, client *Client, channel *connector.Channel) error {
				_, err := client.RefreshSession(ctx, channel, &connector.AuthSession{RefreshToken: "refresh-token"})
				return err
			},
		},
		{
			name: "check_auth",
			run: func(ctx context.Context, client *Client, channel *connector.Channel) error {
				return client.CheckAuth(ctx, channel, &connector.AuthSession{AccessToken: "access-token"})
			},
		},
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func TestKnownAPIOriginTransientAuthFailuresUseShortRetry(t *testing.T) {
	for _, operation := range knownAPIAuthOperations() {
		operation := operation
		t.Run(operation.name, func(t *testing.T) {
			for _, status := range []int{
				http.StatusRequestTimeout,
				http.StatusTooEarly,
				http.StatusTooManyRequests,
				http.StatusInternalServerError,
				http.StatusServiceUnavailable,
			} {
				status := status
				t.Run(fmt.Sprintf("http_%d", status), func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						http.Error(w, "temporary upstream failure", status)
					}))
					defer server.Close()

					err := operation.run(context.Background(), New(), &connector.Channel{
						SiteURL:        server.URL,
						APIOriginKnown: true,
						Username:       "user",
						Password:       "secret",
					})
					if err == nil {
						t.Fatalf("operation succeeded, want HTTP %d", status)
					}
					if delay, ok := connector.RetryDelay(err); !ok || delay != transientDiscoveryRetry {
						t.Fatalf("retry delay = %s/%v, want %s; error=%v", delay, ok, transientDiscoveryRetry, err)
					}
				})
			}

			t.Run("network_timeout", func(t *testing.T) {
				client := New()
				client.http.GetClient().Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, discoveryClassificationNetError{timeout: true}
				})
				err := operation.run(context.Background(), client, &connector.Channel{
					SiteURL:        "https://api.example.invalid",
					APIOriginKnown: true,
					Username:       "user",
					Password:       "secret",
				})
				if err == nil {
					t.Fatal("operation succeeded, want network timeout")
				}
				if delay, ok := connector.RetryDelay(err); !ok || delay != transientDiscoveryRetry {
					t.Fatalf("retry delay = %s/%v, want %s; error=%v", delay, ok, transientDiscoveryRetry, err)
				}
			})
		})
	}
}

func TestKnownAPIOriginPermanentAuthFailuresDoNotUseShortRetry(t *testing.T) {
	failures := []struct {
		name   string
		status int
		body   string
	}{
		{name: "http_401", status: http.StatusUnauthorized, body: "invalid credentials"},
		{name: "http_403", status: http.StatusForbidden, body: "account forbidden"},
		{name: "business_error", status: http.StatusOK, body: `{"code":1001,"message":"account disabled","data":null}`},
	}
	for _, operation := range knownAPIAuthOperations() {
		operation := operation
		t.Run(operation.name, func(t *testing.T) {
			for _, failure := range failures {
				failure := failure
				t.Run(failure.name, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(failure.status)
						_, _ = w.Write([]byte(failure.body))
					}))
					defer server.Close()

					err := operation.run(context.Background(), New(), &connector.Channel{
						SiteURL:        server.URL,
						APIOriginKnown: true,
						Username:       "user",
						Password:       "secret",
					})
					if err == nil {
						t.Fatal("operation succeeded, want permanent authentication failure")
					}
					if delay, ok := connector.RetryDelay(err); ok {
						t.Fatalf("permanent failure received retry delay %s: %v", delay, err)
					}
				})
			}
		})
	}
}

func TestKnownAPIOriginAllowsHomepageOnDifferentSite(t *testing.T) {
	var loginRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		loginRequests.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"access_token":"token","expires_in":3600}}`))
	}))
	defer server.Close()

	session, err := New().Login(context.Background(), &connector.Channel{
		SiteURL:        server.URL,
		HomepageURL:    "https://unrelated.example",
		APIOriginKnown: true,
		Username:       "user",
		Password:       "secret",
	})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session == nil || session.AccessToken != "token" {
		t.Fatalf("session = %#v, want token from persisted API origin", session)
	}
	if got := loginRequests.Load(); got != 1 {
		t.Fatalf("credential requests = %d, want 1", got)
	}
}

func TestCallerCancellationDoesNotCacheAPIOriginFailure(t *testing.T) {
	client := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	channel := &connector.Channel{
		SiteURL:        "https://example.com",
		HomepageURL:    "https://example.com/",
		APIOriginKnown: true,
	}
	if err := client.ensureVerifiedAPIOrigin(ctx, channel); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("ensureVerifiedAPIOrigin error = %v, want canceled context", err)
	}
	if cached := client.cachedAPIOriginFailure(channel.SiteURL, time.Now()); cached != nil {
		t.Fatalf("caller cancellation was cached: %v", cached)
	}
}

func TestCallerCancellationTakesPrecedenceOverAPIOriginCaches(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		prime func(*Client, string)
	}{
		{
			name: "negative cache",
			prime: func(client *Client, base string) {
				client.cacheAPIOriginFailure(base, errors.New("cached failure"), time.Now())
			},
		},
		{
			name: "verified cache",
			prime: func(client *Client, base string) {
				client.markVerifiedAPIOrigin(base, json.RawMessage(`{"turnstile_enabled":false}`))
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client := New()
			base := "https://example.com"
			testCase.prime(client, base)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := client.ensureVerifiedAPIOrigin(ctx, &connector.Channel{SiteURL: base, HomepageURL: base})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("ensureVerifiedAPIOrigin error = %v, want canceled context", err)
			}
		})
	}
}

func TestAPIOriginFailureCacheExpires(t *testing.T) {
	client := New()
	base := "https://example.com"
	client.cacheAPIOriginFailure(base, errors.New("temporary"), time.Now().Add(-apiOriginFailureCacheTTL))
	if cached := client.cachedAPIOriginFailure(base, time.Now()); cached != nil {
		t.Fatalf("expired API origin failure = %v, want nil", cached)
	}
}

func TestSameDiscoveryOriginIgnoresPathCaseAndDefaultPort(t *testing.T) {
	if !sameDiscoveryOrigin("https://EXAMPLE.com/api", "https://example.COM:443/") {
		t.Fatal("equivalent HTTPS origins were treated as different")
	}
	if sameDiscoveryOrigin("https://example.com", "https://example.com:444") {
		t.Fatal("different ports were treated as the same origin")
	}
}

func TestAPIOriginValidationReportsTLSStage(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	}))
	defer server.Close()

	_, err := New().GetTurnstileSiteKey(context.Background(), &connector.Channel{
		SiteURL:        server.URL,
		HomepageURL:    server.URL,
		APIOriginKnown: true,
	})
	if err == nil || !strings.Contains(err.Error(), "TLS 握手或证书验证") {
		t.Fatalf("GetTurnstileSiteKey error = %v, want TLS stage", err)
	}
}

func TestDiscoveryReportsCandidateResponseHeaderStage(t *testing.T) {
	candidate := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer candidate.Close()
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `<script>const apiOrigin=%q;</script>`, candidate.URL)
	}))
	defer page.Close()

	client := New()
	client.discoveryTimeout = 100 * time.Millisecond
	_, err := client.DiscoverAPIOrigin(context.Background(), page.URL)
	if err == nil || !strings.Contains(err.Error(), candidate.URL) || !strings.Contains(err.Error(), "等待 HTTP 响应头") {
		t.Fatalf("DiscoverAPIOrigin error = %v, want candidate response-header stage", err)
	}
}

func TestDiscoveryPreservesDeadlineAfterEarlierCandidateFailure(t *testing.T) {
	candidate := httptest.NewServer(http.NotFoundHandler())
	defer candidate.Close()
	pageMux := http.NewServeMux()
	pageMux.HandleFunc("/slow.js", func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	pageMux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `<script>const apiOrigin=%q;</script><script src="/slow.js"></script>`, candidate.URL)
	})
	page := httptest.NewServer(pageMux)
	defer page.Close()

	client := New()
	client.discoveryTimeout = 100 * time.Millisecond
	_, err := client.DiscoverAPIOrigin(context.Background(), page.URL)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), candidate.URL) {
		t.Fatalf("DiscoverAPIOrigin error = %v, want deadline plus earlier candidate failure", err)
	}
}

func TestAPIOriginValidationReportsResponseBodyStage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	client := New()
	client.discoveryTimeout = 100 * time.Millisecond
	_, err := client.GetTurnstileSiteKey(context.Background(), &connector.Channel{
		SiteURL:        server.URL,
		HomepageURL:    server.URL,
		APIOriginKnown: true,
	})
	if err == nil || !strings.Contains(err.Error(), "读取 HTTP 响应正文") {
		t.Fatalf("GetTurnstileSiteKey error = %v, want response-body stage", err)
	}
}

func TestCredentialRedirectDoesNotForwardBodyCrossOrigin(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	targetURL := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetURL, http.StatusTemporaryRedirect)
	})
	origin := httptest.NewServer(mux)
	defer origin.Close()

	_, err := New().Login(context.Background(), &connector.Channel{SiteURL: origin.URL, Username: "user", Password: "secret"})
	if err == nil || !strings.Contains(err.Error(), "跨源重定向") {
		t.Fatalf("Login error = %v, want blocked cross-origin redirect", err)
	}
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("cross-origin redirect target requests = %d, want 0", got)
	}
}

func TestLoginRediscoversAfterCrossOriginRedirectWithoutForwarding(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	targetURL := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)

	var alternateLogins atomic.Int32
	alternateMux := http.NewServeMux()
	alternateMux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	alternateMux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		alternateLogins.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"access_token":"alternate-token","expires_in":3600}}`))
	})
	alternate := httptest.NewServer(alternateMux)
	defer alternate.Close()

	homepage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `<script>const apiOrigin=%q;</script>`, alternate.URL)
	}))
	defer homepage.Close()

	originMux := http.NewServeMux()
	originMux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetURL, http.StatusTemporaryRedirect)
	})
	origin := httptest.NewServer(originMux)
	defer origin.Close()

	channel := &connector.Channel{
		SiteURL:        origin.URL,
		HomepageURL:    homepage.URL,
		APIOriginKnown: true,
		Username:       "user",
		Password:       "secret",
	}
	session, err := New().Login(context.Background(), channel)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session == nil || session.AccessToken != "alternate-token" || channel.SiteURL != alternate.URL {
		t.Fatalf("session=%#v site=%q, want alternate API origin", session, channel.SiteURL)
	}
	if targetRequests.Load() != 0 {
		t.Fatalf("cross-origin redirect target requests = %d, want 0", targetRequests.Load())
	}
	if alternateLogins.Load() != 1 {
		t.Fatalf("alternate login requests = %d, want 1", alternateLogins.Load())
	}
}

type discoveryClassificationNetError struct {
	timeout   bool
	temporary bool
}

func (err discoveryClassificationNetError) Error() string   { return "network failure" }
func (err discoveryClassificationNetError) Timeout() bool   { return err.timeout }
func (err discoveryClassificationNetError) Temporary() bool { return err.temporary }

func TestDiscoveryFailureClassificationDoesNotShortRetryPermanentErrors(t *testing.T) {
	stage := func(err error) error {
		return &discoveryStageError{Stage: discoveryStageConnect, Err: err}
	}
	for name, err := range map[string]error{
		"certificate or connection refusal": errors.New("x509: certificate is not valid"),
		"non-temporary network error":       discoveryClassificationNetError{},
		"canceled":                          context.Canceled,
	} {
		t.Run(name, func(t *testing.T) {
			classified := stage(err)
			if isTransientDiscoveryFailure(classified) {
				t.Fatalf("error %v was classified as transient", err)
			}
			if delay, ok := connector.RetryDelay(withTransientDiscoveryRetry(context.Background(), classified)); ok {
				t.Fatalf("error %v received short retry delay %s", err, delay)
			}
		})
	}
	for name, err := range map[string]error{
		"deadline":          context.DeadlineExceeded,
		"network timeout":   discoveryClassificationNetError{timeout: true},
		"temporary network": discoveryClassificationNetError{temporary: true},
	} {
		t.Run(name, func(t *testing.T) {
			classified := stage(err)
			if !isTransientDiscoveryFailure(classified) {
				t.Fatalf("error %v was not classified as transient", err)
			}
			if delay, ok := connector.RetryDelay(withTransientDiscoveryRetry(context.Background(), classified)); !ok || delay != transientDiscoveryRetry {
				t.Fatalf("error %v retry delay = %s/%v, want %s", err, delay, ok, transientDiscoveryRetry)
			}
		})
	}
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway} {
		err := connector.HTTPStatusError(status, nil)
		if !isTransientDiscoveryFailure(err) {
			t.Fatalf("HTTP %d was not classified as transient", status)
		}
		if delay, ok := connector.RetryDelay(withTransientDiscoveryRetry(context.Background(), err)); !ok || delay != transientDiscoveryRetry {
			t.Fatalf("HTTP %d retry delay = %s/%v, want %s", status, delay, ok, transientDiscoveryRetry)
		}
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict} {
		err := connector.HTTPStatusError(status, nil)
		if isTransientDiscoveryFailure(err) {
			t.Fatalf("HTTP %d was classified as transient", status)
		}
		if delay, ok := connector.RetryDelay(withTransientDiscoveryRetry(context.Background(), err)); ok {
			t.Fatalf("HTTP %d received short retry delay %s", status, delay)
		}
	}
}

func TestDiscoveryBlocksCrossSiteRedirectBeforeTargetRequest(t *testing.T) {
	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{}}`))
	}))
	defer target.Close()
	targetURL := strings.Replace(target.URL, "127.0.0.1", "localhost", 1)

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetURL, http.StatusFound)
	}))
	defer source.Close()

	_, err := New().DiscoverAPIOrigin(context.Background(), source.URL)
	if err == nil {
		t.Fatal("DiscoverAPIOrigin succeeded, want redirect rejection")
	}
	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("cross-site redirect target requests = %d, want 0", got)
	}
}

func TestDiscoveryHasTotalDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	client := New()
	client.discoveryTimeout = 50 * time.Millisecond
	started := time.Now()
	_, err := client.DiscoverAPIOrigin(context.Background(), server.URL)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DiscoverAPIOrigin error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("discovery elapsed = %s, want bounded total duration", elapsed)
	}
}

func TestDiscoveryValidatesInlineCandidateBeforeSlowScript(t *testing.T) {
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	apiServer := httptest.NewServer(apiMux)
	defer apiServer.Close()

	var slowScriptRequests atomic.Int32
	pageMux := http.NewServeMux()
	pageMux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `<script>const apiOrigin=%q;</script><script src="/slow.js"></script>`, apiServer.URL)
	})
	pageMux.HandleFunc("/slow.js", func(w http.ResponseWriter, _ *http.Request) {
		slowScriptRequests.Add(1)
		time.Sleep(250 * time.Millisecond)
	})
	pageServer := httptest.NewServer(pageMux)
	defer pageServer.Close()

	discovered, err := New().DiscoverAPIOrigin(context.Background(), pageServer.URL)
	if err != nil {
		t.Fatalf("DiscoverAPIOrigin: %v", err)
	}
	if discovered != apiServer.URL {
		t.Fatalf("discovered = %q, want %q", discovered, apiServer.URL)
	}
	if got := slowScriptRequests.Load(); got != 0 {
		t.Fatalf("slow script requests = %d, want 0", got)
	}
}

func TestDiscoveryRejectsCrossSiteBundleCandidate(t *testing.T) {
	pageServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`<script>const apiOrigin="https://api.unrelated.example"</script>`))
	}))
	defer pageServer.Close()

	_, err := New().DiscoverAPIOrigin(context.Background(), pageServer.URL)
	if err == nil || !strings.Contains(err.Error(), "未能") {
		t.Fatalf("DiscoverAPIOrigin error = %v", err)
	}
}

func TestGetTurnstileSiteKeyDoesNotCacheMalformedSettings(t *testing.T) {
	var loginRequests atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":null}`))
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		loginRequests.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"access_token":"unexpected"}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := New()
	channel := &connector.Channel{SiteURL: server.URL, Username: "u", Password: "p"}
	if _, err := client.GetTurnstileSiteKey(context.Background(), channel); err == nil {
		t.Fatal("GetTurnstileSiteKey succeeded for null settings data")
	}
	if _, err := client.Login(context.Background(), channel); err == nil {
		t.Fatal("Login succeeded after malformed settings cache attempt")
	}
	if got := loginRequests.Load(); got != 0 {
		t.Fatalf("login requests after malformed settings = %d, want 0", got)
	}
}

func TestLoginDoesNotTrustSettingsWithoutTurnstileMarker(t *testing.T) {
	var loginRequests atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{}}`))
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		loginRequests.Add(1)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := New().Login(context.Background(), &connector.Channel{SiteURL: server.URL, Username: "u", Password: "p"})
	if err == nil {
		t.Fatal("Login succeeded without settings marker")
	}
	if got := loginRequests.Load(); got != 0 {
		t.Fatalf("login requests after unrecognized settings = %d, want 0", got)
	}
}

func TestLoginDoesNotTrustNullTurnstileMarker(t *testing.T) {
	var loginRequests atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":null}}`))
	})
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		loginRequests.Add(1)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	_, err := New().Login(context.Background(), &connector.Channel{SiteURL: server.URL, Username: "u", Password: "p"})
	if err == nil {
		t.Fatal("Login succeeded with null settings marker")
	}
	if got := loginRequests.Load(); got != 0 {
		t.Fatalf("login requests after null settings marker = %d, want 0", got)
	}
}

func TestDiscoveryRejectsHTTPSDowngradeCandidate(t *testing.T) {
	source, err := url.Parse("https://www.example.com")
	if err != nil {
		t.Fatal(err)
	}
	target, err := url.Parse("http://api.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if safeDiscoveryRedirect(source, target) {
		t.Fatal("HTTPS to HTTP discovery candidate was accepted")
	}
}

func TestRefreshSession(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/settings/public", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"turnstile_enabled":false}}`))
	})
	mux.HandleFunc("/api/v1/auth/refresh", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["refresh_token"] != "old-refresh" {
			t.Fatalf("refresh_token = %q", body["refresh_token"])
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"access_token":"new-token","refresh_token":"new-refresh","expires_in":3600}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session, err := c.RefreshSession(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{RefreshToken: "old-refresh"})
	if err != nil {
		t.Fatalf("RefreshSession: %v", err)
	}
	if session.AccessToken != "new-token" {
		t.Fatalf("access token = %q, want new-token", session.AccessToken)
	}
	if session.RefreshToken != "new-refresh" {
		t.Fatalf("refresh token = %q, want new-refresh", session.RefreshToken)
	}
	if time.Until(session.ExpiresAt) <= 0 {
		t.Fatalf("expires at = %s, want future", session.ExpiresAt)
	}
}

func TestGetCostsUsesAsiaShanghaiTodayAcrossUTCDateBoundary(t *testing.T) {
	fixedNow := time.Date(2026, 7, 17, 16, 30, 0, 0, time.UTC)
	shanghai := time.FixedZone("Asia/Shanghai", 8*60*60)
	costByDate := map[string]float64{
		"2026-07-17": 1.23,
		"2026-07-18": 2.34,
	}
	if fixedNow.Format(time.DateOnly) == fixedNow.In(shanghai).Format(time.DateOnly) {
		t.Fatal("test instant must cross the UTC and Asia/Shanghai date boundary")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"","data":{"today_actual_cost":%v,"total_actual_cost":45.67}}`, costByDate[fixedNow.Format(time.DateOnly)])
	})
	mux.HandleFunc("/api/v1/usage/stats", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("timezone"); got != "Asia/Shanghai" {
			t.Fatalf("timezone = %q, want Asia/Shanghai", got)
		}
		if got := r.URL.Query().Get("start_date"); got != "2026-07-18" {
			t.Fatalf("start_date = %q, want 2026-07-18", got)
		}
		if got := r.URL.Query().Get("end_date"); got != "2026-07-18" {
			t.Fatalf("end_date = %q, want 2026-07-18", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q", got)
		}
		localDate := fixedNow.In(shanghai).Format(time.DateOnly)
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"","data":{"total_actual_cost":%v}}`, costByDate[localDate])
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	c.now = func() time.Time { return fixedNow }
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL:  srv.URL,
		TimeZone: "Asia/Shanghai",
	}, &connector.AuthSession{
		AccessToken: "token",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 2.34 {
		t.Fatalf("today cost = %v, want 2.34", res.TodayCost)
	}
	if res.TotalCost != 45.67 {
		t.Fatalf("total cost = %v, want 45.67", res.TotalCost)
	}
	if res.RawTodayCost != res.TodayCost || res.RawTotalCost != res.TotalCost || res.AppliedFactor != 1 || res.FactorSource != "default" || !res.SampledAt.Equal(fixedNow) {
		t.Fatalf("default cost provenance = %+v", res)
	}
}

func TestGetCostsFallsBackToDashboardTodayForLegacyUpstream(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone, http.StatusNotImplemented} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/usage/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"today_actual_cost":1.23,"total_actual_cost":45.67}}`))
			})
			mux.HandleFunc("/api/v1/usage/stats", func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unsupported", status)
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			res, err := New().GetCosts(context.Background(), &connector.Channel{
				SiteURL:  srv.URL,
				TimeZone: "Asia/Shanghai",
			}, &connector.AuthSession{AccessToken: "token"})
			if err != nil {
				t.Fatalf("GetCosts: %v", err)
			}
			if res.RawTodayCost != 1.23 || res.RawTotalCost != 45.67 {
				t.Fatalf("legacy fallback costs = %+v", res)
			}
		})
	}
}

func TestGetCostsUsesDailyStatsWhenDashboardIsUnavailable(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone, http.StatusNotImplemented} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			fixedNow := time.Date(2026, 8, 9, 15, 59, 59, 0, time.UTC)
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/usage/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "unsupported", status)
			})
			mux.HandleFunc("/api/v1/usage/stats", func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("timezone"); got != "Asia/Shanghai" {
					t.Fatalf("timezone = %q, want Asia/Shanghai", got)
				}
				if got := r.URL.Query().Get("start_date"); got != "2026-08-09" {
					t.Fatalf("start_date = %q, want 2026-08-09", got)
				}
				if got := r.URL.Query().Get("end_date"); got != "2026-08-09" {
					t.Fatalf("end_date = %q, want 2026-08-09", got)
				}
				_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"total_actual_cost":2.34}}`))
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			client := New()
			client.now = func() time.Time { return fixedNow }
			res, err := client.GetCosts(context.Background(), &connector.Channel{
				SiteURL:  srv.URL,
				TimeZone: "Asia/Shanghai",
			}, &connector.AuthSession{AccessToken: "token"})
			if err != nil {
				t.Fatalf("GetCosts: %v", err)
			}
			if res.RawTodayCost != 2.34 || res.TodayCost != 2.34 || res.RawTotalCost != 2.34 || res.TotalCost != 2.34 || !res.CumulativeCostUnavailable || !res.SampledAt.Equal(fixedNow) {
				t.Fatalf("daily-only costs = %+v", res)
			}
		})
	}
}

func TestGetCostsFailsWhenBothStatsEndpointsAreUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	mux.HandleFunc("/api/v1/usage/stats", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().GetCosts(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if err == nil || !strings.Contains(err.Error(), "sub2api usage stats") {
		t.Fatalf("GetCosts error = %v, want usage stats failure", err)
	}
}

func TestGetHistoricalCostsUsesCompleteCalendarDayRange(t *testing.T) {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage/stats", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("start_date"); got != "2026-08-09" {
			t.Fatalf("start_date = %q", got)
		}
		if got := r.URL.Query().Get("end_date"); got != "2026-08-10" {
			t.Fatalf("end_date = %q", got)
		}
		if got := r.URL.Query().Get("timezone"); got != "Asia/Shanghai" {
			t.Fatalf("timezone = %q", got)
		}
		if r.URL.Query().Has("period") {
			t.Fatalf("unexpected period: %s", r.URL.RawQuery)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"total_actual_cost":7.25}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().GetHistoricalCosts(
		context.Background(),
		&connector.Channel{SiteURL: srv.URL, TimeZone: "Asia/Shanghai"},
		&connector.AuthSession{AccessToken: "token"},
		time.Date(2026, 8, 9, 0, 0, 0, 0, shanghai),
		time.Date(2026, 8, 11, 0, 0, 0, 0, shanghai),
	)
	if err != nil {
		t.Fatalf("GetHistoricalCosts: %v", err)
	}
	if result == nil || result.RawCost != 7.25 {
		t.Fatalf("historical costs = %+v", result)
	}
}

func TestGetHistoricalCostsRejectsNonCalendarBoundary(t *testing.T) {
	_, err := New().GetHistoricalCosts(
		context.Background(),
		&connector.Channel{TimeZone: "UTC"},
		&connector.AuthSession{},
		time.Date(2026, 8, 9, 1, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
	)
	if err == nil || !strings.Contains(err.Error(), "calendar-day boundaries") {
		t.Fatalf("non-boundary error = %v", err)
	}
}

func TestGetHistoricalCostsRejectsInvalidAggregate(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage/stats", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"total_actual_cost":-1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().GetHistoricalCosts(
		context.Background(),
		&connector.Channel{SiteURL: srv.URL, TimeZone: "UTC"},
		&connector.AuthSession{},
		time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
	)
	if err == nil || !strings.Contains(err.Error(), "invalid total_actual_cost") {
		t.Fatalf("invalid aggregate error = %v", err)
	}
}

func TestLookupRequestCostUsesActualCost(t *testing.T) {
	now := time.Date(2026, 7, 10, 0, 0, 5, 0, time.FixedZone("Asia/Shanghai", 8*60*60))
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("api_key_id"); got != "9" {
			t.Fatalf("api_key_id = %q", got)
		}
		if got := r.URL.Query().Get("page_size"); got != "1000" {
			t.Fatalf("page_size = %q, want 1000", got)
		}
		if got := r.URL.Query().Get("sort_by"); got != "created_at" {
			t.Fatalf("sort_by = %q, want created_at", got)
		}
		if got := r.URL.Query().Get("sort_order"); got != "desc" {
			t.Fatalf("sort_order = %q, want desc", got)
		}
		if got := r.URL.Query().Get("timezone"); got != "UTC" {
			t.Fatalf("timezone = %q, want UTC", got)
		}
		if got := r.URL.Query().Get("request_id"); got != "client:req-1" {
			t.Fatalf("request_id = %q, want client:req-1", got)
		}
		if got := r.URL.Query().Get("start_date"); got != "2026-07-09" {
			t.Fatalf("start_date = %q, want UTC request date 2026-07-09", got)
		}
		if got := r.URL.Query().Get("end_date"); got != "2026-07-09" {
			t.Fatalf("end_date = %q, want UTC request date 2026-07-09", got)
		}
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"request_id":"client:req-1","model":"gpt-test","input_tokens":20,"output_tokens":30,"cache_read_tokens":100,"cache_creation_tokens":5,"actual_cost":0.0123,"created_at":%q}],"total":1,"page":1,"page_size":1000,"pages":1}}`, now.Format(time.RFC3339Nano))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	result, err := c.LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID:         9,
		ModelName:        "gpt-test",
		PromptTokens:     125,
		CompletionTokens: 30,
		RequestID:        "req-1",
		StartedAt:        now.Add(-time.Second),
		FinishedAt:       now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("LookupRequestCost: %v", err)
	}
	if result.Cost != 0.0123 || result.Source != "sub2api_actual_cost" || result.UsageID != "7" || result.CacheCreationTokens != 5 || result.CacheReadTokens != 100 || !result.HasCacheTokenMetrics {
		t.Fatalf("result = %+v", result)
	}
	if result.PromptTokens != 125 || result.CompletionTokens != 30 || result.TotalTokens != 155 || !result.HasTokenMetrics {
		t.Fatalf("token metrics = %+v, want prompt=125 completion=30 total=155 present=true", result)
	}
}

func TestLookupRequestCostExactRequestIDDoesNotRequireLocalModelOrTokens(t *testing.T) {
	tests := []struct {
		name                     string
		queryModel               string
		upstreamModel            string
		promptTokens             int64
		completionTokens         int64
		upstreamPromptTokens     int64
		upstreamCompletionTokens int64
	}{
		{name: "tokens match", queryModel: "gpt-test", upstreamModel: "gpt-test", promptTokens: 125, completionTokens: 30, upstreamPromptTokens: 125, upstreamCompletionTokens: 30},
		{name: "accepted cancel has no local tokens", queryModel: "gpt-test", upstreamModel: "gpt-test", promptTokens: 0, completionTokens: 0, upstreamPromptTokens: 125, upstreamCompletionTokens: 30},
		{name: "local tokens differ", queryModel: "gpt-test", upstreamModel: "gpt-test", promptTokens: 999, completionTokens: 888, upstreamPromptTokens: 125, upstreamCompletionTokens: 30},
		{name: "upstream tokens missing", queryModel: "gpt-test", upstreamModel: "gpt-test", promptTokens: 125, completionTokens: 30},
		{name: "model differs", queryModel: "gpt-test", upstreamModel: "gpt-other", promptTokens: 125, completionTokens: 30, upstreamPromptTokens: 125, upstreamCompletionTokens: 30},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().Truncate(time.Microsecond)
			queryModel := test.queryModel
			if queryModel == "" {
				queryModel = "gpt-test"
			}
			upstreamModel := test.upstreamModel
			if upstreamModel == "" {
				upstreamModel = "gpt-test"
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"request_id":"client:req-exact","model":%q,"input_tokens":%d,"output_tokens":%d,"actual_cost":0.0123,"created_at":%q},{"id":6,"api_key_id":0,"request_id":"client:req-exact","model":%q,"input_tokens":%d,"output_tokens":%d,"actual_cost":0.01,"created_at":%q}],"total":2,"page":1,"page_size":1000,"pages":1}}`, upstreamModel, test.upstreamPromptTokens, test.upstreamCompletionTokens, now.Format(time.RFC3339Nano), upstreamModel, test.upstreamPromptTokens, test.upstreamCompletionTokens, now.Format(time.RFC3339Nano))
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
				APIKeyID:         9,
				ModelName:        queryModel,
				PromptTokens:     test.promptTokens,
				CompletionTokens: test.completionTokens,
				RequestID:        "req-exact",
				StartedAt:        now.Add(-time.Second),
				FinishedAt:       now.Add(time.Second),
			})
			if err != nil {
				t.Fatalf("LookupRequestCost: %v", err)
			}
			if result.Cost != 0.0123 || result.UsageID != "7" {
				t.Fatalf("result = %+v", result)
			}
			if test.upstreamPromptTokens == 0 && test.upstreamCompletionTokens == 0 {
				if result.PromptTokens != 0 || result.CompletionTokens != 0 || result.TotalTokens != 0 || result.HasTokenMetrics {
					t.Fatalf("token metrics = %+v, want missing upstream metrics", result)
				}
			} else if result.PromptTokens != test.upstreamPromptTokens || result.CompletionTokens != test.upstreamCompletionTokens || result.TotalTokens != test.upstreamPromptTokens+test.upstreamCompletionTokens || !result.HasTokenMetrics {
				t.Fatalf("token metrics = %+v, want upstream metrics", result)
			}
		})
	}
}

func TestLookupRequestCostExactRequestIDUsesNormalizedModelMatch(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"request_id":"req-model-normalized","model":"gpt-test","input_tokens":12,"output_tokens":3,"actual_cost":0.0123,"created_at":%q}],"total":1,"page":1,"page_size":1000,"pages":1}}`, now.Format(time.RFC3339Nano))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: " GPT-TEST ", PromptTokens: 12, CompletionTokens: 3,
		RequestID: "req-model-normalized", StartedAt: now.Add(-time.Second), FinishedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("LookupRequestCost: %v", err)
	}
	if result.ModelName != "gpt-test" || result.PromptTokens != 12 || result.CompletionTokens != 3 || result.Cost != 0.0123 {
		t.Fatalf("result = %+v", result)
	}
}

func TestLookupRequestCostExactRequestIDIgnoresExactTimeWindowWithinUTCDateGuard(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("request_id"); got != "client:req-outside-window" || r.URL.Query().Get("start_date") != "2026-08-30" || r.URL.Query().Get("end_date") != "2026-08-30" {
			t.Fatalf("exact-ID query = %q", r.URL.RawQuery)
		}
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"request_id":"client:req-outside-window","model":"upstream-alias","input_tokens":12,"output_tokens":3,"actual_cost":0.0123,"created_at":%q}],"total":1,"page":1,"page_size":1000,"pages":1}}`, now.Add(time.Hour).Format(time.RFC3339Nano))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-outside-window", StartedAt: now, FinishedAt: now,
	})
	if err != nil || result == nil || result.Cost != 0.0123 || result.UsageID != "7" {
		t.Fatalf("result=%+v err=%v, want authoritative exact-ID match", result, err)
	}
}

func TestLookupRequestCostExactRequestIDUsesUTCDateGuardWhenTimingAvailable(t *testing.T) {
	for _, test := range []struct {
		name       string
		startedAt  time.Time
		finishedAt time.Time
		wantStart  string
		wantEnd    string
	}{
		{
			name:       "same day",
			startedAt:  time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
			finishedAt: time.Date(2026, 8, 30, 12, 0, 1, 0, time.UTC),
			wantStart:  "2026-08-30",
			wantEnd:    "2026-08-30",
		},
		{
			name:       "lower guard crosses midnight",
			startedAt:  time.Date(2026, 8, 30, 0, 0, 5, 0, time.UTC),
			finishedAt: time.Date(2026, 8, 30, 0, 0, 6, 0, time.UTC),
			wantStart:  "2026-08-29",
			wantEnd:    "2026-08-30",
		},
		{
			name:       "upper guard crosses midnight",
			startedAt:  time.Date(2026, 8, 30, 23, 59, 54, 0, time.UTC),
			finishedAt: time.Date(2026, 8, 30, 23, 59, 55, 0, time.UTC),
			wantStart:  "2026-08-30",
			wantEnd:    "2026-08-31",
		},
		{name: "missing timing"},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
				requests++
				query := r.URL.Query()
				if query.Get("request_id") != "client:req-date-guard" || query.Get("start_date") != test.wantStart || query.Get("end_date") != test.wantEnd {
					t.Fatalf("exact-ID query = %q", r.URL.RawQuery)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 0, "message": "success",
					"data": map[string]any{"items": []map[string]any{}, "total": 0, "page": 1, "page_size": 1000, "pages": 1},
				})
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: "req-date-guard", StartedAt: test.startedAt, FinishedAt: test.finishedAt,
			})
			if err == nil || !strings.Contains(err.Error(), "usage record not found") || result != nil {
				t.Fatalf("result=%+v err=%v, want no matching usage", result, err)
			}
			if requests != 2 {
				t.Fatalf("requests=%d, want bounded initial and final snapshot requests", requests)
			}
		})
	}
}

func TestLookupRequestCostToleratesIgnoredRequestIDFilterWithinDateRange(t *testing.T) {
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		requests++
		query := r.URL.Query()
		if query.Get("request_id") != "client:req-ignored-filter" || query.Get("start_date") != "2026-08-30" || query.Get("end_date") != "2026-08-30" {
			t.Fatalf("bounded exact-ID query = %q", r.URL.RawQuery)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{
				"items": []map[string]any{
					{"id": 9, "api_key_id": 9, "request_id": "client:unrelated-newer", "model": "gpt-test", "created_at": now.Add(2 * time.Second).Format(time.RFC3339Nano)},
					{"id": 8, "api_key_id": 9, "request_id": "client:req-ignored-filter", "model": "gpt-test", "input_tokens": 12, "output_tokens": 3, "actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano)},
					{"id": 7, "api_key_id": 9, "request_id": "client:unrelated-older", "model": "gpt-test", "created_at": now.Add(-2 * time.Second).Format(time.RFC3339Nano)},
				},
				"total": 3, "page": 1, "page_size": 1000, "pages": 1,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-ignored-filter", StartedAt: now.Add(-time.Second), FinishedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("LookupRequestCost: %v", err)
	}
	if result == nil || result.UsageID != "8" || result.Cost != 0.0123 {
		t.Fatalf("result = %+v, want exact target from response containing unrelated rows", result)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want bounded initial and final snapshot requests", requests)
	}
}

func TestLookupRequestCostExactRequestIDRequiresUniqueTargetKeyRow(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	for _, test := range []struct {
		name      string
		items     []map[string]any
		wantError string
	}{
		{
			name: "duplicate exact ID",
			items: []map[string]any{
				{"id": 8, "api_key_id": 9, "request_id": "client:req-unique", "model": "a", "actual_cost": 0.01, "created_at": now.Format(time.RFC3339Nano)},
				{"id": 7, "api_key_id": 9, "request_id": "client:req-unique", "model": "b", "actual_cost": 0.02, "created_at": now.Format(time.RFC3339Nano)},
			},
			wantError: "multiple usage records matched",
		},
		{
			name: "exact ID on wrong key",
			items: []map[string]any{
				{"id": 7, "api_key_id": 8, "request_id": "client:req-unique", "model": "gpt-test", "actual_cost": 0.02, "created_at": now.Format(time.RFC3339Nano)},
			},
			wantError: "usage record not found",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "success", "data": map[string]any{
					"items": test.items, "total": len(test.items), "page": 1, "page_size": 1000, "pages": 1,
				}})
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: "req-unique", StartedAt: now, FinishedAt: now,
			})
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("err=%v, want %q", err, test.wantError)
			}
		})
	}
}

func TestLookupRequestCostWithoutRequestIDDoesNotRelaxMissingLocalMetrics(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"model":"gpt-test","input_tokens":0,"output_tokens":0,"actual_cost":0.0123,"created_at":%q}],"total":1}}`, now.Format(time.RFC3339Nano))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", StartedAt: now, FinishedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "usage record not found") {
		t.Fatalf("err=%v, want strict ID-less fallback rejection", err)
	}
}

func TestLookupRequestCostWithoutRequestIDRejectsMultipleCandidates(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{"id":8,"api_key_id":9,"request_id":"candidate-b","model":"gpt-test","input_tokens":20,"output_tokens":30,"cache_read_tokens":100,"cache_creation_tokens":5,"actual_cost":0.0456,"created_at":%q},{"id":7,"api_key_id":9,"request_id":"candidate-a","model":"gpt-test","input_tokens":20,"output_tokens":30,"cache_read_tokens":100,"cache_creation_tokens":5,"actual_cost":0.0123,"created_at":%q}],"total":2}}`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID:         9,
		ModelName:        "gpt-test",
		PromptTokens:     125,
		CompletionTokens: 30,
		StartedAt:        now.Add(-time.Second),
		FinishedAt:       now.Add(time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "multiple usage records matched") {
		t.Fatalf("err = %v, want multiple usage records matched", err)
	}
}

func TestLookupRequestCostWithRequestIDDoesNotFallbackAndRetriesExact(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	pages := 0
	exactAvailable := false
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		pages++
		requestedPage := 1
		if r.URL.Query().Get("page") == "2" {
			requestedPage = 2
		}
		items := make([]map[string]any, 0, 100)
		if requestedPage == 1 {
			for i := 0; i < 100; i++ {
				items = append(items, map[string]any{"id": 100 - i, "api_key_id": 9, "model": "other", "input_tokens": 1, "output_tokens": 1, "actual_cost": 0.1, "created_at": now.Add(time.Second).Format(time.RFC3339Nano)})
			}
		} else {
			requestID := "billing-id"
			if exactAvailable {
				requestID = "client:requested-id"
			}
			items = append(items, map[string]any{"id": 101, "api_key_id": 9, "request_id": requestID, "model": "gpt-test", "input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5, "actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano)})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "message": "success", "data": map[string]any{"items": items, "total": 101, "page": requestedPage, "page_size": 100, "pages": 2}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	lookup := func() (*connector.RequestCostResult, error) {
		return New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
			APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 125, CompletionTokens: 30,
			RequestID: "requested-id", StartedAt: now.Add(-time.Second), FinishedAt: now.Add(time.Second),
		})
	}
	result, err := lookup()
	if err == nil || !strings.Contains(err.Error(), "usage record not found") || result != nil {
		t.Fatalf("first lookup result=%+v err=%v, want exact-only not found", result, err)
	}
	if pages != 3 {
		t.Fatalf("pages after first lookup = %d, want 3", pages)
	}

	exactAvailable = true
	result, err = lookup()
	if err != nil {
		t.Fatalf("LookupRequestCost after exact usage appeared: %v", err)
	}
	if pages != 6 || result.UsageID != "101" || result.Cost != 0.0123 {
		t.Fatalf("pages=%d result=%+v", pages, result)
	}
}

func TestLookupRequestCostExactRequestIDRejectsIncompletePaginationMetadata(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"request_id":"client:req-incomplete","actual_cost":0.01,"created_at":%q}]}}`, now.Format(time.RFC3339Nano))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-incomplete",
	})
	if err == nil || !strings.Contains(err.Error(), "incomplete pagination metadata") || result != nil {
		t.Fatalf("result=%+v err=%v, want incomplete pagination rejection", result, err)
	}
}

func TestLookupRequestCostRejectsIdenticalDuplicateUsageIDOnSamePage(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	item := map[string]any{
		"id": 7, "api_key_id": 9, "request_id": "client:req-duplicate-same-page",
		"model": "gpt-test", "input_tokens": 10, "output_tokens": 2,
		"actual_cost": 0.01, "created_at": now.Format(time.RFC3339Nano),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success", "data": map[string]any{
				"items": []map[string]any{item, item}, "total": 2, "page": 1, "page_size": 1000, "pages": 1,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-duplicate-same-page",
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate usage id") || result != nil {
		t.Fatalf("requests=%d result=%+v err=%v, want same-page duplicate rejection", requests, result, err)
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want failure during initial page scan", requests)
	}
}

func TestLookupRequestCostRejectsIdenticalDuplicateUsageIDAcrossPages(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	item := map[string]any{
		"id": 7, "api_key_id": 9, "request_id": "client:req-duplicate-pages",
		"model": "gpt-test", "input_tokens": 10, "output_tokens": 2,
		"actual_cost": 0.01, "created_at": now.Format(time.RFC3339Nano),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		requests++
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success", "data": map[string]any{
				"items": []map[string]any{item}, "total": 2, "page": page, "page_size": 1, "pages": 2,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-duplicate-pages",
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate usage id") || result != nil {
		t.Fatalf("requests=%d result=%+v err=%v, want cross-page duplicate rejection", requests, result, err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want page one and page two", requests)
	}
}

func TestLookupRequestCostFinalSnapshotRejectsIdenticalDuplicateUsageID(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	item := map[string]any{
		"id": 7, "api_key_id": 9, "request_id": "client:req-duplicate-final",
		"model": "gpt-test", "input_tokens": 10, "output_tokens": 2,
		"actual_cost": 0.01, "created_at": now.Format(time.RFC3339Nano),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		items := []map[string]any{item}
		if requests > 1 {
			items = append(items, item)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success", "data": map[string]any{
				"items": items, "total": len(items), "page": 1, "page_size": 1000, "pages": 1,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-duplicate-final",
	})
	if err == nil || !strings.Contains(err.Error(), "final snapshot contains duplicate usage id") || result != nil {
		t.Fatalf("requests=%d result=%+v err=%v, want final-snapshot duplicate rejection", requests, result, err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want initial and final page-one snapshots", requests)
	}
}

func TestLookupRequestCostCacheTokenPresenceDistinguishesMissingFromZero(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	for _, test := range []struct {
		name        string
		cacheFields map[string]any
		wantPresent bool
	}{
		{name: "both explicit zero", cacheFields: map[string]any{"cache_read_tokens": 0, "cache_creation_tokens": 0}, wantPresent: true},
		{name: "both missing", cacheFields: map[string]any{}, wantPresent: false},
		{name: "one missing", cacheFields: map[string]any{"cache_read_tokens": 0}, wantPresent: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			item := map[string]any{
				"id": 7, "api_key_id": 9, "request_id": "client:req-cache-presence", "model": "gpt-test",
				"input_tokens": 10, "output_tokens": 2, "actual_cost": 0.01, "created_at": now.Format(time.RFC3339Nano),
			}
			for name, value := range test.cacheFields {
				item[name] = value
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 0, "message": "success", "data": map[string]any{
						"items": []map[string]any{item}, "total": 1, "page": 1, "page_size": 1000, "pages": 1,
					},
				})
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: "req-cache-presence",
			})
			if err != nil {
				t.Fatalf("LookupRequestCost: %v", err)
			}
			if result.HasCacheTokenMetrics != test.wantPresent || result.CacheReadTokens != 0 || result.CacheCreationTokens != 0 {
				t.Fatalf("result=%+v, want cache presence %t", result, test.wantPresent)
			}
		})
	}
}

func TestLookupRequestCostExactRequestIDRejectsConcurrentDuplicateGrowth(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		items := []map[string]any{{
			"id": 7, "api_key_id": 9, "request_id": "client:req-growth", "actual_cost": 0.01,
			"created_at": now.Format(time.RFC3339Nano),
		}}
		total := 1
		if requests > 1 {
			items = append([]map[string]any{{
				"id": 8, "api_key_id": 9, "request_id": "client:req-growth", "actual_cost": 0.02,
				"created_at": now.Add(time.Second).Format(time.RFC3339Nano),
			}}, items...)
			total = 2
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success", "data": map[string]any{
				"items": items, "total": total, "page": 1, "page_size": 1000, "pages": 1,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-growth",
	})
	if err == nil || !strings.Contains(err.Error(), "exact pagination snapshot changed") || result != nil {
		t.Fatalf("result=%+v err=%v, want concurrent duplicate rejection", result, err)
	}
}

func TestLookupRequestCostStopsAfterCrossingRequestWindow(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		requests++
		if got := r.URL.Query().Get("page_size"); got != "1000" {
			t.Fatalf("page_size = %q, want 1000", got)
		}
		items := make([]map[string]any, 0, 1000)
		for i := 0; i < 998; i++ {
			items = append(items, map[string]any{
				"id": int64(5000 - i), "api_key_id": 9, "model": "other", "input_tokens": 1,
				"output_tokens": 1, "actual_cost": 0.1, "created_at": now.Add(2 * time.Second).Format(time.RFC3339Nano),
			})
		}
		items = append(items,
			map[string]any{
				"id": 4002, "api_key_id": 9, "request_id": "client:req-window", "model": "gpt-test",
				"input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5,
				"actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano),
			},
			map[string]any{
				"id": 4001, "api_key_id": 9, "model": "other", "input_tokens": 1,
				"output_tokens": 1, "actual_cost": 0.1, "created_at": now.Add(-12 * time.Second).Format(time.RFC3339Nano),
			},
		)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{"items": items, "total": 5000, "page": 1, "page_size": 1000, "pages": 5},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 125, CompletionTokens: 30,
		StartedAt: now, FinishedAt: now,
	})
	if err != nil {
		t.Fatalf("LookupRequestCost: %v", err)
	}
	if requests != 2 || result.UsageID != "4002" || result.Cost != 0.0123 {
		t.Fatalf("requests=%d result=%+v", requests, result)
	}
}

func TestLookupRequestCostDoesNotReturnExactIDBeforeWindowIsComplete(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		requests++
		requestedPage := 1
		if r.URL.Query().Get("page") == "2" {
			requestedPage = 2
		}
		items := make([]map[string]any, 0, 100)
		if requestedPage == 1 {
			items = append(items, map[string]any{
				"id": 200, "api_key_id": 9, "request_id": "client:req-duplicate", "model": "gpt-test",
				"input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5,
				"actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano),
			})
			for id := int64(199); id >= 101; id-- {
				items = append(items, map[string]any{
					"id": id, "api_key_id": 9, "model": "other", "input_tokens": 1,
					"output_tokens": 1, "actual_cost": 0.1, "created_at": now.Format(time.RFC3339Nano),
				})
			}
		} else {
			items = append(items, map[string]any{
				"id": 100, "api_key_id": 9, "request_id": "client:req-duplicate", "model": "gpt-test",
				"input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5,
				"actual_cost": 0.0456, "created_at": now.Add(-time.Second).Format(time.RFC3339Nano),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{"items": items, "total": 101, "page": requestedPage, "page_size": 100, "pages": 2},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 125, CompletionTokens: 30,
		RequestID: "req-duplicate", StartedAt: now.Add(-time.Second), FinishedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "multiple usage records matched") {
		t.Fatalf("err = %v, want multiple exact matches", err)
	}
	if requests != 3 {
		t.Fatalf("requests = %d, want 3", requests)
	}
}

func TestLookupRequestCostRejectsNewerRowAfterCrossingWindowBoundary(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{"items": []map[string]any{
				{
					"id": 3, "api_key_id": 9, "request_id": "client:req-order", "model": "gpt-test",
					"input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5,
					"actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano),
				},
				{"id": 2, "api_key_id": 9, "model": "other", "created_at": now.Add(-12 * time.Second).Format(time.RFC3339Nano)},
				{"id": 1, "api_key_id": 9, "model": "other", "created_at": now.Add(-time.Second).Format(time.RFC3339Nano)},
			}, "total": 3, "page": 1, "page_size": 1000, "pages": 1},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 125, CompletionTokens: 30,
		RequestID: "req-order", StartedAt: now, FinishedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "usage ordering invalid") {
		t.Fatalf("err = %v, want fail-closed ordering error", err)
	}
}

func TestLookupRequestCostRejectsMalformedTimestampAfterWindowBoundary(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{"items": []map[string]any{
				{
					"id": 3, "api_key_id": 9, "request_id": "client:req-time", "model": "gpt-test",
					"input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5,
					"actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano),
				},
				{"id": 2, "api_key_id": 9, "model": "other", "created_at": now.Add(-12 * time.Second).Format(time.RFC3339Nano)},
				{"id": 1, "api_key_id": 9, "model": "other", "created_at": "not-a-time"},
			}, "total": 3, "page": 1, "page_size": 1000, "pages": 1},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 125, CompletionTokens: 30,
		RequestID: "req-time", StartedAt: now, FinishedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "usage timestamp invalid") {
		t.Fatalf("err = %v, want fail-closed timestamp error", err)
	}
}

func TestLookupRequestCostRejectsInvalidMatchingUsageMetrics(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	tests := []struct {
		name             string
		item             map[string]any
		promptTokens     int64
		completionTokens int64
		wantError        string
	}{
		{
			name:         "missing usage id",
			item:         map[string]any{"id": 0, "input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5},
			promptTokens: 125, completionTokens: 30, wantError: "missing id",
		},
		{
			name:         "negative usage id",
			item:         map[string]any{"id": -7, "input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5},
			promptTokens: 125, completionTokens: 30, wantError: "missing id",
		},
		{
			name:         "negative input token",
			item:         map[string]any{"id": 7, "input_tokens": -1, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5},
			promptTokens: 104, completionTokens: 30, wantError: "token metrics invalid",
		},
		{
			name:         "negative output token",
			item:         map[string]any{"id": 7, "input_tokens": 20, "output_tokens": -1, "cache_read_tokens": 100, "cache_creation_tokens": 5},
			promptTokens: 125, completionTokens: -1, wantError: "token metrics invalid",
		},
		{
			name:         "negative cache read token",
			item:         map[string]any{"id": 7, "input_tokens": 20, "output_tokens": 30, "cache_read_tokens": -1, "cache_creation_tokens": 5},
			promptTokens: 24, completionTokens: 30, wantError: "token metrics invalid",
		},
		{
			name:         "negative cache creation token",
			item:         map[string]any{"id": 7, "input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": -1},
			promptTokens: 119, completionTokens: 30, wantError: "token metrics invalid",
		},
		{
			name:         "prompt overflow",
			item:         map[string]any{"id": 7, "input_tokens": int64(math.MaxInt64), "output_tokens": 0, "cache_read_tokens": 1, "cache_creation_tokens": 0},
			promptTokens: math.MaxInt64, completionTokens: 0, wantError: "token metrics invalid",
		},
		{
			name:         "cache creation overflow",
			item:         map[string]any{"id": 7, "input_tokens": int64(math.MaxInt64 - 1), "output_tokens": 0, "cache_read_tokens": 1, "cache_creation_tokens": 1},
			promptTokens: math.MaxInt64, completionTokens: 0, wantError: "token metrics invalid",
		},
		{
			name:         "total overflow",
			item:         map[string]any{"id": 7, "input_tokens": int64(math.MaxInt64), "output_tokens": 1, "cache_read_tokens": 0, "cache_creation_tokens": 0},
			promptTokens: math.MaxInt64, completionTokens: 1, wantError: "token total overflow",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := map[string]any{
				"api_key_id": 9, "request_id": "client:req-invalid", "model": "gpt-test",
				"actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano),
			}
			for key, value := range test.item {
				item[key] = value
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code": 0, "message": "success", "data": map[string]any{
						"items": []map[string]any{item}, "total": 1, "page": 1, "page_size": 1000, "pages": 1,
					},
				})
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
				APIKeyID: 9, ModelName: "gpt-test", PromptTokens: test.promptTokens, CompletionTokens: test.completionTokens,
				RequestID: "req-invalid", StartedAt: now, FinishedAt: now,
			})
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("err = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestLookupRequestCostExactRequestIDRejectsNewerPrefixGrowth(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	pageOneRequests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		requests++
		requestedPage := 1
		pageSize := 3
		total := 6
		pages := 2
		items := []map[string]any{
			{"id": 8, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
			{"id": 7, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
			{"id": 6, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
		}
		switch r.URL.Query().Get("page") {
		case "1":
			pageOneRequests++
			if pageOneRequests > 1 {
				items = []map[string]any{
					{"id": 10, "api_key_id": 9, "model": "other", "created_at": now.Add(12 * time.Second).Format(time.RFC3339Nano)},
					{"id": 9, "api_key_id": 9, "model": "other", "created_at": now.Add(11 * time.Second).Format(time.RFC3339Nano)},
					{"id": 8, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
				}
				total = 8
				pages = 3
			}
		case "2":
			requestedPage = 2
			items = []map[string]any{
				{"id": 5, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
				{
					"id": 4, "api_key_id": 9, "request_id": "client:req-snapshot", "model": "gpt-test",
					"input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5,
					"actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano),
				},
				{"id": 3, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
			}
			total = 7
			pages = 3
		case "3":
			requestedPage = 3
			items = []map[string]any{
				{"id": 2, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
				{"id": 1, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
			}
			total = 8
			pages = 3
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{"items": items, "total": total, "page": requestedPage, "page_size": pageSize, "pages": pages},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 125, CompletionTokens: 30, RequestID: "req-snapshot",
		StartedAt: now.Add(-time.Second), FinishedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "exact pagination snapshot changed") || result != nil {
		t.Fatalf("requests=%d result=%+v err=%v, want exact snapshot rejection", requests, result, err)
	}
	if requests != 4 {
		t.Fatalf("requests=%d, want three scanned pages and final page one", requests)
	}
}

func TestLookupRequestCostRejectsLateWindowInsertionBeforeReturningFallback(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	pageOneRequests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		requests++
		requestedPage := 1
		items := []map[string]any{
			{
				"id": 4, "api_key_id": 9, "request_id": "unrelated", "model": "gpt-test",
				"input_tokens": 20, "output_tokens": 30, "cache_read_tokens": 100, "cache_creation_tokens": 5,
				"actual_cost": 0.0123, "created_at": now.Format(time.RFC3339Nano),
			},
			{"id": 3, "api_key_id": 9, "model": "other", "created_at": now.Add(-time.Second).Format(time.RFC3339Nano)},
		}
		total := 3
		if r.URL.Query().Get("page") == "2" {
			requestedPage = 2
			items = []map[string]any{
				{"id": 2, "api_key_id": 9, "model": "other", "created_at": now.Add(-2 * time.Second).Format(time.RFC3339Nano)},
				{"id": 1, "api_key_id": 9, "model": "other", "created_at": now.Add(-3 * time.Second).Format(time.RFC3339Nano)},
			}
			total = 4
		} else {
			pageOneRequests++
			if pageOneRequests > 1 {
				items = []map[string]any{
					items[0],
					{"id": 5, "api_key_id": 9, "model": "other", "created_at": now.Add(-500 * time.Millisecond).Format(time.RFC3339Nano)},
				}
				total = 4
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{"items": items, "total": total, "page": requestedPage, "page_size": 2, "pages": 2},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 125, CompletionTokens: 30,
		StartedAt: now.Add(-3 * time.Second), FinishedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "growth was not confined") || result != nil {
		t.Fatalf("result=%+v err=%v, want fail-closed late insertion", result, err)
	}
	if requests != 3 {
		t.Fatalf("requests = %d, want 3", requests)
	}
}

func TestLookupRequestCostRejectsPaginationMetadataRegression(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		requests++
		requestedPage := 1
		total := 4
		items := []map[string]any{
			{"id": 4, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
			{"id": 3, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
		}
		if r.URL.Query().Get("page") == "2" {
			requestedPage = 2
			total = 3
			items = []map[string]any{
				{"id": 2, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)},
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{"items": items, "total": total, "page": requestedPage, "page_size": 2, "pages": 2},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 1, CompletionTokens: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "total moved backwards") || result != nil {
		t.Fatalf("result=%+v err=%v, want metadata regression", result, err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestLookupRequestCostFinalSnapshotValidatesRowsAfterAnchor(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		secondCreatedAt := now.Add(-time.Second).Format(time.RFC3339Nano)
		if requests > 1 {
			secondCreatedAt = "not-a-time"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{
				"items": []map[string]any{
					{
						"id": 3, "api_key_id": 9, "request_id": "client:req-final-row", "model": "gpt-test",
						"input_tokens": 10, "output_tokens": 2, "actual_cost": 0.01,
						"created_at": now.Format(time.RFC3339Nano),
					},
					{"id": 2, "api_key_id": 9, "model": "other", "created_at": secondCreatedAt},
				},
				"total": 2, "page": 1, "page_size": 2, "pages": 1,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 10, CompletionTokens: 2,
		RequestID: "req-final-row", StartedAt: now.Add(-time.Second), FinishedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "final snapshot usage timestamp invalid") || result != nil {
		t.Fatalf("result=%+v err=%v, want final-page row validation", result, err)
	}
}

func TestLookupRequestCostContextCancellationDiscardsMatchedCost(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	requests := 0
	var cancel context.CancelFunc
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests > 1 {
			cancel()
			<-r.Context().Done()
			return
		}
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"request_id":"client:req-cancel","model":"gpt-test","input_tokens":10,"output_tokens":2,"actual_cost":0.01,"created_at":%q}],"total":1,"page":1,"page_size":1000,"pages":1}}`, now.Format(time.RFC3339Nano))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancelContext := context.WithCancel(context.Background())
	cancel = cancelContext
	defer cancelContext()
	result, err := New().LookupRequestCost(ctx, &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 10, CompletionTokens: 2,
		RequestID: "req-cancel", StartedAt: now.Add(-time.Second), FinishedAt: now,
	})
	if err == nil || result != nil {
		t.Fatalf("result=%+v err=%v, want cancellation with no assigned cost", result, err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestLookupRequestCostRejectsResponsePageSizeAboveRequestedMaximum(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{
				"items": []map[string]any{{"id": 1, "api_key_id": 9, "model": "other", "created_at": now.Format(time.RFC3339Nano)}},
				"total": 1, "page": 1, "page_size": 1001, "pages": 1,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{APIKeyID: 9})
	if err == nil || !strings.Contains(err.Error(), "exceeds requested maximum") || result != nil {
		t.Fatalf("result=%+v err=%v, want page_size rejection", result, err)
	}
}

func TestLookupRequestCostStopsAfterBoundedPagesWhenServerClampsPageSize(t *testing.T) {
	now := time.Now().Truncate(time.Microsecond)
	pages := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage", func(w http.ResponseWriter, r *http.Request) {
		pages++
		items := make([]map[string]any, 0, 100)
		pageTime := now.Add(-time.Duration(pages) * time.Millisecond)
		for i := 0; i < 100; i++ {
			item := map[string]any{
				"id": int64(3000 - (pages-1)*100 - i), "api_key_id": 9, "model": "other", "input_tokens": 1,
				"output_tokens": 1, "actual_cost": 0.1, "created_at": pageTime.Format(time.RFC3339Nano),
			}
			if pages == 1 && i == 0 {
				item["model"] = "gpt-test"
				item["input_tokens"] = 20
				item["output_tokens"] = 30
				item["cache_read_tokens"] = 100
				item["cache_creation_tokens"] = 5
				item["actual_cost"] = 0.0123
			}
			items = append(items, item)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0, "message": "success",
			"data": map[string]any{"items": items, "total": 2001, "page": pages, "page_size": 100, "pages": 21},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 125, CompletionTokens: 30,
	})
	if err == nil || !strings.Contains(err.Error(), "usage scan incomplete") {
		t.Fatalf("err = %v, want bounded incomplete scan", err)
	}
	if pages != 10 {
		t.Fatalf("pages = %d, want 10", pages)
	}
}

func TestGetCostsAppliesUpstreamRechargeMultiplier(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"today_actual_cost":14.4,"total_actual_cost":72}}`))
	})
	mux.HandleFunc("/api/v1/usage/stats", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"total_actual_cost":14.4}}`))
	})
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"balance_recharge_multiplier":7.2}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	}, &connector.AuthSession{
		AccessToken: "token",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 2 {
		t.Fatalf("today cost = %v, want 2", res.TodayCost)
	}
	if res.TotalCost != 10 {
		t.Fatalf("total cost = %v, want 10", res.TotalCost)
	}
	if res.RawTodayCost != 14.4 || res.RawTotalCost != 72 || res.AppliedFactor != 1.0/7.2 || res.FactorSource != "advertised" {
		t.Fatalf("cost provenance = %+v", res)
	}
	if res.TodayCost != connector.ApplyRechargeMultiplier(res.RawTodayCost, &res.AppliedFactor, connector.RechargeMultiplierModeMultiply) ||
		res.TotalCost != connector.ApplyRechargeMultiplier(res.RawTotalCost, &res.AppliedFactor, connector.RechargeMultiplierModeMultiply) {
		t.Fatalf("compatibility costs must equal raw*factor: %+v", res)
	}
}

func TestGetProviderFactorUsesOneCheckoutRequest(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/v1/payment/checkout-info" {
			t.Fatalf("unexpected provider-factor path %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"balance_recharge_multiplier":8}}`))
	}))
	defer srv.Close()

	observation, err := New().GetProviderFactor(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetProviderFactor: %v", err)
	}
	if observation == nil || observation.AppliedFactor != 0.125 ||
		observation.FactorSource != "advertised" || observation.SampledAt.IsZero() {
		t.Fatalf("provider factor observation = %+v", observation)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("checkout calls = %d, want 1", got)
	}
}

func TestGetProviderFactorFailsClosedOnMissingAdvertisedEvidence(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{}}`))
	}))
	defer srv.Close()

	observation, err := New().GetProviderFactor(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	}, &connector.AuthSession{AccessToken: "token"})
	if err == nil || observation != nil || !strings.Contains(err.Error(), "missing or invalid") {
		t.Fatalf("GetProviderFactor = %+v, %v", observation, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("checkout calls = %d, want 1", got)
	}
}

func TestGetBalanceAppliesManualRechargeMultiplier(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/me", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"balance":12.5}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	multiplier := 3.0
	res, err := c.GetBalance(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplier:     &multiplier,
		RechargeMultiplierMode: connector.RechargeMultiplierModeMultiply,
	}, &connector.AuthSession{
		AccessToken: "token",
	})
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if res.Balance != 37.5 {
		t.Fatalf("balance = %v, want 37.5", res.Balance)
	}
	if res.RawBalance != 12.5 || res.AppliedFactor != 3 || res.FactorSource != "manual" {
		t.Fatalf("balance provenance = %+v", res)
	}
}

func TestGetRechargeInfo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"methods":{"alipay_direct":{"payment_type":"alipay","currency":"CNY","fee_rate":0,"single_min":5,"single_max":100},"wxpay":{"payment_type":"wxpay","currency":"CNY","fee_rate":0,"single_min":8,"single_max":80},"stripe":{"payment_type":"stripe","single_min":10,"single_max":90}},"global_min":5,"global_max":100,"help_text":"请联系客服","help_image_url":"https://img.example/help.png","alipay_force_qrcode":true}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetRechargeInfo(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetRechargeInfo: %v", err)
	}
	if len(info.Methods) != 2 {
		t.Fatalf("methods len = %d, want 2", len(info.Methods))
	}
	if info.Methods[0].Type != "alipay" || info.Methods[1].Type != "wxpay" {
		t.Fatalf("methods = %#v", info.Methods)
	}
	if !info.AlipayForceQRCode {
		t.Fatal("want force qrcode")
	}
}

func TestGetRechargeHistoryReturnsOnlyCompletedBalanceRecords(t *testing.T) {
	completed := "2026-07-15T10:20:30+08:00"
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders/my", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if query.Get("page") != "1" || query.Get("page_size") != "100" || query.Get("status") != "completed" || query.Get("order_type") != "balance" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"","data":{"items":[
			{"id":1,"out_trade_no":"order-1","amount":100,"pay_amount":14,"status":"completed","order_type":"balance","completed_at":%q},
			{"id":2,"amount":50,"pay_amount":8,"status":"COMPLETED","order_type":"BALANCE","completed_at":%q},
			{"id":3,"out_trade_no":"subscription","amount":100,"pay_amount":10,"status":"completed","order_type":"subscription","completed_at":%q},
			{"id":4,"out_trade_no":"pending","amount":100,"pay_amount":10,"status":"pending","order_type":"balance","completed_at":%q},
			{"id":5,"out_trade_no":"zero","amount":100,"pay_amount":0,"status":"completed","order_type":"balance","completed_at":%q}
		],"total":5,"page":1,"page_size":100,"pages":1}}`, completed, completed, completed, completed, completed)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var history connector.RechargeHistory = New()
	records, err := history.GetRechargeHistory(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetRechargeHistory: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("records = %#v", records)
	}
	if records[0].SourceID != "order-1" || records[0].CreditedAmount != 100 || records[0].PaidAmount == nil || *records[0].PaidAmount != 14 || records[0].CompletedAt.Format(time.RFC3339) != "2026-07-15T02:20:30Z" {
		t.Fatalf("first record = %#v", records[0])
	}
	if records[1].SourceID != "2" {
		t.Fatalf("fallback record = %#v", records[1])
	}
	if records[2].SourceID != "zero" || records[2].PaidAmount == nil || *records[2].PaidAmount != 0 {
		t.Fatalf("free-credit record = %#v", records[2])
	}
}

func TestGetRechargeHistoryReportsUnsupportedEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	_, err := New().GetRechargeHistory(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if !errors.Is(err, connector.ErrUnsupported) {
		t.Fatalf("error = %v, want ErrUnsupported", err)
	}
}

func TestGetRechargeInfoFiltersUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"methods":{"alipay":{"single_min":5,"single_max":100,"available":false},"wxpay":{"single_min":8,"single_max":80,"available":true}},"global_min":5,"global_max":100}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetRechargeInfo(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetRechargeInfo: %v", err)
	}
	if len(info.Methods) != 1 || info.Methods[0].Type != "wxpay" {
		t.Fatalf("methods = %#v", info.Methods)
	}
}

func TestGetRechargeInfoFallsBackToAvailableAlias(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"methods":{"alipay":{"single_min":50,"single_max":60,"available":false},"alipay_direct":{"single_min":5,"single_max":100,"available":true}},"global_min":5,"global_max":100}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetRechargeInfo(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetRechargeInfo: %v", err)
	}
	if len(info.Methods) != 1 || info.Methods[0].Type != "alipay" || info.Methods[0].MinAmount != 5 {
		t.Fatalf("methods = %#v", info.Methods)
	}
}

func TestCreateRechargePrefersQRCodeOnDesktop(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test","expires_at":"2026-01-02T03:04:05Z"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
		IsMobile:      false,
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "qrcode" || launch.QRCode == "" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateRechargePrefersRedirectOnMobile(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
		IsMobile:      true,
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "redirect" || launch.PayURL != "https://pay.example.com/redirect" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateRechargeUsesPaymentModeRedirect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"payment_mode":"redirect","pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
		IsMobile:      false,
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "redirect" || launch.PayURL != "https://pay.example.com/redirect" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateRechargeUsesPaymentModeQRCode(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"payment_mode":"native","pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
		IsMobile:      true,
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "qrcode" || launch.QRCode == "" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateRechargeRejectsComplexFlow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"result_type":"oauth_required","oauth":{"authorize_url":"/api/v1/auth/oauth/wechat/payment/start"}}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	_, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
	})
	if err == nil || !strings.Contains(err.Error(), "暂不支持") {
		t.Fatalf("err = %v, want unsupported error", err)
	}
}

func TestGetSubscriptionInfo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"methods":{"alipay_direct":{"payment_type":"alipay","currency":"CNY","single_min":5,"single_max":100,"available":true},"wxpay":{"payment_type":"wxpay","currency":"CNY","single_min":8,"single_max":80,"available":true},"stripe":{"payment_type":"stripe","available":true}},"plans":[{"id":7,"group_id":3,"group_name":"pro","name":"Pro","description":"专业版","price":29.9,"daily_limit_usd":10,"weekly_limit_usd":50,"monthly_limit_usd":200,"validity_days":30,"validity_unit":"day","features":["高速","独享"],"for_sale":true}]}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetSubscriptionInfo(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetSubscriptionInfo: %v", err)
	}
	if len(info.Plans) != 1 || info.Plans[0].ID != "7" || info.Plans[0].GroupName != "pro" || len(info.Plans[0].Features) != 2 {
		t.Fatalf("plans = %#v", info.Plans)
	}
	if info.Plans[0].DailyLimitUSD == nil || *info.Plans[0].DailyLimitUSD != 10 || info.Plans[0].WeeklyLimitUSD == nil || *info.Plans[0].WeeklyLimitUSD != 50 || info.Plans[0].MonthlyLimitUSD == nil || *info.Plans[0].MonthlyLimitUSD != 200 {
		t.Fatalf("limits = %#v", info.Plans[0])
	}
	if len(info.Methods) != 2 || info.Methods[0].Type != "alipay" || info.Methods[1].Type != "wxpay" {
		t.Fatalf("methods = %#v", info.Methods)
	}
	if got := strings.Join(info.Plans[0].PaymentMethods, ","); got != "alipay,wxpay" {
		t.Fatalf("payment methods = %q", got)
	}
}

func TestGetSubscriptionUsage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/subscriptions/progress", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"subscription":{"id":9,"group_id":3,"status":"active","starts_at":"2026-01-01T00:00:00Z","expires_at":"2026-02-01T00:00:00Z","group":{"id":3,"name":"pro"}},"progress":{"id":9,"group_name":"pro","expires_at":"2026-02-01T00:00:00Z","expires_in_days":12,"daily":{"limit_usd":10,"used_usd":8,"remaining_usd":2,"percentage":80,"window_start":"2026-01-02T00:00:00Z","resets_at":"2026-01-03T00:00:00Z","resets_in_seconds":3600},"weekly":{"limit_usd":0,"used_usd":0,"remaining_usd":0,"percentage":0},"monthly":{"limit_usd":100,"used_usd":95,"remaining_usd":5,"percentage":95,"window_start":"2026-01-01T00:00:00Z","resets_at":"2026-02-01T00:00:00Z","resets_in_seconds":86400}}}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetSubscriptionUsage(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetSubscriptionUsage: %v", err)
	}
	if len(info.Items) != 1 {
		t.Fatalf("items = %#v", info.Items)
	}
	item := info.Items[0]
	if item.ID != 9 || item.GroupID != 3 || item.GroupName != "pro" || item.Status != "active" {
		t.Fatalf("item = %#v", item)
	}
	if item.Daily == nil || item.Daily.UsedPercent != 80 || item.Daily.RemainingPercent != 20 || item.Daily.RemainingUSD != 2 {
		t.Fatalf("daily = %#v", item.Daily)
	}
	if item.Weekly != nil {
		t.Fatalf("weekly should be hidden for unlimited limit: %#v", item.Weekly)
	}
	if item.Monthly == nil || item.Monthly.RemainingPercent != 5 {
		t.Fatalf("monthly = %#v", item.Monthly)
	}
}

func TestGetSubscriptionUsageDirectSubscriptionList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/subscriptions/progress", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":21,"user_id":1,"group_id":14,"starts_at":"2026-06-17T20:08:51.441599+08:00","expires_at":"2026-06-18T20:08:51.441599+08:00","status":"active","daily_window_start":null,"weekly_window_start":null,"monthly_window_start":null,"daily_usage_usd":25,"weekly_usage_usd":0,"monthly_usage_usd":0,"group":{"id":14,"name":"Codex 100刀","daily_limit_usd":100,"weekly_limit_usd":0,"monthly_limit_usd":0}}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetSubscriptionUsage(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetSubscriptionUsage: %v", err)
	}
	if len(info.Items) != 1 {
		t.Fatalf("items = %#v", info.Items)
	}
	item := info.Items[0]
	if item.ID != 21 || item.GroupID != 14 || item.GroupName != "Codex 100刀" || item.Status != "active" {
		t.Fatalf("item = %#v", item)
	}
	if item.Daily == nil || item.Daily.LimitUSD != 100 || item.Daily.UsedUSD != 25 || item.Daily.RemainingUSD != 75 || item.Daily.RemainingPercent != 75 {
		t.Fatalf("daily = %#v", item.Daily)
	}
	if item.Weekly != nil || item.Monthly != nil {
		t.Fatalf("unlimited windows should be hidden: weekly=%#v monthly=%#v", item.Weekly, item.Monthly)
	}
}

func TestGetSubscriptionUsageSubscriptionFallbackLimits(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/subscriptions/progress", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"subscription":{"id":21,"group_id":14,"status":"active","starts_at":"2026-06-17T20:08:51.441599+08:00","expires_at":"2026-06-18T20:08:51.441599+08:00","daily_usage_usd":0,"weekly_usage_usd":0,"monthly_usage_usd":0,"group":{"id":14,"name":"Codex 100刀","daily_limit_usd":100,"weekly_limit_usd":0,"monthly_limit_usd":0}},"progress":{"id":21,"group_name":"Codex 100刀","expires_at":"2026-06-18T20:08:51.441599+08:00","expires_in_days":1}}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetSubscriptionUsage(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetSubscriptionUsage: %v", err)
	}
	if len(info.Items) != 1 {
		t.Fatalf("items = %#v", info.Items)
	}
	item := info.Items[0]
	if item.Daily == nil || item.Daily.LimitUSD != 100 || item.Daily.UsedUSD != 0 || item.Daily.RemainingUSD != 100 || item.Daily.RemainingPercent != 100 {
		t.Fatalf("daily = %#v", item.Daily)
	}
	if item.Weekly != nil || item.Monthly != nil {
		t.Fatalf("unlimited windows should be hidden: weekly=%#v monthly=%#v", item.Weekly, item.Monthly)
	}
}

func TestCreateSubscriptionQRCode(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["order_type"] != "subscription" || body["plan_id"] != float64(7) || body["payment_type"] != "wxpay" {
			t.Fatalf("body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"payment_mode":"native","pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test","expires_at":"2026-01-02T03:04:05Z"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateSubscription(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.SubscriptionRequest{
		PlanID:        "7",
		PaymentMethod: "wxpay",
		IsMobile:      true,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if launch.Mode != "qrcode" || launch.QRCode == "" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateSubscriptionRedirect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"payment_mode":"redirect","pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateSubscription(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.SubscriptionRequest{
		PlanID:        "7",
		PaymentMethod: "alipay",
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if launch.Mode != "redirect" || launch.PayURL != "https://pay.example.com/redirect" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateSubscriptionRejectsComplexFlow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"client_secret":"secret"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	_, err := c.CreateSubscription(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.SubscriptionRequest{
		PlanID:        "7",
		PaymentMethod: "wxpay",
	})
	if err == nil || !strings.Contains(err.Error(), "暂不支持") {
		t.Fatalf("err = %v, want unsupported error", err)
	}
}

func TestListAPIKeys(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":3,"name":"pro","description":"专业组","rate_multiplier":1.2}]}`))
	})
	mux.HandleFunc("/api/v1/groups/rates", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"3":1.5}}`))
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("page"); got != "2" {
			t.Fatalf("page = %q, want 2", got)
		}
		if got := r.URL.Query().Get("page_size"); got != "10" {
			t.Fatalf("page_size = %q, want 10", got)
		}
		if got := r.URL.Query().Get("search"); got != "main" {
			t.Fatalf("search = %q, want main", got)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"key":"sk-full","name":"main","group_id":3,"status":"active","ip_whitelist":["1.1.1.1"],"ip_blacklist":[],"quota":10,"quota_used":2,"expires_at":null,"rate_limit_5h":1,"rate_limit_1d":0,"rate_limit_7d":0,"usage_5h":0.5}],"total":11,"page":2,"page_size":10,"pages":2}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	page, err := c.ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{
		Page:     2,
		PageSize: 10,
		Search:   "main",
	})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Key != "sk-full" || page.Items[0].GroupID == nil || *page.Items[0].GroupID != 3 || page.Items[0].GroupName != "pro" || page.Items[0].GroupRatio != 1.5 {
		t.Fatalf("page = %#v", page)
	}
	key := page.Items[0]
	if key.Source != connector.APIKeySourceSub2API || !key.ObservedFields.Quota || !key.ObservedFields.ExpiresAt || !key.ObservedFields.IPWhitelist || !key.ObservedFields.IPBlacklist || !key.ObservedFields.RateLimit5h || !key.ObservedFields.RateLimit1d || !key.ObservedFields.RateLimit7d {
		t.Fatalf("observed sub2api key fields = source=%q fields=%+v", key.Source, key.ObservedFields)
	}
}

func TestListAPIKeysFailsClosedWhenGroupLookupFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary group failure", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"name":"BOG-p-0123456789abcdef","group_id":3,"status":"active"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 10, RequireCompleteGroupIdentity: true})
	if err == nil || page != nil || !strings.Contains(err.Error(), "api key group lookup") {
		t.Fatalf("ListAPIKeys = %#v, %v; want group lookup error", page, err)
	}
}

func TestListAPIKeysKeepsOrdinaryInventoryWhenGroupLookupFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary group failure", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"name":"ordinary","group_id":3,"status":"active"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 10})
	if err != nil || page == nil || len(page.Items) != 1 || page.Items[0].GroupDirectoryComplete {
		t.Fatalf("ordinary ListAPIKeys = %#v, %v; want unresolved inventory", page, err)
	}
}

func TestListAPIKeysStrictGroupIdentityRejectsOmittedGroupFields(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"name":"BOG-p-0123456789abcdef","status":"active"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 10, RequireCompleteGroupIdentity: true})
	if err == nil || page != nil || !strings.Contains(err.Error(), "group identity is incomplete") {
		t.Fatalf("strict ListAPIKeys = %#v, %v; want incomplete group identity", page, err)
	}
}

func TestListAPIKeysMarksDeletedGroupIDFromCompleteDirectory(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":4,"name":"current","rate_multiplier":1}]}`))
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"name":"BOG-p-0123456789abcdef","group_id":3,"status":"active"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 10, RequireCompleteGroupIdentity: true})
	if err != nil || page == nil || len(page.Items) != 1 || !page.Items[0].GroupDirectoryComplete || page.Items[0].GroupName != "" {
		t.Fatalf("deleted group evidence = %#v, %v", page, err)
	}
}

func TestListAPIKeysStrictIdentityRejectsInvalidGroupDirectory(t *testing.T) {
	for _, test := range []struct {
		name       string
		groupsJSON string
		want       string
	}{
		{name: "duplicate id", groupsJSON: `[{"id":3,"name":"first"},{"id":3,"name":"second"}]`, want: "duplicate id"},
		{name: "duplicate name", groupsJSON: `[{"id":3,"name":"same"},{"id":4,"name":" SAME "}]`, want: "duplicate name"},
		{name: "invalid id", groupsJSON: `[{"id":0,"name":"invalid"}]`, want: "invalid id"},
		{name: "empty name", groupsJSON: `[{"id":3,"name":"  "}]`, want: "empty name"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"code":0,"message":"success","data":` + test.groupsJSON + `}`))
			})
			mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"name":"BOG-p-0123456789abcdef","group_id":3,"status":"active"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 10, RequireCompleteGroupIdentity: true})
			if err == nil || page != nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("strict invalid directory = %#v, %v; want %q", page, err, test.want)
			}
		})
	}
}

func TestListAPIKeysUsesEmbeddedGroupWithoutLookup(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "must not be required", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"name":"BOG-p-0123456789abcdef","group_id":3,"group":{"id":3,"name":"pro","rate_multiplier":1.2},"status":"active"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 10})
	if err != nil || page == nil || len(page.Items) != 1 || page.Items[0].GroupName != "pro" {
		t.Fatalf("ListAPIKeys = %#v, %v; want embedded group", page, err)
	}
}

func TestListAPIKeysPreservesOmittedCreationFields(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":3,"name":"pro","rate_multiplier":1}]}`))
	})
	mux.HandleFunc("/api/v1/groups/rates", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"3":1}}`))
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"name":"BOG - pro","group_id":3,"status":"active"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("items = %#v", page.Items)
	}
	key := page.Items[0]
	if key.Source != connector.APIKeySourceSub2API || key.ObservedFields.Quota || key.ObservedFields.ExpiresAt || key.ObservedFields.IPWhitelist || key.ObservedFields.IPBlacklist || key.ObservedFields.RateLimit5h || key.ObservedFields.RateLimit1d || key.ObservedFields.RateLimit7d {
		t.Fatalf("omitted fields were treated as observed: source=%q fields=%+v", key.Source, key.ObservedFields)
	}
}

func TestListAPIKeysTreatsNullNumericRecoveryFieldsAsUnknown(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[]}`))
	})
	mux.HandleFunc("/api/v1/groups/rates", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{}}`))
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"name":"BOG - pro","status":"active","quota":null,"expires_at":null,"ip_whitelist":[],"ip_blacklist":[],"rate_limit_5h":null,"rate_limit_1d":null,"rate_limit_7d":null}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("items = %#v", page.Items)
	}
	key := page.Items[0]
	if key.ObservedFields.Quota || key.ObservedFields.RateLimit5h || key.ObservedFields.RateLimit1d || key.ObservedFields.RateLimit7d {
		t.Fatalf("null numeric fields were treated as known zeroes: %+v", key.ObservedFields)
	}
	if !key.NullFields.Quota || !key.NullFields.RateLimit5h || !key.NullFields.RateLimit1d || !key.NullFields.RateLimit7d {
		t.Fatalf("null numeric fields were not preserved: %+v", key.NullFields)
	}
	if !key.ObservedFields.ExpiresAt || key.ExpiresAt != nil {
		t.Fatalf("expires_at:null lost non-expiring semantics: observed=%v value=%v", key.ObservedFields.ExpiresAt, key.ExpiresAt)
	}
}

func TestListAPIKeysPreservesMissingPaginationMetadata(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[]}`))
	})
	mux.HandleFunc("/api/v1/groups/rates", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{}}`))
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":1},{"id":2}]}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: 1, PageSize: 2})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if page.Total != 0 || page.Page != 1 || page.PageSize != 2 || page.Pages != 0 || len(page.Items) != 2 {
		t.Fatalf("page = %#v", page)
	}
}

func TestListAPIKeysRejectsInconsistentPaginationMetadata(t *testing.T) {
	for _, test := range []struct {
		name      string
		queryPage int
		data      string
	}{
		{name: "page differs from request", queryPage: 2, data: `{"items":[],"page":1}`},
		{name: "page exceeds pages", queryPage: 2, data: `{"items":[],"total":10,"page":2,"page_size":10,"pages":1}`},
		{name: "pages disagree with total", queryPage: 1, data: `{"items":[{},{},{},{},{},{},{},{},{},{}],"total":11,"page":1,"page_size":10,"pages":1}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":%s}`, test.data)
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{Page: test.queryPage, PageSize: 10})
			if err == nil || page != nil || !strings.Contains(err.Error(), "pagination") {
				t.Fatalf("ListAPIKeys = %#v, %v; want pagination error", page, err)
			}
		})
	}
}

func TestListAPIKeyGroups(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":3,"name":"pro","description":"专业组","rate_multiplier":1.2}]}`))
	})
	mux.HandleFunc("/api/v1/groups/rates", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"3":1.5}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	groups, err := c.ListAPIKeyGroups(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("ListAPIKeyGroups: %v", err)
	}
	if len(groups) != 1 || groups[0].ID == nil || *groups[0].ID != 3 || groups[0].Name != "pro" || groups[0].Ratio != 1.5 {
		t.Fatalf("groups = %#v", groups)
	}
}

func TestGetRatesRejectsIncompleteOverridesSnapshot(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":3,"name":"pro","rate_multiplier":1.2}]}`))
	})
	mux.HandleFunc("/api/v1/groups/rates", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary failure", http.StatusBadGateway)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	snapshot, err := New().GetRates(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if err == nil || snapshot != nil || !strings.Contains(err.Error(), "group rates") {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}

func TestGetAnnouncements(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/announcements", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":9,"title":"维护公告","content":"今晚维护","notify_mode":"popup","created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T04:04:05Z"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	items, err := c.GetAnnouncements(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1", len(items))
	}
	if items[0].SourceKey != "9" || items[0].Title != "维护公告" || items[0].Type != "popup" {
		t.Fatalf("item = %#v", items[0])
	}
	if items[0].PublishedAt == nil || items[0].PublishedAt.Format("2006-01-02T15:04:05Z") != "2026-01-02T03:04:05Z" {
		t.Fatalf("published at = %#v", items[0].PublishedAt)
	}
	if items[0].SourceUpdatedAt == nil || items[0].SourceUpdatedAt.Format("2006-01-02T15:04:05Z") != "2026-01-02T04:04:05Z" {
		t.Fatalf("updated at = %#v", items[0].SourceUpdatedAt)
	}
}

func TestCreateUpdateDeleteRevealAPIKey(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode create: %v", err)
		}
		if body["name"] != "main" || body["custom_key"] != "sk-custom" {
			t.Fatalf("create body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":8,"key":"sk-custom","name":"main","status":"active"}}`))
	})
	mux.HandleFunc("/api/v1/keys/8", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode update: %v", err)
			}
			if body["status"] != "inactive" {
				t.Fatalf("update body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":8,"key":"sk-custom","name":"main","status":"disabled"}}`))
		case http.MethodDelete:
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"message":"ok"}}`))
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":8,"key":"sk-custom","name":"main","status":"active"}}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session := &connector.AuthSession{AccessToken: "token"}
	created, err := c.CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, connector.APIKeyCreateRequest{
		Name:      "main",
		CustomKey: "sk-custom",
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if created.Key != "sk-custom" {
		t.Fatalf("created = %#v", created)
	}
	updated, err := c.UpdateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 8, connector.APIKeyUpdateRequest{
		Status: strPtr("disabled"),
	})
	if err != nil {
		t.Fatalf("UpdateAPIKey: %v", err)
	}
	if updated.Status != "disabled" {
		t.Fatalf("updated status = %q", updated.Status)
	}
	if err := c.DeleteAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 8); err != nil {
		t.Fatalf("DeleteAPIKey: %v", err)
	}
	key, err := c.RevealAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 8)
	if err != nil {
		t.Fatalf("RevealAPIKey: %v", err)
	}
	if key != "sk-custom" {
		t.Fatalf("key = %q", key)
	}
}

func TestCreateAPIKeyRejectsResponseWithoutIDOrActualKey(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"name":"main","status":"active"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	created, err := New().CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyCreateRequest{Name: "main"})
	if err == nil || created != nil || !strings.Contains(err.Error(), "missing key id and actual key") {
		t.Fatalf("CreateAPIKey = %#v, %v; want missing identity error", created, err)
	}
}

func TestGetModelPricesUsesAuthenticatedModelPlazaPricing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/model-plaza", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer account-token" {
			t.Fatalf("authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{
			"code":0,
			"message":"success",
			"data":{"description":"current prices","groups":[{
				"id":7,
				"name":"PLUS",
				"rate_multiplier":0.01,
				"user_rate_multiplier":0.02,
				"peak_rate_enabled":false,
				"long_context_pricing_enabled":true,
				"models":[{
					"name":"gpt-5.6-sol",
					"platform":"openai",
					"pricing":{"billing_mode":"token","input_price":0.000005,"output_price":0.00003,"cache_write_price":0.00000625,"cache_read_price":0.0000005,"intervals":[]},
					"official_pricing":{"input_price":99,"output_price":99,"cache_write_price":99,"cache_read_price":99}
				},{
					"name":"gpt-tiered",
					"pricing":{"billing_mode":"token","input_price":0.0000025,"output_price":0.000015,"cache_write_price":0.000003125,"cache_read_price":0.00000025,"intervals":[
						{"min_tokens":0,"max_tokens":272000,"input_price":0.0000025,"output_price":0.000015,"cache_write_price":0.000003125,"cache_read_price":0.00000025},
						{"min_tokens":272000,"max_tokens":null,"input_price":0.000005,"output_price":0.0000225,"cache_write_price":0.00000625,"cache_read_price":0.0000005}
					]},
					"long_context_basis":"whole_request"
				},{
					"name":"cache-unknown",
					"pricing":{"billing_mode":"token","input_price":0.000001,"output_price":0.000002,"cache_write_price":null,"cache_read_price":null,"intervals":[]}
				}]
			}]}}
		`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New()
	observedAt := time.Date(2026, time.August, 30, 12, 0, 0, 123456789, time.FixedZone("test", 8*60*60))
	client.now = func() time.Time { return observedAt }
	snapshot, err := client.GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "account-token"})
	if err != nil {
		t.Fatalf("GetModelPrices: %v", err)
	}
	if snapshot == nil || !snapshot.Complete || snapshot.SourceVersion != sub2APIModelPriceSourceVersion || len(snapshot.SourceChecksum) != 64 ||
		!snapshot.ObservedAt.Equal(observedAt.UTC().Truncate(time.Microsecond)) || len(snapshot.Items) != 3 {
		t.Fatalf("model price snapshot = %+v", snapshot)
	}

	static := snapshot.Items[0]
	if static.ModelName != "cache-unknown" || static.UpstreamGroupID != 7 || static.UpstreamGroupName != "PLUS" ||
		static.GroupRatio == nil || *static.GroupRatio != 0.02 || static.InputUnitPriceUSD != 1 || static.OutputUnitPriceUSD != 2 ||
		static.CacheReadUnitPriceUSD != nil || static.CacheWriteUnitPriceUSD != nil {
		t.Fatalf("cache-unknown model price = %+v", static)
	}
	static = snapshot.Items[1]
	if static.ModelName != "gpt-5.6-sol" || static.InputUnitPriceUSD != 5 || static.OutputUnitPriceUSD != 30 ||
		static.CacheReadUnitPriceUSD == nil || *static.CacheReadUnitPriceUSD != 0.5 ||
		static.CacheWriteUnitPriceUSD == nil || *static.CacheWriteUnitPriceUSD != 6.25 {
		t.Fatalf("static model price = %+v", static)
	}
	tiered := snapshot.Items[2]
	if tiered.ModelName != "gpt-tiered" || tiered.LongContextBasis != "whole_request" ||
		tiered.LongContextThresholdTokens == nil || *tiered.LongContextThresholdTokens != 272000 ||
		tiered.InputUnitPriceUSD != 2.5 || tiered.OutputUnitPriceUSD != 15 ||
		tiered.LongContextInputUnitPriceUSD == nil || *tiered.LongContextInputUnitPriceUSD != 5 ||
		tiered.LongContextOutputUnitPriceUSD == nil || *tiered.LongContextOutputUnitPriceUSD != 22.5 {
		t.Fatalf("tiered model price = %+v", tiered)
	}
}

func TestNormalizeSub2APIModelPriceRejectsUnreachableLongContextThreshold(t *testing.T) {
	inputPrice, outputPrice := 0.000001, 0.000002
	group := sub2APIModelPlazaGroup{ID: 1, Name: "default", LongContextPricingEnabled: true}
	for _, threshold := range []int64{-1, 0, math.MaxInt64} {
		t.Run(strconv.FormatInt(threshold, 10), func(t *testing.T) {
			model := sub2APIModelPlazaModel{
				Name: "invalid-threshold", LongContextBasis: sub2APIModelPriceLongContextBasis,
				Pricing: &sub2APIModelPlazaPricing{
					BillingMode: "token", InputPrice: &inputPrice, OutputPrice: &outputPrice,
					Intervals: []sub2APIModelPlazaPriceInterval{
						{MinTokens: 0, MaxTokens: &threshold, InputPrice: &inputPrice, OutputPrice: &outputPrice},
						{MinTokens: threshold, InputPrice: &inputPrice, OutputPrice: &outputPrice},
					},
				},
			}
			if item, ok, err := normalizeSub2APIModelPrice(group, model, 1); err != nil || ok {
				t.Fatalf("normalize threshold %d = %+v, ok=%t, err=%v; want unsupported tier", threshold, item, ok, err)
			}
		})
	}
}

func TestModelPricesDoNotCollapseDifferentCacheWriteTTLs(t *testing.T) {
	for _, test := range []struct {
		name  string
		hour  *float64
		known bool
	}{
		{"not provided", nil, true},
		{"same price", modelPriceFloatPointer(0.00000125), true},
		{"different TTL price", modelPriceFloatPointer(0.000002), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			group := sub2APIModelPlazaGroup{ID: 1, Name: "default", LongContextPricingEnabled: true}
			model := sub2APIModelPlazaModel{Name: "claude-test", Pricing: &sub2APIModelPlazaPricing{
				BillingMode: "token", InputPrice: modelPriceFloatPointer(0.000001), OutputPrice: modelPriceFloatPointer(0.000005),
				CacheWritePrice: modelPriceFloatPointer(0.00000125), CacheWrite1hPrice: test.hour,
			}}
			item, ok, err := normalizeSub2APIModelPrice(group, model, 1)
			if err != nil || !ok || (item.CacheWriteUnitPriceUSD != nil) != test.known {
				t.Fatalf("cache TTL pricing=%+v ok=%v error=%v", item, ok, err)
			}
			threshold := int64(200000)
			model.LongContextBasis = "whole_request"
			model.Pricing.Intervals = []sub2APIModelPlazaPriceInterval{
				{MinTokens: 0, MaxTokens: &threshold, InputPrice: model.Pricing.InputPrice, OutputPrice: model.Pricing.OutputPrice, CacheWritePrice: model.Pricing.CacheWritePrice, CacheWrite1hPrice: test.hour},
				{MinTokens: threshold, InputPrice: model.Pricing.InputPrice, OutputPrice: model.Pricing.OutputPrice, CacheWritePrice: model.Pricing.CacheWritePrice, CacheWrite1hPrice: test.hour},
			}
			item, ok, err = normalizeSub2APIModelPrice(group, model, 1)
			if err != nil || !ok || (item.CacheWriteUnitPriceUSD != nil) != test.known || (item.LongContextCacheWriteUnitPriceUSD != nil) != test.known {
				t.Fatalf("tiered cache TTL pricing=%+v ok=%v error=%v", item, ok, err)
			}
		})
	}
}

func TestGetModelPricesRetiresStaticPricesForDynamicPricing(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/model-plaza", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"groups":[
			{"id":1,"name":"peak","rate_multiplier":1,"peak_rate_enabled":true,"models":[
				{"name":"peak-model","pricing":{"billing_mode":"token","input_price":0.000001,"output_price":0.000002,"cache_write_price":0.000001,"cache_read_price":0.0000001,"intervals":[]}}
			]},
			{"id":2,"name":"dynamic","rate_multiplier":1,"peak_rate_enabled":false,"long_context_pricing_enabled":true,"models":[
				{"name":"time-model","pricing":{"billing_mode":"token","input_price":0.000001,"output_price":0.000002,"cache_write_price":0.000001,"cache_read_price":0.0000001,"intervals":[]},"time_pricing":{"timezone":"UTC","periods":[{"start_time":"00:00","end_time":"08:00","multiplier":0.5}]}},
				{"name":"marginal-model","pricing":{"billing_mode":"token","input_price":0.000001,"output_price":0.000002,"cache_write_price":0.000001,"cache_read_price":0.0000001,"intervals":[{"min_tokens":0,"max_tokens":100,"input_price":0.000001,"output_price":0.000002},{"min_tokens":100,"max_tokens":null,"input_price":0.000003,"output_price":0.000004}]},"long_context_basis":"marginal"},
				{"name":"request-model","pricing":{"billing_mode":"per_request","input_price":null,"output_price":null,"cache_write_price":null,"cache_read_price":null,"intervals":[]}},
				{"name":"official-only","pricing":null,"official_pricing":{"input_price":0.000001,"output_price":0.000002}}
			]}
		]}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if err != nil || snapshot == nil || !snapshot.Complete || len(snapshot.Items) != 0 || len(snapshot.SourceChecksum) != 64 {
		t.Fatalf("GetModelPrices = %+v, %v; want complete empty replacement", snapshot, err)
	}
}

func TestGetModelPricesTreatsUnavailableModelPlazaAsUnsupported(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "model plaza unavailable", status)
			}))
			defer srv.Close()

			snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
			if snapshot != nil || !errors.Is(err, connector.ErrUnsupported) {
				t.Fatalf("GetModelPrices = %+v, %v; want ErrUnsupported", snapshot, err)
			}
		})
	}
}

func TestGetModelPricesPreservesAuthenticationFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "expired", http.StatusUnauthorized)
	}))
	defer srv.Close()

	snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if snapshot != nil || err == nil || errors.Is(err, connector.ErrUnsupported) || !connector.IsAuthHTTPError(err) {
		t.Fatalf("GetModelPrices = %+v, %v; want authentication error", snapshot, err)
	}
}

func TestGetModelPricesRejectsRedirectAndOversizedResponse(t *testing.T) {
	t.Run("redirect", func(t *testing.T) {
		var redirected atomic.Int32
		mux := http.NewServeMux()
		mux.HandleFunc("/api/v1/model-plaza", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/prices", http.StatusFound)
		})
		mux.HandleFunc("/prices", func(w http.ResponseWriter, _ *http.Request) {
			redirected.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"data":{"groups":[]}}`))
		})
		srv := httptest.NewServer(mux)
		defer srv.Close()

		snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
		if snapshot != nil || err == nil || redirected.Load() != 0 {
			t.Fatalf("GetModelPrices = %+v, %v; redirected=%d", snapshot, err, redirected.Load())
		}
	})

	t.Run("content length", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.FormatInt(sub2APIModelPlazaMaxBytes+1, 10))
			_, _ = w.Write([]byte("x"))
		}))
		defer srv.Close()

		snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
		if snapshot != nil || err == nil || !strings.Contains(err.Error(), "size limit") {
			t.Fatalf("GetModelPrices = %+v, %v; want size rejection", snapshot, err)
		}
	})
}

func TestGetModelPricesRejectsInvalidSnapshot(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "missing groups", data: `{}`},
		{name: "duplicate group id", data: `{"groups":[{"id":1,"name":"a","rate_multiplier":1,"models":[]},{"id":1,"name":"b","rate_multiplier":1,"models":[]}]}`},
		{name: "duplicate canonical group name", data: `{"groups":[{"id":1,"name":"A","rate_multiplier":1,"models":[]},{"id":2,"name":"a","rate_multiplier":1,"models":[]}]}`},
		{name: "negative user multiplier", data: `{"groups":[{"id":1,"name":"a","rate_multiplier":1,"user_rate_multiplier":-1,"models":[]}]}`},
		{name: "duplicate model", data: `{"groups":[{"id":1,"name":"a","rate_multiplier":1,"models":[{"name":"m","pricing":null},{"name":"m","pricing":null}]}]}`},
		{name: "negative price", data: `{"groups":[{"id":1,"name":"a","rate_multiplier":1,"models":[{"name":"m","pricing":{"billing_mode":"token","input_price":-1,"output_price":1,"intervals":[]}}]}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":%s}`, test.data)
			}))
			defer srv.Close()
			snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
			if err == nil || snapshot != nil {
				t.Fatalf("GetModelPrices = %+v, %v; want invalid snapshot rejection", snapshot, err)
			}
		})
	}
}

func TestGetModelPricesRequiresAuthenticatedSession(t *testing.T) {
	client := New()
	for _, session := range []*connector.AuthSession{nil, {}} {
		if snapshot, err := client.GetModelPrices(context.Background(), &connector.Channel{SiteURL: "https://example.invalid"}, session); err == nil || snapshot != nil {
			t.Fatalf("GetModelPrices = %+v, %v; want session rejection", snapshot, err)
		}
	}
}

func strPtr(v string) *string {
	return &v
}

func TestLookupRequestCostRequiresExplicitActualCost(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   any
		present bool
		wantErr bool
		want    float64
	}{
		{name: "missing", wantErr: true},
		{name: "null", present: true, wantErr: true},
		{name: "explicit zero", value: 0, present: true},
		{name: "actual amount", value: 0.01234567, present: true, want: 0.01234567},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := map[string]any{"id": 7, "api_key_id": 9, "request_id": "client:cost-presence", "model": "gpt-test", "input_tokens": 10, "output_tokens": 2, "created_at": time.Now().UTC().Format(time.RFC3339Nano)}
			if tc.present {
				item["actual_cost"] = tc.value
			}
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []map[string]any{item}, "total": 1, "page": 1, "page_size": 1000, "pages": 1}})
			}))
			defer srv.Close()
			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "fixture-token"}, connector.RequestCostQuery{APIKeyID: 9, RequestID: "cost-presence"})
			if tc.wantErr {
				if err == nil || result != nil || !strings.Contains(err.Error(), "actual cost evidence unavailable") {
					t.Fatalf("missing evidence result=%+v error=%v", result, err)
				}
				return
			}
			if err != nil || result == nil || result.Cost != tc.want {
				t.Fatalf("explicit amount result=%+v error=%v", result, err)
			}
			if calls.Load() < 2 {
				t.Fatal("expected final first-page snapshot to be decoded and compared again")
			}
		})
	}
}
