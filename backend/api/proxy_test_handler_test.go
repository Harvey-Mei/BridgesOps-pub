package api

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/gin-gonic/gin"
)

func TestProxyTestUsesStoredPasswordWhenRequestLeavesItBlank(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ip":"203.0.113.10"}`))
	}))
	defer upstream.Close()

	const username = "proxy-user"
	const password = "stored-proxy-password"
	expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	authorized := false
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != expectedAuth {
			w.Header().Set("Proxy-Authenticate", `Basic realm="proxy"`)
			http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
			return
		}
		authorized = true
		resp, err := http.Get(r.URL.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()

	oldProviders := proxyIPProviders
	proxyIPProviders = []proxyIPProvider{{
		name: "stored-password",
		url:  upstream.URL,
		parseIP: func(body []byte) string {
			var out struct {
				IP string `json:"ip"`
			}
			_ = json.Unmarshal(body, &out)
			return out.IP
		},
	}}
	t.Cleanup(func() { proxyIPProviders = oldProviders })

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := config.Save(path, &config.Config{
		Gateway: config.GatewayConfig{RequestAdmission: config.DefaultGatewayRequestAdmissionConfig()},
		Proxy: config.ProxyConfig{
			Protocol: "http",
			Host:     proxyURL.Hostname(),
			Port:     mustPort(t, proxyURL.Port()),
			Username: username,
			Password: password,
		},
	}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	r := gin.New()
	registerSettings(r.Group("/api"), &Deps{
		Runtime: runtimeconfig.New(path, "", nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil),
	})
	body := `{"protocol":"http","host":"` + proxyURL.Hostname() + `","port":` + proxyURL.Port() + `,"username":"` + username + `","password":""}`
	req := httptest.NewRequest(http.MethodPost, "/api/settings/proxy/test", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var got struct {
		Data proxyTestResult `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !authorized || !got.Data.OK || got.Data.IP != "203.0.113.10" {
		t.Fatalf("authorized = %t, result = %#v", authorized, got.Data)
	}
}

func mustPort(t *testing.T, value string) int {
	t.Helper()
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("parse port %q: %v", value, err)
	}
	return parsed
}

func TestProxyTestFallsBackToSecondProvider(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ipify":
			http.Error(w, "down", http.StatusBadGateway)
		case "/ipinfo":
			_, _ = w.Write([]byte(`{"ip":"203.0.113.9"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	proxyHits := 0
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits++
		target := r.URL.String()
		if !strings.HasPrefix(target, "http") {
			target = upstream.URL + r.URL.Path
		}
		resp, err := http.Get(target)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()

	oldProviders := proxyIPProviders
	proxyIPProviders = []proxyIPProvider{
		{
			name: "ipify",
			url:  upstream.URL + "/ipify",
			parseIP: func(body []byte) string {
				var out struct {
					IP string `json:"ip"`
				}
				_ = json.Unmarshal(body, &out)
				return out.IP
			},
		},
		{
			name: "ipinfo",
			url:  upstream.URL + "/ipinfo",
			parseIP: func(body []byte) string {
				var out struct {
					IP string `json:"ip"`
				}
				_ = json.Unmarshal(body, &out)
				return out.IP
			},
		},
	}
	t.Cleanup(func() { proxyIPProviders = oldProviders })

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}
	r := gin.New()
	apiGroup := r.Group("/api")
	registerSettings(apiGroup, &Deps{})

	body := `{"protocol":"http","host":"` + proxyURL.Hostname() + `","port":` + proxyURL.Port() + `}`
	req := httptest.NewRequest(http.MethodPost, "/api/settings/proxy/test", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}

	var got struct {
		Data proxyTestResult `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.Data.OK || got.Data.Provider != "ipinfo" || got.Data.IP != "203.0.113.9" {
		t.Fatalf("result = %#v", got.Data)
	}
	if proxyHits != 2 {
		t.Fatalf("proxy hits = %d, want 2", proxyHits)
	}
}
