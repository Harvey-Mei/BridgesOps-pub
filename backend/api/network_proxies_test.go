package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestNetworkProxyAPICreatesWithoutLeakingCredentialsAndBlocksDeleteWhenBound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	cipher, err := crypto.NewCipher("test-network-proxy-secret")
	if err != nil {
		t.Fatal(err)
	}
	proxies := storage.NewNetworkProxies(db)
	channels := storage.NewChannels(db)
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{Cipher: cipher, NetworkProxies: proxies})

	create := httptest.NewRequest(http.MethodPost, "/api/network-proxies", strings.NewReader(`{"name":"office","protocol":"socks5","host":"127.0.0.1","port":1080,"username":"user","password":"should-not-leak","enabled":true}`))
	create.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, create)
	if response.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "should-not-leak") || strings.Contains(response.Body.String(), "\"user\"") {
		t.Fatalf("credential leaked in response: %s", response.Body.String())
	}
	var created struct {
		Data struct {
			ID             uint `json:"id"`
			AuthConfigured bool `json:"auth_configured"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Data.ID == 0 || !created.Data.AuthConfigured {
		t.Fatalf("created = %#v", created)
	}

	if err := channels.Create(&storage.Channel{
		Name:           "bound",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "cipher",
		NetworkProxyID: &created.Data.ID,
	}); err != nil {
		t.Fatal(err)
	}
	deleteResponse := httptest.NewRecorder()
	router.ServeHTTP(deleteResponse, httptest.NewRequest(http.MethodDelete, "/api/network-proxies/"+strconv.Itoa(int(created.Data.ID)), nil))
	if deleteResponse.Code != http.StatusConflict {
		t.Fatalf("delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	if !strings.Contains(deleteResponse.Body.String(), "仍被渠道使用") {
		t.Fatalf("delete response lost in-use semantics: %s", deleteResponse.Body.String())
	}
}

func TestNetworkProxyAPIImportsOnlyDirectEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	cipher, err := crypto.NewCipher("test-network-proxy-secret")
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{Cipher: cipher, NetworkProxies: storage.NewNetworkProxies(db)})
	body := bytes.NewBufferString(`{"format":"uri","content":"http://user:secret@proxy.example:8080\nvless://do-not-return-this"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/network-proxies/import", body)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("import status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "do-not-return-this") {
		t.Fatalf("import response leaked sensitive content: %s", response.Body.String())
	}
	var decoded struct {
		Data struct {
			Created []struct {
				Protocol string `json:"protocol"`
			} `json:"created"`
			Skipped []struct {
				Reason string `json:"reason"`
			} `json:"skipped"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Data.Created) != 1 || decoded.Data.Created[0].Protocol != "http" || len(decoded.Data.Skipped) != 1 {
		t.Fatalf("import result = %#v", decoded.Data)
	}
}

func TestNetworkProxyAPIRejectsNodeLinkWithoutPersistingIt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	proxies := storage.NewNetworkProxies(db)
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{NetworkProxies: proxies})
	request := httptest.NewRequest(http.MethodPost, "/api/network-proxies/import", strings.NewReader(`{"format":"uri","content":"vless://opaque-node@example.invalid"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("import status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "opaque-node") {
		t.Fatalf("node link leaked in response: %s", response.Body.String())
	}
	var decoded struct {
		Data struct {
			Created []any `json:"created"`
			Skipped []struct {
				Entry  string `json:"entry"`
				Reason string `json:"reason"`
			} `json:"skipped"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Data.Created) != 0 || len(decoded.Data.Skipped) != 1 || decoded.Data.Skipped[0].Entry != "第 1 项" || !strings.Contains(decoded.Data.Skipped[0].Reason, "HTTP/SOCKS") {
		t.Fatalf("import result = %#v", decoded.Data)
	}
	var count int64
	if err := db.Model(&storage.NetworkProxy{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("node link persisted count=%d err=%v", count, err)
	}
}

func TestNetworkProxyAPIClashImportNeverLeaksSensitiveNames(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	cipher, err := crypto.NewCipher("test-network-proxy-secret")
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{Cipher: cipher, NetworkProxies: storage.NewNetworkProxies(db)})
	payload, err := json.Marshal(map[string]any{
		"format": "clash",
		"content": `proxies:
  - name: "socks5://user:clash-password@name.example:1080?token=clash-token"
    type: socks5
    server: safe.example
    port: 1080
  - name: "password=skip-secret"
    type: ss
    server: ignored.example
    port: 443`,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/network-proxies/import", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("import status=%d body=%s", response.Code, response.Body.String())
	}
	for _, secret := range []string{"clash-password", "clash-token", "skip-secret", "user@"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("import response leaked %q: %s", secret, response.Body.String())
		}
	}
	var decoded struct {
		Data struct {
			Created []struct {
				Name string `json:"name"`
			} `json:"created"`
			Skipped []struct {
				Entry string `json:"entry"`
			} `json:"skipped"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Data.Created) != 1 || decoded.Data.Created[0].Name != "safe.example:1080" {
		t.Fatalf("created = %#v", decoded.Data.Created)
	}
	if len(decoded.Data.Skipped) != 1 || decoded.Data.Skipped[0].Entry != "第 2 项" {
		t.Fatalf("skipped = %#v", decoded.Data.Skipped)
	}
	var stored storage.NetworkProxy
	if err := db.First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Name != "safe.example:1080" || stored.Source != "clash" {
		t.Fatalf("stored proxy = %#v", stored)
	}
	for _, secret := range []string{"clash-password", "clash-token", "skip-secret", "user@", "name.example", "?", "@"} {
		if strings.Contains(stored.Name, secret) {
			t.Fatalf("stored name leaked %q: %q", secret, stored.Name)
		}
	}
}

func TestNetworkProxyAPIClashNodeImportSkipsWithoutPersistence(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	proxies := storage.NewNetworkProxies(db)
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{NetworkProxies: proxies})
	payload, err := json.Marshal(map[string]any{
		"format": "clash",
		"content": `proxies:
  - name: "token=clash-node-secret"
    type: vless
    server: node.safe.example
    port: 443
    uuid: 123e4567-e89b-12d3-a456-426614174000
    network: tcp
    security: none
  - name: "regional token private-value"
    type: vless
    server: invalid.safe.example
    port: 443
    uuid: invalid`,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/network-proxies/import", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("import status=%d body=%s", response.Code, response.Body.String())
	}
	for _, secret := range []string{"clash-node-secret", "token=", "private-value"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("node import response leaked %q: %s", secret, response.Body.String())
		}
	}
	var decoded struct {
		Data struct {
			Created []struct {
				Name string `json:"name"`
			} `json:"created"`
			Skipped []struct {
				Entry string `json:"entry"`
			} `json:"skipped"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Data.Created) != 0 {
		t.Fatalf("created = %#v", decoded.Data.Created)
	}
	if len(decoded.Data.Skipped) != 2 || decoded.Data.Skipped[0].Entry != "第 1 项" || decoded.Data.Skipped[1].Entry != "第 2 项" {
		t.Fatalf("skipped = %#v", decoded.Data.Skipped)
	}
	var count int64
	if err := db.Model(&storage.NetworkProxy{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("node configuration persisted count=%d err=%v", count, err)
	}
}

func TestNetworkProxyAPIOutputNormalizesLegacyClashName(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	proxies := storage.NewNetworkProxies(db)
	if err := db.Create(&storage.NetworkProxy{
		Name:     "token=private-value",
		Protocol: "http",
		Host:     "safe.example",
		Port:     8080,
		Source:   "clash",
		Enabled:  true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{NetworkProxies: proxies})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/network-proxies", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	for _, secret := range []string{"token=", "private-value", "user@"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("legacy list leaked %q: %s", secret, response.Body.String())
		}
	}
	if !strings.Contains(response.Body.String(), "safe.example:8080") {
		t.Fatalf("legacy Clash name was not normalized: %s", response.Body.String())
	}
}

func TestNetworkProxyAPIHidesMalformedLegacyEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	proxies := storage.NewNetworkProxies(db)
	if err := db.Create(&storage.NetworkProxy{
		Name:     "legacy-malformed",
		Protocol: "http",
		Host:     "http://user:legacy-password@proxy.example?token=legacy-token",
		Port:     8080,
		Enabled:  true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{NetworkProxies: proxies})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/network-proxies", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	for _, secret := range []string{"legacy-password", "legacy-token", "user@", "proxy.example"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("malformed endpoint leaked %q: %s", secret, response.Body.String())
		}
	}
	if !strings.Contains(response.Body.String(), "无效代理") {
		t.Fatalf("malformed endpoint did not receive a safe label: %s", response.Body.String())
	}
}

func TestNetworkProxyAPIRejectsOversizedManualInput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	cipher, err := crypto.NewCipher("test-network-proxy-secret")
	if err != nil {
		t.Fatal(err)
	}
	proxies := storage.NewNetworkProxies(db)
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{Cipher: cipher, NetworkProxies: proxies})

	payload := func(overrides map[string]any) []byte {
		body := map[string]any{
			"name":     "bounded",
			"protocol": "http",
			"host":     "proxy.example",
			"port":     8080,
			"username": "user",
			"password": "password",
			"enabled":  true,
		}
		for key, value := range overrides {
			body[key] = value
		}
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	request := func(method, target string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}

	for _, tt := range []struct {
		name  string
		field string
		value string
	}{
		{name: "name", field: "name", value: strings.Repeat("n", netproxy.MaxNetworkProxyNameBytes+1)},
		{name: "host", field: "host", value: strings.Repeat("h", netproxy.MaxNetworkProxyHostBytes+1)},
		{name: "username", field: "username", value: strings.Repeat("u", netproxy.MaxNetworkProxyUsernameBytes+1)},
		{name: "password", field: "password", value: strings.Repeat("p", netproxy.MaxNetworkProxyPasswordBytes+1)},
	} {
		t.Run("create_"+tt.name, func(t *testing.T) {
			response := request(http.MethodPost, "/api/network-proxies", payload(map[string]any{tt.field: tt.value}))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}

	overstated := request(http.MethodPost, "/api/network-proxies", payload(map[string]any{
		"padding": strings.Repeat("x", networkProxyRequestLimit),
	}))
	if overstated.Code != http.StatusBadRequest {
		t.Fatalf("oversized body status=%d body=%s", overstated.Code, overstated.Body.String())
	}
	list, err := proxies.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("invalid create persisted proxies: %#v", list)
	}

	created := request(http.MethodPost, "/api/network-proxies", payload(nil))
	if created.Code != http.StatusOK {
		t.Fatalf("valid create status=%d body=%s", created.Code, created.Body.String())
	}
	var decoded struct {
		Data struct {
			ID uint `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Data.ID == 0 {
		t.Fatalf("created = %#v", decoded)
	}
	overstatedUpdate := request(http.MethodPut, "/api/network-proxies/"+strconv.Itoa(int(decoded.Data.ID)), payload(map[string]any{
		"name":    "must-not-persist",
		"padding": strings.Repeat("x", networkProxyRequestLimit),
	}))
	if overstatedUpdate.Code != http.StatusBadRequest {
		t.Fatalf("oversized update body status=%d body=%s", overstatedUpdate.Code, overstatedUpdate.Body.String())
	}

	for _, tt := range []struct {
		name  string
		field string
		value string
	}{
		{name: "name", field: "name", value: strings.Repeat("n", netproxy.MaxNetworkProxyNameBytes+1)},
		{name: "host", field: "host", value: strings.Repeat("h", netproxy.MaxNetworkProxyHostBytes+1)},
		{name: "username", field: "username", value: strings.Repeat("u", netproxy.MaxNetworkProxyUsernameBytes+1)},
		{name: "password", field: "password", value: strings.Repeat("p", netproxy.MaxNetworkProxyPasswordBytes+1)},
	} {
		t.Run("update_"+tt.name, func(t *testing.T) {
			response := request(http.MethodPut, "/api/network-proxies/"+strconv.Itoa(int(decoded.Data.ID)), payload(map[string]any{tt.field: tt.value}))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("update status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	stored, err := proxies.FindByID(decoded.Data.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != "bounded" || stored.Host != "proxy.example" {
		t.Fatalf("invalid update changed proxy: %#v", stored)
	}
}

func TestNetworkProxyAPIRejectsSensitiveManualNames(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	cipher, err := crypto.NewCipher("test-network-proxy-secret")
	if err != nil {
		t.Fatal(err)
	}
	proxies := storage.NewNetworkProxies(db)
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{Cipher: cipher, NetworkProxies: proxies})
	payload := func(name string) []byte {
		body, err := json.Marshal(map[string]any{
			"name":     name,
			"protocol": "http",
			"host":     "proxy.example",
			"port":     8080,
		})
		if err != nil {
			t.Fatal(err)
		}
		return body
	}

	create := httptest.NewRequest(http.MethodPost, "/api/network-proxies", bytes.NewReader(payload("password=manual-secret")))
	create.Header.Set("Content-Type", "application/json")
	createResponse := httptest.NewRecorder()
	router.ServeHTTP(createResponse, create)
	if createResponse.Code != http.StatusBadRequest || strings.Contains(createResponse.Body.String(), "manual-secret") {
		t.Fatal("sensitive manual name was not rejected safely")
	}
	var count int64
	if err := db.Model(&storage.NetworkProxy{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("rejected manual proxy name was persisted")
	}

	profile := &storage.NetworkProxy{Name: "manual-safe", Protocol: "http", Host: "proxy.example", Port: 8080, Enabled: true}
	if err := proxies.Create(profile); err != nil {
		t.Fatal(err)
	}
	update := httptest.NewRequest(http.MethodPut, "/api/network-proxies/"+strconv.Itoa(int(profile.ID)), bytes.NewReader(payload("socks5://user:manual-secret@name.example:1080?token=manual-token")))
	update.Header.Set("Content-Type", "application/json")
	updateResponse := httptest.NewRecorder()
	router.ServeHTTP(updateResponse, update)
	if updateResponse.Code != http.StatusBadRequest || strings.Contains(updateResponse.Body.String(), "manual-secret") || strings.Contains(updateResponse.Body.String(), "manual-token") {
		t.Fatal("sensitive manual name update was not rejected safely")
	}
	stored, err := proxies.FindByID(profile.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != "manual-safe" {
		t.Fatal("rejected manual proxy update changed the stored name")
	}
}

func TestNetworkProxyAPIOutputRedactsLegacyDiagnosticSecrets(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	proxies := storage.NewNetworkProxies(db)
	profile := &storage.NetworkProxy{
		Name:      "legacy-error",
		Protocol:  "http",
		Host:      "proxy.example",
		Port:      8080,
		Enabled:   true,
		LastError: "dial socks5://user:secret@proxy.example:1080?token=query-secret; Authorization: Bearer bearer-secret",
	}
	if err := proxies.Create(profile); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerNetworkProxies(router.Group("/api"), &Deps{NetworkProxies: proxies})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/network-proxies", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body.String())
	}
	for _, secret := range []string{"user:secret", "query-secret", "bearer-secret"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatalf("list leaked %q: %s", secret, response.Body.String())
		}
	}
}

func TestChannelUpdateRejectsEnabledProxyWithoutNetworkProxyBinding(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerChannels(router.Group("/api"), &Deps{})

	request := httptest.NewRequest(http.MethodPut, "/api/channels/1", strings.NewReader(`{"proxy_enabled":true,"network_proxy_id":0}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("update status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "渠道代理必须选择一个网络代理") {
		t.Fatalf("unexpected response: %s", response.Body.String())
	}
}
