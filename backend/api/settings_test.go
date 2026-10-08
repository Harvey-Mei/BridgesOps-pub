package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/Harvey-Mei/BridgeOps/backend/scheduler"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"github.com/gin-gonic/gin"
)

func TestSettingsSecretsAreRedactedAndMerged(t *testing.T) {
	gin.SetMode(gin.TestMode)

	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := &config.Config{
		App:     config.AppConfig{TimeZone: "UTC"},
		Gateway: config.GatewayConfig{RequestAdmission: config.DefaultGatewayRequestAdmissionConfig()},
		Auth: config.AuthConfig{
			Enabled:         true,
			Username:        "admin",
			Password:        "existing-admin-password",
			TokenSecret:     "existing-token-secret",
			SessionTTLHours: 168,
		},
		Proxy: config.ProxyConfig{
			Enabled:  true,
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8080,
			Username: "proxy-user",
			Password: "existing-proxy-password",
		},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	effectiveTime, err := usertime.New("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	registerSettings(api, &Deps{
		Runtime:  runtimeconfig.New(path, "", nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil),
		UserTime: effectiveTime,
	})

	getReq := httptest.NewRequest(http.MethodGet, "/api/settings/config", nil)
	getRec := httptest.NewRecorder()
	r.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", getRec.Code, getRec.Body.String())
	}
	for _, secret := range []string{cfg.Auth.Password, cfg.Auth.TokenSecret, cfg.Proxy.Password} {
		if strings.Contains(getRec.Body.String(), secret) {
			t.Fatalf("settings response exposed secret %q", secret)
		}
	}
	var response struct {
		Data struct {
			EffectiveTimeZone string             `json:"effective_time_zone"`
			Config            settingsConfigView `json:"config"`
		} `json:"data"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode settings response: %v", err)
	}
	view := response.Data.Config
	if response.Data.EffectiveTimeZone != "Asia/Shanghai" || view.App.TimeZone != "UTC" {
		t.Fatalf("effective/file timezones = %q / %q", response.Data.EffectiveTimeZone, view.App.TimeZone)
	}
	if view.Auth.Password != "" || view.Auth.TokenSecret != "" || view.Proxy.Password != "" {
		t.Fatalf("redacted values = auth password %q token %q proxy password %q", view.Auth.Password, view.Auth.TokenSecret, view.Proxy.Password)
	}
	if !view.Auth.PasswordConfigured || !view.Auth.TokenSecretConfigured || !view.Proxy.PasswordConfigured {
		t.Fatalf("configured flags = auth password %t token %t proxy password %t", view.Auth.PasswordConfigured, view.Auth.TokenSecretConfigured, view.Proxy.PasswordConfigured)
	}

	input := settingsConfigInput{
		App: cfg.App,
		Auth: settingsAuthConfigInput{
			Enabled:         cfg.Auth.Enabled,
			Username:        cfg.Auth.Username,
			SessionTTLHours: cfg.Auth.SessionTTLHours,
		},
		Scheduler:     cfg.Scheduler,
		Notifications: cfg.Notifications,
		Proxy: settingsProxyConfigInput{
			Enabled:  cfg.Proxy.Enabled,
			Protocol: cfg.Proxy.Protocol,
			Host:     cfg.Proxy.Host,
			Port:     cfg.Proxy.Port,
			Username: cfg.Proxy.Username,
		},
		Upstream: cfg.Upstream,
	}
	put := func(value settingsConfigInput) {
		t.Helper()
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("encode settings input: %v", err)
		}
		req := httptest.NewRequest(http.MethodPut, "/api/settings/config", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("put status = %d, body = %s", rec.Code, rec.Body.String())
		}
	}

	put(input)
	preserved, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load preserved config: %v", err)
	}
	if preserved.Auth.Password != cfg.Auth.Password || preserved.Auth.TokenSecret != cfg.Auth.TokenSecret || preserved.Proxy.Password != cfg.Proxy.Password {
		t.Fatalf("blank secret update did not preserve values")
	}

	input.Auth.Password = "replacement-admin-password"
	input.Auth.TokenSecret = "replacement-token-secret"
	input.Proxy.Password = "replacement-proxy-password"
	put(input)
	replaced, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load replaced config: %v", err)
	}
	if replaced.Auth.Password != input.Auth.Password || replaced.Auth.TokenSecret != input.Auth.TokenSecret || replaced.Proxy.Password != input.Proxy.Password {
		t.Fatalf("replacement secrets were not saved")
	}

	input.Auth.Password = ""
	input.Auth.TokenSecret = ""
	input.Auth.ClearTokenSecret = true
	input.Proxy.Password = ""
	input.Proxy.ClearPassword = true
	put(input)
	cleared, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load cleared config: %v", err)
	}
	if cleared.Auth.Password != "replacement-admin-password" || cleared.Auth.TokenSecret != "" || cleared.Proxy.Password != "" {
		t.Fatalf("clear semantics = auth password %q token %q proxy password %q", cleared.Auth.Password, cleared.Auth.TokenSecret, cleared.Proxy.Password)
	}
}

func TestSaveSettingsKeepsAppVersion(t *testing.T) {
	gin.SetMode(gin.TestMode)

	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := &config.Config{
		App: config.AppConfig{
			Title:              "Old",
			NotificationPrefix: "[Old] ",
			TimeZone:           "UTC",
		},
		Gateway: config.GatewayConfig{RequestAdmission: config.DefaultGatewayRequestAdmissionConfig()},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerSettings(api, &Deps{
		Runtime: runtimeconfig.New(path, "", nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil),
	})

	body := `{
		"app":{"title":"New","notificationPrefix":"[New] ","timeZone":"Asia/Shanghai"},
		"auth":{"enabled":false,"username":"admin","password":"","tokenSecret":"","sessionTTLHours":168},
		"scheduler":{"balanceCron":"37 */15 * * * *","rateCron":"13 * * * * *","rateRefreshIntervalMinutes":1,"concurrency":4,"retention":{"cron":"0 17 3 * * *","monitorLogsDays":30,"balanceSnapshotsDays":90,"notificationLogsDays":90,"announcementsDays":90}},
		"notifications":{"batchRateChanges":true,"minChangePct":0,"balanceLowCooldownMinutes":60,"subscriptionDailyRemainingThresholdPct":0,"subscriptionWeeklyRemainingThresholdPct":0,"subscriptionMonthlyRemainingThresholdPct":0,"subscriptionExpiryThresholdHours":0,"subscriptionAlertCooldownMinutes":1440,"sendMaxAttempts":3},
		"proxy":{"enabled":true,"versionCheckEnabled":true,"protocol":"socks5","host":"127.0.0.1","port":1080,"username":"u","password":"p"},
		"upstream":{"timeoutSeconds":45,"userAgent":"custom-agent"}
	}`
	req := httptest.NewRequest(http.MethodPut, "/api/settings/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	got, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got.App.Title != "New" {
		t.Fatalf("title = %q", got.App.Title)
	}
	if got.App.NotificationPrefix != "[New] " {
		t.Fatalf("notification prefix = %q", got.App.NotificationPrefix)
	}
	if got.App.TimeZone != "Asia/Shanghai" {
		t.Fatalf("time zone = %q", got.App.TimeZone)
	}
	if !got.Proxy.Enabled || !got.Proxy.VersionCheckEnabled || got.Proxy.Protocol != "socks5" || got.Proxy.Host != "127.0.0.1" || got.Proxy.Port != 1080 || got.Proxy.Username != "u" || got.Proxy.Password != "p" {
		t.Fatalf("proxy = %#v", got.Proxy)
	}
	if got.Upstream.TimeoutSeconds != 45 || got.Upstream.UserAgent != "custom-agent" {
		t.Fatalf("upstream = %#v", got.Upstream)
	}

	invalidBody := strings.Replace(body, "Asia/Shanghai", "Mars/Olympus", 1)
	invalidReq := httptest.NewRequest(http.MethodPut, "/api/settings/config", strings.NewReader(invalidBody))
	invalidReq.Header.Set("Content-Type", "application/json")
	invalidRec := httptest.NewRecorder()
	r.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid time zone status = %d, body = %s", invalidRec.Code, invalidRec.Body.String())
	}
}

func TestApplySettingsConfigReportsStartupAdmissionRestartRequired(t *testing.T) {
	gin.SetMode(gin.TestMode)

	path := filepath.Join(t.TempDir(), "config.yaml")
	startupAdmission := config.DefaultGatewayRequestAdmissionConfig()
	changedAdmission := startupAdmission
	changedAdmission.MaxQueueWaitMilliseconds = startupAdmission.MaxQueueWaitMilliseconds - 1
	cfg := &config.Config{
		App:     config.AppConfig{TimeZone: "UTC"},
		Gateway: config.GatewayConfig{RequestAdmission: changedAdmission},
		Scheduler: config.SchedulerConfig{
			BalanceCron: "",
			RateCron:    "",
			Retention:   config.RetentionConfig{Cron: ""},
		},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	factoryCalls := 0
	mgr := runtimeconfig.New(
		path,
		"",
		nil,
		nil,
		nil,
		nil,
		nil,
		config.ProxyConfig{},
		config.UpstreamConfig{},
		func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
			factoryCalls++
			return scheduler.New(scfg, nil, nil, nil, nil, nil, nil, nil, nil, pcfg, nil)
		},
	)
	if err := mgr.SetStartupGatewayRequestAdmission(startupAdmission); err != nil {
		t.Fatalf("set startup admission: %v", err)
	}
	router := gin.New()
	registerSettings(router.Group("/api"), &Deps{Runtime: mgr})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/settings/apply", nil))

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Error                   string   `json:"error"`
		Code                    string   `json:"code"`
		RestartRequiredSections []string `json:"restart_required_sections"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Code != "restart_required" || len(response.RestartRequiredSections) != 1 || response.RestartRequiredSections[0] != runtimeconfig.GatewayRequestAdmissionSection {
		t.Fatalf("restart-required response = %+v", response)
	}
	if response.Error == "" {
		t.Fatal("restart-required response omitted error message")
	}
	if factoryCalls != 0 {
		t.Fatalf("scheduler factory called %d times for rejected apply", factoryCalls)
	}
}

func TestScopedSettingsSavesMergeStaleFullSnapshots(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := scopedSettingsTestConfig()
	if err := config.Save(path, initial); err != nil {
		t.Fatalf("save config: %v", err)
	}
	router := scopedSettingsTestRouter(path)

	systemInput := settingsInputFromConfig(initial)
	systemInput.App = config.AppConfig{Title: "system-new", NotificationPrefix: "[system] ", TimeZone: "Asia/Shanghai"}
	systemInput.Auth.Username = "system-admin"
	systemInput.Auth.Password = ""
	systemInput.Auth.TokenSecret = ""
	systemInput.Auth.SessionTTLHours = 336
	systemInput.Scheduler.BalanceCron = "0 0 */2 * * *"
	systemInput.Scheduler.Concurrency = 11
	systemInput.Scheduler.CodexRadarRefreshIntervalMins = 17
	systemInput.Scheduler.Retention = config.RetentionConfig{Cron: "0 0 4 * * *", MonitorLogsDays: 14, BalanceSnapshotsDays: 28, NotificationLogsDays: 42, AnnouncementsDays: 56}
	systemInput.Proxy.Host = "system-proxy"
	systemInput.Proxy.Password = ""
	systemInput.Upstream = config.UpstreamConfig{TimeoutSeconds: 67, UserAgent: "system-agent"}

	gatewayInput := settingsInputFromConfig(initial)
	gatewayInput.App.Title = "must-not-win-from-gateway"
	gatewayInput.Notifications.MinChangePct = 999
	gatewayInput.Scheduler.RateRefreshIntervalMinutes = 2
	gatewayInput.Scheduler.SessionKeepAliveEnabled = false
	gatewayInput.Scheduler.SessionKeepAliveIntervalMins = 8
	gatewayInput.Scheduler.LatencyProbeEnabled = false
	gatewayInput.Scheduler.LatencyProbeIntervalSeconds = 75
	gatewayInput.Scheduler.LatencyProbeMaxCandidates = 7
	gatewayInput.Scheduler.LatencyProbeTimeoutSeconds = 19
	gatewayInput.Scheduler.LatencyProbeSampleSize = 13
	gatewayInput.Scheduler.LatencyStatsWindowHours = 72
	gatewayInput.Scheduler.LatencyProbeModelSyncMinutes = 9
	gatewayInput.Scheduler.LatencyFullScoreMs = 8_000
	gatewayInput.Scheduler.LatencyZeroScoreMs = 90_000
	gatewayInput.Scheduler.UnknownLatencyScoreFactor = 0.65
	gatewayInput.Scheduler.RoutingPriceScoreMax = 65
	gatewayInput.Scheduler.RoutingLatencyScoreMax = 35
	gatewayInput.Scheduler.RoutingFailures = config.RoutingFailureConfig{ResponseTimeoutSeconds: 55, ManualCooldownMinutes: 23}
	gatewayInput.Scheduler.SessionAffinityMaxAgeMinutes = 21
	gatewayInput.Scheduler.SessionAffinityStableSamples = 12

	notificationsInput := settingsInputFromConfig(initial)
	notificationsInput.App.Title = "must-not-win-from-notifications"
	notificationsInput.Scheduler.RateRefreshIntervalMinutes = 999
	notificationsInput.Notifications = config.NotificationsConfig{
		BatchRateChanges: true, MinChangePct: 3.5, BalanceLowCooldownMinutes: 123,
		SubscriptionDailyRemainingThresholdPct: 10, SubscriptionWeeklyRemainingThresholdPct: 20,
		SubscriptionMonthlyRemainingThresholdPct: 30, SubscriptionExpiryThresholdHours: 48,
		SubscriptionAlertCooldownMinutes: 720, SendMaxAttempts: 6,
	}

	type request struct {
		path string
		body string
	}
	requests := make([]request, 0, 3)
	for _, item := range []struct {
		path  string
		input settingsConfigInput
	}{
		{path: "/api/settings/config/system", input: systemInput},
		{path: "/api/settings/config/gateway", input: gatewayInput},
		{path: "/api/settings/config/notifications", input: notificationsInput},
	} {
		body, err := json.Marshal(item.input)
		if err != nil {
			t.Fatalf("encode %s request: %v", item.path, err)
		}
		requests = append(requests, request{path: item.path, body: string(body)})
	}
	start := make(chan struct{})
	errorsCh := make(chan string, len(requests))
	var wg sync.WaitGroup
	for _, item := range requests {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			request := httptest.NewRequest(http.MethodPatch, item.path, strings.NewReader(item.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				errorsCh <- item.path + ": " + recorder.Body.String()
				return
			}
			var response struct {
				Data struct {
					Message string `json:"message"`
				} `json:"data"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Data.Message != "已写入配置文件" {
				errorsCh <- item.path + ": unexpected response " + recorder.Body.String()
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errorsCh)
	for message := range errorsCh {
		t.Error(message)
	}
	if t.Failed() {
		return
	}

	got, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load merged config: %v", err)
	}
	if got.App != systemInput.App || got.Auth.Username != systemInput.Auth.Username || got.Auth.SessionTTLHours != systemInput.Auth.SessionTTLHours {
		t.Fatalf("system scope fields = app=%+v auth=%+v", got.App, got.Auth)
	}
	if got.Auth.Password != initial.Auth.Password || got.Auth.TokenSecret != initial.Auth.TokenSecret {
		t.Fatalf("blank system secrets overwrote stored values: auth=%+v", got.Auth)
	}
	if got.Proxy.Host != systemInput.Proxy.Host || got.Proxy.Password != initial.Proxy.Password || got.Upstream.TimeoutSeconds != 67 || got.Upstream.UserAgent != "system-agent" {
		t.Fatalf("system proxy/upstream = proxy=%+v upstream=%+v", got.Proxy, got.Upstream)
	}
	if got.Notifications != notificationsInput.Notifications {
		t.Fatalf("notifications = %+v, want %+v", got.Notifications, notificationsInput.Notifications)
	}
	expectedScheduler := initial.Scheduler
	expectedScheduler.BalanceCron = systemInput.Scheduler.BalanceCron
	expectedScheduler.Concurrency = systemInput.Scheduler.Concurrency
	expectedScheduler.CodexRadarRefreshIntervalMins = systemInput.Scheduler.CodexRadarRefreshIntervalMins
	expectedScheduler.Retention = systemInput.Scheduler.Retention
	expectedScheduler.RateRefreshIntervalMinutes = gatewayInput.Scheduler.RateRefreshIntervalMinutes
	expectedScheduler.SessionKeepAliveEnabled = gatewayInput.Scheduler.SessionKeepAliveEnabled
	expectedScheduler.SessionKeepAliveIntervalMins = gatewayInput.Scheduler.SessionKeepAliveIntervalMins
	expectedScheduler.LatencyProbeEnabled = gatewayInput.Scheduler.LatencyProbeEnabled
	expectedScheduler.LatencyProbeIntervalSeconds = gatewayInput.Scheduler.LatencyProbeIntervalSeconds
	expectedScheduler.LatencyProbeMaxCandidates = gatewayInput.Scheduler.LatencyProbeMaxCandidates
	expectedScheduler.LatencyProbeTimeoutSeconds = gatewayInput.Scheduler.LatencyProbeTimeoutSeconds
	expectedScheduler.LatencyProbeSampleSize = gatewayInput.Scheduler.LatencyProbeSampleSize
	expectedScheduler.LatencyStatsWindowHours = gatewayInput.Scheduler.LatencyStatsWindowHours
	expectedScheduler.LatencyProbeModelSyncMinutes = gatewayInput.Scheduler.LatencyProbeModelSyncMinutes
	expectedScheduler.LatencyFullScoreMs = gatewayInput.Scheduler.LatencyFullScoreMs
	expectedScheduler.LatencyZeroScoreMs = gatewayInput.Scheduler.LatencyZeroScoreMs
	expectedScheduler.UnknownLatencyScoreFactor = gatewayInput.Scheduler.UnknownLatencyScoreFactor
	expectedScheduler.RoutingPriceScoreMax = gatewayInput.Scheduler.RoutingPriceScoreMax
	expectedScheduler.RoutingLatencyScoreMax = gatewayInput.Scheduler.RoutingLatencyScoreMax
	expectedScheduler.RoutingFailures = gatewayInput.Scheduler.RoutingFailures
	expectedScheduler.SessionAffinityMaxAgeMinutes = gatewayInput.Scheduler.SessionAffinityMaxAgeMinutes
	expectedScheduler.SessionAffinityStableSamples = gatewayInput.Scheduler.SessionAffinityStableSamples
	if got.Scheduler != expectedScheduler {
		t.Fatalf("scheduler = %+v, want %+v", got.Scheduler, expectedScheduler)
	}
}

func TestSystemSettingsScopeSecretMergeAndTimeZoneValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := scopedSettingsTestConfig()
	if err := config.Save(path, initial); err != nil {
		t.Fatalf("save config: %v", err)
	}
	router := scopedSettingsTestRouter(path)
	input := settingsInputFromConfig(initial)

	patchSystem := func(value settingsConfigInput, wantStatus int) {
		t.Helper()
		recorder := settingsJSONRequest(t, router, http.MethodPatch, "/api/settings/config/system", value)
		if recorder.Code != wantStatus {
			t.Fatalf("system patch status=%d body=%s, want %d", recorder.Code, recorder.Body.String(), wantStatus)
		}
	}
	patchSystem(input, http.StatusOK)
	preserved, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load preserved config: %v", err)
	}
	if preserved.Auth.Password != initial.Auth.Password || preserved.Auth.TokenSecret != initial.Auth.TokenSecret || preserved.Proxy.Password != initial.Proxy.Password {
		t.Fatalf("blank secrets were not preserved")
	}

	input.Auth.Password = "replacement-admin"
	input.Auth.TokenSecret = "replacement-token"
	input.Proxy.Password = "replacement-proxy"
	patchSystem(input, http.StatusOK)
	replaced, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load replaced config: %v", err)
	}
	if replaced.Auth.Password != "replacement-admin" || replaced.Auth.TokenSecret != "replacement-token" || replaced.Proxy.Password != "replacement-proxy" {
		t.Fatalf("replacement secrets = auth=%+v proxy=%+v", replaced.Auth, replaced.Proxy)
	}

	input.Auth.Password = ""
	input.Auth.TokenSecret = ""
	input.Auth.ClearTokenSecret = true
	input.Proxy.Password = ""
	input.Proxy.ClearPassword = true
	patchSystem(input, http.StatusOK)
	cleared, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load cleared config: %v", err)
	}
	if cleared.Auth.Password != "replacement-admin" || cleared.Auth.TokenSecret != "" || cleared.Proxy.Password != "" {
		t.Fatalf("clear semantics = auth=%+v proxy=%+v", cleared.Auth, cleared.Proxy)
	}

	input.App.Title = "must-not-save"
	input.App.TimeZone = "Mars/Olympus"
	patchSystem(input, http.StatusBadRequest)
	invalid, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load after invalid time zone: %v", err)
	}
	if invalid.App.Title == "must-not-save" || invalid.App.TimeZone != initial.App.TimeZone {
		t.Fatalf("invalid time zone request changed app config: %+v", invalid.App)
	}
}

func TestScopedSettingsRequireOwnedSections(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := scopedSettingsTestConfig()
	if err := config.Save(path, initial); err != nil {
		t.Fatalf("save config: %v", err)
	}
	router := scopedSettingsTestRouter(path)
	for _, tt := range []struct {
		name string
		path string
		body string
	}{
		{name: "system empty", path: "/api/settings/config/system", body: `{}`},
		{name: "system missing upstream", path: "/api/settings/config/system", body: `{"app":{},"auth":{},"scheduler":{},"proxy":{}}`},
		{name: "gateway empty", path: "/api/settings/config/gateway", body: `{}`},
		{name: "gateway missing scheduler", path: "/api/settings/config/gateway", body: `{"notifications":{}}`},
		{name: "notifications empty", path: "/api/settings/config/notifications", body: `{}`},
		{name: "notifications missing section", path: "/api/settings/config/notifications", body: `{"scheduler":{}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := settingsRawRequest(router, http.MethodPatch, tt.path, tt.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s, want 400", recorder.Code, recorder.Body.String())
			}
		})
	}
	stored, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load unchanged config: %v", err)
	}
	if stored.App != initial.App || stored.Auth != initial.Auth || stored.Scheduler != initial.Scheduler || stored.Notifications != initial.Notifications || stored.Proxy != initial.Proxy || stored.Upstream != initial.Upstream {
		t.Fatal("invalid scoped requests changed configuration")
	}
}

func TestGatewaySettingsClampFundingDetectionThreshold(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := scopedSettingsTestConfig()
	if err := config.Save(path, initial); err != nil {
		t.Fatal(err)
	}
	router := scopedSettingsTestRouter(path)

	patch := func(value float64) {
		t.Helper()
		scheduler := initial.Scheduler
		scheduler.FundingDetectionThreshold = value
		recorder := settingsJSONRequest(t, router, http.MethodPatch, "/api/settings/config/gateway", settingsGatewayConfigInput{Scheduler: &scheduler})
		if recorder.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	}

	patch(0.5)
	stored, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Scheduler.FundingDetectionThreshold != config.MinimumFundingDetectionThreshold {
		t.Fatalf("low gateway setting = %v", stored.Scheduler.FundingDetectionThreshold)
	}

	patch(2)
	stored, err = config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Scheduler.FundingDetectionThreshold != 2 {
		t.Fatalf("raised gateway setting = %v, want 2", stored.Scheduler.FundingDetectionThreshold)
	}
}

func TestGatewaySettingsNormalizeUnknownLatencyScoreFactor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := scopedSettingsTestConfig()
	if err := config.Save(path, initial); err != nil {
		t.Fatal(err)
	}
	router := scopedSettingsTestRouter(path)

	patch := func(value float64) {
		t.Helper()
		scheduler := initial.Scheduler
		scheduler.UnknownLatencyScoreFactor = value
		recorder := settingsJSONRequest(t, router, http.MethodPatch, "/api/settings/config/gateway", settingsGatewayConfigInput{Scheduler: &scheduler})
		if recorder.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	}

	patch(0)
	stored, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Scheduler.UnknownLatencyScoreFactor != config.DefaultUnknownLatencyScoreFactor {
		t.Fatalf("zero unknown latency factor = %v", stored.Scheduler.UnknownLatencyScoreFactor)
	}

	patch(1.5)
	stored, err = config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Scheduler.UnknownLatencyScoreFactor != 1 {
		t.Fatalf("upper bound unknown latency factor = %v, want 1", stored.Scheduler.UnknownLatencyScoreFactor)
	}
}

func TestGatewaySettingsValidateManualRoutingCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := scopedSettingsTestConfig()
	if err := config.Save(path, initial); err != nil {
		t.Fatal(err)
	}
	router := scopedSettingsTestRouter(path)

	scheduler := initial.Scheduler
	scheduler.RoutingFailures.ManualCooldownMinutes = config.MaximumManualRoutingCooldownMinutes + 1
	recorder := settingsJSONRequest(t, router, http.MethodPatch, "/api/settings/config/gateway", settingsGatewayConfigInput{Scheduler: &scheduler})
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "manual routing cooldown") {
		t.Fatalf("invalid manual cooldown response = %d %s", recorder.Code, recorder.Body.String())
	}

	stored, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Scheduler.RoutingFailures.ManualCooldownMinutes != config.DefaultManualRoutingCooldownMinutes {
		t.Fatalf("invalid manual cooldown was persisted: %+v", stored.Scheduler.RoutingFailures)
	}
}

func scopedSettingsTestConfig() *config.Config {
	return &config.Config{
		App:     config.AppConfig{Title: "initial", NotificationPrefix: "[initial] ", TimeZone: "UTC"},
		Gateway: config.GatewayConfig{RequestAdmission: config.DefaultGatewayRequestAdmissionConfig()},
		Auth:    config.AuthConfig{Enabled: true, Username: "admin", Password: "stored-admin", TokenSecret: "stored-token", SessionTTLHours: 168},
		Scheduler: config.SchedulerConfig{
			BalanceCron: "0 */5 * * * *", RateCron: "13 */30 * * * *", FundingDetectionThreshold: config.MinimumFundingDetectionThreshold, RateRefreshIntervalMinutes: 30, Concurrency: 3,
			SessionKeepAliveEnabled: true, SessionKeepAliveIntervalMins: 5,
			LatencyProbeEnabled: true, LatencyProbeIntervalSeconds: 60, LatencyProbeMaxCandidates: 5,
			LatencyProbeTimeoutSeconds: 30, LatencyProbeSampleSize: 10, LatencyStatsWindowHours: 168,
			LatencyProbeModelSyncMinutes: 30, LatencyFullScoreMs: 10_000, LatencyZeroScoreMs: 80_000, UnknownLatencyScoreFactor: config.DefaultUnknownLatencyScoreFactor,
			RoutingPriceScoreMax: 70, RoutingLatencyScoreMax: 30,
			CacheInputPriceMultiplier: config.DefaultCacheInputPriceMultiplier, CachePriceMinInputTokens: config.DefaultCachePriceInputTokens,
			CachePriceWindowInputTokens: config.DefaultCachePriceInputTokens, CacheStatsMaxAgeDays: config.DefaultCacheStatsMaxAgeDays,
			RoutingFailures: config.RoutingFailureConfig{ResponseTimeoutSeconds: 30, ManualCooldownMinutes: config.DefaultManualRoutingCooldownMinutes}, SessionAffinityMaxAgeMinutes: 10,
			SessionAffinityStableSamples: 10, CodexRadarRefreshIntervalMins: 30,
			Retention: config.RetentionConfig{Cron: "0 0 3 * * *", MonitorLogsDays: 30, BalanceSnapshotsDays: 90, NotificationLogsDays: 90, AnnouncementsDays: 90},
		},
		Notifications: config.NotificationsConfig{MinChangePct: 1, BalanceLowCooldownMinutes: 60, SendMaxAttempts: 3},
		Proxy:         config.ProxyConfig{Enabled: true, Protocol: "http", Host: "initial-proxy", Port: 8080, Username: "proxy-user", Password: "stored-proxy"},
		Upstream:      config.UpstreamConfig{TimeoutSeconds: 45, UserAgent: "initial-agent"},
	}
}

func settingsInputFromConfig(cfg *config.Config) settingsConfigInput {
	return settingsConfigInput{
		App: cfg.App,
		Auth: settingsAuthConfigInput{
			Enabled: cfg.Auth.Enabled, Username: cfg.Auth.Username, SessionTTLHours: cfg.Auth.SessionTTLHours,
		},
		Scheduler:     cfg.Scheduler,
		Notifications: cfg.Notifications,
		Proxy: settingsProxyConfigInput{
			Enabled: cfg.Proxy.Enabled, VersionCheckEnabled: cfg.Proxy.VersionCheckEnabled,
			Protocol: cfg.Proxy.Protocol, Host: cfg.Proxy.Host, Port: cfg.Proxy.Port, Username: cfg.Proxy.Username,
		},
		Upstream: cfg.Upstream,
	}
}

func scopedSettingsTestRouter(path string) *gin.Engine {
	router := gin.New()
	registerSettings(router.Group("/api"), &Deps{
		Runtime: runtimeconfig.New(path, "", nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil),
	})
	return router
}

func settingsJSONRequest(t *testing.T, router *gin.Engine, method, path string, value any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode settings request: %v", err)
	}
	request := httptest.NewRequest(method, path, strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func settingsRawRequest(router *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}
