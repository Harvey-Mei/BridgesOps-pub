package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	_ "github.com/Harvey-Mei/BridgeOps/backend/connector/newapi"
	_ "github.com/Harvey-Mei/BridgeOps/backend/connector/sub2api"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/progress"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	return testdb.Open(t)
}

func TestConsistentRechargeHistoryFactor(t *testing.T) {
	paidFive := 5.0
	paidTen := 10.0
	factor, ok := consistentRechargeHistoryFactor([]connector.RechargeRecord{
		{SourceID: "one", PaidAmount: &paidFive, CreditedAmount: 5},
		{SourceID: "two", PaidAmount: &paidTen, CreditedAmount: 10},
	})
	if !ok || factor != 1 {
		t.Fatalf("factor = %v/%v, want 1/true", factor, ok)
	}

	paidDiscount := 5.0
	if factor, ok := consistentRechargeHistoryFactor([]connector.RechargeRecord{
		{SourceID: "one", PaidAmount: &paidTen, CreditedAmount: 10},
		{SourceID: "two", PaidAmount: &paidDiscount, CreditedAmount: 10},
	}); ok || factor != 0 {
		t.Fatalf("conflicting factor = %v/%v, want 0/false", factor, ok)
	}
	if factor, ok := consistentRechargeHistoryFactor([]connector.RechargeRecord{
		{SourceID: "known", PaidAmount: &paidTen, CreditedAmount: 10},
		{SourceID: "unknown", PaidAmount: nil, CreditedAmount: 10},
	}); ok || factor != 0 {
		t.Fatalf("incomplete factor = %v/%v, want 0/false", factor, ok)
	}
}

type historicalCostSamplerFunc func(context.Context, *connector.Channel, *connector.AuthSession, time.Time, time.Time) (*connector.HistoricalCostResult, error)

func (f historicalCostSamplerFunc) GetHistoricalCosts(ctx context.Context, channel *connector.Channel, session *connector.AuthSession, startDay, endDay time.Time) (*connector.HistoricalCostResult, error) {
	return f(ctx, channel, session, startDay, endDay)
}

func monitorFloatPointer(value float64) *float64 {
	return &value
}

func TestRecoverCumulativeCostFromDaily(t *testing.T) {
	previousAt := time.Date(2026, 8, 9, 1, 0, 0, 0, time.UTC)
	state := &storage.ChannelCostState{
		RawTodayCost: 2, RawTotalCost: 10, ActualTotalCost: 8, CostSampledAt: &previousAt,
	}

	for _, test := range []struct {
		name       string
		sampledAt  time.Time
		today      float64
		wantToday  float64
		wantRaw    float64
		wantActual float64
		historical *float64
	}{
		{name: "same day delta", sampledAt: previousAt.Add(time.Hour), today: 3, wantToday: 3, wantRaw: 11, wantActual: 8.5},
		{name: "new day includes previous day tail", sampledAt: previousAt.Add(24 * time.Hour), today: 0.5, wantToday: 0.5, wantRaw: 13.5, wantActual: 9.75, historical: monitorFloatPointer(5)},
		{name: "multi-day gap includes every completed day", sampledAt: previousAt.Add(72 * time.Hour), today: 1, wantToday: 1, wantRaw: 18, wantActual: 12, historical: monitorFloatPointer(9)},
		{name: "same day correction preserves high water", sampledAt: previousAt.Add(2 * time.Hour), today: 1, wantToday: 2, wantRaw: 10, wantActual: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := &connector.CostResult{
				RawTodayCost: test.today, AppliedFactor: 0.5, CumulativeCostUnavailable: true,
			}
			var sampler connector.HistoricalCostSampler
			if test.historical != nil {
				sampler = historicalCostSamplerFunc(func(_ context.Context, _ *connector.Channel, _ *connector.AuthSession, startDay, endDay time.Time) (*connector.HistoricalCostResult, error) {
					if !startDay.Equal(time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)) {
						t.Fatalf("historical start = %v", startDay)
					}
					wantEnd := time.Date(test.sampledAt.Year(), test.sampledAt.Month(), test.sampledAt.Day(), 0, 0, 0, 0, time.UTC)
					if !endDay.Equal(wantEnd) {
						t.Fatalf("historical end = %v, want %v", endDay, wantEnd)
					}
					return &connector.HistoricalCostResult{RawCost: *test.historical}, nil
				})
			}
			if err := recoverCumulativeCostFromDaily(context.Background(), sampler, &connector.Channel{}, &connector.AuthSession{}, state, result, test.sampledAt, "UTC"); err != nil {
				t.Fatalf("recoverCumulativeCostFromDaily: %v", err)
			}
			if result.RawTodayCost != test.wantToday || result.RawTotalCost != test.wantRaw || result.TotalCost != test.wantActual {
				t.Fatalf("recovered costs = today %v raw %v actual %v, want today %v raw %v actual %v", result.RawTodayCost, result.RawTotalCost, result.TotalCost, test.wantToday, test.wantRaw, test.wantActual)
			}
		})
	}

	t.Run("uses upstream query timezone", func(t *testing.T) {
		previousAt := time.Date(2026, 8, 9, 20, 0, 0, 0, time.UTC)
		state := &storage.ChannelCostState{
			RawTodayCost: 2, RawTotalCost: 10, ActualTotalCost: 8, CostSampledAt: &previousAt,
		}
		result := &connector.CostResult{
			RawTodayCost: 3, AppliedFactor: 0.5, CumulativeCostUnavailable: true,
		}
		if err := recoverCumulativeCostFromDaily(context.Background(), nil, &connector.Channel{}, &connector.AuthSession{}, state, result, previousAt.Add(5*time.Hour), "Asia/Shanghai"); err != nil {
			t.Fatalf("recoverCumulativeCostFromDaily: %v", err)
		}
		if result.RawTotalCost != 11 || result.TotalCost != 8.5 {
			t.Fatalf("timezone-aware totals = raw %v actual %v, want raw 11 actual 8.5", result.RawTotalCost, result.TotalCost)
		}
	})

	t.Run("rejects cross-day recovery without historical support", func(t *testing.T) {
		result := &connector.CostResult{
			RawTodayCost: 1, AppliedFactor: 0.5, CumulativeCostUnavailable: true,
		}
		err := recoverCumulativeCostFromDaily(context.Background(), nil, &connector.Channel{}, &connector.AuthSession{}, state, result, previousAt.Add(24*time.Hour), "UTC")
		if err == nil || !strings.Contains(err.Error(), "without historical cost support") {
			t.Fatalf("cross-day recovery error = %v", err)
		}
		if result.RawTotalCost != 0 || result.TotalCost != 0 {
			t.Fatalf("cross-day recovery mutated totals: %+v", result)
		}
	})

	t.Run("rejects incomplete completed-day history", func(t *testing.T) {
		result := &connector.CostResult{
			RawTodayCost: 1, AppliedFactor: 0.5, CumulativeCostUnavailable: true,
		}
		sampler := historicalCostSamplerFunc(func(context.Context, *connector.Channel, *connector.AuthSession, time.Time, time.Time) (*connector.HistoricalCostResult, error) {
			return &connector.HistoricalCostResult{RawCost: 1.5}, nil
		})
		err := recoverCumulativeCostFromDaily(context.Background(), sampler, &connector.Channel{}, &connector.AuthSession{}, state, result, previousAt.Add(24*time.Hour), "UTC")
		if err == nil || !strings.Contains(err.Error(), "below the previous daily high-water mark") {
			t.Fatalf("incomplete history error = %v", err)
		}
		if result.RawTotalCost != 0 || result.TotalCost != 0 {
			t.Fatalf("incomplete history mutated totals: %+v", result)
		}
	})
}

type monitorTestHarness struct {
	db           *gorm.DB
	service      *Service
	channels     *storage.Channels
	authSessions *storage.AuthSessions
	rates        *storage.Rates
	monitorLogs  *storage.MonitorLogs
	cipher       *crypto.Cipher
}

func newMonitorTestHarness(t *testing.T, ch *storage.Channel) *monitorTestHarness {
	t.Helper()
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if strings.TrimSpace(ch.LastError) == "" {
		if err := channels.MarkSyncFailure(ch.ID, "stale login failure"); err != nil {
			t.Fatalf("seed stale sync error: %v", err)
		}
	}
	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	return &monitorTestHarness{
		db:           db,
		service:      NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil))),
		channels:     channels,
		authSessions: authSessions,
		rates:        rates,
		monitorLogs:  monitorLogs,
		cipher:       cipher,
	}
}

func seedMonitorPasswordSession(t *testing.T, h *monitorTestHarness, channelID uint) {
	t.Helper()
	expiresAt := time.Now().Add(time.Hour)
	if err := h.authSessions.Upsert(&storage.AuthSession{
		ChannelID:          channelID,
		UserID:             "7",
		AccessTokenCipher:  mustEncrypt(t, h.cipher, ""),
		RefreshTokenCipher: mustEncrypt(t, h.cipher, ""),
		CookieCipher:       mustEncrypt(t, h.cipher, "session=1"),
		CSRFTokenCipher:    mustEncrypt(t, h.cipher, ""),
		ExpiresAt:          &expiresAt,
	}); err != nil {
		t.Fatalf("seed password session: %v", err)
	}
}

func setMonitorCredential(t *testing.T, h *monitorTestHarness, ch *storage.Channel, raw string) {
	t.Helper()
	ch.PasswordCipher = mustEncrypt(t, h.cipher, raw)
	if err := h.channels.UpdateWithUpstreamAccountIdentityChange(ch); err != nil {
		t.Fatalf("update credential: %v", err)
	}
}

func TestKeepAliveSessionsRecordsBalanceFactWithoutOverwritingSyncError(t *testing.T) {
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/user/self" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"INSUFFICIENT_BALANCE"}}`))
	}))
	defer apiServer.Close()

	ch := &storage.Channel{
		Name: "keepalive-balance", Type: storage.ChannelTypeNewAPI, SiteURL: apiServer.URL,
		CredentialMode: storage.CredentialModeToken,
	}
	h := newMonitorTestHarness(t, ch)
	setMonitorCredential(t, h, ch, `{"cookie":"session=1","user_id":"7"}`)

	result := h.service.KeepAliveSessions(context.Background())
	if result.Total != 1 || result.Success != 0 || result.Failed != 1 {
		t.Fatalf("keepalive result = %+v", result)
	}
	stored, err := h.channels.FindByID(ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastError != "stale login failure" {
		t.Fatalf("last error = %q, want existing sync error preserved", stored.LastError)
	}
	if !stored.HasCurrentUpstreamAccountBalanceInsufficient() {
		t.Fatalf("current account balance fact was not recorded: %+v", stored)
	}
	if stored.BalanceError != "" || stored.BalanceErrorAt != nil {
		t.Fatalf("keepalive polluted balance collection error: %q at %v", stored.BalanceError, stored.BalanceErrorAt)
	}
}

func TestRefreshRatesRecordsBalanceFactWithoutSchedulingSyncRetry(t *testing.T) {
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/user/self" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"message":"Insufficient account balance"}}`))
	}))
	defer apiServer.Close()

	ch := &storage.Channel{
		Name: "rates-balance", Type: storage.ChannelTypeNewAPI, SiteURL: apiServer.URL,
		CredentialMode: storage.CredentialModeToken, MonitorEnabled: true,
	}
	h := newMonitorTestHarness(t, ch)
	setMonitorCredential(t, h, ch, `{"cookie":"session=1","user_id":"7"}`)
	if err := h.channels.ClearSyncFailureForIdentity(ch.ID, ch.UpstreamAccountIdentityRevision); err != nil {
		t.Fatalf("clear seeded sync failure: %v", err)
	}

	err := h.service.RefreshRates(context.Background(), ch)
	if err == nil || !connector.IsAccountBalanceHTTPError(err) {
		t.Fatalf("RefreshRates error = %v, want account balance failure", err)
	}
	stored, findErr := h.channels.FindByID(ch.ID)
	if findErr != nil {
		t.Fatal(findErr)
	}
	if !stored.HasCurrentUpstreamAccountBalanceInsufficient() {
		t.Fatalf("current account balance fact was not recorded: %+v", stored)
	}
	if stored.LastError != "" || stored.SyncFailureCount != 0 || stored.NextSyncRetryAt != nil {
		t.Fatalf("balance rejection entered sync retry state: %+v", stored)
	}
	if stored.BalanceError != "" || stored.BalanceErrorAt != nil {
		t.Fatalf("rate refresh polluted balance collection error: %q at %v", stored.BalanceError, stored.BalanceErrorAt)
	}
}

func TestRefreshRatesUsesConnectorShortRetryHint(t *testing.T) {
	var settingsRequests atomic.Int32
	var loginRequests atomic.Int32
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/settings/public":
			settingsRequests.Add(1)
			http.Error(w, "temporary", http.StatusServiceUnavailable)
		case "/api/v1/auth/login":
			loginRequests.Add(1)
			http.Error(w, "unexpected login", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiServer.Close()

	ch := &storage.Channel{
		Name:           "sub2api-short-retry",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        apiServer.URL,
		Username:       "operator",
		CredentialMode: storage.CredentialModePassword,
		MonitorEnabled: true,
	}
	h := newMonitorTestHarness(t, ch)
	setMonitorCredential(t, h, ch, "password")

	before := time.Now()
	err := h.service.RefreshRates(context.Background(), ch)
	if err == nil {
		t.Fatal("RefreshRates succeeded, want transient discovery failure")
	}
	if delay, ok := connector.RetryDelay(err); !ok || delay != time.Minute {
		t.Fatalf("RefreshRates retry delay = %s/%v, want 1m", delay, ok)
	}
	stored, findErr := h.channels.FindByID(ch.ID)
	if findErr != nil {
		t.Fatal(findErr)
	}
	if stored.LastError == "" || stored.SyncFailureCount != 0 || stored.NextSyncRetryAt == nil {
		t.Fatalf("short retry recovery state = %+v", stored)
	}
	if stored.NextSyncRetryAt.Before(before.Add(50*time.Second)) || stored.NextSyncRetryAt.After(before.Add(70*time.Second)) {
		t.Fatalf("short retry scheduled at %s", stored.NextSyncRetryAt)
	}
	if settingsRequests.Load() != 1 || loginRequests.Load() != 0 {
		t.Fatalf("requests: settings=%d login=%d", settingsRequests.Load(), loginRequests.Load())
	}
}

func TestRefreshBalanceCompensatesBalanceFailureOnce(t *testing.T) {
	var statusCalls atomic.Int32
	var loginCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/login":
			loginCalls.Add(1)
			http.Error(w, "login must not be called", http.StatusInternalServerError)
		case "/api/status":
			if statusCalls.Add(1) == 1 {
				http.Error(w, "temporary balance failure", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":100,"price":1}}`))
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":1000,"used_quota":100}}`))
		case "/api/log/self/stat":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":10}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{Name: "balance-retry", Type: storage.ChannelTypeNewAPI, SiteURL: apiSrv.URL, CredentialMode: storage.CredentialModeToken}
	h := newMonitorTestHarness(t, ch)
	setMonitorCredential(t, h, ch, `{"cookie":"session=1","user_id":"7"}`)

	if err := h.service.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatalf("RefreshBalance: %v", err)
	}
	if got := statusCalls.Load(); got != 3 {
		t.Fatalf("status calls = %d, want initial failure + balance retry + cost", got)
	}
	if got := loginCalls.Load(); got != 0 {
		t.Fatalf("login calls = %d, want 0", got)
	}
}

func TestRefreshBalanceCompensatesCostFailureOnce(t *testing.T) {
	var statusCalls atomic.Int32
	var statCalls atomic.Int32
	var loginCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/login":
			loginCalls.Add(1)
			http.Error(w, "login must not be called", http.StatusInternalServerError)
		case "/api/status":
			statusCalls.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":100,"price":1}}`))
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":1000,"used_quota":100}}`))
		case "/api/log/self/stat":
			if statCalls.Add(1) == 1 {
				http.Error(w, "temporary cost failure", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":10}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{Name: "cost-retry", Type: storage.ChannelTypeNewAPI, SiteURL: apiSrv.URL, CredentialMode: storage.CredentialModeToken}
	h := newMonitorTestHarness(t, ch)
	setMonitorCredential(t, h, ch, `{"cookie":"session=1","user_id":"7"}`)

	if err := h.service.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatalf("RefreshBalance: %v", err)
	}
	if got := statCalls.Load(); got != 2 {
		t.Fatalf("cost calls = %d, want 2", got)
	}
	if got := statusCalls.Load(); got != 3 {
		t.Fatalf("status calls = %d, want balance + two cost attempts", got)
	}
	if got := loginCalls.Load(); got != 0 {
		t.Fatalf("login calls = %d, want 0", got)
	}
}

func TestRefreshBalanceCompensationFailureDoesNotRetry(t *testing.T) {
	var statusCalls atomic.Int32
	var selfCalls atomic.Int32
	var loginCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/login":
			loginCalls.Add(1)
			http.Error(w, "login must not be called", http.StatusInternalServerError)
		case "/api/status":
			statusCalls.Add(1)
			http.Error(w, "balance failure", http.StatusInternalServerError)
		case "/api/user/self":
			if selfCalls.Add(1) == 2 {
				http.Error(w, "session sync failure", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":1000,"used_quota":100}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{Name: "sync-failure", Type: storage.ChannelTypeNewAPI, SiteURL: apiSrv.URL, CredentialMode: storage.CredentialModeToken}
	h := newMonitorTestHarness(t, ch)
	setMonitorCredential(t, h, ch, `{"cookie":"session=1","user_id":"7"}`)

	if err := h.service.RefreshBalance(context.Background(), ch); err == nil {
		t.Fatal("RefreshBalance succeeded after compensation failure")
	}
	if got := statusCalls.Load(); got != 1 {
		t.Fatalf("status calls = %d, want 1", got)
	}
	if got := loginCalls.Load(); got != 0 {
		t.Fatalf("login calls = %d, want 0", got)
	}
}

func TestRefreshBalanceRepeatedFailureDoesNotLoop(t *testing.T) {
	var statusCalls atomic.Int32
	var selfCalls atomic.Int32
	var loginCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/login":
			loginCalls.Add(1)
			http.Error(w, "login must not be called", http.StatusInternalServerError)
		case "/api/status":
			if statusCalls.Add(1) <= 2 {
				http.Error(w, "repeated balance failure", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":100,"price":1}}`))
		case "/api/user/self":
			selfCalls.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":1000,"used_quota":100}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{Name: "no-loop", Type: storage.ChannelTypeNewAPI, SiteURL: apiSrv.URL, CredentialMode: storage.CredentialModeToken}
	h := newMonitorTestHarness(t, ch)
	setMonitorCredential(t, h, ch, `{"cookie":"session=1","user_id":"7"}`)

	if err := h.service.RefreshBalance(context.Background(), ch); err == nil {
		t.Fatal("RefreshBalance succeeded after repeated collection failure")
	}
	if got := statusCalls.Load(); got != 2 {
		t.Fatalf("status calls = %d, want 2", got)
	}
	if got := selfCalls.Load(); got != 2 {
		t.Fatalf("session checks = %d, want initial check plus one compensation", got)
	}
	if got := loginCalls.Load(); got != 0 {
		t.Fatalf("login calls = %d, want 0", got)
	}
}

func TestRefreshRatesCompensationReusesPasswordSession(t *testing.T) {
	var groupCalls atomic.Int32
	var loginCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/login":
			loginCalls.Add(1)
			http.Error(w, "login must not be called", http.StatusInternalServerError)
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":7}}`))
		case "/api/user/self/groups":
			if groupCalls.Add(1) == 1 {
				http.Error(w, "temporary rates failure", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"default":{"ratio":1,"desc":"default"}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name: "password-rates", Type: storage.ChannelTypeNewAPI, SiteURL: apiSrv.URL,
		Username: "user", CredentialMode: storage.CredentialModePassword,
		IgnoreAnnouncements: true,
	}
	h := newMonitorTestHarness(t, ch)
	setMonitorCredential(t, h, ch, "password")
	seedMonitorPasswordSession(t, h, ch.ID)

	if err := h.service.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("RefreshRates: %v", err)
	}
	if got := groupCalls.Load(); got != 2 {
		t.Fatalf("group calls = %d, want 2", got)
	}
	if got := loginCalls.Load(); got != 0 {
		t.Fatalf("login calls = %d, want 0", got)
	}
}

func TestRefreshBalanceUsesCanonicalCostState(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatal(err)
	}

	var balanceQuota atomic.Int64
	balanceQuota.Store(1000)
	var advertisedPrice atomic.Uint64
	advertisedPrice.Store(math.Float64bits(0.5))
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/status":
			price := strconv.FormatFloat(math.Float64frombits(advertisedPrice.Load()), 'f', -1, 64)
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":100,"price":` + price + `}}`))
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":` + strconv.FormatInt(balanceQuota.Load(), 10) + `,"used_quota":1000}}`))
		case "/api/user/topup/self":
			money := 10 * math.Float64frombits(advertisedPrice.Load())
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[{"id":1,"amount":10,"money":` + strconv.FormatFloat(money, 'f', -1, 64) + `,"trade_no":"paid-1","status":"success","complete_time":` + strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10) + `}]}}`))
		case "/api/log/self/stat":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":200}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name: "canonical-cost", Type: storage.ChannelTypeNewAPI,
		SiteURL: apiSrv.URL, Username: "u",
		PasswordCipher: mustEncrypt(t, cipher, `{"cookie":"session=1","user_id":"7"}`),
		CredentialMode: storage.CredentialModeToken, MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatal(err)
	}
	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	costingSvc := costing.NewService(storage.NewCosting(db))
	svc.SetCosting(costingSvc)

	if err := svc.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	got, err := channels.FindByID(ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.RawLastBalance == nil || *got.RawLastBalance != 10 || got.LastBalance == nil || *got.LastBalance != 5 {
		t.Fatalf("balance projection = raw %#v actual %#v", got.RawLastBalance, got.LastBalance)
	}
	if got.RawTodayCost == nil || *got.RawTodayCost != 2 || got.TodayCost == nil || *got.TodayCost != 1 ||
		got.RawTotalCost == nil || *got.RawTotalCost != 10 || got.TotalCost == nil || *got.TotalCost != 5 {
		t.Fatalf("cost projection = %+v", got)
	}
	if got.FundingFactor != 0.5 || got.FundingFactorSource != "recharge_history" {
		t.Fatalf("cost basis projection = %v/%q", got.FundingFactor, got.FundingFactorSource)
	}
	state, err := costingSvc.State(ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 2 || state.FundingFactor != 0.5 || state.ActualInventory != 5 {
		t.Fatalf("cost state = %+v", state)
	}
	balanceHistory, err := rates.BalanceHistory(ch.ID, 10)
	if err != nil || len(balanceHistory) != 1 {
		t.Fatalf("balance history = %+v, %v", balanceHistory, err)
	}
	if balanceHistory[0].RawBalance == nil || *balanceHistory[0].RawBalance != 10 ||
		balanceHistory[0].FundingFactor == nil || *balanceHistory[0].FundingFactor != 0.5 ||
		balanceHistory[0].Balance != 5 || balanceHistory[0].CostBasisVersion != 2 {
		t.Fatalf("balance snapshot = %+v", balanceHistory[0])
	}
	var costHistory []storage.CostSnapshot
	if err := db.Where("channel_id = ?", ch.ID).Find(&costHistory).Error; err != nil || len(costHistory) != 1 {
		t.Fatalf("cost history = %+v, %v", costHistory, err)
	}
	if costHistory[0].RawTodayCost == nil || *costHistory[0].RawTodayCost != 2 ||
		costHistory[0].FundingFactor == nil || *costHistory[0].FundingFactor != 0.5 ||
		costHistory[0].TodayCost != 1 || costHistory[0].CostBasisVersion != 2 {
		t.Fatalf("cost snapshot = %+v", costHistory[0])
	}

	// A small observed increase still advances inventory and snapshots, but it
	// must not create a pending recharge candidate.
	balanceQuota.Store(1098)
	if err := svc.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	candidates, total, err := costingSvc.FundingCandidates(ch.ID, "pending", 10, 0)
	if err != nil || total != 0 || len(candidates) != 0 {
		t.Fatalf("small balance increase created candidates = %d/%d, %v", len(candidates), total, err)
	}
	state, err = costingSvc.State(ch.ID)
	if err != nil || math.Abs(state.RawBalance-10.98) > 1e-12 {
		t.Fatalf("small balance increase was not recorded: %+v, %v", state, err)
	}

	// Detect the threshold-reaching increase before RecordBalance advances the
	// state, preserving the actual pre-recharge raw inventory for confirmation.
	balanceQuota.Store(2000)
	if err := svc.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	candidates, total, err = costingSvc.FundingCandidates(ch.ID, "pending", 10, 0)
	if err != nil || total != 1 || len(candidates) != 1 {
		t.Fatalf("funding candidates = %d/%d, %v", len(candidates), total, err)
	}
	if candidates[0].Source != "balance_increase" || math.Abs(candidates[0].RawBalanceBefore-10.98) > 1e-12 ||
		candidates[0].RawBalanceDetected != 20 || math.Abs(candidates[0].CreditedAmount-9.02) > 1e-12 {
		t.Fatalf("funding candidate did not freeze pre-recharge balance: %+v", candidates[0])
	}
	state, err = costingSvc.State(ch.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Once an operator recharge establishes a weighted factor, connector
	// observations must not replace that inventory cost basis.
	state, _, err = costingSvc.ConfirmFundingCandidate(ch.ID, candidates[0].ID, 1.51, state.Version, "test recharge")
	if err != nil {
		t.Fatal(err)
	}
	if state.FundingFactor != 0.35 {
		t.Fatalf("weighted factor = %v", state.FundingFactor)
	}
	if err := svc.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatal(err)
	}
	state, err = costingSvc.State(ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.FundingFactor != 0.35 || state.Source != "confirmed_balance_increase" {
		t.Fatalf("observed factor replaced weighted basis: %+v", state)
	}
	got, err = channels.FindByID(ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TodayCost == nil || math.Abs(*got.TodayCost-1) > 1e-12 || got.TotalCost == nil || math.Abs(*got.TotalCost-5) > 1e-12 {
		t.Fatalf("weighted actual costs = today %#v total %#v", got.TodayCost, got.TotalCost)
	}

	// A 1:1 channel does not need a paid-amount reconciliation reminder.
	advertisedPrice.Store(math.Float64bits(1))
	unitChannel := &storage.Channel{
		Name: "unit-cost", Type: storage.ChannelTypeNewAPI,
		SiteURL: apiSrv.URL, Username: "u",
		PasswordCipher: mustEncrypt(t, cipher, `{"cookie":"session=1","user_id":"7"}`),
		CredentialMode: storage.CredentialModeToken, MonitorEnabled: true,
	}
	if err := channels.Create(unitChannel); err != nil {
		t.Fatal(err)
	}
	balanceQuota.Store(2000)
	if err := svc.RefreshBalance(context.Background(), unitChannel); err != nil {
		t.Fatal(err)
	}
	balanceQuota.Store(3000)
	if err := svc.RefreshBalance(context.Background(), unitChannel); err != nil {
		t.Fatal(err)
	}
	unitCandidates, unitTotal, err := costingSvc.FundingCandidates(unitChannel.ID, "pending", 10, 0)
	if err != nil || unitTotal != 0 || len(unitCandidates) != 0 {
		t.Fatalf("1:1 balance increase created candidates = %d/%d, %v", len(unitCandidates), unitTotal, err)
	}
}

func TestRefreshRatesSyncAnnouncementsAndNotify(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	var webhookHits atomic.Int32
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		var body struct {
			Event string `json:"event"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode webhook: %v", err)
		}
		if body.Event != string(storage.EventAnnouncement) {
			t.Fatalf("webhook event = %q, want announcement", body.Event)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()

	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notify channel: %v", err)
	}

	var statusCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/status":
			call := statusCalls.Add(1)
			id := 1
			title := "公告一"
			content := "首次公告"
			// Calls 1, 3 and 4 are model-price unit probes. Announcement
			// collection remains calls 2 and 5 in this three-refresh test.
			if call >= 5 {
				id = 2
				title = "公告二"
				content = "新增公告"
			}
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000,"announcements":[{"id":` +
				jsonNumber(id) + `,"title":"` + title + `","content":"` + content + `","type":"warning","publishDate":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T03:04:05Z"}]}}`))
		case "/api/user/self":
			if got := r.Header.Get("Cookie"); got != "session=1" {
				t.Fatalf("cookie = %q", got)
			}
			if got := r.Header.Get("New-Api-User"); got != "7" {
				t.Fatalf("New-Api-User = %q", got)
			}
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000,"used_quota":500000}}`))
		case "/api/user/self/groups":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"default":{"ratio":1,"desc":"default"}}}`))
		case "/api/notice":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name:           "demo",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        apiSrv.URL,
		Username:       "u",
		PasswordCipher: mustEncrypt(t, cipher, `{"cookie":"session=1","user_id":"7"}`),
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		SendMaxAttempts: 1,
	})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	stored, err := channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("find refreshed channel: %v", err)
	}
	if stored.LastError != "" || stored.SyncFailureCount != 0 || stored.NextSyncRetryAt != nil {
		t.Fatalf("successful rate sync kept a failure state: %+v", stored)
	}
	if got := webhookHits.Load(); got != 0 {
		t.Fatalf("first refresh webhook hits = %d, want 0", got)
	}

	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("throttled refresh: %v", err)
	}
	if got := webhookHits.Load(); got != 0 {
		t.Fatalf("throttled refresh webhook hits = %d, want 0", got)
	}
	if got := statusCalls.Load(); got != 3 {
		t.Fatalf("status calls = %d, want two price-unit probes and one announcement sync", got)
	}
	svc.cadenceMu.Lock()
	svc.announcementAt[channelIdentityRevisionKey{ChannelID: ch.ID, Revision: ch.UpstreamAccountIdentityRevision}] = time.Now().Add(-announcementSyncInterval)
	svc.cadenceMu.Unlock()
	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("second announcement refresh: %v", err)
	}
	if got := webhookHits.Load(); got != 1 {
		t.Fatalf("second announcement refresh webhook hits = %d, want 1", got)
	}

	logs, err := notifies.ListLogs(10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(logs) != 1 || logs[0].Event != storage.EventAnnouncement || !logs[0].Success {
		t.Fatalf("logs = %#v", logs)
	}
}

func TestRateSuccessMonitorLogUsesHeartbeat(t *testing.T) {
	db := openTestDB(t)
	logs := storage.NewMonitorLogs(db)
	svc := NewService(nil, nil, nil, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	started := time.Now().Add(-time.Second)
	finished := time.Now()
	svc.appendRateMonitorLog(1, started, finished, nil)
	svc.appendRateMonitorLog(1, started.Add(time.Second), finished.Add(time.Second), nil)
	items, err := logs.List(1, 10)
	if err != nil || len(items) != 1 || !items[0].Success {
		t.Fatalf("throttled success logs = %+v, err=%v", items, err)
	}
	svc.appendRateMonitorLog(1, started.Add(2*time.Second), finished.Add(2*time.Second), errors.New("failed"))
	svc.appendRateMonitorLog(1, started.Add(3*time.Second), finished.Add(3*time.Second), nil)
	items, err = logs.List(1, 10)
	if err != nil || len(items) != 3 || !items[0].Success || items[1].Success {
		t.Fatalf("failure and recovery logs = %+v, err=%v", items, err)
	}
}

func TestRefreshRatesSkipsAnnouncementsWhenIgnored(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	var webhookHits atomic.Int32
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()

	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notify channel: %v", err)
	}

	var statusCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/status":
			statusCalls.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000,"announcements":[{"id":1,"title":"公告一","content":"首次公告","type":"warning","publishDate":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T03:04:05Z"}]}}`))
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000,"used_quota":500000}}`))
		case "/api/user/self/groups":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"default":{"ratio":1,"desc":"default"}}}`))
		case "/api/notice":
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name:                "demo",
		Type:                storage.ChannelTypeNewAPI,
		SiteURL:             apiSrv.URL,
		Username:            "u",
		PasswordCipher:      mustEncrypt(t, cipher, `{"cookie":"session=1","user_id":"7"}`),
		CredentialMode:      storage.CredentialModeToken,
		MonitorEnabled:      true,
		IgnoreAnnouncements: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list announcements: %v", err)
	}
	if total != 0 || len(list) != 0 {
		t.Fatalf("announcements should be skipped: total=%d list=%#v", total, list)
	}
	if got := statusCalls.Load(); got != 1 {
		t.Fatalf("status calls = %d, want one price-unit probe and no announcement sync", got)
	}
	logs, err := notifies.ListLogs(10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("notification logs = %#v", logs)
	}
}

func TestRefreshRatesEmitsRateAddedAndRemoved(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	var webhookHits atomic.Int32
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()

	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notify channel: %v", err)
	}

	var call atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/settings/public":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"turnstile_enabled":false}}`))
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"balance":50}}`))
		case "/api/v1/usage/dashboard/stats":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"today_actual_cost":1,"total_actual_cost":2}}`))
		case "/api/v1/groups/available":
			n := call.Add(1)
			if n == 1 {
				_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[
					{"id":1,"name":"alpha","description":"a","rate_multiplier":1.0},
					{"id":2,"name":"beta","description":"b","rate_multiplier":2.0}
				]}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[
				{"id":2,"name":"beta","description":"b","rate_multiplier":2.0},
				{"id":3,"name":"gamma","description":"c","rate_multiplier":3.0}
			]}`))
		case "/api/v1/groups/rates":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{}}`))
		case "/api/v1/announcements":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name:                "sub",
		Type:                storage.ChannelTypeSub2API,
		SiteURL:             apiSrv.URL,
		Username:            "u",
		PasswordCipher:      mustEncrypt(t, cipher, `{"access_token":"token"}`),
		CredentialMode:      storage.CredentialModeToken,
		MonitorEnabled:      true,
		SubscriptionEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		BatchRateChanges: true,
		SendMaxAttempts:  1,
	})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	gatewayStore := storage.NewGateway(db)
	syncer := &testGatewaySynchronizer{store: gatewayStore}
	svc.SetGatewaySync(syncer)

	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if err := gatewayStore.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: ch.ID, GroupName: "alpha", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", Enabled: true,
	}); err != nil {
		t.Fatalf("seed alpha gateway key: %v", err)
	}
	if err := gatewayStore.MarkModelCapability(ch.ID, "alpha", "gpt-test", true, ""); err != nil {
		t.Fatalf("seed alpha model capability: %v", err)
	}
	historical := &storage.GatewayRequestLog{ChannelID: ch.ID, GroupName: "alpha", Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: time.Now()}
	if err := gatewayStore.AppendRequestLog(historical); err != nil {
		t.Fatalf("seed historical request: %v", err)
	}
	logs, err := notifies.ListLogs(10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("first refresh logs = %#v", logs)
	}

	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	logs, err = notifies.ListLogs(20)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	var structureChanged int
	for _, log := range logs {
		if log.Event == storage.EventRateStructureChanged {
			structureChanged++
			if !strings.Contains(log.Subject, "[分组变动通知]") ||
				!strings.Contains(log.Subject, "新增 1 / 删除 1") ||
				!strings.Contains(log.Body, "gamma") ||
				!strings.Contains(log.Body, "alpha") {
				t.Fatalf("unexpected structure change log = %#v", log)
			}
		}
	}
	if structureChanged != 1 {
		t.Fatalf("structureChanged=%d logs=%#v", structureChanged, logs)
	}
	if got := webhookHits.Load(); got != 1 {
		t.Fatalf("webhook hits = %d, want 1", got)
	}

	list, err := rates.ListByChannel(ch.ID)
	if err != nil {
		t.Fatalf("list rates: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("rate snapshots len = %d, want 2", len(list))
	}
	got := map[string]bool{}
	for _, item := range list {
		got[item.ModelName] = true
	}
	if !got["beta"] || !got["gamma"] || got["alpha"] {
		t.Fatalf("rate snapshots = %#v", got)
	}
	// The monitor commits the authoritative snapshot and lets the gateway
	// control-plane consumer reconcile removed routing state. It must not issue
	// per-group remote mutations from the fact-collection transaction.
	if _, err := gatewayStore.FindUpstreamKeyAny(ch.ID, "alpha"); err != nil {
		t.Fatalf("removed group gateway key lookup: %v", err)
	}
	var historicalCount int64
	if err := db.Model(&storage.GatewayRequestLog{}).Where("id = ? AND group_name = ?", historical.ID, "alpha").Count(&historicalCount).Error; err != nil {
		t.Fatalf("count historical request: %v", err)
	}
	if historicalCount != 1 {
		t.Fatalf("historical request count = %d, want 1", historicalCount)
	}
	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("unchanged refresh: %v", err)
	}
	if syncer.syncCalls != 0 || syncer.applyCalls != 3 || len(syncer.syncForces) != 0 || len(syncer.deletedGroups) != 0 {
		t.Fatalf("gateway sync calls=%d apply=%d forces=%v deleted=%v", syncer.syncCalls, syncer.applyCalls, syncer.syncForces, syncer.deletedGroups)
	}
}

type testGatewaySynchronizer struct {
	store         *storage.Gateway
	syncCalls     int
	applyCalls    int
	syncErr       error
	applyErr      error
	applyFunc     func(context.Context) error
	applyDeadline time.Time
	syncForces    []bool
	deleteErr     error
	deletedGroups map[string]int
}

func (s *testGatewaySynchronizer) SyncChannelModelFamilies(_ context.Context, _ uint, force bool) error {
	s.syncCalls++
	s.syncForces = append(s.syncForces, force)
	return s.syncErr
}

func (s *testGatewaySynchronizer) ApplyUpstreamGroupFacts(ctx context.Context, _ uint) error {
	s.applyCalls++
	if deadline, ok := ctx.Deadline(); ok {
		s.applyDeadline = deadline
	}
	if s.applyFunc != nil {
		return s.applyFunc(ctx)
	}
	return s.applyErr
}

type testProgressObserver struct {
	events []progress.Event
}

func (o *testProgressObserver) Emit(event progress.Event) {
	o.events = append(o.events, event)
}

func (o *testProgressObserver) last(stage progress.Stage) (progress.Event, bool) {
	for index := len(o.events) - 1; index >= 0; index-- {
		if o.events[index].Stage == stage {
			return o.events[index], true
		}
	}
	return progress.Event{}, false
}

func (s *testGatewaySynchronizer) DeleteGroupRoutingState(_ context.Context, channelID uint, groupName string) error {
	if s.deletedGroups == nil {
		s.deletedGroups = make(map[string]int)
	}
	s.deletedGroups[groupName]++
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.store.DeleteGroupRoutingState(channelID, groupName)
}

func TestApplyUpstreamGroupFactsUsesIndependentBudget(t *testing.T) {
	parentCtx, cancelParent := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancelParent()

	syncer := &testGatewaySynchronizer{}
	service := &Service{}
	if err := service.applyUpstreamGroupFacts(parentCtx, syncer, 1); err != nil {
		t.Fatalf("apply upstream group facts: %v", err)
	}
	remaining := time.Until(syncer.applyDeadline)
	if remaining < 7*time.Second || remaining > managedKeyMaintenanceTimeout {
		t.Fatalf("default managed key budget = %v, want about %v", remaining, managedKeyMaintenanceTimeout)
	}

	service.managedKeyTimeout = 20 * time.Millisecond
	syncer.applyFunc = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	started := time.Now()
	err := service.applyUpstreamGroupFacts(parentCtx, syncer, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("managed key timeout error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("managed key timeout elapsed = %v, want under 1s", elapsed)
	}
}

func TestValidateRateSnapshotRejectsUntrustedInput(t *testing.T) {
	tests := []struct {
		name     string
		results  []connector.RateResult
		existing int
	}{
		{name: "empty replacement", existing: 1},
		{name: "blank group", results: []connector.RateResult{{ModelName: "  ", Ratio: 1}}},
		{name: "duplicate group", results: []connector.RateResult{{ModelName: "same", Ratio: 1}, {ModelName: " same ", Ratio: 2}}},
		{name: "negative ratio", results: []connector.RateResult{{ModelName: "bad", Ratio: -1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateRateSnapshot(&connector.RateSnapshotResult{Items: tt.results, Complete: true}, tt.existing); err == nil {
				t.Fatal("validateRateSnapshot succeeded")
			}
		})
	}
	if err := validateRateSnapshot(&connector.RateSnapshotResult{Complete: true}, 0); err != nil {
		t.Fatalf("empty initial snapshot: %v", err)
	}
	if err := validateRateSnapshot(&connector.RateSnapshotResult{Items: []connector.RateResult{{ModelName: "partial", Ratio: 1}}}, 1); err == nil {
		t.Fatal("incomplete non-empty snapshot succeeded")
	}
}

func TestRefreshRatesKeepsRoutingStateOnFailedOrEmptySnapshot(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	var mode atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":7}}`))
		case "/api/user/self/groups":
			if mode.Load() == 0 {
				_, _ = w.Write([]byte(`{"success":true,"data":`))
				return
			}
			if mode.Load() == 2 {
				_, _ = w.Write([]byte(`{"success":true,"data":{"keep":{"ratio":{"bad":true}},"other":{"ratio":1}}}`))
				return
			}
			if mode.Load() == 3 {
				_, _ = w.Write([]byte(`{"success":true,"data":{"keep":{"ratio":1}}}`))
				return
			}
			if mode.Load() == 4 {
				_, _ = w.Write([]byte(`{"success":true,"data":{"other":{"ratio":1}}}`))
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name: "newapi", Type: storage.ChannelTypeNewAPI, SiteURL: apiSrv.URL,
		PasswordCipher: mustEncrypt(t, cipher, `{"access_token":"token","user_id":"7"}`),
		CredentialMode: storage.CredentialModeToken, MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: ch.ID, ModelName: "keep", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("seed rate: %v", err)
	}
	gatewayStore := storage.NewGateway(db)
	if err := gatewayStore.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: ch.ID, GroupName: "keep", APIKeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatalf("seed gateway key: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	syncer := &testGatewaySynchronizer{store: gatewayStore}
	svc.SetGatewaySync(syncer)

	for index, testMode := range []int32{0, 1, 2} {
		mode.Store(testMode)
		if err := svc.RefreshRates(context.Background(), ch); err == nil {
			t.Fatalf("refresh mode %d succeeded", testMode)
		}
		if index == 0 {
			stored, err := channels.FindByID(ch.ID)
			if err != nil {
				t.Fatalf("find failed refresh channel: %v", err)
			}
			if stored.LastError == "" || stored.SyncFailureCount != 0 || stored.NextSyncRetryAt == nil || stored.NextSyncRetryAt.Before(time.Now().Add(4*time.Minute)) {
				t.Fatalf("initial failed rate sync recovery state = %+v", stored)
			}
		}
		if _, err := rates.FindByChannelAndModel(ch.ID, "keep"); err != nil {
			t.Fatalf("rate removed after mode %d: %v", testMode, err)
		}
		if _, err := gatewayStore.FindUpstreamKeyAny(ch.ID, "keep"); err != nil {
			t.Fatalf("gateway key removed after mode %d: %v", testMode, err)
		}
	}
	if len(syncer.deletedGroups) != 0 || syncer.syncCalls != 0 || syncer.applyCalls != 0 {
		t.Fatalf("untrusted snapshots changed gateway state: deleted=%v syncCalls=%d applyCalls=%d", syncer.deletedGroups, syncer.syncCalls, syncer.applyCalls)
	}
	if err := db.Exec(`CREATE FUNCTION fail_keep_rate_delete_fn() RETURNS trigger AS $$
		BEGIN
			IF OLD.channel_id = ` + strconv.FormatUint(uint64(ch.ID), 10) + ` AND OLD.model_name = 'keep' THEN
				RAISE EXCEPTION 'rate delete failed';
			END IF;
			RETURN OLD;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_keep_rate_delete BEFORE DELETE ON rate_snapshots
		FOR EACH ROW EXECUTE FUNCTION fail_keep_rate_delete_fn()`).Error; err != nil {
		t.Fatalf("create delete failure trigger: %v", err)
	}
	mode.Store(4)
	if err := svc.RefreshRates(context.Background(), ch); err == nil {
		t.Fatal("rate snapshot delete failure was reported as success")
	}
	if len(syncer.deletedGroups) != 0 {
		t.Fatalf("gateway cleanup ran after rate delete failure: %v", syncer.deletedGroups)
	}
	if err := db.Exec("DROP TRIGGER fail_keep_rate_delete ON rate_snapshots; DROP FUNCTION fail_keep_rate_delete_fn()").Error; err != nil {
		t.Fatalf("drop delete failure trigger: %v", err)
	}
	mode.Store(4)
	syncer.applyErr = errors.New("gateway cleanup failed")
	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("committed rate snapshot failed on gateway cleanup warning: %v", err)
	}
	if _, err := gatewayStore.FindUpstreamKeyAny(ch.ID, "keep"); err != nil {
		t.Fatalf("gateway key removed despite cleanup failure: %v", err)
	}
	syncer.applyErr = nil
	mode.Store(3)
	syncer.applyErr = errors.New("one managed key group failed")
	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("partial managed key maintenance failed channel refresh: %v", err)
	}
	logs, err := monitorLogs.List(ch.ID, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("list monitor logs: logs=%v err=%v", logs, err)
	}
	if !logs[0].Success || logs[0].ErrorMessage != "" {
		t.Fatalf("partial monitor log = %+v", logs[0])
	}
	recovered, err := channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("find partial-sync channel: %v", err)
	}
	if recovered.LastError != "" || recovered.SyncFailureCount != 0 || recovered.NextSyncRetryAt != nil {
		t.Fatalf("partial model sync polluted channel state = %+v", recovered)
	}

	syncer.applyErr = nil
	syncer.applyFunc = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	svc.managedKeyTimeout = 20 * time.Millisecond
	observer := &testProgressObserver{}
	started := time.Now()
	if err := svc.RefreshRates(progress.WithObserver(context.Background(), observer), ch); err != nil {
		t.Fatalf("managed key timeout failed committed rate refresh: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("managed key timeout refresh elapsed = %v, want under 1s", elapsed)
	}
	if syncer.applyDeadline.IsZero() || syncer.applyDeadline.Sub(started) > 100*time.Millisecond {
		t.Fatalf("managed key deadline = %v, start = %v", syncer.applyDeadline, started)
	}
	if event, ok := observer.last(progress.StageRates); !ok || event.OK == nil || !*event.OK {
		t.Fatalf("rate progress event = %+v, found=%v", event, ok)
	}
	if event, ok := observer.last(progress.StageAPIKeys); !ok || event.OK == nil || *event.OK || !strings.Contains(event.Message, "超时") {
		t.Fatalf("managed key progress event = %+v, found=%v", event, ok)
	}
	recovered, err = channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("find timeout-warning channel: %v", err)
	}
	if recovered.LastError != "" || recovered.SyncFailureCount != 0 || recovered.NextSyncRetryAt != nil {
		t.Fatalf("managed key timeout polluted channel state = %+v", recovered)
	}
	syncer.applyFunc = nil
	svc.managedKeyTimeout = managedKeyMaintenanceTimeout
	previousCalls := syncer.applyCalls
	svc.RecoverDueRateSyncs(context.Background())
	if syncer.applyCalls != previousCalls {
		t.Fatalf("group-scoped managed key error entered channel recovery queue: calls=%d before=%d", syncer.applyCalls, previousCalls)
	}
}

func TestRefreshRatesAfterPendingChannelDeleteDoesNotEmitOldStructureChange(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	var webhookHits atomic.Int32
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()

	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notify channel: %v", err)
	}

	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/settings/public":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"turnstile_enabled":false}}`))
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"balance":50}}`))
		case "/api/v1/groups/available":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[
				{"id":1,"name":"new","description":"n","rate_multiplier":1.0}
			]}`))
		case "/api/v1/groups/rates":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{}}`))
		case "/api/v1/announcements":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	old := &storage.Channel{
		ID:             1,
		Name:           "old",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        apiSrv.URL,
		Username:       "u",
		PasswordCipher: mustEncrypt(t, cipher, `{"access_token":"token"}`),
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}
	if err := channels.Create(old); err != nil {
		t.Fatalf("create old channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID:  old.ID,
		ModelName:  "old",
		Ratio:      1,
		LastSeenAt: time.Now(),
	}); err != nil {
		t.Fatalf("upsert old snapshot: %v", err)
	}
	if err := channels.Delete(old.ID); !errors.Is(err, storage.ErrChannelDeletionPending) {
		t.Fatalf("prepare old channel deletion: %v", err)
	}

	ch := &storage.Channel{
		ID:             old.ID + 1,
		Name:           "new",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        apiSrv.URL,
		Username:       "u",
		PasswordCipher: mustEncrypt(t, cipher, `{"access_token":"token"}`),
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create new channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		BatchRateChanges: true,
		SendMaxAttempts:  1,
	})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	logs, err := notifies.ListLogs(10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	for _, log := range logs {
		if log.Event == storage.EventRateStructureChanged {
			t.Fatalf("unexpected structure change log: %#v", log)
		}
	}
	if got := webhookHits.Load(); got != 0 {
		t.Fatalf("webhook hits = %d, want 0", got)
	}
}

func TestRateEventSubscriptionFiltersGroups(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	var webhookHits atomic.Int32
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()

	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:          "webhook",
		Type:          storage.NotifyWebhook,
		ConfigCipher:  mustEncrypt(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
		Subscriptions: `[{"channel_id":1,"mode":"groups","groups":["beta"]}]`,
	}); err != nil {
		t.Fatalf("create notify channel: %v", err)
	}

	var call atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/settings/public":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"turnstile_enabled":false}}`))
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"balance":50}}`))
		case "/api/v1/usage/dashboard/stats":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"today_actual_cost":1,"total_actual_cost":2}}`))
		case "/api/v1/groups/available":
			n := call.Add(1)
			if n == 1 {
				_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[
					{"id":1,"name":"alpha","description":"a","rate_multiplier":1.0}
				]}`))
				return
			}
			if n == 2 {
				_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[
					{"id":1,"name":"alpha","description":"a","rate_multiplier":1.0},
					{"id":2,"name":"beta","description":"b","rate_multiplier":2.0},
					{"id":3,"name":"gamma","description":"c","rate_multiplier":3.0}
				]}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[
				{"id":1,"name":"alpha","description":"a","rate_multiplier":1.0},
				{"id":3,"name":"gamma","description":"c","rate_multiplier":3.0}
			]}`))
		case "/api/v1/groups/rates":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{}}`))
		case "/api/v1/announcements":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name:                "sub",
		Type:                storage.ChannelTypeSub2API,
		SiteURL:             apiSrv.URL,
		Username:            "u",
		PasswordCipher:      mustEncrypt(t, cipher, `{"access_token":"token"}`),
		CredentialMode:      storage.CredentialModeToken,
		MonitorEnabled:      true,
		SubscriptionEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		BatchRateChanges: true,
		SendMaxAttempts:  1,
	})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if err := svc.RefreshRates(context.Background(), ch); err != nil {
		t.Fatalf("third refresh: %v", err)
	}
	logs, err := notifies.ListLogs(20)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	var foundAddedBeta, foundRemovedBeta bool
	for _, log := range logs {
		if log.Event != storage.EventRateStructureChanged {
			continue
		}
		if strings.Contains(log.Subject, "新增 1 / 删除 0") &&
			strings.Contains(log.Body, "beta") {
			foundAddedBeta = true
		}
		if strings.Contains(log.Subject, "新增 0 / 删除 1") &&
			strings.Contains(log.Body, "beta") {
			foundRemovedBeta = true
		}
	}
	if !foundAddedBeta || !foundRemovedBeta {
		t.Fatalf("logs = %#v", logs)
	}
	if got := webhookHits.Load(); got != 2 {
		t.Fatalf("webhook hits = %d, want 2", got)
	}
}

func TestSubscriptionUsageAlertsAndCooldown(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	var webhookHits atomic.Int32
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		var body struct {
			Event string `json:"event"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode webhook: %v", err)
		}
		if body.Event != string(storage.EventSubscriptionDailyLow) &&
			body.Event != string(storage.EventSubscriptionMonthlyLow) &&
			body.Event != string(storage.EventSubscriptionExpiring) {
			t.Fatalf("unexpected event = %q", body.Event)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()

	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notify channel: %v", err)
	}

	expiresAt := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/settings/public":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"turnstile_enabled":false}}`))
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"balance":50}}`))
		case "/api/v1/usage/dashboard/stats":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"today_actual_cost":1,"total_actual_cost":2}}`))
		case "/api/v1/subscriptions/progress":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"subscription":{"id":9,"group_id":3,"status":"active","starts_at":"2026-01-01T00:00:00Z","expires_at":"` + expiresAt + `","group":{"id":3,"name":"pro"}},"progress":{"id":9,"group_name":"pro","expires_at":"` + expiresAt + `","expires_in_days":0,"daily":{"limit_usd":10,"used_usd":9,"remaining_usd":1,"percentage":90,"window_start":"2026-01-01T00:00:00Z","resets_at":"2026-01-02T00:00:00Z","resets_in_seconds":3600},"weekly":{"limit_usd":0,"used_usd":0,"remaining_usd":0,"percentage":0},"monthly":{"limit_usd":100,"used_usd":96,"remaining_usd":4,"percentage":96,"window_start":"2026-01-01T00:00:00Z","resets_at":"2026-02-01T00:00:00Z","resets_in_seconds":3600}}}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name:                "sub",
		Type:                storage.ChannelTypeSub2API,
		SiteURL:             apiSrv.URL,
		Username:            "u",
		PasswordCipher:      mustEncrypt(t, cipher, `{"access_token":"token"}`),
		CredentialMode:      storage.CredentialModeToken,
		MonitorEnabled:      true,
		SubscriptionEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		SubscriptionDailyRemainingThresholdPct:   15,
		SubscriptionWeeklyRemainingThresholdPct:  15,
		SubscriptionMonthlyRemainingThresholdPct: 5,
		SubscriptionExpiryThreshold:              3 * time.Hour,
		SubscriptionAlertCooldown:                time.Hour,
		SendMaxAttempts:                          1,
	})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))

	svc.ScanAllBalances(context.Background())
	if got := webhookHits.Load(); got != 3 {
		t.Fatalf("webhook hits = %d, want 3", got)
	}
	svc.ScanAllBalances(context.Background())
	if got := webhookHits.Load(); got != 3 {
		t.Fatalf("webhook hits after cooldown = %d, want 3", got)
	}

	logs, err := notifies.ListLogs(10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	seen := map[storage.NotificationEvent]bool{}
	for _, log := range logs {
		seen[log.Event] = true
	}
	if !seen[storage.EventSubscriptionDailyLow] || seen[storage.EventSubscriptionWeeklyLow] || !seen[storage.EventSubscriptionMonthlyLow] || !seen[storage.EventSubscriptionExpiring] {
		t.Fatalf("events = %#v", seen)
	}
}

func TestSubscriptionUsageAlertsSupportsNewAPI(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	var webhookHits atomic.Int32
	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()
	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notify channel: %v", err)
	}

	var subscriptionCalls atomic.Int32
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":7}}`))
		case "/api/subscription/self":
			subscriptionCalls.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"data":{"subscriptions":[{"subscription":{"id":5,"plan_id":2,"amount_total":10000000,"amount_used":9000000,"start_time":1700000000,"end_time":4102444800,"status":"active","next_reset_time":4102444800}}]}}`))
		case "/api/subscription/plans":
			_, _ = w.Write([]byte(`{"success":true,"data":[{"plan":{"id":2,"title":"Pro","quota_reset_period":"daily"}}]}`))
		case "/api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":1000000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name:                "newapi-subscription",
		Type:                storage.ChannelTypeNewAPI,
		SiteURL:             apiSrv.URL,
		PasswordCipher:      mustEncrypt(t, cipher, `{"access_token":"token","user_id":"7"}`),
		CredentialMode:      storage.CredentialModeToken,
		MonitorEnabled:      true,
		SubscriptionEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		SubscriptionDailyRemainingThresholdPct: 15,
		SendMaxAttempts:                        1,
	})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := svc.CheckSubscriptionUsageAlerts(context.Background(), ch); err != nil {
		t.Fatalf("CheckSubscriptionUsageAlerts: %v", err)
	}
	if got := subscriptionCalls.Load(); got != 1 {
		t.Fatalf("subscription calls = %d, want 1", got)
	}
	if got := webhookHits.Load(); got != 1 {
		t.Fatalf("webhook hits = %d, want 1", got)
	}
}

func mustEncrypt(t *testing.T, cipher *crypto.Cipher, plain string) string {
	t.Helper()
	out, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	return out
}

func jsonNumber(v int) string {
	b, _ := json.Marshal(v)
	return string(b)
}
