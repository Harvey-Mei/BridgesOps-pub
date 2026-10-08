package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestStaleChannelErrorNotificationDoesNotCrossIdentityRevision(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	notifications := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("identity-error-notification-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	var webhookHits atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		webhookHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhook.Close()
	if err := notifications.CreateChannel(&storage.NotificationChannel{
		Name: "webhook", Type: storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhook.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}

	stale := &storage.Channel{
		Name: "account-a", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid",
		Username: "a", PasswordCipher: "credential-a", CredentialMode: storage.CredentialModeToken,
	}
	if err := channels.Create(stale); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	current := *stale
	current.PasswordCipher = "credential-b"
	current.Name = "account-b"
	if err := channels.UpdateWithUpstreamAccountIdentityChange(&current); err != nil {
		t.Fatalf("switch account identity: %v", err)
	}

	dispatcher := notify.NewDispatcher(notifications, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	svc := NewService(channels, nil, nil, nil, nil, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.notifyError(context.Background(), stale, storage.EventMonitorFailed, "旧账号采集失败", errors.New("delayed account-a error"))
	if got := webhookHits.Load(); got != 0 {
		t.Fatalf("stale account notification hits = %d, want 0", got)
	}
	svc.notifyError(context.Background(), &current, storage.EventMonitorFailed, "新账号采集失败", errors.New("current account-b error"))
	if got := webhookHits.Load(); got != 1 {
		t.Fatalf("current account notification hits = %d, want 1", got)
	}
}

func TestChannelIdentityDispatchHasBoundedDeadline(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	c := &storage.Channel{
		Name: "bounded-notification", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://example.invalid", PasswordCipher: "credential",
	}
	if err := channels.Create(c); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	svc := NewService(channels, nil, nil, nil, nil, &notify.Dispatcher{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.identityDispatchTimeout = 50 * time.Millisecond
	startedAt := time.Now()
	err := svc.dispatchWithChannelIdentity(context.Background(), c, func(ctx context.Context) error {
		if !storage.ChannelOperationLockHeld(ctx, c.ID) {
			t.Fatal("dispatch context did not carry channel operation lock ownership")
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded dispatch error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("bounded dispatch returned after %s", elapsed)
	}
}

func TestDelayedAnnouncementCannotPersistOrSuppressNextIdentity(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	notifications := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("identity-announcement-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	var webhookHits atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		webhookHits.Add(1)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhook.Close()
	if err := notifications.CreateChannel(&storage.NotificationChannel{
		Name: "webhook", Type: storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhook.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var statusCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/status":
			call := statusCalls.Add(1)
			if call == 1 {
				close(firstStarted)
				<-releaseFirst
				_, _ = w.Write([]byte(`{"success":true,"data":{"announcements":[{"id":101,"title":"account-a","content":"stale","type":"warning"}]}}`))
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"announcements":[{"id":202,"title":"account-b","content":"current","type":"notice"}]}}`))
		case "/api/notice":
			_, _ = w.Write([]byte(`{"success":true,"data":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	stale := &storage.Channel{
		Name: "announcement-account", Type: storage.ChannelTypeNewAPI, SiteURL: upstream.URL,
		PasswordCipher: "credential-a", CredentialMode: storage.CredentialModeToken,
	}
	if err := channels.Create(stale); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := announcements.Sync(stale.ID, []storage.UpstreamAnnouncement{{SourceKey: "baseline-a", Title: "baseline"}}); err != nil {
		t.Fatalf("seed account-a announcement: %v", err)
	}
	dispatcher := notify.NewDispatcher(notifications, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	svc := NewService(channels, announcements, nil, nil, nil, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	conn, err := connector.For(connector.TypeNewAPI)
	if err != nil {
		t.Fatalf("newapi connector: %v", err)
	}
	resolved := &connector.Channel{ID: stale.ID, Type: connector.TypeNewAPI, SiteURL: upstream.URL}
	result := make(chan error, 1)
	go func() {
		result <- svc.syncAnnouncements(context.Background(), stale, resolved, conn, &connector.AuthSession{})
	}()
	<-firstStarted
	current := *stale
	current.PasswordCipher = "credential-b"
	if err := channels.UpdateWithUpstreamAccountIdentityChange(&current); err != nil {
		t.Fatalf("switch account identity: %v", err)
	}
	close(releaseFirst)
	if err := <-result; !errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale announcement result error = %v", err)
	}
	if count, err := announcements.CountByChannel(stale.ID); err != nil || count != 0 {
		t.Fatalf("stale announcements count = %d, err=%v", count, err)
	}
	if got := webhookHits.Load(); got != 0 {
		t.Fatalf("stale announcement webhook hits = %d, want 0", got)
	}

	svc.cadenceMu.Lock()
	svc.announcementAt[channelIdentityRevisionKey{ChannelID: current.ID, Revision: current.UpstreamAccountIdentityRevision + 1}] = time.Now()
	svc.cadenceMu.Unlock()
	if err := svc.syncAnnouncements(context.Background(), &current, resolved, conn, &connector.AuthSession{}); err != nil {
		t.Fatalf("sync current identity announcement: %v", err)
	}
	if got := statusCalls.Load(); got != 2 {
		t.Fatalf("announcement status calls = %d, want 2", got)
	}
	items, err := announcements.ListLatest(10)
	if err != nil || len(items) != 1 || items[0].Title != "account-b" {
		t.Fatalf("current announcements = %+v, err=%v", items, err)
	}
	svc.cadenceMu.Lock()
	_, currentCadence := svc.announcementAt[channelIdentityRevisionKey{ChannelID: current.ID, Revision: current.UpstreamAccountIdentityRevision}]
	cadenceEntries := len(svc.announcementAt)
	svc.cadenceMu.Unlock()
	if !currentCadence || cadenceEntries != 1 {
		t.Fatalf("announcement cadence entries = %d, current=%t, want one current identity", cadenceEntries, currentCadence)
	}
}

func TestDelayedSubscriptionAlertCannotClaimNextIdentityCooldown(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifications := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("identity-subscription-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	var webhookHits atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookHits.Add(1)
		var body struct {
			Event string `json:"event"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode webhook: %v", err)
		}
		if body.Event != string(storage.EventSubscriptionDailyLow) {
			t.Fatalf("webhook event = %q", body.Event)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhook.Close()
	if err := notifications.CreateChannel(&storage.NotificationChannel{
		Name: "webhook", Type: storage.NotifyWebhook,
		ConfigCipher: mustEncrypt(t, cipher, `{"url":"`+webhook.URL+`"}`),
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var progressCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/settings/public":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"turnstile_enabled":false}}`))
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":7,"balance":50}}`))
		case "/api/v1/subscriptions/progress":
			if progressCalls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
			}
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"subscription":{"id":9,"group_id":3,"status":"active","group":{"id":3,"name":"pro"}},"progress":{"id":9,"group_name":"pro","daily":{"limit_usd":10,"used_usd":9,"remaining_usd":1,"percentage":90}}}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	stale := &storage.Channel{
		Name: "subscription-account", Type: storage.ChannelTypeSub2API, SiteURL: upstream.URL, APIURL: upstream.URL,
		PasswordCipher: mustEncrypt(t, cipher, `{"access_token":"token-a"}`),
		CredentialMode: storage.CredentialModeToken, MonitorEnabled: true, SubscriptionEnabled: true,
	}
	if err := channels.Create(stale); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if claimed, err := notifications.TryClaimCooldown(stale.ID, storage.EventSubscriptionDailyLow, time.Hour); err != nil || !claimed {
		t.Fatalf("seed account-a subscription cooldown: claimed=%t err=%v", claimed, err)
	}
	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifications, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		SubscriptionDailyRemainingThresholdPct: 15,
		SubscriptionAlertCooldown:              time.Hour,
		SendMaxAttempts:                        1,
	})
	svc := NewService(channels, nil, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	result := make(chan error, 1)
	go func() {
		result <- svc.CheckSubscriptionUsageAlerts(context.Background(), stale)
	}()
	<-firstStarted
	current := *stale
	current.PasswordCipher = mustEncrypt(t, cipher, `{"access_token":"token-b"}`)
	if err := channels.UpdateWithUpstreamAccountIdentityChange(&current); err != nil {
		t.Fatalf("switch account identity: %v", err)
	}
	close(releaseFirst)
	if err := <-result; !errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale subscription result error = %v", err)
	}
	if got := webhookHits.Load(); got != 0 {
		t.Fatalf("stale subscription webhook hits = %d, want 0", got)
	}
	if err := svc.CheckSubscriptionUsageAlerts(context.Background(), &current); err != nil {
		t.Fatalf("current subscription alert: %v", err)
	}
	if got := webhookHits.Load(); got != 1 {
		t.Fatalf("current subscription webhook hits = %d, want 1", got)
	}
}
