package monitor

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// TestRefreshBalanceBindsFirstUpstreamAccountIdentity covers the bootstrap of a
// brand-new Sub2API channel. The account-identity freeze taken before financial
// sampling has nothing to compare against on the very first sample, because the
// binding is created by that same sample. Treating the missing binding as a
// failure deadlocked every newly created Sub2API channel: it could never
// complete a balance refresh, and the resulting error was not even classified
// as an accounting gate, so it also suppressed unrelated monitoring.
func TestRefreshBalanceBindsFirstUpstreamAccountIdentity(t *testing.T) {
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

	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/settings/public":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"turnstile_enabled":false}}`))
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":4242,"balance":50}}`))
		case "/api/v1/payment/checkout-info":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"balance_recharge_multiplier":1}}`))
		case "/api/v1/usage/dashboard/stats":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"today_actual_cost":2,"total_actual_cost":30}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	ch := &storage.Channel{
		Name: "sub2api-bootstrap", Type: storage.ChannelTypeSub2API,
		SiteURL: apiSrv.URL, Username: "u",
		PasswordCipher: mustEncrypt(t, cipher, `{"access_token":"token"}`),
		CredentialMode: storage.CredentialModeToken, MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatal(err)
	}
	if _, err := channels.CurrentUpstreamAccountIdentity(ch.ID); err == nil {
		t.Fatal("a freshly created channel must start without an account binding")
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{SendMaxAttempts: 1})
	svc := NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))
	costingSvc := costing.NewService(storage.NewCosting(db))
	svc.SetCosting(costingSvc)

	if err := svc.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatalf("first refresh of a new Sub2API channel: %v", err)
	}

	identity, err := channels.CurrentUpstreamAccountIdentity(ch.ID)
	if err != nil {
		t.Fatalf("first sample did not bind an upstream account: %v", err)
	}
	if identity.ChannelIdentityRevision != ch.UpstreamAccountIdentityRevision {
		t.Fatalf("first binding does not match the resolved channel identity: channel=%+v identity=%+v", ch, identity)
	}

	state, err := costingSvc.State(ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.RawBalance != 50 || state.RawTotalCost != 30 || state.RawTodayCost != 2 {
		t.Fatalf("bootstrap cost state = %+v", state)
	}

	// A second refresh must still be fenced against the now-existing binding.
	if err := svc.RefreshBalance(context.Background(), ch); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	stored, err := channels.FindByID(ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.UpstreamAccountIdentityRevision != identity.ChannelIdentityRevision || stored.CostUnitEpoch != ch.CostUnitEpoch {
		t.Fatalf("stable account produced an identity change: %+v", stored)
	}
	if stored.LastError != "" {
		t.Fatalf("bootstrap left a visible sync error: %q", stored.LastError)
	}
}
