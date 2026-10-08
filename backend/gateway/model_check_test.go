package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestModelCheckFallbackIgnoresLegacyLatencyProbeModel(t *testing.T) {
	source := storage.GatewayModelCheckSource{
		App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-large", "gpt-mini"},
		LatencyProbeModel: "gpt-large",
	}
	if got := modelCheckFallbackModel(source); got != "gpt-mini" {
		t.Fatalf("model-check fallback = %q, want synchronized low-cost model", got)
	}
}

func TestModelCheckFallbackUsesOnlyModelsCompatibleWithGatewayApp(t *testing.T) {
	tests := []struct {
		name   string
		app    storage.GatewayApp
		models []string
		want   string
	}{
		{name: "claude", app: storage.GatewayAppClaude, models: []string{"gpt-mini", "claude-haiku"}, want: "claude-haiku"},
		{name: "gemini", app: storage.GatewayAppGemini, models: []string{"gpt-mini", "gemini-flash"}, want: "gemini-flash"},
		{name: "no compatible model", app: storage.GatewayAppClaude, models: []string{"gpt-mini", "gemini-flash"}, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := storage.GatewayModelCheckSource{App: test.app, ModelIDs: test.models}
			if got := modelCheckFallbackModel(source); got != test.want {
				t.Fatalf("model-check fallback = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDiscoverModelCheckTargetsIncludesLowBalanceChannels(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	healthy := &storage.Channel{Name: "healthy", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: "https://healthy.invalid", MonitorEnabled: true}
	low := &storage.Channel{Name: "low", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: "https://low.invalid", MonitorEnabled: true}
	for _, channel := range []*storage.Channel{healthy, low} {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel %s: %v", channel.Name, err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
			t.Fatalf("upsert rate %s: %v", channel.Name, err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: "gpt", UpstreamKeyID: 1, UpstreamKeyName: "BOG model check",
			OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
			APIKeyCipher:        "cipher", APIKeyPrefix: "sk-test",
			ModelFamilies: "codex", ModelIDs: "gpt-5-mini,gpt-5", LatencyProbeModel: "gpt-5-mini",
			LatencyProbeEnabled: true, Enabled: true,
		}); err != nil {
			t.Fatalf("upsert key %s: %v", channel.Name, err)
		}
	}
	if err := channels.UpdateBalance(healthy.ID, 10, time.Now(), ""); err != nil {
		t.Fatalf("set healthy balance: %v", err)
	}
	if err := channels.UpdateBalance(low.ID, 0.05, time.Now(), ""); err != nil {
		t.Fatalf("set low balance: %v", err)
	}
	group := &storage.GatewayCandidateGroup{Name: "active"}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{
		{ChannelID: healthy.ID, GroupName: "gpt"},
		{ChannelID: low.ID, GroupName: "gpt"},
	}); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}

	service := NewService(channels, rates, store, nil, nil, nil)
	targets, err := service.DiscoverTargets(context.Background(), time.Now())
	if err != nil {
		t.Fatalf("discover targets: %v", err)
	}
	if len(targets) != 2 || targets[0].ChannelID != healthy.ID || targets[1].ChannelID != low.ID {
		t.Fatalf("targets = %+v, want healthy and low-balance channels", targets)
	}
	manual, err := service.DiscoverManualTargets(context.Background(), time.Now())
	if err != nil || len(manual) != 2 {
		t.Fatalf("manual inventory = %+v err=%v", manual, err)
	}
	for _, channel := range []*storage.Channel{healthy, low} {
		found := false
		for _, option := range manual {
			if option.ChannelID == channel.ID && option.ModelName == "" && option.GroupName == "gpt" && option.UpstreamAccountIdentityRevision == channel.UpstreamAccountIdentityRevision {
				found = true
			}
		}
		if !found {
			t.Fatal("manual inventory omitted an eligible group")
		}
	}

}

func TestRunModelCheckProbeStillRunsForLowBalance(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	rates := storage.NewRates(db)
	cipher, err := appcrypto.NewCipher("model-check-low-balance-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	channel := &storage.Channel{Name: "low", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := channels.UpdateBalance(channel.ID, 0.05, time.Now(), ""); err != nil {
		t.Fatalf("set low balance: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	ciphertext, err := cipher.Encrypt("sk-low-balance")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "gpt", UpstreamKeyID: 1, UpstreamKeyName: "BOG low-balance",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: ciphertext,
		APIKeyPrefix: "sk-low", ModelFamilies: "codex", ModelIDs: "gpt-5", LatencyProbeModel: "gpt-5",
		Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("upsert key: %v", err)
	}
	recordModelCheckGeneration(t, store, channel.ID, "gpt")
	service := NewService(channels, rates, store, cipher, nil, nil)
	_, err = service.RunProbe(context.Background(), modelcheck.TargetDescriptor{
		ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex,
	}, modelcheck.ProbeRequest{Prompt: "test", MaxTokens: 8})
	if err == nil || err.Error() == modelcheck.ProbeErrorTargetUnavailable {
		t.Fatalf("run probe error = %v, want upstream response after network", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("network requests = %d, want one probe request", requests.Load())
	}
}

func TestRunModelCheckProbeReturnsOnlyHTTPFailureCategory(t *testing.T) {
	const upstreamKey = "short-key"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("proxy failure Proxy-Authorization: Basic dXNlcjpwYXNz Authorization: Bearer t1 https://user:password@proxy.invalid/check?token=q1&password=p1"))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("model-check-http-error-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	channel := &storage.Channel{Name: "http-error", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := channels.UpdateBalance(channel.ID, 10, time.Now(), ""); err != nil {
		t.Fatalf("set balance: %v", err)
	}
	ciphertext, err := cipher.Encrypt(upstreamKey)
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "gpt", GroupRatio: 1,
		UpstreamKeyID: 1, UpstreamKeyName: "managed-model-check-key",
		APIKeyCipher: ciphertext, APIKeyPrefix: "test", Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	recordModelCheckGeneration(t, store, channel.ID, "gpt")

	service := NewService(channels, storage.NewRates(db), store, cipher, nil, nil)
	_, err = service.RunProbe(context.Background(), modelcheck.TargetDescriptor{
		ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex,
	}, modelcheck.ProbeRequest{Prompt: "test", MaxTokens: 8})
	var probeErr *modelcheck.ProbeError
	if !errors.As(err, &probeErr) {
		t.Fatalf("run probe error = %v, want ProbeError", err)
	}
	if probeErr.Category != modelcheck.ProbeErrorHTTPStatus || probeErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("probe error = %+v", probeErr)
	}
	if got := err.Error(); got != "http_status (HTTP 502)" {
		t.Fatalf("probe error text = %q", got)
	}
	for _, secret := range []string{"dXNlcjpwYXNz", "t1", "q1", "p1", "password", "Basic", "Proxy-Authorization"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("probe error leaked %q: %q", secret, err)
		}
	}
}

func TestRunModelCheckProbeIgnoresLatencyProbeSwitchAndPreservesRoutingHealth(t *testing.T) {
	const upstreamKey = "model-check-key"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-5","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("model-check-probe-switch-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	channel := &storage.Channel{Name: "probe-paused", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	ciphertext, err := cipher.Encrypt(upstreamKey)
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "gpt", GroupRatio: 1,
		UpstreamKeyID: 1, UpstreamKeyName: "managed-model-check-key",
		APIKeyCipher: ciphertext, APIKeyPrefix: "test", ModelFamilies: "codex", ModelIDs: "gpt-5",
		Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	recordModelCheckGeneration(t, store, channel.ID, "gpt")
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "gpt", false); err != nil {
		t.Fatalf("pause latency probe: %v", err)
	}
	failureAt := time.Now().Add(-time.Minute)
	if err := store.RecordUpstreamKeyRoutingFailure(channel.ID, "gpt", RoutingFailureTransport, 0, "existing inference failure", failureAt); err != nil {
		t.Fatalf("seed routing failure: %v", err)
	}
	before, err := store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("read routing health: %v", err)
	}

	service := NewService(channels, storage.NewRates(db), store, cipher, nil, nil)
	result, err := service.RunProbe(context.Background(), modelcheck.TargetDescriptor{
		ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex,
	}, modelcheck.ProbeRequest{Prompt: "test", MaxTokens: 8})
	if err != nil || result.Text != "ok" {
		t.Fatalf("run paused model check result=%+v err=%v", result, err)
	}
	after, err := store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil || after.LastRoutingFailureKind != before.LastRoutingFailureKind || after.LastRoutingFailureStatus != before.LastRoutingFailureStatus || after.RoutingFailureCount != before.RoutingFailureCount || after.LastRoutingFailureAt == nil || before.LastRoutingFailureAt == nil || !after.LastRoutingFailureAt.Equal(*before.LastRoutingFailureAt) {
		t.Fatalf("model check changed inference health: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestRunModelCheckProbeAccountConcurrencyRejectionSkipsNetworkAndRoutingHealth(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-5","choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("model-check-account-concurrency-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "model-check-budget", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	ciphertext, err := cipher.Encrypt("model-check-budget-key")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "gpt", GroupRatio: 1,
		UpstreamKeyID: 1, UpstreamKeyName: "BOG model check",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        ciphertext, APIKeyPrefix: "test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	generation := recordModelCheckGeneration(t, store, channel.ID, "gpt")
	service := NewService(channels, storage.NewRates(db), store, cipher, nil, nil)
	active, decision := service.TryAcquireCandidateAccountConcurrency(Candidate{
		Channel: *channel, Rate: storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt"}, UpstreamGeneration: generation,
	}, AccountConcurrencySmartData)
	if active == nil || !decision.Admitted {
		t.Fatalf("active data-plane decision = %#v", decision)
	}
	defer active.Release()

	_, err = service.RunProbe(context.Background(), modelcheck.TargetDescriptor{
		ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex,
	}, modelcheck.ProbeRequest{Prompt: "test", MaxTokens: 8})
	if !errors.Is(err, modelcheck.ErrProbeDeferred) {
		t.Fatalf("run probe error = %v, want deferred", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("network requests = %d, want 0", requests.Load())
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastRoutingFailureAt != nil || stored.RoutingCooldownUntil != nil || stored.RoutingFailureCount != 0 {
		t.Fatalf("admission rejection polluted routing health: %+v", stored)
	}
}

func TestModelCheckTransportProbeErrorClassifiesContextWithoutDetails(t *testing.T) {
	timeoutCtx, cancelTimeout := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelTimeout()
	<-timeoutCtx.Done()
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, test := range []struct {
		name     string
		ctx      context.Context
		err      error
		category string
	}{
		{name: "deadline", ctx: timeoutCtx, err: errors.New("dial proxy secret=value"), category: modelcheck.ProbeErrorTimeout},
		{name: "canceled", ctx: canceledCtx, err: errors.New("request canceled token=secret"), category: modelcheck.ProbeErrorCanceled},
		{name: "network", ctx: context.Background(), err: errors.New("proxy failure password=secret"), category: modelcheck.ProbeErrorNetwork},
	} {
		t.Run(test.name, func(t *testing.T) {
			var probeErr *modelcheck.ProbeError
			if err := modelCheckTransportProbeError(test.ctx, test.err); !errors.As(err, &probeErr) || probeErr.Category != test.category {
				t.Fatalf("probe error = %#v, want category %q", err, test.category)
			}
			if strings.Contains(probeErr.Error(), "secret") {
				t.Fatalf("transport diagnostic leaked detail: %q", probeErr.Error())
			}
		})
	}
}

func recordModelCheckGeneration(t *testing.T, store *storage.Gateway, channelID uint, groupName string) storage.UpstreamGroupGeneration {
	t.Helper()
	if err := store.RecordUpstreamGroupChange(channelID, groupName, "added", 1, "BOG model check", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channelID, groupName)
	if err != nil {
		t.Fatal(err)
	}
	return generation
}
