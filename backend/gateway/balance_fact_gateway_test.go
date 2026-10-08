package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestCurrentBalanceFactBlocksRoutingButNotControlPlaneProbes(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-test"}]}`))
	}))
	defer upstream.Close()

	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("gateway-balance-fact-gates-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "balance-gated", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: upstream.URL, MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: now}); err != nil {
		t.Fatal(err)
	}
	keyCipher, err := cipher.Encrypt("sk-balance-gated")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", GroupRatio: 1,
		APIKeyCipher: keyCipher, APIKeyPrefix: "sk-gated", UpstreamKeyID: 1, UpstreamKeyName: "BOG gated",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		ModelFamilies:       "codex", ModelIDs: "gpt-test", LatencyProbeModel: "gpt-test",
		ModelsSyncedAt: &now, Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "BOG gated", now); err != nil {
		t.Fatal(err)
	}
	group := &storage.GatewayCandidateGroup{Name: "balance gated"}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "primary"}}); err != nil {
		t.Fatal(err)
	}
	if err := channels.MarkUpstreamAccountBalanceInsufficient(channel.ID, channel.UpstreamAccountIdentityRevision, now); err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, rates, store, cipher, &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "primary", Ratio: 1}},
	}, nil)

	if candidates, err := service.CandidatesForApp(storage.GatewayAppCodex, "gpt-test"); err == nil || len(candidates) != 0 {
		t.Fatalf("smart candidates=%+v err=%v, want account-balance gate", candidates, err)
	}
	if targets, err := service.DiscoverTargets(context.Background(), now); err != nil || len(targets) != 1 {
		t.Fatalf("model-check targets=%+v err=%v, want account-balance target", targets, err)
	}
	if _, err := service.RunProbe(context.Background(), modelcheck.TargetDescriptor{
		ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "primary", ModelName: "gpt-test", App: storage.GatewayAppCodex,
	}, modelcheck.ProbeRequest{Prompt: "test", MaxTokens: 8}); err == nil || err.Error() == modelcheck.ProbeErrorTargetUnavailable {
		t.Fatalf("model-check probe error=%v, want upstream response after network", err)
	}
	items, err := service.SyncModelFamilies(context.Background(), channel.ID)
	if err != nil || len(items) != 1 || items[0].Skipped {
		t.Fatalf("model sync items=%+v err=%v, want active account-balance control-plane sync", items, err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	healthFence := storage.NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
	for i := 0; i < 3; i++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(healthFence, RoutingFailureTransport, 0, "existing cooldown", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	results, err := service.RunLatencyProbes(context.Background(), LatencyProbeOptions{
		MaxCandidates: 1, Timeout: time.Second, Interval: time.Minute, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ChannelID != channel.ID {
		t.Fatalf("latency probe results=%+v, want balance-gated cooldown recovery probe", results)
	}
	manual, err := service.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "primary", "gpt-test")
	if err != nil || len(manual) != 1 || manual[0].Channel.ID != channel.ID {
		t.Fatalf("manual route did not bypass account-balance policy: candidates=%+v err=%v", manual, err)
	}
	if requests.Load() == 0 {
		t.Fatalf("control-plane probes made no network requests")
	}
}

func TestSuccessfulGatewayProbesDoNotClearBalanceFactObservedInFlight(t *testing.T) {
	var requests atomic.Int32
	var channels *storage.Channels
	var channel storage.Channel
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		if err := channels.MarkUpstreamAccountBalanceInsufficient(
			channel.ID, channel.UpstreamAccountIdentityRevision, time.Now().UTC(),
		); err != nil {
			t.Errorf("mark in-flight balance fact: %v", err)
			http.Error(w, "failed to record test fact", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"gpt-test","choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer upstream.Close()

	db := testdb.Open(t)
	channels = storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("gateway-balance-probe-success-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel = storage.Channel{
		Name: "probe-success", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: upstream.URL, MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(&channel); err != nil {
		t.Fatal(err)
	}
	keyCipher, err := cipher.Encrypt("sk-probe-success")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", GroupRatio: 1,
		APIKeyCipher: keyCipher, APIKeyPrefix: "sk-probe", UpstreamKeyID: 1, UpstreamKeyName: "BOG probe",
		ModelFamilies: "codex", ModelIDs: "gpt-test", LatencyProbeModel: "gpt-test",
		Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	generation := recordModelCheckGeneration(t, store, channel.ID, "primary")
	service := NewService(channels, rates, store, cipher, nil, nil)

	result, err := service.RunProbe(context.Background(), modelcheck.TargetDescriptor{
		ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "primary", ModelName: "gpt-test", App: storage.GatewayAppCodex,
	}, modelcheck.ProbeRequest{Prompt: "test", MaxTokens: 8})
	if err != nil || result.Text != "ok" {
		t.Fatalf("successful model-check probe result=%+v err=%v", result, err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil || !stored.HasCurrentUpstreamAccountBalanceInsufficient() {
		t.Fatalf("model-check success cleared in-flight balance fact: channel=%+v err=%v", stored, err)
	}
	if err := channels.ClearUpstreamAccountBalanceInsufficient(channel.ID, channel.UpstreamAccountIdentityRevision); err != nil {
		t.Fatal(err)
	}

	candidate := Candidate{
		Channel:            channel,
		Rate:               storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1},
		UpstreamGeneration: generation, App: storage.GatewayAppCodex,
		ModelIDs: []string{"gpt-test"}, LatencyProbeModel: "gpt-test", RawRatio: 1,
	}
	latency := service.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, time.Second)
	if latency.StatusCode != http.StatusOK || latency.Error != "" {
		t.Fatalf("successful latency probe=%+v", latency)
	}
	stored, err = channels.FindByID(channel.ID)
	if err != nil || !stored.HasCurrentUpstreamAccountBalanceInsufficient() {
		t.Fatalf("latency success cleared in-flight balance fact: channel=%+v err=%v", stored, err)
	}
	if want := int32(1 + gatewayLatencyProbeAttempts); requests.Load() != want {
		t.Fatalf("probe requests=%d, want %d", requests.Load(), want)
	}
}
