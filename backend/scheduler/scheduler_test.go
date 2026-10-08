package scheduler

import (
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/monitor"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	return testdb.Open(t)
}

type schedulerModelCheckRunner struct {
	calls            int
	channelID        uint
	identityRevision uint64
}

func (r *schedulerModelCheckRunner) DiscoverTargets(context.Context, time.Time) ([]modelcheck.TargetDescriptor, error) {
	return []modelcheck.TargetDescriptor{{
		ChannelID: r.channelID, UpstreamAccountIdentityRevision: r.identityRevision,
		ChannelName: "Scheduler", GroupName: "gpt", ModelName: "gpt-5", App: storage.GatewayAppCodex,
	}}, nil
}

func (r *schedulerModelCheckRunner) RunProbe(_ context.Context, target modelcheck.TargetDescriptor, _ modelcheck.ProbeRequest) (modelcheck.ProbeResponse, error) {
	r.calls++
	return modelcheck.ProbeResponse{Text: `{}`, ActualModel: target.ModelName, LatencyMS: 10}, nil
}

func TestRunModelCheckPersistsScheduledBatch(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewModelChecks(db)
	channels := storage.NewChannels(db)
	channel := &storage.Channel{
		Name: "scheduler-model-check", Type: storage.ChannelTypeNewAPI,
		GatewayApp: storage.GatewayAppCodex, SiteURL: "https://scheduler-model-check.invalid",
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	runner := &schedulerModelCheckRunner{channelID: channel.ID, identityRevision: channel.UpstreamAccountIdentityRevision}
	service := modelcheck.New(modelcheck.Options{Store: store, Runner: runner})
	cfg := config.ModelCheckConfig{
		Enabled: true, RunsPerDay: 3, Concurrency: 1, MaxTargetsPerRun: 2,
		ProbesPerTarget: 1, RequestBudget24h: 10, TimeoutSeconds: 30, AlertFailures: 2, HistoryDays: 90,
	}
	scheduler := New(config.SchedulerConfig{ModelCheck: cfg}, nil, nil, nil, nil, nil, nil, nil, nil, config.ProxyConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	scheduler.SetModelCheck(service)
	scheduler.runModelCheck(context.Background(), cfg)

	targets, err := store.ListTargets(true)
	if err != nil || len(targets) != 1 {
		t.Fatalf("scheduled targets = %+v, %v", targets, err)
	}
	runs, total, err := store.ListRunsPage(targets[0].ID, 1, 10)
	if err != nil || total != 1 || len(runs) != 1 || runner.calls != 2 || runs[0].RequestCount != 2 {
		t.Fatalf("scheduled runs total=%d runs=%+v calls=%d err=%v", total, runs, runner.calls, err)
	}
}

func TestLatencyProbeRunTimeoutIncludesAppsAndConfirmationAttempts(t *testing.T) {
	got := latencyProbeRunTimeout(30*time.Second, 5)
	probeBudget := 30 * time.Second * 5 * latencyProbeGatewayAppCount * gateway.LatencyProbeConfirmationAttempts
	want := probeBudget + probeBudget/10
	if got != want {
		t.Fatalf("latency probe run timeout = %s, want %s", got, want)
	}
}

func TestLatencyProbeRunTimeoutUsesGatewayCandidateDefault(t *testing.T) {
	want := latencyProbeRunTimeout(30*time.Second, gateway.DefaultLatencyProbeMaxCandidates)
	for _, maxCandidates := range []int{0, -1, math.MinInt} {
		if got := latencyProbeRunTimeout(30*time.Second, maxCandidates); got != want {
			t.Errorf("latency probe run timeout for maxCandidates=%d = %s, want default %s", maxCandidates, got, want)
		}
	}
}

func TestLatencyProbeRunTimeoutSaturatesExtremeInputs(t *testing.T) {
	tests := []struct {
		name          string
		timeout       time.Duration
		maxCandidates int
	}{
		{name: "maximum candidate count", timeout: 30 * time.Second, maxCandidates: math.MaxInt},
		{name: "maximum attempt timeout", timeout: time.Duration(math.MaxInt64), maxCandidates: 1},
		{
			name:          "management overhead addition",
			timeout:       time.Duration(math.MaxInt64 / (latencyProbeGatewayAppCount * gateway.LatencyProbeConfirmationAttempts)),
			maxCandidates: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := latencyProbeRunTimeout(tt.timeout, tt.maxCandidates)
			if got != latencyProbeMaxRunTimeout {
				t.Fatalf("latency probe run timeout = %s, want saturated %s", got, latencyProbeMaxRunTimeout)
			}
			ctx, cancel := context.WithTimeout(context.Background(), got)
			defer cancel()
			select {
			case <-ctx.Done():
				t.Fatalf("saturated latency probe deadline was canceled immediately: %v", ctx.Err())
			default:
			}
		})
	}
}

func TestLatencyProbeAttemptTimeoutHandlesInvalidAndExtremeSeconds(t *testing.T) {
	for _, seconds := range []int{0, -1, math.MinInt} {
		if got := latencyProbeAttemptTimeout(seconds); got != gateway.DefaultLatencyProbeTimeout {
			t.Errorf("latency probe timeout for seconds=%d = %s, want default %s", seconds, got, gateway.DefaultLatencyProbeTimeout)
		}
	}

	got := latencyProbeAttemptTimeout(math.MaxInt)
	want := latencyProbeMaxRunTimeout
	maxSeconds := time.Duration(math.MaxInt)
	if maxSeconds <= latencyProbeMaxRunTimeout/time.Second {
		want = maxSeconds * time.Second
	}
	if got != want || got <= 0 {
		t.Fatalf("latency probe timeout for maximum seconds = %s, want positive saturated value %s", got, want)
	}
}

func TestLatencyProbeCadenceUsesSafeDefaultAndSeconds(t *testing.T) {
	for _, seconds := range []int{0, -1, math.MinInt} {
		if got := latencyProbeCadence(seconds); got != time.Minute {
			t.Fatalf("cadence for %d = %s, want %s", seconds, got, time.Minute)
		}
	}
	if got := latencyProbeCadence(7); got != 7*time.Second {
		t.Fatalf("cadence = %s, want 7s", got)
	}
}

func TestStartLatencyProbeIgnoresLegacyDisabledFlagForRecoveryScanner(t *testing.T) {
	scheduler := &Scheduler{
		cfg:     config.SchedulerConfig{LatencyProbeEnabled: false, LatencyProbeIntervalSeconds: 60},
		gateway: &gateway.Service{},
	}
	scheduler.startLatencyProbe()
	if scheduler.stopLatency == nil {
		t.Fatal("legacy disabled flag prevented cooldown recovery scanner startup")
	}
	scheduler.stopLatency()
}

func TestRunLatencyProbeIgnoresLegacyDisabledFlagForCooldownRecovery(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("scheduler-disabled-recovery-test")
	if err != nil {
		t.Fatal(err)
	}
	keyCipher, err := cipher.Encrypt("sk-disabled-recovery")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "disabled recovery", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: server.URL, MonitorEnabled: true, LatencyProbeEnabled: false,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := channels.SetLatencyProbeEnabled(channel.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "BOG primary", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: keyCipher, APIKeyPrefix: "sk-disabled",
		UpstreamKeyID: 1, UpstreamKeyName: "BOG primary", ModelFamilies: "codex", ModelIDs: "gpt-test",
		Enabled: true, LatencyProbeEnabled: false,
	}); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-test", generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "primary", false); err != nil {
		t.Fatal(err)
	}
	healthFence := storage.NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
	for i := 0; i < 3; i++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(healthFence, gateway.RoutingFailureTransport, 0, "existing cooldown", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}

	service := gateway.NewService(channels, rates, store, cipher, nil, nil)
	scheduler := &Scheduler{
		cfg: config.SchedulerConfig{
			LatencyProbeEnabled: false, LatencyProbeIntervalSeconds: 60,
			LatencyProbeMaxCandidates: 1, LatencyProbeTimeoutSeconds: 1, Concurrency: 1,
		},
		gateway: service,
	}
	if delay := scheduler.runLatencyProbe(time.Minute); delay <= 0 {
		t.Fatalf("recovery scanner returned invalid delay %s", delay)
	}
	if requests.Load() != gateway.LatencyProbeConfirmationAttempts {
		t.Fatalf("cooldown recovery requests = %d, want %d", requests.Load(), gateway.LatencyProbeConfirmationAttempts)
	}
}

func TestLatencyProbeWakeDelayWakesForMixedConfirmationBeforeDefaultCadence(t *testing.T) {
	now := time.Date(2026, time.July, 23, 4, 0, 0, 0, time.UTC)
	next := now.Add(5 * time.Second)
	if got := latencyProbeWakeDelay(time.Minute, now, &next); got != 5*time.Second {
		t.Fatalf("wake delay = %s, want 5s", got)
	}
	nearCooldown := now.Add(15 * time.Second)
	if got := latencyProbeWakeDelay(time.Minute, now, &nearCooldown); got != 15*time.Second {
		t.Fatalf("wake delay for near cooldown = %s, want 15s", got)
	}
	if got := latencyProbeWakeDelay(time.Minute, now, nil); got != time.Minute {
		t.Fatalf("wake delay without due work = %s, want 1m", got)
	}
	past := now.Add(-time.Second)
	if got := latencyProbeWakeDelay(time.Minute, now, &past); got != time.Minute {
		t.Fatalf("wake delay for overdue deferred work = %s, want 1m", got)
	}
}

func TestNextLatencyProbeWakeDelayIgnoresHealthyHistoricalState(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "healthy-probe-wake", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://healthy-probe-wake.invalid", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "BOG primary", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "cipher", ModelFamilies: "codex",
		Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	lastProbe := now.Add(-time.Minute)
	next := now.Add(5 * time.Second)
	if err := store.UpsertLatencyProbeState(&storage.GatewayLatencyProbeState{
		ChannelID: channel.ID, GroupName: "primary", GatewayApp: storage.GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
		LastProbeAt: &lastProbe, NextProbeAt: &next,
	}); err != nil {
		t.Fatal(err)
	}
	scheduler := &Scheduler{gateway: gateway.NewService(channels, rates, store, nil, nil, nil)}
	if got := scheduler.nextLatencyProbeWakeDelay(time.Minute); got != time.Minute {
		t.Fatalf("healthy historical state wake delay = %s, want idle cadence", got)
	}
}

func TestLatencyProbeRunTimeoutLetsLastCandidateConfirmFailure(t *testing.T) {
	const attemptTimeout = 250 * time.Millisecond
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		// Every attempt consumes its own deadline. The scheduler-level deadline
		// must remain alive long enough for the final attempt to be classified
		// and persisted instead of looking like a caller cancellation.
		time.Sleep(2 * attemptTimeout)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("scheduler-latency-probe-budget-test")
	if err != nil {
		t.Fatal(err)
	}
	keyCipher, err := cipher.Encrypt("sk-probe-budget")
	if err != nil {
		t.Fatal(err)
	}

	var lastChannelID uint
	for index, item := range []struct {
		app       storage.GatewayApp
		family    string
		model     string
		groupName string
	}{
		{app: storage.GatewayAppCodex, family: "codex", model: "gpt-4.1", groupName: "codex-probe"},
		{app: storage.GatewayAppClaude, family: "claude", model: "claude-3-5-haiku", groupName: "claude-probe"},
		{app: storage.GatewayAppGemini, family: "gemini", model: "gemini-2.0-flash", groupName: "gemini-probe"},
	} {
		channel := &storage.Channel{
			Name: item.groupName, Type: storage.ChannelTypeNewAPI, GatewayApp: item.app,
			SiteURL: server.URL, MonitorEnabled: true, LatencyProbeEnabled: true,
		}
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		lastChannelID = channel.ID
		if _, err := rates.Upsert(&storage.RateSnapshot{
			ChannelID: channel.ID, ModelName: item.groupName, Ratio: float64(index + 1), LastSeenAt: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: item.groupName, GroupRatio: float64(index + 1),
			UpstreamKeyID: int64(index + 1), UpstreamKeyName: "BOG " + item.groupName,
			APIKeyCipher: keyCipher, APIKeyPrefix: "sk-probe", ModelFamilies: item.family,
			ModelIDs: item.model, LatencyProbeModel: item.model, LatencyProbeEnabled: true, Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, item.groupName, "added", 0, "", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		generation, generationErr := store.CaptureUpstreamGroupGeneration(channel.ID, item.groupName)
		if generationErr != nil || !generation.Valid() {
			t.Fatalf("capture probe generation=%+v err=%v", generation, generationErr)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, item.groupName, item.family, item.model, generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
			t.Fatalf("fence probe model facts: %v", err)
		}
		healthFence := storage.NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
		for i := 0; i < 3; i++ {
			if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(healthFence, gateway.RoutingFailureTransport, 0, "existing cooldown", time.Now().UTC()); err != nil {
				t.Fatalf("seed recovery cooldown: %v", err)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), latencyProbeRunTimeout(attemptTimeout, 1))
	defer cancel()
	service := gateway.NewService(channels, rates, store, cipher, nil, nil)
	results, err := service.RunLatencyProbes(ctx, gateway.LatencyProbeOptions{MaxCandidates: 1, Timeout: attemptTimeout})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != latencyProbeGatewayAppCount {
		t.Fatalf("probe results = %d, want %d: %+v", len(results), latencyProbeGatewayAppCount, results)
	}
	wantRequests := int32(latencyProbeGatewayAppCount * gateway.LatencyProbeConfirmationAttempts)
	if requests.Load() != wantRequests {
		t.Fatalf("probe requests = %d, want %d", requests.Load(), wantRequests)
	}
	lastKey, err := store.FindUpstreamKeyAny(lastChannelID, "gemini-probe")
	if err != nil {
		t.Fatal(err)
	}
	if lastKey.RoutingFailureCount != 6 || lastKey.LastRoutingFailureAt == nil || lastKey.LastRoutingFailureKind != gateway.RoutingFailureTransport {
		t.Fatalf("last candidate failure was not persisted: %+v", lastKey)
	}
}

func TestRunRetentionDeletesAnnouncements(t *testing.T) {
	db := openTestDB(t)
	announcements := storage.NewUpstreamAnnouncements(db)
	notifies := storage.NewNotifications(db)
	monLogs := storage.NewMonitorLogs(db)
	rates := storage.NewRates(db)

	oldTime := time.Now().AddDate(0, 0, -10)
	if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{{
		ChannelID:   1,
		SourceKey:   "old",
		Content:     "old",
		FirstSeenAt: oldTime,
	}}); err != nil {
		t.Fatalf("sync announcement: %v", err)
	}

	s := New(
		config.SchedulerConfig{
			Retention: config.RetentionConfig{
				AnnouncementsDays: 1,
			},
		},
		&monitor.Service{},
		monLogs,
		rates,
		notifies,
		announcements,
		nil,
		nil,
		nil,
		config.ProxyConfig{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	s.runRetention()

	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list announcements: %v", err)
	}
	if total != 0 || len(list) != 0 {
		t.Fatalf("announcements not cleaned: total=%d list=%#v", total, list)
	}
}
