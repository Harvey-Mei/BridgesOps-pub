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
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestLatencyProbeIntervalUsesFixedIsolationCadence(t *testing.T) {
	base := time.Minute
	if got := latencyProbeInterval(base, 99, true, 0); got != base {
		t.Fatalf("isolation interval=%s, want fixed cadence %s", got, base)
	}
}

func TestLatencyProbeModelUsesSynchronizedModelIDs(t *testing.T) {
	candidate := Candidate{
		App:               storage.GatewayAppCodex,
		ModelIDs:          []string{"gpt-large", "gpt-mini"},
		LatencyProbeModel: "legacy-admin-choice",
	}
	if got := latencyProbeModelForCandidate(candidate); got != "gpt-mini" {
		t.Fatalf("recovery probe model = %q, want synchronized low-cost model", got)
	}
	candidate.ModelIDs = nil
	if got := latencyProbeModelForCandidate(candidate); got != "" {
		t.Fatalf("recovery probe without synchronized model IDs = %q, want empty", got)
	}
}

func TestSelectDueLatencyProbeCandidatesKeepsRoutingOrderAndPullsNewIsolationForward(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	newFailure := now.Add(-10 * time.Second)
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1, Name: "smart-first", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "first"}},
		{Channel: storage.Channel{ID: 2, Name: "smart-second", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "second"}},
		{Channel: storage.Channel{ID: 3, Name: "isolated-first", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "first-isolated"}, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), CoolingDown: true},
		{Channel: storage.Channel{ID: 4, Name: "newly-isolated", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "newly-isolated"}, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), CoolingDown: true, LastRoutingFailureAt: &newFailure},
	}
	selected, intervals, err := selectDueLatencyProbeCandidates(candidates, nil, storage.GatewayAppCodex, 4, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || len(intervals) != 2 {
		t.Fatalf("selected=%+v intervals=%+v", selected, intervals)
	}
	// Healthy entries are never actively probed. Bootstrap isolation work keeps
	// smart-routing order because there is no release countdown to prioritize.
	if selected[0].Channel.ID != 3 || selected[1].Channel.ID != 4 {
		t.Fatalf("selected order = %+v", selected)
	}
	if intervals[0] != time.Minute || intervals[1] != time.Minute {
		t.Fatalf("isolation intervals = %+v", intervals)
	}

	notDue := now.Add(time.Hour)
	lastProbe := now.Add(-time.Minute)
	states := []storage.GatewayLatencyProbeState{{
		ChannelID: 4, GroupName: "newly-isolated", GatewayApp: storage.GatewayAppCodex, NextProbeAt: &notDue, LastProbeAt: &lastProbe,
	}}
	selected, _, err = selectDueLatencyProbeCandidates(candidates, states, storage.GatewayAppCodex, 4, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	foundNearCooldown := false
	for _, candidate := range selected {
		if candidate.Channel.ID == 4 {
			foundNearCooldown = true
		}
	}
	if !foundNearCooldown {
		t.Fatalf("new isolation kept stale historical due time: %+v", selected)
	}
}

func TestSelectDueLatencyProbeCandidatesCoolingWinsOverUnprobedWithOneSlot(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1, Name: "unprobed-one", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "one"}},
		{Channel: storage.Channel{ID: 2, Name: "unprobed-two", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "two"}},
		{Channel: storage.Channel{ID: 3, Name: "unprobed-three", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "three"}},
		{Channel: storage.Channel{ID: 4, Name: "cooling-near", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "near"}, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), CoolingDown: true, CooldownRemaining: 10 * time.Second},
	}
	selected, _, err := selectDueLatencyProbeCandidates(candidates, nil, storage.GatewayAppCodex, 1, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Channel.ID != 4 {
		t.Fatalf("selected=%+v, want nearest cooldown candidate", selected)
	}
}

func TestFilterProbeGenerationCandidatesSkipsUnscopedRoutes(t *testing.T) {
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1}, Rate: storage.RateSnapshot{ModelName: "legacy"}},
		{Channel: storage.Channel{ID: 2}, Rate: storage.RateSnapshot{ModelName: "current"}, UpstreamGeneration: storage.UpstreamGroupGeneration{ID: 9, ChannelID: 2, CanonicalGroupName: "current", PresenceGeneration: 1}},
	}
	filtered := filterProbeGenerationCandidates(candidates)
	if len(filtered) != 1 || filtered[0].Channel.ID != 2 {
		t.Fatalf("filtered candidates=%+v, want only generation-scoped route", filtered)
	}
}

func TestSelectDueLatencyProbeCandidatesIgnoresHealthyDueState(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	lastProbe := now.Add(-2 * time.Minute)
	overdue := now.Add(-time.Minute)
	candidate := Candidate{Channel: storage.Channel{ID: 1}, Rate: storage.RateSnapshot{ModelName: "promoted"}}
	state := storage.GatewayLatencyProbeState{
		ChannelID: 1, GroupName: "promoted", GatewayApp: storage.GatewayAppCodex,
		NextProbeAt: &overdue, LastProbeAt: &lastProbe,
	}
	selected, _, err := selectDueLatencyProbeCandidates([]Candidate{candidate}, []storage.GatewayLatencyProbeState{state}, storage.GatewayAppCodex, 1, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 0 {
		t.Fatalf("healthy historical state selected active probes: %+v", selected)
	}
}

func TestLatencyProbeEffectiveDueAtPullsNewIsolationForwardOnce(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	lastProbe := now.Add(-10 * time.Second)
	nextProbe := now.Add(time.Hour)
	newFailure := now.Add(-5 * time.Second)
	candidate := Candidate{CoolingDown: true, LastRoutingFailureAt: &newFailure}
	state := storage.GatewayLatencyProbeState{NextProbeAt: &nextProbe, LastProbeAt: &lastProbe}
	if got := latencyProbeEffectiveDueAt(candidate, state, true, time.Minute, now); !got.Equal(now) {
		t.Fatalf("new isolation due at %s, want immediate bootstrap", got)
	}
	oldFailure := now.Add(-20 * time.Second)
	candidate.LastRoutingFailureAt = &oldFailure
	want := lastProbe.Add(time.Minute)
	if got := latencyProbeEffectiveDueAt(candidate, state, true, time.Minute, now); !got.Equal(want) {
		t.Fatalf("existing isolation due at %s, want persisted cadence %s", got, want)
	}
}

func TestSelectDueLatencyProbeCandidatesKeepsExpiredLegacyCooldownIsolated(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1, Name: "healthy", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "healthy"}},
		{Channel: storage.Channel{ID: 2, Name: "expired", LatencyProbeEnabled: true}, Rate: storage.RateSnapshot{ModelName: "expired"}, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), CoolingDown: true, CooldownUntil: ptrTime(now.Add(-time.Second))},
	}
	selected, _, err := selectDueLatencyProbeCandidates(candidates, nil, storage.GatewayAppCodex, 1, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Channel.ID != 2 {
		t.Fatalf("expired legacy cooldown was not probed while still isolated: %+v", selected)
	}
}

func TestSelectDueLatencyProbeCandidatesSkipsCooldownWithoutUsableModel(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1}, Rate: storage.RateSnapshot{ModelName: "missing"}, CoolingDown: true, CooldownRemaining: time.Minute},
		{Channel: storage.Channel{ID: 2}, Rate: storage.RateSnapshot{ModelName: "wrong-app"}, App: storage.GatewayAppClaude, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), CoolingDown: true, CooldownRemaining: time.Minute},
		{Channel: storage.Channel{ID: 3}, Rate: storage.RateSnapshot{ModelName: "usable"}, App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), CoolingDown: true, CooldownRemaining: time.Minute},
	}
	selected, _, err := selectDueLatencyProbeCandidates(candidates, nil, storage.GatewayAppCodex, 3, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Channel.ID != 3 {
		t.Fatalf("selected=%+v, want only cooldown candidate with a usable synchronized model", selected)
	}
}

func TestSelectDueLatencyProbeCandidatesSkipsStaleOrFailedModelSync(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	discoveredAt := now.Add(-5 * time.Minute)
	generation := func(id, channelID uint, groupName string) storage.UpstreamGroupGeneration {
		return storage.UpstreamGroupGeneration{
			ID: id, ChannelID: channelID, CanonicalGroupName: groupName, PresenceGeneration: 2, DiscoveredAt: discoveredAt,
		}
	}
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1}, Rate: storage.RateSnapshot{ModelName: "sync-error"}, UpstreamGeneration: generation(9, 1, "sync-error"), ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now), ModelsError: "upstream unavailable", CoolingDown: true, CooldownRemaining: time.Minute},
		{Channel: storage.Channel{ID: 2}, Rate: storage.RateSnapshot{ModelName: "missing-sync-time"}, UpstreamGeneration: generation(10, 2, "missing-sync-time"), ModelIDs: []string{"gpt-mini"}, CoolingDown: true, CooldownRemaining: time.Minute},
		{Channel: storage.Channel{ID: 3}, Rate: storage.RateSnapshot{ModelName: "prior-generation"}, UpstreamGeneration: generation(11, 3, "prior-generation"), ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(discoveredAt.Add(-time.Second)), CoolingDown: true, CooldownRemaining: time.Minute},
		{Channel: storage.Channel{ID: 4}, Rate: storage.RateSnapshot{ModelName: "current"}, UpstreamGeneration: generation(12, 4, "current"), ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(discoveredAt.Add(time.Second)), CoolingDown: true, CooldownRemaining: time.Minute},
	}
	selected, _, err := selectDueLatencyProbeCandidates(candidates, nil, storage.GatewayAppCodex, len(candidates), time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Channel.ID != 4 {
		t.Fatalf("selected=%+v, want only candidate with current successful model sync", selected)
	}
}

func TestSelectDueLatencyProbeCandidatesAllowsDirectProjectedModelWithoutSyncTimestamp(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	candidate := Candidate{
		Channel: storage.Channel{ID: 1, CredentialMode: storage.CredentialModeDirect},
		Rate:    storage.RateSnapshot{ModelName: "direct"},
		App:     storage.GatewayAppCodex, ModelIDs: []string{"vendor-model"},
		CoolingDown: true, CooldownRemaining: time.Minute,
	}
	selected, _, err := selectDueLatencyProbeCandidates([]Candidate{candidate}, nil, storage.GatewayAppCodex, 1, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || selected[0].Channel.ID != candidate.Channel.ID {
		t.Fatalf("selected=%+v, want direct projected model without managed sync timestamp", selected)
	}
}

func TestDirectLatencyProbeCandidateMatchesCurrentProjection(t *testing.T) {
	credentialID := uint(21)
	channel := storage.Channel{
		ID: 5, CredentialMode: storage.CredentialModeDirect, GatewayApp: storage.GatewayAppCodex,
		UpstreamAccountIdentityRevision: 4, AdminRevision: 9,
	}
	credential := storage.ChannelDirectCredential{
		ID: credentialID, ChannelID: channel.ID, GroupName: "primary", BaseURL: "https://direct.invalid",
		APIKeyCipher: "cipher", Models: storage.EncodeDirectModels([]string{"model-a", "model-b"}),
		Enabled: true, Revision: 7,
	}
	key := storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "PRIMARY", APIKeyCipher: credential.APIKeyCipher,
		DirectCredentialID: &credentialID, DirectCredentialRevision: credential.Revision,
		DirectBaseURL: "https://direct.invalid/", ModelIDs: "model-a,model-b", Enabled: true,
	}
	candidate := Candidate{
		Channel: channel, Rate: storage.RateSnapshot{ModelName: "primary"}, App: storage.GatewayAppCodex,
		ModelIDs: []string{"model-a", "model-b"}, DirectBaseURL: "https://direct.invalid",
	}
	if !directLatencyProbeCandidateMatchesCurrentProjection(candidate, channel, key, []storage.ChannelDirectCredential{credential}) {
		t.Fatal("current direct source did not match its latency-probe candidate")
	}

	t.Run("disabled source", func(t *testing.T) {
		changed := credential
		changed.Enabled = false
		if directLatencyProbeCandidateMatchesCurrentProjection(candidate, channel, key, []storage.ChannelDirectCredential{changed}) {
			t.Fatal("disabled direct source matched")
		}
	})
	t.Run("stale candidate admin revision", func(t *testing.T) {
		changed := candidate
		changed.Channel.AdminRevision--
		if directLatencyProbeCandidateMatchesCurrentProjection(changed, channel, key, []storage.ChannelDirectCredential{credential}) {
			t.Fatal("candidate from prior direct configuration revision matched")
		}
	})
	t.Run("different channel", func(t *testing.T) {
		changed := credential
		changed.ChannelID++
		if directLatencyProbeCandidateMatchesCurrentProjection(candidate, channel, key, []storage.ChannelDirectCredential{changed}) {
			t.Fatal("direct source from another channel matched")
		}
	})
	t.Run("replaced revision", func(t *testing.T) {
		changed := credential
		changed.Revision++
		if directLatencyProbeCandidateMatchesCurrentProjection(candidate, channel, key, []storage.ChannelDirectCredential{changed}) {
			t.Fatal("replaced direct source matched stale runtime key")
		}
	})
	t.Run("rotated secret", func(t *testing.T) {
		changed := credential
		changed.APIKeyCipher = "replacement"
		if directLatencyProbeCandidateMatchesCurrentProjection(candidate, channel, key, []storage.ChannelDirectCredential{changed}) {
			t.Fatal("rotated direct source matched stale runtime key")
		}
	})
	t.Run("changed models", func(t *testing.T) {
		changed := credential
		changed.Models = storage.EncodeDirectModels([]string{"model-b"})
		if directLatencyProbeCandidateMatchesCurrentProjection(candidate, channel, key, []storage.ChannelDirectCredential{changed}) {
			t.Fatal("changed direct model projection matched")
		}
	})
	t.Run("stale candidate projection", func(t *testing.T) {
		changed := candidate
		changed.DirectBaseURL = "https://old.invalid"
		if directLatencyProbeCandidateMatchesCurrentProjection(changed, channel, key, []storage.ChannelDirectCredential{credential}) {
			t.Fatal("stale candidate base URL matched current runtime projection")
		}
	})
}

func TestFilterCurrentDirectLatencyProbeCandidatesSkipsDisabledSource(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "direct recovery source", Type: storage.ChannelTypeNewAPI, SiteURL: "https://direct.invalid",
		PasswordCipher: "legacy-cipher", CredentialMode: storage.CredentialModeDirect,
		GatewayApp: storage.GatewayAppCodex, DirectModels: storage.EncodeDirectModels([]string{"model-a"}), MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	credential := &storage.ChannelDirectCredential{
		ChannelID: channel.ID, GroupName: "primary", BaseURL: "https://direct.invalid",
		APIKeyCipher: "cipher", Models: storage.EncodeDirectModels([]string{"model-a"}),
		Ratio: 1, CompletionRatio: 1, Enabled: true, Revision: 1,
	}
	if err := db.Create(credential).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDirectUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: credential.GroupName, APIKeyCipher: credential.APIKeyCipher,
		DirectCredentialID: &credential.ID, DirectCredentialRevision: credential.Revision,
		DirectBaseURL: credential.BaseURL, ModelIDs: "model-a", Enabled: true,
	}, channel.UpstreamAccountIdentityRevision); err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{
		Channel: *channel, Rate: storage.RateSnapshot{ModelName: credential.GroupName}, App: storage.GatewayAppCodex,
		ModelIDs: []string{"model-a"}, DirectBaseURL: credential.BaseURL,
	}
	service := NewService(channels, nil, store, nil, nil, nil)
	if filtered := service.filterCurrentDirectLatencyProbeCandidates(context.Background(), []Candidate{candidate}); len(filtered) != 1 {
		t.Fatalf("current direct candidate filtered=%+v", filtered)
	}
	if err := db.Model(&storage.ChannelDirectCredential{}).Where("id = ?", credential.ID).Update("enabled", false).Error; err != nil {
		t.Fatal(err)
	}
	if filtered := service.filterCurrentDirectLatencyProbeCandidates(context.Background(), []Candidate{candidate}); len(filtered) != 0 {
		t.Fatalf("disabled direct source remained eligible: %+v", filtered)
	}
}

func TestSelectDueLatencyProbeCandidatesSkipsOverBudgetHeadWithoutStarvingAffordableRoutes(t *testing.T) {
	now := time.Date(2026, time.July, 23, 3, 0, 0, 0, time.UTC)
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1}, Rate: storage.RateSnapshot{ModelName: "over-budget"}, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), ProbeEstimatedCostUSD: 0.03, CoolingDown: true, CooldownRemaining: time.Minute},
		{Channel: storage.Channel{ID: 2}, Rate: storage.RateSnapshot{ModelName: "affordable-one"}, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), ProbeEstimatedCostUSD: 0.01, CoolingDown: true, CooldownRemaining: time.Minute},
		{Channel: storage.Channel{ID: 3}, Rate: storage.RateSnapshot{ModelName: "affordable-two"}, ModelIDs: []string{"gpt-mini"}, ModelsSyncedAt: ptrTime(now.Add(-time.Minute)), ProbeEstimatedCostUSD: 0.01, CoolingDown: true, CooldownRemaining: time.Minute},
	}
	selected, _, usedCost, err := selectDueLatencyProbeCandidatesWithBudget(candidates, nil, storage.GatewayAppCodex, 2, time.Minute, now, 0.061)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].Channel.ID != 2 || selected[1].Channel.ID != 3 {
		t.Fatalf("budget selection=%+v, want both affordable routes", selected)
	}
	if usedCost < 0.059 || usedCost > 0.061 {
		t.Fatalf("used budget=%f, want three attempts for each affordable route", usedCost)
	}
}

func TestLatencyProbeCandidateStateKeyUsesGenerationScopedCanonicalName(t *testing.T) {
	candidate := Candidate{
		Channel: storage.Channel{ID: 7, UpstreamAccountIdentityRevision: 4}, Rate: storage.RateSnapshot{ModelName: "Stale Name"},
		UpstreamGeneration: storage.UpstreamGroupGeneration{ID: 9, ChannelID: 7, CanonicalGroupName: "postgres-canonical", PresenceGeneration: 2},
	}
	got := latencyProbeCandidateStateKey(candidate)
	want := latencyProbeStateKey(7, 4, "postgres-canonical", 9, 2)
	if got != want {
		t.Fatalf("state key = %q, want %q", got, want)
	}
}

func TestSelectDueLatencyProbeCandidatesIgnoresPriorChannelIdentityState(t *testing.T) {
	now := time.Now().UTC()
	candidate := Candidate{
		Channel:        storage.Channel{ID: 7, UpstreamAccountIdentityRevision: 2},
		Rate:           storage.RateSnapshot{ModelName: "primary"},
		ModelIDs:       []string{"gpt-mini"},
		ModelsSyncedAt: ptrTime(now.Add(-time.Minute)),
		CoolingDown:    true, CooldownRemaining: time.Minute,
		UpstreamGeneration: storage.UpstreamGroupGeneration{
			ID: 9, ChannelID: 7, CanonicalGroupName: "primary", PresenceGeneration: 3,
		},
	}
	priorProbeAt := now.Add(-time.Minute)
	priorNextProbeAt := now.Add(time.Hour)
	states := []storage.GatewayLatencyProbeState{{
		ChannelID: 7, ChannelIdentityRevision: 1, GroupName: "primary", GatewayApp: storage.GatewayAppCodex,
		UpstreamGroupID: 9, PresenceGeneration: 3, LastProbeAt: &priorProbeAt, NextProbeAt: &priorNextProbeAt,
	}}
	selected, _, err := selectDueLatencyProbeCandidates([]Candidate{candidate}, states, storage.GatewayAppCodex, 1, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 {
		t.Fatalf("current account candidate was suppressed by prior account state: selected=%+v", selected)
	}
}

func TestRunLatencyProbesDoesNotProbeHealthyCandidate(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("latency-probe-due-state-test")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "due state", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true, LatencyProbeEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	keyCipher, err := cipher.Encrypt("sk-due-state")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: keyCipher, APIKeyPrefix: "sk-due",
		UpstreamKeyID: 1, UpstreamKeyName: "BOG primary", ModelFamilies: "codex", ModelIDs: "gpt-test", LatencyProbeModel: "gpt-test", Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-test", generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 23, 3, 30, 0, 0, time.UTC)
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "primary", Ratio: 1}}}
	service := NewService(channels, rates, store, cipher, keys, nil)
	results, err := service.RunLatencyProbes(context.Background(), LatencyProbeOptions{MaxCandidates: 1, Timeout: time.Second, Interval: 10 * time.Minute, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 || requests.Load() != 0 || keys.listGroupCalls != 0 {
		t.Fatalf("healthy scheduler run results=%+v requests=%d model-sync-calls=%d, want no active probe", results, requests.Load(), keys.listGroupCalls)
	}
	state, err := store.FindLatencyProbeState(channel.ID, "primary", storage.GatewayAppCodex, generation)
	if err != nil || state != nil {
		t.Fatalf("healthy scheduler persisted probe state=%+v err=%v", state, err)
	}
}

func TestRunLatencyProbesKeepsExistingCooldownInRecoveryCadence(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"temporarily unavailable"}`))
	}))
	defer server.Close()

	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("latency-probe-new-cooldown-test")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "new cooldown", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true, LatencyProbeEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	keyCipher, err := cipher.Encrypt("sk-new-cooldown")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: keyCipher, APIKeyPrefix: "sk-new",
		UpstreamKeyID: 1, UpstreamKeyName: "BOG primary", ModelFamilies: "codex", ModelIDs: "gpt-test", LatencyProbeModel: "gpt-test", Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-test", generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	healthFence := storage.NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
	for i := 0; i < 3; i++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(healthFence, RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "previous failure", now); err != nil {
			t.Fatal(err)
		}
	}
	lastProbe := now.Add(-time.Minute)
	due := now.Add(-time.Second)
	if err := store.UpsertLatencyProbeState(&storage.GatewayLatencyProbeState{
		ChannelID: channel.ID, GroupName: "primary", GatewayApp: storage.GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration, LastProbeAt: &lastProbe, NextProbeAt: &due,
	}); err != nil {
		t.Fatal(err)
	}

	service := NewService(channels, rates, store, cipher, nil, nil)
	results, err := service.RunLatencyProbes(context.Background(), LatencyProbeOptions{
		MaxCandidates: 1, Timeout: time.Second, Interval: 8 * time.Minute, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := latencyProbeResultForApp(results, storage.GatewayAppCodex)
	if result == nil || result.confirmation != "failure" || requests.Load() != LatencyProbeConfirmationAttempts {
		t.Fatalf("probe results=%+v requests=%d", results, requests.Load())
	}
	key, err := store.FindUpstreamKey(channel.ID, "primary")
	if err != nil || key.RoutingFailureCount != 6 || key.RoutingCooldownUntil != nil {
		t.Fatalf("new routing state=%+v err=%v", key, err)
	}
	state, err := store.FindLatencyProbeState(channel.ID, "primary", storage.GatewayAppCodex, generation)
	earliestNext := now.Add(8 * time.Minute)
	latestNext := result.completedAt.Add(8 * time.Minute)
	if err != nil || state == nil || state.NextProbeAt == nil || state.NextProbeAt.Before(earliestNext) || state.NextProbeAt.After(latestNext) {
		t.Fatalf("new cooldown probe schedule=%+v err=%v, want between %s and %s", state, err, earliestNext, latestNext)
	}
	if !state.NextProbeAt.After(now) {
		t.Fatalf("isolated route did not retain a bounded recovery cadence: next=%s", state.NextProbeAt)
	}
}

func TestRunLatencyProbesIgnoresLegacySwitchesForCooldownRecovery(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("latency-probe-gates-test")
	if err != nil {
		t.Fatal(err)
	}
	syncedAt := time.Now().UTC()
	lowBalance := storage.MinimumChannelBalanceThreshold - 0.01
	fixtures := []struct {
		name           string
		channelEnabled bool
		groupEnabled   bool
		lastBalance    *float64
	}{
		{name: "channel-disabled", channelEnabled: false, groupEnabled: true},
		{name: "group-disabled", channelEnabled: true, groupEnabled: false},
		{name: "low-balance", channelEnabled: true, groupEnabled: true, lastBalance: &lowBalance},
	}
	for index, fixture := range fixtures {
		channel := &storage.Channel{
			Name: fixture.name, Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true,
			LatencyProbeEnabled: fixture.channelEnabled, LastBalance: fixture.lastBalance,
		}
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: syncedAt}); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", int64(index+1), "BOG primary", syncedAt); err != nil {
			t.Fatal(err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
		if err != nil {
			t.Fatal(err)
		}
		keyCipher, err := cipher.Encrypt("sk-gated-" + fixture.name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: keyCipher, APIKeyPrefix: "sk-gated",
			UpstreamKeyID: int64(index + 1), UpstreamKeyName: "BOG primary", ModelFamilies: "codex", ModelIDs: "gpt-test", LatencyProbeModel: "gpt-test",
			ModelsSyncedAt: &syncedAt, Enabled: true, LatencyProbeEnabled: fixture.groupEnabled,
			ModelFactsUpstreamGroupID: generation.ID, ModelFactsPresenceGeneration: generation.PresenceGeneration,
			ModelFactsChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		}); err != nil {
			t.Fatal(err)
		}
		healthFence := storage.NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
		for i := 0; i < 3; i++ {
			if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(healthFence, RoutingFailureTransport, 0, "existing cooldown", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
		}
		if !fixture.channelEnabled {
			if err := channels.SetLatencyProbeEnabled(channel.ID, false); err != nil {
				t.Fatal(err)
			}
		}
		if !fixture.groupEnabled {
			if err := store.UpdateLatencyProbeEnabled(channel.ID, "primary", false); err != nil {
				t.Fatal(err)
			}
		}
	}

	service := NewService(channels, rates, store, cipher, nil, nil)
	results, err := service.RunLatencyProbes(context.Background(), LatencyProbeOptions{MaxCandidates: 3, Timeout: time.Second, Interval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != len(fixtures) {
		t.Fatalf("cooldown recovery results=%+v, want legacy-disabled and low-balance candidates", results)
	}
	wantRequests := int32(len(fixtures) * gatewayLatencyProbeAttempts)
	if requests.Load() != wantRequests {
		t.Fatalf("cooldown recovery requests=%d, want %d", requests.Load(), wantRequests)
	}
	states, err := store.ListLatencyProbeStates()
	if err != nil || len(states) != len(fixtures) {
		t.Fatalf("cooldown recovery states=%+v err=%v, want %d", states, err, len(fixtures))
	}
}

func TestRunLatencyProbesBoundsConcurrentRounds(t *testing.T) {
	var requests atomic.Int32
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		requests.Add(1)
		current := inFlight.Add(1)
		for {
			previous := maxInFlight.Load()
			if current <= previous || maxInFlight.CompareAndSwap(previous, current) {
				break
			}
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		inFlight.Add(-1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("latency-probe-concurrency-test")
	if err != nil {
		t.Fatal(err)
	}
	syncedAt := time.Now().UTC()
	for index, name := range []string{"concurrency-one", "concurrency-two"} {
		channel := &storage.Channel{Name: name, Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true, LatencyProbeEnabled: true}
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: syncedAt}); err != nil {
			t.Fatal(err)
		}
		keyCipher, err := cipher.Encrypt("sk-concurrency-" + name)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: keyCipher, APIKeyPrefix: "sk-concurrency",
			UpstreamKeyID: int64(index + 1), UpstreamKeyName: "BOG primary", ModelFamilies: "codex", ModelIDs: "gpt-test", LatencyProbeModel: "gpt-test",
			ModelsSyncedAt: &syncedAt, Enabled: true, LatencyProbeEnabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-test", generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
			t.Fatal(err)
		}
		healthFence := storage.NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
		for i := 0; i < 3; i++ {
			if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(healthFence, RoutingFailureTransport, 0, "existing cooldown", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
		}
	}

	type runResult struct {
		results []LatencyProbeResult
		err     error
	}
	service := NewService(channels, rates, store, cipher, nil, nil)
	done := make(chan runResult, 1)
	go func() {
		results, err := service.RunLatencyProbes(context.Background(), LatencyProbeOptions{
			MaxCandidates: 2, MaxConcurrent: 2, Timeout: 5 * time.Second, Interval: time.Minute,
		})
		done <- runResult{results: results, err: err}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent probe rounds did not start")
		}
	}
	close(release)
	released = true
	result := <-done
	actualResults := 0
	for _, probe := range result.results {
		if probe.ChannelID != 0 {
			actualResults++
		}
	}
	if result.err != nil || actualResults != 2 {
		t.Fatalf("concurrent probe run results=%+v err=%v", result.results, result.err)
	}
	if got := maxInFlight.Load(); got != 2 {
		t.Fatalf("maximum concurrent requests=%d, want 2", got)
	}
	if requests.Load() != 2*LatencyProbeConfirmationAttempts {
		t.Fatalf("concurrent probe requests=%d, want %d", requests.Load(), 2*LatencyProbeConfirmationAttempts)
	}
}

func latencyProbeResultForApp(results []LatencyProbeResult, app storage.GatewayApp) *LatencyProbeResult {
	for index := range results {
		if results[index].App == app && results[index].ChannelID != 0 {
			return &results[index]
		}
	}
	return nil
}
