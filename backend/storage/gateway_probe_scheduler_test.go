package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestLatencyProbeStateIsGenerationScopedAndRejectsLateWrite(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "probe state generation", Type: ChannelTypeNewAPI, SiteURL: "https://probe-state.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "Primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", ModelFamilies: "codex", Enabled: true, LatencyProbeEnabled: true}); err != nil {
		t.Fatal(err)
	}
	oldGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	seedActiveLatencyProbeCooldown(t, store, *channel, oldGeneration)
	next := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	state := &GatewayLatencyProbeState{
		ChannelID: channel.ID, GroupName: oldGeneration.CanonicalGroupName, GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(oldGeneration.ID), PresenceGeneration: oldGeneration.PresenceGeneration,
		NextProbeAt: &next, ConfirmationOutcome: "failure", ConfirmationCount: 3,
	}
	if err := store.UpsertLatencyProbeState(state); err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due == nil {
		t.Fatalf("active cooldown next due=%v err=%v, want a recovery wake", due, err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	newGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if newGeneration.PresenceGeneration == oldGeneration.PresenceGeneration {
		t.Fatalf("generation did not advance: old=%+v new=%+v", oldGeneration, newGeneration)
	}
	if got, err := store.FindLatencyProbeState(channel.ID, "primary", GatewayAppCodex, newGeneration); err != nil || got != nil {
		t.Fatalf("new generation inherited old state: state=%+v err=%v", got, err)
	}
	legacyNext := time.Now().UTC().Add(time.Minute)
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: channel.ID, GroupName: "primary", GatewayApp: GatewayAppCodex, NextProbeAt: &legacyNext,
	}); err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("removed-generation or legacy state woke scheduler: due=%v err=%v", due, err)
	}
	if err := store.UpsertLatencyProbeState(state); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("late state write error = %v, want generation changed", err)
	}
}

func TestNextLatencyProbeDueAtIgnoresHealthyHistoricalState(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "probe rank wake", Type: ChannelTypeNewAPI, SiteURL: "https://probe-rank-wake.example", MonitorEnabled: true, LatencyProbeEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).Update("last_balance", 0.01).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test",
		ModelFamilies: "codex", Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	lastProbe := now.Add(-30 * time.Second)
	next := now.Add(30 * time.Minute)
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: channel.ID, GroupName: "primary", GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
		LastProbeAt: &lastProbe, NextProbeAt: &next,
	}); err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(time.Minute); err != nil || due != nil {
		t.Fatalf("healthy historical probe state woke scheduler: due=%v err=%v", due, err)
	}
}

func TestNextLatencyProbeDueAtSkipsOrphanedAndUnroutableState(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "probe due filtering", Type: ChannelTypeNewAPI, SiteURL: "https://probe-due-filter.example", MonitorEnabled: true, LatencyProbeEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test",
		ModelFamilies: "codex", Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	seedActiveLatencyProbeCooldown(t, store, *channel, generation)
	next := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: channel.ID, GroupName: "primary", GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration, NextProbeAt: &next,
	}); err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due == nil || due.Before(next.Add(-2*time.Second)) || due.After(next.Add(2*time.Second)) {
		t.Fatalf("initial cooldown due=%v err=%v, want near %v", due, err, next)
	}
	if err := db.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "primary").Update("provisioning", true).Error; err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("provisioning key woke scheduler: due=%v err=%v", due, err)
	}
	if err := db.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "primary").Update("provisioning", false).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "primary").Update("model_ids", "").Error; err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("model-less key woke scheduler: due=%v err=%v", due, err)
	}
	seedCurrentLatencyProbeModels(t, store, *channel, generation)
	if err := db.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "primary").Update("model_facts_presence_generation", generation.PresenceGeneration+1).Error; err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("stale model-fact generation woke scheduler: due=%v err=%v", due, err)
	}
	seedCurrentLatencyProbeModels(t, store, *channel, generation)

	if err := db.Where("channel_id = ? AND lower(btrim(model_name)) = ?", channel.ID, "primary").Delete(&RateSnapshot{}).Error; err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("orphaned rate state woke scheduler: due=%v err=%v", due, err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "primary").Update("api_key_cipher", "").Error; err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("keyless state woke scheduler: due=%v err=%v", due, err)
	}
	if err := db.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "primary").Updates(map[string]any{"api_key_cipher": "cipher", "model_families": "claude"}).Error; err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("unsupported-app state woke scheduler: due=%v err=%v", due, err)
	}
	if err := db.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "primary").Updates(map[string]any{
		"model_families": "codex", "latency_probe_enabled": false,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).Update("latency_probe_enabled", false).Error; err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due == nil {
		t.Fatalf("legacy hidden probe switches blocked cooldown wake: due=%v err=%v", due, err)
	}
}

func seedActiveLatencyProbeCooldown(t *testing.T, store *Gateway, channel Channel, generation UpstreamGroupGeneration) {
	t.Helper()
	seedCurrentLatencyProbeModels(t, store, channel, generation)
	fence := NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
	for i := 0; i < 3; i++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "transport", 0, "existing cooldown", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
}

func seedCurrentLatencyProbeModels(t *testing.T, store *Gateway, channel Channel, generation UpstreamGroupGeneration) {
	t.Helper()
	fence := NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision)
	if err := store.UpdateUpstreamKeyModelFamiliesForIdentity(fence, "codex", "gpt-mini", time.Now().UTC(), ""); err != nil {
		t.Fatal(err)
	}
}

func TestNextLatencyProbeDueAtUsesPersistedIsolationScheduleWithoutHotLoop(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "probe future wake", Type: ChannelTypeNewAPI, SiteURL: "https://probe-future-wake.example", MonitorEnabled: true, LatencyProbeEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, group := range []string{"overdue", "future"} {
		var failureAt *time.Time
		failureCount := 0
		if group == "future" {
			observed := time.Now().UTC()
			failureAt = &observed
			failureCount = GatewayAdaptiveRoutingFailureThreshold
		}
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: group, Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, group, "added", 0, "", time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, group)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: group, APIKeyCipher: "cipher", APIKeyPrefix: "sk-test",
			ModelFamilies: "codex", Enabled: true, LatencyProbeEnabled: true,
			LastRoutingFailureKind: "transport", LastRoutingFailureAt: failureAt, RoutingFailureCount: failureCount,
			RoutingIsolatedAt:            failureAt,
			RoutingHealthUpstreamGroupID: generation.ID, RoutingHealthPresenceGeneration: generation.PresenceGeneration,
			RoutingHealthChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, group := range []string{"overdue", "future"} {
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, group)
		if err != nil {
			t.Fatal(err)
		}
		seedCurrentLatencyProbeModels(t, store, *channel, generation)
		next := time.Now().UTC().Add(-time.Minute)
		if group == "future" {
			next = time.Now().UTC().Add(30 * time.Minute)
		}
		if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
			ChannelID: channel.ID, GroupName: group, GatewayApp: GatewayAppCodex,
			UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
			NextProbeAt: &next,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Model(&GatewayUpstreamKey{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "future").
		Updates(map[string]any{
			"last_routing_failure_error":  "",
			"last_routing_failure_kind":   "",
			"last_routing_failure_status": 0,
			"last_routing_failure_at":     nil,
			"routing_failure_count":       0,
		}).Error; err != nil {
		t.Fatal(err)
	}
	due, err := store.NextLatencyProbeDueAt()
	if err != nil {
		t.Fatal(err)
	}
	if due == nil || due.Before(time.Now().UTC().Add(28*time.Minute)) || due.After(time.Now().UTC().Add(31*time.Minute)) {
		t.Fatalf("next due=%v, want persisted future latch schedule without an expiry-driven hot loop", due)
	}
}

func TestNextLatencyProbeDueAtRequiresCurrentDirectCredentialProjection(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: "direct probe wake", Type: ChannelTypeNewAPI, SiteURL: "https://direct-probe-wake.example",
		PasswordCipher: "legacy-direct-cipher", CredentialMode: CredentialModeDirect,
		GatewayApp: GatewayAppCodex, DirectModels: EncodeDirectModels([]string{"gpt-mini"}), MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	credential := &ChannelDirectCredential{
		ChannelID: channel.ID, GroupName: "primary", BaseURL: "https://direct-upstream.example/",
		APIKeyCipher: "direct-cipher", APIKeyPrefix: "sk-test", Models: EncodeDirectModels([]string{"gpt-mini"}),
		Ratio: 1, CompletionRatio: 1, Enabled: true, Revision: 1,
	}
	if err := db.Create(credential).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: credential.APIKeyCipher, APIKeyPrefix: credential.APIKeyPrefix,
		DirectCredentialID: &credential.ID, DirectCredentialRevision: credential.Revision, DirectBaseURL: credential.BaseURL,
		ModelFamilies: "codex", ModelIDs: "gpt-mini", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	seedActiveLatencyProbeCooldown(t, store, *channel, generation)
	next := time.Now().UTC().Add(time.Minute)
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		GroupName: "primary", GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration, NextProbeAt: &next,
	}); err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due == nil {
		t.Fatalf("current direct projection due=%v err=%v, want recovery wake", due, err)
	}
	if err := db.Model(&ChannelDirectCredential{}).Where("id = ?", credential.ID).Update("revision", credential.Revision+1).Error; err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("stale direct projection woke scheduler: due=%v err=%v", due, err)
	}
}

func TestNextLatencyProbeDueAtIgnoresCooldownFromAnotherHealthIdentity(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "probe stale health wake", Type: ChannelTypeNewAPI, SiteURL: "https://probe-stale-health-wake.example", MonitorEnabled: true, LatencyProbeEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	cooldownUntil := time.Now().UTC().Add(10 * time.Minute)
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test",
		ModelFamilies: "codex", Enabled: true, LatencyProbeEnabled: true, RoutingCooldownUntil: &cooldownUntil,
		RoutingHealthUpstreamGroupID: generation.ID, RoutingHealthPresenceGeneration: generation.PresenceGeneration,
		RoutingHealthChannelIdentityRevision: channel.UpstreamAccountIdentityRevision + 1,
	}); err != nil {
		t.Fatal(err)
	}
	overdue := time.Now().UTC().Add(-time.Minute)
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: channel.ID, GroupName: "primary", GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration, NextProbeAt: &overdue,
	}); err != nil {
		t.Fatal(err)
	}
	if due, err := store.NextLatencyProbeDueAt(); err != nil || due != nil {
		t.Fatalf("stale routing cooldown woke scheduler: due=%v err=%v", due, err)
	}
}

func TestLatencyProbeStateUpsertDoesNotRegressConfirmedRecoveryTimestamp(t *testing.T) {
	store, generation := routingEnableFixture(t, "probe-state-monotonic")
	base := time.Now().UTC().Truncate(time.Microsecond)
	completedAt := base.Add(2 * time.Second)
	outerRunStartedAt := base.Add(time.Second)
	next := base.Add(10 * time.Minute)
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: generation.ChannelID, GroupName: generation.CanonicalGroupName, GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
		LastProbeAt: &completedAt, NextProbeAt: &next,
		ConfirmationOutcome: "success", ConfirmationCount: 3, LastConfirmedAt: &completedAt, LastConfirmedRecoveryAt: &completedAt,
	}); err != nil {
		t.Fatal(err)
	}
	// The scheduler's scheduling pass can use a timestamp captured before its
	// three requests finish. It must not overwrite the completion-time recovery
	// fact with that older timestamp or a different outcome.
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: generation.ChannelID, GroupName: generation.CanonicalGroupName, GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
		LastProbeAt: &outerRunStartedAt, NextProbeAt: &next,
		ConfirmationOutcome: "failure", ConfirmationCount: 3, LastConfirmedAt: &outerRunStartedAt,
	}); err != nil {
		t.Fatal(err)
	}
	state, err := store.FindLatencyProbeState(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, generation)
	if err != nil || state == nil || state.LastProbeAt == nil || !state.LastProbeAt.Equal(completedAt) || state.LastConfirmedAt == nil || !state.LastConfirmedAt.Equal(completedAt) || state.LastConfirmedRecoveryAt == nil || !state.LastConfirmedRecoveryAt.Equal(completedAt) || state.ConfirmationOutcome != "success" || state.ConfirmationCount != 3 {
		t.Fatalf("confirmed recovery regressed: state=%+v err=%v", state, err)
	}

	// A later confirmed failure remains the newest diagnostic outcome, but it
	// must not overwrite the older recovery fence used to reject late failures.
	failureAt := completedAt.Add(time.Second)
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: generation.ChannelID, GroupName: generation.CanonicalGroupName, GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
		LastProbeAt: &failureAt, NextProbeAt: &next,
		ConfirmationOutcome: "failure", ConfirmationCount: 3, LastConfirmedAt: &failureAt,
	}); err != nil {
		t.Fatal(err)
	}
	state, err = store.FindLatencyProbeState(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, generation)
	if err != nil || state == nil || state.LastConfirmedAt == nil || !state.LastConfirmedAt.Equal(failureAt) || state.ConfirmationOutcome != "failure" || state.ConfirmationCount != 3 || state.LastConfirmedRecoveryAt == nil || !state.LastConfirmedRecoveryAt.Equal(completedAt) {
		t.Fatalf("later failure corrupted recovery fence or diagnostics: state=%+v err=%v", state, err)
	}

	// An unconfirmed scheduler update also advances schedule state without
	// erasing the durable confirmed recovery watermark.
	mixedAt := failureAt.Add(time.Second)
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: generation.ChannelID, GroupName: generation.CanonicalGroupName, GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
		LastProbeAt: &mixedAt, NextProbeAt: &next,
	}); err != nil {
		t.Fatal(err)
	}
	state, err = store.FindLatencyProbeState(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, generation)
	if err != nil || state == nil || state.LastConfirmedRecoveryAt == nil || !state.LastConfirmedRecoveryAt.Equal(completedAt) {
		t.Fatalf("unconfirmed schedule update erased recovery fence: state=%+v err=%v", state, err)
	}
}
