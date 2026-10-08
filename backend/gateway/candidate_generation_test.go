package gateway

import (
	"errors"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestSmartCandidatesFailClosedWithoutCurrentUpstreamGeneration(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "missing smart generation", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://missing-smart-generation.example", PasswordCipher: "cipher", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	// Simulate a legacy/corrupt row that predates the presence-generation
	// writer. Rates.Upsert now creates the generation atomically, so using it
	// here would no longer exercise the fail-closed path named by this test.
	if err := db.Create(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now()}).Error; err != nil {
		t.Fatal(err)
	}
	syncedAt := time.Now().UTC()
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", Enabled: true, APIKeyCipher: "cipher",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		ModelFamilies:       "codex", ModelIDs: "gpt-test", ModelsSyncedAt: &syncedAt,
	}); err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, rates, store, nil, nil, nil)
	candidates, exclusions, err := service.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err == nil || len(candidates) != 0 {
		t.Fatalf("smart route accepted missing generation: candidates=%+v err=%v", candidates, err)
	}
	if len(exclusions) != 1 || exclusions[0].ChannelID != channel.ID || exclusions[0].GroupName != "primary" || exclusions[0].Reason != "upstream_generation_unavailable" {
		t.Fatalf("missing generation exclusions = %+v", exclusions)
	}

	fixed, err := service.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "primary", "gpt-test")
	var gateErr *ManualRouteGateError
	if err == nil || len(fixed) != 0 || !errors.As(err, &gateErr) || gateErr.Reason != ManualRouteGateReasonGenerationUnavailable {
		t.Fatalf("fixed route accepted missing current generation: candidates=%+v err=%v", fixed, err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "managed", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-test", generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
		t.Fatal(err)
	}
	fixed, err = service.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "primary", "gpt-test")
	if err != nil || len(fixed) != 1 || !fixed[0].UpstreamGeneration.Valid() {
		t.Fatalf("fixed route did not retain a current generation fence: candidates=%+v err=%v", fixed, err)
	}
}

func TestCandidateExclusionSnapshotFactsAreGenerationBound(t *testing.T) {
	cooldownUntil := time.Date(2026, time.July, 23, 12, 34, 56, 0, time.FixedZone("test", 8*60*60))
	candidate := Candidate{
		Channel: storage.Channel{ID: 17, Name: "safe channel"},
		Rate:    storage.RateSnapshot{ModelName: " VIP "},
		UpstreamGeneration: storage.UpstreamGroupGeneration{
			ID:                 23,
			ChannelID:          17,
			CanonicalGroupName: "vip",
			PresenceGeneration: 5,
		},
		FailureCount:  4,
		CooldownUntil: &cooldownUntil,
	}

	exclusion := newCandidateExclusion(candidate, candidateExclusionReasonUpstreamTemporarilyUnavailable, "private upstream error")
	if exclusion.UpstreamGroupID != 23 || exclusion.PresenceGeneration != 5 || exclusion.FailureCount != 4 {
		t.Fatalf("snapshot facts = %+v", exclusion)
	}
	if exclusion.CooldownUntil == nil || !exclusion.CooldownUntil.Equal(cooldownUntil.UTC()) {
		t.Fatalf("snapshot cooldown = %v, want %v", exclusion.CooldownUntil, cooldownUntil.UTC())
	}
	if exclusion.CooldownUntil == candidate.CooldownUntil {
		t.Fatal("snapshot retained mutable cooldown pointer")
	}
	if exclusion.Reason != candidateExclusionReasonUpstreamTemporarilyUnavailable {
		t.Fatalf("snapshot reason = %q", exclusion.Reason)
	}

	mismatched := candidate
	mismatched.UpstreamGeneration.CanonicalGroupName = "other"
	exclusion = newCandidateExclusion(mismatched, candidateExclusionReasonModelNotListed, "gpt-private")
	if exclusion.UpstreamGroupID != 0 || exclusion.PresenceGeneration != 0 {
		t.Fatalf("mismatched generation leaked into snapshot: %+v", exclusion)
	}
	if got := normalizeCandidateExclusionReason("private upstream reason"); got != candidateExclusionReasonUnknown {
		t.Fatalf("unrecognized reason = %q, want %q", got, candidateExclusionReasonUnknown)
	}
}

func TestStaleSmartCandidateCannotWriteRecreatedGenerationHealth(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "stale smart generation", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://stale-smart-generation.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "primary", Enabled: true, APIKeyCipher: "old-cipher"}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "old-key", base); err != nil {
		t.Fatal(err)
	}
	staleGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 1, "old-key", base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 2, "new-key", base.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	failureAt := base.Add(3 * time.Minute)
	cooldownUntil := failureAt.Add(10 * time.Minute)
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND group_name = ?", channel.ID, "primary").
		Updates(map[string]any{
			"api_key_cipher":              "new-cipher",
			"last_routing_failure_error":  "current generation failure",
			"last_routing_failure_kind":   RoutingFailureTransport,
			"last_routing_failure_at":     failureAt,
			"routing_failure_count":       1,
			"routing_cooldown_until":      cooldownUntil,
			"last_routing_failure_status": 0,
		}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&storage.GatewayRoutingFailure{ChannelID: channel.ID, GroupName: "primary", OccurredAt: failureAt}).Error; err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, storage.NewRates(db), store, nil, nil, nil)
	service.TouchUpstreamGroupAt(channel.ID, "primary", base.Add(4*time.Minute), staleGeneration)
	service.MarkUpstreamGroupRoutingFailure(channel.ID, "primary", RoutingFailureHTTPStatus, 503, "stale candidate failure", staleGeneration)

	current, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if current.LastUsedAt != nil || current.LastRoutingFailureError != "current generation failure" || current.LastRoutingFailureAt == nil || !current.LastRoutingFailureAt.Equal(failureAt) || current.RoutingFailureCount != 1 || current.RoutingCooldownUntil == nil || !current.RoutingCooldownUntil.Equal(cooldownUntil) {
		t.Fatalf("stale candidate changed current generation health: %+v", current)
	}
	var facts int64
	if err := db.Model(&storage.GatewayRoutingFailure{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "primary").Count(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if facts != 1 {
		t.Fatalf("stale candidate changed current generation facts = %d", facts)
	}
}

func TestCaseVariantUpsertCannotBypassCanonicalCooldown(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "case upsert cooldown", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://case-upsert-cooldown.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "VIP", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "VIP", "added", 1, "managed", base); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "VIP")
	if err != nil {
		t.Fatal(err)
	}
	olderSync := base.Add(time.Second)
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "VIP", Enabled: true, APIKeyCipher: "old-cipher",
		ModelFamilies: "codex", ModelIDs: "gpt-test", ModelsSyncedAt: &olderSync,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "VIP", "codex", "gpt-test", olderSync.Add(time.Second), "", generation); err != nil {
		t.Fatal(err)
	}
	failureAt := base.Add(2 * time.Second)
	cooldownUntil := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Microsecond)
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND group_name = ?", channel.ID, "VIP").
		Updates(map[string]any{
			"last_routing_failure_error": "transport failure", "last_routing_failure_kind": RoutingFailureTransport,
			"last_routing_failure_at": failureAt, "routing_failure_count": 4, "routing_cooldown_until": cooldownUntil,
			"routing_isolated_at":              failureAt,
			"routing_health_upstream_group_id": generation.ID, "routing_health_presence_generation": generation.PresenceGeneration,
			"routing_health_channel_identity_revision": channel.UpstreamAccountIdentityRevision,
		}).Error; err != nil {
		t.Fatal(err)
	}
	newerSync := base.Add(3 * time.Second)
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "vip", Enabled: true, APIKeyCipher: "new-cipher",
		ModelFamilies: "codex", ModelIDs: "gpt-test", ModelsSyncedAt: &newerSync,
	}); err != nil {
		t.Fatal(err)
	}

	var rows int64
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "vip").Count(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("case-variant upsert created %d canonical rows", rows)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "vip")
	if err != nil {
		t.Fatal(err)
	}
	if stored.APIKeyCipher != "new-cipher" || stored.LastRoutingFailureAt == nil || !stored.LastRoutingFailureAt.Equal(failureAt) || stored.RoutingFailureCount != 4 || stored.RoutingCooldownUntil == nil || !stored.RoutingCooldownUntil.Equal(cooldownUntil) || stored.RoutingIsolatedAt == nil || !stored.RoutingIsolatedAt.Equal(failureAt) {
		t.Fatalf("case-variant upsert lost canonical cooldown: %+v", stored)
	}
	service := NewService(channels, rates, store, nil, nil, nil)
	candidates, exclusions, err := service.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err == nil || len(candidates) != 0 {
		t.Fatalf("case-variant upsert bypassed cooldown: candidates=%+v err=%v", candidates, err)
	}
	if len(exclusions) != 1 || exclusions[0].Reason != "upstream_temporarily_unavailable" || exclusions[0].ChannelID != channel.ID {
		t.Fatalf("case-variant cooldown exclusions = %+v", exclusions)
	}
}
