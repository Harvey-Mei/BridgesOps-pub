package storage

import (
	"errors"
	"testing"
	"time"
)

func routingEnableFixture(t *testing.T, groupName string) (*Gateway, UpstreamGroupGeneration) {
	t.Helper()
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "routing enable " + groupName, Type: ChannelTypeNewAPI, SiteURL: "https://routing-enable.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: groupName, APIKeyCipher: "cipher", Enabled: true, LatencyProbeEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, groupName, "added", 1, "managed", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, groupName)
	if err != nil {
		t.Fatal(err)
	}
	return store, generation
}

func routingEnableFence(t testing.TB, store *Gateway, generation UpstreamGroupGeneration) GatewayRoutingHealthFence {
	t.Helper()
	var channel Channel
	if err := store.db.Select("upstream_account_identity_revision").First(&channel, generation.ChannelID).Error; err != nil {
		t.Fatal(err)
	}
	fence := NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
	if !fence.Valid() {
		t.Fatalf("invalid routing enable fence: %+v", fence)
	}
	return fence
}

func TestEnableUpstreamGroupRoutingClearsExactGenerationAndIsIdempotent(t *testing.T) {
	store, generation := routingEnableFixture(t, "adaptive")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Round(time.Second)
	for attempt := 0; attempt < 3; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "http_status", 503, "failed", base.Add(time.Duration(attempt)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}

	result, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-adaptive-1", "operator_one_click_enable", base.Add(5*time.Second))
	if err != nil {
		t.Fatalf("enable routing: %v", err)
	}
	if !result.Changed || result.IdempotentReplay || result.PreviousFailureCount != 3 || result.PreviousCooldownUntil != nil {
		t.Fatalf("enable result = %+v", result)
	}
	stored, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || stored.RoutingFailureCount != 0 || stored.LastRoutingFailureAt != nil || stored.RoutingCooldownUntil != nil || stored.RoutingIsolatedAt != nil || stored.LastRoutingFailureKind != "" {
		t.Fatalf("routing health was not cleared: key=%+v err=%v", stored, err)
	}
	var failureCount int64
	if err := store.db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", generation.ChannelID, generation.CanonicalGroupName).Count(&failureCount).Error; err != nil || failureCount != 0 {
		t.Fatalf("adaptive failure rows = %d err=%v", failureCount, err)
	}

	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "transport", 0, "new failure", base.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	replay, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-adaptive-1", "changed payload is ignored on replay", base.Add(7*time.Second))
	if err != nil || !replay.IdempotentReplay || replay.PreviousFailureCount != 3 {
		t.Fatalf("idempotent replay = %+v err=%v", replay, err)
	}
	stored, err = store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || stored.RoutingFailureCount != 1 || stored.RoutingIsolatedAt != nil || stored.LastRoutingFailureKind != "transport" {
		t.Fatalf("replay cleared newer failure: key=%+v err=%v", stored, err)
	}
	var audits int64
	if err := store.db.Model(&GatewayRoutingEnableAudit{}).Where("upstream_group_id = ?", generation.ID).Count(&audits).Error; err != nil || audits != 1 {
		t.Fatalf("routing enable audits = %d err=%v", audits, err)
	}
	if _, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "other-admin", "enable-adaptive-1", "operator_one_click_enable", base.Add(8*time.Second)); !errors.Is(err, ErrGatewayRoutingEnableIdempotencyConflict) {
		t.Fatalf("actor conflict = %v", err)
	}
}

func TestEnableUpstreamGroupRoutingRetainsNewIsolationAfterWatermark(t *testing.T) {
	store, generation := routingEnableFixture(t, "enable-new-isolation")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	enableAt := base.Add(time.Second)
	for attempt := 0; attempt < GatewayAdaptiveRoutingFailureThreshold; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(
			fence, "transport", 0, "failure after enable event",
			enableAt.Add(time.Duration(attempt+1)*time.Second),
		); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.EnableUpstreamGroupRouting(
		generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision,
		"admin", "enable-with-new-isolation", "operator_one_click_enable", enableAt,
	)
	if err != nil || !result.Changed {
		t.Fatalf("enable routing with newer failures: result=%+v err=%v", result, err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingIsolatedAt == nil || key.RoutingFailureCount != GatewayAdaptiveRoutingFailureThreshold ||
		key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.After(enableAt) {
		t.Fatalf("routing enable released newer isolation: key=%+v err=%v", key, err)
	}
}

func TestEnableUpstreamGroupRoutingPreservesManualCooldownAudit(t *testing.T) {
	store, generation := routingEnableFixture(t, "manual")
	fence := routingEnableFence(t, store, generation)
	requestAt := time.Now().UTC().Add(time.Second)
	requestLog := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: generation.ChannelID, GroupName: generation.CanonicalGroupName,
		Method: "POST", Path: "/v1/responses", StatusCode: 200, FirstByteMs: 45_000, LatencyMs: 46_000,
		RouteReason: "smart_primary", CreatedAt: requestAt,
	}
	bindRoutingGenerationToRequestLog(t, store.db, generation, &requestLog)
	if err := store.db.Create(&requestLog).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyRequestLogManualRoutingCooldown(requestLog.ID, 10*time.Minute, "admin", "manual-cooldown-1", requestAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-manual-1", "operator_one_click_enable", requestAt.Add(2*time.Second))
	if err != nil || !result.Changed || result.PreviousFailureKind != GatewayRoutingFailureManualCooldown || result.PreviousCooldownUntil == nil {
		t.Fatalf("enable manual cooldown = %+v err=%v", result, err)
	}
	var manualAudits, enableAudits int64
	if err := store.db.Model(&GatewayManualRoutingCooldownAudit{}).Where("request_log_id = ?", requestLog.ID).Count(&manualAudits).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.db.Model(&GatewayRoutingEnableAudit{}).Where("upstream_group_id = ?", generation.ID).Count(&enableAudits).Error; err != nil {
		t.Fatal(err)
	}
	if manualAudits != 1 || enableAudits != 1 {
		t.Fatalf("manual audit=%d enable audit=%d", manualAudits, enableAudits)
	}
}

func TestEnableUpstreamGroupRoutingRejectsStaleGenerationAndProvisioningKey(t *testing.T) {
	store, generation := routingEnableFixture(t, "structural")
	fence := routingEnableFence(t, store, generation)
	if err := store.db.Model(&GatewayUpstreamKey{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", generation.ChannelID, generation.CanonicalGroupName).
		Update("provisioning", true).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-provisioning", "operator_one_click_enable", time.Now().UTC()); !errors.Is(err, ErrGatewayRoutingEnableUpstreamKeyUnavailable) {
		t.Fatalf("provisioning key error = %v", err)
	}
	if err := store.db.Model(&GatewayUpstreamGroup{}).Where("id = ?", generation.ID).
		Update("presence_generation", generation.PresenceGeneration+1).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-stale", "operator_one_click_enable", time.Now().UTC()); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale generation error = %v", err)
	}
	var audits int64
	if err := store.db.Model(&GatewayRoutingEnableAudit{}).Where("upstream_group_id = ?", generation.ID).Count(&audits).Error; err != nil || audits != 0 {
		t.Fatalf("rejected action audits = %d err=%v", audits, err)
	}
}

func TestEnableUpstreamGroupRoutingRejectsPriorChannelIdentity(t *testing.T) {
	store, generation := routingEnableFixture(t, "account-switch")
	oldFence := routingEnableFence(t, store, generation)
	newRevision := oldFence.ChannelIdentityRevision + 1
	if err := store.db.Exec(
		"UPDATE channels SET upstream_account_identity_revision = ? WHERE id = ?",
		newRevision, generation.ChannelID,
	).Error; err != nil {
		t.Fatal(err)
	}
	newFence := NewGatewayRoutingHealthFence(generation, newRevision)
	failureAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(newFence, "transport", 0, "new account failure", failureAt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnableUpstreamGroupRouting(
		generation.ID, generation.PresenceGeneration, oldFence.ChannelIdentityRevision,
		"admin", "enable-old-account", "stale_operator_page", failureAt.Add(time.Second),
	); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale account enable error = %v", err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(failureAt) ||
		key.RoutingFailureCount != 1 || !newFence.MatchesUpstreamKey(key) {
		t.Fatalf("stale account enable changed current health: key=%+v err=%v", key, err)
	}
	var audits int64
	if err := store.db.Model(&GatewayRoutingEnableAudit{}).Where("upstream_group_id = ?", generation.ID).Count(&audits).Error; err != nil || audits != 0 {
		t.Fatalf("stale account enable audits = %d err=%v", audits, err)
	}

	result, err := store.EnableUpstreamGroupRouting(
		generation.ID, generation.PresenceGeneration, newRevision,
		"admin", "enable-new-account", "current_operator_page", failureAt.Add(2*time.Second),
	)
	if err != nil || result.ChannelIdentityRevision != newRevision || !result.Changed {
		t.Fatalf("current account enable = %+v err=%v", result, err)
	}
}

func TestEnableUpstreamGroupRoutingWatermarkRejectsLatePreEnableFailure(t *testing.T) {
	store, generation := routingEnableFixture(t, "watermark")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	failureAt := base.Add(time.Second)
	enableAt := base.Add(2 * time.Second)

	if _, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-watermark", "operator_one_click_enable", enableAt); err != nil {
		t.Fatal(err)
	}
	// This models a data-plane failure that happened before the button click but
	// waited behind the canonical health lock until after the enable completed.
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "transport", 0, "late pre-enable failure", failureAt); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("late pre-enable failure recreated health: key=%+v err=%v", key, err)
	}
	var facts int64
	if err := store.db.Model(&GatewayRoutingFailure{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", generation.ChannelID, generation.CanonicalGroupName).
		Count(&facts).Error; err != nil || facts != 0 {
		t.Fatalf("late pre-enable failure facts=%d err=%v", facts, err)
	}

	postEnableAt := enableAt.Add(time.Second)
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "http_status", 503, "post-enable failure", postEnableAt); err != nil {
		t.Fatal(err)
	}
	key, err = store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(postEnableAt) || key.LastRoutingFailureKind != "http_status" || key.RoutingFailureCount != 1 {
		t.Fatalf("post-enable failure was incorrectly rejected: key=%+v err=%v", key, err)
	}
}

func TestEnableUpstreamGroupRoutingConcurrentOldFailureCannotRecreateCooldown(t *testing.T) {
	store, generation := routingEnableFixture(t, "watermark-concurrent")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	failureAt := base.Add(time.Second)
	enableAt := base.Add(2 * time.Second)
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-concurrent-watermark", "operator_one_click_enable", enableAt)
		errs <- err
	}()
	go func() {
		<-start
		errs <- store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "transport", 0, "old concurrent failure", failureAt)
	}()
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("concurrent old failure recreated health: key=%+v err=%v", key, err)
	}
	var facts int64
	if err := store.db.Model(&GatewayRoutingFailure{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", generation.ChannelID, generation.CanonicalGroupName).
		Count(&facts).Error; err != nil || facts != 0 {
		t.Fatalf("concurrent old failure facts=%d err=%v", facts, err)
	}
}

func TestEnableUpstreamGroupRoutingPreservesFailureAfterItsWatermark(t *testing.T) {
	store, generation := routingEnableFixture(t, "watermark-future")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	enableAt := base.Add(time.Second)
	failureAt := base.Add(2 * time.Second)
	// The failure writes first because it acquired the lock first, but its
	// event timestamp is after the administrator action. Enable must not erase
	// a genuinely newer failure merely because lock acquisition was reordered.
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(fence, "http_status", 503, "newer failure", failureAt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-preserve-future", "operator_one_click_enable", enableAt); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(failureAt) || key.LastRoutingFailureKind != "http_status" || key.LastRoutingFailureStatus != 503 || key.RoutingFailureCount != 1 {
		t.Fatalf("enable erased failure after its watermark: key=%+v err=%v", key, err)
	}
}

func TestEnableUpstreamGroupRoutingWatermarkIsGenerationBound(t *testing.T) {
	store, generation := routingEnableFixture(t, "watermark-generation")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	enableAt := base.Add(2 * time.Second)
	if _, err := store.EnableUpstreamGroupRouting(generation.ID, generation.PresenceGeneration, fence.ChannelIdentityRevision, "admin", "enable-generation-watermark", "operator_one_click_enable", enableAt); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(generation.ChannelID, generation.CanonicalGroupName, "removed", 0, "", enableAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(generation.ChannelID, generation.CanonicalGroupName, "added", 0, "", enableAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	recreated, err := store.CaptureUpstreamGroupGeneration(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil {
		t.Fatal(err)
	}
	if recreated.PresenceGeneration == generation.PresenceGeneration {
		t.Fatalf("generation did not advance: before=%+v after=%+v", generation, recreated)
	}
	recreatedFence := routingEnableFence(t, store, recreated)
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(recreatedFence, "transport", 0, "new generation failure", enableAt); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKey(recreated.ChannelID, recreated.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(enableAt) || key.RoutingFailureCount != 1 {
		t.Fatalf("old-generation enable watermark crossed generations: key=%+v err=%v", key, err)
	}
}
