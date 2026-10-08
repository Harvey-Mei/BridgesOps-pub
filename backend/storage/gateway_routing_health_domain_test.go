package storage

import (
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestRoutingHealthCanonicalSurvivorMatchesCandidatePreference(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "routing survivor order", Type: ChannelTypeNewAPI, SiteURL: "https://routing-survivor.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	base := time.Now().UTC().Truncate(time.Microsecond)
	older := base.Add(-time.Minute)
	newer := base
	keys := []GatewayUpstreamKey{
		{ChannelID: channel.ID, GroupName: "VIP", Enabled: false, APIKeyCipher: "ready", ModelsSyncedAt: &newer},
		{ChannelID: channel.ID, GroupName: "Vip", Enabled: true, Provisioning: true, APIKeyCipher: "ready", ModelsSyncedAt: &newer},
		{ChannelID: channel.ID, GroupName: "vIP", Enabled: true, ModelsSyncedAt: &newer},
		{ChannelID: channel.ID, GroupName: "viP", Enabled: true, APIKeyCipher: "ready", ModelsSyncedAt: &older},
		{ChannelID: channel.ID, GroupName: "vip", Enabled: true, APIKeyCipher: "ready", ModelsSyncedAt: &newer},
		{ChannelID: channel.ID, GroupName: " VIP ", Enabled: true, APIKeyCipher: "ready", ModelsSyncedAt: &newer},
	}
	for index := range keys {
		if err := db.Create(&keys[index]).Error; err != nil {
			t.Fatal(err)
		}
	}
	successAt := base.Add(time.Second)
	if err := store.TouchUpstreamKey(channel.ID, "vIp", successAt); err != nil {
		t.Fatal(err)
	}
	for index := range keys {
		var stored GatewayUpstreamKey
		if err := db.First(&stored, keys[index].ID).Error; err != nil {
			t.Fatal(err)
		}
		if stored.ID == keys[len(keys)-1].ID {
			if stored.LastUsedAt == nil || !stored.LastUsedAt.Equal(successAt) {
				t.Fatalf("preferred survivor last_used_at = %v", stored.LastUsedAt)
			}
		} else if stored.LastUsedAt != nil {
			t.Fatalf("non-survivor %d retained last_used_at %v", stored.ID, stored.LastUsedAt)
		}
	}
}

func TestCaseVariantManualCooldownUpdatesCanonicalSurvivor(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "case manual cooldown", Type: ChannelTypeNewAPI, SiteURL: "https://case-manual.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	base := time.Now().UTC().Truncate(time.Microsecond)
	olderSync := base.Add(-time.Minute)
	newerSync := base
	createdAt := base.Add(-time.Hour)
	upper := GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "VIP", Enabled: true, APIKeyCipher: "upper", ModelsSyncedAt: &olderSync, CreatedAt: createdAt}
	lower := GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "vip", Enabled: true, APIKeyCipher: "lower", ModelsSyncedAt: &newerSync, CreatedAt: createdAt}
	for _, key := range []*GatewayUpstreamKey{&upper, &lower} {
		if err := db.Create(key).Error; err != nil {
			t.Fatal(err)
		}
	}
	requestLog := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: "VIP",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, FirstByteMs: 45_000, LatencyMs: 46_000,
		RouteReason: "smart_primary", CreatedAt: base,
	}
	routingGenerationForRequestLog(t, db, channel.ID, "VIP", &requestLog)
	if err := db.Create(&requestLog).Error; err != nil {
		t.Fatal(err)
	}
	appliedAt := base.Add(time.Minute)
	result, err := store.ApplyRequestLogManualRoutingCooldown(requestLog.ID, 10*time.Minute, "admin", "case-manual", appliedAt)
	if err != nil {
		t.Fatal(err)
	}
	if result.GroupName != lower.GroupName || !result.CooldownUntil.Equal(appliedAt.Add(10*time.Minute)) {
		t.Fatalf("manual cooldown result = %+v", result)
	}
	var storedUpper GatewayUpstreamKey
	if err := db.First(&storedUpper, upper.ID).Error; err != nil {
		t.Fatal(err)
	}
	var storedLower GatewayUpstreamKey
	if err := db.First(&storedLower, lower.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedUpper.LastUsedAt != nil || storedUpper.LastRoutingFailureAt != nil || storedUpper.RoutingCooldownUntil != nil {
		t.Fatalf("non-survivor manual health = %+v", storedUpper)
	}
	if storedLower.LastRoutingFailureKind != GatewayRoutingFailureManualCooldown || storedLower.LastRoutingFailureAt == nil || !storedLower.LastRoutingFailureAt.Equal(appliedAt) || storedLower.RoutingCooldownUntil == nil || !storedLower.RoutingCooldownUntil.Equal(result.CooldownUntil) {
		t.Fatalf("survivor manual health = %+v", storedLower)
	}
}

func TestCaseVariantSuccessClearsAdaptiveCountButPreservesManualCooldown(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "case manual success", Type: ChannelTypeNewAPI, SiteURL: "https://case-manual-success.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	oldFailureAt := base
	manualAt := base.Add(time.Minute)
	successAt := base.Add(2 * time.Minute)
	cooldownUntil := base.Add(15 * time.Minute)
	olderSync := base.Add(-time.Minute)
	newerSync := base
	upper := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "VIP", Enabled: true, APIKeyCipher: "upper", ModelsSyncedAt: &olderSync,
		LastUsedAt: &successAt,
	}
	lower := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "vip", Enabled: true, APIKeyCipher: "lower", ModelsSyncedAt: &newerSync,
		LastRoutingFailureError: "administrator manual cooldown", LastRoutingFailureKind: GatewayRoutingFailureManualCooldown,
		LastRoutingFailureAt: &manualAt, RoutingFailureCount: 4, RoutingCooldownUntil: &cooldownUntil,
	}
	for _, key := range []*GatewayUpstreamKey{&upper, &lower} {
		if err := db.Create(key).Error; err != nil {
			t.Fatal(err)
		}
	}
	routingGenerationForRequestLog(t, db, channel.ID, "VIP", &GatewayRequestLog{CreatedAt: base})
	if err := db.Create(&GatewayRoutingFailure{ChannelID: channel.ID, GroupName: "VIP", OccurredAt: oldFailureAt}).Error; err != nil {
		t.Fatal(err)
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		_, err := lockUpstreamRoutingHealthDomainTx(tx, channel.ID, " ViP ", successAt.Add(time.Minute))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var storedLower GatewayUpstreamKey
	if err := db.First(&storedLower, lower.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedLower.LastUsedAt == nil || !storedLower.LastUsedAt.Equal(successAt) || storedLower.LastRoutingFailureKind != GatewayRoutingFailureManualCooldown || storedLower.LastRoutingFailureAt == nil || !storedLower.LastRoutingFailureAt.Equal(manualAt) || storedLower.RoutingFailureCount != 0 || storedLower.RoutingCooldownUntil == nil || !storedLower.RoutingCooldownUntil.Equal(cooldownUntil) {
		t.Fatalf("manual cooldown merge after success = %+v", storedLower)
	}
	var storedUpper GatewayUpstreamKey
	if err := db.First(&storedUpper, upper.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedUpper.LastUsedAt != nil || storedUpper.LastRoutingFailureAt != nil || storedUpper.RoutingFailureCount != 0 || storedUpper.RoutingCooldownUntil != nil {
		t.Fatalf("non-survivor retained merged health = %+v", storedUpper)
	}
	var facts int64
	if err := db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "vip").Count(&facts).Error; err != nil || facts != 0 {
		t.Fatalf("adaptive failure facts after manual success merge = %d err=%v", facts, err)
	}
}

func TestDeleteGroupRoutingStateRemovesAllCanonicalVariants(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "canonical delete", Type: ChannelTypeNewAPI, SiteURL: "https://canonical-delete.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, groupName := range []string{"VIP", "vip"} {
		if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: groupName, Enabled: true, APIKeyCipher: "ready"}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&GatewayModelCapability{ChannelID: channel.ID, GroupName: groupName, ModelName: "gpt-test", Supported: true, LastCheckedAt: now}).Error; err != nil {
			t.Fatal(err)
		}
		candidateGroup := GatewayCandidateGroup{Name: "canonical delete " + groupName}
		if err := db.Create(&candidateGroup).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&GatewayCandidateGroupMember{CandidateGroupID: candidateGroup.ID, ChannelID: channel.ID, GroupName: groupName}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&GatewayRoutingFailure{ChannelID: channel.ID, GroupName: groupName, OccurredAt: now}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteGroupRoutingState(channel.ID, " ViP "); err != nil {
		t.Fatal(err)
	}
	for name, model := range map[string]any{
		"keys":         &GatewayUpstreamKey{},
		"capabilities": &GatewayModelCapability{},
		"candidates":   &GatewayCandidateGroupMember{},
		"failures":     &GatewayRoutingFailure{},
	} {
		var count int64
		if err := db.Model(model).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "vip").Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("canonical %s remaining = %d", name, count)
		}
	}
}

func TestCaseVariantSuccessAndFailureLinearizeByTimestamp(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "case health concurrency", Type: ChannelTypeNewAPI, SiteURL: "https://case-health-concurrency.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	base := time.Now().UTC().Truncate(time.Microsecond)
	olderSync := base.Add(-time.Minute)
	newerSync := base
	for _, key := range []GatewayUpstreamKey{
		{ChannelID: channel.ID, GroupName: "VIP", Enabled: true, APIKeyCipher: "upper", ModelsSyncedAt: &olderSync},
		{ChannelID: channel.ID, GroupName: "vip", Enabled: true, APIKeyCipher: "lower", ModelsSyncedAt: &newerSync},
	} {
		key := key
		if err := db.Create(&key).Error; err != nil {
			t.Fatal(err)
		}
	}

	runConcurrently := func(first, second func() error) {
		t.Helper()
		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() { <-start; errs <- first() }()
		go func() { <-start; errs <- second() }()
		close(start)
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
	}

	successAt := base.Add(time.Second)
	newFailureAt := base.Add(2 * time.Second)
	runConcurrently(
		func() error { return store.TouchUpstreamKey(channel.ID, "VIP", successAt) },
		func() error {
			return store.RecordUpstreamKeyRoutingFailure(channel.ID, "vip", "transport", 0, "newer failure", newFailureAt)
		},
	)
	key, err := store.FindUpstreamKeyAny(channel.ID, "VIP")
	if err != nil {
		t.Fatal(err)
	}
	if key.LastUsedAt == nil || !key.LastUsedAt.Equal(successAt) || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(newFailureAt) || key.RoutingFailureCount != 1 {
		t.Fatalf("newer failure did not survive interleaving: %+v", key)
	}

	olderFailureAt := base.Add(3 * time.Second)
	newerSuccessAt := base.Add(4 * time.Second)
	runConcurrently(
		func() error {
			return store.RecordUpstreamKeyRoutingFailure(channel.ID, "VIP", "transport", 0, "older failure", olderFailureAt)
		},
		func() error { return store.TouchUpstreamKey(channel.ID, "vip", newerSuccessAt) },
	)
	key, err = store.FindUpstreamKeyAny(channel.ID, "vip")
	if err != nil {
		t.Fatal(err)
	}
	if key.LastUsedAt == nil || !key.LastUsedAt.Equal(newerSuccessAt) || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("newer success did not clear interleaved failure: %+v", key)
	}
	var facts int64
	if err := db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "vip").Count(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if facts != 0 {
		t.Fatalf("routing failure facts after newer success = %d", facts)
	}
}

func TestCaseVariantSuccessWatermarkStartsNewFailureEpoch(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "case health watermark", Type: ChannelTypeNewAPI, SiteURL: "https://case-health-watermark.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	base := time.Now().UTC().Truncate(time.Microsecond)
	oldFailureAt := base.Add(time.Second)
	successAt := base.Add(2 * time.Second)
	newFailureAt := base.Add(3 * time.Second)
	olderSync := base.Add(-time.Minute)
	newerSync := base
	upper := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "VIP", Enabled: true, APIKeyCipher: "upper", ModelsSyncedAt: &olderSync,
		LastRoutingFailureError: "stale failure", LastRoutingFailureKind: "transport",
		LastRoutingFailureAt: &oldFailureAt, RoutingFailureCount: 1,
	}
	lower := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "vip", Enabled: true, APIKeyCipher: "lower", ModelsSyncedAt: &newerSync,
		LastUsedAt: &successAt,
	}
	for _, key := range []*GatewayUpstreamKey{&upper, &lower} {
		if err := db.Create(key).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&GatewayRoutingFailure{ChannelID: channel.ID, GroupName: "VIP", OccurredAt: oldFailureAt}).Error; err != nil {
		t.Fatal(err)
	}

	if err := store.RecordUpstreamKeyRoutingFailure(channel.ID, "ViP", "http_status", 503, "new failure", newFailureAt); err != nil {
		t.Fatal(err)
	}

	key, err := store.FindUpstreamKeyAny(channel.ID, "vip")
	if err != nil {
		t.Fatal(err)
	}
	if key.ID != lower.ID || key.LastUsedAt == nil || !key.LastUsedAt.Equal(successAt) || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(newFailureAt) || key.RoutingFailureCount != 1 {
		t.Fatalf("canonical health after new epoch failure = %+v", key)
	}
	var facts []GatewayRoutingFailure
	if err := db.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "vip").Order("occurred_at ASC").Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || !facts[0].OccurredAt.Equal(newFailureAt) {
		t.Fatalf("failure facts after success watermark = %+v", facts)
	}
	var storedUpper GatewayUpstreamKey
	if err := db.First(&storedUpper, upper.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedUpper.LastUsedAt != nil || storedUpper.LastRoutingFailureAt != nil || storedUpper.RoutingFailureCount != 0 || storedUpper.RoutingCooldownUntil != nil {
		t.Fatalf("non-survivor retained stale health = %+v", storedUpper)
	}
}

func TestCaseVariantSuccessWatermarkRecountsDuringMerge(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "case health merge recount", Type: ChannelTypeNewAPI, SiteURL: "https://case-health-merge-recount.example", MonitorEnabled: true}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	oldFailureAt := base.Add(time.Second)
	successAt := base.Add(2 * time.Second)
	newFailureAt := base.Add(3 * time.Second)
	olderSync := base.Add(-time.Minute)
	newerSync := base
	upper := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "VIP", Enabled: true, APIKeyCipher: "upper", ModelsSyncedAt: &olderSync,
		LastUsedAt: &successAt,
	}
	lower := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "vip", Enabled: true, APIKeyCipher: "lower", ModelsSyncedAt: &newerSync,
		LastRoutingFailureError: "new failure", LastRoutingFailureKind: "transport",
		LastRoutingFailureAt: &newFailureAt, RoutingFailureCount: 5,
	}
	staleCooldown := newFailureAt.Add(4 * time.Minute)
	lower.RoutingCooldownUntil = &staleCooldown
	for _, key := range []*GatewayUpstreamKey{&upper, &lower} {
		if err := db.Create(key).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, fact := range []GatewayRoutingFailure{
		{ChannelID: channel.ID, GroupName: "VIP", OccurredAt: oldFailureAt},
		{ChannelID: channel.ID, GroupName: "vip", OccurredAt: newFailureAt},
	} {
		fact := fact
		if err := db.Create(&fact).Error; err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		_, err := lockUpstreamRoutingHealthDomainTx(tx, channel.ID, "ViP", base.Add(4*time.Second))
		return err
	}); err != nil {
		t.Fatal(err)
	}

	var stored GatewayUpstreamKey
	if err := db.First(&stored, lower.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.RoutingFailureCount != 1 || stored.LastRoutingFailureAt == nil || !stored.LastRoutingFailureAt.Equal(newFailureAt) || stored.RoutingCooldownUntil != nil {
		t.Fatalf("merged survivor retained stale epoch count: %+v", stored)
	}
	var facts []GatewayRoutingFailure
	if err := db.Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "vip").Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || !facts[0].OccurredAt.Equal(newFailureAt) {
		t.Fatalf("facts after merge recount = %+v", facts)
	}
}

func TestConfirmedLatencyProbeRecoveryUsesSeparateWatermarkFromRealInferenceSuccess(t *testing.T) {
	store, generation := routingEnableFixture(t, "probe-recovery-watermark")
	base := time.Now().UTC().Truncate(time.Microsecond)
	failureAt := base.Add(time.Second)
	probeRecoveryAt := base.Add(2 * time.Second)
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "before probe recovery", failureAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, probeRecoveryAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastUsedAt != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("probe recovery impersonated real success or retained failure: key=%+v err=%v", key, err)
	}
	state, err := store.FindLatencyProbeState(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, generation)
	if err != nil || state == nil || state.LastConfirmedAt == nil || !state.LastConfirmedAt.Equal(probeRecoveryAt) || state.ConfirmationOutcome != "success" || state.ConfirmationCount != 3 {
		t.Fatalf("probe recovery fact=%+v err=%v", state, err)
	}

	// The older failure may arrive after the probe action acquired the lock. It
	// is retired by the distinct probe-recovery watermark, not LastUsedAt.
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "late before probe recovery", failureAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err = store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastUsedAt != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 {
		t.Fatalf("late old failure crossed probe recovery fact: key=%+v err=%v", key, err)
	}

	postRecoveryFailureAt := probeRecoveryAt.Add(time.Second)
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "http_status", 503, "after probe recovery", postRecoveryFailureAt, generation); err != nil {
		t.Fatal(err)
	}
	realSuccessAt := postRecoveryFailureAt.Add(time.Second)
	if err := store.TouchUpstreamKey(generation.ChannelID, generation.CanonicalGroupName, realSuccessAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err = store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastUsedAt == nil || !key.LastUsedAt.Equal(realSuccessAt) || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("real cross-key success no longer cleared health after probe recovery: key=%+v err=%v", key, err)
	}
}

func TestConfirmedLatencyProbeRecoveryPreservesActiveManualCooldown(t *testing.T) {
	store, generation := routingEnableFixture(t, "probe-recovery-manual")
	base := time.Now().UTC().Truncate(time.Microsecond)
	manualAt := base.Add(time.Second)
	cooldownUntil := manualAt.Add(10 * time.Minute)
	recoveryAt := manualAt.Add(time.Second)
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).Updates(map[string]any{
		"last_routing_failure_error":  "administrator manual cooldown",
		"last_routing_failure_kind":   GatewayRoutingFailureManualCooldown,
		"last_routing_failure_status": 0,
		"last_routing_failure_at":     manualAt,
		"routing_failure_count":       4,
		"routing_cooldown_until":      cooldownUntil,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, recoveryAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err = store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastUsedAt != nil || key.LastRoutingFailureKind != GatewayRoutingFailureManualCooldown || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(manualAt) || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil == nil || !key.RoutingCooldownUntil.Equal(cooldownUntil) {
		t.Fatalf("probe recovery changed active manual cooldown: key=%+v err=%v", key, err)
	}
}

func TestConfirmedLatencyProbeRecoveryRetainsNewIsolationAfterWatermark(t *testing.T) {
	store, generation := routingEnableFixture(t, "probe-recovery-new-isolation")
	fence := routingEnableFence(t, store, generation)
	base := time.Now().UTC().Truncate(time.Microsecond)
	recoveryAt := base.Add(time.Second)
	for attempt := 0; attempt < GatewayAdaptiveRoutingFailureThreshold; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(
			fence, "http_status", 503, "failure after recovery event",
			recoveryAt.Add(time.Duration(attempt+1)*time.Second),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(
		fence, GatewayAppCodex, recoveryAt,
	); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingIsolatedAt == nil || key.RoutingFailureCount != GatewayAdaptiveRoutingFailureThreshold ||
		key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.After(recoveryAt) {
		t.Fatalf("probe recovery released newer isolation: key=%+v err=%v", key, err)
	}
}

func TestConfirmedLatencyProbeRecoveryWatermarkSurvivesLaterProbeOutcomes(t *testing.T) {
	store, generation := routingEnableFixture(t, "probe-recovery-durable")
	base := time.Now().UTC().Truncate(time.Microsecond)
	oldFailureAt := base.Add(time.Second)
	recoveryAt := base.Add(2 * time.Second)
	laterConfirmedFailureAt := base.Add(3 * time.Second)
	mixedScheduleAt := base.Add(4 * time.Second)
	next := base.Add(10 * time.Minute)

	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "before durable recovery", oldFailureAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, recoveryAt, generation); err != nil {
		t.Fatal(err)
	}

	// Scheduler diagnostics retain a later confirmed failure without erasing the
	// earlier recovery event. The actual routing failure for this later probe is
	// recorded independently by runLatencyProbe.
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: generation.ChannelID, GroupName: generation.CanonicalGroupName, GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
		NextProbeAt: &next, LastProbeAt: &laterConfirmedFailureAt,
		ConfirmationOutcome: "failure", ConfirmationCount: 3, LastConfirmedAt: &laterConfirmedFailureAt,
	}); err != nil {
		t.Fatal(err)
	}
	state, err := store.FindLatencyProbeState(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, generation)
	if err != nil || state == nil || state.LastConfirmedAt == nil || !state.LastConfirmedAt.Equal(laterConfirmedFailureAt) || state.ConfirmationOutcome != "failure" || state.LastConfirmedRecoveryAt == nil || !state.LastConfirmedRecoveryAt.Equal(recoveryAt) {
		t.Fatalf("later probe outcome lost durable recovery fact: state=%+v err=%v", state, err)
	}
	if err := store.UpsertLatencyProbeState(&GatewayLatencyProbeState{
		ChannelID: generation.ChannelID, GroupName: generation.CanonicalGroupName, GatewayApp: GatewayAppCodex,
		UpstreamGroupID: uint64(generation.ID), PresenceGeneration: generation.PresenceGeneration,
		NextProbeAt: &next, LastProbeAt: &mixedScheduleAt,
	}); err != nil {
		t.Fatal(err)
	}

	// The t1 failure was already retired at t2. It may acquire the health lock
	// after later probe work, but must never count toward the t3 failure chain.
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "late pre-recovery failure", oldFailureAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("late pre-recovery failure recreated health after later probes: key=%+v err=%v", key, err)
	}
}

func TestConfirmedLatencyProbeRecoveryWatermarkIsGenerationBound(t *testing.T) {
	store, generation := routingEnableFixture(t, "probe-recovery-generation")
	base := time.Now().UTC().Truncate(time.Microsecond)
	oldFailureAt := base.Add(time.Second)
	recoveryAt := base.Add(2 * time.Second)
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, recoveryAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(generation.ChannelID, generation.CanonicalGroupName, "removed", 0, "", recoveryAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(generation.ChannelID, generation.CanonicalGroupName, "added", 0, "", recoveryAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	recreated, err := store.CaptureUpstreamGroupGeneration(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil {
		t.Fatal(err)
	}
	if recreated.PresenceGeneration == generation.PresenceGeneration {
		t.Fatalf("generation did not advance: before=%+v after=%+v", generation, recreated)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(recreated.ChannelID, recreated.CanonicalGroupName, "transport", 0, "new-generation failure", oldFailureAt, recreated); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKey(recreated.ChannelID, recreated.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(oldFailureAt) || key.RoutingFailureCount != 1 {
		t.Fatalf("old-generation recovery watermark crossed generations: key=%+v err=%v", key, err)
	}
}

func TestConfirmedLatencyProbeRecoveryWatermarkUsesLatestAppFactForGlobalHealth(t *testing.T) {
	store, generation := routingEnableFixture(t, "probe-recovery-app-facts")
	base := time.Now().UTC().Truncate(time.Microsecond)
	codexRecoveryAt := base.Add(2 * time.Second)
	claudeRecoveryAt := base.Add(3 * time.Second)
	lateBetweenAppRecoveriesAt := base.Add(2500 * time.Millisecond)
	postRecoveryFailureAt := base.Add(4 * time.Second)
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, codexRecoveryAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(generation.ChannelID, generation.CanonicalGroupName, GatewayAppClaude, claudeRecoveryAt, generation); err != nil {
		t.Fatal(err)
	}
	codexState, err := store.FindLatencyProbeState(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, generation)
	if err != nil || codexState == nil || codexState.LastConfirmedRecoveryAt == nil || !codexState.LastConfirmedRecoveryAt.Equal(codexRecoveryAt) {
		t.Fatalf("codex recovery state=%+v err=%v", codexState, err)
	}
	claudeState, err := store.FindLatencyProbeState(generation.ChannelID, generation.CanonicalGroupName, GatewayAppClaude, generation)
	if err != nil || claudeState == nil || claudeState.LastConfirmedRecoveryAt == nil || !claudeState.LastConfirmedRecoveryAt.Equal(claudeRecoveryAt) {
		t.Fatalf("claude recovery state=%+v err=%v", claudeState, err)
	}

	// Routing health is intentionally group-global. A later Claude confirmation
	// retires a late Codex-era failure even though their scheduler facts remain
	// app-scoped. A truly newer data-plane failure still starts a new chain.
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "late between app recoveries", lateBetweenAppRecoveriesAt, generation); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "after latest app recovery", postRecoveryFailureAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(postRecoveryFailureAt) || key.RoutingFailureCount != 1 {
		t.Fatalf("latest app recovery watermark did not protect global health: key=%+v err=%v", key, err)
	}
}

func TestConfirmedLatencyProbeRecoveryWatermarkSurvivesStoreRestart(t *testing.T) {
	store, generation := routingEnableFixture(t, "probe-recovery-restart")
	base := time.Now().UTC().Truncate(time.Microsecond)
	oldFailureAt := base.Add(time.Second)
	recoveryAt := base.Add(2 * time.Second)
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(generation.ChannelID, generation.CanonicalGroupName, GatewayAppCodex, recoveryAt, generation); err != nil {
		t.Fatal(err)
	}

	// A new Gateway instance models a process restart. The recovery fact must
	// remain durable rather than relying on an in-memory success cache.
	restarted := NewGateway(store.db)
	if err := restarted.RecordUpstreamKeyRoutingFailure(generation.ChannelID, generation.CanonicalGroupName, "transport", 0, "late after restart", oldFailureAt, generation); err != nil {
		t.Fatal(err)
	}
	key, err := restarted.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("restart lost durable recovery watermark: key=%+v err=%v", key, err)
	}
}
