package gateway

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func requireDirectGatewayDB(t *testing.T) {
	t.Helper()
	if strings.TrimSpace(os.Getenv("BRIDGEOPS_TEST_POSTGRES_DSN")) == "" {
		t.Skip("BRIDGEOPS_TEST_POSTGRES_DSN is required")
	}
}

func TestRequestUsesDirectCostBasisUsesPersistedRequestFact(t *testing.T) {
	direct := storage.GatewayRequestLog{ChannelID: 7, CostBasisSource: " Direct "}
	managed := storage.GatewayRequestLog{ChannelID: 7, CostBasisSource: "channel_balance"}
	if !requestUsesDirectCostBasis(direct) {
		t.Fatal("persisted direct cost basis was not recognized")
	}
	if requestUsesDirectCostBasis(managed) {
		t.Fatal("managed cost basis was classified as direct")
	}
}

func TestDirectAuthenticationInvalidationSurvivesBackgroundSync(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("direct-auth-invalidation-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("sk-rejected-direct")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "direct invalidation", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://direct-invalidation.invalid", CredentialMode: storage.CredentialModeDirect,
		PasswordCipher: secret, DirectModels: storage.EncodeDirectModels([]string{"vendor-alias"}),
		GatewayApp: storage.GatewayAppCodex, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, storage.NewRates(db), store, cipher, &fakeKeyManager{}, nil)
	if _, err := service.syncChannelModelFamilies(context.Background(), channel, true); err != nil {
		t.Fatalf("initial direct sync: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	fence := storage.NewGatewayUpstreamKeyInvalidationFence(generation, channel.UpstreamAccountIdentityRevision, key.ID, key.KeyGeneration)
	if err := store.DisableUpstreamKeyForIdentity(fence, "observed 401"); err != nil {
		t.Fatal(err)
	}

	if _, err := service.syncChannelModelFamilies(context.Background(), channel, true); !errors.Is(err, storage.ErrGatewayUpstreamKeyAuthenticationInvalidated) {
		t.Fatalf("same-identity background sync error = %v", err)
	}
	disabled, err := store.FindUpstreamKeyAny(channel.ID, storage.DirectChannelGroupName)
	if err != nil || disabled.Enabled || disabled.APIKeyCipher != "" || disabled.AuthenticationInvalidatedChannelIdentityRevision != channel.UpstreamAccountIdentityRevision {
		t.Fatalf("invalidated direct key was restored: key=%+v err=%v", disabled, err)
	}

	channel.SiteURL = "https://direct-replacement.invalid"
	if err := channels.UpdateAdminFields(channel, storage.ChannelAdminFieldSiteURL); err != nil {
		t.Fatalf("advance direct channel identity: %v", err)
	}
	if _, err := service.syncChannelModelFamilies(context.Background(), channel, true); err != nil {
		t.Fatalf("new-identity direct sync: %v", err)
	}
	restored, err := store.FindUpstreamKeyAny(channel.ID, storage.DirectChannelGroupName)
	if err != nil || !restored.Enabled || restored.APIKeyCipher == "" || restored.AuthenticationInvalidatedChannelIdentityRevision != 0 {
		t.Fatalf("new direct identity did not restore routing: key=%+v err=%v", restored, err)
	}
}

func TestDirectBackgroundSyncPreservesKeyGenerationForInFlightAuthenticationFence(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("direct-generation-stability-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("sk-stable-direct")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "direct generation", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://direct-generation.invalid", CredentialMode: storage.CredentialModeDirect,
		PasswordCipher: secret, DirectModels: storage.EncodeDirectModels([]string{"vendor-alias"}),
		GatewayApp: storage.GatewayAppCodex, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, storage.NewRates(db), store, cipher, &fakeKeyManager{}, nil)
	if _, err := service.syncChannelModelFamilies(context.Background(), channel, true); err != nil {
		t.Fatalf("initial direct sync: %v", err)
	}
	before, err := store.FindUpstreamKeyAny(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	fence := storage.NewGatewayUpstreamKeyInvalidationFence(generation, channel.UpstreamAccountIdentityRevision, before.ID, before.KeyGeneration)

	if _, err := service.syncChannelModelFamilies(context.Background(), channel, true); err != nil {
		t.Fatalf("repeat direct sync: %v", err)
	}
	after, err := store.FindUpstreamKeyAny(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	if after.KeyGeneration != before.KeyGeneration || after.APIKeyCipher != before.APIKeyCipher {
		t.Fatalf("same direct key changed identity: before=%d after=%d cipher_changed=%t", before.KeyGeneration, after.KeyGeneration, after.APIKeyCipher != before.APIKeyCipher)
	}
	if err := store.DisableUpstreamKeyForIdentity(fence, "observed delayed 401"); err != nil {
		t.Fatalf("in-flight authentication fence was invalidated by background sync: %v", err)
	}
}

func TestInvalidateDirectRoutingPreservesStateWhenChannelUpdateFails(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("direct-invalidation-recovery-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("sk-direct-recovery")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "direct invalidation recovery", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://direct-invalidation-recovery.invalid", CredentialMode: storage.CredentialModeDirect,
		PasswordCipher: secret, DirectModels: storage.EncodeDirectModels([]string{"vendor-alias"}),
		GatewayApp: storage.GatewayAppCodex, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: storage.DirectChannelGroupName,
		Description: "direct rate evidence", Ratio: 1.25, CompletionRatio: 1.5,
		LastSeenAt: observedAt,
	}); err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, rates, store, cipher, &fakeKeyManager{}, nil)
	if _, err := service.syncChannelModelFamilies(context.Background(), channel, true); err != nil {
		t.Fatal(err)
	}
	generationBefore, err := store.CaptureUpstreamGroupGeneration(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	healthFence := storage.NewGatewayRoutingHealthFence(generationBefore, channel.UpstreamAccountIdentityRevision)
	failureAt := observedAt.Add(10 * time.Second)
	for attempt := 0; attempt < 3; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(
			healthFence, RoutingFailureHTTPStatus, 503, "direct upstream unavailable",
			failureAt.Add(time.Duration(attempt)*time.Millisecond),
		); err != nil {
			t.Fatalf("record direct routing failure %d: %v", attempt+1, err)
		}
	}
	keyBefore, err := store.FindUpstreamKeyAny(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	invalidationFence := storage.NewGatewayUpstreamKeyInvalidationFence(
		generationBefore, channel.UpstreamAccountIdentityRevision, keyBefore.ID, keyBefore.KeyGeneration,
	)
	if err := store.DisableUpstreamKeyForIdentity(invalidationFence, "observed direct 401"); err != nil {
		t.Fatal(err)
	}
	keyBefore, err = store.FindUpstreamKeyAny(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	rateBefore, err := rates.FindByChannelAndModel(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	var failuresBefore int64
	if err := db.Model(&storage.GatewayRoutingFailure{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, storage.DirectChannelGroupName).
		Count(&failuresBefore).Error; err != nil {
		t.Fatal(err)
	}
	updateErr := errors.New("direct channel update failed")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = service.WithChannelOperationLockAndThen(ctx, channel.ID, func(lockedCtx context.Context) error {
		return service.InvalidateDirectRoutingStateAndThen(lockedCtx, channel.ID, true, func(context.Context) error {
			return updateErr
		})
	})
	if !errors.Is(err, updateErr) {
		t.Fatalf("direct invalidation error = %v", err)
	}
	rateAfter, err := rates.FindByChannelAndModel(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatalf("direct rate changed after failed update: %v", err)
	}
	if rateAfter.ID != rateBefore.ID || rateAfter.Description != rateBefore.Description ||
		rateAfter.Ratio != rateBefore.Ratio || rateAfter.CompletionRatio != rateBefore.CompletionRatio ||
		!rateAfter.FirstSeenAt.Equal(rateBefore.FirstSeenAt) || !rateAfter.LastSeenAt.Equal(rateBefore.LastSeenAt) {
		t.Fatalf("direct rate mutated after failed update: before=%+v after=%+v", rateBefore, rateAfter)
	}
	keyAfter, err := store.FindUpstreamKeyAny(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatalf("direct key changed after failed update: %v", err)
	}
	if keyAfter.ID != keyBefore.ID || keyAfter.KeyGeneration != keyBefore.KeyGeneration ||
		keyAfter.Enabled != keyBefore.Enabled || keyAfter.APIKeyCipher != keyBefore.APIKeyCipher ||
		keyAfter.APIKeyPrefix != keyBefore.APIKeyPrefix || keyAfter.LastError != keyBefore.LastError ||
		keyAfter.AuthenticationInvalidatedChannelIdentityRevision != keyBefore.AuthenticationInvalidatedChannelIdentityRevision ||
		keyAfter.LastRoutingFailureError != keyBefore.LastRoutingFailureError ||
		keyAfter.LastRoutingFailureKind != keyBefore.LastRoutingFailureKind ||
		keyAfter.LastRoutingFailureStatus != keyBefore.LastRoutingFailureStatus ||
		keyAfter.RoutingFailureCount != keyBefore.RoutingFailureCount ||
		keyAfter.RoutingHealthUpstreamGroupID != keyBefore.RoutingHealthUpstreamGroupID ||
		keyAfter.RoutingHealthPresenceGeneration != keyBefore.RoutingHealthPresenceGeneration ||
		keyAfter.RoutingHealthChannelIdentityRevision != keyBefore.RoutingHealthChannelIdentityRevision ||
		keyAfter.LastRoutingFailureAt == nil || keyBefore.LastRoutingFailureAt == nil ||
		!keyAfter.LastRoutingFailureAt.Equal(*keyBefore.LastRoutingFailureAt) ||
		(keyAfter.RoutingCooldownUntil == nil) != (keyBefore.RoutingCooldownUntil == nil) ||
		(keyAfter.RoutingCooldownUntil != nil && !keyAfter.RoutingCooldownUntil.Equal(*keyBefore.RoutingCooldownUntil)) {
		t.Fatalf("direct key evidence mutated after failed update: before=%+v after=%+v", keyBefore, keyAfter)
	}
	generationAfter, err := store.CaptureUpstreamGroupGeneration(channel.ID, storage.DirectChannelGroupName)
	if err != nil || generationAfter != generationBefore {
		t.Fatalf("direct generation changed after failed update: before=%+v after=%+v err=%v", generationBefore, generationAfter, err)
	}
	var failuresAfter int64
	if err := db.Model(&storage.GatewayRoutingFailure{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, storage.DirectChannelGroupName).
		Count(&failuresAfter).Error; err != nil {
		t.Fatal(err)
	}
	if failuresAfter != failuresBefore || failuresBefore != 3 {
		t.Fatalf("direct failure facts changed after failed update: before=%d after=%d", failuresBefore, failuresAfter)
	}
}

func TestDirectChannelUsesExactSelectedModelsWithoutKeyManagement(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("direct-gateway-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("sk-direct")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "direct", Type: storage.ChannelTypeNewAPI, SiteURL: "https://direct.invalid",
		CredentialMode: storage.CredentialModeDirect, PasswordCipher: secret,
		DirectModels: storage.EncodeDirectModels([]string{"vendor-alias", "claude-sonnet-4"}),
		GatewayApp:   storage.GatewayAppCodex, MonitorEnabled: true, FundingFactor: 9,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: storage.DirectChannelGroupName,
		Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{}
	service := NewService(channels, rates, store, cipher, keys, nil)
	items, err := service.syncChannelModelFamilies(context.Background(), channel, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].GroupName != storage.DirectChannelGroupName {
		t.Fatalf("direct sync items = %+v", items)
	}
	lease, err := service.EnsureUpstreamAPIKeyLeaseForGroup(context.Background(), channel.ID, storage.DirectChannelGroupName, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Key != "sk-direct" || lease.UpstreamKeyID != 0 || lease.LocalKeyID == 0 || lease.KeyGeneration == 0 {
		t.Fatalf("direct lease = %+v", lease)
	}
	candidates, err := service.CandidatesForApp(storage.GatewayAppCodex, "claude-sonnet-4")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].Channel.ID != channel.ID || candidates[0].FundingFactor != 1 || candidates[0].CostBasisSource != "direct" {
		t.Fatalf("direct candidates = %+v", candidates)
	}
	if keys.listGroupCalls != 0 || len(keys.created) != 0 || len(keys.revealCalls) != 0 {
		t.Fatalf("direct route called key management: groups=%d creates=%d reveals=%d", keys.listGroupCalls, len(keys.created), len(keys.revealCalls))
	}
}

func TestDirectCandidatesAndPreviewUseCurrentChannelPolicyWhenCachedFactsLag(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	service := NewService(channels, rates, store, nil, nil, nil)
	channel := &storage.Channel{
		Name: "direct current policy", Type: storage.ChannelTypeNewAPI, SiteURL: "https://direct-policy.invalid",
		CredentialMode: storage.CredentialModeDirect, PasswordCipher: "cipher",
		DirectModels: storage.EncodeDirectModels([]string{"Current-Alias"}),
		GatewayApp:   storage.GatewayAppClaude, MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: storage.DirectChannelGroupName,
		Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: storage.DirectChannelGroupName,
		UpstreamKeyName: directUpstreamKeyName, APIKeyCipher: "cipher", Enabled: true,
		ModelFamilies: "codex", ModelIDs: "cached-old",
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	if err := store.RecordUpstreamGroupChange(channel.ID, storage.DirectChannelGroupName, "added", 0, directUpstreamKeyName, now); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, storage.DirectChannelGroupName)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamiliesForIdentity(
		storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision),
		"codex", "cached-old", now.Add(time.Second), "",
	); err != nil {
		t.Fatal(err)
	}

	candidates, err := service.CandidatesForApp(storage.GatewayAppClaude, "Current-Alias")
	if err != nil || len(candidates) != 1 {
		t.Fatalf("current direct candidates = %+v err=%v", candidates, err)
	}
	if candidates[0].App != storage.GatewayAppClaude || !slices.Equal(candidates[0].ModelIDs, []string{"Current-Alias"}) ||
		!slices.Equal(candidates[0].ModelFamilies, []string{"claude"}) {
		t.Fatalf("current direct candidate policy = app=%q models=%v families=%v", candidates[0].App, candidates[0].ModelIDs, candidates[0].ModelFamilies)
	}
	if stale, staleErr := service.CandidatesForApp(storage.GatewayAppClaude, "cached-old"); staleErr == nil || len(stale) != 0 {
		t.Fatalf("stale cached direct model remained routable: candidates=%+v err=%v", stale, staleErr)
	}
	if wrongApp, wrongAppErr := service.CandidatesForApp(storage.GatewayAppCodex, "Current-Alias"); wrongAppErr == nil || len(wrongApp) != 0 {
		t.Fatalf("stale cached direct protocol remained routable: candidates=%+v err=%v", wrongApp, wrongAppErr)
	}

	group := &storage.GatewayCandidateGroup{Name: "direct current policy group"}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: storage.DirectChannelGroupName}}); err != nil {
		t.Fatal(err)
	}
	downstream := &storage.GatewayDownstreamKey{
		Name: "direct current policy key", App: storage.GatewayAppClaude, ModelFamily: "claude",
		CandidateGroupID: group.ID, KeyHash: "direct-current-policy", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(downstream); err != nil {
		t.Fatal(err)
	}
	previews, err := service.CandidateGroupMemberPreviewForDownstreamKey(downstream.ID, "Current-Alias", false)
	if err != nil || len(previews) != 1 {
		t.Fatalf("current direct preview = %+v err=%v", previews, err)
	}
	preview := previews[0]
	if !preview.RoutingAvailable || preview.App != storage.GatewayAppClaude || !slices.Equal(preview.ModelIDs, []string{"Current-Alias"}) ||
		!slices.Equal(preview.ModelFamilies, []string{"claude"}) || containsString(preview.AvailabilityReasons, "gateway_app_mismatch") ||
		containsString(preview.AvailabilityReasons, "model_family_mismatch") {
		t.Fatalf("current direct preview policy = %+v", preview)
	}
	stalePreviews, err := service.CandidateGroupMemberPreviewForDownstreamKey(downstream.ID, "cached-old", false)
	if err != nil || len(stalePreviews) != 1 || stalePreviews[0].RoutingAvailable ||
		!containsString(stalePreviews[0].AvailabilityReasons, "model_not_listed") ||
		!slices.Equal(stalePreviews[0].ModelIDs, []string{"Current-Alias"}) {
		t.Fatalf("stale cached direct preview remained routable: previews=%+v err=%v", stalePreviews, err)
	}
}

func TestRetireManagedChannelRoutingDeletesRemoteKeysAndLocalState(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "retire-managed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	keyName := upstreamProvisioningKeyPrefix + "0123456789abcdef"
	cipher, err := appcrypto.NewCipher("retire-update-failure-secret")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := cipher.Encrypt("sk-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7, UpstreamKeyName: keyName,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        plain, APIKeyPrefix: "sk-test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	remote := []connector.APIKey{managedUpstreamKeyFixture(7, keyName, "promo")}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}}, listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		return &connector.APIKeyPage{Page: 1, Pages: 1, Items: remote}, nil
	}}
	service := NewService(channels, rates, store, cipher, keys, nil)
	validationErr := errors.New("invalid prospective direct configuration")
	if err := service.RetireManagedChannelRoutingAndThen(
		context.Background(), channel.ID,
		func(context.Context) error { return validationErr },
		func(context.Context) error { t.Fatal("update ran after failed validation"); return nil },
	); !errors.Is(err, validationErr) {
		t.Fatalf("pre-retirement validation error = %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("remote keys deleted before direct validation: %v", keys.deleted)
	}
	validated, updated := false, false
	if err := service.RetireManagedChannelRoutingAndThen(
		context.Background(),
		channel.ID,
		func(ctx context.Context) error {
			validated = storage.ChannelOperationLockHeld(ctx, channel.ID)
			return nil
		},
		func(ctx context.Context) error {
			updated = storage.ChannelOperationLockHeld(ctx, channel.ID)
			return nil
		},
	); err != nil {
		t.Fatalf("retire managed routing: %v", err)
	}
	if !validated || !updated {
		t.Fatalf("managed-to-direct callbacks escaped channel lock: validated=%t updated=%t", validated, updated)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != 7 {
		t.Fatalf("deleted remote keys = %v, want [7]", keys.deleted)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("local upstream key remains after retirement: %v", err)
	}
}

func TestRetireManagedChannelRoutingLeavesPartialFailureRecoverable(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "retire-partial", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	remote := make([]connector.APIKey, 0, 2)
	for index, groupName := range []string{"alpha", "beta"} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
		keyID := int64(index + 7)
		keyName := upstreamProvisioningKeyPrefix + []string{"0123456789abcdef", "fedcba9876543210"}[index]
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: groupName, UpstreamKeyID: keyID, UpstreamKeyName: keyName,
			OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
			APIKeyCipher:        "cipher", APIKeyPrefix: "sk-test", Enabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		remote = append(remote, managedUpstreamKeyFixture(keyID, keyName, groupName))
	}
	keys := &fakeKeyManager{
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: remote}, nil
		},
		deleteAPIKey: func(id int64) error {
			if id == 8 {
				return errors.New("temporary upstream delete failure")
			}
			return nil
		},
	}
	service := NewService(channels, rates, store, nil, keys, nil)
	err := service.RetireManagedChannelRouting(context.Background(), channel.ID)
	if err == nil || !strings.Contains(err.Error(), "temporary upstream delete failure") {
		t.Fatalf("partial retirement error = %v", err)
	}
	if len(keys.deleted) != 2 || keys.deleted[0] != 7 || keys.deleted[1] != 8 {
		t.Fatalf("remote delete attempts = %v, want [7 8]", keys.deleted)
	}
	latest, findErr := channels.FindByID(channel.ID)
	if findErr != nil || latest.CredentialMode == storage.CredentialModeDirect {
		t.Fatalf("partial retirement replaced managed credentials: channel=%+v err=%v", latest, findErr)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "beta"); err != nil {
		t.Fatalf("failed remote deletion lost recoverable local beta key: %v", err)
	}
}

func TestRetireManagedChannelRoutingRestoresKeysWhenLocalUpdateFails(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "retire-update-failure", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	keyName := upstreamProvisioningKeyPrefix + "0123456789abcdef"
	cipher, err := appcrypto.NewCipher("retire-update-failure-secret")
	if err != nil {
		t.Fatal(err)
	}
	keyCipher, err := cipher.Encrypt("sk-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7, UpstreamKeyName: keyName,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        keyCipher, APIKeyPrefix: "sk-test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(7, keyName, "promo")
	remoteInventory := map[int64]connector.APIKey{remote.ID: remote}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}}, listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		items := make([]connector.APIKey, 0, len(remoteInventory))
		for _, item := range remoteInventory {
			items = append(items, item)
		}
		return &connector.APIKeyPage{Page: 1, Pages: 1, Items: items}, nil
	}}
	keys.deleteAPIKey = func(id int64) error {
		delete(remoteInventory, id)
		return nil
	}
	service := NewService(channels, rates, store, cipher, keys, nil)
	updateErr := errors.New("local direct-mode update failed")
	err = service.RetireManagedChannelRoutingAndThen(
		context.Background(), channel.ID, nil,
		func(context.Context) error { return updateErr },
	)
	if !errors.Is(err, updateErr) {
		t.Fatalf("retirement update error = %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != 7 {
		t.Fatalf("deleted remote keys = %v, want [7]", keys.deleted)
	}
	if len(keys.created) != 1 || keys.created[0].Group != "promo" {
		t.Fatalf("managed key was not restored before unlock: %+v", keys.created)
	}
	latest, findErr := channels.FindByID(channel.ID)
	if findErr != nil || latest.CredentialMode == storage.CredentialModeDirect {
		t.Fatalf("failed update replaced managed credentials: channel=%+v err=%v", latest, findErr)
	}
}

func TestRetireManagedChannelRoutingRejectsAmbiguousManagedKey(t *testing.T) {
	requireDirectGatewayDB(t)
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	service := NewService(channels, storage.NewRates(db), storage.NewGateway(db), nil, &fakeKeyManager{
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{{
				ID: 9, Name: upstreamProvisioningKeyPrefix + "0123456789abcdef",
			}}}, nil
		},
	}, nil)
	channel := &storage.Channel{Name: "ambiguous-managed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	err := service.RetireManagedChannelRouting(context.Background(), channel.ID)
	if err == nil || !strings.Contains(err.Error(), "cannot be safely mapped") {
		t.Fatalf("retire ambiguous key error = %v", err)
	}
	keys := service.Keys.(*fakeKeyManager)
	if len(keys.deleted) != 0 {
		t.Fatalf("ambiguous remote key was deleted: %v", keys.deleted)
	}
}

func TestDirectChannelModelFamiliesFollowProtocol(t *testing.T) {
	channel := &storage.Channel{GatewayApp: storage.GatewayAppCodex}
	got := directChannelModelFamilies(channel, []string{"claude-sonnet-4"})
	if len(got) != 1 || got[0] != string(storage.GatewayAppCodex) {
		t.Fatalf("families = %#v, want [codex]", got)
	}
	if !directModelIDSupportsRequest([]string{"vendor-alias"}, "vendor-alias") {
		t.Fatal("exact administrator-selected model ID was rejected")
	}
	if directModelIDSupportsRequest([]string{"Vendor-Alias"}, "vendor-alias") {
		t.Fatal("case-insensitive direct model ID match was accepted")
	}
}

func TestDirectModelHelpersUseExactConfiguredAliases(t *testing.T) {
	candidate := Candidate{
		Channel:  storage.Channel{CredentialMode: storage.CredentialModeDirect, GatewayApp: storage.GatewayAppClaude},
		App:      storage.GatewayAppClaude,
		ModelIDs: []string{"vendor-alias"},
	}
	if got := latencyProbeModelForCandidate(candidate); got != "vendor-alias" {
		t.Fatalf("direct latency probe model = %q, want vendor-alias", got)
	}
	service := &Service{}
	compatible, exclusions, err := service.FilterCandidatesForMappedModel([]Candidate{candidate}, "vendor-alias")
	if err != nil || len(compatible) != 1 || len(exclusions) != 0 {
		t.Fatalf("direct mapped alias filter: compatible=%+v exclusions=%+v err=%v", compatible, exclusions, err)
	}
	compatible, exclusions, err = service.FilterCandidatesForMappedModel([]Candidate{candidate}, "Vendor-Alias")
	if err != nil || len(compatible) != 0 || len(exclusions) != 1 {
		t.Fatalf("case-mismatched direct alias filter: compatible=%+v exclusions=%+v err=%v", compatible, exclusions, err)
	}
}

func TestDirectChannelAllowsOnlyCurrentProtocolAndExactModels(t *testing.T) {
	channel := storage.Channel{
		CredentialMode: storage.CredentialModeDirect,
		GatewayApp:     storage.GatewayAppClaude,
		DirectModels:   storage.EncodeDirectModels([]string{"Vendor-Alias"}),
	}
	if !DirectChannelAllowsModel(channel, storage.GatewayAppClaude, "Vendor-Alias") {
		t.Fatal("current exact direct model was rejected")
	}
	if DirectChannelAllowsModel(channel, storage.GatewayAppClaude, "vendor-alias") {
		t.Fatal("case-mismatched direct model was accepted")
	}
	if DirectChannelAllowsModel(channel, storage.GatewayAppCodex, "Vendor-Alias") {
		t.Fatal("direct model was accepted for another protocol")
	}
	channel.DirectModels = `not-json`
	if DirectChannelAllowsModel(channel, storage.GatewayAppClaude, "Vendor-Alias") {
		t.Fatal("invalid direct allowlist failed open")
	}
}

func TestDirectModelCheckHelpersUseExactConfiguredAliases(t *testing.T) {
	source := storage.GatewayModelCheckSource{
		CredentialMode:    storage.CredentialModeDirect,
		ModelIDs:          []string{"Vendor-Alias"},
		LatencyProbeModel: "vendor-alias",
	}
	if got := modelCheckFallbackModel(source); got != "Vendor-Alias" {
		t.Fatalf("case-mismatched direct model-check fallback = %q, want selected exact model", got)
	}
	source.LatencyProbeModel = "Vendor-Alias"
	if got := modelCheckFallbackModel(source); got != "Vendor-Alias" {
		t.Fatalf("exact direct model-check fallback = %q", got)
	}
}

func TestDirectCandidatePreviewModelGateUsesExactConfiguredAliases(t *testing.T) {
	channel := storage.Channel{
		CredentialMode: storage.CredentialModeDirect,
		GatewayApp:     storage.GatewayAppClaude,
		DirectModels:   storage.EncodeDirectModels([]string{"Vendor-Alias"}),
	}
	if modelAvailableForChannel(channel, []string{"Vendor-Alias"}, "vendor-alias", storage.GatewayAppClaude) {
		t.Fatal("case-mismatched direct preview model was accepted")
	}
	if !modelAvailableForChannel(channel, []string{"Vendor-Alias"}, "Vendor-Alias", storage.GatewayAppClaude) {
		t.Fatal("exact direct preview model was rejected")
	}
}
