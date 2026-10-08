package storage

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestChannelDeletionReservedNameDispositionNeverDeletesAdoptedKey(t *testing.T) {
	reserved := GatewayUpstreamKey{UpstreamKeyName: "BOG-p-0123456789abcdef"}
	if got := channelDeletionKeyDispositionForKey(reserved, GatewayUpstreamKeyProvenanceUnknown); got != ChannelDeletionKeyDispositionVerifyManagedDelete {
		t.Fatalf("reserved unknown-key disposition = %q", got)
	}
	if got := channelDeletionKeyDispositionForKey(reserved, GatewayUpstreamKeyProvenanceCreated); got != ChannelDeletionKeyDispositionVerifyManagedDelete {
		t.Fatalf("reserved created-key disposition = %q", got)
	}
	if got := channelDeletionKeyDispositionForKey(reserved, GatewayUpstreamKeyProvenanceAdopted); got != ChannelDeletionKeyDispositionRetainAdopted {
		t.Fatalf("reserved adopted-key disposition = %q", got)
	}
}

func createDirectChannelDeletionFixture(t *testing.T) (*gorm.DB, *Channels, *Channel, *GatewayUpstreamKey) {
	t.Helper()
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: t.Name(), Type: ChannelTypeNewAPI,
		SiteURL: "https://direct-deletion.invalid", PasswordCipher: "direct-channel-cipher",
		CredentialMode: CredentialModeDirect, DirectModels: EncodeDirectModels([]string{"vendor-alias"}),
		GatewayApp: GatewayAppCodex, MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create direct channel: %v", err)
	}
	if _, err := NewRates(db).Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: DirectChannelGroupName, Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create direct rate: %v", err)
	}
	key := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: " DIRECT ", UpstreamKeyID: 0,
		UpstreamKeyName: DirectChannelUpstreamKeyName, OwnershipProvenance: GatewayUpstreamKeyProvenanceUnknown,
		APIKeyCipher: "direct-routing-cipher", APIKeyPrefix: "sk-direct", Enabled: true,
	}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create direct routing key: %v", err)
	}
	return db, channels, channel, key
}

func TestChannelDeletePrepareRetiresSyntheticDirectRoutingStateWithoutClaim(t *testing.T) {
	db, channels, channel, key := createDirectChannelDeletionFixture(t)
	now := time.Now().UTC()
	if err := db.Create(&GatewayModelCapability{
		ChannelID: channel.ID, GroupName: "Direct", ModelName: "vendor-alias", Supported: true, LastCheckedAt: now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayRoutingFailure{
		ChannelID: channel.ID, GroupName: "direct", OccurredAt: now,
	}).Error; err != nil {
		t.Fatal(err)
	}

	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare direct deletion: %v", err)
	}
	if err := db.First(&GatewayUpstreamKey{}, key.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("synthetic direct key lookup error = %v", err)
	}
	for name, model := range map[string]any{
		"claim":      &ChannelDeletionKeyClaim{},
		"audit":      &ChannelDeletionKeyAudit{},
		"capability": &GatewayModelCapability{},
		"failure":    &GatewayRoutingFailure{},
	} {
		var count int64
		if err := db.Model(model).Where("channel_id = ?", channel.ID).Count(&count).Error; err != nil {
			t.Fatalf("count %s: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("direct deletion prepare retained %s rows = %d", name, count)
		}
	}
}

func TestChannelDeletePrepareKeepsUnexpectedDirectModeKeyForReview(t *testing.T) {
	db, channels, channel, directKey := createDirectChannelDeletionFixture(t)
	unexpected := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "Direct", UpstreamKeyID: 77,
		UpstreamKeyName: DirectChannelUpstreamKeyName, OwnershipProvenance: GatewayUpstreamKeyProvenanceUnknown,
		APIKeyCipher: "unexpected-cipher", APIKeyPrefix: "sk-unexpected", Enabled: true,
	}
	if err := db.Create(&unexpected).Error; err != nil {
		t.Fatal(err)
	}

	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare direct deletion with unexpected key: %v", err)
	}
	if err := db.First(&GatewayUpstreamKey{}, directKey.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("synthetic direct key lookup error = %v", err)
	}
	var retained GatewayUpstreamKey
	if err := db.First(&retained, unexpected.ID).Error; err != nil {
		t.Fatalf("unexpected key was discarded: %v", err)
	}
	var claim ChannelDeletionKeyClaim
	if err := db.Where("channel_id = ? AND local_key_id = ?", channel.ID, unexpected.ID).First(&claim).Error; err != nil {
		t.Fatalf("load unexpected-key claim: %v", err)
	}
	if claim.Status != ChannelDeletionKeyManualReviewRequired || claim.Disposition != ChannelDeletionKeyDispositionManualReview {
		t.Fatalf("unexpected direct-mode key claim = %+v", claim)
	}
}

func TestChannelDeletePrepareRollsBackSyntheticDirectStateOnFailure(t *testing.T) {
	db, channels, channel, key := createDirectChannelDeletionFixture(t)
	injectedErr := errors.New("injected direct deletion prepare failure")
	const callbackName = "channel_deletion_prepare:fail_direct_intent_create"
	if err := db.Callback().Create().Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
		if tx != nil && tx.Error == nil && tx.Statement != nil && tx.Statement.Table == (ChannelDeletionIntent{}).TableName() {
			tx.AddError(injectedErr)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Create().Remove(callbackName) })

	if err := channels.Delete(channel.ID); !errors.Is(err, injectedErr) {
		t.Fatalf("direct deletion prepare error = %v", err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.DeletionPending || stored.DeletionEpoch != 0 || stored.DeletionState != "" || !stored.MonitorEnabled || !stored.LatencyProbeEnabled {
		t.Fatalf("direct channel changed after rollback: %+v", stored)
	}
	var restored GatewayUpstreamKey
	if err := db.First(&restored, key.ID).Error; err != nil {
		t.Fatalf("direct routing key was not restored by rollback: %v", err)
	}
	if restored.APIKeyCipher != key.APIKeyCipher || restored.Enabled != key.Enabled {
		t.Fatalf("restored direct routing key changed: before=%+v after=%+v", key, restored)
	}
	if _, err := NewRates(db).FindByChannelAndModel(channel.ID, DirectChannelGroupName); err != nil {
		t.Fatalf("direct rate was not restored by rollback: %v", err)
	}
}

func TestChannelDeletePreparesIdentityFencedIntentAndRetainsLocalEvidence(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "deletion-prepare", Type: ChannelTypeNewAPI, SiteURL: "https://deletion-prepare.example",
		Username: "operator", PasswordCipher: "cipher", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&AuthSession{ChannelID: channel.ID, AccessTokenCipher: "session-cipher"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := NewRates(db).Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "created", Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	keys := []*GatewayUpstreamKey{
		{
			ChannelID: channel.ID, GroupName: "created", UpstreamKeyID: 101, UpstreamKeyName: "managed-created",
			KeyGeneration: 3, OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
			APIKeyCipher: "created-cipher", APIKeyPrefix: "sk-created", Enabled: true,
		},
		{
			ChannelID: channel.ID, GroupName: "adopted", UpstreamKeyID: 102, UpstreamKeyName: "manual-key",
			KeyGeneration: 4, OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
			APIKeyCipher: "adopted-cipher", APIKeyPrefix: "sk-adopted", Enabled: true,
		},
		{
			ChannelID: channel.ID, GroupName: "unknown", UpstreamKeyID: 103, UpstreamKeyName: "unknown-key",
			KeyGeneration: 5, OwnershipProvenance: GatewayUpstreamKeyProvenanceUnknown,
			APIKeyCipher: "unknown-cipher", APIKeyPrefix: "sk-unknown", Enabled: false,
		},
	}
	for _, key := range keys {
		if err := db.Create(key).Error; err != nil {
			t.Fatal(err)
		}
	}

	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("delete prepare error = %v, want ErrChannelDeletionPending", err)
	}

	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find pending channel: %v", err)
	}
	if !stored.DeletionPending || stored.DeletionEpoch != 1 ||
		stored.DeletionIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		stored.DeletionState != string(ChannelDeletionIntentPendingRemoteCleanup) ||
		stored.DeletionRequestedAt == nil || stored.MonitorEnabled || stored.LatencyProbeEnabled {
		t.Fatalf("prepared channel state = %+v", stored)
	}
	if stored.AdminRevision != channel.AdminRevision+1 {
		t.Fatalf("prepare did not invalidate old admin snapshot: revision=%d original=%d", stored.AdminRevision, channel.AdminRevision)
	}
	if _, err := channels.RequirePendingDeletionFence(channel.ID, stored.DeletionEpoch, stored.DeletionIdentityRevision); err != nil {
		t.Fatalf("require current deletion fence: %v", err)
	}
	if _, err := channels.RequirePendingDeletionFence(channel.ID, stored.DeletionEpoch+1, stored.DeletionIdentityRevision); !errors.Is(err, ErrStaleChannelDeletionFence) {
		t.Fatalf("wrong deletion epoch error = %v", err)
	}

	for _, check := range []struct {
		name  string
		model any
	}{
		{name: "auth session", model: &AuthSession{}},
		{name: "upstream key", model: &GatewayUpstreamKey{}},
	} {
		var count int64
		if err := db.Model(check.model).Where("channel_id = ?", channel.ID).Count(&count).Error; err != nil {
			t.Fatalf("count %s: %v", check.name, err)
		}
		if count == 0 {
			t.Fatalf("delete prepare removed %s", check.name)
		}
	}
	var remainingRates, presentGroups int64
	if err := db.Model(&RateSnapshot{}).Where("channel_id = ?", channel.ID).Count(&remainingRates).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayUpstreamGroup{}).Where("channel_id = ? AND present = ?", channel.ID, true).Count(&presentGroups).Error; err != nil {
		t.Fatal(err)
	}
	if remainingRates != 0 || presentGroups != 0 {
		t.Fatalf("delete prepare left rate/group presence: rates=%d present_groups=%d", remainingRates, presentGroups)
	}

	var intent ChannelDeletionIntent
	if err := db.Where("channel_id = ? AND deletion_epoch = ?", channel.ID, stored.DeletionEpoch).First(&intent).Error; err != nil {
		t.Fatalf("read deletion intent: %v", err)
	}
	if intent.ChannelIdentityRevision != stored.DeletionIdentityRevision ||
		intent.ChannelNameSnapshot != channel.Name || intent.ChannelTypeSnapshot != channel.Type ||
		intent.Status != ChannelDeletionIntentPendingRemoteCleanup || intent.ID == 0 {
		t.Fatalf("prepared deletion intent = %+v", intent)
	}

	var audits []ChannelDeletionKeyAudit
	if err := db.Where("intent_id = ?", intent.ID).Order("local_key_id ASC").Find(&audits).Error; err != nil {
		t.Fatalf("read key audits: %v", err)
	}
	if len(audits) != len(keys) {
		t.Fatalf("prepared key audits = %d, want %d", len(audits), len(keys))
	}
	var claims []ChannelDeletionKeyClaim
	if err := db.Where("intent_id = ?", intent.ID).Order("local_key_id ASC").Find(&claims).Error; err != nil {
		t.Fatalf("read deletion key claims: %v", err)
	}
	if len(claims) != len(keys) {
		t.Fatalf("prepared key claims = %d, want %d", len(claims), len(keys))
	}
	byLocalID := make(map[uint]ChannelDeletionKeyAudit, len(audits))
	for _, audit := range audits {
		byLocalID[audit.LocalKeyID] = audit
		if audit.IntentID != intent.ID || audit.ChannelID != channel.ID || audit.DeletionEpoch != stored.DeletionEpoch ||
			audit.ChannelIdentityRevision != stored.DeletionIdentityRevision || audit.CreatedAt.IsZero() {
			t.Fatalf("audit identity = %+v", audit)
		}
	}
	if got := byLocalID[keys[0].ID]; got.Disposition != ChannelDeletionKeyDispositionVerifyManagedDelete || got.OwnershipProvenance != GatewayUpstreamKeyProvenanceCreated || got.UpstreamGroupID == nil || got.PresenceGeneration == nil {
		t.Fatalf("created key audit = %+v", got)
	}
	if got := byLocalID[keys[1].ID]; got.Disposition != ChannelDeletionKeyDispositionRetainAdopted || got.OwnershipProvenance != GatewayUpstreamKeyProvenanceAdopted {
		t.Fatalf("adopted key was marked deletable: %+v", got)
	}
	if got := byLocalID[keys[2].ID]; got.Disposition != ChannelDeletionKeyDispositionManualReview || got.OwnershipProvenance != GatewayUpstreamKeyProvenanceUnknown {
		t.Fatalf("unknown key audit = %+v", got)
	}
	claimByLocalID := make(map[uint]ChannelDeletionKeyClaim, len(claims))
	for _, claim := range claims {
		claimByLocalID[claim.LocalKeyID] = claim
		if claim.IntentID != intent.ID || claim.ChannelID != channel.ID ||
			claim.DeletionEpoch != stored.DeletionEpoch ||
			claim.ChannelIdentityRevision != stored.DeletionIdentityRevision ||
			claim.ClaimDigest == "" || claim.ClaimDigestVersion != channelDeletionClaimDigestVersion {
			t.Fatalf("prepared claim identity = %+v", claim)
		}
	}
	if got := claimByLocalID[keys[0].ID]; got.Status != ChannelDeletionKeyPendingVerification || got.Disposition != ChannelDeletionKeyDispositionVerifyManagedDelete {
		t.Fatalf("created key claim = %+v", got)
	}
	if got := claimByLocalID[keys[1].ID]; got.Status != ChannelDeletionKeyPendingVerification || got.Disposition != ChannelDeletionKeyDispositionRetainAdopted {
		t.Fatalf("adopted key claim = %+v", got)
	}
	if got := claimByLocalID[keys[2].ID]; got.Status != ChannelDeletionKeyManualReviewRequired || got.Disposition != ChannelDeletionKeyDispositionManualReview {
		t.Fatalf("unknown key claim = %+v", got)
	}
	var frozenKeys []GatewayUpstreamKey
	if err := db.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&frozenKeys).Error; err != nil {
		t.Fatalf("read frozen upstream keys: %v", err)
	}
	for _, key := range frozenKeys {
		claim, ok := claimByLocalID[key.ID]
		if !ok || key.Enabled || key.ChannelDeletionIntentID == nil || *key.ChannelDeletionIntentID != intent.ID ||
			key.ChannelDeletionEpoch == nil || *key.ChannelDeletionEpoch != stored.DeletionEpoch ||
			key.ChannelDeletionIdentityRevision == nil || *key.ChannelDeletionIdentityRevision != stored.DeletionIdentityRevision ||
			key.ChannelDeletionUpstreamGroupID == nil || claim.UpstreamGroupID == nil || *key.ChannelDeletionUpstreamGroupID != *claim.UpstreamGroupID ||
			key.ChannelDeletionPresenceGeneration == nil || claim.PresenceGeneration == nil || *key.ChannelDeletionPresenceGeneration != *claim.PresenceGeneration {
			t.Fatalf("upstream key was not frozen with its claim: key=%+v claim=%+v", key, claim)
		}
	}
	if err := db.Model(&ChannelDeletionKeyAudit{}).Where("id = ?", audits[0].ID).Update("reason", "forged").Error; err == nil {
		t.Fatal("deletion key audit update unexpectedly succeeded")
	}
	if err := db.Delete(&ChannelDeletionKeyAudit{}, audits[0].ID).Error; err == nil {
		t.Fatal("deletion key audit delete unexpectedly succeeded")
	}
}

func TestChannelDeletePrepareIsIdempotentWithinTheSameEpoch(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{Name: "deletion-idempotent", Type: ChannelTypeSub2API, SiteURL: "https://deletion-idempotent.example", PasswordCipher: "cipher", MonitorEnabled: true, LatencyProbeEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "manual", UpstreamKeyID: 7, UpstreamKeyName: "manual-key",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", APIKeyPrefix: "sk", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}

	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("first prepare error = %v", err)
	}
	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("second prepare error = %v", err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.DeletionEpoch != 1 {
		t.Fatalf("deletion epoch = %d, want 1", stored.DeletionEpoch)
	}
	var intents, audits, claims int64
	if err := db.Model(&ChannelDeletionIntent{}).Where("channel_id = ?", channel.ID).Count(&intents).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelDeletionKeyAudit{}).Where("channel_id = ?", channel.ID).Count(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelDeletionKeyClaim{}).Where("channel_id = ?", channel.ID).Count(&claims).Error; err != nil {
		t.Fatal(err)
	}
	if intents != 1 || audits != 1 || claims != 1 {
		t.Fatalf("idempotent prepare wrote intents=%d audits=%d claims=%d, want 1/1/1", intents, audits, claims)
	}
}
