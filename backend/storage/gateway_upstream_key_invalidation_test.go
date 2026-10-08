package storage

import (
	"errors"
	"testing"
	"time"
)

func upstreamKeyInvalidationFixture(t *testing.T) (*Gateway, *Channel, UpstreamGroupGeneration, *GatewayUpstreamKey) {
	t.Helper()
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: "key invalidation fence", Type: ChannelTypeNewAPI,
		SiteURL: "https://key-invalidation.invalid", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	key := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "old-cipher",
		APIKeyPrefix: "old-prefix", Enabled: true,
	}
	if err := db.Create(key).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "managed", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	return store, channel, generation, key
}

func TestDisableUpstreamKeyForIdentityRejectsPriorAccountRevision(t *testing.T) {
	store, channel, generation, key := upstreamKeyInvalidationFixture(t)
	fence := NewGatewayUpstreamKeyInvalidationFence(generation, channel.UpstreamAccountIdentityRevision, key.ID, key.KeyGeneration)
	newRevision := channel.UpstreamAccountIdentityRevision + 1
	if err := store.db.Model(&Channel{}).Where("id = ?", channel.ID).
		Update("upstream_account_identity_revision", newRevision).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.db.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).
		Updates(map[string]any{"api_key_cipher": "replacement-cipher", "api_key_prefix": "replacement", "enabled": true}).Error; err != nil {
		t.Fatal(err)
	}

	if err := store.DisableUpstreamKeyForIdentity(fence, "stale auth failure"); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale account invalidation error = %v", err)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil || !stored.Enabled || stored.APIKeyCipher != "replacement-cipher" || stored.APIKeyPrefix != "replacement" {
		t.Fatalf("stale account invalidation changed replacement key: key=%+v err=%v", stored, err)
	}
}

func TestDisableUpstreamKeyForIdentityRejectsRotatedKeyGeneration(t *testing.T) {
	store, channel, generation, key := upstreamKeyInvalidationFixture(t)
	fence := NewGatewayUpstreamKeyInvalidationFence(generation, channel.UpstreamAccountIdentityRevision, key.ID, key.KeyGeneration)
	if err := store.db.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).
		Updates(map[string]any{"api_key_cipher": "rotated-cipher", "api_key_prefix": "rotated", "enabled": true}).Error; err != nil {
		t.Fatal(err)
	}

	if err := store.DisableUpstreamKeyForIdentity(fence, "stale auth failure"); !errors.Is(err, ErrGatewayUpstreamKeyInvalidationFenceChanged) {
		t.Fatalf("rotated key invalidation error = %v", err)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil || !stored.Enabled || stored.APIKeyCipher != "rotated-cipher" || stored.APIKeyPrefix != "rotated" || stored.KeyGeneration == key.KeyGeneration {
		t.Fatalf("stale key invalidation changed rotated key: key=%+v err=%v", stored, err)
	}
}

func TestDisableUpstreamKeyForIdentityDisablesExactLease(t *testing.T) {
	store, channel, generation, key := upstreamKeyInvalidationFixture(t)
	fence := NewGatewayUpstreamKeyInvalidationFence(generation, channel.UpstreamAccountIdentityRevision, key.ID, key.KeyGeneration)

	if err := store.DisableUpstreamKeyForIdentity(fence, "observed 401"); err != nil {
		t.Fatal(err)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil || stored.Enabled || stored.APIKeyCipher != "" || stored.APIKeyPrefix != "" || stored.LastError != "observed 401" ||
		stored.AuthenticationInvalidatedChannelIdentityRevision != channel.UpstreamAccountIdentityRevision || stored.KeyGeneration == key.KeyGeneration {
		t.Fatalf("exact invalidation result: key=%+v err=%v", stored, err)
	}
}
