package storage

import (
	"errors"
	"testing"
	"time"
)

func TestLegacyModelSyncRejectsRetiredIdentityBeforeKeyMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*Gateway, uint, time.Time) error
	}{
		{
			name: "families",
			run: func(store *Gateway, channelID uint, at time.Time) error {
				return store.UpdateUpstreamKeyModelFamilies(channelID, "vip", "claude", "claude-sonnet", at, "")
			},
		},
		{
			name: "set error",
			run: func(store *Gateway, channelID uint, at time.Time) error {
				return store.SetUpstreamKeyModelsError(channelID, "vip", at, "late sync error")
			},
		},
		{
			name: "record error",
			run: func(store *Gateway, channelID uint, at time.Time) error {
				return store.RecordUpstreamKeyModelsError(channelID, "vip", at, "late sync error")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			channels := NewChannels(db)
			rates := NewRates(db)
			store := NewGateway(db)
			channel := &Channel{Name: "model sync lock " + test.name, Type: ChannelTypeNewAPI, SiteURL: "https://model-sync-lock.example", MonitorEnabled: true}
			if err := channels.Create(channel); err != nil {
				t.Fatal(err)
			}
			base := time.Now().UTC().Truncate(time.Microsecond)
			if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "VIP", Ratio: 1, LastSeenAt: base}); err != nil {
				t.Fatal(err)
			}
			if err := store.RecordUpstreamGroupChange(channel.ID, "VIP", "added", 1, "managed", base); err != nil {
				t.Fatal(err)
			}
			syncedAt := base.Add(time.Second)
			if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
				ChannelID: channel.ID, GroupName: "VIP", Enabled: true, APIKeyCipher: "cipher",
				ModelFamilies: "codex", ModelIDs: "gpt-test", ModelsSyncedAt: &syncedAt,
			}); err != nil {
				t.Fatal(err)
			}
			if err := store.RecordUpstreamGroupChange(channel.ID, "VIP", "removed", 1, "managed", base.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}

			err := test.run(store, channel.ID, base.Add(3*time.Second))
			if !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
				t.Fatalf("legacy model sync error = %v", err)
			}
			key, err := store.FindUpstreamKeyAny(channel.ID, "vip")
			if err != nil {
				t.Fatal(err)
			}
			if key.ModelFamilies != "codex" || key.ModelIDs != "gpt-test" || key.ModelsError != "" || key.ModelsSyncedAt == nil || !key.ModelsSyncedAt.Equal(syncedAt) {
				t.Fatalf("retired generation key was mutated: %+v", key)
			}
			var identity GatewayUpstreamGroup
			if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "vip").First(&identity).Error; err != nil {
				t.Fatal(err)
			}
			if identity.Present || identity.PresenceGeneration != 1 {
				t.Fatalf("retired identity was recreated: %+v", identity)
			}
		})
	}
}

func TestLegacyModelSyncRejectsGroupWithoutCurrentRate(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "model sync missing rate", Type: ChannelTypeNewAPI, SiteURL: "https://model-sync-missing-rate.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "VIP", Ratio: 1, LastSeenAt: base}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "VIP", "added", 1, "managed", base); err != nil {
		t.Fatal(err)
	}
	syncedAt := base.Add(time.Second)
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "VIP", Enabled: true, APIKeyCipher: "cipher",
		ModelFamilies: "codex", ModelIDs: "gpt-test", ModelsSyncedAt: &syncedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Where("channel_id = ? AND lower(btrim(model_name)) = ?", channel.ID, "vip").Delete(&RateSnapshot{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "vip", "claude", "claude-sonnet", base.Add(2*time.Second), ""); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("missing-rate model sync error = %v", err)
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "vip")
	if err != nil {
		t.Fatal(err)
	}
	if key.ModelFamilies != "codex" || key.ModelIDs != "gpt-test" || key.ModelsSyncedAt == nil || !key.ModelsSyncedAt.Equal(syncedAt) {
		t.Fatalf("missing-rate model sync mutated key: %+v", key)
	}
}
