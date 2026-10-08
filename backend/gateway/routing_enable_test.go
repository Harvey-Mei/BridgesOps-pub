package gateway

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestConfirmedLatencyProbeRecoveryServiceDoesNotTouchInferenceWatermark(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "probe-recovery-service", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://probe-recovery-service.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "ready", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 0, "", base); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	failureAt := base.Add(time.Second)
	if err := store.RecordUpstreamKeyRoutingFailure(channel.ID, "primary", RoutingFailureTransport, 0, "old adaptive failure", failureAt, generation); err != nil {
		t.Fatal(err)
	}

	service := NewService(channels, rates, store, nil, nil, nil)
	recoveryAt := base.Add(2 * time.Second)
	service.RecoverUpstreamGroupRoutingFromConfirmedLatencyProbeAt(channel.ID, "primary", storage.GatewayAppCodex, recoveryAt, generation)

	key, err := store.FindUpstreamKey(channel.ID, "primary")
	if err != nil || key.LastUsedAt != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("probe recovery service changed real inference watermark or retained health: key=%+v err=%v", key, err)
	}
}
