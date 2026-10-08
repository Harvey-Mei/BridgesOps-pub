package gateway

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestTouchUpstreamGroupClearsFailureWrittenByAnotherServiceInstance(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	channel := &storage.Channel{
		Name: "cross instance touch", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://cross-instance-touch.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	storeA := storage.NewGateway(db)
	storeB := storage.NewGateway(db)
	if err := storeA.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", Enabled: true, APIKeyCipher: "cipher",
	}); err != nil {
		t.Fatal(err)
	}
	serviceA := NewService(channels, storage.NewRates(db), storeA, nil, nil, nil)
	serviceB := NewService(channels, storage.NewRates(db), storeB, nil, nil, nil)
	base := time.Now().UTC().Truncate(time.Microsecond)

	serviceA.TouchUpstreamGroupAt(channel.ID, "primary", base)
	serviceB.MarkUpstreamGroupRoutingFailure(channel.ID, "primary", RoutingFailureTransport, 0, "failure from service B")
	successAt := time.Now().UTC().Add(time.Second).Truncate(time.Microsecond)
	serviceA.TouchUpstreamGroupAt(channel.ID, "primary", successAt)

	key, err := storeA.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if key.LastUsedAt == nil || !key.LastUsedAt.Equal(successAt) || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("cross-instance success did not clear routing health: %+v", key)
	}
	var facts int64
	if err := db.Model(&storage.GatewayRoutingFailure{}).Where("channel_id = ? AND group_name = ?", channel.ID, "primary").Count(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if facts != 0 {
		t.Fatalf("cross-instance success left failure facts = %d", facts)
	}
}
