package monitor

import (
	"context"
	"errors"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestDirectChannelSkipsUpstreamManagementCollectors(t *testing.T) {
	service := &Service{}
	channel := &storage.Channel{
		CredentialMode:      storage.CredentialModeDirect,
		MonitorEnabled:      true,
		SubscriptionEnabled: true,
		Type:                storage.ChannelTypeSub2API,
	}
	ctx := context.Background()

	if err := service.keepAliveSession(ctx, channel); err != nil {
		t.Fatalf("keepAliveSession: %v", err)
	}
	if err := service.refreshBalance(ctx, channel); err != nil {
		t.Fatalf("refreshBalance: %v", err)
	}
	if err := service.CheckSubscriptionUsageAlerts(ctx, channel); err != nil {
		t.Fatalf("CheckSubscriptionUsageAlerts: %v", err)
	}
	if _, _, _, err := service.prepare(ctx, channel); !errors.Is(err, connector.ErrUnsupported) {
		t.Fatalf("prepare error = %v, want connector.ErrUnsupported", err)
	}
	if err := service.syncAnnouncements(ctx, channel, nil, nil, nil); err != nil {
		t.Fatalf("syncAnnouncements: %v", err)
	}
}

func TestKeepAliveSessionsSkipsDirectChannelWithoutClearingError(t *testing.T) {
	channel := &storage.Channel{
		Name:           "direct-keepalive",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://direct.example.test/v1",
		CredentialMode: storage.CredentialModeDirect,
		DirectModels:   `["gpt-test"]`,
		LastError:      "直连 API Key 已清空，请重新编辑渠道填写凭据",
	}
	h := newMonitorTestHarness(t, channel)

	result := h.service.KeepAliveSessions(context.Background())
	if result.Total != 1 || result.Success != 0 || result.Failed != 0 {
		t.Fatalf("keepalive result = %+v", result)
	}
	stored, err := h.channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastError != channel.LastError {
		t.Fatalf("last error = %q, want %q", stored.LastError, channel.LastError)
	}
	logs, err := h.monitorLogs.List(channel.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("direct keepalive logs = %+v, want none", logs)
	}
}
