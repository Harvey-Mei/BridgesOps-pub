package api

import (
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestDirectGatewayChannelAllowsModelUsesExactScopedTarget(t *testing.T) {
	channel := storage.Channel{
		CredentialMode: storage.CredentialModeDirect,
		GatewayApp:     storage.GatewayAppClaude,
		DirectModels:   `["vendor-alias"]`,
	}
	if !directGatewayChannelAllowsModel(channel, storage.DirectChannelGroupName, storage.GatewayAppClaude, "vendor-alias") {
		t.Fatal("configured direct model was rejected")
	}
	if directGatewayChannelAllowsModel(channel, storage.DirectChannelGroupName, storage.GatewayAppClaude, "Vendor-Alias") {
		t.Fatal("direct model admission must be case-sensitive")
	}
	if directGatewayChannelAllowsModel(channel, "other-group", storage.GatewayAppClaude, "vendor-alias") {
		t.Fatal("direct model admission escaped the direct group")
	}
	if directGatewayChannelAllowsModel(channel, storage.DirectChannelGroupName, storage.GatewayAppCodex, "vendor-alias") {
		t.Fatal("direct model admission escaped the configured app")
	}
}
