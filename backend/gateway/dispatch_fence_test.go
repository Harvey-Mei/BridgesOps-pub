package gateway

import (
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestValidateCurrentUpstreamDispatchFenceRejectsDirectPolicyAndLeaseDrift(t *testing.T) {
	channel := storage.Channel{
		ID:                              7,
		CredentialMode:                  storage.CredentialModeDirect,
		GatewayApp:                      storage.GatewayAppCodex,
		DirectModels:                    `["vendor-alias"]`,
		UpstreamAccountIdentityRevision: 3,
		AdminRevision:                   5,
	}
	key := storage.GatewayUpstreamKey{
		ID: 11, ChannelID: 7, GroupName: storage.DirectChannelGroupName,
		KeyGeneration: 9, Enabled: true, APIKeyCipher: "ciphertext",
	}
	fence := UpstreamDispatchFence{
		ChannelID: 7, GroupName: storage.DirectChannelGroupName,
		CredentialMode: storage.CredentialModeDirect, ChannelIdentityRevision: 3,
		ChannelAdminRevision: 5, LocalKeyID: 11, KeyGeneration: 9,
		App: storage.GatewayAppCodex, Model: "vendor-alias",
	}
	if err := validateCurrentUpstreamDispatchFence(channel, key, fence); err != nil {
		t.Fatalf("current direct fence rejected: %v", err)
	}

	changedPolicy := channel
	changedPolicy.AdminRevision++
	if err := validateCurrentUpstreamDispatchFence(changedPolicy, key, fence); err == nil {
		t.Fatal("direct admin revision drift was accepted")
	}
	changedModel := channel
	changedModel.DirectModels = `["other-model"]`
	if err := validateCurrentUpstreamDispatchFence(changedModel, key, fence); err == nil {
		t.Fatal("direct exact-model drift was accepted")
	}
	changedMode := channel
	changedMode.CredentialMode = storage.CredentialModePassword
	if err := validateCurrentUpstreamDispatchFence(changedMode, key, fence); err == nil {
		t.Fatal("credential-mode drift was accepted")
	}
	changedKey := key
	changedKey.KeyGeneration++
	if err := validateCurrentUpstreamDispatchFence(channel, changedKey, fence); err == nil {
		t.Fatal("key-generation drift was accepted")
	}
	disabledKey := key
	disabledKey.Enabled = false
	if err := validateCurrentUpstreamDispatchFence(channel, disabledKey, fence); err == nil {
		t.Fatal("disabled key was accepted")
	}
}

func TestValidateCurrentUpstreamDispatchFenceAllowsManagedAdminRevisionDrift(t *testing.T) {
	channel := storage.Channel{
		ID: 8, CredentialMode: storage.CredentialModePassword,
		UpstreamAccountIdentityRevision: 4, AdminRevision: 99,
	}
	key := storage.GatewayUpstreamKey{
		ID: 12, ChannelID: 8, GroupName: "primary",
		KeyGeneration: 2, Enabled: true, APIKeyCipher: "ciphertext",
	}
	fence := UpstreamDispatchFence{
		ChannelID: 8, GroupName: "primary", CredentialMode: storage.CredentialModePassword,
		ChannelIdentityRevision: 4, ChannelAdminRevision: 1,
		LocalKeyID: 12, KeyGeneration: 2, App: storage.GatewayAppCodex,
	}
	if err := validateCurrentUpstreamDispatchFence(channel, key, fence); err != nil {
		t.Fatalf("unrelated managed admin revision rejected: %v", err)
	}
}
