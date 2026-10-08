package gateway

import (
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestDirectProjectedModelsUsesSelectedCredentialInsteadOfChannelAggregate(t *testing.T) {
	credentialID := uint(17)
	channel := storage.Channel{
		CredentialMode: storage.CredentialModeDirect,
		DirectModels:   storage.EncodeDirectModels([]string{"other-group-model"}),
	}
	key := storage.GatewayUpstreamKey{
		DirectCredentialID:       &credentialID,
		DirectCredentialRevision: 3,
		DirectBaseURL:            "https://selected.invalid",
		ModelIDs:                 "selected-model",
	}

	models, ok := directProjectedModels(channel, key)
	if !ok || len(models) != 1 || models[0] != "selected-model" {
		t.Fatalf("direct projected models = %#v, ok=%t", models, ok)
	}

	orphaned := key
	orphaned.DirectCredentialID = nil // ON DELETE SET NULL retains revision/base URL evidence.
	if models, ok := directProjectedModels(channel, orphaned); ok || len(models) != 0 {
		t.Fatalf("orphaned projection used aggregate models = %#v, ok=%t", models, ok)
	}
}

func TestDirectProjectedCredentialMatchFencesEverySourceField(t *testing.T) {
	credentialID := uint(21)
	credential := storage.ChannelDirectCredential{
		ID: 21, ChannelID: 5, GroupName: "priority", BaseURL: "https://direct.invalid",
		APIKeyCipher: "cipher", Models: storage.EncodeDirectModels([]string{"model-a", "model-b"}),
		Enabled: true, Revision: 4,
	}
	key := storage.GatewayUpstreamKey{
		ChannelID: 5, GroupName: "PRIORITY", APIKeyCipher: "cipher", ModelIDs: "model-a,model-b",
		DirectCredentialID: &credentialID, DirectCredentialRevision: 4, DirectBaseURL: "https://direct.invalid/",
	}
	if !directProjectedCredentialMatches(credential, key) {
		t.Fatal("current direct source did not match its runtime projection")
	}

	mutations := []struct {
		name   string
		mutate func(*storage.ChannelDirectCredential)
	}{
		{name: "cleared secret", mutate: func(item *storage.ChannelDirectCredential) { item.APIKeyCipher = "" }},
		{name: "rotated secret", mutate: func(item *storage.ChannelDirectCredential) { item.APIKeyCipher = "replacement" }},
		{name: "disabled", mutate: func(item *storage.ChannelDirectCredential) { item.Enabled = false }},
		{name: "revision", mutate: func(item *storage.ChannelDirectCredential) { item.Revision++ }},
		{name: "group", mutate: func(item *storage.ChannelDirectCredential) { item.GroupName = "fallback" }},
		{name: "base URL", mutate: func(item *storage.ChannelDirectCredential) { item.BaseURL = "https://replacement.invalid" }},
		{name: "models", mutate: func(item *storage.ChannelDirectCredential) {
			item.Models = storage.EncodeDirectModels([]string{"model-b"})
		}},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			changed := credential
			test.mutate(&changed)
			if directProjectedCredentialMatches(changed, key) {
				t.Fatal("stale runtime projection matched changed source")
			}
		})
	}
}
