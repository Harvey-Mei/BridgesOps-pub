package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestChannelDirectCredentialOutputDoesNotEchoSecrets(t *testing.T) {
	const secret = "sk-secret-that-must-never-be-returned"
	output := channelOutput{
		Channel: storage.Channel{ID: 9, Name: "direct", PasswordCipher: secret},
		DirectCredentials: []directCredentialOutput{
			{
				ID: 12, GroupName: "primary", BaseURL: "https://direct.example",
				APIKeyPrefix: "sk-public-pr", KeyConfigured: true,
				Models: storage.EncodeDirectModels([]string{"model"}), Ratio: 1, CompletionRatio: 1, Enabled: true, Revision: 2,
			},
		},
	}
	raw, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal channel output: %v", err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatalf("channel output leaked secret: %s", raw)
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode channel output: %v", err)
	}
	for _, forbidden := range []string{"password", "password_cipher", "direct_api_key"} {
		if _, ok := decoded[forbidden]; ok {
			t.Fatalf("channel output exposed top-level field %q", forbidden)
		}
	}
	items, ok := decoded["direct_credentials"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("direct credentials JSON = %#v", decoded["direct_credentials"])
	}
	credential, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("direct credential JSON = %#v", items[0])
	}
	for _, forbidden := range []string{"api_key", "api_key_cipher", "password_cipher"} {
		if _, ok := credential[forbidden]; ok {
			t.Fatalf("direct credential output exposed field %q", forbidden)
		}
	}
	if credential["api_key_prefix"] != "sk-public-pr" || credential["key_configured"] != true {
		t.Fatalf("safe key metadata = %#v", credential)
	}
}
