package channel

import (
	"strings"
	"testing"

	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestPrepareDirectCredentialsBlankKeyRetainsExistingSecret(t *testing.T) {
	cipher, err := appcrypto.NewCipher("12345678901234567890123456789012")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	ciphertext, err := cipher.Encrypt("sk-existing-secret")
	if err != nil {
		t.Fatalf("encrypt existing key: %v", err)
	}
	svc := &Service{Cipher: cipher}
	existing := storage.ChannelDirectCredential{
		ID: 17, ChannelID: 4, GroupName: "Primary", BaseURL: "https://old.example",
		APIKeyCipher: ciphertext, APIKeyPrefix: "sk-existing-", Models: storage.EncodeDirectModels([]string{"old-model"}),
		Ratio: 1, CompletionRatio: 1, Enabled: true, Revision: 3,
	}
	enabled := true
	prepared, _, err := svc.prepareDirectCredentials([]DirectCredentialInput{
		{
			ID: existing.ID, GroupName: existing.GroupName, BaseURL: "https://new.example/v1",
			APIKey: "", Models: storage.EncodeDirectModels([]string{"new-model"}),
			Ratio: 1.25, CompletionRatio: 1.5, Enabled: &enabled,
		},
	}, []storage.ChannelDirectCredential{existing})
	if err != nil {
		t.Fatalf("prepare direct credentials: %v", err)
	}
	if len(prepared) != 1 || prepared[0].ID != existing.ID {
		t.Fatalf("prepared credentials = %+v", prepared)
	}
	if prepared[0].APIKeyCipher != ciphertext || prepared[0].APIKeyPrefix != existing.APIKeyPrefix {
		t.Fatal("blank API key did not retain the existing encrypted key metadata")
	}
	plain, err := cipher.Decrypt(prepared[0].APIKeyCipher)
	if err != nil || plain != "sk-existing-secret" {
		t.Fatalf("retained API key = %q, err = %v", plain, err)
	}
	if prepared[0].BaseURL != "https://new.example" || prepared[0].Ratio != 1.25 || prepared[0].CompletionRatio != 1.5 {
		t.Fatalf("updated non-secret fields = %+v", prepared[0])
	}
}

func TestPrepareDirectCredentialsBlankKeyCannotCreateGroup(t *testing.T) {
	cipher, err := appcrypto.NewCipher("12345678901234567890123456789012")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	svc := &Service{Cipher: cipher}
	_, _, err = svc.prepareDirectCredentials([]DirectCredentialInput{
		{
			GroupName: "Primary", BaseURL: "https://new.example", Models: storage.EncodeDirectModels([]string{"model"}),
			Ratio: 1, CompletionRatio: 1,
		},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "API Key") {
		t.Fatalf("new blank-key error = %v", err)
	}
}
