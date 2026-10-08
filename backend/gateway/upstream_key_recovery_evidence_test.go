package gateway

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestUpstreamKeyErrorsAloneAreNotRecoveryEvidence(t *testing.T) {
	key := &storage.GatewayUpstreamKey{
		LastError:   "temporary login failure",
		ModelsError: "model sync failed",
	}
	if hasUpstreamKeyRecoveryEvidence(key) {
		t.Fatal("error-only placeholder was treated as evidence that a remote key exists")
	}
	if !hasUpstreamKeyErrorState(key) {
		t.Fatal("error-only placeholder lost its retry state")
	}
}

func TestUpstreamKeyIdentityAndProvisioningRemainRecoveryEvidence(t *testing.T) {
	started := time.Now()
	tests := map[string]storage.GatewayUpstreamKey{
		"provisioning":            {Provisioning: true},
		"provisioning started at": {ProvisioningStartedAt: &started},
		"remote id":               {UpstreamKeyID: 1},
		"remote name":             {UpstreamKeyName: "BOG-p-0123456789abcdef"},
		"encrypted key":           {APIKeyCipher: "ciphertext"},
		"key prefix":              {APIKeyPrefix: "sk-prefix"},
	}
	for name, key := range tests {
		t.Run(name, func(t *testing.T) {
			if !hasUpstreamKeyRecoveryEvidence(&key) {
				t.Fatal("real remote-key evidence was ignored")
			}
		})
	}
}
