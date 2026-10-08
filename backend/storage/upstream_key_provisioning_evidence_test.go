package storage

import (
	"testing"
	"time"
)

func TestUpstreamKeyErrorsAloneDoNotBlockProvisioningClaim(t *testing.T) {
	if upstreamKeyHasProvisioningEvidence(&GatewayUpstreamKey{
		LastError:   "temporary login failure",
		ModelsError: "model sync failed",
	}) {
		t.Fatal("error-only placeholder was treated as provisioning evidence")
	}
}

func TestUpstreamKeyDurableIdentityStillBlocksDuplicateProvisioning(t *testing.T) {
	started := time.Now()
	tests := map[string]GatewayUpstreamKey{
		"provisioning started at": {ProvisioningStartedAt: &started},
		"remote id":               {UpstreamKeyID: 1},
		"remote name":             {UpstreamKeyName: "BOG-p-0123456789abcdef"},
		"encrypted key":           {APIKeyCipher: "ciphertext"},
		"key prefix":              {APIKeyPrefix: "sk-prefix"},
	}
	for name, key := range tests {
		t.Run(name, func(t *testing.T) {
			if !upstreamKeyHasProvisioningEvidence(&key) {
				t.Fatal("durable remote-key evidence was ignored")
			}
		})
	}
}
