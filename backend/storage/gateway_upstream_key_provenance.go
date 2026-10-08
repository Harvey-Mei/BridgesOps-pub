package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	GatewayUpstreamKeyProvenanceCreated           = "created"
	GatewayUpstreamKeyProvenanceProvisioningClaim = "provisioning_claim"
	GatewayUpstreamKeyProvenanceAdopted           = "adopted"
	GatewayUpstreamKeyProvenanceUnknown           = "unknown"
)

var gatewayProvisioningKeyNamePattern = regexp.MustCompile(`^BOG-p-[0-9a-f]{16}$`)

type managedUpstreamKeyOwnershipClaim struct {
	Version               int    `json:"version"`
	LocalKeyID            uint   `json:"local_key_id"`
	ChannelID             uint   `json:"channel_id"`
	GroupName             string `json:"group_name"`
	UpstreamKeyID         int64  `json:"upstream_key_id"`
	UpstreamKeyName       string `json:"upstream_key_name"`
	OwnershipProvenance   string `json:"ownership_provenance"`
	Provisioning          bool   `json:"provisioning"`
	ProvisioningStartedAt string `json:"provisioning_started_at"`
	APIKeyCipherHash      string `json:"api_key_cipher_hash"`
	APIKeyPrefix          string `json:"api_key_prefix"`
}

// ManagedUpstreamKeyOwnershipClaimDigest binds a destructive intent to the
// exact local ownership facts that were verified. It contains only a hash of
// ciphertext, never the ciphertext or plaintext secret itself.
func ManagedUpstreamKeyOwnershipClaimDigest(key GatewayUpstreamKey) (string, error) {
	provenance, err := normalizeGatewayUpstreamKeyOwnershipProvenance(key.OwnershipProvenance)
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(key.UpstreamKeyName)
	switch provenance {
	case GatewayUpstreamKeyProvenanceCreated:
		if key.ID == 0 || key.ChannelID == 0 || key.UpstreamKeyID <= 0 || name == "" || strings.TrimSpace(key.APIKeyCipher) == "" {
			return "", errors.New("created upstream key ownership is incomplete")
		}
	case GatewayUpstreamKeyProvenanceProvisioningClaim:
		if key.ID == 0 || key.ChannelID == 0 || !key.Provisioning || key.ProvisioningStartedAt == nil || !gatewayProvisioningKeyNamePattern.MatchString(name) {
			return "", errors.New("provisioning upstream key ownership is incomplete")
		}
	default:
		return "", errors.New("upstream key ownership provenance is not deletable")
	}
	return managedUpstreamKeyOwnershipClaimDigest(key, provenance)
}

// ChannelDeletionManagedOwnershipClaimDigest also accepts the random BOG-p
// namespace as ownership evidence. Those names are generated from 128 bits of
// randomness and reserved exclusively for BridgeOps-managed keys, so mutable
// provider IDs and incomplete historical provenance do not require an
// operator decision during whole-channel cleanup.
func ChannelDeletionManagedOwnershipClaimDigest(key GatewayUpstreamKey) (string, error) {
	provenance, err := normalizeGatewayUpstreamKeyOwnershipProvenance(key.OwnershipProvenance)
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(key.UpstreamKeyName)
	if !gatewayProvisioningKeyNamePattern.MatchString(name) {
		return ManagedUpstreamKeyOwnershipClaimDigest(key)
	}
	if key.ID == 0 || key.ChannelID == 0 {
		return "", errors.New("reserved upstream key ownership is incomplete")
	}
	return managedUpstreamKeyOwnershipClaimDigest(key, provenance)
}

func managedUpstreamKeyOwnershipClaimDigest(key GatewayUpstreamKey, provenance string) (string, error) {
	name := strings.TrimSpace(key.UpstreamKeyName)
	startedAt := ""
	if key.ProvisioningStartedAt != nil {
		startedAt = key.ProvisioningStartedAt.UTC().Format(time.RFC3339Nano)
	}
	cipherHash := sha256.Sum256([]byte(key.APIKeyCipher))
	claim := managedUpstreamKeyOwnershipClaim{
		Version: 1, LocalKeyID: key.ID, ChannelID: key.ChannelID,
		GroupName: strings.TrimSpace(key.GroupName), UpstreamKeyID: key.UpstreamKeyID,
		UpstreamKeyName: name, OwnershipProvenance: provenance,
		Provisioning: key.Provisioning, ProvisioningStartedAt: startedAt,
		APIKeyCipherHash: hex.EncodeToString(cipherHash[:]), APIKeyPrefix: strings.TrimSpace(key.APIKeyPrefix),
	}
	raw, err := json.Marshal(claim)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func normalizeGatewayUpstreamKeyOwnershipProvenance(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return GatewayUpstreamKeyProvenanceUnknown, nil
	}
	switch value {
	case GatewayUpstreamKeyProvenanceCreated,
		GatewayUpstreamKeyProvenanceProvisioningClaim,
		GatewayUpstreamKeyProvenanceAdopted,
		GatewayUpstreamKeyProvenanceUnknown:
		return value, nil
	default:
		return "", fmt.Errorf("invalid upstream key ownership provenance %q", value)
	}
}
