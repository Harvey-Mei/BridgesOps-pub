package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

const managedKeyRemovalReviewMessage = "managed upstream key removal requires manual review"
const managedKeyRemovalCleanupTimeout = 3 * time.Second

type managedKeyRemovalEvidence struct {
	Version                 int                             `json:"version"`
	ChannelID               uint                            `json:"channel_id"`
	ChannelIdentityRevision uint64                          `json:"channel_identity_revision"`
	UpstreamGroupID         uint                            `json:"upstream_group_id"`
	Generation              uint64                          `json:"presence_generation"`
	GroupName               string                          `json:"group_name"`
	Remote                  managedKeyRemovalRemoteSnapshot `json:"remote"`
	OwnershipMode           string                          `json:"ownership_mode"`
	OwnershipDigest         string                          `json:"ownership_digest"`
	LocalClaimDigest        string                          `json:"local_claim_digest"`
	RemoteSecretHash        string                          `json:"remote_secret_hash"`
}

type managedKeyRemovalRemoteSnapshot struct {
	ID                 int64                          `json:"id"`
	Name               string                         `json:"name"`
	Group              string                         `json:"group"`
	GroupName          string                         `json:"group_name"`
	GroupID            *int64                         `json:"group_id"`
	Source             connector.APIKeySource         `json:"source"`
	Quota              float64                        `json:"quota"`
	UnlimitedQuota     bool                           `json:"unlimited_quota"`
	ExpiredTime        int64                          `json:"expired_time"`
	ExpiresAt          *time.Time                     `json:"expires_at"`
	AllowIPs           string                         `json:"allow_ips"`
	IPWhitelist        []string                       `json:"ip_whitelist"`
	IPBlacklist        []string                       `json:"ip_blacklist"`
	ModelLimitsEnabled bool                           `json:"model_limits_enabled"`
	ModelLimits        string                         `json:"model_limits"`
	CrossGroupRetry    bool                           `json:"cross_group_retry"`
	RateLimit5h        float64                        `json:"rate_limit_5h"`
	RateLimit1d        float64                        `json:"rate_limit_1d"`
	RateLimit7d        float64                        `json:"rate_limit_7d"`
	ObservedFields     connector.APIKeyObservedFields `json:"observed_fields"`
	NullFields         connector.APIKeyObservedFields `json:"null_fields"`
}

type managedKeyRebuildTarget struct {
	Remote                  connector.APIKey
	Local                   storage.GatewayUpstreamKey
	Generation              storage.UpstreamGroupGeneration
	ChannelIdentityRevision uint64
	Absent                  bool
	EvidenceDigest          string
	OwnershipClaim          string
}

func (s *Service) removeManagedKeyForAbsentGeneration(ctx context.Context, generation storage.UpstreamGroupGeneration, groupName string, local *storage.GatewayUpstreamKey, initial connector.APIKey, expectedIdentityRevision uint64, preserveCandidates bool) (bool, error) {
	store := s.Store.WithContext(ctx)
	if expectedIdentityRevision == 0 {
		return false, errors.New("channel account identity revision is required for managed key removal")
	}
	digest, reviewReason, err := s.managedKeyRemovalEvidenceDigest(ctx, generation, groupName, local, initial, expectedIdentityRevision)
	if err != nil {
		return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, 0, groupName, initial.ID, initial.Name, 0, preserveCandidates, time.Now())
	}
	if reviewReason != "" {
		return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, 0, groupName, initial.ID, initial.Name, 0, preserveCandidates, time.Now())
	}
	ownershipClaim, err := managedUpstreamKeyRemovalOwnershipClaim(*local)
	if err != nil {
		return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, 0, groupName, initial.ID, initial.Name, 0, preserveCandidates, time.Now())
	}
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
		generation, expectedIdentityRevision, groupName, initial.ID, initial.Name, digest, ownershipClaim, time.Now(), managedUpstreamKeyRemovalStaleAfter,
	)
	if err != nil {
		return false, err
	}
	claimEpoch := uint64(0)
	if intent != nil && intent.RemovalClaimEpoch != nil {
		claimEpoch = *intent.RemovalClaimEpoch
	}
	if intent != nil && claimEpoch == 0 {
		return false, storage.ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if !claimed {
		if intent == nil {
			return false, storage.ErrManagedUpstreamKeyRemovalIntentChanged
		}
		switch intent.Status {
		case "acknowledged":
			markUpstreamInventoryPassKeyDeleted(ctx, generation.ChannelID, initial.ID)
			return true, nil
		case "processing":
			return false, store.QuarantineAbsentGenerationManagedUpstreamKeyProcessing(generation, expectedIdentityRevision, claimEpoch, intent.ID, time.Now())
		default:
			return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
		}
	}

	current, nameReused, err := s.findUpstreamKeyByIDWithExpectedName(ctx, generation.ChannelID, expectedIdentityRevision, initial.ID, initial.Name)
	if nameReused {
		return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if err := store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
			generation, expectedIdentityRevision, claimEpoch, intent.ID, local.ID, initial.ID, initial.Name, digest, ownershipClaim, time.Now(), nil,
		); err != nil {
			if errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) || errors.Is(err, storage.ErrManagedUpstreamKeyRemovalIdentityConflict) || errors.Is(err, storage.ErrManagedUpstreamKeyRemovalIntentChanged) || errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
				return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
			}
			return false, err
		}
		markUpstreamInventoryPassKeyDeleted(ctx, generation.ChannelID, initial.ID)
		return true, nil
	}
	if err != nil {
		return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
	}
	currentDigest, reviewReason, err := s.managedKeyRemovalEvidenceDigest(ctx, generation, groupName, local, *current, expectedIdentityRevision)
	if err != nil {
		return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
	}
	if reviewReason != "" || currentDigest != digest {
		return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
	}
	remoteDelete, err := s.Keys.PrepareManagedAPIKeyDeletion(ctx, generation.ChannelID, *current, expectedIdentityRevision)
	if err != nil {
		return false, store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
	}
	remoteDeleteCompleted := false
	trackedRemoteDelete := func() error {
		deleteErr := remoteDelete()
		remoteDeleteCompleted = deleteErr == nil
		return deleteErr
	}
	finalizeStore := s.Store.WithContext(context.WithoutCancel(ctx))
	err = finalizeStore.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		generation, expectedIdentityRevision, claimEpoch, intent.ID, local.ID, current.ID, current.Name, currentDigest, ownershipClaim, time.Now(),
		trackedRemoteDelete,
	)
	if err != nil {
		if remoteDeleteCompleted {
			cleanupErr := s.withManagedKeyRemovalCleanupStore(func(cleanupStore *storage.Gateway) error {
				return cleanupStore.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
					generation, expectedIdentityRevision, claimEpoch, intent.ID, local.ID, current.ID, current.Name, currentDigest, ownershipClaim, time.Now(), nil,
				)
			})
			if cleanupErr == nil {
				markUpstreamInventoryPassKeyDeleted(ctx, generation.ChannelID, current.ID)
				return true, nil
			}
			err = errors.Join(err, fmt.Errorf("cleanup diagnostic: remote completed; cleanup=%w", cleanupErr))
			err = errors.Join(err, fmt.Errorf("finalize completed managed upstream key removal: %w", cleanupErr))
		}
		var compensationErr error
		if errors.Is(err, connector.ErrManagedAPIKeyDeletionIdentityChanged) || errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) || errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
			compensationErr = s.withManagedKeyRemovalCleanupStore(func(cleanupStore *storage.Gateway) error {
				return cleanupStore.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
			})
			if compensationErr != nil {
				return false, errors.Join(err, compensationErr)
			}
			return false, nil
		}
		if errors.Is(err, storage.ErrManagedUpstreamKeyRemovalIdentityConflict) || errors.Is(err, storage.ErrManagedUpstreamKeyRemovalIntentChanged) {
			compensationErr = s.withManagedKeyRemovalCleanupStore(func(cleanupStore *storage.Gateway) error {
				return cleanupStore.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(generation, expectedIdentityRevision, claimEpoch, groupName, initial.ID, initial.Name, intent.ID, preserveCandidates, time.Now())
			})
			if compensationErr != nil {
				return false, errors.Join(err, compensationErr)
			}
			return false, nil
		}
		if quarantineErr := s.withManagedKeyRemovalCleanupStore(func(cleanupStore *storage.Gateway) error {
			return cleanupStore.QuarantineAbsentGenerationManagedUpstreamKeyProcessing(generation, expectedIdentityRevision, claimEpoch, intent.ID, time.Now())
		}); quarantineErr != nil {
			return false, errors.Join(err, quarantineErr)
		}
		return false, fmt.Errorf("managed absent removal failed (remote completed=%v): %w", remoteDeleteCompleted, err)
	}
	markUpstreamInventoryPassKeyDeleted(ctx, generation.ChannelID, current.ID)
	return true, nil
}

func (s *Service) withManagedKeyRemovalCleanupStore(run func(*storage.Gateway) error) error {
	if s == nil || s.Store == nil || run == nil {
		return errors.New("managed upstream key removal cleanup is unavailable")
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), managedKeyRemovalCleanupTimeout)
	defer cancel()
	return run(s.Store.WithContext(cleanupCtx))
}

// findUpstreamKeyByIDWithExpectedName reads a complete remote inventory and
// reports name reuse separately, so an absent ID cannot be mistaken for
// deletion when its expected name has already been reused by another key.
func (s *Service) findUpstreamKeyByIDWithExpectedName(ctx context.Context, channelID uint, channelIdentityRevision uint64, keyID int64, expectedName string) (*connector.APIKey, bool, error) {
	remote, err := s.listAllUpstreamKeysAllowingDuplicateNames(ctx, channelID, channelIdentityRevision)
	if err != nil {
		return nil, false, err
	}
	expectedName = strings.TrimSpace(expectedName)
	var selected *connector.APIKey
	nameReused := false
	for _, item := range remote {
		if item.ID == keyID {
			item := item
			selected = &item
			continue
		}
		if expectedName != "" && strings.TrimSpace(item.Name) == expectedName {
			nameReused = true
		}
	}
	if selected == nil {
		return nil, nameReused, gorm.ErrRecordNotFound
	}
	return selected, nameReused, nil
}

func (s *Service) managedKeyRemovalEvidenceDigest(ctx context.Context, generation storage.UpstreamGroupGeneration, groupName string, local *storage.GatewayUpstreamKey, remote connector.APIKey, channelIdentityRevision uint64) (string, string, error) {
	groupName = strings.TrimSpace(groupName)
	if !generation.Valid() || groupName == "" || local == nil || channelIdentityRevision == 0 {
		return "", "missing_local_ownership", nil
	}
	if local.ChannelID != generation.ChannelID || strings.TrimSpace(local.GroupName) != groupName {
		return "", "local_group_mismatch", nil
	}
	remoteName := strings.TrimSpace(remote.Name)
	localName := strings.TrimSpace(local.UpstreamKeyName)
	strongPrefixOwnership := isStronglyManagedUpstreamKeyName(remoteName)
	if remote.ID <= 0 || remoteName == "" || localName == "" {
		return "", "remote_identity_mismatch", nil
	}
	if !strongPrefixOwnership && remoteName != localName {
		return "", "remote_identity_mismatch", nil
	}
	if !strongPrefixOwnership && local.UpstreamKeyID > 0 && local.UpstreamKeyID != remote.ID {
		return "", "remote_id_changed", nil
	}
	if !strongPrefixOwnership && remote.Source != connector.APIKeySourceNewAPI && remote.Source != connector.APIKeySourceSub2API {
		return "", "connector_source_unknown", nil
	}
	if !strongPrefixOwnership {
		groupMatches, err := s.strictManagedKeyRemovalGroupMatches(ctx, generation.ChannelID, channelIdentityRevision, remote, groupName)
		if err != nil {
			return "", "", err
		}
		if !groupMatches || !managedUpstreamKeyFingerprintMatchesForName(remote, localName, groupName, nil, false) {
			return "", "creation_fingerprint_incomplete", nil
		}
	}

	ownershipMode := ""
	ownershipDigest := ""
	remoteSecretHash := ""
	switch strings.TrimSpace(local.OwnershipProvenance) {
	case storage.GatewayUpstreamKeyProvenanceCreated:
		if strongPrefixOwnership {
			ownershipDigest = HashKey(remoteName)
			ownershipMode = "managed_prefix"
			break
		}
		if strings.TrimSpace(local.APIKeyCipher) == "" {
			return "", "missing_local_ownership", nil
		}
		if local.UpstreamKeyID <= 0 || local.UpstreamKeyID != remote.ID || s.Cipher == nil {
			return "", "bound_identity_incomplete", nil
		}
		plain, err := s.Cipher.Decrypt(local.APIKeyCipher)
		if err != nil || strings.TrimSpace(plain) == "" {
			return "", "local_secret_unavailable", nil
		}
		plain = strings.TrimSpace(plain)
		if prefix := strings.TrimSpace(local.APIKeyPrefix); prefix != "" && prefix != keyPrefix(plain) {
			return "", "local_secret_prefix_changed", nil
		}
		revealed, err := s.revealAPIKeyForIdentityRevision(ctx, generation.ChannelID, channelIdentityRevision, remote.ID)
		if err != nil {
			return "", "", fmt.Errorf("reveal managed upstream key %d before removal: %w", remote.ID, err)
		}
		remoteSecretHash = HashKey(revealed)
		ownershipDigest = HashKey(plain)
		if strings.TrimSpace(revealed) == "" || remoteSecretHash != ownershipDigest {
			return "", "remote_secret_changed", nil
		}
		ownershipMode = "bound_secret"
	case storage.GatewayUpstreamKeyProvenanceProvisioningClaim:
		if !local.Provisioning || local.ProvisioningStartedAt == nil || !upstreamProvisioningKeyNamePattern.MatchString(localName) {
			return "", "missing_local_ownership", nil
		}
		if strongPrefixOwnership {
			ownershipDigest = HashKey(remoteName)
			ownershipMode = "managed_prefix"
			break
		}
		if !local.Provisioning || local.ProvisioningStartedAt == nil || !upstreamProvisioningKeyNamePattern.MatchString(localName) {
			return "", "missing_local_ownership", nil
		}
		revealed, err := s.revealAPIKeyForIdentityRevision(ctx, generation.ChannelID, channelIdentityRevision, remote.ID)
		if err != nil {
			return "", "", fmt.Errorf("reveal provisioned upstream key %d before removal: %w", remote.ID, err)
		}
		if strings.TrimSpace(revealed) == "" {
			return "", "remote_secret_unavailable", nil
		}
		remoteSecretHash = HashKey(revealed)
		ownershipDigest = provisioningRemovalOwnershipDigest(*local)
		ownershipMode = "provisioning_claim"
	default:
		if !strongPrefixOwnership {
			return "", "ownership_provenance_untrusted", nil
		}
		ownershipDigest = HashKey(remoteName)
		ownershipMode = "managed_prefix"
	}
	if strongPrefixOwnership {
		// The prefix, not the historical secret or ID, is the ownership proof.
		// Do not reject a key merely because an upstream administrator changed
		// those mutable fields.
		ownershipDigest = HashKey(remoteName)
		ownershipMode = "managed_prefix"
		remoteSecretHash = ""
	}
	localClaimDigest, err := managedUpstreamKeyRemovalOwnershipClaim(*local)
	if err != nil {
		return "", "local_ownership_claim_incomplete", nil
	}

	evidence := managedKeyRemovalEvidence{
		Version:                 3,
		ChannelID:               generation.ChannelID,
		ChannelIdentityRevision: channelIdentityRevision,
		UpstreamGroupID:         generation.ID,
		Generation:              generation.PresenceGeneration,
		GroupName:               groupName,
		Remote:                  managedKeyRemovalSnapshot(remote),
		OwnershipMode:           ownershipMode,
		OwnershipDigest:         ownershipDigest,
		LocalClaimDigest:        localClaimDigest,
		RemoteSecretHash:        remoteSecretHash,
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), "", nil
}

func (s *Service) strictManagedKeyRemovalGroupMatches(ctx context.Context, channelID uint, channelIdentityRevision uint64, key connector.APIKey, groupName string) (bool, error) {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" {
		return false, nil
	}
	observedName := false
	for _, remoteGroup := range []string{key.GroupName, key.Group} {
		remoteGroup = strings.TrimSpace(remoteGroup)
		if remoteGroup == "" {
			continue
		}
		observedName = true
		if remoteGroup != groupName {
			return false, nil
		}
	}
	if key.Source == connector.APIKeySourceSub2API && (key.GroupID == nil || *key.GroupID <= 0) {
		return false, nil
	}
	if key.GroupID != nil && *key.GroupID <= 0 {
		return false, nil
	}
	if !observedName {
		return false, nil
	}
	if key.GroupID == nil {
		return true, nil
	}
	groups, err := s.listAPIKeyGroupsForIdentityRevision(ctx, channelID, channelIdentityRevision)
	if err != nil {
		return false, fmt.Errorf("verify managed upstream key group %d before removal: %w", *key.GroupID, err)
	}
	matched := false
	for _, group := range groups {
		if group.ID != nil && *group.ID == *key.GroupID {
			if matched || strings.TrimSpace(group.Name) != groupName {
				return false, nil
			}
			matched = true
		}
	}
	return matched, nil
}

func managedKeyRemovalSnapshot(key connector.APIKey) managedKeyRemovalRemoteSnapshot {
	return managedKeyRemovalRemoteSnapshot{
		ID: key.ID, Name: strings.TrimSpace(key.Name), Group: strings.TrimSpace(key.Group),
		GroupName: strings.TrimSpace(key.GroupName), GroupID: cloneInt64Pointer(key.GroupID), Source: key.Source,
		Quota: key.Quota, UnlimitedQuota: key.UnlimitedQuota, ExpiredTime: key.ExpiredTime,
		ExpiresAt: cloneTimePointer(key.ExpiresAt), AllowIPs: strings.TrimSpace(key.AllowIPs),
		IPWhitelist: append([]string(nil), key.IPWhitelist...), IPBlacklist: append([]string(nil), key.IPBlacklist...),
		ModelLimitsEnabled: key.ModelLimitsEnabled, ModelLimits: strings.TrimSpace(key.ModelLimits),
		CrossGroupRetry: key.CrossGroupRetry, RateLimit5h: key.RateLimit5h,
		RateLimit1d: key.RateLimit1d, RateLimit7d: key.RateLimit7d,
		ObservedFields: key.ObservedFields, NullFields: key.NullFields,
	}
}

func provisioningRemovalOwnershipDigest(local storage.GatewayUpstreamKey) string {
	startedAt := ""
	if local.ProvisioningStartedAt != nil {
		startedAt = local.ProvisioningStartedAt.UTC().Format(time.RFC3339Nano)
	}
	raw := fmt.Sprintf("managed-key-provisioning:v1:%d:%d:%s:%s:%s", local.ChannelID, local.ID, strings.TrimSpace(local.GroupName), strings.TrimSpace(local.UpstreamKeyName), startedAt)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func managedUpstreamKeyRemovalOwnershipClaim(local storage.GatewayUpstreamKey) (string, error) {
	if isStronglyManagedUpstreamKeyName(local.UpstreamKeyName) {
		return storage.ChannelDeletionManagedOwnershipClaimDigest(local)
	}
	return storage.ManagedUpstreamKeyOwnershipClaimDigest(local)
}

func (s *Service) findRemovedGroupRemoteKey(ctx context.Context, channelID uint, channelIdentityRevision uint64, local *storage.GatewayUpstreamKey) (*connector.APIKey, bool, error) {
	if local == nil {
		return nil, false, nil
	}
	if local.UpstreamKeyID > 0 {
		remote, nameReused, err := s.findUpstreamKeyByIDWithExpectedName(ctx, channelID, channelIdentityRevision, local.UpstreamKeyID, local.UpstreamKeyName)
		if nameReused {
			return nil, true, nil
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			expectedName := strings.TrimSpace(local.UpstreamKeyName)
			if expectedName == "" {
				return nil, false, nil
			}
			all, listErr := s.listAllUpstreamKeysAllowingDuplicateNames(ctx, channelID, channelIdentityRevision)
			if listErr != nil {
				return nil, false, listErr
			}
			for _, item := range all {
				if strings.TrimSpace(item.Name) == expectedName {
					return nil, true, nil
				}
			}
			return nil, false, nil
		}
		return remote, false, err
	}
	name := strings.TrimSpace(local.UpstreamKeyName)
	if name == "" {
		return nil, false, nil
	}
	remote, err := s.listAllUpstreamKeysAllowingDuplicateNames(ctx, channelID, channelIdentityRevision)
	if err != nil {
		return nil, false, err
	}
	var selected *connector.APIKey
	for _, item := range remote {
		if strings.TrimSpace(item.Name) != name {
			continue
		}
		if selected != nil && selected.ID != item.ID {
			return nil, true, nil
		}
		item := item
		selected = &item
	}
	return selected, false, nil
}

// findManagedUpstreamKeysForGroup uses the reserved provisioning prefix as the
// ownership proof. A unique local remote-ID binding supplies only the missing
// group name when the provider omits it; explicit provider group fields still
// win, and duplicate names remain valid inventory facts.
func (s *Service) findManagedUpstreamKeysForGroup(ctx context.Context, channelID uint, channelIdentityRevision uint64, groupName string) ([]connector.APIKey, error) {
	groupName = strings.TrimSpace(groupName)
	if groupName == "" {
		return nil, nil
	}
	remote, err := s.listAllUpstreamKeysAllowingDuplicateNames(ctx, channelID, channelIdentityRevision)
	if err != nil {
		return nil, err
	}
	localKeys, err := s.Store.WithContext(ctx).ListUpstreamKeys([]uint{channelID})
	if err != nil {
		return nil, fmt.Errorf("list local upstream keys for group removal: %w", err)
	}
	localByRemoteID := uniqueManagedLocalKeysByRemoteID(localKeys)
	canonical := canonicalGroupName(groupName)
	matched := make([]connector.APIKey, 0, 1)
	for _, item := range remote {
		if item.ID <= 0 || !isStronglyManagedUpstreamKeyName(item.Name) {
			continue
		}
		if canonicalGroupName(managedUpstreamKeyGroupName(item, localByRemoteID)) == canonical {
			matched = append(matched, item)
		}
	}
	return matched, nil
}

func (s *Service) strictManagedKeysForRebuild(ctx context.Context, channelID uint, channelIdentityRevision uint64, localKeys []storage.GatewayUpstreamKey) ([]managedKeyRebuildTarget, error) {
	remote, err := s.listAllUpstreamKeysForIdentityRevision(ctx, channelID, channelIdentityRevision)
	if err != nil {
		return nil, err
	}
	return s.strictManagedKeysForRebuildInventory(ctx, channelID, channelIdentityRevision, localKeys, remote)
}

func (s *Service) strictManagedKeysForRebuildInventory(ctx context.Context, channelID uint, channelIdentityRevision uint64, localKeys []storage.GatewayUpstreamKey, remote []connector.APIKey) ([]managedKeyRebuildTarget, error) {
	if channelIdentityRevision == 0 {
		return nil, errors.New("channel account identity revision is required for managed key rebuild")
	}
	localByID := make(map[int64]storage.GatewayUpstreamKey, len(localKeys))
	localByName := make(map[string][]storage.GatewayUpstreamKey)
	provisioningByName := make(map[string][]storage.GatewayUpstreamKey)
	for _, local := range localKeys {
		if local.UpstreamKeyID > 0 {
			if existing, exists := localByID[local.UpstreamKeyID]; exists && existing.ID != local.ID {
				return nil, fmt.Errorf("managed upstream key %d has ambiguous local ownership", local.UpstreamKeyID)
			}
			localByID[local.UpstreamKeyID] = local
		}
		name := strings.TrimSpace(local.UpstreamKeyName)
		if name != "" {
			localByName[name] = append(localByName[name], local)
		}
		if local.Provisioning && local.ProvisioningStartedAt != nil && upstreamProvisioningKeyNamePattern.MatchString(name) {
			provisioningByName[name] = append(provisioningByName[name], local)
		}
	}
	targets := make([]managedKeyRebuildTarget, 0, len(localKeys))
	seenLocal := make(map[uint]struct{}, len(localKeys))
	for _, item := range remote {
		var local *storage.GatewayUpstreamKey
		if matched, ok := localByID[item.ID]; ok {
			copyLocal := matched
			local = &copyLocal
		} else {
			claims := provisioningByName[strings.TrimSpace(item.Name)]
			if len(claims) > 1 {
				return nil, fmt.Errorf("managed upstream key %d has ambiguous provisioning ownership", item.ID)
			}
			if len(claims) == 1 {
				copyLocal := claims[0]
				local = &copyLocal
			}
		}
		if local == nil {
			nameMatches := localByName[strings.TrimSpace(item.Name)]
			if item.ID <= 0 && len(nameMatches) > 0 {
				return nil, fmt.Errorf("managed upstream key %q has no deletable remote id", strings.TrimSpace(item.Name))
			}
			unboundNameMatch := false
			for _, nameMatch := range nameMatches {
				if nameMatch.UpstreamKeyID <= 0 {
					unboundNameMatch = true
					break
				}
			}
			if unboundNameMatch {
				return nil, fmt.Errorf("managed upstream key %d has no bound local ownership", item.ID)
			}
			if len(nameMatches) > 0 {
				return nil, fmt.Errorf("managed upstream key name %q belongs to a different bound remote identity", strings.TrimSpace(item.Name))
			}
			continue
		}
		if item.ID <= 0 {
			return nil, fmt.Errorf("managed upstream key %q has no deletable remote id", strings.TrimSpace(item.Name))
		}
		if _, duplicate := seenLocal[local.ID]; duplicate {
			return nil, fmt.Errorf("managed upstream key %q matched multiple remote identities", strings.TrimSpace(local.UpstreamKeyName))
		}
		generation, err := s.Store.CaptureUpstreamGroupGeneration(channelID, local.GroupName)
		absent := false
		if errors.Is(err, gorm.ErrRecordNotFound) {
			generation, err = s.Store.RetireUpstreamGroupForKeyRemoval(channelID, channelIdentityRevision, local.GroupName, time.Now(), true)
			absent = err == nil
		}
		if err != nil {
			return nil, fmt.Errorf("capture managed upstream key %d generation: %w", item.ID, err)
		}
		digest, reviewReason, err := s.managedKeyRebuildEvidenceDigest(ctx, generation, strings.TrimSpace(local.GroupName), local, item, channelIdentityRevision)
		if err != nil {
			return nil, err
		}
		if reviewReason != "" {
			return nil, fmt.Errorf("managed upstream key %d requires manual review before rebuild", item.ID)
		}
		ownershipClaim, err := managedUpstreamKeyRemovalOwnershipClaim(*local)
		if err != nil {
			return nil, fmt.Errorf("managed upstream key %d ownership claim is incomplete", item.ID)
		}
		seenLocal[local.ID] = struct{}{}
		targets = append(targets, managedKeyRebuildTarget{
			Remote: item, Local: *local, Generation: generation, ChannelIdentityRevision: channelIdentityRevision,
			Absent: absent, EvidenceDigest: digest, OwnershipClaim: ownershipClaim,
		})
	}
	return targets, nil
}

func (s *Service) revalidateManagedKeyRebuildTarget(ctx context.Context, channelID uint, target managedKeyRebuildTarget) (*connector.APIKey, error) {
	remote, err := s.listAllUpstreamKeysForIdentityRevision(ctx, channelID, target.ChannelIdentityRevision)
	if err != nil {
		return nil, err
	}
	expectedName := strings.TrimSpace(target.Remote.Name)
	if target.Remote.ID <= 0 || expectedName == "" {
		return nil, errors.New("managed upstream key rebuild identity is incomplete")
	}
	var current *connector.APIKey
	for _, item := range remote {
		name := strings.TrimSpace(item.Name)
		if name == expectedName && item.ID != target.Remote.ID {
			return nil, fmt.Errorf("managed upstream key %d name %q reappeared under remote id %d", target.Remote.ID, expectedName, item.ID)
		}
		if item.ID == target.Remote.ID {
			item := item
			current = &item
		}
	}
	if current == nil {
		return nil, gorm.ErrRecordNotFound
	}
	digest, reviewReason, err := s.managedKeyRebuildEvidenceDigest(ctx, target.Generation, strings.TrimSpace(target.Local.GroupName), &target.Local, *current, target.ChannelIdentityRevision)
	if err != nil {
		return nil, err
	}
	if reviewReason != "" || digest != target.EvidenceDigest {
		return nil, fmt.Errorf("managed upstream key %d changed after rebuild verification", target.Remote.ID)
	}
	return current, nil
}

func (s *Service) managedKeyRebuildEvidenceDigest(ctx context.Context, generation storage.UpstreamGroupGeneration, groupName string, local *storage.GatewayUpstreamKey, remote connector.APIKey, channelIdentityRevision uint64) (string, string, error) {
	return s.managedKeyRemovalEvidenceDigest(ctx, generation, groupName, local, remote, channelIdentityRevision)
}

func managedRemovalLocalKey(remote connector.APIKey, groupName string, localByID map[int64]storage.GatewayUpstreamKey, localKeys []storage.GatewayUpstreamKey) *storage.GatewayUpstreamKey {
	groupName = strings.TrimSpace(groupName)
	remoteName := strings.TrimSpace(remote.Name)
	if local, ok := localByID[remote.ID]; ok && strings.TrimSpace(local.GroupName) == groupName && strings.TrimSpace(local.UpstreamKeyName) == remoteName {
		copyLocal := local
		return &copyLocal
	}
	for _, local := range localKeys {
		if strings.TrimSpace(local.GroupName) != groupName || strings.TrimSpace(local.UpstreamKeyName) != remoteName {
			continue
		}
		if local.UpstreamKeyID > 0 && local.UpstreamKeyID != remote.ID {
			continue
		}
		copyLocal := local
		return &copyLocal
	}
	return nil
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}
