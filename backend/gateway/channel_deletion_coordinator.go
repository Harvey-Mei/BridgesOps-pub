package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const (
	channelDeletionCoordinatorActor              = "channel_deletion_coordinator"
	channelDeletionOutcomeCommitTimeout          = 3 * time.Second
	channelDeletionEvidenceDigestVersion  uint16 = 2
	channelDeletionOwnershipDigestVersion uint16 = 1
	channelDeletionFingerprintVersion     uint16 = 1
	channelDeletionInventoryVersion       uint16 = 1
)

type channelDeletionKeyManager interface {
	ListAPIKeysForChannelDeletion(context.Context, uint, uint64, uint64, connector.APIKeyQuery) (*connector.APIKeyPage, error)
	ListAPIKeyGroupsForChannelDeletion(context.Context, uint, uint64, uint64) ([]connector.APIKeyGroup, error)
	RevealAPIKeyForChannelDeletion(context.Context, uint, uint64, uint64, int64) (string, error)
	PrepareAPIKeyDeletionForChannelDeletion(context.Context, uint, uint64, uint64, int64) (func() error, error)
}

type ChannelDeletionCoordinatorConfig struct {
	PollInterval       time.Duration
	ClaimLease         time.Duration
	RetryBase          time.Duration
	RetryMax           time.Duration
	DeleteTimeout      time.Duration
	InventoryTimeout   time.Duration
	DeleteCommitMargin time.Duration
	IntentBatchSize    int
	InventoryPageSize  int
	InventoryMaxPages  int
}

func DefaultChannelDeletionCoordinatorConfig() ChannelDeletionCoordinatorConfig {
	return ChannelDeletionCoordinatorConfig{
		PollInterval: 5 * time.Second, ClaimLease: 10 * time.Minute,
		RetryBase: 15 * time.Second, RetryMax: 15 * time.Minute,
		DeleteTimeout: 30 * time.Second, InventoryTimeout: time.Minute, DeleteCommitMargin: 15 * time.Second,
		IntentBatchSize: 20, InventoryPageSize: 100, InventoryMaxPages: 20,
	}
}

func (config ChannelDeletionCoordinatorConfig) normalized() (ChannelDeletionCoordinatorConfig, error) {
	defaults := DefaultChannelDeletionCoordinatorConfig()
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.ClaimLease == 0 {
		config.ClaimLease = defaults.ClaimLease
	}
	if config.RetryBase == 0 {
		config.RetryBase = defaults.RetryBase
	}
	if config.RetryMax == 0 {
		config.RetryMax = defaults.RetryMax
	}
	if config.DeleteTimeout == 0 {
		config.DeleteTimeout = defaults.DeleteTimeout
	}
	if config.InventoryTimeout == 0 {
		config.InventoryTimeout = defaults.InventoryTimeout
	}
	if config.DeleteCommitMargin == 0 {
		config.DeleteCommitMargin = defaults.DeleteCommitMargin
	}
	if config.IntentBatchSize == 0 {
		config.IntentBatchSize = defaults.IntentBatchSize
	}
	if config.InventoryPageSize == 0 {
		config.InventoryPageSize = defaults.InventoryPageSize
	}
	if config.InventoryMaxPages == 0 {
		config.InventoryMaxPages = defaults.InventoryMaxPages
	}
	if config.PollInterval <= 0 || config.ClaimLease <= 0 || config.RetryBase <= 0 || config.RetryMax < config.RetryBase ||
		config.DeleteTimeout <= 0 || config.InventoryTimeout <= 0 || config.DeleteCommitMargin <= 0 ||
		config.ClaimLease <= config.DeleteTimeout+2*config.InventoryTimeout+config.DeleteCommitMargin ||
		config.IntentBatchSize < 1 || config.IntentBatchSize > 500 ||
		config.InventoryPageSize < 1 || config.InventoryPageSize > 100 ||
		config.InventoryMaxPages < 1 || config.InventoryMaxPages > 100 {
		return config, errors.New("channel deletion coordinator configuration is invalid")
	}
	return config, nil
}

type ChannelDeletionCoordinatorResult struct {
	IntentsScanned int
	Finalized      int
	Failed         int
}

// RunChannelDeletionCoordinator resumes durable channel deletion work at
// startup and continues polling until ctx is canceled.
func (s *Service) RunChannelDeletionCoordinator(ctx context.Context, config ChannelDeletionCoordinatorConfig) error {
	if ctx == nil {
		return errors.New("channel deletion coordinator context is required")
	}
	config, err := config.normalized()
	if err != nil {
		return err
	}
	if s == nil || s.Channels == nil || s.Store == nil || s.Keys == nil {
		return errors.New("channel deletion coordinator dependencies are unavailable")
	}
	if _, ok := s.Keys.(channelDeletionKeyManager); !ok {
		return errors.New("channel deletion key manager is unavailable")
	}
	for {
		result, reconcileErr := s.ReconcileChannelDeletions(ctx, config)
		if reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) && s.Log != nil {
			s.Log.Warn("channel deletion reconciliation failed",
				"intents_scanned", result.IntentsScanned, "finalized", result.Finalized,
				"failed", result.Failed, "err", reconcileErr)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		timer := time.NewTimer(config.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// ReconcileChannelDeletions runs one bounded pass. Individual intent failures
// are persisted with backoff and do not prevent another channel from making
// progress in the same pass.
func (s *Service) ReconcileChannelDeletions(ctx context.Context, config ChannelDeletionCoordinatorConfig) (ChannelDeletionCoordinatorResult, error) {
	result := ChannelDeletionCoordinatorResult{}
	if ctx == nil {
		return result, errors.New("channel deletion coordinator context is required")
	}
	config, err := config.normalized()
	if err != nil {
		return result, err
	}
	if s == nil || s.Channels == nil || s.Store == nil || s.Keys == nil {
		return result, errors.New("channel deletion coordinator dependencies are unavailable")
	}
	manager, ok := s.Keys.(channelDeletionKeyManager)
	if !ok {
		return result, errors.New("channel deletion key manager is unavailable")
	}
	intents, err := s.Channels.ListPendingChannelDeletionIntents(ctx, config.IntentBatchSize)
	if err != nil {
		return result, err
	}
	var failures []error
	for _, intent := range intents {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			break
		}
		result.IntentsScanned++
		finalized, reconcileErr := s.reconcileChannelDeletionIntent(ctx, manager, intent, config)
		if finalized {
			result.Finalized++
		}
		if reconcileErr == nil {
			continue
		}
		result.Failed++
		failures = append(failures, fmt.Errorf("channel deletion intent %d: %w", intent.ID, reconcileErr))
		if ctx.Err() == nil && !errors.Is(reconcileErr, storage.ErrStaleChannelDeletionFence) {
			s.scheduleChannelDeletionRetry(ctx, intent, config, reconcileErr)
		}
	}
	return result, errors.Join(failures...)
}

func channelDeletionFence(intent storage.ChannelDeletionIntent) storage.ChannelDeletionFence {
	return storage.ChannelDeletionFence{
		ChannelID: intent.ChannelID, IntentID: intent.ID, DeletionEpoch: intent.DeletionEpoch,
		IdentityRevision: intent.ChannelIdentityRevision,
	}
}

func (s *Service) reconcileChannelDeletionIntent(ctx context.Context, manager channelDeletionKeyManager, intent storage.ChannelDeletionIntent, config ChannelDeletionCoordinatorConfig) (bool, error) {
	fence := channelDeletionFence(intent)
	if fence.ChannelID == 0 || fence.IntentID == 0 || fence.DeletionEpoch == 0 || fence.IdentityRevision == 0 {
		return false, storage.ErrStaleChannelDeletionFence
	}
	lockedCtx, unlock, err := s.lockModelSyncChannelContext(ctx, fence.ChannelID)
	if err != nil {
		return false, err
	}
	defer unlock()
	if err := s.Channels.RequireRetiredChannelDeletionDomains(lockedCtx, fence); err != nil {
		return false, err
	}
	claims, err := s.Channels.ListChannelDeletionKeyWork(lockedCtx, intent.ID)
	if err != nil {
		return false, err
	}
	for _, listed := range claims {
		if err := lockedCtx.Err(); err != nil {
			return false, err
		}
		if channelDeletionClaimHandled(listed.Status) {
			continue
		}
		if listed.Status == storage.ChannelDeletionKeyManualReviewRequired {
			if err := s.resolveChannelDeletionManualClaimByPolicy(lockedCtx, fence, listed); err != nil {
				return false, err
			}
			continue
		}
		claim, acquired, err := s.Channels.ClaimChannelDeletionKey(lockedCtx, fence, listed.ID, config.ClaimLease)
		if err != nil {
			return false, err
		}
		if !acquired {
			continue
		}
		recovering := listed.Status != storage.ChannelDeletionKeyPendingVerification || claim.ClaimEpoch > 1
		recoveringUnknownDelete := listed.Status == storage.ChannelDeletionKeyProcessingUnknown
		if err := s.processChannelDeletionKeyClaim(lockedCtx, manager, fence, intent.ChannelTypeSnapshot, claim, recovering, recoveringUnknownDelete, config); err != nil {
			return false, err
		}
	}
	claims, err = s.Channels.ListChannelDeletionKeyWork(lockedCtx, intent.ID)
	if err != nil {
		return false, err
	}
	hasManual := false
	allHandled := true
	var nextAttemptAt *time.Time
	for _, claim := range claims {
		hasManual = hasManual || claim.Status == storage.ChannelDeletionKeyManualReviewRequired
		allHandled = allHandled && channelDeletionClaimHandled(claim.Status)
		if claim.Status == storage.ChannelDeletionKeyProcessing && claim.LeaseExpiresAt != nil &&
			(nextAttemptAt == nil || claim.LeaseExpiresAt.Before(*nextAttemptAt)) {
			next := claim.LeaseExpiresAt.UTC()
			nextAttemptAt = &next
		}
		if claim.Status == storage.ChannelDeletionKeyProcessingUnknown {
			next := time.Now().UTC().Add(channelDeletionRetryDelay(config, intent.AttemptCount+1))
			if nextAttemptAt == nil || next.Before(*nextAttemptAt) {
				nextAttemptAt = &next
			}
		}
	}
	now := time.Now().UTC()
	if hasManual {
		if err := s.Channels.MarkChannelDeletionIntentState(lockedCtx, fence, storage.ChannelDeletionIntentManualReviewRequired, "channel deletion requires manual review", nil, now); err != nil {
			return false, err
		}
		return false, nil
	}
	if !allHandled {
		if err := s.Channels.MarkChannelDeletionIntentState(lockedCtx, fence, storage.ChannelDeletionIntentPendingRemoteCleanup, "", nextAttemptAt, now); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := s.Channels.MarkChannelDeletionIntentState(lockedCtx, fence, storage.ChannelDeletionIntentReadyForFinalization, "", nil, now); err != nil {
		return false, err
	}
	if err := s.Channels.FinalizeChannelDeletion(lockedCtx, fence, channelDeletionCoordinatorActor, time.Now().UTC()); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) resolveChannelDeletionManualClaimByPolicy(ctx context.Context, fence storage.ChannelDeletionFence, claim storage.ChannelDeletionKeyClaim) error {
	reviewFence := storage.ChannelDeletionKeyReviewFence{
		ChannelDeletionFence: fence,
		KeyClaimID:           claim.ID,
		ClaimEpoch:           claim.ClaimEpoch,
	}
	groupEvidenceComplete := claim.UpstreamGroupID != nil && claim.PresenceGeneration != nil &&
		strings.TrimSpace(claim.CanonicalGroupName) != ""
	ownershipClaimComplete := len(strings.TrimSpace(claim.LocalOwnershipClaim)) == sha256.Size*2
	if claim.Disposition == storage.ChannelDeletionKeyDispositionVerifyManagedDelete &&
		groupEvidenceComplete && ownershipClaimComplete {
		return s.Channels.RequeueChannelDeletionKeyManualReview(
			ctx, reviewFence, "automatic_policy_revalidation", channelDeletionCoordinatorActor, time.Now().UTC(),
		)
	}
	return s.Channels.RetainChannelDeletionKeyAfterReview(
		ctx, reviewFence, "automatic_policy_retained", channelDeletionCoordinatorActor, time.Now().UTC(),
	)
}

func channelDeletionClaimHandled(status storage.ChannelDeletionKeyClaimStatus) bool {
	switch status {
	case storage.ChannelDeletionKeyDeleteConfirmed,
		storage.ChannelDeletionKeyInitiallyAbsentConfirmed,
		storage.ChannelDeletionKeyRetainedAdopted,
		storage.ChannelDeletionKeyRetainedByReview:
		return true
	default:
		return false
	}
}

func (s *Service) scheduleChannelDeletionRetry(ctx context.Context, intent storage.ChannelDeletionIntent, config ChannelDeletionCoordinatorConfig, reconcileErr error) {
	if ctx == nil || ctx.Err() != nil || errors.Is(reconcileErr, storage.ErrStaleChannelDeletionFence) {
		return
	}
	delay := channelDeletionRetryDelay(config, intent.AttemptCount)
	now := time.Now().UTC()
	next := now.Add(delay)
	status := storage.ChannelDeletionIntentPendingRemoteCleanup
	if intent.Status == storage.ChannelDeletionIntentReadyForFinalization {
		status = storage.ChannelDeletionIntentReadyForFinalization
	}
	_ = s.Channels.MarkChannelDeletionIntentState(ctx, channelDeletionFence(intent), status, "channel deletion retry scheduled", &next, now)
}

func channelDeletionRetryDelay(config ChannelDeletionCoordinatorConfig, attemptCount uint64) time.Duration {
	delay := config.RetryBase
	for attempt := uint64(0); attempt < attemptCount && delay < config.RetryMax; attempt++ {
		if delay > config.RetryMax/2 {
			delay = config.RetryMax
			break
		}
		delay *= 2
	}
	if delay > config.RetryMax {
		delay = config.RetryMax
	}
	return delay
}

type channelDeletionInventory struct {
	Keys   []connector.APIKey
	Digest string
}

var errChannelDeletionInventoryInvalid = errors.New("upstream api key inventory is invalid")

func invalidChannelDeletionInventory(reason string) error {
	return fmt.Errorf("%w: %s", errChannelDeletionInventoryInvalid, reason)
}

func (s *Service) listChannelDeletionInventory(ctx context.Context, manager channelDeletionKeyManager, fence storage.ChannelDeletionFence, config ChannelDeletionCoordinatorConfig) (channelDeletionInventory, error) {
	inventory := channelDeletionInventory{Keys: make([]connector.APIKey, 0)}
	inventoryCtx, cancelInventory := context.WithTimeout(ctx, config.InventoryTimeout)
	defer cancelInventory()
	seenIDs := make(map[int64]struct{})
	seenNames := make(map[string]int64)
	expectedPageSize := 0
	expectedPages := 0
	var expectedTotal int64
	for pageNumber := 1; pageNumber <= config.InventoryMaxPages; pageNumber++ {
		page, err := manager.ListAPIKeysForChannelDeletion(inventoryCtx, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision, connector.APIKeyQuery{
			Page: pageNumber, PageSize: config.InventoryPageSize,
		})
		if err != nil {
			return inventory, err
		}
		if page == nil || page.Page != pageNumber || page.PageSize < 1 || page.PageSize > config.InventoryPageSize || len(page.Items) > page.PageSize || page.Pages < 0 || page.Total < 0 {
			return inventory, invalidChannelDeletionInventory("pagination metadata is invalid")
		}
		if pageNumber == 1 {
			expectedPageSize, expectedPages, expectedTotal = page.PageSize, page.Pages, page.Total
			if expectedPages == 0 && expectedTotal == 0 && expectedPageSize != config.InventoryPageSize {
				return inventory, invalidChannelDeletionInventory("an unbounded response changed the requested page size")
			}
		} else if page.PageSize != expectedPageSize || page.Pages != expectedPages || page.Total != expectedTotal {
			return inventory, invalidChannelDeletionInventory("pagination changed during scan")
		}
		if expectedPages > 0 {
			if pageNumber > expectedPages || (pageNumber < expectedPages && len(page.Items) != expectedPageSize) {
				return inventory, invalidChannelDeletionInventory("a short or out-of-range page precedes the declared final page")
			}
		}
		if expectedTotal > 0 {
			firstItem := int64(pageNumber-1) * int64(expectedPageSize)
			returnedThrough := firstItem + int64(len(page.Items))
			if returnedThrough > expectedTotal || (len(page.Items) < expectedPageSize && returnedThrough < expectedTotal) {
				return inventory, invalidChannelDeletionInventory("page contents contradict the declared total")
			}
			if expectedPages > 0 {
				pagesFromTotal := int((expectedTotal + int64(expectedPageSize) - 1) / int64(expectedPageSize))
				if pagesFromTotal != expectedPages {
					return inventory, invalidChannelDeletionInventory("page count contradicts the declared total")
				}
			}
		}
		for _, item := range page.Items {
			if item.ID <= 0 {
				return inventory, invalidChannelDeletionInventory("an item has an invalid id")
			}
			if _, duplicate := seenIDs[item.ID]; duplicate {
				return inventory, invalidChannelDeletionInventory(fmt.Sprintf("api key id %d is ambiguous", item.ID))
			}
			seenIDs[item.ID] = struct{}{}
			name := strings.TrimSpace(item.Name)
			if previousID, duplicate := seenNames[name]; name != "" && duplicate {
				return inventory, invalidChannelDeletionInventory(fmt.Sprintf("api key name %q is ambiguous between ids %d and %d", name, previousID, item.ID))
			}
			if name != "" {
				seenNames[name] = item.ID
			}
			inventory.Keys = append(inventory.Keys, item)
		}
		complete := false
		switch {
		case expectedPages > 0:
			complete = pageNumber == expectedPages
		case expectedTotal > 0:
			complete = int64(len(inventory.Keys)) == expectedTotal
		default:
			complete = len(page.Items) < expectedPageSize
		}
		if complete {
			if expectedTotal > 0 && int64(len(inventory.Keys)) != expectedTotal {
				return inventory, invalidChannelDeletionInventory("the complete scan does not match the declared total")
			}
			sort.Slice(inventory.Keys, func(i, j int) bool { return inventory.Keys[i].ID < inventory.Keys[j].ID })
			inventory.Digest, err = channelDeletionInventoryDigest(inventory.Keys)
			return inventory, err
		}
	}
	return inventory, invalidChannelDeletionInventory("the scan exceeds the safe page limit")
}

func channelDeletionInventoryDigest(keys []connector.APIKey) (string, error) {
	snapshots := make([]managedKeyRemovalRemoteSnapshot, 0, len(keys))
	for _, key := range keys {
		snapshots = append(snapshots, managedKeyRemovalSnapshot(key))
	}
	return channelDeletionJSONDigest(struct {
		Version uint16                            `json:"version"`
		Keys    []managedKeyRemovalRemoteSnapshot `json:"keys"`
	}{Version: channelDeletionInventoryVersion, Keys: snapshots})
}

func channelDeletionJSONDigest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

type channelDeletionLocatedKey struct {
	Remote *connector.APIKey
	Absent bool
	Reason string
}

func locateChannelDeletionKey(inventory channelDeletionInventory, claim *storage.ChannelDeletionKeyClaim) channelDeletionLocatedKey {
	if claim == nil || strings.TrimSpace(claim.UpstreamKeyName) == "" {
		return channelDeletionLocatedKey{Reason: "remote_identity_incomplete"}
	}
	expectedName := strings.TrimSpace(claim.UpstreamKeyName)
	expectedRemoteID := claim.UpstreamKeyID
	if expectedRemoteID <= 0 {
		expectedRemoteID = claim.ObservedRemoteID
	}
	var byID, byName *connector.APIKey
	for index := range inventory.Keys {
		item := &inventory.Keys[index]
		if expectedRemoteID > 0 && item.ID == expectedRemoteID {
			byID = item
		}
		if strings.TrimSpace(item.Name) == expectedName {
			byName = item
		}
	}
	if isStronglyManagedUpstreamKeyName(expectedName) {
		if byName == nil {
			return channelDeletionLocatedKey{Absent: true}
		}
		copyRemote := *byName
		return channelDeletionLocatedKey{Remote: &copyRemote}
	}
	if expectedRemoteID > 0 {
		if byID == nil {
			if byName != nil {
				return channelDeletionLocatedKey{Reason: "remote_name_reused"}
			}
			return channelDeletionLocatedKey{Absent: true}
		}
		if strings.TrimSpace(byID.Name) != expectedName || (byName != nil && byName.ID != byID.ID) {
			return channelDeletionLocatedKey{Reason: "remote_identity_changed"}
		}
		copyRemote := *byID
		return channelDeletionLocatedKey{Remote: &copyRemote}
	}
	if claim.OwnershipProvenance != storage.GatewayUpstreamKeyProvenanceProvisioningClaim {
		return channelDeletionLocatedKey{Reason: "remote_id_missing"}
	}
	if byName == nil {
		return channelDeletionLocatedKey{Absent: true}
	}
	copyRemote := *byName
	return channelDeletionLocatedKey{Remote: &copyRemote}
}

func (s *Service) processChannelDeletionKeyClaim(ctx context.Context, manager channelDeletionKeyManager, fence storage.ChannelDeletionFence, channelType storage.ChannelType, claim *storage.ChannelDeletionKeyClaim, recovering, recoveringUnknownDelete bool, config ChannelDeletionCoordinatorConfig) error {
	if claim == nil || claim.LocalKey == nil {
		return storage.ErrChannelDeletionKeyFreezeChanged
	}
	inventory, err := s.listChannelDeletionInventory(ctx, manager, fence, config)
	if err != nil {
		if recoveringUnknownDelete {
			return s.keepChannelDeletionDeleteOutcomeUnknown(ctx, fence, claim, "recovery_inventory_failed", "", "remote deletion confirmation is unavailable")
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return s.retryOrRetainChannelDeletionClaim(ctx, fence, claim, "inventory_read_failed", err, "", "", "")
	}
	located := locateChannelDeletionKey(inventory, claim)
	if recoveringUnknownDelete {
		return s.reconcileUnknownChannelDeletionOutcome(ctx, manager, fence, claim, inventory, located, config)
	}
	if located.Reason != "" {
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, located.Reason, inventory.Digest, "", "")
	}
	if located.Remote != nil {
		if claim.ObservedRemoteID > 0 && claim.ObservedRemoteID != located.Remote.ID {
			return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, "remote_name_reused", inventory.Digest, "", "")
		}
		claimFence := storage.ChannelDeletionKeyClaimFence{
			ChannelDeletionFence: fence, KeyClaimID: claim.ID, ClaimEpoch: claim.ClaimEpoch,
		}
		if err := s.Channels.BindChannelDeletionObservedRemoteID(ctx, claimFence, located.Remote.ID); err != nil {
			return err
		}
		claim.ObservedRemoteID = located.Remote.ID
	}
	if claim.Disposition == storage.ChannelDeletionKeyDispositionRetainAdopted {
		fingerprint, err := channelDeletionRemoteFingerprint(located.Remote)
		if err != nil {
			return err
		}
		observedRemoteID := int64(0)
		if located.Remote != nil {
			observedRemoteID = located.Remote.ID
		}
		ownership, err := channelDeletionOwnershipDigestWithRemote(*claim, "", "", observedRemoteID, "")
		if err != nil {
			return err
		}
		reason := "adopted_remote_retained"
		if located.Absent {
			reason = "adopted_remote_absent"
		}
		return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyRetainedAdopted, reason, ownership, fingerprint, inventory.Digest, "")
	}
	if claim.Disposition != storage.ChannelDeletionKeyDispositionVerifyManagedDelete {
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, "ownership_provenance_untrusted", inventory.Digest, "", "")
	}
	localSecretHash, ownership, reason, err := s.channelDeletionLocalOwnership(*claim)
	if err != nil {
		return err
	}
	if reason != "" {
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, reason, inventory.Digest, "", ownership)
	}
	if located.Absent {
		second, err := s.listChannelDeletionInventory(ctx, manager, fence, config)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return s.retryOrRetainChannelDeletionClaim(ctx, fence, claim, "inventory_revalidation_failed", err, inventory.Digest, "", ownership)
		}
		current := locateChannelDeletionKey(second, claim)
		if current.Reason != "" || !current.Absent || second.Digest != inventory.Digest {
			return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, "remote_identity_changed", second.Digest, "", ownership)
		}
		status := storage.ChannelDeletionKeyInitiallyAbsentConfirmed
		outcomeReason := "remote_initially_absent"
		if recovering {
			status = storage.ChannelDeletionKeyDeleteConfirmed
			outcomeReason = "remote_absence_confirmed_after_recovery"
		}
		return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, status, outcomeReason, ownership, "", second.Digest, "")
	}
	fingerprint, revealedSecretHash, reason, err := s.verifyChannelDeletionRemoteKey(ctx, manager, fence, channelType, claim, *located.Remote, localSecretHash)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return s.retryOrRetainChannelDeletionClaim(ctx, fence, claim, "remote_verification_failed", err, inventory.Digest, "", ownership)
	}
	if reason != "" {
		observedOwnership, digestErr := channelDeletionOwnershipDigestWithRemote(*claim, claim.LocalOwnershipClaim, localSecretHash, located.Remote.ID, revealedSecretHash)
		if digestErr != nil {
			return digestErr
		}
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, reason, inventory.Digest, fingerprint, observedOwnership)
	}
	ownership, err = channelDeletionOwnershipDigestWithRemote(*claim, claim.LocalOwnershipClaim, localSecretHash, located.Remote.ID, revealedSecretHash)
	if err != nil {
		return err
	}
	second, err := s.listChannelDeletionInventory(ctx, manager, fence, config)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return s.retryOrRetainChannelDeletionClaim(ctx, fence, claim, "inventory_revalidation_failed", err, inventory.Digest, fingerprint, ownership)
	}
	current := locateChannelDeletionKey(second, claim)
	if current.Reason != "" || current.Absent || current.Remote == nil {
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, "remote_identity_changed", second.Digest, fingerprint, ownership)
	}
	claim.ObservedRemoteID = current.Remote.ID
	currentFingerprint, currentSecretHash, reason, err := s.verifyChannelDeletionRemoteKey(ctx, manager, fence, channelType, claim, *current.Remote, localSecretHash)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return s.retryOrRetainChannelDeletionClaim(ctx, fence, claim, "remote_revalidation_failed", err, second.Digest, fingerprint, ownership)
	}
	if reason != "" || currentFingerprint != fingerprint || currentSecretHash != revealedSecretHash || second.Digest != inventory.Digest {
		observedOwnership, digestErr := channelDeletionOwnershipDigestWithRemote(*claim, claim.LocalOwnershipClaim, localSecretHash, current.Remote.ID, currentSecretHash)
		if digestErr != nil {
			return digestErr
		}
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, "remote_facts_changed", second.Digest, currentFingerprint, observedOwnership)
	}
	ownership, err = channelDeletionOwnershipDigestWithRemote(*claim, claim.LocalOwnershipClaim, localSecretHash, current.Remote.ID, currentSecretHash)
	if err != nil {
		return err
	}
	deleteCtx, cancelDelete := context.WithTimeout(ctx, config.DeleteTimeout)
	defer cancelDelete()
	remoteDelete, err := manager.PrepareAPIKeyDeletionForChannelDeletion(deleteCtx, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision, current.Remote.ID)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return s.retryOrRetainChannelDeletionClaim(ctx, fence, claim, "delete_prepare_failed", err, second.Digest, currentFingerprint, ownership)
	}
	if remoteDelete == nil {
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, "delete_prepare_incomplete", second.Digest, currentFingerprint, ownership)
	}
	claimFence := storage.ChannelDeletionKeyClaimFence{ChannelDeletionFence: fence, KeyClaimID: claim.ID, ClaimEpoch: claim.ClaimEpoch}
	deleteConfirmationBound := config.DeleteTimeout + 2*config.InventoryTimeout + config.DeleteCommitMargin
	if _, err := s.Channels.RenewChannelDeletionKeyClaim(ctx, claimFence, config.ClaimLease, deleteConfirmationBound); err != nil {
		return err
	}
	if err := remoteDelete(); err != nil {
		return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyProcessingUnknown, "remote_delete_result_unknown", ownership, currentFingerprint, second.Digest, "remote deletion outcome is unknown")
	}
	postDelete, err := s.listChannelDeletionInventory(ctx, manager, fence, config)
	if err != nil {
		return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyProcessingUnknown, "post_delete_inventory_failed", ownership, currentFingerprint, second.Digest, "remote deletion confirmation is unavailable")
	}
	remaining := locateChannelDeletionKey(postDelete, claim)
	if ctx.Err() != nil {
		return s.keepChannelDeletionDeleteOutcomeUnknown(ctx, fence, claim, "post_delete_inventory_canceled", postDelete.Digest, "remote deletion confirmation was canceled")
	}
	if remaining.Reason != "" {
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, remaining.Reason, postDelete.Digest, currentFingerprint, ownership)
	}
	if !remaining.Absent {
		return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyProcessingUnknown, "remote_delete_not_confirmed", ownership, currentFingerprint, postDelete.Digest, "remote key remains present after deletion")
	}
	stablePostDelete, err := s.listChannelDeletionInventory(ctx, manager, fence, config)
	if err != nil {
		return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyProcessingUnknown, "post_delete_inventory_revalidation_failed", ownership, currentFingerprint, postDelete.Digest, "remote deletion confirmation is unstable")
	}
	stableRemaining := locateChannelDeletionKey(stablePostDelete, claim)
	if ctx.Err() != nil {
		return s.keepChannelDeletionDeleteOutcomeUnknown(ctx, fence, claim, "post_delete_inventory_revalidation_canceled", stablePostDelete.Digest, "remote deletion confirmation was canceled")
	}
	if stableRemaining.Reason != "" {
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, stableRemaining.Reason, stablePostDelete.Digest, currentFingerprint, ownership)
	}
	if !stableRemaining.Absent || stablePostDelete.Digest != postDelete.Digest {
		return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyProcessingUnknown, "post_delete_inventory_changed", ownership, currentFingerprint, stablePostDelete.Digest, "remote deletion confirmation is unstable")
	}
	return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyDeleteConfirmed, "remote_delete_confirmed", ownership, currentFingerprint, stablePostDelete.Digest, "")
}

func (s *Service) reconcileUnknownChannelDeletionOutcome(ctx context.Context, manager channelDeletionKeyManager, fence storage.ChannelDeletionFence, claim *storage.ChannelDeletionKeyClaim, inventory channelDeletionInventory, located channelDeletionLocatedKey, config ChannelDeletionCoordinatorConfig) error {
	if located.Reason != "" {
		return s.keepChannelDeletionDeleteOutcomeUnknown(ctx, fence, claim, "recovery_remote_identity_unconfirmed", inventory.Digest, "remote deletion identity is ambiguous")
	}
	if !located.Absent {
		return s.keepChannelDeletionDeleteOutcomeUnknown(ctx, fence, claim, "remote_delete_still_present_after_recovery", inventory.Digest, "remote key remains present after an uncertain deletion")
	}
	second, err := s.listChannelDeletionInventory(ctx, manager, fence, config)
	if err != nil {
		return s.keepChannelDeletionDeleteOutcomeUnknown(ctx, fence, claim, "recovery_inventory_revalidation_failed", inventory.Digest, "remote deletion confirmation is unstable")
	}
	current := locateChannelDeletionKey(second, claim)
	if current.Reason != "" || !current.Absent || second.Digest != inventory.Digest {
		return s.keepChannelDeletionDeleteOutcomeUnknown(ctx, fence, claim, "recovery_remote_absence_unstable", second.Digest, "remote deletion confirmation is unstable")
	}
	ownership := strings.TrimSpace(claim.OwnershipDigest)
	if !channelDeletionDigestValid(ownership) {
		var digestErr error
		ownership, digestErr = channelDeletionOwnershipDigest(*claim, "", "")
		if digestErr != nil {
			return digestErr
		}
	}
	return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyDeleteConfirmed, "remote_absence_confirmed_after_recovery", ownership, claim.ObservedFingerprint, second.Digest, "")
}

func (s *Service) keepChannelDeletionDeleteOutcomeUnknown(ctx context.Context, fence storage.ChannelDeletionFence, claim *storage.ChannelDeletionKeyClaim, reason, inventoryDigest, errorSummary string) error {
	if claim == nil {
		return storage.ErrChannelDeletionKeyClaimLost
	}
	ownership := strings.TrimSpace(claim.OwnershipDigest)
	if !channelDeletionDigestValid(ownership) {
		var err error
		ownership, err = channelDeletionOwnershipDigest(*claim, "", "")
		if err != nil {
			return err
		}
	}
	return s.recordChannelDeletionClaimOutcome(ctx, fence, claim, storage.ChannelDeletionKeyProcessingUnknown, reason, ownership, claim.ObservedFingerprint, inventoryDigest, errorSummary)
}

func channelDeletionDigestValid(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (s *Service) channelDeletionLocalOwnership(claim storage.ChannelDeletionKeyClaim) (string, string, string, error) {
	local := claim.LocalKey
	if local == nil || local.ID != claim.LocalKeyID || local.ChannelID != claim.ChannelID || local.KeyGeneration != claim.KeyGeneration {
		return "", "", "local_ownership_changed", nil
	}
	localClaimDigest, err := storage.ChannelDeletionManagedOwnershipClaimDigest(*local)
	if err != nil || localClaimDigest != claim.LocalOwnershipClaim {
		ownership, digestErr := channelDeletionOwnershipDigest(claim, "", "")
		return "", ownership, "local_ownership_changed", digestErr
	}
	if isStronglyManagedUpstreamKeyName(local.UpstreamKeyName) {
		ownership, digestErr := channelDeletionOwnershipDigest(claim, localClaimDigest, "")
		return "", ownership, "", digestErr
	}
	switch claim.OwnershipProvenance {
	case storage.GatewayUpstreamKeyProvenanceCreated:
		if s.Cipher == nil || strings.TrimSpace(local.APIKeyCipher) == "" || strings.TrimSpace(local.APIKeyPrefix) == "" || local.UpstreamKeyID <= 0 {
			ownership, digestErr := channelDeletionOwnershipDigest(claim, localClaimDigest, "")
			return "", ownership, "created_ownership_incomplete", digestErr
		}
		plain, decryptErr := s.Cipher.Decrypt(local.APIKeyCipher)
		plain = strings.TrimSpace(plain)
		if decryptErr != nil || plain == "" || keyPrefix(plain) != strings.TrimSpace(local.APIKeyPrefix) {
			ownership, digestErr := channelDeletionOwnershipDigest(claim, localClaimDigest, "")
			return "", ownership, "local_secret_unavailable", digestErr
		}
		secretHash := HashKey(plain)
		ownership, digestErr := channelDeletionOwnershipDigest(claim, localClaimDigest, secretHash)
		return secretHash, ownership, "", digestErr
	case storage.GatewayUpstreamKeyProvenanceProvisioningClaim:
		if !local.Provisioning || local.ProvisioningStartedAt == nil || !upstreamProvisioningKeyNamePattern.MatchString(strings.TrimSpace(local.UpstreamKeyName)) {
			ownership, digestErr := channelDeletionOwnershipDigest(claim, localClaimDigest, "")
			return "", ownership, "provisioning_claim_incomplete", digestErr
		}
		ownership, digestErr := channelDeletionOwnershipDigest(claim, localClaimDigest, "")
		return "", ownership, "", digestErr
	default:
		ownership, digestErr := channelDeletionOwnershipDigest(claim, localClaimDigest, "")
		return "", ownership, "ownership_provenance_untrusted", digestErr
	}
}

func channelDeletionOwnershipDigest(claim storage.ChannelDeletionKeyClaim, localClaimDigest, secretHash string) (string, error) {
	return channelDeletionOwnershipDigestWithRemote(claim, localClaimDigest, secretHash, 0, "")
}

func channelDeletionOwnershipDigestWithRemote(claim storage.ChannelDeletionKeyClaim, localClaimDigest, secretHash string, observedRemoteID int64, revealedSecretHash string) (string, error) {
	cipherHash := ""
	if claim.LocalKey != nil && strings.TrimSpace(claim.LocalKey.APIKeyCipher) != "" {
		sum := sha256.Sum256([]byte(claim.LocalKey.APIKeyCipher))
		cipherHash = hex.EncodeToString(sum[:])
	}
	startedAt := ""
	if claim.ProvisioningStartedAt != nil {
		startedAt = claim.ProvisioningStartedAt.UTC().Format(time.RFC3339Nano)
	}
	return channelDeletionJSONDigest(struct {
		Version               uint16 `json:"version"`
		ClaimDigest           string `json:"claim_digest"`
		LocalClaimDigest      string `json:"local_claim_digest"`
		OwnershipProvenance   string `json:"ownership_provenance"`
		Provisioning          bool   `json:"provisioning"`
		ProvisioningStartedAt string `json:"provisioning_started_at"`
		CipherHash            string `json:"cipher_hash"`
		SecretHash            string `json:"secret_hash"`
		ObservedRemoteID      int64  `json:"observed_remote_id"`
		RevealedSecretHash    string `json:"revealed_secret_hash"`
	}{
		Version: channelDeletionOwnershipDigestVersion, ClaimDigest: claim.ClaimDigest,
		LocalClaimDigest: localClaimDigest, OwnershipProvenance: claim.OwnershipProvenance,
		Provisioning: claim.Provisioning, ProvisioningStartedAt: startedAt,
		CipherHash: cipherHash, SecretHash: secretHash,
		ObservedRemoteID: observedRemoteID, RevealedSecretHash: revealedSecretHash,
	})
}

func channelDeletionRemoteFingerprint(remote *connector.APIKey) (string, error) {
	if remote == nil {
		return "", nil
	}
	return channelDeletionJSONDigest(struct {
		Version uint16                          `json:"version"`
		Remote  managedKeyRemovalRemoteSnapshot `json:"remote"`
	}{Version: channelDeletionFingerprintVersion, Remote: managedKeyRemovalSnapshot(*remote)})
}

func (s *Service) verifyChannelDeletionRemoteKey(ctx context.Context, manager channelDeletionKeyManager, fence storage.ChannelDeletionFence, channelType storage.ChannelType, claim *storage.ChannelDeletionKeyClaim, remote connector.APIKey, localSecretHash string) (string, string, string, error) {
	fingerprint, err := channelDeletionRemoteFingerprint(&remote)
	if err != nil {
		return "", "", "", err
	}
	if isStronglyManagedUpstreamKeyName(claim.UpstreamKeyName) {
		if remote.ID <= 0 || strings.TrimSpace(remote.Name) != strings.TrimSpace(claim.UpstreamKeyName) {
			return fingerprint, "", "remote_identity_changed", nil
		}
		return fingerprint, "", "", nil
	}
	if claim.OwnershipProvenance == storage.GatewayUpstreamKeyProvenanceCreated &&
		claim.UpstreamKeyID > 0 && remote.ID == claim.UpstreamKeyID &&
		strings.TrimSpace(remote.Name) == strings.TrimSpace(claim.UpstreamKeyName) && localSecretHash != "" {
		revealed, revealErr := manager.RevealAPIKeyForChannelDeletion(
			ctx, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision, remote.ID,
		)
		if revealErr != nil {
			return fingerprint, "", "", revealErr
		}
		revealedHash := HashKey(revealed)
		if strings.TrimSpace(revealed) == "" || revealedHash != localSecretHash {
			return fingerprint, revealedHash, "remote_secret_changed", nil
		}
		return fingerprint, revealedHash, "", nil
	}
	expectedSource := connector.APIKeySource("")
	switch channelType {
	case storage.ChannelTypeNewAPI:
		expectedSource = connector.APIKeySourceNewAPI
	case storage.ChannelTypeSub2API:
		expectedSource = connector.APIKeySourceSub2API
	default:
		return fingerprint, "", "channel_type_invalid", nil
	}
	if remote.ID <= 0 || (claim.UpstreamKeyID > 0 && remote.ID != claim.UpstreamKeyID) ||
		strings.TrimSpace(remote.Name) != strings.TrimSpace(claim.UpstreamKeyName) ||
		remote.Source != expectedSource {
		return fingerprint, "", "remote_identity_changed", nil
	}
	expectedGroup := strings.TrimSpace(claim.LocalKey.GroupName)
	if !managedUpstreamKeyFingerprintMatchesForName(remote, claim.UpstreamKeyName, expectedGroup, nil, false) {
		return fingerprint, "", "creation_fingerprint_incomplete", nil
	}
	if remote.Source == connector.APIKeySourceSub2API && (remote.GroupID == nil || *remote.GroupID <= 0) {
		return fingerprint, "", "sub2api_group_identity_invalid", nil
	}
	if remote.GroupID != nil {
		if *remote.GroupID <= 0 {
			return fingerprint, "", "remote_group_identity_invalid", nil
		}
		groups, err := manager.ListAPIKeyGroupsForChannelDeletion(ctx, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision)
		if err != nil {
			return fingerprint, "", "", err
		}
		matches := 0
		seenIDs := make(map[int64]struct{})
		for _, group := range groups {
			if group.ID == nil {
				continue
			}
			if *group.ID <= 0 {
				return fingerprint, "", "remote_group_identity_invalid", nil
			}
			if _, duplicate := seenIDs[*group.ID]; duplicate {
				return fingerprint, "", "remote_group_identity_ambiguous", nil
			}
			seenIDs[*group.ID] = struct{}{}
			if *group.ID == *remote.GroupID {
				if strings.TrimSpace(group.Name) != expectedGroup {
					return fingerprint, "", "remote_group_identity_changed", nil
				}
				matches++
			}
		}
		if matches != 1 {
			return fingerprint, "", "remote_group_identity_incomplete", nil
		}
	}
	revealed, err := manager.RevealAPIKeyForChannelDeletion(ctx, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision, remote.ID)
	if err != nil {
		return fingerprint, "", "", err
	}
	revealedHash := HashKey(revealed)
	if strings.TrimSpace(revealed) == "" ||
		(claim.OwnershipProvenance == storage.GatewayUpstreamKeyProvenanceCreated && revealedHash != localSecretHash) {
		return fingerprint, revealedHash, "remote_secret_changed", nil
	}
	return fingerprint, revealedHash, "", nil
}

func (s *Service) retainChannelDeletionClaimByPolicy(ctx context.Context, fence storage.ChannelDeletionFence, claim *storage.ChannelDeletionKeyClaim, reason, inventoryDigest, fingerprint, ownership string) error {
	if ownership == "" {
		var err error
		ownership, err = channelDeletionOwnershipDigest(*claim, "", "")
		if err != nil {
			return err
		}
	}
	return s.recordChannelDeletionClaimOutcome(
		ctx, fence, claim, storage.ChannelDeletionKeyRetainedByReview,
		reason, ownership, fingerprint, inventoryDigest, "automatic cleanup retained the remote key",
	)
}

func channelDeletionErrorIsRetryable(err error) bool {
	if err == nil || errors.Is(err, errChannelDeletionInventoryInvalid) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var httpErr *connector.HTTPError
	if errors.As(err, &httpErr) && httpErr != nil {
		switch httpErr.StatusCode {
		case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
			return true
		default:
			return httpErr.StatusCode >= http.StatusInternalServerError && httpErr.StatusCode <= 599
		}
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		if networkErr.Timeout() {
			return true
		}
		temporary, ok := any(networkErr).(interface{ Temporary() bool })
		return ok && temporary.Temporary()
	}
	return false
}

func (s *Service) retryOrRetainChannelDeletionClaim(ctx context.Context, fence storage.ChannelDeletionFence, claim *storage.ChannelDeletionKeyClaim, reason string, remoteErr error, inventoryDigest, fingerprint, ownership string) error {
	if !channelDeletionErrorIsRetryable(remoteErr) {
		return s.retainChannelDeletionClaimByPolicy(ctx, fence, claim, reason, inventoryDigest, fingerprint, ownership)
	}
	if claim == nil {
		return storage.ErrChannelDeletionKeyClaimLost
	}
	claimFence := storage.ChannelDeletionKeyClaimFence{
		ChannelDeletionFence: fence,
		KeyClaimID:           claim.ID,
		ClaimEpoch:           claim.ClaimEpoch,
	}
	if err := s.Channels.RetryChannelDeletionKeyClaim(ctx, claimFence, reason, channelDeletionCoordinatorActor, time.Now().UTC()); err != nil {
		return err
	}
	return fmt.Errorf("retryable channel deletion key failure: %s", reason)
}

func (s *Service) recordChannelDeletionClaimOutcome(ctx context.Context, fence storage.ChannelDeletionFence, claim *storage.ChannelDeletionKeyClaim, status storage.ChannelDeletionKeyClaimStatus, reason, ownershipDigest, fingerprint, inventoryDigest, errorSummary string) error {
	if claim == nil {
		return storage.ErrChannelDeletionKeyClaimLost
	}
	evidenceDigest, err := channelDeletionJSONDigest(struct {
		Version          uint16                                `json:"version"`
		IntentID         uint64                                `json:"intent_id"`
		KeyClaimID       uint64                                `json:"key_claim_id"`
		ClaimEpoch       uint64                                `json:"claim_epoch"`
		ClaimDigest      string                                `json:"claim_digest"`
		Status           storage.ChannelDeletionKeyClaimStatus `json:"status"`
		Reason           string                                `json:"reason"`
		OwnershipDigest  string                                `json:"ownership_digest"`
		Fingerprint      string                                `json:"fingerprint"`
		InventoryDigest  string                                `json:"inventory_digest"`
		ObservedRemoteID int64                                 `json:"observed_remote_id"`
	}{
		Version: channelDeletionEvidenceDigestVersion, IntentID: fence.IntentID,
		KeyClaimID: claim.ID, ClaimEpoch: claim.ClaimEpoch, ClaimDigest: claim.ClaimDigest,
		Status: status, Reason: reason, OwnershipDigest: ownershipDigest,
		Fingerprint: fingerprint, InventoryDigest: inventoryDigest,
		ObservedRemoteID: claim.ObservedRemoteID,
	})
	if err != nil {
		return err
	}
	outcome := storage.ChannelDeletionKeyOutcome{
		Status: status, EvidenceDigest: evidenceDigest, EvidenceDigestVersion: channelDeletionEvidenceDigestVersion,
		ObservedRemoteID: claim.ObservedRemoteID,
		OwnershipDigest:  ownershipDigest, OwnershipDigestVersion: channelDeletionOwnershipDigestVersion,
		ObservedFingerprint: fingerprint, InventoryDigest: inventoryDigest,
		Reason: reason, Actor: channelDeletionCoordinatorActor, ErrorSummary: errorSummary,
		ObservedAt: time.Now().UTC(),
	}
	if fingerprint != "" {
		outcome.FingerprintDigestVersion = channelDeletionFingerprintVersion
	}
	if inventoryDigest != "" {
		outcome.InventoryDigestVersion = channelDeletionInventoryVersion
	}
	commitCtx := ctx
	var canceled error
	if status == storage.ChannelDeletionKeyProcessingUnknown && ctx != nil {
		canceled = ctx.Err()
		var cancel context.CancelFunc
		commitCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), channelDeletionOutcomeCommitTimeout)
		defer cancel()
	}
	err = s.Channels.MarkChannelDeletionKeyOutcome(commitCtx, storage.ChannelDeletionKeyClaimFence{
		ChannelDeletionFence: fence, KeyClaimID: claim.ID, ClaimEpoch: claim.ClaimEpoch,
	}, outcome)
	if canceled == nil && ctx != nil {
		canceled = ctx.Err()
	}
	if canceled != nil {
		if err != nil {
			return errors.Join(canceled, err)
		}
		return canceled
	}
	return err
}
