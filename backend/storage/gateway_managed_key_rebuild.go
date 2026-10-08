package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	GatewayManagedKeyRebuildPendingError = "managed upstream key rebuild pending"

	managedKeyRebuildClaimDigestVersion     uint16 = 2
	managedKeyRebuildEvidenceDigestVersion  uint16 = 3
	managedKeyRebuildOwnershipDigestVersion uint16 = 1
	managedKeyRebuildMaxClaimEpoch                 = uint64(1<<63 - 1)

	managedKeyRebuildReviewActorBegin       = "rebuild_begin"
	managedKeyRebuildReviewActorReconcile   = "rebuild_reconcile"
	managedKeyRebuildReviewResolutionFacts  = "facts_revalidated"
	managedKeyRebuildReviewResolutionAbsent = "remote_absence_revalidated"
)

var ErrManagedUpstreamKeyRebuildClaimLost = errors.New("managed upstream key rebuild claim lease was lost")

// BeginGenerationSafeManagedUpstreamKeyRebuild persists an executable intent
// for a remote key that belongs to the exact currently-present group
// generation. The local ownership claim is recomputed under row locks before
// the intent is created or reclaimed.
func (g *Gateway) BeginGenerationSafeManagedUpstreamKeyRebuild(expected UpstreamGroupGeneration, channelIdentityRevision uint64, local GatewayUpstreamKey, upstreamKeyID int64, upstreamKeyName, evidenceDigest, ownershipClaim string, observedAt time.Time, staleAfter time.Duration) (*GatewayManagedKeyRebuildIntent, bool, error) {
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	if !expected.Valid() || channelIdentityRevision == 0 || local.ID == 0 || local.ChannelID != expected.ChannelID || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) {
		return nil, false, ErrManagedUpstreamKeyRemovalIntentChanged
	}
	canonical, err := canonicalUpstreamGroupName(g.db, local.GroupName)
	if err != nil || canonical != expected.CanonicalGroupName {
		return nil, false, ErrUpstreamGroupGenerationChanged
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	claimKey := managedUpstreamKeyRebuildClaimKey(expected, channelIdentityRevision, local.ID, upstreamKeyID, upstreamKeyName)
	for attempt := 0; attempt < 3; attempt++ {
		intent := GatewayManagedKeyRebuildIntent{
			ChannelID: expected.ChannelID, ChannelIdentityRevision: channelIdentityRevision, UpstreamGroupID: expected.ID,
			PresenceGeneration: expected.PresenceGeneration, CanonicalGroupName: expected.CanonicalGroupName,
			LocalKeyID: local.ID, UpstreamKeyID: upstreamKeyID, UpstreamKeyName: upstreamKeyName,
			ClaimKey: claimKey, ClaimEpoch: 1, ClaimDigestVersion: managedKeyRebuildClaimDigestVersion,
			EvidenceDigest: evidenceDigest, EvidenceDigestVersion: managedKeyRebuildEvidenceDigestVersion,
			OwnershipClaim: ownershipClaim, OwnershipDigestVersion: managedKeyRebuildOwnershipDigestVersion,
			Status: "processing", DetectedAt: observedAt,
		}
		inserted := false
		err := g.db.Transaction(func(tx *gorm.DB) error {
			if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
				return err
			}
			if err := requireExecutableManagedKeyRebuildOwnershipTx(tx, expected, local.ID, upstreamKeyID, upstreamKeyName, ownershipClaim, observedAt); err != nil {
				return err
			}
			result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&intent)
			inserted = result.RowsAffected == 1
			return result.Error
		})
		if err != nil {
			return nil, false, err
		}
		if inserted {
			return &intent, true, nil
		}

		holder, err := g.findManagedKeyRebuildIntentHolder(expected, channelIdentityRevision, local.ID, upstreamKeyID, upstreamKeyName, claimKey)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			return nil, false, err
		}
		if !managedKeyRebuildIntentIdentityMatches(*holder, expected, channelIdentityRevision, local.ID, upstreamKeyID, upstreamKeyName) {
			return holder, false, ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if !managedKeyRebuildIntentMatches(*holder, expected, channelIdentityRevision, local.ID, upstreamKeyID, upstreamKeyName, evidenceDigest, ownershipClaim) {
			if holder.Status != "manual_review" {
				reason := "rebuild_evidence_changed"
				if !managedKeyRebuildDigestVersionsSupported(*holder) {
					reason = "unsupported_digest_version"
				}
				if err := g.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(
					expected, channelIdentityRevision, holder.ID, holder.ClaimEpoch, reason, managedKeyRebuildReviewActorBegin, observedAt,
				); err != nil {
					return nil, false, err
				}
				if err := g.db.First(holder, holder.ID).Error; err != nil {
					return nil, false, err
				}
			}
			return holder, false, nil
		}
		switch holder.Status {
		case "processing":
			lastClaimedAt := holder.UpdatedAt
			if lastClaimedAt.IsZero() {
				lastClaimedAt = holder.CreatedAt
			}
			if staleAfter <= 0 || lastClaimedAt.Add(staleAfter).After(observedAt) {
				return holder, false, nil
			}
			if holder.ClaimEpoch == 0 || holder.ClaimEpoch >= managedKeyRebuildMaxClaimEpoch {
				return nil, false, ErrManagedUpstreamKeyRebuildClaimLost
			}
			reclaimed := false
			err := g.db.Transaction(func(tx *gorm.DB) error {
				if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
					return err
				}
				if err := requireExecutableManagedKeyRebuildOwnershipTx(tx, expected, local.ID, upstreamKeyID, upstreamKeyName, ownershipClaim, observedAt); err != nil {
					return err
				}
				claimed := tx.Model(&GatewayManagedKeyRebuildIntent{}).
					Where("id = ? AND status = ? AND claim_key = ? AND channel_identity_revision = ? AND claim_epoch = ? AND updated_at <= ?",
						holder.ID, "processing", claimKey, channelIdentityRevision, holder.ClaimEpoch, lastClaimedAt).
					Updates(map[string]any{"claim_epoch": holder.ClaimEpoch + 1, "updated_at": observedAt})
				reclaimed = claimed.RowsAffected == 1
				return claimed.Error
			})
			if err != nil {
				return nil, false, err
			}
			if reclaimed {
				holder.ClaimEpoch++
				holder.UpdatedAt = observedAt
				return holder, true, nil
			}
			continue
		case "acknowledged":
			return holder, false, ErrManagedUpstreamKeyRemovalIntentChanged
		case "manual_review":
			return holder, false, nil
		default:
			return holder, false, ErrManagedUpstreamKeyRemovalIntentChanged
		}
	}
	return nil, false, errors.New("managed upstream key rebuild claim changed concurrently")
}

// FinalizeGenerationSafeManagedUpstreamKeyRebuild holds the present
// generation, local ownership row, and durable intent while the DB-free remote
// callback runs. Callback or commit uncertainty leaves the earlier intent in
// processing for full-inventory reconciliation.
func (g *Gateway) FinalizeGenerationSafeManagedUpstreamKeyRebuild(expected UpstreamGroupGeneration, channelIdentityRevision uint64, intentID uint, claimEpoch uint64, local GatewayUpstreamKey, upstreamKeyID int64, upstreamKeyName, evidenceDigest, ownershipClaim string, at time.Time, remoteMutation func() error) error {
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	if !expected.Valid() || channelIdentityRevision == 0 || intentID == 0 || claimEpoch == 0 || local.ID == 0 || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		// The channel row is the account-identity fence. Holding it through the
		// DB-free callback prevents any identity revision writer from switching
		// the connector to another upstream account before DELETE is acknowledged.
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			return err
		}
		if err := requireExecutableManagedKeyRebuildOwnershipTx(tx, expected, local.ID, upstreamKeyID, upstreamKeyName, ownershipClaim, at); err != nil {
			return err
		}
		var intent GatewayManagedKeyRebuildIntent
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", intentID).First(&intent).Error; err != nil {
			return err
		}
		if !managedKeyRebuildIntentMatches(intent, expected, channelIdentityRevision, local.ID, upstreamKeyID, upstreamKeyName, evidenceDigest, ownershipClaim) {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if intent.ClaimEpoch != claimEpoch {
			return ErrManagedUpstreamKeyRebuildClaimLost
		}
		if intent.Status == "acknowledged" {
			return nil
		}
		if intent.Status != "processing" {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if remoteMutation != nil {
			if err := remoteMutation(); err != nil {
				return err
			}
		}
		result := tx.Model(&GatewayManagedKeyRebuildIntent{}).
			Where("id = ? AND status = ? AND claim_key = ? AND channel_identity_revision = ? AND claim_epoch = ?", intent.ID, "processing", intent.ClaimKey, channelIdentityRevision, claimEpoch).
			Updates(map[string]any{"status": "acknowledged", "resolved_at": at, "updated_at": at})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		return nil
	})
}

func (g *Gateway) FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(expected UpstreamGroupGeneration, channelIdentityRevision uint64, intentID uint, claimEpoch uint64, reason, actor string, at time.Time) error {
	reason, actor, err := normalizeManagedKeyRebuildReview(reason, actor)
	if err != nil {
		return err
	}
	if !expected.Valid() || channelIdentityRevision == 0 || intentID == 0 || claimEpoch == 0 {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	generationChanged := false
	identityChanged := false
	err = g.db.Transaction(func(tx *gorm.DB) error {
		if revisionErr := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); errors.Is(revisionErr, ErrStaleChannelAccountIdentityRevision) {
			identityChanged = true
		} else if revisionErr != nil {
			return revisionErr
		}
		if identityChanged {
			return markManagedUpstreamKeyRebuildManualReviewTx(tx, expected, channelIdentityRevision, intentID, claimEpoch, reason, actor, at)
		}
		domain, lockErr := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, at, expected)
		if errors.Is(lockErr, ErrUpstreamGroupGenerationChanged) {
			generationChanged = true
		} else if lockErr != nil && !errors.Is(lockErr, gorm.ErrRecordNotFound) {
			return lockErr
		}
		if err := markManagedUpstreamKeyRebuildManualReviewTx(tx, expected, channelIdentityRevision, intentID, claimEpoch, reason, actor, at); err != nil {
			return err
		}
		if generationChanged {
			return nil
		}
		if domain == nil || domain.CanonicalGroupName != expected.CanonicalGroupName {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if err := tx.Model(&GatewayUpstreamKey{}).
			Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).
			Updates(map[string]any{"enabled": false, "last_error": managedKeyRebuildManualReviewError(reason), "updated_at": at}).Error; err != nil {
			return err
		}
		if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).
			Delete(&GatewayModelCapability{}).Error; err != nil {
			return err
		}
		return tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).
			Delete(&GatewayRoutingFailure{}).Error
	})
	if err != nil {
		return err
	}
	if identityChanged {
		return ErrStaleChannelAccountIdentityRevision
	}
	if generationChanged {
		return ErrUpstreamGroupGenerationChanged
	}
	return nil
}

// ResolveGenerationSafeManagedUpstreamKeyRebuildManualReview is the audited
// recovery edge for an intent whose current remote facts were revalidated by
// the caller. It atomically restores the local rebuild-pending marker, rotates
// the claim epoch, and refreshes all digest algorithms before work can resume.
func (g *Gateway) ResolveGenerationSafeManagedUpstreamKeyRebuildManualReview(expected UpstreamGroupGeneration, channelIdentityRevision uint64, intentID uint, local GatewayUpstreamKey, upstreamKeyID int64, upstreamKeyName, evidenceDigest, ownershipClaim, actor, resolution string, at time.Time) (*GatewayManagedKeyRebuildIntent, error) {
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	actor = strings.TrimSpace(actor)
	resolution = strings.TrimSpace(resolution)
	if !expected.Valid() || channelIdentityRevision == 0 || intentID == 0 || local.ID == 0 || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) || actor == "" || len(actor) > 128 ||
		(resolution != managedKeyRebuildReviewResolutionFacts && resolution != managedKeyRebuildReviewResolutionAbsent) {
		return nil, ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	var resolved GatewayManagedKeyRebuildIntent
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			return err
		}
		current, err := requirePresentManagedKeyOwnershipTx(tx, expected, local.ID, upstreamKeyID, upstreamKeyName, ownershipClaim, at)
		if err != nil {
			return err
		}
		if current.Enabled {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", intentID).First(&resolved).Error; err != nil {
			return err
		}
		if resolved.Status != "manual_review" || resolved.ClaimEpoch == 0 || resolved.ClaimEpoch >= managedKeyRebuildMaxClaimEpoch ||
			!managedKeyRebuildIntentIdentityMatches(resolved, expected, channelIdentityRevision, local.ID, upstreamKeyID, upstreamKeyName) ||
			strings.TrimSpace(resolved.ManualReviewReason) == "" || strings.TrimSpace(resolved.ManualReviewActor) == "" || resolved.ManualReviewedAt == nil {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}

		if err := tx.Model(&GatewayUpstreamKey{}).Where("id = ? AND enabled = ?", current.ID, false).Updates(map[string]any{
			"last_error": GatewayManagedKeyRebuildPendingError, "updated_at": at,
		}).Error; err != nil {
			return err
		}
		claimKey := managedUpstreamKeyRebuildClaimKey(expected, channelIdentityRevision, local.ID, upstreamKeyID, upstreamKeyName)
		result := tx.Model(&GatewayManagedKeyRebuildIntent{}).
			Where("id = ? AND status = ? AND claim_epoch = ?", resolved.ID, "manual_review", resolved.ClaimEpoch).
			Updates(map[string]any{
				"status": "processing", "claim_key": claimKey, "claim_epoch": resolved.ClaimEpoch + 1,
				"claim_digest_version": managedKeyRebuildClaimDigestVersion,
				"evidence_digest":      evidenceDigest, "evidence_digest_version": managedKeyRebuildEvidenceDigestVersion,
				"ownership_claim": ownershipClaim, "ownership_digest_version": managedKeyRebuildOwnershipDigestVersion,
				"manual_review_resolution": resolution, "manual_review_resolved_by": actor,
				"manual_review_resolved_at": at, "resolved_at": nil, "updated_at": at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrManagedUpstreamKeyRebuildClaimLost
		}
		resolved.Status = "processing"
		resolved.ClaimKey = claimKey
		resolved.ClaimEpoch++
		resolved.ClaimDigestVersion = managedKeyRebuildClaimDigestVersion
		resolved.EvidenceDigest = evidenceDigest
		resolved.EvidenceDigestVersion = managedKeyRebuildEvidenceDigestVersion
		resolved.OwnershipClaim = ownershipClaim
		resolved.OwnershipDigestVersion = managedKeyRebuildOwnershipDigestVersion
		resolved.ManualReviewResolution = resolution
		resolved.ManualReviewResolvedBy = actor
		resolved.ManualReviewResolvedAt = &at
		resolved.ResolvedAt = nil
		resolved.UpdatedAt = at
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &resolved, nil
}

// ReconcileGenerationSafeManagedUpstreamKeyRebuilds consumes a complete,
// duplicate-free remote inventory. An absent ID is acknowledged only when the
// same name did not reappear under another ID.
func (g *Gateway) ReconcileGenerationSafeManagedUpstreamKeyRebuilds(channelID uint, channelIdentityRevision uint64, presentRemote map[int64]string, at time.Time) error {
	if channelID == 0 || channelIdentityRevision == 0 {
		return errors.New("channel id and account identity revision are required")
	}
	if at.IsZero() {
		at = time.Now()
	}
	nameIDs := make(map[string]int64, len(presentRemote))
	for id, rawName := range presentRemote {
		name := strings.TrimSpace(rawName)
		if id <= 0 || name == "" {
			continue
		}
		if previous, exists := nameIDs[name]; exists && previous != id {
			return fmt.Errorf("upstream api key name %q is ambiguous", name)
		}
		nameIDs[name] = id
	}
	if err := g.db.Transaction(func(tx *gorm.DB) error {
		return requireUpstreamAccountIdentityRevisionTx(tx, channelID, channelIdentityRevision)
	}); err != nil {
		return err
	}
	var intents []GatewayManagedKeyRebuildIntent
	if err := g.db.Where("channel_id = ? AND status IN ?", channelID, []string{"processing", "acknowledged", "manual_review"}).Order("id ASC").Find(&intents).Error; err != nil {
		return err
	}
	for _, intent := range intents {
		if intent.ChannelIdentityRevision != channelIdentityRevision {
			if intent.Status == "processing" {
				if err := g.markManagedUpstreamKeyRebuildManualReviewByID(intent.ID, intent.ChannelIdentityRevision, intent.ClaimEpoch, "channel_identity_changed", managedKeyRebuildReviewActorReconcile, at); err != nil {
					return err
				}
			}
			continue
		}
		expected := UpstreamGroupGeneration{
			ID: intent.UpstreamGroupID, ChannelID: intent.ChannelID,
			CanonicalGroupName: intent.CanonicalGroupName, PresenceGeneration: intent.PresenceGeneration,
		}
		valid := expected.Valid() && intent.ChannelIdentityRevision > 0 && intent.LocalKeyID > 0 && intent.UpstreamKeyID > 0 && intent.ClaimEpoch > 0 && strings.TrimSpace(intent.UpstreamKeyName) != "" &&
			validRemovalEvidenceDigest(intent.EvidenceDigest) && validRemovalEvidenceDigest(intent.OwnershipClaim) &&
			managedKeyRebuildDigestVersionsSupported(intent) &&
			intent.ClaimKey == managedUpstreamKeyRebuildClaimKey(expected, channelIdentityRevision, intent.LocalKeyID, intent.UpstreamKeyID, intent.UpstreamKeyName)
		if !valid {
			if intent.Status != "manual_review" {
				if err := g.markManagedUpstreamKeyRebuildManualReviewByID(intent.ID, channelIdentityRevision, intent.ClaimEpoch, "invalid_or_unsupported_intent", managedKeyRebuildReviewActorReconcile, at); err != nil {
					return err
				}
			}
			continue
		}
		if intent.Status == "manual_review" {
			remoteName, remotePresent := presentRemote[intent.UpstreamKeyID]
			nameReappearedID, nameReappeared := nameIDs[strings.TrimSpace(intent.UpstreamKeyName)]
			if remotePresent || (nameReappeared && nameReappearedID != intent.UpstreamKeyID) {
				_ = remoteName
				continue
			}
			local := GatewayUpstreamKey{ID: intent.LocalKeyID, ChannelID: intent.ChannelID, GroupName: intent.CanonicalGroupName}
			resolved, err := g.ResolveGenerationSafeManagedUpstreamKeyRebuildManualReview(
				expected, channelIdentityRevision, intent.ID, local, intent.UpstreamKeyID, intent.UpstreamKeyName,
				intent.EvidenceDigest, intent.OwnershipClaim, managedKeyRebuildReviewActorReconcile, managedKeyRebuildReviewResolutionAbsent, at,
			)
			if err != nil {
				// This audit can outlive either its presence generation or the
				// local row's remote-key binding after a later rebuild. Retain the
				// audit, but do not let it fence recovery of the current identity.
				if errors.Is(err, ErrUpstreamGroupGenerationChanged) ||
					errors.Is(err, ErrManagedUpstreamKeyRemovalIdentityConflict) ||
					errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) ||
					errors.Is(err, ErrManagedUpstreamKeyRebuildClaimLost) {
					continue
				}
				return err
			}
			if err := g.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
				expected, channelIdentityRevision, resolved.ID, resolved.ClaimEpoch, local, resolved.UpstreamKeyID, resolved.UpstreamKeyName,
				resolved.EvidenceDigest, resolved.OwnershipClaim, at, nil,
			); err != nil {
				if errors.Is(err, ErrUpstreamGroupGenerationChanged) {
					continue
				}
				return err
			}
			continue
		}
		remoteName, remotePresent := presentRemote[intent.UpstreamKeyID]
		nameReappearedID, nameReappeared := nameIDs[strings.TrimSpace(intent.UpstreamKeyName)]
		if remotePresent {
			if strings.TrimSpace(remoteName) != strings.TrimSpace(intent.UpstreamKeyName) || intent.Status == "acknowledged" {
				if err := g.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(expected, channelIdentityRevision, intent.ID, intent.ClaimEpoch, "remote_identity_changed", managedKeyRebuildReviewActorReconcile, at); err != nil && !errors.Is(err, ErrUpstreamGroupGenerationChanged) && !errors.Is(err, ErrStaleChannelAccountIdentityRevision) && !errors.Is(err, ErrManagedUpstreamKeyRebuildClaimLost) {
					return err
				}
			}
			continue
		}
		if nameReappeared && nameReappearedID != intent.UpstreamKeyID {
			if err := g.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(expected, channelIdentityRevision, intent.ID, intent.ClaimEpoch, "remote_name_reappeared", managedKeyRebuildReviewActorReconcile, at); err != nil && !errors.Is(err, ErrUpstreamGroupGenerationChanged) && !errors.Is(err, ErrStaleChannelAccountIdentityRevision) && !errors.Is(err, ErrManagedUpstreamKeyRebuildClaimLost) {
				return err
			}
			continue
		}
		if intent.Status == "acknowledged" {
			continue
		}
		local := GatewayUpstreamKey{ID: intent.LocalKeyID, ChannelID: intent.ChannelID, GroupName: intent.CanonicalGroupName}
		err := g.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
			expected, channelIdentityRevision, intent.ID, intent.ClaimEpoch, local, intent.UpstreamKeyID, intent.UpstreamKeyName,
			intent.EvidenceDigest, intent.OwnershipClaim, at, nil,
		)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrUpstreamGroupGenerationChanged) || errors.Is(err, ErrStaleChannelAccountIdentityRevision) || errors.Is(err, ErrManagedUpstreamKeyRemovalIdentityConflict) || errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) || errors.Is(err, ErrManagedUpstreamKeyRebuildClaimLost) {
			if reviewErr := g.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(expected, channelIdentityRevision, intent.ID, intent.ClaimEpoch, "finalize_revalidation_failed", managedKeyRebuildReviewActorReconcile, at); reviewErr != nil && !errors.Is(reviewErr, ErrUpstreamGroupGenerationChanged) && !errors.Is(reviewErr, ErrStaleChannelAccountIdentityRevision) && !errors.Is(reviewErr, ErrManagedUpstreamKeyRebuildClaimLost) {
				return reviewErr
			}
			continue
		}
		return err
	}
	return nil
}

func requirePresentManagedKeyOwnershipTx(tx *gorm.DB, expected UpstreamGroupGeneration, localKeyID uint, upstreamKeyID int64, upstreamKeyName, ownershipClaim string, at time.Time) (*GatewayUpstreamKey, error) {
	domain, err := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, at, expected)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
		}
		return nil, err
	}
	if domain == nil || domain.CanonicalGroupName != expected.CanonicalGroupName {
		return nil, ErrManagedUpstreamKeyRemovalIntentChanged
	}
	current, err := requireManagedKeyRebuildIdentityTx(tx, expected, localKeyID, upstreamKeyID, upstreamKeyName)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	currentClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*current)
	if err != nil || currentClaim != strings.ToLower(strings.TrimSpace(ownershipClaim)) {
		return nil, ErrManagedUpstreamKeyRemovalIntentChanged
	}
	return current, nil
}

// requireManagedKeyRebuildIdentityTx permits case-only duplicate local rows
// while the rebuild is deleting each explicitly bound remote identity. The
// ordinary removal path intentionally rejects such a domain, but rejecting it
// here would disable every local row before canonicalization can safely merge
// them after all remote keys have been handled.
func requireManagedKeyRebuildIdentityTx(tx *gorm.DB, expected UpstreamGroupGeneration, localKeyID uint, upstreamKeyID int64, upstreamKeyName string) (*GatewayUpstreamKey, error) {
	remoteName := strings.TrimSpace(upstreamKeyName)
	if localKeyID == 0 || upstreamKeyID <= 0 || remoteName == "" {
		return nil, ErrManagedUpstreamKeyRemovalIntentChanged
	}
	var current GatewayUpstreamKey
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND channel_id = ?", localKeyID, expected.ChannelID).First(&current).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
		}
		return nil, err
	}
	canonical, err := canonicalUpstreamGroupName(tx, current.GroupName)
	if err != nil || canonical != expected.CanonicalGroupName {
		return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	if current.UpstreamKeyID > 0 && current.UpstreamKeyID != upstreamKeyID {
		return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	if strings.TrimSpace(current.UpstreamKeyName) != remoteName {
		return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
	}

	// A remote ID or exact remote name must not be claimed by another local
	// row, including a row in a different case variant of the same group.
	var aliases []GatewayUpstreamKey
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND (upstream_key_id = ? OR btrim(upstream_key_name) = ?)", expected.ChannelID, upstreamKeyID, remoteName).
		Order("id ASC").Find(&aliases).Error; err != nil {
		return nil, err
	}
	for _, alias := range aliases {
		aliasCanonical, err := canonicalUpstreamGroupName(tx, alias.GroupName)
		if err != nil || aliasCanonical != expected.CanonicalGroupName || alias.ID != current.ID {
			return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
		}
	}
	return &current, nil
}

func requireExecutableManagedKeyRebuildOwnershipTx(tx *gorm.DB, expected UpstreamGroupGeneration, localKeyID uint, upstreamKeyID int64, upstreamKeyName, ownershipClaim string, at time.Time) error {
	current, err := requirePresentManagedKeyOwnershipTx(tx, expected, localKeyID, upstreamKeyID, upstreamKeyName, ownershipClaim, at)
	if err != nil {
		return err
	}
	if current.Enabled || strings.TrimSpace(current.LastError) != GatewayManagedKeyRebuildPendingError {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	return nil
}

func (g *Gateway) markManagedUpstreamKeyRebuildManualReviewByID(intentID uint, channelIdentityRevision, claimEpoch uint64, reason, actor string, at time.Time) error {
	reason, actor, err := normalizeManagedKeyRebuildReview(reason, actor)
	if err != nil {
		return err
	}
	if intentID == 0 || channelIdentityRevision == 0 || claimEpoch == 0 || claimEpoch >= managedKeyRebuildMaxClaimEpoch {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	result := g.db.Model(&GatewayManagedKeyRebuildIntent{}).
		Where("id = ? AND channel_identity_revision = ? AND claim_epoch = ? AND status IN ?", intentID, channelIdentityRevision, claimEpoch, []string{"processing", "acknowledged"}).
		Updates(managedKeyRebuildManualReviewUpdates(claimEpoch, reason, actor, at))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrManagedUpstreamKeyRebuildClaimLost
	}
	return nil
}

func markManagedUpstreamKeyRebuildManualReviewTx(tx *gorm.DB, expected UpstreamGroupGeneration, channelIdentityRevision uint64, intentID uint, claimEpoch uint64, reason, actor string, at time.Time) error {
	if claimEpoch == 0 || claimEpoch >= managedKeyRebuildMaxClaimEpoch {
		return ErrManagedUpstreamKeyRebuildClaimLost
	}
	result := tx.Model(&GatewayManagedKeyRebuildIntent{}).
		Where("id = ? AND channel_id = ? AND channel_identity_revision = ? AND upstream_group_id = ? AND presence_generation = ? AND claim_epoch = ? AND status IN ?",
			intentID, expected.ChannelID, channelIdentityRevision, expected.ID, expected.PresenceGeneration, claimEpoch, []string{"processing", "acknowledged"}).
		Updates(managedKeyRebuildManualReviewUpdates(claimEpoch, reason, actor, at))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrManagedUpstreamKeyRebuildClaimLost
	}
	return nil
}

func managedKeyRebuildManualReviewUpdates(claimEpoch uint64, reason, actor string, at time.Time) map[string]any {
	return map[string]any{
		"status": "manual_review", "claim_epoch": claimEpoch + 1,
		"manual_review_reason": reason, "manual_review_actor": actor, "manual_reviewed_at": at,
		"manual_review_resolution": "", "manual_review_resolved_by": "", "manual_review_resolved_at": nil,
		"resolved_at": nil, "updated_at": at,
	}
}

func normalizeManagedKeyRebuildReview(reason, actor string) (string, string, error) {
	reason = strings.TrimSpace(reason)
	actor = strings.TrimSpace(actor)
	if reason == "" || len(reason) > 64 || actor == "" || len(actor) > 128 {
		return "", "", ErrManagedUpstreamKeyRemovalIntentChanged
	}
	return reason, actor, nil
}

func managedKeyRebuildManualReviewError(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return "managed upstream key rebuild requires manual review"
	}
	return "managed upstream key rebuild requires manual review: " + reason
}

func managedUpstreamKeyRebuildClaimKey(expected UpstreamGroupGeneration, channelIdentityRevision uint64, localKeyID uint, upstreamKeyID int64, upstreamKeyName string) string {
	raw := fmt.Sprintf("managed-key-rebuild:v2:%d:%d:%d:%d:%d:%d:%s", expected.ChannelID, channelIdentityRevision, expected.ID, expected.PresenceGeneration, localKeyID, upstreamKeyID, strings.TrimSpace(upstreamKeyName))
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func (g *Gateway) findManagedKeyRebuildIntentHolder(expected UpstreamGroupGeneration, channelIdentityRevision uint64, localKeyID uint, upstreamKeyID int64, upstreamKeyName, claimKey string) (*GatewayManagedKeyRebuildIntent, error) {
	var holder GatewayManagedKeyRebuildIntent
	err := g.db.Where("claim_key = ?", claimKey).First(&holder).Error
	if err == nil {
		return &holder, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	err = g.db.Where(
		"channel_id = ? AND channel_identity_revision = ? AND upstream_group_id = ? AND presence_generation = ? AND local_key_id = ? AND upstream_key_id = ? AND upstream_key_name = ?",
		expected.ChannelID, channelIdentityRevision, expected.ID, expected.PresenceGeneration, localKeyID, upstreamKeyID, strings.TrimSpace(upstreamKeyName),
	).First(&holder).Error
	if err != nil {
		return nil, err
	}
	return &holder, nil
}

func managedKeyRebuildDigestVersionsSupported(intent GatewayManagedKeyRebuildIntent) bool {
	return intent.ClaimDigestVersion == managedKeyRebuildClaimDigestVersion &&
		intent.EvidenceDigestVersion == managedKeyRebuildEvidenceDigestVersion &&
		intent.OwnershipDigestVersion == managedKeyRebuildOwnershipDigestVersion
}

func managedKeyRebuildIntentIdentityMatches(intent GatewayManagedKeyRebuildIntent, expected UpstreamGroupGeneration, channelIdentityRevision uint64, localKeyID uint, upstreamKeyID int64, upstreamKeyName string) bool {
	return intent.ChannelID == expected.ChannelID && intent.ChannelIdentityRevision == channelIdentityRevision && intent.UpstreamGroupID == expected.ID &&
		intent.PresenceGeneration == expected.PresenceGeneration && intent.CanonicalGroupName == expected.CanonicalGroupName &&
		intent.LocalKeyID == localKeyID && intent.UpstreamKeyID == upstreamKeyID &&
		strings.TrimSpace(intent.UpstreamKeyName) == strings.TrimSpace(upstreamKeyName)
}

func managedKeyRebuildIntentMatches(intent GatewayManagedKeyRebuildIntent, expected UpstreamGroupGeneration, channelIdentityRevision uint64, localKeyID uint, upstreamKeyID int64, upstreamKeyName, evidenceDigest, ownershipClaim string) bool {
	return managedKeyRebuildIntentIdentityMatches(intent, expected, channelIdentityRevision, localKeyID, upstreamKeyID, upstreamKeyName) &&
		managedKeyRebuildDigestVersionsSupported(intent) &&
		strings.EqualFold(strings.TrimSpace(intent.EvidenceDigest), strings.TrimSpace(evidenceDigest)) &&
		strings.EqualFold(strings.TrimSpace(intent.OwnershipClaim), strings.TrimSpace(ownershipClaim)) &&
		intent.ClaimKey == managedUpstreamKeyRebuildClaimKey(expected, channelIdentityRevision, localKeyID, upstreamKeyID, upstreamKeyName)
}
