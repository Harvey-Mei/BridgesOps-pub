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
	managedUpstreamKeyRemovalManualReview           = "manual_review_required"
	managedUpstreamKeyRemovalClaimDigestVersion     = uint16(3)
	managedUpstreamKeyRemovalEvidenceDigestVersion  = uint16(3)
	managedUpstreamKeyRemovalOwnershipDigestVersion = uint16(1)
	managedUpstreamKeyRemovalSystemActor            = "system"
)

const managedUpstreamKeyRemovalPendingReconciliation = "managed upstream key removal pending reconciliation"

var (
	ErrManagedUpstreamKeyRemovalIdentityConflict  = errors.New("managed upstream key removal identity conflicts with local routing state")
	ErrManagedUpstreamKeyRemovalIntentChanged     = errors.New("managed upstream key removal intent changed")
	ErrLegacyManagedUpstreamKeyRemovalUnsupported = errors.New("legacy managed upstream key removal is unsupported")
)

// RetireUpstreamGroupForKeyRemoval makes the current presence generation
// absent before any remote key mutation is attempted. Callers also hold the
// per-channel advisory lock while using the returned generation.
func (g *Gateway) RetireUpstreamGroupForKeyRemoval(channelID uint, channelIdentityRevision uint64, groupName string, at time.Time, preserveCandidates ...bool) (UpstreamGroupGeneration, error) {
	groupName = strings.TrimSpace(groupName)
	if channelID == 0 || channelIdentityRevision == 0 || groupName == "" {
		return UpstreamGroupGeneration{}, errors.New("invalid upstream group removal")
	}
	if at.IsZero() {
		at = time.Now()
	}
	var generation UpstreamGroupGeneration
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, channelID, channelIdentityRevision); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonicalGroupName, err := lockRateSnapshotPresenceDomainTx(tx, channelID, groupName)
		if err != nil {
			return err
		}
		present, err := canonicalRateSnapshotExistsTx(tx, channelID, canonicalGroupName)
		if err != nil {
			return err
		}
		if present {
			return ErrUpstreamGroupGenerationChanged
		}
		identity, _, err := ensureUpstreamGroupIdentityTx(tx, channelID, groupName, false, at)
		if err != nil {
			return err
		}
		if identity.Present || identity.CanonicalGroupName != canonicalGroupName {
			return ErrUpstreamGroupGenerationChanged
		}
		retire := retireUpstreamGroupGenerationTx
		if len(preserveCandidates) > 0 && preserveCandidates[0] {
			// Rebuilds preserve administrator-owned candidate configuration,
			// but an automatic claim belongs only to the generation that
			// produced it and must never follow a same-named replacement.
			retire = retireAutomaticUpstreamGroupGenerationTx
		}
		if err := retire(tx, identity, at); err != nil {
			return err
		}
		generation = upstreamGroupGeneration(*identity)
		return nil
	})
	return generation, err
}

// RequireAbsentUpstreamGroupGeneration rejects an intent as soon as the same
// identity reappears or advances to another presence generation.
func (g *Gateway) RequireAbsentUpstreamGroupGeneration(expected UpstreamGroupGeneration) error {
	if !expected.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	var current GatewayUpstreamGroup
	err := g.db.Select("id", "channel_id", "canonical_group_name", "presence_generation", "present").
		Where("id = ?", expected.ID).First(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrUpstreamGroupGenerationChanged
	}
	if err != nil {
		return err
	}
	if current.Present || current.ChannelID != expected.ChannelID ||
		current.CanonicalGroupName != expected.CanonicalGroupName ||
		current.PresenceGeneration != expected.PresenceGeneration {
		return ErrUpstreamGroupGenerationChanged
	}
	return nil
}

// BeginGenerationSafeManagedUpstreamKeyRemoval persists the exact absent
// generation and evidence digest before the remote DELETE. An existing legacy
// or mismatched claim is never promoted into an executable intent.
func (g *Gateway) BeginGenerationSafeManagedUpstreamKeyRemoval(expected UpstreamGroupGeneration, channelIdentityRevision uint64, groupName string, upstreamKeyID int64, upstreamKeyName, evidenceDigest, ownershipClaim string, observedAt time.Time, staleAfter time.Duration) (*GatewayUpstreamGroupChange, bool, error) {
	return g.beginGenerationSafeManagedUpstreamKeyRemoval(expected, channelIdentityRevision, groupName, upstreamKeyID, upstreamKeyName, false, evidenceDigest, ownershipClaim, observedAt, staleAfter, false)
}

// BeginGenerationSafeManagedUpstreamKeyInventoryRemoval persists a durable
// intent for one exact remote inventory identity. Unlike the legacy removal
// API, it does not require a local GatewayUpstreamKey row and accepts either
// the current (present=true) or retired (present=false) group generation.
// Duplicate names are therefore represented by their remote IDs, not by a
// local name/ID binding.
func (g *Gateway) BeginGenerationSafeManagedUpstreamKeyInventoryRemoval(expected UpstreamGroupGeneration, channelIdentityRevision uint64, groupName string, upstreamKeyID int64, upstreamKeyName string, present bool, evidenceDigest, ownershipClaim string, observedAt time.Time, staleAfter time.Duration) (*GatewayUpstreamGroupChange, bool, error) {
	return g.beginGenerationSafeManagedUpstreamKeyRemoval(expected, channelIdentityRevision, groupName, upstreamKeyID, upstreamKeyName, present, evidenceDigest, ownershipClaim, observedAt, staleAfter, true)
}

func (g *Gateway) beginGenerationSafeManagedUpstreamKeyRemoval(expected UpstreamGroupGeneration, channelIdentityRevision uint64, groupName string, upstreamKeyID int64, upstreamKeyName string, present bool, evidenceDigest, ownershipClaim string, observedAt time.Time, staleAfter time.Duration, inventory bool) (*GatewayUpstreamGroupChange, bool, error) {
	groupName = strings.TrimSpace(groupName)
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	if !expected.Valid() || channelIdentityRevision == 0 || groupName == "" || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) {
		return nil, false, errors.New("invalid generation-safe managed upstream key removal")
	}
	canonical, err := canonicalUpstreamGroupName(g.db, groupName)
	if err != nil {
		return nil, false, err
	}
	if expected.ChannelID == 0 || canonical != expected.CanonicalGroupName {
		return nil, false, ErrUpstreamGroupGenerationChanged
	}
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	claimKey := managedUpstreamKeyRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName)
	if inventory {
		claimKey = managedUpstreamKeyInventoryRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, present)
	}
	for attempt := 0; attempt < 3; attempt++ {
		change := GatewayUpstreamGroupChange{
			ChannelID:       expected.ChannelID,
			GroupName:       groupName,
			UpstreamGroupID: uintPointer(expected.ID),
			PresenceGeneration: uint64Pointer(
				expected.PresenceGeneration,
			),
			Kind:                  "removed",
			Status:                "processing",
			UpstreamKeyID:         upstreamKeyID,
			UpstreamKeyName:       upstreamKeyName,
			RemovalClaimKey:       &claimKey,
			RemovalOwnershipClaim: ownershipClaim,
			RemovalChannelIdentityRevision: uint64Pointer(
				channelIdentityRevision,
			),
			RemovalClaimEpoch:             uint64Pointer(1),
			RemovalClaimDigestVersion:     uint16Pointer(managedUpstreamKeyRemovalClaimDigestVersion),
			RemovalEvidenceDigestVersion:  uint16Pointer(managedUpstreamKeyRemovalEvidenceDigestVersion),
			RemovalOwnershipDigestVersion: uint16Pointer(managedUpstreamKeyRemovalOwnershipDigestVersion),
			FactsDigest:                   evidenceDigest,
			DetectedAt:                    observedAt,
		}
		inserted := false
		created := g.db.Transaction(func(tx *gorm.DB) error {
			if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
				return err
			}
			if err := requireUpstreamGroupGenerationStateTx(tx, expected, present); err != nil {
				return err
			}
			result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&change)
			inserted = result.RowsAffected == 1
			return result.Error
		})
		if created != nil {
			return nil, false, created
		}
		if inserted {
			return &change, true, nil
		}
		if err := g.RequireUpstreamAccountIdentityRevision(expected.ChannelID, channelIdentityRevision); err != nil {
			return nil, false, err
		}

		var holder GatewayUpstreamGroupChange
		if err := g.db.Where("removal_claim_key = ?", claimKey).First(&holder).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			return nil, false, err
		}
		if !managedRemovalIntentMatches(holder, expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, evidenceDigest, ownershipClaim) {
			if err := g.markManagedUpstreamKeyRemovalManualReviewByID(holder.ID, "removal_evidence_changed", observedAt); err != nil {
				return nil, false, err
			}
			holder.Status = "ignored"
			holder.AssignmentState = "ignored"
			holder.AssignmentReasonCode = managedUpstreamKeyRemovalManualReview
			return &holder, false, nil
		}
		switch holder.Status {
		case "processing":
			if holder.RemovalClaimEpoch == nil || *holder.RemovalClaimEpoch == 0 || *holder.RemovalClaimEpoch >= maxUpstreamAccountIdentityRevision {
				if err := g.markManagedUpstreamKeyRemovalManualReviewByID(holder.ID, "removal_claim_epoch_invalid", observedAt); err != nil {
					return nil, false, err
				}
				return &holder, false, nil
			}
			oldClaimEpoch := *holder.RemovalClaimEpoch
			nextClaimEpoch := oldClaimEpoch + 1
			lastClaimedAt := holder.UpdatedAt
			if lastClaimedAt.IsZero() {
				lastClaimedAt = holder.CreatedAt
			}
			if staleAfter <= 0 || lastClaimedAt.Add(staleAfter).After(observedAt) {
				return &holder, false, nil
			}
			claimed := false
			claimErr := g.db.Transaction(func(tx *gorm.DB) error {
				if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
					return err
				}
				if err := requireUpstreamGroupGenerationStateTx(tx, expected, present); err != nil {
					return err
				}
				result := tx.Model(&GatewayUpstreamGroupChange{}).
					Where("id = ? AND status = ? AND removal_claim_key = ? AND removal_channel_identity_revision = ? AND removal_claim_epoch = ? AND updated_at <= ?", holder.ID, "processing", claimKey, channelIdentityRevision, oldClaimEpoch, lastClaimedAt).
					Updates(map[string]any{"removal_claim_epoch": nextClaimEpoch, "updated_at": observedAt, "facts_digest": evidenceDigest})
				if result.Error != nil {
					return result.Error
				}
				claimed = result.RowsAffected == 1
				return nil
			})
			if claimErr != nil {
				return nil, false, claimErr
			}
			if claimed {
				holder.UpdatedAt = observedAt
				holder.RemovalClaimEpoch = uint64Pointer(nextClaimEpoch)
				return &holder, true, nil
			}
			continue
		case "acknowledged":
			if err := g.markManagedUpstreamKeyRemovalManualReviewByID(holder.ID, "remote_identity_reappeared", observedAt); err != nil {
				return nil, false, err
			}
			holder.Status = "ignored"
			holder.AssignmentState = "ignored"
			holder.AssignmentReasonCode = managedUpstreamKeyRemovalManualReview
			return &holder, false, nil
		case "ignored":
			return &holder, false, nil
		default:
			if err := g.markManagedUpstreamKeyRemovalManualReviewByID(holder.ID, "removal_intent_state_changed", observedAt); err != nil {
				return nil, false, err
			}
			return &holder, false, nil
		}
	}
	return nil, false, errors.New("managed upstream key removal claim changed concurrently")
}

// RecordManagedUpstreamKeyRemovalManualReview records an uncertain key once
// for the exact absent generation. Its claim remains occupied so periodic
// synchronization cannot repeatedly attempt or silently upgrade it.
func (g *Gateway) RecordManagedUpstreamKeyRemovalManualReview(expected UpstreamGroupGeneration, channelIdentityRevision uint64, groupName string, upstreamKeyID int64, upstreamKeyName, reviewReason string, at time.Time) error {
	groupName = strings.TrimSpace(groupName)
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	reviewReason = normalizeManagedUpstreamKeyRemovalReviewReason(reviewReason)
	if !expected.Valid() || channelIdentityRevision == 0 || groupName == "" || upstreamKeyID < 0 || (upstreamKeyID == 0 && upstreamKeyName == "") {
		return errors.New("invalid managed upstream key manual review")
	}
	if at.IsZero() {
		at = time.Now()
	}
	claimKey := managedUpstreamKeyRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName)
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			return err
		}
		if err := requireAbsentUpstreamGroupGenerationTx(tx, expected); err != nil {
			return err
		}
		change := GatewayUpstreamGroupChange{
			ChannelID:          expected.ChannelID,
			GroupName:          groupName,
			UpstreamGroupID:    uintPointer(expected.ID),
			PresenceGeneration: uint64Pointer(expected.PresenceGeneration),
			Kind:               "removed",
			Status:             "ignored",
			UpstreamKeyID:      upstreamKeyID,
			UpstreamKeyName:    upstreamKeyName,
			RemovalClaimKey:    &claimKey,
			RemovalChannelIdentityRevision: uint64Pointer(
				channelIdentityRevision,
			),
			RemovalClaimEpoch:             uint64Pointer(1),
			RemovalClaimDigestVersion:     uint16Pointer(managedUpstreamKeyRemovalClaimDigestVersion),
			RemovalEvidenceDigestVersion:  uint16Pointer(managedUpstreamKeyRemovalEvidenceDigestVersion),
			RemovalOwnershipDigestVersion: uint16Pointer(managedUpstreamKeyRemovalOwnershipDigestVersion),
			RemovalManualReviewReason:     reviewReason,
			RemovalManualReviewActor:      managedUpstreamKeyRemovalSystemActor,
			RemovalManualReviewedAt:       &at,
			AssignmentState:               "ignored",
			AssignmentReasonCode:          managedUpstreamKeyRemovalManualReview,
			FactsDigest:                   "",
			DetectedAt:                    at,
			EvaluatedAt:                   &at,
		}
		created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&change)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 1 {
			return tx.Model(&GatewayUpstreamKey{}).
				Where("channel_id = ? AND lower(btrim(group_name)) = ? AND upstream_key_id = ? AND btrim(upstream_key_name) = ?", expected.ChannelID, expected.CanonicalGroupName, upstreamKeyID, upstreamKeyName).
				Updates(map[string]any{"enabled": false, "last_error": "managed upstream key removal requires manual review", "updated_at": at}).Error
		}
		var holder GatewayUpstreamGroupChange
		if err := tx.Where("removal_claim_key = ?", claimKey).First(&holder).Error; err != nil {
			return err
		}
		if err := tx.Model(&GatewayUpstreamKey{}).
			Where("channel_id = ? AND lower(btrim(group_name)) = ? AND upstream_key_id = ? AND btrim(upstream_key_name) = ?", expected.ChannelID, expected.CanonicalGroupName, upstreamKeyID, upstreamKeyName).
			Updates(map[string]any{"enabled": false, "last_error": "managed upstream key removal requires manual review", "updated_at": at}).Error; err != nil {
			return err
		}
		return tx.Model(&GatewayUpstreamGroupChange{}).Where("id = ?", holder.ID).Updates(map[string]any{
			"status":                       "ignored",
			"assignment_state":             "ignored",
			"assignment_reason_code":       managedUpstreamKeyRemovalManualReview,
			"removal_manual_review_reason": reviewReason,
			"removal_manual_review_actor":  managedUpstreamKeyRemovalSystemActor,
			"removal_manual_reviewed_at":   at,
			"resolved_at":                  nil,
			"evaluated_at":                 at,
			"updated_at":                   at,
		}).Error
	})
}

// markManagedUpstreamKeyRemovalManualReviewByID admits the channel write before
// changing the exact observed audit state. A deletion fence or concurrent
// lifecycle transition makes the stale review fail closed.
func (g *Gateway) markManagedUpstreamKeyRemovalManualReviewByID(id uint, reviewReason string, at time.Time) error {
	if g == nil || g.db == nil || id == 0 {
		return errors.New("managed upstream key removal id is required")
	}
	if at.IsZero() {
		at = time.Now()
	}
	reviewReason = normalizeManagedUpstreamKeyRemovalReviewReason(reviewReason)
	var observed GatewayUpstreamGroupChange
	if err := g.db.Select("id", "channel_id", "status").Where("id = ? AND kind = ?", id, "removed").First(&observed).Error; err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if _, err := requireOrdinaryChannelWriteTx(tx, observed.ChannelID); err != nil {
			return err
		}
		result := tx.Model(&GatewayUpstreamGroupChange{}).
			Where("id = ? AND channel_id = ? AND kind = ? AND status = ?", id, observed.ChannelID, "removed", observed.Status).
			Updates(map[string]any{
				"status":                       "ignored",
				"assignment_state":             "ignored",
				"assignment_reason_code":       managedUpstreamKeyRemovalManualReview,
				"removal_manual_review_reason": reviewReason,
				"removal_manual_review_actor":  managedUpstreamKeyRemovalSystemActor,
				"removal_manual_reviewed_at":   at,
				"resolved_at":                  nil,
				"evaluated_at":                 at,
				"updated_at":                   at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		return nil
	})
}

// closeManagedUpstreamKeyRemovalForIdentityChangeTx closes only the durable
// intent that belongs to the account revision which initiated it. Callers use
// this after locking the current channel row and discovering that the account
// changed, so it must never quarantine or purge the new account's local state.
func closeManagedUpstreamKeyRemovalForIdentityChangeTx(tx *gorm.DB, intentID uint, channelIdentityRevision uint64, at time.Time) error {
	if intentID == 0 || channelIdentityRevision == 0 {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	result := tx.Model(&GatewayUpstreamGroupChange{}).
		Where("id = ? AND kind = ? AND removal_channel_identity_revision = ?", intentID, "removed", channelIdentityRevision).
		Updates(map[string]any{
			"status": "ignored", "assignment_state": "ignored",
			"assignment_reason_code":       managedUpstreamKeyRemovalManualReview,
			"removal_manual_review_reason": "channel_identity_changed",
			"removal_manual_review_actor":  managedUpstreamKeyRemovalSystemActor,
			"removal_manual_reviewed_at":   at,
			"resolved_at":                  nil, "evaluated_at": at, "updated_at": at,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	return nil
}

// QuarantineAbsentGenerationManagedUpstreamKeyProcessing makes an unfinished
// remote deletion non-routable without resolving its durable processing
// intent. A later full remote inventory can still acknowledge the same exact
// generation, while a reappeared generation is never modified by this call.
func (g *Gateway) QuarantineAbsentGenerationManagedUpstreamKeyProcessing(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID uint, at time.Time) error {
	if !expected.Valid() || channelIdentityRevision == 0 || claimEpoch == 0 || intentID == 0 {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	identityChanged := false
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			if errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				identityChanged = true
				return closeManagedUpstreamKeyRemovalForIdentityChangeTx(tx, intentID, channelIdentityRevision, at)
			}
			return err
		}
		domain, lockErr := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, at)
		if lockErr != nil && !errors.Is(lockErr, gorm.ErrRecordNotFound) {
			return lockErr
		}
		if domain == nil || domain.CanonicalGroupName != expected.CanonicalGroupName {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if err := requireAbsentUpstreamGroupGenerationTx(tx, expected); err != nil {
			return err
		}

		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND kind = ?", intentID, "removed").First(&change).Error; err != nil {
			return err
		}
		canonical, err := canonicalUpstreamGroupName(tx, change.GroupName)
		if err != nil || canonical != expected.CanonicalGroupName || change.UpstreamGroupID == nil ||
			*change.UpstreamGroupID != expected.ID || change.PresenceGeneration == nil ||
			*change.PresenceGeneration != expected.PresenceGeneration || change.Status != "processing" ||
			change.RemovalChannelIdentityRevision == nil || *change.RemovalChannelIdentityRevision != channelIdentityRevision ||
			change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch != claimEpoch {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}

		if err := tx.Model(&GatewayUpstreamKey{}).
			Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).
			Updates(map[string]any{
				"enabled":    false,
				"last_error": managedUpstreamKeyRemovalPendingReconciliation,
				"updated_at": at,
			}).Error; err != nil {
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
	return nil
}

// FinalizeAbsentGenerationManagedUpstreamKeyManualReview records uncertain
// ownership and quarantines local routing state under the same absent
// generation fence. A reappeared group may close the old audit, but it never
// changes state belonging to the new generation.
func (g *Gateway) FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, groupName string, upstreamKeyID int64, upstreamKeyName string, intentID uint, preserveCandidates bool, at time.Time) error {
	groupName = strings.TrimSpace(groupName)
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	if !expected.Valid() || channelIdentityRevision == 0 || groupName == "" || upstreamKeyID < 0 || (upstreamKeyID == 0 && upstreamKeyName == "") || (intentID != 0 && claimEpoch == 0) {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	generationChanged := false
	identityChanged := false
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			if errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				identityChanged = true
				if intentID == 0 {
					return nil
				}
				return closeManagedUpstreamKeyRemovalForIdentityChangeTx(tx, intentID, channelIdentityRevision, at)
			}
			return err
		}
		domain, lockErr := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, at)
		if lockErr != nil && !errors.Is(lockErr, gorm.ErrRecordNotFound) {
			return lockErr
		}
		if domain == nil || domain.CanonicalGroupName != expected.CanonicalGroupName {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if err := requireAbsentUpstreamGroupGenerationTx(tx, expected); err != nil {
			if errors.Is(err, ErrUpstreamGroupGenerationChanged) {
				generationChanged = true
				if intentID != 0 {
					result := tx.Model(&GatewayUpstreamGroupChange{}).
						Where("id = ? AND kind = ? AND upstream_group_id = ? AND presence_generation = ? AND removal_channel_identity_revision = ? AND removal_claim_epoch = ? AND upstream_key_id = ? AND btrim(upstream_key_name) = ?", intentID, "removed", expected.ID, expected.PresenceGeneration, channelIdentityRevision, claimEpoch, upstreamKeyID, upstreamKeyName).
						Updates(map[string]any{
							"status": "ignored", "assignment_state": "ignored",
							"assignment_reason_code":       managedUpstreamKeyRemovalManualReview,
							"removal_manual_review_reason": "group_generation_changed",
							"removal_manual_review_actor":  managedUpstreamKeyRemovalSystemActor,
							"removal_manual_reviewed_at":   at,
							"resolved_at":                  nil, "evaluated_at": at, "updated_at": at,
						})
					if result.Error != nil {
						return result.Error
					}
					if result.RowsAffected != 1 {
						return ErrManagedUpstreamKeyRemovalIntentChanged
					}
				}
				return nil
			}
			return err
		}
		canonical, err := canonicalUpstreamGroupName(tx, groupName)
		if err != nil || canonical != expected.CanonicalGroupName {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}

		if intentID != 0 {
			result := tx.Model(&GatewayUpstreamGroupChange{}).
				Where("id = ? AND kind = ? AND upstream_group_id = ? AND presence_generation = ? AND removal_channel_identity_revision = ? AND removal_claim_epoch = ? AND upstream_key_id = ? AND btrim(upstream_key_name) = ?", intentID, "removed", expected.ID, expected.PresenceGeneration, channelIdentityRevision, claimEpoch, upstreamKeyID, upstreamKeyName).
				Updates(map[string]any{
					"status": "ignored", "assignment_state": "ignored",
					"assignment_reason_code":       managedUpstreamKeyRemovalManualReview,
					"removal_manual_review_reason": "revalidation_required",
					"removal_manual_review_actor":  managedUpstreamKeyRemovalSystemActor,
					"removal_manual_reviewed_at":   at,
					"resolved_at":                  nil, "evaluated_at": at, "updated_at": at,
				})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrManagedUpstreamKeyRemovalIntentChanged
			}
		} else {
			claimKey := managedUpstreamKeyRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName)
			change := GatewayUpstreamGroupChange{
				ChannelID: expected.ChannelID, GroupName: groupName,
				UpstreamGroupID: uintPointer(expected.ID), PresenceGeneration: uint64Pointer(expected.PresenceGeneration),
				Kind: "removed", Status: "ignored", UpstreamKeyID: upstreamKeyID, UpstreamKeyName: upstreamKeyName,
				RemovalClaimKey: &claimKey, RemovalChannelIdentityRevision: uint64Pointer(channelIdentityRevision),
				RemovalClaimEpoch:             uint64Pointer(1),
				RemovalClaimDigestVersion:     uint16Pointer(managedUpstreamKeyRemovalClaimDigestVersion),
				RemovalEvidenceDigestVersion:  uint16Pointer(managedUpstreamKeyRemovalEvidenceDigestVersion),
				RemovalOwnershipDigestVersion: uint16Pointer(managedUpstreamKeyRemovalOwnershipDigestVersion),
				RemovalManualReviewReason:     "revalidation_required",
				RemovalManualReviewActor:      managedUpstreamKeyRemovalSystemActor,
				RemovalManualReviewedAt:       &at,
				AssignmentState:               "ignored", AssignmentReasonCode: managedUpstreamKeyRemovalManualReview,
				DetectedAt: at, EvaluatedAt: &at,
			}
			created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&change)
			if created.Error != nil {
				return created.Error
			}
			if created.RowsAffected == 0 {
				result := tx.Model(&GatewayUpstreamGroupChange{}).Where("removal_claim_key = ?", claimKey).Updates(map[string]any{
					"status": "ignored", "assignment_state": "ignored",
					"assignment_reason_code":       managedUpstreamKeyRemovalManualReview,
					"removal_manual_review_reason": "revalidation_required",
					"removal_manual_review_actor":  managedUpstreamKeyRemovalSystemActor,
					"removal_manual_reviewed_at":   at,
					"resolved_at":                  nil, "evaluated_at": at, "updated_at": at,
				})
				if result.Error != nil {
					return result.Error
				}
			}
		}

		if !preserveCandidates {
			if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).Delete(&GatewayCandidateGroupMember{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).Updates(map[string]any{
			"enabled": false, "last_error": "managed upstream key removal requires manual review", "updated_at": at,
		}).Error; err != nil {
			return err
		}
		if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).Delete(&GatewayModelCapability{}).Error; err != nil {
			return err
		}
		return tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).Delete(&GatewayRoutingFailure{}).Error
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

// ResumeGenerationSafeManagedUpstreamKeyRemovalManualReview reopens one
// explicitly reviewed intent only after the caller has freshly revalidated the
// remote identity and local ownership. The new epoch fences any worker that
// still holds the pre-review claim.
func (g *Gateway) ResumeGenerationSafeManagedUpstreamKeyRemovalManualReview(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID, localKeyID uint, upstreamKeyID int64, upstreamKeyName, evidenceDigest, ownershipClaim, actor, resolution string, at time.Time) (*GatewayUpstreamGroupChange, error) {
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	actor = normalizeManagedUpstreamKeyRemovalReviewActor(actor)
	resolution = normalizeManagedUpstreamKeyRemovalReviewResolution(resolution)
	if !expected.Valid() || channelIdentityRevision == 0 || claimEpoch == 0 || intentID == 0 || localKeyID == 0 || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) {
		return nil, ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	if claimEpoch >= maxUpstreamAccountIdentityRevision {
		return nil, ErrManagedUpstreamKeyRemovalIntentChanged
	}
	claimKey := managedUpstreamKeyRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName)
	var resumed GatewayUpstreamGroupChange
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			return err
		}
		if err := requireAbsentUpstreamGroupGenerationTx(tx, expected); err != nil {
			return err
		}
		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND kind = ?", intentID, "removed").First(&change).Error; err != nil {
			return err
		}
		if change.Status != "ignored" || change.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview ||
			change.RemovalClaimKey == nil || *change.RemovalClaimKey != claimKey ||
			change.RemovalChannelIdentityRevision == nil || *change.RemovalChannelIdentityRevision != channelIdentityRevision ||
			change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch != claimEpoch ||
			change.RemovalClaimDigestVersion == nil || *change.RemovalClaimDigestVersion != managedUpstreamKeyRemovalClaimDigestVersion ||
			change.RemovalEvidenceDigestVersion == nil || *change.RemovalEvidenceDigestVersion != managedUpstreamKeyRemovalEvidenceDigestVersion ||
			change.RemovalOwnershipDigestVersion == nil || *change.RemovalOwnershipDigestVersion != managedUpstreamKeyRemovalOwnershipDigestVersion ||
			change.UpstreamGroupID == nil || *change.UpstreamGroupID != expected.ID ||
			change.PresenceGeneration == nil || *change.PresenceGeneration != expected.PresenceGeneration ||
			change.UpstreamKeyID != upstreamKeyID || strings.TrimSpace(change.UpstreamKeyName) != upstreamKeyName {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		currentLocal, err := requireExclusiveManagedUpstreamKeyRemovalIdentityTx(tx, expected, change, localKeyID)
		if err != nil {
			return err
		}
		if currentLocal == nil || currentLocal.Enabled {
			return ErrManagedUpstreamKeyRemovalIdentityConflict
		}
		currentClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*currentLocal)
		if err != nil || currentClaim != ownershipClaim {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		nextEpoch := claimEpoch + 1
		result := tx.Model(&GatewayUpstreamGroupChange{}).
			Where("id = ? AND status = ? AND removal_claim_key = ? AND removal_claim_epoch = ?", intentID, "ignored", claimKey, claimEpoch).
			Updates(map[string]any{
				"status":                            "processing",
				"assignment_state":                  "",
				"assignment_reason_code":            "",
				"facts_digest":                      evidenceDigest,
				"removal_ownership_claim":           ownershipClaim,
				"removal_claim_epoch":               nextEpoch,
				"removal_claim_digest_version":      managedUpstreamKeyRemovalClaimDigestVersion,
				"removal_evidence_digest_version":   managedUpstreamKeyRemovalEvidenceDigestVersion,
				"removal_ownership_digest_version":  managedUpstreamKeyRemovalOwnershipDigestVersion,
				"removal_manual_review_resolution":  resolution,
				"removal_manual_review_resolved_by": actor,
				"removal_manual_review_resolved_at": at,
				"resolved_at":                       nil,
				"evaluated_at":                      at,
				"updated_at":                        at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if err := tx.Model(&GatewayUpstreamKey{}).Where("id = ? AND enabled = ?", localKeyID, false).
			Updates(map[string]any{"last_error": managedUpstreamKeyRemovalPendingReconciliation, "updated_at": at}).Error; err != nil {
			return err
		}
		resumed = change
		resumed.Status = "processing"
		resumed.AssignmentState = ""
		resumed.AssignmentReasonCode = ""
		resumed.FactsDigest = evidenceDigest
		resumed.RemovalOwnershipClaim = ownershipClaim
		resumed.RemovalClaimEpoch = uint64Pointer(nextEpoch)
		resumed.RemovalManualReviewResolution = resolution
		resumed.RemovalManualReviewResolvedBy = actor
		resumed.RemovalManualReviewResolvedAt = &at
		resumed.ResolvedAt = nil
		resumed.UpdatedAt = at
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &resumed, nil
}

// FinalizeGenerationSafeManagedUpstreamKeyRemoval holds the absent identity
// row lock across the remote mutation, local purge, and acknowledged audit.
// A callback error or transaction outcome error intentionally leaves the
// previously committed intent processing so a later full remote list can
// recover an uncertain DELETE without issuing an untracked replacement.
func (g *Gateway) FinalizeGenerationSafeManagedUpstreamKeyRemoval(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID, localKeyID uint, upstreamKeyID int64, upstreamKeyName, evidenceDigest, ownershipClaim string, at time.Time, remoteMutation func() error) error {
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	if !expected.Valid() || channelIdentityRevision == 0 || claimEpoch == 0 || intentID == 0 || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	identityChanged := false
	err := g.db.Transaction(func(tx *gorm.DB) error {
		// This row lock is deliberately first. It fences an administrator account
		// switch across the remote DELETE callback and the local acknowledgement.
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			if errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				identityChanged = true
				return closeManagedUpstreamKeyRemovalForIdentityChangeTx(tx, intentID, channelIdentityRevision, at)
			}
			return err
		}
		// Routing-health writers take this advisory lock before their generation
		// row lock. The channel row fence above intentionally precedes both.
		domain, lockErr := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, at)
		if lockErr != nil && !errors.Is(lockErr, gorm.ErrRecordNotFound) {
			return lockErr
		}
		if domain == nil || domain.CanonicalGroupName != expected.CanonicalGroupName {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if err := requireAbsentUpstreamGroupGenerationTx(tx, expected); err != nil {
			return err
		}

		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND kind = ?", intentID, "removed").First(&change).Error; err != nil {
			return err
		}
		canonical, err := canonicalUpstreamGroupName(tx, change.GroupName)
		if err != nil || canonical != expected.CanonicalGroupName ||
			(change.Status != "processing" && change.Status != "acknowledged") ||
			!managedRemovalIntentMatches(change, expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, evidenceDigest, ownershipClaim) ||
			change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch != claimEpoch ||
			change.RemovalClaimKey == nil || *change.RemovalClaimKey != managedUpstreamKeyRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName) {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}

		currentLocal, err := requireExclusiveManagedUpstreamKeyRemovalIdentityTx(tx, expected, change, localKeyID)
		if err != nil {
			return err
		}
		if currentLocal == nil {
			return ErrManagedUpstreamKeyRemovalIdentityConflict
		}
		currentClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*currentLocal)
		if err != nil || currentClaim != ownershipClaim {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}

		if change.Status == "processing" && remoteMutation != nil {
			if err := remoteMutation(); err != nil {
				return err
			}
		}
		if err := purgeManagedUpstreamGroupStateTx(tx, expected); err != nil {
			return err
		}
		claimKey := managedUpstreamKeyRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName)
		result := tx.Model(&GatewayUpstreamGroupChange{}).
			Where("id = ? AND kind = ? AND status IN ? AND removal_claim_key = ? AND removal_claim_epoch = ?", change.ID, "removed", []string{"processing", "acknowledged"}, claimKey, claimEpoch).
			Updates(map[string]any{"status": "acknowledged", "resolved_at": at, "updated_at": at})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		return nil
	})
	if err != nil {
		return err
	}
	if identityChanged {
		return ErrStaleChannelAccountIdentityRevision
	}
	return nil
}

// FinalizeGenerationSafeManagedUpstreamKeyInventoryRemoval fences and applies
// one inventory removal intent. It deliberately has no localKeyID parameter:
// a managed remote key may be duplicated or may have no local GatewayUpstreamKey
// row at all. Absent-group cleanup purges local state by default; callers that
// detect an administrator-owned local key use the KeepLocal variant below.
func (g *Gateway) FinalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID uint, upstreamKeyID int64, upstreamKeyName string, present bool, evidenceDigest, ownershipClaim string, at time.Time, remoteMutation func() error) error {
	return g.finalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(expected, channelIdentityRevision, claimEpoch, intentID, upstreamKeyID, upstreamKeyName, present, !present, evidenceDigest, ownershipClaim, at, remoteMutation)
}

// FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalKeepLocal preserves
// unrelated local routing state while still clearing an exact binding to the
// deleted remote identity under the same deletion fence and audit transaction.
func (g *Gateway) FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalKeepLocal(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID uint, upstreamKeyID int64, upstreamKeyName string, present bool, evidenceDigest, ownershipClaim string, at time.Time, remoteMutation func() error) error {
	return g.finalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(expected, channelIdentityRevision, claimEpoch, intentID, upstreamKeyID, upstreamKeyName, present, false, evidenceDigest, ownershipClaim, at, remoteMutation)
}

// FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalManualReview closes
// the exact claimed inventory intent without mutating the provider. It is used
// when the final pre-DELETE read proves that the remote identity drifted. The
// exact persisted claim remains sufficient audit authority even if the channel
// revision or group generation changed concurrently; those changes must not
// leave an old processing intent permanently open.
func (g *Gateway) FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalManualReview(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID uint, upstreamKeyID int64, upstreamKeyName string, present bool, evidenceDigest, ownershipClaim, reason string, at time.Time) error {
	return g.finalizeGenerationSafeManagedUpstreamKeyInventoryRemovalManualReview(
		expected, channelIdentityRevision, claimEpoch, intentID, upstreamKeyID, upstreamKeyName,
		present, evidenceDigest, ownershipClaim, reason, at, false,
	)
}

// QuarantineGenerationSafeManagedUpstreamKeyInventoryRemoval makes every
// local binding to an ambiguously deleted remote identity non-routable while
// preserving the processing intent for a later complete-inventory reconcile.
func (g *Gateway) QuarantineGenerationSafeManagedUpstreamKeyInventoryRemoval(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID uint, upstreamKeyID int64, upstreamKeyName string, present bool, evidenceDigest, ownershipClaim string, at time.Time) error {
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	if !expected.Valid() || channelIdentityRevision == 0 || claimEpoch == 0 || intentID == 0 || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	claimKey := managedUpstreamKeyInventoryRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, present)
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			return err
		}
		domain, lockErr := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, at)
		if lockErr != nil && !errors.Is(lockErr, gorm.ErrRecordNotFound) {
			return lockErr
		}
		if domain == nil || domain.CanonicalGroupName != expected.CanonicalGroupName {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if err := requireUpstreamGroupGenerationStateTx(tx, expected, present); err != nil {
			return err
		}

		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND kind = ?", intentID, "removed").First(&change).Error; err != nil {
			return err
		}
		if change.Status != "processing" || change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch != claimEpoch ||
			change.RemovalClaimKey == nil || *change.RemovalClaimKey != claimKey ||
			!managedRemovalIntentMatches(change, expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, evidenceDigest, ownershipClaim) {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		return tx.Model(&GatewayUpstreamKey{}).
			Where("channel_id = ? AND upstream_key_id = ? AND upstream_key_name = ?", expected.ChannelID, upstreamKeyID, upstreamKeyName).
			Updates(map[string]any{
				"enabled":    false,
				"last_error": managedUpstreamKeyRemovalPendingReconciliation,
				"updated_at": at,
			}).Error
	})
}

func (g *Gateway) finalizeGenerationSafeManagedUpstreamKeyInventoryRemovalManualReview(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID uint, upstreamKeyID int64, upstreamKeyName string, present bool, evidenceDigest, ownershipClaim, reason string, at time.Time, allowAcknowledged bool) error {
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	if !expected.Valid() || channelIdentityRevision == 0 || claimEpoch == 0 || intentID == 0 || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	claimKey := managedUpstreamKeyInventoryRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, present)
	return g.db.Transaction(func(tx *gorm.DB) error {
		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND kind = ?", intentID, "removed").First(&change).Error; err != nil {
			return err
		}
		validStatus := change.Status == "processing" || allowAcknowledged && change.Status == "acknowledged"
		if !validStatus || change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch != claimEpoch ||
			change.RemovalClaimKey == nil || *change.RemovalClaimKey != claimKey ||
			!managedRemovalIntentMatches(change, expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, evidenceDigest, ownershipClaim) {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		return markManagedUpstreamKeyInventoryRemovalManualReviewTx(tx, intentID, claimKey, claimEpoch, reason, at)
	})
}

func (g *Gateway) finalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(expected UpstreamGroupGeneration, channelIdentityRevision, claimEpoch uint64, intentID uint, upstreamKeyID int64, upstreamKeyName string, present, purgeLocal bool, evidenceDigest, ownershipClaim string, at time.Time, remoteMutation func() error) error {
	upstreamKeyName = strings.TrimSpace(upstreamKeyName)
	evidenceDigest = strings.ToLower(strings.TrimSpace(evidenceDigest))
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	if !expected.Valid() || channelIdentityRevision == 0 || claimEpoch == 0 || intentID == 0 || upstreamKeyID <= 0 || upstreamKeyName == "" ||
		!validRemovalEvidenceDigest(evidenceDigest) || !validRemovalEvidenceDigest(ownershipClaim) {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	claimKey := managedUpstreamKeyInventoryRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, present)
	identityChanged := false
	generationChanged := false
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			if errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				identityChanged = true
				return closeManagedUpstreamKeyRemovalForIdentityChangeTx(tx, intentID, channelIdentityRevision, at)
			}
			return err
		}
		domain, lockErr := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, at)
		if lockErr != nil && !errors.Is(lockErr, gorm.ErrRecordNotFound) {
			return lockErr
		}
		if domain == nil || domain.CanonicalGroupName != expected.CanonicalGroupName {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if err := requireUpstreamGroupGenerationStateTx(tx, expected, present); err != nil {
			if errors.Is(err, ErrUpstreamGroupGenerationChanged) {
				generationChanged = true
				return markManagedUpstreamKeyInventoryRemovalManualReviewTx(tx, intentID, claimKey, claimEpoch, "group_generation_changed", at)
			}
			return err
		}

		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND kind = ?", intentID, "removed").First(&change).Error; err != nil {
			return err
		}
		if change.Status != "processing" && change.Status != "acknowledged" ||
			!managedRemovalIntentMatches(change, expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, evidenceDigest, ownershipClaim) ||
			change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch != claimEpoch || change.RemovalClaimKey == nil || *change.RemovalClaimKey != claimKey {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		if change.Status == "processing" && remoteMutation != nil {
			if err := remoteMutation(); err != nil {
				return err
			}
		}
		// Clear every local row that still points at the exact managed identity
		// deleted above. Keeping this in the acknowledgement transaction prevents
		// a crash from leaving usable stale credentials, while the exact ID/name
		// predicates leave concurrent rebinds untouched. This also repairs an old
		// named row when an absent Sub2API group is now represented by its numeric
		// internal identity.
		if err := resetExactManagedUpstreamKeyInventoryBindingTx(tx, expected.ChannelID, upstreamKeyID, upstreamKeyName, at); err != nil {
			return err
		}
		if purgeLocal {
			if err := purgeManagedUpstreamGroupStateTx(tx, expected); err != nil {
				return err
			}
		}
		result := tx.Model(&GatewayUpstreamGroupChange{}).
			Where("id = ? AND kind = ? AND status IN ? AND removal_claim_key = ? AND removal_claim_epoch = ?", intentID, "removed", []string{"processing", "acknowledged"}, claimKey, claimEpoch).
			Updates(map[string]any{"status": "acknowledged", "resolved_at": at, "updated_at": at})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		return nil
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

func resetExactManagedUpstreamKeyInventoryBindingTx(tx *gorm.DB, channelID uint, upstreamKeyID int64, upstreamKeyName string, at time.Time) error {
	return tx.Model(&GatewayUpstreamKey{}).
		Where("channel_id = ? AND upstream_key_id = ? AND upstream_key_name = ?", channelID, upstreamKeyID, strings.TrimSpace(upstreamKeyName)).
		Updates(map[string]any{
			"upstream_key_id":         0,
			"upstream_key_name":       "",
			"api_key_cipher":          "",
			"api_key_prefix":          "",
			"ownership_provenance":    GatewayUpstreamKeyProvenanceUnknown,
			"enabled":                 false,
			"provisioning":            false,
			"provisioning_started_at": nil,
			"last_error":              "managed upstream key deleted",
			"updated_at":              at,
		}).Error
}

// FinalizeAbsentUpstreamGroupState handles a group that has no remote identity
// to mutate. It still locks and verifies the exact absent generation before
// purging local routing state and recording the removed audit atomically.
func (g *Gateway) FinalizeAbsentUpstreamGroupState(expected UpstreamGroupGeneration, channelIdentityRevision uint64, groupName string, upstreamKeyID int64, upstreamKeyName string, at time.Time) error {
	groupName = strings.TrimSpace(groupName)
	if !expected.Valid() || channelIdentityRevision == 0 || groupName == "" {
		return ErrUpstreamGroupGenerationChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, expected.ChannelID, channelIdentityRevision); err != nil {
			return err
		}
		domain, lockErr := lockUpstreamRoutingHealthDomainTx(tx, expected.ChannelID, expected.CanonicalGroupName, at)
		if lockErr != nil && !errors.Is(lockErr, gorm.ErrRecordNotFound) {
			return lockErr
		}
		if domain == nil || domain.CanonicalGroupName != expected.CanonicalGroupName {
			return ErrUpstreamGroupGenerationChanged
		}
		if err := requireAbsentUpstreamGroupGenerationTx(tx, expected); err != nil {
			return err
		}
		canonical, err := canonicalUpstreamGroupName(tx, groupName)
		if err != nil || canonical != expected.CanonicalGroupName {
			return ErrUpstreamGroupGenerationChanged
		}

		var latest GatewayUpstreamGroupChange
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("upstream_group_id = ?", expected.ID).Order("id DESC").First(&latest).Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		if findErr == nil && latest.Kind == "removed" && latest.PresenceGeneration != nil && *latest.PresenceGeneration == expected.PresenceGeneration {
			switch latest.Status {
			case "ignored":
				// An ignored removal is a durable manual-review decision. The
				// local key was intentionally quarantined, so repeating an absent
				// inventory must be idempotent and must not turn the preserved
				// evidence into a transient synchronization failure.
				return nil
			case "acknowledged":
				return purgeManagedUpstreamGroupStateTx(tx, expected)
			default:
				return ErrManagedUpstreamKeyRemovalIntentChanged
			}
		}
		if err := purgeManagedUpstreamGroupStateTx(tx, expected); err != nil {
			return err
		}
		change := newUpstreamGroupChange(expected.ChannelID, groupName, "removed", upstreamKeyID, upstreamKeyName, at)
		change.UpstreamGroupID = uintPointer(expected.ID)
		change.PresenceGeneration = uint64Pointer(expected.PresenceGeneration)
		return tx.Create(change).Error
	})
}

func requireExclusiveManagedUpstreamKeyRemovalIdentityTx(tx *gorm.DB, expected UpstreamGroupGeneration, change GatewayUpstreamGroupChange, localKeyID uint) (*GatewayUpstreamKey, error) {
	remoteName := strings.TrimSpace(change.UpstreamKeyName)
	if change.UpstreamKeyID <= 0 || remoteName == "" {
		return nil, ErrManagedUpstreamKeyRemovalIntentChanged
	}
	groupRows := make([]GatewayUpstreamKey, 0, 2)
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).
		Order("id ASC").Find(&groupRows).Error; err != nil {
		return nil, err
	}
	if len(groupRows) > 1 || (localKeyID != 0 && (len(groupRows) != 1 || groupRows[0].ID != localKeyID)) {
		return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	rows := make([]GatewayUpstreamKey, 0, 2)
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND (upstream_key_id = ? OR btrim(upstream_key_name) = ?)", expected.ChannelID, change.UpstreamKeyID, remoteName).
		Order("id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	var matchedID uint
	for _, row := range rows {
		canonical, err := canonicalUpstreamGroupName(tx, row.GroupName)
		if err != nil || canonical != expected.CanonicalGroupName || (matchedID != 0 && matchedID != row.ID) {
			return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
		}
		matchedID = row.ID
	}
	if len(groupRows) == 1 && matchedID != groupRows[0].ID {
		return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	if localKeyID != 0 && matchedID != localKeyID {
		return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	if len(groupRows) != 1 {
		return nil, ErrManagedUpstreamKeyRemovalIdentityConflict
	}
	return &groupRows[0], nil
}

func purgeManagedUpstreamGroupStateTx(tx *gorm.DB, expected UpstreamGroupGeneration) error {
	if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).Delete(&GatewayUpstreamKey{}).Error; err != nil {
		return err
	}
	if err := tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).Delete(&GatewayModelCapability{}).Error; err != nil {
		return err
	}
	return tx.Where("channel_id = ? AND lower(btrim(group_name)) = ?", expected.ChannelID, expected.CanonicalGroupName).Delete(&GatewayRoutingFailure{}).Error
}

// ReconcileGenerationSafeManagedUpstreamKeyRemovals consumes a complete remote
// inventory. Duplicate names quarantine only intents for that name; unrelated
// intents can still be reconciled by exact ID. It only acknowledges a
// disappeared identity when both its ID and expected name are absent and its
// durable intent contains generation and evidence. Legacy intents are retained
// for manual review, never resumed.
func (g *Gateway) ReconcileGenerationSafeManagedUpstreamKeyRemovals(channelID uint, inventoryIdentityRevision uint64, presentRemote map[int64]string, at time.Time) error {
	if channelID == 0 || inventoryIdentityRevision == 0 {
		return errors.New("channel id and account identity revision are required")
	}
	if at.IsZero() {
		at = time.Now()
	}
	nameIDs, ambiguousNames := managedUpstreamKeyRemoteNameIndex(presentRemote)
	if err := g.RequireUpstreamAccountIdentityRevision(channelID, inventoryIdentityRevision); err != nil {
		return err
	}
	var changes []GatewayUpstreamGroupChange
	if err := g.db.Where(
		"channel_id = ? AND kind = ? AND (status = ? OR (status = ? AND removal_claim_key IS NOT NULL))",
		channelID, "removed", "processing", "acknowledged",
	).Find(&changes).Error; err != nil {
		return err
	}
	for _, change := range changes {
		if change.RemovalChannelIdentityRevision == nil {
			if err := g.markManagedUpstreamKeyRemovalManualReviewByID(change.ID, "legacy_removal_intent", at); err != nil {
				return err
			}
			continue
		}
		if *change.RemovalChannelIdentityRevision != inventoryIdentityRevision {
			if err := g.markManagedUpstreamKeyRemovalManualReviewByID(change.ID, "channel_identity_changed", at); err != nil {
				return err
			}
			continue
		}
		if change.UpstreamGroupID == nil || change.PresenceGeneration == nil || change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch == 0 || change.UpstreamKeyID <= 0 ||
			strings.TrimSpace(change.UpstreamKeyName) == "" || !validRemovalEvidenceDigest(change.FactsDigest) ||
			!validRemovalEvidenceDigest(change.RemovalOwnershipClaim) {
			if err := g.markManagedUpstreamKeyRemovalManualReviewByID(change.ID, "legacy_removal_intent", at); err != nil {
				return err
			}
			continue
		}
		expected := UpstreamGroupGeneration{
			ID: *change.UpstreamGroupID, ChannelID: change.ChannelID,
			CanonicalGroupName: mustCanonicalRemovalGroup(g.db, change.GroupName),
			PresenceGeneration: *change.PresenceGeneration,
		}
		expectedClaimKey := managedUpstreamKeyRemovalClaimKey(expected, inventoryIdentityRevision, change.UpstreamKeyID, change.UpstreamKeyName)
		if !expected.Valid() || change.RemovalClaimKey == nil || *change.RemovalClaimKey != expectedClaimKey ||
			!managedRemovalIntentMatches(change, expected, inventoryIdentityRevision, change.UpstreamKeyID, change.UpstreamKeyName, change.FactsDigest, change.RemovalOwnershipClaim) {
			if err := g.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.GroupName, change.UpstreamKeyID, change.UpstreamKeyName, change.ID, false, at); err != nil && !errors.Is(err, ErrUpstreamGroupGenerationChanged) && !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				return err
			}
			continue
		}
		if err := g.RequireAbsentUpstreamGroupGeneration(expected); err != nil {
			if errors.Is(err, ErrUpstreamGroupGenerationChanged) {
				if reviewErr := g.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.GroupName, change.UpstreamKeyID, change.UpstreamKeyName, change.ID, false, at); reviewErr != nil && !errors.Is(reviewErr, ErrUpstreamGroupGenerationChanged) && !errors.Is(reviewErr, ErrStaleChannelAccountIdentityRevision) {
					return reviewErr
				}
				continue
			}
			return err
		}
		expectedName := strings.TrimSpace(change.UpstreamKeyName)
		if _, ambiguous := ambiguousNames[expectedName]; ambiguous {
			if err := g.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.GroupName, change.UpstreamKeyID, change.UpstreamKeyName, change.ID, false, at); err != nil && !errors.Is(err, ErrUpstreamGroupGenerationChanged) && !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				return err
			}
			continue
		}
		remoteName, remotePresent := presentRemote[change.UpstreamKeyID]
		if remotePresent {
			if strings.TrimSpace(remoteName) != expectedName || change.Status == "acknowledged" {
				if err := g.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.GroupName, change.UpstreamKeyID, change.UpstreamKeyName, change.ID, false, at); err != nil && !errors.Is(err, ErrUpstreamGroupGenerationChanged) && !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
					return err
				}
			}
			continue
		}
		if nameReappearedID, nameReappeared := nameIDs[expectedName]; nameReappeared && nameReappearedID != change.UpstreamKeyID {
			if err := g.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.GroupName, change.UpstreamKeyID, change.UpstreamKeyName, change.ID, false, at); err != nil && !errors.Is(err, ErrUpstreamGroupGenerationChanged) && !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				return err
			}
			continue
		}
		if change.Status == "acknowledged" {
			continue
		}
		if err := g.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
			expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.ID, 0, change.UpstreamKeyID, change.UpstreamKeyName, change.FactsDigest, change.RemovalOwnershipClaim, at, nil,
		); err != nil {
			if errors.Is(err, ErrUpstreamGroupGenerationChanged) || errors.Is(err, ErrManagedUpstreamKeyRemovalIdentityConflict) || errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) || errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				if reviewErr := g.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.GroupName, change.UpstreamKeyID, change.UpstreamKeyName, change.ID, false, at); reviewErr != nil && !errors.Is(reviewErr, ErrUpstreamGroupGenerationChanged) && !errors.Is(reviewErr, ErrStaleChannelAccountIdentityRevision) {
					return reviewErr
				}
				continue
			}
			return err
		}
	}
	return nil
}

// ReconcileGenerationSafeManagedUpstreamKeyInventoryRemovals consumes a
// complete remote inventory for deletion intents created from managed-key
// inventory reconciliation. A disappeared exact ID/name proves that a
// processing provider mutation committed, while an acknowledged intent is
// replayed to repair stale local bindings left by older releases.
func (g *Gateway) ReconcileGenerationSafeManagedUpstreamKeyInventoryRemovals(channelID uint, inventoryIdentityRevision uint64, presentRemote map[int64]string, at time.Time) error {
	if channelID == 0 || inventoryIdentityRevision == 0 {
		return errors.New("channel id and account identity revision are required")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	nameIDs, ambiguousNames := managedUpstreamKeyRemoteNameIndex(presentRemote)
	if err := g.RequireUpstreamAccountIdentityRevision(channelID, inventoryIdentityRevision); err != nil {
		return err
	}
	var changes []GatewayUpstreamGroupChange
	if err := g.db.Where(
		"channel_id = ? AND kind = ? AND status IN ? AND removal_claim_key IS NOT NULL",
		channelID, "removed", []string{"processing", "acknowledged"},
	).Find(&changes).Error; err != nil {
		return err
	}
	for _, change := range changes {
		markByID := func(reason string) error {
			return g.markManagedUpstreamKeyRemovalManualReviewByID(change.ID, reason, at)
		}
		if change.UpstreamGroupID == nil || change.PresenceGeneration == nil || change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch == 0 ||
			change.RemovalChannelIdentityRevision == nil || *change.RemovalChannelIdentityRevision != inventoryIdentityRevision ||
			change.UpstreamKeyID <= 0 || strings.TrimSpace(change.UpstreamKeyName) == "" ||
			!validRemovalEvidenceDigest(change.FactsDigest) || !validRemovalEvidenceDigest(change.RemovalOwnershipClaim) {
			if err := markByID("inventory_removal_intent_invalid"); err != nil {
				return err
			}
			continue
		}
		expected := UpstreamGroupGeneration{
			ID: *change.UpstreamGroupID, ChannelID: change.ChannelID,
			CanonicalGroupName: mustCanonicalRemovalGroup(g.db, change.GroupName),
			PresenceGeneration: *change.PresenceGeneration,
		}
		if !expected.Valid() || change.RemovalClaimKey == nil {
			if err := markByID("inventory_removal_identity_invalid"); err != nil {
				return err
			}
			continue
		}
		claimKey := *change.RemovalClaimKey
		groupPresent := false
		switch claimKey {
		case managedUpstreamKeyInventoryRemovalClaimKey(expected, inventoryIdentityRevision, change.UpstreamKeyID, change.UpstreamKeyName, true):
			groupPresent = true
		case managedUpstreamKeyInventoryRemovalClaimKey(expected, inventoryIdentityRevision, change.UpstreamKeyID, change.UpstreamKeyName, false):
			groupPresent = false
		default:
			if claimKey == managedUpstreamKeyRemovalClaimKey(expected, inventoryIdentityRevision, change.UpstreamKeyID, change.UpstreamKeyName) {
				continue
			}
			if err := markByID("inventory_removal_claim_invalid"); err != nil {
				return err
			}
			continue
		}
		if !managedRemovalIntentMatches(change, expected, inventoryIdentityRevision, change.UpstreamKeyID, change.UpstreamKeyName, change.FactsDigest, change.RemovalOwnershipClaim) {
			if err := markByID("inventory_removal_evidence_invalid"); err != nil {
				return err
			}
			continue
		}
		markExact := func(reason string) error {
			return g.finalizeGenerationSafeManagedUpstreamKeyInventoryRemovalManualReview(
				expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.ID,
				change.UpstreamKeyID, change.UpstreamKeyName, groupPresent, change.FactsDigest,
				change.RemovalOwnershipClaim, reason, at, true,
			)
		}
		expectedName := strings.TrimSpace(change.UpstreamKeyName)
		if _, ambiguous := ambiguousNames[expectedName]; ambiguous {
			if err := markExact("remote_name_ambiguous"); err != nil {
				return err
			}
			continue
		}
		if remoteName, remotePresent := presentRemote[change.UpstreamKeyID]; remotePresent {
			// A still-visible processing identity remains owned by the ordinary
			// deletion path. An acknowledged identity that reappears is retained
			// for manual review instead of clearing local credentials.
			if strings.TrimSpace(remoteName) != expectedName || change.Status == "acknowledged" {
				reason := "remote_identity_changed"
				if strings.TrimSpace(remoteName) == expectedName {
					reason = "remote_identity_reappeared"
				}
				if err := markExact(reason); err != nil {
					return err
				}
				continue
			}
			continue
		}
		if reappearedID, reappeared := nameIDs[expectedName]; reappeared && reappearedID != change.UpstreamKeyID {
			if err := markExact("remote_name_reappeared"); err != nil {
				return err
			}
			continue
		}
		if err := g.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemovalKeepLocal(
			expected, inventoryIdentityRevision, *change.RemovalClaimEpoch, change.ID,
			change.UpstreamKeyID, change.UpstreamKeyName, groupPresent, change.FactsDigest,
			change.RemovalOwnershipClaim, at, nil,
		); err != nil {
			if errors.Is(err, ErrUpstreamGroupGenerationChanged) || errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) || errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
				if reviewErr := markExact("inventory_removal_fence_changed"); reviewErr != nil {
					return errors.Join(err, reviewErr)
				}
				continue
			}
			return err
		}
	}
	return nil
}

func managedUpstreamKeyRemoteNameIndex(presentRemote map[int64]string) (map[string]int64, map[string]struct{}) {
	nameIDs := make(map[string]int64, len(presentRemote))
	ambiguousNames := make(map[string]struct{})
	for id, rawName := range presentRemote {
		name := strings.TrimSpace(rawName)
		if id <= 0 || name == "" {
			continue
		}
		if previous, exists := nameIDs[name]; exists && previous != id {
			ambiguousNames[name] = struct{}{}
			continue
		}
		nameIDs[name] = id
	}
	return nameIDs, ambiguousNames
}

// AcknowledgeGenerationSafeManagedUpstreamKeyRemoval records crash recovery
// after the caller completed a full remote list and proved this one ID absent.
// It never upgrades a legacy or cross-generation intent.
func (g *Gateway) AcknowledgeGenerationSafeManagedUpstreamKeyRemoval(expected UpstreamGroupGeneration, channelIdentityRevision uint64, upstreamKeyID int64, upstreamKeyName string, localKeyID uint, ownershipClaim string, at time.Time) error {
	ownershipClaim = strings.ToLower(strings.TrimSpace(ownershipClaim))
	if !expected.Valid() || channelIdentityRevision == 0 || upstreamKeyID <= 0 || strings.TrimSpace(upstreamKeyName) == "" || !validRemovalEvidenceDigest(ownershipClaim) {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if at.IsZero() {
		at = time.Now()
	}
	claimKey := managedUpstreamKeyRemovalClaimKey(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName)
	var change GatewayUpstreamGroupChange
	err := g.db.Where("removal_claim_key = ?", claimKey).First(&change).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		changePointer, _, beginErr := g.BeginGenerationSafeManagedUpstreamKeyRemoval(
			expected, channelIdentityRevision, expected.CanonicalGroupName, upstreamKeyID, upstreamKeyName,
			managedUpstreamKeyAbsenceEvidenceDigest(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, ownershipClaim), ownershipClaim, at, 0,
		)
		if beginErr != nil {
			return beginErr
		}
		if changePointer == nil {
			return ErrManagedUpstreamKeyRemovalIntentChanged
		}
		change = *changePointer
		err = nil
	}
	if err != nil {
		return err
	}
	if change.Status != "processing" && change.Status != "acknowledged" {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if change.RemovalClaimKey == nil || *change.RemovalClaimKey != claimKey ||
		!managedRemovalIntentMatches(change, expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, change.FactsDigest, ownershipClaim) || !validRemovalEvidenceDigest(change.FactsDigest) {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	if change.RemovalClaimEpoch == nil || *change.RemovalClaimEpoch == 0 {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	return g.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		expected, channelIdentityRevision, *change.RemovalClaimEpoch, change.ID, localKeyID, change.UpstreamKeyID, change.UpstreamKeyName, change.FactsDigest, ownershipClaim, at, nil,
	)
}

func requireAbsentUpstreamGroupGenerationTx(tx *gorm.DB, expected UpstreamGroupGeneration) error {
	if !expected.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	var current GatewayUpstreamGroup
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "channel_id", "canonical_group_name", "presence_generation", "present").
		Where("id = ?", expected.ID).First(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrUpstreamGroupGenerationChanged
	}
	if err != nil {
		return err
	}
	if current.Present || current.ChannelID != expected.ChannelID ||
		current.CanonicalGroupName != expected.CanonicalGroupName ||
		current.PresenceGeneration != expected.PresenceGeneration {
		return ErrUpstreamGroupGenerationChanged
	}
	return nil
}

func requireUpstreamGroupGenerationStateTx(tx *gorm.DB, expected UpstreamGroupGeneration, present bool) error {
	if present {
		return requireUpstreamGroupGenerationTx(tx, expected)
	}
	return requireAbsentUpstreamGroupGenerationTx(tx, expected)
}

func markManagedUpstreamKeyInventoryRemovalManualReviewTx(tx *gorm.DB, intentID uint, claimKey string, claimEpoch uint64, reason string, at time.Time) error {
	if tx == nil || intentID == 0 || strings.TrimSpace(claimKey) == "" || claimEpoch == 0 {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	result := tx.Model(&GatewayUpstreamGroupChange{}).
		Where("id = ? AND kind = ? AND status IN ? AND removal_claim_key = ? AND removal_claim_epoch = ?", intentID, "removed", []string{"processing", "acknowledged"}, claimKey, claimEpoch).
		Updates(map[string]any{
			"status":                       "ignored",
			"assignment_state":             "ignored",
			"assignment_reason_code":       managedUpstreamKeyRemovalManualReview,
			"removal_manual_review_reason": normalizeManagedUpstreamKeyRemovalReviewReason(reason),
			"removal_manual_review_actor":  managedUpstreamKeyRemovalSystemActor,
			"removal_manual_reviewed_at":   at,
			"resolved_at":                  nil,
			"evaluated_at":                 at,
			"updated_at":                   at,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrManagedUpstreamKeyRemovalIntentChanged
	}
	return nil
}

func managedUpstreamKeyRemovalClaimKey(expected UpstreamGroupGeneration, channelIdentityRevision uint64, upstreamKeyID int64, upstreamKeyName string) string {
	raw := fmt.Sprintf("managed-key-removal:v3:%d:%d:%d:%d:%d:%s", expected.ChannelID, channelIdentityRevision, expected.ID, expected.PresenceGeneration, upstreamKeyID, strings.TrimSpace(upstreamKeyName))
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func managedUpstreamKeyInventoryRemovalClaimKey(expected UpstreamGroupGeneration, channelIdentityRevision uint64, upstreamKeyID int64, upstreamKeyName string, present bool) string {
	state := "absent"
	if present {
		state = "present"
	}
	raw := fmt.Sprintf("managed-key-inventory-removal:v1:%s:%d:%d:%d:%d:%d:%s", state, expected.ChannelID, channelIdentityRevision, expected.ID, expected.PresenceGeneration, upstreamKeyID, strings.TrimSpace(upstreamKeyName))
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func managedUpstreamKeyAbsenceEvidenceDigest(expected UpstreamGroupGeneration, channelIdentityRevision uint64, upstreamKeyID int64, upstreamKeyName, ownershipClaim string) string {
	raw := fmt.Sprintf("managed-key-removal-absent:v3:%d:%d:%d:%d:%d:%s:%s", expected.ChannelID, channelIdentityRevision, expected.ID, expected.PresenceGeneration, upstreamKeyID, strings.TrimSpace(upstreamKeyName), strings.TrimSpace(ownershipClaim))
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// ManagedUpstreamKeyAbsenceEvidenceDigest returns the durable evidence digest
// for a complete remote inventory that did not contain the expected key. It is
// intentionally exported for the service/API manual-review recovery path;
// callers must still perform the inventory and ownership checks themselves.
func ManagedUpstreamKeyAbsenceEvidenceDigest(expected UpstreamGroupGeneration, channelIdentityRevision uint64, upstreamKeyID int64, upstreamKeyName, ownershipClaim string) string {
	return managedUpstreamKeyAbsenceEvidenceDigest(expected, channelIdentityRevision, upstreamKeyID, upstreamKeyName, ownershipClaim)
}

func managedRemovalIntentMatches(change GatewayUpstreamGroupChange, expected UpstreamGroupGeneration, channelIdentityRevision uint64, upstreamKeyID int64, upstreamKeyName, evidenceDigest, ownershipClaim string) bool {
	return change.UpstreamGroupID != nil && *change.UpstreamGroupID == expected.ID &&
		change.PresenceGeneration != nil && *change.PresenceGeneration == expected.PresenceGeneration &&
		change.RemovalChannelIdentityRevision != nil && *change.RemovalChannelIdentityRevision == channelIdentityRevision &&
		change.RemovalClaimEpoch != nil && *change.RemovalClaimEpoch > 0 &&
		change.RemovalClaimDigestVersion != nil && *change.RemovalClaimDigestVersion == managedUpstreamKeyRemovalClaimDigestVersion &&
		change.RemovalEvidenceDigestVersion != nil && *change.RemovalEvidenceDigestVersion == managedUpstreamKeyRemovalEvidenceDigestVersion &&
		change.RemovalOwnershipDigestVersion != nil && *change.RemovalOwnershipDigestVersion == managedUpstreamKeyRemovalOwnershipDigestVersion &&
		change.ChannelID == expected.ChannelID && change.UpstreamKeyID == upstreamKeyID &&
		strings.TrimSpace(change.UpstreamKeyName) == strings.TrimSpace(upstreamKeyName) &&
		strings.EqualFold(strings.TrimSpace(change.FactsDigest), strings.TrimSpace(evidenceDigest)) &&
		strings.EqualFold(strings.TrimSpace(change.RemovalOwnershipClaim), strings.TrimSpace(ownershipClaim))
}

func validRemovalEvidenceDigest(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func normalizeManagedUpstreamKeyRemovalReviewReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > 64 {
		return "manual_review_required"
	}
	return reason
}

func normalizeManagedUpstreamKeyRemovalReviewActor(actor string) string {
	actor = strings.TrimSpace(actor)
	if actor == "" || len(actor) > 128 {
		return "administrator"
	}
	return actor
}

func normalizeManagedUpstreamKeyRemovalReviewResolution(resolution string) string {
	resolution = strings.TrimSpace(resolution)
	if resolution == "" || len(resolution) > 64 {
		return "revalidated"
	}
	return resolution
}

func mustCanonicalRemovalGroup(db *gorm.DB, groupName string) string {
	canonical, err := canonicalUpstreamGroupName(db, groupName)
	if err != nil {
		return ""
	}
	return canonical
}

func uintPointer(value uint) *uint       { return &value }
func uint64Pointer(value uint64) *uint64 { return &value }
func uint16Pointer(value uint16) *uint16 { return &value }
