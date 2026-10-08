package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	channelDeletionClaimDigestVersion  uint16 = 1
	channelDeletionFinalDigestVersion  uint16 = 2
	channelDeletionReviewDigestVersion uint16 = 1
	// Migration 54 reserves MaxInt64 itself, so this is the largest epoch that
	// can actually be persisted by the PostgreSQL CHECK constraint.
	channelDeletionMaxClaimEpoch    = uint64(1<<63 - 2)
	channelDeletionDefaultListLimit = 50
	channelDeletionMaximumListLimit = 500
	channelDeletionCoordinatorActor = "channel_deletion_coordinator"
)

var (
	ErrChannelDeletionKeyClaimLost        = errors.New("channel deletion key claim was lost")
	ErrChannelDeletionKeyFreezeChanged    = errors.New("channel deletion key freeze changed")
	ErrChannelDeletionFinalizationBlocked = errors.New("channel deletion finalization is blocked")
)

type ChannelDeletionKeyClaimStatus string

const (
	ChannelDeletionKeyPendingVerification      ChannelDeletionKeyClaimStatus = "pending_verification"
	ChannelDeletionKeyProcessing               ChannelDeletionKeyClaimStatus = "processing"
	ChannelDeletionKeyProcessingUnknown        ChannelDeletionKeyClaimStatus = "processing_unknown"
	ChannelDeletionKeyDeleteConfirmed          ChannelDeletionKeyClaimStatus = "delete_confirmed"
	ChannelDeletionKeyInitiallyAbsentConfirmed ChannelDeletionKeyClaimStatus = "initially_absent_confirmed"
	ChannelDeletionKeyRetainedAdopted          ChannelDeletionKeyClaimStatus = "retained_adopted"
	ChannelDeletionKeyRetainedByReview         ChannelDeletionKeyClaimStatus = "retained_by_review"
	ChannelDeletionKeyManualReviewRequired     ChannelDeletionKeyClaimStatus = "manual_review_required"
)

// ChannelDeletionKeyClaim is mutable coordinator state. It contains frozen
// identifiers and hashes only. LocalKey is populated only by a fenced claim or
// revalidation call and is never persisted or serialized.
type ChannelDeletionKeyClaim struct {
	ID                       uint64                        `gorm:"primaryKey" json:"id"`
	IntentID                 uint64                        `gorm:"not null;uniqueIndex:idx_channel_deletion_claim_local,priority:1" json:"intent_id"`
	PrepareAuditID           uint64                        `gorm:"not null" json:"prepare_audit_id"`
	ChannelID                uint                          `gorm:"not null" json:"channel_id"`
	DeletionEpoch            uint64                        `gorm:"not null" json:"deletion_epoch"`
	ChannelIdentityRevision  uint64                        `gorm:"not null" json:"-"`
	LocalKeyID               uint                          `gorm:"not null;uniqueIndex:idx_channel_deletion_claim_local,priority:2" json:"local_key_id"`
	KeyGeneration            uint64                        `gorm:"not null" json:"key_generation"`
	UpstreamGroupID          *uint                         `json:"upstream_group_id,omitempty"`
	PresenceGeneration       *uint64                       `json:"presence_generation,omitempty"`
	CanonicalGroupName       string                        `gorm:"size:128;not null;default:''" json:"canonical_group_name,omitempty"`
	UpstreamKeyID            int64                         `gorm:"not null;default:0" json:"upstream_key_id,omitempty"`
	ObservedRemoteID         int64                         `gorm:"not null;default:0" json:"observed_remote_id,omitempty"`
	UpstreamKeyName          string                        `gorm:"size:128;not null;default:''" json:"upstream_key_name,omitempty"`
	OwnershipProvenance      string                        `gorm:"size:32;not null" json:"ownership_provenance"`
	Provisioning             bool                          `gorm:"not null;default:false" json:"provisioning"`
	ProvisioningStartedAt    *time.Time                    `json:"provisioning_started_at,omitempty"`
	LocalOwnershipClaim      string                        `gorm:"size:64;not null;default:''" json:"local_ownership_claim,omitempty"`
	Disposition              ChannelDeletionKeyDisposition `gorm:"size:32;not null" json:"disposition"`
	Status                   ChannelDeletionKeyClaimStatus `gorm:"size:32;not null;default:'pending_verification'" json:"status"`
	ClaimEpoch               uint64                        `gorm:"not null;default:0" json:"claim_epoch"`
	ClaimDigest              string                        `gorm:"size:64;not null" json:"claim_digest"`
	ClaimDigestVersion       uint16                        `gorm:"not null" json:"claim_digest_version"`
	ClaimedAt                *time.Time                    `json:"claimed_at,omitempty"`
	LeaseExpiresAt           *time.Time                    `json:"lease_expires_at,omitempty"`
	EvidenceDigest           string                        `gorm:"size:64;not null;default:''" json:"evidence_digest,omitempty"`
	EvidenceDigestVersion    *uint16                       `json:"evidence_digest_version,omitempty"`
	OwnershipDigest          string                        `gorm:"size:64;not null;default:''" json:"ownership_digest,omitempty"`
	OwnershipDigestVersion   *uint16                       `json:"ownership_digest_version,omitempty"`
	ObservedFingerprint      string                        `gorm:"size:64;not null;default:''" json:"observed_fingerprint,omitempty"`
	FingerprintDigestVersion *uint16                       `json:"fingerprint_digest_version,omitempty"`
	InventoryDigest          string                        `gorm:"size:64;not null;default:''" json:"inventory_digest,omitempty"`
	InventoryDigestVersion   *uint16                       `json:"inventory_digest_version,omitempty"`
	OutcomeReason            string                        `gorm:"size:64;not null;default:''" json:"outcome_reason,omitempty"`
	OutcomeActor             string                        `gorm:"size:128;not null;default:''" json:"outcome_actor,omitempty"`
	OutcomeAt                *time.Time                    `json:"outcome_at,omitempty"`
	LastError                string                        `gorm:"type:text;not null;default:''" json:"last_error,omitempty"`
	CreatedAt                time.Time                     `gorm:"not null" json:"created_at"`
	UpdatedAt                time.Time                     `gorm:"not null" json:"updated_at"`
	LocalKey                 *GatewayUpstreamKey           `gorm:"-" json:"-"`
}

func (ChannelDeletionKeyClaim) TableName() string {
	return "gateway_channel_deletion_key_claims"
}

type ChannelDeletionKeyEvidence struct {
	ID                       uint64                        `gorm:"primaryKey" json:"id"`
	KeyClaimID               uint64                        `gorm:"not null" json:"key_claim_id"`
	IntentID                 uint64                        `gorm:"not null" json:"intent_id"`
	ChannelID                uint                          `gorm:"not null" json:"channel_id"`
	DeletionEpoch            uint64                        `gorm:"not null" json:"deletion_epoch"`
	ChannelIdentityRevision  uint64                        `gorm:"not null" json:"-"`
	LocalKeyID               uint                          `gorm:"not null" json:"local_key_id"`
	KeyGeneration            uint64                        `gorm:"not null" json:"key_generation"`
	UpstreamGroupID          *uint                         `json:"upstream_group_id,omitempty"`
	PresenceGeneration       *uint64                       `json:"presence_generation,omitempty"`
	CanonicalGroupName       string                        `gorm:"size:128;not null;default:''" json:"canonical_group_name,omitempty"`
	UpstreamKeyID            int64                         `gorm:"not null;default:0" json:"upstream_key_id,omitempty"`
	ObservedRemoteID         int64                         `gorm:"not null;default:0" json:"observed_remote_id,omitempty"`
	UpstreamKeyName          string                        `gorm:"size:128;not null;default:''" json:"upstream_key_name,omitempty"`
	LocalOwnershipClaim      string                        `gorm:"size:64;not null;default:''" json:"local_ownership_claim,omitempty"`
	ClaimEpoch               uint64                        `gorm:"not null" json:"claim_epoch"`
	EventKind                string                        `gorm:"size:32;not null" json:"event_kind"`
	StatusBefore             ChannelDeletionKeyClaimStatus `gorm:"size:32;not null" json:"status_before"`
	StatusAfter              ChannelDeletionKeyClaimStatus `gorm:"size:32;not null" json:"status_after"`
	ClaimDigest              string                        `gorm:"size:64;not null" json:"claim_digest"`
	ClaimDigestVersion       uint16                        `gorm:"not null" json:"claim_digest_version"`
	EvidenceDigest           string                        `gorm:"size:64;not null;default:''" json:"evidence_digest,omitempty"`
	EvidenceDigestVersion    *uint16                       `json:"evidence_digest_version,omitempty"`
	OwnershipDigest          string                        `gorm:"size:64;not null;default:''" json:"ownership_digest,omitempty"`
	OwnershipDigestVersion   *uint16                       `json:"ownership_digest_version,omitempty"`
	ObservedFingerprint      string                        `gorm:"size:64;not null;default:''" json:"observed_fingerprint,omitempty"`
	FingerprintDigestVersion *uint16                       `json:"fingerprint_digest_version,omitempty"`
	InventoryDigest          string                        `gorm:"size:64;not null;default:''" json:"inventory_digest,omitempty"`
	InventoryDigestVersion   *uint16                       `json:"inventory_digest_version,omitempty"`
	Reason                   string                        `gorm:"size:64;not null;default:''" json:"reason,omitempty"`
	Actor                    string                        `gorm:"size:128;not null" json:"actor"`
	ErrorSummary             string                        `gorm:"type:text;not null;default:''" json:"error_summary,omitempty"`
	ObservedAt               time.Time                     `gorm:"not null" json:"observed_at"`
	CreatedAt                time.Time                     `gorm:"not null" json:"created_at"`
}

func (ChannelDeletionKeyEvidence) TableName() string {
	return "gateway_channel_deletion_key_evidence"
}

type ChannelDeletionFinalAudit struct {
	ID                       uint64      `gorm:"primaryKey" json:"id"`
	IntentID                 uint64      `gorm:"not null;uniqueIndex" json:"intent_id"`
	ChannelID                uint        `gorm:"not null" json:"channel_id"`
	DeletionEpoch            uint64      `gorm:"not null" json:"deletion_epoch"`
	ChannelIdentityRevision  uint64      `gorm:"not null" json:"-"`
	ChannelNameSnapshot      string      `gorm:"size:128;not null" json:"channel_name_snapshot"`
	ChannelTypeSnapshot      ChannelType `gorm:"size:32;not null" json:"channel_type_snapshot"`
	PreparedKeyCount         int64       `gorm:"not null" json:"prepared_key_count"`
	DeleteConfirmedCount     int64       `gorm:"not null" json:"delete_confirmed_count"`
	InitiallyAbsentCount     int64       `gorm:"not null" json:"initially_absent_count"`
	RetainedAdoptedCount     int64       `gorm:"not null" json:"retained_adopted_count"`
	RetainedByReviewCount    int64       `gorm:"not null" json:"retained_by_review_count"`
	KeyOutcomeDigest         string      `gorm:"size:64;not null" json:"key_outcome_digest"`
	KeyOutcomeDigestVersion  uint16      `gorm:"not null" json:"key_outcome_digest_version"`
	AuthSessionCleanupStatus string      `gorm:"size:16;not null" json:"auth_session_cleanup_status"`
	Actor                    string      `gorm:"size:128;not null" json:"actor"`
	FinalizedAt              time.Time   `gorm:"not null" json:"finalized_at"`
	CreatedAt                time.Time   `gorm:"not null" json:"created_at"`
}

func (ChannelDeletionFinalAudit) TableName() string {
	return "gateway_channel_deletion_final_audits"
}

type ChannelDeletionFence struct {
	ChannelID        uint
	IntentID         uint64
	DeletionEpoch    uint64
	IdentityRevision uint64
}

func (f ChannelDeletionFence) valid() bool {
	return f.ChannelID > 0 && f.IntentID > 0 && f.DeletionEpoch > 0 && f.IdentityRevision > 0
}

type ChannelDeletionKeyClaimFence struct {
	ChannelDeletionFence
	KeyClaimID uint64
	ClaimEpoch uint64
}

func (f ChannelDeletionKeyClaimFence) valid() bool {
	return f.ChannelDeletionFence.valid() && f.KeyClaimID > 0 && f.ClaimEpoch > 0
}

// ChannelDeletionKeyReviewFence is the administrator CAS fence for a manual
// review decision. Unlike a worker claim, a prepared manual-review row may
// legitimately still be at epoch zero.
type ChannelDeletionKeyReviewFence struct {
	ChannelDeletionFence
	KeyClaimID uint64
	ClaimEpoch uint64
}

func (f ChannelDeletionKeyReviewFence) valid() bool {
	return f.ChannelDeletionFence.valid() && f.KeyClaimID > 0 && f.ClaimEpoch <= channelDeletionMaxClaimEpoch
}

type ChannelDeletionKeyOutcome struct {
	Status                   ChannelDeletionKeyClaimStatus
	ObservedRemoteID         int64
	EvidenceDigest           string
	EvidenceDigestVersion    uint16
	OwnershipDigest          string
	OwnershipDigestVersion   uint16
	ObservedFingerprint      string
	FingerprintDigestVersion uint16
	InventoryDigest          string
	InventoryDigestVersion   uint16
	Reason                   string
	Actor                    string
	ErrorSummary             string
	ObservedAt               time.Time
}

type channelDeletionClaimDigestPayload struct {
	Version                 uint16                        `json:"version"`
	IntentID                uint64                        `json:"intent_id"`
	PrepareAuditID          uint64                        `json:"prepare_audit_id"`
	ChannelID               uint                          `json:"channel_id"`
	DeletionEpoch           uint64                        `json:"deletion_epoch"`
	ChannelIdentityRevision uint64                        `json:"channel_identity_revision"`
	LocalKeyID              uint                          `json:"local_key_id"`
	KeyGeneration           uint64                        `json:"key_generation"`
	UpstreamGroupID         *uint                         `json:"upstream_group_id"`
	PresenceGeneration      *uint64                       `json:"presence_generation"`
	CanonicalGroupName      string                        `json:"canonical_group_name"`
	UpstreamKeyID           int64                         `json:"upstream_key_id"`
	UpstreamKeyName         string                        `json:"upstream_key_name"`
	OwnershipProvenance     string                        `json:"ownership_provenance"`
	Provisioning            bool                          `json:"provisioning"`
	ProvisioningStartedAt   string                        `json:"provisioning_started_at"`
	LocalOwnershipClaim     string                        `json:"local_ownership_claim"`
	Disposition             ChannelDeletionKeyDisposition `json:"disposition"`
}

func channelDeletionKeyClaimDigest(claim ChannelDeletionKeyClaim) (string, error) {
	startedAt := ""
	if claim.ProvisioningStartedAt != nil {
		startedAt = claim.ProvisioningStartedAt.UTC().Format(time.RFC3339Nano)
	}
	payload := channelDeletionClaimDigestPayload{
		Version:  channelDeletionClaimDigestVersion,
		IntentID: claim.IntentID, PrepareAuditID: claim.PrepareAuditID,
		ChannelID: claim.ChannelID, DeletionEpoch: claim.DeletionEpoch,
		ChannelIdentityRevision: claim.ChannelIdentityRevision,
		LocalKeyID:              claim.LocalKeyID, KeyGeneration: claim.KeyGeneration,
		UpstreamGroupID: claim.UpstreamGroupID, PresenceGeneration: claim.PresenceGeneration,
		CanonicalGroupName: claim.CanonicalGroupName, UpstreamKeyID: claim.UpstreamKeyID,
		UpstreamKeyName:     strings.TrimSpace(claim.UpstreamKeyName),
		OwnershipProvenance: claim.OwnershipProvenance, Provisioning: claim.Provisioning,
		ProvisioningStartedAt: startedAt, LocalOwnershipClaim: claim.LocalOwnershipClaim,
		Disposition: claim.Disposition,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func validChannelDeletionDigest(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func channelDeletionHandledStatus(status ChannelDeletionKeyClaimStatus) bool {
	switch status {
	case ChannelDeletionKeyDeleteConfirmed,
		ChannelDeletionKeyInitiallyAbsentConfirmed,
		ChannelDeletionKeyRetainedAdopted,
		ChannelDeletionKeyRetainedByReview:
		return true
	default:
		return false
	}
}

func newPreparedChannelDeletionKeyClaim(intent ChannelDeletionIntent, audit ChannelDeletionKeyAudit, key GatewayUpstreamKey, at time.Time) (*ChannelDeletionKeyClaim, error) {
	if intent.ID == 0 || audit.ID == 0 || key.ID == 0 ||
		intent.ChannelID != audit.ChannelID || intent.ChannelID != key.ChannelID ||
		intent.DeletionEpoch != audit.DeletionEpoch ||
		intent.ChannelIdentityRevision != audit.ChannelIdentityRevision ||
		audit.LocalKeyID != key.ID {
		return nil, ErrStaleChannelDeletionFence
	}
	claim := &ChannelDeletionKeyClaim{
		IntentID: intent.ID, PrepareAuditID: audit.ID,
		ChannelID: intent.ChannelID, DeletionEpoch: intent.DeletionEpoch,
		ChannelIdentityRevision: intent.ChannelIdentityRevision,
		LocalKeyID:              key.ID, KeyGeneration: key.KeyGeneration,
		UpstreamGroupID: audit.UpstreamGroupID, PresenceGeneration: audit.PresenceGeneration,
		CanonicalGroupName: audit.CanonicalGroupName,
		UpstreamKeyID:      key.UpstreamKeyID, UpstreamKeyName: strings.TrimSpace(key.UpstreamKeyName),
		OwnershipProvenance: audit.OwnershipProvenance, Provisioning: key.Provisioning,
		ProvisioningStartedAt: key.ProvisioningStartedAt,
		LocalOwnershipClaim:   audit.LocalOwnershipClaim,
		Disposition:           audit.Disposition, Status: ChannelDeletionKeyPendingVerification,
		ClaimDigestVersion: channelDeletionClaimDigestVersion,
		CreatedAt:          at.UTC(), UpdatedAt: at.UTC(),
	}
	if claim.Disposition == ChannelDeletionKeyDispositionManualReview ||
		(claim.Disposition == ChannelDeletionKeyDispositionVerifyManagedDelete && !validChannelDeletionDigest(claim.LocalOwnershipClaim)) ||
		claim.UpstreamGroupID == nil || claim.PresenceGeneration == nil || claim.CanonicalGroupName == "" {
		claim.Status = ChannelDeletionKeyManualReviewRequired
		claim.OutcomeReason = "prepared_evidence_incomplete"
		claim.OutcomeActor = "channel_deletion_prepare"
		claim.OutcomeAt = timePointer(at.UTC())
	}
	digest, err := channelDeletionKeyClaimDigest(*claim)
	if err != nil {
		return nil, err
	}
	claim.ClaimDigest = digest
	return claim, nil
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func appendPreparedChannelDeletionKeyClaimTx(tx *gorm.DB, intent ChannelDeletionIntent, audit ChannelDeletionKeyAudit, key GatewayUpstreamKey, at time.Time) error {
	if tx == nil {
		return ErrStaleChannelDeletionFence
	}
	claim, err := newPreparedChannelDeletionKeyClaim(intent, audit, key, at)
	if err != nil {
		return err
	}
	updates := map[string]any{
		"enabled":                              false,
		"channel_deletion_intent_id":           intent.ID,
		"channel_deletion_epoch":               intent.DeletionEpoch,
		"channel_deletion_identity_revision":   intent.ChannelIdentityRevision,
		"channel_deletion_upstream_group_id":   claim.UpstreamGroupID,
		"channel_deletion_presence_generation": claim.PresenceGeneration,
		"updated_at":                           at.UTC(),
	}
	result := tx.Model(&GatewayUpstreamKey{}).
		Where("id = ? AND channel_id = ? AND key_generation = ? AND channel_deletion_intent_id IS NULL", key.ID, key.ChannelID, key.KeyGeneration).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrChannelDeletionKeyFreezeChanged
	}
	return tx.Create(claim).Error
}

func (r *Channels) ListPendingChannelDeletionIntents(ctx context.Context, limit int) ([]ChannelDeletionIntent, error) {
	if r == nil || r.db == nil || ctx == nil {
		return nil, errors.New("channel deletion intent store is unavailable")
	}
	if limit <= 0 {
		limit = channelDeletionDefaultListLimit
	}
	if limit > channelDeletionMaximumListLimit {
		limit = channelDeletionMaximumListLimit
	}
	intents := make([]ChannelDeletionIntent, 0)
	err := r.db.WithContext(ctx).
		Where("status IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)", []ChannelDeletionIntentStatus{
			ChannelDeletionIntentPendingRemoteCleanup,
			ChannelDeletionIntentManualReviewRequired,
			ChannelDeletionIntentReadyForFinalization,
		}).
		Order("COALESCE(next_attempt_at, requested_at) ASC, id ASC").
		Limit(limit).Find(&intents).Error
	return intents, err
}

func (r *Channels) ListChannelDeletionKeyWork(ctx context.Context, intentID uint64) ([]ChannelDeletionKeyClaim, error) {
	if r == nil || r.db == nil || ctx == nil || intentID == 0 {
		return nil, errors.New("channel deletion key work store is unavailable")
	}
	claims := make([]ChannelDeletionKeyClaim, 0)
	err := r.db.WithContext(ctx).Where("intent_id = ?", intentID).Order("id ASC").Find(&claims).Error
	return claims, err
}

// RequireRetiredChannelDeletionDomains rechecks the retirement committed by
// deletion prepare. It never repairs drift: a restored rate, present group, or
// newly enabled/unclaimed key must stop remote cleanup rather than be hidden by
// a coordinator retry.
func (r *Channels) RequireRetiredChannelDeletionDomains(ctx context.Context, fence ChannelDeletionFence) error {
	if r == nil || r.db == nil || ctx == nil || !fence.valid() {
		return ErrStaleChannelDeletionFence
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, _, err := requireChannelDeletionFenceTx(tx, fence); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		if err := lockAllRateSnapshotPresenceDomainsForChannelTx(tx, fence.ChannelID); err != nil {
			return err
		}
		var groups []GatewayUpstreamGroup
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("channel_id = ?", fence.ChannelID).
			Order("canonical_group_name ASC, id ASC").Find(&groups).Error; err != nil {
			return err
		}
		claims := make([]ChannelDeletionKeyClaim, 0)
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("intent_id = ?", fence.IntentID).Order("id ASC").Find(&claims).Error; err != nil {
			return err
		}
		var preparedCount, rateCount, presentGroupCount, keyCount, frozenKeyCount int64
		if err := tx.Model(&ChannelDeletionKeyAudit{}).
			Where("intent_id = ? AND channel_id = ? AND deletion_epoch = ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch).
			Count(&preparedCount).Error; err != nil {
			return err
		}
		if err := tx.Model(&RateSnapshot{}).Where("channel_id = ?", fence.ChannelID).Count(&rateCount).Error; err != nil {
			return err
		}
		if err := tx.Model(&GatewayUpstreamGroup{}).
			Where("channel_id = ? AND present = ?", fence.ChannelID, true).
			Count(&presentGroupCount).Error; err != nil {
			return err
		}
		if err := tx.Model(&GatewayUpstreamKey{}).Where("channel_id = ?", fence.ChannelID).Count(&keyCount).Error; err != nil {
			return err
		}
		if err := tx.Model(&GatewayUpstreamKey{}).
			Where(
				"channel_id = ? AND enabled = ? AND channel_deletion_intent_id = ? AND channel_deletion_epoch = ? AND channel_deletion_identity_revision = ?",
				fence.ChannelID, false, fence.IntentID, fence.DeletionEpoch, fence.IdentityRevision,
			).
			Count(&frozenKeyCount).Error; err != nil {
			return err
		}
		if preparedCount != int64(len(claims)) || rateCount != 0 || presentGroupCount != 0 ||
			keyCount != int64(len(claims)) || frozenKeyCount != keyCount {
			return ErrChannelDeletionKeyFreezeChanged
		}
		for index := range claims {
			claim := &claims[index]
			groupEvidenceEmpty := claim.UpstreamGroupID == nil && claim.PresenceGeneration == nil && claim.CanonicalGroupName == ""
			groupEvidenceComplete := claim.UpstreamGroupID != nil && claim.PresenceGeneration != nil && claim.CanonicalGroupName != ""
			if groupEvidenceEmpty {
				if claim.Status != ChannelDeletionKeyManualReviewRequired && claim.Status != ChannelDeletionKeyRetainedByReview {
					return ErrChannelDeletionKeyFreezeChanged
				}
				if _, err := requireFrozenChannelDeletionLocalKeyTx(tx, fence, claim); err != nil {
					return err
				}
				continue
			}
			if !groupEvidenceComplete {
				return ErrChannelDeletionKeyFreezeChanged
			}
			if _, err := requireFrozenChannelDeletionKeyTx(tx, fence, claim); err != nil {
				return err
			}
		}
		return nil
	})
}

func requireChannelDeletionFenceTx(tx *gorm.DB, fence ChannelDeletionFence) (Channel, ChannelDeletionIntent, error) {
	var intent ChannelDeletionIntent
	if tx == nil || !fence.valid() {
		return Channel{}, intent, ErrStaleChannelDeletionFence
	}
	channel, err := requirePendingChannelDeletionFenceTx(tx, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision)
	if err != nil {
		return channel, intent, err
	}
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND channel_id = ? AND deletion_epoch = ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch).
		First(&intent).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return channel, intent, ErrChannelDeletionIntentMissing
	}
	if err != nil {
		return channel, intent, err
	}
	if intent.ChannelIdentityRevision != fence.IdentityRevision ||
		intent.ChannelNameSnapshot != channel.Name || intent.ChannelTypeSnapshot != channel.Type ||
		intent.Status == ChannelDeletionIntentFinalized ||
		channel.DeletionState != string(intent.Status) {
		return channel, intent, ErrStaleChannelDeletionFence
	}
	return channel, intent, nil
}

func loadChannelDeletionKeyClaimTx(tx *gorm.DB, fence ChannelDeletionFence, keyClaimID uint64) (ChannelDeletionKeyClaim, error) {
	var claim ChannelDeletionKeyClaim
	if tx == nil || !fence.valid() || keyClaimID == 0 {
		return claim, ErrChannelDeletionKeyClaimLost
	}
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where(
			"id = ? AND intent_id = ? AND channel_id = ? AND deletion_epoch = ? AND channel_identity_revision = ?",
			keyClaimID, fence.IntentID, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision,
		).First(&claim).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return claim, ErrChannelDeletionKeyClaimLost
	}
	if err != nil {
		return claim, err
	}
	expectedDigest, err := channelDeletionKeyClaimDigest(claim)
	if err != nil || claim.ClaimDigestVersion != channelDeletionClaimDigestVersion || claim.ClaimDigest != expectedDigest {
		return claim, ErrChannelDeletionKeyFreezeChanged
	}
	return claim, nil
}

func requireFrozenChannelDeletionKeyTx(tx *gorm.DB, fence ChannelDeletionFence, claim *ChannelDeletionKeyClaim) (*GatewayUpstreamKey, error) {
	if tx == nil || claim == nil || !fence.valid() || claim.IntentID != fence.IntentID ||
		claim.ChannelID != fence.ChannelID || claim.DeletionEpoch != fence.DeletionEpoch ||
		claim.ChannelIdentityRevision != fence.IdentityRevision || claim.UpstreamGroupID == nil ||
		claim.PresenceGeneration == nil || strings.TrimSpace(claim.CanonicalGroupName) == "" {
		return nil, ErrChannelDeletionKeyFreezeChanged
	}
	canonical, err := canonicalUpstreamGroupName(tx, claim.CanonicalGroupName)
	if err != nil || canonical != claim.CanonicalGroupName {
		return nil, ErrChannelDeletionKeyFreezeChanged
	}
	if _, err := lockRateSnapshotPresenceDomainTx(tx, fence.ChannelID, canonical); err != nil {
		return nil, err
	}
	var group GatewayUpstreamGroup
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND channel_id = ?", *claim.UpstreamGroupID, fence.ChannelID).
		First(&group).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChannelDeletionKeyFreezeChanged
		}
		return nil, err
	}
	if group.CanonicalGroupName != canonical || group.PresenceGeneration != *claim.PresenceGeneration || group.Present {
		return nil, ErrChannelDeletionKeyFreezeChanged
	}
	var rateCount int64
	if err := tx.Model(&RateSnapshot{}).
		Where("channel_id = ? AND lower(btrim(model_name)) = ?", fence.ChannelID, canonical).
		Count(&rateCount).Error; err != nil {
		return nil, err
	}
	if rateCount != 0 {
		return nil, ErrChannelDeletionKeyFreezeChanged
	}

	return requireFrozenChannelDeletionLocalKeyTx(tx, fence, claim)
}

func requireFrozenChannelDeletionLocalKeyTx(tx *gorm.DB, fence ChannelDeletionFence, claim *ChannelDeletionKeyClaim) (*GatewayUpstreamKey, error) {
	if tx == nil || claim == nil || !fence.valid() || claim.IntentID != fence.IntentID ||
		claim.ChannelID != fence.ChannelID || claim.DeletionEpoch != fence.DeletionEpoch ||
		claim.ChannelIdentityRevision != fence.IdentityRevision {
		return nil, ErrChannelDeletionKeyFreezeChanged
	}
	var key GatewayUpstreamKey
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND channel_id = ?", claim.LocalKeyID, fence.ChannelID).
		First(&key).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChannelDeletionKeyFreezeChanged
		}
		return nil, err
	}
	groupMatches := false
	if claim.UpstreamGroupID == nil && claim.PresenceGeneration == nil && strings.TrimSpace(claim.CanonicalGroupName) == "" {
		groupMatches = strings.TrimSpace(key.GroupName) == "" &&
			key.ChannelDeletionUpstreamGroupID == nil && key.ChannelDeletionPresenceGeneration == nil
	} else if claim.UpstreamGroupID != nil && claim.PresenceGeneration != nil && strings.TrimSpace(claim.CanonicalGroupName) != "" {
		keyCanonical, err := canonicalUpstreamGroupName(tx, key.GroupName)
		groupMatches = err == nil && keyCanonical == claim.CanonicalGroupName &&
			sameOptionalUint(key.ChannelDeletionUpstreamGroupID, *claim.UpstreamGroupID) &&
			sameOptionalUint64(key.ChannelDeletionPresenceGeneration, *claim.PresenceGeneration)
	}
	if !groupMatches || key.KeyGeneration != claim.KeyGeneration ||
		key.UpstreamKeyID != claim.UpstreamKeyID || strings.TrimSpace(key.UpstreamKeyName) != claim.UpstreamKeyName ||
		key.OwnershipProvenance != claim.OwnershipProvenance || key.Provisioning != claim.Provisioning ||
		!sameOptionalTime(key.ProvisioningStartedAt, claim.ProvisioningStartedAt) || key.Enabled ||
		!sameOptionalUint64(key.ChannelDeletionIntentID, fence.IntentID) ||
		!sameOptionalUint64(key.ChannelDeletionEpoch, fence.DeletionEpoch) ||
		!sameOptionalUint64(key.ChannelDeletionIdentityRevision, fence.IdentityRevision) {
		return nil, ErrChannelDeletionKeyFreezeChanged
	}
	if claim.Disposition == ChannelDeletionKeyDispositionVerifyManagedDelete {
		currentOwnershipClaim, claimErr := ChannelDeletionManagedOwnershipClaimDigest(key)
		if claimErr != nil || !validChannelDeletionDigest(claim.LocalOwnershipClaim) || currentOwnershipClaim != claim.LocalOwnershipClaim {
			return nil, ErrChannelDeletionKeyFreezeChanged
		}
	}
	return &key, nil
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func sameOptionalUint(value *uint, expected uint) bool {
	return value != nil && *value == expected
}

func sameOptionalUint64(value *uint64, expected uint64) bool {
	return value != nil && *value == expected
}

// ClaimChannelDeletionKey takes over pending, uncertain, or expired work by
// advancing its claim epoch. The returned LocalKey contains the still-frozen
// ciphertext and prefix required for ownership verification. No remote work is
// performed while this transaction is open.
func (r *Channels) ClaimChannelDeletionKey(ctx context.Context, fence ChannelDeletionFence, keyClaimID uint64, staleAfter time.Duration) (*ChannelDeletionKeyClaim, bool, error) {
	if r == nil || r.db == nil || ctx == nil || !fence.valid() || keyClaimID == 0 || staleAfter <= 0 {
		return nil, false, errors.New("channel deletion key claim is invalid")
	}
	var claimed ChannelDeletionKeyClaim
	acquired := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, _, err := requireChannelDeletionFenceTx(tx, fence); err != nil {
			return err
		}
		claim, err := loadChannelDeletionKeyClaimTx(tx, fence, keyClaimID)
		if err != nil {
			return err
		}
		now, err := channelDeletionDatabaseNowTx(tx)
		if err != nil {
			return err
		}
		previousStatus := claim.Status
		switch claim.Status {
		case ChannelDeletionKeyPendingVerification, ChannelDeletionKeyProcessingUnknown:
		case ChannelDeletionKeyProcessing:
			if claim.LeaseExpiresAt != nil && claim.LeaseExpiresAt.After(now) {
				claimed = claim
				return nil
			}
		default:
			claimed = claim
			return nil
		}
		if claim.ClaimEpoch >= channelDeletionMaxClaimEpoch {
			return errors.New("channel deletion key claim epoch is exhausted")
		}
		localKey, err := requireFrozenChannelDeletionKeyTx(tx, fence, &claim)
		if err != nil {
			return err
		}
		nextEpoch := claim.ClaimEpoch + 1
		leaseExpiresAt := now.Add(staleAfter).UTC()
		result := tx.Model(&ChannelDeletionKeyClaim{}).
			Where("id = ? AND status = ? AND claim_epoch = ?", claim.ID, claim.Status, claim.ClaimEpoch).
			Updates(map[string]any{
				"status":           ChannelDeletionKeyProcessing,
				"claim_epoch":      nextEpoch,
				"claimed_at":       now,
				"lease_expires_at": leaseExpiresAt,
				"outcome_reason":   "",
				"outcome_actor":    "",
				"outcome_at":       nil,
				"last_error":       "",
				"updated_at":       now,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelDeletionKeyClaimLost
		}
		claim.Status = ChannelDeletionKeyProcessing
		claim.ClaimEpoch = nextEpoch
		claim.ClaimedAt = timePointer(now)
		claim.LeaseExpiresAt = timePointer(leaseExpiresAt)
		claim.OutcomeReason = ""
		claim.OutcomeActor = ""
		claim.OutcomeAt = nil
		claim.LastError = ""
		claim.UpdatedAt = now
		claim.LocalKey = localKey
		eventKind := "claim_acquired"
		if previousStatus != ChannelDeletionKeyPendingVerification || nextEpoch > 1 {
			eventKind = "claim_recovered"
		}
		if err := appendChannelDeletionKeyEvidenceTx(tx, claim, previousStatus, eventKind, "", channelDeletionCoordinatorActor, "", now); err != nil {
			return err
		}
		if err := tx.Model(&ChannelDeletionIntent{}).
			Where("id = ? AND channel_id = ? AND deletion_epoch = ? AND status <> ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch, ChannelDeletionIntentFinalized).
			Updates(map[string]any{
				"attempt_count":   gorm.Expr("attempt_count + 1"),
				"last_attempt_at": now,
				"next_attempt_at": nil,
				"updated_at":      now,
			}).Error; err != nil {
			return err
		}
		claimed = claim
		acquired = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return &claimed, acquired, nil
}

// RequireChannelDeletionKeyClaim is the immediate pre-DELETE fence. It does
// not renew or mutate the lease and must be called after remote verification,
// directly before the destructive request.
func (r *Channels) RequireChannelDeletionKeyClaim(ctx context.Context, fence ChannelDeletionKeyClaimFence) (*ChannelDeletionKeyClaim, error) {
	if r == nil || r.db == nil || ctx == nil || !fence.valid() {
		return nil, ErrChannelDeletionKeyClaimLost
	}
	var required ChannelDeletionKeyClaim
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, _, err := requireChannelDeletionFenceTx(tx, fence.ChannelDeletionFence); err != nil {
			return err
		}
		claim, err := loadChannelDeletionKeyClaimTx(tx, fence.ChannelDeletionFence, fence.KeyClaimID)
		if err != nil {
			return err
		}
		databaseNow, err := channelDeletionDatabaseNowTx(tx)
		if err != nil {
			return err
		}
		if claim.Status != ChannelDeletionKeyProcessing || claim.ClaimEpoch != fence.ClaimEpoch ||
			claim.LeaseExpiresAt == nil || !claim.LeaseExpiresAt.After(databaseNow) {
			return ErrChannelDeletionKeyClaimLost
		}
		localKey, err := requireFrozenChannelDeletionKeyTx(tx, fence.ChannelDeletionFence, &claim)
		if err != nil {
			return err
		}
		claim.LocalKey = localKey
		required = claim
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &required, nil
}

// RenewChannelDeletionKeyClaim extends the exact processing epoch immediately
// before a bounded remote DELETE. Database time decides expiry so instances
// with skewed clocks cannot overlap destructive ownership.
func (r *Channels) RenewChannelDeletionKeyClaim(ctx context.Context, fence ChannelDeletionKeyClaimFence, leaseDuration, minimumRemaining time.Duration) (*ChannelDeletionKeyClaim, error) {
	if r == nil || r.db == nil || ctx == nil || !fence.valid() || leaseDuration <= minimumRemaining || minimumRemaining <= 0 {
		return nil, ErrChannelDeletionKeyClaimLost
	}
	var renewed ChannelDeletionKeyClaim
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, _, err := requireChannelDeletionFenceTx(tx, fence.ChannelDeletionFence); err != nil {
			return err
		}
		claim, err := loadChannelDeletionKeyClaimTx(tx, fence.ChannelDeletionFence, fence.KeyClaimID)
		if err != nil {
			return err
		}
		databaseNow, err := channelDeletionDatabaseNowTx(tx)
		if err != nil {
			return err
		}
		if claim.Status != ChannelDeletionKeyProcessing || claim.ClaimEpoch != fence.ClaimEpoch ||
			claim.LeaseExpiresAt == nil || !claim.LeaseExpiresAt.After(databaseNow) {
			return ErrChannelDeletionKeyClaimLost
		}
		localKey, err := requireFrozenChannelDeletionKeyTx(tx, fence.ChannelDeletionFence, &claim)
		if err != nil {
			return err
		}
		leaseExpiresAt := databaseNow.Add(leaseDuration).UTC()
		if !leaseExpiresAt.After(databaseNow.Add(minimumRemaining)) {
			return ErrChannelDeletionKeyClaimLost
		}
		result := tx.Model(&ChannelDeletionKeyClaim{}).
			Where("id = ? AND status = ? AND claim_epoch = ?", claim.ID, ChannelDeletionKeyProcessing, fence.ClaimEpoch).
			Updates(map[string]any{"lease_expires_at": leaseExpiresAt, "updated_at": databaseNow})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelDeletionKeyClaimLost
		}
		claim.LeaseExpiresAt = timePointer(leaseExpiresAt)
		claim.UpdatedAt = databaseNow
		claim.LocalKey = localKey
		if err := appendChannelDeletionKeyEvidenceTx(tx, claim, claim.Status, "claim_renewed", "delete_deadline_lease", channelDeletionCoordinatorActor, "", databaseNow); err != nil {
			return err
		}
		renewed = claim
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &renewed, nil
}

// BindChannelDeletionObservedRemoteID makes the first exact remote identity a
// durable part of the processing claim before any reveal or DELETE call. This
// is essential for provisioning claims whose frozen upstream_key_id is zero:
// retries must never follow a reused name to a different remote key.
func (r *Channels) BindChannelDeletionObservedRemoteID(ctx context.Context, fence ChannelDeletionKeyClaimFence, observedRemoteID int64) error {
	if r == nil || r.db == nil || ctx == nil || !fence.valid() || observedRemoteID <= 0 {
		return ErrChannelDeletionKeyClaimLost
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, _, err := requireChannelDeletionFenceTx(tx, fence.ChannelDeletionFence); err != nil {
			return err
		}
		claim, err := loadChannelDeletionKeyClaimTx(tx, fence.ChannelDeletionFence, fence.KeyClaimID)
		if err != nil {
			return err
		}
		if claim.Status != ChannelDeletionKeyProcessing || claim.ClaimEpoch != fence.ClaimEpoch {
			return ErrChannelDeletionKeyClaimLost
		}
		if _, err := requireFrozenChannelDeletionKeyTx(tx, fence.ChannelDeletionFence, &claim); err != nil {
			return err
		}
		if claim.ObservedRemoteID != 0 {
			if claim.ObservedRemoteID != observedRemoteID {
				return ErrChannelDeletionKeyFreezeChanged
			}
			return nil
		}
		databaseNow, err := channelDeletionDatabaseNowTx(tx)
		if err != nil {
			return err
		}
		result := tx.Model(&ChannelDeletionKeyClaim{}).
			Where("id = ? AND status = ? AND claim_epoch = ? AND observed_remote_id = 0", claim.ID, ChannelDeletionKeyProcessing, fence.ClaimEpoch).
			Updates(map[string]any{"observed_remote_id": observedRemoteID, "updated_at": databaseNow})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelDeletionKeyClaimLost
		}
		claim.ObservedRemoteID = observedRemoteID
		claim.UpdatedAt = databaseNow
		return appendChannelDeletionKeyEvidenceTx(
			tx, claim, claim.Status, "remote_identity_bound", "remote_identity_observed",
			channelDeletionCoordinatorActor, "", databaseNow,
		)
	})
}

// PostgreSQL CURRENT_TIMESTAMP is fixed at transaction start. Lease decisions
// need wall-clock database time after row locks have been acquired, otherwise a
// long transaction can renew or take over work using an already-stale instant.
func channelDeletionDatabaseNowTx(tx *gorm.DB) (time.Time, error) {
	if tx == nil {
		return time.Time{}, errors.New("channel deletion database clock is unavailable")
	}
	query := "SELECT CURRENT_TIMESTAMP"
	if tx.Dialector != nil && tx.Dialector.Name() == "postgres" {
		query = "SELECT clock_timestamp()"
	}
	var databaseNow time.Time
	if err := tx.Raw(query).Scan(&databaseNow).Error; err != nil {
		return time.Time{}, err
	}
	if databaseNow.IsZero() {
		return time.Time{}, errors.New("channel deletion database clock returned zero")
	}
	return databaseNow.UTC(), nil
}

// RetryChannelDeletionKeyClaim releases a pre-DELETE processing claim back to
// the durable queue. It is never used after DELETE may have been sent; those
// outcomes remain processing_unknown until absence is proven.
func (r *Channels) RetryChannelDeletionKeyClaim(ctx context.Context, fence ChannelDeletionKeyClaimFence, reason, actor string, at time.Time) error {
	reason = strings.TrimSpace(reason)
	actor = strings.TrimSpace(actor)
	if r == nil || r.db == nil || ctx == nil || !fence.valid() || reason == "" || actor == "" {
		return ErrChannelDeletionKeyClaimLost
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, _, err := requireChannelDeletionFenceTx(tx, fence.ChannelDeletionFence); err != nil {
			return err
		}
		claim, err := loadChannelDeletionKeyClaimTx(tx, fence.ChannelDeletionFence, fence.KeyClaimID)
		if err != nil {
			return err
		}
		if claim.Status != ChannelDeletionKeyProcessing || claim.ClaimEpoch != fence.ClaimEpoch {
			return ErrChannelDeletionKeyClaimLost
		}
		if _, err := requireFrozenChannelDeletionKeyTx(tx, fence.ChannelDeletionFence, &claim); err != nil {
			return err
		}
		result := tx.Model(&ChannelDeletionKeyClaim{}).
			Where("id = ? AND status = ? AND claim_epoch = ?", claim.ID, ChannelDeletionKeyProcessing, fence.ClaimEpoch).
			Updates(map[string]any{
				"status":           ChannelDeletionKeyPendingVerification,
				"lease_expires_at": nil,
				"last_error":       reason,
				"updated_at":       at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelDeletionKeyClaimLost
		}
		previous := claim.Status
		claim.Status = ChannelDeletionKeyPendingVerification
		claim.LeaseExpiresAt = nil
		claim.LastError = reason
		claim.UpdatedAt = at
		if err := appendChannelDeletionKeyEvidenceTx(tx, claim, previous, "retry_scheduled", reason, actor, reason, at); err != nil {
			return err
		}
		return recomputeChannelDeletionIntentStateTx(tx, fence.ChannelDeletionFence, at)
	})
}

func normalizeChannelDeletionKeyOutcome(claim ChannelDeletionKeyClaim, outcome ChannelDeletionKeyOutcome) (ChannelDeletionKeyOutcome, error) {
	outcome.Reason = strings.TrimSpace(outcome.Reason)
	outcome.Actor = strings.TrimSpace(outcome.Actor)
	outcome.EvidenceDigest = strings.ToLower(strings.TrimSpace(outcome.EvidenceDigest))
	outcome.OwnershipDigest = strings.ToLower(strings.TrimSpace(outcome.OwnershipDigest))
	outcome.ObservedFingerprint = strings.ToLower(strings.TrimSpace(outcome.ObservedFingerprint))
	outcome.InventoryDigest = strings.ToLower(strings.TrimSpace(outcome.InventoryDigest))
	if outcome.ObservedAt.IsZero() {
		outcome.ObservedAt = time.Now().UTC()
	} else {
		outcome.ObservedAt = outcome.ObservedAt.UTC()
	}
	if outcome.Actor == "" || outcome.Reason == "" || outcome.ObservedRemoteID < 0 ||
		!validChannelDeletionDigest(outcome.EvidenceDigest) || outcome.EvidenceDigestVersion == 0 ||
		!validChannelDeletionDigest(outcome.OwnershipDigest) || outcome.OwnershipDigestVersion == 0 {
		return outcome, errors.New("channel deletion key outcome evidence is incomplete")
	}
	if (outcome.ObservedFingerprint == "") != (outcome.FingerprintDigestVersion == 0) ||
		(outcome.ObservedFingerprint != "" && !validChannelDeletionDigest(outcome.ObservedFingerprint)) ||
		(outcome.InventoryDigest == "") != (outcome.InventoryDigestVersion == 0) ||
		(outcome.InventoryDigest != "" && !validChannelDeletionDigest(outcome.InventoryDigest)) {
		return outcome, errors.New("channel deletion key outcome optional digest is invalid")
	}
	switch outcome.Status {
	case ChannelDeletionKeyDeleteConfirmed, ChannelDeletionKeyInitiallyAbsentConfirmed:
		if claim.Disposition != ChannelDeletionKeyDispositionVerifyManagedDelete {
			return outcome, errors.New("only a verified managed key can have a deletion outcome")
		}
	case ChannelDeletionKeyRetainedAdopted:
		if claim.Disposition != ChannelDeletionKeyDispositionRetainAdopted {
			return outcome, errors.New("only an adopted key can have a retained outcome")
		}
	case ChannelDeletionKeyRetainedByReview:
		// Deterministic cleanup policy retains any key whose ownership cannot be
		// proven strongly enough for deletion. The persisted status name remains
		// compatible with existing databases and final-audit projections.
	case ChannelDeletionKeyProcessingUnknown:
		if claim.Disposition != ChannelDeletionKeyDispositionVerifyManagedDelete {
			return outcome, errors.New("only a managed key deletion can remain uncertain")
		}
	case ChannelDeletionKeyManualReviewRequired:
	default:
		return outcome, errors.New("invalid channel deletion key outcome")
	}
	return outcome, nil
}

// MarkChannelDeletionKeyOutcome is the post-remote-operation commit fence. It
// advances only the exact processing claim epoch and rechecks the disabled
// local key plus group generation before recording immutable evidence.
func (r *Channels) MarkChannelDeletionKeyOutcome(ctx context.Context, fence ChannelDeletionKeyClaimFence, outcome ChannelDeletionKeyOutcome) error {
	if r == nil || r.db == nil || ctx == nil || !fence.valid() {
		return ErrChannelDeletionKeyClaimLost
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, _, err := requireChannelDeletionFenceTx(tx, fence.ChannelDeletionFence); err != nil {
			return err
		}
		claim, err := loadChannelDeletionKeyClaimTx(tx, fence.ChannelDeletionFence, fence.KeyClaimID)
		if err != nil {
			return err
		}
		if claim.Status != ChannelDeletionKeyProcessing || claim.ClaimEpoch != fence.ClaimEpoch {
			return ErrChannelDeletionKeyClaimLost
		}
		if _, err := requireFrozenChannelDeletionKeyTx(tx, fence.ChannelDeletionFence, &claim); err != nil {
			return err
		}
		outcome, err = normalizeChannelDeletionKeyOutcome(claim, outcome)
		if err != nil {
			return err
		}
		evidenceVersion := outcome.EvidenceDigestVersion
		ownershipVersion := outcome.OwnershipDigestVersion
		updates := map[string]any{
			"status":                     outcome.Status,
			"lease_expires_at":           nil,
			"observed_remote_id":         outcome.ObservedRemoteID,
			"evidence_digest":            outcome.EvidenceDigest,
			"evidence_digest_version":    evidenceVersion,
			"ownership_digest":           outcome.OwnershipDigest,
			"ownership_digest_version":   ownershipVersion,
			"observed_fingerprint":       outcome.ObservedFingerprint,
			"fingerprint_digest_version": nullableDigestVersion(outcome.ObservedFingerprint, outcome.FingerprintDigestVersion),
			"inventory_digest":           outcome.InventoryDigest,
			"inventory_digest_version":   nullableDigestVersion(outcome.InventoryDigest, outcome.InventoryDigestVersion),
			"outcome_reason":             outcome.Reason,
			"outcome_actor":              outcome.Actor,
			"outcome_at":                 outcome.ObservedAt,
			"last_error":                 strings.TrimSpace(outcome.ErrorSummary),
			"updated_at":                 outcome.ObservedAt,
		}
		result := tx.Model(&ChannelDeletionKeyClaim{}).
			Where("id = ? AND status = ? AND claim_epoch = ?", claim.ID, ChannelDeletionKeyProcessing, fence.ClaimEpoch).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelDeletionKeyClaimLost
		}
		before := claim.Status
		claim.Status = outcome.Status
		claim.LeaseExpiresAt = nil
		claim.ObservedRemoteID = outcome.ObservedRemoteID
		claim.EvidenceDigest = outcome.EvidenceDigest
		claim.EvidenceDigestVersion = uint16Pointer(evidenceVersion)
		claim.OwnershipDigest = outcome.OwnershipDigest
		claim.OwnershipDigestVersion = uint16Pointer(ownershipVersion)
		claim.ObservedFingerprint = outcome.ObservedFingerprint
		claim.FingerprintDigestVersion = nullableDigestVersionPointer(outcome.ObservedFingerprint, outcome.FingerprintDigestVersion)
		claim.InventoryDigest = outcome.InventoryDigest
		claim.InventoryDigestVersion = nullableDigestVersionPointer(outcome.InventoryDigest, outcome.InventoryDigestVersion)
		claim.OutcomeReason = outcome.Reason
		claim.OutcomeActor = outcome.Actor
		claim.OutcomeAt = timePointer(outcome.ObservedAt)
		claim.LastError = strings.TrimSpace(outcome.ErrorSummary)
		claim.UpdatedAt = outcome.ObservedAt
		if err := appendChannelDeletionKeyEvidenceTx(tx, claim, before, "outcome_recorded", outcome.Reason, outcome.Actor, outcome.ErrorSummary, outcome.ObservedAt); err != nil {
			return err
		}
		return recomputeChannelDeletionIntentStateTx(tx, fence.ChannelDeletionFence, outcome.ObservedAt)
	})
}

func nullableDigestVersion(digest string, version uint16) any {
	if strings.TrimSpace(digest) == "" {
		return nil
	}
	return version
}

func nullableDigestVersionPointer(digest string, version uint16) *uint16 {
	if strings.TrimSpace(digest) == "" {
		return nil
	}
	return uint16Pointer(version)
}

func appendChannelDeletionKeyEvidenceTx(tx *gorm.DB, claim ChannelDeletionKeyClaim, before ChannelDeletionKeyClaimStatus, eventKind, reason, actor, errorSummary string, observedAt time.Time) error {
	if tx == nil || claim.ID == 0 || claim.ClaimEpoch == 0 || strings.TrimSpace(actor) == "" {
		return ErrChannelDeletionKeyClaimLost
	}
	evidence := ChannelDeletionKeyEvidence{
		KeyClaimID: claim.ID, IntentID: claim.IntentID,
		ChannelID: claim.ChannelID, DeletionEpoch: claim.DeletionEpoch,
		ChannelIdentityRevision: claim.ChannelIdentityRevision,
		LocalKeyID:              claim.LocalKeyID, KeyGeneration: claim.KeyGeneration,
		UpstreamGroupID: claim.UpstreamGroupID, PresenceGeneration: claim.PresenceGeneration,
		CanonicalGroupName: claim.CanonicalGroupName,
		UpstreamKeyID:      claim.UpstreamKeyID, ObservedRemoteID: claim.ObservedRemoteID,
		UpstreamKeyName:     claim.UpstreamKeyName,
		LocalOwnershipClaim: claim.LocalOwnershipClaim,
		ClaimEpoch:          claim.ClaimEpoch, EventKind: eventKind,
		StatusBefore: before, StatusAfter: claim.Status,
		ClaimDigest: claim.ClaimDigest, ClaimDigestVersion: claim.ClaimDigestVersion,
		EvidenceDigest: claim.EvidenceDigest, EvidenceDigestVersion: claim.EvidenceDigestVersion,
		OwnershipDigest: claim.OwnershipDigest, OwnershipDigestVersion: claim.OwnershipDigestVersion,
		ObservedFingerprint: claim.ObservedFingerprint, FingerprintDigestVersion: claim.FingerprintDigestVersion,
		InventoryDigest: claim.InventoryDigest, InventoryDigestVersion: claim.InventoryDigestVersion,
		Reason: strings.TrimSpace(reason), Actor: strings.TrimSpace(actor),
		ErrorSummary: strings.TrimSpace(errorSummary), ObservedAt: observedAt.UTC(), CreatedAt: observedAt.UTC(),
	}
	return tx.Create(&evidence).Error
}

func recomputeChannelDeletionIntentStateTx(tx *gorm.DB, fence ChannelDeletionFence, at time.Time) error {
	if tx == nil || !fence.valid() {
		return ErrStaleChannelDeletionFence
	}
	var claims []ChannelDeletionKeyClaim
	if err := tx.Where("intent_id = ?", fence.IntentID).Order("id ASC").Find(&claims).Error; err != nil {
		return err
	}
	status := ChannelDeletionIntentReadyForFinalization
	lastError := ""
	for _, claim := range claims {
		if claim.Status == ChannelDeletionKeyManualReviewRequired {
			status = ChannelDeletionIntentManualReviewRequired
			lastError = strings.TrimSpace(claim.OutcomeReason)
			if lastError == "" {
				lastError = "channel deletion key requires manual review"
			}
			break
		}
		if !channelDeletionHandledStatus(claim.Status) {
			status = ChannelDeletionIntentPendingRemoteCleanup
			if lastError == "" {
				lastError = strings.TrimSpace(claim.LastError)
			}
		}
	}
	result := tx.Model(&ChannelDeletionIntent{}).
		Where("id = ? AND channel_id = ? AND deletion_epoch = ? AND channel_identity_revision = ? AND status <> ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision, ChannelDeletionIntentFinalized).
		Updates(map[string]any{
			"status":          status,
			"last_error":      lastError,
			"next_attempt_at": nil,
			"updated_at":      at.UTC(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrStaleChannelDeletionFence
	}
	result = tx.Model(&Channel{}).
		Where("id = ? AND deletion_pending = ? AND deletion_epoch = ? AND deletion_identity_revision = ?", fence.ChannelID, true, fence.DeletionEpoch, fence.IdentityRevision).
		Updates(map[string]any{"deletion_state": status, "updated_at": at.UTC()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrStaleChannelDeletionFence
	}
	return nil
}

// MarkChannelDeletionIntentState records channel-wide inventory/retry failures
// that occur before one key can be claimed. Ready/manual states are accepted
// only when the current key ledger actually supports them.
func (r *Channels) MarkChannelDeletionIntentState(ctx context.Context, fence ChannelDeletionFence, status ChannelDeletionIntentStatus, lastError string, nextAttemptAt *time.Time, at time.Time) error {
	if r == nil || r.db == nil || ctx == nil || !fence.valid() || status == ChannelDeletionIntentFinalized {
		return ErrStaleChannelDeletionFence
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	if nextAttemptAt != nil {
		next := nextAttemptAt.UTC()
		nextAttemptAt = &next
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, _, err := requireChannelDeletionFenceTx(tx, fence); err != nil {
			return err
		}
		var claims []ChannelDeletionKeyClaim
		if err := tx.Where("intent_id = ?", fence.IntentID).Find(&claims).Error; err != nil {
			return err
		}
		hasManual := false
		allHandled := true
		for _, claim := range claims {
			hasManual = hasManual || claim.Status == ChannelDeletionKeyManualReviewRequired
			allHandled = allHandled && channelDeletionHandledStatus(claim.Status)
		}
		if status == ChannelDeletionIntentManualReviewRequired && !hasManual {
			return ErrChannelDeletionFinalizationBlocked
		}
		if status == ChannelDeletionIntentReadyForFinalization && !allHandled {
			return ErrChannelDeletionFinalizationBlocked
		}
		result := tx.Model(&ChannelDeletionIntent{}).
			Where("id = ? AND channel_id = ? AND deletion_epoch = ? AND channel_identity_revision = ? AND status <> ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision, ChannelDeletionIntentFinalized).
			Updates(map[string]any{
				"status":          status,
				"last_error":      strings.TrimSpace(lastError),
				"attempt_count":   gorm.Expr("attempt_count + 1"),
				"last_attempt_at": at,
				"next_attempt_at": nextAttemptAt,
				"updated_at":      at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrStaleChannelDeletionFence
		}
		result = tx.Model(&Channel{}).
			Where("id = ? AND deletion_pending = ? AND deletion_epoch = ? AND deletion_identity_revision = ?", fence.ChannelID, true, fence.DeletionEpoch, fence.IdentityRevision).
			Updates(map[string]any{"deletion_state": status, "updated_at": at})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrStaleChannelDeletionFence
		}
		return nil
	})
}

// RequeueChannelDeletionKeyManualReview records an audited administrator
// resolution and returns the same frozen identity to automatic verification.
// It never edits prior evidence and advances the epoch so an older reviewer or
// worker cannot commit against the requeued row.
func (r *Channels) RequeueChannelDeletionKeyManualReview(ctx context.Context, fence ChannelDeletionKeyReviewFence, resolution, actor string, at time.Time) error {
	resolution = strings.TrimSpace(resolution)
	actor = strings.TrimSpace(actor)
	if r == nil || r.db == nil || ctx == nil || !fence.valid() || resolution == "" || utf8.RuneCountInString(resolution) > 64 || actor == "" || utf8.RuneCountInString(actor) > 128 {
		return errors.New("channel deletion manual-review resolution is invalid")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		replayed, err := channelDeletionManualReviewAlreadyAppliedTx(tx, fence, "manual_review_requeued", resolution, actor)
		if err != nil {
			return err
		}
		if replayed {
			return nil
		}
		if _, _, err := requireChannelDeletionFenceTx(tx, fence.ChannelDeletionFence); err != nil {
			replayed, replayErr := channelDeletionManualReviewAlreadyAppliedTx(tx, fence, "manual_review_requeued", resolution, actor)
			if replayErr != nil {
				return replayErr
			}
			if replayed {
				return nil
			}
			return err
		}
		claim, err := loadChannelDeletionKeyClaimTx(tx, fence.ChannelDeletionFence, fence.KeyClaimID)
		if err != nil {
			return err
		}
		if claim.ClaimEpoch != fence.ClaimEpoch {
			replayed, err := channelDeletionManualReviewAlreadyAppliedTx(tx, fence, "manual_review_requeued", resolution, actor)
			if err != nil {
				return err
			}
			if replayed {
				return nil
			}
			return ErrChannelDeletionKeyClaimLost
		}
		if claim.Status != ChannelDeletionKeyManualReviewRequired ||
			claim.Disposition == ChannelDeletionKeyDispositionManualReview ||
			claim.ClaimEpoch >= channelDeletionMaxClaimEpoch {
			return ErrChannelDeletionKeyClaimLost
		}
		if _, err := requireFrozenChannelDeletionReviewKeyTx(tx, fence.ChannelDeletionFence, &claim); err != nil {
			return err
		}
		previous := claim.Status
		nextEpoch := claim.ClaimEpoch + 1
		result := tx.Model(&ChannelDeletionKeyClaim{}).
			Where("id = ? AND status = ? AND claim_epoch = ?", claim.ID, ChannelDeletionKeyManualReviewRequired, fence.ClaimEpoch).
			Updates(map[string]any{
				"status":                     ChannelDeletionKeyPendingVerification,
				"claim_epoch":                nextEpoch,
				"claimed_at":                 nil,
				"lease_expires_at":           nil,
				"observed_remote_id":         0,
				"evidence_digest":            "",
				"evidence_digest_version":    nil,
				"ownership_digest":           "",
				"ownership_digest_version":   nil,
				"observed_fingerprint":       "",
				"fingerprint_digest_version": nil,
				"inventory_digest":           "",
				"inventory_digest_version":   nil,
				"outcome_reason":             "",
				"outcome_actor":              "",
				"outcome_at":                 nil,
				"last_error":                 "",
				"updated_at":                 at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelDeletionKeyClaimLost
		}
		claim.Status = ChannelDeletionKeyPendingVerification
		claim.ClaimEpoch = nextEpoch
		claim.ClaimedAt = nil
		claim.LeaseExpiresAt = nil
		claim.ObservedRemoteID = 0
		claim.EvidenceDigest = ""
		claim.EvidenceDigestVersion = nil
		claim.OwnershipDigest = ""
		claim.OwnershipDigestVersion = nil
		claim.ObservedFingerprint = ""
		claim.FingerprintDigestVersion = nil
		claim.InventoryDigest = ""
		claim.InventoryDigestVersion = nil
		claim.OutcomeReason = ""
		claim.OutcomeActor = ""
		claim.OutcomeAt = nil
		claim.LastError = ""
		claim.UpdatedAt = at
		if err := appendChannelDeletionKeyEvidenceTx(tx, claim, previous, "manual_review_requeued", resolution, actor, "", at); err != nil {
			return err
		}
		return recomputeChannelDeletionIntentStateTx(tx, fence.ChannelDeletionFence, at)
	})
}

// RetainChannelDeletionKeyAfterReview records an explicit operator decision
// that the remote key must not be deleted. It advances the claim epoch, binds
// the resolution and actor into immutable digests, and leaves the frozen local
// row in place until the normal channel finalizer commits.
func (r *Channels) RetainChannelDeletionKeyAfterReview(ctx context.Context, fence ChannelDeletionKeyReviewFence, resolution, actor string, at time.Time) error {
	resolution = strings.TrimSpace(resolution)
	actor = strings.TrimSpace(actor)
	if r == nil || r.db == nil || ctx == nil || !fence.valid() ||
		resolution == "" || utf8.RuneCountInString(resolution) > 64 || actor == "" || utf8.RuneCountInString(actor) > 128 {
		return errors.New("channel deletion manual-review retention is invalid")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		replayed, err := channelDeletionManualReviewAlreadyAppliedTx(tx, fence, "manual_review_retained", resolution, actor)
		if err != nil {
			return err
		}
		if replayed {
			return nil
		}
		if _, _, err := requireChannelDeletionFenceTx(tx, fence.ChannelDeletionFence); err != nil {
			replayed, replayErr := channelDeletionManualReviewAlreadyAppliedTx(tx, fence, "manual_review_retained", resolution, actor)
			if replayErr != nil {
				return replayErr
			}
			if replayed {
				return nil
			}
			return err
		}
		claim, err := loadChannelDeletionKeyClaimTx(tx, fence.ChannelDeletionFence, fence.KeyClaimID)
		if err != nil {
			return err
		}
		if claim.ClaimEpoch != fence.ClaimEpoch {
			replayed, err := channelDeletionManualReviewAlreadyAppliedTx(tx, fence, "manual_review_retained", resolution, actor)
			if err != nil {
				return err
			}
			if replayed {
				return nil
			}
			return ErrChannelDeletionKeyClaimLost
		}
		if claim.Status != ChannelDeletionKeyManualReviewRequired || claim.ClaimEpoch >= channelDeletionMaxClaimEpoch {
			return ErrChannelDeletionKeyClaimLost
		}
		if _, err := requireFrozenChannelDeletionReviewKeyTx(tx, fence.ChannelDeletionFence, &claim); err != nil {
			return err
		}
		evidenceDigest, err := channelDeletionManualReviewResolutionDigest(claim, resolution, actor, "retention_evidence")
		if err != nil {
			return err
		}
		ownershipDigest, err := channelDeletionManualReviewResolutionDigest(claim, resolution, actor, "remote_retained_without_ownership")
		if err != nil {
			return err
		}
		previous := claim.Status
		nextEpoch := claim.ClaimEpoch + 1
		result := tx.Model(&ChannelDeletionKeyClaim{}).
			Where("id = ? AND status = ? AND claim_epoch = ?", claim.ID, ChannelDeletionKeyManualReviewRequired, fence.ClaimEpoch).
			Updates(map[string]any{
				"status":                   ChannelDeletionKeyRetainedByReview,
				"claim_epoch":              nextEpoch,
				"claimed_at":               nil,
				"lease_expires_at":         nil,
				"evidence_digest":          evidenceDigest,
				"evidence_digest_version":  channelDeletionReviewDigestVersion,
				"ownership_digest":         ownershipDigest,
				"ownership_digest_version": channelDeletionReviewDigestVersion,
				"outcome_reason":           resolution,
				"outcome_actor":            actor,
				"outcome_at":               at,
				"last_error":               "",
				"updated_at":               at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelDeletionKeyClaimLost
		}
		claim.Status = ChannelDeletionKeyRetainedByReview
		claim.ClaimEpoch = nextEpoch
		claim.ClaimedAt = nil
		claim.LeaseExpiresAt = nil
		claim.EvidenceDigest = evidenceDigest
		claim.EvidenceDigestVersion = uint16Pointer(channelDeletionReviewDigestVersion)
		claim.OwnershipDigest = ownershipDigest
		claim.OwnershipDigestVersion = uint16Pointer(channelDeletionReviewDigestVersion)
		claim.OutcomeReason = resolution
		claim.OutcomeActor = actor
		claim.OutcomeAt = timePointer(at)
		claim.LastError = ""
		claim.UpdatedAt = at
		if err := appendChannelDeletionKeyEvidenceTx(tx, claim, previous, "manual_review_retained", resolution, actor, "", at); err != nil {
			return err
		}
		return recomputeChannelDeletionIntentStateTx(tx, fence.ChannelDeletionFence, at)
	})
}

func channelDeletionManualReviewAlreadyAppliedTx(tx *gorm.DB, fence ChannelDeletionKeyReviewFence, eventKind, resolution, actor string) (bool, error) {
	if tx == nil || !fence.valid() {
		return false, ErrChannelDeletionKeyClaimLost
	}
	var count int64
	err := tx.Model(&ChannelDeletionKeyEvidence{}).Where(
		"key_claim_id = ? AND intent_id = ? AND channel_id = ? AND deletion_epoch = ? AND channel_identity_revision = ? AND claim_epoch = ? AND event_kind = ? AND reason = ? AND actor = ?",
		fence.KeyClaimID, fence.IntentID, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision,
		fence.ClaimEpoch+1, eventKind, resolution, actor,
	).Count(&count).Error
	return count == 1, err
}

func requireFrozenChannelDeletionReviewKeyTx(tx *gorm.DB, fence ChannelDeletionFence, claim *ChannelDeletionKeyClaim) (*GatewayUpstreamKey, error) {
	if claim == nil {
		return nil, ErrChannelDeletionKeyFreezeChanged
	}
	if claim.UpstreamGroupID == nil && claim.PresenceGeneration == nil && strings.TrimSpace(claim.CanonicalGroupName) == "" {
		return requireFrozenChannelDeletionLocalKeyTx(tx, fence, claim)
	}
	return requireFrozenChannelDeletionKeyTx(tx, fence, claim)
}

func channelDeletionManualReviewResolutionDigest(claim ChannelDeletionKeyClaim, resolution, actor, kind string) (string, error) {
	raw, err := json.Marshal(struct {
		Version     uint16 `json:"version"`
		Kind        string `json:"kind"`
		ClaimID     uint64 `json:"claim_id"`
		ClaimDigest string `json:"claim_digest"`
		Resolution  string `json:"resolution"`
		Actor       string `json:"actor"`
	}{
		Version: channelDeletionReviewDigestVersion, Kind: kind,
		ClaimID: claim.ID, ClaimDigest: claim.ClaimDigest,
		Resolution: resolution, Actor: actor,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

type channelDeletionFinalOutcomeDigestRow struct {
	KeyClaimID               uint64                        `json:"key_claim_id"`
	LocalKeyID               uint                          `json:"local_key_id"`
	KeyGeneration            uint64                        `json:"key_generation"`
	UpstreamGroupID          *uint                         `json:"upstream_group_id"`
	PresenceGeneration       *uint64                       `json:"presence_generation"`
	CanonicalGroupName       string                        `json:"canonical_group_name"`
	UpstreamKeyID            int64                         `json:"upstream_key_id"`
	ObservedRemoteID         int64                         `json:"observed_remote_id"`
	UpstreamKeyName          string                        `json:"upstream_key_name"`
	Disposition              ChannelDeletionKeyDisposition `json:"disposition"`
	Status                   ChannelDeletionKeyClaimStatus `json:"status"`
	ClaimEpoch               uint64                        `json:"claim_epoch"`
	ClaimDigest              string                        `json:"claim_digest"`
	EvidenceDigest           string                        `json:"evidence_digest"`
	EvidenceDigestVersion    *uint16                       `json:"evidence_digest_version"`
	OwnershipDigest          string                        `json:"ownership_digest"`
	OwnershipDigestVersion   *uint16                       `json:"ownership_digest_version"`
	ObservedFingerprint      string                        `json:"observed_fingerprint"`
	FingerprintDigestVersion *uint16                       `json:"fingerprint_digest_version"`
	InventoryDigest          string                        `json:"inventory_digest"`
	InventoryDigestVersion   *uint16                       `json:"inventory_digest_version"`
}

func channelDeletionFinalOutcomeDigest(claims []ChannelDeletionKeyClaim) (string, error) {
	ordered := append([]ChannelDeletionKeyClaim(nil), claims...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	rows := make([]channelDeletionFinalOutcomeDigestRow, 0, len(ordered))
	for _, claim := range ordered {
		rows = append(rows, channelDeletionFinalOutcomeDigestRow{
			KeyClaimID: claim.ID, LocalKeyID: claim.LocalKeyID, KeyGeneration: claim.KeyGeneration,
			UpstreamGroupID: claim.UpstreamGroupID, PresenceGeneration: claim.PresenceGeneration,
			CanonicalGroupName: claim.CanonicalGroupName,
			UpstreamKeyID:      claim.UpstreamKeyID, ObservedRemoteID: claim.ObservedRemoteID,
			UpstreamKeyName: claim.UpstreamKeyName,
			Disposition:     claim.Disposition, Status: claim.Status,
			ClaimEpoch: claim.ClaimEpoch, ClaimDigest: claim.ClaimDigest,
			EvidenceDigest: claim.EvidenceDigest, EvidenceDigestVersion: claim.EvidenceDigestVersion,
			OwnershipDigest: claim.OwnershipDigest, OwnershipDigestVersion: claim.OwnershipDigestVersion,
			ObservedFingerprint: claim.ObservedFingerprint, FingerprintDigestVersion: claim.FingerprintDigestVersion,
			InventoryDigest: claim.InventoryDigest, InventoryDigestVersion: claim.InventoryDigestVersion,
		})
	}
	payload := struct {
		Version uint16                                 `json:"version"`
		Rows    []channelDeletionFinalOutcomeDigestRow `json:"rows"`
	}{Version: channelDeletionFinalDigestVersion, Rows: rows}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func channelDeletionAlreadyFinalizedTx(tx *gorm.DB, fence ChannelDeletionFence) (bool, error) {
	if tx == nil || !fence.valid() {
		return false, ErrStaleChannelDeletionFence
	}
	var intent ChannelDeletionIntent
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ? AND channel_id = ? AND deletion_epoch = ? AND channel_identity_revision = ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision).
		First(&intent).Error
	if err != nil {
		return false, err
	}
	if intent.Status != ChannelDeletionIntentFinalized || intent.FinalizedAt == nil {
		return false, ErrStaleChannelDeletionFence
	}
	var count int64
	if err := tx.Model(&ChannelDeletionFinalAudit{}).
		Where("intent_id = ? AND channel_id = ? AND deletion_epoch = ? AND channel_identity_revision = ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision).
		Count(&count).Error; err != nil {
		return false, err
	}
	return count == 1, nil
}

// FinalizeChannelDeletion performs the only physical channel delete. The
// entire cleanup, request-history snapshot, AuthSession deletion, immutable
// final audit, intent transition, and expected-epoch channel delete commit or
// roll back together. Repeating the same finalized fence is idempotent.
func (r *Channels) FinalizeChannelDeletion(ctx context.Context, fence ChannelDeletionFence, actor string, at time.Time) error {
	actor = strings.TrimSpace(actor)
	if r == nil || r.db == nil || ctx == nil || !fence.valid() || actor == "" {
		return errors.New("channel deletion finalizer is invalid")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var channel Channel
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&channel, fence.ChannelID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			finalized, finalErr := channelDeletionAlreadyFinalizedTx(tx, fence)
			if finalErr != nil {
				return finalErr
			}
			if finalized {
				return nil
			}
			return ErrStaleChannelDeletionFence
		}
		if err != nil {
			return err
		}
		if !channel.DeletionPending || channel.DeletionEpoch != fence.DeletionEpoch ||
			channel.DeletionIdentityRevision != fence.IdentityRevision ||
			channel.UpstreamAccountIdentityRevision != fence.IdentityRevision ||
			channel.DeletionState != string(ChannelDeletionIntentReadyForFinalization) ||
			channel.MonitorEnabled || channel.LatencyProbeEnabled {
			return ErrStaleChannelDeletionFence
		}
		var intent ChannelDeletionIntent
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND channel_id = ? AND deletion_epoch = ? AND channel_identity_revision = ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision).
			First(&intent).Error; err != nil {
			return err
		}
		if intent.Status != ChannelDeletionIntentReadyForFinalization ||
			intent.ChannelNameSnapshot != channel.Name || intent.ChannelTypeSnapshot != channel.Type {
			return ErrChannelDeletionFinalizationBlocked
		}

		claims := make([]ChannelDeletionKeyClaim, 0)
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("intent_id = ?", fence.IntentID).Order("canonical_group_name ASC, id ASC").Find(&claims).Error; err != nil {
			return err
		}
		var preparedCount int64
		if err := tx.Model(&ChannelDeletionKeyAudit{}).
			Where("intent_id = ? AND channel_id = ? AND deletion_epoch = ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch).
			Count(&preparedCount).Error; err != nil {
			return err
		}
		if preparedCount != int64(len(claims)) {
			return ErrChannelDeletionFinalizationBlocked
		}
		var deletedCount, absentCount, retainedCount, retainedByReviewCount int64
		for index := range claims {
			claim := &claims[index]
			expectedDigest, digestErr := channelDeletionKeyClaimDigest(*claim)
			if digestErr != nil || expectedDigest != claim.ClaimDigest ||
				claim.ClaimDigestVersion != channelDeletionClaimDigestVersion ||
				!channelDeletionHandledStatus(claim.Status) ||
				!validChannelDeletionDigest(claim.EvidenceDigest) || claim.EvidenceDigestVersion == nil ||
				!validChannelDeletionDigest(claim.OwnershipDigest) || claim.OwnershipDigestVersion == nil {
				return ErrChannelDeletionFinalizationBlocked
			}
			if claim.UpstreamGroupID == nil || claim.PresenceGeneration == nil || claim.CanonicalGroupName == "" {
				if _, err := requireFrozenChannelDeletionLocalKeyTx(tx, fence, claim); err != nil {
					return err
				}
			} else if _, err := requireFrozenChannelDeletionKeyTx(tx, fence, claim); err != nil {
				return err
			}
			switch claim.Status {
			case ChannelDeletionKeyDeleteConfirmed:
				deletedCount++
			case ChannelDeletionKeyInitiallyAbsentConfirmed:
				absentCount++
			case ChannelDeletionKeyRetainedAdopted:
				retainedCount++
			case ChannelDeletionKeyRetainedByReview:
				retainedByReviewCount++
			}
		}
		var currentRateCount, currentGroupCount, enabledKeyCount int64
		if err := tx.Model(&RateSnapshot{}).Where("channel_id = ?", fence.ChannelID).Count(&currentRateCount).Error; err != nil {
			return err
		}
		if err := tx.Model(&GatewayUpstreamGroup{}).Where("channel_id = ? AND present = ?", fence.ChannelID, true).Count(&currentGroupCount).Error; err != nil {
			return err
		}
		if err := tx.Model(&GatewayUpstreamKey{}).Where("channel_id = ? AND enabled = ?", fence.ChannelID, true).Count(&enabledKeyCount).Error; err != nil {
			return err
		}
		if currentRateCount != 0 || currentGroupCount != 0 || enabledKeyCount != 0 {
			return ErrChannelDeletionFinalizationBlocked
		}
		outcomeDigest, err := channelDeletionFinalOutcomeDigest(claims)
		if err != nil {
			return err
		}
		if err := snapshotGatewayRequestHistoryBeforeChannelDelete(tx, channel.ID, channel.Name); err != nil {
			return err
		}
		sessionDelete := tx.Where("channel_id = ?", fence.ChannelID).Delete(&AuthSession{})
		if sessionDelete.Error != nil {
			return sessionDelete.Error
		}
		sessionCleanupStatus := "not_present"
		if sessionDelete.RowsAffected > 0 {
			sessionCleanupStatus = "deleted"
		}
		// Close mutable lifecycle rows while the channel fence is held. Already
		// completed history is deliberately left unchanged.
		if err := tx.Model(&GatewayUpstreamGroupChange{}).
			Where("channel_id = ? AND status IN ?", fence.ChannelID, []string{"pending", "processing"}).
			Updates(map[string]any{
				"status":                 "superseded",
				"assignment_state":       "superseded",
				"assignment_reason_code": "channel_deleted",
				"resolved_at":            at,
				"evaluated_at":           at,
				"updated_at":             at,
			}).Error; err != nil {
			return err
		}
		for _, model := range []any{
			&ChannelDirectCredential{},
			&GatewayRequestFundingCorrection{},
			&ChannelFundingEvent{},
			&ChannelFundingCandidate{},
			&ChannelCostState{},
			&RateSnapshot{},
			&RateChangeLog{},
			&BalanceSnapshot{},
			&CostSnapshot{},
			&GatewayUpstreamKey{},
			&GatewayModelCapability{},
			&GatewayRoutingFailure{},
			&GatewayCandidateGroupMember{},
			&GatewayRouteExclusion{},
			&GatewayManagedKeyRebuildIntent{},
			&GatewayLatencyProbeState{},
			&ModelCheckTarget{},
			&MonitorLog{},
			&NotificationCooldown{},
			&UpstreamAnnouncement{},
		} {
			if err := tx.Where("channel_id = ?", fence.ChannelID).Delete(model).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("upstream_channel_id = ?", fence.ChannelID).Delete(&NotificationLog{}).Error; err != nil {
			return err
		}
		if channel.Name != "" {
			pattern := "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(channel.Name) + "%"
			if err := tx.Where("upstream_channel_id = 0 AND (subject LIKE ? ESCAPE '!' OR body LIKE ? ESCAPE '!')", pattern, pattern).
				Delete(&NotificationLog{}).Error; err != nil {
				return err
			}
		}

		finalAudit := ChannelDeletionFinalAudit{
			IntentID: intent.ID, ChannelID: channel.ID,
			DeletionEpoch: fence.DeletionEpoch, ChannelIdentityRevision: fence.IdentityRevision,
			ChannelNameSnapshot: intent.ChannelNameSnapshot, ChannelTypeSnapshot: intent.ChannelTypeSnapshot,
			PreparedKeyCount: preparedCount, DeleteConfirmedCount: deletedCount,
			InitiallyAbsentCount: absentCount, RetainedAdoptedCount: retainedCount,
			RetainedByReviewCount: retainedByReviewCount,
			KeyOutcomeDigest:      outcomeDigest, KeyOutcomeDigestVersion: channelDeletionFinalDigestVersion,
			AuthSessionCleanupStatus: sessionCleanupStatus, Actor: actor,
			FinalizedAt: at, CreatedAt: at,
		}
		if err := tx.Create(&finalAudit).Error; err != nil {
			return err
		}
		result := tx.Model(&ChannelDeletionIntent{}).
			Where("id = ? AND channel_id = ? AND deletion_epoch = ? AND channel_identity_revision = ? AND status = ?", fence.IntentID, fence.ChannelID, fence.DeletionEpoch, fence.IdentityRevision, ChannelDeletionIntentReadyForFinalization).
			Updates(map[string]any{
				"status":          ChannelDeletionIntentFinalized,
				"last_error":      "",
				"next_attempt_at": nil,
				"finalized_at":    at,
				"updated_at":      at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrStaleChannelDeletionFence
		}
		result = tx.Where(
			"id = ? AND deletion_pending = ? AND deletion_epoch = ? AND deletion_identity_revision = ? AND upstream_account_identity_revision = ? AND deletion_state = ?",
			fence.ChannelID, true, fence.DeletionEpoch, fence.IdentityRevision, fence.IdentityRevision, ChannelDeletionIntentReadyForFinalization,
		).Delete(&Channel{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrStaleChannelDeletionFence
		}
		return nil
	})
}
