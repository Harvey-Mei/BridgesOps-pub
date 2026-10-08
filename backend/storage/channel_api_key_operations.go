package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrChannelAPIKeyOperationConflict   = errors.New("channel api key operation conflicts with existing request")
	ErrChannelAPIKeyRemoteIDConflict    = errors.New("channel api key operation is bound to another remote id")
	ErrChannelAPIKeyRejectedPostState   = errors.New("channel api key rejected post state changed")
	ErrChannelAPIKeyAbsenceState        = errors.New("channel api key absence confirmation state changed")
	ErrChannelAPIKeyRecoveryFence       = errors.New("channel api key recovery fence is no longer current")
	ErrChannelAPIKeyOperationGeneration = errors.New("channel api key operation generation changed")
)

type ChannelAPIKeyOperationRecoveryFence struct {
	ChannelIdentityRevision uint64
	UpstreamGroupID         uint
	PresenceGeneration      uint64
	CanonicalGroupName      string
}

func (f ChannelAPIKeyOperationRecoveryFence) Valid() bool {
	return f.ChannelIdentityRevision > 0 && f.UpstreamGroupID > 0 &&
		f.PresenceGeneration > 0 && strings.TrimSpace(f.CanonicalGroupName) != ""
}

func (f ChannelAPIKeyOperationRecoveryFence) empty() bool {
	return f.ChannelIdentityRevision == 0 && f.UpstreamGroupID == 0 &&
		f.PresenceGeneration == 0 && strings.TrimSpace(f.CanonicalGroupName) == ""
}

type ChannelAPIKeyOperationStart struct {
	ChannelID                  uint
	Provider                   ChannelType
	ScopeHash                  string
	RequestFingerprint         string
	LegacyRecoveryFingerprints []string
	ManagedName                string
	GroupIdentity              string
	CustomKeyHash              string
	RecoveryGroupName          string
	RecoveryFence              ChannelAPIKeyOperationRecoveryFence
}

// ChannelAPIKeyOperations persists the one-way transition from a locally
// claimed creation operation to a bound remote ID and finally completion.
type ChannelAPIKeyOperations struct{ db *gorm.DB }

func NewChannelAPIKeyOperations(db *gorm.DB) *ChannelAPIKeyOperations {
	return &ChannelAPIKeyOperations{db: db}
}

func NewChannelAPIKeyOperationsFromChannels(channels *Channels) *ChannelAPIKeyOperations {
	if channels == nil {
		return nil
	}
	return NewChannelAPIKeyOperations(channels.db)
}

func (r *ChannelAPIKeyOperations) WithContext(ctx context.Context) *ChannelAPIKeyOperations {
	if r == nil || r.db == nil {
		return r
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &ChannelAPIKeyOperations{db: r.db.WithContext(ctx)}
}

// Begin atomically claims one durable operation slot. Only a newly inserted
// row authorizes a remote POST; callers receiving created=false must recover
// the existing operation without posting again.
func (r *ChannelAPIKeyOperations) Begin(input ChannelAPIKeyOperationStart) (*ChannelAPIKeyOperation, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("channel api key operation store is unavailable")
	}
	if input.ChannelID == 0 || strings.TrimSpace(input.ManagedName) == "" {
		return nil, false, errors.New("channel api key operation identity is invalid")
	}
	if input.Provider != ChannelTypeNewAPI && input.Provider != ChannelTypeSub2API {
		return nil, false, fmt.Errorf("unsupported channel api key provider %q", input.Provider)
	}
	if len(input.ScopeHash) != 64 || len(input.RequestFingerprint) != 64 || (input.CustomKeyHash != "" && len(input.CustomKeyHash) != 64) {
		return nil, false, errors.New("channel api key operation fingerprint is invalid")
	}
	for _, fingerprint := range input.LegacyRecoveryFingerprints {
		if len(fingerprint) != 64 || fingerprint == input.RequestFingerprint {
			return nil, false, errors.New("channel api key legacy recovery fingerprint is invalid")
		}
	}
	if !input.RecoveryFence.empty() {
		if !input.RecoveryFence.Valid() {
			return nil, false, ErrChannelAPIKeyRecoveryFence
		}
		recoveryGroupName := strings.TrimSpace(input.RecoveryGroupName)
		if recoveryGroupName == "" && input.Provider == ChannelTypeNewAPI {
			recoveryGroupName = input.GroupIdentity
		}
		if err := r.validateRecoveryFence(input.ChannelID, recoveryGroupName, input.RecoveryFence); err != nil {
			return nil, false, err
		}
	}

	candidate := &ChannelAPIKeyOperation{
		ChannelID:               input.ChannelID,
		Provider:                input.Provider,
		ScopeHash:               input.ScopeHash,
		OperationGeneration:     1,
		RequestFingerprint:      input.RequestFingerprint,
		ManagedName:             input.ManagedName,
		GroupIdentity:           input.GroupIdentity,
		CustomKeyHash:           input.CustomKeyHash,
		Status:                  ChannelAPIKeyOperationPending,
		ChannelIdentityRevision: input.RecoveryFence.ChannelIdentityRevision,
		UpstreamGroupID:         input.RecoveryFence.UpstreamGroupID,
		PresenceGeneration:      input.RecoveryFence.PresenceGeneration,
		CanonicalGroupName:      strings.TrimSpace(input.RecoveryFence.CanonicalGroupName),
	}
	result := r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "channel_id"}, {Name: "provider"}, {Name: "scope_hash"}},
		DoNothing: true,
	}).Create(candidate)
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 1 {
		return candidate, true, nil
	}

	existing, err := r.Find(input.ChannelID, input.Provider, input.ScopeHash)
	if err != nil {
		return nil, false, err
	}
	if existing.ManagedName != input.ManagedName ||
		existing.GroupIdentity != input.GroupIdentity ||
		existing.CustomKeyHash != input.CustomKeyHash {
		return nil, false, ErrChannelAPIKeyOperationConflict
	}
	if existing.RequestFingerprint != input.RequestFingerprint &&
		!legacyChannelAPIKeyOperationRecoveryMatches(existing, input) {
		return nil, false, ErrChannelAPIKeyOperationConflict
	}
	return existing, false, nil
}

func legacyChannelAPIKeyOperationRecoveryMatches(existing *ChannelAPIKeyOperation, input ChannelAPIKeyOperationStart) bool {
	if existing == nil || existing.Status != ChannelAPIKeyOperationPending ||
		input.Provider != ChannelTypeNewAPI || !input.RecoveryFence.Valid() {
		return false
	}
	allowed := false
	for _, fingerprint := range input.LegacyRecoveryFingerprints {
		if existing.RequestFingerprint == fingerprint {
			allowed = true
			break
		}
	}
	return allowed &&
		existing.ChannelIdentityRevision == input.RecoveryFence.ChannelIdentityRevision &&
		existing.UpstreamGroupID == input.RecoveryFence.UpstreamGroupID &&
		existing.PresenceGeneration == input.RecoveryFence.PresenceGeneration &&
		strings.TrimSpace(existing.CanonicalGroupName) == strings.TrimSpace(input.RecoveryFence.CanonicalGroupName)
}

func (r *ChannelAPIKeyOperations) validateRecoveryFence(channelID uint, groupName string, fence ChannelAPIKeyOperationRecoveryFence) error {
	canonicalGroupName, err := canonicalUpstreamGroupName(r.db, groupName)
	if err != nil || canonicalGroupName != strings.TrimSpace(fence.CanonicalGroupName) {
		return ErrChannelAPIKeyRecoveryFence
	}
	var channel Channel
	if err := r.db.Select("id", "upstream_account_identity_revision").First(&channel, channelID).Error; err != nil {
		return err
	}
	if channel.UpstreamAccountIdentityRevision != fence.ChannelIdentityRevision {
		return ErrChannelAPIKeyRecoveryFence
	}
	var group GatewayUpstreamGroup
	if err := r.db.Select("id", "channel_id", "canonical_group_name", "present", "presence_generation").First(&group, fence.UpstreamGroupID).Error; err != nil {
		return err
	}
	if group.ChannelID != channelID || !group.Present || group.PresenceGeneration != fence.PresenceGeneration ||
		strings.TrimSpace(group.CanonicalGroupName) != canonicalGroupName {
		return ErrChannelAPIKeyRecoveryFence
	}
	return nil
}

func (r *ChannelAPIKeyOperations) Find(channelID uint, provider ChannelType, scopeHash string) (*ChannelAPIKeyOperation, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("channel api key operation store is unavailable")
	}
	var operation ChannelAPIKeyOperation
	if err := r.db.Where("channel_id = ? AND provider = ? AND scope_hash = ?", channelID, provider, scopeHash).
		First(&operation).Error; err != nil {
		return nil, err
	}
	return &operation, nil
}

// ConfirmAbsenceAndRotate records one complete, strict remote-inventory
// observation. Only observations separated in time under an unchanged account
// and group-generation fence can supersede a commit-unknown operation and
// create the next operation generation.
func (r *ChannelAPIKeyOperations) ConfirmAbsenceAndRotate(
	operationID uint64,
	operationGeneration uint32,
	fence ChannelAPIKeyOperationRecoveryFence,
	at time.Time,
	minimumInterval time.Duration,
	requiredConfirmations uint32,
) (*ChannelAPIKeyOperation, bool, error) {
	return r.confirmAbsenceAndRotate(
		operationID,
		operationGeneration,
		fence,
		at,
		minimumInterval,
		requiredConfirmations,
		"",
	)
}

// ConfirmAbsenceAndRotateWithRequestFingerprint records the same fenced
// absence evidence as ConfirmAbsenceAndRotate, but carries a verified
// replacement create fingerprint into the next generation. The old intent is
// preserved in the immutable superseded-generation record. This is only for a
// compatibility migration after the old operation has been safely proven
// absent; it never authorizes a POST by itself.
func (r *ChannelAPIKeyOperations) ConfirmAbsenceAndRotateWithRequestFingerprint(
	operationID uint64,
	operationGeneration uint32,
	fence ChannelAPIKeyOperationRecoveryFence,
	at time.Time,
	minimumInterval time.Duration,
	requiredConfirmations uint32,
	nextRequestFingerprint string,
) (*ChannelAPIKeyOperation, bool, error) {
	if len(nextRequestFingerprint) != 64 {
		return nil, false, ErrChannelAPIKeyAbsenceState
	}
	return r.confirmAbsenceAndRotate(
		operationID,
		operationGeneration,
		fence,
		at,
		minimumInterval,
		requiredConfirmations,
		nextRequestFingerprint,
	)
}

func (r *ChannelAPIKeyOperations) confirmAbsenceAndRotate(
	operationID uint64,
	operationGeneration uint32,
	fence ChannelAPIKeyOperationRecoveryFence,
	at time.Time,
	minimumInterval time.Duration,
	requiredConfirmations uint32,
	nextRequestFingerprint string,
) (*ChannelAPIKeyOperation, bool, error) {
	if r == nil || r.db == nil || operationID == 0 || operationGeneration == 0 || !fence.Valid() || minimumInterval <= 0 || requiredConfirmations < 2 {
		return nil, false, ErrChannelAPIKeyAbsenceState
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	fence.CanonicalGroupName = strings.TrimSpace(fence.CanonicalGroupName)

	var current ChannelAPIKeyOperation
	rotated := false
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var preview ChannelAPIKeyOperation
		if err := tx.Select("id", "channel_id").First(&preview, operationID).Error; err != nil {
			return err
		}
		var channel Channel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "upstream_account_identity_revision").First(&channel, preview.ChannelID).Error; err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != fence.ChannelIdentityRevision {
			return ErrChannelAPIKeyRecoveryFence
		}
		var group GatewayUpstreamGroup
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "channel_id", "canonical_group_name", "present", "presence_generation").
			First(&group, fence.UpstreamGroupID).Error; err != nil {
			return err
		}
		if group.ChannelID != preview.ChannelID || !group.Present ||
			group.PresenceGeneration != fence.PresenceGeneration ||
			strings.TrimSpace(group.CanonicalGroupName) != fence.CanonicalGroupName {
			return ErrChannelAPIKeyRecoveryFence
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, operationID).Error; err != nil {
			return err
		}
		if current.OperationGeneration != operationGeneration {
			return ErrChannelAPIKeyOperationGeneration
		}
		rejectedRetryCommitUnknown := current.RejectedPostStatus == 429 && current.RejectedRetryAt != nil
		if current.ChannelID != preview.ChannelID || current.Status != ChannelAPIKeyOperationPending ||
			current.RemoteID != nil || (current.RejectedPostStatus != 0 && !rejectedRetryCommitUnknown) {
			return ErrChannelAPIKeyAbsenceState
		}

		storedFence := ChannelAPIKeyOperationRecoveryFence{
			ChannelIdentityRevision: current.ChannelIdentityRevision,
			UpstreamGroupID:         current.UpstreamGroupID,
			PresenceGeneration:      current.PresenceGeneration,
			CanonicalGroupName:      strings.TrimSpace(current.CanonicalGroupName),
		}
		if storedFence != fence {
			updates := map[string]any{
				"channel_identity_revision":  fence.ChannelIdentityRevision,
				"upstream_group_id":          fence.UpstreamGroupID,
				"presence_generation":        fence.PresenceGeneration,
				"canonical_group_name":       fence.CanonicalGroupName,
				"absence_confirmation_count": 1,
				"absence_first_confirmed_at": at,
				"absence_last_confirmed_at":  at,
				"updated_at":                 at,
			}
			if err := tx.Model(&current).Updates(updates).Error; err != nil {
				return err
			}
			current.ChannelIdentityRevision = fence.ChannelIdentityRevision
			current.UpstreamGroupID = fence.UpstreamGroupID
			current.PresenceGeneration = fence.PresenceGeneration
			current.CanonicalGroupName = fence.CanonicalGroupName
			current.AbsenceConfirmationCount = 1
			current.AbsenceFirstConfirmedAt = &at
			current.AbsenceLastConfirmedAt = &at
			current.UpdatedAt = at
			return nil
		}
		if current.AbsenceLastConfirmedAt != nil && current.AbsenceLastConfirmedAt.Add(minimumInterval).After(at) {
			return nil
		}

		confirmationCount := current.AbsenceConfirmationCount + 1
		if current.AbsenceConfirmationCount == 0 {
			confirmationCount = 1
		}
		if confirmationCount < requiredConfirmations {
			updates := map[string]any{
				"absence_confirmation_count": confirmationCount,
				"absence_last_confirmed_at":  at,
				"updated_at":                 at,
			}
			if current.AbsenceFirstConfirmedAt == nil {
				updates["absence_first_confirmed_at"] = at
			}
			if err := tx.Model(&current).Updates(updates).Error; err != nil {
				return err
			}
			current.AbsenceConfirmationCount = confirmationCount
			if current.AbsenceFirstConfirmedAt == nil {
				current.AbsenceFirstConfirmedAt = &at
			}
			current.AbsenceLastConfirmedAt = &at
			current.UpdatedAt = at
			return nil
		}

		generation := current.OperationGeneration
		if generation == 0 {
			generation = 1
		}
		if generation == ^uint32(0) {
			return ErrChannelAPIKeyAbsenceState
		}
		if current.AbsenceFirstConfirmedAt == nil {
			return ErrChannelAPIKeyAbsenceState
		}
		archive := ChannelAPIKeyOperationGeneration{
			OperationID: current.ID, ChannelID: current.ChannelID, Provider: current.Provider, ScopeHash: current.ScopeHash,
			OperationGeneration: generation, RequestFingerprint: current.RequestFingerprint,
			ManagedName: current.ManagedName, GroupIdentity: current.GroupIdentity, CustomKeyHash: current.CustomKeyHash,
			Status:                  ChannelAPIKeyOperationSuperseded,
			ChannelIdentityRevision: fence.ChannelIdentityRevision, UpstreamGroupID: fence.UpstreamGroupID,
			PresenceGeneration: fence.PresenceGeneration, CanonicalGroupName: fence.CanonicalGroupName,
			AbsenceConfirmationCount: confirmationCount, AbsenceFirstConfirmedAt: *current.AbsenceFirstConfirmedAt,
			AbsenceLastConfirmedAt: at, SupersededByGeneration: generation + 1, SupersededAt: at,
			RejectedPostStatus: current.RejectedPostStatus, RejectedPostAt: current.RejectedPostAt,
			RejectedRetryAt: current.RejectedRetryAt, RejectedRetryCount: current.RejectedRetryCount,
			CreatedAt: current.CreatedAt, UpdatedAt: at,
		}
		if err := tx.Create(&archive).Error; err != nil {
			return err
		}
		updates := map[string]any{
			"operation_generation":       generation + 1,
			"channel_identity_revision":  fence.ChannelIdentityRevision,
			"upstream_group_id":          fence.UpstreamGroupID,
			"presence_generation":        fence.PresenceGeneration,
			"canonical_group_name":       fence.CanonicalGroupName,
			"absence_confirmation_count": 0,
			"absence_first_confirmed_at": nil,
			"absence_last_confirmed_at":  nil,
			"rejected_post_status":       0,
			"rejected_post_at":           nil,
			"rejected_retry_at":          nil,
			"rejected_retry_count":       0,
			"completed_at":               nil,
			"created_at":                 at,
			"updated_at":                 at,
		}
		if nextRequestFingerprint != "" {
			updates["request_fingerprint"] = nextRequestFingerprint
		}
		result := tx.Model(&current).
			Where("id = ? AND status = ? AND remote_id IS NULL AND operation_generation = ?", current.ID, ChannelAPIKeyOperationPending, generation).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelAPIKeyAbsenceState
		}
		if err := tx.First(&current, current.ID).Error; err != nil {
			return err
		}
		rotated = true
		return nil
	})
	return &current, rotated, err
}

func (r *ChannelAPIKeyOperations) BindRemoteID(operationID uint64, operationGeneration uint32, remoteID int64) (*ChannelAPIKeyOperation, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("channel api key operation store is unavailable")
	}
	if operationID == 0 || operationGeneration == 0 || remoteID <= 0 {
		return nil, errors.New("channel api key remote identity is invalid")
	}
	result := r.db.Model(&ChannelAPIKeyOperation{}).
		Where("id = ? AND operation_generation = ? AND (remote_id IS NULL OR remote_id = ?)", operationID, operationGeneration, remoteID).
		Updates(map[string]any{"remote_id": remoteID, "updated_at": time.Now().UTC()})
	if result.Error != nil {
		return nil, result.Error
	}
	var operation ChannelAPIKeyOperation
	if err := r.db.First(&operation, operationID).Error; err != nil {
		return nil, err
	}
	if operation.OperationGeneration != operationGeneration {
		return nil, ErrChannelAPIKeyOperationGeneration
	}
	if result.RowsAffected != 1 && (operation.RemoteID == nil || *operation.RemoteID != remoteID) {
		return nil, ErrChannelAPIKeyRemoteIDConflict
	}
	return &operation, nil
}

// RecordRejectedPost records only an explicit provider rejection. A timeout or
// other commit-unknown result must never enter this retry-authorized state.
func (r *ChannelAPIKeyOperations) RecordRejectedPost(operationID uint64, operationGeneration uint32, status int, at time.Time) (*ChannelAPIKeyOperation, error) {
	if r == nil || r.db == nil || operationID == 0 || operationGeneration == 0 || status != 429 {
		return nil, ErrChannelAPIKeyRejectedPostState
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	result := r.db.Model(&ChannelAPIKeyOperation{}).
		Where("id = ? AND operation_generation = ? AND status = ? AND remote_id IS NULL", operationID, operationGeneration, ChannelAPIKeyOperationPending).
		Updates(map[string]any{
			"rejected_post_status": status,
			"rejected_post_at":     at,
			"rejected_retry_at":    nil,
			"updated_at":           at,
		})
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected != 1 {
		var current ChannelAPIKeyOperation
		if err := r.db.Select("operation_generation").First(&current, operationID).Error; err == nil && current.OperationGeneration != operationGeneration {
			return nil, ErrChannelAPIKeyOperationGeneration
		}
		return nil, ErrChannelAPIKeyRejectedPostState
	}
	var operation ChannelAPIKeyOperation
	if err := r.db.First(&operation, operationID).Error; err != nil {
		return nil, err
	}
	return &operation, nil
}

// ClaimRejectedPostRetry is the sole authorization for another remote POST.
// The caller must perform a complete recovery inventory immediately before
// this claim; the row lock ensures only one process can claim each rejection.
func (r *ChannelAPIKeyOperations) ClaimRejectedPostRetry(operationID uint64, operationGeneration uint32, at time.Time, minimumAge time.Duration) (*ChannelAPIKeyOperation, bool, error) {
	if r == nil || r.db == nil || operationID == 0 || operationGeneration == 0 || minimumAge <= 0 {
		return nil, false, ErrChannelAPIKeyRejectedPostState
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	var operation ChannelAPIKeyOperation
	claimed := false
	err := r.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&operation, operationID).Error; err != nil {
			return err
		}
		if operation.OperationGeneration != operationGeneration {
			return ErrChannelAPIKeyOperationGeneration
		}
		if operation.Status != ChannelAPIKeyOperationPending || operation.RemoteID != nil ||
			operation.RejectedPostStatus != 429 || operation.RejectedPostAt == nil || operation.RejectedRetryAt != nil ||
			operation.RejectedPostAt.Add(minimumAge).After(at) {
			return nil
		}
		result := tx.Model(&operation).
			Where("id = ? AND operation_generation = ? AND status = ? AND remote_id IS NULL AND rejected_post_status = 429 AND rejected_post_at = ? AND rejected_retry_at IS NULL",
				operation.ID, operationGeneration, ChannelAPIKeyOperationPending, *operation.RejectedPostAt).
			Updates(map[string]any{
				"rejected_retry_at":    at,
				"rejected_retry_count": gorm.Expr("rejected_retry_count + 1"),
				"updated_at":           at,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrChannelAPIKeyRejectedPostState
		}
		operation.RejectedRetryAt = &at
		operation.RejectedRetryCount++
		claimed = true
		return nil
	})
	return &operation, claimed, err
}

func (r *ChannelAPIKeyOperations) Complete(operationID uint64, operationGeneration uint32, remoteID int64) (*ChannelAPIKeyOperation, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("channel api key operation store is unavailable")
	}
	if operationID == 0 || operationGeneration == 0 || remoteID <= 0 {
		return nil, errors.New("channel api key remote identity is invalid")
	}
	completedAt := time.Now().UTC()
	result := r.db.Model(&ChannelAPIKeyOperation{}).
		Where("id = ? AND operation_generation = ? AND remote_id = ? AND status IN ?", operationID, operationGeneration, remoteID, []ChannelAPIKeyOperationStatus{
			ChannelAPIKeyOperationPending,
			ChannelAPIKeyOperationCompleted,
		}).
		Updates(map[string]any{
			"status":       ChannelAPIKeyOperationCompleted,
			"completed_at": completedAt,
			"updated_at":   completedAt,
		})
	if result.Error != nil {
		return nil, result.Error
	}
	var operation ChannelAPIKeyOperation
	if err := r.db.First(&operation, operationID).Error; err != nil {
		return nil, err
	}
	if operation.OperationGeneration != operationGeneration {
		return nil, ErrChannelAPIKeyOperationGeneration
	}
	if result.RowsAffected != 1 || operation.RemoteID == nil || *operation.RemoteID != remoteID {
		return nil, ErrChannelAPIKeyRemoteIDConflict
	}
	return &operation, nil
}
