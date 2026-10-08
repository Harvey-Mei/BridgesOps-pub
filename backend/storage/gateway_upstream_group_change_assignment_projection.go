package storage

import (
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	assignmentBlockedReasonGroupGeneration = "上游分组不存在或代次已变化"
	assignmentBlockedReasonMissingKey      = "当前分组没有可用上游密钥"
	assignmentBlockedReasonDisabledKey     = "当前上游密钥已禁用"
	assignmentBlockedReasonProvisioningKey = "当前上游密钥仍在创建中"
	assignmentBlockedReasonMissingCipher   = "当前上游密钥凭据不可用"
)

type gatewayUpstreamGroupAssignmentKeyProjection struct {
	ChangeID           uint   `gorm:"column:change_id"`
	UpstreamGroupID    uint   `gorm:"column:upstream_group_id"`
	ChannelID          uint   `gorm:"column:channel_id"`
	PresenceGeneration uint64 `gorm:"column:presence_generation"`
	Present            bool   `gorm:"column:present"`
	KeyID              uint   `gorm:"column:key_id"`
	UpstreamKeyID      int64  `gorm:"column:upstream_key_id"`
	UpstreamKeyName    string `gorm:"column:upstream_key_name"`
	Enabled            bool   `gorm:"column:enabled"`
	Provisioning       bool   `gorm:"column:provisioning"`
	APIKeyCipher       string `gorm:"column:api_key_cipher"`
}

// supersedeStalePendingUpstreamGroupChanges keeps duplicate identities and
// retired generations out of the actionable queue. The conditional UPDATEs
// are serialization points: concurrent assignment or discovery updates
// re-check the pending predicate after acquiring the same change-row lock.
func (g *Gateway) supersedeStalePendingUpstreamGroupChanges(at time.Time) error {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`
			WITH duplicate_pending AS (
				SELECT id
				FROM (
					SELECT id,
						ROW_NUMBER() OVER (
							PARTITION BY upstream_group_id, presence_generation
							ORDER BY id DESC
						) AS identity_rank
					FROM gateway_upstream_group_changes
					WHERE status = 'pending'
						AND kind = 'added'
						AND upstream_group_id IS NOT NULL
						AND presence_generation IS NOT NULL
				) AS ranked_pending
				WHERE identity_rank > 1
			)
			UPDATE gateway_upstream_group_changes AS pending_change
			SET status = 'superseded',
				assignment_state = 'superseded',
				assignment_reason_code = 'duplicate_pending_identity',
				evaluation_token = NULL,
				evaluated_at = ?,
				resolved_at = ?,
				updated_at = ?
			FROM duplicate_pending
			WHERE pending_change.id = duplicate_pending.id`, at, at, at).Error; err != nil {
			return err
		}
		return tx.Exec(`
			UPDATE gateway_upstream_group_changes AS pending_change
			SET status = 'superseded',
				assignment_state = 'superseded',
				assignment_reason_code = 'group_generation_changed',
				evaluation_token = NULL,
				resolved_at = ?,
				updated_at = ?
			WHERE pending_change.status = 'pending'
				AND pending_change.kind = 'added'
				AND (
					pending_change.upstream_group_id IS NULL
					OR pending_change.presence_generation IS NULL
					OR NOT EXISTS (
						SELECT 1
						FROM gateway_upstream_groups AS current_group
						WHERE current_group.id = pending_change.upstream_group_id
							AND current_group.channel_id = pending_change.channel_id
							AND current_group.canonical_group_name = lower(btrim(pending_change.group_name))
							AND current_group.present = TRUE
							AND current_group.presence_generation = pending_change.presence_generation
					)
				)`, at, at).Error
	})
}

// projectUpstreamGroupChangeAssignment computes the current, non-persisted
// admission state used by the administrator change queue. The change row
// remains an immutable audit snapshot: a remote key can be recreated or
// rebound during the same presence generation without changing that history.
func (g *Gateway) projectUpstreamGroupChangeAssignment(changes []GatewayUpstreamGroupChange) error {
	changeIDs := make([]uint, 0, len(changes))
	for index := range changes {
		change := &changes[index]
		change.AssignmentActionable = false
		change.AssignmentBlockedReason = ""
		if change.Status != "pending" || change.Kind != "added" {
			continue
		}
		if change.ChannelID == 0 || change.UpstreamGroupID == nil || *change.UpstreamGroupID == 0 || change.PresenceGeneration == nil {
			change.AssignmentBlockedReason = assignmentBlockedReasonGroupGeneration
			continue
		}
		changeIDs = append(changeIDs, change.ID)
	}
	if len(changeIDs) == 0 {
		return nil
	}

	var projections []gatewayUpstreamGroupAssignmentKeyProjection
	if err := g.db.Table("gateway_upstream_group_changes AS pending_change").
		Select(`DISTINCT ON (pending_change.id)
			pending_change.id AS change_id,
			upstream_group.id AS upstream_group_id,
			upstream_group.channel_id,
			upstream_group.presence_generation,
			upstream_group.present,
			COALESCE(upstream_key.id, 0) AS key_id,
			COALESCE(upstream_key.upstream_key_id, 0) AS upstream_key_id,
			COALESCE(upstream_key.upstream_key_name, '') AS upstream_key_name,
			COALESCE(upstream_key.enabled, FALSE) AS enabled,
			COALESCE(upstream_key.provisioning, FALSE) AS provisioning,
			COALESCE(upstream_key.api_key_cipher, '') AS api_key_cipher`).
		Joins(`JOIN gateway_upstream_groups AS upstream_group
			ON upstream_group.id = pending_change.upstream_group_id
			AND upstream_group.channel_id = pending_change.channel_id
			AND upstream_group.canonical_group_name = lower(btrim(pending_change.group_name))`).
		Joins(`LEFT JOIN gateway_upstream_keys AS upstream_key
			ON upstream_group.channel_id = upstream_key.channel_id
			AND upstream_group.canonical_group_name = lower(btrim(upstream_key.group_name))`).
		Where("pending_change.id IN ? AND pending_change.status = ? AND pending_change.kind = ?", changeIDs, "pending", "added").
		Order(`pending_change.id ASC,
			upstream_key.enabled DESC,
			upstream_key.provisioning ASC,
			CASE WHEN btrim(upstream_key.api_key_cipher) <> '' THEN 1 ELSE 0 END DESC,
			upstream_key.models_synced_at DESC NULLS LAST,
			upstream_key.id DESC`).
		Scan(&projections).Error; err != nil {
		return err
	}
	projectionsByChangeID := make(map[uint]gatewayUpstreamGroupAssignmentKeyProjection, len(projections))
	for _, projection := range projections {
		projectionsByChangeID[projection.ChangeID] = projection
	}

	for index := range changes {
		change := &changes[index]
		if change.Status != "pending" || change.Kind != "added" || change.UpstreamGroupID == nil || change.PresenceGeneration == nil {
			continue
		}
		projection, found := projectionsByChangeID[change.ID]
		if !found || !projection.Present || projection.ChannelID != change.ChannelID ||
			projection.UpstreamGroupID != *change.UpstreamGroupID ||
			*change.PresenceGeneration != projection.PresenceGeneration {
			change.AssignmentBlockedReason = assignmentBlockedReasonGroupGeneration
			continue
		}

		if projection.KeyID == 0 {
			change.UpstreamKeyID = 0
			change.UpstreamKeyName = ""
			change.AssignmentBlockedReason = assignmentBlockedReasonMissingKey
			continue
		}
		change.UpstreamKeyID = projection.UpstreamKeyID
		change.UpstreamKeyName = strings.TrimSpace(projection.UpstreamKeyName)
		switch {
		case projection.Provisioning:
			change.AssignmentBlockedReason = assignmentBlockedReasonProvisioningKey
		case !projection.Enabled:
			change.AssignmentBlockedReason = assignmentBlockedReasonDisabledKey
		case strings.TrimSpace(projection.APIKeyCipher) == "":
			change.AssignmentBlockedReason = assignmentBlockedReasonMissingCipher
		default:
			change.AssignmentActionable = true
		}
	}
	return nil
}
