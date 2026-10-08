package storage

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// resetChannelModelFactsForIdentityChangeTx removes account-scoped discovery
// facts before the channel revision advances. Presence identities and manual
// service-tier configuration remain, but no model fact from the old account is
// eligible for routing, probing, or automatic assignment under the new one.
func resetChannelModelFactsForIdentityChangeTx(tx *gorm.DB, channelID uint, at time.Time) error {
	if tx == nil || channelID == 0 {
		return ErrUpstreamGroupGenerationChanged
	}
	at = at.UTC()
	if at.IsZero() {
		at = time.Now().UTC()
	}
	if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
		return err
	}
	groups := []GatewayUpstreamGroup{}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "channel_id", "canonical_group_name", "present", "presence_generation").
		Where("channel_id = ?", channelID).
		Order("canonical_group_name ASC, id ASC").
		Find(&groups).Error; err != nil {
		return err
	}
	if err := retireChannelAutomaticModelFactMembershipTx(tx, groups, at); err != nil {
		return err
	}
	if err := resetChannelAutomaticModelFactChangesTx(tx, groups, at); err != nil {
		return err
	}
	if err := tx.Model(&GatewayUpstreamKey{}).Where("channel_id = ?", channelID).Updates(map[string]any{
		"model_families":                        "",
		"model_ids":                             "",
		"models_synced_at":                      nil,
		"models_error":                          "",
		"model_facts_upstream_group_id":         0,
		"model_facts_presence_generation":       0,
		"model_facts_channel_identity_revision": 0,
		"updated_at":                            at,
	}).Error; err != nil {
		return err
	}
	if err := tx.Where("channel_id = ?", channelID).Delete(&GatewayModelCapability{}).Error; err != nil {
		return err
	}
	return tx.Model(&GatewayUpstreamGroup{}).Where("channel_id = ?", channelID).Updates(map[string]any{
		"model_family_codes":                    gatewayJSONB([]string{}),
		"model_ids":                             gatewayJSONB([]string{}),
		"unclassified_model_ids":                gatewayJSONB([]string{}),
		"ambiguous_model_ids":                   gatewayJSONB([]string{}),
		"gateway_apps":                          gatewayJSONB([]string{}),
		"gateway_app_sources":                   gatewayJSONB(map[string]string{}),
		"probed_gateway_apps":                   gatewayJSONB([]string{}),
		"capability_profile":                    gatewayJSONB(map[string]map[string]any{}),
		"classification_version":                "",
		"facts_complete":                        false,
		"incomplete_reasons":                    gatewayJSONB([]string{"channel_identity_changed"}),
		"facts_version":                         gorm.Expr("facts_version + 1"),
		"source_revision":                       "",
		"models_synced_at":                      nil,
		"facts_built_at":                        nil,
		"identity_conflict":                     false,
		"model_facts_channel_identity_revision": 0,
		"updated_at":                            at,
	}).Error
}

func retireChannelAutomaticModelFactMembershipTx(tx *gorm.DB, groups []GatewayUpstreamGroup, at time.Time) error {
	groupIDs := make([]uint, 0, len(groups))
	for _, group := range groups {
		if group.ID > 0 {
			groupIDs = append(groupIDs, group.ID)
		}
	}
	if len(groupIDs) == 0 {
		return nil
	}

	claims := []GatewayCandidateGroupMembershipClaim{}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("upstream_group_id IN ? AND source_kind = ? AND active = ?", groupIDs, "auto_rule", true).
		Order("candidate_group_id ASC, upstream_group_id ASC, id ASC").
		Find(&claims).Error; err != nil {
		return err
	}
	if len(claims) == 0 {
		return nil
	}

	claimIDs := make([]uint, 0, len(claims))
	affectedCandidates := make(map[uint]struct{})
	affectedMembers := make(map[[2]uint]struct{})
	for _, claim := range claims {
		claimIDs = append(claimIDs, claim.ID)
		affectedCandidates[claim.CandidateGroupID] = struct{}{}
		affectedMembers[[2]uint{claim.CandidateGroupID, claim.UpstreamGroupID}] = struct{}{}
	}
	if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("id IN ?", claimIDs).
		Updates(map[string]any{"active": false, "revoked_at": at}).Error; err != nil {
		return err
	}
	for identity := range affectedMembers {
		var remaining int64
		if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
			Where("candidate_group_id = ? AND upstream_group_id = ? AND active = ?", identity[0], identity[1], true).
			Count(&remaining).Error; err != nil {
			return err
		}
		if remaining == 0 {
			if err := tx.Where("candidate_group_id = ? AND upstream_group_id = ?", identity[0], identity[1]).
				Delete(&GatewayCandidateGroupMember{}).Error; err != nil {
				return err
			}
		}
	}
	candidateIDs := make([]uint, 0, len(affectedCandidates))
	for candidateID := range affectedCandidates {
		candidateIDs = append(candidateIDs, candidateID)
	}
	return tx.Model(&GatewayCandidateGroup{}).Where("id IN ?", candidateIDs).
		UpdateColumn("membership_version", gorm.Expr("membership_version + 1")).Error
}

func resetChannelAutomaticModelFactChangesTx(tx *gorm.DB, groups []GatewayUpstreamGroup, at time.Time) error {
	for _, group := range groups {
		if !group.Present || group.ID == 0 || group.PresenceGeneration == 0 {
			continue
		}
		pending := []GatewayUpstreamGroupChange{}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where(
				"upstream_group_id = ? AND presence_generation = ? AND kind = ? AND status = ?",
				group.ID, group.PresenceGeneration, "added", "pending",
			).
			Order("id DESC").Find(&pending).Error; err != nil {
			return err
		}
		if len(pending) == 0 {
			change := newUpstreamGroupChange(group.ChannelID, group.CanonicalGroupName, "added", 0, "", at)
			change.UpstreamGroupID = &group.ID
			change.PresenceGeneration = &group.PresenceGeneration
			change.AssignmentState = "awaiting_facts"
			change.AssignmentReasonCode = "channel_identity_changed"
			if err := tx.Create(change).Error; err != nil {
				return err
			}
			continue
		}
		if err := tx.Model(&pending[0]).Updates(map[string]any{
			"assignment_state":              "awaiting_facts",
			"assignment_reason_code":        "channel_identity_changed",
			"suggested_candidate_group_ids": gatewayJSONB([]uint{}),
			"evaluated_at":                  nil,
			"evaluation_token":              nil,
			"facts_digest":                  "",
			"resolved_at":                   nil,
			"updated_at":                    at,
		}).Error; err != nil {
			return err
		}
		if len(pending) > 1 {
			staleIDs := make([]uint, 0, len(pending)-1)
			for _, stale := range pending[1:] {
				staleIDs = append(staleIDs, stale.ID)
			}
			if err := tx.Model(&GatewayUpstreamGroupChange{}).Where("id IN ?", staleIDs).Updates(map[string]any{
				"status":                 "superseded",
				"assignment_state":       "superseded",
				"assignment_reason_code": "channel_identity_changed_duplicate",
				"evaluated_at":           at,
				"resolved_at":            at,
				"updated_at":             at,
			}).Error; err != nil {
				return err
			}
		}
	}
	return nil
}
