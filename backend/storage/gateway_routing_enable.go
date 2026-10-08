package storage

import (
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

var ErrGatewayRoutingEnableIdempotencyConflict = errors.New("routing enable idempotency key belongs to another actor")
var ErrGatewayRoutingEnableUpstreamKeyUnavailable = errors.New("upstream key is not structurally available")

// GatewayRoutingEnableAudit is append-only evidence that an administrator
// explicitly cleared routing health for one exact upstream group generation.
type GatewayRoutingEnableAudit struct {
	ID                      uint64     `gorm:"primaryKey" json:"id"`
	UpstreamGroupID         uint       `gorm:"not null;uniqueIndex:idx_gateway_routing_enable_idempotency,priority:1" json:"upstream_group_id"`
	ChannelID               uint       `gorm:"not null;index" json:"channel_id"`
	ChannelIdentityRevision uint64     `gorm:"not null;default:0;uniqueIndex:idx_gateway_routing_enable_idempotency,priority:2" json:"-"`
	CanonicalGroupName      string     `gorm:"size:128;not null" json:"canonical_group_name"`
	PresenceGeneration      uint64     `gorm:"not null;uniqueIndex:idx_gateway_routing_enable_idempotency,priority:3" json:"presence_generation"`
	Actor                   string     `gorm:"size:128;not null" json:"actor"`
	IdempotencyKey          string     `gorm:"size:128;not null;uniqueIndex:idx_gateway_routing_enable_idempotency,priority:4" json:"idempotency_key"`
	Reason                  string     `gorm:"size:256;not null" json:"reason"`
	Changed                 bool       `gorm:"not null" json:"changed"`
	PreviousFailureKind     string     `gorm:"size:32;not null" json:"previous_failure_kind"`
	PreviousFailureStatus   int        `gorm:"not null" json:"previous_failure_status"`
	PreviousFailureCount    int        `gorm:"not null" json:"previous_failure_count"`
	PreviousFailureAt       *time.Time `json:"previous_failure_at,omitempty"`
	PreviousCooldownUntil   *time.Time `json:"previous_cooldown_until,omitempty"`
	CreatedAt               time.Time  `gorm:"not null;index" json:"created_at"`
}

func (GatewayRoutingEnableAudit) TableName() string { return "gateway_routing_enable_audits" }

type GatewayRoutingEnableResult struct {
	UpstreamGroupID         uint       `json:"upstream_group_id"`
	ChannelID               uint       `json:"channel_id"`
	ChannelIdentityRevision uint64     `json:"channel_identity_revision"`
	CanonicalGroupName      string     `json:"group_name"`
	PresenceGeneration      uint64     `json:"presence_generation"`
	Changed                 bool       `json:"changed"`
	PreviousFailureKind     string     `json:"previous_failure_kind,omitempty"`
	PreviousFailureCount    int        `json:"previous_failure_count"`
	PreviousCooldownUntil   *time.Time `json:"previous_cooldown_until,omitempty"`
	IdempotentReplay        bool       `json:"idempotent_replay"`
}

func routingEnableResultFromAudit(audit GatewayRoutingEnableAudit, replay bool) *GatewayRoutingEnableResult {
	return &GatewayRoutingEnableResult{
		UpstreamGroupID:         audit.UpstreamGroupID,
		ChannelID:               audit.ChannelID,
		ChannelIdentityRevision: audit.ChannelIdentityRevision,
		CanonicalGroupName:      audit.CanonicalGroupName,
		PresenceGeneration:      audit.PresenceGeneration,
		Changed:                 audit.Changed,
		PreviousFailureKind:     audit.PreviousFailureKind,
		PreviousFailureCount:    audit.PreviousFailureCount,
		PreviousCooldownUntil:   cloneRoutingHealthTime(audit.PreviousCooldownUntil),
		IdempotentReplay:        replay,
	}
}

// latestGatewayRoutingEnableWatermarkTx returns the latest explicit operator
// enable action for one exact present group generation. The audit is also the
// durable event-time watermark: a request failure observed before this action
// must not recreate adaptive cooldown merely because it acquired the routing
// lock later.
//
// Callers must already hold the canonical routing-health advisory lock and
// have validated generation. Enable and routing-failure writers share that
// lock, so the selected audit cannot race a concurrent enable action.
func latestGatewayRoutingEnableWatermarkTx(tx *gorm.DB, generation UpstreamGroupGeneration, fences ...*GatewayRoutingHealthFence) (*time.Time, error) {
	if !generation.Valid() {
		return nil, nil
	}
	var audit GatewayRoutingEnableAudit
	query := tx.Select("created_at").
		Where("upstream_group_id = ? AND presence_generation = ?", generation.ID, generation.PresenceGeneration)
	if len(fences) > 0 && fences[0] != nil {
		query = query.Where("channel_identity_revision = ?", fences[0].ChannelIdentityRevision)
	}
	err := query.
		Order("created_at DESC, id DESC").
		First(&audit).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	watermark := audit.CreatedAt.UTC().Truncate(time.Microsecond)
	return &watermark, nil
}

// routingFailureFactsAfterWatermarkTx retains only failures that happened
// after an event-time watermark. The exact failure class is held on the
// canonical key; the fact rows are deliberately a bounded timestamp chain
// used to recalculate count and the exact third-failure isolation threshold.
func routingFailureFactsAfterWatermarkTx(tx *gorm.DB, channelID uint, canonicalGroupName string, watermark time.Time, fences ...*GatewayRoutingHealthFence) (*time.Time, *time.Time, int, error) {
	var fence *GatewayRoutingHealthFence
	if len(fences) > 0 {
		fence = fences[0]
	}
	var newest struct {
		OccurredAt *time.Time `gorm:"column:occurred_at"`
	}
	if err := gatewayRoutingFailureFactsQuery(tx.Model(&GatewayRoutingFailure{}), channelID, canonicalGroupName, fence).
		Where("occurred_at > ?", watermark).
		Select("MAX(occurred_at) AS occurred_at").
		Scan(&newest).Error; err != nil {
		return nil, nil, 0, err
	}
	if newest.OccurredAt == nil {
		return nil, nil, 0, nil
	}
	windowStart := newest.OccurredAt.Add(-10 * time.Minute)
	if err := gatewayRoutingFailureFactsQuery(tx, channelID, canonicalGroupName, fence).
		Where("occurred_at > ? AND occurred_at < ?", watermark, windowStart).
		Delete(&GatewayRoutingFailure{}).Error; err != nil {
		return nil, nil, 0, err
	}
	var count int64
	if err := gatewayRoutingFailureFactsQuery(tx.Model(&GatewayRoutingFailure{}), channelID, canonicalGroupName, fence).
		Where("occurred_at > ? AND occurred_at >= ? AND occurred_at <= ?", watermark, windowStart, *newest.OccurredAt).
		Count(&count).Error; err != nil {
		return nil, nil, 0, err
	}
	latest := newest.OccurredAt.UTC().Truncate(time.Microsecond)
	var isolatedAt *time.Time
	if int(count) >= GatewayAdaptiveRoutingFailureThreshold {
		thresholdAt, err := routingFailureIsolationThresholdAtTx(
			tx, channelID, canonicalGroupName, &watermark, windowStart, latest, fence,
		)
		if err != nil {
			return nil, nil, 0, err
		}
		isolatedAt = thresholdAt
	}
	return &latest, isolatedAt, int(count), nil
}

// EnableUpstreamGroupRouting clears only adaptive routing health for the
// requested current generation. It never changes key material or key state.
func (g *Gateway) EnableUpstreamGroupRouting(upstreamGroupID uint, presenceGeneration, channelIdentityRevision uint64, actor, idempotencyKey, reason string, at time.Time) (*GatewayRoutingEnableResult, error) {
	if upstreamGroupID == 0 || presenceGeneration == 0 || channelIdentityRevision == 0 {
		return nil, ErrUpstreamGroupGenerationChanged
	}
	actor = strings.TrimSpace(actor)
	if actor == "" || len(actor) > 128 {
		return nil, errors.New("routing enable actor is required")
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey == "" || len(idempotencyKey) > 128 {
		return nil, errors.New("routing enable idempotency key is invalid")
	}
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > 256 {
		return nil, errors.New("routing enable reason is required")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	at = at.UTC().Truncate(time.Microsecond)

	var enabled *GatewayRoutingEnableResult
	err := g.db.Transaction(func(tx *gorm.DB) error {
		var existing GatewayRoutingEnableAudit
		err := tx.Where("upstream_group_id = ? AND channel_identity_revision = ? AND presence_generation = ? AND idempotency_key = ?", upstreamGroupID, channelIdentityRevision, presenceGeneration, idempotencyKey).
			First(&existing).Error
		if err == nil {
			if existing.Actor != actor {
				return ErrGatewayRoutingEnableIdempotencyConflict
			}
			enabled = routingEnableResultFromAudit(existing, true)
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		var group GatewayUpstreamGroup
		// The routing-health helper acquires the canonical advisory lock before
		// locking this generation row; keep that global lock order here too.
		if err := tx.
			Select("id", "channel_id", "canonical_group_name", "presence_generation", "present", "discovered_at").
			First(&group, upstreamGroupID).Error; err != nil {
			return err
		}

		channel, err := requireOrdinaryChannelWriteTx(tx, group.ChannelID)
		if err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != channelIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		if !group.Present || group.PresenceGeneration != presenceGeneration {
			return ErrUpstreamGroupGenerationChanged
		}

		generation := upstreamGroupGeneration(group)
		fence := NewGatewayRoutingHealthFence(generation, channelIdentityRevision)
		domain, err := lockUpstreamRoutingHealthDomainForFenceTx(tx, fence, at)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrGatewayRoutingEnableUpstreamKeyUnavailable
		}
		if err != nil {
			return err
		}
		// A concurrent request can create the same audit while this transaction
		// waits on the canonical advisory lock. Recheck after acquiring that lock
		// so an idempotent replay never falls through to a unique-key error or
		// clears health recorded after the original action.
		err = tx.Where("upstream_group_id = ? AND channel_identity_revision = ? AND presence_generation = ? AND idempotency_key = ?", upstreamGroupID, channelIdentityRevision, presenceGeneration, idempotencyKey).
			First(&existing).Error
		if err == nil {
			if existing.Actor != actor {
				return ErrGatewayRoutingEnableIdempotencyConflict
			}
			enabled = routingEnableResultFromAudit(existing, true)
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		key := domain.Key
		if key == nil || !key.Enabled || key.Provisioning || strings.TrimSpace(key.APIKeyCipher) == "" {
			return ErrGatewayRoutingEnableUpstreamKeyUnavailable
		}

		audit := GatewayRoutingEnableAudit{
			UpstreamGroupID:         group.ID,
			ChannelID:               group.ChannelID,
			ChannelIdentityRevision: channelIdentityRevision,
			CanonicalGroupName:      group.CanonicalGroupName,
			PresenceGeneration:      group.PresenceGeneration,
			Actor:                   actor,
			IdempotencyKey:          idempotencyKey,
			Reason:                  reason,
			Changed:                 key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil || key.RoutingIsolatedAt != nil || key.RoutingFailureCount > 0 || strings.TrimSpace(key.LastRoutingFailureKind) != "",
			PreviousFailureKind:     strings.TrimSpace(key.LastRoutingFailureKind),
			PreviousFailureStatus:   key.LastRoutingFailureStatus,
			PreviousFailureCount:    key.RoutingFailureCount,
			PreviousFailureAt:       cloneRoutingHealthTime(key.LastRoutingFailureAt),
			PreviousCooldownUntil:   cloneRoutingHealthTime(key.RoutingCooldownUntil),
			CreatedAt:               at,
		}
		// The audit timestamp is a generation-bound event-time watermark. Keep
		// any failure observed after the administrator action even if it acquired
		// the lock first; the converse late writer is rejected by
		// RecordUpstreamKeyRoutingFailure after it sees this audit.
		if err := gatewayRoutingFailureFactsQuery(tx, group.ChannelID, group.CanonicalGroupName, &fence).Where("occurred_at <= ?", at).
			Delete(&GatewayRoutingFailure{}).Error; err != nil {
			return err
		}
		latestFailureAt, remainingIsolationAt, remainingFailureCount, err := routingFailureFactsAfterWatermarkTx(tx, group.ChannelID, group.CanonicalGroupName, at, &fence)
		if err != nil {
			return err
		}
		updates := map[string]any{
			"routing_failure_count": remainingFailureCount,
		}
		updates["routing_isolated_at"] = remainingIsolationAt
		manualAfterEnable := key.LastRoutingFailureKind == GatewayRoutingFailureManualCooldown &&
			key.LastRoutingFailureAt != nil && key.LastRoutingFailureAt.After(at) &&
			key.RoutingCooldownUntil != nil && at.Before(*key.RoutingCooldownUntil)
		switch {
		case manualAfterEnable:
			updates["last_routing_failure_error"] = key.LastRoutingFailureError
			updates["last_routing_failure_kind"] = key.LastRoutingFailureKind
			updates["last_routing_failure_status"] = key.LastRoutingFailureStatus
			updates["last_routing_failure_at"] = key.LastRoutingFailureAt
			updates["routing_cooldown_until"] = key.RoutingCooldownUntil
		case latestFailureAt != nil:
			lastKind := "transport"
			lastStatus := 0
			lastMessage := "adaptive routing failure after routing enable"
			if key.LastRoutingFailureAt != nil && key.LastRoutingFailureAt.Equal(*latestFailureAt) &&
				strings.TrimSpace(key.LastRoutingFailureKind) != "" && key.LastRoutingFailureKind != GatewayRoutingFailureManualCooldown {
				lastKind = key.LastRoutingFailureKind
				lastStatus = key.LastRoutingFailureStatus
				lastMessage = key.LastRoutingFailureError
			}
			updates["last_routing_failure_error"] = lastMessage
			updates["last_routing_failure_kind"] = lastKind
			updates["last_routing_failure_status"] = lastStatus
			updates["last_routing_failure_at"] = *latestFailureAt
			updates["routing_cooldown_until"] = nil
		default:
			updates["last_routing_failure_error"] = ""
			updates["last_routing_failure_kind"] = ""
			updates["last_routing_failure_status"] = 0
			updates["last_routing_failure_at"] = nil
			updates["routing_failure_count"] = 0
			updates["routing_cooldown_until"] = nil
		}
		result := tx.Model(&GatewayUpstreamKey{}).Where("id = ?", key.ID).Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrGatewayRoutingEnableUpstreamKeyUnavailable
		}
		if err := tx.Create(&audit).Error; err != nil {
			return err
		}
		enabled = routingEnableResultFromAudit(audit, false)
		return nil
	})
	return enabled, err
}
