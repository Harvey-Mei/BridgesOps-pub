package storage

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const gatewayRoutingHealthSuccessOutboxMaximumBatch = 500

// GatewayRoutingHealthSuccessOutbox preserves a real inference success until
// its exact account/group fence has been applied to adaptive routing health.
// DispatchID makes finalization retries and concurrent consumers idempotent.
type GatewayRoutingHealthSuccessOutbox struct {
	DispatchID              uint64 `gorm:"primaryKey"`
	RequestLogID            uint   `gorm:"not null"`
	ChannelID               uint   `gorm:"not null"`
	ChannelIdentityRevision uint64 `gorm:"not null"`
	UpstreamGroupID         uint   `gorm:"not null"`
	PresenceGeneration      uint64 `gorm:"not null"`
	CanonicalGroupName      string `gorm:"size:128;not null"`
	ObservedAt              time.Time
	AttemptCount            int    `gorm:"not null;default:0"`
	LastError               string `gorm:"size:512;not null;default:''"`
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

func (GatewayRoutingHealthSuccessOutbox) TableName() string {
	return "gateway_routing_health_success_outbox"
}

func gatewayRequestCarriesRoutingHealthSuccess(log *GatewayRequestLog) bool {
	if log == nil || log.ID == 0 || log.DownstreamKeyID == 0 || log.StatusCode < 200 || log.StatusCode >= 300 ||
		!IsSuccessfulGatewayRequest(log) || isGatewayRequestCountTokens(log) {
		return false
	}
	reason := strings.ToLower(strings.TrimSpace(log.RouteReason))
	switch reason {
	case gatewayRouteReasonLatencyProbe, gatewayRouteReasonKeyTest, "model_check":
		return false
	}
	return !strings.HasPrefix(reason, "internal_")
}

// enqueueGatewayRoutingHealthSuccessTx is called from dispatch finalization
// after the request log has received its immutable route snapshot. It is in the
// same transaction as that log and the dispatch terminal, so a process exit can
// never leave a committed success without either this row or a rolled-back log.
func enqueueGatewayRoutingHealthSuccessTx(tx *gorm.DB, audit *GatewayRequestDispatchAudit, log *GatewayRequestLog, facts GatewayRequestDispatchFinalizationFacts, observedAt time.Time) error {
	if !gatewayRequestCarriesRoutingHealthSuccess(log) {
		return nil
	}
	if tx == nil || audit == nil {
		return errors.New("routing health success outbox transaction is unavailable")
	}
	if audit.ID == 0 || log.ChannelID != audit.ChannelID {
		return ErrGatewayRequestDispatchConflict
	}
	fence, ok, err := gatewayRoutingHealthSuccessFenceTx(tx, audit, facts)
	if err != nil {
		return err
	}
	if !ok {
		// Legacy and fixed no-rate dispatches do not carry a trustworthy routing
		// health fence. They intentionally remain outside shared adaptive health.
		return nil
	}
	if log.ChannelID != fence.ChannelID {
		return ErrGatewayRequestDispatchConflict
	}
	if observedAt.IsZero() {
		observedAt = log.CreatedAt
	}
	if observedAt.IsZero() {
		return errors.New("routing health success observation time is required")
	}
	observedAt = observedAt.UTC().Truncate(time.Microsecond)
	row := GatewayRoutingHealthSuccessOutbox{
		DispatchID:              audit.ID,
		RequestLogID:            log.ID,
		ChannelID:               fence.ChannelID,
		ChannelIdentityRevision: fence.ChannelIdentityRevision,
		UpstreamGroupID:         fence.UpstreamGroupID,
		PresenceGeneration:      fence.PresenceGeneration,
		CanonicalGroupName:      fence.CanonicalGroupName,
		ObservedAt:              observedAt,
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "dispatch_id"}},
		DoNothing: true,
	}).Create(&row).Error
}

func gatewayRoutingHealthSuccessFenceTx(tx *gorm.DB, audit *GatewayRequestDispatchAudit, facts GatewayRequestDispatchFinalizationFacts) (GatewayRoutingHealthFence, bool, error) {
	if tx == nil || audit == nil || audit.ID == 0 || audit.ChannelID == 0 {
		return GatewayRoutingHealthFence{}, false, ErrGatewayRequestDispatchConflict
	}
	auditCanonicalGroup, err := canonicalUpstreamGroupName(tx, audit.GroupName)
	if err != nil {
		return GatewayRoutingHealthFence{}, false, err
	}
	if facts.RoutingHealthFence != nil {
		fence := *facts.RoutingHealthFence
		fenceCanonicalGroup, err := canonicalUpstreamGroupName(tx, fence.CanonicalGroupName)
		if err != nil {
			return GatewayRoutingHealthFence{}, false, err
		}
		fence.CanonicalGroupName = fenceCanonicalGroup
		if !fence.Valid() || fence.ChannelID != audit.ChannelID || fence.CanonicalGroupName != auditCanonicalGroup ||
			(audit.ChannelIdentityRevision != 0 && audit.ChannelIdentityRevision != fence.ChannelIdentityRevision) ||
			(audit.UpstreamGroupID != 0 && audit.UpstreamGroupID != fence.UpstreamGroupID) ||
			(audit.UpstreamGroupGeneration != 0 && audit.UpstreamGroupGeneration != fence.PresenceGeneration) {
			return GatewayRoutingHealthFence{}, false, ErrGatewayRequestDispatchConflict
		}
		return fence, true, nil
	}
	if audit.MultiplierProvenanceVersion != GatewayRequestDispatchMultiplierProvenanceVersion ||
		audit.ChannelIdentityRevision == 0 || audit.UpstreamGroupID == 0 || audit.UpstreamGroupGeneration == 0 ||
		audit.UpstreamKeyLocalID == 0 || audit.UpstreamKeyGeneration == 0 {
		return GatewayRoutingHealthFence{}, false, nil
	}
	fence := GatewayRoutingHealthFence{
		ChannelID: audit.ChannelID, ChannelIdentityRevision: audit.ChannelIdentityRevision,
		UpstreamGroupID: audit.UpstreamGroupID, PresenceGeneration: audit.UpstreamGroupGeneration,
		CanonicalGroupName: auditCanonicalGroup,
	}
	return fence, fence.Valid(), nil
}

// DeliverRoutingHealthSuccessOutbox performs one bounded at-least-once pass.
// Applying the success and deleting the row are deliberately separate: a
// crash between them only replays the monotonic, identity-fenced health write.
func (g *Gateway) DeliverRoutingHealthSuccessOutbox(limit int) (int, error) {
	if g == nil || g.db == nil {
		return 0, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > gatewayRoutingHealthSuccessOutboxMaximumBatch {
		limit = gatewayRoutingHealthSuccessOutboxMaximumBatch
	}
	rows := make([]GatewayRoutingHealthSuccessOutbox, 0, limit)
	if err := g.db.Order("created_at ASC, dispatch_id ASC").Limit(limit).Find(&rows).Error; err != nil {
		return 0, err
	}

	delivered := 0
	var deliveryErrors []error
	for index := range rows {
		row := rows[index]
		fence := GatewayRoutingHealthFence{
			ChannelID:               row.ChannelID,
			ChannelIdentityRevision: row.ChannelIdentityRevision,
			UpstreamGroupID:         row.UpstreamGroupID,
			PresenceGeneration:      row.PresenceGeneration,
			CanonicalGroupName:      row.CanonicalGroupName,
		}
		err := g.TouchUpstreamKeyForRoutingHealth(fence, row.ObservedAt)
		if err != nil && !gatewayRoutingHealthSuccessOutboxObsolete(err) {
			if recordErr := g.recordRoutingHealthSuccessOutboxFailure(row.DispatchID, err); recordErr != nil {
				deliveryErrors = append(deliveryErrors, fmt.Errorf("record routing health success delivery failure for dispatch %d: %w", row.DispatchID, recordErr))
			}
			deliveryErrors = append(deliveryErrors, fmt.Errorf("deliver routing health success for dispatch %d: %w", row.DispatchID, err))
			continue
		}
		if deleteErr := g.db.Where("dispatch_id = ?", row.DispatchID).Delete(&GatewayRoutingHealthSuccessOutbox{}).Error; deleteErr != nil {
			deliveryErrors = append(deliveryErrors, fmt.Errorf("delete delivered routing health success for dispatch %d: %w", row.DispatchID, deleteErr))
			continue
		}
		delivered++
	}
	return delivered, errors.Join(deliveryErrors...)
}

func gatewayRoutingHealthSuccessOutboxObsolete(err error) bool {
	return errors.Is(err, ErrUpstreamGroupGenerationChanged) ||
		errors.Is(err, ErrStaleChannelAccountIdentityRevision) ||
		errors.Is(err, ErrChannelDeletionPending) ||
		errors.Is(err, gorm.ErrRecordNotFound)
}

func (g *Gateway) recordRoutingHealthSuccessOutboxFailure(dispatchID uint64, deliveryErr error) error {
	if dispatchID == 0 {
		return errors.New("routing health success dispatch id is required")
	}
	message := "routing health success delivery failed"
	if deliveryErr != nil && strings.TrimSpace(deliveryErr.Error()) != "" {
		message = strings.TrimSpace(deliveryErr.Error())
	}
	runes := []rune(message)
	if len(runes) > 512 {
		message = string(runes[:512])
	}
	return g.db.Model(&GatewayRoutingHealthSuccessOutbox{}).
		Where("dispatch_id = ?", dispatchID).
		Updates(map[string]any{
			"attempt_count": gorm.Expr("attempt_count + 1"),
			"last_error":    message,
			"updated_at":    time.Now().UTC(),
		}).Error
}
