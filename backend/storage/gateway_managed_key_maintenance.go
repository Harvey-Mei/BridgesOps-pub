package storage

import (
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	GatewayManagedKeyFailureHTTPStatus      = "http_status"
	GatewayManagedKeyFailureTimeout         = "timeout"
	GatewayManagedKeyFailureRecoveryPending = "recovery_pending"
)

var gatewayManagedKeyMaintenanceBackoff = [...]time.Duration{
	10 * time.Minute,
	30 * time.Minute,
	time.Hour,
	2 * time.Hour,
}

// GatewayManagedKeyMaintenanceState is a channel-scoped provider-management
// circuit breaker. It contains no key material and is deliberately separate
// from channel monitoring and routing health.
type GatewayManagedKeyMaintenanceState struct {
	ChannelID               uint       `gorm:"primaryKey" json:"channel_id"`
	ChannelIdentityRevision uint64     `gorm:"not null" json:"-"`
	FailureCount            int        `gorm:"not null;default:0" json:"failure_count"`
	RetryAt                 *time.Time `json:"retry_at,omitempty"`
	LastFailureKind         string     `gorm:"size:32;not null;default:''" json:"last_failure_kind,omitempty"`
	LastFailureStatus       int        `gorm:"not null;default:0" json:"last_failure_status,omitempty"`
	LastFailureAt           *time.Time `json:"last_failure_at,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

func (GatewayManagedKeyMaintenanceState) TableName() string {
	return "gateway_managed_key_maintenance_states"
}

func managedKeyMaintenanceDelay(failureCount int) time.Duration {
	if failureCount <= 0 {
		failureCount = 1
	}
	index := failureCount - 1
	if index >= len(gatewayManagedKeyMaintenanceBackoff) {
		index = len(gatewayManagedKeyMaintenanceBackoff) - 1
	}
	return gatewayManagedKeyMaintenanceBackoff[index]
}

// ManagedKeyMaintenanceState returns only state belonging to the currently
// selected upstream account identity. A retained row from an older identity is
// inert until the next failure overwrites it under the channel-row lock.
func (g *Gateway) ManagedKeyMaintenanceState(channelID uint, expectedIdentityRevision uint64) (*GatewayManagedKeyMaintenanceState, error) {
	if g == nil || g.db == nil || channelID == 0 || expectedIdentityRevision == 0 {
		return nil, ErrStaleChannelAccountIdentityRevision
	}
	var state GatewayManagedKeyMaintenanceState
	err := g.db.Table("gateway_managed_key_maintenance_states AS states").
		Select("states.*").
		Joins("JOIN channels ON channels.id = states.channel_id AND channels.upstream_account_identity_revision = states.channel_identity_revision").
		Where("states.channel_id = ? AND states.channel_identity_revision = ? AND states.last_failure_kind <> ?", channelID, expectedIdentityRevision, GatewayManagedKeyFailureRecoveryPending).
		First(&state).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

// RecordManagedKeyMaintenanceFailure atomically advances the bounded retry
// ladder while fencing the write to the exact upstream account identity.
func (g *Gateway) RecordManagedKeyMaintenanceFailure(channelID uint, expectedIdentityRevision uint64, kind string, status int, at time.Time) (*GatewayManagedKeyMaintenanceState, error) {
	if g == nil || g.db == nil || channelID == 0 || expectedIdentityRevision == 0 {
		return nil, ErrStaleChannelAccountIdentityRevision
	}
	if kind != GatewayManagedKeyFailureHTTPStatus && kind != GatewayManagedKeyFailureTimeout && kind != GatewayManagedKeyFailureRecoveryPending {
		return nil, errors.New("invalid managed key maintenance failure kind")
	}
	if kind == GatewayManagedKeyFailureTimeout || kind == GatewayManagedKeyFailureRecoveryPending {
		status = 0
	}
	if kind == GatewayManagedKeyFailureHTTPStatus && (status < 100 || status > 599) {
		return nil, errors.New("invalid managed key maintenance failure status")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}

	var saved GatewayManagedKeyMaintenanceState
	err := g.db.Transaction(func(tx *gorm.DB) error {
		var channel Channel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "upstream_account_identity_revision").First(&channel, channelID).Error; err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != expectedIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}

		failureCount := 1
		var current GatewayManagedKeyMaintenanceState
		findErr := tx.Where("channel_id = ?", channelID).First(&current).Error
		switch {
		case findErr == nil && current.ChannelIdentityRevision == expectedIdentityRevision && current.LastFailureKind != GatewayManagedKeyFailureRecoveryPending:
			failureCount = current.FailureCount + 1
		case findErr == nil:
			failureCount = 1
		case !errors.Is(findErr, gorm.ErrRecordNotFound):
			return findErr
		}
		retryAt := at.Add(managedKeyMaintenanceDelay(failureCount))
		saved = GatewayManagedKeyMaintenanceState{
			ChannelID: channelID, ChannelIdentityRevision: expectedIdentityRevision,
			FailureCount: failureCount, RetryAt: &retryAt,
			LastFailureKind: kind, LastFailureStatus: status, LastFailureAt: &at,
		}
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "channel_id"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"channel_identity_revision", "failure_count", "retry_at",
				"last_failure_kind", "last_failure_status", "last_failure_at", "updated_at",
			}),
		}).Create(&saved).Error
	})
	if err != nil {
		return nil, err
	}
	return &saved, nil
}

// ClearManagedKeyMaintenanceFailure clears only this control-plane state and
// refuses a late success from a previous account identity.
func (g *Gateway) ClearManagedKeyMaintenanceFailure(channelID uint, expectedIdentityRevision uint64) error {
	if g == nil || g.db == nil || channelID == 0 || expectedIdentityRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		var channel Channel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "upstream_account_identity_revision").First(&channel, channelID).Error; err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != expectedIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		return tx.Where("channel_id = ? AND channel_identity_revision = ?", channelID, expectedIdentityRevision).
			Delete(&GatewayManagedKeyMaintenanceState{}).Error
	})
}
