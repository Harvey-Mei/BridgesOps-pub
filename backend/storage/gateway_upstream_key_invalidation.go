package storage

import (
	"context"
	"errors"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrGatewayUpstreamKeyInvalidationFenceChanged = errors.New("upstream key invalidation identity changed")
var ErrGatewayUpstreamKeyAuthenticationInvalidated = errors.New("upstream key authentication is invalidated for the current channel identity")

// GatewayUpstreamKeyInvalidationFence identifies the exact local secret that
// produced an authentication failure. Group presence alone is insufficient:
// an account switch or in-generation key rotation may replace the key while a
// request is still in flight.
type GatewayUpstreamKeyInvalidationFence struct {
	ChannelID               uint
	ChannelIdentityRevision uint64
	UpstreamGroupID         uint
	PresenceGeneration      uint64
	CanonicalGroupName      string
	LocalKeyID              uint
	KeyGeneration           uint64
}

func NewGatewayUpstreamKeyInvalidationFence(generation UpstreamGroupGeneration, channelIdentityRevision uint64, localKeyID uint, keyGeneration uint64) GatewayUpstreamKeyInvalidationFence {
	return GatewayUpstreamKeyInvalidationFence{
		ChannelID:               generation.ChannelID,
		ChannelIdentityRevision: channelIdentityRevision,
		UpstreamGroupID:         generation.ID,
		PresenceGeneration:      generation.PresenceGeneration,
		CanonicalGroupName:      strings.TrimSpace(generation.CanonicalGroupName),
		LocalKeyID:              localKeyID,
		KeyGeneration:           keyGeneration,
	}
}

func (f GatewayUpstreamKeyInvalidationFence) Valid() bool {
	return f.ChannelID > 0 && f.ChannelIdentityRevision > 0 && f.UpstreamGroupID > 0 &&
		f.PresenceGeneration > 0 && strings.TrimSpace(f.CanonicalGroupName) != "" &&
		f.LocalKeyID > 0 && f.KeyGeneration > 0
}

func (f GatewayUpstreamKeyInvalidationFence) UpstreamGeneration() UpstreamGroupGeneration {
	return UpstreamGroupGeneration{
		ID:                 f.UpstreamGroupID,
		ChannelID:          f.ChannelID,
		CanonicalGroupName: f.CanonicalGroupName,
		PresenceGeneration: f.PresenceGeneration,
	}
}

// DisableUpstreamKeyForIdentity invalidates only the exact account, group and
// local key generation that issued the observed 401/403 response.
func (g *Gateway) DisableUpstreamKeyForIdentity(fence GatewayUpstreamKeyInvalidationFence, msg string) error {
	return g.DisableUpstreamKeyForIdentityContext(context.Background(), fence, msg)
}

// DisableUpstreamKeyForIdentityContext serializes exact-identity
// invalidation with the final dispatch fence. A caller already holding the
// channel operation lock must pass its marked context so this helper does not
// re-enter the non-reentrant PostgreSQL advisory lock.
func (g *Gateway) DisableUpstreamKeyForIdentityContext(ctx context.Context, fence GatewayUpstreamKeyInvalidationFence, msg string) error {
	if g == nil || g.db == nil || !fence.Valid() {
		return ErrGatewayUpstreamKeyInvalidationFenceChanged
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ChannelOperationLockHeld(ctx, fence.ChannelID) {
		return g.WithContext(ctx).disableUpstreamKeyForIdentityLocked(fence, msg)
	}
	unlock, err := g.LockChannelOperation(ctx, fence.ChannelID)
	if err != nil {
		return err
	}
	defer unlock()
	lockedCtx := WithChannelOperationLockHeld(ctx, fence.ChannelID)
	return g.WithContext(lockedCtx).disableUpstreamKeyForIdentityLocked(fence, msg)
}

func (g *Gateway) disableUpstreamKeyForIdentityLocked(fence GatewayUpstreamKeyInvalidationFence, msg string) error {
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := requireChannelIdentityRevisionTx(tx, fence.ChannelID, fence.ChannelIdentityRevision); err != nil {
			return err
		}
		if err := requireUpstreamGroupGenerationTx(tx, fence.UpstreamGeneration()); err != nil {
			return err
		}

		var key GatewayUpstreamKey
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Select("id", "channel_id", "group_name", "key_generation").
			Where(
				"id = ? AND channel_id = ? AND key_generation = ? AND lower(btrim(group_name)) = ?",
				fence.LocalKeyID, fence.ChannelID, fence.KeyGeneration, fence.CanonicalGroupName,
			).
			First(&key).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrGatewayUpstreamKeyInvalidationFenceChanged
		}
		if err != nil {
			return err
		}
		result := tx.Model(&GatewayUpstreamKey{}).
			Where("id = ? AND channel_id = ? AND key_generation = ?", fence.LocalKeyID, fence.ChannelID, fence.KeyGeneration).
			Updates(map[string]any{
				"api_key_cipher": "",
				"api_key_prefix": "",
				"authentication_invalidated_channel_identity_revision": fence.ChannelIdentityRevision,
				"ownership_provenance":                                 GatewayUpstreamKeyProvenanceUnknown,
				"enabled":                                              false,
				"provisioning":                                         false,
				"provisioning_started_at":                              nil,
				"last_error":                                           msg,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayUpstreamKeyInvalidationFenceChanged
		}
		return nil
	})
}
