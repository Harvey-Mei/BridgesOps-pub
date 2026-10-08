package storage

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// requireOrdinaryChannelWriteTx is the common admission boundary for every
// non-deletion workflow that can create or refresh channel-scoped state. The
// channel row is always the first durable row lock in these transactions so a
// committed deletion prepare cannot be followed by a late ordinary write.
func requireOrdinaryChannelWriteTx(tx *gorm.DB, channelID uint) (Channel, error) {
	var channel Channel
	if tx == nil || channelID == 0 {
		return channel, errors.New("channel write admission is unavailable")
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&channel, channelID).Error; err != nil {
		return channel, err
	}
	if channel.DeletionPending {
		return channel, ErrChannelDeletionPending
	}
	return channel, nil
}

// RequireOrdinaryChannelWrite is used by service paths that must reject a
// pending channel before performing remote work or returning a cached routing
// lease. Storage writers must still call requireOrdinaryChannelWriteTx in the
// transaction that performs their mutation.
func (r *Channels) RequireOrdinaryChannelWrite(ctx context.Context, channelID uint) error {
	if r == nil || r.db == nil || channelID == 0 {
		return errors.New("channel write admission is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		_, err := requireOrdinaryChannelWriteTx(tx, channelID)
		return err
	})
}

// RequirePendingDeletionFenceContext is the context-aware read fence used
// around remote deletion-coordinator calls. The transaction is intentionally
// short; durable per-key claim epochs fence work across the remote round trip.
func (r *Channels) RequirePendingDeletionFenceContext(ctx context.Context, channelID uint, deletionEpoch, identityRevision uint64) (*Channel, error) {
	if r == nil || r.db == nil || ctx == nil || channelID == 0 || deletionEpoch == 0 || identityRevision == 0 {
		return nil, ErrStaleChannelDeletionFence
	}
	var channel Channel
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		channel, err = requireChannelDeletionWorkflowTx(tx, channelID, deletionEpoch, identityRevision)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &channel, nil
}

// requireChannelDeletionWorkflowTx is the only admission path that permits a
// write while deletion_pending is true. Callers must supply the complete
// immutable deletion fence; ordinary writers must never call this helper.
func requireChannelDeletionWorkflowTx(tx *gorm.DB, channelID uint, deletionEpoch, identityRevision uint64) (Channel, error) {
	channel, err := requirePendingChannelDeletionFenceTx(tx, channelID, deletionEpoch, identityRevision)
	if err != nil {
		return Channel{}, err
	}
	return channel, nil
}
