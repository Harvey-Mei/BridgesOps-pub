package storage

import (
	"context"
	"database/sql"
	"errors"

	"gorm.io/gorm"
)

// ChannelDeletionReviewSnapshot is a transactionally consistent administrator
// view of one durable deletion intent and all of its per-key claims.
type ChannelDeletionReviewSnapshot struct {
	Intent ChannelDeletionIntent
	Claims []ChannelDeletionKeyClaim
}

func (r *Channels) ChannelDeletionReview(ctx context.Context, channelID uint) (*ChannelDeletionReviewSnapshot, error) {
	if r == nil || r.db == nil || ctx == nil || channelID == 0 {
		return nil, errors.New("channel deletion review is unavailable")
	}
	var snapshot ChannelDeletionReviewSnapshot
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("channel_id = ?", channelID).
			Order("deletion_epoch DESC, id DESC").First(&snapshot.Intent).Error; err != nil {
			return err
		}
		snapshot.Claims = make([]ChannelDeletionKeyClaim, 0)
		return tx.Where("intent_id = ?", snapshot.Intent.ID).Order("id ASC").Find(&snapshot.Claims).Error
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func (r *Channels) ListChannelDeletionReviews(ctx context.Context, statuses []ChannelDeletionIntentStatus, page, pageSize int) ([]ChannelDeletionReviewSnapshot, int64, error) {
	if r == nil || r.db == nil || ctx == nil || page < 1 || pageSize < 1 || pageSize > 100 {
		return nil, 0, errors.New("channel deletion review list is unavailable")
	}
	snapshots := make([]ChannelDeletionReviewSnapshot, 0)
	var total int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Model(&ChannelDeletionIntent{})
		if len(statuses) > 0 {
			query = query.Where("status IN ?", statuses)
		}
		if err := query.Count(&total).Error; err != nil {
			return err
		}
		intents := make([]ChannelDeletionIntent, 0, pageSize)
		if err := query.Order("requested_at DESC, id DESC").
			Offset((page - 1) * pageSize).Limit(pageSize).Find(&intents).Error; err != nil {
			return err
		}
		if len(intents) == 0 {
			return nil
		}
		intentIDs := make([]uint64, 0, len(intents))
		for _, intent := range intents {
			intentIDs = append(intentIDs, intent.ID)
		}
		claims := make([]ChannelDeletionKeyClaim, 0)
		if err := tx.Where("intent_id IN ?", intentIDs).Order("intent_id ASC, id ASC").Find(&claims).Error; err != nil {
			return err
		}
		claimsByIntent := make(map[uint64][]ChannelDeletionKeyClaim, len(intents))
		for _, claim := range claims {
			claimsByIntent[claim.IntentID] = append(claimsByIntent[claim.IntentID], claim)
		}
		for _, intent := range intents {
			intentClaims := claimsByIntent[intent.ID]
			if intentClaims == nil {
				intentClaims = make([]ChannelDeletionKeyClaim, 0)
			}
			snapshots = append(snapshots, ChannelDeletionReviewSnapshot{Intent: intent, Claims: intentClaims})
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return snapshots, total, err
}
