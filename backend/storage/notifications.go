package storage

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Notifications struct{ db *gorm.DB }

func NewNotifications(db *gorm.DB) *Notifications { return &Notifications{db: db} }

func (r *Notifications) ListChannels() ([]NotificationChannel, error) {
	var list []NotificationChannel
	if err := r.db.Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *Notifications) ListEnabledChannels() ([]NotificationChannel, error) {
	return r.ListEnabledChannelsContext(context.Background())
}

func (r *Notifications) ListEnabledChannelsContext(ctx context.Context) ([]NotificationChannel, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var list []NotificationChannel
	if err := r.db.WithContext(ctx).Where("enabled = ?", true).Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *Notifications) FindChannel(id uint) (*NotificationChannel, error) {
	var c NotificationChannel
	if err := r.db.First(&c, id).Error; err != nil {
		return nil, err
	}
	return &c, nil
}

func (r *Notifications) CreateChannel(c *NotificationChannel) error { return r.db.Create(c).Error }
func (r *Notifications) UpdateChannel(c *NotificationChannel) error { return r.db.Save(c).Error }
func (r *Notifications) DeleteChannel(id uint) error {
	return r.db.Delete(&NotificationChannel{}, id).Error
}

func (r *Notifications) AppendLog(l *NotificationLog) error {
	return r.AppendLogContext(context.Background(), l)
}

func (r *Notifications) AppendLogContext(ctx context.Context, l *NotificationLog) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if l.SentAt.IsZero() {
		l.SentAt = time.Now()
	}
	return r.db.WithContext(ctx).Create(l).Error
}

func (r *Notifications) ListLogs(limit int) ([]NotificationLog, error) {
	if limit <= 0 {
		limit = 100
	}
	var list []NotificationLog
	if err := r.db.Order("sent_at DESC").Limit(limit).Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *Notifications) ListLogsPage(page, pageSize int) ([]NotificationLog, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	var total int64
	if err := r.db.Model(&NotificationLog{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []NotificationLog
	if err := r.db.Order("sent_at DESC").
		Offset((page - 1) * pageSize).
		Limit(pageSize).
		Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// DeleteLogsBefore 删除 sent_at < cutoff 的通知日志，返回删除行数。
func (r *Notifications) DeleteLogsBefore(cutoff time.Time) (int64, error) {
	res := r.db.Where("sent_at < ?", cutoff).Delete(&NotificationLog{})
	return res.RowsAffected, res.Error
}

// TryClaimCooldown 原子地尝试占用 (channelID, event) 的发送名额。
//
// 语义：
//   - 不存在该记录 → 插入 (last_sent_at=now)，返回 true（应该发送）
//   - 存在但 last_sent_at < now - cooldown → 更新成 now，返回 true
//   - 存在且仍在冷却窗口 → 不动，返回 false（跳过发送）
//
// 通过"先更新过期记录，再插入新记录"完成原子占用，避免并发扫描下重复发送。
//
// cooldown <= 0 时直接返回 true 不写表。
func (r *Notifications) TryClaimCooldown(channelID uint, event NotificationEvent, cooldown time.Duration) (bool, error) {
	claimed, _, err := r.TryClaimCooldownContext(context.Background(), channelID, event, cooldown)
	return claimed, err
}

// TryClaimCooldownContext is the cancelable form used while a channel account
// identity lock is held. claimedAt is the exact persisted claim token and may
// be passed to ReleaseCooldownClaimContext only when no network send began.
func (r *Notifications) TryClaimCooldownContext(ctx context.Context, channelID uint, event NotificationEvent, cooldown time.Duration) (bool, time.Time, error) {
	if cooldown <= 0 {
		return true, time.Time{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	threshold := now.Add(-cooldown)

	db := r.db.WithContext(ctx)
	res := db.Model(&NotificationCooldown{}).
		Where("channel_id = ? AND event = ? AND last_sent_at < ?", channelID, event, threshold).
		Updates(map[string]any{
			"last_sent_at": now,
			"updated_at":   now,
		})
	if res.Error != nil {
		return false, time.Time{}, res.Error
	}
	if res.RowsAffected > 0 {
		return true, now, nil
	}

	res = db.Clauses(clause.OnConflict{DoNothing: true}).Create(&NotificationCooldown{
		ChannelID:  channelID,
		Event:      event,
		LastSentAt: now,
		UpdatedAt:  now,
	})
	if res.Error != nil {
		return false, time.Time{}, res.Error
	}
	if res.RowsAffected == 0 {
		return false, time.Time{}, nil
	}
	return true, now, nil
}

// ReleaseCooldownClaimContext conditionally removes only the caller's exact
// claim. A later successful claimant is never cleared by a delayed cancel.
func (r *Notifications) ReleaseCooldownClaimContext(ctx context.Context, channelID uint, event NotificationEvent, claimedAt time.Time) error {
	if claimedAt.IsZero() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return r.db.WithContext(ctx).
		Where("channel_id = ? AND event = ? AND last_sent_at = ?", channelID, event, claimedAt.UTC().Truncate(time.Microsecond)).
		Delete(&NotificationCooldown{}).Error
}

// ResetCooldown 删除某个 (channelID, event) 的冷却记录。
// 主要给测试 / 调试用，业务路径不需要主动调用。
func (r *Notifications) ResetCooldown(channelID uint, event NotificationEvent) error {
	return r.db.Where("channel_id = ? AND event = ?", channelID, event).
		Delete(&NotificationCooldown{}).Error
}
