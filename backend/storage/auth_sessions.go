package storage

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

type AuthSessions struct{ db *gorm.DB }

func NewAuthSessions(db *gorm.DB) *AuthSessions { return &AuthSessions{db: db} }

func (r *AuthSessions) WithContext(ctx context.Context) *AuthSessions {
	if r == nil || r.db == nil {
		return r
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &AuthSessions{db: r.db.WithContext(ctx)}
}

// FindByChannel 取渠道的会话凭据。返回 (nil, nil) 表示尚无 session。
func (r *AuthSessions) FindByChannel(channelID uint) (*AuthSession, error) {
	var s AuthSession
	err := r.db.First(&s, "channel_id = ?", channelID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (r *AuthSessions) ListByChannels(channelIDs []uint) (map[uint]AuthSession, error) {
	out := make(map[uint]AuthSession, len(channelIDs))
	if len(channelIDs) == 0 {
		return out, nil
	}
	var sessions []AuthSession
	if err := r.db.Where("channel_id IN ?", channelIDs).Find(&sessions).Error; err != nil {
		return nil, err
	}
	for _, session := range sessions {
		out[session.ChannelID] = session
	}
	return out, nil
}

// Upsert 写入或更新会话。
func (r *AuthSessions) Upsert(s *AuthSession) error {
	return upsertAuthSession(r.db, s)
}

// UpsertForChannelIdentityRevision prevents a login or refresh started with
// an old channel configuration from recreating a session after an
// administrator changed that channel's account identity. It locks the same
// channel row used by the configuration update before writing the session.
func (r *AuthSessions) UpsertForChannelIdentityRevision(channelID uint, expectedRevision uint64, s *AuthSession) error {
	if r == nil || r.db == nil || s == nil || channelID == 0 || s.ChannelID != channelID || expectedRevision == 0 || expectedRevision > maxUpstreamAccountIdentityRevision {
		return errors.New("channel identity-bound auth session is invalid")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		channel, err := requireOrdinaryChannelWriteTx(tx, channelID)
		if err != nil {
			return err
		}
		if channel.UpstreamAccountIdentityRevision != expectedRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		return upsertAuthSession(tx, s)
	})
}

func upsertAuthSession(db *gorm.DB, s *AuthSession) error {
	var existing AuthSession
	err := db.First(&existing, "channel_id = ?", s.ChannelID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return db.Create(s).Error
	}
	if err != nil {
		return err
	}
	return db.Save(s).Error
}

// Delete 删除渠道的 session（例如手动重置）。
func (r *AuthSessions) Delete(channelID uint) error {
	return r.db.Delete(&AuthSession{}, "channel_id = ?", channelID).Error
}
