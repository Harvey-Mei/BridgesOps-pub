package storage

import (
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CodexRadarStore 持久化当前 Radar 状态和模型历史点位。
type CodexRadarStore struct{ db *gorm.DB }

func NewCodexRadarStore(db *gorm.DB) *CodexRadarStore { return &CodexRadarStore{db: db} }

func (s *CodexRadarStore) LoadState(sourceURL string) (*CodexRadarState, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("codex radar store is nil")
	}
	var state CodexRadarState
	if err := s.db.Where("source_url = ?", sourceURL).First(&state).Error; err != nil {
		return nil, err
	}
	return &state, nil
}

// ListModelPoints 返回指定时间之后的历史点，按时间正序排列，方便前端直接绘图。
func (s *CodexRadarStore) ListModelPoints(sourceURL string, since time.Time) ([]CodexRadarModelPoint, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("codex radar store is nil")
	}
	var points []CodexRadarModelPoint
	query := s.db.Where("source_url = ?", sourceURL)
	if !since.IsZero() {
		query = query.Where("observed_at >= ?", since)
	}
	if err := query.Order("observed_at ASC").Order("id ASC").Find(&points).Error; err != nil {
		return nil, err
	}
	return points, nil
}

// SaveSuccess 更新当前状态，并以 snapshot hash + config key 幂等写入历史点。
// payload 可以包含较大的公开摘要，因此状态字段使用 LONGTEXT。
func (s *CodexRadarStore) SaveSuccess(sourceURL, payload, payloadHash, modelHash string, fetchedAt time.Time, points []CodexRadarModelPoint) error {
	if s == nil || s.db == nil {
		return errors.New("codex radar store is nil")
	}
	if fetchedAt.IsZero() {
		fetchedAt = time.Now().UTC()
	}
	return s.db.Transaction(func(tx *gorm.DB) error {
		var state CodexRadarState
		err := tx.Where("source_url = ?", sourceURL).First(&state).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			state = CodexRadarState{SourceURL: sourceURL}
		case err != nil:
			return err
		}
		state.PayloadJSON = payload
		state.PayloadHash = payloadHash
		state.ModelHash = modelHash
		fetched := fetchedAt
		state.FetchedAt = &fetched
		state.LastAttemptAt = &fetched
		state.LastError = ""
		if state.ID == 0 {
			if err := tx.Create(&state).Error; err != nil {
				return err
			}
		} else if err := tx.Save(&state).Error; err != nil {
			return err
		}

		for index := range points {
			points[index].SourceURL = sourceURL
			points[index].SnapshotHash = modelHash
			if points[index].ObservedAt.IsZero() {
				points[index].ObservedAt = fetchedAt
			}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&points[index]).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// SaveAttempt 只更新当前状态的探测元数据，不创建历史模型点。
func (s *CodexRadarStore) SaveAttempt(sourceURL string, attemptedAt time.Time, lastError string) error {
	if s == nil || s.db == nil {
		return errors.New("codex radar store is nil")
	}
	if attemptedAt.IsZero() {
		attemptedAt = time.Now().UTC()
	}
	var state CodexRadarState
	err := s.db.Where("source_url = ?", sourceURL).First(&state).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		state = CodexRadarState{SourceURL: sourceURL}
	case err != nil:
		return err
	}
	state.LastAttemptAt = &attemptedAt
	state.LastError = lastError
	if state.ID == 0 {
		return s.db.Create(&state).Error
	}
	return s.db.Model(&state).Updates(map[string]any{
		"last_attempt_at": attemptedAt,
		"last_error":      lastError,
	}).Error
}

func (s *CodexRadarStore) DeleteHistoryBefore(cutoff time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("codex radar store is nil")
	}
	result := s.db.Where("observed_at < ?", cutoff).Delete(&CodexRadarModelPoint{})
	return result.RowsAffected, result.Error
}
