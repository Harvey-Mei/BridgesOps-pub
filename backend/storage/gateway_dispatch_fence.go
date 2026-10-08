package storage

import (
	"errors"

	"gorm.io/gorm"
)

// FindUpstreamKeyByID returns the exact local key row referenced by a
// request-scoped lease. Route-preference lookups are unsuitable at the final
// dispatch boundary because a concurrent rotation may have installed another
// row for the same channel and group.
func (g *Gateway) FindUpstreamKeyByID(id uint) (*GatewayUpstreamKey, error) {
	if g == nil || g.db == nil || id == 0 {
		return nil, errors.New("upstream key identity is unavailable")
	}
	var key GatewayUpstreamKey
	if err := g.db.First(&key, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, err
	}
	return &key, nil
}
