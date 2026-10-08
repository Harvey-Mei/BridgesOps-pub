package gateway

import (
	"context"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
)

type upstreamInventoryPassCacheContextKey struct{}

// upstreamInventoryPassCache is owned by one ApplyUpstreamGroupFacts call.
// It deliberately lives in the context so ordinary gateway requests and later
// maintenance passes always observe fresh provider state.
type upstreamInventoryPassCache struct {
	channelID        uint
	identityRevision uint64
	keys             []connector.APIKey
	keysLoaded       bool
	groups           []connector.APIKeyGroup
	groupsLoaded     bool
	deletedRemoteID  map[int64]struct{}
}

func withUpstreamInventoryPassCache(ctx context.Context, channelID uint, identityRevision uint64, deletedRemoteIDs map[int64]struct{}) context.Context {
	if deletedRemoteIDs == nil {
		deletedRemoteIDs = make(map[int64]struct{})
	}
	return context.WithValue(ctx, upstreamInventoryPassCacheContextKey{}, &upstreamInventoryPassCache{
		channelID:        channelID,
		identityRevision: identityRevision,
		deletedRemoteID:  deletedRemoteIDs,
	})
}

func upstreamInventoryCacheFromContext(ctx context.Context, channelID uint) *upstreamInventoryPassCache {
	if ctx == nil || channelID == 0 {
		return nil
	}
	cache, _ := ctx.Value(upstreamInventoryPassCacheContextKey{}).(*upstreamInventoryPassCache)
	if cache == nil || cache.channelID != channelID {
		return nil
	}
	return cache
}

func (cache *upstreamInventoryPassCache) bindIdentityRevision(identityRevision uint64) {
	if cache == nil || identityRevision == 0 {
		return
	}
	if cache.identityRevision != 0 && cache.identityRevision != identityRevision {
		cache.keys = nil
		cache.keysLoaded = false
		cache.groups = nil
		cache.groupsLoaded = false
		cache.deletedRemoteID = make(map[int64]struct{})
	}
	cache.identityRevision = identityRevision
}

func (cache *upstreamInventoryPassCache) currentKeys() ([]connector.APIKey, bool) {
	if cache == nil || !cache.keysLoaded {
		return nil, false
	}
	items := make([]connector.APIKey, 0, len(cache.keys))
	for _, item := range cache.keys {
		if _, deleted := cache.deletedRemoteID[item.ID]; item.ID > 0 && deleted {
			continue
		}
		items = append(items, item)
	}
	return items, true
}

func (cache *upstreamInventoryPassCache) storeKeys(items []connector.APIKey) {
	if cache == nil || cache.keysLoaded {
		return
	}
	cache.keys = append([]connector.APIKey(nil), items...)
	cache.keysLoaded = true
}

func (cache *upstreamInventoryPassCache) currentGroups() ([]connector.APIKeyGroup, bool) {
	if cache == nil || !cache.groupsLoaded {
		return nil, false
	}
	return append([]connector.APIKeyGroup(nil), cache.groups...), true
}

func (cache *upstreamInventoryPassCache) storeGroups(groups []connector.APIKeyGroup) {
	if cache == nil || cache.groupsLoaded {
		return
	}
	cache.groups = append([]connector.APIKeyGroup(nil), groups...)
	cache.groupsLoaded = true
}

func markUpstreamInventoryPassKeyDeleted(ctx context.Context, channelID uint, remoteID int64) {
	if remoteID <= 0 {
		return
	}
	if cache := upstreamInventoryCacheFromContext(ctx, channelID); cache != nil {
		cache.deletedRemoteID[remoteID] = struct{}{}
	}
}

func (s *Service) listAPIKeysUsingPassCache(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	if cache := upstreamInventoryCacheFromContext(ctx, channelID); cache != nil {
		if items, ok := cache.currentKeys(); ok {
			return &connector.APIKeyPage{
				Items:    items,
				Total:    int64(len(items)),
				Page:     1,
				PageSize: len(items),
				Pages:    1,
			}, nil
		}
	}
	return s.Keys.ListAPIKeys(ctx, channelID, query)
}

func (s *Service) listAPIKeyGroupsUsingPassCache(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error) {
	cache := upstreamInventoryCacheFromContext(ctx, channelID)
	if groups, ok := cache.currentGroups(); ok {
		return groups, nil
	}
	groups, err := s.Keys.ListAPIKeyGroups(ctx, channelID)
	if err != nil {
		return nil, err
	}
	cache.storeGroups(groups)
	return groups, nil
}
