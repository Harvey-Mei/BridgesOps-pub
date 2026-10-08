package gateway

import (
	"context"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
)

func TestUpstreamInventoryPassCacheInvalidatesOnIdentityRevisionChange(t *testing.T) {
	deleted := map[int64]struct{}{42: {}}
	ctx := withUpstreamInventoryPassCache(context.Background(), 7, 3, deleted)
	cache := upstreamInventoryCacheFromContext(ctx, 7)
	cache.storeKeys([]connector.APIKey{{ID: 42}, {ID: 43}})
	cache.storeGroups([]connector.APIKeyGroup{{Name: "old-account"}})

	if keys, ok := cache.currentKeys(); !ok || len(keys) != 1 || keys[0].ID != 43 {
		t.Fatalf("initial cached keys = %#v, loaded=%v", keys, ok)
	}
	if groups, ok := cache.currentGroups(); !ok || len(groups) != 1 || groups[0].Name != "old-account" {
		t.Fatalf("initial cached groups = %#v, loaded=%v", groups, ok)
	}

	cache.bindIdentityRevision(4)
	if _, ok := cache.currentKeys(); ok {
		t.Fatal("key inventory remained loaded after identity revision changed")
	}
	if _, ok := cache.currentGroups(); ok {
		t.Fatal("group inventory remained loaded after identity revision changed")
	}
	if len(cache.deletedRemoteID) != 0 {
		t.Fatalf("deleted remote IDs survived identity change: %v", cache.deletedRemoteID)
	}
}
