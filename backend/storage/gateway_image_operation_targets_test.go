package storage

import (
	"errors"
	"testing"
	"time"
)

func TestImageOperationTargetInventoryUsesCurrentKeyModelFactsWithoutRates(t *testing.T) {
	db := openTestDB(t)
	channel := Channel{
		Name: "image-target-inventory", Type: ChannelTypeNewAPI, SiteURL: "https://image-target-inventory.example",
		PasswordCipher: "test", MonitorEnabled: true,
	}
	if err := db.Create(&channel).Error; err != nil {
		t.Fatalf("create inventory channel: %v", err)
	}
	store := NewGateway(db)
	discoveredAt := time.Now().UTC().Add(-time.Minute)
	if err := store.RecordUpstreamGroupChange(channel.ID, "No Rate Image", "added", 91, "BOG no-rate", discoveredAt); err != nil {
		t.Fatalf("create no-rate current group: %v", err)
	}
	syncedAt := discoveredAt.Add(time.Second)
	key := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "No Rate Image", APIKeyCipher: "ciphertext", Enabled: false,
		LatencyProbeEnabled: false, ModelIDs: "image-alpha,image-beta", ModelsSyncedAt: &syncedAt,
	}
	if err := db.Create(&key).Error; err != nil {
		t.Fatalf("create no-rate key model facts: %v", err)
	}

	first, err := store.ListImageOperationTargetInventoryPage("", 1)
	if err != nil {
		t.Fatalf("list first target inventory page: %v", err)
	}
	if len(first.Items) != 1 || first.Items[0].ModelName != "image-alpha" || first.Items[0].ChannelID != channel.ID || first.NextCursor == "" || first.Snapshot == "" {
		t.Fatalf("first no-rate inventory page = %+v", first)
	}
	second, err := store.ListImageOperationTargetInventoryPage(first.NextCursor, 1)
	if err != nil {
		t.Fatalf("list second target inventory page: %v", err)
	}
	if len(second.Items) != 1 || second.Items[0].ModelName != "image-beta" || second.NextCursor != "" || second.Snapshot != first.Snapshot {
		t.Fatalf("second no-rate inventory page = %+v", second)
	}

	if err := db.Model(&key).Updates(map[string]any{
		"model_ids": "image-alpha,image-beta,image-gamma", "updated_at": syncedAt.Add(time.Minute),
	}).Error; err != nil {
		t.Fatalf("change inventory facts between pages: %v", err)
	}
	if _, err := store.ListImageOperationTargetInventoryPage(first.NextCursor, 1); !errors.Is(err, ErrGatewayImageOperationTargetInventorySnapshotChanged) {
		t.Fatalf("changed inventory cursor error=%v, want snapshot conflict", err)
	}
	if _, err := store.ListImageOperationTargetInventoryPage("not-a-cursor", 1); !errors.Is(err, ErrGatewayImageOperationTargetInventoryCursorInvalid) {
		t.Fatalf("invalid inventory cursor error=%v", err)
	}
	if _, err := store.ListImageOperationTargetInventoryPage("", 501); !errors.Is(err, ErrGatewayImageOperationTargetInventoryPageSizeInvalid) {
		t.Fatalf("oversized inventory page error=%v", err)
	}
}
