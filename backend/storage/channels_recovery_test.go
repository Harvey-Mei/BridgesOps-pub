package storage

import (
	"testing"
	"time"
)

func TestChannelsPersistSyncRecoveryCadence(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name:           "sync-recovery",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "operator",
		PasswordCipher: "cipher",
		MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if err := channels.MarkSyncFailure(channel.ID, "login failed"); err != nil {
		t.Fatalf("mark sync failure: %v", err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find channel after mark: %v", err)
	}
	if stored.LastError != "login failed" || stored.SyncFailureCount != 0 || stored.NextSyncRetryAt == nil {
		t.Fatalf("initial recovery state = %+v", stored)
	}
	due, err := channels.ListDueSyncRetries(time.Now().Add(time.Second))
	if err != nil || len(due) != 1 || due[0].ID != channel.ID {
		t.Fatalf("due initial retry = %#v, %v", due, err)
	}

	for attempt := 1; attempt <= 3; attempt++ {
		before := time.Now()
		if err := channels.RecordSyncRetryFailure(channel.ID, "still unavailable"); err != nil {
			t.Fatalf("record retry %d: %v", attempt, err)
		}
		stored, err = channels.FindByID(channel.ID)
		if err != nil {
			t.Fatalf("find retry %d: %v", attempt, err)
		}
		if stored.SyncFailureCount != attempt || stored.NextSyncRetryAt == nil {
			t.Fatalf("retry %d state = %+v", attempt, stored)
		}
		minimumDelay := 4 * time.Minute
		if attempt == 3 {
			minimumDelay = 29 * time.Minute
		}
		if stored.NextSyncRetryAt.Before(before.Add(minimumDelay)) {
			t.Fatalf("retry %d scheduled too early: %s", attempt, stored.NextSyncRetryAt)
		}
	}

	if err := channels.ClearSyncFailure(channel.ID); err != nil {
		t.Fatalf("clear sync failure: %v", err)
	}
	stored, err = channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find cleared channel: %v", err)
	}
	if stored.LastError != "" || stored.SyncFailureCount != 0 || stored.NextSyncRetryAt != nil {
		t.Fatalf("cleared recovery state = %+v", stored)
	}
}

func TestChannelsShortRetryPreservesGeneralFailureCount(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "short-sync-recovery", Type: ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "operator",
		PasswordCipher: "cipher", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := channels.MarkSyncFailure(channel.ID, "credential failed"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := channels.RecordSyncRetryFailure(channel.ID, "credential failed"); err != nil {
			t.Fatal(err)
		}
	}
	before := time.Now()
	if err := channels.RecordSyncRetryFailureAfter(channel.ID, "temporary discovery failure", time.Minute); err != nil {
		t.Fatal(err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SyncFailureCount != 3 || stored.LastError != "temporary discovery failure" || stored.NextSyncRetryAt == nil {
		t.Fatalf("short retry state = %+v", stored)
	}
	if stored.NextSyncRetryAt.Before(before.Add(50*time.Second)) || stored.NextSyncRetryAt.After(before.Add(70*time.Second)) {
		t.Fatalf("short retry scheduled at %s", stored.NextSyncRetryAt)
	}
	if err := channels.ClearSyncFailure(channel.ID); err != nil {
		t.Fatal(err)
	}
	stored, err = channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastError != "" || stored.SyncFailureCount != 0 || stored.NextSyncRetryAt != nil {
		t.Fatalf("successful recovery did not clear short retry: %+v", stored)
	}
}

func TestChannelsSuccessfulBalanceUpdateKeepsPendingSyncRecovery(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name:           "pending-rate-recovery",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "operator",
		PasswordCipher: "cipher",
		MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := channels.MarkSyncFailure(channel.ID, "rate sync failed"); err != nil {
		t.Fatalf("mark sync failure: %v", err)
	}
	if err := channels.RecordSyncRetryFailure(channel.ID, "rate sync still failed"); err != nil {
		t.Fatalf("record retry: %v", err)
	}
	before, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find pending channel: %v", err)
	}
	if err := channels.UpdateBalance(channel.ID, 42, time.Now(), ""); err != nil {
		t.Fatalf("update balance: %v", err)
	}
	if err := channels.SetLastError(channel.ID, ""); err != nil {
		t.Fatalf("clear unrelated error: %v", err)
	}
	after, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find updated channel: %v", err)
	}
	if after.LastError != before.LastError || after.SyncFailureCount != before.SyncFailureCount || after.NextSyncRetryAt == nil || !after.NextSyncRetryAt.Equal(*before.NextSyncRetryAt) {
		t.Fatalf("balance update cleared pending rate recovery: before=%+v after=%+v", before, after)
	}
}

func TestChannelsSkipUnchangedErrorWrites(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name:           "unchanged-error",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "operator",
		PasswordCipher: "cipher",
		MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	fixed := time.Date(2026, 7, 18, 8, 0, 0, 0, time.UTC)
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).UpdateColumn("updated_at", fixed).Error; err != nil {
		t.Fatalf("set fixed timestamp: %v", err)
	}
	if err := channels.SetLastError(channel.ID, ""); err != nil {
		t.Fatalf("repeat healthy state: %v", err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find healthy channel: %v", err)
	}
	if !stored.UpdatedAt.Equal(fixed) {
		t.Fatalf("healthy no-op changed updated_at: got %s want %s", stored.UpdatedAt, fixed)
	}

	if err := channels.MarkSyncFailure(channel.ID, "upstream unavailable"); err != nil {
		t.Fatalf("mark failure: %v", err)
	}
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).UpdateColumn("updated_at", fixed).Error; err != nil {
		t.Fatalf("reset fixed timestamp: %v", err)
	}
	if err := channels.MarkSyncFailure(channel.ID, "upstream unavailable"); err != nil {
		t.Fatalf("repeat failure: %v", err)
	}
	stored, err = channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find failed channel: %v", err)
	}
	if !stored.UpdatedAt.Equal(fixed) {
		t.Fatalf("repeated failure changed updated_at: got %s want %s", stored.UpdatedAt, fixed)
	}
}
