package storage

import (
	"errors"
	"testing"
	"time"
)

func TestChannelsUpdatePreservesConcurrentFinancialProjection(t *testing.T) {
	db := openTestDB(t)
	repo := NewChannels(db)
	channel := &Channel{
		Name: "stale-channel-update", Type: ChannelTypeNewAPI,
		SiteURL: "https://example.com", Username: "before", PasswordCipher: "secret",
	}
	if err := repo.Create(channel); err != nil {
		t.Fatal(err)
	}

	stale, err := repo.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	sampledAt := time.Now().UTC().Truncate(time.Microsecond)
	retryAt := sampledAt.Add(5 * time.Minute)
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).Updates(map[string]any{
		"raw_last_balance":          67.25,
		"last_balance":              67.5,
		"last_balance_at":           sampledAt,
		"funding_factor":            1.005,
		"funding_factor_source":     "cost_unit_rebase",
		"raw_today_cost":            3.5,
		"today_cost":                3.5175,
		"raw_total_cost":            802.5,
		"total_cost":                85.1,
		"today_cost_at":             sampledAt,
		"rate_snapshot_observed_at": sampledAt,
		"last_error":                "new sync error",
		"sync_failure_count":        2,
		"next_sync_retry_at":        retryAt,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"raw_balance": 67.25, "actual_inventory": 67.5,
		"funding_factor": 1.005, "raw_today_cost": 3.5, "actual_today_cost": 3.5175,
		"raw_total_cost": 802.5, "actual_total_cost": 85.1,
		"balance_sampled_at": sampledAt, "cost_sampled_at": sampledAt,
		"observed_provider_factor": 1.005, "observed_provider_factor_source": "advertised",
		"observed_provider_factor_sampled_at": sampledAt,
		"version":                             7, "source": "cost_unit_rebase", "status": "ready",
	}).Error; err != nil {
		t.Fatal(err)
	}
	stateBefore, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}

	stale.Name = "configuration edit"
	stale.LastError = "stale error"
	stale.SyncFailureCount = 0
	stale.NextSyncRetryAt = nil
	if err := repo.UpdateAdminFields(stale, ChannelAdminFieldName); err != nil {
		t.Fatal(err)
	}
	stale, err = repo.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}

	if stale.Name != "configuration edit" || stale.Username != "before" {
		t.Fatalf("configuration update was not persisted: %+v", stale)
	}
	if stale.LastBalance == nil || *stale.LastBalance != 67.5 ||
		stale.RawLastBalance == nil || *stale.RawLastBalance != 67.25 ||
		stale.FundingFactor != 1.005 || stale.FundingFactorSource != "cost_unit_rebase" ||
		stale.TodayCost == nil || *stale.TodayCost != 3.5175 ||
		stale.RawTodayCost == nil || *stale.RawTodayCost != 3.5 ||
		stale.TotalCost == nil || *stale.TotalCost != 85.1 ||
		stale.RawTotalCost == nil || *stale.RawTotalCost != 802.5 ||
		stale.LastBalanceAt == nil || !stale.LastBalanceAt.Equal(sampledAt) ||
		stale.TodayCostAt == nil || !stale.TodayCostAt.Equal(sampledAt) ||
		stale.RateSnapshotObservedAt == nil || !stale.RateSnapshotObservedAt.Equal(sampledAt) ||
		stale.UpstreamAccountIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		stale.CostUnitEpoch != channel.CostUnitEpoch {
		t.Fatalf("stale configuration update overwrote financial projection: %+v", stale)
	}
	if stale.LastError != "new sync error" || stale.SyncFailureCount != 2 ||
		stale.NextSyncRetryAt == nil || !stale.NextSyncRetryAt.Equal(retryAt) {
		t.Fatalf("stale configuration update overwrote sync recovery state: %+v", stale)
	}
	stateAfter, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stateAfter.RawBalance != stateBefore.RawBalance || stateAfter.ActualInventory != stateBefore.ActualInventory ||
		stateAfter.FundingFactor != stateBefore.FundingFactor ||
		stateAfter.RawTodayCost != stateBefore.RawTodayCost || stateAfter.ActualTodayCost != stateBefore.ActualTodayCost ||
		stateAfter.RawTotalCost != stateBefore.RawTotalCost || stateAfter.ActualTotalCost != stateBefore.ActualTotalCost ||
		stateAfter.CostUnitEpoch != stateBefore.CostUnitEpoch || stateAfter.Version != stateBefore.Version ||
		stateAfter.Source != stateBefore.Source || stateAfter.Status != stateBefore.Status || stateAfter.LastError != stateBefore.LastError ||
		!sameOptionalFloat(stateAfter.ObservedProviderFactor, stateBefore.ObservedProviderFactor) ||
		stateAfter.ObservedProviderFactorSource != stateBefore.ObservedProviderFactorSource ||
		!sameOptionalTime(stateAfter.ObservedProviderFactorSampledAt, stateBefore.ObservedProviderFactorSampledAt) ||
		!sameOptionalTime(stateAfter.BalanceSampledAt, stateBefore.BalanceSampledAt) ||
		!sameOptionalTime(stateAfter.CostSampledAt, stateBefore.CostSampledAt) {
		t.Fatalf("configuration update changed canonical cost state: before=%+v after=%+v", stateBefore, stateAfter)
	}
}

func TestChannelsPasswordFieldUpdateDoesNotOverwriteConcurrentConfiguration(t *testing.T) {
	db := openTestDB(t)
	repo := NewChannels(db)
	channel := &Channel{
		Name: "credential-only-update", Type: ChannelTypeSub2API,
		SiteURL: "https://before.example.com", Username: "user", PasswordCipher: "old",
		MonitorEnabled: true,
	}
	if err := repo.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).Updates(map[string]any{
		"site_url": "https://after.example.com", "monitor_enabled": false,
	}).Error; err != nil {
		t.Fatal(err)
	}
	channel.PasswordCipher = "refreshed"
	if err := repo.UpdateAdminFields(channel, ChannelAdminFieldPasswordCipher); err != nil {
		t.Fatal(err)
	}

	stored, err := repo.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.PasswordCipher != "refreshed" || stored.SiteURL != "https://after.example.com" || stored.MonitorEnabled {
		t.Fatalf("credential-only update overwrote configuration: %+v", stored)
	}
}

func TestChannelsAdminUpdateDoesNotPersistProviderTypeFromSnapshot(t *testing.T) {
	db := openTestDB(t)
	repo := NewChannels(db)
	channel := &Channel{
		Name: "provider-change", Type: ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "user", PasswordCipher: "old",
	}
	if err := repo.Create(channel); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	observeAccountIdentity(t, repo, channel.ID, "account-a", observedAt)
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"raw_balance": 12.5, "actual_inventory": 13.5, "funding_factor": 1.08,
		"raw_total_cost": 90.0, "actual_total_cost": 97.2,
		"version": 4, "source": "advertised", "status": "ready",
	}).Error; err != nil {
		t.Fatal(err)
	}
	before, err := repo.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	stateBefore, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	identityBefore, err := repo.CurrentUpstreamAccountIdentity(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&AuthSession{ChannelID: channel.ID, UserID: "session-must-survive"}).Error; err != nil {
		t.Fatal(err)
	}
	var auditsBefore int64
	if err := db.Model(&GatewayChannelUpstreamAccountBindingAudit{}).Where("channel_id = ?", channel.ID).Count(&auditsBefore).Error; err != nil {
		t.Fatal(err)
	}

	changed := *before
	changed.Type = ChannelTypeNewAPI
	changed.Name = "configuration-only-change"
	if err := repo.UpdateAdminFields(&changed, ChannelAdminFieldName); err != nil {
		t.Fatalf("explicit administrator field update: %v", err)
	}

	after, err := repo.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	stateAfter, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	identityAfter, err := repo.CurrentUpstreamAccountIdentity(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Type != before.Type || after.Name != changed.Name ||
		after.UpstreamAccountIdentityRevision != before.UpstreamAccountIdentityRevision || after.CostUnitEpoch != before.CostUnitEpoch {
		t.Fatalf("administrator whitelist persisted unselected provider type: before=%+v after=%+v", before, after)
	}
	if stateAfter.RawBalance != stateBefore.RawBalance || stateAfter.ActualInventory != stateBefore.ActualInventory ||
		stateAfter.FundingFactor != stateBefore.FundingFactor ||
		stateAfter.RawTotalCost != stateBefore.RawTotalCost || stateAfter.ActualTotalCost != stateBefore.ActualTotalCost ||
		stateAfter.CostUnitEpoch != stateBefore.CostUnitEpoch || stateAfter.Version != stateBefore.Version ||
		stateAfter.Source != stateBefore.Source || stateAfter.Status != stateBefore.Status {
		t.Fatalf("rejected provider change mutated cost state: before=%+v after=%+v", stateBefore, stateAfter)
	}
	if identityAfter.AccountRecordID != identityBefore.AccountRecordID ||
		identityAfter.ChannelIdentityRevision != identityBefore.ChannelIdentityRevision ||
		identityAfter.Provider != identityBefore.Provider || identityAfter.Fingerprint != identityBefore.Fingerprint ||
		identityAfter.FingerprintVersion != identityBefore.FingerprintVersion || identityAfter.Source != identityBefore.Source ||
		!identityAfter.ObservedAt.Equal(identityBefore.ObservedAt) {
		t.Fatalf("rejected provider change mutated binding: before=%+v after=%+v", identityBefore, identityAfter)
	}
	var session AuthSession
	if err := db.First(&session, "channel_id = ?", channel.ID).Error; err != nil || session.UserID != "session-must-survive" {
		t.Fatalf("rejected provider change mutated auth session: session=%+v err=%v", session, err)
	}
	var auditsAfter int64
	if err := db.Model(&GatewayChannelUpstreamAccountBindingAudit{}).Where("channel_id = ?", channel.ID).Count(&auditsAfter).Error; err != nil {
		t.Fatal(err)
	}
	if auditsAfter != auditsBefore {
		t.Fatalf("rejected provider change appended binding audits: before=%d after=%d", auditsBefore, auditsAfter)
	}
}

func TestChannelDeletionPendingRejectsSyncRecoveryWriters(t *testing.T) {
	db := openTestDB(t)
	repo := NewChannels(db)
	channel := &Channel{
		Name: "pending-sync-writers", Type: ChannelTypeNewAPI,
		SiteURL: "https://example.com", PasswordCipher: "secret",
	}
	if err := repo.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare deletion error = %v", err)
	}

	writers := []struct {
		name string
		fn   func() error
	}{
		{name: "set last error clear", fn: func() error { return repo.SetLastError(channel.ID, "") }},
		{name: "mark sync failure", fn: func() error { return repo.MarkSyncFailure(channel.ID, "late failure") }},
		{name: "record retry failure", fn: func() error { return repo.RecordSyncRetryFailure(channel.ID, "late retry") }},
		{name: "record delayed retry failure", fn: func() error {
			return repo.RecordSyncRetryFailureAfter(channel.ID, "late discovery", time.Minute)
		}},
		{name: "clear sync failure", fn: func() error { return repo.ClearSyncFailure(channel.ID) }},
	}
	for _, writer := range writers {
		t.Run(writer.name, func(t *testing.T) {
			if err := writer.fn(); !errors.Is(err, ErrChannelDeletionPending) {
				t.Fatalf("error = %v, want %v", err, ErrChannelDeletionPending)
			}
		})
	}
}

func sameOptionalFloat(left, right *float64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
