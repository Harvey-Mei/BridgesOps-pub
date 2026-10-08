package storage

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func observeAccountIdentity(t *testing.T, repo *Channels, channelID uint, externalID string, observedAt time.Time) {
	t.Helper()
	current, err := repo.FindByID(channelID)
	if err != nil {
		t.Fatalf("load channel: %v", err)
	}
	if _, err := repo.ObserveUpstreamAccountIdentity(UpstreamAccountIdentityObservation{
		ChannelID: channelID, ExpectedIdentityRevision: current.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: current.AdminRevision, Provider: ChannelTypeSub2API,
		APIOrigin: "https://example.com", AccountID: externalID,
		Scope: UpstreamConcurrencyScopeAccount, Source: "auth_me",
		ObservedAt: observedAt.UTC().Truncate(time.Microsecond),
	}); err != nil {
		t.Fatalf("observe upstream account identity %q: %v", externalID, err)
	}
}

func latestBindingAudit(t *testing.T, db *gorm.DB, channelID uint) GatewayChannelUpstreamAccountBindingAudit {
	t.Helper()
	var audit GatewayChannelUpstreamAccountBindingAudit
	if err := db.Where("channel_id = ?", channelID).Order("id DESC").First(&audit).Error; err != nil {
		t.Fatalf("load binding audit: %v", err)
	}
	return audit
}

// TestUpstreamAccountIdentityChangeAuditsClearedCostState proves that the one
// production path which zeroes a channel's accumulated balance and cumulative
// cost writes immutable evidence of the amounts it destroyed, in the same
// transaction as the clear.
func TestUpstreamAccountIdentityChangeAuditsClearedCostState(t *testing.T) {
	db := openTestDB(t)
	repo := NewChannels(db)
	channel := &Channel{
		Name: "identity-clear-audit", Type: ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "u", PasswordCipher: "x",
	}
	if err := repo.Create(channel); err != nil {
		t.Fatal(err)
	}

	firstObservedAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	observeAccountIdentity(t, repo, channel.ID, "account-a", firstObservedAt)

	// A first binding creates no money and must not claim it cleared any.
	firstAudit := latestBindingAudit(t, db, channel.ID)
	if firstAudit.CostStateCleared || firstAudit.RawTotalCostBefore != nil ||
		firstAudit.PreviousChannelIdentityRevision != nil || firstAudit.StateSourceBefore != "" {
		t.Fatalf("first binding audit claimed a cost-state clear: %+v", firstAudit)
	}

	balanceAt := time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Microsecond)
	costAt := balanceAt.Add(time.Second)
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"raw_balance": 67.313680180000, "actual_inventory": 67.652552,
		"funding_factor": 1.0050346486909446,
		"raw_today_cost": 3.665355635600, "actual_today_cost": 3.683805,
		"raw_total_cost": 802.566633635600, "actual_total_cost": 806.6067,
		"balance_sampled_at": balanceAt, "cost_sampled_at": costAt,
		"version": 9, "source": "advertised", "status": "ready",
	}).Error; err != nil {
		t.Fatal(err)
	}
	before, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	failureAt := costAt.Add(time.Second)
	cooldownUntil := failureAt.Add(time.Minute)
	key := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "identity-fenced", APIKeyCipher: "cipher", Enabled: true,
		ModelFamilies: "codex", ModelIDs: "model-a", ModelsSyncedAt: &failureAt,
		LastRoutingFailureKind: "transport", LastRoutingFailureAt: &failureAt,
		RoutingFailureCount: 4, RoutingCooldownUntil: &cooldownUntil, RoutingIsolatedAt: &failureAt,
		RoutingHealthUpstreamGroupID: 1, RoutingHealthPresenceGeneration: 1,
		RoutingHealthChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
	}
	if err := db.Create(key).Error; err != nil {
		t.Fatal(err)
	}
	for _, record := range []any{
		&GatewayRoutingFailure{
			ChannelID: channel.ID, GroupName: key.GroupName,
			UpstreamGroupID: 1, PresenceGeneration: 1,
			ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision, OccurredAt: failureAt,
		},
		&UpstreamAnnouncement{ChannelID: channel.ID, SourceKey: "identity-fenced", Content: "old account", FirstSeenAt: failureAt},
		&NotificationCooldown{ChannelID: channel.ID, Event: EventBalanceLow, LastSentAt: failureAt},
	} {
		if err := db.Create(record).Error; err != nil {
			t.Fatal(err)
		}
	}

	secondObservedAt := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Microsecond)
	observeAccountIdentity(t, repo, channel.ID, "account-b", secondObservedAt)

	after, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.RawTotalCost != 0 || after.ActualTotalCost != 0 || after.RawBalance != 0 || after.ActualInventory != 0 {
		t.Fatalf("account switch did not clear the cost state: %+v", after)
	}
	var invalidated GatewayUpstreamKey
	if err := db.First(&invalidated, key.ID).Error; err != nil {
		t.Fatal(err)
	}
	if invalidated.Enabled || invalidated.ModelFamilies != "" || invalidated.ModelIDs != "" ||
		invalidated.ModelsSyncedAt != nil || invalidated.LastRoutingFailureAt != nil ||
		invalidated.RoutingFailureCount != 0 || invalidated.RoutingCooldownUntil != nil ||
		invalidated.RoutingIsolatedAt != nil ||
		invalidated.RoutingHealthChannelIdentityRevision != 0 {
		t.Fatalf("account switch retained managed-key or routing state: %+v", invalidated)
	}
	for name, model := range map[string]any{
		"routing failures": &GatewayRoutingFailure{},
		"announcements":    &UpstreamAnnouncement{},
		"cooldowns":        &NotificationCooldown{},
	} {
		var count int64
		if err := db.Model(model).Where("channel_id = ?", channel.ID).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("account switch retained %d %s", count, name)
		}
	}

	audit := latestBindingAudit(t, db, channel.ID)
	if !audit.CostStateCleared {
		t.Fatalf("cleared cost state was not audited: %+v", audit)
	}
	if audit.PreviousUpstreamAccountID == nil || *audit.PreviousUpstreamAccountID == audit.UpstreamAccountID {
		t.Fatalf("audit does not identify the replaced account: %+v", audit)
	}
	if audit.PreviousChannelIdentityRevision == nil ||
		*audit.PreviousChannelIdentityRevision != audit.ChannelIdentityRevision-1 {
		t.Fatalf("audit identity revisions = %+v", audit)
	}
	if audit.CostUnitEpochBefore == nil || audit.CostUnitEpochAfter == nil ||
		*audit.CostUnitEpochAfter != *audit.CostUnitEpochBefore+1 || *audit.CostUnitEpochAfter != after.CostUnitEpoch {
		t.Fatalf("audit cost-unit epochs = %+v, state = %+v", audit, after)
	}
	if audit.CostStateVersionBefore == nil || audit.CostStateVersionAfter == nil ||
		*audit.CostStateVersionBefore != before.Version || *audit.CostStateVersionAfter != after.Version {
		t.Fatalf("audit cost-state versions = %+v, before = %d after = %d", audit, before.Version, after.Version)
	}
	if audit.StateSourceBefore != before.Source {
		t.Fatalf("audit state source = %q, want %q", audit.StateSourceBefore, before.Source)
	}
	for _, field := range []struct {
		name string
		got  *float64
		want float64
	}{
		{"funding_factor_before", audit.FundingFactorBefore, before.FundingFactor},
		{"raw_balance_before", audit.RawBalanceBefore, before.RawBalance},
		{"actual_inventory_before", audit.ActualInventoryBefore, before.ActualInventory},
		{"raw_today_cost_before", audit.RawTodayCostBefore, before.RawTodayCost},
		{"actual_today_cost_before", audit.ActualTodayCostBefore, before.ActualTodayCost},
		{"raw_total_cost_before", audit.RawTotalCostBefore, before.RawTotalCost},
		{"actual_total_cost_before", audit.ActualTotalCostBefore, before.ActualTotalCost},
	} {
		if field.got == nil || *field.got != field.want {
			t.Fatalf("audit %s = %v, want %v", field.name, field.got, field.want)
		}
	}
	if audit.BalanceSampledAtBefore == nil || !audit.BalanceSampledAtBefore.Equal(balanceAt) ||
		audit.CostSampledAtBefore == nil || !audit.CostSampledAtBefore.Equal(costAt) {
		t.Fatalf("audit sample times = %+v", audit)
	}

	// The evidence must be immutable: a later repair cannot rewrite or erase
	// the amounts that were cleared.
	if err := db.Exec("UPDATE gateway_channel_upstream_account_binding_audits SET raw_total_cost_before = 0 WHERE id = ?", audit.ID).Error; err == nil {
		t.Fatal("binding audit accepted an update")
	}
	if err := db.Exec("DELETE FROM gateway_channel_upstream_account_binding_audits WHERE id = ?", audit.ID).Error; err == nil {
		t.Fatal("binding audit accepted a delete")
	}
	var remaining int64
	if err := db.Model(&GatewayChannelUpstreamAccountBindingAudit{}).
		Where("channel_id = ? AND cost_state_cleared", channel.ID).Count(&remaining).Error; err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("cleared-cost audit rows = %d, want 1", remaining)
	}
}

// TestMarkCostUnitReviewWithoutAccountIdentityProvider proves a provider that
// cannot produce account identity evidence can still be paused for review, and
// that a provider which can is still fenced on it.
func TestMarkCostUnitReviewWithoutAccountIdentityProvider(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	repo := NewCosting(db)

	newapi := &Channel{Name: "newapi-review", Type: ChannelTypeNewAPI, SiteURL: "https://newapi.example", PasswordCipher: "x"}
	if err := channels.Create(newapi); err != nil {
		t.Fatal(err)
	}
	state, err := repo.FindState(newapi.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkCostUnitReview(newapi.ID, state.Version, "provider factor changed"); err != nil {
		t.Fatalf("mark newapi cost unit review: %v", err)
	}
	reviewed, err := repo.FindState(newapi.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reviewed.Status != "needs_review" || reviewed.LastError != "provider factor changed" ||
		reviewed.Version != state.Version+1 {
		t.Fatalf("newapi cost state = %+v", reviewed)
	}

	sub := &Channel{Name: "sub2api-review", Type: ChannelTypeSub2API, SiteURL: "https://example.com", PasswordCipher: "x"}
	if err := channels.Create(sub); err != nil {
		t.Fatal(err)
	}
	subState, err := repo.FindState(sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	// An unbound Sub2API channel has no account evidence, so the fence still
	// refuses the mark instead of silently degrading to a weaker check.
	err = repo.MarkCostUnitReview(sub.ID, subState.Version, "provider factor changed")
	if !errors.Is(err, ErrUpstreamAccountIdentityUnbound) {
		t.Fatalf("unbound sub2api review error = %v, want ErrUpstreamAccountIdentityUnbound", err)
	}
	observeAccountIdentity(t, channels, sub.ID, "account-review", time.Now().UTC().Add(-time.Minute))
	subState, err = repo.FindState(sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkCostUnitReview(sub.ID, subState.Version, "provider factor changed"); err != nil {
		t.Fatalf("mark sub2api cost unit review: %v", err)
	}
	// The strict entry point must never accept a missing account fence.
	if err := repo.MarkCostUnitReviewForIdentity(newapi.ID, reviewed.Version, newapi.UpstreamAccountIdentityRevision, 0, "x"); err == nil {
		t.Fatal("strict review entry accepted a missing account fence")
	}
}
