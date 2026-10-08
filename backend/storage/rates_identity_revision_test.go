package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestRateAndCostSnapshotsRejectStaleChannelIdentity(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	channel := &Channel{Name: "stale-rate-identity", Type: ChannelTypeNewAPI, SiteURL: "https://stale-rate.example"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	oldRevision := channel.UpstreamAccountIdentityRevision
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).
		Update("upstream_account_identity_revision", oldRevision+1).Error; err != nil {
		t.Fatal(err)
	}

	when := time.Now().UTC()
	if err := rates.AppendBalanceForChannelIdentity(&BalanceSnapshot{ChannelID: channel.ID, Balance: 1, SampledAt: when}, oldRevision); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale balance append error = %v", err)
	}
	if err := rates.AppendCostForChannelIdentity(&CostSnapshot{ChannelID: channel.ID, TodayCost: 1, TotalCost: 1, SampledAt: when}, oldRevision); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale cost append error = %v", err)
	}
	if _, err := rates.UpsertLoadedForChannelIdentity(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: when}, nil, oldRevision); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale rate upsert error = %v", err)
	}
	if err := rates.AppendChangeForChannelIdentity(&RateChangeLog{ChannelID: channel.ID, ModelName: "primary", NewRatio: 1, ChangedAt: when}, oldRevision); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale rate change error = %v", err)
	}

	var balanceCount, costCount, rateCount, changeCount int64
	if err := db.Model(&BalanceSnapshot{}).Where("channel_id = ?", channel.ID).Count(&balanceCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&CostSnapshot{}).Where("channel_id = ?", channel.ID).Count(&costCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&RateSnapshot{}).Where("channel_id = ?", channel.ID).Count(&rateCount).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&RateChangeLog{}).Where("channel_id = ?", channel.ID).Count(&changeCount).Error; err != nil {
		t.Fatal(err)
	}
	if balanceCount != 0 || costCount != 0 || rateCount != 0 || changeCount != 0 {
		t.Fatalf("stale identity wrote rows: balances=%d costs=%d rates=%d changes=%d", balanceCount, costCount, rateCount, changeCount)
	}
}
