package gateway

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestEnsureUpstreamKeyLeaseCarriesRefreshedAccountConcurrencySnapshot(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForType(t, "concurrency-lease-refresh", storage.ChannelTypeSub2API)
	const discoveredAPIURL = "https://api.concurrency-lease.example"
	originalRevision := channel.UpstreamAccountIdentityRevision
	var discoveredRevision uint64
	var discoverOnce sync.Once
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		beforeListGroups: func() {
			discoverOnce.Do(func() {
				updated, revision, err := channels.UpdateAPIURLRevisionIfUnchanged(
					channel.ID,
					originalRevision,
					channel.SiteURL,
					"",
					discoveredAPIURL,
				)
				if err != nil {
					t.Fatalf("persist discovered API origin: %v", err)
				}
				if !updated || revision <= originalRevision {
					t.Fatalf("discovered API origin update = %v revision %d, want advanced from %d", updated, revision, originalRevision)
				}
				discoveredRevision = revision
				limit := 3
				if _, err := channels.ObserveUpstreamAccountConcurrency(storage.UpstreamAccountConcurrencyObservation{
					ChannelID: channel.ID, ChannelIdentityRevision: revision,
					Provider: storage.ChannelTypeSub2API, APIOrigin: discoveredAPIURL, ExternalAccountID: "lease-account",
					Limit: &limit, Scope: storage.UpstreamConcurrencyScopeAccount,
					Source: "sub2api.auth.me.concurrency", ObservedAt: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
				}); err != nil {
					t.Fatalf("observe discovered account concurrency: %v", err)
				}
			})
		},
	}

	service := NewService(channels, rates, store, cipher, keys, nil)
	lease, err := service.EnsureUpstreamAPIKeyLeaseForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil {
		t.Fatalf("ensure upstream key lease: %v", err)
	}
	if lease.ChannelSnapshot == nil || lease.UpstreamAccountConcurrency == nil {
		t.Fatalf("slow lease omitted refreshed identity facts: %+v", lease)
	}
	snapshot := lease.UpstreamAccountConcurrency
	if lease.ChannelSnapshot.APIURL != discoveredAPIURL ||
		lease.ChannelSnapshot.UpstreamAccountIdentityRevision != discoveredRevision ||
		snapshot.ChannelIdentityRevision != discoveredRevision ||
		snapshot.AccountRecordID == 0 || snapshot.Limit == nil || *snapshot.Limit != 3 {
		t.Fatalf("slow lease identity facts do not match: channel=%+v concurrency=%+v", lease.ChannelSnapshot, snapshot)
	}
}
