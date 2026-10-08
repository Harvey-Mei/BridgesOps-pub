package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func TestDownstreamKeyCandidateTargetIsExplicitAndPolicyBound(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("candidate-target-policy-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "candidate target", Type: storage.ChannelTypeNewAPI, SiteURL: "https://candidate-target.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "pro", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	markGroupFamilies(t, store, channel.ID, "pro", "codex")
	candidate := &storage.GatewayCandidateGroup{Name: "pro target"}
	if err := store.SaveCandidateGroup(candidate, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "pro"}}); err != nil {
		t.Fatal(err)
	}
	keyGroup := &storage.GatewayKeyGroup{Name: "workstations"}
	if err := store.SaveKeyGroup(keyGroup); err != nil {
		t.Fatal(err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)
	if _, err := svc.CreateDownstreamKeyWithCandidateTarget("missing policy", storage.GatewayAppCodex, "codex", 0, keyGroup.ID, "key_group_policy"); err == nil || !strings.Contains(err.Error(), "no default route policy") {
		t.Fatalf("missing policy error = %v", err)
	}
	targetID := candidate.ID
	policy := &storage.GatewayKeyGroupRoutePolicy{KeyGroupID: keyGroup.ID, GatewayApp: "codex", ModelFamily: "codex", DefaultCandidateGroupID: &targetID, TargetMode: "candidate_group", Enabled: true}
	if err := store.SaveKeyGroupRoutePolicy(policy, 0); err != nil {
		t.Fatal(err)
	}
	created, err := svc.CreateDownstreamKeyWithCandidateTarget("policy key", storage.GatewayAppCodex, "codex", 0, keyGroup.ID, "key_group_policy")
	if err != nil {
		t.Fatal(err)
	}
	if created.Key.CandidateGroupID != candidate.ID || created.Key.CandidateTargetSource != "key_group_policy" || created.Key.RoutePolicyID == nil {
		t.Fatalf("created policy target = %+v", created.Key)
	}
	policy.TargetMode = "all_candidates"
	policy.DefaultCandidateGroupID = nil
	if err := store.SaveKeyGroupRoutePolicy(policy, policy.PolicyVersion); err != nil {
		t.Fatal(err)
	}
	renamed := "policy key renamed"
	updated, err := svc.UpdateDownstreamKeyWithCandidateTargetAndMapping(created.Key.ID, &renamed, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unrelated edit must preserve the one-time policy result: %v", err)
	}
	if updated.CandidateGroupID != candidate.ID || updated.CandidateTargetSource != "key_group_policy" || updated.Name != renamed {
		t.Fatalf("unrelated edit changed policy-initialized route = %+v", updated)
	}
	all := uint(0)
	allMode := "all_candidates"
	updated, err = svc.UpdateDownstreamKeyWithCandidateTargetAndMapping(created.Key.ID, nil, nil, nil, nil, &all, &allMode, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if updated.CandidateGroupID != 0 || updated.CandidateTargetMode != "all_candidates" || updated.CandidateTargetSource != "manual" || updated.RoutePolicyID != nil {
		t.Fatalf("explicit all-candidates target = %+v", updated)
	}
}

func TestSyncChannelModelFamiliesAddsNewGroupSkipsCacheAndSupportsForce(t *testing.T) {
	var modelHits atomic.Int32
	var modelVersion atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		modelHits.Add(1)
		if modelVersion.Load() > 0 {
			_, _ = w.Write([]byte(`{"data":[{"id":"gemini-2.5-pro"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"},{"id":"claude-sonnet-4"}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channel := &storage.Channel{Name: "new", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "new-group", Ratio: 0.5, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "new-group", Ratio: 0.5}},
		listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			page := query.Page
			if page <= 0 {
				page = 1
			}
			cached, findErr := store.FindUpstreamKeyAny(channel.ID, "new-group")
			if errors.Is(findErr, gorm.ErrRecordNotFound) {
				return &connector.APIKeyPage{Page: page, Pages: 1}, nil
			}
			if findErr != nil {
				return nil, findErr
			}
			remote := managedUpstreamKeyFixture(cached.UpstreamKeyID, cached.UpstreamKeyName, cached.GroupName)
			remote.GroupRatio = cached.GroupRatio
			return &connector.APIKeyPage{Items: []connector.APIKey{remote}, Page: page, Pages: 1}, nil
		},
		revealAPIKey: func(int64) (string, error) {
			return "upstream-generated-key", nil
		},
	}
	service := NewService(channels, rates, store, cipher, keys, nil)

	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, false); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	cached, err := store.FindUpstreamKey(channel.ID, "new-group")
	if err != nil {
		t.Fatalf("find synced key: %v", err)
	}
	if cached.ModelFamilies != "codex,claude" {
		t.Fatalf("model families = %q", cached.ModelFamilies)
	}
	if len(keys.created) != 1 || modelHits.Load() != 1 {
		t.Fatalf("created=%d modelHits=%d", len(keys.created), modelHits.Load())
	}
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, false); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if len(keys.created) != 1 || modelHits.Load() != 1 {
		t.Fatalf("fresh cache was re-synced: created=%d modelHits=%d", len(keys.created), modelHits.Load())
	}
	modelVersion.Store(1)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("forced sync: %v", err)
	}
	cached, err = store.FindUpstreamKey(channel.ID, "new-group")
	if err != nil || cached.ModelFamilies != "gemini" || modelHits.Load() != 2 {
		t.Fatalf("forced sync cache=%+v modelHits=%d err=%v", cached, modelHits.Load(), err)
	}
	items, err := service.SyncModelFamilies(context.Background(), channel.ID)
	if err != nil {
		t.Fatalf("manual sync: %v", err)
	}
	if len(items) != 1 || len(keys.created) != 1 || modelHits.Load() != 3 {
		t.Fatalf("manual sync did not use shared force path: items=%d created=%d modelHits=%d", len(items), len(keys.created), modelHits.Load())
	}
}

func TestManualModelSyncDoesNotRewriteChannelFailureState(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		wantError bool
	}{
		{name: "success", status: http.StatusOK},
		{name: "failure", status: http.StatusBadGateway, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/models" {
					http.NotFound(w, r)
					return
				}
				hits.Add(1)
				if tc.wantError {
					http.Error(w, "temporary failure", tc.status)
					return
				}
				_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
			}))
			defer server.Close()

			db, err := testdb.OpenWithError(t)
			if err != nil {
				t.Fatal(err)
			}
			channels := storage.NewChannels(db)
			rates := storage.NewRates(db)
			store := storage.NewGateway(db)
			cipher, err := appcrypto.NewCipher("manual-sync-test-secret")
			if err != nil {
				t.Fatal(err)
			}
			channel := &storage.Channel{Name: "manual-" + tc.name, Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
			if err := channels.Create(channel); err != nil {
				t.Fatal(err)
			}
			if err := channels.SetLastError(channel.ID, "previous channel failure"); err != nil {
				t.Fatal(err)
			}
			if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
				t.Fatal(err)
			}
			svc := NewService(channels, rates, store, cipher, &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}}}, nil)

			items, err := svc.SyncModelFamilies(context.Background(), channel.ID)
			if err != nil {
				t.Fatalf("manual sync: %v", err)
			}
			if hits.Load() != 1 || len(items) != 1 || items[0].Skipped || (items[0].Error != "") != tc.wantError {
				t.Fatalf("manual sync result: hits=%d items=%+v", hits.Load(), items)
			}
			stored, err := channels.FindByID(channel.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.LastError != "previous channel failure" {
				t.Fatalf("manual model sync rewrote channel error: %+v", stored)
			}
		})
	}
}

func TestManualModelSyncReportsSkippedChannel(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("monitor-disabled-key-maintenance")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "monitor-disabled", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: false}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&storage.Channel{}).Where("id = ?", channel.ID).Update("monitor_enabled", false).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}}}
	items, err := NewService(channels, rates, store, cipher, keys, nil).SyncModelFamilies(context.Background(), channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || !items[0].Skipped || !strings.Contains(items[0].Error, availabilityReasonMonitorDisabled) {
		t.Fatalf("skipped manual sync item = %+v", items)
	}
	if len(keys.created) != 1 {
		t.Fatalf("monitor-disabled channel managed-key creates = %d, want 1", len(keys.created))
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !stored.Enabled || stored.Provisioning || stored.APIKeyCipher == "" || stored.ModelsSyncedAt != nil {
		t.Fatalf("monitor-disabled managed key state = %+v, err=%v", stored, err)
	}
}

func TestModelSyncMaintainsManagedKeyAndProbesWhenBalanceBlocksRouting(t *testing.T) {
	tests := []struct {
		name  string
		block func(*testing.T, *gorm.DB, *storage.Channels, *storage.Channel)
	}{
		{
			name: "low balance",
			block: func(t *testing.T, db *gorm.DB, _ *storage.Channels, channel *storage.Channel) {
				t.Helper()
				if err := db.Model(&storage.Channel{}).Where("id = ?", channel.ID).Update("last_balance", 0.01).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "account balance unavailable",
			block: func(t *testing.T, _ *gorm.DB, channels *storage.Channels, channel *storage.Channel) {
				t.Helper()
				if err := channels.MarkUpstreamAccountBalanceInsufficient(channel.ID, channel.UpstreamAccountIdentityRevision, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var modelHits atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				modelHits.Add(1)
				_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
			}))
			defer server.Close()

			db, err := testdb.OpenWithError(t)
			if err != nil {
				t.Fatal(err)
			}
			channels := storage.NewChannels(db)
			rates := storage.NewRates(db)
			store := storage.NewGateway(db)
			cipher, err := appcrypto.NewCipher("balance-gated-managed-key-secret")
			if err != nil {
				t.Fatal(err)
			}
			channel := &storage.Channel{Name: "balance-gated-" + test.name, Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
			if err := channels.Create(channel); err != nil {
				t.Fatal(err)
			}
			test.block(t, db, channels, channel)
			if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
				t.Fatal(err)
			}
			keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}}}
			svc := NewService(channels, rates, store, cipher, keys, nil)

			items, err := svc.SyncModelFamilies(context.Background(), channel.ID)
			if err != nil {
				t.Fatalf("sync balance-gated channel: %v", err)
			}
			if len(items) != 1 || items[0].Skipped || items[0].Error != "" || len(items[0].ModelIDs) != 1 || items[0].ModelIDs[0] != "gpt-5" {
				t.Fatalf("balance-gated sync items = %+v", items)
			}
			if len(keys.created) != 1 {
				t.Fatalf("managed key creates = %d, want 1", len(keys.created))
			}
			if modelHits.Load() != 1 {
				t.Fatalf("balance-gated model probes = %d, want 1", modelHits.Load())
			}
			key, err := store.FindUpstreamKey(channel.ID, "promo")
			if err != nil || !key.Enabled || key.Provisioning || strings.TrimSpace(key.APIKeyCipher) == "" {
				t.Fatalf("maintained managed key = %+v, err=%v", key, err)
			}
			if err := store.RecordUpstreamKeyModelsError(channel.ID, "promo", time.Now(), "recent model probe failure"); err != nil {
				t.Fatalf("record recent model probe failure: %v", err)
			}
			if err := svc.SyncChannelModelFamilies(context.Background(), channel.ID, false); err != nil {
				t.Fatalf("recent model failure blocked balance-gated probe retry: %v", err)
			}
			if modelHits.Load() != 2 {
				t.Fatalf("balance-gated retry model probes = %d, want 2", modelHits.Load())
			}
		})
	}
}

func TestModelSyncStopsLaterProbesAfterAccountBalanceRejectionButMaintainsKeys(t *testing.T) {
	var modelHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		modelHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"insufficient_balance","message":"insufficient account balance"}}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("balance-probe-stop-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "balance-probe-stop", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	groups := []string{"a-balance", "b-history", "c-create"}
	for _, groupName := range groups {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}); err != nil {
			t.Fatalf("upsert %s rate: %v", groupName, err)
		}
	}
	historicalKey, err := cipher.Encrypt("historical-upstream-key")
	if err != nil {
		t.Fatal(err)
	}
	historicalSyncAt := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute)
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "b-history", GroupRatio: 1,
		UpstreamKeyID: 22, UpstreamKeyName: upstreamKeyNameForGroup("b-history"),
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        historicalKey, APIKeyPrefix: keyPrefix("historical-upstream-key"), Enabled: true,
		ModelFamilies: "codex", ModelIDs: "gpt-history", ModelsSyncedAt: &historicalSyncAt,
		ModelsError: "historical model warning",
	}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{
		{Name: "a-balance", Ratio: 1},
		{Name: "b-history", Ratio: 1},
		{Name: "c-create", Ratio: 1},
	}}
	service := NewService(channels, rates, store, cipher, keys, nil)

	for pass := int32(1); pass <= 2; pass++ {
		items, err := service.syncModelFamilies(context.Background(), channel.ID, false)
		if err != nil {
			t.Fatalf("sync pass %d: %v", pass, err)
		}
		if len(items) != len(groups) {
			t.Fatalf("sync pass %d items = %+v", pass, items)
		}
		for index, item := range items {
			if item.GroupName != groups[index] || !item.Skipped || item.Error != "" {
				t.Fatalf("sync pass %d item %d = %+v", pass, index, item)
			}
		}
		if modelHits.Load() != pass {
			t.Fatalf("sync pass %d model hits = %d, want %d", pass, modelHits.Load(), pass)
		}
	}
	if len(keys.created) != 2 || keys.created[0].Group != "a-balance" || keys.created[1].Group != "c-create" {
		t.Fatalf("managed key creates = %+v", keys.created)
	}
	created, err := store.FindUpstreamKey(channel.ID, "c-create")
	if err != nil || !created.Enabled || created.Provisioning || strings.TrimSpace(created.APIKeyCipher) == "" || created.ModelsSyncedAt != nil || created.ModelsError != "" {
		t.Fatalf("later maintained key = %+v, err=%v", created, err)
	}
	historical, err := store.FindUpstreamKey(channel.ID, "b-history")
	if err != nil {
		t.Fatal(err)
	}
	if historical.ModelFamilies != "codex" || historical.ModelIDs != "gpt-history" || historical.ModelsError != "historical model warning" || historical.ModelsSyncedAt == nil || !historical.ModelsSyncedAt.Equal(historicalSyncAt) {
		t.Fatalf("historical model evidence changed: %+v", historical)
	}
	storedChannel, err := channels.FindByID(channel.ID)
	if err != nil || !storedChannel.HasCurrentUpstreamAccountBalanceInsufficient() {
		t.Fatalf("account balance fact = %+v, err=%v", storedChannel, err)
	}
}

func TestModelSyncContinuesAfterChannelControlPlaneFailure(t *testing.T) {
	models := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer models.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("channel-isolated-model-sync-secret")
	if err != nil {
		t.Fatal(err)
	}
	created := make([]*storage.Channel, 0, 2)
	for _, name := range []string{"failing-control-plane", "healthy-control-plane"} {
		channel := &storage.Channel{Name: name, Type: storage.ChannelTypeNewAPI, SiteURL: models.URL, MonitorEnabled: true}
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
			t.Fatal(err)
		}
		created = append(created, channel)
	}
	remoteErr := errors.New("remote key inventory unavailable")
	baseKeys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}}}
	keys := &channelFailingKeyManager{fakeKeyManager: baseKeys, failingChannelID: created[0].ID, err: remoteErr}
	svc := NewService(channels, rates, store, cipher, keys, nil)

	items, err := svc.SyncModelFamilies(context.Background(), 0)
	if err != nil {
		t.Fatalf("sync all channels: %v", err)
	}
	var failed, healthy bool
	for _, item := range items {
		switch item.ChannelID {
		case created[0].ID:
			failed = strings.Contains(item.Error, remoteErr.Error())
		case created[1].ID:
			healthy = item.Error == "" && !item.Skipped && len(item.ModelIDs) == 1 && item.ModelIDs[0] == "gpt-5"
		}
	}
	if !failed || !healthy {
		t.Fatalf("channel-isolated sync items = %+v", items)
	}
	if len(baseKeys.created) != 1 {
		t.Fatalf("healthy channel managed-key creates = %d, want 1", len(baseKeys.created))
	}
}

func TestForcedModelSyncContinuesActiveGroupEnsureAfterManagedReconcileGroupFailure(t *testing.T) {
	var brokenProbeHits atomic.Int32
	brokenPlain := "broken-managed-key"
	models := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer "+brokenPlain {
			brokenProbeHits.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer models.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("group-isolated-managed-reconcile-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "group-isolated-managed-reconcile", Type: storage.ChannelTypeNewAPI, SiteURL: models.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, groupName := range []string{"broken", "healthy"} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
	}
	brokenName := upstreamProvisioningKeyPrefix + "0123456789abcdef"
	brokenCipher, err := cipher.Encrypt(brokenPlain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "broken", GroupRatio: 1,
		UpstreamKeyID: 7, UpstreamKeyName: brokenName,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        brokenCipher, APIKeyPrefix: "stale-prefix", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	brokenRemote := managedUpstreamKeyFixture(7, brokenName, "broken")
	deleteErr := errors.New("managed delete unavailable for broken group")
	var keys *fakeKeyManager
	keys = &fakeKeyManager{
		groups:       []connector.APIKeyGroup{{Name: "broken", Ratio: 1}, {Name: "healthy", Ratio: 1}},
		revealedKeys: map[int64]string{1: "upstream-generated-key"},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			items := []connector.APIKey{brokenRemote}
			for index, created := range keys.created {
				items = append(items, managedUpstreamKeyFixture(int64(index+1), created.Name, created.Group))
			}
			return &connector.APIKeyPage{Items: items, Page: 1, Pages: 1, Total: int64(len(items))}, nil
		},
		deleteAPIKey: func(id int64) error {
			if id == brokenRemote.ID {
				return deleteErr
			}
			return nil
		},
	}
	service := NewService(channels, rates, store, cipher, keys, nil)

	items, err := service.SyncModelFamilies(context.Background(), channel.ID)
	if err != nil {
		t.Fatalf("forced model sync: %v", err)
	}
	byGroup := make(map[string]ModelFamilySyncItem, len(items))
	for _, item := range items {
		byGroup[item.GroupName] = item
	}
	if item := byGroup["broken"]; !strings.Contains(item.Error, deleteErr.Error()) {
		t.Fatalf("broken reconcile item = %+v", item)
	}
	if item := byGroup["healthy"]; item.Error != "" || item.Skipped || len(item.ModelIDs) != 1 || item.ModelIDs[0] != "gpt-5" {
		t.Fatalf("healthy model-sync item = %+v", item)
	}
	if len(keys.created) != 1 || keys.created[0].Group != "healthy" {
		t.Fatalf("managed key creates = %+v, want only healthy", keys.created)
	}
	storedBroken, err := store.FindUpstreamKeyAny(channel.ID, "broken")
	if err != nil || !strings.Contains(storedBroken.LastError, deleteErr.Error()) {
		t.Fatalf("persisted broken reconcile error = %+v, err=%v", storedBroken, err)
	}
	if brokenProbeHits.Load() != 0 {
		t.Fatalf("broken group was probed %d times", brokenProbeHits.Load())
	}
	partialErr := service.SyncChannelModelFamilies(context.Background(), channel.ID, true)
	var partial *ModelFamilySyncPartialError
	if !errors.As(partialErr, &partial) || !strings.Contains(partialErr.Error(), "pending revalidation or manual review") {
		t.Fatalf("pending group removal was not returned as a partial failure: %v", partialErr)
	}
	if len(keys.created) != 1 || brokenProbeHits.Load() != 0 {
		t.Fatalf("pending removal bypassed its group fence: creates=%+v broken_probes=%d", keys.created, brokenProbeHits.Load())
	}
}

func TestForcedModelSyncDoesNotEnsureAnyGroupWithoutCompleteManagedInventory(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("managed-inventory-fail-closed-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "managed-inventory-fail-closed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, groupName := range []string{"first", "second"} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
	}
	inventoryErr := errors.New("remote managed inventory is incomplete")
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "first", Ratio: 1}, {Name: "second", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return nil, inventoryErr
		},
	}
	service := NewService(channels, rates, store, cipher, keys, nil)

	items, err := service.SyncModelFamilies(context.Background(), channel.ID)
	if err != nil {
		t.Fatalf("forced model sync result: %v", err)
	}
	if len(items) != 1 || items[0].GroupName != "" || !strings.Contains(items[0].Error, inventoryErr.Error()) {
		t.Fatalf("inventory failure items = %+v", items)
	}
	if keys.listGroupCalls != 0 || len(keys.created) != 0 {
		t.Fatalf("untrusted inventory reached ensure: group_lists=%d creates=%+v", keys.listGroupCalls, keys.created)
	}
}

func TestModelProbeClientBoundsTLSHandshakeTimeout(t *testing.T) {
	timeout := 17 * time.Second
	client, err := modelProbeClient("", timeout)
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("model probe transport = %T", client.Transport)
	}
	if client.Timeout != timeout || transport.TLSHandshakeTimeout != timeout {
		t.Fatalf("model probe timeouts = client:%s tls:%s, want %s", client.Timeout, transport.TLSHandshakeTimeout, timeout)
	}
	if transport.Proxy != nil {
		t.Fatal("unbound model probe must not inherit an environment proxy")
	}
	svc := NewService(nil, nil, nil, nil, nil, nil)
	svc.UpdateRoutingConfig(RoutingConfig{FailurePolicy: RoutingFailurePolicy{ResponseTimeout: timeout}})
	if got := svc.modelProbeTimeout(); got != timeout {
		t.Fatalf("configured model probe timeout = %s, want %s", got, timeout)
	}
}

func TestProbeModelFamiliesBoundsResponseSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", gatewayProbeResponseMaxBytes+1)))
	}))
	defer server.Close()

	service := NewService(nil, nil, nil, nil, nil, nil)
	candidate := accountConcurrencyCandidate(1, 1, "gpt")
	_, _, err := service.probeModelFamilies(context.Background(), gatewayProbeTarget{SiteURL: server.URL}, "test-key", candidate)
	if err == nil || !strings.Contains(err.Error(), "exceeded 1 MiB") {
		t.Fatalf("probe error = %v", err)
	}
}

func TestProbeModelFamiliesReturnsMalformedRedirectWithoutFollowing(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "%gh")
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	service := NewService(nil, nil, nil, nil, nil, nil)
	candidate := accountConcurrencyCandidate(3, 1, "gpt")
	_, _, err := service.probeModelFamilies(context.Background(), gatewayProbeTarget{SiteURL: server.URL}, "test-key", candidate)
	if err == nil || !strings.Contains(err.Error(), "models probe") || strings.Contains(err.Error(), "failed to parse Location") {
		t.Fatalf("model probe malformed redirect error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("model probe calls=%d, want 1", calls.Load())
	}
}

func TestSendLatencyProbeReturnsMalformedRedirectWithoutFollowing(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "%gh")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	response, err := sendLatencyProbeRequest(
		context.Background(),
		gatewayProbeTarget{SiteURL: server.URL},
		"test-key",
		storage.GatewayAppCodex,
		"gpt-test",
		time.Second,
	)
	if err != nil {
		t.Fatalf("latency probe malformed redirect: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || response.Header.Get("Location") != "%gh" {
		t.Fatalf("latency probe response status/location=%d/%q", response.StatusCode, response.Header.Get("Location"))
	}
	if calls.Load() != 1 {
		t.Fatalf("latency probe calls=%d, want 1", calls.Load())
	}
}

func TestProbeModelFamiliesAccountConcurrencyRejectionSkipsNetwork(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()

	service := NewService(nil, nil, nil, nil, nil, nil)
	candidate := accountConcurrencyCandidate(2, 1, "gpt")
	active, decision := service.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencySmartData)
	if active == nil || !decision.Admitted {
		t.Fatalf("active data-plane decision = %#v", decision)
	}
	defer active.Release()
	_, _, err := service.probeModelFamilies(context.Background(), gatewayProbeTarget{SiteURL: server.URL}, "test-key", candidate)
	if !errors.Is(err, errAccountConcurrencyProbeDeferred) {
		t.Fatalf("model probe error = %v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("network requests = %d, want 0", requests.Load())
	}
}

func TestModelSyncChannelLockHonorsContext(t *testing.T) {
	service := NewService(nil, nil, nil, nil, nil, nil)
	unlock, err := service.lockModelSyncChannel(context.Background(), 7)
	if err != nil {
		t.Fatalf("acquire initial lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	blockedUnlock, err := service.lockModelSyncChannel(ctx, 7)
	if blockedUnlock != nil {
		blockedUnlock()
		t.Fatal("timed-out lock returned an unlock function")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked lock error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("blocked lock returned after %s, want prompt context cancellation", elapsed)
	}

	unlock()
	nextUnlock, err := service.lockModelSyncChannel(context.Background(), 7)
	if err != nil {
		t.Fatalf("reacquire released lock: %v", err)
	}
	nextUnlock()
}

func TestCandidateGroupMembershipAllowsUnsyncedFactsWhileMappingStaysStrict(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "mapping", Type: storage.ChannelTypeNewAPI, SiteURL: "https://mapping.example", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "main", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "main", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", ModelFamilies: "grok", ModelIDs: "grok-3", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(channels, rates, store, nil, nil, nil)
	members := []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "main"}}
	group, err := svc.SaveCandidateGroupWithMappingFamily(0, "radar", "codex", members)
	if err != nil {
		t.Fatalf("unsynced candidate membership was rejected: %v", err)
	}
	mapped := &storage.GatewayDownstreamKey{Name: "mapped", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID, ModelMappingStrategy: "off", KeyHash: "mapped-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true}
	if err := store.CreateDownstreamKey(mapped); err != nil {
		t.Fatal(err)
	}
	strategy, model, effort := "manual", "gpt-5", "high"
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(mapped.ID, nil, nil, nil, nil, nil, nil, &strategy, &model, &effort, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "synchronized codex radar capability") {
		t.Fatalf("model mapping accepted unsynchronized candidate facts: %v", err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "main", "codex,claude", "gpt-5,claude-sonnet", time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(mapped.ID, nil, nil, nil, nil, nil, nil, &strategy, &model, &effort, nil, nil, nil); err != nil {
		t.Fatalf("enable mapping after model sync: %v", err)
	}
	group, err = svc.SaveCandidateGroupWithMappingFamily(group.ID, group.Name, "codex", members)
	if err != nil {
		t.Fatalf("save synchronized Codex radar group: %v", err)
	}
	if _, err := svc.SaveCandidateGroupWithMappingFamily(group.ID, group.Name, "claude", members); err == nil {
		t.Fatal("candidate group bound to a Codex mapped key changed to Claude")
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "grok", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "grok", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", ModelFamilies: "grok", ModelIDs: "grok-3", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "grok", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "grok", "grok", "grok-3", time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 1 {
		t.Fatalf("changes=%+v err=%v", changes, err)
	}
	if err := svc.ResolveUpstreamGroupChange(changes[0].ID, []uint{group.ID}, false); err != nil {
		t.Fatalf("mixed-capability candidate group should remain usable for mapped routing: %v", err)
	}
}

func TestDownstreamKeyMappingPatchValidatesCandidateModelFactsOnlyWhenRelevant(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	type upstreamFixture struct {
		name     string
		modelIDs string
	}
	fixtures := []upstreamFixture{
		{name: "supported", modelIDs: "gpt-5,GPT-5.4"},
		{name: "unsupported", modelIDs: "gpt-4.1"},
		{name: "unknown", modelIDs: ""},
		{name: "alternate", modelIDs: "gpt-5.4"},
	}
	groups := make(map[string]*storage.GatewayCandidateGroup, len(fixtures))
	channelsByName := make(map[string]*storage.Channel, len(fixtures))
	for _, fixture := range fixtures {
		channel := &storage.Channel{Name: fixture.name, Type: storage.ChannelTypeNewAPI, SiteURL: "https://" + fixture.name + ".example", PasswordCipher: "cipher", MonitorEnabled: true, LatencyProbeEnabled: true}
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: fixture.name, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: fixture.name, APIKeyCipher: "cipher", APIKeyPrefix: "sk",
			OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
			ModelFamilies:       "codex", ModelIDs: fixture.modelIDs, ModelsSyncedAt: ptrTime(time.Now()),
			Enabled: true, LatencyProbeEnabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, fixture.name, "codex", fixture.modelIDs, time.Now(), ""); err != nil {
			t.Fatal(err)
		}
		group, err := svc.SaveCandidateGroupWithMappingFamily(0, fixture.name, "codex", []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: fixture.name}})
		if err != nil {
			t.Fatalf("save %s candidate group: %v", fixture.name, err)
		}
		groups[fixture.name] = group
		channelsByName[fixture.name] = channel
	}

	key := &storage.GatewayDownstreamKey{
		Name: "mapping-client", App: storage.GatewayAppCodex, ModelFamily: "codex",
		CandidateGroupID: groups["supported"].ID, ModelMappingStrategy: "off",
		KeyHash: "mapping-validation-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatal(err)
	}
	strategy, model, effort := "manual", "gpt-5.4", "high"
	updated, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, nil, nil, nil, nil, nil, &strategy, &model, &effort, nil, nil, nil)
	if err != nil {
		t.Fatalf("enable case-insensitive manual mapping: %v", err)
	}
	if updated.ModelMappingModel != model {
		t.Fatalf("mapped model = %q, want %q", updated.ModelMappingModel, model)
	}
	if err := store.SaveCandidateGroup(groups["supported"], []storage.GatewayCandidateGroupMember{
		{ChannelID: channelsByName["supported"].ID, GroupName: "supported"},
		{ChannelID: channelsByName["alternate"].ID, GroupName: "alternate"},
	}); err != nil {
		t.Fatal(err)
	}

	missingModel := "gpt-missing"
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, nil, nil, nil, nil, nil, nil, &missingModel, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("unsupported mapping patch error = %v", err)
	}
	unsupportedGroupID := groups["unsupported"].ID
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, nil, nil, nil, &unsupportedGroupID, nil, nil, nil, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("unsupported candidate group switch error = %v", err)
	}
	unknownGroupID := groups["unknown"].ID
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, nil, nil, nil, &unknownGroupID, nil, nil, nil, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "no synchronized model IDs") {
		t.Fatalf("unknown candidate group switch error = %v", err)
	}

	if err := db.Model(&storage.GatewayDownstreamKey{}).Where("id = ?", key.ID).Update("model_mapping_model", "historically-missing").Error; err != nil {
		t.Fatal(err)
	}
	renamed := "renamed-client"
	sameModel := "historically-missing"
	updated, err = svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, &renamed, nil, nil, nil, nil, nil, &strategy, &sameModel, &effort, nil, nil, nil)
	if err != nil {
		t.Fatalf("unrelated rename with unchanged mapping fields: %v", err)
	}
	if updated.Name != renamed {
		t.Fatalf("renamed key = %+v", updated)
	}
	disabled := false
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, &disabled, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("enabled-state patch revalidated historical mapping: %v", err)
	}
	keyGroup := &storage.GatewayKeyGroup{Name: "mapping-key-group"}
	if err := store.SaveKeyGroup(keyGroup); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, nil, nil, nil, nil, &keyGroup.ID, nil, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("key-group patch revalidated historical mapping: %v", err)
	}
	smartRouting := false
	manualChannelID := channelsByName["supported"].ID
	manualGroupName := "supported"
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, nil, nil, nil, nil, nil, nil, nil, nil, &smartRouting, &manualChannelID, &manualGroupName); err == nil {
		t.Fatal("fixed-route patch bypassed the persisted manual model gate")
	} else if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonModelNotListed {
		t.Fatalf("fixed-route patch gate reason = %q ok=%t err=%v", reason, ok, err)
	}
	manualChannelID = channelsByName["alternate"].ID
	manualGroupName = "alternate"
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &manualChannelID, &manualGroupName); err != nil {
		t.Fatalf("inactive manual target patch revalidated historical mapping: %v", err)
	}
	xhigh := "xhigh"
	if _, err := svc.UpdateDownstreamKeyWithFamilyGroupsAndMapping(key.ID, nil, nil, nil, nil, nil, nil, nil, nil, &xhigh, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "does not support") {
		t.Fatalf("changed mapping effort did not revalidate historical model: %v", err)
	}
}

func TestSyncModelFamiliesRecordsAddedAfterInitialProbeFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		if calls.Add(1) == 1 {
			http.Error(w, "temporary failure", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("retry-sync-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "retry", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(channels, rates, store, cipher, &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}}}, nil)
	if err := svc.SyncChannelModelFamilies(context.Background(), channel.ID, true); err == nil {
		t.Fatal("initial failed model probe reported success")
	}
	if err := svc.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("retry sync: %v", err)
	}
	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 1 || changes[0].Kind != "added" || changes[0].GroupName != "promo" {
		t.Fatalf("changes after retry = %+v err=%v", changes, err)
	}
	if err := svc.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("third sync: %v", err)
	}
	changes, err = store.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 1 {
		t.Fatalf("duplicate added change after successful retry: %+v err=%v", changes, err)
	}
}

func TestSyncChannelModelFamiliesRefreshesLegacyCacheWithoutModelIDs(t *testing.T) {
	var modelHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		modelHits.Add(1)
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"},{"id":"gpt-5-mini"}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channel := &storage.Channel{Name: "new", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "new-group", Ratio: 0.5, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	encryptedKey, err := cipher.Encrypt("sk-test")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID:      channel.ID,
		APIKeyCipher:   encryptedKey,
		APIKeyPrefix:   "sk-test",
		GroupName:      "new-group",
		GroupRatio:     0.5,
		ModelFamilies:  "codex",
		ModelsSyncedAt: ptrTime(time.Now().Add(-time.Hour)),
		Enabled:        true,
	}); err != nil {
		t.Fatalf("upsert legacy upstream key: %v", err)
	}

	service := NewService(channels, rates, store, cipher, &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "new-group", Ratio: 0.5}}}, nil)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, false); err != nil {
		t.Fatalf("sync legacy cache: %v", err)
	}
	cached, err := store.FindUpstreamKey(channel.ID, "new-group")
	if err != nil {
		t.Fatalf("find synced key: %v", err)
	}
	if cached.ModelIDs != "gpt-5,gpt-5-mini" {
		t.Fatalf("model ids = %q, want gpt-5,gpt-5-mini", cached.ModelIDs)
	}
	if modelHits.Load() != 1 {
		t.Fatalf("model hits = %d, want 1", modelHits.Load())
	}
}

func TestSyncChannelModelFamiliesDefersRecentErrorRetry(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("model-sync-error-backoff-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	encryptedKey, err := cipher.Encrypt("cached-upstream-key")
	if err != nil {
		t.Fatalf("encrypt cached key: %v", err)
	}
	channel := &storage.Channel{Name: "backoff", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "limited", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	now := time.Now()
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "limited", Enabled: true, APIKeyCipher: encryptedKey,
		ModelsSyncedAt: &now, ModelsError: "upstream rate limited",
	}); err != nil {
		t.Fatalf("upsert failed model cache: %v", err)
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "limited", Ratio: 1}}}
	service := NewService(channels, rates, store, cipher, keys, nil)

	err = service.SyncChannelModelFamilies(context.Background(), channel.ID, false)
	if err == nil || !strings.Contains(err.Error(), "upstream rate limited") {
		t.Fatalf("cached model error = %v", err)
	}
	if keys.listGroupCalls != 0 || len(keys.created) != 0 {
		t.Fatalf("recent model error retried upstream: groupCalls=%d created=%d", keys.listGroupCalls, len(keys.created))
	}
}

func TestSyncChannelModelFamiliesContinuesAfterGroupProbeFailure(t *testing.T) {
	var modelHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if modelHits.Add(1) == 1 {
			http.Error(w, "temporary failure", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channel := &storage.Channel{Name: "new", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, group := range []string{"alpha", "beta"} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: group, Ratio: 1, LastSeenAt: time.Now()}); err != nil {
			t.Fatalf("upsert %s: %v", group, err)
		}
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "alpha", Ratio: 1}, {Name: "beta", Ratio: 1}}}
	service := NewService(channels, rates, store, cipher, keys, nil)

	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, false); err == nil {
		t.Fatal("sync succeeded despite one failed group")
	}
	if modelHits.Load() != 2 {
		t.Fatalf("model hits = %d, want 2", modelHits.Load())
	}
	alpha, err := store.FindUpstreamKeyAny(channel.ID, "alpha")
	if err != nil || alpha.ModelsError == "" {
		t.Fatalf("alpha error not persisted: key=%+v err=%v", alpha, err)
	}
	beta, err := store.FindUpstreamKey(channel.ID, "beta")
	if err != nil {
		t.Fatalf("find beta: %v", err)
	}
	if beta.ModelFamilies != "codex" || beta.ModelsError != "" {
		t.Fatalf("beta sync = %+v", beta)
	}
}

func TestSyncChannelModelFamiliesContinuesActiveProbeAfterStaleRemovalFailure(t *testing.T) {
	var modelHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		modelHits.Add(1)
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("stale-removal-isolation-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channel := &storage.Channel{Name: "stale-isolation", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "active", Ratio: 1, LastSeenAt: time.Now().UTC()}); err != nil {
		t.Fatalf("upsert active rate: %v", err)
	}
	activeCipher, err := cipher.Encrypt("active-local-key")
	if err != nil {
		t.Fatalf("encrypt active key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "active", GroupRatio: 1, UpstreamKeyID: 41,
		UpstreamKeyName: upstreamKeyNameForGroup("active"), OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: activeCipher, APIKeyPrefix: keyPrefix("active-local-key"), Enabled: true,
	}); err != nil {
		t.Fatalf("seed active key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "stale", GroupRatio: 1, UpstreamKeyID: 42,
		UpstreamKeyName: "BOG - stale", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		Enabled: true,
	}); err != nil {
		t.Fatalf("seed stale key: %v", err)
	}
	var listCalls atomic.Int32
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "active", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			if listCalls.Add(1) == 1 {
				return nil, connector.HTTPStatusError(http.StatusServiceUnavailable, nil)
			}
			return &connector.APIKeyPage{}, nil
		},
	}
	service := NewService(channels, rates, store, cipher, keys, nil)
	err = service.SyncChannelModelFamilies(context.Background(), channel.ID, true)
	if err == nil || !strings.Contains(err.Error(), "delete stale routing group") {
		t.Fatalf("sync error = %v, want isolated stale-removal error", err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("active probe attempted managed-key create after stale failure = %+v", keys.created)
	}
	active, findErr := store.FindUpstreamKey(channel.ID, "active")
	if findErr != nil || active.ModelFamilies != "codex" || active.ModelsError != "" {
		t.Fatalf("active group sync = %+v, err=%v", active, findErr)
	}
	if modelHits.Load() != 1 {
		t.Fatalf("active model probes = %d, want one", modelHits.Load())
	}
	maintenance, stateErr := store.ManagedKeyMaintenanceState(channel.ID, channel.UpstreamAccountIdentityRevision)
	if stateErr != nil || maintenance == nil || maintenance.FailureCount != 1 || maintenance.LastFailureStatus != http.StatusServiceUnavailable {
		t.Fatalf("stale-removal maintenance backoff = %+v, err=%v listCalls=%d keyListCalls=%d", maintenance, stateErr, listCalls.Load(), keys.listGroupCalls)
	}
	if listCalls.Load() != 1 {
		t.Fatalf("remote inventory calls = %d, want one before maintenance backoff", listCalls.Load())
	}
}

func TestSyncChannelModelFamiliesSerializesConcurrentRuns(t *testing.T) {
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	var once sync.Once
	var modelHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modelHits.Add(1)
		once.Do(func() { close(probeStarted) })
		<-releaseProbe
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, _ := appcrypto.NewCipher("test-secret")
	channel := &storage.Channel{Name: "new", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "group", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "group", Ratio: 1}}}
	service := NewService(channels, rates, store, cipher, keys, nil)

	errs := make(chan error, 2)
	go func() { errs <- service.SyncChannelModelFamilies(context.Background(), channel.ID, false) }()
	<-probeStarted
	dbWrite := make(chan error, 1)
	go func() {
		dbWrite <- store.AppendRequestLog(&storage.GatewayRequestLog{ChannelID: channel.ID, GroupName: "group", Method: "GET", Path: "/transaction-check", StatusCode: 200})
	}()
	select {
	case err := <-dbWrite:
		if err != nil {
			t.Fatalf("database write during network probe: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("network probe appears to hold the database transaction")
	}
	go func() { errs <- service.SyncChannelModelFamilies(context.Background(), channel.ID, false) }()
	close(releaseProbe)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("sync: %v", err)
		}
	}
	if len(keys.created) != 1 || modelHits.Load() != 1 {
		t.Fatalf("created=%d modelHits=%d, want 1/1", len(keys.created), modelHits.Load())
	}
}

func TestSyncChannelModelFamiliesKeepsLocalSnapshotGroupName(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, _ := appcrypto.NewCipher("test-secret")
	channel := &storage.Channel{Name: "new", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "VIP", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "vip", Ratio: 1}}}
	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, false); err != nil {
		t.Fatalf("sync: %v", err)
	}
	key, err := store.FindUpstreamKey(channel.ID, "VIP")
	if err != nil {
		t.Fatalf("find local group key: %v", err)
	}
	if key.ModelFamilies != "codex" || len(keys.created) != 1 || keys.created[0].Group != "vip" {
		t.Fatalf("key=%+v created=%+v", key, keys.created)
	}
	alias, err := store.FindUpstreamKeyAny(channel.ID, "vip")
	if err != nil || alias.ID != key.ID || alias.GroupName != "VIP" {
		t.Fatalf("canonical alias lookup = %+v err=%v", alias, err)
	}
}

func TestSyncChannelModelFamiliesReusesCaseVariantRoutingGroup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("case-variant-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "case-variant", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "vip", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	plain := "sk-existing-vip"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "vip", GroupRatio: 1, UpstreamKeyID: 10,
		UpstreamKeyName: upstreamKeyNameForGroup("vip"), OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: encrypted,
		APIKeyPrefix: keyPrefix(plain), Enabled: true, ModelFamilies: "codex", ModelIDs: "gpt-4.1",
	}); err != nil {
		t.Fatal(err)
	}
	candidateGroup := &storage.GatewayCandidateGroup{Name: "case-preserved"}
	if err := store.SaveCandidateGroup(candidateGroup, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "vip"}}); err != nil {
		t.Fatal(err)
	}
	if err := rates.DeleteSnapshot(channel.ID, "vip"); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "VIP", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(10, upstreamKeyNameForGroup("vip"), "vip")
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "VIP", Ratio: 1}}}
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		return &connector.APIKeyPage{Items: []connector.APIKey{remote}, Page: 1, Pages: 1, Total: 1}, nil
	}
	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("force sync: %v", err)
	}
	if len(keys.deleted) != 0 || len(keys.created) != 0 {
		t.Fatalf("case-variant group mutated remote keys: deleted=%v created=%+v", keys.deleted, keys.created)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "vip")
	if err != nil || !stored.Enabled || stored.UpstreamKeyID != 10 || stored.ModelIDs != "gpt-5" {
		t.Fatalf("case-variant cached key = %+v err=%v", stored, err)
	}
	alias, err := store.FindUpstreamKeyAny(channel.ID, "VIP")
	if err != nil || alias.ID != stored.ID || alias.GroupName != "vip" {
		t.Fatalf("case-variant alias lookup = %+v err=%v", alias, err)
	}
	var canonicalRows int64
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "vip").
		Count(&canonicalRows).Error; err != nil || canonicalRows != 1 {
		t.Fatalf("canonical key rows = %d err=%v", canonicalRows, err)
	}
	preserved, err := store.FindCandidateGroup(candidateGroup.ID)
	if err != nil || len(preserved.Members) != 1 || preserved.Members[0].GroupName != "vip" {
		t.Fatalf("case-variant candidate membership = %+v err=%v", preserved, err)
	}
}

func TestSyncChannelModelFamiliesDeduplicatesConcurrentCaseVariantRates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("duplicate-case-rates-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "duplicate-case-rates", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	oldSeenAt := time.Now().Add(-time.Minute)
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "vip", Ratio: 2, LastSeenAt: oldSeenAt}); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "VIP", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "VIP", Ratio: 1}}}
	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("force sync: %v", err)
	}
	if len(keys.created) != 1 || keys.created[0].Group != "VIP" {
		t.Fatalf("created keys for duplicate case rates = %+v", keys.created)
	}
	stored, err := store.ListUpstreamKeys([]uint{channel.ID})
	if err != nil || len(stored) != 1 || stored[0].GroupName != "VIP" {
		t.Fatalf("stored keys for duplicate case rates = %+v err=%v", stored, err)
	}
}

func TestSyncChannelModelFamiliesTracksAddedAndRemovedGroups(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, _ := appcrypto.NewCipher("test-secret")
	channel := &storage.Channel{Name: "upstream", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 0.5, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups:       []connector.APIKeyGroup{{Name: "promo", Ratio: 0.5}},
		revealedKeys: map[int64]string{},
	}
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		if len(keys.created) == 0 {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		}
		req := keys.created[0]
		item := managedUpstreamKeyFixture(1, req.Name, req.Group)
		item.GroupID = req.GroupID
		return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{item}}, nil
	}
	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("initial sync: %v", err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("created upstream keys = %+v", keys.created)
	}
	keys.revealedKeys[1] = "upstream-generated-key"
	createdRequest := keys.created[0]
	remote := map[int64]connector.APIKey{}
	createdRemote := managedUpstreamKeyFixture(1, createdRequest.Name, createdRequest.Group)
	createdRemote.GroupID = createdRequest.GroupID
	createdRemote.GroupRatio = 0.5
	remote[createdRemote.ID] = createdRemote
	keys.listAPIKeys = func(_ connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		items := make([]connector.APIKey, 0, len(remote))
		for _, item := range remote {
			items = append(items, item)
		}
		return &connector.APIKeyPage{
			Page: 1, Pages: 1, Total: int64(len(items)), Items: items,
		}, nil
	}
	keys.deleteAPIKey = func(id int64) error {
		delete(remote, id)
		return nil
	}
	changes, err := service.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 1 || changes[0].Kind != "added" || changes[0].GroupName != "promo" {
		t.Fatalf("added changes=%+v err=%v", changes, err)
	}
	candidateGroup := &storage.GatewayCandidateGroup{Name: "discounts"}
	if err := store.SaveCandidateGroup(candidateGroup, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "promo"}}); err != nil {
		t.Fatal(err)
	}
	if err := service.ResolveUpstreamGroupChange(changes[0].ID, []uint{candidateGroup.ID}, false); err != nil {
		t.Fatalf("assign added group: %v", err)
	}
	assigned, err := store.FindCandidateGroup(candidateGroup.ID)
	if err != nil || len(assigned.Members) != 1 {
		t.Fatalf("assigned group=%+v err=%v", assigned, err)
	}
	if err := rates.DeleteSnapshot(channel.ID, "promo"); err != nil {
		t.Fatal(err)
	}
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("removal sync: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != 1 {
		t.Fatalf("deleted upstream keys = %+v", keys.deleted)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("removed local key error = %v", err)
	}
	changes, err = service.ListUpstreamGroupChanges(true)
	if err != nil || len(changes) != 0 {
		t.Fatalf("removed changes should be auto-acknowledged: changes=%+v err=%v", changes, err)
	}
	history, err := service.ListUpstreamGroupChanges(false)
	// Rate removal and remote managed-key deletion are distinct append-only
	// facts: the former retires routing presence immediately, while the latter
	// carries the ownership claim proving which remote credential was deleted.
	if err != nil || len(history) != 3 ||
		history[0].Kind != "removed" || history[0].Status != "acknowledged" || history[0].ResolvedAt == nil || history[0].UpstreamKeyID != 1 || history[0].UpstreamKeyName != keys.created[0].Name || history[0].RemovalClaimKey == nil ||
		history[1].Kind != "removed" || history[1].Status != "acknowledged" || history[1].ResolvedAt == nil || history[1].UpstreamKeyID != 0 || history[1].RemovalClaimKey != nil ||
		history[2].Kind != "added" {
		t.Fatalf("change history=%+v err=%v", history, err)
	}
}

func TestSyncChannelModelFamiliesQuarantinesOrphanedKeysWithoutLocalOwnership(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("orphaned-managed-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "orphan-cleanup", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "active", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	plain := "sk-active-managed"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "active", GroupRatio: 1, UpstreamKeyID: 10,
		UpstreamKeyName: upstreamProvisioningKeyPrefix + "aaaaaaaaaaaaaaaa", APIKeyCipher: encrypted,
		APIKeyPrefix: keyPrefix(plain), Enabled: true, ModelFamilies: "codex", ModelIDs: "gpt-5",
	}); err != nil {
		t.Fatal(err)
	}

	active := managedUpstreamKeyFixture(10, upstreamProvisioningKeyPrefix+"aaaaaaaaaaaaaaaa", "active")
	orphaned := managedUpstreamKeyFixture(11, upstreamProvisioningKeyPrefix+"bbbbbbbbbbbbbbbb", "removed")
	duplicateOrphaned := managedUpstreamKeyFixture(18, orphaned.Name, "removed")
	manual := managedUpstreamKeyFixture(12, "manual-key", "manual")
	manual.Name = "manual-key"
	manual.CrossGroupRetry = false
	caseVariantActive := managedUpstreamKeyFixture(13, upstreamProvisioningKeyPrefix+"cccccccccccccccc", "active")
	secondOrphaned := managedUpstreamKeyFixture(14, upstreamProvisioningKeyPrefix+"dddddddddddddddd", "removed-legacy")
	disabledOrphaned := managedUpstreamKeyFixture(15, upstreamProvisioningKeyPrefix+"eeeeeeeeeeeeeeee", "removed-disabled")
	disabledOrphaned.Status = "disabled"
	randomNamedOrphaned := managedUpstreamKeyFixture(16, "BOG-p-0123456789abcdef", "removed-random")
	conflictingRandomName := managedUpstreamKeyFixture(17, "BOG-p-fedcba9876543210", "removed")
	conflictingRandomName.GroupName = "different-group"
	remote := map[int64]connector.APIKey{
		active.ID: active, orphaned.ID: orphaned, duplicateOrphaned.ID: duplicateOrphaned, manual.ID: manual,
		caseVariantActive.ID: caseVariantActive, secondOrphaned.ID: secondOrphaned,
		disabledOrphaned.ID: disabledOrphaned, randomNamedOrphaned.ID: randomNamedOrphaned,
		conflictingRandomName.ID: conflictingRandomName,
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "active", Ratio: 1}}, revealedKeys: map[int64]string{10: plain}}
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		items := make([]connector.APIKey, 0, len(remote))
		for _, item := range remote {
			items = append(items, item)
		}
		sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
		return &connector.APIKeyPage{Items: items, Page: 1, PageSize: upstreamKeyLookupPageSize, Pages: 1, Total: int64(len(items))}, nil
	}
	keys.deleteAPIKey = func(id int64) error {
		delete(remote, id)
		return nil
	}

	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("force sync: %v", err)
	}
	wantDeleted := map[int64]bool{
		orphaned.ID: true, duplicateOrphaned.ID: true, caseVariantActive.ID: true,
		secondOrphaned.ID: true, disabledOrphaned.ID: true, randomNamedOrphaned.ID: true,
	}
	if len(keys.deleted) != len(wantDeleted) {
		t.Fatalf("managed-prefix deletions = %v, want %v", keys.deleted, wantDeleted)
	}
	for _, id := range keys.deleted {
		if !wantDeleted[id] {
			t.Fatalf("unexpected managed-prefix deletion id %d", id)
		}
	}
	if len(remote) != 3 {
		t.Fatalf("remote keys changed after reconcile = %+v", remote)
	}
	history, err := service.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatalf("orphan cleanup audit = %+v err=%v", history, err)
	}
	seenAudits := map[int64]bool{}
	for _, change := range history {
		if change.Kind != "removed" {
			continue
		}
		if change.Status != "acknowledged" || change.ResolvedAt == nil || !wantDeleted[change.UpstreamKeyID] {
			t.Fatalf("managed inventory deletion audit = %+v", change)
		}
		seenAudits[change.UpstreamKeyID] = true
	}
	if len(seenAudits) != len(wantDeleted) {
		t.Fatalf("managed inventory deletion audit ids = %+v, want %v", seenAudits, wantDeleted)
	}
}

func TestSyncChannelModelFamiliesPersistsOrphanRemovalIntentBeforeDelete(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("orphan-removal-intent-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "orphan-removal-intent", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "removed", "added", 17, upstreamKeyNameForGroup("removed"), time.Now()); err != nil {
		t.Fatal(err)
	}
	plain := "sk-removal-intent"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	orphaned := managedUpstreamKeyFixture(17, upstreamKeyNameForGroup("removed"), "removed")
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "removed", UpstreamKeyID: orphaned.ID, UpstreamKeyName: orphaned.Name,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        encrypted, APIKeyPrefix: keyPrefix(plain), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{revealedKeys: map[int64]string{orphaned.ID: plain}}
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		return &connector.APIKeyPage{Items: []connector.APIKey{orphaned}, Page: 1, Pages: 1, Total: 1}, nil
	}
	keys.deleteAPIKey = func(int64) error {
		if err := sqlDB.Close(); err != nil {
			return err
		}
		return errors.New("remote deletion result is unknown")
	}
	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "removed"); err == nil {
		t.Fatal("delete succeeded after audit database was closed")
	}

	reopened, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatal(err)
	}
	reopenedStore := storage.NewGateway(reopened)
	history, err := reopenedStore.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatalf("durable orphan removal intent = %+v err=%v", history, err)
	}
	var intent *storage.GatewayUpstreamGroupChange
	for index := range history {
		if history[index].Status == "processing" && history[index].UpstreamKeyID == orphaned.ID {
			intent = &history[index]
			break
		}
	}
	if intent == nil || intent.UpstreamGroupID == nil || intent.PresenceGeneration == nil || len(intent.FactsDigest) != 64 {
		t.Fatalf("durable generation-safe removal intent = %+v", history)
	}
	if err := reopenedStore.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now()); err != nil {
		t.Fatalf("reconcile orphan removal intent: %v", err)
	}
	history, err = reopenedStore.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.First(intent, intent.ID).Error; err != nil || intent.Status != "acknowledged" || intent.ResolvedAt == nil {
		t.Fatalf("reconciled orphan removal audit = %+v err=%v", history, err)
	}
}

func TestSyncChannelModelFamiliesPersistsPreKeyFailure(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, _ := appcrypto.NewCipher("test-secret")
	channel := &storage.Channel{Name: "new", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "group", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	service := NewService(channels, rates, store, cipher, &fakeKeyManager{groupsErr: errors.New("group lookup failed")}, nil)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, false); err == nil {
		t.Fatal("sync succeeded")
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "group")
	if err != nil {
		t.Fatalf("find error row: %v", err)
	}
	if key.Enabled || key.ModelsError == "" {
		t.Fatalf("error row = %+v", key)
	}
}

type fakeKeyManager struct {
	groups           []connector.APIKeyGroup
	groupsErr        error
	listGroupCalls   int
	beforeListGroups func()
	created          []connector.APIKeyCreateRequest
	createResult     *connector.APIKey
	createErr        error
	listAPIKeys      func(connector.APIKeyQuery) (*connector.APIKeyPage, error)
	revealedKeys     map[int64]string
	revealAPIKey     func(int64) (string, error)
	revealCalls      []int64
	requestCost      *connector.RequestCostResult
	requestCostError error
	requestCostCalls int
	deleted          []int64
	deleteAPIKey     func(int64) error
	prepareDelete    func(connector.APIKey) (func() error, error)
}

type channelFailingKeyManager struct {
	*fakeKeyManager
	failingChannelID uint
	err              error
}

func (f *channelFailingKeyManager) ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	if channelID == f.failingChannelID {
		return nil, f.err
	}
	return f.fakeKeyManager.ListAPIKeys(ctx, channelID, query)
}

type originAwareFakeKeyManager struct {
	*fakeKeyManager
}

func (f *originAwareFakeKeyManager) Resolve(_ context.Context, channel *storage.Channel) (*connector.Channel, error) {
	if channel == nil {
		return nil, errors.New("channel is required")
	}
	apiURL := strings.TrimSpace(channel.APIURL)
	if apiURL == "" {
		apiURL = strings.TrimSpace(channel.SiteURL)
	}
	return &connector.Channel{
		ID:             channel.ID,
		Name:           channel.Name,
		Type:           connector.ChannelType(channel.Type),
		SiteURL:        apiURL,
		HomepageURL:    channel.SiteURL,
		APIOriginKnown: strings.TrimSpace(channel.APIURL) != "",
	}, nil
}

type orderedCostManager struct {
	fakeKeyManager
	mu           sync.Mutex
	lookupCount  int
	calls        chan string
	releaseFirst chan struct{}
}

func (m *orderedCostManager) LookupRequestCost(ctx context.Context, _ uint, query connector.RequestCostQuery) (*connector.RequestCostResult, error) {
	m.mu.Lock()
	m.lookupCount++
	call := m.lookupCount
	m.mu.Unlock()
	m.calls <- query.RequestID
	if call == 1 {
		select {
		case <-m.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("lookup pending")
}

func (f *fakeKeyManager) ListAPIKeys(_ context.Context, _ uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	if f.listAPIKeys != nil {
		return f.listAPIKeys(query)
	}
	return &connector.APIKeyPage{}, nil
}

func (f *fakeKeyManager) ListAPIKeyGroups(context.Context, uint) ([]connector.APIKeyGroup, error) {
	f.listGroupCalls++
	if f.beforeListGroups != nil {
		f.beforeListGroups()
	}
	return f.groups, f.groupsErr
}

func (f *fakeKeyManager) CreateAPIKey(_ context.Context, _ uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	f.created = append(f.created, req)
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.createResult != nil {
		out := *f.createResult
		return &out, nil
	}
	return &connector.APIKey{
		ID:                 int64(len(f.created)),
		Key:                "upstream-generated-key",
		Name:               req.Name,
		Group:              req.Group,
		GroupName:          req.Group,
		GroupID:            req.GroupID,
		Status:             "active",
		UnlimitedQuota:     true,
		ExpiredTime:        -1,
		CrossGroupRetry:    req.CrossGroupRetry != nil && *req.CrossGroupRetry,
		ModelLimitsEnabled: false,
	}, nil
}

func (f *fakeKeyManager) RevealAPIKey(_ context.Context, _ uint, id int64) (string, error) {
	f.revealCalls = append(f.revealCalls, id)
	if f.revealAPIKey != nil {
		return f.revealAPIKey(id)
	}
	return f.revealedKeys[id], nil
}

func (f *fakeKeyManager) DeleteAPIKey(_ context.Context, _ uint, id int64) error {
	f.deleted = append(f.deleted, id)
	if f.deleteAPIKey != nil {
		return f.deleteAPIKey(id)
	}
	return nil
}

func (f *fakeKeyManager) PrepareManagedAPIKeyDeletion(_ context.Context, _ uint, expected connector.APIKey, _ uint64) (func() error, error) {
	if f.prepareDelete != nil {
		return f.prepareDelete(expected)
	}
	return func() error { return f.DeleteAPIKey(context.Background(), 0, expected.ID) }, nil
}

func (f *fakeKeyManager) LookupRequestCost(context.Context, uint, connector.RequestCostQuery) (*connector.RequestCostResult, error) {
	f.requestCostCalls++
	return f.requestCost, f.requestCostError
}

func newUpstreamKeyRecoveryFixture(t *testing.T, name string) (*storage.Channel, *storage.Channels, *storage.Rates, *storage.Gateway, *appcrypto.Cipher) {
	_, channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureWithDB(t, name)
	return channel, channels, rates, store, cipher
}

func newUpstreamKeyRecoveryFixtureWithDB(t *testing.T, name string) (*gorm.DB, *storage.Channel, *storage.Channels, *storage.Rates, *storage.Gateway, *appcrypto.Cipher) {
	return newUpstreamKeyRecoveryFixtureForTypeWithDB(t, name, storage.ChannelTypeNewAPI)
}

func TestRebuildManagedUpstreamKeysDeletesOnlyManagedKeysAndRecreatesGroups(t *testing.T) {
	models := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer models.Close()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("rebuild-managed-key-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "rebuild", Type: storage.ChannelTypeNewAPI, SiteURL: models.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	oldPlain := "sk-old-managed"
	oldCipher, err := cipher.Encrypt(oldPlain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1, UpstreamKeyID: 10,
		UpstreamKeyName: "BOG - promo", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: oldCipher, APIKeyPrefix: keyPrefix(oldPlain), Enabled: true,
		ModelFamilies: "codex", ModelIDs: "gpt-4.1", LastError: "old failure", RoutingFailureCount: 3,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "obsolete", GroupRatio: 2, UpstreamKeyID: 11,
		UpstreamKeyName: "BridgeOps Gateway - obsolete", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: oldCipher, APIKeyPrefix: keyPrefix(oldPlain), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	// Bypass the canonical upsert to model a legacy case-only duplicate that
	// predates canonical group ownership enforcement.
	if err := db.Create(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "PROMO", GroupRatio: 3, UpstreamKeyID: 12,
		UpstreamKeyName: "BOG - PROMO", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: oldCipher, APIKeyPrefix: keyPrefix(oldPlain), Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	candidateGroup := &storage.GatewayCandidateGroup{Name: "preserved"}
	if err := store.SaveCandidateGroup(candidateGroup, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "promo"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&storage.GatewayCandidateGroupMember{CandidateGroupID: candidateGroup.ID, ChannelID: channel.ID, GroupName: "obsolete"}).Error; err != nil {
		t.Fatal(err)
	}
	downstream := &storage.GatewayDownstreamKey{
		Name: "case-route", App: storage.GatewayAppCodex, ModelFamily: "codex",
		CandidateGroupID: candidateGroup.ID, KeyHash: "case-route-hash", KeyPrefix: "sk-case",
		SmartRouting: false, ManualChannelID: channel.ID, ManualGroupName: "PROMO", Enabled: true,
	}
	if err := db.Create(downstream).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&storage.GatewayRouteExclusion{DownstreamKeyID: downstream.ID, ChannelID: channel.ID, GroupName: "promo"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&storage.GatewayRouteExclusion{DownstreamKeyID: downstream.ID, ChannelID: channel.ID, GroupName: "PROMO"}).Error; err != nil {
		t.Fatal(err)
	}
	remote := map[int64]connector.APIKey{
		10: managedUpstreamKeyFixture(10, "BOG - promo", "promo"),
		11: managedUpstreamKeyFixture(11, "BridgeOps Gateway - obsolete", "obsolete"),
		12: managedUpstreamKeyFixture(12, "BOG - PROMO", "PROMO"),
		98: {ID: 98, Name: "BOG - promo manual"},
		99: {ID: 99, Name: "manual-key"},
	}
	disabledManaged := managedUpstreamKeyFixture(97, legacyUpstreamKeyName+" - PROMO", "promo")
	disabledManaged.Status = "disabled"
	remote[disabledManaged.ID] = disabledManaged
	keys := &fakeKeyManager{
		groups:       []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		revealedKeys: map[int64]string{10: oldPlain, 11: oldPlain, 12: oldPlain},
	}
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		items := make([]connector.APIKey, 0, len(remote))
		for _, item := range remote {
			items = append(items, item)
		}
		sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
		return &connector.APIKeyPage{Items: items, Page: 1, PageSize: upstreamKeyLookupPageSize, Pages: 1, Total: int64(len(items))}, nil
	}
	keys.deleteAPIKey = func(id int64) error {
		delete(remote, id)
		return nil
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	result, err := svc.RebuildManagedUpstreamKeys(context.Background(), channel.ID)
	if err != nil {
		t.Fatalf("rebuild managed keys: %v", err)
	}
	if result.Matched != 3 || result.Deleted != 3 || result.Recreated != 1 || len(result.Items) != 1 || result.Items[0].Error != "" {
		t.Fatalf("rebuild result = %+v", result)
	}
	if len(keys.deleted) != 3 || keys.deleted[0] != 10 || keys.deleted[1] != 11 || keys.deleted[2] != 12 {
		t.Fatalf("deleted keys = %v", keys.deleted)
	}
	if _, ok := remote[99]; !ok {
		t.Fatal("manual upstream key was deleted")
	}
	if _, ok := remote[98]; !ok {
		t.Fatal("manual managed-looking key without a local ownership binding was deleted")
	}
	if _, ok := remote[disabledManaged.ID]; !ok {
		t.Fatal("unbound disabled managed-looking key was deleted")
	}
	if len(keys.created) != 1 || keys.created[0].Group != "promo" {
		t.Fatalf("created keys = %+v", keys.created)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !stored.Enabled || stored.UpstreamKeyID == 10 || stored.APIKeyCipher == oldCipher || stored.ModelIDs != "gpt-5" || stored.LastError != "" || stored.RoutingFailureCount != 0 {
		t.Fatalf("rebuilt local key = %+v err=%v", stored, err)
	}
	preserved, err := store.FindCandidateGroup(candidateGroup.ID)
	if err != nil || len(preserved.Members) != 2 {
		t.Fatalf("candidate group after rebuild = %+v err=%v", preserved, err)
	}
	preservedGroups := make(map[string]bool, len(preserved.Members))
	for _, member := range preserved.Members {
		if member.ChannelID == channel.ID {
			preservedGroups[member.GroupName] = true
		}
	}
	if !preservedGroups["promo"] || !preservedGroups["obsolete"] {
		t.Fatalf("candidate group routing configuration was not preserved: %+v", preserved)
	}
	storedKeys, err := store.ListUpstreamKeys([]uint{channel.ID})
	if err != nil || len(storedKeys) != 1 || storedKeys[0].GroupName != "promo" || !storedKeys[0].Enabled || storedKeys[0].LastError == managedUpstreamKeyRebuildPendingError {
		t.Fatalf("canonical local keys after rebuild = %+v err=%v", storedKeys, err)
	}
	var migratedDownstream storage.GatewayDownstreamKey
	if err := db.First(&migratedDownstream, downstream.ID).Error; err != nil || migratedDownstream.ManualGroupName != "promo" {
		t.Fatalf("manual route after rebuild = %+v err=%v", migratedDownstream, err)
	}
	exclusions, err := store.ListRouteExclusions(downstream.ID)
	if err != nil || len(exclusions) != 1 || exclusions[0].GroupName != "promo" {
		t.Fatalf("route exclusions after rebuild = %+v err=%v", exclusions, err)
	}
}

func TestRebuildManagedUpstreamKeysIdentityDriftEntersManualReview(t *testing.T) {
	db, channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureWithDB(t, "rebuild-identity-drift")
	plain := "sk-rebuild-identity-drift"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(91, upstreamProvisioningKeyPrefix+"abcdef0123456789", "promo")
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1, UpstreamKeyID: remote.ID,
		UpstreamKeyName: remote.Name, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: encrypted, APIKeyPrefix: keyPrefix(plain), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{listAPIKeys: managedKeyRemovalListFixture(map[int64]connector.APIKey{remote.ID: remote})}
	keys.prepareDelete = func(connector.APIKey) (func() error, error) {
		return func() error { return connector.ErrManagedAPIKeyDeletionIdentityChanged }, nil
	}
	service := NewService(channels, rates, store, cipher, keys, nil)
	result, err := service.RebuildManagedUpstreamKeys(context.Background(), channel.ID)
	if !errors.Is(err, connector.ErrManagedAPIKeyDeletionIdentityChanged) || result == nil || result.Matched != 1 || len(keys.deleted) != 0 {
		t.Fatalf("rebuild identity drift result=%+v deletes=%v err=%v", result, keys.deleted, err)
	}
	var intent storage.GatewayManagedKeyRebuildIntent
	if err := db.Where("channel_id = ?", channel.ID).First(&intent).Error; err != nil {
		t.Fatal(err)
	}
	if intent.Status != "manual_review" || intent.ManualReviewReason != "remote_identity_changed" {
		t.Fatalf("rebuild identity drift intent = %+v", intent)
	}
}

func TestRebuildManagedUpstreamKeysDisablesLocalStateWhenRemoteDeletePartiallyFails(t *testing.T) {
	db, channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureWithDB(t, "rebuild-delete-failure")
	models := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5"}]}`))
	}))
	defer models.Close()
	channel.SiteURL = models.URL
	if err := channels.UpdateWithUpstreamAccountIdentityChange(channel); err != nil {
		t.Fatal(err)
	}
	oldPlain := "sk-still-local"
	oldCipher, err := cipher.Encrypt(oldPlain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1, UpstreamKeyID: 7,
		UpstreamKeyName: "BOG - promo", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: oldCipher, APIKeyPrefix: keyPrefix(oldPlain), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	// Bypass the canonical upsert to model a legacy case-only duplicate that
	// the rebuild must delete independently before canonicalizing local state.
	if err := db.Create(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "PROMO", GroupRatio: 2, UpstreamKeyID: 8,
		UpstreamKeyName: "BOG - PROMO", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: oldCipher, APIKeyPrefix: keyPrefix(oldPlain), Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	remote := map[int64]connector.APIKey{
		7: managedUpstreamKeyFixture(7, "BOG - promo", "promo"),
		8: managedUpstreamKeyFixture(8, "BOG - PROMO", "PROMO"),
	}
	failSecondDelete := true
	keys := &fakeKeyManager{
		groups:       []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		revealedKeys: map[int64]string{7: oldPlain, 8: oldPlain},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			items := make([]connector.APIKey, 0, len(remote))
			for _, item := range remote {
				items = append(items, item)
			}
			sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
			return &connector.APIKeyPage{Items: items, Page: 1, Pages: 1, Total: int64(len(items))}, nil
		},
		deleteAPIKey: func(id int64) error {
			if id == 8 && failSecondDelete {
				return errors.New("delete unavailable")
			}
			delete(remote, id)
			return nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	result, err := svc.RebuildManagedUpstreamKeys(context.Background(), channel.ID)
	if err == nil || !strings.Contains(err.Error(), "delete unavailable") || result == nil || result.Matched != 2 || result.Deleted != 1 {
		t.Fatalf("rebuild result=%+v error=%v", result, err)
	}
	localKeys, err := store.ListUpstreamKeys([]uint{channel.ID})
	if err != nil {
		t.Fatalf("list local keys after failed delete: %v", err)
	}
	localByUpstreamID := make(map[int64]storage.GatewayUpstreamKey, len(localKeys))
	for _, localKey := range localKeys {
		localByUpstreamID[localKey.UpstreamKeyID] = localKey
	}
	stored := localByUpstreamID[7]
	if stored.ID == 0 || stored.Enabled || stored.APIKeyCipher != oldCipher || stored.LastError != managedUpstreamKeyRebuildPendingError {
		t.Fatalf("local key changed after failed delete = %+v", stored)
	}
	duplicate := localByUpstreamID[8]
	if duplicate.ID == 0 || duplicate.Enabled || duplicate.APIKeyCipher != oldCipher || duplicate.LastError != managedUpstreamKeyRebuildPendingError {
		t.Fatalf("duplicate identity lost after failed delete = %+v", duplicate)
	}
	if _, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err == nil || !strings.Contains(err.Error(), "rebuild is incomplete") {
		t.Fatalf("pending rebuild was reused: %v", err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("created after failed delete = %+v", keys.created)
	}
	// A failed remote DELETE has an uncertain outcome. Let its durable claim
	// lease expire before retrying so the rebuild can safely revalidate and
	// reclaim the exact pending intent.
	if err := db.Model(&storage.GatewayManagedKeyRebuildIntent{}).
		Where("channel_id = ? AND upstream_key_id = ? AND status = ?", channel.ID, 8, "processing").
		Update("updated_at", time.Now().Add(-managedUpstreamKeyRemovalStaleAfter-time.Second)).Error; err != nil {
		t.Fatalf("expire uncertain delete claim: %v", err)
	}
	failSecondDelete = false
	result, err = svc.RebuildManagedUpstreamKeys(context.Background(), channel.ID)
	if err != nil || result.Matched != 1 || result.Deleted != 1 || result.Recreated != 1 {
		t.Fatalf("retry rebuild result=%+v error=%v", result, err)
	}
	storedKeys, err := store.ListUpstreamKeys([]uint{channel.ID})
	if err != nil || len(storedKeys) != 1 || storedKeys[0].GroupName != "promo" || !storedKeys[0].Enabled {
		t.Fatalf("retry canonical keys = %+v err=%v", storedKeys, err)
	}
}

func TestRebuildManagedUpstreamKeysKeepsPendingStateWhenSyncFails(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "rebuild-sync-failure")
	oldPlain := "sk-old"
	oldCipher, err := cipher.Encrypt(oldPlain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1, UpstreamKeyID: 7,
		UpstreamKeyName: upstreamKeyNameForGroup("promo"), OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: oldCipher,
		APIKeyPrefix: keyPrefix(oldPlain), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	remote := map[int64]connector.APIKey{
		7: managedUpstreamKeyFixture(7, upstreamKeyNameForGroup("promo"), "promo"),
	}
	listCalls := 0
	keys := &fakeKeyManager{
		groups:       []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		revealedKeys: map[int64]string{7: oldPlain},
		deleteAPIKey: func(id int64) error {
			delete(remote, id)
			return nil
		},
	}
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		listCalls++
		// Calls one and two are the initial inventory and the strict pre-delete
		// revalidation. Fail only when forced model sync lists remote orphans.
		if listCalls > 2 {
			return nil, errors.New("remote list unavailable during sync")
		}
		items := make([]connector.APIKey, 0, len(remote))
		for _, item := range remote {
			items = append(items, item)
		}
		return &connector.APIKeyPage{Items: items, Page: 1, Pages: 1, Total: int64(len(items))}, nil
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	result, err := svc.RebuildManagedUpstreamKeys(context.Background(), channel.ID)
	if err == nil || !strings.Contains(err.Error(), "remote list unavailable") || result == nil || result.Deleted != 1 {
		t.Fatalf("rebuild result=%+v error=%v", result, err)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || stored.Enabled || stored.UpstreamKeyID != 0 || stored.APIKeyCipher != "" || stored.LastError != managedUpstreamKeyRebuildPendingError {
		t.Fatalf("local key after failed rebuild sync = %+v err=%v", stored, err)
	}
	if _, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err == nil || !strings.Contains(err.Error(), "rebuild is incomplete") {
		t.Fatalf("ordinary request bypassed pending rebuild: %v", err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("created keys after failed rebuild sync = %+v", keys.created)
	}
}

func TestRebuildManagedUpstreamKeysRejectsManagedRemoteKeyWithoutID(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "rebuild-missing-id")
	oldCipher, err := cipher.Encrypt("sk-old")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1,
		UpstreamKeyName: upstreamKeyNameForGroup("promo"), OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: oldCipher,
		APIKeyPrefix: "sk-old", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		return &connector.APIKeyPage{Items: []connector.APIKey{{ID: 0, Name: upstreamKeyNameForGroup("promo")}}, Page: 1, Pages: 1, Total: 1}, nil
	}}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	result, err := svc.RebuildManagedUpstreamKeys(context.Background(), channel.ID)
	if err == nil || !strings.Contains(err.Error(), "no deletable remote id") || result != nil {
		t.Fatalf("rebuild result=%+v error=%v", result, err)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !stored.Enabled || stored.APIKeyCipher != oldCipher || stored.LastError != "" {
		t.Fatalf("local key changed after missing remote id = %+v err=%v", stored, err)
	}
	if len(keys.deleted) != 0 || len(keys.created) != 0 {
		t.Fatalf("remote mutations after missing id: deleted=%v created=%+v", keys.deleted, keys.created)
	}
}

func newUpstreamKeyRecoveryFixtureForType(t *testing.T, name string, channelType storage.ChannelType) (*storage.Channel, *storage.Channels, *storage.Rates, *storage.Gateway, *appcrypto.Cipher) {
	_, channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForTypeWithDB(t, name, channelType)
	return channel, channels, rates, store, cipher
}

func newUpstreamKeyRecoveryFixtureForTypeWithDB(t *testing.T, name string, channelType storage.ChannelType) (*gorm.DB, *storage.Channel, *storage.Channels, *storage.Rates, *storage.Gateway, *appcrypto.Cipher) {
	t.Helper()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("managed-key-recovery-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: name, Type: channelType, SiteURL: "https://recovery.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	return db, channel, channels, rates, store, cipher
}

func managedSub2APIKeyFixture(id int64, name, group string) connector.APIKey {
	return connector.APIKey{
		ID:        id,
		Name:      name,
		Status:    "active",
		GroupName: group,
		Quota:     0,
		Source:    connector.APIKeySourceSub2API,
		ObservedFields: connector.APIKeyObservedFields{
			Quota:       true,
			ExpiresAt:   true,
			IPWhitelist: true,
			IPBlacklist: true,
			RateLimit5h: true,
			RateLimit1d: true,
			RateLimit7d: true,
		},
	}
}

func TestRecoverableSub2APIKeyRejectsExplicitNullNumericFingerprint(t *testing.T) {
	key := managedSub2APIKeyFixture(7, upstreamKeyNameForGroup("promo"), "promo")
	key.ObservedFields.Quota = false
	key.NullFields.Quota = true
	if recoverableSub2APIKeyFingerprint(key, true) {
		t.Fatal("bound sub2api key accepted explicit null quota")
	}
	key.NullFields.Quota = false
	key.NullFields.RateLimit5h = true
	if recoverableSub2APIKeyFingerprint(key, true) {
		t.Fatal("bound sub2api key accepted explicit null rate limit")
	}
}

func managedUpstreamKeyFixture(id int64, name, group string) connector.APIKey {
	return connector.APIKey{
		ID:                 id,
		Name:               name,
		Status:             "active",
		Group:              group,
		GroupName:          group,
		UnlimitedQuota:     true,
		ExpiredTime:        -1,
		ModelLimitsEnabled: false,
		CrossGroupRetry:    false,
		Source:             connector.APIKeySourceNewAPI,
		ObservedFields: connector.APIKeyObservedFields{
			Quota:              true,
			UnlimitedQuota:     true,
			ExpiredTime:        true,
			AllowIPs:           true,
			ModelLimitsEnabled: true,
			ModelLimits:        true,
			CrossGroupRetry:    true,
		},
	}
}

func TestManagedUpstreamKeyGroupNameFallsBackToUniqueLocalRemoteID(t *testing.T) {
	managedName := upstreamProvisioningKeyPrefix + "0123456789abcdef"
	remote := managedSub2APIKeyFixture(71, managedName, "")
	groupID := int64(999)
	remote.GroupID = &groupID
	remote.GroupDirectoryComplete = true
	local := storage.GatewayUpstreamKey{
		ID: 17, ChannelID: 3, GroupName: "removed",
		UpstreamKeyID: remote.ID, UpstreamKeyName: managedName,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
	}
	localByRemoteID := uniqueManagedLocalKeysByRemoteID([]storage.GatewayUpstreamKey{local})
	if got := managedUpstreamKeyGroupName(remote, localByRemoteID); got != "removed" {
		t.Fatalf("fallback group = %q, want removed", got)
	}

	remote.GroupName = "current"
	if got := managedUpstreamKeyGroupName(remote, localByRemoteID); got != "current" {
		t.Fatalf("explicit provider group = %q, want current", got)
	}

	remote.GroupName = ""
	remote.Name = upstreamProvisioningKeyPrefix + "1111111111111111"
	if got := managedUpstreamKeyGroupName(remote, localByRemoteID); got != "" {
		t.Fatalf("reused remote ID inherited stale local group %q", got)
	}
}

func TestUniqueManagedLocalKeysByRemoteIDRejectsAmbiguousBindings(t *testing.T) {
	managedName := upstreamProvisioningKeyPrefix + "fedcba9876543210"
	locals := []storage.GatewayUpstreamKey{
		{ID: 1, ChannelID: 3, GroupName: "first", UpstreamKeyID: 81, UpstreamKeyName: managedName, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated},
		{ID: 2, ChannelID: 3, GroupName: "second", UpstreamKeyID: 81, UpstreamKeyName: managedName, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated},
	}
	if got := uniqueManagedLocalKeysByRemoteID(locals); len(got) != 0 {
		t.Fatalf("ambiguous local bindings were accepted: %+v", got)
	}
}

func TestManagedUpstreamKeyGroupNameRejectsIncompleteOrUntrustedFallback(t *testing.T) {
	managedName := upstreamProvisioningKeyPrefix + "abcdef0123456789"
	groupID := int64(92)
	remote := managedSub2APIKeyFixture(92, managedName, "")
	remote.GroupID = &groupID
	local := storage.GatewayUpstreamKey{
		ID: 9, ChannelID: 3, GroupName: "removed", UpstreamKeyID: remote.ID,
		UpstreamKeyName: managedName, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
	}
	localByRemoteID := uniqueManagedLocalKeysByRemoteID([]storage.GatewayUpstreamKey{local})
	if got := managedUpstreamKeyGroupName(remote, localByRemoteID); got != "" {
		t.Fatalf("incomplete group directory inherited stale local group %q", got)
	}

	remote.GroupDirectoryComplete = true
	remote.GroupID = nil
	if got := managedUpstreamKeyGroupName(remote, localByRemoteID); got != "" {
		t.Fatalf("missing group id inherited stale local group %q", got)
	}

	remote.GroupID = &groupID
	local.OwnershipProvenance = storage.GatewayUpstreamKeyProvenanceUnknown
	if got := managedUpstreamKeyGroupName(remote, uniqueManagedLocalKeysByRemoteID([]storage.GatewayUpstreamKey{local})); got != "" {
		t.Fatalf("untrusted local ownership supplied group %q", got)
	}
}

func TestUsableLocalManagedKeyMatchingRemoteSkipsOnlyExactTrustedBinding(t *testing.T) {
	cipher, err := appcrypto.NewCipher("local-managed-key-fast-path")
	if err != nil {
		t.Fatal(err)
	}
	plain := "sk-local-managed"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(101, upstreamProvisioningKeyPrefix+"0123456789abcdef", "promo")
	local := storage.GatewayUpstreamKey{
		ID: 1, ChannelID: 2, GroupName: "promo", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: encrypted,
		APIKeyPrefix: keyPrefix(plain), Enabled: true,
	}
	service := &Service{Cipher: cipher}
	if service.usableLocalManagedKeyMatchingRemote([]storage.GatewayUpstreamKey{local}, remote, "promo") == nil {
		t.Fatal("exact trusted local binding was not reusable")
	}

	local.OwnershipProvenance = storage.GatewayUpstreamKeyProvenanceUnknown
	if service.usableLocalManagedKeyMatchingRemote([]storage.GatewayUpstreamKey{local}, remote, "promo") != nil {
		t.Fatal("untrusted local binding bypassed remote reveal")
	}
	local.OwnershipProvenance = storage.GatewayUpstreamKeyProvenanceCreated
	local.UpstreamKeyID++
	if service.usableLocalManagedKeyMatchingRemote([]storage.GatewayUpstreamKey{local}, remote, "promo") != nil {
		t.Fatal("different remote id bypassed remote reveal")
	}
}

func TestManagedInventoryRequestsCompleteGroupIdentity(t *testing.T) {
	strict := false
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		strict = query.RequireCompleteGroupIdentity
		return nil, errors.New("incomplete group inventory")
	}}}
	items, err := service.listAllUpstreamKeys(context.Background(), 1)
	if err == nil || items != nil || !strict {
		t.Fatalf("managed inventory items=%v err=%v strict=%v", items, err, strict)
	}
}

func TestFindReusableUpstreamKeyScopesSub2APIInventoryToGroupID(t *testing.T) {
	groupID := int64(17)
	queries := make([]connector.APIKeyQuery, 0, 1)
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		queries = append(queries, query)
		return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
	}}}
	if _, err := service.findReusableUpstreamKey(context.Background(), 3, "promo", &groupID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("find reusable key error = %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("queries = %+v", queries)
	}
	for _, query := range queries {
		if query.GroupID != "17" || query.Search != "" {
			t.Fatalf("unscoped Sub2API recovery query = %+v", query)
		}
	}
}

func TestFindCachedUpstreamIdentityScopesSub2APIInventoryToGroupID(t *testing.T) {
	groupID := int64(19)
	queries := make([]connector.APIKeyQuery, 0, 1)
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		queries = append(queries, query)
		return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
	}}}
	if _, err := service.findCachedUpstreamIdentity(context.Background(), 3, "promo", &groupID, "sk-cached"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("find cached upstream identity error = %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("queries = %+v", queries)
	}
	for _, query := range queries {
		if query.GroupID != "19" || query.Search != "" {
			t.Fatalf("unscoped Sub2API cached identity query = %+v", query)
		}
	}
}

func TestFindReusableUpstreamKeyKeepsLegacySearchesWithoutNumericGroupID(t *testing.T) {
	queries := make([]connector.APIKeyQuery, 0, len(managedUpstreamKeySearches()))
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		queries = append(queries, query)
		return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
	}}}
	if _, err := service.findReusableUpstreamKey(context.Background(), 3, "promo", nil); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("find reusable key error = %v", err)
	}
	searches := managedUpstreamKeySearches()
	if len(queries) != len(searches) {
		t.Fatalf("queries = %+v", queries)
	}
	for index, query := range queries {
		if query.GroupID != "" || query.Search != searches[index] {
			t.Fatalf("provider-agnostic recovery query[%d] = %+v", index, query)
		}
	}
}

func TestDeletedSub2APIManagedKeyInventoryGroupRequiresCompleteOrphanEvidence(t *testing.T) {
	groupID := int64(55)
	remote := managedSub2APIKeyFixture(105, upstreamProvisioningKeyPrefix+"fedcba9876543210", "")
	remote.GroupID = &groupID
	remote.GroupDirectoryComplete = true
	groupName, inventoryKey, ok := deletedSub2APIManagedKeyInventoryGroup(remote)
	if !ok || groupName != storage.ReservedInternalUpstreamGroupPrefix+"deleted_sub2api_group_id__:55" ||
		inventoryKey != "\x00"+groupName || canonicalGroupName(groupName) == inventoryKey {
		t.Fatalf("deleted group identity name=%q key=%q ok=%v", groupName, inventoryKey, ok)
	}

	remote.GroupDirectoryComplete = false
	if _, _, ok := deletedSub2APIManagedKeyInventoryGroup(remote); ok {
		t.Fatal("incomplete group directory classified a managed key as orphaned")
	}
	remote.GroupDirectoryComplete = true
	remote.GroupName = "current"
	if _, _, ok := deletedSub2APIManagedKeyInventoryGroup(remote); ok {
		t.Fatal("explicit provider group classified a managed key as orphaned")
	}
}

func TestStrongManagedUpstreamKeyPrefixIsTheAutomaticOwnershipBoundary(t *testing.T) {
	managed := []string{
		upstreamProvisioningKeyPrefix + "0123456789abcdef",
	}
	for _, name := range managed {
		if !isStronglyManagedUpstreamKeyName(name) {
			t.Fatalf("strong managed prefix rejected %q", name)
		}
	}
	manualNames := []string{
		"BOG - promo",
		legacyUpstreamKeyName + " - promo",
		historicUpstreamKeyName + " - promo",
	}
	for _, name := range manualNames {
		if isStronglyManagedUpstreamKeyName(name) {
			t.Fatalf("historical/manual name accepted as automatic ownership %q", name)
		}
	}
	if isStronglyManagedUpstreamKeyName("manual-key") {
		t.Fatal("unprefixed manual key accepted")
	}
}

func TestManagedProvisioningKeyAttributesAcceptRandomManagedName(t *testing.T) {
	key := managedUpstreamKeyFixture(7, upstreamProvisioningKeyPrefix+"0123456789abcdef", "promo")
	if !managedUpstreamKeyExpectedAttributes(key, "promo") {
		t.Fatalf("valid provisioning key rejected: %+v", key)
	}
	key.CrossGroupRetry = true
	if !managedUpstreamKeyExpectedAttributes(key, "promo") {
		t.Fatal("historical cross-group provisioning key rejected")
	}
}

func TestEnsureUpstreamKeyDoesNotReuseNameMatchedManualKey(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("managed-key-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "managed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://managed.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	manualID := int64(99)
	keys := &fakeKeyManager{
		groups:       []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		revealedKeys: map[int64]string{manualID: "sk-manual"},
	}
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		items := []connector.APIKey{{ID: manualID, Name: upstreamKeyNameForGroup("promo"), GroupName: "promo", UnlimitedQuota: true}}
		if len(keys.created) > 0 {
			items = append(items, managedUpstreamKeyFixture(1, keys.created[0].Name, keys.created[0].Group))
		}
		return &connector.APIKeyPage{Items: items}, nil
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	if _, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err != nil {
		t.Fatalf("ensure upstream key: %v", err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("manual name-matched key was reused, created=%d", len(keys.created))
	}
	keys.revealedKeys[1] = "upstream-generated-key"
	if err := rates.DeleteSnapshot(channel.ID, "promo"); err != nil {
		t.Fatalf("delete promo rate: %v", err)
	}
	if err := svc.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("delete group state: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != 1 {
		t.Fatalf("managed upstream key deletion = %+v", keys.deleted)
	}
	for _, deleted := range keys.deleted {
		if deleted == manualID {
			t.Fatal("name-matched manual upstream key was deleted")
		}
	}
}

func TestEnsureUpstreamKeyAdoptsOldestManagedKeyAtRemoteLimit(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("managed-key-limit-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "limited", Type: storage.ChannelTypeNewAPI, SiteURL: "https://limited.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups:    []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		createErr: errors.New("newapi create api key: 已达到最大令牌数量限制 (20)"),
		listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			items := []connector.APIKey{
				{ID: 1, Name: "manual", GroupName: "promo", Status: "active", UnlimitedQuota: true},
				{ID: 9, Name: "BOG - promo", GroupName: "promo", Status: "active", UnlimitedQuota: true, ExpiredTime: -1, CrossGroupRetry: true},
				{ID: 4, Name: "BridgeOps Gateway - promo", GroupName: "promo", Status: "active", UnlimitedQuota: true, ExpiredTime: -1, CrossGroupRetry: true},
				{ID: 2, Name: "BOG - other", GroupName: "other", Status: "active", UnlimitedQuota: true},
			}
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: items}, nil
		},
		revealedKeys: map[int64]string{4: "sk-oldest-managed", 9: "sk-newer-managed"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || plain != "sk-oldest-managed" {
		t.Fatalf("ensure upstream key = %q, %v", plain, err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("create calls = %d, want 1", len(keys.created))
	}
	adopted, err := store.FindUpstreamKey(channel.ID, "promo")
	if err != nil || !adopted.Enabled || adopted.Provisioning || adopted.UpstreamKeyID != 4 {
		t.Fatalf("adopted upstream key = %+v, err=%v", adopted, err)
	}
}

func TestEnsureUpstreamKeyRecoversManagedKeyFromLocalErrorRow(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("managed-key-error-row-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "recover", Type: storage.ChannelTypeNewAPI, SiteURL: "https://recover.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamKeyModelsError(channel.ID, "promo", time.Now(), "newapi create api key: legacy failure"); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{{
				ID: 7, Name: "BOG - promo", GroupName: "promo", Status: "active", UnlimitedQuota: true, ExpiredTime: -1, CrossGroupRetry: true,
			}}}, nil
		},
		revealedKeys: map[int64]string{7: "sk-recovered"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || plain != "sk-recovered" {
		t.Fatalf("recover upstream key = %q, %v", plain, err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("create calls = %d, want 0", len(keys.created))
	}
}

func TestEnsureUpstreamKeyRefusesReplacementAfterConfirmedDeletion(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("existing-key-state-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "existing", Type: storage.ChannelTypeNewAPI, SiteURL: "https://existing.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7,
		UpstreamKeyName: upstreamProvisioningKeyPrefix + "deadbeefdeadbeef", Enabled: false, LastError: "upstream key invalidated",
	}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "refusing automatic replacement") {
		t.Fatalf("deleted managed key replacement = %q, %v", plain, err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("create calls after failed recovery = %d, want 0", len(keys.created))
	}
	stored, err := store.FindUpstreamKey(channel.ID, "promo")
	if err != nil {
		t.Fatalf("find failed recovery state: %v", err)
	}
	if stored.Provisioning || stored.UpstreamKeyID != 7 || stored.UpstreamKeyName != upstreamProvisioningKeyPrefix+"deadbeefdeadbeef" || !strings.Contains(stored.LastError, "refusing automatic replacement") {
		t.Fatalf("failed recovery state = %+v", stored)
	}
}

func TestEnsureUpstreamKeyRefusesAutomaticReplacementWithoutManagedIdentity(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("unknown-key-state-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "unknown", Type: storage.ChannelTypeNewAPI, SiteURL: "https://unknown.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7,
		UpstreamKeyName: "manual-key", Enabled: false, LastError: "upstream key invalidated",
	}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "refusing automatic replacement") {
		t.Fatalf("unknown state replacement = %q, %v", plain, err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("create calls = %d, want 0", len(keys.created))
	}
}

func TestEnsureUpstreamKeyRepairsHistoricManagedIdentity(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("historic-key-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "historic", Type: storage.ChannelTypeSub2API, SiteURL: "https://historic.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	plain := "sk-historic"
	cipherText, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", APIKeyCipher: cipherText, APIKeyPrefix: "sk-his", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	listCalls := 0
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			listCalls++
			if query.Search == historicUpstreamKeyName {
				return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{{
					ID: 44, Name: historicUpstreamKeyName + " - promo", GroupName: "promo", Group: "promo", Status: "active",
					UnlimitedQuota: true, ExpiredTime: -1, CrossGroupRetry: true,
				}}}, nil
			}
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
		// The legacy managed key still carries the creation-time settings that
		// BridgeOps uses to distinguish it from a hand-created same-name key.
		revealedKeys: map[int64]string{44: plain},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	lease, err := svc.EnsureUpstreamAPIKeyLeaseForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || lease.Key != plain {
		t.Fatalf("repair historic key = %+v, %v", lease, err)
	}
	if lease.UpstreamKeyID != 44 || lease.UpstreamKeyName != historicUpstreamKeyName+" - promo" {
		t.Fatalf("repair historic key lease = %+v", lease)
	}
	stored, err := store.FindUpstreamKey(channel.ID, "promo")
	if err != nil || stored.UpstreamKeyID != 44 || stored.UpstreamKeyName != historicUpstreamKeyName+" - promo" {
		t.Fatalf("repaired identity = %+v, err=%v", stored, err)
	}
	if len(keys.created) != 0 || listCalls == 0 {
		t.Fatalf("create/list calls = %d/%d", len(keys.created), listCalls)
	}
	listCallsBefore := listCalls
	if got, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err != nil || got != plain {
		t.Fatalf("use repaired key = %q, %v", got, err)
	}
	if listCalls != listCallsBefore {
		t.Fatalf("repaired identity should use the hot path, list calls = %d, want %d", listCalls, listCallsBefore)
	}
}

func TestFixedUpstreamKeyLeaseUsesLocalCipherWithoutIdentityRepair(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("fixed-local-key-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "fixed-local-key", Type: storage.ChannelTypeSub2API, SiteURL: "https://fixed-local.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	plain := "sk-fixed-local"
	cipherText, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", APIKeyCipher: cipherText, APIKeyPrefix: "sk-fix", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	controlPlaneCalls := 0
	keys := &fakeKeyManager{
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			controlPlaneCalls++
			return nil, errors.New("control-plane lookup must not run for a fixed route")
		},
	}
	svc := NewService(channels, storage.NewRates(db), store, cipher, keys, nil)
	lease, err := svc.FixedUpstreamAPIKeyLeaseForGroup(channel.ID, " promo ")
	if err != nil {
		t.Fatalf("fixed local key lease: %v", err)
	}
	if lease.Key != plain || lease.UpstreamKeyID != 0 || lease.UpstreamKeyName != "" {
		t.Fatalf("fixed local key lease = %+v", lease)
	}
	if controlPlaneCalls != 0 {
		t.Fatalf("fixed local key lease made %d control-plane calls", controlPlaneCalls)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || stored.UpstreamKeyID != 0 || strings.TrimSpace(stored.UpstreamKeyName) != "" {
		t.Fatalf("fixed local key identity changed: key=%+v err=%v", stored, err)
	}
}

func TestEnsureUpstreamKeyLeaseReturnsPostDiscoveryChannelSnapshot(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForType(t, "sub2api-origin-discovery", storage.ChannelTypeSub2API)
	const discoveredAPIURL = "https://api.recovery.example"
	originalRevision := channel.UpstreamAccountIdentityRevision
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
			})
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	lease, err := svc.EnsureUpstreamAPIKeyLeaseForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil {
		t.Fatalf("ensure upstream key lease: %v", err)
	}
	if lease.Key == "" || lease.ChannelSnapshot == nil {
		t.Fatalf("lease did not freeze post-discovery channel: %+v", lease)
	}
	if lease.ChannelSnapshot.APIURL != discoveredAPIURL || lease.ChannelSnapshot.UpstreamAccountIdentityRevision <= originalRevision {
		t.Fatalf("lease channel snapshot = api=%q revision=%d, want api=%q revision>%d", lease.ChannelSnapshot.APIURL, lease.ChannelSnapshot.UpstreamAccountIdentityRevision, discoveredAPIURL, originalRevision)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find discovered channel: %v", err)
	}
	if stored.APIURL != lease.ChannelSnapshot.APIURL || stored.UpstreamAccountIdentityRevision != lease.ChannelSnapshot.UpstreamAccountIdentityRevision {
		t.Fatalf("lease snapshot does not match locked channel: lease=%+v stored=%+v", lease.ChannelSnapshot, stored)
	}
}

func TestModelSyncRestartsFromDiscoveredAPIOrigin(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	var homepageHits atomic.Int64
	homepage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		homepageHits.Add(1)
		http.Error(w, "homepage is not the API origin", http.StatusNotFound)
	}))
	defer homepage.Close()
	var apiHits atomic.Int64
	apiOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		apiHits.Add(1)
		if request.URL.Path != "/v1/models" {
			http.Error(w, "unexpected API path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[{"id":"claude-sonnet-4"}]}`)
	}))
	defer apiOrigin.Close()

	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("sub2api-model-origin-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "sub2api model origin", Type: storage.ChannelTypeSub2API,
		SiteURL: homepage.URL, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	originalRevision := channel.UpstreamAccountIdentityRevision
	var discoverOnce sync.Once
	baseKeys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		beforeListGroups: func() {
			discoverOnce.Do(func() {
				updated, _, updateErr := channels.UpdateAPIURLRevisionIfUnchanged(
					channel.ID, originalRevision, channel.SiteURL, "", apiOrigin.URL,
				)
				if updateErr != nil {
					t.Fatalf("persist model API origin: %v", updateErr)
				}
				if !updated {
					t.Fatal("model API origin was not persisted")
				}
			})
		},
	}
	service := NewService(channels, rates, store, cipher, &originAwareFakeKeyManager{fakeKeyManager: baseKeys}, nil)
	if err := service.SyncChannelModelFamilies(context.Background(), channel.ID, true); err != nil {
		t.Fatalf("sync model families after API-origin discovery: %v", err)
	}
	if got := homepageHits.Load(); got != 0 {
		t.Fatalf("model sync sent %d request(s) to the homepage", got)
	}
	if got := apiHits.Load(); got == 0 {
		t.Fatal("model sync never reached the discovered API origin")
	}
	key, err := store.FindUpstreamKey(channel.ID, "promo")
	if err != nil {
		t.Fatalf("find synced upstream key: %v", err)
	}
	if key.ModelsSyncedAt == nil || key.ModelsError != "" || !strings.Contains(key.ModelIDs, "claude-sonnet-4") {
		t.Fatalf("synced key = %+v", key)
	}
}

func TestEnsureUpstreamKeyBacksOffMissingIdentityRepair(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("missing-identity-key-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "missing-identity", Type: storage.ChannelTypeSub2API, SiteURL: "https://missing-identity.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	plain := "sk-missing-identity"
	cipherText, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", APIKeyCipher: cipherText, APIKeyPrefix: "sk-mis", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	listCalls := 0
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			listCalls++
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	if got, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err != nil || got != plain {
		t.Fatalf("first missing-identity use = %q, %v", got, err)
	}
	listCallsAfterFirst := listCalls
	if listCallsAfterFirst == 0 {
		t.Fatal("first missing-identity use did not try to repair")
	}
	if got, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err != nil || got != plain {
		t.Fatalf("second missing-identity use = %q, %v", got, err)
	}
	if listCalls != listCallsAfterFirst {
		t.Fatalf("missing-identity repair was retried without backoff: %d -> %d", listCallsAfterFirst, listCalls)
	}
}

func TestEnsureUpstreamKeyCreatesFromUnidentifiedErrorState(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("orphaned-key-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "orphaned", Type: storage.ChannelTypeNewAPI, SiteURL: "https://orphaned.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", Enabled: false, ModelsError: "previous upstream key provisioning failed",
	}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	got, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || got == "" {
		t.Fatalf("create from error-only state = %q, %v", got, err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("create calls after error-only recovery = %d, want 1", len(keys.created))
	}
	stored, err := store.FindUpstreamKey(channel.ID, "promo")
	if err != nil || stored.Provisioning || !stored.Enabled || stored.APIKeyCipher == "" || stored.LastError != "" {
		t.Fatalf("created key state = %+v, err=%v", stored, err)
	}
}

func TestEnsureUpstreamKeyRefusesCreateWithoutRecoveryGeneration(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "missing-managed-key-recovery-generation")
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "removed", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "capture managed key recovery generation") {
		t.Fatalf("create without recovery generation = %q, %v", plain, err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("create without recovery generation posted %d request(s)", len(keys.created))
	}
	if pending, findErr := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(findErr, gorm.ErrRecordNotFound) || pending != nil {
		t.Fatalf("create without recovery generation left provisioning evidence = %#v, %v", pending, findErr)
	}
}

func TestEnsureUpstreamKeyDoesNotPostForActiveProvisioningClaim(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "active-provisioning-claim")
	nonce := upstreamProvisioningKeyPrefix + "0123456789abcdef"
	startedAt := time.Now().UTC()
	if _, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "promo", nonce, 1, startedAt, upstreamKeyProvisioningStaleAfter); err != nil || !claimed || !fresh {
		t.Fatalf("seed active provisioning claim: claimed=%v fresh=%v err=%v", claimed, fresh, err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "recovery is already in progress") {
		t.Fatalf("active provisioning claim ensure = %q, %v", plain, err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("active provisioning claim posted %d create requests", len(keys.created))
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || stored.UpstreamKeyName != nonce || !stored.Provisioning {
		t.Fatalf("active provisioning claim changed = %+v, err=%v", stored, err)
	}
}

func TestEnsureUpstreamKeyRetriesStaleProvisioningClaimOnceWithPersistedNonce(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "stale-provisioning-claim")
	nonce := upstreamProvisioningKeyPrefix + "fedcba9876543210"
	startedAt := time.Now().UTC().Add(-upstreamKeyProvisioningStaleAfter - time.Minute)
	if _, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "promo", nonce, 1, startedAt, upstreamKeyProvisioningStaleAfter); err != nil || !claimed || !fresh {
		t.Fatalf("seed stale provisioning claim: claimed=%v fresh=%v err=%v", claimed, fresh, err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)

	type ensureResult struct {
		plain string
		err   error
	}
	results := make(chan ensureResult, 2)
	for range 2 {
		go func() {
			plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
			results <- ensureResult{plain: plain, err: err}
		}()
	}
	succeeded := 0
	for range 2 {
		result := <-results
		if result.err == nil {
			succeeded++
			if result.plain != "upstream-generated-key" {
				t.Fatalf("stale provisioning recovered key = %q", result.plain)
			}
			continue
		}
		if result.plain != "" || !strings.Contains(result.err.Error(), "recovery is already in progress") {
			t.Fatalf("unclaimed stale provisioning recovery = %q, %v", result.plain, result.err)
		}
	}
	if succeeded == 0 {
		t.Fatal("no caller completed stale provisioning recovery")
	}
	if len(keys.created) != 1 {
		t.Fatalf("stale provisioning create calls = %d, want 1", len(keys.created))
	}
	if keys.created[0].Name != nonce {
		t.Fatalf("stale provisioning nonce = %q, want %q", keys.created[0].Name, nonce)
	}
	stored, err := store.FindUpstreamKey(channel.ID, "promo")
	if err != nil || stored.Provisioning || !stored.Enabled || stored.UpstreamKeyName != nonce {
		t.Fatalf("recovered stale provisioning key = %+v, err=%v", stored, err)
	}
}

func TestEnsureUpstreamKeyPassesCurrentRecoveryFenceToCreateOperation(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "managed-key-recovery-fence")
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || plain == "" || len(keys.created) != 1 {
		t.Fatalf("create with recovery fence = %q requests=%+v err=%v", plain, keys.created, err)
	}
	fence := keys.created[0].RecoveryFence
	if fence == nil || !fence.Valid() || fence.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision ||
		fence.UpstreamGroupID != generation.ID || fence.PresenceGeneration != generation.PresenceGeneration ||
		fence.CanonicalGroupName != generation.CanonicalGroupName {
		t.Fatalf("managed-key recovery fence = %#v, generation=%+v", fence, generation)
	}
}

func TestEnsureSub2APIKeyUsesResolvedRenamedGroupRecoveryFence(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForType(t, "renamed-managed-key-recovery-fence", storage.ChannelTypeSub2API)
	if err := store.RecordUpstreamGroupChange(channel.ID, "renamed-promo", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	renamedGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "renamed-promo")
	if err != nil {
		t.Fatal(err)
	}
	groupID := int64(17)
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{ID: &groupID, Name: "renamed-promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", &groupID, 1)
	if err != nil || plain == "" || len(keys.created) != 1 {
		t.Fatalf("create for renamed Sub2API group = %q requests=%+v err=%v", plain, keys.created, err)
	}
	request := keys.created[0]
	if request.Group != "renamed-promo" || request.GroupID == nil || *request.GroupID != groupID {
		t.Fatalf("renamed Sub2API create target = %+v", request)
	}
	fence := request.RecoveryFence
	if fence == nil || fence.UpstreamGroupID != renamedGeneration.ID ||
		fence.PresenceGeneration != renamedGeneration.PresenceGeneration ||
		fence.CanonicalGroupName != renamedGeneration.CanonicalGroupName {
		t.Fatalf("renamed Sub2API recovery fence = %#v, generation=%+v", fence, renamedGeneration)
	}
}

func TestManagedKeyReconcilePreservesAbsentStaleProvisioningNonce(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "reconcile-stale-provisioning")
	nonce := upstreamProvisioningKeyPrefix + "0123456789abcdef"
	startedAt := time.Now().UTC().Add(-upstreamKeyProvisioningStaleAfter - time.Minute)
	if _, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "promo", nonce, 1, startedAt, upstreamKeyProvisioningStaleAfter); err != nil || !claimed || !fresh {
		t.Fatalf("seed stale provisioning claim: claimed=%v fresh=%v err=%v", claimed, fresh, err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	failures, err := svc.reconcileManagedUpstreamKeysForSync(context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision,
		map[string]struct{}{"promo": {}}, map[string]string{"promo": "promo"}, nil)
	if err != nil || len(failures) != 0 {
		t.Fatalf("reconcile stale provisioning claim: failures=%+v err=%v", failures, err)
	}
	pending, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !pending.Provisioning || pending.UpstreamKeyID != 0 || pending.UpstreamKeyName != nonce {
		t.Fatalf("preserved stale provisioning claim = %+v, err=%v", pending, err)
	}
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || plain == "" || len(keys.created) != 1 || keys.created[0].Name != nonce {
		t.Fatalf("same-nonce retry after inventory absence = %q, created=%+v, err=%v", plain, keys.created, err)
	}
}

func TestManagedKeyReconcilePreservesFreshAndManualProvisioningClaims(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "reconcile-preserve-provisioning")
	freshNonce := upstreamProvisioningKeyPrefix + "fedcba9876543210"
	startedAt := time.Now().UTC()
	if _, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "promo", freshNonce, 1, startedAt, upstreamKeyProvisioningStaleAfter); err != nil || !claimed || !fresh {
		t.Fatalf("seed fresh provisioning claim: claimed=%v fresh=%v err=%v", claimed, fresh, err)
	}
	keys := &fakeKeyManager{listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
	}}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	if failures, err := svc.reconcileManagedUpstreamKeysForSync(context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision,
		map[string]struct{}{"promo": {}}, map[string]string{"promo": "promo"}, nil); err != nil || len(failures) != 0 {
		t.Fatalf("reconcile fresh provisioning claim: failures=%+v err=%v", failures, err)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !stored.Provisioning || stored.UpstreamKeyName != freshNonce {
		t.Fatalf("fresh provisioning claim changed = %+v, err=%v", stored, err)
	}

	manualStartedAt := time.Now().UTC().Add(-upstreamKeyProvisioningStaleAfter - time.Minute)
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1, UpstreamKeyName: "operator-managed-key",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceProvisioningClaim,
		Provisioning:        true, ProvisioningStartedAt: &manualStartedAt, Enabled: false,
	}); err != nil {
		t.Fatalf("seed manual provisioning claim: %v", err)
	}
	if failures, err := svc.reconcileManagedUpstreamKeysForSync(context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision,
		map[string]struct{}{"promo": {}}, map[string]string{"promo": "promo"}, nil); err != nil || len(failures) != 0 {
		t.Fatalf("reconcile manual provisioning claim: failures=%+v err=%v", failures, err)
	}
	stored, err = store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !stored.Provisioning || stored.UpstreamKeyName != "operator-managed-key" {
		t.Fatalf("manual provisioning claim changed = %+v, err=%v", stored, err)
	}
}

func TestManagedKeyReconcileRecoversRemoteStaleProvisioningClaim(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "reconcile-remote-provisioning")
	nonce := upstreamProvisioningKeyPrefix + "0011223344556677"
	startedAt := time.Now().UTC().Add(-upstreamKeyProvisioningStaleAfter - time.Minute)
	if _, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "promo", nonce, 1, startedAt, upstreamKeyProvisioningStaleAfter); err != nil || !claimed || !fresh {
		t.Fatalf("seed stale provisioning claim: claimed=%v fresh=%v err=%v", claimed, fresh, err)
	}
	remote := managedUpstreamKeyFixture(91, nonce, "promo")
	keys := &fakeKeyManager{
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Items: []connector.APIKey{remote}, Page: 1, Pages: 1, Total: 1}, nil
		},
		revealedKeys: map[int64]string{remote.ID: "sk-recovered-stale"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	failures, err := svc.reconcileManagedUpstreamKeysForSync(context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision,
		map[string]struct{}{"promo": {}}, map[string]string{"promo": "promo"}, nil)
	if err != nil || len(failures) != 0 {
		t.Fatalf("reconcile remote provisioning claim: failures=%+v err=%v", failures, err)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || stored.Provisioning || !stored.Enabled || stored.UpstreamKeyID != remote.ID || stored.UpstreamKeyName != nonce || stored.APIKeyCipher == "" {
		t.Fatalf("recovered remote provisioning claim = %+v, err=%v", stored, err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("remote provisioning recovery created duplicate keys = %+v", keys.created)
	}
}

func TestManagedKeyReconcileRepairsDuplicateStrongProvisioningName(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "reconcile-ambiguous-provisioning")
	nonce := upstreamProvisioningKeyPrefix + "aabbccddeeff0011"
	startedAt := time.Now().UTC().Add(-upstreamKeyProvisioningStaleAfter - time.Minute)
	if _, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "promo", nonce, 1, startedAt, upstreamKeyProvisioningStaleAfter); err != nil || !claimed || !fresh {
		t.Fatalf("seed stale provisioning claim: claimed=%v fresh=%v err=%v", claimed, fresh, err)
	}
	first := managedUpstreamKeyFixture(101, nonce, "promo")
	second := managedUpstreamKeyFixture(102, nonce, "promo")
	keys := &fakeKeyManager{
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Items: []connector.APIKey{first, second}, Page: 1, Pages: 1, Total: 2}, nil
		},
		revealedKeys: map[int64]string{first.ID: "sk-recovered-duplicate"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	failures, err := svc.reconcileManagedUpstreamKeysForSync(context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision,
		map[string]struct{}{"promo": {}}, map[string]string{"promo": "promo"}, nil)
	if err != nil || len(failures) != 0 {
		t.Fatalf("duplicate provisioning reconcile: failures=%+v err=%v", failures, err)
	}
	stored, findErr := store.FindUpstreamKeyAny(channel.ID, "promo")
	if findErr != nil || stored.Provisioning || !stored.Enabled || stored.UpstreamKeyID != first.ID || stored.UpstreamKeyName != nonce || stored.APIKeyCipher == "" {
		t.Fatalf("duplicate provisioning claim was not repaired = %+v, err=%v", stored, findErr)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != second.ID || len(keys.created) != 0 {
		t.Fatalf("duplicate provisioning repair mutations: deleted=%v created=%v revealed=%v", keys.deleted, keys.created, keys.revealCalls)
	}
}

func TestManagedKeyReconcileResetIsIdentityFenced(t *testing.T) {
	db, channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureWithDB(t, "reconcile-provisioning-identity")
	nonce := upstreamProvisioningKeyPrefix + "8899aabbccddeeff"
	startedAt := time.Now().UTC().Add(-upstreamKeyProvisioningStaleAfter - time.Minute)
	if _, claimed, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "promo", nonce, 1, startedAt, upstreamKeyProvisioningStaleAfter); err != nil || !claimed || !fresh {
		t.Fatalf("seed stale provisioning claim: claimed=%v fresh=%v err=%v", claimed, fresh, err)
	}
	_ = channels
	_ = rates
	_ = cipher
	if err := db.Model(&storage.Channel{}).Where("id = ?", channel.ID).
		Update("upstream_account_identity_revision", channel.UpstreamAccountIdentityRevision+1).Error; err != nil {
		t.Fatalf("advance identity revision: %v", err)
	}
	err := store.ResetUpstreamKeyAfterManagedReconcile(channel.ID, channel.UpstreamAccountIdentityRevision, "promo", "must not reset")
	if !errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("identity-fenced provisioning reconcile error = %v", err)
	}
	stored, findErr := store.FindUpstreamKeyAny(channel.ID, "promo")
	if findErr != nil || !stored.Provisioning || stored.UpstreamKeyName != nonce {
		t.Fatalf("identity-fenced provisioning claim changed = %+v, err=%v", stored, findErr)
	}
}

func TestEnsureUpstreamKeyRecoversExistingProvisionedName(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("recover-provisioned-name-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "recover-provisioned", Type: storage.ChannelTypeNewAPI, SiteURL: "https://recover-provisioned.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	managedName := upstreamProvisioningKeyPrefix + "0123456789abcdef"
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7,
		UpstreamKeyName: managedName, Enabled: false, LastError: "upstream key invalidated",
	}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{{
				ID: 7, Name: managedName, GroupName: "promo", Status: "active", UnlimitedQuota: true, ExpiredTime: -1, CrossGroupRetry: true,
			}}}, nil
		},
		revealedKeys: map[int64]string{7: "sk-recovered-existing"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || plain != "sk-recovered-existing" {
		t.Fatalf("recover existing provisioned key = %q, %v", plain, err)
	}
	if len(keys.created) != 0 {
		t.Fatalf("create calls = %d, want 0", len(keys.created))
	}
}

func TestEnsureUpstreamKeyRejectsModifiedCachedManagedKey(t *testing.T) {
	for _, tc := range []struct {
		name   string
		want   string
		mutate func(*connector.APIKey)
	}{
		{name: "remain quota", want: "creation fingerprint changed", mutate: func(key *connector.APIKey) { key.Quota = 1 }},
		{name: "unlimited quota", want: "creation fingerprint changed", mutate: func(key *connector.APIKey) { key.UnlimitedQuota = false; key.Quota = 1 }},
		{name: "expiry", want: "creation fingerprint changed", mutate: func(key *connector.APIKey) { key.ExpiredTime = time.Now().Add(time.Hour).Unix() }},
		{name: "allow ips", want: "creation fingerprint changed", mutate: func(key *connector.APIKey) { key.AllowIPs = "127.0.0.1" }},
		{name: "model limits enabled", want: "creation fingerprint changed", mutate: func(key *connector.APIKey) { key.ModelLimitsEnabled = true }},
		{name: "model limits", want: "creation fingerprint changed", mutate: func(key *connector.APIKey) { key.ModelLimits = "gpt-5" }},
		{name: "name", want: "creation fingerprint changed", mutate: func(key *connector.APIKey) { key.Name = "BOG - manually-renamed" }},
		{name: "id", want: "unexpected remote id", mutate: func(key *connector.APIKey) { key.ID = 8 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "modified-"+tc.name)
			expectedName := upstreamKeyNameForGroup("promo")
			if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
				ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7, UpstreamKeyName: expectedName,
				Enabled: false, LastError: "previous provisioning failed",
			}); err != nil {
				t.Fatal(err)
			}
			remote := managedUpstreamKeyFixture(7, expectedName, "promo")
			tc.mutate(&remote)
			keys := &fakeKeyManager{
				groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
				listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
					return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
				},
				revealedKeys: map[int64]string{7: "sk-manual"},
			}
			svc := NewService(channels, rates, store, cipher, keys, nil)
			plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
			if err == nil || plain != "" || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("modified managed key recovery = %q, %v", plain, err)
			}
			if len(keys.revealCalls) != 0 || len(keys.created) != 0 {
				t.Fatalf("modified managed key revealed or recreated: reveals=%v creates=%d", keys.revealCalls, len(keys.created))
			}
		})
	}
}

func TestEnsureUpstreamKeyRecoversCachedNewAPIKeyWithOmittedFingerprintFields(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "newapi-omitted-fields")
	expectedName := upstreamKeyNameForGroup("promo")
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7, UpstreamKeyName: expectedName,
		Enabled: false, LastError: "upstream key invalidated",
	}); err != nil {
		t.Fatal(err)
	}
	remote := connector.APIKey{
		ID: 7, Name: expectedName, Status: "active", Group: "promo", GroupName: "promo",
		Source: connector.APIKeySourceNewAPI,
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
		},
		revealedKeys: map[int64]string{7: "sk-newapi-omitted"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || plain != "sk-newapi-omitted" {
		t.Fatalf("recover omitted-field newapi key = %q, %v", plain, err)
	}
	if len(keys.created) != 0 || len(keys.revealCalls) != 1 || keys.revealCalls[0] != 7 {
		t.Fatalf("unexpected recovery calls: creates=%d reveals=%v", len(keys.created), keys.revealCalls)
	}
}

func TestEnsureUnboundOrdinaryProvisioningNameRequiresCompleteFingerprint(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "ordinary-provisioning-name")
	expectedName := upstreamKeyNameForGroup("promo")
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyName: expectedName,
		Provisioning: true, Enabled: false,
	}); err != nil {
		t.Fatal(err)
	}
	remote := connector.APIKey{
		ID: 7, Name: expectedName, Status: "active", Group: "promo", GroupName: "promo",
		Source: connector.APIKeySourceNewAPI,
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
		},
		revealedKeys: map[int64]string{7: "sk-unknown-fingerprint"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "incompatible remote records") {
		t.Fatalf("ordinary provisioning recovery = %q, %v", plain, err)
	}
	if len(keys.revealCalls) != 0 || len(keys.created) != 0 {
		t.Fatalf("ordinary incomplete key was revealed or recreated: reveals=%v creates=%d", keys.revealCalls, len(keys.created))
	}
}

func TestEnsureUpstreamKeyStillRejectsExplicitNewAPIFingerprintChange(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "newapi-explicit-change")
	expectedName := upstreamKeyNameForGroup("promo")
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7, UpstreamKeyName: expectedName,
		Enabled: false, LastError: "upstream key invalidated",
	}); err != nil {
		t.Fatal(err)
	}
	remote := connector.APIKey{
		ID: 7, Name: expectedName, Status: "active", Group: "promo", GroupName: "promo",
		Source:         connector.APIKeySourceNewAPI,
		ObservedFields: connector.APIKeyObservedFields{UnlimitedQuota: true},
		UnlimitedQuota: false,
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
		},
		revealedKeys: map[int64]string{7: "sk-newapi-changed"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "identity or creation fingerprint changed") {
		t.Fatalf("explicitly changed newapi key = %q, %v", plain, err)
	}
	if len(keys.revealCalls) != 0 || len(keys.created) != 0 {
		t.Fatalf("changed key was revealed or recreated: reveals=%v creates=%d", keys.revealCalls, len(keys.created))
	}
}

func TestEnsureUpstreamKeyRejectsExplicitNewAPINullFingerprint(t *testing.T) {
	key := managedUpstreamKeyFixture(7, upstreamKeyNameForGroup("promo"), "promo")
	key.NullFields.ModelLimitsEnabled = true
	if recoverableManagedUpstreamKey(key, "promo", nil, true) {
		t.Fatal("explicit null NewAPI creation field was treated as omitted")
	}
}

func TestEnsureUpstreamKeyDoesNotAdoptUnboundNewAPIKeyWithOmittedFingerprint(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "newapi-unbound-omitted")
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", Enabled: false, LastError: "previous upstream key failed",
	}); err != nil {
		t.Fatal(err)
	}
	remote := connector.APIKey{
		ID: 9, Name: upstreamKeyNameForGroup("promo"), Status: "active", Group: "promo", GroupName: "promo",
		Source: connector.APIKeySourceNewAPI,
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
		},
		revealedKeys: map[int64]string{9: "sk-unbound"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "incompatible creation fingerprint") {
		t.Fatalf("unbound omitted-field key = %q, %v", plain, err)
	}
	if len(keys.revealCalls) != 0 || len(keys.created) != 0 {
		t.Fatalf("unbound key was revealed or recreated: reveals=%v creates=%d", keys.revealCalls, len(keys.created))
	}
}

func TestEnsureUpstreamKeyAdoptsSub2APIManagedKeyUsingSub2Fingerprint(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForType(t, "sub2api-recovery", storage.ChannelTypeSub2API)
	groupID := int64(3)
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", Enabled: false, LastError: "existing upstream key missing locally",
	}); err != nil {
		t.Fatal(err)
	}
	remote := managedSub2APIKeyFixture(11, upstreamKeyNameForGroup("promo"), "promo")
	remote.GroupID = &groupID
	remote.GroupName = ""
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{ID: &groupID, Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
		},
		revealedKeys: map[int64]string{11: "sk-sub2api-existing"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", &groupID, 1)
	if err != nil || plain != "sk-sub2api-existing" {
		t.Fatalf("adopt sub2api key = %q, %v", plain, err)
	}
	if len(keys.created) != 0 || len(keys.revealCalls) != 1 || keys.revealCalls[0] != 11 {
		t.Fatalf("unexpected sub2api recovery calls: creates=%d reveals=%v", len(keys.created), keys.revealCalls)
	}
}

func TestEnsureUpstreamKeyDoesNotAdoptUnboundSub2APIKeyWithMissingFingerprint(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForType(t, "sub2api-missing-fingerprint", storage.ChannelTypeSub2API)
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", Enabled: false, LastError: "existing upstream key missing locally",
	}); err != nil {
		t.Fatal(err)
	}
	remote := managedSub2APIKeyFixture(11, upstreamKeyNameForGroup("promo"), "promo")
	remote.ObservedFields = connector.APIKeyObservedFields{}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
		},
		revealedKeys: map[int64]string{11: "sk-sub2api-missing"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "incompatible creation fingerprint") {
		t.Fatalf("missing-field sub2api key = %q, %v", plain, err)
	}
	if len(keys.revealCalls) != 0 || len(keys.created) != 0 {
		t.Fatalf("missing-field sub2api key was revealed or recreated: reveals=%v creates=%d", keys.revealCalls, len(keys.created))
	}
}

func TestEnsureUpstreamKeyRequiresActiveSub2APIStatus(t *testing.T) {
	for _, status := range []string{"", "disabled", "quota_exhausted", "expired"} {
		t.Run(status, func(t *testing.T) {
			key := managedSub2APIKeyFixture(11, upstreamKeyNameForGroup("promo"), "promo")
			key.Status = status
			if recoverableManagedUpstreamKey(key, "promo", nil, false) {
				t.Fatalf("sub2api status %q was treated as recoverable", status)
			}
		})
	}
}

func TestEnsureUpstreamKeyRejectsModifiedSub2APIFingerprint(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*connector.APIKey)
	}{
		{name: "quota", mutate: func(key *connector.APIKey) { key.Quota = 1 }},
		{name: "ip", mutate: func(key *connector.APIKey) { key.IPWhitelist = []string{"127.0.0.1"} }},
		{name: "rate", mutate: func(key *connector.APIKey) { key.RateLimit5h = 1 }},
		{name: "expiry", mutate: func(key *connector.APIKey) { expires := time.Now().Add(time.Hour); key.ExpiresAt = &expires }},
	} {
		t.Run(test.name, func(t *testing.T) {
			channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForType(t, "sub2api-modified-"+test.name, storage.ChannelTypeSub2API)
			if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
				ChannelID: channel.ID, GroupName: "promo", Enabled: false, LastError: "existing upstream key missing locally",
			}); err != nil {
				t.Fatal(err)
			}
			remote := managedSub2APIKeyFixture(11, upstreamKeyNameForGroup("promo"), "promo")
			test.mutate(&remote)
			keys := &fakeKeyManager{
				groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
				listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
					return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
				},
				revealedKeys: map[int64]string{11: "sk-sub2api-modified"},
			}
			svc := NewService(channels, rates, store, cipher, keys, nil)
			plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
			if err == nil || plain != "" || !strings.Contains(err.Error(), "incompatible creation fingerprint") {
				t.Fatalf("modified sub2api key = %q, %v", plain, err)
			}
			if len(keys.revealCalls) != 0 || len(keys.created) != 0 {
				t.Fatalf("modified sub2api key was revealed or recreated: reveals=%v creates=%d", keys.revealCalls, len(keys.created))
			}
		})
	}
}

func TestEnsureUpstreamKeyRejectsModifiedProvisionedKeyBeforeReveal(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "modified-provisioned")
	pendingName := upstreamProvisioningKeyPrefix + "fedcba9876543210"
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7, UpstreamKeyName: pendingName,
		Provisioning: true, Enabled: false,
	}); err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(7, pendingName, "promo")
	remote.AllowIPs = "127.0.0.1"
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
		},
		revealedKeys: map[int64]string{7: "sk-manual"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "identity or creation fingerprint changed") {
		t.Fatalf("modified provisioned key recovery = %q, %v", plain, err)
	}
	if len(keys.revealCalls) != 0 || len(keys.created) != 0 {
		t.Fatalf("modified provisioned key revealed or recreated: reveals=%v creates=%d", keys.revealCalls, len(keys.created))
	}
}

func TestManagedProvisioningKeyNameValidation(t *testing.T) {
	for _, name := range []string{
		upstreamProvisioningKeyPrefix + "0123456789abcdef",
		upstreamProvisioningKeyPrefix + "deadbeefdeadbeef",
	} {
		if !isManagedUpstreamKeyName(name) || !isExpectedManagedUpstreamKeyName(name, "promo") {
			t.Fatalf("valid provisioning name %q was rejected", name)
		}
	}
	for _, name := range []string{
		upstreamProvisioningKeyPrefix,
		upstreamProvisioningKeyPrefix + "pending",
		upstreamProvisioningKeyPrefix + "0123456789abcde",
		upstreamProvisioningKeyPrefix + "0123456789abcdef0",
		upstreamProvisioningKeyPrefix + "0123456789abcdeF",
		upstreamProvisioningKeyPrefix + "0123456789abcdeg",
	} {
		if isManagedUpstreamKeyName(name) || isExpectedManagedUpstreamKeyName(name, "promo") {
			t.Fatalf("invalid provisioning name %q was accepted", name)
		}
	}
}

func TestEnsureUpstreamKeyRejectsIncompatibleManagedRecoveryCandidate(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "incompatible-candidate")
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", Enabled: false, LastError: "previous provisioning failed",
	}); err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(9, upstreamKeyNameForGroup("promo"), "promo")
	remote.ModelLimitsEnabled = true
	remote.ModelLimits = "gpt-5"
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{remote}}, nil
		},
		revealedKeys: map[int64]string{9: "sk-manual"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err == nil || plain != "" || !strings.Contains(err.Error(), "incompatible creation fingerprint") {
		t.Fatalf("incompatible recovery candidate = %q, %v", plain, err)
	}
	if len(keys.revealCalls) != 0 || len(keys.created) != 0 {
		t.Fatalf("incompatible recovery candidate revealed or recreated: reveals=%v creates=%d", keys.revealCalls, len(keys.created))
	}
}

func TestEnsureUpstreamKeyDoesNotAdoptManualKeyAtRemoteLimit(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("manual-key-limit-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "manual-limit", Type: storage.ChannelTypeNewAPI, SiteURL: "https://manual.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups:    []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		createErr: errors.New("newapi create api key: 已达到最大令牌数量限制 (20)"),
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{{
				ID: 99, Name: "BOG - promo", GroupName: "promo", Status: "active", UnlimitedQuota: true,
			}}}, nil
		},
		revealedKeys: map[int64]string{99: "sk-manual"},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	if plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err == nil || plain != "" {
		t.Fatalf("manual key was adopted at limit: plain=%q err=%v", plain, err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("create calls = %d, want 1", len(keys.created))
	}
	svc = NewService(channels, rates, store, cipher, keys, nil)
	if plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err == nil || plain != "" {
		t.Fatalf("second ensure after remote limit = %q, %v", plain, err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("create calls after failed recovery = %d, want 1", len(keys.created))
	}
}

func TestEnsureUpstreamKeyRejectsCreateWithoutActualKey(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("missing-actual-key-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "missing-key", Type: storage.ChannelTypeNewAPI, SiteURL: "https://missing-key.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups:       []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		createResult: &connector.APIKey{ID: 1},
		revealedKeys: map[int64]string{},
	}
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		if len(keys.created) == 0 {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		}
		req := keys.created[0]
		return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{{
			ID: 1, Name: req.Name, Group: req.Group, GroupName: req.Group, GroupID: req.GroupID,
			Status: "active", UnlimitedQuota: true, ExpiredTime: -1, CrossGroupRetry: true,
		}}}, nil
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	if key, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err == nil || key != "" {
		t.Fatalf("EnsureUpstreamAPIKeyForGroup = %q, %v; want actual-key error", key, err)
	}
	pending, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatalf("find provisioning row: %v", err)
	}
	if !pending.Provisioning || pending.Enabled || pending.UpstreamKeyID != 1 || pending.APIKeyCipher != "" || !strings.HasPrefix(pending.UpstreamKeyName, "BOG-p-") {
		t.Fatalf("provisioning row = %+v", pending)
	}
	if key, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err == nil || key != "" {
		t.Fatalf("second ensure = %q, %v; want reveal error", key, err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("create calls after reveal failure = %d, want 1", len(keys.created))
	}
	keys.revealedKeys[1] = "upstream-real-key"
	if key, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err != nil || key != "upstream-real-key" {
		t.Fatalf("recovered ensure = %q, %v", key, err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("create calls after recovery = %d, want 1", len(keys.created))
	}
	recovered, err := store.FindUpstreamKey(channel.ID, "promo")
	if err != nil || recovered.Provisioning || !recovered.Enabled || recovered.UpstreamKeyID != 1 || recovered.UpstreamKeyName != pending.UpstreamKeyName {
		t.Fatalf("recovered row = %+v, err=%v", recovered, err)
	}
}

func TestRevealExistingUpstreamKeyFallsBackToUnfilteredList(t *testing.T) {
	var searches []string
	keys := &fakeKeyManager{
		revealedKeys: map[int64]string{42: "sk-existing-real"},
		listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			searches = append(searches, query.Search)
			if query.Search != "" {
				return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
			}
			return &connector.APIKeyPage{Page: 1, Pages: 1, Items: []connector.APIKey{{
				ID: 42, Name: "BridgeOps Gateway - default", GroupName: "default", Status: "active", UnlimitedQuota: true, ExpiredTime: -1, CrossGroupRetry: true,
			}}}, nil
		},
	}
	svc := &Service{Keys: keys}
	item, plain, err := svc.revealExistingUpstreamKey(context.Background(), 7, "default", nil)
	if err != nil {
		t.Fatalf("reveal existing key: %v", err)
	}
	if item.ID != 42 || plain != "sk-existing-real" {
		t.Fatalf("item/plain = %+v/%q", item, plain)
	}
	if len(searches) != 4 || searches[0] != "BOG" || searches[1] != "BridgeOps Gateway" || searches[2] != "UpstreamOps Gateway" || searches[3] != "" {
		t.Fatalf("search fallback sequence = %#v", searches)
	}
}

func TestRevealExistingUpstreamKeyStopsAtSafeLookupLimit(t *testing.T) {
	keys := &fakeKeyManager{listAPIKeys: func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		if query.Search != "" {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		}
		items := make([]connector.APIKey, upstreamKeyLookupPageSize)
		for index := range items {
			items[index] = connector.APIKey{ID: int64((query.Page-1)*upstreamKeyLookupPageSize + index + 1), Name: "unrelated", Status: "active", UnlimitedQuota: true}
		}
		if query.Page == 1 {
			items[0] = managedUpstreamKeyFixture(1, upstreamKeyNameForGroup("default"), "default")
		}
		return &connector.APIKeyPage{Page: query.Page, Pages: upstreamKeyLookupMaxPages + 1, Items: items}, nil
	}}
	svc := &Service{Keys: keys}
	_, _, err := svc.revealExistingUpstreamKey(context.Background(), 7, "default", nil)
	if err == nil || errors.Is(err, gorm.ErrRecordNotFound) || !strings.Contains(err.Error(), "safe lookup limit") {
		t.Fatalf("truncated lookup error = %v", err)
	}
}

func TestAPIKeyPageCompleteRequiresConsistentMetadata(t *testing.T) {
	for _, test := range []struct {
		name string
		page *connector.APIKeyPage
		want bool
	}{
		{name: "consistent final page", page: &connector.APIKeyPage{Page: 3, Pages: 3, Total: 101, PageSize: 50, Items: []connector.APIKey{{}}}, want: true},
		{name: "pages understate total", page: &connector.APIKeyPage{Page: 1, Pages: 1, Total: 101, PageSize: 50}, want: false},
		{name: "stale response page", page: &connector.APIKeyPage{Page: 1, Pages: 1, Total: 1, PageSize: 50}, want: false},
		{name: "full page without metadata", page: &connector.APIKeyPage{Page: 1, Items: make([]connector.APIKey, 50)}, want: false},
		{name: "short page without metadata", page: &connector.APIKeyPage{Page: 1, Items: []connector.APIKey{{}}}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			requestedPage := 1
			if test.name == "stale response page" {
				requestedPage = 2
			}
			if got := apiKeyPageComplete(test.page, requestedPage, 50); got != test.want {
				t.Fatalf("complete = %v, want %v", got, test.want)
			}
		})
	}
}

func TestEnsureUpstreamKeyDoesNotScanUnrelatedRemoteKeys(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "truncated", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "default", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "default", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			t.Fatal("creating a new upstream key must not scan unrelated remote keys")
			return nil, nil
		},
	}
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, rates, store, cipher, keys, nil)
	if key, err := service.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "default", nil, 1); err != nil || key == "" {
		t.Fatalf("ensure upstream key = %q, %v", key, err)
	}
	if len(keys.created) != 1 {
		t.Fatalf("created upstream keys = %+v", keys.created)
	}
}

func TestReconcilePendingCostRetriesThenAbandonsDuplicateUsage(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now()
	owner := &storage.GatewayRequestLog{ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: now}
	pending := &storage.GatewayRequestLog{ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.2, CostEstimated: true, UpstreamRequestID: "request-2", CostReconcileTries: 5, CreatedAt: now.Add(time.Second)}
	for _, log := range []*storage.GatewayRequestLog{owner, pending} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	if err := store.UpdateRequestLogActualCost(owner.ID, "usage-1", 0.01, "upstream", 0, 0, false); err != nil {
		t.Fatalf("claim owner usage: %v", err)
	}
	if err := store.ScheduleRequestCostReconcile(pending.ID, now); err != nil {
		t.Fatalf("schedule pending reconcile: %v", err)
	}
	var pendingBefore storage.GatewayRequestLog
	if err := db.First(&pendingBefore, pending.ID).Error; err != nil {
		t.Fatalf("read pending request before conflicts: %v", err)
	}
	keys := &fakeKeyManager{requestCost: &connector.RequestCostResult{UsageID: "usage-1", Cost: 0.02, Source: "upstream"}}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, keys, nil)
	for conflict := 1; conflict <= maxUsageConflictRetries; conflict++ {
		var current storage.GatewayRequestLog
		if err := db.First(&current, pending.ID).Error; err != nil {
			t.Fatalf("read conflict %d request: %v", conflict, err)
		}
		svc.reconcilePendingCost(current, now, now.Add(time.Second))
		var updated storage.GatewayRequestLog
		if err := db.First(&updated, pending.ID).Error; err != nil {
			t.Fatalf("read conflict %d result: %v", conflict, err)
		}
		if updated.CostReconcileConflicts != conflict || updated.CostReconcileTries != 5+conflict {
			t.Fatalf("conflict %d counters = %+v", conflict, updated)
		}
		if conflict < maxUsageConflictRetries && updated.CostReconcileAt == nil {
			t.Fatalf("conflict %d should remain scheduled: %+v", conflict, updated)
		}
	}
	var abandoned storage.GatewayRequestLog
	if err := db.First(&abandoned, pending.ID).Error; err != nil {
		t.Fatalf("read abandoned request: %v", err)
	}
	if abandoned.CostReconcileAt != nil || abandoned.UpstreamUsageID != nil || !strings.Contains(abandoned.CostReconcileError, "already assigned") {
		t.Fatalf("terminal duplicate was not preserved: %+v", abandoned)
	}
	sameOptionalFloat := func(left, right *float64) bool {
		return left == nil && right == nil || left != nil && right != nil && *left == *right
	}
	if abandoned.Cost != pendingBefore.Cost || abandoned.CostEstimated != pendingBefore.CostEstimated ||
		abandoned.CostSource != pendingBefore.CostSource || abandoned.CostStatus != pendingBefore.CostStatus ||
		abandoned.CostUnavailableReason != pendingBefore.CostUnavailableReason ||
		!sameOptionalFloat(abandoned.RawCost, pendingBefore.RawCost) ||
		!sameOptionalFloat(abandoned.FundingFactor, pendingBefore.FundingFactor) ||
		abandoned.CostBasisVersion != pendingBefore.CostBasisVersion || abandoned.CostBasisSource != pendingBefore.CostBasisSource {
		t.Fatalf("terminal duplicate changed accounting fields: before=%+v after=%+v", pendingBefore, abandoned)
	}
}

func TestCostDispatcherKeepsDueOrderAndWakesAfterCompletion(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now().UTC()
	oldest := &storage.GatewayRequestLog{
		ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		UpstreamRequestID: "oldest", CreatedAt: now.Add(-2 * time.Second),
	}
	newer := &storage.GatewayRequestLog{
		ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		UpstreamRequestID: "newer", CreatedAt: now.Add(-time.Second),
	}
	for _, log := range []*storage.GatewayRequestLog{oldest, newer} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}
	if err := store.ScheduleRequestCostReconcile(oldest.ID, now.Add(-2*time.Second)); err != nil {
		t.Fatalf("schedule oldest: %v", err)
	}
	if err := store.ScheduleRequestCostReconcile(newer.ID, now.Add(-time.Second)); err != nil {
		t.Fatalf("schedule newer: %v", err)
	}

	manager := &orderedCostManager{
		calls:        make(chan string, 2),
		releaseFirst: make(chan struct{}),
	}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, manager, nil)
	svc.costLookupSlots = make(chan struct{}, 1)
	svc.dispatchPendingCosts()

	select {
	case requestID := <-manager.calls:
		if requestID != "oldest" {
			t.Fatalf("first lookup = %q, want oldest", requestID)
		}
	case <-time.After(time.Second):
		t.Fatal("oldest lookup did not start")
	}
	// A dispatcher pass while the only slot is occupied must not claim or run
	// the newer item ahead of the persisted oldest request.
	svc.dispatchPendingCosts()
	select {
	case requestID := <-manager.calls:
		t.Fatalf("lookup %q started while the only slot was occupied", requestID)
	default:
	}

	close(manager.releaseFirst)
	select {
	case <-svc.costWorkerWake:
	case <-time.After(time.Second):
		t.Fatal("completed lookup did not wake the dispatcher")
	}
	svc.dispatchPendingCosts()
	select {
	case requestID := <-manager.calls:
		if requestID != "newer" {
			t.Fatalf("second lookup = %q, want newer", requestID)
		}
	case <-time.After(time.Second):
		t.Fatal("newer lookup did not fill the released slot")
	}
	select {
	case <-svc.costWorkerWake:
	case <-time.After(time.Second):
		t.Fatal("newer lookup did not complete")
	}
}

func TestReconcileRequestCostDoesNotScheduleUnmatchableRequest(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now()
	log := &storage.GatewayRequestLog{
		ChannelID:       2,
		Method:          "POST",
		Path:            "/v1/responses",
		StatusCode:      200,
		CostReconcileAt: timePtr(now),
		CreatedAt:       now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	keys := &fakeKeyManager{}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, keys, nil)
	svc.ReconcileRequestCost(log, now.Add(-time.Second), now)

	var stored storage.GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read request log: %v", err)
	}
	if stored.CostReconcileAt != nil {
		t.Fatalf("unmatchable request remained scheduled: %+v", stored)
	}
	if !strings.Contains(stored.CostReconcileError, "cannot be matched") {
		t.Fatalf("unmatchable reason = %q", stored.CostReconcileError)
	}
	if keys.requestCostCalls != 0 {
		t.Fatalf("unmatchable request performed %d upstream lookups", keys.requestCostCalls)
	}
	pending, err := store.ListPendingRequestCostReconciles(10, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("list pending reconciles: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("unmatchable request is still pending: %+v", pending)
	}
}

func TestReconcilePendingCostRetryStoresSanitizedReason(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now()
	log := &storage.GatewayRequestLog{
		ChannelID:       2,
		Method:          "POST",
		Path:            "/v1/responses",
		StatusCode:      200,
		PromptTokens:    10,
		CostReconcileAt: timePtr(now),
		CreatedAt:       now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	keys := &fakeKeyManager{requestCostError: errors.New("lookup failed Authorization: Bearer abc123; x-api-key=short-key; token=mini-secret; Authorization: Basic dXNlcjpw")}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, keys, nil)
	svc.reconcilePendingCost(*log, now.Add(-time.Second), now)

	var stored storage.GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read request log: %v", err)
	}
	if stored.CostReconcileAt == nil || stored.CostReconcileTries != 1 {
		t.Fatalf("retry state = %+v", stored)
	}
	if !strings.Contains(stored.CostReconcileError, "lookup failed") {
		t.Fatalf("retry reason = %q", stored.CostReconcileError)
	}
	for _, secret := range []string{"abc123", "short-key", "mini-secret", "dXNlcjpw", "Authorization", "x-api-key"} {
		if strings.Contains(stored.CostReconcileError, secret) {
			t.Fatalf("retry reason retained %q: %q", secret, stored.CostReconcileError)
		}
	}
	if !strings.Contains(stored.CostReconcileError, "[redacted") {
		t.Fatalf("retry reason was not sanitized: %q", stored.CostReconcileError)
	}
}

func TestSanitizeGatewayServiceErrorRedactsShortCredentials(t *testing.T) {
	for _, test := range []struct {
		name      string
		input     string
		secret    string
		sensitive []string
	}{
		{name: "bearer header", input: "lookup failed Authorization: Bearer abc123", secret: "abc123"},
		{name: "api key header", input: "lookup failed x-api-key=short-key", secret: "short-key"},
		{name: "token assignment", input: "lookup failed token=mini-secret", secret: "mini-secret"},
		{name: "basic header", input: "lookup failed Authorization: Basic dXNlcjpw", secret: "dXNlcjpw"},
		{name: "known material", input: "lookup failed opaque", secret: "opaque", sensitive: []string{"opaque"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := sanitizeGatewayServiceError(test.input, test.sensitive...)
			if strings.Contains(got, test.secret) || !strings.Contains(got, "[redacted") {
				t.Fatalf("sanitizeGatewayServiceError(%q) = %q", test.input, got)
			}
		})
	}
}

func TestReconcilePendingCostNilResultStoresReason(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now()
	log := &storage.GatewayRequestLog{ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 10, CostReconcileAt: timePtr(now), CreatedAt: now}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, &fakeKeyManager{}, nil)
	svc.reconcilePendingCost(*log, now.Add(-time.Second), now)

	var stored storage.GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read request log: %v", err)
	}
	if stored.CostReconcileAt == nil || !strings.Contains(stored.CostReconcileError, "no result") {
		t.Fatalf("nil-result retry state = %+v", stored)
	}
}

func TestReconcilePendingCostWritesAuthoritativeUsageMetrics(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now()
	log := &storage.GatewayRequestLog{
		ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200,
		PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2,
		Cost: 0.2, CostEstimated: true, CostSource: "gateway_estimate",
		UpstreamRequestID: "request-authoritative", CostReconcileAt: timePtr(now), CreatedAt: now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	keys := &fakeKeyManager{requestCost: &connector.RequestCostResult{
		UsageID: "usage-authoritative", Cost: 0.1, Source: "sub2api_actual_cost",
		PromptTokens: 125, CompletionTokens: 30, TotalTokens: 155, HasTokenMetrics: true,
		CacheCreationTokens: 5, CacheReadTokens: 100, HasCacheTokenMetrics: true,
	}}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, keys, nil)
	svc.reconcilePendingCost(*log, now.Add(-time.Second), now)

	var stored storage.GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read reconciled request: %v", err)
	}
	if stored.CostStatus != storage.GatewayCostStatusReconciled || stored.Cost != 0.1 || stored.PromptTokens != 125 || stored.CompletionTokens != 30 || stored.TotalTokens != 155 || stored.CacheCreationTokens != 5 || stored.CacheReadTokens != 100 {
		t.Fatalf("reconciled request = %+v", stored)
	}
}

func TestReconcilePendingCostStopsAtRetryAndAgeLimits(t *testing.T) {
	tests := []struct {
		name       string
		createdAt  func(time.Time) time.Time
		tries      int
		wantReason string
		wantCalls  int
	}{
		{
			name:       "retry limit",
			createdAt:  func(now time.Time) time.Time { return now },
			tries:      maxCostReconcileAttempts - 1,
			wantReason: "lookup failed",
			wantCalls:  1,
		},
		{
			name:       "age limit",
			createdAt:  func(now time.Time) time.Time { return now.Add(-maxCostReconcileAge) },
			wantReason: "expired after 7 days",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := testdb.OpenWithError(t)
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			store := storage.NewGateway(db)
			now := time.Now()
			log := &storage.GatewayRequestLog{
				ChannelID:          2,
				Method:             "POST",
				Path:               "/v1/responses",
				StatusCode:         200,
				PromptTokens:       10,
				CostReconcileAt:    timePtr(now),
				CostReconcileTries: tt.tries,
				CreatedAt:          tt.createdAt(now),
			}
			if err := store.AppendRequestLog(log); err != nil {
				t.Fatalf("append request log: %v", err)
			}
			keys := &fakeKeyManager{requestCostError: errors.New("lookup failed")}
			svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, keys, nil)
			svc.reconcilePendingCost(*log, now.Add(-time.Second), now)

			var stored storage.GatewayRequestLog
			if err := db.First(&stored, log.ID).Error; err != nil {
				t.Fatalf("read request log: %v", err)
			}
			if stored.CostReconcileAt != nil || !strings.Contains(stored.CostReconcileError, tt.wantReason) {
				t.Fatalf("terminal retry state = %+v", stored)
			}
			if keys.requestCostCalls != tt.wantCalls {
				t.Fatalf("upstream lookup calls = %d, want %d", keys.requestCostCalls, tt.wantCalls)
			}
			pending, err := store.ListPendingRequestCostReconciles(10, now.Add(2*maxCostReconcileAge))
			if err != nil {
				t.Fatalf("list pending reconciles: %v", err)
			}
			if len(pending) != 0 {
				t.Fatalf("terminal request is still pending: %+v", pending)
			}
		})
	}
}

func TestReconcilePendingCostRetriesIncompleteUsageScans(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	now := time.Now()
	log := &storage.GatewayRequestLog{
		ChannelID:         32,
		Method:            "POST",
		Path:              "/v1/responses",
		StatusCode:        200,
		PromptTokens:      10,
		UpstreamRequestID: "upstream-usage-scan",
		CostReconcileAt:   timePtr(now),
		CreatedAt:         now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	keys := &fakeKeyManager{requestCostError: errors.New("sub2api request cost: usage scan incomplete")}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, keys, nil)
	for attempt := 0; attempt < maxDeterministicCostReconcileAttempts; attempt++ {
		var current storage.GatewayRequestLog
		if err := db.First(&current, log.ID).Error; err != nil {
			t.Fatalf("read attempt %d: %v", attempt+1, err)
		}
		svc.reconcilePendingCost(current, now.Add(-time.Second), now)
	}
	var stored storage.GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read terminal request: %v", err)
	}
	if stored.CostReconcileAt == nil || stored.CostReconcileState != storage.GatewayRequestCostReconcileRetryScheduled || stored.CostReconcileTries != maxDeterministicCostReconcileAttempts || !strings.Contains(stored.CostReconcileError, "usage scan incomplete") {
		t.Fatalf("incomplete scan retry state = %+v", stored)
	}
}

func TestDeterministicCostReconcileFailureReason(t *testing.T) {
	for _, test := range []struct {
		name   string
		reason string
		want   string
	}{
		{name: "usage scan incomplete remains retryable", reason: "upstream usage lookup failed: sub2api request cost: usage scan incomplete", want: ""},
		{name: "duplicate usage id", reason: "upstream usage lookup failed: sub2api request cost: usage response contains duplicate usage id", want: storage.GatewayRequestCostReconcileReasonDuplicateUsageID},
		{name: "final snapshot duplicate usage id", reason: "upstream usage lookup failed: sub2api request cost: final snapshot contains duplicate usage id", want: storage.GatewayRequestCostReconcileReasonDuplicateUsageID},
		{name: "network failure remains retryable", reason: "upstream usage lookup failed: context deadline exceeded", want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := deterministicCostReconcileFailureReason(test.reason)
			if (test.want != "") != ok || got != test.want {
				t.Fatalf("reason = %q, classified=%t, want %q", got, ok, test.want)
			}
		})
	}
}

func markGroupFamilies(t *testing.T, store *storage.Gateway, channelID uint, groupName string, families string) {
	t.Helper()
	now := time.Now()
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID:           channelID,
		UpstreamKeyID:       1,
		UpstreamKeyName:     "test-key",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "cipher",
		APIKeyPrefix:        "sk-test",
		GroupName:           groupName,
		Enabled:             true,
	}); err != nil {
		t.Fatalf("mark group families: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channelID, groupName, "added", 1, "test-key", now); err != nil {
		t.Fatalf("mark group generation: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channelID, groupName)
	if err != nil {
		t.Fatalf("capture group generation: %v", err)
	}
	syncedAt := time.Now()
	if !syncedAt.After(generation.DiscoveredAt) {
		syncedAt = generation.DiscoveredAt.Add(time.Microsecond)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channelID, groupName, families, "", syncedAt, "", generation); err != nil {
		t.Fatalf("bind group families to current generation: %v", err)
	}
}

func ptrTime(v time.Time) *time.Time {
	return &v
}

func TestCandidatesPreferLowestRate(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	cheap := storage.Channel{Name: "cheap", Type: storage.ChannelTypeNewAPI, SiteURL: "https://cheap.example", PasswordCipher: "x", MonitorEnabled: true}
	expensive := storage.Channel{Name: "expensive", Type: storage.ChannelTypeNewAPI, SiteURL: "https://expensive.example", PasswordCipher: "x", MonitorEnabled: true}
	failed := storage.Channel{Name: "failed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://failed.example", PasswordCipher: "x", MonitorEnabled: true, LastError: "down"}
	for _, ch := range []*storage.Channel{&expensive, &cheap, &failed} {
		if err := channels.Create(ch); err != nil {
			t.Fatalf("create channel %s: %v", ch.Name, err)
		}
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: expensive.ID, ModelName: "gpt-test", Ratio: 2, CompletionRatio: 2},
		{ChannelID: cheap.ID, ModelName: "gpt-test", Ratio: 0.5, CompletionRatio: 0.5},
		{ChannelID: failed.ID, ModelName: "gpt-test", Ratio: 0.1, CompletionRatio: 0.1},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, expensive.ID, "gpt-test", "codex")
	markGroupFamilies(t, store, cheap.ID, "gpt-test", "codex")
	markGroupFamilies(t, store, failed.ID, "gpt-test", "codex")

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("candidates len = %d, want 2", len(candidates))
	}
	if candidates[0].Channel.ID != cheap.ID {
		t.Fatalf("first candidate = %s, want cheap", candidates[0].Channel.Name)
	}
	anyCandidates, err := svc.Candidates("")
	if err != nil {
		t.Fatalf("any candidates: %v", err)
	}
	if anyCandidates[0].Channel.ID != cheap.ID {
		t.Fatalf("first any candidate = %s, want cheap", anyCandidates[0].Channel.Name)
	}
}

func TestCandidateGroupFiltersAndScoresPerDownstreamKey(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)
	cheap := &storage.Channel{Name: "cheap", Type: storage.ChannelTypeNewAPI, SiteURL: "https://cheap.example", PasswordCipher: "x", MonitorEnabled: true}
	expensive := &storage.Channel{Name: "expensive", Type: storage.ChannelTypeNewAPI, SiteURL: "https://expensive.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, channel := range []*storage.Channel{cheap, expensive} {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: cheap.ID, ModelName: "gpt", Ratio: 0.5, CompletionRatio: 0.5},
		{ChannelID: expensive.ID, ModelName: "gpt", Ratio: 1, CompletionRatio: 1},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, cheap.ID, "gpt", "codex")
	markGroupFamilies(t, store, expensive.ID, "gpt", "codex")
	group := &storage.GatewayCandidateGroup{Name: "premium only"}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{{ChannelID: expensive.ID, GroupName: "gpt"}}); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	key := &storage.GatewayDownstreamKey{Name: "grouped", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID, KeyHash: "grouped-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	candidates, exclusions, err := svc.CandidatesForDownstreamKeyWithExclusions(key.ID, storage.GatewayAppCodex, "gpt", false)
	if err != nil {
		t.Fatalf("grouped candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Channel.ID != expensive.ID {
		t.Fatalf("grouped candidates = %+v", candidates)
	}
	if math.Abs(candidates[0].PricePriorityScore-DefaultRoutingConfig().PriceScoreMax) > 1e-9 {
		t.Fatalf("group-local price score = %v, want %v", candidates[0].PricePriorityScore, DefaultRoutingConfig().PriceScoreMax)
	}
	if len(exclusions) != 0 {
		t.Fatalf("group-external candidates leaked into exclusions: %+v", exclusions)
	}
	if _, err := svc.ManualCandidateForApp(key.ID, key.App, cheap.ID, "gpt", "gpt"); err == nil {
		t.Fatal("manual route accepted a candidate outside the current candidate group")
	}
	key.SmartRouting = false
	key.ManualChannelID = expensive.ID
	key.ManualGroupName = "gpt"
	if err := store.UpdateDownstreamKey(key); err != nil {
		t.Fatalf("disable smart routing: %v", err)
	}
	all, _, err := svc.CandidatesForDownstreamKeyWithExclusions(key.ID, key.App, "gpt", false)
	if err != nil || len(all) != 1 || all[0].Channel.ID != expensive.ID {
		t.Fatalf("manual key preview should stay in its candidate group: candidates=%+v err=%v", all, err)
	}
	if err := store.UpdateLatencyProbeEnabled(expensive.ID, "gpt", false); err != nil {
		t.Fatalf("disable candidate-group member probe: %v", err)
	}
	storedGroup, err := store.FindCandidateGroup(group.ID)
	if err != nil || len(storedGroup.Members) != 1 {
		t.Fatalf("probe-disabled member was removed: group=%+v err=%v", storedGroup, err)
	}
	all, _, err = svc.CandidatesForDownstreamKeyWithExclusions(key.ID, key.App, "gpt", false)
	if err != nil || len(all) != 1 || !all[0].ProbeDisabled || all[0].Channel.ID != expensive.ID {
		t.Fatalf("group probe switch changed smart routing: candidates=%+v err=%v", all, err)
	}
	all, _, err = svc.CandidatesForDownstreamKeyWithExclusions(key.ID, key.App, "gpt", true)
	if err != nil || len(all) != 1 || !all[0].ProbeDisabled {
		t.Fatalf("probe-disabled candidate-group preview = %+v err=%v", all, err)
	}
	manual, err := svc.ManualCandidateForApp(key.ID, key.App, expensive.ID, "gpt", "gpt")
	if err != nil || len(manual) != 1 || !manual[0].ProbeDisabled || manual[0].Channel.ID != expensive.ID {
		t.Fatalf("manual route did not bypass the group probe switch: candidates=%+v err=%v", manual, err)
	}
}

func TestManualRoutePreviewSurfacesKnownProtocolMismatch(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	channel := &storage.Channel{
		Name: "mixed fixed preview", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: "https://fixed-preview.example", PasswordCipher: "secret", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	groups := []struct {
		name     string
		families string
		models   string
	}{
		{name: "cached-codex", families: "codex", models: "gpt-test"},
		{name: "cached-unknown", families: "vendor-unsupported", models: "vendor-test"},
	}
	members := make([]storage.GatewayCandidateGroupMember, 0, len(groups))
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	for _, item := range groups {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: item.name, Ratio: 1, CompletionRatio: 1}); err != nil {
			t.Fatalf("upsert rate %s: %v", item.name, err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: item.name, UpstreamKeyID: 1, UpstreamKeyName: "fixed-preview",
			OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
			ModelFamilies: item.families, ModelIDs: item.models,
		}); err != nil {
			t.Fatalf("upsert upstream key %s: %v", item.name, err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, item.name, "added", 1, "fixed-preview", base); err != nil {
			t.Fatalf("record group generation %s: %v", item.name, err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, item.name)
		if err != nil {
			t.Fatalf("capture group generation %s: %v", item.name, err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, item.name, item.families, item.models, generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
			t.Fatalf("record current model sync %s: %v", item.name, err)
		}
		members = append(members, storage.GatewayCandidateGroupMember{ChannelID: channel.ID, GroupName: item.name})
	}
	candidateGroup := &storage.GatewayCandidateGroup{Name: "fixed protocol facts"}
	if err := store.SaveCandidateGroup(candidateGroup, members); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	downstream := &storage.GatewayDownstreamKey{
		Name: "fixed claude", App: storage.GatewayAppClaude, ModelFamily: "claude", CandidateGroupID: candidateGroup.ID,
		KeyHash: "fixed-protocol-preview", KeyPrefix: "sk", Enabled: true, SmartRouting: false,
		ManualChannelID: channel.ID, ManualGroupName: groups[0].name,
	}
	if err := store.CreateDownstreamKey(downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	previews, err := svc.ManualRouteMemberPreviewForDownstreamKey(downstream.ID)
	if err != nil || len(previews) != len(groups) {
		t.Fatalf("manual route preview = %+v err=%v", previews, err)
	}
	for _, preview := range previews {
		reasons := strings.Join(preview.AvailabilityReasons, ",")
		if !strings.Contains(reasons, string(ManualRouteGateReasonProtocolMismatch)) {
			t.Fatalf("fixed preview omitted known protocol mismatch for %s: %+v", preview.Rate.ModelName, preview.AvailabilityReasons)
		}
		if preview.App != storage.GatewayAppClaude {
			t.Fatalf("fixed preview app for %s = %q, want claude", preview.Rate.ModelName, preview.App)
		}
		manual, err := svc.ManualCandidateForApp(downstream.ID, downstream.App, channel.ID, preview.Rate.ModelName, "claude-test")
		if len(manual) != 0 {
			t.Fatalf("fixed route %s accepted known incompatible protocol: candidates=%+v err=%v", preview.Rate.ModelName, manual, err)
		}
		if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonProtocolMismatch {
			t.Fatalf("fixed route %s gate reason = %q ok=%t err=%v, want %q", preview.Rate.ModelName, reason, ok, err, ManualRouteGateReasonProtocolMismatch)
		}
	}
}

func TestManualRoutePreviewUsesOnlyCurrentSuccessfulProtocolFacts(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	channel := &storage.Channel{Name: "fixed preview fact generation", Type: storage.ChannelTypeNewAPI, SiteURL: "https://fixed-preview-facts.example", PasswordCipher: "secret", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 1, UpstreamKeyName: "fixed-preview-facts-v1",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "fixed-preview-facts-v1", base); err != nil {
		t.Fatalf("record initial generation: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture initial generation: %v", err)
	}
	base = generation.DiscoveredAt
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-5.4", base.Add(time.Second), "", generation); err != nil {
		t.Fatalf("record initial model sync: %v", err)
	}
	downstream := &storage.GatewayDownstreamKey{
		Name: "fixed preview claude", App: storage.GatewayAppClaude, ModelFamily: "claude",
		KeyHash: "fixed-preview-generation", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	preview := func() CandidateGroupMemberPreview {
		items, previewErr := svc.ManualRouteMemberPreviewForDownstreamKey(downstream.ID)
		if previewErr != nil || len(items) != 1 {
			t.Fatalf("manual route preview = %+v err=%v", items, previewErr)
		}
		return items[0]
	}
	current := preview()
	if !containsString(current.AvailabilityReasons, string(ManualRouteGateReasonProtocolMismatch)) || current.RoutingAvailable {
		t.Fatalf("current successful incompatible preview = %+v", current)
	}

	if err := store.RecordUpstreamKeyModelsError(channel.ID, "primary", base.Add(2*time.Second), "model sync unavailable", generation); err != nil {
		t.Fatalf("record current model sync failure: %v", err)
	}
	failed := preview()
	if containsString(failed.AvailabilityReasons, string(ManualRouteGateReasonProtocolMismatch)) || !failed.RoutingAvailable || failed.ModelsError == "" {
		t.Fatalf("failed current sync remained a fixed protocol blocker: %+v", failed)
	}

	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-5.4", base.Add(3*time.Second), "", generation); err != nil {
		t.Fatalf("restore current model sync: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 1, "fixed-preview-facts-v1", base.Add(4*time.Second)); err != nil {
		t.Fatalf("retire initial generation: %v", err)
	}
	retired := preview()
	if !containsString(retired.AvailabilityReasons, string(ManualRouteGateReasonGenerationUnavailable)) || retired.RoutingAvailable {
		t.Fatalf("retired fixed preview was still selectable: %+v", retired)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 2, "fixed-preview-facts-v2", base.Add(5*time.Second)); err != nil {
		t.Fatalf("record replacement generation: %v", err)
	}
	stale := preview()
	if containsString(stale.AvailabilityReasons, string(ManualRouteGateReasonProtocolMismatch)) || !stale.RoutingAvailable {
		t.Fatalf("old-generation protocol facts remained a fixed preview blocker: %+v", stale)
	}
}

func TestManualRouteGateReasonOf(t *testing.T) {
	for _, reason := range []ManualRouteGateReason{
		ManualRouteGateReasonTargetRequired,
		ManualRouteGateReasonChannelMissing,
		ManualRouteGateReasonUpstreamKeyMissing,
		ManualRouteGateReasonProtocolMismatch,
		ManualRouteGateReasonModelNotListed,
		ManualRouteGateReasonModelNotSupported,
		ManualRouteGateReasonGenerationUnavailable,
	} {
		err := fmt.Errorf("route validation: %w", &ManualRouteGateError{Reason: reason, Message: "safe detail"})
		got, ok := ManualRouteGateReasonOf(err)
		if !ok || got != reason {
			t.Fatalf("ManualRouteGateReasonOf(%q) = %q ok=%t", reason, got, ok)
		}
	}
	if reason, ok := ManualRouteGateReasonOf(errors.New("ordinary error")); ok || reason != "" {
		t.Fatalf("ordinary error returned manual gate reason %q ok=%t", reason, ok)
	}
}

func TestCandidateGroupChangesRejectInvalidManualRoutesAtomically(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	fixed := &storage.Channel{Name: "fixed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://fixed.example", PasswordCipher: "x", MonitorEnabled: true}
	replacement := &storage.Channel{Name: "replacement", Type: storage.ChannelTypeNewAPI, SiteURL: "https://replacement.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, channel := range []*storage.Channel{fixed, replacement} {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1, CompletionRatio: 1}); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
		markGroupFamilies(t, store, channel.ID, "gpt", "codex")
	}
	primary, err := svc.SaveCandidateGroup(0, "primary", []storage.GatewayCandidateGroupMember{{ChannelID: fixed.ID, GroupName: "gpt"}})
	if err != nil {
		t.Fatalf("save primary candidate group: %v", err)
	}
	alternate, err := svc.SaveCandidateGroup(0, "alternate", []storage.GatewayCandidateGroupMember{{ChannelID: replacement.ID, GroupName: "gpt"}})
	if err != nil {
		t.Fatalf("save alternate candidate group: %v", err)
	}
	key := &storage.GatewayDownstreamKey{
		Name: "fixed client", App: storage.GatewayAppCodex, ModelFamily: "codex",
		CandidateGroupID: primary.ID, ManualChannelID: fixed.ID, ManualGroupName: "gpt",
		KeyHash: "fixed-client", KeyPrefix: "sk", Enabled: true, SmartRouting: false,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	key.SmartRouting = false
	if err := store.UpdateDownstreamKey(key); err != nil {
		t.Fatalf("disable smart routing: %v", err)
	}

	if _, err := svc.UpdateDownstreamKeyWithFamilyAndGroup(key.ID, nil, nil, nil, nil, &alternate.ID, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "candidate group change would invalidate manual route") {
		t.Fatalf("candidate group change error = %v", err)
	}
	storedKey, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("find downstream key: %v", err)
	}
	if storedKey.CandidateGroupID != primary.ID || storedKey.ManualChannelID != fixed.ID || storedKey.ManualGroupName != "gpt" || storedKey.SmartRouting {
		t.Fatalf("rejected key update changed stored route: %+v", storedKey)
	}

	if _, err := svc.SaveCandidateGroup(primary.ID, primary.Name, []storage.GatewayCandidateGroupMember{{ChannelID: replacement.ID, GroupName: "gpt"}}); err == nil || !strings.Contains(err.Error(), key.Name) || !strings.Contains(err.Error(), "manual route") {
		t.Fatalf("candidate group member removal error = %v", err)
	}
	storedGroup, err := store.FindCandidateGroup(primary.ID)
	if err != nil {
		t.Fatalf("find candidate group: %v", err)
	}
	if len(storedGroup.Members) != 1 || storedGroup.Members[0].ChannelID != fixed.ID || storedGroup.Members[0].GroupName != "gpt" {
		t.Fatalf("rejected candidate group update changed stored members: %+v", storedGroup.Members)
	}
	if _, err := svc.SaveCandidateGroup(primary.ID, primary.Name, []storage.GatewayCandidateGroupMember{
		{ChannelID: fixed.ID, GroupName: "gpt"},
		{ChannelID: replacement.ID, GroupName: "gpt"},
	}); err != nil {
		t.Fatalf("candidate group edit preserving manual route: %v", err)
	}
}

func TestProbeSwitchesOnlyControlAutomaticProbes(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	channel := &storage.Channel{Name: "probe paused", Type: storage.ChannelTypeNewAPI, SiteURL: "https://paused.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1, CompletionRatio: 1}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	markGroupFamilies(t, store, channel.ID, "gpt", "codex")
	if err := channels.SetLatencyProbeEnabled(channel.ID, false); err != nil {
		t.Fatalf("pause channel probes: %v", err)
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "gpt", false); err != nil {
		t.Fatalf("pause group probes: %v", err)
	}
	group, err := svc.SaveCandidateGroup(0, "paused channel", []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "gpt"}})
	if err != nil {
		t.Fatalf("channel-level probe switch blocked candidate addition: %v", err)
	}
	key := &storage.GatewayDownstreamKey{
		Name: "manual paused", App: storage.GatewayAppCodex, ModelFamily: "codex",
		CandidateGroupID: group.ID, ManualChannelID: channel.ID, ManualGroupName: "gpt",
		KeyHash: "manual-paused", KeyPrefix: "sk", Enabled: true, SmartRouting: false,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	key.SmartRouting = false
	if err := store.UpdateDownstreamKey(key); err != nil {
		t.Fatalf("disable smart routing: %v", err)
	}

	candidates, _, err := svc.CandidatesForDownstreamKeyWithExclusions(key.ID, key.App, "gpt", false)
	if err != nil || len(candidates) != 1 || candidates[0].Channel.ID != channel.ID || candidates[0].Channel.LatencyProbeEnabled || !candidates[0].ProbeDisabled {
		t.Fatalf("probe switches changed routable candidates: candidates=%+v err=%v", candidates, err)
	}
	manual, err := svc.ManualCandidateForApp(key.ID, key.App, channel.ID, "gpt", "gpt")
	if err != nil || len(manual) != 1 || manual[0].Channel.ID != channel.ID {
		t.Fatalf("channel probe switch changed manual route: candidates=%+v err=%v", manual, err)
	}
	if automatic := filterProbeEnabledCandidates(candidates); len(automatic) != 0 {
		t.Fatalf("paused route remained eligible for automatic probes: %+v", automatic)
	}
	config, err := svc.ProbeConfig()
	if err != nil || len(config) != 1 || !config[0].RoutingAvailable || len(config[0].Groups) != 1 || config[0].Groups[0].ProbeEligible {
		t.Fatalf("probe config mixed routing and probe availability: config=%+v err=%v", config, err)
	}
}

func TestCandidateScoringIgnoresProbeSwitch(t *testing.T) {
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1}, Rate: storage.RateSnapshot{ModelName: "paused-cheap"}, EffectivePrice: 1, ProbeDisabled: true, AvgLatencyMs: 1000},
		{Channel: storage.Channel{ID: 2}, Rate: storage.RateSnapshot{ModelName: "enabled-expensive"}, EffectivePrice: 2, AvgLatencyMs: 1000},
	}
	scoreCandidates(candidates)
	sortCandidates(candidates)
	if candidates[0].Channel.ID != 1 || candidates[0].PricePriorityScore != DefaultRoutingConfig().PriceScoreMax {
		t.Fatalf("probe switch changed smart score/order: %+v", candidates)
	}
}

func TestCandidatesRankByAvailabilityPriceAndLatency(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	fastFair := storage.Channel{Name: "fast-fair", Type: storage.ChannelTypeNewAPI, SiteURL: "https://fast-fair.example", PasswordCipher: "x", MonitorEnabled: true}
	cheapSlow := storage.Channel{Name: "cheap-slow", Type: storage.ChannelTypeNewAPI, SiteURL: "https://cheap-slow.example", PasswordCipher: "x", MonitorEnabled: true}
	expensiveFast := storage.Channel{Name: "expensive-fast", Type: storage.ChannelTypeNewAPI, SiteURL: "https://expensive-fast.example", PasswordCipher: "x", MonitorEnabled: true}
	unavailable := storage.Channel{Name: "unavailable", Type: storage.ChannelTypeNewAPI, SiteURL: "https://unavailable.example", PasswordCipher: "x", MonitorEnabled: true, LastError: "down"}
	for _, ch := range []*storage.Channel{&fastFair, &cheapSlow, &expensiveFast, &unavailable} {
		if err := channels.Create(ch); err != nil {
			t.Fatalf("create channel %s: %v", ch.Name, err)
		}
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: fastFair.ID, ModelName: "gpt-test", Ratio: 0.14, CompletionRatio: 0.14},
		{ChannelID: cheapSlow.ID, ModelName: "gpt-test", Ratio: 0.10, CompletionRatio: 0.10},
		{ChannelID: expensiveFast.ID, ModelName: "gpt-test", Ratio: 0.20, CompletionRatio: 0.20},
		{ChannelID: unavailable.ID, ModelName: "gpt-test", Ratio: 0.01, CompletionRatio: 0.01},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, fastFair.ID, "gpt-test", "codex")
	markGroupFamilies(t, store, cheapSlow.ID, "gpt-test", "codex")
	markGroupFamilies(t, store, expensiveFast.ID, "gpt-test", "codex")
	markGroupFamilies(t, store, unavailable.ID, "gpt-test", "codex")
	now := time.Now()
	for _, item := range []struct {
		channelID uint
		latencyMs int64
	}{
		{channelID: fastFair.ID, latencyMs: 10_000},
		{channelID: cheapSlow.ID, latencyMs: 80_000},
		{channelID: expensiveFast.ID, latencyMs: 10_000},
	} {
		for i := 0; i < defaultRoutingLatencySampleSize; i++ {
			if err := store.AppendRequestLog(&storage.GatewayRequestLog{DownstreamKeyID: 1, ChannelID: item.channelID, GroupName: "gpt-test", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, LatencyMs: item.latencyMs, CreatedAt: now}); err != nil {
				t.Fatalf("append request log: %v", err)
			}
		}
	}

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 3 {
		t.Fatalf("candidates len = %d, want 3", len(candidates))
	}
	if candidates[0].Channel.ID != fastFair.ID || candidates[1].Channel.ID != cheapSlow.ID || candidates[2].Channel.ID != expensiveFast.ID {
		t.Fatalf("candidate order = %s, %s, %s; want fast-fair, cheap-slow, expensive-fast", candidates[0].Channel.Name, candidates[1].Channel.Name, candidates[2].Channel.Name)
	}
	if candidates[0].Score >= candidates[1].Score || candidates[1].Score >= candidates[2].Score {
		t.Fatalf("scores not ascending: %.3f, %.3f, %.3f", candidates[0].Score, candidates[1].Score, candidates[2].Score)
	}
	for _, candidate := range candidates {
		priority := (1 - candidate.Score) * 100
		components := candidate.PricePriorityScore + candidate.LatencyPriorityScore
		if candidate.PricePriorityScore < 0 || candidate.PricePriorityScore > 70 {
			t.Fatalf("price priority score = %.3f, want 0..70", candidate.PricePriorityScore)
		}
		if candidate.LatencyPriorityScore < 0 || candidate.LatencyPriorityScore > 30 {
			t.Fatalf("latency priority score = %.3f, want 0..30", candidate.LatencyPriorityScore)
		}
		if math.Abs(priority-components) > 1e-9 {
			t.Fatalf("priority %.3f != components %.3f", priority, components)
		}
	}
}

func TestCandidatesReadUpdatedRateSnapshotForEachRequest(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	channel := storage.Channel{Name: "changing-rate", Type: storage.ChannelTypeNewAPI, SiteURL: "https://changing-rate.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 0.1, CompletionRatio: 0.2, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert initial rate: %v", err)
	}
	markGroupFamilies(t, store, channel.ID, "gpt", "codex")

	first, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("first candidates: %v", err)
	}
	if len(first) != 1 || first[0].Rate.Ratio != 0.1 || first[0].Rate.CompletionRatio != 0.2 {
		t.Fatalf("first candidate rate = %+v", first)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 0.3, CompletionRatio: 0.4, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert changed rate: %v", err)
	}

	second, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("second candidates: %v", err)
	}
	if len(second) != 1 || second[0].Rate.Ratio != 0.3 || second[0].Rate.CompletionRatio != 0.4 {
		t.Fatalf("candidate used stale rate snapshot: %+v", second)
	}
}

func TestCandidatesSkipRecentTransientUpstreamFailure(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	failed := storage.Channel{Name: "failed-low-price", Type: storage.ChannelTypeNewAPI, SiteURL: "https://failed.example", PasswordCipher: "x", MonitorEnabled: true}
	healthy := storage.Channel{Name: "healthy-fallback", Type: storage.ChannelTypeNewAPI, SiteURL: "https://healthy.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, channel := range []*storage.Channel{&failed, &healthy} {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: failed.ID, ModelName: "gpt", Ratio: 0.01, CompletionRatio: 0.01, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert failed rate: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: healthy.ID, ModelName: "gpt", Ratio: 0.02, CompletionRatio: 0.02, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert healthy rate: %v", err)
	}
	markGroupFamilies(t, store, failed.ID, "gpt", "codex")
	markGroupFamilies(t, store, healthy.ID, "gpt", "codex")
	group := &storage.GatewayCandidateGroup{Name: "failed only"}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{{ChannelID: failed.ID, GroupName: "gpt"}}); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	key := &storage.GatewayDownstreamKey{
		Name: "manual cooling", App: storage.GatewayAppCodex, ModelFamily: "codex",
		CandidateGroupID: group.ID, KeyHash: "manual-cooling", KeyPrefix: "sk", Enabled: true, SmartRouting: false,
		ManualChannelID: failed.ID, ManualGroupName: "gpt",
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	if err := store.SetUpstreamKeyError(failed.ID, "gpt", "gateway status 503"); err != nil {
		t.Fatalf("mark generic error: %v", err)
	}

	candidates, exclusions, err := svc.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates with generic error: %v", err)
	}
	if len(candidates) != 2 || len(exclusions) != 0 {
		t.Fatalf("generic error text must not drive routing health: candidates=%+v exclusions=%+v", candidates, exclusions)
	}
	failedHealthFence := mustCurrentRoutingHealthFence(t, store, failed, "gpt")
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(failedHealthFence, RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "gateway status 503", time.Now()); err != nil {
		t.Fatalf("mark structured transient failure: %v", err)
	}
	for attempt := 2; attempt <= 3; attempt++ {
		candidates, exclusions, err = svc.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
		if err != nil || (attempt < 3 && (len(candidates) != 2 || len(exclusions) != 0)) {
			t.Fatalf("failure %d cooled too early: candidates=%+v exclusions=%+v err=%v", attempt-1, candidates, exclusions, err)
		}
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(failedHealthFence, RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "gateway status 503", time.Now()); err != nil {
			t.Fatalf("mark structured transient failure %d: %v", attempt, err)
		}
	}
	candidates, exclusions, err = svc.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Channel.ID != healthy.ID {
		t.Fatalf("candidates = %+v, want only healthy fallback", candidates)
	}
	if len(exclusions) != 1 || exclusions[0].Reason != "upstream_temporarily_unavailable" || exclusions[0].ChannelName != failed.Name || exclusions[0].Detail != "HTTP 503" {
		t.Fatalf("exclusions = %+v", exclusions)
	}
	manual, err := svc.ManualCandidateForApp(key.ID, storage.GatewayAppCodex, failed.ID, "gpt", "gpt-test")
	if err != nil || len(manual) != 1 || manual[0].Channel.ID != failed.ID {
		t.Fatalf("manual route should allow a cooling candidate in the current candidate group: candidates=%+v err=%v", manual, err)
	}
	probeCandidates, _, err := svc.candidatesForApp(storage.GatewayAppCodex, "gpt-test", 0, false, true)
	if err != nil || len(probeCandidates) != 2 {
		t.Fatalf("probe candidates should include cooling group: candidates=%+v err=%v", probeCandidates, err)
	}
	expiredAt := time.Now().Add(-time.Hour)
	if err := db.Model(&storage.GatewayUpstreamKey{}).Where("channel_id = ? AND group_name = ?", failed.ID, "gpt").Updates(map[string]any{
		"last_routing_failure_at": expiredAt,
		"routing_cooldown_until":  expiredAt.Add(time.Minute),
	}).Error; err != nil {
		t.Fatalf("expire transient failure: %v", err)
	}
	candidates, exclusions, err = svc.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err != nil || len(candidates) != 1 || candidates[0].Channel.ID != healthy.ID || len(exclusions) != 1 {
		t.Fatalf("expired legacy cooldown released real traffic before probe recovery: candidates=%+v exclusions=%+v err=%v", candidates, exclusions, err)
	}
	probeCandidates, _, err = svc.candidatesForApp(storage.GatewayAppCodex, "gpt-test", 0, false, true)
	if err != nil || len(probeCandidates) != 2 || !probeCandidates[1].CoolingDown {
		t.Fatalf("expired isolated route was not retained for probes: candidates=%+v err=%v", probeCandidates, err)
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(failedHealthFence, storage.GatewayAppCodex, time.Now()); err != nil {
		t.Fatalf("clear transient failure after confirmed probe recovery: %v", err)
	}
	candidates, exclusions, err = svc.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates after recovery: %v", err)
	}
	if len(candidates) != 2 || candidates[0].Channel.ID != failed.ID || len(exclusions) != 0 {
		t.Fatalf("recovered candidates = %+v exclusions=%+v", candidates, exclusions)
	}
}

func TestCandidatesReselectAfterThirdRateLimit(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	limited := storage.Channel{Name: "limited-low-price", Type: storage.ChannelTypeNewAPI, SiteURL: "https://limited.example", PasswordCipher: "x", MonitorEnabled: true}
	healthy := storage.Channel{Name: "healthy-fallback", Type: storage.ChannelTypeNewAPI, SiteURL: "https://healthy.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, channel := range []*storage.Channel{&limited, &healthy} {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		ratio := 0.02
		if channel == &limited {
			ratio = 0.01
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: ratio, CompletionRatio: ratio, LastSeenAt: time.Now()}); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
		markGroupFamilies(t, store, channel.ID, "gpt", "codex")
	}
	limitedHealthFence := mustCurrentRoutingHealthFence(t, store, limited, "gpt")
	for attempt := 1; attempt <= 3; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(limitedHealthFence, RoutingFailureHTTPStatus, http.StatusTooManyRequests, "gateway status 429", time.Now()); err != nil {
			t.Fatalf("record rate limit %d: %v", attempt, err)
		}
	}

	candidates, exclusions, err := svc.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("select candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Channel.ID != healthy.ID {
		t.Fatalf("candidates = %+v, want healthy fallback after third 429", candidates)
	}
	if len(exclusions) != 1 || exclusions[0].ChannelID != limited.ID || exclusions[0].Reason != "upstream_temporarily_unavailable" || exclusions[0].Detail != "HTTP 429" {
		t.Fatalf("rate-limited exclusions = %+v", exclusions)
	}
}

func TestCandidatePreviewIncludesCoolingGroupWithoutScoringIt(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)
	failed := storage.Channel{Name: "cooling", Type: storage.ChannelTypeNewAPI, SiteURL: "https://cooling.example", PasswordCipher: "x", MonitorEnabled: true}
	healthy := storage.Channel{Name: "healthy", Type: storage.ChannelTypeNewAPI, SiteURL: "https://healthy.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, ch := range []*storage.Channel{&failed, &healthy} {
		if err := channels.Create(ch); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: ch.ID, ModelName: "gpt", Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now()}); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
		markGroupFamilies(t, store, ch.ID, "gpt", "codex")
	}
	failedHealthFence := mustCurrentRoutingHealthFence(t, store, failed, "gpt")
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(failedHealthFence, RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "failed", time.Now()); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	for attempt := 2; attempt <= 3; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(failedHealthFence, RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "failed", time.Now()); err != nil {
			t.Fatalf("record failure %d: %v", attempt, err)
		}
	}
	candidates, _, err := svc.candidatesForApp(storage.GatewayAppCodex, "gpt-test", 0, true, false)
	if err != nil || len(candidates) != 2 {
		t.Fatalf("preview candidates = %+v err=%v", candidates, err)
	}
	if candidates[0].Channel.ID != healthy.ID || candidates[0].CoolingDown {
		t.Fatalf("available candidate = %+v", candidates[0])
	}
	cooling := candidates[1]
	if cooling.Channel.ID != failed.ID || !cooling.CoolingDown || cooling.FailureCount != 3 || cooling.CooldownUntil != nil || cooling.PricePriorityScore != 0 || cooling.LatencyPriorityScore != 0 {
		t.Fatalf("cooling candidate = %+v", cooling)
	}
}

func TestGatewayCandidateTransientFailureUsesStructuredState(t *testing.T) {
	now := time.Now()
	recent := now.Add(-time.Second)
	expired := now.Add(-time.Hour)
	future := now.Add(time.Minute)
	tests := []struct {
		name string
		info cachedGatewayAppInfo
		want bool
	}{
		{name: "preparation", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailurePreparation, LastRoutingFailureAt: &recent, RoutingFailureCount: 3, RoutingIsolatedAt: &recent}, want: true},
		{name: "transport", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureTransport, LastRoutingFailureAt: &recent, RoutingFailureCount: 3, RoutingIsolatedAt: &recent}, want: true},
		{name: "response timeout", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureResponseTimeout, LastRoutingFailureStatus: http.StatusGatewayTimeout, LastRoutingFailureAt: &recent, RoutingFailureCount: 3, RoutingIsolatedAt: &recent}, want: true},
		{name: "request timeout", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureHTTPStatus, LastRoutingFailureStatus: http.StatusRequestTimeout, LastRoutingFailureAt: &recent, RoutingFailureCount: 3, RoutingIsolatedAt: &recent}, want: true},
		{name: "conflict", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureHTTPStatus, LastRoutingFailureStatus: http.StatusConflict, LastRoutingFailureAt: &recent, RoutingFailureCount: 3, RoutingIsolatedAt: &recent}, want: true},
		{name: "too early", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureHTTPStatus, LastRoutingFailureStatus: http.StatusTooEarly, LastRoutingFailureAt: &recent, RoutingFailureCount: 3, RoutingIsolatedAt: &recent}, want: true},
		{name: "rate limited", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureHTTPStatus, LastRoutingFailureStatus: http.StatusTooManyRequests, LastRoutingFailureAt: &recent, RoutingFailureCount: 3, RoutingIsolatedAt: &recent}, want: true},
		{name: "server error", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureHTTPStatus, LastRoutingFailureStatus: http.StatusBadGateway, LastRoutingFailureAt: &recent, RoutingFailureCount: 3, RoutingIsolatedAt: &recent}, want: true},
		{name: "nonstandard 600 status", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureHTTPStatus, LastRoutingFailureStatus: 600, LastRoutingFailureAt: &recent}, want: false},
		{name: "client error", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureHTTPStatus, LastRoutingFailureStatus: http.StatusBadRequest, LastRoutingFailureAt: &recent}, want: false},
		{name: "first failure is not isolated", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureTransport, LastRoutingFailureAt: &recent, RoutingFailureCount: 1}, want: false},
		{name: "legacy adaptive countdown is not isolation", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureTransport, LastRoutingFailureAt: &recent, RoutingFailureCount: 2, RoutingCooldownUntil: &future}, want: false},
		{name: "dirty threshold count without latch is not isolated", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureTransport, LastRoutingFailureAt: &recent, RoutingFailureCount: 3}, want: false},
		{name: "expired adaptive marker stays isolated", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureHTTPStatus, LastRoutingFailureStatus: http.StatusServiceUnavailable, LastRoutingFailureAt: &expired, RoutingFailureCount: 3, RoutingIsolatedAt: &expired}, want: true},
		{name: "active manual cooldown", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureManualCooldown, LastRoutingFailureAt: &recent, RoutingCooldownUntil: &future}, want: true},
		{name: "expired manual cooldown", info: cachedGatewayAppInfo{LastRoutingFailureKind: RoutingFailureManualCooldown, LastRoutingFailureAt: &expired, RoutingCooldownUntil: &recent}, want: false},
		{name: "generic error has no routing timestamp", info: cachedGatewayAppInfo{}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := gatewayCandidateTransientFailure(tt.info, now)
			if got != tt.want {
				t.Fatalf("unavailable = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestIsSessionScopedCapacityNoticeRequiresAStandaloneNotice(t *testing.T) {
	for _, message := range []string{
		"Selected model is at capacity.",
		"Selected model is at capacity. Please try a different model.",
		"The selected model is at capacity — please select a different model.",
		"Model is currently at capacity!",
		"Selected model is at capacity; please try another model.",
		"Our servers are currently overloaded. Please try again later.",
	} {
		if !IsSessionScopedCapacityNotice(message) {
			t.Fatalf("capacity notice was not recognized: %q", message)
		}
	}
	for _, message := range []string{
		"The user said: selected model is at capacity.",
		"Selected model is at capacity, but continue with this response.",
		"The model is currently overloaded.",
		"Our servers are currently overloaded. Please try again later. I can help with something else.",
		"",
	} {
		if IsSessionScopedCapacityNotice(message) {
			t.Fatalf("ordinary output was classified as a capacity notice: %q", message)
		}
	}
}

func TestIsSessionScopedCapacityNoticePrefix(t *testing.T) {
	for _, message := range []string{
		"Selected",
		"Selected model is at ",
		"Selected model is at capacity. Please try a different",
		"Our servers are currently overloaded. Please ",
	} {
		if !IsSessionScopedCapacityNoticePrefix(message) {
			t.Fatalf("capacity notice prefix was not recognized: %q", message)
		}
	}
	for _, message := range []string{
		"The user said",
		"Selected model is healthy",
		"Selected model is at capacity, but continue",
		"Our servers are currently overloaded, but continue",
		"",
	} {
		if IsSessionScopedCapacityNoticePrefix(message) {
			t.Fatalf("ordinary output was classified as a capacity notice prefix: %q", message)
		}
	}
}

func TestIsSessionScopedUnmarkedCapacityNoticePrefixIsStrict(t *testing.T) {
	for _, message := range []string{
		"Our",
		"Our servers are currently overloaded. Please ",
		"Our servers are currently overloaded. Please try again later.",
	} {
		if !IsSessionScopedUnmarkedCapacityNoticePrefix(message) {
			t.Fatalf("unmarked capacity notice prefix was not recognized: %q", message)
		}
	}
	for _, message := range []string{
		"Selected model is at capacity",
		"The model is currently overloaded",
		"Our servers are currently overloaded. Please try again later. Continue",
		"",
	} {
		if IsSessionScopedUnmarkedCapacityNoticePrefix(message) {
			t.Fatalf("ordinary output was classified as an unmarked capacity notice: %q", message)
		}
	}
}

func TestFilterProbeEnabledCandidatesRequiresChannelAndGroup(t *testing.T) {
	candidates := []Candidate{
		{Channel: storage.Channel{ID: 1, LatencyProbeEnabled: true}},
		{Channel: storage.Channel{ID: 2, LatencyProbeEnabled: true}, ProbeDisabled: true},
		{Channel: storage.Channel{ID: 3, LatencyProbeEnabled: false}},
	}
	filtered := filterProbeEnabledCandidates(candidates)
	if len(filtered) != 1 || filtered[0].Channel.ID != 1 {
		t.Fatalf("probe candidates = %+v, want channel and group enabled only", filtered)
	}
}

func TestLatencyProbeRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   bool
	}{
		{status: http.StatusRequestTimeout, want: true},
		{status: http.StatusConflict, want: true},
		{status: http.StatusTooEarly, want: true},
		{status: http.StatusTooManyRequests, want: true},
		{status: http.StatusInternalServerError, want: true},
		{status: 599, want: true},
		{status: http.StatusBadRequest, want: false},
		{status: http.StatusUnauthorized, want: false},
		{status: http.StatusNotFound, want: false},
		{status: 600, want: false},
	} {
		if got := isRetryableLatencyProbeStatus(tc.status); got != tc.want {
			t.Fatalf("status %d retryable = %t, want %t", tc.status, got, tc.want)
		}
	}

	ctx := context.Background()
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "temporary http", err: connector.HTTPStatusError(http.StatusServiceUnavailable, nil), want: true},
		{name: "too early http", err: connector.HTTPStatusError(http.StatusTooEarly, nil), want: true},
		{name: "authentication http", err: connector.HTTPStatusError(http.StatusUnauthorized, nil), want: false},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, want: true},
		{name: "temporary dns", err: &net.DNSError{Name: "probe.invalid", IsTemporary: true}, want: true},
		{name: "dns timeout", err: &net.DNSError{Name: "probe.invalid", IsTimeout: true}, want: true},
		{name: "permanent dns", err: &net.DNSError{Name: "probe.invalid"}, want: false},
		{name: "caller canceled", err: context.Canceled, want: false},
		{name: "attempt deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "ordinary error", err: errors.New("invalid probe request"), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableLatencyProbeError(ctx, tc.err); got != tc.want {
				t.Fatalf("retryable = %t, want %t for %v", got, tc.want, tc.err)
			}
		})
	}
}

func TestLatencyProbeClassifiesFailuresAndSuccessClearsRoutingState(t *testing.T) {
	var healthy atomic.Bool
	var rateLimited atomic.Bool
	var clientRejected atomic.Bool
	var requests atomic.Int32
	var transientFailures atomic.Int32
	var neutralAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		requests.Add(1)
		if rateLimited.Load() {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"account rate limit"}`))
			return
		}
		if neutralAttempts.Load() > 0 {
			neutralAttempts.Add(-1)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"account rate limit"}`))
			return
		}
		if transientFailures.Load() > 0 {
			transientFailures.Add(-1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"temporarily unavailable"}`))
			return
		}
		if clientRejected.Load() {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"probe payload rejected"}`))
			return
		}
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"temporarily unavailable"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channel := storage.Channel{Name: "probe", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	keyCipher, err := cipher.Encrypt("sk-probe")
	if err != nil {
		t.Fatalf("encrypt upstream key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "gpt", APIKeyCipher: keyCipher, APIKeyPrefix: "sk-probe", ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "gpt", Ratio: 0.01}}}, nil)
	candidate := latencyProbeTestCandidateGeneration(t, store, Candidate{Channel: channel, Rate: storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 0.01}, App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-test"}, LatencyProbeModel: "gpt-test"})

	failed := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, 5*time.Second)
	if failed.StatusCode != http.StatusServiceUnavailable || failed.Error == "" {
		t.Fatalf("failed probe = %+v", failed)
	}
	if requests.Load() != gatewayLatencyProbeAttempts {
		t.Fatalf("failed probe requests = %d, want %d", requests.Load(), gatewayLatencyProbeAttempts)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find failed probe state: %v", err)
	}
	if stored.LastRoutingFailureAt == nil || stored.LastRoutingFailureKind != RoutingFailureHTTPStatus || stored.LastRoutingFailureStatus != http.StatusServiceUnavailable || stored.RoutingFailureCount != gatewayLatencyProbeAttempts || stored.RoutingCooldownUntil != nil {
		t.Fatalf("probe did not record structured routing failure: %+v", stored)
	}

	healthy.Store(true)
	requestsBefore := requests.Load()
	succeeded := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, 5*time.Second)
	if succeeded.StatusCode != http.StatusOK || succeeded.Error != "" {
		t.Fatalf("successful probe = %+v", succeeded)
	}
	if requests.Load()-requestsBefore != gatewayLatencyProbeAttempts {
		t.Fatalf("successful probe requests = %d, want %d", requests.Load()-requestsBefore, gatewayLatencyProbeAttempts)
	}
	stored, err = store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find recovered probe state: %v", err)
	}
	if stored.LastUsedAt != nil || stored.LastRoutingFailureAt != nil || stored.LastRoutingFailureKind != "" || stored.LastRoutingFailureStatus != 0 || stored.LastError != "" {
		t.Fatalf("successful probe did not clear cooldown: %+v", stored)
	}
	probeState, err := store.FindLatencyProbeState(channel.ID, "gpt", storage.GatewayAppCodex, candidate.UpstreamGeneration)
	if err != nil || probeState == nil || probeState.ConfirmationOutcome != "success" || probeState.ConfirmationCount != gatewayLatencyProbeAttempts || probeState.LastConfirmedRecoveryAt == nil {
		t.Fatalf("successful probe did not record separate recovery fact: state=%+v err=%v", probeState, err)
	}

	transientFailures.Store(gatewayLatencyProbeAttempts - 1)
	requestsBefore = requests.Load()
	recovered := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, 5*time.Second)
	if recovered.StatusCode != http.StatusOK || recovered.Error == "" || !recovered.confirmationMixed || requests.Load()-requestsBefore != gatewayLatencyProbeAttempts {
		t.Fatalf("recovered probe = %+v requests=%d", recovered, requests.Load()-requestsBefore)
	}
	stored, err = store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find transiently recovered probe state: %v", err)
	}
	if stored.LastRoutingFailureAt != nil || stored.RoutingFailureCount != 0 || stored.RoutingCooldownUntil != nil {
		t.Fatalf("mixed confirmation changed routing state: %+v", stored)
	}

	rateLimited.Store(true)
	requestsBefore = requests.Load()
	limited := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, 5*time.Second)
	if limited.StatusCode != http.StatusTooManyRequests || limited.Error == "" {
		t.Fatalf("rate-limited probe = %+v", limited)
	}
	if requests.Load()-requestsBefore != gatewayLatencyProbeAttempts {
		t.Fatalf("rate-limited probe requests = %d, want %d", requests.Load()-requestsBefore, gatewayLatencyProbeAttempts)
	}
	stored, err = store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find rate-limited probe state: %v", err)
	}
	if stored.LastRoutingFailureAt != nil || stored.RoutingCooldownUntil != nil {
		t.Fatalf("rate-limited probe changed global routing state: %+v", stored)
	}

	rateLimited.Store(false)
	neutralAttempts.Store(1)
	requestsBefore = requests.Load()
	mixedNeutral := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, 5*time.Second)
	if mixedNeutral.StatusCode != http.StatusOK || mixedNeutral.Error == "" || !mixedNeutral.confirmationMixed || mixedNeutral.confirmationNeutral || requests.Load()-requestsBefore != gatewayLatencyProbeAttempts {
		t.Fatalf("mixed neutral probe = %+v requests=%d", mixedNeutral, requests.Load()-requestsBefore)
	}
	stored, err = store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find mixed neutral probe state: %v", err)
	}
	if stored.LastRoutingFailureAt != nil || stored.RoutingCooldownUntil != nil {
		t.Fatalf("mixed neutral probe changed global routing state: %+v", stored)
	}

	clientRejected.Store(true)
	requestsBefore = requests.Load()
	rejected := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, 5*time.Second)
	if rejected.StatusCode != http.StatusBadRequest || rejected.Error == "" {
		t.Fatalf("client-rejected probe = %+v", rejected)
	}
	if requests.Load()-requestsBefore != 1 {
		t.Fatalf("client-rejected probe requests = %d, want 1", requests.Load()-requestsBefore)
	}
	stored, err = store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find client-rejected probe state: %v", err)
	}
	if stored.LastError != "" || stored.LastRoutingFailureAt != nil || stored.RoutingCooldownUntil != nil {
		t.Fatalf("ordinary probe rejection polluted key or routing state: %+v", stored)
	}
}

func TestLatencyProbeAccountConcurrencyRejectionSkipsNetworkAndRoutingHealth(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("latency-probe-account-concurrency")
	if err != nil {
		t.Fatal(err)
	}
	channel := storage.Channel{Name: "latency budget", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatal(err)
	}
	ciphertext, err := cipher.Encrypt("latency-budget-key")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "gpt", UpstreamKeyID: 1, UpstreamKeyName: "BOG latency budget",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        ciphertext, APIKeyPrefix: "test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	candidate := latencyProbeTestCandidateGeneration(t, store, Candidate{
		Channel: channel, Rate: storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1},
		App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-test"}, LatencyProbeModel: "gpt-test",
	})
	service := NewService(channels, storage.NewRates(db), store, cipher, nil, nil)
	active, decision := service.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencySmartData)
	if active == nil || !decision.Admitted {
		t.Fatalf("active data-plane decision = %#v", decision)
	}
	defer active.Release()

	result := service.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, time.Second)
	if !result.deferred || result.Error != "" {
		t.Fatalf("latency probe result = %+v", result)
	}
	if requests.Load() != 0 {
		t.Fatalf("network requests = %d, want 0", requests.Load())
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastRoutingFailureAt != nil || stored.RoutingCooldownUntil != nil || stored.RoutingFailureCount != 0 {
		t.Fatalf("admission rejection polluted routing health: %+v", stored)
	}
}

func TestLatencyProbeDoesNotWriteToRecreatedUpstreamGroup(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		startedOnce.Do(func() { close(started) })
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer server.Close()

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("latency-probe-generation-race")
	if err != nil {
		t.Fatal(err)
	}
	channel := storage.Channel{Name: "probe generation race", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	oldCiphertext, err := cipher.Encrypt("sk-old-generation")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: oldCiphertext, APIKeyPrefix: "sk-old",
		ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "old key", now); err != nil {
		t.Fatal(err)
	}
	staleGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, rates, store, cipher, &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "primary", Ratio: 1}}}, nil)
	candidate := Candidate{
		Channel:            storage.Channel{ID: channel.ID, Name: channel.Name, SiteURL: server.URL},
		Rate:               storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1},
		UpstreamGeneration: staleGeneration,
		App:                storage.GatewayAppCodex, ModelIDs: []string{"gpt-test"}, LatencyProbeModel: "gpt-test", RawRatio: 1,
	}
	candidate = latencyProbeTestCandidateGeneration(t, store, candidate)
	resultCh := make(chan LatencyProbeResult, 1)
	go func() {
		resultCh <- service.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, 5*time.Second)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("latency probe did not reach upstream")
	}

	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 1, "old key", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteGroupRoutingState(channel.ID, "primary"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 2, "new key", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	newCiphertext, err := cipher.Encrypt("sk-new-generation")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: newCiphertext, APIKeyPrefix: "sk-new",
		ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case result := <-resultCh:
		if !result.deferred || result.Error != "" || result.StatusCode != 0 {
			t.Fatalf("latency probe result = %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("latency probe did not finish")
	}

	current, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if !current.Enabled || current.APIKeyCipher != newCiphertext || current.LastUsedAt != nil {
		t.Fatalf("stale probe touched recreated key: %+v", current)
	}
	capability, err := store.FindModelCapability(channel.ID, "primary", "gpt-test")
	if err != nil || capability != nil {
		t.Fatalf("stale probe wrote model capability: %+v err=%v", capability, err)
	}
}

func TestLatencyProbeUpstreamKeyRateLimitDoesNotCooldown(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("probe-key-rate-limit-test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channel := storage.Channel{Name: "probe-key-limit", Type: storage.ChannelTypeNewAPI, SiteURL: "https://probe.example", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		// Keep the cache row but leave it unusable so the test exercises the
		// remote group/key preparation error rather than the gateway hot path.
		ChannelID: channel.ID, GroupName: "gpt",
		ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	svc := NewService(channels, storage.NewRates(db), store, cipher, &fakeKeyManager{
		groupsErr: connector.HTTPStatusError(http.StatusTooManyRequests, []byte(`{"error":"account rate limit"}`)),
	}, nil)
	candidate := latencyProbeTestCandidateGeneration(t, store, Candidate{Channel: channel, Rate: storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 0.01}, App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-test"}, LatencyProbeModel: "gpt-test"})

	result := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, time.Second)
	if result.Error == "" || result.StatusCode != 0 {
		t.Fatalf("rate-limited key preparation result = %+v", result)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "gpt")
	if err != nil {
		t.Fatalf("find upstream key: %v", err)
	}
	if stored.LastRoutingFailureAt != nil || stored.RoutingCooldownUntil != nil {
		t.Fatalf("rate-limited key preparation changed global routing state: %+v", stored)
	}
}

func TestLatencyProbePreparationFailureConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name              string
		failure           error
		wantCalls         int
		wantRoutingFailed bool
	}{
		{name: "transient service unavailable", failure: connector.HTTPStatusError(http.StatusServiceUnavailable, nil), wantCalls: gatewayLatencyProbeAttempts, wantRoutingFailed: true},
		{name: "authentication", failure: connector.HTTPStatusError(http.StatusUnauthorized, nil), wantCalls: 1},
		{name: "permanent configuration", failure: errors.New("invalid upstream group configuration"), wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := testdb.OpenWithError(t)
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			channels := storage.NewChannels(db)
			store := storage.NewGateway(db)
			cipher, err := appcrypto.NewCipher("probe-preparation-confirmation-test-secret")
			if err != nil {
				t.Fatal(err)
			}
			channel := storage.Channel{Name: "probe preparation", Type: storage.ChannelTypeNewAPI, SiteURL: "https://probe.invalid", MonitorEnabled: true}
			if err := channels.Create(&channel); err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
				ChannelID: channel.ID, GroupName: "gpt", ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true,
			}); err != nil {
				t.Fatal(err)
			}
			keys := &fakeKeyManager{groupsErr: tc.failure}
			svc := NewService(channels, storage.NewRates(db), store, cipher, keys, nil)
			candidate := latencyProbeTestCandidateGeneration(t, store, Candidate{Channel: channel, Rate: storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 0.01}, App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-test"}, LatencyProbeModel: "gpt-test"})
			result := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, time.Second)
			if result.Error == "" || keys.listGroupCalls != tc.wantCalls {
				t.Fatalf("probe result=%+v group calls=%d, want %d", result, keys.listGroupCalls, tc.wantCalls)
			}
			stored, err := store.FindUpstreamKeyAny(channel.ID, "gpt")
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantRoutingFailed {
				if stored.LastRoutingFailureAt == nil || stored.RoutingFailureCount != gatewayLatencyProbeAttempts || stored.RoutingCooldownUntil != nil || stored.LastRoutingFailureKind != RoutingFailurePreparation {
					t.Fatalf("confirmed preparation failure = %+v", stored)
				}
			} else if stored.LastRoutingFailureAt != nil || stored.RoutingFailureCount != 0 || stored.RoutingCooldownUntil != nil {
				t.Fatalf("non-transient preparation error changed routing state: %+v", stored)
			}
		})
	}
}

func TestLatencyProbeErrorResponsesAreClassifiedByStatus(t *testing.T) {
	for _, tc := range []struct {
		name              string
		status            int
		wantCalls         int32
		wantRoutingFailed bool
		truncated         bool
		invalidatesKey    bool
	}{
		{name: "empty service unavailable", status: http.StatusServiceUnavailable, wantCalls: gatewayLatencyProbeAttempts, wantRoutingFailed: true},
		{name: "empty rate limit", status: http.StatusTooManyRequests, wantCalls: gatewayLatencyProbeAttempts},
		{name: "empty bad request", status: http.StatusBadRequest, wantCalls: 1},
		{name: "truncated service unavailable", status: http.StatusServiceUnavailable, wantCalls: gatewayLatencyProbeAttempts, wantRoutingFailed: true, truncated: true},
		{name: "truncated rate limit", status: http.StatusTooManyRequests, wantCalls: gatewayLatencyProbeAttempts, truncated: true},
		{name: "truncated unauthorized", status: http.StatusUnauthorized, wantCalls: 1, truncated: true, invalidatesKey: true},
		{name: "truncated bad request", status: http.StatusBadRequest, wantCalls: 1, truncated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				if tc.truncated {
					w.Header().Set("Content-Length", "128")
				}
				w.WriteHeader(tc.status)
				if tc.truncated {
					_, _ = w.Write([]byte(`{"error":`))
				}
			}))
			defer server.Close()
			db, err := testdb.OpenWithError(t)
			if err != nil {
				t.Fatal(err)
			}
			channels := storage.NewChannels(db)
			rates := storage.NewRates(db)
			store := storage.NewGateway(db)
			cipher, err := appcrypto.NewCipher("probe-empty-response-test-secret")
			if err != nil {
				t.Fatal(err)
			}
			channel := storage.Channel{Name: "empty probe", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
			if err := channels.Create(&channel); err != nil {
				t.Fatal(err)
			}
			keyCipher, err := cipher.Encrypt("sk-empty-probe")
			if err != nil {
				t.Fatal(err)
			}
			if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "gpt", APIKeyCipher: keyCipher, APIKeyPrefix: "sk-empty", UpstreamKeyID: 1, UpstreamKeyName: "BOG test", Enabled: true}); err != nil {
				t.Fatal(err)
			}
			svc := NewService(channels, rates, store, cipher, nil, nil)
			candidate := latencyProbeTestCandidateGeneration(t, store, Candidate{Channel: channel, Rate: storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt", Ratio: 1}, App: storage.GatewayAppCodex, ModelIDs: []string{"gpt-test"}, LatencyProbeModel: "gpt-test"})
			result := svc.runLatencyProbe(context.Background(), storage.GatewayAppCodex, candidate, time.Second)
			if result.Error == "" || requests.Load() != tc.wantCalls {
				t.Fatalf("result=%+v requests=%d, want %d", result, requests.Load(), tc.wantCalls)
			}
			stored, err := store.FindUpstreamKeyAny(channel.ID, "gpt")
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantRoutingFailed {
				if stored.LastRoutingFailureAt == nil || stored.RoutingFailureCount != gatewayLatencyProbeAttempts || stored.RoutingCooldownUntil != nil {
					t.Fatalf("transient status was not confirmed: %+v", stored)
				}
			} else if stored.LastRoutingFailureAt != nil || stored.RoutingFailureCount != 0 {
				t.Fatalf("empty non-failure response changed routing state: %+v", stored)
			}
			if stored.Enabled == tc.invalidatesKey {
				t.Fatalf("key enabled=%t, invalidates=%t", stored.Enabled, tc.invalidatesKey)
			}
		})
	}
}

func TestPriceScoreUsesCheapestCandidateRatio(t *testing.T) {
	candidates := []Candidate{
		{EffectivePrice: 1, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 2, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 4, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 8, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 16, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 32, AvgLatencyMs: routingLatencyFullScoreMs},
	}
	scoreCandidates(candidates)

	wantPriceScores := []float64{
		70,
		35,
		17.5,
		8.75,
		4.375,
		2.1875,
	}
	for i, want := range wantPriceScores {
		if math.Abs(candidates[i].PricePriorityScore-want) > 1e-9 {
			t.Fatalf("candidate %d price priority score = %.6f, want %.6f", i, candidates[i].PricePriorityScore, want)
		}
	}
	if candidates[0].Score >= candidates[1].Score || candidates[1].Score >= candidates[2].Score || candidates[2].Score >= candidates[3].Score {
		t.Fatalf("candidates not ordered by price ratio score: %.6f %.6f %.6f %.6f", candidates[0].Score, candidates[1].Score, candidates[2].Score, candidates[3].Score)
	}
}

func TestCandidatesApplyChannelFundingFactorToEffectiveRate(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "funded", Type: storage.ChannelTypeNewAPI, SiteURL: "https://funded.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt-group", Ratio: 2, CompletionRatio: 4}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	markGroupFamilies(t, store, channel.ID, "gpt-group", "codex")
	repo := storage.NewCosting(db)
	state, err := repo.FindState(channel.ID)
	if err != nil {
		t.Fatalf("find costing state: %v", err)
	}
	state.FundingFactor = 0.5
	state.ActualInventory = state.RawBalance * state.FundingFactor
	if err := repo.UpdateState(state, state.Version); err != nil {
		t.Fatalf("update costing state: %v", err)
	}
	if err := channels.UpdateBalance(channel.ID, 10, time.Now(), ""); err != nil {
		t.Fatalf("set available balance: %v", err)
	}
	svc := NewService(channels, rates, store, nil, nil, nil)
	svc.SetCosting(costing.NewService(repo))
	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	candidate := candidates[0]
	if candidate.RawRatio != 2 || candidate.RawCompletionRatio != 4 || candidate.FundingFactor != 0.5 || candidate.Rate.Ratio != 1 || candidate.Rate.CompletionRatio != 2 {
		t.Fatalf("candidate rate snapshot = %+v", candidate)
	}
	state, err = repo.FindState(channel.ID)
	if err != nil {
		t.Fatalf("reload costing state: %v", err)
	}
	state.FundingFactor = 0
	state.ActualInventory = 0
	if err := repo.UpdateState(state, state.Version); err != nil {
		t.Fatalf("set free-credit factor: %v", err)
	}
	if err := channels.UpdateBalance(channel.ID, 10, time.Now(), ""); err != nil {
		t.Fatalf("keep free-credit channel available: %v", err)
	}
	candidates, err = svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("free-credit candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].FundingFactor != 0 || candidates[0].Rate.Ratio != 0 || candidates[0].Rate.CompletionRatio != 0 {
		t.Fatalf("free-credit candidate rate snapshot = %+v", candidates)
	}
}

func TestCandidatesExcludeNeedsReviewCostBasisExceptFixedRoute(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	reviewed := &storage.Channel{Name: "reviewed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://reviewed.invalid", MonitorEnabled: true}
	healthy := &storage.Channel{Name: "healthy", Type: storage.ChannelTypeNewAPI, SiteURL: "https://healthy.invalid", MonitorEnabled: true}
	for _, channel := range []*storage.Channel{reviewed, healthy} {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel %s: %v", channel.Name, err)
		}
		markGroupFamilies(t, store, channel.ID, "gpt-group", "codex")
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: reviewed.ID, ModelName: "gpt-group", Ratio: 0.1, CompletionRatio: 0.2}); err != nil {
		t.Fatalf("upsert reviewed rate: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: healthy.ID, ModelName: "gpt-group", Ratio: 1, CompletionRatio: 2}); err != nil {
		t.Fatalf("upsert healthy rate: %v", err)
	}

	repo := storage.NewCosting(db)
	reviewedState, err := repo.FindState(reviewed.ID)
	if err != nil {
		t.Fatalf("find reviewed state: %v", err)
	}
	reviewedState.FundingFactor = 0.25
	reviewedState.RawBalance = 40
	reviewedState.ActualInventory = 10
	if err := repo.UpdateState(reviewedState, reviewedState.Version); err != nil {
		t.Fatalf("set reviewed funding basis: %v", err)
	}
	if err := repo.MarkCostUnitReview(reviewed.ID, reviewedState.Version, "provider factor changed"); err != nil {
		t.Fatalf("mark cost unit review: %v", err)
	}
	reviewedState, err = repo.FindState(reviewed.ID)
	if err != nil {
		t.Fatalf("reload reviewed state: %v", err)
	}

	key := &storage.GatewayDownstreamKey{
		Name: "fixed reviewed", App: storage.GatewayAppCodex, ModelFamily: "codex",
		KeyHash: "fixed-reviewed-cost", KeyPrefix: "sk", Enabled: true, SmartRouting: false,
		ManualChannelID: reviewed.ID, ManualGroupName: "gpt-group",
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	svc := NewService(channels, rates, store, nil, nil, nil)
	svc.SetCosting(costing.NewService(repo))

	for _, includeDisabled := range []bool{false, true} {
		candidates, exclusions, err := svc.CandidatesForDownstreamKeyWithExclusions(key.ID, key.App, "gpt-test", includeDisabled)
		if err != nil {
			t.Fatalf("smart candidates includeDisabled=%t: %v", includeDisabled, err)
		}
		if len(candidates) != 1 || candidates[0].Channel.ID != healthy.ID {
			t.Fatalf("smart candidates includeDisabled=%t = %+v, want only healthy", includeDisabled, candidates)
		}
		if len(exclusions) != 1 || exclusions[0].ChannelID != reviewed.ID || exclusions[0].Reason != "cost_basis_needs_review" {
			t.Fatalf("cost-review exclusions includeDisabled=%t = %+v", includeDisabled, exclusions)
		}
	}

	manual, err := svc.ManualCandidateForApp(key.ID, key.App, key.ManualChannelID, key.ManualGroupName, "gpt-test")
	if err != nil {
		t.Fatalf("manual candidate: %v", err)
	}
	if len(manual) != 1 || manual[0].Channel.ID != reviewed.ID || manual[0].Rate.ModelName != "gpt-group" {
		t.Fatalf("manual candidates = %+v, want reviewed fixed target only", manual)
	}
	if manual[0].FundingFactor != reviewedState.FundingFactor || manual[0].CostBasisVersion != reviewedState.Version {
		t.Fatalf("manual cost basis = factor %.6f version %d, want %.6f/%d", manual[0].FundingFactor, manual[0].CostBasisVersion, reviewedState.FundingFactor, reviewedState.Version)
	}

	reviewedState.Status = costing.StateStatusReady
	reviewedState.LastError = ""
	if err := repo.UpdateState(reviewedState, reviewedState.Version); err != nil {
		t.Fatalf("restore ready cost basis: %v", err)
	}
	readyState, err := repo.FindState(reviewed.ID)
	if err != nil {
		t.Fatalf("reload ready state: %v", err)
	}
	candidates, exclusions, err := svc.CandidatesForDownstreamKeyWithExclusions(key.ID, key.App, "gpt-test", false)
	if err != nil {
		t.Fatalf("smart candidates after review: %v", err)
	}
	if len(candidates) != 2 || candidates[0].Channel.ID != reviewed.ID || len(exclusions) != 0 {
		t.Fatalf("restored smart candidates = %+v exclusions=%+v", candidates, exclusions)
	}
	if candidates[0].FundingFactor != readyState.FundingFactor || candidates[0].CostBasisVersion != readyState.Version {
		t.Fatalf("restored cost basis = factor %.6f version %d, want %.6f/%d", candidates[0].FundingFactor, candidates[0].CostBasisVersion, readyState.FundingFactor, readyState.Version)
	}
}

func TestChannelFundingFactorsTrustLegacyFallbacks(t *testing.T) {
	channel := storage.Channel{ID: 987654, FundingFactor: 0.75, FundingFactorSource: "legacy_projection"}
	withoutCosting := NewService(nil, nil, nil, nil, nil, nil).channelFundingFactors([]storage.Channel{channel})[channel.ID]
	if !withoutCosting.trusted || withoutCosting.factor != channel.FundingFactor || withoutCosting.source != channel.FundingFactorSource {
		t.Fatalf("fallback without costing = %+v", withoutCosting)
	}

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	svc := NewService(nil, nil, nil, nil, nil, nil)
	svc.SetCosting(costing.NewService(storage.NewCosting(db)))
	missingState := svc.channelFundingFactors([]storage.Channel{channel})[channel.ID]
	if !missingState.trusted || missingState.factor != channel.FundingFactor || missingState.source != channel.FundingFactorSource || missingState.version != 0 {
		t.Fatalf("fallback with missing cost state = %+v", missingState)
	}
}

func TestSplitRateCostBasisSnapshotsKeepsAtomicPairAndFailsClosedOnUnknownStatus(t *testing.T) {
	paired := []storage.RateCostBasisSnapshot{
		{
			ID: 1, ChannelID: 32, ModelName: "plus", Ratio: 0.06, CompletionRatio: 0.3,
			FundingFactor: 1.00503464869094, CostBasisVersion: 19,
			CostBasisSource: "cost_unit_rebase", CostBasisStatus: costing.StateStatusReady,
			CostBasisStateFound: true,
		},
		{
			ID: 2, ChannelID: 32, ModelName: "special", Ratio: 0.015, CompletionRatio: 0.075,
			FundingFactor: 1.00503464869094, CostBasisVersion: 19,
			CostBasisSource: "cost_unit_rebase", CostBasisStatus: costing.StateStatusReady,
			CostBasisStateFound: true,
		},
		{
			ID: 3, ChannelID: 33, ModelName: "legacy", Ratio: 1,
			FundingFactor: 0.5, CostBasisSource: "legacy_projection",
			CostBasisStateFound: false,
		},
		{
			ID: 4, ChannelID: 34, ModelName: "invalid", Ratio: 1,
			FundingFactor: 1, CostBasisVersion: 7, CostBasisSource: "unknown",
			CostBasisStatus: "unexpected_status", CostBasisStateFound: true,
		},
	}
	rates, funding := splitRateCostBasisSnapshots(paired)
	if len(rates) != len(paired) || rates[0].Ratio != paired[0].Ratio || rates[1].Ratio != paired[1].Ratio {
		t.Fatalf("paired rates = %+v", rates)
	}
	if got := funding[32]; !got.trusted || got.factor != paired[0].FundingFactor || got.version != 19 || got.source != "cost_unit_rebase" {
		t.Fatalf("ready paired cost basis = %+v", got)
	}
	if got := funding[33]; !got.trusted || got.factor != 0.5 || got.version != 0 {
		t.Fatalf("legacy paired cost basis = %+v", got)
	}
	if got := funding[34]; got.trusted {
		t.Fatalf("unknown cost status was trusted: %+v", got)
	}
}

func TestPriceScoreIgnoresDisabledCandidates(t *testing.T) {
	candidates := []Candidate{
		{EffectivePrice: 0.01, AvgLatencyMs: routingLatencyFullScoreMs, DisabledForKey: true},
		{EffectivePrice: 1, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 2, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 4, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 8, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 16, AvgLatencyMs: routingLatencyFullScoreMs},
		{EffectivePrice: 32, AvgLatencyMs: routingLatencyFullScoreMs},
	}
	scoreCandidates(candidates)

	if candidates[0].PricePriorityScore != 0 || candidates[0].LatencyPriorityScore != 0 || candidates[0].Score != 1 {
		t.Fatalf("disabled candidate scores = total %.6f, price %.6f, latency %.6f; want lowest priority", candidates[0].Score, candidates[0].PricePriorityScore, candidates[0].LatencyPriorityScore)
	}
	wantPriceScores := map[int]float64{
		1: 70,
		2: 35,
		3: 17.5,
		4: 8.75,
		5: 4.375,
		6: 2.1875,
	}
	for idx, want := range wantPriceScores {
		if math.Abs(candidates[idx].PricePriorityScore-want) > 1e-9 {
			t.Fatalf("candidate %d price priority score = %.6f, want %.6f", idx, candidates[idx].PricePriorityScore, want)
		}
	}
}

func TestRankedModelExclusionsOnlyReturnsFiltersAheadOfRoutablePrimary(t *testing.T) {
	routing := DefaultRoutingConfig()
	routable := []Candidate{{
		Channel: storage.Channel{ID: 2, Name: "selected"}, Rate: storage.RateSnapshot{ModelName: "standard"}, EffectivePrice: 0.2, AvgLatencyMs: 1000,
	}}
	exclusions := []CandidateExclusion{
		{ChannelName: "higher", GroupName: "cheap", Reason: "model_not_listed", Detail: "gpt-test", candidate: Candidate{Channel: storage.Channel{ID: 1, Name: "higher"}, Rate: storage.RateSnapshot{ModelName: "cheap"}, EffectivePrice: 0.1, AvgLatencyMs: 1000}},
		{ChannelName: "lower", GroupName: "expensive", Reason: "model_not_supported", Detail: "gpt-test", candidate: Candidate{Channel: storage.Channel{ID: 3, Name: "lower"}, Rate: storage.RateSnapshot{ModelName: "expensive"}, EffectivePrice: 0.4, AvgLatencyMs: 1000}},
		{ChannelName: "cooldown", GroupName: "standard", Reason: "upstream_temporarily_unavailable"},
	}

	got := rankedModelExclusions(routable, exclusions, routing)
	if len(got) != 2 {
		t.Fatalf("ranked exclusions = %+v, want cooldown plus one higher-ranked model filter", got)
	}
	if got[0].Reason != "upstream_temporarily_unavailable" || got[1].ChannelName != "higher" {
		t.Fatalf("ranked exclusions = %+v", got)
	}
	for _, exclusion := range got {
		if exclusion.ChannelName == "lower" {
			t.Fatalf("lower-ranked model filter was retained: %+v", got)
		}
	}
}

func TestCandidatesUseFixedLatencyScoreThresholds(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	fast := storage.Channel{Name: "fast", Type: storage.ChannelTypeNewAPI, SiteURL: "https://fast.example", PasswordCipher: "x", MonitorEnabled: true}
	mid := storage.Channel{Name: "mid", Type: storage.ChannelTypeNewAPI, SiteURL: "https://mid.example", PasswordCipher: "x", MonitorEnabled: true}
	slow := storage.Channel{Name: "slow", Type: storage.ChannelTypeNewAPI, SiteURL: "https://slow.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, ch := range []*storage.Channel{&fast, &mid, &slow} {
		if err := channels.Create(ch); err != nil {
			t.Fatalf("create channel %s: %v", ch.Name, err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: ch.ID, ModelName: "gpt-test", Ratio: 0.1, CompletionRatio: 0.1}); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
		markGroupFamilies(t, store, ch.ID, "gpt-test", "codex")
	}
	now := time.Now()
	for _, item := range []struct {
		channelID uint
		latencyMs int64
	}{
		{channelID: fast.ID, latencyMs: 10_000},
		{channelID: mid.ID, latencyMs: 45_000},
		{channelID: slow.ID, latencyMs: 80_000},
	} {
		for i := 0; i < defaultRoutingLatencySampleSize; i++ {
			if err := store.AppendRequestLog(&storage.GatewayRequestLog{DownstreamKeyID: 1, ChannelID: item.channelID, GroupName: "gpt-test", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: item.latencyMs, CreatedAt: now}); err != nil {
				t.Fatalf("append request log: %v", err)
			}
		}
	}

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 3 {
		t.Fatalf("candidates len = %d, want 3", len(candidates))
	}
	if candidates[0].Channel.ID != fast.ID || candidates[1].Channel.ID != mid.ID || candidates[2].Channel.ID != slow.ID {
		t.Fatalf("candidate order = %s, %s, %s; want fast, mid, slow", candidates[0].Channel.Name, candidates[1].Channel.Name, candidates[2].Channel.Name)
	}
	wantLatencyScores := map[uint]float64{fast.ID: 30, mid.ID: 15, slow.ID: 0}
	for _, candidate := range candidates {
		want := wantLatencyScores[candidate.Channel.ID]
		if math.Abs(candidate.LatencyPriorityScore-want) > 1e-9 {
			t.Fatalf("%s latency priority score = %.3f, want %.3f", candidate.Channel.Name, candidate.LatencyPriorityScore, want)
		}
	}
}

func TestRoutingLatencyCacheLoadsOnceAndUpdatesIncrementally(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, nil, nil)
	svc.UpdateRoutingConfig(RoutingConfig{LatencySamples: 2, LatencyWindow: time.Hour})
	now := time.Now()
	for index, latency := range []int64{100, 200} {
		if err := store.AppendRequestLog(&storage.GatewayRequestLog{
			DownstreamKeyID: 1, ChannelID: 9, GroupName: " GPT ", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, LatencyMs: latency, CreatedAt: now.Add(time.Duration(index) * time.Second),
		}); err != nil {
			t.Fatalf("append latency log: %v", err)
		}
	}
	first, err := svc.loadCandidateLatencySamples()
	if err != nil || len(first.groups[candidateLatencyKey{ChannelID: 9, GroupName: "gpt"}]) != 2 {
		t.Fatalf("initial latency snapshot = %+v, err=%v", first, err)
	}
	if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&storage.GatewayRequestLog{}).Error; err != nil {
		t.Fatalf("remove raw latency logs: %v", err)
	}
	second, err := svc.loadCandidateLatencySamples()
	if err != nil || len(second.groups[candidateLatencyKey{ChannelID: 9, GroupName: "gpt"}]) != 2 {
		t.Fatalf("cached latency snapshot = %+v, err=%v", second, err)
	}
	svc.LogRequest(&storage.GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 9, GroupName: "GpT", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, LatencyMs: 500, CreatedAt: now.Add(2 * time.Second),
	})
	third, err := svc.loadCandidateLatencySamples()
	values := third.groups[candidateLatencyKey{ChannelID: 9, GroupName: "gpt"}]
	if err != nil || len(values) != 2 || values[0] != 500 {
		t.Fatalf("incremental latency snapshot = %+v, err=%v", values, err)
	}
}

func TestManualCandidateUsesFixedRoute(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	cheap := storage.Channel{Name: "cheap", Type: storage.ChannelTypeNewAPI, SiteURL: "https://cheap.example", PasswordCipher: "x", MonitorEnabled: true}
	fixed := storage.Channel{Name: "fixed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://fixed.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, ch := range []*storage.Channel{&cheap, &fixed} {
		if err := channels.Create(ch); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: cheap.ID, ModelName: "gpt-test", Ratio: 0.1, CompletionRatio: 0.1},
		{ChannelID: fixed.ID, ModelName: "gpt-test", Ratio: 0.8, CompletionRatio: 0.8},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, cheap.ID, "gpt-test", "codex")
	markGroupFamilies(t, store, fixed.ID, "gpt-test", "codex")

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if candidates[0].Channel.ID != cheap.ID {
		t.Fatalf("smart first candidate = %s, want cheap", candidates[0].Channel.Name)
	}
	manual, err := svc.ManualCandidateForApp(0, storage.GatewayAppCodex, fixed.ID, "gpt-test", "gpt-test")
	if err != nil {
		t.Fatalf("manual candidate: %v", err)
	}
	if len(manual) != 1 || manual[0].Channel.ID != fixed.ID {
		t.Fatalf("manual candidate = %#v, want fixed only", manual)
	}
	if err := store.UpdateLatencyProbeEnabled(fixed.ID, "gpt-test", false); err != nil {
		t.Fatalf("disable group probe: %v", err)
	}
	candidates, err = svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil || len(candidates) != 2 || candidates[0].Channel.ID != cheap.ID || candidates[1].Channel.ID != fixed.ID || !candidates[1].ProbeDisabled {
		t.Fatalf("smart candidates with probe-disabled group = %+v err=%v", candidates, err)
	}
	preview, _, err := svc.candidatesForApp(storage.GatewayAppCodex, "gpt-test", 0, true, false)
	if err != nil || len(preview) != 2 || !preview[1].ProbeDisabled || preview[1].Channel.ID != fixed.ID {
		t.Fatalf("probe-disabled preview = %+v err=%v", preview, err)
	}
	manual, err = svc.ManualCandidateForApp(0, storage.GatewayAppCodex, fixed.ID, "gpt-test", "gpt-test")
	if err != nil || len(manual) != 1 || !manual[0].ProbeDisabled || manual[0].Channel.ID != fixed.ID {
		t.Fatalf("manual candidate did not bypass the group probe switch: candidates=%+v err=%v", manual, err)
	}
	staleSync := time.Now().Add(-24 * time.Hour)
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND group_name = ?", fixed.ID, "gpt-test").
		Updates(map[string]any{
			"model_families":   "codex",
			"model_ids":        "gpt-test",
			"models_synced_at": staleSync,
			"models_error":     "old generation sync failed",
		}).Error; err != nil {
		t.Fatalf("make fixed route model facts stale: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(fixed.ID, "gpt-test")
	if err != nil {
		t.Fatalf("capture fixed route generation: %v", err)
	}
	healthFence := storage.NewGatewayRoutingHealthFence(generation, fixed.UpstreamAccountIdentityRevision)
	failureAt := time.Now().UTC().Add(-time.Second)
	for attempt := 0; attempt < 3; attempt++ {
		if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(healthFence, RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "smart route failure", failureAt.Add(time.Duration(attempt)*time.Millisecond)); err != nil {
			t.Fatalf("record fixed route smart health attempt %d: %v", attempt+1, err)
		}
	}
	manual, err = svc.ManualCandidateForApp(0, storage.GatewayAppCodex, fixed.ID, "gpt-test", "gpt-test")
	if err != nil || len(manual) != 1 || !manual[0].ProbeDisabled || !manual[0].CoolingDown || manual[0].Channel.ID != fixed.ID {
		t.Fatalf("manual candidate applied stale model or smart-health filters: candidates=%+v err=%v", manual, err)
	}
	if err := db.Model(&storage.GatewayUpstreamKey{}).
		Where("channel_id = ? AND group_name = ?", fixed.ID, "gpt-test").
		Updates(map[string]any{
			"model_families": "claude",
			"model_ids":      "claude-stale-only",
		}).Error; err != nil {
		t.Fatalf("make fixed route protocol facts incompatible: %v", err)
	}
	manual, err = svc.ManualCandidateForApp(0, storage.GatewayAppCodex, fixed.ID, "gpt-test", "gpt-test")
	if err != nil || len(manual) != 1 || len(manual[0].ModelFamilies) != 0 || len(manual[0].ModelIDs) != 0 {
		t.Fatalf("manual candidate trusted failed-sync protocol facts: candidates=%+v err=%v", manual, err)
	}
	generation, err = store.CaptureUpstreamGroupGeneration(fixed.ID, "gpt-test")
	if err != nil {
		t.Fatalf("capture fixed route generation: %v", err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(fixed.ID, "gpt-test", "claude", "claude-current", time.Now().UTC(), "", generation); err != nil {
		t.Fatalf("record current fixed route protocol facts: %v", err)
	}
	manual, err = svc.ManualCandidateForApp(0, storage.GatewayAppCodex, fixed.ID, "gpt-test", "gpt-test")
	if len(manual) != 0 {
		t.Fatalf("manual candidate accepted current incompatible protocol: candidates=%+v err=%v", manual, err)
	}
	if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonProtocolMismatch {
		t.Fatalf("current incompatible fixed protocol gate reason = %q ok=%t err=%v", reason, ok, err)
	}
	if err := store.UpdateLatencyProbeEnabled(fixed.ID, "gpt-test", true); err != nil {
		t.Fatalf("reenable group probe: %v", err)
	}
	if err := store.DisableUpstreamKey(fixed.ID, "gpt-test", "invalidated"); err != nil {
		t.Fatalf("disable upstream key: %v", err)
	}
	if _, err := svc.ManualCandidateForApp(0, storage.GatewayAppCodex, fixed.ID, "gpt-test", "gpt-test"); err == nil {
		t.Fatal("manual candidate succeeded for disabled upstream key")
	}
	markGroupFamilies(t, store, fixed.ID, "gpt-test", "codex")
	if err := rates.DeleteSnapshot(fixed.ID, "gpt-test"); err != nil {
		t.Fatalf("delete fixed rate: %v", err)
	}
	if err := svc.DeleteGroupRoutingState(context.Background(), fixed.ID, "gpt-test"); err != nil {
		t.Fatalf("delete fixed routing state: %v", err)
	}
	if _, err := svc.ManualCandidateForApp(0, storage.GatewayAppCodex, fixed.ID, "gpt-test", "gpt-test"); err == nil {
		t.Fatal("manual candidate silently rerouted after group deletion")
	}
}

func TestManualCandidateUsesOnlyCurrentSuccessfulModelFacts(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	channel := &storage.Channel{Name: "fixed model fact generation", Type: storage.ChannelTypeNewAPI, SiteURL: "https://fixed-facts.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 1, UpstreamKeyName: "fixed-facts-v1",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", APIKeyPrefix: "sk-fixed",
		Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "fixed-facts-v1", base); err != nil {
		t.Fatalf("record initial generation: %v", err)
	}
	initialGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture initial generation: %v", err)
	}
	base = initialGeneration.DiscoveredAt
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "claude", "claude-sonnet-current", base.Add(time.Second), "", initialGeneration); err != nil {
		t.Fatalf("record initial model sync: %v", err)
	}
	if _, err := svc.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "primary", "gpt-5.4"); err == nil {
		t.Fatal("current successful incompatible protocol was accepted")
	} else if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonProtocolMismatch {
		t.Fatalf("initial protocol gate reason = %q ok=%t err=%v", reason, ok, err)
	}

	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 1, "fixed-facts-v1", base.Add(2*time.Second)); err != nil {
		t.Fatalf("retire initial generation: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 2, "fixed-facts-v2", base.Add(3*time.Second)); err != nil {
		t.Fatalf("record replacement generation: %v", err)
	}
	currentGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture replacement generation: %v", err)
	}
	if currentGeneration.PresenceGeneration == initialGeneration.PresenceGeneration {
		t.Fatalf("replacement generation did not advance: old=%+v current=%+v", initialGeneration, currentGeneration)
	}
	manual, err := svc.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "primary", "gpt-5.4")
	if err != nil || len(manual) != 1 || len(manual[0].ModelFamilies) != 0 || len(manual[0].ModelIDs) != 0 {
		t.Fatalf("stale model facts blocked or leaked into fixed candidate: candidates=%+v err=%v", manual, err)
	}

	if err := store.RecordUpstreamKeyModelsError(channel.ID, "primary", base.Add(4*time.Second), "replacement model sync failed", currentGeneration); err != nil {
		t.Fatalf("record replacement model sync failure: %v", err)
	}
	manual, err = svc.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "primary", "gpt-5.4")
	if err != nil || len(manual) != 1 || len(manual[0].ModelFamilies) != 0 || len(manual[0].ModelIDs) != 0 {
		t.Fatalf("failed current sync facts blocked or leaked into fixed candidate: candidates=%+v err=%v", manual, err)
	}

	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "claude", "claude-sonnet-current", base.Add(5*time.Second), "", currentGeneration); err != nil {
		t.Fatalf("record current incompatible model sync: %v", err)
	}
	if _, err := svc.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "primary", "gpt-5.4"); err == nil {
		t.Fatal("current successful incompatible protocol was accepted after replacement")
	} else if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonProtocolMismatch {
		t.Fatalf("replacement protocol gate reason = %q ok=%t err=%v", reason, ok, err)
	}

	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-5.4", base.Add(6*time.Second), "", currentGeneration); err != nil {
		t.Fatalf("record current exact model sync: %v", err)
	}
	if _, err := svc.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "primary", "gpt-5.5"); err == nil {
		t.Fatal("current successful exact model mismatch was accepted")
	} else if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonModelNotListed {
		t.Fatalf("exact model gate reason = %q ok=%t err=%v", reason, ok, err)
	}
}

func TestManualCandidateAllowsMissingRateSnapshotWithoutHealthFence(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	channel := &storage.Channel{Name: "fixed no rate", Type: storage.ChannelTypeNewAPI, SiteURL: "https://fixed-no-rate.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 2, CompletionRatio: 3}); err != nil {
		t.Fatalf("upsert initial rate: %v", err)
	}
	now := time.Now()
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 1, UpstreamKeyName: "fixed-no-rate",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", APIKeyPrefix: "sk-fixed",
		ModelFamilies: "codex", ModelIDs: "gpt-5.4", ModelsSyncedAt: &now, Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "fixed-no-rate", now); err != nil {
		t.Fatalf("record group generation: %v", err)
	}
	candidateGroup := &storage.GatewayCandidateGroup{Name: "fixed no-rate group"}
	if err := store.SaveCandidateGroup(candidateGroup, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "primary"}}); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	downstream := &storage.GatewayDownstreamKey{
		Name: "fixed no-rate key", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: candidateGroup.ID,
		KeyHash: "fixed-no-rate-key", KeyPrefix: "sk", Enabled: true, SmartRouting: false,
		ManualChannelID: channel.ID, ManualGroupName: "primary",
	}
	if err := store.CreateDownstreamKey(downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	if err := rates.DeleteSnapshot(channel.ID, "primary"); err != nil {
		t.Fatalf("delete rate snapshot: %v", err)
	}

	manual, err := svc.ManualCandidateForApp(downstream.ID, downstream.App, channel.ID, "primary", "gpt-5.4")
	if err != nil || len(manual) != 1 {
		t.Fatalf("fixed no-rate candidate = %+v err=%v", manual, err)
	}
	if manual[0].Rate.Ratio != 0 || manual[0].Rate.CompletionRatio != 0 || manual[0].RawRatio != 0 || manual[0].RawCompletionRatio != 0 {
		t.Fatalf("fixed no-rate candidate fabricated pricing: %+v", manual[0])
	}
	if manual[0].UpstreamGeneration.Valid() {
		t.Fatalf("fixed no-rate candidate fabricated a health generation: %+v", manual[0].UpstreamGeneration)
	}
	if !manual[0].ModelFactGeneration.Valid() {
		t.Fatalf("fixed no-rate candidate lost its model-fact generation: %+v", manual[0].ModelFactGeneration)
	}
	if !manual[0].KeyStateGeneration.Valid() {
		t.Fatalf("fixed no-rate candidate lost its key-state generation: %+v", manual[0].KeyStateGeneration)
	}

	previews, err := svc.ManualRouteMemberPreviewForDownstreamKey(downstream.ID)
	if err != nil || len(previews) != 1 {
		t.Fatalf("fixed no-rate preview = %+v err=%v", previews, err)
	}
	preview := previews[0]
	if !containsString(preview.AvailabilityReasons, availabilityReasonNoRateSnapshot) || !containsString(preview.ProbeBlockers, availabilityReasonNoRateSnapshot) {
		t.Fatalf("fixed no-rate preview omitted pricing warning: %+v", preview)
	}
	if containsString(preview.AvailabilityReasons, probeBlockerKeyMissing) || !preview.RoutingAvailable {
		t.Fatalf("fixed no-rate preview treated ready key as structural blocker: %+v", preview)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil || !generation.Valid() {
		t.Fatalf("capture current no-rate generation = %+v err=%v", generation, err)
	}
	if err := store.MarkModelCapability(channel.ID, "primary", "gpt-5.4", false, "confirmed unsupported", generation); err != nil {
		t.Fatalf("mark no-rate negative capability: %v", err)
	}
	manual, err = svc.ManualCandidateForApp(downstream.ID, downstream.App, channel.ID, "primary", "gpt-5.4")
	if len(manual) != 0 {
		t.Fatalf("fixed no-rate candidate accepted current negative capability: %+v err=%v", manual, err)
	}
	if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonModelNotSupported {
		t.Fatalf("no-rate negative capability gate reason = %q ok=%t err=%v", reason, ok, err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 1, "fixed-no-rate", time.Now()); err != nil {
		t.Fatalf("retire no-rate group generation: %v", err)
	}
	manual, err = svc.ManualCandidateForApp(downstream.ID, downstream.App, channel.ID, "primary", "gpt-5.4")
	if len(manual) != 0 {
		t.Fatalf("fixed no-rate candidate accepted a retired group: %+v err=%v", manual, err)
	}
	if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonCandidateGroupMismatch {
		t.Fatalf("retired no-rate group gate reason = %q ok=%t err=%v", reason, ok, err)
	}
}

func TestDownstreamRouteExclusionFiltersCandidateForKey(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	cheap := storage.Channel{Name: "cheap", Type: storage.ChannelTypeNewAPI, SiteURL: "https://cheap.example", PasswordCipher: "x", MonitorEnabled: true}
	fallback := storage.Channel{Name: "fallback", Type: storage.ChannelTypeNewAPI, SiteURL: "https://fallback.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, ch := range []*storage.Channel{&cheap, &fallback} {
		if err := channels.Create(ch); err != nil {
			t.Fatalf("create channel %s: %v", ch.Name, err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: ch.ID, ModelName: "gpt-test", Ratio: 0.5, CompletionRatio: 0.5}); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
		markGroupFamilies(t, store, ch.ID, "gpt-test", "codex")
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: cheap.ID, ModelName: "gpt-test", Ratio: 0.1, CompletionRatio: 0.1}); err != nil {
		t.Fatalf("upsert cheap rate: %v", err)
	}
	created, err := svc.CreateDownstreamKey("client", storage.GatewayAppCodex)
	if err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	if err := svc.SetRouteExclusion(created.Key.ID, cheap.ID, "gpt-test", true); err != nil {
		t.Fatalf("set route exclusion: %v", err)
	}
	candidates, _, err := svc.CandidatesForDownstreamKeyWithExclusions(created.Key.ID, storage.GatewayAppCodex, "gpt-test", false)
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Channel.ID != fallback.ID {
		t.Fatalf("filtered candidates = %+v, want fallback only", candidates)
	}
	preview, _, err := svc.CandidatesForDownstreamKeyWithExclusions(created.Key.ID, storage.GatewayAppCodex, "gpt-test", true)
	if err != nil {
		t.Fatalf("preview candidates: %v", err)
	}
	if len(preview) != 2 {
		t.Fatalf("preview candidates len = %d, want 2", len(preview))
	}
	foundDisabled := false
	for _, candidate := range preview {
		if candidate.Channel.ID == cheap.ID {
			foundDisabled = candidate.DisabledForKey
		}
	}
	if !foundDisabled {
		t.Fatal("disabled candidate was not marked in preview")
	}
	manual, err := svc.ManualCandidateForApp(created.Key.ID, storage.GatewayAppCodex, cheap.ID, "gpt-test", "gpt-test")
	if err != nil || len(manual) != 1 || manual[0].Channel.ID != cheap.ID {
		t.Fatalf("manual route did not bypass the current Key exclusion: candidates=%+v err=%v", manual, err)
	}
	smartRouting := false
	manualChannelID := cheap.ID
	manualGroupName := "gpt-test"
	updated, err := svc.UpdateDownstreamKey(created.Key.ID, nil, nil, nil, &smartRouting, &manualChannelID, &manualGroupName)
	if err != nil || updated.SmartRouting || updated.ManualChannelID != cheap.ID || updated.ManualGroupName != "gpt-test" {
		t.Fatalf("save excluded manual route: key=%+v err=%v", updated, err)
	}
	if err := channels.SetLastError(cheap.ID, "temporarily unavailable"); err != nil {
		t.Fatalf("mark manual channel unavailable: %v", err)
	}
	manualPreview, err := svc.ManualRouteMemberPreviewForDownstreamKey(created.Key.ID)
	if err != nil {
		t.Fatalf("manual route preview: %v", err)
	}
	previewedUnavailable := false
	for _, candidate := range manualPreview {
		if candidate.Channel.ID == cheap.ID {
			previewedUnavailable = candidate.DisabledForKey && !candidate.RoutingAvailable && strings.Contains(strings.Join(candidate.AvailabilityReasons, ","), "channel_error")
		}
	}
	if !previewedUnavailable {
		t.Fatalf("manual route preview omitted unavailable excluded channel: %+v", manualPreview)
	}
	updated, err = svc.UpdateDownstreamKey(created.Key.ID, nil, nil, nil, &smartRouting, &manualChannelID, &manualGroupName)
	if err != nil || updated.SmartRouting || updated.ManualChannelID != cheap.ID || updated.ManualGroupName != "gpt-test" {
		t.Fatalf("temporarily unhealthy fixed route was rejected: key=%+v err=%v", updated, err)
	}
	manual, err = svc.ManualCandidateForApp(created.Key.ID, storage.GatewayAppCodex, cheap.ID, "gpt-test", "gpt-test")
	if err != nil || len(manual) != 1 || manual[0].Channel.ID != cheap.ID {
		t.Fatalf("temporarily unhealthy fixed route did not stay direct: candidates=%+v err=%v", manual, err)
	}
	missingGroup := "missing-group"
	if _, err := svc.UpdateDownstreamKey(created.Key.ID, nil, nil, nil, &smartRouting, &manualChannelID, &missingGroup); err == nil {
		t.Fatal("missing fixed route did not fail explicitly")
	} else if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonUpstreamKeyMissing {
		t.Fatalf("missing fixed route gate reason = %q ok=%t err=%v, want %q", reason, ok, err, ManualRouteGateReasonUpstreamKeyMissing)
	}
	if err := channels.ClearSyncFailure(cheap.ID); err != nil {
		t.Fatalf("restore manual channel: %v", err)
	}
	if err := store.UpdateLatencyProbeEnabled(cheap.ID, "gpt-test", false); err != nil {
		t.Fatalf("pause manual group: %v", err)
	}
	updated, err = svc.UpdateDownstreamKey(created.Key.ID, nil, nil, nil, &smartRouting, &manualChannelID, &manualGroupName)
	if err != nil || updated.SmartRouting || updated.ManualChannelID != cheap.ID || updated.ManualGroupName != "gpt-test" {
		t.Fatalf("probe-paused fixed route was rejected: key=%+v err=%v", updated, err)
	}
	if err := store.UpdateLatencyProbeEnabled(cheap.ID, "gpt-test", true); err != nil {
		t.Fatalf("restore manual group: %v", err)
	}
	claudeApp := storage.GatewayAppClaude
	claudeFamily := "claude"
	updated, err = svc.UpdateDownstreamKeyWithFamily(created.Key.ID, nil, nil, &claudeApp, &claudeFamily, &smartRouting, &manualChannelID, &manualGroupName)
	if updated != nil {
		t.Fatalf("incompatible fixed route update returned key: %+v", updated)
	}
	if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonProtocolMismatch {
		t.Fatalf("incompatible fixed route update gate reason = %q ok=%t err=%v", reason, ok, err)
	}
	codexApp := storage.GatewayAppCodex
	codexFamily := "codex"
	updated, err = svc.UpdateDownstreamKeyWithFamily(created.Key.ID, nil, nil, &codexApp, &codexFamily, &smartRouting, &manualChannelID, &manualGroupName)
	if err != nil || updated.App != storage.GatewayAppCodex || updated.ModelFamily != "codex" || updated.SmartRouting {
		t.Fatalf("restore fixed route protocol: key=%+v err=%v", updated, err)
	}

	if err := svc.SetRouteExclusion(created.Key.ID, cheap.ID, "gpt-test", false); err != nil {
		t.Fatalf("clear route exclusion: %v", err)
	}
	candidates, _, err = svc.CandidatesForDownstreamKeyWithExclusions(created.Key.ID, storage.GatewayAppCodex, "gpt-test", false)
	if err != nil {
		t.Fatalf("candidates after clear: %v", err)
	}
	if len(candidates) != 2 || candidates[0].Channel.ID != cheap.ID {
		t.Fatalf("candidates after clear = %+v, want cheap restored first", candidates)
	}
}

func TestRoutePreviewScoresAreIsolatedPerDownstreamKey(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	upstreams := []*storage.Channel{
		{Name: "cheap", Type: storage.ChannelTypeNewAPI, SiteURL: "https://cheap.example", PasswordCipher: "x", MonitorEnabled: true},
		{Name: "middle", Type: storage.ChannelTypeNewAPI, SiteURL: "https://middle.example", PasswordCipher: "x", MonitorEnabled: true},
		{Name: "expensive", Type: storage.ChannelTypeNewAPI, SiteURL: "https://expensive.example", PasswordCipher: "x", MonitorEnabled: true},
	}
	prices := []float64{0.1, 0.2, 0.4}
	for index, upstream := range upstreams {
		if err := channels.Create(upstream); err != nil {
			t.Fatalf("create channel %s: %v", upstream.Name, err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{
			ChannelID: upstream.ID, ModelName: "gpt-test", Ratio: prices[index], CompletionRatio: prices[index], LastSeenAt: time.Now(),
		}); err != nil {
			t.Fatalf("upsert rate %s: %v", upstream.Name, err)
		}
		markGroupFamilies(t, store, upstream.ID, "gpt-test", "codex")
	}

	keyA, err := svc.CreateDownstreamKey("key-a", storage.GatewayAppCodex)
	if err != nil {
		t.Fatalf("create key A: %v", err)
	}
	keyB, err := svc.CreateDownstreamKey("key-b", storage.GatewayAppCodex)
	if err != nil {
		t.Fatalf("create key B: %v", err)
	}
	if err := svc.SetRouteExclusion(keyA.Key.ID, upstreams[0].ID, "gpt-test", true); err != nil {
		t.Fatalf("disable cheap for key A: %v", err)
	}
	if err := svc.SetRouteExclusion(keyB.Key.ID, upstreams[2].ID, "gpt-test", true); err != nil {
		t.Fatalf("disable expensive for key B: %v", err)
	}

	assertKeyPreview := func(keyID uint, wantOrder []string) {
		t.Helper()
		preview, _, err := svc.CandidatesForDownstreamKeyWithExclusions(keyID, storage.GatewayAppCodex, "gpt-test", true)
		if err != nil {
			t.Fatalf("preview key %d: %v", keyID, err)
		}
		if len(preview) != len(wantOrder) {
			t.Fatalf("preview key %d length = %d, want %d", keyID, len(preview), len(wantOrder))
		}
		for index, name := range wantOrder {
			if preview[index].Channel.Name != name {
				t.Fatalf("preview key %d candidate %d = %s, want %s", keyID, index, preview[index].Channel.Name, name)
			}
		}
		if preview[0].DisabledForKey || preview[1].DisabledForKey || !preview[2].DisabledForKey {
			t.Fatalf("preview key %d disabled flags = %t/%t/%t", keyID, preview[0].DisabledForKey, preview[1].DisabledForKey, preview[2].DisabledForKey)
		}
		if math.Abs(preview[0].PricePriorityScore-70) > 1e-9 || math.Abs(preview[1].PricePriorityScore-35) > 1e-9 {
			t.Fatalf("preview key %d available price scores = %.3f/%.3f, want 70/35", keyID, preview[0].PricePriorityScore, preview[1].PricePriorityScore)
		}
		disabled := preview[2]
		if disabled.PricePriorityScore != 0 || disabled.LatencyPriorityScore != 0 || disabled.Score != 1 {
			t.Fatalf("preview key %d disabled scores = total %.3f price %.3f latency %.3f", keyID, disabled.Score, disabled.PricePriorityScore, disabled.LatencyPriorityScore)
		}

		routable, _, err := svc.CandidatesForDownstreamKeyWithExclusions(keyID, storage.GatewayAppCodex, "gpt-test", false)
		if err != nil {
			t.Fatalf("real candidates key %d: %v", keyID, err)
		}
		if len(routable) != 2 || routable[0].Channel.Name != wantOrder[0] || routable[1].Channel.Name != wantOrder[1] {
			t.Fatalf("real candidates key %d = %+v", keyID, routable)
		}
		if routable[0].PricePriorityScore != preview[0].PricePriorityScore || routable[1].PricePriorityScore != preview[1].PricePriorityScore {
			t.Fatalf("preview changed real scores for key %d", keyID)
		}
	}

	assertKeyPreview(keyA.Key.ID, []string{"middle", "expensive", "cheap"})
	assertKeyPreview(keyB.Key.ID, []string{"cheap", "middle", "expensive"})
}

func TestRoutePreviewRejectsUnknownDownstreamKey(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), storage.NewGateway(db), nil, nil, nil)
	if _, _, err := svc.CandidatesForDownstreamKeyWithExclusions(999999, storage.GatewayAppCodex, "", true); err == nil {
		t.Fatal("unknown downstream key preview unexpectedly succeeded")
	}
}

func TestUpdateDownstreamKeyValidatesManualRoute(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	channel := storage.Channel{Name: "manual", Type: storage.ChannelTypeNewAPI, SiteURL: "https://manual.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt-test", Ratio: 0.1, CompletionRatio: 0.1}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	markGroupFamilies(t, store, channel.ID, "gpt-test", "codex")
	created, err := svc.CreateDownstreamKey("client", storage.GatewayAppCodex)
	if err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	off := false
	badGroup := "missing"
	if _, err := svc.UpdateDownstreamKey(created.Key.ID, nil, nil, nil, &off, &channel.ID, &badGroup); err == nil {
		t.Fatal("update with missing manual route succeeded")
	}
	got, err := store.FindDownstreamKey(created.Key.ID)
	if err != nil {
		t.Fatalf("find downstream key: %v", err)
	}
	if !got.SmartRouting {
		t.Fatal("invalid manual route should not disable smart routing")
	}
	goodGroup := "gpt-test"
	updated, err := svc.UpdateDownstreamKey(created.Key.ID, nil, nil, nil, &off, &channel.ID, &goodGroup)
	if err != nil {
		t.Fatalf("update valid manual route: %v", err)
	}
	if updated.SmartRouting || updated.ManualChannelID != channel.ID || updated.ManualGroupName != goodGroup {
		t.Fatalf("manual route not saved: %+v", updated)
	}
	if err := store.DisableUpstreamKey(channel.ID, goodGroup, "invalidated"); err != nil {
		t.Fatalf("disable upstream key: %v", err)
	}
	disabled := false
	updated, err = svc.UpdateDownstreamKey(created.Key.ID, nil, &disabled, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("disable downstream key with invalid manual route: %v", err)
	}
	if updated.Enabled {
		t.Fatal("downstream key should be disabled")
	}
	renamed := "renamed"
	updated, err = svc.UpdateDownstreamKey(created.Key.ID, &renamed, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("rename downstream key with invalid manual route: %v", err)
	}
	if updated.Name != renamed {
		t.Fatalf("name = %q, want %q", updated.Name, renamed)
	}
}

func TestCandidatesUseRecentLatencySamplesForChannelFallback(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	channel := storage.Channel{Name: "mixed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://mixed.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "new-group", Ratio: 0.1, CompletionRatio: 0.1}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	markGroupFamilies(t, store, channel.ID, "new-group", "codex")
	now := time.Now()
	for i := 0; i < 100; i++ {
		if err := store.AppendRequestLog(&storage.GatewayRequestLog{DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "old-stable", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, LatencyMs: 100, CreatedAt: now.Add(-time.Hour)}); err != nil {
			t.Fatalf("append stable log: %v", err)
		}
	}
	for i := 0; i < defaultRoutingLatencySampleSize; i++ {
		if err := store.AppendRequestLog(&storage.GatewayRequestLog{DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "recent", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, LatencyMs: 1000 + int64(i*10), CreatedAt: now.Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatalf("append recent log: %v", err)
		}
	}

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates len = %d, want 1", len(candidates))
	}
	want := float64(10450) / 10
	if candidates[0].AvgLatencyMs < want-0.001 || candidates[0].AvgLatencyMs > want+0.001 {
		t.Fatalf("fallback latency = %.3f, want %.3f", candidates[0].AvgLatencyMs, want)
	}
}

func TestCandidatesTreatInsufficientLatencyHistoryWithConfiguredDefault(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	withHistory := storage.Channel{Name: "with-history", Type: storage.ChannelTypeNewAPI, SiteURL: "https://with-history.example", PasswordCipher: "x", MonitorEnabled: true}
	withoutHistory := storage.Channel{Name: "without-history", Type: storage.ChannelTypeNewAPI, SiteURL: "https://without-history.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, ch := range []*storage.Channel{&withHistory, &withoutHistory} {
		if err := channels.Create(ch); err != nil {
			t.Fatalf("create channel %s: %v", ch.Name, err)
		}
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: withHistory.ID, ModelName: "gpt-test", Ratio: 0.2, CompletionRatio: 0.2},
		{ChannelID: withoutHistory.ID, ModelName: "gpt-test", Ratio: 0.1, CompletionRatio: 0.1},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, withHistory.ID, "gpt-test", "codex")
	markGroupFamilies(t, store, withoutHistory.ID, "gpt-test", "codex")
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{DownstreamKeyID: 1, ChannelID: withHistory.ID, GroupName: "gpt-test", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, LatencyMs: 100, CreatedAt: time.Now()}); err != nil {
		t.Fatalf("append request log: %v", err)
	}

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("candidates len = %d, want 2", len(candidates))
	}
	if candidates[0].Channel.ID != withoutHistory.ID {
		t.Fatalf("first candidate = %s, want without-history", candidates[0].Channel.Name)
	}
	if candidates[0].Score >= candidates[1].Score {
		t.Fatalf("scores = %.3f, %.3f; cheaper no-history candidate should still win", candidates[0].Score, candidates[1].Score)
	}
	for _, candidate := range candidates {
		if candidate.AvgLatencyMs != 0 {
			t.Fatalf("%s latency = %.3f, want ignored with fewer than %d samples", candidate.Channel.Name, candidate.AvgLatencyMs, defaultRoutingLatencySampleSize)
		}
		if math.Abs(candidate.LatencyPriorityScore-24) > 1e-9 {
			t.Fatalf("%s latency priority score = %.3f, want default 24", candidate.Channel.Name, candidate.LatencyPriorityScore)
		}
	}
}

func TestUnknownLatencyScoreFactorControlsLatencyPriority(t *testing.T) {
	candidates := []Candidate{{EffectivePrice: 1, AvgLatencyMs: 0}}
	scoreCandidatesWithConfig(candidates, RoutingConfig{LatencyScoreMax: 30, UnknownLatencyScoreFactor: 0.6})
	if got := candidates[0].LatencyPriorityScore; math.Abs(got-18) > 1e-9 {
		t.Fatalf("unknown latency priority score = %.3f, want 18", got)
	}
	if got := latencyPenaltyScore(0, RoutingConfig{}); math.Abs(got-0.2) > 1e-9 {
		t.Fatalf("default unknown latency penalty = %.3f, want 0.2", got)
	}
}

func TestCandidatesUseTrimmedMeanLatencyFilter(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	channel := storage.Channel{Name: "stable", Type: storage.ChannelTypeNewAPI, SiteURL: "https://stable.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "gpt-test", Ratio: 0.2, CompletionRatio: 0.2}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	markGroupFamilies(t, store, channel.ID, "gpt-test", "codex")

	now := time.Now()
	latencies := []int64{100, 102, 101, 98, 99, 100, 103, 101, 100, 5000}
	for _, latency := range latencies {
		if err := store.AppendRequestLog(&storage.GatewayRequestLog{DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "gpt-test", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, LatencyMs: latency, CreatedAt: now}); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates len = %d, want 1", len(candidates))
	}
	if candidates[0].AvgLatencyMs < 99 || candidates[0].AvgLatencyMs > 102 {
		t.Fatalf("trimmed latency = %.3f, want about 100", candidates[0].AvgLatencyMs)
	}
}

func TestCandidatesKeepAllChannelGroups(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	channel := storage.Channel{Name: "mixed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://mixed.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: channel.ID, ModelName: "expensive-group", Ratio: 1, CompletionRatio: 1},
		{ChannelID: channel.ID, ModelName: "cheap-group", Ratio: 0.1, CompletionRatio: 0.1},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, channel.ID, "expensive-group", "codex")
	markGroupFamilies(t, store, channel.ID, "cheap-group", "codex")

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("candidates len = %d, want 2", len(candidates))
	}
	if candidates[0].Rate.ModelName != "cheap-group" || candidates[1].Rate.ModelName != "expensive-group" {
		t.Fatalf("candidate order = %q, %q; want cheap-group, expensive-group", candidates[0].Rate.ModelName, candidates[1].Rate.ModelName)
	}
}

func TestCandidatesInferGatewayAppPerRateGroup(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	channel := storage.Channel{Name: "mixed", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: "https://mixed.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: channel.ID, ModelName: "codex-特价", Ratio: 0.1, CompletionRatio: 0.1},
		{ChannelID: channel.ID, ModelName: "claude-kiro", Ratio: 0.2, CompletionRatio: 0.2},
		{ChannelID: channel.ID, ModelName: "Krio90高缓存", Ratio: 0.25, CompletionRatio: 0.25},
		{ChannelID: channel.ID, ModelName: "gemini", Ratio: 0.28, CompletionRatio: 0.28},
		{ChannelID: channel.ID, ModelName: "通用分组", Ratio: 0.3, CompletionRatio: 0.3},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, channel.ID, "codex-特价", "codex")
	markGroupFamilies(t, store, channel.ID, "claude-kiro", "claude")
	markGroupFamilies(t, store, channel.ID, "Krio90高缓存", "claude")
	markGroupFamilies(t, store, channel.ID, "gemini", "gemini")
	markGroupFamilies(t, store, channel.ID, "通用分组", "codex")

	codexCandidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "")
	if err != nil {
		t.Fatalf("codex candidates: %v", err)
	}
	if len(codexCandidates) != 2 {
		t.Fatalf("codex candidates len = %d, want 2", len(codexCandidates))
	}
	if codexCandidates[0].Rate.ModelName != "codex-特价" || codexCandidates[1].Rate.ModelName != "通用分组" {
		t.Fatalf("codex groups = %q, %q; want codex-特价, 通用分组", codexCandidates[0].Rate.ModelName, codexCandidates[1].Rate.ModelName)
	}
	for _, candidate := range codexCandidates {
		if candidate.App != storage.GatewayAppCodex {
			t.Fatalf("codex candidate app = %q, want codex", candidate.App)
		}
	}

	claudeCandidates, err := svc.CandidatesForApp(storage.GatewayAppClaude, "")
	if err != nil {
		t.Fatalf("claude candidates: %v", err)
	}
	if len(claudeCandidates) != 2 {
		t.Fatalf("claude candidates len = %d, want 2", len(claudeCandidates))
	}
	if claudeCandidates[0].Rate.ModelName != "claude-kiro" || claudeCandidates[1].Rate.ModelName != "Krio90高缓存" {
		t.Fatalf("claude groups = %q, %q; want claude-kiro, Krio90高缓存", claudeCandidates[0].Rate.ModelName, claudeCandidates[1].Rate.ModelName)
	}
	for _, candidate := range claudeCandidates {
		if candidate.App != storage.GatewayAppClaude {
			t.Fatalf("claude candidate app = %q, want claude", candidate.App)
		}
	}

	geminiCandidates, err := svc.CandidatesForApp(storage.GatewayAppGemini, "")
	if err != nil {
		t.Fatalf("gemini candidates: %v", err)
	}
	if len(geminiCandidates) != 1 {
		t.Fatalf("gemini candidates len = %d, want 1", len(geminiCandidates))
	}
	if geminiCandidates[0].Rate.ModelName != "gemini" || geminiCandidates[0].App != storage.GatewayAppGemini {
		t.Fatalf("gemini candidate = %q/%q, want gemini/gemini", geminiCandidates[0].Rate.ModelName, geminiCandidates[0].App)
	}
}

func TestDownstreamKeyModelFamilyPinsOpenAICompatibleCandidates(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)
	channel := storage.Channel{Name: "mixed-families", Type: storage.ChannelTypeNewAPI, SiteURL: "https://mixed.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, item := range []struct {
		group    string
		families string
		modelIDs string
		ratio    float64
	}{
		{group: "gpt-group", families: "codex", modelIDs: "gpt-5", ratio: 0.2},
		{group: "grok-group", families: "grok", modelIDs: "grok-3", ratio: 0.1},
		{group: "mixed-group", families: "codex,grok", modelIDs: "gpt-5,grok-3", ratio: 0.05},
	} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: item.group, Ratio: item.ratio, CompletionRatio: item.ratio}); err != nil {
			t.Fatalf("upsert %s: %v", item.group, err)
		}
		key := &storage.GatewayUpstreamKey{
			ChannelID:         channel.ID,
			GroupName:         item.group,
			APIKeyCipher:      "encrypted",
			Enabled:           true,
			LatencyProbeModel: "",
		}
		if err := store.UpsertUpstreamKey(key); err != nil {
			t.Fatalf("upsert upstream key %s: %v", item.group, err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, item.group, "added", 1, "test-key", time.Now()); err != nil {
			t.Fatalf("record upstream generation %s: %v", item.group, err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, item.group)
		if err != nil {
			t.Fatalf("capture upstream generation %s: %v", item.group, err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, item.group, item.families, item.modelIDs, generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
			t.Fatalf("bind upstream model facts %s: %v", item.group, err)
		}
	}
	gptKey, err := svc.CreateDownstreamKeyWithFamily("gpt-only", storage.GatewayAppCodex, "codex")
	if err != nil {
		t.Fatalf("create gpt key: %v", err)
	}
	gptCandidates, _, err := svc.CandidatesForDownstreamKeyWithExclusions(gptKey.Key.ID, storage.GatewayAppCodex, "", false)
	if err != nil {
		t.Fatalf("gpt candidates: %v", err)
	}
	if len(gptCandidates) != 1 || gptCandidates[0].Rate.ModelName != "gpt-group" {
		t.Fatalf("gpt candidates = %+v, want only gpt-group", gptCandidates)
	}

	grokKey, err := svc.CreateDownstreamKeyWithFamily("grok-only", storage.GatewayAppCodex, "grok")
	if err != nil {
		t.Fatalf("create grok key: %v", err)
	}
	grokCandidates, _, err := svc.CandidatesForDownstreamKeyWithExclusions(grokKey.Key.ID, storage.GatewayAppCodex, "", false)
	if err != nil {
		t.Fatalf("grok candidates: %v", err)
	}
	if len(grokCandidates) != 1 || grokCandidates[0].Rate.ModelName != "grok-group" {
		t.Fatalf("grok candidates = %+v, want only grok-group", grokCandidates)
	}

	if _, err := svc.CreateDownstreamKeyWithFamily("invalid", storage.GatewayAppClaude, "grok"); err == nil {
		t.Fatal("claude key accepted grok model family")
	}
}

func TestModelIDSupportsRequestRequiresExactSyncedModel(t *testing.T) {
	if !modelIDSupportsRequest([]string{"deepseek-chat", "deepseek-reasoner"}, "deepseek-chat", storage.GatewayAppCodex) {
		t.Fatal("exact model id should be supported")
	}
	if modelIDSupportsRequest([]string{"deepseek-chat"}, "deepseek-r1", storage.GatewayAppCodex) {
		t.Fatal("same-family but absent model id should not be treated as supported")
	}
	if modelIDSupportsRequest([]string{"claude-sonnet-4"}, "claude-sonnet-4", storage.GatewayAppCodex) {
		t.Fatal("Claude model should not be routed through an OpenAI-compatible candidate")
	}
}

func TestCandidatesUseCachedModelFamiliesForGatewayApp(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	channel := storage.Channel{Name: "mixed", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppClaude, SiteURL: "https://mixed.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: channel.ID, ModelName: "gpt-4o", Description: "also mentions claude in notes", Ratio: 0.1, CompletionRatio: 0.1},
		{ChannelID: channel.ID, ModelName: "claude-sonnet", Description: "openai compatible proxy", Ratio: 0.2, CompletionRatio: 0.2},
		{ChannelID: channel.ID, ModelName: "通用分组", Description: "codex available", Ratio: 0.3, CompletionRatio: 0.3},
		{ChannelID: channel.ID, ModelName: "默认分组", Ratio: 0.4, CompletionRatio: 0.4},
		{ChannelID: channel.ID, ModelName: "claude-openai-compatible", Ratio: 0.5, CompletionRatio: 0.5},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, channel.ID, "gpt-4o", "codex")
	markGroupFamilies(t, store, channel.ID, "claude-sonnet", "claude")
	markGroupFamilies(t, store, channel.ID, "通用分组", "codex")
	markGroupFamilies(t, store, channel.ID, "默认分组", "claude")
	markGroupFamilies(t, store, channel.ID, "claude-openai-compatible", "claude")

	codexCandidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "")
	if err != nil {
		t.Fatalf("codex candidates: %v", err)
	}
	if len(codexCandidates) != 2 {
		t.Fatalf("codex candidates len = %d, want 2", len(codexCandidates))
	}
	if codexCandidates[0].Rate.ModelName != "gpt-4o" || codexCandidates[1].Rate.ModelName != "通用分组" {
		t.Fatalf("codex groups = %q, %q; want gpt-4o, 通用分组", codexCandidates[0].Rate.ModelName, codexCandidates[1].Rate.ModelName)
	}

	claudeCandidates, err := svc.CandidatesForApp(storage.GatewayAppClaude, "")
	if err != nil {
		t.Fatalf("claude candidates: %v", err)
	}
	if len(claudeCandidates) != 3 {
		t.Fatalf("claude candidates len = %d, want 3", len(claudeCandidates))
	}
	if claudeCandidates[0].Rate.ModelName != "claude-sonnet" || claudeCandidates[1].Rate.ModelName != "默认分组" || claudeCandidates[2].Rate.ModelName != "claude-openai-compatible" {
		t.Fatalf("claude groups = %q, %q, %q; want claude-sonnet, 默认分组, claude-openai-compatible", claudeCandidates[0].Rate.ModelName, claudeCandidates[1].Rate.ModelName, claudeCandidates[2].Rate.ModelName)
	}
}

func TestCandidatesSkipDisabledUpstreamKeyWithCachedFamilies(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	active := storage.Channel{Name: "active", Type: storage.ChannelTypeNewAPI, SiteURL: "https://active.example", PasswordCipher: "x", MonitorEnabled: true}
	disabled := storage.Channel{Name: "disabled", Type: storage.ChannelTypeNewAPI, SiteURL: "https://disabled.example", PasswordCipher: "x", MonitorEnabled: true}
	for _, ch := range []*storage.Channel{&active, &disabled} {
		if err := channels.Create(ch); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: ch.ID, ModelName: "gpt-test", Ratio: 0.1, CompletionRatio: 0.1}); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
		markGroupFamilies(t, store, ch.ID, "gpt-test", "codex")
	}
	if err := store.DisableUpstreamKey(disabled.ID, "gpt-test", "invalidated"); err != nil {
		t.Fatalf("disable upstream key: %v", err)
	}

	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Channel.ID != active.ID {
		t.Fatalf("candidates = %#v, want only active channel", candidates)
	}
}

func TestCandidatesSkipKnownUnsupportedModelGroup(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(channels, rates, store, cipher, nil, nil)

	channel := storage.Channel{Name: "mixed", Type: storage.ChannelTypeNewAPI, SiteURL: "https://mixed.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: channel.ID, ModelName: "cheap-group", Ratio: 0.1, CompletionRatio: 0.1},
		{ChannelID: channel.ID, ModelName: "fallback-group", Ratio: 0.5, CompletionRatio: 0.5},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	markGroupFamilies(t, store, channel.ID, "cheap-group", "codex")
	markGroupFamilies(t, store, channel.ID, "fallback-group", "codex")
	if err := store.MarkModelCapability(channel.ID, "cheap-group", "gpt-test", false, "unsupported"); err != nil {
		t.Fatalf("mark capability: %v", err)
	}

	candidates, exclusions, err := svc.CandidatesForAppWithExclusions(storage.GatewayAppCodex, "gpt-test")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("candidates len = %d, want 1", len(candidates))
	}
	if candidates[0].Rate.ModelName != "fallback-group" {
		t.Fatalf("candidate group = %q, want fallback-group", candidates[0].Rate.ModelName)
	}
	if len(exclusions) != 1 || exclusions[0].ChannelName != "mixed" || exclusions[0].GroupName != "cheap-group" || exclusions[0].Reason != "model_not_supported" {
		t.Fatalf("exclusions = %#v", exclusions)
	}
}

func TestEnsureUpstreamAPIKeyCachesPerGroup(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	keys := &fakeKeyManager{groups: []connector.APIKeyGroup{
		{Name: "cheap-group", Ratio: 0.1},
		{Name: "fallback-group", Ratio: 1},
	}}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "key cache", Type: storage.ChannelTypeNewAPI, SiteURL: "https://key-cache.invalid", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, rate := range []*storage.RateSnapshot{
		{ChannelID: channel.ID, ModelName: "cheap-group", Ratio: 0.1},
		{ChannelID: channel.ID, ModelName: "fallback-group", Ratio: 1},
	} {
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert group rate: %v", err)
		}
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)

	if _, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "cheap-group", nil, 0.1); err != nil {
		t.Fatalf("ensure cheap group: %v", err)
	}
	if _, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "fallback-group", nil, 1); err != nil {
		t.Fatalf("ensure fallback group: %v", err)
	}
	if len(keys.created) != 2 {
		t.Fatalf("created keys = %d, want 2", len(keys.created))
	}
	for _, request := range keys.created {
		if request.CustomKey != "" {
			t.Fatalf("upstream create request unexpectedly set custom key: %+v", request)
		}
	}
	if _, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "cheap-group", nil, 0.1); err != nil {
		t.Fatalf("ensure cached cheap group: %v", err)
	}
	if len(keys.created) != 2 {
		t.Fatalf("created keys after cache hit = %d, want 2", len(keys.created))
	}
	if keys.listGroupCalls != 2 {
		t.Fatalf("remote group lookups after cache hit = %d, want 2", keys.listGroupCalls)
	}
	stored, err := svc.Store.FindUpstreamKey(channel.ID, "cheap-group")
	if err != nil {
		t.Fatalf("find cached upstream key: %v", err)
	}
	if stored.UpstreamKeyID != 1 || stored.UpstreamKeyName != keys.created[0].Name || !strings.HasPrefix(stored.UpstreamKeyName, "BOG-p-") {
		t.Fatalf("cached upstream identity = %d/%q", stored.UpstreamKeyID, stored.UpstreamKeyName)
	}
}

func TestDownstreamKeyAuth(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	cipher, err := appcrypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), storage.NewGateway(db), cipher, nil, nil)

	created, err := svc.CreateDownstreamKey("client", storage.GatewayAppCodex)
	if err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	if created.Plain == "" {
		t.Fatal("plain key is empty")
	}
	if !strings.HasPrefix(created.Plain, "sk-briops-") {
		t.Fatalf("downstream key prefix = %q, want sk-briops-", created.Plain)
	}
	if !created.Key.SmartRouting {
		t.Fatal("created downstream key should default to smart routing")
	}
	got, err := svc.Authenticate("Bearer " + created.Plain)
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if got.ID != created.Key.ID {
		t.Fatalf("authenticated key id = %d, want %d", got.ID, created.Key.ID)
	}
	if _, err := svc.Authenticate("Bearer bad"); err == nil {
		t.Fatal("authenticate bad key succeeded")
	}
	if _, err := svc.Authenticate(created.Plain); err == nil {
		t.Fatal("authenticate bare key succeeded")
	}
}

func TestProbeConfigIncludesUnavailableChannelsAndGroupDiagnostics(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)
	svc.UpdateRoutingConfig(RoutingConfig{LatencySamples: 3, LatencyWindow: time.Hour})

	lowBalance := 1.0
	now := time.Now()
	items := []*storage.Channel{
		{Name: "healthy", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: "https://healthy.invalid", Username: "healthy-user", PasswordCipher: "secret", MonitorEnabled: true, LatencyProbeEnabled: true},
		{Name: "monitor-off", Type: storage.ChannelTypeSub2API, GatewayApp: storage.GatewayAppClaude, SiteURL: "https://monitor.invalid", Username: "monitor-user", PasswordCipher: "secret", MonitorEnabled: false, LatencyProbeEnabled: true},
		{Name: "channel-error", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: "https://error.invalid", Username: "error-user", PasswordCipher: "secret", MonitorEnabled: true, LatencyProbeEnabled: true, LastError: "login failed at https://error.invalid for error-user"},
		{Name: "low-balance", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppGemini, SiteURL: "https://balance.invalid", Username: "balance-user", PasswordCipher: "secret", MonitorEnabled: true, LatencyProbeEnabled: true, LastBalance: &lowBalance, LastBalanceAt: &now, BalanceThreshold: 2},
		{Name: "no-rate", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: "https://empty.invalid", Username: "empty-user", PasswordCipher: "secret", MonitorEnabled: true, LatencyProbeEnabled: true},
	}
	for _, channel := range items {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel %s: %v", channel.Name, err)
		}
	}
	items[1].MonitorEnabled = false
	if err := db.Model(&storage.Channel{}).Where("id = ?", items[1].ID).Update("monitor_enabled", false).Error; err != nil {
		t.Fatalf("disable channel monitor: %v", err)
	}

	addGroup := func(channel *storage.Channel, group, family, model string) {
		t.Helper()
		if _, err := rates.Upsert(&storage.RateSnapshot{
			ChannelID: channel.ID, ModelName: group, Description: group + " description",
			Ratio: 0.1, CompletionRatio: 0.2, LastSeenAt: now,
		}); err != nil {
			t.Fatalf("upsert rate %s/%s: %v", channel.Name, group, err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: group, APIKeyCipher: "private-cipher",
			APIKeyPrefix: "private-prefix", ModelFamilies: family, ModelIDs: model,
			LatencyProbeModel: model, ModelsSyncedAt: &now, Enabled: true,
		}); err != nil {
			t.Fatalf("upsert key %s/%s: %v", channel.Name, group, err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, group)
		if err != nil {
			t.Fatalf("capture group generation %s/%s: %v", channel.Name, group, err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, group, family, model, now.Add(time.Second), "", generation); err != nil {
			t.Fatalf("bind group model facts %s/%s: %v", channel.Name, group, err)
		}
	}
	addGroup(items[0], "primary", "codex", "gpt-mini")
	addGroup(items[0], "disabled", "codex", "gpt-disabled")
	addGroup(items[0], "unsupported", "alien-family", "alien-model")
	if err := store.UpdateLatencyProbeEnabled(items[0].ID, "unsupported", false); err != nil {
		t.Fatalf("disable group probe: %v", err)
	}
	if err := store.DisableUpstreamKey(items[0].ID, "disabled", "disabled after https://healthy.invalid failed"); err != nil {
		t.Fatalf("disable key: %v", err)
	}
	addGroup(items[1], "claude", "claude", "claude-haiku")
	addGroup(items[2], "errored", "codex", "gpt-error")
	addGroup(items[3], "gemini", "gemini", "gemini-flash-lite")
	failureAt := now.Add(-2 * time.Minute)
	healthFence := mustCurrentRoutingHealthFence(t, store, *items[0], "primary")
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(healthFence, RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "route failed at https://healthy.invalid using private-prefix", failureAt); err != nil {
		t.Fatalf("record routing failure: %v", err)
	}
	for index, latency := range []int64{1000, 2000, 3000} {
		if err := store.AppendRequestLog(&storage.GatewayRequestLog{
			ChannelID: items[0].ID, GroupName: "primary", Method: http.MethodPost,
			Path: "/v1/chat/completions", StatusCode: http.StatusOK, LatencyMs: latency,
			CreatedAt: now.Add(-time.Duration(index+1) * time.Second),
		}); err != nil {
			t.Fatalf("append latency log: %v", err)
		}
	}

	config, err := svc.ProbeConfig()
	if err != nil {
		t.Fatalf("probe config: %v", err)
	}
	if len(config) != len(items) {
		t.Fatalf("channel count = %d, want %d", len(config), len(items))
	}
	byName := make(map[string]ProbeConfigChannel, len(config))
	for _, channel := range config {
		byName[channel.ChannelName] = channel
	}
	for name, reason := range map[string]string{
		"monitor-off":   availabilityReasonMonitorDisabled,
		"channel-error": availabilityReasonChannelError,
		"low-balance":   availabilityReasonLowBalance,
		"no-rate":       availabilityReasonNoRateSnapshot,
	} {
		channel := byName[name]
		if channel.RoutingAvailable || !containsString(channel.AvailabilityReasons, reason) {
			t.Fatalf("channel %s availability = %t reasons=%v", name, channel.RoutingAvailable, channel.AvailabilityReasons)
		}
	}
	if got := byName["no-rate"].Groups; got == nil || len(got) != 0 {
		t.Fatalf("no-rate groups = %#v, want non-nil empty list", got)
	}
	healthy := byName["healthy"]
	if !healthy.RoutingAvailable || len(healthy.AvailabilityReasons) != 0 || len(healthy.Groups) != 3 {
		t.Fatalf("healthy channel = %+v", healthy)
	}
	groups := make(map[string]ProbeConfigGroup, len(healthy.Groups))
	for _, group := range healthy.Groups {
		groups[group.GroupName] = group
	}
	primary := groups["primary"]
	if !primary.ProbeEnabled || !primary.ProbeEligible || primary.Status != probeStatusReady || !primary.UpstreamKeyConfigured || !primary.UpstreamKeyEnabled {
		t.Fatalf("primary eligibility = %+v", primary)
	}
	if !primary.LatencyQualified || primary.LatencySource != latencySourceGroup || primary.LatencySamples != 3 || len(primary.LatencyRawSamples) != 3 || primary.AvgLatencyMs != 2000 {
		t.Fatalf("primary latency = %+v", primary)
	}
	if primary.LastRoutingFailureKind != RoutingFailureHTTPStatus || primary.LastRoutingFailureStatus != http.StatusServiceUnavailable || primary.LastRoutingFailureAt == nil {
		t.Fatalf("primary routing failure = %+v", primary)
	}
	if primary.LastError != "" || primary.LastRoutingFailureError == "" || strings.Contains(primary.LastRoutingFailureError, "https://") || strings.Contains(primary.LastRoutingFailureError, "private-prefix") {
		t.Fatalf("primary routing error was not isolated and safely redacted: key=%q routing=%q", primary.LastError, primary.LastRoutingFailureError)
	}
	disabled := groups["disabled"]
	if disabled.ProbeEligible || disabled.UpstreamKeyConfigured || disabled.UpstreamKeyEnabled || !containsString(disabled.ProbeBlockers, probeBlockerKeyDisabled) {
		t.Fatalf("disabled group = %+v", disabled)
	}
	unsupported := groups["unsupported"]
	if unsupported.ProbeEnabled || unsupported.ProbeEligible || len(unsupported.GatewayApps) != 0 || !containsString(unsupported.ProbeBlockers, probeBlockerProbeDisabled) || !containsString(unsupported.ProbeBlockers, probeBlockerUnsupportedApp) {
		t.Fatalf("unsupported group = %+v", unsupported)
	}
	if byName["channel-error"].LastError == "" || strings.Contains(byName["channel-error"].LastError, "https://") || strings.Contains(byName["channel-error"].LastError, "error-user") {
		t.Fatalf("channel error was not safely redacted: %q", byName["channel-error"].LastError)
	}
}

func TestChannelAvailabilityUsesMinimumBalanceThreshold(t *testing.T) {
	belowFloor := storage.MinimumChannelBalanceThreshold - 0.001
	atFloor := storage.MinimumChannelBalanceThreshold
	aboveFloor := storage.MinimumChannelBalanceThreshold + 0.001

	for _, tc := range []struct {
		name      string
		channel   storage.Channel
		wantLow   bool
		threshold float64
	}{
		{name: "legacy zero threshold below floor", channel: storage.Channel{MonitorEnabled: true, LastBalance: &belowFloor}, wantLow: true, threshold: storage.MinimumChannelBalanceThreshold},
		{name: "exact floor remains available", channel: storage.Channel{MonitorEnabled: true, LastBalance: &atFloor}, wantLow: false, threshold: storage.MinimumChannelBalanceThreshold},
		{name: "above floor remains available", channel: storage.Channel{MonitorEnabled: true, LastBalance: &aboveFloor}, wantLow: false, threshold: storage.MinimumChannelBalanceThreshold},
		{name: "operator threshold is preserved", channel: storage.Channel{MonitorEnabled: true, LastBalance: &aboveFloor, BalanceThreshold: 1}, wantLow: true, threshold: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reasons := ChannelAvailabilityReasons(tc.channel)
			if got := containsString(reasons, availabilityReasonLowBalance); got != tc.wantLow {
				t.Fatalf("low-balance reason = %t, want %t; reasons=%v", got, tc.wantLow, reasons)
			}
			if got := tc.channel.EffectiveBalanceThreshold(); got != tc.threshold {
				t.Fatalf("effective threshold = %v, want %v", got, tc.threshold)
			}
		})
	}
}

func TestLowBalanceSuspendsEveryChannelGroupAndRecoversAtThreshold(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := NewService(channels, rates, store, nil, nil, nil)

	lowBalance := storage.MinimumChannelBalanceThreshold - 0.01
	channel := &storage.Channel{
		Name: "low-balance-all-groups", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://low-balance.invalid", PasswordCipher: "secret",
		MonitorEnabled: true, LastBalance: &lowBalance, LastBalanceAt: ptrTime(time.Now()),
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	for _, groupName := range []string{"gpt-primary", "gpt-secondary"} {
		if _, err := rates.Upsert(&storage.RateSnapshot{
			ChannelID: channel.ID, ModelName: groupName, Ratio: 1, CompletionRatio: 1,
		}); err != nil {
			t.Fatalf("upsert rate %s: %v", groupName, err)
		}
		markGroupFamilies(t, store, channel.ID, groupName, "codex")
	}

	if candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, ""); err == nil || len(candidates) != 0 {
		t.Fatalf("low-balance channel remained routable: candidates=%+v err=%v", candidates, err)
	}
	manual, err := svc.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "gpt-primary", "")
	if err != nil || len(manual) != 1 || manual[0].Channel.ID != channel.ID || manual[0].Rate.ModelName != "gpt-primary" {
		t.Fatalf("low-balance fixed route was blocked: candidates=%+v err=%v", manual, err)
	}
	config, err := svc.ProbeConfig()
	if err != nil || len(config) != 1 || config[0].RoutingAvailable {
		t.Fatalf("low-balance probe config = %+v err=%v", config, err)
	}
	for _, group := range config[0].Groups {
		if !containsString(group.ProbeBlockers, availabilityReasonLowBalance) {
			t.Fatalf("group %q missing low-balance blocker: %+v", group.GroupName, group.ProbeBlockers)
		}
	}

	if err := channels.UpdateBalance(channel.ID, storage.MinimumChannelBalanceThreshold, time.Now(), ""); err != nil {
		t.Fatalf("restore balance: %v", err)
	}
	candidates, err := svc.CandidatesForApp(storage.GatewayAppCodex, "")
	if err != nil || len(candidates) != 2 {
		t.Fatalf("restored channel candidates = %+v err=%v", candidates, err)
	}
	manual, err = svc.ManualCandidateForApp(0, storage.GatewayAppCodex, channel.ID, "gpt-primary", "")
	if err != nil || len(manual) != 1 {
		t.Fatalf("restored manual candidate = %+v err=%v", manual, err)
	}
}

func TestProbeConfigReturnsEmptySliceForEmptyDatabase(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	svc := NewService(storage.NewChannels(db), storage.NewRates(db), storage.NewGateway(db), nil, nil, nil)
	config, err := svc.ProbeConfig()
	if err != nil {
		t.Fatalf("probe config: %v", err)
	}
	if config == nil || len(config) != 0 {
		t.Fatalf("empty config = %#v, want non-nil empty list", config)
	}
}

func TestProbeConfigErrorSummaryRequiresHTTPStatusContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		want string
	}{
		{name: "http prefix", text: "upstream returned HTTP 503", want: "上游请求失败 HTTP 503"},
		{name: "status prefix", text: "request failed with status: 429", want: "上游请求失败 HTTP 429"},
		{name: "duration is not status", text: "request timeout after 500ms", want: "上游请求超时"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := probeConfigErrorSummary(tc.text); got != tc.want {
				t.Fatalf("probeConfigErrorSummary(%q) = %q, want %q", tc.text, got, tc.want)
			}
		})
	}
}

func TestDownstreamKeyTestTokenIsBoundAndSingleUse(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	store := storage.NewGateway(db)
	key := storage.GatewayDownstreamKey{
		Name:         "test-token-key",
		App:          storage.GatewayAppCodex,
		KeyHash:      "test-token-hash",
		APIKeyCipher: "cipher",
		KeyPrefix:    "sk-test",
		Enabled:      true,
	}
	if err := store.CreateDownstreamKey(&key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	svc := NewService(nil, nil, store, nil, nil, nil)

	token, err := svc.IssueDownstreamKeyTestToken(key.ID)
	if err != nil {
		t.Fatalf("issue test token: %v", err)
	}
	if svc.ConsumeDownstreamKeyTestToken(token, key.ID+1) {
		t.Fatal("test token was accepted for a different downstream key")
	}

	token, err = svc.IssueDownstreamKeyTestToken(key.ID)
	if err != nil {
		t.Fatalf("issue second test token: %v", err)
	}
	if !svc.ConsumeDownstreamKeyTestToken(token, key.ID) {
		t.Fatal("matching downstream key did not consume test token")
	}
	if svc.ConsumeDownstreamKeyTestToken(token, key.ID) {
		t.Fatal("test token was accepted more than once")
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestCacheAdjustedCandidatePriceOnlyDiscountsCachedInput(t *testing.T) {
	rate := storage.RateSnapshot{Ratio: 2, CompletionRatio: 4}
	if got := effectiveCandidatePriceWithCache(rate, 0, 0.1); math.Abs(got-3) > 1e-9 {
		t.Fatalf("uncached effective price = %v, want 3", got)
	}
	if got := effectiveCandidatePriceWithCache(rate, 1, 0.1); math.Abs(got-2.1) > 1e-9 {
		t.Fatalf("fully cached effective price = %v, want 2.1", got)
	}
	if got := effectiveCandidatePriceWithCache(rate, 0.25, 0.1); math.Abs(got-2.775) > 1e-9 {
		t.Fatalf("partially cached effective price = %v, want 2.775", got)
	}
}

func TestDefaultCachePricingKeepsUpstreamRatio(t *testing.T) {
	rate := storage.RateSnapshot{Ratio: 0.08}
	for _, cacheRatio := range []float64{0, 0.5, 1} {
		got := effectiveCandidatePriceWithCache(rate, cacheRatio, DefaultRoutingConfig().CacheInputPriceMultiplier)
		if math.Abs(got-rate.Ratio) > 1e-9 {
			t.Fatalf("cache ratio %v changed default effective price to %v, want %v", cacheRatio, got, rate.Ratio)
		}
	}
}

func TestNormalizeRoutingConfigCacheDefaultsAndZeroMultiplier(t *testing.T) {
	cfg := NormalizeRoutingConfig(RoutingConfig{CacheInputPriceMultiplier: 0})
	if cfg.CacheInputPriceMultiplier != 0 {
		t.Fatalf("zero cache multiplier was not preserved: %v", cfg.CacheInputPriceMultiplier)
	}
	if cfg.CachePriceMinInputTokens != defaultRoutingCachePriceMinInputTokens || cfg.CachePriceWindowInputTokens != defaultRoutingCachePriceWindowInputTokens || cfg.CacheStatsMaxAge != defaultRoutingCacheStatsMaxAge {
		t.Fatalf("cache defaults = %+v", cfg)
	}
	cfg = NormalizeRoutingConfig(RoutingConfig{CacheInputPriceMultiplier: 2, CachePriceMinInputTokens: maximumRoutingCachePriceInputTokens + 1, CachePriceWindowInputTokens: maximumRoutingCachePriceInputTokens + 1, CacheStatsMaxAge: maximumRoutingCacheStatsMaxAge + time.Hour})
	if cfg.CacheInputPriceMultiplier != defaultRoutingCacheInputPriceMultiplier || cfg.CachePriceMinInputTokens != defaultRoutingCachePriceMinInputTokens || cfg.CachePriceWindowInputTokens != defaultRoutingCachePriceWindowInputTokens || cfg.CacheStatsMaxAge != defaultRoutingCacheStatsMaxAge {
		t.Fatalf("invalid cache values were not normalized: %+v", cfg)
	}
}

func TestCacheRatioForCandidateUsesAverageForUnobservedAndZeroForExpired(t *testing.T) {
	key := candidateLatencyKey{ChannelID: 9, GroupName: "pro"}
	other := candidateLatencyKey{ChannelID: 10, GroupName: "max"}
	snapshot := cacheRatioSnapshot{
		Ratios:  map[candidateLatencyKey]float64{key: 0.75},
		Average: 0.4, HasAverage: true,
		Expired: map[candidateLatencyKey]struct{}{other: {}},
	}
	if ratio, known := cacheRatioForCandidate(snapshot, key); math.Abs(ratio-0.75) > 1e-9 || !known {
		t.Fatalf("observed cache ratio = %v/%v", ratio, known)
	}
	if ratio, known := cacheRatioForCandidate(snapshot, candidateLatencyKey{ChannelID: 11, GroupName: "new"}); math.Abs(ratio-0.4) > 1e-9 || !known {
		t.Fatalf("unobserved cache ratio = %v/%v, want average", ratio, known)
	}
	if ratio, known := cacheRatioForCandidate(snapshot, other); ratio != 0 || !known {
		t.Fatalf("expired cache ratio = %v/%v, want known zero", ratio, known)
	}
}

func TestCachedRoutingCacheSnapshotRechecksMaxAge(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	configKey := cacheStatsConfigKey{MinTokens: 100, MaxAge: 7 * 24 * time.Hour}
	fresh := candidateLatencyKey{ChannelID: 1, GroupName: "fresh"}
	expired := candidateLatencyKey{ChannelID: 2, GroupName: "expired"}
	unobserved := candidateLatencyKey{ChannelID: 3, GroupName: "unobserved"}
	missingTimestamp := candidateLatencyKey{ChannelID: 4, GroupName: "missing-timestamp"}
	svc := &Service{
		cacheStatsConfig: configKey,
		cacheStats: map[candidateLatencyKey]cacheRatioStat{
			fresh:            {Ratio: 0.5, PromptTokens: 100, CacheTokens: 50, LastInputAt: now.Add(-time.Hour)},
			expired:          {Ratio: 0.9, PromptTokens: 1_000, CacheTokens: 900, LastInputAt: now.Add(-8 * 24 * time.Hour)},
			missingTimestamp: {Ratio: 0.9, PromptTokens: 1_000, CacheTokens: 900},
		},
	}
	snapshot := svc.cachedRoutingCacheSnapshotLocked(configKey, now)
	assertCacheRatio(t, snapshot, fresh, 0.5, true)
	assertCacheRatio(t, snapshot, expired, 0, true)
	assertCacheRatio(t, snapshot, missingTimestamp, 0, true)
	// The expired high-volume route must also be removed from the inherited
	// average when a published snapshot crosses the max-age boundary.
	assertCacheRatio(t, snapshot, unobserved, 0.5, true)
}

func TestRoutingCacheRatiosRefreshesAsynchronouslyAndCoalescesConcurrentReaders(t *testing.T) {
	routes := []storage.GatewayRequestGroupCacheRoutingRoute{
		{ChannelID: 1, GroupName: "threshold"},
		{ChannelID: 2, GroupName: "weighted"},
		{ChannelID: 3, GroupName: "small"},
		{ChannelID: 4, GroupName: "expired"},
		{ChannelID: 5, GroupName: "new"},
	}
	routing := DefaultRoutingConfig()
	routing.CachePriceMinInputTokens = 100
	routing.CachePriceWindowInputTokens = 100
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	var calls atomic.Int64
	svc := &Service{Store: &storage.Gateway{}, cacheStats: make(map[candidateLatencyKey]cacheRatioStat)}
	svc.cacheStatsLoader = func(ctx context.Context, now time.Time, _ time.Duration, window int64, gotRoutes []storage.GatewayRequestGroupCacheRoutingRoute) ([]storage.GatewayRequestGroupCacheRoutingStat, error) {
		calls.Add(1)
		startOnce.Do(func() { close(started) })
		if window != 100 || len(gotRoutes) != len(routes) {
			return nil, errors.New("unexpected cache refresh request")
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []storage.GatewayRequestGroupCacheRoutingStat{
			{ChannelID: 1, GroupName: "threshold", PromptTokens: 100, CacheReadTokens: 50, LastInputAt: now},
			{ChannelID: 2, GroupName: "weighted", PromptTokens: 300, CacheReadTokens: 30, LastInputAt: now},
			{ChannelID: 3, GroupName: "small", PromptTokens: 50, CacheReadTokens: 50, LastInputAt: now},
			{ChannelID: 4, GroupName: "expired", LastInputAt: now.Add(-8 * 24 * time.Hour), Expired: true},
		}, nil
	}

	const readers = 32
	var readersDone sync.WaitGroup
	readersDone.Add(readers)
	for range readers {
		go func() {
			defer readersDone.Done()
			snapshot := svc.routingCacheRatios(routing, routes)
			if snapshot.HasAverage || len(snapshot.Ratios) != 0 {
				t.Errorf("cold hot-path snapshot = %+v", snapshot)
			}
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("asynchronous cache refresh did not start")
	}
	readersDone.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent readers started %d refreshes, want 1", calls.Load())
	}
	close(release)
	waitForRoutingCacheRefresh(t, svc)
	if calls.Load() != 1 {
		t.Fatalf("coalesced refresh calls = %d, want 1", calls.Load())
	}

	snapshot := svc.routingCacheRatios(routing, routes)
	// Weighted average = (50 + 30) / (100 + 300) = 0.2.
	assertCacheRatio(t, snapshot, candidateLatencyKey{ChannelID: 1, GroupName: "threshold"}, 0.5, true)
	assertCacheRatio(t, snapshot, candidateLatencyKey{ChannelID: 2, GroupName: "weighted"}, 0.1, true)
	assertCacheRatio(t, snapshot, candidateLatencyKey{ChannelID: 3, GroupName: "small"}, 0.2, true)
	assertCacheRatio(t, snapshot, candidateLatencyKey{ChannelID: 4, GroupName: "expired"}, 0, true)
	assertCacheRatio(t, snapshot, candidateLatencyKey{ChannelID: 5, GroupName: "new"}, 0.2, true)
}

func TestRoutingCacheRatiosPublishedSnapshotFailsClosedAfterRefreshFailure(t *testing.T) {
	routes := []storage.GatewayRequestGroupCacheRoutingRoute{{ChannelID: 6, GroupName: "pro"}}
	key := candidateLatencyKey{ChannelID: 6, GroupName: "pro"}
	routing := DefaultRoutingConfig()
	routing.CachePriceMinInputTokens = 10
	var calls atomic.Int64
	svc := &Service{Store: &storage.Gateway{}, cacheStats: make(map[candidateLatencyKey]cacheRatioStat)}
	svc.cacheStatsLoader = func(_ context.Context, now time.Time, _ time.Duration, _ int64, _ []storage.GatewayRequestGroupCacheRoutingRoute) ([]storage.GatewayRequestGroupCacheRoutingStat, error) {
		if calls.Add(1) == 1 {
			return []storage.GatewayRequestGroupCacheRoutingStat{{ChannelID: 6, GroupName: "pro", PromptTokens: 10, CacheReadTokens: 8, LastInputAt: time.Now().UTC()}}, nil
		}
		return nil, errors.New("persistent aggregate read failure")
	}

	_ = svc.routingCacheRatios(routing, routes)
	waitForRoutingCacheRefresh(t, svc)
	assertCacheRatio(t, svc.routingCacheRatios(routing, routes), key, 0.8, true)

	svc.cacheStatsMu.Lock()
	svc.cacheStatsAt = time.Now().Add(-routingCacheStatsRefreshInterval - time.Second)
	svc.cacheStatsMu.Unlock()
	_ = svc.routingCacheRatios(routing, routes)
	waitForRoutingCacheRefresh(t, svc)
	if calls.Load() != 2 {
		t.Fatalf("failed refresh calls = %d, want 2", calls.Load())
	}
	assertCacheRatio(t, svc.routingCacheRatios(routing, routes), key, 0, false)

	// Backoff and subsequent failures must never republish the old discount.
	svc.cacheStatsMu.Lock()
	svc.cacheStatsFailureAt = time.Now().Add(-routingCacheStatsFailureBackoff - time.Second)
	svc.cacheStatsMu.Unlock()
	_ = svc.routingCacheRatios(routing, routes)
	waitForRoutingCacheRefresh(t, svc)
	if calls.Load() != 3 {
		t.Fatalf("persistent failed refresh calls = %d, want 3", calls.Load())
	}
	assertCacheRatio(t, svc.routingCacheRatios(routing, routes), key, 0, false)
}

func TestRoutingCacheRatiosFailureBackoffAndRetry(t *testing.T) {
	routes := []storage.GatewayRequestGroupCacheRoutingRoute{{ChannelID: 7, GroupName: "pro"}}
	routing := DefaultRoutingConfig()
	routing.CachePriceMinInputTokens = 10
	var calls atomic.Int64
	svc := &Service{Store: &storage.Gateway{}, cacheStats: make(map[candidateLatencyKey]cacheRatioStat)}
	svc.cacheStatsLoader = func(_ context.Context, now time.Time, _ time.Duration, _ int64, _ []storage.GatewayRequestGroupCacheRoutingRoute) ([]storage.GatewayRequestGroupCacheRoutingStat, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("temporary aggregate read failure")
		}
		return []storage.GatewayRequestGroupCacheRoutingStat{{ChannelID: 7, GroupName: "pro", PromptTokens: 10, CacheReadTokens: 4, LastInputAt: now}}, nil
	}

	_ = svc.routingCacheRatios(routing, routes)
	waitForRoutingCacheRefresh(t, svc)
	if calls.Load() != 1 {
		t.Fatalf("first failed refresh calls = %d", calls.Load())
	}
	_ = svc.routingCacheRatios(routing, routes)
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("failure backoff allowed immediate retry: %d calls", calls.Load())
	}
	svc.cacheStatsMu.Lock()
	svc.cacheStatsFailureAt = time.Now().Add(-routingCacheStatsFailureBackoff - time.Second)
	svc.cacheStatsMu.Unlock()
	_ = svc.routingCacheRatios(routing, routes)
	waitForRoutingCacheRefresh(t, svc)
	if calls.Load() != 2 {
		t.Fatalf("post-backoff refresh calls = %d, want 2", calls.Load())
	}
	assertCacheRatio(t, svc.routingCacheRatios(routing, routes), candidateLatencyKey{ChannelID: 7, GroupName: "pro"}, 0.4, true)
}

func TestRoutingCacheRatiosConfigurationChangeDiscardsInflightSnapshot(t *testing.T) {
	routes := []storage.GatewayRequestGroupCacheRoutingRoute{{ChannelID: 9, GroupName: "pro"}}
	oldRouting := DefaultRoutingConfig()
	oldRouting.CachePriceMinInputTokens = 100
	newRouting := oldRouting
	newRouting.CachePriceMinInputTokens = 200
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	var oldOnce sync.Once
	var calls atomic.Int64
	svc := &Service{Store: &storage.Gateway{}, cacheStats: make(map[candidateLatencyKey]cacheRatioStat)}
	svc.cacheStatsLoader = func(ctx context.Context, now time.Time, _ time.Duration, _ int64, _ []storage.GatewayRequestGroupCacheRoutingRoute) ([]storage.GatewayRequestGroupCacheRoutingStat, error) {
		calls.Add(1)
		if calls.Load() == 1 {
			oldOnce.Do(func() { close(oldStarted) })
			select {
			case <-releaseOld:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return []storage.GatewayRequestGroupCacheRoutingStat{{ChannelID: 9, GroupName: "pro", PromptTokens: 100, CacheReadTokens: 90, LastInputAt: now}}, nil
		}
		return []storage.GatewayRequestGroupCacheRoutingStat{{ChannelID: 9, GroupName: "pro", PromptTokens: 200, CacheReadTokens: 40, LastInputAt: now}}, nil
	}

	_ = svc.routingCacheRatios(oldRouting, routes)
	select {
	case <-oldStarted:
	case <-time.After(time.Second):
		t.Fatal("old configuration refresh did not start")
	}
	svc.UpdateRoutingConfig(newRouting)
	_ = svc.routingCacheRatios(newRouting, routes)
	close(releaseOld)
	waitForRoutingCacheRefresh(t, svc)
	if calls.Load() != 2 {
		t.Fatalf("configuration change refresh calls = %d, want 2", calls.Load())
	}
	assertCacheRatio(t, svc.routingCacheRatios(newRouting, routes), candidateLatencyKey{ChannelID: 9, GroupName: "pro"}, 0.2, true)
	svc.cacheStatsMu.RLock()
	loadedMinTokens := svc.cacheStatsConfig.MinTokens
	svc.cacheStatsMu.RUnlock()
	if loadedMinTokens != 200 {
		t.Fatalf("published cache configuration min tokens = %d, want 200", loadedMinTokens)
	}
}

func waitForRoutingCacheRefresh(t *testing.T, svc *Service) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		svc.cacheStatsMu.RLock()
		refreshing := svc.cacheStatsRefreshing
		svc.cacheStatsMu.RUnlock()
		if !refreshing {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for routing cache refresh")
}

func assertCacheRatio(t *testing.T, snapshot cacheRatioSnapshot, key candidateLatencyKey, want float64, wantKnown bool) {
	t.Helper()
	got, known := cacheRatioForCandidate(snapshot, key)
	if math.Abs(got-want) > 1e-9 || known != wantKnown {
		t.Fatalf("cache ratio for %+v = %v/%v, want %v/%v (snapshot=%+v)", key, got, known, want, wantKnown, snapshot)
	}
}

func latencyProbeTestCandidateGeneration(t *testing.T, store *storage.Gateway, candidate Candidate) Candidate {
	t.Helper()
	if candidate.Channel.UpstreamAccountIdentityRevision == 0 {
		candidate.Channel.UpstreamAccountIdentityRevision = 1
	}
	if candidate.UpstreamGeneration.Valid() {
		return candidate
	}
	if err := store.RecordUpstreamGroupChange(candidate.Channel.ID, candidate.Rate.ModelName, "added", 1, "BOG latency probe", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatal(err)
	}
	candidate.UpstreamGeneration = generation
	return candidate
}
