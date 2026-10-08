package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

type deadlineIsolatingKeyManager struct {
	*fakeKeyManager
	timedOutGroup string
	attempted     []string
}

func (m *deadlineIsolatingKeyManager) CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	m.attempted = append(m.attempted, req.Group)
	if req.Group == m.timedOutGroup {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > managedKeyRemoteOperationTimeout+100*time.Millisecond {
			return nil, errors.New("managed key create did not receive the bounded group deadline")
		}
		return nil, context.DeadlineExceeded
	}
	return m.fakeKeyManager.CreateAPIKey(ctx, channelID, req)
}

func TestApplyUpstreamGroupFactsReconcilesManagedInventoryAndCreatesMissingGroupKey(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "apply-group-facts")
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "standard", Ratio: 2}); err != nil {
		t.Fatal(err)
	}
	managedOrphan := managedUpstreamKeyFixture(40, upstreamProvisioningKeyPrefix+"aaaaaaaaaaaaaaaa", "removed")
	manual := managedUpstreamKeyFixture(41, "manual-key", "removed")
	remote := map[int64]connector.APIKey{managedOrphan.ID: managedOrphan, manual.ID: manual}
	var listAPIKeyCalls int
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}, {Name: "standard", Ratio: 2}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			listAPIKeyCalls++
			items := make([]connector.APIKey, 0, len(remote))
			for _, item := range remote {
				items = append(items, item)
			}
			return &connector.APIKeyPage{Page: 1, Pages: 1, Total: int64(len(items)), Items: items}, nil
		},
	}
	keys.deleteAPIKey = func(id int64) error {
		delete(remote, id)
		return nil
	}

	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.ApplyUpstreamGroupFacts(context.Background(), channel.ID); err != nil {
		t.Fatalf("apply upstream group facts: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != managedOrphan.ID {
		t.Fatalf("deleted managed keys = %v, want [%d]", keys.deleted, managedOrphan.ID)
	}
	if _, ok := remote[manual.ID]; !ok {
		t.Fatal("manual upstream key was deleted")
	}
	if len(keys.created) != 2 || keys.created[0].Group != "promo" || keys.created[1].Group != "standard" {
		t.Fatalf("created managed keys = %+v", keys.created)
	}
	if listAPIKeyCalls != 1 {
		t.Fatalf("ListAPIKeys calls = %d, want 1 complete inventory read", listAPIKeyCalls)
	}
	if keys.listGroupCalls != 1 {
		t.Fatalf("ListAPIKeyGroups calls = %d, want 1 per maintenance pass", keys.listGroupCalls)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !stored.Enabled || stored.Provisioning || strings.TrimSpace(stored.APIKeyCipher) == "" {
		t.Fatalf("created managed key state = %+v, err=%v", stored, err)
	}
}

func TestApplyUpstreamGroupFactsDeletesManagedKeyWhenRemovedSub2APIGroupNameIsOmitted(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForType(t, "apply-removed-sub2api-group", storage.ChannelTypeSub2API)
	managedName := upstreamProvisioningKeyPrefix + "dddddddddddddddd"
	plain := "sk-removed-sub2api-group"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "removed", GroupRatio: 1,
		UpstreamKeyID: 70, UpstreamKeyName: managedName,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        encrypted, APIKeyPrefix: keyPrefix(plain), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "removed", "added", 70, managedName, time.Now()); err != nil {
		t.Fatal(err)
	}
	removedGroupID := int64(999)
	remote := managedSub2APIKeyFixture(70, managedName, "")
	remote.GroupID = &removedGroupID
	remote.GroupDirectoryComplete = true
	remoteInventory := map[int64]connector.APIKey{remote.ID: remote}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			items := make([]connector.APIKey, 0, len(remoteInventory))
			for _, item := range remoteInventory {
				items = append(items, item)
			}
			return &connector.APIKeyPage{Page: 1, Pages: 1, Total: int64(len(items)), Items: items}, nil
		},
	}
	keys.deleteAPIKey = func(id int64) error {
		delete(remoteInventory, id)
		return nil
	}

	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.ApplyUpstreamGroupFacts(context.Background(), channel.ID); err != nil {
		t.Fatalf("apply group facts with removed Sub2API group: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != remote.ID {
		t.Fatalf("deleted managed keys = %v, want [%d]", keys.deleted, remote.ID)
	}
	if len(keys.created) != 1 || keys.created[0].Group != "promo" {
		t.Fatalf("created active managed keys = %+v", keys.created)
	}
	removedLocal, err := store.FindUpstreamKeyAny(channel.ID, "removed")
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("removed managed key local binding = %+v, err=%v", removedLocal, err)
	}
	created := managedSub2APIKeyFixture(1, keys.created[0].Name, "promo")
	remoteInventory[created.ID] = created
	if err := service.ApplyUpstreamGroupFacts(context.Background(), channel.ID); err != nil {
		t.Fatalf("repeat apply group facts: %v", err)
	}
	if len(keys.deleted) != 1 || len(keys.created) != 1 {
		t.Fatalf("repeat apply was not idempotent: deleted=%v created=%+v", keys.deleted, keys.created)
	}
}

func TestApplyUpstreamGroupFactsDeletesManagedKeyForRemovedSub2APIGroupWithoutLocalBinding(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureForType(t, "apply-removed-sub2api-unbound", storage.ChannelTypeSub2API)
	managedName := upstreamProvisioningKeyPrefix + "eeeeeeeeeeeeeeee"
	removedGroupID := int64(998)
	remote := managedSub2APIKeyFixture(71, managedName, "")
	remote.GroupID = &removedGroupID
	remote.GroupDirectoryComplete = true
	remoteInventory := map[int64]connector.APIKey{remote.ID: remote}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			items := make([]connector.APIKey, 0, len(remoteInventory))
			for _, item := range remoteInventory {
				items = append(items, item)
			}
			return &connector.APIKeyPage{Page: 1, Pages: 1, Total: int64(len(items)), Items: items}, nil
		},
	}
	keys.deleteAPIKey = func(id int64) error {
		delete(remoteInventory, id)
		return nil
	}

	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.ApplyUpstreamGroupFacts(context.Background(), channel.ID); err != nil {
		t.Fatalf("apply unbound removed Sub2API group: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != remote.ID {
		t.Fatalf("deleted unbound managed keys = %v, want [%d]", keys.deleted, remote.ID)
	}
	if len(keys.created) != 1 || keys.created[0].Group != "promo" {
		t.Fatalf("created active managed keys = %+v", keys.created)
	}
}

func TestApplyUpstreamGroupFactsReplacesUpstreamModifiedManagedKey(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "apply-modified-key")
	plain := "sk-old-managed"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	managedName := upstreamProvisioningKeyPrefix + "bbbbbbbbbbbbbbbb"
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1,
		UpstreamKeyID: 50, UpstreamKeyName: managedName,
		APIKeyCipher: encrypted, APIKeyPrefix: keyPrefix(plain), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	modified := managedUpstreamKeyFixture(50, managedName, "moved-by-upstream")
	remote := map[int64]connector.APIKey{modified.ID: modified}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			items := make([]connector.APIKey, 0, len(remote))
			for _, item := range remote {
				items = append(items, item)
			}
			return &connector.APIKeyPage{Page: 1, Pages: 1, Total: int64(len(items)), Items: items}, nil
		},
	}
	keys.deleteAPIKey = func(id int64) error {
		delete(remote, id)
		return nil
	}

	service := NewService(channels, rates, store, cipher, keys, nil)
	if err := service.ApplyUpstreamGroupFacts(context.Background(), channel.ID); err != nil {
		t.Fatalf("replace modified managed key: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != modified.ID {
		t.Fatalf("deleted modified keys = %v", keys.deleted)
	}
	if len(keys.created) != 1 || keys.created[0].Group != "promo" {
		t.Fatalf("replacement creates = %+v", keys.created)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || stored.UpstreamKeyID == modified.ID || stored.UpstreamKeyName == managedName || !stored.Enabled {
		t.Fatalf("replacement managed key state = %+v, err=%v", stored, err)
	}
}

func TestApplyUpstreamGroupFactsContinuesAfterManagedGroupMutationFailure(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "apply-partial-failure")
	failed := managedUpstreamKeyFixture(60, upstreamProvisioningKeyPrefix+"cccccccccccccccc", "removed")
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1, Total: 1, Items: []connector.APIKey{failed}}, nil
		},
		deleteAPIKey: func(int64) error {
			return connector.HTTPStatusError(http.StatusServiceUnavailable, []byte("temporary provider failure"))
		},
	}

	service := NewService(channels, rates, store, cipher, keys, nil)
	err := service.ApplyUpstreamGroupFacts(context.Background(), channel.ID)
	if err == nil || !strings.Contains(err.Error(), "removed") {
		t.Fatalf("partial managed key failure = %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != failed.ID {
		t.Fatalf("failed deletion attempts = %v", keys.deleted)
	}
	if len(keys.created) != 1 || keys.created[0].Group != "promo" {
		t.Fatalf("unrelated group did not continue provisioning: %+v", keys.created)
	}
	state, stateErr := store.ManagedKeyMaintenanceState(channel.ID, channel.UpstreamAccountIdentityRevision)
	if stateErr != nil || state == nil || state.RetryAt == nil || !state.RetryAt.After(time.Now()) {
		t.Fatalf("partial failure backoff = %+v, err=%v", state, stateErr)
	}
}

func TestApplyUpstreamGroupFactsContinuesAfterActiveGroupTimeout(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "apply-active-timeout")
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "standard", Ratio: 2}); err != nil {
		t.Fatal(err)
	}
	baseKeys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}, {Name: "standard", Ratio: 2}},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	keys := &deadlineIsolatingKeyManager{fakeKeyManager: baseKeys, timedOutGroup: "promo"}

	service := NewService(channels, rates, store, cipher, keys, nil)
	err := service.ApplyUpstreamGroupFacts(context.Background(), channel.ID)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("active group timeout = %v, want deadline exceeded", err)
	}
	if len(keys.attempted) != 2 || keys.attempted[0] != "promo" || keys.attempted[1] != "standard" {
		t.Fatalf("active group create attempts = %v, want [promo standard]", keys.attempted)
	}
	if len(baseKeys.created) != 1 || baseKeys.created[0].Group != "standard" {
		t.Fatalf("healthy group creates = %+v, want standard", baseKeys.created)
	}
	state, stateErr := store.ManagedKeyMaintenanceState(channel.ID, channel.UpstreamAccountIdentityRevision)
	if stateErr != nil || state == nil || state.RetryAt == nil || !state.RetryAt.After(time.Now()) {
		t.Fatalf("timeout durable backoff = %+v, err=%v", state, stateErr)
	}
}

func TestManagedKeyControlPlaneMaintenanceIgnoresRoutingHealthGates(t *testing.T) {
	tests := []struct {
		name       string
		gateReason string
		gate       func(*testing.T, *gorm.DB, uint)
	}{
		{
			name:       "low balance",
			gateReason: availabilityReasonLowBalance,
			gate: func(t *testing.T, db *gorm.DB, channelID uint) {
				t.Helper()
				if err := db.Model(&storage.Channel{}).Where("id = ?", channelID).Updates(map[string]any{
					"last_balance":      0.01,
					"balance_threshold": 1.0,
				}).Error; err != nil {
					t.Fatalf("set low-balance routing fact: %v", err)
				}
			},
		},
		{
			name:       "monitor disabled",
			gateReason: availabilityReasonMonitorDisabled,
			gate: func(t *testing.T, db *gorm.DB, channelID uint) {
				t.Helper()
				if err := db.Model(&storage.Channel{}).Where("id = ?", channelID).Update("monitor_enabled", false).Error; err != nil {
					t.Fatalf("disable channel monitoring: %v", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var modelProbeCalls atomic.Int64
			modelEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				modelProbeCalls.Add(1)
				http.Error(w, "model probing is outside managed-key maintenance", http.StatusInternalServerError)
			}))
			defer modelEndpoint.Close()

			db, channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureWithDB(t, "control-plane "+test.name)
			if err := db.Model(&storage.Channel{}).Where("id = ?", channel.ID).Update("site_url", modelEndpoint.URL).Error; err != nil {
				t.Fatalf("set model endpoint: %v", err)
			}
			test.gate(t, db, channel.ID)
			current, err := channels.FindByID(channel.ID)
			if err != nil {
				t.Fatalf("load gated channel: %v", err)
			}
			if !containsString(ChannelAvailabilityReasons(*current), test.gateReason) {
				t.Fatalf("channel does not expose expected routing gate %q: %+v", test.gateReason, current)
			}

			keys := &fakeKeyManager{groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}}}
			service := NewService(channels, rates, store, cipher, keys, nil)
			if err := service.MaintainManagedUpstreamKeyForGroup(context.Background(), channel.ID, "promo"); err != nil {
				t.Fatalf("maintain managed key through %s: %v", test.gateReason, err)
			}
			if len(keys.created) != 1 {
				t.Fatalf("managed key creates = %d, want 1", len(keys.created))
			}
			stored, err := store.FindUpstreamKeyAny(channel.ID, "promo")
			if err != nil || !stored.Enabled || stored.Provisioning || strings.TrimSpace(stored.APIKeyCipher) == "" {
				t.Fatalf("managed key state = %+v, err=%v", stored, err)
			}
			if stored.ModelsSyncedAt != nil || strings.TrimSpace(stored.ModelIDs) != "" || strings.TrimSpace(stored.ModelFamilies) != "" {
				t.Fatalf("managed-key maintenance wrote model facts: %+v", stored)
			}
			if got := modelProbeCalls.Load(); got != 0 {
				t.Fatalf("managed-key maintenance sent %d model probe request(s), want zero", got)
			}
		})
	}
}
