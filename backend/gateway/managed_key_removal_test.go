package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func managedKeyRemovalIdentityFixture(t *testing.T) (*storage.Channels, *storage.Channel) {
	t.Helper()
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	channel := &storage.Channel{
		Name: t.Name(), Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://managed-key-removal.invalid", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create managed-key removal channel: %v", err)
	}
	return channels, channel
}

func TestManagedKeyRemovalEvidenceRequiresCompleteFingerprintAndBoundSecret(t *testing.T) {
	cipher, err := appcrypto.NewCipher("strict-managed-removal-secret")
	if err != nil {
		t.Fatal(err)
	}
	plain := "sk-strict-owned"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(7, upstreamKeyNameForGroup("promo"), "promo")
	local := &storage.GatewayUpstreamKey{
		ID: 3, ChannelID: 1, GroupName: "promo", UpstreamKeyID: remote.ID,
		UpstreamKeyName: remote.Name, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: encrypted, APIKeyPrefix: keyPrefix(plain), Enabled: true,
	}
	channels, channel := managedKeyRemovalIdentityFixture(t)
	local.ChannelID = channel.ID
	generation := storage.UpstreamGroupGeneration{ID: 9, ChannelID: channel.ID, CanonicalGroupName: "promo", PresenceGeneration: 4}
	keys := &fakeKeyManager{revealedKeys: map[int64]string{remote.ID: plain}}
	service := &Service{Channels: channels, Cipher: cipher, Keys: keys}

	digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, channel.UpstreamAccountIdentityRevision)
	if err != nil || reason != "" || len(digest) != sha256HexLength {
		t.Fatalf("strict evidence digest=%q reason=%q err=%v", digest, reason, err)
	}

	incomplete := remote
	incomplete.ObservedFields.ModelLimits = false
	if digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, incomplete, channel.UpstreamAccountIdentityRevision); err != nil || digest != "" || reason != "creation_fingerprint_incomplete" {
		t.Fatalf("incomplete fingerprint digest=%q reason=%q err=%v", digest, reason, err)
	}

	keys.revealedKeys[remote.ID] = "sk-reused-id"
	if digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, channel.UpstreamAccountIdentityRevision); err != nil || digest != "" || reason != "remote_secret_changed" {
		t.Fatalf("reused id digest=%q reason=%q err=%v", digest, reason, err)
	}
}

func TestManagedKeyRemovalProvisioningNameRequiresPersistedClaim(t *testing.T) {
	cipher, err := appcrypto.NewCipher("strict-provisioning-removal-secret")
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now().Add(-time.Minute).UTC().Round(time.Microsecond)
	remote := managedUpstreamKeyFixture(12, "BOG-p-0123456789abcdef", "promo")
	local := &storage.GatewayUpstreamKey{
		ID: 5, ChannelID: 1, GroupName: "promo", UpstreamKeyID: remote.ID,
		UpstreamKeyName: remote.Name, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceProvisioningClaim,
		Provisioning: true, ProvisioningStartedAt: &startedAt,
	}
	channels, channel := managedKeyRemovalIdentityFixture(t)
	local.ChannelID = channel.ID
	generation := storage.UpstreamGroupGeneration{ID: 10, ChannelID: channel.ID, CanonicalGroupName: "promo", PresenceGeneration: 2}
	keys := &fakeKeyManager{revealedKeys: map[int64]string{remote.ID: "sk-provisioned"}}
	service := &Service{Channels: channels, Cipher: cipher, Keys: keys}

	digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, channel.UpstreamAccountIdentityRevision)
	if err != nil || reason != "" || len(digest) != sha256HexLength {
		t.Fatalf("provisioning evidence digest=%q reason=%q err=%v", digest, reason, err)
	}

	local.Provisioning = false
	if digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, channel.UpstreamAccountIdentityRevision); err != nil || digest != "" || reason != "missing_local_ownership" {
		t.Fatalf("name-only provisioning digest=%q reason=%q err=%v", digest, reason, err)
	}
}

func TestManagedKeyRemovalEvidenceRejectsAdoptedAndUnknownOwnership(t *testing.T) {
	cipher, err := appcrypto.NewCipher("strict-provenance-removal-secret")
	if err != nil {
		t.Fatal(err)
	}
	plain := "sk-historical-adoption"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(15, upstreamKeyNameForGroup("promo"), "promo")
	generation := storage.UpstreamGroupGeneration{ID: 14, ChannelID: 1, CanonicalGroupName: "promo", PresenceGeneration: 2}
	service := &Service{Cipher: cipher, Keys: &fakeKeyManager{revealedKeys: map[int64]string{remote.ID: plain}}}
	for _, provenance := range []string{storage.GatewayUpstreamKeyProvenanceAdopted, storage.GatewayUpstreamKeyProvenanceUnknown, ""} {
		local := &storage.GatewayUpstreamKey{
			ID: 8, ChannelID: 1, GroupName: "promo", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name,
			OwnershipProvenance: provenance, APIKeyCipher: encrypted, APIKeyPrefix: keyPrefix(plain), Enabled: true,
		}
		digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, 1)
		if err != nil || digest != "" || reason != "ownership_provenance_untrusted" {
			t.Fatalf("provenance %q digest=%q reason=%q err=%v", provenance, digest, reason, err)
		}
	}
}

func TestManagedKeyRemovalEvidenceRejectsGroupAndSourceChanges(t *testing.T) {
	cipher, err := appcrypto.NewCipher("strict-removal-identity-secret")
	if err != nil {
		t.Fatal(err)
	}
	plain := "sk-identity"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(18, upstreamKeyNameForGroup("promo"), "promo")
	local := &storage.GatewayUpstreamKey{
		ChannelID: 1, GroupName: "promo", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: encrypted,
	}
	generation := storage.UpstreamGroupGeneration{ID: 11, ChannelID: 1, CanonicalGroupName: "promo", PresenceGeneration: 1}
	service := &Service{Cipher: cipher, Keys: &fakeKeyManager{revealedKeys: map[int64]string{remote.ID: plain}}}

	changedGroup := remote
	changedGroup.GroupName = "Promo"
	if digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, changedGroup, 1); err != nil || digest != "" || reason != "creation_fingerprint_incomplete" {
		t.Fatalf("changed group digest=%q reason=%q err=%v", digest, reason, err)
	}

	unknownSource := remote
	unknownSource.Source = connector.APIKeySource("")
	if digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, unknownSource, 1); err != nil || digest != "" || reason != "connector_source_unknown" {
		t.Fatalf("unknown source digest=%q reason=%q err=%v", digest, reason, err)
	}
}

func TestManagedKeyRemovalEvidenceValidatesSub2APIGroupID(t *testing.T) {
	cipher, err := appcrypto.NewCipher("strict-sub2api-group-id-secret")
	if err != nil {
		t.Fatal(err)
	}
	plain := "sk-sub2api-owned"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	groupID := int64(42)
	remote := managedSub2APIKeyFixture(19, upstreamKeyNameForGroup("promo"), "promo")
	remote.GroupID = &groupID
	local := &storage.GatewayUpstreamKey{
		ID: 6, ChannelID: 1, GroupName: "promo", UpstreamKeyID: remote.ID,
		UpstreamKeyName: remote.Name, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher: encrypted, APIKeyPrefix: keyPrefix(plain),
	}
	channels, channel := managedKeyRemovalIdentityFixture(t)
	local.ChannelID = channel.ID
	generation := storage.UpstreamGroupGeneration{ID: 12, ChannelID: channel.ID, CanonicalGroupName: "promo", PresenceGeneration: 3}
	keys := &fakeKeyManager{
		groups:       []connector.APIKeyGroup{{ID: &groupID, Name: "promo"}},
		revealedKeys: map[int64]string{remote.ID: plain},
	}
	service := &Service{Channels: channels, Cipher: cipher, Keys: keys}

	digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, channel.UpstreamAccountIdentityRevision)
	if err != nil || reason != "" || len(digest) != sha256HexLength || keys.listGroupCalls != 1 {
		t.Fatalf("sub2api group-id evidence digest=%q reason=%q calls=%d err=%v", digest, reason, keys.listGroupCalls, err)
	}

	keys.groups[0].Name = "reused-group-id"
	if digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, channel.UpstreamAccountIdentityRevision); err != nil || digest != "" || reason != "creation_fingerprint_incomplete" {
		t.Fatalf("reused sub2api group id digest=%q reason=%q err=%v", digest, reason, err)
	}

	keys.groups = []connector.APIKeyGroup{{ID: &groupID, Name: "promo"}, {ID: &groupID, Name: "promo"}}
	if digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, channel.UpstreamAccountIdentityRevision); err != nil || digest != "" || reason != "creation_fingerprint_incomplete" {
		t.Fatalf("duplicate sub2api group id digest=%q reason=%q err=%v", digest, reason, err)
	}

	remote.GroupID = nil
	keys.groups = []connector.APIKeyGroup{{ID: &groupID, Name: "promo"}}
	if digest, reason, err := service.managedKeyRemovalEvidenceDigest(context.Background(), generation, "promo", local, remote, channel.UpstreamAccountIdentityRevision); err != nil || digest != "" || reason != "creation_fingerprint_incomplete" {
		t.Fatalf("missing sub2api group id digest=%q reason=%q err=%v", digest, reason, err)
	}
}

func TestStrictManagedKeysForRebuildRejectsDuplicateBoundLocalIdentity(t *testing.T) {
	remote := managedUpstreamKeyFixture(31, upstreamKeyNameForGroup("promo"), "promo")
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: managedKeyRemovalListFixture(map[int64]connector.APIKey{remote.ID: remote})}}
	localKeys := []storage.GatewayUpstreamKey{
		{ID: 1, ChannelID: 1, GroupName: "promo", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name},
		{ID: 2, ChannelID: 1, GroupName: "PROMO", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name},
	}

	if targets, err := service.strictManagedKeysForRebuildInventory(context.Background(), 1, 1, localKeys, []connector.APIKey{remote}); err == nil || !strings.Contains(err.Error(), "ambiguous local ownership") || targets != nil {
		t.Fatalf("duplicate local identity targets=%+v err=%v", targets, err)
	}
}

func TestStrictManagedKeysForRebuildRejectsSameNameWithDifferentBoundRemoteID(t *testing.T) {
	remote := managedUpstreamKeyFixture(32, upstreamKeyNameForGroup("promo"), "promo")
	service := &Service{}
	localKeys := []storage.GatewayUpstreamKey{{
		ID: 1, ChannelID: 1, GroupName: "promo", UpstreamKeyID: 31, UpstreamKeyName: remote.Name,
	}}

	targets, err := service.strictManagedKeysForRebuildInventory(context.Background(), 1, 1, localKeys, []connector.APIKey{remote})
	if err == nil || !strings.Contains(err.Error(), "different bound remote identity") || targets != nil {
		t.Fatalf("same-name different-id targets=%+v err=%v", targets, err)
	}
}

func TestRevalidateManagedKeyRebuildTargetRejectsSameNameWithNewRemoteID(t *testing.T) {
	remote := managedUpstreamKeyFixture(41, upstreamKeyNameForGroup("promo"), "promo")
	reappeared := remote
	reappeared.ID = 42
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: managedKeyRemovalListFixture(map[int64]connector.APIKey{
		reappeared.ID: reappeared,
	})}}
	target := managedKeyRebuildTarget{
		Remote: remote,
		Local: storage.GatewayUpstreamKey{
			ID: 7, ChannelID: 1, GroupName: "promo", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name,
		},
		Generation: storage.UpstreamGroupGeneration{ID: 8, ChannelID: 1, CanonicalGroupName: "promo", PresenceGeneration: 3},
	}

	current, err := service.revalidateManagedKeyRebuildTarget(context.Background(), 1, target)
	if err == nil || errors.Is(err, gorm.ErrRecordNotFound) || !strings.Contains(err.Error(), "reappeared under remote id 42") || current != nil {
		t.Fatalf("same-name replacement current=%+v err=%v", current, err)
	}
}

func TestRevalidateManagedKeyRebuildTargetReportsTrueRemoteAbsence(t *testing.T) {
	remote := managedUpstreamKeyFixture(43, upstreamKeyNameForGroup("promo"), "promo")
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: managedKeyRemovalListFixture(map[int64]connector.APIKey{})}}
	target := managedKeyRebuildTarget{
		Remote: remote,
		Local: storage.GatewayUpstreamKey{
			ID: 9, ChannelID: 1, GroupName: "promo", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name,
		},
		Generation: storage.UpstreamGroupGeneration{ID: 10, ChannelID: 1, CanonicalGroupName: "promo", PresenceGeneration: 4},
	}

	current, err := service.revalidateManagedKeyRebuildTarget(context.Background(), 1, target)
	if !errors.Is(err, gorm.ErrRecordNotFound) || current != nil {
		t.Fatalf("absent remote current=%+v err=%v", current, err)
	}
}

func TestFindUpstreamKeyByIDWithExpectedNameReportsSameNameReplacement(t *testing.T) {
	replacement := managedUpstreamKeyFixture(52, upstreamKeyNameForGroup("promo"), "promo")
	channels, channel := managedKeyRemovalIdentityFixture(t)
	service := &Service{Channels: channels, Keys: &fakeKeyManager{listAPIKeys: managedKeyRemovalListFixture(map[int64]connector.APIKey{
		replacement.ID: replacement,
	})}}

	current, nameReused, err := service.findUpstreamKeyByIDWithExpectedName(context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision, 51, "  "+replacement.Name+"  ")
	if !errors.Is(err, gorm.ErrRecordNotFound) || current != nil || !nameReused {
		t.Fatalf("same-name replacement current=%+v nameReused=%v err=%v", current, nameReused, err)
	}
}

func TestFindUpstreamKeyByIDWithExpectedNameAllowsDuplicateInventory(t *testing.T) {
	current := managedUpstreamKeyFixture(51, upstreamKeyNameForGroup("promo"), "promo")
	duplicate := managedUpstreamKeyFixture(52, current.Name, "promo")
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: managedKeyRemovalListFixture(map[int64]connector.APIKey{
		current.ID: current, duplicate.ID: duplicate,
	})}}

	selected, nameReused, err := service.findUpstreamKeyByIDWithExpectedName(context.Background(), 1, 0, current.ID, current.Name)
	if err != nil || selected == nil || selected.ID != current.ID || !nameReused {
		t.Fatalf("duplicate-name inventory selected=%+v nameReused=%v err=%v", selected, nameReused, err)
	}
}

func TestFindRemovedGroupRemoteKeyIgnoresUnrelatedDuplicateNames(t *testing.T) {
	duplicateA := managedUpstreamKeyFixture(60, upstreamKeyNameForGroup("other"), "other")
	duplicateB := managedUpstreamKeyFixture(61, duplicateA.Name, "other")
	service := &Service{Keys: &fakeKeyManager{listAPIKeys: managedKeyRemovalListFixture(map[int64]connector.APIKey{
		duplicateA.ID: duplicateA, duplicateB.ID: duplicateB,
	})}}
	local := &storage.GatewayUpstreamKey{
		ChannelID: 1, GroupName: "promo", UpstreamKeyID: 51, UpstreamKeyName: upstreamKeyNameForGroup("promo"),
	}

	selected, ambiguous, err := service.findRemovedGroupRemoteKey(context.Background(), 1, 0, local)
	if err != nil || selected != nil || ambiguous {
		t.Fatalf("unrelated duplicate inventory selected=%+v ambiguous=%v err=%v", selected, ambiguous, err)
	}
}

func TestListAllUpstreamKeysRejectsDuplicateRemoteIdentities(t *testing.T) {
	tests := []struct {
		name  string
		items []connector.APIKey
		want  string
	}{
		{
			name: "duplicate-id",
			items: []connector.APIKey{
				{ID: 51, Name: "first"},
				{ID: 51, Name: "second"},
			},
			want: "id 51 is ambiguous",
		},
		{
			name: "duplicate-name",
			items: []connector.APIKey{
				{ID: 52, Name: "shared"},
				{ID: 53, Name: "shared"},
			},
			want: "name \"shared\" is ambiguous",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &Service{Keys: &fakeKeyManager{listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
				return &connector.APIKeyPage{Items: test.items, Page: 1, PageSize: upstreamKeyLookupPageSize, Pages: 1, Total: int64(len(test.items))}, nil
			}}}
			items, err := service.listAllUpstreamKeys(context.Background(), 1)
			if err == nil || !strings.Contains(err.Error(), test.want) || items != nil {
				t.Fatalf("list items=%+v err=%v", items, err)
			}
		})
	}
}

const sha256HexLength = 64

func TestManagedKeyRemovalSnapshotDoesNotContainPlainSecret(t *testing.T) {
	remote := managedUpstreamKeyFixture(21, upstreamKeyNameForGroup("promo"), "promo")
	remote.Key = "sk-must-not-enter-evidence"
	raw, err := json.Marshal(managedKeyRemovalSnapshot(remote))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), remote.Key) {
		t.Fatal("plain remote key entered removal evidence")
	}
}

func TestDeleteGroupRoutingStateUsesGenerationSafeManagedKeyIntent(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "strict-delete")
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("delete group routing state: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != remote.ID {
		t.Fatalf("remote deletes = %v", keys.deleted)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("removed local key error = %v", err)
	}
	changes, err := store.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	var removal *storage.GatewayUpstreamGroupChange
	for index := range changes {
		if changes[index].Kind == "removed" && changes[index].UpstreamKeyID == remote.ID {
			removal = &changes[index]
			break
		}
	}
	if removal == nil || removal.Status != "acknowledged" || removal.UpstreamGroupID == nil || removal.PresenceGeneration == nil || len(removal.FactsDigest) != sha256HexLength {
		t.Fatalf("generation-safe removal audit = %+v", removal)
	}
}

func TestDeleteGroupRoutingStateRefusesReappearedGeneration(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "reappeared-delete")
	listCalls := 0
	baseList := keys.listAPIKeys
	keys.listAPIKeys = func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		listCalls++
		if listCalls == 2 {
			if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", remote.ID, remote.Name, time.Now().Add(time.Second)); err != nil {
				return nil, err
			}
		}
		return baseList(query)
	}
	err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo")
	if !errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("reappeared generation error = %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("reappeared generation deleted remote key = %v", keys.deleted)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil || generation.PresenceGeneration != 2 {
		t.Fatalf("reappeared generation = %+v err=%v", generation, err)
	}
}

func TestDeleteGroupRoutingStateRechecksGenerationWhenRemoteAlreadyDisappeared(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "reappeared-not-found")
	listCalls := 0
	baseList := keys.listAPIKeys
	keys.listAPIKeys = func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		listCalls++
		if listCalls == 2 {
			if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", remote.ID, remote.Name, time.Now().Add(time.Second)); err != nil {
				return nil, err
			}
			return &connector.APIKeyPage{Page: 1, PageSize: upstreamKeyLookupPageSize, Pages: 1}, nil
		}
		return baseList(query)
	}
	err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo")
	if !errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("not-found reappeared generation error = %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("not-found reappeared generation deleted remote key = %v", keys.deleted)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); err != nil {
		t.Fatalf("not-found reappeared generation purged local key: %v", err)
	}
}

func TestDeleteGroupRoutingStateQuarantinesSameNameReplacementDuringRevalidation(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "same-name-replacement")
	replacement := remote
	replacement.ID = remote.ID + 100
	listCalls := 0
	baseList := keys.listAPIKeys
	replacementList := managedKeyRemovalListFixture(map[int64]connector.APIKey{replacement.ID: replacement})
	keys.listAPIKeys = func(query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		listCalls++
		if listCalls == 2 {
			return replacementList(query)
		}
		return baseList(query)
	}

	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("quarantine same-name replacement: %v", err)
	}
	if listCalls < 2 {
		t.Fatalf("remote inventory was not revalidated, list calls = %d", listCalls)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("same-name replacement triggered remote delete = %v", keys.deleted)
	}
	local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || local.Enabled || local.UpstreamKeyID != remote.ID || local.LastError != managedKeyRemovalReviewMessage {
		t.Fatalf("same-name replacement local quarantine = %+v err=%v", local, err)
	}
	changes, err := store.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range changes {
		if change.Kind == "removed" && change.UpstreamKeyID == remote.ID && change.Status == "ignored" && change.AssignmentReasonCode == "manual_review_required" {
			return
		}
	}
	t.Fatalf("same-name replacement manual-review audit missing = %+v", changes)
}

func TestDeleteGroupRoutingStateFinalizesInitiallyAbsentRemoteUnderGenerationLock(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "initially-absent")
	keys.listAPIKeys = func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		return &connector.APIKeyPage{Page: 1, PageSize: upstreamKeyLookupPageSize, Pages: 1}, nil
	}
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("finalize initially absent remote: %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("initially absent remote deletion calls = %v", keys.deleted)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("initially absent remote local key error = %v", err)
	}
	changes, err := store.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range changes {
		if change.Kind == "removed" && change.UpstreamKeyID == remote.ID {
			if change.Status != "acknowledged" || change.RemovalClaimKey == nil || len(change.FactsDigest) != sha256HexLength {
				t.Fatalf("initially absent remote audit = %+v", change)
			}
			return
		}
	}
	t.Fatalf("initially absent remote audit missing = %+v", changes)
}

func TestDeleteGroupRoutingStateNeverDeletesAdoptedKey(t *testing.T) {
	service, store, channel, keys, _ := newManagedKeyRemovalIntegrationFixture(t, "adopted-key")
	local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	local.OwnershipProvenance = storage.GatewayUpstreamKeyProvenanceAdopted
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("adopted key should quarantine, got %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("adopted key deleted remotely = %v", keys.deleted)
	}
	local, err = store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || local.Enabled || local.LastError != managedKeyRemovalReviewMessage {
		t.Fatalf("adopted key local state = %+v err=%v", local, err)
	}
}

func TestDeleteGroupRoutingStateRejectsLocalOwnershipDriftAfterRemoteRevalidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*storage.GatewayUpstreamKey)
	}{
		{
			name: "provenance",
			mutate: func(local *storage.GatewayUpstreamKey) {
				local.OwnershipProvenance = storage.GatewayUpstreamKeyProvenanceAdopted
			},
		},
		{
			name: "cipher",
			mutate: func(local *storage.GatewayUpstreamKey) {
				local.APIKeyCipher += "-changed"
			},
		},
		{
			name: "prefix",
			mutate: func(local *storage.GatewayUpstreamKey) {
				local.APIKeyPrefix = "sk-changed"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "ownership-drift-"+test.name)
			plain := keys.revealedKeys[remote.ID]
			keys.revealAPIKey = func(id int64) (string, error) {
				if len(keys.revealCalls) == 2 {
					local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
					if err != nil {
						return "", err
					}
					test.mutate(local)
					if err := store.UpsertUpstreamKey(local); err != nil {
						return "", err
					}
				}
				return plain, nil
			}

			if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
				t.Fatalf("ownership drift should quarantine: %v", err)
			}
			if len(keys.deleted) != 0 {
				t.Fatalf("ownership drift deleted remote key = %v", keys.deleted)
			}
			local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
			if err != nil || local.Enabled || local.LastError != managedKeyRemovalReviewMessage {
				t.Fatalf("ownership drift local quarantine = %+v err=%v", local, err)
			}
		})
	}
}

func TestDeleteGroupRoutingStateRejectsDuplicateLocalRemoteIdentity(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "duplicate-local-identity")
	target, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "other", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher:        target.APIKeyCipher, APIKeyPrefix: target.APIKeyPrefix, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("duplicate identity should quarantine, got %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("duplicate identity deleted remote key = %v", keys.deleted)
	}
	other, err := store.FindUpstreamKeyAny(channel.ID, "other")
	if err != nil || !other.Enabled {
		t.Fatalf("active duplicate route changed = %+v err=%v", other, err)
	}
	target, err = store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || target.Enabled || target.LastError != managedKeyRemovalReviewMessage {
		t.Fatalf("duplicate identity target was not quarantined = %+v err=%v", target, err)
	}
}

func TestDeleteGroupRoutingStateRejectsUnboundDuplicateRemoteName(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "duplicate-local-name")
	target, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "other", UpstreamKeyName: remote.Name,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceUnknown,
		APIKeyCipher:        target.APIKeyCipher, APIKeyPrefix: target.APIKeyPrefix, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("duplicate name should quarantine, got %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("duplicate name deleted remote key = %v", keys.deleted)
	}
	other, err := store.FindUpstreamKeyAny(channel.ID, "other")
	if err != nil || !other.Enabled {
		t.Fatalf("unbound duplicate route changed = %+v err=%v", other, err)
	}
}

func TestDeleteGroupRoutingStateKeepsUncertainDeleteProcessingForReconcile(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "uncertain-delete")
	keys.deleteAPIKey = func(int64) error { return errors.New("connection lost after delete was sent") }
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err == nil {
		t.Fatal("uncertain delete unexpectedly succeeded")
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != remote.ID {
		t.Fatalf("uncertain remote delete calls = %v", keys.deleted)
	}
	changes, err := store.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	var intent *storage.GatewayUpstreamGroupChange
	for index := range changes {
		if changes[index].Kind == "removed" && changes[index].UpstreamKeyID == remote.ID {
			intent = &changes[index]
			break
		}
	}
	if intent == nil || intent.Status != "processing" {
		t.Fatalf("uncertain delete intent = %+v", intent)
	}
	pending, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || pending.Enabled || pending.LastError != "managed upstream key removal pending reconciliation" {
		t.Fatalf("uncertain delete local quarantine = %+v err=%v", pending, err)
	}
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("retry while removal intent is processing: %v", err)
	}
	if len(keys.deleted) != 1 {
		t.Fatalf("processing removal intent repeated remote delete = %v", keys.deleted)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now()); err != nil {
		t.Fatalf("reconcile uncertain delete: %v", err)
	}
	changes, err = store.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	for index := range changes {
		if changes[index].ID == intent.ID {
			intent = &changes[index]
			break
		}
	}
	if intent.Status != "acknowledged" || intent.ResolvedAt == nil {
		t.Fatalf("reconciled uncertain delete intent = %+v", intent)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("reconciled uncertain delete local key error = %v", err)
	}
}

func TestDeleteGroupRoutingStateFinalizesAfterRequestCancellationFollowingRemoteDelete(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "deadline-after-delete")
	ctx, cancel := context.WithCancel(context.Background())
	keys.deleteAPIKey = func(id int64) error {
		cancel()
		return nil
	}
	if err := service.DeleteGroupRoutingState(ctx, channel.ID, "promo"); err != nil {
		t.Fatalf("finalize after request cancellation: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != remote.ID {
		t.Fatalf("remote delete calls = %v", keys.deleted)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("local key survived completed remote delete: %v", err)
	}
}

func TestDeleteGroupRoutingStateIdentityDriftReturnsToManualReview(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "absent-identity-drift")
	keys.prepareDelete = func(connector.APIKey) (func() error, error) {
		return func() error { return connector.ErrManagedAPIKeyDeletionIdentityChanged }, nil
	}
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("identity drift removal: %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("identity drift issued remote deletes = %v", keys.deleted)
	}
	intent := managedRemovalManualReviewIntent(t, store, remote.ID)
	if intent.Status != "ignored" {
		t.Fatalf("identity drift intent = %+v", intent)
	}
}

func TestDeleteGroupRoutingStateQuarantinesAbsentRemoteWithMissingLocalName(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "missing-local-name")
	local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	local.UpstreamKeyName = ""
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	keys.listAPIKeys = managedKeyRemovalListFixture(map[int64]connector.APIKey{})

	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("quarantine incomplete absent identity: %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("incomplete absent identity deleted remotely = %v", keys.deleted)
	}
	local, err = store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || local.Enabled || local.LastError != managedKeyRemovalReviewMessage {
		t.Fatalf("incomplete absent identity local state = %+v err=%v", local, err)
	}
	changes, err := store.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range changes {
		if change.Kind == "removed" && change.UpstreamKeyID == remote.ID && change.UpstreamKeyName == "" &&
			change.Status == "ignored" && change.AssignmentReasonCode == "manual_review_required" {
			return
		}
	}
	t.Fatalf("incomplete absent identity review audit missing = %+v", changes)
}

func TestDeleteGroupRoutingStateQuarantinesIncompleteRemoteFingerprint(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "incomplete-delete")
	remote.ObservedFields.ModelLimits = false
	keys.listAPIKeys = managedKeyRemovalListFixture(map[int64]connector.APIKey{remote.ID: remote})
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("quarantine incomplete fingerprint: %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("incomplete fingerprint deleted remote key = %v", keys.deleted)
	}
	local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || local.Enabled || local.APIKeyCipher == "" || local.LastError != managedKeyRemovalReviewMessage {
		t.Fatalf("quarantined local key = %+v err=%v", local, err)
	}
	changes, err := store.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	foundReview := false
	for _, change := range changes {
		if change.Kind == "removed" && change.UpstreamKeyID == remote.ID && change.Status == "ignored" && change.AssignmentReasonCode == "manual_review_required" {
			foundReview = true
		}
	}
	if !foundReview {
		t.Fatalf("manual review audit missing = %+v", changes)
	}
}

func TestManagedKeyRemovalManualReviewResumeRevalidatesAndDeletesExactRemoteKey(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "manual-resume-present")
	incomplete := remote
	incomplete.ObservedFields.ModelLimits = false
	keys.listAPIKeys = managedKeyRemovalListFixture(map[int64]connector.APIKey{remote.ID: incomplete})
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("quarantine incomplete remote key: %v", err)
	}

	intent := managedRemovalManualReviewIntent(t, store, remote.ID)
	keys.listAPIKeys = managedKeyRemovalListFixture(map[int64]connector.APIKey{remote.ID: remote})
	if err := service.ResumeManagedUpstreamKeyRemovalManualReview(context.Background(), intent.ID, "operator", "remote_revalidated"); err != nil {
		t.Fatalf("resume manual-review removal: %v", err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != remote.ID {
		t.Fatalf("manual-review remote deletes = %v", keys.deleted)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("manual-review local key was not purged: %v", err)
	}
	resolved, err := store.FindUpstreamGroupChange(intent.ID)
	if err != nil || resolved.Status != "acknowledged" || resolved.ResolvedAt == nil ||
		resolved.RemovalManualReviewResolution != "remote_revalidated" || resolved.RemovalManualReviewResolvedBy != "operator" ||
		resolved.RemovalManualReviewResolvedAt == nil || resolved.RemovalClaimEpoch == nil || *resolved.RemovalClaimEpoch <= *intent.RemovalClaimEpoch {
		t.Fatalf("manual-review resolved intent = %+v err=%v", resolved, err)
	}
}

func TestManagedKeyRemovalManualReviewResumeIdentityDriftStaysReviewable(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "manual-resume-identity-drift")
	incomplete := remote
	incomplete.ObservedFields.ModelLimits = false
	keys.listAPIKeys = managedKeyRemovalListFixture(map[int64]connector.APIKey{remote.ID: incomplete})
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("quarantine incomplete remote key: %v", err)
	}

	intent := managedRemovalManualReviewIntent(t, store, remote.ID)
	keys.listAPIKeys = managedKeyRemovalListFixture(map[int64]connector.APIKey{remote.ID: remote})
	keys.prepareDelete = func(connector.APIKey) (func() error, error) {
		return func() error { return connector.ErrManagedAPIKeyDeletionIdentityChanged }, nil
	}
	err := service.ResumeManagedUpstreamKeyRemovalManualReview(context.Background(), intent.ID, "operator", "remote_revalidated")
	if !errors.Is(err, connector.ErrManagedAPIKeyDeletionIdentityChanged) || len(keys.deleted) != 0 {
		t.Fatalf("resume identity drift err=%v deletes=%v", err, keys.deleted)
	}
	reviewed := managedRemovalManualReviewIntent(t, store, remote.ID)
	if reviewed.Status != "ignored" || reviewed.RemovalClaimEpoch == nil || *reviewed.RemovalClaimEpoch <= *intent.RemovalClaimEpoch {
		t.Fatalf("resumed identity drift intent = %+v", reviewed)
	}
}

func TestManagedKeyRemovalManualReviewResumeAcknowledgesFreshRemoteAbsence(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "manual-resume-absent")
	incomplete := remote
	incomplete.ObservedFields.ModelLimits = false
	keys.listAPIKeys = managedKeyRemovalListFixture(map[int64]connector.APIKey{remote.ID: incomplete})
	if err := service.DeleteGroupRoutingState(context.Background(), channel.ID, "promo"); err != nil {
		t.Fatalf("quarantine incomplete remote key: %v", err)
	}

	intent := managedRemovalManualReviewIntent(t, store, remote.ID)
	keys.listAPIKeys = managedKeyRemovalListFixture(map[int64]connector.APIKey{})
	if err := service.ResumeManagedUpstreamKeyRemovalManualReview(context.Background(), intent.ID, "operator", "remote_absence_revalidated"); err != nil {
		t.Fatalf("resume absent manual-review removal: %v", err)
	}
	if len(keys.deleted) != 0 {
		t.Fatalf("fresh remote absence issued delete calls = %v", keys.deleted)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("absent manual-review local key was not purged: %v", err)
	}
	resolved, err := store.FindUpstreamGroupChange(intent.ID)
	if err != nil || resolved.Status != "acknowledged" || resolved.ResolvedAt == nil ||
		resolved.RemovalManualReviewResolution != "remote_absence_revalidated" || resolved.RemovalManualReviewResolvedAt == nil {
		t.Fatalf("absent manual-review resolved intent = %+v err=%v", resolved, err)
	}
}

func managedRemovalManualReviewIntent(t *testing.T, store *storage.Gateway, remoteID int64) *storage.GatewayUpstreamGroupChange {
	t.Helper()
	changes, err := store.ListUpstreamGroupChanges(false)
	if err != nil {
		t.Fatal(err)
	}
	for index := range changes {
		change := &changes[index]
		if change.Kind == "removed" && change.UpstreamKeyID == remoteID && change.Status == "ignored" &&
			change.AssignmentReasonCode == "manual_review_required" && change.RemovalClaimEpoch != nil {
			return change
		}
	}
	t.Fatalf("manual-review removal intent %d not found in %+v", remoteID, changes)
	return nil
}

func newManagedKeyRemovalIntegrationFixture(t *testing.T, name string) (*Service, *storage.Gateway, *storage.Channel, *fakeKeyManager, connector.APIKey) {
	t.Helper()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("managed-removal-integration-secret-" + name)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: name, Type: storage.ChannelTypeNewAPI, SiteURL: "https://managed-removal.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 7, upstreamKeyNameForGroup("promo"), time.Now()); err != nil {
		t.Fatal(err)
	}
	plain := "sk-managed-removal-" + name
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	remote := managedUpstreamKeyFixture(7, upstreamKeyNameForGroup("promo"), "promo")
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: remote.ID, UpstreamKeyName: remote.Name,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        encrypted, APIKeyPrefix: keyPrefix(plain), Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	remoteItems := map[int64]connector.APIKey{remote.ID: remote}
	keys := &fakeKeyManager{
		listAPIKeys:  managedKeyRemovalListFixture(remoteItems),
		revealedKeys: map[int64]string{remote.ID: plain},
	}
	keys.deleteAPIKey = func(id int64) error {
		delete(remoteItems, id)
		return nil
	}
	return NewService(channels, rates, store, cipher, keys, nil), store, channel, keys, remote
}

func TestInventoryRemovalProcessingIntentDoesNotClearLocalBinding(t *testing.T) {
	service, store, channel, _, remote := newManagedKeyRemovalIntegrationFixture(t, "inventory-processing")
	strongName := upstreamProvisioningKeyPrefix + "0123456789abcdef"
	local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	local.UpstreamKeyName = strongName
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	remote.Name = strongName
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	evidence := managedInventoryRemovalEvidenceDigest(generation, remote)
	ownership := HashKey("managed-prefix:" + strongName)
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyInventoryRemoval(
		generation, channel.UpstreamAccountIdentityRevision, "promo", remote.ID, strongName, true,
		evidence, ownership, time.Now(), time.Minute,
	)
	if err != nil || !claimed || intent == nil {
		t.Fatalf("begin processing inventory intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	deleted, err := service.deleteManagedRemoteKeyForSync(context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision, remote, generation, true, "promo")
	if err != nil || deleted {
		t.Fatalf("processing inventory removal deleted=%v err=%v", deleted, err)
	}
	retained, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || retained.UpstreamKeyID != remote.ID || retained.UpstreamKeyName != strongName {
		t.Fatalf("processing inventory removal changed local binding=%+v err=%v", retained, err)
	}
}

func TestInventoryRemovalIdentityDriftEntersManualReviewWithoutDelete(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "inventory-identity-drift")
	strongName := upstreamProvisioningKeyPrefix + "abcdef0123456789"
	local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	local.UpstreamKeyName = strongName
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	remote.Name = strongName
	keys.prepareDelete = func(connector.APIKey) (func() error, error) {
		return func() error { return connector.ErrManagedAPIKeyDeletionIdentityChanged }, nil
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}

	deleted, err := service.deleteManagedRemoteKeyForSync(context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision, remote, generation, true, "promo")
	if err != nil || deleted || len(keys.deleted) != 0 {
		t.Fatalf("identity drift deletion deleted=%v calls=%v err=%v", deleted, keys.deleted, err)
	}
	intent := managedRemovalManualReviewIntent(t, store, remote.ID)
	if intent.RemovalManualReviewReason != "remote_identity_changed" {
		t.Fatalf("identity drift review = %+v", intent)
	}
	retained, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || retained.UpstreamKeyID != remote.ID || retained.UpstreamKeyName != strongName {
		t.Fatalf("identity drift changed local binding=%+v err=%v", retained, err)
	}
}

func TestInventoryRemovalFinalizesAfterRequestCancellationFollowingRemoteDelete(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "inventory-deadline-after-delete")
	strongName := upstreamProvisioningKeyPrefix + "fedcba9876543210"
	local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	local.UpstreamKeyName = strongName
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	remote.Name = strongName
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	keys.deleteAPIKey = func(int64) error {
		cancel()
		return nil
	}

	deleted, err := service.deleteManagedRemoteKeyForSync(
		ctx, channel.ID, channel.UpstreamAccountIdentityRevision, remote, generation, true, "promo",
	)
	if err != nil || !deleted {
		t.Fatalf("finalize inventory removal after request cancellation deleted=%v err=%v", deleted, err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != remote.ID {
		t.Fatalf("remote delete calls = %v", keys.deleted)
	}
	cleared, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatalf("reload cleared local binding: %v", err)
	}
	if cleared.Enabled || cleared.UpstreamKeyID != 0 || cleared.UpstreamKeyName != "" || cleared.APIKeyCipher != "" || cleared.APIKeyPrefix != "" || cleared.LastError != "managed upstream key deleted" {
		t.Fatalf("local binding survived completed remote delete = %+v", cleared)
	}
}

func TestInventoryRemovalQuarantinesAmbiguousCancellationWithoutReplayingDelete(t *testing.T) {
	service, store, channel, keys, remote := newManagedKeyRemovalIntegrationFixture(t, "inventory-ambiguous-cancellation")
	strongName := upstreamProvisioningKeyPrefix + "1029384756abcdef"
	local, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	local.UpstreamKeyName = strongName
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	remote.Name = strongName
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	keys.deleteAPIKey = func(int64) error {
		cancel()
		return context.Canceled
	}

	deleted, err := service.deleteManagedRemoteKeyForSync(
		ctx, channel.ID, channel.UpstreamAccountIdentityRevision, remote, generation, true, "promo",
	)
	if deleted || !errors.Is(err, context.Canceled) {
		t.Fatalf("ambiguous inventory removal deleted=%v err=%v", deleted, err)
	}
	if len(keys.deleted) != 1 || keys.deleted[0] != remote.ID {
		t.Fatalf("remote delete calls = %v", keys.deleted)
	}
	quarantined, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatalf("reload quarantined local binding: %v", err)
	}
	if quarantined.Enabled || quarantined.UpstreamKeyID != remote.ID || quarantined.UpstreamKeyName != strongName || quarantined.LastError != "managed upstream key removal pending reconciliation" {
		t.Fatalf("ambiguous remote delete was not quarantined = %+v", quarantined)
	}

	deleted, err = service.deleteManagedRemoteKeyForSync(
		context.Background(), channel.ID, channel.UpstreamAccountIdentityRevision, remote, generation, true, "promo",
	)
	if err != nil || deleted || len(keys.deleted) != 1 {
		t.Fatalf("processing intent replayed remote delete deleted=%v calls=%v err=%v", deleted, keys.deleted, err)
	}
}

func managedKeyRemovalListFixture(items map[int64]connector.APIKey) func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	return func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		list := make([]connector.APIKey, 0, len(items))
		for _, item := range items {
			list = append(list, item)
		}
		sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
		return &connector.APIKeyPage{Items: list, Page: 1, PageSize: upstreamKeyLookupPageSize, Pages: 1, Total: int64(len(list))}, nil
	}
}
