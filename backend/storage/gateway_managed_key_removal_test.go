package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func retireManagedKeyRemovalGeneration(t *testing.T, store *Gateway, channel *Channel, upstreamKeyID int64, upstreamKeyName string) UpstreamGroupGeneration {
	t.Helper()
	discoveredAt := time.Now().UTC().Add(-time.Minute)
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", upstreamKeyID, upstreamKeyName, discoveredAt); err != nil {
		t.Fatal(err)
	}
	generation, err := store.RetireUpstreamGroupForKeyRemoval(
		channel.ID, channel.UpstreamAccountIdentityRevision, "promo", discoveredAt.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	return generation
}

func TestRecordUpstreamGroupChangeDoesNotRepeatAddedAfterManagedRemovalAudit(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "managed removal audit dedupe", Type: ChannelTypeNewAPI, SiteURL: "https://dedupe.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now().UTC()
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 7, "managed-promo", observedAt); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	removedAudit := newUpstreamGroupChange(channel.ID, "promo", "removed", 7, "managed-promo", observedAt.Add(time.Second))
	removedAudit.UpstreamGroupID = &generation.ID
	removedAudit.PresenceGeneration = &generation.PresenceGeneration
	if err := db.Create(removedAudit).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 7, "managed-promo", observedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 7, "managed-promo", observedAt.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}

	var addedCount int64
	if err := db.Model(&GatewayUpstreamGroupChange{}).
		Where("upstream_group_id = ? AND presence_generation = ? AND kind = ?", generation.ID, generation.PresenceGeneration, "added").
		Count(&addedCount).Error; err != nil {
		t.Fatal(err)
	}
	if addedCount != 1 {
		t.Fatalf("same-generation added change count = %d, want 1", addedCount)
	}
}

func TestGenerationSafeManagedKeyRemovalAcknowledgementRejectsIncompleteIdentity(t *testing.T) {
	expected := UpstreamGroupGeneration{
		ID: 1, ChannelID: 2, CanonicalGroupName: "promo", PresenceGeneration: 3,
	}
	store := &Gateway{}
	if err := store.AcknowledgeGenerationSafeManagedUpstreamKeyRemoval(expected, 1, 7, "", 9, strings.Repeat("a", 64), time.Now()); !errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) {
		t.Fatalf("missing remote name error = %v", err)
	}
	if err := store.AcknowledgeGenerationSafeManagedUpstreamKeyRemoval(UpstreamGroupGeneration{}, 1, 7, "BOG - promo", 9, strings.Repeat("a", 64), time.Now()); !errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) {
		t.Fatalf("invalid generation error = %v", err)
	}
}

func TestManagedUpstreamKeyRemovalClaimBindsChannelIdentityRevision(t *testing.T) {
	expected := UpstreamGroupGeneration{
		ID: 1, ChannelID: 2, CanonicalGroupName: "promo", PresenceGeneration: 3,
	}
	claimA := managedUpstreamKeyRemovalClaimKey(expected, 4, 7, "BOG - promo")
	claimB := managedUpstreamKeyRemovalClaimKey(expected, 5, 7, "BOG - promo")
	if claimA == claimB || !validRemovalEvidenceDigest(claimA) || !validRemovalEvidenceDigest(claimB) {
		t.Fatalf("revision-bound claims A=%q B=%q", claimA, claimB)
	}
	evidenceA := managedUpstreamKeyAbsenceEvidenceDigest(expected, 4, 7, "BOG - promo", strings.Repeat("a", 64))
	evidenceB := managedUpstreamKeyAbsenceEvidenceDigest(expected, 5, 7, "BOG - promo", strings.Repeat("a", 64))
	if evidenceA == evidenceB || !validRemovalEvidenceDigest(evidenceA) || !validRemovalEvidenceDigest(evidenceB) {
		t.Fatalf("revision-bound absence evidence A=%q B=%q", evidenceA, evidenceB)
	}
}

func TestGenerationSafeManagedKeyRemovalIntentRejectsReappearedGroup(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "generation-safe-removal", Type: ChannelTypeNewAPI, SiteURL: "https://removal.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 7, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "reappeared-cipher", APIKeyPrefix: "sk-reappeared", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*local)
	if err != nil {
		t.Fatal(err)
	}
	generation := retireManagedKeyRemovalGeneration(t, store, channel, local.UpstreamKeyID, local.UpstreamKeyName)
	digest := strings.Repeat("a", 64)
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName, digest, ownershipClaim, time.Now(), 10*time.Minute)
	if err != nil || !claimed || intent == nil || intent.UpstreamGroupID == nil || intent.PresenceGeneration == nil || intent.RemovalClaimEpoch == nil {
		t.Fatalf("generation-safe intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	if err := store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, intent.ID, local.ID,
		local.UpstreamKeyID, local.UpstreamKeyName, digest, ownershipClaim, time.Now(), nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 8, "BOG - promo", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName, digest, ownershipClaim, time.Now().Add(11*time.Minute), 10*time.Minute); !errors.Is(err, ErrUpstreamGroupGenerationChanged) || claimed {
		t.Fatalf("stale generation claimed=%v err=%v", claimed, err)
	}
}

func TestGenerationSafeManagedKeyRemovalReconcileQuarantinesLegacyIntent(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "legacy-removal-intent", Type: ChannelTypeNewAPI, SiteURL: "https://legacy-removal.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	claimKey := strings.Repeat("1", 64)
	legacy := &GatewayUpstreamGroupChange{
		ChannelID: channel.ID, GroupName: "promo", Kind: "removed", Status: "processing",
		UpstreamKeyID: 17, UpstreamKeyName: "BOG - promo", RemovalClaimKey: &claimKey,
		RemovalClaimEpoch:             uint64Pointer(1),
		RemovalClaimDigestVersion:     uint16Pointer(1),
		RemovalEvidenceDigestVersion:  uint16Pointer(1),
		RemovalOwnershipDigestVersion: uint16Pointer(1),
		DetectedAt:                    time.Now(),
	}
	if err := db.Create(legacy).Error; err != nil {
		t.Fatalf("create migrated legacy intent: %v", err)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var reconciled GatewayUpstreamGroupChange
	if err := db.First(&reconciled, legacy.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reconciled.Status != "ignored" || reconciled.AssignmentState != "ignored" || reconciled.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview || reconciled.RemovalClaimKey == nil {
		t.Fatalf("legacy intent was not quarantined = %+v", reconciled)
	}
}

func TestGenerationSafeManagedKeyRemovalReconcileLeavesOrdinaryRemovedAuditAlone(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "ordinary-removed-audit", Type: ChannelTypeNewAPI, SiteURL: "https://ordinary-removed.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "removed", 0, "", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var before GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND kind = ?", channel.ID, "removed").First(&before).Error; err != nil {
		t.Fatal(err)
	}
	if before.RemovalClaimKey != nil || before.Status != "acknowledged" {
		t.Fatalf("ordinary removed audit setup = %+v", before)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var after GatewayUpstreamGroupChange
	if err := db.First(&after, before.ID).Error; err != nil {
		t.Fatal(err)
	}
	if after.Status != "acknowledged" || after.AssignmentReasonCode != "" {
		t.Fatalf("ordinary removed audit changed = %+v", after)
	}
}

func TestGenerationSafeManagedKeyRemovalReconcileCompletesVerifiedIntent(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "verified-removal-intent", Type: ChannelTypeNewAPI, SiteURL: "https://verified-removal.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 23, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "verified-cipher", APIKeyPrefix: "sk-verified", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*local)
	if err != nil {
		t.Fatal(err)
	}
	generation := retireManagedKeyRemovalGeneration(t, store, channel, local.UpstreamKeyID, local.UpstreamKeyName)
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName, strings.Repeat("b", 64), ownershipClaim, time.Now(), 10*time.Minute)
	if err != nil || !claimed || intent == nil {
		t.Fatalf("verified intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var reconciled GatewayUpstreamGroupChange
	if err := db.First(&reconciled, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reconciled.Status != "acknowledged" || reconciled.ResolvedAt == nil || reconciled.FactsDigest == "" || reconciled.UpstreamGroupID == nil || reconciled.PresenceGeneration == nil {
		t.Fatalf("verified intent was not completed = %+v", reconciled)
	}
}

func TestGenerationSafeManagedKeyRemovalReconcileQuarantinesRemoteIdentityChanges(t *testing.T) {
	tests := []struct {
		name      string
		inventory map[int64]string
	}{
		{name: "same-name-new-id", inventory: map[int64]string{24: " BOG - promo "}},
		{name: "same-id-renamed", inventory: map[int64]string{23: "BOG - renamed"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := testdb.OpenWithError(t)
			if err != nil {
				t.Fatal(err)
			}
			channels := NewChannels(db)
			store := NewGateway(db)
			channel := &Channel{Name: "removal-identity-" + test.name, Type: ChannelTypeNewAPI, SiteURL: "https://removal-identity.example", MonitorEnabled: true}
			if err := channels.Create(channel); err != nil {
				t.Fatal(err)
			}
			local := &GatewayUpstreamKey{
				ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 23, UpstreamKeyName: "BOG - promo",
				OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
				APIKeyCipher:        "cipher", APIKeyPrefix: "sk-test", Enabled: true,
			}
			if err := store.UpsertUpstreamKey(local); err != nil {
				t.Fatal(err)
			}
			generation := retireManagedKeyRemovalGeneration(t, store, channel, local.UpstreamKeyID, local.UpstreamKeyName)
			intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
				generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName,
				strings.Repeat("b", 64), strings.Repeat("c", 64), time.Now(), 10*time.Minute,
			)
			if err != nil || !claimed || intent == nil {
				t.Fatalf("identity-change intent=%+v claimed=%v err=%v", intent, claimed, err)
			}
			if err := store.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, channel.UpstreamAccountIdentityRevision, test.inventory, time.Now()); err != nil {
				t.Fatal(err)
			}
			var reconciled GatewayUpstreamGroupChange
			if err := db.First(&reconciled, intent.ID).Error; err != nil {
				t.Fatal(err)
			}
			if reconciled.Status != "ignored" || reconciled.AssignmentState != "ignored" || reconciled.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview {
				t.Fatalf("identity-change intent was not quarantined = %+v", reconciled)
			}
			retained, err := store.FindUpstreamKeyAny(channel.ID, "promo")
			if err != nil || retained.Enabled || retained.UpstreamKeyID != local.UpstreamKeyID || retained.LastError != "managed upstream key removal requires manual review" {
				t.Fatalf("identity-change local key was not retained for review = %+v err=%v", retained, err)
			}
		})
	}
}

func TestManagedUpstreamKeyRemoteNameIndexTracksAmbiguousNames(t *testing.T) {
	nameIDs, ambiguous := managedUpstreamKeyRemoteNameIndex(map[int64]string{
		1: "BOG - promo",
		2: " BOG - promo ",
		3: "BOG - other",
	})
	if _, ok := ambiguous["BOG - promo"]; !ok || nameIDs["BOG - other"] != 3 {
		t.Fatalf("remote name index=%v ambiguous=%v", nameIDs, ambiguous)
	}
}

func TestGenerationSafeManagedKeyRemovalReconcileRejectsForgedV2ClaimKey(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "forged-removal-claim", Type: ChannelTypeNewAPI, SiteURL: "https://forged-removal.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	generation := retireManagedKeyRemovalGeneration(t, store, channel, 25, "BOG - promo")
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(generation, channel.UpstreamAccountIdentityRevision, "promo", 25, "BOG - promo", strings.Repeat("e", 64), strings.Repeat("d", 64), time.Now(), 10*time.Minute)
	if err != nil || !claimed || intent == nil {
		t.Fatalf("forged claim intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	forged := strings.Repeat("f", 64)
	if err := db.Model(&GatewayUpstreamGroupChange{}).Where("id = ?", intent.ID).Update("removal_claim_key", forged).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var reconciled GatewayUpstreamGroupChange
	if err := db.First(&reconciled, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reconciled.Status != "ignored" || reconciled.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview {
		t.Fatalf("forged v2 claim was not quarantined = %+v", reconciled)
	}
}

func TestGenerationSafeManagedKeyRemovalReconcileQuarantinesAcknowledgedIdentityThatReappears(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "acknowledged-reappeared-identity", Type: ChannelTypeNewAPI, SiteURL: "https://acknowledged.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 29, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "acknowledged-cipher", APIKeyPrefix: "sk-ack", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*local)
	if err != nil {
		t.Fatal(err)
	}
	generation := retireManagedKeyRemovalGeneration(t, store, channel, local.UpstreamKeyID, local.UpstreamKeyName)
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName, strings.Repeat("c", 64), ownershipClaim, time.Now(), 10*time.Minute)
	if err != nil || !claimed || intent == nil {
		t.Fatalf("acknowledged intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	if intent.RemovalClaimEpoch == nil {
		t.Fatalf("acknowledged intent has no claim epoch: %+v", intent)
	}
	if err := store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, intent.ID, local.ID,
		local.UpstreamKeyID, local.UpstreamKeyName, strings.Repeat("c", 64), ownershipClaim, time.Now(), nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{29: "BOG - promo"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var reconciled GatewayUpstreamGroupChange
	if err := db.First(&reconciled, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reconciled.Status != "ignored" || reconciled.AssignmentState != "ignored" || reconciled.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview {
		t.Fatalf("reappeared acknowledged identity was not quarantined = %+v", reconciled)
	}
}

func TestGenerationSafeManagedKeyRemovalClaimIsAtomicAcrossPostgresConnections(t *testing.T) {
	dbA, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	dbB, err := testdb.ReopenWithError(t, dbA)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(dbA)
	channel := &Channel{Name: "generation-safe-concurrent-removal", Type: ChannelTypeNewAPI, SiteURL: "https://generation-safe-concurrent.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	storeA := NewGateway(dbA)
	generation := retireManagedKeyRemovalGeneration(t, storeA, channel, 37, "BOG - promo")
	stores := []*Gateway{storeA, NewGateway(dbB)}
	start := make(chan struct{})
	type claimResult struct {
		change  *GatewayUpstreamGroupChange
		claimed bool
		err     error
	}
	results := make(chan claimResult, len(stores))
	for _, store := range stores {
		store := store
		go func() {
			<-start
			change, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
				generation, channel.UpstreamAccountIdentityRevision, "promo", 37, "BOG - promo", strings.Repeat("d", 64), strings.Repeat("e", 64), time.Now(), 10*time.Minute,
			)
			results <- claimResult{change: change, claimed: claimed, err: err}
		}()
	}
	close(start)
	claimedCount := 0
	var claimID uint
	for range stores {
		result := <-results
		if result.err != nil || result.change == nil {
			t.Fatalf("concurrent generation-safe claim = %+v", result)
		}
		if result.claimed {
			claimedCount++
		}
		if claimID == 0 {
			claimID = result.change.ID
		} else if result.change.ID != claimID {
			t.Fatalf("concurrent generation-safe claim ids differ: %d != %d", result.change.ID, claimID)
		}
	}
	if claimedCount != 1 {
		t.Fatalf("concurrent generation-safe claimed count = %d", claimedCount)
	}
}

func TestAbsentGenerationManualReviewDoesNotQuarantineReappearedGroup(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "manual-review-generation-fence", Type: ChannelTypeNewAPI, SiteURL: "https://manual-review-generation.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	oldKey := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 41, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "old-cipher", APIKeyPrefix: "sk-old", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(oldKey); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", oldKey.UpstreamKeyID, oldKey.UpstreamKeyName, time.Now()); err != nil {
		t.Fatal(err)
	}
	oldGeneration, err := store.RetireUpstreamGroupForKeyRemoval(channel.ID, channel.UpstreamAccountIdentityRevision, "promo", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*oldKey)
	if err != nil {
		t.Fatal(err)
	}
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
		oldGeneration, channel.UpstreamAccountIdentityRevision, "promo", oldKey.UpstreamKeyID, oldKey.UpstreamKeyName,
		strings.Repeat("a", 64), ownershipClaim, time.Now().Add(2*time.Second), 10*time.Minute,
	)
	if err != nil || !claimed || intent == nil {
		t.Fatalf("old generation removal intent=%+v claimed=%v err=%v", intent, claimed, err)
	}

	newKey := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 42, UpstreamKeyName: "BOG - promo replacement",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "new-cipher", APIKeyPrefix: "sk-new", Enabled: true,
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", newKey.UpstreamKeyID, newKey.UpstreamKeyName, time.Now().Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(newKey); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordManagedUpstreamKeyRemovalManualReview(
		oldGeneration, channel.UpstreamAccountIdentityRevision, "promo", 99, "BOG - stale audit", "manual_review_required", time.Now().Add(4*time.Second),
	); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale manual-review audit error = %v, want generation changed", err)
	}

	err = store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(
		oldGeneration, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, "promo", oldKey.UpstreamKeyID, oldKey.UpstreamKeyName, intent.ID, false, time.Now().Add(5*time.Second),
	)
	if !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale manual review error = %v, want generation changed", err)
	}
	current, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	if !current.Enabled || current.UpstreamKeyID != newKey.UpstreamKeyID || current.UpstreamKeyName != newKey.UpstreamKeyName ||
		current.APIKeyCipher != newKey.APIKeyCipher || current.APIKeyPrefix != newKey.APIKeyPrefix || current.LastError != "" {
		t.Fatalf("stale manual review changed reappeared generation key: %+v", current)
	}
	var reviewed GatewayUpstreamGroupChange
	if err := db.First(&reviewed, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reviewed.Status != "ignored" || reviewed.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview {
		t.Fatalf("old generation audit was not closed for manual review: %+v", reviewed)
	}
	var staleAuditCount int64
	if err := db.Model(&GatewayUpstreamGroupChange{}).Where("channel_id = ? AND upstream_key_id = ?", channel.ID, 99).Count(&staleAuditCount).Error; err != nil {
		t.Fatal(err)
	}
	if staleAuditCount != 0 {
		t.Fatalf("stale generation created %d manual-review audit rows", staleAuditCount)
	}
}

func TestFinalizeAbsentUpstreamGroupStateIsIdempotentForIgnoredManualReview(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "ignored-finalize-idempotent", Type: ChannelTypeNewAPI, SiteURL: "https://ignored-finalize.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 81, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "quarantined", APIKeyPrefix: "sk-review", Enabled: false,
		Provisioning: true, LastError: "managed upstream key removal requires manual review",
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", local.UpstreamKeyID, local.UpstreamKeyName, time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.RetireUpstreamGroupForKeyRemoval(channel.ID, channel.UpstreamAccountIdentityRevision, "promo", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordManagedUpstreamKeyRemovalManualReview(
		generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName,
		"revalidation_required", time.Now().Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}

	if err := store.FinalizeAbsentUpstreamGroupState(
		generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName,
		time.Now().Add(3*time.Second),
	); err != nil {
		t.Fatalf("repeated absent finalize = %v, want idempotent success", err)
	}
	retained, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	if retained.Enabled || !retained.Provisioning || retained.LastError != "managed upstream key removal requires manual review" {
		t.Fatalf("ignored manual-review evidence was changed: %+v", retained)
	}
}

func TestGenerationSafeManagedKeyRemovalFinalizeRejectsChangedChannelIdentity(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "removal-channel-revision", Type: ChannelTypeNewAPI, SiteURL: "https://removal-revision.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	oldRevision := channel.UpstreamAccountIdentityRevision
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 73, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "same-cipher-on-both-accounts", APIKeyPrefix: "sk-same", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	generation := retireManagedKeyRemovalGeneration(t, store, channel, local.UpstreamKeyID, local.UpstreamKeyName)
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*local)
	if err != nil {
		t.Fatal(err)
	}
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
		generation, oldRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName,
		strings.Repeat("a", 64), ownershipClaim, time.Now(), 10*time.Minute,
	)
	if err != nil || !claimed || intent == nil {
		t.Fatalf("begin old-account removal intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	if intent.RemovalChannelIdentityRevision == nil || *intent.RemovalChannelIdentityRevision != oldRevision {
		t.Fatalf("removal intent revision = %+v, want %d", intent.RemovalChannelIdentityRevision, oldRevision)
	}
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).
		Update("upstream_account_identity_revision", oldRevision+1).Error; err != nil {
		t.Fatal(err)
	}

	callbackCalls := 0
	err = store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		generation, oldRevision, *intent.RemovalClaimEpoch, intent.ID, local.ID, local.UpstreamKeyID, local.UpstreamKeyName,
		intent.FactsDigest, ownershipClaim, time.Now(), func() error {
			callbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale account finalize error = %v", err)
	}
	if callbackCalls != 0 {
		t.Fatalf("stale account executed %d remote delete callbacks", callbackCalls)
	}
	retained, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !retained.Enabled || retained.UpstreamKeyID != local.UpstreamKeyID || retained.APIKeyCipher != local.APIKeyCipher {
		t.Fatalf("new-account local state changed = %+v err=%v", retained, err)
	}
	var closed GatewayUpstreamGroupChange
	if err := db.First(&closed, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if closed.Status != "ignored" || closed.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview {
		t.Fatalf("old-account intent was not closed = %+v", closed)
	}
}

func TestGenerationSafeManagedKeyRemovalReconcileDoesNotConsumeAnotherChannelIdentity(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "removal-reconcile-revision", Type: ChannelTypeNewAPI, SiteURL: "https://removal-reconcile-revision.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	oldRevision := channel.UpstreamAccountIdentityRevision
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 79, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "cipher", APIKeyPrefix: "sk-reconcile", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	generation := retireManagedKeyRemovalGeneration(t, store, channel, local.UpstreamKeyID, local.UpstreamKeyName)
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*local)
	if err != nil {
		t.Fatal(err)
	}
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
		generation, oldRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName,
		strings.Repeat("b", 64), ownershipClaim, time.Now(), 10*time.Minute,
	)
	if err != nil || !claimed || intent == nil {
		t.Fatalf("begin old-account reconcile intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	newRevision := oldRevision + 1
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).
		Update("upstream_account_identity_revision", newRevision).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyRemovals(channel.ID, newRevision, map[int64]string{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	retained, err := store.FindUpstreamKeyAny(channel.ID, "promo")
	if err != nil || !retained.Enabled || retained.UpstreamKeyID != local.UpstreamKeyID {
		t.Fatalf("new-account local state changed by old reconcile = %+v err=%v", retained, err)
	}
	var closed GatewayUpstreamGroupChange
	if err := db.First(&closed, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if closed.Status != "ignored" || closed.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview {
		t.Fatalf("old-account reconcile intent was not closed = %+v", closed)
	}
}

func TestRetireUpstreamGroupForKeyRemovalRejectsStaleChannelIdentityBeforeMembershipMutation(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "retire-channel-revision", Type: ChannelTypeNewAPI, SiteURL: "https://retire-revision.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	identity, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	candidate := &GatewayCandidateGroup{Name: "retire-revision-candidate"}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}
	member := &GatewayCandidateGroupMember{
		CandidateGroupID: candidate.ID, UpstreamGroupID: identity.ID,
		ChannelID: channel.ID, GroupName: "promo",
	}
	if err := db.Create(member).Error; err != nil {
		t.Fatal(err)
	}
	oldRevision := channel.UpstreamAccountIdentityRevision
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).
		Update("upstream_account_identity_revision", oldRevision+1).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.RetireUpstreamGroupForKeyRemoval(channel.ID, oldRevision, "promo", time.Now()); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale identity retire error = %v", err)
	}
	var memberCount int64
	if err := db.Model(&GatewayCandidateGroupMember{}).Where("id = ?", member.ID).Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 1 {
		t.Fatalf("stale identity retire removed %d candidate members", 1-memberCount)
	}
	current, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil || current.ID != identity.ID || current.PresenceGeneration != identity.PresenceGeneration {
		t.Fatalf("stale identity retire changed group generation=%+v err=%v", current, err)
	}
}

func TestRetireUpstreamGroupForKeyRemovalAllowsReservedInternalIdentity(t *testing.T) {
	db := testdb.Open(t)
	channel := &Channel{
		Name: "internal-orphan-removal", Type: ChannelTypeSub2API,
		SiteURL: "https://internal-orphan-removal.example", MonitorEnabled: true,
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	groupName := ReservedInternalUpstreamGroupPrefix + "deleted_sub2api_group_id__:140"
	generation, err := NewGateway(db).RetireUpstreamGroupForKeyRemoval(
		channel.ID, channel.UpstreamAccountIdentityRevision, groupName, time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !generation.Valid() || generation.CanonicalGroupName != groupName {
		t.Fatalf("internal orphan generation = %+v", generation)
	}
}

func TestGenerationSafeManagedKeyRemovalClaimEpochFencesStaleFinalizer(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "removal-claim-epoch", Type: ChannelTypeNewAPI, SiteURL: "https://claim-epoch.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 83, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "claim-epoch-cipher", APIKeyPrefix: "sk-epoch", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	generation := retireManagedKeyRemovalGeneration(t, store, channel, local.UpstreamKeyID, local.UpstreamKeyName)
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*local)
	if err != nil {
		t.Fatal(err)
	}
	evidence := strings.Repeat("c", 64)
	first, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName,
		evidence, ownershipClaim, time.Now().Add(-2*time.Hour), time.Minute,
	)
	if err != nil || !claimed || first == nil || first.RemovalClaimEpoch == nil || *first.RemovalClaimEpoch != 1 {
		t.Fatalf("first epoch claim=%+v claimed=%v err=%v", first, claimed, err)
	}
	if err := db.Model(&GatewayUpstreamGroupChange{}).Where("id = ?", first.ID).
		Update("updated_at", time.Now().Add(-2*time.Hour)).Error; err != nil {
		t.Fatal(err)
	}
	second, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName,
		evidence, ownershipClaim, time.Now(), time.Minute,
	)
	if err != nil || !claimed || second == nil || second.RemovalClaimEpoch == nil || *second.RemovalClaimEpoch != 2 {
		t.Fatalf("reclaimed epoch claim=%+v claimed=%v err=%v", second, claimed, err)
	}
	callbackCalls := 0
	err = store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *first.RemovalClaimEpoch, first.ID, local.ID,
		local.UpstreamKeyID, local.UpstreamKeyName, evidence, ownershipClaim, time.Now(), func() error {
			callbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) {
		t.Fatalf("stale epoch finalize error = %v", err)
	}
	if callbackCalls != 0 {
		t.Fatalf("stale epoch executed %d remote delete callbacks", callbackCalls)
	}
	if err := store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *second.RemovalClaimEpoch, second.ID, local.ID,
		local.UpstreamKeyID, local.UpstreamKeyName, evidence, ownershipClaim, time.Now(), nil,
	); err != nil {
		t.Fatalf("current epoch finalize = %v", err)
	}
}

func TestGenerationSafeManagedKeyRemovalManualReviewCanResumeWithNewEpoch(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "removal-manual-resume", Type: ChannelTypeNewAPI, SiteURL: "https://manual-resume.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 89, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "manual-resume-cipher", APIKeyPrefix: "sk-resume", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	generation := retireManagedKeyRemovalGeneration(t, store, channel, local.UpstreamKeyID, local.UpstreamKeyName)
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*local)
	if err != nil {
		t.Fatal(err)
	}
	evidence := strings.Repeat("d", 64)
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, "promo", local.UpstreamKeyID, local.UpstreamKeyName,
		evidence, ownershipClaim, time.Now(), time.Minute,
	)
	if err != nil || !claimed || intent == nil || intent.RemovalClaimEpoch == nil {
		t.Fatalf("begin manual resume intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	if err := store.FinalizeAbsentGenerationManagedUpstreamKeyManualReview(
		generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, "promo", local.UpstreamKeyID, local.UpstreamKeyName, intent.ID, false, time.Now(),
	); err != nil {
		t.Fatal(err)
	}
	resumed, err := store.ResumeGenerationSafeManagedUpstreamKeyRemovalManualReview(
		generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, intent.ID, local.ID,
		local.UpstreamKeyID, local.UpstreamKeyName, evidence, ownershipClaim, "operator", "remote_revalidated", time.Now(),
	)
	if err != nil || resumed == nil || resumed.Status != "processing" || resumed.RemovalClaimEpoch == nil || *resumed.RemovalClaimEpoch != *intent.RemovalClaimEpoch+1 {
		t.Fatalf("manual resume=%+v err=%v", resumed, err)
	}
	if resumed.RemovalManualReviewResolution != "remote_revalidated" || resumed.RemovalManualReviewResolvedBy != "operator" || resumed.RemovalManualReviewResolvedAt == nil {
		t.Fatalf("manual resume audit metadata=%+v", resumed)
	}
	callbackCalls := 0
	err = store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, intent.ID, local.ID,
		local.UpstreamKeyID, local.UpstreamKeyName, evidence, ownershipClaim, time.Now(), func() error {
			callbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) || callbackCalls != 0 {
		t.Fatalf("pre-resume epoch finalize err=%v callbacks=%d", err, callbackCalls)
	}
	if err := store.FinalizeGenerationSafeManagedUpstreamKeyRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *resumed.RemovalClaimEpoch, resumed.ID, local.ID,
		local.UpstreamKeyID, local.UpstreamKeyName, evidence, ownershipClaim, time.Now(), nil,
	); err != nil {
		t.Fatalf("resumed epoch finalize = %v", err)
	}
}

func TestGenerationSafeManagedKeyInventoryRemovalSupportsUnboundDuplicates(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "inventory-duplicates", Type: ChannelTypeNewAPI, SiteURL: "https://inventory-duplicates.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const ownership = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	var callbacks int
	for _, remote := range []struct {
		id   int64
		name string
	}{{41, "BOG-p-aaaaaaaaaaaaaaaa"}, {42, "BOG-p-aaaaaaaaaaaaaaaa"}} {
		intent, claimed, beginErr := store.BeginGenerationSafeManagedUpstreamKeyInventoryRemoval(
			generation, channel.UpstreamAccountIdentityRevision, "promo", remote.id, remote.name, true,
			digest, ownership, time.Now().UTC(), time.Minute,
		)
		if beginErr != nil || !claimed || intent == nil || intent.RemovalClaimEpoch == nil {
			t.Fatalf("inventory begin remote=%+v intent=%+v claimed=%v err=%v", remote, intent, claimed, beginErr)
		}
		if finalizeErr := store.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(
			generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, intent.ID,
			remote.id, remote.name, true, digest, ownership, time.Now().UTC(), func() error {
				callbacks++
				return nil
			},
		); finalizeErr != nil {
			t.Fatalf("inventory finalize remote=%+v: %v", remote, finalizeErr)
		}
		var audited GatewayUpstreamGroupChange
		if err := db.First(&audited, intent.ID).Error; err != nil {
			t.Fatal(err)
		}
		if audited.Status != "acknowledged" || audited.ResolvedAt == nil {
			t.Fatalf("inventory audit remote=%+v: %+v", remote, audited)
		}
	}
	if callbacks != 2 {
		t.Fatalf("inventory callbacks=%d, want 2", callbacks)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("unbound inventory created local key: %v", err)
	}
}

func TestGenerationSafeManagedKeyInventoryRemovalAbsentGenerationPurgesAndFences(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "inventory-absent", Type: ChannelTypeNewAPI, SiteURL: "https://inventory-absent.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 73, UpstreamKeyName: "BOG-p-0123456789abcdef", APIKeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	generation, err := store.RetireUpstreamGroupForKeyRemoval(channel.ID, channel.UpstreamAccountIdentityRevision, "promo", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	const digest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const ownership = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyInventoryRemoval(
		generation, channel.UpstreamAccountIdentityRevision, "promo", 73, "BOG-p-0123456789abcdef", false,
		digest, ownership, time.Now().UTC(), time.Minute,
	)
	if err != nil || !claimed || intent == nil || intent.RemovalClaimEpoch == nil {
		t.Fatalf("absent inventory begin intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	callbackCalls := 0
	if err := store.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, intent.ID, 73,
		"BOG-p-0123456789abcdef", false, digest, ownership, time.Now().UTC(), func() error {
			callbackCalls++
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if callbackCalls != 1 {
		t.Fatalf("absent inventory callback calls=%d, want 1", callbackCalls)
	}
	if _, err := store.FindUpstreamKeyAny(channel.ID, "promo"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("absent inventory retained local key: %v", err)
	}
	var audited GatewayUpstreamGroupChange
	if err := db.First(&audited, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if audited.Status != "acknowledged" || audited.ResolvedAt == nil {
		t.Fatalf("absent inventory audit: %+v", audited)
	}
}

func TestGenerationSafeManagedKeyInventoryRemovalClearsExactBindingOutsideGroupIdentity(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "inventory-exact-binding", Type: ChannelTypeSub2API, SiteURL: "https://inventory-exact-binding.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	const remoteID = int64(74)
	const remoteName = "BOG-p-fedcba9876543210"
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "legacy-name", UpstreamKeyID: remoteID, UpstreamKeyName: remoteName,
		APIKeyCipher: "cipher", APIKeyPrefix: "sk", OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		Enabled: true, Provisioning: true,
	}); err != nil {
		t.Fatal(err)
	}
	internalGroup := ReservedInternalUpstreamGroupPrefix + "deleted_sub2api_group_id__:999"
	if err := store.RecordUpstreamGroupChange(channel.ID, internalGroup, "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.RetireUpstreamGroupForKeyRemoval(channel.ID, channel.UpstreamAccountIdentityRevision, internalGroup, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	const digest = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	const ownership = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyInventoryRemoval(
		generation, channel.UpstreamAccountIdentityRevision, internalGroup, remoteID, remoteName, false,
		digest, ownership, time.Now().UTC(), time.Minute,
	)
	if err != nil || !claimed || intent == nil || intent.RemovalClaimEpoch == nil {
		t.Fatalf("begin exact binding removal intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	if err := store.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, intent.ID, remoteID,
		remoteName, false, digest, ownership, time.Now().UTC(), func() error { return nil },
	); err != nil {
		t.Fatal(err)
	}
	local, err := store.FindUpstreamKeyAny(channel.ID, "legacy-name")
	if err != nil {
		t.Fatal(err)
	}
	if local.UpstreamKeyID != 0 || local.UpstreamKeyName != "" || local.APIKeyCipher != "" || local.APIKeyPrefix != "" ||
		local.OwnershipProvenance != GatewayUpstreamKeyProvenanceUnknown || local.Enabled || local.Provisioning {
		t.Fatalf("exact binding was not cleared atomically: %+v", local)
	}
	local.UpstreamKeyID = remoteID
	local.UpstreamKeyName = remoteName
	local.APIKeyCipher = "legacy-cipher"
	local.APIKeyPrefix = "sk"
	local.OwnershipProvenance = GatewayUpstreamKeyProvenanceCreated
	local.Enabled = true
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyInventoryRemovals(
		channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	repaired, err := store.FindUpstreamKeyAny(channel.ID, "legacy-name")
	if err != nil || repaired.UpstreamKeyID != 0 || repaired.UpstreamKeyName != "" || repaired.APIKeyCipher != "" || repaired.Enabled {
		t.Fatalf("acknowledged exact binding repair = %+v, err=%v", repaired, err)
	}
	if err := store.ReconcileGenerationSafeManagedUpstreamKeyInventoryRemovals(
		channel.ID, channel.UpstreamAccountIdentityRevision, map[int64]string{remoteID: remoteName}, time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	var reviewed GatewayUpstreamGroupChange
	if err := db.First(&reviewed, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reviewed.Status != "ignored" || reviewed.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview ||
		reviewed.RemovalManualReviewReason != "remote_identity_reappeared" {
		t.Fatalf("reappeared acknowledged inventory intent = %+v", reviewed)
	}
}

func TestGenerationSafeManagedKeyInventoryRemovalDoesNotDeleteAcrossGeneration(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "inventory-generation-fence", Type: ChannelTypeNewAPI, SiteURL: "https://inventory-generation-fence.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	const digest = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	const ownership = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	intent, claimed, err := store.BeginGenerationSafeManagedUpstreamKeyInventoryRemoval(
		generation, channel.UpstreamAccountIdentityRevision, "promo", 91, "BOG-p-fedcba9876543210", true,
		digest, ownership, time.Now().UTC(), time.Minute,
	)
	if err != nil || !claimed || intent == nil || intent.RemovalClaimEpoch == nil {
		t.Fatalf("generation-fence begin intent=%+v claimed=%v err=%v", intent, claimed, err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "removed", 0, "", time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	callbackCalls := 0
	err = store.FinalizeGenerationSafeManagedUpstreamKeyInventoryRemoval(
		generation, channel.UpstreamAccountIdentityRevision, *intent.RemovalClaimEpoch, intent.ID, 91,
		"BOG-p-fedcba9876543210", true, digest, ownership, time.Now().UTC().Add(2*time.Second), func() error {
			callbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrUpstreamGroupGenerationChanged) || callbackCalls != 0 {
		t.Fatalf("generation-fence finalize err=%v callbacks=%d", err, callbackCalls)
	}
	var audited GatewayUpstreamGroupChange
	if err := db.First(&audited, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if audited.Status != "ignored" || audited.AssignmentReasonCode != managedUpstreamKeyRemovalManualReview {
		t.Fatalf("generation-fence audit: %+v", audited)
	}
}
