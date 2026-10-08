package storage

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestAssignUpstreamGroupChangeRejectsPreviousPresenceGeneration(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: "stale manual assignment generation", Type: ChannelTypeNewAPI,
		SiteURL: "https://stale-manual-assignment.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, groupName := range []string{"promo", "seed"} {
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1, LastSeenAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "seed", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "seed-cipher", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	target := &GatewayCandidateGroup{Name: "stale generation target"}
	if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 1, "old-key", base); err != nil {
		t.Fatal(err)
	}
	var oldChange GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ? AND kind = ?", channel.ID, "promo", "added").Order("id ASC").First(&oldChange).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "removed", 1, "old-key", base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 2, "new-key", base.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var currentChange GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ? AND kind = ?", channel.ID, "promo", "added").Order("id DESC").First(&currentChange).Error; err != nil {
		t.Fatal(err)
	}
	if oldChange.PresenceGeneration == nil || currentChange.PresenceGeneration == nil || *oldChange.PresenceGeneration == *currentChange.PresenceGeneration {
		t.Fatalf("changes did not span generations: old=%+v current=%+v", oldChange, currentChange)
	}

	err := store.AssignUpstreamGroupChange(oldChange.ID, []uint{target.ID}, base.Add(3*time.Minute))
	if !errors.Is(err, ErrUpstreamGroupChangeSuperseded) {
		t.Fatalf("stale generation assignment error = %v", err)
	}
	if err := db.First(&oldChange, oldChange.ID).Error; err != nil {
		t.Fatal(err)
	}
	if oldChange.Status != "superseded" || oldChange.ResolvedAt == nil {
		t.Fatalf("stale change was not atomically superseded: %+v", oldChange)
	}
	if err := db.First(&currentChange, currentChange.ID).Error; err != nil {
		t.Fatal(err)
	}
	if currentChange.Status != "pending" {
		t.Fatalf("current generation change was modified: %+v", currentChange)
	}
	storedTarget, err := store.FindCandidateGroup(target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(storedTarget.Members) != 1 || storedTarget.Members[0].GroupName != "seed" {
		t.Fatalf("stale assignment changed membership: %+v", storedTarget.Members)
	}
}

func newManualAssignmentKeyFixture(t *testing.T, changeKeyID int64, changeKeyName string, current *GatewayUpstreamKey) (*Gateway, *gorm.DB, *GatewayCandidateGroup, GatewayUpstreamGroupChange) {
	t.Helper()
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "manual assignment key gate", Type: ChannelTypeNewAPI, SiteURL: "https://manual-assignment-key-gate.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, groupName := range []string{"promo", "seed"} {
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1, LastSeenAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "seed", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "seed-cipher", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	target := &GatewayCandidateGroup{Name: "manual assignment key target"}
	if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", changeKeyID, changeKeyName, base); err != nil {
		t.Fatal(err)
	}
	if current != nil {
		current.ChannelID = channel.ID
		current.GroupName = "PROMO"
		if current.OwnershipProvenance == "" {
			current.OwnershipProvenance = GatewayUpstreamKeyProvenanceAdopted
		}
		if err := store.UpsertUpstreamKey(current); err != nil {
			t.Fatal(err)
		}
	}
	var change GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ? AND kind = ?", channel.ID, "promo", "added").Order("id DESC").First(&change).Error; err != nil {
		t.Fatal(err)
	}
	return store, db, target, change
}

func TestAssignUpstreamGroupChangeRejectsMissingCurrentKey(t *testing.T) {
	store, db, target, change := newManualAssignmentKeyFixture(t, 7, "managed-promo", nil)
	err := store.AssignUpstreamGroupChange(change.ID, []uint{target.ID}, time.Now().UTC())
	if !errors.Is(err, ErrUpstreamGroupAssignmentKeyUnavailable) {
		t.Fatalf("missing current key assignment error = %v", err)
	}
	if err := db.First(&change, change.ID).Error; err != nil {
		t.Fatal(err)
	}
	if change.Status != "pending" {
		t.Fatalf("missing-key change status = %+v", change)
	}
}

func TestAssignUpstreamGroupChangeAllowsCurrentGenerationKeyReplacement(t *testing.T) {
	store, _, target, change := newManualAssignmentKeyFixture(t, 7, "managed-promo", &GatewayUpstreamKey{
		UpstreamKeyID: 8, UpstreamKeyName: "managed-other", OwnershipProvenance: GatewayUpstreamKeyProvenanceUnknown,
		Enabled: true, APIKeyCipher: "cipher",
	})
	if err := store.AssignUpstreamGroupChange(change.ID, []uint{target.ID}, time.Now().UTC()); err != nil {
		t.Fatalf("current generation key replacement assignment: %v", err)
	}
	stored, err := store.FindCandidateGroup(target.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, member := range stored.Members {
		if member.ChannelID == change.ChannelID && member.UpstreamGroupID == *change.UpstreamGroupID {
			found = true
		}
	}
	if !found {
		t.Fatalf("assigned candidate members = %+v", stored.Members)
	}
}

func TestAssignUpstreamGroupChangeAllowsInitialUnknownKeyIDWithUsableOwnedKey(t *testing.T) {
	store, _, target, change := newManualAssignmentKeyFixture(t, 0, "BOG - promo-placeholder", &GatewayUpstreamKey{
		UpstreamKeyID: 9, UpstreamKeyName: "BOG-p-random-provider-name", Enabled: true, APIKeyCipher: "cipher",
	})
	if err := store.AssignUpstreamGroupChange(change.ID, []uint{target.ID}, time.Now().UTC()); err != nil {
		t.Fatalf("initial unknown key id assignment: %v", err)
	}
	stored, err := store.FindCandidateGroup(target.ID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, member := range stored.Members {
		if member.ChannelID == change.ChannelID && member.UpstreamGroupID == *change.UpstreamGroupID {
			found = true
		}
	}
	if !found {
		t.Fatalf("assigned candidate members = %+v", stored.Members)
	}
}

func TestAssignUpstreamGroupChangeRejectsStaleAccountIdentityRevision(t *testing.T) {
	store, db, target, change := newManualAssignmentKeyFixture(t, 7, "managed-promo", &GatewayUpstreamKey{
		UpstreamKeyID: 7, UpstreamKeyName: "managed-promo", Enabled: true, APIKeyCipher: "cipher",
	})
	var channel Channel
	if err := db.Select("id", "upstream_account_identity_revision").First(&channel, change.ChannelID).Error; err != nil {
		t.Fatal(err)
	}
	staleRevision := channel.UpstreamAccountIdentityRevision
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).
		Update("upstream_account_identity_revision", staleRevision+1).Error; err != nil {
		t.Fatal(err)
	}

	err := store.AssignUpstreamGroupChangeAsWithIdentityRevision(
		change.ID, []uint{target.ID}, "operator:stale-account", time.Now().UTC(), staleRevision,
	)
	if !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale account assignment error = %v", err)
	}
	if err := db.First(&change, change.ID).Error; err != nil {
		t.Fatal(err)
	}
	if change.Status != "pending" {
		t.Fatalf("stale account assignment changed status: %+v", change)
	}
	storedTarget, err := store.FindCandidateGroup(target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(storedTarget.Members) != 1 || storedTarget.Members[0].GroupName != "seed" {
		t.Fatalf("stale account assignment changed membership: %+v", storedTarget.Members)
	}
}

func TestSaveCandidateGroupCurrentGenerationRejectsStaleEditorSnapshot(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "candidate generation CAS", Type: ChannelTypeNewAPI, SiteURL: "https://candidate-generation-cas.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 1, UpstreamKeyName: "managed",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "managed", base); err != nil {
		t.Fatal(err)
	}
	stale, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	candidate := &GatewayCandidateGroup{Name: "generation CAS target"}
	if err := store.SaveCandidateGroup(candidate, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "primary"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 1, "managed", base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 2, "managed-2", base.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	updated := &GatewayCandidateGroup{ID: candidate.ID, Name: "must roll back"}
	err = store.SaveCandidateGroupCurrentGenerationAs(updated, []GatewayCandidateGroupMember{{
		ChannelID: channel.ID, GroupName: "primary", UpstreamGroupID: stale.ID, PresenceGeneration: stale.PresenceGeneration,
	}}, "operator:stale-editor")
	if !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale candidate editor error = %v", err)
	}
	stored, err := store.FindCandidateGroup(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != candidate.Name || len(stored.Members) != 0 {
		t.Fatalf("stale candidate save was not rolled back: %+v", stored)
	}
}
