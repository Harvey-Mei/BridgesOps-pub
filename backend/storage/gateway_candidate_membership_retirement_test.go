package storage

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestRateSnapshotDeletionPreservesPersistentMembershipAndRetiresAutomaticClaims(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: "membership retirement", Type: ChannelTypeNewAPI,
		SiteURL: "https://membership-retirement.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "Primary", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "Primary")
	if err := store.RecordUpstreamGroupChange(channel.ID, "Primary", "added", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var pendingAdded GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND kind = ?", channel.ID, "added").First(&pendingAdded).Error; err != nil {
		t.Fatal(err)
	}

	manualGroup := &GatewayCandidateGroup{Name: "persistent administrator target"}
	if err := store.SaveCandidateGroup(manualGroup, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "Primary"}}); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}

	automaticGroup := &GatewayCandidateGroup{Name: "derived automatic target"}
	if err := db.Create(automaticGroup).Error; err != nil {
		t.Fatal(err)
	}
	var automaticClaim GatewayCandidateGroupMembershipClaim
	if err := db.Transaction(func(tx *gorm.DB) error {
		var identity GatewayUpstreamGroup
		if err := tx.First(&identity, generation.ID).Error; err != nil {
			return err
		}
		if err := ensureMembershipClaimTx(tx, automaticGroup.ID, &identity, "Primary", "auto_rule", 7001, time.Now().UTC()); err != nil {
			return err
		}
		return tx.Where(
			"candidate_group_id = ? AND upstream_group_id = ? AND presence_generation = ? AND source_kind = ?",
			automaticGroup.ID, generation.ID, generation.PresenceGeneration, "auto_rule",
		).First(&automaticClaim).Error
	}); err != nil {
		t.Fatal(err)
	}

	for _, sourceKind := range []string{"manual", "override", "auto_rule"} {
		var count int64
		if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
			Where("upstream_group_id = ? AND presence_generation = ? AND source_kind = ? AND active = ?", generation.ID, generation.PresenceGeneration, sourceKind, true).
			Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Fatalf("missing active %s claim before rate deletion", sourceKind)
		}
	}

	if err := rates.DeleteSnapshot(channel.ID, "Primary"); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&pendingAdded, pendingAdded.ID).Error; err != nil || pendingAdded.Status != "superseded" || pendingAdded.AssignmentReasonCode != "group_removed" {
		t.Fatalf("pending automatic assignment after rate deletion = %+v err=%v", pendingAdded, err)
	}
	var removedChanges int64
	if err := db.Model(&GatewayUpstreamGroupChange{}).
		Where("upstream_group_id = ? AND presence_generation = ? AND kind = ?", generation.ID, generation.PresenceGeneration, "removed").
		Count(&removedChanges).Error; err != nil {
		t.Fatal(err)
	}
	if removedChanges != 1 {
		t.Fatalf("rate deletion appended %d removed changes, want 1", removedChanges)
	}
	preservedGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "PRIMARY")
	if err != nil || preservedGeneration.ID != generation.ID || preservedGeneration.PresenceGeneration != generation.PresenceGeneration {
		t.Fatalf("persistent no-rate generation = %+v err=%v, want %+v", preservedGeneration, err, generation)
	}
	for _, sourceKind := range []string{"manual", "override"} {
		var count int64
		if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
			Where("upstream_group_id = ? AND presence_generation = ? AND source_kind = ? AND active = ?", generation.ID, generation.PresenceGeneration, sourceKind, true).
			Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Fatalf("rate deletion revoked persistent %s claim", sourceKind)
		}
	}
	if err := db.First(&automaticClaim, automaticClaim.ID).Error; err != nil || automaticClaim.Active || automaticClaim.RevokedAt == nil {
		t.Fatalf("automatic claim after rate deletion = %+v err=%v", automaticClaim, err)
	}
	assertCandidateMembershipCount(t, db, manualGroup.ID, generation.ID, 1)
	assertCandidateMembershipCount(t, db, automaticGroup.ID, generation.ID, 0)

	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "PRIMARY", Ratio: 2}); err != nil {
		t.Fatal(err)
	}
	restoredGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil || restoredGeneration.PresenceGeneration != generation.PresenceGeneration {
		t.Fatalf("pricing restore changed retained generation = %+v err=%v", restoredGeneration, err)
	}
	assertCandidateMembershipCount(t, db, automaticGroup.ID, generation.ID, 0)

	if err := rates.DeleteSnapshot(channel.ID, "PRIMARY"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "Primary", "removed", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary"); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("real removal left generation present: %v", err)
	}
	assertCandidateMembershipCount(t, db, manualGroup.ID, generation.ID, 0)
	if err := store.RecordUpstreamGroupChange(channel.ID, "PRIMARY", "added", 2, "replacement", time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	replacement, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil || replacement.PresenceGeneration != generation.PresenceGeneration+1 {
		t.Fatalf("replacement generation = %+v err=%v, initial=%+v", replacement, err, generation)
	}
	assertCandidateMembershipCount(t, db, automaticGroup.ID, replacement.ID, 0)
	var replacementAutomaticClaims int64
	if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("upstream_group_id = ? AND presence_generation = ? AND source_kind = ? AND active = ?", replacement.ID, replacement.PresenceGeneration, "auto_rule", true).
		Count(&replacementAutomaticClaims).Error; err != nil {
		t.Fatal(err)
	}
	if replacementAutomaticClaims != 0 {
		t.Fatalf("old automatic claim crossed into replacement generation: %d", replacementAutomaticClaims)
	}
}

func TestPreservedCandidateRetirementDoesNotCarryAutomaticClaimIntoReplacementGeneration(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: "preserved rebuild retirement", Type: ChannelTypeNewAPI,
		SiteURL: "https://preserved-rebuild-retirement.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "Primary", Ratio: 1}); err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "Primary")
	manualGroup := &GatewayCandidateGroup{Name: "rebuild persistent target"}
	if err := store.SaveCandidateGroup(manualGroup, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "Primary"}}); err != nil {
		t.Fatal(err)
	}
	initial, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	automaticGroup := &GatewayCandidateGroup{Name: "rebuild automatic target"}
	if err := db.Create(automaticGroup).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		var identity GatewayUpstreamGroup
		if err := tx.First(&identity, initial.ID).Error; err != nil {
			return err
		}
		return ensureMembershipClaimTx(tx, automaticGroup.ID, &identity, "Primary", "auto_rule", 8001, time.Now().UTC())
	}); err != nil {
		t.Fatal(err)
	}

	// Model the already-fenced inventory removal that precedes managed-key
	// retirement. The production caller holds the same canonical domain lock.
	if err := db.Where("channel_id = ? AND model_name = ?", channel.ID, "Primary").Delete(&RateSnapshot{}).Error; err != nil {
		t.Fatal(err)
	}
	retired, err := store.RetireUpstreamGroupForKeyRemoval(
		channel.ID, channel.UpstreamAccountIdentityRevision, "primary", time.Now().UTC(), true,
	)
	if err != nil || retired.ID != initial.ID || retired.PresenceGeneration != initial.PresenceGeneration {
		t.Fatalf("preserved candidate retirement = %+v err=%v, initial=%+v", retired, err, initial)
	}
	assertCandidateMembershipCount(t, db, manualGroup.ID, initial.ID, 1)
	assertCandidateMembershipCount(t, db, automaticGroup.ID, initial.ID, 0)
	var oldAutomaticClaims int64
	if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("upstream_group_id = ? AND presence_generation = ? AND source_kind = ? AND active = ?", initial.ID, initial.PresenceGeneration, "auto_rule", true).
		Count(&oldAutomaticClaims).Error; err != nil {
		t.Fatal(err)
	}
	if oldAutomaticClaims != 0 {
		t.Fatalf("preserved retirement left %d automatic claims active", oldAutomaticClaims)
	}

	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "PRIMARY", Ratio: 2}); err != nil {
		t.Fatal(err)
	}
	replacement, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil || replacement.PresenceGeneration != initial.PresenceGeneration+1 {
		t.Fatalf("replacement after preserved retirement = %+v err=%v, initial=%+v", replacement, err, initial)
	}
	assertCandidateMembershipCount(t, db, manualGroup.ID, replacement.ID, 1)
	assertCandidateMembershipCount(t, db, automaticGroup.ID, replacement.ID, 0)

	if err := rates.DeleteSnapshot(channel.ID, "PRIMARY"); err != nil {
		t.Fatal(err)
	}
	retainedReplacement, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil || retainedReplacement.PresenceGeneration != replacement.PresenceGeneration {
		t.Fatalf("second no-rate generation = %+v err=%v, replacement=%+v", retainedReplacement, err, replacement)
	}
	assertCandidateMembershipCount(t, db, manualGroup.ID, replacement.ID, 1)
	assertCandidateMembershipCount(t, db, automaticGroup.ID, replacement.ID, 0)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "removed", 0, "", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	assertCandidateMembershipCount(t, db, manualGroup.ID, replacement.ID, 0)
	var activePersistentClaims int64
	if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("upstream_group_id = ? AND presence_generation <= ? AND source_kind IN ? AND active = ?", replacement.ID, replacement.PresenceGeneration, []string{"manual", "override"}, true).
		Count(&activePersistentClaims).Error; err != nil {
		t.Fatal(err)
	}
	if activePersistentClaims != 0 {
		t.Fatalf("real removal left %d older persistent claims active", activePersistentClaims)
	}
}

func assertCandidateMembershipCount(t *testing.T, db *gorm.DB, candidateGroupID, upstreamGroupID uint, want int64) {
	t.Helper()
	var count int64
	if err := db.Model(&GatewayCandidateGroupMember{}).
		Where("candidate_group_id = ? AND upstream_group_id = ?", candidateGroupID, upstreamGroupID).
		Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("candidate %d upstream %d member count = %d, want %d", candidateGroupID, upstreamGroupID, count, want)
	}
}
