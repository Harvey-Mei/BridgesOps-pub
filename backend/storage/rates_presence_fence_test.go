package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func TestRateSnapshotUpsertAdvancesRetiredPresenceGeneration(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: "rate-presence-generation", Type: ChannelTypeNewAPI,
		SiteURL: "https://rate-presence.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}

	firstSeen := time.Now().UTC().Add(-time.Minute)
	if _, err := rates.Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "Promo", Ratio: 1, LastSeenAt: firstSeen,
	}); err != nil {
		t.Fatalf("insert initial rate: %v", err)
	}
	first, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil || first.PresenceGeneration != 1 {
		t.Fatalf("initial generation = %+v, err=%v", first, err)
	}

	if err := rates.DeleteSnapshot(channel.ID, "Promo"); err != nil {
		t.Fatalf("delete initial rate: %v", err)
	}
	retired, err := store.RetireUpstreamGroupForKeyRemoval(channel.ID, channel.UpstreamAccountIdentityRevision, "PROMO", time.Now().UTC())
	if err != nil || retired.PresenceGeneration != first.PresenceGeneration {
		t.Fatalf("retired generation = %+v, err=%v", retired, err)
	}
	if _, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo"); err == nil {
		t.Fatal("retired generation remained present")
	}

	if _, err := rates.Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "Promo", Ratio: 1.25, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("restore rate: %v", err)
	}
	restored, err := store.CaptureUpstreamGroupGeneration(channel.ID, " promo ")
	if err != nil {
		t.Fatalf("capture restored generation: %v", err)
	}
	if restored.ID != first.ID || restored.PresenceGeneration != first.PresenceGeneration+1 {
		t.Fatalf("restored generation = %+v, first=%+v", restored, first)
	}
}

func TestRateSnapshotWritesRejectReservedInternalGroupNamespace(t *testing.T) {
	reserved := "  " + strings.ToUpper(ReservedInternalUpstreamGroupPrefix) + "provider_collision  "
	rates := NewRates(&gorm.DB{})

	if _, err := rates.Upsert(&RateSnapshot{
		ChannelID: 1, ModelName: reserved, Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); !errors.Is(err, ErrReservedInternalUpstreamGroupName) {
		t.Fatalf("single rate write error = %v", err)
	}

	if _, err := rates.ReplaceChannelSnapshotForCostUnitEpoch(RateSnapshotBatchInput{
		ChannelID: 1, ExpectedIdentityRevision: 1, ExpectedCostUnitEpoch: 1,
		ObservedAt: time.Now().UTC(), Items: []RateSnapshot{{ModelName: reserved, Ratio: 1}},
	}); !errors.Is(err, ErrReservedInternalUpstreamGroupName) {
		t.Fatalf("batch rate write error = %v", err)
	}
}

func TestCanonicalRateSnapshotPresenceHelpersShareRoutingDomain(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	channel := &Channel{
		Name: "canonical-rate-presence", Type: ChannelTypeNewAPI,
		SiteURL: "https://canonical-rate.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "VIP", Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		canonical, err := lockRateSnapshotPresenceDomainTx(tx, channel.ID, " vIp ")
		if err != nil {
			return err
		}
		if canonical != "vip" {
			t.Fatalf("canonical group = %q", canonical)
		}
		present, err := canonicalRateSnapshotExistsTx(tx, channel.ID, canonical)
		if err != nil {
			return err
		}
		if !present {
			t.Fatal("canonical rate was not found while its domain was locked")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := rates.DeleteSnapshot(channel.ID, "VIP"); err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		canonical, err := lockRateSnapshotPresenceDomainTx(tx, channel.ID, "vip")
		if err != nil {
			return err
		}
		present, err := canonicalRateSnapshotExistsTx(tx, channel.ID, canonical)
		if err != nil {
			return err
		}
		if present {
			t.Fatal("deleted canonical rate remained present")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRetireUpstreamGroupSerializesConcurrentCanonicalRateRestore(t *testing.T) {
	dbA := testdb.Open(t)
	dbB, err := testdb.ReopenWithError(t, dbA)
	if err != nil {
		t.Fatal(err)
	}
	dbC, err := testdb.ReopenWithError(t, dbA)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(dbA)
	channel := &Channel{
		Name: "concurrent-rate-restore", Type: ChannelTypeNewAPI,
		SiteURL: "https://concurrent-rate.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	storeA := NewGateway(dbA)
	if err := storeA.RecordUpstreamGroupChange(channel.ID, "Promo", "added", 1, "managed", base); err != nil {
		t.Fatal(err)
	}

	holderPID, releaseAssignment, assignmentDone := holdAutomaticAssignmentFenceForPresenceTest(t, dbA)

	type retireResult struct {
		generation UpstreamGroupGeneration
		err        error
	}
	retireStarted := make(chan struct{})
	retired := make(chan retireResult, 1)
	go func() {
		close(retireStarted)
		generation, err := NewGateway(dbB).RetireUpstreamGroupForKeyRemoval(channel.ID, channel.UpstreamAccountIdentityRevision, " promo ", base.Add(time.Second))
		retired <- retireResult{generation: generation, err: err}
	}()
	<-retireStarted
	if err := waitForAutomaticAssignmentFenceWaiter(dbC, holderPID, 10*time.Second); err != nil {
		close(releaseAssignment)
		_ = waitForPresenceTestError(assignmentDone, 10*time.Second)
		t.Fatal(err)
	}

	restored := make(chan error, 1)
	go func() {
		_, err := NewRates(dbC).Upsert(&RateSnapshot{
			ChannelID: channel.ID, ModelName: "PROMO", Ratio: 1, LastSeenAt: base.Add(2 * time.Second),
		})
		restored <- err
	}()
	close(releaseAssignment)
	if err := waitForPresenceTestError(assignmentDone, 10*time.Second); err != nil {
		t.Fatalf("release automatic-assignment lock: %v", err)
	}
	var result retireResult
	select {
	case result = <-retired:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for upstream group retirement")
	}
	if err := waitForPresenceTestError(restored, 10*time.Second); err != nil {
		t.Fatalf("restore rate while retire waits: %v", err)
	}
	if result.err != nil || !result.generation.Valid() || result.generation.PresenceGeneration != 1 {
		t.Fatalf("retire result = %+v, err=%v", result.generation, result.err)
	}

	current, err := storeA.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatalf("capture current generation: %v", err)
	}
	if current.PresenceGeneration != 2 {
		t.Fatalf("queued restore did not advance the retired generation: %+v", current)
	}
}

func TestChannelDeletePrepareWaitsForEveryRateSnapshotPresenceFence(t *testing.T) {
	dbA := testdb.Open(t)
	dbB, err := testdb.ReopenWithError(t, dbA)
	if err != nil {
		t.Fatal(err)
	}
	dbC, err := testdb.ReopenWithError(t, dbA)
	if err != nil {
		t.Fatal(err)
	}
	channel := &Channel{
		Name: "channel-delete-presence-fence", Type: ChannelTypeNewAPI,
		SiteURL: "https://channel-delete-presence.example", MonitorEnabled: true,
	}
	if err := NewChannels(dbA).Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRates(dbA).Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "VIP", Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	holderPID, releaseFence, fenceDone := holdRateSnapshotPresenceFenceForTest(t, dbA, channel.ID, "vip")
	deleted := make(chan error, 1)
	go func() { deleted <- NewChannels(dbB).Delete(channel.ID) }()
	if err := waitForAutomaticAssignmentFenceWaiter(dbC, holderPID, 10*time.Second); err != nil {
		close(releaseFence)
		_ = waitForPresenceTestError(fenceDone, 10*time.Second)
		t.Fatal(err)
	}
	lateRate := make(chan error, 1)
	go func() {
		_, err := NewRates(dbC).Upsert(&RateSnapshot{
			ChannelID: channel.ID, ModelName: "brand-new", Ratio: 1, LastSeenAt: time.Now().UTC(),
		})
		lateRate <- err
	}()
	close(releaseFence)
	if err := waitForPresenceTestError(fenceDone, 10*time.Second); err != nil {
		t.Fatalf("release channel-delete presence fence: %v", err)
	}
	if err := waitForPresenceTestError(deleted, 10*time.Second); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion after presence fence: %v", err)
	}
	if err := waitForPresenceTestError(lateRate, 10*time.Second); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("late new-canonical rate error = %v, want channel deletion pending", err)
	}
	var lateRateCount int64
	if err := dbA.Model(&RateSnapshot{}).
		Where("channel_id = ? AND lower(btrim(model_name)) = lower(btrim(?))", channel.ID, "brand-new").
		Count(&lateRateCount).Error; err != nil {
		t.Fatalf("count rejected late rate: %v", err)
	}
	if lateRateCount != 0 {
		t.Fatalf("pending channel deletion admitted %d late rates", lateRateCount)
	}

	checks := []struct {
		name  string
		model any
		query string
	}{
		{name: "channel", model: &Channel{}, query: "id = ?"},
		{name: "identity", model: &GatewayUpstreamGroup{}, query: "channel_id = ?"},
	}
	for _, check := range checks {
		var count int64
		if err := dbA.Model(check.model).Where(check.query, channel.ID).Count(&count).Error; err != nil {
			t.Fatalf("count %s after channel delete prepare: %v", check.name, err)
		}
		if count == 0 {
			t.Fatalf("delete prepare removed %s rows", check.name)
		}
	}
	var remainingRates, presentGroups int64
	if err := dbA.Model(&RateSnapshot{}).Where("channel_id = ?", channel.ID).Count(&remainingRates).Error; err != nil {
		t.Fatalf("count rates after channel delete prepare: %v", err)
	}
	if err := dbA.Model(&GatewayUpstreamGroup{}).
		Where("channel_id = ? AND present = ?", channel.ID, true).
		Count(&presentGroups).Error; err != nil {
		t.Fatalf("count present groups after channel delete prepare: %v", err)
	}
	if remainingRates != 0 || presentGroups != 0 {
		t.Fatalf("delete prepare did not retire presence: rates=%d present_groups=%d", remainingRates, presentGroups)
	}
}

func TestDeleteSnapshotPreservesWhitespaceForExactRateRow(t *testing.T) {
	db := testdb.Open(t)
	channel := &Channel{
		Name: "whitespace-rate-delete", Type: ChannelTypeNewAPI,
		SiteURL: "https://whitespace-rate-delete.example", MonitorEnabled: true,
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	snapshot := &RateSnapshot{
		ChannelID: channel.ID, ModelName: " VIP ", Ratio: 1, LastSeenAt: time.Now().UTC(),
	}
	if _, err := NewRates(db).Upsert(snapshot); err != nil {
		t.Fatal(err)
	}
	if err := NewRates(db).DeleteSnapshotForChannelIdentity(channel.ID, snapshot.ModelName, channel.UpstreamAccountIdentityRevision); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&RateSnapshot{}).
		Where("channel_id = ? AND lower(btrim(model_name)) = ?", channel.ID, "vip").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("whitespace rate rows after exact delete = %d", count)
	}
	retired, err := NewGateway(db).RetireUpstreamGroupForKeyRemoval(
		channel.ID, channel.UpstreamAccountIdentityRevision, "vip", time.Now().UTC(),
	)
	if err != nil || retired.PresenceGeneration != 1 {
		t.Fatalf("retire whitespace rate generation = %+v err=%v", retired, err)
	}
}

func TestNormalizeCandidateGroupMembersUsesCanonicalLockOrder(t *testing.T) {
	db := testdb.Open(t)
	members, err := normalizeCandidateGroupMembers(db, []GatewayCandidateGroupMember{
		{ChannelID: 7, GroupName: "B"},
		{ChannelID: 7, GroupName: "a"},
		{ChannelID: 7, GroupName: " A "},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0].GroupName != "a" || members[1].GroupName != "B" {
		t.Fatalf("canonical member lock order/deduplication = %+v", members)
	}
}

func TestRecordUpstreamGroupChangeFencesFirstCanonicalIdentity(t *testing.T) {
	dbA := testdb.Open(t)
	dbB, err := testdb.ReopenWithError(t, dbA)
	if err != nil {
		t.Fatal(err)
	}
	dbC, err := testdb.ReopenWithError(t, dbA)
	if err != nil {
		t.Fatal(err)
	}
	channel := &Channel{
		Name: "first-group-identity-fence", Type: ChannelTypeNewAPI,
		SiteURL: "https://first-group-identity.example", MonitorEnabled: true,
	}
	if err := NewChannels(dbA).Create(channel); err != nil {
		t.Fatal(err)
	}

	holderPID, releaseFence, fenceDone := holdRateSnapshotPresenceFenceForTest(t, dbA, channel.ID, "vip")
	recorded := make(chan error, 1)
	go func() {
		recorded <- NewGateway(dbB).RecordUpstreamGroupChange(channel.ID, " VIP ", "added", 7, "managed", time.Now().UTC())
	}()
	if err := waitForAutomaticAssignmentFenceWaiter(dbC, holderPID, 10*time.Second); err != nil {
		close(releaseFence)
		_ = waitForPresenceTestError(fenceDone, 10*time.Second)
		t.Fatal(err)
	}
	var count int64
	countErr := dbC.Model(&GatewayUpstreamGroup{}).Where("channel_id = ?", channel.ID).Count(&count).Error
	close(releaseFence)
	if err := waitForPresenceTestError(fenceDone, 10*time.Second); err != nil {
		t.Fatalf("release first-identity presence fence: %v", err)
	}
	if countErr != nil {
		t.Fatal(countErr)
	}
	if count != 0 {
		t.Fatalf("first identity was created outside its canonical fence: %d", count)
	}
	if err := waitForPresenceTestError(recorded, 10*time.Second); err != nil {
		t.Fatalf("record first canonical identity: %v", err)
	}
	var group GatewayUpstreamGroup
	if err := dbA.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "vip").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if !group.Present || group.PresenceGeneration != 1 {
		t.Fatalf("first canonical identity = %+v", group)
	}
}
