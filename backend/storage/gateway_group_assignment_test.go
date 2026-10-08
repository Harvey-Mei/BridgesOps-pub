package storage

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func createUnsyncedTrustedAssignmentKeyFixture(t *testing.T, db *gorm.DB, channelID uint, groupName string) {
	t.Helper()
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channelID, GroupName: groupName, OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "fixture-cipher", Enabled: true, LatencyProbeEnabled: false,
	}).Error; err != nil {
		t.Fatal(err)
	}
}

type assignmentChannelRowHoldReady struct {
	pid int
	err error
}

func holdAssignmentChannelRowForTest(t *testing.T, db *gorm.DB, channelID uint) (int, chan struct{}, <-chan error) {
	t.Helper()
	ready := make(chan assignmentChannelRowHoldReady, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.Transaction(func(tx *gorm.DB) error {
			if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
				ready <- assignmentChannelRowHoldReady{err: err}
				return err
			}
			var pid int
			if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				ready <- assignmentChannelRowHoldReady{err: err}
				return err
			}
			ready <- assignmentChannelRowHoldReady{pid: pid}
			<-release
			return nil
		})
	}()
	select {
	case result := <-ready:
		if result.err != nil {
			close(release)
			t.Fatalf("hold assignment channel row: %v", result.err)
		}
		return result.pid, release, done
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("timed out holding assignment channel row")
		return 0, release, done
	}
}

func reopenAssignmentWorkerForTest(t *testing.T, db *gorm.DB) (*gorm.DB, int) {
	t.Helper()
	workerDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatalf("reopen assignment worker database: %v", err)
	}
	sqlDB, err := workerDB.DB()
	if err != nil {
		t.Fatalf("open assignment worker connection pool: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	var pid int
	if err := workerDB.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
		t.Fatalf("read assignment worker backend pid: %v", err)
	}
	if pid == 0 {
		t.Fatal("assignment worker backend pid is zero")
	}
	return workerDB, pid
}

func waitForAssignmentChannelRowWaiters(db *gorm.DB, blockerPID int, waiterPIDs [2]int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var waiters int
		if err := db.Raw(`
			WITH RECURSIVE blocker_chain(waiter_pid, blocker_pid) AS (
				SELECT waiting.pid, blocker.pid
				FROM pg_stat_activity waiting
				CROSS JOIN LATERAL unnest(pg_blocking_pids(waiting.pid)) AS blocker(pid)
				WHERE waiting.datname = current_database()
				  AND waiting.pid IN (?, ?)
				  AND waiting.wait_event_type = 'Lock'
				UNION
				SELECT chain.waiter_pid, blocker.pid
				FROM blocker_chain chain
				CROSS JOIN LATERAL unnest(pg_blocking_pids(chain.blocker_pid)) AS blocker(pid)
				WHERE chain.blocker_pid <> ?
			)
			SELECT COUNT(DISTINCT waiter_pid)
			FROM blocker_chain
			WHERE blocker_pid = ?`, waiterPIDs[0], waiterPIDs[1], blockerPID, blockerPID).Scan(&waiters).Error; err != nil {
			return err
		}
		if waiters == len(waiterPIDs) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for assignment workers %v behind channel blocker %d (got %d)", waiterPIDs, blockerPID, waiters)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func tryLockAutomaticAssignmentForTest(db *gorm.DB) (bool, error) {
	var acquired bool
	err := db.Transaction(func(tx *gorm.DB) error {
		return tx.Raw("SELECT pg_try_advisory_xact_lock(hashtextextended(?, 0))", "bridgeops:automatic-group-assignment").Scan(&acquired).Error
	})
	return acquired, err
}

func TestRoutingAndAutomaticAssignmentMutationsSerializeWithoutDeadlock(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "assignment locking", Type: ChannelTypeNewAPI, SiteURL: "https://assignment-locking.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "pro", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "pro", UpstreamKeyID: 7, UpstreamKeyName: "managed-pro",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "cipher", Enabled: true, LatencyProbeEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "pro", "added", 7, "managed-pro", base); err != nil {
		t.Fatal(err)
	}
	candidate := &GatewayCandidateGroup{Name: "locking target"}
	member := []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "pro"}}
	if err := store.SaveCandidateGroup(candidate, member); err != nil {
		t.Fatal(err)
	}
	var change GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ? AND kind = ?", channel.ID, "pro", "added").First(&change).Error; err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 3)
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		<-start
		if err := store.AssignUpstreamGroupChange(change.ID, []uint{candidate.ID}, base.Add(time.Second)); err != nil {
			errs <- err
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for index := 0; index < 8; index++ {
			if err := store.UpdateLatencyProbeEnabled(channel.ID, "pro", index%2 == 0); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for index := 0; index < 8; index++ {
			if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "pro", "codex", "gpt-5.4", time.Now().Add(time.Duration(index)*time.Second), ""); err != nil {
				errs <- err
				return
			}
		}
	}()
	close(start)
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("routing and automatic assignment mutations timed out; possible lock inversion")
	}
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent automatic assignment mutation: %v", err)
	}
	if err := db.First(&change, change.ID).Error; err != nil {
		t.Fatal(err)
	}
	if change.Status != "assigned" {
		t.Fatalf("concurrent manual assignment status = %+v", change)
	}
}

func TestAutomaticAndManualAssignmentUseOneChannelFirstLockOrder(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "assignment entrypoint lock order", Type: ChannelTypeNewAPI, SiteURL: "https://assignment-entrypoint-lock-order.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "pro", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "pro", UpstreamKeyID: 7, UpstreamKeyName: "managed-pro",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "pro", "added", 7, "managed-pro", base); err != nil {
		t.Fatal(err)
	}
	candidate := &GatewayCandidateGroup{Name: "entrypoint lock target"}
	if err := store.SaveCandidateGroup(candidate, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "pro"}}); err != nil {
		t.Fatal(err)
	}
	var change GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ? AND kind = ?", channel.ID, "pro", "added").First(&change).Error; err != nil {
		t.Fatal(err)
	}
	previews, err := store.PreviewAutomaticAssignments([]uint{change.ID}, true)
	if err != nil || len(previews) != 1 || previews[0].State != "ready" || previews[0].EvaluationToken == "" {
		t.Fatalf("entrypoint lock preview = %+v err=%v", previews, err)
	}
	token := previews[0].EvaluationToken
	automaticDB, automaticPID := reopenAssignmentWorkerForTest(t, db)
	manualDB, manualPID := reopenAssignmentWorkerForTest(t, db)
	if automaticPID == manualPID {
		t.Fatalf("assignment workers unexpectedly share backend pid %d", automaticPID)
	}
	automaticStore := NewGateway(automaticDB)
	manualStore := NewGateway(manualDB)

	blockerPID, releaseChannel, blockerDone := holdAssignmentChannelRowForTest(t, db, channel.ID)
	released := false
	workersDone := make(chan struct{})
	defer func() {
		if !released {
			close(releaseChannel)
			if err := <-blockerDone; err != nil {
				t.Errorf("release assignment channel row: %v", err)
			}
		}
		select {
		case <-workersDone:
		case <-time.After(10 * time.Second):
			t.Errorf("assignment entrypoint workers did not stop during cleanup")
		}
	}()

	results := make(chan error, 2)
	var entrypointWorkers sync.WaitGroup
	entrypointWorkers.Add(2)
	go func() {
		defer entrypointWorkers.Done()
		_, err := automaticStore.ExecuteAutomaticAssignment(change.ID, token, "test", GatewayAssignmentTriggerManual)
		results <- err
	}()
	go func() {
		defer entrypointWorkers.Done()
		results <- manualStore.AssignUpstreamGroupChangeAs(change.ID, []uint{candidate.ID}, "test", base.Add(time.Second))
	}()
	go func() {
		entrypointWorkers.Wait()
		close(workersDone)
	}()
	if err := waitForAssignmentChannelRowWaiters(db, blockerPID, [2]int{automaticPID, manualPID}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if acquired, err := tryLockAutomaticAssignmentForTest(db); err != nil {
		t.Fatalf("probe automatic-assignment lock: %v", err)
	} else if !acquired {
		t.Fatal("an assignment entrypoint acquired automatic-assignment lock before its channel row")
	}
	close(releaseChannel)
	released = true
	if err := <-blockerDone; err != nil {
		t.Fatalf("release assignment channel row: %v", err)
	}

	successes := 0
	for index := 0; index < 2; index++ {
		select {
		case err := <-results:
			if err == nil {
				successes++
				continue
			}
			if !errors.Is(err, ErrUpstreamGroupChangeSuperseded) && !errors.Is(err, gorm.ErrRecordNotFound) && !errors.Is(err, ErrGatewayEvaluationStale) {
				t.Fatalf("assignment entrypoint %d returned unexpected error: %v", index+1, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("assignment entrypoints did not finish after releasing channel row")
		}
	}
	if successes != 1 {
		t.Fatalf("assignment entrypoint successes = %d, want exactly one serialized winner", successes)
	}
	if err := db.First(&change, change.ID).Error; err != nil {
		t.Fatal(err)
	}
	if change.Status != "assigned" {
		t.Fatalf("serialized assignment change = %+v", change)
	}
}

func TestLatencyProbePlaceholderAndModelSyncSerializeWithoutDeadlock(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	rates := NewRates(db)
	store := NewGateway(db)
	channel := &Channel{Name: "placeholder locking", Type: ChannelTypeNewAPI, SiteURL: "https://placeholder-locking.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "placeholder", Ratio: 1, LastSeenAt: base}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "placeholder", "added", 0, "", base); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		errs <- store.UpdateLatencyProbeEnabled(channel.ID, " PLACEHOLDER ", false)
	}()
	go func() {
		defer workers.Done()
		<-start
		errs <- store.RecordUpstreamKeyModelsError(channel.ID, "placeholder", base.Add(time.Second), "model sync unavailable")
	}()
	close(start)
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("placeholder probe toggle and model sync timed out; possible lock inversion")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent placeholder mutation: %v", err)
		}
	}
	key, err := store.FindUpstreamKeyAny(channel.ID, "placeholder")
	if err != nil {
		t.Fatal(err)
	}
	if key.LatencyProbeEnabled || key.ModelsError != "model sync unavailable" {
		t.Fatalf("placeholder state after concurrent mutations = %+v", key)
	}
}

func TestReconcileAutomaticAssignmentPresenceUsesCurrentRates(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "presence reconcile", Type: ChannelTypeNewAPI, SiteURL: "https://presence-reconcile.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "live", Ratio: 1, FirstSeenAt: now, LastSeenAt: now}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "live", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "live", "removed", 1, "managed", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "stale", "added", 2, "managed", now); err != nil {
		t.Fatal(err)
	}
	oldSync := now.Add(-time.Hour)
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "stale", ModelIDs: "gpt-5.4", ModelFamilies: "codex",
		ModelsSyncedAt: &oldSync, Enabled: true, LatencyProbeEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	processed, err := store.ReconcileAutomaticAssignmentPresence(1)
	if err != nil || processed != 1 {
		t.Fatalf("presence reconcile processed=%d err=%v", processed, err)
	}
	var live GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "live").First(&live).Error; err != nil {
		t.Fatal(err)
	}
	if !live.Present || live.PresenceGeneration != 2 {
		t.Fatalf("live identity was not restored from current rates: %+v", live)
	}
	var stale GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "stale").First(&stale).Error; err != nil {
		t.Fatal(err)
	}
	if stale.Present {
		t.Fatalf("stale identity remained present without a current rate: %+v", stale)
	}
	if _, err := store.BackfillAutomaticAssignmentFacts(1); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&stale, stale.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stale.Present || stale.FactsComplete || !containsString(stale.IncompleteReasons, "group_not_present") {
		t.Fatalf("stale key revived an absent upstream group during facts backfill: %+v", stale)
	}
}

func TestReconcileAutomaticAssignmentPresenceDoesNotRepeatAddedAfterRemovalAudit(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "presence audit dedupe", Type: ChannelTypeNewAPI, SiteURL: "https://presence-audit-dedupe.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now().UTC()
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "promo", Ratio: 1, FirstSeenAt: observedAt, LastSeenAt: observedAt}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.reconcileAutomaticAssignmentPresence(channel.ID, "promo", observedAt); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "promo")
	if err != nil {
		t.Fatal(err)
	}
	removedAudit := newUpstreamGroupChange(channel.ID, "promo", "removed", 0, "", observedAt.Add(time.Second))
	removedAudit.UpstreamGroupID = &generation.ID
	removedAudit.PresenceGeneration = &generation.PresenceGeneration
	if err := db.Create(removedAudit).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.reconcileAutomaticAssignmentPresence(channel.ID, "promo", observedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}

	var addedCount int64
	if err := db.Model(&GatewayUpstreamGroupChange{}).
		Where("upstream_group_id = ? AND presence_generation = ? AND kind = ?", generation.ID, generation.PresenceGeneration, "added").
		Count(&addedCount).Error; err != nil {
		t.Fatal(err)
	}
	if addedCount != 1 {
		t.Fatalf("same-generation reconciled added change count = %d, want 1", addedCount)
	}
}

func TestAutomaticAssignmentBackfillRejectsFactsOlderThanPresenceGeneration(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "old facts backfill", Type: ChannelTypeNewAPI, SiteURL: "https://old-facts.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	discoveredAt := time.Now().UTC()
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "live", Ratio: 1, FirstSeenAt: discoveredAt, LastSeenAt: discoveredAt}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "live", "added", 1, "managed", discoveredAt); err != nil {
		t.Fatal(err)
	}
	oldSync := discoveredAt.Add(-time.Hour)
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "live", ModelIDs: "gpt-5.4", ModelFamilies: "codex",
		ModelsSyncedAt: &oldSync, Enabled: true, LatencyProbeEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.BackfillAutomaticAssignmentFacts(1); err != nil {
		t.Fatal(err)
	}
	var group GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "live").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if group.FactsComplete || len(group.ModelIDs) != 0 || group.ModelsSyncedAt != nil {
		t.Fatalf("pre-generation model facts were accepted by backfill: %+v", group)
	}
}

func TestUpstreamGroupFactsAreGenerationScoped(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "facts", Type: ChannelTypeNewAPI, SiteURL: "https://facts.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "VIP", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	key := &GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "VIP", Enabled: true, LatencyProbeEnabled: true}
	if err := db.Create(key).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Round(time.Second)
	if err := store.RecordUpstreamGroupChange(channel.ID, "VIP", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "VIP", "codex", "gpt-5,unknown,claude-gpt-bridge", now, ""); err != nil {
		t.Fatal(err)
	}
	var group GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "vip").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if group.FactsComplete || !reflect.DeepEqual(group.UnclassifiedModelIDs, []string{"unknown"}) || !reflect.DeepEqual(group.AmbiguousModelIDs, []string{"claude-gpt-bridge"}) {
		t.Fatalf("incomplete facts = %+v", group)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "VIP", "codex", "gpt-5,o3", now.Add(time.Minute), ""); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if !group.FactsComplete || !reflect.DeepEqual(group.ModelFamilyCodes, []string{"codex"}) || !reflect.DeepEqual(group.GatewayApps, []string{"codex"}) {
		t.Fatalf("complete facts = %+v", group)
	}
	if err := store.ObserveModelContextLimit(channel.ID, "VIP", "gpt-5", 1_000_000, 877_808, 58_192, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&group).Updates(map[string]any{"service_tier": "max", "service_tier_source": "manual"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "vip", "removed", 1, "managed", now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "VIP", "added", 2, "managed-2", now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if group.PresenceGeneration != 2 || group.FactsComplete || group.ServiceTier != "max" || !reflect.DeepEqual(group.IncompleteReasons, []string{"models_not_synced_for_generation"}) {
		t.Fatalf("reappeared group = %+v", group)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "VIP", "codex", "gpt-5", now.Add(4*time.Minute), ""); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(group.CapabilityProfile) != 0 {
		t.Fatalf("old generation capability leaked into reappeared group: %+v", group.CapabilityProfile)
	}
	var added []GatewayUpstreamGroupChange
	if err := db.Where("upstream_group_id = ? AND kind = ?", group.ID, "added").Order("id ASC").Find(&added).Error; err != nil {
		t.Fatal(err)
	}
	if len(added) != 2 || added[0].Status != "superseded" || added[1].Status != "pending" || added[1].PresenceGeneration == nil || *added[1].PresenceGeneration != 2 {
		t.Fatalf("generation changes = %+v", added)
	}
}

func TestCaseVariantFactsConflictStopsAutomaticAssignment(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "case facts", Type: ChannelTypeNewAPI, SiteURL: "https://case-facts.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	rates := NewRates(db)
	facts := map[string]struct {
		families string
		models   string
	}{
		"VIP": {families: "codex", models: "gpt-5.4"},
		"vip": {families: "claude", models: "claude-sonnet-4-5"},
	}
	for _, name := range []string{"VIP", "vip"} {
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: name, Ratio: 1, LastSeenAt: now}); err != nil {
			t.Fatal(err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, name)
		if err != nil {
			t.Fatal(err)
		}
		fact := facts[name]
		if err := db.Create(&GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: name, OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
			APIKeyCipher: "case-variant-" + name, Enabled: true, LatencyProbeEnabled: true,
			ModelFamilies: fact.families, ModelIDs: fact.models, ModelsSyncedAt: &now,
			ModelFactsUpstreamGroupID: generation.ID, ModelFactsPresenceGeneration: generation.PresenceGeneration,
			ModelFactsChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "vip", "claude", "claude-sonnet-4-5", now, ""); err != nil {
		t.Fatal(err)
	}
	var identity GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "vip").First(&identity).Error; err != nil {
		t.Fatal(err)
	}
	if !identity.IdentityConflict || identity.FactsComplete || !containsString(identity.IncompleteReasons, "identity_conflict") {
		t.Fatalf("variant conflict facts = %+v", identity)
	}
	if err := db.Where("channel_id = ? AND group_name = ?", channel.ID, "vip").Delete(&GatewayUpstreamKey{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "VIP", "codex", "gpt-5.4", now.Add(time.Minute), ""); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&identity, identity.ID).Error; err != nil {
		t.Fatal(err)
	}
	if identity.IdentityConflict || !identity.FactsComplete {
		t.Fatalf("variant conflict did not recover = %+v", identity)
	}
}

func TestNonASCIIUpstreamGroupUsesDatabaseCanonicalNameEverywhere(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "unicode canonical", Type: ChannelTypeNewAPI, SiteURL: "https://unicode-canonical.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	rawName := "ÄÖÜ-模型"
	var databaseCanonical string
	if err := db.Raw("SELECT lower(btrim(?))", rawName).Scan(&databaseCanonical).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: rawName, Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: rawName, Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Round(time.Second)
	if err := store.RecordUpstreamGroupChange(channel.ID, rawName, "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, rawName, "codex", "gpt-5.4", now.Add(time.Second), ""); err != nil {
		t.Fatal(err)
	}
	var identities []GatewayUpstreamGroup
	if err := db.Where("channel_id = ?", channel.ID).Find(&identities).Error; err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 || identities[0].CanonicalGroupName != databaseCanonical || !identities[0].FactsComplete {
		t.Fatalf("non-ASCII identity = %+v, database canonical %q", identities, databaseCanonical)
	}
	if generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, rawName); err != nil || generation.ID != identities[0].ID || generation.CanonicalGroupName != databaseCanonical {
		t.Fatalf("captured generation = %+v err=%v", generation, err)
	}
	if key, err := store.FindUpstreamKey(channel.ID, rawName); err != nil || key.GroupName != rawName {
		t.Fatalf("canonical key lookup = %+v err=%v", key, err)
	}
}

func TestCaseVariantHealthWritesUseCanonicalSurvivor(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "case health", Type: ChannelTypeNewAPI, SiteURL: "https://case-health.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	olderSync := base.Add(-time.Minute)
	newerSync := base
	upper := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "VIP", Enabled: true, LatencyProbeEnabled: true,
		APIKeyCipher: "upper-cipher", ModelsSyncedAt: &olderSync,
	}
	lower := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "vip", Enabled: true, LatencyProbeEnabled: true,
		APIKeyCipher: "lower-cipher", ModelsSyncedAt: &newerSync,
	}
	for _, key := range []*GatewayUpstreamKey{&upper, &lower} {
		if err := db.Create(key).Error; err != nil {
			t.Fatal(err)
		}
	}
	staleFailureAt := base.Add(-time.Second)
	staleCooldownUntil := base.Add(10 * time.Minute)
	if err := db.Model(&GatewayUpstreamKey{}).
		Where("id = ?", upper.ID).
		Updates(map[string]any{
			"routing_failure_count":      7,
			"last_routing_failure_error": "stale upper variant failure",
			"last_routing_failure_kind":  "transport",
			"last_routing_failure_at":    staleFailureAt,
			"routing_cooldown_until":     staleCooldownUntil,
		}).Error; err != nil {
		t.Fatal(err)
	}
	failureAt := base.Add(time.Second)
	if err := store.RecordUpstreamKeyRoutingFailure(channel.ID, "VIP", "transport", 502, "canonical failure", failureAt); err != nil {
		t.Fatal(err)
	}
	successAt := base.Add(2 * time.Second)
	if err := store.TouchUpstreamKey(channel.ID, "vip", successAt); err != nil {
		t.Fatal(err)
	}

	if err := db.First(&upper, upper.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&lower, lower.ID).Error; err != nil {
		t.Fatal(err)
	}
	if upper.LastUsedAt != nil || upper.LastRoutingFailureAt != nil || upper.RoutingFailureCount != 0 || upper.RoutingCooldownUntil != nil {
		t.Fatalf("non-survivor retained stale health: %+v", upper)
	}
	if lower.LastUsedAt == nil || !lower.LastUsedAt.Equal(successAt) || lower.LastRoutingFailureAt != nil || lower.RoutingFailureCount != 0 || lower.RoutingCooldownUntil != nil {
		t.Fatalf("canonical survivor health = %+v", lower)
	}
	var failures int64
	if err := db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND lower(btrim(group_name)) = ?", channel.ID, "vip").Count(&failures).Error; err != nil {
		t.Fatal(err)
	}
	if failures != 0 {
		t.Fatalf("canonical routing failure facts = %d", failures)
	}
}

func TestAutomaticAssignmentSeparatesSameModelsByServiceTier(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "unrelated display name", Type: ChannelTypeNewAPI, SiteURL: "https://tiers.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	proPool := &GatewayCandidateGroup{Name: "premium one"}
	maxPool := &GatewayCandidateGroup{Name: "premium two"}
	seed := []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}
	if err := store.SaveCandidateGroup(proPool, seed); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCandidateGroup(maxPool, seed); err != nil {
		t.Fatal(err)
	}
	state, err := store.automaticAssignmentState(db)
	if err != nil {
		t.Fatal(err)
	}
	proRule := &GatewayCandidateGroupAutoRule{
		CandidateGroupID: proPool.ID, Enabled: true, Priority: 100,
		ModelFamilies: []string{"codex"}, FamilyMatchMode: "single_any", ServiceTiers: []string{"pro"},
	}
	state, err = store.SaveAutomaticAssignmentRule(proRule, state.RulesetVersion)
	if err != nil {
		t.Fatal(err)
	}
	maxRule := &GatewayCandidateGroupAutoRule{
		CandidateGroupID: maxPool.ID, Enabled: true, Priority: 100,
		ModelFamilies: []string{"codex"}, FamilyMatchMode: "single_any", ServiceTiers: []string{"max"},
	}
	if _, err := store.SaveAutomaticAssignmentRule(maxRule, state.RulesetVersion); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Round(time.Second)
	changes := make(map[string]GatewayUpstreamGroupChange)
	for index, item := range []struct {
		name string
		tier string
	}{{"not-pro-in-the-name", "pro"}, {"ordinary-looking", "max"}, {"max-looking-but-unset", ""}} {
		if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: item.name, Ratio: 1}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: item.name, Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, item.name, "added", int64(index+1), "managed", now.Add(time.Duration(index)*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, item.name, "codex", "gpt-5.4,o3", now.Add(time.Duration(index)*time.Minute), ""); err != nil {
			t.Fatal(err)
		}
		var identity GatewayUpstreamGroup
		if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, item.name).First(&identity).Error; err != nil {
			t.Fatal(err)
		}
		if item.tier != "" {
			if _, err := store.SetUpstreamGroupServiceTier(identity.ID, item.tier, "manual"); err != nil {
				t.Fatal(err)
			}
		}
		var change GatewayUpstreamGroupChange
		if err := db.Where("upstream_group_id = ? AND kind = ?", identity.ID, "added").First(&change).Error; err != nil {
			t.Fatal(err)
		}
		changes[item.tier] = change
	}

	previews, err := store.PreviewAutomaticAssignments(nil, true)
	if err != nil {
		t.Fatal(err)
	}
	byChange := make(map[uint]GatewayAutomaticAssignmentPreview)
	for _, preview := range previews {
		byChange[preview.ChangeID] = preview
	}
	if preview := byChange[changes["pro"].ID]; preview.State != "ready" || !reflect.DeepEqual(preview.SuggestedCandidateGroupIDs, []uint{proPool.ID}) {
		t.Fatalf("pro preview = %+v", preview)
	}
	if preview := byChange[changes["max"].ID]; preview.State != "ready" || !reflect.DeepEqual(preview.SuggestedCandidateGroupIDs, []uint{maxPool.ID}) {
		t.Fatalf("max preview = %+v", preview)
	}
	if preview := byChange[changes[""].ID]; preview.State != "awaiting_facts" || !containsString(preview.MissingFacts, "service_tier_missing") {
		t.Fatalf("unset tier preview = %+v", preview)
	}

	proPreview := byChange[changes["pro"].ID]
	if _, err := store.SetUpstreamGroupServiceTier(proPreview.UpstreamGroupID, "max", "manual"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ExecuteAutomaticAssignment(proPreview.ChangeID, proPreview.EvaluationToken, "test", GatewayAssignmentTriggerManual); !errors.Is(err, ErrGatewayEvaluationStale) {
		t.Fatalf("stale token error = %v", err)
	}
	if _, err := store.SetUpstreamGroupServiceTier(proPreview.UpstreamGroupID, "pro", "manual"); err != nil {
		t.Fatal(err)
	}
	fresh, err := store.previewAutomaticAssignment(proPreview.ChangeID, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "not-pro-in-the-name", false); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ExecuteAutomaticAssignment(fresh.ChangeID, fresh.EvaluationToken, "system", GatewayAssignmentTriggerSync); !errors.Is(err, ErrGatewayEvaluationStale) {
		t.Fatalf("system execution ignored disabled automatic assignment: %v", err)
	}
	if _, err := store.ExecuteAutomaticAssignment(fresh.ChangeID, fresh.EvaluationToken, "test", GatewayAssignmentTriggerManual); err != nil {
		t.Fatal(err)
	}
	var manualAudit GatewayUpstreamGroupAssignmentAudit
	if err := db.Where("change_id = ?", fresh.ChangeID).Order("id DESC").First(&manualAudit).Error; err != nil {
		t.Fatal(err)
	}
	if manualAudit.TriggerSource != GatewayAssignmentTriggerManual {
		t.Fatalf("manual execution trigger = %q", manualAudit.TriggerSource)
	}
	var claim GatewayCandidateGroupMembershipClaim
	if err := db.Where("candidate_group_id = ? AND upstream_group_id = ? AND source_kind = ? AND active = ?", proPool.ID, fresh.UpstreamGroupID, "auto_rule", true).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.SourceID != uint64(proRule.ID) {
		t.Fatalf("auto claim source = %d, want rule %d", claim.SourceID, proRule.ID)
	}
}

func TestAutomaticAssignmentBatchRefreshesTokensForSharedTarget(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "shared target batch", Type: ChannelTypeNewAPI, SiteURL: "https://shared-target-batch.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	target := &GatewayCandidateGroup{Name: "shared automatic target"}
	if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	state, err := store.automaticAssignmentState(db)
	if err != nil {
		t.Fatal(err)
	}
	state, err = store.SaveAutomaticAssignmentRule(&GatewayCandidateGroupAutoRule{
		CandidateGroupID: target.ID,
		Enabled:          true,
		Priority:         100,
		ModelFamilies:    []string{"codex"},
		FamilyMatchMode:  "single_any",
	}, state.RulesetVersion)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Round(time.Second)
	for index, name := range []string{"batch-one", "batch-two"} {
		at := now.Add(time.Duration(index) * time.Second)
		if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: name, Ratio: 1, FirstSeenAt: at, LastSeenAt: at}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: name, Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
			t.Fatal(err)
		}
		if err := store.RecordUpstreamGroupChange(channel.ID, name, "added", int64(index+1), "managed", at); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, name, "codex", "gpt-5.4", at.Add(time.Millisecond), ""); err != nil {
			t.Fatal(err)
		}
	}
	previews, err := store.PreviewAutomaticAssignments(nil, true)
	if err != nil || len(previews) != 2 {
		t.Fatalf("shared-target previews = %+v err=%v", previews, err)
	}
	if _, err := store.UpdateAutomaticAssignmentState(state.RulesetVersion, true, true); err != nil {
		t.Fatal(err)
	}
	assigned, failures := store.ReevaluatePendingAutomaticAssignments(true)
	if assigned != 2 || len(failures) != 0 {
		t.Fatalf("shared-target batch assigned=%d failures=%v", assigned, failures)
	}
	changeIDs := make([]uint, 0, len(previews))
	for _, preview := range previews {
		changeIDs = append(changeIDs, preview.ChangeID)
	}
	var audits []GatewayUpstreamGroupAssignmentAudit
	if err := db.Where("change_id IN ?", changeIDs).Order("id ASC").Find(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if len(audits) != 2 {
		t.Fatalf("shared-target audits = %+v", audits)
	}
	for _, audit := range audits {
		if audit.TriggerSource != GatewayAssignmentTriggerRuleRecheck {
			t.Fatalf("batch audit trigger = %q", audit.TriggerSource)
		}
	}
}

func TestAutomaticAssignmentEqualPriorityTargetsConflict(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "conflict", Type: ChannelTypeSub2API, SiteURL: "https://conflict.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	groups := []*GatewayCandidateGroup{{Name: "one"}, {Name: "two"}}
	for _, group := range groups {
		if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
			t.Fatal(err)
		}
	}
	state, _ := store.automaticAssignmentState(db)
	for _, group := range groups {
		rule := &GatewayCandidateGroupAutoRule{CandidateGroupID: group.ID, Enabled: true, Priority: 7, ModelFamilies: []string{"claude"}, FamilyMatchMode: "single_any"}
		var err error
		state, err = store.SaveAutomaticAssignmentRule(rule, state.RulesetVersion)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "random", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "random", Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "random", "added", 1, "managed", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "random", "claude", "claude-sonnet-4-5", time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	previews, err := store.PreviewAutomaticAssignments(nil, true)
	if err != nil || len(previews) != 1 {
		t.Fatalf("previews=%+v err=%v", previews, err)
	}
	if previews[0].State != "conflict" || !reflect.DeepEqual(previews[0].SuggestedCandidateGroupIDs, []uint{groups[0].ID, groups[1].ID}) {
		t.Fatalf("conflict preview = %+v", previews[0])
	}
}

func TestMembershipClaimSourcesAreIndependent(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{Name: "claim ownership", Type: ChannelTypeNewAPI, SiteURL: "https://ownership.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "shared", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	group := &GatewayCandidateGroup{Name: "shared pool"}
	if err := db.Create(group).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		identity, _, err := ensureUpstreamGroupIdentityTx(tx, channel.ID, "shared", true, time.Now())
		if err != nil {
			return err
		}
		if err := ensureMembershipClaimTx(tx, group.ID, identity, "shared", "manual", 111, time.Now()); err != nil {
			return err
		}
		return ensureMembershipClaimTx(tx, group.ID, identity, "shared", "auto_rule", 999, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	var identity GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "shared").First(&identity).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error { return revokeMembershipClaimsTx(tx, "manual", 111, time.Now()) }); err != nil {
		t.Fatal(err)
	}
	var memberCount int64
	if err := db.Model(&GatewayCandidateGroupMember{}).Where("candidate_group_id = ? AND upstream_group_id = ?", group.ID, identity.ID).Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 1 {
		t.Fatalf("automatic claim did not retain member after manual revoke: %d", memberCount)
	}
	if err := db.Transaction(func(tx *gorm.DB) error { return revokeMembershipClaimsTx(tx, "auto_rule", 999, time.Now()) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayCandidateGroupMember{}).Where("candidate_group_id = ? AND upstream_group_id = ?", group.ID, identity.ID).Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 0 {
		t.Fatalf("last claim did not remove materialized member: %d", memberCount)
	}
}

func TestCandidateGroupCannotRemoveItsLastAutomaticMember(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "last automatic member", Type: ChannelTypeNewAPI, SiteURL: "https://last-automatic.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "automatic", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	group := &GatewayCandidateGroup{Name: "automatic only"}
	if err := db.Create(group).Error; err != nil {
		t.Fatal(err)
	}
	var identity *GatewayUpstreamGroup
	if err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		identity, _, err = ensureUpstreamGroupIdentityTx(tx, channel.ID, "automatic", true, time.Now())
		if err != nil {
			return err
		}
		return ensureMembershipClaimTx(tx, group.ID, identity, "automatic", "auto_rule", 1001, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCandidateGroup(group, nil); err == nil {
		t.Fatal("candidate group accepted removal of its last member")
	}
	var memberCount int64
	if err := db.Model(&GatewayCandidateGroupMember{}).
		Where("candidate_group_id = ? AND upstream_group_id = ?", group.ID, identity.ID).
		Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 1 {
		t.Fatalf("rejected last-member removal changed storage: %d", memberCount)
	}
}

func TestCandidateGroupRenameDoesNotPromoteAutomaticMembership(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "rename automatic member", Type: ChannelTypeNewAPI, SiteURL: "https://rename-automatic.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"seed", "automatic"} {
		if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: name, Ratio: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "automatic")
	group := &GatewayCandidateGroup{Name: "before rename"}
	if err := store.SaveCandidateGroup(group, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	var identity GatewayUpstreamGroup
	if err := db.Transaction(func(tx *gorm.DB) error {
		created, _, err := ensureUpstreamGroupIdentityTx(tx, channel.ID, "automatic", true, time.Now())
		if err != nil {
			return err
		}
		identity = *created
		return ensureMembershipClaimTx(tx, group.ID, created, "automatic", "auto_rule", 9001, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Name = "after rename"
	if err := store.SaveCandidateGroup(loaded, loaded.Members); err != nil {
		t.Fatal(err)
	}
	var automaticClaims, manualClaims, overrides int64
	if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("candidate_group_id = ? AND upstream_group_id = ? AND source_kind = ? AND active = ?", group.ID, identity.ID, "auto_rule", true).
		Count(&automaticClaims).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("candidate_group_id = ? AND upstream_group_id = ? AND source_kind = ? AND active = ?", group.ID, identity.ID, "manual", true).
		Count(&manualClaims).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayUpstreamGroupAssignmentOverride{}).Where("upstream_group_id = ?", identity.ID).Count(&overrides).Error; err != nil {
		t.Fatal(err)
	}
	if automaticClaims != 1 || manualClaims != 0 || overrides != 0 {
		t.Fatalf("rename changed automatic ownership: auto=%d manual=%d overrides=%d", automaticClaims, manualClaims, overrides)
	}
	if renamed, err := store.FindCandidateGroup(group.ID); err != nil || renamed.Name != "after rename" || len(renamed.Members) != 2 {
		t.Fatalf("renamed group = %+v err=%v", renamed, err)
	}
}

func TestKeyGroupRoutePolicyAppliesStructuredAppAndFamilyDefault(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "key policy", Type: ChannelTypeNewAPI, SiteURL: "https://key-policy.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	keyGroup := &GatewayKeyGroup{Name: "workstations"}
	if err := store.SaveKeyGroup(keyGroup); err != nil {
		t.Fatal(err)
	}
	candidate := &GatewayCandidateGroup{Name: "grok pro"}
	if err := store.SaveCandidateGroup(candidate, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	targetID := candidate.ID
	policy := &GatewayKeyGroupRoutePolicy{
		KeyGroupID: keyGroup.ID, GatewayApp: "codex", ModelFamily: "grok",
		DefaultCandidateGroupID: &targetID, TargetMode: "candidate_group", Enabled: true,
	}
	if err := store.SaveKeyGroupRoutePolicy(policy, 0); err != nil {
		t.Fatal(err)
	}
	key := &GatewayDownstreamKey{KeyGroupID: keyGroup.ID, App: GatewayAppCodex, ModelFamily: "grok"}
	applied, err := store.ApplyKeyGroupRoutePolicy(key)
	if err != nil || !applied {
		t.Fatalf("apply policy: applied=%t err=%v", applied, err)
	}
	if key.CandidateGroupID != candidate.ID || key.CandidateTargetMode != "candidate_group" || key.CandidateTargetSource != "key_group_policy" || key.RoutePolicyVersion == nil || *key.RoutePolicyVersion != 1 {
		t.Fatalf("policy target = %+v", key)
	}
	stale := *policy
	stale.TargetMode = "all_candidates"
	stale.DefaultCandidateGroupID = nil
	if err := store.SaveKeyGroupRoutePolicy(&stale, policy.PolicyVersion+1); !errors.Is(err, ErrGatewayRulesetChanged) {
		t.Fatalf("stale policy error = %v", err)
	}
}

func TestManualAssignmentOverrideDoesNotCrossPresenceGeneration(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "reappear", Type: ChannelTypeNewAPI, SiteURL: "https://reappear.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "pool", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "pool", UpstreamKeyID: 1, UpstreamKeyName: "managed", OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true, LatencyProbeEnabled: false}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "seed", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "seed-cipher", Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	target := &GatewayCandidateGroup{Name: "target"}
	if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Round(time.Second)
	if err := store.RecordUpstreamGroupChange(channel.ID, "pool", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	var first GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND kind = ? AND status = ?", channel.ID, "added", "pending").First(&first).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.AssignUpstreamGroupChange(first.ID, []uint{target.ID}, now.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&first, first.ID).Error; err != nil {
		t.Fatal(err)
	}
	if first.Status != "assigned" || first.AssignmentReasonCode != "manual_override_incomplete_facts" {
		t.Fatalf("manual assignment before current-generation model sync = %+v", first)
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "pool", false); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "pool", "removed", 1, "managed", now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "POOL", UpstreamKeyID: 2, UpstreamKeyName: "managed-2",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher-2", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "pool", "added", 2, "managed-2", now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var second GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND kind = ?", channel.ID, "added").Order("id DESC").First(&second).Error; err != nil {
		t.Fatal(err)
	}
	if second.Status != "pending" || second.AssignmentReasonCode == "manual_override_incomplete_facts" || second.AssignmentReasonCode == "manual_assign_override" {
		t.Fatalf("stale assign override crossed into the new generation: %+v", second)
	}
	var memberCount int64
	if err := db.Model(&GatewayCandidateGroupMember{}).Where("candidate_group_id = ? AND upstream_group_id = ?", target.ID, *second.UpstreamGroupID).Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 0 {
		t.Fatalf("stale assign override restored %d new-generation members", memberCount)
	}
	var identity GatewayUpstreamGroup
	if err := db.First(&identity, *second.UpstreamGroupID).Error; err != nil {
		t.Fatal(err)
	}
	if identity.PresenceGeneration != 2 {
		t.Fatalf("current generation = %d", identity.PresenceGeneration)
	}
	var override GatewayUpstreamGroupAssignmentOverride
	if err := db.Where("upstream_group_id = ?", identity.ID).First(&override).Error; err != nil {
		t.Fatal(err)
	}
	if override.PresenceGeneration != 1 {
		t.Fatalf("stale override generation = %d, want 1", override.PresenceGeneration)
	}
}

func TestManualIgnoreOverrideDoesNotCrossPresenceGeneration(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "ignore reappear", Type: ChannelTypeNewAPI, SiteURL: "https://ignore-reappear.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "ignored-pool", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "ignored-pool", Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Round(time.Second)
	if err := store.RecordUpstreamGroupChange(channel.ID, "ignored-pool", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	var first GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND kind = ?", channel.ID, "added").Order("id DESC").First(&first).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.IgnoreUpstreamGroupChange(first.ID, "persistent ignore", "test", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "ignored-pool", "removed", 1, "managed", now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "ignored-pool", "added", 2, "managed-2", now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var second GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND kind = ?", channel.ID, "added").Order("id DESC").First(&second).Error; err != nil {
		t.Fatal(err)
	}
	if second.Status != "pending" || second.AssignmentReasonCode == "manual_ignore_override" || second.PresenceGeneration == nil || *second.PresenceGeneration != 2 {
		t.Fatalf("stale ignore override crossed into the new generation: %+v", second)
	}
	var override GatewayUpstreamGroupAssignmentOverride
	if err := db.Where("upstream_group_id = ?", *second.UpstreamGroupID).First(&override).Error; err != nil {
		t.Fatal(err)
	}
	if override.PresenceGeneration != 1 {
		t.Fatalf("stale ignore override generation = %d, want 1", override.PresenceGeneration)
	}
}

func TestManualCandidateMembershipCreatesClaimAndRemovalRevokesIt(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "claims", Type: ChannelTypeSub2API, SiteURL: "https://claims.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "pro", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "pro", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "pro-cipher", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	candidate := &GatewayCandidateGroup{Name: "pro pool"}
	if err := store.SaveCandidateGroup(candidate, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "pro"}}); err != nil {
		t.Fatal(err)
	}
	var member GatewayCandidateGroupMember
	if err := db.Where("candidate_group_id = ?", candidate.ID).First(&member).Error; err != nil || member.UpstreamGroupID == 0 {
		t.Fatalf("member = %+v err=%v", member, err)
	}
	var claim GatewayCandidateGroupMembershipClaim
	if err := db.Where("candidate_group_id = ? AND upstream_group_id = ? AND active = ?", candidate.ID, member.UpstreamGroupID, true).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.SourceKind != "manual" || claim.SourceID == 0 {
		t.Fatalf("claim = %+v", claim)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "pro", "removed", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	var memberCount int64
	if err := db.Model(&GatewayCandidateGroupMember{}).Where("upstream_group_id = ?", member.UpstreamGroupID).Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 0 {
		t.Fatalf("live members after removal = %d", memberCount)
	}
	if err := db.First(&claim, claim.ID).Error; err != nil || claim.Active || claim.RevokedAt == nil {
		t.Fatalf("revoked claim = %+v err=%v", claim, err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "pro", UpstreamKeyID: 2, UpstreamKeyName: "managed-2",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "pro-cipher-2", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "pro", "added", 2, "managed-2", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "pro", "codex", "gpt-5.4", time.Now().Add(2*time.Minute), ""); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayCandidateGroupMember{}).Where("candidate_group_id = ? AND upstream_group_id = ?", candidate.ID, member.UpstreamGroupID).Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 0 {
		t.Fatalf("manual candidate override crossed presence generations: %d", memberCount)
	}
}

func TestManualCandidateRemovalRevokesAutomaticMembershipAndPersistsIgnoreOverride(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "automatic removal", Type: ChannelTypeNewAPI, SiteURL: "https://automatic-removal.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, groupName := range []string{"seed", "automatic"} {
		if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "seed", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "seed", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	candidate := &GatewayCandidateGroup{Name: "automatic removal target"}
	seedMember := GatewayCandidateGroupMember{ChannelID: channel.ID, GroupName: "seed"}
	if err := store.SaveCandidateGroup(candidate, []GatewayCandidateGroupMember{seedMember}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.RecordUpstreamGroupChange(channel.ID, "automatic", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	var identity GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "automatic").First(&identity).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return ensureMembershipClaimTx(tx, candidate.ID, &identity, "automatic", "auto_rule", 991, now)
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCandidateGroup(candidate, []GatewayCandidateGroupMember{seedMember}); err != nil {
		t.Fatal(err)
	}
	var memberCount int64
	if err := db.Model(&GatewayCandidateGroupMember{}).
		Where("candidate_group_id = ? AND upstream_group_id = ?", candidate.ID, identity.ID).
		Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 0 {
		t.Fatalf("automatic member remained after successful save: %d", memberCount)
	}
	var activeClaims int64
	if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("candidate_group_id = ? AND upstream_group_id = ? AND presence_generation = ? AND active = ?", candidate.ID, identity.ID, identity.PresenceGeneration, true).
		Count(&activeClaims).Error; err != nil {
		t.Fatal(err)
	}
	if activeClaims != 0 {
		t.Fatalf("automatic claims remained active after removal: %d", activeClaims)
	}
	var override GatewayUpstreamGroupAssignmentOverride
	if err := db.Where("upstream_group_id = ?", identity.ID).First(&override).Error; err != nil {
		t.Fatal(err)
	}
	if override.Action != "ignore" || override.PresenceGeneration != identity.PresenceGeneration {
		t.Fatalf("automatic removal override = %+v", override)
	}
	previews, err := store.PreviewAutomaticAssignments(nil, true)
	if err != nil || len(previews) != 1 {
		t.Fatalf("negative override preview = %+v err=%v", previews, err)
	}
	if previews[0].ReasonCode != "manual_ignore_override" || previews[0].State != "ready" || len(previews[0].SuggestedCandidateGroupIDs) != 0 {
		t.Fatalf("negative override was not preserved during rule recheck: %+v", previews[0])
	}
	if _, err := store.ExecuteAutomaticAssignment(previews[0].ChangeID, previews[0].EvaluationToken, "system", GatewayAssignmentTriggerRuleRecheck); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayCandidateGroupMember{}).
		Where("candidate_group_id = ? AND upstream_group_id = ?", candidate.ID, identity.ID).
		Count(&memberCount).Error; err != nil {
		t.Fatal(err)
	}
	if memberCount != 0 {
		t.Fatalf("rule recheck restored explicitly removed member: %d", memberCount)
	}
}

func TestProviderServiceTierDoesNotCrossPresenceGeneration(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "provider tier generation", Type: ChannelTypeNewAPI, SiteURL: "https://provider-tier.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.RecordUpstreamGroupChange(channel.ID, "max", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	var group GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "max").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetUpstreamGroupServiceTier(group.ID, "max", "provider"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "max", "removed", 1, "managed", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "max", "added", 2, "managed-2", now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if group.ServiceTier != "" || group.ServiceTierSource != "manual" {
		t.Fatalf("provider tier crossed generation: %+v", group)
	}
}

func TestManualCandidateEditPreservesDeletedOverrideTargetTombstone(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "override tombstone", Type: ChannelTypeNewAPI, SiteURL: "https://override-tombstone.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "shared", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "shared", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "shared-cipher", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	member := []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "shared"}}
	kept := &GatewayCandidateGroup{Name: "kept target"}
	deleted := &GatewayCandidateGroup{Name: "deleted target"}
	if err := store.SaveCandidateGroup(kept, member); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCandidateGroup(deleted, member); err != nil {
		t.Fatal(err)
	}
	var identity GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "shared").First(&identity).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteCandidateGroup(deleted.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveCandidateGroup(&GatewayCandidateGroup{ID: kept.ID, Name: kept.Name}, member); err != nil {
		t.Fatal(err)
	}
	var override GatewayUpstreamGroupAssignmentOverride
	if err := db.Where("upstream_group_id = ?", identity.ID).First(&override).Error; err != nil {
		t.Fatal(err)
	}
	var tombstone GatewayUpstreamGroupAssignmentOverrideTarget
	if err := db.Where("override_id = ? AND candidate_group_id IS NULL AND candidate_group_id_snapshot = ?", override.ID, deleted.ID).First(&tombstone).Error; err != nil {
		t.Fatalf("deleted override target tombstone was lost: %v", err)
	}
}
