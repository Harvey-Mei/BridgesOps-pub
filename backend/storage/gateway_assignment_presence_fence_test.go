package storage

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

type automaticAssignmentFenceReady struct {
	pid int
	err error
}

type automaticAssignmentPresenceResult struct {
	processed int
	err       error
}

func TestReconcileAutomaticAssignmentPresenceRejectsDetachedPresentAfterRateDelete(t *testing.T) {
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
		Name: "assignment-presence-delete-fence", Type: ChannelTypeNewAPI,
		SiteURL: "https://assignment-presence-delete.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := NewRates(dbA).Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "Promo", Ratio: 1, LastSeenAt: base,
	}); err != nil {
		t.Fatal(err)
	}

	holderPID, releaseAssignment, assignmentDone := holdAutomaticAssignmentFenceForPresenceTest(t, dbA)
	reconciled := make(chan automaticAssignmentPresenceResult, 1)
	go func() {
		processed, err := NewGateway(dbB).ReconcileAutomaticAssignmentPresence(1)
		reconciled <- automaticAssignmentPresenceResult{processed: processed, err: err}
	}()

	reconcilePID, waitErr := waitForAutomaticAssignmentFenceWaiterPID(dbC, holderPID, 10*time.Second)
	deleted := make(chan error, 1)
	deleteBlockedErr := error(nil)
	if waitErr == nil {
		go func() { deleted <- NewRates(dbC).DeleteSnapshot(channel.ID, "Promo") }()
		deleteBlockedErr = waitForPresenceTestBlocker(dbA, reconcilePID, 10*time.Second)
	}
	close(releaseAssignment)
	if err := waitForPresenceTestError(assignmentDone, 10*time.Second); err != nil {
		t.Fatalf("release automatic-assignment fence: %v", err)
	}
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	if deleteBlockedErr != nil {
		t.Fatal(deleteBlockedErr)
	}
	if err := waitForPresenceTestError(deleted, 10*time.Second); err != nil {
		t.Fatalf("delete rate while reconcile waits: %v", err)
	}
	result, err := waitForAutomaticAssignmentPresenceResult(reconciled, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.err != nil || result.processed != 1 {
		t.Fatalf("presence reconcile processed=%d err=%v", result.processed, result.err)
	}

	var group GatewayUpstreamGroup
	if err := dbA.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "promo").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if group.Present {
		t.Fatalf("deleted rate was restored from detached scan: %+v", group)
	}
}

func TestReconcileAutomaticAssignmentPresenceSerializesQueuedRateRestore(t *testing.T) {
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
		Name: "assignment-presence-restore-fence", Type: ChannelTypeNewAPI,
		SiteURL: "https://assignment-presence-restore.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	store := NewGateway(dbA)
	if err := store.RecordUpstreamGroupChange(channel.ID, "Promo", "added", 7, "managed", base); err != nil {
		t.Fatal(err)
	}

	holderPID, releaseAssignment, assignmentDone := holdAutomaticAssignmentFenceForPresenceTest(t, dbA)
	reconciled := make(chan automaticAssignmentPresenceResult, 1)
	go func() {
		processed, err := NewGateway(dbB).ReconcileAutomaticAssignmentPresence(1)
		reconciled <- automaticAssignmentPresenceResult{processed: processed, err: err}
	}()

	waitErr := waitForAutomaticAssignmentFenceWaiter(dbC, holderPID, 10*time.Second)
	restored := make(chan error, 1)
	if waitErr == nil {
		go func() {
			_, err := NewRates(dbC).Upsert(&RateSnapshot{
				ChannelID: channel.ID, ModelName: "PROMO", Ratio: 1, LastSeenAt: base.Add(time.Second),
			})
			restored <- err
		}()
	}
	close(releaseAssignment)
	if err := waitForPresenceTestError(assignmentDone, 10*time.Second); err != nil {
		t.Fatalf("release automatic-assignment fence: %v", err)
	}
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	if err := waitForPresenceTestError(restored, 10*time.Second); err != nil {
		t.Fatalf("restore rate while reconcile waits: %v", err)
	}
	result, err := waitForAutomaticAssignmentPresenceResult(reconciled, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.err != nil || result.processed != 0 {
		t.Fatalf("presence reconcile processed=%d err=%v", result.processed, result.err)
	}

	var group GatewayUpstreamGroup
	if err := dbA.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "promo").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if !group.Present || group.PresenceGeneration != 2 {
		t.Fatalf("queued restore did not advance the retired generation: %+v", group)
	}
	var removedCount int64
	if err := dbA.Model(&GatewayUpstreamGroupChange{}).
		Where("upstream_group_id = ? AND kind = ?", group.ID, "removed").Count(&removedCount).Error; err != nil {
		t.Fatal(err)
	}
	if removedCount != 1 {
		t.Fatalf("serialized absence created %d removed audits, want 1", removedCount)
	}
}

func TestReconcileAutomaticAssignmentPresenceRechecksSeenRateBeforeRemovalPass(t *testing.T) {
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
		Name: "assignment-presence-seen-recheck", Type: ChannelTypeNewAPI,
		SiteURL: "https://assignment-presence-seen.example", MonitorEnabled: true,
	}
	if err := NewChannels(dbA).Create(channel); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	rates := NewRates(dbA)
	for index, groupName := range []string{"Promo", "Blocker"} {
		if _, err := rates.Upsert(&RateSnapshot{
			ChannelID: channel.ID, ModelName: groupName, Ratio: 1,
			LastSeenAt: base.Add(time.Duration(index) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}

	holderPID, releaseBlocker, blockerDone := holdRateSnapshotPresenceFenceForTest(t, dbA, channel.ID, "blocker")
	reconciled := make(chan automaticAssignmentPresenceResult, 1)
	go func() {
		processed, err := NewGateway(dbB).ReconcileAutomaticAssignmentPresence(1)
		reconciled <- automaticAssignmentPresenceResult{processed: processed, err: err}
	}()

	reconcilePID, waitErr := waitForAutomaticAssignmentFenceWaiterPID(dbC, holderPID, 10*time.Second)
	deleted := make(chan error, 1)
	deleteBlockedErr := error(nil)
	if waitErr == nil {
		go func() { deleted <- NewRates(dbC).DeleteSnapshot(channel.ID, "Promo") }()
		deleteBlockedErr = waitForPresenceTestBlocker(dbA, reconcilePID, 10*time.Second)
	}
	close(releaseBlocker)
	if err := waitForPresenceTestError(blockerDone, 10*time.Second); err != nil {
		t.Fatalf("release blocker presence fence: %v", err)
	}
	if waitErr != nil {
		t.Fatal(waitErr)
	}
	if deleteBlockedErr != nil {
		t.Fatal(deleteBlockedErr)
	}
	if err := waitForPresenceTestError(deleted, 10*time.Second); err != nil {
		t.Fatalf("delete previously seen rate: %v", err)
	}
	result, err := waitForAutomaticAssignmentPresenceResult(reconciled, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.err != nil || result.processed != 2 {
		t.Fatalf("presence reconcile processed=%d err=%v", result.processed, result.err)
	}

	var group GatewayUpstreamGroup
	if err := dbA.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "promo").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if group.Present {
		t.Fatalf("rate deleted after the added pass remained present: %+v", group)
	}
	var removedCount int64
	if err := dbA.Model(&GatewayUpstreamGroupChange{}).
		Where("upstream_group_id = ? AND kind = ?", group.ID, "removed").Count(&removedCount).Error; err != nil {
		t.Fatal(err)
	}
	if removedCount != 1 {
		t.Fatalf("removed audits after seen-rate deletion = %d, want 1", removedCount)
	}
}

func holdRateSnapshotPresenceFenceForTest(t *testing.T, db *gorm.DB, channelID uint, groupName string) (int, chan struct{}, <-chan error) {
	t.Helper()
	ready := make(chan automaticAssignmentFenceReady, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.Transaction(func(tx *gorm.DB) error {
			if _, err := lockRateSnapshotPresenceDomainTx(tx, channelID, groupName); err != nil {
				ready <- automaticAssignmentFenceReady{err: err}
				return err
			}
			var pid int
			if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				ready <- automaticAssignmentFenceReady{err: err}
				return err
			}
			ready <- automaticAssignmentFenceReady{pid: pid}
			<-release
			return nil
		})
	}()
	select {
	case result := <-ready:
		if result.err != nil {
			t.Fatalf("hold rate snapshot presence fence: %v", result.err)
		}
		return result.pid, release, done
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("timed out holding rate snapshot presence fence")
		return 0, release, done
	}
}

func holdAutomaticAssignmentFenceForPresenceTest(t *testing.T, db *gorm.DB) (int, chan struct{}, <-chan error) {
	t.Helper()
	ready := make(chan automaticAssignmentFenceReady, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.Transaction(func(tx *gorm.DB) error {
			if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
				ready <- automaticAssignmentFenceReady{err: err}
				return err
			}
			var pid int
			if err := tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error; err != nil {
				ready <- automaticAssignmentFenceReady{err: err}
				return err
			}
			ready <- automaticAssignmentFenceReady{pid: pid}
			<-release
			return nil
		})
	}()
	select {
	case result := <-ready:
		if result.err != nil {
			t.Fatalf("hold automatic-assignment fence: %v", result.err)
		}
		return result.pid, release, done
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("timed out holding automatic-assignment fence")
		return 0, release, done
	}
}

func waitForAutomaticAssignmentFenceWaiter(db *gorm.DB, holderPID int, timeout time.Duration) error {
	_, err := waitForAutomaticAssignmentFenceWaiterPID(db, holderPID, timeout)
	return err
}

func waitForAutomaticAssignmentFenceWaiterPID(db *gorm.DB, holderPID int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		var waiterPID int
		err := db.Raw(`
				SELECT COALESCE((
					SELECT waiting.pid
					FROM pg_locks waiting
					JOIN pg_locks holder
				  ON holder.locktype = waiting.locktype
				 AND holder.database IS NOT DISTINCT FROM waiting.database
				 AND holder.classid IS NOT DISTINCT FROM waiting.classid
				 AND holder.objid IS NOT DISTINCT FROM waiting.objid
				 AND holder.objsubid IS NOT DISTINCT FROM waiting.objsubid
				WHERE holder.pid = ?
				  AND holder.granted
					  AND waiting.pid <> holder.pid
					  AND NOT waiting.granted
					  AND waiting.locktype = 'advisory'
					ORDER BY waiting.pid
					LIMIT 1
				), 0)`, holderPID).Scan(&waiterPID).Error
		if err != nil {
			return 0, err
		}
		if waiterPID != 0 {
			return waiterPID, nil
		}
		if time.Now().After(deadline) {
			return 0, errors.New("timed out waiting for automatic-assignment fence waiter")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForPresenceTestBlocker(db *gorm.DB, blockerPID int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var blocked bool
		if err := db.Raw(`
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity waiting
				WHERE waiting.datname = current_database()
				  AND waiting.pid <> ?
				  AND ? = ANY(pg_blocking_pids(waiting.pid))
			)`, blockerPID, blockerPID).Scan(&blocked).Error; err != nil {
			return err
		}
		if blocked {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for channel-row waiter")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForPresenceTestError(result <-chan error, timeout time.Duration) error {
	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		return errors.New("timed out waiting for automatic-assignment fence release")
	}
}

func waitForAutomaticAssignmentPresenceResult(result <-chan automaticAssignmentPresenceResult, timeout time.Duration) (automaticAssignmentPresenceResult, error) {
	select {
	case value := <-result:
		return value, nil
	case <-time.After(timeout):
		return automaticAssignmentPresenceResult{}, fmt.Errorf("timed out waiting for automatic-assignment presence reconcile")
	}
}
