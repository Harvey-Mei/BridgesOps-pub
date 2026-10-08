package storage

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

type managedKeyRebuildRevisionFixture struct {
	db             *gorm.DB
	channels       *Channels
	store          *Gateway
	channel        *Channel
	generation     UpstreamGroupGeneration
	local          *GatewayUpstreamKey
	evidenceDigest string
	ownershipClaim string
}

func TestManagedKeyRebuildClaimKeyBindsChannelIdentityRevision(t *testing.T) {
	generation := UpstreamGroupGeneration{
		ID: 3, ChannelID: 5, CanonicalGroupName: "promo", PresenceGeneration: 7,
	}
	first := managedUpstreamKeyRebuildClaimKey(generation, 11, 13, 17, "BOG - promo")
	second := managedUpstreamKeyRebuildClaimKey(generation, 12, 13, 17, "BOG - promo")
	if first == second || len(first) != 64 || len(second) != 64 {
		t.Fatalf("revision-bound claim keys first=%q second=%q", first, second)
	}
}

func newManagedKeyRebuildRevisionFixture(t *testing.T, name string) managedKeyRebuildRevisionFixture {
	t.Helper()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{
		Name: name, Type: ChannelTypeNewAPI,
		SiteURL: "https://rebuild-revision.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	local := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1,
		UpstreamKeyID: 71, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "cipher-revision", APIKeyPrefix: "sk-revision", Enabled: true,
	}
	if err := store.UpsertUpstreamKey(local); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, local.GroupName, "added", local.UpstreamKeyID, local.UpstreamKeyName, time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, local.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	ownershipClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(*local)
	if err != nil {
		t.Fatal(err)
	}
	return managedKeyRebuildRevisionFixture{
		db: db, channels: channels, store: store, channel: channel,
		generation: generation, local: local,
		evidenceDigest: strings.Repeat("a", 64), ownershipClaim: ownershipClaim,
	}
}

func (f managedKeyRebuildRevisionFixture) begin(t *testing.T) *GatewayManagedKeyRebuildIntent {
	t.Helper()
	if err := f.store.PrepareChannelUpstreamKeysForRebuild(
		f.channel.ID, f.channel.UpstreamAccountIdentityRevision, GatewayManagedKeyRebuildPendingError,
	); err != nil {
		t.Fatalf("prepare rebuild key: %v", err)
	}
	intent, claimed, err := f.store.BeginGenerationSafeManagedUpstreamKeyRebuild(
		f.generation, f.channel.UpstreamAccountIdentityRevision, *f.local,
		f.local.UpstreamKeyID, f.local.UpstreamKeyName, f.evidenceDigest, f.ownershipClaim,
		time.Now(), 10*time.Minute,
	)
	if err != nil || !claimed || intent == nil {
		t.Fatalf("begin rebuild intent=%+v claimed=%t err=%v", intent, claimed, err)
	}
	return intent
}

func (f managedKeyRebuildRevisionFixture) advanceIdentity(t *testing.T) uint64 {
	t.Helper()
	previous := f.channel.UpstreamAccountIdentityRevision
	f.channel.SiteURL = "https://rebuild-revision-new-account.example"
	if err := f.channels.UpdateWithUpstreamAccountIdentityChange(f.channel); err != nil {
		t.Fatal(err)
	}
	if f.channel.UpstreamAccountIdentityRevision != previous+1 {
		t.Fatalf("identity revision=%d, want %d", f.channel.UpstreamAccountIdentityRevision, previous+1)
	}
	return previous
}

func TestManagedKeyRebuildIntentPersistsChannelIdentityRevision(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-intent-revision")
	intent := fixture.begin(t)
	if intent.ChannelIdentityRevision != fixture.channel.UpstreamAccountIdentityRevision {
		t.Fatalf("intent revision=%d, want %d", intent.ChannelIdentityRevision, fixture.channel.UpstreamAccountIdentityRevision)
	}
	var persisted GatewayManagedKeyRebuildIntent
	if err := fixture.db.First(&persisted, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.ChannelIdentityRevision != fixture.channel.UpstreamAccountIdentityRevision {
		t.Fatalf("persisted revision=%d, want %d", persisted.ChannelIdentityRevision, fixture.channel.UpstreamAccountIdentityRevision)
	}
	oldClaim := managedUpstreamKeyRebuildClaimKey(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision,
		fixture.local.ID, fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName,
	)
	newClaim := managedUpstreamKeyRebuildClaimKey(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision+1,
		fixture.local.ID, fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName,
	)
	if oldClaim == newClaim || persisted.ClaimKey != oldClaim {
		t.Fatalf("revision-bound claim keys old=%q new=%q persisted=%q", oldClaim, newClaim, persisted.ClaimKey)
	}
}

func TestManagedKeyRebuildBeginRejectsStaleChannelIdentityRevision(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-begin-stale-revision")
	oldRevision := fixture.advanceIdentity(t)
	intent, claimed, err := fixture.store.BeginGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, oldRevision, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName,
		fixture.evidenceDigest, fixture.ownershipClaim, time.Now(), 10*time.Minute,
	)
	if !errors.Is(err, ErrStaleChannelAccountIdentityRevision) || claimed || intent != nil {
		t.Fatalf("stale begin intent=%+v claimed=%t err=%v", intent, claimed, err)
	}
	var count int64
	if err := fixture.db.Model(&GatewayManagedKeyRebuildIntent{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("stale begin persisted %d intents", count)
	}
}

func TestManagedKeyRebuildPrepareRejectsStaleIdentityWithoutDisablingCurrentState(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-prepare-stale-revision")
	oldRevision := fixture.advanceIdentity(t)
	err := fixture.store.PrepareChannelUpstreamKeysForRebuild(
		fixture.channel.ID, oldRevision, "managed upstream key rebuild pending",
	)
	if !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("prepare error=%v, want stale identity revision", err)
	}
	current, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.local.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	if !current.Enabled || current.LastError != "" {
		t.Fatalf("stale prepare changed current routing key: %+v", current)
	}
}

func TestManagedKeyRebuildFinalizeRejectsChangedIdentityBeforeCallback(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-finalize-revision")
	intent := fixture.begin(t)
	oldRevision := fixture.advanceIdentity(t)
	callbackCalls := 0
	err := fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, oldRevision, intent.ID, intent.ClaimEpoch, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName,
		fixture.evidenceDigest, fixture.ownershipClaim, time.Now(), func() error {
			callbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("finalize error=%v, want stale identity revision", err)
	}
	if callbackCalls != 0 {
		t.Fatalf("remote callback calls=%d, want 0", callbackCalls)
	}
	var persisted GatewayManagedKeyRebuildIntent
	if err := fixture.db.First(&persisted, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Status != "processing" {
		t.Fatalf("intent status=%q, want processing", persisted.Status)
	}
	beforeReview, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.local.GroupName)
	if err != nil {
		t.Fatal(err)
	}

	err = fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(
		fixture.generation, oldRevision, intent.ID, intent.ClaimEpoch, "identity_changed", "test", time.Now(),
	)
	if !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("manual-review error=%v, want stale identity revision", err)
	}
	if err := fixture.db.First(&persisted, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Status != "manual_review" {
		t.Fatalf("intent status=%q, want manual_review", persisted.Status)
	}
	current, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.local.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	if current.Enabled != beforeReview.Enabled || current.LastError != beforeReview.LastError {
		t.Fatalf("old-revision manual review changed current routing key: %+v", current)
	}
}

func TestManagedKeyRebuildReconcileDoesNotAckAnotherIdentityRevision(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-reconcile-revision")
	intent := fixture.begin(t)
	fixture.advanceIdentity(t)
	if err := fixture.store.ReconcileGenerationSafeManagedUpstreamKeyRebuilds(
		fixture.channel.ID, fixture.channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now(),
	); err != nil {
		t.Fatal(err)
	}
	var persisted GatewayManagedKeyRebuildIntent
	if err := fixture.db.First(&persisted, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Status != "manual_review" {
		t.Fatalf("cross-revision intent status=%q, want manual_review", persisted.Status)
	}
	beforeReconcile, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.local.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.ReconcileGenerationSafeManagedUpstreamKeyRebuilds(
		fixture.channel.ID, fixture.channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now().Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	current, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.local.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	if current.Enabled != beforeReconcile.Enabled || current.LastError != beforeReconcile.LastError {
		t.Fatalf("cross-revision reconcile changed current routing key: %+v", current)
	}
}

func TestManagedKeyRebuildFinalizeHoldsChannelIdentityRowAcrossCallback(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-finalize-row-lock")
	intent := fixture.begin(t)
	dbB, err := testdb.ReopenWithError(t, fixture.db)
	if err != nil {
		t.Fatal(err)
	}
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
	defer release()
	finalized := make(chan error, 1)
	go func() {
		finalized <- fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
			fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, intent.ID, intent.ClaimEpoch, *fixture.local,
			fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName,
			fixture.evidenceDigest, fixture.ownershipClaim, time.Now(), func() error {
				close(callbackEntered)
				<-releaseCallback
				return nil
			},
		)
	}()
	select {
	case <-callbackEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("finalize callback did not start")
	}

	updatedChannel := *fixture.channel
	updatedChannel.SiteURL = "https://rebuild-finalize-row-lock-new.example"
	updateStarted := make(chan struct{})
	updated := make(chan error, 1)
	go func() {
		close(updateStarted)
		updated <- NewChannels(dbB).UpdateWithUpstreamAccountIdentityChange(&updatedChannel)
	}()
	<-updateStarted
	select {
	case err := <-updated:
		t.Fatalf("identity update completed while remote callback held channel row: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	release()
	if err := <-finalized; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("identity update remained blocked after finalize commit")
	}
	if updatedChannel.UpstreamAccountIdentityRevision != fixture.channel.UpstreamAccountIdentityRevision+1 {
		t.Fatalf("updated revision=%d, want %d", updatedChannel.UpstreamAccountIdentityRevision, fixture.channel.UpstreamAccountIdentityRevision+1)
	}
}

func TestManagedKeyRebuildStaleClaimantCannotExecuteCallbackAfterReclaim(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-claim-epoch")
	first := fixture.begin(t)
	reclaimed, claimed, err := fixture.store.BeginGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		first.UpdatedAt.Add(time.Hour), time.Minute,
	)
	if err != nil || !claimed || reclaimed == nil || reclaimed.ClaimEpoch != first.ClaimEpoch+1 {
		t.Fatalf("reclaimed intent=%+v claimed=%t err=%v", reclaimed, claimed, err)
	}
	callbackCalls := 0
	err = fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, first.ID, first.ClaimEpoch, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		time.Now(), func() error {
			callbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrManagedUpstreamKeyRebuildClaimLost) || callbackCalls != 0 {
		t.Fatalf("stale finalize err=%v callback_calls=%d", err, callbackCalls)
	}
	if err := fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, reclaimed.ID, reclaimed.ClaimEpoch, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		time.Now(), func() error {
			callbackCalls++
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if callbackCalls != 1 {
		t.Fatalf("current claimant callback_calls=%d, want 1", callbackCalls)
	}
}

func TestManagedKeyRebuildFinalizeRejectsReenabledLocalKeyBeforeCallback(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-local-state-fence")
	intent := fixture.begin(t)
	if err := fixture.db.Model(&GatewayUpstreamKey{}).Where("id = ?", fixture.local.ID).Updates(map[string]any{
		"enabled": true, "last_error": "",
	}).Error; err != nil {
		t.Fatal(err)
	}
	callbackCalls := 0
	err := fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, intent.ID, intent.ClaimEpoch, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		time.Now(), func() error {
			callbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) || callbackCalls != 0 {
		t.Fatalf("reenabled finalize err=%v callback_calls=%d", err, callbackCalls)
	}
}

func TestManagedKeyRebuildBeginRejectsEnabledLocalKey(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-begin-local-state-fence")
	intent, claimed, err := fixture.store.BeginGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		time.Now(), time.Minute,
	)
	if !errors.Is(err, ErrManagedUpstreamKeyRemovalIntentChanged) || claimed || intent != nil {
		t.Fatalf("enabled begin intent=%+v claimed=%t err=%v", intent, claimed, err)
	}
	var count int64
	if err := fixture.db.Model(&GatewayManagedKeyRebuildIntent{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("enabled local key persisted %d intents", count)
	}
}

func TestManagedKeyRebuildAllowsCanonicalSiblingWhenIdentityIsExplicitlyBound(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-canonical-sibling")
	sibling := *fixture.local
	sibling.ID = 0
	sibling.GroupName = "PROMO"
	sibling.UpstreamKeyID = 72
	sibling.UpstreamKeyName = "BOG - PROMO"
	sibling.APIKeyCipher = "cipher-revision-sibling"
	sibling.APIKeyPrefix = "sk-sibling"
	// Insert the pre-existing raw-name sibling directly. The ordinary upsert
	// path intentionally merges keys in one canonical routing domain.
	if err := fixture.db.Create(&sibling).Error; err != nil {
		t.Fatal(err)
	}
	siblingClaim, err := ManagedUpstreamKeyOwnershipClaimDigest(sibling)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.PrepareChannelUpstreamKeysForRebuild(
		fixture.channel.ID, fixture.channel.UpstreamAccountIdentityRevision, GatewayManagedKeyRebuildPendingError,
	); err != nil {
		t.Fatal(err)
	}
	first, firstClaimed, err := fixture.store.BeginGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		time.Now(), time.Minute,
	)
	if err != nil || !firstClaimed || first == nil {
		t.Fatalf("first canonical sibling intent=%+v claimed=%t err=%v", first, firstClaimed, err)
	}
	second, secondClaimed, err := fixture.store.BeginGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, sibling,
		sibling.UpstreamKeyID, sibling.UpstreamKeyName, strings.Repeat("b", 64), siblingClaim,
		time.Now(), time.Minute,
	)
	if err != nil || !secondClaimed || second == nil || second.ID == first.ID {
		t.Fatalf("second canonical sibling intent=%+v claimed=%t err=%v", second, secondClaimed, err)
	}
}

func TestManagedKeyRebuildReconcileSkipsManualReviewForSupersededGeneration(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-reconcile-superseded-review")
	intent := fixture.begin(t)
	if err := fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, intent.ID, intent.ClaimEpoch,
		"remote_revalidation_failed", "test_reviewer", time.Now(),
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.RecordUpstreamGroupChange(fixture.channel.ID, fixture.local.GroupName, "removed", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.RecordUpstreamGroupChange(fixture.channel.ID, fixture.local.GroupName, "added", 0, "", time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	currentGeneration, err := fixture.store.CaptureUpstreamGroupGeneration(fixture.channel.ID, fixture.local.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	if currentGeneration.PresenceGeneration == fixture.generation.PresenceGeneration {
		t.Fatalf("generation did not advance: old=%+v current=%+v", fixture.generation, currentGeneration)
	}
	if err := fixture.store.ReconcileGenerationSafeManagedUpstreamKeyRebuilds(
		fixture.channel.ID, fixture.channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now().Add(2*time.Second),
	); err != nil {
		t.Fatalf("reconcile stale manual-review intent: %v", err)
	}
	var persisted GatewayManagedKeyRebuildIntent
	if err := fixture.db.First(&persisted, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Status != "manual_review" {
		t.Fatalf("superseded manual-review intent status=%q, want manual_review", persisted.Status)
	}
}

func TestManagedKeyRebuildReconcileSkipsManualReviewForReboundLocalIdentity(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-reconcile-rebound-review")
	intent := fixture.begin(t)
	if err := fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, intent.ID, intent.ClaimEpoch,
		"remote_revalidation_failed", "test_reviewer", time.Now(),
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.Model(&GatewayUpstreamKey{}).Where("id = ?", fixture.local.ID).Updates(map[string]any{
		"upstream_key_id":   72,
		"upstream_key_name": "BOG - promo replacement",
		"api_key_cipher":    "cipher-replacement",
		"api_key_prefix":    "sk-replacement",
		"enabled":           false,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.ReconcileGenerationSafeManagedUpstreamKeyRebuilds(
		fixture.channel.ID, fixture.channel.UpstreamAccountIdentityRevision, map[int64]string{}, time.Now().Add(time.Second),
	); err != nil {
		t.Fatalf("reconcile stale rebound manual-review intent: %v", err)
	}
	var persisted GatewayManagedKeyRebuildIntent
	if err := fixture.db.First(&persisted, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Status != "manual_review" || persisted.ClaimEpoch != intent.ClaimEpoch+1 {
		t.Fatalf("rebound manual-review intent changed: %+v", persisted)
	}
}

func TestManagedKeyRebuildManualReviewPersistsAuditAndRevalidates(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-manual-review-recovery")
	intent := fixture.begin(t)
	reviewedAt := time.Now().UTC().Add(time.Second)
	if err := fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuildManualReview(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, intent.ID, intent.ClaimEpoch,
		"remote_revalidation_failed", "test_reviewer", reviewedAt,
	); err != nil {
		t.Fatal(err)
	}
	var reviewed GatewayManagedKeyRebuildIntent
	if err := fixture.db.First(&reviewed, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reviewed.Status != "manual_review" || reviewed.ClaimEpoch != intent.ClaimEpoch+1 ||
		reviewed.ManualReviewReason != "remote_revalidation_failed" || reviewed.ManualReviewActor != "test_reviewer" ||
		reviewed.ManualReviewedAt == nil || reviewed.ResolvedAt != nil || reviewed.ManualReviewResolution != "" {
		t.Fatalf("manual-review audit=%+v", reviewed)
	}
	resolvedAt := reviewedAt.Add(time.Second)
	resolved, err := fixture.store.ResolveGenerationSafeManagedUpstreamKeyRebuildManualReview(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, reviewed.ID, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		"test_administrator", managedKeyRebuildReviewResolutionFacts, resolvedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Status != "processing" || resolved.ClaimEpoch != reviewed.ClaimEpoch+1 ||
		resolved.ManualReviewResolution != managedKeyRebuildReviewResolutionFacts || resolved.ManualReviewResolvedBy != "test_administrator" ||
		resolved.ManualReviewResolvedAt == nil {
		t.Fatalf("resolved manual-review intent=%+v", resolved)
	}
	current, err := fixture.store.FindUpstreamKeyAny(fixture.channel.ID, fixture.local.GroupName)
	if err != nil {
		t.Fatal(err)
	}
	if current.Enabled || current.LastError != GatewayManagedKeyRebuildPendingError {
		t.Fatalf("resolved local state=%+v", current)
	}
	callbackCalls := 0
	err = fixture.store.FinalizeGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, reviewed.ID, reviewed.ClaimEpoch, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		time.Now(), func() error {
			callbackCalls++
			return nil
		},
	)
	if !errors.Is(err, ErrManagedUpstreamKeyRebuildClaimLost) || callbackCalls != 0 {
		t.Fatalf("pre-resolution lease err=%v callback_calls=%d", err, callbackCalls)
	}
}

func TestManagedKeyRebuildDigestUpgradeReusesIntentAndCanBeRevalidated(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-digest-upgrade")
	intent := fixture.begin(t)
	if err := fixture.db.Model(&GatewayManagedKeyRebuildIntent{}).Where("id = ?", intent.ID).
		Update("evidence_digest_version", 99).Error; err != nil {
		t.Fatal(err)
	}
	holder, claimed, err := fixture.store.BeginGenerationSafeManagedUpstreamKeyRebuild(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		time.Now().Add(time.Hour), time.Minute,
	)
	if err != nil || claimed || holder == nil || holder.ID != intent.ID || holder.Status != "manual_review" || holder.ManualReviewReason != "unsupported_digest_version" {
		t.Fatalf("unsupported digest holder=%+v claimed=%t err=%v", holder, claimed, err)
	}
	var count int64
	if err := fixture.db.Model(&GatewayManagedKeyRebuildIntent{}).Where("channel_id = ?", fixture.channel.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("digest upgrade created %d intents, want 1", count)
	}
	resolved, err := fixture.store.ResolveGenerationSafeManagedUpstreamKeyRebuildManualReview(
		fixture.generation, fixture.channel.UpstreamAccountIdentityRevision, holder.ID, *fixture.local,
		fixture.local.UpstreamKeyID, fixture.local.UpstreamKeyName, fixture.evidenceDigest, fixture.ownershipClaim,
		"test_administrator", managedKeyRebuildReviewResolutionFacts, time.Now().Add(2*time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !managedKeyRebuildDigestVersionsSupported(*resolved) || resolved.ID != intent.ID || resolved.Status != "processing" {
		t.Fatalf("revalidated digest intent=%+v", resolved)
	}
}

func TestChannelDeletePrepareRetainsManagedKeyRebuildIntents(t *testing.T) {
	fixture := newManagedKeyRebuildRevisionFixture(t, "rebuild-channel-delete")
	fixture.begin(t)
	if err := fixture.channels.Delete(fixture.channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatal(err)
	}
	var count int64
	if err := fixture.db.Model(&GatewayManagedKeyRebuildIntent{}).Where("channel_id = ?", fixture.channel.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("channel delete prepare changed rebuild intents: %d", count)
	}
}
