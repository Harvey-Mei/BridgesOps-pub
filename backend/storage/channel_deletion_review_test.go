package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

type channelDeletionManualReviewFixture struct {
	db       *gorm.DB
	channels *Channels
	channel  Channel
	intent   ChannelDeletionIntent
	claim    ChannelDeletionKeyClaim
}

func newChannelDeletionManualReviewFixture(t *testing.T) channelDeletionManualReviewFixture {
	t.Helper()
	db := testdb.Open(t)
	channels := NewChannels(db)
	channel := Channel{
		Name: "channel-deletion-manual-review", Type: ChannelTypeNewAPI,
		SiteURL: "https://channel-deletion-manual-review.invalid", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := NewRates(db).Upsert(&RateSnapshot{
		ChannelID: channel.ID, ModelName: "review", Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create rate: %v", err)
	}
	key := GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "review", UpstreamKeyID: 901, UpstreamKeyName: "managed-review",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
		APIKeyCipher:        "ciphertext", APIKeyPrefix: "sk-review", Enabled: true,
	}
	if err := db.Create(&key).Error; err != nil {
		t.Fatalf("create upstream key: %v", err)
	}
	if err := channels.DeleteContext(context.Background(), channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion: %v", err)
	}
	var intent ChannelDeletionIntent
	if err := db.Where("channel_id = ?", channel.ID).First(&intent).Error; err != nil {
		t.Fatalf("load deletion intent: %v", err)
	}
	var claim ChannelDeletionKeyClaim
	if err := db.Where("intent_id = ?", intent.ID).First(&claim).Error; err != nil {
		t.Fatalf("load deletion claim: %v", err)
	}
	return channelDeletionManualReviewFixture{db: db, channels: channels, channel: channel, intent: intent, claim: claim}
}

func (f channelDeletionManualReviewFixture) fence(claimEpoch uint64) ChannelDeletionKeyReviewFence {
	return ChannelDeletionKeyReviewFence{
		ChannelDeletionFence: ChannelDeletionFence{
			ChannelID: f.channel.ID, IntentID: f.intent.ID,
			DeletionEpoch: f.intent.DeletionEpoch, IdentityRevision: f.intent.ChannelIdentityRevision,
		},
		KeyClaimID: f.claim.ID, ClaimEpoch: claimEpoch,
	}
}

func (f channelDeletionManualReviewFixture) stageManualReview(t *testing.T, claimEpoch uint64) {
	t.Helper()
	at := time.Now().UTC()
	digests := []string{
		strings.Repeat("a", 64), strings.Repeat("b", 64),
		strings.Repeat("c", 64), strings.Repeat("d", 64),
	}
	if err := f.db.Model(&ChannelDeletionKeyClaim{}).Where("id = ?", f.claim.ID).Updates(map[string]any{
		"status":                     ChannelDeletionKeyManualReviewRequired,
		"claim_epoch":                claimEpoch,
		"observed_remote_id":         777,
		"claimed_at":                 nil,
		"lease_expires_at":           nil,
		"evidence_digest":            digests[0],
		"evidence_digest_version":    1,
		"ownership_digest":           digests[1],
		"ownership_digest_version":   1,
		"observed_fingerprint":       digests[2],
		"fingerprint_digest_version": 1,
		"inventory_digest":           digests[3],
		"inventory_digest_version":   1,
		"outcome_reason":             "remote_facts_changed",
		"outcome_actor":              channelDeletionCoordinatorActor,
		"outcome_at":                 at,
		"last_error":                 "remote_facts_changed",
		"updated_at":                 at,
	}).Error; err != nil {
		t.Fatalf("stage manual-review claim: %v", err)
	}
	if err := f.db.Model(&ChannelDeletionIntent{}).Where("id = ?", f.intent.ID).Updates(map[string]any{
		"status": ChannelDeletionIntentManualReviewRequired, "last_error": "remote_facts_changed", "updated_at": at,
	}).Error; err != nil {
		t.Fatalf("stage manual-review intent: %v", err)
	}
	if err := f.db.Model(&Channel{}).Where("id = ?", f.channel.ID).Updates(map[string]any{
		"deletion_state": ChannelDeletionIntentManualReviewRequired, "updated_at": at,
	}).Error; err != nil {
		t.Fatalf("stage manual-review channel: %v", err)
	}
}

func channelDeletionOutcomeDigestsCleared(claim ChannelDeletionKeyClaim) bool {
	return claim.ObservedRemoteID == 0 && claim.EvidenceDigest == "" && claim.EvidenceDigestVersion == nil &&
		claim.OwnershipDigest == "" && claim.OwnershipDigestVersion == nil &&
		claim.ObservedFingerprint == "" && claim.FingerprintDigestVersion == nil &&
		claim.InventoryDigest == "" && claim.InventoryDigestVersion == nil
}

func channelDeletionEvidenceOutcomeDigestsCleared(evidence ChannelDeletionKeyEvidence) bool {
	return evidence.ObservedRemoteID == 0 && evidence.EvidenceDigest == "" && evidence.EvidenceDigestVersion == nil &&
		evidence.OwnershipDigest == "" && evidence.OwnershipDigestVersion == nil &&
		evidence.ObservedFingerprint == "" && evidence.FingerprintDigestVersion == nil &&
		evidence.InventoryDigest == "" && evidence.InventoryDigestVersion == nil
}

func TestChannelDeletionManualReviewRequeueClearsCurrentAndEventOutcomeDigests(t *testing.T) {
	fixture := newChannelDeletionManualReviewFixture(t)
	fixture.stageManualReview(t, 0)

	if err := fixture.channels.RequeueChannelDeletionKeyManualReview(
		context.Background(), fixture.fence(0), "remote_inventory_rechecked", "review-admin", time.Now().UTC(),
	); err != nil {
		t.Fatalf("requeue manual-review claim: %v", err)
	}
	var claim ChannelDeletionKeyClaim
	if err := fixture.db.First(&claim, fixture.claim.ID).Error; err != nil {
		t.Fatalf("load requeued claim: %v", err)
	}
	if claim.Status != ChannelDeletionKeyPendingVerification || claim.ClaimEpoch != 1 || !channelDeletionOutcomeDigestsCleared(claim) {
		t.Fatalf("requeued claim retained stale outcome evidence: %+v", claim)
	}
	var evidence ChannelDeletionKeyEvidence
	if err := fixture.db.Where("key_claim_id = ? AND event_kind = ?", fixture.claim.ID, "manual_review_requeued").First(&evidence).Error; err != nil {
		t.Fatalf("load requeue evidence: %v", err)
	}
	if evidence.StatusAfter != ChannelDeletionKeyPendingVerification || evidence.ClaimEpoch != 1 || !channelDeletionEvidenceOutcomeDigestsCleared(evidence) {
		t.Fatalf("requeue event retained stale outcome evidence: %+v", evidence)
	}
}

func TestChannelDeletionClaimEpochCannotAdvancePastPostgresLimit(t *testing.T) {
	const highestPersistedEpoch = uint64(1<<63 - 2)
	if channelDeletionMaxClaimEpoch != highestPersistedEpoch {
		t.Fatalf("maximum persisted claim epoch = %d, want %d", channelDeletionMaxClaimEpoch, highestPersistedEpoch)
	}
	fixture := newChannelDeletionManualReviewFixture(t)
	fixture.stageManualReview(t, highestPersistedEpoch)
	fence := fixture.fence(highestPersistedEpoch)

	if err := fixture.channels.RequeueChannelDeletionKeyManualReview(
		context.Background(), fence, "remote_inventory_rechecked", "review-admin", time.Now().UTC(),
	); !errors.Is(err, ErrChannelDeletionKeyClaimLost) {
		t.Fatalf("requeue at maximum epoch error = %v, want ErrChannelDeletionKeyClaimLost", err)
	}
	if err := fixture.channels.RetainChannelDeletionKeyAfterReview(
		context.Background(), fence, "verified_external_owner", "review-admin", time.Now().UTC(),
	); !errors.Is(err, ErrChannelDeletionKeyClaimLost) {
		t.Fatalf("retain at maximum epoch error = %v, want ErrChannelDeletionKeyClaimLost", err)
	}
	if err := fixture.db.Model(&ChannelDeletionKeyClaim{}).Where("id = ?", fixture.claim.ID).Updates(map[string]any{
		"status":                     ChannelDeletionKeyPendingVerification,
		"evidence_digest":            "",
		"evidence_digest_version":    nil,
		"ownership_digest":           "",
		"ownership_digest_version":   nil,
		"observed_fingerprint":       "",
		"fingerprint_digest_version": nil,
		"inventory_digest":           "",
		"inventory_digest_version":   nil,
		"outcome_reason":             "",
		"outcome_actor":              "",
		"outcome_at":                 nil,
		"last_error":                 "",
	}).Error; err != nil {
		t.Fatalf("stage maximum-epoch worker claim: %v", err)
	}
	workerFence := fence.ChannelDeletionFence
	if _, claimed, err := fixture.channels.ClaimChannelDeletionKey(
		context.Background(), workerFence, fixture.claim.ID, time.Minute,
	); err == nil || claimed || !strings.Contains(err.Error(), "claim epoch is exhausted") {
		t.Fatalf("worker claim at maximum epoch claimed=%v error=%v", claimed, err)
	}
	var persisted ChannelDeletionKeyClaim
	if err := fixture.db.First(&persisted, fixture.claim.ID).Error; err != nil {
		t.Fatalf("load maximum-epoch claim: %v", err)
	}
	if persisted.ClaimEpoch != highestPersistedEpoch || persisted.Status != ChannelDeletionKeyPendingVerification {
		t.Fatalf("maximum-epoch claim mutated: %+v", persisted)
	}
}
