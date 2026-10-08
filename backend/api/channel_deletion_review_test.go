package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type channelDeletionReviewAPIFixture struct {
	db       *gorm.DB
	channels *storage.Channels
	router   *gin.Engine
	channel  storage.Channel
	intent   storage.ChannelDeletionIntent
	claim    storage.ChannelDeletionKeyClaim
}

func newChannelDeletionReviewAPIFixture(t *testing.T, provenance string) channelDeletionReviewAPIFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	storedChannel := storage.Channel{
		Name: t.Name(), Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://channel-deletion-review.invalid", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(&storedChannel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := storage.NewRates(db).Upsert(&storage.RateSnapshot{
		ChannelID: storedChannel.ID, ModelName: "review", Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create rate: %v", err)
	}
	key := storage.GatewayUpstreamKey{
		ChannelID: storedChannel.ID, GroupName: "review", UpstreamKeyID: 901,
		UpstreamKeyName: "managed-review", OwnershipProvenance: provenance,
		APIKeyCipher: "ciphertext", APIKeyPrefix: "sk-review", Enabled: true,
	}
	if err := db.Create(&key).Error; err != nil {
		t.Fatalf("create upstream key: %v", err)
	}
	if err := channels.DeleteContext(context.Background(), storedChannel.ID); !errors.Is(err, storage.ErrChannelDeletionPending) {
		t.Fatalf("prepare deletion: %v", err)
	}
	var intent storage.ChannelDeletionIntent
	if err := db.Where("channel_id = ?", storedChannel.ID).First(&intent).Error; err != nil {
		t.Fatalf("load deletion intent: %v", err)
	}
	var claim storage.ChannelDeletionKeyClaim
	if err := db.Where("intent_id = ?", intent.ID).First(&claim).Error; err != nil {
		t.Fatalf("load deletion claim: %v", err)
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "review-admin")
		c.Next()
	})
	rates := storage.NewRates(db)
	service := channel.NewService(channels, nil, nil, rates, nil, nil)
	registerChannels(router.Group("/api"), &Deps{
		Channels:   channels,
		ChannelSvc: service,
		Gateway:    gateway.NewService(channels, rates, storage.NewGateway(db), nil, nil, nil),
	})
	return channelDeletionReviewAPIFixture{
		db: db, channels: channels, router: router, channel: storedChannel, intent: intent, claim: claim,
	}
}

func (f channelDeletionReviewAPIFixture) request(t *testing.T, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	f.router.ServeHTTP(rec, req)
	return rec
}

func (f channelDeletionReviewAPIFixture) actionBody(resolution string) map[string]string {
	return map[string]string{
		"expected_intent_id":         fmt.Sprint(f.intent.ID),
		"expected_deletion_epoch":    fmt.Sprint(f.intent.DeletionEpoch),
		"expected_identity_revision": fmt.Sprint(f.intent.ChannelIdentityRevision),
		"expected_claim_epoch":       fmt.Sprint(f.claim.ClaimEpoch),
		"resolution":                 resolution,
	}
}

func TestChannelDeletionReviewAPIRequiresClaimEpochFencePresence(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	deps := &Deps{Channels: storage.NewChannels(nil)}
	router.POST("/api/channels/:id/deletion/claims/:claim_id/retain", func(c *gin.Context) {
		retainChannelDeletionReviewClaim(c, deps)
	})

	body := `{"expected_intent_id":"1","expected_deletion_epoch":"1","expected_identity_revision":"1","resolution":"retain"}`
	req := httptest.NewRequest(http.MethodPost, "/api/channels/1/deletion/claims/1/retain", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest || !bytes.Contains(rec.Body.Bytes(), []byte("fence or resolution is invalid")) {
		t.Fatalf("missing claim epoch status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestChannelDeletionReviewOutputSerializesEveryDatabaseIdentifierExactly(t *testing.T) {
	const base = uint64(9007199254740993)
	channelID := uint(base + 1)
	localKeyID := uint(base + 2)
	upstreamGroupID := uint(base + 3)
	upstreamKeyID := int64(base + 4)
	snapshot := storage.ChannelDeletionReviewSnapshot{
		Intent: storage.ChannelDeletionIntent{
			ID: base, ChannelID: channelID, DeletionEpoch: base + 5,
			ChannelIdentityRevision: base + 6,
		},
		Claims: []storage.ChannelDeletionKeyClaim{{
			ID: base + 7, IntentID: base, ChannelID: channelID,
			LocalKeyID: localKeyID, UpstreamGroupID: &upstreamGroupID,
			UpstreamKeyID: upstreamKeyID, ObservedRemoteID: int64(base + 8),
		}},
	}
	raw, err := json.Marshal(channelDeletionReviewOutputFor(snapshot))
	if err != nil {
		t.Fatal(err)
	}
	for _, exact := range []string{
		`"intent_id":"9007199254740993"`,
		`"channel_id":"9007199254740994"`,
		`"local_key_id":"9007199254740995"`,
		`"upstream_group_id":"9007199254740996"`,
		`"upstream_key_id":"9007199254740997"`,
		`"observed_remote_id":"9007199254741001"`,
	} {
		if !bytes.Contains(raw, []byte(exact)) {
			t.Fatalf("review output missing exact identifier %s: %s", exact, raw)
		}
	}
}

func TestChannelDeletionReviewAPIListsExactFenceAndRetainsWithActorIdempotently(t *testing.T) {
	fixture := newChannelDeletionReviewAPIFixture(t, storage.GatewayUpstreamKeyProvenanceUnknown)
	if fixture.claim.Status != storage.ChannelDeletionKeyManualReviewRequired || fixture.claim.ClaimEpoch != 0 {
		t.Fatalf("prepared unknown claim = %+v", fixture.claim)
	}

	rec := fixture.request(t, http.MethodGet, fmt.Sprintf("/api/channels/%d/deletion", fixture.channel.ID), nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, exact := range []string{
		fmt.Sprintf(`"intent_id":"%d"`, fixture.intent.ID),
		fmt.Sprintf(`"deletion_epoch":"%d"`, fixture.intent.DeletionEpoch),
		fmt.Sprintf(`"channel_identity_revision":"%d"`, fixture.intent.ChannelIdentityRevision),
		fmt.Sprintf(`"id":"%d"`, fixture.claim.ID),
		`"claim_epoch":"0"`,
	} {
		if !bytes.Contains(rec.Body.Bytes(), []byte(exact)) {
			t.Fatalf("detail missing %s: %s", exact, rec.Body.String())
		}
	}

	path := fmt.Sprintf("/api/channels/%d/deletion/claims/%d/retain", fixture.channel.ID, fixture.claim.ID)
	body := fixture.actionBody("verified_external_owner")
	for attempt := 0; attempt < 2; attempt++ {
		rec = fixture.request(t, http.MethodPost, path, body)
		if rec.Code != http.StatusOK {
			t.Fatalf("retain attempt %d status=%d body=%s", attempt+1, rec.Code, rec.Body.String())
		}
	}
	var retained storage.ChannelDeletionKeyClaim
	if err := fixture.db.First(&retained, fixture.claim.ID).Error; err != nil {
		t.Fatal(err)
	}
	if retained.Status != storage.ChannelDeletionKeyRetainedByReview || retained.ClaimEpoch != 1 ||
		retained.OutcomeActor != "review-admin" || retained.OutcomeReason != "verified_external_owner" {
		t.Fatalf("retained claim = %+v", retained)
	}
	var evidenceCount int64
	if err := fixture.db.Model(&storage.ChannelDeletionKeyEvidence{}).
		Where("key_claim_id = ? AND event_kind = ? AND actor = ?", fixture.claim.ID, "manual_review_retained", "review-admin").
		Count(&evidenceCount).Error; err != nil {
		t.Fatal(err)
	}
	if evidenceCount != 1 {
		t.Fatalf("retention evidence rows = %d, want 1", evidenceCount)
	}

	conflicting := fixture.actionBody("different_decision")
	rec = fixture.request(t, http.MethodPost, path, conflicting)
	if rec.Code != http.StatusConflict {
		t.Fatalf("stale different decision status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = fixture.request(t, http.MethodGet, "/api/channels/deletions?status=ready_for_local_finalize&page=1&page_size=10", nil)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(fmt.Sprintf(`"intent_id":"%d"`, fixture.intent.ID))) {
		t.Fatalf("review list status=%d body=%s", rec.Code, rec.Body.String())
	}

	fence := storage.ChannelDeletionFence{
		IntentID: fixture.intent.ID, ChannelID: fixture.channel.ID,
		DeletionEpoch: fixture.intent.DeletionEpoch, IdentityRevision: fixture.intent.ChannelIdentityRevision,
	}
	if err := fixture.channels.FinalizeChannelDeletion(context.Background(), fence, "review-finalizer", time.Now().UTC()); err != nil {
		t.Fatalf("finalize retained review: %v", err)
	}
	rec = fixture.request(t, http.MethodPost, path, body)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`"status":"finalized"`)) {
		t.Fatalf("post-finalization replay status=%d body=%s", rec.Code, rec.Body.String())
	}
	if err := fixture.db.Model(&storage.ChannelDeletionKeyEvidence{}).
		Where("key_claim_id = ? AND event_kind = ? AND actor = ?", fixture.claim.ID, "manual_review_retained", "review-admin").
		Count(&evidenceCount).Error; err != nil {
		t.Fatal(err)
	}
	if evidenceCount != 1 {
		t.Fatalf("post-finalization replay evidence rows = %d, want 1", evidenceCount)
	}
}

func TestChannelDeletionReviewAPIRequeuesRetryableClaimWithExactFence(t *testing.T) {
	fixture := newChannelDeletionReviewAPIFixture(t, storage.GatewayUpstreamKeyProvenanceCreated)
	if fixture.claim.Status != storage.ChannelDeletionKeyPendingVerification {
		t.Fatalf("prepared managed claim status = %q", fixture.claim.Status)
	}
	at := time.Now().UTC()
	if err := fixture.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&storage.ChannelDeletionKeyClaim{}).Where("id = ?", fixture.claim.ID).Updates(map[string]any{
			"status": storage.ChannelDeletionKeyManualReviewRequired, "outcome_reason": "remote_contract_changed", "updated_at": at,
		}).Error; err != nil {
			return err
		}
		if err := tx.Model(&storage.ChannelDeletionIntent{}).Where("id = ?", fixture.intent.ID).Updates(map[string]any{
			"status": storage.ChannelDeletionIntentManualReviewRequired, "last_error": "remote_contract_changed", "updated_at": at,
		}).Error; err != nil {
			return err
		}
		return tx.Model(&storage.Channel{}).Where("id = ?", fixture.channel.ID).Updates(map[string]any{
			"deletion_state": storage.ChannelDeletionIntentManualReviewRequired, "updated_at": at,
		}).Error
	}); err != nil {
		t.Fatalf("stage retryable manual review: %v", err)
	}

	path := fmt.Sprintf("/api/channels/%d/deletion/claims/%d/requeue", fixture.channel.ID, fixture.claim.ID)
	body := fixture.actionBody("remote_inventory_rechecked")
	for attempt := 0; attempt < 2; attempt++ {
		rec := fixture.request(t, http.MethodPost, path, body)
		if rec.Code != http.StatusOK {
			t.Fatalf("requeue attempt %d status=%d body=%s", attempt+1, rec.Code, rec.Body.String())
		}
	}
	var requeued storage.ChannelDeletionKeyClaim
	if err := fixture.db.First(&requeued, fixture.claim.ID).Error; err != nil {
		t.Fatal(err)
	}
	if requeued.Status != storage.ChannelDeletionKeyPendingVerification || requeued.ClaimEpoch != 1 {
		t.Fatalf("requeued claim = %+v", requeued)
	}
	var evidence storage.ChannelDeletionKeyEvidence
	if err := fixture.db.Where("key_claim_id = ? AND event_kind = ?", fixture.claim.ID, "manual_review_requeued").First(&evidence).Error; err != nil {
		t.Fatal(err)
	}
	if evidence.Actor != "review-admin" || evidence.Reason != "remote_inventory_rechecked" {
		t.Fatalf("requeue evidence = %+v", evidence)
	}

	wrongFence := fixture.actionBody("remote_inventory_rechecked")
	wrongFence["expected_identity_revision"] = fmt.Sprint(fixture.intent.ChannelIdentityRevision + 1)
	rec := fixture.request(t, http.MethodPost, path, wrongFence)
	if rec.Code != http.StatusConflict {
		t.Fatalf("wrong identity fence status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRepeatedDeleteReturnsCurrentManualReviewState(t *testing.T) {
	fixture := newChannelDeletionReviewAPIFixture(t, storage.GatewayUpstreamKeyProvenanceUnknown)
	at := time.Now().UTC()
	if err := fixture.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&storage.ChannelDeletionIntent{}).Where("id = ?", fixture.intent.ID).Updates(map[string]any{
			"status": storage.ChannelDeletionIntentManualReviewRequired, "last_error": "ownership_unknown", "updated_at": at,
		}).Error; err != nil {
			return err
		}
		return tx.Model(&storage.Channel{}).Where("id = ?", fixture.channel.ID).Updates(map[string]any{
			"deletion_state": storage.ChannelDeletionIntentManualReviewRequired, "updated_at": at,
		}).Error
	}); err != nil {
		t.Fatal(err)
	}
	rec := fixture.request(t, http.MethodDelete, fmt.Sprintf("/api/channels/%d", fixture.channel.ID), nil)
	if rec.Code != http.StatusAccepted || !bytes.Contains(rec.Body.Bytes(), []byte(`"status":"manual_review_required"`)) {
		t.Fatalf("repeated delete status=%d body=%s", rec.Code, rec.Body.String())
	}
}
