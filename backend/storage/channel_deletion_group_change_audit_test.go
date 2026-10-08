package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestFinalizeChannelDeletionRetainsAndClosesGroupChangeAuditHistory(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "deletion-group-change-audit", Type: ChannelTypeNewAPI,
		SiteURL: "https://deletion-group-change-audit.example", PasswordCipher: "cipher",
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}

	detectedAt := time.Date(2026, 7, 28, 1, 0, 0, 0, time.UTC)
	acknowledgedAt := detectedAt.Add(time.Minute)
	acknowledged := GatewayUpstreamGroupChange{
		ChannelID: channel.ID, GroupName: "acknowledged-group", Kind: "removed", Status: "acknowledged",
		UpstreamKeyID: 41, UpstreamKeyName: "retained-key-name",
		AssignmentState: "ignored", AssignmentReasonCode: "remote_key_deleted",
		DetectedAt: detectedAt, ResolvedAt: &acknowledgedAt,
		CreatedAt: detectedAt, UpdatedAt: acknowledgedAt,
	}
	pending := GatewayUpstreamGroupChange{
		ChannelID: channel.ID, GroupName: "pending-group", Kind: "added", Status: "pending",
		AssignmentState: "awaiting_facts", AssignmentReasonCode: "models_not_synced_for_generation",
		DetectedAt: detectedAt.Add(2 * time.Minute),
	}
	processing := GatewayUpstreamGroupChange{
		ChannelID: channel.ID, GroupName: "processing-group", Kind: "removed", Status: "processing",
		UpstreamKeyID: 42, UpstreamKeyName: "processing-key",
		DetectedAt: detectedAt.Add(3 * time.Minute),
	}
	for _, change := range []*GatewayUpstreamGroupChange{&acknowledged, &pending, &processing} {
		if err := db.Create(change).Error; err != nil {
			t.Fatalf("create %s change: %v", change.Status, err)
		}
	}
	gateway := NewGateway(db)
	if err := gateway.RecordUpstreamGroupChange(
		channel.ID, "linked-group", "added", 43, "linked-key", detectedAt.Add(4*time.Minute),
	); err != nil {
		t.Fatalf("create group-linked change: %v", err)
	}
	var linked GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ?", channel.ID, "linked-group").First(&linked).Error; err != nil {
		t.Fatalf("read group-linked change: %v", err)
	}
	if linked.UpstreamGroupID == nil || *linked.UpstreamGroupID == 0 || linked.PresenceGeneration == nil {
		t.Fatalf("group-linked change did not start with a live group reference: %+v", linked)
	}
	linkedGroupID := *linked.UpstreamGroupID

	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion: %v", err)
	}
	if err := gateway.ResolveUpstreamGroupChange(
		pending.ID, "acknowledged", detectedAt.Add(5*time.Minute),
	); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("late group-change resolution error = %v, want ErrChannelDeletionPending", err)
	}
	var pendingAfterPrepare GatewayUpstreamGroupChange
	if err := db.First(&pendingAfterPrepare, pending.ID).Error; err != nil {
		t.Fatalf("read pending change after late resolution: %v", err)
	}
	if pendingAfterPrepare.Status != "pending" || pendingAfterPrepare.ResolvedAt != nil {
		t.Fatalf("late resolution changed deletion-fenced audit: %+v", pendingAfterPrepare)
	}
	prepared, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("read prepared channel: %v", err)
	}
	var intent ChannelDeletionIntent
	if err := db.Where("channel_id = ? AND deletion_epoch = ?", channel.ID, prepared.DeletionEpoch).First(&intent).Error; err != nil {
		t.Fatalf("read deletion intent: %v", err)
	}
	fence := ChannelDeletionFence{
		IntentID: intent.ID, ChannelID: channel.ID,
		DeletionEpoch: prepared.DeletionEpoch, IdentityRevision: prepared.DeletionIdentityRevision,
	}
	finalizedAt := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)
	if err := channels.MarkChannelDeletionIntentState(
		context.Background(), fence, ChannelDeletionIntentReadyForFinalization, "", nil, finalizedAt,
	); err != nil {
		t.Fatalf("mark deletion ready: %v", err)
	}
	if err := channels.FinalizeChannelDeletion(context.Background(), fence, "group-change-audit-test", finalizedAt); err != nil {
		t.Fatalf("finalize channel deletion: %v", err)
	}
	if err := db.First(&Channel{}, channel.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted channel lookup error = %v, want record not found", err)
	}

	var retained []GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&retained).Error; err != nil {
		t.Fatalf("read retained group-change history: %v", err)
	}
	if len(retained) != 4 {
		t.Fatalf("retained group-change rows = %d, want 4: %+v", len(retained), retained)
	}
	byID := make(map[uint]GatewayUpstreamGroupChange, len(retained))
	for _, change := range retained {
		byID[change.ID] = change
	}
	gotAcknowledged := byID[acknowledged.ID]
	if gotAcknowledged.Status != acknowledged.Status || gotAcknowledged.AssignmentState != acknowledged.AssignmentState ||
		gotAcknowledged.AssignmentReasonCode != acknowledged.AssignmentReasonCode ||
		gotAcknowledged.GroupName != acknowledged.GroupName || gotAcknowledged.UpstreamKeyID != acknowledged.UpstreamKeyID ||
		gotAcknowledged.UpstreamKeyName != acknowledged.UpstreamKeyName || gotAcknowledged.ResolvedAt == nil ||
		!gotAcknowledged.ResolvedAt.Equal(acknowledgedAt) || !gotAcknowledged.UpdatedAt.Equal(acknowledgedAt) {
		t.Fatalf("acknowledged audit changed during deletion: %+v", gotAcknowledged)
	}
	for _, original := range []GatewayUpstreamGroupChange{pending, processing} {
		got := byID[original.ID]
		if got.Status != "superseded" || got.AssignmentState != "superseded" ||
			got.AssignmentReasonCode != "channel_deleted" || got.ResolvedAt == nil || got.EvaluatedAt == nil ||
			!got.ResolvedAt.Equal(finalizedAt) || !got.EvaluatedAt.Equal(finalizedAt) || !got.UpdatedAt.Equal(finalizedAt) {
			t.Fatalf("unfinished audit was not closed at finalization: %+v", got)
		}
	}
	gotLinked := byID[linked.ID]
	if gotLinked.Status != "superseded" || gotLinked.AssignmentState != "superseded" ||
		gotLinked.AssignmentReasonCode != "channel_deleted" || gotLinked.UpstreamGroupID != nil ||
		gotLinked.PresenceGeneration == nil || *gotLinked.PresenceGeneration != *linked.PresenceGeneration ||
		gotLinked.GroupName != linked.GroupName || gotLinked.UpstreamKeyID != linked.UpstreamKeyID ||
		gotLinked.UpstreamKeyName != linked.UpstreamKeyName || gotLinked.ResolvedAt == nil || gotLinked.EvaluatedAt == nil ||
		!gotLinked.ResolvedAt.Equal(finalizedAt) || !gotLinked.EvaluatedAt.Equal(finalizedAt) || !gotLinked.UpdatedAt.Equal(finalizedAt) {
		t.Fatalf("group-linked audit was not retained after its live group was deleted: %+v", gotLinked)
	}
	var linkedGroupCount int64
	if err := db.Model(&GatewayUpstreamGroup{}).Where("id = ?", linkedGroupID).Count(&linkedGroupCount).Error; err != nil {
		t.Fatalf("count deleted linked group: %v", err)
	}
	if linkedGroupCount != 0 {
		t.Fatalf("linked live group count after channel deletion = %d, want 0", linkedGroupCount)
	}

	var finalAuditBeforeReplay ChannelDeletionFinalAudit
	if err := db.Where("intent_id = ?", intent.ID).First(&finalAuditBeforeReplay).Error; err != nil {
		t.Fatalf("read final audit before replay: %v", err)
	}
	var intentBeforeReplay ChannelDeletionIntent
	if err := db.First(&intentBeforeReplay, intent.ID).Error; err != nil {
		t.Fatalf("read deletion intent before replay: %v", err)
	}
	if err := channels.FinalizeChannelDeletion(
		context.Background(), fence, "different-replay-actor", finalizedAt.Add(time.Hour),
	); err != nil {
		t.Fatalf("repeat finalization: %v", err)
	}
	var retainedAfterReplay []GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&retainedAfterReplay).Error; err != nil {
		t.Fatalf("read retained history after replay: %v", err)
	}
	if !reflect.DeepEqual(retainedAfterReplay, retained) {
		t.Fatalf("replay changed retained group-change history: before=%+v after=%+v", retained, retainedAfterReplay)
	}
	var finalAuditAfterReplay ChannelDeletionFinalAudit
	if err := db.Where("intent_id = ?", intent.ID).First(&finalAuditAfterReplay).Error; err != nil {
		t.Fatalf("read final audit after replay: %v", err)
	}
	var intentAfterReplay ChannelDeletionIntent
	if err := db.First(&intentAfterReplay, intent.ID).Error; err != nil {
		t.Fatalf("read deletion intent after replay: %v", err)
	}
	if !reflect.DeepEqual(finalAuditAfterReplay, finalAuditBeforeReplay) || !reflect.DeepEqual(intentAfterReplay, intentBeforeReplay) {
		t.Fatalf("replay changed deletion tombstones: audit_before=%+v audit_after=%+v intent_before=%+v intent_after=%+v", finalAuditBeforeReplay, finalAuditAfterReplay, intentBeforeReplay, intentAfterReplay)
	}
	var finalAuditCount int64
	if err := db.Model(&ChannelDeletionFinalAudit{}).Where("intent_id = ?", intent.ID).Count(&finalAuditCount).Error; err != nil {
		t.Fatalf("count final audits after replay: %v", err)
	}
	if finalAuditCount != 1 {
		t.Fatalf("final audit count after replay = %d, want 1", finalAuditCount)
	}
	if err := gateway.markManagedUpstreamKeyRemovalManualReviewByID(
		processing.ID, "stale_worker_review", finalizedAt.Add(time.Minute),
	); err == nil {
		t.Fatal("stale managed-key worker rewrote a finalized deletion audit")
	}
	var processingAfterStaleWorker GatewayUpstreamGroupChange
	if err := db.First(&processingAfterStaleWorker, processing.ID).Error; err != nil {
		t.Fatalf("read processing audit after stale worker: %v", err)
	}
	if processingAfterStaleWorker.Status != "superseded" ||
		processingAfterStaleWorker.AssignmentReasonCode != "channel_deleted" ||
		processingAfterStaleWorker.ResolvedAt == nil || !processingAfterStaleWorker.ResolvedAt.Equal(finalizedAt) {
		t.Fatalf("stale worker changed finalized deletion audit: %+v", processingAfterStaleWorker)
	}
}
