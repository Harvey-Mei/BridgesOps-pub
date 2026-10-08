package storage

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChannelAPIKeyOperationsBeginIsAtomic(t *testing.T) {
	db := openTestDB(t)
	operations := NewChannelAPIKeyOperations(db)
	input := ChannelAPIKeyOperationStart{
		ChannelID:          7,
		Provider:           ChannelTypeNewAPI,
		ScopeHash:          strings.Repeat("a", 64),
		RequestFingerprint: strings.Repeat("b", 64),
		ManagedName:        "managed",
		GroupIdentity:      "default",
	}

	const workers = 16
	var createdCount atomic.Int32
	ids := make(chan uint64, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			operation, created, err := operations.Begin(input)
			if err != nil {
				errs <- err
				return
			}
			if created {
				createdCount.Add(1)
			}
			ids <- operation.ID
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		t.Fatalf("begin operation: %v", err)
	}
	if got := createdCount.Load(); got != 1 {
		t.Fatalf("created operations = %d, want 1", got)
	}
	var firstID uint64
	for id := range ids {
		if firstID == 0 {
			firstID = id
		}
		if id != firstID {
			t.Fatalf("operation ids differ: got %d and %d", firstID, id)
		}
	}
}

func TestChannelAPIKeyOperationsRetryOnlyExplicitRejectedPost(t *testing.T) {
	db := openTestDB(t)
	operations := NewChannelAPIKeyOperations(db)
	operation, created, err := operations.Begin(ChannelAPIKeyOperationStart{
		ChannelID:          10,
		Provider:           ChannelTypeNewAPI,
		ScopeHash:          strings.Repeat("3", 64),
		RequestFingerprint: strings.Repeat("4", 64),
		ManagedName:        "retry-managed",
		GroupIdentity:      "default",
	})
	if err != nil || !created {
		t.Fatalf("begin = created %v, err %v", created, err)
	}
	if _, _, err := operations.ClaimRejectedPostRetry(operation.ID, operation.OperationGeneration, time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := operations.RecordRejectedPost(operation.ID, operation.OperationGeneration, 504, time.Now()); !errors.Is(err, ErrChannelAPIKeyRejectedPostState) {
		t.Fatalf("commit-unknown status authorized retry: %v", err)
	}
	rejectedAt := time.Now().UTC().Add(-2 * time.Minute)
	operation, err = operations.RecordRejectedPost(operation.ID, operation.OperationGeneration, 429, rejectedAt)
	if err != nil || operation.RejectedPostStatus != 429 || operation.RejectedPostAt == nil {
		t.Fatalf("record rejected post = %#v, %v", operation, err)
	}
	if _, claimed, err := operations.ClaimRejectedPostRetry(operation.ID, operation.OperationGeneration, rejectedAt.Add(30*time.Second), time.Minute); err != nil || claimed {
		t.Fatalf("early rejected-post retry = claimed %v, err %v", claimed, err)
	}
	operation, claimed, err := operations.ClaimRejectedPostRetry(operation.ID, operation.OperationGeneration, rejectedAt.Add(time.Minute), time.Minute)
	if err != nil || !claimed || operation.RejectedRetryAt == nil || operation.RejectedRetryCount != 1 {
		t.Fatalf("due rejected-post retry = %#v, claimed %v, err %v", operation, claimed, err)
	}
	if _, claimed, err := operations.ClaimRejectedPostRetry(operation.ID, operation.OperationGeneration, rejectedAt.Add(2*time.Minute), time.Minute); err != nil || claimed {
		t.Fatalf("duplicate rejected-post retry = claimed %v, err %v", claimed, err)
	}
}

func TestChannelAPIKeyOperationsFailClosedOnFingerprintConflict(t *testing.T) {
	db := openTestDB(t)
	operations := NewChannelAPIKeyOperations(db)
	input := ChannelAPIKeyOperationStart{
		ChannelID:          8,
		Provider:           ChannelTypeSub2API,
		ScopeHash:          strings.Repeat("c", 64),
		RequestFingerprint: strings.Repeat("d", 64),
		ManagedName:        "managed",
		GroupIdentity:      "group-id:2",
	}
	if _, created, err := operations.Begin(input); err != nil || !created {
		t.Fatalf("first begin = created %v, err %v", created, err)
	}
	input.RequestFingerprint = strings.Repeat("e", 64)
	if _, created, err := operations.Begin(input); created || !errors.Is(err, ErrChannelAPIKeyOperationConflict) {
		t.Fatalf("conflicting begin = created %v, err %v", created, err)
	}
}

func TestChannelAPIKeyOperationsAllowsFencedLegacyNewAPIRecoveryFingerprint(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "legacy operation", Type: ChannelTypeNewAPI, SiteURL: "https://legacy-operation.example"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	if err := store.RecordUpstreamGroupChange(channel.ID, "default", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "default")
	if err != nil {
		t.Fatal(err)
	}
	fence := ChannelAPIKeyOperationRecoveryFence{
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         generation.ID, PresenceGeneration: generation.PresenceGeneration,
		CanonicalGroupName: generation.CanonicalGroupName,
	}
	operations := NewChannelAPIKeyOperations(db)
	legacyFingerprint := strings.Repeat("8", 64)
	input := ChannelAPIKeyOperationStart{
		ChannelID: channel.ID, Provider: ChannelTypeNewAPI,
		ScopeHash: strings.Repeat("6", 64), RequestFingerprint: legacyFingerprint,
		ManagedName: "BOG-p-0123456789abcdef", GroupIdentity: "default", RecoveryGroupName: "default", RecoveryFence: fence,
	}
	if _, created, err := operations.Begin(input); err != nil || !created {
		t.Fatalf("begin legacy operation = created %v, err %v", created, err)
	}
	input.RequestFingerprint = strings.Repeat("9", 64)
	input.LegacyRecoveryFingerprints = []string{legacyFingerprint}
	if _, created, err := operations.Begin(input); err != nil || created {
		t.Fatalf("resume fenced legacy operation = created %v, err %v", created, err)
	}
	input.RecoveryFence.ChannelIdentityRevision++
	if _, created, err := operations.Begin(input); created || !errors.Is(err, ErrChannelAPIKeyRecoveryFence) {
		t.Fatalf("identity-drifted legacy recovery = created %v, err %v", created, err)
	}
}

func TestChannelAPIKeyOperationsRotatesLegacyFingerprintIntoCurrentGeneration(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "legacy rotation", Type: ChannelTypeNewAPI, SiteURL: "https://legacy-rotation.example"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	if err := store.RecordUpstreamGroupChange(channel.ID, "default", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "default")
	if err != nil {
		t.Fatal(err)
	}
	fence := ChannelAPIKeyOperationRecoveryFence{
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         generation.ID,
		PresenceGeneration:      generation.PresenceGeneration,
		CanonicalGroupName:      generation.CanonicalGroupName,
	}
	operations := NewChannelAPIKeyOperations(db)
	legacyFingerprint := strings.Repeat("a", 64)
	currentFingerprint := strings.Repeat("b", 64)
	operation, created, err := operations.Begin(ChannelAPIKeyOperationStart{
		ChannelID: channel.ID, Provider: ChannelTypeNewAPI,
		ScopeHash: strings.Repeat("c", 64), RequestFingerprint: legacyFingerprint,
		ManagedName: "BOG-p-0123456789abcdef", GroupIdentity: "default", RecoveryGroupName: "default", RecoveryFence: fence,
	})
	if err != nil || !created {
		t.Fatalf("begin legacy operation = %#v created=%v err=%v", operation, created, err)
	}
	firstAt := time.Now().UTC().Add(-time.Minute)
	observed, rotated, err := operations.ConfirmAbsenceAndRotateWithRequestFingerprint(
		operation.ID, operation.OperationGeneration, fence, firstAt, time.Minute, 2, currentFingerprint,
	)
	if err != nil || rotated || observed.RequestFingerprint != legacyFingerprint || observed.AbsenceConfirmationCount != 1 {
		t.Fatalf("first legacy absence = %#v rotated=%v err=%v", observed, rotated, err)
	}
	next, rotated, err := operations.ConfirmAbsenceAndRotateWithRequestFingerprint(
		operation.ID, operation.OperationGeneration, fence, firstAt.Add(time.Minute), time.Minute, 2, currentFingerprint,
	)
	if err != nil || !rotated || next.OperationGeneration != 2 || next.RequestFingerprint != currentFingerprint {
		t.Fatalf("rotate legacy operation = %#v rotated=%v err=%v", next, rotated, err)
	}
	var history ChannelAPIKeyOperationGeneration
	if err := db.Where("operation_id = ? AND operation_generation = ?", operation.ID, 1).First(&history).Error; err != nil {
		t.Fatal(err)
	}
	if history.RequestFingerprint != legacyFingerprint || history.SupersededByGeneration != 2 {
		t.Fatalf("legacy history = %#v", history)
	}
	if _, rotated, err := operations.ConfirmAbsenceAndRotateWithRequestFingerprint(
		next.ID, next.OperationGeneration, fence, firstAt.Add(2*time.Minute), time.Minute, 2, "bad",
	); rotated || !errors.Is(err, ErrChannelAPIKeyAbsenceState) {
		t.Fatalf("invalid replacement fingerprint = rotated=%v err=%v", rotated, err)
	}
}

func TestChannelAPIKeyOperationsBindAndCompleteWithoutPlaintextKey(t *testing.T) {
	db := openTestDB(t)
	operations := NewChannelAPIKeyOperations(db)
	operation, created, err := operations.Begin(ChannelAPIKeyOperationStart{
		ChannelID:          9,
		Provider:           ChannelTypeNewAPI,
		ScopeHash:          strings.Repeat("f", 64),
		RequestFingerprint: strings.Repeat("1", 64),
		ManagedName:        "managed",
		GroupIdentity:      "default",
		CustomKeyHash:      strings.Repeat("2", 64),
	})
	if err != nil || !created {
		t.Fatalf("begin = created %v, err %v", created, err)
	}
	operation, err = operations.BindRemoteID(operation.ID, operation.OperationGeneration, 42)
	if err != nil || operation.RemoteID == nil || *operation.RemoteID != 42 {
		t.Fatalf("bind remote id = %#v, err %v", operation, err)
	}
	operation, err = operations.Complete(operation.ID, operation.OperationGeneration, 42)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if operation.Status != ChannelAPIKeyOperationCompleted || operation.CompletedAt == nil {
		t.Fatalf("completed operation = %#v", operation)
	}

	columns, err := db.Migrator().ColumnTypes(&ChannelAPIKeyOperation{})
	if err != nil {
		t.Fatalf("operation columns: %v", err)
	}
	for _, column := range columns {
		if column.Name() == "key" || column.Name() == "api_key" {
			t.Fatalf("plaintext key column %q must not exist", column.Name())
		}
	}
}

func TestChannelAPIKeyOperationsRotateAfterSeparatedFencedAbsenceConfirmations(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "operation rotation", Type: ChannelTypeNewAPI, SiteURL: "https://operation-rotation.example"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	if err := store.RecordUpstreamGroupChange(channel.ID, "default", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "default")
	if err != nil {
		t.Fatal(err)
	}
	fence := ChannelAPIKeyOperationRecoveryFence{
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         generation.ID,
		PresenceGeneration:      generation.PresenceGeneration,
		CanonicalGroupName:      generation.CanonicalGroupName,
	}
	operations := NewChannelAPIKeyOperations(db)
	input := ChannelAPIKeyOperationStart{
		ChannelID: channel.ID, Provider: ChannelTypeNewAPI,
		ScopeHash: strings.Repeat("6", 64), RequestFingerprint: strings.Repeat("7", 64),
		ManagedName: "managed-rotation", GroupIdentity: "default", RecoveryFence: fence,
	}
	operation, created, err := operations.Begin(input)
	if err != nil || !created || operation.OperationGeneration != 1 {
		t.Fatalf("begin rotation operation = %#v created=%v err=%v", operation, created, err)
	}

	firstAt := time.Now().UTC()
	observed, rotated, err := operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, firstAt, 10*time.Minute, 2)
	if err != nil || rotated || observed.AbsenceConfirmationCount != 1 || observed.AbsenceFirstConfirmedAt == nil {
		t.Fatalf("first absence confirmation = %#v rotated=%v err=%v", observed, rotated, err)
	}
	observed, rotated, err = operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, firstAt.Add(9*time.Minute), 10*time.Minute, 2)
	if err != nil || rotated || observed.AbsenceConfirmationCount != 1 {
		t.Fatalf("early duplicate absence confirmation = %#v rotated=%v err=%v", observed, rotated, err)
	}
	next, rotated, err := operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, firstAt.Add(10*time.Minute), 10*time.Minute, 2)
	if err != nil || !rotated || next.ID != operation.ID || next.OperationGeneration != 2 || next.AbsenceConfirmationCount != 0 ||
		next.AbsenceFirstConfirmedAt != nil || next.AbsenceLastConfirmedAt != nil {
		t.Fatalf("rotated operation = %#v rotated=%v err=%v", next, rotated, err)
	}
	var superseded ChannelAPIKeyOperationGeneration
	if err := db.Where("operation_id = ? AND operation_generation = ?", operation.ID, 1).First(&superseded).Error; err != nil {
		t.Fatal(err)
	}
	if superseded.Status != ChannelAPIKeyOperationSuperseded || superseded.SupersededByGeneration != 2 ||
		superseded.SupersededAt.IsZero() || superseded.AbsenceConfirmationCount != 2 {
		t.Fatalf("superseded operation = %#v", superseded)
	}
	current, created, err := operations.Begin(input)
	if err != nil || created || current.ID != next.ID {
		t.Fatalf("begin after rotation = %#v created=%v err=%v", current, created, err)
	}
}

func TestChannelAPIKeyOperationsRejectRecoveryFenceForAnotherRequestGroup(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "operation wrong group fence", Type: ChannelTypeNewAPI, SiteURL: "https://operation-wrong-group.example"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	for _, groupName := range []string{"default", "other"} {
		if err := store.RecordUpstreamGroupChange(channel.ID, groupName, "added", 0, "", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	other, err := store.CaptureUpstreamGroupGeneration(channel.ID, "other")
	if err != nil {
		t.Fatal(err)
	}
	fence := ChannelAPIKeyOperationRecoveryFence{
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         other.ID, PresenceGeneration: other.PresenceGeneration,
		CanonicalGroupName: other.CanonicalGroupName,
	}
	_, created, err := NewChannelAPIKeyOperations(db).Begin(ChannelAPIKeyOperationStart{
		ChannelID: channel.ID, Provider: ChannelTypeNewAPI,
		ScopeHash: strings.Repeat("0", 64), RequestFingerprint: strings.Repeat("1", 64),
		ManagedName: "managed-wrong-group", GroupIdentity: "default", RecoveryGroupName: "default", RecoveryFence: fence,
	})
	if created || !errors.Is(err, ErrChannelAPIKeyRecoveryFence) {
		t.Fatalf("wrong request-group fence = created %v, err %v", created, err)
	}
}

func TestChannelAPIKeyOperationsResetAbsenceConfirmationsWhenFenceChanges(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "operation refence", Type: ChannelTypeNewAPI, SiteURL: "https://operation-refence.example"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	if err := store.RecordUpstreamGroupChange(channel.ID, "default", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "default")
	if err != nil {
		t.Fatal(err)
	}
	fence := ChannelAPIKeyOperationRecoveryFence{
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         generation.ID, PresenceGeneration: generation.PresenceGeneration,
		CanonicalGroupName: generation.CanonicalGroupName,
	}
	operations := NewChannelAPIKeyOperations(db)
	operation, _, err := operations.Begin(ChannelAPIKeyOperationStart{
		ChannelID: channel.ID, Provider: ChannelTypeNewAPI,
		ScopeHash: strings.Repeat("8", 64), RequestFingerprint: strings.Repeat("9", 64),
		ManagedName: "managed-refence", GroupIdentity: "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	firstAt := time.Now().UTC()
	if observed, rotated, err := operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, firstAt, time.Minute, 2); err != nil || rotated || observed.AbsenceConfirmationCount != 1 {
		t.Fatalf("legacy operation baseline = %#v rotated=%v err=%v", observed, rotated, err)
	}
	if err := db.Model(&GatewayUpstreamGroup{}).Where("id = ?", generation.ID).
		Update("presence_generation", generation.PresenceGeneration+1).Error; err != nil {
		t.Fatal(err)
	}
	fence.PresenceGeneration++
	observed, rotated, err := operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, firstAt.Add(time.Minute), time.Minute, 2)
	if err != nil || rotated || observed.AbsenceConfirmationCount != 1 || observed.PresenceGeneration != fence.PresenceGeneration {
		t.Fatalf("changed-fence baseline = %#v rotated=%v err=%v", observed, rotated, err)
	}
	next, rotated, err := operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, firstAt.Add(2*time.Minute), time.Minute, 2)
	if err != nil || !rotated || next.OperationGeneration != 2 {
		t.Fatalf("changed-fence rotation = %#v rotated=%v err=%v", next, rotated, err)
	}
}

func TestChannelAPIKeyOperationsDoNotRotateExplicitRejectedPost(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "operation rejection", Type: ChannelTypeNewAPI, SiteURL: "https://operation-rejection.example"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	if err := store.RecordUpstreamGroupChange(channel.ID, "default", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "default")
	if err != nil {
		t.Fatal(err)
	}
	fence := ChannelAPIKeyOperationRecoveryFence{
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         generation.ID, PresenceGeneration: generation.PresenceGeneration,
		CanonicalGroupName: generation.CanonicalGroupName,
	}
	operations := NewChannelAPIKeyOperations(db)
	operation, _, err := operations.Begin(ChannelAPIKeyOperationStart{
		ChannelID: channel.ID, Provider: ChannelTypeNewAPI,
		ScopeHash: strings.Repeat("a", 64), RequestFingerprint: strings.Repeat("b", 64),
		ManagedName: "managed-rejection", GroupIdentity: "default", RecoveryFence: fence,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operations.RecordRejectedPost(operation.ID, operation.OperationGeneration, 429, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, rotated, err := operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, time.Now().Add(time.Hour), time.Minute, 2); rotated || !errors.Is(err, ErrChannelAPIKeyAbsenceState) {
		t.Fatalf("explicit rejection absence rotation = rotated=%v err=%v", rotated, err)
	}
}

func TestChannelAPIKeyOperationWritersRejectSupersededGeneration(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{Name: "operation stale writers", Type: ChannelTypeNewAPI, SiteURL: "https://operation-stale-writers.example"}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	if err := store.RecordUpstreamGroupChange(channel.ID, "default", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "default")
	if err != nil {
		t.Fatal(err)
	}
	fence := ChannelAPIKeyOperationRecoveryFence{
		ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		UpstreamGroupID:         generation.ID,
		PresenceGeneration:      generation.PresenceGeneration,
		CanonicalGroupName:      generation.CanonicalGroupName,
	}
	operations := NewChannelAPIKeyOperations(db)
	operation, _, err := operations.Begin(ChannelAPIKeyOperationStart{
		ChannelID: channel.ID, Provider: ChannelTypeNewAPI,
		ScopeHash: strings.Repeat("c", 64), RequestFingerprint: strings.Repeat("d", 64),
		ManagedName: "managed-stale-writers", GroupIdentity: "default", RecoveryFence: fence,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstAt := time.Now().UTC()
	if _, rotated, err := operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, firstAt, time.Minute, 2); err != nil || rotated {
		t.Fatalf("first absence confirmation = rotated %v, err %v", rotated, err)
	}
	current, rotated, err := operations.ConfirmAbsenceAndRotate(operation.ID, operation.OperationGeneration, fence, firstAt.Add(time.Minute), time.Minute, 2)
	if err != nil || !rotated || current.OperationGeneration != 2 {
		t.Fatalf("rotate current generation = %#v, rotated %v, err %v", current, rotated, err)
	}

	if _, err := operations.BindRemoteID(operation.ID, operation.OperationGeneration, 42); !errors.Is(err, ErrChannelAPIKeyOperationGeneration) {
		t.Fatalf("stale bind remote id error = %v", err)
	}
	if _, err := operations.RecordRejectedPost(operation.ID, operation.OperationGeneration, 429, time.Now()); !errors.Is(err, ErrChannelAPIKeyOperationGeneration) {
		t.Fatalf("stale rejected post error = %v", err)
	}

	rejectedAt := time.Now().UTC().Add(-2 * time.Minute)
	current, err = operations.RecordRejectedPost(current.ID, current.OperationGeneration, 429, rejectedAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := operations.ClaimRejectedPostRetry(current.ID, operation.OperationGeneration, rejectedAt.Add(time.Minute), time.Minute); !errors.Is(err, ErrChannelAPIKeyOperationGeneration) {
		t.Fatalf("stale rejected retry claim error = %v", err)
	}
	current, claimed, err := operations.ClaimRejectedPostRetry(current.ID, current.OperationGeneration, rejectedAt.Add(time.Minute), time.Minute)
	if err != nil || !claimed || current.RejectedPostStatus != 429 || current.RejectedPostAt == nil || current.RejectedRetryAt == nil {
		t.Fatalf("current rejected retry claim = %#v, claimed %v, err %v", current, claimed, err)
	}

	secondAt := firstAt.Add(2 * time.Minute)
	if _, rotated, err := operations.ConfirmAbsenceAndRotate(current.ID, current.OperationGeneration, fence, secondAt, time.Minute, 2); err != nil || rotated {
		t.Fatalf("retry commit-unknown first absence = rotated %v, err %v", rotated, err)
	}
	next, rotated, err := operations.ConfirmAbsenceAndRotate(current.ID, current.OperationGeneration, fence, secondAt.Add(time.Minute), time.Minute, 2)
	if err != nil || !rotated || next.OperationGeneration != 3 {
		t.Fatalf("retry commit-unknown rotation = %#v, rotated %v, err %v", next, rotated, err)
	}
	var retryHistory ChannelAPIKeyOperationGeneration
	if err := db.Where("operation_id = ? AND operation_generation = ?", current.ID, current.OperationGeneration).First(&retryHistory).Error; err != nil {
		t.Fatal(err)
	}
	if retryHistory.RejectedPostStatus != 429 || retryHistory.RejectedPostAt == nil ||
		retryHistory.RejectedRetryAt == nil || retryHistory.RejectedRetryCount != 1 {
		t.Fatalf("archived rejected retry evidence = %#v", retryHistory)
	}
	next, err = operations.BindRemoteID(next.ID, next.OperationGeneration, 43)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operations.Complete(next.ID, current.OperationGeneration, 43); !errors.Is(err, ErrChannelAPIKeyOperationGeneration) {
		t.Fatalf("stale completion error = %v", err)
	}
	stored, err := operations.Find(channel.ID, ChannelTypeNewAPI, strings.Repeat("c", 64))
	if err != nil || stored.Status != ChannelAPIKeyOperationPending || stored.OperationGeneration != 3 {
		t.Fatalf("current operation after stale writes = %#v, err %v", stored, err)
	}
}
