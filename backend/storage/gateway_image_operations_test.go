package storage

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func setupGatewayImageCapabilityTest(t *testing.T) (*Gateway, UpstreamGroupGeneration) {
	t.Helper()
	db := openTestDB(t)
	channel := Channel{Name: "image-capability", Type: ChannelTypeNewAPI, SiteURL: "https://image.example", PasswordCipher: "test", MonitorEnabled: true}
	if err := db.Create(&channel).Error; err != nil {
		t.Fatalf("create channel: %v", err)
	}
	now := time.Now().UTC()
	if _, err := NewRates(db).Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "Primary", Ratio: 1, LastSeenAt: now}); err != nil {
		t.Fatalf("create rate: %v", err)
	}
	store := NewGateway(db)
	if err := store.RecordUpstreamGroupChange(channel.ID, "Primary", "added", 11, "BOG test", now); err != nil {
		t.Fatalf("record group: %v", err)
	}
	for _, id := range []uint{41, 42, 51, 52} {
		if err := db.Create(&GatewayRequestLog{ID: id, ChannelID: channel.ID, GroupName: "primary", ModelName: "gpt-image-test", Method: "POST", Path: "/v1/images/generations", StatusCode: 200, RouteReason: "smart_primary", CreatedAt: now}).Error; err != nil {
			t.Fatalf("create request log %d: %v", id, err)
		}
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture generation: %v", err)
	}
	return store, generation
}

func configuredAsyncImageCapability(generation UpstreamGroupGeneration) GatewayImageOperationCapability {
	return GatewayImageOperationCapability{
		ChannelID: generation.ChannelID, ModelName: "gpt-image-test", Operation: GatewayImageOperationGeneration,
		Protocol: GatewayImageProtocolImages, EndpointPath: "/v1/images/generations", TaskMode: GatewayImageTaskModeAsync,
		IdempotencyKeySupported: true, IdempotencyHeader: "Idempotency-Key",
		TaskIDReturned: true, TaskIDJSONPath: "task.id", TaskQuerySupported: true, TaskQueryMethod: "GET",
		TaskQueryPathTemplate: "/v1/tasks/{task_id}", TaskStatusJSONPath: "task.status",
		TaskSuccessValues: []string{"completed", "succeeded"}, TaskFailureValues: []string{"failed", "cancelled"},
	}
}

func TestGatewayImageCapabilityConfigurationIsAuditedAndFailClosed(t *testing.T) {
	store, generation := setupGatewayImageCapabilityTest(t)
	capability, err := store.SaveImageOperationCapabilityConfig(configuredAsyncImageCapability(generation), generation, 0, "administrator")
	if err != nil {
		t.Fatalf("save capability: %v", err)
	}
	if capability.TaskMode != GatewayImageTaskModeAsync || !capability.IdempotencyKeySupported || !capability.TaskQuerySupported {
		t.Fatalf("saved capability = %+v", capability)
	}
	if capability.RecoveryComplete || capability.AutomaticReplayAllowed || !containsString(capability.RecoveryBlockers, "gateway_task_recovery_not_implemented") {
		t.Fatalf("configured async capability did not fail closed: %+v", capability)
	}
	current, err := store.FindCurrentImageOperationCapability(generation.ChannelID, "PRIMARY", "gpt-image-test", GatewayImageProtocolImages)
	if err != nil || current == nil || current.EvidenceSource != GatewayImageEvidenceAdminConfig || current.EvidenceObservedAt.IsZero() {
		t.Fatalf("current capability = %+v, %v", current, err)
	}
	audits, err := store.ListImageOperationCapabilityAudits(capability.ID)
	if err != nil || len(audits) != 1 || audits[0].Action != "configured" || audits[0].Actor != "administrator" {
		t.Fatalf("capability audits = %+v, %v", audits, err)
	}
	raw, err := json.Marshal(audits[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"api_key", "credential", "raw_body", "task-secret", "https://image.example"} {
		if containsFold(string(raw), forbidden) {
			t.Fatalf("audit exposed %q: %s", forbidden, raw)
		}
	}
}

func TestGatewayImageCapabilityEvidencePrecedence(t *testing.T) {
	store, generation := setupGatewayImageCapabilityTest(t)
	observed := GatewayImageOperationCapability{
		ChannelID: generation.ChannelID, ModelName: "gpt-image-test", Operation: GatewayImageOperationGeneration,
		Protocol: GatewayImageProtocolImages, EndpointPath: "/v1/images/generations", TaskMode: GatewayImageTaskModeSync,
		EvidenceDetail: "observed_sync_response", EvidenceObservedAt: time.Now().UTC(),
	}
	first, err := store.ObserveImageOperationCapability(observed, generation, 41)
	if err != nil || first.EvidenceSource != GatewayImageEvidenceObservedSuccess {
		t.Fatalf("observe capability = %+v, %v", first, err)
	}
	provider := configuredAsyncImageCapability(generation)
	provider.EvidenceObservedAt = time.Now().UTC().Add(time.Second)
	second, err := store.RecordProviderImageOperationCapability(provider, generation, "metadata_sync")
	if err != nil || second.EvidenceSource != GatewayImageEvidenceProviderMetadata || second.TaskMode != GatewayImageTaskModeAsync {
		t.Fatalf("provider capability = %+v, %v", second, err)
	}
	ignored, err := store.ObserveImageOperationCapability(observed, generation, 42)
	if err != nil || ignored.EvidenceSource != GatewayImageEvidenceProviderMetadata || ignored.TaskMode != GatewayImageTaskModeAsync {
		t.Fatalf("lower-priority observation replaced provider metadata: %+v, %v", ignored, err)
	}
	configured, err := store.SaveImageOperationCapabilityConfig(configuredAsyncImageCapability(generation), generation, ignored.CapabilityVersion, "administrator")
	if err != nil || configured.EvidenceSource != GatewayImageEvidenceAdminConfig {
		t.Fatalf("admin capability = %+v, %v", configured, err)
	}
	after, err := store.RecordProviderImageOperationCapability(provider, generation, "metadata_sync")
	if err != nil || after.EvidenceSource != GatewayImageEvidenceAdminConfig {
		t.Fatalf("provider metadata replaced administrator config: %+v, %v", after, err)
	}
	audits, err := store.ListImageOperationCapabilityAudits(configured.ID)
	if err != nil || len(audits) != 5 {
		t.Fatalf("evidence audits = %d, %v", len(audits), err)
	}
	ignoredCount := 0
	for _, audit := range audits {
		if audit.Action == "evidence_ignored" {
			ignoredCount++
		}
	}
	if ignoredCount != 2 {
		t.Fatalf("ignored evidence audits = %d, want 2", ignoredCount)
	}
}

func TestGatewayImageCapabilityCombinesObservedSyncAndAsyncModes(t *testing.T) {
	store, generation := setupGatewayImageCapabilityTest(t)
	now := time.Now().UTC()
	syncCapability := GatewayImageOperationCapability{
		ChannelID: generation.ChannelID, ModelName: "gpt-image-test", Operation: GatewayImageOperationGeneration,
		Protocol: GatewayImageProtocolImages, EndpointPath: "/v1/images/generations", TaskMode: GatewayImageTaskModeSync,
		EvidenceDetail: "observed_sync_response", EvidenceObservedAt: now,
	}
	if _, err := store.ObserveImageOperationCapability(syncCapability, generation, 51); err != nil {
		t.Fatal(err)
	}
	asyncCapability := GatewayImageOperationCapability{
		ChannelID: generation.ChannelID, ModelName: "gpt-image-test", Operation: GatewayImageOperationGeneration,
		Protocol: GatewayImageProtocolImages, EndpointPath: "/v1/images/generations", TaskMode: GatewayImageTaskModeAsync,
		TaskIDReturned: true, TaskIDJSONPath: "task.id", EvidenceDetail: "observed_async_task", EvidenceObservedAt: now.Add(time.Second),
	}
	combined, err := store.ObserveImageOperationCapability(asyncCapability, generation, 52)
	if err != nil {
		t.Fatal(err)
	}
	if combined.TaskMode != GatewayImageTaskModeSyncAsync || !combined.TaskIDReturned || combined.TaskIDJSONPath != "task.id" {
		t.Fatalf("combined capability = %+v", combined)
	}
	if combined.RecoveryComplete || combined.AutomaticReplayAllowed || !containsString(combined.RecoveryBlockers, "gateway_task_recovery_not_implemented") {
		t.Fatalf("combined capability did not remain fail closed: %+v", combined)
	}
}

func TestGatewayImageCapabilityIgnoresOlderSameSourceEvidence(t *testing.T) {
	store, generation := setupGatewayImageCapabilityTest(t)
	newer := configuredAsyncImageCapability(generation)
	newer.EvidenceObservedAt = time.Now().UTC()
	saved, err := store.RecordProviderImageOperationCapability(newer, generation, "metadata_sync")
	if err != nil {
		t.Fatal(err)
	}
	older := newer
	older.TaskMode = GatewayImageTaskModeSync
	older.TaskIDReturned = false
	older.TaskIDJSONPath = ""
	older.EvidenceObservedAt = newer.EvidenceObservedAt.Add(-time.Hour)
	result, err := store.RecordProviderImageOperationCapability(older, generation, "metadata_sync")
	if err != nil {
		t.Fatal(err)
	}
	if result.CapabilityVersion != saved.CapabilityVersion || result.TaskMode != GatewayImageTaskModeAsync {
		t.Fatalf("older provider evidence replaced current capability: before=%+v after=%+v", saved, result)
	}
	audits, err := store.ListImageOperationCapabilityAudits(saved.ID)
	if err != nil || len(audits) != 2 || audits[0].Action != "evidence_ignored" {
		t.Fatalf("older evidence audit = %+v, %v", audits, err)
	}
}

func TestGatewayImageCapabilityConfigurationUsesOptimisticConcurrency(t *testing.T) {
	store, generation := setupGatewayImageCapabilityTest(t)
	initial := configuredAsyncImageCapability(generation)
	initial.TaskMode = GatewayImageTaskModeUnknown
	initial.TaskIDReturned = false
	initial.TaskIDJSONPath = ""
	initial.TaskQuerySupported = false
	initial.TaskQueryMethod = ""
	initial.TaskQueryPathTemplate = ""
	initial.TaskStatusJSONPath = ""
	initial.TaskSuccessValues = nil
	initial.TaskFailureValues = nil
	saved, err := store.SaveImageOperationCapabilityConfig(initial, generation, 0, "administrator")
	if err != nil {
		t.Fatal(err)
	}
	updates := []GatewayImageOperationCapability{configuredAsyncImageCapability(generation), configuredAsyncImageCapability(generation)}
	updates[0].TaskMode = GatewayImageTaskModeSync
	updates[0].TaskIDReturned = false
	updates[0].TaskIDJSONPath = ""
	updates[0].TaskQuerySupported = false
	updates[0].TaskQueryMethod = ""
	updates[0].TaskQueryPathTemplate = ""
	updates[0].TaskStatusJSONPath = ""
	updates[0].TaskSuccessValues = nil
	updates[0].TaskFailureValues = nil
	var wg sync.WaitGroup
	errs := make(chan error, len(updates))
	for _, update := range updates {
		wg.Add(1)
		go func(value GatewayImageOperationCapability) {
			defer wg.Done()
			_, saveErr := store.SaveImageOperationCapabilityConfig(value, generation, saved.CapabilityVersion, "administrator")
			errs <- saveErr
		}(update)
	}
	wg.Wait()
	close(errs)
	successes := 0
	conflicts := 0
	for saveErr := range errs {
		switch {
		case saveErr == nil:
			successes++
		case errors.Is(saveErr, ErrGatewayImageCapabilityVersionConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent save error: %v", saveErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent saves success=%d conflict=%d", successes, conflicts)
	}
}

func TestGatewayImageCapabilityRejectsAmbiguousTerminalState(t *testing.T) {
	store, generation := setupGatewayImageCapabilityTest(t)
	capability := configuredAsyncImageCapability(generation)
	capability.TaskSuccessValues = []string{"completed", "shared"}
	capability.TaskFailureValues = []string{"failed", "SHARED"}
	if _, err := store.SaveImageOperationCapabilityConfig(capability, generation, 0, "administrator"); err == nil || !strings.Contains(err.Error(), "both successful and failed") {
		t.Fatalf("ambiguous terminal states error = %v", err)
	}
}

func TestGatewayImageCapabilityObservationRequiresNormalSuccessLog(t *testing.T) {
	store, generation := setupGatewayImageCapabilityTest(t)
	capability := GatewayImageOperationCapability{
		ChannelID: generation.ChannelID, ModelName: "gpt-image-test", Operation: GatewayImageOperationGeneration,
		Protocol: GatewayImageProtocolImages, EndpointPath: "/v1/images/generations", TaskMode: GatewayImageTaskModeSync,
		EvidenceDetail: "observed_sync_response", EvidenceObservedAt: time.Now().UTC(),
	}
	if _, err := store.ObserveImageOperationCapability(capability, generation, 0); err == nil {
		t.Fatal("zero request log ID was accepted")
	}
	if err := store.db.Create(&GatewayRequestLog{ID: 70, ChannelID: generation.ChannelID, GroupName: generation.CanonicalGroupName, ModelName: capability.ModelName, Method: "POST", Path: "/v1/images/generations", StatusCode: 200, RouteReason: "api_test", CreatedAt: time.Now().UTC()}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ObserveImageOperationCapability(capability, generation, 70); err == nil {
		t.Fatal("internal api_test request log was accepted as normal evidence")
	}
	if _, err := store.ObserveImageOperationCapability(capability, generation, 41); err != nil {
		t.Fatalf("normal successful request log was rejected: %v", err)
	}
}

func TestGatewayImageCapabilityRejectsStalePresenceGeneration(t *testing.T) {
	store, generation := setupGatewayImageCapabilityTest(t)
	if _, err := store.SaveImageOperationCapabilityConfig(configuredAsyncImageCapability(generation), generation, 0, "administrator"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(generation.ChannelID, generation.CanonicalGroupName, "removed", 11, "BOG test", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveImageOperationCapabilityConfig(configuredAsyncImageCapability(generation), generation, 1, "administrator"); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("stale generation save error = %v", err)
	}
	items, err := store.ListCurrentImageOperationCapabilities(GatewayImageOperationCapabilityFilter{ChannelID: generation.ChannelID})
	if err != nil || len(items) != 0 {
		t.Fatalf("retired generation capabilities = %+v, %v", items, err)
	}
}

func containsFold(value, target string) bool {
	return strings.Contains(strings.ToLower(value), strings.ToLower(target))
}
