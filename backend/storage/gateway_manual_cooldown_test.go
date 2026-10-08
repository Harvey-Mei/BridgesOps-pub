package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func bindRoutingGenerationToRequestLog(t *testing.T, db *gorm.DB, generation UpstreamGroupGeneration, log *GatewayRequestLog) {
	t.Helper()
	var channel Channel
	if err := db.Select("id", "upstream_account_identity_revision").First(&channel, generation.ChannelID).Error; err != nil {
		t.Fatalf("load routing channel identity: %v", err)
	}
	var key GatewayUpstreamKey
	if err := db.Where("channel_id = ? AND lower(btrim(group_name)) = ?", generation.ChannelID, generation.CanonicalGroupName).
		Order(upstreamKeyRoutePreferenceOrder).First(&key).Error; err != nil {
		t.Fatalf("load routing upstream key: %v", err)
	}
	log.ChannelID = generation.ChannelID
	log.ChannelIdentityRevision = channel.UpstreamAccountIdentityRevision
	log.UpstreamGroupID = generation.ID
	log.UpstreamGroupGeneration = generation.PresenceGeneration
	log.UpstreamKeyLocalID = key.ID
	log.UpstreamKeyGeneration = key.KeyGeneration
	if strings.TrimSpace(log.ModelName) == "" {
		log.ModelName = "routing-fixture-model"
	}
	log.PriceCanonicalGroupName = generation.CanonicalGroupName
	log.PriceModelName = log.ModelName
	log.PriceOperation = GatewayPricingOperationTextGeneration
	log.PriceProtocol = GatewayPricingProtocolOpenAIResponses
	log.UsageSchema = GatewayPricingUsageSchemaTokensV1
	log.UsageSchemaVersion = 1
}

func routingGenerationForRequestLog(t *testing.T, db *gorm.DB, channelID uint, groupName string, log *GatewayRequestLog) UpstreamGroupGeneration {
	t.Helper()
	at := log.CreatedAt
	if at.IsZero() {
		at = time.Now().UTC()
	}
	if _, err := NewRates(db).Upsert(&RateSnapshot{ChannelID: channelID, ModelName: groupName, Ratio: 1, LastSeenAt: at}); err != nil {
		t.Fatalf("create routing generation rate: %v", err)
	}
	generation, err := NewGateway(db).CaptureUpstreamGroupGeneration(channelID, groupName)
	if err != nil {
		t.Fatalf("capture routing generation: %v", err)
	}
	bindRoutingGenerationToRequestLog(t, db, generation, log)
	return generation
}

func TestApplyRequestLogManualRoutingCooldownResolvesStoredRouteAndAudits(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channel := Channel{Name: "actual-channel", Type: ChannelTypeNewAPI, SiteURL: "https://actual.example", PasswordCipher: "test"}
	if err := db.Create(&channel).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "actual-group", APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	requestLog := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, GroupName: "actual-group",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, FirstByteMs: 45_000, LatencyMs: 46_000,
		RouteReason: "smart_primary", CreatedAt: time.Now().UTC(),
	}
	routingGenerationForRequestLog(t, db, channel.ID, "actual-group", &requestLog)
	if err := db.Create(&requestLog).Error; err != nil {
		t.Fatal(err)
	}

	appliedAt := time.Now().UTC().Truncate(time.Microsecond)
	result, err := store.ApplyRequestLogManualRoutingCooldown(requestLog.ID, 10*time.Minute, "admin-user", "cooldown-action-1", appliedAt)
	if err != nil {
		t.Fatalf("apply manual cooldown: %v", err)
	}
	if result.ChannelID != channel.ID || result.ChannelName != channel.Name || result.GroupName != "actual-group" || result.DurationSeconds != 600 || result.IdempotentReplay || !result.CooldownUntil.Equal(appliedAt.Add(10*time.Minute)) {
		t.Fatalf("manual cooldown result = %+v", result)
	}
	stored, err := store.FindUpstreamKey(channel.ID, "actual-group")
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastRoutingFailureKind != GatewayRoutingFailureManualCooldown || stored.LastRoutingFailureAt == nil || !stored.LastRoutingFailureAt.Equal(appliedAt) || stored.RoutingCooldownUntil == nil || !stored.RoutingCooldownUntil.Equal(appliedAt.Add(10*time.Minute)) {
		t.Fatalf("stored manual cooldown = %+v", stored)
	}
	var audit GatewayManualRoutingCooldownAudit
	if err := db.Where("request_log_id = ?", requestLog.ID).First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	if audit.ChannelID != channel.ID || audit.GroupName != "actual-group" || audit.Actor != "admin-user" || audit.IdempotencyKey != "cooldown-action-1" || audit.RequestFirstByteMs != 45_000 || !audit.RequestCreatedAt.Equal(requestLog.CreatedAt.Truncate(time.Microsecond)) || audit.DurationSeconds != 600 || !audit.CooldownUntil.Equal(appliedAt.Add(10*time.Minute)) {
		t.Fatalf("manual cooldown audit = %+v", audit)
	}

	replay, err := store.ApplyRequestLogManualRoutingCooldown(requestLog.ID, 30*time.Minute, "admin-user", "cooldown-action-1", appliedAt.Add(time.Minute))
	if err != nil {
		t.Fatalf("replay manual cooldown: %v", err)
	}
	if !replay.IdempotentReplay || replay.DurationSeconds != 600 || !replay.CooldownUntil.Equal(result.CooldownUntil) {
		t.Fatalf("manual cooldown replay = %+v", replay)
	}
	var auditCount int64
	if err := db.Model(&GatewayManualRoutingCooldownAudit{}).Where("request_log_id = ?", requestLog.ID).Count(&auditCount).Error; err != nil || auditCount != 1 {
		t.Fatalf("manual cooldown audit count = %d, err=%v", auditCount, err)
	}
	if _, err := store.ApplyRequestLogManualRoutingCooldown(requestLog.ID, 10*time.Minute, "other-admin", "cooldown-action-1", appliedAt.Add(2*time.Minute)); !errors.Is(err, ErrGatewayManualCooldownIdempotencyConflict) {
		t.Fatalf("manual cooldown actor conflict = %v", err)
	}

	if err := store.RecordUpstreamKeyRoutingFailure(channel.ID, "actual-group", "transport", 0, "late in-flight failure", appliedAt.Add(time.Minute)); err != nil {
		t.Fatalf("record failure during manual cooldown: %v", err)
	}
	if err := store.TouchUpstreamKey(channel.ID, "actual-group", appliedAt.Add(5*time.Minute)); err != nil {
		t.Fatalf("touch during manual cooldown: %v", err)
	}
	stored, err = store.FindUpstreamKey(channel.ID, "actual-group")
	if err != nil || stored.LastRoutingFailureKind != GatewayRoutingFailureManualCooldown || stored.RoutingFailureCount != 0 || stored.RoutingCooldownUntil == nil || !stored.RoutingCooldownUntil.Equal(result.CooldownUntil) {
		t.Fatalf("manual cooldown was cleared by success: %+v err=%v", stored, err)
	}
	var adaptiveFailures int64
	if err := db.Model(&GatewayRoutingFailure{}).Where("channel_id = ? AND group_name = ?", channel.ID, "actual-group").Count(&adaptiveFailures).Error; err != nil || adaptiveFailures != 0 {
		t.Fatalf("success during manual cooldown left adaptive failures=%d err=%v", adaptiveFailures, err)
	}
	if err := store.TouchUpstreamKey(channel.ID, "actual-group", appliedAt.Add(11*time.Minute)); err != nil {
		t.Fatalf("touch after manual cooldown: %v", err)
	}
	stored, err = store.FindUpstreamKey(channel.ID, "actual-group")
	if err != nil || stored.LastRoutingFailureKind != "" || stored.RoutingCooldownUntil != nil {
		t.Fatalf("expired manual cooldown was not cleared: %+v err=%v", stored, err)
	}
	if err := db.Model(&GatewayManualRoutingCooldownAudit{}).Where("request_log_id = ?", requestLog.ID).Count(&auditCount).Error; err != nil || auditCount != 1 {
		t.Fatalf("success changed manual cooldown audit count=%d err=%v", auditCount, err)
	}
}

func TestApplyRequestLogManualRoutingCooldownRejectsLogWithoutStoredRoute(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	requestLog := GatewayRequestLog{
		DownstreamKeyID: 1, Method: "POST", Path: "/v1/responses", StatusCode: 502, FirstByteMs: 31_000, LatencyMs: 31_000,
		RouteReason: "smart_primary", CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(&requestLog).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyRequestLogManualRoutingCooldown(requestLog.ID, 10*time.Minute, "admin", "missing-route", time.Now().UTC()); !errors.Is(err, ErrGatewayRequestLogRouteUnavailable) {
		t.Fatalf("manual cooldown error = %v", err)
	}
	var audits int64
	if err := db.Model(&GatewayManualRoutingCooldownAudit{}).Count(&audits).Error; err != nil || audits != 0 {
		t.Fatalf("unexpected audits = %d, err=%v", audits, err)
	}
}

func TestApplyRequestLogManualRoutingCooldownRejectsFastRequest(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channel := Channel{Name: "actual-channel", Type: ChannelTypeNewAPI, SiteURL: "https://actual.example", PasswordCipher: "test"}
	if err := db.Create(&channel).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "actual-group", APIKeyCipher: "cipher", Enabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	requestLog := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "actual-group", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, FirstByteMs: GatewayManualRoutingCooldownMinimumFirstByte.Milliseconds(), LatencyMs: 31_000, CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(&requestLog).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyRequestLogManualRoutingCooldown(requestLog.ID, 10*time.Minute, "admin", "fast-request", time.Now().UTC()); !errors.Is(err, ErrGatewayRequestLogManualCooldownIneligible) {
		t.Fatalf("fast request manual cooldown error = %v", err)
	}
	stored, err := store.FindUpstreamKey(channel.ID, "actual-group")
	if err != nil || stored.RoutingCooldownUntil != nil {
		t.Fatalf("fast request changed routing state: %+v err=%v", stored, err)
	}

	internalLog := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "actual-group", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, FirstByteMs: 31_000, LatencyMs: 32_000, RouteReason: gatewayRouteReasonLatencyProbe, CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(&internalLog).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyRequestLogManualRoutingCooldown(internalLog.ID, 10*time.Minute, "admin", "internal-request", time.Now().UTC()); !errors.Is(err, ErrGatewayRequestLogManualCooldownIneligible) {
		t.Fatalf("internal request manual cooldown error = %v", err)
	}

	manualLog := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "actual-group", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, FirstByteMs: 45_000, LatencyMs: 46_000, RouteReason: "manual", CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(&manualLog).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyRequestLogManualRoutingCooldown(manualLog.ID, 10*time.Minute, "admin", "manual-request", time.Now().UTC()); !errors.Is(err, ErrGatewayRequestLogManualCooldownIneligible) {
		t.Fatalf("manual route cooldown error = %v", err)
	}

	unknownLog := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "actual-group", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, FirstByteMs: 45_000, LatencyMs: 46_000, RouteReason: "future_route_reason", CreatedAt: time.Now().UTC(),
	}
	if err := db.Create(&unknownLog).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyRequestLogManualRoutingCooldown(unknownLog.ID, 10*time.Minute, "admin", "unknown-request", time.Now().UTC()); !errors.Is(err, ErrGatewayRequestLogManualCooldownIneligible) {
		t.Fatalf("unknown route cooldown error = %v", err)
	}
}

func TestApplyRequestLogManualRoutingCooldownRejectsRecreatedGroup(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channel := Channel{Name: "actual-channel", Type: ChannelTypeNewAPI, SiteURL: "https://actual.example", PasswordCipher: "test"}
	if err := db.Create(&channel).Error; err != nil {
		t.Fatal(err)
	}
	original := GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "actual-group", APIKeyCipher: "cipher-old", Enabled: true}
	if err := db.Create(&original).Error; err != nil {
		t.Fatal(err)
	}
	requestLog := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "actual-group", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, FirstByteMs: 31_000, LatencyMs: 32_000, RouteReason: "smart_primary", CreatedAt: time.Now().UTC().Add(-time.Minute),
	}
	routingGenerationForRequestLog(t, db, channel.ID, "actual-group", &requestLog)
	if err := db.Create(&requestLog).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(&original).Error; err != nil {
		t.Fatal(err)
	}
	recreated := GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "actual-group", APIKeyCipher: "cipher-new", Enabled: true}
	if err := db.Create(&recreated).Error; err != nil {
		t.Fatal(err)
	}

	_, err := store.ApplyRequestLogManualRoutingCooldown(requestLog.ID, 10*time.Minute, "admin", "recreated-group", time.Now().UTC())
	if !errors.Is(err, ErrGatewayRequestLogUpstreamGroupUnavailable) {
		t.Fatalf("recreated group manual cooldown error = %v", err)
	}
	stored, err := store.FindUpstreamKey(channel.ID, "actual-group")
	if err != nil || stored.ID != recreated.ID || stored.RoutingCooldownUntil != nil {
		t.Fatalf("recreated group changed by historical request: %+v err=%v", stored, err)
	}
	var audits int64
	if err := db.Model(&GatewayManualRoutingCooldownAudit{}).Where("request_log_id = ?", requestLog.ID).Count(&audits).Error; err != nil || audits != 0 {
		t.Fatalf("recreated group audits = %d, err=%v", audits, err)
	}
}
