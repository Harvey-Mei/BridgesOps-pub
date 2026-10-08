package storage

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestDownstreamKeyRecentRouteUsesLatestRealSuccess(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	for id := uint(1); id <= 3; id++ {
		key := GatewayDownstreamKey{ID: id, Name: "test-key", KeyHash: string(rune('a' + id)), App: GatewayAppCodex, ModelFamily: "codex"}
		if err := db.Create(&key).Error; err != nil {
			t.Fatal(err)
		}
	}
	at := time.Date(2026, 9, 18, 9, 0, 0, 0, time.UTC)
	log := func(keyID, channelID uint, offset int, reason string) GatewayRequestLog {
		return GatewayRequestLog{DownstreamKeyID: keyID, ChannelID: channelID, ChannelNameSnapshot: "channel-snapshot", GroupName: "primary", ModelName: "gpt-5.4", Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: reason, CreatedAt: at.Add(time.Duration(offset) * time.Minute)}
	}
	rows := []GatewayRequestLog{log(1, 101, 0, "smart_primary"), log(1, 102, 1, "fallback")}
	for _, reason := range []string{"latency_probe", "api_test", "model_check"} {
		rows = append(rows, log(1, 200, 3, reason))
	}
	management := log(1, 201, 4, "")
	management.Method, management.Path = "GET", "/v1/models"
	failed := log(1, 202, 5, "smart_primary")
	failed.StatusCode = 500
	streamError := log(1, 203, 6, "smart_primary")
	streamError.Error = "stream failed"
	canceled := log(1, 204, 7, "client_canceled")
	canceled.Error = "context canceled"
	evidence := log(1, 205, 8, "smart_primary")
	evidence.RecordKind = GatewayRequestRecordKindDispatchCostEvidence
	evidence.StatusCode = 0
	evidence.UpstreamRequestID = "fixture-dispatch-evidence"
	fundingFactor := 1.0
	evidence.FundingFactor = &fundingFactor
	rows = append(rows, management, failed, streamError, canceled, evidence, log(2, 301, 2, "smart_primary"), log(2, 302, 2, "session_affinity"))
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	latest, err := store.latestRealRequestsByDownstreamKey([]uint{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 2 || latest[1] == nil || latest[1].ChannelID != 102 || latest[1].RequestID != rows[1].ID || !latest[1].CreatedAt.Equal(at.Add(time.Minute)) || latest[1].ChannelName != "channel-snapshot" || latest[1].GroupName != "primary" {
		t.Fatalf("latest real route = %+v", latest)
	}
	if latest[2] == nil || latest[2].ChannelID != 302 {
		t.Fatalf("equal timestamp must use greatest request ID: %+v", latest[2])
	}
	models, err := store.LatestRealRequestModelsByDownstreamKey([]uint{1, 2, 3})
	if err != nil || len(models) != 2 || models[1] != "gpt-5.4" || models[3] != "" {
		t.Fatalf("model preview compatibility: %+v, %v", models, err)
	}
	keys, err := store.ListDownstreamKeys()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if key.ID == 3 {
			if key.RecentRoute != nil {
				t.Fatal("unused key must not invent a recent route")
			}
			continue
		}
		if key.RecentRoute == nil || key.RecentRoute.RequestID != latest[key.ID].RequestID || key.CandidatePreviewModel != key.RecentRoute.ModelName {
			t.Fatalf("key projection diverged: %+v", key.RecentRoute)
		}
	}
	empty, err := store.latestRealRequestsByDownstreamKey(nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty key selection: %+v, %v", empty, err)
	}
}
