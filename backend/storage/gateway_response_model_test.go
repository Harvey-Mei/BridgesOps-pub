package storage

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestRequestLogResponseModelSurvivesListProjection(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	key := GatewayDownstreamKey{Name: "model-audit", KeyHash: "model-audit-key-hash", App: GatewayAppCodex, ModelFamily: "codex"}
	if err := db.Create(&key).Error; err != nil {
		t.Fatal(err)
	}
	row := GatewayRequestLog{DownstreamKeyID: key.ID, ModelName: "sent-model", UpstreamModelName: "billing-model", UpstreamResponseModel: "reported-model", Method: "POST", Path: "/v1/responses", StatusCode: 200, CreatedAt: time.Now().UTC()}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	items, err := store.ListRequestLogsFiltered(GatewayRequestLogFilter{}, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("list: %d rows, %v", len(items), err)
	}
	if items[0].ModelName != "sent-model" || items[0].UpstreamModelName != "billing-model" || items[0].UpstreamResponseModel != "reported-model" {
		t.Fatalf("list lost model identities: %+v", items[0])
	}
	page, _, _, err := store.ListRequestLogsPageFiltered(1, 10, GatewayRequestLogFilter{})
	if err != nil || len(page) != 1 || page[0].UpstreamResponseModel != "reported-model" {
		t.Fatalf("paged list lost response model: %v", err)
	}
}
