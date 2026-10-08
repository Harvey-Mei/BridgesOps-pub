package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

type waitingProbeCostManager struct {
	*fakeKeyManager
	duringLookup func()
}

func (m *waitingProbeCostManager) LookupRequestCost(ctx context.Context, id uint, query connector.RequestCostQuery) (*connector.RequestCostResult, error) {
	if m.duringLookup != nil {
		m.duringLookup()
	}
	return m.fakeKeyManager.LookupRequestCost(ctx, id, query)
}

func TestWaitingProbeCostIdentityAndLatencyExclusion(t *testing.T) {
	for _, mode := range []string{"current", "account-before", "unit-before", "account-during", "unit-during", "untrusted", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			db := testdb.Open(t)
			channels, store := storage.NewChannels(db), storage.NewGateway(db)
			channel := &storage.Channel{Name: "waiting", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid"}
			if err := channels.Create(channel); err != nil {
				t.Fatal(err)
			}
			now, factor := time.Now().UTC(), 1.0
			log := storage.GatewayRequestLog{ChannelID: channel.ID, RouteDetails: WaitingProbeCostDetails(Candidate{Channel: *channel, FundingFactor: factor}), GroupName: "primary", ModelName: "gpt-test", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, PromptTokens: 10, TotalTokens: 10, LatencyMs: 20, UpstreamKeyID: 7, UpstreamKeyName: "BOG primary", FundingFactor: &factor, CostStatus: storage.GatewayCostStatusUnavailable, CostUnavailableReason: storage.GatewayPricingUnavailableFactMissing, CreatedAt: now}
			keys := &waitingProbeCostManager{fakeKeyManager: &fakeKeyManager{requestCost: &connector.RequestCostResult{UsageID: "waiting-cost", Cost: .02, Source: "newapi_quota"}}}
			service := NewService(channels, storage.NewRates(db), store, nil, keys, nil)
			service.costWorkerOnce.Do(func() {})
			change := func(column string) {
				if err := db.Model(&storage.Channel{}).Where("id = ?", channel.ID).Update(column, gorm.Expr(column+" + 1")).Error; err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "account-before":
				change("upstream_account_identity_revision")
			case "unit-before":
				change("cost_unit_epoch")
			case "account-during":
				keys.duringLookup = func() { change("upstream_account_identity_revision") }
			case "unit-during":
				keys.duringLookup = func() { change("cost_unit_epoch") }
			case "untrusted":
				if err := db.Model(&storage.ChannelCostState{}).Where("channel_id = ?", channel.ID).Update("status", costing.StateStatusNeedsReview).Error; err != nil {
					t.Fatal(err)
				}
				service.Costing = costing.NewService(storage.NewCosting(db))
			case "legacy":
				log.RouteDetails = storage.GatewayWaitingProbeDetail
			}
			if err := service.LogWaitingProbe(context.Background(), &log, now); err != nil {
				t.Fatal(err)
			}
			service.reconcilePendingCost(log, now.Add(-time.Second), now)
			var saved storage.GatewayRequestLog
			if err := db.First(&saved, log.ID).Error; err != nil {
				t.Fatal(err)
			}
			if mode == "current" {
				if saved.CostStatus != storage.GatewayCostStatusReconciled || saved.Cost != .02 {
					t.Fatalf("valid waiting cost failed: %+v", saved)
				}
			} else if saved.CostStatus == storage.GatewayCostStatusReconciled || saved.Cost != 0 || saved.CostReconcileAt != nil {
				t.Fatalf("stale waiting cost reconciled: %+v", saved)
			}
			wantCalls := 0
			if mode == "current" || mode == "account-during" || mode == "unit-during" {
				wantCalls = 1
			}
			if keys.requestCostCalls != wantCalls {
				t.Fatalf("lookups=%d want=%d", keys.requestCostCalls, wantCalls)
			}
			logs, err := store.ListRecentSuccessfulLatencyLogsSince(now.Add(-time.Hour), 10)
			if err != nil || len(logs) != 0 {
				t.Fatal("waiting probe polluted routing latency", err)
			}
		})
	}
}
