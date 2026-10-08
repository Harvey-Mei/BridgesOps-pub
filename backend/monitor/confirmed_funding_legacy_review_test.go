package monitor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestRefreshBalanceKeepsIncompleteDailyHistoryRetryableForConfirmedFunding(t *testing.T) {
	calls := &advertisedFactorSamplingCalls{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/me":
			calls.authMe.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"id":4242,"balance":9.32040524}}`))
		case "/api/v1/payment/checkout-info":
			calls.checkout.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"balance_recharge_multiplier":1}}`))
		case "/api/v1/usage/dashboard/stats":
			http.NotFound(w, r)
		case "/api/v1/usage/stats":
			cost := 1.0
			if r.URL.Query().Get("start_date") != r.URL.Query().Get("end_date") {
				cost = 1.5
			}
			_, _ = fmt.Fprintf(w, `{"code":0,"message":"","data":{"total_actual_cost":%g}}`, cost)
		case "/api/v1/payment/orders/my":
			http.NotFound(w, r)
		case "/api/v1/groups/available":
			calls.groupsAvailable.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":[{"id":7,"name":"pro","rate_multiplier":0.06}]}`))
		case "/api/v1/groups/rates":
			calls.groupRates.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"7":0.06}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc, rates, channelRecord, costSvc, db := newAdvertisedFactorSamplingService(t, srv.URL)
	costSampledAt := time.Now().UTC().Add(-48 * time.Hour)
	result := db.Model(&storage.ChannelCostState{}).Where("channel_id = ?", channelRecord.ID).Updates(map[string]any{
		"funding_factor":                      0.9893191983141713,
		"source":                              "confirmed_redeem_code",
		"status":                              costing.StateStatusNeedsReview,
		"last_error":                          costing.LegacyAdvertisedFactorEvidenceReviewReason,
		"raw_today_cost":                      3.68194317,
		"actual_today_cost":                   3.68194317,
		"raw_total_cost":                      7.179144135,
		"actual_total_cost":                   7.179144135,
		"cost_sampled_at":                     costSampledAt,
		"observed_provider_factor":            nil,
		"observed_provider_factor_source":     "",
		"observed_provider_factor_sampled_at": nil,
	})
	if result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("configure confirmed funding legacy state: err=%v rows=%d", result.Error, result.RowsAffected)
	}
	before, err := costSvc.State(channelRecord.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := svc.RefreshBalance(context.Background(), channelRecord); err != nil {
		t.Fatalf("RefreshBalance treated incomplete history as a channel failure: %v", err)
	}
	after, err := costSvc.State(channelRecord.ID)
	if err != nil || after.Status != costing.StateStatusReady || after.LastError != "" || after.Version != before.Version+1 ||
		after.RawBalance != before.RawBalance || after.ActualInventory != before.ActualInventory || after.FundingFactor != before.FundingFactor ||
		after.RawTodayCost != before.RawTodayCost || after.ActualTodayCost != before.ActualTodayCost ||
		after.RawTotalCost != before.RawTotalCost || after.ActualTotalCost != before.ActualTotalCost ||
		after.BalanceSampledAt == nil || before.BalanceSampledAt == nil || !after.BalanceSampledAt.Equal(*before.BalanceSampledAt) ||
		after.CostSampledAt == nil || before.CostSampledAt == nil || !after.CostSampledAt.Equal(*before.CostSampledAt) ||
		after.ObservedProviderFactor != nil || after.ObservedProviderFactorSource != "" || after.ObservedProviderFactorSampledAt != nil {
		t.Fatalf("retryable history recovery changed trusted financial facts: before=%+v after=%+v err=%v", before, after, err)
	}

	if err := svc.RefreshRates(context.Background(), channelRecord); err != nil {
		t.Fatalf("RefreshRates remained blocked by the stale accounting review: %v", err)
	}
	stored, err := rates.ListByChannel(channelRecord.ID)
	if err != nil || len(stored) != 1 || stored[0].ModelName != "pro" {
		t.Fatalf("rate facts after retryable accounting warning = %+v, %v", stored, err)
	}
	if got := calls.groupsAvailable.Load(); got != 1 {
		t.Fatalf("groups/available calls = %d, want 1", got)
	}
	if got := calls.groupRates.Load(); got != 1 {
		t.Fatalf("groups/rates calls = %d, want 1", got)
	}
}
