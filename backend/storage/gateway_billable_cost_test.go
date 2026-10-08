package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestRequestAggregateFromLogGatesOnlyMonetaryContributions(t *testing.T) {
	for _, test := range []struct {
		name           string
		log            GatewayRequestLog
		totalCost      float64
		estimatedCost  float64
		reportedCost   float64
		reconciledCost float64
		unavailable    int64
	}{
		{
			name: "successful reported",
			log: GatewayRequestLog{Path: "/v1/responses", StatusCode: 200, Cost: 1,
				CostStatus: GatewayCostStatusReported, TotalTokens: 10},
			totalCost: 1, reportedCost: 1,
		},
		{
			name: "ordinary failed residual",
			log: GatewayRequestLog{Path: "/v1/responses", StatusCode: 502, Error: "rejected", Cost: 9,
				CostStatus: GatewayCostStatusReported, TotalTokens: 20},
		},
		{
			name: "accepted failed estimate",
			log: GatewayRequestLog{Path: "/v1/responses", StatusCode: 502, Error: "stream failed", UpstreamAccepted: true,
				Cost: 0.2, CostStatus: GatewayCostStatusEstimated, TotalTokens: 30},
			unavailable: 1,
		},
		{
			name: "successful legacy unavailable preserves audit value only",
			log: GatewayRequestLog{Path: "/v1/responses", StatusCode: 200, Cost: 9,
				CostSource: "gateway_estimate", CostStatus: GatewayCostStatusUnavailable,
				CostUnavailableReason: "legacy_unverified_estimate", TotalTokens: 35},
			unavailable: 1,
		},
		{
			name: "legacy estimate remains unverified",
			log: GatewayRequestLog{Path: "/v1/responses", StatusCode: 502, Error: "legacy", CostReconcileLegacyPending: true,
				Cost: 0.3, CostStatus: GatewayCostStatusEstimated, TotalTokens: 40},
			unavailable: 1,
		},
		{
			name: "legacy reported is retained",
			log: GatewayRequestLog{Path: "/v1/responses", StatusCode: 502, Error: "legacy", CostReconcileLegacyPending: true,
				Cost: 0.4, CostStatus: GatewayCostStatusReported, TotalTokens: 50},
			totalCost: 0.4, reportedCost: 0.4,
		},
		{
			name: "count tokens is never billed",
			log: GatewayRequestLog{Path: "/v1/messages/count_tokens", StatusCode: 200, Cost: 7,
				CostStatus: GatewayCostStatusReported, TotalTokens: 60},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			row, ok := requestAggregateFromLog(&test.log)
			if !ok {
				t.Fatal("request was unexpectedly excluded from request/result aggregates")
			}
			if row.Requests != 1 || row.TotalTokens != test.log.TotalTokens {
				t.Fatalf("request/token counters = %+v, want one request and %d tokens", row, test.log.TotalTokens)
			}
			assertClose := func(label string, got, want float64) {
				t.Helper()
				if math.Abs(got-want) > 1e-12 {
					t.Fatalf("%s = %.12f, want %.12f; aggregate=%+v", label, got, want, row)
				}
			}
			assertClose("total cost", row.TotalCost, test.totalCost)
			assertClose("estimated cost", row.EstimatedCost, test.estimatedCost)
			assertClose("reported cost", row.ReportedCost, test.reportedCost)
			assertClose("reconciled cost", row.ReconciledCost, test.reconciledCost)
			if row.UnavailableRequests != test.unavailable {
				t.Fatalf("unavailable requests = %d, want %d; aggregate=%+v", row.UnavailableRequests, test.unavailable, row)
			}
		})
	}
}

func TestGatewayRequestBillableRawAndAggregateSummariesMatch(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	logs := []GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 1, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 1, CostStatus: GatewayCostStatusReported, TotalTokens: 10, CreatedAt: now},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "rejected", Cost: 9, CostStatus: GatewayCostStatusReported, TotalTokens: 20, CreatedAt: now.Add(time.Second)},
		{DownstreamKeyID: 1, ChannelID: 3, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "accepted stream failed", UpstreamAccepted: true, Cost: 0.2, CostStatus: GatewayCostStatusEstimated, TotalTokens: 30, CreatedAt: now.Add(2 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 4, Method: "POST", Path: "/v1/images/generations", StatusCode: 499, RouteReason: gatewayRouteReasonClientCanceled, BillingCommitted: true, Cost: 0.3, CostStatus: GatewayCostStatusReported, CreatedAt: now.Add(3 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 5, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "legacy estimate", CostReconcileLegacyPending: true, Cost: 0.4, CostEstimated: true, CostStatus: GatewayCostStatusEstimated, CreatedAt: now.Add(4 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 6, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "legacy reported", CostReconcileLegacyPending: true, Cost: 0.5, CostStatus: GatewayCostStatusReported, CreatedAt: now.Add(5 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 7, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "legacy unknown", CostReconcileLegacyPending: true, Cost: 8, CreatedAt: now.Add(6 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 8, Method: "POST", Path: "/v1/messages/count_tokens", StatusCode: 200, Cost: 7, CostStatus: GatewayCostStatusReported, TotalTokens: 100, CreatedAt: now.Add(7 * time.Second)},
		{DownstreamKeyID: 1, ChannelID: 9, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 6, CostSource: "gateway_estimate", CostStatus: GatewayCostStatusUnavailable, CostUnavailableReason: "legacy_unverified_estimate", TotalTokens: 110, CreatedAt: now.Add(8 * time.Second)},
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatalf("create request logs: %v", err)
	}
	raw, err := store.requestLogSummaryFilteredAtRaw(GatewayRequestLogFilter{}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("raw summary: %v", err)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 2, 0); err != nil {
		t.Fatalf("backfill aggregates: %v", err)
	}
	aggregate, used, err := store.requestLogSummaryFromAggregates(GatewayRequestLogFilter{}, now.Add(time.Minute))
	if err != nil || !used {
		t.Fatalf("aggregate summary used=%v err=%v", used, err)
	}
	for _, value := range []struct {
		label string
		raw   float64
		agg   float64
		want  float64
	}{
		{label: "total", raw: raw.TotalCost, agg: aggregate.TotalCost, want: 1.8},
		{label: "estimated", raw: raw.EstimatedCost, agg: aggregate.EstimatedCost, want: 0},
		{label: "reported", raw: raw.ReportedCost, agg: aggregate.ReportedCost, want: 1.8},
		{label: "actual", raw: raw.ActualCost, agg: aggregate.ActualCost, want: 1.8},
	} {
		if math.Abs(value.raw-value.want) > 1e-12 || math.Abs(value.agg-value.want) > 1e-12 {
			t.Fatalf("%s cost raw/aggregate = %.12f/%.12f, want %.12f; raw=%+v aggregate=%+v", value.label, value.raw, value.agg, value.want, raw, aggregate)
		}
	}
	if raw.TotalRequests != int64(len(logs)) || aggregate.TotalRequests != int64(len(logs)) {
		t.Fatalf("request counts raw/aggregate = %d/%d, want %d", raw.TotalRequests, aggregate.TotalRequests, len(logs))
	}
	if raw.UnavailableRequests != 3 || aggregate.UnavailableRequests != 3 {
		t.Fatalf("unavailable raw/aggregate = %d/%d, want 3", raw.UnavailableRequests, aggregate.UnavailableRequests)
	}
}

func TestAcceptedFailedReconcileReplacesEstimatedAggregateWithActual(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	log := &GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 502,
		Error: "stream failed after acceptance", UpstreamAccepted: true, Cost: 0.4, CostEstimated: true,
		CostSource: "gateway_estimate", CreatedAt: now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	if err := store.UpdateRequestLogActualCostWithUsage(log.ID, "usage-accepted", 0.25, 0.25, gatewayCostSourceSub2APIActualCost, GatewayRequestUsageMetrics{}); err != nil {
		t.Fatalf("reconcile accepted failure: %v", err)
	}
	var aggregate struct {
		Total      float64
		Estimated  float64
		Reconciled float64
		Actual     float64
	}
	if err := db.Model(&GatewayRequestAggregate{}).
		Select("COALESCE(SUM(total_cost), 0) AS total, COALESCE(SUM(estimated_cost), 0) AS estimated, COALESCE(SUM(reconciled_cost), 0) AS reconciled, COALESCE(SUM(actual_cost), 0) AS actual").
		Scan(&aggregate).Error; err != nil {
		t.Fatalf("sum aggregate costs: %v", err)
	}
	if math.Abs(aggregate.Total-0.25) > 1e-12 || math.Abs(aggregate.Estimated) > 1e-12 || math.Abs(aggregate.Reconciled-0.25) > 1e-12 || math.Abs(aggregate.Actual-0.25) > 1e-12 {
		t.Fatalf("reconciled aggregate = %+v", aggregate)
	}
}

func TestLegacyPendingQueueSurvivesCleanupAndTerminatesNormally(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	legacy := &GatewayRequestLog{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "legacy", CostReconcileLegacyPending: true, CreatedAt: now}
	ordinary := &GatewayRequestLog{DownstreamKeyID: 1, ChannelID: 3, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "ordinary", CreatedAt: now.Add(time.Second)}
	for _, log := range []*GatewayRequestLog{legacy, ordinary} {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append log: %v", err)
		}
		if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Second)); err != nil {
			t.Fatalf("schedule log: %v", err)
		}
	}
	if cleared, err := store.ClearIneligibleRequestCostReconciles(10); err != nil || cleared != 1 {
		t.Fatalf("cleanup = %d, err=%v", cleared, err)
	}
	pending, err := store.ListPendingRequestCostReconciles(10, now)
	if err != nil || len(pending) != 1 || pending[0].ID != legacy.ID {
		t.Fatalf("pending after cleanup = %+v, err=%v", pending, err)
	}
	if err := store.MarkRequestCostReconcileFailed(legacy.ID, "exhausted"); err != nil {
		t.Fatalf("terminate legacy reconcile: %v", err)
	}
	pending, err = store.ListPendingRequestCostReconciles(10, now.Add(time.Minute))
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after terminal failure = %+v, err=%v", pending, err)
	}
}
