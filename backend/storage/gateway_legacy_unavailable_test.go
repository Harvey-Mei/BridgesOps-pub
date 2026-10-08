package storage

import (
	"math"
	"testing"
	"time"
)

const gatewayLegacyUnverifiedEstimateReasonForTest = "legacy_unverified_estimate"

func TestGatewayRequestCostStatusReclassifiesLegacyUnverifiedEstimates(t *testing.T) {
	usageID := "usage-verified"
	priceFactID := uint64(42)
	tests := []struct {
		name string
		log  GatewayRequestLog
		want GatewayCostStatus
	}{
		{
			name: "explicit estimated status without price fact",
			log: GatewayRequestLog{
				Cost: 0.41, CostStatus: GatewayCostStatusEstimated,
			},
			want: GatewayCostStatusUnavailable,
		},
		{
			name: "estimated flag without price fact",
			log: GatewayRequestLog{
				Cost: 0.42, CostEstimated: true,
			},
			want: GatewayCostStatusUnavailable,
		},
		{
			name: "gateway estimate source without price fact",
			log: GatewayRequestLog{
				Cost: 0.43, CostSource: " gateway_estimate ",
			},
			want: GatewayCostStatusUnavailable,
		},
		{
			name: "group ratio source without price fact",
			log: GatewayRequestLog{
				Cost: 0.44, CostSource: " GROUP_RATIO_DEFAULT_QUOTA ",
			},
			want: GatewayCostStatusUnavailable,
		},
		{
			name: "explicit reported status wins over legacy markers",
			log: GatewayRequestLog{
				Cost: 0.45, CostEstimated: true, CostSource: "gateway_estimate", CostStatus: GatewayCostStatusReported,
			},
			want: GatewayCostStatusReported,
		},
		{
			name: "explicit reconciled status wins over legacy markers",
			log: GatewayRequestLog{
				Cost: 0.46, CostEstimated: true, CostSource: "group_ratio_default_quota", CostStatus: GatewayCostStatusReconciled,
			},
			want: GatewayCostStatusReconciled,
		},
		{
			name: "upstream usage evidence is reconciled",
			log: GatewayRequestLog{
				Cost: 0.47, CostEstimated: true, CostSource: "gateway_estimate", UpstreamUsageID: &usageID,
			},
			want: GatewayCostStatusReconciled,
		},
		{
			name: "reconciled provider source is reconciled",
			log: GatewayRequestLog{
				Cost: 0.48, CostEstimated: true, CostSource: gatewayCostSourceSub2APIActualCost,
			},
			want: GatewayCostStatusReconciled,
		},
		{
			name: "provider reported source remains reported",
			log: GatewayRequestLog{
				Cost: 0.49, CostSource: "actual_cost",
			},
			want: GatewayCostStatusReported,
		},
		{
			name: "price fact backed estimate remains estimated",
			log: GatewayRequestLog{
				Cost: 0.50, CostEstimated: true, CostSource: "gateway_estimate", PriceFactID: &priceFactID,
			},
			want: GatewayCostStatusEstimated,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := gatewayRequestCostStatus(&test.log); got != test.want {
				t.Fatalf("cost status = %q, want %q; log=%+v", got, test.want, test.log)
			}
		})
	}
}

func TestGatewayRequestEffectiveCostStatusSQLReclassifiesLegacyUnverifiedEstimates(t *testing.T) {
	db := openTestDB(t)
	usageID := "usage-sql-verified"
	logs := []GatewayRequestLog{
		{DownstreamKeyID: 1, ChannelID: 1, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.41, CostStatus: GatewayCostStatusEstimated},
		{DownstreamKeyID: 1, ChannelID: 2, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.42, CostEstimated: true},
		{DownstreamKeyID: 1, ChannelID: 3, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.43, CostSource: "gateway_estimate"},
		{DownstreamKeyID: 1, ChannelID: 4, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.44, CostSource: "group_ratio_default_quota"},
		{DownstreamKeyID: 1, ChannelID: 5, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.45, CostEstimated: true, CostSource: "gateway_estimate", CostStatus: GatewayCostStatusReported},
		{DownstreamKeyID: 1, ChannelID: 6, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.46, CostEstimated: true, CostSource: "group_ratio_default_quota", CostStatus: GatewayCostStatusReconciled},
		{DownstreamKeyID: 1, ChannelID: 7, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.47, CostEstimated: true, CostSource: "gateway_estimate", UpstreamUsageID: &usageID},
		{DownstreamKeyID: 1, ChannelID: 8, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.48, CostEstimated: true, CostSource: gatewayCostSourceNewAPIQuota},
		{DownstreamKeyID: 1, ChannelID: 9, Method: "POST", Path: "/v1/responses", StatusCode: 200, Cost: 0.49, CostSource: "actual_cost"},
	}
	for index := range logs {
		logs[index].CreatedAt = time.Date(2026, 7, 24, 8, index, 0, 0, time.UTC)
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatalf("create legacy status fixtures: %v", err)
	}

	type effectiveStatusRow struct {
		ID              uint
		EffectiveStatus GatewayCostStatus
	}
	var rows []effectiveStatusRow
	if err := db.Table("gateway_request_logs AS logs").
		Select("logs.id, "+gatewayRequestEffectiveCostStatusSQL("logs")+" AS effective_status").
		Where("logs.id IN ?", []uint{logs[0].ID, logs[1].ID, logs[2].ID, logs[3].ID, logs[4].ID, logs[5].ID, logs[6].ID, logs[7].ID, logs[8].ID}).
		Order("logs.id ASC").
		Scan(&rows).Error; err != nil {
		t.Fatalf("evaluate effective cost status SQL: %v", err)
	}
	wants := []GatewayCostStatus{
		GatewayCostStatusUnavailable,
		GatewayCostStatusUnavailable,
		GatewayCostStatusUnavailable,
		GatewayCostStatusUnavailable,
		GatewayCostStatusReported,
		GatewayCostStatusReconciled,
		GatewayCostStatusReconciled,
		GatewayCostStatusReconciled,
		GatewayCostStatusReported,
	}
	if len(rows) != len(wants) {
		t.Fatalf("effective status rows = %d, want %d: %+v", len(rows), len(wants), rows)
	}
	for index, want := range wants {
		if rows[index].ID != logs[index].ID || rows[index].EffectiveStatus != want {
			t.Fatalf("effective status row %d = %+v, want id=%d status=%q", index, rows[index], logs[index].ID, want)
		}
	}
}

func TestGatewayRequestAggregateBatchPersistsLegacyUnavailableWithoutBilling(t *testing.T) {
	db := openTestDB(t)
	createdAt := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)
	rawCost := 0.84
	logs := []GatewayRequestLog{
		{
			DownstreamKeyID: 11, ChannelID: 21, ModelName: "legacy-model", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30,
			Cost: 0.42, RawCost: &rawCost, CostEstimated: true, CostSource: "group_ratio_default_quota", CostStatus: GatewayCostStatusEstimated,
			AggregateVersion: gatewayRequestAggregateVersion - 1, GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion,
			CreatedAt: createdAt,
		},
		{
			DownstreamKeyID: 12, ChannelID: 22, ModelName: "next-batch", Method: "POST", Path: "/v1/responses",
			StatusCode: 200, Cost: 0.24, CostEstimated: true, CostSource: "gateway_estimate", CostStatus: GatewayCostStatusEstimated,
			AggregateVersion: gatewayRequestAggregateVersion - 1, GroupCacheAggregateVersion: gatewayRequestGroupCacheAggregateVersion,
			CreatedAt: createdAt.Add(time.Minute),
		},
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatalf("create aggregate backfill fixtures: %v", err)
	}

	processed, err := migrateGatewayRequestAggregateBatch(db, 1)
	if err != nil {
		t.Fatalf("migrate one aggregate batch: %v", err)
	}
	if processed != 1 {
		t.Fatalf("processed rows = %d, want one bounded row", processed)
	}

	var stored GatewayRequestLog
	if err := db.First(&stored, logs[0].ID).Error; err != nil {
		t.Fatalf("load reclassified request: %v", err)
	}
	if stored.AggregateVersion != gatewayRequestAggregateVersion || stored.CostStatus != GatewayCostStatusUnavailable || stored.CostEstimated || stored.CostUnavailableReason != gatewayLegacyUnverifiedEstimateReasonForTest {
		t.Fatalf("reclassified request facts = %+v", stored)
	}
	if math.Abs(stored.Cost-logs[0].Cost) > 1e-12 || stored.RawCost == nil || math.Abs(*stored.RawCost-rawCost) > 1e-12 || stored.CostSource != logs[0].CostSource {
		t.Fatalf("legacy audit values were not preserved: before=%+v after=%+v", logs[0], stored)
	}

	var untouched GatewayRequestLog
	if err := db.First(&untouched, logs[1].ID).Error; err != nil {
		t.Fatalf("load next-batch request: %v", err)
	}
	if untouched.AggregateVersion != gatewayRequestAggregateVersion-1 || untouched.CostStatus != GatewayCostStatusEstimated || !untouched.CostEstimated || untouched.CostUnavailableReason != "" {
		t.Fatalf("batch size one modified a later request: %+v", untouched)
	}

	var aggregate GatewayRequestAggregate
	if err := db.Where("downstream_key_id = ? AND version = ?", logs[0].DownstreamKeyID, gatewayRequestAggregateVersion).First(&aggregate).Error; err != nil {
		t.Fatalf("load rebuilt aggregate: %v", err)
	}
	if aggregate.Requests != 1 || aggregate.UnavailableRequests != 1 {
		t.Fatalf("rebuilt request counters = %+v", aggregate)
	}
	for label, value := range map[string]float64{
		"total":              aggregate.TotalCost,
		"estimated":          aggregate.EstimatedCost,
		"actual":             aggregate.ActualCost,
		"reported":           aggregate.ReportedCost,
		"reconciled":         aggregate.ReconciledCost,
		"performance":        aggregate.PerformanceCost,
		"performance_input":  aggregate.PerformanceInputCost,
		"performance_output": aggregate.PerformanceOutputCost,
	} {
		if math.Abs(value) > 1e-12 {
			t.Fatalf("%s cost = %.12f, want zero; aggregate=%+v", label, value, aggregate)
		}
	}
}
