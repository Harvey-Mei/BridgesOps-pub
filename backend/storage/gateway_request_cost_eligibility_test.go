package storage

import (
	"strings"
	"testing"
)

func TestIsGatewayRequestCostReconcileEligible(t *testing.T) {
	for _, test := range []struct {
		name string
		log  *GatewayRequestLog
		want bool
	}{
		{name: "nil"},
		{name: "successful", log: &GatewayRequestLog{StatusCode: 200}, want: true},
		{name: "accepted failed stream", log: &GatewayRequestLog{StatusCode: 502, Error: "stream failed", UpstreamAccepted: true}, want: true},
		{name: "committed canceled image", log: &GatewayRequestLog{StatusCode: 499, RouteReason: gatewayRouteReasonClientCanceled, BillingCommitted: true}, want: true},
		{name: "legacy pending failure", log: &GatewayRequestLog{StatusCode: 502, Error: "legacy failure", CostReconcileLegacyPending: true}, want: true},
		{name: "ordinary failure with provider ID", log: &GatewayRequestLog{StatusCode: 502, Error: "provider failure", UpstreamRequestID: "provider-failed"}, want: true},
		{name: "hidden provider ID evidence", log: &GatewayRequestLog{RecordKind: GatewayRequestRecordKindDispatchCostEvidence, UpstreamRequestID: "provider-evidence"}, want: true},
		{name: "hidden evidence without provider ID", log: &GatewayRequestLog{RecordKind: GatewayRequestRecordKindDispatchCostEvidence}},
		{name: "scope-incomplete pricing fence", log: &GatewayRequestLog{StatusCode: 200, CostStatus: GatewayCostStatusUnavailable, CostUnavailableReason: GatewayPricingUnavailableScopeIncomplete}},
		{name: "identity-changed pricing fence", log: &GatewayRequestLog{StatusCode: 200, CostStatus: GatewayCostStatusUnavailable, CostUnavailableReason: GatewayPricingUnavailableIdentityChanged}},
		{name: "count tokens success", log: &GatewayRequestLog{Path: "/v1/messages/count_tokens", StatusCode: 200}},
		{name: "count tokens accepted failure", log: &GatewayRequestLog{Path: "/v1/messages/count_tokens", StatusCode: 502, UpstreamAccepted: true}},
		{name: "ordinary failure", log: &GatewayRequestLog{StatusCode: 502, Error: "upstream failed"}},
		{name: "ordinary cancellation", log: &GatewayRequestLog{StatusCode: 499, RouteReason: gatewayRouteReasonClientCanceled}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsGatewayRequestCostReconcileEligible(test.log); got != test.want {
				t.Fatalf("eligible = %v, want %v for %+v", got, test.want, test.log)
			}
		})
	}
}

func TestGatewayRequestCostReconcileEligibleSQLUsesExplicitBillingFacts(t *testing.T) {
	sql := gatewayRequestCostReconcileEligibleSQL("logs")
	for _, required := range []string{"logs.record_kind", "dispatch_cost_evidence", "logs.status_code", "logs.path", "logs.billing_committed", "logs.upstream_accepted", "logs.cost_reconcile_legacy_pending", "logs.upstream_request_id", string(GatewayCostStatusUnavailable), GatewayPricingUnavailableScopeIncomplete, GatewayPricingUnavailableIdentityChanged} {
		if !strings.Contains(sql, required) {
			t.Fatalf("eligibility SQL %q missing %q", sql, required)
		}
	}
}

func TestGatewayRequestFundingCorrectionEligibleSQLRequiresReconciledDispatchEvidence(t *testing.T) {
	sql := gatewayRequestFundingCorrectionEligibleSQL("logs")
	for _, required := range []string{
		"logs.record_kind", string(GatewayRequestRecordKindRequest), string(GatewayRequestRecordKindDispatchCostEvidence),
		"logs.cost_status", string(GatewayCostStatusReconciled), "logs.upstream_usage_id", "logs.raw_cost is not null", "logs.price_fact_id is null",
	} {
		if !strings.Contains(strings.ToLower(sql), strings.ToLower(required)) {
			t.Fatalf("funding correction SQL %q missing %q", sql, required)
		}
	}
}

func TestIsGatewayRequestCostBillable(t *testing.T) {
	usageID := "usage-1"
	for _, test := range []struct {
		name string
		log  *GatewayRequestLog
		want bool
	}{
		{name: "nil"},
		{name: "successful estimated", log: &GatewayRequestLog{StatusCode: 200, Cost: 0.1, CostEstimated: true}, want: true},
		{name: "accepted failed estimated", log: &GatewayRequestLog{StatusCode: 502, UpstreamAccepted: true, Cost: 0.2, CostStatus: GatewayCostStatusEstimated}, want: true},
		{name: "committed canceled reported", log: &GatewayRequestLog{StatusCode: 499, RouteReason: gatewayRouteReasonClientCanceled, BillingCommitted: true, Cost: 0.3, CostStatus: GatewayCostStatusReported}, want: true},
		{name: "ordinary failure with residual status", log: &GatewayRequestLog{StatusCode: 502, Cost: 9, CostStatus: GatewayCostStatusReported}},
		{name: "ordinary failure with residual source", log: &GatewayRequestLog{StatusCode: 502, Cost: 9, CostSource: "reported"}},
		{name: "provider ID failure remains unverified", log: &GatewayRequestLog{StatusCode: 502, Error: "provider failure", UpstreamRequestID: "provider-failed", Cost: 9}},
		{name: "provider ID failure reconciled", log: &GatewayRequestLog{StatusCode: 502, Error: "provider failure", UpstreamRequestID: "provider-failed", Cost: 0.4, CostStatus: GatewayCostStatusReconciled}, want: true},
		{name: "hidden evidence is never ordinary billable", log: &GatewayRequestLog{RecordKind: GatewayRequestRecordKindDispatchCostEvidence, UpstreamRequestID: "provider-evidence", Cost: 0.4, CostStatus: GatewayCostStatusReconciled}},
		{name: "legacy estimated remains unverified", log: &GatewayRequestLog{StatusCode: 502, Cost: 0.4, CostEstimated: true, CostStatus: GatewayCostStatusEstimated, CostReconcileLegacyPending: true}},
		{name: "legacy reported", log: &GatewayRequestLog{StatusCode: 502, Cost: 0.4, CostStatus: GatewayCostStatusReported, CostReconcileLegacyPending: true}, want: true},
		{name: "legacy reconciled", log: &GatewayRequestLog{StatusCode: 502, Cost: 0.4, CostStatus: GatewayCostStatusReconciled, CostReconcileLegacyPending: true}, want: true},
		{name: "legacy usage evidence", log: &GatewayRequestLog{StatusCode: 502, Cost: 0.5, UpstreamUsageID: &usageID, CostReconcileLegacyPending: true}, want: true},
		{name: "legacy provider cost source", log: &GatewayRequestLog{StatusCode: 502, Cost: 0.6, CostSource: "actual_cost", CostReconcileLegacyPending: true}, want: true},
		{name: "legacy generic source remains unverified", log: &GatewayRequestLog{StatusCode: 502, Cost: 9, CostSource: "gateway_estimate", CostReconcileLegacyPending: true}},
		{name: "legacy unexplained residual", log: &GatewayRequestLog{StatusCode: 502, Cost: 9, CostReconcileLegacyPending: true}},
		{name: "count tokens", log: &GatewayRequestLog{Path: "/v1/messages/count_tokens", StatusCode: 200, Cost: 1, CostStatus: GatewayCostStatusReported}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsGatewayRequestCostBillable(test.log); got != test.want {
				t.Fatalf("billable = %v, want %v for %+v", got, test.want, test.log)
			}
		})
	}
}

func TestGatewayRequestCostBillableSQLUsesFactsAndReliableLegacyEvidence(t *testing.T) {
	sql := gatewayRequestCostBillableSQL("logs")
	for _, required := range []string{
		"logs.path", "logs.status_code", "logs.billing_committed", "logs.upstream_accepted",
		"logs.cost_reconcile_legacy_pending", "logs.cost_status", "logs.upstream_usage_id",
		"logs.record_kind", "logs.upstream_request_id", "logs.cost_source", "actual_cost", "newapi_quota", "sub2api_actual_cost",
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("billable SQL %q missing %q", sql, required)
		}
	}
	if strings.Contains(sql, "logs.cost <> 0") {
		t.Fatalf("billable SQL trusts an unexplained residual cost: %q", sql)
	}
	if strings.Contains(sql, "logs.cost_estimated") {
		t.Fatalf("billable SQL trusts a legacy estimate: %q", sql)
	}
}

func TestGatewayRequestBillableCostSQLExcludesUnavailableAmounts(t *testing.T) {
	sql := gatewayRequestBillableCostSQL("logs")
	for _, required := range []string{
		"logs.cost_status", string(GatewayCostStatusEstimated),
		string(GatewayCostStatusReported), string(GatewayCostStatusReconciled),
	} {
		if !strings.Contains(sql, required) {
			t.Fatalf("billable cost SQL %q missing %q", sql, required)
		}
	}
	if strings.Contains(sql, "'"+string(GatewayCostStatusUnavailable)+"')") {
		t.Fatalf("billable cost SQL includes unavailable amounts: %q", sql)
	}
}
