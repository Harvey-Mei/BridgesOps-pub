package gateway

import (
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestLogRequestDoesNotRefillPricingFenceCostBasis(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	service := NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, nil, nil)
	log := &storage.GatewayRequestLog{
		ChannelID:             99,
		RecordKind:            storage.GatewayRequestRecordKindRequest,
		Method:                "POST",
		Path:                  "/v1/responses",
		StatusCode:            200,
		CostStatus:            storage.GatewayCostStatusUnavailable,
		CostUnavailableReason: storage.GatewayPricingUnavailableScopeIncomplete,
		UpstreamRequestID:     "provider-log-pricing-fence",
	}

	service.LogRequest(log)
	if log.ID == 0 {
		t.Fatal("pricing-fenced log was not persisted")
	}
	if log.FundingFactor != nil || log.RawGroupRatio != nil || log.RawCompletionRatio != nil || log.RawCost != nil ||
		log.CostBasisVersion != 0 || log.CostBasisSource != "" {
		t.Fatalf("service refilled pricing-fenced cost basis: %+v", log)
	}

	var stored storage.GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read pricing-fenced log: %v", err)
	}
	if stored.FundingFactor != nil || stored.RawGroupRatio != nil || stored.RawCompletionRatio != nil || stored.RawCost != nil ||
		stored.CostBasisVersion != 0 || stored.CostBasisSource != "" {
		t.Fatalf("stored service log refilled pricing-fenced cost basis: %+v", stored)
	}
}
