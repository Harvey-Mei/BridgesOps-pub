package gateway

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// Smart candidates have already passed the funding trust gate. Keep their
// frozen monetary identity alongside the stable marker, without fabricating
// a price-fact snapshot for this separate tiny inference request.
func WaitingProbeCostDetails(candidate Candidate) string {
	details, _ := json.Marshal(modelCheckUsageDetails{
		ChannelIdentityRevision: candidate.Channel.UpstreamAccountIdentityRevision,
		CostUnitEpoch:           candidate.Channel.CostUnitEpoch,
		FundingTrusted:          candidate.FundingFactor >= 0 && !math.IsNaN(candidate.FundingFactor) && !math.IsInf(candidate.FundingFactor, 0),
	})
	return storage.GatewayWaitingProbeDetail + "\n" + string(details)
}

// Waiting probes keep cost evidence but cannot recover a route or contribute
// their deliberately tiny prompt's latency to normal routing samples.
func (s *Service) LogWaitingProbe(ctx context.Context, log *storage.GatewayRequestLog, finishedAt time.Time) error {
	log.RouteReason = latencyProbeRouteReason
	if !storage.IsGatewayWaitingProbeDetails(log.RouteDetails) {
		log.RouteDetails = storage.GatewayWaitingProbeDetail
	}
	if _, supported := s.Keys.(requestCostManager); supported && storage.IsGatewayRequestCostReconcileEligible(log) && !requestUsesDirectCostBasis(*log) && !requestCostCannotBeMatched(*log) {
		at := InitialRequestCostReconcileAt(finishedAt)
		log.CostReconcileAt = &at
	}
	if err := s.Store.WithContext(ctx).AppendRequestLog(log); err != nil {
		return err
	}
	if log.CostReconcileAt != nil {
		s.startCostWorker()
		s.wakeCostWorker()
	}
	return nil
}
