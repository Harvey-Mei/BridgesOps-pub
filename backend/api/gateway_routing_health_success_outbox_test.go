package api

import (
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestGatewayDispatchRecoveryFactsCarryIndependentRoutingHealthFence(t *testing.T) {
	candidate := gateway.Candidate{
		Channel: storage.Channel{ID: 7, UpstreamAccountIdentityRevision: 11},
		Rate: storage.RateSnapshot{
			ID: 3, ChannelID: 7, ModelName: "primary",
		},
		UpstreamGeneration: storage.UpstreamGroupGeneration{
			ID: 13, ChannelID: 7, CanonicalGroupName: "primary", PresenceGeneration: 17,
		},
	}
	state := &gatewayDispatchAuditState{route: gatewayRouteSnapshot{Candidate: candidate}}
	facts := state.recoveryFacts()
	if facts.RoutingHealthFence == nil || facts.RoutingHealthFence.ChannelID != 7 ||
		facts.RoutingHealthFence.ChannelIdentityRevision != 11 || facts.RoutingHealthFence.UpstreamGroupID != 13 ||
		facts.RoutingHealthFence.PresenceGeneration != 17 || facts.RoutingHealthFence.CanonicalGroupName != "primary" {
		t.Fatalf("dispatch recovery routing health fence = %+v", facts.RoutingHealthFence)
	}

	candidate.UpstreamGeneration = storage.UpstreamGroupGeneration{}
	state.route.Candidate = candidate
	if facts := state.recoveryFacts(); facts.RoutingHealthFence != nil {
		t.Fatalf("fixed no-rate dispatch emitted routing health fence: %+v", facts.RoutingHealthFence)
	}
}
