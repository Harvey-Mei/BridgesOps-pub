package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestGatewayRouteCandidateOutputIncludesProvisioningAvailability(t *testing.T) {
	preview := gateway.CandidateGroupMemberPreview{
		Candidate: gateway.Candidate{
			Channel: storage.Channel{ID: 7, Name: "channel"},
			Rate:    storage.RateSnapshot{ChannelID: 7, ModelName: "group"},
			App:     storage.GatewayAppCodex,
		},
		AvailabilityReasons:     []string{"upstream_key_unavailable"},
		UpstreamKeyConfigured:   true,
		UpstreamKeyEnabled:      true,
		UpstreamKeyProvisioning: true,
		UpstreamKeyAvailable:    false,
	}
	out := gatewayRouteCandidateOutputFromMemberPreview(preview)
	if out.UpstreamKeyProvisioning == nil || !*out.UpstreamKeyProvisioning || out.UpstreamKeyAvailable == nil || *out.UpstreamKeyAvailable {
		t.Fatalf("provisioning output = %+v", out)
	}
	body, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"upstream_key_provisioning":true`, `"upstream_key_available":false`, `"upstream_key_unavailable"`} {
		if !strings.Contains(string(body), field) {
			t.Fatalf("candidate output missing %s: %s", field, body)
		}
	}
}
