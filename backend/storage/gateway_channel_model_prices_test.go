package storage

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestNormalizeGatewayChannelModelPriceSnapshotKeepsGroupModelRowsDistinct(t *testing.T) {
	defaultRatio, plusRatio := 1.0, 0.01
	input := GatewayChannelModelPriceSnapshotInput{
		ChannelID: 7, ExpectedIdentityRevision: 2, ExpectedCostUnitEpoch: 3,
		SourceVersion: " price-v1 ", SourceChecksum: strings.Repeat("A", 64),
		ObservedAt: time.Date(2026, time.August, 30, 12, 0, 0, 123456789, time.FixedZone("test", 8*60*60)),
		Items: []GatewayChannelModelPriceSnapshotItem{
			{UpstreamGroupName: "PLUS", GroupRatio: &plusRatio, ModelName: "gpt-test", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30},
			{UpstreamGroupName: "default", GroupRatio: &defaultRatio, ModelName: "gpt-test", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30},
		},
	}
	if err := normalizeGatewayChannelModelPriceSnapshot(&input); err != nil {
		t.Fatalf("normalize snapshot: %v", err)
	}
	if input.SourceVersion != "price-v1" || input.SourceChecksum != strings.Repeat("a", 64) ||
		input.ObservedAt.Location() != time.UTC || input.ObservedAt.Nanosecond() != 123456000 {
		t.Fatalf("normalized snapshot metadata = %+v", input)
	}
	if len(input.Items) != 2 || input.Items[0].UpstreamGroupName != "default" ||
		input.Items[1].UpstreamGroupName != "plus" || input.Items[0].ModelName != input.Items[1].ModelName {
		t.Fatalf("normalized group-model rows = %+v", input.Items)
	}
}

func TestNormalizeGatewayChannelModelPriceSnapshotRejectsDuplicateCanonicalGroupModel(t *testing.T) {
	ratio := 1.0
	input := validGatewayChannelModelPriceSnapshotInput(ratio)
	input.Items = append(input.Items, GatewayChannelModelPriceSnapshotItem{
		UpstreamGroupName: "DEFAULT", GroupRatio: &ratio, ModelName: "gpt-test",
		InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
	})
	if err := normalizeGatewayChannelModelPriceSnapshot(&input); !errors.Is(err, ErrGatewayModelPriceInvalid) {
		t.Fatalf("duplicate canonical group-model error = %v", err)
	}
}

func TestNormalizeGatewayChannelModelPriceSnapshotValidatesGroupAndLongContext(t *testing.T) {
	ratio := 1.0
	tests := []struct {
		name   string
		mutate func(*GatewayChannelModelPriceSnapshotItem)
	}{
		{name: "missing group ratio", mutate: func(item *GatewayChannelModelPriceSnapshotItem) { item.GroupRatio = nil }},
		{name: "negative provider group id", mutate: func(item *GatewayChannelModelPriceSnapshotItem) { item.UpstreamGroupID = -1 }},
		{name: "unsupported long context basis", mutate: func(item *GatewayChannelModelPriceSnapshotItem) {
			threshold, inputPrice, outputPrice := int64(272_000), 10.0, 60.0
			item.LongContextBasis = "marginal"
			item.LongContextThresholdTokens = &threshold
			item.LongContextInputUnitPriceUSD = &inputPrice
			item.LongContextOutputUnitPriceUSD = &outputPrice
		}},
		{name: "incomplete long context prices", mutate: func(item *GatewayChannelModelPriceSnapshotItem) {
			threshold, inputPrice := int64(272_000), 10.0
			item.LongContextBasis = "whole_request"
			item.LongContextThresholdTokens = &threshold
			item.LongContextInputUnitPriceUSD = &inputPrice
		}},
		{name: "unreachable long context threshold", mutate: func(item *GatewayChannelModelPriceSnapshotItem) {
			threshold, inputPrice, outputPrice := int64(math.MaxInt64), 10.0, 60.0
			item.LongContextBasis = "whole_request"
			item.LongContextThresholdTokens = &threshold
			item.LongContextInputUnitPriceUSD = &inputPrice
			item.LongContextOutputUnitPriceUSD = &outputPrice
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validGatewayChannelModelPriceSnapshotInput(ratio)
			test.mutate(&input.Items[0])
			if err := normalizeGatewayChannelModelPriceSnapshot(&input); !errors.Is(err, ErrGatewayModelPriceInvalid) {
				t.Fatalf("normalize error = %v", err)
			}
		})
	}

	threshold, longInput, longOutput := int64(128_000), 10.0, 60.0
	valid := validGatewayChannelModelPriceSnapshotInput(ratio)
	valid.Items[0].LongContextBasis = "whole_request"
	valid.Items[0].LongContextThresholdTokens = &threshold
	valid.Items[0].LongContextInputUnitPriceUSD = &longInput
	valid.Items[0].LongContextOutputUnitPriceUSD = &longOutput
	if err := normalizeGatewayChannelModelPriceSnapshot(&valid); err != nil {
		t.Fatalf("valid long-context price rejected: %v", err)
	}
}

func TestGatewayRequestPriceFactFromChannelPriceRequiresExactGroupGeneration(t *testing.T) {
	rawRatio := 1.0
	price := GatewayChannelModelPrice{
		ChannelID: 1, ChannelIdentityRevision: 2, CostUnitEpoch: 3,
		GatewayUpstreamGroupID: 4, UpstreamGroupPresenceGeneration: 5,
		UpstreamGroupName: "default", GroupRatio: rawRatio, ModelName: "gpt-test",
		InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
	}
	scope := GatewayRequestPriceScope{
		ChannelID: 1, ChannelAccountIdentityRevision: 2,
		UpstreamGroupID: 4, UpstreamGroupPresenceGeneration: 5,
		CanonicalGroupName: "default", ModelName: "gpt-test",
		Operation: GatewayPricingOperationTextGeneration, Protocol: GatewayPricingProtocolOpenAIResponses,
	}
	if _, ok := gatewayRequestPriceFactFromChannelPrice(scope, 3, 1, 7, &rawRatio, &price, time.Now()); !ok {
		t.Fatal("exact channel group generation did not resolve")
	}

	wrongID := scope
	wrongID.UpstreamGroupID++
	if _, ok := gatewayRequestPriceFactFromChannelPrice(wrongID, 3, 1, 7, &rawRatio, &price, time.Now()); ok {
		t.Fatal("channel price crossed a stable group id")
	}
	wrongGeneration := scope
	wrongGeneration.UpstreamGroupPresenceGeneration++
	if _, ok := gatewayRequestPriceFactFromChannelPrice(wrongGeneration, 3, 1, 7, &rawRatio, &price, time.Now()); ok {
		t.Fatal("channel price crossed a group presence generation")
	}
}

func validGatewayChannelModelPriceSnapshotInput(groupRatio float64) GatewayChannelModelPriceSnapshotInput {
	return GatewayChannelModelPriceSnapshotInput{
		ChannelID: 7, ExpectedIdentityRevision: 2, ExpectedCostUnitEpoch: 3,
		SourceVersion: "v1", SourceChecksum: strings.Repeat("a", 64), ObservedAt: time.Now(),
		Items: []GatewayChannelModelPriceSnapshotItem{{
			UpstreamGroupID: 1, UpstreamGroupName: "default", GroupRatio: &groupRatio,
			ModelName: "gpt-test", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
		}},
	}
}
