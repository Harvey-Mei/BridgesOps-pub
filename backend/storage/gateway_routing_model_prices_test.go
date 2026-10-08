package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestRoutingModelPricesUsesExactGroupAndAggregateWorkload(t *testing.T) {
	db, _, channel, audit := gatewayChannelPriceResolutionFixture(t, "routing-price-model")
	now := time.Now().UTC()
	replaceGatewayChannelPriceResolutionSnapshot(t, db, channel, now, "a", gatewayChannelPriceResolutionItem("primary", audit.ModelName, 2, 8, gatewayChannelPriceResolutionFloat(0.2), gatewayChannelPriceResolutionFloat(2)))
	rows := []GatewayRequestAggregate{
		{BucketStart: now, ModelName: audit.ModelName, ChannelID: channel.ID, Result: GatewayRequestResultSuccess, RouteReason: "smart_primary", Version: gatewayRequestAggregateVersion, Requests: 10, PromptTokens: 980, CompletionTokens: 20},
		{BucketStart: now, ModelName: audit.ModelName, ChannelID: channel.ID, Result: GatewayRequestResultSuccess, RouteReason: "api_test", Version: gatewayRequestAggregateVersion, Requests: 1000, PromptTokens: 1, CompletionTokens: 99999},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	store := NewRates(db)
	got, err := store.RoutingModelPrices(context.Background(), audit.ModelName, []GatewayRequestGroupCacheRoutingRoute{{ChannelID: channel.ID, GroupName: "PRIMARY"}}, now)
	if err != nil || len(got.Prices) != 1 || !got.WorkloadKnown || math.Abs(got.InputShare-0.98) > 1e-9 {
		t.Fatalf("model price snapshot=%+v error=%v", got, err)
	}
	missing, err := store.RoutingModelPrices(context.Background(), audit.ModelName, []GatewayRequestGroupCacheRoutingRoute{{ChannelID: channel.ID, GroupName: "other"}}, now)
	if err != nil || len(missing.Prices) != 0 {
		t.Fatal("price leaked across groups", err)
	}
}
