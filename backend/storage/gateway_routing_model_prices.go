package storage

import (
	"context"
	"errors"
	"strings"
	"time"
)

// RoutingModelPriceSnapshot contains prediction inputs only. It is never used
// to settle a request or replace its immutable dispatch price fact.
type RoutingModelPriceSnapshot struct {
	Prices        []GatewayChannelModelPrice
	InputShare    float64
	WorkloadKnown bool
}

func (r *Rates) RoutingModelPrices(ctx context.Context, model string, routes []GatewayRequestGroupCacheRoutingRoute, now time.Time) (RoutingModelPriceSnapshot, error) {
	out := RoutingModelPriceSnapshot{InputShare: 0.5}
	if r == nil || r.db == nil || strings.TrimSpace(model) == "" || len(model) > 256 || len(routes) == 0 || len(routes) > 2000 {
		return out, errors.New("routing model price scope unavailable")
	}
	pairs := make([][]any, 0, len(routes))
	for _, route := range routes {
		pairs = append(pairs, []any{route.ChannelID, strings.ToLower(strings.TrimSpace(route.GroupName))})
	}
	if err := r.db.WithContext(ctx).Where("model_name = ? AND (channel_id, upstream_group_name) IN ?", model, pairs).
		Limit(2001).Find(&out.Prices).Error; err != nil {
		return out, err
	}
	if len(out.Prices) > 2000 {
		return RoutingModelPriceSnapshot{}, errors.New("routing model price snapshot exceeds bound")
	}
	if len(out.Prices) == 0 {
		return out, nil
	}
	// One shared model workload makes channels comparable. SQL reduces existing
	// 15-minute aggregates; no raw request scans or per-candidate history reads.
	var workload struct {
		Input    float64
		Output   float64
		Requests int64
	}
	err := r.db.WithContext(ctx).Model(&GatewayRequestAggregate{}).
		Select("COALESCE(SUM(prompt_tokens), 0) AS input, COALESCE(SUM(completion_tokens), 0) AS output, COALESCE(SUM(requests), 0) AS requests").
		Where("version = ? AND model_name = ? AND bucket_start >= ? AND result = ?", gatewayRequestAggregateVersion, model, now.Add(-7*24*time.Hour), GatewayRequestResultSuccess).
		Where("route_reason NOT IN ?", []string{"latency_probe", "api_test", "model_check"}).Scan(&workload).Error
	if err != nil {
		return RoutingModelPriceSnapshot{}, err
	}
	if workload.Requests >= 10 && workload.Input > 0 && workload.Output >= 0 && workload.Input+workload.Output > 0 {
		out.InputShare = workload.Input / (workload.Input + workload.Output)
		out.WorkloadKnown = true
	}
	return out, nil
}
