package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/analyticscache"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"golang.org/x/sync/singleflight"
)

const (
	gatewayRequestAnalyticsCacheTTL           = 30 * time.Second
	gatewayRequestAnalyticsCacheSchemaVersion = 2
)

var gatewayRequestAnalyticsFlights singleflight.Group

// gatewayRequestAnalyticsCacheKeyInput contains every filter that changes an
// aggregate result. Raw pagination and sorting are intentionally absent: this
// cache is only for summaries and chart analytics, never request-log rows.
type gatewayRequestAnalyticsCacheKeyInput struct {
	SchemaVersion     int    `json:"schema_version"`
	Kind              string `json:"kind"`
	Start             string `json:"start"`
	End               string `json:"end"`
	SystemTimeZone    string `json:"system_time_zone"`
	DownstreamKeyID   uint   `json:"downstream_key_id"`
	ModelName         string `json:"model_name"`
	ChannelID         uint   `json:"channel_id"`
	GroupName         string `json:"group_name"`
	GroupUnassigned   bool   `json:"group_unassigned"`
	UpstreamRequestID string `json:"upstream_request_id"`
	UpstreamKeyName   string `json:"upstream_key_name"`
	Result            string `json:"result"`
	RouteReason       string `json:"route_reason"`
	Limit             int    `json:"limit"`
}

func gatewayRequestAnalyticsCacheKey(kind string, filter storage.GatewayRequestLogFilter, limit int) string {
	input := gatewayRequestAnalyticsCacheKeyInput{
		SchemaVersion:     gatewayRequestAnalyticsCacheSchemaVersion,
		Kind:              kind,
		Start:             gatewayAnalyticsCacheTime(filter.Start),
		End:               gatewayAnalyticsCacheTime(filter.End),
		SystemTimeZone:    strings.TrimSpace(filter.TimeZone),
		DownstreamKeyID:   filter.DownstreamKeyID,
		ModelName:         strings.TrimSpace(filter.ModelName),
		ChannelID:         filter.ChannelID,
		GroupName:         strings.TrimSpace(filter.GroupName),
		GroupUnassigned:   filter.GroupUnassigned,
		UpstreamRequestID: filter.UpstreamRequestID,
		UpstreamKeyName:   strings.TrimSpace(filter.UpstreamKeyName),
		Result:            strings.ToLower(strings.TrimSpace(filter.Result)),
		RouteReason:       strings.TrimSpace(filter.RouteReason),
		Limit:             limit,
	}
	payload, _ := json.Marshal(input)
	digest := sha256.Sum256(payload)
	return kind + ":" + hex.EncodeToString(digest[:])
}

type gatewayRequestAnalyticsCacheEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Kind          string          `json:"kind"`
	Payload       json.RawMessage `json:"payload"`
}

func marshalGatewayRequestAnalyticsCache(kind string, value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(gatewayRequestAnalyticsCacheEnvelope{
		SchemaVersion: gatewayRequestAnalyticsCacheSchemaVersion,
		Kind:          kind,
		Payload:       payload,
	})
}

func unmarshalGatewayRequestAnalyticsCache(payload []byte, kind string, target any) bool {
	var envelope gatewayRequestAnalyticsCacheEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil ||
		envelope.SchemaVersion != gatewayRequestAnalyticsCacheSchemaVersion ||
		envelope.Kind != kind || len(envelope.Payload) == 0 || string(envelope.Payload) == "null" {
		return false
	}
	return json.Unmarshal(envelope.Payload, target) == nil
}

func gatewayAnalyticsCacheTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func gatewayAnalyticsCacheForFilter(d *Deps) (analyticscache.JSONCache, bool) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil || d.AnalyticsCache == nil {
		return nil, false
	}
	ready, err := d.Gateway.Store.RequestAggregatesReady()
	if err != nil || !ready {
		return nil, false
	}
	return d.AnalyticsCache, true
}

func cachedGatewayRequestLogSummary(ctx context.Context, d *Deps, filter storage.GatewayRequestLogFilter, bypass bool) (*storage.GatewayRequestLogSummary, error) {
	cache, cacheable := gatewayAnalyticsCacheForFilter(d)
	if !cacheable {
		return d.Gateway.Store.RequestLogSummaryFiltered(filter)
	}

	key := gatewayRequestAnalyticsCacheKey("summary", filter, 0)
	if !bypass {
		if payload, err := cache.GetJSON(ctx, key); err == nil {
			var cached storage.GatewayRequestLogSummary
			if unmarshalGatewayRequestAnalyticsCache(payload, "summary", &cached) {
				return &cached, nil
			}
		}
	}

	value, err, _ := gatewayRequestAnalyticsFlights.Do(gatewayAnalyticsFlightKey(d, key, bypass), func() (any, error) {
		if !bypass {
			if payload, cacheErr := cache.GetJSON(ctx, key); cacheErr == nil {
				var cached storage.GatewayRequestLogSummary
				if unmarshalGatewayRequestAnalyticsCache(payload, "summary", &cached) {
					return payload, nil
				}
			}
		}
		summary, queryErr := d.Gateway.Store.RequestLogSummaryFiltered(filter)
		if queryErr != nil {
			return nil, queryErr
		}
		payload, marshalErr := marshalGatewayRequestAnalyticsCache("summary", summary)
		if marshalErr != nil {
			return nil, marshalErr
		}
		_ = cache.SetJSON(ctx, key, payload, gatewayRequestAnalyticsCacheTTL)
		return payload, nil
	})
	if err != nil {
		return nil, err
	}
	payload, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("analytics summary cache returned %T", value)
	}
	var summary storage.GatewayRequestLogSummary
	if !unmarshalGatewayRequestAnalyticsCache(payload, "summary", &summary) {
		return nil, fmt.Errorf("analytics summary cache envelope invalid")
	}
	return &summary, nil
}

func cachedGatewayRequestLogAnalytics(ctx context.Context, d *Deps, filter storage.GatewayRequestLogFilter, limit int, bypass bool) (*storage.GatewayRequestLogAnalytics, error) {
	cache, cacheable := gatewayAnalyticsCacheForFilter(d)
	if !cacheable {
		return d.Gateway.Store.RequestLogAnalytics(filter, limit)
	}

	key := gatewayRequestAnalyticsCacheKey("analytics", filter, limit)
	if !bypass {
		if payload, err := cache.GetJSON(ctx, key); err == nil {
			var cached storage.GatewayRequestLogAnalytics
			if unmarshalGatewayRequestAnalyticsCache(payload, "analytics", &cached) {
				return &cached, nil
			}
		}
	}

	value, err, _ := gatewayRequestAnalyticsFlights.Do(gatewayAnalyticsFlightKey(d, key, bypass), func() (any, error) {
		if !bypass {
			if payload, cacheErr := cache.GetJSON(ctx, key); cacheErr == nil {
				var cached storage.GatewayRequestLogAnalytics
				if unmarshalGatewayRequestAnalyticsCache(payload, "analytics", &cached) {
					return payload, nil
				}
			}
		}
		analytics, queryErr := d.Gateway.Store.RequestLogAnalytics(filter, limit)
		if queryErr != nil {
			return nil, queryErr
		}
		payload, marshalErr := marshalGatewayRequestAnalyticsCache("analytics", analytics)
		if marshalErr != nil {
			return nil, marshalErr
		}
		_ = cache.SetJSON(ctx, key, payload, gatewayRequestAnalyticsCacheTTL)
		return payload, nil
	})
	if err != nil {
		return nil, err
	}
	payload, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("analytics cache returned %T", value)
	}
	var analytics storage.GatewayRequestLogAnalytics
	if !unmarshalGatewayRequestAnalyticsCache(payload, "analytics", &analytics) {
		return nil, fmt.Errorf("analytics cache envelope invalid")
	}
	return &analytics, nil
}

func gatewayAnalyticsFlightKey(d *Deps, key string, bypass bool) string {
	return fmt.Sprintf("%p:%s:%t", d.Gateway.Store, key, bypass)
}
