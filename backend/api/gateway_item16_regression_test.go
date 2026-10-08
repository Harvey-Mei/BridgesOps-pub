package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

type delayedGatewayKeyGroupManager struct {
	gateway.KeyManager
	delay time.Duration
}

func (m delayedGatewayKeyGroupManager) ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error) {
	timer := time.NewTimer(m.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return m.KeyManager.ListAPIKeyGroups(ctx, channelID)
	}
}

func TestGatewayFixedResponsesPassesCapacityNoticesThrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name   string
		frames string
	}{
		{
			name: "prelude capacity notice",
			frames: "event: response.output_text.delta\n" +
				"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n" +
				"event: response.completed\n" +
				"data: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp-fixed-prelude\",\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{
			name: "capacity notice after acceptance",
			frames: "event: response.created\n" +
				"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp-fixed-late\"}}\n\n" +
				"event: response.output_text.delta\n" +
				"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n" +
				"event: response.completed\n" +
				"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp-fixed-late\",\"status\":\"completed\",\"error\":null}}\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(test.frames))
			}))
			defer upstream.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "fixed", groupName: "fixed", siteURL: upstream.URL, ratio: 1},
			})
			decision.SmartRouting = false
			recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`, false)
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "Selected model is at capacity") {
				t.Fatalf("fixed response = %d %q", recorder.Code, recorder.Body.String())
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("fixed upstream calls = %d, want 1", got)
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
			if err != nil || len(logs) != 1 || logs[0].RouteReason != gatewayRouteReasonManual || logs[0].RouteAttemptedChannels != "" || logs[0].Error != "" {
				t.Fatalf("fixed route audit = %+v, err=%v", logs, err)
			}
		})
	}
}

func TestGatewayImageDispatchStopsRoutingFirstByteTimerWithoutReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls.Add(1)
		time.Sleep(800 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Oneapi-Request-Id", "req-image-delayed")
		_, _ = w.Write([]byte(`{"data":[{"url":"https://example.invalid/image.png"}]}`))
	}))
	defer primary.Close()
	var fallbackCalls atomic.Int32
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"url":"https://example.invalid/fallback.png"}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	const firstByteBudget = 500 * time.Millisecond
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: firstByteBudget}})
	startedAt := time.Now()
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-image-test","prompt":"draw"}`)
	logs, logsErr := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "image.png") {
		t.Fatalf("image response = %d %q calls=%d/%d logs=%+v logs_err=%v", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load(), logs, logsErr)
	}
	if elapsed := time.Since(startedAt); elapsed < firstByteBudget {
		t.Fatalf("image response returned in %s before delayed upstream completed", elapsed)
	}
	if primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
		t.Fatalf("image upstream calls = %d/%d, want 1/0", primaryCalls.Load(), fallbackCalls.Load())
	}
	if logsErr != nil || len(logs) != 1 || logs[0].StatusCode != http.StatusOK || !logs[0].BillingCommitted || logs[0].ChannelID != 1 || logs[0].UpstreamRequestID != "req-image-delayed" {
		t.Fatalf("delayed image billing audit = %+v, err=%v", logs, logsErr)
	}
}

func TestGatewayImagePreparationStillUsesRoutingFirstByteBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		_, _ = w.Write([]byte(`{"data":[{"url":"unexpected"}]}`))
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "preparation", groupName: "preparation", siteURL: upstream.URL, ratio: 1},
	})
	key, err := store.FindUpstreamKeyAny(1, "preparation")
	if err != nil {
		t.Fatalf("find cached key: %v", err)
	}
	key.Enabled = false
	if err := store.UpsertUpstreamKey(key); err != nil {
		t.Fatalf("disable cached key: %v", err)
	}
	svc.Keys = delayedGatewayKeyGroupManager{KeyManager: svc.Keys, delay: 120 * time.Millisecond}
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: 40 * time.Millisecond}})

	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-image-test","prompt":"draw"}`)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("pre-dispatch image response = %d %q, want 502", recorder.Code, recorder.Body.String())
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("pre-dispatch image upstream calls = %d, want 0", got)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 || logs[0].BillingCommitted || logs[0].UpstreamAccepted {
		t.Fatalf("pre-dispatch image audit = %+v, err=%v", logs, err)
	}
}
