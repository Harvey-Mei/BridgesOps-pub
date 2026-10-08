package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

func TestGatewayPreparationRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "temporary http", err: connector.HTTPStatusError(http.StatusServiceUnavailable, nil), want: true},
		{name: "too early", err: connector.HTTPStatusError(http.StatusTooEarly, nil), want: true},
		{name: "authentication", err: connector.HTTPStatusError(http.StatusUnauthorized, nil), want: false},
		{name: "permanent configuration", err: errors.New("unsupported protocol scheme"), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryableGatewayPreparationError(context.Background(), tc.err); got != tc.want {
				t.Fatalf("retryable = %t, want %t for %v", got, tc.want, tc.err)
			}
		})
	}
}

func TestGatewayPreparationRetryClassificationDistinguishesProviderCancellation(t *testing.T) {
	if !isRetryableGatewayPreparationError(context.Background(), context.Canceled) {
		t.Fatal("provider cancellation should be retryable while the downstream request is alive")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if isRetryableGatewayPreparationError(canceled, context.Canceled) {
		t.Fatal("downstream cancellation must not be retryable")
	}
}

func runGatewayRetryTestRequest(svc *gateway.Service, decision *gatewayRouteDecision, path, body string) *httptest.ResponseRecorder {
	return runGatewayRetryTestRequestWithMode(svc, decision, path, body, true)
}

func runGatewayRetryTestRequestWithMode(svc *gateway.Service, decision *gatewayRouteDecision, path, body string, smartRouting bool) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	mapping := newGatewayModelMappingResult("gpt-test", "", []byte(body))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: smartRouting}, decision, "gpt-test", "", []byte(body), mapping, time.Now())
	return recorder
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func TestGatewayForwardManualRouteWaitsForSlowFirstOutputAndClearsAdaptiveHealth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	calls := 0
	catalogCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/models" {
			catalogCalls++
			_, _ = w.Write([]byte(`{"data":[{"id":"claude-stale"}]}`))
			return
		}
		calls++
		time.Sleep(120 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"manual route completed\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_manual_slow\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1},
	})
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: 40 * time.Millisecond}})
	decision.SmartRouting = false
	if err := store.UpdateUpstreamKeyModelFamilies(1, "manual", "claude", "claude-stale", time.Now(), "", gatewayCandidateGenerationArgs(decision.Candidates[0])...); err != nil {
		t.Fatalf("seed incompatible cached model facts: %v", err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(1, "manual", gateway.RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "existing smart failure", time.Now(), gatewayCandidateGenerationArgs(decision.Candidates[0])...); err != nil {
		t.Fatalf("seed existing smart-routing health: %v", err)
	}
	startedAt := time.Now()
	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`, false)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "manual route completed") || calls != 1 || catalogCalls != 0 {
		t.Fatalf("manual response=%d %q calls=%d catalog_calls=%d", recorder.Code, recorder.Body.String(), calls, catalogCalls)
	}
	if elapsed := time.Since(startedAt); elapsed < 100*time.Millisecond {
		t.Fatalf("manual route returned in %s before the slow upstream response", elapsed)
	}
	after, err := store.FindUpstreamKeyAny(1, "manual")
	if err != nil || after.LastRoutingFailureKind != "" || after.LastRoutingFailureStatus != 0 || after.RoutingFailureCount != 0 || after.LastRoutingFailureAt != nil || after.RoutingCooldownUntil != nil || after.LastUsedAt == nil || after.LastUsedAt.Before(startedAt) {
		t.Fatalf("manual slow route did not clear adaptive health at success: after=%+v err=%v", after, err)
	}
}

func TestGatewayForwardManualRouteUsesLocalLeaseWithoutIdentityRepair(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1},
	})
	decision.SmartRouting = false
	key, err := store.FindUpstreamKeyAny(1, "manual")
	if err != nil {
		t.Fatalf("read upstream key: %v", err)
	}
	key.UpstreamKeyID = 0
	key.UpstreamKeyName = ""
	if err := store.UpsertUpstreamKey(key); err != nil {
		t.Fatalf("clear upstream identity: %v", err)
	}
	manager := svc.Keys.(gatewayProxyTestKeyManager)
	manager.listCalls = &atomic.Int32{}
	manager.groupCalls = &atomic.Int32{}
	manager.createCalls = &atomic.Int32{}
	svc.Keys = manager

	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test"}`, false)
	if recorder.Code != http.StatusOK || calls.Load() != 1 || !strings.Contains(recorder.Body.String(), `"ok":true`) {
		t.Fatalf("manual local-lease response=%d body=%q upstream_calls=%d", recorder.Code, recorder.Body.String(), calls.Load())
	}
	if manager.listCalls.Load() != 0 || manager.groupCalls.Load() != 0 || manager.createCalls.Load() != 0 {
		t.Fatalf("fixed route invoked control plane: list=%d groups=%d create=%d", manager.listCalls.Load(), manager.groupCalls.Load(), manager.createCalls.Load())
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 || logs[0].UpstreamKeyID != 0 || strings.TrimSpace(logs[0].UpstreamKeyName) != "" {
		t.Fatalf("fixed local-lease identity audit = %+v err=%v", logs, err)
	}
}

func TestGatewayForwardAPIKeyTestSuccessDoesNotClearAdaptiveHealth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"test","choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "test", groupName: "test", siteURL: upstream.URL, ratio: 1,
	}})
	if err := store.RecordUpstreamKeyRoutingFailure(1, "test", gateway.RoutingFailureTransport, 0, "existing failure", time.Now()); err != nil {
		t.Fatalf("seed adaptive health: %v", err)
	}
	before, err := store.FindUpstreamKeyAny(1, "test")
	if err != nil {
		t.Fatalf("read adaptive health: %v", err)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := `{"model":"gpt-test"}`
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
	ctx.Set(gatewayDownstreamKeyTestContextKey, true)
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(body), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	after, err := store.FindUpstreamKeyAny(1, "test")
	if recorder.Code != http.StatusOK || err != nil || after.LastRoutingFailureKind != before.LastRoutingFailureKind || after.LastRoutingFailureStatus != before.LastRoutingFailureStatus || after.RoutingFailureCount != before.RoutingFailureCount || !sameOptionalTime(after.LastRoutingFailureAt, before.LastRoutingFailureAt) || !sameOptionalTime(after.RoutingCooldownUntil, before.RoutingCooldownUntil) {
		t.Fatalf("API key test changed adaptive health: status=%d before=%+v after=%+v err=%v", recorder.Code, before, after, err)
	}
}

func TestGatewayForwardManualRouteDoesNotRetryOrRecordSmartFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1},
	})
	decision.SmartRouting = false
	if err := store.RecordUpstreamKeyRoutingFailure(1, "manual", gateway.RoutingFailureTransport, 0, "existing smart failure", time.Now()); err != nil {
		t.Fatalf("seed existing smart-routing health: %v", err)
	}
	before, err := store.FindUpstreamKeyAny(1, "manual")
	if err != nil {
		t.Fatalf("read existing smart-routing health: %v", err)
	}
	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`, false)

	if recorder.Code != http.StatusBadGateway || calls != 1 || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
		t.Fatalf("manual failure=%d %q calls=%d", recorder.Code, recorder.Body.String(), calls)
	}
	after, err := store.FindUpstreamKeyAny(1, "manual")
	if err != nil || after.LastRoutingFailureKind != before.LastRoutingFailureKind || after.LastRoutingFailureStatus != before.LastRoutingFailureStatus || after.RoutingFailureCount != before.RoutingFailureCount || !sameOptionalTime(after.LastRoutingFailureAt, before.LastRoutingFailureAt) || !sameOptionalTime(after.RoutingCooldownUntil, before.RoutingCooldownUntil) {
		t.Fatalf("manual failure changed smart-routing health: before=%+v after=%+v err=%v", before, after, err)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if err != nil || len(logs) != 1 || logs[0].ChannelID != 1 || logs[0].GroupName != "manual" || logs[0].UpstreamKeyID != 1 || logs[0].UpstreamKeyName != "BOG test" || logs[0].RouteSelectionRank != 1 {
		t.Fatalf("manual failure route audit = %+v err=%v", logs, err)
	}
	if logs[0].RouteReason != gatewayRouteReasonManual || logs[0].RouteAttemptedChannels != "" || strings.Contains(logs[0].RouteDetails, "attempt failed") || !strings.Contains(logs[0].RouteDetails, "selected manual/manual") {
		t.Fatalf("manual failure was recorded as candidate fallback: %+v", logs[0])
	}
}

func TestGatewayForwardManualRouteUsesOnlyFirstConfiguredCandidate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	firstCalls := 0
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		firstCalls++
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer first.Close()
	secondCalls := 0
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls++
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer second.Close()

	svc, _, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "manual-first", groupName: "manual-first", siteURL: first.URL, ratio: 1},
		{channelID: 2, name: "manual-second", groupName: "manual-second", siteURL: second.URL, ratio: 1},
	})
	decision.SmartRouting = false
	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test"}`, false)

	if recorder.Code != http.StatusBadGateway || firstCalls != 1 || secondCalls != 0 {
		t.Fatalf("manual multi-candidate response=%d first_calls=%d second_calls=%d body=%q", recorder.Code, firstCalls, secondCalls, recorder.Body.String())
	}
}

func TestGatewayForwardManualRouteDoesNotFollowReplayRedirect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	initialCalls := 0
	redirectedCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			initialCalls++
			http.Redirect(w, r, "/replayed", http.StatusTemporaryRedirect)
		case "/replayed":
			redirectedCalls++
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	svc, _, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1},
	})
	decision.SmartRouting = false
	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test"}`, false)

	if recorder.Code != http.StatusBadGateway || initialCalls != 1 || redirectedCalls != 0 {
		t.Fatalf("manual redirect response=%d initial_calls=%d redirected_calls=%d body=%q", recorder.Code, initialCalls, redirectedCalls, recorder.Body.String())
	}
}

func TestGatewayForwardManualClaudeRouteSkipsContextCountPreflight(t *testing.T) {
	gin.SetMode(gin.TestMode)
	messageCalls := 0
	countCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/messages/count_tokens":
			countCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"input_tokens":999}`))
		case "/v1/messages":
			messageCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"content":[{"type":"text","text":"manual direct"}],"usage":{"input_tokens":1,"output_tokens":1}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1},
	})
	decision.SmartRouting = false
	decision.Candidates[0].App = storage.GatewayAppClaude
	if err := store.ObserveModelContextLimit(1, "manual", "claude-opus", 0, 1, 0, time.Now(), gatewayCandidateGenerationArgs(decision.Candidates[0])...); err != nil {
		t.Fatalf("seed known context budget: %v", err)
	}
	body := `{"model":"claude-opus","max_tokens":1,"messages":[{"role":"user","content":"this body is deliberately larger than the known safe input budget"}]}`
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppClaude, SmartRouting: false}, decision, "claude-opus", "", []byte(body), gatewayModelMappingResult{Model: "claude-opus"}, time.Now())

	if recorder.Code != http.StatusOK || messageCalls != 1 || countCalls != 0 || !strings.Contains(recorder.Body.String(), "manual direct") {
		t.Fatalf("manual Claude response=%d message_calls=%d count_calls=%d body=%q", recorder.Code, messageCalls, countCalls, recorder.Body.String())
	}
}

func TestGatewayForwardManualRouteStillInvalidatesRejectedKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		http.Error(w, "invalid key", http.StatusUnauthorized)
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1},
	})
	decision.SmartRouting = false
	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test"}`, false)

	key, err := store.FindUpstreamKeyAny(1, "manual")
	if recorder.Code != http.StatusBadGateway || calls != 1 || err != nil || key.Enabled || strings.TrimSpace(key.APIKeyCipher) != "" {
		t.Fatalf("manual auth rejection response=%d calls=%d key=%+v err=%v body=%q", recorder.Code, calls, key, err, recorder.Body.String())
	}
}

func TestGatewayForwardManualRouteStopsWhenClientCancels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var calls atomic.Int32
	dispatched := make(chan struct{}, 1)
	upstreamCanceled := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Oneapi-Request-Id", "req-canceled-accepted")
		_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_thinking\"}}\n\n"))
		w.(http.Flusher).Flush()
		dispatched <- struct{}{}
		<-r.Context().Done()
		upstreamCanceled <- struct{}{}
	}))
	defer upstream.Close()

	db := openTestDB(t)
	svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{
		{channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1},
	})
	decision.SmartRouting = false
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	requestContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := `{"model":"gpt-test","stream":true}`
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body)).WithContext(requestContext)
	done := make(chan struct{})
	go func() {
		gatewayForward(ginContext, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: false}, decision, "gpt-test", "", []byte(body), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())
		close(done)
	}()

	select {
	case <-dispatched:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("manual request was not dispatched within 1s")
	}
	acceptanceObservationDeadline := time.Now().Add(3 * time.Second)
	for {
		var accepted int64
		if err := db.Model(&storage.GatewayRequestDispatchAudit{}).
			Where("provider_request_id = ? AND response_observed_at IS NOT NULL AND accepted_at IS NOT NULL AND accepted_event_type = ? AND accepted_object_id = ?", "req-canceled-accepted", "response.created", "resp_thinking").
			Count(&accepted).Error; err != nil {
			t.Fatalf("query manual acceptance observation: %v", err)
		}
		if accepted == 1 {
			break
		}
		if time.Now().After(acceptanceObservationDeadline) {
			var audits []storage.GatewayRequestDispatchAudit
			_ = db.Order("id").Find(&audits).Error
			t.Fatalf("manual response acceptance was not persisted before cancellation: %+v", audits)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manual request did not stop within 1s after client cancellation")
	}
	select {
	case <-upstreamCanceled:
	case <-time.After(time.Second):
		t.Fatal("upstream request context was not canceled within 1s")
	}

	if calls.Load() != 1 {
		t.Fatalf("manual canceled request calls = %d, want 1", calls.Load())
	}
	key, err := store.FindUpstreamKey(1, "manual")
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("manual cancellation changed smart-routing health: %+v err=%v", key, err)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if err != nil || len(logs) != 1 {
		t.Fatalf("manual cancellation logs = %+v err=%v", logs, err)
	}
	if logs[0].StatusCode != gatewayClientClosedStatus || logs[0].RouteReason != gatewayRouteReasonClientCanceled || logs[0].Error != "客户端取消" || logs[0].ChannelID != 1 || logs[0].GroupName != "manual" || !logs[0].UpstreamAccepted || logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-canceled-accepted" {
		t.Fatalf("manual cancellation log = %+v", logs[0])
	}
}

func TestGatewayForwardKeepsCandidateWhenThirdAttemptSucceeds(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		if primaryCalls < gatewayCandidateRetryAttempts {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"primary recovered\"}\n\n"))
	}))
	defer primary.Close()

	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"fallback\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: 2 * time.Second}})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "primary recovered") || primaryCalls != gatewayCandidateRetryAttempts || fallbackCalls != 0 {
		t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("recovered candidate retained failure state: %+v", key)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 || logs[0].ChannelID != 1 || logs[0].RouteReason != gatewayRouteReasonSmartPrimary || logs[0].RouteAttemptedChannels != "" {
		t.Fatalf("recovered request log = %+v err=%v", logs, err)
	}
}

func TestGatewayForwardAllowsSlowResponseWithinCandidateBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"slow but healthy\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_slow_healthy\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"unexpected fallback\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_slow_fallback\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "slow healthy", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: 500 * time.Millisecond}})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "slow but healthy") || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("slow response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil {
		t.Fatalf("slow healthy route failure state=%+v err=%v", key, err)
	}
}

func TestGatewayForwardManualRouteDoesNotApplyFirstByteTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		time.Sleep(80 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"manual route completed\"}\n\n"))
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1,
	}})
	decision.SmartRouting = false
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: 20 * time.Millisecond}})

	startedAt := time.Now()
	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`, false)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "manual route completed") || calls != 1 {
		t.Fatalf("manual response=%d %q calls=%d", recorder.Code, recorder.Body.String(), calls)
	}
	if elapsed := time.Since(startedAt); elapsed < 60*time.Millisecond {
		t.Fatalf("manual route returned after %s, want it to wait for the slow first output", elapsed)
	}
	key, err := store.FindUpstreamKey(1, "manual")
	if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
		t.Fatalf("manual slow response changed routing health: key=%+v err=%v", key, err)
	}
}

func TestGatewayForwardTransientStatusExhaustionCoolsAndFallsBack(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer primary.Close()

	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"fallback succeeded\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback succeeded") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || primaryCalls != gatewayCandidateRetryAttempts || fallbackCalls != 1 {
		t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.LastRoutingFailureAt == nil || key.LastRoutingFailureStatus != http.StatusServiceUnavailable || key.RoutingFailureCount != gatewayCandidateRetryAttempts || key.RoutingCooldownUntil != nil {
		t.Fatalf("confirmed failure = %+v err=%v", key, err)
	}
}

func TestGatewayForwardProviderCancellationExhaustionCoolsAndFallsBack(t *testing.T) {
	originalClient := gatewayHTTPClient
	t.Cleanup(func() { gatewayHTTPClient = originalClient })

	var primaryCalls, fallbackCalls atomic.Int32
	gatewayHTTPClient = &http.Client{Transport: gatewayTestRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Host {
		case "primary.example":
			primaryCalls.Add(1)
			return nil, context.Canceled
		case "fallback.example":
			fallbackCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader("data: {\"delta\":\"unexpected fallback\"}\n\n")),
			}, nil
		default:
			return nil, fmt.Errorf("unexpected test host %q", request.URL.Host)
		}
	})}

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: "https://primary.example", ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: "https://fallback.example", ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "unexpected fallback") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || primaryCalls.Load() != gatewayCandidateRetryAttempts || fallbackCalls.Load() != 1 {
		t.Fatalf("provider cancellation response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != gatewayCandidateRetryAttempts || key.LastRoutingFailureKind != gateway.RoutingFailureTransport || key.RoutingCooldownUntil != nil {
		t.Fatalf("provider cancellation routing health=%+v err=%v", key, err)
	}
}

func TestGatewayForwardRouteConnectionFailureCanFallback(t *testing.T) {
	originalClient := gatewayHTTPClient
	t.Cleanup(func() { gatewayHTTPClient = originalClient })

	var primaryCalls, fallbackCalls atomic.Int32
	gatewayHTTPClient = &http.Client{Transport: gatewayTestRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Host {
		case "unreachable.example":
			primaryCalls.Add(1)
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		case "healthy.example":
			fallbackCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader("data: {\"delta\":\"fallback after route failure\"}\n\n")),
			}, nil
		default:
			return nil, fmt.Errorf("unexpected test host %q", request.URL.Host)
		}
	})}

	svc, _, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "unreachable", groupName: "unreachable", siteURL: "https://unreachable.example", ratio: 0.1},
		{channelID: 2, name: "healthy", groupName: "healthy", siteURL: "https://healthy.example", ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback after route failure") || primaryCalls.Load() != gatewayCandidateRetryAttempts || fallbackCalls.Load() != 1 {
		t.Fatalf("route failure response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
	}
}

func TestGatewayForwardStreamReadErrorExhaustionCoolsAndFallsBack(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"stream_read_error\",\"message\":\"private read detail\"}}\n\n"))
	}))
	defer primary.Close()
	var fallbackCalls atomic.Int32
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"unexpected fallback\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_stream_read_fallback\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "unexpected fallback") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || strings.Contains(recorder.Body.String(), "private read detail") || primaryCalls.Load() != gatewayCandidateRetryAttempts || fallbackCalls.Load() != 1 {
		t.Fatalf("stream_read_error response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 4)
	if err != nil || len(logs) != 1 || logs[0].StatusCode != http.StatusOK || logs[0].RouteReason != gatewayRouteReasonFallback || logs[0].Error != "" || !strings.Contains(logs[0].RouteDetails, "private read detail") {
		t.Fatalf("stream_read_error audit=%+v err=%v", logs, err)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != gatewayCandidateRetryAttempts || key.RoutingCooldownUntil != nil {
		t.Fatalf("stream_read_error routing health=%+v err=%v", key, err)
	}
}

func TestGatewayForwardDoesNotCallPreparationFailureFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"healthy after preparation failure\"}\n\n"))
	}))
	defer upstream.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "bad-target", groupName: "bad-target", siteURL: "://not-a-url", ratio: 0.1},
		{channelID: 2, name: "healthy", groupName: "healthy", siteURL: upstream.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", "{}")
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "healthy after preparation failure") {
		t.Fatalf("response=%d %q", recorder.Code, recorder.Body.String())
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("request logs=%+v err=%v", logs, err)
	}
	if logs[0].RouteReason != gatewayRouteReasonSmartPrimary || logs[0].RouteAttemptedChannels != "" {
		t.Fatalf("preparation failure was treated as fallback: %+v", logs[0])
	}
}

func TestGatewayForwardDoesNotReplayPermanentStatusWithTruncatedBody(t *testing.T) {
	for _, tc := range []struct {
		name           string
		status         int
		invalidatesKey bool
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, invalidatesKey: true},
		{name: "forbidden", status: http.StatusForbidden, invalidatesKey: true},
		{name: "payment required", status: http.StatusPaymentRequired},
		{name: "bad request", status: http.StatusBadRequest},
		{name: "payload too large", status: http.StatusRequestEntityTooLarge},
		{name: "unprocessable entity", status: http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			primaryCalls := 0
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls++
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", "128")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":`))
			}))
			defer primary.Close()
			fallbackCalls := 0
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls++
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {\"delta\":\"fallback\"}\n\n"))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "permanent", groupName: "permanent", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback") || primaryCalls != 1 || fallbackCalls != 1 {
				t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
			}
			key, err := store.FindUpstreamKeyAny(1, "permanent")
			if err != nil {
				t.Fatal(err)
			}
			if key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
				t.Fatalf("permanent status changed routing health: %+v", key)
			}
			if key.Enabled == tc.invalidatesKey {
				t.Fatalf("key enabled=%t, invalidates=%t", key.Enabled, tc.invalidatesKey)
			}
			if tc.status == http.StatusUnprocessableEntity {
				logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
				if err != nil || len(logs) != 1 || !strings.Contains(logs[0].RouteAttemptedChannels, `"status_code":422`) {
					t.Fatalf("422 fallback audit = %+v err=%v", logs, err)
				}
			}
		})
	}
}

func TestGatewayForwardDoesNotReplayImageGeneration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		http.Error(w, "sensitive image upstream failure", http.StatusServiceUnavailable)
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte(`{"data":[{"url":"https://example.invalid/image.png"}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-test","prompt":"test"}`)
	if recorder.Code != http.StatusBadGateway || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("image response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	if !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) || strings.Contains(recorder.Body.String(), "sensitive") || strings.Contains(recorder.Body.String(), "primary") {
		t.Fatalf("image response leaked upstream details: %q", recorder.Body.String())
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.LastRoutingFailureKind != gateway.RoutingFailureHTTPStatus || key.LastRoutingFailureStatus != http.StatusServiceUnavailable || key.RoutingFailureCount != 1 {
		t.Fatalf("single dispatched image failure was not recorded once: %+v err=%v", key, err)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if err != nil || len(logs) != 1 || logs[0].ChannelID != 1 || logs[0].Path != "/v1/images/generations" || logs[0].StatusCode != http.StatusBadGateway {
		t.Fatalf("image failure log = %+v err=%v", logs, err)
	}
}

func TestGatewayForwardCommittedImageTransientStatusWithTruncatedBodyMarksHTTPFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "256")
		w.Header().Set("X-Oneapi-Request-Id", "req-image-truncated-503")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"message":"truncated transient response"}`))
	}))
	defer primary.Close()

	var fallbackCalls atomic.Int32
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls.Add(1)
		_, _ = w.Write([]byte(`{"data":[{"url":"unexpected"}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-test","prompt":"test"}`)
	if recorder.Code != http.StatusBadGateway || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
		t.Fatalf("truncated image 503 response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 1 || key.LastRoutingFailureKind != gateway.RoutingFailureHTTPStatus || key.LastRoutingFailureStatus != http.StatusServiceUnavailable {
		t.Fatalf("truncated image 503 routing health=%+v err=%v", key, err)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if err != nil || len(logs) != 1 || !logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-image-truncated-503" {
		t.Fatalf("truncated image 503 terminal log=%+v err=%v", logs, err)
	}
}

func TestGatewayForwardStructuredTransientExhaustionCoolsAndFallsBack(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":{"type":"overloaded_error","message":"temporarily overloaded"}}`))
	}))
	defer primary.Close()

	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"fallback","choices":[{"message":{"content":"fallback succeeded"}}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":false}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback succeeded") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || primaryCalls != gatewayCandidateRetryAttempts || fallbackCalls != 1 {
		t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != gatewayCandidateRetryAttempts || key.LastRoutingFailureKind != gateway.RoutingFailureTransport || key.LastRoutingFailureStatus != http.StatusOK || key.RoutingCooldownUntil != nil {
		t.Fatalf("structured transient failure state=%+v err=%v", key, err)
	}
}

func TestGatewayForwardPassesThroughStructuredNonStreamingNullError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"completed","error":null,"choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer primary.Close()

	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte(`{"unexpected":true}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/chat/completions", `{"model":"gpt-test","messages":[]}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"error":null`) || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
		t.Fatalf("null error changed routing health: %+v err=%v", key, err)
	}
}

func TestGatewayForwardFallsBackAfterStructuredNonStreamingPermanentError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error","message":"invalid request"}}`))
	}))
	defer primary.Close()

	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"fallback","choices":[{"message":{"content":"fallback succeeded"}}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/chat/completions", `{"model":"gpt-test","messages":[]}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback succeeded") || primaryCalls != 1 || fallbackCalls != 1 {
		t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
		t.Fatalf("structured permanent error changed routing health: %+v err=%v", key, err)
	}
}

func TestGatewayForwardUsesOuterPermanentStatusBeforeNonStreamingErrorBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":{"type":"overloaded_error","message":"not replayable"}}`))
	}))
	defer primary.Close()

	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"fallback","choices":[{"message":{"content":"fallback succeeded"}}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/chat/completions", `{"model":"gpt-test","messages":[]}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback succeeded") || primaryCalls != 1 || fallbackCalls != 1 {
		t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
		t.Fatalf("outer permanent status changed routing health: %+v err=%v", key, err)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("outer permanent logs=%+v err=%v", logs, err)
	}
	var traces []gatewayRouteAttemptTrace
	if err := json.Unmarshal([]byte(logs[0].RouteAttemptedChannels), &traces); err != nil || len(traces) != 1 || traces[0].Reason != "upstream_status" || traces[0].StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("outer permanent attempts=%q traces=%+v err=%v", logs[0].RouteAttemptedChannels, traces, err)
	}
}

func TestGatewayForwardStructuredNonStreamingAuthenticationInvalidatesKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":"invalid_api_key"}`))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"fallback","choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/chat/completions", `{"model":"gpt-test","messages":[]}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "ok") {
		t.Fatalf("response=%d %q", recorder.Code, recorder.Body.String())
	}
	key, err := store.FindUpstreamKeyAny(1, "primary")
	if err != nil || key.Enabled {
		t.Fatalf("authentication error did not invalidate key: %+v err=%v", key, err)
	}
	if key.RoutingFailureCount != 0 {
		t.Fatalf("authentication error polluted routing health: %+v", key)
	}
}

func TestGatewayStructuredFailureInvalidatesOnlyExplicitKeyErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want bool
	}{
		{name: "nested unauthorized status", raw: `{"error":{"status_code":401}}`, want: true},
		{name: "nested forbidden status", raw: `{"response":{"status":"failed","error":{"status":403}}}`, want: true},
		{name: "authentication type", raw: `{"error":{"type":"authentication_error"}}`, want: true},
		{name: "string authentication error", raw: `{"error":"authentication_error"}`, want: true},
		{name: "string invalid key error", raw: `{"error":"invalid_api_key"}`, want: true},
		{name: "message only invalid key", raw: `{"error":{"message":"the API key is invalid"}}`, want: true},
		{name: "model permission is not a key error", raw: `{"error":{"type":"model_forbidden","message":"model access is forbidden"}}`, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classification := classifyGatewayStreamFailure([]byte(tc.raw))
			if !classification.Found {
				t.Fatalf("classification=%+v", classification)
			}
			if got := gatewayStructuredFailureInvalidatesKey([]byte(tc.raw), classification); got != tc.want {
				t.Fatalf("invalidates key=%t, want %t for %+v", got, tc.want, classification)
			}
		})
	}
}

func TestGatewayForwardStructuredSSEAuthenticationInvalidatesKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":\"authentication_error\"}\n\n"))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"fallback succeeded\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback succeeded") {
		t.Fatalf("response=%d %q", recorder.Code, recorder.Body.String())
	}
	key, err := store.FindUpstreamKeyAny(1, "primary")
	if err != nil || key.Enabled {
		t.Fatalf("SSE authentication error did not invalidate key: %+v err=%v", key, err)
	}
	if key.RoutingFailureCount != 0 {
		t.Fatalf("SSE authentication error polluted routing health: %+v", key)
	}
}

func TestGatewayForwardAppliesPostOutputSSEFactsWithoutReplayOrCooldown(t *testing.T) {
	for _, tc := range []struct {
		name             string
		failure          string
		invalidatesKey   bool
		modelUnsupported bool
	}{
		{
			name:           "authentication",
			failure:        `{"type":"error","error":{"type":"invalid_api_key"}}`,
			invalidatesKey: true,
		},
		{
			name:             "model not found",
			failure:          `{"type":"error","error":{"code":"model_not_found"}}`,
			modelUnsupported: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial output\"}\n\n"))
				_, _ = w.Write([]byte("event: error\ndata: " + tc.failure + "\n\n"))
			}))
			defer upstream.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
				channelID: 1, name: "fixed", groupName: "fixed", siteURL: upstream.URL, ratio: 1,
			}})
			decision.SmartRouting = false
			recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`, false)
			if recorder.Code != http.StatusOK || calls.Load() != 1 || !strings.Contains(recorder.Body.String(), "partial output") || !strings.Contains(recorder.Body.String(), gatewayPublicStreamFailureMessage) {
				t.Fatalf("fixed post-output response=%d %q calls=%d", recorder.Code, recorder.Body.String(), calls.Load())
			}

			key, err := store.FindUpstreamKeyAny(1, "fixed")
			if err != nil || key.Enabled == tc.invalidatesKey || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
				t.Fatalf("fixed post-output key state=%+v err=%v", key, err)
			}
			capability, capabilityErr := store.FindModelCapability(1, "fixed", "gpt-test")
			if tc.modelUnsupported {
				if capabilityErr != nil || capability == nil || capability.Supported {
					t.Fatalf("post-output model evidence=%+v err=%v", capability, capabilityErr)
				}
			} else if capabilityErr == nil && capability != nil && !capability.Supported {
				t.Fatalf("authentication error changed model capability: %+v", capability)
			}
		})
	}
}

func TestGatewayForwardStructuredNonStreamingModelErrorMarksCapability(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":{"type":"model_not_found","message":"model is not supported by this account"}}`))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"fallback","choices":[{"message":{"content":"ok"}}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/chat/completions", `{"model":"gpt-test","messages":[]}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "ok") {
		t.Fatalf("response=%d %q", recorder.Code, recorder.Body.String())
	}
	capability, err := store.FindModelCapability(1, "primary", "gpt-test")
	if err != nil || capability == nil || capability.Supported {
		t.Fatalf("model error did not mark unsupported capability: %+v err=%v", capability, err)
	}
}

func TestGatewayImageStructuredNonStreamingTransientErrorDoesNotReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Oneapi-Request-Id", "req-image-committed")
		_, _ = w.Write([]byte(`{"error":{"type":"overloaded_error","message":"image task overloaded"},"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`))
	}))
	defer primary.Close()

	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte(`{"data":[{"url":"https://example.invalid/image.png"}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-image-test","prompt":"test"}`)
	if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) || strings.Contains(recorder.Body.String(), "image task overloaded") || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 1 || key.LastRoutingFailureKind != gateway.RoutingFailureTransport || key.LastRoutingFailureStatus != http.StatusOK {
		t.Fatalf("committed image structured failure state=%+v err=%v", key, err)
	}
	healthFence, ok := gateway.RoutingHealthFenceForCandidate(decision.Candidates[0])
	if !ok || !healthFence.MatchesUpstreamKey(key) {
		t.Fatalf("committed image structured failure fence=%+v key=%+v", healthFence, key)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("committed image logs=%+v err=%v", logs, err)
	}
	if logs[0].UpstreamAccepted || !logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-image-committed" || logs[0].TotalTokens != 4 {
		t.Fatalf("committed image billing facts=%+v", logs[0])
	}
	var traces []gatewayRouteAttemptTrace
	if err := json.Unmarshal([]byte(logs[0].RouteAttemptedChannels), &traces); err != nil || len(traces) != 1 || traces[0].ChannelID != 1 {
		t.Fatalf("committed image attempts=%q traces=%+v err=%v", logs[0].RouteAttemptedChannels, traces, err)
	}
}

func TestGatewayImagePreparationFailureCanUseNextCandidate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"url":"https://example.invalid/image.png"}]}`))
	}))
	defer fallback.Close()

	svc, _, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "invalid target", groupName: "invalid", siteURL: "://invalid", ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-image-test","prompt":"test"}`)
	if recorder.Code != http.StatusOK || fallbackCalls != 1 || !strings.Contains(recorder.Body.String(), "image.png") {
		t.Fatalf("image preparation fallback=%d %q calls=%d", recorder.Code, recorder.Body.String(), fallbackCalls)
	}
}

func TestGatewayImageDispatchSkipsCanceledPreparation(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"prompt":"test"}`))
	ctx, cancel := context.WithCancel(request.Context())
	cancel()
	request = request.WithContext(ctx)
	dispatches := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatalf("canceled image request reached upstream")
	}))
	defer server.Close()
	_, err := forwardGatewayRequestOnDispatch(request, gatewayResolvedTarget{SiteURL: server.URL}, "sk-test", []byte(`{"prompt":"test"}`), func() error {
		dispatches++
		return nil
	}, nil, nil)
	if !errors.Is(err, context.Canceled) || dispatches != 0 {
		t.Fatalf("canceled dispatch err=%v callbacks=%d", err, dispatches)
	}
}

func TestGatewayCommittedImageCancelPreservesBillingRouteAndRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls, fallbackCalls atomic.Int32
	dispatched := make(chan struct{}, 1)
	upstreamCanceled := make(chan struct{}, 1)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "512")
		w.Header().Set("X-Oneapi-Request-Id", "req-image-canceled")
		_, _ = w.Write([]byte(`{"data":[`))
		w.(http.Flusher).Flush()
		dispatched <- struct{}{}
		<-r.Context().Done()
		upstreamCanceled <- struct{}{}
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls.Add(1)
		_, _ = w.Write([]byte(`{"data":[{"url":"unexpected"}]}`))
	}))
	defer fallback.Close()

	db := openTestDB(t)
	svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	requestContext, cancel := context.WithCancel(context.Background())
	body := `{"model":"gpt-image-test","prompt":"test"}`
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(body)).WithContext(requestContext)
	defer cancel()
	done := make(chan struct{})
	go func() {
		gatewayForward(ginContext, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-image-test", "", []byte(body), gatewayModelMappingResult{Model: "gpt-image-test"}, time.Now())
		close(done)
	}()
	select {
	case <-dispatched:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("image request did not reach the committed response body")
	}
	responseObservationDeadline := time.Now().Add(3 * time.Second)
	for {
		var observed int64
		if err := db.Model(&storage.GatewayRequestDispatchAudit{}).
			Where("provider_request_id = ? AND response_observed_at IS NOT NULL", "req-image-canceled").
			Count(&observed).Error; err != nil {
			t.Fatalf("query committed image response observation: %v", err)
		}
		if observed == 1 {
			break
		}
		if time.Now().After(responseObservationDeadline) {
			var audits []storage.GatewayRequestDispatchAudit
			_ = db.Order("id").Find(&audits).Error
			t.Fatalf("committed image response identity was not persisted before cancellation: %+v", audits)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("committed image request did not stop after cancellation")
	}
	select {
	case <-upstreamCanceled:
	case <-time.After(time.Second):
		t.Fatal("committed image upstream was not canceled")
	}
	if primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
		t.Fatalf("committed image cancel calls=%d/%d", primaryCalls.Load(), fallbackCalls.Load())
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("committed image cancel logs=%+v err=%v", logs, err)
	}
	if logs[0].StatusCode != gatewayClientClosedStatus || logs[0].RouteReason != gatewayRouteReasonClientCanceled || logs[0].ChannelID != 1 || logs[0].GroupName != "primary" || logs[0].UpstreamAccepted || !logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-image-canceled" {
		t.Fatalf("committed image cancel log=%+v", logs[0])
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("committed image cancel changed routing health: key=%+v err=%v", key, err)
	}
}

func TestGatewayImageRequestDoesNotFollowRedirect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	redirectTargetCalls := 0
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectTargetCalls++
		_, _ = w.Write([]byte(`{"data":[{"url":"redirected"}]}`))
	}))
	defer redirectTarget.Close()
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Location", redirectTarget.URL+"/v1/images/generations")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte(`{"data":[{"url":"unexpected"}]}`))
	}))
	defer fallback.Close()
	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "redirect", groupName: "redirect", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-image-test","prompt":"test"}`)
	if recorder.Code != http.StatusBadGateway || primaryCalls != 1 || redirectTargetCalls != 0 || fallbackCalls != 0 {
		t.Fatalf("redirect image response=%d %q calls=%d/%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, redirectTargetCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "redirect")
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 {
		t.Fatalf("image redirect changed routing health: %+v err=%v", key, err)
	}
}

func TestGatewayForwardDoesNotReplayCommittedImageFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		handler     http.HandlerFunc
		failureKind string
	}{
		{
			name:        "transport disconnect",
			failureKind: gateway.RoutingFailureTransport,
			handler: func(w http.ResponseWriter, _ *http.Request) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = conn.Close()
				}
			},
		},
		{
			name:        "empty response",
			failureKind: gateway.RoutingFailureTransport,
			handler:     func(http.ResponseWriter, *http.Request) {},
		},
		{
			name:        "truncated response",
			failureKind: gateway.RoutingFailureTransport,
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", "128")
				_, _ = w.Write([]byte(`{"data":[`))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			var primaryCalls atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				primaryCalls.Add(1)
				tc.handler(w, r)
			}))
			defer primary.Close()
			var fallbackCalls atomic.Int32
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls.Add(1)
				_, _ = w.Write([]byte(`{"data":[{"url":"unexpected"}]}`))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-test","prompt":"test"}`)
			if recorder.Code != http.StatusBadGateway || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
				t.Fatalf("image response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
			}
			key, err := store.FindUpstreamKey(1, "primary")
			if err != nil || key.RoutingFailureCount != 1 || key.LastRoutingFailureKind != tc.failureKind {
				t.Fatalf("committed image failure health = %+v err=%v", key, err)
			}
			healthFence, ok := gateway.RoutingHealthFenceForCandidate(decision.Candidates[0])
			if !ok || !healthFence.MatchesUpstreamKey(key) {
				t.Fatalf("committed image failure fence=%+v key=%+v", healthFence, key)
			}
		})
	}
}

func TestGatewayImageGenerationRequestClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		body string
		want bool
	}{
		{name: "images endpoint", path: "/v1/images/generations", body: `{}`, want: true},
		{name: "responses image tool", path: "/v1/responses", body: `{"tools":[{"type":"image_generation"}]}`, want: true},
		{name: "responses image tool with string choice", path: "/v1/responses", body: `{"tools":[{"type":"image_generation"}],"tool_choice":"auto"}`, want: true},
		{name: "responses image choice", path: "/v1/responses", body: `{"tool_choice":{"type":"image_generation"}}`, want: true},
		{name: "responses string choice without image", path: "/v1/responses", body: `{"tools":[{"type":"web_search"}],"tool_choice":"auto"}`, want: false},
		{name: "responses web search", path: "/v1/responses", body: `{"tools":[{"type":"web_search"}]}`, want: false},
		{name: "invalid body", path: "/v1/responses", body: `{`, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isGatewayImageGenerationRequest(tc.path, []byte(tc.body)); got != tc.want {
				t.Fatalf("classified = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestGatewayForwardDoesNotReplayResponsesImageTool(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"image task failed\"}}\n\n"))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"unexpected fallback\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	body := `{"model":"gpt-test","tools":[{"type":"image_generation"}],"stream":true}`
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", body)
	if recorder.Code != http.StatusBadGateway || primaryCalls != 1 || fallbackCalls != 0 || strings.Contains(recorder.Body.String(), "image task failed") {
		t.Fatalf("responses image result=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if err != nil || len(logs) != 1 || logs[0].ChannelID != 1 || logs[0].RouteReason != gatewayRouteReasonAllFailed {
		t.Fatalf("responses image failure log = %+v err=%v", logs, err)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 {
		t.Fatalf("unknown image SSE error changed routing health: %+v err=%v", key, err)
	}
}

func TestGatewayCommittedResponsesImageTerminalBranchesFinalizeOnce(t *testing.T) {
	for _, test := range []struct {
		name        string
		contentType string
		body        string
		requestBody string
	}{
		{
			name:        "json context limit",
			contentType: "application/json",
			body:        `{"error":{"type":"invalid_request_error","message":"context length exceeded the maximum token limit"}}`,
			requestBody: `{"model":"gpt-test","tools":[{"type":"image_generation"}]}`,
		},
		{
			name:        "sse context limit",
			contentType: "text/event-stream",
			body:        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"context length exceeded the maximum token limit\"}}\n\n",
			requestBody: `{"model":"gpt-test","tools":[{"type":"image_generation"}],"stream":true}`,
		},
		{
			name:        "sse capacity notice",
			contentType: "text/event-stream",
			body:        "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n",
			requestBody: `{"model":"gpt-test","tools":[{"type":"image_generation"}],"stream":true}`,
		},
		{
			name:        "sse model unsupported",
			contentType: "text/event-stream",
			body:        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"requested model is not supported\"}}\n\n",
			requestBody: `{"model":"gpt-test","tools":[{"type":"image_generation"}],"stream":true}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			var primaryCalls atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls.Add(1)
				w.Header().Set("Content-Type", test.contentType)
				w.Header().Set("X-Oneapi-Request-Id", "req-image-terminal")
				_, _ = w.Write([]byte(test.body))
			}))
			defer primary.Close()

			var fallbackCalls atomic.Int32
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls.Add(1)
				_, _ = w.Write([]byte(`{"unexpected":"fallback"}`))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", test.requestBody)
			if recorder.Code != http.StatusBadGateway || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
				t.Fatalf("response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 3)
			if err != nil || len(logs) != 1 {
				t.Fatalf("logs=%+v err=%v", logs, err)
			}
			if !logs[0].BillingCommitted || logs[0].ChannelID != 1 || logs[0].UpstreamRequestID != "req-image-terminal" {
				t.Fatalf("committed image terminal log=%+v", logs[0])
			}
			now := time.Now().UTC()
			recoverable, err := store.ListRecoverableRequestDispatches(now.Add(time.Minute), now, 10)
			if err != nil || len(recoverable) != 0 {
				t.Fatalf("recoverable dispatches=%+v err=%v", recoverable, err)
			}
		})
	}
}

func TestGatewayForwardRecordsTransientResponsesImageSSEOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"upstream overloaded\"}}\n\n"))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte("unexpected fallback"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	body := `{"model":"gpt-test","tools":[{"type":"image_generation"}],"stream":true}`
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", body)
	if recorder.Code != http.StatusBadGateway || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("transient image SSE result=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 1 || key.LastRoutingFailureKind != gateway.RoutingFailureTransport {
		t.Fatalf("transient image SSE health = %+v err=%v", key, err)
	}
}

func TestGatewayImagePermanentStatusesDoNotChangeRoutingHealth(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusPaymentRequired,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusRequestEntityTooLarge,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			calls := 0
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.Header().Set("Content-Length", "128")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":"truncated permanent response"}`))
			}))
			defer primary.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
				channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 1,
			}})
			recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-test","prompt":"test"}`)
			if recorder.Code != http.StatusBadGateway || calls != 1 {
				t.Fatalf("permanent image status=%d response=%d %q calls=%d", status, recorder.Code, recorder.Body.String(), calls)
			}
			key, err := store.FindUpstreamKeyAny(1, "primary")
			if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
				t.Fatalf("permanent image status=%d changed routing health: %+v err=%v", status, key, err)
			}
			wantEnabled := status != http.StatusUnauthorized && status != http.StatusForbidden
			if key.Enabled != wantEnabled {
				t.Fatalf("permanent image status=%d key enabled=%t, want %t", status, key.Enabled, wantEnabled)
			}
		})
	}
}

func TestGatewayImageFailuresEnterIsolationAcrossRealRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var calls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer primary.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 1,
	}})
	for requestNumber := 1; requestNumber <= 3; requestNumber++ {
		recorder := runGatewayRetryTestRequest(svc, decision, "/v1/images/generations", `{"model":"gpt-test","prompt":"test"}`)
		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("image request %d response=%d %q", requestNumber, recorder.Code, recorder.Body.String())
		}
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || calls.Load() != 3 || key.RoutingFailureCount != 3 || key.LastRoutingFailureAt == nil || key.RoutingCooldownUntil != nil {
		t.Fatalf("three image requests did not enter persistent isolation: calls=%d key=%+v err=%v", calls.Load(), key, err)
	}
}

func TestGatewayForwardDoesNotRetryPermanentOrUnknownSSEErrors(t *testing.T) {
	for _, tc := range []struct {
		name           string
		payload        string
		invalidatesKey bool
	}{
		{name: "invalid request", payload: `{"type":"error","error":{"type":"invalid_request_error","message":"invalid request"}}`},
		{name: "authentication", payload: `{"type":"error","error":{"type":"authentication_error","message":"authentication failed"}}`, invalidatesKey: true},
		{name: "tool policy", payload: `{"type":"error","error":{"code":"tool_not_allowed","message":"tool use is not allowed by policy"}}`},
		{name: "unknown provider error", payload: `{"type":"error","error":{"type":"provider_error","message":"opaque provider failure"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			primaryCalls := 0
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls++
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("event: error\ndata: " + tc.payload + "\n\n"))
			}))
			defer primary.Close()
			fallbackCalls := 0
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls++
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {\"delta\":\"fallback\"}\n\n"))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback") || primaryCalls != 1 || fallbackCalls != 1 {
				t.Fatalf("SSE response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
			}
			key, err := store.FindUpstreamKeyAny(1, "primary")
			if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
				t.Fatalf("permanent or unknown SSE changed routing health: %+v err=%v", key, err)
			}
			if key.Enabled == tc.invalidatesKey {
				t.Fatalf("permanent or unknown SSE key enabled=%t, invalidates=%t", key.Enabled, tc.invalidatesKey)
			}
		})
	}
}

func TestGatewayForwardPreservesPermanentOuterStatusAcrossSSEFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		handler func(http.ResponseWriter)
	}{
		{
			name:   "redirect empty stream",
			status: http.StatusNotModified,
			handler: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusNotModified)
			},
		},
		{
			name:   "prelude read failure",
			status: http.StatusMethodNotAllowed,
			handler: func(w http.ResponseWriter) {
				w.Header().Set("Content-Length", "256")
				w.WriteHeader(http.StatusMethodNotAllowed)
				_, _ = w.Write([]byte("event: error\ndata: {"))
			},
		},
		{
			name:   "structured overloaded error",
			status: http.StatusGone,
			handler: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusGone)
				_, _ = w.Write([]byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\"}}\n\n"))
			},
		},
		{
			name:   "empty stream",
			status: http.StatusUnsupportedMediaType,
			handler: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusUnsupportedMediaType)
			},
		},
		{
			name:   "metadata only stream",
			status: http.StatusUnprocessableEntity,
			handler: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\ndata: [DONE]\n\n"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			primaryCalls := 0
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls++
				w.Header().Set("Content-Type", "text/event-stream")
				tc.handler(w)
			}))
			defer primary.Close()
			fallbackCalls := 0
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls++
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {\"delta\":\"fallback\"}\n\n"))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback") || primaryCalls != 1 || fallbackCalls != 1 {
				t.Fatalf("outer status %d response=%d %q calls=%d/%d", tc.status, recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
			}
			key, err := store.FindUpstreamKey(1, "primary")
			if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
				t.Fatalf("outer status %d changed routing health: %+v err=%v", tc.status, key, err)
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
			if err != nil || len(logs) != 1 {
				t.Fatalf("outer status %d logs=%+v err=%v", tc.status, logs, err)
			}
			var traces []gatewayRouteAttemptTrace
			if err := json.Unmarshal([]byte(logs[0].RouteAttemptedChannels), &traces); err != nil || len(traces) != 1 || traces[0].StatusCode != tc.status || traces[0].Reason != "upstream_status" {
				t.Fatalf("outer status %d audit=%q traces=%+v err=%v", tc.status, logs[0].RouteAttemptedChannels, traces, err)
			}
		})
	}
}

func TestGatewayForwardPassesThroughResponsesCompletedNullError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_completed\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte("unexpected fallback"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "response.completed") || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("completed SSE response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil {
		t.Fatalf("completed SSE with null error changed routing health: %+v err=%v", key, err)
	}
}

func TestGatewayStreamPayloadNullErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		body  string
		found bool
	}{
		{name: "root null", body: `{"error":null}`},
		{name: "completed response null", body: `{"type":"response.completed","response":{"status":"completed","error":null}}`},
		{name: "completed response empty object", body: `{"type":"response.completed","response":{"status":"completed","error":{}}}`},
		{name: "failed response null", body: `{"type":"response.failed","response":{"status":"failed","error":null}}`, found: true},
		{name: "completed response nonempty error", body: `{"type":"response.completed","response":{"status":"completed","error":{"message":"failed"}}}`, found: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classification := classifyGatewayStreamFailure([]byte("data: " + tc.body + "\n\n"))
			if classification.Found != tc.found {
				t.Fatalf("classification=%+v, want found=%t", classification, tc.found)
			}
			if got := gatewayStreamFrameHasError([]byte("data: " + tc.body + "\n\n")); got != tc.found {
				t.Fatalf("frame error=%t, want %t", got, tc.found)
			}
		})
	}
}

func TestGatewayStreamFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name        string
		payload     string
		disposition gatewayStreamFailureDisposition
		status      int
	}{
		{name: "overloaded", payload: `{"type":"error","error":{"type":"overloaded_error"}}`, disposition: gatewayStreamFailureTransient},
		{name: "stream read error", payload: `{"type":"error","error":{"type":"stream_read_error"}}`, disposition: gatewayStreamFailureTransient},
		{name: "rate limit", payload: `{"error":{"code":"rate_limit_error"}}`, disposition: gatewayStreamFailureTransient},
		{name: "anthropic api error", payload: `{"type":"error","error":{"type":"api_error"}}`, disposition: gatewayStreamFailureTransient},
		{name: "success status does not hide type", payload: `{"type":"error","error":{"status_code":200,"type":"overloaded_error"}}`, disposition: gatewayStreamFailureTransient},
		{name: "embedded status", payload: `{"response":{"status":"failed","error":{"status_code":503,"type":"api_error"}}}`, disposition: gatewayStreamFailureTransient, status: 503},
		{name: "nested numeric code", payload: `{"error":{"code":503}}`, disposition: gatewayStreamFailureTransient, status: 503},
		{name: "root numeric code", payload: `{"type":"error","code":503}`, disposition: gatewayStreamFailureTransient, status: 503},
		{name: "nonstandard 600 status", payload: `{"response":{"status":"failed","error":{"status_code":600,"type":"api_error"}}}`, disposition: gatewayStreamFailurePermanent, status: 600},
		{name: "nonstandard 600 code", payload: `{"error":{"code":600}}`, disposition: gatewayStreamFailurePermanent, status: 600},
		{name: "plain service unavailable", payload: `service unavailable`, disposition: gatewayStreamFailureTransient},
		{name: "invalid request", payload: `{"error":{"type":"invalid_request_error"}}`, disposition: gatewayStreamFailurePermanent},
		{name: "billing", payload: `{"error":{"type":"billing_error"}}`, disposition: gatewayStreamFailurePermanent},
		{name: "request too large", payload: `{"error":{"type":"request_too_large"}}`, disposition: gatewayStreamFailurePermanent},
		{name: "insufficient quota", payload: `{"error":{"type":"insufficient_quota"}}`, disposition: gatewayStreamFailurePermanent},
		{name: "unknown provider error", payload: `{"error":{"type":"provider_error"}}`, disposition: gatewayStreamFailureUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			classification := classifyGatewayStreamFailure([]byte("event: error\ndata: " + tc.payload + "\n\n"))
			if !classification.Found || classification.Disposition != tc.disposition || classification.Status != tc.status {
				t.Fatalf("classification=%+v, want disposition=%d status=%d", classification, tc.disposition, tc.status)
			}
		})
	}
}

func TestGatewayImageRequestUsesFullCandidateBudget(t *testing.T) {
	for _, tc := range []struct {
		name        string
		path        string
		body        string
		contentType string
		response    string
	}{
		{
			name:        "images endpoint",
			path:        "/v1/images/generations",
			body:        `{"model":"gpt-image-test","prompt":"test"}`,
			contentType: "application/json",
			response:    `{"data":[{"url":"https://example.invalid/image.png"}]}`,
		},
		{
			name:        "responses image tool",
			path:        "/v1/responses",
			body:        `{"model":"gpt-test","tools":[{"type":"image_generation"}],"stream":true}`,
			contentType: "text/event-stream",
			response:    "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_image_completed\",\"status\":\"completed\",\"error\":null}}\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				time.Sleep(200 * time.Millisecond)
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = w.Write([]byte(tc.response))
			}))
			defer upstream.Close()
			svc, _, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "image", groupName: "image", siteURL: upstream.URL, ratio: 0.1},
			})
			svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: 500 * time.Millisecond}})
			recorder := runGatewayRetryTestRequest(svc, decision, tc.path, tc.body)
			if recorder.Code != http.StatusOK || calls != 1 {
				t.Fatalf("image response=%d %q calls=%d", recorder.Code, recorder.Body.String(), calls)
			}
		})
	}
}

func TestGatewayForwardRetriesTooEarlyOnSameCandidate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		if primaryCalls < gatewayCandidateRetryAttempts {
			http.Error(w, "try again", http.StatusTooEarly)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"ready\"}\n\n"))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte("fallback"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	key, err := store.FindUpstreamKey(1, "primary")
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "ready") || primaryCalls != gatewayCandidateRetryAttempts || fallbackCalls != 0 || err != nil || key.RoutingFailureCount != 0 {
		t.Fatalf("425 response=%d %q calls=%d/%d key=%+v err=%v", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls, key, err)
	}
}

func TestGatewayForwardAcceptedRouteOutlivesFirstByteBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const budget = 500 * time.Millisecond
	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_accepted\"}}\n\n"))
		w.(http.Flusher).Flush()
		time.Sleep(budget + 300*time.Millisecond)
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"primary after thinking\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_accepted\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer primary.Close()
	var fallbackCalls atomic.Int32
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls.Add(1)
		_, _ = w.Write([]byte("unexpected fallback"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: budget}})
	started := time.Now()
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	elapsed := time.Since(started)
	key, err := store.FindUpstreamKey(1, "primary")
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "primary after thinking") || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 || err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil {
		t.Fatalf("accepted response=%d %q calls=%d/%d elapsed=%s key=%+v err=%v", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load(), elapsed, key, err)
	}
	if elapsed <= budget {
		t.Fatalf("accepted route completed in %s, want it to remain attached beyond %s", elapsed, budget)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("fallback request logs = %+v, err=%v", logs, err)
	}
	if logs[0].ChannelID != 1 || logs[0].FirstByteMs <= budget.Milliseconds() {
		t.Fatalf("accepted route useful-output latency = channel=%d first_byte_ms=%d, want >%d", logs[0].ChannelID, logs[0].FirstByteMs, budget.Milliseconds())
	}
}

func TestGatewayForwardAllowsBufferedResponseToFinishAfterFirstByteBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	primaryCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{"))
		w.(http.Flusher).Flush()
		time.Sleep(250 * time.Millisecond)
		_, _ = w.Write([]byte(`"output_text":"primary complete"}`))
	}))
	defer primary.Close()
	fallbackCalls := 0
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output_text":"buffered fallback"}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "partial", groupName: "partial", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	const budget = 120 * time.Millisecond
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: budget}})
	started := time.Now()
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":false}`)
	elapsed := time.Since(started)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "primary complete") || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("buffered response=%d %q calls=%d/%d elapsed=%s", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls, elapsed)
	}
	if elapsed <= budget {
		t.Fatalf("buffered response took %s, want total time beyond first-byte budget %s", elapsed, budget)
	}
	key, err := store.FindUpstreamKey(1, "partial")
	if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil {
		t.Fatalf("successful buffered response changed routing health: %+v err=%v", key, err)
	}
}

type gatewayDelayedTargetResolver struct {
	channelService
	delay   time.Duration
	siteURL string
}

func (r gatewayDelayedTargetResolver) Resolve(ctx context.Context, _ *storage.Channel) (*connector.Channel, error) {
	timer := time.NewTimer(r.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-timer.C:
		return &connector.Channel{SiteURL: r.siteURL}, nil
	}
}

func TestGatewayForwardStartsFirstByteBudgetAfterCandidatePreparation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const (
		budget           = 400 * time.Millisecond
		preparationDelay = 300 * time.Millisecond
		responseDelay    = 200 * time.Millisecond
	)
	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls.Add(1)
		time.Sleep(responseDelay)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output_text":"prepared primary"}`))
	}))
	defer primary.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 1,
	}})
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: budget}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"gpt-test","stream":false}`)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
	mapping := newGatewayModelMappingResult("gpt-test", "", body)
	started := time.Now()
	gatewayForward(ctx, &Deps{
		Gateway:    svc,
		ChannelSvc: gatewayDelayedTargetResolver{delay: preparationDelay, siteURL: primary.URL},
	}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", body, mapping, started)
	elapsed := time.Since(started)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "prepared primary") || primaryCalls.Load() != 1 {
		t.Fatalf("prepared response=%d %q calls=%d elapsed=%s", recorder.Code, recorder.Body.String(), primaryCalls.Load(), elapsed)
	}
	if elapsed < preparationDelay {
		t.Fatalf("prepared response completed in %s, want preparation delay %s", elapsed, preparationDelay)
	}
	if elapsed <= budget {
		t.Fatalf("prepared response completed in %s, want preparation plus response to exceed old shared budget %s", elapsed, budget)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != 0 || key.LastRoutingFailureAt != nil {
		t.Fatalf("candidate preparation changed routing health: %+v err=%v", key, err)
	}
}

func TestGatewayForwardRetriesThenChecksStalledCandidateBeforeFallback(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const budget = 1500 * time.Millisecond
	var primaryCalls atomic.Int32
	thirdStarted := make(chan time.Time, 1)
	thirdCanceled := make(chan time.Time, 1)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "Reply OK.") {
			<-r.Context().Done()
			return
		}
		call := primaryCalls.Add(1)
		if call < gatewayCandidateRetryAttempts {
			time.Sleep(80 * time.Millisecond)
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		thirdStarted <- time.Now()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: heartbeat\ndata: {\"type\":\"heartbeat\"}\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		thirdCanceled <- time.Now()
	}))
	defer primary.Close()
	var secondCalls atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"fallback after bounded retries\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_bounded_fallback\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer second.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "second", groupName: "second", siteURL: second.URL, ratio: 1},
	})
	svc.UpdateRoutingConfig(gateway.RoutingConfig{WaitingProbeInterval: 200 * time.Millisecond, WaitingProbeTimeout: budget, FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: budget}})
	started := time.Now()
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	elapsed := time.Since(started)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback after bounded retries") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || primaryCalls.Load() != gatewayCandidateRetryAttempts || secondCalls.Load() != 1 {
		t.Fatalf("candidate budget response=%d %q calls=%d/%d elapsed=%s", recorder.Code, recorder.Body.String(), primaryCalls.Load(), secondCalls.Load(), elapsed)
	}
	if elapsed < budget-100*time.Millisecond || elapsed > budget+1500*time.Millisecond {
		t.Fatalf("candidate retries took %s, want a health-check deadline after retries near %s", elapsed, budget)
	}
	thirdDuration := (<-thirdCanceled).Sub(<-thirdStarted)
	if thirdDuration < budget {
		t.Fatalf("stalled request was cut off by the old first-byte budget: duration=%s budget=%s", thirdDuration, budget)
	}
	if key, err := store.FindUpstreamKey(1, "primary"); err != nil {
		t.Fatal(err)
	} else if key.RoutingFailureCount != gatewayCandidateRetryAttempts || key.LastRoutingFailureAt == nil || key.RoutingCooldownUntil != nil {
		t.Fatalf("primary candidate timeout batch was not isolated: %+v", key)
	}
}
