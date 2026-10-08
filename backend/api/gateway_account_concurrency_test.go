package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func gatewayAccountConcurrencySnapshot(accountID uint64, limit int) *storage.UpstreamAccountConcurrencySnapshot {
	observedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	return &storage.UpstreamAccountConcurrencySnapshot{
		AccountRecordID:         accountID,
		ChannelIdentityRevision: 1,
		Limit:                   &limit,
		Scope:                   storage.UpstreamConcurrencyScopeAccount,
		Source:                  "sub2api.auth.me.concurrency",
		ObservedAt:              &observedAt,
	}
}

func TestGatewayForwardSmartConcurrencyAdmissionSkipsFullCandidateAndReleasesFallbackLease(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		primaryCalls.Add(1)
	}))
	defer primary.Close()
	var fallbackCalls atomic.Int32
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"fallback"}}]}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "full", groupName: "primary", siteURL: primary.URL, ratio: 1},
		{channelID: 2, name: "fallback", groupName: "secondary", siteURL: fallback.URL, ratio: 1},
	})
	decision.RankedCandidates = append([]gateway.Candidate(nil), decision.Candidates...)
	decision.Candidates[0].UpstreamAccountConcurrency = gatewayAccountConcurrencySnapshot(101, 1)
	decision.Candidates[1].UpstreamAccountConcurrency = gatewayAccountConcurrencySnapshot(202, 1)
	held, heldDecision := svc.TryAcquireCandidateAccountConcurrency(decision.Candidates[0], gateway.AccountConcurrencySmartData)
	if held == nil || !heldDecision.Admitted {
		t.Fatalf("seed account lease = %#v", heldDecision)
	}
	defer held.Release()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := `{"model":"gpt-test"}`
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(body), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || primaryCalls.Load() != 0 || fallbackCalls.Load() != 1 {
		t.Fatalf("response=%d primary=%d fallback=%d body=%q", recorder.Code, primaryCalls.Load(), fallbackCalls.Load(), recorder.Body.String())
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 || logs[0].ChannelID != 2 || logs[0].RouteAttemptedChannels != "" || logs[0].RouteReason != gatewayRouteReasonSmartPrimary || logs[0].RealFallback || logs[0].PriorPhysicallyDispatchedFailureCount != 0 {
		t.Fatalf("route log=%+v err=%v", logs, err)
	}
	available, availableDecision := svc.TryAcquireCandidateAccountConcurrency(decision.Candidates[1], gateway.AccountConcurrencySmartData)
	if available == nil || !availableDecision.Admitted {
		t.Fatalf("fallback account lease was not released: %#v", availableDecision)
	}
	available.Release()
}

func TestGatewayForwardFixedConcurrencyOnlyObservesAndReleases(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"fixed"}}]}`))
	}))
	defer upstream.Close()

	svc, _, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "fixed", groupName: "primary", siteURL: upstream.URL, ratio: 1,
	}})
	decision.SmartRouting = false
	decision.RankedCandidates = append([]gateway.Candidate(nil), decision.Candidates...)
	decision.Candidates[0].UpstreamAccountConcurrency = gatewayAccountConcurrencySnapshot(303, 1)
	held, heldDecision := svc.TryAcquireCandidateAccountConcurrency(decision.Candidates[0], gateway.AccountConcurrencySmartData)
	if held == nil || !heldDecision.Admitted {
		t.Fatalf("seed account lease = %#v", heldDecision)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := `{"model":"gpt-test"}`
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: false}, decision, "gpt-test", "", []byte(body), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("fixed response=%d calls=%d body=%q", recorder.Code, calls.Load(), recorder.Body.String())
	}
	blocked, blockedDecision := svc.TryAcquireCandidateAccountConcurrency(decision.Candidates[0], gateway.AccountConcurrencySmartData)
	if blocked != nil || blockedDecision.Admitted {
		t.Fatalf("fixed route did not remain observable while seed request is active: %#v", blockedDecision)
	}
	held.Release()
	available, availableDecision := svc.TryAcquireCandidateAccountConcurrency(decision.Candidates[0], gateway.AccountConcurrencySmartData)
	if available == nil || !availableDecision.Admitted {
		t.Fatalf("fixed account lease was not released: %#v", availableDecision)
	}
	available.Release()
}
