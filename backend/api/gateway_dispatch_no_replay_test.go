package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestForwardGatewayRequestDoesNotFollowRedirectOrReplayBody(t *testing.T) {
	var initialCalls atomic.Int32
	var redirectedCalls atomic.Int32
	redirected := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectedCalls.Add(1)
	}))
	defer redirected.Close()

	initial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		initialCalls.Add(1)
		if raw, err := io.ReadAll(r.Body); err != nil || string(raw) != `{"model":"gpt-test"}` {
			t.Errorf("initial request body=%q err=%v", raw, err)
		}
		w.Header().Set("Location", redirected.URL+"/replayed")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer initial.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	response, err := forwardGatewayRequestOnDispatch(
		request,
		gatewayResolvedTarget{SiteURL: initial.URL},
		"sk-test",
		[]byte(`{"model":"gpt-test"}`),
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("forward gateway request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status=%d, want %d", response.StatusCode, http.StatusTemporaryRedirect)
	}
	if initialCalls.Load() != 1 || redirectedCalls.Load() != 0 {
		t.Fatalf("initial calls=%d redirected calls=%d, want 1/0", initialCalls.Load(), redirectedCalls.Load())
	}
}

func TestForwardGatewayRequestReturnsEveryRedirectWithoutParsingLocation(t *testing.T) {
	for _, status := range []int{
		http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect,
	} {
		for _, location := range []string{"/redirected", "%gh"} {
			name := fmt.Sprintf("%d/%s", status, location)
			t.Run(name, func(t *testing.T) {
				var calls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls.Add(1)
					w.Header().Set("Location", location)
					w.WriteHeader(status)
				}))
				defer upstream.Close()

				request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
				response, err := forwardGatewayRequestOnDispatch(
					request,
					gatewayResolvedTarget{SiteURL: upstream.URL},
					"sk-test",
					[]byte(`{"model":"gpt-test"}`),
					nil,
					nil,
					nil,
				)
				if err != nil {
					t.Fatalf("forward gateway request: %v", err)
				}
				defer response.Body.Close()
				if response.StatusCode != status || response.Header.Get("Location") != location {
					t.Fatalf("response status/location=%d/%q, want %d/%q", response.StatusCode, response.Header.Get("Location"), status, location)
				}
				if calls.Load() != 1 {
					t.Fatalf("upstream calls=%d, want 1", calls.Load())
				}
			})
		}
	}
}

func TestGatewayImageMalformedRedirectKeepsFirstHopEvidenceWithoutRoutingFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls.Add(1)
		w.Header().Set("Location", "%gh")
		w.Header().Set("X-Oneapi-Request-Id", "req-malformed-redirect")
		w.WriteHeader(http.StatusFound)
	}))
	defer primary.Close()
	var fallbackCalls atomic.Int32
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls.Add(1)
		_, _ = w.Write([]byte(`{"data":[{"url":"unexpected"}]}`))
	}))
	defer fallback.Close()

	service, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "redirect", groupName: "redirect", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(service, decision, "/v1/images/generations", `{"model":"gpt-image-test","prompt":"test"}`)
	if recorder.Code != http.StatusBadGateway || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
		t.Fatalf("malformed redirect image response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("malformed redirect logs=%+v err=%v", logs, err)
	}
	if logs[0].StatusCode != http.StatusBadGateway || logs[0].UpstreamRequestID != "req-malformed-redirect" || !logs[0].BillingCommitted {
		t.Fatalf("malformed redirect first-hop evidence=%+v", logs[0])
	}
	var attempts []gatewayRouteAttemptTrace
	if err := json.Unmarshal([]byte(logs[0].RouteAttemptedChannels), &attempts); err != nil || len(attempts) != 1 || attempts[0].StatusCode != http.StatusFound || !attempts[0].Physical {
		t.Fatalf("malformed redirect physical first-hop attempts=%q decoded=%+v err=%v", logs[0].RouteAttemptedChannels, attempts, err)
	}
	key, err := store.FindUpstreamKey(1, "redirect")
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("malformed redirect changed routing health: key=%+v err=%v", key, err)
	}
}
