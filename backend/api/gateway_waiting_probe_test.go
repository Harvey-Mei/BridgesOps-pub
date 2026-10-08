package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestGatewayWaitingProbeAllowsSlowHealthyResponse(t *testing.T) {
	var probes, main atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "Reply OK.") {
			probes.Add(1)
		} else {
			main.Add(1)
			select {
			case <-time.After(100 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"model":"gpt-test","choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer upstream.Close()
	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{channelID: 1, groupName: "primary", siteURL: upstream.URL, ratio: 1}})
	svc.UpdateRoutingConfig(gateway.RoutingConfig{WaitingProbeInterval: 15 * time.Millisecond, WaitingProbeTimeout: 50 * time.Millisecond, FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: 30 * time.Millisecond}})
	response := runGatewayRetryTestRequest(svc, decision, "/v1/chat/completions", `{"model":"gpt-test"}`)
	if response.Code != 200 || main.Load() != 1 || probes.Load() < 1 {
		t.Fatalf("response=%d main=%d probes=%d", response.Code, main.Load(), probes.Load())
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 20)
	if err != nil || len(logs) != 1 || logs[0].RouteReason == "fallback" {
		t.Fatalf("probe appeared in usage or healthy request retried: %+v %v", logs, err)
	}
}

func TestGatewayWaitingProbeTimeoutDoesNotReplayAcceptedRequest(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "unaccepted fallback", true: "accepted terminal"}[accepted], func(t *testing.T) {
			var probes, main, fallback atomic.Int32
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if strings.Contains(string(body), "Reply OK.") {
					probes.Add(1)
				} else {
					main.Add(1)
					if accepted {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_waiting\",\"status\":\"in_progress\",\"model\":\"gpt-test\"}}\n\n")
						w.(http.Flusher).Flush()
					}
				}
				<-r.Context().Done()
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fallback.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"model":"gpt-test","output":[],"status":"completed"}`)
			}))
			defer second.Close()
			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{channelID: 1, groupName: "primary", siteURL: first.URL, ratio: 1}, {channelID: 2, groupName: "backup", siteURL: second.URL, ratio: 2}})
			svc.UpdateRoutingConfig(gateway.RoutingConfig{WaitingProbeInterval: 10 * time.Millisecond, WaitingProbeTimeout: 35 * time.Millisecond})
			response := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
			if main.Load() != 1 || probes.Load() != 1 {
				t.Fatalf("request/probe replay: main=%d probes=%d", main.Load(), probes.Load())
			}
			if accepted && (fallback.Load() != 0 || !strings.Contains(response.Body.String(), "response.failed")) {
				t.Fatalf("accepted request did not terminate safely: fallback=%d response=%s", fallback.Load(), response.Body.String())
			}
			if !accepted && (fallback.Load() != 1 || response.Code != 200) {
				t.Fatalf("unaccepted request did not fall back once: fallback=%d status=%d", fallback.Load(), response.Code)
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 20)
			if err != nil || len(logs) != 1 {
				t.Fatalf("usage logs=%+v error=%v", logs, err)
			}
			if accepted && !logs[0].UpstreamAccepted {
				t.Fatal("accepted evidence lost")
			}
		})
	}
}

func TestGatewayWaitingProbeStopsAfterUsefulOutput(t *testing.T) {
	var probes atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "Reply OK.") {
			probes.Add(1)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-time.After(60 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()
	svc, _, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{channelID: 1, groupName: "primary", siteURL: upstream.URL, ratio: 1}})
	svc.UpdateRoutingConfig(gateway.RoutingConfig{WaitingProbeInterval: 20 * time.Millisecond, WaitingProbeTimeout: 30 * time.Millisecond})
	response := runGatewayRetryTestRequest(svc, decision, "/v1/chat/completions", `{"model":"gpt-test","stream":true}`)
	if response.Code != 200 || probes.Load() != 0 || !strings.Contains(response.Body.String(), "[DONE]") {
		t.Fatalf("semantic output did not stop probes: %d %d %s", response.Code, probes.Load(), response.Body.String())
	}
}

func TestGatewayWaitingProbeSharesUntilLastWaiterLeaves(t *testing.T) {
	p := gatewayWaitingProbeCoordinator{slots: make(chan struct{}, 1)}
	first := newGatewayFirstByteDeadline(context.Background(), 0)
	defer first.Close()
	second := newGatewayFirstByteDeadline(context.Background(), 0)
	defer second.Close()
	started, finished := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	run := func(ctx context.Context) bool {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		close(finished)
		return false
	}
	stopFirst := p.subscribe("same-target", first, time.Millisecond, run)
	stopSecond := p.subscribe("same-target", second, time.Millisecond, run)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("probe not started")
	}
	stopFirst()
	select {
	case <-finished:
		t.Fatal("one subscriber canceled shared probe")
	default:
	}
	stopSecond()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("last subscriber did not cancel")
	}
	if calls.Load() != 1 {
		t.Fatal("shared target produced duplicate probes")
	}
}

func TestGatewayWaitingProbeCannotCancelUsefulOutputDuringAcceptanceWrite(t *testing.T) {
	deadline := newGatewayFirstByteDeadline(context.Background(), 0)
	deadline.waiting = true
	defer deadline.Close()
	stream := io.NopCloser(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
	_, _, useful, _, err := prepareGatewayStreamPreludeWithAcceptance(stream, "/v1/chat/completions", func(gatewayStreamAcceptanceEvidence) error {
		deadline.failWaitingProbe()
		return nil
	}, deadline.MarkFirstByte)
	if err != nil || !useful || deadline.WaitingProbeFailed() {
		t.Fatalf("useful output lost to a late probe: useful=%v error=%v", useful, err)
	}
}

func TestGatewayWaitingProbeObservesAcceptedResponsesUntilUsefulOutput(t *testing.T) {
	deadline := newGatewayFirstByteDeadline(context.Background(), 0)
	deadline.waiting = true
	defer deadline.Close()
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_waiting\",\"status\":\"in_progress\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n"
	acceptedCalls, usefulCalls := 0, 0
	_, _, useful, accepted, err := prepareGatewayStreamPreludeWithAcceptance(io.NopCloser(strings.NewReader(body)), "/v1/responses", func(evidence gatewayStreamAcceptanceEvidence) error {
		acceptedCalls++
		if evidence.EventType != "response.created" || usefulCalls != 0 || deadline.firstByte.Load() {
			t.Fatal("acceptance was delayed until useful output")
		}
		deadline.MarkAccepted()
		if deadline.firstByte.Load() {
			t.Fatal("acceptance stopped waiting health observation")
		}
		return nil
	}, func() {
		usefulCalls++
		deadline.MarkFirstByte()
		deadline.failWaitingProbe()
	})
	if err != nil || !useful || !accepted || acceptedCalls != 1 || usefulCalls != 1 || deadline.WaitingProbeFailed() {
		t.Fatalf("acceptance/useful boundaries: useful=%v accepted=%v callbacks=%d/%d error=%v", useful, accepted, acceptedCalls, usefulCalls, err)
	}
}
