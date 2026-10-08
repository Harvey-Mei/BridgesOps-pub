package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// gatewayOwnerLossStreamRecorder signals once the upstream usage block has
// actually been forwarded downstream. Only then is the tail buffer guaranteed
// to hold the usage, which is the exact state the regression is about.
type gatewayOwnerLossStreamRecorder struct {
	*httptest.ResponseRecorder
	once      sync.Once
	usageSeen chan struct{}
}

func (r *gatewayOwnerLossStreamRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseRecorder.Write(p)
	if bytes.Contains(p, []byte("response.completed")) {
		r.once.Do(func() { close(r.usageSeen) })
	}
	return n, err
}

// TestGatewayForwardOwnerLossAfterStreamedUsageKeepsExactTokenUsage is the
// regression for the streaming owner-loss path. The upstream usage block is
// already in the tail buffer when the owner lease is fenced, so the durable
// handoff must carry the complete terminal log with its token usage instead of
// the deferred fallback log, which has no usage and no cost basis at all.
func TestGatewayForwardOwnerLossAfterStreamedUsageKeepsExactTokenUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	usageStreamed := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpstream := func() { releaseOnce.Do(func() { close(release) }) }

	first := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v1/models" {
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"object":"list","data":[{"id":"gpt-test"}]}`))
			return
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(writer, "event: response.created\n"+
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp-owner-loss-usage\",\"status\":\"in_progress\"}}\n\n")
		_, _ = io.WriteString(writer, "event: response.output_text.delta\n"+
			"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"streamed output\"}\n\n")
		_, _ = io.WriteString(writer, "event: response.completed\n"+
			"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp-owner-loss-usage\",\"status\":\"completed\",\"usage\":{\"input_tokens\":11,\"output_tokens\":19,\"cache_read_tokens\":0,\"cache_write_tokens\":0,\"total_tokens\":30,\"actual_cost\":0.25}}}\n\n")
		if flusher, ok := writer.(http.Flusher); ok {
			flusher.Flush()
		}
		close(usageStreamed)
		select {
		case <-release:
		case <-request.Context().Done():
		}
	}))
	t.Cleanup(func() {
		releaseUpstream()
		first.Close()
	})
	second, secondCalls := newGatewayOwnerLossSecondUpstream(t)
	fixture := setupGatewayOwnerLossAPIFixture(t, first.URL, second.URL)
	before := seedGatewayOwnerLossRoutingHealth(t, fixture)

	recorder := &gatewayOwnerLossStreamRecorder{ResponseRecorder: httptest.NewRecorder(), usageSeen: make(chan struct{})}
	ginContext, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"gpt-test","stream":true}`)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
	finished := make(chan struct{})
	go func() {
		gatewayForward(
			ginContext, &Deps{Gateway: fixture.service},
			&storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true},
			fixture.decision, "gpt-test", "", body,
			gatewayModelMappingResult{Model: "gpt-test"}, time.Now(),
		)
		close(finished)
	}()

	waitForGatewayOwnerLossSignal(t, usageStreamed, "streamed upstream usage block")
	waitForGatewayOwnerLossSignal(t, recorder.usageSeen, "downstream delivery of the upstream usage block")
	stopContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := fixture.service.StopRequestDispatchOwner(stopContext, time.Now())
	cancel()
	if err != nil {
		t.Fatalf("stop request dispatch owner: %v", err)
	}
	releaseUpstream()
	waitForGatewayOwnerLossSignal(t, finished, "gateway owner-loss response")

	if secondCalls.Load() != 0 {
		t.Fatalf("second candidate received %d requests after owner loss", secondCalls.Load())
	}
	assertGatewayOwnerLossRoutingHealthUnchanged(t, fixture, before)

	var audits []storage.GatewayRequestDispatchAudit
	if err := fixture.db.Order("id ASC").Find(&audits).Error; err != nil {
		t.Fatalf("list owner-loss dispatch audits: %v", err)
	}
	if len(audits) != 1 {
		t.Fatalf("owner-loss dispatch audits = %d, want one", len(audits))
	}
	audit := audits[0]
	if audit.RecoveryRequestedAt == nil || audit.RecoveryTerminalKind != storage.GatewayRequestDispatchTerminalOwnerLeaseLost {
		t.Fatalf("streaming owner loss did not surrender through the durable handoff: %+v", audit)
	}
	if audit.TerminalAt != nil || audit.RequestLogID != nil {
		t.Fatalf("fenced owner finalized the dispatch itself: %+v", audit)
	}

	handoffs, err := fixture.store.ListRequestDispatchTerminalHandoffs(10)
	if err != nil || len(handoffs) != 1 {
		t.Fatalf("durable terminal handoffs = %+v err=%v, want exactly one", handoffs, err)
	}
	decoded, err := storage.DecodeGatewayRequestDispatchTerminalHandoff(handoffs[0])
	if err != nil {
		t.Fatalf("decode durable terminal handoff: %v", err)
	}
	if decoded.DispatchID != audit.ID || decoded.TerminalKind != storage.GatewayRequestDispatchTerminalOwnerLeaseLost || !decoded.PhysicallyDispatched {
		t.Fatalf("durable handoff identity = %+v", decoded)
	}
	if decoded.Log == nil {
		t.Fatal("streaming owner loss handed off without an exact terminal log")
	}
	if decoded.Log.PromptTokens != 11 || decoded.Log.CompletionTokens != 19 || decoded.Log.TotalTokens != 30 {
		t.Fatalf("durable handoff lost the streamed tail usage: prompt=%d completion=%d total=%d",
			decoded.Log.PromptTokens, decoded.Log.CompletionTokens, decoded.Log.TotalTokens)
	}
	if !decoded.Log.UsageInputPresent || !decoded.Log.UsageOutputPresent ||
		!decoded.Log.UsageCacheReadPresent || !decoded.Log.UsageCacheWritePresent {
		t.Fatalf("durable handoff lost explicit usage presence: %+v", decoded.Log)
	}
	if decoded.Log.Cost != 0 || decoded.Log.RawCost == nil || *decoded.Log.RawCost != 0.25 ||
		decoded.Log.CostEstimated || decoded.Log.CostSource != "actual_cost" {
		t.Fatalf("durable handoff lost the explicit upstream cost: %+v", decoded.Log)
	}
	if decoded.Log.UpstreamRequestID == "" && decoded.Facts.ProviderRequestID == "" && !decoded.Facts.Accepted {
		t.Fatalf("durable handoff lost the observed lifecycle facts: log=%+v facts=%+v", decoded.Log, decoded.Facts)
	}

	var requestLogs int64
	if err := fixture.db.Model(&storage.GatewayRequestLog{}).Count(&requestLogs).Error; err != nil {
		t.Fatalf("count owner-loss request logs: %v", err)
	}
	if requestLogs != 0 {
		t.Fatalf("fenced owner wrote %d request logs before the durable retry", requestLogs)
	}
}
