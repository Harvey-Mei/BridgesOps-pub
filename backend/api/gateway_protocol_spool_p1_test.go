package api

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// gatewayPairedPhysicalBillingCommitUnknownPool makes exactly one already
// committed transaction look ambiguous to GORM. The callback below arms it
// only after storage has issued the v1 paired true/true update, so the initial
// dispatch audit and the recovery handoff remain ordinary committed writes.
type gatewayPairedPhysicalBillingCommitUnknownPool struct {
	gorm.ConnPool
	armed    atomic.Bool
	injected atomic.Bool
	armCalls atomic.Int32
	commits  atomic.Int32
	markerAt atomic.Int32
}

func (p *gatewayPairedPhysicalBillingCommitUnknownPool) GetDBConn() (*sql.DB, error) {
	if p == nil {
		return nil, gorm.ErrInvalidDB
	}
	if connector, ok := p.ConnPool.(gorm.GetDBConnector); ok {
		return connector.GetDBConn()
	}
	if db, ok := p.ConnPool.(*sql.DB); ok {
		return db, nil
	}
	return nil, gorm.ErrInvalidDB
}

func (p *gatewayPairedPhysicalBillingCommitUnknownPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	beginner, ok := p.ConnPool.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	})
	if !ok {
		return nil, gorm.ErrInvalidTransaction
	}
	tx, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &gatewayPairedPhysicalBillingCommitUnknownTx{Tx: tx, pool: p}, nil
}

func (p *gatewayPairedPhysicalBillingCommitUnknownPool) armPairedCommitUnknown() {
	if p != nil && p.armed.CompareAndSwap(false, true) {
		p.armCalls.Add(1)
	}
}

type gatewayPairedPhysicalBillingCommitUnknownTx struct {
	*sql.Tx
	pool *gatewayPairedPhysicalBillingCommitUnknownPool
}

func (tx *gatewayPairedPhysicalBillingCommitUnknownTx) Commit() error {
	commitNumber := tx.pool.commits.Add(1)
	var pairedMarkerCommitted bool
	if tx.pool != nil && !tx.pool.injected.Load() {
		if err := tx.Tx.QueryRowContext(context.Background(), `
			SELECT EXISTS (
				SELECT 1
				FROM public.gateway_request_dispatch_audits
				WHERE physically_dispatched = TRUE
				  AND billing_committed = TRUE
				  AND transport_state = 'armed'
				  AND transport_return_confirmed = FALSE
				  AND recovery_requested_at IS NULL
				  AND terminal_at IS NULL
			)
		`).Scan(&pairedMarkerCommitted); err != nil {
			_ = tx.Tx.Rollback()
			return err
		}
	}
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.pool == nil || tx.pool.injected.Load() || !pairedMarkerCommitted {
		return nil
	}
	tx.pool.markerAt.CompareAndSwap(0, commitNumber)
	if tx.pool.injected.CompareAndSwap(false, true) {
		tx.pool.armCalls.Add(1)
		return driver.ErrBadConn
	}
	return nil
}

func TestGatewayResponseSpoolLocalFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "capacity", err: errGatewayResponseSpoolCapacity, want: true},
		{name: "queue full", err: errGatewayResponseSpoolQueueFull, want: true},
		{name: "queued deadline", err: gatewayResponseSpoolQueueWaitError(errGatewayFirstByteTimeout), want: true},
		{name: "released lease", err: errGatewayResponseSpoolReleased, want: true},
		{name: "invalid config", err: errGatewayResponseBufferInvalid, want: true},
		{name: "short write", err: io.ErrShortWrite, want: true},
		{name: "local path", err: &os.PathError{Op: "create", Path: "/spool", Err: errors.New("disk full")}, want: true},
		{name: "upstream deadline", err: errGatewayFirstByteTimeout, want: false},
		{name: "ordinary upstream read", err: io.ErrUnexpectedEOF, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := gatewayResponseSpoolLocalFailure(tc.err); got != tc.want {
				t.Fatalf("gatewayResponseSpoolLocalFailure(%v)=%t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

func TestBufferGatewayUpstreamResponseProjectionFailureKeepsBodyStartedFact(t *testing.T) {
	controller, _, err := newGatewayUpstreamResponseSpoolController(GatewayUpstreamResponseSpoolLimits{
		TempDir:             gatewayResponseSpoolTestDirectory(t),
		MaxResponseBytes:    1024,
		PrefixBytes:         128,
		ModelsResponseBytes: 256,
		ErrorResponseBytes:  256,
		MaxInFlightBytes:    1024,
		MaxInFlightFiles:    1,
		MaxQueuedFiles:      0,
		OrphanMinimumAge:    time.Hour,
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("new spool controller: %v", err)
	}
	buffered, err := bufferGatewayUpstreamResponse(context.Background(), controller, strings.NewReader(`{"error":{"message":"\uD800"}}`), "/v1/responses", nil)
	if !errors.Is(err, errGatewayResponseProjectionInvalid) || !buffered.FirstBodyObserved || buffered.Spool != nil {
		t.Fatalf("projection failure buffered=%+v err=%v, want body-started without replay spool", buffered, err)
	}
}

func runGatewayProtocolSpoolP1Request(svc *gateway.Service, deps *Deps, decision *gatewayRouteDecision, path, body string, smartRouting bool) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if deps == nil {
		deps = &Deps{}
	}
	deps.Gateway = svc
	gatewayForward(ctx, deps, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: smartRouting}, decision, "gpt-test", "", []byte(body), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())
	return recorder
}

type gatewayProtocolSpoolP1ReadErrorBody struct {
	err error
}

func (b gatewayProtocolSpoolP1ReadErrorBody) Read([]byte) (int, error) {
	return 0, b.err
}

func (gatewayProtocolSpoolP1ReadErrorBody) Close() error {
	return nil
}

func TestGatewayForwardLegacyCompletionsSSEDoesNotRetryOrSynthesizeFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name  string
		smart bool
	}{
		{name: "smart", smart: true},
		{name: "fixed", smart: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var primaryCalls atomic.Int32
			var fallbackCalls atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Oneapi-Request-Id", "req-legacy-completion")
				_, _ = w.Write([]byte("data: {\"id\":\"cmpl_legacy\",\"object\":\"text_completion\",\"choices\":[{\"text\":\"legacy completion\",\"index\":0,\"logprobs\":null,\"finish_reason\":null}]}\n\ndata: [DONE]\n\n"))
			}))
			defer primary.Close()
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("data: {\"id\":\"cmpl_fallback\",\"object\":\"text_completion\",\"choices\":[{\"text\":\"unexpected fallback\",\"index\":0,\"finish_reason\":null}]}\n\ndata: [DONE]\n\n"))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			decision.SmartRouting = tc.smart
			body := `{"model":"gpt-test","prompt":"hello","stream":true}`
			recorder := runGatewayProtocolSpoolP1Request(svc, nil, decision, "/v1/completions", body, tc.smart)
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "legacy completion") || !strings.Contains(recorder.Body.String(), "data: [DONE]") || strings.Contains(recorder.Body.String(), gatewayPublicStreamFailureMessage) || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
				t.Fatalf("legacy completion response=%d body=%q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
			if err != nil || len(logs) != 1 {
				t.Fatalf("legacy completion logs=%+v err=%v", logs, err)
			}
			if !logs[0].UpstreamAccepted || logs[0].UpstreamRequestID != "req-legacy-completion" || logs[0].StatusCode != http.StatusOK || logs[0].Error != "" {
				t.Fatalf("legacy completion audit=%+v", logs[0])
			}
			key, err := store.FindUpstreamKey(1, "primary")
			if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
				t.Fatalf("legacy completion changed routing health: key=%+v err=%v", key, err)
			}
		})
	}
}

func TestGatewayForwardLegacyCompletionsSSEMalformedDoneStopsAfterAcceptance(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name  string
		smart bool
	}{
		{name: "smart", smart: true},
		{name: "fixed", smart: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var primaryCalls atomic.Int32
			var fallbackCalls atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Oneapi-Request-Id", "req-legacy-malformed-done")
				_, _ = w.Write([]byte("data: {\"id\":\"cmpl_legacy\",\"object\":\"text_completion\",\"choices\":[{\"text\":\"partial\",\"index\":0,\"finish_reason\":null}]}\n\n data: [DONE]\n\n"))
			}))
			defer primary.Close()
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls.Add(1)
				_, _ = w.Write([]byte("unexpected fallback"))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			decision.SmartRouting = tc.smart
			body := `{"model":"gpt-test","prompt":"hello","stream":true}`
			recorder := runGatewayProtocolSpoolP1Request(svc, nil, decision, "/v1/completions", body, tc.smart)
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "partial") || !strings.Contains(recorder.Body.String(), gatewayPublicStreamFailureMessage) || strings.Contains(recorder.Body.String(), "[DONE]") || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
				t.Fatalf("malformed legacy completion response=%d body=%q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
			if err != nil || len(logs) != 1 {
				t.Fatalf("malformed legacy completion logs=%+v err=%v", logs, err)
			}
			if !logs[0].UpstreamAccepted || logs[0].UpstreamRequestID != "req-legacy-malformed-done" || logs[0].StatusCode != http.StatusBadGateway || logs[0].Error == "" {
				t.Fatalf("malformed legacy completion audit=%+v", logs[0])
			}
			key, err := store.FindUpstreamKey(1, "primary")
			if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
				t.Fatalf("malformed legacy completion changed routing health: key=%+v err=%v", key, err)
			}
		})
	}
}

func TestGatewayForwardLocalResponseSpoolFailureStopsAfterPhysicalDispatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name       string
		timeout    time.Duration
		controller func(t *testing.T) (*gatewayUpstreamResponseSpoolController, func())
	}{
		{
			name: "admission capacity",
			controller: func(t *testing.T) (*gatewayUpstreamResponseSpoolController, func()) {
				t.Helper()
				controller, _, err := newGatewayUpstreamResponseSpoolController(GatewayUpstreamResponseSpoolLimits{
					TempDir:             gatewayResponseSpoolTestDirectory(t),
					MaxResponseBytes:    1024,
					PrefixBytes:         128,
					ModelsResponseBytes: 256,
					ErrorResponseBytes:  256,
					MaxInFlightBytes:    1024,
					MaxInFlightFiles:    1,
					MaxQueuedFiles:      0,
					OrphanMinimumAge:    time.Hour,
				}, time.Now().UTC())
				if err != nil {
					t.Fatalf("new spool controller: %v", err)
				}
				lease, err := controller.admission.Acquire(context.Background(), controller.maxResponseBytes)
				if err != nil {
					t.Fatalf("reserve response spool admission: %v", err)
				}
				return controller, lease.Release
			},
		},
		{
			name: "directory creation validation",
			controller: func(t *testing.T) (*gatewayUpstreamResponseSpoolController, func()) {
				t.Helper()
				controller, _, err := newGatewayUpstreamResponseSpoolController(GatewayUpstreamResponseSpoolLimits{
					TempDir:             gatewayResponseSpoolTestDirectory(t),
					MaxResponseBytes:    1024,
					PrefixBytes:         128,
					ModelsResponseBytes: 256,
					ErrorResponseBytes:  256,
					MaxInFlightBytes:    1024,
					MaxInFlightFiles:    1,
					MaxQueuedFiles:      0,
					OrphanMinimumAge:    time.Hour,
				}, time.Now().UTC())
				if err != nil {
					t.Fatalf("new spool controller: %v", err)
				}
				file := filepath.Join(t.TempDir(), "not-a-spool-directory")
				if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
					t.Fatalf("create invalid spool path: %v", err)
				}
				controller.tempDir = file
				return controller, func() {}
			},
		},
		{
			name:    "queued admission first-byte deadline",
			timeout: 500 * time.Millisecond,
			controller: func(t *testing.T) (*gatewayUpstreamResponseSpoolController, func()) {
				t.Helper()
				controller, _, err := newGatewayUpstreamResponseSpoolController(GatewayUpstreamResponseSpoolLimits{
					TempDir:             gatewayResponseSpoolTestDirectory(t),
					MaxResponseBytes:    1024,
					PrefixBytes:         128,
					ModelsResponseBytes: 256,
					ErrorResponseBytes:  256,
					MaxInFlightBytes:    1024,
					MaxInFlightFiles:    1,
					MaxQueuedFiles:      1,
					OrphanMinimumAge:    time.Hour,
				}, time.Now().UTC())
				if err != nil {
					t.Fatalf("new spool controller: %v", err)
				}
				lease, err := controller.admission.Acquire(context.Background(), controller.maxResponseBytes)
				if err != nil {
					t.Fatalf("reserve queued response spool admission: %v", err)
				}
				return controller, lease.Release
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var primaryCalls atomic.Int32
			var fallbackCalls atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Oneapi-Request-Id", "req-local-spool")
				_, _ = w.Write([]byte(`{"id":"resp_local_spool","object":"response","status":"completed","output":[]}`))
			}))
			defer primary.Close()
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"resp_fallback","output":[]}`))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			controller, cleanup := tc.controller(t)
			defer cleanup()
			if tc.timeout > 0 {
				svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: tc.timeout}})
			}
			body := `{"model":"gpt-test"}`
			recorder := runGatewayProtocolSpoolP1Request(svc, &Deps{gatewayResponseSpool: controller}, decision, "/v1/responses", body, true)
			if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
				t.Fatalf("local spool response=%d body=%q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
			}
			key, err := store.FindUpstreamKey(1, "primary")
			if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
				t.Fatalf("local spool failure changed routing health: key=%+v err=%v", key, err)
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
			if err != nil || len(logs) != 1 {
				t.Fatalf("local spool logs=%+v err=%v", logs, err)
			}
			if logs[0].ChannelID != 1 || logs[0].UpstreamAccepted || logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-local-spool" || !strings.Contains(logs[0].Error, "local response spool unavailable after physical dispatch") {
				t.Fatalf("local spool audit=%+v", logs[0])
			}
			var attempts []gatewayRouteAttemptTrace
			if err := json.Unmarshal([]byte(logs[0].RouteAttemptedChannels), &attempts); err != nil || len(attempts) != 1 || !attempts[0].Physical || attempts[0].ChannelID != 1 || attempts[0].Reason != "response_spool_local" {
				t.Fatalf("local spool physical dispatch evidence=%q traces=%+v err=%v", logs[0].RouteAttemptedChannels, attempts, err)
			}
			if logs[0].PriorPhysicallyDispatchedFailureCount != 0 || logs[0].TerminalFailureCode != "response_spool_local" {
				t.Fatalf("local spool telemetry = prior %d terminal %q", logs[0].PriorPhysicallyDispatchedFailureCount, logs[0].TerminalFailureCode)
			}
		})
	}
}

func TestGatewayForwardCommittedImageLocalResponseSpoolFailureDoesNotChangeRoutingHealth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	newController := func(t *testing.T, maxQueuedFiles int64) *gatewayUpstreamResponseSpoolController {
		t.Helper()
		controller, _, err := newGatewayUpstreamResponseSpoolController(GatewayUpstreamResponseSpoolLimits{
			TempDir:             gatewayResponseSpoolTestDirectory(t),
			MaxResponseBytes:    1024,
			PrefixBytes:         128,
			ModelsResponseBytes: 256,
			ErrorResponseBytes:  256,
			MaxInFlightBytes:    1024,
			MaxInFlightFiles:    1,
			MaxQueuedFiles:      maxQueuedFiles,
			OrphanMinimumAge:    time.Hour,
		}, time.Now().UTC())
		if err != nil {
			t.Fatalf("new spool controller: %v", err)
		}
		return controller
	}

	for _, tc := range []struct {
		name              string
		wantReason        string
		responseBodyError error
		controller        func(t *testing.T) (*gatewayUpstreamResponseSpoolController, func())
	}{
		{
			name:       "admission capacity",
			wantReason: "response_spool_capacity",
			controller: func(t *testing.T) (*gatewayUpstreamResponseSpoolController, func()) {
				controller := newController(t, 0)
				lease, err := controller.admission.Acquire(context.Background(), controller.maxResponseBytes)
				if err != nil {
					t.Fatalf("reserve response spool admission: %v", err)
				}
				return controller, lease.Release
			},
		},
		{
			name:       "missing spool path",
			wantReason: "response_spool_local",
			controller: func(t *testing.T) (*gatewayUpstreamResponseSpoolController, func()) {
				controller := newController(t, 0)
				controller.tempDir = filepath.Join(t.TempDir(), "missing-spool-directory")
				return controller, func() {}
			},
		},
		{
			name:              "queued admission wait cancellation",
			wantReason:        "response_spool_local",
			responseBodyError: gatewayResponseSpoolQueueWaitError(errGatewayFirstByteTimeout),
			controller: func(t *testing.T) (*gatewayUpstreamResponseSpoolController, func()) {
				return newController(t, 1), func() {}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var primaryCalls atomic.Int32
			var fallbackCalls atomic.Int32
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Oneapi-Request-Id", "req-image-local-spool")
				_, _ = w.Write([]byte(`{"data":[{"url":"https://example.invalid/image.png"}]}`))
			}))
			defer primary.Close()
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls.Add(1)
				_, _ = w.Write([]byte(`{"data":[{"url":"unexpected"}]}`))
			}))
			defer fallback.Close()
			if tc.responseBodyError != nil {
				originalClient := gatewayHTTPClient
				gatewayHTTPClient = &http.Client{Transport: gatewayTestRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
					if strings.HasPrefix(request.URL.String(), primary.URL) {
						primaryCalls.Add(1)
					} else {
						fallbackCalls.Add(1)
					}
					header := make(http.Header)
					header.Set("Content-Type", "application/json")
					header.Set("X-Oneapi-Request-Id", "req-image-local-spool")
					return &http.Response{
						StatusCode:    http.StatusOK,
						Header:        header,
						Body:          gatewayProtocolSpoolP1ReadErrorBody{err: tc.responseBodyError},
						ContentLength: -1,
						Request:       request,
					}, nil
				})}
				t.Cleanup(func() { gatewayHTTPClient = originalClient })
			}

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			controller, cleanup := tc.controller(t)
			defer cleanup()
			body := `{"model":"gpt-test","prompt":"test"}`
			recorder := runGatewayProtocolSpoolP1Request(svc, &Deps{gatewayResponseSpool: controller}, decision, "/v1/images/generations", body, true)
			if recorder.Code != http.StatusBadGateway || primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 {
				t.Fatalf("committed image local spool response=%d body=%q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls.Load(), fallbackCalls.Load())
			}
			key, err := store.FindUpstreamKey(1, "primary")
			if err != nil || key.LastRoutingFailureKind != "" || key.LastRoutingFailureStatus != 0 || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
				t.Fatalf("committed image local spool failure changed routing health: key=%+v err=%v", key, err)
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
			if err != nil || len(logs) != 1 {
				t.Fatalf("committed image local spool logs=%+v err=%v", logs, err)
			}
			if logs[0].ChannelID != 1 || logs[0].UpstreamAccepted || !logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-image-local-spool" {
				t.Fatalf("committed image local spool audit=%+v", logs[0])
			}
			var attempts []gatewayRouteAttemptTrace
			if err := json.Unmarshal([]byte(logs[0].RouteAttemptedChannels), &attempts); err != nil || len(attempts) != 1 || !attempts[0].Physical || attempts[0].ChannelID != 1 || attempts[0].Reason != tc.wantReason {
				t.Fatalf("committed image local spool attempt=%q traces=%+v err=%v", logs[0].RouteAttemptedChannels, attempts, err)
			}
		})
	}
}

func TestGatewayForwardImagePairedPhysicalBillingCommitUnknownNeverReachesRoundTripper(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var serviceLogs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&serviceLogs, nil))
	originalClient := gatewayHTTPClient
	var baseRoundTrips atomic.Int32
	var basePath atomic.Value
	gatewayHTTPClient = &http.Client{Transport: gatewayTestRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/v1/models" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"object":"list","data":[{"id":"gpt-test"}]}`)),
				Request:    request,
			}, nil
		}
		baseRoundTrips.Add(1)
		basePath.Store(request.URL.Path)
		return nil, errors.New("base RoundTrip must not run after paired physical/billing commit ambiguity")
	})}
	t.Cleanup(func() { gatewayHTTPClient = originalClient })

	fixture := setupGatewayOwnerLossAPIFixture(t, "https://paired-commit-unknown.invalid", "https://unused-fallback.invalid")
	fixture.decision.Candidates = fixture.decision.Candidates[:1]
	fixture.decision.RankedCandidates = append([]gateway.Candidate(nil), fixture.decision.Candidates...)

	faultDB := fixture.db.Session(&gorm.Session{NewDB: true})
	pool := &gatewayPairedPhysicalBillingCommitUnknownPool{ConnPool: faultDB.Statement.ConnPool}
	faultDB.Statement.ConnPool = pool
	fixture.service.Store = storage.NewGateway(faultDB)

	body := []byte(`{"model":"gpt-test","prompt":"draw"}`)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(string(body)))
	if !gatewayForward(
		ctx,
		&Deps{Gateway: fixture.service, Log: logger},
		&storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true},
		fixture.decision,
		"gpt-test",
		"",
		body,
		gatewayModelMappingResult{Model: "gpt-test"},
		time.Now(),
	) {
		t.Fatal("gateway image forward did not handle paired commit-unknown failure")
	}
	if baseRoundTrips.Load() != 0 {
		t.Fatalf("base RoundTrip calls = %d path=%v, want 0 after pre-RoundTripper commit ambiguity (commits=%d marker_commit=%d injected=%t)", baseRoundTrips.Load(), basePath.Load(), pool.commits.Load(), pool.markerAt.Load(), pool.injected.Load())
	}
	if !pool.injected.Load() || pool.armCalls.Load() != 1 {
		var diagnosticAudits []storage.GatewayRequestDispatchAudit
		var diagnosticLogs []storage.GatewayRequestLog
		_ = fixture.db.Order("id ASC").Find(&diagnosticAudits).Error
		_ = fixture.db.Order("id ASC").Find(&diagnosticLogs).Error
		t.Fatalf("paired commit-unknown injection = injected:%t arms:%d commits:%d marker_commit:%d response:%d body:%q service_logs:%q audits:%+v request_logs:%+v, want true/1", pool.injected.Load(), pool.armCalls.Load(), pool.commits.Load(), pool.markerAt.Load(), recorder.Code, recorder.Body.String(), serviceLogs.String(), diagnosticAudits, diagnosticLogs)
	}
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
		t.Fatalf("paired commit-unknown response = %d retry-after=%q body=%q, want retryable durable-state failure", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
	}

	var audits []storage.GatewayRequestDispatchAudit
	if err := fixture.db.Order("id ASC").Find(&audits).Error; err != nil {
		t.Fatalf("list paired commit-unknown dispatch audits: %v", err)
	}
	if len(audits) != 1 {
		t.Fatalf("paired commit-unknown dispatch audits = %d, want one", len(audits))
	}
	audit := audits[0]
	if audit.PhysicallyDispatched || audit.BillingCommitted || audit.ResponseObservedAt != nil || audit.AcceptedAt != nil || audit.ProviderRequestID != "" || audit.AcceptedEventType != "" || audit.AcceptedObjectID != "" {
		t.Fatalf("paired commit-unknown audit retained physical, billing, response, or acceptance evidence: %+v", audit)
	}
	if audit.RecoveryRequestedAt == nil || audit.RecoveryTerminalKind != storage.GatewayRequestDispatchTerminalOwnerLeaseLost || audit.RequestLogID != nil || audit.TerminalAt != nil {
		t.Fatalf("paired commit-unknown recovery handoff was not retained as unsent: %+v", audit)
	}

	var logs []storage.GatewayRequestLog
	if err := fixture.db.Order("id ASC").Find(&logs).Error; err != nil {
		t.Fatalf("list paired commit-unknown request logs: %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("paired commit-unknown created request log or cost evidence: %+v", logs)
	}
	var aggregates int64
	if err := fixture.db.Model(&storage.GatewayRequestAggregate{}).Count(&aggregates).Error; err != nil {
		t.Fatalf("count paired commit-unknown request aggregates: %v", err)
	}
	if aggregates != 0 {
		t.Fatalf("paired commit-unknown created %d request aggregates without a billable request", aggregates)
	}
}

func TestGatewayForwardImagePairedCommitUnknownHandoffFailureStaysNonBillable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var serviceLogs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&serviceLogs, nil))
	originalClient := gatewayHTTPClient
	var baseRoundTrips atomic.Int32
	var basePath atomic.Value
	gatewayHTTPClient = &http.Client{Transport: gatewayTestRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/v1/models" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"object":"list","data":[{"id":"gpt-test"}]}`)),
				Request:    request,
			}, nil
		}
		baseRoundTrips.Add(1)
		basePath.Store(request.URL.Path)
		return nil, errors.New("base RoundTrip must not run after paired physical/billing commit ambiguity")
	})}
	t.Cleanup(func() { gatewayHTTPClient = originalClient })

	fixture := setupGatewayOwnerLossAPIFixture(t, "https://paired-handoff-failure.invalid", "https://unused-fallback.invalid")
	fixture.decision.Candidates = fixture.decision.Candidates[:1]
	fixture.decision.RankedCandidates = append([]gateway.Candidate(nil), fixture.decision.Candidates...)

	faultDB := fixture.db.Session(&gorm.Session{NewDB: true})
	pool := &gatewayPairedPhysicalBillingCommitUnknownPool{ConnPool: faultDB.Statement.ConnPool}
	faultDB.Statement.ConnPool = pool
	const failHandoffCallback = "gateway_protocol_spool_p1:fail_paired_commit_unknown_handoff"
	injectedHandoffErr := errors.New("inject paired known-unsent handoff failure")
	var handoffFailures atomic.Int32
	if err := faultDB.Callback().Update().Before("gorm:update").Register(failHandoffCallback, func(tx *gorm.DB) {
		if tx == nil || tx.Error != nil || tx.Statement == nil || tx.Statement.Table != "gateway_request_dispatch_audits" || !pool.injected.Load() {
			return
		}
		updates, ok := tx.Statement.Dest.(map[string]any)
		if !ok {
			return
		}
		physical, physicalPresent := updates["physically_dispatched"].(bool)
		billing, billingPresent := updates["billing_committed"].(bool)
		transportState, statePresent := updates["transport_state"].(string)
		if physicalPresent && billingPresent && statePresent && !physical && !billing && transportState == storage.GatewayRequestDispatchTransportStateNotDispatched && handoffFailures.CompareAndSwap(0, 1) {
			tx.AddError(injectedHandoffErr)
		}
	}); err != nil {
		t.Fatalf("register paired handoff failure callback: %v", err)
	}
	t.Cleanup(func() {
		if err := faultDB.Callback().Update().Remove(failHandoffCallback); err != nil {
			t.Errorf("remove paired handoff failure callback: %v", err)
		}
	})
	fixture.service.Store = storage.NewGateway(faultDB)

	body := []byte(`{"model":"gpt-test","prompt":"draw"}`)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(string(body)))
	if !gatewayForward(
		ctx,
		&Deps{Gateway: fixture.service, Log: logger},
		&storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true},
		fixture.decision,
		"gpt-test",
		"",
		body,
		gatewayModelMappingResult{Model: "gpt-test"},
		time.Now(),
	) {
		t.Fatal("gateway image forward did not handle failed paired commit-unknown handoff")
	}
	if baseRoundTrips.Load() != 0 || !pool.injected.Load() || handoffFailures.Load() != 1 {
		var diagnosticAudits []storage.GatewayRequestDispatchAudit
		var diagnosticLogs []storage.GatewayRequestLog
		_ = fixture.db.Order("id ASC").Find(&diagnosticAudits).Error
		_ = fixture.db.Order("id ASC").Find(&diagnosticLogs).Error
		t.Fatalf("paired handoff failure injection: base=%d path=%v commit_unknown=%t handoff_failures=%d commits=%d marker_commit=%d response=%d body=%q service_logs:%q audits:%+v request_logs:%+v", baseRoundTrips.Load(), basePath.Load(), pool.injected.Load(), handoffFailures.Load(), pool.commits.Load(), pool.markerAt.Load(), recorder.Code, recorder.Body.String(), serviceLogs.String(), diagnosticAudits, diagnosticLogs)
	}
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
		t.Fatalf("paired handoff failure response = %d retry-after=%q body=%q", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
	}

	var audit storage.GatewayRequestDispatchAudit
	if err := fixture.db.Order("id ASC").First(&audit).Error; err != nil {
		t.Fatalf("load paired handoff failure audit: %v", err)
	}
	if !audit.PhysicallyDispatched || !audit.BillingCommitted || audit.TransportState != storage.GatewayRequestDispatchTransportStateArmed || audit.RecoveryRequestedAt != nil || audit.TerminalAt != nil {
		t.Fatalf("failed dedicated handoff was hidden by a generic handoff: %+v", audit)
	}
	var handoffCount int64
	if err := fixture.db.Model(&storage.GatewayRequestDispatchTerminalHandoff{}).Where("dispatch_id = ?", audit.ID).Count(&handoffCount).Error; err != nil || handoffCount != 0 {
		t.Fatalf("generic handoff survived dedicated failure: count=%d err=%v", handoffCount, err)
	}

	stopContext, stopCancel := context.WithTimeout(context.Background(), time.Second)
	if err := fixture.service.StopRequestDispatchOwner(stopContext, time.Now()); err != nil {
		stopCancel()
		t.Fatalf("stop failed dispatch owner: %v", err)
	}
	stopCancel()
	if err := fixture.service.RecoverRequestDispatchOrphans(time.Now().Add(2 * time.Hour)); err != nil {
		t.Fatalf("recover failed paired handoff audit: %v", err)
	}
	if err := fixture.db.First(&audit, "id = ?", audit.ID).Error; err != nil {
		t.Fatalf("reload paired handoff failure audit: %v", err)
	}
	if audit.PhysicallyDispatched || audit.BillingCommitted || audit.TransportState != storage.GatewayRequestDispatchTransportStateManualReview || audit.TerminalAt == nil || audit.RequestLogID != nil {
		t.Fatalf("failed paired handoff recovery was not nonbillable manual review: %+v", audit)
	}
	var logs int64
	if err := fixture.db.Model(&storage.GatewayRequestLog{}).Count(&logs).Error; err != nil || logs != 0 {
		t.Fatalf("failed paired handoff created request evidence: logs=%d err=%v", logs, err)
	}
	var aggregates int64
	if err := fixture.db.Model(&storage.GatewayRequestAggregate{}).Count(&aggregates).Error; err != nil || aggregates != 0 {
		t.Fatalf("failed paired handoff created billable aggregates: aggregates=%d err=%v", aggregates, err)
	}
}
