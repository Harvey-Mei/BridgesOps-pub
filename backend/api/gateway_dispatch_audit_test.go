package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gatewayservice "github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type gatewayTestRoundTripperFunc func(*http.Request) (*http.Response, error)

func (fn gatewayTestRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type gatewayTestResponseBody struct {
	closed atomic.Bool
}

func (*gatewayTestResponseBody) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (b *gatewayTestResponseBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestGatewayDispatchConfirmationFailureFreezesTransportOutcome(t *testing.T) {
	now := time.Now().UTC()
	audit := &gatewayDispatchAuditState{
		id:                   991,
		physicallyDispatched: true,
		telemetryVersion:     storage.GatewayRequestTelemetryVersion,
	}
	failure := &gatewayTransportConfirmationFailure{
		StatusCode:       http.StatusTooManyRequests,
		ResponseObserved: true,
		Header: http.Header{
			"X-Oneapi-Request-Id": []string{"provider-confirm-failure"},
		},
		TransportErr: errors.New("temporary transport failure containing sk-private-value"),
		ConfirmErr:   errors.New("database unavailable"),
	}
	log := gatewayDispatchConfirmationFailureLog(audit, failure, storage.ChannelTypeNewAPI, "sk-private-value", now.Add(-time.Second), now)
	if log == nil || log.StatusCode != http.StatusTooManyRequests || log.UpstreamRequestID != "provider-confirm-failure" {
		t.Fatalf("frozen confirmation outcome = %+v", log)
	}
	if strings.Contains(log.Error, "sk-private-value") || !strings.Contains(log.Error, "temporary transport failure") {
		t.Fatalf("confirmation transport error was not safely frozen: %q", log.Error)
	}
	if audit.providerRequestID != "provider-confirm-failure" || audit.responseObservedAt.IsZero() {
		t.Fatalf("confirmation facts were not frozen: %+v", audit)
	}
}

func TestGatewayDispatchConfirmationFailureFreezesResponseWithoutProviderID(t *testing.T) {
	now := time.Now().UTC()
	audit := &gatewayDispatchAuditState{id: 992, physicallyDispatched: true}
	log := gatewayDispatchConfirmationFailureLog(audit, &gatewayTransportConfirmationFailure{
		StatusCode:       http.StatusServiceUnavailable,
		ResponseObserved: true,
		ConfirmErr:       errors.New("database unavailable"),
	}, storage.ChannelTypeNewAPI, "", now.Add(-time.Second), now)
	if log == nil || log.StatusCode != http.StatusServiceUnavailable || log.UpstreamRequestID != "" {
		t.Fatalf("frozen response without provider ID = %+v", log)
	}
	if audit.responseObservedAt.IsZero() || !audit.responseObservedAt.Equal(now) {
		t.Fatalf("response observation was not frozen without provider ID: %+v", audit)
	}
	if facts := audit.recoveryFacts(); facts.ResponseObservedAt.IsZero() || facts.ProviderRequestID != "" {
		t.Fatalf("recovery facts without provider ID = %+v", facts)
	}
}

func TestForwardGatewayRequestPreparationHookFailurePreventsDo(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	wantErr := errors.New("begin audit failed")
	response, err := forwardGatewayRequestOnDispatch(
		request,
		gatewayResolvedTarget{SiteURL: upstream.URL},
		"sk-test",
		[]byte(`{"model":"gpt-test"}`),
		func() error { return wantErr },
		nil,
		nil,
	)
	if response != nil || !errors.Is(err, wantErr) {
		t.Fatalf("response=%v err=%v, want hook error", response, err)
	}
	if calls := upstreamCalls.Load(); calls != 0 {
		t.Fatalf("upstream Do calls=%d, want 0", calls)
	}
}

func TestForwardGatewayRequestPhysicalMarkFailurePreventsRoundTrip(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	wantErr := errors.New("physical dispatch audit failed")
	var confirmCalls atomic.Int32
	response, err := forwardGatewayRequestOnDispatch(
		request,
		gatewayResolvedTarget{SiteURL: upstream.URL},
		"sk-test",
		[]byte(`{"model":"gpt-test"}`),
		nil,
		func() error { return wantErr },
		func() error {
			confirmCalls.Add(1)
			return nil
		},
	)
	if response != nil || !errors.Is(err, wantErr) {
		t.Fatalf("response=%v err=%v, want physical mark error", response, err)
	}
	if calls := upstreamCalls.Load(); calls != 0 {
		t.Fatalf("upstream calls=%d, want 0 after durable mark failure", calls)
	}
	if calls := confirmCalls.Load(); calls != 0 {
		t.Fatalf("transport confirmation calls=%d, want 0 before RoundTripper entry", calls)
	}
}

func TestForwardGatewayRequestDispatchHooksRunOncePerDo(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	var beginCalls atomic.Int32
	var physicalCalls atomic.Int32
	var confirmCalls atomic.Int32
	response, err := forwardGatewayRequestOnDispatch(
		request,
		gatewayResolvedTarget{SiteURL: upstream.URL},
		"sk-test",
		[]byte(`{"model":"gpt-test"}`),
		func() error {
			beginCalls.Add(1)
			return nil
		},
		func() error {
			physicalCalls.Add(1)
			return nil
		},
		func() error {
			confirmCalls.Add(1)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("forward request: %v", err)
	}
	_ = response.Body.Close()
	if beginCalls.Load() != 1 || physicalCalls.Load() != 1 || upstreamCalls.Load() != 1 || confirmCalls.Load() != 1 {
		t.Fatalf("begin/physical/upstream/confirm calls=%d/%d/%d/%d, want 1/1/1/1", beginCalls.Load(), physicalCalls.Load(), upstreamCalls.Load(), confirmCalls.Load())
	}
}

func TestForwardGatewayRequestCanceledAfterPreparationDoesNotMarkPhysical(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	ctx, cancel := context.WithCancel(request.Context())
	request = request.WithContext(ctx)
	var prepareCalls atomic.Int32
	var physicalCalls atomic.Int32
	var confirmCalls atomic.Int32
	response, err := forwardGatewayRequestOnDispatch(
		request,
		gatewayResolvedTarget{SiteURL: upstream.URL},
		"sk-test",
		[]byte(`{"model":"gpt-test"}`),
		func() error {
			prepareCalls.Add(1)
			cancel()
			return nil
		},
		func() error {
			physicalCalls.Add(1)
			return nil
		},
		func() error {
			confirmCalls.Add(1)
			return nil
		},
	)
	if response != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("response=%v err=%v, want context canceled", response, err)
	}
	if prepareCalls.Load() != 1 || physicalCalls.Load() != 0 || upstreamCalls.Load() != 0 || confirmCalls.Load() != 0 {
		t.Fatalf("prepare/physical/upstream/confirm calls=%d/%d/%d/%d, want 1/0/0/0", prepareCalls.Load(), physicalCalls.Load(), upstreamCalls.Load(), confirmCalls.Load())
	}
}

func TestForwardGatewayRequestInvalidHeaderDoesNotMarkPhysical(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	request.Header["X-Invalid-Upstream-Header"] = []string{"invalid\nvalue"}
	var prepareCalls atomic.Int32
	var physicalCalls atomic.Int32
	response, err := forwardGatewayRequestOnDispatch(
		request,
		gatewayResolvedTarget{SiteURL: upstream.URL},
		"sk-test",
		[]byte(`{"model":"gpt-test"}`),
		func() error {
			prepareCalls.Add(1)
			return nil
		},
		func() error {
			physicalCalls.Add(1)
			return nil
		},
		nil,
	)
	if response != nil || err == nil {
		t.Fatalf("response=%v err=%v, want invalid-header failure", response, err)
	}
	if prepareCalls.Load() != 1 || physicalCalls.Load() != 0 || upstreamCalls.Load() != 0 {
		t.Fatalf("prepare calls=%d physical calls=%d upstream calls=%d, want 1/0/0", prepareCalls.Load(), physicalCalls.Load(), upstreamCalls.Load())
	}
}

func TestForwardGatewayRequestTransportEntryMarksDialFailure(t *testing.T) {
	originalClient := gatewayHTTPClient
	t.Cleanup(func() { gatewayHTTPClient = originalClient })

	wantErr := errors.New("transport failed")
	var transportCalls atomic.Int32
	gatewayHTTPClient = &http.Client{Transport: gatewayTestRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		transportCalls.Add(1)
		return nil, wantErr
	})}

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	var prepareCalls atomic.Int32
	var physicalCalls atomic.Int32
	var confirmCalls atomic.Int32
	response, err := forwardGatewayRequestOnDispatch(
		request,
		gatewayResolvedTarget{SiteURL: "https://upstream.invalid"},
		"sk-test",
		[]byte(`{"model":"gpt-test"}`),
		func() error {
			prepareCalls.Add(1)
			return nil
		},
		func() error {
			physicalCalls.Add(1)
			return nil
		},
		func() error {
			confirmCalls.Add(1)
			return nil
		},
	)
	if response != nil || !errors.Is(err, wantErr) {
		t.Fatalf("response=%v err=%v, want transport failure", response, err)
	}
	if prepareCalls.Load() != 1 || physicalCalls.Load() != 1 || transportCalls.Load() != 1 || confirmCalls.Load() != 1 {
		t.Fatalf("prepare/physical/transport/confirm calls=%d/%d/%d/%d, want 1/1/1/1", prepareCalls.Load(), physicalCalls.Load(), transportCalls.Load(), confirmCalls.Load())
	}
}

func TestForwardGatewayRequestConfirmationFailurePreservesTransportOutcome(t *testing.T) {
	originalClient := gatewayHTTPClient
	t.Cleanup(func() { gatewayHTTPClient = originalClient })

	t.Run("successful transport response is closed", func(t *testing.T) {
		body := &gatewayTestResponseBody{}
		var transportCalls atomic.Int32
		gatewayHTTPClient = &http.Client{Transport: gatewayTestRoundTripperFunc(func(*http.Request) (*http.Response, error) {
			transportCalls.Add(1)
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
		})}
		confirmErr := storage.ErrGatewayRequestDispatchCommitUnknown
		var confirmCalls atomic.Int32
		response, err := forwardGatewayRequestOnDispatch(
			httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`)),
			gatewayResolvedTarget{SiteURL: "https://upstream.invalid"},
			"sk-test",
			[]byte(`{"model":"gpt-test"}`),
			nil,
			func() error { return nil },
			func() error {
				confirmCalls.Add(1)
				return confirmErr
			},
		)
		if response != nil || !errors.Is(err, errGatewayRequestTransportConfirm) || !errors.Is(err, confirmErr) {
			t.Fatalf("response=%v err=%v, want confirmation commit-unknown", response, err)
		}
		var outcome *gatewayTransportConfirmationFailure
		if !errors.As(err, &outcome) || outcome == nil || !outcome.ResponseObserved || outcome.StatusCode != http.StatusOK {
			t.Fatalf("confirmation outcome did not preserve observed HTTP response: %+v", outcome)
		}
		if transportCalls.Load() != 1 || confirmCalls.Load() != 1 || !body.closed.Load() {
			t.Fatalf("transport/confirm/closed=%d/%d/%t, want 1/1/true", transportCalls.Load(), confirmCalls.Load(), body.closed.Load())
		}
	})

	t.Run("transport error is retained", func(t *testing.T) {
		transportErr := errors.New("dial returned control")
		confirmErr := errors.New("confirm failed")
		var transportReturned atomic.Bool
		gatewayHTTPClient = &http.Client{Transport: gatewayTestRoundTripperFunc(func(*http.Request) (*http.Response, error) {
			transportReturned.Store(true)
			return nil, transportErr
		})}
		response, err := forwardGatewayRequestOnDispatch(
			httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`)),
			gatewayResolvedTarget{SiteURL: "https://upstream.invalid"},
			"sk-test",
			[]byte(`{"model":"gpt-test"}`),
			nil,
			func() error { return nil },
			func() error {
				if !transportReturned.Load() {
					t.Fatal("transport confirmation ran before RoundTrip returned")
				}
				return confirmErr
			},
		)
		if response != nil || !errors.Is(err, transportErr) || !errors.Is(err, errGatewayRequestTransportConfirm) || !errors.Is(err, confirmErr) {
			t.Fatalf("response=%v err=%v, want joined transport and confirmation errors", response, err)
		}
		var outcome *gatewayTransportConfirmationFailure
		if !errors.As(err, &outcome) || outcome == nil || outcome.ResponseObserved {
			t.Fatalf("transport-only failure incorrectly claimed an HTTP response: %+v", outcome)
		}
	})
}

func TestForwardGatewayRequestEmptyBodyPreservesZeroLengthWireFormat(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		upstreamCalls.Add(1)
		if request.ContentLength != 0 {
			t.Errorf("content length = %d, want 0", request.ContentLength)
		}
		if len(request.TransferEncoding) != 0 {
			t.Errorf("transfer encoding = %v, want none", request.TransferEncoding)
		}
		raw, err := io.ReadAll(request.Body)
		if err != nil || len(raw) != 0 {
			t.Errorf("empty body = %q err=%v", raw, err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/empty", nil)
	request.Header.Set("Idempotency-Key", "must-not-enable-transport-replay")
	var physicalCalls atomic.Int32
	response, err := forwardGatewayRequestOnDispatch(
		request,
		gatewayResolvedTarget{SiteURL: upstream.URL},
		"sk-test",
		nil,
		nil,
		func() error {
			physicalCalls.Add(1)
			return nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("forward empty request: %v", err)
	}
	_ = response.Body.Close()
	if physicalCalls.Load() != 1 || upstreamCalls.Load() != 1 {
		t.Fatalf("physical/upstream calls = %d/%d, want 1/1", physicalCalls.Load(), upstreamCalls.Load())
	}
}

func TestGatewaySingleAttemptEmptyBodyTransportUsesFreshHTTP1Transport(t *testing.T) {
	base := &http.Transport{ForceAttemptHTTP2: true}
	got, ok := gatewaySingleAttemptEmptyBodyTransport(base).(*http.Transport)
	if !ok {
		t.Fatalf("single-attempt transport type = %T", gatewaySingleAttemptEmptyBodyTransport(base))
	}
	if got == base {
		t.Fatal("single-attempt transport reused the shared transport instance")
	}
	if !got.DisableKeepAlives || got.ForceAttemptHTTP2 || got.TLSNextProto == nil {
		t.Fatalf("single-attempt transport = disable_keepalives:%t force_h2:%t tls_next_proto_nil:%t", got.DisableKeepAlives, got.ForceAttemptHTTP2, got.TLSNextProto == nil)
	}
}

func TestGatewayStreamAcceptanceEvidenceKeepsProtocolObjectIDSeparate(t *testing.T) {
	for _, test := range []struct {
		name      string
		path      string
		frame     string
		eventType string
		objectID  string
	}{
		{
			name:      "responses lifecycle",
			path:      "/v1/responses",
			frame:     "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_object_1\"}}\n\n",
			eventType: "response.created",
			objectID:  "resp_object_1",
		},
		{
			name:      "claude lifecycle",
			path:      "/v1/messages",
			frame:     "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_object_1\",\"role\":\"assistant\"}}\n\n",
			eventType: "message_start",
			objectID:  "msg_object_1",
		},
		{
			name:      "chat chunk",
			path:      "/v1/chat/completions",
			frame:     "data: {\"id\":\"chat_object_1\",\"object\":\"chat.completion.chunk\",\"choices\":[]}\n\n",
			eventType: "chat.completion.chunk",
			objectID:  "chat_object_1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			evidence, accepted := gatewayStreamFrameAcceptanceEvidence([]byte(test.frame), test.path)
			if !accepted || evidence.EventType != test.eventType || evidence.ObjectID != test.objectID {
				t.Fatalf("accepted=%t evidence=%+v", accepted, evidence)
			}
		})
	}
}

func TestGatewayStreamSanitizerMarkFailureSuppressesAcceptedFrame(t *testing.T) {
	original := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_private_object\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"must not pass\"}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(
		io.NopCloser(strings.NewReader(original)),
		&gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit},
		"sk-private",
		"/v1/responses",
	)
	markErr := errors.New("database unavailable")
	markCalls := 0
	sanitizer.onAcceptedEvidence = func(evidence gatewayStreamAcceptanceEvidence) error {
		markCalls++
		if evidence.EventType != "response.created" || evidence.ObjectID != "resp_private_object" {
			t.Fatalf("unexpected evidence: %+v", evidence)
		}
		return markErr
	}

	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read sanitized stream: %v", err)
	}
	if markCalls != 1 || !sanitizer.accepted || sanitizer.useful {
		t.Fatalf("markCalls=%d accepted=%t useful=%t", markCalls, sanitizer.accepted, sanitizer.useful)
	}
	if strings.Contains(string(output), "resp_private_object") || strings.Contains(string(output), "must not pass") {
		t.Fatalf("accepted upstream frame leaked after durable mark failure: %q", output)
	}
	if !strings.Contains(string(output), "event: response.failed") || !strings.Contains(sanitizer.ErrorText(), "persist upstream acceptance") {
		t.Fatalf("missing protocol terminal/error detail: output=%q error=%q", output, sanitizer.ErrorText())
	}
}

func TestGatewayDispatchFinalizeDoesNotDowngradeObservedAcceptance(t *testing.T) {
	state := &gatewayDispatchAuditState{
		id:                 17,
		acceptanceObserved: true,
		acceptanceEvidence: gatewayStreamAcceptanceEvidence{EventType: "message_start", ObjectID: "msg_17"},
	}
	log := &storage.GatewayRequestLog{Method: http.MethodPost, Path: "/v1/messages", StatusCode: http.StatusBadGateway}
	created, err := state.finalize(&gatewayservice.Service{}, log, gatewayDispatchTerminalFailedAfterAcceptance, time.Now().Add(-time.Second), time.Now())
	if err == nil || created || log.ID != 0 {
		t.Fatalf("created=%t logID=%d err=%v, want failed closed before unaccepted finalization", created, log.ID, err)
	}
}

func TestGatewayDispatchBillingCommitmentCannotCompleteWithoutFinalization(t *testing.T) {
	state := &gatewayDispatchAuditState{id: 23, billingCommitted: true}
	err := state.complete(gatewayDispatchTerminalFailedBeforeAcceptance, time.Now())
	if !errors.Is(err, storage.ErrGatewayRequestDispatchRequiresFinalization) || state.closed {
		t.Fatalf("complete billing-committed dispatch err=%v closed=%t", err, state.closed)
	}
}

func TestGatewayDispatchRecoveryPreservesProviderRequestIDBytes(t *testing.T) {
	const providerRequestID = " \u00a0provider-id\t"
	state := &gatewayDispatchAuditState{
		id:                29,
		providerRequestID: providerRequestID,
	}

	facts := state.recoveryFacts()
	if facts.ProviderRequestID != providerRequestID {
		t.Fatalf("recovery facts provider request ID = %q, want exact %q", facts.ProviderRequestID, providerRequestID)
	}
	log := gatewayDispatchRecoveryFallbackLog(
		state,
		gatewayDispatchTerminalFailedBeforeAcceptance,
		time.Unix(1, 0),
		time.Unix(2, 0),
	)
	if log == nil || log.RecordKind != storage.GatewayRequestRecordKindDispatchCostEvidence || log.UpstreamRequestID != providerRequestID {
		t.Fatalf("recovery fallback log = %+v, want exact provider request ID", log)
	}
}

func TestGatewayDispatchRecoveryFallbackPreservesUsageAndExplicitCost(t *testing.T) {
	state := &gatewayDispatchAuditState{
		id:                   31,
		physicallyDispatched: true,
		route: gatewayRouteSnapshot{Candidate: gatewayservice.Candidate{
			FundingFactor: 0.5,
		}},
	}
	metrics := parseGatewayUsageMetricsForPath([]byte(`{"usage":{"input_tokens":20,"output_tokens":4,"cache_read_tokens":0,"cache_write_tokens":0,"total_tokens":24,"actual_cost":0.25}}`), "/v1/responses")
	log := gatewayDispatchOwnerLossRecoveryLog(
		state,
		nil,
		state,
		gatewayUsageMetrics{},
		metrics,
		false,
		httptest.NewRequest(http.MethodPost, "/v1/responses", nil),
		time.Unix(1, 0),
		time.Unix(2, 0),
	)

	if log == nil || log.Method != http.MethodPost || log.Path != "/v1/responses" ||
		log.PromptTokens != 20 || log.CompletionTokens != 4 || log.TotalTokens != 24 ||
		!log.UsageInputPresent || !log.UsageOutputPresent || !log.UsageCacheReadPresent || !log.UsageCacheWritePresent {
		t.Fatalf("recovery fallback lost request usage: %+v", log)
	}
	if log.Cost != 0.125 || log.RawCost == nil || *log.RawCost != 0.25 ||
		log.CostEstimated || log.CostSource != "actual_cost" {
		t.Fatalf("recovery fallback lost explicit upstream cost: %+v", log)
	}
}

func TestGatewayForwardPhysicalRolloverMarkerFailureKeepsPriorPhysicalTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name              string
		providerRequestID string
	}{
		{name: "without provider request id"},
		{name: "with provider request id", providerRequestID: "req-first"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var serviceLogs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&serviceLogs, nil))
			var upstreamCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				upstreamCalls.Add(1)
				if tc.providerRequestID != "" {
					writer.Header().Set("X-Oneapi-Request-Id", tc.providerRequestID)
				}
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusServiceUnavailable)
				_, _ = writer.Write([]byte(`{"error":{"message":"retry"}}`))
			}))
			defer upstream.Close()

			db := openTestDB(t)
			svc, _, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
				channelID: 1, name: "primary", groupName: "primary", siteURL: upstream.URL, ratio: 1,
			}})
			faultDB := db.Session(&gorm.Session{NewDB: true})
			const callbackName = "gateway_dispatch_audit:fail_second_text_physical_marker"
			injectedErr := errors.New("inject second text physical marker failure")
			var physicalMarkerUpdates atomic.Int32
			var injected atomic.Bool
			if err := faultDB.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
				if tx == nil || tx.Error != nil || tx.Statement == nil || tx.Statement.Table != "gateway_request_dispatch_audits" {
					return
				}
				updates, ok := tx.Statement.Dest.(map[string]any)
				if !ok {
					return
				}
				physical, physicalPresent := updates["physically_dispatched"].(bool)
				_, billingPresent := updates["billing_committed"]
				if !physicalPresent || !physical || billingPresent {
					return
				}
				if physicalMarkerUpdates.Add(1) == 2 {
					injected.Store(true)
					tx.AddError(injectedErr)
				}
			}); err != nil {
				t.Fatalf("register physical marker failure callback: %v", err)
			}
			t.Cleanup(func() {
				if err := faultDB.Callback().Update().Remove(callbackName); err != nil {
					t.Errorf("remove physical marker failure callback: %v", err)
				}
			})
			svc.Store = storage.NewGateway(faultDB)

			body := []byte(`{"model":"gpt-test"}`)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
			if !gatewayForward(
				ctx,
				&Deps{Gateway: svc, Log: logger},
				&storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true},
				decision,
				"gpt-test",
				"",
				body,
				gatewayModelMappingResult{Model: "gpt-test"},
				time.Now(),
			) {
				t.Fatal("gateway forward did not handle marker failure")
			}
			if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
				var diagnosticAudits []storage.GatewayRequestDispatchAudit
				var diagnosticLogs []storage.GatewayRequestLog
				var diagnosticHandoff storage.GatewayRequestDispatchTerminalHandoff
				_ = db.Order("id").Find(&diagnosticAudits).Error
				_ = db.Order("id").Find(&diagnosticLogs).Error
				_ = db.Order("dispatch_id").First(&diagnosticHandoff).Error
				decodedHandoff, _ := storage.DecodeGatewayRequestDispatchTerminalHandoff(diagnosticHandoff)
				t.Fatalf("gateway response = %d %q, want normal 502; upstream=%d markers=%d injected=%t audits=%+v logs=%+v handoff=%+v handoff_log=%+v service_logs=%q", recorder.Code, recorder.Body.String(), upstreamCalls.Load(), physicalMarkerUpdates.Load(), injected.Load(), diagnosticAudits, diagnosticLogs, decodedHandoff, decodedHandoff.Log, serviceLogs.String())
			}
			if upstreamCalls.Load() != 1 || physicalMarkerUpdates.Load() != 2 || !injected.Load() {
				var diagnosticLogs []storage.GatewayRequestLog
				_ = db.Order("id").Find(&diagnosticLogs).Error
				t.Fatalf("upstream=%d physical marker updates=%d injected=%t, want 1/2/true; response=%q candidate=%+v logs=%+v", upstreamCalls.Load(), physicalMarkerUpdates.Load(), injected.Load(), recorder.Body.String(), decision.Candidates[0], diagnosticLogs)
			}

			var audits []storage.GatewayRequestDispatchAudit
			if err := db.Order("id").Find(&audits).Error; err != nil {
				t.Fatalf("list dispatch audits: %v", err)
			}
			if len(audits) != 2 {
				t.Fatalf("dispatch audits=%+v, want physical and failed-pre-Do attempts", audits)
			}
			var physical *storage.GatewayRequestDispatchAudit
			for index := range audits {
				audit := &audits[index]
				if audit.PhysicallyDispatched {
					if audit.Stage == storage.GatewayRequestDispatchStageCompleted {
						t.Fatalf("physical audit was completed instead of finalized: %+v", audit)
					}
					if physical != nil {
						t.Fatalf("more than one physical audit: first=%+v next=%+v", physical, audit)
					}
					physical = audit
				}
			}
			if physical == nil || physical.Stage != storage.GatewayRequestDispatchStageFinalized || physical.TerminalAt == nil || physical.RequestLogID == nil {
				t.Fatalf("sole physical audit was not finalized: %+v", physical)
			}

			var logs []storage.GatewayRequestLog
			if err := db.Order("id").Find(&logs).Error; err != nil {
				t.Fatalf("list request logs: %v", err)
			}
			if len(logs) != 1 || logs[0].RecordKind != storage.GatewayRequestRecordKindRequest || logs[0].StatusCode != http.StatusBadGateway || logs[0].ID != *physical.RequestLogID {
				t.Fatalf("terminal request log=%+v physical=%+v", logs, physical)
			}
			if logs[0].UpstreamRequestID != tc.providerRequestID || physical.ProviderRequestID != tc.providerRequestID {
				t.Fatalf("provider request id log=%q audit=%q want=%q", logs[0].UpstreamRequestID, physical.ProviderRequestID, tc.providerRequestID)
			}
			var evidenceCount int64
			if err := db.Model(&storage.GatewayRequestLog{}).Where("record_kind = ?", storage.GatewayRequestRecordKindDispatchCostEvidence).Count(&evidenceCount).Error; err != nil || evidenceCount != 0 {
				t.Fatalf("hidden dispatch evidence count=%d err=%v, want 0", evidenceCount, err)
			}
		})
	}
}

type gatewayOwnerLossAfterReader struct {
	reader *strings.Reader
	lost   atomic.Bool
}

func (r *gatewayOwnerLossAfterReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		return n, nil
	}
	r.lost.Store(true)
	if err == io.EOF {
		return 0, context.Canceled
	}
	return n, err
}

func (r *gatewayOwnerLossAfterReader) Close() error { return nil }

func TestGatewayStreamOwnerLossAfterVisibleOutputEmitsProtocolTerminal(t *testing.T) {
	source := &gatewayOwnerLossAfterReader{reader: strings.NewReader("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial output\"}\n\n")}
	sanitizer := newGatewaySSEStreamSanitizer(source, &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "", "/v1/responses")
	sanitizer.ownerLeaseLost = source.lost.Load
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read sanitized owner-loss stream: %v", err)
	}
	text := string(output)
	if !strings.Contains(text, "partial output") || !strings.Contains(text, "event: response.failed") || !strings.Contains(text, gatewayPublicStreamFailureMessage) {
		t.Fatalf("owner-loss stream terminal = %q", text)
	}
	if !sanitizer.OwnerLeaseLost() || !strings.Contains(sanitizer.ErrorText(), "owner lease loss") {
		t.Fatalf("owner loss was not retained for durable finalization: observed=%t error=%q", sanitizer.OwnerLeaseLost(), sanitizer.ErrorText())
	}
}
