package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

// gatewayProxyAdmissionHTTPTrackingReader makes this regression prove that a
// request waiting for admission is never decoded or retained after its client
// has gone away.
type gatewayProxyAdmissionHTTPTrackingReader struct {
	reader *strings.Reader
	reads  int
	bytes  int
}

func (r *gatewayProxyAdmissionHTTPTrackingReader) Read(p []byte) (int, error) {
	r.reads++
	n, err := r.reader.Read(p)
	r.bytes += n
	return n, err
}

func TestGatewayProxyQueuedAdmissionHTTPOutcomes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const (
		token       = "sk-gateway-admission-http-test"
		requestBody = `{"model":"gpt-test"}`
	)

	downstream := &storage.GatewayDownstreamKey{
		ID:           91,
		Name:         "gateway-admission-http",
		App:          storage.GatewayAppCodex,
		ModelFamily:  "codex",
		Enabled:      true,
		SmartRouting: true,
	}
	// The test exits at admission, so this service needs no storage. Its only
	// role here is preserving gatewayProxy's normal test-token handling.
	service := gateway.NewService(nil, nil, nil, nil, nil, nil)

	newRequest := func(ctx context.Context, reader *gatewayProxyAdmissionHTTPTrackingReader) *http.Request {
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", reader).WithContext(ctx)
		request.ContentLength = int64(len(requestBody))
		request.Header.Set("Authorization", "Bearer "+token)
		return request
	}
	newAdmission := func(t *testing.T, maxQueueWait time.Duration) *gatewayRequestAdmission {
		t.Helper()
		return mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       1 << 20,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 1 << 20,
			MaxQueuedRequests:          1,
			MaxQueuedRequestsPerKey:    1,
			MaxQueueWait:               maxQueueWait,
		})
	}
	newRouter := func(admission *gatewayRequestAdmission) *gin.Engine {
		router := gin.New()
		Register(router, &Deps{
			Gateway:                 service,
			gatewayRequestAdmission: admission,
			gatewayAuthenticate: func(string) (*storage.GatewayDownstreamKey, error) {
				return downstream, nil
			},
		})
		return router
	}

	t.Run("client cancellation with busy cause returns 499 before body read", func(t *testing.T) {
		admission := newAdmission(t, time.Second)
		holder, err := admission.Acquire(context.Background(), downstream.ID, 0, gatewayBodyLimit, gatewayRequestAdmissionStandardProfile)
		if err != nil {
			t.Fatalf("acquire admission holder: %v", err)
		}
		t.Cleanup(holder.Release)

		clientContext, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(context.Canceled) })
		reader := &gatewayProxyAdmissionHTTPTrackingReader{reader: strings.NewReader(requestBody)}
		recorder := httptest.NewRecorder()
		finished := make(chan struct{})
		go func() {
			newRouter(admission).ServeHTTP(recorder, newRequest(clientContext, reader))
			close(finished)
		}()

		waitForGatewayRequestAdmission(t, admission, downstream.ID, func(snapshot gatewayRequestAdmissionSnapshot) bool {
			return snapshot.InFlightRequests == 1 && snapshot.KeyInFlightRequests == 1 &&
				snapshot.QueuedRequests == 1 && snapshot.KeyQueuedRequests == 1
		})
		// The cause deliberately looks like controller pressure.  The data-plane
		// response must still honor the canceled caller, not return retryable 503.
		cancel(errGatewayRequestAdmissionBusy)
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("gateway proxy did not return after queued client cancellation")
		}

		if recorder.Code != gatewayClientClosedStatus {
			t.Fatalf("canceled queued response status = %d, want %d", recorder.Code, gatewayClientClosedStatus)
		}
		if retryAfter := recorder.Header().Get("Retry-After"); retryAfter != "" {
			t.Fatalf("canceled queued response retry-after = %q, want empty", retryAfter)
		}
		if reader.reads != 0 || reader.bytes != 0 {
			t.Fatalf("canceled queued request read body chunks=%d bytes=%d", reader.reads, reader.bytes)
		}
		if snapshot := admission.snapshot(downstream.ID); snapshot.InFlightRequests != 1 || snapshot.KeyInFlightRequests != 1 ||
			snapshot.QueuedRequests != 0 || snapshot.KeyQueuedRequests != 0 ||
			snapshot.InFlightBodyBytes == 0 || snapshot.KeyInFlightBodyBytes == 0 {
			t.Fatalf("canceled queued request retained unexpected admission state: %+v", snapshot)
		}

		holder.Release()
		assertGatewayRequestAdmissionIdle(t, admission)
	})

	t.Run("controller queue timeout remains retryable", func(t *testing.T) {
		admission := newAdmission(t, 50*time.Millisecond)
		holder, err := admission.Acquire(context.Background(), downstream.ID, 0, gatewayBodyLimit, gatewayRequestAdmissionStandardProfile)
		if err != nil {
			t.Fatalf("acquire admission holder: %v", err)
		}
		t.Cleanup(holder.Release)

		reader := &gatewayProxyAdmissionHTTPTrackingReader{reader: strings.NewReader(requestBody)}
		recorder := httptest.NewRecorder()
		newRouter(admission).ServeHTTP(recorder, newRequest(context.Background(), reader))

		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("queue-timeout response status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
		if retryAfter := recorder.Header().Get("Retry-After"); retryAfter != "1" {
			t.Fatalf("queue-timeout retry-after = %q, want 1", retryAfter)
		}
		if reader.reads != 0 || reader.bytes != 0 {
			t.Fatalf("queue-timeout request read body chunks=%d bytes=%d", reader.reads, reader.bytes)
		}

		holder.Release()
		assertGatewayRequestAdmissionIdle(t, admission)
	})
}
