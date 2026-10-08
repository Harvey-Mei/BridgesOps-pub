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

type gatewayAdmissionTrackingReader struct {
	reader *strings.Reader
	reads  int
	bytes  int
}

func (r *gatewayAdmissionTrackingReader) Read(p []byte) (int, error) {
	r.reads++
	n, err := r.reader.Read(p)
	r.bytes += n
	return n, err
}

func TestGatewayProxyRequestAdmissionRejectsBeforeReadAndReleasesOnResolutionFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const requestBody = `{"model":"gpt-test"}`
	db := openTestDB(t)
	store := storage.NewGateway(db)
	const token = "sk-gateway-admission-test"
	downstream := &storage.GatewayDownstreamKey{
		Name: "gateway-admission", App: storage.GatewayAppCodex, ModelFamily: "codex",
		KeyHash: gateway.HashKey(token), APIKeyCipher: "cipher", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	service := gateway.NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, nil, nil)
	newContext := func(reader *gatewayAdmissionTrackingReader) (*gin.Context, *httptest.ResponseRecorder) {
		recorder := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(recorder)
		context.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", reader)
		context.Request.ContentLength = int64(len(requestBody))
		context.Request.Header.Set("Authorization", "Bearer "+token)
		return context, recorder
	}

	t.Run("permanent weighted overflow returns 413 before read", func(t *testing.T) {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       1,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 1,
			MaxQueuedRequests:          0,
			MaxQueuedRequestsPerKey:    0,
		})
		reader := &gatewayAdmissionTrackingReader{reader: strings.NewReader(requestBody)}
		context, recorder := newContext(reader)
		gatewayProxy(context, &Deps{Gateway: service, gatewayRequestAdmission: admission})
		if recorder.Code != http.StatusRequestEntityTooLarge || recorder.Header().Get("Retry-After") != "" || !strings.Contains(recorder.Body.String(), "request body exceeds configured limit") {
			t.Fatalf("permanent overflow response = status %d retry-after %q body %q", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
		}
		if reader.reads != 0 {
			t.Fatalf("permanent overflow read %d body chunks", reader.reads)
		}
		assertGatewayRequestAdmissionIdle(t, admission)
	})

	t.Run("transient request saturation returns 503 before read", func(t *testing.T) {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       1 << 20,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 1 << 20,
			MaxQueuedRequests:          0,
			MaxQueuedRequestsPerKey:    0,
		})
		holder, err := admission.Acquire(context.Background(), downstream.ID, 0, gatewayBodyLimit, gatewayRequestAdmissionStandardProfile)
		if err != nil {
			t.Fatalf("acquire saturation holder: %v", err)
		}
		reader := &gatewayAdmissionTrackingReader{reader: strings.NewReader(requestBody)}
		requestContext, recorder := newContext(reader)
		gatewayProxy(requestContext, &Deps{Gateway: service, gatewayRequestAdmission: admission})
		if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
			t.Fatalf("transient saturation response = status %d retry-after %q body %q", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
		}
		if reader.reads != 0 {
			t.Fatalf("transient saturation read %d body chunks", reader.reads)
		}
		holder.Release()
		assertGatewayRequestAdmissionIdle(t, admission)
	})

	t.Run("queued request timeout returns 503 before read", func(t *testing.T) {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       1 << 20,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 1 << 20,
			MaxQueuedRequests:          1,
			MaxQueuedRequestsPerKey:    1,
			MaxQueueWait:               20 * time.Millisecond,
		})
		holder, err := admission.Acquire(context.Background(), downstream.ID, 0, gatewayBodyLimit, gatewayRequestAdmissionStandardProfile)
		if err != nil {
			t.Fatalf("acquire saturation holder: %v", err)
		}
		reader := &gatewayAdmissionTrackingReader{reader: strings.NewReader(requestBody)}
		requestContext, recorder := newContext(reader)
		gatewayProxy(requestContext, &Deps{Gateway: service, gatewayRequestAdmission: admission})
		if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "1" || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
			t.Fatalf("queue timeout response = status %d retry-after %q body %q", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
		}
		if reader.reads != 0 {
			t.Fatalf("queue timeout read %d body chunks", reader.reads)
		}
		holder.Release()
		assertGatewayRequestAdmissionIdle(t, admission)
	})

	t.Run("declared oversized body returns 413 before read", func(t *testing.T) {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       1 << 30,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 1 << 30,
			MaxQueuedRequests:          0,
			MaxQueuedRequestsPerKey:    0,
		})
		reader := &gatewayAdmissionTrackingReader{reader: strings.NewReader(requestBody)}
		context, recorder := newContext(reader)
		context.Request.ContentLength = gatewayBodyLimit + 1
		gatewayProxy(context, &Deps{Gateway: service, gatewayRequestAdmission: admission})
		if recorder.Code != http.StatusRequestEntityTooLarge || recorder.Header().Get("Retry-After") != "" || !strings.Contains(recorder.Body.String(), "request body exceeds configured limit") {
			t.Fatalf("oversized response = status %d retry-after %q body %q", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
		}
		if reader.reads != 0 {
			t.Fatalf("declared oversized request read %d body chunks", reader.reads)
		}
		assertGatewayRequestAdmissionIdle(t, admission)
	})

	t.Run("candidate resolution return releases lease", func(t *testing.T) {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       1 << 20,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 1 << 20,
			MaxQueuedRequests:          0,
			MaxQueuedRequestsPerKey:    0,
		})
		reader := &gatewayAdmissionTrackingReader{reader: strings.NewReader(requestBody)}
		context, recorder := newContext(reader)
		gatewayProxy(context, &Deps{Gateway: service, gatewayRequestAdmission: admission})
		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("candidate resolution response = status %d body %q", recorder.Code, recorder.Body.String())
		}
		if reader.reads == 0 {
			t.Fatal("candidate resolution did not read its admitted body")
		}
		assertGatewayRequestAdmissionIdle(t, admission)
	})

	t.Run("small chunked body grows its lease and releases", func(t *testing.T) {
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       1 << 20,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: 1 << 20,
			MaxQueuedRequests:          0,
			MaxQueuedRequestsPerKey:    0,
		})
		reader := &gatewayAdmissionTrackingReader{reader: strings.NewReader(requestBody)}
		requestContext, recorder := newContext(reader)
		requestContext.Request.ContentLength = -1
		gatewayProxy(requestContext, &Deps{Gateway: service, gatewayRequestAdmission: admission})
		if recorder.Code != http.StatusBadGateway {
			t.Fatalf("chunked candidate resolution response = status %d body %q", recorder.Code, recorder.Body.String())
		}
		if reader.reads == 0 || reader.bytes != len(requestBody) {
			t.Fatalf("chunked body reads=%d bytes=%d, want %d bytes", reader.reads, reader.bytes, len(requestBody))
		}
		assertGatewayRequestAdmissionIdle(t, admission)
	})

	t.Run("chunked weighted overflow stops during read with 413", func(t *testing.T) {
		const firstChunkWeighted = gatewayRequestBodyStandardOverhead + gatewayRequestBodyStandardMultiplier*(32<<10)
		admission := mustGatewayRequestAdmission(t, gatewayRequestAdmissionLimits{
			MaxInFlightRequests:        1,
			MaxInFlightBodyBytes:       firstChunkWeighted,
			MaxInFlightRequestsPerKey:  1,
			MaxInFlightBodyBytesPerKey: firstChunkWeighted,
			MaxQueuedRequests:          0,
			MaxQueuedRequestsPerKey:    0,
		})
		largeBody := requestBody + strings.Repeat("x", 96<<10)
		reader := &gatewayAdmissionTrackingReader{reader: strings.NewReader(largeBody)}
		requestContext, recorder := newContext(reader)
		requestContext.Request.ContentLength = -1
		gatewayProxy(requestContext, &Deps{Gateway: service, gatewayRequestAdmission: admission})
		if recorder.Code != http.StatusRequestEntityTooLarge || recorder.Header().Get("Retry-After") != "" || !strings.Contains(recorder.Body.String(), "request body exceeds configured limit") {
			t.Fatalf("chunked overflow response = status %d retry-after %q body %q", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
		}
		if reader.bytes == 0 || reader.bytes >= len(largeBody) {
			t.Fatalf("chunked overflow consumed %d of %d bytes", reader.bytes, len(largeBody))
		}
		assertGatewayRequestAdmissionIdle(t, admission)
	})
}
