package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGatewayRequestAdmissionFailurePrioritizesClientCancellation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	parent, cancel := context.WithCancelCause(context.Background())
	cancel(errGatewayRequestAdmissionBusy)

	recorder := httptest.NewRecorder()
	requestContext, _ := gin.CreateTestContext(recorder)
	gatewayWriteRequestAdmissionFailure(requestContext, gatewayRequestAdmissionContextError(parent))
	requestContext.Writer.WriteHeaderNow()

	if recorder.Code != gatewayClientClosedStatus {
		t.Fatalf("canceled busy-wrapped admission failure status = %d, want %d", recorder.Code, gatewayClientClosedStatus)
	}
	if retryAfter := recorder.Header().Get("Retry-After"); retryAfter != "" {
		t.Fatalf("canceled admission failure retry-after = %q, want empty", retryAfter)
	}
}

func TestGatewayRequestAdmissionFailureKeepsControllerPressureRetryable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	requestContext, _ := gin.CreateTestContext(recorder)

	gatewayWriteRequestAdmissionFailure(requestContext, errGatewayRequestAdmissionQueueWaitExceeded)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("queue timeout status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if retryAfter := recorder.Header().Get("Retry-After"); retryAfter != "1" {
		t.Fatalf("queue timeout retry-after = %q, want 1", retryAfter)
	}
}
