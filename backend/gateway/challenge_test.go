package gateway

import (
	"context"
	"net/http"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
)

func TestLatencyProbeChallengeDoesNotInvalidateManagedKey(t *testing.T) {
	challenge := []byte("<html><title>Just a moment...</title><script src='/cdn-cgi/challenge-platform/'></script></html>")
	if isModelProbeAuthError(connector.HTTPStatusError(http.StatusForbidden, challenge)) {
		t.Fatal("Cloudflare challenge was classified as a model-probe authentication failure")
	}
	if !isRetryableLatencyProbeResponse(http.StatusForbidden, challenge) {
		t.Fatal("Cloudflare challenge was not retryable for latency probes")
	}
	if !isRetryableLatencyProbeError(context.Background(), connector.HTTPStatusError(http.StatusForbidden, challenge)) {
		t.Fatal("wrapped Cloudflare challenge was not retryable for latency probes")
	}
	if gatewayHTTPFailureInvalidatesKey(http.StatusForbidden, challenge) {
		t.Fatal("Cloudflare challenge invalidated a managed key")
	}
	if !gatewayHTTPFailureInvalidatesKey(http.StatusForbidden, []byte(`{"error":"invalid key"}`)) {
		t.Fatal("ordinary forbidden authentication response did not invalidate a managed key")
	}
	if !isModelProbeAuthError(connector.HTTPStatusError(http.StatusForbidden, []byte(`{"error":"invalid key"}`))) {
		t.Fatal("ordinary forbidden authentication response was not classified as an authentication failure")
	}
	if isRetryableLatencyProbeResponse(http.StatusForbidden, []byte(`{"error":"invalid key"}`)) {
		t.Fatal("ordinary forbidden authentication response became retryable")
	}
}
