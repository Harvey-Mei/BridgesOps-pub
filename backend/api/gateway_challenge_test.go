package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
)

func TestGatewayChallengeDoesNotInvalidateManagedKey(t *testing.T) {
	challenge := []byte("<html><title>Just a moment...</title><script src='/cdn-cgi/challenge-platform/'></script></html>")
	challengeErr := connector.HTTPStatusError(http.StatusForbidden, challenge)
	if !isTransientGatewayResponse(http.StatusForbidden, challenge) {
		t.Fatal("Cloudflare challenge was not classified as a transient gateway response")
	}
	if !isRetryableGatewayPreparationError(context.Background(), challengeErr) {
		t.Fatal("Cloudflare challenge was not retryable during gateway preparation")
	}
	if isGatewayProbeAuthError(challengeErr) {
		t.Fatal("Cloudflare challenge was classified as a model-probe authentication failure")
	}
	if gatewayHTTPFailureInvalidatesKey(http.StatusForbidden, challenge) {
		t.Fatal("Cloudflare challenge invalidated a managed key")
	}
	if gatewayStructuredFailureInvalidatesKey(challenge, gatewayStreamFailureClassification{Status: http.StatusForbidden}) {
		t.Fatal("Cloudflare challenge frame invalidated a managed key")
	}
	if !gatewayHTTPFailureInvalidatesKey(http.StatusForbidden, []byte(`{"error":"invalid key"}`)) {
		t.Fatal("ordinary forbidden authentication response did not invalidate a managed key")
	}
	if !isGatewayProbeAuthError(connector.HTTPStatusError(http.StatusForbidden, []byte(`{"error":"invalid key"}`))) {
		t.Fatal("ordinary forbidden authentication response was not classified as an authentication failure")
	}
	stream := []byte("event: error\ndata: {\"error\":{\"status\":403,\"message\":\"<html><title>Just a moment...</title></html>\"}}\n\n")
	classification := classifyGatewayStreamFailure(stream)
	if classification.Disposition != gatewayStreamFailureTransient || classification.Status != http.StatusForbidden {
		t.Fatalf("structured Cloudflare challenge = %+v, want transient 403", classification)
	}
}

func TestGatewayChallengeMarkersInUsefulStreamOutputRemainOutput(t *testing.T) {
	frames := [][]byte{
		[]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"Cloudflare Ray ID and cf-mitigated are response headers\"}\n\n"),
		[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"<title>Just a moment...</title>\"}}]}\n\n"),
		[]byte("data: Cloudflare Ray ID: generated documentation text\n\n"),
	}
	for _, frame := range frames {
		if classification := classifyGatewayStreamFailure(frame); classification.Found {
			t.Fatalf("useful stream output was classified as a challenge: %+v, frame=%q", classification, frame)
		}
		if gatewayStreamFrameHasError(frame) {
			t.Fatalf("useful stream output was classified as an error: %q", frame)
		}
	}
}
