package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
)

func TestGatewayRequestDispatchOwnerLeaseLossIsNotClientCancellation(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	ownerContext, cancelOwner := context.WithCancelCause(request.Context())
	ownerCause := fmt.Errorf("heartbeat fenced: %w", gateway.ErrRequestDispatchOwnerLeaseLost)
	cancelOwner(ownerCause)
	request = request.WithContext(ownerContext)

	if !gatewayRequestDispatchOwnerLeaseLost(request, context.Canceled) {
		t.Fatal("owner lease loss was not recovered from the request context cause")
	}
	if isGatewayClientCanceled(request, context.Canceled) {
		t.Fatal("owner lease loss was misclassified as a client cancellation")
	}
}

func TestGatewayClientCancellationRemainsDistinctFromOwnerLeaseLoss(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	clientContext, cancelClient := context.WithCancel(request.Context())
	cancelClient()
	request = request.WithContext(clientContext)

	if gatewayRequestDispatchOwnerLeaseLost(request, context.Canceled) {
		t.Fatal("client cancellation was misclassified as owner lease loss")
	}
	if !isGatewayClientCanceled(request, context.Canceled) {
		t.Fatal("client cancellation was not recognized")
	}
}
