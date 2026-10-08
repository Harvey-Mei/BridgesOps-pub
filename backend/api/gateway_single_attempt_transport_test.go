package api

import (
	"bytes"
	"net/http"
	"testing"
)

func TestGatewayEmptyBodySingleAttemptTransportDoesNotRequireDispatchHook(t *testing.T) {
	base := &http.Transport{ForceAttemptHTTP2: true}
	empty, err := http.NewRequest(http.MethodGet, "https://example.test/v1/models", nil)
	if err != nil {
		t.Fatalf("new empty request: %v", err)
	}
	selected, ok := gatewaySingleAttemptTransportForRequest(base, empty).(*http.Transport)
	if !ok || selected == base || !selected.DisableKeepAlives || selected.ForceAttemptHTTP2 {
		t.Fatalf("empty request transport = %#v", selected)
	}

	nonEmpty, err := http.NewRequest(http.MethodPost, "https://example.test/v1/responses", bytes.NewBufferString("{}"))
	if err != nil {
		t.Fatalf("new non-empty request: %v", err)
	}
	if selected := gatewaySingleAttemptTransportForRequest(base, nonEmpty); selected != base {
		t.Fatalf("non-empty request transport = %T %p, want original %p", selected, selected, base)
	}
}
