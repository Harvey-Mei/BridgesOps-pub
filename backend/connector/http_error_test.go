package connector

import (
	"fmt"
	"net/http"
	"testing"
)

func TestIsUpstreamChallengeResponse(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "cloudflare page", status: http.StatusForbidden, body: "<title>Just a moment...</title><script src=challenge-platform></script>", want: true},
		{name: "cloudflare marker", status: http.StatusForbidden, body: "cf-mitigated: challenge", want: true},
		{name: "provider permission error", status: http.StatusForbidden, body: `{"error":"permission denied"}`, want: false},
		{name: "provider just a moment text", status: http.StatusForbidden, body: `{"error":"just a moment"}`, want: false},
		{name: "challenge marker on success", status: http.StatusOK, body: "Just a moment", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := IsUpstreamChallengeResponse(test.status, []byte(test.body)); got != test.want {
				t.Fatalf("IsUpstreamChallengeResponse(%d, %q) = %t, want %t", test.status, test.body, got, test.want)
			}
		})
	}

	if !IsUpstreamChallengeHTTPError(HTTPStatusError(http.StatusForbidden, []byte("<title>Just a moment...</title>"))) {
		t.Fatal("wrapped Cloudflare challenge was not recognized")
	}
	if !IsUpstreamChallengeHTTPError(fmt.Errorf("provider diagnostic: %w", ErrUpstreamChallenge)) {
		t.Fatal("typed provider challenge was not recognized")
	}
}
