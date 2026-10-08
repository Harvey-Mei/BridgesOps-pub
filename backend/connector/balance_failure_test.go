package connector

import (
	"net/http"
	"testing"
)

func TestIsAccountBalanceFailure(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{name: "structured code", status: http.StatusForbidden, body: `{"error":{"code":"INSUFFICIENT_BALANCE"}}`, want: true},
		{name: "nested account balance", status: http.StatusUnauthorized, body: `{"response":{"error":{"detail":"Insufficient account balance"}}}`, want: true},
		{name: "plain account balance", status: http.StatusForbidden, body: `Insufficient account balance`},
		{name: "html account balance", status: http.StatusForbidden, body: `<html><body>Insufficient account balance</body></html>`},
		{name: "generic insufficient quota", status: http.StatusForbidden, body: `{"error":{"type":"insufficient_quota"}}`},
		{name: "quota exceeded message", status: http.StatusForbidden, body: `{"error":{"message":"quota exceeded"}}`},
		{name: "permission denied", status: http.StatusForbidden, body: `{"error":{"message":"permission denied"}}`},
		{name: "empty body", status: http.StatusForbidden},
		{name: "truncated json with marker", status: http.StatusForbidden, body: `{"error":{"code":"INSUFFICIENT_BALANCE"`},
		{name: "non auth status", status: http.StatusPaymentRequired, body: `{"error":{"code":"INSUFFICIENT_BALANCE"}}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsAccountBalanceFailure(tc.status, []byte(tc.body)); got != tc.want {
				t.Fatalf("IsAccountBalanceFailure(%d, %q) = %t, want %t", tc.status, tc.body, got, tc.want)
			}
		})
	}
}
