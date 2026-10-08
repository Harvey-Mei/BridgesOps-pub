package api

import (
	"net/http"
	"testing"
)

func TestGatewayStructuredFailureRequiresAccountBalanceStatusInSamePayload(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{
			name: "nested JSON payload",
			raw:  `{"error":{"status_code":403,"message":"Insufficient account balance"}}`,
			want: true,
		},
		{
			name: "SSE JSON payload",
			raw:  "event: error\ndata: {\"error\":{\"status_code\":401,\"message\":\"insufficient balance\"}}\n\n",
			want: true,
		},
		{
			name: "status and message split across frames",
			raw:  "data: {\"error\":{\"status_code\":403}}\n\ndata: {\"error\":{\"message\":\"insufficient account balance\"}}\n\n",
			want: false,
		},
		{
			name: "generic permission failure",
			raw:  `{"error":{"status_code":403,"message":"permission denied"}}`,
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := gatewayStructuredFailureIsAccountBalance([]byte(tc.raw), gatewayStreamFailureClassification{Status: http.StatusForbidden})
			if got != tc.want {
				t.Fatalf("gatewayStructuredFailureIsAccountBalance(%q) = %t, want %t", tc.raw, got, tc.want)
			}
		})
	}
}
