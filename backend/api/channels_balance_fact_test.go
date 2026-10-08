package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestChannelOutputOnlyExposesCurrentAccountBalanceFact(t *testing.T) {
	observedAt := time.Now().UTC()
	tests := []struct {
		name string
		ch   storage.Channel
		want bool
	}{
		{
			name: "current complete fact",
			ch: storage.Channel{
				UpstreamAccountIdentityRevision:            7,
				UpstreamAccountBalanceInsufficient:         true,
				UpstreamAccountBalanceInsufficientAt:       &observedAt,
				UpstreamAccountBalanceInsufficientRevision: 7,
			},
			want: true,
		},
		{
			name: "stale identity fact",
			ch: storage.Channel{
				UpstreamAccountIdentityRevision:            8,
				UpstreamAccountBalanceInsufficient:         true,
				UpstreamAccountBalanceInsufficientAt:       &observedAt,
				UpstreamAccountBalanceInsufficientRevision: 7,
			},
		},
		{
			name: "incomplete fact",
			ch: storage.Channel{
				UpstreamAccountIdentityRevision:            7,
				UpstreamAccountBalanceInsufficient:         true,
				UpstreamAccountBalanceInsufficientRevision: 7,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(channelOutputFor(nil, tc.ch))
			if err != nil {
				t.Fatal(err)
			}
			var payload map[string]any
			if err := json.Unmarshal(raw, &payload); err != nil {
				t.Fatal(err)
			}
			got, _ := payload["upstream_account_balance_insufficient"].(bool)
			if got != tc.want {
				t.Fatalf("account balance fact = %t, want %t: %s", got, tc.want, raw)
			}
			_, hasObservedAt := payload["upstream_account_balance_insufficient_at"]
			if hasObservedAt != tc.want {
				t.Fatalf("account balance observed_at present = %t, want %t: %s", hasObservedAt, tc.want, raw)
			}
		})
	}
}
