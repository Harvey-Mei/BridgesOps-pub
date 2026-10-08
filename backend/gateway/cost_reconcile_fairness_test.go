package gateway

import (
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestSelectFairRequestCostReconcilesLimitsEachBillingIdentity(t *testing.T) {
	now := time.Now().UTC()
	pending := []storage.GatewayRequestLog{
		{ID: 1, ChannelID: 10, UpstreamKeyID: 100, CostReconcileAt: timePtr(now.Add(-4 * time.Second))},
		{ID: 2, ChannelID: 10, UpstreamKeyID: 100, CostReconcileAt: timePtr(now.Add(-3 * time.Second))},
		{ID: 3, ChannelID: 11, UpstreamKeyID: 100, CostReconcileAt: timePtr(now.Add(-2 * time.Second))},
		{ID: 4, ChannelID: 10, UpstreamKeyID: 101, CostReconcileAt: timePtr(now.Add(-time.Second))},
	}

	selected := selectFairRequestCostReconciles(pending, 4, nil)
	if len(selected) != 3 || selected[0].ID != 1 || selected[1].ID != 3 || selected[2].ID != 4 {
		t.Fatalf("selected IDs = %v, want [1 3 4]", requestLogIDs(selected))
	}
}

func TestSelectFairRequestCostReconcilesSkipsBusyIdentity(t *testing.T) {
	pending := []storage.GatewayRequestLog{
		{ID: 1, ChannelID: 10, UpstreamKeyID: 100},
		{ID: 2, ChannelID: 10, UpstreamKeyID: 100},
		{ID: 3, ChannelID: 11, UpstreamKeyID: 200},
		{ID: 4, ChannelID: 12, UpstreamKeyID: 300},
	}
	busy := map[costReconcileIdentity]struct{}{
		{channelID: 10, upstreamKeyID: 100}: {},
	}

	selected := selectFairRequestCostReconciles(pending, 2, busy)
	if len(selected) != 2 || selected[0].ID != 3 || selected[1].ID != 4 {
		t.Fatalf("selected IDs = %v, want [3 4]", requestLogIDs(selected))
	}
}

func TestSelectFairRequestCostReconcilesCollapsesLegacyRowsByChannel(t *testing.T) {
	pending := []storage.GatewayRequestLog{
		{ID: 1, ChannelID: 10, GroupName: "alpha"},
		{ID: 2, ChannelID: 10, GroupName: "beta"},
		{ID: 3, ChannelID: 11, GroupName: "alpha"},
	}

	selected := selectFairRequestCostReconciles(pending, 3, nil)
	if len(selected) != 2 || selected[0].ID != 1 || selected[1].ID != 3 {
		t.Fatalf("selected IDs = %v, want [1 3]", requestLogIDs(selected))
	}
}

func TestCostReconcileIdentityLimiterTracksAcquireAndRelease(t *testing.T) {
	limiter := newCostReconcileIdentityLimiter()
	identity := costReconcileIdentity{channelID: 10, upstreamKeyID: 100}
	if !limiter.tryAcquire(identity) {
		t.Fatal("first acquire was rejected")
	}
	if limiter.tryAcquire(identity) {
		t.Fatal("duplicate acquire was accepted")
	}
	if snapshot := limiter.snapshot(); len(snapshot) != 1 {
		t.Fatalf("busy snapshot = %v, want one identity", snapshot)
	}
	limiter.release(identity)
	if !limiter.tryAcquire(identity) {
		t.Fatal("identity was not released")
	}
}

func TestRequestCostReconcileTerminalReasonIsTyped(t *testing.T) {
	now := time.Date(2026, 7, 28, 4, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name                  string
		log                   storage.GatewayRequestLog
		includeCurrentFailure bool
		want                  string
	}{
		{
			name: "still retryable",
			log: storage.GatewayRequestLog{
				CreatedAt: now.Add(-time.Hour), CostReconcileTries: maxCostReconcileAttempts - 2,
			},
			includeCurrentFailure: true,
		},
		{
			name: "current failure reaches limit",
			log: storage.GatewayRequestLog{
				CreatedAt: now.Add(-time.Hour), CostReconcileTries: maxCostReconcileAttempts - 1,
			},
			includeCurrentFailure: true,
			want:                  storage.GatewayRequestCostReconcileReasonRetryLimit,
		},
		{
			name: "already at limit",
			log: storage.GatewayRequestLog{
				CreatedAt: now.Add(-time.Hour), CostReconcileTries: maxCostReconcileAttempts,
			},
			want: storage.GatewayRequestCostReconcileReasonRetryLimit,
		},
		{
			name: "age fence wins",
			log: storage.GatewayRequestLog{
				CreatedAt: now.Add(-maxCostReconcileAge), CostReconcileTries: maxCostReconcileAttempts,
			},
			want: storage.GatewayRequestCostReconcileReasonExpired,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := requestCostReconcileTerminalReason(test.log, now, test.includeCurrentFailure); got != test.want {
				t.Fatalf("terminal reason = %q, want %q", got, test.want)
			}
		})
	}
}

func requestLogIDs(logs []storage.GatewayRequestLog) []uint {
	ids := make([]uint, 0, len(logs))
	for _, log := range logs {
		ids = append(ids, log.ID)
	}
	return ids
}
