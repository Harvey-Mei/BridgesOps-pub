package storage

import (
	"testing"
	"time"
)

func TestListFairPendingRequestCostReconcilesReturnsOneHeadPerIdentity(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	logs := []*GatewayRequestLog{
		{ChannelID: 10, UpstreamKeyID: 100, Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "a-oldest", CreatedAt: now.Add(-5 * time.Second)},
		{ChannelID: 10, UpstreamKeyID: 100, Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "a-next", CreatedAt: now.Add(-4 * time.Second)},
		{ChannelID: 11, UpstreamKeyID: 100, Method: "POST", Path: "/v1/responses", StatusCode: 502, Error: "stream failed", UpstreamAccepted: true, UpstreamRequestID: "b", CreatedAt: now.Add(-3 * time.Second)},
		{ChannelID: 10, UpstreamKeyID: 101, Method: "POST", Path: "/v1/images/generations", StatusCode: 499, RouteReason: "client_canceled", BillingCommitted: true, UpstreamRequestID: "c", CreatedAt: now.Add(-2 * time.Second)},
		{ChannelID: 12, GroupName: "legacy-a", Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "legacy-oldest", CreatedAt: now.Add(-time.Second)},
		{ChannelID: 12, GroupName: "legacy-b", Method: "POST", Path: "/v1/responses", StatusCode: 200, UpstreamRequestID: "legacy-next", CreatedAt: now},
	}
	for index, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append log %d: %v", index, err)
		}
		if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(time.Duration(index-10)*time.Second)); err != nil {
			t.Fatalf("schedule log %d: %v", index, err)
		}
	}

	pending, err := store.ListFairPendingRequestCostReconciles(10, now)
	if err != nil {
		t.Fatalf("list fair pending: %v", err)
	}
	want := []uint{logs[0].ID, logs[2].ID, logs[3].ID, logs[4].ID}
	if got := fairPendingRequestLogIDs(pending); !sameFairPendingIDs(got, want) {
		t.Fatalf("pending IDs = %v, want %v", got, want)
	}
}

func fairPendingRequestLogIDs(logs []GatewayRequestLog) []uint {
	ids := make([]uint, 0, len(logs))
	for _, log := range logs {
		ids = append(ids, log.ID)
	}
	return ids
}

func sameFairPendingIDs(left, right []uint) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
