package gateway

import (
	"testing"
	"time"
)

func TestInitialRequestCostReconcileAtWaitsForUpstreamSettlement(t *testing.T) {
	finishedAt := time.Date(2026, 7, 23, 17, 0, 0, 0, time.UTC)
	if got, want := InitialRequestCostReconcileAt(finishedAt), finishedAt.Add(6*time.Second); !got.Equal(want) {
		t.Fatalf("initial reconcile at = %v, want %v", got, want)
	}
}
