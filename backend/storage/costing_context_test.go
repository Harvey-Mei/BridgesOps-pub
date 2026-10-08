package storage

import (
	"context"
	"errors"
	"testing"
)

func TestReconcileSupersededFundingCandidatesContextStopsBeforeDatabaseAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := NewCosting(nil).ReconcileSupersededFundingCandidatesBatchContext(ctx, 0, 0, 500)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcile error = %v, want context cancellation", err)
	}
	if result != (SupersededFundingCandidateReconcileBatch{}) {
		t.Fatalf("reconcile result = %+v, want zero accounting", result)
	}
}
