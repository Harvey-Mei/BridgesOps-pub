package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestAnalyticsCacheNamespaceIsStableAndDeploymentScoped(t *testing.T) {
	base := &config.Config{
		Security: config.SecurityConfig{AppSecret: "deployment-one"},
		Database: config.DatabaseConfig{Driver: "postgres", Host: "db.internal", Port: 5432, User: "bridgeops", Name: "bridgeops"},
	}
	first := analyticsCacheNamespace(base)
	if first == "" || first != analyticsCacheNamespace(base) {
		t.Fatalf("unstable namespace = %q", first)
	}
	other := *base
	other.Security.AppSecret = "deployment-two"
	if first == analyticsCacheNamespace(&other) {
		t.Fatal("different deployments produced the same analytics namespace")
	}
	base.Redis.Namespace = "shared-db"
	if got := analyticsCacheNamespace(base); got != "shared-db" {
		t.Fatalf("explicit namespace = %q", got)
	}
}

func TestApplyPostgresSchemaOnlyRejectsNonPostgres(t *testing.T) {
	if err := applyPostgresSchemaOnly(nil); err == nil {
		t.Fatal("nil config unexpectedly accepted")
	}
	if err := applyPostgresSchemaOnly(&config.Config{Database: config.DatabaseConfig{Driver: "unsupported"}}); err == nil {
		t.Fatal("unsupported database config unexpectedly accepted")
	}
}

func TestReconcileSupersededFundingCandidatesDrainsBoundedBatches(t *testing.T) {
	tests := []struct {
		name    string
		results []storage.SupersededFundingCandidateReconcileBatch
		want    int64
	}{
		{
			name: "partial final batch",
			results: []storage.SupersededFundingCandidateReconcileBatch{
				{HighWaterID: 5, NextID: 2, Scanned: 2, Resolved: 2},
				{HighWaterID: 5, NextID: 4, Scanned: 2, Resolved: 2},
				{HighWaterID: 5, NextID: 5, Scanned: 1, Resolved: 1, Exhausted: true},
				{HighWaterID: 5, Exhausted: true},
			},
			want: 5,
		},
		{
			name: "exact multiple reaches high water",
			results: []storage.SupersededFundingCandidateReconcileBatch{
				{HighWaterID: 4, NextID: 2, Scanned: 2, Resolved: 2},
				{HighWaterID: 4, NextID: 4, Scanned: 2, Resolved: 2, Exhausted: true},
				{HighWaterID: 4, Exhausted: true},
			},
			want: 4,
		},
		{
			name: "unmatched page still advances",
			results: []storage.SupersededFundingCandidateReconcileBatch{
				{HighWaterID: 3, NextID: 2, Scanned: 2, Resolved: 0},
				{HighWaterID: 3, NextID: 3, Scanned: 1, Resolved: 1, Exhausted: true},
				{HighWaterID: 3, NextID: 2, Scanned: 2, Resolved: 0},
				{HighWaterID: 3, Exhausted: true},
			},
			want: 1,
		},
		{
			name: "verification pass catches evidence behind cursor",
			results: []storage.SupersededFundingCandidateReconcileBatch{
				{HighWaterID: 3, NextID: 2, Scanned: 2, Resolved: 0},
				{HighWaterID: 3, NextID: 3, Scanned: 1, Resolved: 1, Exhausted: true},
				{HighWaterID: 3, NextID: 1, Scanned: 1, Resolved: 1, Exhausted: true},
				{HighWaterID: 3, Exhausted: true},
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			var expectedAfterID, expectedHighWaterID uint64
			total, err := reconcileSupersededFundingCandidates(
				context.Background(),
				2,
				time.Nanosecond,
				func(ctx context.Context, afterID, highWaterID uint64, limit int) (storage.SupersededFundingCandidateReconcileBatch, error) {
					if ctx == nil {
						t.Fatal("reconcile received a nil context")
					}
					if limit != 2 {
						t.Fatalf("batch limit = %d, want 2", limit)
					}
					if afterID != expectedAfterID || highWaterID != expectedHighWaterID {
						t.Fatalf("batch cursor=(%d,%d), want (%d,%d)", afterID, highWaterID, expectedAfterID, expectedHighWaterID)
					}
					if calls >= len(tt.results) {
						t.Fatalf("unexpected reconcile call %d", calls+1)
					}
					result := tt.results[calls]
					calls++
					if result.Exhausted && calls < len(tt.results) {
						expectedAfterID = 0
					} else {
						expectedAfterID = result.NextID
					}
					expectedHighWaterID = result.HighWaterID
					return result, nil
				},
			)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if total != tt.want || calls != len(tt.results) {
				t.Fatalf("reconcile total=%d calls=%d, want total=%d calls=%d", total, calls, tt.want, len(tt.results))
			}
		})
	}
}

func TestReconcileSupersededFundingCandidatesSurfacesFailureAfterCommittedBatches(t *testing.T) {
	sentinel := errors.New("batch failed")
	calls := 0
	total, err := reconcileSupersededFundingCandidates(
		context.Background(),
		2,
		time.Nanosecond,
		func(context.Context, uint64, uint64, int) (storage.SupersededFundingCandidateReconcileBatch, error) {
			calls++
			if calls == 1 {
				return storage.SupersededFundingCandidateReconcileBatch{HighWaterID: 4, NextID: 2, Scanned: 2, Resolved: 2}, nil
			}
			return storage.SupersededFundingCandidateReconcileBatch{HighWaterID: 4, NextID: 3, Scanned: 1, Resolved: 1}, sentinel
		},
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("reconcile error = %v, want %v", err, sentinel)
	}
	if total != 2 || calls != 2 {
		t.Fatalf("reconcile total=%d calls=%d, want total=2 calls=2", total, calls)
	}
}

func TestReconcileSupersededFundingCandidatesCancellationStopsBetweenBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstBatch := make(chan struct{})
	type result struct {
		total int64
		err   error
		calls int
	}
	resultCh := make(chan result, 1)
	go func() {
		calls := 0
		total, err := reconcileSupersededFundingCandidates(
			ctx,
			1,
			time.Hour,
			func(context.Context, uint64, uint64, int) (storage.SupersededFundingCandidateReconcileBatch, error) {
				calls++
				if calls == 1 {
					close(firstBatch)
				}
				return storage.SupersededFundingCandidateReconcileBatch{HighWaterID: 2, NextID: 1, Scanned: 1, Resolved: 1}, nil
			},
		)
		resultCh <- result{total: total, err: err, calls: calls}
	}()

	select {
	case <-firstBatch:
	case <-time.After(time.Second):
		t.Fatal("first reconcile batch did not run")
	}
	cancel()
	select {
	case got := <-resultCh:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("reconcile error = %v, want context cancellation", got.err)
		}
		if got.total != 1 || got.calls != 1 {
			t.Fatalf("reconcile total=%d calls=%d, want total=1 calls=1", got.total, got.calls)
		}
	case <-time.After(time.Second):
		t.Fatal("reconcile did not stop after cancellation")
	}
}

func TestReconcileSupersededFundingCandidatesRejectsInvalidBatchResults(t *testing.T) {
	tests := []struct {
		name   string
		result storage.SupersededFundingCandidateReconcileBatch
	}{
		{name: "negative resolved", result: storage.SupersededFundingCandidateReconcileBatch{HighWaterID: 1, NextID: 1, Scanned: 1, Resolved: -1, Exhausted: true}},
		{name: "resolved above scanned", result: storage.SupersededFundingCandidateReconcileBatch{HighWaterID: 1, NextID: 1, Scanned: 1, Resolved: 2, Exhausted: true}},
		{name: "scanned above limit", result: storage.SupersededFundingCandidateReconcileBatch{HighWaterID: 3, NextID: 3, Scanned: 3, Resolved: 1, Exhausted: true}},
		{name: "no cursor progress", result: storage.SupersededFundingCandidateReconcileBatch{HighWaterID: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			total, err := reconcileSupersededFundingCandidates(
				context.Background(),
				2,
				time.Nanosecond,
				func(context.Context, uint64, uint64, int) (storage.SupersededFundingCandidateReconcileBatch, error) {
					calls++
					return tt.result, nil
				},
			)
			if err == nil {
				t.Fatalf("result=%+v was accepted", tt.result)
			}
			if total != 0 || calls != 1 {
				t.Fatalf("result=%+v total=%d calls=%d, want total=0 calls=1", tt.result, total, calls)
			}
		})
	}
}
