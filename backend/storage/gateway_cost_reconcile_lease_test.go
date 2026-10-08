package storage

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestGatewayUsageAssignmentConflictRecognizesOnlyUsageIndex(t *testing.T) {
	if !gatewayUsageAssignmentConflict(&pgconn.PgError{Code: "23505", ConstraintName: "idx_gateway_channel_usage"}) {
		t.Fatal("usage assignment unique violation was not recognized")
	}
	for _, err := range []error{
		&pgconn.PgError{Code: "23505", ConstraintName: "some_other_index"},
		&pgconn.PgError{Code: "40001", ConstraintName: "idx_gateway_channel_usage"},
		errors.New("duplicate key value violates unique constraint idx_gateway_channel_usage"),
	} {
		if gatewayUsageAssignmentConflict(err) {
			t.Fatalf("unrelated error was recognized as usage conflict: %v", err)
		}
	}
}

func TestRequestCostReconcileClaimRejectsStaleWorkerWrites(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Microsecond)

	type staleWrite struct {
		name string
		run  func(id uint, oldLease time.Time) error
	}
	writes := []staleWrite{
		{
			name: "retry",
			run: func(id uint, oldLease time.Time) error {
				return store.MarkClaimedRequestCostReconcileRetry(id, oldLease, now.Add(5*time.Minute), "stale retry")
			},
		},
		{
			name: "failed",
			run: func(id uint, oldLease time.Time) error {
				return store.MarkClaimedRequestCostReconcileFailed(id, oldLease, "stale failure")
			},
		},
		{
			name: "conflict",
			run: func(id uint, oldLease time.Time) error {
				_, err := store.MarkClaimedRequestCostReconcileConflict(id, oldLease, now.Add(10*time.Second), 3)
				return err
			},
		},
		{
			name: "release",
			run: func(id uint, oldLease time.Time) error {
				return store.ReleaseRequestCostReconcileClaim(id, oldLease, now)
			},
		},
		{
			name: "success",
			run: func(id uint, oldLease time.Time) error {
				return store.UpdateClaimedRequestLogActualCostWithUsage(id, oldLease, "usage-stale", 0.2, 0.2, gatewayCostSourceSub2APIActualCost, GatewayRequestUsageMetrics{})
			},
		},
	}

	for _, write := range writes {
		t.Run(write.name, func(t *testing.T) {
			log := &GatewayRequestLog{
				DownstreamKeyID: 1,
				ChannelID:       2,
				Method:          "POST",
				Path:            "/v1/responses",
				StatusCode:      200,
				Cost:            0.1,
				CostEstimated:   true,
				CreatedAt:       now,
			}
			if err := store.AppendRequestLog(log); err != nil {
				t.Fatalf("append log: %v", err)
			}
			if err := store.ScheduleRequestCostReconcile(log.ID, now.Add(-time.Second)); err != nil {
				t.Fatalf("schedule log: %v", err)
			}

			oldLease := now.Add(time.Minute)
			claimed, err := store.ClaimRequestCostReconcile(log.ID, now, oldLease)
			if err != nil || !claimed {
				t.Fatalf("old claim = %v err=%v", claimed, err)
			}
			newLease := now.Add(2 * time.Minute)
			claimed, err = store.ClaimRequestCostReconcile(log.ID, oldLease.Add(time.Second), newLease)
			if err != nil || !claimed {
				t.Fatalf("new claim = %v err=%v", claimed, err)
			}
			var before GatewayRequestLog
			if err := db.First(&before, log.ID).Error; err != nil {
				t.Fatalf("read log before stale write: %v", err)
			}

			if err := write.run(log.ID, oldLease); !errors.Is(err, ErrRequestCostReconcileLeaseLost) {
				t.Fatalf("stale %s error = %v, want lease lost", write.name, err)
			}
			var stored GatewayRequestLog
			if err := db.First(&stored, log.ID).Error; err != nil {
				t.Fatalf("read log: %v", err)
			}
			if !reflect.DeepEqual(stored, before) {
				t.Fatalf("stale %s mutated request state:\nbefore=%+v\nafter=%+v", write.name, before, stored)
			}
		})
	}
}

func TestCurrentRequestCostReconcileClaimCanComplete(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	log := &GatewayRequestLog{
		DownstreamKeyID:     1,
		ChannelID:           2,
		Method:              "POST",
		Path:                "/v1/responses",
		StatusCode:          200,
		Cost:                0.1,
		CostEstimated:       true,
		PromptTokens:        12,
		CompletionTokens:    3,
		TotalTokens:         15,
		CacheCreationTokens: 4,
		CacheReadTokens:     8,
		CreatedAt:           now,
	}
	if err := store.AppendRequestLog(log); err != nil {
		t.Fatalf("append log: %v", err)
	}
	if err := store.ScheduleRequestCostReconcile(log.ID, now); err != nil {
		t.Fatalf("schedule log: %v", err)
	}
	leaseUntil := now.Add(time.Minute)
	claimed, err := store.ClaimRequestCostReconcile(log.ID, now, leaseUntil)
	if err != nil || !claimed {
		t.Fatalf("claim = %v err=%v", claimed, err)
	}
	if err := store.UpdateClaimedRequestLogActualCostWithUsage(log.ID, leaseUntil, "usage-current", 0.25, 0.25, gatewayCostSourceSub2APIActualCost, GatewayRequestUsageMetrics{}); err != nil {
		t.Fatalf("complete claimed reconcile: %v", err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatalf("read log: %v", err)
	}
	if stored.CostReconcileAt != nil || stored.Cost != 0.25 || stored.CostEstimated || stored.CostStatus != GatewayCostStatusReconciled || stored.UpstreamUsageID == nil || *stored.UpstreamUsageID != "usage-current" {
		t.Fatalf("completed reconcile = %+v", stored)
	}
	if stored.PromptTokens != 12 || stored.CompletionTokens != 3 || stored.TotalTokens != 15 || stored.CacheCreationTokens != 4 || stored.CacheReadTokens != 8 {
		t.Fatalf("reconcile without authoritative usage flags overwrote existing metrics: %+v", stored)
	}
}
