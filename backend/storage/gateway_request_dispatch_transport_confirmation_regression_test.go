package storage

import (
	"errors"
	"testing"
	"time"
)

func TestPersistRequestDispatchTerminalHandoffConfirmsArmedTransportBeforeResponseFacts(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	audit, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 681), now)
	if err != nil {
		t.Fatalf("begin request dispatch: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(audit.ID, now.Add(time.Millisecond)); err != nil {
		t.Fatalf("arm request dispatch: %v", err)
	}

	finishedAt := now.Add(3 * time.Millisecond)
	log := &GatewayRequestLog{StatusCode: 502, Error: "transport returned", CreatedAt: finishedAt}
	persisted, err := store.PersistRequestDispatchTerminalHandoff(
		audit.ID,
		audit.OwnerInstanceID,
		audit.OwnerFenceToken,
		"transport_returned_before_confirmation_commit",
		finishedAt,
		now,
		finishedAt,
		log,
		GatewayRequestDispatchFinalizationFacts{ResponseObservedAt: finishedAt},
		audit.TelemetryVersion,
		true,
	)
	if err != nil || !persisted {
		t.Fatalf("persist armed terminal handoff: persisted=%t err=%v", persisted, err)
	}

	stored, err := store.FindRequestDispatchAudit(audit.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.TransportState != GatewayRequestDispatchTransportStateConfirmed ||
		!stored.TransportReturnConfirmed || stored.TransportReturnConfirmedAt == nil ||
		!stored.PhysicallyDispatched || stored.ResponseObservedAt == nil ||
		stored.Stage != GatewayRequestDispatchStageResponded || stored.RecoveryRequestedAt == nil {
		t.Fatalf("armed terminal handoff evidence = %+v", stored)
	}

	var handoff GatewayRequestDispatchTerminalHandoff
	if err := db.First(&handoff, "dispatch_id = ?", audit.ID).Error; err != nil {
		t.Fatalf("read durable terminal handoff: %v", err)
	}
	decoded, err := DecodeGatewayRequestDispatchTerminalHandoff(handoff)
	if err != nil || decoded.Log == nil || decoded.Log.StatusCode != 502 || decoded.Facts.ResponseObservedAt.IsZero() {
		t.Fatalf("decoded terminal handoff = %+v err=%v", decoded, err)
	}
}

func TestPersistLinkedRequestDispatchTerminalHandoffsConfirmsArmedSuccessorBeforeResponseFacts(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	now := time.Now().UTC()
	previous, err := store.BeginRequestDispatch(requestDispatchCurrentTelemetryTestSnapshot(t, now, 682), now)
	if err != nil {
		t.Fatalf("begin previous request dispatch: %v", err)
	}
	confirmPlainRequestDispatchForTest(t, store, previous.ID, now.Add(time.Millisecond))

	nextSnapshot := requestDispatchCurrentTelemetryTestSnapshot(t, now, 683)
	nextSnapshot.PriorPhysicallyDispatchedFailureCount = 1
	next, err := store.BeginNextRequestDispatch(previous.ID, nextSnapshot, now.Add(4*time.Millisecond))
	if err != nil {
		t.Fatalf("begin linked successor: %v", err)
	}
	if err := store.MarkRequestDispatchPhysicallyDispatched(next.ID, now.Add(5*time.Millisecond)); err != nil {
		t.Fatalf("arm linked successor: %v", err)
	}

	finishedAt := now.Add(7 * time.Millisecond)
	err = store.PersistLinkedRequestDispatchTerminalHandoffs(
		GatewayRequestDispatchTerminalHandoffInput{
			DispatchID: previous.ID, OwnerInstanceID: previous.OwnerInstanceID, OwnerFenceToken: previous.OwnerFenceToken,
			TerminalKind: "linked_transport_returned", RequestedAt: finishedAt,
			StartedAt: now, FinishedAt: finishedAt,
			Log:              &GatewayRequestLog{StatusCode: 502, Error: "previous attempt failed", CreatedAt: finishedAt},
			TelemetryVersion: previous.TelemetryVersion, PhysicallyDispatched: true,
		},
		GatewayRequestDispatchTerminalHandoffInput{
			DispatchID: next.ID, OwnerInstanceID: next.OwnerInstanceID, OwnerFenceToken: next.OwnerFenceToken,
			TerminalKind: "linked_transport_returned", RequestedAt: finishedAt,
			StartedAt: now, FinishedAt: finishedAt,
			Log:              &GatewayRequestLog{StatusCode: 503, Error: "successor transport returned", CreatedAt: finishedAt},
			Facts:            GatewayRequestDispatchFinalizationFacts{ResponseObservedAt: finishedAt},
			TelemetryVersion: next.TelemetryVersion, PhysicallyDispatched: true,
		},
		false,
	)
	if err != nil {
		t.Fatalf("persist linked armed terminal handoffs: %v", err)
	}

	storedNext, err := store.FindRequestDispatchAudit(next.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedNext.TransportState != GatewayRequestDispatchTransportStateConfirmed ||
		!storedNext.TransportReturnConfirmed || storedNext.TransportReturnConfirmedAt == nil ||
		!storedNext.PhysicallyDispatched || storedNext.ResponseObservedAt == nil ||
		storedNext.Stage != GatewayRequestDispatchStageResponded || storedNext.RecoveryRequestedAt == nil {
		t.Fatalf("linked armed successor evidence = %+v", storedNext)
	}
	var handoffCount int64
	if err := db.Model(&GatewayRequestDispatchTerminalHandoff{}).
		Where("dispatch_id IN ?", []uint64{previous.ID, next.ID}).Count(&handoffCount).Error; err != nil {
		t.Fatal(err)
	}
	if handoffCount != 2 {
		t.Fatalf("linked durable terminal handoffs = %d, want 2", handoffCount)
	}
}

func TestUnconfirmedArmedImageDispatchRecoveryEntersManualReviewWithoutBillingEvidence(t *testing.T) {
	t.Run("durable terminal handoff", func(t *testing.T) {
		db := openTestDB(t)
		store := NewGateway(db)
		now := time.Now().UTC()
		audit, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now, 684), now)
		if err != nil {
			t.Fatalf("begin image request dispatch: %v", err)
		}
		if err := store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(audit.ID, now.Add(time.Millisecond)); err != nil {
			t.Fatalf("arm image request dispatch: %v", err)
		}
		if err := store.StopRequestDispatchOwner(nil, now.Add(2*time.Millisecond)); err != nil {
			t.Fatalf("stop request dispatch owner: %v", err)
		}
		if err := db.Create(&GatewayRequestDispatchTerminalHandoff{
			DispatchID: audit.ID, Version: gatewayRequestDispatchTerminalHandoffVersion,
			TerminalKind: "transport_entry_unconfirmed", RequestedAt: now.Add(3 * time.Millisecond),
			StartedAt: now, FinishedAt: now.Add(3 * time.Millisecond), FactsSnapshotJSON: "{}",
			TelemetryVersion: audit.TelemetryVersion, PhysicallyDispatched: true, CreatedAt: now.Add(3 * time.Millisecond),
		}).Error; err != nil {
			t.Fatalf("insert unconfirmed durable handoff: %v", err)
		}

		resolved, err := store.ResolveUnconfirmedRequestDispatchTerminalHandoff(
			audit.ID, "transport_entry_unconfirmed_manual_review", now.Add(-time.Hour), now.Add(time.Second),
		)
		if err != nil || !resolved {
			t.Fatalf("resolve unconfirmed durable handoff: resolved=%t err=%v", resolved, err)
		}
		assertUnconfirmedDispatchManualReview(t, store, audit.ID)
		if err := store.DeleteRequestDispatchTerminalHandoff(audit.ID); !errors.Is(err, ErrGatewayRequestDispatchManualReviewRequired) {
			t.Fatalf("delete manual-review durable handoff error = %v", err)
		}
	})

	t.Run("stale dispatch without handoff", func(t *testing.T) {
		db := openTestDB(t)
		store := NewGateway(db)
		now := time.Now().UTC()
		audit, err := store.BeginRequestDispatch(requestDispatchImageTestSnapshot(t, now, 685), now)
		if err != nil {
			t.Fatalf("begin image request dispatch: %v", err)
		}
		if err := store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(audit.ID, now.Add(time.Millisecond)); err != nil {
			t.Fatalf("arm image request dispatch: %v", err)
		}
		if err := store.StopRequestDispatchOwner(nil, now.Add(2*time.Millisecond)); err != nil {
			t.Fatalf("stop request dispatch owner: %v", err)
		}

		completed, err := store.CompleteStaleRequestDispatches(now.Add(-time.Hour), now.Add(time.Second), 10)
		if err != nil || completed != 1 {
			t.Fatalf("complete stale unconfirmed dispatches = %d err=%v", completed, err)
		}
		assertUnconfirmedDispatchManualReview(t, store, audit.ID)
	})
}

func assertUnconfirmedDispatchManualReview(t *testing.T, store *Gateway, dispatchID uint64) {
	t.Helper()
	stored, err := store.FindRequestDispatchAudit(dispatchID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.TransportState != GatewayRequestDispatchTransportStateManualReview ||
		stored.PhysicallyDispatched || stored.BillingCommitted ||
		stored.TerminalAt == nil || stored.RequestLogID != nil ||
		stored.ResponseObservedAt != nil || stored.AcceptedAt != nil {
		t.Fatalf("unconfirmed manual-review dispatch = %+v", stored)
	}
}
