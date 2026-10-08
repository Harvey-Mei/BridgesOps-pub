package gateway

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type fakeRequestDispatchRecoveryStore struct {
	list     func(time.Time, time.Time, int) ([]storage.GatewayRequestDispatchAudit, error)
	finalize func(uint64, *storage.GatewayRequestLog, string, time.Time, time.Time) (bool, error)
	complete func(time.Time, time.Time, int) (int64, error)
	prune    func(time.Time, time.Time, int) (int64, error)
}

func (f *fakeRequestDispatchRecoveryStore) ListRecoverableRequestDispatches(legacyBefore, at time.Time, limit int) ([]storage.GatewayRequestDispatchAudit, error) {
	return f.list(legacyBefore, at, limit)
}

func (f *fakeRequestDispatchRecoveryStore) FinalizeRecoveredRequestDispatchResult(id uint64, log *storage.GatewayRequestLog, terminalKind string, legacyBefore, at time.Time) (bool, error) {
	return f.finalize(id, log, terminalKind, legacyBefore, at)
}

func (f *fakeRequestDispatchRecoveryStore) CompleteStaleRequestDispatches(before, at time.Time, limit int) (int64, error) {
	return f.complete(before, at, limit)
}

func (f *fakeRequestDispatchRecoveryStore) PruneRequestDispatchAudits(finalizedBefore, completedBefore time.Time, limit int) (int64, error) {
	return f.prune(finalizedBefore, completedBefore, limit)
}

func TestRecoverRequestDispatchOrphansFinalizesNeutralLogsInPhaseOrder(t *testing.T) {
	now := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.FixedZone("test", 8*60*60))
	nowUTC := now.UTC()
	acceptedAt := nowUTC.Add(-2 * time.Hour)
	routeSnapshot, err := storage.NewGatewayRouteExclusionSnapshot(nowUTC.Add(-3*time.Hour), nil)
	if err != nil {
		t.Fatalf("build route exclusion snapshot: %v", err)
	}
	audits := []storage.GatewayRequestDispatchAudit{
		{
			ID:                            11,
			TelemetryVersion:              storage.GatewayRequestTelemetryVersion,
			LogicalRequestTraceID:         strings.Repeat("a", 32),
			RoutingMode:                   storage.GatewayRoutingModeSmart,
			GatewayApp:                    string(storage.GatewayAppCodex),
			RouteExclusionSnapshotVersion: storage.GatewayRouteExclusionSnapshotVersion,
			RouteExclusionsSnapshot:       routeSnapshot,
			LegacyOwnerless:               true,
			ChannelID:                     21,
			Method:                        http.MethodPost,
			Path:                          "/v1/responses",
			RequestStartedAt:              nowUTC.Add(-2 * time.Hour),
			AcceptedAt:                    &acceptedAt,
			ProviderRequestID:             "provider-accepted",
		},
		{
			ID:                            12,
			TelemetryVersion:              storage.GatewayRequestTelemetryVersion,
			LogicalRequestTraceID:         strings.Repeat("b", 32),
			RoutingMode:                   storage.GatewayRoutingModeSmart,
			GatewayApp:                    string(storage.GatewayAppCodex),
			RouteExclusionSnapshotVersion: storage.GatewayRouteExclusionSnapshotVersion,
			RouteExclusionsSnapshot:       routeSnapshot,
			LegacyOwnerless:               true,
			ChannelID:                     22,
			Method:                        http.MethodPost,
			Path:                          "/v1/images/generations",
			RequestStartedAt:              nowUTC.Add(time.Minute),
			BillingCommitted:              true,
			ProviderRequestID:             "provider-committed",
		},
	}

	var events []string
	var finalized []storage.GatewayRequestLog
	var reconciled []storage.GatewayRequestLog
	store := &fakeRequestDispatchRecoveryStore{}
	store.list = func(before, at time.Time, limit int) ([]storage.GatewayRequestDispatchAudit, error) {
		events = append(events, "list")
		if !before.Equal(nowUTC.Add(-requestDispatchRecoveryOrphanGrace)) || !at.Equal(nowUTC) || limit != requestDispatchRecoveryBatchSize {
			t.Fatalf("list args = (%v, %v, %d)", before, at, limit)
		}
		return audits, nil
	}
	store.finalize = func(id uint64, log *storage.GatewayRequestLog, terminalKind string, before, at time.Time) (bool, error) {
		events = append(events, fmt.Sprintf("finalize:%d", id))
		if terminalKind != requestDispatchRecoveryTerminalKind || !before.Equal(nowUTC.Add(-requestDispatchRecoveryOrphanGrace)) || !at.Equal(nowUTC) {
			t.Fatalf("finalize metadata = (%q, %v, %v)", terminalKind, before, at)
		}
		finalized = append(finalized, *log)
		for _, audit := range audits {
			if audit.ID == id {
				// Match the real store contract: finalize applies the durable safe
				// snapshot and returns the complete persisted request log.
				log.ID = uint(id + 100)
				log.ChannelID = audit.ChannelID
				log.Method = audit.Method
				log.Path = audit.Path
				log.RouteExclusionSnapshotVersion = audit.RouteExclusionSnapshotVersion
				log.RouteExclusionsSnapshot = audit.RouteExclusionsSnapshot
				return true, nil
			}
		}
		return false, fmt.Errorf("unknown audit %d", id)
	}
	store.complete = func(before, at time.Time, limit int) (int64, error) {
		events = append(events, "complete")
		if !before.Equal(nowUTC.Add(-requestDispatchRecoveryOrphanGrace)) || !at.Equal(nowUTC) || limit != requestDispatchRecoveryBatchSize {
			t.Fatalf("complete args = (%v, %v, %d)", before, at, limit)
		}
		return 0, nil
	}
	store.prune = func(finalizedBefore, completedBefore time.Time, limit int) (int64, error) {
		events = append(events, "prune")
		if !finalizedBefore.Equal(nowUTC.Add(-requestDispatchRecoveryFinalizedRetention)) || !completedBefore.Equal(nowUTC.Add(-requestDispatchRecoveryCompletedRetention)) || limit != requestDispatchRecoveryBatchSize {
			t.Fatalf("prune args = (%v, %v, %d)", finalizedBefore, completedBefore, limit)
		}
		return 0, nil
	}

	err = recoverRequestDispatchOrphans(store, func(log *storage.GatewayRequestLog, startedAt, finishedAt time.Time) {
		events = append(events, fmt.Sprintf("reconcile:%d", log.ID))
		if !startedAt.Equal(audits[len(reconciled)].RequestStartedAt) || !finishedAt.Equal(nowUTC) {
			t.Fatalf("reconcile times = (%v, %v)", startedAt, finishedAt)
		}
		reconciled = append(reconciled, *log)
	}, now)
	if err != nil {
		t.Fatalf("recover request dispatches: %v", err)
	}

	wantEvents := []string{"list", "finalize:11", "reconcile:111", "finalize:12", "reconcile:112", "complete", "prune"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
	if len(finalized) != 2 || len(reconciled) != 2 {
		t.Fatalf("finalized=%d reconciled=%d", len(finalized), len(reconciled))
	}
	for i, log := range finalized {
		if log.StatusCode != http.StatusBadGateway || log.Error != requestDispatchRecoveryError || log.RouteReason != requestDispatchRecoveryRouteReason || log.TerminalFailureCode != requestDispatchRecoveryRouteReason {
			t.Fatalf("neutral log %d = %+v", i, log)
		}
		if log.UpstreamRequestID != audits[i].ProviderRequestID || log.UpstreamAccepted != (audits[i].AcceptedAt != nil) || log.BillingCommitted != audits[i].BillingCommitted {
			t.Fatalf("durable evidence fields %d = %+v", i, log)
		}
		wantReconcileAt := InitialRequestCostReconcileAt(nowUTC)
		if log.CostReconcileAt == nil || !log.CostReconcileAt.Equal(wantReconcileAt) {
			t.Fatalf("cost reconcile time %d = %v, want %v", i, log.CostReconcileAt, wantReconcileAt)
		}
		if !log.CreatedAt.Equal(nowUTC) {
			t.Fatalf("created at %d = %v, want %v", i, log.CreatedAt, nowUTC)
		}
	}
	if finalized[0].LatencyMs != (2 * time.Hour).Milliseconds() {
		t.Fatalf("accepted latency = %d", finalized[0].LatencyMs)
	}
	if finalized[1].LatencyMs != 0 {
		t.Fatalf("future-start latency = %d, want 0", finalized[1].LatencyMs)
	}
	if reconciled[0].ID == 0 || reconciled[0].ChannelID != audits[0].ChannelID || reconciled[1].ID == 0 || reconciled[1].ChannelID != audits[1].ChannelID {
		t.Fatalf("reconciler did not receive complete persisted logs: %+v", reconciled)
	}
	for index := range reconciled {
		if reconciled[index].RouteExclusionSnapshotVersion != storage.GatewayRouteExclusionSnapshotVersion || reconciled[index].RouteExclusionsSnapshot != routeSnapshot {
			t.Fatalf("recovered route exclusion snapshot %d = version %d raw %q", index, reconciled[index].RouteExclusionSnapshotVersion, reconciled[index].RouteExclusionsSnapshot)
		}
	}
}

func TestRecoverRequestDispatchOrphansReconcilesIdempotentFinalizeResult(t *testing.T) {
	now := time.Date(2026, time.July, 23, 4, 0, 0, 0, time.UTC)
	acceptedAt := now.Add(-2 * time.Hour)
	audit := storage.GatewayRequestDispatchAudit{ID: 91, ChannelID: 7, RequestStartedAt: now.Add(-2 * time.Hour), AcceptedAt: &acceptedAt}
	reconcileCalls := 0
	store := &fakeRequestDispatchRecoveryStore{
		list: func(time.Time, time.Time, int) ([]storage.GatewayRequestDispatchAudit, error) {
			return []storage.GatewayRequestDispatchAudit{audit}, nil
		},
		finalize: func(_ uint64, log *storage.GatewayRequestLog, _ string, _ time.Time, _ time.Time) (bool, error) {
			log.ID = 901
			log.ChannelID = audit.ChannelID
			return false, nil
		},
		complete: func(time.Time, time.Time, int) (int64, error) { return 0, nil },
		prune:    func(time.Time, time.Time, int) (int64, error) { return 0, nil },
	}

	err := recoverRequestDispatchOrphans(store, func(log *storage.GatewayRequestLog, _, _ time.Time) {
		reconcileCalls++
		if log.ID != 901 || log.ChannelID != audit.ChannelID {
			t.Fatalf("idempotent finalize log = %+v", log)
		}
	}, now)
	if err != nil {
		t.Fatalf("recover request dispatches: %v", err)
	}
	if reconcileCalls != 1 {
		t.Fatalf("reconcile calls = %d, want 1", reconcileCalls)
	}
}

func TestRecoverRequestDispatchOrphansBoundsEveryPhase(t *testing.T) {
	now := time.Date(2026, time.July, 23, 4, 0, 0, 0, time.UTC)
	acceptedAt := now.Add(-2 * time.Hour)
	listCalls := 0
	finalizeCalls := 0
	reconcileCalls := 0
	completeCalls := 0
	pruneCalls := 0
	store := &fakeRequestDispatchRecoveryStore{}
	store.list = func(_ time.Time, _ time.Time, limit int) ([]storage.GatewayRequestDispatchAudit, error) {
		listCalls++
		if limit != requestDispatchRecoveryBatchSize {
			t.Fatalf("list limit = %d", limit)
		}
		rows := make([]storage.GatewayRequestDispatchAudit, limit)
		for i := range rows {
			rows[i] = storage.GatewayRequestDispatchAudit{
				ID:               uint64((listCalls-1)*limit + i + 1),
				RequestStartedAt: now.Add(-2 * time.Hour),
				AcceptedAt:       &acceptedAt,
			}
		}
		return rows, nil
	}
	store.finalize = func(id uint64, log *storage.GatewayRequestLog, _ string, _ time.Time, _ time.Time) (bool, error) {
		finalizeCalls++
		log.ID = uint(id)
		log.ChannelID = 1
		return true, nil
	}
	store.complete = func(_ time.Time, _ time.Time, limit int) (int64, error) {
		completeCalls++
		return int64(limit), nil
	}
	store.prune = func(_ time.Time, _ time.Time, limit int) (int64, error) {
		pruneCalls++
		return int64(limit), nil
	}

	err := recoverRequestDispatchOrphans(store, func(*storage.GatewayRequestLog, time.Time, time.Time) {
		reconcileCalls++
	}, now)
	if err != nil {
		t.Fatalf("recover request dispatches: %v", err)
	}
	wantBatches := requestDispatchRecoveryMaxRowsPerPhase / requestDispatchRecoveryBatchSize
	if listCalls != wantBatches || completeCalls != wantBatches || pruneCalls != wantBatches {
		t.Fatalf("phase calls = list:%d complete:%d prune:%d, want %d each", listCalls, completeCalls, pruneCalls, wantBatches)
	}
	if finalizeCalls != requestDispatchRecoveryMaxRowsPerPhase || reconcileCalls != requestDispatchRecoveryMaxRowsPerPhase {
		t.Fatalf("row calls = finalize:%d reconcile:%d, want %d each", finalizeCalls, reconcileCalls, requestDispatchRecoveryMaxRowsPerPhase)
	}
}

func TestRecoverRequestDispatchOrphansStopsFailedPhaseButRunsCleanup(t *testing.T) {
	now := time.Date(2026, time.July, 23, 4, 0, 0, 0, time.UTC)
	acceptedAt := now.Add(-2 * time.Hour)
	errFinalize := errors.New("finalize unavailable")
	errComplete := errors.New("complete unavailable")
	errPrune := errors.New("prune unavailable")
	var events []string
	store := &fakeRequestDispatchRecoveryStore{
		list: func(time.Time, time.Time, int) ([]storage.GatewayRequestDispatchAudit, error) {
			events = append(events, "list")
			return []storage.GatewayRequestDispatchAudit{
				{ID: 41, RequestStartedAt: now.Add(-2 * time.Hour), AcceptedAt: &acceptedAt},
				{ID: 42, ChannelID: 9, RequestStartedAt: now.Add(-2 * time.Hour), AcceptedAt: &acceptedAt},
			}, nil
		},
		finalize: func(id uint64, log *storage.GatewayRequestLog, _ string, _ time.Time, _ time.Time) (bool, error) {
			events = append(events, fmt.Sprintf("finalize:%d", id))
			if id == 41 {
				return false, errFinalize
			}
			log.ID = 142
			log.ChannelID = 9
			return true, nil
		},
		complete: func(time.Time, time.Time, int) (int64, error) {
			events = append(events, "complete")
			return 0, errComplete
		},
		prune: func(time.Time, time.Time, int) (int64, error) {
			events = append(events, "prune")
			return 0, errPrune
		},
	}

	err := recoverRequestDispatchOrphans(store, func(log *storage.GatewayRequestLog, _ time.Time, _ time.Time) {
		events = append(events, "reconcile")
		if log.ID != 142 || log.ChannelID != 9 {
			t.Fatalf("successful same-batch log = %+v", log)
		}
	}, now)
	if !errors.Is(err, errFinalize) || !errors.Is(err, errComplete) || !errors.Is(err, errPrune) {
		t.Fatalf("joined error = %v", err)
	}
	if want := []string{"list", "finalize:41", "finalize:42", "reconcile", "complete", "prune"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestRecoverRequestDispatchOrphansStopsWhenAFullBatchRepeats(t *testing.T) {
	now := time.Date(2026, time.July, 23, 4, 0, 0, 0, time.UTC)
	acceptedAt := now.Add(-2 * time.Hour)
	rows := make([]storage.GatewayRequestDispatchAudit, requestDispatchRecoveryBatchSize)
	for i := range rows {
		rows[i] = storage.GatewayRequestDispatchAudit{ID: uint64(i + 1), RequestStartedAt: now.Add(-2 * time.Hour), AcceptedAt: &acceptedAt}
	}
	listCalls := 0
	completeCalls := 0
	pruneCalls := 0
	store := &fakeRequestDispatchRecoveryStore{
		list: func(time.Time, time.Time, int) ([]storage.GatewayRequestDispatchAudit, error) {
			listCalls++
			return rows, nil
		},
		finalize: func(id uint64, log *storage.GatewayRequestLog, _ string, _ time.Time, _ time.Time) (bool, error) {
			log.ID = uint(id)
			return false, nil
		},
		complete: func(time.Time, time.Time, int) (int64, error) {
			completeCalls++
			return 0, nil
		},
		prune: func(time.Time, time.Time, int) (int64, error) {
			pruneCalls++
			return 0, nil
		},
	}

	err := recoverRequestDispatchOrphans(store, func(*storage.GatewayRequestLog, time.Time, time.Time) {}, now)
	if err == nil || !strings.Contains(err.Error(), "made no progress") {
		t.Fatalf("no-progress error = %v", err)
	}
	if listCalls != 2 || completeCalls != 1 || pruneCalls != 1 {
		t.Fatalf("calls = list:%d complete:%d prune:%d", listCalls, completeCalls, pruneCalls)
	}
}

func TestRecoverRequestDispatchOrphansCapsPeriodicEligibilityAtServiceStartup(t *testing.T) {
	startupAt := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{name: "initial pass", now: startupAt, want: startupAt.Add(-requestDispatchRecoveryOrphanGrace)},
		{name: "before grace elapses", now: startupAt.Add(30 * time.Minute), want: startupAt.Add(-30 * time.Minute)},
		{name: "at grace", now: startupAt.Add(time.Hour), want: startupAt},
		{name: "after grace", now: startupAt.Add(3 * time.Hour), want: startupAt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeRequestDispatchRecoveryStore{
				list: func(before, at time.Time, limit int) ([]storage.GatewayRequestDispatchAudit, error) {
					if !before.Equal(tc.want) || !at.Equal(tc.now) || limit != requestDispatchRecoveryBatchSize {
						t.Fatalf("recoverable before = (%v, %v, %d), want (%v, %v, %d)", before, at, limit, tc.want, tc.now, requestDispatchRecoveryBatchSize)
					}
					return nil, nil
				},
				finalize: func(uint64, *storage.GatewayRequestLog, string, time.Time, time.Time) (bool, error) {
					t.Fatal("no recoverable rows should be finalized")
					return false, nil
				},
				complete: func(before, at time.Time, limit int) (int64, error) {
					if !before.Equal(tc.want) || !at.Equal(tc.now) || limit != requestDispatchRecoveryBatchSize {
						t.Fatalf("stale before = (%v, %v, %d), want (%v, %v, %d)", before, at, limit, tc.want, tc.now, requestDispatchRecoveryBatchSize)
					}
					return 0, nil
				},
				prune: func(time.Time, time.Time, int) (int64, error) { return 0, nil },
			}
			if err := recoverRequestDispatchOrphansWithMaximumDispatchedAt(store, func(*storage.GatewayRequestLog, time.Time, time.Time) {}, tc.now, startupAt); err != nil {
				t.Fatalf("recover request dispatches: %v", err)
			}
		})
	}
}
