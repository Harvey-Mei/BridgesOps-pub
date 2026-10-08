package gateway

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const (
	requestDispatchRecoveryBatchSize          = 100
	requestDispatchRecoveryMaxRowsPerPhase    = 400
	requestDispatchRecoveryOrphanGrace        = time.Hour
	requestDispatchRecoveryFinalizedRetention = 30 * 24 * time.Hour
	requestDispatchRecoveryCompletedRetention = 24 * time.Hour

	requestDispatchRecoveryRouteReason  = "dispatch_recovery"
	requestDispatchRecoveryTerminalKind = "orphaned_after_service_interruption"
	requestDispatchRecoveryError        = "request completion could not be confirmed after a service interruption"
)

type requestDispatchRecoveryStore interface {
	ListRecoverableRequestDispatches(legacyBefore, at time.Time, limit int) ([]storage.GatewayRequestDispatchAudit, error)
	FinalizeRecoveredRequestDispatchResult(id uint64, log *storage.GatewayRequestLog, terminalKind string, legacyBefore, at time.Time) (bool, error)
	CompleteStaleRequestDispatches(before, at time.Time, limit int) (int64, error)
	PruneRequestDispatchAudits(finalizedBefore, completedBefore time.Time, limit int) (int64, error)
}

// RecoverRequestDispatchOrphans reconstructs bounded, neutral request logs for
// old physical dispatches and dispatches that have durable acceptance or
// billing evidence. Startup and periodic scheduling live with the service
// lifecycle; this method performs one bounded maintenance pass only.
func (s *Service) RecoverRequestDispatchOrphans(now time.Time) error {
	return s.recoverRequestDispatchOrphansWithMaximumDispatchedAt(now, time.Time{})
}

// recoverRequestDispatchOrphansWithMaximumDispatchedAt lets the service
// lifecycle keep periodic recovery behind its startup waterline. A zero
// maximum preserves the one-shot default of now minus the grace period.
func (s *Service) recoverRequestDispatchOrphansWithMaximumDispatchedAt(now, maximumDispatchedAt time.Time) error {
	if s == nil || s.Store == nil {
		return nil
	}
	return recoverRequestDispatchOrphansWithMaximumDispatchedAt(s.Store, s.ReconcileRequestCost, now, maximumDispatchedAt)
}

func recoverRequestDispatchOrphans(
	store requestDispatchRecoveryStore,
	reconcile func(*storage.GatewayRequestLog, time.Time, time.Time),
	now time.Time,
) error {
	return recoverRequestDispatchOrphansWithMaximumDispatchedAt(store, reconcile, now, time.Time{})
}

func recoverRequestDispatchOrphansWithMaximumDispatchedAt(
	store requestDispatchRecoveryStore,
	reconcile func(*storage.GatewayRequestLog, time.Time, time.Time),
	now, maximumDispatchedAt time.Time,
) error {
	if store == nil {
		return errors.New("request dispatch recovery store is unavailable")
	}
	if reconcile == nil {
		return errors.New("request dispatch cost reconciler is unavailable")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	eligibleBefore := requestDispatchRecoveryEligibleBefore(now, maximumDispatchedAt)

	var phaseErrors []error
	if err := recoverAcceptedRequestDispatches(store, reconcile, now, eligibleBefore); err != nil {
		phaseErrors = append(phaseErrors, err)
	}
	if err := completeStaleRequestDispatches(store, now, eligibleBefore); err != nil {
		phaseErrors = append(phaseErrors, err)
	}
	if err := pruneRequestDispatchAudits(store, now); err != nil {
		phaseErrors = append(phaseErrors, err)
	}
	return errors.Join(phaseErrors...)
}

func recoverAcceptedRequestDispatches(
	store requestDispatchRecoveryStore,
	reconcile func(*storage.GatewayRequestLog, time.Time, time.Time),
	now, eligibleBefore time.Time,
) error {
	seen := make(map[uint64]struct{}, requestDispatchRecoveryMaxRowsPerPhase)
	remaining := requestDispatchRecoveryMaxRowsPerPhase

	for remaining > 0 {
		limit := requestDispatchRecoveryBatchSize
		if remaining < limit {
			limit = remaining
		}
		audits, err := store.ListRecoverableRequestDispatches(eligibleBefore, now, limit)
		if err != nil {
			return fmt.Errorf("list recoverable request dispatches: %w", err)
		}
		if len(audits) == 0 {
			return nil
		}
		if len(audits) > limit {
			return fmt.Errorf("list recoverable request dispatches returned %d rows for limit %d", len(audits), limit)
		}

		var batchErrors []error
		for i := range audits {
			audit := audits[i]
			if audit.ID == 0 {
				batchErrors = append(batchErrors, errors.New("recoverable request dispatch has no durable ID"))
				continue
			}
			if _, duplicate := seen[audit.ID]; duplicate {
				batchErrors = append(batchErrors, fmt.Errorf("request dispatch recovery made no progress: dispatch %d was listed again", audit.ID))
				continue
			}
			seen[audit.ID] = struct{}{}
			hasProviderRequestID := audit.ProviderRequestID != ""
			confirmedPhysical := audit.PhysicallyDispatched && (audit.TransportFenceVersion == 0 ||
				audit.TransportFenceVersion == storage.GatewayRequestDispatchTransportFenceVersion && audit.TransportState == storage.GatewayRequestDispatchTransportStateConfirmed)
			// Transport-fence v0 retains its historical commitment semantics because
			// its exact pre/post-RoundTripper state cannot be reconstructed.
			billingCommitted := audit.BillingCommitted && (audit.TransportFenceVersion == 0 || confirmedPhysical)
			if audit.AcceptedAt == nil && !billingCommitted && !hasProviderRequestID && !confirmedPhysical {
				batchErrors = append(batchErrors, fmt.Errorf("recoverable request dispatch %d has no durable physical, acceptance, billing, or provider-ID evidence", audit.ID))
				continue
			}

			reconcileAt := InitialRequestCostReconcileAt(now)
			providerOnly := audit.AcceptedAt == nil && !billingCommitted && hasProviderRequestID
			log := &storage.GatewayRequestLog{
				UpstreamRequestID: audit.ProviderRequestID,
				CostReconcileAt:   &reconcileAt,
				CreatedAt:         now,
			}
			if providerOnly {
				log.RecordKind = storage.GatewayRequestRecordKindDispatchCostEvidence
			} else {
				log.StatusCode = http.StatusBadGateway
				log.LatencyMs = requestDispatchRecoveryLatency(audit.RequestStartedAt, now)
				log.UpstreamAccepted = audit.AcceptedAt != nil
				log.BillingCommitted = billingCommitted
				log.RouteReason = requestDispatchRecoveryRouteReason
				if audit.TelemetryVersion != 0 {
					log.TerminalFailureCode = requestDispatchRecoveryRouteReason
				}
				log.Error = requestDispatchRecoveryError
			}
			terminalKind := requestDispatchRecoveryTerminalKind
			if strings.TrimSpace(audit.RecoveryTerminalKind) != "" {
				terminalKind = audit.RecoveryTerminalKind
			} else if !audit.LegacyOwnerless {
				terminalKind = storage.GatewayRequestDispatchTerminalOwnerLeaseLost
			}
			if _, err := store.FinalizeRecoveredRequestDispatchResult(audit.ID, log, terminalKind, eligibleBefore, now); err != nil {
				if errors.Is(err, storage.ErrGatewayRequestDispatchOwnerActive) {
					continue
				}
				batchErrors = append(batchErrors, fmt.Errorf("finalize recoverable request dispatch %d: %w", audit.ID, err))
				continue
			}

			// Finalize returns the complete persisted log even when another
			// instance won the CAS. Always hand that log to the cost dispatcher.
			reconcile(log, audit.RequestStartedAt, now)
			remaining--
		}
		if len(batchErrors) > 0 {
			// Do not fetch another oldest-first page while a persistent row
			// remains visible, but do let every other row in this page progress.
			return errors.Join(batchErrors...)
		}
		if len(audits) < limit {
			return nil
		}
	}
	return nil
}

func completeStaleRequestDispatches(store requestDispatchRecoveryStore, now, eligibleBefore time.Time) error {
	remaining := requestDispatchRecoveryMaxRowsPerPhase
	for remaining > 0 {
		limit := requestDispatchRecoveryBatchSize
		if remaining < limit {
			limit = remaining
		}
		completed, err := store.CompleteStaleRequestDispatches(eligibleBefore, now, limit)
		if err != nil {
			return fmt.Errorf("complete stale request dispatches: %w", err)
		}
		if completed <= 0 {
			return nil
		}
		if completed > int64(limit) {
			return fmt.Errorf("complete stale request dispatches affected %d rows for limit %d", completed, limit)
		}
		remaining -= int(completed)
		if completed < int64(limit) {
			return nil
		}
	}
	return nil
}

func pruneRequestDispatchAudits(store requestDispatchRecoveryStore, now time.Time) error {
	finalizedBefore := now.Add(-requestDispatchRecoveryFinalizedRetention)
	completedBefore := now.Add(-requestDispatchRecoveryCompletedRetention)
	remaining := requestDispatchRecoveryMaxRowsPerPhase
	for remaining > 0 {
		limit := requestDispatchRecoveryBatchSize
		if remaining < limit {
			limit = remaining
		}
		pruned, err := store.PruneRequestDispatchAudits(finalizedBefore, completedBefore, limit)
		if err != nil {
			return fmt.Errorf("prune request dispatch audits: %w", err)
		}
		if pruned <= 0 {
			return nil
		}
		if pruned > int64(limit) {
			return fmt.Errorf("prune request dispatch audits affected %d rows for limit %d", pruned, limit)
		}
		remaining -= int(pruned)
		if pruned < int64(limit) {
			return nil
		}
	}
	return nil
}

func requestDispatchRecoveryLatency(startedAt, finishedAt time.Time) int64 {
	if startedAt.IsZero() || !startedAt.Before(finishedAt) {
		return 0
	}
	return finishedAt.Sub(startedAt).Milliseconds()
}

func requestDispatchRecoveryEligibleBefore(now, maximumDispatchedAt time.Time) time.Time {
	eligibleBefore := now.Add(-requestDispatchRecoveryOrphanGrace)
	if maximumDispatchedAt.IsZero() {
		return eligibleBefore
	}
	maximumDispatchedAt = maximumDispatchedAt.UTC()
	if maximumDispatchedAt.Before(eligibleBefore) {
		return maximumDispatchedAt
	}
	return eligibleBefore
}
