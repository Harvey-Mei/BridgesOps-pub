package gateway

import (
	"errors"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// ErrRequestDispatchKnownUnsentHandoffFailed marks the fail-closed case where
// a pre-RoundTripper marker commit became ambiguous and its dedicated unsent
// handoff also failed. The audit must be left for owner-fenced orphan recovery;
// a generic terminal handoff is not allowed to erase the armed marker.
var ErrRequestDispatchKnownUnsentHandoffFailed = errors.New("gateway known-unsent request dispatch handoff failed")

// BeginRequestDispatch records the physical attempt before the upstream
// transport runs. A commit-unknown result is fail-closed: the exact audit is
// handed off as unsent and this owner is fenced before the caller can retry.
func (s *Service) BeginRequestDispatch(snapshot storage.GatewayRequestDispatchSnapshot, at time.Time) (*storage.GatewayRequestDispatchAudit, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway service is unavailable")
	}
	audit, err := s.Store.BeginRequestDispatch(snapshot, at)
	if err != nil {
		if errors.Is(err, storage.ErrGatewayRequestDispatchCommitUnknown) {
			if audit != nil && audit.ID != 0 {
				if handoffErr := s.Store.HandoffUnsentRequestDispatchRecovery(
					audit.ID,
					storage.GatewayRequestDispatchTerminalOwnerLeaseLost,
					time.Now().UTC(),
				); handoffErr != nil && s.Log != nil {
					s.Log.Warn("handoff unsent request dispatch after unknown begin commit", "dispatch_id", audit.ID, "err", handoffErr)
				}
			}
			s.failRequestDispatchOwner(err)
			s.restartFailedRequestDispatchOwner()
		}
		return audit, err
	}
	return audit, nil
}

// MarkRequestDispatchPhysicallyDispatchedAndCommitBilling is the only
// service-level operation that records a billed image dispatch. It keeps the
// physical-send marker and billing commitment in one owner-fenced storage
// transaction and must run from the RoundTripper entry hook, immediately
// before the upstream request becomes observable.
func (s *Service) MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(dispatchID uint64, at time.Time) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway service is unavailable")
	}
	err := s.Store.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(dispatchID, at)
	if !errors.Is(err, storage.ErrGatewayRequestDispatchCommitUnknown) {
		return err
	}
	// The paired marker runs before base.RoundTrip. An ambiguous commit can
	// therefore have persisted both facts while the upstream was certainly not
	// invoked. Clear the v1 pair through the narrow paired-commit-unknown
	// handoff before fencing this owner, so recovery cannot synthesize a
	// billable image failure. Ordinary unsent recovery may not clear a real
	// physical marker.
	handoffErr := s.Store.HandoffUnknownPairedPhysicalBillingCommitRecovery(
		dispatchID,
		storage.GatewayRequestDispatchTerminalOwnerLeaseLost,
		time.Now().UTC(),
	)
	if handoffErr != nil {
		if s.Log != nil {
			s.Log.Warn("handoff unsent request dispatch after unknown physical billing commit", "dispatch_id", dispatchID, "err", handoffErr)
		}
		err = errors.Join(err, ErrRequestDispatchKnownUnsentHandoffFailed, handoffErr)
	}
	s.failRequestDispatchOwner(err)
	if handoffErr == nil {
		s.restartFailedRequestDispatchOwner()
	}
	return err
}

// MarkRequestDispatchPhysicallyDispatched records a plain-text transport-entry
// marker. Like the paired image marker, an ambiguous commit happens before
// base.RoundTrip; clear only that proven-unsent marker before fencing this
// owner so recovery cannot turn it into a synthetic physical request.
func (s *Service) MarkRequestDispatchPhysicallyDispatched(dispatchID uint64, at time.Time) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway service is unavailable")
	}
	err := s.Store.MarkRequestDispatchPhysicallyDispatched(dispatchID, at)
	if !errors.Is(err, storage.ErrGatewayRequestDispatchCommitUnknown) {
		return err
	}
	if handoffErr := s.Store.HandoffUnknownPhysicalDispatchMarkRecovery(
		dispatchID,
		storage.GatewayRequestDispatchTerminalOwnerLeaseLost,
		time.Now().UTC(),
	); handoffErr != nil && s.Log != nil {
		s.Log.Warn("handoff unsent request dispatch after unknown physical marker commit", "dispatch_id", dispatchID, "err", handoffErr)
	}
	s.failRequestDispatchOwner(err)
	s.restartFailedRequestDispatchOwner()
	return err
}

// ConfirmRequestDispatchTransportInvocation records that the configured base
// RoundTripper returned control. Any failed confirmation fences this owner: the
// live handler may still durably hand off the exact terminal outcome, while an
// armed row that outlives the process can only become manual review.
func (s *Service) ConfirmRequestDispatchTransportInvocation(dispatchID uint64, at time.Time) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway service is unavailable")
	}
	err := s.Store.ConfirmRequestDispatchTransportInvocation(dispatchID, at)
	if err != nil {
		s.failRequestDispatchOwner(err)
	}
	return err
}

// BeginNextRequestDispatch creates only the linked preparation audit. Closing
// the predecessor is deferred until the successor's RoundTripper has returned
// and its confirmed transport state is durable.
func (s *Service) BeginNextRequestDispatch(previousID uint64, nextSnapshot storage.GatewayRequestDispatchSnapshot, at time.Time) (*storage.GatewayRequestDispatchAudit, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway service is unavailable")
	}
	next, err := s.Store.BeginNextRequestDispatch(previousID, nextSnapshot, at)
	if !errors.Is(err, storage.ErrGatewayRequestDispatchCommitUnknown) {
		return next, err
	}
	if next != nil && next.ID != 0 {
		if handoffErr := s.Store.HandoffUnsentRequestDispatchRecovery(
			next.ID,
			storage.GatewayRequestDispatchTerminalOwnerLeaseLost,
			time.Now().UTC(),
		); handoffErr != nil && s.Log != nil {
			s.Log.Warn("handoff possible unsent successor after unknown begin commit", "dispatch_id", next.ID, "err", handoffErr)
		}
	}
	s.failRequestDispatchOwner(err)
	s.restartFailedRequestDispatchOwner()
	return next, err
}

// FinalizeRequestDispatch commits the durable dispatch lifecycle and its
// request log as one storage transaction. The latency cache is updated only
// for the transaction that actually created the log; cost reconciliation is
// then entered through the same scheduler used by ordinary gateway logs.
func (s *Service) FinalizeRequestDispatch(dispatchID uint64, log *storage.GatewayRequestLog, terminalKind string, startedAt, finishedAt time.Time) (bool, error) {
	return s.FinalizeRequestDispatchWithFacts(dispatchID, log, terminalKind, storage.GatewayRequestDispatchFinalizationFacts{}, startedAt, finishedAt)
}

// FinalizeRequestDispatchWithFacts atomically carries response and acceptance
// facts observed by the handler into the same transaction as its terminal log.
func (s *Service) FinalizeRequestDispatchWithFacts(
	dispatchID uint64,
	log *storage.GatewayRequestLog,
	terminalKind string,
	facts storage.GatewayRequestDispatchFinalizationFacts,
	startedAt, finishedAt time.Time,
) (bool, error) {
	if s == nil || s.Store == nil {
		return false, errors.New("gateway service is unavailable")
	}
	created, err := s.Store.FinalizeRequestDispatchWithFactsResult(dispatchID, log, terminalKind, facts, finishedAt)
	if err != nil {
		return false, err
	}
	if created {
		s.recordRoutingLatency(log)
	}
	s.ReconcileRequestCost(log, startedAt, finishedAt)
	return created, nil
}

// BeginNextRequestDispatchAndClosePrevious atomically closes the prior failed
// physical attempt and creates the durable audit that gates the next HTTP Do.
func (s *Service) BeginNextRequestDispatchAndClosePrevious(
	previousID uint64,
	previousEvidence *storage.GatewayRequestLog,
	previousTerminalKind string,
	previousFacts storage.GatewayRequestDispatchFinalizationFacts,
	previousStartedAt time.Time,
	previousFinishedAt time.Time,
	nextSnapshot storage.GatewayRequestDispatchSnapshot,
	nextDispatchedAt time.Time,
) (*storage.GatewayRequestDispatchAudit, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway service is unavailable")
	}
	next, previousLogCreated, err := s.Store.BeginNextRequestDispatchAndClosePrevious(
		previousID,
		previousEvidence,
		previousTerminalKind,
		previousFacts,
		previousFinishedAt,
		nextSnapshot,
		nextDispatchedAt,
	)
	if err != nil {
		if errors.Is(err, storage.ErrGatewayRequestDispatchCommitUnknown) {
			// The previous/next rollover may have committed even though the
			// caller did not receive a successful COMMIT. Preserve both possible
			// rows, fence this process incarnation, and force the recovery worker
			// to close them before any further upstream dispatch is attempted.
			handoffAt := time.Now().UTC()
			handoffSucceeded := false
			if next != nil && next.ID != 0 {
				handoffErr := s.scheduleAmbiguousLinkedRequestDispatchRecovery(previousID, next.ID, previousEvidence, previousFacts, previousStartedAt, previousFinishedAt, true, handoffAt)
				if handoffErr != nil && s.Log != nil {
					s.Log.Error("atomically handoff linked request dispatches after unknown rollover commit", "previous_dispatch_id", previousID, "next_dispatch_id", next.ID, "err", handoffErr)
				} else if handoffErr == nil {
					handoffSucceeded = true
				}
			} else if handoffErr := s.Store.HandoffRequestDispatchRecoveryWithFacts(previousID, storage.GatewayRequestDispatchTerminalOwnerLeaseLost, previousFacts, handoffAt); handoffErr != nil && s.Log != nil {
				s.Log.Error("handoff previous request dispatch after unknown rollover commit", "dispatch_id", previousID, "err", handoffErr)
			}
			s.failRequestDispatchOwner(err)
			if handoffSucceeded {
				s.restartFailedRequestDispatchOwner()
			}
		}
		return next, err
	}
	if previousLogCreated {
		s.ReconcileRequestCost(previousEvidence, previousStartedAt, previousFinishedAt)
	}
	return next, nil
}

// ClosePreviousRequestDispatchAfterNextPhysical records the hidden
// intermediate outcome only after the successor has actually entered its
// RoundTripper. Terminal attempts use FinalizeRequestDispatch instead.
func (s *Service) ClosePreviousRequestDispatchAfterNextPhysical(
	previousID uint64,
	nextID uint64,
	previousEvidence *storage.GatewayRequestLog,
	previousTerminalKind string,
	previousFacts storage.GatewayRequestDispatchFinalizationFacts,
	previousStartedAt time.Time,
	previousFinishedAt time.Time,
) error {
	if s == nil || s.Store == nil {
		return errors.New("gateway service is unavailable")
	}
	previousLogCreated, err := s.Store.ClosePreviousRequestDispatchAfterNextPhysical(
		previousID,
		nextID,
		previousEvidence,
		previousTerminalKind,
		previousFacts,
		previousFinishedAt,
	)
	if err != nil {
		if errors.Is(err, storage.ErrGatewayRequestDispatchCommitUnknown) {
			// The prior closure may have committed after the successor physically
			// entered its RoundTripper. Preserve both possible physical attempts and
			// fence this owner before any deferred cleanup can classify either one.
			handoffAt := time.Now().UTC()
			handoffErr := s.scheduleAmbiguousLinkedRequestDispatchRecovery(previousID, nextID, previousEvidence, previousFacts, previousStartedAt, previousFinishedAt, false, handoffAt)
			if handoffErr != nil && s.Log != nil {
				s.Log.Error("atomically handoff linked request dispatches after unknown rollover close", "previous_dispatch_id", previousID, "next_dispatch_id", nextID, "err", handoffErr)
			}
			s.failRequestDispatchOwner(err)
			if handoffErr == nil {
				s.restartFailedRequestDispatchOwner()
			}
		}
		return err
	}
	if previousLogCreated {
		s.ReconcileRequestCost(previousEvidence, previousStartedAt, previousFinishedAt)
	}
	return nil
}

func (s *Service) scheduleAmbiguousLinkedRequestDispatchRecovery(
	previousID, nextID uint64,
	previousLog *storage.GatewayRequestLog,
	previousFacts storage.GatewayRequestDispatchFinalizationFacts,
	previousStartedAt, previousFinishedAt time.Time,
	knownUnsentSuccessor bool,
	requestedAt time.Time,
) error {
	previous, err := s.Store.FindRequestDispatchAudit(previousID)
	if err != nil {
		return err
	}
	next, err := s.Store.FindRequestDispatchAudit(nextID)
	if err != nil {
		return err
	}
	return s.ScheduleLinkedRequestDispatchRecovery(
		RequestDispatchRecoveryJob{
			DispatchID: previousID, OwnerInstanceID: previous.OwnerInstanceID, OwnerFenceToken: previous.OwnerFenceToken,
			TerminalKind: storage.GatewayRequestDispatchTerminalOwnerLeaseLost,
			RequestedAt:  requestedAt, StartedAt: previousStartedAt, FinishedAt: previousFinishedAt,
			Log: previousLog, Facts: previousFacts, TelemetryVersion: previous.TelemetryVersion,
			PhysicallyDispatched: previous.PhysicallyDispatched,
		},
		RequestDispatchRecoveryJob{
			DispatchID: nextID, OwnerInstanceID: next.OwnerInstanceID, OwnerFenceToken: next.OwnerFenceToken,
			TerminalKind: storage.GatewayRequestDispatchTerminalOwnerLeaseLost,
			RequestedAt:  requestedAt, StartedAt: next.RequestStartedAt, FinishedAt: requestedAt,
			TelemetryVersion: next.TelemetryVersion, PhysicallyDispatched: next.PhysicallyDispatched && !knownUnsentSuccessor,
		},
		knownUnsentSuccessor,
	)
}
