package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const gatewayRequestDispatchTerminalHandoffVersion = 1

var ErrGatewayRequestDispatchManualReviewRequired = errors.New("gateway request dispatch requires manual review")

// GatewayRequestDispatchTerminalHandoff is the durable source of truth for a
// terminal retry. The JSON columns preserve the exact request log and lifecycle
// facts that the live handler observed; the process-local queue only wakes the
// worker that consumes these rows.
type GatewayRequestDispatchTerminalHandoff struct {
	DispatchID           uint64 `gorm:"primaryKey"`
	Version              int    `gorm:"not null;default:1"`
	TerminalKind         string `gorm:"size:64;not null"`
	RequestedAt          time.Time
	StartedAt            time.Time
	FinishedAt           time.Time
	LogSnapshotJSON      *string `gorm:"column:log_snapshot;type:jsonb"`
	FactsSnapshotJSON    string  `gorm:"column:facts_snapshot;type:jsonb;not null;default:'{}'"`
	TelemetryVersion     int     `gorm:"not null;default:0"`
	PhysicallyDispatched bool    `gorm:"not null;default:false"`
	CreatedAt            time.Time
	Log                  *GatewayRequestLog                      `gorm:"-"`
	Facts                GatewayRequestDispatchFinalizationFacts `gorm:"-"`
}

// GatewayRequestDispatchTerminalHandoffInput carries one live handler's exact
// terminal outcome into either the single-dispatch or linked atomic handoff.
type GatewayRequestDispatchTerminalHandoffInput struct {
	DispatchID           uint64
	OwnerInstanceID      string
	OwnerFenceToken      uint64
	TerminalKind         string
	RequestedAt          time.Time
	StartedAt            time.Time
	FinishedAt           time.Time
	Log                  *GatewayRequestLog
	Facts                GatewayRequestDispatchFinalizationFacts
	TelemetryVersion     int
	PhysicallyDispatched bool
}

func (GatewayRequestDispatchTerminalHandoff) TableName() string {
	return "gateway_request_dispatch_terminal_handoffs"
}

// gatewayRequestDispatchTerminalLogSnapshot supplements GatewayRequestLog's
// public JSON shape with the internal immutable fields that finalization also
// needs. None of these fields contains a request body or credential.
type gatewayRequestDispatchTerminalLogSnapshot struct {
	*GatewayRequestLog
	RecordKind                          GatewayRequestRecordKind `json:"record_kind"`
	ChannelIdentityRevision             uint64                   `json:"channel_identity_revision"`
	UpstreamGroupID                     uint                     `json:"upstream_group_id"`
	UpstreamGroupGeneration             uint64                   `json:"upstream_group_generation"`
	UpstreamKeyLocalID                  uint                     `json:"upstream_key_local_id"`
	UpstreamKeyGeneration               uint64                   `json:"upstream_key_generation"`
	PriceCanonicalGroupName             string                   `json:"price_canonical_group_name"`
	CostReconcileLegacyPending          bool                     `json:"cost_reconcile_legacy_pending"`
	SessionHash                         string                   `json:"session_hash"`
	SessionAffinityOverriddenCandidates string                   `json:"session_affinity_overridden_candidates"`
	RouteExclusionsSnapshot             string                   `json:"route_exclusions_snapshot"`
	AggregateVersion                    int                      `json:"aggregate_version"`
	GroupCacheAggregateVersion          int                      `json:"group_cache_aggregate_version"`
}

func newGatewayRequestDispatchTerminalLogSnapshot(log *GatewayRequestLog) gatewayRequestDispatchTerminalLogSnapshot {
	return gatewayRequestDispatchTerminalLogSnapshot{
		GatewayRequestLog:                   log,
		RecordKind:                          log.RecordKind,
		ChannelIdentityRevision:             log.ChannelIdentityRevision,
		UpstreamGroupID:                     log.UpstreamGroupID,
		UpstreamGroupGeneration:             log.UpstreamGroupGeneration,
		UpstreamKeyLocalID:                  log.UpstreamKeyLocalID,
		UpstreamKeyGeneration:               log.UpstreamKeyGeneration,
		PriceCanonicalGroupName:             log.PriceCanonicalGroupName,
		CostReconcileLegacyPending:          log.CostReconcileLegacyPending,
		SessionHash:                         log.SessionHash,
		SessionAffinityOverriddenCandidates: log.SessionAffinityOverriddenCandidates,
		RouteExclusionsSnapshot:             log.RouteExclusionsSnapshot,
		AggregateVersion:                    log.AggregateVersion,
		GroupCacheAggregateVersion:          log.GroupCacheAggregateVersion,
	}
}

func (snapshot gatewayRequestDispatchTerminalLogSnapshot) requestLog() *GatewayRequestLog {
	if snapshot.GatewayRequestLog == nil {
		return nil
	}
	log := snapshot.GatewayRequestLog
	log.RecordKind = snapshot.RecordKind
	log.ChannelIdentityRevision = snapshot.ChannelIdentityRevision
	log.UpstreamGroupID = snapshot.UpstreamGroupID
	log.UpstreamGroupGeneration = snapshot.UpstreamGroupGeneration
	log.UpstreamKeyLocalID = snapshot.UpstreamKeyLocalID
	log.UpstreamKeyGeneration = snapshot.UpstreamKeyGeneration
	log.PriceCanonicalGroupName = snapshot.PriceCanonicalGroupName
	log.CostReconcileLegacyPending = snapshot.CostReconcileLegacyPending
	log.SessionHash = snapshot.SessionHash
	log.SessionAffinityOverriddenCandidates = snapshot.SessionAffinityOverriddenCandidates
	log.RouteExclusionsSnapshot = snapshot.RouteExclusionsSnapshot
	log.AggregateVersion = snapshot.AggregateVersion
	log.GroupCacheAggregateVersion = snapshot.GroupCacheAggregateVersion
	return log
}

func encodeGatewayRequestDispatchTerminalLog(log *GatewayRequestLog) (*string, error) {
	if log == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(newGatewayRequestDispatchTerminalLogSnapshot(log))
	if err != nil {
		return nil, fmt.Errorf("encode request dispatch terminal log: %w", err)
	}
	if len(encoded) > 256*1024 {
		return nil, errors.New("request dispatch terminal log exceeds durable handoff limit")
	}
	value := string(encoded)
	return &value, nil
}

func decodeGatewayRequestDispatchTerminalLog(encoded *string) (*GatewayRequestLog, error) {
	if encoded == nil {
		return nil, nil
	}
	if len(*encoded) > 256*1024 {
		return nil, errors.New("request dispatch terminal log exceeds durable handoff limit")
	}
	snapshot := gatewayRequestDispatchTerminalLogSnapshot{GatewayRequestLog: &GatewayRequestLog{}}
	if err := json.Unmarshal([]byte(*encoded), &snapshot); err != nil {
		return nil, fmt.Errorf("decode request dispatch terminal log: %w", err)
	}
	return snapshot.requestLog(), nil
}

func encodeGatewayRequestDispatchTerminalFacts(facts GatewayRequestDispatchFinalizationFacts) (string, error) {
	encoded, err := json.Marshal(facts)
	if err != nil {
		return "", fmt.Errorf("encode request dispatch terminal facts: %w", err)
	}
	if len(encoded) > 8*1024 {
		return "", errors.New("request dispatch terminal facts exceed durable handoff limit")
	}
	return string(encoded), nil
}

func decodeGatewayRequestDispatchTerminalFacts(encoded string) (GatewayRequestDispatchFinalizationFacts, error) {
	var facts GatewayRequestDispatchFinalizationFacts
	if len(encoded) > 8*1024 {
		return facts, errors.New("request dispatch terminal facts exceed durable handoff limit")
	}
	if err := json.Unmarshal([]byte(encoded), &facts); err != nil {
		return facts, fmt.Errorf("decode request dispatch terminal facts: %w", err)
	}
	return facts, nil
}

func (g *Gateway) localRequestDispatchOwnerIdentity() (instanceID string, fenceToken uint64, stopped bool, err error) {
	if g == nil || g.db == nil {
		return "", 0, false, errors.New("gateway store is unavailable")
	}
	g.requestDispatchOwnerMu.Lock()
	defer g.requestDispatchOwnerMu.Unlock()
	if g.requestDispatchOwnerInitErr != nil {
		return "", 0, false, g.requestDispatchOwnerInitErr
	}
	if err := validateGatewayRequestDispatchOwnerIdentity(g.requestDispatchOwnerID, g.requestDispatchOwnerFenceToken); err != nil {
		return "", 0, false, err
	}
	return g.requestDispatchOwnerID, g.requestDispatchOwnerFenceToken, g.requestDispatchOwnerStopped, nil
}

// PersistRequestDispatchTerminalHandoff is first-wins. It attests the exact
// local owner, records any facts, and can promote an armed transport to
// confirmed when the live handler attests that base RoundTrip returned. It may
// complete a prepared transport as not_dispatched, but it deliberately cannot
// clear an armed marker. Commit-unknown pre-Do markers must use their narrow
// known-unsent handoff so this generic API cannot erase a real attempt.
func (g *Gateway) PersistRequestDispatchTerminalHandoff(
	dispatchID uint64,
	ownerInstanceID string,
	ownerFenceToken uint64,
	terminalKind string,
	requestedAt, startedAt, finishedAt time.Time,
	log *GatewayRequestLog,
	facts GatewayRequestDispatchFinalizationFacts,
	telemetryVersion int,
	physicallyDispatched bool,
) (persisted bool, err error) {
	if g == nil || g.db == nil {
		return false, errors.New("gateway store is unavailable")
	}
	terminalKind, err = validateGatewayRequestDispatchText("request dispatch recovery terminal kind", terminalKind, 64, true)
	if err != nil {
		return false, err
	}
	requestedAt = normalizeGatewayRequestDispatchTime(requestedAt)
	startedAt = normalizeGatewayRequestDispatchTime(startedAt)
	finishedAt = normalizeGatewayRequestDispatchTime(finishedAt)
	if finishedAt.Before(startedAt) || requestedAt.Before(startedAt) {
		return false, errors.New("request dispatch terminal handoff times are invalid")
	}
	facts, err = normalizeGatewayRequestDispatchFinalizationFacts(facts, finishedAt)
	if err != nil {
		return false, err
	}
	if physicallyDispatched && log == nil {
		return false, errors.New("physical request dispatch handoff requires an exact terminal log")
	}
	if !physicallyDispatched && (facts.Accepted || facts.ProviderRequestID != "" || log != nil) {
		return false, errors.New("nonphysical request dispatch handoff carries terminal evidence")
	}
	logSnapshot, err := encodeGatewayRequestDispatchTerminalLog(log)
	if err != nil {
		return false, err
	}
	factsSnapshot, err := encodeGatewayRequestDispatchTerminalFacts(facts)
	if err != nil {
		return false, err
	}
	ownerInstanceID = strings.TrimSpace(ownerInstanceID)
	if ownerInstanceID == "" || ownerFenceToken == 0 {
		return false, errors.New("request dispatch terminal handoff owner identity is required")
	}
	handoff := &GatewayRequestDispatchTerminalHandoff{
		DispatchID:           dispatchID,
		Version:              gatewayRequestDispatchTerminalHandoffVersion,
		TerminalKind:         terminalKind,
		RequestedAt:          requestedAt,
		StartedAt:            startedAt,
		FinishedAt:           finishedAt,
		LogSnapshotJSON:      logSnapshot,
		FactsSnapshotJSON:    factsSnapshot,
		TelemetryVersion:     telemetryVersion,
		PhysicallyDispatched: physicallyDispatched,
		CreatedAt:            requestedAt,
	}
	err = g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, dispatchID)
		if err != nil {
			return err
		}
		if audit.OwnerInstanceID != ownerInstanceID || audit.OwnerFenceToken != ownerFenceToken {
			return ErrGatewayRequestDispatchOwnerFenced
		}
		if audit.TerminalAt != nil {
			return nil
		}
		var existing int64
		if err := tx.Model(&GatewayRequestDispatchTerminalHandoff{}).Where("dispatch_id = ?", dispatchID).Count(&existing).Error; err != nil {
			return err
		}
		if existing > 0 {
			persisted = true
			return nil
		}
		if telemetryVersion != audit.TelemetryVersion {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && physicallyDispatched &&
			audit.PhysicallyDispatched && audit.TransportState == GatewayRequestDispatchTransportStateArmed {
			confirmation := tx.Model(&GatewayRequestDispatchAudit{}).
				Where("id = ? AND transport_fence_version = ? AND transport_state = ? AND transport_confirmation_version = 1 AND transport_return_confirmed = FALSE AND transport_return_confirmed_at IS NULL AND physically_dispatched = TRUE AND terminal_at IS NULL AND request_log_id IS NULL AND recovery_requested_at IS NULL",
					audit.ID, GatewayRequestDispatchTransportFenceVersion, GatewayRequestDispatchTransportStateArmed).
				Updates(map[string]any{
					"transport_state":               GatewayRequestDispatchTransportStateConfirmed,
					"transport_return_confirmed":    true,
					"transport_return_confirmed_at": finishedAt,
				})
			if confirmation.Error != nil {
				return confirmation.Error
			}
			if confirmation.RowsAffected != 1 {
				return ErrGatewayRequestDispatchConflict
			}
			audit.TransportState = GatewayRequestDispatchTransportStateConfirmed
			audit.TransportReturnConfirmed = true
			confirmedAt := finishedAt
			audit.TransportReturnConfirmedAt = &confirmedAt
		}
		updates := map[string]any{
			"recovery_requested_at":  requestedAt,
			"recovery_terminal_kind": terminalKind,
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion {
			switch {
			case physicallyDispatched && audit.PhysicallyDispatched && audit.TransportState == GatewayRequestDispatchTransportStateConfirmed:
			case !physicallyDispatched && audit.TransportState == GatewayRequestDispatchTransportStatePrepared:
				updates["transport_state"] = GatewayRequestDispatchTransportStateNotDispatched
			case !physicallyDispatched && audit.TransportState == GatewayRequestDispatchTransportStateNotDispatched:
			default:
				return ErrGatewayRequestDispatchConflict
			}
		} else if physicallyDispatched != audit.PhysicallyDispatched {
			return ErrGatewayRequestDispatchConflict
		}
		if facts.ProviderRequestID != "" {
			if audit.ProviderRequestID != "" && audit.ProviderRequestID != facts.ProviderRequestID {
				return ErrGatewayRequestDispatchConflict
			}
			updates["provider_request_id"] = facts.ProviderRequestID
		}
		if !facts.ResponseObservedAt.IsZero() && audit.ResponseObservedAt == nil {
			updates["response_observed_at"] = facts.ResponseObservedAt
			if audit.Stage == GatewayRequestDispatchStageDispatched {
				updates["stage"] = GatewayRequestDispatchStageResponded
			}
		}
		if facts.Accepted {
			if audit.AcceptedAt != nil && (audit.AcceptedEventType != facts.AcceptedEventType || audit.AcceptedObjectID != facts.AcceptedObjectID) {
				return ErrGatewayRequestDispatchConflict
			}
			updates["stage"] = GatewayRequestDispatchStageAccepted
			updates["accepted_at"] = facts.AcceptedAt
			updates["accepted_event_type"] = facts.AcceptedEventType
			updates["accepted_object_id"] = facts.AcceptedObjectID
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND terminal_at IS NULL AND request_log_id IS NULL", audit.ID).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(handoff).Error; err != nil {
			return err
		}
		persisted = true
		return nil
	})
	return persisted, err
}

// PersistLinkedRequestDispatchTerminalHandoffs makes both exact terminal
// snapshots and both recovery fences visible in one transaction. This is the
// only safe handoff after a linked rollover becomes ambiguous: a worker can
// never observe one recoverable side without the other side's exact outcome.
func (g *Gateway) PersistLinkedRequestDispatchTerminalHandoffs(previousInput, nextInput GatewayRequestDispatchTerminalHandoffInput, knownUnsentSuccessor bool) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	if previousInput.DispatchID == 0 || nextInput.DispatchID == 0 || previousInput.DispatchID == nextInput.DispatchID {
		return errors.New("linked request dispatch ids are invalid")
	}
	previousHandoff, previousFacts, err := normalizeGatewayRequestDispatchTerminalHandoffInput(previousInput)
	if err != nil {
		return err
	}
	nextHandoff, nextFacts, err := normalizeGatewayRequestDispatchTerminalHandoffInput(nextInput)
	if err != nil {
		return err
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		firstID, secondID := previousInput.DispatchID, nextInput.DispatchID
		if secondID < firstID {
			firstID, secondID = secondID, firstID
		}
		first, err := lockGatewayRequestDispatchAuditTx(tx, firstID)
		if err != nil {
			return err
		}
		second, err := lockGatewayRequestDispatchAuditTx(tx, secondID)
		if err != nil {
			return err
		}
		previous, next := first, second
		if previous.ID != previousInput.DispatchID {
			previous, next = second, first
		}
		if next.RolloverPredecessorID == nil || *next.RolloverPredecessorID != previous.ID ||
			previous.OwnerInstanceID != next.OwnerInstanceID || previous.OwnerFenceToken != next.OwnerFenceToken ||
			previous.OwnerInstanceID != strings.TrimSpace(previousInput.OwnerInstanceID) || previous.OwnerFenceToken != previousInput.OwnerFenceToken ||
			next.OwnerInstanceID != strings.TrimSpace(nextInput.OwnerInstanceID) || next.OwnerFenceToken != nextInput.OwnerFenceToken {
			return ErrGatewayRequestDispatchConflict
		}
		if knownUnsentSuccessor && nextInput.PhysicallyDispatched {
			return ErrGatewayRequestDispatchConflict
		}
		if err := persistGatewayRequestDispatchTerminalHandoffTx(tx, previous, previousHandoff, previousFacts, previousInput.PhysicallyDispatched, false); err != nil {
			return err
		}
		return persistGatewayRequestDispatchTerminalHandoffTx(tx, next, nextHandoff, nextFacts, nextInput.PhysicallyDispatched, knownUnsentSuccessor)
	})
}

func normalizeGatewayRequestDispatchTerminalHandoffInput(input GatewayRequestDispatchTerminalHandoffInput) (*GatewayRequestDispatchTerminalHandoff, GatewayRequestDispatchFinalizationFacts, error) {
	var zero GatewayRequestDispatchFinalizationFacts
	terminalKind, err := validateGatewayRequestDispatchText("request dispatch recovery terminal kind", input.TerminalKind, 64, true)
	if err != nil {
		return nil, zero, err
	}
	requestedAt := normalizeGatewayRequestDispatchTime(input.RequestedAt)
	startedAt := normalizeGatewayRequestDispatchTime(input.StartedAt)
	finishedAt := normalizeGatewayRequestDispatchTime(input.FinishedAt)
	input.OwnerInstanceID = strings.TrimSpace(input.OwnerInstanceID)
	if input.DispatchID == 0 || input.OwnerInstanceID == "" || input.OwnerFenceToken == 0 || finishedAt.Before(startedAt) || requestedAt.Before(startedAt) {
		return nil, zero, errors.New("request dispatch terminal handoff input is invalid")
	}
	facts, err := normalizeGatewayRequestDispatchFinalizationFacts(input.Facts, finishedAt)
	if err != nil {
		return nil, zero, err
	}
	if input.PhysicallyDispatched && input.Log == nil {
		return nil, zero, errors.New("physical request dispatch handoff requires an exact terminal log")
	}
	if !input.PhysicallyDispatched && (facts.Accepted || facts.ProviderRequestID != "" || input.Log != nil) {
		return nil, zero, errors.New("nonphysical request dispatch handoff carries terminal evidence")
	}
	logSnapshot, err := encodeGatewayRequestDispatchTerminalLog(input.Log)
	if err != nil {
		return nil, zero, err
	}
	factsSnapshot, err := encodeGatewayRequestDispatchTerminalFacts(facts)
	if err != nil {
		return nil, zero, err
	}
	return &GatewayRequestDispatchTerminalHandoff{
		DispatchID: input.DispatchID, Version: gatewayRequestDispatchTerminalHandoffVersion,
		TerminalKind: terminalKind, RequestedAt: requestedAt, StartedAt: startedAt, FinishedAt: finishedAt,
		LogSnapshotJSON: logSnapshot, FactsSnapshotJSON: factsSnapshot, TelemetryVersion: input.TelemetryVersion,
		PhysicallyDispatched: input.PhysicallyDispatched, CreatedAt: requestedAt,
	}, facts, nil
}

func persistGatewayRequestDispatchTerminalHandoffTx(tx *gorm.DB, audit *GatewayRequestDispatchAudit, handoff *GatewayRequestDispatchTerminalHandoff, facts GatewayRequestDispatchFinalizationFacts, physicallyDispatched, knownUnsent bool) error {
	if audit == nil || handoff == nil || audit.ID != handoff.DispatchID {
		return ErrGatewayRequestDispatchConflict
	}
	if audit.TerminalAt != nil {
		if audit.Stage != GatewayRequestDispatchStageCompleted && audit.Stage != GatewayRequestDispatchStageFinalized {
			return ErrGatewayRequestDispatchConflict
		}
		return nil
	}
	if audit.RequestLogID != nil {
		return ErrGatewayRequestDispatchConflict
	}
	if audit.TelemetryVersion != handoff.TelemetryVersion || audit.RecoveryRequestedAt != nil {
		return ErrGatewayRequestDispatchConflict
	}
	if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && physicallyDispatched &&
		audit.PhysicallyDispatched && audit.TransportState == GatewayRequestDispatchTransportStateArmed {
		confirmation := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND transport_fence_version = ? AND transport_state = ? AND transport_confirmation_version = 1 AND transport_return_confirmed = FALSE AND transport_return_confirmed_at IS NULL AND physically_dispatched = TRUE AND terminal_at IS NULL AND request_log_id IS NULL AND recovery_requested_at IS NULL",
				audit.ID, GatewayRequestDispatchTransportFenceVersion, GatewayRequestDispatchTransportStateArmed).
			Updates(map[string]any{
				"transport_state":               GatewayRequestDispatchTransportStateConfirmed,
				"transport_return_confirmed":    true,
				"transport_return_confirmed_at": handoff.FinishedAt,
			})
		if confirmation.Error != nil {
			return confirmation.Error
		}
		if confirmation.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		audit.TransportState = GatewayRequestDispatchTransportStateConfirmed
		audit.TransportReturnConfirmed = true
		confirmedAt := handoff.FinishedAt
		audit.TransportReturnConfirmedAt = &confirmedAt
	}
	updates := map[string]any{"recovery_requested_at": handoff.RequestedAt, "recovery_terminal_kind": handoff.TerminalKind}
	if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion {
		switch {
		case knownUnsent && !physicallyDispatched && (audit.TransportState == GatewayRequestDispatchTransportStatePrepared || audit.TransportState == GatewayRequestDispatchTransportStateArmed):
			updates["transport_state"] = GatewayRequestDispatchTransportStateNotDispatched
			updates["physically_dispatched"] = false
			updates["billing_committed"] = false
		case physicallyDispatched && audit.PhysicallyDispatched && audit.TransportState == GatewayRequestDispatchTransportStateConfirmed:
		case !physicallyDispatched && audit.TransportState == GatewayRequestDispatchTransportStateNotDispatched:
		default:
			return ErrGatewayRequestDispatchConflict
		}
	} else if physicallyDispatched != audit.PhysicallyDispatched {
		return ErrGatewayRequestDispatchConflict
	}
	if facts.ProviderRequestID != "" {
		if audit.ProviderRequestID != "" && audit.ProviderRequestID != facts.ProviderRequestID {
			return ErrGatewayRequestDispatchConflict
		}
		updates["provider_request_id"] = facts.ProviderRequestID
	}
	if !facts.ResponseObservedAt.IsZero() && audit.ResponseObservedAt == nil {
		updates["response_observed_at"] = facts.ResponseObservedAt
		if audit.Stage == GatewayRequestDispatchStageDispatched {
			updates["stage"] = GatewayRequestDispatchStageResponded
		}
	}
	if facts.Accepted {
		if audit.AcceptedAt != nil && (audit.AcceptedEventType != facts.AcceptedEventType || audit.AcceptedObjectID != facts.AcceptedObjectID) {
			return ErrGatewayRequestDispatchConflict
		}
		updates["stage"] = GatewayRequestDispatchStageAccepted
		updates["accepted_at"] = facts.AcceptedAt
		updates["accepted_event_type"] = facts.AcceptedEventType
		updates["accepted_object_id"] = facts.AcceptedObjectID
	}
	result := tx.Model(&GatewayRequestDispatchAudit{}).Where("id = ? AND terminal_at IS NULL AND request_log_id IS NULL AND recovery_requested_at IS NULL", audit.ID).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrGatewayRequestDispatchConflict
	}
	result = tx.Clauses(clause.OnConflict{DoNothing: true}).Create(handoff)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrGatewayRequestDispatchConflict
	}
	return nil
}

func (g *Gateway) ListRequestDispatchTerminalHandoffs(limit int) ([]GatewayRequestDispatchTerminalHandoff, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	var handoffs []GatewayRequestDispatchTerminalHandoff
	if err := g.db.
		Joins("JOIN gateway_request_dispatch_audits ON gateway_request_dispatch_audits.id = gateway_request_dispatch_terminal_handoffs.dispatch_id").
		Where("gateway_request_dispatch_audits.transport_state <> ?", GatewayRequestDispatchTransportStateManualReview).
		Order("gateway_request_dispatch_terminal_handoffs.requested_at ASC, gateway_request_dispatch_terminal_handoffs.dispatch_id ASC").
		Limit(limit).
		Find(&handoffs).Error; err != nil {
		return nil, err
	}
	return handoffs, nil
}

func DecodeGatewayRequestDispatchTerminalHandoff(handoff GatewayRequestDispatchTerminalHandoff) (GatewayRequestDispatchTerminalHandoff, error) {
	if handoff.Version != gatewayRequestDispatchTerminalHandoffVersion || handoff.DispatchID == 0 || strings.TrimSpace(handoff.TerminalKind) == "" {
		return handoff, errors.New("request dispatch terminal handoff is invalid")
	}
	log, err := decodeGatewayRequestDispatchTerminalLog(handoff.LogSnapshotJSON)
	if err != nil {
		return handoff, err
	}
	facts, err := decodeGatewayRequestDispatchTerminalFacts(handoff.FactsSnapshotJSON)
	if err != nil {
		return handoff, err
	}
	handoff.Log = log
	handoff.Facts = facts
	return handoff, nil
}

func gatewayRequestDispatchTerminalHandoffExistsTx(tx *gorm.DB, dispatchID uint64) (bool, error) {
	var count int64
	if err := tx.Model(&GatewayRequestDispatchTerminalHandoff{}).Where("dispatch_id = ?", dispatchID).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// ResolveUnconfirmedRequestDispatchTerminalHandoff consumes only the transport
// states that cannot be automatically billed or finalized. An armed row from a
// lost owner is quarantined for manual review; a prepared/not-dispatched row is
// closed as definitively nonphysical. Confirmed and legacy rows continue to the
// exact terminal finalizer.
func (g *Gateway) ResolveUnconfirmedRequestDispatchTerminalHandoff(dispatchID uint64, terminalKind string, legacyBefore, at time.Time) (resolved bool, err error) {
	if g == nil || g.db == nil {
		return false, errors.New("gateway store is unavailable")
	}
	terminalKind, err = validateGatewayRequestDispatchText("request dispatch recovery terminal kind", terminalKind, 64, true)
	if err != nil {
		return false, err
	}
	legacyBefore = normalizeGatewayRequestDispatchTime(legacyBefore)
	at = normalizeGatewayRequestDispatchTime(at)
	err = g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, dispatchID)
		if err != nil {
			return err
		}
		exists, err := gatewayRequestDispatchTerminalHandoffExistsTx(tx, dispatchID)
		if err != nil {
			return err
		}
		if !exists {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.TerminalAt != nil {
			resolved = true
			return nil
		}
		if audit.TransportFenceVersion != GatewayRequestDispatchTransportFenceVersion || audit.TransportState == GatewayRequestDispatchTransportStateConfirmed {
			return nil
		}
		if err := requireRecoverableGatewayRequestDispatchOwnerTx(tx, audit, legacyBefore, at); err != nil {
			return err
		}
		if audit.AcceptedAt != nil || audit.ProviderRequestID != "" || audit.RequestLogID != nil {
			return ErrGatewayRequestDispatchConflict
		}
		updates := map[string]any{
			"stage":                 GatewayRequestDispatchStageCompleted,
			"terminal_at":           at,
			"terminal_kind":         terminalKind,
			"billing_committed":     false,
			"physically_dispatched": false,
		}
		switch audit.TransportState {
		case GatewayRequestDispatchTransportStateArmed:
			updates["transport_state"] = GatewayRequestDispatchTransportStateManualReview
		case GatewayRequestDispatchTransportStatePrepared, GatewayRequestDispatchTransportStateNotDispatched:
			updates["transport_state"] = GatewayRequestDispatchTransportStateNotDispatched
		case GatewayRequestDispatchTransportStateManualReview:
			updates["transport_state"] = GatewayRequestDispatchTransportStateManualReview
		default:
			return ErrGatewayRequestDispatchConflict
		}
		result := tx.Model(&GatewayRequestDispatchAudit{}).
			Where("id = ? AND terminal_at IS NULL AND request_log_id IS NULL", audit.ID).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchConflict
		}
		resolved = true
		return nil
	})
	return resolved, err
}

// DeleteRequestDispatchTerminalHandoff removes a retry record only after the
// linked dispatch is durably terminal. A failed delete is harmless: replaying
// Finalize loads the already-linked request log and remains exactly once.
func (g *Gateway) DeleteRequestDispatchTerminalHandoff(dispatchID uint64) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		audit, err := lockGatewayRequestDispatchAuditTx(tx, dispatchID)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				var remaining int64
				if countErr := tx.Model(&GatewayRequestDispatchTerminalHandoff{}).Where("dispatch_id = ?", dispatchID).Count(&remaining).Error; countErr != nil {
					return countErr
				}
				if remaining == 0 {
					return nil
				}
			}
			return err
		}
		if audit.TerminalAt == nil || (audit.Stage != GatewayRequestDispatchStageCompleted && audit.Stage != GatewayRequestDispatchStageFinalized) {
			return ErrGatewayRequestDispatchConflict
		}
		if audit.TransportFenceVersion == GatewayRequestDispatchTransportFenceVersion && audit.TransportState == GatewayRequestDispatchTransportStateManualReview {
			return ErrGatewayRequestDispatchManualReviewRequired
		}
		return tx.Where("dispatch_id = ?", dispatchID).Delete(&GatewayRequestDispatchTerminalHandoff{}).Error
	})
}
