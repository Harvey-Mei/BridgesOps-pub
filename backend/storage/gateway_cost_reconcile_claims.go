package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const gatewayRequestCostReconcileMaxClaimEpoch = uint64(1<<63 - 2)

const (
	GatewayRequestCostReconcileReasonScheduled                 = "scheduled"
	GatewayRequestCostReconcileReasonClaimed                   = "claimed"
	GatewayRequestCostReconcileReasonDispatcherSlotUnavailable = "dispatcher_slot_unavailable"
	GatewayRequestCostReconcileReasonIdentityLeaseExpired      = "identity_lease_expired"
	GatewayRequestCostReconcileReasonLookupFailed              = "lookup_failed"
	GatewayRequestCostReconcileReasonLookupNoResult            = "lookup_no_result"
	GatewayRequestCostReconcileReasonUsageScanIncomplete       = "usage_scan_incomplete"
	GatewayRequestCostReconcileReasonDuplicateUsageID          = "duplicate_usage_id"
	GatewayRequestCostReconcileReasonNegativeCost              = "negative_cost"
	GatewayRequestCostReconcileReasonEmptyCostSource           = "empty_cost_source"
	GatewayRequestCostReconcileReasonCostConversionFailed      = "cost_conversion_failed"
	GatewayRequestCostReconcileReasonCostUpdateFailed          = "cost_update_failed"
	GatewayRequestCostReconcileReasonConflictUpdateFailed      = "conflict_update_failed"
	GatewayRequestCostReconcileReasonUsageConflict             = "usage_conflict"
	GatewayRequestCostReconcileReasonUnmatchable               = "unmatchable"
	GatewayRequestCostReconcileReasonRetryLimit                = "retry_limit"
	GatewayRequestCostReconcileReasonExpired                   = "expired"
	GatewayRequestCostReconcileReasonPricingUnavailable        = "pricing_unavailable"
	GatewayRequestCostReconcileReasonIneligible                = "ineligible"
	GatewayRequestCostReconcileReasonSucceeded                 = "succeeded"
	GatewayRequestCostReconcileReasonLegacyRetry               = "legacy_retry"
	GatewayRequestCostReconcileReasonHistoricalBackfill        = "historical_backfill"
	GatewayRequestCostReconcileReasonLegacyTerminalFailure     = "legacy_terminal_failure"
	GatewayRequestCostReconcileReasonLegacyTerminalConflict    = "legacy_terminal_conflict"
)

var gatewayRequestCostReconcileReasonPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type GatewayRequestCostReconcileIdentityLease struct {
	ID             uint64     `gorm:"primaryKey" json:"-"`
	ChannelID      uint       `gorm:"not null" json:"-"`
	UpstreamKeyID  int64      `gorm:"not null" json:"-"`
	ClaimEpoch     uint64     `gorm:"not null;default:0" json:"-"`
	ClaimToken     string     `gorm:"size:64;not null;default:''" json:"-"`
	RequestLogID   *uint      `json:"-"`
	ClaimedAt      *time.Time `json:"-"`
	LeaseExpiresAt *time.Time `json:"-"`
	CreatedAt      time.Time  `json:"-"`
	UpdatedAt      time.Time  `json:"-"`
}

func (GatewayRequestCostReconcileIdentityLease) TableName() string {
	return "gateway_request_cost_reconcile_identity_leases"
}

type GatewayRequestCostReconcileEvent struct {
	ID              uint64                             `gorm:"primaryKey" json:"id"`
	RequestLogID    uint                               `gorm:"not null" json:"request_log_id"`
	IdentityLeaseID *uint64                            `json:"identity_lease_id,omitempty"`
	ChannelID       uint                               `gorm:"not null" json:"channel_id"`
	UpstreamKeyID   int64                              `gorm:"not null" json:"upstream_key_id"`
	ClaimEpoch      uint64                             `gorm:"not null;default:0" json:"claim_epoch"`
	ClaimToken      string                             `gorm:"size:64;not null;default:''" json:"-"`
	EventKind       string                             `gorm:"size:16;not null" json:"event_kind"`
	StateBefore     GatewayRequestCostReconcileState   `gorm:"size:24;not null" json:"state_before"`
	StateAfter      GatewayRequestCostReconcileState   `gorm:"size:24;not null" json:"state_after"`
	Outcome         GatewayRequestCostReconcileOutcome `gorm:"size:32;not null" json:"outcome"`
	ReasonCode      string                             `gorm:"size:64;not null" json:"reason_code"`
	ErrorSummary    string                             `gorm:"type:text;not null" json:"error_summary,omitempty"`
	RetryAt         *time.Time                         `json:"retry_at,omitempty"`
	LeaseExpiresAt  *time.Time                         `json:"lease_expires_at,omitempty"`
	RecordedAt      time.Time                          `gorm:"not null" json:"recorded_at"`
}

func (GatewayRequestCostReconcileEvent) TableName() string {
	return "gateway_request_cost_reconcile_events"
}

type GatewayRequestCostReconcileFence struct {
	RequestLogID    uint
	IdentityLeaseID uint64
	ChannelID       uint
	UpstreamKeyID   int64
	ClaimEpoch      uint64
	ClaimToken      string
	LeaseExpiresAt  time.Time
}

func (f GatewayRequestCostReconcileFence) valid() bool {
	return f.RequestLogID > 0 && f.IdentityLeaseID > 0 && f.ChannelID > 0 &&
		f.UpstreamKeyID >= 0 && f.ClaimEpoch > 0 &&
		validGatewayRequestCostReconcileToken(f.ClaimToken) && !f.LeaseExpiresAt.IsZero()
}

func gatewayRequestCostReconcileFenceLease(fence *GatewayRequestCostReconcileFence) *time.Time {
	if fence == nil || fence.LeaseExpiresAt.IsZero() {
		return nil
	}
	value := fence.LeaseExpiresAt.UTC()
	return &value
}

type GatewayRequestCostReconcileClaim struct {
	Log   GatewayRequestLog
	Fence GatewayRequestCostReconcileFence
}

func gatewayRequestCostReconcileBillingKey(log *GatewayRequestLog) (uint, int64) {
	if log == nil {
		return 0, 0
	}
	upstreamKeyID := log.UpstreamKeyID
	if upstreamKeyID < 0 {
		upstreamKeyID = 0
	}
	return log.ChannelID, upstreamKeyID
}

func newGatewayRequestCostReconcileToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate request cost reconcile claim token: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func validGatewayRequestCostReconcileToken(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func normalizeGatewayRequestCostReconcileReason(reason string) string {
	reason = strings.ToLower(strings.TrimSpace(reason))
	if !gatewayRequestCostReconcileReasonPattern.MatchString(reason) {
		return GatewayRequestCostReconcileReasonLegacyTerminalFailure
	}
	return reason
}

func databaseClockNowTx(tx *gorm.DB) (time.Time, error) {
	if tx == nil {
		return time.Time{}, errors.New("database transaction is unavailable")
	}
	var now time.Time
	if err := tx.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		return time.Time{}, err
	}
	return now.UTC(), nil
}

func lockGatewayRequestCostReconcileIdentityLeaseTx(tx *gorm.DB, channelID uint, upstreamKeyID int64) (*GatewayRequestCostReconcileIdentityLease, error) {
	if tx == nil || channelID == 0 || upstreamKeyID < 0 {
		return nil, errors.New("request cost reconcile billing identity is invalid")
	}
	seed := GatewayRequestCostReconcileIdentityLease{
		ChannelID:     channelID,
		UpstreamKeyID: upstreamKeyID,
	}
	if err := tx.Omit("CreatedAt", "UpdatedAt").Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "channel_id"}, {Name: "upstream_key_id"}},
		DoNothing: true,
	}).Create(&seed).Error; err != nil {
		return nil, err
	}
	var lease GatewayRequestCostReconcileIdentityLease
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND upstream_key_id = ?", channelID, upstreamKeyID).
		First(&lease).Error; err != nil {
		return nil, err
	}
	return &lease, nil
}

func requestCostReconcileClaimMatches(log *GatewayRequestLog, fence GatewayRequestCostReconcileFence) bool {
	if log == nil || !fence.valid() || log.ID != fence.RequestLogID ||
		log.CostReconcileState != GatewayRequestCostReconcileClaimed ||
		log.CostReconcileIdentityLeaseID == nil || *log.CostReconcileIdentityLeaseID != fence.IdentityLeaseID ||
		log.CostReconcileClaimEpoch != fence.ClaimEpoch || log.CostReconcileClaimToken != fence.ClaimToken ||
		log.CostReconcileLeaseExpiresAt == nil || !log.CostReconcileLeaseExpiresAt.Equal(fence.LeaseExpiresAt) {
		return false
	}
	channelID, upstreamKeyID := gatewayRequestCostReconcileBillingKey(log)
	return channelID == fence.ChannelID && upstreamKeyID == fence.UpstreamKeyID
}

func lockGatewayRequestCostReconcileFenceTx(tx *gorm.DB, fence GatewayRequestCostReconcileFence) (*GatewayRequestCostReconcileIdentityLease, time.Time, error) {
	if tx == nil || !fence.valid() {
		return nil, time.Time{}, ErrRequestCostReconcileLeaseLost
	}
	lease, err := lockGatewayRequestCostReconcileIdentityLeaseTx(tx, fence.ChannelID, fence.UpstreamKeyID)
	if err != nil {
		return nil, time.Time{}, err
	}
	databaseNow, err := databaseClockNowTx(tx)
	if err != nil {
		return nil, time.Time{}, err
	}
	if !gatewayRequestCostReconcileIdentityLeaseMatchesFence(lease, fence, databaseNow) {
		return nil, databaseNow, ErrRequestCostReconcileLeaseLost
	}
	return lease, databaseNow, nil
}

func gatewayRequestCostReconcileIdentityLeaseMatchesFence(lease *GatewayRequestCostReconcileIdentityLease, fence GatewayRequestCostReconcileFence, databaseNow time.Time) bool {
	return lease != nil && fence.valid() && lease.ID == fence.IdentityLeaseID &&
		lease.RequestLogID != nil && *lease.RequestLogID == fence.RequestLogID &&
		lease.ClaimEpoch == fence.ClaimEpoch && lease.ClaimToken == fence.ClaimToken &&
		lease.LeaseExpiresAt != nil && lease.LeaseExpiresAt.Equal(fence.LeaseExpiresAt) &&
		lease.LeaseExpiresAt.After(databaseNow)
}

func clearGatewayRequestCostReconcileClaimUpdates() map[string]any {
	return map[string]any{
		"cost_reconcile_identity_lease_id": nil,
		"cost_reconcile_claim_epoch":       0,
		"cost_reconcile_claim_token":       "",
		"cost_reconcile_claimed_at":        nil,
		"cost_reconcile_lease_expires_at":  nil,
	}
}

func mergeGatewayRequestCostReconcileUpdates(target map[string]any, source map[string]any) {
	for key, value := range source {
		target[key] = value
	}
}

func releaseGatewayRequestCostReconcileIdentityLeaseTx(tx *gorm.DB, lease *GatewayRequestCostReconcileIdentityLease, fence GatewayRequestCostReconcileFence, databaseNow time.Time) error {
	if tx == nil || lease == nil || lease.ID == 0 || !fence.valid() {
		return ErrRequestCostReconcileLeaseLost
	}
	result := tx.Model(&GatewayRequestCostReconcileIdentityLease{}).
		Where("id = ? AND request_log_id = ? AND claim_epoch = ? AND claim_token = ? AND lease_expires_at = ?",
			lease.ID, fence.RequestLogID, fence.ClaimEpoch, fence.ClaimToken, fence.LeaseExpiresAt.UTC()).
		Updates(map[string]any{
			"claim_token":      "",
			"request_log_id":   nil,
			"claimed_at":       nil,
			"lease_expires_at": nil,
			"updated_at":       databaseNow,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrRequestCostReconcileLeaseLost
	}
	return nil
}

func appendGatewayRequestCostReconcileEventTx(tx *gorm.DB, log GatewayRequestLog, fence *GatewayRequestCostReconcileFence, eventKind string, before, after GatewayRequestCostReconcileState, outcome GatewayRequestCostReconcileOutcome, reasonCode, errorSummary string, retryAt, leaseExpiresAt *time.Time) error {
	if tx == nil || log.ID == 0 || log.ChannelID == 0 {
		return errors.New("request cost reconcile event scope is invalid")
	}
	reasonCode = normalizeGatewayRequestCostReconcileReason(reasonCode)
	_, upstreamKeyID := gatewayRequestCostReconcileBillingKey(&log)
	event := GatewayRequestCostReconcileEvent{
		RequestLogID:  log.ID,
		ChannelID:     log.ChannelID,
		UpstreamKeyID: upstreamKeyID,
		EventKind:     eventKind,
		StateBefore:   before,
		StateAfter:    after,
		Outcome:       outcome,
		ReasonCode:    reasonCode,
		ErrorSummary:  strings.TrimSpace(errorSummary),
	}
	if fence != nil {
		identityLeaseID := fence.IdentityLeaseID
		event.IdentityLeaseID = &identityLeaseID
		event.ClaimEpoch = fence.ClaimEpoch
		event.ClaimToken = fence.ClaimToken
	}
	if retryAt != nil {
		value := retryAt.UTC()
		event.RetryAt = &value
	}
	if leaseExpiresAt != nil {
		value := leaseExpiresAt.UTC()
		event.LeaseExpiresAt = &value
	}
	// recorded_at intentionally uses the database default. All ordering and
	// lease decisions therefore share the database clock.
	return tx.Omit("RecordedAt").Create(&event).Error
}

// ClaimRequestCostReconcileDurable acquires both the request and its billing
// identity. The database clock is sampled after any identity-lock wait, so a
// queued instance can never manufacture a fresh lease from stale local time.
func (g *Gateway) ClaimRequestCostReconcileDurable(id uint, leaseDuration time.Duration) (*GatewayRequestCostReconcileClaim, bool, error) {
	if g == nil || g.db == nil || id == 0 || leaseDuration <= 0 {
		return nil, false, errors.New("request cost reconcile claim is invalid")
	}
	var claim GatewayRequestCostReconcileClaim
	acquired := false
	err := g.db.Transaction(func(tx *gorm.DB) error {
		// Read only enough scope to find the identity row. The scope is rechecked
		// after all identity and request locks have been acquired.
		var observed GatewayRequestLog
		if err := tx.Select("id", "channel_id", "upstream_key_id").First(&observed, id).Error; err != nil {
			return err
		}
		channelID, upstreamKeyID := gatewayRequestCostReconcileBillingKey(&observed)
		if channelID == 0 {
			return nil
		}
		lease, err := lockGatewayRequestCostReconcileIdentityLeaseTx(tx, channelID, upstreamKeyID)
		if err != nil {
			return err
		}
		databaseNow, err := databaseClockNowTx(tx)
		if err != nil {
			return err
		}
		if lease.RequestLogID != nil && lease.LeaseExpiresAt != nil && lease.LeaseExpiresAt.After(databaseNow) {
			return nil
		}

		requestIDs := []uint{id}
		if lease.RequestLogID != nil && *lease.RequestLogID != id {
			requestIDs = append(requestIDs, *lease.RequestLogID)
		}
		sort.Slice(requestIDs, func(left, right int) bool { return requestIDs[left] < requestIDs[right] })
		var lockedLogs []GatewayRequestLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id IN ?", requestIDs).Order("id ASC").Find(&lockedLogs).Error; err != nil {
			return err
		}
		var target *GatewayRequestLog
		var expired *GatewayRequestLog
		for index := range lockedLogs {
			current := &lockedLogs[index]
			if current.ID == id {
				target = current
			}
			if lease.RequestLogID != nil && current.ID == *lease.RequestLogID {
				expired = current
			}
		}
		if target == nil {
			return gorm.ErrRecordNotFound
		}
		lockedChannelID, lockedUpstreamKeyID := gatewayRequestCostReconcileBillingKey(target)
		if lockedChannelID != channelID || lockedUpstreamKeyID != upstreamKeyID {
			return ErrRequestCostReconcileLeaseLost
		}
		databaseNow, err = databaseClockNowTx(tx)
		if err != nil {
			return err
		}
		if target.CostReconcileAt == nil || target.CostReconcileAt.After(databaseNow) || !IsGatewayRequestCostReconcileEligible(target) {
			return nil
		}
		if target.CostReconcileState == GatewayRequestCostReconcileClaimed &&
			(lease.RequestLogID == nil || *lease.RequestLogID != target.ID) {
			return ErrRequestCostReconcileLeaseLost
		}

		// An expired identity may be handed to a different request. Retire the
		// old request's exact claim first, in the same transaction, so it becomes
		// due work again instead of remaining permanently stranded as claimed.
		if lease.RequestLogID != nil {
			if expired == nil || lease.LeaseExpiresAt == nil {
				return ErrRequestCostReconcileLeaseLost
			}
			expiredFence := GatewayRequestCostReconcileFence{
				RequestLogID: expired.ID, IdentityLeaseID: lease.ID,
				ChannelID: channelID, UpstreamKeyID: upstreamKeyID,
				ClaimEpoch: lease.ClaimEpoch, ClaimToken: lease.ClaimToken,
				LeaseExpiresAt: lease.LeaseExpiresAt.UTC(),
			}
			if !requestCostReconcileClaimMatches(expired, expiredFence) || lease.LeaseExpiresAt.After(databaseNow) {
				return ErrRequestCostReconcileLeaseLost
			}
			retryAt := databaseNow
			expiredUpdates := clearGatewayRequestCostReconcileClaimUpdates()
			expiredUpdates["cost_reconcile_at"] = retryAt
			expiredUpdates["cost_reconcile_state"] = GatewayRequestCostReconcileRetryScheduled
			expiredUpdates["cost_reconcile_outcome"] = GatewayRequestCostReconcileOutcomeRetry
			expiredUpdates["cost_reconcile_tries"] = gorm.Expr("cost_reconcile_tries + 1")
			expiredUpdates["cost_reconcile_error"] = "billing identity lease expired before a durable outcome"
			expiredUpdates["cost_reconcile_last_event_at"] = databaseNow
			result := tx.Model(&GatewayRequestLog{}).
				Where("id = ? AND cost_reconcile_state = ? AND cost_reconcile_identity_lease_id = ? AND cost_reconcile_claim_epoch = ? AND cost_reconcile_claim_token = ?",
					expired.ID, GatewayRequestCostReconcileClaimed, lease.ID, lease.ClaimEpoch, lease.ClaimToken).
				Updates(expiredUpdates)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrRequestCostReconcileLeaseLost
			}
			if err := appendGatewayRequestCostReconcileEventTx(tx, *expired, &expiredFence, "released", GatewayRequestCostReconcileClaimed, GatewayRequestCostReconcileRetryScheduled, GatewayRequestCostReconcileOutcomeRetry, GatewayRequestCostReconcileReasonIdentityLeaseExpired, "billing identity lease expired before a durable outcome", &retryAt, &expiredFence.LeaseExpiresAt); err != nil {
				return err
			}
			if expired.ID == target.ID {
				target.CostReconcileAt = timePointer(retryAt)
				target.CostReconcileState = GatewayRequestCostReconcileRetryScheduled
				target.CostReconcileOutcome = GatewayRequestCostReconcileOutcomeRetry
				target.CostReconcileIdentityLeaseID = nil
				target.CostReconcileClaimEpoch = 0
				target.CostReconcileClaimToken = ""
				target.CostReconcileClaimedAt = nil
				target.CostReconcileLeaseExpiresAt = nil
				target.CostReconcileTries++
				target.CostReconcileError = "billing identity lease expired before a durable outcome"
			}
		}
		if lease.ClaimEpoch >= gatewayRequestCostReconcileMaxClaimEpoch {
			return errors.New("request cost reconcile claim epoch is exhausted")
		}
		token, err := newGatewayRequestCostReconcileToken()
		if err != nil {
			return err
		}
		nextEpoch := lease.ClaimEpoch + 1
		leaseExpiresAt := databaseNow.Add(leaseDuration).UTC()
		requestLogID := target.ID
		leaseResult := tx.Model(&GatewayRequestCostReconcileIdentityLease{}).
			Where("id = ? AND claim_epoch = ?", lease.ID, lease.ClaimEpoch).
			Updates(map[string]any{
				"claim_epoch":      nextEpoch,
				"claim_token":      token,
				"request_log_id":   requestLogID,
				"claimed_at":       databaseNow,
				"lease_expires_at": leaseExpiresAt,
				"updated_at":       databaseNow,
			})
		if leaseResult.Error != nil {
			return leaseResult.Error
		}
		if leaseResult.RowsAffected != 1 {
			return ErrRequestCostReconcileLeaseLost
		}
		before := target.CostReconcileState
		updates := map[string]any{
			"cost_reconcile_at":                leaseExpiresAt,
			"cost_reconcile_state":             GatewayRequestCostReconcileClaimed,
			"cost_reconcile_outcome":           GatewayRequestCostReconcileOutcomeNone,
			"cost_reconcile_identity_lease_id": lease.ID,
			"cost_reconcile_claim_epoch":       nextEpoch,
			"cost_reconcile_claim_token":       token,
			"cost_reconcile_claimed_at":        databaseNow,
			"cost_reconcile_lease_expires_at":  leaseExpiresAt,
			"cost_reconcile_last_event_at":     databaseNow,
		}
		result := tx.Model(&GatewayRequestLog{}).
			Where("id = ? AND cost_reconcile_at IS NOT NULL AND cost_reconcile_at <= ?", target.ID, databaseNow).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrRequestCostReconcileLeaseLost
		}
		fence := GatewayRequestCostReconcileFence{
			RequestLogID: target.ID, IdentityLeaseID: lease.ID,
			ChannelID: channelID, UpstreamKeyID: upstreamKeyID,
			ClaimEpoch: nextEpoch, ClaimToken: token, LeaseExpiresAt: leaseExpiresAt,
		}
		if err := appendGatewayRequestCostReconcileEventTx(tx, *target, &fence, "claimed", before, GatewayRequestCostReconcileClaimed, GatewayRequestCostReconcileOutcomeNone, GatewayRequestCostReconcileReasonClaimed, "", nil, &leaseExpiresAt); err != nil {
			return err
		}
		target.CostReconcileAt = timePointer(leaseExpiresAt)
		target.CostReconcileState = GatewayRequestCostReconcileClaimed
		target.CostReconcileOutcome = GatewayRequestCostReconcileOutcomeNone
		target.CostReconcileIdentityLeaseID = uint64Pointer(lease.ID)
		target.CostReconcileClaimEpoch = nextEpoch
		target.CostReconcileClaimToken = token
		target.CostReconcileClaimedAt = timePointer(databaseNow)
		target.CostReconcileLeaseExpiresAt = timePointer(leaseExpiresAt)
		target.CostReconcileLastEventAt = timePointer(databaseNow)
		claim = GatewayRequestCostReconcileClaim{Log: *target, Fence: fence}
		acquired = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !acquired {
		return nil, false, nil
	}
	return &claim, true, nil
}

func (g *Gateway) markFencedRequestCostReconcileOutcome(fence GatewayRequestCostReconcileFence, state GatewayRequestCostReconcileState, outcome GatewayRequestCostReconcileOutcome, retryAt *time.Time, incrementConflicts bool, reasonCode, errorSummary string) error {
	if g == nil || g.db == nil || !fence.valid() {
		return ErrRequestCostReconcileLeaseLost
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		lease, _, err := lockGatewayRequestCostReconcileFenceTx(tx, fence)
		if err != nil {
			return err
		}
		var log GatewayRequestLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&log, fence.RequestLogID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrRequestCostReconcileLeaseLost
			}
			return err
		}
		databaseNow, err := databaseClockNowTx(tx)
		if err != nil {
			return err
		}
		if !requestCostReconcileClaimMatches(&log, fence) || !gatewayRequestCostReconcileIdentityLeaseMatchesFence(lease, fence, databaseNow) {
			return ErrRequestCostReconcileLeaseLost
		}
		updates := clearGatewayRequestCostReconcileClaimUpdates()
		updates["cost_reconcile_state"] = state
		updates["cost_reconcile_outcome"] = outcome
		updates["cost_reconcile_tries"] = gorm.Expr("cost_reconcile_tries + 1")
		updates["cost_reconcile_error"] = strings.TrimSpace(errorSummary)
		updates["cost_reconcile_last_event_at"] = databaseNow
		if retryAt == nil {
			updates["cost_reconcile_at"] = nil
		} else {
			value := retryAt.UTC()
			if retryAt.IsZero() {
				value = databaseNow
			}
			retryAt = &value
			updates["cost_reconcile_at"] = value
		}
		if incrementConflicts {
			updates["cost_reconcile_conflicts"] = gorm.Expr("cost_reconcile_conflicts + 1")
		}
		result := tx.Model(&GatewayRequestLog{}).
			Where("id = ? AND cost_reconcile_state = ? AND cost_reconcile_identity_lease_id = ? AND cost_reconcile_claim_epoch = ? AND cost_reconcile_claim_token = ?",
				fence.RequestLogID, GatewayRequestCostReconcileClaimed, fence.IdentityLeaseID, fence.ClaimEpoch, fence.ClaimToken).
			Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrRequestCostReconcileLeaseLost
		}
		if err := releaseGatewayRequestCostReconcileIdentityLeaseTx(tx, lease, fence, databaseNow); err != nil {
			return err
		}
		return appendGatewayRequestCostReconcileEventTx(tx, log, &fence, "outcome", GatewayRequestCostReconcileClaimed, state, outcome, reasonCode, errorSummary, retryAt, &fence.LeaseExpiresAt)
	})
}

func (g *Gateway) ReleaseRequestCostReconcileDurableClaim(fence GatewayRequestCostReconcileFence, reasonCode string) error {
	immediate := time.Time{}
	return g.markFencedRequestCostReconcileOutcome(fence, GatewayRequestCostReconcileRetryScheduled, GatewayRequestCostReconcileOutcomeRetry, &immediate, false, reasonCode, "")
}

func (g *Gateway) MarkClaimedRequestCostReconcileRetryDurable(fence GatewayRequestCostReconcileFence, retryAt time.Time, reasonCode, errorSummary string) error {
	return g.markFencedRequestCostReconcileOutcome(fence, GatewayRequestCostReconcileRetryScheduled, GatewayRequestCostReconcileOutcomeRetry, &retryAt, false, reasonCode, errorSummary)
}

func (g *Gateway) MarkClaimedRequestCostReconcileFailedDurable(fence GatewayRequestCostReconcileFence, reasonCode, errorSummary string) error {
	return g.markFencedRequestCostReconcileOutcome(fence, GatewayRequestCostReconcileTerminal, GatewayRequestCostReconcileOutcomeTerminalFailure, nil, false, reasonCode, errorSummary)
}

func (g *Gateway) MarkClaimedRequestCostReconcileConflictDurable(fence GatewayRequestCostReconcileFence, retryAt time.Time, maxConflicts int, reasonCode string) (bool, error) {
	if maxConflicts <= 0 {
		return false, errors.New("request cost reconcile conflict limit must be positive")
	}
	var current GatewayRequestLog
	if err := g.db.Select("cost_reconcile_conflicts").First(&current, fence.RequestLogID).Error; err != nil {
		return false, err
	}
	terminal := current.CostReconcileConflicts+1 >= maxConflicts
	if terminal {
		err := g.markFencedRequestCostReconcileOutcome(fence, GatewayRequestCostReconcileTerminal, GatewayRequestCostReconcileOutcomeTerminalConflict, nil, true, reasonCode, gatewayCostReconcileConflictError)
		return err == nil, err
	}
	err := g.markFencedRequestCostReconcileOutcome(fence, GatewayRequestCostReconcileRetryScheduled, GatewayRequestCostReconcileOutcomeConflictRetry, &retryAt, true, reasonCode, "")
	return false, err
}

func (g *Gateway) CountRequestCostReconcileEvents(requestLogID uint) (int64, error) {
	if g == nil || g.db == nil || requestLogID == 0 {
		return 0, nil
	}
	var count int64
	err := g.db.Model(&GatewayRequestCostReconcileEvent{}).Where("request_log_id = ?", requestLogID).Count(&count).Error
	return count, err
}
