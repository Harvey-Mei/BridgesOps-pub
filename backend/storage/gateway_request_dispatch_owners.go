package storage

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const GatewayRequestDispatchOwnerLeaseDuration = 45 * time.Second

var (
	ErrGatewayRequestDispatchOwnerFenced = errors.New("gateway request dispatch owner is fenced")
	ErrGatewayRequestDispatchOwnerActive = errors.New("gateway request dispatch owner is still active")
)

// GatewayRequestDispatchOwner is one process incarnation. InstanceID contains
// random process-local entropy only; it never contains a hostname, address, or
// other infrastructure identity. FenceToken prevents a stale incarnation from
// renewing, stopping, or finalizing work owned by another incarnation.
type GatewayRequestDispatchOwner struct {
	InstanceID     string     `gorm:"primaryKey;size:32"`
	FenceToken     uint64     `gorm:"not null"`
	StartedAt      time.Time  `gorm:"not null"`
	HeartbeatAt    time.Time  `gorm:"not null"`
	LeaseExpiresAt time.Time  `gorm:"not null;index"`
	StoppedAt      *time.Time `gorm:"index"`
	// DrainForcedAt records that this incarnation was rotated away before every
	// old-identity request had durably handed off its exact terminal outcome.
	// The audit is append-only and carries no request, usage or billing fact.
	DrainForcedAt       *time.Time `gorm:"column:drain_forced_at"`
	DrainForcedBindings int        `gorm:"column:drain_forced_bindings;not null;default:0"`
	DrainForcedHandoffs int        `gorm:"column:drain_forced_handoffs;not null;default:0"`
}

// GatewayRequestDispatchForcedDrain describes one bounded degrade of the owner
// drain gate. It only counts what was still outstanding; it never classifies,
// completes or synthesizes any dispatch outcome.
type GatewayRequestDispatchForcedDrain struct {
	ActiveBindings   int
	PendingHandoffs  int
	DrainWaitedFor   time.Duration
	DrainMaximumWait time.Duration
}

func (GatewayRequestDispatchOwner) TableName() string {
	return "gateway_request_dispatch_owners"
}

func newGatewayRequestDispatchOwnerIdentity() (string, uint64, error) {
	var entropy [24]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", 0, fmt.Errorf("generate request dispatch owner identity: %w", err)
	}
	// PostgreSQL BIGINT is signed. Keep the random token positive and non-zero.
	token := binary.BigEndian.Uint64(entropy[16:]) & uint64(^uint64(0)>>1)
	if token == 0 {
		token = 1
	}
	return hex.EncodeToString(entropy[:16]), token, nil
}

func validateGatewayRequestDispatchOwnerIdentity(instanceID string, fenceToken uint64) error {
	instanceID = strings.TrimSpace(instanceID)
	if len(instanceID) != 32 || fenceToken == 0 || fenceToken > uint64(^uint64(0)>>1) {
		return ErrGatewayRequestDispatchOwnerFenced
	}
	decoded, err := hex.DecodeString(instanceID)
	if err != nil || len(decoded) != 16 || instanceID != strings.ToLower(instanceID) {
		return ErrGatewayRequestDispatchOwnerFenced
	}
	return nil
}

func gatewayRequestDispatchOwnerLeaseDuration(now, leaseUntil time.Time) (time.Duration, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	leaseUntil = leaseUntil.UTC()
	duration := leaseUntil.Sub(now)
	if leaseUntil.IsZero() || duration <= 0 || duration > 5*time.Minute {
		return 0, errors.New("request dispatch owner lease duration must be between zero and five minutes")
	}
	return duration, nil
}

func normalizeGatewayRequestDispatchOwnerContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// RequestDispatchOwnerIdentity exposes the opaque local identity for tests and
// lifecycle diagnostics. It must not be returned through public APIs.
func (g *Gateway) RequestDispatchOwnerIdentity() (string, uint64, error) {
	if g == nil {
		return "", 0, errors.New("gateway store is unavailable")
	}
	g.requestDispatchOwnerMu.Lock()
	defer g.requestDispatchOwnerMu.Unlock()
	if g.requestDispatchOwnerInitErr != nil {
		return "", 0, g.requestDispatchOwnerInitErr
	}
	if err := validateGatewayRequestDispatchOwnerIdentity(g.requestDispatchOwnerID, g.requestDispatchOwnerFenceToken); err != nil {
		return "", 0, err
	}
	return g.requestDispatchOwnerID, g.requestDispatchOwnerFenceToken, nil
}

// RegisterRequestDispatchOwner creates this process incarnation's durable
// lease. Retrying an ambiguous insert is safe only while the exact same fence
// remains active; an expired or stopped incarnation can never be revived.
func (g *Gateway) RegisterRequestDispatchOwner(ctx context.Context, now, leaseUntil time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	leaseDuration, err := gatewayRequestDispatchOwnerLeaseDuration(now, leaseUntil)
	if err != nil {
		return err
	}
	ctx = normalizeGatewayRequestDispatchOwnerContext(ctx)
	g.requestDispatchOwnerMu.Lock()
	defer g.requestDispatchOwnerMu.Unlock()
	if g.requestDispatchOwnerInitErr != nil {
		return g.requestDispatchOwnerInitErr
	}
	instanceID := g.requestDispatchOwnerID
	fenceToken := g.requestDispatchOwnerFenceToken
	if err := validateGatewayRequestDispatchOwnerIdentity(instanceID, fenceToken); err != nil || g.requestDispatchOwnerStopped {
		return ErrGatewayRequestDispatchOwnerFenced
	}

	err = g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var databaseNow time.Time
		if err := tx.Raw("SELECT clock_timestamp()").Scan(&databaseNow).Error; err != nil {
			return err
		}
		databaseNow = databaseNow.UTC()
		databaseLeaseUntil := databaseNow.Add(leaseDuration)

		var owner GatewayRequestDispatchOwner
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&owner, "instance_id = ?", instanceID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			owner = GatewayRequestDispatchOwner{
				InstanceID:     instanceID,
				FenceToken:     fenceToken,
				StartedAt:      databaseNow,
				HeartbeatAt:    databaseNow,
				LeaseExpiresAt: databaseLeaseUntil,
			}
			return tx.Create(&owner).Error
		}
		if err != nil {
			return err
		}
		if owner.FenceToken != fenceToken || owner.StoppedAt != nil || !owner.LeaseExpiresAt.After(databaseNow) {
			return ErrGatewayRequestDispatchOwnerFenced
		}
		result := tx.Model(&GatewayRequestDispatchOwner{}).
			Where("instance_id = ? AND fence_token = ? AND stopped_at IS NULL AND lease_expires_at > clock_timestamp()", instanceID, fenceToken).
			Updates(map[string]any{
				"heartbeat_at":     gorm.Expr("clock_timestamp()"),
				"lease_expires_at": gorm.Expr("clock_timestamp() + (? * interval '1 microsecond')", leaseDuration.Microseconds()),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGatewayRequestDispatchOwnerFenced
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrGatewayRequestDispatchOwnerFenced) {
			g.requestDispatchOwnerStopped = true
		}
		return err
	}
	g.requestDispatchOwnerRegistered = true
	// Keep the process-local deadline monotonic. Absolute UTC timestamps are
	// used only for database parameters; wall-clock adjustments must not delay
	// or spuriously accelerate the local renewal check.
	g.requestDispatchOwnerLeaseUntil = time.Now().Add(leaseDuration)
	return nil
}

// RenewRequestDispatchOwner extends only an unexpired lease with the exact
// fence token. clock_timestamp() is evaluated after lock waits, so a delayed
// heartbeat cannot resurrect a lease that expired while the UPDATE waited.
func (g *Gateway) RenewRequestDispatchOwner(ctx context.Context, now, leaseUntil time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	leaseDuration, err := gatewayRequestDispatchOwnerLeaseDuration(now, leaseUntil)
	if err != nil {
		return err
	}
	ctx = normalizeGatewayRequestDispatchOwnerContext(ctx)
	g.requestDispatchOwnerMu.Lock()
	defer g.requestDispatchOwnerMu.Unlock()
	if g.requestDispatchOwnerInitErr != nil {
		return g.requestDispatchOwnerInitErr
	}
	if !g.requestDispatchOwnerRegistered || g.requestDispatchOwnerStopped {
		return ErrGatewayRequestDispatchOwnerFenced
	}
	instanceID := g.requestDispatchOwnerID
	fenceToken := g.requestDispatchOwnerFenceToken
	result := g.db.WithContext(ctx).Model(&GatewayRequestDispatchOwner{}).
		Where("instance_id = ? AND fence_token = ? AND stopped_at IS NULL AND lease_expires_at > clock_timestamp()", instanceID, fenceToken).
		Updates(map[string]any{
			"heartbeat_at":     gorm.Expr("clock_timestamp()"),
			"lease_expires_at": gorm.Expr("clock_timestamp() + (? * interval '1 microsecond')", leaseDuration.Microseconds()),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		g.requestDispatchOwnerStopped = true
		return ErrGatewayRequestDispatchOwnerFenced
	}
	g.requestDispatchOwnerLeaseUntil = time.Now().Add(leaseDuration)
	return nil
}

// StopRequestDispatchOwner permanently fences this process incarnation.
func (g *Gateway) StopRequestDispatchOwner(ctx context.Context, now time.Time) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	_ = now
	ctx = normalizeGatewayRequestDispatchOwnerContext(ctx)
	g.requestDispatchOwnerMu.Lock()
	defer g.requestDispatchOwnerMu.Unlock()
	if g.requestDispatchOwnerInitErr != nil {
		return g.requestDispatchOwnerInitErr
	}
	if !g.requestDispatchOwnerRegistered {
		return nil
	}
	if g.requestDispatchOwnerStopped {
		return nil
	}
	instanceID := g.requestDispatchOwnerID
	fenceToken := g.requestDispatchOwnerFenceToken
	result := g.db.WithContext(ctx).Model(&GatewayRequestDispatchOwner{}).
		Where("instance_id = ? AND fence_token = ? AND stopped_at IS NULL", instanceID, fenceToken).
		Update("stopped_at", gorm.Expr("clock_timestamp()"))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		g.requestDispatchOwnerStopped = true
		return ErrGatewayRequestDispatchOwnerFenced
	}
	g.requestDispatchOwnerStopped = true
	return nil
}

// RotateRequestDispatchOwner permanently stops the current local incarnation
// and installs a fresh owner in one database transaction. It is used after a
// live handler has been fenced by a durable-handoff failure: waiting for the
// old lease to expire would leave every new request in this process unable to
// obtain an owner for up to the full lease duration.
func (g *Gateway) RotateRequestDispatchOwner(ctx context.Context, now, leaseUntil time.Time) error {
	return g.rotateRequestDispatchOwner(ctx, now, leaseUntil, nil)
}

// RotateForcedDrainRequestDispatchOwner rotates after the bounded drain wait
// elapsed and stamps the abandoned incarnation with an append-only record of
// what was still outstanding. The stamp is deliberately limited to lifecycle
// counters: outstanding exact handoffs stay authorized by the immutable
// audit-bound owner identity and must keep retrying, so a bounded degrade can
// never turn an imprecise snapshot into an exact fact.
func (g *Gateway) RotateForcedDrainRequestDispatchOwner(ctx context.Context, now, leaseUntil time.Time, forced GatewayRequestDispatchForcedDrain) error {
	if forced.ActiveBindings < 0 || forced.PendingHandoffs < 0 {
		return errors.New("forced request dispatch owner drain counters are invalid")
	}
	return g.rotateRequestDispatchOwner(ctx, now, leaseUntil, &forced)
}

func (g *Gateway) rotateRequestDispatchOwner(ctx context.Context, now, leaseUntil time.Time, forced *GatewayRequestDispatchForcedDrain) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	leaseDuration, err := gatewayRequestDispatchOwnerLeaseDuration(now, leaseUntil)
	if err != nil {
		return err
	}
	ctx = normalizeGatewayRequestDispatchOwnerContext(ctx)
	newInstanceID, newFenceToken, err := newGatewayRequestDispatchOwnerIdentity()
	if err != nil {
		return err
	}

	g.requestDispatchOwnerMu.Lock()
	defer g.requestDispatchOwnerMu.Unlock()
	if g.requestDispatchOwnerInitErr != nil {
		return g.requestDispatchOwnerInitErr
	}
	oldInstanceID := g.requestDispatchOwnerID
	oldFenceToken := g.requestDispatchOwnerFenceToken
	oldRegistered := g.requestDispatchOwnerRegistered

	err = g.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var databaseNow time.Time
		if err := tx.Raw("SELECT clock_timestamp()").Scan(&databaseNow).Error; err != nil {
			return err
		}
		databaseNow = databaseNow.UTC()
		if oldRegistered {
			result := tx.Model(&GatewayRequestDispatchOwner{}).
				Where("instance_id = ? AND fence_token = ? AND stopped_at IS NULL", oldInstanceID, oldFenceToken).
				Update("stopped_at", databaseNow)
			if result.Error != nil {
				return result.Error
			}
			// A concurrently expired/fenced owner is already unusable. The fresh
			// identity is still safe because no audit can change its owner token.
			if forced != nil {
				// Stamp the degrade even when the row was already stopped, and
				// only once: the audit must survive a concurrent expiry without
				// ever being rewritten by a later forced rotation.
				result = tx.Model(&GatewayRequestDispatchOwner{}).
					Where("instance_id = ? AND fence_token = ? AND drain_forced_at IS NULL", oldInstanceID, oldFenceToken).
					Updates(map[string]any{
						"stopped_at":            gorm.Expr("COALESCE(stopped_at, ?)", databaseNow),
						"drain_forced_at":       databaseNow,
						"drain_forced_bindings": forced.ActiveBindings,
						"drain_forced_handoffs": forced.PendingHandoffs,
					})
				if result.Error != nil {
					return result.Error
				}
			}
		}
		owner := &GatewayRequestDispatchOwner{
			InstanceID:     newInstanceID,
			FenceToken:     newFenceToken,
			StartedAt:      databaseNow,
			HeartbeatAt:    databaseNow,
			LeaseExpiresAt: databaseNow.Add(leaseDuration),
		}
		return tx.Create(owner).Error
	})
	if err != nil {
		return err
	}
	g.requestDispatchOwnerID = newInstanceID
	g.requestDispatchOwnerFenceToken = newFenceToken
	g.requestDispatchOwnerRegistered = true
	g.requestDispatchOwnerStopped = false
	g.requestDispatchOwnerLeaseUntil = time.Now().Add(leaseDuration)
	return nil
}

func (g *Gateway) ensureRequestDispatchOwner() (string, uint64, error) {
	if g == nil || g.db == nil {
		return "", 0, errors.New("gateway store is unavailable")
	}
	localNow := time.Now()
	g.requestDispatchOwnerMu.Lock()
	if g.requestDispatchOwnerInitErr != nil {
		err := g.requestDispatchOwnerInitErr
		g.requestDispatchOwnerMu.Unlock()
		return "", 0, err
	}
	instanceID := g.requestDispatchOwnerID
	fenceToken := g.requestDispatchOwnerFenceToken
	registered := g.requestDispatchOwnerRegistered
	stopped := g.requestDispatchOwnerStopped
	leaseUntil := g.requestDispatchOwnerLeaseUntil
	g.requestDispatchOwnerMu.Unlock()
	if stopped {
		return "", 0, ErrGatewayRequestDispatchOwnerFenced
	}
	if !registered {
		if err := g.RegisterRequestDispatchOwner(context.Background(), localNow, localNow.Add(GatewayRequestDispatchOwnerLeaseDuration)); err != nil {
			return "", 0, err
		}
	} else if !leaseUntil.After(localNow.Add(5 * time.Second)) {
		if err := g.RenewRequestDispatchOwner(context.Background(), localNow, localNow.Add(GatewayRequestDispatchOwnerLeaseDuration)); err != nil {
			return "", 0, err
		}
	}
	return instanceID, fenceToken, nil
}

func lockGatewayRequestDispatchOwnerTx(tx *gorm.DB, instanceID string, fenceToken uint64) (*GatewayRequestDispatchOwner, time.Time, error) {
	if tx == nil || validateGatewayRequestDispatchOwnerIdentity(instanceID, fenceToken) != nil {
		return nil, time.Time{}, ErrGatewayRequestDispatchOwnerFenced
	}
	var owner GatewayRequestDispatchOwner
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&owner, "instance_id = ? AND fence_token = ?", instanceID, fenceToken).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, time.Time{}, ErrGatewayRequestDispatchOwnerFenced
		}
		return nil, time.Time{}, err
	}
	var databaseNow time.Time
	if err := tx.Raw("SELECT clock_timestamp()").Scan(&databaseNow).Error; err != nil {
		return nil, time.Time{}, err
	}
	return &owner, databaseNow.UTC(), nil
}

func requireActiveGatewayRequestDispatchOwnerTx(tx *gorm.DB, audit *GatewayRequestDispatchAudit, expectedInstanceID string, expectedFenceToken uint64, _ time.Time) error {
	if audit == nil || strings.TrimSpace(audit.OwnerInstanceID) == "" || audit.OwnerFenceToken == 0 {
		return ErrGatewayRequestDispatchOwnerFenced
	}
	if audit.OwnerInstanceID != expectedInstanceID || audit.OwnerFenceToken != expectedFenceToken {
		return ErrGatewayRequestDispatchOwnerFenced
	}
	if audit.RecoveryRequestedAt != nil {
		return ErrGatewayRequestDispatchRecoveryRequested
	}
	return requireGatewayRequestDispatchOwnerLeaseActiveTx(tx, audit, expectedInstanceID, expectedFenceToken)
}

func requireGatewayRequestDispatchOwnerLeaseActiveTx(tx *gorm.DB, audit *GatewayRequestDispatchAudit, expectedInstanceID string, expectedFenceToken uint64) error {
	if audit == nil || strings.TrimSpace(audit.OwnerInstanceID) == "" || audit.OwnerFenceToken == 0 {
		return ErrGatewayRequestDispatchOwnerFenced
	}
	if audit.OwnerInstanceID != expectedInstanceID || audit.OwnerFenceToken != expectedFenceToken {
		return ErrGatewayRequestDispatchOwnerFenced
	}
	owner, databaseNow, err := lockGatewayRequestDispatchOwnerTx(tx, audit.OwnerInstanceID, audit.OwnerFenceToken)
	if err != nil {
		return err
	}
	if owner.StoppedAt != nil || !owner.LeaseExpiresAt.After(databaseNow) {
		return ErrGatewayRequestDispatchOwnerFenced
	}
	return nil
}

func requireRecoverableGatewayRequestDispatchOwnerTx(tx *gorm.DB, audit *GatewayRequestDispatchAudit, legacyBefore, _ time.Time) error {
	if audit == nil {
		return ErrGatewayRequestDispatchOwnerActive
	}
	if audit.RecoveryRequestedAt != nil && strings.TrimSpace(audit.RecoveryTerminalKind) != "" {
		return nil
	}
	if strings.TrimSpace(audit.OwnerInstanceID) == "" || audit.OwnerFenceToken == 0 {
		if audit.LegacyOwnerless && !audit.DispatchedAt.After(legacyBefore) {
			return nil
		}
		return ErrGatewayRequestDispatchOwnerActive
	}
	owner, databaseNow, err := lockGatewayRequestDispatchOwnerTx(tx, audit.OwnerInstanceID, audit.OwnerFenceToken)
	if err != nil {
		return err
	}
	if owner.StoppedAt != nil {
		return nil
	}
	if owner.LeaseExpiresAt.After(databaseNow) {
		return ErrGatewayRequestDispatchOwnerActive
	}
	result := tx.Model(&GatewayRequestDispatchOwner{}).
		Where("instance_id = ? AND fence_token = ? AND stopped_at IS NULL AND lease_expires_at <= clock_timestamp()", owner.InstanceID, owner.FenceToken).
		Update("stopped_at", databaseNow)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrGatewayRequestDispatchOwnerActive
	}
	return nil
}
