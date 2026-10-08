package gateway

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

var errPairedCommitUnknownHandoffRejected = errors.New("paired commit-unknown handoff rejected")

type pairedCommitUnknownHandoffFailurePool struct {
	gorm.ConnPool
	armed                 atomic.Bool
	commitUnknownInjected atomic.Bool
	handoffRejected       atomic.Bool
}

func (p *pairedCommitUnknownHandoffFailurePool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	if p.commitUnknownInjected.Load() {
		p.handoffRejected.Store(true)
		return nil, errPairedCommitUnknownHandoffRejected
	}
	beginner, ok := p.ConnPool.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	})
	if !ok {
		return nil, gorm.ErrInvalidTransaction
	}
	tx, err := beginner.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &pairedCommitUnknownHandoffFailureTx{Tx: tx, pool: p}, nil
}

type pairedCommitUnknownHandoffFailureTx struct {
	*sql.Tx
	pool *pairedCommitUnknownHandoffFailurePool
}

func (tx *pairedCommitUnknownHandoffFailureTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.pool.armed.Load() && tx.pool.commitUnknownInjected.CompareAndSwap(false, true) {
		return driver.ErrBadConn
	}
	return nil
}

func TestPairedPhysicalBillingCommitUnknownHandoffFailureDoesNotRestartOwner(t *testing.T) {
	db := testdb.Open(t)
	faultDB := db.Session(&gorm.Session{NewDB: true})
	pool := &pairedCommitUnknownHandoffFailurePool{ConnPool: faultDB.Statement.ConnPool}
	faultDB.Statement.ConnPool = pool
	store := storage.NewGateway(faultDB)
	service := &Service{Store: store}
	if err := service.StartRequestDispatchOwner(); err != nil {
		t.Fatalf("start request dispatch owner: %v", err)
	}

	now := time.Now().UTC()
	snapshot := postgresDispatchRecoveryCurrentTelemetrySnapshot(t, now, 947, "/v1/images/generations")
	snapshot.PriceOperation = storage.GatewayImageOperationGeneration
	snapshot.PriceProtocol = storage.GatewayImageProtocolImages
	snapshot.UsageSchema = storage.GatewayPricingUsageSchemaOperationV1
	snapshot.UsageSchemaVersion = 1
	snapshot.UsageOperationPresent = true
	operationUnits := int64(1)
	snapshot.OperationUnits = &operationUnits
	audit, err := service.BeginRequestDispatch(snapshot, now)
	if err != nil {
		t.Fatalf("begin image request dispatch: %v", err)
	}

	bound, release, err := service.BindRequestDispatchOwnerContext(context.Background())
	if err != nil {
		t.Fatalf("bind request dispatch owner: %v", err)
	}
	defer release()
	service.requestDispatchOwnerMu.RLock()
	originalOwner := service.requestDispatchOwner
	service.requestDispatchOwnerMu.RUnlock()

	pool.armed.Store(true)
	err = service.MarkRequestDispatchPhysicallyDispatchedAndCommitBilling(audit.ID, now.Add(time.Second))
	if !errors.Is(err, storage.ErrGatewayRequestDispatchCommitUnknown) ||
		!errors.Is(err, ErrRequestDispatchKnownUnsentHandoffFailed) ||
		!errors.Is(err, errPairedCommitUnknownHandoffRejected) {
		t.Fatalf("paired marker error = %v, want commit-unknown plus typed durable handoff failure", err)
	}
	if !pool.commitUnknownInjected.Load() || !pool.handoffRejected.Load() {
		t.Fatalf("fault injection commit_unknown=%t handoff_rejected=%t, want true/true", pool.commitUnknownInjected.Load(), pool.handoffRejected.Load())
	}
	select {
	case <-bound.Done():
	case <-time.After(time.Second):
		t.Fatal("paired commit-unknown did not fence the active owner")
	}
	if !IsRequestDispatchOwnerLeaseLost(context.Cause(bound)) {
		t.Fatalf("owner cancellation cause = %v, want owner lease loss", context.Cause(bound))
	}

	service.requestDispatchOwnerRecoveryMu.Lock()
	recovering := service.requestDispatchOwnerRecovering
	service.requestDispatchOwnerRecoveryMu.Unlock()
	service.requestDispatchOwnerMu.RLock()
	currentOwner := service.requestDispatchOwner
	service.requestDispatchOwnerMu.RUnlock()
	if recovering || currentOwner != originalOwner {
		t.Fatalf("owner restarted without durable handoff: recovering=%t original=%p current=%p", recovering, originalOwner, currentOwner)
	}

	var persisted storage.GatewayRequestDispatchAudit
	if err := db.First(&persisted, audit.ID).Error; err != nil {
		t.Fatalf("reload ambiguous image dispatch: %v", err)
	}
	if !persisted.PhysicallyDispatched || !persisted.BillingCommitted || persisted.RecoveryRequestedAt != nil || persisted.TerminalAt != nil {
		t.Fatalf("failed handoff silently rewrote ambiguous dispatch: %+v", persisted)
	}
}
