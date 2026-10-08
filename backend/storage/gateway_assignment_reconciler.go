package storage

import (
	"context"
	"errors"
	"sync"
	"time"
)

type GatewayAutomaticAssignmentReconcileResult struct {
	PresenceProcessed int  `json:"presence_processed"`
	FactsProcessed    int  `json:"facts_processed"`
	PreviewsPersisted int  `json:"previews_persisted"`
	Skipped           bool `json:"skipped"`
}

type GatewayAutomaticAssignmentReconcileObserver func(GatewayAutomaticAssignmentReconcileResult, error)

type GatewayAutomaticAssignmentReconciler struct {
	gateway     *Gateway
	interval    time.Duration
	limit       int
	mu          sync.Mutex
	reconcileFn func(context.Context, int) (GatewayAutomaticAssignmentReconcileResult, error)
}

func NewGatewayAutomaticAssignmentReconciler(gateway *Gateway, interval time.Duration, limit int) *GatewayAutomaticAssignmentReconciler {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if limit <= 0 {
		limit = 500
	}
	return &GatewayAutomaticAssignmentReconciler{gateway: gateway, interval: interval, limit: limit}
}

func (r *GatewayAutomaticAssignmentReconciler) Run(ctx context.Context, observer GatewayAutomaticAssignmentReconcileObserver) {
	if ctx == nil {
		ctx = context.Background()
	}
	run := func() {
		result, err := r.ReconcileOnce(ctx)
		if observer != nil {
			observer(result, err)
		}
	}
	run()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

func (r *GatewayAutomaticAssignmentReconciler) ReconcileOnce(ctx context.Context) (GatewayAutomaticAssignmentReconcileResult, error) {
	if !r.mu.TryLock() {
		return GatewayAutomaticAssignmentReconcileResult{Skipped: true}, nil
	}
	defer r.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return GatewayAutomaticAssignmentReconcileResult{}, err
	}
	if r.reconcileFn != nil {
		return r.reconcileFn(ctx, r.limit)
	}
	if r.gateway == nil || r.gateway.db == nil {
		return GatewayAutomaticAssignmentReconcileResult{}, errors.New("automatic assignment reconciler requires a gateway store")
	}

	scoped := r.gateway.WithContext(ctx)
	result := GatewayAutomaticAssignmentReconcileResult{}

	processed, err := scoped.ReconcileAutomaticAssignmentPresence(r.limit)
	result.PresenceProcessed = processed
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	processed, err = scoped.BackfillAutomaticAssignmentFacts(r.limit)
	result.FactsProcessed = processed
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	previews, err := scoped.PreviewAutomaticAssignments(nil, true)
	result.PreviewsPersisted = len(previews)
	return result, err
}
