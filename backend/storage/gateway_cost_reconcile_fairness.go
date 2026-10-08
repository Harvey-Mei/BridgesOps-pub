package storage

import (
	"errors"
	"time"
)

// ListFairPendingRequestCostReconciles returns the oldest due row for each
// upstream billing identity, then orders those identity heads globally by due
// time. Callers can over-fetch by their number of already-busy identities and
// filter those identities without a hot key hiding other channels behind it.
func (g *Gateway) ListFairPendingRequestCostReconciles(limit int, now time.Time) ([]GatewayRequestLog, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 20
	}
	ranked := g.db.Model(&GatewayRequestLog{}).
		Select(`gateway_request_logs.*, ROW_NUMBER() OVER (
			PARTITION BY channel_id, CASE WHEN upstream_key_id > 0 THEN upstream_key_id ELSE 0 END
			ORDER BY cost_reconcile_at ASC, id ASC
		) AS cost_reconcile_identity_rank`).
		Where("channel_id > 0 AND cost_reconcile_at IS NOT NULL AND cost_reconcile_at <= ? AND "+gatewayRequestCostReconcileEligibleSQL(""), now)
	var logs []GatewayRequestLog
	err := g.db.Table("(?) AS ranked_cost_reconciles", ranked).
		Where("cost_reconcile_identity_rank = 1").
		Order("cost_reconcile_at ASC, id ASC").
		Limit(limit).
		Find(&logs).Error
	return logs, err
}

// ListFairDueRequestCostReconciles uses the database clock and omits billing
// identities held by any live instance. The window function then exposes one
// due head per remaining identity, preventing a remote instance's slow lookup
// from occupying this instance's entire candidate page.
func (g *Gateway) ListFairDueRequestCostReconciles(limit int) ([]GatewayRequestLog, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if limit <= 0 {
		limit = 20
	}
	ranked := g.db.Table("gateway_request_logs AS logs").
		Select(`logs.*, ROW_NUMBER() OVER (
			PARTITION BY logs.channel_id, CASE WHEN logs.upstream_key_id > 0 THEN logs.upstream_key_id ELSE 0 END
			ORDER BY logs.cost_reconcile_at ASC, logs.id ASC
		) AS cost_reconcile_identity_rank`).
		Joins(`LEFT JOIN gateway_request_cost_reconcile_identity_leases AS identity_leases
			ON identity_leases.channel_id = logs.channel_id
			AND identity_leases.upstream_key_id = CASE WHEN logs.upstream_key_id > 0 THEN logs.upstream_key_id ELSE 0 END`).
		Where(`logs.channel_id > 0
			AND (
				logs.cost_reconcile_state IN ?
				OR (
					logs.cost_reconcile_state = ?
					AND identity_leases.id = logs.cost_reconcile_identity_lease_id
					AND identity_leases.request_log_id = logs.id
					AND identity_leases.claim_epoch = logs.cost_reconcile_claim_epoch
					AND identity_leases.claim_token = logs.cost_reconcile_claim_token
					AND identity_leases.lease_expires_at = logs.cost_reconcile_lease_expires_at
				)
			)
			AND logs.cost_reconcile_at IS NOT NULL
			AND logs.cost_reconcile_at <= clock_timestamp()
			AND `+gatewayRequestCostReconcileEligibleSQL("logs")+`
			AND (
				identity_leases.id IS NULL
				OR identity_leases.request_log_id IS NULL
				OR identity_leases.lease_expires_at <= clock_timestamp()
			)`, []GatewayRequestCostReconcileState{
			GatewayRequestCostReconcilePending,
			GatewayRequestCostReconcileRetryScheduled,
		}, GatewayRequestCostReconcileClaimed)
	var logs []GatewayRequestLog
	err := g.db.Table("(?) AS ranked_cost_reconciles", ranked).
		Where("cost_reconcile_identity_rank = 1").
		Order("cost_reconcile_at ASC, id ASC").
		Limit(limit).
		Find(&logs).Error
	return logs, err
}
