package storage

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Version 5 tightens this ledger to successful, real downstream data-plane
// traffic and records the exact last normal-use timestamp used by routing.
// It intentionally advances independently from the general request aggregate:
// analytics history does not need a rebuild when only the routing cache-price
// evidence changes.
const gatewayRequestGroupCacheAggregateVersion = 6

const gatewayRequestGroupCacheBackfillStateID int16 = 1

type gatewayRequestGroupCacheAggregateKey struct {
	BucketStart         time.Time
	DownstreamKeyID     uint
	ChannelID           uint
	ChannelNameSnapshot string
	GroupName           string
	ModelName           string
}

func gatewayRequestGroupCacheBackfillStateTx(tx *gorm.DB) (*GatewayRequestGroupCacheBackfillState, error) {
	if tx == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	var state GatewayRequestGroupCacheBackfillState
	result := tx.First(&state, gatewayRequestGroupCacheBackfillStateID)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		state = GatewayRequestGroupCacheBackfillState{
			ID:            gatewayRequestGroupCacheBackfillStateID,
			TargetVersion: gatewayRequestGroupCacheAggregateVersion,
		}
		if err := tx.Create(&state).Error; err != nil {
			return nil, err
		}
		return &state, nil
	}
	if result.Error != nil {
		return nil, result.Error
	}
	return &state, nil
}

func groupCacheAggregateKey(row *GatewayRequestGroupCacheAggregate) gatewayRequestGroupCacheAggregateKey {
	if row == nil {
		return gatewayRequestGroupCacheAggregateKey{}
	}
	return gatewayRequestGroupCacheAggregateKey{
		BucketStart:         row.BucketStart,
		DownstreamKeyID:     row.DownstreamKeyID,
		ChannelID:           row.ChannelID,
		ChannelNameSnapshot: strings.TrimSpace(row.ChannelNameSnapshot),
		GroupName:           strings.TrimSpace(row.GroupName),
		ModelName:           strings.TrimSpace(row.ModelName),
	}
}

func requestGroupCacheAggregateFromLog(log *GatewayRequestLog) (*GatewayRequestGroupCacheAggregate, bool) {
	if !isNormalDownstreamCachePriceRequest(log) {
		return nil, false
	}
	createdAt := log.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	} else {
		createdAt = createdAt.UTC()
	}
	promptTokens := max(log.PromptTokens, 0)
	cacheReadTokens := int64(0)
	cacheCreationTokens := int64(0)
	lastNormalUseAt := aggregateBucketStart(createdAt)
	if promptTokens > 0 {
		cacheReadTokens = max(log.CacheReadTokens, 0)
		cacheCreationTokens = max(log.CacheCreationTokens, 0)
		lastNormalUseAt = createdAt
	}
	return &GatewayRequestGroupCacheAggregate{
		BucketStart:         aggregateBucketStart(createdAt),
		DownstreamKeyID:     log.DownstreamKeyID,
		ChannelID:           log.ChannelID,
		ChannelNameSnapshot: strings.TrimSpace(log.ChannelNameSnapshot),
		GroupName:           strings.TrimSpace(log.GroupName),
		ModelName:           strings.TrimSpace(log.ModelName),
		CacheReadTokens:     cacheReadTokens,
		CacheCreationTokens: cacheCreationTokens,
		PromptTokens:        promptTokens,
		ValidRequests:       1,
		LastNormalUseAt:     lastNormalUseAt,
		Version:             gatewayRequestGroupCacheAggregateVersion,
	}, true
}

func isNormalDownstreamCachePriceRequest(log *GatewayRequestLog) bool {
	if !isGatewayRequestRecord(log) || log.DownstreamKeyID == 0 || log.ChannelID == 0 || strings.TrimSpace(log.GroupName) == "" {
		return false
	}
	if log.StatusCode < 200 || log.StatusCode >= 300 || strings.TrimSpace(log.Error) != "" {
		return false
	}
	reason := strings.ToLower(strings.TrimSpace(log.RouteReason))
	switch reason {
	case gatewayRouteReasonLatencyProbe, gatewayRouteReasonKeyTest, "model_check", "internal":
		return false
	}
	if strings.HasPrefix(reason, "internal_") {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(log.Method), "POST") {
		return false
	}
	switch strings.TrimSpace(log.Path) {
	case "/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/responses", "/v1/messages", "/v1/images/generations":
		return true
	default:
		return false
	}
}

func normalDownstreamCachePriceRequestSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	routeReasonSQL := "LOWER(TRIM(COALESCE(" + prefix + "route_reason, '')))"
	methodSQL := "UPPER(TRIM(COALESCE(" + prefix + "method, '')))"
	pathSQL := "TRIM(COALESCE(" + prefix + "path, ''))"
	return "(" + gatewayRequestRecordSQL(alias) +
		" AND " + prefix + "downstream_key_id > 0" +
		" AND " + prefix + "channel_id > 0" +
		" AND TRIM(COALESCE(" + prefix + "group_name, '')) <> ''" +
		" AND " + prefix + "status_code >= 200 AND " + prefix + "status_code < 300" +
		" AND TRIM(COALESCE(" + prefix + "error, '')) = ''" +
		" AND " + methodSQL + " = 'POST'" +
		" AND " + pathSQL + " IN ('/v1/chat/completions', '/v1/completions', '/v1/embeddings', '/v1/responses', '/v1/messages', '/v1/images/generations')" +
		" AND " + routeReasonSQL + " NOT IN ('" + gatewayRouteReasonLatencyProbe + "', '" + gatewayRouteReasonKeyTest + "', 'model_check', 'internal')" +
		" AND LEFT(" + routeReasonSQL + ", 9) <> 'internal_')"
}

func mergeGatewayRequestGroupCacheAggregate(dst, src *GatewayRequestGroupCacheAggregate) {
	if dst == nil || src == nil {
		return
	}
	dst.CacheReadTokens = saturatingAggregateInt64Add(dst.CacheReadTokens, src.CacheReadTokens)
	dst.CacheCreationTokens = saturatingAggregateInt64Add(dst.CacheCreationTokens, src.CacheCreationTokens)
	dst.PromptTokens = saturatingAggregateInt64Add(dst.PromptTokens, src.PromptTokens)
	dst.ValidRequests = saturatingAggregateInt64Add(dst.ValidRequests, src.ValidRequests)
	if src.LastNormalUseAt.After(dst.LastNormalUseAt) {
		dst.LastNormalUseAt = src.LastNormalUseAt
	}
}

func lockGatewayRequestGroupCacheBackfillStateTx(tx *gorm.DB) (*GatewayRequestGroupCacheBackfillState, error) {
	if tx == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	var state GatewayRequestGroupCacheBackfillState
	result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, gatewayRequestGroupCacheBackfillStateID)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		state = GatewayRequestGroupCacheBackfillState{
			ID:            gatewayRequestGroupCacheBackfillStateID,
			TargetVersion: gatewayRequestGroupCacheAggregateVersion,
		}
		if err := tx.Create(&state).Error; err != nil {
			return nil, err
		}
		return &state, nil
	}
	if result.Error != nil {
		return nil, result.Error
	}
	return &state, nil
}

func advanceGatewayRequestGroupCacheBackfillStateTx(tx *gorm.DB, state *GatewayRequestGroupCacheBackfillState, requestID uint) error {
	if tx == nil || state == nil {
		return errors.New("gateway store is unavailable")
	}
	if uint64(requestID) > state.LastRequestID {
		state.LastRequestID = uint64(requestID)
	}
	state.UpdatedAt = time.Now().UTC()
	return tx.Model(&GatewayRequestGroupCacheBackfillState{}).Where("id = ?", state.ID).Updates(map[string]any{
		"last_request_id": state.LastRequestID,
		"target_version":  state.TargetVersion,
		"completed":       state.Completed,
		"updated_at":      state.UpdatedAt,
	}).Error
}

func completeGatewayRequestGroupCacheBackfillStateTx(tx *gorm.DB, state *GatewayRequestGroupCacheBackfillState) error {
	if tx == nil || state == nil {
		return errors.New("gateway store is unavailable")
	}
	state.Completed = true
	return advanceGatewayRequestGroupCacheBackfillStateTx(tx, state, 0)
}

func markGatewayRequestGroupCacheLogsProcessedTx(tx *gorm.DB, ids []uint) error {
	if tx == nil || len(ids) == 0 {
		return nil
	}
	result := tx.Model(&GatewayRequestLog{}).
		Where("id IN ? AND group_cache_aggregate_version < ?", ids, gatewayRequestGroupCacheAggregateVersion).
		Update("group_cache_aggregate_version", gatewayRequestGroupCacheAggregateVersion)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(ids)) {
		return errGatewayRequestAggregateClaimConflict
	}
	return nil
}

// ensureGatewayRequestGroupCacheLogsProcessedTx is idempotent for a live
// reconciliation. The row may already carry the current marker when only its
// cost/token fields changed, so unlike the backfill claim it must allow a
// zero-row update. Future markers still fail closed instead of being silently
// downgraded.
func ensureGatewayRequestGroupCacheLogsProcessedTx(tx *gorm.DB, ids []uint) error {
	if tx == nil || len(ids) == 0 {
		return nil
	}
	var future int64
	if err := tx.Model(&GatewayRequestLog{}).
		Where("id IN ? AND group_cache_aggregate_version > ?", ids, gatewayRequestGroupCacheAggregateVersion).
		Count(&future).Error; err != nil {
		return err
	}
	if future > 0 {
		return errGatewayRequestGroupCacheAggregateVersionConflict
	}
	return tx.Model(&GatewayRequestLog{}).
		Where("id IN ? AND group_cache_aggregate_version < ?", ids, gatewayRequestGroupCacheAggregateVersion).
		Update("group_cache_aggregate_version", gatewayRequestGroupCacheAggregateVersion).Error
}

func deferGatewayRequestGroupCacheLogToBackfillTx(tx *gorm.DB, log *GatewayRequestLog) error {
	if tx == nil || log == nil || log.ID == 0 {
		return nil
	}
	if log.GroupCacheAggregateVersion > gatewayRequestGroupCacheAggregateVersion {
		return errGatewayRequestGroupCacheAggregateVersionConflict
	}
	if log.GroupCacheAggregateVersion < gatewayRequestGroupCacheAggregateVersion {
		return nil
	}
	if row, ok := requestGroupCacheAggregateFromLog(log); ok {
		// The old values were already included in the ledger. Remove that
		// contribution before the marker scan adds the updated raw row.
		if err := subtractGatewayRequestGroupCacheAggregateTx(tx, row, log.ID); err != nil {
			return err
		}
	}
	return tx.Model(&GatewayRequestLog{}).
		Where("id = ? AND group_cache_aggregate_version = ?", log.ID, gatewayRequestGroupCacheAggregateVersion).
		Update("group_cache_aggregate_version", 0).Error
}

// resetGatewayRequestGroupCacheBackfillTx clears the independent ledger before
// a full rebuild. Per-log markers are reset together so the next pass can
// reconstruct rows whose keys may have changed (for example after a channel
// rename).
func resetGatewayRequestGroupCacheBackfillTx(tx *gorm.DB) error {
	if tx == nil {
		return errors.New("gateway store is unavailable")
	}
	if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&GatewayRequestGroupCacheAggregate{}).Error; err != nil {
		return err
	}
	if err := tx.Model(&GatewayRequestLog{}).
		Where("group_cache_aggregate_version <= ?", gatewayRequestGroupCacheAggregateVersion).
		Update("group_cache_aggregate_version", 0).Error; err != nil {
		return err
	}
	state, err := lockGatewayRequestGroupCacheBackfillStateTx(tx)
	if err != nil {
		return err
	}
	state.LastRequestID = 0
	state.Completed = false
	state.TargetVersion = gatewayRequestGroupCacheAggregateVersion
	return advanceGatewayRequestGroupCacheBackfillStateTx(tx, state, 0)
}

func upsertGatewayRequestGroupCacheAggregateTx(tx *gorm.DB, row *GatewayRequestGroupCacheAggregate) error {
	if tx == nil || row == nil || row.ValidRequests <= 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "bucket_start"}, {Name: "downstream_key_id"}, {Name: "channel_id"},
			{Name: "channel_name_snapshot"}, {Name: "group_name"}, {Name: "model_name"},
		},
		DoUpdates: clause.Assignments(map[string]any{
			"cache_read_tokens":     groupCacheValueUpdate("cache_read_tokens", row.CacheReadTokens),
			"cache_creation_tokens": groupCacheValueUpdate("cache_creation_tokens", row.CacheCreationTokens),
			"prompt_tokens":         groupCacheValueUpdate("prompt_tokens", row.PromptTokens),
			"valid_requests":        groupCacheValueUpdate("valid_requests", row.ValidRequests),
			"last_normal_use_at":    groupCacheLastNormalUseUpdate(row.LastNormalUseAt),
			"version":               groupCacheVersionUpdate(),
		}),
	}).Create(row).Error
}

func groupCacheCurrentColumn(name string) clause.Column {
	return clause.Column{Table: clause.CurrentTable, Name: name}
}

func groupCacheValueUpdate(column string, value int64) clause.Expr {
	value = max(value, 0)
	return gorm.Expr(
		"CASE WHEN ? < ? THEN ? WHEN ? > ? THEN ? ELSE CAST(LEAST(?::numeric + ?::numeric, ?::numeric) AS bigint) END",
		groupCacheCurrentColumn("version"), gatewayRequestGroupCacheAggregateVersion, value,
		groupCacheCurrentColumn("version"), gatewayRequestGroupCacheAggregateVersion, groupCacheCurrentColumn(column),
		groupCacheCurrentColumn(column), value, int64(math.MaxInt64),
	)
}

func groupCacheVersionUpdate() clause.Expr {
	return gorm.Expr(
		"CASE WHEN ? > ? THEN ? ELSE ? END",
		groupCacheCurrentColumn("version"), gatewayRequestGroupCacheAggregateVersion,
		groupCacheCurrentColumn("version"), gatewayRequestGroupCacheAggregateVersion,
	)
}

func groupCacheLastNormalUseUpdate(value time.Time) clause.Expr {
	return gorm.Expr(
		"CASE WHEN ? < ? THEN ? WHEN ? > ? THEN ? ELSE GREATEST(?, ?) END",
		groupCacheCurrentColumn("version"), gatewayRequestGroupCacheAggregateVersion, value,
		groupCacheCurrentColumn("version"), gatewayRequestGroupCacheAggregateVersion, groupCacheCurrentColumn("last_normal_use_at"),
		groupCacheCurrentColumn("last_normal_use_at"), value,
	)
}

func subtractGatewayRequestGroupCacheAggregateTx(tx *gorm.DB, row *GatewayRequestGroupCacheAggregate, excludedLogID uint) error {
	if tx == nil || row == nil || row.ValidRequests <= 0 {
		return nil
	}
	key := groupCacheAggregateKey(row)
	query := tx.Model(&GatewayRequestGroupCacheAggregate{}).Where(
		"bucket_start = ? AND downstream_key_id = ? AND channel_id = ? AND channel_name_snapshot = ? AND group_name = ? AND model_name = ? AND version = ?",
		key.BucketStart, key.DownstreamKeyID, key.ChannelID, key.ChannelNameSnapshot, key.GroupName, key.ModelName,
		gatewayRequestGroupCacheAggregateVersion,
	)
	if err := query.Updates(map[string]any{
		"cache_read_tokens":     gorm.Expr("cache_read_tokens - ?", row.CacheReadTokens),
		"cache_creation_tokens": gorm.Expr("cache_creation_tokens - ?", row.CacheCreationTokens),
		"prompt_tokens":         gorm.Expr("prompt_tokens - ?", row.PromptTokens),
		"valid_requests":        gorm.Expr("valid_requests - ?", row.ValidRequests),
	}).Error; err != nil {
		return err
	}
	if err := tx.Where(
		"bucket_start = ? AND downstream_key_id = ? AND channel_id = ? AND channel_name_snapshot = ? AND group_name = ? AND model_name = ? AND version = ? AND valid_requests <= 0",
		key.BucketStart, key.DownstreamKeyID, key.ChannelID, key.ChannelNameSnapshot, key.GroupName, key.ModelName,
		gatewayRequestGroupCacheAggregateVersion,
	).Delete(&GatewayRequestGroupCacheAggregate{}).Error; err != nil {
		return err
	}

	var remaining GatewayRequestGroupCacheAggregate
	err := tx.Select("bucket_start").Where(
		"bucket_start = ? AND downstream_key_id = ? AND channel_id = ? AND channel_name_snapshot = ? AND group_name = ? AND model_name = ? AND version = ?",
		key.BucketStart, key.DownstreamKeyID, key.ChannelID, key.ChannelNameSnapshot, key.GroupName, key.ModelName,
		gatewayRequestGroupCacheAggregateVersion,
	).First(&remaining).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	// Reconciliation can remove the newest request from an otherwise non-empty
	// bucket. Recompute the timestamp from the remaining current-version logs;
	// retaining the removed request's time would postpone expiry by up to one
	// aggregate interval.
	type latestUse struct {
		At *time.Time `gorm:"column:last_normal_use_at"`
	}
	var latest latestUse
	query = tx.Model(&GatewayRequestLog{}).
		Select("MAX(created_at) AS last_normal_use_at").
		Where("id <> ?", excludedLogID).
		Where(normalDownstreamCachePriceRequestSQL("")).
		Where("group_cache_aggregate_version = ?", gatewayRequestGroupCacheAggregateVersion).
		Where("created_at >= ? AND created_at < ?", key.BucketStart, key.BucketStart.Add(gatewayRequestAggregateBucket)).
		Where("downstream_key_id = ? AND channel_id = ? AND channel_name_snapshot = ? AND group_name = ? AND model_name = ?",
			key.DownstreamKeyID, key.ChannelID, key.ChannelNameSnapshot, key.GroupName, key.ModelName).
		Where("prompt_tokens > 0")
	if err := query.Scan(&latest).Error; err != nil {
		return err
	}
	lastNormalUseAt := key.BucketStart
	if latest.At != nil && !latest.At.IsZero() {
		lastNormalUseAt = latest.At.UTC()
	}
	return tx.Model(&GatewayRequestGroupCacheAggregate{}).Where(
		"bucket_start = ? AND downstream_key_id = ? AND channel_id = ? AND channel_name_snapshot = ? AND group_name = ? AND model_name = ? AND version = ?",
		key.BucketStart, key.DownstreamKeyID, key.ChannelID, key.ChannelNameSnapshot, key.GroupName, key.ModelName,
		gatewayRequestGroupCacheAggregateVersion,
	).UpdateColumn("last_normal_use_at", lastNormalUseAt).Error
}

func replaceGatewayRequestGroupCacheAggregateTx(tx *gorm.DB, before, after *GatewayRequestLog) error {
	if before == nil {
		return nil
	}
	if before.AggregateVersion < gatewayRequestAggregateVersion {
		// The main aggregate backfill owns logs that were not yet current.  It
		// will fold the updated raw row exactly once after this transaction.
		return deferGatewayRequestGroupCacheLogToBackfillTx(tx, before)
	}
	if before.AggregateVersion > gatewayRequestAggregateVersion {
		return errGatewayRequestAggregateVersionConflict
	}
	// Completed is a read-mostly state. Avoid taking the singleton row lock on
	// every request; only the incomplete path joins the backfill's lock order.
	state, err := gatewayRequestGroupCacheBackfillStateTx(tx)
	if err != nil {
		return err
	}
	if state.TargetVersion != gatewayRequestGroupCacheAggregateVersion || !state.Completed {
		state, err = lockGatewayRequestGroupCacheBackfillStateTx(tx)
		if err != nil {
			return err
		}
	}
	if state.TargetVersion != gatewayRequestGroupCacheAggregateVersion || !state.Completed {
		// The marker scan owns every historical row until it marks the ledger
		// complete. If an earlier batch already marked this row, clear that
		// marker so the updated values are replayed exactly once.
		return deferGatewayRequestGroupCacheLogToBackfillTx(tx, before)
	}
	oldRow, oldOK := requestGroupCacheAggregateFromLog(before)
	newRow, newOK := requestGroupCacheAggregateFromLog(after)
	if oldOK && before.GroupCacheAggregateVersion >= gatewayRequestGroupCacheAggregateVersion {
		if err := subtractGatewayRequestGroupCacheAggregateTx(tx, oldRow, before.ID); err != nil {
			return err
		}
	}
	if newOK {
		if err := upsertGatewayRequestGroupCacheAggregateTx(tx, newRow); err != nil {
			return err
		}
	}
	return ensureGatewayRequestGroupCacheLogsProcessedTx(tx, []uint{before.ID})
}

func groupCacheAggregateFilterSupported(filter GatewayRequestLogFilter) bool {
	// The dedicated ledger is keyed by channel, group, model, time, and key.
	// Requests for dimensions it does not preserve use the raw-log path so the
	// result cannot silently include another outcome or upstream request.
	return filter.UpstreamRequestID == "" &&
		strings.TrimSpace(filter.UpstreamKeyName) == "" &&
		strings.TrimSpace(filter.RouteReason) == "" &&
		strings.TrimSpace(filter.Result) == ""
}

type gatewayRequestGroupCacheMetrics struct {
	CacheReadTokens     int64
	CacheCreationTokens int64
	PromptTokens        int64
	ValidRequests       int64
}

// GatewayRequestGroupCacheRoutingStat is the recent cache usage snapshot for
// one upstream channel/group. It is intentionally derived from the dedicated
// aggregate ledger instead of raw request logs so route selection never has to
// scan the high-volume log table. Rows in that ledger are created only for
// normal downstream requests; latency probes are excluded at ingestion.
type GatewayRequestGroupCacheRoutingStat struct {
	ChannelID       uint
	GroupName       string
	PromptTokens    int64
	CacheReadTokens int64
	LastInputAt     time.Time
	Expired         bool
}

// GatewayRequestGroupCacheRoutingRoute bounds a cache-price refresh to routes
// that are currently candidates. The ledger can retain a large request
// history, so routing must never issue an unbounded historical group-by.
type GatewayRequestGroupCacheRoutingRoute struct {
	ChannelID uint
	GroupName string
}

const (
	gatewayRequestGroupCacheRoutingRouteLimit   = 2_000
	gatewayRequestGroupCacheRoutingBucketBudget = 2_048
)

// ListGroupCacheRoutingStats returns recent, per-channel/group cache metrics.
// The input window is counted from newest aggregate buckets backwards. A
// partial bucket is scaled proportionally so callers can use a token window
// without reading individual request rows. maxAge is an expiry boundary for
// the evidence, not a wall-clock aggregation window.
func (g *Gateway) ListGroupCacheRoutingStats(now time.Time, maxAge time.Duration, inputTokenWindow int64, routes []GatewayRequestGroupCacheRoutingRoute) ([]GatewayRequestGroupCacheRoutingStat, error) {
	return g.ListGroupCacheRoutingStatsContext(context.Background(), now, maxAge, inputTokenWindow, routes)
}

// ListGroupCacheRoutingStatsContext is the bounded-query variant used by the
// gateway hot path. Callers should provide a short deadline; a cache-price
// refresh must never hold up a downstream request indefinitely.
func (g *Gateway) ListGroupCacheRoutingStatsContext(ctx context.Context, now time.Time, maxAge time.Duration, inputTokenWindow int64, routes []GatewayRequestGroupCacheRoutingRoute) ([]GatewayRequestGroupCacheRoutingStat, error) {
	if g == nil || g.db == nil {
		return []GatewayRequestGroupCacheRoutingStat{}, nil
	}
	routes, err := normalizeGatewayRequestGroupCacheRoutingRoutes(routes)
	if err != nil {
		return nil, err
	}
	if len(routes) == 0 {
		return []GatewayRequestGroupCacheRoutingStat{}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db := g.db.WithContext(ctx)
	ready, err := groupCacheRoutingStatsReady(db)
	if err != nil {
		return nil, err
	}
	if !ready {
		return []GatewayRequestGroupCacheRoutingStat{}, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	if maxAge <= 0 {
		maxAge = 7 * 24 * time.Hour
	}
	if inputTokenWindow <= 0 {
		inputTokenWindow = 100_000_000
	}
	start := now.Add(-maxAge)
	routeValues, routeArgs := gatewayRequestGroupCacheRoutingRouteValues(routes)
	type latest struct {
		ChannelID   uint
		GroupName   string
		LastInputAt time.Time
	}
	latestRows := make([]latest, 0, len(routes))
	latestSQL := `WITH requested_routes(channel_id, group_name) AS (VALUES ` + routeValues + `)
SELECT requested.channel_id,
	requested.group_name,
	latest.last_normal_use_at AS last_input_at
FROM requested_routes AS requested
JOIN LATERAL (
	SELECT cache.last_normal_use_at
	FROM public.gateway_request_group_cache_aggregates AS cache
	WHERE cache.version = ?
	  AND cache.channel_id = requested.channel_id
	  AND LOWER(BTRIM(cache.group_name)) = requested.group_name
	  AND cache.prompt_tokens > 0
	ORDER BY cache.bucket_start DESC, cache.last_normal_use_at DESC
	LIMIT 1
) AS latest ON TRUE
ORDER BY requested.channel_id ASC, requested.group_name ASC`
	latestArgs := append(append([]any{}, routeArgs...), gatewayRequestGroupCacheAggregateVersion)
	if err := db.Raw(latestSQL, latestArgs...).Scan(&latestRows).Error; err != nil {
		return nil, err
	}
	values := make(map[gatewayRequestGroupCacheRoutingKey]GatewayRequestGroupCacheRoutingStat, len(latestRows))
	activeRoutes := make([]GatewayRequestGroupCacheRoutingRoute, 0, len(latestRows))
	for _, row := range latestRows {
		if row.ChannelID == 0 || row.LastInputAt.IsZero() {
			continue
		}
		key := gatewayRequestGroupCacheRoutingKey{ChannelID: row.ChannelID, GroupName: strings.ToLower(strings.TrimSpace(row.GroupName))}
		value := GatewayRequestGroupCacheRoutingStat{ChannelID: row.ChannelID, GroupName: key.GroupName, LastInputAt: row.LastInputAt}
		if row.LastInputAt.Before(start) {
			value.Expired = true
			values[key] = value
			continue
		}
		values[key] = value
		activeRoutes = append(activeRoutes, GatewayRequestGroupCacheRoutingRoute{ChannelID: row.ChannelID, GroupName: key.GroupName})
	}

	type recent struct {
		ChannelID       uint
		GroupName       string
		PromptTokens    int64
		CacheReadTokens int64
		LastInputAt     time.Time
	}
	recentRows := make([]recent, 0, len(activeRoutes))
	if len(activeRoutes) > 0 {
		activeRouteValues, activeRouteArgs := gatewayRequestGroupCacheRoutingRouteValues(activeRoutes)
		perRouteBucketLimit := max(1, gatewayRequestGroupCacheRoutingBucketBudget/len(activeRoutes))
		recentSQL := `WITH RECURSIVE requested_routes(channel_id, group_name) AS (VALUES ` + activeRouteValues + `),
bucket_walk AS (
	SELECT requested.channel_id,
		requested.group_name,
		bucket.bucket_start,
		bucket.prompt_tokens,
			bucket.cache_read_tokens,
			bucket.last_input_at,
			0::bigint AS previous_prompt_tokens,
			1::integer AS bucket_count
	FROM requested_routes AS requested
	JOIN LATERAL (
		SELECT cache.bucket_start,
			SUM(GREATEST(cache.prompt_tokens, 0))::bigint AS prompt_tokens,
			LEAST(SUM(GREATEST(cache.cache_read_tokens, 0)), SUM(GREATEST(cache.prompt_tokens, 0)))::bigint AS cache_read_tokens,
			MAX(cache.last_normal_use_at) AS last_input_at
		FROM public.gateway_request_group_cache_aggregates AS cache
		WHERE cache.version = ?
		  AND cache.channel_id = requested.channel_id
		  AND LOWER(BTRIM(cache.group_name)) = requested.group_name
		  AND cache.prompt_tokens > 0
		GROUP BY cache.bucket_start
		ORDER BY cache.bucket_start DESC
		LIMIT 1
	) AS bucket ON TRUE

	UNION ALL

	SELECT walk.channel_id,
		walk.group_name,
		bucket.bucket_start,
		bucket.prompt_tokens,
			bucket.cache_read_tokens,
			bucket.last_input_at,
			(walk.previous_prompt_tokens + walk.prompt_tokens)::bigint AS previous_prompt_tokens,
			(walk.bucket_count + 1)::integer AS bucket_count
	FROM bucket_walk AS walk
	JOIN LATERAL (
		SELECT cache.bucket_start,
			SUM(GREATEST(cache.prompt_tokens, 0))::bigint AS prompt_tokens,
			LEAST(SUM(GREATEST(cache.cache_read_tokens, 0)), SUM(GREATEST(cache.prompt_tokens, 0)))::bigint AS cache_read_tokens,
			MAX(cache.last_normal_use_at) AS last_input_at
		FROM public.gateway_request_group_cache_aggregates AS cache
		WHERE cache.version = ?
		  AND cache.channel_id = walk.channel_id
		  AND LOWER(BTRIM(cache.group_name)) = walk.group_name
		  AND cache.bucket_start < walk.bucket_start
		  AND cache.prompt_tokens > 0
		GROUP BY cache.bucket_start
		ORDER BY cache.bucket_start DESC
		LIMIT 1
	) AS bucket ON walk.previous_prompt_tokens + walk.prompt_tokens < ?::bigint
		AND walk.bucket_count <= ?::integer
), taken AS (
	SELECT bucket_walk.*,
		LEAST(prompt_tokens, GREATEST(?::bigint - previous_prompt_tokens, 0))::bigint AS taken_prompt_tokens
	FROM bucket_walk
)
SELECT channel_id,
	group_name,
	SUM(taken_prompt_tokens) FILTER (WHERE bucket_count <= ?::integer)::bigint AS prompt_tokens,
	SUM(FLOOR(cache_read_tokens::numeric * taken_prompt_tokens::numeric / NULLIF(prompt_tokens, 0))) FILTER (WHERE bucket_count <= ?::integer)::bigint AS cache_read_tokens,
	MAX(last_input_at) AS last_input_at
FROM taken
WHERE taken_prompt_tokens > 0
GROUP BY channel_id, group_name
HAVING COALESCE(SUM(taken_prompt_tokens) FILTER (WHERE bucket_count <= ?::integer), 0) >= ?::bigint
	OR MAX(bucket_count) < ?::integer
ORDER BY channel_id ASC, group_name ASC`
		recentArgs := append(append([]any{}, activeRouteArgs...),
			gatewayRequestGroupCacheAggregateVersion, gatewayRequestGroupCacheAggregateVersion,
			inputTokenWindow, perRouteBucketLimit, inputTokenWindow,
			perRouteBucketLimit, perRouteBucketLimit, perRouteBucketLimit, inputTokenWindow, perRouteBucketLimit,
		)
		if err := db.Raw(recentSQL, recentArgs...).Scan(&recentRows).Error; err != nil {
			return nil, err
		}
	}
	for _, row := range recentRows {
		if row.ChannelID == 0 || row.PromptTokens <= 0 {
			continue
		}
		key := gatewayRequestGroupCacheRoutingKey{ChannelID: row.ChannelID, GroupName: strings.ToLower(strings.TrimSpace(row.GroupName))}
		value := values[key]
		value.PromptTokens = row.PromptTokens
		value.CacheReadTokens = row.CacheReadTokens
		values[key] = value
	}
	out := make([]GatewayRequestGroupCacheRoutingStat, 0, len(values))
	for _, value := range values {
		if value.PromptTokens <= 0 && !value.Expired {
			continue
		}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ChannelID != out[j].ChannelID {
			return out[i].ChannelID < out[j].ChannelID
		}
		return out[i].GroupName < out[j].GroupName
	})
	return out, nil
}

func normalizeGatewayRequestGroupCacheRoutingRoutes(routes []GatewayRequestGroupCacheRoutingRoute) ([]GatewayRequestGroupCacheRoutingRoute, error) {
	values := make([]GatewayRequestGroupCacheRoutingRoute, 0, len(routes))
	seen := make(map[gatewayRequestGroupCacheRoutingKey]struct{}, len(routes))
	for _, route := range routes {
		groupName := strings.ToLower(strings.TrimSpace(route.GroupName))
		if route.ChannelID == 0 || groupName == "" {
			continue
		}
		key := gatewayRequestGroupCacheRoutingKey{ChannelID: route.ChannelID, GroupName: groupName}
		if _, ok := seen[key]; ok {
			continue
		}
		if len(values) >= gatewayRequestGroupCacheRoutingRouteLimit {
			return nil, errGatewayRequestGroupCacheRoutingRouteLimit
		}
		seen[key] = struct{}{}
		values = append(values, GatewayRequestGroupCacheRoutingRoute{ChannelID: route.ChannelID, GroupName: groupName})
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].ChannelID != values[j].ChannelID {
			return values[i].ChannelID < values[j].ChannelID
		}
		return values[i].GroupName < values[j].GroupName
	})
	return values, nil
}

func gatewayRequestGroupCacheRoutingRouteValues(routes []GatewayRequestGroupCacheRoutingRoute) (string, []any) {
	values := make([]string, 0, len(routes))
	args := make([]any, 0, len(routes)*2)
	for _, route := range routes {
		values = append(values, "(?::bigint, ?::text)")
		args = append(args, route.ChannelID, route.GroupName)
	}
	return strings.Join(values, ", "), args
}

type gatewayRequestGroupCacheRoutingKey struct {
	ChannelID uint
	GroupName string
}

// groupCacheRoutingStatsReady uses the singleton backfill state plus bounded
// version probes. The log probes are needed because a historical correction
// can invalidate a few rows after a completed rebuild; the aggregate probe
// prevents a current process from silently ignoring rows written by a newer
// ledger version.
func groupCacheRoutingStatsReady(db *gorm.DB) (bool, error) {
	var state GatewayRequestGroupCacheBackfillState
	err := db.Select("target_version", "completed").First(&state, gatewayRequestGroupCacheBackfillStateID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if state.TargetVersion != gatewayRequestGroupCacheAggregateVersion || !state.Completed {
		return false, nil
	}
	var incompatibleAggregate GatewayRequestGroupCacheAggregate
	result := db.Select("version").Where("version > ?", gatewayRequestGroupCacheAggregateVersion).Order("version DESC").Limit(1).Find(&incompatibleAggregate)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected > 0 {
		return false, nil
	}
	result = db.Select("version").Where("version < ?", gatewayRequestGroupCacheAggregateVersion).Order("version ASC").Limit(1).Find(&incompatibleAggregate)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected > 0 {
		return false, nil
	}
	var future GatewayRequestLog
	result = db.Select("id").Where("group_cache_aggregate_version > ?", gatewayRequestGroupCacheAggregateVersion).Order("group_cache_aggregate_version DESC, id DESC").Limit(1).Find(&future)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected > 0 {
		return false, nil
	}
	var pending GatewayRequestLog
	result = db.Select("id").Where("group_cache_aggregate_version < ?", gatewayRequestGroupCacheAggregateVersion).Order("group_cache_aggregate_version ASC, id ASC").Limit(1).Find(&pending)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 0, nil
}

func (m *gatewayRequestGroupCacheMetrics) merge(other gatewayRequestGroupCacheMetrics) {
	if m == nil {
		return
	}
	m.CacheReadTokens += other.CacheReadTokens
	m.CacheCreationTokens += other.CacheCreationTokens
	m.PromptTokens += other.PromptTokens
	m.ValidRequests += other.ValidRequests
}

func (m *gatewayRequestGroupCacheMetrics) addAggregate(row *GatewayRequestGroupCacheAggregate) {
	if m == nil || row == nil {
		return
	}
	m.CacheReadTokens += row.CacheReadTokens
	m.CacheCreationTokens += row.CacheCreationTokens
	m.PromptTokens += row.PromptTokens
	m.ValidRequests += row.ValidRequests
}

func gatewayRequestGroupCacheMetricsKey(channelID uint, snapshot, group string) gatewayRequestGroupCacheAggregateKey {
	return gatewayRequestGroupCacheAggregateKey{
		ChannelID:           channelID,
		ChannelNameSnapshot: strings.TrimSpace(snapshot),
		GroupName:           strings.TrimSpace(group),
	}
}

func (g *Gateway) requestLogGroupCacheRatiosRaw(filter GatewayRequestLogFilter, groupLimit int) ([]GatewayRequestLogGroupCacheRatio, error) {
	if groupLimit <= 0 {
		groupLimit = gatewayRequestAggregateGroupedReadLimit
	}
	type scan struct {
		ChannelID           uint
		ChannelNameSnapshot string
		ChannelName         string
		GroupName           string
		CacheReadTokens     int64
		CacheCreationTokens int64
		PromptTokens        int64
		ValidRequests       int64
	}
	rows := make([]scan, 0)
	query := g.applyRequestLogFilter(g.db.Table("gateway_request_logs AS logs"), "logs", filter).
		Where(normalDownstreamCachePriceRequestSQL("logs"))
	if err := query.Select(`logs.channel_id AS channel_id,
			COALESCE(NULLIF(TRIM(logs.channel_name_snapshot), ''), channels.name, '未知渠道') AS channel_name,
			COALESCE(NULLIF(TRIM(logs.channel_name_snapshot), ''), '') AS channel_name_snapshot,
			COALESCE(NULLIF(TRIM(logs.group_name), ''), '') AS group_name,
			COALESCE(SUM(CASE WHEN COALESCE(logs.prompt_tokens, 0) > 0 THEN GREATEST(COALESCE(logs.cache_read_tokens, 0), 0) ELSE 0 END), 0) AS cache_read_tokens,
			COALESCE(SUM(CASE WHEN COALESCE(logs.prompt_tokens, 0) > 0 THEN GREATEST(COALESCE(logs.cache_creation_tokens, 0), 0) ELSE 0 END), 0) AS cache_creation_tokens,
			COALESCE(SUM(GREATEST(COALESCE(logs.prompt_tokens, 0), 0)), 0) AS prompt_tokens,
			COUNT(*) AS valid_requests`).
		Joins("LEFT JOIN channels ON channels.id = logs.channel_id").
		Group("logs.channel_id, NULLIF(TRIM(logs.channel_name_snapshot), ''), channels.name, NULLIF(TRIM(logs.group_name), '')").
		Order("channel_name ASC, group_name ASC, logs.channel_id ASC").
		Limit(groupLimit + 1).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) > groupLimit {
		return nil, errGatewayRequestAggregateGroupedLimit
	}
	items := make([]GatewayRequestLogGroupCacheRatio, 0, len(rows))
	for _, row := range rows {
		item := GatewayRequestLogGroupCacheRatio{
			ChannelID: row.ChannelID, ChannelName: row.ChannelName, GroupName: row.GroupName,
			CacheReadTokens: row.CacheReadTokens, CacheCreationTokens: row.CacheCreationTokens,
			PromptTokens: row.PromptTokens, ValidRequests: row.ValidRequests,
			channelNameSnapshot: row.ChannelNameSnapshot,
		}
		if row.PromptTokens > 0 {
			ratio := float64(row.CacheReadTokens) / float64(row.PromptTokens)
			item.Ratio = &ratio
		}
		items = append(items, item)
	}
	return items, nil
}

func (g *Gateway) requestLogGroupCacheRatiosFromAggregates(filter GatewayRequestLogFilter, groupLimit int) ([]GatewayRequestLogGroupCacheRatio, bool, error) {
	if !groupCacheAggregateFilterSupported(filter) {
		return nil, false, nil
	}
	ready, err := groupCacheRoutingStatsReady(g.db)
	if err != nil {
		return nil, true, err
	}
	if !ready {
		return nil, false, nil
	}
	if groupLimit <= 0 {
		groupLimit = gatewayRequestAggregateGroupedReadLimit
	}
	metrics := make(map[gatewayRequestGroupCacheAggregateKey]*gatewayRequestGroupCacheMetrics)
	add := func(row *GatewayRequestGroupCacheAggregate) error {
		if row == nil || row.ValidRequests <= 0 {
			return nil
		}
		key := gatewayRequestGroupCacheMetricsKey(row.ChannelID, row.ChannelNameSnapshot, row.GroupName)
		value := metrics[key]
		if value == nil {
			if len(metrics) >= groupLimit {
				return errGatewayRequestAggregateGroupedLimit
			}
			value = &gatewayRequestGroupCacheMetrics{}
			metrics[key] = value
		}
		value.addAggregate(row)
		return nil
	}

	var aggregateStart, aggregateEnd *time.Time
	if filter.Start != nil {
		value := aggregateBucketCeil(*filter.Start)
		aggregateStart = &value
	}
	if filter.End != nil {
		value := aggregateBucketStart(*filter.End)
		aggregateEnd = &value
	}
	useAggregateRange := aggregateStart == nil || aggregateEnd == nil || aggregateEnd.After(*aggregateStart)
	if useAggregateRange {
		query := g.db.Model(&GatewayRequestGroupCacheAggregate{}).
			Where("version = ?", gatewayRequestGroupCacheAggregateVersion).
			Where("valid_requests > 0")
		if filter.DownstreamKeyID > 0 {
			query = query.Where("downstream_key_id = ?", filter.DownstreamKeyID)
		}
		if filter.ChannelID > 0 {
			query = query.Where("channel_id = ?", filter.ChannelID)
		}
		if modelName := strings.TrimSpace(filter.ModelName); modelName != "" {
			query = query.Where("model_name = ?", modelName)
		}
		if filter.GroupName != "" {
			query = query.Where("TRIM(COALESCE(group_name, '')) = ?", strings.TrimSpace(filter.GroupName))
		} else if filter.GroupUnassigned {
			query = query.Where("TRIM(COALESCE(group_name, '')) = ''")
		}
		if aggregateStart != nil {
			query = query.Where("bucket_start >= ?", aggregateStart)
		}
		if aggregateEnd != nil {
			query = query.Where("bucket_start < ?", aggregateEnd)
		}
		var rows []GatewayRequestGroupCacheAggregate
		if err := query.Select(`channel_id, channel_name_snapshot, group_name,
				COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
				COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
				COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
				COALESCE(SUM(valid_requests), 0) AS valid_requests`).
			Group("channel_id, channel_name_snapshot, group_name").
			Order("channel_id ASC, channel_name_snapshot ASC, group_name ASC").
			Limit(groupLimit + 1).Scan(&rows).Error; err != nil {
			return nil, true, err
		}
		if len(rows) > groupLimit {
			return nil, true, errGatewayRequestAggregateGroupedLimit
		}
		for index := range rows {
			if err := add(&rows[index]); err != nil {
				return nil, true, err
			}
		}
	}
	for _, edge := range aggregateEdgeRanges(filter, aggregateStart, aggregateEnd) {
		edgeFilter := filter
		edgeFilter.Start = &edge.start
		edgeFilter.End = &edge.end
		edgeRows, rawErr := g.requestLogGroupCacheRatiosRaw(edgeFilter, groupLimit)
		if rawErr != nil {
			return nil, true, rawErr
		}
		for _, row := range edgeRows {
			key := gatewayRequestGroupCacheMetricsKey(row.ChannelID, row.channelNameSnapshot, row.GroupName)
			value := metrics[key]
			if value == nil {
				if len(metrics) >= groupLimit {
					return nil, true, errGatewayRequestAggregateGroupedLimit
				}
				value = &gatewayRequestGroupCacheMetrics{}
				metrics[key] = value
			}
			value.merge(gatewayRequestGroupCacheMetrics{CacheReadTokens: row.CacheReadTokens, CacheCreationTokens: row.CacheCreationTokens, PromptTokens: row.PromptTokens, ValidRequests: row.ValidRequests})
		}
	}
	ratios, err := g.gatewayRequestGroupCacheRatios(metrics)
	return ratios, true, err
}

func (g *Gateway) gatewayRequestGroupCacheRatios(values map[gatewayRequestGroupCacheAggregateKey]*gatewayRequestGroupCacheMetrics) ([]GatewayRequestLogGroupCacheRatio, error) {
	if len(values) == 0 {
		return []GatewayRequestLogGroupCacheRatio{}, nil
	}
	fallbackIDs := make(map[uint]struct{})
	for key := range values {
		if key.ChannelID > 0 && key.ChannelNameSnapshot == "" {
			fallbackIDs[key.ChannelID] = struct{}{}
		}
	}
	names, err := g.requestAggregateChannelNames(fallbackIDs)
	if err != nil {
		return nil, err
	}
	items := make([]GatewayRequestLogGroupCacheRatio, 0, len(values))
	for key, metrics := range values {
		if metrics == nil || metrics.ValidRequests <= 0 {
			continue
		}
		channelName := key.ChannelNameSnapshot
		if channelName == "" {
			channelName = names[key.ChannelID]
		}
		if channelName == "" {
			channelName = "未知渠道"
		}
		item := GatewayRequestLogGroupCacheRatio{
			ChannelID: key.ChannelID, ChannelName: channelName, GroupName: key.GroupName,
			CacheReadTokens: metrics.CacheReadTokens, CacheCreationTokens: metrics.CacheCreationTokens,
			PromptTokens: metrics.PromptTokens, ValidRequests: metrics.ValidRequests,
		}
		if metrics.PromptTokens > 0 {
			ratio := float64(metrics.CacheReadTokens) / float64(metrics.PromptTokens)
			item.Ratio = &ratio
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ChannelName != items[j].ChannelName {
			return items[i].ChannelName < items[j].ChannelName
		}
		if items[i].GroupName != items[j].GroupName {
			return items[i].GroupName < items[j].GroupName
		}
		return items[i].ChannelID < items[j].ChannelID
	})
	return items, nil
}

func (g *Gateway) requestLogGroupCacheRatios(filter GatewayRequestLogFilter, groupLimit int) ([]GatewayRequestLogGroupCacheRatio, error) {
	if ratios, used, err := g.requestLogGroupCacheRatiosFromAggregates(filter, groupLimit); used || err != nil {
		return ratios, err
	}
	return g.requestLogGroupCacheRatiosRaw(filter, groupLimit)
}

// BackfillGatewayRequestGroupCacheAggregates is kept as a named entry point
// for operators and tests.  The main aggregate backfill writes both ledgers in
// one bounded transaction, so invoking either entry point has identical
// exactly-once behavior.
func BackfillGatewayRequestGroupCacheAggregates(db *gorm.DB) error {
	return migrateGatewayRequestAggregates(db)
}

var errGatewayRequestGroupCacheAggregateVersionConflict = errors.New("gateway request group cache aggregate version conflict")
var errGatewayRequestGroupCacheRoutingRouteLimit = errors.New("gateway request group cache routing route limit exceeded")

// ensureGatewayRequestGroupCacheResetVersionCompatible prevents a historical
// invalidation from deleting data written by a newer process version. The
// caller may then fail closed and let the newer writer own the rebuild.
func ensureGatewayRequestGroupCacheResetVersionCompatible(scope *gorm.DB) error {
	if scope == nil {
		return errors.New("gateway store is unavailable")
	}
	var row GatewayRequestGroupCacheAggregate
	result := scope.Select("bucket_start").Where("version > ?", gatewayRequestGroupCacheAggregateVersion).Limit(1).Find(&row)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return errGatewayRequestGroupCacheAggregateVersionConflict
	}
	return nil
}

func ensureGatewayRequestGroupCacheLogResetVersionCompatible(scope *gorm.DB) error {
	if scope == nil {
		return errors.New("gateway store is unavailable")
	}
	var log GatewayRequestLog
	result := scope.Model(&GatewayRequestLog{}).Select("id").
		Where("group_cache_aggregate_version > ?", gatewayRequestGroupCacheAggregateVersion).
		Limit(1).Find(&log)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return errGatewayRequestGroupCacheAggregateVersionConflict
	}
	return nil
}
