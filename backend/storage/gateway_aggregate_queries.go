package storage

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

const (
	gatewayRequestAggregateDefaultReadLimit    = 10000
	gatewayRequestAggregateDefaultGroupedLimit = 10000
	gatewayRequestAggregateGroupedSourceLimit  = 100000
	gatewayRequestAggregateMigrationCacheSize  = 32
	gatewayRequestAggregateMigrationCacheTTL   = 2 * time.Minute
)

var (
	gatewayRequestAggregateReadLimit        = gatewayRequestAggregateDefaultReadLimit
	gatewayRequestAggregateGroupedReadLimit = gatewayRequestAggregateDefaultGroupedLimit
	gatewayRequestAggregateGroupedBatchSize = 512
)

var errGatewayRequestAggregatesNotReady = errors.New("gateway request aggregates are still rebuilding")
var errGatewayRequestAggregateReadLimit = errors.New("gateway request aggregate row limit exceeded")
var errGatewayRequestAggregateGroupedLimit = errors.New("gateway request aggregate analytics range is too large; narrow the time range")

type requestAggregateRange struct {
	start time.Time
	end   time.Time
}

type gatewayRequestAggregateMigrationCacheKey struct {
	db                *gorm.DB
	kind              uint8
	start             string
	end               string
	timeZone          string
	downstreamKeyID   uint
	modelName         string
	channelID         uint
	groupName         string
	groupUnassigned   bool
	upstreamRequestID string
	upstreamKeyName   string
	result            string
	routeReason       string
	sortBy            string
	sortOrder         string
	limit             int
	extra             string
}

type gatewayRequestAggregateMigrationCacheValue struct {
	expiresAt time.Time
	summary   *GatewayRequestLogSummary
	analytics *GatewayRequestLogAnalytics
	usage     map[uint]GatewayDownstreamKeyUsage
}

var gatewayRequestAggregateMigrationCache = struct {
	sync.Mutex
	entries map[gatewayRequestAggregateMigrationCacheKey]gatewayRequestAggregateMigrationCacheValue
}{entries: make(map[gatewayRequestAggregateMigrationCacheKey]gatewayRequestAggregateMigrationCacheValue)}

func (g *Gateway) requestAggregateMigrationCacheKey(kind uint8, filter GatewayRequestLogFilter, limit int, extra string) gatewayRequestAggregateMigrationCacheKey {
	key := gatewayRequestAggregateMigrationCacheKey{
		db: g.db, kind: kind, timeZone: strings.TrimSpace(filter.TimeZone),
		downstreamKeyID: filter.DownstreamKeyID, modelName: strings.TrimSpace(filter.ModelName),
		channelID: filter.ChannelID, groupName: strings.TrimSpace(filter.GroupName), groupUnassigned: filter.GroupUnassigned,
		upstreamRequestID: filter.UpstreamRequestID,
		upstreamKeyName:   strings.TrimSpace(filter.UpstreamKeyName), result: strings.TrimSpace(filter.Result),
		routeReason: strings.TrimSpace(filter.RouteReason), sortBy: strings.TrimSpace(filter.SortBy),
		sortOrder: strings.TrimSpace(filter.SortOrder), limit: limit, extra: extra,
	}
	if filter.Start != nil {
		key.start = filter.Start.UTC().Format(time.RFC3339Nano)
	}
	if filter.End != nil {
		key.end = filter.End.UTC().Format(time.RFC3339Nano)
	}
	return key
}

func requestAggregateMigrationCacheGet(key gatewayRequestAggregateMigrationCacheKey) (gatewayRequestAggregateMigrationCacheValue, bool) {
	now := time.Now()
	gatewayRequestAggregateMigrationCache.Lock()
	defer gatewayRequestAggregateMigrationCache.Unlock()
	for cachedKey, value := range gatewayRequestAggregateMigrationCache.entries {
		if !now.Before(value.expiresAt) {
			delete(gatewayRequestAggregateMigrationCache.entries, cachedKey)
		}
	}
	value, ok := gatewayRequestAggregateMigrationCache.entries[key]
	return value, ok
}

func requestAggregateMigrationCacheStore(key gatewayRequestAggregateMigrationCacheKey, value gatewayRequestAggregateMigrationCacheValue) {
	gatewayRequestAggregateMigrationCache.Lock()
	defer gatewayRequestAggregateMigrationCache.Unlock()
	if len(gatewayRequestAggregateMigrationCache.entries) >= gatewayRequestAggregateMigrationCacheSize {
		clear(gatewayRequestAggregateMigrationCache.entries)
	}
	value.expiresAt = time.Now().Add(gatewayRequestAggregateMigrationCacheTTL)
	gatewayRequestAggregateMigrationCache.entries[key] = value
}

func cloneGatewayRequestLogSummary(value *GatewayRequestLogSummary) *GatewayRequestLogSummary {
	if value == nil {
		return nil
	}
	clone := *value
	if value.FirstRequestAt != nil {
		first := *value.FirstRequestAt
		clone.FirstRequestAt = &first
	}
	if value.LastRequestAt != nil {
		last := *value.LastRequestAt
		clone.LastRequestAt = &last
	}
	return &clone
}

func cloneGatewayRequestLogAnalytics(value *GatewayRequestLogAnalytics) *GatewayRequestLogAnalytics {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Trend = append([]GatewayRequestLogTrendPoint(nil), value.Trend...)
	clone.ModelUnitPrices = append([]GatewayRequestLogModelUnitPriceTrendPoint(nil), value.ModelUnitPrices...)
	clone.Channels = append([]GatewayRequestLogBreakdownItem(nil), value.Channels...)
	clone.Models = append([]GatewayRequestLogBreakdownItem(nil), value.Models...)
	clone.ModelsByTokens = append([]GatewayRequestLogBreakdownItem(nil), value.ModelsByTokens...)
	clone.Keys = append([]GatewayRequestLogBreakdownItem(nil), value.Keys...)
	clone.CostSources = append([]GatewayRequestLogBreakdownItem(nil), value.CostSources...)
	clone.RouteReasons = append([]GatewayRequestLogBreakdownItem(nil), value.RouteReasons...)
	clone.GroupCacheRatios = append([]GatewayRequestLogGroupCacheRatio(nil), value.GroupCacheRatios...)
	return &clone
}

func cloneGatewayDownstreamKeyUsage(value map[uint]GatewayDownstreamKeyUsage) map[uint]GatewayDownstreamKeyUsage {
	clone := make(map[uint]GatewayDownstreamKeyUsage, len(value))
	for id, usage := range value {
		clone[id] = usage
	}
	return clone
}

func (g *Gateway) requestLogSummaryDuringAggregateMigration(filter GatewayRequestLogFilter, now time.Time) (*GatewayRequestLogSummary, error) {
	key := g.requestAggregateMigrationCacheKey(1, filter, 0, "")
	if cached, ok := requestAggregateMigrationCacheGet(key); ok && cached.summary != nil {
		return cloneGatewayRequestLogSummary(cached.summary), nil
	}
	summary, err := g.requestLogSummaryFilteredAtRaw(filter, now)
	if err != nil {
		return nil, err
	}
	requestAggregateMigrationCacheStore(key, gatewayRequestAggregateMigrationCacheValue{summary: cloneGatewayRequestLogSummary(summary)})
	return summary, nil
}

func (g *Gateway) requestLogAnalyticsDuringAggregateMigration(filter GatewayRequestLogFilter, limit int) (*GatewayRequestLogAnalytics, error) {
	key := g.requestAggregateMigrationCacheKey(2, filter, limit, "")
	if cached, ok := requestAggregateMigrationCacheGet(key); ok && cached.analytics != nil {
		return cloneGatewayRequestLogAnalytics(cached.analytics), nil
	}
	analytics, err := g.requestLogAnalyticsRawBounded(filter, limit, gatewayRequestAggregateGroupedReadLimit+1)
	if err != nil {
		return nil, err
	}
	requestAggregateMigrationCacheStore(key, gatewayRequestAggregateMigrationCacheValue{analytics: cloneGatewayRequestLogAnalytics(analytics)})
	return analytics, nil
}

func gatewayRequestAggregateUsageCacheKey(ids []uint, dayStart time.Time) string {
	ordered := append([]uint(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	var key strings.Builder
	key.WriteString(dayStart.UTC().Format(time.RFC3339Nano))
	for _, id := range ordered {
		key.WriteByte(':')
		key.WriteString(strconv.FormatUint(uint64(id), 10))
	}
	return key.String()
}

func (g *Gateway) downstreamKeyUsageDuringAggregateMigration(ids []uint, now time.Time) (map[uint]GatewayDownstreamKeyUsage, error) {
	dayStart := time.Date(now.In(now.Location()).Year(), now.In(now.Location()).Month(), now.In(now.Location()).Day(), 0, 0, 0, 0, now.Location())
	key := g.requestAggregateMigrationCacheKey(3, GatewayRequestLogFilter{}, 0, gatewayRequestAggregateUsageCacheKey(ids, dayStart))
	if cached, ok := requestAggregateMigrationCacheGet(key); ok && cached.usage != nil {
		return cloneGatewayDownstreamKeyUsage(cached.usage), nil
	}
	usage, err := g.summarizeDownstreamKeyUsageRaw(ids, now)
	if err != nil {
		return nil, err
	}
	requestAggregateMigrationCacheStore(key, gatewayRequestAggregateMigrationCacheValue{usage: cloneGatewayDownstreamKeyUsage(usage)})
	return usage, nil
}

func (g *Gateway) requestAggregateRows(filter GatewayRequestLogFilter) ([]GatewayRequestAggregate, bool, error) {
	if filter.UpstreamRequestID != "" || strings.TrimSpace(filter.UpstreamKeyName) != "" || strings.TrimSpace(filter.GroupName) != "" || filter.GroupUnassigned {
		return nil, false, nil
	}
	ready, err := requestAggregatesReady(g.db)
	if err != nil {
		return nil, true, err
	}
	if !ready {
		return nil, true, errGatewayRequestAggregatesNotReady
	}
	query, edges := g.requestAggregateInteriorQuery(filter)
	rows := make([]GatewayRequestAggregate, 0)
	if err := query.Limit(gatewayRequestAggregateReadLimit + 1).Find(&rows).Error; err != nil {
		return nil, true, err
	}
	if len(rows) > gatewayRequestAggregateReadLimit {
		return nil, true, errGatewayRequestAggregateReadLimit
	}
	if strings.TrimSpace(filter.Result) == "" {
		remaining := gatewayRequestAggregateReadLimit - len(rows)
		if remaining <= 0 {
			return nil, true, errGatewayRequestAggregateReadLimit
		}
		evidenceRows := make([]GatewayRequestAggregate, 0)
		if err := g.requestAggregateEvidenceInteriorQuery(filter).Limit(remaining + 1).Find(&evidenceRows).Error; err != nil {
			return nil, true, err
		}
		if len(evidenceRows) > remaining {
			return nil, true, errGatewayRequestAggregateReadLimit
		}
		rows = append(rows, evidenceRows...)
	}

	grouped := make(map[gatewayRequestAggregateKey]*GatewayRequestAggregate, len(rows))
	for index := range rows {
		row := rows[index]
		grouped[aggregateKey(&row)] = &row
	}
	loaded := len(rows)
	for _, edge := range edges {
		edgeFilter := filter
		edgeFilter.Start = &edge.start
		edgeFilter.End = &edge.end
		var logs []GatewayRequestLog
		remaining := gatewayRequestAggregateReadLimit - loaded
		if remaining <= 0 {
			return nil, true, errGatewayRequestAggregateReadLimit
		}
		if err := g.applyRequestLedgerFilter(g.db.Model(&GatewayRequestLog{}), "", edgeFilter).
			Order("created_at ASC").Limit(remaining + 1).Find(&logs).Error; err != nil {
			return nil, true, err
		}
		if len(logs) > remaining {
			return nil, true, errGatewayRequestAggregateReadLimit
		}
		loaded += len(logs)
		for index := range logs {
			row, ok := requestAggregateFromLog(&logs[index])
			if !ok {
				continue
			}
			key := aggregateKey(row)
			if existing := grouped[key]; existing != nil {
				mergeRequestAggregate(existing, row)
			} else {
				grouped[key] = row
			}
		}
	}

	rows = rows[:0]
	for _, row := range grouped {
		rows = append(rows, *row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].BucketStart.Equal(rows[j].BucketStart) {
			return rows[i].BucketStart.Before(rows[j].BucketStart)
		}
		if rows[i].DownstreamKeyID != rows[j].DownstreamKeyID {
			return rows[i].DownstreamKeyID < rows[j].DownstreamKeyID
		}
		if rows[i].ChannelID != rows[j].ChannelID {
			return rows[i].ChannelID < rows[j].ChannelID
		}
		if rows[i].ChannelNameSnapshot != rows[j].ChannelNameSnapshot {
			return rows[i].ChannelNameSnapshot < rows[j].ChannelNameSnapshot
		}
		return rows[i].ModelName < rows[j].ModelName
	})
	return rows, true, nil
}

func (g *Gateway) requestAggregateInteriorQuery(filter GatewayRequestLogFilter) (*gorm.DB, []requestAggregateRange) {
	return g.requestAggregateScopedInteriorQuery(filter, false)
}

func (g *Gateway) requestAggregateEvidenceInteriorQuery(filter GatewayRequestLogFilter) *gorm.DB {
	query, _ := g.requestAggregateScopedInteriorQuery(filter, true)
	return query
}

func (g *Gateway) requestAggregateScopedInteriorQuery(filter GatewayRequestLogFilter, evidence bool) (*gorm.DB, []requestAggregateRange) {
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
	query := g.db.Model(&GatewayRequestAggregate{})
	if evidence {
		query = g.applyRequestAggregateEvidenceFilter(query, filter)
	} else {
		query = g.applyRequestAggregateFilter(query, filter)
	}
	if useAggregateRange {
		if aggregateStart != nil {
			query = query.Where("bucket_start >= ?", g.requestLogTimeBoundary(*aggregateStart))
		}
		if aggregateEnd != nil {
			query = query.Where("bucket_start < ?", g.requestLogTimeBoundary(*aggregateEnd))
		}
	} else {
		query = query.Where("1 = 0")
	}
	edges := aggregateEdgeRanges(filter, aggregateStart, aggregateEnd)
	return query, edges
}

func aggregateEdgeRanges(filter GatewayRequestLogFilter, aggregateStart, aggregateEnd *time.Time) []requestAggregateRange {
	ranges := make([]requestAggregateRange, 0, 2)
	appendRange := func(start, end time.Time) {
		if !end.After(start) {
			return
		}
		if len(ranges) > 0 && !start.After(ranges[len(ranges)-1].end) {
			if end.After(ranges[len(ranges)-1].end) {
				ranges[len(ranges)-1].end = end
			}
			return
		}
		ranges = append(ranges, requestAggregateRange{start: start, end: end})
	}
	if filter.Start != nil && aggregateStart != nil && aggregateStart.After(*filter.Start) {
		end := *aggregateStart
		if filter.End != nil && filter.End.Before(end) {
			end = *filter.End
		}
		appendRange(*filter.Start, end)
	}
	if filter.End != nil && aggregateEnd != nil && filter.End.After(*aggregateEnd) {
		start := *aggregateEnd
		if filter.Start != nil && filter.Start.After(start) {
			start = *filter.Start
		}
		appendRange(start, *filter.End)
	}
	return ranges
}

func (g *Gateway) applyRequestAggregateFilter(query *gorm.DB, filter GatewayRequestLogFilter) *gorm.DB {
	query = g.applyRequestAggregateDimensions(query, filter)
	switch strings.ToLower(strings.TrimSpace(filter.Result)) {
	case "":
		query = query.Where("result IN ?", []string{GatewayRequestResultSuccess, GatewayRequestResultFailed, GatewayRequestResultCanceled})
	case GatewayRequestResultSuccess, GatewayRequestResultFailed, GatewayRequestResultCanceled:
		query = query.Where("result = ?", strings.ToLower(strings.TrimSpace(filter.Result)))
	default:
		query = query.Where("1 = 0")
	}
	return query
}

func (g *Gateway) applyRequestAggregateEvidenceFilter(query *gorm.DB, filter GatewayRequestLogFilter) *gorm.DB {
	query = g.applyRequestAggregateDimensions(query, filter)
	if strings.TrimSpace(filter.Result) != "" {
		return query.Where("1 = 0")
	}
	return query.Where("result = ?", gatewayRequestResultDispatchEvidence)
}

func (g *Gateway) applyRequestAggregateDimensions(query *gorm.DB, filter GatewayRequestLogFilter) *gorm.DB {
	query = query.Where("version = ?", gatewayRequestAggregateVersion)
	if filter.DownstreamKeyID > 0 {
		query = query.Where("downstream_key_id = ?", filter.DownstreamKeyID)
	}
	if model := strings.TrimSpace(filter.ModelName); model != "" {
		query = query.Where("model_name = ?", model)
	}
	if filter.ChannelID > 0 {
		query = query.Where("channel_id = ?", filter.ChannelID)
	}
	if reason := strings.TrimSpace(filter.RouteReason); reason != "" {
		query = query.Where("route_reason = ?", normalizedAggregateRouteReason(reason))
	}
	return query
}

func (g *Gateway) requestLogSummaryFromAggregates(filter GatewayRequestLogFilter, now time.Time) (*GatewayRequestLogSummary, bool, error) {
	rows, used, err := g.requestAggregateRows(filter)
	if errors.Is(err, errGatewayRequestAggregatesNotReady) {
		summary, rawErr := g.requestLogSummaryDuringAggregateMigration(filter, now)
		return summary, true, rawErr
	}
	if errors.Is(err, errGatewayRequestAggregateReadLimit) {
		summary, groupedErr := g.requestLogSummaryFromAggregateSQL(filter, now)
		return summary, true, groupedErr
	}
	if err != nil || !used {
		return nil, used, err
	}
	row := &GatewayRequestLogSummary{CurrentConcurrencyScope: "instance"}
	var latencyTotal, latencySamples int64
	for index := range rows {
		item := &rows[index]
		row.DispatchEvidencePending += item.DispatchEvidencePending
		row.DispatchEvidenceActual += item.DispatchEvidenceActual
		row.DispatchEvidenceReported += item.DispatchEvidenceReported
		row.DispatchEvidenceReconciled += item.DispatchEvidenceReconciled
		if item.Result == gatewayRequestResultDispatchEvidence || item.Requests <= 0 {
			continue
		}
		row.TotalRequests += item.Requests
		switch item.Result {
		case GatewayRequestResultSuccess:
			row.Success += item.Requests
		case GatewayRequestResultCanceled:
			row.Canceled += item.Requests
		default:
			row.Failed += item.Requests
		}
		row.TotalTokens += item.TotalTokens
		row.PromptTokens += item.PromptTokens
		row.CompletionTokens += item.CompletionTokens
		row.CacheCreationTokens += item.CacheCreationTokens
		row.CacheReadTokens += item.CacheReadTokens
		row.TotalCost += item.TotalCost
		row.EstimatedCost += item.EstimatedCost
		row.ActualCost += item.ActualCost
		row.ReportedCost += item.ReportedCost
		row.ReconciledCost += item.ReconciledCost
		row.UnavailableRequests += item.UnavailableRequests
		latencyTotal += item.LatencyTotalMs
		latencySamples += item.LatencySamples
		if row.FirstRequestAt == nil || item.FirstRequestAt.Before(*row.FirstRequestAt) {
			value := item.FirstRequestAt
			row.FirstRequestAt = &value
		}
		if row.LastRequestAt == nil || item.LastRequestAt.After(*row.LastRequestAt) {
			value := item.LastRequestAt
			row.LastRequestAt = &value
		}
	}
	if latencySamples > 0 {
		row.AvgLatency = float64(latencyTotal) / float64(latencySamples)
	}
	row.WindowSeconds = requestLogRateWindowSeconds(row.FirstRequestAt, row.LastRequestAt, filter)
	performance, err := g.requestLogPerformanceFilteredAt(filter, now)
	if err != nil {
		return nil, true, err
	}
	applyPerformanceSummary(row, performance)
	return row, true, nil
}

type requestAggregateSummaryScan struct {
	TotalRequests              int64
	Success                    int64
	Failed                     int64
	Canceled                   int64
	TotalTokens                int64
	PromptTokens               int64
	CompletionTokens           int64
	CacheCreationTokens        int64
	CacheReadTokens            int64
	TotalCost                  float64
	EstimatedCost              float64
	ReportedCost               float64
	ReconciledCost             float64
	ActualCost                 float64
	UnavailableRequests        int64
	DispatchEvidencePending    int64
	DispatchEvidenceActual     float64
	DispatchEvidenceReported   float64
	DispatchEvidenceReconciled float64
	LatencyTotalMs             int64
	LatencySamples             int64
	FirstRequestAt             string
	LastRequestAt              string
}

const requestAggregateSummarySelectSQL = `
	COALESCE(SUM(requests), 0) AS total_requests,
	COALESCE(SUM(CASE WHEN result = '` + GatewayRequestResultSuccess + `' THEN requests ELSE 0 END), 0) AS success,
	COALESCE(SUM(CASE WHEN result = '` + GatewayRequestResultFailed + `' THEN requests ELSE 0 END), 0) AS failed,
	COALESCE(SUM(CASE WHEN result = '` + GatewayRequestResultCanceled + `' THEN requests ELSE 0 END), 0) AS canceled,
	COALESCE(SUM(total_tokens), 0) AS total_tokens,
	COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
	COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
	COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
	COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
	COALESCE(SUM(total_cost), 0) AS total_cost,
	COALESCE(SUM(estimated_cost), 0) AS estimated_cost,
	COALESCE(SUM(reported_cost), 0) AS reported_cost,
	COALESCE(SUM(reconciled_cost), 0) AS reconciled_cost,
	COALESCE(SUM(actual_cost), 0) AS actual_cost,
	COALESCE(SUM(unavailable_requests), 0) AS unavailable_requests,
	COALESCE(SUM(dispatch_evidence_pending), 0) AS dispatch_evidence_pending,
	COALESCE(SUM(dispatch_evidence_actual), 0) AS dispatch_evidence_actual,
	COALESCE(SUM(dispatch_evidence_reported), 0) AS dispatch_evidence_reported,
	COALESCE(SUM(dispatch_evidence_reconciled), 0) AS dispatch_evidence_reconciled,
	COALESCE(SUM(latency_total_ms), 0) AS latency_total_ms,
	COALESCE(SUM(latency_samples), 0) AS latency_samples,
	MIN(CASE WHEN requests > 0 THEN first_request_at END) AS first_request_at,
	MAX(CASE WHEN requests > 0 THEN last_request_at END) AS last_request_at`

func (g *Gateway) requestAggregateSummary(scan requestAggregateSummaryScan) *GatewayRequestLogSummary {
	row := &GatewayRequestLogSummary{
		TotalRequests: scan.TotalRequests, Success: scan.Success, Failed: scan.Failed, Canceled: scan.Canceled,
		TotalTokens: scan.TotalTokens, PromptTokens: scan.PromptTokens, CompletionTokens: scan.CompletionTokens,
		CacheCreationTokens: scan.CacheCreationTokens, CacheReadTokens: scan.CacheReadTokens,
		TotalCost: scan.TotalCost, EstimatedCost: scan.EstimatedCost, ReportedCost: scan.ReportedCost,
		ReconciledCost: scan.ReconciledCost, ActualCost: scan.ActualCost,
		UnavailableRequests: scan.UnavailableRequests, CurrentConcurrencyScope: "instance",
		DispatchEvidencePending: scan.DispatchEvidencePending, DispatchEvidenceActual: scan.DispatchEvidenceActual,
		DispatchEvidenceReported: scan.DispatchEvidenceReported, DispatchEvidenceReconciled: scan.DispatchEvidenceReconciled,
	}
	if scan.LatencySamples > 0 {
		row.AvgLatency = float64(scan.LatencyTotalMs) / float64(scan.LatencySamples)
	}
	if first, ok := g.parseRequestLogAggregateTime(scan.FirstRequestAt); ok {
		row.FirstRequestAt = &first
	}
	if last, ok := g.parseRequestLogAggregateTime(scan.LastRequestAt); ok {
		row.LastRequestAt = &last
	}
	return row
}

func mergeGatewayRequestLogSummary(dst, src *GatewayRequestLogSummary) {
	if dst == nil || src == nil {
		return
	}
	latencyTotal := dst.AvgLatency*float64(dst.Success) + src.AvgLatency*float64(src.Success)
	dst.TotalRequests += src.TotalRequests
	dst.Success += src.Success
	dst.Failed += src.Failed
	dst.Canceled += src.Canceled
	dst.TotalTokens += src.TotalTokens
	dst.PromptTokens += src.PromptTokens
	dst.CompletionTokens += src.CompletionTokens
	dst.CacheCreationTokens += src.CacheCreationTokens
	dst.CacheReadTokens += src.CacheReadTokens
	dst.TotalCost += src.TotalCost
	dst.EstimatedCost += src.EstimatedCost
	dst.ReportedCost += src.ReportedCost
	dst.ReconciledCost += src.ReconciledCost
	dst.ActualCost += src.ActualCost
	dst.UnavailableRequests += src.UnavailableRequests
	dst.DispatchEvidencePending += src.DispatchEvidencePending
	dst.DispatchEvidenceActual += src.DispatchEvidenceActual
	dst.DispatchEvidenceReported += src.DispatchEvidenceReported
	dst.DispatchEvidenceReconciled += src.DispatchEvidenceReconciled
	if dst.Success > 0 {
		dst.AvgLatency = latencyTotal / float64(dst.Success)
	}
	if src.FirstRequestAt != nil && (dst.FirstRequestAt == nil || src.FirstRequestAt.Before(*dst.FirstRequestAt)) {
		first := *src.FirstRequestAt
		dst.FirstRequestAt = &first
	}
	if src.LastRequestAt != nil && (dst.LastRequestAt == nil || src.LastRequestAt.After(*dst.LastRequestAt)) {
		last := *src.LastRequestAt
		dst.LastRequestAt = &last
	}
}

func (g *Gateway) requestLogSummaryFromAggregateSQL(filter GatewayRequestLogFilter, now time.Time) (*GatewayRequestLogSummary, error) {
	query, edges := g.requestAggregateInteriorQuery(filter)
	var scan requestAggregateSummaryScan
	if err := query.Select(requestAggregateSummarySelectSQL).Scan(&scan).Error; err != nil {
		return nil, err
	}
	row := g.requestAggregateSummary(scan)
	var evidenceScan requestAggregateSummaryScan
	if err := g.requestAggregateEvidenceInteriorQuery(filter).Select(requestAggregateSummarySelectSQL).Scan(&evidenceScan).Error; err != nil {
		return nil, err
	}
	mergeGatewayRequestLogSummary(row, g.requestAggregateSummary(evidenceScan))
	for _, edge := range edges {
		edgeFilter := filter
		edgeFilter.Start = &edge.start
		edgeFilter.End = &edge.end
		edgeSummary, err := g.requestLogSummaryFilteredAtRaw(edgeFilter, now)
		if err != nil {
			return nil, err
		}
		mergeGatewayRequestLogSummary(row, edgeSummary)
	}
	row.WindowSeconds = requestLogRateWindowSeconds(row.FirstRequestAt, row.LastRequestAt, filter)
	performance, err := g.requestLogPerformanceFilteredAt(filter, now)
	if err != nil {
		return nil, err
	}
	applyPerformanceSummary(row, performance)
	return row, nil
}

func applyPerformanceSummary(row *GatewayRequestLogSummary, performance *gatewayRequestLogPerformanceScan) {
	row.PerformanceWindowSeconds = gatewayPerformanceWindow.Seconds()
	if performance == nil {
		return
	}
	row.PerformanceRequests = performance.Requests
	row.PerformanceTokens = performance.Tokens
	row.PerformanceAvgLatency = performance.AvgLatency
	windowMinutes := int64(gatewayPerformanceWindow / time.Minute)
	if windowMinutes <= 0 {
		return
	}
	row.RPM = float64(performance.Requests / windowMinutes)
	row.TPM = float64(performance.Tokens / windowMinutes)
	performanceRPM := float64(performance.Requests) / float64(windowMinutes)
	if performanceRPM > 0 && row.PerformanceAvgLatency > 0 {
		row.EstimatedConcurrency = performanceRPM * (row.PerformanceAvgLatency / 1000) / 60
	}
}

type aggregateMetrics struct {
	requests                   int64
	success                    int64
	failed                     int64
	canceled                   int64
	totalTokens                int64
	promptTokens               int64
	completionTokens           int64
	cacheCreationTokens        int64
	cacheReadTokens            int64
	totalCost                  float64
	estimatedCost              float64
	reportedCost               float64
	reconciledCost             float64
	actualCost                 float64
	unavailableRequests        int64
	dispatchEvidencePending    int64
	dispatchEvidenceActual     float64
	dispatchEvidenceReported   float64
	dispatchEvidenceReconciled float64
	latencyTotal               float64
	latencySamples             int64
}

func (m *aggregateMetrics) add(row *GatewayRequestAggregate) {
	if m == nil || row == nil {
		return
	}
	m.requests += row.Requests
	switch row.Result {
	case GatewayRequestResultSuccess:
		m.success += row.Requests
	case GatewayRequestResultCanceled:
		m.canceled += row.Requests
	default:
		m.failed += row.Requests
	}
	m.totalTokens += row.TotalTokens
	m.promptTokens += row.PromptTokens
	m.completionTokens += row.CompletionTokens
	m.cacheCreationTokens += row.CacheCreationTokens
	m.cacheReadTokens += row.CacheReadTokens
	m.totalCost += row.TotalCost
	m.estimatedCost += row.EstimatedCost
	m.actualCost += row.ActualCost
	m.reportedCost += row.ReportedCost
	m.reconciledCost += row.ReconciledCost
	m.unavailableRequests += row.UnavailableRequests
	m.dispatchEvidencePending += row.DispatchEvidencePending
	m.dispatchEvidenceActual += row.DispatchEvidenceActual
	m.dispatchEvidenceReported += row.DispatchEvidenceReported
	m.dispatchEvidenceReconciled += row.DispatchEvidenceReconciled
	m.latencyTotal += float64(row.LatencyTotalMs)
	m.latencySamples += row.LatencySamples
}

func (m *aggregateMetrics) merge(other aggregateMetrics) {
	if m == nil {
		return
	}
	m.requests += other.requests
	m.success += other.success
	m.failed += other.failed
	m.canceled += other.canceled
	m.totalTokens += other.totalTokens
	m.promptTokens += other.promptTokens
	m.completionTokens += other.completionTokens
	m.cacheCreationTokens += other.cacheCreationTokens
	m.cacheReadTokens += other.cacheReadTokens
	m.totalCost += other.totalCost
	m.estimatedCost += other.estimatedCost
	m.reportedCost += other.reportedCost
	m.reconciledCost += other.reconciledCost
	m.actualCost += other.actualCost
	m.unavailableRequests += other.unavailableRequests
	m.dispatchEvidencePending += other.dispatchEvidencePending
	m.dispatchEvidenceActual += other.dispatchEvidenceActual
	m.dispatchEvidenceReported += other.dispatchEvidenceReported
	m.dispatchEvidenceReconciled += other.dispatchEvidenceReconciled
	m.latencyTotal += other.latencyTotal
	m.latencySamples += other.latencySamples
}

func (m aggregateMetrics) avgLatency() float64 {
	if m.latencySamples <= 0 {
		return 0
	}
	return float64(m.latencyTotal) / float64(m.latencySamples)
}

func (m aggregateMetrics) trend(bucket string) GatewayRequestLogTrendPoint {
	return GatewayRequestLogTrendPoint{
		Bucket: bucket, Requests: m.requests, Success: m.success, Failed: m.failed, Canceled: m.canceled,
		TotalTokens: m.totalTokens, PromptTokens: m.promptTokens, CompletionTokens: m.completionTokens,
		CacheCreationTokens: m.cacheCreationTokens, CacheReadTokens: m.cacheReadTokens,
		TotalCost: m.totalCost, EstimatedCost: m.estimatedCost, ReportedCost: m.reportedCost,
		ReconciledCost: m.reconciledCost, ActualCost: m.actualCost,
		UnavailableRequests: m.unavailableRequests, AvgLatency: m.avgLatency(),
		DispatchEvidencePending: m.dispatchEvidencePending, DispatchEvidenceActual: m.dispatchEvidenceActual,
		DispatchEvidenceReported: m.dispatchEvidenceReported, DispatchEvidenceReconciled: m.dispatchEvidenceReconciled,
	}
}

func (m aggregateMetrics) breakdown(name string, channelID, downstreamKeyID uint) GatewayRequestLogBreakdownItem {
	return GatewayRequestLogBreakdownItem{
		Name: name, ChannelID: channelID, DownstreamKeyID: downstreamKeyID,
		Requests: m.requests, Success: m.success, Failed: m.failed, Canceled: m.canceled,
		TotalTokens: m.totalTokens, PromptTokens: m.promptTokens, CompletionTokens: m.completionTokens,
		CacheCreationTokens: m.cacheCreationTokens, CacheReadTokens: m.cacheReadTokens,
		TotalCost: m.totalCost, EstimatedCost: m.estimatedCost, ReportedCost: m.reportedCost,
		ReconciledCost: m.reconciledCost, ActualCost: m.actualCost,
		UnavailableRequests: m.unavailableRequests, AvgLatency: m.avgLatency(),
		DispatchEvidencePending: m.dispatchEvidencePending, DispatchEvidenceActual: m.dispatchEvidenceActual,
		DispatchEvidenceReported: m.dispatchEvidenceReported, DispatchEvidenceReconciled: m.dispatchEvidenceReconciled,
	}
}

type aggregateUnitPrice struct {
	gatewayRequestUnitPriceContribution
}

func (value *aggregateUnitPrice) mergePoint(point GatewayRequestLogModelUnitPriceTrendPoint) {
	if value == nil || point.TotalTokens <= 0 {
		return
	}
	cost := 0.0
	if point.TotalCost != nil {
		cost = *point.TotalCost
	}
	switch point.TokenType {
	case GatewayTokenPriceInput:
		value.InputTokens += point.TotalTokens
		value.InputCost += cost
		if point.TotalCost != nil {
			value.InputPricedTokens += point.TotalTokens
		}
	case GatewayTokenPriceOutput:
		value.OutputTokens += point.TotalTokens
		value.OutputCost += cost
		if point.TotalCost != nil {
			value.OutputPricedTokens += point.TotalTokens
		}
	case GatewayTokenPriceCacheCreation:
		value.CacheCreationTokens += point.TotalTokens
	case GatewayTokenPriceCacheRead:
		value.CacheReadTokens += point.TotalTokens
	}
}

type requestAggregateMetricsScan struct {
	BucketStart                time.Time
	GroupName                  string
	GroupID                    uint
	Requests                   int64
	Success                    int64
	Failed                     int64
	Canceled                   int64
	TotalTokens                int64
	PromptTokens               int64
	CompletionTokens           int64
	CacheCreationTokens        int64
	CacheReadTokens            int64
	TotalCost                  float64
	EstimatedCost              float64
	ReportedCost               float64
	ReconciledCost             float64
	ActualCost                 float64
	UnavailableRequests        int64
	DispatchEvidencePending    int64
	DispatchEvidenceActual     float64
	DispatchEvidenceReported   float64
	DispatchEvidenceReconciled float64
	LatencyTotalMs             float64
	LatencySamples             int64
}

const requestAggregateMetricsSelectSQL = `
	COALESCE(SUM(requests), 0) AS requests,
	COALESCE(SUM(CASE WHEN result = '` + GatewayRequestResultSuccess + `' THEN requests ELSE 0 END), 0) AS success,
	COALESCE(SUM(CASE WHEN result = '` + GatewayRequestResultFailed + `' THEN requests ELSE 0 END), 0) AS failed,
	COALESCE(SUM(CASE WHEN result = '` + GatewayRequestResultCanceled + `' THEN requests ELSE 0 END), 0) AS canceled,
	COALESCE(SUM(total_tokens), 0) AS total_tokens,
	COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
	COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
	COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
	COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
	COALESCE(SUM(total_cost), 0) AS total_cost,
	COALESCE(SUM(estimated_cost), 0) AS estimated_cost,
	COALESCE(SUM(reported_cost), 0) AS reported_cost,
	COALESCE(SUM(reconciled_cost), 0) AS reconciled_cost,
	COALESCE(SUM(actual_cost), 0) AS actual_cost,
	COALESCE(SUM(unavailable_requests), 0) AS unavailable_requests,
	COALESCE(SUM(dispatch_evidence_pending), 0) AS dispatch_evidence_pending,
	COALESCE(SUM(dispatch_evidence_actual), 0) AS dispatch_evidence_actual,
	COALESCE(SUM(dispatch_evidence_reported), 0) AS dispatch_evidence_reported,
	COALESCE(SUM(dispatch_evidence_reconciled), 0) AS dispatch_evidence_reconciled,
	COALESCE(SUM(latency_total_ms), 0) AS latency_total_ms,
	COALESCE(SUM(latency_samples), 0) AS latency_samples`

func (row requestAggregateMetricsScan) metrics() aggregateMetrics {
	return aggregateMetrics{
		requests: row.Requests, success: row.Success, failed: row.Failed, canceled: row.Canceled,
		totalTokens: row.TotalTokens, promptTokens: row.PromptTokens, completionTokens: row.CompletionTokens,
		cacheCreationTokens: row.CacheCreationTokens, cacheReadTokens: row.CacheReadTokens,
		totalCost: row.TotalCost, estimatedCost: row.EstimatedCost, reportedCost: row.ReportedCost,
		reconciledCost: row.ReconciledCost, actualCost: row.ActualCost,
		unavailableRequests:     row.UnavailableRequests,
		dispatchEvidencePending: row.DispatchEvidencePending, dispatchEvidenceActual: row.DispatchEvidenceActual,
		dispatchEvidenceReported: row.DispatchEvidenceReported, dispatchEvidenceReconciled: row.DispatchEvidenceReconciled,
		latencyTotal: row.LatencyTotalMs, latencySamples: row.LatencySamples,
	}
}

func aggregateMetricsFromTrend(row GatewayRequestLogTrendPoint) aggregateMetrics {
	return aggregateMetrics{
		requests: row.Requests, success: row.Success, failed: row.Failed, canceled: row.Canceled,
		totalTokens: row.TotalTokens, promptTokens: row.PromptTokens, completionTokens: row.CompletionTokens,
		cacheCreationTokens: row.CacheCreationTokens, cacheReadTokens: row.CacheReadTokens,
		totalCost: row.TotalCost, estimatedCost: row.EstimatedCost, reportedCost: row.ReportedCost,
		reconciledCost: row.ReconciledCost, actualCost: row.ActualCost,
		dispatchEvidencePending: row.DispatchEvidencePending, dispatchEvidenceActual: row.DispatchEvidenceActual,
		dispatchEvidenceReported: row.DispatchEvidenceReported, dispatchEvidenceReconciled: row.DispatchEvidenceReconciled,
		latencyTotal: row.AvgLatency * float64(row.Success), latencySamples: row.Success,
	}
}

func aggregateMetricsFromBreakdown(row GatewayRequestLogBreakdownItem) aggregateMetrics {
	return aggregateMetricsFromTrend(GatewayRequestLogTrendPoint{
		Requests: row.Requests, Success: row.Success, Failed: row.Failed, Canceled: row.Canceled,
		TotalTokens: row.TotalTokens, PromptTokens: row.PromptTokens, CompletionTokens: row.CompletionTokens,
		CacheCreationTokens: row.CacheCreationTokens, CacheReadTokens: row.CacheReadTokens,
		TotalCost: row.TotalCost, EstimatedCost: row.EstimatedCost, ReportedCost: row.ReportedCost,
		ReconciledCost: row.ReconciledCost, ActualCost: row.ActualCost,
		UnavailableRequests: row.UnavailableRequests, AvgLatency: row.AvgLatency,
		DispatchEvidencePending: row.DispatchEvidencePending, DispatchEvidenceActual: row.DispatchEvidenceActual,
		DispatchEvidenceReported: row.DispatchEvidenceReported, DispatchEvidenceReconciled: row.DispatchEvidenceReconciled,
	})
}

func mergeRequestAggregateEdgeBreakdowns(
	edges []*GatewayRequestLogAnalytics,
	channelMap map[gatewayRequestAggregateChannelKey]*aggregateMetrics,
	modelMap map[string]*aggregateMetrics,
	keyMap map[uint]*aggregateMetrics,
	costSourceMap map[string]*aggregateMetrics,
	routeReasonMap map[string]*aggregateMetrics,
) error {
	// Build every logical-request dimension across both partial edges before
	// considering evidence-only groups. Otherwise evidence in the first edge
	// would be lost when the matching request exists only in the second edge.
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		for _, row := range edge.Channels {
			if row.Requests <= 0 {
				continue
			}
			key := requestAggregateChannelKey(row.ChannelID, row.Name)
			if err := ensureAggregateGroupedCapacity(channelMap, key); err != nil {
				return err
			}
			metricsForRequestAggregateChannel(channelMap, key).merge(aggregateMetricsFromBreakdown(row))
		}
		for _, row := range edge.Models {
			if row.Requests <= 0 {
				continue
			}
			if err := ensureAggregateGroupedCapacity(modelMap, row.Name); err != nil {
				return err
			}
			metricsFor(modelMap, row.Name).merge(aggregateMetricsFromBreakdown(row))
		}
		for _, row := range edge.Keys {
			if row.Requests <= 0 {
				continue
			}
			if err := ensureAggregateGroupedCapacity(keyMap, row.DownstreamKeyID); err != nil {
				return err
			}
			metricsForUint(keyMap, row.DownstreamKeyID).merge(aggregateMetricsFromBreakdown(row))
		}
		for _, row := range edge.CostSources {
			if row.Requests <= 0 {
				continue
			}
			if err := ensureAggregateGroupedCapacity(costSourceMap, row.Name); err != nil {
				return err
			}
			metricsFor(costSourceMap, row.Name).merge(aggregateMetricsFromBreakdown(row))
		}
		for _, row := range edge.RouteReasons {
			if row.Requests <= 0 {
				continue
			}
			if err := ensureAggregateGroupedCapacity(routeReasonMap, row.Name); err != nil {
				return err
			}
			metricsFor(routeReasonMap, row.Name).merge(aggregateMetricsFromBreakdown(row))
		}
	}

	// Evidence contributes only to dimensions already made visible by a normal
	// request somewhere in the complete range. Evidence-only dimensions remain
	// hidden while their spend stays available in the independent summary/trend.
	for _, edge := range edges {
		if edge == nil {
			continue
		}
		for _, row := range edge.Channels {
			if row.Requests > 0 {
				continue
			}
			if metrics := channelMap[requestAggregateChannelKey(row.ChannelID, row.Name)]; metrics != nil {
				metrics.merge(aggregateMetricsFromBreakdown(row))
			}
		}
		for _, row := range edge.Models {
			if row.Requests > 0 {
				continue
			}
			if metrics := modelMap[row.Name]; metrics != nil {
				metrics.merge(aggregateMetricsFromBreakdown(row))
			}
		}
		for _, row := range edge.Keys {
			if row.Requests > 0 {
				continue
			}
			if metrics := keyMap[row.DownstreamKeyID]; metrics != nil {
				metrics.merge(aggregateMetricsFromBreakdown(row))
			}
		}
		for _, row := range edge.CostSources {
			if row.Requests > 0 {
				continue
			}
			if metrics := costSourceMap[row.Name]; metrics != nil {
				metrics.merge(aggregateMetricsFromBreakdown(row))
			}
		}
		for _, row := range edge.RouteReasons {
			if row.Requests > 0 {
				continue
			}
			if metrics := routeReasonMap[row.Name]; metrics != nil {
				metrics.merge(aggregateMetricsFromBreakdown(row))
			}
		}
	}
	return nil
}

func (g *Gateway) visitRequestAggregateGroupedMetrics(
	filter GatewayRequestLogFilter,
	groupSelect string,
	groupBy string,
	cursorValue func(requestAggregateMetricsScan) any,
	visit func(requestAggregateMetricsScan) error,
) error {
	return g.visitRequestAggregateGroupedMetricsScoped(filter, groupSelect, groupBy, cursorValue, visit, false)
}

func (g *Gateway) visitRequestAggregateGroupedMetricsScoped(
	filter GatewayRequestLogFilter,
	groupSelect string,
	groupBy string,
	cursorValue func(requestAggregateMetricsScan) any,
	visit func(requestAggregateMetricsScan) error,
	evidence bool,
) error {
	batchSize := gatewayRequestAggregateGroupedBatchSize
	if batchSize <= 0 {
		batchSize = 512
	}
	var after any
	visited := 0
	for {
		var query *gorm.DB
		if evidence {
			query = g.requestAggregateEvidenceInteriorQuery(filter)
		} else {
			query, _ = g.requestAggregateInteriorQuery(filter)
		}
		if after != nil {
			query = query.Where(groupBy+" > ?", after)
		}
		rows := make([]requestAggregateMetricsScan, 0, batchSize)
		if err := query.Select(groupSelect + ", " + requestAggregateMetricsSelectSQL).
			Group(groupBy).Order(groupBy + " ASC").Limit(batchSize).Scan(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			visited++
			if visited > gatewayRequestAggregateGroupedSourceLimit {
				return errGatewayRequestAggregateGroupedLimit
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		if len(rows) < batchSize {
			return nil
		}
		after = cursorValue(rows[len(rows)-1])
	}
}

func (g *Gateway) visitRequestAggregateChannelMetrics(filter GatewayRequestLogFilter, visit func(requestAggregateMetricsScan) error) error {
	return g.visitRequestAggregateChannelMetricsScoped(filter, visit, false)
}

func (g *Gateway) visitRequestAggregateChannelMetricsScoped(filter GatewayRequestLogFilter, visit func(requestAggregateMetricsScan) error, evidence bool) error {
	batchSize := gatewayRequestAggregateGroupedBatchSize
	if batchSize <= 0 {
		batchSize = 512
	}
	var afterID uint
	afterName := ""
	hasAfter := false
	visited := 0
	for {
		var query *gorm.DB
		if evidence {
			query = g.requestAggregateEvidenceInteriorQuery(filter)
		} else {
			query, _ = g.requestAggregateInteriorQuery(filter)
		}
		if hasAfter {
			query = query.Where("channel_id > ? OR (channel_id = ? AND COALESCE(channel_name_snapshot, '') > ?)", afterID, afterID, afterName)
		}
		rows := make([]requestAggregateMetricsScan, 0, batchSize)
		if err := query.
			Select("channel_id AS group_id, COALESCE(channel_name_snapshot, '') AS group_name, " + requestAggregateMetricsSelectSQL).
			Group("channel_id, COALESCE(channel_name_snapshot, '')").
			Order("channel_id ASC, COALESCE(channel_name_snapshot, '') ASC").
			Limit(batchSize).
			Scan(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			visited++
			if visited > gatewayRequestAggregateGroupedSourceLimit {
				return errGatewayRequestAggregateGroupedLimit
			}
			if err := visit(row); err != nil {
				return err
			}
		}
		if len(rows) < batchSize {
			return nil
		}
		last := rows[len(rows)-1]
		afterID = last.GroupID
		afterName = last.GroupName
		hasAfter = true
	}
}

func ensureAggregateGroupedCapacity[K comparable, V any](values map[K]V, key K) error {
	if _, exists := values[key]; !exists && len(values) >= gatewayRequestAggregateGroupedReadLimit {
		return errGatewayRequestAggregateGroupedLimit
	}
	return nil
}

func (g *Gateway) requestLogAnalyticsFromAggregateSQL(filter GatewayRequestLogFilter, limit int) (*GatewayRequestLogAnalytics, error) {
	if limit <= 0 {
		limit = 8
	}
	location := time.UTC
	if name := strings.TrimSpace(filter.TimeZone); name != "" {
		if configured, err := time.LoadLocation(name); err == nil {
			location = configured
		}
	}
	hourly := filter.Start != nil && filter.End != nil && filter.End.After(*filter.Start) && filter.End.Sub(*filter.Start) <= 48*time.Hour
	bucketName := func(value time.Time) string {
		if hourly {
			return value.UTC().Truncate(time.Hour).Format("2006-01-02T15:00:00Z")
		}
		return value.In(location).Format("2006-01-02")
	}

	trendMap := make(map[string]*aggregateMetrics)
	channelMap := make(map[gatewayRequestAggregateChannelKey]*aggregateMetrics)
	modelMap := make(map[string]*aggregateMetrics)
	keyMap := make(map[uint]*aggregateMetrics)
	costSourceMap := make(map[string]*aggregateMetrics)
	routeReasonMap := make(map[string]*aggregateMetrics)
	unitPriceMap := make(map[string]*aggregateUnitPrice)

	if err := g.visitRequestAggregateGroupedMetrics(filter, "bucket_start AS bucket_start", "bucket_start", func(row requestAggregateMetricsScan) any {
		return row.BucketStart
	}, func(row requestAggregateMetricsScan) error {
		key := bucketName(row.BucketStart)
		if err := ensureAggregateGroupedCapacity(trendMap, key); err != nil {
			return err
		}
		metricsFor(trendMap, key).merge(row.metrics())
		return nil
	}); err != nil {
		return nil, err
	}
	if err := g.visitRequestAggregateChannelMetrics(filter, func(row requestAggregateMetricsScan) error {
		key := requestAggregateChannelKey(row.GroupID, row.GroupName)
		if err := ensureAggregateGroupedCapacity(channelMap, key); err != nil {
			return err
		}
		metricsForRequestAggregateChannel(channelMap, key).merge(row.metrics())
		return nil
	}); err != nil {
		return nil, err
	}

	for _, dimension := range []struct {
		selectSQL   string
		groupBy     string
		cursorValue func(requestAggregateMetricsScan) any
		apply       func(requestAggregateMetricsScan) error
	}{
		{"model_name AS group_name", "model_name", func(row requestAggregateMetricsScan) any { return row.GroupName }, func(row requestAggregateMetricsScan) error {
			key := normalizedAggregateModel(row.GroupName)
			if err := ensureAggregateGroupedCapacity(modelMap, key); err != nil {
				return err
			}
			metricsFor(modelMap, key).merge(row.metrics())
			return nil
		}},
		{"downstream_key_id AS group_id", "downstream_key_id", func(row requestAggregateMetricsScan) any { return row.GroupID }, func(row requestAggregateMetricsScan) error {
			if err := ensureAggregateGroupedCapacity(keyMap, row.GroupID); err != nil {
				return err
			}
			metricsForUint(keyMap, row.GroupID).merge(row.metrics())
			return nil
		}},
		{"cost_source AS group_name", "cost_source", func(row requestAggregateMetricsScan) any { return row.GroupName }, func(row requestAggregateMetricsScan) error {
			key := normalizedAggregateCostSource(row.GroupName)
			if err := ensureAggregateGroupedCapacity(costSourceMap, key); err != nil {
				return err
			}
			metricsFor(costSourceMap, key).merge(row.metrics())
			return nil
		}},
		{"route_reason AS group_name", "route_reason", func(row requestAggregateMetricsScan) any { return row.GroupName }, func(row requestAggregateMetricsScan) error {
			key := normalizedAggregateRouteReason(row.GroupName)
			if err := ensureAggregateGroupedCapacity(routeReasonMap, key); err != nil {
				return err
			}
			metricsFor(routeReasonMap, key).merge(row.metrics())
			return nil
		}},
	} {
		if queryErr := g.visitRequestAggregateGroupedMetrics(filter, dimension.selectSQL, dimension.groupBy, dimension.cursorValue, dimension.apply); queryErr != nil {
			return nil, queryErr
		}
	}

	if strings.TrimSpace(filter.Result) == "" {
		if err := g.visitRequestAggregateGroupedMetricsScoped(filter, "bucket_start AS bucket_start", "bucket_start", func(row requestAggregateMetricsScan) any {
			return row.BucketStart
		}, func(row requestAggregateMetricsScan) error {
			key := bucketName(row.BucketStart)
			if err := ensureAggregateGroupedCapacity(trendMap, key); err != nil {
				return err
			}
			metricsFor(trendMap, key).merge(row.metrics())
			return nil
		}, true); err != nil {
			return nil, err
		}
		if err := g.visitRequestAggregateChannelMetricsScoped(filter, func(row requestAggregateMetricsScan) error {
			key := requestAggregateChannelKey(row.GroupID, row.GroupName)
			if metrics := channelMap[key]; metrics != nil {
				metrics.merge(row.metrics())
			}
			return nil
		}, true); err != nil {
			return nil, err
		}
		for _, dimension := range []struct {
			selectSQL   string
			groupBy     string
			cursorValue func(requestAggregateMetricsScan) any
			apply       func(requestAggregateMetricsScan)
		}{
			{"model_name AS group_name", "model_name", func(row requestAggregateMetricsScan) any { return row.GroupName }, func(row requestAggregateMetricsScan) {
				if metrics := modelMap[normalizedAggregateModel(row.GroupName)]; metrics != nil {
					metrics.merge(row.metrics())
				}
			}},
			{"downstream_key_id AS group_id", "downstream_key_id", func(row requestAggregateMetricsScan) any { return row.GroupID }, func(row requestAggregateMetricsScan) {
				if metrics := keyMap[row.GroupID]; metrics != nil {
					metrics.merge(row.metrics())
				}
			}},
			{"cost_source AS group_name", "cost_source", func(row requestAggregateMetricsScan) any { return row.GroupName }, func(row requestAggregateMetricsScan) {
				if metrics := costSourceMap[normalizedAggregateCostSource(row.GroupName)]; metrics != nil {
					metrics.merge(row.metrics())
				}
			}},
			{"route_reason AS group_name", "route_reason", func(row requestAggregateMetricsScan) any { return row.GroupName }, func(row requestAggregateMetricsScan) {
				if metrics := routeReasonMap[normalizedAggregateRouteReason(row.GroupName)]; metrics != nil {
					metrics.merge(row.metrics())
				}
			}},
		} {
			if queryErr := g.visitRequestAggregateGroupedMetricsScoped(filter, dimension.selectSQL, dimension.groupBy, dimension.cursorValue, func(row requestAggregateMetricsScan) error {
				dimension.apply(row)
				return nil
			}, true); queryErr != nil {
				return nil, queryErr
			}
		}
	}

	type unitPriceScan struct {
		BucketStart         time.Time
		ModelName           string
		InputTokens         int64
		OutputTokens        int64
		InputPricedTokens   int64
		OutputPricedTokens  int64
		CacheCreationTokens int64
		CacheReadTokens     int64
		InputCost           float64
		OutputCost          float64
	}
	_, edges := g.requestAggregateInteriorQuery(filter)
	batchSize := gatewayRequestAggregateGroupedBatchSize
	if batchSize <= 0 {
		batchSize = 512
	}
	var afterBucket time.Time
	afterModel := ""
	visitedUnitPrices := 0
	for {
		unitRows := make([]unitPriceScan, 0, batchSize)
		unitQuery, _ := g.requestAggregateInteriorQuery(filter)
		if !afterBucket.IsZero() {
			unitQuery = unitQuery.Where("bucket_start > ? OR (bucket_start = ? AND model_name > ?)", afterBucket, afterBucket, afterModel)
		}
		if err := unitQuery.Where("downstream_key_id > 0 AND (performance_input_tokens > 0 OR performance_output_tokens > 0 OR performance_cache_creation_tokens > 0 OR performance_cache_read_tokens > 0) AND TRIM(model_name) <> ''").
			Select(`bucket_start, model_name,
				COALESCE(SUM(performance_input_tokens), 0) AS input_tokens,
				COALESCE(SUM(performance_output_tokens), 0) AS output_tokens,
				COALESCE(SUM(performance_input_priced_tokens), 0) AS input_priced_tokens,
				COALESCE(SUM(performance_output_priced_tokens), 0) AS output_priced_tokens,
				COALESCE(SUM(performance_cache_creation_tokens), 0) AS cache_creation_tokens,
				COALESCE(SUM(performance_cache_read_tokens), 0) AS cache_read_tokens,
				COALESCE(SUM(performance_input_cost), 0) AS input_cost,
				COALESCE(SUM(performance_output_cost), 0) AS output_cost`).
			Group("bucket_start, model_name").Order("bucket_start ASC, model_name ASC").
			Limit(batchSize).Scan(&unitRows).Error; err != nil {
			return nil, err
		}
		for _, row := range unitRows {
			visitedUnitPrices++
			if visitedUnitPrices > gatewayRequestAggregateGroupedSourceLimit {
				return nil, errGatewayRequestAggregateGroupedLimit
			}
			family := gatewayUnitPriceModelFamily(row.ModelName)
			if family == "" {
				continue
			}
			key := bucketName(row.BucketStart) + "\x00" + family + "\x00" + strings.TrimSpace(row.ModelName)
			if err := ensureAggregateGroupedCapacity(unitPriceMap, key); err != nil {
				return nil, err
			}
			value := unitPriceMap[key]
			if value == nil {
				value = &aggregateUnitPrice{}
				unitPriceMap[key] = value
			}
			value.InputTokens += row.InputTokens
			value.OutputTokens += row.OutputTokens
			value.InputPricedTokens += row.InputPricedTokens
			value.OutputPricedTokens += row.OutputPricedTokens
			value.CacheCreationTokens += row.CacheCreationTokens
			value.CacheReadTokens += row.CacheReadTokens
			value.InputCost += row.InputCost
			value.OutputCost += row.OutputCost
		}
		if len(unitRows) < batchSize {
			break
		}
		last := unitRows[len(unitRows)-1]
		afterBucket = last.BucketStart
		afterModel = last.ModelName
	}

	edgeAnalyticsRows := make([]*GatewayRequestLogAnalytics, 0, len(edges))
	for _, edge := range edges {
		edgeFilter := filter
		edgeFilter.Start = &edge.start
		edgeFilter.End = &edge.end
		strictLimit := gatewayRequestAggregateGroupedReadLimit
		edgeAnalytics, edgeErr := g.requestLogAnalyticsRawBoundedWithGranularity(edgeFilter, strictLimit+1, strictLimit+1, hourly, true)
		if edgeErr != nil {
			return nil, edgeErr
		}
		if requestLogAnalyticsExceedsLimit(edgeAnalytics, strictLimit) {
			return nil, errGatewayRequestAggregateGroupedLimit
		}
		edgeAnalyticsRows = append(edgeAnalyticsRows, edgeAnalytics)
	}

	for _, edgeAnalytics := range edgeAnalyticsRows {
		for _, row := range edgeAnalytics.Trend {
			if err := ensureAggregateGroupedCapacity(trendMap, row.Bucket); err != nil {
				return nil, err
			}
			metricsFor(trendMap, row.Bucket).merge(aggregateMetricsFromTrend(row))
		}
		for _, row := range edgeAnalytics.ModelUnitPrices {
			key := row.Bucket + "\x00" + row.ModelFamily + "\x00" + row.ModelName
			if err := ensureAggregateGroupedCapacity(unitPriceMap, key); err != nil {
				return nil, err
			}
			value := unitPriceMap[key]
			if value == nil {
				value = &aggregateUnitPrice{}
				unitPriceMap[key] = value
			}
			value.mergePoint(row)
		}
	}
	if err := mergeRequestAggregateEdgeBreakdowns(
		edgeAnalyticsRows, channelMap, modelMap, keyMap, costSourceMap, routeReasonMap,
	); err != nil {
		return nil, err
	}

	channels, err := g.requestAggregateChannelBreakdowns(channelMap)
	if err != nil {
		return nil, err
	}
	keyNames, err := g.requestAggregateKeyNames(keyMap)
	if err != nil {
		return nil, err
	}
	out := &GatewayRequestLogAnalytics{
		Trend: make([]GatewayRequestLogTrendPoint, 0, len(trendMap)), ModelUnitPrices: make([]GatewayRequestLogModelUnitPriceTrendPoint, 0, len(unitPriceMap)),
		Channels: channels, Models: make([]GatewayRequestLogBreakdownItem, 0, len(modelMap)),
		ModelsByTokens: make([]GatewayRequestLogBreakdownItem, 0, len(modelMap)), Keys: make([]GatewayRequestLogBreakdownItem, 0, len(keyMap)),
		CostSources: make([]GatewayRequestLogBreakdownItem, 0, len(costSourceMap)), RouteReasons: make([]GatewayRequestLogBreakdownItem, 0, len(routeReasonMap)),
		GroupCacheRatios: []GatewayRequestLogGroupCacheRatio{},
	}
	for bucket, metrics := range trendMap {
		out.Trend = append(out.Trend, metrics.trend(bucket))
	}
	for name, metrics := range modelMap {
		item := metrics.breakdown(name, 0, 0)
		out.Models = append(out.Models, item)
		out.ModelsByTokens = append(out.ModelsByTokens, item)
	}
	for id, metrics := range keyMap {
		name := keyNames[id]
		if name == "" {
			name = "未知 Key"
		}
		out.Keys = append(out.Keys, metrics.breakdown(name, 0, id))
	}
	for name, metrics := range costSourceMap {
		out.CostSources = append(out.CostSources, metrics.breakdown(name, 0, 0))
	}
	for name, metrics := range routeReasonMap {
		out.RouteReasons = append(out.RouteReasons, metrics.breakdown(name, 0, 0))
	}
	for key, value := range unitPriceMap {
		parts := strings.Split(key, "\x00")
		if len(parts) != 3 {
			continue
		}
		out.ModelUnitPrices = appendGatewayUnitPricePoints(out.ModelUnitPrices, parts[0], parts[1], parts[2], value.gatewayRequestUnitPriceContribution)
	}
	sort.Slice(out.Trend, func(i, j int) bool { return out.Trend[i].Bucket < out.Trend[j].Bucket })
	sort.Slice(out.ModelUnitPrices, func(i, j int) bool {
		if out.ModelUnitPrices[i].Bucket != out.ModelUnitPrices[j].Bucket {
			return out.ModelUnitPrices[i].Bucket < out.ModelUnitPrices[j].Bucket
		}
		if out.ModelUnitPrices[i].ModelName != out.ModelUnitPrices[j].ModelName {
			return out.ModelUnitPrices[i].ModelName < out.ModelUnitPrices[j].ModelName
		}
		return out.ModelUnitPrices[i].TokenType < out.ModelUnitPrices[j].TokenType
	})
	sortBreakdowns(out.Channels, false)
	sortBreakdowns(out.Models, false)
	sortBreakdowns(out.ModelsByTokens, true)
	sortBreakdowns(out.Keys, false)
	sortBreakdowns(out.CostSources, false)
	sortBreakdowns(out.RouteReasons, false)
	out.Channels = limitBreakdowns(out.Channels, limit)
	out.Models = limitBreakdowns(out.Models, limit)
	out.ModelsByTokens = limitBreakdowns(out.ModelsByTokens, limit)
	out.Keys = limitBreakdowns(out.Keys, limit)
	out.CostSources = limitBreakdowns(out.CostSources, limit)
	out.RouteReasons = limitBreakdowns(out.RouteReasons, limit)
	return out, nil
}

func requestLogAnalyticsExceedsLimit(analytics *GatewayRequestLogAnalytics, limit int) bool {
	if analytics == nil || limit <= 0 {
		return false
	}
	return len(analytics.Trend) > limit ||
		requestLogModelUnitPriceGroupCount(analytics.ModelUnitPrices) > limit ||
		len(analytics.Channels) > limit ||
		len(analytics.Models) > limit ||
		len(analytics.ModelsByTokens) > limit ||
		len(analytics.Keys) > limit ||
		len(analytics.CostSources) > limit ||
		len(analytics.RouteReasons) > limit
}

func requestLogModelUnitPriceGroupCount(points []GatewayRequestLogModelUnitPriceTrendPoint) int {
	groups := make(map[string]struct{}, len(points))
	for _, point := range points {
		groups[point.Bucket+"\x00"+point.ModelFamily+"\x00"+point.ModelName] = struct{}{}
	}
	return len(groups)
}

func (g *Gateway) requestLogAnalyticsFromAggregates(filter GatewayRequestLogFilter, limit int) (*GatewayRequestLogAnalytics, bool, error) {
	rows, used, err := g.requestAggregateRows(filter)
	if errors.Is(err, errGatewayRequestAggregatesNotReady) {
		analytics, rawErr := g.requestLogAnalyticsDuringAggregateMigration(filter, limit)
		return analytics, true, rawErr
	}
	if errors.Is(err, errGatewayRequestAggregateReadLimit) {
		analytics, groupedErr := g.requestLogAnalyticsFromAggregateSQL(filter, limit)
		return analytics, true, groupedErr
	}
	if err != nil || !used {
		return nil, used, err
	}
	if limit <= 0 {
		limit = 8
	}
	location := time.UTC
	if name := strings.TrimSpace(filter.TimeZone); name != "" {
		if configured, loadErr := time.LoadLocation(name); loadErr == nil {
			location = configured
		}
	}
	hourly := filter.Start != nil && filter.End != nil && filter.End.After(*filter.Start) && filter.End.Sub(*filter.Start) <= 48*time.Hour
	bucketName := func(value time.Time) string {
		if hourly {
			return value.UTC().Truncate(time.Hour).Format("2006-01-02T15:00:00Z")
		}
		return value.In(location).Format("2006-01-02")
	}

	trendMap := make(map[string]*aggregateMetrics)
	channelMap := make(map[gatewayRequestAggregateChannelKey]*aggregateMetrics)
	modelMap := make(map[string]*aggregateMetrics)
	keyMap := make(map[uint]*aggregateMetrics)
	costSourceMap := make(map[string]*aggregateMetrics)
	routeReasonMap := make(map[string]*aggregateMetrics)
	unitPriceMap := make(map[string]*aggregateUnitPrice)
	for index := range rows {
		row := &rows[index]
		if row.Result == gatewayRequestResultDispatchEvidence || row.Requests <= 0 {
			continue
		}
		bucket := bucketName(row.BucketStart)
		metricsFor(trendMap, bucket).add(row)
		metricsForRequestAggregateChannel(channelMap, requestAggregateChannelKey(row.ChannelID, row.ChannelNameSnapshot)).add(row)
		metricsFor(modelMap, normalizedAggregateModel(row.ModelName)).add(row)
		metricsForUint(keyMap, row.DownstreamKeyID).add(row)
		metricsFor(costSourceMap, normalizedAggregateCostSource(row.CostSource)).add(row)
		metricsFor(routeReasonMap, normalizedAggregateRouteReason(row.RouteReason)).add(row)
		family := gatewayUnitPriceModelFamily(row.ModelName)
		if family != "" && row.DownstreamKeyID > 0 && (row.PerformanceInputTokens > 0 || row.PerformanceOutputTokens > 0 || row.PerformanceCacheCreationTokens > 0 || row.PerformanceCacheReadTokens > 0) {
			key := bucket + "\x00" + family + "\x00" + strings.TrimSpace(row.ModelName)
			value := unitPriceMap[key]
			if value == nil {
				value = &aggregateUnitPrice{}
				unitPriceMap[key] = value
			}
			value.InputTokens += row.PerformanceInputTokens
			value.OutputTokens += row.PerformanceOutputTokens
			value.InputPricedTokens += row.PerformanceInputPricedTokens
			value.OutputPricedTokens += row.PerformanceOutputPricedTokens
			value.CacheCreationTokens += row.PerformanceCacheCreationTokens
			value.CacheReadTokens += row.PerformanceCacheReadTokens
			value.InputCost += row.PerformanceInputCost
			value.OutputCost += row.PerformanceOutputCost
		}
	}
	if strings.TrimSpace(filter.Result) == "" {
		for index := range rows {
			row := &rows[index]
			if row.Result != gatewayRequestResultDispatchEvidence {
				continue
			}
			bucket := bucketName(row.BucketStart)
			metricsFor(trendMap, bucket).add(row)
			if metrics := channelMap[requestAggregateChannelKey(row.ChannelID, row.ChannelNameSnapshot)]; metrics != nil {
				metrics.add(row)
			}
			if metrics := modelMap[normalizedAggregateModel(row.ModelName)]; metrics != nil {
				metrics.add(row)
			}
			if metrics := keyMap[row.DownstreamKeyID]; metrics != nil {
				metrics.add(row)
			}
			if metrics := costSourceMap[normalizedAggregateCostSource(row.CostSource)]; metrics != nil {
				metrics.add(row)
			}
			if metrics := routeReasonMap[normalizedAggregateRouteReason(row.RouteReason)]; metrics != nil {
				metrics.add(row)
			}
		}
	}

	channels, err := g.requestAggregateChannelBreakdowns(channelMap)
	if err != nil {
		return nil, true, err
	}
	keyNames, err := g.requestAggregateKeyNames(keyMap)
	if err != nil {
		return nil, true, err
	}
	out := &GatewayRequestLogAnalytics{
		Trend:            make([]GatewayRequestLogTrendPoint, 0, len(trendMap)),
		ModelUnitPrices:  make([]GatewayRequestLogModelUnitPriceTrendPoint, 0, len(unitPriceMap)),
		Channels:         channels,
		Models:           make([]GatewayRequestLogBreakdownItem, 0, len(modelMap)),
		ModelsByTokens:   make([]GatewayRequestLogBreakdownItem, 0, len(modelMap)),
		Keys:             make([]GatewayRequestLogBreakdownItem, 0, len(keyMap)),
		CostSources:      make([]GatewayRequestLogBreakdownItem, 0, len(costSourceMap)),
		RouteReasons:     make([]GatewayRequestLogBreakdownItem, 0, len(routeReasonMap)),
		GroupCacheRatios: []GatewayRequestLogGroupCacheRatio{},
	}
	for bucket, metrics := range trendMap {
		out.Trend = append(out.Trend, metrics.trend(bucket))
	}
	for name, metrics := range modelMap {
		item := metrics.breakdown(name, 0, 0)
		out.Models = append(out.Models, item)
		out.ModelsByTokens = append(out.ModelsByTokens, item)
	}
	for id, metrics := range keyMap {
		name := keyNames[id]
		if name == "" {
			name = "未知 Key"
		}
		out.Keys = append(out.Keys, metrics.breakdown(name, 0, id))
	}
	for name, metrics := range costSourceMap {
		out.CostSources = append(out.CostSources, metrics.breakdown(name, 0, 0))
	}
	for name, metrics := range routeReasonMap {
		out.RouteReasons = append(out.RouteReasons, metrics.breakdown(name, 0, 0))
	}
	for key, value := range unitPriceMap {
		parts := strings.Split(key, "\x00")
		if len(parts) != 3 {
			continue
		}
		out.ModelUnitPrices = appendGatewayUnitPricePoints(out.ModelUnitPrices, parts[0], parts[1], parts[2], value.gatewayRequestUnitPriceContribution)
	}
	sort.Slice(out.Trend, func(i, j int) bool { return out.Trend[i].Bucket < out.Trend[j].Bucket })
	sort.Slice(out.ModelUnitPrices, func(i, j int) bool {
		if out.ModelUnitPrices[i].Bucket != out.ModelUnitPrices[j].Bucket {
			return out.ModelUnitPrices[i].Bucket < out.ModelUnitPrices[j].Bucket
		}
		if out.ModelUnitPrices[i].ModelName != out.ModelUnitPrices[j].ModelName {
			return out.ModelUnitPrices[i].ModelName < out.ModelUnitPrices[j].ModelName
		}
		return out.ModelUnitPrices[i].TokenType < out.ModelUnitPrices[j].TokenType
	})
	sortBreakdowns(out.Channels, false)
	sortBreakdowns(out.Models, false)
	sortBreakdowns(out.ModelsByTokens, true)
	sortBreakdowns(out.Keys, false)
	sortBreakdowns(out.CostSources, false)
	sortBreakdowns(out.RouteReasons, false)
	out.Channels = limitBreakdowns(out.Channels, limit)
	out.Models = limitBreakdowns(out.Models, limit)
	out.ModelsByTokens = limitBreakdowns(out.ModelsByTokens, limit)
	out.Keys = limitBreakdowns(out.Keys, limit)
	out.CostSources = limitBreakdowns(out.CostSources, limit)
	out.RouteReasons = limitBreakdowns(out.RouteReasons, limit)
	return out, true, nil
}

func (g *Gateway) requestLogChannelCostsFromAggregates(filter GatewayRequestLogFilter) (map[uint]float64, bool, error) {
	rows, used, err := g.requestAggregateRows(filter)
	if errors.Is(err, errGatewayRequestAggregatesNotReady) {
		costs, rawErr := g.requestLogChannelCostsRaw(filter)
		return costs, true, rawErr
	}
	if errors.Is(err, errGatewayRequestAggregateReadLimit) {
		costs, groupedErr := g.requestLogChannelCostsFromAggregateSQL(filter)
		return costs, true, groupedErr
	}
	if err != nil || !used {
		return nil, used, err
	}
	costs := map[uint]float64{}
	for index := range rows {
		if rows[index].ChannelID > 0 && rows[index].Result != gatewayRequestResultDispatchEvidence && rows[index].Requests > 0 {
			costs[rows[index].ChannelID] += rows[index].TotalCost
		}
	}
	return costs, true, nil
}

func (g *Gateway) requestLogChannelCostsFromAggregateSQL(filter GatewayRequestLogFilter) (map[uint]float64, error) {
	costs := map[uint]float64{}
	if err := g.visitRequestAggregateChannelMetrics(filter, func(row requestAggregateMetricsScan) error {
		if row.GroupID > 0 {
			costs[row.GroupID] += row.TotalCost
		}
		return nil
	}); err != nil {
		return nil, err
	}
	_, edges := g.requestAggregateInteriorQuery(filter)
	for _, edge := range edges {
		edgeFilter := filter
		edgeFilter.Start = &edge.start
		edgeFilter.End = &edge.end
		edgeCosts, err := g.requestLogChannelCostsRaw(edgeFilter)
		if err != nil {
			return nil, err
		}
		for channelID, cost := range edgeCosts {
			costs[channelID] += cost
		}
	}
	return costs, nil
}

type gatewayRequestAggregateChannelKey struct {
	ChannelID           uint
	ChannelNameSnapshot string
}

func requestAggregateChannelKey(channelID uint, snapshot string) gatewayRequestAggregateChannelKey {
	return gatewayRequestAggregateChannelKey{ChannelID: channelID, ChannelNameSnapshot: strings.TrimSpace(snapshot)}
}

func metricsForRequestAggregateChannel(values map[gatewayRequestAggregateChannelKey]*aggregateMetrics, key gatewayRequestAggregateChannelKey) *aggregateMetrics {
	if values[key] == nil {
		values[key] = &aggregateMetrics{}
	}
	return values[key]
}

func (g *Gateway) requestAggregateChannelBreakdowns(values map[gatewayRequestAggregateChannelKey]*aggregateMetrics) ([]GatewayRequestLogBreakdownItem, error) {
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
	items := make([]GatewayRequestLogBreakdownItem, 0, len(values))
	for key, metrics := range values {
		name := key.ChannelNameSnapshot
		if name == "" {
			name = names[key.ChannelID]
		}
		if name == "" {
			name = "未知渠道"
		}
		items = append(items, metrics.breakdown(name, key.ChannelID, 0))
	}
	return items, nil
}

func metricsFor(values map[string]*aggregateMetrics, key string) *aggregateMetrics {
	if values[key] == nil {
		values[key] = &aggregateMetrics{}
	}
	return values[key]
}

func metricsForUint(values map[uint]*aggregateMetrics, key uint) *aggregateMetrics {
	if values[key] == nil {
		values[key] = &aggregateMetrics{}
	}
	return values[key]
}

func normalizedAggregateModel(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "未知模型"
}

func normalizedAggregateCostSource(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "未提供费用来源"
}

func sortBreakdowns(items []GatewayRequestLogBreakdownItem, byTokens bool) {
	sort.Slice(items, func(i, j int) bool {
		if byTokens && items[i].TotalTokens != items[j].TotalTokens {
			return items[i].TotalTokens > items[j].TotalTokens
		}
		if !byTokens && items[i].TotalCost != items[j].TotalCost {
			return items[i].TotalCost > items[j].TotalCost
		}
		if items[i].Requests != items[j].Requests {
			return items[i].Requests > items[j].Requests
		}
		return items[i].Name < items[j].Name
	})
}

func limitBreakdowns(items []GatewayRequestLogBreakdownItem, limit int) []GatewayRequestLogBreakdownItem {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func (g *Gateway) requestAggregateChannelNames(values map[uint]struct{}) (map[uint]string, error) {
	ids := make([]uint, 0, len(values))
	for id := range values {
		if id > 0 {
			ids = append(ids, id)
		}
	}
	out := make(map[uint]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	var channels []Channel
	if err := g.db.Select("id", "name").Where("id IN ?", ids).Find(&channels).Error; err != nil {
		return nil, err
	}
	for _, channel := range channels {
		out[channel.ID] = channel.Name
	}
	return out, nil
}

func (g *Gateway) requestAggregateKeyNames(values map[uint]*aggregateMetrics) (map[uint]string, error) {
	ids := make([]uint, 0, len(values))
	for id := range values {
		if id > 0 {
			ids = append(ids, id)
		}
	}
	out := make(map[uint]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	var keys []GatewayDownstreamKey
	if err := g.db.Select("id", "name").Where("id IN ?", ids).Find(&keys).Error; err != nil {
		return nil, err
	}
	for _, key := range keys {
		out[key.ID] = key.Name
	}
	return out, nil
}

func (g *Gateway) summarizeUsageFromAggregates(downstreamKeyID uint) (*GatewayUsageSummary, bool, error) {
	ready, err := requestAggregatesReady(g.db)
	if err != nil {
		return nil, true, err
	}
	if !ready {
		return nil, false, nil
	}
	var row struct {
		TotalRequests       int64
		Success             int64
		LatencyTotalMs      int64
		LatencySamples      int64
		LastUsedAt          string
		TotalTokens         int64
		PromptTokens        int64
		CompletionTokens    int64
		TotalCost           float64
		EstimatedCost       float64
		ReportedCost        float64
		ReconciledCost      float64
		ActualCost          float64
		UnavailableRequests int64
	}
	err = g.db.Model(&GatewayRequestAggregate{}).
		Select(`COALESCE(SUM(requests), 0) AS total_requests,
			COALESCE(SUM(CASE WHEN result = '`+GatewayRequestResultSuccess+`' THEN requests ELSE 0 END), 0) AS success,
			COALESCE(SUM(latency_total_ms), 0) AS latency_total_ms,
			COALESCE(SUM(latency_samples), 0) AS latency_samples,
			MAX(last_request_at) AS last_used_at,
			COALESCE(SUM(total_tokens), 0) AS total_tokens,
			COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
			COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
			COALESCE(SUM(total_cost), 0) AS total_cost,
			COALESCE(SUM(estimated_cost), 0) AS estimated_cost,
			COALESCE(SUM(reported_cost), 0) AS reported_cost,
			COALESCE(SUM(reconciled_cost), 0) AS reconciled_cost,
			COALESCE(SUM(actual_cost), 0) AS actual_cost,
			COALESCE(SUM(unavailable_requests), 0) AS unavailable_requests`).
		Where("downstream_key_id = ? AND version = ? AND requests > 0 AND COALESCE(route_reason, '') <> ?", downstreamKeyID, gatewayRequestAggregateVersion, gatewayRouteReasonLatencyProbe).
		Scan(&row).Error
	if err != nil {
		return nil, true, err
	}
	var lastUsedAt *time.Time
	if parsed, ok := g.parseRequestLogAggregateTime(row.LastUsedAt); ok {
		lastUsedAt = &parsed
	}
	avgLatency := 0.0
	if row.LatencySamples > 0 {
		avgLatency = float64(row.LatencyTotalMs) / float64(row.LatencySamples)
	}
	failed := row.TotalRequests - row.Success
	if failed < 0 {
		failed = 0
	}
	unit := "requests"
	used := float64(row.TotalRequests)
	total := float64(row.TotalRequests)
	if row.TotalCost > 0 {
		unit = "estimated_usd"
		if row.ActualCost > 0 && row.EstimatedCost == 0 {
			unit = "USD"
		}
		used = row.TotalCost
		total = row.TotalCost
	} else if row.TotalTokens > 0 {
		unit = "tokens"
		used = float64(row.TotalTokens)
		total = float64(row.TotalTokens)
	}
	return &GatewayUsageSummary{
		IsActive: true, Used: used, Total: total, Remaining: 0, Unit: unit,
		Requests: row.TotalRequests, Success: row.Success, Failed: failed,
		AvgLatency: avgLatency, LastUsedAt: lastUsedAt,
		TotalTokens: row.TotalTokens, PromptTokens: row.PromptTokens, CompletionTokens: row.CompletionTokens,
		TotalCost: row.TotalCost, ActualCost: row.ActualCost, EstimatedCost: row.EstimatedCost,
		ReportedCost: row.ReportedCost, ReconciledCost: row.ReconciledCost,
		UnavailableRequests: row.UnavailableRequests,
		Extra:               "requests, tokens, and cost totals",
	}, true, nil
}

func (g *Gateway) summarizeDownstreamKeyUsageFromAggregates(ids []uint, now time.Time) (map[uint]GatewayDownstreamKeyUsage, bool, error) {
	result := make(map[uint]GatewayDownstreamKeyUsage, len(ids))
	if len(ids) == 0 {
		return result, true, nil
	}
	ready, err := requestAggregatesReady(g.db)
	if err != nil {
		return nil, true, err
	}
	if !ready {
		usage, rawErr := g.downstreamKeyUsageDuringAggregateMigration(ids, now)
		return usage, true, rawErr
	}
	localNow := now.In(now.Location())
	dayStart := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, now.Location()).UTC()
	if !aggregateBucketStart(dayStart).Equal(dayStart) {
		return nil, false, nil
	}
	type usageRow struct {
		DownstreamKeyID          uint
		TotalCost                float64
		TodayCost                float64
		TotalRequests            int64
		TodayRequests            int64
		TotalTokens              int64
		TodayTokens              int64
		ActualCost               float64
		EstimatedCost            float64
		ReportedCost             float64
		ReconciledCost           float64
		UnavailableRequests      int64
		TodayUnavailableRequests int64
	}
	var rows []usageRow
	if err := g.db.Model(&GatewayRequestAggregate{}).
		Select(`downstream_key_id,
			SUM(total_cost) AS total_cost,
			SUM(CASE WHEN bucket_start >= ? THEN total_cost ELSE 0 END) AS today_cost,
			SUM(requests) AS total_requests,
			SUM(CASE WHEN bucket_start >= ? THEN requests ELSE 0 END) AS today_requests,
			SUM(total_tokens) AS total_tokens,
			SUM(CASE WHEN bucket_start >= ? THEN total_tokens ELSE 0 END) AS today_tokens,
			SUM(actual_cost) AS actual_cost,
			SUM(estimated_cost) AS estimated_cost,
			SUM(reported_cost) AS reported_cost,
			SUM(reconciled_cost) AS reconciled_cost,
			SUM(unavailable_requests) AS unavailable_requests,
			SUM(CASE WHEN bucket_start >= ? THEN unavailable_requests ELSE 0 END) AS today_unavailable_requests`, dayStart, dayStart, dayStart, dayStart).
		Where("downstream_key_id IN ? AND version = ? AND requests > 0", ids, gatewayRequestAggregateVersion).
		Group("downstream_key_id").Scan(&rows).Error; err != nil {
		return nil, true, err
	}
	for _, row := range rows {
		result[row.DownstreamKeyID] = GatewayDownstreamKeyUsage{
			TotalCost: row.TotalCost, TodayCost: row.TodayCost,
			TotalRequests: row.TotalRequests, TodayRequests: row.TodayRequests,
			TotalTokens: row.TotalTokens, TodayTokens: row.TodayTokens,
			ActualCost: row.ActualCost, EstimatedCost: row.EstimatedCost,
			ReportedCost: row.ReportedCost, ReconciledCost: row.ReconciledCost,
			UnavailableRequests:      row.UnavailableRequests,
			TodayUnavailableRequests: row.TodayUnavailableRequests,
		}
	}
	return result, true, nil
}
