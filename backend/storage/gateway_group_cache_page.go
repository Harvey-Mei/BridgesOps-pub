package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
)

const (
	gatewayRequestGroupCachePageDefaultSize = 50
	gatewayRequestGroupCachePageMaxSize     = 100
	gatewayRequestGroupCacheCursorVersion   = 1
	gatewayRequestGroupCacheCursorMaxLength = 1024
)

var (
	ErrGatewayRequestGroupCachePageInvalid   = errors.New("gateway request group cache page is invalid")
	ErrGatewayRequestGroupCacheCursorInvalid = errors.New("gateway request group cache cursor is invalid")
)

// GatewayRequestLogGroupCacheRatioCursorPage is one bounded keyset page. The
// opaque cursor is tied to the normalized filter and page size, so callers
// cannot accidentally continue a previous result set after changing either.
type GatewayRequestLogGroupCacheRatioCursorPage struct {
	Items      []GatewayRequestLogGroupCacheRatio `json:"items"`
	NextCursor string                             `json:"next_cursor,omitempty"`
	PageSize   int                                `json:"page_size"`
}

type gatewayRequestGroupCacheCursor struct {
	Version   int    `json:"v"`
	Scope     string `json:"s"`
	ChannelID uint   `json:"c"`
	GroupName string `json:"g"`
}

type gatewayRequestGroupCachePageKey struct {
	ChannelID uint
	GroupName string
}

type gatewayRequestGroupCachePageScan struct {
	ChannelID           uint
	ChannelNameSnapshot string
	GroupName           string
	CacheReadTokens     int64
	CacheCreationTokens int64
	PromptTokens        int64
	ValidRequests       int64
}

type gatewayRequestGroupCachePageValue struct {
	channelNameSnapshot string
	metrics             gatewayRequestGroupCacheMetrics
}

// ListRequestLogGroupCacheRatioCursorPage reads at most page_size+1 grouped
// rows from each source. A ready ledger contributes the complete fifteen-minute
// buckets while at most two raw-log edge windows cover partial buckets. During
// a rebuild, one bounded raw grouped query remains available for compatibility.
func (g *Gateway) ListRequestLogGroupCacheRatioCursorPage(ctx context.Context, filter GatewayRequestLogFilter, cursor string, pageSize int) (*GatewayRequestLogGroupCacheRatioCursorPage, error) {
	pageSize, err := normalizeGatewayRequestGroupCachePageSize(pageSize)
	if err != nil {
		return nil, err
	}
	parsedCursor, err := parseGatewayRequestGroupCacheCursor(cursor, filter, pageSize)
	if err != nil {
		return nil, err
	}
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	sources, err := g.requestLogGroupCachePageSources(ctx, filter, parsedCursor, pageSize+1)
	if err != nil {
		return nil, err
	}
	values := make(map[gatewayRequestGroupCachePageKey]*gatewayRequestGroupCachePageValue, (pageSize+1)*len(sources))
	for _, rows := range sources {
		for _, row := range rows {
			if row.ValidRequests <= 0 {
				continue
			}
			key := gatewayRequestGroupCachePageKey{ChannelID: row.ChannelID, GroupName: strings.TrimSpace(row.GroupName)}
			value := values[key]
			if value == nil {
				value = &gatewayRequestGroupCachePageValue{}
				values[key] = value
			}
			snapshot := strings.TrimSpace(row.ChannelNameSnapshot)
			if snapshot > value.channelNameSnapshot {
				value.channelNameSnapshot = snapshot
			}
			value.metrics.merge(gatewayRequestGroupCacheMetrics{
				CacheReadTokens:     row.CacheReadTokens,
				CacheCreationTokens: row.CacheCreationTokens,
				PromptTokens:        row.PromptTokens,
				ValidRequests:       row.ValidRequests,
			})
		}
	}

	keys := make([]gatewayRequestGroupCachePageKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].ChannelID != keys[j].ChannelID {
			return keys[i].ChannelID < keys[j].ChannelID
		}
		return keys[i].GroupName < keys[j].GroupName
	})
	hasMore := len(keys) > pageSize
	if hasMore {
		keys = keys[:pageSize]
	}

	channelIDs := make(map[uint]struct{}, len(keys))
	for _, key := range keys {
		if key.ChannelID > 0 {
			channelIDs[key.ChannelID] = struct{}{}
		}
	}
	channelNames, err := g.requestAggregateChannelNames(channelIDs)
	if err != nil {
		return nil, err
	}
	page := &GatewayRequestLogGroupCacheRatioCursorPage{
		Items:    make([]GatewayRequestLogGroupCacheRatio, 0, len(keys)),
		PageSize: pageSize,
	}
	for _, key := range keys {
		value := values[key]
		if value == nil || value.metrics.ValidRequests <= 0 {
			continue
		}
		channelName := strings.TrimSpace(channelNames[key.ChannelID])
		if channelName == "" {
			channelName = value.channelNameSnapshot
		}
		if channelName == "" {
			channelName = "未知渠道"
		}
		item := GatewayRequestLogGroupCacheRatio{
			ChannelID: key.ChannelID, ChannelName: channelName, GroupName: key.GroupName,
			CacheReadTokens: value.metrics.CacheReadTokens, CacheCreationTokens: value.metrics.CacheCreationTokens,
			PromptTokens: value.metrics.PromptTokens, ValidRequests: value.metrics.ValidRequests,
		}
		if value.metrics.PromptTokens > 0 {
			ratio := float64(value.metrics.CacheReadTokens) / float64(value.metrics.PromptTokens)
			item.Ratio = &ratio
		}
		page.Items = append(page.Items, item)
	}
	if hasMore && len(keys) > 0 {
		page.NextCursor, err = encodeGatewayRequestGroupCacheCursor(filter, pageSize, keys[len(keys)-1])
		if err != nil {
			return nil, err
		}
	}
	return page, nil
}

func normalizeGatewayRequestGroupCachePageSize(pageSize int) (int, error) {
	if pageSize <= 0 {
		return gatewayRequestGroupCachePageDefaultSize, nil
	}
	if pageSize > gatewayRequestGroupCachePageMaxSize {
		return 0, fmt.Errorf("%w: page_size", ErrGatewayRequestGroupCachePageInvalid)
	}
	return pageSize, nil
}

func gatewayRequestGroupCacheCursorScope(filter GatewayRequestLogFilter, pageSize int) string {
	start, end := "", ""
	if filter.Start != nil {
		start = filter.Start.UTC().Format(time.RFC3339Nano)
	}
	if filter.End != nil {
		end = filter.End.UTC().Format(time.RFC3339Nano)
	}
	payload, _ := json.Marshal([]any{
		start, end, strings.TrimSpace(filter.TimeZone), filter.DownstreamKeyID,
		strings.TrimSpace(filter.ModelName), filter.ChannelID, strings.TrimSpace(filter.GroupName), filter.GroupUnassigned,
		filter.UpstreamRequestID, strings.TrimSpace(filter.UpstreamKeyName), strings.ToLower(strings.TrimSpace(filter.Result)),
		strings.TrimSpace(filter.RouteReason), pageSize,
	})
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func encodeGatewayRequestGroupCacheCursor(filter GatewayRequestLogFilter, pageSize int, key gatewayRequestGroupCachePageKey) (string, error) {
	payload, err := json.Marshal(gatewayRequestGroupCacheCursor{
		Version: gatewayRequestGroupCacheCursorVersion,
		Scope:   gatewayRequestGroupCacheCursorScope(filter, pageSize), ChannelID: key.ChannelID, GroupName: key.GroupName,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func parseGatewayRequestGroupCacheCursor(value string, filter GatewayRequestLogFilter, pageSize int) (*gatewayRequestGroupCacheCursor, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if len(value) > gatewayRequestGroupCacheCursorMaxLength {
		return nil, fmt.Errorf("%w: length", ErrGatewayRequestGroupCacheCursorInvalid)
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(payload) == 0 || len(payload) > gatewayRequestGroupCacheCursorMaxLength {
		return nil, fmt.Errorf("%w: encoding", ErrGatewayRequestGroupCacheCursorInvalid)
	}
	var cursor gatewayRequestGroupCacheCursor
	if err := json.Unmarshal(payload, &cursor); err != nil || cursor.Version != gatewayRequestGroupCacheCursorVersion ||
		cursor.Scope != gatewayRequestGroupCacheCursorScope(filter, pageSize) || !utf8.ValidString(cursor.GroupName) || len(cursor.GroupName) > 128 {
		return nil, fmt.Errorf("%w: value", ErrGatewayRequestGroupCacheCursorInvalid)
	}
	cursor.GroupName = strings.TrimSpace(cursor.GroupName)
	return &cursor, nil
}

func (g *Gateway) requestLogGroupCachePageSources(ctx context.Context, filter GatewayRequestLogFilter, cursor *gatewayRequestGroupCacheCursor, limit int) ([][]gatewayRequestGroupCachePageScan, error) {
	if groupCacheAggregateFilterSupported(filter) {
		ready, err := groupCacheRoutingStatsReady(g.db)
		if err != nil {
			return nil, err
		}
		if ready {
			return g.requestLogGroupCacheAggregatePageSources(ctx, filter, cursor, limit)
		}
	}
	rows, err := g.requestLogGroupCacheRawPageScan(ctx, filter, cursor, limit)
	if err != nil {
		return nil, err
	}
	return [][]gatewayRequestGroupCachePageScan{rows}, nil
}

func (g *Gateway) requestLogGroupCacheAggregatePageSources(ctx context.Context, filter GatewayRequestLogFilter, cursor *gatewayRequestGroupCacheCursor, limit int) ([][]gatewayRequestGroupCachePageScan, error) {
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
	query := g.db.WithContext(ctx).Table("gateway_request_group_cache_aggregates AS cache").
		Where("cache.version = ?", gatewayRequestGroupCacheAggregateVersion)
	if filter.DownstreamKeyID > 0 {
		query = query.Where("cache.downstream_key_id = ?", filter.DownstreamKeyID)
	}
	if filter.ChannelID > 0 {
		query = query.Where("cache.channel_id = ?", filter.ChannelID)
	}
	if modelName := strings.TrimSpace(filter.ModelName); modelName != "" {
		query = query.Where("cache.model_name = ?", modelName)
	}
	if groupName := strings.TrimSpace(filter.GroupName); groupName != "" {
		query = query.Where("TRIM(COALESCE(cache.group_name, '')) = ?", groupName)
	} else if filter.GroupUnassigned {
		query = query.Where("TRIM(COALESCE(cache.group_name, '')) = ''")
	}
	if useAggregateRange {
		if aggregateStart != nil {
			query = query.Where("cache.bucket_start >= ?", g.requestLogTimeBoundary(*aggregateStart))
		}
		if aggregateEnd != nil {
			query = query.Where("cache.bucket_start < ?", g.requestLogTimeBoundary(*aggregateEnd))
		}
	} else {
		query = query.Where("1 = 0")
	}
	aggregateRows, err := g.scanRequestLogGroupCachePage(ctx, query, "cache", true, cursor, limit)
	if err != nil {
		return nil, err
	}
	sources := make([][]gatewayRequestGroupCachePageScan, 0, 3)
	sources = append(sources, aggregateRows)
	for _, edge := range aggregateEdgeRanges(filter, aggregateStart, aggregateEnd) {
		edgeFilter := filter
		edgeFilter.Start = &edge.start
		edgeFilter.End = &edge.end
		rows, edgeErr := g.requestLogGroupCacheRawPageScan(ctx, edgeFilter, cursor, limit)
		if edgeErr != nil {
			return nil, edgeErr
		}
		sources = append(sources, rows)
	}
	return sources, nil
}

func (g *Gateway) requestLogGroupCacheRawPageScan(ctx context.Context, filter GatewayRequestLogFilter, cursor *gatewayRequestGroupCacheCursor, limit int) ([]gatewayRequestGroupCachePageScan, error) {
	query := g.applyRequestLogFilter(g.db.WithContext(ctx).Table("gateway_request_logs AS logs"), "logs", filter)
	return g.scanRequestLogGroupCachePage(ctx, query, "logs", false, cursor, limit)
}

func (g *Gateway) scanRequestLogGroupCachePage(ctx context.Context, query *gorm.DB, alias string, aggregate bool, cursor *gatewayRequestGroupCacheCursor, limit int) ([]gatewayRequestGroupCachePageScan, error) {
	groupExpr := "TRIM(COALESCE(" + alias + ".group_name, ''))"
	groupOrderExpr := groupExpr
	if g.db.Dialector.Name() == "postgres" {
		groupOrderExpr += ` COLLATE "C"`
	}
	query = query.WithContext(ctx)
	if aggregate {
		query = query.Where("COALESCE(" + alias + ".valid_requests, 0) > 0")
	} else {
		query = query.Where(normalDownstreamCachePriceRequestSQL(alias))
	}
	if cursor != nil {
		query = query.Where(
			fmt.Sprintf("(%s.channel_id > ? OR (%s.channel_id = ? AND %s > ?))", alias, alias, groupOrderExpr),
			cursor.ChannelID, cursor.ChannelID, cursor.GroupName,
		)
	}
	validRequestsExpr := "COUNT(*)"
	if aggregate {
		validRequestsExpr = "COALESCE(SUM(GREATEST(COALESCE(" + alias + ".valid_requests, 0), 0)), 0)"
	}
	cacheReadTokensExpr := "COALESCE(SUM(GREATEST(COALESCE(" + alias + ".cache_read_tokens, 0), 0)), 0)"
	cacheCreationTokensExpr := "COALESCE(SUM(GREATEST(COALESCE(" + alias + ".cache_creation_tokens, 0), 0)), 0)"
	if !aggregate {
		cacheReadTokensExpr = "COALESCE(SUM(CASE WHEN COALESCE(" + alias + ".prompt_tokens, 0) > 0 THEN GREATEST(COALESCE(" + alias + ".cache_read_tokens, 0), 0) ELSE 0 END), 0)"
		cacheCreationTokensExpr = "COALESCE(SUM(CASE WHEN COALESCE(" + alias + ".prompt_tokens, 0) > 0 THEN GREATEST(COALESCE(" + alias + ".cache_creation_tokens, 0), 0) ELSE 0 END), 0)"
	}
	selectSQL := fmt.Sprintf(`%s.channel_id AS channel_id,
			COALESCE(MAX(NULLIF(TRIM(COALESCE(%s.channel_name_snapshot, '')), '')), '') AS channel_name_snapshot,
			%s AS group_name,
			%s AS cache_read_tokens,
			%s AS cache_creation_tokens,
			COALESCE(SUM(GREATEST(COALESCE(%s.prompt_tokens, 0), 0)), 0) AS prompt_tokens,
			%s AS valid_requests`, alias, alias, groupExpr, cacheReadTokensExpr, cacheCreationTokensExpr, alias, validRequestsExpr)
	rows := make([]gatewayRequestGroupCachePageScan, 0, limit)
	if err := query.Select(selectSQL).
		Group(alias + ".channel_id, " + groupExpr).
		Order(alias + ".channel_id ASC").
		Order(groupOrderExpr + " ASC").
		Limit(limit).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}
