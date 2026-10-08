package storage

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
)

const (
	gatewayUpstreamGroupChangeHistorySearchLimit   = 200
	gatewayUpstreamGroupChangeHistoryFacetLimit    = 500
	gatewayUpstreamGroupChangeHistoryPageLimit     = 1_000_000
	gatewayUpstreamGroupChangeHistoryPageSize      = 50
	gatewayUpstreamGroupChangeHistoryPageMaxSize   = 100
	gatewayUpstreamGroupChangeHistoryCursorLimit   = 512
	gatewayUpstreamGroupChangeHistoryCursorV1      = 1
	gatewayUpstreamGroupChangeHistoryFacetCacheTTL = 30 * time.Second
	gatewayUpstreamGroupChangeHistoryFacetCacheMax = 4
)

var (
	ErrGatewayUpstreamGroupChangeHistoryFilterInvalid = errors.New("upstream group change history filter is invalid")
	ErrGatewayUpstreamGroupChangeHistoryCursorInvalid = errors.New("upstream group change history cursor is invalid")
)

type GatewayUpstreamGroupChangeHistoryFilter struct {
	View    string
	Search  string
	Channel string
	Group   string
	Source  string
}

type GatewayUpstreamGroupChangeHistoryFilterOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

type GatewayUpstreamGroupChangeHistoryFilterOptions struct {
	Channels  []GatewayUpstreamGroupChangeHistoryFilterOption `json:"channels"`
	Groups    []GatewayUpstreamGroupChangeHistoryFilterOption `json:"groups"`
	Sources   []GatewayUpstreamGroupChangeHistoryFilterOption `json:"sources"`
	Truncated bool                                            `json:"truncated,omitempty"`
}

// GatewayUpstreamGroupChangeHistoryCursorPage is one bounded keyset page for
// the administrative history. NextCursor is scoped to the exact normalized
// filter, so a stale cursor cannot be accidentally reused after a filter edit.
// The cursor is intentionally opaque to API consumers.
type GatewayUpstreamGroupChangeHistoryCursorPage struct {
	Items         []GatewayUpstreamGroupChange                   `json:"items"`
	NextCursor    string                                         `json:"next_cursor,omitempty"`
	FilterOptions GatewayUpstreamGroupChangeHistoryFilterOptions `json:"filter_options"`
}

func (g *Gateway) CountPendingUpstreamGroupChangeManualReviews() (int64, error) {
	if g == nil || g.db == nil {
		return 0, errors.New("gateway storage is required")
	}
	var count int64
	err := g.db.Model(&GatewayUpstreamGroupChange{}).
		Where(
			"status = ? AND kind = ? AND assignment_reason_code = ? AND removal_manual_review_resolved_at IS NULL",
			"ignored", "removed", "manual_review_required",
		).
		Count(&count).Error
	return count, err
}

func (g *Gateway) ListPendingUpstreamGroupChangeManualReviews() ([]GatewayUpstreamGroupChange, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	var changes []GatewayUpstreamGroupChange
	err := g.db.Where(
		"status = ? AND kind = ? AND assignment_reason_code = ? AND removal_manual_review_resolved_at IS NULL",
		"ignored", "removed", "manual_review_required",
	).Order("detected_at ASC, id ASC").Find(&changes).Error
	return changes, err
}

type parsedGatewayUpstreamGroupChangeHistoryFilter struct {
	view            string
	search          string
	channelID       uint
	upstreamGroupID uint
	groupName       string
	upstreamKeyID   int64
	sourceName      string
	unknownSource   bool
}

type gatewayUpstreamGroupChangeHistoryCursor struct {
	Version    int       `json:"v"`
	Scope      string    `json:"s"`
	DetectedAt time.Time `json:"t"`
	ID         uint      `json:"i"`
}

// gatewayUpstreamGroupChangeHistoryFacetCacheEntry is deliberately scoped to
// the bounded set of supported history views. Facets do not depend on the page
// cursor or other filters, so keeping one short-lived snapshot per view avoids
// three full-history GROUP BY scans on every page navigation.
type gatewayUpstreamGroupChangeHistoryFacetCacheEntry struct {
	expiresAt time.Time
	options   GatewayUpstreamGroupChangeHistoryFilterOptions
}

// ListUpstreamGroupChangesHistoryFilteredCursorPage returns exactly one
// server-filtered history page. It is deliberately keyset based rather than
// offset based: long histories stay bounded in database work and the browser
// never needs to fetch preceding pages to render the current page.
func (g *Gateway) ListUpstreamGroupChangesHistoryFilteredCursorPage(filter GatewayUpstreamGroupChangeHistoryFilter, cursor string, pageSize int) (*GatewayUpstreamGroupChangeHistoryCursorPage, error) {
	pageSize, err := normalizeGatewayUpstreamGroupChangeHistoryPageSize(pageSize)
	if err != nil {
		return nil, err
	}
	parsed, err := parseGatewayUpstreamGroupChangeHistoryFilter(filter)
	if err != nil {
		return nil, err
	}
	parsedCursor, err := parseGatewayUpstreamGroupChangeHistoryCursor(cursor, parsed)
	if err != nil {
		return nil, err
	}
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}

	query := gatewayUpstreamGroupChangeHistoryQuery(g.db, parsed)
	if parsedCursor != nil {
		query = gatewayUpstreamGroupChangeHistoryAfterCursor(query, *parsedCursor)
	}
	changes := make([]GatewayUpstreamGroupChange, 0, pageSize+1)
	if err := query.Select("changes.*").
		Order("changes.detected_at DESC, changes.id DESC").
		Limit(pageSize + 1).
		Find(&changes).Error; err != nil {
		return nil, err
	}

	page := &GatewayUpstreamGroupChangeHistoryCursorPage{Items: []GatewayUpstreamGroupChange{}}
	if len(changes) > pageSize {
		changes = changes[:pageSize]
		page.NextCursor, err = encodeGatewayUpstreamGroupChangeHistoryCursor(parsed, changes[len(changes)-1])
		if err != nil {
			return nil, err
		}
	}
	page.Items, err = g.withUpstreamGroupChangeChannelNames(changes)
	if err != nil {
		return nil, err
	}
	if err := g.withUpstreamGroupChangeAssignmentAudits(page.Items); err != nil {
		return nil, err
	}
	page.FilterOptions, err = g.listUpstreamGroupChangeHistoryFilterOptions(parsed.view, gatewayUpstreamGroupChangeHistoryFacetLimit)
	if err != nil {
		return nil, err
	}
	return page, nil
}

func normalizeGatewayUpstreamGroupChangeHistoryPageSize(pageSize int) (int, error) {
	if pageSize <= 0 {
		return gatewayUpstreamGroupChangeHistoryPageSize, nil
	}
	if pageSize > gatewayUpstreamGroupChangeHistoryPageMaxSize {
		return 0, fmt.Errorf("%w: page_size", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
	}
	return pageSize, nil
}

func gatewayUpstreamGroupChangeHistoryCursorScope(filter parsedGatewayUpstreamGroupChangeHistoryFilter) string {
	payload, _ := json.Marshal([]any{
		filter.view, filter.search, filter.channelID, filter.upstreamGroupID, filter.groupName,
		filter.upstreamKeyID, filter.sourceName, filter.unknownSource,
	})
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func encodeGatewayUpstreamGroupChangeHistoryCursor(filter parsedGatewayUpstreamGroupChangeHistoryFilter, change GatewayUpstreamGroupChange) (string, error) {
	if change.ID == 0 || change.DetectedAt.IsZero() {
		return "", fmt.Errorf("%w: boundary", ErrGatewayUpstreamGroupChangeHistoryCursorInvalid)
	}
	payload, err := json.Marshal(gatewayUpstreamGroupChangeHistoryCursor{
		Version: gatewayUpstreamGroupChangeHistoryCursorV1,
		Scope:   gatewayUpstreamGroupChangeHistoryCursorScope(filter), DetectedAt: change.DetectedAt.UTC(), ID: change.ID,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func parseGatewayUpstreamGroupChangeHistoryCursor(value string, filter parsedGatewayUpstreamGroupChangeHistoryFilter) (*gatewayUpstreamGroupChangeHistoryCursor, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if len(value) > gatewayUpstreamGroupChangeHistoryCursorLimit {
		return nil, fmt.Errorf("%w: length", ErrGatewayUpstreamGroupChangeHistoryCursorInvalid)
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(payload) == 0 || len(payload) > gatewayUpstreamGroupChangeHistoryCursorLimit {
		return nil, fmt.Errorf("%w: encoding", ErrGatewayUpstreamGroupChangeHistoryCursorInvalid)
	}
	var cursor gatewayUpstreamGroupChangeHistoryCursor
	if err := json.Unmarshal(payload, &cursor); err != nil || cursor.Version != gatewayUpstreamGroupChangeHistoryCursorV1 ||
		cursor.ID == 0 || cursor.DetectedAt.IsZero() || cursor.Scope != gatewayUpstreamGroupChangeHistoryCursorScope(filter) {
		return nil, fmt.Errorf("%w: value", ErrGatewayUpstreamGroupChangeHistoryCursorInvalid)
	}
	cursor.DetectedAt = cursor.DetectedAt.UTC()
	return &cursor, nil
}

func gatewayUpstreamGroupChangeHistoryAfterCursor(query *gorm.DB, cursor gatewayUpstreamGroupChangeHistoryCursor) *gorm.DB {
	return query.Where(`(changes.detected_at < ? OR (changes.detected_at = ? AND changes.id < ?))`, cursor.DetectedAt, cursor.DetectedAt, cursor.ID)
}

// ListUpstreamGroupChangesHistoryFilteredPage returns one stable, bounded page
// and bounded facet values. No caller needs to load preceding or subsequent
// pages to search or filter history.
//
// Deprecated: new API consumers must call
// ListUpstreamGroupChangesHistoryFilteredCursorPage. This offset method only
// remains temporarily for compatibility with older administrative clients.
func (g *Gateway) ListUpstreamGroupChangesHistoryFilteredPage(filter GatewayUpstreamGroupChangeHistoryFilter, page, pageSize int) ([]GatewayUpstreamGroupChange, int64, GatewayUpstreamGroupChangeHistoryFilterOptions, error) {
	if page < 1 {
		page = 1
	}
	if page > gatewayUpstreamGroupChangeHistoryPageLimit {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, fmt.Errorf("%w: page", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
	}
	pageSize, err := normalizeGatewayUpstreamGroupChangeHistoryPageSize(pageSize)
	if err != nil {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, err
	}
	if g == nil || g.db == nil {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, errors.New("gateway storage is required")
	}
	parsed, err := parseGatewayUpstreamGroupChangeHistoryFilter(filter)
	if err != nil {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, err
	}

	var total int64
	if err := gatewayUpstreamGroupChangeHistoryQuery(g.db, parsed).Distinct("changes.id").Count(&total).Error; err != nil {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, err
	}
	changes := []GatewayUpstreamGroupChange{}
	if err := gatewayUpstreamGroupChangeHistoryQuery(g.db, parsed).
		Select("changes.*").
		Order("changes.detected_at DESC, changes.id DESC").
		Limit(pageSize).
		Offset((page - 1) * pageSize).
		Find(&changes).Error; err != nil {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, err
	}
	changes, err = g.withUpstreamGroupChangeChannelNames(changes)
	if err != nil {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, err
	}
	if err := g.withUpstreamGroupChangeAssignmentAudits(changes); err != nil {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, err
	}
	options, err := g.listUpstreamGroupChangeHistoryFilterOptions(parsed.view, gatewayUpstreamGroupChangeHistoryFacetLimit)
	if err != nil {
		return nil, 0, GatewayUpstreamGroupChangeHistoryFilterOptions{}, err
	}
	return changes, total, options, nil
}

func (g *Gateway) withUpstreamGroupChangeAssignmentAudits(changes []GatewayUpstreamGroupChange) error {
	changeIDs := make([]uint, 0, len(changes))
	for _, change := range changes {
		if change.ID > 0 {
			changeIDs = append(changeIDs, change.ID)
		}
	}
	if len(changeIDs) == 0 {
		return nil
	}

	var audits []GatewayUpstreamGroupAssignmentAudit
	if err := g.db.Where("change_id IN ?", changeIDs).
		Order("created_at DESC, id DESC").
		Find(&audits).Error; err != nil {
		return err
	}
	latestByChangeID := make(map[uint]GatewayUpstreamGroupAssignmentAudit, len(changeIDs))
	for _, audit := range audits {
		if audit.ChangeID == nil {
			continue
		}
		if _, exists := latestByChangeID[*audit.ChangeID]; !exists {
			latestByChangeID[*audit.ChangeID] = audit
		}
	}
	for index := range changes {
		audit, exists := latestByChangeID[changes[index].ID]
		if !exists {
			continue
		}
		createdAt := audit.CreatedAt
		changes[index].AssignmentAuditAction = audit.Action
		changes[index].AssignmentAuditTrigger = audit.TriggerSource
		changes[index].AssignmentAuditActor = audit.Actor
		changes[index].AssignmentAuditTargetGroupIDs = append([]uint(nil), audit.TargetCandidateGroupIDs...)
		changes[index].AssignmentAuditPresenceGeneration = audit.PresenceGeneration
		changes[index].AssignmentAuditCreatedAt = &createdAt
	}
	return nil
}

func parseGatewayUpstreamGroupChangeHistoryFilter(filter GatewayUpstreamGroupChangeHistoryFilter) (parsedGatewayUpstreamGroupChangeHistoryFilter, error) {
	parsed := parsedGatewayUpstreamGroupChangeHistoryFilter{
		view:   strings.ToLower(strings.TrimSpace(filter.View)),
		search: strings.TrimSpace(filter.Search),
	}
	if parsed.view != "" && parsed.view != "ignored" && parsed.view != "resolved" && parsed.view != "review" {
		return parsed, fmt.Errorf("%w: view", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
	}
	if !utf8.ValidString(parsed.search) || utf8.RuneCountInString(parsed.search) > gatewayUpstreamGroupChangeHistorySearchLimit {
		return parsed, fmt.Errorf("%w: search", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
	}

	channel := normalizeGatewayUpstreamGroupChangeHistorySelection(filter.Channel)
	if channel != "" {
		value, err := strconv.ParseUint(channel, 10, strconv.IntSize)
		if err != nil || value == 0 {
			return parsed, fmt.Errorf("%w: channel", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
		}
		parsed.channelID = uint(value)
	}

	group := normalizeGatewayUpstreamGroupChangeHistorySelection(filter.Group)
	if group != "" {
		switch {
		case strings.HasPrefix(group, "group:"):
			value, err := strconv.ParseUint(strings.TrimPrefix(group, "group:"), 10, strconv.IntSize)
			if err != nil || value == 0 {
				return parsed, fmt.Errorf("%w: group", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
			}
			parsed.upstreamGroupID = uint(value)
		case strings.HasPrefix(group, "name:"):
			parsed.groupName = strings.TrimSpace(strings.TrimPrefix(group, "name:"))
			if !validGatewayUpstreamGroupChangeHistoryName(parsed.groupName) {
				return parsed, fmt.Errorf("%w: group", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
			}
		default:
			return parsed, fmt.Errorf("%w: group", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
		}
	}

	source := normalizeGatewayUpstreamGroupChangeHistorySelection(filter.Source)
	if source != "" {
		switch {
		case source == "unknown":
			parsed.unknownSource = true
		case strings.HasPrefix(source, "key:"):
			value, err := strconv.ParseInt(strings.TrimPrefix(source, "key:"), 10, 64)
			if err != nil || value <= 0 {
				return parsed, fmt.Errorf("%w: source", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
			}
			parsed.upstreamKeyID = value
		case strings.HasPrefix(source, "name:"):
			parsed.sourceName = strings.TrimSpace(strings.TrimPrefix(source, "name:"))
			if !validGatewayUpstreamGroupChangeHistoryName(parsed.sourceName) {
				return parsed, fmt.Errorf("%w: source", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
			}
		default:
			return parsed, fmt.Errorf("%w: source", ErrGatewayUpstreamGroupChangeHistoryFilterInvalid)
		}
	}
	return parsed, nil
}

func normalizeGatewayUpstreamGroupChangeHistorySelection(value string) string {
	value = strings.TrimSpace(value)
	if value == "__all__" {
		return ""
	}
	return value
}

func validGatewayUpstreamGroupChangeHistoryName(value string) bool {
	return value != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= 128 && !strings.ContainsRune(value, 0)
}

func gatewayUpstreamGroupChangeHistoryQuery(db *gorm.DB, filter parsedGatewayUpstreamGroupChangeHistoryFilter) *gorm.DB {
	query := db.Table("gateway_upstream_group_changes AS changes").
		Joins("LEFT JOIN channels AS history_channels ON history_channels.id = changes.channel_id")
	query = applyGatewayUpstreamGroupChangeHistoryView(query, filter.view)
	if filter.search != "" {
		pattern := "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(filter.search) + "%"
		query = query.Where(`
				history_channels.name ILIKE ? ESCAPE '!' OR changes.group_name ILIKE ? ESCAPE '!' OR
				changes.upstream_key_name ILIKE ? ESCAPE '!' OR changes.assignment_state ILIKE ? ESCAPE '!' OR
				changes.assignment_reason_code ILIKE ? ESCAPE '!' OR changes.status ILIKE ? ESCAPE '!' OR changes.kind ILIKE ? ESCAPE '!' OR
				changes.removal_manual_review_reason ILIKE ? ESCAPE '!' OR changes.removal_manual_review_actor ILIKE ? ESCAPE '!' OR
				changes.removal_manual_review_resolution ILIKE ? ESCAPE '!' OR changes.removal_manual_review_resolved_by ILIKE ? ESCAPE '!' OR
				EXISTS (SELECT 1 FROM gateway_upstream_group_assignment_audits AS assignment_audits
					WHERE assignment_audits.change_id = changes.id AND (
						assignment_audits.actor ILIKE ? ESCAPE '!' OR assignment_audits.action ILIKE ? ESCAPE '!' OR
						assignment_audits.trigger_source ILIKE ? ESCAPE '!' OR assignment_audits.target_candidate_group_ids::text ILIKE ? ESCAPE '!'))`,
			pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern,
		)
	}
	if filter.channelID > 0 {
		query = query.Where("changes.channel_id = ?", filter.channelID)
	}
	if filter.upstreamGroupID > 0 {
		query = query.Where("changes.upstream_group_id = ?", filter.upstreamGroupID)
	} else if filter.groupName != "" {
		query = query.Where("lower(btrim(changes.group_name)) = lower(btrim(?))", filter.groupName)
	}
	if filter.upstreamKeyID > 0 {
		query = query.Where("changes.upstream_key_id = ?", filter.upstreamKeyID)
	} else if filter.sourceName != "" {
		query = query.Where("COALESCE(changes.upstream_key_id, 0) = 0 AND lower(btrim(changes.upstream_key_name)) = lower(btrim(?))", filter.sourceName)
	} else if filter.unknownSource {
		query = query.Where("COALESCE(changes.upstream_key_id, 0) = 0 AND btrim(COALESCE(changes.upstream_key_name, '')) = ''")
	}
	return query
}

func applyGatewayUpstreamGroupChangeHistoryView(query *gorm.DB, view string) *gorm.DB {
	switch view {
	case "review":
		return query.Where(
			"changes.status = ? AND changes.kind = ? AND changes.assignment_reason_code = ? AND changes.removal_manual_review_resolved_at IS NULL",
			"ignored", "removed", "manual_review_required",
		)
	case "ignored":
		return query.Where("changes.status = ?", "ignored")
	case "resolved":
		return query.Where("changes.status <> ? AND changes.status <> ?", "pending", "ignored")
	default:
		return query.Where(
			"changes.status <> ? AND NOT (changes.status = ? AND changes.kind = ? AND changes.assignment_reason_code = ? AND changes.removal_manual_review_resolved_at IS NULL)",
			"pending", "ignored", "removed", "manual_review_required",
		)
	}
}

func (g *Gateway) listUpstreamGroupChangeHistoryFilterOptions(view string, limit int) (GatewayUpstreamGroupChangeHistoryFilterOptions, error) {
	if limit <= 0 || limit > gatewayUpstreamGroupChangeHistoryFacetLimit {
		limit = gatewayUpstreamGroupChangeHistoryFacetLimit
	}
	options := newGatewayUpstreamGroupChangeHistoryFilterOptions()
	if g == nil || g.db == nil {
		return options, errors.New("gateway storage is required")
	}
	if !isGatewayUpstreamGroupChangeHistoryFacetCacheView(view) {
		return g.queryUpstreamGroupChangeHistoryFilterOptions(view, limit)
	}

	// Hold the narrow cache lock while building a missing view. This prevents a
	// burst of page-1/page-2 requests from independently issuing the same three
	// unbounded facet scans. No request-routing or other Gateway state uses it.
	g.historyFacetCacheMu.Lock()
	defer g.historyFacetCacheMu.Unlock()
	now := time.Now()
	if entry, ok := g.historyFacetCache[view]; ok && now.Before(entry.expiresAt) {
		return limitGatewayUpstreamGroupChangeHistoryFilterOptions(entry.options, limit), nil
	}

	options, err := g.queryUpstreamGroupChangeHistoryFilterOptions(view, gatewayUpstreamGroupChangeHistoryFacetLimit)
	if err != nil {
		return options, err
	}
	if g.historyFacetCache == nil {
		g.historyFacetCache = make(map[string]gatewayUpstreamGroupChangeHistoryFacetCacheEntry, gatewayUpstreamGroupChangeHistoryFacetCacheMax)
	}
	cachedAt := time.Now()
	for cachedView, entry := range g.historyFacetCache {
		if !cachedAt.Before(entry.expiresAt) {
			delete(g.historyFacetCache, cachedView)
		}
	}
	// The input is one of a small fixed set of valid views. This defensive eviction
	// preserves a hard memory bound even if a future caller changes that set.
	if len(g.historyFacetCache) >= gatewayUpstreamGroupChangeHistoryFacetCacheMax {
		var oldestView string
		var oldestExpiry time.Time
		for cachedView, entry := range g.historyFacetCache {
			if oldestExpiry.IsZero() || entry.expiresAt.Before(oldestExpiry) {
				oldestView, oldestExpiry = cachedView, entry.expiresAt
			}
		}
		delete(g.historyFacetCache, oldestView)
	}
	g.historyFacetCache[view] = gatewayUpstreamGroupChangeHistoryFacetCacheEntry{
		expiresAt: cachedAt.Add(gatewayUpstreamGroupChangeHistoryFacetCacheTTL),
		options:   cloneGatewayUpstreamGroupChangeHistoryFilterOptions(options),
	}
	return limitGatewayUpstreamGroupChangeHistoryFilterOptions(options, limit), nil
}

func isGatewayUpstreamGroupChangeHistoryFacetCacheView(view string) bool {
	switch view {
	case "", "ignored", "resolved", "review":
		return true
	default:
		return false
	}
}

func newGatewayUpstreamGroupChangeHistoryFilterOptions() GatewayUpstreamGroupChangeHistoryFilterOptions {
	return GatewayUpstreamGroupChangeHistoryFilterOptions{
		Channels: []GatewayUpstreamGroupChangeHistoryFilterOption{},
		Groups:   []GatewayUpstreamGroupChangeHistoryFilterOption{},
		Sources:  []GatewayUpstreamGroupChangeHistoryFilterOption{},
	}
}

func cloneGatewayUpstreamGroupChangeHistoryFilterOptions(options GatewayUpstreamGroupChangeHistoryFilterOptions) GatewayUpstreamGroupChangeHistoryFilterOptions {
	clone := GatewayUpstreamGroupChangeHistoryFilterOptions{
		Channels:  make([]GatewayUpstreamGroupChangeHistoryFilterOption, len(options.Channels)),
		Groups:    make([]GatewayUpstreamGroupChangeHistoryFilterOption, len(options.Groups)),
		Sources:   make([]GatewayUpstreamGroupChangeHistoryFilterOption, len(options.Sources)),
		Truncated: options.Truncated,
	}
	copy(clone.Channels, options.Channels)
	copy(clone.Groups, options.Groups)
	copy(clone.Sources, options.Sources)
	return clone
}

func limitGatewayUpstreamGroupChangeHistoryFilterOptions(options GatewayUpstreamGroupChangeHistoryFilterOptions, limit int) GatewayUpstreamGroupChangeHistoryFilterOptions {
	limited := cloneGatewayUpstreamGroupChangeHistoryFilterOptions(options)
	if len(limited.Channels) > limit {
		limited.Channels = limited.Channels[:limit]
		limited.Truncated = true
	}
	if len(limited.Groups) > limit {
		limited.Groups = limited.Groups[:limit]
		limited.Truncated = true
	}
	if len(limited.Sources) > limit {
		limited.Sources = limited.Sources[:limit]
		limited.Truncated = true
	}
	return limited
}

func (g *Gateway) queryUpstreamGroupChangeHistoryFilterOptions(view string, limit int) (GatewayUpstreamGroupChangeHistoryFilterOptions, error) {
	options := newGatewayUpstreamGroupChangeHistoryFilterOptions()

	type channelRow struct {
		ChannelID   uint
		ChannelName string
	}
	channelRows := []channelRow{}
	channelQuery := applyGatewayUpstreamGroupChangeHistoryView(
		g.db.Table("gateway_upstream_group_changes AS changes").
			Joins("LEFT JOIN channels AS history_channels ON history_channels.id = changes.channel_id"), view,
	)
	if err := channelQuery.
		Select("changes.channel_id AS channel_id, COALESCE(MAX(history_channels.name), '') AS channel_name").
		Group("changes.channel_id").
		Order("lower(COALESCE(MAX(history_channels.name), '')) ASC, changes.channel_id ASC").
		Limit(limit + 1).
		Scan(&channelRows).Error; err != nil {
		return options, err
	}
	if len(channelRows) > limit {
		options.Truncated = true
		channelRows = channelRows[:limit]
	}
	for _, row := range channelRows {
		label := strings.TrimSpace(row.ChannelName)
		if label == "" {
			label = fmt.Sprintf("渠道 #%d", row.ChannelID)
		}
		options.Channels = append(options.Channels, GatewayUpstreamGroupChangeHistoryFilterOption{Value: strconv.FormatUint(uint64(row.ChannelID), 10), Label: label})
	}

	type groupRow struct {
		UpstreamGroupID *uint
		GroupName       string
		ChannelID       uint
		ChannelName     string
	}
	groupRows := []groupRow{}
	groupQuery := applyGatewayUpstreamGroupChangeHistoryView(
		g.db.Table("gateway_upstream_group_changes AS changes").
			Joins("LEFT JOIN channels AS history_channels ON history_channels.id = changes.channel_id"), view,
	)
	if err := groupQuery.
		Select(`changes.upstream_group_id AS upstream_group_id,
			MIN(btrim(changes.group_name)) AS group_name,
			MIN(changes.channel_id) AS channel_id,
			COALESCE(MIN(history_channels.name), '') AS channel_name`).
		Group("changes.upstream_group_id, CASE WHEN changes.upstream_group_id IS NULL THEN lower(btrim(changes.group_name)) ELSE '' END").
		Order("lower(MIN(btrim(changes.group_name))) ASC, changes.upstream_group_id ASC NULLS LAST").
		Limit(limit + 1).
		Scan(&groupRows).Error; err != nil {
		return options, err
	}
	if len(groupRows) > limit {
		options.Truncated = true
		groupRows = groupRows[:limit]
	}
	groupNameCounts := make(map[string]int, len(groupRows))
	for _, row := range groupRows {
		groupNameCounts[strings.ToLower(strings.TrimSpace(row.GroupName))]++
	}
	for _, row := range groupRows {
		label := strings.TrimSpace(row.GroupName)
		if label == "" {
			continue
		}
		value := "name:" + label
		if row.UpstreamGroupID != nil && *row.UpstreamGroupID > 0 {
			value = "group:" + strconv.FormatUint(uint64(*row.UpstreamGroupID), 10)
			if groupNameCounts[strings.ToLower(label)] > 1 {
				channelLabel := strings.TrimSpace(row.ChannelName)
				if channelLabel == "" {
					channelLabel = fmt.Sprintf("渠道 #%d", row.ChannelID)
				}
				label = fmt.Sprintf("%s（%s，分组 #%d）", label, channelLabel, *row.UpstreamGroupID)
			}
		}
		options.Groups = append(options.Groups, GatewayUpstreamGroupChangeHistoryFilterOption{Value: value, Label: label})
	}

	type sourceRow struct {
		UpstreamKeyID   int64
		UpstreamKeyName string
	}
	sourceRows := []sourceRow{}
	sourceQuery := applyGatewayUpstreamGroupChangeHistoryView(g.db.Table("gateway_upstream_group_changes AS changes"), view)
	if err := sourceQuery.
		Select(`COALESCE(changes.upstream_key_id, 0) AS upstream_key_id, MIN(btrim(COALESCE(changes.upstream_key_name, ''))) AS upstream_key_name`).
		Group("COALESCE(changes.upstream_key_id, 0), CASE WHEN COALESCE(changes.upstream_key_id, 0) = 0 THEN lower(btrim(COALESCE(changes.upstream_key_name, ''))) ELSE '' END").
		Order("lower(MIN(btrim(COALESCE(changes.upstream_key_name, '')))) ASC, COALESCE(changes.upstream_key_id, 0) ASC").
		Limit(limit + 1).
		Scan(&sourceRows).Error; err != nil {
		return options, err
	}
	if len(sourceRows) > limit {
		options.Truncated = true
		sourceRows = sourceRows[:limit]
	}
	for _, row := range sourceRows {
		label := strings.TrimSpace(row.UpstreamKeyName)
		value := "unknown"
		switch {
		case row.UpstreamKeyID > 0:
			value = "key:" + strconv.FormatInt(row.UpstreamKeyID, 10)
			if label == "" {
				label = fmt.Sprintf("Key #%d", row.UpstreamKeyID)
			}
		case label != "":
			value = "name:" + label
		default:
			label = "未标记来源"
		}
		options.Sources = append(options.Sources, GatewayUpstreamGroupChangeHistoryFilterOption{Value: value, Label: label})
	}
	return options, nil
}
