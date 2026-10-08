package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const gatewayUpstreamGroupChangeHistoryFacetCacheTestDriverName = "bridgeops-history-facet-cache-test"

var (
	gatewayUpstreamGroupChangeHistoryFacetCacheTestDriverOnce sync.Once
	gatewayUpstreamGroupChangeHistoryFacetCacheTestQueries    atomic.Int64
)

type gatewayUpstreamGroupChangeHistoryFacetCacheTestDriver struct{}

func (gatewayUpstreamGroupChangeHistoryFacetCacheTestDriver) Open(string) (driver.Conn, error) {
	return gatewayUpstreamGroupChangeHistoryFacetCacheTestConn{}, nil
}

type gatewayUpstreamGroupChangeHistoryFacetCacheTestConn struct{}

func (gatewayUpstreamGroupChangeHistoryFacetCacheTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("history facet cache test driver does not prepare statements")
}

func (gatewayUpstreamGroupChangeHistoryFacetCacheTestConn) Close() error { return nil }

func (gatewayUpstreamGroupChangeHistoryFacetCacheTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("history facet cache test driver does not start transactions")
}

func (gatewayUpstreamGroupChangeHistoryFacetCacheTestConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	gatewayUpstreamGroupChangeHistoryFacetCacheTestQueries.Add(1)
	return gatewayUpstreamGroupChangeHistoryFacetCacheTestRows{}, nil
}

type gatewayUpstreamGroupChangeHistoryFacetCacheTestRows struct{}

func (gatewayUpstreamGroupChangeHistoryFacetCacheTestRows) Columns() []string { return []string{} }
func (gatewayUpstreamGroupChangeHistoryFacetCacheTestRows) Close() error      { return nil }
func (gatewayUpstreamGroupChangeHistoryFacetCacheTestRows) Next([]driver.Value) error {
	return io.EOF
}

func openGatewayUpstreamGroupChangeHistoryFacetCacheTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	gatewayUpstreamGroupChangeHistoryFacetCacheTestDriverOnce.Do(func() {
		sql.Register(gatewayUpstreamGroupChangeHistoryFacetCacheTestDriverName, gatewayUpstreamGroupChangeHistoryFacetCacheTestDriver{})
	})
	sqlDB, err := sql.Open(gatewayUpstreamGroupChangeHistoryFacetCacheTestDriverName, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestGatewayUpstreamGroupChangeHistoryFilterValidation(t *testing.T) {
	for _, filter := range []GatewayUpstreamGroupChangeHistoryFilter{
		{View: "pending"},
		{Channel: "0"},
		{Group: "group:0"},
		{Group: "untyped"},
		{Source: "key:-1"},
		{Source: "untyped"},
		{Search: string(make([]byte, gatewayUpstreamGroupChangeHistorySearchLimit+1))},
	} {
		if _, err := parseGatewayUpstreamGroupChangeHistoryFilter(filter); !errors.Is(err, ErrGatewayUpstreamGroupChangeHistoryFilterInvalid) {
			t.Fatalf("filter %+v error = %v", filter, err)
		}
	}
	parsed, err := parseGatewayUpstreamGroupChangeHistoryFilter(GatewayUpstreamGroupChangeHistoryFilter{
		View: "review", Search: "青色渠道", Channel: "7", Group: "name:官渠", Source: "unknown",
	})
	if err != nil || parsed.view != "review" || parsed.search != "青色渠道" || parsed.channelID != 7 || parsed.groupName != "官渠" || !parsed.unknownSource {
		t.Fatalf("parsed filter = %+v, %v", parsed, err)
	}
	store := &Gateway{}
	if _, _, _, err := store.ListUpstreamGroupChangesHistoryFilteredPage(GatewayUpstreamGroupChangeHistoryFilter{}, gatewayUpstreamGroupChangeHistoryPageLimit+1, 50); !errors.Is(err, ErrGatewayUpstreamGroupChangeHistoryFilterInvalid) {
		t.Fatalf("oversized page error = %v", err)
	}
	if _, err := store.ListUpstreamGroupChangesHistoryFilteredCursorPage(GatewayUpstreamGroupChangeHistoryFilter{}, "not-a-cursor", 50); !errors.Is(err, ErrGatewayUpstreamGroupChangeHistoryCursorInvalid) {
		t.Fatalf("invalid cursor error = %v", err)
	}
	if _, err := store.ListUpstreamGroupChangesHistoryFilteredCursorPage(GatewayUpstreamGroupChangeHistoryFilter{}, "", gatewayUpstreamGroupChangeHistoryPageMaxSize+1); !errors.Is(err, ErrGatewayUpstreamGroupChangeHistoryFilterInvalid) {
		t.Fatalf("oversized cursor page error = %v", err)
	}
}

func TestGatewayUpstreamGroupChangeReviewViewOnlyReturnsUnresolvedRemovalReviews(t *testing.T) {
	sqlDB, err := sql.Open("pgx", "host=127.0.0.1 user=bridgeops database=bridgeops sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB}), &gorm.Config{DisableAutomaticPing: true, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	filter, err := parseGatewayUpstreamGroupChangeHistoryFilter(GatewayUpstreamGroupChangeHistoryFilter{View: "review"})
	if err != nil {
		t.Fatal(err)
	}
	query := gatewayUpstreamGroupChangeHistoryQuery(db, filter).Find(&[]GatewayUpstreamGroupChange{})
	generated := strings.ToLower(query.Statement.SQL.String())
	for _, fragment := range []string{"changes.status =", "changes.kind =", "changes.assignment_reason_code =", "changes.removal_manual_review_resolved_at is null"} {
		if !strings.Contains(generated, fragment) {
			t.Fatalf("review SQL missing %q: %s", fragment, generated)
		}
	}
	if variables := fmt.Sprint(query.Statement.Vars); !strings.Contains(variables, "ignored") || !strings.Contains(variables, "removed") || !strings.Contains(variables, "manual_review_required") {
		t.Fatalf("review SQL variables = %s", variables)
	}
	historyFilter, err := parseGatewayUpstreamGroupChangeHistoryFilter(GatewayUpstreamGroupChangeHistoryFilter{})
	if err != nil {
		t.Fatal(err)
	}
	historyQuery := gatewayUpstreamGroupChangeHistoryQuery(db, historyFilter).Find(&[]GatewayUpstreamGroupChange{})
	historySQL := strings.ToLower(historyQuery.Statement.SQL.String())
	if !strings.Contains(historySQL, "not (changes.status =") || !strings.Contains(historySQL, "changes.removal_manual_review_resolved_at is null") {
		t.Fatalf("history SQL does not exclude active review work: %s", historySQL)
	}
}

func TestGatewayUpstreamGroupChangeHistoryCursorQueryIsStableAndEscapesSearchWildcards(t *testing.T) {
	sqlDB, err := sql.Open("pgx", "host=127.0.0.1 user=bridgeops database=bridgeops sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB}), &gorm.Config{DisableAutomaticPing: true, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	filter, err := parseGatewayUpstreamGroupChangeHistoryFilter(GatewayUpstreamGroupChangeHistoryFilter{
		View: "resolved", Search: "50%_!", Channel: "7", Group: "name:VIP", Source: "name:Managed Key",
	})
	if err != nil {
		t.Fatal(err)
	}
	cursor := gatewayUpstreamGroupChangeHistoryCursor{
		Version:    gatewayUpstreamGroupChangeHistoryCursorV1,
		Scope:      gatewayUpstreamGroupChangeHistoryCursorScope(filter),
		DetectedAt: time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC), ID: 91,
	}
	query := gatewayUpstreamGroupChangeHistoryAfterCursor(gatewayUpstreamGroupChangeHistoryQuery(db, filter), cursor).
		Select("changes.*").
		Order("changes.detected_at DESC, changes.id DESC").
		Limit(51).
		Find(&[]GatewayUpstreamGroupChange{})
	generated := strings.ToLower(query.Statement.SQL.String())
	for _, fragment := range []string{
		"left join channels", "changes.status <>", "ilike", "escape '!'", "changes.channel_id =",
		"lower(btrim(changes.group_name))", "lower(btrim(changes.upstream_key_name))",
		"changes.detected_at <", "changes.id <", "order by changes.detected_at desc, changes.id desc", "limit",
	} {
		if !strings.Contains(generated, fragment) {
			t.Fatalf("history SQL missing %q: %s", fragment, generated)
		}
	}
	if strings.Contains(generated, "offset") {
		t.Fatalf("cursor history SQL unexpectedly uses offset: %s", generated)
	}
	if variables := fmt.Sprint(query.Statement.Vars); !strings.Contains(variables, "%50!%!_!!%") {
		t.Fatalf("history SQL variables did not escape search wildcards: %s", variables)
	}
}

func TestGatewayUpstreamGroupChangeHistorySearchIncludesAuditEvidence(t *testing.T) {
	sqlDB, err := sql.Open("pgx", "host=127.0.0.1 user=bridgeops database=bridgeops sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB}), &gorm.Config{DisableAutomaticPing: true, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	filter, err := parseGatewayUpstreamGroupChangeHistoryFilter(GatewayUpstreamGroupChangeHistoryFilter{Search: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	query := gatewayUpstreamGroupChangeHistoryQuery(db, filter).Find(&[]GatewayUpstreamGroupChange{})
	generated := strings.ToLower(query.Statement.SQL.String())
	for _, fragment := range []string{
		"changes.removal_manual_review_reason ilike",
		"changes.removal_manual_review_actor ilike",
		"changes.removal_manual_review_resolution ilike",
		"changes.removal_manual_review_resolved_by ilike",
		"gateway_upstream_group_assignment_audits",
		"assignment_audits.target_candidate_group_ids::text ilike",
	} {
		if !strings.Contains(generated, fragment) {
			t.Fatalf("history search SQL missing %q: %s", fragment, generated)
		}
	}
}

func TestGatewayUpstreamGroupChangeHistoryCursorPaginationIsBoundedFilteredAndStable(t *testing.T) {
	db := testdb.Open(t)
	store := NewGateway(db)
	const (
		historySize  = 20_000
		identitySize = 601
	)
	detectedAt := time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC)
	channels := make([]Channel, 0, identitySize)
	for identity := 0; identity < identitySize; identity++ {
		channels = append(channels, Channel{
			ID:             uint(identity + 1),
			Name:           fmt.Sprintf("history-channel-%03d", identity),
			Type:           ChannelTypeNewAPI,
			SiteURL:        fmt.Sprintf("https://history-channel-%03d.example", identity),
			PasswordCipher: "test",
		})
	}
	if err := db.CreateInBatches(&channels, 200).Error; err != nil {
		t.Fatal(err)
	}

	changes := make([]GatewayUpstreamGroupChange, 0, historySize)
	wantFiltered := 0
	for index := 0; index < historySize; index++ {
		identity := index % identitySize
		status := "assigned"
		if index%3 == 0 {
			status = "ignored"
		} else if index%3 == 2 {
			status = "acknowledged"
		}
		if identity == 6 && status == "ignored" {
			wantFiltered++
		}
		changes = append(changes, GatewayUpstreamGroupChange{
			ChannelID:       uint(identity + 1),
			GroupName:       fmt.Sprintf("group-%03d", identity),
			Kind:            "added",
			Status:          status,
			UpstreamKeyID:   int64(identity + 1),
			UpstreamKeyName: fmt.Sprintf("key-%03d", identity),
			DetectedAt:      detectedAt,
			CreatedAt:       detectedAt,
			UpdatedAt:       detectedAt,
		})
	}
	if err := db.CreateInBatches(&changes, 500).Error; err != nil {
		t.Fatal(err)
	}
	changes = nil

	filter := GatewayUpstreamGroupChangeHistoryFilter{
		View: "ignored", Search: "group-006", Channel: "7", Group: "name:group-006", Source: "key:7",
	}
	firstPage, err := store.ListUpstreamGroupChangesHistoryFilteredCursorPage(filter, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	first := firstPage.Items
	if len(first) != min(10, wantFiltered) || firstPage.NextCursor == "" {
		t.Fatalf("filtered cursor page items=%d next_cursor=%q want items=%d and next cursor", len(first), firstPage.NextCursor, min(10, wantFiltered))
	}
	if len(firstPage.FilterOptions.Channels) != gatewayUpstreamGroupChangeHistoryFacetLimit || len(firstPage.FilterOptions.Groups) != gatewayUpstreamGroupChangeHistoryFacetLimit || len(firstPage.FilterOptions.Sources) != gatewayUpstreamGroupChangeHistoryFacetLimit || !firstPage.FilterOptions.Truncated {
		t.Fatalf("bounded options channels=%d groups=%d sources=%d truncated=%t", len(firstPage.FilterOptions.Channels), len(firstPage.FilterOptions.Groups), len(firstPage.FilterOptions.Sources), firstPage.FilterOptions.Truncated)
	}
	for index := 1; index < len(first); index++ {
		if first[index-1].ID <= first[index].ID {
			t.Fatalf("same-timestamp order is not id-descending: %d then %d", first[index-1].ID, first[index].ID)
		}
	}

	secondPage, err := store.ListUpstreamGroupChangesHistoryFilteredCursorPage(filter, firstPage.NextCursor, 10)
	if err != nil {
		t.Fatalf("second cursor page error=%v", err)
	}
	second := secondPage.Items
	firstIDs := make(map[uint]struct{}, len(first))
	for _, item := range first {
		firstIDs[item.ID] = struct{}{}
	}
	for _, item := range second {
		if _, duplicate := firstIDs[item.ID]; duplicate {
			t.Fatalf("history id %d appeared on adjacent stable pages", item.ID)
		}
	}
	repeatedPage, err := store.ListUpstreamGroupChangesHistoryFilteredCursorPage(filter, "", 10)
	if err != nil {
		t.Fatalf("repeated cursor page error=%v", err)
	}
	if len(repeatedPage.Items) != len(first) || repeatedPage.NextCursor != firstPage.NextCursor {
		t.Fatalf("repeated cursor page next_cursor=%q items=%d err=%v", repeatedPage.NextCursor, len(repeatedPage.Items), err)
	}
	for index := range first {
		if repeatedPage.Items[index].ID != first[index].ID {
			t.Fatalf("repeated cursor page id[%d]=%d want %d", index, repeatedPage.Items[index].ID, first[index].ID)
		}
	}
	changedFilter := filter
	changedFilter.Search = "group-007"
	if _, err := store.ListUpstreamGroupChangesHistoryFilteredCursorPage(changedFilter, firstPage.NextCursor, 10); !errors.Is(err, ErrGatewayUpstreamGroupChangeHistoryCursorInvalid) {
		t.Fatalf("filter-scoped cursor error = %v", err)
	}
}

func TestGatewayUpstreamGroupChangeHistoryGroupFacetsDistinguishSameNameStableGroups(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	first := &Channel{Name: "first channel", Type: ChannelTypeNewAPI, SiteURL: "https://first-history-facet.example", MonitorEnabled: true}
	second := &Channel{Name: "second channel", Type: ChannelTypeNewAPI, SiteURL: "https://second-history-facet.example", MonitorEnabled: true}
	if err := channels.Create(first); err != nil {
		t.Fatal(err)
	}
	if err := channels.Create(second); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(first.ID, "promo", "added", 1, "first-key", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(second.ID, "promo", "added", 2, "second-key", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayUpstreamGroupChange{}).Where("channel_id IN ?", []uint{first.ID, second.ID}).
		Updates(map[string]any{"status": "assigned", "assignment_state": "assigned"}).Error; err != nil {
		t.Fatal(err)
	}
	var groups []GatewayUpstreamGroup
	if err := db.Where("channel_id IN ?", []uint{first.ID, second.ID}).Order("channel_id ASC").Find(&groups).Error; err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("stable groups = %+v", groups)
	}

	options, err := store.queryUpstreamGroupChangeHistoryFilterOptions("", 10)
	if err != nil {
		t.Fatal(err)
	}
	labels := make(map[string]string, len(options.Groups))
	for _, option := range options.Groups {
		labels[option.Value] = option.Label
	}
	if labels[fmt.Sprintf("group:%d", groups[0].ID)] != fmt.Sprintf("promo（first channel，分组 #%d）", groups[0].ID) {
		t.Fatalf("first same-name group label = %q", labels[fmt.Sprintf("group:%d", groups[0].ID)])
	}
	if labels[fmt.Sprintf("group:%d", groups[1].ID)] != fmt.Sprintf("promo（second channel，分组 #%d）", groups[1].ID) {
		t.Fatalf("second same-name group label = %q", labels[fmt.Sprintf("group:%d", groups[1].ID)])
	}
}

func TestGatewayUpstreamGroupChangeHistoryFacetCacheBoundsAndClonesResponses(t *testing.T) {
	store := &Gateway{
		db: &gorm.DB{},
		historyFacetCache: map[string]gatewayUpstreamGroupChangeHistoryFacetCacheEntry{
			"": {
				expiresAt: time.Now().Add(time.Minute),
				options: GatewayUpstreamGroupChangeHistoryFilterOptions{
					Channels: []GatewayUpstreamGroupChangeHistoryFilterOption{{Value: "1", Label: "first"}, {Value: "2", Label: "second"}},
					Groups:   []GatewayUpstreamGroupChangeHistoryFilterOption{{Value: "group:1", Label: "alpha"}, {Value: "group:2", Label: "beta"}},
					Sources:  []GatewayUpstreamGroupChangeHistoryFilterOption{{Value: "key:1", Label: "managed"}},
				},
			},
		},
	}

	first, err := store.listUpstreamGroupChangeHistoryFilterOptions("", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Channels) != 1 || len(first.Groups) != 1 || len(first.Sources) != 1 || !first.Truncated {
		t.Fatalf("limited cached facets = %+v", first)
	}
	first.Channels[0].Label = "mutated"
	first.Groups[0].Label = "mutated"

	second, err := store.listUpstreamGroupChangeHistoryFilterOptions("", 1)
	if err != nil {
		t.Fatal(err)
	}
	if second.Channels[0].Label != "first" || second.Groups[0].Label != "alpha" {
		t.Fatalf("cached facet response was mutated through a caller alias: %+v", second)
	}
	if len(store.historyFacetCache) != 1 || !isGatewayUpstreamGroupChangeHistoryFacetCacheView("") ||
		!isGatewayUpstreamGroupChangeHistoryFacetCacheView("ignored") || !isGatewayUpstreamGroupChangeHistoryFacetCacheView("resolved") ||
		!isGatewayUpstreamGroupChangeHistoryFacetCacheView("review") ||
		isGatewayUpstreamGroupChangeHistoryFacetCacheView("pending") {
		t.Fatalf("unexpected cache view policy or cache size: %d", len(store.historyFacetCache))
	}
}

func TestGatewayUpstreamGroupChangeHistoryCursorPagesReuseFacetCache(t *testing.T) {
	db := openGatewayUpstreamGroupChangeHistoryFacetCacheTestDB(t)
	store := NewGateway(db)
	gatewayUpstreamGroupChangeHistoryFacetCacheTestQueries.Store(0)

	first, err := store.ListUpstreamGroupChangesHistoryFilteredCursorPage(GatewayUpstreamGroupChangeHistoryFilter{}, "", 10)
	if err != nil {
		t.Fatalf("first cursor page: %v", err)
	}
	parsed, err := parseGatewayUpstreamGroupChangeHistoryFilter(GatewayUpstreamGroupChangeHistoryFilter{})
	if err != nil {
		t.Fatal(err)
	}
	secondCursor, err := encodeGatewayUpstreamGroupChangeHistoryCursor(parsed, GatewayUpstreamGroupChange{
		ID: 1, DetectedAt: time.Date(2026, 7, 24, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.ListUpstreamGroupChangesHistoryFilteredCursorPage(GatewayUpstreamGroupChangeHistoryFilter{}, secondCursor, 10)
	if err != nil {
		t.Fatalf("second cursor page: %v", err)
	}
	if len(first.Items) != 0 || len(second.Items) != 0 {
		t.Fatalf("empty test driver unexpectedly returned history items: first=%+v second=%+v", first.Items, second.Items)
	}
	// The first page performs its history query plus the three facet GROUP BY
	// scans. The cursor page adds only its history query; it must not rebuild
	// the facets before the short cache TTL expires.
	if queries := gatewayUpstreamGroupChangeHistoryFacetCacheTestQueries.Load(); queries != 5 {
		t.Fatalf("history page queries = %d, want 5 (two page queries plus one three-query facet build)", queries)
	}
	if len(store.historyFacetCache) != 1 {
		t.Fatalf("history facet cache entries = %d, want one default-view entry", len(store.historyFacetCache))
	}
	store.historyFacetCacheMu.Lock()
	expired := store.historyFacetCache[""]
	expired.expiresAt = time.Now().Add(-time.Second)
	store.historyFacetCache[""] = expired
	store.historyFacetCacheMu.Unlock()
	if _, err := store.listUpstreamGroupChangeHistoryFilterOptions("", gatewayUpstreamGroupChangeHistoryFacetLimit); err != nil {
		t.Fatalf("expired default-view facets: %v", err)
	}
	if queries := gatewayUpstreamGroupChangeHistoryFacetCacheTestQueries.Load(); queries != 8 {
		t.Fatalf("expired facet cache queries = %d, want cache refresh with three new scans", queries)
	}

	if _, err := store.listUpstreamGroupChangeHistoryFilterOptions("pending", 10); err != nil {
		t.Fatalf("non-cacheable view facets: %v", err)
	}
	if len(store.historyFacetCache) != 1 {
		t.Fatalf("non-cacheable view populated facet cache: %d entries", len(store.historyFacetCache))
	}
}

func TestGatewayUpstreamGroupChangeHistoryFacetCacheSerializesFirstBuild(t *testing.T) {
	db := openGatewayUpstreamGroupChangeHistoryFacetCacheTestDB(t)
	store := NewGateway(db)
	gatewayUpstreamGroupChangeHistoryFacetCacheTestQueries.Store(0)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.listUpstreamGroupChangeHistoryFilterOptions("ignored", gatewayUpstreamGroupChangeHistoryFacetLimit)
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent facet lookup: %v", err)
		}
	}
	if queries := gatewayUpstreamGroupChangeHistoryFacetCacheTestQueries.Load(); queries != 3 {
		t.Fatalf("concurrent first facet build queries = %d, want one three-query build", queries)
	}
}
