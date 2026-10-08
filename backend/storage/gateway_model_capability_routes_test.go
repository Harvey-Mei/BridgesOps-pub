package storage

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestListModelCapabilitiesForRoutesReadsOnlyRequestedCanonicalPairs(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	rates := NewRates(db)
	checkedAt := time.Now().UTC().Add(-time.Minute)
	const mappedModel = "mapped-model"
	channelRevisions := make(map[uint]uint64)
	for _, channelID := range []uint{101, 102, 103} {
		channel := &Channel{ID: channelID, Name: fmt.Sprintf("capability-route-%d", channelID), Type: ChannelTypeNewAPI, SiteURL: fmt.Sprintf("https://capability-route-%d.example", channelID), MonitorEnabled: true}
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create capability channel %d: %v", channelID, err)
		}
		channelRevisions[channelID] = channel.UpstreamAccountIdentityRevision
	}
	for _, route := range []GatewayModelCapabilityRoute{
		{ChannelID: 101, GroupName: "primary"}, {ChannelID: 101, GroupName: "second"},
		{ChannelID: 102, GroupName: "primary"}, {ChannelID: 102, GroupName: "second"},
		{ChannelID: 103, GroupName: "primary"},
	} {
		if _, err := rates.Upsert(&RateSnapshot{ChannelID: route.ChannelID, ModelName: route.GroupName, Ratio: 1, LastSeenAt: checkedAt}); err != nil {
			t.Fatalf("create capability group %d/%s: %v", route.ChannelID, route.GroupName, err)
		}
	}
	capabilities := []GatewayModelCapability{
		// Requested routes deliberately use different casing and surrounding
		// whitespace from their stored capability rows.
		{ChannelID: 101, GroupName: " PRIMARY ", ModelName: mappedModel, Supported: false, LastCheckedAt: checkedAt},
		{ChannelID: 102, GroupName: " SECOND ", ModelName: mappedModel, Supported: true, LastCheckedAt: checkedAt},

		// These use one requested channel and one requested group each, but are
		// not requested pairs. A pair-bounded read must not include this cross
		// product or routes outside the candidate member set.
		{ChannelID: 101, GroupName: "second", ModelName: mappedModel, Supported: false, LastCheckedAt: checkedAt},
		{ChannelID: 102, GroupName: "primary", ModelName: mappedModel, Supported: false, LastCheckedAt: checkedAt},
		{ChannelID: 103, GroupName: "primary", ModelName: mappedModel, Supported: false, LastCheckedAt: checkedAt},

		// A different model on an otherwise requested route is also unrelated.
		{ChannelID: 101, GroupName: "primary", ModelName: "other-model", Supported: false, LastCheckedAt: checkedAt},
	}
	for index := range capabilities {
		generation, err := store.CaptureUpstreamGroupGeneration(capabilities[index].ChannelID, capabilities[index].GroupName)
		if err != nil {
			t.Fatalf("capture capability group %d/%s: %v", capabilities[index].ChannelID, capabilities[index].GroupName, err)
		}
		capabilities[index].UpstreamGroupID = generation.ID
		capabilities[index].PresenceGeneration = generation.PresenceGeneration
		capabilities[index].ChannelIdentityRevision = channelRevisions[capabilities[index].ChannelID]
	}
	if err := db.Create(&capabilities).Error; err != nil {
		t.Fatalf("create model capabilities: %v", err)
	}

	got, err := store.ListModelCapabilitiesForRoutes(mappedModel, []GatewayModelCapabilityRoute{
		{ChannelID: 101, GroupName: "primary"},
		{ChannelID: 102, GroupName: " second "},
	}, time.Hour)
	if err != nil {
		t.Fatalf("list route-bounded model capabilities: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("route-bounded capability rows = %+v, want exactly the two requested pairs", got)
	}
	wantRoutes := map[string]struct{}{"101/primary": {}, "102/second": {}}
	for _, capability := range got {
		route := strings.TrimSpace(capability.GroupName)
		route = strings.ToLower(route)
		key := strconv.FormatUint(uint64(capability.ChannelID), 10) + "/" + route
		if _, exists := wantRoutes[key]; !exists {
			t.Fatalf("unrelated capability route leaked into bounded read: %+v", capability)
		}
		delete(wantRoutes, key)
	}
	if len(wantRoutes) != 0 {
		t.Fatalf("requested capability routes missing: %+v", wantRoutes)
	}
}

func TestListModelCapabilitiesForRoutesDryRunKeepsPairConstraintBounded(t *testing.T) {
	// DryRun never connects, but this is still deliberately an unconnectable,
	// test-only endpoint so a future configuration change cannot target a
	// production-shaped database by accident.
	sqlDB, err := sql.Open("pgx", "host=127.0.0.1 port=1 user=bridgeops_test database=bridgeops_test sslmode=disable")
	if err != nil {
		t.Fatalf("open lazy PostgreSQL connection: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgresDriver.New(postgresDriver.Config{Conn: sqlDB}), &gorm.Config{
		DisableAutomaticPing: true,
		DryRun:               true,
	})
	if err != nil {
		t.Fatalf("open dry-run PostgreSQL database: %v", err)
	}
	store := NewGateway(db)

	var generated string
	var variables []any
	queryCount := 0
	const callbackName = "bridgeops:test-model-capability-route-bound"
	if err := db.Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		queryCount++
		generated = tx.Statement.SQL.String()
		variables = append([]any(nil), tx.Statement.Vars...)
	}); err != nil {
		t.Fatalf("register query capture: %v", err)
	}
	_, err = store.ListModelCapabilitiesForRoutes("server-owned-model", []GatewayModelCapabilityRoute{
		{ChannelID: 11, GroupName: " First "},
		{ChannelID: 12, GroupName: "second"},
		{ChannelID: 11, GroupName: " First "}, // exact duplicate
		{ChannelID: 0, GroupName: "ignored-channel"},
		{ChannelID: 13, GroupName: "   "},
	}, time.Hour)
	if err != nil {
		t.Fatalf("dry-run route-bounded capability lookup: %v", err)
	}
	generated = strings.ToLower(generated)
	if generated == "" {
		t.Fatal("route-bounded capability lookup did not issue a query")
	}
	if queryCount != 1 {
		t.Fatalf("route-bounded capability lookup issued %d queries, want one", queryCount)
	}
	if strings.Contains(generated, "channel_id in") || strings.Contains(generated, "group_name in") {
		t.Fatalf("route lookup used independent IN filters instead of exact pairs: %s", generated)
	}
	if strings.Count(generated, "(channel_id =") != 2 ||
		strings.Count(generated, "lower(btrim(group_name)) = lower(btrim(") != 2 {
		t.Fatalf("route lookup was not constrained to its two valid unique route pairs: %s", generated)
	}
	if !strings.Contains(generated, "model_name =") {
		t.Fatalf("route lookup lost its exact model filter: %s", generated)
	}
	argumentText := fmt.Sprint(variables)
	for _, unexpected := range []string{"ignored-channel", "   "} {
		if strings.Contains(argumentText, unexpected) {
			t.Fatalf("invalid route expanded the capability query: vars=%s", argumentText)
		}
	}
}
