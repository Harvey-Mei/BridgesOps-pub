package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGatewayRequestHistoryKeepsChannelNameSnapshotAfterRename(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "before-rename", Type: ChannelTypeNewAPI, SiteURL: "https://before.example", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := store.AppendRequestLog(&GatewayRequestLog{
		ChannelID: channel.ID, GroupName: "default", ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, LatencyMs: 100, CreatedAt: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	channel.Name = "after-rename"
	if err := channels.UpdateAdminFields(channel, ChannelAdminFieldName); err != nil {
		t.Fatalf("rename channel: %v", err)
	}

	items, err := store.ListRequestLogsFiltered(GatewayRequestLogFilter{}, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("list request logs = %+v, %v", items, err)
	}
	if items[0].ChannelName != "before-rename" {
		t.Fatalf("request log channel name = %q, want snapshot", items[0].ChannelName)
	}
	analytics, err := store.RequestLogAnalytics(GatewayRequestLogFilter{}, 8)
	if err != nil || len(analytics.Channels) != 1 {
		t.Fatalf("channel analytics = %+v, %v", analytics, err)
	}
	if analytics.Channels[0].Name != "before-rename" {
		t.Fatalf("analytics channel name = %q, want snapshot", analytics.Channels[0].Name)
	}
}

func TestPrepareChannelDeletionKeepsGatewayRequestLedger(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "deleted-channel", Type: ChannelTypeSub2API, SiteURL: "https://deleted.example", PasswordCipher: "cipher", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := store.AppendRequestLog(&GatewayRequestLog{
		ChannelID: channel.ID, GroupName: "default", ModelName: "gpt-5", Method: "POST", Path: "/v1/responses",
		StatusCode: 200, LatencyMs: 100, CreatedAt: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("append request log: %v", err)
	}
	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion: %v", err)
	}

	var logs, aggregates int64
	if err := db.Model(&GatewayRequestLog{}).Where("channel_id = ?", channel.ID).Count(&logs).Error; err != nil {
		t.Fatalf("count request logs: %v", err)
	}
	if err := db.Model(&GatewayRequestAggregate{}).Where("channel_id = ?", channel.ID).Count(&aggregates).Error; err != nil {
		t.Fatalf("count request aggregates: %v", err)
	}
	if logs != 1 || aggregates != 1 {
		t.Fatalf("preserved ledger rows = logs %d aggregates %d, want 1/1", logs, aggregates)
	}
	items, err := store.ListRequestLogsFiltered(GatewayRequestLogFilter{}, 10)
	if err != nil || len(items) != 1 || items[0].ChannelName != "deleted-channel" {
		t.Fatalf("historical request log after deletion = %+v, %v", items, err)
	}
}

func TestPrepareChannelDeletionLeavesPendingGatewayHistoryForLaterFinalizer(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "pending-history", Type: ChannelTypeNewAPI, SiteURL: "https://pending.example", PasswordCipher: "cipher"}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	log := GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: channel.ID, GroupName: "default", ModelName: "gpt-5",
		Method: "POST", Path: "/v1/responses", StatusCode: 200, AggregateVersion: 0,
		CreatedAt: time.Date(2026, 7, 19, 13, 0, 0, 0, time.UTC),
	}
	if err := db.Create(&log).Error; err != nil {
		t.Fatal(err)
	}
	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatal(err)
	}
	var stored GatewayRequestLog
	if err := db.First(&stored, log.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.ChannelNameSnapshot != "" || stored.AggregateVersion != 0 {
		t.Fatalf("prepare mutated pending log = %+v", stored)
	}
	if _, err := backfillGatewayRequestAggregates(context.Background(), db, 1, 0); err != nil {
		t.Fatal(err)
	}
	items, err := store.ListRequestLogsFiltered(GatewayRequestLogFilter{}, 10)
	if err != nil || len(items) != 1 || items[0].ChannelName != "pending-history" {
		t.Fatalf("historical request after deletion = %+v, err=%v", items, err)
	}
	var aggregate GatewayRequestAggregate
	if err := db.Where("channel_id = ? AND version = ?", channel.ID, gatewayRequestAggregateVersion).First(&aggregate).Error; err != nil {
		t.Fatal(err)
	}
	if aggregate.ChannelNameSnapshot != "pending-history" || aggregate.Requests != 1 {
		t.Fatalf("rebuilt historical aggregate = %+v", aggregate)
	}
}
