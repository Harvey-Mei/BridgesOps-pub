package storage

import (
	"testing"
	"time"
)

func TestModelCheckRetentionIsGlobalAndPreservesBudgetLedger(t *testing.T) {
	db := openTestDB(t)
	store := NewModelChecks(db)
	channel := Channel{Name: "pelican", Type: ChannelTypeNewAPI, SiteURL: "https://example.invalid", MonitorEnabled: true}
	if err := NewChannels(db).Create(&channel); err != nil {
		t.Fatal(err)
	}
	targets := []ModelCheckTarget{
		{ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "a", ModelName: "gpt-a", App: GatewayAppCodex, Active: true, Enabled: true},
		{ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "b", ModelName: "gpt-b", App: GatewayAppCodex, Active: true, Enabled: true},
	}
	for i := range targets {
		if err := db.Create(&targets[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	for i := 0; i < 15; i++ {
		target := &targets[i%2]
		run := ModelCheckRun{TargetID: target.ID, ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: target.GroupName, ModelName: target.ModelName, App: target.App, ProbeVersion: "pelican-v1", Status: ModelCheckRunPassed, EvidenceJSON: `[{"kind":"pelican_svg","response":"bounded"}]`, AnomalyReasons: `[]`, RequestCount: 2, StartedAt: now, FinishedAt: now.Add(time.Duration(i) * time.Millisecond)}
		if err := store.SaveRunAndTarget(&run, target); err != nil {
			t.Fatal(err)
		}
	}
	latest, err := store.LatestModelCheckResults()
	if err != nil || len(latest) != 12 {
		t.Fatalf("latest=%d err=%v", len(latest), err)
	}
	var rows []ModelCheckRun
	if err := db.Order("id ASC").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 15 {
		t.Fatal("request ledger was deleted")
	}
	for i, row := range rows {
		if (row.EvidenceJSON == "[]") != (i < 3) {
			t.Fatalf("payload retention for row %d", i)
		}
	}
	count, err := store.CountRequestsSince(now.Add(-time.Second))
	if err != nil || count != 30 {
		t.Fatalf("requests=%d err=%v", count, err)
	}
	for _, target := range targets {
		runs, total, err := store.ListRunsPage(target.ID, 1, 100)
		if err != nil || total != 6 || len(runs) != 6 {
			t.Fatalf("target page total=%d len=%d err=%v", total, len(runs), err)
		}
	}
}
