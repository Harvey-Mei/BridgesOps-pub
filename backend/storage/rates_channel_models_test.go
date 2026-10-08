package storage

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRateChangeAnalyticsKeepsSameModelSeparateByChannel(t *testing.T) {
	db := openTestDB(t)
	channels := []Channel{
		{Name: "channel-one", Type: ChannelTypeNewAPI, SiteURL: "https://one.example", Username: "one", PasswordCipher: "cipher"},
		{Name: "channel-two", Type: ChannelTypeSub2API, SiteURL: "https://two.example", Username: "two", PasswordCipher: "cipher"},
	}
	if err := db.Create(&channels).Error; err != nil {
		t.Fatalf("create channels: %v", err)
	}

	rates := NewRates(db)
	now := time.Now().In(trendLocation)
	oldOne, oldTwo := 1.0, 2.0
	changes := []*RateChangeLog{
		{ChannelID: channels[0].ID, ModelName: "shared-model", OldRatio: &oldOne, NewRatio: 2, ChangedAt: now.Add(-time.Hour)},
		{ChannelID: channels[0].ID, ModelName: "shared-model", OldRatio: &oldTwo, NewRatio: 1, ChangedAt: now.Add(-2 * time.Hour)},
		{ChannelID: channels[1].ID, ModelName: "shared-model", OldRatio: &oldOne, NewRatio: 3, ChangedAt: now.Add(-3 * time.Hour)},
	}
	for _, change := range changes {
		if err := rates.AppendChange(change); err != nil {
			t.Fatalf("append rate change: %v", err)
		}
	}

	got, err := rates.RateChangeAnalytics(0, now.Add(-24*time.Hour), 20)
	if err != nil {
		t.Fatalf("rate change analytics: %v", err)
	}
	if len(got.Models) != 2 {
		t.Fatalf("model aggregates = %+v, want two channel-specific items", got.Models)
	}

	byChannel := make(map[uint]RateChangeModelAggregate, len(got.Models))
	for _, item := range got.Models {
		byChannel[item.ChannelID] = item
	}
	first := byChannel[channels[0].ID]
	if first.ChannelName != channels[0].Name || first.ModelName != "shared-model" || first.Changes != 2 || first.Up != 1 || first.Down != 1 {
		t.Fatalf("first channel aggregate = %+v", first)
	}
	second := byChannel[channels[1].ID]
	if second.ChannelName != channels[1].Name || second.ModelName != "shared-model" || second.Changes != 1 || second.Up != 1 || second.Down != 0 {
		t.Fatalf("second channel aggregate = %+v", second)
	}

	payload, err := json.Marshal(got.Models)
	if err != nil {
		t.Fatalf("marshal model aggregates: %v", err)
	}
	var encoded []map[string]any
	if err := json.Unmarshal(payload, &encoded); err != nil {
		t.Fatalf("decode model aggregates: %v", err)
	}
	for _, item := range encoded {
		if _, ok := item["channel_id"]; !ok {
			t.Fatalf("encoded aggregate missing channel_id: %s", payload)
		}
		if _, ok := item["channel_name"]; !ok {
			t.Fatalf("encoded aggregate missing channel_name: %s", payload)
		}
	}
}
