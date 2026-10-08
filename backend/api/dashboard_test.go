package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	_ "github.com/Harvey-Mei/BridgeOps/backend/connector/sub2api"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/monitor"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/progress"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	return testdb.Open(t)
}

func TestDashboardSummaryIncludesCosts(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	notifies := storage.NewNotifications(db)
	rates := storage.NewRates(db)
	announcements := storage.NewUpstreamAnnouncements(db)

	balance1 := 20.0
	today1 := 1.25
	total1 := 10.5
	todayAt := time.Now()
	if err := channels.Create(&storage.Channel{
		Name:           "a",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://a.example.com",
		Username:       "u1",
		PasswordCipher: "x",
		MonitorEnabled: true,
		LastBalance:    &balance1,
		TodayCost:      &today1,
		TodayCostAt:    &todayAt,
		TotalCost:      &total1,
	}); err != nil {
		t.Fatalf("create channel1: %v", err)
	}

	balance2 := 15.0
	today2 := 2.75
	total2 := 30.0
	if err := channels.Create(&storage.Channel{
		Name:           "b",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://b.example.com",
		Username:       "u2",
		PasswordCipher: "y",
		MonitorEnabled: true,
		LastBalance:    &balance2,
		TodayCost:      &today2,
		TodayCostAt:    &todayAt,
		TotalCost:      &total2,
	}); err != nil {
		t.Fatalf("create channel2: %v", err)
	}
	if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{{
		ChannelID:   1,
		SourceKey:   "ann-1",
		Title:       "公告",
		Content:     "维护通知",
		FirstSeenAt: time.Now(),
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerDashboard(api, &Deps{
		Channels:      channels,
		Notifies:      notifies,
		Announcements: announcements,
		Rates:         rates,
	})

	req := httptest.NewRequest(http.MethodGet, "/api/dashboard/summary", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			TodayTotalCost float64 `json:"today_total_cost"`
			TotalCost      float64 `json:"total_cost"`
			Channels       []struct {
				TodayCost        float64 `json:"today_cost"`
				TotalCost        float64 `json:"total_cost"`
				TodayCostIsToday bool    `json:"today_cost_is_today"`
			} `json:"channels"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Data.TodayTotalCost != 4.0 {
		t.Fatalf("today total cost = %v, want 4", resp.Data.TodayTotalCost)
	}
	if resp.Data.TotalCost != 40.5 {
		t.Fatalf("total cost = %v, want 40.5", resp.Data.TotalCost)
	}
	if len(resp.Data.Channels) != 2 {
		t.Fatalf("channels len = %d, want 2", len(resp.Data.Channels))
	}
	if !resp.Data.Channels[0].TodayCostIsToday || !resp.Data.Channels[1].TodayCostIsToday {
		t.Fatal("today cost markers should be true for current samples")
	}
}

func TestChannelTodayMarkersUseConfiguredBusinessTimeZone(t *testing.T) {
	userTime, err := usertime.New("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	// Both instants are July 19 in Shanghai, while the sample is still July 18 in UTC.
	sampledAt := time.Date(2026, 7, 18, 16, 5, 0, 0, time.UTC)
	now := time.Date(2026, 7, 18, 23, 0, 0, 0, time.UTC)
	if !userTime.IsToday(&sampledAt, now) {
		t.Fatal("configured timezone did not treat the sample as today")
	}
	utcTime, err := usertime.New("UTC")
	if err != nil {
		t.Fatal(err)
	}
	if !utcTime.IsToday(&sampledAt, now) {
		t.Fatal("UTC control sample should still be on July 18")
	}
	nextUTCDate := time.Date(2026, 7, 19, 0, 5, 0, 0, time.UTC)
	if utcTime.IsToday(&sampledAt, nextUTCDate) {
		t.Fatal("UTC control unexpectedly crossed the business day")
	}
	if !userTime.IsToday(&sampledAt, nextUTCDate) {
		t.Fatal("Shanghai business day should remain July 19")
	}
	currentBusinessDay := userTime.DayStart(time.Now()).Add(time.Hour)
	output := channelOutputFor(&Deps{UserTime: userTime}, storage.Channel{TodayCostAt: &currentBusinessDay})
	if !output.TodayCostIsToday {
		t.Fatal("channel output ignored the configured business timezone")
	}
	previousBusinessDay := currentBusinessDay.AddDate(0, 0, -1)
	output = channelOutputFor(&Deps{UserTime: userTime}, storage.Channel{TodayCostAt: &previousBusinessDay})
	if output.TodayCostIsToday {
		t.Fatal("channel output treated the previous business day as today")
	}
}

func TestNotificationsLogsPage(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	notifies := storage.NewNotifications(db)
	if err := notifies.CreateChannel(&storage.NotificationChannel{
		ID:            1,
		Name:          "alpha",
		Type:          storage.NotifyTelegram,
		ConfigCipher:  "x",
		Subscriptions: "[]",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notify channel1: %v", err)
	}
	if err := notifies.CreateChannel(&storage.NotificationChannel{
		ID:            2,
		Name:          "beta",
		Type:          storage.NotifyWebhook,
		ConfigCipher:  "y",
		Subscriptions: "[]",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notify channel2: %v", err)
	}
	if err := notifies.CreateChannel(&storage.NotificationChannel{
		ID:            3,
		Name:          "gamma",
		Type:          storage.NotifyEmail,
		ConfigCipher:  "z",
		Subscriptions: "[]",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notify channel3: %v", err)
	}
	now := time.Now()
	for i := 0; i < 3; i++ {
		if err := notifies.AppendLog(&storage.NotificationLog{
			ChannelID: uint(i + 1),
			Event:     storage.EventAnnouncement,
			Subject:   "test",
			Success:   i%2 == 0,
			SentAt:    now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("append log: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerNotifications(api, &Deps{Notifies: notifies})

	req := httptest.NewRequest(http.MethodGet, "/api/notifications/logs?page=1&page_size=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items []struct {
				ChannelID   uint   `json:"channel_id"`
				ChannelName string `json:"channel_name"`
				ChannelType string `json:"channel_type"`
			} `json:"items"`
			Total int64 `json:"total"`
			Page  int   `json:"page"`
			Pages int   `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Items) != 2 || resp.Data.Total != 3 || resp.Data.Page != 1 || resp.Data.Pages != 2 {
		t.Fatalf("unexpected page response: %#v", resp.Data)
	}
	if resp.Data.Items[0].ChannelID != 3 || resp.Data.Items[0].ChannelName != "gamma" || resp.Data.Items[0].ChannelType != "email" {
		t.Fatalf("unexpected first item: %#v", resp.Data.Items[0])
	}
}

func TestChannelsPageAll(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	for i := 0; i < 3; i++ {
		if err := channels.Create(&storage.Channel{
			Name:           "channel-" + strconv.Itoa(i),
			Type:           storage.ChannelTypeNewAPI,
			SiteURL:        "https://example.com",
			Username:       "u",
			PasswordCipher: "x",
			MonitorEnabled: true,
		}); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerChannels(api, &Deps{Channels: channels})

	req := httptest.NewRequest(http.MethodGet, "/api/channels?page=1&page_size=-1", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items    []storage.Channel `json:"items"`
			Total    int64             `json:"total"`
			PageSize int               `json:"page_size"`
			Pages    int               `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Items) != 3 || resp.Data.Total != 3 || resp.Data.PageSize != -1 || resp.Data.Pages != 1 {
		t.Fatalf("unexpected all page response: %#v", resp.Data)
	}
}

func TestChannelsListSortOrder(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	items := []storage.Channel{
		{Name: "low", Type: storage.ChannelTypeNewAPI, SiteURL: "https://a.example.com", Username: "u", PasswordCipher: "x", SortOrder: 1, MonitorEnabled: true},
		{Name: "high", Type: storage.ChannelTypeNewAPI, SiteURL: "https://b.example.com", Username: "u", PasswordCipher: "x", SortOrder: 9, MonitorEnabled: true},
	}
	for i := range items {
		if err := channels.Create(&items[i]); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}

	r := gin.New()
	registerChannels(r.Group("/api"), &Deps{Channels: channels})

	req := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data []storage.Channel `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 2 || resp.Data[0].Name != "high" || resp.Data[1].Name != "low" {
		t.Fatalf("unexpected order: %#v", resp.Data)
	}
}

func TestChannelCostComparisonSeparatesRollingAndBusinessDayWindows(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	userTime, err := usertime.New("Asia/Shanghai")
	if err != nil {
		t.Fatalf("user time: %v", err)
	}
	now := time.Now().UTC()
	dayStart, _ := userTime.DayBoundsUTC(now)
	rollingStart := now.Add(-24 * time.Hour)
	previousBusinessDayWithinRollingWindow := rollingStart.Add(dayStart.Sub(rollingStart) / 2)
	accountToday := 9.0
	todaySample := dayStart.Add(now.Sub(dayStart) / 2)
	channel := storage.Channel{Name: "compared", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", PasswordCipher: "x", MonitorEnabled: true, TodayCost: &accountToday, TodayCostAt: &todaySample}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	staleCost := 20.0
	staleSample := dayStart.Add(-time.Hour)
	stale := storage.Channel{Name: "stale", Type: storage.ChannelTypeNewAPI, SiteURL: "https://stale.example.com", PasswordCipher: "x", MonitorEnabled: true, TodayCost: &staleCost, TodayCostAt: &staleSample}
	if err := channels.Create(&stale); err != nil {
		t.Fatalf("create stale channel: %v", err)
	}
	for _, requestLog := range []*storage.GatewayRequestLog{
		{ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, Cost: 2, CostStatus: storage.GatewayCostStatusReported, Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, CreatedAt: previousBusinessDayWithinRollingWindow},
		{ChannelID: channel.ID, ChannelNameSnapshot: channel.Name, Cost: 3, CostStatus: storage.GatewayCostStatusReported, Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, CreatedAt: todaySample},
	} {
		if err := store.AppendRequestLog(requestLog); err != nil {
			t.Fatalf("append request log: %v", err)
		}
	}

	router := gin.New()
	registerChannels(router.Group("/api"), &Deps{Channels: channels, Gateway: &gateway.Service{Store: store}, UserTime: userTime})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/channels/cost-comparison", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data struct {
			Rows []channelCostComparisonOutput `json:"rows"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Data.Rows) != 2 {
		t.Fatalf("rows = %#v", response.Data.Rows)
	}
	compared := response.Data.Rows[0]
	if compared.ChannelID != channel.ID || compared.GatewayCost24Hours != 5 || compared.GatewayCostToday != 3 {
		t.Fatalf("compared row = %#v", compared)
	}
	if compared.AccountCostToday == nil || *compared.AccountCostToday != 9 || compared.Difference24Hours == nil || *compared.Difference24Hours != 4 || compared.DifferenceToday == nil || *compared.DifferenceToday != 6 {
		t.Fatalf("compared differences = %#v", compared)
	}
	if response.Data.Rows[1].AccountCostToday != nil {
		t.Fatalf("stale account snapshot was treated as today: %#v", response.Data.Rows[1])
	}
}

func TestChannelsListIncludesNewAPIUserID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	enc, err := cipher.Encrypt(`{"cookie":"session=secret","user_id":"123"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := channels.Create(&storage.Channel{
		Name:           "newapi-token",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: enc,
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	r := gin.New()
	registerChannels(r.Group("/api"), &Deps{Channels: channels, Cipher: cipher})

	req := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "session=secret") {
		t.Fatalf("response leaked cookie: %s", rec.Body.String())
	}
	var resp struct {
		Data []struct {
			UserID string `json:"user_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 1 || resp.Data[0].UserID != "123" {
		t.Fatalf("unexpected response: %#v", resp.Data)
	}
}

func TestAnnouncementsPage(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	announcements := storage.NewUpstreamAnnouncements(db)
	now := time.Now()
	publishedOld := now.Add(-3 * time.Hour)
	publishedMid := now.Add(-2 * time.Hour)
	publishedNew := now.Add(-1 * time.Hour)
	items := []storage.UpstreamAnnouncement{
		{ChannelID: 1, SourceKey: "ann-0", Title: "公告", Content: "维护通知", PublishedAt: &publishedMid, FirstSeenAt: now.Add(3 * time.Minute)},
		{ChannelID: 1, SourceKey: "ann-1", Title: "公告", Content: "维护通知", PublishedAt: &publishedNew, FirstSeenAt: now.Add(1 * time.Minute)},
		{ChannelID: 1, SourceKey: "ann-2", Title: "公告", Content: "维护通知", PublishedAt: &publishedOld, FirstSeenAt: now.Add(2 * time.Minute)},
	}
	for _, item := range items {
		item := item
		if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{item}); err != nil {
			t.Fatalf("sync announcements: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerAnnouncements(api, &Deps{Announcements: announcements})

	req := httptest.NewRequest(http.MethodGet, "/api/announcements?page=1&page_size=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items []storage.UpstreamAnnouncement `json:"items"`
			Total int64                          `json:"total"`
			Page  int                            `json:"page"`
			Pages int                            `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Items) != 2 || resp.Data.Total != 3 || resp.Data.Page != 1 || resp.Data.Pages != 2 {
		t.Fatalf("unexpected page response: %#v", resp.Data)
	}
	if resp.Data.Items[0].SourceKey != "ann-1" || resp.Data.Items[1].SourceKey != "ann-0" {
		t.Fatalf("unexpected announcement order: %#v", resp.Data.Items)
	}
}

func TestRateChangesPage(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	rates := storage.NewRates(db)
	now := time.Now()
	for i := 0; i < 3; i++ {
		if err := rates.AppendChange(&storage.RateChangeLog{
			ChannelID: uint((i % 2) + 1),
			ModelName: "gpt-" + strconv.Itoa(i),
			NewRatio:  float64(i + 1),
			ChangedAt: now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("append rate change: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerRates(api, &Deps{Rates: rates})

	req := httptest.NewRequest(http.MethodGet, "/api/rate-changes?page=1&page_size=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items []storage.RateChangeLog `json:"items"`
			Total int64                   `json:"total"`
			Page  int                     `json:"page"`
			Pages int                     `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Items) != 2 || resp.Data.Total != 3 || resp.Data.Page != 1 || resp.Data.Pages != 2 {
		t.Fatalf("unexpected page response: %#v", resp.Data)
	}
	if resp.Data.Items[0].ModelName != "gpt-2" || resp.Data.Items[1].ModelName != "gpt-1" {
		t.Fatalf("unexpected order: %#v", resp.Data.Items)
	}
}

func TestRateChangesPageFiltersChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	rates := storage.NewRates(db)
	now := time.Now()
	for i := 0; i < 4; i++ {
		channelID := uint(1)
		if i >= 2 {
			channelID = 2
		}
		if err := rates.AppendChange(&storage.RateChangeLog{
			ChannelID: channelID,
			ModelName: "model-" + strconv.Itoa(i),
			NewRatio:  float64(i + 1),
			ChangedAt: now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("append rate change: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerRates(api, &Deps{Rates: rates})

	req := httptest.NewRequest(http.MethodGet, "/api/rate-changes?page=1&page_size=10&channel_id=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items []storage.RateChangeLog `json:"items"`
			Total int64                   `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Data.Total != 2 || len(resp.Data.Items) != 2 {
		t.Fatalf("unexpected filtered response: %#v", resp.Data)
	}
	for _, item := range resp.Data.Items {
		if item.ChannelID != 2 {
			t.Fatalf("unexpected channel id: %#v", resp.Data.Items)
		}
	}
}

func TestRateChangeAnalyticsValidatesWindowAndFiltersChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	rates := storage.NewRates(db)
	old := 1.0
	for _, item := range []*storage.RateChangeLog{
		{ChannelID: 1, ModelName: "gpt-a", OldRatio: &old, NewRatio: 2, ChangedAt: time.Now().Add(-time.Hour)},
		{ChannelID: 2, ModelName: "gpt-b", OldRatio: &old, NewRatio: 3, ChangedAt: time.Now().Add(-time.Hour)},
	} {
		if err := rates.AppendChange(item); err != nil {
			t.Fatalf("append rate change: %v", err)
		}
	}
	r := gin.New()
	registerRates(r.Group("/api"), &Deps{Rates: rates})

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/rate-change-analytics?days=7&channel_id=1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Data storage.RateChangeAnalytics `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.Total != 1 || response.Data.Up != 1 || len(response.Data.Recent) != 1 || response.Data.Recent[0].ChannelID != 1 {
		t.Fatalf("analytics = %+v", response.Data)
	}

	invalid := httptest.NewRecorder()
	r.ServeHTTP(invalid, httptest.NewRequest(http.MethodGet, "/api/rate-change-analytics?days=365", nil))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid days status = %d body = %s", invalid.Code, invalid.Body.String())
	}
}

func TestDashboardCostTrend(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	notifies := storage.NewNotifications(db)
	rates := storage.NewRates(db)

	if err := channels.Create(&storage.Channel{
		Name:           "a",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://a.example.com",
		Username:       "u1",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel1: %v", err)
	}
	if err := channels.Create(&storage.Channel{
		Name:           "b",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://b.example.com",
		Username:       "u2",
		PasswordCipher: "y",
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel2: %v", err)
	}

	trendLoc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		trendLoc = time.FixedZone("Asia/Shanghai", 8*60*60)
	}
	now := time.Now().In(trendLoc)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLoc)
	day1 := day0.AddDate(0, 0, -1)
	if err := rates.AppendCost(&storage.CostSnapshot{ChannelID: 1, TodayCost: 1.2, SampledAt: day1.Add(10 * time.Hour)}); err != nil {
		t.Fatalf("append cost1: %v", err)
	}
	if err := rates.AppendCost(&storage.CostSnapshot{ChannelID: 1, TodayCost: 2.2, SampledAt: day1.Add(22 * time.Hour)}); err != nil {
		t.Fatalf("append cost2: %v", err)
	}
	if err := rates.AppendCost(&storage.CostSnapshot{ChannelID: 2, TodayCost: 0.8, SampledAt: day1.Add(20 * time.Hour)}); err != nil {
		t.Fatalf("append cost3: %v", err)
	}
	if err := rates.AppendCost(&storage.CostSnapshot{ChannelID: 2, TodayCost: 3.5, SampledAt: day0.Add(9 * time.Hour)}); err != nil {
		t.Fatalf("append cost4: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerDashboard(api, &Deps{
		Channels: channels,
		Notifies: notifies,
		Rates:    rates,
	})

	req := httptest.NewRequest(http.MethodGet, "/api/dashboard/cost-trend?days=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data []struct {
			Day  string  `json:"day"`
			Cost float64 `json:"cost"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("trend len = %d, want 2", len(resp.Data))
	}
	if resp.Data[0].Cost != 3.0 {
		t.Fatalf("day1 cost = %v, want 3.0", resp.Data[0].Cost)
	}
	if resp.Data[1].Cost != 3.5 {
		t.Fatalf("day0 cost = %v, want 3.5", resp.Data[1].Cost)
	}
}

func TestDashboardMultiplierTrendRequiresBoundedRange(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api")
	registerDashboard(api, &Deps{Gateway: &gateway.Service{Store: &storage.Gateway{}}})

	for _, testCase := range []struct {
		name string
		path string
	}{
		{name: "missing range", path: "/api/dashboard/multiplier-trend"},
		{name: "missing end", path: "/api/dashboard/multiplier-trend?start=2026-07-01T00:00:00Z"},
		{name: "oversized range", path: "/api/dashboard/multiplier-trend?start=2026-07-01T00:00:00Z&end=2026-08-02T00:00:00Z"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, testCase.path, nil)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestDashboardMultiplierTrendUsesFinalPhysicalDispatchFacts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	store := storage.NewGateway(db)
	userTime, err := usertime.New("America/New_York")
	if err != nil {
		t.Fatalf("user time: %v", err)
	}
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("location: %v", err)
	}
	localSuccessAt := time.Date(2026, 3, 8, 0, 30, 0, 0, location)
	routeSnapshot, err := storage.NewGatewayRouteExclusionSnapshot(localSuccessAt, []storage.GatewayRouteExclusionSnapshotEntry{})
	if err != nil {
		t.Fatalf("route snapshot: %v", err)
	}
	appendFinalPhysicalDispatch := func(log *storage.GatewayRequestLog, app storage.GatewayApp, groupRatio, completionRatio float64, sequence int) {
		t.Helper()
		if log.GroupName == "" {
			log.GroupName = "primary"
		}
		if log.ModelName == "" {
			log.ModelName = "gpt-test"
		}
		log.TelemetryVersion = storage.GatewayRequestTelemetryVersion
		log.LogicalRequestTraceID = fmt.Sprintf("%032x", sequence)
		log.RoutingMode = storage.GatewayRoutingModeSmart
		log.RouteExclusionSnapshotVersion = storage.GatewayRouteExclusionSnapshotVersion
		log.RouteExclusionsSnapshot = routeSnapshot
		log.ChannelIdentityRevision = 1
		log.UpstreamGroupID = 1
		log.UpstreamGroupGeneration = 1
		log.UpstreamKeyLocalID = 1
		log.UpstreamKeyGeneration = 1
		log.PriceCanonicalGroupName = log.GroupName
		log.PriceModelName = log.ModelName
		log.PriceOperation = storage.GatewayPricingOperationTextGeneration
		log.UsageSchema = storage.GatewayPricingUsageSchemaTokensV1
		log.UsageSchemaVersion = 1
		if app == storage.GatewayAppClaude {
			log.PriceProtocol = storage.GatewayPricingProtocolAnthropicMessages
		} else {
			log.PriceProtocol = storage.GatewayPricingProtocolOpenAIResponses
		}
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append valid telemetered log: %v", err)
		}
		terminalAt := log.CreatedAt
		if err := store.RegisterRequestDispatchOwner(context.Background(), log.CreatedAt.Add(-2*time.Second), log.CreatedAt.Add(time.Minute)); err != nil {
			t.Fatalf("register dispatch owner: %v", err)
		}
		ownerInstanceID, ownerFenceToken, err := store.RequestDispatchOwnerIdentity()
		if err != nil {
			t.Fatalf("load dispatch owner identity: %v", err)
		}
		audit := &storage.GatewayRequestDispatchAudit{
			LogicalRequestTraceID:           log.LogicalRequestTraceID,
			TelemetryVersion:                storage.GatewayRequestTelemetryVersion,
			RoutingMode:                     storage.GatewayRoutingModeSmart,
			GatewayApp:                      string(app),
			PhysicallyDispatched:            false,
			PhysicalBillingInvariantVersion: storage.GatewayRequestDispatchPhysicalBillingInvariantVersion,
			TransportFenceVersion:           storage.GatewayRequestDispatchTransportFenceVersion,
			TransportState:                  storage.GatewayRequestDispatchTransportStatePrepared,
			RouteExclusionSnapshotVersion:   storage.GatewayRouteExclusionSnapshotVersion,
			RouteExclusionsSnapshot:         routeSnapshot,
			DownstreamKeyID:                 log.DownstreamKeyID,
			ChannelID:                       log.ChannelID,
			GroupName:                       log.GroupName,
			ModelName:                       log.ModelName,
			Method:                          log.Method,
			Path:                            log.Path,
			RouteReason:                     log.RouteReason,
			GroupRatio:                      groupRatio,
			CompletionRatio:                 completionRatio,
			MultiplierProvenanceVersion:     storage.GatewayRequestDispatchMultiplierProvenanceVersion,
			ChannelIdentityRevision:         log.ChannelIdentityRevision,
			UpstreamGroupID:                 log.UpstreamGroupID,
			UpstreamGroupGeneration:         log.UpstreamGroupGeneration,
			UpstreamKeyLocalID:              log.UpstreamKeyLocalID,
			UpstreamKeyGeneration:           log.UpstreamKeyGeneration,
			PriceCanonicalGroupName:         log.PriceCanonicalGroupName,
			PriceModelName:                  log.PriceModelName,
			PriceOperation:                  log.PriceOperation,
			PriceProtocol:                   log.PriceProtocol,
			UsageSchema:                     log.UsageSchema,
			UsageSchemaVersion:              log.UsageSchemaVersion,
			RequestStartedAt:                log.CreatedAt.Add(-time.Second),
			OwnerInstanceID:                 ownerInstanceID,
			OwnerFenceToken:                 ownerFenceToken,
			Stage:                           storage.GatewayRequestDispatchStageDispatched,
			DispatchedAt:                    log.CreatedAt.Add(-time.Second),
			CreatedAt:                       log.CreatedAt.Add(-time.Second),
		}
		if err := db.Create(audit).Error; err != nil {
			t.Fatalf("append prepared physical dispatch audit: %v", err)
		}
		if err := db.Model(&storage.GatewayRequestDispatchAudit{}).Where("id = ?", audit.ID).Updates(map[string]any{
			"transport_state":       storage.GatewayRequestDispatchTransportStateArmed,
			"physically_dispatched": true,
		}).Error; err != nil {
			t.Fatalf("arm physical dispatch audit: %v", err)
		}
		confirmedAt := log.CreatedAt.Add(-500 * time.Millisecond)
		if err := db.Model(&storage.GatewayRequestDispatchAudit{}).Where("id = ?", audit.ID).Updates(map[string]any{
			"transport_state":               storage.GatewayRequestDispatchTransportStateConfirmed,
			"transport_return_confirmed":    true,
			"transport_return_confirmed_at": confirmedAt,
		}).Error; err != nil {
			t.Fatalf("confirm physical dispatch audit: %v", err)
		}
		if err := db.Model(&storage.GatewayRequestDispatchAudit{}).Where("id = ?", audit.ID).Updates(map[string]any{
			"stage":          storage.GatewayRequestDispatchStageFinalized,
			"terminal_at":    terminalAt,
			"terminal_kind":  "success",
			"request_log_id": log.ID,
		}).Error; err != nil {
			t.Fatalf("finalize physical dispatch audit: %v", err)
		}
	}
	codex := &storage.GatewayRequestLog{
		DownstreamKeyID: 1, ChannelID: 1, GatewayApp: string(storage.GatewayAppCodex),
		GroupRatio: 2, CompletionRatio: 3, PromptTokens: 100, CompletionTokens: 20,
		Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, CreatedAt: localSuccessAt.UTC(),
	}
	appendFinalPhysicalDispatch(codex, storage.GatewayAppCodex, 2, 3, 1)
	// Funding reconciliation can rewrite request-log accounting projections.
	// The dashboard must instead retain the dispatch-time multiplier fact.
	if err := db.Model(&storage.GatewayRequestLog{}).Where("id = ?", codex.ID).UpdateColumns(map[string]any{
		"group_ratio":      99.0,
		"completion_ratio": 98.0,
	}).Error; err != nil {
		t.Fatalf("simulate mutable request-log correction: %v", err)
	}
	appendFinalPhysicalDispatch(&storage.GatewayRequestLog{
		DownstreamKeyID: 2, ChannelID: 1, GatewayApp: string(storage.GatewayAppClaude),
		GroupRatio: 1.5, CompletionRatio: 4, PromptTokens: 100, CompletionTokens: 50,
		Method: http.MethodPost, Path: "/v1/messages", StatusCode: http.StatusOK, CreatedAt: localSuccessAt.Add(30 * time.Minute).UTC(),
	}, storage.GatewayAppClaude, 1.5, 4, 2)
	// A frozen 0x multiplier is an explicit free-credit fact, not missing
	// coverage. It must keep the request's tokens in both denominator and
	// multiplier coverage.
	appendFinalPhysicalDispatch(&storage.GatewayRequestLog{
		DownstreamKeyID: 3, ChannelID: 1, GatewayApp: string(storage.GatewayAppClaude),
		GroupRatio: 0, CompletionRatio: 0, PromptTokens: 20, CompletionTokens: 10,
		Method: http.MethodPost, Path: "/v1/messages", StatusCode: http.StatusOK, CreatedAt: localSuccessAt.Add(time.Hour).UTC(),
	}, storage.GatewayAppClaude, 0, 0, 3)
	appendFinalPhysicalDispatch(&storage.GatewayRequestLog{
		DownstreamKeyID: 4, ChannelID: 1, GatewayApp: string(storage.GatewayAppCodex), RouteReason: "client_canceled",
		GroupRatio: 99, CompletionRatio: 99, PromptTokens: 999, CompletionTokens: 999,
		Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, CreatedAt: localSuccessAt.Add(30 * time.Minute).UTC(),
	}, storage.GatewayAppCodex, 99, 99, 4)
	// A known Codex v1 request without a trusted audit remains an honest token
	// denominator. It gets no multiplier coverage and must not be backfilled.
	missingAudit := &storage.GatewayRequestLog{
		DownstreamKeyID: 5, ChannelID: 1, GatewayApp: string(storage.GatewayAppCodex),
		GroupName: "primary", ModelName: "gpt-test", GroupRatio: 77, CompletionRatio: 77, PromptTokens: 777, CompletionTokens: 777,
		Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, CreatedAt: localSuccessAt.Add(90 * time.Minute).UTC(),
		TelemetryVersion: storage.GatewayRequestTelemetryVersion, LogicalRequestTraceID: fmt.Sprintf("%032x", 5), RoutingMode: storage.GatewayRoutingModeSmart,
		RouteExclusionSnapshotVersion: storage.GatewayRouteExclusionSnapshotVersion, RouteExclusionsSnapshot: routeSnapshot,
		ChannelIdentityRevision: 1, UpstreamGroupID: 1, UpstreamGroupGeneration: 1, UpstreamKeyLocalID: 1, UpstreamKeyGeneration: 1,
		PriceCanonicalGroupName: "primary", PriceModelName: "gpt-test", PriceOperation: storage.GatewayPricingOperationTextGeneration,
		PriceProtocol: storage.GatewayPricingProtocolOpenAIResponses, UsageSchema: storage.GatewayPricingUsageSchemaTokensV1, UsageSchemaVersion: 1,
	}
	if err := store.AppendRequestLog(missingAudit); err != nil {
		t.Fatalf("append missing-audit log: %v", err)
	}

	r := gin.New()
	registerDashboard(r.Group("/api"), &Deps{Gateway: &gateway.Service{Store: store}, UserTime: userTime})
	req := httptest.NewRequest(http.MethodGet, "/api/dashboard/multiplier-trend?start=2026-03-01T05:00:00Z&end=2026-03-15T04:00:00Z", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Data storage.DashboardMultiplierTrend `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	points := map[string]storage.DashboardMultiplierTrendPoint{}
	apps := map[string]storage.DashboardMultiplierTrendApp{}
	for _, app := range response.Data.Apps {
		apps[app.App] = app
		for index := range app.Points {
			points[app.App+":"+app.Points[index].Bucket] = app.Points[index]
		}
	}
	if len(apps) != 2 || apps[string(storage.GatewayAppCodex)].App != string(storage.GatewayAppCodex) || apps[string(storage.GatewayAppClaude)].App != string(storage.GatewayAppClaude) {
		t.Fatalf("expected separate Codex/Claude series, got %+v", response.Data.Apps)
	}
	codexPoint, codexOK := points[string(storage.GatewayAppCodex)+":2026-03-08"]
	claudePoint, claudeOK := points[string(storage.GatewayAppClaude)+":2026-03-08"]
	if !codexOK || !claudeOK {
		t.Fatalf("configured local DST day bucket missing: %+v", response.Data)
	}
	if codexPoint.SuccessfulRequests != 2 || codexPoint.InputTokens != 877 || codexPoint.OutputTokens != 797 || codexPoint.InputCoveredTokens != 100 || codexPoint.OutputCoveredTokens != 20 {
		t.Fatalf("Codex denominator/coverage did not retain the unaudited request as a gap: %+v", codexPoint)
	}
	if codexPoint.InputMultiplier == nil || *codexPoint.InputMultiplier != 2 || codexPoint.OutputMultiplier == nil || *codexPoint.OutputMultiplier != 3 {
		t.Fatalf("Codex multiplier read mutable request-log values instead of audit: %+v", codexPoint)
	}
	if claudePoint.SuccessfulRequests != 2 || claudePoint.InputTokens != 120 || claudePoint.OutputTokens != 60 || claudePoint.InputCoveredTokens != 120 || claudePoint.OutputCoveredTokens != 60 {
		t.Fatalf("Claude coverage = %+v", claudePoint)
	}
	if claudePoint.InputMultiplier == nil || math.Abs(*claudePoint.InputMultiplier-1.25) > 1e-12 || claudePoint.OutputMultiplier == nil || math.Abs(*claudePoint.OutputMultiplier-(10.0/3.0)) > 1e-12 {
		t.Fatalf("Claude multiplier = %+v", claudePoint)
	}
	noData, noDataOK := points[string(storage.GatewayAppCodex)+":2026-03-09"]
	if !noDataOK || noData.SuccessfulRequests != 0 || noData.InputTokens != 0 || noData.OutputTokens != 0 ||
		noData.InputCoveredTokens != 0 || noData.OutputCoveredTokens != 0 ||
		noData.InputSampleRequests != 0 || noData.OutputSampleRequests != 0 ||
		noData.InputMultiplier != nil || noData.OutputMultiplier != nil ||
		noData.SmoothedInputMultiplier != nil || noData.SmoothedOutputMultiplier != nil ||
		noData.SmoothedInputCoveredTokens != 0 || noData.SmoothedOutputCoveredTokens != 0 ||
		noData.SmoothedInputSampleRequests != 0 || noData.SmoothedOutputSampleRequests != 0 {
		t.Fatalf("no-data bucket must remain explicit and uncovered: %+v", noData)
	}
}

func TestSyncAllChannelsEmitsChannelMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	notifies := storage.NewNotifications(db)

	if err := channels.Create(&storage.Channel{
		Name:           "a",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://a.example.com",
		Username:       "u1",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerChannels(api, &Deps{
		Channels: channels,
		Rates:    rates,
		Notifies: notifies,
		Monitor:  &stubMonitor{},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/channels/sync-all", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected sse body")
	}
	first := findFirstJSONBody(rec.Body.String())
	if !json.Valid([]byte(first)) {
		t.Fatal("invalid sse payload")
	}
	var ev struct {
		Stage     string `json:"stage"`
		ChannelID uint   `json:"channel_id"`
		Index     int    `json:"index"`
		Total     int    `json:"total"`
	}
	if err := json.Unmarshal([]byte(first), &ev); err != nil {
		t.Fatalf("decode sse payload: %v", err)
	}
	if ev.ChannelID != 1 || ev.Index != 1 || ev.Total != 1 {
		t.Fatalf("metadata mismatch: %#v", ev)
	}
}

func TestSyncChannelTreatsAccountingGatesAsWarnings(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	notifies := storage.NewNotifications(db)
	channelRecord := &storage.Channel{
		Name: "accounting-warning", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://accounting-warning.example", Username: "u", PasswordCipher: "x", MonitorEnabled: true,
	}
	if err := channels.Create(channelRecord); err != nil {
		t.Fatal(err)
	}
	monitor := &stubMonitor{
		balanceErr: fmt.Errorf("%w: completed-day history is incomplete", costing.ErrCostUnitEvidenceUnavailable),
		rateErr:    fmt.Errorf("%w: persisted advertised factor evidence is incomplete", costing.ErrCostUnitEvidenceUnavailable),
	}
	r := gin.New()
	registerChannels(r.Group("/api"), &Deps{Channels: channels, Rates: rates, Notifies: notifies, Monitor: monitor})
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/channels/%d/sync", channelRecord.ID), nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, `"stage":"error"`) || !strings.Contains(body, `"stage":"done"`) ||
		!strings.Contains(body, "同步完成，账务采集待重试") {
		t.Fatalf("accounting-only sync was reported as failed: %s", body)
	}
}

func TestSyncChannelChecksSubscriptionAlerts(t *testing.T) {
	syncSubscriptionAlertTest(t, "/api/channels/1/sync")
}

func TestSyncAllChannelsChecksSubscriptionAlerts(t *testing.T) {
	syncSubscriptionAlertTest(t, "/api/channels/sync-all")
}

func syncSubscriptionAlertTest(t *testing.T, path string) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()

	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncryptAPI(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}

	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/settings/public":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"turnstile_enabled":false}}`))
		case "/api/v1/auth/login":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"access_token":"token","expires_in":3600}}`))
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"balance":10}}`))
		case "/api/v1/usage/dashboard/stats":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"today_actual_cost":1,"total_actual_cost":2}}`))
		case "/api/v1/subscriptions/progress":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"subscription":{"id":9,"group_id":3,"status":"active","expires_at":"2026-06-22T00:00:00Z","group":{"id":3,"name":"pro"}},"progress":{"id":9,"group_name":"pro","daily":{"limit_usd":1,"used_usd":0.5,"remaining_usd":0.5,"percentage":50}}}]}`))
		case "/api/v1/groups/available":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"name":"pro","rate_multiplier":1}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	if err := channels.Create(&storage.Channel{
		Name:                "sub",
		Type:                storage.ChannelTypeSub2API,
		SiteURL:             apiSrv.URL,
		Username:            "u",
		PasswordCipher:      mustEncryptAPI(t, cipher, "p"),
		CredentialMode:      storage.CredentialModePassword,
		SubscriptionEnabled: true,
		MonitorEnabled:      true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		SubscriptionDailyRemainingThresholdPct: 90,
		SubscriptionAlertCooldown:              time.Hour,
		SendMaxAttempts:                        1,
	})
	monitorSvc := monitor.NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))

	r := gin.New()
	registerChannels(r.Group("/api"), &Deps{
		Channels: channels,
		Rates:    rates,
		Notifies: notifies,
		Monitor:  monitorSvc,
	})

	req := httptest.NewRequest(http.MethodPost, path, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	logs, err := notifies.ListLogs(10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	found := false
	for _, log := range logs {
		if log.Event == storage.EventSubscriptionDailyLow && log.Success {
			found = true
		}
	}
	if !found {
		t.Fatalf("subscription alert log not found: %#v", logs)
	}
}

func TestScheduleChannelGatewayInitializationCoalescesPendingRun(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	channel := &storage.Channel{Name: "token", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", PasswordCipher: "x", CredentialMode: storage.CredentialModeToken, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	started := make(chan uint, 2)
	release := make(chan struct{})
	monitor := &stubMonitor{rateStarted: started, rateRelease: release}
	deps := &Deps{Channels: channels, Monitor: monitor}
	channelGatewayInitializations.Delete(channel.ID)
	defer channelGatewayInitializations.Delete(channel.ID)
	scheduleChannelGatewayInitialization(deps, channel.ID)
	scheduleChannelGatewayInitialization(deps, channel.ID)
	select {
	case got := <-started:
		if got != channel.ID {
			t.Fatalf("initialized channel = %d, want %d", got, channel.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway initialization did not start")
	}
	select {
	case duplicate := <-started:
		t.Fatalf("duplicate gateway initialization started for channel %d", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case got := <-started:
		if got != channel.ID {
			t.Fatalf("pending initialization channel = %d, want %d", got, channel.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("pending gateway initialization was dropped")
	}
}

type stubMonitor struct {
	rateStarted chan<- uint
	rateRelease <-chan struct{}
	balanceErr  error
	rateErr     error
}

func (s *stubMonitor) RefreshBalance(ctx context.Context, c *storage.Channel) error {
	progress.OK(ctx, progress.StageBalance, "ok")
	return s.balanceErr
}

func (s *stubMonitor) RefreshRates(ctx context.Context, c *storage.Channel) error {
	if s.rateStarted != nil {
		s.rateStarted <- c.ID
	}
	if s.rateRelease != nil {
		select {
		case <-s.rateRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.rateErr
}

func (s *stubMonitor) RefreshUpstreamGroupFacts(ctx context.Context, c *storage.Channel) error {
	return s.RefreshRates(ctx, c)
}

func (s *stubMonitor) CheckSubscriptionUsageAlerts(ctx context.Context, c *storage.Channel) error {
	return nil
}

func mustEncryptAPI(t *testing.T, cipher *crypto.Cipher, plain string) string {
	t.Helper()
	out, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	return out
}

func findFirstJSONBody(s string) string {
	start := -1
	for i := 0; i < len(s); i++ {
		if s[i] == '{' {
			start = i
			break
		}
	}
	if start < 0 {
		return ""
	}
	end := start
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				end = i + 1
				return s[start:end]
			}
		}
	}
	return ""
}

func TestDeleteChannelReturnsPendingRemoteCleanupAndRetainsRecoveryEvidence(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	notifies := storage.NewNotifications(db)
	rates := storage.NewRates(db)
	monLogs := storage.NewMonitorLogs(db)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monLogs, cipher)

	if err := channels.Create(&storage.Channel{
		ID:             1,
		Name:           "a",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://a.example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := db.Create(&storage.AuthSession{ChannelID: 1, AccessTokenCipher: "session-cipher"}).Error; err != nil {
		t.Fatalf("create auth session: %v", err)
	}
	if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{{
		ChannelID:   1,
		SourceKey:   "a",
		Content:     "one",
		FirstSeenAt: time.Now(),
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if err := notifies.AppendLog(&storage.NotificationLog{
		ChannelID:         99,
		UpstreamChannelID: 1,
		Event:             storage.EventBalanceLow,
		Subject:           "a 余额低于阈值",
		Success:           true,
		SentAt:            time.Now(),
	}); err != nil {
		t.Fatalf("append log: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerChannels(api, &Deps{
		Channels:   channels,
		ChannelSvc: channelSvc,
		Gateway:    gateway.NewService(channels, rates, storage.NewGateway(db), cipher, nil, nil),
	})

	req := httptest.NewRequest(http.MethodDelete, "/api/channels/1", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"status":"pending_remote_cleanup"`) || !strings.Contains(rec.Body.String(), "本地删除已准备，远端清理尚未完成") {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	// A repeat request must describe the same pending remote cleanup rather
	// than reporting a physical deletion that has not happened yet.
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"status":"pending_remote_cleanup"`) {
		t.Fatalf("idempotent status = %d, body = %s", rec.Code, rec.Body.String())
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list announcements: %v", err)
	}
	if total != 1 || len(list) != 1 {
		t.Fatalf("announcements must remain for remote-cleanup recovery: total=%d list=%#v", total, list)
	}
	logs, total, err := notifies.ListLogsPage(1, 10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if total != 1 || len(logs) != 1 {
		t.Fatalf("notification logs must remain for remote-cleanup recovery: total=%d list=%#v", total, logs)
	}
	if session, err := authSessions.FindByChannel(1); err != nil || session == nil || session.AccessTokenCipher != "session-cipher" {
		t.Fatalf("service eagerly removed AuthSession during pending delete: session=%+v err=%v", session, err)
	}
	stored, err := channels.FindByID(1)
	if err != nil || !stored.DeletionPending || stored.DeletionEpoch != 1 {
		t.Fatalf("pending channel state=%+v err=%v", stored, err)
	}
}

func TestChannelCreateAndUpdateIgnoreAnnouncements(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	notifies := storage.NewNotifications(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	monLogs := storage.NewMonitorLogs(db)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monLogs, cipher)

	r := gin.New()
	api := r.Group("/api")
	registerChannels(api, &Deps{Channels: channels, ChannelSvc: channelSvc, Notifies: notifies})

	createReq := httptest.NewRequest(http.MethodPost, "/api/channels", strings.NewReader(`{
			"name":"demo",
			"type":"sub2api",
			"site_url":"https://a.example.com",
			"username":"u",
			"password":"p",
		"ignore_announcements":true,
		"subscription_enabled":true
	}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	r.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d body = %s", createRec.Code, createRec.Body.String())
	}

	ch, err := channels.FindByID(1)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if !ch.IgnoreAnnouncements {
		t.Fatalf("ignore_announcements = false, want true")
	}
	if !ch.SubscriptionEnabled {
		t.Fatalf("subscription_enabled = false, want true")
	}

	updateReq := httptest.NewRequest(http.MethodPut, "/api/channels/1", strings.NewReader(fmt.Sprintf(`{
		"expected_admin_revision":%d,
		"ignore_announcements":false,
		"subscription_enabled":false
	}`, ch.AdminRevision)))
	updateReq.Header.Set("Content-Type", "application/json")
	updateRec := httptest.NewRecorder()
	r.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status = %d body = %s", updateRec.Code, updateRec.Body.String())
	}
	ch, err = channels.FindByID(1)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if ch.IgnoreAnnouncements {
		t.Fatalf("ignore_announcements = true, want false")
	}
	if ch.SubscriptionEnabled {
		t.Fatalf("subscription_enabled = true, want false")
	}

	newAPIReq := httptest.NewRequest(http.MethodPost, "/api/channels", strings.NewReader(`{
		"name":"demo-newapi",
		"type":"newapi",
		"site_url":"https://b.example.com",
		"username":"u",
		"password":"p",
		"subscription_enabled":true
	}`))
	newAPIReq.Header.Set("Content-Type", "application/json")
	newAPIRec := httptest.NewRecorder()
	r.ServeHTTP(newAPIRec, newAPIReq)
	if newAPIRec.Code != http.StatusOK {
		t.Fatalf("newapi create status = %d body = %s", newAPIRec.Code, newAPIRec.Body.String())
	}
	newAPICh, err := channels.FindByID(2)
	if err != nil {
		t.Fatalf("find newapi channel: %v", err)
	}
	if !newAPICh.SubscriptionEnabled {
		t.Fatalf("newapi subscription_enabled = false, want true")
	}
}

func TestChannelAPIDisallowsLegacyGlobalProxyFlag(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	cipher, err := crypto.NewCipher("test-channel-proxy-secret")
	if err != nil {
		t.Fatal(err)
	}
	channelSvc := channel.NewService(channels, storage.NewAuthSessions(db), storage.NewCaptchas(db), storage.NewRates(db), storage.NewMonitorLogs(db), cipher)
	router := gin.New()
	registerChannels(router.Group("/api"), &Deps{Channels: channels, ChannelSvc: channelSvc})

	request := httptest.NewRequest(http.MethodPost, "/api/channels", strings.NewReader(`{
		"name":"legacy-proxy", "type":"newapi", "site_url":"https://example.com", "username":"u", "password":"p", "proxy_enabled":true
	}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "网络代理") {
		t.Fatalf("legacy proxy create status=%d body=%s", response.Code, response.Body.String())
	}
}
