package newapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
)

func TestSetHTTPConfigAppliesUserAgentAndTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "custom-agent" {
			t.Fatalf("user agent = %q", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{}}`))
	}))
	defer srv.Close()

	c := New()
	c.SetHTTPConfig(connector.HTTPConfig{
		Timeout:   45 * time.Second,
		UserAgent: "custom-agent",
	})
	if c.http.GetClient().Timeout != 45*time.Second {
		t.Fatalf("timeout = %s", c.http.GetClient().Timeout)
	}
	transport, ok := c.http.GetClient().Transport.(*http.Transport)
	if !ok || transport.TLSHandshakeTimeout != 45*time.Second {
		t.Fatalf("TLS handshake timeout = %#v", c.http.GetClient().Transport)
	}
	if _, err := c.getJSON(context.Background(), srv.URL, nil); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
}

func TestLoginAddsExtraParams(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["username"] != "u" || body["password"] != "p" || body["device_id"] != "d1" {
			t.Fatalf("body = %#v", body)
		}
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc"})
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":7}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session, err := c.Login(context.Background(), &connector.Channel{
		SiteURL:          srv.URL,
		Username:         "u",
		Password:         "p",
		LoginExtraParams: map[string]any{"device_id": "d1"},
	})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.UserID != "7" || session.Cookie == "" {
		t.Fatalf("session = %#v", session)
	}
}

func TestLoginDoesNotInferAccountConcurrencyFromUnknownNewAPIField(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, _ *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc"})
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":7,"concurrency":9,"current_concurrency":3}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	session, err := New().Login(context.Background(), &connector.Channel{SiteURL: srv.URL, Username: "u", Password: "p"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.AccountConcurrency != nil {
		t.Fatalf("NewAPI inferred a non-standard concurrency limit: %#v", session.AccountConcurrency)
	}
}

func TestGetCosts(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self/stat", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("type"); got != "0" {
			t.Fatalf("type = %q, want 0", got)
		}
		if r.URL.Query().Get("start_timestamp") == "" || r.URL.Query().Get("end_timestamp") == "" {
			t.Fatalf("expected start/end timestamp")
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000,"rpm":0,"tpm":0}}`))
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":3416846,"used_quota":39091119}}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 2 {
		t.Fatalf("today cost = %v, want 2", res.TodayCost)
	}
	if res.TotalCost != 78.1822 {
		t.Fatalf("total cost = %v, want 78.1822", res.TotalCost)
	}
	if res.RawTodayCost != res.TodayCost || res.RawTotalCost != res.TotalCost || res.AppliedFactor != 1 || res.FactorSource != "default" {
		t.Fatalf("default cost provenance = %+v", res)
	}
}

func TestGetCostsUsesChannelTimeZoneForTodayBoundary(t *testing.T) {
	now := time.Now()
	serverLocation := now.Location()
	serverNow := now.In(serverLocation)
	serverStart := time.Date(serverNow.Year(), serverNow.Month(), serverNow.Day(), 0, 0, 0, 0, serverLocation).Unix()
	var timeZone string
	var userLocation *time.Location
	for _, candidate := range []string{"Pacific/Kiritimati", "Pacific/Pago_Pago"} {
		location, err := time.LoadLocation(candidate)
		if err != nil {
			t.Fatalf("LoadLocation(%q): %v", candidate, err)
		}
		localNow := now.In(location)
		localStart := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, location).Unix()
		if localStart != serverStart {
			timeZone = candidate
			userLocation = location
			break
		}
	}
	if userLocation == nil {
		t.Fatal("expected at least one test timezone to differ from server day boundary")
	}

	var gotStart, gotEnd int64
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self/stat", func(w http.ResponseWriter, r *http.Request) {
		gotStart, _ = strconv.ParseInt(r.URL.Query().Get("start_timestamp"), 10, 64)
		gotEnd, _ = strconv.ParseInt(r.URL.Query().Get("end_timestamp"), 10, 64)
		now := time.Now()
		localNow := now.In(userLocation)
		wantStart := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, userLocation).Unix()
		serverNow := now.In(serverLocation)
		serverStart := time.Date(serverNow.Year(), serverNow.Month(), serverNow.Day(), 0, 0, 0, 0, serverLocation).Unix()
		if gotStart != wantStart {
			t.Fatalf("start_timestamp = %d, want user day start %d", gotStart, wantStart)
		}
		if gotStart == serverStart {
			t.Fatalf("start_timestamp unexpectedly used server day start %d", serverStart)
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota":0}}`))
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"used_quota":0}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	before := time.Now().Unix()
	_, err := New().GetCosts(context.Background(), &connector.Channel{
		SiteURL:  srv.URL,
		TimeZone: timeZone,
	}, &connector.AuthSession{})
	after := time.Now().Unix()
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if gotEnd < before || gotEnd > after {
		t.Fatalf("end_timestamp = %d, want between %d and %d", gotEnd, before, after)
	}
}

func TestGetCostsAppliesManualRechargeMultiplier(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self/stat", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000}}`))
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"used_quota":5000000}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	multiplier := 2.0
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplier:     &multiplier,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 1 {
		t.Fatalf("today cost = %v, want 1", res.TodayCost)
	}
	if res.TotalCost != 5 {
		t.Fatalf("total cost = %v, want 5", res.TotalCost)
	}
	if res.RawTodayCost != 2 || res.RawTotalCost != 10 || res.AppliedFactor != 0.5 || res.FactorSource != "manual" {
		t.Fatalf("cost provenance = %+v", res)
	}
	if res.TodayCost != round4(res.RawTodayCost*res.AppliedFactor) || res.TotalCost != round4(res.RawTotalCost*res.AppliedFactor) {
		t.Fatalf("compatibility costs must equal raw*factor: %+v", res)
	}
}

func TestGetCostsDoesNotTreatStatusPriceAsRechargeMultiplier(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":500000,"price":7.2}}`))
	})
	mux.HandleFunc("/api/log/self/stat", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota":1000000}}`))
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"used_quota":5000000}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 2 {
		t.Fatalf("today cost = %v, want 2", res.TodayCost)
	}
	if res.TotalCost != 10 {
		t.Fatalf("total cost = %v, want 10", res.TotalCost)
	}
	if res.RawTodayCost != 2 || res.RawTotalCost != 10 || res.AppliedFactor != 1 || res.FactorSource != "default" {
		t.Fatalf("cost provenance = %+v", res)
	}
}

func TestGetRechargeInfo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/topup/info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"pay_methods":"[{\"name\":\"支付宝\",\"type\":\"alipay\",\"min_topup\":\"10\"},{\"name\":\"微信\",\"type\":\"wxpay\",\"min_topup\":\"12\"},{\"name\":\"Stripe\",\"type\":\"stripe\",\"min_topup\":\"30\"}]","min_topup":8,"amount_options":"[10,20,50]"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetRechargeInfo(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetRechargeInfo: %v", err)
	}
	if len(info.Methods) != 2 {
		t.Fatalf("methods len = %d, want 2", len(info.Methods))
	}
	if info.Methods[0].Type != "alipay" || info.Methods[0].MinAmount != 10 {
		t.Fatalf("alipay method = %#v", info.Methods[0])
	}
	if info.AmountStep != 1 {
		t.Fatalf("amount step = %v, want 1", info.AmountStep)
	}
	if len(info.PresetAmounts) != 3 || info.PresetAmounts[2] != 50 {
		t.Fatalf("preset amounts = %#v", info.PresetAmounts)
	}
}

func TestGetRechargeHistoryReturnsOnlyCompletedPositiveRecords(t *testing.T) {
	completedSeconds := int64(1_752_552_123)
	completedMillis := completedSeconds * 1000
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/topup/self", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("p") != "1" || r.URL.Query().Get("page_size") != "100" {
			t.Errorf("query = %s", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "token" || r.Header.Get("New-Api-User") != "7" {
			t.Errorf("auth headers = %q/%q", r.Header.Get("Authorization"), r.Header.Get("New-Api-User"))
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"message":"","data":{"items":[
			{"id":1,"trade_no":"trade-1","amount":100,"money":14,"status":"success","complete_time":%d},
			{"id":2,"amount":200,"money":20,"status":"SUCCESS","complete_time":%d},
			{"id":3,"trade_no":"pending","amount":100,"money":10,"status":"pending","complete_time":%d},
			{"id":4,"trade_no":"zero","amount":0,"money":10,"status":"success","complete_time":%d},
			{"id":0,"trade_no":"","amount":10,"money":1,"status":"success","complete_time":%d}
		]}}`, completedSeconds, completedMillis, completedSeconds, completedSeconds, completedSeconds)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var history connector.RechargeHistory = New()
	records, err := history.GetRechargeHistory(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token", UserID: "7"})
	if err != nil {
		t.Fatalf("GetRechargeHistory: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %#v", records)
	}
	if records[0].SourceID != "trade-1" || records[0].CreditedAmount != 100 || records[0].PaidAmount == nil || *records[0].PaidAmount != 14 || records[0].CompletedAt.Unix() != completedSeconds {
		t.Fatalf("first record = %#v", records[0])
	}
	if records[1].SourceID != "2" || records[1].CompletedAt.Unix() != completedSeconds {
		t.Fatalf("fallback record = %#v", records[1])
	}
}

func TestGetRechargeHistoryReportsUnsupportedEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	_, err := New().GetRechargeHistory(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if !errors.Is(err, connector.ErrUnsupported) {
		t.Fatalf("error = %v, want ErrUnsupported", err)
	}
}

func TestGetRechargeHistoryReadsAllPages(t *testing.T) {
	pageCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/topup/self", func(w http.ResponseWriter, r *http.Request) {
		pageCalls++
		page, _ := strconv.Atoi(r.URL.Query().Get("p"))
		var items strings.Builder
		count := 20
		if page == 2 {
			count = 1
		}
		for index := 0; index < count; index++ {
			if index > 0 {
				items.WriteByte(',')
			}
			id := (page-1)*20 + index + 1
			_, _ = fmt.Fprintf(&items, `{"id":%d,"amount":10,"money":10,"trade_no":"trade-%d","status":"success","complete_time":1752552123}`, id, id)
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[%s],"total":21,"page":%d,"page_size":20,"pages":2}}`, items.String(), page)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	records, err := New().GetRechargeHistory(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if err != nil {
		t.Fatalf("GetRechargeHistory: %v", err)
	}
	if len(records) != 21 || pageCalls != 2 || records[20].SourceID != "trade-21" {
		t.Fatalf("records/calls = %d/%d, last=%+v", len(records), pageCalls, records[len(records)-1])
	}
}

func TestCreateRecharge(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/pay", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Cookie"); got != "session=1" {
			t.Fatalf("cookie = %q", got)
		}
		if got := r.Header.Get("New-Api-User"); got != "7" {
			t.Fatalf("user header = %q", got)
		}
		_, _ = w.Write([]byte(`{"message":"success","data":{"pid":"123","type":"alipay"},"url":"https://pay.example.com/submit"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	}, connector.RechargeRequest{
		Amount:        20,
		PaymentMethod: "alipay",
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "form" {
		t.Fatalf("mode = %q, want form", launch.Mode)
	}
	if launch.FormAction != "https://pay.example.com/submit" {
		t.Fatalf("action = %q", launch.FormAction)
	}
	if launch.FormFields["pid"] != "123" {
		t.Fatalf("pid = %q", launch.FormFields["pid"])
	}
}

func TestCreateRechargeReturnsDataError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/pay", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"message":"error","data":"支付方式不存在"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	_, err := c.CreateRecharge(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	}, connector.RechargeRequest{
		Amount:        20,
		PaymentMethod: "alipay",
	})
	if err == nil || !strings.Contains(err.Error(), "支付方式不存在") {
		t.Fatalf("err = %v, want data error", err)
	}
}

func TestCreateRechargeRejectsFloatAmount(t *testing.T) {
	c := New()
	_, err := c.CreateRecharge(context.Background(), &connector.Channel{}, &connector.AuthSession{}, connector.RechargeRequest{
		Amount:        1.5,
		PaymentMethod: "alipay",
	})
	if err == nil || !strings.Contains(err.Error(), "正整数") {
		t.Fatalf("err = %v, want integer error", err)
	}
}

func TestSubscriptionPurchaseUnsupported(t *testing.T) {
	c := New()
	_, err := c.GetSubscriptionInfo(context.Background(), &connector.Channel{}, &connector.AuthSession{})
	if err == nil || !strings.Contains(err.Error(), "不支持订阅") {
		t.Fatalf("GetSubscriptionInfo err = %v, want unsupported error", err)
	}
	_, err = c.CreateSubscription(context.Background(), &connector.Channel{}, &connector.AuthSession{}, connector.SubscriptionRequest{})
	if err == nil || !strings.Contains(err.Error(), "不支持订阅") {
		t.Fatalf("CreateSubscription err = %v, want unsupported error", err)
	}
}

func TestGetSubscriptionUsage(t *testing.T) {
	now := time.Now().UTC()
	nextReset := now.Add(8 * time.Hour).Unix()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/subscription/self", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Cookie"); got != "session=1" {
			t.Fatalf("cookie = %q", got)
		}
		if got := r.Header.Get("New-Api-User"); got != "7" {
			t.Fatalf("New-Api-User = %q", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"subscriptions":[
			{"subscription":{"id":10,"plan_id":4,"amount_total":10000000,"amount_used":2500000,"start_time":1700000000,"end_time":` + strconv.FormatInt(now.Add(10*24*time.Hour).Unix(), 10) + `,"status":"active","last_reset_time":` + strconv.FormatInt(now.Add(-16*time.Hour).Unix(), 10) + `,"next_reset_time":` + strconv.FormatInt(nextReset, 10) + `}},
			{"subscription":{"id":11,"plan_id":5,"amount_total":5000000,"amount_used":1000000,"start_time":1700000000,"end_time":` + strconv.FormatInt(now.Add(20*24*time.Hour).Unix(), 10) + `,"status":"active"}}
		]}}`))
	})
	mux.HandleFunc("/api/subscription/plans", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("New-Api-User"); got != "7" {
			t.Fatalf("New-Api-User = %q", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":[
			{"plan":{"id":4,"title":"Codex Pro","quota_reset_period":"daily"}},
			{"plan":{"id":5,"title":"额度包","quota_reset_period":"never"}}
		]}`))
	})
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"quota_per_unit":1000000}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	info, err := New().GetSubscriptionUsage(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("GetSubscriptionUsage: %v", err)
	}
	if len(info.Items) != 2 {
		t.Fatalf("items = %#v", info.Items)
	}
	daily := info.Items[0]
	if daily.GroupName != "Codex Pro" || daily.Daily == nil || daily.Total != nil {
		t.Fatalf("daily subscription = %#v", daily)
	}
	if daily.Daily.LimitUSD != 10 || daily.Daily.UsedUSD != 2.5 || daily.Daily.RemainingUSD != 7.5 || daily.Daily.RemainingPercent != 75 {
		t.Fatalf("daily window = %#v", daily.Daily)
	}
	if daily.Daily.ResetsAt == nil || daily.Daily.ResetsAt.Unix() != nextReset {
		t.Fatalf("daily reset = %#v", daily.Daily.ResetsAt)
	}
	total := info.Items[1]
	if total.GroupName != "额度包" || total.Total == nil || total.Daily != nil {
		t.Fatalf("total subscription = %#v", total)
	}
	if total.Total.LimitUSD != 5 || total.Total.UsedUSD != 1 || total.Total.RemainingPercent != 80 {
		t.Fatalf("total window = %#v", total.Total)
	}
}

func TestListAPIKeys(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"default":{"ratio":1.25,"desc":"默认分组"}}}`))
	})
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("p"); got != "2" {
			t.Fatalf("p = %q, want 2", got)
		}
		if got := r.URL.Query().Get("page_size"); got != "10" {
			t.Fatalf("page_size = %q, want 10", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[{"id":9,"key":"sk**********abcd","status":1,"name":"main","created_time":1700000000,"accessed_time":1700000100,"expired_time":-1,"remain_quota":100,"used_quota":5,"unlimited_quota":false,"model_limits_enabled":true,"model_limits":"gpt-4","allow_ips":"1.1.1.1","group":"default","cross_group_retry":true}],"total":11,"page":2,"page_size":10,"pages":2}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	page, err := c.ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	}, connector.APIKeyQuery{Page: 2, PageSize: 10})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Status != "active" || page.Items[0].AllowIPs != "1.1.1.1" || page.Items[0].GroupName != "default" || page.Items[0].GroupRatio != 1.25 {
		t.Fatalf("page = %#v", page)
	}
	key := page.Items[0]
	if key.Source != connector.APIKeySourceNewAPI || !key.ObservedFields.Quota || !key.ObservedFields.UnlimitedQuota || !key.ObservedFields.ExpiredTime || !key.ObservedFields.ModelLimitsEnabled || !key.ObservedFields.ModelLimits || !key.ObservedFields.AllowIPs || !key.ObservedFields.CrossGroupRetry {
		t.Fatalf("observed token fields = source=%q fields=%+v", key.Source, key.ObservedFields)
	}
}

func TestListAPIKeysStrictIdentityUsesRawGroupWhenEnrichmentFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary group enrichment failure", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[{"id":9,"status":1,"name":"BOG-p-0123456789abcdef","group":"default"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{Cookie: "session=1", UserID: "7"}, connector.APIKeyQuery{Page: 1, PageSize: 10, RequireCompleteGroupIdentity: true})
	if err != nil || page == nil || len(page.Items) != 1 || page.Items[0].Group != "default" || page.Items[0].GroupName != "default" {
		t.Fatalf("strict raw group inventory = %#v, %v", page, err)
	}
}

func TestListAPIKeysStrictIdentityRejectsMissingRawGroup(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{}}`))
	})
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[{"id":9,"status":1,"name":"BOG-p-0123456789abcdef"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{Cookie: "session=1", UserID: "7"}, connector.APIKeyQuery{Page: 1, PageSize: 10, RequireCompleteGroupIdentity: true})
	if err == nil || page != nil || !strings.Contains(err.Error(), "group identity is incomplete") {
		t.Fatalf("strict missing group inventory = %#v, %v", page, err)
	}
}

func TestListAPIKeysPreservesOmittedCreationFields(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"default":{"ratio":1,"desc":""}}}`))
	})
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[{"id":9,"status":1,"name":"BOG - default","group":"default"}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{Cookie: "session=1", UserID: "7"}, connector.APIKeyQuery{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("items = %#v", page.Items)
	}
	key := page.Items[0]
	if key.Source != connector.APIKeySourceNewAPI || key.ObservedFields.Quota || key.ObservedFields.UnlimitedQuota || key.ObservedFields.ExpiredTime || key.ObservedFields.ModelLimitsEnabled || key.ObservedFields.ModelLimits || key.ObservedFields.AllowIPs || key.ObservedFields.CrossGroupRetry {
		t.Fatalf("omitted fields were treated as observed: source=%q fields=%+v", key.Source, key.ObservedFields)
	}
}

func TestListAPIKeysPreservesExplicitNullCreationFields(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"default":{"ratio":1,"desc":""}}}`))
	})
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[{"id":9,"status":1,"name":"BOG - default","group":"default","expired_time":null,"remain_quota":null,"unlimited_quota":null,"model_limits_enabled":null,"model_limits":null,"allow_ips":null,"cross_group_retry":null}],"total":1,"page":1,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{Cookie: "session=1", UserID: "7"}, connector.APIKeyQuery{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("items = %#v", page.Items)
	}
	key := page.Items[0]
	if !key.ObservedFields.Quota || !key.ObservedFields.UnlimitedQuota || !key.ObservedFields.ExpiredTime || !key.ObservedFields.ModelLimitsEnabled || !key.ObservedFields.ModelLimits || !key.ObservedFields.AllowIPs || !key.ObservedFields.CrossGroupRetry {
		t.Fatalf("null fields were treated as omitted: %+v", key.ObservedFields)
	}
	if !key.NullFields.Quota || !key.NullFields.UnlimitedQuota || !key.NullFields.ExpiredTime || !key.NullFields.ModelLimitsEnabled || !key.NullFields.ModelLimits || !key.NullFields.AllowIPs || !key.NullFields.CrossGroupRetry {
		t.Fatalf("null fields were not preserved: %+v", key.NullFields)
	}
}

func TestListAPIKeysPreservesMissingPaginationMetadata(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{}}`))
	})
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[{"id":1},{"id":2}]}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.APIKeyQuery{Page: 1, PageSize: 2})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if page.Total != 0 || page.Page != 1 || page.PageSize != 2 || page.Pages != 0 {
		t.Fatalf("pagination = total:%d page:%d page_size:%d pages:%d", page.Total, page.Page, page.PageSize, page.Pages)
	}
	if paginatedResponseComplete(page.Page, page.Pages, page.Total, page.PageSize, len(page.Items), 2) {
		t.Fatal("full page without pagination metadata was treated as complete")
	}
}

func TestListAPIKeysRejectsInconsistentPaginationMetadata(t *testing.T) {
	for _, test := range []struct {
		name      string
		queryPage int
		data      string
	}{
		{name: "page differs from request", queryPage: 2, data: `{"items":[],"page":1}`},
		{name: "page exceeds pages", queryPage: 2, data: `{"items":[],"total":10,"page":2,"page_size":10,"pages":1}`},
		{name: "pages disagree with total", queryPage: 1, data: `{"items":[{},{},{},{},{},{},{},{},{},{}],"total":11,"page":1,"page_size":10,"pages":1}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"success":true,"message":"","data":%s}`, test.data)
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			page, err := New().ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.APIKeyQuery{Page: test.queryPage, PageSize: 10})
			if err == nil || page != nil || !strings.Contains(err.Error(), "pagination") {
				t.Fatalf("ListAPIKeys = %#v, %v; want pagination error", page, err)
			}
		})
	}
}

func TestListAPIKeyGroups(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"default":{"ratio":1.25,"desc":"默认分组"},"auto":{"ratio":"自动","desc":"跳过"}}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	groups, err := c.ListAPIKeyGroups(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	})
	if err != nil {
		t.Fatalf("ListAPIKeyGroups: %v", err)
	}
	if len(groups) != 1 || groups[0].Name != "default" || groups[0].Description != "默认分组" || groups[0].Ratio != 1.25 {
		t.Fatalf("groups = %#v", groups)
	}
}

func TestPaginatedResponseCompleteRequiresConsistentMetadata(t *testing.T) {
	for _, test := range []struct {
		name     string
		page     int
		pages    int
		total    int64
		pageSize int
		items    int
		want     bool
	}{
		{name: "consistent final page", page: 3, pages: 3, total: 101, pageSize: 50, items: 1, want: true},
		{name: "pages understate total", page: 1, pages: 1, total: 101, pageSize: 50, items: 50, want: false},
		{name: "total requires another page", page: 2, pages: 3, total: 101, pageSize: 50, items: 50, want: false},
		{name: "response page advanced past metadata", page: 2, pages: 1, total: 1, pageSize: 50, want: false},
		{name: "full page without metadata", page: 1, pageSize: 50, items: 50, want: false},
		{name: "short page without metadata", page: 1, pageSize: 50, items: 1, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := paginatedResponseComplete(test.page, test.pages, test.total, test.pageSize, test.items, 50); got != test.want {
				t.Fatalf("complete = %v, want %v", got, test.want)
			}
		})
	}
}

func TestGetAnnouncements(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"announcements":[{"id":12,"title":"平台公告","content":"维护通知","type":"warning","publishDate":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T04:04:05Z"}]}}`))
	})
	mux.HandleFunc("/api/notice", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":"站点公告"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	items, err := c.GetAnnouncements(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("items len = %d, want 2", len(items))
	}
	if items[0].SourceKey != "id:12" || items[0].Title != "平台公告" || items[0].Type != "warning" {
		t.Fatalf("first item = %#v", items[0])
	}
	if !strings.HasPrefix(items[1].SourceKey, "hash:") || items[1].Content != "站点公告" {
		t.Fatalf("second item = %#v", items[1])
	}
}

func TestGetAnnouncementsFromNoticeOnly(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"announcements":[]}}`))
	})
	mux.HandleFunc("/api/notice", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":"文本公告"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	items, err := c.GetAnnouncements(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1", len(items))
	}
	if !strings.HasPrefix(items[0].SourceKey, "hash:") || items[0].Content != "文本公告" {
		t.Fatalf("item = %#v", items[0])
	}
}

func TestGetAnnouncementsEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"announcements":[]}}`))
	})
	mux.HandleFunc("/api/notice", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":""}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	items, err := c.GetAnnouncements(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("items len = %d, want 0", len(items))
	}
}

func TestSearchAPIKeys(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/token/search", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("keyword"); got != "main" {
			t.Fatalf("keyword = %q, want main", got)
		}
		if got := r.URL.Query().Get("token"); got != "" {
			t.Fatalf("token = %q, want empty when searching by name", got)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[],"total":0,"page":1,"page_size":20,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	_, err := c.ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{
		Cookie: "session=1",
		UserID: "7",
	}, connector.APIKeyQuery{Search: "main"})
	if err != nil {
		t.Fatalf("ListAPIKeys search: %v", err)
	}
}

func TestLookupRequestCostUsesUpstreamQuota(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000,"price":9}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("token_name"); got != "BridgeOps Gateway - default" {
			t.Fatalf("token_name = %q", got)
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[{"id":7,"created_at":%d,"token_id":9,"token_name":"BridgeOps Gateway - default","model_name":"gpt-test","prompt_tokens":120,"completion_tokens":30,"quota":1,"other":"{\"cache_tokens\":24,\"cache_creation_tokens\":6}"}]}}`, now.Unix())
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	result, err := c.LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID:         9,
		APIKeyName:       "BridgeOps Gateway - default",
		GroupName:        "default",
		ModelName:        "gpt-test",
		PromptTokens:     120,
		CompletionTokens: 30,
		StartedAt:        now.Add(-time.Second),
		FinishedAt:       now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("LookupRequestCost: %v", err)
	}
	if result.Cost != 0.000002 || result.Source != "newapi_quota" || !strings.HasPrefix(result.UsageID, "newapi:") || result.CacheCreationTokens != 6 || result.CacheReadTokens != 24 || !result.HasCacheTokenMetrics {
		t.Fatalf("result = %+v", result)
	}
	if result.PromptTokens != 120 || result.CompletionTokens != 30 || result.TotalTokens != 150 || !result.HasTokenMetrics {
		t.Fatalf("token metrics = %+v, want prompt=120 completion=30 total=150 present=true", result)
	}
}

func TestLookupRequestCostNormalizesAnthropicUsageTokens(t *testing.T) {
	tests := []struct {
		name       string
		other      string
		wantPrompt int64
		wantTotal  int64
	}{
		{
			name:       "explicit total input",
			other:      `{"usage_semantic":"anthropic","cache_tokens":20,"cache_creation_tokens":10,"input_tokens_total":130}`,
			wantPrompt: 130,
			wantTotal:  160,
		},
		{
			name:       "reconstruct cache input",
			other:      `{"usage_semantic":"anthropic","cache_tokens":20,"cache_creation_tokens":10}`,
			wantPrompt: 130,
			wantTotal:  160,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().Truncate(time.Second)
			mux := http.NewServeMux()
			mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
			})
			mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[{"id":7,"created_at":%d,"token_id":9,"model_name":"claude-test","prompt_tokens":100,"completion_tokens":30,"quota":2,"request_id":"req-anthropic","other":%q}],"total":1,"page":1,"page_size":100,"pages":1}}`, now.Unix(), test.other)
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
				APIKeyID: 9, ModelName: "claude-test", PromptTokens: test.wantPrompt, CompletionTokens: 30,
				RequestID: "req-anthropic", StartedAt: now.Add(-time.Second), FinishedAt: now.Add(time.Second),
			})
			if err != nil {
				t.Fatalf("LookupRequestCost: %v", err)
			}
			if result.PromptTokens != test.wantPrompt || result.CompletionTokens != 30 || result.TotalTokens != test.wantTotal || result.CacheReadTokens != 20 || result.CacheCreationTokens != 10 || !result.HasTokenMetrics || !result.HasCacheTokenMetrics {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestLookupRequestCostExactRequestIDDoesNotRequireLocalModelOrTokens(t *testing.T) {
	tests := []struct {
		name                     string
		queryModel               string
		upstreamModel            string
		promptTokens             int64
		completionTokens         int64
		upstreamPromptTokens     int64
		upstreamCompletionTokens int64
	}{
		{name: "tokens match", queryModel: "gpt-test", upstreamModel: "gpt-test", promptTokens: 120, completionTokens: 30, upstreamPromptTokens: 120, upstreamCompletionTokens: 30},
		{name: "accepted cancel has no local tokens", queryModel: "gpt-test", upstreamModel: "gpt-test", promptTokens: 0, completionTokens: 0, upstreamPromptTokens: 120, upstreamCompletionTokens: 30},
		{name: "local tokens differ", queryModel: "gpt-test", upstreamModel: "gpt-test", promptTokens: 999, completionTokens: 888, upstreamPromptTokens: 120, upstreamCompletionTokens: 30},
		{name: "upstream tokens missing", queryModel: "gpt-test", upstreamModel: "gpt-test", promptTokens: 120, completionTokens: 30},
		{name: "model differs", queryModel: "gpt-test", upstreamModel: "gpt-other", promptTokens: 120, completionTokens: 30, upstreamPromptTokens: 120, upstreamCompletionTokens: 30},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now().Truncate(time.Second)
			queryModel := test.queryModel
			if queryModel == "" {
				queryModel = "gpt-test"
			}
			upstreamModel := test.upstreamModel
			if upstreamModel == "" {
				upstreamModel = "gpt-test"
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
			})
			mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[{"id":6,"created_at":%d,"token_id":0,"model_name":%q,"prompt_tokens":%d,"completion_tokens":%d,"quota":1,"request_id":"req-exact"},{"id":7,"created_at":%d,"token_id":9,"model_name":%q,"prompt_tokens":%d,"completion_tokens":%d,"quota":2,"request_id":"req-exact"}],"total":2,"page":1,"page_size":100,"pages":1}}`, now.Unix(), upstreamModel, test.upstreamPromptTokens, test.upstreamCompletionTokens, now.Unix(), upstreamModel, test.upstreamPromptTokens, test.upstreamCompletionTokens)
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
				APIKeyID:         9,
				ModelName:        queryModel,
				PromptTokens:     test.promptTokens,
				CompletionTokens: test.completionTokens,
				RequestID:        "req-exact",
				StartedAt:        now.Add(-time.Second),
				FinishedAt:       now.Add(time.Second),
			})
			if err != nil {
				t.Fatalf("LookupRequestCost: %v", err)
			}
			if result.Cost != 0.000004 || !strings.HasPrefix(result.UsageID, "newapi:") {
				t.Fatalf("result = %+v", result)
			}
			if test.upstreamPromptTokens == 0 && test.upstreamCompletionTokens == 0 {
				if result.PromptTokens != 0 || result.CompletionTokens != 0 || result.TotalTokens != 0 || result.HasTokenMetrics {
					t.Fatalf("token metrics = %+v, want missing upstream metrics", result)
				}
			} else if result.PromptTokens != test.upstreamPromptTokens || result.CompletionTokens != test.upstreamCompletionTokens || result.TotalTokens != test.upstreamPromptTokens+test.upstreamCompletionTokens || !result.HasTokenMetrics {
				t.Fatalf("token metrics = %+v, want upstream metrics", result)
			}
		})
	}
}

func TestLookupRequestCostExactRequestIDUsesNormalizedModelMatch(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("request_id"); got != "req-model-normalized" {
			t.Fatalf("request_id query = %q", got)
		}
		for _, name := range []string{"token_name", "group", "model_name", "start_timestamp", "end_timestamp"} {
			if got := r.URL.Query().Get(name); got != "" {
				t.Fatalf("%s query = %q, want empty when request ID is available", name, got)
			}
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[{"id":7,"created_at":%d,"token_id":9,"model_name":"gpt-test","prompt_tokens":12,"completion_tokens":3,"quota":2,"request_id":"req-model-normalized"}],"total":1,"page":1,"page_size":100,"pages":1}}`, now.Unix())
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: " GPT-TEST ", PromptTokens: 12, CompletionTokens: 3,
		RequestID: "req-model-normalized", StartedAt: now.Add(-time.Second), FinishedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("LookupRequestCost: %v", err)
	}
	if result.ModelName != "gpt-test" || result.PromptTokens != 12 || result.CompletionTokens != 3 || result.Cost != 0.000004 {
		t.Fatalf("result = %+v", result)
	}
}

func TestLookupRequestCostExactRequestIDIgnoresLocalTimeWindow(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("request_id"); got != "req-outside-window" || r.URL.Query().Get("start_timestamp") != "" || r.URL.Query().Get("end_timestamp") != "" {
			t.Fatalf("exact-ID query = %q", r.URL.RawQuery)
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[{"id":7,"created_at":%d,"token_id":9,"model_name":"upstream-alias","prompt_tokens":12,"completion_tokens":3,"quota":2,"request_id":"req-outside-window"}],"total":1,"page":1,"page_size":100,"pages":1}}`, now.Add(time.Hour).Unix())
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-outside-window", StartedAt: now, FinishedAt: now,
	})
	if err != nil || result == nil || result.Cost != 0.000004 {
		t.Fatalf("result=%+v err=%v, want authoritative exact-ID match", result, err)
	}
}

func TestLookupRequestCostExactRequestIDRequiresUniqueTargetKeyRow(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	for _, test := range []struct {
		name      string
		items     string
		total     int
		wantError string
	}{
		{
			name:      "duplicate exact ID",
			items:     fmt.Sprintf(`[{"id":8,"created_at":%d,"token_id":9,"model_name":"a","quota":1,"request_id":"req-unique"},{"id":7,"created_at":%d,"token_id":9,"model_name":"b","quota":2,"request_id":"req-unique"}]`, now.Unix(), now.Unix()),
			total:     2,
			wantError: "multiple usage records matched",
		},
		{
			name:      "exact ID on wrong key",
			items:     fmt.Sprintf(`[{"id":7,"created_at":%d,"token_id":8,"model_name":"gpt-test","quota":2,"request_id":"req-unique"}]`, now.Unix()),
			total:     1,
			wantError: "usage record not found",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
			})
			mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":%s,"total":%d,"page":1,"page_size":100,"pages":1}}`, test.items, test.total)
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()

			_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: "req-unique", StartedAt: now, FinishedAt: now,
			})
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("err=%v, want %q", err, test.wantError)
			}
		})
	}
}

func TestLookupRequestCostWithoutRequestIDDoesNotRelaxMissingLocalMetrics(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[{"id":7,"created_at":%d,"token_id":9,"model_name":"gpt-test","prompt_tokens":0,"completion_tokens":0,"quota":2}],"total":1}}`, now.Unix())
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", StartedAt: now, FinishedAt: now,
	})
	if err == nil || !strings.Contains(err.Error(), "usage record not found") {
		t.Fatalf("err=%v, want strict ID-less fallback rejection", err)
	}
}

func TestLookupRequestCostWithoutRequestIDRejectsMultipleCandidates(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[{"id":7,"created_at":%d,"token_id":9,"model_name":"gpt-test","prompt_tokens":120,"completion_tokens":30,"quota":1,"request_id":"candidate-a"},{"id":8,"created_at":%d,"token_id":9,"model_name":"gpt-test","prompt_tokens":120,"completion_tokens":30,"quota":2,"request_id":"candidate-b"}],"total":2}}`, now.Unix(), now.Unix())
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	_, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID:         9,
		ModelName:        "gpt-test",
		PromptTokens:     120,
		CompletionTokens: 30,
		StartedAt:        now.Add(-time.Second),
		FinishedAt:       now.Add(time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "multiple usage records matched") {
		t.Fatalf("err = %v, want multiple usage records matched", err)
	}
}

func TestLookupRequestCostCacheAliasesUseMaximum(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{"items": []map[string]any{{
				"created_at":        now.Unix(),
				"token_id":          9,
				"model_name":        "gpt-test",
				"prompt_tokens":     120,
				"completion_tokens": 30,
				"quota":             2,
				"request_id":        "request-with-cache",
				"other": map[string]any{
					"cache_tokens":          40,
					"cached_tokens":         60,
					"cache_read_tokens":     50,
					"cache_write_tokens":    8,
					"cache_creation_tokens": 9,
				},
			}}, "total": 1, "page": 1, "page_size": 100, "pages": 1},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID: 9, ModelName: "gpt-test", PromptTokens: 120, CompletionTokens: 30,
		RequestID: "request-with-cache", StartedAt: now.Add(-time.Second), FinishedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("LookupRequestCost: %v", err)
	}
	if result.CacheReadTokens != 60 || result.CacheCreationTokens != 9 || !result.HasCacheTokenMetrics {
		t.Fatalf("cache metrics = %+v, want read=60 creation=9 present=true", result)
	}
}

func TestNewAPICacheTokenMetricsTreatsExplicitZeroAsReliable(t *testing.T) {
	creation, read, ok := newAPICacheTokenMetrics(json.RawMessage(`{"cache_read_tokens":0,"cache_write_tokens":"0"}`))
	if creation != 0 || read != 0 || !ok {
		t.Fatalf("cache metrics = %d/%d present=%t, want 0/0 present=true", creation, read, ok)
	}
}

func TestLookupRequestCostWithRequestIDDoesNotFallbackAndRetriesExact(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	pages := 0
	exactAvailable := false
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
		pages++
		page := r.URL.Query().Get("p")
		items := make([]map[string]any, 0, 100)
		if page == "1" {
			for i := 0; i < 100; i++ {
				items = append(items, map[string]any{"id": i + 1, "created_at": now.Unix(), "token_id": 9, "model_name": "other", "prompt_tokens": 1, "completion_tokens": 1, "quota": 1})
			}
		} else {
			requestID := "billing-id"
			if exactAvailable {
				requestID = "requested-id"
			}
			items = append(items, map[string]any{"id": 101, "created_at": now.Unix(), "token_id": 9, "model_name": "gpt-test", "prompt_tokens": 120, "completion_tokens": 30, "quota": 2, "request_id": requestID})
		}
		requestedPage, _ := strconv.Atoi(page)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"items": items, "total": 101, "page": requestedPage, "page_size": 100, "pages": 2}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	lookup := func() (*connector.RequestCostResult, error) {
		return New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
			APIKeyID: 9, APIKeyName: "BridgeOps Gateway - default", GroupName: "default", ModelName: "gpt-test",
			PromptTokens: 120, CompletionTokens: 30, RequestID: "requested-id",
			StartedAt: now.Add(-time.Second), FinishedAt: now.Add(time.Second),
		})
	}
	result, err := lookup()
	if err == nil || !strings.Contains(err.Error(), "usage record not found") || result != nil {
		t.Fatalf("first lookup result=%+v err=%v, want exact-only not found", result, err)
	}
	if pages != 3 {
		t.Fatalf("pages after first lookup = %d, want 3", pages)
	}

	exactAvailable = true
	result, err = lookup()
	if err != nil {
		t.Fatalf("LookupRequestCost after exact usage appeared: %v", err)
	}
	if pages != 6 || !strings.HasPrefix(result.UsageID, "newapi:") || result.Cost != 0.000004 {
		t.Fatalf("pages=%d result=%+v", pages, result)
	}
}

func TestLookupRequestCostExactRequestIDRejectsIncompletePaginationMetadata(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[{"id":7,"created_at":%d,"token_id":9,"request_id":"req-incomplete","quota":1}]}}`, now.Unix())
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-incomplete",
	})
	if err == nil || !strings.Contains(err.Error(), "incomplete pagination metadata") || result != nil {
		t.Fatalf("result=%+v err=%v, want incomplete pagination rejection", result, err)
	}
}

func TestLookupRequestCostExactRequestIDScansServerClampedPagesForDuplicates(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
		requests++
		page, _ := strconv.Atoi(r.URL.Query().Get("p"))
		id := 8
		if page == 2 {
			id = 7
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"items": []map[string]any{{"id": id, "created_at": now.Unix(), "token_id": 9, "request_id": "req-clamped", "quota": 1}},
				"total": 2, "page": page, "page_size": 1, "pages": 2,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-clamped",
	})
	if err == nil || !strings.Contains(err.Error(), "multiple usage records matched") || result != nil {
		t.Fatalf("result=%+v err=%v, want duplicate rejection", result, err)
	}
	if requests != 3 {
		t.Fatalf("requests=%d, want page 1, page 2, and final page 1", requests)
	}
}

func TestLookupRequestCostExactRequestIDRejectsConcurrentPrefixDuplicate(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	requests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, _ *http.Request) {
		requests++
		items := []map[string]any{{"id": 7, "created_at": now.Unix(), "token_id": 9, "request_id": "req-growth", "quota": 1}}
		total := 1
		if requests > 1 {
			items = append([]map[string]any{{"id": 8, "created_at": now.Add(time.Second).Unix(), "token_id": 9, "request_id": "req-growth", "quota": 2}}, items...)
			total = 2
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data":    map[string]any{"items": items, "total": total, "page": 1, "page_size": 100, "pages": 1},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-growth",
	})
	if err == nil || !strings.Contains(err.Error(), "snapshot changed") || result != nil {
		t.Fatalf("result=%+v err=%v, want concurrent snapshot rejection", result, err)
	}
}

func TestLookupRequestCostExactRequestIDRequiresStablePaginationAcrossPages(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/log/self", func(w http.ResponseWriter, r *http.Request) {
		page, _ := strconv.Atoi(r.URL.Query().Get("p"))
		total := 2
		if page == 2 {
			total = 3
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"items": []map[string]any{{"id": 10 - page, "created_at": now.Unix(), "token_id": 8, "request_id": "req-metadata", "quota": 1}},
				"total": total, "page": page, "page_size": 1, "pages": total,
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
		APIKeyID: 9, RequestID: "req-metadata",
	})
	if err == nil || !strings.Contains(err.Error(), "snapshot changed during scan") || result != nil {
		t.Fatalf("result=%+v err=%v, want unstable pagination rejection", result, err)
	}
}

func TestRequestIDsEqualDoesNotFoldSub2Namespace(t *testing.T) {
	if requestIDsEqual(" requested-id ", "requested-id") {
		t.Fatal("NewAPI request IDs must preserve surrounding whitespace")
	}
	if requestIDsEqual("client:requested-id", "requested-id") {
		t.Fatal("NewAPI request IDs must not fold the Sub2API client namespace")
	}
}

func TestNewAPIUsageIdentityDoesNotUseDisplayID(t *testing.T) {
	first := newAPIUsageIdentity("request-a", 9, 100, "gpt-test", 10, 2, 1)
	second := newAPIUsageIdentity("request-b", 9, 100, "gpt-test", 10, 2, 1)
	if first == second {
		t.Fatalf("different request ids produced the same usage identity: %q", first)
	}
	if first != newAPIUsageIdentity("request-a", 9, 100, "gpt-test", 10, 2, 1) {
		t.Fatal("usage identity is not stable")
	}
}

func TestCreateUpdateDeleteRevealAPIKey(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create: %v", err)
			}
			if body["name"] != "main" || body["custom_key"] != "sk-custom" {
				t.Fatalf("create body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":9,"name":"main","group":"default"}}`))
		case http.MethodGet:
			t.Fatal("creating a key with an upstream id must not scan the full key list")
		case http.MethodPut:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode update: %v", err)
			}
			if body["id"] != float64(9) || body["status"] != float64(2) {
				t.Fatalf("update body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":9,"status":2,"name":"main disabled"}}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	mux.HandleFunc("/api/token/9", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":""}`))
	})
	mux.HandleFunc("/api/token/9/key", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"key":"sk-full"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session := &connector.AuthSession{Cookie: "session=1", UserID: "7"}
	created, err := c.CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, connector.APIKeyCreateRequest{
		Name:      "main",
		CustomKey: "sk-custom",
		Group:     "default",
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if created.Key != "sk-full" {
		t.Fatalf("created key = %q, want sk-full", created.Key)
	}
	updated, err := c.UpdateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 9, connector.APIKeyUpdateRequest{
		Status: strPtr("disabled"),
	})
	if err != nil {
		t.Fatalf("UpdateAPIKey: %v", err)
	}
	if updated.Status != "disabled" {
		t.Fatalf("updated status = %q", updated.Status)
	}
	if err := c.DeleteAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 9); err != nil {
		t.Fatalf("DeleteAPIKey: %v", err)
	}
	key, err := c.RevealAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 9)
	if err != nil {
		t.Fatalf("RevealAPIKey: %v", err)
	}
	if key != "sk-full" {
		t.Fatalf("key = %q", key)
	}
}

func TestCreateAPIKeyPrefersRevealWhenCreateReturnsKey(t *testing.T) {
	var revealCalled bool
	mux := http.NewServeMux()
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
			return
		}
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":9,"key":"sk-custom","status":1,"name":"main","group":"default"}}`))
	})
	mux.HandleFunc("/api/token/9/key", func(w http.ResponseWriter, r *http.Request) {
		revealCalled = true
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"key":"sk-real-full"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	created, err := c.CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.APIKeyCreateRequest{
		Name:      "main",
		CustomKey: "sk-custom",
		Group:     "default",
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if !revealCalled {
		t.Fatal("RevealAPIKey was not called")
	}
	if created.Key != "sk-real-full" {
		t.Fatalf("created key = %q, want revealed key", created.Key)
	}
}

func TestDecodeNewAPICreatedKeyTracksIdentityFieldPresence(t *testing.T) {
	idOnly, err := decodeNewAPICreatedKey(json.RawMessage(`{"id":17}`))
	if err != nil || idOnly.ID != 17 || idOnly.Source != connector.APIKeySourceNewAPI ||
		idOnly.ObservedIdentityFields != (connector.APIKeyIdentityFields{}) ||
		idOnly.NullIdentityFields != (connector.APIKeyIdentityFields{}) {
		t.Fatalf("ID-only created key = %#v, %v", idOnly, err)
	}
	explicitNull, err := decodeNewAPICreatedKey(json.RawMessage(`{"id":18,"name":null,"group":null,"status":null}`))
	if err != nil || explicitNull.ID != 18 ||
		explicitNull.ObservedIdentityFields != (connector.APIKeyIdentityFields{Name: true, Group: true, Status: true}) ||
		explicitNull.NullIdentityFields != (connector.APIKeyIdentityFields{Name: true, Group: true, Status: true}) {
		t.Fatalf("explicit-null created key = %#v, %v", explicitNull, err)
	}
}

func TestDecodeNewAPICreatedKeyRejectsMalformedIdentityField(t *testing.T) {
	invalid := []json.RawMessage{
		json.RawMessage(`{"id":19,"name":17}`),
		json.RawMessage(`{"id":19.5}`),
		json.RawMessage(`{"id":9223372036854775808}`),
		json.RawMessage(`{"id":19,"token_id":20}`),
		json.RawMessage(`{"name":"managed","data":{"id":19}}`),
	}
	for _, response := range invalid {
		created, err := decodeNewAPICreatedKey(response)
		if err == nil || created != nil {
			t.Fatalf("malformed created key response %s = %#v, %v", response, created, err)
		}
	}
	nested, err := decodeNewAPICreatedKey(json.RawMessage(`{"key":"not-an-id","data":{"id":"20","name":null,"group":"other"}}`))
	if err != nil || nested.ID != 20 || nested.Source != connector.APIKeySourceNewAPI {
		t.Fatalf("nested ID-only created key = %#v, %v", nested, err)
	}
	if !nested.NullIdentityFields.Name || !nested.ObservedIdentityFields.Name || !nested.ObservedIdentityFields.Group || nested.Group != "other" {
		t.Fatalf("nested identity evidence was lost: %#v", nested)
	}
}

func TestDecodeNewAPICreatedKeyRejectsConflictingCandidateIDs(t *testing.T) {
	tests := []json.RawMessage{
		json.RawMessage(`{"id":21,"data":{"id":22}}`),
		json.RawMessage(`{"token":{"id":23},"data":{"key_id":"24"}}`),
	}
	for _, response := range tests {
		created, err := decodeNewAPICreatedKey(response)
		if err == nil || created != nil || !strings.Contains(err.Error(), "conflicting ids") {
			t.Fatalf("conflicting created key response %s = %#v, %v", response, created, err)
		}
	}
}

func TestDecodeNewAPICreatedKeyRejectsMalformedWrappedCandidateID(t *testing.T) {
	tests := []json.RawMessage{
		json.RawMessage(`{"data":{"id":null}}`),
		json.RawMessage(`{"token":{"key_id":19.5}}`),
	}
	for _, response := range tests {
		created, err := decodeNewAPICreatedKey(response)
		if err == nil || created != nil || !strings.Contains(err.Error(), "id is invalid") {
			t.Fatalf("malformed wrapped created key response %s = %#v, %v", response, created, err)
		}
	}
}

func TestDecodeNewAPICreatedKeyRejectsConflictingCandidateIdentity(t *testing.T) {
	tests := []json.RawMessage{
		json.RawMessage(`{"id":25,"name":"managed","data":{"id":25,"name":"other"}}`),
		json.RawMessage(`{"id":26,"group":"default","token":{"id":26,"group":null}}`),
		json.RawMessage(`{"id":27,"data":{"id":27,"status":"active"}}`),
	}
	for _, response := range tests {
		created, err := decodeNewAPICreatedKey(response)
		if err == nil || created != nil {
			t.Fatalf("contradictory created key response %s = %#v, %v", response, created, err)
		}
	}
}

func TestDecodeNewAPICreatedKeyMergesCompatibleCandidateIdentity(t *testing.T) {
	created, err := decodeNewAPICreatedKey(json.RawMessage(`{
		"id":28,
		"token":{"token_id":"28","name":"managed","group":null,"status":1},
		"data":{"key_id":28,"name":"managed","group":null,"status":1}
	}`))
	if err != nil {
		t.Fatalf("decode compatible wrapped response: %v", err)
	}
	if created.ID != 28 || created.Name != "managed" || created.Status != "active" || created.Group != "" {
		t.Fatalf("compatible wrapped created key = %#v", created)
	}
	wantFields := connector.APIKeyIdentityFields{Name: true, Group: true, Status: true}
	if created.ObservedIdentityFields != wantFields ||
		created.NullIdentityFields != (connector.APIKeyIdentityFields{Group: true}) {
		t.Fatalf("compatible wrapped identity evidence = observed %#v, null %#v", created.ObservedIdentityFields, created.NullIdentityFields)
	}
}

func TestCreateAPIKeyAcceptsWrappedCreatedKeyIdentity(t *testing.T) {
	createPosts := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected token method %s", r.Method)
		}
		createPosts++
		_, _ = w.Write([]byte(`{"success":true,"data":{"id":31,"data":{"id":31,"name":"managed","group":"default","status":1}}}`))
	})
	mux.HandleFunc("/api/token/31/key", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected reveal method %s", r.Method)
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-created-full"}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	created, err := New().CreateAPIKey(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{}, connector.APIKeyCreateRequest{
		Name: "managed", Group: "default",
	})
	if err != nil {
		t.Fatalf("create wrapped key: %v", err)
	}
	if createPosts != 1 || created.ID != 31 || created.Name != "managed" || created.Group != "default" || created.Status != "active" || created.Key != "sk-created-full" {
		t.Fatalf("wrapped created key = %#v, posts = %d", created, createPosts)
	}
}

func TestCreateAPIKeyDoesNotFallbackToCustomKeyWhenRevealFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			_, _ = w.Write([]byte(`{"success":true,"message":""}`))
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	mux.HandleFunc("/api/token/search", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[],"total":0,"page":1,"page_size":20,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	created, err := c.CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.APIKeyCreateRequest{
		Name:      "main",
		CustomKey: "sk-custom-should-not-be-used",
		Group:     "default",
	})
	if err == nil {
		t.Fatalf("CreateAPIKey returned key %#v, want error", created)
	}
	if strings.Contains(err.Error(), "sk-custom-should-not-be-used") {
		t.Fatalf("error leaked custom key: %v", err)
	}
}

func TestCreateAPIKeyRetriesRevealForCreatedID(t *testing.T) {
	revealCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":18,"key":"sk-local-placeholder","status":1,"name":"BridgeOps Gateway - default","group":"default"}}`))
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	mux.HandleFunc("/api/token/18/key", func(w http.ResponseWriter, r *http.Request) {
		revealCalls++
		if revealCalls < 3 {
			http.Error(w, "reveal not ready", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"key":"sk-real-created"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	created, err := New().CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.APIKeyCreateRequest{
		Name: "BridgeOps Gateway - default", Group: "default", CustomKey: "sk-local-placeholder",
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if created.Key != "sk-real-created" {
		t.Fatalf("created key = %q", created.Key)
	}
	if revealCalls != 3 {
		t.Fatalf("reveal calls = %d, want 3", revealCalls)
	}
}

func TestCreateAPIKeyDoesNotRevealExistingMatchWhenNewKeyIsDelayed(t *testing.T) {
	var revealedExisting bool
	mux := http.NewServeMux()
	list := func(w http.ResponseWriter) {
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"items":[{"id":7,"key":"sk**********old","status":1,"name":"main","group":"default"}],"total":1,"page":1,"page_size":100,"pages":1}}`))
	}
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			_, _ = w.Write([]byte(`{"success":true,"message":""}`))
		case http.MethodGet:
			list(w)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	mux.HandleFunc("/api/token/search", func(w http.ResponseWriter, r *http.Request) { list(w) })
	mux.HandleFunc("/api/token/7/key", func(w http.ResponseWriter, r *http.Request) {
		revealedExisting = true
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"key":"sk-old"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	created, err := New().CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{}, connector.APIKeyCreateRequest{Name: "main", Group: "default"})
	if err == nil || created != nil {
		t.Fatalf("CreateAPIKey = %#v, %v; want delayed-key error", created, err)
	}
	if revealedExisting {
		t.Fatal("existing key was revealed as the newly created key")
	}
}

func TestGetModelPricesConvertsAuthenticatedTokenPricing(t *testing.T) {
	statusCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		statusCalls++
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		assertNewAPIModelPricingAuth(t, r)
		_, _ = w.Write([]byte(`{"success":true,"data":{"PLUS":{"ratio":0.01},"auto":{"ratio":"自动"}}}`))
	})
	mux.HandleFunc("/api/pricing", func(w http.ResponseWriter, r *http.Request) {
		assertNewAPIModelPricingAuth(t, r)
		_, _ = w.Write([]byte(`{
			"success":true,
			"pricing_version":"price-v1",
			"group_ratio":{"PLUS":0.01},
			"usable_group":{"PLUS":"plus"},
			"data":[
				{"model_name":"gpt-5.6-sol","quota_type":0,"model_ratio":2.5,"completion_ratio":6,"cache_ratio":0.1,"create_cache_ratio":1.25,"enable_groups":["all"]},
				{"model_name":"cache-unknown","quota_type":0,"model_ratio":1,"completion_ratio":2,"enable_groups":["PLUS"]},
				{"model_name":"fixed","quota_type":1,"model_price":1,"enable_groups":["PLUS"]},
				{"model_name":"dynamic","quota_type":0,"enable_groups":["PLUS"],"billing_mode":"tiered_expr","billing_expr":"1+1"},
				{"model_name":"unavailable","quota_type":0,"enable_groups":["OTHER"]}
			]
		}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
	if err != nil {
		t.Fatalf("GetModelPrices: %v", err)
	}
	if statusCalls != 2 || snapshot == nil || !snapshot.Complete || snapshot.QuotaPerUnit != 500000 ||
		snapshot.SourceVersion != "price-v1" || len(snapshot.SourceChecksum) != 64 {
		t.Fatalf("model price snapshot = %+v, status calls = %d", snapshot, statusCalls)
	}
	if len(snapshot.Items) != 2 {
		t.Fatalf("model price items = %+v, want two token-priced models", snapshot.Items)
	}
	unknown, priced := snapshot.Items[0], snapshot.Items[1]
	if unknown.ModelName != "cache-unknown" || unknown.InputUnitPriceUSD != 2 || unknown.OutputUnitPriceUSD != 4 ||
		unknown.CacheReadUnitPriceUSD != nil || unknown.CacheWriteUnitPriceUSD != nil {
		t.Fatalf("cache-unknown price = %+v", unknown)
	}
	if priced.ModelName != "gpt-5.6-sol" || len(priced.EnabledGroups) != 1 || priced.EnabledGroups[0] != "plus" ||
		priced.UpstreamGroupName != "plus" || priced.GroupRatio == nil || *priced.GroupRatio != 0.01 ||
		priced.InputUnitPriceUSD != 5 || priced.OutputUnitPriceUSD != 30 ||
		priced.CacheReadUnitPriceUSD == nil || *priced.CacheReadUnitPriceUSD != 0.5 ||
		priced.CacheWriteUnitPriceUSD == nil || *priced.CacheWriteUnitPriceUSD != 6.25 {
		t.Fatalf("converted model price = %+v", priced)
	}
}

func TestGetModelPricesExpandsEveryEligibleGroup(t *testing.T) {
	statusCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		statusCalls++
		_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
	})
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		assertNewAPIModelPricingAuth(t, r)
		_, _ = w.Write([]byte(`{"success":true,"data":{"PLUS":{"ratio":0.01},"default":{"ratio":1}}}`))
	})
	mux.HandleFunc("/api/pricing", func(w http.ResponseWriter, r *http.Request) {
		assertNewAPIModelPricingAuth(t, r)
		_, _ = w.Write([]byte(`{
			"success":true,
			"pricing_version":"price-v2",
			"group_ratio":{"PLUS":0.01,"default":1},
			"usable_group":{"PLUS":"plus","default":"default"},
			"data":[
				{"model_name":"gpt-5.6-sol","quota_type":0,"model_ratio":2.5,"completion_ratio":6,"cache_ratio":0.1,"enable_groups":["all"]}
			]
		}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
	if err != nil {
		t.Fatalf("GetModelPrices: %v", err)
	}
	if statusCalls != 2 || snapshot == nil || !snapshot.Complete || len(snapshot.Items) != 2 {
		t.Fatalf("model price snapshot = %+v, status calls = %d", snapshot, statusCalls)
	}
	for index, expected := range []struct {
		group string
		ratio float64
	}{{group: "default", ratio: 1}, {group: "plus", ratio: 0.01}} {
		item := snapshot.Items[index]
		if item.ModelName != "gpt-5.6-sol" || item.UpstreamGroupName != expected.group ||
			len(item.EnabledGroups) != 1 || item.EnabledGroups[0] != expected.group ||
			item.GroupRatio == nil || *item.GroupRatio != expected.ratio ||
			item.InputUnitPriceUSD != 5 || item.OutputUnitPriceUSD != 30 ||
			item.CacheReadUnitPriceUSD == nil || *item.CacheReadUnitPriceUSD != 0.5 {
			t.Fatalf("expanded price %d = %+v", index, item)
		}
	}
}

func TestGetModelPricesRequiresAuthenticatedAccountSession(t *testing.T) {
	for name, session := range map[string]*connector.AuthSession{
		"nil":           nil,
		"no credential": {UserID: "7"},
		"no user id":    {Cookie: "session=abc"},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: "http://127.0.0.1"}, session)
			if err == nil || snapshot != nil || !strings.Contains(err.Error(), "authenticated account session") {
				t.Fatalf("GetModelPrices = %+v, %v", snapshot, err)
			}
		})
	}
}

func TestGetModelPricesRejectsQuotaUnitDrift(t *testing.T) {
	statusCalls := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		statusCalls++
		quota := 500000
		if statusCalls > 1 {
			quota = 1000000
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"quota_per_unit":%d}}`, quota)
	})
	serveNewAPIModelPricingGroups(mux, 0.01)
	mux.HandleFunc("/api/pricing", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(newAPIModelPricingPayload(0.01,
			`{"model_name":"gpt-5.6-sol","quota_type":0,"model_ratio":2.5,"completion_ratio":6,"enable_groups":["PLUS"]}`)))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
	if err == nil || snapshot != nil || !strings.Contains(err.Error(), "unit changed") {
		t.Fatalf("GetModelPrices = %+v, %v; want unit drift rejection", snapshot, err)
	}
}

func TestGetModelPricesRejectsGroupViewMismatch(t *testing.T) {
	mux := http.NewServeMux()
	serveNewAPIModelPricingStatus(mux, 500000)
	serveNewAPIModelPricingGroups(mux, 0.01)
	mux.HandleFunc("/api/pricing", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(newAPIModelPricingPayload(0.02,
			`{"model_name":"gpt-5.6-sol","quota_type":0,"model_ratio":2.5,"completion_ratio":6,"enable_groups":["PLUS"]}`)))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
	if err == nil || snapshot != nil || !strings.Contains(err.Error(), "ratio does not match") {
		t.Fatalf("GetModelPrices = %+v, %v; want group mismatch rejection", snapshot, err)
	}
}

func TestGetModelPricesWithdrawsFixedAndDynamicModelsFromTokenSnapshot(t *testing.T) {
	mode := "token"
	mux := http.NewServeMux()
	serveNewAPIModelPricingStatus(mux, 500000)
	serveNewAPIModelPricingGroups(mux, 0.01)
	mux.HandleFunc("/api/pricing", func(w http.ResponseWriter, _ *http.Request) {
		item := `{"model_name":"changing","quota_type":0,"model_ratio":2.5,"completion_ratio":6,"enable_groups":["PLUS"]}`
		if mode == "fixed" {
			item = `{"model_name":"changing","quota_type":1,"model_price":0.2,"enable_groups":["PLUS"]}`
		} else if mode == "dynamic" {
			item = `{"model_name":"changing","quota_type":0,"enable_groups":["PLUS"],"billing_mode":"tiered_expr","billing_expr":"usage.input*2"}`
		}
		_, _ = w.Write([]byte(newAPIModelPricingPayload(0.01, item)))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client := New()

	token, err := client.GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
	if err != nil || len(token.Items) != 1 {
		t.Fatalf("token snapshot = %+v, %v", token, err)
	}
	mode = "fixed"
	fixed, err := client.GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
	if err != nil || len(fixed.Items) != 0 || fixed.SourceChecksum == token.SourceChecksum {
		t.Fatalf("fixed snapshot = %+v, %v", fixed, err)
	}
	mode = "dynamic"
	dynamic, err := client.GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
	if err != nil || len(dynamic.Items) != 0 || dynamic.SourceChecksum == fixed.SourceChecksum {
		t.Fatalf("dynamic snapshot = %+v, %v", dynamic, err)
	}
}

func TestGetModelPricesRejectsRedirectAndOversizedResponse(t *testing.T) {
	t.Run("redirect", func(t *testing.T) {
		redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			t.Fatal("pricing redirect was followed")
			w.WriteHeader(http.StatusOK)
		}))
		defer redirectTarget.Close()
		mux := http.NewServeMux()
		serveNewAPIModelPricingStatus(mux, 500000)
		serveNewAPIModelPricingGroups(mux, 0.01)
		mux.HandleFunc("/api/pricing", func(w http.ResponseWriter, _ *http.Request) {
			http.Redirect(w, nil, redirectTarget.URL, http.StatusFound)
		})
		srv := httptest.NewServer(mux)
		defer srv.Close()
		snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
		if err == nil || snapshot != nil {
			t.Fatalf("GetModelPrices = %+v, %v; want redirect rejection", snapshot, err)
		}
	})

	t.Run("oversized", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", strconv.Itoa(newAPIModelPricingMaxBytes+1))
			_, _ = w.Write([]byte("x"))
		}))
		defer srv.Close()
		_, err := New().getBoundedRawNoRedirect(context.Background(), srv.URL, nil, newAPIModelPricingMaxBytes)
		if err == nil || !strings.Contains(err.Error(), "size limit") {
			t.Fatalf("getBoundedRawNoRedirect error = %v", err)
		}
	})
}

func TestGetModelPricesRejectsMissingRequiredFieldsAndDirtyNames(t *testing.T) {
	tests := []struct {
		name string
		item string
		want string
	}{
		{name: "quota type", item: `{"model_name":"model","model_ratio":1,"completion_ratio":1,"enable_groups":["PLUS"]}`, want: "quota_type"},
		{name: "unknown quota type", item: `{"model_name":"model","quota_type":2,"model_price":1,"enable_groups":["PLUS"]}`, want: "unsupported quota_type"},
		{name: "model ratio", item: `{"model_name":"model","quota_type":0,"completion_ratio":1,"enable_groups":["PLUS"]}`, want: "token ratios"},
		{name: "completion ratio", item: `{"model_name":"model","quota_type":0,"model_ratio":1,"enable_groups":["PLUS"]}`, want: "token ratios"},
		{name: "enabled groups", item: `{"model_name":"model","quota_type":0,"model_ratio":1,"completion_ratio":1}`, want: "enabled groups"},
		{name: "fixed price", item: `{"model_name":"model","quota_type":1,"enable_groups":["PLUS"]}`, want: "fixed price"},
		{name: "model whitespace", item: `{"model_name":" model","quota_type":0,"model_ratio":1,"completion_ratio":1,"enable_groups":["PLUS"]}`, want: "invalid model name"},
		{name: "group whitespace", item: `{"model_name":"model","quota_type":0,"model_ratio":1,"completion_ratio":1,"enable_groups":[" PLUS"]}`, want: "invalid enabled group"},
		{name: "duplicate group", item: `{"model_name":"model","quota_type":0,"model_ratio":1,"completion_ratio":1,"enable_groups":["PLUS","plus"]}`, want: "duplicate enabled group"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mux := http.NewServeMux()
			serveNewAPIModelPricingStatus(mux, 500000)
			serveNewAPIModelPricingGroups(mux, 0.01)
			mux.HandleFunc("/api/pricing", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(newAPIModelPricingPayload(0.01, test.item)))
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()
			snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
			if err == nil || snapshot != nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("GetModelPrices = %+v, %v; want %q", snapshot, err, test.want)
			}
		})
	}
}

func TestGetModelPricesRejectsMissingCostUnitGroupFactsAndDuplicateModels(t *testing.T) {
	t.Run("missing quota unit", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"success":true,"data":{}}`))
		})
		srv := httptest.NewServer(mux)
		defer srv.Close()
		snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
		if err == nil || snapshot != nil || !strings.Contains(err.Error(), "quota_per_unit") {
			t.Fatalf("GetModelPrices = %+v, %v", snapshot, err)
		}
	})

	for _, test := range []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "missing top-level group facts",
			payload: `{"success":true,"data":[{"model_name":"model","quota_type":0,"model_ratio":1,"completion_ratio":1,"enable_groups":["PLUS"]}]}`,
			want:    "group facts are missing",
		},
		{
			name: "duplicate model",
			payload: newAPIModelPricingPayload(0.01,
				`{"model_name":"model","quota_type":0,"model_ratio":1,"completion_ratio":1,"enable_groups":["PLUS"]}`,
				`{"model_name":"model","quota_type":0,"model_ratio":1,"completion_ratio":1,"enable_groups":["PLUS"]}`),
			want: "duplicate model",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mux := http.NewServeMux()
			serveNewAPIModelPricingStatus(mux, 500000)
			serveNewAPIModelPricingGroups(mux, 0.01)
			mux.HandleFunc("/api/pricing", func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(test.payload))
			})
			srv := httptest.NewServer(mux)
			defer srv.Close()
			snapshot, err := New().GetModelPrices(context.Background(), &connector.Channel{SiteURL: srv.URL}, newAPIModelPricingTestSession())
			if err == nil || snapshot != nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("GetModelPrices = %+v, %v; want %q", snapshot, err, test.want)
			}
		})
	}
}

func newAPIModelPricingTestSession() *connector.AuthSession {
	return &connector.AuthSession{UserID: "7", Cookie: "session=abc"}
}

func assertNewAPIModelPricingAuth(t *testing.T, r *http.Request) {
	t.Helper()
	if r.Header.Get("Cookie") != "session=abc" || r.Header.Get("New-Api-User") != "7" {
		t.Fatalf("pricing auth headers = Cookie %q, New-Api-User %q", r.Header.Get("Cookie"), r.Header.Get("New-Api-User"))
	}
}

func serveNewAPIModelPricingStatus(mux *http.ServeMux, quotaPerUnit int) {
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"quota_per_unit":%d}}`, quotaPerUnit)
	})
}

func serveNewAPIModelPricingGroups(mux *http.ServeMux, ratio float64) {
	mux.HandleFunc("/api/user/self/groups", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "session=abc" || r.Header.Get("New-Api-User") != "7" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"PLUS":{"ratio":%g}}}`, ratio)
	})
}

func newAPIModelPricingPayload(groupRatio float64, items ...string) string {
	return fmt.Sprintf(`{"success":true,"pricing_version":"v1","group_ratio":{"PLUS":%g},"usable_group":{"PLUS":"plus"},"data":[%s]}`,
		groupRatio, strings.Join(items, ","))
}

func strPtr(v string) *string {
	return &v
}
