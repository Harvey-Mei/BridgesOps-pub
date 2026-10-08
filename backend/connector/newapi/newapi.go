// Package newapi 实现对 NewAPI 风格上游站点的 connector，参考 docs/USER_BALANCE_GROUP_RATE_AUTH_API_CN-newapi.md。
package newapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/providerid"
	"github.com/go-resty/resty/v2"
)

func init() {
	connector.Register(connector.TypeNewAPI, func() connector.Connector { return New() })
}

// Client NewAPI connector 实现。
type Client struct {
	http *resty.Client
}

func New() *Client {
	c := resty.New().
		SetTimeout(30*time.Second).
		SetHeader("User-Agent", "bridgeops/0.1").
		SetHeader("Accept", "application/json")
	direct, _ := netproxy.NewTransport(nil, "")
	c.GetClient().Transport = direct
	connector.SetHTTPClientTLSHandshakeTimeout(c.GetClient(), 30*time.Second)
	return &Client{http: c}
}

func (c *Client) SetProxy(proxyURL string) error {
	client, err := netproxy.NewClient(c.http.GetClient(), proxyURL)
	if err != nil {
		return err
	}
	c.http.GetClient().Transport = client.Transport
	return nil
}

func (c *Client) SetHTTPConfig(cfg connector.HTTPConfig) {
	timeout := c.http.GetClient().Timeout
	if cfg.Timeout > 0 {
		c.http.SetTimeout(cfg.Timeout)
		timeout = cfg.Timeout
	}
	connector.SetHTTPClientTLSHandshakeTimeout(c.http.GetClient(), timeout)
	if strings.TrimSpace(cfg.UserAgent) != "" {
		c.http.SetHeader("User-Agent", cfg.UserAgent)
	}
}

// newapiResp NewAPI 统一响应外壳：{ success, message, data }。
type newapiResp struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (c *Client) GetTurnstileSiteKey(ctx context.Context, ch *connector.Channel) (string, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/status", nil)
	if err != nil {
		return "", fmt.Errorf("newapi status: %w", err)
	}
	var status struct {
		TurnstileCheck   bool   `json:"turnstile_check"`
		TurnstileSiteKey string `json:"turnstile_site_key"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		return "", fmt.Errorf("newapi status decode: %w", err)
	}
	if !status.TurnstileCheck {
		return "", nil
	}
	return status.TurnstileSiteKey, nil
}

func (c *Client) Login(ctx context.Context, ch *connector.Channel) (*connector.AuthSession, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	body := map[string]any{
		"username": ch.Username,
		"password": ch.Password,
	}
	for k, v := range ch.LoginExtraParams {
		body[k] = v
	}
	req := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(body)
	if ch.TurnstileToken != "" {
		req.SetQueryParam("turnstile", ch.TurnstileToken)
	}

	resp, err := req.Post(site + "/api/user/login")
	if err != nil {
		return nil, fmt.Errorf("newapi login http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("newapi login: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	var wrapped newapiResp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("newapi login decode: %w", err)
	}
	if !wrapped.Success {
		return nil, fmt.Errorf("newapi login: %s", wrapped.Message)
	}

	var data struct {
		Require2FA bool  `json:"require_2fa"`
		ID         int64 `json:"id"`
	}
	_ = json.Unmarshal(wrapped.Data, &data)
	if data.Require2FA {
		return nil, errors.New("newapi account requires 2FA; please disable it for monitoring accounts")
	}

	cookie := joinCookies(resp.Cookies())
	if cookie == "" {
		return nil, errors.New("newapi login: no session cookie returned")
	}
	if data.ID == 0 {
		// 用户 id 是后续 New-Api-User 头的必需值；缺失说明响应格式不对。
		return nil, errors.New("newapi login: missing user id in response")
	}
	// NewAPI session 默认有效期较长，保守按 7 天估算；CheckAuth 会兜底失效检测。
	return &connector.AuthSession{
		UserID:    strconv.FormatInt(data.ID, 10),
		Cookie:    cookie,
		ExpiresAt: time.Now().Add(7 * 24 * time.Hour),
	}, nil
}

func (c *Client) CheckAuth(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) error {
	if session == nil || !newAPIHasAuth(session) {
		return errors.New("missing newapi credential: 需要提供 Cookie 或系统访问令牌")
	}
	_, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/user/self", session)
	return err
}

// newAPIHasAuth 判断 session 是否带有 NewAPI 通过鉴权所必需的凭据。
// NewAPI 支持两种鉴权：
//   - 浏览器 session：Cookie 头（典型值 session=xxxxx; ...）
//   - 系统访问令牌：Authorization 头（user.access_token，32 位字符串）
//
// 两者都需要搭配 New-Api-User 头，见 applyNewAPIAuth。
func newAPIHasAuth(session *connector.AuthSession) bool {
	if session == nil {
		return false
	}
	return strings.TrimSpace(session.Cookie) != "" || strings.TrimSpace(session.AccessToken) != ""
}

// applyNewAPIAuth 把当前 session 的鉴权头挂到 resty 请求上。
//   - 优先 Cookie（浏览器 session）；
//   - 没填 Cookie 但填了 AccessToken 时改走 Authorization: <token>，对应 NewAPI 的系统访问令牌；
//   - New-Api-User 始终带上（NewAPI 即便用 session 也要求这个头）。
func applyNewAPIAuth(req *resty.Request, session *connector.AuthSession) {
	if session == nil {
		return
	}
	if strings.TrimSpace(session.Cookie) != "" {
		req.SetHeader("Cookie", session.Cookie)
	} else if token := strings.TrimSpace(session.AccessToken); token != "" {
		// NewAPI middleware 会自动去掉 "Bearer " 前缀，这里直接给裸 token，行为最贴近 dashboard。
		req.SetHeader("Authorization", token)
	}
	if strings.TrimSpace(session.UserID) != "" {
		req.SetHeader("New-Api-User", session.UserID)
	}
}

func (c *Client) GetBalance(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.BalanceResult, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	statusBody, err := c.getJSON(ctx, site+"/api/status", nil)
	if err != nil {
		return nil, fmt.Errorf("newapi status: %w", err)
	}
	var status struct {
		QuotaPerUnit float64 `json:"quota_per_unit"`
		Price        float64 `json:"price"`
	}
	if err := json.Unmarshal(statusBody, &status); err != nil {
		return nil, fmt.Errorf("newapi status decode: %w", err)
	}
	if status.QuotaPerUnit <= 0 || math.IsNaN(status.QuotaPerUnit) || math.IsInf(status.QuotaPerUnit, 0) {
		status.QuotaPerUnit = 500000
	}

	selfBody, err := c.getJSON(ctx, site+"/api/user/self", session)
	if err != nil {
		return nil, fmt.Errorf("newapi self: %w", err)
	}
	var self struct {
		Quota float64 `json:"quota"`
	}
	if err := json.Unmarshal(selfBody, &self); err != nil {
		return nil, fmt.Errorf("newapi self decode: %w", err)
	}
	balance := c.quotaToUSD(self.Quota, status.QuotaPerUnit)
	factor, factorSource := newAPIRechargeFactor(ch, status.Price)
	return &connector.BalanceResult{
		Balance:       round4(balance * factor),
		RawBalance:    balance,
		AppliedFactor: factor,
		FactorSource:  factorSource,
		SampledAt:     time.Now(),
	}, nil
}

func (c *Client) GetCosts(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.CostResult, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	statusBody, err := c.getJSON(ctx, site+"/api/status", nil)
	if err != nil {
		return nil, fmt.Errorf("newapi status: %w", err)
	}
	var status struct {
		QuotaPerUnit float64 `json:"quota_per_unit"`
		Price        float64 `json:"price"`
	}
	if err := json.Unmarshal(statusBody, &status); err != nil {
		return nil, fmt.Errorf("newapi status decode: %w", err)
	}
	if status.QuotaPerUnit <= 0 {
		status.QuotaPerUnit = 500000
	}

	location := time.UTC
	if timeZone := strings.TrimSpace(ch.TimeZone); timeZone != "" {
		location, err = time.LoadLocation(timeZone)
		if err != nil {
			return nil, fmt.Errorf("newapi costs timezone %q: %w", timeZone, err)
		}
	}
	now := time.Now().In(location)
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, location).Unix()
	end := now.Unix()
	logBody, err := c.getJSON(ctx, site+"/api/log/self/stat?type=0&token_name=&model_name=&start_timestamp="+strconv.FormatInt(start, 10)+"&end_timestamp="+strconv.FormatInt(end, 10)+"&group=", session)
	if err != nil {
		return nil, fmt.Errorf("newapi self stat: %w", err)
	}
	var todayStat struct {
		Quota float64 `json:"quota"`
	}
	if err := json.Unmarshal(logBody, &todayStat); err != nil {
		return nil, fmt.Errorf("newapi self stat decode: %w", err)
	}

	usageBody, err := c.getJSON(ctx, site+"/api/user/self", session)
	if err != nil {
		return nil, fmt.Errorf("newapi self total: %w", err)
	}
	var usage struct {
		UsedQuota float64 `json:"used_quota"`
	}
	if err := json.Unmarshal(usageBody, &usage); err != nil {
		return nil, fmt.Errorf("newapi self total decode: %w", err)
	}

	todayCost := c.quotaToUSD(todayStat.Quota, status.QuotaPerUnit)
	totalCost := c.quotaToUSD(usage.UsedQuota, status.QuotaPerUnit)
	factor, factorSource := newAPIRechargeFactor(ch, status.Price)
	return &connector.CostResult{
		TodayCost:     round4(todayCost * factor),
		TotalCost:     round4(totalCost * factor),
		RawTodayCost:  todayCost,
		RawTotalCost:  totalCost,
		AppliedFactor: factor,
		FactorSource:  factorSource,
	}, nil
}

func (c *Client) GetRates(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.RateSnapshotResult, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/user/self/groups", session)
	if err != nil {
		return nil, fmt.Errorf("newapi groups: %w", err)
	}
	// data: { "default": { "ratio": 1, "desc": "..." }, "auto": { "ratio": "自动", ... } }
	raw := map[string]struct {
		Ratio json.RawMessage `json:"ratio"`
		Desc  string          `json:"desc"`
	}{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("newapi groups decode: %w", err)
	}
	out := make([]connector.RateResult, 0, len(raw))
	for name, v := range raw {
		var ratio float64
		if err := json.Unmarshal(v.Ratio, &ratio); err != nil {
			var label string
			if json.Unmarshal(v.Ratio, &label) == nil && strings.TrimSpace(label) == "自动" {
				continue
			}
			return nil, fmt.Errorf("newapi group %q ratio decode: %w", name, err)
		}
		out = append(out, connector.RateResult{
			ModelName:   name,
			Description: v.Desc,
			Ratio:       ratio,
		})
	}
	return &connector.RateSnapshotResult{Items: out, Complete: true}, nil
}

const (
	newAPIModelPricingMaxBytes      = 8 << 20
	newAPIModelPricingMaxItems      = 4096
	newAPIModelPricingGroupMaxBytes = 1 << 20
	newAPIStatusMaxBytes            = 1 << 20
)

// GetModelPrices reads the prices that this exact NewAPI account can use. The
// status endpoint is sampled on both sides of /api/pricing so a concurrent
// quota-unit change can never be persisted with prices decoded under the old
// denominator.
func (c *Client) GetModelPrices(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
	if ch == nil {
		return nil, errors.New("newapi model pricing: channel is required")
	}
	if !newAPIHasAuth(session) || strings.TrimSpace(session.UserID) == "" {
		return nil, errors.New("newapi model pricing: authenticated account session is required")
	}
	site := strings.TrimRight(ch.SiteURL, "/")
	quotaPerUnitBefore, err := c.getQuotaPerUnit(ctx, site)
	if err != nil {
		return nil, fmt.Errorf("newapi model pricing unit before snapshot: %w", err)
	}
	authenticatedGroups, err := c.getModelPricingGroups(ctx, site, session)
	if err != nil {
		return nil, fmt.Errorf("newapi model pricing groups: %w", err)
	}
	body, err := c.getBoundedRawNoRedirect(ctx, site+"/api/pricing", session, newAPIModelPricingMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("newapi model pricing: %w", err)
	}
	quotaPerUnitAfter, err := c.getQuotaPerUnit(ctx, site)
	if err != nil {
		return nil, fmt.Errorf("newapi model pricing unit after snapshot: %w", err)
	}
	if quotaPerUnitBefore != quotaPerUnitAfter {
		return nil, errors.New("newapi model pricing unit changed during snapshot")
	}

	var payload struct {
		Success        bool               `json:"success"`
		Message        string             `json:"message"`
		Data           json.RawMessage    `json:"data"`
		PricingVersion string             `json:"pricing_version"`
		GroupRatio     map[string]float64 `json:"group_ratio"`
		UsableGroup    map[string]string  `json:"usable_group"`
	}
	if !utf8.Valid(body) {
		return nil, errors.New("newapi model pricing response is not valid UTF-8")
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("newapi model pricing decode: %w", err)
	}
	if !payload.Success {
		message := strings.TrimSpace(payload.Message)
		if message == "" {
			message = "upstream rejected pricing request"
		}
		return nil, errors.New(message)
	}
	var rawItems []struct {
		ModelName        string    `json:"model_name"`
		QuotaType        *int      `json:"quota_type"`
		ModelRatio       *float64  `json:"model_ratio"`
		ModelPrice       *float64  `json:"model_price"`
		CompletionRatio  *float64  `json:"completion_ratio"`
		CacheRatio       *float64  `json:"cache_ratio"`
		CreateCacheRatio *float64  `json:"create_cache_ratio"`
		EnableGroups     *[]string `json:"enable_groups"`
		BillingMode      string    `json:"billing_mode"`
		BillingExpr      string    `json:"billing_expr"`
	}
	if err := json.Unmarshal(payload.Data, &rawItems); err != nil {
		return nil, fmt.Errorf("newapi model pricing data decode: %w", err)
	}
	if len(rawItems) == 0 || len(rawItems) > newAPIModelPricingMaxItems {
		return nil, errors.New("newapi model pricing returned an empty or oversized snapshot")
	}
	rateGroups, err := validateNewAPIModelPricingGroups(authenticatedGroups, payload.UsableGroup, payload.GroupRatio)
	if err != nil {
		return nil, err
	}

	items := make([]connector.ModelPriceResult, 0, len(rawItems))
	type checksumItem struct {
		ModelName        string   `json:"model_name"`
		Status           string   `json:"status"`
		AdvertisedGroups []string `json:"advertised_groups"`
		EnabledGroups    []string `json:"enabled_groups"`
		QuotaType        *int     `json:"quota_type"`
		ModelRatio       *float64 `json:"model_ratio"`
		ModelPrice       *float64 `json:"model_price"`
		CompletionRatio  *float64 `json:"completion_ratio"`
		CacheRatio       *float64 `json:"cache_ratio"`
		CreateCacheRatio *float64 `json:"create_cache_ratio"`
		BillingMode      string   `json:"billing_mode"`
		BillingExpr      string   `json:"billing_expr"`
	}
	checksumItems := make([]checksumItem, 0, len(rawItems))
	seen := make(map[string]struct{}, len(rawItems))
	for _, raw := range rawItems {
		modelName := raw.ModelName
		if modelName == "" || modelName != strings.TrimSpace(modelName) || strings.ContainsRune(modelName, '\x00') ||
			!utf8.ValidString(modelName) || utf8.RuneCountInString(modelName) > 256 {
			return nil, errors.New("newapi model pricing contains an invalid model name")
		}
		if _, duplicate := seen[modelName]; duplicate {
			return nil, fmt.Errorf("newapi model pricing contains duplicate model %q", modelName)
		}
		seen[modelName] = struct{}{}
		if raw.EnableGroups == nil || len(*raw.EnableGroups) == 0 {
			return nil, fmt.Errorf("newapi model pricing is missing enabled groups for model %q", modelName)
		}
		advertisedGroups, groups, err := enabledNewAPIModelPricingGroups(*raw.EnableGroups, rateGroups)
		if err != nil {
			return nil, fmt.Errorf("newapi model pricing groups for model %q: %w", modelName, err)
		}
		status := "token"
		billingMode, billingExpr := strings.TrimSpace(raw.BillingMode), strings.TrimSpace(raw.BillingExpr)
		switch {
		case raw.QuotaType == nil:
			return nil, fmt.Errorf("newapi model pricing is missing quota_type for model %q", modelName)
		case *raw.QuotaType == 1:
			status = "fixed"
		case *raw.QuotaType != 0:
			return nil, fmt.Errorf("newapi model pricing has unsupported quota_type for model %q", modelName)
		case billingMode != "" || billingExpr != "":
			status = "dynamic"
		case len(groups) == 0:
			status = "no_eligible_group"
		}
		for _, value := range []*float64{raw.ModelRatio, raw.ModelPrice, raw.CompletionRatio, raw.CacheRatio, raw.CreateCacheRatio} {
			if value != nil && (*value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0)) {
				return nil, fmt.Errorf("newapi model pricing contains invalid values for model %q", modelName)
			}
		}
		if status == "fixed" && raw.ModelPrice == nil {
			return nil, fmt.Errorf("newapi model pricing is missing fixed price for model %q", modelName)
		}
		checksumItems = append(checksumItems, checksumItem{
			ModelName: modelName, Status: status, AdvertisedGroups: advertisedGroups, EnabledGroups: groups,
			QuotaType: raw.QuotaType, ModelRatio: raw.ModelRatio, ModelPrice: raw.ModelPrice, CompletionRatio: raw.CompletionRatio,
			CacheRatio: raw.CacheRatio, CreateCacheRatio: raw.CreateCacheRatio,
			BillingMode: billingMode, BillingExpr: billingExpr,
		})
		if status != "token" {
			continue
		}
		if raw.ModelRatio == nil || raw.CompletionRatio == nil {
			return nil, fmt.Errorf("newapi model pricing is missing token ratios for model %q", modelName)
		}
		input := *raw.ModelRatio * 1_000_000 / quotaPerUnitBefore
		output := input * *raw.CompletionRatio
		var cacheRead, cacheWrite *float64
		if raw.CacheRatio != nil {
			value := input * *raw.CacheRatio
			cacheRead = &value
		}
		if raw.CreateCacheRatio != nil {
			value := input * *raw.CreateCacheRatio
			cacheWrite = &value
		}
		for _, price := range []*float64{&input, &output, cacheRead, cacheWrite} {
			if price == nil {
				continue
			}
			if *price < 0 || math.IsNaN(*price) || math.IsInf(*price, 0) {
				return nil, fmt.Errorf("newapi model pricing overflows for model %q", modelName)
			}
		}
		for _, groupName := range groups {
			groupRatio := rateGroups[groupName]
			items = append(items, connector.ModelPriceResult{
				ModelName: modelName, EnabledGroups: []string{groupName},
				UpstreamGroupName: groupName, GroupRatio: &groupRatio,
				InputUnitPriceUSD: input, OutputUnitPriceUSD: output,
				CacheReadUnitPriceUSD: cacheRead, CacheWriteUnitPriceUSD: cacheWrite,
			})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ModelName != items[j].ModelName {
			return items[i].ModelName < items[j].ModelName
		}
		return items[i].UpstreamGroupName < items[j].UpstreamGroupName
	})
	sort.Slice(checksumItems, func(i, j int) bool { return checksumItems[i].ModelName < checksumItems[j].ModelName })
	version := strings.TrimSpace(payload.PricingVersion)
	if len(version) > 128 || strings.ContainsRune(version, '\x00') || !utf8.ValidString(version) {
		version = ""
	}
	type checksumGroup struct {
		Name  string  `json:"name"`
		Ratio float64 `json:"ratio"`
	}
	checksumGroups := make([]checksumGroup, 0, len(rateGroups))
	for name, ratio := range rateGroups {
		checksumGroups = append(checksumGroups, checksumGroup{Name: name, Ratio: ratio})
	}
	sort.Slice(checksumGroups, func(i, j int) bool { return checksumGroups[i].Name < checksumGroups[j].Name })
	checksumPayload := struct {
		QuotaPerUnit float64         `json:"quota_per_unit"`
		Groups       []checksumGroup `json:"groups"`
		Items        []checksumItem  `json:"items"`
	}{QuotaPerUnit: quotaPerUnitBefore, Groups: checksumGroups, Items: checksumItems}
	encoded, err := json.Marshal(checksumPayload)
	if err != nil {
		return nil, fmt.Errorf("newapi model pricing checksum encode: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return &connector.ModelPriceSnapshotResult{
		Items: items, Complete: true, SourceVersion: version,
		SourceChecksum: hex.EncodeToString(digest[:]), QuotaPerUnit: quotaPerUnitBefore,
		ObservedAt: time.Now().UTC().Truncate(time.Microsecond),
	}, nil
}

func (c *Client) getQuotaPerUnit(ctx context.Context, site string) (float64, error) {
	body, err := c.getBoundedRawNoRedirect(ctx, site+"/api/status", nil, newAPIStatusMaxBytes)
	if err != nil {
		return 0, err
	}
	if !utf8.Valid(body) {
		return 0, errors.New("status response is not valid UTF-8")
	}
	var wrapped struct {
		Success bool `json:"success"`
		Data    struct {
			QuotaPerUnit *float64 `json:"quota_per_unit"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return 0, err
	}
	if !wrapped.Success || wrapped.Data.QuotaPerUnit == nil || *wrapped.Data.QuotaPerUnit <= 0 ||
		math.IsNaN(*wrapped.Data.QuotaPerUnit) || math.IsInf(*wrapped.Data.QuotaPerUnit, 0) {
		return 0, errors.New("quota_per_unit is missing or invalid")
	}
	return *wrapped.Data.QuotaPerUnit, nil
}

func (c *Client) getModelPricingGroups(ctx context.Context, site string, session *connector.AuthSession) (map[string]float64, error) {
	body, err := c.getBoundedRawNoRedirect(ctx, site+"/api/user/self/groups", session, newAPIModelPricingGroupMaxBytes)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(body) {
		return nil, errors.New("authenticated group response is not valid UTF-8")
	}
	var wrapped struct {
		Success bool            `json:"success"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, err
	}
	if !wrapped.Success {
		message := strings.TrimSpace(wrapped.Message)
		if message == "" {
			message = "upstream rejected authenticated group request"
		}
		return nil, errors.New(message)
	}
	var raw map[string]struct {
		Ratio json.RawMessage `json:"ratio"`
	}
	if err := json.Unmarshal(wrapped.Data, &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, errors.New("authenticated group snapshot is empty")
	}
	groups := make(map[string]float64, len(raw))
	for name, item := range raw {
		canonical, err := strictNewAPIModelPricingGroupName(name)
		if err != nil {
			return nil, errors.New("authenticated group snapshot contains an invalid name")
		}
		if canonical == "auto" {
			var label string
			if json.Unmarshal(item.Ratio, &label) != nil || strings.TrimSpace(label) != "自动" {
				return nil, errors.New("authenticated auto group has an invalid ratio")
			}
			continue
		}
		if _, duplicate := groups[canonical]; duplicate {
			return nil, errors.New("authenticated groups contain a canonical duplicate")
		}
		var ratio *float64
		if err := json.Unmarshal(item.Ratio, &ratio); err != nil || ratio == nil || *ratio < 0 || math.IsNaN(*ratio) || math.IsInf(*ratio, 0) {
			return nil, fmt.Errorf("authenticated group %q has an invalid ratio", name)
		}
		groups[canonical] = *ratio
	}
	if len(groups) == 0 {
		return nil, errors.New("authenticated group snapshot has no numeric groups")
	}
	return groups, nil
}

func validateNewAPIModelPricingGroups(authenticated map[string]float64, usable map[string]string, advertised map[string]float64) (map[string]float64, error) {
	if usable == nil || advertised == nil {
		return nil, errors.New("newapi model pricing group facts are missing")
	}
	usableCanonical := make(map[string]struct{}, len(usable))
	for name := range usable {
		canonical, err := strictNewAPIModelPricingGroupName(name)
		if err != nil {
			return nil, errors.New("newapi model pricing contains an invalid usable group")
		}
		if canonical == "auto" {
			continue
		}
		if _, duplicate := usableCanonical[canonical]; duplicate {
			return nil, errors.New("newapi model pricing contains duplicate canonical usable groups")
		}
		usableCanonical[canonical] = struct{}{}
	}
	advertisedCanonical := make(map[string]float64, len(advertised))
	for name, ratio := range advertised {
		canonical, err := strictNewAPIModelPricingGroupName(name)
		if err != nil || ratio < 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
			return nil, errors.New("newapi model pricing contains invalid group ratios")
		}
		if canonical == "auto" {
			continue
		}
		if _, duplicate := advertisedCanonical[canonical]; duplicate {
			return nil, errors.New("newapi model pricing contains duplicate canonical groups")
		}
		advertisedCanonical[canonical] = ratio
	}
	result := make(map[string]float64, len(authenticated))
	for canonical, ratio := range authenticated {
		if _, ok := usableCanonical[canonical]; !ok {
			return nil, fmt.Errorf("newapi model pricing group %q is not usable by the authenticated account", canonical)
		}
		advertisedRatio, ok := advertisedCanonical[canonical]
		if !ok || advertisedRatio != ratio {
			return nil, fmt.Errorf("newapi model pricing group %q ratio does not match the authenticated snapshot", canonical)
		}
		result[canonical] = ratio
	}
	return result, nil
}

func enabledNewAPIModelPricingGroups(values []string, authenticated map[string]float64) ([]string, []string, error) {
	seen := make(map[string]struct{}, len(values))
	advertised := make([]string, 0, len(values))
	all := false
	for _, value := range values {
		canonical, err := strictNewAPIModelPricingGroupName(value)
		if err != nil {
			return nil, nil, errors.New("invalid enabled group")
		}
		if _, duplicate := seen[canonical]; duplicate {
			return nil, nil, errors.New("duplicate enabled group")
		}
		seen[canonical] = struct{}{}
		advertised = append(advertised, canonical)
		if canonical == "auto" {
			continue
		}
		if canonical == "all" {
			all = true
		}
	}
	sort.Strings(advertised)
	eligible := make(map[string]struct{}, len(values))
	for _, group := range advertised {
		if _, ok := authenticated[group]; ok {
			eligible[group] = struct{}{}
		}
	}
	if all {
		for group := range authenticated {
			eligible[group] = struct{}{}
		}
	}
	groups := make([]string, 0, len(eligible))
	for group := range eligible {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	return advertised, groups, nil
}

func strictNewAPIModelPricingGroupName(value string) (string, error) {
	if value == "" || value != strings.TrimSpace(value) || strings.ContainsRune(value, '\x00') ||
		!utf8.ValidString(value) || utf8.RuneCountInString(value) > 128 {
		return "", errors.New("invalid group name")
	}
	return strings.ToLower(value), nil
}

func (c *Client) getBoundedRawNoRedirect(ctx context.Context, requestURL string, session *connector.AuthSession, maximumBytes int64) ([]byte, error) {
	if maximumBytes <= 0 {
		return nil, errors.New("response size limit is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	if userAgent := strings.TrimSpace(c.http.Header.Get("User-Agent")); userAgent != "" {
		request.Header.Set("User-Agent", userAgent)
	}
	if session != nil {
		if cookie := strings.TrimSpace(session.Cookie); cookie != "" {
			request.Header.Set("Cookie", cookie)
		} else if token := strings.TrimSpace(session.AccessToken); token != "" {
			request.Header.Set("Authorization", token)
		}
		if userID := strings.TrimSpace(session.UserID); userID != "" {
			request.Header.Set("New-Api-User", userID)
		}
	}
	response, err := netproxy.DoWithoutRedirects(c.http.GetClient(), request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		errorBody, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, connector.HTTPStatusError(response.StatusCode, errorBody)
	}
	if response.ContentLength > maximumBytes {
		return nil, errors.New("response exceeds its size limit")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maximumBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) == 0 || int64(len(body)) > maximumBytes {
		return nil, errors.New("response is empty or exceeds its size limit")
	}
	return body, nil
}

func (c *Client) GetAnnouncements(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.AnnouncementResult, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	var items []connector.AnnouncementResult

	if body, err := c.getJSON(ctx, site+"/api/status", nil); err == nil {
		var status struct {
			Announcements []struct {
				ID              any             `json:"id"`
				Title           string          `json:"title"`
				Content         string          `json:"content"`
				Type            string          `json:"type"`
				Link            string          `json:"link"`
				PublishDate     string          `json:"publishDate"`
				PublishedAt     string          `json:"published_at"`
				CreatedAt       string          `json:"created_at"`
				UpdatedAt       string          `json:"updated_at"`
				SourceUpdatedAt string          `json:"source_updated_at"`
				Extra           json.RawMessage `json:"extra"`
			} `json:"announcements"`
		}
		if err := json.Unmarshal(body, &status); err == nil {
			for _, a := range status.Announcements {
				publishedAt := parseAnnouncementTime(a.PublishDate, a.PublishedAt, a.CreatedAt)
				updatedAt := parseAnnouncementTime(a.SourceUpdatedAt, a.UpdatedAt)
				items = append(items, connector.AnnouncementResult{
					SourceKey:       newAPIAnnouncementSourceKey(a.ID, a.Title, a.Content, a.Type, a.PublishDate, a.PublishedAt, a.CreatedAt, a.UpdatedAt),
					Title:           strings.TrimSpace(a.Title),
					Content:         strings.TrimSpace(a.Content),
					Type:            strings.TrimSpace(a.Type),
					Link:            strings.TrimSpace(a.Link),
					PublishedAt:     publishedAt,
					SourceUpdatedAt: updatedAt,
				})
			}
		}
	}

	if body, err := c.getRaw(ctx, site+"/api/notice", nil); err == nil {
		var wrapped newapiResp
		if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.Success {
			text := strings.TrimSpace(newAPIStringFromRaw(wrapped.Data))
			if text != "" {
				items = append(items, connector.AnnouncementResult{
					SourceKey: hashAnnouncementKey("notice", text),
					Title:     "公告",
					Content:   text,
					Type:      "notice",
				})
			}
		}
	}

	return dedupeAnnouncements(items), nil
}

func (c *Client) RedeemCode(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, code string) (*connector.RedeemResult, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	statusBody, err := c.getJSON(ctx, site+"/api/status", nil)
	if err != nil {
		return nil, fmt.Errorf("newapi status: %w", err)
	}
	var status struct {
		QuotaPerUnit float64 `json:"quota_per_unit"`
		Price        float64 `json:"price"`
	}
	if err := json.Unmarshal(statusBody, &status); err != nil {
		return nil, fmt.Errorf("newapi status decode: %w", err)
	}
	if status.QuotaPerUnit <= 0 {
		status.QuotaPerUnit = 500000
	}

	req := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]string{"key": code})
	applyNewAPIAuth(req, session)
	resp, err := req.Post(site + "/api/user/topup")
	if err != nil {
		return nil, fmt.Errorf("newapi redeem http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("newapi redeem: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	var wrapped newapiResp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("newapi redeem decode: %w", err)
	}
	if !wrapped.Success {
		return nil, fmt.Errorf("newapi redeem: %s", wrapped.Message)
	}

	var quota float64
	if err := json.Unmarshal(wrapped.Data, &quota); err != nil {
		return nil, fmt.Errorf("newapi redeem data: %w", err)
	}
	value := quota / status.QuotaPerUnit
	factor, _ := newAPIRechargeFactor(ch, status.Price)
	return &connector.RedeemResult{
		Message: "兑换成功",
		Type:    "balance",
		Value:   round4(value * factor),
	}, nil
}

func (c *Client) GetRechargeInfo(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.RechargeInfo, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/user/topup/info", session)
	if err != nil {
		return nil, fmt.Errorf("newapi topup info: %w", err)
	}
	var raw struct {
		PayMethods    json.RawMessage `json:"pay_methods"`
		MinTopup      float64         `json:"min_topup"`
		AmountOptions json.RawMessage `json:"amount_options"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("newapi topup info decode: %w", err)
	}
	payMethods, err := parseNewAPIPayMethods(raw.PayMethods)
	if err != nil {
		return nil, fmt.Errorf("newapi topup methods decode: %w", err)
	}
	amountOptions, err := parseNewAPIAmountOptions(raw.AmountOptions)
	if err != nil {
		return nil, fmt.Errorf("newapi amount options decode: %w", err)
	}
	methods := make([]connector.RechargeMethod, 0, len(payMethods))
	for _, m := range payMethods {
		t := strings.TrimSpace(m.Type)
		if t != "alipay" && t != "wxpay" {
			continue
		}
		minAmount := raw.MinTopup
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(m.MinTopup), 64); err == nil && parsed > 0 {
			minAmount = parsed
		}
		methods = append(methods, connector.RechargeMethod{
			Type:      t,
			Name:      strings.TrimSpace(m.Name),
			MinAmount: minAmount,
		})
	}
	if len(methods) == 0 {
		return nil, errors.New("上游未配置可用的支付宝/微信支付方式")
	}
	if raw.MinTopup <= 0 {
		raw.MinTopup = methods[0].MinAmount
	}
	if raw.MinTopup <= 0 {
		raw.MinTopup = 1
	}
	if len(amountOptions) == 0 && raw.MinTopup > 0 {
		amountOptions = []float64{raw.MinTopup}
	}
	return &connector.RechargeInfo{
		AmountLabel:   "充值数量",
		AmountStep:    1,
		MinAmount:     raw.MinTopup,
		PresetAmounts: amountOptions,
		Methods:       methods,
	}, nil
}

func (c *Client) CreateRecharge(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, req connector.RechargeRequest) (*connector.RechargeLaunch, error) {
	if req.PaymentMethod != "alipay" && req.PaymentMethod != "wxpay" {
		return nil, errors.New("newapi 仅支持 alipay 或 wxpay")
	}
	if req.Amount <= 0 || math.Trunc(req.Amount) != req.Amount {
		return nil, errors.New("newapi 充值数量必须是正整数")
	}
	r := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]any{
			"amount":         int64(req.Amount),
			"payment_method": req.PaymentMethod,
		})
	applyNewAPIAuth(r, session)
	resp, err := r.Post(strings.TrimRight(ch.SiteURL, "/") + "/api/user/pay")
	if err != nil {
		return nil, fmt.Errorf("newapi create recharge http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("newapi create recharge: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	var wrapped struct {
		Success bool            `json:"success"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
		URL     string          `json:"url"`
	}
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("newapi create recharge decode: %w", err)
	}
	if !wrapped.Success && !strings.EqualFold(strings.TrimSpace(wrapped.Message), "success") {
		msg := newAPIStringFromRaw(wrapped.Data)
		if msg == "" || strings.EqualFold(msg, "null") {
			msg = strings.TrimSpace(wrapped.Message)
		}
		if msg == "" {
			msg = "newapi 发起支付失败"
		}
		return nil, errors.New(msg)
	}
	fields := map[string]string{}
	if len(wrapped.Data) > 0 && string(wrapped.Data) != "null" {
		var rawFields map[string]any
		if err := json.Unmarshal(wrapped.Data, &rawFields); err != nil {
			return nil, fmt.Errorf("newapi create recharge data: %w", err)
		}
		for k, v := range rawFields {
			fields[k] = fmt.Sprint(v)
		}
	}
	action := strings.TrimSpace(wrapped.URL)
	if action == "" || len(fields) == 0 {
		return nil, errors.New("newapi 返回的支付表单信息不完整")
	}
	return &connector.RechargeLaunch{
		Mode:       "form",
		FormAction: action,
		FormFields: fields,
	}, nil
}

func (c *Client) GetRechargeHistory(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.RechargeRecord, error) {
	const pageSize = 100
	type rechargePage struct {
		Items []struct {
			ID           int64    `json:"id"`
			Amount       float64  `json:"amount"`
			Money        *float64 `json:"money"`
			TradeNo      string   `json:"trade_no"`
			Status       string   `json:"status"`
			CompleteTime int64    `json:"complete_time"`
		} `json:"items"`
		Total    *int64 `json:"total"`
		Page     *int   `json:"page"`
		PageSize *int   `json:"page_size"`
		Pages    *int   `json:"pages"`
	}
	result := make([]connector.RechargeRecord, 0, pageSize)
	seenSourceIDs := make(map[string]struct{})
	for pageNum := 1; pageNum <= 100; pageNum++ {
		endpoint := strings.TrimRight(ch.SiteURL, "/") + "/api/user/topup/self?p=" + strconv.Itoa(pageNum) + "&page_size=" + strconv.Itoa(pageSize)
		body, err := c.getJSON(ctx, endpoint, session)
		if err != nil {
			var httpErr *connector.HTTPError
			if pageNum == 1 && errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusNotFound || httpErr.StatusCode == http.StatusMethodNotAllowed || httpErr.StatusCode == http.StatusNotImplemented) {
				return nil, fmt.Errorf("%w: newapi recharge history", connector.ErrUnsupported)
			}
			return nil, fmt.Errorf("newapi recharge history page %d: %w", pageNum, err)
		}
		var page rechargePage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("newapi recharge history page %d decode: %w", pageNum, err)
		}
		total, currentPage, responsePageSize, pages, err := normalizePaginationMetadata(
			pageNum, pageSize, len(page.Items), page.Total, page.Page, page.PageSize, page.Pages,
		)
		if err != nil {
			return nil, fmt.Errorf("newapi recharge history page %d pagination: %w", pageNum, err)
		}
		for _, item := range page.Items {
			if !strings.EqualFold(strings.TrimSpace(item.Status), "success") || item.Amount <= 0 || item.CompleteTime <= 0 ||
				math.IsNaN(item.Amount) || math.IsInf(item.Amount, 0) || (item.Money != nil && (*item.Money < 0 || math.IsNaN(*item.Money) || math.IsInf(*item.Money, 0))) {
				continue
			}
			completedAt := unixFlexible(item.CompleteTime)
			if completedAt.IsZero() {
				continue
			}
			sourceID := strings.TrimSpace(item.TradeNo)
			if sourceID == "" && item.ID > 0 {
				sourceID = strconv.FormatInt(item.ID, 10)
			}
			if sourceID == "" {
				continue
			}
			if _, exists := seenSourceIDs[sourceID]; exists {
				return nil, errors.New("newapi recharge history pagination returned duplicate records")
			}
			seenSourceIDs[sourceID] = struct{}{}
			result = append(result, connector.RechargeRecord{
				SourceID: sourceID, PaidAmount: item.Money,
				CreditedAmount: item.Amount, CompletedAt: completedAt,
			})
		}
		complete := paginatedResponseComplete(currentPage, pages, total, responsePageSize, len(page.Items), pageSize)
		if complete {
			return result, nil
		}
	}
	return nil, errors.New("newapi recharge history exceeds 100 pages; refusing incomplete factor inference")
}

func unixFlexible(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	if value >= 1_000_000_000_000 {
		return time.UnixMilli(value).UTC()
	}
	return time.Unix(value, 0).UTC()
}

func (c *Client) GetSubscriptionInfo(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.SubscriptionInfo, error) {
	return nil, errors.New("newapi 不支持订阅购买")
}

func (c *Client) CreateSubscription(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, req connector.SubscriptionRequest) (*connector.SubscriptionLaunch, error) {
	return nil, errors.New("newapi 不支持订阅购买")
}

func (c *Client) GetSubscriptionUsage(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.SubscriptionUsageInfo, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	selfBody, err := c.getJSON(ctx, site+"/api/subscription/self", session)
	if err != nil {
		return nil, fmt.Errorf("newapi subscription self: %w", err)
	}
	var self newAPISubscriptionSelf
	if err := json.Unmarshal(selfBody, &self); err != nil {
		return nil, fmt.Errorf("newapi subscription self decode: %w", err)
	}

	statusBody, err := c.getJSON(ctx, site+"/api/status", nil)
	if err != nil {
		return nil, fmt.Errorf("newapi subscription status: %w", err)
	}
	var status struct {
		QuotaPerUnit float64 `json:"quota_per_unit"`
	}
	if err := json.Unmarshal(statusBody, &status); err != nil {
		return nil, fmt.Errorf("newapi subscription status decode: %w", err)
	}
	if status.QuotaPerUnit <= 0 {
		status.QuotaPerUnit = 500000
	}

	plans := make(map[int64]newAPISubscriptionPlan)
	if plansBody, err := c.getJSON(ctx, site+"/api/subscription/plans", session); err == nil {
		var entries []newAPISubscriptionPlanEntry
		if json.Unmarshal(plansBody, &entries) == nil {
			for _, entry := range entries {
				if entry.Plan.ID > 0 {
					plans[entry.Plan.ID] = entry.Plan
				}
			}
		}
	}

	now := time.Now().UTC()
	items := make([]connector.SubscriptionUsage, 0, len(self.Subscriptions))
	for _, entry := range self.Subscriptions {
		subscription := entry.Subscription
		if subscription.ID <= 0 {
			continue
		}
		plan := plans[subscription.PlanID]
		name := strings.TrimSpace(plan.Title)
		if name == "" {
			name = fmt.Sprintf("订阅 #%d", subscription.ID)
		}

		item := connector.SubscriptionUsage{
			ID:            subscription.ID,
			GroupID:       subscription.PlanID,
			GroupName:     name,
			Status:        strings.TrimSpace(subscription.Status),
			StartsAt:      newAPIUnixTime(subscription.StartTime),
			ExpiresAt:     newAPIUnixTime(subscription.EndTime),
			ExpiresInDays: newAPISubscriptionExpiresInDays(subscription.EndTime, now),
		}
		window := newAPISubscriptionUsageWindow(subscription, status.QuotaPerUnit)
		if window != nil {
			switch strings.ToLower(strings.TrimSpace(plan.QuotaResetPeriod)) {
			case "daily":
				item.Daily = window
			case "weekly":
				item.Weekly = window
			case "monthly":
				item.Monthly = window
			case "custom":
				item.Cycle = window
			default:
				if subscription.NextResetTime > now.Unix() {
					item.Cycle = window
				} else {
					item.Total = window
				}
			}
		}
		items = append(items, item)
	}
	return &connector.SubscriptionUsageInfo{Items: items}, nil
}

type newAPISubscriptionSelf struct {
	Subscriptions []newAPISubscriptionEntry `json:"subscriptions"`
}

type newAPISubscriptionEntry struct {
	Subscription newAPIUserSubscription `json:"subscription"`
}

type newAPIUserSubscription struct {
	ID            int64  `json:"id"`
	PlanID        int64  `json:"plan_id"`
	AmountTotal   int64  `json:"amount_total"`
	AmountUsed    int64  `json:"amount_used"`
	StartTime     int64  `json:"start_time"`
	EndTime       int64  `json:"end_time"`
	Status        string `json:"status"`
	LastResetTime int64  `json:"last_reset_time"`
	NextResetTime int64  `json:"next_reset_time"`
}

type newAPISubscriptionPlanEntry struct {
	Plan newAPISubscriptionPlan `json:"plan"`
}

type newAPISubscriptionPlan struct {
	ID               int64  `json:"id"`
	Title            string `json:"title"`
	QuotaResetPeriod string `json:"quota_reset_period"`
}

func newAPIUnixTime(value int64) *time.Time {
	if value <= 0 {
		return nil
	}
	at := time.Unix(value, 0).UTC()
	return &at
}

func newAPISubscriptionExpiresInDays(expiresAt int64, now time.Time) int {
	if expiresAt <= now.Unix() {
		return 0
	}
	return int(math.Ceil(time.Unix(expiresAt, 0).Sub(now).Hours() / 24))
}

func newAPISubscriptionUsageWindow(subscription newAPIUserSubscription, quotaPerUnit float64) *connector.SubscriptionUsageWindow {
	if subscription.AmountTotal <= 0 || quotaPerUnit <= 0 {
		return nil
	}
	limit := float64(subscription.AmountTotal) / quotaPerUnit
	used := float64(subscription.AmountUsed) / quotaPerUnit
	if used < 0 {
		used = 0
	}
	remaining := math.Max(0, limit-used)
	usedPercent := math.Min(100, math.Max(0, used/limit*100))
	return &connector.SubscriptionUsageWindow{
		LimitUSD:         round4(limit),
		UsedUSD:          round4(used),
		RemainingUSD:     round4(remaining),
		RemainingPercent: round4(100 - usedPercent),
		UsedPercent:      round4(usedPercent),
		WindowStart:      newAPIUnixTime(subscription.LastResetTime),
		ResetsAt:         newAPIUnixTime(subscription.NextResetTime),
		ResetsInSeconds:  max(int64(0), subscription.NextResetTime-time.Now().Unix()),
	}
}

func (c *Client) ListAPIKeys(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	page, pageSize := normalizeAPIKeyPage(query.Page, query.PageSize)
	site := strings.TrimRight(ch.SiteURL, "/")
	params := url.Values{}
	params.Set("p", strconv.Itoa(page))
	params.Set("page_size", strconv.Itoa(pageSize))
	path := "/api/token/"
	search := strings.TrimSpace(query.Search)
	if search != "" {
		path = "/api/token/search"
		params.Set("keyword", search)
	}
	body, err := c.getJSON(ctx, site+path+"?"+params.Encode(), session)
	if err != nil {
		return nil, fmt.Errorf("newapi api keys: %w", err)
	}
	var raw struct {
		Items    []newAPIToken `json:"items"`
		Total    *int64        `json:"total"`
		Page     *int          `json:"page"`
		PageSize *int          `json:"page_size"`
		Pages    *int          `json:"pages"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("newapi api keys decode: %w", err)
	}
	total, responsePage, responsePageSize, pages, err := normalizePaginationMetadata(
		page, pageSize, len(raw.Items), raw.Total, raw.Page, raw.PageSize, raw.Pages,
	)
	if err != nil {
		return nil, fmt.Errorf("newapi api keys pagination: %w", err)
	}
	if raw.Total == nil && raw.Pages == nil && responsePageSize != pageSize {
		return nil, fmt.Errorf("newapi api keys pagination: response page_size %d does not match requested page_size %d without total or pages", responsePageSize, pageSize)
	}
	groups, _ := c.newAPIGroupMap(ctx, ch, session)
	items := make([]connector.APIKey, 0, len(raw.Items))
	for _, item := range raw.Items {
		key := item.toConnector()
		// NewAPI's token.group string is the authoritative key identity. The
		// separate group endpoint only enriches description and ratio, so a
		// temporary enrichment failure must not make DELETE revalidation drift.
		key.Group = strings.TrimSpace(key.Group)
		key.GroupName = key.Group
		if query.RequireCompleteGroupIdentity && key.Group == "" {
			return nil, fmt.Errorf("newapi api key %d group identity is incomplete", key.ID)
		}
		if g, ok := groups[key.Group]; ok {
			key.GroupDescription = g.Description
			key.GroupRatio = g.Ratio
		}
		items = append(items, key)
	}
	return &connector.APIKeyPage{
		Items:    items,
		Total:    total,
		Page:     responsePage,
		PageSize: responsePageSize,
		Pages:    pages,
	}, nil
}

func (c *Client) LookupRequestCost(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, query connector.RequestCostQuery) (*connector.RequestCostResult, error) {
	keyID := query.APIKeyID
	if keyID <= 0 {
		return nil, errors.New("newapi request cost: api key not found")
	}
	requestID, err := providerid.Normalize(query.RequestID)
	if err != nil {
		return nil, fmt.Errorf("newapi request cost: %w", err)
	}
	query.RequestID = requestID
	statusBody, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/status", nil)
	if err != nil {
		return nil, fmt.Errorf("newapi request cost status: %w", err)
	}
	var status struct {
		QuotaPerUnit float64 `json:"quota_per_unit"`
		Price        float64 `json:"price"`
	}
	if err := json.Unmarshal(statusBody, &status); err != nil {
		return nil, fmt.Errorf("newapi request cost status decode: %w", err)
	}
	if status.QuotaPerUnit <= 0 || math.IsNaN(status.QuotaPerUnit) || math.IsInf(status.QuotaPerUnit, 0) {
		status.QuotaPerUnit = 500000
	}
	start := query.StartedAt.Add(-10 * time.Second).Unix()
	end := query.FinishedAt.Add(10 * time.Second).Unix()
	if query.StartedAt.IsZero() {
		start = time.Now().Add(-time.Minute).Unix()
	}
	if query.FinishedAt.IsZero() {
		end = time.Now().Add(time.Minute).Unix()
	}
	type usageItem struct {
		ID               int64           `json:"id"`
		CreatedAt        int64           `json:"created_at"`
		TokenID          int64           `json:"token_id"`
		TokenName        string          `json:"token_name"`
		ModelName        string          `json:"model_name"`
		PromptTokens     int64           `json:"prompt_tokens"`
		CompletionTokens int64           `json:"completion_tokens"`
		Quota            float64         `json:"quota"`
		RequestID        string          `json:"request_id"`
		Other            json.RawMessage `json:"other"`
	}
	type usagePage struct {
		Items    []usageItem `json:"items"`
		Total    *int64      `json:"total"`
		Page     *int        `json:"page"`
		PageSize *int        `json:"page_size"`
		Pages    *int        `json:"pages"`
	}
	type candidate struct {
		usageID              string
		modelName            string
		cost                 float64
		promptTokens         int64
		completionTokens     int64
		totalTokens          int64
		hasTokenMetrics      bool
		cacheCreationTokens  int64
		cacheReadTokens      int64
		hasCacheTokenMetrics bool
	}
	const pageSize = 100
	const maxPages = 20
	hasRequestID := query.RequestID != ""
	hasLocalTokenMetrics := query.HasTokenMetrics || query.PromptTokens != 0 || query.CompletionTokens != 0
	exact := make([]candidate, 0, 1)
	fallback := make([]candidate, 0, 1)
	complete := false
	var initialExactPage usagePage
	var initialExactTotal int64
	var initialExactPageSize int
	var initialExactPages *int
	var exactFirstPageURL string
	for page := 1; page <= maxPages; page++ {
		params := url.Values{}
		params.Set("p", strconv.Itoa(page))
		params.Set("page_size", strconv.Itoa(pageSize))
		params.Set("type", "0")
		if hasRequestID {
			// Do not let local key names, groups, model aliases, or request timing
			// exclude the provider-authoritative billing ID before local key-ID and
			// uniqueness checks run.
			params.Set("request_id", query.RequestID)
		} else {
			params.Set("token_name", strings.TrimSpace(query.APIKeyName))
			params.Set("model_name", strings.TrimSpace(query.ModelName))
			params.Set("start_timestamp", strconv.FormatInt(start, 10))
			params.Set("end_timestamp", strconv.FormatInt(end, 10))
			params.Set("group", strings.TrimSpace(query.GroupName))
		}
		requestURL := strings.TrimRight(ch.SiteURL, "/") + "/api/log/self?" + params.Encode()
		if hasRequestID && page == 1 {
			exactFirstPageURL = requestURL
		}
		body, err := c.getJSON(ctx, requestURL, session)
		if err != nil {
			return nil, fmt.Errorf("newapi request cost: %w", err)
		}
		if !utf8.Valid(body) {
			return nil, errors.New("newapi request cost: usage response is not valid UTF-8")
		}
		var raw usagePage
		if err := json.Unmarshal(body, &raw); err != nil {
			return nil, fmt.Errorf("newapi request cost decode: %w", err)
		}
		responsePageSize := pageSize
		if hasRequestID {
			if raw.Total == nil || raw.Page == nil || raw.PageSize == nil {
				return nil, errors.New("newapi request cost: incomplete pagination metadata")
			}
			_, _, normalizedPageSize, _, paginationErr := normalizePaginationMetadata(
				page, pageSize, len(raw.Items), raw.Total, raw.Page, raw.PageSize, raw.Pages,
			)
			if paginationErr != nil {
				return nil, fmt.Errorf("newapi request cost: invalid pagination metadata: %w", paginationErr)
			}
			if normalizedPageSize > pageSize {
				return nil, errors.New("newapi request cost: response page_size exceeds requested maximum")
			}
			responsePageSize = normalizedPageSize
			if page == 1 {
				initialExactPage = raw
				initialExactPage.Items = append([]usageItem(nil), raw.Items...)
				initialExactTotal = *raw.Total
				initialExactPageSize = responsePageSize
				if raw.Pages != nil {
					value := *raw.Pages
					initialExactPages = &value
				}
			} else if *raw.Total != initialExactTotal || responsePageSize != initialExactPageSize ||
				(raw.Pages == nil) != (initialExactPages == nil) ||
				(raw.Pages != nil && *raw.Pages != *initialExactPages) {
				return nil, errors.New("newapi request cost: pagination snapshot changed during scan")
			}
		}
		for _, item := range raw.Items {
			normalizedRequestID, requestIDErr := providerid.Normalize(item.RequestID)
			if requestIDErr != nil {
				return nil, fmt.Errorf("newapi request cost: invalid usage provider request ID: %w", requestIDErr)
			}
			item.RequestID = normalizedRequestID
			if item.TokenID != keyID {
				continue
			}
			exactRequestID := hasRequestID && requestIDsEqual(item.RequestID, query.RequestID)
			if hasRequestID && !exactRequestID {
				continue
			}
			createdAt := time.Unix(item.CreatedAt, 0)
			if item.CreatedAt <= 0 {
				if exactRequestID {
					return nil, errors.New("newapi request cost: usage timestamp invalid")
				}
				continue
			}
			if !exactRequestID {
				if !hasLocalTokenMetrics || !requestCostTimeMatches(createdAt, query.StartedAt, query.FinishedAt) {
					continue
				}
				if query.ModelName != "" && !strings.EqualFold(strings.TrimSpace(item.ModelName), strings.TrimSpace(query.ModelName)) {
					continue
				}
			}
			tokens, validTokens := newAPIUsageTokenMetrics(item.PromptTokens, item.CompletionTokens, item.Other)
			if !validTokens {
				return nil, errors.New("newapi request cost: usage token metrics invalid")
			}
			if !exactRequestID && (tokens.PromptTokens != query.PromptTokens || tokens.CompletionTokens != query.CompletionTokens) {
				continue
			}
			if item.Quota < 0 || math.IsNaN(item.Quota) || math.IsInf(item.Quota, 0) {
				return nil, errors.New("newapi request cost: usage cost invalid")
			}
			cost := item.Quota / status.QuotaPerUnit
			if cost < 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
				return nil, errors.New("newapi request cost: usage cost invalid")
			}
			matched := candidate{
				usageID:              newAPIUsageIdentity(item.RequestID, keyID, item.CreatedAt, item.ModelName, tokens.PromptTokens, tokens.CompletionTokens, item.Quota),
				modelName:            strings.TrimSpace(item.ModelName),
				cost:                 cost,
				promptTokens:         tokens.PromptTokens,
				completionTokens:     tokens.CompletionTokens,
				totalTokens:          tokens.TotalTokens,
				hasTokenMetrics:      tokens.HasTokenMetrics,
				cacheCreationTokens:  tokens.CacheCreationTokens,
				cacheReadTokens:      tokens.CacheReadTokens,
				hasCacheTokenMetrics: tokens.HasCacheTokenMetrics,
			}
			if exactRequestID {
				exact = append(exact, matched)
				continue
			}
			if !hasRequestID {
				fallback = append(fallback, matched)
			}
		}
		if hasRequestID {
			if int64(page)*int64(responsePageSize) >= *raw.Total {
				complete = true
				break
			}
		} else if len(raw.Items) < pageSize || (raw.Total != nil && *raw.Total > 0 && int64(page*pageSize) >= *raw.Total) {
			complete = true
			break
		}
	}
	if !complete {
		return nil, errors.New("newapi request cost: usage scan incomplete")
	}
	if hasRequestID {
		refreshBody, err := c.getJSON(ctx, exactFirstPageURL, session)
		if err != nil {
			return nil, fmt.Errorf("newapi request cost final snapshot: %w", err)
		}
		if !utf8.Valid(refreshBody) {
			return nil, errors.New("newapi request cost: final snapshot usage response is not valid UTF-8")
		}
		var refreshed usagePage
		if err := json.Unmarshal(refreshBody, &refreshed); err != nil {
			return nil, fmt.Errorf("newapi request cost final snapshot decode: %w", err)
		}
		if refreshed.Total == nil || refreshed.Page == nil || refreshed.PageSize == nil {
			return nil, errors.New("newapi request cost: final snapshot incomplete pagination metadata")
		}
		_, _, refreshedPageSize, _, paginationErr := normalizePaginationMetadata(
			1, pageSize, len(refreshed.Items), refreshed.Total, refreshed.Page, refreshed.PageSize, refreshed.Pages,
		)
		if paginationErr != nil {
			return nil, fmt.Errorf("newapi request cost final snapshot pagination: %w", paginationErr)
		}
		if refreshedPageSize > pageSize {
			return nil, errors.New("newapi request cost: final snapshot page_size exceeds requested maximum")
		}
		if *refreshed.Total != initialExactTotal || refreshedPageSize != initialExactPageSize ||
			(refreshed.Pages == nil) != (initialExactPages == nil) ||
			(refreshed.Pages != nil && *refreshed.Pages != *initialExactPages) ||
			!reflect.DeepEqual(refreshed.Items, initialExactPage.Items) {
			return nil, errors.New("newapi request cost: exact pagination snapshot changed")
		}
	}
	if len(exact) == 1 {
		matched := exact[0]
		return &connector.RequestCostResult{
			APIKeyID:             keyID,
			UsageID:              matched.usageID,
			ModelName:            matched.modelName,
			Cost:                 matched.cost,
			Source:               "newapi_quota",
			PromptTokens:         matched.promptTokens,
			CompletionTokens:     matched.completionTokens,
			TotalTokens:          matched.totalTokens,
			HasTokenMetrics:      matched.hasTokenMetrics,
			CacheCreationTokens:  matched.cacheCreationTokens,
			CacheReadTokens:      matched.cacheReadTokens,
			HasCacheTokenMetrics: matched.hasCacheTokenMetrics,
		}, nil
	}
	if len(exact) > 1 {
		return nil, errors.New("newapi request cost: multiple usage records matched")
	}
	if hasRequestID {
		return nil, errors.New("newapi request cost: usage record not found")
	}
	if len(fallback) == 1 {
		matched := fallback[0]
		return &connector.RequestCostResult{
			APIKeyID:             keyID,
			UsageID:              matched.usageID,
			ModelName:            matched.modelName,
			Cost:                 matched.cost,
			Source:               "newapi_quota",
			PromptTokens:         matched.promptTokens,
			CompletionTokens:     matched.completionTokens,
			TotalTokens:          matched.totalTokens,
			HasTokenMetrics:      matched.hasTokenMetrics,
			CacheCreationTokens:  matched.cacheCreationTokens,
			CacheReadTokens:      matched.cacheReadTokens,
			HasCacheTokenMetrics: matched.hasCacheTokenMetrics,
		}, nil
	}
	if len(fallback) > 1 {
		return nil, errors.New("newapi request cost: multiple usage records matched")
	}
	return nil, errors.New("newapi request cost: usage record not found")
}

type newAPIUsageTokens struct {
	PromptTokens         int64
	CompletionTokens     int64
	TotalTokens          int64
	CacheCreationTokens  int64
	CacheReadTokens      int64
	HasTokenMetrics      bool
	HasCacheTokenMetrics bool
}

func newAPIUsageTokenMetrics(promptTokens, completionTokens int64, raw json.RawMessage) (newAPIUsageTokens, bool) {
	if promptTokens < 0 || completionTokens < 0 {
		return newAPIUsageTokens{}, false
	}
	fields, ok := newAPIUsageOtherFields(raw)
	if !ok {
		totalTokens, valid := newAPIAddTokenCounts(promptTokens, completionTokens)
		if !valid {
			return newAPIUsageTokens{}, false
		}
		return newAPIUsageTokens{
			PromptTokens:     promptTokens,
			CompletionTokens: completionTokens,
			TotalTokens:      totalTokens,
			HasTokenMetrics:  promptTokens > 0 || completionTokens > 0,
		}, true
	}
	if !newAPIUsageTokenFieldsValid(fields,
		"cache_tokens", "cached_tokens", "cache_read_tokens", "cache_write_tokens", "cache_creation_tokens", "input_tokens_total",
	) {
		return newAPIUsageTokens{}, false
	}
	cacheCreationTokens, cacheReadTokens, hasCacheTokenMetrics := newAPICacheTokenMetricsFromFields(fields)
	normalizedPromptTokens := promptTokens
	semantic := strings.ToLower(strings.TrimSpace(newAPIUsageString(fields, "usage_semantic")))
	if semantic == "anthropic" {
		if inputTokens, hasInputTokens := maxNewAPICacheTokenValue(fields, "input_tokens_total"); hasInputTokens {
			normalizedPromptTokens = inputTokens
		} else {
			var valid bool
			normalizedPromptTokens, valid = newAPIAddTokenCounts(normalizedPromptTokens, cacheCreationTokens, cacheReadTokens)
			if !valid {
				return newAPIUsageTokens{}, false
			}
		}
	}
	totalTokens, valid := newAPIAddTokenCounts(normalizedPromptTokens, completionTokens)
	if !valid {
		return newAPIUsageTokens{}, false
	}
	return newAPIUsageTokens{
		PromptTokens:         normalizedPromptTokens,
		CompletionTokens:     completionTokens,
		TotalTokens:          totalTokens,
		CacheCreationTokens:  cacheCreationTokens,
		CacheReadTokens:      cacheReadTokens,
		HasTokenMetrics:      normalizedPromptTokens > 0 || completionTokens > 0 || (semantic == "anthropic" && hasCacheTokenMetrics),
		HasCacheTokenMetrics: hasCacheTokenMetrics,
	}, true
}

func newAPIAddTokenCounts(values ...int64) (int64, bool) {
	var total int64
	for _, value := range values {
		if value < 0 || total > math.MaxInt64-value {
			return 0, false
		}
		total += value
	}
	return total, true
}

func newAPIUsageTokenFieldsValid(fields map[string]json.RawMessage, names ...string) bool {
	for _, name := range names {
		raw, exists := fields[name]
		if !exists {
			continue
		}
		value := strings.TrimSpace(string(raw))
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			if err := json.Unmarshal(raw, &value); err != nil {
				return false
			}
			value = strings.TrimSpace(value)
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			return false
		}
	}
	return true
}

func newAPICacheTokenMetrics(raw json.RawMessage) (cacheCreationTokens, cacheReadTokens int64, ok bool) {
	fields, ok := newAPIUsageOtherFields(raw)
	if !ok {
		return 0, 0, false
	}
	return newAPICacheTokenMetricsFromFields(fields)
}

func newAPICacheTokenMetricsFromFields(fields map[string]json.RawMessage) (cacheCreationTokens, cacheReadTokens int64, ok bool) {
	if len(fields) == 0 {
		return 0, 0, false
	}
	cacheReadTokens, hasCacheReadTokens := maxNewAPICacheTokenValue(fields, "cache_tokens", "cached_tokens", "cache_read_tokens")
	cacheCreationTokens, hasCacheCreationTokens := maxNewAPICacheTokenValue(fields, "cache_write_tokens", "cache_creation_tokens")
	return cacheCreationTokens, cacheReadTokens, hasCacheReadTokens || hasCacheCreationTokens
}

func newAPIUsageOtherFields(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	payload := strings.TrimSpace(string(raw))
	if payload == "" || payload == "null" {
		return nil, false
	}
	if payload[0] == '"' {
		var encoded string
		if err := json.Unmarshal(raw, &encoded); err != nil {
			return nil, false
		}
		payload = strings.TrimSpace(encoded)
	}
	var fields map[string]json.RawMessage
	if payload == "" || json.Unmarshal([]byte(payload), &fields) != nil {
		return nil, false
	}
	return fields, true
}

func newAPIUsageString(fields map[string]json.RawMessage, name string) string {
	raw, ok := fields[name]
	if !ok {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return value
	}
	return strings.Trim(string(raw), "\"")
}

func maxNewAPICacheTokenValue(fields map[string]json.RawMessage, names ...string) (int64, bool) {
	var maximum int64
	found := false
	for _, name := range names {
		raw, exists := fields[name]
		if !exists {
			continue
		}
		value := strings.TrimSpace(string(raw))
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			if err := json.Unmarshal(raw, &value); err != nil {
				continue
			}
			value = strings.TrimSpace(value)
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			continue
		}
		found = true
		if parsed > maximum {
			maximum = parsed
		}
	}
	return maximum, found
}

func newAPIUsageIdentity(requestID string, keyID, createdAt int64, model string, promptTokens, completionTokens int64, quota float64) string {
	stable := requestID
	if stable == "" {
		stable = fmt.Sprintf("%d|%d|%s|%d|%d|%.17g", keyID, createdAt, strings.TrimSpace(model), promptTokens, completionTokens, quota)
	}
	sum := sha256.Sum256([]byte(stable))
	return "newapi:" + hex.EncodeToString(sum[:])
}

func requestCostTimeMatches(createdAt, startedAt, finishedAt time.Time) bool {
	if startedAt.IsZero() && finishedAt.IsZero() {
		return true
	}
	if finishedAt.IsZero() {
		finishedAt = startedAt
	}
	if startedAt.IsZero() {
		startedAt = finishedAt
	}
	return !createdAt.Before(startedAt.Add(-10*time.Second)) && !createdAt.After(finishedAt.Add(10*time.Second))
}

func requestIDsEqual(left, right string) bool {
	return left != "" && left == right
}

func (c *Client) ListAPIKeyGroups(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.APIKeyGroup, error) {
	groups, err := c.newAPIGroupMap(ctx, ch, session)
	if err != nil {
		return nil, err
	}
	out := make([]connector.APIKeyGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, g)
	}
	return out, nil
}

func (c *Client) CreateAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	if strings.TrimSpace(req.Name) == "" {
		return nil, errors.New("密钥名称不能为空")
	}
	body := buildNewAPICreateToken(req)
	createStartedAt := time.Now()
	restyReq := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(body)
	applyNewAPIAuth(restyReq, session)
	resp, err := restyReq.Post(strings.TrimRight(ch.SiteURL, "/") + "/api/token/")
	if err != nil {
		return nil, fmt.Errorf("newapi create api key http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("newapi create api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	data, err := decodeNewAPIWriteData(resp.Body(), "newapi create api key")
	if err != nil {
		return nil, err
	}
	var created *connector.APIKey
	if len(data) > 0 && string(data) != "null" {
		if out, decodeErr := decodeNewAPICreatedKey(data); decodeErr == nil {
			created = out
		}
	}
	if created == nil || created.ID <= 0 {
		var findErr error
		created, findErr = c.findNewlyCreatedAPIKey(ctx, ch, session, req.Name, req.Group, createStartedAt)
		if findErr != nil {
			return nil, fmt.Errorf("newapi create api key: created but failed to identify actual key: %s", newAPISanitizeErrorText(findErr.Error()))
		}
	}
	key, revealErr := c.revealNewlyCreatedAPIKey(ctx, ch, session, created.ID)
	if revealErr != nil || strings.TrimSpace(key) == "" {
		if revealErr == nil {
			revealErr = errors.New("empty key")
		}
		return created, fmt.Errorf("newapi create api key: created but failed to reveal actual key: %s", newAPISanitizeErrorText(revealErr.Error()))
	}
	created.Key = strings.TrimSpace(key)
	if strings.TrimSpace(created.Name) == "" {
		created.Name = strings.TrimSpace(req.Name)
	}
	if strings.TrimSpace(created.Group) == "" {
		created.Group = strings.TrimSpace(req.Group)
	}
	return created, nil
}

func decodeNewAPICreatedKey(data json.RawMessage) (*connector.APIKey, error) {
	return decodeNewAPICreatedKeyValue(data, 0)
}

func decodeNewAPICreatedKeyValue(data json.RawMessage, depth int) (*connector.APIKey, error) {
	candidates, structurallyPresent, err := collectNewAPICreatedKeyCandidates(data, depth)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		if structurallyPresent {
			return nil, errors.New("newapi created key id is invalid")
		}
		return nil, errors.New("newapi created key id was not returned")
	}
	return mergeNewAPICreatedKeyCandidates(candidates)
}

func collectNewAPICreatedKeyCandidates(data json.RawMessage, depth int) ([]connector.APIKey, bool, error) {
	if depth > 3 {
		return nil, true, errors.New("newapi created key response nesting is too deep")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err == nil && object != nil {
		candidates := make([]connector.APIKey, 0, 4)
		structurallyPresent := false
		var createdID int64
		idPresent := false
		for _, idField := range []string{"id", "token_id", "key_id"} {
			rawID, ok := object[idField]
			if !ok {
				continue
			}
			structurallyPresent = true
			id, idErr := parseNewAPICreatedKeyID(rawID)
			if idErr != nil {
				return nil, true, fmt.Errorf("newapi created key %s: %w", idField, idErr)
			}
			if idPresent && id != createdID {
				return nil, true, errors.New("newapi created key response contains conflicting ids")
			}
			createdID = id
			idPresent = true
		}
		if idPresent {
			candidate, err := decodeNewAPICreatedKeyObjectCandidate(object, createdID)
			if err != nil {
				return nil, true, err
			}
			candidates = append(candidates, candidate)
		} else if newAPICreatedKeyObjectHasIdentity(object) {
			return nil, true, errors.New("newapi created key response identity is detached from its id")
		}
		for _, containerField := range []string{"token", "key", "data"} {
			nested, ok := object[containerField]
			if !ok {
				continue
			}
			nestedCandidates, nestedPresent, nestedErr := collectNewAPICreatedKeyCandidates(nested, depth+1)
			if nestedErr != nil {
				return nil, true, nestedErr
			}
			structurallyPresent = structurallyPresent || nestedPresent
			candidates = append(candidates, nestedCandidates...)
		}
		return candidates, structurallyPresent, nil
	}
	id, err := parseNewAPICreatedKeyID(data)
	if err != nil {
		return nil, false, nil
	}
	return []connector.APIKey{{ID: id, Source: connector.APIKeySourceNewAPI}}, true, nil
}

func decodeNewAPICreatedKeyObjectCandidate(object map[string]json.RawMessage, id int64) (connector.APIKey, error) {
	candidateObject := make(map[string]json.RawMessage, len(object)+1)
	for name, value := range object {
		candidateObject[name] = value
	}
	candidateObject["id"] = json.RawMessage(strconv.FormatInt(id, 10))
	delete(candidateObject, "token_id")
	delete(candidateObject, "key_id")
	delete(candidateObject, "token")
	delete(candidateObject, "data")
	delete(candidateObject, "key")
	candidate, err := json.Marshal(candidateObject)
	if err != nil {
		return connector.APIKey{}, fmt.Errorf("newapi created key response identity encode: %w", err)
	}
	var token newAPIToken
	if err := json.Unmarshal(candidate, &token); err != nil {
		return connector.APIKey{}, fmt.Errorf("newapi created key response identity decode: %w", err)
	}
	out := token.toConnector()
	out.ID = id
	return out, nil
}

func newAPICreatedKeyObjectHasIdentity(object map[string]json.RawMessage) bool {
	for _, field := range []string{"name", "group", "status"} {
		if _, ok := object[field]; ok {
			return true
		}
	}
	return false
}

func mergeNewAPICreatedKeyCandidates(candidates []connector.APIKey) (*connector.APIKey, error) {
	out := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.ID != out.ID {
			return nil, errors.New("newapi created key response contains conflicting ids")
		}
		if err := mergeNewAPICreatedKeyIdentity(&out, candidate); err != nil {
			return nil, err
		}
	}
	return &out, nil
}

func mergeNewAPICreatedKeyIdentity(out *connector.APIKey, candidate connector.APIKey) error {
	if err := mergeNewAPICreatedKeyIdentityField(
		"name",
		&out.Name,
		&out.ObservedIdentityFields.Name,
		&out.NullIdentityFields.Name,
		candidate.Name,
		candidate.ObservedIdentityFields.Name,
		candidate.NullIdentityFields.Name,
	); err != nil {
		return err
	}
	if err := mergeNewAPICreatedKeyIdentityField(
		"group",
		&out.Group,
		&out.ObservedIdentityFields.Group,
		&out.NullIdentityFields.Group,
		candidate.Group,
		candidate.ObservedIdentityFields.Group,
		candidate.NullIdentityFields.Group,
	); err != nil {
		return err
	}
	return mergeNewAPICreatedKeyIdentityField(
		"status",
		&out.Status,
		&out.ObservedIdentityFields.Status,
		&out.NullIdentityFields.Status,
		candidate.Status,
		candidate.ObservedIdentityFields.Status,
		candidate.NullIdentityFields.Status,
	)
}

func mergeNewAPICreatedKeyIdentityField(
	field string,
	outValue *string,
	outObserved *bool,
	outNull *bool,
	candidateValue string,
	candidateObserved bool,
	candidateNull bool,
) error {
	if !candidateObserved {
		return nil
	}
	if *outObserved {
		if *outNull != candidateNull || (!candidateNull && *outValue != candidateValue) {
			return fmt.Errorf("newapi created key response contains conflicting %s identity", field)
		}
		return nil
	}
	*outValue = candidateValue
	*outObserved = true
	*outNull = candidateNull
	return nil
}

func parseNewAPICreatedKeyID(data json.RawMessage) (int64, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return 0, errors.New("newapi created key id is invalid")
	}
	var text string
	switch typed := value.(type) {
	case json.Number:
		text = typed.String()
	case string:
		text = strings.TrimSpace(typed)
	default:
		return 0, errors.New("newapi created key id is invalid")
	}
	id, err := strconv.ParseInt(text, 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("newapi created key id is invalid")
	}
	return id, nil
}

func (c *Client) findNewlyCreatedAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, name, group string, createdAfter time.Time) (*connector.APIKey, error) {
	const maxAttempts = 3
	const maxPages = 10
	const pageSize = 100
	name = strings.TrimSpace(name)
	group = strings.TrimSpace(group)
	createdAfter = createdAfter.Add(-2 * time.Second)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		matches := make(map[int64]connector.APIKey)
		complete := false
		for pageNum := 1; pageNum <= maxPages; pageNum++ {
			page, err := c.ListAPIKeys(ctx, ch, session, connector.APIKeyQuery{Page: pageNum, PageSize: pageSize, Search: name})
			if err != nil {
				return nil, err
			}
			for _, item := range page.Items {
				itemGroup := strings.TrimSpace(item.GroupName)
				if itemGroup == "" {
					itemGroup = strings.TrimSpace(item.Group)
				}
				if item.ID <= 0 || strings.TrimSpace(item.Name) != name || !strings.EqualFold(itemGroup, group) || item.CreatedAt == nil || item.CreatedAt.Before(createdAfter) {
					continue
				}
				matches[item.ID] = item
			}
			if paginatedResponseComplete(page.Page, page.Pages, page.Total, page.PageSize, len(page.Items), pageSize) {
				complete = true
				break
			}
		}
		if !complete {
			return nil, errors.New("newapi created key lookup exceeds safe page limit")
		}
		if len(matches) == 1 {
			for _, item := range matches {
				return &item, nil
			}
		}
		if len(matches) > 1 {
			return nil, fmt.Errorf("newapi created key lookup matched %d new keys", len(matches))
		}
		if attempt+1 < maxAttempts {
			if err := waitForNewAPIKey(ctx, time.Duration(attempt+1)*150*time.Millisecond); err != nil {
				return nil, err
			}
		}
	}
	return nil, errors.New("newapi created key id was not returned")
}

func waitForNewAPIKey(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) revealNewlyCreatedAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, createdID int64) (string, error) {
	if createdID > 0 {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			key, err := c.RevealAPIKey(ctx, ch, session, createdID)
			if err == nil && strings.TrimSpace(key) != "" {
				return key, nil
			}
			lastErr = err
			if attempt < 2 {
				if waitErr := waitForNewAPIKey(ctx, time.Duration(attempt+1)*100*time.Millisecond); waitErr != nil {
					return "", waitErr
				}
			}
		}
		if lastErr == nil {
			lastErr = errors.New("empty key")
		}
		return "", fmt.Errorf("newapi created key id %d could not be revealed: %w", createdID, lastErr)
	}
	return "", errors.New("newapi created key id was not returned")
}

func normalizePaginationMetadata(requestedPage, requestedPageSize, itemCount int, total *int64, responsePage, responsePageSize, pages *int) (int64, int, int, int, error) {
	currentPage := requestedPage
	if responsePage != nil {
		if *responsePage != requestedPage {
			return 0, 0, 0, 0, fmt.Errorf("response page %d does not match requested page %d", *responsePage, requestedPage)
		}
		currentPage = *responsePage
	}
	pageSize := requestedPageSize
	if responsePageSize != nil {
		pageSize = *responsePageSize
	}
	if currentPage < 1 || pageSize < 1 || itemCount > pageSize {
		return 0, 0, 0, 0, errors.New("invalid page, page_size, or item count")
	}

	totalValue := int64(0)
	if total != nil {
		if *total < 0 {
			return 0, 0, 0, 0, errors.New("negative total")
		}
		totalValue = *total
		firstItem := int64(currentPage-1) * int64(pageSize)
		returnedThrough := firstItem + int64(itemCount)
		if totalValue < returnedThrough {
			return 0, 0, 0, 0, fmt.Errorf("total %d is smaller than returned item range ending at %d", totalValue, returnedThrough)
		}
		if itemCount < pageSize && totalValue > returnedThrough {
			return 0, 0, 0, 0, fmt.Errorf("short page ended at %d before total %d", returnedThrough, totalValue)
		}
	}

	pagesValue := 0
	if pages != nil {
		if *pages < 1 {
			return 0, 0, 0, 0, errors.New("pages must be positive")
		}
		pagesValue = *pages
		if currentPage > pagesValue {
			return 0, 0, 0, 0, fmt.Errorf("response page %d exceeds pages %d", currentPage, pagesValue)
		}
		if currentPage < pagesValue && itemCount < pageSize {
			return 0, 0, 0, 0, fmt.Errorf("short page %d precedes final page %d", currentPage, pagesValue)
		}
	}
	if total != nil && pages != nil {
		expectedPages := pagesFromTotal(totalValue, pageSize)
		if pagesValue != expectedPages {
			return 0, 0, 0, 0, fmt.Errorf("pages %d does not match total %d and page_size %d", pagesValue, totalValue, pageSize)
		}
	}
	return totalValue, currentPage, pageSize, pagesValue, nil
}

func paginatedResponseComplete(currentPage, pages int, total int64, responsePageSize, itemCount, requestedPageSize int) bool {
	if currentPage < 1 {
		return false
	}
	metadataPresent := false
	if pages > 0 {
		metadataPresent = true
		if currentPage < pages || currentPage > pages {
			return false
		}
	}
	if total > 0 {
		metadataPresent = true
		pageSize := responsePageSize
		if pageSize <= 0 {
			pageSize = requestedPageSize
		}
		if pageSize <= 0 || int64(currentPage*pageSize) < total {
			return false
		}
		if pages > 0 {
			expectedPages := int((total + int64(pageSize) - 1) / int64(pageSize))
			if pages < expectedPages {
				return false
			}
		}
	}
	if metadataPresent {
		return true
	}
	pageSize := responsePageSize
	if pageSize <= 0 {
		pageSize = requestedPageSize
	}
	return pageSize > 0 && itemCount < pageSize
}

func (c *Client) UpdateAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	if id <= 0 {
		return nil, errors.New("密钥 ID 无效")
	}
	body := buildNewAPIUpdateToken(id, req)
	restyReq := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(body)
	applyNewAPIAuth(restyReq, session)
	resp, err := restyReq.Put(strings.TrimRight(ch.SiteURL, "/") + "/api/token/")
	if err != nil {
		return nil, fmt.Errorf("newapi update api key http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("newapi update api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	data, err := decodeNewAPIWriteData(resp.Body(), "newapi update api key")
	if err != nil {
		return nil, err
	}
	var token newAPIToken
	if len(data) > 0 && string(data) != "null" {
		_ = json.Unmarshal(data, &token)
	}
	if token.ID == 0 {
		token.ID = int(id)
		if v, ok := body["name"].(string); ok {
			token.Name = v
		}
		if v, ok := body["status"].(int); ok {
			token.Status = v
		}
	}
	out := token.toConnector()
	return &out, nil
}

func (c *Client) DeleteAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64) error {
	if id <= 0 {
		return errors.New("密钥 ID 无效")
	}
	delReq := c.http.R().SetContext(ctx)
	applyNewAPIAuth(delReq, session)
	resp, err := delReq.Delete(strings.TrimRight(ch.SiteURL, "/") + "/api/token/" + strconv.FormatInt(id, 10))
	if err != nil {
		return fmt.Errorf("newapi delete api key http: %w", err)
	}
	if resp.IsError() {
		return fmt.Errorf("newapi delete api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	return decodeNewAPIWrite(resp.Body(), "newapi delete api key")
}

func (c *Client) RevealAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64) (string, error) {
	if id <= 0 {
		return "", errors.New("密钥 ID 无效")
	}
	revealReq := c.http.R().SetContext(ctx)
	applyNewAPIAuth(revealReq, session)
	resp, err := revealReq.Post(strings.TrimRight(ch.SiteURL, "/") + "/api/token/" + strconv.FormatInt(id, 10) + "/key")
	if err != nil {
		return "", fmt.Errorf("newapi reveal api key http: %w", err)
	}
	if resp.IsError() {
		return "", fmt.Errorf("newapi reveal api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	data, err := decodeNewAPIWriteData(resp.Body(), "newapi reveal api key")
	if err != nil {
		return "", err
	}
	var raw struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return "", fmt.Errorf("newapi reveal api key data: %w", err)
	}
	if strings.TrimSpace(raw.Key) == "" {
		return "", errors.New("newapi 未返回完整密钥")
	}
	return raw.Key, nil
}

type newAPIPayMethod struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	MinTopup string `json:"min_topup"`
}

func parseNewAPIPayMethods(raw json.RawMessage) ([]newAPIPayMethod, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var methods []newAPIPayMethod
	if err := json.Unmarshal(raw, &methods); err == nil {
		return methods, nil
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return nil, err
	}
	if strings.TrimSpace(encoded) == "" {
		return nil, nil
	}
	if err := json.Unmarshal([]byte(encoded), &methods); err != nil {
		return nil, err
	}
	return methods, nil
}

func parseNewAPIAmountOptions(raw json.RawMessage) ([]float64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var options []float64
	if err := json.Unmarshal(raw, &options); err == nil {
		return options, nil
	}
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return nil, err
	}
	if strings.TrimSpace(encoded) == "" {
		return nil, nil
	}
	if err := json.Unmarshal([]byte(encoded), &options); err != nil {
		return nil, err
	}
	return options, nil
}

func newAPIStringFromRaw(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(string(raw))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func rawObjectToStringMap(raw json.RawMessage) (map[string]string, error) {
	fields := map[string]string{}
	if len(raw) == 0 || string(raw) == "null" {
		return fields, nil
	}
	var rawFields map[string]any
	if err := json.Unmarshal(raw, &rawFields); err != nil {
		return nil, err
	}
	for k, v := range rawFields {
		fields[k] = fmt.Sprint(v)
	}
	return fields, nil
}

func stringFieldFromRaw(raw json.RawMessage, field string) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return ""
	}
	value, ok := data[field]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func decodeNewAPIPaymentResponse(body []byte, prefix string) (json.RawMessage, string, string, error) {
	var wrapped struct {
		Success bool            `json:"success"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
		URL     string          `json:"url"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, "", "", fmt.Errorf("%s decode: %w", prefix, err)
	}
	if !wrapped.Success && !strings.EqualFold(strings.TrimSpace(wrapped.Message), "success") {
		msg := strings.TrimSpace(wrapped.Message)
		if msg == "" {
			msg = newAPIStringFromRaw(wrapped.Data)
		}
		if msg == "" || strings.EqualFold(msg, "null") {
			msg = prefix + " failed"
		}
		return nil, "", "", errors.New(msg)
	}
	return wrapped.Data, strings.TrimSpace(wrapped.URL), strings.TrimSpace(wrapped.Message), nil
}

func (c *Client) getJSON(ctx context.Context, url string, session *connector.AuthSession) ([]byte, error) {
	body, err := c.getRaw(ctx, url, session)
	if err != nil {
		return nil, err
	}
	var wrapped newapiResp
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if !wrapped.Success {
		return nil, errors.New(wrapped.Message)
	}
	return wrapped.Data, nil
}

func (c *Client) postJSON(ctx context.Context, url string, session *connector.AuthSession, body any) ([]byte, error) {
	req := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(body)
	applyNewAPIAuth(req, session)
	resp, err := req.Post(url)
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, connector.HTTPStatusError(resp.StatusCode(), resp.Body())
	}
	return resp.Body(), nil
}

func (c *Client) getRaw(ctx context.Context, url string, session *connector.AuthSession) ([]byte, error) {
	req := c.http.R().SetContext(ctx)
	applyNewAPIAuth(req, session)
	// NewAPI 即便用 session 鉴权也要求带 New-Api-User 头（"unauthorized, New-Api-User header not provided"）。
	resp, err := req.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, connector.HTTPStatusError(resp.StatusCode(), resp.Body())
	}
	return resp.Body(), nil
}

func (c *Client) quotaToUSD(quota float64, quotaPerUnit float64) float64 {
	return round4(quota / quotaPerUnit)
}

func newAPIRechargeFactor(ch *connector.Channel, price float64) (float64, string) {
	if ch.RechargeMultiplier != nil && *ch.RechargeMultiplier > 0 && !math.IsNaN(*ch.RechargeMultiplier) && !math.IsInf(*ch.RechargeMultiplier, 0) {
		return connector.RechargeCostFactor(ch.RechargeMultiplier, ch.RechargeMultiplierMode), "manual"
	}
	// Different NewAPI forks use status.price for exchange-rate display,
	// quota pricing, or site-specific model pricing. It is not evidence of the
	// amount actually paid for the credited balance.
	return 1, "default"
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}

func joinCookies(cookies []*http.Cookie) string {
	if len(cookies) == 0 {
		return ""
	}
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; ")
}

func parseAnnouncementTime(values ...string) *time.Time {
	for _, raw := range values {
		raw = strings.TrimSpace(raw)
		if raw == "" || raw == "0" {
			continue
		}
		if t, ok := parseFlexibleTime(raw); ok {
			return &t
		}
	}
	return nil
}

func parseFlexibleTime(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		switch {
		case n > 1e12:
			return time.UnixMilli(n), true
		case n > 1e9:
			return time.Unix(n, 0), true
		}
	}
	return time.Time{}, false
}

func newAPIAnnouncementSourceKey(id any, parts ...string) string {
	if id != nil {
		switch v := id.(type) {
		case string:
			if s := strings.TrimSpace(v); s != "" {
				return "id:" + s
			}
		case float64:
			if v != 0 {
				return "id:" + strconv.FormatInt(int64(v), 10)
			}
		default:
			s := strings.TrimSpace(fmt.Sprint(v))
			if s != "" && s != "<nil>" && s != "0" {
				return "id:" + s
			}
		}
	}
	return hashAnnouncementKey(parts...)
}

func hashAnnouncementKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(strings.TrimSpace(p)))
		h.Write([]byte{0})
	}
	return "hash:" + hex.EncodeToString(h.Sum(nil))
}

func dedupeAnnouncements(items []connector.AnnouncementResult) []connector.AnnouncementResult {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	out := make([]connector.AnnouncementResult, 0, len(items))
	for _, item := range items {
		key := strings.TrimSpace(item.SourceKey)
		if key == "" {
			key = hashAnnouncementKey(item.Title, item.Content, item.Type, item.Link)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		item.SourceKey = key
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].PublishedAt == nil || out[j].PublishedAt == nil {
			return i < j
		}
		return out[i].PublishedAt.After(*out[j].PublishedAt)
	})
	return out
}

func (c *Client) newAPIGroupMap(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (map[string]connector.APIKeyGroup, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/user/self/groups", session)
	if err != nil {
		return nil, fmt.Errorf("newapi api key groups: %w", err)
	}
	raw := map[string]struct {
		Ratio json.RawMessage `json:"ratio"`
		Desc  string          `json:"desc"`
	}{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("newapi api key groups decode: %w", err)
	}
	out := make(map[string]connector.APIKeyGroup, len(raw))
	for name, v := range raw {
		var ratio float64
		if err := json.Unmarshal(v.Ratio, &ratio); err != nil {
			continue
		}
		out[name] = connector.APIKeyGroup{
			Name:        name,
			Description: v.Desc,
			Ratio:       ratio,
		}
	}
	return out, nil
}

type newAPIToken struct {
	ID                 int     `json:"id"`
	Key                string  `json:"key"`
	Status             int     `json:"status"`
	Name               string  `json:"name"`
	CreatedTime        int64   `json:"created_time"`
	AccessedTime       int64   `json:"accessed_time"`
	ExpiredTime        *int64  `json:"expired_time"`
	RemainQuota        *int    `json:"remain_quota"`
	UsedQuota          int     `json:"used_quota"`
	UnlimitedQuota     *bool   `json:"unlimited_quota"`
	ModelLimitsEnabled *bool   `json:"model_limits_enabled"`
	ModelLimits        *string `json:"model_limits"`
	AllowIPs           *string `json:"allow_ips"`
	Group              string  `json:"group"`
	CrossGroupRetry    *bool   `json:"cross_group_retry"`

	observedIdentityFields connector.APIKeyIdentityFields
	nullIdentityFields     connector.APIKeyIdentityFields
	observedFields         connector.APIKeyObservedFields
	nullFields             connector.APIKeyObservedFields
}

func (t *newAPIToken) UnmarshalJSON(data []byte) error {
	type wireToken newAPIToken
	var decoded wireToken
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*t = newAPIToken(decoded)
	_, t.observedIdentityFields.Name = fields["name"]
	_, t.observedIdentityFields.Status = fields["status"]
	_, t.observedIdentityFields.Group = fields["group"]
	_, t.observedFields.Quota = fields["remain_quota"]
	_, t.observedFields.UnlimitedQuota = fields["unlimited_quota"]
	_, t.observedFields.ExpiredTime = fields["expired_time"]
	_, t.observedFields.AllowIPs = fields["allow_ips"]
	_, t.observedFields.ModelLimitsEnabled = fields["model_limits_enabled"]
	_, t.observedFields.ModelLimits = fields["model_limits"]
	_, t.observedFields.CrossGroupRetry = fields["cross_group_retry"]
	t.nullIdentityFields.Name = jsonFieldIsNull(fields, "name")
	t.nullIdentityFields.Status = jsonFieldIsNull(fields, "status")
	t.nullIdentityFields.Group = jsonFieldIsNull(fields, "group")
	t.nullFields.Quota = jsonFieldIsNull(fields, "remain_quota")
	t.nullFields.UnlimitedQuota = jsonFieldIsNull(fields, "unlimited_quota")
	t.nullFields.ExpiredTime = jsonFieldIsNull(fields, "expired_time")
	t.nullFields.AllowIPs = jsonFieldIsNull(fields, "allow_ips")
	t.nullFields.ModelLimitsEnabled = jsonFieldIsNull(fields, "model_limits_enabled")
	t.nullFields.ModelLimits = jsonFieldIsNull(fields, "model_limits")
	t.nullFields.CrossGroupRetry = jsonFieldIsNull(fields, "cross_group_retry")
	return nil
}

func jsonFieldIsNull(fields map[string]json.RawMessage, name string) bool {
	raw, ok := fields[name]
	return ok && strings.TrimSpace(string(raw)) == "null"
}

func (t newAPIToken) toConnector() connector.APIKey {
	var createdAt *time.Time
	if t.CreatedTime > 0 {
		v := time.Unix(t.CreatedTime, 0)
		createdAt = &v
	}
	var lastUsedAt *time.Time
	if t.AccessedTime > 0 {
		v := time.Unix(t.AccessedTime, 0)
		lastUsedAt = &v
	}
	allowIPs := ""
	if t.AllowIPs != nil {
		allowIPs = *t.AllowIPs
	}
	expiredTime := int64(0)
	if t.ExpiredTime != nil {
		expiredTime = *t.ExpiredTime
	}
	remainQuota := 0
	if t.RemainQuota != nil {
		remainQuota = *t.RemainQuota
	}
	unlimitedQuota := false
	if t.UnlimitedQuota != nil {
		unlimitedQuota = *t.UnlimitedQuota
	}
	modelLimitsEnabled := false
	if t.ModelLimitsEnabled != nil {
		modelLimitsEnabled = *t.ModelLimitsEnabled
	}
	modelLimits := ""
	if t.ModelLimits != nil {
		modelLimits = *t.ModelLimits
	}
	crossGroupRetry := false
	if t.CrossGroupRetry != nil {
		crossGroupRetry = *t.CrossGroupRetry
	}
	return connector.APIKey{
		ID:                     int64(t.ID),
		Key:                    t.Key,
		Name:                   t.Name,
		Status:                 newAPITokenStatusToString(t.Status),
		Group:                  t.Group,
		Quota:                  float64(remainQuota),
		QuotaUsed:              float64(t.UsedQuota),
		UnlimitedQuota:         unlimitedQuota,
		ExpiredTime:            expiredTime,
		CreatedAt:              createdAt,
		LastUsedAt:             lastUsedAt,
		AllowIPs:               allowIPs,
		ModelLimitsEnabled:     modelLimitsEnabled,
		ModelLimits:            modelLimits,
		CrossGroupRetry:        crossGroupRetry,
		ObservedIdentityFields: t.observedIdentityFields,
		NullIdentityFields:     t.nullIdentityFields,
		ObservedFields:         t.observedFields,
		NullFields:             t.nullFields,
		Source:                 connector.APIKeySourceNewAPI,
	}
}

func buildNewAPICreateToken(req connector.APIKeyCreateRequest) map[string]any {
	body := map[string]any{
		"name":                 strings.TrimSpace(req.Name),
		"expired_time":         valueOr(req.ExpiredTime, int64(-1)),
		"remain_quota":         valueOr(req.RemainQuota, 0),
		"unlimited_quota":      valueOr(req.UnlimitedQuota, false),
		"model_limits_enabled": valueOr(req.ModelLimitsEnabled, false),
		"model_limits":         req.ModelLimits,
		"allow_ips":            req.AllowIPs,
		"group":                req.Group,
		"cross_group_retry":    valueOr(req.CrossGroupRetry, false),
	}
	if strings.TrimSpace(req.CustomKey) != "" {
		body["custom_key"] = strings.TrimSpace(req.CustomKey)
	}
	return body
}

func buildNewAPIUpdateToken(id int64, req connector.APIKeyUpdateRequest) map[string]any {
	body := map[string]any{"id": int(id)}
	if req.Name != nil {
		body["name"] = strings.TrimSpace(*req.Name)
	}
	if req.Status != nil {
		body["status"] = newAPITokenStatusFromString(*req.Status)
	}
	if req.Group != nil {
		body["group"] = strings.TrimSpace(*req.Group)
	}
	if req.RemainQuota != nil {
		body["remain_quota"] = *req.RemainQuota
	}
	if req.UnlimitedQuota != nil {
		body["unlimited_quota"] = *req.UnlimitedQuota
	}
	if req.ExpiredTime != nil {
		body["expired_time"] = *req.ExpiredTime
	}
	if req.ModelLimitsEnabled != nil {
		body["model_limits_enabled"] = *req.ModelLimitsEnabled
	}
	if req.ModelLimits != nil {
		body["model_limits"] = *req.ModelLimits
	}
	if req.AllowIPs != nil {
		body["allow_ips"] = *req.AllowIPs
	}
	if req.CrossGroupRetry != nil {
		body["cross_group_retry"] = *req.CrossGroupRetry
	}
	return body
}

func decodeNewAPIWrite(body []byte, prefix string) error {
	_, err := decodeNewAPIWriteData(body, prefix)
	return err
}

func decodeNewAPIWriteData(body []byte, prefix string) (json.RawMessage, error) {
	var wrapped newapiResp
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, fmt.Errorf("%s decode: %w", prefix, err)
	}
	if !wrapped.Success {
		msg := strings.TrimSpace(wrapped.Message)
		if msg == "" {
			msg = newAPIStringFromRaw(wrapped.Data)
		}
		if msg == "" {
			msg = prefix + " failed"
		}
		return nil, errors.New(newAPISanitizeErrorText(msg))
	}
	return wrapped.Data, nil
}

var newAPISecretPattern = regexp.MustCompile(`(?i)(bearer\s+)?(sk-[a-z0-9._=-]{8,}|sk-upops-[a-z0-9._=-]{8,}|[a-z0-9_-]{32,})`)

func newAPISanitizeErrorText(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	text = newAPISecretPattern.ReplaceAllString(text, "[redacted]")
	for _, marker := range []string{"Authorization", "authorization", "x-api-key", "X-Api-Key", "X-API-Key"} {
		if strings.Contains(text, marker) {
			text = strings.ReplaceAll(text, marker, "[redacted-header]")
		}
	}
	if len(text) > 512 {
		text = text[:512]
	}
	return text
}

func newAPITokenStatusToString(status int) string {
	switch status {
	case 1:
		return "active"
	case 2:
		return "disabled"
	case 3:
		return "expired"
	case 4:
		return "quota_exhausted"
	default:
		return "unknown"
	}
}

func newAPITokenStatusFromString(status string) int {
	switch strings.TrimSpace(status) {
	case "active":
		return 1
	case "disabled":
		return 2
	case "expired":
		return 3
	case "quota_exhausted":
		return 4
	default:
		return 0
	}
}

func normalizeAPIKeyPage(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize
}

func pagesFromTotal(total int64, pageSize int) int {
	if pageSize <= 0 {
		pageSize = 20
	}
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	if pages < 1 {
		return 1
	}
	return pages
}

func valueOr[T any](ptr *T, fallback T) T {
	if ptr == nil {
		return fallback
	}
	return *ptr
}
