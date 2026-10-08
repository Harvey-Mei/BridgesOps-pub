// Package sub2api 实现 sub2api 风格上游站点的 connector，参考 docs/USER_BALANCE_GROUP_RATE_AUTH_API_CN-sub2api.md。
package sub2api

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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/providerid"
	"github.com/go-resty/resty/v2"
)

func init() {
	connector.Register(connector.TypeSub2API, func() connector.Connector { return New() })
}

type Client struct {
	http                 *resty.Client
	now                  func() time.Time
	discoveryTimeout     time.Duration
	verifiedMu           sync.RWMutex
	verifiedOrigins      map[string]json.RawMessage
	verificationFailures map[string]apiOriginVerificationFailure
}

func New() *Client {
	c := resty.New().
		SetTimeout(30*time.Second).
		SetHeader("User-Agent", "bridgeops/0.1").
		SetHeader("Accept", "application/json")
	direct, _ := netproxy.NewTransport(nil, "")
	c.GetClient().Transport = direct
	c.GetClient().CheckRedirect = sameOriginRedirectPolicy
	connector.SetHTTPClientTLSHandshakeTimeout(c.GetClient(), 30*time.Second)
	return &Client{
		http:                 c,
		now:                  time.Now,
		discoveryTimeout:     defaultDiscoveryTimeout,
		verifiedOrigins:      make(map[string]json.RawMessage),
		verificationFailures: make(map[string]apiOriginVerificationFailure),
	}
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

// sub2Resp sub2api 统一响应外壳：{ code, message, data }。code 0 = 成功。
type sub2Resp struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

const (
	sub2APIConcurrencySourceLogin  = "sub2api.auth.login.user.concurrency"
	sub2APIConcurrencySourceAuthMe = "sub2api.auth.me.concurrency"
)

// sub2APIAccountProfile extracts only the canonical account fields exposed by
// Sub2API. current_concurrency belongs to an individual API key and is
// intentionally ignored here because it is not a maximum concurrency limit.
func sub2APIAccountProfile(data json.RawMessage, nestedUser bool, source string, observedAt time.Time) (string, *connector.AccountConcurrencyFact) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return "", nil
	}
	if nestedUser {
		user, ok := fields["user"]
		if !ok || len(user) == 0 || strings.TrimSpace(string(user)) == "null" {
			return "", nil
		}
		if err := json.Unmarshal(user, &fields); err != nil {
			return "", nil
		}
	}

	accountID := sub2APIAccountID(fields["id"])
	if accountID == "" {
		return "", nil
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	} else {
		observedAt = observedAt.UTC()
	}
	fact := &connector.AccountConcurrencyFact{
		Provider:   connector.TypeSub2API,
		AccountID:  accountID,
		Scope:      connector.AccountConcurrencyScopeAccount,
		Source:     source,
		ObservedAt: observedAt,
	}
	rawLimit, present := fields["concurrency"]
	if !present || len(rawLimit) == 0 || strings.TrimSpace(string(rawLimit)) == "null" {
		return accountID, fact
	}
	var value int
	if err := json.Unmarshal(rawLimit, &value); err != nil {
		return accountID, fact
	}
	if value <= 0 {
		value = 0
	}
	limit := value
	fact.Limit = &limit
	return accountID, fact
}

func sub2APIAccountID(raw json.RawMessage) string {
	if len(raw) == 0 || strings.TrimSpace(string(raw)) == "null" {
		return ""
	}
	var numeric int64
	if err := json.Unmarshal(raw, &numeric); err == nil {
		if numeric <= 0 {
			return ""
		}
		return strconv.FormatInt(numeric, 10)
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return ""
	}
	text = strings.TrimSpace(text)
	if text == "" || text == "0" {
		return ""
	}
	return text
}

func (c *Client) GetTurnstileSiteKey(ctx context.Context, ch *connector.Channel) (string, error) {
	if err := c.ensureVerifiedAPIOrigin(ctx, ch); err != nil {
		return "", fmt.Errorf("sub2api public settings API 服务域名验证失败：%w", err)
	}
	body, cached := c.takeVerifiedAPISettings(ch.SiteURL)
	if !cached {
		var err error
		body, err = c.fetchPublicSettings(ctx, ch.SiteURL)
		if err != nil && ch.APIOriginKnown && isExplicitAPIOriginFailure(err) {
			if discoveryErr := c.rediscoverExplicitlyInvalidAPIOrigin(ctx, ch, err); discoveryErr != nil {
				return "", fmt.Errorf("sub2api public settings: %w", discoveryErr)
			}
			body, err = c.fetchPublicSettings(ctx, ch.SiteURL)
		}
		if err != nil {
			return "", withTransientDiscoveryRetry(ctx, fmt.Errorf("sub2api public settings: %w", withAPIOriginHint(err)))
		}
		c.markVerifiedAPIOrigin(ch.SiteURL, nil)
	}
	var settings struct {
		TurnstileEnabled bool   `json:"turnstile_enabled"`
		TurnstileSiteKey string `json:"turnstile_site_key"`
	}
	if err := json.Unmarshal(body, &settings); err != nil {
		return "", fmt.Errorf("sub2api public settings decode: %w", err)
	}
	if !settings.TurnstileEnabled {
		return "", nil
	}
	if strings.TrimSpace(settings.TurnstileSiteKey) == "" {
		return "", fmt.Errorf("%w: sub2api public settings", connector.ErrTurnstileSiteKeyMissing)
	}
	return strings.TrimSpace(settings.TurnstileSiteKey), nil
}

func (c *Client) Login(ctx context.Context, ch *connector.Channel) (*connector.AuthSession, error) {
	persistedOrigin := ch != nil && ch.APIOriginKnown
	if err := c.ensureVerifiedAPIOrigin(ctx, ch); err != nil {
		return nil, fmt.Errorf("sub2api login API 服务域名验证失败（可填写网页前端域名自动检测）：%w", err)
	}
	session, err := c.loginOnce(ctx, ch)
	if err == nil || !ch.APIOriginKnown || !isExplicitAPIOriginFailure(err) {
		return session, withKnownAPIOriginRetry(ctx, persistedOrigin, err)
	}
	if discoveryErr := c.rediscoverExplicitlyInvalidAPIOrigin(ctx, ch, err); discoveryErr != nil {
		return nil, fmt.Errorf("sub2api login API 地址恢复失败：%w", discoveryErr)
	}
	session, err = c.loginOnce(ctx, ch)
	return session, withKnownAPIOriginRetry(ctx, true, err)
}

func (c *Client) loginOnce(ctx context.Context, ch *connector.Channel) (*connector.AuthSession, error) {
	body := map[string]any{
		"email":    ch.Username,
		"password": ch.Password,
	}
	for k, v := range ch.LoginExtraParams {
		body[k] = v
	}
	if ch.TurnstileToken != "" {
		body["turnstile_token"] = ch.TurnstileToken
	}

	resp, err := c.http.R().SetContext(ctx).SetHeader("Content-Type", "application/json").SetBody(body).
		Post(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/auth/login")
	if err != nil {
		return nil, fmt.Errorf("sub2api login http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api login: %w", withAPIOriginHint(connector.HTTPStatusError(resp.StatusCode(), resp.Body())))
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("sub2api login decode: %w", &apiOriginResponseError{err: err, mismatch: true})
	}
	if wrapped.Code != 0 {
		return nil, fmt.Errorf("sub2api login: %s", wrapped.Message)
	}

	var data struct {
		Requires2FA  bool   `json:"requires_2fa"`
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(wrapped.Data, &data); err != nil {
		return nil, fmt.Errorf("sub2api login data: %w", err)
	}
	if data.Requires2FA {
		return nil, errors.New("sub2api account requires 2FA; please disable it for monitoring accounts")
	}
	if data.AccessToken == "" {
		return nil, errors.New("sub2api login: empty access_token")
	}

	expires := time.Now().Add(time.Duration(data.ExpiresIn) * time.Second)
	if data.ExpiresIn <= 0 {
		expires = time.Now().Add(time.Hour)
	}
	_, concurrency := sub2APIAccountProfile(
		wrapped.Data,
		true,
		sub2APIConcurrencySourceLogin,
		time.Now().UTC(),
	)
	return &connector.AuthSession{
		AccessToken:        data.AccessToken,
		RefreshToken:       data.RefreshToken,
		ExpiresAt:          expires,
		AccountConcurrency: concurrency,
	}, nil
}

func (c *Client) RefreshSession(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.AuthSession, error) {
	if session == nil || strings.TrimSpace(session.RefreshToken) == "" {
		return nil, errors.New("missing sub2api refresh_token")
	}
	persistedOrigin := ch != nil && ch.APIOriginKnown
	if err := c.ensureVerifiedAPIOrigin(ctx, ch); err != nil {
		return nil, fmt.Errorf("sub2api refresh token API 服务域名验证失败（可填写网页前端域名自动检测）：%w", err)
	}
	refreshed, err := c.refreshSessionOnce(ctx, ch, session)
	if err == nil || !ch.APIOriginKnown || !isExplicitAPIOriginFailure(err) {
		return refreshed, withKnownAPIOriginRetry(ctx, persistedOrigin, err)
	}
	if discoveryErr := c.rediscoverExplicitlyInvalidAPIOrigin(ctx, ch, err); discoveryErr != nil {
		return nil, fmt.Errorf("sub2api refresh token API 地址恢复失败：%w", discoveryErr)
	}
	refreshed, err = c.refreshSessionOnce(ctx, ch, session)
	return refreshed, withKnownAPIOriginRetry(ctx, true, err)
}

func (c *Client) refreshSessionOnce(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.AuthSession, error) {
	resp, err := c.http.R().SetContext(ctx).SetHeader("Content-Type", "application/json").
		SetBody(map[string]string{"refresh_token": strings.TrimSpace(session.RefreshToken)}).
		Post(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/auth/refresh")
	if err != nil {
		return nil, fmt.Errorf("sub2api refresh token http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api refresh token: %w", withAPIOriginHint(connector.HTTPStatusError(resp.StatusCode(), resp.Body())))
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("sub2api refresh token decode: %w", &apiOriginResponseError{err: err, mismatch: true})
	}
	if wrapped.Code != 0 {
		return nil, fmt.Errorf("sub2api refresh token: %s", wrapped.Message)
	}
	var data struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(wrapped.Data, &data); err != nil {
		return nil, fmt.Errorf("sub2api refresh token data: %w", err)
	}
	if strings.TrimSpace(data.AccessToken) == "" {
		return nil, errors.New("sub2api refresh token: empty access_token")
	}
	expires := time.Now().Add(time.Duration(data.ExpiresIn) * time.Second)
	if data.ExpiresIn <= 0 {
		expires = time.Now().Add(time.Hour)
	}
	refreshToken := strings.TrimSpace(data.RefreshToken)
	if refreshToken == "" {
		refreshToken = strings.TrimSpace(session.RefreshToken)
	}
	return &connector.AuthSession{
		AccessToken:        strings.TrimSpace(data.AccessToken),
		RefreshToken:       refreshToken,
		ExpiresAt:          expires,
		AccountConcurrency: session.AccountConcurrency,
	}, nil
}

func (c *Client) CheckAuth(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) error {
	if session == nil || session.AccessToken == "" {
		return errors.New("missing sub2api access_token")
	}
	persistedOrigin := ch != nil && ch.APIOriginKnown
	if err := c.ensureVerifiedAPIOrigin(ctx, ch); err != nil {
		return fmt.Errorf("sub2api auth API 服务域名验证失败（可填写网页前端域名自动检测）：%w", err)
	}
	err := c.checkAuthOnce(ctx, ch, session)
	if err == nil || !ch.APIOriginKnown || !isExplicitAPIOriginFailure(err) {
		return withKnownAPIOriginRetry(ctx, persistedOrigin, err)
	}
	if discoveryErr := c.rediscoverExplicitlyInvalidAPIOrigin(ctx, ch, err); discoveryErr != nil {
		return fmt.Errorf("sub2api auth API 地址恢复失败：%w", discoveryErr)
	}
	return withKnownAPIOriginRetry(ctx, true, c.checkAuthOnce(ctx, ch, session))
}

func withKnownAPIOriginRetry(ctx context.Context, known bool, err error) error {
	if !known || err == nil || errors.Is(err, context.Canceled) || (ctx != nil && errors.Is(ctx.Err(), context.Canceled)) {
		return err
	}
	if !isTransientDiscoveryFailure(err) {
		var networkErr interface{ Timeout() bool }
		if !errors.As(err, &networkErr) || !networkErr.Timeout() {
			return err
		}
	}
	return connector.WithRetryDelay(err, transientDiscoveryRetry)
}

func (c *Client) checkAuthOnce(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) error {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/auth/me", session)
	if err != nil {
		return withAPIOriginHint(err)
	}
	_, concurrency := sub2APIAccountProfile(
		body,
		false,
		sub2APIConcurrencySourceAuthMe,
		time.Now().UTC(),
	)
	if concurrency != nil {
		session.AccountConcurrency = concurrency
	}
	return nil
}

func withAPIOriginHint(err error) error {
	if err == nil {
		return nil
	}
	var httpErr *connector.HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%w；请确认站点地址填写的是 Sub2API API 服务域名，而不是网页前端域名", err)
	}
	return err
}

func (c *Client) GetBalance(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.BalanceResult, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/auth/me", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api me: %w", err)
	}
	var me struct {
		Balance float64 `json:"balance"`
	}
	if err := json.Unmarshal(body, &me); err != nil {
		return nil, fmt.Errorf("sub2api me decode: %w", err)
	}
	_, concurrency := sub2APIAccountProfile(
		body,
		false,
		sub2APIConcurrencySourceAuthMe,
		time.Now().UTC(),
	)
	if session != nil {
		if concurrency != nil {
			session.AccountConcurrency = concurrency
		}
	}
	multiplier, factorSource := c.rechargeMultiplier(ctx, ch, session)
	factor := connector.RechargeCostFactor(multiplier, ch.RechargeMultiplierMode)
	return &connector.BalanceResult{
		Balance:            connector.ApplyRechargeMultiplier(me.Balance, multiplier, ch.RechargeMultiplierMode),
		RawBalance:         me.Balance,
		AppliedFactor:      factor,
		FactorSource:       factorSource,
		SampledAt:          time.Now(),
		AccountConcurrency: concurrency,
	}, nil
}

func (c *Client) GetCosts(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.CostResult, error) {
	sampledAt := time.Now().UTC()
	if c != nil && c.now != nil {
		sampledAt = c.now().UTC()
	}
	site := strings.TrimRight(ch.SiteURL, "/")
	body, err := c.getJSON(ctx, site+"/api/v1/usage/dashboard/stats", session)
	dashboardUnavailable := err != nil && isUsageStatsUnsupported(err)
	if err != nil && !dashboardUnavailable {
		return nil, fmt.Errorf("sub2api dashboard stats: %w", err)
	}
	var stats struct {
		TodayActualCost float64 `json:"today_actual_cost"`
		TotalActualCost float64 `json:"total_actual_cost"`
	}
	if !dashboardUnavailable {
		if err := json.Unmarshal(body, &stats); err != nil {
			return nil, fmt.Errorf("sub2api dashboard stats decode: %w", err)
		}
	}

	todayActualCost := stats.TodayActualCost
	totalActualCost := stats.TotalActualCost
	timeZone := strings.TrimSpace(ch.TimeZone)
	if timeZone == "" {
		timeZone = "UTC"
	}
	location, err := time.LoadLocation(timeZone)
	if err != nil {
		return nil, fmt.Errorf("sub2api usage stats: invalid timezone %q: %w", timeZone, err)
	}
	businessDate := sampledAt.In(location).Format(time.DateOnly)
	params := url.Values{}
	params.Set("start_date", businessDate)
	params.Set("end_date", businessDate)
	params.Set("timezone", timeZone)
	todayBody, err := c.getJSON(ctx, site+"/api/v1/usage/stats?"+params.Encode(), session)
	if err != nil {
		if dashboardUnavailable || !isUsageStatsUnsupported(err) {
			return nil, fmt.Errorf("sub2api usage stats: %w", err)
		}
	} else {
		var todayStats struct {
			TotalActualCost *float64 `json:"total_actual_cost"`
		}
		if err := json.Unmarshal(todayBody, &todayStats); err != nil {
			return nil, fmt.Errorf("sub2api usage stats decode: %w", err)
		}
		if todayStats.TotalActualCost == nil {
			return nil, errors.New("sub2api usage stats decode: missing total_actual_cost")
		}
		todayActualCost = *todayStats.TotalActualCost
	}

	if dashboardUnavailable {
		// The daily endpoint has no lifetime counter. Use today's value as the
		// initial baseline; the monitor layer extends an existing cumulative
		// state with daily deltas when one is available.
		totalActualCost = todayActualCost
	}

	multiplier, factorSource := c.rechargeMultiplier(ctx, ch, session)
	factor := connector.RechargeCostFactor(multiplier, ch.RechargeMultiplierMode)
	return &connector.CostResult{
		TodayCost:                 connector.ApplyRechargeMultiplier(todayActualCost, multiplier, ch.RechargeMultiplierMode),
		TotalCost:                 connector.ApplyRechargeMultiplier(totalActualCost, multiplier, ch.RechargeMultiplierMode),
		RawTodayCost:              todayActualCost,
		RawTotalCost:              totalActualCost,
		AppliedFactor:             factor,
		FactorSource:              factorSource,
		CumulativeCostUnavailable: dashboardUnavailable,
		SampledAt:                 sampledAt,
	}, nil
}

func (c *Client) GetHistoricalCosts(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, startDay, endDay time.Time) (*connector.HistoricalCostResult, error) {
	timeZone := strings.TrimSpace(ch.TimeZone)
	if timeZone == "" {
		timeZone = "UTC"
	}
	location, err := time.LoadLocation(timeZone)
	if err != nil {
		return nil, fmt.Errorf("sub2api historical costs: invalid timezone %q: %w", timeZone, err)
	}
	startLocal := startDay.In(location)
	endLocal := endDay.In(location)
	startBoundary := time.Date(startLocal.Year(), startLocal.Month(), startLocal.Day(), 0, 0, 0, 0, location)
	endBoundary := time.Date(endLocal.Year(), endLocal.Month(), endLocal.Day(), 0, 0, 0, 0, location)
	if !startDay.Equal(startBoundary) || !endDay.Equal(endBoundary) || !endDay.After(startDay) {
		return nil, errors.New("sub2api historical costs: range must use increasing calendar-day boundaries")
	}

	params := url.Values{}
	params.Set("start_date", startLocal.Format(time.DateOnly))
	params.Set("end_date", endLocal.AddDate(0, 0, -1).Format(time.DateOnly))
	params.Set("timezone", timeZone)
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/usage/stats?"+params.Encode(), session)
	if err != nil {
		return nil, fmt.Errorf("sub2api historical costs: %w", err)
	}
	var stats struct {
		TotalActualCost *float64 `json:"total_actual_cost"`
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, fmt.Errorf("sub2api historical costs decode: %w", err)
	}
	if stats.TotalActualCost == nil {
		return nil, errors.New("sub2api historical costs decode: missing total_actual_cost")
	}
	if *stats.TotalActualCost < 0 || math.IsNaN(*stats.TotalActualCost) || math.IsInf(*stats.TotalActualCost, 0) {
		return nil, errors.New("sub2api historical costs decode: invalid total_actual_cost")
	}
	return &connector.HistoricalCostResult{RawCost: *stats.TotalActualCost}, nil
}

func isUsageStatsUnsupported(err error) bool {
	var httpErr *connector.HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	switch httpErr.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func (c *Client) rechargeMultiplier(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*float64, string) {
	multiplier, source, err := c.sampleRechargeMultiplier(ctx, ch, session)
	if err != nil {
		return nil, "default"
	}
	return multiplier, source
}

// GetProviderFactor performs exactly one lightweight factor read. Unlike the
// compatibility balance/cost paths, failure is explicit: a caller using this
// observation as a monetary-unit fence must never treat missing evidence as a
// provider assertion that the factor is one.
func (c *Client) GetProviderFactor(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.ProviderFactorObservation, error) {
	multiplier, source, err := c.sampleRechargeMultiplier(ctx, ch, session)
	if err != nil {
		return nil, err
	}
	factor := connector.RechargeCostFactor(multiplier, ch.RechargeMultiplierMode)
	if factor <= 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return nil, errors.New("sub2api provider factor is invalid")
	}
	return &connector.ProviderFactorObservation{
		AppliedFactor: factor,
		FactorSource:  source,
		SampledAt:     time.Now().UTC(),
	}, nil
}

func (c *Client) sampleRechargeMultiplier(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*float64, string, error) {
	if ch == nil {
		return nil, "", errors.New("sub2api channel is required")
	}
	if ch.RechargeMultiplier != nil && *ch.RechargeMultiplier > 0 && !math.IsNaN(*ch.RechargeMultiplier) && !math.IsInf(*ch.RechargeMultiplier, 0) {
		return ch.RechargeMultiplier, "manual", nil
	}
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/payment/checkout-info", session)
	if err != nil {
		return nil, "", fmt.Errorf("sub2api checkout factor: %w", err)
	}
	var raw struct {
		BalanceRechargeMultiplier float64 `json:"balance_recharge_multiplier"`
	}
	if err := json.Unmarshal(body, &raw); err != nil || raw.BalanceRechargeMultiplier <= 0 || math.IsNaN(raw.BalanceRechargeMultiplier) || math.IsInf(raw.BalanceRechargeMultiplier, 0) {
		return nil, "", errors.New("sub2api checkout factor is missing or invalid")
	}
	return &raw.BalanceRechargeMultiplier, "advertised", nil
}

func (c *Client) GetRates(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.RateSnapshotResult, error) {
	site := strings.TrimRight(ch.SiteURL, "/")

	availBody, err := c.getJSON(ctx, site+"/api/v1/groups/available", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api groups available: %w", err)
	}
	var groups []struct {
		ID             uint64  `json:"id"`
		Name           string  `json:"name"`
		Description    string  `json:"description"`
		RateMultiplier float64 `json:"rate_multiplier"`
	}
	if err := json.Unmarshal(availBody, &groups); err != nil {
		return nil, fmt.Errorf("sub2api groups available decode: %w", err)
	}

	overrides := map[string]float64{}
	ratesBody, err := c.getJSON(ctx, site+"/api/v1/groups/rates", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api group rates: %w", err)
	}
	if err := json.Unmarshal(ratesBody, &overrides); err != nil {
		return nil, fmt.Errorf("sub2api group rates decode: %w", err)
	}

	out := make([]connector.RateResult, 0, len(groups))
	for _, g := range groups {
		rate := g.RateMultiplier
		if v, ok := overrides[strconv.FormatUint(g.ID, 10)]; ok {
			rate = v
		}
		out = append(out, connector.RateResult{
			ModelName:   g.Name,
			Description: g.Description,
			Ratio:       rate,
		})
	}
	return &connector.RateSnapshotResult{Items: out, Complete: true}, nil
}

const (
	sub2APIModelPlazaMaxBytes         = 16 << 20
	sub2APIModelPriceSourceVersion    = "sub2api-model-plaza-v1"
	sub2APIModelPricePerTokenScale    = 1_000_000
	sub2APIModelPriceLongContextBasis = "whole_request"
)

// GetModelPrices reads Sub2API's billing-derived model plaza. Unlike
// official_pricing, pricing is generated by the provider's active billing
// resolver. Each result is bound to the exact upstream group ID and name.
func (c *Client) GetModelPrices(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
	if ch == nil || strings.TrimSpace(ch.SiteURL) == "" {
		return nil, errors.New("sub2api model pricing: channel is required")
	}
	if session == nil || strings.TrimSpace(session.AccessToken) == "" {
		return nil, errors.New("sub2api model pricing: authenticated session is required")
	}
	body, err := c.getBoundedJSONNoRedirect(
		ctx,
		strings.TrimRight(ch.SiteURL, "/")+"/api/v1/model-plaza",
		session,
		sub2APIModelPlazaMaxBytes,
	)
	if err != nil {
		var httpErr *connector.HTTPError
		if errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusNotFound || httpErr.StatusCode == http.StatusMethodNotAllowed || httpErr.StatusCode == http.StatusNotImplemented) {
			return nil, fmt.Errorf("%w: sub2api model plaza", connector.ErrUnsupported)
		}
		return nil, fmt.Errorf("sub2api model pricing: %w", err)
	}

	var payload sub2APIModelPlaza
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("sub2api model pricing decode: %w", err)
	}
	if payload.Groups == nil {
		return nil, errors.New("sub2api model pricing groups are missing")
	}
	items, err := normalizeSub2APIModelPrices(payload.Groups)
	if err != nil {
		return nil, err
	}
	// Hash the complete provider payload, including unsupported dynamic and
	// per-operation entries. Those entries are intentionally absent from Items
	// so a complete replacement retires any formerly static token price.
	digest := sha256.Sum256(body)
	return &connector.ModelPriceSnapshotResult{
		Items: items, Complete: true, SourceVersion: sub2APIModelPriceSourceVersion,
		SourceChecksum: hex.EncodeToString(digest[:]),
		ObservedAt:     c.now().UTC().Truncate(time.Microsecond),
	}, nil
}

type sub2APIModelPlaza struct {
	Groups []sub2APIModelPlazaGroup `json:"groups"`
}

type sub2APIModelPlazaGroup struct {
	ID                        int64                    `json:"id"`
	Name                      string                   `json:"name"`
	RateMultiplier            float64                  `json:"rate_multiplier"`
	UserRateMultiplier        *float64                 `json:"user_rate_multiplier"`
	PeakRateEnabled           bool                     `json:"peak_rate_enabled"`
	LongContextPricingEnabled bool                     `json:"long_context_pricing_enabled"`
	Models                    []sub2APIModelPlazaModel `json:"models"`
}

type sub2APIModelPlazaModel struct {
	Name             string                    `json:"name"`
	Pricing          *sub2APIModelPlazaPricing `json:"pricing"`
	LongContextBasis string                    `json:"long_context_basis"`
	TimePricing      json.RawMessage           `json:"time_pricing"`
}

type sub2APIModelPlazaPricing struct {
	BillingMode       string                           `json:"billing_mode"`
	InputPrice        *float64                         `json:"input_price"`
	OutputPrice       *float64                         `json:"output_price"`
	CacheWritePrice   *float64                         `json:"cache_write_price"`
	CacheWrite1hPrice *float64                         `json:"cache_write_1h_price"`
	CacheReadPrice    *float64                         `json:"cache_read_price"`
	Intervals         []sub2APIModelPlazaPriceInterval `json:"intervals"`
}

type sub2APIModelPlazaPriceInterval struct {
	MinTokens         int64    `json:"min_tokens"`
	MaxTokens         *int64   `json:"max_tokens"`
	InputPrice        *float64 `json:"input_price"`
	OutputPrice       *float64 `json:"output_price"`
	CacheWritePrice   *float64 `json:"cache_write_price"`
	CacheWrite1hPrice *float64 `json:"cache_write_1h_price"`
	CacheReadPrice    *float64 `json:"cache_read_price"`
}

func normalizeSub2APIModelPrices(groups []sub2APIModelPlazaGroup) ([]connector.ModelPriceResult, error) {
	items := make([]connector.ModelPriceResult, 0)
	seenGroups := make(map[int64]string, len(groups))
	seenGroupNames := make(map[string]int64, len(groups))
	for _, group := range groups {
		name := strings.TrimSpace(group.Name)
		if group.ID <= 0 || name == "" || strings.ContainsRune(name, '\x00') || !utf8.ValidString(name) || utf8.RuneCountInString(name) > 128 {
			return nil, errors.New("sub2api model pricing contains an invalid group identity")
		}
		if previous, duplicate := seenGroups[group.ID]; duplicate {
			return nil, fmt.Errorf("sub2api model pricing contains duplicate group id %d (%s, %s)", group.ID, previous, name)
		}
		seenGroups[group.ID] = name
		canonicalName := strings.ToLower(name)
		if previous, duplicate := seenGroupNames[canonicalName]; duplicate {
			return nil, fmt.Errorf("sub2api model pricing contains duplicate group name %q (%d, %d)", name, previous, group.ID)
		}
		seenGroupNames[canonicalName] = group.ID
		if group.PeakRateEnabled {
			continue
		}
		ratio := group.RateMultiplier
		if group.UserRateMultiplier != nil {
			ratio = *group.UserRateMultiplier
		}
		if !finiteNonNegativeSub2APIModelPrice(ratio) {
			return nil, fmt.Errorf("sub2api model pricing contains an invalid multiplier for group %d", group.ID)
		}
		seenModels := make(map[string]struct{}, len(group.Models))
		for _, model := range group.Models {
			modelName := strings.TrimSpace(model.Name)
			if modelName == "" || !utf8.ValidString(modelName) || strings.ContainsRune(modelName, '\x00') || utf8.RuneCountInString(modelName) > 256 {
				return nil, fmt.Errorf("sub2api model pricing contains an invalid model name in group %d", group.ID)
			}
			if _, duplicate := seenModels[modelName]; duplicate {
				return nil, fmt.Errorf("sub2api model pricing contains duplicate model %q in group %d", modelName, group.ID)
			}
			seenModels[modelName] = struct{}{}
			if model.Pricing == nil || strings.TrimSpace(model.Pricing.BillingMode) != "token" || hasSub2APITimePricing(model.TimePricing) {
				continue
			}
			item, ok, err := normalizeSub2APIModelPrice(group, model, ratio)
			if err != nil {
				return nil, err
			}
			if ok {
				items = append(items, item)
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].UpstreamGroupID != items[j].UpstreamGroupID {
			return items[i].UpstreamGroupID < items[j].UpstreamGroupID
		}
		return items[i].ModelName < items[j].ModelName
	})
	return items, nil
}

func normalizeSub2APIModelPrice(group sub2APIModelPlazaGroup, model sub2APIModelPlazaModel, ratio float64) (connector.ModelPriceResult, bool, error) {
	pricing := model.Pricing
	if pricing.InputPrice == nil || pricing.OutputPrice == nil {
		return connector.ModelPriceResult{}, false, nil
	}
	base := []*float64{pricing.InputPrice, pricing.OutputPrice, pricing.CacheReadPrice, pricing.CacheWritePrice, pricing.CacheWrite1hPrice}
	for _, value := range base {
		if value != nil && !finiteNonNegativeSub2APIModelPrice(*value) {
			return connector.ModelPriceResult{}, false, fmt.Errorf("sub2api model pricing contains an invalid value for model %q", strings.TrimSpace(model.Name))
		}
	}
	item := connector.ModelPriceResult{
		ModelName: strings.TrimSpace(model.Name), EnabledGroups: []string{strings.TrimSpace(group.Name)},
		UpstreamGroupID: group.ID, UpstreamGroupName: strings.TrimSpace(group.Name), GroupRatio: modelPriceFloatPointer(ratio),
		InputUnitPriceUSD:      *pricing.InputPrice * sub2APIModelPricePerTokenScale,
		OutputUnitPriceUSD:     *pricing.OutputPrice * sub2APIModelPricePerTokenScale,
		CacheReadUnitPriceUSD:  scaledSub2APIModelPrice(pricing.CacheReadPrice),
		CacheWriteUnitPriceUSD: scaledSub2APICacheWritePrice(pricing.CacheWritePrice, pricing.CacheWrite1hPrice),
	}
	if !validSub2APIConnectorModelPrice(item) {
		return connector.ModelPriceResult{}, false, fmt.Errorf("sub2api model pricing overflows for model %q", item.ModelName)
	}
	if len(pricing.Intervals) == 0 || !group.LongContextPricingEnabled {
		return item, true, nil
	}
	if strings.TrimSpace(model.LongContextBasis) != sub2APIModelPriceLongContextBasis || len(pricing.Intervals) != 2 {
		return connector.ModelPriceResult{}, false, nil
	}
	first, second := pricing.Intervals[0], pricing.Intervals[1]
	if first.MinTokens != 0 || first.MaxTokens == nil || *first.MaxTokens <= 0 || *first.MaxTokens >= math.MaxInt64 ||
		second.MinTokens != *first.MaxTokens || second.MaxTokens != nil ||
		first.InputPrice == nil || first.OutputPrice == nil || second.InputPrice == nil || second.OutputPrice == nil {
		return connector.ModelPriceResult{}, false, nil
	}
	for _, value := range []*float64{first.InputPrice, first.OutputPrice, first.CacheReadPrice, first.CacheWritePrice, first.CacheWrite1hPrice, second.InputPrice, second.OutputPrice, second.CacheReadPrice, second.CacheWritePrice, second.CacheWrite1hPrice} {
		if value != nil && !finiteNonNegativeSub2APIModelPrice(*value) {
			return connector.ModelPriceResult{}, false, fmt.Errorf("sub2api model pricing contains an invalid interval for model %q", item.ModelName)
		}
	}
	threshold := *first.MaxTokens
	item.InputUnitPriceUSD = *first.InputPrice * sub2APIModelPricePerTokenScale
	item.OutputUnitPriceUSD = *first.OutputPrice * sub2APIModelPricePerTokenScale
	item.CacheReadUnitPriceUSD = scaledSub2APIModelPrice(first.CacheReadPrice)
	item.CacheWriteUnitPriceUSD = scaledSub2APICacheWritePrice(first.CacheWritePrice, first.CacheWrite1hPrice)
	item.LongContextBasis = sub2APIModelPriceLongContextBasis
	item.LongContextThresholdTokens = &threshold
	item.LongContextInputUnitPriceUSD = scaledSub2APIModelPrice(second.InputPrice)
	item.LongContextOutputUnitPriceUSD = scaledSub2APIModelPrice(second.OutputPrice)
	item.LongContextCacheReadUnitPriceUSD = scaledSub2APIModelPrice(second.CacheReadPrice)
	item.LongContextCacheWriteUnitPriceUSD = scaledSub2APICacheWritePrice(second.CacheWritePrice, second.CacheWrite1hPrice)
	if !validSub2APIConnectorModelPrice(item) {
		return connector.ModelPriceResult{}, false, fmt.Errorf("sub2api model pricing interval overflows for model %q", item.ModelName)
	}
	return item, true, nil
}

func hasSub2APITimePricing(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) && !bytes.Equal(trimmed, []byte("{}"))
}

// A single write component cannot distinguish short and one-hour cache writes.
// Preserve unknown instead of silently applying the cheaper TTL to both.
func scaledSub2APICacheWritePrice(short, long *float64) *float64 {
	if long != nil && (short == nil || *short != *long) {
		return nil
	}
	return scaledSub2APIModelPrice(short)
}

func finiteNonNegativeSub2APIModelPrice(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func scaledSub2APIModelPrice(value *float64) *float64 {
	if value == nil {
		return nil
	}
	scaled := *value * sub2APIModelPricePerTokenScale
	return &scaled
}

func validSub2APIConnectorModelPrice(item connector.ModelPriceResult) bool {
	if !finiteNonNegativeSub2APIModelPrice(item.InputUnitPriceUSD) || !finiteNonNegativeSub2APIModelPrice(item.OutputUnitPriceUSD) {
		return false
	}
	for _, value := range []*float64{
		item.CacheReadUnitPriceUSD, item.CacheWriteUnitPriceUSD,
		item.LongContextInputUnitPriceUSD, item.LongContextOutputUnitPriceUSD,
		item.LongContextCacheReadUnitPriceUSD, item.LongContextCacheWriteUnitPriceUSD,
	} {
		if value != nil && !finiteNonNegativeSub2APIModelPrice(*value) {
			return false
		}
	}
	return true
}

func modelPriceFloatPointer(value float64) *float64 { return &value }

func (c *Client) getBoundedJSONNoRedirect(ctx context.Context, requestURL string, session *connector.AuthSession, maximumBytes int64) ([]byte, error) {
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
	if session != nil && strings.TrimSpace(session.AccessToken) != "" {
		request.Header.Set("Authorization", "Bearer "+session.AccessToken)
	}
	response, err := netproxy.DoWithoutRedirects(c.http.GetClient(), request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		errorBody, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, classifyAPIOriginHTTPError(response.StatusCode, errorBody)
	}
	if response.ContentLength > maximumBytes {
		return nil, errors.New("response exceeds its size limit")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maximumBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) == 0 || int64(len(raw)) > maximumBytes {
		return nil, errors.New("response is empty or exceeds its size limit")
	}
	if isHTMLDocumentResponse(raw, response.Header.Get("Content-Type")) {
		return nil, &apiOriginResponseError{err: errors.New("Sub2API API response is HTML instead of JSON"), mismatch: true}
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return nil, fmt.Errorf("decode: %w", &apiOriginResponseError{err: err})
	}
	if wrapped.Code != 0 {
		return nil, errors.New(wrapped.Message)
	}
	if len(bytes.TrimSpace(wrapped.Data)) == 0 {
		return nil, errors.New("response data is empty")
	}
	return wrapped.Data, nil
}

func (c *Client) GetAnnouncements(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.AnnouncementResult, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/announcements", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api announcements: %w", err)
	}
	var raw []struct {
		ID         int64  `json:"id"`
		Title      string `json:"title"`
		Content    string `json:"content"`
		NotifyMode string `json:"notify_mode"`
		StartsAt   string `json:"starts_at"`
		EndsAt     string `json:"ends_at"`
		CreatedAt  string `json:"created_at"`
		UpdatedAt  string `json:"updated_at"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api announcements decode: %w", err)
	}
	out := make([]connector.AnnouncementResult, 0, len(raw))
	for _, a := range raw {
		if a.ID == 0 && strings.TrimSpace(a.Content) == "" && strings.TrimSpace(a.Title) == "" {
			continue
		}
		publishedAt := parseAnnouncementTime(a.StartsAt, a.CreatedAt)
		updatedAt := parseAnnouncementTime(a.UpdatedAt)
		sourceKey := strconv.FormatInt(a.ID, 10)
		if a.ID == 0 {
			sourceKey = hashAnnouncementKey(a.Title, a.Content, a.CreatedAt, a.UpdatedAt)
		}
		out = append(out, connector.AnnouncementResult{
			SourceKey:       sourceKey,
			Title:           strings.TrimSpace(a.Title),
			Content:         strings.TrimSpace(a.Content),
			Type:            strings.TrimSpace(a.NotifyMode),
			PublishedAt:     publishedAt,
			SourceUpdatedAt: updatedAt,
		})
	}
	return out, nil
}

func (c *Client) RedeemCode(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, code string) (*connector.RedeemResult, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(map[string]string{"code": code}).
		Post(site + "/api/v1/redeem")
	if err != nil {
		return nil, fmt.Errorf("sub2api redeem http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api redeem: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("sub2api redeem decode: %w", err)
	}
	if wrapped.Code != 0 {
		return nil, fmt.Errorf("sub2api redeem: %s", wrapped.Message)
	}

	var raw struct {
		Message        string   `json:"message"`
		Type           string   `json:"type"`
		Value          float64  `json:"value"`
		NewBalance     *float64 `json:"new_balance"`
		NewConcurrency *int     `json:"new_concurrency"`
		GroupName      *string  `json:"group_name"`
		ValidityDays   *int     `json:"validity_days"`
		Group          *struct {
			Name string `json:"name"`
		} `json:"group"`
	}
	if err := json.Unmarshal(wrapped.Data, &raw); err != nil {
		return nil, fmt.Errorf("sub2api redeem data: %w", err)
	}

	res := &connector.RedeemResult{
		Message:        raw.Message,
		Type:           raw.Type,
		Value:          raw.Value,
		NewBalance:     raw.NewBalance,
		NewConcurrency: raw.NewConcurrency,
		GroupName:      raw.GroupName,
		ValidityDays:   raw.ValidityDays,
	}
	if res.GroupName == nil && raw.Group != nil && raw.Group.Name != "" {
		name := raw.Group.Name
		res.GroupName = &name
	}
	if res.Type == "" {
		if raw.Group != nil && raw.ValidityDays != nil {
			res.Type = "subscription"
		} else if raw.NewConcurrency != nil {
			res.Type = "concurrency"
		} else {
			res.Type = "balance"
		}
	}
	if res.Message == "" {
		res.Message = "兑换成功"
	}
	if res.Type == "balance" {
		multiplier, _ := c.rechargeMultiplier(ctx, ch, session)
		res.Value = connector.ApplyRechargeMultiplier(res.Value, multiplier, ch.RechargeMultiplierMode)
		if res.NewBalance != nil {
			balance := connector.ApplyRechargeMultiplier(*res.NewBalance, multiplier, ch.RechargeMultiplierMode)
			res.NewBalance = &balance
		}
	}
	return res, nil
}

func (c *Client) GetRechargeInfo(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.RechargeInfo, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/payment/checkout-info", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api checkout info: %w", err)
	}
	var raw struct {
		Methods           map[string]sub2MethodLimit `json:"methods"`
		GlobalMin         float64                    `json:"global_min"`
		GlobalMax         float64                    `json:"global_max"`
		HelpText          string                     `json:"help_text"`
		HelpImageURL      string                     `json:"help_image_url"`
		AlipayForceQRCode bool                       `json:"alipay_force_qrcode"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api checkout info decode: %w", err)
	}
	methods := make([]connector.RechargeMethod, 0, 2)
	addMethod := func(visibleType, name string) {
		best, ok := pickVisibleMethodLimit(raw.Methods, visibleType)
		if !ok || (best.Available != nil && !*best.Available) {
			return
		}
		methods = append(methods, connector.RechargeMethod{
			Type:      visibleType,
			Name:      name,
			MinAmount: best.SingleMin,
			MaxAmount: best.SingleMax,
		})
	}
	addMethod("alipay", "支付宝")
	addMethod("wxpay", "微信支付")
	if len(methods) == 0 {
		return nil, errors.New("上游未配置可用的支付宝/微信支付方式")
	}
	return &connector.RechargeInfo{
		AmountLabel:       "充值金额",
		AmountStep:        0.01,
		MinAmount:         raw.GlobalMin,
		MaxAmount:         raw.GlobalMax,
		PresetAmounts:     []float64{},
		HelpText:          raw.HelpText,
		HelpImageURL:      raw.HelpImageURL,
		AlipayForceQRCode: raw.AlipayForceQRCode,
		Methods:           methods,
	}, nil
}

func (c *Client) GetSubscriptionInfo(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.SubscriptionInfo, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/payment/checkout-info", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api checkout info: %w", err)
	}
	var raw struct {
		Methods map[string]sub2MethodLimit     `json:"methods"`
		Plans   []sub2SubscriptionCheckoutPlan `json:"plans"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api checkout info decode: %w", err)
	}

	methods := make([]connector.SubscriptionMethod, 0, 2)
	availableTypes := map[string]bool{}
	addMethod := func(visibleType, name string) {
		best, ok := pickVisibleMethodLimit(raw.Methods, visibleType)
		if !ok || (best.Available != nil && !*best.Available) {
			return
		}
		availableTypes[visibleType] = true
		methods = append(methods, connector.SubscriptionMethod{
			Type: visibleType,
			Name: name,
		})
	}
	addMethod("alipay", "支付宝")
	addMethod("wxpay", "微信支付")

	plans := make([]connector.SubscriptionPlan, 0, len(raw.Plans))
	for _, plan := range raw.Plans {
		if plan.ID <= 0 {
			continue
		}
		paymentMethods := make([]string, 0, len(methods))
		for _, method := range methods {
			if availableTypes[method.Type] {
				paymentMethods = append(paymentMethods, method.Type)
			}
		}
		plans = append(plans, connector.SubscriptionPlan{
			ID:              strconv.FormatInt(plan.ID, 10),
			Name:            strings.TrimSpace(plan.Name),
			Description:     strings.TrimSpace(plan.Description),
			Price:           plan.Price,
			Currency:        "CNY",
			Validity:        formatSub2SubscriptionValidity(plan.ValidityDays, plan.ValidityUnit),
			GroupName:       strings.TrimSpace(plan.GroupName),
			DailyLimitUSD:   plan.DailyLimitUSD,
			WeeklyLimitUSD:  plan.WeeklyLimitUSD,
			MonthlyLimitUSD: plan.MonthlyLimitUSD,
			Features:        safeStringSlice(plan.Features),
			PaymentMethods:  paymentMethods,
		})
	}
	return &connector.SubscriptionInfo{Plans: plans, Methods: methods}, nil
}

func (c *Client) CreateRecharge(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, req connector.RechargeRequest) (*connector.RechargeLaunch, error) {
	if req.PaymentMethod != "alipay" && req.PaymentMethod != "wxpay" {
		return nil, errors.New("sub2api 仅支持 alipay 或 wxpay")
	}
	if req.Amount <= 0 {
		return nil, errors.New("充值金额必须大于 0")
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(map[string]any{
			"amount":         round2(req.Amount),
			"payment_type":   req.PaymentMethod,
			"order_type":     "balance",
			"is_mobile":      req.IsMobile,
			"payment_source": "hosted_redirect",
		}).
		Post(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/payment/orders")
	if err != nil {
		return nil, fmt.Errorf("sub2api create recharge http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api create recharge: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	raw, err := decodeSub2OrderPaymentResult(resp.Body(), "sub2api create recharge")
	if err != nil {
		return nil, err
	}
	return raw.toLaunch(req.IsMobile)
}

func (c *Client) GetRechargeHistory(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.RechargeRecord, error) {
	endpoint := strings.TrimRight(ch.SiteURL, "/") + "/api/v1/payment/orders/my?page=1&page_size=100&status=completed&order_type=balance"
	body, err := c.getJSON(ctx, endpoint, session)
	if err != nil {
		var httpErr *connector.HTTPError
		if errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusNotFound || httpErr.StatusCode == http.StatusMethodNotAllowed || httpErr.StatusCode == http.StatusNotImplemented) {
			return nil, fmt.Errorf("%w: sub2api recharge history", connector.ErrUnsupported)
		}
		return nil, fmt.Errorf("sub2api recharge history: %w", err)
	}
	var page struct {
		Items []struct {
			ID          int64      `json:"id"`
			Amount      float64    `json:"amount"`
			PayAmount   *float64   `json:"pay_amount"`
			OutTradeNo  string     `json:"out_trade_no"`
			Status      string     `json:"status"`
			OrderType   string     `json:"order_type"`
			CompletedAt *time.Time `json:"completed_at"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, fmt.Errorf("sub2api recharge history decode: %w", err)
	}
	result := make([]connector.RechargeRecord, 0, len(page.Items))
	for _, item := range page.Items {
		orderType := strings.TrimSpace(item.OrderType)
		if !strings.EqualFold(strings.TrimSpace(item.Status), "completed") || (orderType != "" && !strings.EqualFold(orderType, "balance")) ||
			item.CompletedAt == nil || item.CompletedAt.IsZero() || item.Amount <= 0 ||
			math.IsNaN(item.Amount) || math.IsInf(item.Amount, 0) || (item.PayAmount != nil && (*item.PayAmount < 0 || math.IsNaN(*item.PayAmount) || math.IsInf(*item.PayAmount, 0))) {
			continue
		}
		sourceID := strings.TrimSpace(item.OutTradeNo)
		if sourceID == "" && item.ID > 0 {
			sourceID = strconv.FormatInt(item.ID, 10)
		}
		if sourceID == "" {
			continue
		}
		result = append(result, connector.RechargeRecord{
			SourceID: sourceID, PaidAmount: item.PayAmount,
			CreditedAmount: item.Amount, CompletedAt: item.CompletedAt.UTC(),
		})
	}
	return result, nil
}

func (c *Client) CreateSubscription(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, req connector.SubscriptionRequest) (*connector.SubscriptionLaunch, error) {
	planID, err := strconv.ParseInt(strings.TrimSpace(req.PlanID), 10, 64)
	if err != nil || planID <= 0 {
		return nil, errors.New("sub2api 订阅套餐 ID 无效")
	}
	method := strings.TrimSpace(req.PaymentMethod)
	if method != "alipay" && method != "wxpay" {
		return nil, errors.New("sub2api 订阅仅支持 alipay 或 wxpay")
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(map[string]any{
			"payment_type":   method,
			"order_type":     "subscription",
			"plan_id":        planID,
			"is_mobile":      req.IsMobile,
			"payment_source": "hosted_redirect",
		}).
		Post(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/payment/orders")
	if err != nil {
		return nil, fmt.Errorf("sub2api create subscription http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api create subscription: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	raw, err := decodeSub2OrderPaymentResult(resp.Body(), "sub2api create subscription")
	if err != nil {
		return nil, err
	}
	return raw.toLaunch(req.IsMobile)
}

func (c *Client) GetSubscriptionUsage(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.SubscriptionUsageInfo, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/subscriptions/progress", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api subscription progress: %w", err)
	}
	var raw []sub2SubscriptionProgressItem
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api subscription progress decode: %w", err)
	}
	items := make([]connector.SubscriptionUsage, 0, len(raw))
	for _, item := range raw {
		usage := item.toConnector()
		if usage.ID <= 0 {
			continue
		}
		items = append(items, usage)
	}
	return &connector.SubscriptionUsageInfo{Items: items}, nil
}

func (c *Client) ListAPIKeys(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	page, pageSize := normalizeAPIKeyPage(query.Page, query.PageSize)
	params := url.Values{}
	params.Set("page", strconv.Itoa(page))
	params.Set("page_size", strconv.Itoa(pageSize))
	params.Set("sort_by", "created_at")
	params.Set("sort_order", "desc")
	if search := strings.TrimSpace(query.Search); search != "" {
		params.Set("search", search)
	}
	if status := strings.TrimSpace(query.Status); status != "" {
		params.Set("status", status)
	}
	if groupID := strings.TrimSpace(query.GroupID); groupID != "" {
		params.Set("group_id", groupID)
	}
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/keys?"+params.Encode(), session)
	if err != nil {
		return nil, fmt.Errorf("sub2api api keys: %w", err)
	}
	var raw struct {
		Items    []sub2APIKey `json:"items"`
		Total    *int64       `json:"total"`
		Page     *int         `json:"page"`
		PageSize *int         `json:"page_size"`
		Pages    *int         `json:"pages"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api api keys decode: %w", err)
	}
	total, responsePage, responsePageSize, pages, err := normalizePaginationMetadata(
		page, pageSize, len(raw.Items), raw.Total, raw.Page, raw.PageSize, raw.Pages,
	)
	if err != nil {
		return nil, fmt.Errorf("sub2api api keys pagination: %w", err)
	}
	if raw.Total == nil && raw.Pages == nil && responsePageSize != pageSize {
		return nil, fmt.Errorf("sub2api api keys pagination: response page_size %d does not match requested page_size %d without total or pages", responsePageSize, pageSize)
	}
	items := make([]connector.APIKey, 0, len(raw.Items))
	needsGroupLookup := false
	for _, item := range raw.Items {
		key := item.toConnector()
		if key.GroupID != nil && strings.TrimSpace(key.GroupName) == "" {
			needsGroupLookup = true
		}
		items = append(items, key)
	}
	groups := map[int64]connector.APIKeyGroup{}
	if needsGroupLookup {
		groups, err = c.sub2APIGroupMap(ctx, ch, session)
		if err != nil {
			// Group identity is part of the complete key inventory. Returning keys
			// with an empty group would make a deleted group indistinguishable from a
			// temporary provider lookup failure and could drive unsafe reconciliation.
			if query.RequireCompleteGroupIdentity {
				return nil, fmt.Errorf("sub2api api key group lookup: %w", err)
			}
		}
	}
	for index := range items {
		key := &items[index]
		if key.GroupID != nil {
			if g, ok := groups[*key.GroupID]; ok {
				key.GroupName = g.Name
				key.GroupDescription = g.Description
				key.GroupRatio = g.Ratio
			}
			if needsGroupLookup && err == nil {
				key.GroupDirectoryComplete = true
			}
		}
		if query.RequireCompleteGroupIdentity && strings.TrimSpace(key.GroupName) == "" && strings.TrimSpace(key.Group) == "" && key.GroupID == nil {
			return nil, fmt.Errorf("sub2api api key %d group identity is incomplete", key.ID)
		}
	}
	return &connector.APIKeyPage{
		Items:    items,
		Total:    total,
		Page:     responsePage,
		PageSize: responsePageSize,
		Pages:    pages,
	}, nil
}

type optionalUsageInt64 struct {
	Value   int64
	Present bool
}

// Comparable presence-aware amounts keep missing billing evidence distinct
// from an explicit free request, including repeated pagination snapshots.
type optionalUsageAmount struct {
	Value   float64
	Present bool
}

func (v *optionalUsageAmount) UnmarshalJSON(data []byte) error {
	v.Value, v.Present = 0, false
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	if err := json.Unmarshal(data, &v.Value); err != nil {
		return err
	}
	v.Present = true
	return nil
}

func (v *optionalUsageInt64) UnmarshalJSON(data []byte) error {
	if strings.TrimSpace(string(data)) == "null" {
		*v = optionalUsageInt64{}
		return nil
	}
	var value int64
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*v = optionalUsageInt64{Value: value, Present: true}
	return nil
}

func (c *Client) LookupRequestCost(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, query connector.RequestCostQuery) (*connector.RequestCostResult, error) {
	keyID := query.APIKeyID
	if keyID <= 0 {
		return nil, errors.New("sub2api request cost: api key not found")
	}
	requestID, err := providerid.Normalize(query.RequestID)
	if err != nil {
		return nil, fmt.Errorf("sub2api request cost: %w", err)
	}
	if requestID != "" {
		requestID, err = providerid.Normalize(sub2APIUsageRequestID(requestID))
		if err != nil {
			return nil, fmt.Errorf("sub2api request cost: canonical request ID: %w", err)
		}
	}
	query.RequestID = requestID
	type usageItem struct {
		ID                  int64               `json:"id"`
		APIKeyID            int64               `json:"api_key_id"`
		Model               string              `json:"model"`
		InputTokens         int64               `json:"input_tokens"`
		OutputTokens        int64               `json:"output_tokens"`
		CacheReadTokens     optionalUsageInt64  `json:"cache_read_tokens"`
		CacheCreationTokens optionalUsageInt64  `json:"cache_creation_tokens"`
		ActualCost          optionalUsageAmount `json:"actual_cost"`
		RequestID           string              `json:"request_id"`
		CreatedAt           string              `json:"created_at"`
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
	type seenCandidate struct {
		candidate candidate
		exact     bool
	}
	// The usage endpoint accepts up to 1000 rows per page. A larger page
	// keeps high-volume keys from spending the whole lookup deadline walking
	// hundreds of small pages. The page cap remains a fail-closed guard for
	// deployments that return malformed pagination metadata.
	const pageSize = 1000
	const maxPages = 10
	hasRequestID := query.RequestID != ""
	hasLocalTokenMetrics := query.HasTokenMetrics || query.PromptTokens != 0 || query.CompletionTokens != 0
	exact := make([]candidate, 0, 1)
	fallback := make([]candidate, 0, 1)
	seenUsageIDs := make(map[string]seenCandidate, 1)
	complete := false
	hasTimeWindow := !query.StartedAt.IsZero() || !query.FinishedAt.IsZero()
	var lowerTimeBound, upperTimeBound time.Time
	if hasTimeWindow {
		lowerTimeBound, upperTimeBound = requestCostWindow(query.StartedAt, query.FinishedAt)
	}
	var previousCreatedAt time.Time
	var previousID int64
	observedPageSize := 0
	havePrevious := false
	seenItems := make(map[int64]usageItem, pageSize)
	var initialPageItems []usageItem
	var initialTotal *int64
	var initialPages *int
	var latestTotal *int64
	var latestPages *int
	var firstPageURL string
	for page := 1; page <= maxPages; page++ {
		pagePreviousCreatedAt := time.Time{}
		var pagePreviousID int64
		pageHasPrevious := false
		params := url.Values{}
		params.Set("page", strconv.Itoa(page))
		params.Set("page_size", strconv.Itoa(pageSize))
		params.Set("api_key_id", strconv.FormatInt(keyID, 10))
		params.Set("timezone", "UTC")
		// The handler's stable ordering is created_at DESC, id DESC.  Keeping
		// this explicit lets us prove when every remaining row is outside the
		// narrow request window instead of scanning the whole UTC day.
		params.Set("sort_by", "created_at")
		params.Set("sort_order", "desc")
		if hasRequestID {
			params.Set("request_id", sub2APIUsageRequestID(query.RequestID))
		}
		if hasTimeWindow {
			// Some Sub2API deployments accept request_id but do not apply it.
			// Keep the authoritative ID match while bounding those responses to
			// the request's UTC calendar dates instead of scanning the key's
			// entire usage history.
			params.Set("start_date", lowerTimeBound.UTC().Format(time.DateOnly))
			params.Set("end_date", upperTimeBound.UTC().Format(time.DateOnly))
		}
		requestURL := strings.TrimRight(ch.SiteURL, "/") + "/api/v1/usage?" + params.Encode()
		if page == 1 {
			firstPageURL = requestURL
		}
		body, err := c.getJSON(ctx, requestURL, session)
		if err != nil {
			return nil, fmt.Errorf("sub2api request cost: %w", err)
		}
		if !utf8.Valid(body) {
			return nil, errors.New("sub2api request cost: usage response is not valid UTF-8")
		}
		var raw usagePage
		if err := json.Unmarshal(body, &raw); err != nil {
			return nil, fmt.Errorf("sub2api request cost decode: %w", err)
		}
		if hasRequestID && (raw.Total == nil || raw.Page == nil || raw.PageSize == nil || raw.Pages == nil) {
			return nil, errors.New("sub2api request cost: incomplete pagination metadata")
		}
		_, _, responsePageSize, _, err := normalizePaginationMetadata(
			page, pageSize, len(raw.Items), raw.Total, raw.Page, raw.PageSize, raw.Pages,
		)
		if err != nil {
			return nil, fmt.Errorf("sub2api request cost: invalid pagination metadata: %w", err)
		}
		if responsePageSize > pageSize {
			return nil, errors.New("sub2api request cost: response page_size exceeds requested maximum")
		}
		if observedPageSize != 0 && responsePageSize != observedPageSize {
			return nil, errors.New("sub2api request cost: pagination page_size changed")
		}
		observedPageSize = responsePageSize
		if latestTotal != nil && raw.Total == nil {
			return nil, errors.New("sub2api request cost: pagination total disappeared during scan")
		}
		if latestPages != nil && raw.Pages == nil {
			return nil, errors.New("sub2api request cost: pagination pages disappeared during scan")
		}
		if raw.Total != nil {
			if latestTotal != nil && *raw.Total < *latestTotal {
				return nil, errors.New("sub2api request cost: pagination total moved backwards")
			}
			value := *raw.Total
			latestTotal = &value
		}
		if raw.Pages != nil {
			if latestPages != nil && *raw.Pages < *latestPages {
				return nil, errors.New("sub2api request cost: pagination pages moved backwards")
			}
			value := *raw.Pages
			latestPages = &value
		}
		if page == 1 {
			initialPageItems = append([]usageItem(nil), raw.Items...)
			if raw.Total != nil {
				value := *raw.Total
				initialTotal = &value
			}
			if raw.Pages != nil {
				value := *raw.Pages
				initialPages = &value
			}
		}

		crossedRequestWindow := false
		for _, item := range raw.Items {
			normalizedRequestID, requestIDErr := providerid.Normalize(item.RequestID)
			if requestIDErr != nil {
				return nil, fmt.Errorf("sub2api request cost: invalid usage provider request ID: %w", requestIDErr)
			}
			item.RequestID = normalizedRequestID
			createdAt, err := time.Parse(time.RFC3339Nano, item.CreatedAt)
			if err != nil {
				// We cannot establish the ordering boundary with an invalid
				// timestamp. Fail closed instead of treating this page as safe.
				return nil, errors.New("sub2api request cost: usage timestamp invalid")
			}
			if item.ID <= 0 {
				return nil, errors.New("sub2api request cost: usage record missing id")
			}
			if _, duplicate := seenItems[item.ID]; duplicate {
				return nil, errors.New("sub2api request cost: usage response contains duplicate usage id")
			}
			if pageHasPrevious && (createdAt.After(pagePreviousCreatedAt) ||
				(createdAt.Equal(pagePreviousCreatedAt) && item.ID >= pagePreviousID)) {
				return nil, errors.New("sub2api request cost: usage ordering invalid")
			}
			pagePreviousCreatedAt = createdAt
			pagePreviousID = item.ID
			pageHasPrevious = true
			if havePrevious && (createdAt.After(previousCreatedAt) ||
				(createdAt.Equal(previousCreatedAt) && item.ID >= previousID)) {
				return nil, errors.New("sub2api request cost: usage ordering invalid")
			}
			seenItems[item.ID] = item
			previousCreatedAt = createdAt
			previousID = item.ID
			havePrevious = true
			if crossedRequestWindow {
				continue
			}
			if !hasRequestID && hasTimeWindow && createdAt.Before(lowerTimeBound) {
				crossedRequestWindow = true
				continue
			}

			if item.APIKeyID != keyID {
				continue
			}
			exactRequestID := hasRequestID && requestIDsEqual(item.RequestID, query.RequestID)
			if hasRequestID && !exactRequestID {
				continue
			}
			if !exactRequestID {
				if !hasLocalTokenMetrics || !requestCostTimeMatches(createdAt, query.StartedAt, query.FinishedAt) {
					continue
				}
				if query.ModelName != "" && !strings.EqualFold(strings.TrimSpace(item.Model), strings.TrimSpace(query.ModelName)) {
					continue
				}
			}
			if item.InputTokens < 0 || item.OutputTokens < 0 || item.CacheReadTokens.Value < 0 || item.CacheCreationTokens.Value < 0 ||
				item.InputTokens > math.MaxInt64-item.CacheReadTokens.Value ||
				item.InputTokens+item.CacheReadTokens.Value > math.MaxInt64-item.CacheCreationTokens.Value {
				return nil, errors.New("sub2api request cost: usage token metrics invalid")
			}
			promptTokens := item.InputTokens + item.CacheReadTokens.Value + item.CacheCreationTokens.Value
			if promptTokens > math.MaxInt64-item.OutputTokens {
				return nil, errors.New("sub2api request cost: usage token total overflow")
			}
			if !exactRequestID && (promptTokens != query.PromptTokens || item.OutputTokens != query.CompletionTokens) {
				continue
			}
			if !item.ActualCost.Present {
				return nil, errors.New("sub2api request cost: actual cost evidence unavailable")
			}
			if item.ActualCost.Value < 0 || math.IsNaN(item.ActualCost.Value) || math.IsInf(item.ActualCost.Value, 0) {
				return nil, errors.New("sub2api request cost: usage cost invalid")
			}
			matched := candidate{
				usageID:              strconv.FormatInt(item.ID, 10),
				modelName:            strings.TrimSpace(item.Model),
				cost:                 item.ActualCost.Value,
				promptTokens:         promptTokens,
				completionTokens:     item.OutputTokens,
				totalTokens:          promptTokens + item.OutputTokens,
				hasTokenMetrics:      promptTokens > 0 || item.OutputTokens > 0,
				cacheCreationTokens:  item.CacheCreationTokens.Value,
				cacheReadTokens:      item.CacheReadTokens.Value,
				hasCacheTokenMetrics: item.CacheCreationTokens.Present && item.CacheReadTokens.Present,
			}
			usageID := matched.usageID
			if previous, ok := seenUsageIDs[usageID]; ok {
				if previous.candidate != matched || previous.exact != exactRequestID {
					return nil, errors.New("sub2api request cost: usage record identity conflict")
				}
				continue
			}
			seenUsageIDs[usageID] = seenCandidate{candidate: matched, exact: exactRequestID}
			if exactRequestID {
				exact = append(exact, matched)
				continue
			}
			if !hasRequestID {
				fallback = append(fallback, matched)
			}
		}
		if crossedRequestWindow ||
			(latestPages != nil && page >= *latestPages) ||
			(latestTotal != nil && int64(page)*int64(responsePageSize) >= *latestTotal) ||
			(!hasRequestID && latestTotal == nil && latestPages == nil && len(raw.Items) < responsePageSize) {
			complete = true
			break
		}
		// Offset pagination cannot be followed safely without a complete,
		// stable cursor description. The official endpoint supplies all four
		// fields; fail closed for partial third-party implementations.
		if raw.Total == nil || raw.Pages == nil || raw.Page == nil || raw.PageSize == nil {
			return nil, errors.New("sub2api request cost: incomplete pagination metadata")
		}
	}
	if !complete {
		return nil, errors.New("sub2api request cost: usage scan incomplete")
	}

	// Usage rows are committed asynchronously and keep the request's original
	// created_at. Re-read page one before assigning a bill so an insertion into an
	// already-scanned offset page cannot hide a second matching usage row.
	refreshBody, err := c.getJSON(ctx, firstPageURL, session)
	if err != nil {
		return nil, fmt.Errorf("sub2api request cost final snapshot: %w", err)
	}
	if !utf8.Valid(refreshBody) {
		return nil, errors.New("sub2api request cost: final snapshot usage response is not valid UTF-8")
	}
	var refreshed usagePage
	if err := json.Unmarshal(refreshBody, &refreshed); err != nil {
		return nil, fmt.Errorf("sub2api request cost final snapshot decode: %w", err)
	}
	if hasRequestID && (refreshed.Total == nil || refreshed.Page == nil || refreshed.PageSize == nil || refreshed.Pages == nil) {
		return nil, errors.New("sub2api request cost: final snapshot incomplete pagination metadata")
	}
	_, _, refreshedPageSize, _, err := normalizePaginationMetadata(
		1, pageSize, len(refreshed.Items), refreshed.Total, refreshed.Page, refreshed.PageSize, refreshed.Pages,
	)
	if err != nil {
		return nil, fmt.Errorf("sub2api request cost final snapshot pagination: %w", err)
	}
	if refreshedPageSize > pageSize {
		return nil, errors.New("sub2api request cost: final snapshot page_size exceeds requested maximum")
	}
	if refreshedPageSize != observedPageSize {
		return nil, errors.New("sub2api request cost: pagination page_size changed during final snapshot")
	}
	if latestTotal != nil {
		if refreshed.Total == nil {
			return nil, errors.New("sub2api request cost: pagination total disappeared during final snapshot")
		}
		if *refreshed.Total < *latestTotal {
			return nil, errors.New("sub2api request cost: pagination total moved backwards during final snapshot")
		}
	}
	if latestPages != nil {
		if refreshed.Pages == nil {
			return nil, errors.New("sub2api request cost: pagination pages disappeared during final snapshot")
		}
		if *refreshed.Pages < *latestPages {
			return nil, errors.New("sub2api request cost: pagination pages moved backwards during final snapshot")
		}
	}
	refreshedTimes := make([]time.Time, len(refreshed.Items))
	refreshedIDs := make(map[int64]struct{}, len(refreshed.Items))
	for index, item := range refreshed.Items {
		createdAt, err := time.Parse(time.RFC3339Nano, item.CreatedAt)
		if err != nil {
			return nil, errors.New("sub2api request cost: final snapshot usage timestamp invalid")
		}
		if item.ID <= 0 {
			return nil, errors.New("sub2api request cost: final snapshot usage record missing id")
		}
		if _, duplicate := refreshedIDs[item.ID]; duplicate {
			return nil, errors.New("sub2api request cost: final snapshot contains duplicate usage id")
		}
		refreshedIDs[item.ID] = struct{}{}
		if index > 0 && (createdAt.After(refreshedTimes[index-1]) ||
			(createdAt.Equal(refreshedTimes[index-1]) && item.ID >= refreshed.Items[index-1].ID)) {
			return nil, errors.New("sub2api request cost: final snapshot usage ordering invalid")
		}
		refreshedTimes[index] = createdAt
	}
	if (initialTotal == nil) != (refreshed.Total == nil) {
		return nil, errors.New("sub2api request cost: final snapshot total metadata changed")
	}
	if hasRequestID {
		if initialTotal == nil || initialPages == nil ||
			*refreshed.Total != *initialTotal || *refreshed.Pages != *initialPages ||
			len(initialPageItems) != len(refreshed.Items) {
			return nil, errors.New("sub2api request cost: exact pagination snapshot changed")
		}
		for index := range initialPageItems {
			if initialPageItems[index] != refreshed.Items[index] {
				return nil, errors.New("sub2api request cost: exact pagination snapshot changed")
			}
		}
	} else if initialTotal == nil {
		if (initialPages == nil) != (refreshed.Pages == nil) ||
			(initialPages != nil && *initialPages != *refreshed.Pages) ||
			len(initialPageItems) != len(refreshed.Items) {
			return nil, errors.New("sub2api request cost: final snapshot changed without a stable total")
		}
		for index := range initialPageItems {
			if initialPageItems[index] != refreshed.Items[index] {
				return nil, errors.New("sub2api request cost: final snapshot changed without a stable total")
			}
		}
	} else {
		if *refreshed.Total < *initialTotal {
			return nil, errors.New("sub2api request cost: final snapshot total moved backwards")
		}
		growth := *refreshed.Total - *initialTotal
		prefixLength := 0
		if len(initialPageItems) == 0 {
			prefixLength = len(refreshed.Items)
		} else {
			anchorID := initialPageItems[0].ID
			prefixLength = -1
			for index, item := range refreshed.Items {
				if item.ID == anchorID {
					prefixLength = index
					break
				}
			}
			if prefixLength < 0 {
				return nil, errors.New("sub2api request cost: final snapshot displaced first-page anchor")
			}
		}
		if int64(prefixLength) != growth {
			return nil, errors.New("sub2api request cost: final snapshot growth was not confined to the newest prefix")
		}
		if prefixLength > 0 && !hasTimeWindow {
			return nil, errors.New("sub2api request cost: final snapshot grew without a verifiable time boundary")
		}
		for index := 0; index < prefixLength; index++ {
			if !refreshedTimes[index].After(upperTimeBound) {
				return nil, errors.New("sub2api request cost: final snapshot growth overlaps request window")
			}
			if _, existed := seenItems[refreshed.Items[index].ID]; existed {
				return nil, errors.New("sub2api request cost: final snapshot prefix reused an existing usage id")
			}
		}
		expectedPageLength := len(initialPageItems) + prefixLength
		if expectedPageLength > refreshedPageSize {
			expectedPageLength = refreshedPageSize
		}
		if len(refreshed.Items) != expectedPageLength {
			return nil, errors.New("sub2api request cost: final snapshot changed first-page length unexpectedly")
		}
		if len(refreshed.Items)-prefixLength > len(initialPageItems) {
			return nil, errors.New("sub2api request cost: final snapshot inserted rows after the first-page anchor")
		}
		for index := prefixLength; index < len(refreshed.Items); index++ {
			if refreshed.Items[index] != initialPageItems[index-prefixLength] {
				return nil, errors.New("sub2api request cost: final snapshot inserted or changed rows after the first-page anchor")
			}
		}
	}

	if len(exact) == 1 {
		matched := exact[0]
		return &connector.RequestCostResult{
			APIKeyID:             keyID,
			UsageID:              matched.usageID,
			ModelName:            matched.modelName,
			Cost:                 matched.cost,
			Source:               "sub2api_actual_cost",
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
		return nil, errors.New("sub2api request cost: multiple usage records matched")
	}
	if hasRequestID {
		return nil, errors.New("sub2api request cost: usage record not found")
	}
	if len(fallback) == 1 {
		matched := fallback[0]
		return &connector.RequestCostResult{
			APIKeyID:             keyID,
			UsageID:              matched.usageID,
			ModelName:            matched.modelName,
			Cost:                 matched.cost,
			Source:               "sub2api_actual_cost",
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
		return nil, errors.New("sub2api request cost: multiple usage records matched")
	}
	return nil, errors.New("sub2api request cost: usage record not found")
}

func requestCostTimeMatches(createdAt, startedAt, finishedAt time.Time) bool {
	if startedAt.IsZero() && finishedAt.IsZero() {
		return true
	}
	lower, upper := requestCostWindow(startedAt, finishedAt)
	return !createdAt.Before(lower) && !createdAt.After(upper)
}

func requestCostWindow(startedAt, finishedAt time.Time) (time.Time, time.Time) {
	if finishedAt.IsZero() {
		finishedAt = startedAt
	}
	if startedAt.IsZero() {
		startedAt = finishedAt
	}
	return startedAt.Add(-10 * time.Second), finishedAt.Add(10 * time.Second)
}

func requestIDsEqual(left, right string) bool {
	left = sub2APIUsageRequestID(left)
	right = sub2APIUsageRequestID(right)
	return left != "" && left == right
}

func sub2APIUsageRequestID(value string) string {
	if value == "" || strings.HasPrefix(value, "client:") {
		return value
	}
	return "client:" + value
}

func (c *Client) ListAPIKeyGroups(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.APIKeyGroup, error) {
	groups, err := c.sub2APIGroupMap(ctx, ch, session)
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
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(buildSub2CreateAPIKey(req)).
		Post(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/keys")
	if err != nil {
		return nil, fmt.Errorf("sub2api create api key http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api create api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	data, err := decodeSub2WriteData(resp.Body(), "sub2api create api key")
	if err != nil {
		return nil, err
	}
	var raw sub2APIKey
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("sub2api create api key data: %w", err)
	}
	out := raw.toConnector()
	if out.ID <= 0 && strings.TrimSpace(out.Key) == "" {
		return nil, errors.New("sub2api create api key: response missing key id and actual key")
	}
	return &out, nil
}

func (c *Client) UpdateAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	if id <= 0 {
		return nil, errors.New("密钥 ID 无效")
	}
	body, err := buildSub2UpdateAPIKey(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(body).
		Put(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/keys/" + strconv.FormatInt(id, 10))
	if err != nil {
		return nil, fmt.Errorf("sub2api update api key http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api update api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	data, err := decodeSub2WriteData(resp.Body(), "sub2api update api key")
	if err != nil {
		return nil, err
	}
	var raw sub2APIKey
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("sub2api update api key data: %w", err)
	}
	out := raw.toConnector()
	return &out, nil
}

func (c *Client) DeleteAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64) error {
	if id <= 0 {
		return errors.New("密钥 ID 无效")
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		Delete(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/keys/" + strconv.FormatInt(id, 10))
	if err != nil {
		return fmt.Errorf("sub2api delete api key http: %w", err)
	}
	if resp.IsError() {
		return fmt.Errorf("sub2api delete api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	return decodeSub2Write(resp.Body(), "sub2api delete api key")
}

func (c *Client) RevealAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64) (string, error) {
	if id <= 0 {
		return "", errors.New("密钥 ID 无效")
	}
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/keys/"+strconv.FormatInt(id, 10), session)
	if err != nil {
		return "", fmt.Errorf("sub2api reveal api key: %w", err)
	}
	var raw sub2APIKey
	if err := json.Unmarshal(body, &raw); err != nil {
		return "", fmt.Errorf("sub2api reveal api key data: %w", err)
	}
	if strings.TrimSpace(raw.Key) == "" {
		return "", errors.New("sub2api 未返回完整密钥")
	}
	return raw.Key, nil
}

func (c *Client) getJSON(ctx context.Context, url string, session *connector.AuthSession) ([]byte, error) {
	req := c.http.R().SetContext(ctx)
	if session != nil && session.AccessToken != "" {
		req.SetHeader("Authorization", "Bearer "+session.AccessToken)
	}
	resp, err := req.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, classifyAPIOriginHTTPError(resp.StatusCode(), resp.Body())
	}
	if isHTMLDocumentResponse(resp.Body(), resp.Header().Get("Content-Type")) {
		return nil, &apiOriginResponseError{err: errors.New("Sub2API API 响应为网页 HTML 而非 JSON；请确认渠道类型和 API 地址（不要填写网页前端地址）"), mismatch: true}
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("decode: %w", &apiOriginResponseError{err: err})
	}
	if wrapped.Code != 0 {
		return nil, errors.New(wrapped.Message)
	}
	return wrapped.Data, nil
}

func classifyAPIOriginHTTPError(status int, body []byte) error {
	err := connector.HTTPStatusError(status, body)
	switch status {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone:
		return &apiOriginResponseError{err: err, mismatch: true}
	default:
		return err
	}
}

func isHTMLDocumentResponse(body []byte, contentType string) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false
	}
	mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0]))
	lower := bytes.ToLower(trimmed)
	for _, prefix := range [][]byte{[]byte("<!doctype html"), []byte("<html"), []byte("<head"), []byte("<body")} {
		if bytes.HasPrefix(lower, prefix) {
			return true
		}
	}
	// A few deployments send valid JSON with an incorrect text/html content
	// type. Preserve that compatibility by requiring a non-JSON body before
	// trusting the header as evidence of a frontend document.
	if (mediaType == "text/html" || mediaType == "application/xhtml+xml") && trimmed[0] != '{' && trimmed[0] != '[' {
		return true
	}
	return false
}

type sub2MethodLimit struct {
	PaymentType string  `json:"payment_type"`
	Currency    string  `json:"currency"`
	FeeRate     float64 `json:"fee_rate"`
	DailyLimit  float64 `json:"daily_limit"`
	SingleMin   float64 `json:"single_min"`
	SingleMax   float64 `json:"single_max"`
	Available   *bool   `json:"available"`
}

type sub2SubscriptionCheckoutPlan struct {
	ID              int64    `json:"id"`
	GroupID         int64    `json:"group_id"`
	GroupName       string   `json:"group_name"`
	Name            string   `json:"name"`
	Description     string   `json:"description"`
	Price           float64  `json:"price"`
	OriginalPrice   *float64 `json:"original_price"`
	DailyLimitUSD   *float64 `json:"daily_limit_usd"`
	WeeklyLimitUSD  *float64 `json:"weekly_limit_usd"`
	MonthlyLimitUSD *float64 `json:"monthly_limit_usd"`
	ValidityDays    int      `json:"validity_days"`
	ValidityUnit    string   `json:"validity_unit"`
	Features        []string `json:"features"`
}

type sub2OrderPaymentResult struct {
	PayURL       string          `json:"pay_url"`
	QRCode       string          `json:"qr_code"`
	ExpiresAt    string          `json:"expires_at"`
	ResultType   string          `json:"result_type"`
	PaymentMode  string          `json:"payment_mode"`
	PaymentEnv   string          `json:"payment_env"`
	ClientSecret string          `json:"client_secret"`
	IntentID     string          `json:"intent_id"`
	OAuth        json.RawMessage `json:"oauth"`
	JSAPI        json.RawMessage `json:"jsapi"`
	JSAPIPayload json.RawMessage `json:"jsapi_payload"`
}

type sub2SubscriptionProgressItem struct {
	Subscription sub2UserSubscription   `json:"subscription"`
	Progress     sub2Progress           `json:"progress"`
	ID           int64                  `json:"id"`
	GroupID      int64                  `json:"group_id"`
	Status       string                 `json:"status"`
	StartsAt     *time.Time             `json:"starts_at"`
	ExpiresAt    *time.Time             `json:"expires_at"`
	DailyStart   *time.Time             `json:"daily_window_start"`
	WeeklyStart  *time.Time             `json:"weekly_window_start"`
	MonthlyStart *time.Time             `json:"monthly_window_start"`
	DailyUsage   float64                `json:"daily_usage_usd"`
	WeeklyUsage  float64                `json:"weekly_usage_usd"`
	MonthlyUsage float64                `json:"monthly_usage_usd"`
	Group        *sub2SubscriptionGroup `json:"group"`
}

type sub2UserSubscription struct {
	ID              int64                  `json:"id"`
	GroupID         int64                  `json:"group_id"`
	Status          string                 `json:"status"`
	StartsAt        *time.Time             `json:"starts_at"`
	ExpiresAt       *time.Time             `json:"expires_at"`
	DailyStart      *time.Time             `json:"daily_window_start"`
	WeeklyStart     *time.Time             `json:"weekly_window_start"`
	MonthlyStart    *time.Time             `json:"monthly_window_start"`
	DailyUsageUSD   float64                `json:"daily_usage_usd"`
	WeeklyUsageUSD  float64                `json:"weekly_usage_usd"`
	MonthlyUsageUSD float64                `json:"monthly_usage_usd"`
	Group           *sub2SubscriptionGroup `json:"group"`
}

type sub2SubscriptionGroup struct {
	ID              int64   `json:"id"`
	Name            string  `json:"name"`
	DailyLimitUSD   float64 `json:"daily_limit_usd"`
	WeeklyLimitUSD  float64 `json:"weekly_limit_usd"`
	MonthlyLimitUSD float64 `json:"monthly_limit_usd"`
}

type sub2Progress struct {
	ID            int64            `json:"id"`
	GroupName     string           `json:"group_name"`
	ExpiresAt     *time.Time       `json:"expires_at"`
	ExpiresInDays int              `json:"expires_in_days"`
	Daily         *sub2UsageWindow `json:"daily"`
	Weekly        *sub2UsageWindow `json:"weekly"`
	Monthly       *sub2UsageWindow `json:"monthly"`
}

type sub2UsageWindow struct {
	LimitUSD        float64    `json:"limit_usd"`
	UsedUSD         float64    `json:"used_usd"`
	RemainingUSD    float64    `json:"remaining_usd"`
	Percentage      float64    `json:"percentage"`
	WindowStart     *time.Time `json:"window_start"`
	ResetsAt        *time.Time `json:"resets_at"`
	ResetsInSeconds int64      `json:"resets_in_seconds"`
}

func pickVisibleMethodLimit(methods map[string]sub2MethodLimit, visibleType string) (sub2MethodLimit, bool) {
	var first sub2MethodLimit
	found := false
	for _, key := range []string{visibleType, visibleType + "_direct"} {
		v, ok := methods[key]
		if !ok {
			continue
		}
		if !found {
			first = v
			found = true
		}
		if v.Available == nil || *v.Available {
			return v, true
		}
	}
	return first, found
}

func decodeSub2OrderPaymentResult(body []byte, prefix string) (*sub2OrderPaymentResult, error) {
	data, err := decodeSub2WriteData(body, prefix)
	if err != nil {
		return nil, err
	}
	var raw sub2OrderPaymentResult
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%s data: %w", prefix, err)
	}
	if raw.ResultType == "oauth_required" || raw.ResultType == "jsapi_ready" || strings.TrimSpace(raw.ClientSecret) != "" || hasJSONValue(raw.OAuth) || hasJSONValue(raw.JSAPI) || hasJSONValue(raw.JSAPIPayload) {
		return nil, errors.New("当前支付方式需要复杂支付流程，暂不支持")
	}
	return &raw, nil
}

func (r sub2OrderPaymentResult) toLaunch(isMobile bool) (*connector.RechargeLaunch, error) {
	var expiresAt *time.Time
	if r.ExpiresAt != "" {
		if parsed, err := time.Parse(time.RFC3339, r.ExpiresAt); err == nil {
			expiresAt = &parsed
		}
	}
	payURL := strings.TrimSpace(r.PayURL)
	qrCode := strings.TrimSpace(r.QRCode)
	switch strings.ToLower(strings.TrimSpace(r.PaymentMode)) {
	case "redirect", "popup":
		if payURL != "" {
			return &connector.RechargeLaunch{
				Mode:      "redirect",
				PayURL:    payURL,
				ExpiresAt: expiresAt,
			}, nil
		}
	case "qrcode", "native":
		if qrCode != "" {
			return &connector.RechargeLaunch{
				Mode:      "qrcode",
				QRCode:    qrCode,
				ExpiresAt: expiresAt,
			}, nil
		}
	}
	if qrCode != "" && !isMobile {
		return &connector.RechargeLaunch{
			Mode:      "qrcode",
			QRCode:    qrCode,
			ExpiresAt: expiresAt,
		}, nil
	}
	if payURL != "" {
		return &connector.RechargeLaunch{
			Mode:      "redirect",
			PayURL:    payURL,
			ExpiresAt: expiresAt,
		}, nil
	}
	if qrCode != "" {
		return &connector.RechargeLaunch{
			Mode:      "qrcode",
			QRCode:    qrCode,
			ExpiresAt: expiresAt,
		}, nil
	}
	return nil, errors.New("sub2api 未返回可用的支付二维码或跳转地址")
}

func (p sub2SubscriptionProgressItem) toConnector() connector.SubscriptionUsage {
	if p.Subscription.ID == 0 && p.ID > 0 {
		return p.directSubscriptionToConnector()
	}
	sub := p.Subscription
	progress := p.Progress
	groupID := sub.GroupID
	if groupID == 0 && sub.Group != nil {
		groupID = sub.Group.ID
	}
	groupName := strings.TrimSpace(progress.GroupName)
	if groupName == "" && sub.Group != nil {
		groupName = strings.TrimSpace(sub.Group.Name)
	}
	expiresAt := progress.ExpiresAt
	if expiresAt == nil {
		expiresAt = sub.ExpiresAt
	}
	daily := sub2WindowToConnector(progress.Daily)
	weekly := sub2WindowToConnector(progress.Weekly)
	monthly := sub2WindowToConnector(progress.Monthly)
	if sub.Group != nil {
		if daily == nil {
			daily = directSubscriptionWindow(sub.Group.DailyLimitUSD, sub.DailyUsageUSD, sub.DailyStart, nil)
		}
		if weekly == nil {
			weekly = directSubscriptionWindow(sub.Group.WeeklyLimitUSD, sub.WeeklyUsageUSD, sub.WeeklyStart, nil)
		}
		if monthly == nil {
			monthly = directSubscriptionWindow(sub.Group.MonthlyLimitUSD, sub.MonthlyUsageUSD, sub.MonthlyStart, nil)
		}
	}
	return connector.SubscriptionUsage{
		ID:            firstPositiveInt64(progress.ID, sub.ID),
		GroupID:       groupID,
		GroupName:     groupName,
		Status:        strings.TrimSpace(sub.Status),
		StartsAt:      sub.StartsAt,
		ExpiresAt:     expiresAt,
		ExpiresInDays: progress.ExpiresInDays,
		Daily:         daily,
		Weekly:        weekly,
		Monthly:       monthly,
	}
}

func (p sub2SubscriptionProgressItem) directSubscriptionToConnector() connector.SubscriptionUsage {
	groupID := p.GroupID
	groupName := ""
	if p.Group != nil {
		if groupID == 0 {
			groupID = p.Group.ID
		}
		groupName = strings.TrimSpace(p.Group.Name)
	}
	var daily, weekly, monthly *connector.SubscriptionUsageWindow
	if p.Group != nil {
		daily = directSubscriptionWindow(p.Group.DailyLimitUSD, p.DailyUsage, p.DailyStart, nil)
		weekly = directSubscriptionWindow(p.Group.WeeklyLimitUSD, p.WeeklyUsage, p.WeeklyStart, nil)
		monthly = directSubscriptionWindow(p.Group.MonthlyLimitUSD, p.MonthlyUsage, p.MonthlyStart, nil)
	}
	return connector.SubscriptionUsage{
		ID:        p.ID,
		GroupID:   groupID,
		GroupName: groupName,
		Status:    strings.TrimSpace(p.Status),
		StartsAt:  p.StartsAt,
		ExpiresAt: p.ExpiresAt,
		Daily:     daily,
		Weekly:    weekly,
		Monthly:   monthly,
	}
}

func directSubscriptionWindow(limit, used float64, windowStart, resetsAt *time.Time) *connector.SubscriptionUsageWindow {
	if limit <= 0 {
		return nil
	}
	if used < 0 {
		used = 0
	}
	remaining := limit - used
	if remaining < 0 {
		remaining = 0
	}
	usedPct := 0.0
	if limit > 0 {
		usedPct = used / limit * 100
	}
	if usedPct < 0 {
		usedPct = 0
	}
	if usedPct > 100 {
		usedPct = 100
	}
	return &connector.SubscriptionUsageWindow{
		LimitUSD:         limit,
		UsedUSD:          used,
		RemainingUSD:     remaining,
		RemainingPercent: 100 - usedPct,
		UsedPercent:      usedPct,
		WindowStart:      windowStart,
		ResetsAt:         resetsAt,
	}
}

func sub2WindowToConnector(w *sub2UsageWindow) *connector.SubscriptionUsageWindow {
	if w == nil || w.LimitUSD <= 0 {
		return nil
	}
	remaining := w.RemainingUSD
	if remaining < 0 {
		remaining = 0
	}
	usedPct := w.Percentage
	if usedPct < 0 {
		usedPct = 0
	}
	if usedPct > 100 {
		usedPct = 100
	}
	remainingPct := 100 - usedPct
	if remainingPct < 0 {
		remainingPct = 0
	}
	if remainingPct > 100 {
		remainingPct = 100
	}
	return &connector.SubscriptionUsageWindow{
		LimitUSD:         w.LimitUSD,
		UsedUSD:          w.UsedUSD,
		RemainingUSD:     remaining,
		RemainingPercent: remainingPct,
		UsedPercent:      usedPct,
		WindowStart:      w.WindowStart,
		ResetsAt:         w.ResetsAt,
		ResetsInSeconds:  w.ResetsInSeconds,
	}
}

func firstPositiveInt64(values ...int64) int64 {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

func hasJSONValue(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "null" && s != "{}"
}

func formatSub2SubscriptionValidity(days int, unit string) string {
	unit = strings.TrimSpace(unit)
	if days <= 0 {
		return unit
	}
	if unit == "" || unit == "day" || unit == "days" {
		return fmt.Sprintf("%d 天", days)
	}
	return fmt.Sprintf("%d %s", days, unit)
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

type sub2APIKey struct {
	ID      int64  `json:"id"`
	Key     string `json:"key"`
	Name    string `json:"name"`
	GroupID *int64 `json:"group_id"`
	Group   *struct {
		ID             int64   `json:"id"`
		Name           string  `json:"name"`
		Description    string  `json:"description"`
		RateMultiplier float64 `json:"rate_multiplier"`
	} `json:"group"`
	Status      string     `json:"status"`
	IPWhitelist []string   `json:"ip_whitelist"`
	IPBlacklist []string   `json:"ip_blacklist"`
	LastUsedAt  *time.Time `json:"last_used_at"`
	Quota       float64    `json:"quota"`
	QuotaUsed   float64    `json:"quota_used"`
	ExpiresAt   *time.Time `json:"expires_at"`
	CreatedAt   *time.Time `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at"`
	RateLimit5h float64    `json:"rate_limit_5h"`
	RateLimit1d float64    `json:"rate_limit_1d"`
	RateLimit7d float64    `json:"rate_limit_7d"`
	Usage5h     float64    `json:"usage_5h"`
	Usage1d     float64    `json:"usage_1d"`
	Usage7d     float64    `json:"usage_7d"`

	observedFields connector.APIKeyObservedFields
	nullFields     connector.APIKeyObservedFields
}

func (k *sub2APIKey) UnmarshalJSON(data []byte) error {
	type wireKey sub2APIKey
	var decoded wireKey
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*k = sub2APIKey(decoded)
	_, quotaPresent := fields["quota"]
	k.nullFields.Quota = jsonFieldIsNull(fields, "quota")
	k.observedFields.Quota = quotaPresent && !k.nullFields.Quota
	_, k.observedFields.ExpiresAt = fields["expires_at"]
	k.nullFields.ExpiresAt = jsonFieldIsNull(fields, "expires_at")
	_, k.observedFields.IPWhitelist = fields["ip_whitelist"]
	_, k.observedFields.IPBlacklist = fields["ip_blacklist"]
	_, rateLimit5hPresent := fields["rate_limit_5h"]
	k.nullFields.RateLimit5h = jsonFieldIsNull(fields, "rate_limit_5h")
	k.observedFields.RateLimit5h = rateLimit5hPresent && !k.nullFields.RateLimit5h
	_, rateLimit1dPresent := fields["rate_limit_1d"]
	k.nullFields.RateLimit1d = jsonFieldIsNull(fields, "rate_limit_1d")
	k.observedFields.RateLimit1d = rateLimit1dPresent && !k.nullFields.RateLimit1d
	_, rateLimit7dPresent := fields["rate_limit_7d"]
	k.nullFields.RateLimit7d = jsonFieldIsNull(fields, "rate_limit_7d")
	k.observedFields.RateLimit7d = rateLimit7dPresent && !k.nullFields.RateLimit7d
	return nil
}

func jsonFieldIsNull(fields map[string]json.RawMessage, name string) bool {
	raw, ok := fields[name]
	return ok && strings.TrimSpace(string(raw)) == "null"
}

func (k sub2APIKey) toConnector() connector.APIKey {
	out := connector.APIKey{
		ID:             k.ID,
		Key:            k.Key,
		Name:           k.Name,
		Status:         normalizeSub2APIKeyStatus(k.Status),
		GroupID:        k.GroupID,
		Quota:          k.Quota,
		QuotaUsed:      k.QuotaUsed,
		ExpiresAt:      k.ExpiresAt,
		CreatedAt:      k.CreatedAt,
		UpdatedAt:      k.UpdatedAt,
		LastUsedAt:     k.LastUsedAt,
		IPWhitelist:    safeStringSlice(k.IPWhitelist),
		IPBlacklist:    safeStringSlice(k.IPBlacklist),
		RateLimit5h:    k.RateLimit5h,
		RateLimit1d:    k.RateLimit1d,
		RateLimit7d:    k.RateLimit7d,
		Usage5h:        k.Usage5h,
		Usage1d:        k.Usage1d,
		Usage7d:        k.Usage7d,
		ObservedFields: k.observedFields,
		NullFields:     k.nullFields,
		Source:         connector.APIKeySourceSub2API,
	}
	if k.Group != nil {
		id := k.Group.ID
		if out.GroupID == nil {
			out.GroupID = &id
		}
		out.GroupName = k.Group.Name
		out.GroupDescription = k.Group.Description
		out.GroupRatio = k.Group.RateMultiplier
	}
	return out
}

func buildSub2CreateAPIKey(req connector.APIKeyCreateRequest) map[string]any {
	body := map[string]any{
		"name":         strings.TrimSpace(req.Name),
		"ip_whitelist": safeStringSlice(req.IPWhitelist),
		"ip_blacklist": safeStringSlice(req.IPBlacklist),
	}
	if req.GroupID != nil {
		body["group_id"] = *req.GroupID
	}
	if strings.TrimSpace(req.CustomKey) != "" {
		body["custom_key"] = strings.TrimSpace(req.CustomKey)
	}
	if req.Quota != nil {
		body["quota"] = *req.Quota
	}
	if req.ExpiresInDays != nil {
		body["expires_in_days"] = *req.ExpiresInDays
	}
	if req.RateLimit5h != nil {
		body["rate_limit_5h"] = *req.RateLimit5h
	}
	if req.RateLimit1d != nil {
		body["rate_limit_1d"] = *req.RateLimit1d
	}
	if req.RateLimit7d != nil {
		body["rate_limit_7d"] = *req.RateLimit7d
	}
	return body
}

func buildSub2UpdateAPIKey(req connector.APIKeyUpdateRequest) (map[string]any, error) {
	body := map[string]any{
		"ip_whitelist": safeStringSlice(req.IPWhitelist),
		"ip_blacklist": safeStringSlice(req.IPBlacklist),
	}
	if req.Name != nil {
		body["name"] = strings.TrimSpace(*req.Name)
	}
	if req.GroupID != nil {
		body["group_id"] = *req.GroupID
	}
	if req.Status != nil {
		status := normalizeSub2APIKeyStatus(*req.Status)
		if status != "active" && status != "disabled" {
			return nil, errors.New("sub2api 编辑状态仅支持 active 或 disabled")
		}
		body["status"] = sub2APIKeyStatusForUpdate(status)
	}
	if req.Quota != nil {
		body["quota"] = *req.Quota
	}
	if req.ExpiresAt != nil {
		body["expires_at"] = strings.TrimSpace(*req.ExpiresAt)
	}
	if req.ResetQuota != nil {
		body["reset_quota"] = *req.ResetQuota
	}
	if req.RateLimit5h != nil {
		body["rate_limit_5h"] = *req.RateLimit5h
	}
	if req.RateLimit1d != nil {
		body["rate_limit_1d"] = *req.RateLimit1d
	}
	if req.RateLimit7d != nil {
		body["rate_limit_7d"] = *req.RateLimit7d
	}
	if req.ResetRateLimitUsage != nil {
		body["reset_rate_limit_usage"] = *req.ResetRateLimitUsage
	}
	return body, nil
}

func decodeSub2Write(body []byte, prefix string) error {
	_, err := decodeSub2WriteData(body, prefix)
	return err
}

func decodeSub2WriteData(body []byte, prefix string) (json.RawMessage, error) {
	var wrapped sub2Resp
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, fmt.Errorf("%s decode: %w", prefix, err)
	}
	if wrapped.Code != 0 {
		msg := strings.TrimSpace(wrapped.Message)
		if msg == "" {
			msg = prefix + " failed"
		}
		return nil, errors.New(msg)
	}
	return wrapped.Data, nil
}

func normalizeSub2APIKeyStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "inactive":
		return "disabled"
	case "active", "disabled", "quota_exhausted", "expired":
		return strings.TrimSpace(status)
	default:
		return strings.TrimSpace(status)
	}
}

func sub2APIKeyStatusForUpdate(status string) string {
	if status == "disabled" {
		return "inactive"
	}
	return status
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

func safeStringSlice(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
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

func hashAnnouncementKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(strings.TrimSpace(p)))
		h.Write([]byte{0})
	}
	return "hash:" + hex.EncodeToString(h.Sum(nil))
}

func (c *Client) sub2APIGroupMap(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (map[int64]connector.APIKeyGroup, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	body, err := c.getJSON(ctx, site+"/api/v1/groups/available", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api api key groups: %w", err)
	}
	var raw []struct {
		ID             int64   `json:"id"`
		Name           string  `json:"name"`
		Description    string  `json:"description"`
		RateMultiplier float64 `json:"rate_multiplier"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api api key groups decode: %w", err)
	}
	overrides := map[string]float64{}
	if ratesBody, err := c.getJSON(ctx, site+"/api/v1/groups/rates", session); err == nil {
		_ = json.Unmarshal(ratesBody, &overrides)
	}
	out := make(map[int64]connector.APIKeyGroup, len(raw))
	groupIDsByName := make(map[string]int64, len(raw))
	for _, item := range raw {
		id := item.ID
		name := strings.TrimSpace(item.Name)
		if id <= 0 {
			return nil, fmt.Errorf("sub2api api key groups contain invalid id %d", id)
		}
		if name == "" {
			return nil, fmt.Errorf("sub2api api key group %d has an empty name", id)
		}
		if _, duplicate := out[id]; duplicate {
			return nil, fmt.Errorf("sub2api api key groups contain duplicate id %d", id)
		}
		canonicalName := strings.ToLower(name)
		if previousID, duplicate := groupIDsByName[canonicalName]; duplicate && previousID != id {
			return nil, fmt.Errorf("sub2api api key groups contain duplicate name %q", name)
		}
		groupIDsByName[canonicalName] = id
		ratio := item.RateMultiplier
		if v, ok := overrides[strconv.FormatInt(id, 10)]; ok {
			ratio = v
		}
		out[id] = connector.APIKeyGroup{
			ID:          &id,
			Name:        name,
			Description: item.Description,
			Ratio:       ratio,
		}
	}
	return out, nil
}
