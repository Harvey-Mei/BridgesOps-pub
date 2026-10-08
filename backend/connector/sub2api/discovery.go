package sub2api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"golang.org/x/net/html"
	"golang.org/x/net/publicsuffix"
)

const (
	maxDiscoveryPageBytes    = 2 << 20
	maxDiscoveryScriptBytes  = 4 << 20
	maxDiscoveryScripts      = 10
	maxDiscoveryCandidates   = 16
	defaultDiscoveryTimeout  = 15 * time.Second
	discoveryRequestTimeout  = 5 * time.Second
	maxDiscoveryRedirects    = 5
	apiOriginFailureCacheTTL = 10 * time.Second
	transientDiscoveryRetry  = time.Minute
)

var absoluteDiscoveryURLPattern = regexp.MustCompile("https?:\\\\?/\\\\?/[^\\s\\\"'`<>\\\\]+")

// DiscoverAPIOrigin resolves a user-facing Sub2API homepage to the verified
// origin that serves /api/v1. It never sends credentials while discovering.
func (c *Client) DiscoverAPIOrigin(ctx context.Context, siteURL string) (string, error) {
	origin, err := c.discoverAPIOrigin(ctx, siteURL, true)
	return origin, withTransientDiscoveryRetry(ctx, err)
}

func (c *Client) discoverAPIOrigin(ctx context.Context, siteURL string, validateCurrent bool, prechecked ...string) (string, error) {
	timeout := c.discoveryTimeout
	if timeout <= 0 {
		timeout = defaultDiscoveryTimeout
	}
	discoveryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ctx = discoveryCtx

	base, err := normalizeDiscoveryBase(siteURL)
	if err != nil {
		return "", err
	}
	var candidateValidationErr error
	if validateCurrent {
		validationErr := c.validateAPIOrigin(ctx, base)
		if validationErr == nil {
			return base, nil
		}
		if isDiscoveryStageError(validationErr) {
			return "", fmt.Errorf("Sub2API API 地址连接验证失败（未发送登录凭据）：%w", validationErr)
		}
		candidateValidationErr = fmt.Errorf("%s：%w", base, validationErr)
		prechecked = append(prechecked, base)
	}

	status, pageBody, pageURL, err := c.fetchDiscoveryResource(ctx, base, maxDiscoveryPageBytes)
	if err != nil {
		return "", fmt.Errorf("读取 Sub2API 主页失败：%w", err)
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return "", fmt.Errorf("读取 Sub2API 主页失败：%w", connector.HTTPStatusError(status, pageBody))
	}
	pageBase, err := url.Parse(pageURL)
	if err != nil {
		return "", fmt.Errorf("解析 Sub2API 主页地址失败：%w", err)
	}
	originalBase, _ := url.Parse(base)
	if !sameDiscoverySite(originalBase, pageBase) {
		return "", errors.New("Sub2API 主页重定向到了不同站点，已停止自动检测")
	}

	candidates := newDiscoveryCandidates(base)
	candidates.addText(pageBody, pageBase)
	checked := make(map[string]struct{})
	for _, value := range prechecked {
		if normalized, normalizeErr := normalizeDiscoveryBase(value); normalizeErr == nil {
			checked[normalized] = struct{}{}
		}
	}
	if candidate, validationErr := c.firstValidDiscoveryCandidate(ctx, candidates, pageBase, checked); candidate != "" {
		return candidate, nil
	} else {
		candidateValidationErr = preferredDiscoveryValidationError(candidateValidationErr, validationErr)
	}
	for _, scriptURL := range discoveryScriptURLs(pageBody, pageBase) {
		if candidates.scriptCount >= maxDiscoveryScripts {
			break
		}
		parsed, parseErr := url.Parse(scriptURL)
		if parseErr != nil || !safeDiscoveryRedirect(pageBase, parsed) {
			continue
		}
		candidates.scriptCount++
		status, scriptBody, finalScriptURL, fetchErr := c.fetchDiscoveryResource(ctx, scriptURL, maxDiscoveryScriptBytes)
		if fetchErr != nil || status < http.StatusOK || status >= http.StatusMultipleChoices {
			continue
		}
		finalBase, parseErr := url.Parse(finalScriptURL)
		if parseErr != nil {
			continue
		}
		candidates.addText(scriptBody, finalBase)
		if candidate, validationErr := c.firstValidDiscoveryCandidate(ctx, candidates, pageBase, checked); candidate != "" {
			return candidate, nil
		} else {
			candidateValidationErr = preferredDiscoveryValidationError(candidateValidationErr, validationErr)
		}
	}
	if err := ctx.Err(); err != nil {
		if candidateValidationErr != nil {
			return "", fmt.Errorf("Sub2API API 地址自动检测超时；候选地址验证失败：%v：%w", candidateValidationErr, err)
		}
		return "", fmt.Errorf("Sub2API API 地址自动检测超时：%w", err)
	}
	if candidateValidationErr != nil {
		return "", fmt.Errorf("未能从主页或同站点前端资源中识别有效的 Sub2API API 地址；候选地址验证失败：%w", candidateValidationErr)
	}
	return "", errors.New("未能从主页或同站点前端资源中识别有效的 Sub2API API 地址")
}

func (c *Client) firstValidDiscoveryCandidate(ctx context.Context, candidates *discoveryCandidates, pageBase *url.URL, checked map[string]struct{}) (string, error) {
	var validationErr error
	for _, candidate := range candidates.values {
		if len(checked) >= maxDiscoveryCandidates {
			return "", validationErr
		}
		if _, exists := checked[candidate]; exists {
			continue
		}
		checked[candidate] = struct{}{}
		parsed, parseErr := url.Parse(candidate)
		if parseErr != nil || !safeDiscoveryRedirect(pageBase, parsed) {
			continue
		}
		if err := c.validateAPIOrigin(ctx, candidate); err == nil {
			return candidate, nil
		} else {
			validationErr = preferredDiscoveryValidationError(validationErr, fmt.Errorf("%s：%w", candidate, err))
		}
	}
	return "", validationErr
}

func preferredDiscoveryValidationError(current, candidate error) error {
	if candidate == nil {
		return current
	}
	if current != nil && isDiscoveryStageError(current) && !isDiscoveryStageError(candidate) {
		return current
	}
	return candidate
}

func (c *Client) ensureVerifiedAPIOrigin(ctx context.Context, ch *connector.Channel) error {
	if ch == nil {
		return errors.New("缺少 Sub2API 渠道配置")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.isVerifiedAPIOrigin(ch.SiteURL) {
		return nil
	}
	// APIOriginKnown means this origin was explicitly configured or previously
	// discovered and persisted. Do not turn every authenticated operation into
	// a homepage/public-settings probe; an explicit endpoint mismatch below is
	// the signal to rediscover it.
	if ch.APIOriginKnown {
		// The persisted API origin is an administrator-approved endpoint. It may
		// intentionally live on a different registrable domain than the homepage.
		// Auto-discovered candidates still pass the same-site checks below.
		return nil
	}
	if cachedErr := c.cachedAPIOriginFailure(ch.SiteURL, time.Now()); cachedErr != nil {
		return cachedErr
	}
	timeout := c.discoveryTimeout
	if timeout <= 0 {
		timeout = defaultDiscoveryTimeout
	}
	verificationCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	validationErr := c.validateAPIOrigin(verificationCtx, ch.SiteURL)
	if validationErr == nil {
		ch.APIOriginKnown = true
		return nil
	}
	if isTransientDiscoveryFailure(validationErr) {
		wrapped := withTransientDiscoveryRetry(ctx, fmt.Errorf("Sub2API API 地址暂时无法验证（未发送登录凭据）：%w", validationErr))
		if ctx.Err() == nil {
			c.cacheAPIOriginFailure(ch.SiteURL, wrapped, time.Now())
		}
		return wrapped
	}
	homepageURL := strings.TrimSpace(ch.HomepageURL)
	startURL := ch.SiteURL
	if homepageURL != "" {
		startURL = homepageURL
	}
	discovered, err := c.discoverAPIOrigin(verificationCtx, startURL, false, ch.SiteURL)
	if err != nil {
		combined := fmt.Errorf("当前 Sub2API API 地址验证失败（%v）；从主页自动检测也失败：%w", validationErr, err)
		return withTransientDiscoveryRetry(ctx, combined)
	}
	ch.SiteURL = discovered
	ch.APIOriginKnown = true
	return nil
}

func (c *Client) rediscoverExplicitlyInvalidAPIOrigin(ctx context.Context, ch *connector.Channel, cause error) error {
	if ch == nil {
		return errors.New("缺少 Sub2API 渠道配置")
	}
	homepageURL := strings.TrimSpace(ch.HomepageURL)
	if homepageURL == "" {
		return fmt.Errorf("当前 Sub2API API 地址已明确失效（%w），但未配置网页主页地址", cause)
	}
	discovered, err := c.discoverAPIOrigin(ctx, homepageURL, false, ch.SiteURL)
	if err != nil {
		combined := fmt.Errorf("当前 Sub2API API 地址已明确失效（%v）；从主页重新检测也失败：%w", cause, err)
		return withTransientDiscoveryRetry(ctx, combined)
	}
	ch.SiteURL = discovered
	ch.APIOriginKnown = true
	return nil
}

type apiOriginResponseError struct {
	err      error
	mismatch bool
}

func (err *apiOriginResponseError) Error() string { return err.err.Error() }
func (err *apiOriginResponseError) Unwrap() error { return err.err }
func (err *apiOriginResponseError) Is(target error) bool {
	return err.mismatch && target == connector.ErrAPIOriginMismatch
}

func isExplicitAPIOriginFailure(err error) bool {
	var redirectErr *apiOriginRedirectError
	if errors.As(err, &redirectErr) {
		return true
	}
	var responseErr *apiOriginResponseError
	if errors.As(err, &responseErr) {
		return true
	}
	var httpErr *connector.HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	switch httpErr.StatusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusGone:
		return true
	default:
		return false
	}
}

func withTransientDiscoveryRetry(ctx context.Context, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || (ctx != nil && errors.Is(ctx.Err(), context.Canceled)) {
		return err
	}
	if !isTransientDiscoveryFailure(err) {
		return err
	}
	return connector.WithRetryDelay(err, transientDiscoveryRetry)
}

func isTransientDiscoveryFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || isDiscoveryStageError(err) {
		if errors.Is(err, context.DeadlineExceeded) {
			return true
		}
		var stageErr *discoveryStageError
		if errors.As(err, &stageErr) {
			return isTransientDiscoveryStageError(stageErr)
		}
	}
	var httpErr *connector.HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	return httpErr.StatusCode == http.StatusRequestTimeout ||
		httpErr.StatusCode == http.StatusTooEarly ||
		httpErr.StatusCode == http.StatusTooManyRequests ||
		httpErr.StatusCode >= http.StatusInternalServerError
}

func (c *Client) validateAPIOrigin(ctx context.Context, base string) error {
	settings, err := c.fetchPublicSettings(ctx, base)
	if err != nil {
		return err
	}
	c.markVerifiedAPIOrigin(base, settings)
	return nil
}

func sameDiscoveryOrigin(left, right string) bool {
	leftURL, leftErr := url.Parse(strings.TrimSpace(left))
	rightURL, rightErr := url.Parse(strings.TrimSpace(right))
	return leftErr == nil && rightErr == nil && sameURLOrigin(leftURL, rightURL)
}

func (c *Client) fetchPublicSettings(ctx context.Context, base string) (json.RawMessage, error) {
	status, body, finalURL, err := c.fetchDiscoveryResource(ctx, strings.TrimRight(base, "/")+"/api/v1/settings/public", maxDiscoveryPageBytes)
	if err != nil {
		return nil, err
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return nil, connector.HTTPStatusError(status, body)
	}
	requested, requestErr := url.Parse(base)
	final, finalErr := url.Parse(finalURL)
	if requestErr != nil || finalErr != nil || !sameURLOrigin(requested, final) {
		return nil, &apiOriginRedirectError{message: "Sub2API public settings redirected to a different origin"}
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, &apiOriginResponseError{err: fmt.Errorf("decode Sub2API public settings: %w", err), mismatch: true}
	}
	if wrapped.Code != 0 {
		return nil, &apiOriginResponseError{err: errors.New(wrapped.Message), mismatch: true}
	}
	if len(wrapped.Data) == 0 || string(wrapped.Data) == "null" {
		return nil, &apiOriginResponseError{err: errors.New("Sub2API public settings data is empty"), mismatch: true}
	}
	var data map[string]json.RawMessage
	if json.Unmarshal(wrapped.Data, &data) != nil || data == nil {
		return nil, &apiOriginResponseError{err: errors.New("Sub2API public settings data is not an object"), mismatch: true}
	}
	var turnstileEnabled *bool
	if raw, exists := data["turnstile_enabled"]; !exists || json.Unmarshal(raw, &turnstileEnabled) != nil || turnstileEnabled == nil {
		return nil, &apiOriginResponseError{err: errors.New("Sub2API public settings data is missing a boolean turnstile_enabled"), mismatch: true}
	}
	return wrapped.Data, nil
}

func (c *Client) fetchDiscoveryResource(ctx context.Context, rawURL string, limit int64) (int, []byte, string, error) {
	requestCtx, cancel := context.WithTimeout(ctx, discoveryRequestTimeout)
	defer cancel()
	var stage atomic.Int32
	stage.Store(int32(discoveryStagePrepare))
	trace := &httptrace.ClientTrace{
		DNSStart:          func(httptrace.DNSStartInfo) { stage.Store(int32(discoveryStageDNS)) },
		ConnectStart:      func(_, _ string) { stage.Store(int32(discoveryStageConnect)) },
		TLSHandshakeStart: func() { stage.Store(int32(discoveryStageTLS)) },
		GotConn:           func(httptrace.GotConnInfo) { stage.Store(int32(discoveryStageResponseHeader)) },
	}
	requestCtx = httptrace.WithClientTrace(requestCtx, trace)
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, nil, "", err
	}
	req.Header.Set("Accept", "text/html,application/javascript,application/json;q=0.9,*/*;q=0.8")
	if userAgent := strings.TrimSpace(c.http.Header.Get("User-Agent")); userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	requestURL, err := url.Parse(rawURL)
	if err != nil {
		return 0, nil, "", err
	}
	discoveryClient := *c.http.GetClient()
	discoveryClient.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) >= maxDiscoveryRedirects {
			return errors.New("Sub2API 自动检测重定向次数过多")
		}
		if !safeDiscoveryRedirect(requestURL, next.URL) {
			return &apiOriginRedirectError{message: "Sub2API 自动检测已阻止跨站点或降级重定向"}
		}
		return nil
	}
	resp, err := discoveryClient.Do(req)
	if err != nil {
		return 0, nil, "", &discoveryStageError{Stage: discoveryRequestStage(stage.Load()), Err: err}
	}
	defer resp.Body.Close()
	stage.Store(int32(discoveryStageResponseBody))
	if resp.ContentLength > limit {
		return resp.StatusCode, nil, resp.Request.URL.String(), fmt.Errorf("响应超过 %d 字节限制", limit)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return resp.StatusCode, nil, resp.Request.URL.String(), &discoveryStageError{Stage: discoveryStageResponseBody, Err: err}
	}
	if int64(len(body)) > limit {
		return resp.StatusCode, nil, resp.Request.URL.String(), fmt.Errorf("响应超过 %d 字节限制", limit)
	}
	return resp.StatusCode, body, resp.Request.URL.String(), nil
}

func (c *Client) markVerifiedAPIOrigin(raw string, settings json.RawMessage) {
	key, err := normalizeDiscoveryBase(raw)
	if err != nil {
		return
	}
	c.verifiedMu.Lock()
	if c.verifiedOrigins == nil {
		c.verifiedOrigins = make(map[string]json.RawMessage)
	}
	c.verifiedOrigins[key] = append(json.RawMessage(nil), settings...)
	delete(c.verificationFailures, key)
	c.verifiedMu.Unlock()
}

func (c *Client) isVerifiedAPIOrigin(raw string) bool {
	key, err := normalizeDiscoveryBase(raw)
	if err != nil {
		return false
	}
	c.verifiedMu.RLock()
	_, ok := c.verifiedOrigins[key]
	c.verifiedMu.RUnlock()
	return ok
}

func (c *Client) takeVerifiedAPISettings(raw string) (json.RawMessage, bool) {
	key, err := normalizeDiscoveryBase(raw)
	if err != nil {
		return nil, false
	}
	c.verifiedMu.Lock()
	settings, ok := c.verifiedOrigins[key]
	if ok && len(settings) > 0 {
		c.verifiedOrigins[key] = nil
	}
	c.verifiedMu.Unlock()
	return append(json.RawMessage(nil), settings...), ok && len(settings) > 0
}

type apiOriginVerificationFailure struct {
	err error
	at  time.Time
}

// apiOriginRedirectError marks a redirect that crossed an origin boundary or
// downgraded transport. Authenticated requests must fail closed, but the
// caller can still use this signal to rediscover a currently stale API origin
// from the configured homepage.
type apiOriginRedirectError struct{ message string }

func (err *apiOriginRedirectError) Error() string { return err.message }

func (c *Client) cacheAPIOriginFailure(raw string, err error, at time.Time) {
	key, normalizeErr := normalizeDiscoveryBase(raw)
	if normalizeErr != nil || err == nil {
		return
	}
	c.verifiedMu.Lock()
	if c.verificationFailures == nil {
		c.verificationFailures = make(map[string]apiOriginVerificationFailure)
	}
	c.verificationFailures[key] = apiOriginVerificationFailure{err: err, at: at}
	c.verifiedMu.Unlock()
}

func (c *Client) cachedAPIOriginFailure(raw string, now time.Time) error {
	key, err := normalizeDiscoveryBase(raw)
	if err != nil {
		return nil
	}
	c.verifiedMu.Lock()
	failure, ok := c.verificationFailures[key]
	if ok && now.Sub(failure.at) >= apiOriginFailureCacheTTL {
		delete(c.verificationFailures, key)
		ok = false
	}
	c.verifiedMu.Unlock()
	if !ok {
		return nil
	}
	return failure.err
}

type discoveryRequestStage int32

const (
	discoveryStagePrepare discoveryRequestStage = iota
	discoveryStageDNS
	discoveryStageConnect
	discoveryStageTLS
	discoveryStageResponseHeader
	discoveryStageResponseBody
)

func (stage discoveryRequestStage) String() string {
	switch stage {
	case discoveryStageDNS:
		return "DNS 解析"
	case discoveryStageConnect:
		return "TCP/代理连接"
	case discoveryStageTLS:
		return "TLS 握手或证书验证"
	case discoveryStageResponseHeader:
		return "等待 HTTP 响应头"
	case discoveryStageResponseBody:
		return "读取 HTTP 响应正文"
	default:
		return "创建 HTTP 请求"
	}
}

type discoveryStageError struct {
	Stage discoveryRequestStage
	Err   error
}

func (err *discoveryStageError) Error() string {
	return err.Stage.String() + "失败：" + err.Err.Error()
}

func (err *discoveryStageError) Unwrap() error { return err.Err }

func isDiscoveryStageError(err error) bool {
	var stageErr *discoveryStageError
	return errors.As(err, &stageErr)
}

func isTransientDiscoveryStageError(err *discoveryStageError) bool {
	if err == nil {
		return false
	}
	if errors.Is(err.Err, context.DeadlineExceeded) || errors.Is(err.Err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkErr net.Error
	if errors.As(err.Err, &networkErr) {
		return networkErr.Timeout() || networkErr.Temporary()
	}
	return false
}

func sameOriginRedirectPolicy(request *http.Request, via []*http.Request) error {
	if len(via) == 0 || len(via) >= maxDiscoveryRedirects {
		return errors.New("Sub2API 请求重定向次数过多")
	}
	if !sameURLOrigin(via[0].URL, request.URL) {
		return &apiOriginRedirectError{message: "Sub2API 已阻止携带凭据的跨源重定向"}
	}
	return nil
}

func sameURLOrigin(left, right *url.URL) bool {
	if left == nil || right == nil {
		return false
	}
	return strings.EqualFold(left.Scheme, right.Scheme) &&
		strings.EqualFold(left.Hostname(), right.Hostname()) &&
		effectiveURLPort(left) == effectiveURLPort(right)
}

func effectiveURLPort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	if strings.EqualFold(value.Scheme, "https") {
		return "443"
	}
	if strings.EqualFold(value.Scheme, "http") {
		return "80"
	}
	return ""
}

func safeDiscoveryRedirect(source, target *url.URL) bool {
	if !sameDiscoverySite(source, target) {
		return false
	}
	return !strings.EqualFold(source.Scheme, "https") || strings.EqualFold(target.Scheme, "https")
}

func normalizeDiscoveryBase(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("Sub2API 站点地址必须是完整的 http(s) URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("Sub2API 站点地址只支持 http(s)")
	}
	if parsed.User != nil {
		return "", errors.New("Sub2API 站点地址不能包含用户信息")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

type discoveryCandidates struct {
	seen        map[string]struct{}
	values      []string
	scriptCount int
}

func newDiscoveryCandidates(initial string) *discoveryCandidates {
	candidates := &discoveryCandidates{seen: make(map[string]struct{})}
	candidates.add(initial)
	return candidates
}

func (c *discoveryCandidates) addText(body []byte, source *url.URL) {
	for _, match := range absoluteDiscoveryURLPattern.FindAll(body, -1) {
		raw := strings.ReplaceAll(string(match), `\/`, "/")
		raw = strings.TrimRight(raw, ".,;:)]}")
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" || !safeDiscoveryRedirect(source, parsed) {
			continue
		}
		path := parsed.Path
		if index := strings.Index(path, "/api/v1"); index >= 0 {
			path = path[:index]
		} else {
			path = ""
		}
		parsed.Path = strings.TrimRight(path, "/")
		parsed.RawPath = ""
		parsed.RawQuery = ""
		parsed.Fragment = ""
		parsed.User = nil
		c.add(strings.TrimRight(parsed.String(), "/"))
	}
}

func (c *discoveryCandidates) add(value string) {
	if value == "" {
		return
	}
	if _, exists := c.seen[value]; exists {
		return
	}
	c.seen[value] = struct{}{}
	c.values = append(c.values, value)
}

func discoveryScriptURLs(body []byte, base *url.URL) []string {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil
	}
	var scripts []string
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if len(scripts) >= maxDiscoveryScripts || node == nil {
			return
		}
		if node.Type == html.ElementNode && strings.EqualFold(node.Data, "script") {
			for _, attr := range node.Attr {
				if !strings.EqualFold(attr.Key, "src") {
					continue
				}
				reference, parseErr := url.Parse(strings.TrimSpace(attr.Val))
				if parseErr == nil {
					resolved := base.ResolveReference(reference)
					if safeDiscoveryRedirect(base, resolved) {
						scripts = append(scripts, resolved.String())
					}
				}
				break
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)
	return scripts
}

func sameDiscoverySite(source, candidate *url.URL) bool {
	if source == nil || candidate == nil || candidate.User != nil {
		return false
	}
	if candidate.Scheme != "http" && candidate.Scheme != "https" {
		return false
	}
	sourceHost := strings.ToLower(strings.TrimSuffix(source.Hostname(), "."))
	candidateHost := strings.ToLower(strings.TrimSuffix(candidate.Hostname(), "."))
	if sourceHost == "" || candidateHost == "" {
		return false
	}
	if sourceHost == candidateHost {
		return true
	}
	if net.ParseIP(sourceHost) != nil || net.ParseIP(candidateHost) != nil || candidateHost == "localhost" {
		return false
	}
	sourceRoot, sourceErr := publicsuffix.EffectiveTLDPlusOne(sourceHost)
	candidateRoot, candidateErr := publicsuffix.EffectiveTLDPlusOne(candidateHost)
	return sourceErr == nil && candidateErr == nil && sourceRoot == candidateRoot
}
