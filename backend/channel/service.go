// Package channel 提供渠道领域服务：把存储层的加密字段解开成 connector.Channel，
// 处理登录会话的复用与刷新、手动测试登录、手动刷新余额 / 倍率等。
package channel

import (
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
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/captcha"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/progress"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
)

// SessionRefreshThreshold 距离过期还有多久就提前刷新登录。
const SessionRefreshThreshold = 5 * time.Minute

// tokenSessionTTL token 模式下给用户提供的 access_token 一个兜底有效期。
// 真正失效检测靠 connector.CheckAuth；若凭据里有 refresh_token，会优先尝试刷新并回写。
const tokenSessionTTL = 365 * 24 * time.Hour

var ErrDirectAPIKeyDeletionDisabled = errors.New("direct upstream api key deletion is disabled; use managed gateway key removal")

// Service 渠道领域服务。
type Service struct {
	Channels       *storage.Channels
	NetworkProxies *storage.NetworkProxies
	AuthSessions   *storage.AuthSessions
	Captchas       *storage.Captchas
	Rates          *storage.Rates
	MonitorLogs    *storage.MonitorLogs
	APIKeyOps      *storage.ChannelAPIKeyOperations
	Cipher         *crypto.Cipher
	Costing        *costing.Service
	UserTime       *usertime.Service

	mu       sync.RWMutex
	upstream config.UpstreamConfig
}

func (s *Service) SetCosting(service *costing.Service) { s.Costing = service }

func (s *Service) SetNetworkProxies(proxies *storage.NetworkProxies) {
	s.NetworkProxies = proxies
}

func NewService(
	channels *storage.Channels,
	authSessions *storage.AuthSessions,
	captchas *storage.Captchas,
	rates *storage.Rates,
	monitorLogs *storage.MonitorLogs,
	cipher *crypto.Cipher,
	userTimes ...*usertime.Service,
) *Service {
	userTime := usertime.Default()
	if len(userTimes) > 0 && userTimes[0] != nil {
		userTime = userTimes[0]
	}
	return &Service{
		Channels:     channels,
		AuthSessions: authSessions,
		Captchas:     captchas,
		Rates:        rates,
		MonitorLogs:  monitorLogs,
		APIKeyOps:    storage.NewChannelAPIKeyOperationsFromChannels(channels),
		Cipher:       cipher,
		UserTime:     userTime,
	}
}

func (s *Service) UpdateUpstreamConfig(cfg config.UpstreamConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upstream = cfg.WithDefaults()
}

// LegacyNetworkProxyMigration reports a one-time conversion from the retired
// channel-level proxy switch to an explicit network-proxy binding.
type LegacyNetworkProxyMigration struct {
	Bound           int64
	ClearedToDirect int64
}

// MigrateLegacyNetworkProxyBindings converts old proxy_enabled channel rows
// before monitoring and gateway work start. A disabled system proxy meant
// direct traffic even in the old model, so stale flags are cleared. An enabled
// system proxy becomes one encrypted, explicit profile shared only by those
// legacy rows. Invalid legacy configuration fails closed instead of silently
// routing those channels directly.
func (s *Service) MigrateLegacyNetworkProxyBindings(cfg config.ProxyConfig) (LegacyNetworkProxyMigration, error) {
	result := LegacyNetworkProxyMigration{}
	if s == nil || s.Channels == nil || s.NetworkProxies == nil {
		return result, errors.New("channel network proxy migration is unavailable")
	}
	count, err := s.Channels.LegacyNetworkProxyBindingCount()
	if err != nil || count == 0 {
		return result, err
	}
	if !cfg.Enabled {
		result.ClearedToDirect, err = s.Channels.ClearLegacyNetworkProxyFlags()
		return result, err
	}

	rawURL, err := cfg.URL()
	if err != nil {
		return result, fmt.Errorf("legacy channel proxy configuration is invalid: %w", err)
	}
	endpoint, err := netproxy.ParseURL(rawURL)
	if err != nil {
		return result, fmt.Errorf("legacy channel proxy configuration is invalid: %w", err)
	}
	profiles, err := s.NetworkProxies.List()
	if err != nil {
		return result, err
	}
	profile, err := s.findMatchingNetworkProxy(profiles, endpoint)
	if err != nil {
		return result, err
	}
	if profile == nil {
		profile, err = s.createLegacyNetworkProxy(profiles, endpoint)
		if err != nil {
			return result, err
		}
	}
	result.Bound, err = s.Channels.BindLegacyNetworkProxy(profile.ID)
	return result, err
}

func (s *Service) findMatchingNetworkProxy(profiles []storage.NetworkProxy, endpoint netproxy.Endpoint) (*storage.NetworkProxy, error) {
	for index := range profiles {
		if netproxy.Protocol(profiles[index].Protocol) != endpoint.Protocol || profiles[index].Host != endpoint.Host || profiles[index].Port != endpoint.Port {
			continue
		}
		profileEndpoint, err := s.networkProxyEndpoint(&profiles[index])
		if err != nil {
			return nil, errors.New("matching network proxy credentials cannot be read")
		}
		if profileEndpoint != endpoint {
			continue
		}
		if !profiles[index].Enabled {
			return nil, errors.New("the matching network proxy is disabled")
		}
		return &profiles[index], nil
	}
	return nil, nil
}

func (s *Service) createLegacyNetworkProxy(profiles []storage.NetworkProxy, endpoint netproxy.Endpoint) (*storage.NetworkProxy, error) {
	if s.Cipher == nil {
		return nil, errors.New("credential cipher is unavailable")
	}
	credentials, err := (netproxy.Credentials{Username: endpoint.Username, Password: endpoint.Password}).Encode()
	if err != nil {
		return nil, err
	}
	cipherText := ""
	if credentials != "" {
		cipherText, err = s.Cipher.Encrypt(credentials)
		if err != nil {
			return nil, errors.New("encrypt legacy network proxy credentials")
		}
	}
	profile := &storage.NetworkProxy{
		Name:             legacyNetworkProxyName(profiles),
		Protocol:         string(endpoint.Protocol),
		Host:             endpoint.Host,
		Port:             endpoint.Port,
		CredentialCipher: cipherText,
		Source:           "legacy-system",
		Enabled:          true,
	}
	if err := s.NetworkProxies.Create(profile); err != nil {
		return nil, err
	}
	return profile, nil
}

func legacyNetworkProxyName(profiles []storage.NetworkProxy) string {
	used := make(map[string]struct{}, len(profiles))
	for _, profile := range profiles {
		used[strings.ToLower(strings.TrimSpace(profile.Name))] = struct{}{}
	}
	const base = "已迁移系统代理"
	for suffix := 1; ; suffix++ {
		name := base
		if suffix > 1 {
			name = fmt.Sprintf("%s (%d)", base, suffix)
		}
		if _, exists := used[strings.ToLower(name)]; !exists {
			return name
		}
	}
}

func (s *Service) networkProxyURL(c *storage.Channel) (string, string, error) {
	return s.networkProxyURLContext(context.Background(), c)
}

func (s *Service) networkProxyURLContext(ctx context.Context, c *storage.Channel) (string, string, error) {
	if c == nil || c.NetworkProxyID == nil || *c.NetworkProxyID == 0 {
		return "", "", nil
	}
	if s.NetworkProxies == nil {
		return "", "", errors.New("network proxy storage is unavailable")
	}
	profile, err := s.NetworkProxies.FindByIDContext(ctx, *c.NetworkProxyID)
	if err != nil {
		return "", "", errors.New("configured network proxy was not found")
	}
	if !profile.Enabled {
		return "", "", errors.New("configured network proxy is disabled")
	}
	endpoint, err := s.networkProxyEndpoint(profile)
	if err != nil {
		return "", "", err
	}
	proxyURL, err := endpoint.URL()
	if err != nil {
		return "", "", err
	}
	// UpdatedAt also changes after a harmless proxy health test. Cache against
	// the dedicated connection revision instead, otherwise routine testing
	// discards keep-alive reuse for every channel bound to this proxy.
	revision := profile.ConnectionRevision
	if revision == 0 {
		revision = 1
	}
	return proxyURL, fmt.Sprintf("network-proxy:%d:%d", profile.ID, revision), nil
}

func (s *Service) networkProxyEndpoint(profile *storage.NetworkProxy) (netproxy.Endpoint, error) {
	if profile == nil {
		return netproxy.Endpoint{}, errors.New("configured network proxy was not found")
	}
	credentials := netproxy.Credentials{}
	if profile.CredentialCipher != "" {
		if s.Cipher == nil {
			return netproxy.Endpoint{}, errors.New("credential cipher is unavailable")
		}
		raw, err := s.Cipher.Decrypt(profile.CredentialCipher)
		if err != nil {
			return netproxy.Endpoint{}, errors.New("decrypt network proxy credentials")
		}
		credentials, err = netproxy.DecodeCredentials(raw)
		if err != nil {
			return netproxy.Endpoint{}, err
		}
	}
	return (netproxy.Endpoint{
		Protocol: netproxy.Protocol(profile.Protocol),
		Host:     profile.Host,
		Port:     profile.Port,
		Username: credentials.Username,
		Password: credentials.Password,
	}).Normalized()
}

func (s *Service) validateNetworkProxyID(id *uint) (*uint, error) {
	if id == nil || *id == 0 {
		return nil, nil
	}
	if s.NetworkProxies == nil {
		return nil, errors.New("network proxy storage is unavailable")
	}
	profile, err := s.NetworkProxies.FindByID(*id)
	if err != nil {
		return nil, errors.New("network proxy was not found")
	}
	if !profile.Enabled {
		return nil, errors.New("network proxy is disabled")
	}
	resolved := profile.ID
	return &resolved, nil
}

func (s *Service) upstreamConfig() config.UpstreamConfig {
	s.mu.RLock()
	cfg := s.upstream
	s.mu.RUnlock()
	return cfg.WithDefaults()
}

func applyProxy(conn connector.Connector, resolved *connector.Channel) error {
	if resolved == nil || strings.TrimSpace(resolved.ProxyURL) == "" {
		return nil
	}
	if setter, ok := conn.(connector.ProxySetter); ok {
		return setter.SetProxy(resolved.ProxyURL)
	}
	return errors.New("connector does not support the configured network proxy")
}

func (s *Service) ApplyProxy(conn connector.Connector, resolved *connector.Channel) error {
	return applyProxy(conn, resolved)
}

func applyHTTPConfig(conn any, cfg config.UpstreamConfig) {
	if setter, ok := conn.(connector.HTTPConfigSetter); ok {
		cfg = cfg.WithDefaults()
		setter.SetHTTPConfig(connector.HTTPConfig{
			Timeout:   time.Duration(cfg.TimeoutSeconds) * time.Second,
			UserAgent: cfg.UserAgent,
		})
	}
}

func (s *Service) applyHTTPConfig(conn any) {
	applyHTTPConfig(conn, s.upstreamConfig())
}

func (s *Service) ApplyHTTPConfig(conn any) {
	s.applyHTTPConfig(conn)
}

// NewAPITokenCredential token 模式下 NewAPI 的凭据 JSON 结构。
//
// 两种鉴权方式二选一：
//   - Cookie：浏览器 DevTools 里拷出来的整条 Cookie 头（典型形如 session=xxxxx; ...）
//   - AccessToken：NewAPI「个人设置 / 生成的系统访问令牌」即 user.access_token（32 位字符串）
//     发给上游时走 Authorization 头而不是 Cookie 头，session 续期无关。
//
// UserID：上游账号 ID（NewAPI 个人设置页可见，作为 New-Api-User 请求头必填）
type NewAPITokenCredential struct {
	Cookie      string `json:"cookie,omitempty"`
	AccessToken string `json:"access_token,omitempty"`
	UserID      string `json:"user_id"`
}

// Sub2APITokenCredential token 模式下 Sub2API 的凭据。
type Sub2APITokenCredential struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

// CreateInput 新建渠道使用的明文输入。
//
// CredentialMode 决定字段语义：
//   - password: Password 必填；Username 为登录账号
//   - token:    TokenCredential 必填（已序列化为 JSON 字符串）；Username 仅作展示备注
type CreateInput struct {
	Name                   string
	Type                   storage.ChannelType
	GatewayApp             storage.GatewayApp
	SiteURL                string
	Username               string
	SortOrder              int
	Password               string
	CredentialMode         storage.CredentialMode
	TokenCredential        string // JSON：password 模式时为空
	DirectAPIKey           string // direct 模式：OpenAI-compatible API key
	DirectModels           string // direct 模式：JSON model id 数组
	DirectCredentials      []DirectCredentialInput
	LoginExtraParams       string
	TurnstileEnabled       bool
	IgnoreAnnouncements    bool
	SubscriptionEnabled    bool
	ProxyEnabled           bool
	NetworkProxyID         *uint
	CaptchaConfigID        *uint
	BalanceThreshold       float64
	RechargeMultiplier     *float64
	RechargeMultiplierMode string
	MonitorEnabled         bool
}

// DirectCredentialInput is one administrator-configured direct group. APIKey
// may be blank on update to retain the encrypted key already stored for that
// group; it is required when creating a new group.
type DirectCredentialInput struct {
	ID              uint
	GroupName       string
	BaseURL         string
	APIKey          string
	Models          string
	Ratio           float64
	CompletionRatio float64
	Enabled         *bool
}

func (s *Service) Create(in CreateInput) (*storage.Channel, error) {
	mode := in.CredentialMode
	if mode == "" {
		mode = storage.CredentialModePassword
	}
	directInputs := in.DirectCredentials
	if mode == storage.CredentialModeDirect && len(directInputs) == 0 {
		directInputs = []DirectCredentialInput{{GroupName: storage.DirectChannelGroupName, BaseURL: in.SiteURL, APIKey: in.DirectAPIKey, Models: in.DirectModels, Ratio: 1, CompletionRatio: 1}}
	}
	var directRows []storage.ChannelDirectCredential
	directModels := "[]"
	var rawCred string
	var err error
	if mode == storage.CredentialModeDirect {
		if len(directInputs) == 0 || strings.TrimSpace(directInputs[0].APIKey) == "" {
			return nil, errors.New("直连 API 模式下至少要有一条包含 API Key 的分组")
		}
		directRows, directModels, err = s.prepareDirectCredentials(directInputs, nil)
		if err == nil {
			rawCred = strings.TrimSpace(directInputs[0].APIKey)
		}
	} else {
		rawCred, err = selectRawCredential(mode, in.Password, in.TokenCredential, in.DirectAPIKey)
	}
	if err != nil {
		return nil, err
	}
	if err := validateCredential(in.Type, mode, rawCred); err != nil {
		return nil, err
	}
	if in.ProxyEnabled && (in.NetworkProxyID == nil || *in.NetworkProxyID == 0) {
		return nil, errors.New("channel proxy must reference a network proxy")
	}
	networkProxyID, err := s.validateNetworkProxyID(in.NetworkProxyID)
	if err != nil {
		return nil, err
	}
	loginExtraParams := ""
	if mode != storage.CredentialModeDirect {
		loginExtraParams, err = normalizeLoginExtraParams(in.LoginExtraParams)
		if err != nil {
			return nil, err
		}
	}
	siteURL := in.SiteURL
	if mode == storage.CredentialModeDirect {
		if len(directRows) > 0 {
			siteURL = directRows[0].BaseURL
		} else {
			siteURL, err = normalizeDirectBaseURL(in.SiteURL)
		}
		if err != nil {
			return nil, err
		}
	}
	rechargeMultiplier, rechargeMultiplierMode, err := normalizeRechargeMultiplierSettings(
		in.RechargeMultiplier,
		in.RechargeMultiplierMode,
		connector.RechargeMultiplierModeMultiply,
	)
	if err != nil {
		return nil, err
	}
	if mode == storage.CredentialModeDirect {
		rechargeMultiplier = nil
		rechargeMultiplierMode = connector.RechargeMultiplierModeMultiply
	}

	enc := ""
	if mode == storage.CredentialModeDirect {
		enc = directRows[0].APIKeyCipher
	} else {
		enc, err = s.Cipher.Encrypt(rawCred)
		if err != nil {
			return nil, fmt.Errorf("encrypt credential: %w", err)
		}
	}
	c := &storage.Channel{
		Name:                   in.Name,
		Type:                   in.Type,
		GatewayApp:             normalizeGatewayApp(in.GatewayApp),
		SiteURL:                siteURL,
		Username:               in.Username,
		SortOrder:              normalizeSortOrder(in.SortOrder),
		PasswordCipher:         enc,
		CredentialMode:         mode,
		DirectModels:           directModels,
		LoginExtraParams:       loginExtraParams,
		TurnstileEnabled:       in.TurnstileEnabled && mode == storage.CredentialModePassword, // token 模式不需要打码
		IgnoreAnnouncements:    in.IgnoreAnnouncements,
		SubscriptionEnabled:    in.SubscriptionEnabled && mode != storage.CredentialModeDirect,
		ProxyEnabled:           networkProxyID != nil,
		NetworkProxyID:         networkProxyID,
		CaptchaConfigID:        in.CaptchaConfigID,
		BalanceThreshold:       max(in.BalanceThreshold, storage.MinimumChannelBalanceThreshold),
		RechargeMultiplier:     rechargeMultiplier,
		RechargeMultiplierMode: rechargeMultiplierMode,
		MonitorEnabled:         in.MonitorEnabled,
		LatencyProbeEnabled:    true,
	}
	if mode == storage.CredentialModeToken || mode == storage.CredentialModeDirect {
		// token 模式不依赖打码 provider
		c.CaptchaConfigID = nil
	}
	if err := s.Channels.CreateWithDirectCredentials(c, directRows); err != nil {
		return nil, err
	}
	return c, nil
}

// UpdateInput 编辑渠道的可选字段。Password / TokenCredential 为空表示不修改凭据。
type UpdateInput struct {
	ExpectedAdminRevision  *uint64
	Name                   *string
	GatewayApp             *storage.GatewayApp
	SiteURL                *string
	Username               *string
	SortOrder              *int
	Password               *string
	CredentialMode         *storage.CredentialMode
	TokenCredential        *string // JSON
	DirectAPIKey           *string
	DirectModels           *string
	DirectCredentials      *[]DirectCredentialInput
	LoginExtraParams       *string
	TurnstileEnabled       *bool
	IgnoreAnnouncements    *bool
	SubscriptionEnabled    *bool
	ProxyEnabled           *bool
	NetworkProxyID         *uint
	NetworkProxyIDSet      bool
	CaptchaConfigID        *uint
	CaptchaConfigIDSet     bool
	BalanceThreshold       *float64
	RechargeMultiplier     *float64
	RechargeMultiplierSet  bool
	RechargeMultiplierMode *string
	MonitorEnabled         *bool
}

func (s *Service) Update(id uint, in UpdateInput) (*storage.Channel, error) {
	return s.UpdateContext(context.Background(), id, in)
}

// UpdateContext reuses a caller-held channel operation lock during compound
// management-plane transitions. Ordinary callers still acquire the lock here.
func (s *Service) UpdateContext(ctx context.Context, id uint, in UpdateInput) (*storage.Channel, error) {
	if in.ExpectedAdminRevision == nil || *in.ExpectedAdminRevision == 0 {
		return nil, storage.ErrChannelAdminRevisionRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !storage.ChannelOperationLockHeld(ctx, id) {
		unlock, err := s.Channels.LockChannelOperation(ctx, id)
		if err != nil {
			return nil, err
		}
		defer unlock()
	}

	c, err := s.Channels.FindByID(id)
	if err != nil {
		return nil, err
	}
	c.AdminRevision = *in.ExpectedAdminRevision
	fields := make([]storage.ChannelAdminField, 0, 12)
	if in.Name != nil {
		c.Name = *in.Name
		fields = append(fields, storage.ChannelAdminFieldName)
	}
	if in.GatewayApp != nil {
		c.GatewayApp = normalizeGatewayApp(*in.GatewayApp)
		fields = append(fields, storage.ChannelAdminFieldGatewayApp)
	}
	if in.SiteURL != nil {
		if c.SiteURL != *in.SiteURL {
			c.SiteURL = *in.SiteURL
			c.APIURL = ""
		}
		fields = append(fields, storage.ChannelAdminFieldSiteURL, storage.ChannelAdminFieldAPIURL)
	}
	if in.Username != nil {
		c.Username = *in.Username
		fields = append(fields, storage.ChannelAdminFieldUsername)
	}
	if in.SortOrder != nil {
		c.SortOrder = normalizeSortOrder(*in.SortOrder)
		fields = append(fields, storage.ChannelAdminFieldSortOrder)
	}

	// 决定本次更新后的最终凭据模式。
	finalMode := c.CredentialMode
	if in.CredentialMode != nil && *in.CredentialMode != "" {
		finalMode = *in.CredentialMode
	}
	if finalMode == "" {
		finalMode = storage.CredentialModePassword
	}

	// 是否切换了模式 → 强制重写凭据并清空 session
	modeChanged := finalMode != c.CredentialMode
	var directRows []storage.ChannelDirectCredential
	replaceDirectCredentials := c.CredentialMode == storage.CredentialModeDirect && finalMode != storage.CredentialModeDirect
	if finalMode == storage.CredentialModeDirect {
		var directInputs []DirectCredentialInput
		if in.DirectCredentials != nil {
			directInputs = append(directInputs, (*in.DirectCredentials)...)
			replaceDirectCredentials = true
		} else if modeChanged {
			if in.DirectAPIKey == nil || strings.TrimSpace(*in.DirectAPIKey) == "" || in.DirectModels == nil {
				return nil, errors.New("切换到直连 API 模式时必须至少填写一条 API Key、Base URL、模型和倍率配置")
			}
			directInputs = []DirectCredentialInput{{
				GroupName: storage.DirectChannelGroupName, BaseURL: c.SiteURL,
				APIKey: strings.TrimSpace(*in.DirectAPIKey), Models: *in.DirectModels,
				Ratio: 1, CompletionRatio: 1,
			}}
			replaceDirectCredentials = true
		} else if in.DirectAPIKey != nil || in.DirectModels != nil || in.SiteURL != nil {
			existing, listErr := s.Channels.ListDirectCredentials(id)
			if listErr != nil {
				return nil, listErr
			}
			if len(existing) == 0 {
				return nil, errors.New("直连配置缺失，请刷新页面后重新保存")
			}
			for _, item := range existing {
				enabled := item.Enabled
				directInputs = append(directInputs, DirectCredentialInput{
					ID: item.ID, GroupName: item.GroupName, BaseURL: item.BaseURL, Models: item.Models,
					Ratio: item.Ratio, CompletionRatio: item.CompletionRatio, Enabled: &enabled,
				})
			}
			target := 0
			for index := range directInputs {
				if strings.EqualFold(strings.TrimSpace(directInputs[index].GroupName), storage.DirectChannelGroupName) {
					target = index
					break
				}
			}
			if in.DirectAPIKey != nil {
				directInputs[target].APIKey = strings.TrimSpace(*in.DirectAPIKey)
			}
			if in.DirectModels != nil {
				directInputs[target].Models = *in.DirectModels
			}
			if in.SiteURL != nil {
				directInputs[target].BaseURL = *in.SiteURL
			}
			replaceDirectCredentials = true
		}
		if replaceDirectCredentials {
			existing := []storage.ChannelDirectCredential(nil)
			if c.CredentialMode == storage.CredentialModeDirect {
				existing, err = s.Channels.ListDirectCredentials(id)
				if err != nil {
					return nil, err
				}
			}
			var aggregateModels string
			directRows, aggregateModels, err = s.prepareDirectCredentials(directInputs, existing)
			if err != nil {
				return nil, err
			}
			c.SiteURL = directRows[0].BaseURL
			c.APIURL = ""
			c.PasswordCipher = directRows[0].APIKeyCipher
			c.CredentialMode = storage.CredentialModeDirect
			c.DirectModels = aggregateModels
			fields = append(fields,
				storage.ChannelAdminFieldSiteURL,
				storage.ChannelAdminFieldAPIURL,
				storage.ChannelAdminFieldPasswordCipher,
				storage.ChannelAdminFieldCredentialMode,
				storage.ChannelAdminFieldDirectModels,
			)
		}
	}

	var rawCred string
	switch finalMode {
	case storage.CredentialModePassword:
		if in.Password != nil && *in.Password != "" {
			rawCred = *in.Password
		} else if modeChanged {
			return nil, errors.New("切换到账号密码模式时必须填写密码")
		}
	case storage.CredentialModeToken:
		if in.TokenCredential != nil && *in.TokenCredential != "" {
			rawCred = *in.TokenCredential
		} else if modeChanged {
			return nil, errors.New("切换到 token 模式时必须填写凭据")
		}
	case storage.CredentialModeDirect:
		// Direct credentials were normalized above as a collection.
	default:
		return nil, fmt.Errorf("unknown credential mode: %s", finalMode)
	}

	if rawCred != "" {
		if err := validateCredential(c.Type, finalMode, rawCred); err != nil {
			return nil, err
		}
		enc, err := s.Cipher.Encrypt(rawCred)
		if err != nil {
			return nil, fmt.Errorf("encrypt credential: %w", err)
		}
		c.PasswordCipher = enc
		c.CredentialMode = finalMode
		fields = append(fields, storage.ChannelAdminFieldPasswordCipher, storage.ChannelAdminFieldCredentialMode)
	} else if modeChanged && finalMode != storage.CredentialModeDirect {
		// 理论上面已挡住，这里兜底
		return nil, errors.New("凭据模式变更必须同时提供新凭据")
	}
	if finalMode == storage.CredentialModeDirect && !replaceDirectCredentials {
		c.SiteURL, err = normalizeDirectBaseURL(c.SiteURL)
		if err != nil {
			return nil, err
		}
		c.APIURL = ""
		fields = append(fields, storage.ChannelAdminFieldSiteURL, storage.ChannelAdminFieldAPIURL)
	} else if modeChanged && finalMode != storage.CredentialModeDirect {
		c.DirectModels = "[]"
		fields = append(fields, storage.ChannelAdminFieldDirectModels)
	}
	if finalMode == storage.CredentialModeDirect {
		c.LoginExtraParams = ""
		fields = append(fields, storage.ChannelAdminFieldLoginExtraParams)
	} else if in.LoginExtraParams != nil {
		loginExtraParams, err := normalizeLoginExtraParams(*in.LoginExtraParams)
		if err != nil {
			return nil, err
		}
		if loginExtraParams != c.LoginExtraParams {
			c.LoginExtraParams = loginExtraParams
		}
		fields = append(fields, storage.ChannelAdminFieldLoginExtraParams)
	}

	if in.TurnstileEnabled != nil {
		c.TurnstileEnabled = *in.TurnstileEnabled && finalMode == storage.CredentialModePassword
		fields = append(fields, storage.ChannelAdminFieldTurnstileEnabled)
	}
	if in.IgnoreAnnouncements != nil {
		c.IgnoreAnnouncements = *in.IgnoreAnnouncements
		fields = append(fields, storage.ChannelAdminFieldIgnoreAnnouncements)
	}
	if in.SubscriptionEnabled != nil {
		c.SubscriptionEnabled = *in.SubscriptionEnabled && finalMode != storage.CredentialModeDirect
		fields = append(fields, storage.ChannelAdminFieldSubscriptionEnabled)
	}
	if in.NetworkProxyID != nil || in.NetworkProxyIDSet {
		networkProxyID, err := s.validateNetworkProxyID(in.NetworkProxyID)
		if err != nil {
			return nil, err
		}
		c.NetworkProxyID = networkProxyID
		c.ProxyEnabled = networkProxyID != nil
		fields = append(fields, storage.ChannelAdminFieldNetworkProxyID, storage.ChannelAdminFieldProxyEnabled)
	} else if in.ProxyEnabled != nil {
		if *in.ProxyEnabled {
			return nil, errors.New("channel proxy must reference a network proxy")
		}
		c.ProxyEnabled = false
		c.NetworkProxyID = nil
		fields = append(fields, storage.ChannelAdminFieldNetworkProxyID, storage.ChannelAdminFieldProxyEnabled)
	}
	if in.CaptchaConfigID != nil || in.CaptchaConfigIDSet {
		if finalMode == storage.CredentialModePassword {
			c.CaptchaConfigID = in.CaptchaConfigID
		} else {
			c.CaptchaConfigID = nil
		}
		fields = append(fields, storage.ChannelAdminFieldCaptchaConfigID)
	} else if finalMode == storage.CredentialModeToken || finalMode == storage.CredentialModeDirect {
		// token 模式强制清空打码绑定
		c.CaptchaConfigID = nil
		fields = append(fields, storage.ChannelAdminFieldCaptchaConfigID)
	}
	if in.BalanceThreshold != nil {
		c.BalanceThreshold = max(*in.BalanceThreshold, storage.MinimumChannelBalanceThreshold)
		fields = append(fields, storage.ChannelAdminFieldBalanceThreshold)
	}
	if finalMode != storage.CredentialModeDirect && (in.RechargeMultiplier != nil || in.RechargeMultiplierSet) {
		value, _, err := normalizeRechargeMultiplierSettings(in.RechargeMultiplier, c.RechargeMultiplierMode, c.RechargeMultiplierMode)
		if err != nil {
			return nil, err
		}
		c.RechargeMultiplier = value
		fields = append(fields, storage.ChannelAdminFieldRechargeMultiplier)
	}
	if finalMode != storage.CredentialModeDirect && in.RechargeMultiplierMode != nil {
		mode, err := normalizeRechargeMultiplierMode(*in.RechargeMultiplierMode, "")
		if err != nil {
			return nil, err
		}
		c.RechargeMultiplierMode = mode
		fields = append(fields, storage.ChannelAdminFieldRechargeMultiplierMode)
	}
	if finalMode == storage.CredentialModeDirect {
		c.RechargeMultiplier = nil
		c.RechargeMultiplierMode = connector.RechargeMultiplierModeMultiply
		fields = append(fields, storage.ChannelAdminFieldRechargeMultiplier, storage.ChannelAdminFieldRechargeMultiplierMode)
	}
	if in.MonitorEnabled != nil {
		c.MonitorEnabled = *in.MonitorEnabled
		fields = append(fields, storage.ChannelAdminFieldMonitorEnabled)
	}
	if replaceDirectCredentials {
		if err := s.Channels.UpdateAdminFieldsWithDirectCredentials(c, directRows, fields...); err != nil {
			return nil, err
		}
	} else if err := s.Channels.UpdateAdminFields(c, fields...); err != nil {
		return nil, err
	}
	return c, nil
}

func normalizeRechargeMultiplierSettings(value *float64, mode string, defaultMode string) (*float64, string, error) {
	normalizedMode, err := normalizeRechargeMultiplierMode(mode, defaultMode)
	if err != nil {
		return nil, "", err
	}
	if value == nil {
		return nil, normalizedMode, nil
	}
	if *value <= 0 || math.IsNaN(*value) || math.IsInf(*value, 0) {
		return nil, "", errors.New("充值倍率必须是大于 0 的有限数值")
	}
	copyValue := *value
	return &copyValue, normalizedMode, nil
}

func normalizeRechargeMultiplierMode(mode string, defaultMode string) (string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = strings.ToLower(strings.TrimSpace(defaultMode))
	}
	switch mode {
	case connector.RechargeMultiplierModeDivide, connector.RechargeMultiplierModeMultiply:
		return mode, nil
	default:
		return "", errors.New("充值倍率模式必须是 divide 或 multiply")
	}
}

func normalizeSortOrder(v int) int {
	if v == 0 {
		return 1
	}
	return v
}

func normalizeGatewayApp(app storage.GatewayApp) storage.GatewayApp {
	switch storage.GatewayApp(strings.ToLower(strings.TrimSpace(string(app)))) {
	case storage.GatewayAppClaude:
		return storage.GatewayAppClaude
	case storage.GatewayAppGemini:
		return storage.GatewayAppGemini
	default:
		return storage.GatewayAppCodex
	}
}

func normalizeLoginExtraParams(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return "", fmt.Errorf("解析附加表单参数 JSON 失败：%w", err)
	}
	if obj == nil {
		return "", errors.New("附加表单参数必须是 JSON 对象")
	}
	return raw, nil
}

func normalizeDirectModels(raw string) ([]string, error) {
	return storage.ParseDirectModels(strings.TrimSpace(raw))
}

func (s *Service) prepareDirectCredentials(inputs []DirectCredentialInput, existing []storage.ChannelDirectCredential) ([]storage.ChannelDirectCredential, string, error) {
	if s == nil || s.Cipher == nil {
		return nil, "", errors.New("直连 API 凭据加密服务不可用")
	}
	if len(inputs) == 0 {
		return nil, "", errors.New("直连 API 模式至少需要一个分组")
	}
	existingByGroup := make(map[string]storage.ChannelDirectCredential, len(existing))
	existingByID := make(map[uint]storage.ChannelDirectCredential, len(existing))
	for _, item := range existing {
		existingByGroup[strings.ToLower(strings.TrimSpace(item.GroupName))] = item
		existingByID[item.ID] = item
	}
	seen := make(map[string]struct{}, len(inputs))
	prepared := make([]storage.ChannelDirectCredential, 0, len(inputs))
	allModels := make([]string, 0)
	allModelSet := make(map[string]struct{})
	for _, input := range inputs {
		groupName := strings.TrimSpace(input.GroupName)
		if groupName == "" || len(groupName) > 128 {
			return nil, "", errors.New("直连分组名不能为空且不能超过 128 个字符")
		}
		if strings.HasPrefix(strings.ToLower(groupName), storage.ReservedInternalUpstreamGroupPrefix) {
			return nil, "", errors.New("直连分组名使用了系统保留前缀")
		}
		canonical := strings.ToLower(groupName)
		if _, duplicate := seen[canonical]; duplicate {
			return nil, "", fmt.Errorf("直连分组 %q 重复", groupName)
		}
		seen[canonical] = struct{}{}
		baseURL, err := normalizeDirectBaseURL(input.BaseURL)
		if err != nil {
			return nil, "", fmt.Errorf("直连分组 %q：%w", groupName, err)
		}
		models, err := normalizeDirectModels(input.Models)
		if err != nil {
			return nil, "", fmt.Errorf("直连分组 %q：%w", groupName, err)
		}
		ratio := input.Ratio
		completionRatio := input.CompletionRatio
		if ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) || completionRatio <= 0 || math.IsNaN(completionRatio) || math.IsInf(completionRatio, 0) {
			return nil, "", fmt.Errorf("直连分组 %q 的倍率必须是大于 0 的有限数值", groupName)
		}
		enabled := true
		if input.Enabled != nil {
			enabled = *input.Enabled
		}
		old, found := existingByGroup[canonical]
		if input.ID != 0 {
			var idFound bool
			old, idFound = existingByID[input.ID]
			if !idFound {
				return nil, "", fmt.Errorf("直连配置 #%d 不属于当前渠道", input.ID)
			}
			found = true
		}
		cipherText := ""
		prefix := ""
		if strings.TrimSpace(input.APIKey) != "" {
			plain := strings.TrimSpace(input.APIKey)
			cipherText, err = s.Cipher.Encrypt(plain)
			if err != nil {
				return nil, "", fmt.Errorf("加密直连 API Key：%w", err)
			}
			prefix = directAPIKeyPrefix(plain)
		} else if found {
			cipherText = old.APIKeyCipher
			prefix = old.APIKeyPrefix
		} else {
			return nil, "", fmt.Errorf("新增直连分组 %q 必须填写 API Key", groupName)
		}
		if strings.TrimSpace(cipherText) == "" {
			return nil, "", fmt.Errorf("直连分组 %q 没有可用 API Key", groupName)
		}
		revision := uint64(1)
		if found && old.Revision > 0 {
			revision = old.Revision
		}
		encodedModels := storage.EncodeDirectModels(models)
		prepared = append(prepared, storage.ChannelDirectCredential{
			ID: old.ID, GroupName: groupName, BaseURL: baseURL, APIKeyCipher: cipherText, APIKeyPrefix: prefix,
			Models: encodedModels, Ratio: ratio, CompletionRatio: completionRatio, Enabled: enabled, Revision: revision,
		})
		for _, model := range models {
			if _, ok := allModelSet[model]; !ok {
				allModelSet[model] = struct{}{}
				allModels = append(allModels, model)
			}
		}
	}
	return prepared, storage.EncodeDirectModels(allModels), nil
}

func directAPIKeyPrefix(key string) string {
	key = strings.TrimSpace(key)
	if len(key) <= 12 {
		return key
	}
	return key[:12]
}

// ValidateDirectConfiguration performs every deterministic direct-mode check
// that must succeed before managed provider keys are retired.
func (s *Service) ValidateDirectConfiguration(baseURL, directModels string, proxyID *uint) error {
	if s == nil {
		return errors.New("直连渠道配置校验不可用")
	}
	if _, err := normalizeDirectBaseURL(baseURL); err != nil {
		return err
	}
	if _, err := normalizeDirectModels(directModels); err != nil {
		return err
	}
	_, err := s.validateNetworkProxyID(proxyID)
	return err
}

func normalizeDirectBaseURL(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", errors.New("直连 API 模式需要有效的 Base URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("直连 API Base URL 只支持 http 或 https")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	if strings.EqualFold(path.Base(u.Path), "v1") {
		u.Path = strings.TrimSuffix(u.Path, "/"+path.Base(u.Path))
	}
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/"), nil
}

func parseLoginExtraParams(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, fmt.Errorf("解析附加表单参数 JSON 失败：%w", err)
	}
	if obj == nil {
		return nil, errors.New("附加表单参数必须是 JSON 对象")
	}
	return obj, nil
}

// selectRawCredential 在 Create 时根据 mode 决定要落库的明文凭据字符串。
func selectRawCredential(mode storage.CredentialMode, password, tokenCredential, directAPIKey string) (string, error) {
	switch mode {
	case storage.CredentialModePassword:
		if password == "" {
			return "", errors.New("账号密码模式下密码不能为空")
		}
		return password, nil
	case storage.CredentialModeToken:
		if tokenCredential == "" {
			return "", errors.New("token 模式下必须提供凭据")
		}
		return tokenCredential, nil
	case storage.CredentialModeDirect:
		if strings.TrimSpace(directAPIKey) == "" {
			return "", errors.New("直连 API 模式下 API Key 不能为空")
		}
		return strings.TrimSpace(directAPIKey), nil
	default:
		return "", fmt.Errorf("unknown credential mode: %s", mode)
	}
}

// validateCredential 在保存前对凭据做语法 / 必填字段校验，能尽早把无效输入挡在 connector 外。
//
// 注意：这里只做语法层校验，不做"凭据是否真的有效"的网络验证——
// 那个交给后续 TestLogin / 第一次同步去发现。
func validateCredential(channelType storage.ChannelType, mode storage.CredentialMode, raw string) error {
	if mode == storage.CredentialModeDirect {
		if strings.TrimSpace(raw) == "" {
			return errors.New("直连 API 模式下 API Key 不能为空")
		}
		return nil
	}
	if mode != storage.CredentialModeToken {
		return nil
	}
	switch channelType {
	case storage.ChannelTypeNewAPI:
		var cred NewAPITokenCredential
		if err := json.Unmarshal([]byte(raw), &cred); err != nil {
			return fmt.Errorf("解析 NewAPI 凭据 JSON 失败：%w", err)
		}
		cookie := strings.TrimSpace(cred.Cookie)
		accessToken := strings.TrimSpace(cred.AccessToken)
		if cookie == "" && accessToken == "" {
			return errors.New("NewAPI token 模式需要 Cookie 或系统访问令牌（二选一）")
		}
		if cookie != "" && accessToken != "" {
			return errors.New("NewAPI token 模式 Cookie 与系统访问令牌只能二选一")
		}
		if strings.TrimSpace(cred.UserID) == "" {
			return errors.New("NewAPI token 模式需要 User ID（在 NewAPI 个人设置页查看）")
		}
	case storage.ChannelTypeSub2API:
		var cred Sub2APITokenCredential
		if err := json.Unmarshal([]byte(raw), &cred); err != nil {
			return fmt.Errorf("解析 Sub2API 凭据 JSON 失败：%w", err)
		}
		if strings.TrimSpace(cred.AccessToken) == "" {
			return errors.New("Sub2API token 模式需要 access_token")
		}
	default:
		return fmt.Errorf("unknown channel type: %s", channelType)
	}
	return nil
}

func (s *Service) Delete(ctx context.Context, id uint) error {
	return s.DeleteContext(ctx, id)
}

// DeleteContext performs deletion preparation while reusing a caller-held
// channel operation lock during compound gateway cleanup.
func (s *Service) DeleteContext(ctx context.Context, id uint) error {
	if s == nil || s.Channels == nil || id == 0 {
		return errors.New("channel deletion is unavailable")
	}
	if ctx == nil {
		return errors.New("channel deletion context is required")
	}
	if !storage.ChannelOperationLockHeld(ctx, id) {
		unlock, err := s.Channels.LockChannelOperation(ctx, id)
		if err != nil {
			return err
		}
		defer unlock()
	}
	// Whole-channel deletion is now a durable remote-cleanup prepare step.
	// Keep the local session and all other evidence until the identity/epoch
	// fenced finalizer has confirmed every managed remote key. In particular,
	// never discard an AuthSession before that coordinator can revalidate it.
	return s.Channels.DeleteContext(storage.WithChannelOperationLockHeld(ctx, id), id)
}

// ClearLoginInfo 清空渠道当前保存的登录信息。
//
// password 模式：只删除登录后缓存的 AuthSession（access_token / refresh_token / cookie / csrf）。
// token 模式：同时清空用户直接保存的 token/cookie JSON，避免继续复用旧凭据。
func (s *Service) ClearLoginInfo(id uint) (*storage.Channel, error) {
	return s.ClearLoginInfoContext(context.Background(), id)
}

// ClearLoginInfoContext reuses a caller-held channel operation lock when
// direct routing cleanup and credential mutation must be atomic together.
func (s *Service) ClearLoginInfoContext(ctx context.Context, id uint) (*storage.Channel, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !storage.ChannelOperationLockHeld(ctx, id) {
		unlock, err := s.Channels.LockChannelOperation(ctx, id)
		if err != nil {
			return nil, err
		}
		defer unlock()
	}

	c, err := s.Channels.FindByID(id)
	if err != nil {
		return nil, err
	}
	if c.CredentialMode == storage.CredentialModeDirect {
		c.LastError = "直连 API Key 已清空，请重新编辑渠道填写凭据"
		if err := s.Channels.InvalidateAdminDirectAuthState(c); err != nil {
			return nil, err
		}
		if err := s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, c.LastError); err != nil {
			return nil, err
		}
		return c, nil
	}
	if c.CredentialMode == storage.CredentialModeToken {
		c.PasswordCipher = ""
		c.LastError = ""
		if err := s.Channels.UpdateAdminFields(c, storage.ChannelAdminFieldPasswordCipher); err != nil {
			return nil, err
		}
		if err := s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, c.LastError); err != nil {
			return nil, err
		}
		return c, nil
	}
	if err := s.Channels.InvalidateAdminAuthState(c); err != nil {
		return nil, err
	}
	c.LastError = ""
	if err := s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, ""); err != nil {
		return nil, err
	}
	return c, nil
}

// Resolve 把存储层的加密渠道解密成 connector 可用的 Channel。
//
// 注意：这一步**不**求解 Turnstile —— 打码只在真正要登录时做（见 prepareTurnstile），
// 复用现有 session 的路径无需任何打码消耗。
//
// token 模式下 connector.Channel.Password 留空——connector 永远不会读到它。
func (s *Service) Resolve(ctx context.Context, c *storage.Channel) (*connector.Channel, error) {
	_ = ctx
	raw, err := s.Cipher.Decrypt(c.PasswordCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt credential: %w", err)
	}
	apiURL := strings.TrimSpace(c.APIURL)
	apiOriginKnown := apiURL != ""
	if c.CredentialMode == storage.CredentialModeDirect {
		apiURL = strings.TrimSpace(c.SiteURL)
		apiOriginKnown = true
	}
	if apiURL == "" {
		apiURL = c.SiteURL
	}
	resolved := &connector.Channel{
		ID:                     c.ID,
		Name:                   c.Name,
		Type:                   connector.ChannelType(c.Type),
		SiteURL:                apiURL,
		HomepageURL:            c.SiteURL,
		APIOriginKnown:         apiOriginKnown,
		Username:               c.Username,
		LoginExtraParams:       nil,
		TurnstileEnabled:       c.TurnstileEnabled,
		TimeZone:               s.UserTime.Name(),
		RechargeMultiplier:     c.RechargeMultiplier,
		RechargeMultiplierMode: connector.NormalizeRechargeMultiplierMode(c.RechargeMultiplierMode),
	}
	loginExtraParams, err := parseLoginExtraParams(c.LoginExtraParams)
	if err != nil {
		return nil, err
	}
	resolved.LoginExtraParams = loginExtraParams
	if c.NetworkProxyID != nil {
		proxyURL, proxyCacheKey, err := s.networkProxyURLContext(ctx, c)
		if err != nil {
			return nil, err
		}
		resolved.ProxyURL = proxyURL
		resolved.ProxyCacheKey = proxyCacheKey
	} else if c.ProxyEnabled {
		return nil, errors.New("channel has an unmigrated legacy proxy setting; bind a network proxy before use")
	}
	if c.CredentialMode == storage.CredentialModeToken {
		// token 模式：raw 是 JSON，Password 留空避免被 connector 误用
		resolved.Password = ""
	} else {
		resolved.Password = raw
	}
	return resolved, nil
}

// buildSessionFromToken 在 token 模式下，把用户提供的凭据 JSON 解析成 AuthSession。
// 不发任何 HTTP 请求——失效检测留给 connector.CheckAuth + 后续 GetBalance / GetRates。
func (s *Service) buildSessionFromToken(c *storage.Channel) (*connector.AuthSession, error) {
	raw, err := s.Cipher.Decrypt(c.PasswordCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt credential: %w", err)
	}
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("登录信息已清空，请重新编辑渠道填写凭据")
	}
	switch c.Type {
	case storage.ChannelTypeNewAPI:
		var cred NewAPITokenCredential
		if err := json.Unmarshal([]byte(raw), &cred); err != nil {
			return nil, fmt.Errorf("parse newapi token credential: %w", err)
		}
		return &connector.AuthSession{
			UserID:      cred.UserID,
			Cookie:      cred.Cookie,
			AccessToken: cred.AccessToken,
			ExpiresAt:   time.Now().Add(tokenSessionTTL),
		}, nil
	case storage.ChannelTypeSub2API:
		var cred Sub2APITokenCredential
		if err := json.Unmarshal([]byte(raw), &cred); err != nil {
			return nil, fmt.Errorf("parse sub2api token credential: %w", err)
		}
		return &connector.AuthSession{
			AccessToken:  strings.TrimSpace(cred.AccessToken),
			RefreshToken: strings.TrimSpace(cred.RefreshToken),
			ExpiresAt:    time.Now().Add(tokenSessionTTL),
		}, nil
	default:
		return nil, fmt.Errorf("unknown channel type: %s", c.Type)
	}
}

// prepareTurnstile 在调用 conn.Login 之前求解 Turnstile token。
// 没启用 turnstile 或者上游 site 公开接口说"未开启 Turnstile"时是空操作。
func (s *Service) prepareTurnstile(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
) error {
	// Sub2API exposes its Turnstile state through a public endpoint. Check it
	// before password login so a channel without a solver does not make a
	// login request that is guaranteed to fail.
	if c.Type == storage.ChannelTypeSub2API {
		progress.Start(ctx, progress.StageCaptcha, "检测上游 Turnstile…")
		siteKey, err := conn.GetTurnstileSiteKey(ctx, resolved)
		if err != nil {
			if errors.Is(err, connector.ErrTurnstileSiteKeyMissing) {
				actionable := errors.New("上游已开启 Cloudflare Turnstile，但没有返回 site key；请改用 Token（手动登录接管）")
				progress.Fail(ctx, progress.StageCaptcha, actionable.Error())
				return actionable
			}
			if !c.TurnstileEnabled {
				progress.OK(ctx, progress.StageCaptcha, "无法读取 Turnstile 配置，继续登录")
				return nil
			}
			progress.Fail(ctx, progress.StageCaptcha, err.Error())
			return fmt.Errorf("fetch turnstile site key: %w", err)
		}
		if siteKey == "" {
			progress.OK(ctx, progress.StageCaptcha, "上游未开启 Turnstile，跳过")
			return nil
		}
		if !c.TurnstileEnabled || c.CaptchaConfigID == nil {
			err := errors.New("上游已开启 Cloudflare Turnstile，当前渠道未配置解题服务；免费方案：在上游网页手动登录一次，然后编辑渠道，选择 Token（手动登录接管），粘贴 access_token 和 refresh_token")
			progress.Fail(ctx, progress.StageCaptcha, err.Error())
			return err
		}
		progress.Start(ctx, progress.StageCaptcha, "求解 Turnstile…")
		return s.solveTurnstile(ctx, c, resolved, siteKey)
	}

	if !c.TurnstileEnabled || c.CaptchaConfigID == nil {
		return nil
	}
	progress.Start(ctx, progress.StageCaptcha, "求解 Turnstile…")
	siteKey, err := conn.GetTurnstileSiteKey(ctx, resolved)
	if err != nil {
		progress.Fail(ctx, progress.StageCaptcha, err.Error())
		return fmt.Errorf("fetch turnstile site key: %w", err)
	}
	if siteKey == "" {
		progress.OK(ctx, progress.StageCaptcha, "上游未开启 Turnstile，跳过")
		return nil
	}
	return s.solveTurnstile(ctx, c, resolved, siteKey)
}

func (s *Service) solveTurnstile(ctx context.Context, c *storage.Channel, resolved *connector.Channel, siteKey string) error {
	token, err := s.solveCaptcha(ctx, *c.CaptchaConfigID, siteKey, c.SiteURL, resolved.ProxyURL)
	if err != nil {
		progress.Fail(ctx, progress.StageCaptcha, err.Error())
		return fmt.Errorf("solve captcha: %w", err)
	}
	resolved.TurnstileToken = token
	progress.OK(ctx, progress.StageCaptcha, "打码完成")
	return nil
}

func (s *Service) solveCaptcha(ctx context.Context, captchaID uint, siteKey, pageURL, channelProxyURL string) (string, error) {
	cfg, err := s.Captchas.WithContext(ctx).FindByID(captchaID)
	if err != nil {
		return "", err
	}
	if !cfg.Enabled {
		return "", errors.New("captcha config disabled")
	}
	apiKey, err := s.Cipher.Decrypt(cfg.APIKeyCipher)
	if err != nil {
		return "", err
	}
	provider, err := captcha.BuildWithProxy(cfg, apiKey, channelProxyURL)
	if err != nil {
		return "", err
	}
	return provider.SolveTurnstile(ctx, siteKey, pageURL)
}

// EnsureSession 优先复用未过期的 session，否则重新登录并加密回写。
//
// token 模式：
//   - 跳过 AuthSessions 表与 Login 调用
//   - 每次构造一个临时 AuthSession（基于用户提供的凭据）返回
//   - CheckAuth 用来发现 token 是否还有效；鉴权失效会在 last_error 显示，余额停用原样返回
func (s *Service) EnsureSession(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
) (*connector.AuthSession, error) {
	session, sessionErr := s.ensureSession(ctx, c, resolved, conn, true)
	persistErr := s.persistDiscoveredAPIURL(ctx, c, resolved)
	if sessionErr != nil {
		return nil, sessionErr
	}
	if persistErr != nil {
		return nil, persistErr
	}
	if session == nil {
		return nil, errors.New("upstream session is empty")
	}
	if err := s.PersistAccountConcurrencyFactWithContext(ctx, c, resolved, session.AccountConcurrency); err != nil {
		return nil, fmt.Errorf("persist upstream account concurrency fact: %w", err)
	}
	if err := s.Channels.WithContext(ctx).RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision); err != nil {
		return nil, err
	}
	return session, nil
}

// EnsureSessionWithoutLogin 只复用或刷新已有会话，不会进入登录流程。
// 监控采集失败后的补偿路径使用此入口，避免 password 模式因一次上游异常触发登录。
func (s *Service) EnsureSessionWithoutLogin(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
) (*connector.AuthSession, error) {
	session, sessionErr := s.ensureSession(ctx, c, resolved, conn, false)
	persistErr := s.persistDiscoveredAPIURL(ctx, c, resolved)
	if sessionErr != nil {
		return nil, sessionErr
	}
	if persistErr != nil {
		return nil, persistErr
	}
	if session == nil {
		return nil, errors.New("upstream session is empty")
	}
	if err := s.PersistAccountConcurrencyFactWithContext(ctx, c, resolved, session.AccountConcurrency); err != nil {
		return nil, fmt.Errorf("persist upstream account concurrency fact: %w", err)
	}
	if err := s.Channels.WithContext(ctx).RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision); err != nil {
		return nil, err
	}
	return session, nil
}

// PersistAccountConcurrencyFact stores a provider-authenticated account
// identity and its optional explicit limit. Raw account IDs and origins are
// hashed by storage and are never returned through channel APIs or logs.
func (s *Service) PersistAccountConcurrencyFact(c *storage.Channel, resolved *connector.Channel, fact *connector.AccountConcurrencyFact) error {
	return s.PersistAccountConcurrencyFactWithContext(context.Background(), c, resolved, fact)
}

func (s *Service) PersistAccountConcurrencyFactWithContext(ctx context.Context, c *storage.Channel, resolved *connector.Channel, fact *connector.AccountConcurrencyFact) error {
	return s.PersistAccountIdentityFactWithContext(ctx, c, resolved, fact)
}

// PersistAccountIdentityFact hashes provider-authenticated account identity
// material inside storage. Callers must never log the raw account ID.
func (s *Service) PersistAccountIdentityFact(c *storage.Channel, resolved *connector.Channel, fact *connector.AccountConcurrencyFact) error {
	return s.PersistAccountIdentityFactWithContext(context.Background(), c, resolved, fact)
}

func (s *Service) PersistAccountIdentityFactWithContext(ctx context.Context, c *storage.Channel, resolved *connector.Channel, fact *connector.AccountConcurrencyFact) error {
	if fact == nil {
		return nil
	}
	if s == nil || s.Channels == nil || c == nil || resolved == nil || c.ID == 0 || c.UpstreamAccountIdentityRevision == 0 {
		return errors.New("channel account identity is unavailable")
	}
	if fact.Provider != connector.TypeSub2API || c.Type != storage.ChannelTypeSub2API {
		return errors.New("upstream account identity provider does not match channel")
	}
	channels := s.Channels.WithContext(ctx)
	snapshot, err := channels.ObserveUpstreamAccountIdentity(storage.UpstreamAccountIdentityObservation{
		ChannelID: c.ID, ExpectedIdentityRevision: c.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: c.AdminRevision, Provider: storage.ChannelType(fact.Provider),
		APIOrigin: resolved.SiteURL, AccountID: fact.AccountID, Limit: fact.Limit,
		Scope: storage.UpstreamConcurrencyScopeAccount, Source: fact.Source, ObservedAt: fact.ObservedAt,
	})
	if err != nil {
		if errors.Is(err, storage.ErrStaleUpstreamAccountObservation) {
			return errors.Join(storage.ErrStaleUpstreamAccountConcurrencyObservation, err)
		}
		return err
	}
	if snapshot == nil || snapshot.ChannelIdentityRevision == 0 {
		return errors.New("persisted upstream account identity is unavailable")
	}
	if snapshot.ChannelIdentityRevision != c.UpstreamAccountIdentityRevision {
		current, reloadErr := channels.FindByID(c.ID)
		if reloadErr != nil {
			return reloadErr
		}
		c.UpstreamAccountIdentityRevision = current.UpstreamAccountIdentityRevision
		c.CostUnitEpoch = current.CostUnitEpoch
	}
	return nil
}

func accountIdentityFact(session *connector.AuthSession) *connector.AccountConcurrencyFact {
	if session == nil {
		return nil
	}
	return session.AccountConcurrency
}

func (s *Service) persistDiscoveredAPIURL(ctx context.Context, c *storage.Channel, resolved *connector.Channel) error {
	if c == nil || resolved == nil || c.ID == 0 || c.Type != storage.ChannelTypeSub2API {
		return nil
	}
	previous := strings.TrimRight(strings.TrimSpace(c.APIURL), "/")
	discovered := strings.TrimRight(strings.TrimSpace(resolved.SiteURL), "/")
	if discovered == "" || previous == discovered {
		return nil
	}
	channels := s.Channels.WithContext(ctx)
	if !storage.ChannelOperationLockHeld(ctx, c.ID) {
		unlock, err := channels.LockChannelOperation(ctx, c.ID)
		if err != nil {
			return fmt.Errorf("锁定自动识别的 API 地址更新失败：%w", err)
		}
		defer unlock()
		ctx = storage.WithChannelOperationLockHeld(ctx, c.ID)
	}
	updated, revision, err := channels.UpdateAPIURLRevisionIfUnchanged(c.ID, c.UpstreamAccountIdentityRevision, c.SiteURL, c.APIURL, discovered)
	if err != nil {
		return fmt.Errorf("保存自动识别的 API 地址失败：%w", err)
	}
	if updated {
		current, reloadErr := channels.FindByID(c.ID)
		if reloadErr != nil {
			return fmt.Errorf("重新读取渠道身份失败：%w", reloadErr)
		}
		c.APIURL = current.APIURL
		c.UpstreamAccountIdentityRevision = current.UpstreamAccountIdentityRevision
		c.CostUnitEpoch = current.CostUnitEpoch
		_ = revision
		progress.OK(ctx, progress.StageDiscovery, "已自动识别并保存 API 地址", map[string]string{"api_url": discovered})
	}
	return nil
}

func (s *Service) ensureSession(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
	allowLogin bool,
) (*connector.AuthSession, error) {
	if c != nil && c.CredentialMode == storage.CredentialModeDirect {
		return nil, errors.New("直连 API 渠道不提供后台登录会话")
	}
	channels := s.Channels.WithContext(ctx)
	if c.CredentialMode == storage.CredentialModeToken {
		progress.Start(ctx, progress.StageSession, "使用用户提供的 token…")
		session, err := s.buildSessionFromToken(c)
		if err != nil {
			progress.Fail(ctx, progress.StageSession, err.Error())
			_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, err.Error())
			return nil, err
		}
		// 走一次 CheckAuth 确认 token 仍有效。失败时如果有 refresh_token，先尝试刷新并回写。
		if err := conn.CheckAuth(ctx, resolved, session); err != nil {
			if connector.IsUpstreamChallengeHTTPError(err) {
				return nil, s.failProviderChallenge(ctx, c, channels, progress.StageSession)
			}
			if connector.IsAccountBalanceHTTPError(err) {
				progress.Fail(ctx, progress.StageSession, err.Error())
				return nil, err
			}
			if !connector.IsAuthHTTPError(err) {
				progress.Fail(ctx, progress.StageSession, err.Error())
				_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, err.Error())
				return nil, err
			}
			if refreshed, ok, refreshErr := s.refreshProvidedTokenSession(ctx, c, resolved, conn, session); refreshErr != nil {
				if connector.IsUpstreamChallengeHTTPError(refreshErr) {
					return nil, s.failProviderChallenge(ctx, c, channels, progress.StageSession)
				}
				progress.Fail(ctx, progress.StageSession, refreshErr.Error())
				if !connector.IsAccountBalanceHTTPError(refreshErr) {
					_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, refreshErr.Error())
				}
				return nil, refreshErr
			} else if ok {
				return refreshed, nil
			}
			msg := "token 已失效，请重新粘贴凭据：" + err.Error()
			progress.Fail(ctx, progress.StageSession, msg)
			_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, msg)
			return nil, errors.New(msg)
		}
		_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
		progress.OK(ctx, progress.StageSession, "token 有效，跳过登录")
		return session, nil
	}

	saved, err := s.AuthSessions.WithContext(ctx).FindByChannel(c.ID)
	if err != nil {
		return nil, err
	}
	var checkErr error
	if saved != nil {
		session, err := s.decryptSession(saved)
		if err != nil {
			return nil, err
		}
		if saved.ExpiresAt != nil && time.Until(*saved.ExpiresAt) > SessionRefreshThreshold {
			// 轻量校验现有 session，不通过则继续尝试 refresh_token / 重新登录。
			progress.Start(ctx, progress.StageSession, "校验已有会话…")
			if err := conn.CheckAuth(ctx, resolved, session); err == nil {
				_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
				progress.OK(ctx, progress.StageSession, "复用现有会话")
				return session, nil
			} else if connector.IsAccountBalanceHTTPError(err) {
				progress.Fail(ctx, progress.StageSession, err.Error())
				return nil, err
			} else {
				if connector.IsUpstreamChallengeHTTPError(err) {
					return nil, s.failProviderChallenge(ctx, c, channels, progress.StageSession)
				}
				checkErr = err
			}
			progress.OK(ctx, progress.StageSession, "会话校验失败，尝试刷新")
		}
		if refreshed, ok, err := s.refreshStoredSession(ctx, c, resolved, conn, session); err != nil {
			if connector.IsUpstreamChallengeHTTPError(err) {
				return nil, s.failProviderChallenge(ctx, c, channels, progress.StageSession)
			}
			return nil, err
		} else if ok {
			return refreshed, nil
		}
	}
	if !allowLogin {
		if checkErr != nil {
			return nil, fmt.Errorf("复用已有会话失败：%w", checkErr)
		}
		return nil, errors.New("没有可复用的会话，跳过登录")
	}
	return s.login(ctx, c, resolved, conn)
}

func (s *Service) refreshStoredSession(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
) (*connector.AuthSession, bool, error) {
	if strings.TrimSpace(session.RefreshToken) == "" {
		return nil, false, nil
	}
	progress.Start(ctx, progress.StageSession, "使用 refresh_token 刷新会话…")
	refreshed, err := refreshSession(ctx, resolved, conn, session)
	if err != nil {
		if connector.IsUpstreamChallengeHTTPError(err) {
			return nil, false, err
		}
		if connector.IsAccountBalanceHTTPError(err) {
			return nil, false, err
		}
		progress.OK(ctx, progress.StageSession, "刷新失败，重新登录")
		return nil, false, nil
	}
	if err := s.persistSessionContext(ctx, c, refreshed); err != nil {
		progress.Fail(ctx, progress.StageSession, err.Error())
		return nil, false, err
	}
	channels := s.Channels.WithContext(ctx)
	_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	if err := channels.RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision); err != nil {
		return nil, false, err
	}
	progress.OK(ctx, progress.StageSession, "会话刷新成功")
	return refreshed, true, nil
}

func (s *Service) refreshProvidedTokenSession(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
) (*connector.AuthSession, bool, error) {
	if strings.TrimSpace(session.RefreshToken) == "" {
		return nil, false, nil
	}
	progress.Start(ctx, progress.StageSession, "使用 refresh_token 刷新 token…")
	refreshed, err := refreshSession(ctx, resolved, conn, session)
	if err != nil {
		return nil, false, err
	}
	if err := conn.CheckAuth(ctx, resolved, refreshed); err != nil {
		return nil, false, fmt.Errorf("刷新后的 token 校验失败：%w", err)
	}
	if err := s.persistTokenCredentialContext(ctx, c, refreshed); err != nil {
		if errors.Is(err, storage.ErrStaleChannelTokenCredential) {
			return s.reuseConcurrentTokenCredential(ctx, c, resolved, conn)
		}
		return nil, false, err
	}
	_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	progress.OK(ctx, progress.StageSession, "token 刷新成功")
	return refreshed, true, nil
}

func (s *Service) reuseConcurrentTokenCredential(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
) (*connector.AuthSession, bool, error) {
	channels := s.Channels.WithContext(ctx)
	current, err := channels.FindByID(c.ID)
	if err != nil {
		return nil, false, fmt.Errorf("reload concurrently refreshed token: %w", err)
	}
	if current.UpstreamAccountIdentityRevision != c.UpstreamAccountIdentityRevision || current.CredentialMode != storage.CredentialModeToken {
		return nil, false, storage.ErrStaleChannelAccountIdentityRevision
	}
	session, err := s.buildSessionFromToken(current)
	if err != nil {
		return nil, false, fmt.Errorf("load concurrently refreshed token: %w", err)
	}
	if err := conn.CheckAuth(ctx, resolved, session); err != nil {
		return nil, false, fmt.Errorf("concurrently refreshed token validation failed: %w", err)
	}
	c.PasswordCipher = current.PasswordCipher
	c.UpdatedAt = current.UpdatedAt
	_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	progress.OK(ctx, progress.StageSession, "复用并发刷新的 token")
	return session, true, nil
}

func refreshSession(
	ctx context.Context,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
) (*connector.AuthSession, error) {
	refresher, ok := conn.(connector.SessionRefresher)
	if !ok {
		return nil, errors.New("connector does not support refresh_token")
	}
	refreshed, err := refresher.RefreshSession(ctx, resolved, session)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(refreshed.AccessToken) == "" {
		return nil, errors.New("refresh token returned empty access_token")
	}
	if strings.TrimSpace(refreshed.RefreshToken) == "" {
		refreshed.RefreshToken = session.RefreshToken
	}
	return refreshed, nil
}

func (s *Service) persistTokenCredential(c *storage.Channel, session *connector.AuthSession) error {
	return s.persistTokenCredentialContext(context.Background(), c, session)
}

func (s *Service) persistTokenCredentialContext(ctx context.Context, c *storage.Channel, session *connector.AuthSession) error {
	switch c.Type {
	case storage.ChannelTypeSub2API:
		cred := Sub2APITokenCredential{
			AccessToken:  strings.TrimSpace(session.AccessToken),
			RefreshToken: strings.TrimSpace(session.RefreshToken),
		}
		if cred.AccessToken == "" {
			return errors.New("Sub2API token 模式需要 access_token")
		}
		raw, err := json.Marshal(cred)
		if err != nil {
			return fmt.Errorf("marshal sub2api token credential: %w", err)
		}
		enc, err := s.Cipher.Encrypt(string(raw))
		if err != nil {
			return fmt.Errorf("encrypt token credential: %w", err)
		}
		expectedCredential := c.PasswordCipher
		c.PasswordCipher = enc
		if err := s.Channels.WithContext(ctx).UpdateTokenCredentialForIdentity(c, expectedCredential); err != nil {
			c.PasswordCipher = expectedCredential
			return err
		}
		return nil
	default:
		return fmt.Errorf("%s token 模式不支持 refresh_token", c.Type)
	}
}

func (s *Service) login(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
) (*connector.AuthSession, error) {
	if err := s.prepareTurnstile(ctx, c, resolved, conn); err != nil {
		return nil, err
	}
	progress.Start(ctx, progress.StageLogin, "登录上游…")
	started := time.Now()
	session, err := conn.Login(ctx, resolved)
	finished := time.Now()
	_ = s.MonitorLogs.WithContext(ctx).Append(&storage.MonitorLog{
		ChannelID:    c.ID,
		Job:          storage.MonitorJobLogin,
		Success:      err == nil,
		ErrorMessage: errString(err),
		StartedAt:    started,
		FinishedAt:   finished,
	})
	if err != nil {
		if connector.IsUpstreamChallengeHTTPError(err) {
			return nil, s.failProviderChallenge(ctx, c, s.Channels.WithContext(ctx), progress.StageLogin)
		}
		progress.Fail(ctx, progress.StageLogin, err.Error())
		_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, err.Error())
		return nil, err
	}
	if err := s.persistSessionContext(ctx, c, session); err != nil {
		progress.Fail(ctx, progress.StageLogin, err.Error())
		return nil, err
	}
	_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	progress.OK(ctx, progress.StageLogin, "登录成功")
	return session, nil
}

func (s *Service) failProviderChallenge(ctx context.Context, c *storage.Channel, channels *storage.Channels, stage progress.Stage) error {
	message := "上游 Cloudflare 正在拦截业务服务器出口；渠道凭据已保留，请让上游放行该服务器出口 IP/专用 API 域名，或配置获批准的网络代理后重试"
	if channels != nil && c != nil {
		_ = channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, message)
	}
	progress.Fail(ctx, stage, message)
	return fmt.Errorf("%w: %s", connector.ErrUpstreamChallenge, message)
}

func (s *Service) persistSession(c *storage.Channel, session *connector.AuthSession) error {
	return s.persistSessionContext(context.Background(), c, session)
}

func (s *Service) persistSessionContext(ctx context.Context, c *storage.Channel, session *connector.AuthSession) error {
	if c == nil || c.ID == 0 || c.UpstreamAccountIdentityRevision == 0 {
		return errors.New("channel identity is required to persist session")
	}
	acc, err := s.Cipher.Encrypt(session.AccessToken)
	if err != nil {
		return fmt.Errorf("encrypt access token: %w", err)
	}
	refresh, err := s.Cipher.Encrypt(session.RefreshToken)
	if err != nil {
		return fmt.Errorf("encrypt refresh token: %w", err)
	}
	cookie, err := s.Cipher.Encrypt(session.Cookie)
	if err != nil {
		return fmt.Errorf("encrypt cookie: %w", err)
	}
	csrf, err := s.Cipher.Encrypt(session.CSRFToken)
	if err != nil {
		return fmt.Errorf("encrypt csrf: %w", err)
	}
	now := time.Now()
	expires := session.ExpiresAt
	userID := session.UserID
	if session.AccountConcurrency != nil && session.AccountConcurrency.Provider == connector.TypeSub2API {
		// Sub2API account identity exists only to bind the hashed account-level
		// concurrency fact. Unlike NewAPI, it is not an authentication header and
		// must not be persisted in plaintext in auth_sessions.
		userID = ""
	}
	return s.AuthSessions.WithContext(ctx).UpsertForChannelIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision, &storage.AuthSession{
		ChannelID:          c.ID,
		UserID:             userID,
		AccessTokenCipher:  acc,
		RefreshTokenCipher: refresh,
		CookieCipher:       cookie,
		CSRFTokenCipher:    csrf,
		ExpiresAt:          &expires,
		LastLoginAt:        &now,
	})
}

func (s *Service) decryptSession(saved *storage.AuthSession) (*connector.AuthSession, error) {
	acc, err := s.Cipher.Decrypt(saved.AccessTokenCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt access token: %w", err)
	}
	refresh, err := s.Cipher.Decrypt(saved.RefreshTokenCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt refresh token: %w", err)
	}
	cookie, err := s.Cipher.Decrypt(saved.CookieCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt cookie: %w", err)
	}
	csrf, err := s.Cipher.Decrypt(saved.CSRFTokenCipher)
	if err != nil {
		return nil, fmt.Errorf("decrypt csrf: %w", err)
	}
	expires := time.Time{}
	if saved.ExpiresAt != nil {
		expires = *saved.ExpiresAt
	}
	return &connector.AuthSession{
		UserID:       saved.UserID,
		AccessToken:  acc,
		RefreshToken: refresh,
		Cookie:       cookie,
		CSRFToken:    csrf,
		ExpiresAt:    expires,
	}, nil
}

// TestLogin 优先校验已有会话；仅在会话不可复用时重新登录。
func (s *Service) TestLogin(ctx context.Context, channelID uint) error {
	ctx, unlock, err := s.lockOrdinaryChannelOperation(ctx, channelID)
	if err != nil {
		return err
	}
	defer unlock()
	c, err := s.Channels.FindByID(channelID)
	if err != nil {
		return err
	}
	if c.CredentialMode == storage.CredentialModeDirect {
		if s.Cipher == nil {
			return errors.New("直连 API Key 解密服务不可用")
		}
		apiKey, decryptErr := s.Cipher.Decrypt(c.PasswordCipher)
		if decryptErr != nil {
			return fmt.Errorf("读取直连 API Key 失败：%w", decryptErr)
		}
		if _, probeErr := s.ProbeDirectModels(ctx, c.SiteURL, apiKey, c.NetworkProxyID); probeErr != nil {
			_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, probeErr.Error())
			return probeErr
		}
		_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
		return nil
	}
	resolved, err := s.Resolve(ctx, c)
	if err != nil {
		return err
	}
	conn, err := connector.For(connector.ChannelType(c.Type))
	if err != nil {
		return err
	}
	s.applyHTTPConfig(conn)
	if err := applyProxy(conn, resolved); err != nil {
		return err
	}
	_, err = s.EnsureSession(ctx, c, resolved, conn)
	return err
}

// ProbeDirectModels reads an OpenAI-compatible model endpoint with ephemeral
// credentials. The API key is never persisted, returned, or included in errors.
func (s *Service) ProbeDirectModels(ctx context.Context, baseURL, apiKey string, proxyID *uint) ([]string, error) {
	baseURL, err := normalizeDirectBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, errors.New("直连 API 模式下 API Key 不能为空")
	}
	proxyURL := ""
	if proxyID != nil && *proxyID > 0 {
		proxyURL, _, err = s.networkProxyURLContext(ctx, &storage.Channel{NetworkProxyID: proxyID})
		if err != nil {
			return nil, err
		}
	}
	timeout := time.Duration(s.upstreamConfig().TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport is unavailable")
	}
	client, err := netproxy.NewClient(&http.Client{Transport: baseTransport.Clone(), Timeout: timeout}, proxyURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")
	resp, err := netproxy.DoWithoutRedirects(client, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 1<<20 {
		return nil, errors.New("模型列表响应超过 1 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if strings.EqualFold(strings.TrimSpace(resp.Header.Get("cf-mitigated")), "challenge") {
			return nil, errors.New("模型列表请求被上游 Cloudflare 验证拦截，请在上游放行当前出口或为渠道绑定可用网络代理")
		}
		return nil, fmt.Errorf("模型列表请求失败：HTTP %d", resp.StatusCode)
	}
	var payload struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("解析模型列表失败：%w", err)
	}
	ids := make([]string, 0, len(payload.Data))
	for _, item := range payload.Data {
		ids = append(ids, item.ID)
	}
	encoded, _ := json.Marshal(ids)
	return storage.ParseDirectModels(string(encoded))
}

func (s *Service) RedeemCode(ctx context.Context, channelID uint, code string) (*connector.RedeemResult, error) {
	code = strings.TrimSpace(code)
	if code == "" {
		return nil, errors.New("兑换码不能为空")
	}
	ctx, unlock, err := s.lockOrdinaryChannelOperation(ctx, channelID)
	if err != nil {
		return nil, err
	}
	defer unlock()

	c, err := s.Channels.FindByID(channelID)
	if err != nil {
		return nil, err
	}
	if c.CredentialMode == storage.CredentialModeDirect {
		return nil, errors.New("直连 API 渠道不支持兑换码")
	}
	var costState *storage.ChannelCostState
	if s.Costing != nil {
		costState, err = s.Costing.State(channelID)
		if err != nil {
			return nil, fmt.Errorf("读取兑换前成本状态: %w", err)
		}
	}
	resolved, err := s.Resolve(ctx, c)
	if err != nil {
		return nil, err
	}
	conn, err := connector.For(connector.ChannelType(c.Type))
	if err != nil {
		return nil, err
	}
	s.applyHTTPConfig(conn)
	if err := applyProxy(conn, resolved); err != nil {
		return nil, err
	}
	session, err := s.EnsureSession(ctx, c, resolved, conn)
	if err != nil {
		return nil, err
	}

	result, err := conn.RedeemCode(ctx, resolved, session, code)
	if err != nil {
		return nil, err
	}
	_ = s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")

	if result != nil && (result.Type == "balance" || result.NewBalance != nil) {
		bal, balErr := conn.GetBalance(ctx, resolved, session)
		if balErr == nil && bal != nil {
			if validationErr := connector.ValidateBalanceResult(bal); validationErr != nil {
				observedAt := bal.SampledAt
				if observedAt.IsZero() {
					observedAt = time.Now().UTC()
				}
				message := "余额观测无效：" + validationErr.Error()
				balErr = s.Channels.MarkBalanceUnavailable(c.ID, c.UpstreamAccountIdentityRevision, observedAt, message)
				if balErr == nil {
					balErr = validationErr
				}
			}
		}
		if balErr != nil && connector.IsAccountBalanceHTTPError(balErr) {
			_ = s.Channels.MarkUpstreamAccountBalanceInsufficient(c.ID, c.UpstreamAccountIdentityRevision, time.Now().UTC())
		}
		if balErr == nil && bal != nil {
			if factErr := s.PersistAccountConcurrencyFact(c, resolved, bal.AccountConcurrency); factErr != nil {
				warning := "兑换成功，但无法保存上游账号并发额度：" + factErr.Error()
				result.Message = strings.TrimSpace(result.Message + "；" + warning)
				if errors.Is(factErr, storage.ErrStaleUpstreamAccountConcurrencyObservation) || errors.Is(factErr, storage.ErrStaleChannelAccountIdentityRevision) {
					return result, nil
				}
			}
			sampledAt := bal.SampledAt
			if sampledAt.IsZero() {
				sampledAt = time.Now()
			}
			actualBalance := bal.Balance
			rawBalance := bal.RawBalance
			if s.Costing != nil && costState != nil {
				balanceInput := costing.BalanceInput{
					ChannelID: c.ID, ExpectedVersion: costState.Version, ExpectedIdentityRevision: c.UpstreamAccountIdentityRevision, RawBalance: rawBalance,
					ObservedFactor: bal.AppliedFactor, SampledAt: sampledAt, Source: bal.FactorSource,
				}
				if delta := rawBalance - costState.RawBalance; s.Costing.IsFundingCandidateAmount(delta) {
					digest := sha256.Sum256([]byte(code))
					costState, _, balErr = s.Costing.RecordBalanceWithFunding(balanceInput, costing.FundingCandidateInput{
						ChannelID: c.ID, Source: "redeem_code", ExternalID: fmt.Sprintf("redeem:%x", digest[:12]),
						CreditedAmount: delta, RawBalanceBefore: costState.RawBalance, RawBalanceDetected: rawBalance,
						FundingFactorBefore: costState.FundingFactor, RawTodayCostBefore: costState.RawTodayCost,
						RawTotalCostBefore: costState.RawTotalCost, DetectedAt: sampledAt,
						Note: "兑换码到账，待补充实际购买金额",
					})
				} else {
					costState, balErr = s.Costing.RecordBalance(balanceInput)
				}
				if balErr == nil {
					actualBalance = costState.ActualInventory
					if s.Rates != nil {
						rawSnapshot, factorSnapshot := rawBalance, costState.FundingFactor
						balErr = s.Rates.AppendBalanceForChannelIdentity(&storage.BalanceSnapshot{ChannelID: c.ID, Balance: actualBalance, RawBalance: &rawSnapshot, FundingFactor: &factorSnapshot, CostBasisVersion: costState.Version, SampledAt: sampledAt}, c.UpstreamAccountIdentityRevision)
					}
				}
			} else {
				balErr = s.Channels.RecordTrustedBalance(c.ID, c.UpstreamAccountIdentityRevision, rawBalance, actualBalance, sampledAt)
			}
			if balErr != nil {
				warning := "兑换成功，但成本账本更新失败：" + balErr.Error()
				_ = s.Channels.SetBalanceError(c.ID, c.UpstreamAccountIdentityRevision, warning, time.Now().UTC())
				result.Message = strings.TrimSpace(result.Message + "；" + warning)
			} else {
				result.NewBalance = &actualBalance
			}
		} else {
			if balErr == nil {
				balErr = errors.New("上游未返回余额")
			}
			warning := "兑换成功，但无法刷新成本账本：" + balErr.Error()
			_ = s.Channels.SetBalanceError(c.ID, c.UpstreamAccountIdentityRevision, warning, time.Now().UTC())
			result.Message = strings.TrimSpace(result.Message + "；" + warning)
		}
	}

	return result, nil
}

func (s *Service) GetRechargeInfo(ctx context.Context, channelID uint) (*connector.RechargeInfo, error) {
	if err := s.Channels.RequireOrdinaryChannelWrite(ctx, channelID); err != nil {
		return nil, err
	}
	c, err := s.Channels.FindByID(channelID)
	if err != nil {
		return nil, err
	}
	if c.CredentialMode == storage.CredentialModeDirect {
		return nil, errors.New("直连 API 渠道不支持充值")
	}
	resolved, err := s.Resolve(ctx, c)
	if err != nil {
		return nil, err
	}
	conn, err := connector.For(connector.ChannelType(c.Type))
	if err != nil {
		return nil, err
	}
	s.applyHTTPConfig(conn)
	if err := applyProxy(conn, resolved); err != nil {
		return nil, err
	}
	session, err := s.EnsureSession(ctx, c, resolved, conn)
	if err != nil {
		return nil, err
	}
	info, err := conn.GetRechargeInfo(ctx, resolved, session)
	if err != nil {
		return nil, err
	}
	_ = s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return info, nil
}

func (s *Service) CreateRecharge(ctx context.Context, channelID uint, req connector.RechargeRequest) (*connector.RechargeLaunch, error) {
	ctx, unlock, err := s.lockOrdinaryChannelOperation(ctx, channelID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	c, err := s.Channels.FindByID(channelID)
	if err != nil {
		return nil, err
	}
	if c.CredentialMode == storage.CredentialModeDirect {
		return nil, errors.New("直连 API 渠道不支持充值")
	}
	resolved, err := s.Resolve(ctx, c)
	if err != nil {
		return nil, err
	}
	conn, err := connector.For(connector.ChannelType(c.Type))
	if err != nil {
		return nil, err
	}
	s.applyHTTPConfig(conn)
	if err := applyProxy(conn, resolved); err != nil {
		return nil, err
	}
	session, err := s.EnsureSession(ctx, c, resolved, conn)
	if err != nil {
		return nil, err
	}
	launch, err := conn.CreateRecharge(ctx, resolved, session, req)
	if err != nil {
		return nil, err
	}
	_ = s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return launch, nil
}

func (s *Service) GetSubscriptionInfo(ctx context.Context, channelID uint) (*connector.SubscriptionInfo, error) {
	c, resolved, conn, session, err := s.prepareConnectorCall(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if c.Type != storage.ChannelTypeSub2API {
		return nil, errors.New("仅 Sub2API 支持订阅购买")
	}
	info, err := conn.GetSubscriptionInfo(ctx, resolved, session)
	if err != nil {
		return nil, err
	}
	_ = s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return info, nil
}

func (s *Service) CreateSubscription(ctx context.Context, channelID uint, req connector.SubscriptionRequest) (*connector.SubscriptionLaunch, error) {
	ctx, unlock, err := s.lockOrdinaryChannelOperation(ctx, channelID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	req.PlanID = strings.TrimSpace(req.PlanID)
	req.PaymentMethod = strings.TrimSpace(req.PaymentMethod)
	if req.PlanID == "" {
		return nil, errors.New("请选择订阅套餐")
	}
	if req.PaymentMethod == "" {
		return nil, errors.New("请选择支付方式")
	}
	c, resolved, conn, session, err := s.prepareConnectorCall(ctx, channelID)
	if err != nil {
		return nil, err
	}
	if c.Type != storage.ChannelTypeSub2API {
		return nil, errors.New("仅 Sub2API 支持订阅购买")
	}
	launch, err := conn.CreateSubscription(ctx, resolved, session, req)
	if err != nil {
		return nil, err
	}
	_ = s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return launch, nil
}

func (s *Service) GetSubscriptionUsage(ctx context.Context, channelID uint) (*connector.SubscriptionUsageInfo, error) {
	return s.getSubscriptionUsageForIdentity(ctx, channelID, 0)
}

// GetSubscriptionUsageForIdentity rejects a result that finishes after the
// configured upstream account has changed. Monitor notifications must never
// use a subscription snapshot from an earlier account revision.
func (s *Service) GetSubscriptionUsageForIdentity(ctx context.Context, channelID uint, expectedIdentityRevision uint64) (*connector.SubscriptionUsageInfo, error) {
	return s.getSubscriptionUsageForIdentity(ctx, channelID, expectedIdentityRevision)
}

func (s *Service) getSubscriptionUsageForIdentity(ctx context.Context, channelID uint, expectedIdentityRevision uint64) (*connector.SubscriptionUsageInfo, error) {
	c, resolved, conn, session, err := s.prepareConnectorCallForIdentityRevision(ctx, channelID, expectedIdentityRevision)
	if err != nil {
		return nil, err
	}
	info, err := conn.GetSubscriptionUsage(ctx, resolved, session)
	if identityErr := s.Channels.RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision); identityErr != nil {
		return nil, identityErr
	}
	if err != nil {
		return nil, err
	}
	if err := s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, ""); err != nil {
		return nil, err
	}
	return info, nil
}

func (s *Service) ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	c, resolved, conn, session, err := s.prepareConnectorCall(ctx, channelID)
	if err != nil {
		return nil, err
	}
	page, err := conn.ListAPIKeys(ctx, resolved, session, query)
	if err != nil {
		return nil, err
	}
	_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return page, nil
}

func (s *Service) ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error) {
	c, resolved, conn, session, err := s.prepareConnectorCall(ctx, channelID)
	if err != nil {
		return nil, err
	}
	groups, err := conn.ListAPIKeyGroups(ctx, resolved, session)
	if err != nil {
		return nil, err
	}
	_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return groups, nil
}

const (
	channelAPIKeyRecoveryPageSize       = 100
	channelAPIKeyRecoveryMaxPages       = 20
	channelAPIKeyRejectedPostRetryAfter = 10 * time.Minute
	channelAPIKeyAbsenceMinimumInterval = 10 * time.Minute
	channelAPIKeyAbsenceConfirmations   = 2
)

var errChannelAPIKeyRecoveryNotFound = errors.New("matching channel api key is absent from complete recovery inventory")
var errChannelAPIKeyRecoveryMismatch = errors.New("same-name channel api key does not match the persisted operation")
var channelAPIKeyProvisioningNamePattern = regexp.MustCompile(`^BOG-p-[0-9a-f]{16}$`)

type channelAPIKeyRecoveryPendingError struct{ cause error }

func (e *channelAPIKeyRecoveryPendingError) Error() string {
	if e == nil || e.cause == nil {
		return "渠道 API Key 创建操作仍在等待安全恢复"
	}
	return "渠道 API Key 创建操作仍在等待安全恢复: " + e.cause.Error()
}

func (e *channelAPIKeyRecoveryPendingError) Unwrap() error                 { return e.cause }
func (*channelAPIKeyRecoveryPendingError) ManagedKeyRecoveryPending() bool { return true }

type channelAPIKeyCreateSpec struct {
	Request                    connector.APIKeyCreateRequest
	ScopeHash                  string
	RequestFingerprint         string
	LegacyRecoveryFingerprints []string
	ManagedName                string
	GroupIdentity              string
	CustomKeyHash              string
}

type newAPIKeyCreateFingerprint struct {
	Name               string `json:"name"`
	Group              string `json:"group"`
	CustomKeyHash      string `json:"custom_key_hash,omitempty"`
	RemainQuota        int    `json:"remain_quota"`
	UnlimitedQuota     bool   `json:"unlimited_quota"`
	ExpiredTime        int64  `json:"expired_time"`
	ModelLimitsEnabled bool   `json:"model_limits_enabled"`
	ModelLimits        string `json:"model_limits"`
	AllowIPs           string `json:"allow_ips"`
	CrossGroupRetry    bool   `json:"cross_group_retry"`
}

type sub2APIKeyCreateFingerprint struct {
	Name          string   `json:"name"`
	GroupID       *int64   `json:"group_id,omitempty"`
	CustomKeyHash string   `json:"custom_key_hash,omitempty"`
	Quota         *float64 `json:"quota,omitempty"`
	ExpiresInDays *int     `json:"expires_in_days,omitempty"`
	IPWhitelist   []string `json:"ip_whitelist"`
	IPBlacklist   []string `json:"ip_blacklist"`
	RateLimit5h   *float64 `json:"rate_limit_5h,omitempty"`
	RateLimit1d   *float64 `json:"rate_limit_1d,omitempty"`
	RateLimit7d   *float64 `json:"rate_limit_7d,omitempty"`
}

func (s *Service) CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	ctx, unlock, err := s.lockOrdinaryChannelOperation(ctx, channelID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	c, resolved, conn, session, err := s.prepareConnectorCall(ctx, channelID)
	if err != nil {
		return nil, err
	}
	spec, err := makeChannelAPIKeyCreateSpec(c.Type, req)
	if err != nil {
		return nil, err
	}
	recoveryFence, err := channelAPIKeyOperationRecoveryFence(spec.Request)
	if err != nil {
		return nil, err
	}
	if s.APIKeyOps == nil {
		return nil, errors.New("渠道 API Key 创建操作存储不可用")
	}
	operation, mayPost, err := s.APIKeyOps.WithContext(ctx).Begin(storage.ChannelAPIKeyOperationStart{
		ChannelID:                  c.ID,
		Provider:                   c.Type,
		ScopeHash:                  spec.ScopeHash,
		RequestFingerprint:         spec.RequestFingerprint,
		LegacyRecoveryFingerprints: spec.LegacyRecoveryFingerprints,
		ManagedName:                spec.ManagedName,
		GroupIdentity:              spec.GroupIdentity,
		CustomKeyHash:              spec.CustomKeyHash,
		RecoveryGroupName:          spec.Request.Group,
		RecoveryFence:              recoveryFence,
	})
	if err != nil {
		if errors.Is(err, storage.ErrChannelAPIKeyOperationConflict) {
			return nil, errors.New("同名同分组的渠道 API Key 已有不同创建配置，已拒绝重复创建")
		}
		return nil, fmt.Errorf("保存渠道 API Key 创建操作: %w", err)
	}
	if !mayPost {
		return s.resumeChannelAPIKeyOperation(ctx, c, resolved, conn, session, operation, spec)
	}
	return s.postChannelAPIKeyOperation(ctx, c, resolved, conn, session, operation, spec)
}

func (s *Service) postChannelAPIKeyOperation(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
	operation *storage.ChannelAPIKeyOperation,
	spec channelAPIKeyCreateSpec,
) (*connector.APIKey, error) {
	var err error
	created, createErr := conn.CreateAPIKey(ctx, resolved, session, spec.Request)
	if created != nil && created.ID > 0 {
		if !channelAPIKeyCreateResponseIdentityMatches(operation.Provider, spec.Request, *created) {
			return nil, errors.New("上游返回的渠道 API Key 与当前创建操作不匹配，已拒绝绑定")
		}
		if channelAPIKeyHasRecoveryEvidence(*created) {
			observed := *created
			if strings.TrimSpace(observed.Name) == "" {
				observed.Name = spec.Request.Name
			}
			if strings.TrimSpace(observed.GroupName) == "" && strings.TrimSpace(observed.Group) == "" {
				observed.Group = spec.Request.Group
			}
			if !channelAPIKeyReturnedIdentityMatches(operation, spec.Request, observed) {
				return nil, errors.New("上游返回的渠道 API Key 与当前创建操作不匹配，已拒绝绑定")
			}
		}
		operation, err = s.APIKeyOps.WithContext(ctx).BindRemoteID(operation.ID, operation.OperationGeneration, created.ID)
		if err != nil {
			return nil, fmt.Errorf("保存渠道 API Key 远端标识: %w", err)
		}
	}
	if createErr != nil {
		if operation.RemoteID == nil {
			if recovered, _, recoveryErr := s.recoverChannelAPIKeyOperation(ctx, resolved, conn, session, operation, spec); recoveryErr == nil {
				operation, err = s.APIKeyOps.WithContext(ctx).BindRemoteID(operation.ID, operation.OperationGeneration, recovered.ID)
				if err != nil {
					return nil, fmt.Errorf("保存渠道 API Key 远端标识: %w", err)
				}
			}
		}
		if operation.RemoteID == nil {
			var httpErr *connector.HTTPError
			if errors.As(createErr, &httpErr) && httpErr.StatusCode == 429 {
				var recordErr error
				operation, recordErr = s.APIKeyOps.WithContext(ctx).RecordRejectedPost(operation.ID, operation.OperationGeneration, httpErr.StatusCode, time.Now().UTC())
				if recordErr != nil {
					createErr = errors.Join(createErr, fmt.Errorf("记录渠道 API Key 明确拒绝状态: %w", recordErr))
				}
			}
		}
		return nil, fmt.Errorf("渠道 API Key 创建未完成；操作已持久化，后续重试只会恢复原操作: %w", createErr)
	}
	if created == nil {
		return s.resumeChannelAPIKeyOperation(ctx, c, resolved, conn, session, operation, spec)
	}
	if operation.RemoteID == nil {
		recovered, revealed, recoveryErr := s.recoverChannelAPIKeyOperation(ctx, resolved, conn, session, operation, spec)
		if recoveryErr != nil {
			return nil, fmt.Errorf("远端已接受创建但无法安全确认对应 API Key，已禁止重复 POST: %w", recoveryErr)
		}
		operation, err = s.APIKeyOps.WithContext(ctx).BindRemoteID(operation.ID, operation.OperationGeneration, recovered.ID)
		if err != nil {
			return nil, fmt.Errorf("保存渠道 API Key 远端标识: %w", err)
		}
		created.ID = recovered.ID
		if strings.TrimSpace(created.Name) == "" {
			created.Name = recovered.Name
		}
		if strings.TrimSpace(created.Key) == "" {
			created.Key = revealed
		}
	}
	return s.completeChannelAPIKeyOperation(ctx, c, resolved, conn, session, operation, spec, created)
}

func makeChannelAPIKeyCreateSpec(provider storage.ChannelType, req connector.APIKeyCreateRequest) (channelAPIKeyCreateSpec, error) {
	req.Name = strings.TrimSpace(req.Name)
	req.CustomKey = strings.TrimSpace(req.CustomKey)
	if req.Name == "" {
		return channelAPIKeyCreateSpec{}, errors.New("密钥名称不能为空")
	}
	customKeyHash := ""
	if req.CustomKey != "" {
		customKeyHash = channelAPIKeyHash(req.CustomKey)
	}

	var groupIdentity string
	var fingerprintPayload any
	var legacyRecoveryFingerprints []string
	switch provider {
	case storage.ChannelTypeNewAPI:
		req.Group = strings.TrimSpace(req.Group)
		groupIdentity = req.Group
		fingerprintPayload = newAPIKeyCreateFingerprintForRequest(req, customKeyHash, valueOrBool(req.CrossGroupRetry, false))
		if req.CrossGroupRetry != nil && !*req.CrossGroupRetry &&
			channelAPIKeyProvisioningNamePattern.MatchString(req.Name) {
			legacyPayload := newAPIKeyCreateFingerprintForRequest(req, customKeyHash, true)
			legacyJSON, legacyErr := json.Marshal(legacyPayload)
			if legacyErr != nil {
				return channelAPIKeyCreateSpec{}, fmt.Errorf("生成旧版渠道 API Key 创建指纹: %w", legacyErr)
			}
			legacyRecoveryFingerprints = []string{channelAPIKeyHash(string(provider), string(legacyJSON))}
		}
	case storage.ChannelTypeSub2API:
		req.IPWhitelist = append([]string{}, req.IPWhitelist...)
		req.IPBlacklist = append([]string{}, req.IPBlacklist...)
		if req.GroupID == nil || *req.GroupID <= 0 {
			return channelAPIKeyCreateSpec{}, errors.New("Sub2API 创建渠道 API Key 必须选择明确分组")
		}
		groupIdentity = "group-id:" + strconv.FormatInt(*req.GroupID, 10)
		fingerprintPayload = sub2APIKeyCreateFingerprint{
			Name: req.Name, GroupID: req.GroupID, CustomKeyHash: customKeyHash, Quota: req.Quota,
			ExpiresInDays: req.ExpiresInDays, IPWhitelist: req.IPWhitelist, IPBlacklist: req.IPBlacklist,
			RateLimit5h: req.RateLimit5h, RateLimit1d: req.RateLimit1d, RateLimit7d: req.RateLimit7d,
		}
	default:
		return channelAPIKeyCreateSpec{}, fmt.Errorf("不支持的渠道类型 %q", provider)
	}
	fingerprintJSON, err := json.Marshal(fingerprintPayload)
	if err != nil {
		return channelAPIKeyCreateSpec{}, fmt.Errorf("生成渠道 API Key 创建指纹: %w", err)
	}
	return channelAPIKeyCreateSpec{
		Request:                    req,
		ScopeHash:                  channelAPIKeyHash(string(provider), req.Name, groupIdentity),
		RequestFingerprint:         channelAPIKeyHash(string(provider), string(fingerprintJSON)),
		LegacyRecoveryFingerprints: legacyRecoveryFingerprints,
		ManagedName:                req.Name,
		GroupIdentity:              groupIdentity,
		CustomKeyHash:              customKeyHash,
	}, nil
}

func newAPIKeyCreateFingerprintForRequest(req connector.APIKeyCreateRequest, customKeyHash string, crossGroupRetry bool) newAPIKeyCreateFingerprint {
	return newAPIKeyCreateFingerprint{
		Name: req.Name, Group: req.Group, CustomKeyHash: customKeyHash,
		RemainQuota: valueOrInt(req.RemainQuota, 0), UnlimitedQuota: valueOrBool(req.UnlimitedQuota, false),
		ExpiredTime: valueOrInt64(req.ExpiredTime, -1), ModelLimitsEnabled: valueOrBool(req.ModelLimitsEnabled, false),
		ModelLimits: req.ModelLimits, AllowIPs: req.AllowIPs, CrossGroupRetry: crossGroupRetry,
	}
}

func (s *Service) resumeChannelAPIKeyOperation(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
	operation *storage.ChannelAPIKeyOperation,
	spec channelAPIKeyCreateSpec,
) (*connector.APIKey, error) {
	var recovered *connector.APIKey
	if operation.RemoteID == nil {
		var revealed string
		var err error
		recovered, revealed, err = s.recoverChannelAPIKeyOperation(ctx, resolved, conn, session, operation, spec)
		if err != nil {
			if errors.Is(err, errChannelAPIKeyRecoveryNotFound) {
				if operation.RejectedPostStatus == http.StatusTooManyRequests && operation.RejectedRetryAt == nil {
					claimedOperation, claimed, claimErr := s.APIKeyOps.WithContext(ctx).ClaimRejectedPostRetry(operation.ID, operation.OperationGeneration, time.Now().UTC(), channelAPIKeyRejectedPostRetryAfter)
					if claimErr != nil {
						return nil, fmt.Errorf("认领明确拒绝的渠道 API Key 重试: %w", claimErr)
					}
					if claimed {
						return s.postChannelAPIKeyOperation(ctx, c, resolved, conn, session, claimedOperation, spec)
					}
					return nil, &channelAPIKeyRecoveryPendingError{cause: err}
				}
				fence, fenceErr := channelAPIKeyOperationRecoveryFence(spec.Request)
				if fenceErr != nil {
					return nil, fenceErr
				}
				if fence.Valid() {
					var nextOperation *storage.ChannelAPIKeyOperation
					var rotated bool
					var rotateErr error
					if channelAPIKeyOperationUsesLegacyRecovery(operation, spec) {
						nextOperation, rotated, rotateErr = s.APIKeyOps.WithContext(ctx).ConfirmAbsenceAndRotateWithRequestFingerprint(
							operation.ID, operation.OperationGeneration, fence, time.Now().UTC(), channelAPIKeyAbsenceMinimumInterval, channelAPIKeyAbsenceConfirmations, spec.RequestFingerprint,
						)
					} else {
						nextOperation, rotated, rotateErr = s.APIKeyOps.WithContext(ctx).ConfirmAbsenceAndRotate(
							operation.ID, operation.OperationGeneration, fence, time.Now().UTC(), channelAPIKeyAbsenceMinimumInterval, channelAPIKeyAbsenceConfirmations,
						)
					}
					if rotateErr != nil {
						return nil, fmt.Errorf("记录渠道 API Key 远端缺失确认: %w", rotateErr)
					}
					if rotated {
						return s.postChannelAPIKeyOperation(ctx, c, resolved, conn, session, nextOperation, spec)
					}
				}
				return nil, &channelAPIKeyRecoveryPendingError{cause: err}
			}
			return nil, fmt.Errorf("已有渠道 API Key 创建操作尚无法安全恢复，已禁止重复 POST: %w", err)
		}
		operation, err = s.APIKeyOps.WithContext(ctx).BindRemoteID(operation.ID, operation.OperationGeneration, recovered.ID)
		if err != nil {
			return nil, fmt.Errorf("保存渠道 API Key 远端标识: %w", err)
		}
		recovered.Key = revealed
	}
	return s.completeChannelAPIKeyOperation(ctx, c, resolved, conn, session, operation, spec, recovered)
}

func channelAPIKeyOperationUsesLegacyRecovery(operation *storage.ChannelAPIKeyOperation, spec channelAPIKeyCreateSpec) bool {
	if operation == nil || operation.RequestFingerprint == spec.RequestFingerprint {
		return false
	}
	for _, fingerprint := range spec.LegacyRecoveryFingerprints {
		if operation.RequestFingerprint == fingerprint {
			return true
		}
	}
	return false
}

func channelAPIKeyOperationRecoveryFence(req connector.APIKeyCreateRequest) (storage.ChannelAPIKeyOperationRecoveryFence, error) {
	if req.RecoveryFence == nil {
		return storage.ChannelAPIKeyOperationRecoveryFence{}, nil
	}
	if !req.RecoveryFence.Valid() || strings.TrimSpace(req.Group) == "" {
		return storage.ChannelAPIKeyOperationRecoveryFence{}, errors.New("渠道 API Key 恢复围栏不完整")
	}
	return storage.ChannelAPIKeyOperationRecoveryFence{
		ChannelIdentityRevision: req.RecoveryFence.ChannelIdentityRevision,
		UpstreamGroupID:         req.RecoveryFence.UpstreamGroupID,
		PresenceGeneration:      req.RecoveryFence.PresenceGeneration,
		CanonicalGroupName:      strings.TrimSpace(req.RecoveryFence.CanonicalGroupName),
	}, nil
}

func (s *Service) completeChannelAPIKeyOperation(
	ctx context.Context,
	c *storage.Channel,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
	operation *storage.ChannelAPIKeyOperation,
	spec channelAPIKeyCreateSpec,
	result *connector.APIKey,
) (*connector.APIKey, error) {
	if operation == nil || operation.RemoteID == nil || *operation.RemoteID <= 0 {
		return nil, errors.New("渠道 API Key 创建操作缺少远端标识，已拒绝完成")
	}
	remoteID := *operation.RemoteID
	if result == nil {
		result = &connector.APIKey{ID: remoteID, Name: spec.ManagedName}
	}
	result.ID = remoteID
	if strings.TrimSpace(result.Name) == "" {
		result.Name = spec.ManagedName
	}
	if c.Type == storage.ChannelTypeNewAPI && strings.TrimSpace(result.Group) == "" {
		result.Group = spec.Request.Group
	}
	if c.Type == storage.ChannelTypeSub2API && result.GroupID == nil {
		result.GroupID = spec.Request.GroupID
	}
	if !usableChannelAPIKey(result.Key) {
		key, err := conn.RevealAPIKey(ctx, resolved, session, remoteID)
		if err != nil {
			return nil, fmt.Errorf("恢复已创建的渠道 API Key: %w", err)
		}
		result.Key = strings.TrimSpace(key)
	}
	if !usableChannelAPIKey(result.Key) {
		return nil, errors.New("上游未返回完整渠道 API Key")
	}
	if operation.CustomKeyHash != "" && channelAPIKeyHash(strings.TrimSpace(result.Key)) != operation.CustomKeyHash {
		return nil, errors.New("上游返回的渠道 API Key 与当前创建操作不匹配")
	}
	if _, err := s.APIKeyOps.WithContext(ctx).Complete(operation.ID, operation.OperationGeneration, remoteID); err != nil {
		return nil, fmt.Errorf("完成渠道 API Key 创建操作: %w", err)
	}
	_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return result, nil
}

func usableChannelAPIKey(key string) bool {
	key = strings.TrimSpace(key)
	return key != "" && !strings.Contains(key, "*")
}

func channelAPIKeyHash(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func valueOrInt(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}

func valueOrInt64(value *int64, fallback int64) int64 {
	if value == nil {
		return fallback
	}
	return *value
}

func valueOrBool(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func (s *Service) recoverChannelAPIKeyOperation(
	ctx context.Context,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
	operation *storage.ChannelAPIKeyOperation,
	spec channelAPIKeyCreateSpec,
) (*connector.APIKey, string, error) {
	if operation == nil {
		return nil, "", errors.New("渠道 API Key 创建操作不存在")
	}
	items, err := listChannelAPIKeysForRecovery(ctx, resolved, conn, session, spec.ManagedName, false)
	if err != nil {
		return nil, "", err
	}
	selected, selectedKey, err := selectChannelAPIKeyRecoveryCandidate(ctx, resolved, conn, session, operation, spec, items)
	if err == nil || (!errors.Is(err, errChannelAPIKeyRecoveryNotFound) && !errors.Is(err, errChannelAPIKeyRecoveryMismatch)) {
		return selected, selectedKey, err
	}

	// Provider-side search can return false negatives. Only an unfiltered,
	// strictly identified and completely paginated inventory is accepted as an
	// absence observation that may advance an operation generation.
	items, err = listChannelAPIKeysForRecovery(ctx, resolved, conn, session, "", true)
	if err != nil {
		return nil, "", err
	}
	return selectChannelAPIKeyRecoveryCandidate(ctx, resolved, conn, session, operation, spec, items)
}

func selectChannelAPIKeyRecoveryCandidate(
	ctx context.Context,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
	operation *storage.ChannelAPIKeyOperation,
	spec channelAPIKeyCreateSpec,
	items []connector.APIKey,
) (*connector.APIKey, string, error) {

	var selected *connector.APIKey
	var selectedKey string
	var proofErr error
	sameNameObserved := false
	for i := range items {
		item := items[i]
		if strings.TrimSpace(item.Name) == spec.ManagedName {
			sameNameObserved = true
		}
		if item.ID <= 0 || !channelAPIKeyScopeMatches(operation.Provider, spec.Request, item) {
			continue
		}
		if !channelAPIKeyFingerprintMatches(operation, spec.Request, item) {
			continue
		}
		revealed := ""
		if operation.CustomKeyHash != "" {
			revealed, proofErr = conn.RevealAPIKey(ctx, resolved, session, item.ID)
			if proofErr != nil {
				continue
			}
			revealed = strings.TrimSpace(revealed)
			if !usableChannelAPIKey(revealed) || channelAPIKeyHash(revealed) != operation.CustomKeyHash {
				continue
			}
		}
		if selected != nil && item.ID != selected.ID {
			return nil, "", fmt.Errorf("%w: 发现多个名称、分组和创建配置均匹配的远端 API Key", errChannelAPIKeyRecoveryMismatch)
		}
		if selected == nil {
			item.Key = ""
			selected = &item
			selectedKey = revealed
		}
	}
	if selected == nil {
		if proofErr != nil {
			return nil, "", fmt.Errorf("校验已创建渠道 API Key 失败: %w", proofErr)
		}
		if sameNameObserved {
			return nil, "", fmt.Errorf("%w: 发现同名渠道 API Key，但分组或创建配置与持久化操作不匹配", errChannelAPIKeyRecoveryMismatch)
		}
		return nil, "", fmt.Errorf("%w: 没有找到名称、分组和创建配置均完全匹配的远端 API Key", errChannelAPIKeyRecoveryNotFound)
	}
	return selected, selectedKey, nil
}

func listChannelAPIKeysForRecovery(
	ctx context.Context,
	resolved *connector.Channel,
	conn connector.Connector,
	session *connector.AuthSession,
	search string,
	requireCompleteGroupIdentity bool,
) ([]connector.APIKey, error) {
	items := make([]connector.APIKey, 0, channelAPIKeyRecoveryPageSize)
	seenIDs := make(map[int64]struct{})
	expectedPageSize := 0
	expectedPages := 0
	var expectedTotal int64
	for pageNumber := 1; pageNumber <= channelAPIKeyRecoveryMaxPages; pageNumber++ {
		page, err := conn.ListAPIKeys(ctx, resolved, session, connector.APIKeyQuery{
			Page: pageNumber, PageSize: channelAPIKeyRecoveryPageSize, Search: search,
			RequireCompleteGroupIdentity: requireCompleteGroupIdentity,
		})
		if err != nil {
			return nil, fmt.Errorf("查询待恢复的渠道 API Key: %w", err)
		}
		if page == nil {
			return nil, errors.New("查询待恢复的渠道 API Key 返回空结果")
		}
		if requireCompleteGroupIdentity {
			if page.Page != pageNumber || page.PageSize < 1 || page.PageSize > channelAPIKeyRecoveryPageSize ||
				len(page.Items) > page.PageSize || page.Pages < 0 || page.Total < 0 {
				return nil, errors.New("待恢复的渠道 API Key 完整清单分页信息无效")
			}
			if pageNumber == 1 {
				expectedPageSize, expectedPages, expectedTotal = page.PageSize, page.Pages, page.Total
				if expectedPages > channelAPIKeyRecoveryMaxPages ||
					(expectedTotal > int64(expectedPageSize*channelAPIKeyRecoveryMaxPages)) {
					return nil, errors.New("待恢复的渠道 API Key 完整清单超出安全分页上限")
				}
			} else if page.PageSize != expectedPageSize || page.Pages != expectedPages || page.Total != expectedTotal {
				return nil, errors.New("待恢复的渠道 API Key 完整清单分页信息在扫描期间发生变化")
			}
			if expectedPages > 0 && pageNumber < expectedPages && len(page.Items) != expectedPageSize {
				return nil, errors.New("待恢复的渠道 API Key 完整清单在末页前提前结束")
			}
			for _, item := range page.Items {
				if item.ID <= 0 {
					return nil, errors.New("待恢复的渠道 API Key 完整清单包含无效远端标识")
				}
				if _, duplicate := seenIDs[item.ID]; duplicate {
					return nil, errors.New("待恢复的渠道 API Key 完整清单包含重复远端标识")
				}
				seenIDs[item.ID] = struct{}{}
			}
		}
		items = append(items, page.Items...)
		complete, err := channelAPIKeyRecoveryPageComplete(pageNumber, page)
		if err != nil {
			return nil, err
		}
		if complete {
			if requireCompleteGroupIdentity && expectedTotal > 0 && int64(len(items)) != expectedTotal {
				return nil, errors.New("待恢复的渠道 API Key 完整清单数量与分页信息不一致")
			}
			return items, nil
		}
	}
	return nil, errors.New("待恢复的渠道 API Key 超出安全分页上限")
}

func channelAPIKeyRecoveryPageComplete(pageNumber int, page *connector.APIKeyPage) (bool, error) {
	if page.Page > 0 && page.Page != pageNumber {
		return false, errors.New("待恢复的渠道 API Key 分页信息不一致")
	}
	if page.Pages > 0 {
		if page.Pages > channelAPIKeyRecoveryMaxPages || pageNumber > page.Pages {
			return false, errors.New("待恢复的渠道 API Key 分页范围不安全")
		}
		return pageNumber == page.Pages, nil
	}
	pageSize := page.PageSize
	if pageSize <= 0 {
		pageSize = channelAPIKeyRecoveryPageSize
	}
	if page.Total > 0 {
		return int64(pageNumber*pageSize) >= page.Total, nil
	}
	return len(page.Items) < pageSize, nil
}

func channelAPIKeyScopeMatches(provider storage.ChannelType, req connector.APIKeyCreateRequest, item connector.APIKey) bool {
	if strings.TrimSpace(item.Name) != strings.TrimSpace(req.Name) {
		return false
	}
	switch provider {
	case storage.ChannelTypeNewAPI:
		groupName := strings.TrimSpace(item.GroupName)
		group := strings.TrimSpace(item.Group)
		if groupName != "" && group != "" && groupName != group {
			return false
		}
		if groupName != "" {
			group = groupName
		}
		return group == strings.TrimSpace(req.Group)
	case storage.ChannelTypeSub2API:
		if req.GroupID == nil {
			return item.GroupID == nil
		}
		return item.GroupID != nil && *item.GroupID == *req.GroupID
	default:
		return false
	}
}

func channelAPIKeyFingerprintMatches(operation *storage.ChannelAPIKeyOperation, req connector.APIKeyCreateRequest, item connector.APIKey) bool {
	switch operation.Provider {
	case storage.ChannelTypeNewAPI:
		return newAPIKeyFingerprintMatches(operation, req, item)
	case storage.ChannelTypeSub2API:
		return sub2APIKeyFingerprintMatches(operation, req, item)
	default:
		return false
	}
}

func channelAPIKeyHasRecoveryEvidence(item connector.APIKey) bool {
	return item.CreatedAt != nil || apiKeyObservedFieldsAny(item.ObservedFields) || apiKeyObservedFieldsAny(item.NullFields)
}

func channelAPIKeyReturnedIdentityMatches(operation *storage.ChannelAPIKeyOperation, req connector.APIKeyCreateRequest, item connector.APIKey) bool {
	if operation == nil || !channelAPIKeyBasicIdentityMatches(operation.Provider, req, item) {
		return false
	}
	if operation.CustomKeyHash != "" && usableChannelAPIKey(item.Key) &&
		channelAPIKeyHash(strings.TrimSpace(item.Key)) != operation.CustomKeyHash {
		return false
	}
	switch operation.Provider {
	case storage.ChannelTypeNewAPI:
		return newAPIKeyFingerprintMatches(operation, req, item)
	case storage.ChannelTypeSub2API:
		return channelAPIKeyScopeMatches(operation.Provider, req, item) && sub2APIKeyFingerprintMatches(operation, req, item)
	default:
		return false
	}
}

// A successful create response may contain only the new remote ID. Accept
// omitted identity fields for that exact POST result so the durable operation
// can bind the ID and retry reveal without another create. Any field the
// provider did return must still agree with the persisted request.
func channelAPIKeyCreateResponseIdentityMatches(provider storage.ChannelType, req connector.APIKeyCreateRequest, item connector.APIKey) bool {
	if provider != storage.ChannelTypeNewAPI {
		return channelAPIKeyBasicIdentityMatches(provider, req, item)
	}
	if item.ID <= 0 || item.Source != connector.APIKeySourceNewAPI {
		return false
	}
	if item.NullIdentityFields.Name || item.NullIdentityFields.Group || item.NullIdentityFields.Status {
		return false
	}
	if name := strings.TrimSpace(item.Name); name != "" && name != strings.TrimSpace(req.Name) {
		return false
	}
	if item.ObservedIdentityFields.Name && strings.TrimSpace(item.Name) == "" {
		return false
	}
	groupName, group := strings.TrimSpace(item.GroupName), strings.TrimSpace(item.Group)
	if groupName != "" && group != "" && groupName != group {
		return false
	}
	if groupName != "" {
		group = groupName
	}
	if group != "" && group != strings.TrimSpace(req.Group) {
		return false
	}
	if item.ObservedIdentityFields.Group && group == "" {
		return false
	}
	status := strings.TrimSpace(item.Status)
	if item.ObservedIdentityFields.Status {
		return strings.EqualFold(status, "active")
	}
	return status == "" || strings.EqualFold(status, "unknown") || strings.EqualFold(status, "active")
}

func channelAPIKeyBasicIdentityMatches(provider storage.ChannelType, req connector.APIKeyCreateRequest, item connector.APIKey) bool {
	if item.ID <= 0 || strings.TrimSpace(item.Name) != strings.TrimSpace(req.Name) {
		return false
	}
	groupName, group := strings.TrimSpace(item.GroupName), strings.TrimSpace(item.Group)
	if groupName != "" && group != "" && groupName != group {
		return false
	}
	switch provider {
	case storage.ChannelTypeNewAPI:
		if item.Source != connector.APIKeySourceNewAPI {
			return false
		}
		status := strings.TrimSpace(item.Status)
		if status != "" && !strings.EqualFold(status, "unknown") && !strings.EqualFold(status, "active") {
			return false
		}
		if groupName != "" {
			group = groupName
		}
		if group != "" && group != strings.TrimSpace(req.Group) {
			return false
		}
	case storage.ChannelTypeSub2API:
		if item.Source != connector.APIKeySourceSub2API ||
			(item.Status != "" && !strings.EqualFold(strings.TrimSpace(item.Status), "active")) {
			return false
		}
		if req.GroupID != nil && item.GroupID != nil && *req.GroupID != *item.GroupID {
			return false
		}
	default:
		return false
	}
	return true
}

func apiKeyObservedFieldsAny(fields connector.APIKeyObservedFields) bool {
	return fields.Quota || fields.UnlimitedQuota || fields.ExpiredTime || fields.ExpiresAt || fields.AllowIPs ||
		fields.IPWhitelist || fields.IPBlacklist || fields.ModelLimitsEnabled || fields.ModelLimits ||
		fields.CrossGroupRetry || fields.RateLimit5h || fields.RateLimit1d || fields.RateLimit7d
}

func newAPIKeyFingerprintMatches(operation *storage.ChannelAPIKeyOperation, req connector.APIKeyCreateRequest, item connector.APIKey) bool {
	status := strings.TrimSpace(item.Status)
	isProvisioningName := channelAPIKeyProvisioningNamePattern.MatchString(strings.TrimSpace(req.Name))
	if item.Source != connector.APIKeySourceNewAPI ||
		(!strings.EqualFold(status, "active") && !(isProvisioningName && (status == "" || strings.EqualFold(status, "unknown")))) {
		return false
	}
	if item.CreatedAt != nil && item.CreatedAt.Before(operation.CreatedAt.Add(-2*time.Second)) {
		return false
	}
	complete := true
	check := func(observed, nullValue, equal bool) bool {
		if !observed {
			complete = false
			return true
		}
		return !nullValue && equal
	}
	if !check(item.ObservedFields.Quota, item.NullFields.Quota, almostEqual(item.Quota, float64(valueOrInt(req.RemainQuota, 0)))) ||
		!check(item.ObservedFields.UnlimitedQuota, item.NullFields.UnlimitedQuota, item.UnlimitedQuota == valueOrBool(req.UnlimitedQuota, false)) ||
		!check(item.ObservedFields.ExpiredTime, item.NullFields.ExpiredTime, item.ExpiredTime == valueOrInt64(req.ExpiredTime, -1)) ||
		!check(item.ObservedFields.ModelLimitsEnabled, item.NullFields.ModelLimitsEnabled, item.ModelLimitsEnabled == valueOrBool(req.ModelLimitsEnabled, false)) ||
		!check(item.ObservedFields.ModelLimits, item.NullFields.ModelLimits, item.ModelLimits == req.ModelLimits) ||
		!check(item.ObservedFields.AllowIPs, item.NullFields.AllowIPs, item.AllowIPs == req.AllowIPs) ||
		!check(item.ObservedFields.CrossGroupRetry, item.NullFields.CrossGroupRetry, newAPICrossGroupRetryMatches(operation, req, item)) {
		return false
	}
	if isProvisioningName ||
		(operation.RemoteID != nil && *operation.RemoteID == item.ID) ||
		operation.CustomKeyHash != "" {
		return true
	}
	return complete && item.CreatedAt != nil
}

func newAPICrossGroupRetryMatches(operation *storage.ChannelAPIKeyOperation, req connector.APIKeyCreateRequest, item connector.APIKey) bool {
	expected := valueOrBool(req.CrossGroupRetry, false)
	if item.CrossGroupRetry == expected {
		return true
	}
	// A false value is a stricter, group-pinned normalization. Limit this
	// compatibility to the random provisioning namespace so ordinary/manual
	// key creation still requires an exact response fingerprint.
	if expected && !item.CrossGroupRetry &&
		channelAPIKeyProvisioningNamePattern.MatchString(strings.TrimSpace(req.Name)) {
		return true
	}
	if !expected && item.CrossGroupRetry && operation != nil &&
		channelAPIKeyProvisioningNamePattern.MatchString(strings.TrimSpace(req.Name)) {
		legacyPayload := newAPIKeyCreateFingerprintForRequest(req, operation.CustomKeyHash, true)
		legacyJSON, err := json.Marshal(legacyPayload)
		return err == nil && operation.RequestFingerprint == channelAPIKeyHash(string(storage.ChannelTypeNewAPI), string(legacyJSON))
	}
	return false
}

func sub2APIKeyFingerprintMatches(operation *storage.ChannelAPIKeyOperation, req connector.APIKeyCreateRequest, item connector.APIKey) bool {
	if item.Source != connector.APIKeySourceSub2API || !strings.EqualFold(strings.TrimSpace(item.Status), "active") {
		return false
	}
	if item.CreatedAt == nil || item.CreatedAt.Before(operation.CreatedAt.Add(-2*time.Second)) {
		return false
	}
	if valueOrFloat64(req.Quota, 0) != 0 || len(req.IPWhitelist) != 0 || len(req.IPBlacklist) != 0 ||
		valueOrFloat64(req.RateLimit5h, 0) != 0 || valueOrFloat64(req.RateLimit1d, 0) != 0 || valueOrFloat64(req.RateLimit7d, 0) != 0 {
		return false
	}
	observed := item.ObservedFields
	// Sub2API recovery intentionally requires these safety fields to be
	// explicit even when a custom-key hash is available.
	if !observed.Quota || !observed.ExpiresAt || !observed.IPWhitelist || !observed.IPBlacklist ||
		!observed.RateLimit5h || !observed.RateLimit1d || !observed.RateLimit7d {
		return false
	}
	if !almostEqual(item.Quota, 0) || len(item.IPWhitelist) != 0 || len(item.IPBlacklist) != 0 ||
		!almostEqual(item.RateLimit5h, 0) || !almostEqual(item.RateLimit1d, 0) || !almostEqual(item.RateLimit7d, 0) {
		return false
	}
	if item.ExpiresAt != nil && !item.ExpiresAt.After(time.Now()) {
		return false
	}
	if req.ExpiresInDays == nil || *req.ExpiresInDays == 0 {
		return item.ExpiresAt == nil
	}
	if *req.ExpiresInDays < 0 || item.ExpiresAt == nil {
		return false
	}
	expectedExpiry := item.CreatedAt.AddDate(0, 0, *req.ExpiresInDays)
	return absDuration(item.ExpiresAt.Sub(expectedExpiry)) <= 10*time.Minute
}

func valueOrFloat64(value *float64, fallback float64) float64 {
	if value == nil {
		return fallback
	}
	return *value
}

func almostEqual(left, right float64) bool {
	return math.Abs(left-right) <= 1e-9
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}

func (s *Service) UpdateAPIKey(ctx context.Context, channelID uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	ctx, unlock, err := s.lockOrdinaryChannelOperation(ctx, channelID)
	if err != nil {
		return nil, err
	}
	defer unlock()
	c, resolved, conn, session, err := s.prepareConnectorCall(ctx, channelID)
	if err != nil {
		return nil, err
	}
	key, err := conn.UpdateAPIKey(ctx, resolved, session, keyID, req)
	if err != nil {
		return nil, err
	}
	_ = s.Channels.SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return key, nil
}

func (s *Service) DeleteAPIKey(ctx context.Context, channelID uint, keyID int64) error {
	return ErrDirectAPIKeyDeletionDisabled
}

// PrepareManagedAPIKeyDeletion resolves all database-backed channel and
// session state before the gateway enters its generation-locked transaction.
// The returned callback performs only provider HTTP reads and mutation, so it
// cannot exhaust a small SQL pool while the transaction and channel lock hold
// their own connections. It revalidates the stable remote identity immediately
// before DELETE because provider IDs and group assignments are mutable.
func (s *Service) PrepareManagedAPIKeyDeletion(ctx context.Context, channelID uint, expected connector.APIKey, expectedIdentityRevision uint64) (func() error, error) {
	if expected.ID <= 0 {
		return nil, errors.New("upstream api key id is required")
	}
	if expectedIdentityRevision == 0 {
		return nil, errors.New("channel account identity revision is required")
	}
	_, resolved, conn, session, err := s.prepareConnectorCallForIdentityRevision(ctx, channelID, expectedIdentityRevision)
	if err != nil {
		return nil, err
	}
	if err := s.Channels.WithContext(ctx).RequireUpstreamAccountIdentityRevision(channelID, expectedIdentityRevision); err != nil {
		return nil, err
	}
	return func() error {
		return revalidateAndDeleteManagedAPIKey(ctx, conn, resolved, session, expected)
	}, nil
}

var errManagedAPIKeyNotFound = errors.New("managed api key not found")

func revalidateAndDeleteManagedAPIKey(ctx context.Context, conn connector.Connector, channel *connector.Channel, session *connector.AuthSession, expected connector.APIKey) error {
	current, err := findManagedAPIKeyForDeletion(ctx, conn, channel, session, expected)
	if errors.Is(err, errManagedAPIKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(connector.ManagedAPIKeyDeletionIdentity(*current), connector.ManagedAPIKeyDeletionIdentity(expected)) {
		return connector.ErrManagedAPIKeyDeletionIdentityChanged
	}
	return conn.DeleteAPIKey(ctx, channel, session, expected.ID)
}

func findManagedAPIKeyForDeletion(ctx context.Context, conn connector.Connector, channel *connector.Channel, session *connector.AuthSession, expected connector.APIKey) (*connector.APIKey, error) {
	search := strings.TrimSpace(expected.Name)
	current, err := findManagedAPIKeyForDeletionQuery(ctx, conn, channel, session, expected.ID, search)
	if !errors.Is(err, errManagedAPIKeyNotFound) || search == "" {
		return current, err
	}
	// A provider search may return a false negative. Fall back to the complete
	// inventory only when the exact-name result did not contain the expected ID;
	// this preserves ID-reuse and rename detection without rescanning the whole
	// account before every ordinary deletion.
	return findManagedAPIKeyForDeletionQuery(ctx, conn, channel, session, expected.ID, "")
}

func findManagedAPIKeyForDeletionQuery(ctx context.Context, conn connector.Connector, channel *connector.Channel, session *connector.AuthSession, keyID int64, search string) (*connector.APIKey, error) {
	var selected *connector.APIKey
	for pageNumber := 1; pageNumber <= channelAPIKeyRecoveryMaxPages; pageNumber++ {
		page, err := conn.ListAPIKeys(ctx, channel, session, connector.APIKeyQuery{
			Page: pageNumber, PageSize: channelAPIKeyRecoveryPageSize, Search: search, RequireCompleteGroupIdentity: true,
		})
		if err != nil {
			return nil, fmt.Errorf("revalidate managed api key %d before deletion: %w", keyID, err)
		}
		if page == nil {
			return nil, errors.New("revalidate managed api key before deletion returned an empty page")
		}
		for index := range page.Items {
			if page.Items[index].ID == keyID {
				if selected != nil {
					return nil, connector.ErrManagedAPIKeyDeletionIdentityChanged
				}
				item := page.Items[index]
				selected = &item
			}
		}
		complete, err := channelAPIKeyRecoveryPageComplete(pageNumber, page)
		if err != nil {
			return nil, fmt.Errorf("revalidate managed api key pagination: %w", err)
		}
		if complete {
			if selected != nil {
				return selected, nil
			}
			return nil, errManagedAPIKeyNotFound
		}
	}
	return nil, errors.New("managed api key revalidation exceeds safe pagination limit")
}

func (s *Service) RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error) {
	c, resolved, conn, session, err := s.prepareConnectorCall(ctx, channelID)
	if err != nil {
		return "", err
	}
	key, err := conn.RevealAPIKey(ctx, resolved, session, keyID)
	if err != nil {
		return "", err
	}
	_ = s.Channels.WithContext(ctx).SetLastErrorForIdentity(c.ID, c.UpstreamAccountIdentityRevision, "")
	return key, nil
}

// LookupRequestCost 查询上游最终用量记录。它走后台路径，优先复用本地已有会话，
// 不为每条网关请求额外执行一次 CheckAuth。
func (s *Service) LookupRequestCost(ctx context.Context, channelID uint, query connector.RequestCostQuery) (*connector.RequestCostResult, error) {
	c, err := s.Channels.FindByID(channelID)
	if err != nil {
		return nil, err
	}
	if c.CredentialMode == storage.CredentialModeDirect {
		return nil, connector.ErrUnsupported
	}
	resolved, err := s.Resolve(ctx, c)
	if err != nil {
		return nil, err
	}
	conn, err := connector.For(connector.ChannelType(c.Type))
	if err != nil {
		return nil, err
	}
	if err := applyProxy(conn, resolved); err != nil {
		return nil, err
	}
	s.applyHTTPConfig(conn)
	lookup, ok := conn.(connector.RequestCostLookup)
	if !ok {
		return nil, fmt.Errorf("connector %q does not support request cost lookup", c.Type)
	}
	session, err := s.requestCostSession(ctx, c, resolved, conn)
	if err != nil {
		return nil, err
	}
	result, err := lookup.LookupRequestCost(ctx, resolved, session, query)
	if err == nil || !connector.IsAuthHTTPError(err) || connector.IsUpstreamChallengeHTTPError(err) || connector.IsAccountBalanceHTTPError(err) {
		return result, err
	}
	refreshed, refreshErr := s.EnsureSession(ctx, c, resolved, conn)
	if refreshErr != nil {
		return nil, refreshErr
	}
	return lookup.LookupRequestCost(ctx, resolved, refreshed, query)
}

func (s *Service) requestCostSession(ctx context.Context, c *storage.Channel, resolved *connector.Channel, conn connector.Connector) (*connector.AuthSession, error) {
	if c.CredentialMode == storage.CredentialModeToken {
		return s.buildSessionFromToken(c)
	}
	saved, err := s.AuthSessions.FindByChannel(c.ID)
	if err != nil {
		return nil, err
	}
	if saved != nil && (saved.ExpiresAt == nil || saved.ExpiresAt.After(time.Now())) {
		return s.decryptSession(saved)
	}
	return s.EnsureSession(ctx, c, resolved, conn)
}

func (s *Service) prepareConnectorCall(ctx context.Context, channelID uint) (*storage.Channel, *connector.Channel, connector.Connector, *connector.AuthSession, error) {
	return s.prepareConnectorCallForIdentityRevision(ctx, channelID, 0)
}

func (s *Service) prepareConnectorCallForIdentityRevision(ctx context.Context, channelID uint, expectedIdentityRevision uint64) (*storage.Channel, *connector.Channel, connector.Connector, *connector.AuthSession, error) {
	channels := s.Channels.WithContext(ctx)
	if err := channels.RequireOrdinaryChannelWrite(ctx, channelID); err != nil {
		return nil, nil, nil, nil, err
	}
	c, err := channels.FindByID(channelID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if c.CredentialMode == storage.CredentialModeDirect {
		return nil, nil, nil, nil, errors.New("直连 API 渠道不支持后台管理接口")
	}
	if expectedIdentityRevision != 0 && c.UpstreamAccountIdentityRevision != expectedIdentityRevision {
		return nil, nil, nil, nil, storage.ErrStaleChannelAccountIdentityRevision
	}
	resolved, err := s.Resolve(ctx, c)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	conn, err := connector.For(connector.ChannelType(c.Type))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	s.applyHTTPConfig(conn)
	if err := applyProxy(conn, resolved); err != nil {
		return nil, nil, nil, nil, err
	}
	session, err := s.EnsureSession(ctx, c, resolved, conn)
	if err != nil {
		if identityErr := channels.RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision); identityErr != nil {
			return nil, nil, nil, nil, identityErr
		}
		return nil, nil, nil, nil, err
	}
	if err := channels.RequireUpstreamAccountIdentityRevision(c.ID, c.UpstreamAccountIdentityRevision); err != nil {
		return nil, nil, nil, nil, err
	}
	return c, resolved, conn, session, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
