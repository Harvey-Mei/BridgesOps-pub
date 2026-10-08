// Package connector 定义上游渠道连接器接口与公共类型，由 newapi / sub2api 等子包注册具体实现。
//
// 使用方法：
//
//	import _ "github.com/Harvey-Mei/BridgeOps/backend/connector/newapi"
//	import _ "github.com/Harvey-Mei/BridgeOps/backend/connector/sub2api"
//
//	c, err := connector.For("newapi")
//	session, err := c.Login(ctx, channel)
package connector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ChannelType 渠道类型枚举，与 storage.ChannelType 同步。
type ChannelType string

const (
	TypeNewAPI  ChannelType = "newapi"
	TypeSub2API ChannelType = "sub2api"
)

// Channel 已解密的渠道连接信息，由 channel 层负责构造。
type Channel struct {
	ID   uint
	Name string
	Type ChannelType
	// SiteURL is the API base used by connectors. HomepageURL remains the
	// administrator-facing link when the provider separates web and API origins.
	SiteURL                string
	HomepageURL            string
	APIOriginKnown         bool
	Username               string
	Password               string
	LoginExtraParams       map[string]any
	TurnstileEnabled       bool
	ProxyURL               string
	ProxyCacheKey          string
	TimeZone               string
	RechargeMultiplier     *float64
	RechargeMultiplierMode string
	// TurnstileToken 由调用方在 Login 前预先求解打码后填入；为空则直接发起登录。
	TurnstileToken string
}

const (
	RechargeMultiplierModeDivide   = "divide"
	RechargeMultiplierModeMultiply = "multiply"
)

func NormalizeRechargeMultiplierMode(mode string) string {
	switch strings.TrimSpace(mode) {
	case RechargeMultiplierModeMultiply:
		return RechargeMultiplierModeMultiply
	default:
		return RechargeMultiplierModeDivide
	}
}

func ApplyRechargeMultiplier(value float64, multiplier *float64, mode string) float64 {
	factor := RechargeCostFactor(multiplier, mode)
	if multiplier == nil || *multiplier <= 0 || math.IsNaN(*multiplier) || math.IsInf(*multiplier, 0) {
		return value
	}
	return round4(value * factor)
}

// RechargeCostFactor returns the factor applied to an upstream amount before
// exposing it in the compatibility display fields. Invalid or missing
// multipliers intentionally resolve to 1 so callers cannot accidentally
// produce NaN/Inf values or double-convert a cost.
func RechargeCostFactor(multiplier *float64, mode string) float64 {
	if multiplier == nil || *multiplier <= 0 || math.IsNaN(*multiplier) || math.IsInf(*multiplier, 0) {
		return 1
	}
	if NormalizeRechargeMultiplierMode(mode) == RechargeMultiplierModeMultiply {
		return *multiplier
	}
	return 1 / *multiplier
}

func round4(value float64) float64 {
	return math.Round(value*10000) / 10000
}

// AuthSession 登录后产生的会话凭据。明文，由 channel 层负责加密落库。
type AuthSession struct {
	// UserID 上游账号 ID 字符串。NewAPI 必须在后续请求头里附带 `New-Api-User: <id>`。
	// 不是机密信息，channel 层按明文存。
	UserID       string
	AccessToken  string
	RefreshToken string
	Cookie       string
	CSRFToken    string
	ExpiresAt    time.Time
	// AccountConcurrency is present when the provider authenticated an account
	// identity. Limit nil means that account did not disclose a concurrency
	// limit; a non-nil zero means the provider explicitly reported unlimited
	// concurrency.
	AccountConcurrency *AccountConcurrencyFact
}

const AccountConcurrencyScopeAccount = "account"

// AccountConcurrencyFact is an upstream-authenticated account identity and
// optional account-level limit. It deliberately excludes provider-specific
// real-time counters such as an API key's current_concurrency: those counters
// are observations, not limits. AccountID is internal identity material and
// must never be returned by public APIs or written to logs.
type AccountConcurrencyFact struct {
	Provider   ChannelType
	AccountID  string `json:"-"`
	Limit      *int
	Scope      string
	Source     string
	ObservedAt time.Time
}

// BalanceResult 一次余额采集结果。RawBalance 是上游原始展示金额，Balance
// 保留为已按充值倍率换算后的兼容字段。
type BalanceResult struct {
	Balance       float64
	RawBalance    float64
	AppliedFactor float64
	FactorSource  string
	SampledAt     time.Time
	// AccountConcurrency carries the same explicit fact when the balance
	// endpoint is also the provider's authenticated account-profile endpoint.
	AccountConcurrency *AccountConcurrencyFact
}

// CostResult 一次消费采集结果。Raw*Cost 是上游原始展示金额，*Cost
// 保留为已按充值倍率换算后的兼容字段，单位通常是 USD。
type CostResult struct {
	TodayCost                 float64
	TotalCost                 float64
	RawTodayCost              float64
	RawTotalCost              float64
	AppliedFactor             float64
	FactorSource              string
	CumulativeCostUnavailable bool
	SampledAt                 time.Time
}

// HistoricalCostResult is an upstream raw-cost aggregate for a complete
// calendar-day range. The connector defines the calendar using the channel's
// configured timezone.
type HistoricalCostResult struct {
	RawCost float64
}

// RateResult 一条倍率记录。ModelName 在两个上游分别是"分组名"，Description 是该分组的描述（来自上游接口）。
type RateResult struct {
	ModelName       string
	Description     string
	Ratio           float64
	CompletionRatio float64
}

// RateSnapshotResult explicitly records whether the connector obtained a full
// snapshot. Callers must never infer deletions from a partial page or a
// partially decoded response.
type RateSnapshotResult struct {
	Items    []RateResult
	Complete bool
}

// ModelPriceResult is one provider-published, token-based model price. Prices
// are expressed in the provider's raw USD cost unit per one million tokens;
// channel group and funding multipliers are applied only when a request fact is
// frozen for dispatch.
type ModelPriceResult struct {
	ModelName                         string
	EnabledGroups                     []string
	UpstreamGroupID                   int64
	UpstreamGroupName                 string
	GroupRatio                        *float64
	InputUnitPriceUSD                 float64
	OutputUnitPriceUSD                float64
	CacheReadUnitPriceUSD             *float64
	CacheWriteUnitPriceUSD            *float64
	LongContextBasis                  string
	LongContextThresholdTokens        *int64
	LongContextInputUnitPriceUSD      *float64
	LongContextOutputUnitPriceUSD     *float64
	LongContextCacheReadUnitPriceUSD  *float64
	LongContextCacheWriteUnitPriceUSD *float64
}

// ModelPriceSnapshotResult is a complete, normalized provider pricing
// observation. A connector must not return Complete=true when the monetary
// unit, response shape, or source checksum could not be verified.
type ModelPriceSnapshotResult struct {
	Items          []ModelPriceResult
	Complete       bool
	SourceVersion  string
	SourceChecksum string
	QuotaPerUnit   float64
	ObservedAt     time.Time
}

// ProviderFactorObservation is one provider-served monetary denomination
// observation. It deliberately contains no balance or cost fields so callers
// that only need to fence a rate snapshot do not amplify that operation into
// repeated full financial samples.
type ProviderFactorObservation struct {
	AppliedFactor float64
	FactorSource  string
	SampledAt     time.Time
}

// AnnouncementResult 一条从上游同步到的公告。
type AnnouncementResult struct {
	SourceKey       string
	Title           string
	Content         string
	Type            string
	Link            string
	PublishedAt     *time.Time
	SourceUpdatedAt *time.Time
}

// RedeemResult 一次兑换的统一结果。
type RedeemResult struct {
	Message        string   `json:"message"`
	Type           string   `json:"type"`
	Value          float64  `json:"value"`
	NewBalance     *float64 `json:"new_balance,omitempty"`
	NewConcurrency *int     `json:"new_concurrency,omitempty"`
	GroupName      *string  `json:"group_name,omitempty"`
	ValidityDays   *int     `json:"validity_days,omitempty"`
}

type RechargeMethod struct {
	Type      string  `json:"type"`
	Name      string  `json:"name"`
	MinAmount float64 `json:"min_amount"`
	MaxAmount float64 `json:"max_amount"`
}

type RechargeInfo struct {
	AmountLabel       string           `json:"amount_label"`
	AmountStep        float64          `json:"amount_step"`
	MinAmount         float64          `json:"min_amount"`
	MaxAmount         float64          `json:"max_amount"`
	PresetAmounts     []float64        `json:"preset_amounts"`
	HelpText          string           `json:"help_text,omitempty"`
	HelpImageURL      string           `json:"help_image_url,omitempty"`
	AlipayForceQRCode bool             `json:"alipay_force_qrcode"`
	Methods           []RechargeMethod `json:"methods"`
}

type RechargeRequest struct {
	Amount        float64 `json:"amount"`
	PaymentMethod string  `json:"payment_method"`
	IsMobile      bool    `json:"is_mobile"`
}

type RechargeLaunch struct {
	Mode       string            `json:"mode"`
	QRCode     string            `json:"qr_code,omitempty"`
	PayURL     string            `json:"pay_url,omitempty"`
	FormAction string            `json:"form_action,omitempty"`
	FormFields map[string]string `json:"form_fields,omitempty"`
	ExpiresAt  *time.Time        `json:"expires_at,omitempty"`
}

// ErrUnsupported indicates that an optional connector capability is not
// exposed by the upstream deployment. Callers should use errors.Is because
// connectors may wrap it with endpoint context.
var ErrUnsupported = errors.New("connector capability unsupported")
var ErrTurnstileSiteKeyMissing = errors.New("turnstile is enabled but site key is missing")

// RechargeRecord is one completed balance recharge. PaidAmount is actual
// money paid, while CreditedAmount is the raw balance credited upstream.
type RechargeRecord struct {
	SourceID       string    `json:"source_id"`
	PaidAmount     *float64  `json:"paid_amount,omitempty"`
	CreditedAmount float64   `json:"credited_amount"`
	CompletedAt    time.Time `json:"completed_at"`
}

type SubscriptionMethod struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

type SubscriptionPlan struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Description     string   `json:"description,omitempty"`
	Price           float64  `json:"price"`
	Currency        string   `json:"currency,omitempty"`
	Validity        string   `json:"validity,omitempty"`
	GroupName       string   `json:"group_name,omitempty"`
	Quota           float64  `json:"quota,omitempty"`
	DailyLimitUSD   *float64 `json:"daily_limit_usd,omitempty"`
	WeeklyLimitUSD  *float64 `json:"weekly_limit_usd,omitempty"`
	MonthlyLimitUSD *float64 `json:"monthly_limit_usd,omitempty"`
	Features        []string `json:"features,omitempty"`
	PaymentMethods  []string `json:"payment_methods,omitempty"`
}

type SubscriptionInfo struct {
	Plans   []SubscriptionPlan   `json:"plans"`
	Methods []SubscriptionMethod `json:"methods"`
}

type SubscriptionRequest struct {
	PlanID        string `json:"plan_id"`
	PaymentMethod string `json:"payment_method"`
	IsMobile      bool   `json:"is_mobile"`
}

type SubscriptionLaunch = RechargeLaunch

type SubscriptionUsageWindow struct {
	LimitUSD         float64    `json:"limit_usd"`
	UsedUSD          float64    `json:"used_usd"`
	RemainingUSD     float64    `json:"remaining_usd"`
	RemainingPercent float64    `json:"remaining_percent"`
	UsedPercent      float64    `json:"used_percent"`
	WindowStart      *time.Time `json:"window_start,omitempty"`
	ResetsAt         *time.Time `json:"resets_at,omitempty"`
	ResetsInSeconds  int64      `json:"resets_in_seconds"`
}

type SubscriptionUsage struct {
	ID            int64                    `json:"id"`
	GroupID       int64                    `json:"group_id"`
	GroupName     string                   `json:"group_name"`
	Status        string                   `json:"status"`
	StartsAt      *time.Time               `json:"starts_at,omitempty"`
	ExpiresAt     *time.Time               `json:"expires_at,omitempty"`
	ExpiresInDays int                      `json:"expires_in_days"`
	Daily         *SubscriptionUsageWindow `json:"daily,omitempty"`
	Weekly        *SubscriptionUsageWindow `json:"weekly,omitempty"`
	Monthly       *SubscriptionUsageWindow `json:"monthly,omitempty"`
	Cycle         *SubscriptionUsageWindow `json:"cycle,omitempty"`
	Total         *SubscriptionUsageWindow `json:"total,omitempty"`
}

type SubscriptionUsageInfo struct {
	Items []SubscriptionUsage `json:"items"`
}

type APIKeyQuery struct {
	Page                         int    `json:"page"`
	PageSize                     int    `json:"page_size"`
	Search                       string `json:"search"`
	Status                       string `json:"status"`
	GroupID                      string `json:"group_id"`
	RequireCompleteGroupIdentity bool   `json:"-"`
}

type APIKeyPage struct {
	Items    []APIKey `json:"items"`
	Total    int64    `json:"total"`
	Page     int      `json:"page"`
	PageSize int      `json:"page_size"`
	Pages    int      `json:"pages"`
}

type APIKeyGroup struct {
	ID          *int64  `json:"id,omitempty"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Ratio       float64 `json:"ratio"`
}

type APIKey struct {
	ID                 int64      `json:"id"`
	Key                string     `json:"key"`
	Name               string     `json:"name"`
	Status             string     `json:"status"`
	Group              string     `json:"group,omitempty"`
	GroupName          string     `json:"group_name,omitempty"`
	GroupDescription   string     `json:"group_description,omitempty"`
	GroupRatio         float64    `json:"group_ratio"`
	GroupID            *int64     `json:"group_id,omitempty"`
	Quota              float64    `json:"quota"`
	QuotaUsed          float64    `json:"quota_used"`
	UnlimitedQuota     bool       `json:"unlimited_quota"`
	ExpiredTime        int64      `json:"expired_time"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
	CreatedAt          *time.Time `json:"created_at,omitempty"`
	UpdatedAt          *time.Time `json:"updated_at,omitempty"`
	LastUsedAt         *time.Time `json:"last_used_at,omitempty"`
	AllowIPs           string     `json:"allow_ips,omitempty"`
	IPWhitelist        []string   `json:"ip_whitelist,omitempty"`
	IPBlacklist        []string   `json:"ip_blacklist,omitempty"`
	ModelLimitsEnabled bool       `json:"model_limits_enabled"`
	ModelLimits        string     `json:"model_limits,omitempty"`
	CrossGroupRetry    bool       `json:"cross_group_retry"`
	RateLimit5h        float64    `json:"rate_limit_5h"`
	RateLimit1d        float64    `json:"rate_limit_1d"`
	RateLimit7d        float64    `json:"rate_limit_7d"`
	Usage5h            float64    `json:"usage_5h"`
	Usage1d            float64    `json:"usage_1d"`
	Usage7d            float64    `json:"usage_7d"`
	// ObservedFields distinguishes an explicit zero value from an upstream
	// list endpoint that omitted the field. Recovery only fingerprints fields
	// the connector actually observed.
	ObservedFields APIKeyObservedFields `json:"-"`
	NullFields     APIKeyObservedFields `json:"-"`
	Source         APIKeySource         `json:"-"`
	// GroupDirectoryComplete proves that a missing GroupName came from a
	// successful full group-directory lookup, rather than an incomplete key
	// response or a failed provider request.
	GroupDirectoryComplete bool                 `json:"-"`
	ObservedIdentityFields APIKeyIdentityFields `json:"-"`
	NullIdentityFields     APIKeyIdentityFields `json:"-"`
}

// APIKeyDeletionIdentity contains the stable remote fields that must still
// match immediately before deleting a managed key. Usage counters and recent
// activity timestamps are intentionally excluded because they are mutable.
type APIKeyDeletionIdentity struct {
	ID                 int64
	Name               string
	Status             string
	Group              string
	GroupName          string
	GroupID            *int64
	Source             APIKeySource
	Quota              float64
	UnlimitedQuota     bool
	ExpiredTime        int64
	ExpiresAt          *time.Time
	CreatedAt          *time.Time
	AllowIPs           string
	IPWhitelist        []string
	IPBlacklist        []string
	ModelLimitsEnabled bool
	ModelLimits        string
	CrossGroupRetry    bool
	RateLimit5h        float64
	RateLimit1d        float64
	RateLimit7d        float64
	ObservedFields     APIKeyObservedFields
	NullFields         APIKeyObservedFields
}

func ManagedAPIKeyDeletionIdentity(key APIKey) APIKeyDeletionIdentity {
	return APIKeyDeletionIdentity{
		ID: key.ID, Name: strings.TrimSpace(key.Name), Status: strings.TrimSpace(key.Status),
		Group: strings.TrimSpace(key.Group), GroupName: strings.TrimSpace(key.GroupName),
		GroupID: cloneAPIKeyInt64Pointer(key.GroupID), Source: key.Source,
		Quota: key.Quota, UnlimitedQuota: key.UnlimitedQuota, ExpiredTime: key.ExpiredTime,
		ExpiresAt: cloneAPIKeyTimePointer(key.ExpiresAt), CreatedAt: cloneAPIKeyTimePointer(key.CreatedAt),
		AllowIPs: strings.TrimSpace(key.AllowIPs), IPWhitelist: append([]string(nil), key.IPWhitelist...),
		IPBlacklist: append([]string(nil), key.IPBlacklist...), ModelLimitsEnabled: key.ModelLimitsEnabled,
		ModelLimits: strings.TrimSpace(key.ModelLimits), CrossGroupRetry: key.CrossGroupRetry,
		RateLimit5h: key.RateLimit5h, RateLimit1d: key.RateLimit1d, RateLimit7d: key.RateLimit7d,
		ObservedFields: key.ObservedFields, NullFields: key.NullFields,
	}
}

func cloneAPIKeyInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneAPIKeyTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

type APIKeySource string

const (
	APIKeySourceNewAPI  APIKeySource = "newapi"
	APIKeySourceSub2API APIKeySource = "sub2api"
)

var ErrManagedAPIKeyDeletionIdentityChanged = errors.New("managed api key identity changed before deletion")

type APIKeyIdentityFields struct {
	Name   bool
	Status bool
	Group  bool
}

type APIKeyObservedFields struct {
	Quota              bool
	UnlimitedQuota     bool
	ExpiredTime        bool
	ExpiresAt          bool
	AllowIPs           bool
	IPWhitelist        bool
	IPBlacklist        bool
	ModelLimitsEnabled bool
	ModelLimits        bool
	CrossGroupRetry    bool
	RateLimit5h        bool
	RateLimit1d        bool
	RateLimit7d        bool
}

type APIKeyCreateRequest struct {
	Name               string                     `json:"name"`
	CustomKey          string                     `json:"custom_key,omitempty"`
	Group              string                     `json:"group,omitempty"`
	GroupID            *int64                     `json:"group_id,omitempty"`
	RemainQuota        *int                       `json:"remain_quota,omitempty"`
	Quota              *float64                   `json:"quota,omitempty"`
	UnlimitedQuota     *bool                      `json:"unlimited_quota,omitempty"`
	ExpiredTime        *int64                     `json:"expired_time,omitempty"`
	ExpiresInDays      *int                       `json:"expires_in_days,omitempty"`
	ModelLimitsEnabled *bool                      `json:"model_limits_enabled,omitempty"`
	ModelLimits        string                     `json:"model_limits,omitempty"`
	AllowIPs           string                     `json:"allow_ips,omitempty"`
	IPWhitelist        []string                   `json:"ip_whitelist,omitempty"`
	IPBlacklist        []string                   `json:"ip_blacklist,omitempty"`
	CrossGroupRetry    *bool                      `json:"cross_group_retry,omitempty"`
	RateLimit5h        *float64                   `json:"rate_limit_5h,omitempty"`
	RateLimit1d        *float64                   `json:"rate_limit_1d,omitempty"`
	RateLimit7d        *float64                   `json:"rate_limit_7d,omitempty"`
	RecoveryFence      *APIKeyCreateRecoveryFence `json:"-"`
}

// APIKeyCreateRecoveryFence binds automatic recovery to one account and one
// currently-present upstream group generation. Connectors must not serialize
// it into provider requests.
type APIKeyCreateRecoveryFence struct {
	ChannelIdentityRevision uint64
	UpstreamGroupID         uint
	PresenceGeneration      uint64
	CanonicalGroupName      string
}

func (f APIKeyCreateRecoveryFence) Valid() bool {
	return f.ChannelIdentityRevision > 0 && f.UpstreamGroupID > 0 &&
		f.PresenceGeneration > 0 && strings.TrimSpace(f.CanonicalGroupName) != ""
}

type APIKeyUpdateRequest struct {
	Name                *string  `json:"name,omitempty"`
	Group               *string  `json:"group,omitempty"`
	GroupID             *int64   `json:"group_id,omitempty"`
	Status              *string  `json:"status,omitempty"`
	RemainQuota         *int     `json:"remain_quota,omitempty"`
	Quota               *float64 `json:"quota,omitempty"`
	UnlimitedQuota      *bool    `json:"unlimited_quota,omitempty"`
	ExpiredTime         *int64   `json:"expired_time,omitempty"`
	ExpiresAt           *string  `json:"expires_at,omitempty"`
	ModelLimitsEnabled  *bool    `json:"model_limits_enabled,omitempty"`
	ModelLimits         *string  `json:"model_limits,omitempty"`
	AllowIPs            *string  `json:"allow_ips,omitempty"`
	IPWhitelist         []string `json:"ip_whitelist,omitempty"`
	IPBlacklist         []string `json:"ip_blacklist,omitempty"`
	CrossGroupRetry     *bool    `json:"cross_group_retry,omitempty"`
	RateLimit5h         *float64 `json:"rate_limit_5h,omitempty"`
	RateLimit1d         *float64 `json:"rate_limit_1d,omitempty"`
	RateLimit7d         *float64 `json:"rate_limit_7d,omitempty"`
	ResetQuota          *bool    `json:"reset_quota,omitempty"`
	ResetRateLimitUsage *bool    `json:"reset_rate_limit_usage,omitempty"`
}

// RequestCostQuery 用请求的稳定字段关联上游用量记录。APIKeyID 必须是实际转发
// 所使用的上游 key ID；调用方负责对旧缓存做身份校验和回填。
type RequestCostQuery struct {
	APIKeyID         int64
	APIKeyName       string
	GroupName        string
	ModelName        string
	PromptTokens     int64
	CompletionTokens int64
	HasTokenMetrics  bool
	RequestID        string
	StartedAt        time.Time
	FinishedAt       time.Time
}

// RequestCostResult 是上游最终计费记录。Cost 使用上游 raw 展示货币
// （通常为 USD），暂不在 connector 层应用充值倍率；需要实际费用的调用方
// 应显式使用渠道倍率转换，避免重复换算。
type RequestCostResult struct {
	APIKeyID             int64
	UsageID              string
	ModelName            string
	Cost                 float64
	Source               string
	PromptTokens         int64
	CompletionTokens     int64
	TotalTokens          int64
	HasTokenMetrics      bool
	CacheCreationTokens  int64
	CacheReadTokens      int64
	HasCacheTokenMetrics bool
}

// Connector 上游连接器统一接口。
//
//   - GetTurnstileSiteKey  从上游公开接口读取 Turnstile site key（无需鉴权）
//   - Login                登录获取 session
//   - CheckAuth            使用现有 session 做一次轻量校验，确认未过期
//   - GetBalance           拉取当前余额
//   - GetRates             拉取当前所有可见的倍率
//   - RedeemCode           在线兑换兑换码
type Connector interface {
	// GetTurnstileSiteKey 返回上游当前的 Turnstile site key。
	// 站点没有开启 Turnstile 时返回 ""（不视作错误）。
	GetTurnstileSiteKey(ctx context.Context, channel *Channel) (string, error)

	Login(ctx context.Context, channel *Channel) (*AuthSession, error)
	CheckAuth(ctx context.Context, channel *Channel, session *AuthSession) error
	GetBalance(ctx context.Context, channel *Channel, session *AuthSession) (*BalanceResult, error)
	GetCosts(ctx context.Context, channel *Channel, session *AuthSession) (*CostResult, error)
	GetRates(ctx context.Context, channel *Channel, session *AuthSession) (*RateSnapshotResult, error)
	GetAnnouncements(ctx context.Context, channel *Channel, session *AuthSession) ([]AnnouncementResult, error)
	RedeemCode(ctx context.Context, channel *Channel, session *AuthSession, code string) (*RedeemResult, error)
	GetRechargeInfo(ctx context.Context, channel *Channel, session *AuthSession) (*RechargeInfo, error)
	CreateRecharge(ctx context.Context, channel *Channel, session *AuthSession, req RechargeRequest) (*RechargeLaunch, error)
	GetSubscriptionInfo(ctx context.Context, channel *Channel, session *AuthSession) (*SubscriptionInfo, error)
	CreateSubscription(ctx context.Context, channel *Channel, session *AuthSession, req SubscriptionRequest) (*SubscriptionLaunch, error)
	GetSubscriptionUsage(ctx context.Context, channel *Channel, session *AuthSession) (*SubscriptionUsageInfo, error)
	ListAPIKeys(ctx context.Context, channel *Channel, session *AuthSession, query APIKeyQuery) (*APIKeyPage, error)
	ListAPIKeyGroups(ctx context.Context, channel *Channel, session *AuthSession) ([]APIKeyGroup, error)
	CreateAPIKey(ctx context.Context, channel *Channel, session *AuthSession, req APIKeyCreateRequest) (*APIKey, error)
	UpdateAPIKey(ctx context.Context, channel *Channel, session *AuthSession, id int64, req APIKeyUpdateRequest) (*APIKey, error)
	DeleteAPIKey(ctx context.Context, channel *Channel, session *AuthSession, id int64) error
	RevealAPIKey(ctx context.Context, channel *Channel, session *AuthSession, id int64) (string, error)
}

// SessionRefresher 是支持 refresh_token 续期的 connector 可选能力。
type SessionRefresher interface {
	RefreshSession(ctx context.Context, channel *Channel, session *AuthSession) (*AuthSession, error)
}

// RequestCostLookup 是能按请求查询上游实际扣费的 connector 可选能力。
type RequestCostLookup interface {
	LookupRequestCost(ctx context.Context, channel *Channel, session *AuthSession, query RequestCostQuery) (*RequestCostResult, error)
}

// HistoricalCostSampler is the optional ability to aggregate raw cost over
// complete calendar days. startDay is inclusive and endDay is exclusive; both
// inputs must be midnight boundaries in the channel's configured timezone.
type HistoricalCostSampler interface {
	GetHistoricalCosts(ctx context.Context, channel *Channel, session *AuthSession, startDay, endDay time.Time) (*HistoricalCostResult, error)
}

// ModelPricingProvider is optional because not every supported upstream
// exposes its active model prices. Callers retain the last trusted snapshot
// when this operation is unavailable or fails.
type ModelPricingProvider interface {
	GetModelPrices(ctx context.Context, channel *Channel, session *AuthSession) (*ModelPriceSnapshotResult, error)
}

// RechargeHistory is the optional ability to list completed balance
// recharges. It is intentionally separate from Connector because older or
// customized upstream deployments may not expose a history endpoint.
type RechargeHistory interface {
	GetRechargeHistory(ctx context.Context, channel *Channel, session *AuthSession) ([]RechargeRecord, error)
}

// ProviderFactorSampler is the optional lightweight ability to read the
// provider's current monetary factor. Implementations must return an error when
// provider evidence is unavailable; silently substituting the default factor
// would make a rate-snapshot fence accept mixed monetary units.
type ProviderFactorSampler interface {
	GetProviderFactor(ctx context.Context, channel *Channel, session *AuthSession) (*ProviderFactorObservation, error)
}

type ProxySetter interface {
	SetProxy(proxyURL string) error
}

type HTTPConfig struct {
	Timeout   time.Duration
	UserAgent string
}

// SetHTTPClientTLSHandshakeTimeout keeps the transport handshake deadline in
// lockstep with the connector's existing request timeout. Cloning avoids
// mutating http.DefaultTransport or a transport shared by another client.
func SetHTTPClientTLSHandshakeTimeout(client *http.Client, timeout time.Duration) {
	if client == nil || timeout <= 0 {
		return
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
	} else {
		transport = transport.Clone()
	}
	transport.TLSHandshakeTimeout = timeout
	client.Transport = transport
}

type HTTPConfigSetter interface {
	SetHTTPConfig(cfg HTTPConfig)
}

// Factory 构造一个全新的 Connector 实例。
type Factory func() Connector

var (
	mu       sync.RWMutex
	registry = map[ChannelType]Factory{}
)

// Register 由子包在 init() 中调用，注册其类型对应的 Connector 构造器。
func Register(t ChannelType, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	registry[t] = f
}

// For 按 ChannelType 取一个新的 Connector。未注册返回错误。
func For(t ChannelType) (Connector, error) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := registry[t]
	if !ok {
		return nil, fmt.Errorf("connector %q is not registered (did you forget the blank import?)", t)
	}
	return f(), nil
}
