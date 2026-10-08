package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/providerid"
	"gorm.io/gorm"
)

// ChannelType 上游渠道类型。
type ChannelType string

const (
	ChannelTypeNewAPI  ChannelType = "newapi"
	ChannelTypeSub2API ChannelType = "sub2api"
)

// CredentialMode 渠道凭据模式：
//   - password: 经典模式，存账号 + 密码，由 Connector 走完整登录流程
//   - token:    跳过登录，存用户已有的 cookie / access_token，直接构造 AuthSession
//
// token 模式不依赖打码 / 不会自动续期，token 失效时表现为 last_error 显示鉴权失败。
type CredentialMode string

const (
	CredentialModePassword CredentialMode = "password"
	CredentialModeToken    CredentialMode = "token"
	CredentialModeDirect   CredentialMode = "direct"
)

const (
	DirectChannelGroupName       = "direct"
	DirectChannelUpstreamKeyName = "BridgeOps Direct"
)

const (
	maxDirectModelCount = 512
	maxDirectModelIDLen = 256
)

// ParseDirectModels validates and normalizes the administrator-selected model list.
func ParseDirectModels(raw string) ([]string, error) {
	var input []string
	if err := json.Unmarshal([]byte(raw), &input); err != nil {
		return nil, fmt.Errorf("direct models must be a JSON array: %w", err)
	}
	seen := make(map[string]struct{}, len(input))
	models := make([]string, 0, len(input))
	for _, item := range input {
		model := strings.TrimSpace(item)
		if model == "" {
			continue
		}
		if len(model) > maxDirectModelIDLen {
			return nil, fmt.Errorf("direct model id exceeds %d characters", maxDirectModelIDLen)
		}
		if strings.Contains(model, ",") {
			return nil, errors.New("direct model id cannot contain a comma")
		}
		if _, ok := seen[model]; ok {
			continue
		}
		seen[model] = struct{}{}
		models = append(models, model)
		if len(models) > maxDirectModelCount {
			return nil, fmt.Errorf("direct model list cannot exceed %d models", maxDirectModelCount)
		}
	}
	if len(models) == 0 {
		return nil, errors.New("direct channel requires at least one model")
	}
	return models, nil
}

func EncodeDirectModels(models []string) string {
	encoded, _ := json.Marshal(models)
	return string(encoded)
}

// Channel 上游渠道账号。Password / Turnstile API key 等敏感字段都加密保存。
//
// 注意：会话凭据（access_token / refresh_token / cookie / csrf）单独存放在 AuthSession 表。
//
// CredentialMode + PasswordCipher 的语义重载：
//   - password 模式（默认）：Username + PasswordCipher 存账号密码，由 Connector.Login 用
//   - token    模式：PasswordCipher 存 JSON blob（NewAPI: {cookie,user_id} / Sub2API: {access_token,refresh_token}），
//     channel.Service 解析后直接构造 AuthSession，跳过 Login。Username 字段在 token 模式下保留
//     用户填写的备注（一般是邮箱），仅做展示。
//
// 复用 PasswordCipher 而不新增 TokenCipher 是为了让现有的 GORM 行 / 加密路径 / 迁移流程零变动。
type Channel struct {
	ID                     uint           `gorm:"primaryKey" json:"id"`
	Name                   string         `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type                   ChannelType    `gorm:"size:32;not null;index" json:"type"`
	GatewayApp             GatewayApp     `gorm:"size:32;not null;default:'codex';index" json:"gateway_app"`
	SiteURL                string         `gorm:"size:512;not null" json:"site_url"`
	APIURL                 string         `gorm:"size:512;not null;default:''" json:"api_url,omitempty"`
	Username               string         `gorm:"size:256;not null" json:"username"`
	SortOrder              int            `gorm:"not null;default:1" json:"sort_order"`
	PasswordCipher         string         `gorm:"size:4096;not null" json:"-"`
	CredentialMode         CredentialMode `gorm:"size:16;not null;default:'password'" json:"credential_mode"`
	DirectModels           string         `gorm:"type:text;not null;default:'[]'" json:"direct_models,omitempty"`
	LoginExtraParams       string         `gorm:"type:text" json:"login_extra_params"`
	TurnstileEnabled       bool           `gorm:"default:false" json:"turnstile_enabled"`
	IgnoreAnnouncements    bool           `gorm:"default:false" json:"ignore_announcements"`
	SubscriptionEnabled    bool           `gorm:"default:false" json:"subscription_enabled"`
	ProxyEnabled           bool           `gorm:"default:false" json:"proxy_enabled"`
	NetworkProxyID         *uint          `gorm:"index" json:"network_proxy_id,omitempty"`
	NetworkProxy           *NetworkProxy  `gorm:"foreignKey:NetworkProxyID;references:ID;constraint:OnUpdate:RESTRICT,OnDelete:RESTRICT" json:"-"`
	CaptchaConfigID        *uint          `json:"captcha_config_id,omitempty"`
	BalanceThreshold       float64        `gorm:"default:0" json:"balance_threshold"`
	RechargeMultiplier     *float64       `json:"recharge_multiplier,omitempty"`
	RechargeMultiplierMode string         `gorm:"size:16;not null;default:'divide'" json:"recharge_multiplier_mode"`
	MonitorEnabled         bool           `gorm:"default:true;index:idx_channels_monitor_retry,priority:1" json:"monitor_enabled"`
	LatencyProbeEnabled    bool           `gorm:"default:true" json:"latency_probe_enabled"`
	// UpstreamAccountIdentityRevision changes only when fields that can select
	// a different upstream account or API origin change. Account-level facts
	// captured by an older configuration must not bind to the new one.
	UpstreamAccountIdentityRevision uint64 `gorm:"not null;default:1" json:"-"`
	// AdminRevision changes only for administrator-facing configuration
	// commands. It is independent from monitoring telemetry and automatic token
	// refreshes, so an open edit form can detect a real lost update without
	// becoming stale after every balance poll.
	AdminRevision uint64 `gorm:"not null;default:1" json:"admin_revision"`
	// DeletionPending is the durable fail-closed boundary for whole-channel
	// deletion. A delete request never removes local evidence synchronously:
	// it first freezes this channel under one monotonically increasing epoch,
	// bound to the account identity that must be revalidated by the remote-key
	// coordinator before a later physical finalizer can run.
	DeletionPending          bool       `gorm:"not null;default:false;index" json:"deletion_pending,omitempty"`
	DeletionEpoch            uint64     `gorm:"not null;default:0" json:"deletion_epoch,omitempty"`
	DeletionIdentityRevision uint64     `gorm:"not null;default:0" json:"-"`
	DeletionState            string     `gorm:"size:32;not null;default:''" json:"deletion_state,omitempty"`
	DeletionRequestedAt      *time.Time `json:"deletion_requested_at,omitempty"`
	CostUnitEpoch            uint64     `gorm:"not null;default:1" json:"cost_unit_epoch"`
	RateSnapshotObservedAt   *time.Time `json:"-"`
	ModelPriceObservedAt     *time.Time `json:"-"`

	// 最近一次采集结果（聚合视图，便于列表页直接展示）
	LastBalance    *float64 `json:"last_balance,omitempty"`
	RawLastBalance *float64 `json:"raw_last_balance,omitempty"`
	// BalanceError is independent from LastError, which belongs to the rate/login
	// sync recovery loop. A successful rate snapshot cannot prove a balance
	// observation recovered.
	BalanceError   string     `gorm:"type:text;not null;default:''" json:"balance_error,omitempty"`
	BalanceErrorAt *time.Time `json:"balance_error_at,omitempty"`
	// UpstreamAccountBalanceInsufficient is a durable account-level fact, not a
	// generic collection error. The revision fence prevents an observation from
	// an old account identity from blocking a newly configured account.
	UpstreamAccountBalanceInsufficient         bool       `gorm:"not null;default:false" json:"upstream_account_balance_insufficient,omitempty"`
	UpstreamAccountBalanceInsufficientAt       *time.Time `json:"upstream_account_balance_insufficient_at,omitempty"`
	UpstreamAccountBalanceInsufficientRevision uint64     `gorm:"not null;default:0" json:"-"`
	FundingFactor                              float64    `gorm:"not null;default:1" json:"funding_factor"`
	FundingFactorSource                        string     `gorm:"size:32;not null;default:'default'" json:"funding_factor_source"`
	LastBalanceAt                              *time.Time `json:"last_balance_at,omitempty"`
	TodayCost                                  *float64   `json:"today_cost,omitempty"`
	RawTodayCost                               *float64   `json:"raw_today_cost,omitempty"`
	TodayCostAt                                *time.Time `json:"today_cost_at,omitempty"`
	TotalCost                                  *float64   `json:"total_cost,omitempty"`
	RawTotalCost                               *float64   `json:"raw_total_cost,omitempty"`
	LastError                                  string     `gorm:"type:text" json:"last_error,omitempty"`
	// SyncFailureCount and NextSyncRetryAt persist the recovery cadence for a
	// channel that cannot refresh its upstream data. They are intentionally on
	// the channel row so a restart does not reset the 5-minute/30-minute backoff.
	SyncFailureCount int        `gorm:"not null;default:0" json:"sync_failure_count,omitempty"`
	NextSyncRetryAt  *time.Time `gorm:"index:idx_channels_monitor_retry,priority:2" json:"next_sync_retry_at,omitempty"`
	RequestCount     int64      `gorm:"->;-:migration" json:"request_count"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ChannelDirectCredential is the administrator-owned source of truth for one
// direct routing group. GatewayUpstreamKey remains a disposable runtime
// projection so an authentication failure can disable the exact routed key
// without erasing the saved configuration.
type ChannelDirectCredential struct {
	ID              uint      `gorm:"primaryKey" json:"id"`
	ChannelID       uint      `gorm:"not null;uniqueIndex:idx_channel_direct_credential_group" json:"channel_id"`
	GroupName       string    `gorm:"size:128;not null;uniqueIndex:idx_channel_direct_credential_group" json:"group_name"`
	BaseURL         string    `gorm:"size:512;not null" json:"base_url"`
	APIKeyCipher    string    `gorm:"type:text;not null" json:"-"`
	APIKeyPrefix    string    `gorm:"size:24;not null;default:''" json:"api_key_prefix"`
	Models          string    `gorm:"type:text;not null;default:'[]'" json:"models"`
	Ratio           float64   `gorm:"not null;default:1" json:"ratio"`
	CompletionRatio float64   `gorm:"not null;default:1" json:"completion_ratio"`
	Enabled         bool      `gorm:"not null;default:true" json:"enabled"`
	Revision        uint64    `gorm:"not null;default:1" json:"revision"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (ChannelDirectCredential) TableName() string { return "channel_direct_credentials" }

func (Channel) TableName() string { return "channels" }

// ChannelDeletionIntentStatus is the recoverable whole-channel deletion
// coordinator state. It intentionally lives outside channels so an audit and
// pending remote work survive the eventual physical deletion of that row.
type ChannelDeletionIntentStatus string

const (
	ChannelDeletionIntentPendingRemoteCleanup ChannelDeletionIntentStatus = "pending_remote_cleanup"
	ChannelDeletionIntentManualReviewRequired ChannelDeletionIntentStatus = "manual_review_required"
	ChannelDeletionIntentReadyForFinalization ChannelDeletionIntentStatus = "ready_for_local_finalize"
	ChannelDeletionIntentFinalized            ChannelDeletionIntentStatus = "finalized"
)

// ChannelDeletionIntent is mutable coordinator state, not a foreign-key
// child of Channel. The missing FK is deliberate: once a future finalizer
// deletes the channel row, this identity/epoch-bound tombstone remains
// available for recovery and audit.
type ChannelDeletionIntent struct {
	ID                      uint64                      `gorm:"primaryKey" json:"id"`
	ChannelID               uint                        `gorm:"not null;uniqueIndex:idx_channel_deletion_intent_epoch,priority:1;index:idx_channel_deletion_intent_reconcile,priority:1" json:"channel_id"`
	DeletionEpoch           uint64                      `gorm:"not null;uniqueIndex:idx_channel_deletion_intent_epoch,priority:2" json:"deletion_epoch"`
	ChannelIdentityRevision uint64                      `gorm:"not null;index:idx_channel_deletion_intent_reconcile,priority:2" json:"-"`
	ChannelNameSnapshot     string                      `gorm:"size:128;not null" json:"channel_name_snapshot"`
	ChannelTypeSnapshot     ChannelType                 `gorm:"size:32;not null" json:"channel_type_snapshot"`
	Status                  ChannelDeletionIntentStatus `gorm:"size:32;not null;default:'pending_remote_cleanup';index:idx_channel_deletion_intent_reconcile,priority:3" json:"status"`
	LastError               string                      `gorm:"type:text;not null;default:''" json:"last_error,omitempty"`
	AttemptCount            uint64                      `gorm:"not null;default:0" json:"attempt_count,omitempty"`
	LastAttemptAt           *time.Time                  `json:"last_attempt_at,omitempty"`
	NextAttemptAt           *time.Time                  `json:"next_attempt_at,omitempty"`
	FinalizedAt             *time.Time                  `json:"finalized_at,omitempty"`
	RequestedAt             time.Time                   `gorm:"not null" json:"requested_at"`
	CreatedAt               time.Time                   `gorm:"not null" json:"created_at"`
	UpdatedAt               time.Time                   `gorm:"not null" json:"updated_at"`
}

func (ChannelDeletionIntent) TableName() string { return "gateway_channel_deletion_intents" }

// ChannelDeletionKeyDisposition captures only the local, fail-closed plan at
// prepare time. It is not evidence that a remote deletion was attempted or
// confirmed. The coordinator appends later immutable audit rows after its
// inventory and ownership revalidation.
type ChannelDeletionKeyDisposition string

const (
	ChannelDeletionKeyDispositionVerifyManagedDelete ChannelDeletionKeyDisposition = "verify_managed_delete"
	ChannelDeletionKeyDispositionRetainAdopted       ChannelDeletionKeyDisposition = "retain_adopted"
	ChannelDeletionKeyDispositionManualReview        ChannelDeletionKeyDisposition = "manual_review_required"
)

// ChannelDeletionKeyAudit is append-only and deliberately has no FK to the
// intent, channel, or group. It preserves exactly which local key facts were
// seen at every decision even after the parent channel/group rows disappear.
// It must never contain plaintext API keys or ciphertext.
type ChannelDeletionKeyAudit struct {
	ID                      uint64                        `gorm:"primaryKey" json:"id"`
	IntentID                uint64                        `gorm:"not null;index:idx_channel_deletion_key_audit_intent,priority:1" json:"intent_id"`
	ChannelID               uint                          `gorm:"not null;index:idx_channel_deletion_key_audit_intent,priority:2" json:"channel_id"`
	DeletionEpoch           uint64                        `gorm:"not null;index:idx_channel_deletion_key_audit_intent,priority:3" json:"deletion_epoch"`
	ChannelIdentityRevision uint64                        `gorm:"not null" json:"-"`
	LocalKeyID              uint                          `gorm:"not null" json:"local_key_id"`
	UpstreamGroupID         *uint                         `json:"upstream_group_id,omitempty"`
	PresenceGeneration      *uint64                       `json:"presence_generation,omitempty"`
	CanonicalGroupName      string                        `gorm:"size:128;not null;default:''" json:"canonical_group_name,omitempty"`
	UpstreamKeyID           int64                         `gorm:"not null;default:0" json:"upstream_key_id,omitempty"`
	UpstreamKeyName         string                        `gorm:"size:128;not null;default:''" json:"upstream_key_name,omitempty"`
	KeyGeneration           uint64                        `gorm:"not null;default:0" json:"key_generation,omitempty"`
	OwnershipProvenance     string                        `gorm:"size:32;not null" json:"ownership_provenance"`
	Provisioning            bool                          `gorm:"not null;default:false" json:"provisioning"`
	LocalOwnershipClaim     string                        `gorm:"size:64;not null;default:''" json:"local_ownership_claim,omitempty"`
	Disposition             ChannelDeletionKeyDisposition `gorm:"size:32;not null" json:"disposition"`
	Reason                  string                        `gorm:"size:64;not null;default:''" json:"reason,omitempty"`
	CreatedAt               time.Time                     `gorm:"not null;index" json:"created_at"`
}

func (ChannelDeletionKeyAudit) TableName() string { return "gateway_channel_deletion_key_audits" }

// BeforeCreate and BeforeUpdate hold the same parent-row lock as proxy
// deletion. The foreign key is the final integrity guard; this lock keeps the
// normal API path deterministic so a concurrent delete returns in-use rather
// than racing a new channel binding.
func (c *Channel) BeforeCreate(tx *gorm.DB) error {
	return lockNetworkProxyForBinding(tx, c.NetworkProxyID)
}

func (c *Channel) BeforeUpdate(tx *gorm.DB) error {
	return lockNetworkProxyForBinding(tx, c.NetworkProxyID)
}

// NetworkProxy is an administrator-managed outbound proxy endpoint. Sensitive
// credentials are encrypted as one JSON blob and are never returned through
// the API. Channels reference this row by ID so each selected channel keeps a
// stable, isolated outbound route.
type NetworkProxy struct {
	ID               uint   `gorm:"primaryKey" json:"id"`
	Name             string `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Protocol         string `gorm:"size:16;not null" json:"protocol"`
	Host             string `gorm:"size:512;not null" json:"host"`
	Port             int    `gorm:"not null" json:"port"`
	CredentialCipher string `gorm:"type:text" json:"-"`
	// ConnectionRevision changes only when the endpoint configuration changes.
	// Health-test updates deliberately leave it untouched so active channels can
	// continue reusing their isolated HTTP client and keep-alive connections.
	ConnectionRevision uint64     `gorm:"not null;default:1" json:"-"`
	Source             string     `gorm:"size:32;not null;default:'manual'" json:"source"`
	CleanIP            bool       `gorm:"default:false" json:"clean_ip"`
	Enabled            bool       `gorm:"not null;index" json:"enabled"`
	LastTestedAt       *time.Time `json:"last_tested_at,omitempty"`
	LastLatencyMS      *int64     `json:"last_latency_ms,omitempty"`
	LastEgressIP       string     `gorm:"size:64" json:"last_egress_ip,omitempty"`
	LastError          string     `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (NetworkProxy) TableName() string { return "network_proxies" }

type ChannelAPIKeyOperationStatus string

const (
	ChannelAPIKeyOperationPending    ChannelAPIKeyOperationStatus = "pending"
	ChannelAPIKeyOperationCompleted  ChannelAPIKeyOperationStatus = "completed"
	ChannelAPIKeyOperationSuperseded ChannelAPIKeyOperationStatus = "superseded"
)

// ChannelAPIKeyOperation is the durable idempotency boundary for one managed
// upstream API-key creation. It deliberately stores only hashes and remote
// metadata; plaintext API keys must never be persisted here.
type ChannelAPIKeyOperation struct {
	ID                       uint64                       `gorm:"primaryKey" json:"id"`
	ChannelID                uint                         `gorm:"not null;uniqueIndex:idx_channel_api_key_operation_scope,priority:1;uniqueIndex:idx_channel_api_key_operation_remote,priority:1" json:"channel_id"`
	Provider                 ChannelType                  `gorm:"size:32;not null;uniqueIndex:idx_channel_api_key_operation_scope,priority:2;uniqueIndex:idx_channel_api_key_operation_remote,priority:2" json:"provider"`
	ScopeHash                string                       `gorm:"size:64;not null;uniqueIndex:idx_channel_api_key_operation_scope,priority:3" json:"scope_hash"`
	OperationGeneration      uint32                       `gorm:"not null;default:1" json:"operation_generation"`
	RequestFingerprint       string                       `gorm:"size:64;not null" json:"request_fingerprint"`
	ManagedName              string                       `gorm:"size:256;not null" json:"managed_name"`
	GroupIdentity            string                       `gorm:"size:256;not null" json:"group_identity"`
	CustomKeyHash            string                       `gorm:"size:64" json:"-"`
	Status                   ChannelAPIKeyOperationStatus `gorm:"size:24;not null;default:'pending'" json:"status"`
	RemoteID                 *int64                       `gorm:"uniqueIndex:idx_channel_api_key_operation_remote,priority:3" json:"remote_id,omitempty"`
	ChannelIdentityRevision  uint64                       `gorm:"not null;default:0" json:"-"`
	UpstreamGroupID          uint                         `gorm:"not null;default:0" json:"-"`
	PresenceGeneration       uint64                       `gorm:"not null;default:0" json:"-"`
	CanonicalGroupName       string                       `gorm:"size:128;not null;default:''" json:"-"`
	AbsenceConfirmationCount uint32                       `gorm:"not null;default:0" json:"-"`
	AbsenceFirstConfirmedAt  *time.Time                   `json:"-"`
	AbsenceLastConfirmedAt   *time.Time                   `json:"-"`
	RejectedPostStatus       int                          `gorm:"not null;default:0" json:"-"`
	RejectedPostAt           *time.Time                   `json:"-"`
	RejectedRetryAt          *time.Time                   `json:"-"`
	RejectedRetryCount       uint32                       `gorm:"not null;default:0" json:"-"`
	CompletedAt              *time.Time                   `json:"completed_at,omitempty"`
	CreatedAt                time.Time                    `json:"created_at"`
	UpdatedAt                time.Time                    `json:"updated_at"`
}

func (ChannelAPIKeyOperation) TableName() string { return "channel_api_key_operations" }

// ChannelAPIKeyOperationGeneration is an immutable snapshot of one
// commit-unknown generation that was safely superseded. The parent operation
// row remains the single current slot so pre-generation binaries retain their
// original unique-key and lookup contract after a failed deployment rollback.
type ChannelAPIKeyOperationGeneration struct {
	ID                       uint64                       `gorm:"primaryKey" json:"id"`
	OperationID              uint64                       `gorm:"not null;uniqueIndex:idx_channel_api_key_operation_generation,priority:1" json:"operation_id"`
	ChannelID                uint                         `gorm:"not null;index" json:"channel_id"`
	Provider                 ChannelType                  `gorm:"size:32;not null" json:"provider"`
	ScopeHash                string                       `gorm:"size:64;not null" json:"scope_hash"`
	OperationGeneration      uint32                       `gorm:"not null;uniqueIndex:idx_channel_api_key_operation_generation,priority:2" json:"operation_generation"`
	RequestFingerprint       string                       `gorm:"size:64;not null" json:"request_fingerprint"`
	ManagedName              string                       `gorm:"size:256;not null" json:"managed_name"`
	GroupIdentity            string                       `gorm:"size:256;not null" json:"group_identity"`
	CustomKeyHash            string                       `gorm:"size:64" json:"-"`
	Status                   ChannelAPIKeyOperationStatus `gorm:"size:24;not null" json:"status"`
	ChannelIdentityRevision  uint64                       `gorm:"not null" json:"-"`
	UpstreamGroupID          uint                         `gorm:"not null" json:"-"`
	PresenceGeneration       uint64                       `gorm:"not null" json:"-"`
	CanonicalGroupName       string                       `gorm:"size:128;not null" json:"-"`
	AbsenceConfirmationCount uint32                       `gorm:"not null" json:"-"`
	AbsenceFirstConfirmedAt  time.Time                    `gorm:"not null" json:"-"`
	AbsenceLastConfirmedAt   time.Time                    `gorm:"not null" json:"-"`
	RejectedPostStatus       int                          `gorm:"not null;default:0" json:"-"`
	RejectedPostAt           *time.Time                   `json:"-"`
	RejectedRetryAt          *time.Time                   `json:"-"`
	RejectedRetryCount       uint32                       `gorm:"not null;default:0" json:"-"`
	SupersededByGeneration   uint32                       `gorm:"not null" json:"superseded_by_generation"`
	SupersededAt             time.Time                    `gorm:"not null" json:"superseded_at"`
	CreatedAt                time.Time                    `gorm:"not null" json:"created_at"`
	UpdatedAt                time.Time                    `gorm:"not null" json:"updated_at"`
}

func (ChannelAPIKeyOperationGeneration) TableName() string {
	return "channel_api_key_operation_generations"
}

const MinimumChannelBalanceThreshold = 0.1

// EffectiveBalanceThreshold keeps the operator-configured warning threshold
// while enforcing a safety floor for routing. A zero legacy value therefore no
// longer allows an exhausted channel to remain eligible indefinitely.
func (c Channel) EffectiveBalanceThreshold() float64 {
	if c.BalanceThreshold < MinimumChannelBalanceThreshold {
		return MinimumChannelBalanceThreshold
	}
	return c.BalanceThreshold
}

func (c Channel) HasLowBalance() bool {
	return c.LastBalance != nil && *c.LastBalance < c.EffectiveBalanceThreshold()
}

// HasCurrentUpstreamAccountBalanceInsufficient only accepts a complete fact
// observed for the currently configured upstream identity.
func (c Channel) HasCurrentUpstreamAccountBalanceInsufficient() bool {
	return c.UpstreamAccountBalanceInsufficient &&
		c.UpstreamAccountBalanceInsufficientAt != nil &&
		c.UpstreamAccountBalanceInsufficientRevision != 0 &&
		c.UpstreamAccountBalanceInsufficientRevision == c.UpstreamAccountIdentityRevision
}

// ChannelCostState is the single monetary conversion state for a channel.
// FundingFactor always means actual currency per one raw upstream credit, so
// consumers never need to know the legacy multiply/divide convention.
type ChannelCostState struct {
	ChannelID                       uint       `gorm:"primaryKey" json:"channel_id"`
	RawBalance                      float64    `gorm:"not null;default:0" json:"raw_balance"`
	ActualInventory                 float64    `gorm:"not null;default:0" json:"actual_inventory"`
	FundingFactor                   float64    `gorm:"not null;default:1" json:"funding_factor"`
	RawTodayCost                    float64    `gorm:"not null;default:0" json:"raw_today_cost"`
	ActualTodayCost                 float64    `gorm:"not null;default:0" json:"actual_today_cost"`
	RawTotalCost                    float64    `gorm:"not null;default:0" json:"raw_total_cost"`
	ActualTotalCost                 float64    `gorm:"not null;default:0" json:"actual_total_cost"`
	CostSampledAt                   *time.Time `json:"cost_sampled_at,omitempty"`
	BalanceSampledAt                *time.Time `json:"balance_sampled_at,omitempty"`
	ObservedProviderFactor          *float64   `json:"observed_provider_factor,omitempty"`
	ObservedProviderFactorSource    string     `gorm:"size:32;not null;default:''" json:"observed_provider_factor_source,omitempty"`
	ObservedProviderFactorSampledAt *time.Time `json:"observed_provider_factor_sampled_at,omitempty"`
	CostUnitEpoch                   uint64     `gorm:"not null;default:1" json:"cost_unit_epoch"`
	Version                         uint64     `gorm:"not null;default:1" json:"version"`
	Source                          string     `gorm:"size:32;not null;default:'default'" json:"source"`
	Status                          string     `gorm:"size:32;not null;default:'ready';index" json:"status"`
	LastError                       string     `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt                       time.Time  `json:"created_at"`
	UpdatedAt                       time.Time  `json:"updated_at"`
}

func (ChannelCostState) TableName() string { return "channel_cost_states" }

// ChannelCostUnitRebase is append-only evidence for one administrator-approved
// change to an upstream monetary unit. It intentionally survives channel
// deletion so the financial boundary remains auditable.
type ChannelCostUnitRebase struct {
	ID                                   uint64                              `gorm:"primaryKey;index:idx_channel_cost_unit_rebases_channel_applied,priority:3,sort:desc" json:"id"`
	ChannelID                            uint                                `gorm:"not null;uniqueIndex:uq_channel_cost_unit_rebases_idempotency,priority:1;index:idx_channel_cost_unit_rebases_channel_applied,priority:1" json:"channel_id"`
	ChannelIdentityRevision              uint64                              `gorm:"not null;uniqueIndex:uq_channel_cost_unit_rebases_idempotency,priority:2" json:"-"`
	ChannelNameSnapshot                  string                              `gorm:"size:128;not null" json:"channel_name_snapshot"`
	ChannelTypeSnapshot                  ChannelType                         `gorm:"size:32;not null" json:"channel_type_snapshot"`
	AccountIdentityFingerprint           string                              `gorm:"size:64;not null" json:"-"`
	AccountIdentityFingerprintVersion    uint64                              `gorm:"not null" json:"-"`
	AccountIdentityFingerprintSource     string                              `gorm:"size:128;not null" json:"account_identity_fingerprint_source"`
	AccountIdentityFingerprintObservedAt time.Time                           `gorm:"not null" json:"account_identity_fingerprint_observed_at"`
	IdempotencyKey                       string                              `gorm:"size:128;not null;uniqueIndex:uq_channel_cost_unit_rebases_idempotency,priority:3" json:"idempotency_key"`
	RequestFingerprint                   string                              `gorm:"size:64;not null" json:"-"`
	Actor                                string                              `gorm:"size:128;not null" json:"actor"`
	ExpectedStateVersion                 uint64                              `gorm:"not null" json:"expected_state_version"`
	AppliedStateVersion                  uint64                              `gorm:"not null" json:"applied_state_version"`
	ExpectedCostUnitEpoch                uint64                              `gorm:"not null" json:"expected_cost_unit_epoch"`
	AppliedCostUnitEpoch                 uint64                              `gorm:"not null" json:"applied_cost_unit_epoch"`
	PreviousMultiplier                   float64                             `gorm:"not null" json:"previous_multiplier"`
	CurrentMultiplier                    float64                             `gorm:"not null" json:"current_multiplier"`
	PreviousMultiplierMode               string                              `gorm:"size:16;not null" json:"previous_multiplier_mode"`
	CurrentMultiplierMode                string                              `gorm:"size:16;not null" json:"current_multiplier_mode"`
	PreviousProviderFactor               float64                             `gorm:"not null" json:"previous_provider_factor"`
	CurrentProviderFactor                float64                             `gorm:"not null" json:"current_provider_factor"`
	RateTransitionAt                     time.Time                           `gorm:"not null" json:"rate_transition_at"`
	RateRawScale                         float64                             `gorm:"not null" json:"rate_raw_scale"`
	RateEvidenceGroupCount               int                                 `gorm:"not null" json:"rate_evidence_group_count"`
	RateEvidenceDigest                   string                              `gorm:"size:64;not null" json:"rate_evidence_digest"`
	CounterTransition                    string                              `gorm:"size:32;not null" json:"counter_transition"`
	RateTransitionItems                  []CostUnitRateTransitionItem        `gorm:"-" json:"-"`
	RequestRestatement                   *CostUnitRequestRestatementEvidence `gorm:"-" json:"-"`
	FundingFactorBefore                  float64                             `gorm:"not null" json:"funding_factor_before"`
	FundingFactorAfter                   float64                             `gorm:"not null" json:"funding_factor_after"`
	RawBalanceBefore                     float64                             `gorm:"not null" json:"raw_balance_before"`
	ActualInventoryBefore                float64                             `gorm:"not null" json:"actual_inventory_before"`
	RawBalanceAfter                      float64                             `gorm:"not null" json:"raw_balance_after"`
	ActualInventoryAfter                 float64                             `gorm:"not null" json:"actual_inventory_after"`
	RawTodayCostBefore                   float64                             `gorm:"not null" json:"raw_today_cost_before"`
	ActualTodayCostBefore                float64                             `gorm:"not null" json:"actual_today_cost_before"`
	RawTodayCostAfter                    float64                             `gorm:"not null" json:"raw_today_cost_after"`
	ActualTodayCostAfter                 float64                             `gorm:"not null" json:"actual_today_cost_after"`
	RawTotalCostBefore                   float64                             `gorm:"not null" json:"raw_total_cost_before"`
	ActualTotalCostBefore                float64                             `gorm:"not null" json:"actual_total_cost_before"`
	RawTotalCostAfter                    float64                             `gorm:"not null" json:"raw_total_cost_after"`
	ActualTotalCostAfter                 float64                             `gorm:"not null" json:"actual_total_cost_after"`
	BalanceSampledAtBefore               *time.Time                          `json:"balance_sampled_at_before,omitempty"`
	BalanceSampledAtAfter                time.Time                           `gorm:"not null" json:"balance_sampled_at_after"`
	CostSampledAtBefore                  *time.Time                          `json:"cost_sampled_at_before,omitempty"`
	CostSampledAtAfter                   time.Time                           `gorm:"not null" json:"cost_sampled_at_after"`
	FactorSource                         string                              `gorm:"size:32;not null" json:"factor_source"`
	StateSourceBefore                    string                              `gorm:"size:32;not null" json:"state_source_before"`
	StateSourceAfter                     string                              `gorm:"size:32;not null" json:"state_source_after"`
	Note                                 string                              `gorm:"size:1024;not null;default:''" json:"note,omitempty"`
	AppliedAt                            time.Time                           `gorm:"not null;index:idx_channel_cost_unit_rebases_channel_applied,priority:2,sort:desc" json:"applied_at"`
	CreatedAt                            time.Time                           `gorm:"not null" json:"created_at"`
}

func (ChannelCostUnitRebase) TableName() string { return "channel_cost_unit_rebases" }

type ChannelCostUnitRebaseRateItem struct {
	ID                    uint64    `gorm:"primaryKey" json:"id"`
	RebaseID              uint64    `gorm:"not null" json:"rebase_id"`
	Ordinal               int       `gorm:"not null" json:"ordinal"`
	ModelName             string    `gorm:"size:256;not null" json:"model_name"`
	DescriptionBefore     string    `gorm:"size:512;not null" json:"description_before"`
	DescriptionAfter      string    `gorm:"size:512;not null" json:"description_after"`
	RatioBefore           float64   `gorm:"not null" json:"ratio_before"`
	RatioAfter            float64   `gorm:"not null" json:"ratio_after"`
	CompletionRatioBefore float64   `gorm:"not null" json:"completion_ratio_before"`
	CompletionRatioAfter  float64   `gorm:"not null" json:"completion_ratio_after"`
	CreatedAt             time.Time `gorm:"not null" json:"created_at"`
}

func (ChannelCostUnitRebaseRateItem) TableName() string {
	return "channel_cost_unit_rebase_rate_items"
}

// ChannelCostBasisOverride is append-only evidence for an administrator's
// explicit replacement of the current raw-credit funding factor. Unlike a
// cost-unit rebase, it records no provider assertion and never rescales raw
// rates or restates historical request costs.
type ChannelCostBasisOverride struct {
	ID                        uint64      `gorm:"primaryKey" json:"id"`
	ChannelID                 uint        `gorm:"not null" json:"channel_id"`
	ChannelIdentityRevision   uint64      `gorm:"not null" json:"channel_identity_revision"`
	ChannelNameSnapshot       string      `gorm:"size:128;not null" json:"channel_name_snapshot"`
	ChannelTypeSnapshot       ChannelType `gorm:"size:32;not null" json:"channel_type_snapshot"`
	IdempotencyKey            string      `gorm:"size:128;not null" json:"idempotency_key"`
	RequestFingerprint        string      `gorm:"size:64;not null" json:"-"`
	Actor                     string      `gorm:"size:128;not null" json:"actor"`
	Note                      string      `gorm:"size:1024;not null" json:"note"`
	ExpectedStateVersion      uint64      `gorm:"not null" json:"expected_state_version"`
	AppliedStateVersion       uint64      `gorm:"not null" json:"applied_state_version"`
	ExpectedCostUnitEpoch     uint64      `gorm:"not null" json:"expected_cost_unit_epoch"`
	AppliedCostUnitEpoch      uint64      `gorm:"not null" json:"applied_cost_unit_epoch"`
	FundingFactorBefore       float64     `gorm:"not null" json:"funding_factor_before"`
	FundingFactorAfter        float64     `gorm:"not null" json:"funding_factor_after"`
	RawBalance                float64     `gorm:"not null" json:"raw_balance"`
	ActualInventoryBefore     float64     `gorm:"not null" json:"actual_inventory_before"`
	ActualInventoryAfter      float64     `gorm:"not null" json:"actual_inventory_after"`
	RawTodayCost              float64     `gorm:"not null" json:"raw_today_cost"`
	ActualTodayCostBefore     float64     `gorm:"not null" json:"actual_today_cost_before"`
	ActualTodayCostAfter      float64     `gorm:"not null" json:"actual_today_cost_after"`
	RawTotalCost              float64     `gorm:"not null" json:"raw_total_cost"`
	ActualTotalCostBefore     float64     `gorm:"not null" json:"actual_total_cost_before"`
	ActualTotalCostAfter      float64     `gorm:"not null" json:"actual_total_cost_after"`
	StateSourceBefore         string      `gorm:"size:32;not null" json:"state_source_before"`
	StateSourceAfter          string      `gorm:"size:32;not null" json:"state_source_after"`
	StateStatusBefore         string      `gorm:"size:32;not null" json:"state_status_before"`
	StateStatusAfter          string      `gorm:"size:32;not null" json:"state_status_after"`
	StateLastErrorBefore      string      `gorm:"type:text;not null" json:"state_last_error_before"`
	SupersededCandidateCount  int         `gorm:"not null" json:"superseded_candidate_count"`
	SupersededCandidateDigest string      `gorm:"size:64;not null" json:"superseded_candidate_digest"`
	RateSnapshotCount         int         `gorm:"not null" json:"rate_snapshot_count"`
	RateSnapshotDigest        string      `gorm:"size:64;not null" json:"rate_snapshot_digest"`
	AppliedAt                 time.Time   `gorm:"not null" json:"applied_at"`
	CreatedAt                 time.Time   `gorm:"not null" json:"created_at"`
}

func (ChannelCostBasisOverride) TableName() string { return "channel_cost_basis_overrides" }

// ChannelFundingEvent is an append-only audit record. Repository methods in
// this package deliberately expose creation and reads, but no update/delete.
type ChannelFundingEvent struct {
	ID                    uint64    `gorm:"primaryKey" json:"id"`
	ChannelID             uint      `gorm:"not null;index:idx_channel_funding_events_channel_effective,priority:1" json:"channel_id"`
	Kind                  string    `gorm:"size:32;not null;index" json:"kind"`
	PaidAmount            float64   `gorm:"not null" json:"paid_amount"`
	CreditedAmount        float64   `gorm:"not null" json:"credited_amount"`
	RawBalanceBefore      float64   `gorm:"not null" json:"raw_balance_before"`
	RawBalanceAfter       float64   `gorm:"not null" json:"raw_balance_after"`
	ActualInventoryBefore float64   `gorm:"not null" json:"actual_inventory_before"`
	ActualInventoryAfter  float64   `gorm:"not null" json:"actual_inventory_after"`
	FundingFactorBefore   float64   `gorm:"not null" json:"funding_factor_before"`
	FundingFactorAfter    float64   `gorm:"not null" json:"funding_factor_after"`
	RawTodayCostBefore    float64   `gorm:"not null;default:0" json:"raw_today_cost_before"`
	RawTotalCostBefore    float64   `gorm:"not null;default:0" json:"raw_total_cost_before"`
	Note                  string    `gorm:"size:1024" json:"note,omitempty"`
	EffectiveAt           time.Time `gorm:"not null;index:idx_channel_funding_events_channel_effective,priority:2" json:"effective_at"`
	CreatedAt             time.Time `gorm:"not null" json:"created_at"`
}

func (ChannelFundingEvent) TableName() string { return "channel_funding_events" }

// GatewayRequestFundingCorrection is the durable interval projection of an
// immutable funding event. It lets a dispatch audit or request log created
// after the event transaction recover the same historical basis.
type GatewayRequestFundingCorrection struct {
	ID               uint64     `gorm:"primaryKey" json:"-"`
	ChannelID        uint       `gorm:"not null;index:idx_gateway_request_funding_corrections_lookup,priority:1" json:"-"`
	FundingEventID   uint64     `gorm:"not null;uniqueIndex" json:"-"`
	IntervalStart    time.Time  `gorm:"not null;index:idx_gateway_request_funding_corrections_lookup,priority:2" json:"-"`
	IntervalEnd      *time.Time `gorm:"index:idx_gateway_request_funding_corrections_lookup,priority:3" json:"-"`
	FundingFactor    float64    `gorm:"not null" json:"-"`
	CostBasisVersion uint64     `gorm:"not null" json:"-"`
	CostBasisSource  string     `gorm:"size:32;not null" json:"-"`
	CreatedAt        time.Time  `gorm:"not null" json:"-"`
}

func (GatewayRequestFundingCorrection) TableName() string {
	return "gateway_request_funding_corrections"
}

// ChannelFundingCandidate freezes the evidence for a detected balance increase
// until the operator or an authoritative upstream order supplies its cash cost.
type ChannelFundingCandidate struct {
	ID                  uint64     `gorm:"primaryKey" json:"id"`
	ChannelID           uint       `gorm:"not null;uniqueIndex:idx_funding_candidate_external,priority:1;index" json:"channel_id"`
	Source              string     `gorm:"size:32;not null;uniqueIndex:idx_funding_candidate_external,priority:2" json:"source"`
	ExternalID          string     `gorm:"size:191;not null;uniqueIndex:idx_funding_candidate_external,priority:3" json:"external_id"`
	Status              string     `gorm:"size:24;not null;default:'pending';index" json:"status"`
	PaidAmount          *float64   `json:"paid_amount,omitempty"`
	CreditedAmount      float64    `gorm:"not null" json:"credited_amount"`
	RawBalanceBefore    float64    `gorm:"not null" json:"raw_balance_before"`
	RawBalanceDetected  float64    `gorm:"not null" json:"raw_balance_detected"`
	FundingFactorBefore float64    `gorm:"not null;default:1" json:"funding_factor_before"`
	RawTodayCostBefore  float64    `gorm:"not null;default:0" json:"raw_today_cost_before"`
	RawTotalCostBefore  float64    `gorm:"not null;default:0" json:"raw_total_cost_before"`
	OccurredAt          *time.Time `json:"occurred_at,omitempty"`
	DetectedAt          time.Time  `gorm:"not null;index" json:"detected_at"`
	ResolvedEventID     *uint64    `json:"resolved_event_id,omitempty"`
	Note                string     `gorm:"size:1024" json:"note,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

func (ChannelFundingCandidate) TableName() string { return "channel_funding_candidates" }

// AuthSession 渠道登录后保存的凭据，按 ChannelID 一对一关联。
// *Cipher 字段都用 AES-GCM 加密；UserID 是上游账号 ID 字符串（非敏感），明文存放。
type AuthSession struct {
	ChannelID          uint       `gorm:"primaryKey" json:"channel_id"`
	UserID             string     `gorm:"size:64" json:"user_id,omitempty"`
	AccessTokenCipher  string     `gorm:"type:text" json:"-"`
	RefreshTokenCipher string     `gorm:"type:text" json:"-"`
	CookieCipher       string     `gorm:"type:text" json:"-"`
	CSRFTokenCipher    string     `gorm:"size:1024" json:"-"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
	LastLoginAt        *time.Time `json:"last_login_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (AuthSession) TableName() string { return "auth_sessions" }

// CaptchaProviderType 打码平台类型。
type CaptchaProviderType string

const (
	CaptchaCapSolver   CaptchaProviderType = "capsolver"
	CaptchaTwoCaptcha  CaptchaProviderType = "2captcha"
	CaptchaAntiCaptcha CaptchaProviderType = "anticaptcha"
	CaptchaYesCaptcha  CaptchaProviderType = "yescaptcha"
)

// CaptchaConfig 打码平台配置。APIKeyCipher 加密保存，Extra 存放各平台差异化 JSON。
type CaptchaConfig struct {
	ID           uint                `gorm:"primaryKey" json:"id"`
	Name         string              `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type         CaptchaProviderType `gorm:"size:32;not null;index" json:"type"`
	APIKeyCipher string              `gorm:"size:1024" json:"-"`
	Endpoint     string              `gorm:"size:512" json:"endpoint,omitempty"`
	Extra        string              `gorm:"type:text" json:"extra,omitempty"`
	Enabled      bool                `gorm:"default:true" json:"enabled"`
	ProxyEnabled bool                `gorm:"default:false" json:"proxy_enabled"`
	LastBalance  *float64            `json:"last_balance,omitempty"`
	BalanceUnit  string              `gorm:"size:32" json:"balance_unit,omitempty"`
	BalanceAt    *time.Time          `json:"balance_at,omitempty"`
	BalanceError string              `gorm:"type:text" json:"balance_error,omitempty"`
	CreatedAt    time.Time           `json:"created_at"`
	UpdatedAt    time.Time           `json:"updated_at"`
}

func (CaptchaConfig) TableName() string { return "captcha_configs" }

// RateSnapshot 渠道当前观察到的模型 / 分组倍率快照。upsert per (channel_id, model_name)。
// 实际的"变化历史"在 RateChangeLog；此表只保存当前状态。
type RateSnapshot struct {
	ID              uint    `gorm:"primaryKey" json:"id"`
	ChannelID       uint    `gorm:"not null;uniqueIndex:idx_rate_chan_model" json:"channel_id"`
	ModelName       string  `gorm:"size:256;not null;uniqueIndex:idx_rate_chan_model" json:"model_name"`
	Description     string  `gorm:"size:512" json:"description,omitempty"`
	Ratio           float64 `gorm:"not null" json:"ratio"`
	CompletionRatio float64 `json:"completion_ratio"`
	CostUnitEpoch   uint64  `gorm:"not null;default:1" json:"cost_unit_epoch"`

	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
}

func (RateSnapshot) TableName() string { return "rate_snapshots" }

// RateChangeLog 倍率变化历史。每次扫描发现差异时写入一行。
type RateChangeLog struct {
	ID                          uint     `gorm:"primaryKey" json:"id"`
	ChannelID                   uint     `gorm:"not null;index" json:"channel_id"`
	ModelName                   string   `gorm:"size:256;not null;index" json:"model_name"`
	OldRatio                    *float64 `json:"old_ratio,omitempty"`
	NewRatio                    float64  `gorm:"not null" json:"new_ratio"`
	OldCompletionRatio          *float64 `json:"old_completion_ratio,omitempty"`
	NewCompletionRatio          float64  `json:"new_completion_ratio"`
	FundingFactorBefore         float64  `gorm:"not null;default:1" json:"funding_factor_before"`
	FundingFactorAfter          float64  `gorm:"not null;default:1" json:"funding_factor_after"`
	OldEffectiveRatio           *float64 `json:"old_effective_ratio,omitempty"`
	NewEffectiveRatio           float64  `json:"new_effective_ratio"`
	OldEffectiveCompletionRatio *float64 `json:"old_effective_completion_ratio,omitempty"`
	NewEffectiveCompletionRatio float64  `json:"new_effective_completion_ratio"`
	ChangeReason                string   `gorm:"size:32;not null;default:'upstream_rate'" json:"change_reason"`
	CostUnitEpochBefore         uint64   `gorm:"not null;default:1" json:"cost_unit_epoch_before"`
	CostUnitEpochAfter          uint64   `gorm:"not null;default:1" json:"cost_unit_epoch_after"`
	// CostUnitEpoch is a write-only compatibility input for callers being
	// migrated from the former single-epoch shape.
	CostUnitEpoch uint64    `gorm:"-" json:"-"`
	ChangedAt     time.Time `gorm:"not null;index" json:"changed_at"`
}

func (RateChangeLog) TableName() string { return "rate_change_logs" }

func (r *RateChangeLog) BeforeCreate(_ *gorm.DB) error {
	if r == nil || r.CostUnitEpoch == 0 {
		return nil
	}
	if r.CostUnitEpochAfter == 0 || r.CostUnitEpochAfter == 1 {
		r.CostUnitEpochAfter = r.CostUnitEpoch
	}
	if r.CostUnitEpochBefore == 0 || r.CostUnitEpochBefore == 1 {
		r.CostUnitEpochBefore = r.CostUnitEpoch
		if r.ChangeReason == "cost_unit_rebase" && r.CostUnitEpoch > 1 {
			r.CostUnitEpochBefore--
		}
	}
	return nil
}

// UpstreamAnnouncement 保存从上游渠道同步到的公告。
type UpstreamAnnouncement struct {
	ID              uint       `gorm:"primaryKey" json:"id"`
	ChannelID       uint       `gorm:"not null;uniqueIndex:idx_announcement_chan_source;index" json:"channel_id"`
	SourceKey       string     `gorm:"size:512;not null;uniqueIndex:idx_announcement_chan_source" json:"source_key"`
	Title           string     `gorm:"size:512" json:"title,omitempty"`
	Content         string     `gorm:"type:text;not null" json:"content"`
	Type            string     `gorm:"size:64" json:"type,omitempty"`
	Link            string     `gorm:"size:512" json:"link,omitempty"`
	PublishedAt     *time.Time `json:"published_at,omitempty"`
	SourceUpdatedAt *time.Time `json:"source_updated_at,omitempty"`
	FirstSeenAt     time.Time  `gorm:"not null;index" json:"first_seen_at"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (UpstreamAnnouncement) TableName() string { return "upstream_announcements" }

// BalanceSnapshot 周期性余额采样，用于图表展示。
type BalanceSnapshot struct {
	ID               uint      `gorm:"primaryKey" json:"id"`
	ChannelID        uint      `gorm:"not null;index" json:"channel_id"`
	Balance          float64   `gorm:"not null" json:"balance"`
	RawBalance       *float64  `json:"raw_balance,omitempty"`
	FundingFactor    *float64  `json:"funding_factor,omitempty"`
	CostBasisVersion uint64    `gorm:"not null;default:0" json:"cost_basis_version"`
	CostUnitEpoch    uint64    `gorm:"not null;default:1" json:"cost_unit_epoch"`
	SampledAt        time.Time `gorm:"not null;index" json:"sampled_at"`
}

func (BalanceSnapshot) TableName() string { return "balance_snapshots" }

// CostSnapshot 周期性消费采样，用于图表展示。
type CostSnapshot struct {
	ID               uint      `gorm:"primaryKey" json:"id"`
	ChannelID        uint      `gorm:"not null;index" json:"channel_id"`
	TodayCost        float64   `gorm:"not null" json:"today_cost"`
	RawTodayCost     *float64  `json:"raw_today_cost,omitempty"`
	TotalCost        float64   `gorm:"not null;default:0" json:"total_cost"`
	RawTotalCost     *float64  `json:"raw_total_cost,omitempty"`
	FundingFactor    *float64  `json:"funding_factor,omitempty"`
	CostBasisVersion uint64    `gorm:"not null;default:0" json:"cost_basis_version"`
	CostUnitEpoch    uint64    `gorm:"not null;default:1" json:"cost_unit_epoch"`
	SampledAt        time.Time `gorm:"not null;index" json:"sampled_at"`
}

func (CostSnapshot) TableName() string { return "cost_snapshots" }

// CodexRadarState 保存某个 Radar 数据源的当前状态。历史点位单独保存在
// CodexRadarModelPoint，避免每次上游更新都重复保存完整 JSON。
type CodexRadarState struct {
	ID            uint       `gorm:"primaryKey" json:"-"`
	SourceURL     string     `gorm:"size:512;not null;uniqueIndex" json:"source_url"`
	PayloadJSON   string     `gorm:"type:longtext;not null" json:"-"`
	PayloadHash   string     `gorm:"size:64;not null" json:"-"`
	ModelHash     string     `gorm:"size:64;not null;index" json:"-"`
	FetchedAt     *time.Time `json:"fetched_at,omitempty"`
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`
	LastError     string     `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt     time.Time  `json:"-"`
	UpdatedAt     time.Time  `json:"-"`
}

func (CodexRadarState) TableName() string { return "codex_radar_states" }

// CodexRadarModelPoint 是一次模型配置更新时的规范化快照，用于绘制本地历史曲线。
// 指针字段用来区分上游明确返回 0 和没有返回该指标。
type CodexRadarModelPoint struct {
	ID              uint      `gorm:"primaryKey" json:"-"`
	SourceURL       string    `gorm:"size:512;not null;index:idx_codex_radar_point_source_observed,priority:1;uniqueIndex:idx_codex_radar_point_snapshot_config,priority:1" json:"source_url"`
	SnapshotHash    string    `gorm:"size:64;not null;uniqueIndex:idx_codex_radar_point_snapshot_config,priority:2" json:"-"`
	ConfigKey       string    `gorm:"size:128;not null;index:idx_codex_radar_point_source_config,priority:2;uniqueIndex:idx_codex_radar_point_snapshot_config,priority:3" json:"config_key"`
	Label           string    `gorm:"size:256" json:"label,omitempty"`
	Model           string    `gorm:"size:256" json:"model,omitempty"`
	ReasoningEffort string    `gorm:"size:64" json:"reasoning_effort,omitempty"`
	ObservedAt      time.Time `gorm:"not null;index:idx_codex_radar_point_source_observed,priority:2;index" json:"observed_at"`
	Score           *float64  `json:"score,omitempty"`
	Status          string    `gorm:"size:32" json:"status,omitempty"`
	Passed          *int      `json:"passed,omitempty"`
	Tasks           *int      `json:"tasks,omitempty"`
	ValidTasks      *int      `json:"valid_tasks,omitempty"`
	Invalid         *int      `json:"invalid,omitempty"`
	WallSeconds     *float64  `json:"wall_seconds,omitempty"`
	CostUSD         *float64  `json:"cost_usd,omitempty"`
	TotalTokens     *int64    `json:"total_tokens,omitempty"`
	CreatedAt       time.Time `json:"-"`
}

func (CodexRadarModelPoint) TableName() string { return "codex_radar_model_points" }

// GatewayDownstreamKey 是发给下游调用方的 API key。KeyHash 用于请求鉴权；
// APIKeyCipher 只给管理员页面解密展示，避免数据库裸存明文 key。
type GatewayApp string

const (
	GatewayAppCodex  GatewayApp = "codex"
	GatewayAppClaude GatewayApp = "claude"
	GatewayAppGemini GatewayApp = "gemini"
)

type GatewayDownstreamKey struct {
	ID                          uint                        `gorm:"primaryKey" json:"id"`
	Name                        string                      `gorm:"size:128;not null" json:"name"`
	App                         GatewayApp                  `gorm:"size:32;not null;default:'codex';index" json:"app"`
	ModelFamily                 string                      `gorm:"size:32;not null;default:'codex';index" json:"model_family"`
	CandidateGroupID            uint                        `gorm:"not null;default:0;index" json:"candidate_group_id,omitempty"`
	CandidateGroupName          string                      `gorm:"-" json:"candidate_group_name,omitempty"`
	CandidateTargetMode         string                      `gorm:"size:24;not null;default:'unset'" json:"candidate_target_mode"`
	CandidateTargetSource       string                      `gorm:"size:24;not null;default:'manual'" json:"candidate_target_source"`
	RoutePolicyID               *uint                       `json:"route_policy_id,omitempty"`
	RoutePolicyVersion          *uint64                     `json:"route_policy_version,omitempty"`
	ValidateRoutePolicySnapshot bool                        `gorm:"-" json:"-"`
	KeyGroupID                  uint                        `gorm:"not null;default:0;index" json:"key_group_id,omitempty"`
	KeyGroupName                string                      `gorm:"-" json:"key_group_name,omitempty"`
	ModelMappingStrategy        string                      `gorm:"size:32;not null;default:'off'" json:"model_mapping_strategy"`
	ModelMappingModel           string                      `gorm:"size:256" json:"model_mapping_model,omitempty"`
	ModelMappingReasoningEffort string                      `gorm:"size:64" json:"model_mapping_reasoning_effort,omitempty"`
	ModelMappingPreview         *GatewayModelMappingPreview `gorm:"-" json:"model_mapping_preview,omitempty"`
	CandidatePreviewModel       string                      `gorm:"-" json:"candidate_preview_model,omitempty"`
	CandidatePreviewModelSource string                      `gorm:"-" json:"candidate_preview_model_source,omitempty"`
	RecentRoute                 *GatewayKeyRecentRoute      `gorm:"-" json:"recent_route,omitempty"`
	KeyHash                     string                      `gorm:"size:64;not null;uniqueIndex" json:"-"`
	APIKeyCipher                string                      `gorm:"type:text" json:"-"`
	KeyPrefix                   string                      `gorm:"size:24;not null;index" json:"key_prefix"`
	APIKey                      string                      `gorm:"-" json:"api_key,omitempty"`
	SmartRouting                bool                        `gorm:"not null;default:true" json:"smart_routing"`
	ManualChannelID             uint                        `gorm:"default:0" json:"manual_channel_id,omitempty"`
	ManualChannelName           string                      `gorm:"-" json:"manual_channel_name,omitempty"`
	ManualGroupName             string                      `gorm:"size:128" json:"manual_group_name,omitempty"`
	ManualRouteInvalid          bool                        `gorm:"-" json:"manual_route_invalid,omitempty"`
	RouteExclusions             []GatewayRouteExclusion     `gorm:"-" json:"route_exclusions,omitempty"`
	Usage                       GatewayDownstreamKeyUsage   `gorm:"-" json:"usage"`
	CurrentConcurrency          int64                       `gorm:"-" json:"current_concurrency"`
	Enabled                     bool                        `gorm:"default:true" json:"enabled"`
	LastUsedAt                  *time.Time                  `json:"last_used_at,omitempty"`
	CreatedAt                   time.Time                   `json:"created_at"`
	UpdatedAt                   time.Time                   `json:"updated_at"`
}

func (GatewayDownstreamKey) TableName() string { return "gateway_downstream_keys" }

// GatewayKeyRecentRoute describes a completed real request, not a future route.
// It is an administrator-only projection and never changes affinity or routing.
type GatewayKeyRecentRoute struct {
	DownstreamKeyID uint      `json:"-"`
	RequestID       uint      `json:"request_id"`
	ChannelID       uint      `json:"channel_id"`
	ChannelName     string    `json:"channel_name"`
	GroupName       string    `json:"group_name"`
	ModelName       string    `json:"model_name"`
	CreatedAt       time.Time `json:"created_at"`
}

func (key *GatewayDownstreamKey) BeforeCreate(*gorm.DB) error {
	if strings.TrimSpace(key.CandidateTargetMode) == "" {
		if key.CandidateGroupID > 0 {
			key.CandidateTargetMode = "candidate_group"
		} else {
			key.CandidateTargetMode = "all_candidates"
		}
	}
	if strings.TrimSpace(key.CandidateTargetSource) == "" {
		key.CandidateTargetSource = "manual"
	}
	return nil
}

// GatewayModelMappingPreview is computed from the current radar snapshot for
// administrator display. It is not persisted with the downstream key.
type GatewayModelMappingPreview struct {
	Strategy        string `json:"strategy"`
	ConfigKey       string `json:"config_key,omitempty"`
	Label           string `json:"label,omitempty"`
	Model           string `json:"model,omitempty"`
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
	Source          string `json:"source,omitempty"`
	Error           string `json:"error,omitempty"`
}

type GatewayKeyGroup struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Name      string    `gorm:"size:128;not null;uniqueIndex" json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (GatewayKeyGroup) TableName() string { return "gateway_key_groups" }

type GatewayKeyGroupRoutePolicy struct {
	ID                      uint      `gorm:"primaryKey" json:"id"`
	KeyGroupID              uint      `gorm:"not null;uniqueIndex:idx_gateway_key_group_route_policy,priority:1;index" json:"key_group_id"`
	GatewayApp              string    `gorm:"size:32;not null;uniqueIndex:idx_gateway_key_group_route_policy,priority:2" json:"gateway_app"`
	ModelFamily             string    `gorm:"size:32;not null;uniqueIndex:idx_gateway_key_group_route_policy,priority:3" json:"model_family"`
	DefaultCandidateGroupID *uint     `json:"default_candidate_group_id,omitempty"`
	TargetMode              string    `gorm:"size:24;not null" json:"target_mode"`
	Enabled                 bool      `gorm:"not null;default:true" json:"enabled"`
	PolicyVersion           uint64    `gorm:"not null;default:1" json:"policy_version"`
	CreatedAt               time.Time `json:"created_at"`
	UpdatedAt               time.Time `json:"updated_at"`
}

func (GatewayKeyGroupRoutePolicy) TableName() string { return "gateway_key_group_route_policies" }

type GatewayCandidateGroup struct {
	ID                 uint                          `gorm:"primaryKey" json:"id"`
	Name               string                        `gorm:"size:128;not null;uniqueIndex" json:"name"`
	ModelMappingFamily string                        `gorm:"size:32" json:"model_mapping_family,omitempty"`
	MembershipVersion  uint64                        `gorm:"not null;default:1" json:"membership_version"`
	Members            []GatewayCandidateGroupMember `gorm:"-" json:"members"`
	CreatedAt          time.Time                     `json:"created_at"`
	UpdatedAt          time.Time                     `json:"updated_at"`
}

func (GatewayCandidateGroup) TableName() string { return "gateway_candidate_groups" }

type GatewayCandidateGroupMember struct {
	ID                 uint      `gorm:"primaryKey" json:"id"`
	CandidateGroupID   uint      `gorm:"not null;uniqueIndex:idx_gateway_candidate_group_member,priority:1;uniqueIndex:idx_gateway_candidate_group_upstream,priority:1;index" json:"candidate_group_id"`
	UpstreamGroupID    uint      `gorm:"not null;uniqueIndex:idx_gateway_candidate_group_upstream,priority:2;index" json:"upstream_group_id"`
	PresenceGeneration uint64    `gorm:"-" json:"presence_generation,omitempty"`
	ChannelID          uint      `gorm:"not null;uniqueIndex:idx_gateway_candidate_group_member,priority:2;index" json:"channel_id"`
	GroupName          string    `gorm:"size:128;not null;uniqueIndex:idx_gateway_candidate_group_member,priority:3" json:"group_name"`
	CreatedAt          time.Time `json:"created_at"`
}

func (GatewayCandidateGroupMember) TableName() string { return "gateway_candidate_group_members" }

func (member *GatewayCandidateGroupMember) BeforeCreate(tx *gorm.DB) error {
	if member.UpstreamGroupID != 0 {
		return nil
	}
	if err := lockUpstreamGroupParentChannelTx(tx, member.ChannelID); err != nil {
		return err
	}
	if _, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, member.ChannelID, member.GroupName); err != nil {
		return err
	}
	group, _, err := ensureUpstreamGroupIdentityTx(tx, member.ChannelID, member.GroupName, true, time.Now())
	if err != nil {
		return err
	}
	member.UpstreamGroupID = group.ID
	return nil
}

type GatewayUpstreamGroup struct {
	ID                                uint                      `gorm:"primaryKey" json:"id"`
	ChannelID                         uint                      `gorm:"not null;uniqueIndex:idx_gateway_upstream_group_identity,priority:1;index" json:"channel_id"`
	CanonicalGroupName                string                    `gorm:"size:128;not null;uniqueIndex:idx_gateway_upstream_group_identity,priority:2" json:"canonical_group_name"`
	RawGroupNames                     []string                  `gorm:"type:jsonb;serializer:json" json:"raw_group_names"`
	Present                           bool                      `gorm:"not null;default:true;index" json:"present"`
	PresenceGeneration                uint64                    `gorm:"not null;default:1" json:"presence_generation"`
	DiscoveredAt                      time.Time                 `gorm:"not null" json:"discovered_at"`
	RemovedAt                         *time.Time                `json:"removed_at,omitempty"`
	ModelFamilyCodes                  []string                  `gorm:"type:jsonb;serializer:json" json:"model_family_codes"`
	ModelIDs                          []string                  `gorm:"type:jsonb;serializer:json" json:"model_ids"`
	UnclassifiedModelIDs              []string                  `gorm:"type:jsonb;serializer:json" json:"unclassified_model_ids"`
	AmbiguousModelIDs                 []string                  `gorm:"type:jsonb;serializer:json" json:"ambiguous_model_ids"`
	GatewayApps                       []string                  `gorm:"type:jsonb;serializer:json" json:"gateway_apps"`
	GatewayAppSources                 map[string]string         `gorm:"type:jsonb;serializer:json" json:"gateway_app_sources"`
	ProbedGatewayApps                 []string                  `gorm:"type:jsonb;serializer:json" json:"probed_gateway_apps"`
	ChannelType                       ChannelType               `gorm:"size:32;not null" json:"channel_type"`
	ServiceTier                       string                    `gorm:"size:64;not null" json:"service_tier,omitempty"`
	ServiceTierSource                 string                    `gorm:"size:24;not null;default:'manual'" json:"service_tier_source"`
	CapabilityProfile                 map[string]map[string]any `gorm:"type:jsonb;serializer:json" json:"capability_profile"`
	ClassificationVersion             string                    `gorm:"size:64;not null" json:"classification_version"`
	FactsComplete                     bool                      `gorm:"not null;default:false" json:"facts_complete"`
	IncompleteReasons                 []string                  `gorm:"type:jsonb;serializer:json" json:"incomplete_reasons"`
	FactsVersion                      uint64                    `gorm:"not null;default:1" json:"facts_version"`
	SourceRevision                    string                    `gorm:"size:64;not null" json:"source_revision"`
	ModelsSyncedAt                    *time.Time                `json:"models_synced_at,omitempty"`
	FactsBuiltAt                      *time.Time                `json:"facts_built_at,omitempty"`
	ModelFactsChannelIdentityRevision uint64                    `gorm:"not null;default:0" json:"-"`
	IdentityConflict                  bool                      `gorm:"not null;default:false" json:"identity_conflict"`
	CreatedAt                         time.Time                 `json:"created_at"`
	UpdatedAt                         time.Time                 `json:"updated_at"`
}

func (GatewayUpstreamGroup) TableName() string { return "gateway_upstream_groups" }

type GatewayCandidateGroupAutoRule struct {
	ID                       uint                         `gorm:"primaryKey" json:"id"`
	CandidateGroupID         uint                         `gorm:"not null;index" json:"candidate_group_id"`
	Enabled                  bool                         `gorm:"not null;default:true" json:"enabled"`
	Priority                 int                          `gorm:"not null;default:0" json:"priority"`
	ModelFamilies            []string                     `gorm:"type:jsonb;serializer:json" json:"model_families"`
	FamilyMatchMode          string                       `gorm:"size:24;not null" json:"family_match_mode"`
	GatewayApps              []string                     `gorm:"type:jsonb;serializer:json" json:"gateway_apps"`
	AppMatchMode             string                       `gorm:"size:24;not null" json:"app_match_mode"`
	AcceptedAppSources       []string                     `gorm:"type:jsonb;serializer:json" json:"accepted_app_sources"`
	ChannelTypes             []string                     `gorm:"type:jsonb;serializer:json" json:"channel_types"`
	ServiceTiers             []string                     `gorm:"type:jsonb;serializer:json" json:"service_tiers"`
	ModelIDConditions        []GatewayModelIDCondition    `gorm:"type:jsonb;serializer:json" json:"model_id_conditions"`
	ModelConditionQuantifier string                       `gorm:"size:8;not null" json:"model_condition_quantifier"`
	ModelFactCoverage        string                       `gorm:"size:8;not null" json:"model_fact_coverage"`
	CapabilityConditions     []GatewayCapabilityCondition `gorm:"type:jsonb;serializer:json" json:"capability_conditions"`
	RuleVersion              uint64                       `gorm:"not null;default:1" json:"rule_version"`
	CreatedAt                time.Time                    `json:"created_at"`
	UpdatedAt                time.Time                    `json:"updated_at"`
}

func (GatewayCandidateGroupAutoRule) TableName() string { return "gateway_candidate_group_auto_rules" }

type GatewayModelIDCondition struct {
	Match string `json:"match"`
	Value string `json:"value"`
}

type GatewayCapabilityCondition struct {
	SelectorType     string   `json:"selector_type"`
	SelectorValues   []string `json:"selector_values"`
	Quantifier       string   `json:"quantifier"`
	EvidenceSource   string   `json:"evidence_source"`
	MaxAgeHours      int      `json:"max_age_hours"`
	MinContextWindow int64    `json:"min_context_window_tokens,omitempty"`
	MinSafeInput     int64    `json:"min_safe_input_tokens,omitempty"`
}

type GatewayCandidateGroupRulesetState struct {
	ID                 int16     `gorm:"primaryKey" json:"id"`
	RulesetVersion     uint64    `gorm:"not null;default:1" json:"ruleset_version"`
	BackfillComplete   bool      `gorm:"not null;default:false" json:"backfill_complete"`
	AutoExecuteEnabled bool      `gorm:"not null;default:false" json:"auto_execute_enabled"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

func (GatewayCandidateGroupRulesetState) TableName() string {
	return "gateway_candidate_group_ruleset_state"
}

type GatewayCandidateGroupMembershipClaim struct {
	ID                 uint       `gorm:"primaryKey" json:"id"`
	CandidateGroupID   uint       `gorm:"not null;index" json:"candidate_group_id"`
	UpstreamGroupID    uint       `gorm:"not null;index" json:"upstream_group_id"`
	PresenceGeneration uint64     `gorm:"not null" json:"presence_generation"`
	SourceKind         string     `gorm:"size:16;not null" json:"source_kind"`
	SourceID           uint64     `gorm:"not null" json:"source_id"`
	Active             bool       `gorm:"not null;default:true" json:"active"`
	CreatedAt          time.Time  `json:"created_at"`
	RevokedAt          *time.Time `json:"revoked_at,omitempty"`
}

func (GatewayCandidateGroupMembershipClaim) TableName() string {
	return "gateway_candidate_group_membership_claims"
}

type GatewayUpstreamGroupAssignmentOverride struct {
	ID                 uint                                           `gorm:"primaryKey" json:"id"`
	UpstreamGroupID    uint                                           `gorm:"not null;uniqueIndex" json:"upstream_group_id"`
	PresenceGeneration uint64                                         `gorm:"not null;default:0" json:"presence_generation"`
	Action             string                                         `gorm:"size:16;not null" json:"action"`
	Reason             string                                         `gorm:"type:text;not null" json:"reason"`
	CreatedBy          string                                         `gorm:"size:128;not null" json:"created_by"`
	OverrideVersion    uint64                                         `gorm:"not null;default:1" json:"override_version"`
	Targets            []GatewayUpstreamGroupAssignmentOverrideTarget `gorm:"-" json:"targets"`
	CreatedAt          time.Time                                      `json:"created_at"`
	UpdatedAt          time.Time                                      `json:"updated_at"`
}

func (GatewayUpstreamGroupAssignmentOverride) TableName() string {
	return "gateway_upstream_group_assignment_overrides"
}

type GatewayUpstreamGroupAssignmentOverrideTarget struct {
	ID                       uint      `gorm:"primaryKey" json:"id"`
	OverrideID               uint      `gorm:"not null;index" json:"override_id"`
	CandidateGroupID         *uint     `json:"candidate_group_id,omitempty"`
	CandidateGroupIDSnapshot uint      `gorm:"not null" json:"candidate_group_id_snapshot"`
	CreatedAt                time.Time `json:"created_at"`
}

func (GatewayUpstreamGroupAssignmentOverrideTarget) TableName() string {
	return "gateway_upstream_group_assignment_override_targets"
}

type GatewayUpstreamGroupAssignmentAudit struct {
	ID                      uint64         `gorm:"primaryKey" json:"id"`
	ChangeID                *uint          `json:"change_id,omitempty"`
	UpstreamGroupID         uint           `gorm:"not null;index" json:"upstream_group_id"`
	RawGroupName            string         `gorm:"size:128;not null" json:"raw_group_name"`
	CanonicalGroupName      string         `gorm:"size:128;not null" json:"canonical_group_name"`
	PresenceGeneration      uint64         `gorm:"not null" json:"presence_generation"`
	ClassificationVersion   string         `gorm:"size:64;not null" json:"classification_version"`
	FactsVersion            uint64         `gorm:"not null" json:"facts_version"`
	RulesetVersion          uint64         `gorm:"not null" json:"ruleset_version"`
	OverrideVersion         uint64         `gorm:"not null" json:"override_version"`
	Action                  string         `gorm:"size:64;not null" json:"action"`
	TriggerSource           string         `gorm:"size:32;not null" json:"trigger_source"`
	SourceRef               *uint64        `json:"source_ref,omitempty"`
	MatchedRuleIDs          []uint         `gorm:"type:jsonb;serializer:json" json:"matched_rule_ids"`
	TargetCandidateGroupIDs []uint         `gorm:"type:jsonb;serializer:json" json:"target_candidate_group_ids"`
	FactsSnapshot           map[string]any `gorm:"type:jsonb;serializer:json" json:"facts_snapshot"`
	Explanation             map[string]any `gorm:"type:jsonb;serializer:json" json:"explanation"`
	Actor                   string         `gorm:"size:128;not null" json:"actor"`
	CreatedAt               time.Time      `json:"created_at"`
}

func (GatewayUpstreamGroupAssignmentAudit) TableName() string {
	return "gateway_upstream_group_assignment_audits"
}

type GatewayUpstreamGroupChange struct {
	ID                                uint       `gorm:"primaryKey" json:"id"`
	ChannelID                         uint       `gorm:"not null;index:idx_gateway_upstream_group_change_lookup,priority:1" json:"channel_id"`
	ChannelName                       string     `gorm:"-" json:"channel_name,omitempty"`
	GroupName                         string     `gorm:"size:128;not null;index:idx_gateway_upstream_group_change_lookup,priority:2" json:"group_name"`
	UpstreamGroupID                   *uint      `json:"upstream_group_id,omitempty"`
	PresenceGeneration                *uint64    `json:"presence_generation,omitempty"`
	Kind                              string     `gorm:"size:16;not null" json:"kind"`
	Status                            string     `gorm:"size:16;not null;default:'pending';index" json:"status"`
	UpstreamKeyID                     int64      `json:"upstream_key_id,omitempty"`
	UpstreamKeyName                   string     `gorm:"size:128" json:"upstream_key_name,omitempty"`
	RemovalClaimKey                   *string    `gorm:"size:64;uniqueIndex:idx_gateway_upstream_key_removal_claim" json:"-"`
	RemovalOwnershipClaim             string     `gorm:"size:64" json:"-"`
	RemovalChannelIdentityRevision    *uint64    `json:"-"`
	RemovalClaimEpoch                 *uint64    `json:"-"`
	RemovalClaimDigestVersion         *uint16    `json:"-"`
	RemovalEvidenceDigestVersion      *uint16    `json:"-"`
	RemovalOwnershipDigestVersion     *uint16    `json:"-"`
	RemovalManualReviewReason         string     `gorm:"size:64;not null;default:''" json:"removal_manual_review_reason,omitempty"`
	RemovalManualReviewActor          string     `gorm:"size:128;not null;default:''" json:"removal_manual_review_actor,omitempty"`
	RemovalManualReviewResolution     string     `gorm:"size:64;not null;default:''" json:"removal_manual_review_resolution,omitempty"`
	RemovalManualReviewResolvedBy     string     `gorm:"size:128;not null;default:''" json:"removal_manual_review_resolved_by,omitempty"`
	RemovalManualReviewedAt           *time.Time `json:"removal_manual_reviewed_at,omitempty"`
	RemovalManualReviewResolvedAt     *time.Time `json:"removal_manual_review_resolved_at,omitempty"`
	AssignmentState                   string     `gorm:"size:24" json:"assignment_state,omitempty"`
	AssignmentReasonCode              string     `gorm:"size:64" json:"assignment_reason_code,omitempty"`
	AssignmentActionable              bool       `gorm:"-" json:"assignment_actionable"`
	AssignmentBlockedReason           string     `gorm:"-" json:"assignment_blocked_reason,omitempty"`
	SuggestedCandidateGroupIDs        []uint     `gorm:"type:jsonb;serializer:json" json:"suggested_candidate_group_ids,omitempty"`
	EvaluatedAt                       *time.Time `json:"evaluated_at,omitempty"`
	EvaluationToken                   *string    `gorm:"size:64" json:"evaluation_token,omitempty"`
	FactsDigest                       string     `gorm:"size:64" json:"facts_digest,omitempty"`
	DetectedAt                        time.Time  `gorm:"not null" json:"detected_at"`
	ResolvedAt                        *time.Time `json:"resolved_at,omitempty"`
	AssignmentAuditAction             string     `gorm:"-" json:"assignment_audit_action,omitempty"`
	AssignmentAuditTrigger            string     `gorm:"-" json:"assignment_audit_trigger,omitempty"`
	AssignmentAuditActor              string     `gorm:"-" json:"assignment_audit_actor,omitempty"`
	AssignmentAuditTargetGroupIDs     []uint     `gorm:"-" json:"assignment_audit_target_candidate_group_ids,omitempty"`
	AssignmentAuditPresenceGeneration uint64     `gorm:"-" json:"assignment_audit_presence_generation,omitempty"`
	AssignmentAuditCreatedAt          *time.Time `gorm:"-" json:"assignment_audit_created_at,omitempty"`
	CreatedAt                         time.Time  `json:"created_at"`
	UpdatedAt                         time.Time  `json:"updated_at"`
}

func (GatewayUpstreamGroupChange) TableName() string { return "gateway_upstream_group_changes" }

type GatewayManagedKeyRebuildIntent struct {
	ID                      uint       `gorm:"primaryKey" json:"id"`
	ChannelID               uint       `gorm:"not null;index" json:"channel_id"`
	ChannelIdentityRevision uint64     `gorm:"not null" json:"-"`
	UpstreamGroupID         uint       `gorm:"not null" json:"upstream_group_id"`
	PresenceGeneration      uint64     `gorm:"not null" json:"presence_generation"`
	CanonicalGroupName      string     `gorm:"size:128;not null" json:"group_name"`
	LocalKeyID              uint       `gorm:"not null" json:"local_key_id"`
	UpstreamKeyID           int64      `gorm:"not null" json:"upstream_key_id"`
	UpstreamKeyName         string     `gorm:"size:128;not null" json:"upstream_key_name"`
	ClaimKey                string     `gorm:"size:64;not null;uniqueIndex" json:"-"`
	ClaimEpoch              uint64     `gorm:"not null;default:1" json:"-"`
	ClaimDigestVersion      uint16     `gorm:"not null" json:"-"`
	EvidenceDigest          string     `gorm:"size:64;not null" json:"-"`
	EvidenceDigestVersion   uint16     `gorm:"not null" json:"-"`
	OwnershipClaim          string     `gorm:"size:64;not null" json:"-"`
	OwnershipDigestVersion  uint16     `gorm:"not null" json:"-"`
	Status                  string     `gorm:"size:16;not null;default:'processing';index" json:"status"`
	ManualReviewReason      string     `gorm:"size:64;not null;default:''" json:"manual_review_reason,omitempty"`
	ManualReviewActor       string     `gorm:"size:128;not null;default:''" json:"manual_review_actor,omitempty"`
	ManualReviewResolution  string     `gorm:"size:64;not null;default:''" json:"manual_review_resolution,omitempty"`
	ManualReviewResolvedBy  string     `gorm:"size:128;not null;default:''" json:"manual_review_resolved_by,omitempty"`
	ManualReviewedAt        *time.Time `json:"manual_reviewed_at,omitempty"`
	ManualReviewResolvedAt  *time.Time `json:"manual_review_resolved_at,omitempty"`
	DetectedAt              time.Time  `gorm:"not null" json:"detected_at"`
	ResolvedAt              *time.Time `json:"resolved_at,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

func (GatewayManagedKeyRebuildIntent) TableName() string {
	return "gateway_managed_key_rebuild_intents"
}

// GatewayDownstreamKeyUsage is the request-log usage summary shown beside a
// downstream key. It is computed on demand and is intentionally not stored on
// the key row, so deleting or importing a key cannot leave stale totals.
type GatewayDownstreamKeyUsage struct {
	TotalCost                float64 `json:"total_cost"`
	TodayCost                float64 `json:"today_cost"`
	TotalRequests            int64   `json:"total_requests"`
	TodayRequests            int64   `json:"today_requests"`
	TotalTokens              int64   `json:"total_tokens"`
	TodayTokens              int64   `json:"today_tokens"`
	ActualCost               float64 `json:"actual_cost"`
	EstimatedCost            float64 `json:"estimated_cost"`
	ReportedCost             float64 `json:"reported_cost"`
	ReconciledCost           float64 `json:"reconciled_cost"`
	UnavailableRequests      int64   `json:"unavailable_requests"`
	TodayUnavailableRequests int64   `json:"today_unavailable_requests"`
}

// GatewayRouteExclusion 保存某个下游 key 禁用的候选渠道 / 分组。
type GatewayRouteExclusion struct {
	ID              uint      `gorm:"primaryKey" json:"id"`
	DownstreamKeyID uint      `gorm:"not null;uniqueIndex:idx_gateway_route_exclusion,priority:1;index" json:"downstream_key_id"`
	ChannelID       uint      `gorm:"not null;uniqueIndex:idx_gateway_route_exclusion,priority:2;index" json:"channel_id"`
	GroupName       string    `gorm:"size:128;not null;uniqueIndex:idx_gateway_route_exclusion,priority:3" json:"group_name"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (GatewayRouteExclusion) TableName() string { return "gateway_route_exclusions" }

// GatewayUpstreamKey 保存每个上游渠道用于 OpenAI 兼容转发的 API key。
// 它独立于后台登录凭据，避免把管理面板 session 暴露给网关调用路径。
type GatewayUpstreamKey struct {
	ID                                               uint       `gorm:"primaryKey" json:"id"`
	ChannelID                                        uint       `gorm:"not null;uniqueIndex:idx_gateway_upstream_key_group" json:"channel_id"`
	UpstreamKeyID                                    int64      `gorm:"index" json:"upstream_key_id,omitempty"`
	UpstreamKeyName                                  string     `gorm:"size:128" json:"upstream_key_name,omitempty"`
	KeyGeneration                                    uint64     `gorm:"not null;default:1" json:"-"`
	AuthenticationInvalidatedChannelIdentityRevision uint64     `gorm:"not null;default:0" json:"-"`
	OwnershipProvenance                              string     `gorm:"size:32;not null;default:'unknown'" json:"ownership_provenance,omitempty"`
	Provisioning                                     bool       `gorm:"not null;default:false" json:"provisioning,omitempty"`
	ProvisioningStartedAt                            *time.Time `json:"provisioning_started_at,omitempty"`
	APIKeyCipher                                     string     `gorm:"type:text;not null" json:"-"`
	APIKeyPrefix                                     string     `gorm:"size:24;not null" json:"api_key_prefix"`
	GroupName                                        string     `gorm:"size:128;uniqueIndex:idx_gateway_upstream_key_group" json:"group_name,omitempty"`
	GroupRatio                                       float64    `json:"group_ratio,omitempty"`
	DirectCredentialID                               *uint      `gorm:"uniqueIndex" json:"direct_credential_id,omitempty"`
	DirectCredentialRevision                         uint64     `gorm:"not null;default:0" json:"-"`
	DirectBaseURL                                    string     `gorm:"size:512;not null;default:''" json:"-"`
	ModelFamilies                                    string     `gorm:"size:256" json:"model_families,omitempty"`
	ModelIDs                                         string     `gorm:"type:text" json:"model_ids,omitempty"`
	LatencyProbeModel                                string     `gorm:"size:256" json:"latency_probe_model,omitempty"`
	LatencyProbeEnabled                              bool       `gorm:"not null;default:true" json:"probe_enabled"`
	ModelsSyncedAt                                   *time.Time `json:"models_synced_at,omitempty"`
	ModelsError                                      string     `gorm:"type:text" json:"models_error,omitempty"`
	ModelFactsUpstreamGroupID                        uint       `gorm:"not null;default:0" json:"-"`
	ModelFactsPresenceGeneration                     uint64     `gorm:"not null;default:0" json:"-"`
	ModelFactsChannelIdentityRevision                uint64     `gorm:"not null;default:0" json:"-"`
	Enabled                                          bool       `gorm:"default:true" json:"enabled"`
	LastUsedAt                                       *time.Time `json:"last_used_at,omitempty"`
	LastError                                        string     `gorm:"type:text" json:"last_error,omitempty"`
	LastRoutingFailureError                          string     `gorm:"type:text" json:"last_routing_failure_error,omitempty"`
	LastRoutingFailureKind                           string     `gorm:"size:32" json:"last_routing_failure_kind,omitempty"`
	LastRoutingFailureStatus                         int        `json:"last_routing_failure_status,omitempty"`
	LastRoutingFailureAt                             *time.Time `json:"last_routing_failure_at,omitempty"`
	RoutingFailureCount                              int        `gorm:"not null;default:0" json:"failure_count,omitempty"`
	RoutingCooldownUntil                             *time.Time `json:"cooldown_until,omitempty"`
	RoutingIsolatedAt                                *time.Time `json:"routing_isolated_at,omitempty"`
	// RoutingHealth* binds the mutable health summary to the exact account and
	// present group generation that produced it. A zero tuple is legacy or
	// intentionally empty health and must not affect current routing.
	RoutingHealthUpstreamGroupID         uint   `gorm:"not null;default:0" json:"-"`
	RoutingHealthPresenceGeneration      uint64 `gorm:"not null;default:0" json:"-"`
	RoutingHealthChannelIdentityRevision uint64 `gorm:"not null;default:0" json:"-"`
	// ChannelDeletion* is an exact, nullable freeze marker. The coordinator
	// must match every component before a remote DELETE or local finalization.
	// APIKeyCipher and APIKeyPrefix deliberately remain available for ownership
	// verification until the final channel cleanup transaction commits.
	ChannelDeletionIntentID           *uint64   `json:"-"`
	ChannelDeletionEpoch              *uint64   `json:"-"`
	ChannelDeletionIdentityRevision   *uint64   `json:"-"`
	ChannelDeletionUpstreamGroupID    *uint     `json:"-"`
	ChannelDeletionPresenceGeneration *uint64   `json:"-"`
	CreatedAt                         time.Time `json:"created_at"`
	UpdatedAt                         time.Time `json:"updated_at"`
}

func (GatewayUpstreamKey) TableName() string { return "gateway_upstream_keys" }

// GatewayRoutingFailure records exact event ordering within the bounded
// adaptive failure window. Isolation itself is latched on GatewayUpstreamKey.
type GatewayRoutingFailure struct {
	ID                      uint64    `gorm:"primaryKey" json:"id"`
	ChannelID               uint      `gorm:"not null;index:idx_gateway_routing_failures_route_time,priority:1" json:"channel_id"`
	GroupName               string    `gorm:"size:128;not null;index:idx_gateway_routing_failures_route_time,priority:2" json:"group_name"`
	UpstreamGroupID         uint      `gorm:"not null;default:0" json:"-"`
	PresenceGeneration      uint64    `gorm:"not null;default:0" json:"-"`
	ChannelIdentityRevision uint64    `gorm:"not null;default:0" json:"-"`
	OccurredAt              time.Time `gorm:"not null;index:idx_gateway_routing_failures_route_time,priority:3" json:"occurred_at"`
}

func (GatewayRoutingFailure) TableName() string { return "gateway_routing_failures" }

// GatewayManualRoutingCooldownAudit records an administrator's request-log
// driven cooldown without changing the durable channel or group configuration.
type GatewayManualRoutingCooldownAudit struct {
	ID                      uint64     `gorm:"primaryKey" json:"id"`
	RequestLogID            uint       `gorm:"not null;index;uniqueIndex:idx_gateway_manual_cooldown_idempotency,priority:1" json:"request_log_id"`
	RequestFirstByteMs      int64      `gorm:"not null" json:"request_first_byte_ms"`
	RequestCreatedAt        time.Time  `gorm:"not null" json:"request_created_at"`
	ChannelID               uint       `gorm:"not null;index" json:"channel_id"`
	ChannelNameSnapshot     string     `gorm:"size:256;not null" json:"channel_name"`
	GroupName               string     `gorm:"size:128;not null" json:"group_name"`
	UpstreamGroupID         uint       `gorm:"not null;default:0" json:"-"`
	PresenceGeneration      uint64     `gorm:"not null;default:0" json:"-"`
	ChannelIdentityRevision uint64     `gorm:"not null;default:0" json:"-"`
	Actor                   string     `gorm:"size:128;not null" json:"actor"`
	IdempotencyKey          string     `gorm:"size:128;not null;uniqueIndex:idx_gateway_manual_cooldown_idempotency,priority:2" json:"idempotency_key"`
	DurationSeconds         int64      `gorm:"not null" json:"duration_seconds"`
	PreviousCooldownUntil   *time.Time `json:"previous_cooldown_until,omitempty"`
	CooldownUntil           time.Time  `gorm:"not null" json:"cooldown_until"`
	CreatedAt               time.Time  `gorm:"not null;index" json:"created_at"`
}

func (GatewayManualRoutingCooldownAudit) TableName() string {
	return "gateway_manual_routing_cooldown_audits"
}

type GatewayCostStatus string

const (
	GatewayCostStatusNone        GatewayCostStatus = "none"
	GatewayCostStatusEstimated   GatewayCostStatus = "estimated"
	GatewayCostStatusUnavailable GatewayCostStatus = "unavailable"
	GatewayCostStatusReported    GatewayCostStatus = "reported"
	GatewayCostStatusReconciled  GatewayCostStatus = "reconciled"
)

type GatewayRequestCostReconcileState string

const (
	GatewayRequestCostReconcileIdle           GatewayRequestCostReconcileState = "idle"
	GatewayRequestCostReconcilePending        GatewayRequestCostReconcileState = "pending"
	GatewayRequestCostReconcileClaimed        GatewayRequestCostReconcileState = "claimed"
	GatewayRequestCostReconcileRetryScheduled GatewayRequestCostReconcileState = "retry_scheduled"
	GatewayRequestCostReconcileSucceeded      GatewayRequestCostReconcileState = "succeeded"
	GatewayRequestCostReconcileTerminal       GatewayRequestCostReconcileState = "terminal"
)

type GatewayRequestCostReconcileOutcome string

const (
	GatewayRequestCostReconcileOutcomeNone               GatewayRequestCostReconcileOutcome = "none"
	GatewayRequestCostReconcileOutcomeSucceeded          GatewayRequestCostReconcileOutcome = "succeeded"
	GatewayRequestCostReconcileOutcomeRetry              GatewayRequestCostReconcileOutcome = "retry"
	GatewayRequestCostReconcileOutcomeConflictRetry      GatewayRequestCostReconcileOutcome = "conflict_retry"
	GatewayRequestCostReconcileOutcomeTerminalFailure    GatewayRequestCostReconcileOutcome = "terminal_failure"
	GatewayRequestCostReconcileOutcomeTerminalConflict   GatewayRequestCostReconcileOutcome = "terminal_conflict"
	GatewayRequestCostReconcileOutcomePricingUnavailable GatewayRequestCostReconcileOutcome = "pricing_unavailable"
	GatewayRequestCostReconcileOutcomeIneligible         GatewayRequestCostReconcileOutcome = "ineligible"
)

type GatewayRequestRecordKind string

const (
	// GatewayProviderRequestIDMaxLength is a byte limit. Provider request IDs
	// are opaque exact-match evidence and must not be rune-counted or truncated.
	GatewayProviderRequestIDMaxLength                                     = providerid.MaxBytes
	GatewayRequestRecordKindRequest              GatewayRequestRecordKind = "request"
	GatewayRequestRecordKindDispatchCostEvidence GatewayRequestRecordKind = "dispatch_cost_evidence"
	// GatewayRequestTelemetryVersion distinguishes records written after the
	// structured incident telemetry rollout from historical rows. Version zero
	// is deliberately reserved for rows whose facts were never collected.
	GatewayRequestTelemetryVersion = 1
	GatewayRoutingModeSmart        = "smart"
	GatewayRoutingModeFixed        = "fixed"
	GatewayWaitingProbeDetail      = "waiting health inference"
)

func IsGatewayWaitingProbeDetails(details string) bool {
	return details == GatewayWaitingProbeDetail || strings.HasPrefix(details, GatewayWaitingProbeDetail+"\n")
}

// GatewayRequestLog 记录下游请求最终命中的渠道和结果。
type GatewayRequestLog struct {
	ID                       uint                     `gorm:"primaryKey;index:idx_gateway_logs_aggregate_backfill,priority:2;index:idx_gateway_logs_group_cache_backfill,priority:2" json:"id"`
	RecordKind               GatewayRequestRecordKind `gorm:"size:32;not null;default:'request'" json:"-"`
	DownstreamKeyID          uint                     `gorm:"not null;index;index:idx_gateway_logs_cache_route,priority:1;index:idx_gateway_logs_session_route,priority:1;index:idx_gateway_logs_key_time,priority:1" json:"downstream_key_id"`
	ChannelID                uint                     `gorm:"index;uniqueIndex:idx_gateway_channel_usage,priority:1;index:idx_gateway_logs_channel_time,priority:1" json:"channel_id"`
	ChannelNameSnapshot      string                   `gorm:"size:256" json:"channel_name_snapshot,omitempty"`
	GroupName                string                   `gorm:"size:128;index" json:"group_name,omitempty"`
	GroupRatio               float64                  `json:"group_ratio,omitempty"`
	CompletionRatio          float64                  `json:"completion_ratio,omitempty"`
	RawGroupRatio            *float64                 `json:"raw_group_ratio,omitempty"`
	RawCompletionRatio       *float64                 `json:"raw_completion_ratio,omitempty"`
	FundingFactor            *float64                 `json:"funding_factor,omitempty"`
	CostBasisVersion         uint64                   `gorm:"not null;default:0" json:"cost_basis_version"`
	CostBasisSource          string                   `gorm:"size:32" json:"cost_basis_source,omitempty"`
	FundingBasisAt           *time.Time               `json:"-"`
	DeferredFundingFactor    *float64                 `json:"-"`
	DeferredCostBasisVersion *uint64                  `json:"-"`
	DeferredCostBasisSource  *string                  `gorm:"size:32" json:"-"`
	ModelName                string                   `gorm:"size:256;index;index:idx_gateway_logs_cache_route,priority:2;index:idx_gateway_logs_session_route,priority:3;index:idx_gateway_logs_model_time,priority:1" json:"model_name"`
	UpstreamModelName        string                   `gorm:"size:256" json:"upstream_model_name,omitempty"`
	UpstreamResponseModel    string                   `gorm:"size:256;not null;default:''" json:"upstream_response_model,omitempty"`
	ReasoningEffort          string                   `gorm:"size:64" json:"reasoning_effort,omitempty"`
	RequestedModelName       string                   `gorm:"size:256" json:"requested_model_name,omitempty"`
	RequestedReasoningEffort string                   `gorm:"size:64" json:"requested_reasoning_effort,omitempty"`
	ModelMappingStrategy     string                   `gorm:"size:32" json:"model_mapping_strategy,omitempty"`
	ModelMappingDetails      string                   `gorm:"type:text" json:"model_mapping_details,omitempty"`
	Method                   string                   `gorm:"size:16;not null" json:"method"`
	Path                     string                   `gorm:"size:512;not null" json:"path"`
	StatusCode               int                      `gorm:"not null;index:idx_gateway_logs_cache_route,priority:3;index:idx_gateway_logs_session_route,priority:4" json:"status_code"`
	FirstByteMs              int64                    `gorm:"not null;default:0" json:"first_byte_ms"`
	// DownstreamFirstVisibleMs is intentionally separate from FirstByteMs:
	// it measures the first body byte actually written to the downstream client
	// from the logical request start, and is nil when no body became visible.
	DownstreamFirstVisibleMs              *int64                             `json:"downstream_first_visible_ms,omitempty"`
	LatencyMs                             int64                              `gorm:"not null" json:"latency_ms"`
	PromptTokens                          int64                              `json:"prompt_tokens,omitempty"`
	CompletionTokens                      int64                              `json:"completion_tokens,omitempty"`
	CacheCreationTokens                   int64                              `json:"cache_creation_tokens,omitempty"`
	CacheReadTokens                       int64                              `json:"cache_read_tokens,omitempty"`
	TotalTokens                           int64                              `json:"total_tokens,omitempty"`
	Cost                                  float64                            `json:"cost,omitempty"`
	RawCost                               *float64                           `json:"raw_cost,omitempty"`
	CostEstimated                         bool                               `json:"cost_estimated,omitempty"`
	CostSource                            string                             `gorm:"size:32" json:"cost_source,omitempty"`
	CostStatus                            GatewayCostStatus                  `gorm:"size:16;not null;default:'none'" json:"cost_status"`
	ChannelIdentityRevision               uint64                             `gorm:"not null;default:0" json:"-"`
	UpstreamGroupID                       uint                               `gorm:"not null;default:0" json:"-"`
	UpstreamGroupGeneration               uint64                             `gorm:"not null;default:0" json:"-"`
	UpstreamKeyLocalID                    uint                               `gorm:"not null;default:0" json:"-"`
	UpstreamKeyGeneration                 uint64                             `gorm:"not null;default:0" json:"-"`
	PriceFactID                           *uint64                            `json:"price_fact_id,omitempty"`
	PriceCostUnitEpoch                    uint64                             `gorm:"not null;default:0" json:"price_cost_unit_epoch,omitempty"`
	PriceFactVersion                      uint64                             `gorm:"not null;default:0" json:"price_fact_version,omitempty"`
	PriceFactSchemaVersion                int                                `gorm:"not null;default:0" json:"price_fact_schema_version,omitempty"`
	PriceFactChecksum                     string                             `gorm:"size:64;not null;default:''" json:"price_fact_checksum,omitempty"`
	PriceCanonicalGroupName               string                             `gorm:"size:128;not null;default:''" json:"-"`
	PriceModelName                        string                             `gorm:"size:256;not null;default:''" json:"price_model_name,omitempty"`
	PriceOperation                        string                             `gorm:"size:32;not null;default:''" json:"price_operation,omitempty"`
	PriceProtocol                         string                             `gorm:"size:32;not null;default:''" json:"price_protocol,omitempty"`
	PriceInputUnit                        *float64                           `json:"price_input_unit,omitempty"`
	PriceOutputUnit                       *float64                           `json:"price_output_unit,omitempty"`
	PriceCacheReadUnit                    *float64                           `json:"price_cache_read_unit,omitempty"`
	PriceCacheWriteUnit                   *float64                           `json:"price_cache_write_unit,omitempty"`
	PriceLongContextThresholdTokens       *int64                             `json:"price_long_context_threshold_tokens,omitempty"`
	PriceLongContextInputUnit             *float64                           `json:"price_long_context_input_unit,omitempty"`
	PriceLongContextOutputUnit            *float64                           `json:"price_long_context_output_unit,omitempty"`
	PriceLongContextCacheReadUnit         *float64                           `json:"price_long_context_cache_read_unit,omitempty"`
	PriceLongContextCacheWriteUnit        *float64                           `json:"price_long_context_cache_write_unit,omitempty"`
	PriceOperationUnit                    *float64                           `json:"price_operation_unit,omitempty"`
	PriceCurrency                         string                             `gorm:"size:3;not null;default:''" json:"price_currency,omitempty"`
	PriceUnit                             string                             `gorm:"size:32;not null;default:''" json:"price_unit,omitempty"`
	PriceSource                           string                             `gorm:"size:64;not null;default:''" json:"price_source,omitempty"`
	PriceObservedAt                       *time.Time                         `json:"price_observed_at,omitempty"`
	PriceValidFrom                        *time.Time                         `json:"price_valid_from,omitempty"`
	PriceValidUntil                       *time.Time                         `json:"price_valid_until,omitempty"`
	UsageSchema                           string                             `gorm:"size:32;not null;default:''" json:"usage_schema,omitempty"`
	UsageSchemaVersion                    int                                `gorm:"not null;default:0" json:"usage_schema_version,omitempty"`
	UsageInputPresent                     bool                               `gorm:"not null;default:false" json:"usage_input_present,omitempty"`
	UsageOutputPresent                    bool                               `gorm:"not null;default:false" json:"usage_output_present,omitempty"`
	UsageCacheReadPresent                 bool                               `gorm:"not null;default:false" json:"usage_cache_read_present,omitempty"`
	UsageCacheWritePresent                bool                               `gorm:"not null;default:false" json:"usage_cache_write_present,omitempty"`
	UsageOperationPresent                 bool                               `gorm:"not null;default:false" json:"usage_operation_present,omitempty"`
	OperationUnits                        *int64                             `json:"operation_units,omitempty"`
	CostUnavailableReason                 string                             `gorm:"size:64;not null;default:''" json:"cost_unavailable_reason,omitempty"`
	UpstreamKeyID                         int64                              `gorm:"index" json:"upstream_key_id,omitempty"`
	UpstreamKeyName                       string                             `gorm:"size:128" json:"upstream_key_name,omitempty"`
	UpstreamRequestID                     string                             `gorm:"size:512;index" json:"upstream_request_id,omitempty"`
	UpstreamAccepted                      bool                               `gorm:"not null;default:false" json:"upstream_accepted,omitempty"`
	BillingCommitted                      bool                               `gorm:"not null;default:false" json:"billing_committed,omitempty"`
	CostReconcileLegacyPending            bool                               `gorm:"not null;default:false" json:"-"`
	UpstreamUsageID                       *string                            `gorm:"size:128;uniqueIndex:idx_gateway_channel_usage,priority:2" json:"upstream_usage_id,omitempty"`
	ClientIP                              string                             `gorm:"size:64" json:"client_ip,omitempty"`
	ClientIPSource                        string                             `gorm:"size:32" json:"client_ip_source,omitempty"`
	SessionHash                           string                             `gorm:"size:64;index:idx_gateway_logs_session_route,priority:2" json:"-"`
	RouteReason                           string                             `gorm:"size:32;index:idx_gateway_logs_route_time,priority:1" json:"route_reason,omitempty"`
	RouteSelectionRank                    uint                               `gorm:"not null;default:0" json:"route_selection_rank,omitempty"`
	SessionAffinityOriginRequestID        uint                               `gorm:"index:idx_gateway_logs_affinity_origin" json:"session_affinity_origin_request_id,omitempty"`
	SessionAffinityExpiresAt              *time.Time                         `json:"session_affinity_expires_at,omitempty"`
	SessionAffinityOverriddenCandidates   string                             `gorm:"type:text;not null;default:''" json:"-"`
	RouteDetails                          string                             `gorm:"type:text" json:"route_details,omitempty"`
	RouteAttemptedChannels                string                             `gorm:"type:text" json:"route_attempted_channels,omitempty"`
	LogicalRequestTraceID                 string                             `gorm:"size:32;not null;default:'';index" json:"logical_request_trace_id,omitempty"`
	TelemetryVersion                      int                                `gorm:"not null;default:0" json:"telemetry_version"`
	RoutingMode                           string                             `gorm:"size:16;not null;default:''" json:"routing_mode,omitempty"`
	GatewayApp                            string                             `gorm:"size:32;not null;default:''" json:"gateway_app,omitempty"`
	TerminalFailureCode                   string                             `gorm:"size:64;not null;default:''" json:"terminal_failure_code,omitempty"`
	PriorPhysicallyDispatchedFailureCount int                                `gorm:"not null;default:0" json:"prior_physically_dispatched_failure_count"`
	RealFallback                          bool                               `gorm:"not null;default:false" json:"real_fallback"`
	RouteExclusionSnapshotVersion         int                                `gorm:"not null;default:0" json:"route_exclusion_snapshot_version"`
	RouteExclusionsSnapshot               string                             `gorm:"type:jsonb;not null;default:'{}'" json:"-"`
	CostReconcileAt                       *time.Time                         `gorm:"index" json:"cost_reconcile_at,omitempty"`
	CostReconcileTries                    int                                `gorm:"not null;default:0" json:"cost_reconcile_tries,omitempty"`
	CostReconcileConflicts                int                                `gorm:"not null;default:0" json:"cost_reconcile_conflicts,omitempty"`
	CostReconcileError                    string                             `gorm:"type:text" json:"cost_reconcile_error,omitempty"`
	CostReconcileState                    GatewayRequestCostReconcileState   `gorm:"size:24;not null;default:'idle'" json:"cost_reconcile_state"`
	CostReconcileOutcome                  GatewayRequestCostReconcileOutcome `gorm:"size:32;not null;default:'none'" json:"cost_reconcile_outcome"`
	CostReconcileIdentityLeaseID          *uint64                            `json:"-"`
	CostReconcileClaimEpoch               uint64                             `gorm:"not null;default:0" json:"-"`
	CostReconcileClaimToken               string                             `gorm:"size:64;not null;default:''" json:"-"`
	CostReconcileClaimedAt                *time.Time                         `json:"-"`
	CostReconcileLeaseExpiresAt           *time.Time                         `json:"-"`
	CostReconcileLastEventAt              *time.Time                         `json:"cost_reconcile_last_event_at,omitempty"`
	AggregateVersion                      int                                `gorm:"not null;default:0;index:idx_gateway_logs_aggregate_backfill,priority:1" json:"-"`
	GroupCacheAggregateVersion            int                                `gorm:"not null;default:0;index:idx_gateway_logs_group_cache_backfill,priority:1" json:"-"`
	Error                                 string                             `gorm:"type:text" json:"error,omitempty"`
	CreatedAt                             time.Time                          `gorm:"not null;index;index:idx_gateway_logs_cache_route,priority:4;index:idx_gateway_logs_session_route,priority:5;index:idx_gateway_logs_key_time,priority:2;index:idx_gateway_logs_channel_time,priority:2;index:idx_gateway_logs_model_time,priority:2;index:idx_gateway_logs_route_time,priority:2" json:"created_at"`
}

func (GatewayRequestLog) TableName() string { return "gateway_request_logs" }

// GatewayRequestAggregate is the persisted, incrementally maintained request
// statistics key. Fifteen-minute buckets preserve quarter-hour timezone day
// boundaries while keeping dashboard reads independent of raw log volume.
type GatewayRequestAggregate struct {
	ID                             uint64    `gorm:"primaryKey" json:"-"`
	BucketStart                    time.Time `gorm:"not null;uniqueIndex:idx_gateway_request_aggregate_key,priority:1;index" json:"bucket_start"`
	DownstreamKeyID                uint      `gorm:"not null;uniqueIndex:idx_gateway_request_aggregate_key,priority:2;index" json:"downstream_key_id"`
	ChannelID                      uint      `gorm:"not null;uniqueIndex:idx_gateway_request_aggregate_key,priority:3;index" json:"channel_id"`
	ChannelNameSnapshot            string    `gorm:"size:256;not null;default:'';uniqueIndex:idx_gateway_request_aggregate_key,priority:4" json:"channel_name_snapshot,omitempty"`
	ModelName                      string    `gorm:"size:256;not null;uniqueIndex:idx_gateway_request_aggregate_key,priority:5" json:"model_name"`
	CostSource                     string    `gorm:"size:32;not null;uniqueIndex:idx_gateway_request_aggregate_key,priority:6" json:"cost_source"`
	RouteReason                    string    `gorm:"size:32;not null;uniqueIndex:idx_gateway_request_aggregate_key,priority:7" json:"route_reason"`
	Result                         string    `gorm:"size:16;not null;uniqueIndex:idx_gateway_request_aggregate_key,priority:8" json:"result"`
	Requests                       int64     `gorm:"not null;default:0" json:"requests"`
	PromptTokens                   int64     `gorm:"not null;default:0" json:"prompt_tokens"`
	CompletionTokens               int64     `gorm:"not null;default:0" json:"completion_tokens"`
	CacheCreationTokens            int64     `gorm:"not null;default:0" json:"cache_creation_tokens"`
	CacheReadTokens                int64     `gorm:"not null;default:0" json:"cache_read_tokens"`
	TotalTokens                    int64     `gorm:"not null;default:0" json:"total_tokens"`
	TotalCost                      float64   `gorm:"not null;default:0" json:"total_cost"`
	EstimatedCost                  float64   `gorm:"not null;default:0" json:"estimated_cost"`
	ActualCost                     float64   `gorm:"not null;default:0" json:"actual_cost"`
	ReportedCost                   float64   `gorm:"not null;default:0" json:"reported_cost"`
	ReconciledCost                 float64   `gorm:"not null;default:0" json:"reconciled_cost"`
	UnavailableRequests            int64     `gorm:"not null;default:0" json:"unavailable_requests"`
	DispatchEvidencePending        int64     `gorm:"not null;default:0" json:"dispatch_evidence_pending"`
	DispatchEvidenceActual         float64   `gorm:"not null;default:0" json:"dispatch_evidence_actual"`
	DispatchEvidenceReported       float64   `gorm:"not null;default:0" json:"dispatch_evidence_reported"`
	DispatchEvidenceReconciled     float64   `gorm:"not null;default:0" json:"dispatch_evidence_reconciled"`
	LatencyTotalMs                 int64     `gorm:"not null;default:0" json:"latency_total_ms"`
	LatencySamples                 int64     `gorm:"not null;default:0" json:"latency_samples"`
	PerformanceRequests            int64     `gorm:"not null;default:0" json:"performance_requests"`
	PerformanceTokens              int64     `gorm:"not null;default:0" json:"performance_tokens"`
	PerformanceCost                float64   `gorm:"not null;default:0" json:"performance_cost"`
	PerformanceInputTokens         int64     `gorm:"not null;default:0" json:"performance_input_tokens"`
	PerformanceOutputTokens        int64     `gorm:"not null;default:0" json:"performance_output_tokens"`
	PerformanceInputPricedTokens   int64     `gorm:"not null;default:0" json:"performance_input_priced_tokens"`
	PerformanceOutputPricedTokens  int64     `gorm:"not null;default:0" json:"performance_output_priced_tokens"`
	PerformanceCacheCreationTokens int64     `gorm:"not null;default:0" json:"performance_cache_creation_tokens"`
	PerformanceCacheReadTokens     int64     `gorm:"not null;default:0" json:"performance_cache_read_tokens"`
	PerformanceInputCost           float64   `gorm:"not null;default:0" json:"performance_input_cost"`
	PerformanceOutputCost          float64   `gorm:"not null;default:0" json:"performance_output_cost"`
	FirstRequestAt                 time.Time `gorm:"not null" json:"first_request_at"`
	LastRequestAt                  time.Time `gorm:"not null" json:"last_request_at"`
	Version                        int       `gorm:"not null;default:0;index" json:"-"`
	CreatedAt                      time.Time `json:"-"`
	UpdatedAt                      time.Time `json:"-"`
}

func (GatewayRequestAggregate) TableName() string { return "gateway_request_aggregates" }

// GatewayRequestGroupCacheAggregate is the independent, fifteen-minute
// ledger used for per-channel/upstream-group cache statistics.  Group name is
// deliberately part of this table's key; the main request aggregate keeps its
// historical eight-column key and must not be rebuilt around group_name.
type GatewayRequestGroupCacheAggregate struct {
	BucketStart         time.Time `gorm:"not null;uniqueIndex:idx_gateway_request_group_cache_aggregates_key,priority:1;index" json:"bucket_start"`
	DownstreamKeyID     uint      `gorm:"not null;uniqueIndex:idx_gateway_request_group_cache_aggregates_key,priority:2;index" json:"downstream_key_id"`
	ChannelID           uint      `gorm:"not null;uniqueIndex:idx_gateway_request_group_cache_aggregates_key,priority:3;index" json:"channel_id"`
	ChannelNameSnapshot string    `gorm:"size:256;not null;default:'';uniqueIndex:idx_gateway_request_group_cache_aggregates_key,priority:4" json:"channel_name_snapshot,omitempty"`
	GroupName           string    `gorm:"size:128;not null;default:'';uniqueIndex:idx_gateway_request_group_cache_aggregates_key,priority:5;index" json:"group_name,omitempty"`
	ModelName           string    `gorm:"size:256;not null;default:'';uniqueIndex:idx_gateway_request_group_cache_aggregates_key,priority:6;index" json:"model_name,omitempty"`
	CacheReadTokens     int64     `gorm:"not null;default:0" json:"cache_read_tokens"`
	CacheCreationTokens int64     `gorm:"not null;default:0" json:"cache_creation_tokens"`
	PromptTokens        int64     `gorm:"not null;default:0" json:"prompt_tokens"`
	ValidRequests       int64     `gorm:"not null;default:0" json:"valid_requests"`
	LastNormalUseAt     time.Time `gorm:"not null;index" json:"last_normal_use_at"`
	Version             int       `gorm:"not null;default:6;index" json:"-"`
	CreatedAt           time.Time `json:"-"`
	UpdatedAt           time.Time `json:"-"`
}

func (GatewayRequestGroupCacheAggregate) TableName() string {
	return "gateway_request_group_cache_aggregates"
}

// GatewayRequestGroupCacheBackfillState serializes the one-time historical
// group-cache rebuild. LastRequestID is progress telemetry only; the durable
// per-log marker on GatewayRequestLog is the exactly-once boundary.
type GatewayRequestGroupCacheBackfillState struct {
	ID            int16     `gorm:"primaryKey" json:"-"`
	LastRequestID uint64    `gorm:"not null;default:0" json:"last_request_id"`
	TargetVersion int       `gorm:"not null;default:6" json:"target_version"`
	Completed     bool      `gorm:"not null;default:false" json:"completed"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (GatewayRequestGroupCacheBackfillState) TableName() string {
	return "gateway_request_group_cache_backfill_state"
}

// SystemMetadata pins storage-level invariants that must not silently change
// between process restarts.
type SystemMetadata struct {
	Key       string    `gorm:"primaryKey;size:128" json:"-"`
	Value     string    `gorm:"type:text;not null" json:"-"`
	CreatedAt time.Time `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

func (SystemMetadata) TableName() string { return "system_metadata" }

// GatewayModelCapability 缓存网关探测出的 group/model 支持关系。
type GatewayModelCapability struct {
	ID                      uint       `gorm:"primaryKey" json:"id"`
	ChannelID               uint       `gorm:"not null;uniqueIndex:idx_gateway_model_capability" json:"channel_id"`
	UpstreamGroupID         uint       `gorm:"not null;default:0;index" json:"-"`
	PresenceGeneration      uint64     `gorm:"not null;default:0" json:"-"`
	ChannelIdentityRevision uint64     `gorm:"not null;default:0;index" json:"-"`
	GroupName               string     `gorm:"size:128;not null;uniqueIndex:idx_gateway_model_capability" json:"group_name"`
	ModelName               string     `gorm:"size:256;not null;uniqueIndex:idx_gateway_model_capability" json:"model_name"`
	Supported               bool       `gorm:"not null" json:"supported"`
	LastCheckedAt           time.Time  `gorm:"not null;index" json:"last_checked_at"`
	LastError               string     `gorm:"type:text" json:"last_error,omitempty"`
	ContextWindowTokens     int64      `gorm:"not null;default:0" json:"context_window_tokens,omitempty"`
	SafeInputTokens         int64      `gorm:"not null;default:0" json:"safe_input_tokens,omitempty"`
	ContextSafetyTokens     int64      `gorm:"not null;default:0" json:"context_safety_tokens,omitempty"`
	ContextLimitObserved    *time.Time `gorm:"column:context_limit_observed_at" json:"context_limit_observed_at,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

func (GatewayModelCapability) TableName() string { return "gateway_model_capabilities" }

type GatewayImageTaskMode string

const (
	GatewayImageTaskModeUnknown   GatewayImageTaskMode = "unknown"
	GatewayImageTaskModeSync      GatewayImageTaskMode = "sync"
	GatewayImageTaskModeAsync     GatewayImageTaskMode = "async"
	GatewayImageTaskModeSyncAsync GatewayImageTaskMode = "sync_async"
)

const (
	GatewayImageOperationGeneration = "image_generation"
	GatewayImageProtocolImages      = "openai_images"
	GatewayImageProtocolResponses   = "openai_responses"

	GatewayImageEvidenceObservedSuccess  = "observed_success"
	GatewayImageEvidenceProviderMetadata = "provider_metadata"
	GatewayImageEvidenceAdminConfig      = "admin_config"
)

// GatewayImageOperationCapability describes one paid image operation without
// storing task IDs, idempotency-key values, credentials, or upstream bodies.
// Automatic replay remains disabled even when every upstream primitive is
// described; the gateway has no task-recovery execution path yet.
type GatewayImageOperationCapability struct {
	ID                      uint                 `gorm:"primaryKey" json:"id"`
	ChannelID               uint                 `gorm:"not null;index:idx_gateway_image_capability_route,priority:1" json:"channel_id"`
	ChannelName             string               `gorm:"-" json:"channel_name,omitempty"`
	UpstreamGroupID         uint                 `gorm:"not null;uniqueIndex:idx_gateway_image_capability_identity,priority:1;index" json:"upstream_group_id"`
	CanonicalGroupName      string               `gorm:"size:128;not null;index:idx_gateway_image_capability_route,priority:2" json:"group_name"`
	PresenceGeneration      uint64               `gorm:"not null;uniqueIndex:idx_gateway_image_capability_identity,priority:2" json:"presence_generation"`
	ModelName               string               `gorm:"size:256;not null;uniqueIndex:idx_gateway_image_capability_identity,priority:3;index:idx_gateway_image_capability_route,priority:3" json:"model_name"`
	Operation               string               `gorm:"size:32;not null;uniqueIndex:idx_gateway_image_capability_identity,priority:4" json:"operation"`
	Protocol                string               `gorm:"size:32;not null;uniqueIndex:idx_gateway_image_capability_identity,priority:5" json:"protocol"`
	EndpointPath            string               `gorm:"size:128;not null" json:"endpoint_path"`
	TaskMode                GatewayImageTaskMode `gorm:"size:16;not null" json:"task_mode"`
	IdempotencyKeySupported bool                 `gorm:"not null;default:false" json:"idempotency_key_supported"`
	IdempotencyHeader       string               `gorm:"size:128;not null" json:"idempotency_header,omitempty"`
	TaskIDReturned          bool                 `gorm:"not null;default:false" json:"task_id_returned"`
	TaskIDJSONPath          string               `gorm:"size:256;not null" json:"task_id_json_path,omitempty"`
	TaskQuerySupported      bool                 `gorm:"not null;default:false" json:"task_query_supported"`
	TaskQueryMethod         string               `gorm:"size:16;not null" json:"task_query_method,omitempty"`
	TaskQueryPathTemplate   string               `gorm:"size:512;not null" json:"task_query_path_template,omitempty"`
	TaskStatusJSONPath      string               `gorm:"size:256;not null" json:"task_status_json_path,omitempty"`
	TaskSuccessValues       []string             `gorm:"type:jsonb;serializer:json" json:"task_success_values"`
	TaskFailureValues       []string             `gorm:"type:jsonb;serializer:json" json:"task_failure_values"`
	EvidenceSource          string               `gorm:"size:32;not null;index" json:"evidence_source"`
	EvidenceDetail          string               `gorm:"size:64;not null" json:"evidence_detail"`
	EvidenceObservedAt      time.Time            `gorm:"not null;index" json:"evidence_observed_at"`
	ConfiguredBy            string               `gorm:"size:128;not null" json:"configured_by,omitempty"`
	CapabilityVersion       uint64               `gorm:"not null;default:1" json:"capability_version"`
	RecoveryComplete        bool                 `gorm:"-" json:"recovery_complete"`
	AutomaticReplayAllowed  bool                 `gorm:"-" json:"automatic_replay_allowed"`
	RecoveryBlockers        []string             `gorm:"-" json:"recovery_blockers"`
	CreatedAt               time.Time            `json:"created_at"`
	UpdatedAt               time.Time            `json:"updated_at"`
}

func (GatewayImageOperationCapability) TableName() string {
	return "gateway_image_operation_capabilities"
}

// GatewayImageOperationCapabilityAudit is append-only. Snapshot contains only
// the normalized capability descriptor and never contains an upstream body,
// task ID, idempotency-key value, credential, URL origin, or proxy detail.
type GatewayImageOperationCapabilityAudit struct {
	ID                    uint64         `gorm:"primaryKey" json:"id"`
	CapabilityID          *uint          `gorm:"index" json:"capability_id,omitempty"`
	ChannelID             uint           `gorm:"not null;index" json:"channel_id"`
	UpstreamGroupID       uint           `gorm:"not null;index" json:"upstream_group_id"`
	CanonicalGroupName    string         `gorm:"size:128;not null" json:"group_name"`
	PresenceGeneration    uint64         `gorm:"not null" json:"presence_generation"`
	ModelName             string         `gorm:"size:256;not null" json:"model_name"`
	Operation             string         `gorm:"size:32;not null" json:"operation"`
	Protocol              string         `gorm:"size:32;not null" json:"protocol"`
	Action                string         `gorm:"size:32;not null" json:"action"`
	EvidenceSource        string         `gorm:"size:32;not null" json:"evidence_source"`
	EvidenceObservedAt    time.Time      `gorm:"not null" json:"evidence_observed_at"`
	EvidenceReferenceKind string         `gorm:"size:32;not null" json:"evidence_reference_kind,omitempty"`
	EvidenceReferenceID   uint64         `gorm:"not null;default:0" json:"evidence_reference_id,omitempty"`
	Actor                 string         `gorm:"size:128;not null" json:"actor"`
	CapabilityVersion     uint64         `gorm:"not null" json:"capability_version"`
	Snapshot              map[string]any `gorm:"type:jsonb;serializer:json" json:"snapshot"`
	CreatedAt             time.Time      `gorm:"not null;index" json:"created_at"`
}

func (GatewayImageOperationCapabilityAudit) TableName() string {
	return "gateway_image_operation_capability_audits"
}

type ModelCheckTargetStatus string

const (
	ModelCheckTargetPending     ModelCheckTargetStatus = "pending"
	ModelCheckTargetHealthy     ModelCheckTargetStatus = "healthy"
	ModelCheckTargetSuspect     ModelCheckTargetStatus = "suspect"
	ModelCheckTargetUnavailable ModelCheckTargetStatus = "unavailable"
)

type ModelCheckRunStatus string

const (
	ModelCheckRunPassed   ModelCheckRunStatus = "passed"
	ModelCheckRunAnomaly  ModelCheckRunStatus = "anomaly"
	ModelCheckRunError    ModelCheckRunStatus = "error"
	ModelCheckRunDeferred ModelCheckRunStatus = "deferred"
)

// ModelCheckTarget is the incrementally maintained status row read by the
// model-check page and channel warning badges. Raw history stays in
// ModelCheckRun, so management pages never scan the full run table.
type ModelCheckTarget struct {
	ID        uint `gorm:"primaryKey" json:"id"`
	ChannelID uint `gorm:"not null;uniqueIndex:idx_model_check_target,priority:1;index:idx_model_check_channel_alert,priority:1" json:"channel_id"`
	// UpstreamAccountIdentityRevision is the account fence for this mutable
	// current-status row. A channel ID can be reconfigured for a different
	// upstream account, so status without this revision must never be reused.
	UpstreamAccountIdentityRevision uint64                 `gorm:"not null;default:0;index:idx_model_check_current_identity,priority:2" json:"-"`
	UpstreamGroupID                 uint                   `gorm:"not null;default:0" json:"-"`
	UpstreamGroupGeneration         uint64                 `gorm:"not null;default:0" json:"-"`
	ChannelName                     string                 `gorm:"size:128;not null" json:"channel_name"`
	GroupName                       string                 `gorm:"size:128;not null;uniqueIndex:idx_model_check_target,priority:2" json:"group_name"`
	ModelName                       string                 `gorm:"size:256;not null;uniqueIndex:idx_model_check_target,priority:3" json:"model_name"`
	App                             GatewayApp             `gorm:"size:32;not null;uniqueIndex:idx_model_check_target,priority:4" json:"app"`
	Enabled                         bool                   `gorm:"not null;default:true;index:idx_model_check_due,priority:1;index:idx_model_check_current_identity,priority:3" json:"enabled"`
	Active                          bool                   `gorm:"not null;default:true;index:idx_model_check_due,priority:2;index:idx_model_check_current_identity,priority:4" json:"active"`
	LastDiscoveredAt                time.Time              `gorm:"not null" json:"last_discovered_at"`
	LastCheckedAt                   *time.Time             `gorm:"index:idx_model_check_due,priority:3;index:idx_model_check_current_identity,priority:5" json:"last_checked_at,omitempty"`
	NextDueAt                       *time.Time             `json:"next_due_at,omitempty"`
	LastStatus                      ModelCheckTargetStatus `gorm:"size:24;not null;default:'pending';index:idx_model_check_channel_alert,priority:2" json:"status"`
	LastScore                       *float64               `json:"last_score,omitempty"`
	BaselineScore                   *float64               `json:"baseline_score,omitempty"`
	BaselineSamples                 int                    `gorm:"not null;default:0" json:"baseline_samples"`
	RecentAnomalies                 int                    `gorm:"not null;default:0" json:"recent_anomalies"`
	ConsecutiveErrors               int                    `gorm:"not null;default:0" json:"consecutive_errors"`
	AlertActive                     bool                   `gorm:"not null;default:false;index:idx_model_check_channel_alert,priority:3" json:"alert_active"`
	AlertReason                     string                 `gorm:"type:text" json:"alert_reason,omitempty"`
	AlertedAt                       *time.Time             `json:"alerted_at,omitempty"`
	RecoveredAt                     *time.Time             `json:"recovered_at,omitempty"`
	LastActualModel                 string                 `gorm:"size:256" json:"last_actual_model,omitempty"`
	LastError                       string                 `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt                       time.Time              `json:"created_at"`
	UpdatedAt                       time.Time              `json:"updated_at"`
}

func (ModelCheckTarget) TableName() string { return "model_check_targets" }

// ModelCheckRun contains only bounded, redacted evidence. API keys,
// authorization headers and raw credentials must never be persisted here.
type ModelCheckRun struct {
	ID        uint64 `gorm:"primaryKey" json:"id"`
	TargetID  uint   `gorm:"not null;index:idx_model_check_runs_target_time,priority:1;index:idx_model_check_runs_identity_time,priority:1" json:"target_id"`
	ChannelID uint   `gorm:"not null;index" json:"channel_id"`
	// UpstreamAccountIdentityRevision binds this immutable evidence to the
	// account that actually received the probe. Legacy rows remain revision 0
	// and are intentionally hidden from a later account's current target.
	UpstreamAccountIdentityRevision uint64              `gorm:"not null;default:0;index:idx_model_check_runs_identity_time,priority:2" json:"-"`
	UpstreamGroupID                 uint                `gorm:"not null;default:0" json:"-"`
	UpstreamGroupGeneration         uint64              `gorm:"not null;default:0" json:"-"`
	ChannelName                     string              `gorm:"size:128;not null" json:"channel_name"`
	GroupName                       string              `gorm:"size:128;not null" json:"group_name"`
	ModelName                       string              `gorm:"size:256;not null" json:"model_name"`
	ActualModel                     string              `gorm:"size:256" json:"actual_model,omitempty"`
	App                             GatewayApp          `gorm:"size:32;not null" json:"app"`
	ProbeVersion                    string              `gorm:"size:32;not null" json:"probe_version"`
	Status                          ModelCheckRunStatus `gorm:"size:24;not null;index" json:"status"`
	Score                           float64             `gorm:"not null;default:0" json:"score"`
	BaselineBefore                  *float64            `json:"baseline_before,omitempty"`
	HardMismatch                    bool                `gorm:"not null;default:false" json:"hard_mismatch"`
	AnomalyReasons                  string              `gorm:"type:text" json:"-"`
	EvidenceJSON                    string              `gorm:"type:text;not null" json:"-"`
	RequestCount                    int                 `gorm:"not null;default:0" json:"request_count"`
	PromptTokens                    int64               `gorm:"not null;default:0" json:"prompt_tokens"`
	CompletionTokens                int64               `gorm:"not null;default:0" json:"completion_tokens"`
	CacheReadTokens                 int64               `gorm:"not null;default:0" json:"cache_read_tokens"`
	TotalTokens                     int64               `gorm:"not null;default:0" json:"total_tokens"`
	FirstByteMS                     int64               `gorm:"not null;default:0" json:"first_byte_ms"`
	LatencyMS                       int64               `gorm:"not null;default:0" json:"latency_ms"`
	Error                           string              `gorm:"type:text" json:"error,omitempty"`
	StartedAt                       time.Time           `gorm:"not null" json:"started_at"`
	FinishedAt                      time.Time           `gorm:"not null;index:idx_model_check_runs_target_time,priority:2;index:idx_model_check_runs_identity_time,priority:3;index" json:"finished_at"`
	CreatedAt                       time.Time           `json:"created_at"`
}

func (ModelCheckRun) TableName() string { return "model_check_runs" }

// NotificationChannelType 通知渠道类型。第一版至少 telegram，其它预留。
type NotificationChannelType string

const (
	NotifyTelegram    NotificationChannelType = "telegram"
	NotifyWebhook     NotificationChannelType = "webhook"
	NotifyEmail       NotificationChannelType = "email"
	NotifyWecom       NotificationChannelType = "wecom"
	NotifyDingTalk    NotificationChannelType = "dingtalk"
	NotifyFeishu      NotificationChannelType = "feishu"
	NotifyServerChan3 NotificationChannelType = "serverchan3"
)

// NotificationChannel 通知渠道配置。ConfigCipher 加密保存 JSON 配置（含 token / webhook url / 密码等）。
//
// Subscriptions 是 JSON 数组，记录该渠道关心的上游、事件和分组过滤；为空 / "[]" 表示订阅一切。
// 非敏感数据，明文保存，方便 Dispatcher 直接读取过滤而不解密。
type NotificationChannel struct {
	ID           uint                    `gorm:"primaryKey" json:"id"`
	Name         string                  `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type         NotificationChannelType `gorm:"size:32;not null;index" json:"type"`
	ConfigCipher string                  `gorm:"type:text;not null" json:"-"`
	// Config contains only the API-safe editable subset assembled by the API
	// layer. It is never stored by GORM and never contains tokens, passwords,
	// webhook URLs, signing secrets, or raw headers.
	Config        map[string]any `gorm:"-" json:"config,omitempty"`
	Subscriptions string         `gorm:"size:4096;not null;default:'[]'" json:"subscriptions"`
	Enabled       bool           `gorm:"default:true" json:"enabled"`
	ProxyEnabled  bool           `gorm:"default:false" json:"proxy_enabled"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
}

func (NotificationChannel) TableName() string { return "notification_channels" }

// NotificationEvent 系统内部触发的通知事件类型。
type NotificationEvent string

const (
	EventBalanceLow             NotificationEvent = "balance_low"
	EventRateChanged            NotificationEvent = "rate_changed"
	EventRateStructureChanged   NotificationEvent = "rate_structure_changed"
	EventRateAdded              NotificationEvent = "rate_added"
	EventRateRemoved            NotificationEvent = "rate_removed"
	EventAnnouncement           NotificationEvent = "announcement"
	EventLoginFailed            NotificationEvent = "login_failed"
	EventCaptchaFailed          NotificationEvent = "captcha_failed"
	EventMonitorFailed          NotificationEvent = "monitor_failed"
	EventSubscriptionDailyLow   NotificationEvent = "subscription_daily_remaining_low"
	EventSubscriptionWeeklyLow  NotificationEvent = "subscription_weekly_remaining_low"
	EventSubscriptionMonthlyLow NotificationEvent = "subscription_monthly_remaining_low"
	EventSubscriptionExpiring   NotificationEvent = "subscription_expiring"
	EventModelCheckAlert        NotificationEvent = "model_check_alert"
	EventModelCheckRecovered    NotificationEvent = "model_check_recovered"
)

// NotificationLog 通知发送记录。
type NotificationLog struct {
	ID                uint              `gorm:"primaryKey" json:"id"`
	ChannelID         uint              `gorm:"not null;index" json:"channel_id"`
	UpstreamChannelID uint              `gorm:"not null;default:0;index" json:"upstream_channel_id,omitempty"`
	Event             NotificationEvent `gorm:"size:64;not null;index" json:"event"`
	Subject           string            `gorm:"size:512;not null" json:"subject"`
	Body              string            `gorm:"type:text" json:"body"`
	Success           bool              `gorm:"not null" json:"success"`
	ErrorMessage      string            `gorm:"type:text" json:"error_message,omitempty"`
	SentAt            time.Time         `gorm:"not null;index" json:"sent_at"`
}

func (NotificationLog) TableName() string { return "notification_logs" }

// NotificationCooldown 跨重启持久化的通知冷却记录。
//
// 业务键 (ChannelID, Event)：标记某渠道某类事件最近一次发送时间。
// Dispatcher 在发送 cooldown-aware 事件（如 balance_low）前查这张表，
// 命中且未过 cooldown 就跳过。
//
// 不和 NotificationLog 合并是因为：
//   - NotificationLog 是审计/历史日志（用户可见、可清理）
//   - NotificationCooldown 是去抖控制平面（仅最新一条、原子 upsert）
//
// ChannelID 这里指的是**上游渠道**（storage.Channel），不是通知渠道。
type NotificationCooldown struct {
	ChannelID  uint              `gorm:"primaryKey" json:"channel_id"`
	Event      NotificationEvent `gorm:"primaryKey;size:64" json:"event"`
	LastSentAt time.Time         `gorm:"not null" json:"last_sent_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

func (NotificationCooldown) TableName() string { return "notification_cooldowns" }

// MonitorJob 监控任务类型。
type MonitorJob string

const (
	MonitorJobLogin   MonitorJob = "login"
	MonitorJobBalance MonitorJob = "balance"
	MonitorJobCost    MonitorJob = "cost"
	MonitorJobRates   MonitorJob = "rates"
)

// MonitorLog 每次扫描 / 登录尝试的结果，便于诊断失败。
type MonitorLog struct {
	ID           uint       `gorm:"primaryKey" json:"id"`
	ChannelID    uint       `gorm:"not null;index" json:"channel_id"`
	Job          MonitorJob `gorm:"size:32;not null;index" json:"job"`
	Success      bool       `gorm:"not null" json:"success"`
	ErrorMessage string     `gorm:"type:text" json:"error_message,omitempty"`
	DurationMS   int64      `json:"duration_ms"`
	StartedAt    time.Time  `gorm:"not null;index" json:"started_at"`
	FinishedAt   time.Time  `json:"finished_at"`
}

func (MonitorLog) TableName() string { return "monitor_logs" }
