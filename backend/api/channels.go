package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/progress"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func registerChannels(g *gin.RouterGroup, d *Deps) {
	gp := g.Group("/channels")
	gp.GET("", func(c *gin.Context) { listChannels(c, d) })
	gp.GET("/cost-comparison", func(c *gin.Context) { channelCostComparison(c, d) })
	gp.GET("/deletions", func(c *gin.Context) { listChannelDeletionReviews(c, d) })
	gp.POST("", func(c *gin.Context) { createChannel(c, d) })
	gp.POST("/direct-models", func(c *gin.Context) { probeDirectModels(c, d) })
	gp.POST("/sync-all", func(c *gin.Context) { syncAllChannels(c, d) })
	gp.POST("/:id/direct-models", func(c *gin.Context) { probeSavedDirectModels(c, d) })
	gp.POST("/:id/direct-credentials/:credential_id/models", func(c *gin.Context) { probeSavedDirectCredentialModels(c, d) })
	gp.GET("/:id", func(c *gin.Context) { getChannel(c, d) })
	gp.PUT("/:id", func(c *gin.Context) { updateChannel(c, d) })
	gp.DELETE("/:id", func(c *gin.Context) { deleteChannel(c, d) })
	gp.GET("/:id/deletion", func(c *gin.Context) { getChannelDeletionReview(c, d) })
	gp.POST("/:id/deletion/claims/:claim_id/requeue", func(c *gin.Context) { requeueChannelDeletionReviewClaim(c, d) })
	gp.POST("/:id/deletion/claims/:claim_id/retain", func(c *gin.Context) { retainChannelDeletionReviewClaim(c, d) })
	gp.POST("/:id/clear-login-info", func(c *gin.Context) { clearChannelLoginInfo(c, d) })
	gp.POST("/:id/enable", func(c *gin.Context) { toggleChannel(c, d, true) })
	gp.POST("/:id/disable", func(c *gin.Context) { toggleChannel(c, d, false) })
	gp.POST("/:id/test-login", func(c *gin.Context) { testLogin(c, d) })
	gp.POST("/:id/refresh-balance", func(c *gin.Context) { refreshBalance(c, d) })
	gp.POST("/:id/refresh-rates", func(c *gin.Context) { refreshRates(c, d) })
	gp.POST("/:id/redeem", func(c *gin.Context) { redeemChannel(c, d) })
	gp.GET("/:id/recharge-info", func(c *gin.Context) { channelRechargeInfo(c, d) })
	gp.POST("/:id/recharge", func(c *gin.Context) { createChannelRecharge(c, d) })
	gp.GET("/:id/cost-profile", func(c *gin.Context) { channelCostProfile(c, d) })
	gp.POST("/:id/cost-unit-rebase", func(c *gin.Context) { rebaseChannelCostUnit(c, d) })
	gp.POST("/:id/cost-basis-override", func(c *gin.Context) { overrideChannelCostBasis(c, d) })
	gp.POST("/:id/cost-unit-repair-adoption/preview", func(c *gin.Context) { previewChannelCostUnitRepairAdoption(c, d) })
	gp.POST("/:id/cost-unit-repair-adoption", func(c *gin.Context) { applyChannelCostUnitRepairAdoption(c, d) })
	gp.POST("/:id/funding-events", func(c *gin.Context) { createChannelFundingEvent(c, d) })
	gp.POST("/:id/funding-candidates/:candidate_id/confirm", func(c *gin.Context) { confirmChannelFundingCandidate(c, d) })
	gp.GET("/:id/subscription-info", func(c *gin.Context) { channelSubscriptionInfo(c, d) })
	gp.POST("/:id/subscription", func(c *gin.Context) { createChannelSubscription(c, d) })
	gp.GET("/:id/subscription-usage", func(c *gin.Context) { channelSubscriptionUsage(c, d) })
	gp.GET("/:id/api-keys/groups", func(c *gin.Context) { listChannelAPIKeyGroups(c, d) })
	gp.GET("/:id/api-keys", func(c *gin.Context) { listChannelAPIKeys(c, d) })
	gp.POST("/:id/api-keys", func(c *gin.Context) { createChannelAPIKey(c, d) })
	gp.PUT("/:id/api-keys/:key_id", func(c *gin.Context) { updateChannelAPIKey(c, d) })
	gp.DELETE("/:id/api-keys/:key_id", func(c *gin.Context) { deleteChannelAPIKey(c, d) })
	gp.POST("/:id/api-keys/:key_id/reveal", func(c *gin.Context) { revealChannelAPIKey(c, d) })
	gp.POST("/:id/sync", func(c *gin.Context) { syncChannel(c, d) })
	gp.GET("/:id/rates", func(c *gin.Context) { channelRates(c, d) })
	gp.GET("/:id/balance-history", func(c *gin.Context) { balanceHistory(c, d) })
}

type channelInput struct {
	Name                   string                  `json:"name" binding:"required"`
	Type                   storage.ChannelType     `json:"type" binding:"required"`
	GatewayApp             storage.GatewayApp      `json:"gateway_app"`
	SiteURL                string                  `json:"site_url" binding:"required"`
	Username               string                  `json:"username"`
	SortOrder              int                     `json:"sort_order"`
	Password               string                  `json:"password"`
	CredentialMode         storage.CredentialMode  `json:"credential_mode"`
	TokenCredential        string                  `json:"token_credential"` // JSON：token 模式时填写
	DirectAPIKey           string                  `json:"direct_api_key"`   // direct 模式时填写，永不回显
	DirectModels           string                  `json:"direct_models"`    // JSON：direct 模式选择的模型 ID
	DirectCredentials      []directCredentialInput `json:"direct_credentials"`
	LoginExtraParams       string                  `json:"login_extra_params"`
	TurnstileEnabled       bool                    `json:"turnstile_enabled"`
	IgnoreAnnouncements    bool                    `json:"ignore_announcements"`
	SubscriptionEnabled    bool                    `json:"subscription_enabled"`
	ProxyEnabled           bool                    `json:"proxy_enabled"`
	NetworkProxyID         *uint                   `json:"network_proxy_id"`
	CaptchaConfigID        *uint                   `json:"captcha_config_id"`
	BalanceThreshold       float64                 `json:"balance_threshold"`
	RechargeMultiplier     *float64                `json:"recharge_multiplier"`
	RechargeMultiplierMode string                  `json:"recharge_multiplier_mode"`
	MonitorEnabled         bool                    `json:"monitor_enabled"`
}

type channelUpdateInput struct {
	ExpectedAdminRevision     *uint64                  `json:"expected_admin_revision"`
	Name                      *string                  `json:"name"`
	GatewayApp                *storage.GatewayApp      `json:"gateway_app"`
	SiteURL                   *string                  `json:"site_url"`
	Username                  *string                  `json:"username"`
	SortOrder                 *int                     `json:"sort_order"`
	Password                  *string                  `json:"password"`
	CredentialMode            *storage.CredentialMode  `json:"credential_mode"`
	TokenCredential           *string                  `json:"token_credential"`
	DirectAPIKey              *string                  `json:"direct_api_key"`
	DirectModels              *string                  `json:"direct_models"`
	DirectCredentials         *[]directCredentialInput `json:"direct_credentials"`
	LoginExtraParams          *string                  `json:"login_extra_params"`
	TurnstileEnabled          *bool                    `json:"turnstile_enabled"`
	IgnoreAnnouncements       *bool                    `json:"ignore_announcements"`
	SubscriptionEnabled       *bool                    `json:"subscription_enabled"`
	ProxyEnabled              *bool                    `json:"proxy_enabled"`
	NetworkProxyID            *uint                    `json:"network_proxy_id"`
	CaptchaConfigID           *uint                    `json:"captcha_config_id"`
	BalanceThreshold          *float64                 `json:"balance_threshold"`
	RechargeMultiplier        *float64                 `json:"recharge_multiplier"`
	RechargeMultiplierMode    *string                  `json:"recharge_multiplier_mode"`
	MonitorEnabled            *bool                    `json:"monitor_enabled"`
	networkProxyIDPresent     bool
	captchaConfigIDPresent    bool
	rechargeMultiplierPresent bool
}

// UnmarshalJSON preserves the distinction between an omitted nullable field
// and an explicit JSON null. The service layer uses the presence bits to
// perform an intentional clear instead of silently retaining the old value.
func (in *channelUpdateInput) UnmarshalJSON(data []byte) error {
	type plain channelUpdateInput
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*in = channelUpdateInput(decoded)
	_, in.networkProxyIDPresent = fields["network_proxy_id"]
	_, in.captchaConfigIDPresent = fields["captcha_config_id"]
	_, in.rechargeMultiplierPresent = fields["recharge_multiplier"]
	return nil
}

type channelToggleInput struct {
	ExpectedAdminRevision *uint64 `json:"expected_admin_revision"`
}

type directCredentialInput struct {
	ID              uint    `json:"id"`
	GroupName       string  `json:"group_name"`
	BaseURL         string  `json:"base_url"`
	APIKey          string  `json:"api_key"`
	Models          string  `json:"models"`
	Ratio           float64 `json:"ratio"`
	CompletionRatio float64 `json:"completion_ratio"`
	Enabled         *bool   `json:"enabled"`
}

type directCredentialOutput struct {
	ID              uint      `json:"id"`
	GroupName       string    `json:"group_name"`
	BaseURL         string    `json:"base_url"`
	APIKeyPrefix    string    `json:"api_key_prefix,omitempty"`
	KeyConfigured   bool      `json:"key_configured"`
	Models          string    `json:"models"`
	Ratio           float64   `json:"ratio"`
	CompletionRatio float64   `json:"completion_ratio"`
	Enabled         bool      `json:"enabled"`
	Revision        uint64    `json:"revision"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func directCredentialInputs(items []directCredentialInput) []channel.DirectCredentialInput {
	out := make([]channel.DirectCredentialInput, 0, len(items))
	for _, item := range items {
		out = append(out, channel.DirectCredentialInput{
			ID: item.ID, GroupName: item.GroupName, BaseURL: item.BaseURL, APIKey: item.APIKey,
			Models: item.Models, Ratio: item.Ratio, CompletionRatio: item.CompletionRatio, Enabled: item.Enabled,
		})
	}
	return out
}

type channelOutput struct {
	storage.Channel
	DirectCredentials          []directCredentialOutput                    `json:"direct_credentials,omitempty"`
	UpstreamAccountConcurrency *storage.UpstreamAccountConcurrencySnapshot `json:"upstream_account_concurrency,omitempty"`
	// The embedded Channel keeps the storage snapshot convenient for the
	// administrator form, but its raw fact flag may be stale after an identity
	// change. These explicit fields shadow the embedded JSON keys and are filled
	// only when the bool/observed_at/revision tuple belongs to this identity.
	AccountBalanceInsufficient   bool                           `json:"upstream_account_balance_insufficient,omitempty"`
	AccountBalanceInsufficientAt *time.Time                     `json:"upstream_account_balance_insufficient_at,omitempty"`
	UserID                       string                         `json:"user_id,omitempty"`
	TodayCostIsToday             bool                           `json:"today_cost_is_today"`
	PendingFundingCount          int64                          `json:"pending_funding_count"`
	ModelCheckStatus             storage.ModelCheckTargetStatus `json:"model_check_status,omitempty"`
	ModelCheckAlertCount         int                            `json:"model_check_alert_count,omitempty"`
	ModelCheckUnavailableCount   int                            `json:"model_check_unavailable_count,omitempty"`
	ModelCheckLastCheckedAt      *time.Time                     `json:"model_check_last_checked_at,omitempty"`
	ModelCheckLowestScore        *float64                       `json:"model_check_lowest_score,omitempty"`
}

type channelCostComparisonOutput struct {
	ChannelID            uint       `json:"channel_id"`
	ChannelName          string     `json:"channel_name"`
	GatewayCost24Hours   float64    `json:"gateway_cost_24h"`
	GatewayCostToday     float64    `json:"gateway_cost_today"`
	AccountCostToday     *float64   `json:"account_cost_today,omitempty"`
	Difference24Hours    *float64   `json:"difference_24h,omitempty"`
	DifferenceToday      *float64   `json:"difference_today,omitempty"`
	AccountCostSampledAt *time.Time `json:"account_cost_sampled_at,omitempty"`
}

type channelRedeemInput struct {
	Code string `json:"code"`
}

type channelRechargeInput struct {
	Amount        float64 `json:"amount"`
	PaymentMethod string  `json:"payment_method"`
	IsMobile      bool    `json:"is_mobile"`
}

type channelSubscriptionInput struct {
	PlanID        string `json:"plan_id"`
	PaymentMethod string `json:"payment_method"`
	IsMobile      bool   `json:"is_mobile"`
}

type channelAPIKeyCreateInput = connector.APIKeyCreateRequest
type channelAPIKeyUpdateInput = connector.APIKeyUpdateRequest

type directModelsProbeInput struct {
	BaseURL        string `json:"base_url" binding:"required"`
	APIKey         string `json:"api_key" binding:"required"`
	NetworkProxyID *uint  `json:"network_proxy_id"`
}

type savedDirectModelsProbeInput struct {
	BaseURL               string `json:"base_url"`
	NetworkProxyID        *uint  `json:"network_proxy_id"`
	networkProxyIDPresent bool
}

func savedDirectModelsProbeTarget(ch *storage.Channel, in savedDirectModelsProbeInput) (string, *uint, error) {
	if ch == nil || ch.ID == 0 {
		return "", nil, errors.New("渠道不存在")
	}
	if strings.TrimSpace(in.BaseURL) != "" || in.networkProxyIDPresent {
		return "", nil, errors.New("使用已保存 API Key 时只能探测渠道已保存的地址和代理")
	}
	return ch.SiteURL, ch.NetworkProxyID, nil
}

// UnmarshalJSON lets an edit probe distinguish an omitted proxy override from
// an explicit null, which means the administrator wants a direct connection.
func (in *savedDirectModelsProbeInput) UnmarshalJSON(data []byte) error {
	type plain savedDirectModelsProbeInput
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*in = savedDirectModelsProbeInput(decoded)
	_, in.networkProxyIDPresent = fields["network_proxy_id"]
	return nil
}

type directModelsProber interface {
	ProbeDirectModels(ctx context.Context, baseURL, apiKey string, proxyID *uint) ([]string, error)
}

type directConfigurationValidator interface {
	ValidateDirectConfiguration(baseURL, directModels string, proxyID *uint) error
}

type contextChannelUpdater interface {
	UpdateContext(ctx context.Context, id uint, in channel.UpdateInput) (*storage.Channel, error)
}

func listChannels(c *gin.Context, d *Deps) {
	if c.Query("page") != "" || c.Query("page_size") != "" {
		page, pageSize, err := parseChannelPageQuery(c)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		list, total, err := d.Channels.ListPage(page, pageSize)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		pages := 1
		if total > 0 && pageSize != -1 {
			pages = int((total + int64(pageSize) - 1) / int64(pageSize))
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"items":     channelOutputs(d, list),
			"total":     total,
			"page":      page,
			"page_size": pageSize,
			"pages":     pages,
		}})
		return
	}

	list, err := d.Channels.List()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": channelOutputs(d, list)})
}

func channelCostComparison(c *gin.Context, d *Deps) {
	if d == nil || d.Channels == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("channel cost comparison is unavailable"))
		return
	}
	channels, err := d.Channels.List()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	now := time.Now().UTC()
	dayStart, _ := configuredUserTime(d).DayBoundsUTC(now)
	rollingStart := now.Add(-24 * time.Hour)
	rollingByChannel, err := d.Gateway.Store.RequestLogChannelCosts(storage.GatewayRequestLogFilter{Start: &rollingStart, End: &now, TimeZone: configuredUserTime(d).Name()})
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	todayByChannel, err := d.Gateway.Store.RequestLogChannelCosts(storage.GatewayRequestLogFilter{Start: &dayStart, End: &now, TimeZone: configuredUserTime(d).Name()})
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	rows := make([]channelCostComparisonOutput, 0, len(channels))
	for i := range channels {
		channel := &channels[i]
		row := channelCostComparisonOutput{
			ChannelID:          channel.ID,
			ChannelName:        channel.Name,
			GatewayCost24Hours: rollingByChannel[channel.ID],
			GatewayCostToday:   todayByChannel[channel.ID],
		}
		if configuredUserTime(d).IsToday(channel.TodayCostAt, now) && channel.TodayCost != nil {
			accountCost := *channel.TodayCost
			difference24Hours := accountCost - row.GatewayCost24Hours
			differenceToday := accountCost - row.GatewayCostToday
			row.AccountCostToday = &accountCost
			row.Difference24Hours = &difference24Hours
			row.DifferenceToday = &differenceToday
			row.AccountCostSampledAt = channel.TodayCostAt
		}
		rows = append(rows, row)
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"generated_at":       now,
		"business_day_start": dayStart,
		"rows":               rows,
	}})
}

func createChannel(c *gin.Context, d *Deps) {
	var in channelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.ProxyEnabled && (in.NetworkProxyID == nil || *in.NetworkProxyID == 0) {
		fail(c, http.StatusBadRequest, errors.New("渠道代理必须选择一个网络代理"))
		return
	}
	created, err := d.ChannelSvc.Create(channel.CreateInput{
		Name:                   in.Name,
		Type:                   in.Type,
		GatewayApp:             in.GatewayApp,
		SiteURL:                in.SiteURL,
		Username:               in.Username,
		SortOrder:              in.SortOrder,
		Password:               in.Password,
		CredentialMode:         in.CredentialMode,
		TokenCredential:        in.TokenCredential,
		DirectAPIKey:           in.DirectAPIKey,
		DirectModels:           in.DirectModels,
		DirectCredentials:      directCredentialInputs(in.DirectCredentials),
		LoginExtraParams:       in.LoginExtraParams,
		TurnstileEnabled:       in.TurnstileEnabled,
		IgnoreAnnouncements:    in.IgnoreAnnouncements,
		SubscriptionEnabled:    (in.Type == storage.ChannelTypeSub2API || in.Type == storage.ChannelTypeNewAPI) && in.SubscriptionEnabled,
		ProxyEnabled:           in.ProxyEnabled,
		NetworkProxyID:         in.NetworkProxyID,
		CaptchaConfigID:        in.CaptchaConfigID,
		BalanceThreshold:       in.BalanceThreshold,
		RechargeMultiplier:     in.RechargeMultiplier,
		RechargeMultiplierMode: in.RechargeMultiplierMode,
		MonitorEnabled:         in.MonitorEnabled,
	})
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	if created.CredentialMode == storage.CredentialModeDirect {
		refreshDirectChannelGateway(c.Request.Context(), d, created)
	} else {
		scheduleChannelGatewayInitialization(d, created.ID)
	}
	if current, findErr := d.Channels.FindByID(created.ID); findErr == nil {
		created = current
	}
	c.JSON(http.StatusOK, gin.H{"data": channelOutputFor(d, *created)})
}

func probeDirectModels(c *gin.Context, d *Deps) {
	var in directModelsProbeInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if d == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("直连模型探测不可用"))
		return
	}
	prober, ok := d.ChannelSvc.(directModelsProber)
	if !ok {
		fail(c, http.StatusNotImplemented, errors.New("直连模型探测不可用"))
		return
	}
	models, err := prober.ProbeDirectModels(c.Request.Context(), in.BaseURL, in.APIKey, in.NetworkProxyID)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": models})
}

func probeSavedDirectModels(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if d == nil || d.Channels == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("渠道存储不可用"))
		return
	}
	ch, err := d.Channels.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	if ch.CredentialMode != storage.CredentialModeDirect {
		fail(c, http.StatusBadRequest, errors.New("渠道不是直连 API 模式"))
		return
	}

	var in savedDirectModelsProbeInput
	if err := c.ShouldBindJSON(&in); err != nil && !errors.Is(err, io.EOF) {
		fail(c, http.StatusBadRequest, err)
		return
	}
	baseURL, proxyID, err := savedDirectModelsProbeTarget(ch, in)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if d.Cipher == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("凭据解密服务不可用"))
		return
	}
	apiKey, err := d.Cipher.Decrypt(ch.PasswordCipher)
	if err != nil || strings.TrimSpace(apiKey) == "" {
		fail(c, http.StatusBadRequest, errors.New("渠道没有可用的直连 API Key"))
		return
	}
	prober, ok := d.ChannelSvc.(directModelsProber)
	if !ok {
		fail(c, http.StatusNotImplemented, errors.New("直连模型探测不可用"))
		return
	}
	models, err := prober.ProbeDirectModels(c.Request.Context(), baseURL, apiKey, proxyID)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": models})
}

func probeSavedDirectCredentialModels(c *gin.Context, d *Deps) {
	channelID, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	credentialID64, err := strconv.ParseUint(strings.TrimSpace(c.Param("credential_id")), 10, 64)
	if err != nil || credentialID64 == 0 {
		fail(c, http.StatusBadRequest, errors.New("直连配置 ID 无效"))
		return
	}
	if d == nil || d.Channels == nil || d.Cipher == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("直连模型探测不可用"))
		return
	}
	ch, err := d.Channels.FindByID(channelID)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	if ch.CredentialMode != storage.CredentialModeDirect {
		fail(c, http.StatusBadRequest, errors.New("渠道不是直连 API 模式"))
		return
	}
	items, err := d.Channels.ListDirectCredentials(channelID)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	var target *storage.ChannelDirectCredential
	for index := range items {
		if uint64(items[index].ID) == credentialID64 {
			target = &items[index]
			break
		}
	}
	if target == nil {
		fail(c, http.StatusNotFound, errors.New("直连配置不存在"))
		return
	}
	apiKey, err := d.Cipher.Decrypt(target.APIKeyCipher)
	if err != nil || strings.TrimSpace(apiKey) == "" {
		fail(c, http.StatusBadRequest, errors.New("直连配置没有可用的 API Key"))
		return
	}
	prober, ok := d.ChannelSvc.(directModelsProber)
	if !ok {
		fail(c, http.StatusNotImplemented, errors.New("直连模型探测不可用"))
		return
	}
	models, err := prober.ProbeDirectModels(c.Request.Context(), target.BaseURL, apiKey, ch.NetworkProxyID)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": models})
}

func channelOutputs(d *Deps, list []storage.Channel) []channelOutput {
	counts := map[uint]int64{}
	states := map[uint]storage.ChannelCostState{}
	if d != nil && d.Costing != nil {
		if loaded, err := d.Costing.PendingFundingCounts(); err == nil {
			counts = loaded
		}
		if loaded, err := d.Costing.ListStates(); err == nil {
			for i := range loaded {
				states[loaded[i].ChannelID] = loaded[i]
			}
		}
	}
	userIDs := channelUserIDs(d, list)
	modelChecks := channelModelCheckSummaries(d, list)
	accountConcurrency := channelAccountConcurrencySnapshots(d, list)
	userTime := configuredUserTime(d)
	now := time.Now()
	out := make([]channelOutput, 0, len(list))
	for _, ch := range list {
		ch.BalanceThreshold = ch.EffectiveBalanceThreshold()
		accountBalanceInsufficient := ch.HasCurrentUpstreamAccountBalanceInsufficient()
		var accountBalanceInsufficientAt *time.Time
		if accountBalanceInsufficient {
			accountBalanceInsufficientAt = ch.UpstreamAccountBalanceInsufficientAt
		}
		item := channelOutput{
			Channel:                      ch,
			DirectCredentials:            channelDirectCredentialOutputs(d, ch.ID),
			UpstreamAccountConcurrency:   accountConcurrency[ch.ID],
			AccountBalanceInsufficient:   accountBalanceInsufficient,
			AccountBalanceInsufficientAt: accountBalanceInsufficientAt,
			TodayCostIsToday:             userTime.IsToday(ch.TodayCostAt, now),
			UserID:                       userIDs[ch.ID],
			PendingFundingCount:          counts[ch.ID],
		}
		if summary, ok := modelChecks[ch.ID]; ok {
			item.ModelCheckStatus = summary.Status
			item.ModelCheckAlertCount = summary.AlertCount
			item.ModelCheckUnavailableCount = summary.UnavailableCount
			item.ModelCheckLastCheckedAt = summary.LastCheckedAt
			item.ModelCheckLowestScore = summary.LowestScore
		}
		out = append(out, item)
	}
	return out
}

func channelUserIDs(d *Deps, list []storage.Channel) map[uint]string {
	out := make(map[uint]string, len(list))
	passwordChannelIDs := make([]uint, 0, len(list))
	for i := range list {
		ch := &list[i]
		if ch.Type != storage.ChannelTypeNewAPI {
			continue
		}
		if ch.CredentialMode == storage.CredentialModeToken {
			if userID := channelTokenUserID(d, ch); userID != "" {
				out[ch.ID] = userID
			}
			continue
		}
		passwordChannelIDs = append(passwordChannelIDs, ch.ID)
	}
	if d == nil || d.Sessions == nil || len(passwordChannelIDs) == 0 {
		return out
	}
	sessions, err := d.Sessions.ListByChannels(passwordChannelIDs)
	if err != nil {
		return out
	}
	for channelID, session := range sessions {
		if userID := strings.TrimSpace(session.UserID); userID != "" {
			out[channelID] = userID
		}
	}
	return out
}

func channelOutputFor(d *Deps, ch storage.Channel) channelOutput {
	ch.BalanceThreshold = ch.EffectiveBalanceThreshold()
	accountBalanceInsufficient := ch.HasCurrentUpstreamAccountBalanceInsufficient()
	var accountBalanceInsufficientAt *time.Time
	if accountBalanceInsufficient {
		accountBalanceInsufficientAt = ch.UpstreamAccountBalanceInsufficientAt
	}
	out := channelOutput{
		Channel:                      ch,
		DirectCredentials:            channelDirectCredentialOutputs(d, ch.ID),
		UpstreamAccountConcurrency:   channelAccountConcurrencySnapshots(d, []storage.Channel{ch})[ch.ID],
		AccountBalanceInsufficient:   accountBalanceInsufficient,
		AccountBalanceInsufficientAt: accountBalanceInsufficientAt,
		TodayCostIsToday:             configuredUserTime(d).IsToday(ch.TodayCostAt, time.Now()),
	}
	out.UserID = channelUserID(d, &ch)
	if summary, ok := channelModelCheckSummaries(d, []storage.Channel{ch})[ch.ID]; ok {
		out.ModelCheckStatus = summary.Status
		out.ModelCheckAlertCount = summary.AlertCount
		out.ModelCheckUnavailableCount = summary.UnavailableCount
		out.ModelCheckLastCheckedAt = summary.LastCheckedAt
		out.ModelCheckLowestScore = summary.LowestScore
	}
	return out
}

func channelDirectCredentialOutputs(d *Deps, channelID uint) []directCredentialOutput {
	if d == nil || d.Channels == nil || channelID == 0 {
		return nil
	}
	items, err := d.Channels.ListDirectCredentials(channelID)
	if err != nil {
		return nil
	}
	out := make([]directCredentialOutput, 0, len(items))
	for _, item := range items {
		out = append(out, directCredentialOutput{
			ID: item.ID, GroupName: item.GroupName, BaseURL: item.BaseURL,
			APIKeyPrefix: item.APIKeyPrefix, KeyConfigured: strings.TrimSpace(item.APIKeyCipher) != "",
			Models: item.Models, Ratio: item.Ratio, CompletionRatio: item.CompletionRatio,
			Enabled: item.Enabled, Revision: item.Revision, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
		})
	}
	return out
}

func channelAccountConcurrencySnapshots(d *Deps, list []storage.Channel) map[uint]*storage.UpstreamAccountConcurrencySnapshot {
	out := make(map[uint]*storage.UpstreamAccountConcurrencySnapshot)
	if d == nil || d.Channels == nil || len(list) == 0 {
		return out
	}
	ids := make([]uint, 0, len(list))
	for i := range list {
		if list[i].ID > 0 {
			ids = append(ids, list[i].ID)
		}
	}
	snapshots, err := d.Channels.UpstreamAccountConcurrencyForChannels(ids)
	if err != nil {
		return out
	}
	for channelID, snapshot := range snapshots {
		if public, ok := publicChannelAccountConcurrencySnapshot(snapshot); ok {
			out[channelID] = public
		}
	}
	return out
}

func publicChannelAccountConcurrencySnapshot(snapshot storage.UpstreamAccountConcurrencySnapshot) (*storage.UpstreamAccountConcurrencySnapshot, bool) {
	if snapshot.Limit == nil || *snapshot.Limit < 0 || snapshot.Scope != storage.UpstreamConcurrencyScopeAccount ||
		strings.TrimSpace(snapshot.Source) == "" || snapshot.ObservedAt == nil {
		return nil, false
	}
	copy := snapshot
	copy.AccountRecordID = 0
	limit := *snapshot.Limit
	copy.Limit = &limit
	observedAt := snapshot.ObservedAt.UTC()
	copy.ObservedAt = &observedAt
	return &copy, true
}

func channelModelCheckSummaries(d *Deps, list []storage.Channel) map[uint]storage.ModelCheckChannelSummary {
	if d == nil || d.ModelCheck == nil || len(list) == 0 {
		return map[uint]storage.ModelCheckChannelSummary{}
	}
	ids := make([]uint, 0, len(list))
	for i := range list {
		ids = append(ids, list[i].ID)
	}
	summaries, err := d.ModelCheck.ChannelSummaries(ids)
	if err != nil {
		return map[uint]storage.ModelCheckChannelSummary{}
	}
	return summaries
}

func channelUserID(d *Deps, ch *storage.Channel) string {
	if d == nil || ch == nil || ch.Type != storage.ChannelTypeNewAPI {
		return ""
	}
	if userID := channelTokenUserID(d, ch); userID != "" {
		return userID
	}
	if d.Sessions != nil {
		session, err := d.Sessions.FindByChannel(ch.ID)
		if err == nil && session != nil {
			return strings.TrimSpace(session.UserID)
		}
	}
	return ""
}

func channelTokenUserID(d *Deps, ch *storage.Channel) string {
	if d == nil || ch == nil || ch.CredentialMode != storage.CredentialModeToken || d.Cipher == nil || ch.PasswordCipher == "" {
		return ""
	}
	raw, err := d.Cipher.Decrypt(ch.PasswordCipher)
	if err != nil {
		return ""
	}
	var cred channel.NewAPITokenCredential
	if json.Unmarshal([]byte(raw), &cred) != nil {
		return ""
	}
	return strings.TrimSpace(cred.UserID)
}

func getChannel(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Channels.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": channelOutputFor(d, *ch)})
}

func updateChannel(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in channelUpdateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.ProxyEnabled != nil && *in.ProxyEnabled && (in.NetworkProxyID == nil || *in.NetworkProxyID == 0) {
		fail(c, http.StatusBadRequest, errors.New("渠道代理必须选择一个网络代理"))
		return
	}
	current, err := d.Channels.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	if in.ExpectedAdminRevision == nil || *in.ExpectedAdminRevision == 0 {
		fail(c, http.StatusConflict, errors.New("渠道配置版本缺失，请刷新后重试"))
		return
	}
	if current.AdminRevision != *in.ExpectedAdminRevision {
		fail(c, http.StatusConflict, errors.New("渠道配置已被其他操作更新，请刷新后重试"))
		return
	}
	finalMode := current.CredentialMode
	if in.CredentialMode != nil && *in.CredentialMode != "" {
		finalMode = *in.CredentialMode
	}
	directCredentialsChanged := in.DirectCredentials != nil
	directKeyChanged := current.CredentialMode == storage.CredentialModeDirect && ((in.DirectAPIKey != nil && strings.TrimSpace(*in.DirectAPIKey) != "") || directCredentialsChanged)
	leavingDirect := current.CredentialMode == storage.CredentialModeDirect && finalMode != storage.CredentialModeDirect
	enteringDirect := current.CredentialMode != storage.CredentialModeDirect && finalMode == storage.CredentialModeDirect
	directRoutingChanged := current.CredentialMode == storage.CredentialModeDirect && finalMode == storage.CredentialModeDirect &&
		(directKeyChanged || in.DirectModels != nil || in.SiteURL != nil || in.GatewayApp != nil || in.networkProxyIDPresent || in.ProxyEnabled != nil)
	subscriptionEnabled := in.SubscriptionEnabled
	if subscriptionEnabled != nil {
		enabled := (current.Type == storage.ChannelTypeSub2API || current.Type == storage.ChannelTypeNewAPI) && *subscriptionEnabled
		subscriptionEnabled = &enabled
	}
	updateInput := channel.UpdateInput{
		ExpectedAdminRevision:  in.ExpectedAdminRevision,
		Name:                   in.Name,
		GatewayApp:             in.GatewayApp,
		SiteURL:                in.SiteURL,
		Username:               in.Username,
		SortOrder:              in.SortOrder,
		Password:               in.Password,
		CredentialMode:         in.CredentialMode,
		TokenCredential:        in.TokenCredential,
		DirectAPIKey:           in.DirectAPIKey,
		DirectModels:           in.DirectModels,
		DirectCredentials:      nil,
		LoginExtraParams:       in.LoginExtraParams,
		TurnstileEnabled:       in.TurnstileEnabled,
		IgnoreAnnouncements:    in.IgnoreAnnouncements,
		SubscriptionEnabled:    subscriptionEnabled,
		ProxyEnabled:           in.ProxyEnabled,
		NetworkProxyID:         in.NetworkProxyID,
		NetworkProxyIDSet:      in.networkProxyIDPresent,
		CaptchaConfigID:        in.CaptchaConfigID,
		CaptchaConfigIDSet:     in.captchaConfigIDPresent,
		BalanceThreshold:       in.BalanceThreshold,
		RechargeMultiplier:     in.RechargeMultiplier,
		RechargeMultiplierSet:  in.rechargeMultiplierPresent,
		RechargeMultiplierMode: in.RechargeMultiplierMode,
		MonitorEnabled:         in.MonitorEnabled,
	}
	if in.DirectCredentials != nil {
		converted := directCredentialInputs(*in.DirectCredentials)
		updateInput.DirectCredentials = &converted
	}
	var updated *storage.Channel
	if enteringDirect {
		collectionProvided := in.DirectCredentials != nil && len(*in.DirectCredentials) > 0
		legacyProvided := in.DirectAPIKey != nil && strings.TrimSpace(*in.DirectAPIKey) != "" && in.DirectModels != nil
		if !collectionProvided && !legacyProvided {
			fail(c, http.StatusBadRequest, errors.New("切换到直连 API 模式时必须至少填写一条完整分组配置"))
			return
		}
		if d.Gateway == nil {
			fail(c, http.StatusServiceUnavailable, errors.New("切换直连模式前无法清理托管网关密钥"))
			return
		}
		validator, ok := d.ChannelSvc.(directConfigurationValidator)
		if !ok {
			fail(c, http.StatusServiceUnavailable, errors.New("直连渠道配置校验不可用"))
			return
		}
		updater, ok := d.ChannelSvc.(contextChannelUpdater)
		if !ok {
			fail(c, http.StatusServiceUnavailable, errors.New("直连渠道原子更新不可用"))
			return
		}
		baseURL := current.SiteURL
		models := ""
		if collectionProvided {
			baseURL = (*in.DirectCredentials)[0].BaseURL
			models = (*in.DirectCredentials)[0].Models
		} else {
			if in.SiteURL != nil {
				baseURL = *in.SiteURL
			}
			models = *in.DirectModels
		}
		proxyID := current.NetworkProxyID
		if in.networkProxyIDPresent {
			proxyID = in.NetworkProxyID
		} else if in.ProxyEnabled != nil && !*in.ProxyEnabled {
			proxyID = nil
		}
		if validateErr := validator.ValidateDirectConfiguration(baseURL, models, proxyID); validateErr != nil {
			fail(c, http.StatusBadRequest, validateErr)
			return
		}
		if collectionProvided {
			for _, item := range *in.DirectCredentials {
				if validateErr := validator.ValidateDirectConfiguration(item.BaseURL, item.Models, proxyID); validateErr != nil {
					fail(c, http.StatusBadRequest, validateErr)
					return
				}
			}
		}
		cleanupCtx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		cleanupErr := d.Gateway.RetireManagedChannelRoutingAndThen(
			cleanupCtx,
			id,
			func(lockedCtx context.Context) error {
				lockedChannel, findErr := d.Channels.WithContext(lockedCtx).FindByID(id)
				if findErr != nil {
					return findErr
				}
				if lockedChannel.AdminRevision != *in.ExpectedAdminRevision {
					return storage.ErrStaleChannelAdminRevision
				}
				return validator.ValidateDirectConfiguration(baseURL, models, proxyID)
			},
			func(lockedCtx context.Context) error {
				var updateErr error
				updated, updateErr = updater.UpdateContext(lockedCtx, id, updateInput)
				return updateErr
			},
		)
		cancel()
		if cleanupErr != nil {
			scheduleChannelGatewayInitialization(d, current.ID)
			if errors.Is(cleanupErr, storage.ErrStaleChannelAdminRevision) || errors.Is(cleanupErr, storage.ErrStaleChannelAccountIdentityRevision) {
				fail(c, http.StatusConflict, errors.New("渠道配置已被其他操作更新，请刷新后重试"))
				return
			}
			fail(c, http.StatusConflict, fmt.Errorf("切换直连模式失败，原渠道凭据已保留：%w", cleanupErr))
			return
		}
	} else if directRoutingChanged || leavingDirect {
		updater, ok := d.ChannelSvc.(contextChannelUpdater)
		if !ok || d.Gateway == nil {
			fail(c, http.StatusServiceUnavailable, errors.New("直连渠道原子更新不可用"))
			return
		}
		cleanupCtx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		mutate := d.Gateway.UpdateDirectRoutingConfigurationAndThen
		if directKeyChanged || leavingDirect {
			mutate = func(ctx context.Context, channelID uint, update func(context.Context) error) error {
				return d.Gateway.InvalidateDirectRoutingStateAndThen(ctx, channelID, leavingDirect, update)
			}
		}
		err = mutate(cleanupCtx, id, func(lockedCtx context.Context) error {
			var updateErr error
			updated, updateErr = updater.UpdateContext(lockedCtx, id, updateInput)
			return updateErr
		})
		cancel()
	} else {
		updated, err = d.ChannelSvc.Update(id, updateInput)
	}
	if err != nil {
		if directRoutingChanged || leavingDirect || enteringDirect {
			scheduleChannelGatewayInitialization(d, current.ID)
		}
		if errors.Is(err, storage.ErrStaleChannelAdminRevision) || errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
			fail(c, http.StatusConflict, errors.New("渠道配置已被其他操作更新，请刷新后重试"))
			return
		}
		if errors.Is(err, storage.ErrChannelAdminRevisionRequired) {
			fail(c, http.StatusConflict, errors.New("渠道配置版本缺失，请刷新后重试"))
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	directConfigChanged := updated.CredentialMode == storage.CredentialModeDirect &&
		(in.DirectAPIKey != nil || in.DirectModels != nil || in.DirectCredentials != nil || in.SiteURL != nil || in.GatewayApp != nil || in.networkProxyIDPresent || in.ProxyEnabled != nil || in.CredentialMode != nil)
	if directConfigChanged {
		refreshDirectChannelGateway(c.Request.Context(), d, updated)
	} else if (updated.CredentialMode == storage.CredentialModeToken && (in.TokenCredential != nil || in.CredentialMode != nil)) || leavingDirect {
		scheduleChannelGatewayInitialization(d, updated.ID)
	}
	if current, findErr := d.Channels.FindByID(updated.ID); findErr == nil {
		updated = current
	}
	c.JSON(http.StatusOK, gin.H{"data": channelOutputFor(d, *updated)})
}

func deleteChannel(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if d == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("渠道删除不支持复合锁"))
		return
	}
	lifecycle, lifecycleAvailable := d.ChannelSvc.(contextChannelLifecycle)
	if !lifecycleAvailable || d.Channels == nil || d.Gateway == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("渠道删除不支持复合锁"))
		return
	}
	var current *storage.Channel
	var findErr error
	err = d.Gateway.WithChannelOperationLockAndThen(c.Request.Context(), id, func(lockedCtx context.Context) error {
		current, findErr = d.Channels.WithContext(lockedCtx).FindByID(id)
		if findErr != nil {
			return findErr
		}
		return lifecycle.DeleteContext(lockedCtx, id)
	})
	if err != nil {
		if findErr != nil {
			fail(c, http.StatusNotFound, findErr)
			return
		}
		if errors.Is(err, storage.ErrChannelDeletionPending) {
			// A delete request is only prepared at this point. Do not emit a
			// generic failure or a false success: the remote-key coordinator must
			// finish identity-fenced cleanup before local evidence can disappear.
			var review *storage.ChannelDeletionReviewSnapshot
			if d.Channels != nil {
				review, _ = d.Channels.ChannelDeletionReview(c.Request.Context(), id)
			}
			c.JSON(http.StatusAccepted, gin.H{"data": channelDeletionPendingOutput(review, id)})
			return
		}
		if current != nil && current.CredentialMode == storage.CredentialModeDirect {
			scheduleChannelGatewayInitialization(d, current.ID)
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func clearChannelLoginInfo(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if d == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("渠道凭据清理不支持复合锁"))
		return
	}
	lifecycle, lifecycleAvailable := d.ChannelSvc.(contextChannelLifecycle)
	if !lifecycleAvailable || d.Channels == nil || d.Gateway == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("渠道凭据清理不支持复合锁"))
		return
	}
	var current *storage.Channel
	var updated *storage.Channel
	var findErr error
	err = d.Gateway.WithChannelOperationLockAndThen(c.Request.Context(), id, func(lockedCtx context.Context) error {
		current, findErr = d.Channels.WithContext(lockedCtx).FindByID(id)
		if findErr != nil {
			return findErr
		}
		if current.CredentialMode != storage.CredentialModeDirect {
			var callbackErr error
			updated, callbackErr = lifecycle.ClearLoginInfoContext(lockedCtx, id)
			return callbackErr
		}
		return d.Gateway.InvalidateDirectRoutingStateAndThen(lockedCtx, id, true, func(mutationCtx context.Context) error {
			var callbackErr error
			updated, callbackErr = lifecycle.ClearLoginInfoContext(mutationCtx, id)
			return callbackErr
		})
	})
	if err != nil {
		if findErr != nil {
			fail(c, http.StatusNotFound, findErr)
			return
		}
		if current != nil && current.CredentialMode == storage.CredentialModeDirect {
			scheduleChannelGatewayInitialization(d, current.ID)
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": channelOutputFor(d, *updated)})
}

func refreshDirectChannelGateway(ctx context.Context, d *Deps, ch *storage.Channel) {
	if d == nil || d.Monitor == nil || ch == nil || ch.ID == 0 {
		return
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := d.Monitor.RefreshRates(refreshCtx, ch)
	cancel()
	if err == nil {
		return
	}
	if d.Log != nil {
		d.Log.Warn("initialize direct channel gateway key failed", "channelID", ch.ID, "err", err)
	}
	scheduleChannelGatewayInitialization(d, ch.ID)
}

func toggleChannel(c *gin.Context, d *Deps, enabled bool) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var in channelToggleInput
	if err := c.ShouldBindJSON(&in); err != nil || in.ExpectedAdminRevision == nil || *in.ExpectedAdminRevision == 0 {
		fail(c, http.StatusConflict, errors.New("渠道配置版本缺失，请刷新后重试"))
		return
	}
	_, err = d.ChannelSvc.Update(id, channel.UpdateInput{ExpectedAdminRevision: in.ExpectedAdminRevision, MonitorEnabled: &enabled})
	if err != nil {
		if errors.Is(err, storage.ErrStaleChannelAdminRevision) || errors.Is(err, storage.ErrChannelAdminRevisionRequired) {
			fail(c, http.StatusConflict, errors.New("渠道配置已被其他操作更新，请刷新后重试"))
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "monitor_enabled": enabled})
}

func testLogin(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Channels.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}

	obs := setupSSE(c)
	ctx := progress.WithObserver(c.Request.Context(), channelScopedObserver{
		base:        obs,
		channelID:   ch.ID,
		channelName: ch.Name,
		index:       1,
		total:       1,
	})

	if err := d.ChannelSvc.TestLogin(ctx, id); err != nil {
		progress.Fail(ctx, progress.StageError, err.Error())
		return
	}
	scheduleChannelGatewayInitialization(d, id)
	message := "登录测试成功"
	if ch.CredentialMode == storage.CredentialModeDirect {
		message = "直连 API 测试成功"
	}
	progress.OK(ctx, progress.StageDone, message)
}

type channelGatewayInitializationState struct {
	mu      sync.Mutex
	running bool
	pending bool
}

var channelGatewayInitializations sync.Map

func scheduleChannelGatewayInitialization(d *Deps, channelID uint) {
	if d == nil || d.Monitor == nil || d.Channels == nil || channelID == 0 {
		return
	}
	value, _ := channelGatewayInitializations.LoadOrStore(channelID, &channelGatewayInitializationState{})
	state := value.(*channelGatewayInitializationState)
	state.mu.Lock()
	if state.running {
		state.pending = true
		state.mu.Unlock()
		return
	}
	state.running = true
	state.mu.Unlock()
	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			channel, err := d.Channels.FindByID(channelID)
			if err == nil {
				err = d.Monitor.RefreshRates(ctx, channel)
			}
			cancel()
			if err != nil && d.Log != nil {
				d.Log.Warn("initialize channel gateway keys failed", "channelID", channelID, "err", err)
			}
			state.mu.Lock()
			if state.pending {
				state.pending = false
				state.mu.Unlock()
				continue
			}
			state.running = false
			state.mu.Unlock()
			return
		}
	}()
}

func refreshBalance(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Channels.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	if err := d.Monitor.RefreshBalance(c.Request.Context(), ch); err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func refreshRates(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Channels.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}
	if err := d.Monitor.RefreshRates(c.Request.Context(), ch); err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func rejectDirectChannelManagementOperation(c *gin.Context, d *Deps, channelID uint, operation string) bool {
	// Some focused handler tests intentionally use a service stub without a
	// channel store. Production wiring always supplies Channels; the service
	// remains the final guard when this API-layer check cannot inspect the mode.
	if d == nil || d.Channels == nil {
		return false
	}
	ch, err := d.Channels.FindByID(channelID)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return true
	}
	if ch.CredentialMode != storage.CredentialModeDirect {
		return false
	}
	fail(c, http.StatusBadRequest, fmt.Errorf("直连 API 渠道不支持%s", operation))
	return true
}

func redeemChannel(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "兑换码") {
		return
	}

	var in channelRedeemInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	res, err := d.ChannelSvc.RedeemCode(c.Request.Context(), id, in.Code)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": res})
}

func channelRechargeInfo(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "充值") {
		return
	}
	info, err := d.ChannelSvc.GetRechargeInfo(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": info})
}

func createChannelRecharge(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "充值") {
		return
	}
	var in channelRechargeInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.Amount <= 0 {
		fail(c, http.StatusBadRequest, fmt.Errorf("充值金额必须大于 0"))
		return
	}
	if in.PaymentMethod != "alipay" && in.PaymentMethod != "wxpay" {
		fail(c, http.StatusBadRequest, fmt.Errorf("仅支持 alipay 或 wxpay"))
		return
	}
	res, err := d.ChannelSvc.CreateRecharge(c.Request.Context(), id, connector.RechargeRequest{
		Amount:        in.Amount,
		PaymentMethod: in.PaymentMethod,
		IsMobile:      in.IsMobile,
	})
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": res})
}

func channelSubscriptionInfo(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "订阅") {
		return
	}
	info, err := d.ChannelSvc.GetSubscriptionInfo(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": info})
}

func createChannelSubscription(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "订阅") {
		return
	}
	var in channelSubscriptionInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(in.PlanID) == "" {
		fail(c, http.StatusBadRequest, fmt.Errorf("请选择订阅套餐"))
		return
	}
	if strings.TrimSpace(in.PaymentMethod) == "" {
		fail(c, http.StatusBadRequest, fmt.Errorf("请选择支付方式"))
		return
	}
	res, err := d.ChannelSvc.CreateSubscription(c.Request.Context(), id, connector.SubscriptionRequest{
		PlanID:        in.PlanID,
		PaymentMethod: in.PaymentMethod,
		IsMobile:      in.IsMobile,
	})
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": res})
}

func channelSubscriptionUsage(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "订阅") {
		return
	}
	info, err := d.ChannelSvc.GetSubscriptionUsage(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": info})
}

func listChannelAPIKeys(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "上游 API Key 管理") {
		return
	}
	page, pageSize, err := parsePageQuery(c)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	res, err := d.ChannelSvc.ListAPIKeys(c.Request.Context(), id, connector.APIKeyQuery{
		Page:     page,
		PageSize: pageSize,
		Search:   c.Query("search"),
		Status:   c.Query("status"),
		GroupID:  c.Query("group_id"),
	})
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": res})
}

func listChannelAPIKeyGroups(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "上游 API Key 管理") {
		return
	}
	res, err := d.ChannelSvc.ListAPIKeyGroups(c.Request.Context(), id)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": res})
}

func createChannelAPIKey(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "上游 API Key 管理") {
		return
	}
	var in channelAPIKeyCreateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if strings.TrimSpace(in.Name) == "" {
		fail(c, http.StatusBadRequest, fmt.Errorf("密钥名称不能为空"))
		return
	}
	res, err := d.ChannelSvc.CreateAPIKey(c.Request.Context(), id, connector.APIKeyCreateRequest(in))
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": res})
}

func updateChannelAPIKey(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "上游 API Key 管理") {
		return
	}
	keyID, err := int64Param(c, "key_id")
	if err != nil || keyID <= 0 {
		fail(c, http.StatusBadRequest, fmt.Errorf("密钥 ID 无效"))
		return
	}
	var in channelAPIKeyUpdateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	res, err := d.ChannelSvc.UpdateAPIKey(c.Request.Context(), id, keyID, connector.APIKeyUpdateRequest(in))
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": res})
}

func deleteChannelAPIKey(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "上游 API Key 管理") {
		return
	}
	keyID, err := int64Param(c, "key_id")
	if err != nil || keyID <= 0 {
		fail(c, http.StatusBadRequest, fmt.Errorf("密钥 ID 无效"))
		return
	}
	if err := d.ChannelSvc.DeleteAPIKey(c.Request.Context(), id, keyID); err != nil {
		if errors.Is(err, channel.ErrDirectAPIKeyDeletionDisabled) {
			fail(c, http.StatusConflict, errors.New("为保护受管网关密钥，禁止从渠道密钥页直接删除；请通过上游分组变更流程处理"))
			return
		}
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func revealChannelAPIKey(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if rejectDirectChannelManagementOperation(c, d, id, "上游 API Key 管理") {
		return
	}
	keyID, err := int64Param(c, "key_id")
	if err != nil || keyID <= 0 {
		fail(c, http.StatusBadRequest, fmt.Errorf("密钥 ID 无效"))
		return
	}
	key, err := d.ChannelSvc.RevealAPIKey(c.Request.Context(), id, keyID)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"key": key}})
}

func channelRates(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	list, err := d.Rates.ListByChannel(id)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func balanceHistory(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	list, err := d.Rates.BalanceHistory(id, limit)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": list})
}

func uintParam(c *gin.Context, name string) (uint, error) {
	id, err := strconv.ParseUint(c.Param(name), 10, 64)
	return uint(id), err
}

func int64Param(c *gin.Context, name string) (int64, error) {
	return strconv.ParseInt(c.Param(name), 10, 64)
}

func parsePageQuery(c *gin.Context) (int, int, error) {
	page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
	if err != nil || page < 1 {
		return 0, 0, fmt.Errorf("page 必须是正整数")
	}
	pageSize, err := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if err != nil || pageSize < 1 {
		return 0, 0, fmt.Errorf("page_size 必须是正整数")
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize, nil
}

func parseChannelPageQuery(c *gin.Context) (int, int, error) {
	page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
	if err != nil || page < 1 {
		return 0, 0, fmt.Errorf("page 必须是正整数")
	}
	pageSize, err := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if err != nil || pageSize == 0 || pageSize < -1 {
		return 0, 0, fmt.Errorf("page_size 必须是正整数或 -1")
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize, nil
}

// setupSSE 给 ResponseWriter 设上 text/event-stream 头，返回一个就绪的 sseObserver。
// 调用方接下来一般是：
//
//	obs := setupSSE(c)
//	ctx := progress.WithObserver(c.Request.Context(), obs)
//	// ... 业务逻辑里的 progress.Start / OK / Fail 会被实时 stream 出去 ...
//	obs.Emit(progress.Event{Stage: progress.StageDone, Message: "完成"})
func setupSSE(c *gin.Context) *sseObserver {
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache, no-transform")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no") // disable nginx-style proxy buffering
	c.Writer.WriteHeader(http.StatusOK)

	obs := &sseObserver{w: c.Writer}
	if flusher, ok := c.Writer.(http.Flusher); ok {
		obs.flush = flusher.Flush
	}
	return obs
}

// sseObserver 把 progress.Event 序列化成 SSE 格式写入 ResponseWriter。
// 因为 gin 的 Handler 在一个 goroutine 中跑，而 emit 可能从下游同步 / 异步发起，
// 这里加锁保证 writer 串行写。
type sseObserver struct {
	mu     sync.Mutex
	w      io.Writer
	flush  func()
	closed bool
}

func (o *sseObserver) Emit(ev progress.Event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return
	}
	// SSE: "data: <json>\n\n"
	if _, err := io.WriteString(o.w, "data: "); err != nil {
		o.closed = true
		return
	}
	if _, err := o.w.Write(payload); err != nil {
		o.closed = true
		return
	}
	if _, err := io.WriteString(o.w, "\n\n"); err != nil {
		o.closed = true
		return
	}
	if o.flush != nil {
		o.flush()
	}
}

type channelScopedObserver struct {
	base        progress.Observer
	channelID   uint
	channelName string
	index       int
	total       int
}

func (o channelScopedObserver) Emit(ev progress.Event) {
	ev.ChannelID = o.channelID
	ev.ChannelName = o.channelName
	ev.Index = o.index
	ev.Total = o.total
	o.base.Emit(ev)
}

// syncChannel 通过 SSE 把整个同步过程的子步骤实时推给前端。
//
//	GET / POST /api/channels/:id/sync
//	响应 Content-Type: text/event-stream，每条事件形如
//	  data: {"stage":"login","message":"登录上游…","time":"..."}
//
// 前端用 fetch + ReadableStream 读取，按 "\n\n" 切片解析。
func syncChannel(c *gin.Context, d *Deps) {
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	ch, err := d.Channels.FindByID(id)
	if err != nil {
		fail(c, http.StatusNotFound, err)
		return
	}

	obs := setupSSE(c)
	ctx := progress.WithObserver(c.Request.Context(), channelScopedObserver{
		base:        obs,
		channelID:   ch.ID,
		channelName: ch.Name,
		index:       1,
		total:       1,
	})

	// 串行执行：先余额，再订阅告警，再倍率。任一步失败仍尝试下一个，但用 done 表示整体状态。
	//
	// 账务闸门（cost-unit / 上游账号身份证据不可信）只拒绝改动金额账本，不代表上游
	// 会话不可用；订阅额度与到期告警不含任何成本口径，必须照常执行，否则一次账务异常
	// 会连带静默掉不相关的告警。
	balErr := d.Monitor.RefreshBalance(ctx, ch)
	var subErr error
	if balErr == nil || costing.IsAccountingGate(balErr) {
		subErr = d.Monitor.CheckSubscriptionUsageAlerts(ctx, ch)
	}
	rateErr := d.Monitor.RefreshRates(ctx, ch)

	var syncWarnings []string
	if costing.IsAccountingGate(balErr) {
		syncWarnings = append(syncWarnings, balErr.Error())
		balErr = nil
	}
	if costing.IsAccountingGate(rateErr) {
		syncWarnings = append(syncWarnings, rateErr.Error())
		rateErr = nil
	}
	switch {
	case balErr != nil || subErr != nil || rateErr != nil:
		progress.Fail(ctx, progress.StageError, joinErrorMessages(balErr, subErr, rateErr))
	default:
		message := "同步完成"
		if len(syncWarnings) > 0 {
			message = "同步完成，账务采集待重试"
		}
		progress.OK(ctx, progress.StageDone, message, map[string]any{"warnings": syncWarnings})
	}
}

func syncAllChannels(c *gin.Context, d *Deps) {
	list, err := d.Channels.List()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	obs := setupSSE(c)
	baseCtx := c.Request.Context()
	total := len(list)
	var successCount, failedCount int

	if total == 0 {
		obs.Emit(progress.Event{
			Stage:   progress.StageDone,
			Message: "批量同步完成：成功 0，失败 0",
			Time:    time.Now(),
		})
		return
	}

	for i := range list {
		ch := list[i]
		scoped := channelScopedObserver{
			base:        obs,
			channelID:   ch.ID,
			channelName: ch.Name,
			index:       i + 1,
			total:       total,
		}
		ctx := progress.WithObserver(baseCtx, scoped)

		if err := d.Monitor.RefreshBalance(ctx, &ch); err != nil {
			// 见 syncChannel：账务闸门只暂停金额账本，不能连带跳过订阅告警。
			if !costing.IsAccountingGate(err) {
				failedCount++
				scoped.Emit(progress.Event{
					Stage:   progress.StageError,
					Message: fmt.Sprintf("同步失败：%v", err),
					Time:    time.Now(),
				})
				continue
			}
			progress.OK(ctx, progress.StageCost, fmt.Sprintf("账务同步已暂停，等待重试：%v", err), map[string]any{"accounting_warning": err.Error()})
		}
		if err := d.Monitor.CheckSubscriptionUsageAlerts(ctx, &ch); err != nil {
			failedCount++
			scoped.Emit(progress.Event{
				Stage:   progress.StageError,
				Message: fmt.Sprintf("订阅检查失败：%v", err),
				Time:    time.Now(),
			})
			continue
		}
		successCount++
		scoped.Emit(progress.Event{
			Stage:   progress.StageDone,
			Message: "同步完成",
			Time:    time.Now(),
		})
	}

	summary := fmt.Sprintf("批量同步完成：成功 %d，失败 %d", successCount, failedCount)
	stage := progress.StageDone
	if failedCount > 0 {
		stage = progress.StageError
	}
	obs.Emit(progress.Event{
		Stage:   stage,
		Message: summary,
		Time:    time.Now(),
	})
}

func joinErrorMessages(errs ...error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			parts = append(parts, err.Error())
		}
	}
	return strings.Join(parts, " | ")
}
