package api

import (
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/gin-gonic/gin"
)

var settingsConfigMu sync.RWMutex

type settingsConfigView struct {
	App           config.AppConfig           `json:"app"`
	Auth          settingsAuthConfigView     `json:"auth"`
	Scheduler     config.SchedulerConfig     `json:"scheduler"`
	Notifications config.NotificationsConfig `json:"notifications"`
	Proxy         settingsProxyConfigView    `json:"proxy"`
	Upstream      config.UpstreamConfig      `json:"upstream"`
}

type settingsConfigInput struct {
	App           config.AppConfig           `json:"app" binding:"required"`
	Auth          settingsAuthConfigInput    `json:"auth" binding:"required"`
	Scheduler     config.SchedulerConfig     `json:"scheduler" binding:"required"`
	Notifications config.NotificationsConfig `json:"notifications" binding:"required"`
	Proxy         settingsProxyConfigInput   `json:"proxy"`
	Upstream      config.UpstreamConfig      `json:"upstream"`
}

type settingsSystemConfigInput struct {
	App       *config.AppConfig         `json:"app"`
	Auth      *settingsAuthConfigInput  `json:"auth"`
	Scheduler *config.SchedulerConfig   `json:"scheduler"`
	Proxy     *settingsProxyConfigInput `json:"proxy"`
	Upstream  *config.UpstreamConfig    `json:"upstream"`
}

type settingsGatewayConfigInput struct {
	Scheduler *config.SchedulerConfig `json:"scheduler"`
}

type settingsNotificationsConfigInput struct {
	Notifications *config.NotificationsConfig `json:"notifications"`
}

type settingsAuthConfigView struct {
	Enabled               bool   `json:"enabled"`
	Username              string `json:"username"`
	Password              string `json:"password"`
	PasswordConfigured    bool   `json:"passwordConfigured"`
	TokenSecret           string `json:"tokenSecret"`
	TokenSecretConfigured bool   `json:"tokenSecretConfigured"`
	SessionTTLHours       int    `json:"sessionTTLHours"`
}

type settingsAuthConfigInput struct {
	Enabled          bool   `json:"enabled"`
	Username         string `json:"username"`
	Password         string `json:"password"`
	TokenSecret      string `json:"tokenSecret"`
	ClearTokenSecret bool   `json:"clearTokenSecret"`
	SessionTTLHours  int    `json:"sessionTTLHours"`
}

type settingsProxyConfigView struct {
	Enabled             bool   `json:"enabled"`
	VersionCheckEnabled bool   `json:"versionCheckEnabled"`
	Protocol            string `json:"protocol"`
	Host                string `json:"host"`
	Port                int    `json:"port"`
	Username            string `json:"username"`
	Password            string `json:"password"`
	PasswordConfigured  bool   `json:"passwordConfigured"`
}

type settingsProxyConfigInput struct {
	Enabled             bool   `json:"enabled"`
	VersionCheckEnabled bool   `json:"versionCheckEnabled"`
	Protocol            string `json:"protocol"`
	Host                string `json:"host"`
	Port                int    `json:"port"`
	Username            string `json:"username"`
	Password            string `json:"password"`
	ClearPassword       bool   `json:"clearPassword"`
}

func registerSettings(g *gin.RouterGroup, d *Deps) {
	gs := g.Group("/settings")
	gs.GET("/config", func(c *gin.Context) { getSettingsConfig(c, d) })
	gs.PUT("/config", func(c *gin.Context) { saveSettingsConfig(c, d) })
	gs.PATCH("/config/system", func(c *gin.Context) { saveSystemSettingsConfig(c, d) })
	gs.PATCH("/config/gateway", func(c *gin.Context) { saveGatewaySettingsConfig(c, d) })
	gs.PATCH("/config/notifications", func(c *gin.Context) { saveNotificationsSettingsConfig(c, d) })
	gs.POST("/apply", func(c *gin.Context) { applySettingsConfig(c, d) })
	gs.POST("/proxy/test", func(c *gin.Context) { testProxy(c, d) })
}

func getSettingsConfig(c *gin.Context, d *Deps) {
	settingsConfigMu.RLock()
	cfg, err := config.LoadFile(d.Runtime.ConfigPath())
	settingsConfigMu.RUnlock()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"config_path":         d.Runtime.ConfigPath(),
			"effective_time_zone": configuredUserTime(d).Name(),
			"config": settingsConfigView{
				App:           cfg.App,
				Auth:          settingsAuthView(cfg.Auth),
				Scheduler:     cfg.Scheduler,
				Notifications: cfg.Notifications,
				Proxy:         settingsProxyView(cfg.Proxy),
				Upstream:      cfg.Upstream,
			},
		},
	})
}

func saveSettingsConfig(c *gin.Context, d *Deps) {
	var in settingsConfigInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	timeZone, err := normalizeSettingsTimeZone(in.App.TimeZone)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	in.App.TimeZone = timeZone
	in.Scheduler.UnknownLatencyScoreFactor = config.NormalizeUnknownLatencyScoreFactor(in.Scheduler.UnknownLatencyScoreFactor)
	in.Scheduler.CacheInputPriceMultiplier = config.NormalizeCacheInputPriceMultiplier(in.Scheduler.CacheInputPriceMultiplier)
	in.Scheduler.CachePriceMinInputTokens = config.NormalizeCachePriceInputTokens(in.Scheduler.CachePriceMinInputTokens)
	in.Scheduler.CachePriceWindowInputTokens = config.NormalizeCachePriceInputTokens(in.Scheduler.CachePriceWindowInputTokens)
	in.Scheduler.CacheStatsMaxAgeDays = config.NormalizeCacheStatsMaxAgeDays(in.Scheduler.CacheStatsMaxAgeDays)
	in.Scheduler.RoutingFailures = config.NormalizeRoutingFailureConfig(in.Scheduler.RoutingFailures)
	if err := config.ValidateRoutingFailureConfig(in.Scheduler.RoutingFailures); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}

	mutateSettingsConfig(c, d, func(cfg *config.Config) {
		cfg.App.Title = in.App.Title
		cfg.App.NotificationPrefix = in.App.NotificationPrefix
		cfg.App.TimeZone = in.App.TimeZone
		cfg.Auth = in.Auth.merge(cfg.Auth)
		cfg.Scheduler = in.Scheduler
		cfg.Notifications = in.Notifications
		cfg.Proxy = in.Proxy.merge(cfg.Proxy)
		cfg.Upstream = in.Upstream.WithDefaults()
	})
}

func saveSystemSettingsConfig(c *gin.Context, d *Deps) {
	var in settingsSystemConfigInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.App == nil || in.Auth == nil || in.Scheduler == nil || in.Proxy == nil || in.Upstream == nil {
		fail(c, http.StatusBadRequest, errors.New("app, auth, scheduler, proxy, and provider HTTP settings are required"))
		return
	}
	timeZone, err := normalizeSettingsTimeZone(in.App.TimeZone)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	in.App.TimeZone = timeZone

	mutateSettingsConfig(c, d, func(cfg *config.Config) {
		cfg.App.Title = in.App.Title
		cfg.App.NotificationPrefix = in.App.NotificationPrefix
		cfg.App.TimeZone = in.App.TimeZone
		cfg.Auth = in.Auth.merge(cfg.Auth)
		cfg.Scheduler.BalanceCron = in.Scheduler.BalanceCron
		cfg.Scheduler.Concurrency = in.Scheduler.Concurrency
		cfg.Scheduler.CodexRadarRefreshIntervalMins = in.Scheduler.CodexRadarRefreshIntervalMins
		cfg.Scheduler.Retention = in.Scheduler.Retention
		cfg.Proxy = in.Proxy.merge(cfg.Proxy)
		cfg.Upstream = in.Upstream.WithDefaults()
	})
}

func saveGatewaySettingsConfig(c *gin.Context, d *Deps) {
	var in settingsGatewayConfigInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.Scheduler == nil {
		fail(c, http.StatusBadRequest, errors.New("scheduler is required"))
		return
	}
	in.Scheduler.RoutingFailures = config.NormalizeRoutingFailureConfig(in.Scheduler.RoutingFailures)
	if err := config.ValidateRoutingFailureConfig(in.Scheduler.RoutingFailures); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}

	mutateSettingsConfig(c, d, func(cfg *config.Config) {
		fundingDetectionThreshold := config.NormalizeFundingDetectionThreshold(in.Scheduler.FundingDetectionThreshold)
		cfg.Scheduler.RateRefreshIntervalMinutes = in.Scheduler.RateRefreshIntervalMinutes
		cfg.Scheduler.FundingDetectionThreshold = fundingDetectionThreshold
		cfg.Scheduler.SessionKeepAliveEnabled = in.Scheduler.SessionKeepAliveEnabled
		cfg.Scheduler.SessionKeepAliveIntervalMins = in.Scheduler.SessionKeepAliveIntervalMins
		cfg.Scheduler.LatencyProbeEnabled = in.Scheduler.LatencyProbeEnabled
		cfg.Scheduler.LatencyProbeIntervalSeconds = in.Scheduler.LatencyProbeIntervalSeconds
		cfg.Scheduler.LatencyProbeMaxCandidates = in.Scheduler.LatencyProbeMaxCandidates
		cfg.Scheduler.LatencyProbeTimeoutSeconds = in.Scheduler.LatencyProbeTimeoutSeconds
		cfg.Scheduler.LatencyProbeSampleSize = in.Scheduler.LatencyProbeSampleSize
		cfg.Scheduler.LatencyStatsWindowHours = in.Scheduler.LatencyStatsWindowHours
		cfg.Scheduler.LatencyProbeModelSyncMinutes = in.Scheduler.LatencyProbeModelSyncMinutes
		cfg.Scheduler.LatencyFullScoreMs = in.Scheduler.LatencyFullScoreMs
		cfg.Scheduler.LatencyZeroScoreMs = in.Scheduler.LatencyZeroScoreMs
		cfg.Scheduler.UnknownLatencyScoreFactor = config.NormalizeUnknownLatencyScoreFactor(in.Scheduler.UnknownLatencyScoreFactor)
		cfg.Scheduler.RoutingPriceScoreMax = in.Scheduler.RoutingPriceScoreMax
		cfg.Scheduler.RoutingLatencyScoreMax = in.Scheduler.RoutingLatencyScoreMax
		cfg.Scheduler.CacheInputPriceMultiplier = config.NormalizeCacheInputPriceMultiplier(in.Scheduler.CacheInputPriceMultiplier)
		cfg.Scheduler.CachePriceMinInputTokens = config.NormalizeCachePriceInputTokens(in.Scheduler.CachePriceMinInputTokens)
		cfg.Scheduler.CachePriceWindowInputTokens = config.NormalizeCachePriceInputTokens(in.Scheduler.CachePriceWindowInputTokens)
		cfg.Scheduler.CacheStatsMaxAgeDays = config.NormalizeCacheStatsMaxAgeDays(in.Scheduler.CacheStatsMaxAgeDays)
		cfg.Scheduler.RoutingFailures = in.Scheduler.RoutingFailures
		cfg.Scheduler.SessionAffinityMaxAgeMinutes = in.Scheduler.SessionAffinityMaxAgeMinutes
		cfg.Scheduler.SessionAffinityStableSamples = in.Scheduler.SessionAffinityStableSamples
	})
}

func saveNotificationsSettingsConfig(c *gin.Context, d *Deps) {
	var in settingsNotificationsConfigInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if in.Notifications == nil {
		fail(c, http.StatusBadRequest, errors.New("notifications is required"))
		return
	}

	mutateSettingsConfig(c, d, func(cfg *config.Config) {
		cfg.Notifications = *in.Notifications
	})
}

func mutateSettingsConfig(c *gin.Context, d *Deps, mutate func(*config.Config)) {
	path := d.Runtime.ConfigPath()
	settingsConfigMu.Lock()
	defer settingsConfigMu.Unlock()

	cfg, err := config.LoadFile(path)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	mutate(cfg)
	if err := config.Save(path, cfg); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"config_path": path,
			"message":     "已写入配置文件",
		},
	})
}

func normalizeSettingsTimeZone(raw string) (string, error) {
	timeZone := strings.TrimSpace(raw)
	if timeZone == "" {
		timeZone = "Asia/Shanghai"
	}
	if _, err := time.LoadLocation(timeZone); err != nil {
		return "", err
	}
	return timeZone, nil
}

func settingsAuthView(value config.AuthConfig) settingsAuthConfigView {
	return settingsAuthConfigView{
		Enabled:               value.Enabled,
		Username:              value.Username,
		PasswordConfigured:    value.Password != "",
		TokenSecretConfigured: value.TokenSecret != "",
		SessionTTLHours:       value.SessionTTLHours,
	}
}

func (in settingsAuthConfigInput) merge(current config.AuthConfig) config.AuthConfig {
	next := config.AuthConfig{
		Enabled:         in.Enabled,
		Username:        in.Username,
		Password:        current.Password,
		TokenSecret:     current.TokenSecret,
		SessionTTLHours: in.SessionTTLHours,
	}
	if in.Password != "" {
		next.Password = in.Password
	}
	if in.ClearTokenSecret {
		next.TokenSecret = ""
	} else if in.TokenSecret != "" {
		next.TokenSecret = in.TokenSecret
	}
	return next
}

func settingsProxyView(value config.ProxyConfig) settingsProxyConfigView {
	return settingsProxyConfigView{
		Enabled:             value.Enabled,
		VersionCheckEnabled: value.VersionCheckEnabled,
		Protocol:            value.Protocol,
		Host:                value.Host,
		Port:                value.Port,
		Username:            value.Username,
		PasswordConfigured:  value.Password != "",
	}
}

func (in settingsProxyConfigInput) merge(current config.ProxyConfig) config.ProxyConfig {
	next := config.ProxyConfig{
		Enabled:             in.Enabled,
		VersionCheckEnabled: in.VersionCheckEnabled,
		Protocol:            in.Protocol,
		Host:                in.Host,
		Port:                in.Port,
		Username:            in.Username,
		Password:            current.Password,
	}
	if in.ClearPassword {
		next.Password = ""
	} else if in.Password != "" {
		next.Password = in.Password
	}
	return next
}

func applySettingsConfig(c *gin.Context, d *Deps) {
	settingsConfigMu.RLock()
	result, err := d.Runtime.ApplyFromFile()
	settingsConfigMu.RUnlock()
	if err != nil {
		if errors.Is(err, runtimeconfig.ErrRestartRequired) {
			c.JSON(http.StatusConflict, gin.H{
				"error":                     err.Error(),
				"code":                      "restart_required",
				"restart_required_sections": runtimeconfig.RestartRequiredSections(err),
			})
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}
