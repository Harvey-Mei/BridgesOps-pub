package config

import (
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

type Config struct {
	App           AppConfig           `mapstructure:"app" yaml:"app" json:"app"`
	Server        ServerConfig        `mapstructure:"server" yaml:"server" json:"server"`
	GeoIP         GeoIPConfig         `mapstructure:"geoIP" yaml:"geoIP" json:"geoIP"`
	Database      DatabaseConfig      `mapstructure:"database" yaml:"database" json:"database"`
	Redis         RedisConfig         `mapstructure:"redis" yaml:"redis" json:"redis"`
	Security      SecurityConfig      `mapstructure:"security" yaml:"security" json:"security"`
	Auth          AuthConfig          `mapstructure:"auth" yaml:"auth" json:"auth"`
	Scheduler     SchedulerConfig     `mapstructure:"scheduler" yaml:"scheduler" json:"scheduler"`
	Notifications NotificationsConfig `mapstructure:"notifications" yaml:"notifications" json:"notifications"`
	Gateway       GatewayConfig       `mapstructure:"gateway" yaml:"gateway" json:"gateway"`
	Proxy         ProxyConfig         `mapstructure:"proxy" yaml:"proxy" json:"proxy"`
	Upstream      UpstreamConfig      `mapstructure:"upstream" yaml:"upstream" json:"upstream"`
	Log           LogConfig           `mapstructure:"log" yaml:"log" json:"log"`
}

type AppConfig struct {
	Title              string `mapstructure:"title" yaml:"title" json:"title"`
	NotificationPrefix string `mapstructure:"notificationPrefix" yaml:"notificationPrefix" json:"notificationPrefix"`
	TimeZone           string `mapstructure:"timeZone" yaml:"timeZone" json:"timeZone"`
}

type ServerConfig struct {
	Host           string   `mapstructure:"host" yaml:"host" json:"host"`
	Port           int      `mapstructure:"port" yaml:"port" json:"port"`
	Mode           string   `mapstructure:"mode" yaml:"mode" json:"mode"`
	TrustedProxies []string `mapstructure:"trustedProxies" yaml:"trustedProxies" json:"trustedProxies"`
	BaseURL        string   `mapstructure:"baseURL" yaml:"baseURL" json:"baseURL"`
}

// GeoIPConfig only accepts local MaxMind-compatible databases. The server
// never falls back to a public lookup service because request-log IPs must not
// leave this deployment during an administrator lookup.
type GeoIPConfig struct {
	Enabled          bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	CityDatabasePath string `mapstructure:"cityDatabasePath" yaml:"cityDatabasePath" json:"cityDatabasePath"`
	ASNDatabasePath  string `mapstructure:"asnDatabasePath" yaml:"asnDatabasePath" json:"asnDatabasePath"`
}

type DatabaseConfig struct {
	Driver       string `mapstructure:"driver" yaml:"driver" json:"driver"`
	Host         string `mapstructure:"host" yaml:"host" json:"host"`
	Port         int    `mapstructure:"port" yaml:"port" json:"port"`
	User         string `mapstructure:"user" yaml:"user" json:"user"`
	Password     string `mapstructure:"password" yaml:"password" json:"password"`
	Name         string `mapstructure:"name" yaml:"name" json:"name"`
	TimeZone     string `mapstructure:"timeZone" yaml:"timeZone" json:"timeZone"`
	SSLMode      string `mapstructure:"sslMode" yaml:"sslMode" json:"sslMode"`
	MaxOpenConns int    `mapstructure:"maxOpenConns" yaml:"maxOpenConns" json:"maxOpenConns"`
	MaxIdleConns int    `mapstructure:"maxIdleConns" yaml:"maxIdleConns" json:"maxIdleConns"`
}

func (d DatabaseConfig) ToStorageConfig() storage.DBConfig {
	return storage.DBConfig{
		Driver:       storage.DBDriver(d.Driver),
		Host:         d.Host,
		Port:         d.Port,
		User:         d.User,
		Password:     d.Password,
		Name:         d.Name,
		TimeZone:     d.TimeZone,
		SSLMode:      d.SSLMode,
		MaxOpenConns: d.MaxOpenConns,
		MaxIdleConns: d.MaxIdleConns,
	}
}

// RedisConfig configures the optional management analytics cache. PostgreSQL
// remains authoritative: Redis is never used for gateway routing, balances,
// key material, or financial reconciliation.
type RedisConfig struct {
	Enabled bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Host    string `mapstructure:"host" yaml:"host" json:"host"`
	Port    int    `mapstructure:"port" yaml:"port" json:"port"`
	// Namespace isolates disposable analytics entries when multiple BridgeOps
	// deployments share one Redis instance. An empty value is derived from the
	// deployment secret and database identity by cmd/server.
	Namespace  string `mapstructure:"namespace" yaml:"namespace" json:"namespace"`
	Username   string `mapstructure:"username" yaml:"username" json:"username"`
	Password   string `mapstructure:"password" yaml:"password" json:"password"`
	DB         int    `mapstructure:"db" yaml:"db" json:"db"`
	TLSEnabled bool   `mapstructure:"tlsEnabled" yaml:"tlsEnabled" json:"tlsEnabled"`
}

func (r RedisConfig) Address() (string, error) {
	host := strings.TrimSpace(r.Host)
	if host == "" {
		return "", fmt.Errorf("redis host is required")
	}
	if r.Port < 1 || r.Port > 65535 {
		return "", fmt.Errorf("redis port %d is invalid", r.Port)
	}
	if r.DB < 0 {
		return "", fmt.Errorf("redis db %d is invalid", r.DB)
	}
	return net.JoinHostPort(host, strconv.Itoa(r.Port)), nil
}

type SecurityConfig struct {
	// AppSecret 主密钥，用于 AES-GCM。优先从 APP_SECRET 环境变量读取。
	AppSecret string `mapstructure:"appSecret" yaml:"appSecret" json:"appSecret"`
}

// AuthConfig 后台单用户登录配置。
//
// Enabled = false（默认）时整套鉴权被关掉：/api/* 全部免 token，前端检测后跳过登录页。
// 适合纯内网 / 反代后面的部署。需要公网暴露时必须显式 Enabled=true 并设强密码。
//
// Enabled=true 时 Username/Password 是写死的管理员凭据，TokenSecret 用于签发 HMAC token。
// 如果 TokenSecret 为空，会回退使用 Security.AppSecret，保证有合理默认。
type AuthConfig struct {
	Enabled         bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Username        string `mapstructure:"username" yaml:"username" json:"username"`
	Password        string `mapstructure:"password" yaml:"password" json:"password"`
	TokenSecret     string `mapstructure:"tokenSecret" yaml:"tokenSecret" json:"tokenSecret"`
	SessionTTLHours int    `mapstructure:"sessionTTLHours" yaml:"sessionTTLHours" json:"sessionTTLHours"`
}

type SchedulerConfig struct {
	BalanceCron string `mapstructure:"balanceCron" yaml:"balanceCron" json:"balanceCron"`
	RateCron    string `mapstructure:"rateCron" yaml:"rateCron" json:"rateCron"`
	// FundingDetectionThreshold is the smallest observed upstream credit that
	// creates a pending funding reconciliation. Smaller balance changes still
	// update inventory snapshots, but are not treated as a recharge.
	FundingDetectionThreshold float64 `mapstructure:"fundingDetectionThreshold" yaml:"fundingDetectionThreshold" json:"fundingDetectionThreshold"`
	// RateRefreshIntervalMinutes controls the periodic rate snapshot refresh.
	// A value <= 0 keeps using the legacy RateCron expression for compatibility.
	RateRefreshIntervalMinutes    int                  `mapstructure:"rateRefreshIntervalMinutes" yaml:"rateRefreshIntervalMinutes" json:"rateRefreshIntervalMinutes"`
	Concurrency                   int                  `mapstructure:"concurrency" yaml:"concurrency" json:"concurrency"`
	SessionKeepAliveEnabled       bool                 `mapstructure:"sessionKeepAliveEnabled" yaml:"sessionKeepAliveEnabled" json:"sessionKeepAliveEnabled"`
	SessionKeepAliveIntervalMins  int                  `mapstructure:"sessionKeepAliveIntervalMinutes" yaml:"sessionKeepAliveIntervalMinutes" json:"sessionKeepAliveIntervalMinutes"`
	LatencyProbeEnabled           bool                 `mapstructure:"latencyProbeEnabled" yaml:"latencyProbeEnabled" json:"latencyProbeEnabled"`
	LatencyProbeIntervalSeconds   int                  `mapstructure:"latencyProbeIntervalSeconds" yaml:"latencyProbeIntervalSeconds" json:"latencyProbeIntervalSeconds"`
	LatencyProbeMaxCandidates     int                  `mapstructure:"latencyProbeMaxCandidates" yaml:"latencyProbeMaxCandidates" json:"latencyProbeMaxCandidates"`
	LatencyProbeTimeoutSeconds    int                  `mapstructure:"latencyProbeTimeoutSeconds" yaml:"latencyProbeTimeoutSeconds" json:"latencyProbeTimeoutSeconds"`
	LatencyProbeSampleSize        int                  `mapstructure:"latencyProbeSampleSize" yaml:"latencyProbeSampleSize" json:"latencyProbeSampleSize"`
	LatencyStatsWindowHours       int                  `mapstructure:"latencyStatsWindowHours" yaml:"latencyStatsWindowHours" json:"latencyStatsWindowHours"`
	LatencyProbeModelSyncMinutes  int                  `mapstructure:"latencyProbeModelSyncMinutes" yaml:"latencyProbeModelSyncMinutes" json:"latencyProbeModelSyncMinutes"`
	LatencyFullScoreMs            int                  `mapstructure:"latencyFullScoreMs" yaml:"latencyFullScoreMs" json:"latencyFullScoreMs"`
	LatencyZeroScoreMs            int                  `mapstructure:"latencyZeroScoreMs" yaml:"latencyZeroScoreMs" json:"latencyZeroScoreMs"`
	UnknownLatencyScoreFactor     float64              `mapstructure:"unknownLatencyScoreFactor" yaml:"unknownLatencyScoreFactor" json:"unknownLatencyScoreFactor"`
	RoutingPriceScoreMax          float64              `mapstructure:"routingPriceScoreMax" yaml:"routingPriceScoreMax" json:"routingPriceScoreMax"`
	RoutingLatencyScoreMax        float64              `mapstructure:"routingLatencyScoreMax" yaml:"routingLatencyScoreMax" json:"routingLatencyScoreMax"`
	CacheInputPriceMultiplier     float64              `mapstructure:"cacheInputPriceMultiplier" yaml:"cacheInputPriceMultiplier" json:"cacheInputPriceMultiplier"`
	CachePriceMinInputTokens      int64                `mapstructure:"cachePriceMinInputTokens" yaml:"cachePriceMinInputTokens" json:"cachePriceMinInputTokens"`
	CachePriceWindowInputTokens   int64                `mapstructure:"cachePriceWindowInputTokens" yaml:"cachePriceWindowInputTokens" json:"cachePriceWindowInputTokens"`
	CacheStatsMaxAgeDays          int                  `mapstructure:"cacheStatsMaxAgeDays" yaml:"cacheStatsMaxAgeDays" json:"cacheStatsMaxAgeDays"`
	RoutingFailures               RoutingFailureConfig `mapstructure:"routingFailures" yaml:"routingFailures" json:"routingFailures"`
	SessionAffinityMaxAgeMinutes  int                  `mapstructure:"sessionAffinityMaxAgeMinutes" yaml:"sessionAffinityMaxAgeMinutes" json:"sessionAffinityMaxAgeMinutes"`
	SessionAffinityStableSamples  int                  `mapstructure:"sessionAffinityStableSamples" yaml:"sessionAffinityStableSamples" json:"sessionAffinityStableSamples"`
	CodexRadarRefreshIntervalMins int                  `mapstructure:"codexRadarRefreshIntervalMinutes" yaml:"codexRadarRefreshIntervalMinutes" json:"codexRadarRefreshIntervalMinutes"`
	ModelCheck                    ModelCheckConfig     `mapstructure:"modelCheck" yaml:"modelCheck" json:"modelCheck"`
	Retention                     RetentionConfig      `mapstructure:"retention" yaml:"retention" json:"retention"`
}

const MinimumFundingDetectionThreshold = 0.99
const DefaultUnknownLatencyScoreFactor = 0.8
const DefaultCacheInputPriceMultiplier = 1
const DefaultCachePriceInputTokens int64 = 100_000_000
const DefaultCacheStatsMaxAgeDays = 7
const MaximumCachePriceInputTokens int64 = 1_000_000_000_000
const MaximumCacheStatsMaxAgeDays = 365
const DefaultManualRoutingCooldownMinutes = 10
const MinimumManualRoutingCooldownMinutes = 1
const MaximumManualRoutingCooldownMinutes = 24 * 60

// NormalizeFundingDetectionThreshold keeps the reconciliation trigger above
// the amount where upstream rounding noise commonly appears. Callers may make
// it stricter, but configuration can never relax this safety floor.
func NormalizeFundingDetectionThreshold(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < MinimumFundingDetectionThreshold {
		return MinimumFundingDetectionThreshold
	}
	return value
}

// NormalizeUnknownLatencyScoreFactor keeps candidates with insufficient
// latency history from being unfairly penalized while rejecting an unusable
// zero or non-finite configuration value.
func NormalizeUnknownLatencyScoreFactor(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
		return DefaultUnknownLatencyScoreFactor
	}
	if value > 1 {
		return 1
	}
	return value
}

func NormalizeCacheInputPriceMultiplier(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return DefaultCacheInputPriceMultiplier
	}
	return value
}

func NormalizeCachePriceInputTokens(value int64) int64 {
	if value <= 0 || value > MaximumCachePriceInputTokens {
		return DefaultCachePriceInputTokens
	}
	return value
}

func NormalizeCacheStatsMaxAgeDays(value int) int {
	if value <= 0 || value > MaximumCacheStatsMaxAgeDays {
		return DefaultCacheStatsMaxAgeDays
	}
	return value
}

// ModelCheckConfig controls the local model-integrity probes. The request
// budget is a rolling 24-hour cap so changing the display timezone cannot
// accidentally reset paid probe traffic twice.
type ModelCheckConfig struct {
	Enabled               bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	RunsPerDay            int  `mapstructure:"runsPerDay" yaml:"runsPerDay" json:"runsPerDay"`
	Concurrency           int  `mapstructure:"concurrency" yaml:"concurrency" json:"concurrency"`
	MaxTargetsPerRun      int  `mapstructure:"maxTargetsPerRun" yaml:"maxTargetsPerRun" json:"maxTargetsPerRun"`
	ProbesPerTarget       int  `mapstructure:"probesPerTarget" yaml:"probesPerTarget" json:"probesPerTarget"`
	RequestBudget24h      int  `mapstructure:"requestBudget24h" yaml:"requestBudget24h" json:"requestBudget24h"`
	TimeoutSeconds        int  `mapstructure:"timeoutSeconds" yaml:"timeoutSeconds" json:"timeoutSeconds"`
	PelicanTimeoutSeconds int  `mapstructure:"pelicanTimeoutSeconds" yaml:"pelicanTimeoutSeconds" json:"pelicanTimeoutSeconds"`
	AlertFailures         int  `mapstructure:"alertFailures" yaml:"alertFailures" json:"alertFailures"`
	HistoryDays           int  `mapstructure:"historyDays" yaml:"historyDays" json:"historyDays"`
}

// RoutingFailureConfig keeps business-defined gateway failures together so
// future routing health rules do not need independent configuration paths.
type RoutingFailureConfig struct {
	ResponseTimeoutSeconds int `mapstructure:"responseTimeoutSeconds" yaml:"responseTimeoutSeconds" json:"responseTimeoutSeconds"`
	ManualCooldownMinutes  int `mapstructure:"manualCooldownMinutes" yaml:"manualCooldownMinutes" json:"manualCooldownMinutes"`
}

func ValidateRoutingFailureConfig(value RoutingFailureConfig) error {
	if value.ManualCooldownMinutes < MinimumManualRoutingCooldownMinutes || value.ManualCooldownMinutes > MaximumManualRoutingCooldownMinutes {
		return fmt.Errorf("manual routing cooldown must be between %d and %d minutes", MinimumManualRoutingCooldownMinutes, MaximumManualRoutingCooldownMinutes)
	}
	return nil
}

func NormalizeRoutingFailureConfig(value RoutingFailureConfig) RoutingFailureConfig {
	if value.ManualCooldownMinutes == 0 {
		value.ManualCooldownMinutes = DefaultManualRoutingCooldownMinutes
	}
	return value
}

// EffectiveRateCron returns the schedule used by the rate snapshot worker.
// The interval is deliberately represented as minutes in the UI while the
// scheduler continues to use its existing six-field, seconds-aware cron.
// A non-positive interval preserves configurations that still rely on a
// custom legacy RateCron expression.
func (c SchedulerConfig) EffectiveRateCron() string {
	if c.RateRefreshIntervalMinutes <= 0 {
		return c.RateCron
	}
	if c.RateRefreshIntervalMinutes == 1 {
		return "13 * * * * *"
	}
	return fmt.Sprintf("13 */%d * * * *", c.RateRefreshIntervalMinutes)
}

// RetentionConfig 历史数据保留策略。
//
// 字段为 0 表示该表不清理，永久保留（默认 rate_change_logs 永远保留，是核心业务数据）。
// Cron 为空时不启动清理任务。
type RetentionConfig struct {
	Cron                 string `mapstructure:"cron" yaml:"cron" json:"cron"`
	MonitorLogsDays      int    `mapstructure:"monitorLogsDays" yaml:"monitorLogsDays" json:"monitorLogsDays"`
	BalanceSnapshotsDays int    `mapstructure:"balanceSnapshotsDays" yaml:"balanceSnapshotsDays" json:"balanceSnapshotsDays"`
	NotificationLogsDays int    `mapstructure:"notificationLogsDays" yaml:"notificationLogsDays" json:"notificationLogsDays"`
	AnnouncementsDays    int    `mapstructure:"announcementsDays" yaml:"announcementsDays" json:"announcementsDays"`
}

// NotificationsConfig 通知去抖策略。所有字段都是"少烦我"取向，默认不丢消息只合并。
//
//   - BatchRateChanges：同次扫描中将多个分组的变化合并成 1 条消息，避免上游一次大调价
//     瞬间发出 30+ 条通知刷屏。默认 true。
//   - MinChangePct：涨跌幅 < X% 的 rate_changed 跳过推送（仍会写入 rate_change_logs）。
//     0 = 全发，对应原始行为。
//   - BalanceLowCooldownMinutes：同一渠道的 balance_low 在 X 分钟内不重复推送。
//     0 = 不冷却（每次扫描发现仍 < 阈值都发）。冷却状态持久化在数据库的
//     notification_cooldowns 表，跨重启生效。
//   - SendMaxAttempts：单条通知发送失败时最多尝试次数（含首次）。
//     1 = 不重试。重试采用指数退避：1s / 2s / 4s …，上限 30s。
type NotificationsConfig struct {
	BatchRateChanges                         bool    `mapstructure:"batchRateChanges" yaml:"batchRateChanges" json:"batchRateChanges"`
	MinChangePct                             float64 `mapstructure:"minChangePct" yaml:"minChangePct" json:"minChangePct"`
	BalanceLowCooldownMinutes                int     `mapstructure:"balanceLowCooldownMinutes" yaml:"balanceLowCooldownMinutes" json:"balanceLowCooldownMinutes"`
	SubscriptionDailyRemainingThresholdPct   float64 `mapstructure:"subscriptionDailyRemainingThresholdPct" yaml:"subscriptionDailyRemainingThresholdPct" json:"subscriptionDailyRemainingThresholdPct"`
	SubscriptionWeeklyRemainingThresholdPct  float64 `mapstructure:"subscriptionWeeklyRemainingThresholdPct" yaml:"subscriptionWeeklyRemainingThresholdPct" json:"subscriptionWeeklyRemainingThresholdPct"`
	SubscriptionMonthlyRemainingThresholdPct float64 `mapstructure:"subscriptionMonthlyRemainingThresholdPct" yaml:"subscriptionMonthlyRemainingThresholdPct" json:"subscriptionMonthlyRemainingThresholdPct"`
	SubscriptionExpiryThresholdHours         int     `mapstructure:"subscriptionExpiryThresholdHours" yaml:"subscriptionExpiryThresholdHours" json:"subscriptionExpiryThresholdHours"`
	SubscriptionAlertCooldownMinutes         int     `mapstructure:"subscriptionAlertCooldownMinutes" yaml:"subscriptionAlertCooldownMinutes" json:"subscriptionAlertCooldownMinutes"`
	SendMaxAttempts                          int     `mapstructure:"sendMaxAttempts" yaml:"sendMaxAttempts" json:"sendMaxAttempts"`
}

type ProxyConfig struct {
	Enabled             bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	VersionCheckEnabled bool   `mapstructure:"versionCheckEnabled" yaml:"versionCheckEnabled" json:"versionCheckEnabled"`
	Protocol            string `mapstructure:"protocol" yaml:"protocol" json:"protocol"`
	Host                string `mapstructure:"host" yaml:"host" json:"host"`
	Port                int    `mapstructure:"port" yaml:"port" json:"port"`
	Username            string `mapstructure:"username" yaml:"username" json:"username"`
	Password            string `mapstructure:"password" yaml:"password" json:"password"`
}

const (
	DefaultUpstreamTimeoutSeconds = 30
	DefaultUpstreamUserAgent      = "bridgeops/0.1"
)

type UpstreamConfig struct {
	TimeoutSeconds int    `mapstructure:"timeoutSeconds" yaml:"timeoutSeconds" json:"timeoutSeconds"`
	UserAgent      string `mapstructure:"userAgent" yaml:"userAgent" json:"userAgent"`
}

func (u UpstreamConfig) WithDefaults() UpstreamConfig {
	if u.TimeoutSeconds <= 0 {
		u.TimeoutSeconds = DefaultUpstreamTimeoutSeconds
	}
	if strings.TrimSpace(u.UserAgent) == "" {
		u.UserAgent = DefaultUpstreamUserAgent
	}
	return u
}

type LogConfig struct {
	Level  string `mapstructure:"level" yaml:"level" json:"level"`
	Format string `mapstructure:"format" yaml:"format" json:"format"`
}

// Load 读取 config.yaml（可选）+ APP_SECRET / * 环境变量覆盖。
//
// 关键映射：
//
//	APP_SECRET                       -> security.appSecret
//	DATABASE_DRIVER       -> database.driver
//	DATABASE_HOST         -> database.host
//	DATABASE_TIMEZONE     -> database.timeZone
//	REDIS_HOST            -> redis.host
//	REDIS_PORT            -> redis.port
//	SERVER_PORT           -> server.port
//	SCHEDULER_BALANCECRON-> scheduler.balanceCron
func Load(path string) (*Config, error) {
	cfg, _, err := load(path, true)
	return cfg, err
}

func LoadWithPath(path string) (*Config, string, error) {
	if strings.TrimSpace(path) != "" {
		info, err := os.Stat(path)
		if err != nil {
			return nil, "", fmt.Errorf("stat requested config: %w", err)
		}
		if !info.Mode().IsRegular() {
			return nil, "", fmt.Errorf("requested config is not a regular file")
		}
	}
	return load(path, true)
}

func LoadFile(path string) (*Config, error) {
	cfg, _, err := load(path, false)
	return cfg, err
}

func load(path string, withEnv bool) (*Config, string, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("config")
		for _, p := range configSearchPaths() {
			v.AddConfigPath(p)
		}
		v.AddConfigPath("/etc/bridgeops")
	}

	setDefaults(v)

	if withEnv {
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		v.AutomaticEnv()
		// APP_SECRET / ADMIN_USERNAME / ADMIN_PASSWORD / AUTH_ENABLED 是独立约定的环境变量名，不带前缀。
		_ = v.BindEnv("security.appSecret", "APP_SECRET")
		_ = v.BindEnv("auth.enabled", "AUTH_ENABLED")
		_ = v.BindEnv("auth.username", "ADMIN_USERNAME")
		_ = v.BindEnv("auth.password", "ADMIN_PASSWORD")
		_ = v.BindEnv("auth.tokenSecret", "AUTH_TOKEN_SECRET")
		// Viper 坑：AutomaticEnv 只对已通过 SetDefault / BindEnv / 配置文件注册过的 key 生效；
		// 数据库的 user/password 没有合理的默认值（拒绝写"change-me"作默认），
		// 因此显式 BindEnv 以确保从环境变量读取。
		_ = v.BindEnv("database.driver", "DATABASE_DRIVER")
		_ = v.BindEnv("database.host", "DATABASE_HOST")
		_ = v.BindEnv("database.port", "DATABASE_PORT")
		_ = v.BindEnv("database.user", "DATABASE_USER")
		_ = v.BindEnv("database.password", "DATABASE_PASSWORD")
		_ = v.BindEnv("database.name", "DATABASE_NAME")
		_ = v.BindEnv("database.timeZone", "DATABASE_TIMEZONE", "DATABASE_TIME_ZONE")
		_ = v.BindEnv("database.sslMode", "DATABASE_SSLMODE", "DATABASE_SSL_MODE")
		_ = v.BindEnv("redis.enabled", "REDIS_ENABLED")
		_ = v.BindEnv("redis.host", "REDIS_HOST")
		_ = v.BindEnv("redis.port", "REDIS_PORT")
		_ = v.BindEnv("redis.namespace", "REDIS_NAMESPACE")
		_ = v.BindEnv("redis.username", "REDIS_USERNAME")
		_ = v.BindEnv("redis.password", "REDIS_PASSWORD")
		_ = v.BindEnv("redis.db", "REDIS_DB")
		_ = v.BindEnv("redis.tlsEnabled", "REDIS_TLS_ENABLED")
		_ = v.BindEnv("server.port", "SERVER_PORT")
		_ = v.BindEnv("server.host", "SERVER_HOST")
		_ = v.BindEnv("server.mode", "SERVER_MODE")
		_ = v.BindEnv("geoIP.enabled", "GEOIP_ENABLED")
		_ = v.BindEnv("geoIP.cityDatabasePath", "GEOIP_CITY_DATABASE_PATH")
		_ = v.BindEnv("geoIP.asnDatabasePath", "GEOIP_ASN_DATABASE_PATH")
		_ = v.BindEnv("log.level", "LOG_LEVEL")
	}

	if err := v.ReadInConfig(); err != nil {
		if path != "" {
			if !os.IsNotExist(err) {
				return nil, "", fmt.Errorf("read config: %w", err)
			}
		} else {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, "", fmt.Errorf("read config: %w", err)
			}
		}
	}
	// Defaults are applied only when the text-config field is absent. Reject a
	// zero supplied by either the config file or its environment override before
	// unmarshalling, because Viper otherwise makes both look like the default
	// once they reach the Go value. Queue wait has no disabled-zero mode.
	const maxQueueWaitKey = "gateway.requestAdmission.maxQueueWaitMilliseconds"
	if v.GetInt64(maxQueueWaitKey) == 0 {
		return nil, "", fmt.Errorf(
			"gateway request admission max queue wait milliseconds must be between %d and %d; omit it to use the default",
			MinimumGatewayRequestAdmissionQueueWaitMilliseconds,
			MaximumGatewayRequestAdmissionQueueWaitMilliseconds,
		)
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, "", fmt.Errorf("unmarshal config: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(cfg.Database.Driver), string(storage.DBDriverPostgres)) {
		return nil, "", fmt.Errorf("database.driver must be postgres")
	}
	cfg.Upstream = cfg.Upstream.WithDefaults()
	cfg.Scheduler.FundingDetectionThreshold = NormalizeFundingDetectionThreshold(cfg.Scheduler.FundingDetectionThreshold)
	cfg.Scheduler.UnknownLatencyScoreFactor = NormalizeUnknownLatencyScoreFactor(cfg.Scheduler.UnknownLatencyScoreFactor)
	cfg.Scheduler.CacheInputPriceMultiplier = NormalizeCacheInputPriceMultiplier(cfg.Scheduler.CacheInputPriceMultiplier)
	cfg.Scheduler.CachePriceMinInputTokens = NormalizeCachePriceInputTokens(cfg.Scheduler.CachePriceMinInputTokens)
	cfg.Scheduler.CachePriceWindowInputTokens = NormalizeCachePriceInputTokens(cfg.Scheduler.CachePriceWindowInputTokens)
	cfg.Scheduler.CacheStatsMaxAgeDays = NormalizeCacheStatsMaxAgeDays(cfg.Scheduler.CacheStatsMaxAgeDays)
	cfg.Scheduler.RoutingFailures = NormalizeRoutingFailureConfig(cfg.Scheduler.RoutingFailures)
	if err := ValidateRoutingFailureConfig(cfg.Scheduler.RoutingFailures); err != nil {
		return nil, "", err
	}
	cfg.Gateway.RequestAdmission = NormalizeGatewayRequestAdmissionConfig(cfg.Gateway.RequestAdmission)
	if err := ValidateGatewayRequestAdmissionConfig(cfg.Gateway.RequestAdmission); err != nil {
		return nil, "", err
	}
	return cfg, v.ConfigFileUsed(), nil
}

func Save(path string, cfg *Config) error {
	if path == "" {
		return fmt.Errorf("config path is empty")
	}
	targetPath := path
	if info, err := os.Lstat(path); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat config path: %w", err)
		}
	} else if info.Mode()&os.ModeSymlink != 0 {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return fmt.Errorf("resolve config symlink: %w", err)
		}
		targetPath = resolved
	}

	if _, err := os.Stat(targetPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("stat config target: %w", err)
	}
	mode := os.FileMode(0o600)

	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir config dir: %w", err)
	}
	// Persist the normalized value as well, so a manually supplied value below
	// the hard floor cannot reappear after the next reload.
	normalized := *cfg
	if strings.TrimSpace(normalized.Database.Driver) == "" {
		normalized.Database.Driver = string(storage.DBDriverPostgres)
	}
	normalized.Scheduler.FundingDetectionThreshold = NormalizeFundingDetectionThreshold(normalized.Scheduler.FundingDetectionThreshold)
	normalized.Scheduler.UnknownLatencyScoreFactor = NormalizeUnknownLatencyScoreFactor(normalized.Scheduler.UnknownLatencyScoreFactor)
	normalized.Scheduler.CacheInputPriceMultiplier = NormalizeCacheInputPriceMultiplier(normalized.Scheduler.CacheInputPriceMultiplier)
	normalized.Scheduler.CachePriceMinInputTokens = NormalizeCachePriceInputTokens(normalized.Scheduler.CachePriceMinInputTokens)
	normalized.Scheduler.CachePriceWindowInputTokens = NormalizeCachePriceInputTokens(normalized.Scheduler.CachePriceWindowInputTokens)
	normalized.Scheduler.CacheStatsMaxAgeDays = NormalizeCacheStatsMaxAgeDays(normalized.Scheduler.CacheStatsMaxAgeDays)
	normalized.Scheduler.RoutingFailures = NormalizeRoutingFailureConfig(normalized.Scheduler.RoutingFailures)
	if err := ValidateRoutingFailureConfig(normalized.Scheduler.RoutingFailures); err != nil {
		return err
	}
	// A Go value has already crossed the omission boundary. In particular, do
	// not turn MaxQueueWaitMilliseconds: 0 into the default here: validation
	// must fail closed rather than persist a policy the caller did not provide.
	normalized.Gateway.RequestAdmission = NormalizeGatewayRequestAdmissionConfig(normalized.Gateway.RequestAdmission)
	if err := ValidateGatewayRequestAdmissionConfig(normalized.Gateway.RequestAdmission); err != nil {
		return err
	}
	body, err := yaml.Marshal(&normalized)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(targetPath)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary config: %w", err)
	}
	tmpPath := tmp.Name()
	keepTemp := true
	defer func() {
		if tmp != nil {
			_ = tmp.Close()
		}
		if keepTemp {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		return fmt.Errorf("chmod temporary config: %w", err)
	}
	if _, err := tmp.Write(body); err != nil {
		return fmt.Errorf("write temporary config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary config: %w", err)
	}
	tmp = nil
	if err := os.Rename(tmpPath, targetPath); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	keepTemp = false
	return nil
}

func ResolvePath(requested, used string) string {
	if requested != "" {
		return requested
	}
	if used != "" {
		return used
	}
	for _, candidate := range configSearchPaths() {
		candidate = filepath.Join(candidate, "config.yaml")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	if wd, err := os.Getwd(); err == nil && filepath.Base(wd) == "backend" {
		return "../config.yaml"
	}
	return "config.yaml"
}

func configSearchPaths() []string {
	if wd, err := os.Getwd(); err == nil && filepath.Base(wd) == "backend" {
		return []string{"..", "."}
	}
	return []string{"."}
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.title", "BridgeOps")
	v.SetDefault("app.notificationPrefix", "[AI 聚合监控] ")
	v.SetDefault("app.timeZone", "Asia/Shanghai")

	v.SetDefault("server.port", 8418)
	v.SetDefault("server.host", "")
	v.SetDefault("server.mode", "debug")
	v.SetDefault("server.trustedProxies", []string{"127.0.0.1", "::1"})
	v.SetDefault("server.baseURL", "http://localhost:8418")

	// Local-only IP geolocation is opt-in. The database paths are intentionally
	// empty by default and are never exposed through the HTTP API.
	v.SetDefault("geoIP.enabled", false)
	v.SetDefault("geoIP.cityDatabasePath", "")
	v.SetDefault("geoIP.asnDatabasePath", "")

	v.SetDefault("database.driver", "postgres")
	v.SetDefault("database.host", "localhost")
	// A zero port uses PostgreSQL's native 5432 default.
	v.SetDefault("database.port", 0)
	v.SetDefault("database.name", "bridgeops")
	v.SetDefault("database.maxOpenConns", 20)
	v.SetDefault("database.maxIdleConns", 5)

	// Redis is intentionally opt-in. It only accelerates management analytics;
	// a missing or unavailable Redis instance must never block the gateway.
	v.SetDefault("redis.enabled", false)
	v.SetDefault("redis.host", "127.0.0.1")
	v.SetDefault("redis.port", 6379)
	v.SetDefault("redis.namespace", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.tlsEnabled", false)

	// 余额每 15 分钟扫描；倍率变化更直接影响路由成本，默认每分钟同步。
	v.SetDefault("scheduler.balanceCron", "37 */15 * * * *")
	v.SetDefault("scheduler.rateCron", "13 * * * * *")
	v.SetDefault("scheduler.fundingDetectionThreshold", MinimumFundingDetectionThreshold)
	v.SetDefault("scheduler.concurrency", 4)
	v.SetDefault("scheduler.sessionKeepAliveEnabled", true)
	v.SetDefault("scheduler.sessionKeepAliveIntervalMinutes", 10)
	v.SetDefault("scheduler.latencyProbeEnabled", true)
	v.SetDefault("scheduler.latencyProbeIntervalSeconds", 60)
	v.SetDefault("scheduler.latencyProbeMaxCandidates", 5)
	v.SetDefault("scheduler.latencyProbeTimeoutSeconds", 30)
	v.SetDefault("scheduler.latencyProbeSampleSize", 10)
	v.SetDefault("scheduler.latencyStatsWindowHours", 168)
	v.SetDefault("scheduler.latencyProbeModelSyncMinutes", 60)
	v.SetDefault("scheduler.latencyFullScoreMs", 10000)
	v.SetDefault("scheduler.latencyZeroScoreMs", 80000)
	v.SetDefault("scheduler.unknownLatencyScoreFactor", DefaultUnknownLatencyScoreFactor)
	v.SetDefault("scheduler.routingPriceScoreMax", 70)
	v.SetDefault("scheduler.routingLatencyScoreMax", 30)
	v.SetDefault("scheduler.cacheInputPriceMultiplier", DefaultCacheInputPriceMultiplier)
	v.SetDefault("scheduler.cachePriceMinInputTokens", DefaultCachePriceInputTokens)
	v.SetDefault("scheduler.cachePriceWindowInputTokens", DefaultCachePriceInputTokens)
	v.SetDefault("scheduler.cacheStatsMaxAgeDays", DefaultCacheStatsMaxAgeDays)
	v.SetDefault("scheduler.routingFailures.responseTimeoutSeconds", 30)
	v.SetDefault("scheduler.routingFailures.manualCooldownMinutes", DefaultManualRoutingCooldownMinutes)
	v.SetDefault("scheduler.sessionAffinityMaxAgeMinutes", 10)
	v.SetDefault("scheduler.sessionAffinityStableSamples", 10)
	v.SetDefault("scheduler.codexRadarRefreshIntervalMinutes", 15)
	v.SetDefault("scheduler.modelCheck.enabled", false)
	v.SetDefault("scheduler.modelCheck.runsPerDay", 3)
	v.SetDefault("scheduler.modelCheck.concurrency", 2)
	v.SetDefault("scheduler.modelCheck.maxTargetsPerRun", 12)
	v.SetDefault("scheduler.modelCheck.probesPerTarget", 2)
	v.SetDefault("scheduler.modelCheck.requestBudget24h", 72)
	v.SetDefault("scheduler.modelCheck.timeoutSeconds", 60)
	v.SetDefault("scheduler.modelCheck.pelicanTimeoutSeconds", 600)
	v.SetDefault("scheduler.modelCheck.alertFailures", 2)
	v.SetDefault("scheduler.modelCheck.historyDays", 90)

	requestAdmission := DefaultGatewayRequestAdmissionConfig()
	v.SetDefault("gateway.requestAdmission.maxInFlightRequests", requestAdmission.MaxInFlightRequests)
	v.SetDefault("gateway.requestAdmission.maxInFlightBodyBytes", requestAdmission.MaxInFlightBodyBytes)
	v.SetDefault("gateway.requestAdmission.maxInFlightRequestsPerKey", requestAdmission.MaxInFlightRequestsPerKey)
	v.SetDefault("gateway.requestAdmission.maxInFlightBodyBytesPerKey", requestAdmission.MaxInFlightBodyBytesPerKey)
	v.SetDefault("gateway.requestAdmission.maxQueuedRequests", requestAdmission.MaxQueuedRequests)
	v.SetDefault("gateway.requestAdmission.maxQueuedRequestsPerKey", requestAdmission.MaxQueuedRequestsPerKey)
	v.SetDefault("gateway.requestAdmission.maxQueueWaitMilliseconds", requestAdmission.MaxQueueWaitMilliseconds)

	// 历史清理：每天凌晨 3:17 跑一次（6 字段 cron 含秒），
	// monitor 30 天 / balance 90 天 / notify 90 天。rate_change_logs 不清理（业务核心数据）。
	v.SetDefault("scheduler.retention.cron", "0 17 3 * * *")
	v.SetDefault("scheduler.retention.monitorLogsDays", 30)
	v.SetDefault("scheduler.retention.balanceSnapshotsDays", 90)
	v.SetDefault("scheduler.retention.notificationLogsDays", 90)
	v.SetDefault("scheduler.retention.announcementsDays", 90)

	v.SetDefault("auth.enabled", false)
	v.SetDefault("auth.username", "admin")
	v.SetDefault("auth.sessionTTLHours", 168) // 7 天

	// 通知去抖：默认开合并、不过滤涨跌幅、balance_low 1h 内不重复、失败重试 3 次。
	// 即"默认行为是合并刷屏 + 不重复 balance_low + 抗短时网络抖动"，不丢任何 rate_changed 事件。
	v.SetDefault("notifications.batchRateChanges", true)
	v.SetDefault("notifications.minChangePct", 0)
	v.SetDefault("notifications.balanceLowCooldownMinutes", 60)
	v.SetDefault("notifications.subscriptionDailyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionWeeklyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionMonthlyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionExpiryThresholdHours", 0)
	v.SetDefault("notifications.subscriptionAlertCooldownMinutes", 1440)
	v.SetDefault("notifications.sendMaxAttempts", 3)

	v.SetDefault("proxy.protocol", "http")
	v.SetDefault("proxy.port", 0)
	v.SetDefault("proxy.enabled", false)
	v.SetDefault("proxy.versionCheckEnabled", false)

	v.SetDefault("upstream.timeoutSeconds", DefaultUpstreamTimeoutSeconds)
	v.SetDefault("upstream.userAgent", DefaultUpstreamUserAgent)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")
}
