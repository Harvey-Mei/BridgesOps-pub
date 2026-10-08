package runtimeconfig

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/auth"
	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/scheduler"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"github.com/gin-gonic/gin"
)

type SchedulerFactory func(config.SchedulerConfig, config.ProxyConfig) *scheduler.Scheduler

var ErrRestartRequired = errors.New("runtime configuration change requires restart")

const GatewayRequestAdmissionSection = "gateway.requestAdmission"

// RestartRequiredError identifies startup-only sections that differ from the
// process's active configuration. Callers may expose the section names, but
// must not partially apply the rest of the file after this error.
type RestartRequiredError struct {
	Sections []string
}

func (e *RestartRequiredError) Error() string {
	if e == nil || len(e.Sections) == 0 {
		return ErrRestartRequired.Error()
	}
	return fmt.Sprintf("%v: %s", ErrRestartRequired, strings.Join(e.Sections, ", "))
}

func (e *RestartRequiredError) Unwrap() error {
	return ErrRestartRequired
}

func RestartRequiredSections(err error) []string {
	var restartRequired *RestartRequiredError
	if !errors.As(err, &restartRequired) || restartRequired == nil {
		return nil
	}
	return append([]string(nil), restartRequired.Sections...)
}

type Manager struct {
	mu               sync.RWMutex
	configPath       string
	securitySecret   string
	log              *slog.Logger
	dispatcher       *notify.Dispatcher
	channelSvc       *channel.Service
	schedulerFactory SchedulerFactory
	auth             *auth.Service
	scheduler        *scheduler.Scheduler
	proxyConfig      config.ProxyConfig
	upstreamConfig   config.UpstreamConfig
	userTime         *usertime.Service

	startupGatewayRequestAdmission    config.GatewayRequestAdmissionConfig
	startupGatewayRequestAdmissionSet bool
}

type ApplyResult struct {
	AppliedSections []string `json:"applied_sections"`
	Message         string   `json:"message"`
}

func New(
	configPath string,
	securitySecret string,
	log *slog.Logger,
	dispatcher *notify.Dispatcher,
	channelSvc *channel.Service,
	authSvc *auth.Service,
	schedulerSvc *scheduler.Scheduler,
	proxyConfig config.ProxyConfig,
	upstreamConfig config.UpstreamConfig,
	schedulerFactory SchedulerFactory,
) *Manager {
	return &Manager{
		configPath:       configPath,
		securitySecret:   securitySecret,
		log:              log,
		dispatcher:       dispatcher,
		channelSvc:       channelSvc,
		schedulerFactory: schedulerFactory,
		auth:             authSvc,
		scheduler:        schedulerSvc,
		proxyConfig:      proxyConfig,
		upstreamConfig:   upstreamConfig.WithDefaults(),
	}
}

func (m *Manager) ConfigPath() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.configPath
}

func (m *Manager) CurrentAuth() *auth.Service {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.auth
}

func (m *Manager) CurrentProxy() config.ProxyConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.proxyConfig
}

func (m *Manager) CurrentUpstream() config.UpstreamConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.upstreamConfig
}

func (m *Manager) SetUserTime(service *usertime.Service) {
	m.mu.Lock()
	m.userTime = service
	m.mu.Unlock()
}

// SetStartupGatewayRequestAdmission records the one controller configuration
// installed before handlers start. It does not create or replace a controller.
func (m *Manager) SetStartupGatewayRequestAdmission(value config.GatewayRequestAdmissionConfig) error {
	value = config.NormalizeGatewayRequestAdmissionConfig(value)
	if err := config.ValidateGatewayRequestAdmissionConfig(value); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startupGatewayRequestAdmissionSet && m.startupGatewayRequestAdmission != value {
		return fmt.Errorf("%w: startup gateway request admission baseline is already configured", ErrRestartRequired)
	}
	m.startupGatewayRequestAdmission = value
	m.startupGatewayRequestAdmissionSet = true
	return nil
}

func (m *Manager) AuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if auth.IsAnonymousPath(c.FullPath()) || auth.IsAnonymousPath(c.Request.URL.Path) {
			c.Next()
			return
		}
		svc := m.CurrentAuth()
		if svc == nil {
			c.Next()
			return
		}
		svc.Middleware()(c)
	}
}

func (m *Manager) ApplyFromFile() (*ApplyResult, error) {
	m.mu.RLock()
	path := m.configPath
	secret := m.securitySecret
	dispatcher := m.dispatcher
	channelSvc := m.channelSvc
	factory := m.schedulerFactory
	oldScheduler := m.scheduler
	userTime := m.userTime
	startupRequestAdmission := m.startupGatewayRequestAdmission
	startupRequestAdmissionSet := m.startupGatewayRequestAdmissionSet
	m.mu.RUnlock()

	cfg, err := config.LoadFile(path)
	if err != nil {
		return nil, err
	}
	if startupRequestAdmissionSet && cfg.Gateway.RequestAdmission != startupRequestAdmission {
		return nil, &RestartRequiredError{Sections: []string{GatewayRequestAdmissionSection}}
	}

	authSvc, err := buildAuth(cfg.Auth, secret)
	if err != nil {
		return nil, err
	}
	if _, err := usertime.New(cfg.App.TimeZone); err != nil {
		return nil, err
	}

	newScheduler := factory(cfg.Scheduler, cfg.Proxy)
	if err := newScheduler.Start(); err != nil {
		return nil, err
	}
	if userTime != nil {
		if err := userTime.Set(cfg.App.TimeZone); err != nil {
			newScheduler.Stop()
			return nil, err
		}
	}
	if dispatcher != nil {
		dispatcher.UpdatePolicy(notify.Policy{
			NotificationPrefix:                       cfg.App.NotificationPrefix,
			BatchRateChanges:                         cfg.Notifications.BatchRateChanges,
			MinChangePct:                             cfg.Notifications.MinChangePct,
			BalanceLowCooldown:                       time.Duration(cfg.Notifications.BalanceLowCooldownMinutes) * time.Minute,
			SubscriptionDailyRemainingThresholdPct:   cfg.Notifications.SubscriptionDailyRemainingThresholdPct,
			SubscriptionWeeklyRemainingThresholdPct:  cfg.Notifications.SubscriptionWeeklyRemainingThresholdPct,
			SubscriptionMonthlyRemainingThresholdPct: cfg.Notifications.SubscriptionMonthlyRemainingThresholdPct,
			SubscriptionExpiryThreshold:              time.Duration(cfg.Notifications.SubscriptionExpiryThresholdHours) * time.Hour,
			SubscriptionAlertCooldown:                time.Duration(cfg.Notifications.SubscriptionAlertCooldownMinutes) * time.Minute,
			SendMaxAttempts:                          cfg.Notifications.SendMaxAttempts,
		})
		dispatcher.UpdateProxyConfig(cfg.Proxy)
	}
	if channelSvc != nil {
		channelSvc.UpdateUpstreamConfig(cfg.Upstream)
	}

	m.mu.Lock()
	m.auth = authSvc
	m.scheduler = newScheduler
	m.proxyConfig = cfg.Proxy
	m.upstreamConfig = cfg.Upstream.WithDefaults()
	m.mu.Unlock()

	if oldScheduler != nil {
		oldScheduler.Stop()
	}

	if m.log != nil {
		m.log.Info("runtime config applied",
			"sections", []string{"app", "auth", "scheduler", "notifications", "retention", "proxy", "upstream"},
			"config_path", path,
		)
	}

	return &ApplyResult{
		AppliedSections: []string{"app", "auth", "scheduler", "notifications", "retention", "proxy", "upstream"},
		Message:         "应用、认证、调度、通知、保留策略、代理和渠道请求设置已立即生效",
	}, nil
}

func buildAuth(cfg config.AuthConfig, securitySecret string) (*auth.Service, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	tokenSecret := cfg.TokenSecret
	if tokenSecret == "" {
		tokenSecret = securitySecret
	}
	svc, err := auth.New(
		cfg.Username,
		cfg.Password,
		tokenSecret,
		time.Duration(cfg.SessionTTLHours)*time.Hour,
	)
	if err != nil {
		return nil, fmt.Errorf("init auth: %w", err)
	}
	return svc, nil
}
