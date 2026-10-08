// Package scheduler 用 robfig/cron 触发周期性扫描。
package scheduler

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/captcha"
	"github.com/Harvey-Mei/BridgeOps/backend/clauderadar"
	"github.com/Harvey-Mei/BridgeOps/backend/codexradar"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/monitor"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/robfig/cron/v3"
)

type Scheduler struct {
	cfg              config.SchedulerConfig
	log              *slog.Logger
	cron             *cron.Cron
	monitor          *monitor.Service
	monLogs          *storage.MonitorLogs
	rates            *storage.Rates
	notifies         *storage.Notifications
	announcements    *storage.UpstreamAnnouncements
	captchas         *storage.Captchas
	cipher           *crypto.Cipher
	gateway          *gateway.Service
	codexRadar       *codexradar.Service
	claudeRadar      *clauderadar.Service
	modelCheck       *modelcheck.Service
	proxy            config.ProxyConfig
	stopLatency      context.CancelFunc
	stopModelSync    context.CancelFunc
	stopKeepAlive    context.CancelFunc
	stopRateRecovery context.CancelFunc
	stopCodexRadar   context.CancelFunc
	stopClaudeRadar  context.CancelFunc
	stopModelCheck   context.CancelFunc
	latencyMu        sync.Mutex
	rateMu           sync.Mutex
	rateRecoveryMu   sync.Mutex
	modelSyncMu      sync.Mutex
	keepAliveMu      sync.Mutex
	monitorMu        sync.Mutex
	rateRecoveryWG   sync.WaitGroup
	codexRadarWG     sync.WaitGroup
	claudeRadarWG    sync.WaitGroup
	modelCheckWG     sync.WaitGroup
	modelCheckMu     sync.Mutex
}

func New(
	cfg config.SchedulerConfig,
	m *monitor.Service,
	monLogs *storage.MonitorLogs,
	rates *storage.Rates,
	notifies *storage.Notifications,
	announcements *storage.UpstreamAnnouncements,
	captchas *storage.Captchas,
	cipher *crypto.Cipher,
	gatewaySvc *gateway.Service,
	proxy config.ProxyConfig,
	log *slog.Logger,
) *Scheduler {
	if m != nil {
		m.SetScanConcurrency(cfg.Concurrency)
	}
	return &Scheduler{
		cfg:           cfg,
		log:           log,
		cron:          cron.New(cron.WithSeconds()),
		monitor:       m,
		monLogs:       monLogs,
		rates:         rates,
		notifies:      notifies,
		announcements: announcements,
		captchas:      captchas,
		cipher:        cipher,
		gateway:       gatewaySvc,
		proxy:         proxy,
	}
}

// SetCodexRadar attaches the shared in-memory Radar service. Keeping the
// service outside Scheduler lets runtime config reloads restart the timer
// without discarding the last successful payload.
func (s *Scheduler) SetCodexRadar(service *codexradar.Service) {
	if s == nil {
		return
	}
	s.codexRadar = service
}

// SetClaudeRadar attaches the shared Claude Radar service to the scheduler.
func (s *Scheduler) SetClaudeRadar(service *clauderadar.Service) {
	if s == nil {
		return
	}
	s.claudeRadar = service
}

func (s *Scheduler) SetModelCheck(service *modelcheck.Service) {
	if s == nil {
		return
	}
	s.modelCheck = service
}

func (s *Scheduler) Start() error {
	if s.cfg.BalanceCron != "" {
		if _, err := s.cron.AddFunc(s.cfg.BalanceCron, s.runBalance); err != nil {
			return err
		}
	}
	rateCron := s.cfg.EffectiveRateCron()
	if rateCron != "" {
		if _, err := s.cron.AddFunc(rateCron, s.runRates); err != nil {
			return err
		}
	}
	if s.cfg.Retention.Cron != "" && s.hasRetention() {
		if _, err := s.cron.AddFunc(s.cfg.Retention.Cron, s.runRetention); err != nil {
			return err
		}
	}
	if s.monitor != nil {
		s.monitor.SetFundingDetectionThreshold(s.cfg.FundingDetectionThreshold)
	}
	s.startLatencyProbe()
	s.startLatencyModelSync()
	s.startSessionKeepAlive()
	s.startRateRecovery()
	s.startCodexRadar()
	s.startClaudeRadar()
	s.startModelCheck()
	s.cron.Start()
	s.log.Info("scheduler started",
		"balanceCron", s.cfg.BalanceCron,
		"rateCron", rateCron,
		"rateRefreshIntervalMinutes", s.cfg.RateRefreshIntervalMinutes,
		"retentionCron", s.cfg.Retention.Cron,
		"sessionKeepAliveEnabled", s.cfg.SessionKeepAliveEnabled,
		"sessionKeepAliveIntervalMinutes", s.cfg.SessionKeepAliveIntervalMins,
		"latencyProbeEnabled", s.cfg.LatencyProbeEnabled,
		"latencyProbeIntervalSeconds", s.cfg.LatencyProbeIntervalSeconds,
		"codexRadarRefreshIntervalMinutes", s.cfg.CodexRadarRefreshIntervalMins,
		"modelCheckEnabled", s.cfg.ModelCheck.Enabled,
		"modelCheckRunsPerDay", s.cfg.ModelCheck.RunsPerDay,
		"concurrency", s.cfg.Concurrency,
	)
	return nil
}

func (s *Scheduler) Stop() {
	if s.stopLatency != nil {
		s.stopLatency()
	}
	if s.stopModelSync != nil {
		s.stopModelSync()
	}
	if s.stopKeepAlive != nil {
		s.stopKeepAlive()
	}
	if s.stopRateRecovery != nil {
		s.stopRateRecovery()
		s.rateRecoveryWG.Wait()
	}
	if s.stopCodexRadar != nil {
		s.stopCodexRadar()
		s.codexRadarWG.Wait()
	}
	if s.stopClaudeRadar != nil {
		s.stopClaudeRadar()
		s.claudeRadarWG.Wait()
	}
	if s.stopModelCheck != nil {
		s.stopModelCheck()
		s.modelCheckWG.Wait()
	}
	if s.cron != nil {
		<-s.cron.Stop().Done()
	}
}

func (s *Scheduler) startModelCheck() {
	if s.modelCheck == nil || !s.cfg.ModelCheck.Enabled {
		return
	}
	cfg := modelcheck.NormalizeConfig(s.cfg.ModelCheck)
	interval := 24 * time.Hour / time.Duration(cfg.RunsPerDay)
	ctx, cancel := context.WithCancel(context.Background())
	s.stopModelCheck = cancel
	s.modelCheckWG.Add(1)
	go func() {
		defer s.modelCheckWG.Done()
		timer := time.NewTimer(time.Minute)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.runModelCheck(ctx, cfg)
				timer.Reset(interval)
			}
		}
	}()
}

func (s *Scheduler) runModelCheck(ctx context.Context, cfg config.ModelCheckConfig) {
	if s.modelCheck == nil || !s.modelCheckMu.TryLock() {
		return
	}
	defer s.modelCheckMu.Unlock()
	result, err := s.modelCheck.RunScheduled(ctx, cfg)
	if err != nil {
		if s.log != nil {
			s.log.Warn("model integrity check batch finished with errors", "targets", result.Targets, "requests", result.Requests, "err", err)
		}
		return
	}
	if s.log != nil {
		s.log.Info("model integrity check batch finished", "targets", result.Targets, "passed", result.Passed, "anomalies", result.Anomalies, "errors", result.Errors, "requests", result.Requests)
	}
}

func (s *Scheduler) startCodexRadar() {
	if s.codexRadar == nil {
		return
	}
	interval := time.Duration(s.cfg.CodexRadarRefreshIntervalMins) * time.Minute
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.stopCodexRadar = cancel
	s.codexRadarWG.Add(1)
	go func() {
		defer s.codexRadarWG.Done()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.runCodexRadar(ctx)
				timer.Reset(interval)
			}
		}
	}()
}

func (s *Scheduler) runCodexRadar(ctx context.Context) {
	if s.codexRadar == nil {
		return
	}
	_, err := s.codexRadar.Refresh(ctx)
	if s.log == nil {
		return
	}
	if err != nil {
		s.log.Warn("codex radar refresh failed", "err", err)
		return
	}
	s.log.Info("codex radar refreshed")
}

func (s *Scheduler) startClaudeRadar() {
	if s.claudeRadar == nil {
		return
	}
	interval := time.Duration(s.cfg.CodexRadarRefreshIntervalMins) * time.Minute
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.stopClaudeRadar = cancel
	s.claudeRadarWG.Add(1)
	go func() {
		defer s.claudeRadarWG.Done()
		timer := time.NewTimer(7 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.runClaudeRadar(ctx)
				timer.Reset(interval)
			}
		}
	}()
}

func (s *Scheduler) runClaudeRadar(ctx context.Context) {
	if s.claudeRadar == nil {
		return
	}
	_, err := s.claudeRadar.Refresh(ctx)
	if s.log == nil {
		return
	}
	if err != nil {
		s.log.Warn("claude radar refresh failed", "err", err)
		return
	}
	s.log.Info("claude radar refreshed")
}

func (s *Scheduler) startSessionKeepAlive() {
	if s.monitor == nil || !s.cfg.SessionKeepAliveEnabled {
		return
	}
	interval := time.Duration(s.cfg.SessionKeepAliveIntervalMins) * time.Minute
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.stopKeepAlive = cancel
	go func() {
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.runSessionKeepAlive()
				timer.Reset(interval)
			}
		}
	}()
}

// startRateRecovery handles only channels that have a persisted failed-sync
// state. A short initial delay heals existing failures after a restart; later
// passes consult each channel's next_sync_retry_at before doing network work.
func (s *Scheduler) startRateRecovery() {
	if s.monitor == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.stopRateRecovery = cancel
	s.rateRecoveryWG.Add(1)
	go func() {
		defer s.rateRecoveryWG.Done()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.runRateRecovery(ctx)
				timer.Reset(time.Minute)
			}
		}
	}()
}

func (s *Scheduler) startLatencyProbe() {
	if s.gateway == nil {
		return
	}
	// LatencyProbeEnabled is retained for configuration compatibility, but the
	// old global active-probe switch must not disable cooldown recovery after
	// the periodic healthy-probe feature has been removed.
	interval := time.Duration(s.cfg.LatencyProbeIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.stopLatency = cancel
	go func() {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				timer.Reset(s.runLatencyProbe(interval))
			}
		}
	}()
}

func (s *Scheduler) startLatencyModelSync() {
	if s.gateway == nil {
		return
	}
	interval := time.Duration(s.cfg.LatencyProbeModelSyncMinutes) * time.Minute
	if interval <= 0 {
		interval = time.Hour
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.stopModelSync = cancel
	go func() {
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				s.runLatencyModelSync()
				timer.Reset(interval)
			}
		}
	}()
}

func (s *Scheduler) runLatencyModelSync() {
	if s.gateway == nil {
		return
	}
	if !s.modelSyncMu.TryLock() {
		return
	}
	defer s.modelSyncMu.Unlock()
	if _, err := s.gateway.SyncModelFamilies(context.Background(), 0); err != nil && s.log != nil {
		s.log.Warn("gateway latency model sync failed", "err", err)
	}
}

func (s *Scheduler) runLatencyProbe(defaultCadence ...time.Duration) time.Duration {
	cadence := latencyProbeCadence(s.cfg.LatencyProbeIntervalSeconds)
	if len(defaultCadence) > 0 && defaultCadence[0] > 0 {
		cadence = defaultCadence[0]
	}
	if s.gateway == nil {
		return cadence
	}
	if !s.latencyMu.TryLock() {
		return latencyProbeWakeDelay(cadence, time.Now().UTC(), nil)
	}
	defer s.latencyMu.Unlock()
	timeout := latencyProbeAttemptTimeout(s.cfg.LatencyProbeTimeoutSeconds)
	ctx, cancel := context.WithTimeout(context.Background(), latencyProbeRunTimeout(timeout, s.cfg.LatencyProbeMaxCandidates))
	defer cancel()
	results, err := s.gateway.RunLatencyProbes(ctx, gateway.LatencyProbeOptions{
		MaxCandidates:       s.cfg.LatencyProbeMaxCandidates,
		Timeout:             timeout,
		Interval:            cadence,
		MaxConcurrent:       max(1, s.cfg.Concurrency),
		MaxEstimatedCostUSD: gateway.DefaultLatencyProbeBudgetUSD,
	})
	if err != nil {
		if s.log != nil {
			s.log.Warn("gateway latency probe failed", "err", err)
		}
		return s.nextLatencyProbeWakeDelay(cadence)
	}
	failures := 0
	for _, result := range results {
		if result.Error != "" {
			failures++
		}
	}
	if s.log != nil {
		s.log.Info("gateway latency probe finished", "total", len(results), "failures", failures)
	}
	return s.nextLatencyProbeWakeDelay(cadence)
}

func (s *Scheduler) nextLatencyProbeWakeDelay(defaultCadence time.Duration) time.Duration {
	if s == nil || s.gateway == nil || s.gateway.Store == nil {
		return latencyProbeWakeDelay(defaultCadence, time.Now().UTC(), nil)
	}
	next, err := s.gateway.Store.NextLatencyProbeDueAt(defaultCadence)
	if err != nil {
		if s.log != nil {
			s.log.Warn("read next gateway latency probe due time failed", "err", err)
		}
		return latencyProbeWakeDelay(defaultCadence, time.Now().UTC(), nil)
	}
	return latencyProbeWakeDelay(defaultCadence, time.Now().UTC(), next)
}

func latencyProbeWakeDelay(defaultCadence time.Duration, now time.Time, next *time.Time) time.Duration {
	if defaultCadence <= 0 {
		defaultCadence = time.Minute
	}
	if next == nil {
		return defaultCadence
	}
	delay := next.Sub(now)
	if delay <= 0 {
		// A due hint may belong to a candidate deferred by this pass's count or
		// cost cap. Do not turn that bounded deferral into a one-second empty
		// scheduler loop; selected mixed/cooling work always persists a future
		// precise due time and still wakes promptly.
		return defaultCadence
	}
	if delay < defaultCadence {
		return delay
	}
	return defaultCadence
}

const (
	latencyProbeGatewayAppCount = 3
	latencyProbeRunOverhead     = 30 * time.Second
	// Saturate invalidly large administrator budgets at Duration's positive
	// limit so context.WithTimeout never receives a wrapped negative deadline.
	latencyProbeMaxRunTimeout = time.Duration(math.MaxInt64)
)

func latencyProbeRunTimeout(timeout time.Duration, maxCandidates int) time.Duration {
	if timeout <= 0 {
		timeout = gateway.DefaultLatencyProbeTimeout
	}
	if maxCandidates <= 0 {
		maxCandidates = gateway.DefaultLatencyProbeMaxCandidates
	}
	probeBudget := saturatingDurationMultiply(timeout, maxCandidates)
	probeBudget = saturatingDurationMultiply(probeBudget, latencyProbeGatewayAppCount)
	probeBudget = saturatingDurationMultiply(probeBudget, gateway.LatencyProbeConfirmationAttempts)
	return saturatingDurationAdd(probeBudget, max(latencyProbeRunOverhead, probeBudget/10))
}

func latencyProbeAttemptTimeout(seconds int) time.Duration {
	if seconds <= 0 {
		return gateway.DefaultLatencyProbeTimeout
	}
	return saturatingDurationMultiply(time.Duration(seconds), int(time.Second))
}

func latencyProbeCadence(seconds int) time.Duration {
	if seconds <= 0 {
		return time.Minute
	}
	return saturatingDurationMultiply(time.Duration(seconds), int(time.Second))
}

func saturatingDurationMultiply(value time.Duration, factor int) time.Duration {
	if value <= 0 || factor <= 0 {
		return 0
	}
	if value > latencyProbeMaxRunTimeout/time.Duration(factor) {
		return latencyProbeMaxRunTimeout
	}
	return value * time.Duration(factor)
}

func saturatingDurationAdd(left, right time.Duration) time.Duration {
	if left >= latencyProbeMaxRunTimeout || right >= latencyProbeMaxRunTimeout-left {
		return latencyProbeMaxRunTimeout
	}
	return left + right
}

func (s *Scheduler) runSessionKeepAlive() {
	if s.monitor == nil {
		return
	}
	if !s.keepAliveMu.TryLock() {
		return
	}
	defer s.keepAliveMu.Unlock()
	if !s.monitorMu.TryLock() {
		return
	}
	defer s.monitorMu.Unlock()
	interval := time.Duration(s.cfg.SessionKeepAliveIntervalMins) * time.Minute
	if interval <= 0 {
		interval = 10 * time.Minute
	}
	timeout := interval / 2
	if timeout < 30*time.Second {
		timeout = 30 * time.Second
	}
	if timeout > 5*time.Minute {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	result := s.monitor.KeepAliveSessions(ctx)
	if s.log != nil {
		s.log.Info("channel session keepalive finished", "total", result.Total, "success", result.Success, "failed", result.Failed)
	}
}

func (s *Scheduler) runBalance() {
	if s.monitor == nil || !s.monitorMu.TryLock() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	func() {
		defer s.monitorMu.Unlock()
		s.monitor.ScanAllBalances(ctx)
	}()
	if s.captchas != nil && s.cipher != nil {
		if _, err := captcha.RefreshAllBalancesWithProxy(ctx, s.captchas, s.cipher, s.log, s.proxy); err != nil {
			s.log.Warn("refresh captcha balances failed", "err", err)
		}
	}
}

func (s *Scheduler) runRates() {
	if s.monitor == nil || !s.rateMu.TryLock() {
		return
	}
	defer s.rateMu.Unlock()
	if !s.monitorMu.TryLock() {
		return
	}
	defer s.monitorMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s.monitor.ScanAllRates(ctx)
}

func (s *Scheduler) runRateRecovery(parent context.Context) {
	if s.monitor == nil || !s.rateRecoveryMu.TryLock() {
		return
	}
	defer s.rateRecoveryMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	s.monitor.RecoverDueRateSyncs(ctx)
}

func (s *Scheduler) hasRetention() bool {
	r := s.cfg.Retention
	return r.MonitorLogsDays > 0 ||
		r.BalanceSnapshotsDays > 0 ||
		r.NotificationLogsDays > 0 ||
		r.AnnouncementsDays > 0
}

// runRetention 按配置删除过期历史。任一表失败不影响其它，全部错误写日志。
func (s *Scheduler) runRetention() {
	r := s.cfg.Retention
	now := time.Now()

	if r.MonitorLogsDays > 0 {
		cutoff := now.AddDate(0, 0, -r.MonitorLogsDays)
		n, err := s.monLogs.DeleteBefore(cutoff)
		if err != nil {
			s.log.Warn("retention monitor_logs failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention monitor_logs deleted", "rows", n, "before", cutoff)
		}
	}

	if r.BalanceSnapshotsDays > 0 {
		cutoff := now.AddDate(0, 0, -r.BalanceSnapshotsDays)
		n, err := s.rates.DeleteBalanceSnapshotsBefore(cutoff)
		if err != nil {
			s.log.Warn("retention balance_snapshots failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention balance_snapshots deleted", "rows", n, "before", cutoff)
		}

		n, err = s.rates.DeleteCostSnapshotsBefore(cutoff)
		if err != nil {
			s.log.Warn("retention cost_snapshots failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention cost_snapshots deleted", "rows", n, "before", cutoff)
		}
	}

	if r.NotificationLogsDays > 0 {
		cutoff := now.AddDate(0, 0, -r.NotificationLogsDays)
		n, err := s.notifies.DeleteLogsBefore(cutoff)
		if err != nil {
			s.log.Warn("retention notification_logs failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention notification_logs deleted", "rows", n, "before", cutoff)
		}
	}

	if r.AnnouncementsDays > 0 && s.announcements != nil {
		cutoff := now.AddDate(0, 0, -r.AnnouncementsDays)
		n, err := s.announcements.DeleteBefore(cutoff)
		if err != nil {
			s.log.Warn("retention announcements failed", "err", err)
		} else if n > 0 {
			s.log.Info("retention announcements deleted", "rows", n, "before", cutoff)
		}
	}
}
