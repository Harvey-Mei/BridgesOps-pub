package runtimeconfig

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/scheduler"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
)

type fakeHTTPConfigConnector struct {
	cfg connector.HTTPConfig
}

func TestSetStartupGatewayRequestAdmissionRejectsProgrammaticZeroQueueWait(t *testing.T) {
	admission := config.DefaultGatewayRequestAdmissionConfig()
	admission.MaxQueueWaitMilliseconds = 0
	mgr := &Manager{}
	if err := mgr.SetStartupGatewayRequestAdmission(admission); err == nil || !strings.Contains(err.Error(), "max queue wait milliseconds") {
		t.Fatalf("set startup admission error = %v", err)
	}
	if mgr.startupGatewayRequestAdmissionSet {
		t.Fatalf("invalid startup admission set a baseline: %+v", mgr.startupGatewayRequestAdmission)
	}
}

func TestApplyFromFileRejectsZeroGatewayRequestAdmissionQueueWaitBeforeRuntimeChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("gateway:\n  requestAdmission:\n    maxQueueWaitMilliseconds: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	factoryCalls := 0
	mgr := New(
		path,
		"",
		nil,
		nil,
		nil,
		nil,
		nil,
		config.ProxyConfig{},
		config.UpstreamConfig{},
		func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
			factoryCalls++
			return scheduler.New(scfg, nil, nil, nil, nil, nil, nil, nil, nil, pcfg, nil)
		},
	)
	if err := mgr.SetStartupGatewayRequestAdmission(config.DefaultGatewayRequestAdmissionConfig()); err != nil {
		t.Fatalf("set startup admission: %v", err)
	}

	if _, err := mgr.ApplyFromFile(); err == nil || !strings.Contains(err.Error(), "max queue wait milliseconds") {
		t.Fatalf("apply zero queue wait error = %v", err)
	}
	if factoryCalls != 0 {
		t.Fatalf("scheduler factory called %d times after invalid config", factoryCalls)
	}
	if got := mgr.startupGatewayRequestAdmission; got != config.DefaultGatewayRequestAdmissionConfig() {
		t.Fatalf("startup admission changed after invalid runtime apply: %+v", got)
	}
}

func TestApplyFromFileRejectsInvalidTimeZoneBeforeRuntimeChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := &config.Config{
		App:     config.AppConfig{TimeZone: "Invalid/TimeZone"},
		Gateway: config.GatewayConfig{RequestAdmission: config.DefaultGatewayRequestAdmissionConfig()},
		Scheduler: config.SchedulerConfig{
			BalanceCron: "",
			RateCron:    "",
			Retention:   config.RetentionConfig{Cron: ""},
		},
		Upstream: config.UpstreamConfig{TimeoutSeconds: 90, UserAgent: "new-agent"},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	channelSvc := channel.NewService(nil, nil, nil, nil, nil, nil)
	channelSvc.UpdateUpstreamConfig(config.UpstreamConfig{TimeoutSeconds: 15, UserAgent: "old-agent"})
	userTime, err := usertime.New("UTC")
	if err != nil {
		t.Fatalf("new user time: %v", err)
	}
	factoryCalls := 0
	mgr := New(
		path,
		"",
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		channelSvc,
		nil,
		nil,
		config.ProxyConfig{},
		config.UpstreamConfig{TimeoutSeconds: 15, UserAgent: "old-agent"},
		func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
			factoryCalls++
			return scheduler.New(scfg, nil, nil, nil, nil, nil, nil, nil, nil, pcfg, nil)
		},
	)
	mgr.SetUserTime(userTime)

	if _, err := mgr.ApplyFromFile(); err == nil {
		t.Fatal("ApplyFromFile accepted an invalid time zone")
	}
	if factoryCalls != 0 {
		t.Fatalf("scheduler factory called %d times before time zone validation", factoryCalls)
	}
	if got := userTime.Name(); got != "UTC" {
		t.Fatalf("user time changed to %q", got)
	}
	conn := &fakeHTTPConfigConnector{}
	channelSvc.ApplyHTTPConfig(conn)
	if conn.cfg.Timeout != 15*time.Second || conn.cfg.UserAgent != "old-agent" {
		t.Fatalf("channel runtime config changed after rejected apply: %+v", conn.cfg)
	}
}

func (f *fakeHTTPConfigConnector) SetHTTPConfig(cfg connector.HTTPConfig) {
	f.cfg = cfg
}

func TestApplyFromFileRejectsStartupAdmissionChangeBeforeRuntimeMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	startupAdmission := config.DefaultGatewayRequestAdmissionConfig()
	changedAdmission := startupAdmission
	changedAdmission.MaxQueueWaitMilliseconds = startupAdmission.MaxQueueWaitMilliseconds - 1
	cfg := &config.Config{
		App:     config.AppConfig{TimeZone: "Asia/Shanghai"},
		Gateway: config.GatewayConfig{RequestAdmission: changedAdmission},
		Scheduler: config.SchedulerConfig{
			BalanceCron: "",
			RateCron:    "",
			Retention:   config.RetentionConfig{Cron: ""},
		},
		Upstream: config.UpstreamConfig{TimeoutSeconds: 90, UserAgent: "new-agent"},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	channelSvc := channel.NewService(nil, nil, nil, nil, nil, nil)
	channelSvc.UpdateUpstreamConfig(config.UpstreamConfig{TimeoutSeconds: 15, UserAgent: "old-agent"})
	userTime, err := usertime.New("UTC")
	if err != nil {
		t.Fatalf("new user time: %v", err)
	}
	factoryCalls := 0
	mgr := New(
		path,
		"",
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		nil,
		channelSvc,
		nil,
		nil,
		config.ProxyConfig{},
		config.UpstreamConfig{TimeoutSeconds: 15, UserAgent: "old-agent"},
		func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
			factoryCalls++
			return scheduler.New(scfg, nil, nil, nil, nil, nil, nil, nil, nil, pcfg, nil)
		},
	)
	if err := mgr.SetStartupGatewayRequestAdmission(startupAdmission); err != nil {
		t.Fatalf("set startup admission: %v", err)
	}
	mgr.SetUserTime(userTime)

	if _, err := mgr.ApplyFromFile(); !errors.Is(err, ErrRestartRequired) {
		t.Fatalf("ApplyFromFile error = %v, want restart required", err)
	} else if sections := RestartRequiredSections(err); len(sections) != 1 || sections[0] != GatewayRequestAdmissionSection {
		t.Fatalf("restart-required sections = %#v", sections)
	}
	if factoryCalls != 0 {
		t.Fatalf("scheduler factory called %d times before startup-only comparison", factoryCalls)
	}
	if got := userTime.Name(); got != "UTC" {
		t.Fatalf("user time changed to %q", got)
	}
	conn := &fakeHTTPConfigConnector{}
	channelSvc.ApplyHTTPConfig(conn)
	if conn.cfg.Timeout != 15*time.Second || conn.cfg.UserAgent != "old-agent" {
		t.Fatalf("channel runtime config changed after restart-required apply: %+v", conn.cfg)
	}

	restartedFactoryCalls := 0
	restartedLog := slog.New(slog.NewTextHandler(io.Discard, nil))
	restarted := New(
		path,
		"",
		restartedLog,
		nil,
		nil,
		nil,
		nil,
		config.ProxyConfig{},
		config.UpstreamConfig{},
		func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
			restartedFactoryCalls++
			return scheduler.New(scfg, nil, nil, nil, nil, nil, nil, nil, nil, pcfg, restartedLog)
		},
	)
	if err := restarted.SetStartupGatewayRequestAdmission(changedAdmission); err != nil {
		t.Fatalf("set restarted admission: %v", err)
	}
	if _, err := restarted.ApplyFromFile(); err != nil {
		t.Fatalf("restarted manager rejected matching admission: %v", err)
	}
	if restartedFactoryCalls != 1 {
		t.Fatalf("restarted scheduler factory calls = %d, want 1", restartedFactoryCalls)
	}
	restarted.scheduler.Stop()
}

func TestApplyFromFileUpdatesUpstreamConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := &config.Config{
		Gateway: config.GatewayConfig{RequestAdmission: config.DefaultGatewayRequestAdmissionConfig()},
		Scheduler: config.SchedulerConfig{
			BalanceCron: "",
			RateCron:    "",
			Retention:   config.RetentionConfig{Cron: ""},
		},
		Upstream: config.UpstreamConfig{
			TimeoutSeconds: 45,
			UserAgent:      "custom-agent",
		},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	channelSvc := channel.NewService(nil, nil, nil, nil, nil, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := New(
		path,
		"",
		log,
		nil,
		channelSvc,
		nil,
		nil,
		config.ProxyConfig{},
		config.UpstreamConfig{},
		func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
			return scheduler.New(scfg, nil, nil, nil, nil, nil, nil, nil, nil, pcfg, log)
		},
	)

	result, err := mgr.ApplyFromFile()
	if err != nil {
		t.Fatalf("ApplyFromFile: %v", err)
	}
	if len(result.AppliedSections) == 0 {
		t.Fatalf("applied sections empty")
	}
	if got := mgr.CurrentUpstream(); got.TimeoutSeconds != 45 || got.UserAgent != "custom-agent" {
		t.Fatalf("current upstream = %#v", got)
	}

	conn := &fakeHTTPConfigConnector{}
	channelSvc.ApplyHTTPConfig(conn)
	if conn.cfg.Timeout != 45*time.Second {
		t.Fatalf("timeout = %s", conn.cfg.Timeout)
	}
	if conn.cfg.UserAgent != "custom-agent" {
		t.Fatalf("user agent = %q", conn.cfg.UserAgent)
	}
}
