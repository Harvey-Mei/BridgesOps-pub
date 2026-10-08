package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAppliesUpstreamDefaults(t *testing.T) {
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Upstream.TimeoutSeconds != DefaultUpstreamTimeoutSeconds {
		t.Fatalf("timeout seconds = %d", cfg.Upstream.TimeoutSeconds)
	}
	if cfg.Upstream.UserAgent != DefaultUpstreamUserAgent {
		t.Fatalf("user agent = %q", cfg.Upstream.UserAgent)
	}
	if cfg.Scheduler.CodexRadarRefreshIntervalMins != 15 {
		t.Fatalf("codex radar refresh interval = %d, want 15", cfg.Scheduler.CodexRadarRefreshIntervalMins)
	}
	if cfg.Scheduler.RateRefreshIntervalMinutes != 0 {
		t.Fatalf("rate refresh interval = %d, want legacy fallback 0", cfg.Scheduler.RateRefreshIntervalMinutes)
	}
	if cfg.Scheduler.FundingDetectionThreshold != MinimumFundingDetectionThreshold {
		t.Fatalf("funding detection threshold = %v, want %v", cfg.Scheduler.FundingDetectionThreshold, MinimumFundingDetectionThreshold)
	}
	if cfg.Scheduler.UnknownLatencyScoreFactor != DefaultUnknownLatencyScoreFactor {
		t.Fatalf("unknown latency score factor = %v, want %v", cfg.Scheduler.UnknownLatencyScoreFactor, DefaultUnknownLatencyScoreFactor)
	}
	if cfg.Scheduler.CacheInputPriceMultiplier != DefaultCacheInputPriceMultiplier || cfg.Scheduler.CachePriceMinInputTokens != DefaultCachePriceInputTokens || cfg.Scheduler.CachePriceWindowInputTokens != DefaultCachePriceInputTokens || cfg.Scheduler.CacheStatsMaxAgeDays != DefaultCacheStatsMaxAgeDays {
		t.Fatalf("cache pricing defaults = %+v", cfg.Scheduler)
	}
	if cfg.Scheduler.RoutingFailures.ManualCooldownMinutes != DefaultManualRoutingCooldownMinutes {
		t.Fatalf("manual routing cooldown = %d, want %d", cfg.Scheduler.RoutingFailures.ManualCooldownMinutes, DefaultManualRoutingCooldownMinutes)
	}
	if got := cfg.Scheduler.EffectiveRateCron(); got != "13 * * * * *" {
		t.Fatalf("effective rate cron = %q, want every minute", got)
	}
}

func TestManualRoutingCooldownConfigValidation(t *testing.T) {
	for _, value := range []int{MinimumManualRoutingCooldownMinutes, DefaultManualRoutingCooldownMinutes, MaximumManualRoutingCooldownMinutes} {
		cfg := NormalizeRoutingFailureConfig(RoutingFailureConfig{ManualCooldownMinutes: value})
		if err := ValidateRoutingFailureConfig(cfg); err != nil {
			t.Fatalf("manual cooldown %d rejected: %v", value, err)
		}
	}
	if got := NormalizeRoutingFailureConfig(RoutingFailureConfig{}).ManualCooldownMinutes; got != DefaultManualRoutingCooldownMinutes {
		t.Fatalf("zero manual cooldown normalized to %d", got)
	}
	for _, value := range []int{-1, MaximumManualRoutingCooldownMinutes + 1} {
		if err := ValidateRoutingFailureConfig(RoutingFailureConfig{ManualCooldownMinutes: value}); err == nil {
			t.Fatalf("invalid manual cooldown %d was accepted", value)
		}
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("scheduler:\n  routingFailures:\n    manualCooldownMinutes: 1441\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(path); err == nil || !strings.Contains(err.Error(), "manual routing cooldown") {
		t.Fatalf("invalid persisted manual cooldown error = %v", err)
	}
}

func TestLoadWithPathRejectsMissingExplicitConfig(t *testing.T) {
	_, _, err := LoadWithPath(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil {
		t.Fatal("missing explicit config unexpectedly loaded")
	}
}

func TestUnknownLatencyScoreFactorIsBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("scheduler:\n  unknownLatencyScoreFactor: 1.7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Scheduler.UnknownLatencyScoreFactor != 1 {
		t.Fatalf("upper bound = %v, want 1", loaded.Scheduler.UnknownLatencyScoreFactor)
	}

	if err := os.WriteFile(path, []byte("scheduler:\n  unknownLatencyScoreFactor: 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Scheduler.UnknownLatencyScoreFactor != DefaultUnknownLatencyScoreFactor {
		t.Fatalf("zero factor = %v, want default %v", loaded.Scheduler.UnknownLatencyScoreFactor, DefaultUnknownLatencyScoreFactor)
	}
}

func TestCachePricingSettingsAreBounded(t *testing.T) {
	if DefaultCacheInputPriceMultiplier != 1 {
		t.Fatalf("default cache input price multiplier = %v, want 1", DefaultCacheInputPriceMultiplier)
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("scheduler:\n  cacheInputPriceMultiplier: 2\n  cachePriceMinInputTokens: 1000000000001\n  cachePriceWindowInputTokens: 0\n  cacheStatsMaxAgeDays: 366\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Scheduler.CacheInputPriceMultiplier != DefaultCacheInputPriceMultiplier || loaded.Scheduler.CachePriceMinInputTokens != DefaultCachePriceInputTokens || loaded.Scheduler.CachePriceWindowInputTokens != DefaultCachePriceInputTokens || loaded.Scheduler.CacheStatsMaxAgeDays != DefaultCacheStatsMaxAgeDays {
		t.Fatalf("bounded cache settings = %+v", loaded.Scheduler)
	}
}

func TestFundingDetectionThresholdHasHardFloor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("scheduler:\n  fundingDetectionThreshold: 0.5\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Scheduler.FundingDetectionThreshold != MinimumFundingDetectionThreshold {
		t.Fatalf("loaded low threshold = %v", loaded.Scheduler.FundingDetectionThreshold)
	}

	loaded.Scheduler.FundingDetectionThreshold = 2.5
	if err := Save(path, loaded); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Scheduler.FundingDetectionThreshold != 2.5 {
		t.Fatalf("saved threshold = %v, want 2.5", loaded.Scheduler.FundingDetectionThreshold)
	}

	loaded.Scheduler.FundingDetectionThreshold = 0
	if err := Save(path, loaded); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Scheduler.FundingDetectionThreshold != MinimumFundingDetectionThreshold {
		t.Fatalf("persisted low threshold = %v", loaded.Scheduler.FundingDetectionThreshold)
	}
}

func TestLoadBindsDatabaseTimeZoneSeparatelyFromAppTimeZone(t *testing.T) {
	t.Setenv("DATABASE_TIMEZONE", "Asia/Shanghai")
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Database.TimeZone != "Asia/Shanghai" || cfg.Database.ToStorageConfig().TimeZone != "Asia/Shanghai" {
		t.Fatalf("database timezone = %q", cfg.Database.TimeZone)
	}
	if cfg.App.TimeZone != "Asia/Shanghai" {
		t.Fatalf("app timezone changed unexpectedly: %q", cfg.App.TimeZone)
	}
}

func TestLoadAcceptsDatabaseTimeZoneEnvironmentAlias(t *testing.T) {
	t.Setenv("DATABASE_TIMEZONE", "")
	t.Setenv("DATABASE_TIME_ZONE", "UTC")
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Database.TimeZone != "UTC" {
		t.Fatalf("database timezone alias = %q", cfg.Database.TimeZone)
	}
}

func TestLoadBindsOptionalRedisEnvironment(t *testing.T) {
	t.Setenv("REDIS_ENABLED", "true")
	t.Setenv("REDIS_HOST", "cache.internal")
	t.Setenv("REDIS_PORT", "6380")
	t.Setenv("REDIS_NAMESPACE", "bridgeops-prod")
	t.Setenv("REDIS_USERNAME", "bridgeops")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "4")
	t.Setenv("REDIS_TLS_ENABLED", "true")

	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Redis.Enabled || cfg.Redis.Host != "cache.internal" || cfg.Redis.Port != 6380 || cfg.Redis.Namespace != "bridgeops-prod" || cfg.Redis.Username != "bridgeops" || cfg.Redis.Password != "secret" || cfg.Redis.DB != 4 || !cfg.Redis.TLSEnabled {
		t.Fatalf("redis config = %+v", cfg.Redis)
	}
	if address, err := cfg.Redis.Address(); err != nil || address != "cache.internal:6380" {
		t.Fatalf("redis address = %q, %v", address, err)
	}
}

func TestLoadDefaultsTrustedProxiesToLoopbackOnly(t *testing.T) {
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if len(cfg.Server.TrustedProxies) != 2 || cfg.Server.TrustedProxies[0] != "127.0.0.1" || cfg.Server.TrustedProxies[1] != "::1" {
		t.Fatalf("trusted proxies = %#v", cfg.Server.TrustedProxies)
	}
}

func TestRedisConfigAddressRejectsInvalidValues(t *testing.T) {
	for _, cfg := range []RedisConfig{
		{},
		{Host: "cache", Port: 0},
		{Host: "cache", Port: 70000},
		{Host: "cache", Port: 6379, DB: -1},
	} {
		if _, err := cfg.Address(); err == nil {
			t.Fatalf("invalid redis config accepted: %+v", cfg)
		}
	}
	address, err := (RedisConfig{Host: "2001:db8::1", Port: 6379}).Address()
	if err != nil || address != "[2001:db8::1]:6379" {
		t.Fatalf("IPv6 redis address = %q, %v", address, err)
	}
}

func TestUpstreamConfigWithDefaultsKeepsCustomUserAgent(t *testing.T) {
	cfg := UpstreamConfig{
		TimeoutSeconds: 0,
		UserAgent:      "custom-agent",
	}.WithDefaults()
	if cfg.TimeoutSeconds != DefaultUpstreamTimeoutSeconds {
		t.Fatalf("timeout seconds = %d", cfg.TimeoutSeconds)
	}
	if cfg.UserAgent != "custom-agent" {
		t.Fatalf("user agent = %q", cfg.UserAgent)
	}
}

func TestSchedulerConfigEffectiveRateCron(t *testing.T) {
	tests := []struct {
		name     string
		cfg      SchedulerConfig
		wantCron string
	}{
		{
			name:     "interval",
			cfg:      SchedulerConfig{RateCron: "13 */30 * * * *", RateRefreshIntervalMinutes: 5},
			wantCron: "13 */5 * * * *",
		},
		{
			name:     "legacy custom cron",
			cfg:      SchedulerConfig{RateCron: "7 0 */6 * * *"},
			wantCron: "7 0 */6 * * *",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.EffectiveRateCron(); got != tt.wantCron {
				t.Fatalf("effective rate cron = %q, want %q", got, tt.wantCron)
			}
		})
	}
}

func TestSaveAtomicallyReplacesReloadableConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	cfg := &Config{
		App:      AppConfig{Title: "atomic-save", TimeZone: "UTC"},
		Gateway:  GatewayConfig{RequestAdmission: DefaultGatewayRequestAdmissionConfig()},
		Upstream: UpstreamConfig{TimeoutSeconds: 41, UserAgent: "atomic-agent"},
	}
	if err := Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat saved config: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %o, want 600", info.Mode().Perm())
	}
	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatalf("reload saved config: %v", err)
	}
	if loaded.App.Title != cfg.App.Title || loaded.Upstream.TimeoutSeconds != cfg.Upstream.TimeoutSeconds || loaded.Upstream.UserAgent != cfg.Upstream.UserAgent {
		t.Fatalf("reloaded saved config did not preserve expected fields")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read config directory: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".config.yaml.tmp-") {
			t.Fatalf("temporary config file remained after successful save")
		}
	}
}

func TestSaveCleansTemporaryFileAfterReplaceFailure(t *testing.T) {
	path := t.TempDir()
	if err := Save(path, &Config{Gateway: GatewayConfig{RequestAdmission: DefaultGatewayRequestAdmissionConfig()}}); err == nil {
		t.Fatal("save to an existing directory unexpectedly succeeded")
	}
	parent := filepath.Dir(path)
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatalf("read parent directory: %v", err)
	}
	prefix := "." + filepath.Base(path) + ".tmp-"
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
			t.Fatalf("temporary config file remained after failed save")
		}
	}
}

func TestSavePreservesExistingPermissionsAndSymlinkTarget(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.yaml")
	link := filepath.Join(dir, "config.yaml")
	if err := Save(target, &Config{
		App:     AppConfig{Title: "before", TimeZone: "UTC"},
		Gateway: GatewayConfig{RequestAdmission: DefaultGatewayRequestAdmissionConfig()},
	}); err != nil {
		t.Fatalf("save target: %v", err)
	}
	if err := os.Chmod(target, 0o600); err != nil {
		t.Fatalf("chmod target: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create config symlink: %v", err)
	}
	if err := Save(link, &Config{
		App:     AppConfig{Title: "after", TimeZone: "UTC"},
		Gateway: GatewayConfig{RequestAdmission: DefaultGatewayRequestAdmissionConfig()},
	}); err != nil {
		t.Fatalf("save through symlink: %v", err)
	}
	linkInfo, err := os.Lstat(link)
	if err != nil {
		t.Fatalf("lstat config symlink: %v", err)
	}
	if linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatal("save replaced the configuration symlink")
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat target: %v", err)
	}
	if targetInfo.Mode().Perm() != 0o600 {
		t.Fatalf("target mode = %o, want 600", targetInfo.Mode().Perm())
	}
	loaded, err := LoadFile(link)
	if err != nil {
		t.Fatalf("load symlink target: %v", err)
	}
	if loaded.App.Title != "after" {
		t.Fatalf("symlink target did not receive saved config")
	}
}
