package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/analyticscache"
	"github.com/Harvey-Mei/BridgeOps/backend/api"
	"github.com/Harvey-Mei/BridgeOps/backend/auth"
	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/clauderadar"
	"github.com/Harvey-Mei/BridgeOps/backend/codexradar"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/geoip"
	"github.com/Harvey-Mei/BridgeOps/backend/global"
	"github.com/Harvey-Mei/BridgeOps/backend/logger"
	"github.com/Harvey-Mei/BridgeOps/backend/migrations"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/modelprice"
	"github.com/Harvey-Mei/BridgeOps/backend/monitor"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimestatus"
	"github.com/Harvey-Mei/BridgeOps/backend/scheduler"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"github.com/Harvey-Mei/BridgeOps/web"
	"github.com/gin-gonic/gin"

	// 注册 connector 实现。
	_ "github.com/Harvey-Mei/BridgeOps/backend/connector/newapi"
	_ "github.com/Harvey-Mei/BridgeOps/backend/connector/sub2api"
)

const (
	startupFundingCandidateReconcileBatchSize = 500
	startupFundingCandidateReconcilePause     = 100 * time.Millisecond
)

type supersededFundingCandidateReconcileFunc func(context.Context, uint64, uint64, int) (storage.SupersededFundingCandidateReconcileBatch, error)

func main() {
	startedAt := time.Now().UTC()
	configPath := flag.String("config", "", "path to config.yaml (optional; env vars also supported)")
	migrateSchemaOnly := flag.Bool("migrate-schema-only", false, "apply PostgreSQL schema migrations and exit")
	flag.Parse()

	cfg, usedConfigPath, err := config.LoadWithPath(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config failed: %v\n", err)
		os.Exit(1)
	}
	if *migrateSchemaOnly {
		if err := applyPostgresSchemaOnly(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "apply PostgreSQL schema failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stdout, "PostgreSQL schema migrations applied")
		return
	}
	memoryLimits, err := config.DetectGatewayRequestAdmissionMemoryLimits()
	if err != nil {
		fmt.Fprintf(os.Stderr, "detect gateway request admission memory limit failed: %v\n", err)
		os.Exit(1)
	}
	if err := config.ValidateGatewayRequestAdmissionConfigForMemory(cfg.Gateway.RequestAdmission, memoryLimits.EffectiveLimitBytes); err != nil {
		fmt.Fprintf(os.Stderr, "validate gateway request admission memory budget failed: %v\n", err)
		os.Exit(1)
	}
	resolvedConfigPath := config.ResolvePath(*configPath, usedConfigPath)
	userTime, err := usertime.New(cfg.App.TimeZone)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load app timezone failed: %v\n", err)
		os.Exit(1)
	}

	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	log.Info("starting BridgeOps", "port", cfg.Server.Port, "mode", cfg.Server.Mode)
	clientIPGeo, clientIPGeoErr := geoip.Open(geoip.Config{
		Enabled:          cfg.GeoIP.Enabled,
		CityDatabasePath: cfg.GeoIP.CityDatabasePath,
		ASNDatabasePath:  cfg.GeoIP.ASNDatabasePath,
	})
	if clientIPGeoErr != nil {
		log.Warn("local client IP geolocation is unavailable", "err", clientIPGeoErr)
	}
	defer func() {
		if closeErr := clientIPGeo.Close(); closeErr != nil {
			log.Warn("close local client IP geolocation", "err", closeErr)
		}
	}()
	analyticsCache := analyticscache.NewNoop()
	if cfg.Redis.Enabled {
		address, addressErr := cfg.Redis.Address()
		if addressErr != nil {
			log.Warn("disable Redis analytics cache due to invalid configuration", "err", addressErr)
		} else {
			options := analyticscache.RedisOptions{
				Addr:      address,
				Namespace: analyticsCacheNamespace(cfg),
				Username:  cfg.Redis.Username,
				Password:  cfg.Redis.Password,
				DB:        cfg.Redis.DB,
			}
			if cfg.Redis.TLSEnabled {
				options.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
			}
			cache, cacheErr := analyticscache.NewRedis(options)
			if cacheErr != nil {
				log.Warn("disable Redis analytics cache", "err", cacheErr)
			} else {
				analyticsCache = cache
				log.Info("Redis analytics cache enabled", "addr", address, "db", cfg.Redis.DB, "tls", cfg.Redis.TLSEnabled)
			}
		}
	}
	defer func() {
		if closeErr := analyticsCache.Close(); closeErr != nil {
			log.Warn("close Redis analytics cache", "err", closeErr)
		}
	}()

	if _, err := os.Stat(resolvedConfigPath); errors.Is(err, os.ErrNotExist) {
		if err := config.Save(resolvedConfigPath, cfg); err != nil {
			log.Error("create config failed", "path", resolvedConfigPath, "err", err)
			os.Exit(1)
		}
		log.Info("config created", "path", resolvedConfigPath)
	}

	cipher, err := crypto.NewCipher(cfg.Security.AppSecret)
	if err != nil {
		log.Error("init cipher failed (set APP_SECRET)", "err", err)
		os.Exit(1)
	}

	// Auth：默认禁用（AUTH_ENABLED=false），所有 /api/* 免 token；
	// 显式开启时账号/密码必填，token secret 缺省回退到 AppSecret。
	var authSvc *auth.Service
	if cfg.Auth.Enabled {
		tokenSecret := cfg.Auth.TokenSecret
		if tokenSecret == "" {
			tokenSecret = cfg.Security.AppSecret
		}
		authSvc, err = auth.New(
			cfg.Auth.Username,
			cfg.Auth.Password,
			tokenSecret,
			time.Duration(cfg.Auth.SessionTTLHours)*time.Hour,
		)
		if err != nil {
			log.Error("init auth failed (set ADMIN_USERNAME / ADMIN_PASSWORD or AUTH_ENABLED=false)", "err", err)
			os.Exit(1)
		}
		log.Info("auth enabled", "username", cfg.Auth.Username)
	} else {
		log.Warn("auth disabled — all /api/* endpoints are open; set AUTH_ENABLED=true for production exposure")
	}

	databaseConfig := cfg.Database.ToStorageConfig()
	db, err := storage.Open(databaseConfig)
	if err != nil {
		log.Error("open database failed", "err", err)
		os.Exit(1)
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Error("get postgres database handle failed", "err", err)
		os.Exit(1)
	}
	if err := migrations.Apply(context.Background(), sqlDB); err != nil {
		log.Error("apply postgres migrations failed", "err", err)
		os.Exit(1)
	}
	if _, err := runtimestatus.Write(runtimestatus.Path(), global.VERSION, startedAt); err != nil {
		log.Warn("write runtime database status failed; database backups will fail closed", "err", err)
	}

	channels := storage.NewChannels(db)
	networkProxies := storage.NewNetworkProxies(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	notifies := storage.NewNotifications(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db, userTime)
	monLogs := storage.NewMonitorLogs(db)
	codexRadarStore := storage.NewCodexRadarStore(db)
	gatewayStore := storage.NewGateway(db, userTime)
	modelCheckStore := storage.NewModelChecks(db)
	costingStore := storage.NewCosting(db)
	runCtx, stopRun := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopRun()
	reconciled, reconcileErr := reconcileSupersededFundingCandidates(
		runCtx,
		startupFundingCandidateReconcileBatchSize,
		startupFundingCandidateReconcilePause,
		costingStore.ReconcileSupersededFundingCandidatesBatchContext,
	)
	if runCtx.Err() != nil {
		if reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) {
			log.Warn("startup canceled after superseded funding candidate reconciliation failed", "count", reconciled, "err", reconcileErr)
		} else {
			log.Info("startup canceled while reconciling superseded funding candidates", "count", reconciled)
		}
		return
	}
	if reconcileErr != nil {
		log.Warn("reconcile superseded funding candidates failed", "count", reconciled, "err", reconcileErr)
	} else if reconciled > 0 {
		log.Info("reconciled superseded funding candidates", "count", reconciled)
	}
	costingSvc := costing.NewService(costingStore)

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monLogs, cipher, userTime)
	channelSvc.SetNetworkProxies(networkProxies)
	channelSvc.SetCosting(costingSvc)
	channelSvc.UpdateUpstreamConfig(cfg.Upstream)
	if migration, migrationErr := channelSvc.MigrateLegacyNetworkProxyBindings(cfg.Proxy); migrationErr != nil {
		log.Warn("migrate legacy channel proxy bindings failed; affected channels fail closed until repaired", "err", migrationErr)
	} else if migration.Bound > 0 || migration.ClearedToDirect > 0 {
		log.Info("migrated legacy channel proxy bindings", "bound", migration.Bound, "cleared_to_direct", migration.ClearedToDirect)
	}
	dispatcher := notify.NewDispatcher(notifies, cipher, log, notify.Policy{
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
	monitorSvc := monitor.NewService(channels, announcements, rates, monLogs, channelSvc, dispatcher, log)
	monitorSvc.SetCosting(costingSvc)
	codexRadarSvc := codexradar.New(codexradar.Options{Logger: log, Store: codexRadarStore})
	claudeRadarSvc := clauderadar.New(clauderadar.Options{Logger: log, Store: codexRadarStore})
	gatewaySvc := gateway.NewService(channels, rates, gatewayStore, cipher, channelSvc, log)
	gatewaySvc.SetCosting(costingSvc)
	gatewaySvc.UpdateRoutingConfig(gateway.RoutingConfig{
		PriceScoreMax:             cfg.Scheduler.RoutingPriceScoreMax,
		LatencyScoreMax:           cfg.Scheduler.RoutingLatencyScoreMax,
		LatencySamples:            cfg.Scheduler.LatencyProbeSampleSize,
		LatencyWindow:             time.Duration(cfg.Scheduler.LatencyStatsWindowHours) * time.Hour,
		LatencyFullScore:          time.Duration(cfg.Scheduler.LatencyFullScoreMs) * time.Millisecond,
		LatencyZeroScore:          time.Duration(cfg.Scheduler.LatencyZeroScoreMs) * time.Millisecond,
		UnknownLatencyScoreFactor: cfg.Scheduler.UnknownLatencyScoreFactor,
		FailurePolicy: gateway.RoutingFailurePolicy{
			ResponseTimeout: time.Duration(cfg.Scheduler.RoutingFailures.ResponseTimeoutSeconds) * time.Second,
			ManualCooldown:  time.Duration(cfg.Scheduler.RoutingFailures.ManualCooldownMinutes) * time.Minute,
		},
		SessionAffinityMaxAge:        time.Duration(cfg.Scheduler.SessionAffinityMaxAgeMinutes) * time.Minute,
		SessionAffinityStableSamples: cfg.Scheduler.SessionAffinityStableSamples,
		CacheInputPriceMultiplier:    cfg.Scheduler.CacheInputPriceMultiplier,
		CachePriceMinInputTokens:     cfg.Scheduler.CachePriceMinInputTokens,
		CachePriceWindowInputTokens:  cfg.Scheduler.CachePriceWindowInputTokens,
		CacheStatsMaxAge:             time.Duration(cfg.Scheduler.CacheStatsMaxAgeDays) * 24 * time.Hour,
	})
	monitorSvc.SetGatewaySync(gatewaySvc)
	modelCheckSvc := modelcheck.New(modelcheck.Options{
		Store: modelCheckStore, Channels: channels, Runner: gatewaySvc, Dispatcher: dispatcher, Logger: log,
	})
	modelPriceSyncSvc := modelprice.New(gatewayStore, log)
	if runCtx.Err() != nil {
		log.Info("startup canceled before background services started")
		return
	}
	if err := gatewaySvc.StartRequestDispatchOwner(); err != nil {
		log.Error("start gateway request dispatch owner failed", "err", err)
		os.Exit(1)
	}
	gatewaySvc.StartCostReconciler()
	channelDeletionDone := make(chan struct{})
	go func() {
		defer close(channelDeletionDone)
		if err := gatewaySvc.RunChannelDeletionCoordinator(runCtx, gateway.DefaultChannelDeletionCoordinatorConfig()); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("channel deletion coordinator stopped", "err", err)
			stopRun()
		}
	}()
	automaticAssignmentCtx, stopAutomaticAssignment := context.WithCancel(runCtx)
	automaticAssignmentDone := make(chan struct{})
	automaticAssignmentReconciler := storage.NewGatewayAutomaticAssignmentReconciler(gatewayStore, 5*time.Minute, 500)
	go func() {
		defer close(automaticAssignmentDone)
		automaticAssignmentReconciler.Run(automaticAssignmentCtx, func(result storage.GatewayAutomaticAssignmentReconcileResult, reconcileErr error) {
			if reconcileErr != nil {
				if errors.Is(reconcileErr, context.Canceled) && automaticAssignmentCtx.Err() != nil {
					return
				}
				log.Warn("automatic group assignment reconciliation failed",
					"presence_processed", result.PresenceProcessed,
					"facts_processed", result.FactsProcessed,
					"previews_persisted", result.PreviewsPersisted,
					"err", reconcileErr)
				return
			}
			log.Debug("automatic group assignment reconciliation completed",
				"presence_processed", result.PresenceProcessed,
				"facts_processed", result.FactsProcessed,
				"previews_persisted", result.PreviewsPersisted,
				"skipped", result.Skipped)
		})
	}()
	modelPriceSyncCtx, stopModelPriceSync := context.WithCancel(runCtx)
	modelPriceSyncDone := make(chan struct{})
	go func() {
		defer close(modelPriceSyncDone)
		modelPriceSyncSvc.Run(modelPriceSyncCtx)
	}()

	schedulerFactory := func(scfg config.SchedulerConfig, pcfg config.ProxyConfig) *scheduler.Scheduler {
		gatewaySvc.UpdateRoutingConfig(gateway.RoutingConfig{
			PriceScoreMax:             scfg.RoutingPriceScoreMax,
			LatencyScoreMax:           scfg.RoutingLatencyScoreMax,
			LatencySamples:            scfg.LatencyProbeSampleSize,
			LatencyWindow:             time.Duration(scfg.LatencyStatsWindowHours) * time.Hour,
			LatencyFullScore:          time.Duration(scfg.LatencyFullScoreMs) * time.Millisecond,
			LatencyZeroScore:          time.Duration(scfg.LatencyZeroScoreMs) * time.Millisecond,
			UnknownLatencyScoreFactor: scfg.UnknownLatencyScoreFactor,
			FailurePolicy: gateway.RoutingFailurePolicy{
				ResponseTimeout: time.Duration(scfg.RoutingFailures.ResponseTimeoutSeconds) * time.Second,
				ManualCooldown:  time.Duration(scfg.RoutingFailures.ManualCooldownMinutes) * time.Minute,
			},
			SessionAffinityMaxAge:        time.Duration(scfg.SessionAffinityMaxAgeMinutes) * time.Minute,
			SessionAffinityStableSamples: scfg.SessionAffinityStableSamples,
			CacheInputPriceMultiplier:    scfg.CacheInputPriceMultiplier,
			CachePriceMinInputTokens:     scfg.CachePriceMinInputTokens,
			CachePriceWindowInputTokens:  scfg.CachePriceWindowInputTokens,
			CacheStatsMaxAge:             time.Duration(scfg.CacheStatsMaxAgeDays) * 24 * time.Hour,
		})
		service := scheduler.New(scfg, monitorSvc, monLogs, rates, notifies, announcements, captchas, cipher, gatewaySvc, pcfg, log)
		service.SetCodexRadar(codexRadarSvc)
		service.SetClaudeRadar(claudeRadarSvc)
		service.SetModelCheck(modelCheckSvc)
		return service
	}
	sch := schedulerFactory(cfg.Scheduler, cfg.Proxy)
	if err := sch.Start(); err != nil {
		log.Error("start scheduler failed", "err", err)
		os.Exit(1)
	}
	defer sch.Stop()

	runtimeMgr := runtimeconfig.New(
		resolvedConfigPath,
		cfg.Security.AppSecret,
		log,
		dispatcher,
		channelSvc,
		authSvc,
		sch,
		cfg.Proxy,
		cfg.Upstream,
		schedulerFactory,
	)
	if err := runtimeMgr.SetStartupGatewayRequestAdmission(cfg.Gateway.RequestAdmission); err != nil {
		log.Error("configure runtime gateway request admission baseline failed", "err", err)
		os.Exit(1)
	}
	runtimeMgr.SetUserTime(userTime)

	gin.SetMode(cfg.Server.Mode)
	router := gin.New()
	router.Use(gin.Recovery())
	if err := router.SetTrustedProxies(cfg.Server.TrustedProxies); err != nil {
		log.Error("configure trusted proxies failed", "err", err)
		os.Exit(1)
	}

	// 仅在嵌入了真实前端产物时挂载静态 handler。
	// 本地 `go run` 跑出来的二进制 dist 是空占位，此时由 vite dev server 接管 :3010。
	var frontendFS fs.FS
	if web.HasFrontend() {
		frontendFS = web.DistFS()
		log.Info("frontend embedded, serving SPA on /")
	} else {
		log.Info("no embedded frontend, run vite dev server separately for UI")
	}

	apiDeps := &api.Deps{
		DB:                db,
		Cipher:            cipher,
		Runtime:           runtimeMgr,
		Channels:          channels,
		NetworkProxies:    networkProxies,
		Sessions:          authSessions,
		Captchas:          captchas,
		Notifies:          notifies,
		Announcements:     announcements,
		Rates:             rates,
		Costing:           costingSvc,
		CostUnitRebaser:   monitorSvc,
		CostBasisOverride: costingSvc,
		CostUnitRepair:    costingStore,
		MonLogs:           monLogs,
		ChannelSvc:        channelSvc,
		Monitor:           monitorSvc,
		Dispatcher:        dispatcher,
		Gateway:           gatewaySvc,
		GeoIP:             clientIPGeo,
		CodexRadar:        codexRadarSvc,
		ClaudeRadar:       claudeRadarSvc,
		ModelCheck:        modelCheckSvc,
		ModelPriceSync:    modelPriceSyncSvc,
		Log:               log,
		UserTime:          userTime,
		AnalyticsCache:    analyticsCache,
		Frontend:          frontendFS,
	}
	responseSpoolDir := strings.TrimSpace(os.Getenv("BRIDGEOPS_GATEWAY_RESPONSE_SPOOL_DIR"))
	if responseSpoolDir == "" {
		responseSpoolDir = "/var/lib/bridgeops/gateway-response-spool"
	}
	removedResponseSpools, err := apiDeps.ConfigureGatewayUpstreamResponseSpool(
		api.DefaultGatewayUpstreamResponseSpoolLimits(responseSpoolDir),
		time.Now().UTC(),
	)
	if err != nil {
		log.Error("configure gateway upstream response spool failed", "err", err)
		os.Exit(1)
	}
	if removedResponseSpools > 0 {
		log.Info("removed orphaned gateway upstream response spools", "count", removedResponseSpools)
	}
	requestAdmission := cfg.Gateway.RequestAdmission
	if err := apiDeps.ConfigureGatewayRequestAdmission(api.GatewayRequestAdmissionLimits{
		MaxInFlightRequests:        requestAdmission.MaxInFlightRequests,
		MaxInFlightBodyBytes:       requestAdmission.MaxInFlightBodyBytes,
		MaxInFlightRequestsPerKey:  requestAdmission.MaxInFlightRequestsPerKey,
		MaxInFlightBodyBytesPerKey: requestAdmission.MaxInFlightBodyBytesPerKey,
		MaxQueuedRequests:          requestAdmission.MaxQueuedRequests,
		MaxQueuedRequestsPerKey:    requestAdmission.MaxQueuedRequestsPerKey,
		MaxQueueWait:               time.Duration(requestAdmission.MaxQueueWaitMilliseconds) * time.Millisecond,
	}); err != nil {
		log.Error("configure gateway request admission failed", "err", err)
		os.Exit(1)
	}
	api.Register(router, apiDeps)

	serverAddr := fmt.Sprintf(":%d", cfg.Server.Port)
	if cfg.Server.Host != "" {
		serverAddr = net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port))
	}
	srv := &http.Server{
		Addr:              serverAddr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server error", "err", err)
			os.Exit(1)
		}
	}()
	log.Info("http server listening", "addr", srv.Addr)
	backfillCtx, stopBackfill := context.WithCancel(runCtx)
	go func() {
		retryDelay := 5 * time.Second
		for {
			aggregates, aggregateErr := storage.BackfillGatewayRequestAggregates(backfillCtx, db)
			if errors.Is(aggregateErr, context.Canceled) || backfillCtx.Err() != nil {
				return
			}
			wait := 5 * time.Minute
			if aggregateErr != nil {
				log.Warn("gateway request aggregate backfill failed", "processed", aggregates, "err", aggregateErr)
				wait = retryDelay
				retryDelay = min(retryDelay*2, time.Minute)
			} else {
				retryDelay = 5 * time.Second
				if aggregates > 0 {
					log.Info("gateway request aggregate backfill completed", "processed", aggregates)
				}
			}
			timer := time.NewTimer(wait)
			select {
			case <-backfillCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()

	<-runCtx.Done()
	stopBackfill()
	stopAutomaticAssignment()
	stopModelPriceSync()
	<-modelPriceSyncDone
	<-automaticAssignmentDone
	<-channelDeletionDone
	log.Info("shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("http shutdown error", "err", err)
	} else {
		ownerStopCtx, stopOwner := context.WithTimeout(context.Background(), 5*time.Second)
		if err := gatewaySvc.StopRequestDispatchOwner(ownerStopCtx, time.Now().UTC()); err != nil {
			log.Warn("stop gateway request dispatch owner failed", "err", err)
		}
		stopOwner()
	}
	log.Info("bye")
}

func applyPostgresSchemaOnly(cfg *config.Config) error {
	if cfg == nil || !strings.EqualFold(strings.TrimSpace(cfg.Database.Driver), string(storage.DBDriverPostgres)) {
		return errors.New("database.driver must be postgres")
	}
	db, err := storage.Open(cfg.Database.ToStorageConfig())
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	defer sqlDB.Close()
	return migrations.Apply(context.Background(), sqlDB)
}

func analyticsCacheNamespace(cfg *config.Config) string {
	if cfg == nil {
		return "bridgeops"
	}
	if explicit := strings.TrimSpace(cfg.Redis.Namespace); explicit != "" {
		return explicit
	}
	identity := strings.Join([]string{
		cfg.Security.AppSecret,
		cfg.Database.Host,
		strconv.Itoa(cfg.Database.Port),
		cfg.Database.User,
		cfg.Database.Name,
	}, "\x00")
	digest := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("db-%x", digest[:8])
}

func reconcileSupersededFundingCandidates(
	ctx context.Context,
	batchSize int,
	pause time.Duration,
	reconcile supersededFundingCandidateReconcileFunc,
) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if batchSize <= 0 || batchSize > 1000 {
		return 0, fmt.Errorf("superseded funding candidate batch size must be between 1 and 1000: %d", batchSize)
	}
	if pause <= 0 {
		return 0, errors.New("superseded funding candidate batch pause must be positive")
	}
	if reconcile == nil {
		return 0, errors.New("superseded funding candidate reconciler is required")
	}

	var total int64
	var passResolved int64
	var afterID, highWaterID uint64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		batch, err := reconcile(ctx, afterID, highWaterID, batchSize)
		if err != nil {
			return total, fmt.Errorf("reconcile superseded funding candidate batch: %w", err)
		}
		if highWaterID == 0 {
			highWaterID = batch.HighWaterID
		} else if batch.HighWaterID != highWaterID {
			return total, fmt.Errorf("reconcile superseded funding candidate high-water changed from %d to %d", highWaterID, batch.HighWaterID)
		}
		if batch.Scanned < 0 || batch.Scanned > batchSize {
			return total, fmt.Errorf("reconcile superseded funding candidate batch scanned %d rows for limit %d", batch.Scanned, batchSize)
		}
		if batch.Resolved < 0 || batch.Resolved > int64(batch.Scanned) {
			return total, fmt.Errorf("reconcile superseded funding candidate batch resolved %d of %d scanned rows", batch.Resolved, batch.Scanned)
		}
		if batch.Scanned == 0 && !batch.Exhausted {
			return total, errors.New("reconcile superseded funding candidate batch made no cursor progress")
		}
		if batch.Scanned > 0 && (batch.NextID <= afterID || batch.NextID > highWaterID) {
			return total, fmt.Errorf("reconcile superseded funding candidate cursor advanced from %d to invalid id %d with high-water %d", afterID, batch.NextID, highWaterID)
		}
		if !batch.Exhausted && batch.Scanned != batchSize {
			return total, fmt.Errorf("reconcile superseded funding candidate non-final batch scanned %d rows for limit %d", batch.Scanned, batchSize)
		}
		total += batch.Resolved
		passResolved += batch.Resolved
		if err := ctx.Err(); err != nil {
			return total, err
		}
		if batch.Exhausted {
			if passResolved == 0 {
				return total, nil
			}
			// Recheck the same finite ID snapshot so evidence committed behind the
			// cursor during this pass cannot leave a now-covered candidate pending.
			afterID = 0
			passResolved = 0
		} else {
			afterID = batch.NextID
		}
		if err := waitForSupersededFundingCandidateBatch(ctx, pause); err != nil {
			return total, err
		}
	}
}

func waitForSupersededFundingCandidateBatch(ctx context.Context, pause time.Duration) error {
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
