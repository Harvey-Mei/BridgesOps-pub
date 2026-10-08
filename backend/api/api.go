// Package api 注册所有 HTTP 路由，组装各业务 handler。
//
// 单用户场景下走 HMAC token 鉴权：账号密码写在 config 里，登录后下发 token。
package api

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/Harvey-Mei/BridgeOps/backend/analyticscache"
	"github.com/Harvey-Mei/BridgeOps/backend/channel"
	"github.com/Harvey-Mei/BridgeOps/backend/clauderadar"
	"github.com/Harvey-Mei/BridgeOps/backend/codexradar"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/geoip"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/modelprice"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type gatewayClientIPGeoLookup interface {
	Lookup(rawIP string) (geoip.Result, error)
}

type monitorService interface {
	RefreshBalance(ctx context.Context, c *storage.Channel) error
	RefreshRates(ctx context.Context, c *storage.Channel) error
	RefreshUpstreamGroupFacts(ctx context.Context, c *storage.Channel) error
	CheckSubscriptionUsageAlerts(ctx context.Context, c *storage.Channel) error
}

// assignmentPreflightMonitor is an optional fast path implemented by the
// monitor service. It refreshes the authoritative group facts without
// running the full managed-key inventory consumer; assignment preflight
// maintains only the groups being assigned afterwards. Keeping this separate
// preserves the existing monitorService contract for test doubles and other
// callers that need the complete refresh semantics.
type assignmentPreflightMonitor interface {
	RefreshUpstreamGroupFactsForAssignment(ctx context.Context, c *storage.Channel) error
}

type channelCostUnitRebaser interface {
	RebaseCostUnit(context.Context, uint, costing.CostUnitRebaseCommand) (*costing.CostUnitRebaseResult, error)
}

type channelCostBasisOverrider interface {
	OverrideCostBasis(uint, costing.CostBasisOverrideCommand) (*costing.CostBasisOverrideResult, error)
}

type gatewayModelPriceSynchronizer interface {
	SyncNow(context.Context) (modelprice.Result, error)
}

type channelCostUnitRepairAdopter interface {
	PreviewCostUnitRepairAdoption(storage.CostUnitRepairAdoptionAnchor) (*storage.CostUnitRepairAdoptionPreview, error)
	ApplyCostUnitRepairAdoption(storage.CostUnitRepairAdoptionAnchor, string, string, string) (*storage.ChannelCostUnitRepairAdoption, bool, error)
}

type channelService interface {
	Create(in channel.CreateInput) (*storage.Channel, error)
	Update(id uint, in channel.UpdateInput) (*storage.Channel, error)
	Delete(ctx context.Context, id uint) error
	ClearLoginInfo(id uint) (*storage.Channel, error)
	TestLogin(ctx context.Context, channelID uint) error
	RedeemCode(ctx context.Context, channelID uint, code string) (*connector.RedeemResult, error)
	GetRechargeInfo(ctx context.Context, channelID uint) (*connector.RechargeInfo, error)
	CreateRecharge(ctx context.Context, channelID uint, req connector.RechargeRequest) (*connector.RechargeLaunch, error)
	GetSubscriptionInfo(ctx context.Context, channelID uint) (*connector.SubscriptionInfo, error)
	CreateSubscription(ctx context.Context, channelID uint, req connector.SubscriptionRequest) (*connector.SubscriptionLaunch, error)
	GetSubscriptionUsage(ctx context.Context, channelID uint) (*connector.SubscriptionUsageInfo, error)
	ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error)
	ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error)
	CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error)
	UpdateAPIKey(ctx context.Context, channelID uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error)
	DeleteAPIKey(ctx context.Context, channelID uint, keyID int64) error
	RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error)
}

type contextChannelLifecycle interface {
	DeleteContext(ctx context.Context, id uint) error
	ClearLoginInfoContext(ctx context.Context, id uint) (*storage.Channel, error)
}

// Deps 把所有 handler 需要的依赖打包传入。
type Deps struct {
	DB                *gorm.DB
	Cipher            *crypto.Cipher
	Runtime           *runtimeconfig.Manager
	Channels          *storage.Channels
	NetworkProxies    *storage.NetworkProxies
	Sessions          *storage.AuthSessions
	Captchas          *storage.Captchas
	Notifies          *storage.Notifications
	Announcements     *storage.UpstreamAnnouncements
	Rates             *storage.Rates
	Costing           *costing.Service
	MonLogs           *storage.MonitorLogs
	ChannelSvc        channelService
	Monitor           monitorService
	CostUnitRebaser   channelCostUnitRebaser
	CostBasisOverride channelCostBasisOverrider
	CostUnitRepair    channelCostUnitRepairAdopter
	Dispatcher        *notify.Dispatcher
	Gateway           *gateway.Service
	GeoIP             gatewayClientIPGeoLookup
	CodexRadar        *codexradar.Service
	ClaudeRadar       *clauderadar.Service
	ModelCheck        *modelcheck.Service
	ModelPriceSync    gatewayModelPriceSynchronizer
	Log               *slog.Logger
	UserTime          *usertime.Service
	AnalyticsCache    analyticscache.JSONCache

	// Tests may inject an isolated admission controller. cmd/server installs a
	// normalized process-level instance before registering handlers.
	gatewayRequestAdmissionOnce sync.Once
	gatewayRequestAdmission     *gatewayRequestAdmission

	// gatewayAuthenticate is an internal data-plane test seam. Production
	// construction leaves it nil and gatewayProxy uses Gateway.Authenticate.
	gatewayAuthenticate func(string) (*storage.GatewayDownstreamKey, error)

	// Successful non-streaming upstream responses use one process-level,
	// disk-backed spool controller. cmd/server installs it during startup after
	// preparing the dedicated directory and cleaning old owned files.
	gatewayResponseSpoolOnce sync.Once
	gatewayResponseSpool     *gatewayUpstreamResponseSpoolController
	gatewayResponseSpoolErr  error

	// Frontend 可选：传入嵌入的前端 dist 文件系统。nil 表示不挂载（本地开发用 vite dev server）。
	Frontend fs.FS
}

func gatewayRequestAdmissionForDeps(d *Deps) *gatewayRequestAdmission {
	if d == nil {
		panic("api: gateway request admission requires dependencies")
	}
	d.gatewayRequestAdmissionOnce.Do(func() {
		if d.gatewayRequestAdmission == nil {
			d.gatewayRequestAdmission = mustNewGatewayRequestAdmission(gatewayRequestAdmissionFallbackLimits())
		}
	})
	return d.gatewayRequestAdmission
}

// Register 把所有路由挂到给定 gin engine。
func Register(r *gin.Engine, d *Deps) {
	r.GET("/healthz", func(c *gin.Context) {
		sqlDB, err := d.DB.DB()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"status": "down"})
			return
		}
		if err := sqlDB.Ping(); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "db_down"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	if d.Gateway != nil {
		registerGatewayProxy(r, d)
	}

	api := r.Group("/api")
	if d.Runtime != nil {
		api.Use(d.Runtime.AuthMiddleware())
	}
	{
		registerVersion(api, d)
		registerAuth(api, d)
		registerChannels(api, d)
		registerNetworkProxies(api, d)
		registerCaptchas(api, d)
		registerNotifications(api, d)
		registerAnnouncements(api, d)
		registerRates(api, d)
		registerMonitorLogs(api, d)
		registerDashboard(api, d)
		registerSettings(api, d)
		registerCodexRadar(api, d)
		registerClaudeRadar(api, d)
		registerModelChecks(api, d)
		if d.Gateway != nil {
			registerGateway(api, d)
		}
	}

	if d.Frontend != nil {
		registerFrontend(r, d.Frontend, d.Runtime)
	}
}

// registerFrontend 把嵌入的前端 dist 挂在根路径，并处理 SPA fallback：
//
//   - GET /assets/*  → 直接返回文件
//   - GET /          → 返回 index.html
//   - GET /channels  → 返回 index.html（React Router 客户端路由）
//
// /api/*、/healthz 都已被前面的具体路由占了，不会走到这里。
// 安全起见仍然做一次前缀拦截，避免任何意外情况下"未鉴权读 index.html"压到 /api 上。
func registerFrontend(r *gin.Engine, dist fs.FS, authProvider frontendAuthProvider) {
	fileServer := http.FileServer(http.FS(dist))
	registerFrontendLogin(r, authProvider)

	r.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path

		// 永远不让 SPA fallback 覆盖 API / 健康检查路径。
		if strings.HasPrefix(path, "/api/") || strings.HasPrefix(path, "/v1/") || path == "/v1" || path == "/healthz" {
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		if !frontendRequestAuthorized(c.Request, authProvider) {
			rejectFrontendRequest(c)
			return
		}
		if frontendAuthenticationEnabled(authProvider) {
			c.Writer.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			c.Writer.Header().Set("Vary", "Cookie, Authorization")
		}

		// 文件存在就直接 serve，否则回落到 index.html。
		clean := strings.TrimPrefix(path, "/")
		if clean == "" {
			clean = "index.html"
		}
		if strings.HasPrefix(clean, "assets/") {
			if _, err := fs.Stat(dist, clean); err != nil {
				if strings.HasSuffix(clean, ".js") {
					c.Writer.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
					c.Data(http.StatusOK, "text/javascript; charset=utf-8", []byte(missingAssetReloadScript))
					return
				}
				if strings.HasSuffix(clean, ".css") {
					c.Writer.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
					c.Data(http.StatusOK, "text/css; charset=utf-8", []byte(missingAssetStylesheet))
					return
				}
				c.AbortWithStatus(http.StatusNotFound)
				return
			}
			if !frontendAuthenticationEnabled(authProvider) {
				c.Writer.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
			}
		} else if _, err := fs.Stat(dist, clean); err != nil {
			c.Request.URL.Path = "/"
			c.Writer.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		} else if clean == "index.html" {
			c.Writer.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		}
		fileServer.ServeHTTP(c.Writer, c.Request)
	})
}

const missingAssetReloadScript = `
try {
  const key = "bridgeops:missing-asset-reload-at";
  const now = Date.now();
  const last = Number(window.sessionStorage.getItem(key) || 0);
  if (!Number.isFinite(last) || now - last > 30000) {
    window.sessionStorage.setItem(key, String(now));
    window.location.reload();
  } else {
    window.addEventListener("DOMContentLoaded", function () {
      document.body.innerHTML = '<div style="font:14px system-ui,sans-serif;padding:24px;color:#111">前端资源已更新，请刷新页面。</div>';
    });
  }
} catch (_) {
  window.location.reload();
}
export {};
`

const missingAssetStylesheet = `
/* BridgeOps asset was replaced during deploy. Keep old cached HTML renderable until JS reloads the page. */
`

// fail 统一错误响应。
func fail(c *gin.Context, status int, err error) {
	if errors.Is(err, storage.ErrChannelDeletionPending) {
		status = http.StatusConflict
	}
	c.JSON(status, gin.H{"error": err.Error()})
}
