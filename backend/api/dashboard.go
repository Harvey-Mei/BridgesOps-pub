package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

// registerDashboard 提供首页所需聚合视图。
func registerDashboard(g *gin.RouterGroup, d *Deps) {
	g.GET("/dashboard/summary", func(c *gin.Context) { dashboardSummary(c, d) })
	g.GET("/dashboard/balance-trend", func(c *gin.Context) { dashboardBalanceTrend(c, d) })
	g.GET("/dashboard/cost-trend", func(c *gin.Context) { dashboardCostTrend(c, d) })
	g.GET("/dashboard/multiplier-trend", func(c *gin.Context) { dashboardMultiplierTrend(c, d) })
}

type dashboardLowest struct {
	ChannelID uint     `json:"channel_id"`
	Name      string   `json:"name"`
	Balance   *float64 `json:"balance"`
}

type dashboardChannelStat struct {
	ID                           uint       `json:"id"`
	Name                         string     `json:"name"`
	Type                         string     `json:"type"`
	MonitorEnabled               bool       `json:"monitor_enabled"`
	BalanceThreshold             float64    `json:"balance_threshold"`
	LastBalance                  *float64   `json:"last_balance,omitempty"`
	TodayCost                    *float64   `json:"today_cost,omitempty"`
	TodayCostAt                  *time.Time `json:"today_cost_at,omitempty"`
	TodayCostIsToday             bool       `json:"today_cost_is_today"`
	TotalCost                    *float64   `json:"total_cost,omitempty"`
	LastError                    string     `json:"last_error,omitempty"`
	BalanceError                 string     `json:"balance_error,omitempty"`
	BalanceErrorAt               *time.Time `json:"balance_error_at,omitempty"`
	AccountBalanceInsufficient   bool       `json:"upstream_account_balance_insufficient,omitempty"`
	AccountBalanceInsufficientAt *time.Time `json:"upstream_account_balance_insufficient_at,omitempty"`
}

func dashboardSummary(c *gin.Context, d *Deps) {
	channels, err := d.Channels.List()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	stats := make([]dashboardChannelStat, 0, len(channels))
	var totalBalance float64
	var todayTotalCost float64
	var totalCost float64
	var lowest *dashboardLowest
	var activeCount, failedCount int
	userTime := configuredUserTime(d)
	now := time.Now()

	for _, ch := range channels {
		accountBalanceInsufficient := ch.HasCurrentUpstreamAccountBalanceInsufficient()
		var accountBalanceInsufficientAt *time.Time
		if accountBalanceInsufficient {
			accountBalanceInsufficientAt = ch.UpstreamAccountBalanceInsufficientAt
		}
		stat := dashboardChannelStat{
			ID:                           ch.ID,
			Name:                         ch.Name,
			Type:                         string(ch.Type),
			MonitorEnabled:               ch.MonitorEnabled,
			BalanceThreshold:             ch.EffectiveBalanceThreshold(),
			LastBalance:                  ch.LastBalance,
			TodayCost:                    ch.TodayCost,
			TodayCostAt:                  ch.TodayCostAt,
			TodayCostIsToday:             userTime.IsToday(ch.TodayCostAt, now),
			TotalCost:                    ch.TotalCost,
			LastError:                    ch.LastError,
			BalanceError:                 ch.BalanceError,
			BalanceErrorAt:               ch.BalanceErrorAt,
			AccountBalanceInsufficient:   accountBalanceInsufficient,
			AccountBalanceInsufficientAt: accountBalanceInsufficientAt,
		}
		stats = append(stats, stat)
		hasFailure := ch.LastError != "" || ch.BalanceError != "" || ch.HasCurrentUpstreamAccountBalanceInsufficient()
		if hasFailure {
			failedCount++
		} else if ch.MonitorEnabled {
			activeCount++
		}
		if ch.LastBalance != nil {
			totalBalance += *ch.LastBalance
			if lowest == nil || (lowest.Balance == nil) || (*ch.LastBalance < *lowest.Balance) {
				bal := *ch.LastBalance
				lowest = &dashboardLowest{ChannelID: ch.ID, Name: ch.Name, Balance: &bal}
			}
		}
		if ch.TodayCost != nil && userTime.IsToday(ch.TodayCostAt, now) {
			todayTotalCost += *ch.TodayCost
		}
		if ch.TotalCost != nil {
			totalCost += *ch.TotalCost
		}
	}

	recentChanges, err := d.Rates.ListChanges(0, 10)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data": gin.H{
			"total_channels":      len(channels),
			"active_channels":     activeCount,
			"failed_channels":     failedCount,
			"total_balance":       totalBalance,
			"today_total_cost":    todayTotalCost,
			"total_cost":          totalCost,
			"lowest_balance":      lowest,
			"channels":            stats,
			"recent_rate_changes": recentChanges,
		},
	})
}

func dashboardBalanceTrend(c *gin.Context, d *Deps) {
	days, _ := strconv.Atoi(c.DefaultQuery("days", "7"))
	if days <= 0 {
		days = 7
	}
	trend, err := d.Rates.AggregateBalanceTrend(days)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": trend})
}

func dashboardCostTrend(c *gin.Context, d *Deps) {
	days, _ := strconv.Atoi(c.DefaultQuery("days", "7"))
	if days <= 0 {
		days = 7
	}
	trend, err := d.Rates.AggregateCostTrend(days)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": trend})
}

// dashboardMultiplierTrend intentionally does not reuse request-log analytics:
// unit-price analytics derives series by model/cost attribution, while this
// endpoint must read only the multiplier and app facts frozen on dispatch.
func dashboardMultiplierTrend(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusServiceUnavailable, fmt.Errorf("gateway analytics is unavailable"))
		return
	}
	filter, err := parseGatewayRequestLogFilter(c, d)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if filter.Start == nil || filter.End == nil {
		fail(c, http.StatusBadRequest, fmt.Errorf("start and end are required"))
		return
	}
	if filter.End.Sub(*filter.Start) > storage.DashboardMultiplierTrendMaximumRange {
		fail(c, http.StatusBadRequest, fmt.Errorf("range must not exceed %s", storage.DashboardMultiplierTrendMaximumRange))
		return
	}
	trend, err := d.Gateway.Store.DashboardMultiplierTrend(*filter.Start, *filter.End, filter.TimeZone)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": trend})
}
