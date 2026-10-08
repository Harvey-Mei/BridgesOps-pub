package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func registerRates(g *gin.RouterGroup, d *Deps) {
	g.GET("/rate-change-analytics", func(c *gin.Context) {
		days, err := strconv.Atoi(c.DefaultQuery("days", "30"))
		if err != nil || (days != 7 && days != 30 && days != 90) {
			fail(c, http.StatusBadRequest, fmt.Errorf("days must be 7, 30, or 90"))
			return
		}
		var channelID uint
		if raw := c.Query("channel_id"); raw != "" {
			value, parseErr := strconv.ParseUint(raw, 10, 64)
			if parseErr != nil {
				fail(c, http.StatusBadRequest, parseErr)
				return
			}
			channelID = uint(value)
		}
		analytics, analyticsErr := d.Rates.RateChangeAnalyticsForDays(channelID, days, 20)
		if analyticsErr != nil {
			fail(c, http.StatusInternalServerError, analyticsErr)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": analytics})
	})
	g.GET("/rate-changes", func(c *gin.Context) {
		var channelID uint
		if s := c.Query("channel_id"); s != "" {
			id, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				fail(c, http.StatusBadRequest, err)
				return
			}
			channelID = uint(id)
		}
		page, pageSize, err := parsePageQuery(c)
		if err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		list, total, err := d.Rates.ListChangesPage(channelID, page, pageSize)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		pages := 1
		if total > 0 {
			pages = int((total + int64(pageSize) - 1) / int64(pageSize))
		}
		c.JSON(http.StatusOK, gin.H{"data": gin.H{
			"items":     list,
			"total":     total,
			"page":      page,
			"page_size": pageSize,
			"pages":     pages,
		}})
	})
}
