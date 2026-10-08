package api

import (
	"errors"
	"net/http"

	"github.com/Harvey-Mei/BridgeOps/backend/clauderadar"
	"github.com/gin-gonic/gin"
)

var errClaudeRadarUnavailable = errors.New("claude radar service is unavailable")

func registerClaudeRadar(g *gin.RouterGroup, d *Deps) {
	g.GET("/claude-radar", func(c *gin.Context) {
		if d.ClaudeRadar == nil {
			c.JSON(http.StatusOK, gin.H{"data": clauderadar.Snapshot{}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": d.ClaudeRadar.Snapshot()})
	})

	g.POST("/claude-radar/refresh", func(c *gin.Context) {
		if d.ClaudeRadar == nil {
			fail(c, http.StatusNotFound, errClaudeRadarUnavailable)
			return
		}
		snapshot, err := d.ClaudeRadar.Refresh(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"data": snapshot, "refresh_error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": snapshot})
	})
}
