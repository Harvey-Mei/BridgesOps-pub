package api

import (
	"errors"
	"net/http"

	"github.com/Harvey-Mei/BridgeOps/backend/codexradar"
	"github.com/gin-gonic/gin"
)

var errCodexRadarUnavailable = errors.New("codex radar service is unavailable")

func registerCodexRadar(g *gin.RouterGroup, d *Deps) {
	g.GET("/codex-radar", func(c *gin.Context) {
		if d.CodexRadar == nil {
			c.JSON(http.StatusOK, gin.H{"data": codexradar.Snapshot{}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": d.CodexRadar.Snapshot()})
	})

	g.POST("/codex-radar/refresh", func(c *gin.Context) {
		if d.CodexRadar == nil {
			fail(c, http.StatusNotFound, errCodexRadarUnavailable)
			return
		}
		snapshot, err := d.CodexRadar.Refresh(c.Request.Context())
		if err != nil {
			// The service deliberately retains the last successful payload. Return
			// it as a successful response so clients can render stale data and show
			// the refresh error alongside it instead of discarding the response body.
			c.JSON(http.StatusOK, gin.H{"data": snapshot, "refresh_error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": snapshot})
	})
}
