package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func gatewayRequestLogGroupCacheRatios(c *gin.Context, d *Deps) {
	pageSize := 50
	if raw := strings.TrimSpace(c.Query("page_size")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			fail(c, http.StatusBadRequest, fmt.Errorf("page_size invalid"))
			return
		}
		pageSize = parsed
	}
	filter, err := parseGatewayRequestLogFilter(c, d)
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	page, err := d.Gateway.Store.ListRequestLogGroupCacheRatioCursorPage(c.Request.Context(), filter, c.Query("cursor"), pageSize)
	if err != nil {
		if errors.Is(err, storage.ErrGatewayRequestGroupCachePageInvalid) || errors.Is(err, storage.ErrGatewayRequestGroupCacheCursorInvalid) {
			fail(c, http.StatusBadRequest, err)
			return
		}
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": page})
}
