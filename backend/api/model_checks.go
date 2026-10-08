package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

type modelCheckTargetInput struct {
	Enabled *bool `json:"enabled" binding:"required"`
}

type modelCheckConfigInput struct {
	Config config.ModelCheckConfig `json:"config" binding:"required"`
}

type modelCheckSummary struct {
	Total       int   `json:"total"`
	Enabled     int   `json:"enabled"`
	Healthy     int   `json:"healthy"`
	Suspect     int   `json:"suspect"`
	Unavailable int   `json:"unavailable"`
	Pending     int   `json:"pending"`
	Requests24h int64 `json:"requests_24h"`
}

type modelCheckRunOutput struct {
	storage.ModelCheckRun
	Reasons  []string         `json:"reasons"`
	Evidence []map[string]any `json:"evidence"`
}

func registerModelChecks(g *gin.RouterGroup, d *Deps) {
	group := g.Group("/model-checks")
	group.GET("/overview", func(c *gin.Context) { modelCheckOverview(c, d) })
	group.GET("/manual-options", func(c *gin.Context) {
		if d.ModelCheck == nil {
			fail(c, http.StatusServiceUnavailable, errors.New("model check service is unavailable"))
			return
		}
		options, err := d.ModelCheck.ManualTargets(c.Request.Context())
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		if options == nil {
			options = []modelcheck.TargetDescriptor{}
		}
		c.JSON(http.StatusOK, gin.H{"data": options})
	})
	group.GET("/manual-models", func(c *gin.Context) {
		if d.ModelCheck == nil {
			fail(c, http.StatusServiceUnavailable, errors.New("model check service is unavailable"))
			return
		}
		channelID, err := strconv.ParseUint(c.Query("channel_id"), 10, 64)
		if err != nil || channelID == 0 {
			fail(c, http.StatusBadRequest, errors.New("invalid channel"))
			return
		}
		revision, err := strconv.ParseUint(c.Query("identity_revision"), 10, 64)
		if err != nil || revision == 0 {
			fail(c, http.StatusBadRequest, errors.New("invalid identity revision"))
			return
		}
		items, err := d.ModelCheck.ManualModels(c.Request.Context(), modelcheck.TargetDescriptor{ChannelID: uint(channelID), UpstreamAccountIdentityRevision: revision, GroupName: c.Query("group_name"), App: storage.GatewayApp(c.Query("app"))})
		if err != nil {
			fail(c, http.StatusBadGateway, err)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": items})
	})
	group.POST("/run", func(c *gin.Context) {
		if d.ModelCheck == nil || d.Runtime == nil {
			fail(c, http.StatusServiceUnavailable, errors.New("model check service is unavailable"))
			return
		}
		var selection modelcheck.TargetDescriptor
		if err := c.ShouldBindJSON(&selection); err != nil {
			fail(c, http.StatusBadRequest, err)
			return
		}
		cfg, err := loadModelCheckConfig(d)
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		if strings.Contains(c.GetHeader("Accept"), "text/event-stream") {
			streamManualModelCheck(c, d, selection, cfg)
			return
		}
		run, err := d.ModelCheck.RunManualTarget(c.Request.Context(), selection, cfg)
		if err != nil {
			status, publicErr := modelCheckRunHTTPError(err)
			if errors.Is(err, modelcheck.ErrManualTargetUnavailable) {
				status = http.StatusConflict
			}
			fail(c, status, publicErr)
			return
		}
		c.JSON(http.StatusOK, gin.H{"data": modelCheckRunView(*run)})
	})
	group.PATCH("/config", func(c *gin.Context) { updateModelCheckConfig(c, d) })
	group.PATCH("/targets/:id", func(c *gin.Context) { updateModelCheckTarget(c, d) })
	group.POST("/targets/:id/run", func(c *gin.Context) { runModelCheckTarget(c, d) })
	group.GET("/targets/:id/runs", func(c *gin.Context) { listModelCheckRuns(c, d) })
	group.GET("/runs", func(c *gin.Context) {
		if d.ModelCheck == nil {
			fail(c, http.StatusServiceUnavailable, errors.New("model check service is unavailable"))
			return
		}
		runs, err := d.ModelCheck.LatestRuns()
		if err != nil {
			fail(c, http.StatusInternalServerError, err)
			return
		}
		items := make([]modelCheckRunOutput, 0, len(runs))
		for _, run := range runs {
			items = append(items, modelCheckRunView(run))
		}
		c.JSON(http.StatusOK, gin.H{"data": items})
	})
}

func modelCheckOverview(c *gin.Context, d *Deps) {
	if d.ModelCheck == nil || d.Runtime == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("model check service is unavailable"))
		return
	}
	cfg, err := loadModelCheckConfig(d)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	refresh := c.Query("refresh") == "1"
	targets, err := d.ModelCheck.ListTargets(c.Request.Context(), refresh)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	requests, err := d.ModelCheck.RequestsUsed24h()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	summary := summarizeModelCheckTargets(targets)
	summary.Requests24h = requests
	targets = modelCheckTargetsView(targets)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"config":  modelcheck.NormalizeConfig(cfg),
		"summary": summary,
		"targets": targets,
	}})
}

func updateModelCheckConfig(c *gin.Context, d *Deps) {
	if d.Runtime == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("runtime config is unavailable"))
		return
	}
	var input modelCheckConfigInput
	if err := c.ShouldBindJSON(&input); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if err := validateModelCheckConfig(input.Config); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	settingsConfigMu.Lock()
	cfg, err := config.LoadFile(d.Runtime.ConfigPath())
	if err == nil {
		cfg.Scheduler.ModelCheck = input.Config
		err = config.Save(d.Runtime.ConfigPath(), cfg)
	}
	settingsConfigMu.Unlock()
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	applyResult, err := d.Runtime.ApplyFromFile()
	if err != nil {
		fail(c, http.StatusInternalServerError, fmt.Errorf("configuration was saved but could not be applied: %w", err))
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"config": input.Config,
		"apply":  applyResult,
	}})
}

func updateModelCheckTarget(c *gin.Context, d *Deps) {
	if d.ModelCheck == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("model check service is unavailable"))
		return
	}
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	var input modelCheckTargetInput
	if err := c.ShouldBindJSON(&input); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	if input.Enabled == nil {
		fail(c, http.StatusBadRequest, errors.New("enabled is required"))
		return
	}
	if err := d.ModelCheck.SetTargetEnabled(id, *input.Enabled); err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"id": id, "enabled": *input.Enabled}})
}

func runModelCheckTarget(c *gin.Context, d *Deps) {
	if d.ModelCheck == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("model check service is unavailable"))
		return
	}
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	cfg, err := loadModelCheckConfig(d)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	run, err := d.ModelCheck.RunTarget(c.Request.Context(), id, cfg)
	if err != nil {
		status, publicErr := modelCheckRunHTTPError(err)
		fail(c, status, publicErr)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": modelCheckRunView(*run)})
}

func modelCheckRunHTTPError(err error) (int, error) {
	if errors.Is(err, modelcheck.ErrRequestBudgetExceeded) {
		return http.StatusTooManyRequests, err
	}
	if errors.Is(err, modelcheck.ErrProbeDeferred) {
		return http.StatusTooManyRequests, errors.New("本机当前为在线请求保留上游容量，本次模型检测已延后，请稍后重试")
	}
	return http.StatusBadGateway, err
}

func listModelCheckRuns(c *gin.Context, d *Deps) {
	if d.ModelCheck == nil {
		fail(c, http.StatusServiceUnavailable, errors.New("model check service is unavailable"))
		return
	}
	id, err := uintParam(c, "id")
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	page := positiveQueryInt(c.Query("page"), 1)
	pageSize := positiveQueryInt(c.Query("page_size"), 20)
	if pageSize > 100 {
		pageSize = 100
	}
	runs, total, err := d.ModelCheck.ListRunsPage(id, page, pageSize)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	items := make([]modelCheckRunOutput, 0, len(runs))
	for i := range runs {
		items = append(items, modelCheckRunView(runs[i]))
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"items": items, "total": total, "page": page, "page_size": pageSize,
	}})
}

func loadModelCheckConfig(d *Deps) (config.ModelCheckConfig, error) {
	settingsConfigMu.RLock()
	cfg, err := config.LoadFile(d.Runtime.ConfigPath())
	settingsConfigMu.RUnlock()
	if err != nil {
		return config.ModelCheckConfig{}, err
	}
	return modelcheck.NormalizeConfig(cfg.Scheduler.ModelCheck), nil
}

func validateModelCheckConfig(value config.ModelCheckConfig) error {
	checks := []struct {
		name       string
		value, min int
		max        int
	}{
		{"runsPerDay", value.RunsPerDay, 1, 12},
		{"concurrency", value.Concurrency, 1, 4},
		{"maxTargetsPerRun", value.MaxTargetsPerRun, 1, 100},
		{"probesPerTarget", value.ProbesPerTarget, 2, 2},
		{"requestBudget24h", value.RequestBudget24h, 2, 2000},
		{"timeoutSeconds", value.TimeoutSeconds, 10, 180},
		{"pelicanTimeoutSeconds", value.PelicanTimeoutSeconds, 60, 1800},
		{"alertFailures", value.AlertFailures, 1, 3},
		{"historyDays", value.HistoryDays, 7, 365},
	}
	for _, check := range checks {
		if check.value < check.min || check.value > check.max {
			return fmt.Errorf("%s must be between %d and %d", check.name, check.min, check.max)
		}
	}
	if value.RequestBudget24h < value.ProbesPerTarget {
		return errors.New("requestBudget24h must cover at least one target run")
	}
	return nil
}

func summarizeModelCheckTargets(targets []storage.ModelCheckTarget) modelCheckSummary {
	var summary modelCheckSummary
	for i := range targets {
		target := targets[i]
		summary.Total++
		if !target.Enabled {
			continue
		}
		summary.Enabled++
		switch target.LastStatus {
		case storage.ModelCheckTargetHealthy:
			summary.Healthy++
		case storage.ModelCheckTargetSuspect:
			summary.Suspect++
		case storage.ModelCheckTargetUnavailable:
			summary.Unavailable++
		default:
			summary.Pending++
		}
	}
	return summary
}

func modelCheckRunView(run storage.ModelCheckRun) modelCheckRunOutput {
	run.Error = modelcheck.SanitizeProbeErrorDiagnostics(run.Error)
	view := modelCheckRunOutput{ModelCheckRun: run, Reasons: []string{}, Evidence: []map[string]any{}}
	_ = json.Unmarshal([]byte(run.AnomalyReasons), &view.Reasons)
	_ = json.Unmarshal([]byte(run.EvidenceJSON), &view.Evidence)
	for i := range view.Evidence {
		if raw, ok := view.Evidence[i]["artifact_svg"].(string); ok {
			if svg, err := modelcheck.SafePelicanSVG(raw); err == nil {
				view.Evidence[i]["artifact_svg"] = svg
			} else {
				delete(view.Evidence[i], "artifact_svg")
			}
		} else {
			delete(view.Evidence[i], "artifact_svg")
		}
		if value, ok := view.Evidence[i]["error"].(string); ok {
			view.Evidence[i]["error"] = modelcheck.SanitizeProbeErrorDiagnostic(value)
		} else {
			delete(view.Evidence[i], "error")
		}
	}
	return view
}

func modelCheckTargetsView(targets []storage.ModelCheckTarget) []storage.ModelCheckTarget {
	for i := range targets {
		targets[i].LastError = modelcheck.SanitizeProbeErrorDiagnostics(targets[i].LastError)
	}
	return targets
}

func positiveQueryInt(raw string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}
