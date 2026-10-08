// Package modelcheck runs local, self-maintained model-integrity probes.
// It never sends an API key or challenge response to a third-party verifier.
package modelcheck

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/notify"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const (
	probeVersion       = "pelican-v1"
	recentWindowSize   = 3
	baselineMinSamples = 3
	baselineDropPoints = 25.0
	maxEvidenceText    = 2048
	// Model checks are control-plane work, but their notification can still
	// block an account identity switch. Keep the shared channel lock bounded.
	modelCheckIdentityDispatchTimeout = 30 * time.Second
)

var (
	ErrRequestBudgetExceeded = errors.New("model check request budget exhausted")
	// ErrProbeDeferred means the runner skipped the current upstream request
	// because a local low-priority capacity budget was unavailable. Earlier
	// probes in the same target run may already have executed; those requests are
	// audited, but a deferred run must never advance target health state.
	ErrProbeDeferred = errors.New("model check probe deferred by local capacity budget")
)

type TargetDescriptor struct {
	ChannelID                       uint               `json:"channel_id"`
	UpstreamAccountIdentityRevision uint64             `json:"identity_revision"`
	ChannelName                     string             `json:"channel_name"`
	GroupName                       string             `json:"group_name"`
	ModelName                       string             `json:"model_name"`
	App                             storage.GatewayApp `json:"app"`
}

type ManualTargetRunner interface {
	DiscoverManualTargets(context.Context, time.Time) ([]TargetDescriptor, error)
}

type ManualModelRunner interface {
	ReadManualModelTargets(context.Context, TargetDescriptor) ([]TargetDescriptor, error)
}

func (s *Service) ManualModels(ctx context.Context, selected TargetDescriptor) ([]TargetDescriptor, error) {
	runner, ok := s.runner.(ManualModelRunner)
	if !ok {
		return nil, errors.New("manual model inventory is unavailable")
	}
	return runner.ReadManualModelTargets(ctx, selected)
}

var ErrManualTargetUnavailable = errors.New("所选渠道、分组或模型已不可用，请刷新后重新选择")

func (s *Service) ManualTargets(ctx context.Context) ([]TargetDescriptor, error) {
	runner, ok := s.runner.(ManualTargetRunner)
	if !ok {
		return nil, errors.New("manual model checks are unavailable")
	}
	return runner.DiscoverManualTargets(ctx, s.now())
}

func (s *Service) RunManualTarget(ctx context.Context, selected TargetDescriptor, raw config.ModelCheckConfig) (*storage.ModelCheckRun, error) {
	options, err := s.ManualModels(ctx, selected)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		if selected.ChannelID != option.ChannelID || selected.UpstreamAccountIdentityRevision != option.UpstreamAccountIdentityRevision || selected.GroupName != option.GroupName || selected.ModelName != option.ModelName || selected.App != option.App {
			continue
		}
		target, err := s.store.EnsureManualTarget(storage.ModelCheckTargetDiscovery{
			ChannelID: option.ChannelID, UpstreamAccountIdentityRevision: option.UpstreamAccountIdentityRevision,
			ChannelName: option.ChannelName, GroupName: option.GroupName, ModelName: option.ModelName, App: option.App,
		}, s.now())
		if err != nil {
			return nil, err
		}
		if !target.Enabled {
			return nil, errors.New("该检测目标已停用，请先启用")
		}
		// Manual eligibility was freshly checked above. A new model remains
		// inactive for automatic discovery, so selecting it cannot expand the
		// scheduled workload or make internal probe traffic a discovery source.
		return s.executeTarget(ctx, *target, NormalizeConfig(raw))
	}
	return nil, ErrManualTargetUnavailable
}

type ProbeRequest struct {
	Kind      string
	Prompt    string
	MaxTokens int
}

type ProbeResponse struct {
	StatusCode          int
	Text                string
	ActualModel         string
	PromptTokens        int64
	CompletionTokens    int64
	CacheReadTokens     int64
	CacheCreationTokens int64
	TotalTokens         int64
	FirstByteMS         int64
	LatencyMS           int64
}

const (
	ProbeErrorUnknown           = "probe_error"
	ProbeErrorTargetUnavailable = "target_unavailable"
	ProbeErrorUpstreamKey       = "upstream_key_unavailable"
	ProbeErrorProxy             = "proxy_error"
	ProbeErrorRequest           = "request_error"
	ProbeErrorNetwork           = "network_error"
	ProbeErrorHTTPStatus        = "http_status"
	ProbeErrorInvalidResponse   = "invalid_response"
	ProbeErrorTimeout           = "timeout"
	ProbeErrorCanceled          = "canceled"
)

// ProbeError carries only a stable category and an optional HTTP status.
// Its message deliberately excludes upstream, proxy, and transport details.
type ProbeError struct {
	Category   string
	StatusCode int
}

func NewProbeError(category string, statusCode int) *ProbeError {
	return &ProbeError{Category: normalizeProbeErrorCategory(category), StatusCode: normalizeProbeErrorStatus(statusCode)}
}

func (e *ProbeError) Error() string {
	if e == nil {
		return ProbeErrorUnknown
	}
	return formatProbeErrorDiagnostic(e.Category, e.StatusCode)
}

// ProbeErrorDiagnostic converts any runner error into the only format that
// model-check persistence and administrator responses may expose.
func ProbeErrorDiagnostic(err error, statusCode int) string {
	if err == nil {
		return ""
	}
	category := ProbeErrorUnknown
	statusCode = normalizeProbeErrorStatus(statusCode)
	var probeErr *ProbeError
	switch {
	case errors.As(err, &probeErr):
		category = normalizeProbeErrorCategory(probeErr.Category)
		if normalized := normalizeProbeErrorStatus(probeErr.StatusCode); normalized != 0 {
			statusCode = normalized
		}
	case errors.Is(err, context.DeadlineExceeded):
		category = ProbeErrorTimeout
	case errors.Is(err, context.Canceled):
		category = ProbeErrorCanceled
	}
	return formatProbeErrorDiagnostic(category, statusCode)
}

// SanitizeProbeErrorDiagnostic prevents legacy rows from returning arbitrary
// error text. Only diagnostics emitted by ProbeErrorDiagnostic survive.
func SanitizeProbeErrorDiagnostic(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	for _, category := range probeErrorCategories {
		if value == category {
			return category
		}
		prefix := category + " (HTTP "
		if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, ")") {
			continue
		}
		statusCode, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(value, prefix), ")"))
		if err == nil && normalizeProbeErrorStatus(statusCode) != 0 {
			return formatProbeErrorDiagnostic(category, statusCode)
		}
	}
	return ProbeErrorUnknown
}

// SanitizeProbeErrorDiagnostics handles the newline-delimited diagnostics
// stored for a multi-probe run without allowing an unsafe legacy line through.
func SanitizeProbeErrorDiagnostics(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	diagnostics := make([]string, 0, 2)
	for _, item := range strings.FieldsFunc(value, func(r rune) bool { return r == '\r' || r == '\n' }) {
		diagnostic := SanitizeProbeErrorDiagnostic(item)
		diagnostics = appendUnique(diagnostics, diagnostic)
	}
	return strings.Join(diagnostics, "\n")
}

var probeErrorCategories = []string{
	ProbeErrorUnknown,
	ProbeErrorTargetUnavailable,
	ProbeErrorUpstreamKey,
	ProbeErrorProxy,
	ProbeErrorRequest,
	ProbeErrorNetwork,
	ProbeErrorHTTPStatus,
	ProbeErrorInvalidResponse,
	ProbeErrorTimeout,
	ProbeErrorCanceled,
}

func normalizeProbeErrorCategory(category string) string {
	category = strings.TrimSpace(category)
	for _, known := range probeErrorCategories {
		if category == known {
			return category
		}
	}
	return ProbeErrorUnknown
}

func normalizeProbeErrorStatus(statusCode int) int {
	if statusCode < http.StatusContinue || statusCode > 599 {
		return 0
	}
	return statusCode
}

func formatProbeErrorDiagnostic(category string, statusCode int) string {
	category = normalizeProbeErrorCategory(category)
	statusCode = normalizeProbeErrorStatus(statusCode)
	if statusCode == 0 {
		return category
	}
	return fmt.Sprintf("%s (HTTP %d)", category, statusCode)
}

type Runner interface {
	DiscoverTargets(ctx context.Context, now time.Time) ([]TargetDescriptor, error)
	RunProbe(ctx context.Context, target TargetDescriptor, request ProbeRequest) (ProbeResponse, error)
}

type Options struct {
	Store *storage.ModelChecks
	// Channels provides the cross-instance channel-operation lock and the
	// current-account revision fence used immediately before notifications.
	// It is mandatory when Dispatcher is configured; without it notifications
	// fail closed rather than crossing an account identity boundary.
	Channels   *storage.Channels
	Runner     Runner
	Dispatcher *notify.Dispatcher
	Logger     *slog.Logger
	Random     io.Reader
	Now        func() time.Time
}

type Service struct {
	store      *storage.ModelChecks
	channels   *storage.Channels
	runner     Runner
	dispatcher *notify.Dispatcher
	log        *slog.Logger
	random     io.Reader
	now        func() time.Time
	scheduleMu sync.Mutex
	syncMu     sync.Mutex
	locksMu    sync.Mutex
	locks      map[uint]chan struct{}
	runMu      sync.Mutex
	activeRuns int
	runChanged chan struct{}
	budgetMu   sync.Mutex
	reserved   int64
}

type BatchResult struct {
	Targets   int `json:"targets"`
	Passed    int `json:"passed"`
	Anomalies int `json:"anomalies"`
	Errors    int `json:"errors"`
	Requests  int `json:"requests"`
	Deferred  int `json:"deferred"`
}

func New(options Options) *Service {
	randomSource := options.Random
	if randomSource == nil {
		randomSource = rand.Reader
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &Service{
		store:      options.Store,
		channels:   options.Channels,
		runner:     options.Runner,
		dispatcher: options.Dispatcher,
		log:        options.Logger,
		random:     randomSource,
		now:        now,
		locks:      make(map[uint]chan struct{}),
		runChanged: make(chan struct{}),
	}
}

func NormalizeConfig(value config.ModelCheckConfig) config.ModelCheckConfig {
	if value.RunsPerDay < 1 {
		value.RunsPerDay = 3
	}
	if value.RunsPerDay > 12 {
		value.RunsPerDay = 12
	}
	if value.Concurrency < 1 {
		value.Concurrency = 2
	}
	if value.Concurrency > 4 {
		value.Concurrency = 4
	}
	if value.MaxTargetsPerRun < 1 {
		value.MaxTargetsPerRun = 12
	}
	if value.MaxTargetsPerRun > 100 {
		value.MaxTargetsPerRun = 100
	}
	value.ProbesPerTarget = availableProbeCount()
	if value.RequestBudget24h < 1 {
		value.RequestBudget24h = 72
	}
	if value.RequestBudget24h > 2000 {
		value.RequestBudget24h = 2000
	}
	if value.TimeoutSeconds < 10 {
		value.TimeoutSeconds = 60
	}
	if value.TimeoutSeconds > 180 {
		value.TimeoutSeconds = 180
	}
	if value.PelicanTimeoutSeconds < 60 {
		value.PelicanTimeoutSeconds = 600
	}
	if value.PelicanTimeoutSeconds > 1800 {
		value.PelicanTimeoutSeconds = 1800
	}
	if value.AlertFailures < 1 {
		value.AlertFailures = 2
	}
	if value.AlertFailures > recentWindowSize {
		value.AlertFailures = recentWindowSize
	}
	if value.HistoryDays < 7 {
		value.HistoryDays = 90
	}
	if value.HistoryDays > 365 {
		value.HistoryDays = 365
	}
	return value
}

func (s *Service) SyncTargets(ctx context.Context) ([]storage.ModelCheckTarget, error) {
	if s == nil || s.store == nil || s.runner == nil {
		return nil, errors.New("model check service is unavailable")
	}
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	now := s.now()
	descriptors, err := s.runner.DiscoverTargets(ctx, now)
	if err != nil {
		return nil, err
	}
	discovered := make([]storage.ModelCheckTargetDiscovery, 0, len(descriptors))
	for _, descriptor := range descriptors {
		discovered = append(discovered, storage.ModelCheckTargetDiscovery{
			ChannelID:                       descriptor.ChannelID,
			UpstreamAccountIdentityRevision: descriptor.UpstreamAccountIdentityRevision,
			ChannelName:                     descriptor.ChannelName,
			GroupName:                       descriptor.GroupName,
			ModelName:                       descriptor.ModelName,
			App:                             descriptor.App,
		})
	}
	if err := s.store.TrimResultEvidence(); err != nil {
		return nil, err
	}
	return s.store.SyncTargets(discovered, now)
}

func (s *Service) ListTargets(ctx context.Context, refresh bool) ([]storage.ModelCheckTarget, error) {
	if refresh {
		return s.SyncTargets(ctx)
	}
	return s.store.ListTargets(true)
}

func (s *Service) SetTargetEnabled(id uint, enabled bool) error {
	return s.store.SetTargetEnabled(id, enabled)
}

func (s *Service) ListRunsPage(targetID uint, page, pageSize int) ([]storage.ModelCheckRun, int64, error) {
	return s.store.ListRunsPage(targetID, page, pageSize)
}

func (s *Service) LatestRuns() ([]storage.ModelCheckRun, error) {
	return s.store.LatestModelCheckResults()
}

func (s *Service) ChannelSummaries(channelIDs []uint) (map[uint]storage.ModelCheckChannelSummary, error) {
	return s.store.ChannelSummaries(channelIDs)
}

func (s *Service) RequestsUsed24h() (int64, error) {
	return s.store.CountRequestsSince(s.now().Add(-24 * time.Hour))
}

func (s *Service) RunScheduled(ctx context.Context, raw config.ModelCheckConfig) (BatchResult, error) {
	cfg := NormalizeConfig(raw)
	if !cfg.Enabled {
		return BatchResult{}, nil
	}
	if !s.scheduleMu.TryLock() {
		return BatchResult{}, errors.New("model check batch is already running")
	}
	defer s.scheduleMu.Unlock()

	if _, err := s.SyncTargets(ctx); err != nil {
		return BatchResult{}, err
	}
	used, err := s.store.CountRequestsSince(s.now().Add(-24 * time.Hour))
	if err != nil {
		return BatchResult{}, err
	}
	remaining := int64(cfg.RequestBudget24h) - used
	if remaining < int64(cfg.ProbesPerTarget) {
		return BatchResult{}, nil
	}
	limit := min(cfg.MaxTargetsPerRun, int(remaining)/cfg.ProbesPerTarget)
	targets, err := s.store.ListDueTargets(limit)
	if err != nil {
		return BatchResult{}, err
	}

	type outcome struct {
		run *storage.ModelCheckRun
		err error
	}
	jobs := make(chan storage.ModelCheckTarget)
	results := make(chan outcome, len(targets))
	workers := min(cfg.Concurrency, max(1, len(targets)))
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range jobs {
				run, runErr := s.executeTarget(ctx, target, cfg)
				results <- outcome{run: run, err: runErr}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, target := range targets {
			select {
			case <-ctx.Done():
				return
			case jobs <- target:
			}
		}
	}()
	wg.Wait()
	close(results)

	batch := BatchResult{Targets: len(targets)}
	var errs []error
	for result := range results {
		if result.err != nil {
			if errors.Is(result.err, ErrProbeDeferred) {
				batch.Deferred++
				if result.run != nil {
					batch.Requests += result.run.RequestCount
				}
				continue
			}
			batch.Errors++
			errs = append(errs, result.err)
			continue
		}
		batch.Requests += result.run.RequestCount
		switch result.run.Status {
		case storage.ModelCheckRunPassed:
			batch.Passed++
		case storage.ModelCheckRunAnomaly:
			batch.Anomalies++
		default:
			batch.Errors++
		}
	}
	if _, err := s.store.DeleteRunsBefore(s.now().AddDate(0, 0, -cfg.HistoryDays), 500); err != nil && s.log != nil {
		s.log.Warn("model check retention failed", "err", err)
	}
	return batch, errors.Join(errs...)
}

func (s *Service) RunTarget(ctx context.Context, id uint, raw config.ModelCheckConfig) (*storage.ModelCheckRun, error) {
	cfg := NormalizeConfig(raw)
	if _, err := s.SyncTargets(ctx); err != nil {
		return nil, err
	}
	target, err := s.store.FindTarget(id)
	if err != nil {
		return nil, err
	}
	if !target.Active {
		return nil, errors.New("model check target is no longer active")
	}
	if !target.Enabled {
		return nil, errors.New("model check target is disabled")
	}
	return s.executeTarget(ctx, *target, cfg)
}

func (s *Service) executeTarget(ctx context.Context, target storage.ModelCheckTarget, cfg config.ModelCheckConfig) (*storage.ModelCheckRun, error) {
	if err := s.acquireRunSlot(ctx, cfg.Concurrency); err != nil {
		return nil, err
	}
	defer s.releaseRunSlot()
	releaseBudget, err := s.reserveRequestBudget(cfg.ProbesPerTarget, cfg.RequestBudget24h)
	if err != nil {
		return nil, err
	}
	defer releaseBudget()
	return s.runTarget(ctx, target, cfg)
}

func (s *Service) acquireRunSlot(ctx context.Context, limit int) error {
	limit = max(1, min(limit, 4))
	for {
		s.runMu.Lock()
		if s.activeRuns < limit {
			s.activeRuns++
			s.runMu.Unlock()
			return nil
		}
		changed := s.runChanged
		s.runMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (s *Service) releaseRunSlot() {
	s.runMu.Lock()
	if s.activeRuns > 0 {
		s.activeRuns--
	}
	close(s.runChanged)
	s.runChanged = make(chan struct{})
	s.runMu.Unlock()
}

func (s *Service) reserveRequestBudget(requests, budget int) (func(), error) {
	requests = max(1, requests)
	budget = max(1, budget)
	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()
	used, err := s.store.CountRequestsSince(s.now().Add(-24 * time.Hour))
	if err != nil {
		return nil, err
	}
	if used+s.reserved+int64(requests) > int64(budget) {
		return nil, ErrRequestBudgetExceeded
	}
	s.reserved += int64(requests)
	return func() {
		s.budgetMu.Lock()
		s.reserved = max(0, s.reserved-int64(requests))
		s.budgetMu.Unlock()
	}, nil
}

func (s *Service) runTarget(ctx context.Context, target storage.ModelCheckTarget, cfg config.ModelCheckConfig) (*storage.ModelCheckRun, error) {
	lock := s.targetLock(target.ID)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case lock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-lock }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	current, err := s.store.FindTarget(target.ID)
	if err != nil {
		return nil, err
	}
	if current.UpstreamAccountIdentityRevision != target.UpstreamAccountIdentityRevision || current.ChannelID != target.ChannelID || current.GroupName != target.GroupName || current.ModelName != target.ModelName || current.App != target.App {
		return nil, storage.ErrStaleChannelAccountIdentityRevision
	}
	if !current.Enabled {
		return nil, errors.New("该检测目标已停用，请先启用")
	}
	// Queued runs must begin with the latest baseline and operator state.
	target = *current

	startedAt := s.now()
	recent, err := s.store.ListRecentRuns(target.ID, recentWindowSize-1)
	if err != nil {
		return nil, err
	}
	if len(recent) > 0 && recent[0].ProbeVersion != probeVersion {
		// Scores from different question sets are not comparable.
		target.BaselineScore, target.BaselineSamples = nil, 0
	}
	currentRecent := recent[:0]
	for _, previous := range recent {
		if previous.ProbeVersion == probeVersion {
			currentRecent = append(currentRecent, previous)
		}
	}
	recent = currentRecent
	challenges, err := buildChallenges(s.random, cfg.ProbesPerTarget)
	if err != nil {
		return nil, err
	}
	descriptor := TargetDescriptor{
		ChannelID:                       target.ChannelID,
		UpstreamAccountIdentityRevision: target.UpstreamAccountIdentityRevision,
		ChannelName:                     target.ChannelName,
		GroupName:                       target.GroupName,
		ModelName:                       target.ModelName,
		App:                             target.App,
	}
	evidence := make([]probeEvidence, 0, len(challenges))
	requestErrors := make([]string, 0)
	reasons := make([]string, 0)
	passed := 0
	successfulResponses := int64(0)
	executedChallenges := 0
	deferred := false
	hardMismatch := false
	actualModel := ""
	run := &storage.ModelCheckRun{
		TargetID:                        target.ID,
		ChannelID:                       target.ChannelID,
		UpstreamAccountIdentityRevision: target.UpstreamAccountIdentityRevision,
		ChannelName:                     target.ChannelName,
		GroupName:                       target.GroupName,
		ModelName:                       target.ModelName,
		App:                             target.App,
		ProbeVersion:                    probeVersion,
		StartedAt:                       startedAt,
	}
	if target.BaselineScore != nil {
		copy := *target.BaselineScore
		run.BaselineBefore = &copy
	}

	for _, challenge := range challenges {
		EmitProgress(ctx, ProgressEvent{Type: "probe_started", Kind: challenge.kind, Message: "正在准备请求"})
		timeoutSeconds := cfg.TimeoutSeconds
		if challenge.kind == PelicanProbeKind {
			timeoutSeconds = cfg.PelicanTimeoutSeconds
		}
		probeCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
		response, probeErr := s.runner.RunProbe(probeCtx, descriptor, ProbeRequest{
			Kind: challenge.kind, Prompt: challenge.prompt, MaxTokens: challenge.maxTokens,
		})
		cancel()
		if errors.Is(probeErr, ErrProbeDeferred) {
			EmitProgress(ctx, ProgressEvent{Type: "probe_error", Kind: challenge.kind, Message: "本机容量繁忙，本轮已延后"})
			deferred = true
			break
		}
		run.RequestCount++
		executedChallenges++
		item := probeEvidence{
			Kind: challenge.kind, ChallengeHash: challenge.hash, Prompt: challenge.prompt,
			Expected: challenge.expected, Scene: challenge.scene,
		}
		if probeErr != nil {
			item.Error = ProbeErrorDiagnostic(probeErr, response.StatusCode)
			EmitProgress(ctx, ProgressEvent{Type: "probe_error", Kind: challenge.kind, StatusCode: response.StatusCode, Message: item.Error})
			requestErrors = appendUnique(requestErrors, item.Error)
			evidence = append(evidence, item)
			continue
		}
		item.Response = boundedText(response.Text, maxEvidenceText)
		successfulResponses++
		item.ActualModel = strings.TrimSpace(response.ActualModel)
		item.FirstByteMS = response.FirstByteMS
		item.LatencyMS = response.LatencyMS
		item.PromptTokens = response.PromptTokens
		item.CompletionTokens = response.CompletionTokens
		item.CacheReadTokens = response.CacheReadTokens
		item.TotalTokens = response.TotalTokens
		run.PromptTokens += response.PromptTokens
		run.CompletionTokens += response.CompletionTokens
		run.CacheReadTokens += response.CacheReadTokens
		run.TotalTokens += response.TotalTokens
		run.FirstByteMS += response.FirstByteMS
		run.LatencyMS += response.LatencyMS
		if item.ActualModel != "" {
			actualModel = item.ActualModel
			if modelFamiliesConflict(target.ModelName, item.ActualModel) {
				hardMismatch = true
				reasons = appendUnique(reasons, fmt.Sprintf("返回模型 %s 与请求模型族不一致", item.ActualModel))
			}
		}
		if visibleThinkingMarkup(response.Text) {
			reasons = appendUnique(reasons, "响应正文泄漏 <thinking> 思考标签")
		}
		item.Passed, item.Detail = challenge.evaluate(response.Text)
		EmitProgress(ctx, ProgressEvent{Type: "probe_result", Kind: challenge.kind, ResponseText: item.Response, StatusCode: response.StatusCode, Passed: &item.Passed, Message: item.Detail, LatencyMS: item.LatencyMS, TotalTokens: item.TotalTokens})
		if challenge.kind == PelicanProbeKind {
			// Only validated self-contained images may reach the preview. Raw
			// malformed responses retain the existing small evidence bound.
			item.ArtifactSVG, _ = SafePelicanSVG(response.Text)
		}
		if item.Passed {
			passed++
		} else {
			reasons = appendUnique(reasons, fmt.Sprintf("%s 未通过", challenge.label))
		}
		evidence = append(evidence, item)
	}
	if deferred && executedChallenges == 0 {
		return nil, ErrProbeDeferred
	}
	if successfulResponses > 0 {
		run.FirstByteMS /= successfulResponses
		run.LatencyMS /= successfulResponses
		run.Score = math.Round(float64(passed)/float64(executedChallenges)*10000) / 100
		if !deferred && target.BaselineScore != nil && target.BaselineSamples >= baselineMinSamples && run.Score < *target.BaselineScore-baselineDropPoints {
			reasons = appendUnique(reasons, fmt.Sprintf("得分较历史基线下降 %.1f", *target.BaselineScore-run.Score))
		}
	}
	run.ActualModel = actualModel
	run.HardMismatch = hardMismatch
	reasonJSON, _ := json.Marshal(reasons)
	run.AnomalyReasons = string(reasonJSON)
	evidenceJSON, marshalErr := json.Marshal(evidence)
	if marshalErr != nil {
		return nil, marshalErr
	}
	run.EvidenceJSON = string(evidenceJSON)
	run.Error = strings.Join(requestErrors, "\n")
	run.FinishedAt = s.now()
	if deferred {
		run.Status = storage.ModelCheckRunDeferred
		if err := s.store.SaveRun(run); err != nil {
			return nil, err
		}
		return run, ErrProbeDeferred
	}
	switch {
	case successfulResponses == 0:
		run.Status = storage.ModelCheckRunError
	case hardMismatch || len(reasons) > 0:
		run.Status = storage.ModelCheckRunAnomaly
	case len(requestErrors) > 0:
		run.Status = storage.ModelCheckRunError
	default:
		run.Status = storage.ModelCheckRunPassed
	}
	wasAlerting := target.AlertActive
	EmitProgress(ctx, ProgressEvent{Type: "saving", Message: "校验结束，正在保存检测记录"})
	s.updateTargetState(&target, run, recent, reasons, successfulResponses > 0, cfg)
	if err := s.store.SaveRunAndTarget(run, &target); err != nil {
		return nil, err
	}
	if !wasAlerting && target.AlertActive {
		s.dispatchAlert(ctx, target, run, reasons)
	} else if wasAlerting && !target.AlertActive {
		s.dispatchRecovery(ctx, target, run)
	}
	return run, nil
}

func (s *Service) updateTargetState(target *storage.ModelCheckTarget, run *storage.ModelCheckRun, recent []storage.ModelCheckRun, reasons []string, hasSuccessfulResponse bool, cfg config.ModelCheckConfig) {
	now := run.FinishedAt
	target.LastCheckedAt = &now
	if hasSuccessfulResponse {
		score := run.Score
		target.LastScore = &score
		target.LastActualModel = run.ActualModel
	}
	target.LastError = run.Error

	anomalies := 0
	window := append([]storage.ModelCheckRunStatus{run.Status}, recentStatuses(recent)...)
	if len(window) > recentWindowSize {
		window = window[:recentWindowSize]
	}
	for _, status := range window {
		if status == storage.ModelCheckRunAnomaly {
			anomalies++
		}
	}
	target.RecentAnomalies = anomalies

	if run.Status == storage.ModelCheckRunError {
		target.ConsecutiveErrors++
	} else {
		target.ConsecutiveErrors = 0
	}
	shouldAlert := run.HardMismatch || anomalies >= cfg.AlertFailures
	if target.AlertActive && !shouldAlert {
		shouldAlert = !twoConsecutivePasses(run.Status, recent)
	}
	if shouldAlert {
		target.AlertActive = true
		if run.Status == storage.ModelCheckRunError && target.ConsecutiveErrors >= 2 {
			target.LastStatus = storage.ModelCheckTargetUnavailable
		} else {
			target.LastStatus = storage.ModelCheckTargetSuspect
		}
		if len(reasons) > 0 {
			target.AlertReason = strings.Join(reasons, "；")
		}
		if target.AlertedAt == nil {
			copy := now
			target.AlertedAt = &copy
		}
		target.RecoveredAt = nil
		return
	}
	if target.AlertActive {
		copy := now
		target.RecoveredAt = &copy
	}
	target.AlertActive = false
	target.AlertReason = ""
	switch run.Status {
	case storage.ModelCheckRunPassed:
		target.LastStatus = storage.ModelCheckTargetHealthy
		updateBaseline(target, run.Score)
	case storage.ModelCheckRunError:
		if target.ConsecutiveErrors >= 2 {
			target.LastStatus = storage.ModelCheckTargetUnavailable
		} else {
			target.LastStatus = storage.ModelCheckTargetPending
		}
	case storage.ModelCheckRunAnomaly:
		target.LastStatus = storage.ModelCheckTargetSuspect
	default:
		target.LastStatus = storage.ModelCheckTargetPending
	}
}

func updateBaseline(target *storage.ModelCheckTarget, score float64) {
	if target.BaselineScore == nil || target.BaselineSamples <= 0 {
		copy := score
		target.BaselineScore = &copy
		target.BaselineSamples = 1
		return
	}
	weight := min(target.BaselineSamples, 9)
	next := (*target.BaselineScore*float64(weight) + score) / float64(weight+1)
	target.BaselineScore = &next
	target.BaselineSamples++
}

func recentStatuses(runs []storage.ModelCheckRun) []storage.ModelCheckRunStatus {
	out := make([]storage.ModelCheckRunStatus, 0, len(runs))
	for i := range runs {
		out = append(out, runs[i].Status)
	}
	return out
}

func twoConsecutivePasses(current storage.ModelCheckRunStatus, recent []storage.ModelCheckRun) bool {
	return current == storage.ModelCheckRunPassed && len(recent) > 0 && recent[0].Status == storage.ModelCheckRunPassed
}

func (s *Service) targetLock(id uint) chan struct{} {
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	lock := s.locks[id]
	if lock == nil {
		lock = make(chan struct{}, 1)
		s.locks[id] = lock
	}
	return lock
}

func (s *Service) dispatchAlert(ctx context.Context, target storage.ModelCheckTarget, run *storage.ModelCheckRun, reasons []string) {
	if s.dispatcher == nil {
		return
	}
	message := notify.Message{
		Event: storage.EventModelCheckAlert, ChannelID: target.ChannelID, ModelName: target.GroupName,
		Subject: fmt.Sprintf("【掺水检测告警】%s · %s", target.ChannelName, target.ModelName),
		Body: fmt.Sprintf("渠道：%s\n分组：%s\n模型：%s\n协议：%s\n本轮得分：%.2f\n历史基线：%s\n异常：%s\n时间：%s",
			target.ChannelName, target.GroupName, target.ModelName, target.App, run.Score,
			formatOptionalScore(run.BaselineBefore), strings.Join(reasons, "；"), run.FinishedAt.Format("2006-01-02 15:04:05")),
	}
	if err := s.dispatchForTargetIdentity(ctx, target, func(dispatchCtx context.Context) error {
		return s.dispatcher.Dispatch(dispatchCtx, message)
	}); err != nil && s.log != nil {
		s.log.Warn("dispatch model check alert failed", "target_id", target.ID, "err", err)
	}
}

func (s *Service) dispatchRecovery(ctx context.Context, target storage.ModelCheckTarget, run *storage.ModelCheckRun) {
	if s.dispatcher == nil {
		return
	}
	message := notify.Message{
		Event: storage.EventModelCheckRecovered, ChannelID: target.ChannelID, ModelName: target.GroupName,
		Subject: fmt.Sprintf("【掺水检测恢复】%s · %s", target.ChannelName, target.ModelName),
		Body: fmt.Sprintf("渠道：%s\n分组：%s\n模型：%s\n连续复测已恢复，本轮得分：%.2f\n时间：%s",
			target.ChannelName, target.GroupName, target.ModelName, run.Score, run.FinishedAt.Format("2006-01-02 15:04:05")),
	}
	if err := s.dispatchForTargetIdentity(ctx, target, func(dispatchCtx context.Context) error {
		return s.dispatcher.Dispatch(dispatchCtx, message)
	}); err != nil && s.log != nil {
		s.log.Warn("dispatch model check recovery failed", "target_id", target.ID, "err", err)
	}
}

// dispatchForTargetIdentity serializes the final notification with normal
// administrator identity changes and rechecks the exact revision while that
// channel operation lock is held. SaveRunAndTarget has already fenced durable
// state; this separate final fence closes the save-to-notify race.
func (s *Service) dispatchForTargetIdentity(ctx context.Context, target storage.ModelCheckTarget, dispatch func(context.Context) error) error {
	if s == nil || s.dispatcher == nil || dispatch == nil {
		return nil
	}
	if s.channels == nil || target.ChannelID == 0 || target.UpstreamAccountIdentityRevision == 0 {
		return storage.ErrStaleChannelAccountIdentityRevision
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, modelCheckIdentityDispatchTimeout)
	defer cancel()

	unlock, err := s.channels.LockChannelOperation(dispatchCtx, target.ChannelID)
	if err != nil {
		return err
	}
	defer unlock()
	if err := s.channels.RequireUpstreamAccountIdentityRevision(target.ChannelID, target.UpstreamAccountIdentityRevision); err != nil {
		return err
	}
	return dispatch(storage.WithChannelOperationLockHeld(dispatchCtx, target.ChannelID))
}

func formatOptionalScore(score *float64) string {
	if score == nil {
		return "尚未建立"
	}
	return fmt.Sprintf("%.2f", *score)
}

func appendUnique(items []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return items
	}
	for _, item := range items {
		if item == value {
			return items
		}
	}
	return append(items, value)
}

func boundedText(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit]) + "…"
}

func modelFamiliesConflict(requested, actual string) bool {
	want := modelFamily(requested)
	got := modelFamily(actual)
	return want != "" && got != "" && want != got
}

func modelFamily(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch {
	case strings.Contains(value, "claude"), strings.Contains(value, "anthropic"), strings.Contains(value, "sonnet"), strings.Contains(value, "opus"), strings.Contains(value, "haiku"):
		return "claude"
	case strings.Contains(value, "gemini"), strings.Contains(value, "google"):
		return "gemini"
	case strings.Contains(value, "grok"), strings.Contains(value, "xai"):
		return "grok"
	case strings.Contains(value, "deepseek"):
		return "deepseek"
	case strings.Contains(value, "gpt"), strings.Contains(value, "openai"), strings.Contains(value, "codex"):
		return "gpt"
	default:
		return ""
	}
}

func visibleThinkingMarkup(value string) bool {
	value = strings.ToLower(value)
	return strings.Contains(value, "<thinking>") || strings.Contains(value, "</thinking>")
}
