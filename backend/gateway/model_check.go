package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const modelCheckMaxResponseBytes = 1 << 20

func (s *Service) DiscoverTargets(ctx context.Context, now time.Time) ([]modelcheck.TargetDescriptor, error) {
	return s.discoverModelCheckTargets(ctx, now, false)
}

func (s *Service) DiscoverManualTargets(ctx context.Context, now time.Time) ([]modelcheck.TargetDescriptor, error) {
	return s.discoverModelCheckTargets(ctx, now, true)
}

func (s *Service) discoverModelCheckTargets(ctx context.Context, now time.Time, manual bool) ([]modelcheck.TargetDescriptor, error) {
	if s == nil || s.Store == nil || s.Channels == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sources, err := s.Store.ListModelCheckSources(now)
	if err != nil {
		return nil, err
	}
	out := make([]modelcheck.TargetDescriptor, 0, len(sources))
	channels, err := s.Channels.List()
	if err != nil {
		return nil, err
	}
	availableChannels := make(map[uint]struct{}, len(channels))
	for i := range channels {
		if !modelProbeChannelBlocked(channels[i]) {
			availableChannels[channels[i].ID] = struct{}{}
		}
	}
	seen := make(map[string]struct{})
	for _, source := range sources {
		if manual && strings.TrimSpace(source.GroupName) == "" {
			continue
		}
		if _, available := availableChannels[source.ChannelID]; !available {
			continue
		}
		if source.UpstreamAccountIdentityRevision == 0 {
			// A discovery result without an account fence cannot be safely
			// persisted or run. Do not turn it into a cross-account target.
			continue
		}
		if manual {
			out = append(out, modelcheck.TargetDescriptor{ChannelID: source.ChannelID, UpstreamAccountIdentityRevision: source.UpstreamAccountIdentityRevision, ChannelName: source.ChannelName, GroupName: source.GroupName, App: source.App})
			if len(out) > 10000 {
				return nil, errors.New("manual model check inventory exceeds limit")
			}
			continue
		}
		models := append([]string(nil), source.RecentModels...)
		if len(models) == 0 {
			if fallback := modelCheckFallbackModel(source); fallback != "" {
				models = append(models, fallback)
			}
		}
		for _, model := range models {
			model = strings.TrimSpace(model)
			if model == "" {
				continue
			}
			if source.CredentialMode == storage.CredentialModeDirect && !directModelIDSupportsRequest(source.ModelIDs, model) {
				continue
			}
			key := fmt.Sprintf("%d\x00%s\x00%s\x00%s", source.ChannelID, source.GroupName, model, source.App)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, modelcheck.TargetDescriptor{
				ChannelID:                       source.ChannelID,
				UpstreamAccountIdentityRevision: source.UpstreamAccountIdentityRevision,
				ChannelName:                     source.ChannelName,
				GroupName:                       source.GroupName,
				ModelName:                       model,
				App:                             source.App,
			})
		}
	}
	return out, nil
}

func modelCheckFallbackModel(source storage.GatewayModelCheckSource) string {
	best := ""
	bestScore := int(^uint(0) >> 1)
	for _, model := range source.ModelIDs {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		if source.CredentialMode != storage.CredentialModeDirect && !modelIDSupportsRequest(source.ModelIDs, model, source.App) {
			continue
		}
		score := latencyProbeModelRank(model)
		if best == "" || score < bestScore {
			best = model
			bestScore = score
		}
	}
	return best
}

func (s *Service) RunProbe(ctx context.Context, target modelcheck.TargetDescriptor, request modelcheck.ProbeRequest) (result modelcheck.ProbeResponse, returnErr error) {
	if s == nil || s.Channels == nil || s.Store == nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	channel, err := s.Channels.FindByID(target.ChannelID)
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	if target.UpstreamAccountIdentityRevision == 0 || channel.UpstreamAccountIdentityRevision != target.UpstreamAccountIdentityRevision {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	if modelProbeChannelBlocked(*channel) {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	groupName := strings.TrimSpace(target.GroupName)
	modelName := strings.TrimSpace(target.ModelName)
	if groupName == "" || modelName == "" {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	generation, err := s.Store.CaptureUpstreamGroupGeneration(target.ChannelID, groupName)
	if err != nil || !generation.Valid() {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	accountConcurrency, err := s.Channels.UpstreamAccountConcurrencyForChannel(target.ChannelID)
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	candidate := Candidate{
		Channel:                    *channel,
		Rate:                       storage.RateSnapshot{ChannelID: target.ChannelID, ModelName: groupName},
		UpstreamGeneration:         generation,
		UpstreamAccountConcurrency: accountConcurrency,
		App:                        target.App,
	}
	lease, decision := s.TryAcquireCandidateAccountConcurrency(candidate, AccountConcurrencyModelCheck)
	if !decision.Admitted || lease == nil {
		return result, modelcheck.ErrProbeDeferred
	}
	defer lease.Release()
	lease.BindContext(ctx)
	cached, err := s.Store.FindUpstreamKey(target.ChannelID, groupName)
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorUpstreamKey, 0)
	}
	if channel.CredentialMode == storage.CredentialModeDirect {
		directModels, available := directProjectedModels(*channel, *cached)
		if !available || !directModelIDSupportsRequest(directModels, modelName) ||
			normalizeGatewayApp(channel.GatewayApp) != normalizeGatewayApp(target.App) {
			return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
		}
		candidate.ModelIDs = directModels
		candidate.ModelFamilies = directChannelModelFamilies(channel, directModels)
		candidate.DirectBaseURL = strings.TrimSpace(cached.DirectBaseURL)
	}
	if err := s.Channels.RequireUpstreamAccountIdentityRevision(target.ChannelID, target.UpstreamAccountIdentityRevision); err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	keyLease, err := s.EnsureUpstreamAPIKeyLeaseForGroup(ctx, target.ChannelID, groupName, nil, cached.GroupRatio)
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorUpstreamKey, 0)
	}
	upstreamKey := keyLease.Key
	if keyLease.ChannelSnapshot != nil {
		if keyLease.ChannelSnapshot.ID != target.ChannelID ||
			keyLease.ChannelSnapshot.UpstreamAccountIdentityRevision != target.UpstreamAccountIdentityRevision {
			return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
		}
		channel = keyLease.ChannelSnapshot
		candidate.Channel = *channel
		candidate.UpstreamAccountConcurrency = keyLease.UpstreamAccountConcurrency
	}
	if err := s.Channels.RequireUpstreamAccountIdentityRevision(target.ChannelID, target.UpstreamAccountIdentityRevision); err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	resolved, err := s.resolveGatewayProbeTarget(ctx, channel)
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorProxy, 0)
	}
	if channel.CredentialMode == storage.CredentialModeDirect && candidate.DirectBaseURL != "" {
		resolved.SiteURL = strings.TrimRight(candidate.DirectBaseURL, "/")
	}
	if err := s.Channels.RequireUpstreamAccountIdentityRevision(target.ChannelID, target.UpstreamAccountIdentityRevision); err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}

	path, body, err := modelCheckRequest(target.App, modelName, request.Prompt, request.MaxTokens)
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorRequest, 0)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(resolved.SiteURL, "/")+path, strings.NewReader(body))
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorRequest, 0)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+strings.TrimSpace(upstreamKey))
	httpRequest.Header.Set("x-api-key", strings.TrimSpace(upstreamKey))
	httpRequest.Header.Set("Content-Type", "application/json")
	if target.App == storage.GatewayAppClaude {
		httpRequest.Header.Set("anthropic-version", "2023-06-01")
	}
	startedAt := time.Now()
	var firstByteAt time.Time
	trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { firstByteAt = time.Now() }}
	httpRequest = httpRequest.WithContext(httptrace.WithClientTrace(httpRequest.Context(), trace))
	client, err := modelProbeClientWithCacheKey(resolved.ProxyURL, resolved.ProxyCacheKey, modelCheckRequestTimeout(ctx))
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorProxy, 0)
	}
	dispatchFence, err := UpstreamDispatchFenceForCandidate(candidate, keyLease, target.App, modelName)
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	usageLog := s.modelCheckUsageSnapshot(ctx, candidate, keyLease, request, modelName, path)
	dispatched := false
	guardedClient := *client
	guardedClient.Transport = upstreamDispatchFenceRoundTripper{base: client.Transport, service: s, fence: dispatchFence, beforeDispatch: func(context.Context) error {
		dispatched = true
		startedAt = time.Now()
		modelcheck.EmitProgress(ctx, modelcheck.ProgressEvent{Type: "request_sent", Kind: request.Kind, Path: path, RequestBody: body, Message: "请求已发起，等待上游响应"})
		return nil
	}}
	defer func() {
		if !dispatched {
			return
		}
		if err := s.finishModelCheckUsage(usageLog, result, returnErr, startedAt); err != nil {
			modelcheck.EmitProgress(ctx, modelcheck.ProgressEvent{Type: "audit_error", Kind: request.Kind, Message: "使用记录保存失败"})
			if returnErr == nil {
				returnErr = fmt.Errorf("save model-check usage record: %w", err)
			}
		}
	}()
	response, err := netproxy.DoWithoutRedirects(&guardedClient, httpRequest)
	lease.BindHTTPResponse(response, err)
	if err != nil {
		return result, modelCheckTransportProbeError(ctx, err)
	}
	modelcheck.EmitProgress(ctx, modelcheck.ProgressEvent{Type: "response_headers", Kind: request.Kind, StatusCode: response.StatusCode, Message: "已收到上游响应头，正在读取正文"})
	if response.Body == nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorInvalidResponse, response.StatusCode)
	}
	usageLog.UpstreamRequestID = modelCheckRequestID(response.Header, channel.Type, upstreamKey)
	defer response.Body.Close()
	finishedAt := time.Now()
	result.LatencyMS = finishedAt.Sub(startedAt).Milliseconds()
	if !firstByteAt.IsZero() {
		result.FirstByteMS = firstByteAt.Sub(startedAt).Milliseconds()
	}
	result.StatusCode = response.StatusCode
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(response.Body, modelCheckMaxResponseBytes+1))
		if err := s.Channels.RequireUpstreamAccountIdentityRevision(target.ChannelID, target.UpstreamAccountIdentityRevision); err != nil {
			return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
		}
		if connector.IsAccountBalanceFailure(response.StatusCode, raw) {
			_ = s.Channels.MarkUpstreamAccountBalanceInsufficient(channel.ID, target.UpstreamAccountIdentityRevision, finishedAt)
		} else if gatewayHTTPFailureInvalidatesKey(response.StatusCode, raw) {
			if fence, ok := UpstreamKeyInvalidationFenceForCandidate(candidate, keyLease); ok {
				_ = s.InvalidateUpstreamGroupKeyForIdentityContext(ctx, fence)
			}
		}
		if IsExplicitAccountConcurrencyPressure(response.StatusCode, string(raw)) {
			lease.ObservePressure()
		}
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorHTTPStatus, response.StatusCode)
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, modelCheckMaxResponseBytes+1))
	if readErr != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorInvalidResponse, response.StatusCode)
	}
	if len(raw) > modelCheckMaxResponseBytes {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorInvalidResponse, response.StatusCode)
	}
	if target.App == storage.GatewayAppClaude {
		result, err = parseClaudeModelCheckResponse(raw, result)
	} else {
		result, err = parseOpenAIModelCheckResponse(raw, result)
	}
	if err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorInvalidResponse, response.StatusCode)
	}
	// Only normalized response fields enter the UI log; no auth/proxy headers
	// or raw error bodies. Redact the actual credential even on a 2xx echo.
	if secret := strings.TrimSpace(upstreamKey); secret != "" {
		result.Text = strings.ReplaceAll(result.Text, secret, "[redacted]")
		if strings.Contains(result.ActualModel, secret) {
			result.ActualModel = ""
		}
	}

	if err := s.Channels.RequireUpstreamAccountIdentityRevision(target.ChannelID, target.UpstreamAccountIdentityRevision); err != nil {
		return result, modelcheck.NewProbeError(modelcheck.ProbeErrorTargetUnavailable, 0)
	}
	result.LatencyMS = time.Since(startedAt).Milliseconds()
	modelcheck.EmitProgress(ctx, modelcheck.ProgressEvent{Type: "response", Kind: request.Kind, StatusCode: response.StatusCode, ResponseText: result.Text, LatencyMS: result.LatencyMS, TotalTokens: result.TotalTokens, Message: "上游响应正文"})
	lease.ObserveUsefulSuccess()
	return result, nil
}

// modelCheckTransportProbeError keeps timeout and caller-cancellation visible
// as stable probe categories without retaining a provider or proxy error.
func modelCheckTransportProbeError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return modelcheck.NewProbeError(modelcheck.ProbeErrorTimeout, 0)
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return modelcheck.NewProbeError(modelcheck.ProbeErrorCanceled, 0)
	}
	return modelcheck.NewProbeError(modelcheck.ProbeErrorNetwork, 0)
}

func modelCheckRequest(app storage.GatewayApp, modelName, prompt string, maxTokens int) (string, string, error) {
	if maxTokens <= 0 {
		maxTokens = 128
	}
	if app == storage.GatewayAppClaude {
		body, err := json.Marshal(map[string]any{
			"model": modelName, "max_tokens": maxTokens, "temperature": 0,
			"messages": []map[string]any{{"role": "user", "content": prompt}},
		})
		return "/v1/messages", string(body), err
	}
	body, err := json.Marshal(map[string]any{
		"model": modelName, "max_tokens": maxTokens, "temperature": 0, "stream": false,
		"messages": []map[string]any{{"role": "user", "content": prompt}},
	})
	return "/v1/chat/completions", string(body), err
}

func modelCheckRequestTimeout(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 {
			return remaining
		}
	}
	return 60 * time.Second
}

func parseOpenAIModelCheckResponse(raw []byte, result modelcheck.ProbeResponse) (modelcheck.ProbeResponse, error) {
	var parsed struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			TotalTokens      int64 `json:"total_tokens"`
			PromptDetails    struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return result, err
	}
	if len(parsed.Choices) == 0 {
		return result, errors.New("model check response has no choices")
	}
	text, err := openAIMessageText(parsed.Choices[0].Message.Content)
	if err != nil {
		return result, err
	}
	result.Text = text
	result.ActualModel = strings.TrimSpace(parsed.Model)
	result.PromptTokens = parsed.Usage.PromptTokens
	result.CompletionTokens = parsed.Usage.CompletionTokens
	result.CacheReadTokens = parsed.Usage.PromptDetails.CachedTokens
	result.TotalTokens = parsed.Usage.TotalTokens
	return result, nil
}

func openAIMessageText(raw json.RawMessage) (string, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return strings.TrimSpace(text), nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", errors.New("model check response content is not text")
	}
	var builder strings.Builder
	for _, part := range parts {
		if strings.TrimSpace(part.Text) != "" {
			builder.WriteString(part.Text)
		}
	}
	return strings.TrimSpace(builder.String()), nil
}

func parseClaudeModelCheckResponse(raw []byte, result modelcheck.ProbeResponse) (modelcheck.ProbeResponse, error) {
	var parsed struct {
		Model   string `json:"model"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens              int64 `json:"input_tokens"`
			OutputTokens             int64 `json:"output_tokens"`
			CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
			CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return result, err
	}
	var builder strings.Builder
	for _, part := range parsed.Content {
		if part.Type == "text" || part.Type == "" {
			builder.WriteString(part.Text)
		}
	}
	result.Text = strings.TrimSpace(builder.String())
	if result.Text == "" {
		return result, errors.New("model check response has no text content")
	}
	result.ActualModel = strings.TrimSpace(parsed.Model)
	result.PromptTokens = parsed.Usage.InputTokens + parsed.Usage.CacheReadInputTokens + parsed.Usage.CacheCreationInputTokens
	result.CompletionTokens = parsed.Usage.OutputTokens
	result.CacheReadTokens = parsed.Usage.CacheReadInputTokens
	result.CacheCreationTokens = parsed.Usage.CacheCreationInputTokens
	result.TotalTokens = result.PromptTokens + parsed.Usage.OutputTokens
	return result, nil
}
