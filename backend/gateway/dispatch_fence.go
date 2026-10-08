package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// UpstreamDispatchFence freezes the routing and local-secret identity chosen
// for one request until the transport-entry audit has been recorded.
type UpstreamDispatchFence struct {
	ChannelID               uint
	GroupName               string
	CredentialMode          storage.CredentialMode
	ChannelIdentityRevision uint64
	ChannelAdminRevision    uint64
	LocalKeyID              uint
	KeyGeneration           uint64
	App                     storage.GatewayApp
	Model                   string
}

// ErrUpstreamDispatchIdentityChanged means the candidate snapshot became
// stale before transport entry. Callers such as latency probes should defer
// without recording health or model facts for the old generation.
var ErrUpstreamDispatchIdentityChanged = errors.New("upstream dispatch identity changed")

func UpstreamDispatchFenceForCandidate(candidate Candidate, lease UpstreamKeyLease, app storage.GatewayApp, model string) (UpstreamDispatchFence, error) {
	fence := UpstreamDispatchFence{
		ChannelID:               candidate.Channel.ID,
		GroupName:               strings.TrimSpace(candidate.Rate.ModelName),
		CredentialMode:          candidate.Channel.CredentialMode,
		ChannelIdentityRevision: candidate.Channel.UpstreamAccountIdentityRevision,
		ChannelAdminRevision:    candidate.Channel.AdminRevision,
		LocalKeyID:              lease.LocalKeyID,
		KeyGeneration:           lease.KeyGeneration,
		App:                     normalizeGatewayApp(app),
		Model:                   strings.TrimSpace(model),
	}
	if err := fence.validateIdentity(); err != nil {
		return UpstreamDispatchFence{}, err
	}
	return fence, nil
}

func (f UpstreamDispatchFence) validateIdentity() error {
	if f.ChannelID == 0 || strings.TrimSpace(f.GroupName) == "" ||
		f.ChannelIdentityRevision == 0 || f.LocalKeyID == 0 || f.KeyGeneration == 0 {
		return errors.New("upstream dispatch identity is incomplete")
	}
	if f.CredentialMode == storage.CredentialModeDirect && f.ChannelAdminRevision == 0 {
		return errors.New("direct dispatch policy revision is unavailable")
	}
	return nil
}

// WithUpstreamDispatchFenceAndThen serializes the last policy/key validation
// with channel mutations. Callers that send authenticated HTTP traffic must
// use RoundTripWithUpstreamDispatchFence so the lock reaches the real
// transport-entry boundary.
func (s *Service) WithUpstreamDispatchFenceAndThen(ctx context.Context, fence UpstreamDispatchFence, dispatch func(context.Context) error) error {
	if s == nil || s.Channels == nil || s.Store == nil {
		return errors.New("upstream dispatch fence is unavailable")
	}
	if err := fence.validateIdentity(); err != nil {
		return err
	}
	return s.WithChannelOperationLockAndThen(ctx, fence.ChannelID, func(lockedCtx context.Context) error {
		channel, err := s.Channels.WithContext(lockedCtx).FindByID(fence.ChannelID)
		if err != nil {
			return fmt.Errorf("reload channel at dispatch boundary: %w", err)
		}
		key, err := s.Store.WithContext(lockedCtx).FindUpstreamKeyByID(fence.LocalKeyID)
		if err != nil {
			return fmt.Errorf("reload upstream key at dispatch boundary: %w", err)
		}
		if err := validateCurrentUpstreamDispatchFence(*channel, *key, fence); err != nil {
			return err
		}
		if err := s.requireCurrentDirectProjectedCredential(lockedCtx, *channel, *key); err != nil {
			return err
		}
		if dispatch != nil {
			return dispatch(lockedCtx)
		}
		return nil
	})
}

type upstreamDispatchRoundTripResult struct {
	response *http.Response
	err      error
}

// RoundTripWithUpstreamDispatchFence holds the channel operation lock until
// the configured RoundTripper has written request headers, or has completed
// without reaching that boundary. This makes an exact-key invalidation and
// the Authorization header entering transport a strict total order without
// retaining the lock for the response body.
func (s *Service) RoundTripWithUpstreamDispatchFence(
	ctx context.Context,
	fence UpstreamDispatchFence,
	beforeDispatch func(context.Context) error,
	request *http.Request,
	base http.RoundTripper,
) (*http.Response, error) {
	if request == nil {
		return nil, errors.New("upstream dispatch request is unavailable")
	}
	if base == nil {
		base = http.DefaultTransport
	}
	var completed *upstreamDispatchRoundTripResult
	var resultCh chan upstreamDispatchRoundTripResult
	err := s.WithUpstreamDispatchFenceAndThen(ctx, fence, func(lockedCtx context.Context) error {
		if beforeDispatch != nil {
			if err := beforeDispatch(lockedCtx); err != nil {
				return err
			}
		}
		transportEntered := make(chan struct{})
		resultCh = make(chan upstreamDispatchRoundTripResult, 1)
		var entryOnce sync.Once
		trace := &httptrace.ClientTrace{WroteHeaders: func() {
			entryOnce.Do(func() { close(transportEntered) })
		}}
		tracedRequest := request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
		go func() {
			response, err := base.RoundTrip(tracedRequest)
			resultCh <- upstreamDispatchRoundTripResult{response: response, err: err}
		}()
		select {
		case result := <-resultCh:
			completed = &result
			return nil
		case <-transportEntered:
			return nil
		case <-lockedCtx.Done():
			return lockedCtx.Err()
		}
	})
	if err != nil {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, err
	}
	if completed != nil {
		return completed.response, completed.err
	}
	result := <-resultCh
	return result.response, result.err
}

func validateCurrentUpstreamDispatchFence(channel storage.Channel, key storage.GatewayUpstreamKey, fence UpstreamDispatchFence) error {
	if channel.ID != fence.ChannelID || channel.DeletionPending ||
		channel.CredentialMode != fence.CredentialMode ||
		channel.UpstreamAccountIdentityRevision != fence.ChannelIdentityRevision {
		return fmt.Errorf("%w: channel routing identity changed before dispatch", ErrUpstreamDispatchIdentityChanged)
	}
	if fence.CredentialMode == storage.CredentialModeDirect {
		if channel.AdminRevision != fence.ChannelAdminRevision ||
			normalizeGatewayApp(channel.GatewayApp) != normalizeGatewayApp(fence.App) {
			return fmt.Errorf("%w: direct routing policy changed before dispatch", ErrUpstreamDispatchIdentityChanged)
		}
		directModels, available := directProjectedModels(channel, key)
		if !available {
			return fmt.Errorf("%w: direct route configuration is unavailable", ErrUpstreamDispatchIdentityChanged)
		}
		if fence.Model != "" && !directModelIDSupportsRequest(directModels, fence.Model) {
			return fmt.Errorf("%w: direct route no longer allows the requested model", ErrUpstreamDispatchIdentityChanged)
		}
	}
	if key.ID != fence.LocalKeyID || key.ChannelID != fence.ChannelID ||
		key.KeyGeneration != fence.KeyGeneration || !key.Enabled || key.Provisioning ||
		strings.TrimSpace(key.APIKeyCipher) == "" ||
		!strings.EqualFold(strings.TrimSpace(key.GroupName), strings.TrimSpace(fence.GroupName)) {
		return fmt.Errorf("%w: upstream key lease changed before dispatch", ErrUpstreamDispatchIdentityChanged)
	}
	return nil
}

type upstreamDispatchFenceRoundTripper struct {
	base           http.RoundTripper
	service        *Service
	fence          UpstreamDispatchFence
	beforeDispatch func(context.Context) error
}

func (t upstreamDispatchFenceRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil {
		return nil, errors.New("upstream dispatch request is unavailable")
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return t.service.RoundTripWithUpstreamDispatchFence(request.Context(), t.fence, t.beforeDispatch, request, base)
}

func clientWithUpstreamDispatchFence(client *http.Client, service *Service, fence UpstreamDispatchFence) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	copy := *client
	copy.Transport = upstreamDispatchFenceRoundTripper{base: copy.Transport, service: service, fence: fence}
	return &copy
}

func sendLatencyProbeRequestWithDispatchFence(ctx context.Context, target gatewayProbeTarget, upstreamKey string, app storage.GatewayApp, modelName string, timeout time.Duration, lease *AccountConcurrencyLease, service *Service, fence UpstreamDispatchFence) (*http.Response, error) {
	path := latencyProbePath(app)
	body := latencyProbeBody(app, modelName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(target.SiteURL, "/")+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(upstreamKey))
	req.Header.Set("x-api-key", strings.TrimSpace(upstreamKey))
	req.Header.Set("Content-Type", "application/json")
	if app == storage.GatewayAppClaude {
		req.Header.Set("anthropic-version", "2023-06-01")
	}
	client, err := modelProbeClientWithCacheKey(target.ProxyURL, target.ProxyCacheKey, timeout)
	if err != nil {
		return nil, err
	}
	if service != nil {
		client = clientWithUpstreamDispatchFence(client, service, fence)
	}
	response, err := netproxy.DoWithoutRedirects(client, req)
	lease.BindHTTPResponse(response, err)
	return response, err
}
