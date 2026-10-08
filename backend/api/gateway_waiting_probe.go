package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

var errGatewayWaitingProbeTimeout = errors.New("waiting health inference did not respond before its deadline")

type gatewayWaitingProbeGroup struct {
	ctx       context.Context
	cancel    context.CancelFunc
	listeners map[*gatewayFirstByteDeadline]func()
}

type gatewayWaitingProbeCoordinator struct {
	mu     sync.Mutex
	groups map[string]*gatewayWaitingProbeGroup
	slots  chan struct{}
}

var gatewayWaitingProbes = gatewayWaitingProbeCoordinator{slots: make(chan struct{}, 4)}

// A shared probe belongs to an exact transport/key generation and model. One
// request finishing detaches only its own listener; the last detach stops the
// probe. No request prompt, session ID or downstream credentials enter probes.
func (p *gatewayWaitingProbeCoordinator) subscribe(key string, deadline *gatewayFirstByteDeadline, interval time.Duration, run func(context.Context) bool) func() {
	p.mu.Lock()
	if p.groups == nil {
		p.groups = make(map[string]*gatewayWaitingProbeGroup)
	}
	group := p.groups[key]
	if group == nil {
		if len(p.groups) >= 128 {
			p.mu.Unlock()
			return func() {}
		}
		ctx, cancel := context.WithCancel(context.Background())
		group = &gatewayWaitingProbeGroup{ctx: ctx, cancel: cancel, listeners: make(map[*gatewayFirstByteDeadline]func())}
		p.groups[key] = group
		go p.watch(key, group, interval, run)
	}
	if len(group.listeners) >= 256 {
		p.mu.Unlock()
		return func() {}
	}
	group.listeners[deadline] = func() { deadline.failWaitingProbe() }
	p.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			delete(group.listeners, deadline)
			if len(group.listeners) == 0 {
				group.cancel()
				if p.groups[key] == group {
					delete(p.groups, key)
				}
			}
		})
	}
}

func (p *gatewayWaitingProbeCoordinator) watch(key string, group *gatewayWaitingProbeGroup, interval time.Duration, run func(context.Context) bool) {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-group.ctx.Done():
			return
		case <-timer.C:
		}
		select {
		case p.slots <- struct{}{}:
			ctx, cancel := context.WithCancel(group.ctx)
			failed := run(ctx)
			cancel()
			<-p.slots
			if failed && group.ctx.Err() == nil {
				p.mu.Lock()
				for _, notify := range group.listeners {
					notify()
				}
				group.cancel()
				if p.groups[key] == group {
					delete(p.groups, key)
				}
				p.mu.Unlock()
				return
			}
		default: // Local saturation is not evidence that a provider is unhealthy.
		}
		timer.Reset(interval)
	}
}

func gatewayWaitingProbeSupported(path string) bool {
	switch path {
	case "/v1/responses", "/v1/chat/completions", "/v1/messages", "/v1/completions":
		return true
	}
	return false
}

func startGatewayWaitingProbe(d *Deps, deadline *gatewayFirstByteDeadline, candidate gateway.Candidate, target gatewayResolvedTarget, lease gateway.UpstreamKeyLease, fence gateway.UpstreamDispatchFence, path, model string) {
	if d == nil || d.Gateway == nil || !gatewayWaitingProbeSupported(path) {
		return
	}
	config := d.Gateway.CurrentRoutingConfig()
	identity := fmt.Sprintf("%p\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", d.Gateway, candidate.Channel.ID, candidate.Channel.UpstreamAccountIdentityRevision, candidate.UpstreamGeneration.ID, candidate.UpstreamGeneration.PresenceGeneration, lease.LocalKeyID, lease.KeyGeneration, lease.Key, target.SiteURL, target.ProxyCacheKey, target.ProxyURL, path, model)
	digest := sha256.Sum256([]byte(identity))
	stop := gatewayWaitingProbes.subscribe(hex.EncodeToString(digest[:]), deadline, config.WaitingProbeInterval, func(ctx context.Context) bool {
		return runGatewayWaitingProbe(ctx, d, candidate, target, lease, fence, path, model, config.WaitingProbeTimeout)
	})
	stopContext := context.AfterFunc(deadline.ctx, stop)
	deadline.waitStop = func() { stopContext(); stop() }
}

func gatewayWaitingProbeBody(path, model string) []byte {
	request := map[string]any{"model": model, "stream": false}
	switch path {
	case "/v1/responses":
		request["input"] = "Reply OK."
		request["max_output_tokens"] = 32
	case "/v1/completions":
		request["prompt"] = "Reply OK."
		request["max_tokens"] = 32
	default:
		request["messages"] = []map[string]string{{"role": "user", "content": "Reply OK."}}
		request["max_tokens"] = 32
	}
	body, _ := json.Marshal(request)
	return body
}

// Only an actually dispatched probe exhausting its own deadline concludes the
// wait. Unsupported probe payloads, local admission/fence failures, and client
// cancellation cannot mark the target down. The main request handles its own
// explicit upstream errors through the normal bounded retry policy.
func runGatewayWaitingProbe(ctx context.Context, d *Deps, candidate gateway.Candidate, target gatewayResolvedTarget, lease gateway.UpstreamKeyLease, fence gateway.UpstreamDispatchFence, path, model string, probeTimeout time.Duration) bool {
	select {
	case gatewayWaitingProbeLogSlots <- struct{}{}:
	default:
		return false
	}
	logSlotOwned := true
	defer func() {
		if logSlotOwned {
			<-gatewayWaitingProbeLogSlots
		}
	}()
	if ctx.Err() != nil {
		return false
	}
	parent := ctx
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	prepareTimer := time.AfterFunc(5*time.Second, func() { cancel(context.DeadlineExceeded) })
	defer prepareTimer.Stop()
	var networkTimer *time.Timer
	defer func() {
		if networkTimer != nil {
			networkTimer.Stop()
		}
	}()
	account, decision := d.Gateway.TryAcquireCandidateAccountConcurrency(candidate, gateway.AccountConcurrencyWaitingProbe)
	if !decision.Admitted {
		return false
	}
	if account != nil {
		defer account.Release()
		account.BindContext(ctx)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://gateway.invalid"+path, nil)
	if err != nil {
		return false
	}
	request.Header.Set("Content-Type", "application/json")
	if path == "/v1/messages" {
		request.Header.Set("anthropic-version", "2023-06-01")
	}
	dispatched := false
	started := time.Now().UTC()
	resp, err := forwardGatewayRequestWithDispatchFence(request, target, lease.Key, gatewayWaitingProbeBody(path, model), &gatewayTransportDispatchFence{service: d.Gateway, fence: fence, beforeDispatch: func(context.Context) error {
		if !prepareTimer.Stop() || ctx.Err() != nil {
			return context.DeadlineExceeded
		}
		dispatched = true
		networkTimer = time.AfterFunc(probeTimeout, func() { cancel(errGatewayWaitingProbeTimeout) })
		return nil
	}}, nil)
	if account != nil {
		account.BindHTTPResponse(resp, err)
	}
	status := http.StatusBadGateway
	var raw []byte
	if resp != nil {
		status = resp.StatusCode
		if resp.Body != nil {
			raw, err = io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
			_ = resp.Body.Close()
		}
	}
	if networkTimer != nil {
		networkTimer.Stop()
	}
	failed := dispatched && err != nil && parent.Err() == nil && errors.Is(context.Cause(ctx), errGatewayWaitingProbeTimeout)
	if !dispatched {
		return false
	}
	finished := time.Now().UTC()
	metrics := parseGatewayUsageMetricsForPath(raw, path, lease.Key)
	if resp != nil && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
		metrics = parseGatewayStreamUsageMetricsForPath(raw, path, lease.Key)
	}
	message := ""
	if failed {
		message = errGatewayWaitingProbeTimeout.Error()
		status = http.StatusGatewayTimeout
	} else if err != nil {
		message = sanitizeGatewayProbeError([]byte(err.Error()), lease.Key)
	} else if status < 200 || status >= 300 {
		message = "waiting health inference returned a non-success status"
	}
	log := &storage.GatewayRequestLog{ChannelID: candidate.Channel.ID, GroupName: candidate.Rate.ModelName, ModelName: model, Method: http.MethodPost, Path: path, StatusCode: status, LatencyMs: finished.Sub(started).Milliseconds(), RouteReason: "latency_probe", RouteDetails: storage.GatewayWaitingProbeDetail, Error: message, CreatedAt: finished, GroupRatio: candidate.Rate.Ratio, CompletionRatio: candidate.Rate.CompletionRatio, FundingFactor: &candidate.FundingFactor, RawGroupRatio: &candidate.RawRatio, RawCompletionRatio: &candidate.RawCompletionRatio, CostBasisVersion: candidate.CostBasisVersion, CostBasisSource: candidate.CostBasisSource, UpstreamKeyID: lease.UpstreamKeyID, UpstreamKeyName: lease.UpstreamKeyName}
	log.RouteDetails = gateway.WaitingProbeCostDetails(candidate)
	if resp != nil {
		log.UpstreamRequestID, _ = gatewayUpstreamRequestID(resp.Header, candidate.Channel.Type)
		if secret := strings.TrimSpace(lease.Key); secret != "" && strings.Contains(log.UpstreamRequestID, secret) {
			log.UpstreamRequestID = ""
		}
	}
	if len(raw) > 1<<20 {
		log.Error = "waiting health inference response exceeded limit"
		metrics = gatewayUsageMetrics{}
	} else if failure := classifyGatewayStreamFailure(raw); failure.Found {
		log.Error = "waiting health inference returned a structured error"
	} else if resp != nil && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") && gatewayStreamFrameHasError(raw) {
		log.Error = "waiting health inference returned a stream error"
	}
	if !metrics.CostObserved {
		log.CostStatus = storage.GatewayCostStatusUnavailable
		log.CostUnavailableReason = storage.GatewayPricingUnavailableFactMissing
	}
	applyGatewayRequestUsageAndCost(log, metrics, false, candidate.FundingFactor)
	queueGatewayWaitingProbeLog(d, log, finished)
	logSlotOwned = false
	return failed
}

// The network outcome is delivered without waiting for database I/O. A bounded
// writer budget prevents an unavailable database from growing a goroutine queue.
var gatewayWaitingProbeLogSlots = make(chan struct{}, 4)

func queueGatewayWaitingProbeLog(d *Deps, log *storage.GatewayRequestLog, finished time.Time) {
	go func() {
		defer func() { <-gatewayWaitingProbeLogSlots }()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := d.Gateway.LogWaitingProbe(ctx, log, finished); err != nil && d.Log != nil {
			d.Log.Warn("waiting probe cost evidence could not be saved")
		}
	}()
}
