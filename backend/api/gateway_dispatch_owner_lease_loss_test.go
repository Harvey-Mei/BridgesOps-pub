package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type gatewayOwnerLossBlockingUpstream struct {
	server   *httptest.Server
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	calls    atomic.Int32
}

func newGatewayOwnerLossBlockingUpstream(t *testing.T) *gatewayOwnerLossBlockingUpstream {
	t.Helper()
	upstream := &gatewayOwnerLossBlockingUpstream{
		started:  make(chan struct{}),
		canceled: make(chan struct{}, 1),
		release:  make(chan struct{}),
	}
	upstream.server = httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		// net/http only starts the background read that detects a client
		// disconnect once the request body reaches EOF. A real upstream always
		// consumes the inference payload, so an upstream stub that leaves the
		// body unread would never observe the gateway aborting the request and
		// would silently pass this test for every dispatch payload.
		if request.Body != nil {
			_, _ = io.Copy(io.Discard, request.Body)
		}
		if upstream.calls.Add(1) == 1 {
			close(upstream.started)
		}
		select {
		case <-request.Context().Done():
			select {
			case upstream.canceled <- struct{}{}:
			default:
			}
		case <-upstream.release:
		}
	}))
	t.Cleanup(func() {
		close(upstream.release)
		upstream.server.Close()
	})
	return upstream
}

func newGatewayOwnerLossSecondUpstream(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/v1/models" {
			_, _ = writer.Write([]byte(`{"object":"list","data":[{"id":"gpt-test"}]}`))
			return
		}
		_, _ = writer.Write([]byte(`{"id":"resp-second","output":[]}`))
	}))
	t.Cleanup(server.Close)
	return server, calls
}

type gatewayOwnerLossAPIFixture struct {
	db       *gorm.DB
	service  *gateway.Service
	store    *storage.Gateway
	decision *gatewayRouteDecision
}

func setupGatewayOwnerLossAPIFixture(t *testing.T, firstURL, secondURL string) gatewayOwnerLossAPIFixture {
	t.Helper()
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("gateway-owner-loss-api-test-secret")
	if err != nil {
		t.Fatalf("new owner-loss test cipher: %v", err)
	}

	urls := []string{firstURL, secondURL}
	groups := make(map[uint][]connector.APIKeyGroup, len(urls))
	candidates := make([]gateway.Candidate, 0, len(urls))
	for index, siteURL := range urls {
		channelID := uint(index + 1)
		groupName := fmt.Sprintf("owner-loss-%d", channelID)
		ratio := float64(index + 1)
		channel := storage.Channel{
			ID: channelID, Name: fmt.Sprintf("owner-loss-channel-%d", channelID),
			Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
			SiteURL: siteURL, PasswordCipher: "test", MonitorEnabled: true,
			SortOrder: index + 1,
		}
		if err := channels.Create(&channel); err != nil {
			t.Fatalf("create owner-loss candidate channel %d: %v", channelID, err)
		}
		rate := storage.RateSnapshot{
			ChannelID: channelID, ModelName: groupName, Ratio: ratio,
			CompletionRatio: ratio, LastSeenAt: time.Now(),
		}
		if _, err := rates.Upsert(&rate); err != nil {
			t.Fatalf("upsert owner-loss candidate rate %d: %v", channelID, err)
		}
		ciphertext, err := cipher.Encrypt(fmt.Sprintf("sk-owner-loss-%d", channelID))
		if err != nil {
			t.Fatalf("encrypt owner-loss candidate key %d: %v", channelID, err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channelID, GroupName: groupName, GroupRatio: ratio,
			APIKeyCipher: ciphertext, APIKeyPrefix: "sk-owner", UpstreamKeyID: int64(index + 1),
			UpstreamKeyName: "BOG owner loss", ModelFamilies: "codex", ModelIDs: "gpt-test",
			LatencyProbeEnabled: true, Enabled: true,
		}); err != nil {
			t.Fatalf("upsert owner-loss upstream key %d: %v", channelID, err)
		}
		if err := store.RecordUpstreamGroupChange(channelID, groupName, "added", int64(index+1), "BOG owner loss", time.Now()); err != nil {
			t.Fatalf("record owner-loss group generation %d: %v", channelID, err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channelID, groupName)
		if err != nil {
			t.Fatalf("capture owner-loss group generation %d: %v", channelID, err)
		}
		modelFence := storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision)
		if err := store.UpdateUpstreamKeyModelFamiliesForIdentity(modelFence, "codex", "gpt-test", time.Now(), ""); err != nil {
			t.Fatalf("persist owner-loss model facts %d: %v", channelID, err)
		}
		groups[channelID] = []connector.APIKeyGroup{{Name: groupName, Ratio: ratio}}
		candidates = append(candidates, gateway.Candidate{
			Channel: channel, Rate: rate, UpstreamGeneration: generation,
			ModelFactGeneration: generation, KeyStateGeneration: generation,
			App: storage.GatewayAppCodex, ModelFamilies: []string{"codex"}, ModelIDs: []string{"gpt-test"},
			EffectivePrice: ratio, RawRatio: ratio, RawCompletionRatio: ratio,
		})
	}

	service := gateway.NewService(channels, rates, store, cipher, gatewayProxyTestKeyManager{groups: groups}, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.StopRequestDispatchOwner(ctx, time.Now())
	})
	return gatewayOwnerLossAPIFixture{
		db: db, service: service, store: store,
		decision: &gatewayRouteDecision{
			SmartRouting: true, Candidates: candidates,
			RankedCandidates: append([]gateway.Candidate(nil), candidates...),
		},
	}
}

type gatewayOwnerLossRoutingHealth struct {
	kind          string
	status        int
	message       string
	count         int
	failureAt     string
	cooldownUntil string
	lastUsedAt    string
}

func gatewayOwnerLossOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func gatewayOwnerLossRoutingHealthForKey(key *storage.GatewayUpstreamKey) gatewayOwnerLossRoutingHealth {
	if key == nil {
		return gatewayOwnerLossRoutingHealth{}
	}
	return gatewayOwnerLossRoutingHealth{
		kind: key.LastRoutingFailureKind, status: key.LastRoutingFailureStatus,
		message: key.LastRoutingFailureError, count: key.RoutingFailureCount,
		failureAt:     gatewayOwnerLossOptionalTime(key.LastRoutingFailureAt),
		cooldownUntil: gatewayOwnerLossOptionalTime(key.RoutingCooldownUntil),
		lastUsedAt:    gatewayOwnerLossOptionalTime(key.LastUsedAt),
	}
}

func seedGatewayOwnerLossRoutingHealth(t *testing.T, fixture gatewayOwnerLossAPIFixture) map[uint]gatewayOwnerLossRoutingHealth {
	t.Helper()
	before := make(map[uint]gatewayOwnerLossRoutingHealth, len(fixture.decision.Candidates))
	for _, candidate := range fixture.decision.Candidates {
		channelID := candidate.Channel.ID
		groupName := candidate.Rate.ModelName
		if err := fixture.store.RecordUpstreamKeyRoutingFailure(
			channelID, groupName, gateway.RoutingFailureTransport, 0,
			"pre-existing routing health", time.Now().Add(-time.Minute),
		); err != nil {
			t.Fatalf("seed owner-loss routing health for %d/%s: %v", channelID, groupName, err)
		}
		key, err := fixture.store.FindUpstreamKeyAny(channelID, groupName)
		if err != nil {
			t.Fatalf("read owner-loss routing health for %d/%s: %v", channelID, groupName, err)
		}
		before[channelID] = gatewayOwnerLossRoutingHealthForKey(key)
	}
	return before
}

func assertGatewayOwnerLossRoutingHealthUnchanged(t *testing.T, fixture gatewayOwnerLossAPIFixture, before map[uint]gatewayOwnerLossRoutingHealth) {
	t.Helper()
	for _, candidate := range fixture.decision.Candidates {
		channelID := candidate.Channel.ID
		groupName := candidate.Rate.ModelName
		key, err := fixture.store.FindUpstreamKeyAny(channelID, groupName)
		if err != nil {
			t.Fatalf("read post-owner-loss routing health for %d/%s: %v", channelID, groupName, err)
		}
		if got := gatewayOwnerLossRoutingHealthForKey(key); got != before[channelID] {
			t.Fatalf("owner lease loss changed routing health for %d/%s: before=%+v after=%+v", channelID, groupName, before[channelID], got)
		}
	}
}

func waitForGatewayOwnerLossSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func stopGatewayOwnerAfterUpstreamDispatch(t *testing.T, service *gateway.Service, upstream *gatewayOwnerLossBlockingUpstream, finished <-chan struct{}) {
	t.Helper()
	waitForGatewayOwnerLossSignal(t, upstream.started, "first upstream dispatch")
	ownerContext, releaseOwner, err := service.BindRequestDispatchOwnerContext(context.Background())
	if err != nil {
		t.Fatalf("bind owner-loss observer context: %v", err)
	}
	defer releaseOwner()
	// Stop permanently fences this owner and invokes the same owner.lose cancel
	// path used by heartbeat fencing and local lease expiry. The API must observe
	// that cancel cause as lease loss, never as a downstream client cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = service.StopRequestDispatchOwner(ctx, time.Now())
	cancel()
	if err != nil {
		t.Fatalf("stop request dispatch owner: %v", err)
	}
	waitForGatewayOwnerLossSignal(t, ownerContext.Done(), "owner lease-loss cancel cause")
	if cause := context.Cause(ownerContext); !gateway.IsRequestDispatchOwnerLeaseLost(cause) {
		t.Fatalf("owner context cancel cause = %v, want owner lease loss", cause)
	}
	waitForGatewayOwnerLossSignal(t, upstream.canceled, "upstream request context cancellation")
	waitForGatewayOwnerLossSignal(t, finished, "gateway owner-loss response")
}

func assertGatewayOwnerLossDidNotBecomeClientCancellation(t *testing.T, fixture gatewayOwnerLossAPIFixture, expectedDispatches int64) {
	t.Helper()
	var requestLogs int64
	if err := fixture.db.Model(&storage.GatewayRequestLog{}).Count(&requestLogs).Error; err != nil {
		t.Fatalf("count owner-loss request logs: %v", err)
	}
	if requestLogs != 0 {
		t.Fatalf("owner lease loss created %d terminal request logs, want none for the fenced owner", requestLogs)
	}
	var clientCanceledDispatches int64
	if err := fixture.db.Model(&storage.GatewayRequestDispatchAudit{}).
		Where("terminal_kind = ?", gatewayDispatchTerminalClientCanceled).
		Count(&clientCanceledDispatches).Error; err != nil {
		t.Fatalf("count client-canceled owner-loss dispatches: %v", err)
	}
	if clientCanceledDispatches != 0 {
		t.Fatalf("owner lease loss finalized %d dispatches as client canceled", clientCanceledDispatches)
	}
	var dispatches int64
	if err := fixture.db.Model(&storage.GatewayRequestDispatchAudit{}).Count(&dispatches).Error; err != nil {
		t.Fatalf("count owner-loss dispatches: %v", err)
	}
	if dispatches != expectedDispatches {
		t.Fatalf("owner-loss dispatch count = %d, want %d", dispatches, expectedDispatches)
	}
	if expectedDispatches == 1 {
		var audit storage.GatewayRequestDispatchAudit
		if err := fixture.db.First(&audit).Error; err != nil {
			t.Fatalf("load owner-loss dispatch audit: %v", err)
		}
		if audit.Stage != storage.GatewayRequestDispatchStageDispatched {
			t.Fatalf("owner-loss dispatch stage = %q, want %q", audit.Stage, storage.GatewayRequestDispatchStageDispatched)
		}
		if audit.TerminalAt != nil || audit.TerminalKind != "" || audit.RequestLogID != nil {
			t.Fatalf(
				"owner-loss dispatch was incorrectly finalized: terminal_at=%v terminal_kind=%q request_log_id=%v",
				audit.TerminalAt, audit.TerminalKind, audit.RequestLogID,
			)
		}
	}
}

func runGatewayForwardOwnerLossAPITest(t *testing.T, smartRouting bool) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	first := newGatewayOwnerLossBlockingUpstream(t)
	second, secondCalls := newGatewayOwnerLossSecondUpstream(t)
	fixture := setupGatewayOwnerLossAPIFixture(t, first.server.URL, second.URL)
	fixture.decision.SmartRouting = smartRouting
	before := seedGatewayOwnerLossRoutingHealth(t, fixture)

	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"gpt-test","stream":true}`)
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
	finished := make(chan struct{})
	go func() {
		gatewayForward(
			ginContext, &Deps{Gateway: fixture.service},
			&storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: smartRouting},
			fixture.decision, "gpt-test", "", body,
			gatewayModelMappingResult{Model: "gpt-test"}, time.Now(),
		)
		close(finished)
	}()

	stopGatewayOwnerAfterUpstreamDispatch(t, fixture.service, first, finished)
	if first.calls.Load() != 1 {
		t.Fatalf("first upstream calls = %d, want 1", first.calls.Load())
	}
	if secondCalls.Load() != 0 {
		t.Fatalf("second candidate received %d requests after owner loss", secondCalls.Load())
	}
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
		t.Fatalf("owner-loss gateway response = %d %q", recorder.Code, recorder.Body.String())
	}
	assertGatewayOwnerLossDidNotBecomeClientCancellation(t, fixture, 1)
	assertGatewayOwnerLossRoutingHealthUnchanged(t, fixture, before)
}

func TestGatewayForwardSmartOwnerLeaseLossCancelsUpstreamWithoutFallbackOrHealthMutation(t *testing.T) {
	runGatewayForwardOwnerLossAPITest(t, true)
}

func TestGatewayForwardFixedOwnerLeaseLossCancelsUpstreamWithoutFallbackOrHealthMutation(t *testing.T) {
	runGatewayForwardOwnerLossAPITest(t, false)
}

func TestGatewayProxyModelsOwnerLeaseLossCancelsUpstreamWithoutNextCandidateOrHealthMutation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	first := newGatewayOwnerLossBlockingUpstream(t)
	second, secondCalls := newGatewayOwnerLossSecondUpstream(t)
	fixture := setupGatewayOwnerLossAPIFixture(t, first.server.URL, second.URL)
	before := seedGatewayOwnerLossRoutingHealth(t, fixture)

	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	finished := make(chan struct{})
	go func() {
		gatewayProxyModelsWithTimeout(
			ginContext, &Deps{Gateway: fixture.service},
			&storage.GatewayDownstreamKey{App: storage.GatewayAppCodex, SmartRouting: true},
			5*time.Second,
		)
		close(finished)
	}()

	stopGatewayOwnerAfterUpstreamDispatch(t, fixture.service, first, finished)
	if first.calls.Load() != 1 {
		t.Fatalf("models first upstream calls = %d, want 1", first.calls.Load())
	}
	if secondCalls.Load() != 0 {
		t.Fatalf("models second candidate received %d requests after owner loss", secondCalls.Load())
	}
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
		t.Fatalf("models owner-loss response = %d %q", recorder.Code, recorder.Body.String())
	}
	assertGatewayOwnerLossDidNotBecomeClientCancellation(t, fixture, 0)
	assertGatewayOwnerLossRoutingHealthUnchanged(t, fixture, before)
}
