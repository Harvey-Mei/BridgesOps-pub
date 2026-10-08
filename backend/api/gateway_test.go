package api

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/Harvey-Mei/BridgeOps/backend/usertime"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type gatewayProxyTestKeyManager struct {
	groups       map[uint][]connector.APIKeyGroup
	groupErrors  map[uint]error
	createErrors map[uint]error
	listCalls    *atomic.Int32
	groupCalls   *atomic.Int32
	createCalls  *atomic.Int32
}

func TestGatewayDirectClientNeverUsesEnvironmentProxy(t *testing.T) {
	client, err := gatewayClientForProxy("")
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("gateway transport = %T", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("unbound gateway client retained an environment proxy")
	}
	if transport.ResponseHeaderTimeout != 0 {
		t.Fatalf("gateway response-header timeout = %s, want request-scoped candidate deadline", transport.ResponseHeaderTimeout)
	}
}

func TestGatewayProxyModelsUsesIndependentRequestDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-r.Context().Done()
	}))
	defer upstream.Close()

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := storage.Channel{
		Name: "models-timeout", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: upstream.URL, PasswordCipher: "test", MonitorEnabled: true,
	}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	cipher, err := appcrypto.NewCipher("gateway-models-timeout-test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	ciphertext, err := cipher.Encrypt("sk-models-timeout")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", GroupRatio: 1,
		APIKeyCipher: ciphertext, APIKeyPrefix: "sk-test", UpstreamKeyID: 1,
		UpstreamKeyName: "BOG test", ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	svc := gateway.NewService(channels, rates, store, cipher, gatewayProxyTestKeyManager{groups: map[uint][]connector.APIKeyGroup{
		channel.ID: {{Name: "primary", Ratio: 1}},
	}}, nil)
	downstream := &storage.GatewayDownstreamKey{
		App: storage.GatewayAppCodex, ManualChannelID: channel.ID, ManualGroupName: "primary",
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	started := time.Now()
	gatewayProxyModelsWithTimeout(ctx, &Deps{Gateway: svc}, downstream, 500*time.Millisecond)
	elapsed := time.Since(started)
	if recorder.Code != http.StatusBadGateway || calls.Load() != 1 {
		t.Fatalf("models timeout response=%d body=%q calls=%d", recorder.Code, recorder.Body.String(), calls.Load())
	}
	if elapsed < 400*time.Millisecond || elapsed > 1200*time.Millisecond {
		t.Fatalf("models timeout elapsed=%s, want bounded request deadline", elapsed)
	}
}

func TestGatewayProxyModelsManualRoutePreservesSmartRoutingHealth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-test"}]}`))
	}))
	defer upstream.Close()

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := storage.Channel{
		Name: "manual-models", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: upstream.URL, PasswordCipher: "test", MonitorEnabled: true,
	}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	cipher, err := appcrypto.NewCipher("gateway-models-manual-health-test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	ciphertext, err := cipher.Encrypt("sk-models-manual")
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", GroupRatio: 1,
		APIKeyCipher: ciphertext, APIKeyPrefix: "sk-test",
		ModelFamilies: "codex", ModelIDs: "gpt-old", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(channel.ID, "primary", gateway.RoutingFailureTransport, 0, "existing smart failure", time.Now()); err != nil {
		t.Fatalf("seed existing smart-routing health: %v", err)
	}
	before, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if err != nil {
		t.Fatalf("read existing smart-routing health: %v", err)
	}
	listCalls := &atomic.Int32{}
	groupCalls := &atomic.Int32{}
	createCalls := &atomic.Int32{}
	svc := gateway.NewService(channels, rates, store, cipher, gatewayProxyTestKeyManager{
		listCalls: listCalls, groupCalls: groupCalls, createCalls: createCalls,
	}, nil)
	downstream := &storage.GatewayDownstreamKey{
		App: storage.GatewayAppCodex, SmartRouting: false,
		ManualChannelID: channel.ID, ManualGroupName: "primary",
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	gatewayProxyModelsWithTimeout(ctx, &Deps{Gateway: svc}, downstream, time.Second)

	after, err := store.FindUpstreamKeyAny(channel.ID, "primary")
	if recorder.Code != http.StatusOK || err != nil || after.LastRoutingFailureKind != before.LastRoutingFailureKind || after.LastRoutingFailureStatus != before.LastRoutingFailureStatus || after.RoutingFailureCount != before.RoutingFailureCount || !sameOptionalTime(after.LastRoutingFailureAt, before.LastRoutingFailureAt) || !sameOptionalTime(after.RoutingCooldownUntil, before.RoutingCooldownUntil) {
		t.Fatalf("manual models request changed smart-routing health: status=%d before=%+v after=%+v err=%v body=%q", recorder.Code, before, after, err, recorder.Body.String())
	}
	if listCalls.Load() != 0 || groupCalls.Load() != 0 || createCalls.Load() != 0 {
		t.Fatalf("manual models request invoked control plane: list=%d groups=%d create=%d", listCalls.Load(), groupCalls.Load(), createCalls.Load())
	}
}

func TestProbeGatewayModelFamiliesBoundsResponseSize(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", gatewayModelProbeResponseLimit+1)))
	}))
	defer upstream.Close()

	_, _, err := probeGatewayModelFamilies(context.Background(), gatewayResolvedTarget{SiteURL: upstream.URL}, "sk-test", nil)
	if err == nil || !strings.Contains(err.Error(), "exceeded 1 MiB") {
		t.Fatalf("probe error = %v", err)
	}
}

func TestProbeGatewayModelFamiliesDoesNotFollowRedirect(t *testing.T) {
	var initialCalls atomic.Int32
	var redirectedCalls atomic.Int32
	redirected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedCalls.Add(1)
		_, _ = w.Write([]byte(`{"data":[{"id":"redirected-model"}]}`))
	}))
	defer redirected.Close()
	initial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		initialCalls.Add(1)
		http.Redirect(w, r, redirected.URL+"/v1/models", http.StatusTemporaryRedirect)
	}))
	defer initial.Close()

	_, _, err := probeGatewayModelFamilies(context.Background(), gatewayResolvedTarget{SiteURL: initial.URL}, "sk-probe", nil)
	if err == nil || !strings.Contains(err.Error(), "models probe") {
		t.Fatalf("probe redirect error = %v, want the original non-2xx response", err)
	}
	if initialCalls.Load() != 1 || redirectedCalls.Load() != 0 {
		t.Fatalf("probe followed redirect: initial=%d redirected=%d", initialCalls.Load(), redirectedCalls.Load())
	}
}

func TestProbeGatewayModelFamiliesReturnsMalformedRedirect(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Location", "%gh")
		w.WriteHeader(http.StatusFound)
	}))
	defer upstream.Close()

	_, _, err := probeGatewayModelFamilies(context.Background(), gatewayResolvedTarget{SiteURL: upstream.URL}, "sk-probe", nil)
	if err == nil || !strings.Contains(err.Error(), "models probe") || strings.Contains(err.Error(), "failed to parse Location") {
		t.Fatalf("probe malformed redirect error = %v, want the original non-2xx response", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("probe malformed redirect calls=%d, want 1", calls.Load())
	}
}

func TestRebuildGatewayManagedUpstreamKeysRequiresServerConfirmation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{"", `{}`, `{"confirm":"DELETE"}`} {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Params = gin.Params{{Key: "channel_id", Value: "1"}}
		ctx.Request = httptest.NewRequest(http.MethodPost, "/api/gateway/upstream-keys/1/rebuild-managed", strings.NewReader(body))
		ctx.Request.Header.Set("Content-Type", "application/json")
		rebuildGatewayManagedUpstreamKeys(ctx, nil)
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "confirmation is required") {
			t.Fatalf("body %q response = %d %s", body, recorder.Code, recorder.Body.String())
		}
	}
}

func (m gatewayProxyTestKeyManager) ListAPIKeys(context.Context, uint, connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	if m.listCalls != nil {
		m.listCalls.Add(1)
	}
	return &connector.APIKeyPage{}, nil
}

func (m gatewayProxyTestKeyManager) ListAPIKeyGroups(_ context.Context, channelID uint) ([]connector.APIKeyGroup, error) {
	if m.groupCalls != nil {
		m.groupCalls.Add(1)
	}
	if err := m.groupErrors[channelID]; err != nil {
		return nil, err
	}
	return m.groups[channelID], nil
}

func (m gatewayProxyTestKeyManager) CreateAPIKey(_ context.Context, channelID uint, _ connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	if m.createCalls != nil {
		m.createCalls.Add(1)
	}
	if err := m.createErrors[channelID]; err != nil {
		return nil, err
	}
	return nil, errors.New("unexpected upstream key creation")
}

func (gatewayProxyTestKeyManager) RevealAPIKey(context.Context, uint, int64) (string, error) {
	return "", errors.New("unexpected upstream key reveal")
}

func (gatewayProxyTestKeyManager) DeleteAPIKey(context.Context, uint, int64) error {
	return errors.New("unexpected upstream key deletion")
}

func (gatewayProxyTestKeyManager) PrepareManagedAPIKeyDeletion(context.Context, uint, connector.APIKey, uint64) (func() error, error) {
	return nil, errors.New("unexpected upstream key deletion preparation")
}

type gatewayForwardTestCandidate struct {
	channelID uint
	name      string
	groupName string
	siteURL   string
	ratio     float64
}

func setupGatewayForwardWithCachedCandidates(t *testing.T, candidates []gatewayForwardTestCandidate) (*gateway.Service, *storage.Gateway, *gatewayRouteDecision) {
	t.Helper()
	return setupGatewayForwardWithCachedCandidatesOnDB(t, openTestDB(t), candidates)
}

func setupGatewayForwardWithCachedCandidatesOnDB(t *testing.T, db *gorm.DB, candidates []gatewayForwardTestCandidate) (*gateway.Service, *storage.Gateway, *gatewayRouteDecision) {
	t.Helper()
	if db == nil {
		t.Fatal("gateway forward test database is unavailable")
	}
	store := storage.NewGateway(db)
	rates := storage.NewRates(db)
	cipher, err := appcrypto.NewCipher("gateway-forward-candidates-test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	groups := make(map[uint][]connector.APIKeyGroup, len(candidates))
	decisionCandidates := make([]gateway.Candidate, 0, len(candidates))
	for index, candidate := range candidates {
		now := time.Now()
		channel := &storage.Channel{
			ID: candidate.channelID, Name: fmt.Sprintf("gateway-forward-%d", candidate.channelID),
			Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
			SiteURL: candidate.siteURL, PasswordCipher: "test", MonitorEnabled: true,
		}
		if err := storage.NewChannels(db).Create(channel); err != nil {
			t.Fatalf("create candidate channel: %v", err)
		}
		channel, err = storage.NewChannels(db).FindByID(candidate.channelID)
		if err != nil {
			t.Fatalf("reload candidate channel: %v", err)
		}
		rate := &storage.RateSnapshot{
			ChannelID: candidate.channelID, ModelName: candidate.groupName, Ratio: candidate.ratio, LastSeenAt: now,
		}
		if _, err := rates.Upsert(rate); err != nil {
			t.Fatalf("upsert candidate rate: %v", err)
		}
		rate, err = rates.FindByChannelAndModel(candidate.channelID, candidate.groupName)
		if err != nil {
			t.Fatalf("reload candidate rate: %v", err)
		}
		ciphertext, err := cipher.Encrypt(fmt.Sprintf("sk-candidate-%d", candidate.channelID))
		if err != nil {
			t.Fatalf("encrypt upstream key: %v", err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: candidate.channelID, GroupName: candidate.groupName, GroupRatio: candidate.ratio,
			APIKeyCipher: ciphertext, APIKeyPrefix: "sk-test", UpstreamKeyID: int64(index + 1),
			UpstreamKeyName: "BOG test", ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true,
		}); err != nil {
			t.Fatalf("upsert upstream key: %v", err)
		}
		if err := store.RecordUpstreamGroupChange(candidate.channelID, candidate.groupName, "added", int64(index+1), "BOG test", now); err != nil {
			t.Fatalf("record candidate upstream group: %v", err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(candidate.channelID, candidate.groupName)
		if err != nil {
			t.Fatalf("capture candidate upstream group generation: %v", err)
		}
		modelFence := storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision)
		if err := store.UpdateUpstreamKeyModelFamiliesForIdentity(modelFence, "codex", "gpt-test", now, ""); err != nil {
			t.Fatalf("persist candidate model facts: %v", err)
		}
		groups[candidate.channelID] = []connector.APIKeyGroup{{Name: candidate.groupName, Ratio: candidate.ratio}}
		channel.Name = candidate.name
		decisionCandidates = append(decisionCandidates, gateway.Candidate{
			Channel:             *channel,
			Rate:                *rate,
			UpstreamGeneration:  generation,
			ModelFactGeneration: generation,
			KeyStateGeneration:  generation,
			App:                 storage.GatewayAppCodex,
			RawRatio:            candidate.ratio,
		})
	}
	service := gateway.NewService(storage.NewChannels(db), storage.NewRates(db), store, cipher, gatewayProxyTestKeyManager{groups: groups}, nil)
	return service, store, &gatewayRouteDecision{SmartRouting: true, Candidates: decisionCandidates}
}

func TestMarkGatewayCandidateModelCapabilityRejectsStaleUpstreamGeneration(t *testing.T) {
	_, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "primary", groupName: "primary", siteURL: "https://primary.example", ratio: 1,
	}})
	candidate := decision.Candidates[0]
	if !candidate.UpstreamGeneration.Valid() {
		t.Fatalf("candidate generation = %+v", candidate.UpstreamGeneration)
	}
	if err := store.RecordUpstreamGroupChange(candidate.Channel.ID, candidate.Rate.ModelName, "removed", 1, "BOG test", time.Now()); err != nil {
		t.Fatalf("remove upstream group: %v", err)
	}
	markGatewayCandidateModelCapability(store, candidate, candidate.Rate.ModelName, "gpt-test", false, "late upstream response")
	capability, err := store.FindModelCapability(candidate.Channel.ID, candidate.Rate.ModelName, "gpt-test")
	if err != nil || capability != nil {
		t.Fatalf("stale candidate wrote capability: %+v err=%v", capability, err)
	}
}

func TestGatewayModelFactGenerationFencesNoRateFixedRouteWrites(t *testing.T) {
	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "primary", groupName: "primary", siteURL: "https://primary.example", ratio: 1,
	}})
	candidate := decision.Candidates[0]
	keyLease, err := svc.FixedUpstreamAPIKeyLeaseForGroup(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("load fixed key lease: %v", err)
	}
	candidate.ModelFactGeneration = candidate.UpstreamGeneration
	candidate.KeyStateGeneration = candidate.UpstreamGeneration
	candidate.UpstreamGeneration = storage.UpstreamGroupGeneration{}
	if healthArgs := gatewayCandidateGenerationArgs(candidate); len(healthArgs) != 0 {
		t.Fatalf("no-rate fixed candidate retained health generation: %+v", healthArgs)
	}
	if factArgs := gatewayCandidateModelFactGenerationArgs(candidate); len(factArgs) != 1 || factArgs[0] != candidate.ModelFactGeneration {
		t.Fatalf("no-rate fixed candidate lost model-fact generation: %+v", factArgs)
	}

	if err := store.RecordUpstreamGroupChange(candidate.Channel.ID, candidate.Rate.ModelName, "removed", 1, "BOG test", time.Now()); err != nil {
		t.Fatalf("remove upstream group: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(candidate.Channel.ID, candidate.Rate.ModelName, "added", 2, "BOG replacement", time.Now().Add(time.Second)); err != nil {
		t.Fatalf("re-add upstream group: %v", err)
	}
	updateGatewayModelFamilies(
		&Deps{Gateway: svc}, candidate.Channel.ID, candidate.Rate.ModelName,
		[]string{"claude"}, []string{"claude-stale"}, "",
		gatewayCandidateModelFactGenerationArgs(candidate)...,
	)
	key, err := store.FindUpstreamKeyAny(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("find upstream key after stale model write: %v", err)
	}
	if key.ModelFamilies == "claude" || key.ModelIDs == "claude-stale" {
		t.Fatalf("stale no-rate fixed route model facts crossed generation fence: %+v", key)
	}
	markGatewayCandidateModelCapability(store, candidate, candidate.Rate.ModelName, "gpt-5.4", false, "late fixed response")
	capability, err := store.FindModelCapability(candidate.Channel.ID, candidate.Rate.ModelName, "gpt-5.4")
	if err != nil || capability != nil {
		t.Fatalf("stale no-rate fixed capability crossed generation fence: %+v err=%v", capability, err)
	}
	observeGatewayContextLimit(
		&Deps{Gateway: svc}, candidate, "gpt-5.4", []byte(`{"max_tokens":1000}`),
		[]byte(`Prompt cannot fit safely in this model's 10,000 token context: estimated input 9,500 tokens exceeds the safe input budget of 8,000 after reserving 1,000 output tokens and 1,000 safety tokens.`),
	)
	capability, err = store.FindModelCapability(candidate.Channel.ID, candidate.Rate.ModelName, "gpt-5.4")
	if err != nil || capability != nil {
		t.Fatalf("stale no-rate fixed context limit crossed generation fence: %+v err=%v", capability, err)
	}

	// An old no-rate snapshot can lack ModelFactGeneration entirely. Do not let
	// it take the storage layer's legacy unfenced path and overwrite the
	// replacement group with late model evidence.
	withoutModelFactGeneration := candidate
	withoutModelFactGeneration.ModelFactGeneration = storage.UpstreamGroupGeneration{}
	updateGatewayModelFamilies(
		&Deps{Gateway: svc}, candidate.Channel.ID, candidate.Rate.ModelName,
		[]string{"claude"}, []string{"claude-late"}, "late model sync error",
	)
	markGatewayCandidateModelCapability(store, withoutModelFactGeneration, candidate.Rate.ModelName, "gpt-5.4", false, "late unsupported response")
	observeGatewayContextLimit(
		&Deps{Gateway: svc}, withoutModelFactGeneration, "gpt-5.4", []byte(`{"max_tokens":1000}`),
		[]byte(`Prompt cannot fit safely in this model's 10,000 token context: estimated input 9,500 tokens exceeds the safe input budget of 8,000 after reserving 1,000 output tokens and 1,000 safety tokens.`),
	)
	key, err = store.FindUpstreamKeyAny(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("find replacement key after unfenced model facts: %v", err)
	}
	if key.ModelFamilies != "codex" || key.ModelIDs != "gpt-test" || key.ModelsError != "" {
		t.Fatalf("unfenced no-rate model facts mutated replacement: %+v", key)
	}
	capability, err = store.FindModelCapability(candidate.Channel.ID, candidate.Rate.ModelName, "gpt-5.4")
	if err != nil || capability != nil {
		t.Fatalf("unfenced no-rate context limit mutated replacement: %+v err=%v", capability, err)
	}
	deps := &Deps{Gateway: svc}
	observeGatewayHTTPAuthorizationEvidence(context.Background(), deps, candidate, keyLease, candidate.Rate.ModelName, http.StatusUnauthorized, nil)
	observeGatewayStructuredAuthorizationEvidence(context.Background(), deps, candidate, keyLease, candidate.Rate.ModelName, nil, gatewayStreamFailureClassification{Status: http.StatusUnauthorized})
	key, err = store.FindUpstreamKeyAny(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("find upstream key after stale auth evidence: %v", err)
	}
	if !key.Enabled || key.APIKeyCipher == "" {
		t.Fatal("stale no-rate fixed auth evidence disabled replacement key")
	}

	currentGeneration, err := store.CaptureUpstreamGroupGeneration(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("capture replacement generation: %v", err)
	}
	candidate.KeyStateGeneration = currentGeneration
	observeGatewayHTTPAuthorizationEvidence(context.Background(), deps, candidate, keyLease, candidate.Rate.ModelName, http.StatusUnauthorized, nil)
	key, err = store.FindUpstreamKeyAny(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("find upstream key after current auth evidence: %v", err)
	}
	if key.Enabled || key.APIKeyCipher != "" {
		t.Fatal("current fixed auth evidence did not disable its key")
	}
}

func TestGatewayCandidateAllowsAppSeparatesModelFactsFromKeyState(t *testing.T) {
	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "primary", groupName: "primary", siteURL: "https://primary.example", ratio: 1,
	}})
	candidate := decision.Candidates[0]
	candidate.ModelFactGeneration = candidate.UpstreamGeneration
	candidate.KeyStateGeneration = candidate.UpstreamGeneration
	candidate.UpstreamGeneration = storage.UpstreamGroupGeneration{}
	if err := store.RecordUpstreamGroupChange(candidate.Channel.ID, candidate.Rate.ModelName, "removed", 1, "BOG test", time.Now()); err != nil {
		t.Fatalf("remove upstream group: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(candidate.Channel.ID, candidate.Rate.ModelName, "added", 2, "BOG replacement", time.Now().Add(time.Second)); err != nil {
		t.Fatalf("re-add upstream group: %v", err)
	}
	replacementGeneration, err := store.CaptureUpstreamGroupGeneration(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("capture replacement generation: %v", err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(candidate.Channel.ID, candidate.Rate.ModelName, "", "", time.Now().UTC(), "", replacementGeneration); err != nil {
		t.Fatalf("clear cached model facts: %v", err)
	}
	key, err := store.FindUpstreamKeyAny(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("find replacement key: %v", err)
	}
	if key.ModelFamilies != "" || key.ModelIDs != "" {
		t.Fatalf("cached model facts remained after clear: %+v", key)
	}

	var modelRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("model probe path = %q", r.URL.Path)
		}
		modelRequests.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid api key"}`))
	}))
	defer upstream.Close()

	deps := &Deps{Gateway: svc}
	keyLease, err := svc.FixedUpstreamAPIKeyLeaseForGroup(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("load replacement key lease: %v", err)
	}
	dispatchFence, err := gateway.UpstreamDispatchFenceForCandidate(candidate, keyLease, storage.GatewayAppCodex, "")
	if err != nil {
		t.Fatalf("build model probe dispatch fence: %v", err)
	}
	known, allowed := gatewayCandidateAllowsApp(
		context.Background(), deps, candidate, keyLease,
		gatewayResolvedTarget{SiteURL: upstream.URL}, "sk-test", storage.GatewayAppCodex,
		gatewayCandidateModelFactGenerationArgs(candidate), dispatchFence,
	)
	if known || !allowed || modelRequests.Load() != 1 {
		t.Fatalf("stale candidate model probe result known=%t allowed=%t requests=%d", known, allowed, modelRequests.Load())
	}
	key, err = store.FindUpstreamKeyAny(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("find key after stale model probe: %v", err)
	}
	if !key.Enabled || key.APIKeyCipher == "" || key.ModelsError != "" {
		t.Fatalf("stale model/key facts mutated replacement: %+v", key)
	}

	candidate.KeyStateGeneration = replacementGeneration
	known, allowed = gatewayCandidateAllowsApp(
		context.Background(), deps, candidate, keyLease,
		gatewayResolvedTarget{SiteURL: upstream.URL}, "sk-test", storage.GatewayAppCodex,
		gatewayCandidateModelFactGenerationArgs(candidate), dispatchFence,
	)
	if known || !allowed || modelRequests.Load() != 2 {
		t.Fatalf("current key-state probe result known=%t allowed=%t requests=%d", known, allowed, modelRequests.Load())
	}
	key, err = store.FindUpstreamKeyAny(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("find key after current key-state probe: %v", err)
	}
	if key.Enabled || key.APIKeyCipher != "" || key.ModelsError != "" {
		t.Fatalf("key-state/model-fact fence mismatch: %+v", key)
	}
}

func TestGatewayForwardFixedNoRateSuccessRemainsUncovered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(
			"event: response.output_text.delta\n" +
				"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"fixed no-rate success\"}\n\n" +
				"event: response.completed\n" +
				"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_fixed_no_rate\",\"status\":\"completed\",\"error\":null,\"usage\":{\"input_tokens\":11,\"output_tokens\":7,\"total_tokens\":18}}}\n\n",
		))
	}))
	defer upstream.Close()

	db := openTestDB(t)
	svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
		channelID: 1, name: "manual", groupName: "manual", siteURL: upstream.URL, ratio: 1,
	}})
	decision.SmartRouting = false
	candidate := &decision.Candidates[0]
	candidate.ModelFactGeneration = candidate.UpstreamGeneration
	candidate.KeyStateGeneration = candidate.UpstreamGeneration
	candidate.UpstreamGeneration = storage.UpstreamGroupGeneration{}
	candidate.Rate = storage.RateSnapshot{ChannelID: candidate.Channel.ID, ModelName: candidate.Rate.ModelName}
	if err := db.Where("channel_id = ? AND model_name = ?", candidate.Channel.ID, candidate.Rate.ModelName).Delete(&storage.RateSnapshot{}).Error; err != nil {
		t.Fatalf("remove current rate snapshot: %v", err)
	}
	if err := store.RecordUpstreamKeyRoutingFailure(candidate.Channel.ID, candidate.Rate.ModelName, gateway.RoutingFailureHTTPStatus, http.StatusServiceUnavailable, "prior smart failure", time.Now()); err != nil {
		t.Fatalf("seed routing health: %v", err)
	}

	startedAt := time.Now().UTC()
	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`, false)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fixed no-rate success") {
		t.Fatalf("fixed no-rate response=%d body=%q", recorder.Code, recorder.Body.String())
	}
	key, err := store.FindUpstreamKeyAny(candidate.Channel.ID, candidate.Rate.ModelName)
	if err != nil {
		t.Fatalf("find key after fixed no-rate success: %v", err)
	}
	if key.LastRoutingFailureKind != gateway.RoutingFailureHTTPStatus || key.LastRoutingFailureStatus != http.StatusServiceUnavailable || key.RoutingFailureCount == 0 || key.LastUsedAt != nil {
		t.Fatalf("fixed no-rate success mutated shared routing health: kind=%q status=%d count=%d last_used=%v", key.LastRoutingFailureKind, key.LastRoutingFailureStatus, key.RoutingFailureCount, key.LastUsedAt)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if err != nil || len(logs) != 1 {
		t.Fatalf("fixed no-rate request logs=%+v err=%v", logs, err)
	}
	if logs[0].StatusCode != http.StatusOK || logs[0].PromptTokens != 11 || logs[0].CompletionTokens != 7 {
		t.Fatalf("fixed no-rate successful token denominator was not retained: %+v", logs[0])
	}
	persistedLog := gatewayRequestLogForTest(t, db, logs[0].ID)
	assertGatewayScopeIncompletePricing(t, &persistedLog)
	var audits []storage.GatewayRequestDispatchAudit
	if err := db.Find(&audits).Error; err != nil || len(audits) != 1 {
		t.Fatalf("fixed no-rate dispatch audits=%+v err=%v", audits, err)
	}
	if audits[0].PricingUnavailableReason != storage.GatewayPricingUnavailableScopeIncomplete ||
		audits[0].ChannelIdentityRevision != 0 || audits[0].UpstreamGroupID != 0 || audits[0].UpstreamGroupGeneration != 0 ||
		audits[0].UpstreamKeyLocalID != 0 || audits[0].UpstreamKeyGeneration != 0 {
		t.Fatalf("fixed no-rate audit was not hard-fenced: %+v", audits[0])
	}
	point := gatewayMultiplierTrendObservedPointForTest(t, store, startedAt.Add(-time.Hour), time.Now().UTC().Add(time.Hour))
	if point.SuccessfulRequests != 1 || point.InputTokens != 11 || point.OutputTokens != 7 ||
		point.InputCoveredTokens != 0 || point.OutputCoveredTokens != 0 || point.InputSampleRequests != 0 ||
		point.OutputSampleRequests != 0 || point.InputMultiplier != nil || point.OutputMultiplier != nil ||
		point.SmoothedInputMultiplier != nil || point.SmoothedOutputMultiplier != nil {
		t.Fatalf("fixed no-rate success did not remain an uncovered denominator: %+v", point)
	}
}

func TestGatewayForwardPersistedZeroRateRemainsCovered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(
			"event: response.output_text.delta\n" +
				"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"zero-rate success\"}\n\n" +
				"event: response.completed\n" +
				"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_zero_rate\",\"status\":\"completed\",\"error\":null,\"usage\":{\"input_tokens\":13,\"output_tokens\":5,\"total_tokens\":18}}}\n\n",
		))
	}))
	defer upstream.Close()

	db := openTestDB(t)
	svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
		channelID: 1, name: "manual", groupName: "free-credit", siteURL: upstream.URL, ratio: 0,
	}})
	decision.SmartRouting = false
	if decision.Candidates[0].Rate.ID == 0 || !decision.Candidates[0].UpstreamGeneration.Valid() {
		t.Fatalf("zero-rate fixture is not a persisted rate fact: %+v", decision.Candidates[0])
	}
	startedAt := time.Now().UTC()
	recorder := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`, false)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "zero-rate success") {
		t.Fatalf("zero-rate response=%d body=%q", recorder.Code, recorder.Body.String())
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if err != nil || len(logs) != 1 {
		t.Fatalf("zero-rate request logs=%+v err=%v", logs, err)
	}
	persistedLog := gatewayRequestLogForTest(t, db, logs[0].ID)
	if persistedLog.PromptTokens != 13 || persistedLog.CompletionTokens != 5 ||
		persistedLog.ChannelIdentityRevision == 0 || persistedLog.UpstreamGroupID == 0 || persistedLog.UpstreamGroupGeneration == 0 ||
		persistedLog.UpstreamKeyLocalID == 0 || persistedLog.UpstreamKeyGeneration == 0 ||
		persistedLog.CostUnavailableReason == storage.GatewayPricingUnavailableScopeIncomplete ||
		persistedLog.CostUnavailableReason == storage.GatewayPricingUnavailableIdentityChanged {
		t.Fatalf("persisted zero-rate request lost complete provenance: %+v", persistedLog)
	}
	var audits []storage.GatewayRequestDispatchAudit
	if err := db.Find(&audits).Error; err != nil || len(audits) != 1 {
		t.Fatalf("zero-rate dispatch audits=%+v err=%v", audits, err)
	}
	if audits[0].GroupRatio != 0 || audits[0].CompletionRatio != 0 || audits[0].ChannelIdentityRevision == 0 ||
		audits[0].UpstreamGroupID == 0 || audits[0].UpstreamGroupGeneration == 0 || audits[0].UpstreamKeyLocalID == 0 ||
		audits[0].UpstreamKeyGeneration == 0 || audits[0].PricingUnavailableReason == storage.GatewayPricingUnavailableScopeIncomplete ||
		audits[0].PricingUnavailableReason == storage.GatewayPricingUnavailableIdentityChanged {
		t.Fatalf("persisted zero-rate audit was hard-fenced: %+v", audits[0])
	}
	point := gatewayMultiplierTrendObservedPointForTest(t, store, startedAt.Add(-time.Hour), time.Now().UTC().Add(time.Hour))
	if point.SuccessfulRequests != 1 || point.InputTokens != 13 || point.OutputTokens != 5 ||
		point.InputCoveredTokens != 13 || point.OutputCoveredTokens != 5 || point.InputSampleRequests != 1 ||
		point.OutputSampleRequests != 1 || point.InputMultiplier == nil || *point.InputMultiplier != 0 ||
		point.OutputMultiplier == nil || *point.OutputMultiplier != 0 || point.SmoothedInputMultiplier == nil ||
		*point.SmoothedInputMultiplier != 0 || point.SmoothedOutputMultiplier == nil || *point.SmoothedOutputMultiplier != 0 {
		t.Fatalf("persisted zero-rate request was not covered at 0x: %+v", point)
	}
}

func TestGatewayForwardDirectTerminalBeforeLeaseIsPricingFenced(t *testing.T) {
	for _, test := range []struct {
		name       string
		cancel     bool
		smartRoute bool
	}{
		{name: "client canceled while acquiring lease", cancel: true, smartRoute: true},
		{name: "fixed lease failure", smartRoute: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			db := openTestDB(t)
			svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
				channelID: 1, name: "route", groupName: "primary", siteURL: "https://primary.example", ratio: 1,
			}})
			decision.SmartRouting = test.smartRoute
			key, err := store.FindUpstreamKeyAny(1, "primary")
			if err != nil {
				t.Fatalf("load upstream key: %v", err)
			}
			key.Enabled = false
			if err := store.UpsertUpstreamKey(key); err != nil {
				t.Fatalf("make upstream key unavailable: %v", err)
			}

			recorder := httptest.NewRecorder()
			ginContext, _ := gin.CreateTestContext(recorder)
			body := `{"model":"gpt-test"}`
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
			if test.cancel {
				requestContext, cancel := context.WithCancel(request.Context())
				cancel()
				request = request.WithContext(requestContext)
			}
			ginContext.Request = request
			gatewayForward(
				ginContext, &Deps{Gateway: svc},
				&storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: test.smartRoute},
				decision, "gpt-test", "", []byte(body), gatewayModelMappingResult{Model: "gpt-test"}, time.Now(),
			)
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
			if err != nil || len(logs) != 1 {
				t.Fatalf("direct terminal logs=%+v err=%v", logs, err)
			}
			persistedLog := gatewayRequestLogForTest(t, db, logs[0].ID)
			assertGatewayScopeIncompletePricing(t, &persistedLog)
			if test.cancel && (logs[0].StatusCode != gatewayClientClosedStatus || logs[0].RouteReason != gatewayClientCanceledRouteReason(false)) {
				t.Fatalf("lease cancellation terminal classification = %+v", logs[0])
			}
			if !test.cancel && logs[0].StatusCode != http.StatusBadGateway {
				t.Fatalf("fixed lease failure terminal classification = %+v", logs[0])
			}
			var auditCount int64
			if err := db.Model(&storage.GatewayRequestDispatchAudit{}).Count(&auditCount).Error; err != nil || auditCount != 0 {
				t.Fatalf("pre-lease terminal unexpectedly created dispatch audits=%d err=%v", auditCount, err)
			}
		})
	}
}

func gatewayMultiplierTrendObservedPointForTest(t *testing.T, store *storage.Gateway, start, end time.Time) storage.DashboardMultiplierTrendPoint {
	t.Helper()
	trend, err := store.DashboardMultiplierTrend(start, end, "UTC")
	if err != nil {
		t.Fatalf("dashboard multiplier trend: %v", err)
	}
	var observed *storage.DashboardMultiplierTrendPoint
	for _, app := range trend.Apps {
		if app.App != string(storage.GatewayAppCodex) {
			continue
		}
		for index := range app.Points {
			if app.Points[index].SuccessfulRequests == 0 {
				continue
			}
			if observed != nil {
				t.Fatalf("request unexpectedly crossed multiplier buckets: %+v", app.Points)
			}
			point := app.Points[index]
			observed = &point
		}
	}
	if observed == nil {
		t.Fatalf("dashboard multiplier trend omitted successful request: %+v", trend)
	}
	return *observed
}

func gatewayRequestLogForTest(t *testing.T, db *gorm.DB, id uint) storage.GatewayRequestLog {
	t.Helper()
	var log storage.GatewayRequestLog
	if err := db.First(&log, id).Error; err != nil {
		t.Fatalf("load persisted gateway request log %d: %v", id, err)
	}
	return log
}

func TestGatewayForwardFreezesRouteIdentityBeforeUpstreamResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var store *storage.Gateway
	var authorization string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		key, err := store.FindUpstreamKey(1, "default")
		if err != nil {
			t.Errorf("find upstream key in handler: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		key.UpstreamKeyID = 99
		key.UpstreamKeyName = "BOG rotated"
		if err := store.UpsertUpstreamKey(key); err != nil {
			t.Errorf("rotate upstream key in handler: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"ok\"}\n\n"))
	}))
	defer upstream.Close()

	svc, gatewayStore, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "sheapi", groupName: "default", siteURL: upstream.URL, ratio: 1,
	}})
	store = gatewayStore
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK {
		t.Fatalf("gateway response = %d %s", recorder.Code, recorder.Body.String())
	}
	logs, err := gatewayStore.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("list logs = %+v, %v", logs, err)
	}
	if logs[0].ChannelName != "sheapi" || logs[0].UpstreamKeyID != 1 || logs[0].UpstreamKeyName != "BOG test" {
		t.Fatalf("route snapshot changed after dispatch: %+v", logs[0])
	}
	if authorization != "Bearer sk-candidate-1" {
		t.Fatalf("upstream authorization = %q, want the frozen key", authorization)
	}
}

func TestGatewayForwardDoesNotTouchRecreatedUpstreamGroup(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var store *storage.Gateway
	var channelID uint
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now().UTC().Truncate(time.Microsecond)
		if err := store.RecordUpstreamGroupChange(channelID, "primary", "removed", 1, "old key", now); err != nil {
			t.Errorf("remove upstream group: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := store.DeleteGroupRoutingState(channelID, "primary"); err != nil {
			t.Errorf("delete old routing state: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := store.RecordUpstreamGroupChange(channelID, "primary", "added", 2, "new key", now.Add(time.Second)); err != nil {
			t.Errorf("add recreated upstream group: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channelID, GroupName: "primary", APIKeyCipher: "new-cipher", APIKeyPrefix: "new",
			ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true,
		}); err != nil {
			t.Errorf("create new upstream key: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_ok","output":[]}`))
	}))
	defer upstream.Close()

	svc, gatewayStore, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "primary", groupName: "primary", siteURL: upstream.URL, ratio: 1,
	}})
	store = gatewayStore
	channelID = decision.Candidates[0].Channel.ID
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test"}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK {
		t.Fatalf("gateway response = %d %s", recorder.Code, recorder.Body.String())
	}
	current, err := gatewayStore.FindUpstreamKeyAny(channelID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if !current.Enabled || current.APIKeyCipher != "new-cipher" || current.LastUsedAt != nil {
		t.Fatalf("stale request touched recreated key: %+v", current)
	}
	capability, err := gatewayStore.FindModelCapability(channelID, "primary", "gpt-test")
	if err != nil || capability != nil {
		t.Fatalf("stale request wrote recreated capability: %+v err=%v", capability, err)
	}
}

func TestGatewayRouteAttemptedChannelsKeepsOnlyFailedCandidates(t *testing.T) {
	decision := &gatewayRouteDecision{
		SmartRouting: true,
		Exclusions:   []gateway.CandidateExclusion{{ChannelName: "filtered"}},
		RankedCandidates: []gateway.Candidate{
			{Channel: storage.Channel{ID: 1, Name: "first"}, Rate: storage.RateSnapshot{ModelName: "cheap"}},
			{Channel: storage.Channel{ID: 2, Name: "second"}, Rate: storage.RateSnapshot{ModelName: "backup"}},
			{Channel: storage.Channel{ID: 3, Name: "fallback"}, Rate: storage.RateSnapshot{ModelName: "stable"}},
		},
	}
	attempts := []gatewayRouteAttempt{
		{ChannelID: 1, ChannelName: "first", GroupName: "cheap", Reason: "upstream_status", StatusCode: http.StatusServiceUnavailable, Physical: true, FirstByteBudgetMs: 30_000},
		{ChannelID: 2, ChannelName: "second", GroupName: "backup", Reason: "stream_error", Physical: true, FirstByteBudgetMs: 90_000},
	}
	selected := gateway.Candidate{Channel: storage.Channel{ID: 3, Name: "fallback"}}
	raw := gatewayRouteAttemptedChannels(decision, attempts, selected)
	var traces []gatewayRouteAttemptTrace
	if err := json.Unmarshal([]byte(raw), &traces); err != nil {
		t.Fatalf("decode attempted channels %q: %v", raw, err)
	}
	if len(traces) != 2 || traces[0].ChannelName != "first" || traces[0].GroupName != "cheap" || traces[0].EligibleRank != 1 || traces[0].StatusCode != http.StatusServiceUnavailable || !traces[0].Physical || traces[0].FirstByteBudgetMs != 30_000 || traces[1].ChannelName != "second" || traces[1].EligibleRank != 2 || !traces[1].Physical || traces[1].FirstByteBudgetMs != 90_000 {
		t.Fatalf("attempted channels = %#v", traces)
	}
	if strings.Contains(raw, "filtered") || strings.Contains(raw, "fallback") {
		t.Fatalf("attempted channels included non-attempted candidates: %q", raw)
	}
}

func TestGatewayUpstreamRequestIDUsesProviderAuthoritativeHeader(t *testing.T) {
	headers := http.Header{}
	headers.Set("X-Client-Request-ID", "client:sub2-request")
	headers.Set("X-Oneapi-Request-Id", "oneapi-request")
	headers.Set("X-Request-ID", "generic-request")
	if got, err := gatewayUpstreamRequestID(headers, storage.ChannelTypeSub2API); err != nil || got != "client:sub2-request" {
		t.Fatalf("Sub2API request id = %q err=%v, want client:sub2-request", got, err)
	}
	if got, err := gatewayUpstreamRequestID(headers, storage.ChannelTypeNewAPI); err != nil || got != "oneapi-request" {
		t.Fatalf("NewAPI request id = %q err=%v, want oneapi-request", got, err)
	}

	wrongHeaders := http.Header{}
	wrongHeaders.Set("X-Oneapi-Request-Id", "wrong-for-sub2")
	if got, err := gatewayUpstreamRequestID(wrongHeaders, storage.ChannelTypeSub2API); err != nil || got != "" {
		t.Fatalf("Sub2API accepted non-authoritative request id %q err=%v", got, err)
	}
	wrongHeaders = http.Header{}
	wrongHeaders.Set("X-Client-Request-ID", "wrong-for-newapi")
	if got, err := gatewayUpstreamRequestID(wrongHeaders, storage.ChannelTypeNewAPI); err != nil || got != "" {
		t.Fatalf("NewAPI accepted non-authoritative request id %q err=%v", got, err)
	}

	for _, test := range []struct {
		name     string
		provider storage.ChannelType
		header   string
		value    string
	}{
		{name: "sub2api", provider: storage.ChannelTypeSub2API, header: "X-Client-Request-ID", value: "client:duplicate-request"},
		{name: "newapi", provider: storage.ChannelTypeNewAPI, header: "X-Oneapi-Request-Id", value: "duplicate-request"},
	} {
		t.Run("identical duplicate "+test.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Add(test.header, test.value)
			headers.Add(test.header, test.value)
			if got, err := gatewayUpstreamRequestID(headers, test.provider); err != nil || got != test.value {
				t.Fatalf("duplicate %s request ID = %q err=%v, want exact %q", test.name, got, err, test.value)
			}
		})
		t.Run("conflicting duplicate "+test.name, func(t *testing.T) {
			headers := http.Header{}
			headers.Add(test.header, test.value)
			headers.Add(test.header, test.value+"-other")
			if got, err := gatewayUpstreamRequestID(headers, test.provider); err == nil || got != "" {
				t.Fatalf("conflicting %s request IDs = %q err=%v, want fail-closed rejection", test.name, got, err)
			}
		})
	}

	for name, value := range map[string]string{
		"legacy length":          strings.Repeat("a", 128),
		"maximum ascii":          strings.Repeat("a", gatewayUpstreamRequestIDMaxLength),
		"maximum utf8":           strings.Repeat("界", 170) + "aa",
		"surrounding whitespace": " provider-id\t",
		"unicode whitespace":     "\u00a0provider-id\u00a0",
	} {
		headers := http.Header{}
		headers.Set("X-Oneapi-Request-Id", value)
		if got, err := gatewayUpstreamRequestID(headers, storage.ChannelTypeNewAPI); err != nil || got != value {
			t.Fatalf("%s request id bytes=%d got=%q err=%v", name, len(value), got, err)
		}
	}
	for name, value := range map[string]string{
		"513 ascii bytes":           strings.Repeat("a", gatewayUpstreamRequestIDMaxLength+1),
		"512 bytes plus whitespace": strings.Repeat("a", gatewayUpstreamRequestIDMaxLength) + " ",
		"513 utf8 bytes":            strings.Repeat("界", 171),
		"invalid utf8":              string([]byte{'a', 0xff, 'b'}),
	} {
		headers := http.Header{}
		headers.Set("X-Oneapi-Request-Id", value)
		if got, err := gatewayUpstreamRequestID(headers, storage.ChannelTypeNewAPI); err == nil || got != "" {
			t.Fatalf("%s request id bytes=%d got=%q err=%v, want explicit rejection without truncation", name, len(value), got, err)
		}
	}
}

func TestGatewayPriorityScoreIsZeroToOneHundredAndHigherIsBetter(t *testing.T) {
	tests := []struct {
		name  string
		input float64
		want  float64
	}{
		{name: "best", input: 0, want: 100},
		{name: "weighted", input: 0.15, want: 85},
		{name: "middle", input: 0.5, want: 50},
		{name: "worst", input: 1, want: 0},
		{name: "clamp below", input: -0.1, want: 100},
		{name: "clamp above", input: 1.1, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := gatewayPriorityScore(tt.input); got != tt.want {
				t.Fatalf("priority score = %.2f, want %.2f", got, tt.want)
			}
		})
	}
}

func TestGatewayModelUnsupportedDetection(t *testing.T) {
	cases := []string{
		`{"error":{"message":"Model \"gpt-4o-mini\" is not supported by any configured account in this group","type":"model_not_found"}}`,
		`{"error":{"message":"model not found"}}`,
		`{"error":{"message":"gpt-test is not supported"}}`,
	}
	for _, body := range cases {
		if !isGatewayModelUnsupported(body) {
			t.Fatalf("body %q was not detected as model unsupported", body)
		}
	}

	if isGatewayModelUnsupported(`{"error":{"message":"quota exceeded"}}`) {
		t.Fatal("quota exceeded should not be detected as model unsupported")
	}
}

func TestSupportedGatewayPathIncludesClaudeMessages(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/v1/messages/count_tokens"} {
		if !supportedGatewayPath(path) {
			t.Fatalf("path %s should be supported", path)
		}
	}
}

func TestGatewayAppAllowsOnlyMatchingPaths(t *testing.T) {
	if !gatewayAppAllowsPath(storage.GatewayAppCodex, "/v1/chat/completions") {
		t.Fatal("codex key should allow chat completions")
	}
	if !gatewayAppAllowsPath(storage.GatewayAppGemini, "/v1/chat/completions") {
		t.Fatal("gemini key should allow chat completions")
	}
	if gatewayAppAllowsPath(storage.GatewayAppCodex, "/v1/messages") {
		t.Fatal("codex key should not allow claude messages")
	}
	if gatewayAppAllowsPath(storage.GatewayAppGemini, "/v1/messages") {
		t.Fatal("gemini key should not allow claude messages")
	}
	if !gatewayAppAllowsPath(storage.GatewayAppClaude, "/v1/messages") {
		t.Fatal("claude key should allow messages")
	}
	if gatewayAppAllowsPath(storage.GatewayAppClaude, "/v1/chat/completions") {
		t.Fatal("claude key should not allow chat completions")
	}
}

func TestGatewayModelFamiliesFromItems(t *testing.T) {
	families := gatewayModelFamiliesFromItems([]map[string]any{
		{"id": "gpt-4o"},
		{"id": "claude-sonnet-4-5"},
		{"id": "gemini-2.5-pro"},
		{"id": "grok-3-mini"},
		{"id": "deepseek-chat"},
		{"id": "Krio90"},
		{"id": "unknown-video-model"},
	})
	got := strings.Join(families, ",")
	want := "codex,grok,deepseek,claude,gemini,other"
	if got != want {
		t.Fatalf("families = %q, want %q", got, want)
	}
}

func TestExportGatewayRequestLogsCSVUsesFilters(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	channel := storage.Channel{Name: "cheap", Type: storage.ChannelTypeSub2API, GatewayApp: storage.GatewayAppCodex, SiteURL: "https://cheap.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	now := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	if _, err := channels.ObserveUpstreamAccountIdentity(storage.UpstreamAccountIdentityObservation{
		ChannelID: channel.ID, ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channel.AdminRevision, Provider: channel.Type, APIOrigin: channel.SiteURL,
		AccountID: "csv-export-account", Scope: storage.UpstreamConcurrencyScopeAccount,
		Source: "gateway-csv-test.authenticated-account", ObservedAt: now.Add(-time.Hour),
	}); err != nil {
		t.Fatalf("bind CSV pricing account: %v", err)
	}
	if _, err := storage.NewRates(db).Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "cheap", Ratio: 1, LastSeenAt: now.Add(-time.Minute)}); err != nil {
		t.Fatalf("create CSV pricing group rate: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "cheap", "added", 77, "BridgeOps Gateway - cheap", now.Add(-time.Minute)); err != nil {
		t.Fatalf("create CSV pricing group identity: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "cheap")
	if err != nil {
		t.Fatalf("capture CSV pricing group generation: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "cheap", UpstreamKeyID: 77, UpstreamKeyName: "BridgeOps Gateway - cheap",
		APIKeyCipher: "cipher", APIKeyPrefix: "sk-test", ModelFamilies: "codex", ModelIDs: "gpt-5-mini", Enabled: true,
	}); err != nil {
		t.Fatalf("create CSV pricing upstream key: %v", err)
	}
	upstreamKey, err := store.FindUpstreamKey(channel.ID, "cheap")
	if err != nil {
		t.Fatalf("reload CSV pricing upstream key: %v", err)
	}
	if err := db.Select("id", "upstream_account_identity_revision").First(&channel, channel.ID).Error; err != nil {
		t.Fatalf("reload CSV pricing channel identity: %v", err)
	}
	inputPrice, zeroPrice := 2.0, 0.0
	priceFact := storage.GatewayRequestPriceFact{
		ChannelID: channel.ID, ChannelAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, CostUnitEpoch: 1,
		UpstreamGroupID: generation.ID, CanonicalGroupName: generation.CanonicalGroupName, UpstreamGroupPresenceGeneration: generation.PresenceGeneration,
		GatewayUpstreamKeyID: upstreamKey.ID, UpstreamKeyGeneration: upstreamKey.KeyGeneration,
		ModelName: "gpt-5-mini", Operation: storage.GatewayPricingOperationTextGeneration, Protocol: storage.GatewayPricingProtocolOpenAIResponses,
		InputUnitPrice: &inputPrice, OutputUnitPrice: &zeroPrice, CacheReadUnitPrice: &zeroPrice, CacheWriteUnitPrice: &zeroPrice,
		Unit: storage.GatewayPricingUnitPerMillionTokens, Currency: "USD", Source: "test_fixture",
		ObservedAt: now.Add(-30 * time.Minute), ValidFrom: now.Add(-30 * time.Minute),
	}
	if err := store.AppendRequestPriceFact(&priceFact); err != nil {
		t.Fatalf("create CSV export price fact: %v", err)
	}
	attachPriceFact := func(log *storage.GatewayRequestLog, usageComplete bool) {
		factID, observedAt, validFrom := priceFact.ID, priceFact.ObservedAt, priceFact.ValidFrom
		log.ChannelIdentityRevision = priceFact.ChannelAccountIdentityRevision
		log.UpstreamGroupID = priceFact.UpstreamGroupID
		log.UpstreamGroupGeneration = priceFact.UpstreamGroupPresenceGeneration
		log.UpstreamKeyLocalID = priceFact.GatewayUpstreamKeyID
		log.UpstreamKeyGeneration = priceFact.UpstreamKeyGeneration
		log.PriceFactID = &factID
		log.PriceCostUnitEpoch = priceFact.CostUnitEpoch
		log.PriceFactVersion = priceFact.FactVersion
		log.PriceFactSchemaVersion = priceFact.SchemaVersion
		log.PriceFactChecksum = priceFact.Checksum
		log.PriceCanonicalGroupName = priceFact.CanonicalGroupName
		log.PriceModelName = priceFact.ModelName
		log.PriceOperation = priceFact.Operation
		log.PriceProtocol = priceFact.Protocol
		log.PriceInputUnit = priceFact.InputUnitPrice
		log.PriceOutputUnit = priceFact.OutputUnitPrice
		log.PriceCacheReadUnit = priceFact.CacheReadUnitPrice
		log.PriceCacheWriteUnit = priceFact.CacheWriteUnitPrice
		log.PriceCurrency = priceFact.Currency
		log.PriceUnit = priceFact.Unit
		log.PriceSource = priceFact.Source
		log.PriceObservedAt = &observedAt
		log.PriceValidFrom = &validFrom
		log.UsageSchema = storage.GatewayPricingUsageSchemaTokensV1
		log.UsageSchemaVersion = 1
		log.UsageInputPresent = true
		log.UsageOutputPresent = usageComplete
		log.UsageCacheReadPresent = true
		log.UsageCacheWritePresent = true
		log.CostEstimated = true
		log.CostStatus = storage.GatewayCostStatusEstimated
		log.CostSource = "price_fact"
	}
	key := storage.GatewayDownstreamKey{Name: "codex-key", App: storage.GatewayAppCodex, KeyHash: "hash", APIKeyCipher: "cipher", Enabled: true}
	if err := store.CreateDownstreamKey(&key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	logs := []*storage.GatewayRequestLog{
		{DownstreamKeyID: key.ID, ChannelID: channel.ID, GroupName: "cheap", ModelName: "gpt-5-mini", UpstreamModelName: "gpt-5.6mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, FirstByteMs: 250, LatencyMs: 1000, PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15, Cost: 0.01, CostSource: "sub2api_actual_cost", UpstreamKeyID: 77, UpstreamKeyName: "BridgeOps Gateway - cheap", UpstreamRequestID: "upstream-request-123", RouteReason: "smart_primary", RouteDetails: "=candidate", Error: "@upstream", CreatedAt: now},
		{DownstreamKeyID: key.ID, ChannelID: channel.ID, GroupName: "cheap", ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 500, RouteReason: "latency_probe", CreatedAt: now.Add(time.Second)},
		{DownstreamKeyID: key.ID, ChannelID: channel.ID, GroupName: "cheap", ModelName: "gpt-5", Method: "POST", Path: "/v1/responses", StatusCode: 200, LatencyMs: 1000, CreatedAt: now.Add(2 * time.Second)},
		{DownstreamKeyID: key.ID, ChannelID: channel.ID, GroupName: "cheap", ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 1_000_000, Cost: 99, CostStatus: storage.GatewayCostStatusReported, CostSource: "cost", UpstreamRequestID: "priced-export", CreatedAt: now.Add(3 * time.Second)},
		{DownstreamKeyID: key.ID, ChannelID: channel.ID, GroupName: "cheap", ModelName: "gpt-5-mini", Method: "POST", Path: "/v1/responses", StatusCode: 200, PromptTokens: 1_000_000, Cost: 88, CostStatus: storage.GatewayCostStatusReported, CostSource: "cost", UpstreamRequestID: "unavailable-export", CreatedAt: now.Add(4 * time.Second)},
	}
	attachPriceFact(logs[3], true)
	attachPriceFact(logs[4], false)
	for _, log := range logs {
		if err := store.AppendRequestLog(log); err != nil {
			t.Fatalf("append log: %v", err)
		}
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs/export?model=gpt-5-mini&time_zone=Asia%2FShanghai", nil)
	userTime, err := usertime.New("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	exportGatewayRequestLogs(ctx, &Deps{Gateway: gateway.NewService(nil, nil, store, nil, nil, nil), UserTime: userTime})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.HasPrefix(body, "\ufeff") {
		t.Fatalf("csv missing BOM: %q", body[:min(len(body), 12)])
	}
	for _, want := range []string{"时间 (America/New_York),下游 Key,上游渠道", "模型,上游计费模型,思考程度", "首字时间(ms),总耗时(ms)", "上游 Key ID,上游 Key 名称,上游请求 ID", "codex-key", "cheap", "gpt-5-mini,gpt-5.6mini", "2026-07-12T06:00:00-04:00", "sub2api_actual_cost", "77,BridgeOps Gateway - cheap,upstream-request-123"} {
		if !strings.Contains(body, want) {
			t.Fatalf("csv missing %q: %s", want, body)
		}
	}
	for _, want := range []string{"'=candidate", "'@upstream"} {
		if !strings.Contains(body, want) {
			t.Fatalf("csv did not escape dangerous text %q: %s", want, body)
		}
	}
	if strings.Contains(body, "latency_probe") || strings.Contains(body, "gpt-5,") {
		t.Fatalf("csv did not apply filters/user-visible rules: %s", body)
	}
	rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(body, "\ufeff"))).ReadAll()
	if err != nil {
		t.Fatalf("parse exported csv: %v", err)
	}
	byUpstreamRequest := make(map[string][]string, len(rows))
	for _, row := range rows[1:] {
		if len(row) > 26 {
			byUpstreamRequest[row[26]] = row
		}
	}
	if row := byUpstreamRequest["priced-export"]; len(row) == 0 || row[21] != "2.00000000" || row[22] != "estimated" || row[23] != "price_fact" {
		t.Fatalf("calculated price-fact CSV row = %#v", row)
	}
	if row := byUpstreamRequest["unavailable-export"]; len(row) == 0 || row[21] != "" || row[22] != "unavailable" || row[23] != "price_fact" {
		t.Fatalf("unavailable price-fact CSV row = %#v", row)
	}
}

func TestParseGatewayRequestLogFilterSupportsResultRouteAndSort(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs?result=CANCELED&route_reason=fallback&sort_by=latency_ms&sort_order=ASC&time_zone=Asia%2FShanghai&upstream_request_id=req-123&upstream_key_name=BridgeOps%20Gateway", nil)
	userTime, err := usertime.New("America/New_York")
	if err != nil {
		t.Fatal(err)
	}

	filter, err := parseGatewayRequestLogFilter(ctx, &Deps{UserTime: userTime})
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	if filter.Result != storage.GatewayRequestResultCanceled || filter.RouteReason != "fallback" || filter.SortBy != storage.GatewayRequestLogSortLatencyMs || filter.SortOrder != storage.GatewayRequestLogSortAscending || filter.TimeZone != "America/New_York" || filter.UpstreamRequestID != "req-123" || filter.UpstreamKeyName != "BridgeOps Gateway" {
		t.Fatalf("filter = %+v", filter)
	}
}

func TestParseGatewayRequestLogFilterPreservesProviderRequestIDBytes(t *testing.T) {
	for _, test := range []struct {
		name  string
		query string
		want  string
	}{
		{name: "ASCII whitespace", query: "%20provider-id%09", want: " provider-id\t"},
		{name: "Unicode whitespace", query: "%20provider-id%C2%A0", want: " provider-id\u00a0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs?upstream_request_id="+test.query, nil)
			filter, err := parseGatewayRequestLogFilter(ctx, nil)
			if err != nil {
				t.Fatalf("parse filter: %v", err)
			}
			if filter.UpstreamRequestID != test.want {
				t.Fatalf("provider request ID = %q, want byte-exact %q", filter.UpstreamRequestID, test.want)
			}
		})
	}
}

func TestParseGatewayRequestLogFilterDefaultsToConfiguredTimeZone(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs", nil)
	userTime, err := usertime.New("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	filter, err := parseGatewayRequestLogFilter(ctx, &Deps{UserTime: userTime})
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	if filter.TimeZone != "America/New_York" {
		t.Fatalf("default timezone = %q", filter.TimeZone)
	}
}

func TestParseGatewayRequestLogFilterUsesConfiguredTimeZoneForDateBounds(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs?start=2026-07-19&end=2026-07-20&time_zone=Asia%2FShanghai", nil)
	userTime, err := usertime.New("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	filter, err := parseGatewayRequestLogFilter(ctx, &Deps{UserTime: userTime})
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	wantStart := time.Date(2026, 7, 19, 4, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 7, 20, 4, 0, 0, 0, time.UTC)
	if filter.Start == nil || filter.End == nil || !filter.Start.Equal(wantStart) || !filter.End.Equal(wantEnd) {
		t.Fatalf("date bounds = %v .. %v", filter.Start, filter.End)
	}
}

func TestParseGatewayRequestLogFilterNormalizesAbsoluteTimesToUTC(t *testing.T) {
	userTime, err := usertime.New("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name  string
		query string
		want  time.Time
	}{
		{
			name:  "UTC instant",
			query: "/api/gateway/request-logs?start=2026-07-19T00%3A00%3A00Z",
			want:  time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "equivalent offset instant",
			query: "/api/gateway/request-logs?start=2026-07-19T00%3A00%3A00-04%3A00",
			want:  time.Date(2026, 7, 19, 4, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, tt.query, nil)

			filter, err := parseGatewayRequestLogFilter(ctx, &Deps{UserTime: userTime})
			if err != nil {
				t.Fatalf("parse filter: %v", err)
			}
			if filter.Start == nil || !filter.Start.Equal(tt.want) {
				t.Fatalf("start = %v, want %v", filter.Start, tt.want)
			}
		})
	}
}

func TestListGatewayRequestLogsExposesTraceMetadataWithoutKeyMaterial(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	store := storage.NewGateway(db)
	keyGroup := &storage.GatewayKeyGroup{Name: "Dev"}
	if err := store.SaveKeyGroup(keyGroup); err != nil {
		t.Fatalf("create key group: %v", err)
	}
	downstreamKey := &storage.GatewayDownstreamKey{
		Name:         "Claude",
		App:          storage.GatewayAppCodex,
		ModelFamily:  "codex",
		KeyGroupID:   keyGroup.ID,
		KeyHash:      "request-log-key",
		KeyPrefix:    "sk-test",
		APIKeyCipher: "cipher",
		Enabled:      true,
		SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(downstreamKey); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{
		DownstreamKeyID:                downstreamKey.ID,
		ModelName:                      "gpt-5-mini",
		Method:                         "POST",
		Path:                           "/v1/responses",
		StatusCode:                     200,
		FirstByteMs:                    123,
		UpstreamKeyID:                  88,
		UpstreamKeyName:                "Gateway Key",
		UpstreamRequestID:              "req-visible",
		RouteReason:                    gatewayRouteReasonFallback,
		RouteSelectionRank:             2,
		SessionAffinityOriginRequestID: 41,
		RouteDetails:                   "upstream errors: primary/cheap: capacity exceeded; selected fallback/stable",
		RouteAttemptedChannels:         `[{"channel_id":1,"channel_name":"primary","group_name":"cheap","reason":"stream_account_limit","summary":"capacity exceeded","status_code":429,"physically_dispatched":true,"first_byte_budget_ms":30000}]`,
		CreatedAt:                      time.Now(),
	}); err != nil {
		t.Fatalf("append log: %v", err)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs?upstream_request_id=req-visible&route_reason=fallback", nil)
	listGatewayRequestLogs(ctx, &Deps{Gateway: gateway.NewService(nil, nil, store, nil, nil, nil)})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{`"downstream_key_name":"Claude"`, `"downstream_key_group_name":"Dev"`, `"first_byte_ms":123`, `"upstream_key_id":88`, `"upstream_key_name":"Gateway Key"`, `"upstream_request_id":"req-visible"`, `"route_reason":"fallback"`, `"route_selection_rank":2`, `"session_affinity_origin_request_id":41`, `capacity exceeded`, `"route_attempts":[{"channel_id":1,"channel_name":"primary","group_name":"cheap","reason":"stream_account_limit","summary":"capacity exceeded","status_code":429,"physically_dispatched":true,"first_byte_budget_ms":30000}]`} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %s: %s", want, body)
		}
	}
	if strings.Contains(body, "route_attempted_channels") {
		t.Fatalf("response exposed raw route-attempt JSON: %s", body)
	}
	for _, forbidden := range []string{"api_key", "password_cipher", "key_cipher"} {
		if strings.Contains(strings.ToLower(body), forbidden) {
			t.Fatalf("response leaked key material field %q: %s", forbidden, body)
		}
	}
}

func TestGatewayRequestLogOutputSanitizesStructuredRouteAttempts(t *testing.T) {
	output := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
		RouteAttemptedChannels: `[{
			"channel_id":7,
			"channel_name":"首选 Bearer sk-super-secret-token",
			"group_name":"https://user:upstream-password@example.com",
				"reason":"raw upstream error with token=should-not-leak",
				"status_code":700,
				"eligible_rank":100001,
				"physically_dispatched":true,
				"first_byte_budget_ms":-1
			}]`,
	})
	if len(output.RouteAttempts) != 1 {
		t.Fatalf("structured attempts = %#v, want one item", output.RouteAttempts)
	}
	trace := output.RouteAttempts[0]
	if trace.Reason != "unknown" || trace.StatusCode != 0 || trace.EligibleRank != 0 || !trace.Physical || trace.FirstByteBudgetMs != 0 {
		t.Fatalf("unsafe trace fields were preserved: %#v", trace)
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal output: %v", err)
	}
	body := string(encoded)
	for _, forbidden := range []string{"sk-super-secret-token", "upstream-password", "should-not-leak", "route_attempted_channels"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("structured response leaked %q: %s", forbidden, body)
		}
	}
}

func TestParseGatewayRequestLogFilterAllowsEverySortColumn(t *testing.T) {
	for _, sortBy := range []string{
		storage.GatewayRequestLogSortCreatedAt,
		storage.GatewayRequestLogSortModelName,
		storage.GatewayRequestLogSortLatencyMs,
		storage.GatewayRequestLogSortTotalTokens,
		storage.GatewayRequestLogSortCost,
		storage.GatewayRequestLogSortStatusCode,
	} {
		t.Run(sortBy, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs?sort_by="+sortBy+"&sort_order=desc", nil)
			filter, err := parseGatewayRequestLogFilter(ctx, nil)
			if err != nil {
				t.Fatalf("parse filter: %v", err)
			}
			if filter.SortBy != sortBy || filter.SortOrder != storage.GatewayRequestLogSortDescending {
				t.Fatalf("filter = %+v", filter)
			}
		})
	}
}

func TestGatewayRequestLogEndpointsRejectInvalidResultAndSort(t *testing.T) {
	for _, tt := range []struct {
		name  string
		query string
	}{
		{name: "result", query: "result=unknown"},
		{name: "sort by", query: "sort_by=created_at%3BDELETE%20FROM%20gateway_request_logs"},
		{name: "sort order", query: "sort_order=sideways"},
		{name: "upstream request id too long", query: "upstream_request_id=" + strings.Repeat("r", gatewayUpstreamRequestIDMaxLength+1)},
		{name: "upstream key name too long", query: "upstream_key_name=" + strings.Repeat("k", 129)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, endpoint := range []struct {
				name    string
				path    string
				handler func(*gin.Context, *Deps)
			}{
				{name: "list", path: "/api/gateway/request-logs", handler: listGatewayRequestLogs},
				{name: "filters", path: "/api/gateway/request-log-filters", handler: gatewayRequestLogFilters},
				{name: "export", path: "/api/gateway/request-logs/export", handler: exportGatewayRequestLogs},
				{name: "analytics", path: "/api/gateway/request-log-analytics", handler: gatewayRequestLogAnalytics},
			} {
				t.Run(endpoint.name, func(t *testing.T) {
					recorder := httptest.NewRecorder()
					ctx, _ := gin.CreateTestContext(recorder)
					ctx.Request = httptest.NewRequest(http.MethodGet, endpoint.path+"?"+tt.query, nil)
					endpoint.handler(ctx, &Deps{})
					if recorder.Code != http.StatusBadRequest {
						t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
					}
				})
			}
		})
	}
}

func TestListGatewayRequestLogsEmptyResponseUsesArrays(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	store := storage.NewGateway(db)
	deps := &Deps{Gateway: gateway.NewService(nil, nil, store, nil, nil, nil)}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs?page=1&page_size=20", nil)
	listGatewayRequestLogs(ctx, deps)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var logsBody struct {
		Data struct {
			Items []storage.GatewayRequestLogItem `json:"items"`
			Total int64                           `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &logsBody); err != nil {
		t.Fatalf("decode logs response: %v", err)
	}
	if logsBody.Data.Items == nil {
		t.Fatalf("items should be [] not null: %s", recorder.Body.String())
	}
	if logsBody.Data.Total != 0 {
		t.Fatalf("total = %d, want 0", logsBody.Data.Total)
	}

	recorder = httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-log-filters", nil)
	gatewayRequestLogFilters(ctx, deps)

	if recorder.Code != http.StatusOK {
		t.Fatalf("filter status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var filtersBody struct {
		Data struct {
			Models []string `json:"models"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &filtersBody); err != nil {
		t.Fatalf("decode filters response: %v", err)
	}
	if filtersBody.Data.Models == nil {
		t.Fatalf("models should be [] not null: %s", recorder.Body.String())
	}
}

func TestGatewayAppAllowsOnlyMatchingModels(t *testing.T) {
	if !gatewayAppAllowsModel(storage.GatewayAppCodex, "gpt-4o") {
		t.Fatal("codex key should allow gpt model")
	}
	if gatewayAppAllowsModel(storage.GatewayAppClaude, "unknown-video-model") || gatewayAppAllowsModel(storage.GatewayAppGemini, "unknown-video-model") {
		t.Fatal("unknown models should not bypass Claude/Gemini family restrictions")
	}
	if !gatewayAppAllowsModel(storage.GatewayAppCodex, "grok-3-mini") || !gatewayAppAllowsModel(storage.GatewayAppCodex, "deepseek-chat") {
		t.Fatal("openai-compatible key should allow grok and deepseek models")
	}
	if gatewayAppAllowsModel(storage.GatewayAppCodex, "gemini-2.5-pro") {
		t.Fatal("codex key should not allow gemini model")
	}
	if !gatewayAppAllowsModel(storage.GatewayAppGemini, "gemini-2.5-pro") {
		t.Fatal("gemini key should allow gemini model")
	}
	if gatewayAppAllowsModel(storage.GatewayAppGemini, "gpt-4o") {
		t.Fatal("gemini key should not allow gpt model")
	}
	if !gatewayAppAllowsModel(storage.GatewayAppClaude, "claude-sonnet-4-5") {
		t.Fatal("claude key should allow claude model")
	}
	if gatewayAppAllowsModel(storage.GatewayAppClaude, "gemini-2.5-pro") {
		t.Fatal("claude key should not allow gemini model")
	}
	if !gatewayAppAllowsModel(storage.GatewayAppCodex, "vendor-new-model") {
		t.Fatal("unknown model names should be checked against actual upstream candidates, not blocked by local name rules")
	}
}

func TestGatewayModelFamilyAllowsOnlyPinnedFamily(t *testing.T) {
	if !gatewayModelFamilyAllowsModel("codex", storage.GatewayAppCodex, "gpt-5") {
		t.Fatal("GPT family should allow GPT model")
	}
	if gatewayModelFamilyAllowsModel("codex", storage.GatewayAppCodex, "grok-3") {
		t.Fatal("GPT family should reject Grok model")
	}
	if gatewayModelFamilyAllowsModel("grok", storage.GatewayAppCodex, "deepseek-chat") {
		t.Fatal("Grok family should reject DeepSeek model")
	}
	if !gatewayModelFamilyAllowsModel("claude", storage.GatewayAppClaude, "claude-sonnet-4-5") {
		t.Fatal("Claude family should allow Claude model")
	}
}

func TestReorderGatewayCandidatesForCacheAffinityPromotesRecentRoute(t *testing.T) {
	candidates := []gateway.Candidate{
		{
			Channel:        storage.Channel{ID: 1, Name: "cheap"},
			Rate:           storage.RateSnapshot{ModelName: "default"},
			EffectivePrice: 1,
		},
		{
			Channel:        storage.Channel{ID: 2, Name: "recent"},
			Rate:           storage.RateSnapshot{ModelName: "pro"},
			EffectivePrice: 1.05,
		},
	}
	got, applied := reorderGatewayCandidatesForCacheAffinity(candidates, 2, "pro")
	if !applied || got[0].Channel.ID != 2 {
		t.Fatalf("recent route was not promoted: %+v", got)
	}
}

func TestReorderGatewayCandidatesForCacheAffinityIsSessionStickyAcrossPrices(t *testing.T) {
	candidates := []gateway.Candidate{
		{
			Channel:        storage.Channel{ID: 1, Name: "cheap"},
			Rate:           storage.RateSnapshot{ModelName: "default"},
			EffectivePrice: 1,
		},
		{
			Channel:        storage.Channel{ID: 2, Name: "recent-expensive"},
			Rate:           storage.RateSnapshot{ModelName: "pro"},
			EffectivePrice: 1.2,
		},
	}
	got, applied := reorderGatewayCandidatesForCacheAffinity(candidates, 2, "pro")
	if !applied || got[0].Channel.ID != 2 {
		t.Fatalf("session route was not kept: %+v", got)
	}
}

func TestGatewayRequestSessionHashUsesStableIdentifiers(t *testing.T) {
	headerRequest := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	headerRequest.Header.Set(gatewaySessionHeader, "session-a")
	headerHash := gatewayRequestSessionHash(headerRequest, []byte(`{"conversation":"ignored"}`))
	if headerHash == "" || strings.Contains(headerHash, "session-a") {
		t.Fatalf("header session hash = %q", headerHash)
	}
	bodyRequest := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	bodyHash := gatewayRequestSessionHash(bodyRequest, []byte(`{"conversation":{"id":"session-a"}}`))
	if bodyHash != headerHash {
		t.Fatalf("same session produced different hashes: header=%q body=%q", headerHash, bodyHash)
	}
	otherHash := gatewayRequestSessionHash(bodyRequest, []byte(`{"metadata":{"conversation_id":"session-b"}}`))
	if otherHash == "" || otherHash == headerHash {
		t.Fatalf("other session hash = %q", otherHash)
	}
	if got := gatewayRequestSessionHash(bodyRequest, []byte(`{"model":"gpt-test"}`)); got != "" {
		t.Fatalf("request without session id produced hash %q", got)
	}
	if got := gatewayRequestSessionHash(bodyRequest, []byte(`{"prompt_cache_key":"shared-user-bucket"}`)); got != "" {
		t.Fatalf("prompt cache key must not be treated as a session: %q", got)
	}
}

func TestForwardGatewayRequestRemovesInternalSessionHeader(t *testing.T) {
	var forwardedSession string
	var forwardedTestToken string
	var forwardedCookie string
	var forwardedRouteHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwardedSession = r.Header.Get(gatewaySessionHeader)
		forwardedTestToken = r.Header.Get(gatewayDownstreamKeyTestHeader)
		forwardedCookie = r.Header.Get("Cookie")
		forwardedRouteHeader = r.Header.Get("X-BridgeOps-Channel")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	request.Header.Set(gatewaySessionHeader, "private-session")
	request.Header.Set(gatewayDownstreamKeyTestHeader, "private-test-token")
	request.Header.Set("Cookie", "uh_token=private-admin-session")
	request.Header.Set("X-BridgeOps-Channel", "internal-route")
	response, err := forwardGatewayRequest(request, gatewayResolvedTarget{SiteURL: server.URL}, "sk-test-upstream", []byte(`{"model":"gpt-test"}`))
	if err != nil {
		t.Fatalf("forward request: %v", err)
	}
	_ = response.Body.Close()
	if forwardedSession != "" {
		t.Fatalf("internal session header was forwarded: %q", forwardedSession)
	}
	if forwardedTestToken != "" {
		t.Fatalf("internal test token was forwarded: %q", forwardedTestToken)
	}
	if forwardedCookie != "" {
		t.Fatalf("browser cookie was forwarded: %q", forwardedCookie)
	}
	if forwardedRouteHeader != "" {
		t.Fatalf("internal route header was forwarded: %q", forwardedRouteHeader)
	}

	responseHeaders := http.Header{"Content-Type": {"text/event-stream"}, "Set-Cookie": {"upstream=value"}, "Server": {"upstream"}, "X-Upstream": {"ok"}}
	proxiedHeaders := http.Header{}
	copyGatewayHeaders(proxiedHeaders, responseHeaders)
	if proxiedHeaders.Get("Content-Type") != "text/event-stream" || proxiedHeaders.Get("Set-Cookie") != "" || proxiedHeaders.Get("X-Upstream") != "" || proxiedHeaders.Get("Server") != "" {
		t.Fatalf("proxied response headers = %#v", proxiedHeaders)
	}
}

func TestGatewayClientCanceledDetectionRequiresCanceledContext(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	if isGatewayClientCanceled(request, errors.New("upstream context canceled while reading body")) {
		t.Fatal("plain upstream error text should not be treated as client cancellation")
	}
	if isGatewayClientCanceled(request, context.Canceled) {
		t.Fatal("context.Canceled with an active request should not be treated as client cancellation")
	}

	canceledRequest := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	ctx, cancel := context.WithCancel(canceledRequest.Context())
	cancel()
	canceledRequest = canceledRequest.WithContext(ctx)
	if !isGatewayClientCanceled(canceledRequest, errors.New("write tcp: broken pipe")) {
		t.Fatal("canceled request context should be treated as client cancellation")
	}
}

func TestGatewayRequestRouteReasonMarksInternalKeyTests(t *testing.T) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	if got := gatewayRequestRouteReason(ctx, gatewayRouteReasonSmartPrimary); got != gatewayRouteReasonSmartPrimary {
		t.Fatalf("normal route reason = %q", got)
	}
	ctx.Set(gatewayDownstreamKeyTestContextKey, true)
	if got := gatewayRequestRouteReason(ctx, gatewayRouteReasonSmartPrimary); got != "api_test" {
		t.Fatalf("API test route reason = %q", got)
	}
}

func TestGatewayRouteReasonExplainsHigherRankedModelFilter(t *testing.T) {
	decision := &gatewayRouteDecision{
		SmartRouting:   true,
		RequestedModel: "gpt-test",
		Exclusions: []gateway.CandidateExclusion{{
			ChannelName: "primary", GroupName: "cheap", Reason: "model_not_supported", Detail: "gpt-test",
		}},
	}
	selected := gateway.Candidate{
		Channel: storage.Channel{ID: 2, Name: "fallback"},
		Rate:    storage.RateSnapshot{ModelName: "stable"},
	}
	decision.Candidates = []gateway.Candidate{selected}
	if got := gatewayRouteReason(decision, nil); got != gatewayRouteReasonModelFiltered {
		t.Fatalf("route reason = %q, want model filtered", got)
	}
	details := gatewayRouteDetails(decision, nil, selected)
	if !strings.Contains(details, "selected fallback/stable") {
		t.Fatalf("route details %q missing selected candidate", details)
	}
	for _, want := range []string{"higher-ranked primary/cheap filtered", "model gpt-test", "rejected as unsupported"} {
		if !strings.Contains(details, want) {
			t.Fatalf("route details %q missing %q", details, want)
		}
	}
}

func TestGatewayRouteDetailsStoresOnlyStructuredFailureData(t *testing.T) {
	primary := gateway.Candidate{
		Channel: storage.Channel{ID: 1, Name: "primary"},
		Rate:    storage.RateSnapshot{ModelName: "cheap"},
	}
	selected := gateway.Candidate{
		Channel: storage.Channel{ID: 2, Name: "fallback"},
		Rate:    storage.RateSnapshot{ModelName: "stable"},
	}
	decision := &gatewayRouteDecision{SmartRouting: true, Candidates: []gateway.Candidate{primary, selected}}
	details := gatewayRouteDetails(decision, []gatewayRouteAttempt{{
		ChannelName: "primary",
		GroupName:   "cheap",
		Reason:      "model_unsupported",
		StatusCode:  http.StatusNotFound,
	}}, selected)
	for _, want := range []string{"selected fallback/stable", "primary/cheap", "requested model is not supported", "status 404"} {
		if !strings.Contains(details, want) {
			t.Fatalf("route details %q missing %q", details, want)
		}
	}
	for _, forbidden := range []string{"session-a", "Authorization", "response body"} {
		if strings.Contains(details, forbidden) {
			t.Fatalf("route details %q contains free-form value %q", details, forbidden)
		}
	}
}

func TestGatewayRouteDetailsShowsOnlyAttemptedCandidatesBeforeSelection(t *testing.T) {
	decision := &gatewayRouteDecision{SmartRouting: true}
	first := gateway.Candidate{Channel: storage.Channel{ID: 1, Name: "cheapest"}, Rate: storage.RateSnapshot{ModelName: "promo"}}
	selected := gateway.Candidate{Channel: storage.Channel{ID: 2, Name: "fallback"}, Rate: storage.RateSnapshot{ModelName: "stable"}}
	afterSelected := gateway.Candidate{Channel: storage.Channel{ID: 3, Name: "ranked-later"}, Rate: storage.RateSnapshot{ModelName: "backup"}}
	decision.Candidates = []gateway.Candidate{first, selected, afterSelected}
	details := gatewayRouteDetails(decision, []gatewayRouteAttempt{
		newGatewayRouteAttempt(first, "upstream_status", http.StatusServiceUnavailable),
	}, selected)
	for _, want := range []string{"selected fallback/stable", "attempt failed cheapest/promo", "status 503"} {
		if !strings.Contains(details, want) {
			t.Fatalf("route details %q missing %q", details, want)
		}
	}
	for _, forbidden := range []string{"disabled-channel", "model-filtered", "ranked-later"} {
		if strings.Contains(details, forbidden) {
			t.Fatalf("route details %q contains unattempted or post-selection candidate %q", details, forbidden)
		}
	}
	if strings.Index(details, "attempt failed cheapest/promo") > strings.Index(details, "selected fallback/stable") {
		t.Fatalf("route details should put attempt failures before the selected candidate: %q", details)
	}
	if got := gatewayRouteReason(decision, nil); got != gatewayRouteReasonSmartPrimary {
		t.Fatalf("prefiltered candidates changed route reason to %q", got)
	}
}

func TestGatewayRouteReasonRequiresPhysicalCrossCandidateFailure(t *testing.T) {
	primary := gateway.Candidate{
		Channel: storage.Channel{ID: 1, Name: "primary"},
		Rate:    storage.RateSnapshot{ModelName: "cheap"},
	}
	selected := gateway.Candidate{
		Channel: storage.Channel{ID: 2, Name: "backup"},
		Rate:    storage.RateSnapshot{ModelName: "stable"},
	}
	decision := &gatewayRouteDecision{
		SmartRouting:     true,
		Candidates:       []gateway.Candidate{primary, selected},
		RankedCandidates: []gateway.Candidate{primary, selected},
	}
	preparationFailure := newGatewayRouteAttempt(primary, "upstream_key", 0, "key is still provisioning")
	if got := gatewayRouteReason(decision, []gatewayRouteAttempt{preparationFailure}, selected); got != gatewayRouteReasonSmartPrimary {
		t.Fatalf("preparation failure route reason = %q, want %q", got, gatewayRouteReasonSmartPrimary)
	}
	physicalFailure := newGatewayRouteAttemptFromDispatch(
		&gatewayDispatchAuditState{id: 1, physicallyDispatched: true},
		primary,
		"upstream_status",
		http.StatusServiceUnavailable,
		"status 503",
	)
	if got := gatewayRouteReason(decision, []gatewayRouteAttempt{physicalFailure}, selected); got != gatewayRouteReasonFallback {
		t.Fatalf("cross-candidate physical failure route reason = %q, want %q", got, gatewayRouteReasonFallback)
	}
	if got := gatewayRouteReason(decision, []gatewayRouteAttempt{physicalFailure}, primary); got != gatewayRouteReasonSmartPrimary {
		t.Fatalf("same-candidate physical retry route reason = %q, want %q", got, gatewayRouteReasonSmartPrimary)
	}
	if got := gatewayRouteReason(&gatewayRouteDecision{}, []gatewayRouteAttempt{physicalFailure}, selected); got != gatewayRouteReasonManual {
		t.Fatalf("fixed route reason = %q, want %q", got, gatewayRouteReasonManual)
	}
}

func TestGatewayRouteAttemptedChannelsExcludesPreparationFailures(t *testing.T) {
	primary := gateway.Candidate{
		Channel: storage.Channel{ID: 1, Name: "primary"},
		Rate:    storage.RateSnapshot{ModelName: "cheap"},
	}
	selected := gateway.Candidate{
		Channel: storage.Channel{ID: 2, Name: "backup"},
		Rate:    storage.RateSnapshot{ModelName: "stable"},
	}
	decision := &gatewayRouteDecision{
		SmartRouting:     true,
		Candidates:       []gateway.Candidate{primary, selected},
		RankedCandidates: []gateway.Candidate{primary, selected},
	}
	attempts := []gatewayRouteAttempt{
		newGatewayRouteAttempt(primary, "upstream_key", 0),
		newGatewayRouteAttemptFromDispatch(&gatewayDispatchAuditState{id: 1, physicallyDispatched: true, firstByteBudgetMs: 30_000}, primary, "upstream_status", http.StatusServiceUnavailable),
	}
	traces, ok := gatewayRouteAttemptTraces(gatewayRouteAttemptedChannels(decision, attempts, selected))
	if !ok || len(traces) != 1 || traces[0].ChannelID != primary.Channel.ID || traces[0].Reason != "upstream_status" || !traces[0].Physical || traces[0].FirstByteBudgetMs != 30_000 {
		t.Fatalf("route attempt traces = %#v, ok=%t; preparation failures must be excluded", traces, ok)
	}
}

func TestGatewayRouteDetailsExplainsEligibleRankAndSessionAffinity(t *testing.T) {
	first := gateway.Candidate{Channel: storage.Channel{ID: 1, Name: "first"}, Rate: storage.RateSnapshot{ModelName: "cheap"}}
	selected := gateway.Candidate{Channel: storage.Channel{ID: 2, Name: "tenth"}, Rate: storage.RateSnapshot{ModelName: "reliable"}}
	decision := &gatewayRouteDecision{SmartRouting: true, Candidates: []gateway.Candidate{first, selected}, RankedCandidates: []gateway.Candidate{first, selected}}
	details := gatewayRouteDetails(decision, []gatewayRouteAttempt{newGatewayRouteAttempt(first, "upstream_status", http.StatusServiceUnavailable)}, selected)
	if !strings.Contains(details, "eligible priority #2") {
		t.Fatalf("route details missing eligible rank: %q", details)
	}

	decision.AffinityApplied = true
	decision.Candidates = []gateway.Candidate{selected, first}
	decision.AffinityOrigin = &storage.GatewayRecentRoute{
		ID: 77, RouteReason: gatewayRouteReasonFallback, SelectionRank: 2,
		RouteAttemptedChannels: `[{"channel_id":1,"channel_name":"first","group_name":"cheap","reason":"upstream_status","status_code":503,"eligible_rank":1,"physically_dispatched":true}]`,
	}
	details = gatewayRouteDetails(decision, nil, selected)
	for _, want := range []string{"session affinity", "source request #77", "normal eligible priority #2"} {
		if !strings.Contains(details, want) {
			t.Fatalf("route details missing affinity provenance %q: %q", want, details)
		}
	}
	if strings.Contains(details, "normal eligible priority #1") {
		t.Fatalf("affinity-reordered candidates lost their original normal rank: %q", details)
	}
	if !strings.Contains(details, "source request exhausted #1 first/cheap") {
		t.Fatalf("route details missing source fallback reason: %q", details)
	}
	if strings.Contains(details, "eligible priority #1") {
		t.Fatalf("affinity-reordered candidates must not claim their new position is a normal rank: %q", details)
	}
}

func TestGatewaySessionAffinityAuditFreezesExpiryAndOverriddenCandidates(t *testing.T) {
	originAt := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	higher := gateway.Candidate{Channel: storage.Channel{ID: 14, Name: "高优先渠道"}, Rate: storage.RateSnapshot{ModelName: "优先分组"}}
	affinity := gateway.Candidate{Channel: storage.Channel{ID: 21, Name: "亲和渠道"}, Rate: storage.RateSnapshot{ModelName: "稳定分组"}}
	decision := &gatewayRouteDecision{
		SmartRouting:     true,
		RankedCandidates: []gateway.Candidate{higher, affinity},
		Candidates:       []gateway.Candidate{affinity, higher},
	}
	gatewaySetSessionAffinityDecision(&Deps{Gateway: gateway.NewService(nil, nil, nil, nil, nil, nil)}, decision, &storage.GatewayRecentRoute{ID: 77, CreatedAt: originAt})
	if decision.AffinityExpiresAt == nil || !decision.AffinityExpiresAt.Equal(originAt.Add(10*time.Minute)) {
		t.Fatalf("affinity expiry = %v, want %v", decision.AffinityExpiresAt, originAt.Add(10*time.Minute))
	}
	traces := gatewaySessionAffinityCandidateOutputs(gatewaySessionAffinityOverriddenCandidates(decision))
	if len(traces) != 1 || traces[0].ChannelID != 14 || traces[0].ChannelName != "高优先渠道" || traces[0].GroupName != "优先分组" || traces[0].EligibleRank != 1 {
		t.Fatalf("overridden candidates = %#v", traces)
	}
}

func TestGatewayRouteDetailsWithErrorsPreservesSanitizedFailure(t *testing.T) {
	selected := gateway.Candidate{
		Channel: storage.Channel{ID: 2, Name: "fallback"},
		Rate:    storage.RateSnapshot{ModelName: "stable"},
	}
	details := gatewayRouteDetailsWithErrors(
		&gatewayRouteDecision{SmartRouting: true},
		[]gatewayRouteAttempt{{ChannelName: "primary", GroupName: "cheap", Reason: "stream_error"}},
		selected,
		[]string{"primary/cheap: Authorization: Bearer sk-secret-token upstream internal error"},
	)
	if !strings.Contains(details, "upstream internal error") || !strings.Contains(details, "selected fallback/stable") {
		t.Fatalf("route details did not preserve the fallback context: %q", details)
	}
	for _, secret := range []string{"sk-secret-token", "Authorization", "Bearer"} {
		if strings.Contains(details, secret) {
			t.Fatalf("route details leaked %q: %q", secret, details)
		}
	}
}

func TestGatewayRouteDetailsPreservesSelectionWithLongUTF8Errors(t *testing.T) {
	attempts := make([]gatewayRouteAttempt, 0, 12)
	errorsSeen := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("attempt-%02d", i)
		group := fmt.Sprintf("group-%02d", i)
		attempts = append(attempts, gatewayRouteAttempt{ChannelID: uint(i + 1), ChannelName: name, GroupName: group, Reason: "stream_error"})
		errorsSeen = append(errorsSeen, fmt.Sprintf("%s/%s: %s token=short-secret", name, group, strings.Repeat("错误", 900)))
	}
	selected := gateway.Candidate{
		Channel: storage.Channel{ID: 99, Name: "final-channel"},
		Rate:    storage.RateSnapshot{ModelName: "final-group"},
	}
	details := gatewayRouteDetailsWithErrors(&gatewayRouteDecision{SmartRouting: true}, attempts, selected, errorsSeen)
	if !utf8.ValidString(details) {
		t.Fatalf("route details are not valid UTF-8: %q", details)
	}
	if len([]rune(details)) > gatewayRouteDetailsLimit {
		t.Fatalf("route details length = %d, want <= %d", len([]rune(details)), gatewayRouteDetailsLimit)
	}
	for _, want := range []string{"attempt-00/group-00", "selected final-channel/final-group"} {
		if !strings.Contains(details, want) {
			t.Fatalf("route details missing %q: %q", want, details)
		}
	}
	if strings.Contains(details, "short-secret") {
		t.Fatalf("route details leaked a short credential: %q", details)
	}
}

func TestSanitizeGatewayProbeErrorRedactsShortCredentials(t *testing.T) {
	raw := `request failed Authorization: Bearer abc123 x-api-key=short-key token=mini-secret https://user:pass@example.test/path?api_key=query-secret&ok=1 cookie=session-short`
	got := sanitizeGatewayProbeError([]byte(raw), "abc123")
	for _, secret := range []string{"abc123", "short-key", "mini-secret", "user:pass", "query-secret", "session-short"} {
		if strings.Contains(got, secret) {
			t.Fatalf("sanitized error leaked %q: %q", secret, got)
		}
	}
	if !strings.Contains(got, "request failed") || !strings.Contains(got, "[redacted]") {
		t.Fatalf("sanitized error lost useful context: %q", got)
	}
}

func TestGatewayProxyLogsWhenAllCandidatesAreModelFiltered(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	svc := gateway.NewService(channels, rates, store, nil, nil, nil)

	channel := storage.Channel{Name: "primary", Type: storage.ChannelTypeNewAPI, SiteURL: "https://primary.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "cheap", Ratio: 0.1, CompletionRatio: 0.1}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "cheap", APIKeyCipher: "cipher", ModelFamilies: "codex", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "cheap")
	if err != nil {
		t.Fatalf("capture upstream group generation: %v", err)
	}
	modelFence := storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision)
	if err := store.UpdateUpstreamKeyModelFamiliesForIdentity(modelFence, "codex", "gpt-test", time.Now(), ""); err != nil {
		t.Fatalf("persist model facts: %v", err)
	}
	if err := store.MarkModelCapabilityForIdentity(modelFence, "gpt-test", false, "unsupported"); err != nil {
		t.Fatalf("mark model capability: %v", err)
	}
	const downstreamToken = "sk-upops-test-downstream"
	downstream := storage.GatewayDownstreamKey{
		Name: "test", App: storage.GatewayAppCodex, KeyHash: gateway.HashKey(downstreamToken), APIKeyCipher: "cipher", SmartRouting: true, Enabled: true,
	}
	if err := store.CreateDownstreamKey(&downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	ctx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
	ctx.Request.Header.Set(gatewaySessionHeader, "capacity-session")
	ctx.Request.Header.Set(gatewaySessionHeader, "session-a")
	gatewayProxy(ctx, &Deps{Gateway: svc})

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var log storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&log).Error; err != nil {
		t.Fatalf("find request log: %v", err)
	}
	if log.RouteReason != gatewayRouteReasonAllFailed || log.SessionHash == "" || log.SessionHash == "session-a" {
		t.Fatalf("unexpected route log: %#v", log)
	}
	for _, want := range []string{"higher-ranked primary/cheap filtered", "model gpt-test was rejected as unsupported", "no eligible upstream candidate"} {
		if !strings.Contains(log.RouteDetails, want) {
			t.Fatalf("route details %q missing %q", log.RouteDetails, want)
		}
	}
	if strings.Contains(log.RouteDetails, "session-a") || strings.Contains(log.Error, "session-a") {
		t.Fatalf("request log leaked raw session id: %#v", log)
	}
}

func TestGatewayProxyLogsManualCandidateResolutionWithoutFallbackTrace(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	store := storage.NewGateway(db)
	const downstreamToken = "sk-manual-resolution-test"
	downstream := storage.GatewayDownstreamKey{
		Name: "manual-missing", App: storage.GatewayAppCodex,
		KeyHash: gateway.HashKey(downstreamToken), APIKeyCipher: "cipher", Enabled: true, SmartRouting: true,
		ManualChannelID: 999, ManualGroupName: "missing",
	}
	if err := store.CreateDownstreamKey(&downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	downstream.SmartRouting = false
	if err := store.UpdateDownstreamKey(&downstream); err != nil {
		t.Fatalf("switch downstream key to manual routing: %v", err)
	}
	svc := gateway.NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, nil, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	ctx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
	gatewayProxy(ctx, &Deps{Gateway: svc})

	if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), gatewayPublicFailureMessage) {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var requestLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&requestLog).Error; err != nil {
		t.Fatalf("find request log: %v", err)
	}
	if requestLog.RouteReason != gatewayRouteReasonManual || requestLog.RouteAttemptedChannels != "" || strings.Contains(requestLog.RouteDetails, "candidate selection") || strings.Contains(requestLog.RouteDetails, "attempt failed") {
		t.Fatalf("manual resolution failure was recorded as candidate fallback: %+v", requestLog)
	}
	if requestLog.Error == "" || !strings.Contains(requestLog.RouteDetails, requestLog.Error) {
		t.Fatalf("manual resolution failure lost sanitized error details: %+v", requestLog)
	}
}

func TestGatewayGenericConflictFallsBackAndEntersAdaptiveCooldown(t *testing.T) {
	if !shouldFallbackGatewayStatus(http.StatusConflict) || !isTransientGatewayStatus(http.StatusConflict) {
		t.Fatal("generic 409 must use the fallback and cooldown classification")
	}
}

func TestGatewayContextLimitErrorClassification(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{
			name:   "anthropic safe input budget",
			status: http.StatusBadRequest,
			body:   `{"type":"error","error":{"type":"invalid_request_error","message":"Prompt cannot fit safely in this model's 1000000 token context: estimated input exceeds the safe input budget"}}`,
			want:   true,
		},
		{name: "openai context length", status: http.StatusBadRequest, body: `{"error":{"code":"context_length_exceeded","message":"maximum context length exceeded by 100 tokens"}}`, want: true},
		{name: "payload too large context", status: http.StatusRequestEntityTooLarge, body: `context window token limit exceeded`, want: true},
		{name: "anthropic prompt too long", status: http.StatusBadRequest, body: `prompt is too long: 210000 tokens > 200000 maximum`, want: true},
		{name: "ordinary validation", status: http.StatusBadRequest, body: `{"error":{"message":"messages is required"}}`},
		{name: "server error text", status: http.StatusInternalServerError, body: `context length exceeded`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isGatewayContextLimitError(test.status, []byte(test.body)); got != test.want {
				t.Fatalf("classification = %v, want %v", got, test.want)
			}
		})
	}
}

func TestParseGatewayContextCapacity(t *testing.T) {
	body := []byte(`Prompt cannot fit safely in this model's 1,000,000 token context: estimated input 968,522 tokens exceeds the safe input budget of 877,808 after reserving 64,000 output tokens and 58,192 safety tokens.`)
	got := parseGatewayContextCapacity(body, 1024)
	if got.ContextWindowTokens != 1000000 || got.EstimatedInputTokens != 968522 || got.SafeInputTokens != 877808 || got.RequestedOutputTokens != 64000 || got.SafetyTokens != 58192 {
		t.Fatalf("capacity = %+v", got)
	}
	short := parseGatewayContextCapacity([]byte(`prompt is too long: 210000 tokens > 200000 maximum`), 4096)
	if short.EstimatedInputTokens != 210000 || short.SafeInputTokens != 200000 {
		t.Fatalf("short capacity = %+v", short)
	}
}

func TestGatewayCountTokensBodyRemovesGenerationFields(t *testing.T) {
	raw, err := gatewayCountTokensBody([]byte(`{"model":"claude-sonnet","messages":[{"role":"user","content":"hello"}],"system":"rules","tools":[{"name":"read"}],"max_tokens":64000,"stream":true,"temperature":0.2,"metadata":{"user_id":"u"}}`))
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"max_tokens", "stream", "temperature", "metadata"} {
		if _, ok := payload[removed]; ok {
			t.Fatalf("count body retained %s: %s", removed, raw)
		}
	}
	for _, retained := range []string{"model", "messages", "system", "tools"} {
		if _, ok := payload[retained]; !ok {
			t.Fatalf("count body removed %s: %s", retained, raw)
		}
	}
}

func TestGatewayProxyRetriesContextLimitWithoutCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	contextError := []byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Prompt cannot fit safely in this model's 1000000 token context: estimated input 968522 tokens exceeds the safe input budget of 877808 after reserving 64000 output tokens and 58192 safety tokens"}}`)
	primaryMessageCalls := 0
	primaryCountCalls := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/messages/count_tokens" {
			primaryCountCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"input_tokens":950000}`))
			return
		}
		primaryMessageCalls++
		requestBody, _ := io.ReadAll(r.Body)
		if bytes.Contains(requestBody, []byte(`"stream_context":true`)) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: error\ndata: " + string(contextError) + "\n\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(contextError)
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestBody, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		if bytes.Contains(requestBody, []byte(`"all_context":true`)) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write(contextError)
			return
		}
		_, _ = w.Write([]byte(`{"id":"msg_test","type":"message","role":"assistant","content":[{"type":"text","text":"fallback succeeded"}],"model":"claude-sonnet","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer fallback.Close()

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("context-limit-fallback-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	primaryChannel := storage.Channel{Name: "context-primary", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppClaude, SiteURL: primary.URL, MonitorEnabled: true}
	fallbackChannel := storage.Channel{Name: "context-fallback", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppClaude, SiteURL: fallback.URL, MonitorEnabled: true}
	for _, channel := range []*storage.Channel{&primaryChannel, &fallbackChannel} {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}
	for _, rate := range []storage.RateSnapshot{
		{ChannelID: primaryChannel.ID, ModelName: "primary", Ratio: 0.1, CompletionRatio: 0.1},
		{ChannelID: fallbackChannel.ID, ModelName: "fallback", Ratio: 1, CompletionRatio: 1},
	} {
		if _, err := rates.Upsert(&rate); err != nil {
			t.Fatalf("upsert rate: %v", err)
		}
	}
	for _, upstream := range []struct {
		channelID uint
		groupName string
		key       string
		keyID     int64
	}{
		{channelID: primaryChannel.ID, groupName: "primary", key: "sk-context-primary", keyID: 1},
		{channelID: fallbackChannel.ID, groupName: "fallback", key: "sk-context-fallback", keyID: 2},
	} {
		ciphertext, err := cipher.Encrypt(upstream.key)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: upstream.channelID, GroupName: upstream.groupName, GroupRatio: 1,
			APIKeyCipher: ciphertext, APIKeyPrefix: "sk-context", UpstreamKeyID: upstream.keyID,
			UpstreamKeyName: "BOG context test", ModelFamilies: "claude", ModelIDs: "claude-sonnet", Enabled: true,
		}); err != nil {
			t.Fatalf("upsert upstream key: %v", err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(upstream.channelID, upstream.groupName)
		if err != nil {
			t.Fatalf("capture upstream group generation: %v", err)
		}
		channel, err := channels.FindByID(upstream.channelID)
		if err != nil {
			t.Fatalf("reload upstream channel: %v", err)
		}
		if err := store.UpdateUpstreamKeyModelFamiliesForIdentity(
			storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision),
			"claude", "claude-sonnet", time.Now(), "",
		); err != nil {
			t.Fatalf("persist upstream model facts: %v", err)
		}
	}
	const downstreamToken = "sk-downstream-context-fallback"
	downstream := storage.GatewayDownstreamKey{
		Name: "context-fallback", App: storage.GatewayAppClaude, ModelFamily: "claude",
		KeyHash: gateway.HashKey(downstreamToken), APIKeyCipher: "cipher", KeyPrefix: "sk-test", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(&downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	keyManager := gatewayProxyTestKeyManager{groups: map[uint][]connector.APIKeyGroup{
		primaryChannel.ID:  {{Name: "primary", Ratio: 0.1}},
		fallbackChannel.ID: {{Name: "fallback", Ratio: 1}},
	}}
	svc := gateway.NewService(channels, rates, store, cipher, keyManager, nil)
	call := func(body, session string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
		ctx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
		ctx.Request.Header.Set(gatewaySessionHeader, session)
		gatewayProxy(ctx, &Deps{Gateway: svc})
		return recorder
	}

	success := call(`{"model":"claude-sonnet","max_tokens":1024,"messages":[{"role":"user","content":"hello"}]}`, "context-success")
	if success.Code != http.StatusOK || !strings.Contains(success.Body.String(), "fallback succeeded") {
		t.Fatalf("fallback response = status %d body %q", success.Code, success.Body.String())
	}
	failedKey, err := store.FindUpstreamKey(primaryChannel.ID, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if failedKey.LastRoutingFailureAt != nil || failedKey.RoutingCooldownUntil != nil {
		t.Fatalf("context limit changed global routing state: %+v", failedKey)
	}
	capability, err := store.FindModelCapability(primaryChannel.ID, "primary", "claude-sonnet")
	if err != nil || capability == nil {
		t.Fatalf("learned capability = %+v err=%v", capability, err)
	}
	if capability.ContextWindowTokens != 1000000 || capability.SafeInputTokens != 877808 || capability.ContextSafetyTokens != 58192 || capability.ContextLimitObserved == nil {
		t.Fatalf("learned context capacity = %+v", capability)
	}
	var successLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&successLog).Error; err != nil {
		t.Fatal(err)
	}
	var attempts []gatewayRouteAttemptTrace
	if err := json.Unmarshal([]byte(successLog.RouteAttemptedChannels), &attempts); err != nil || len(attempts) != 1 || attempts[0].Reason != "context_limit" || !attempts[0].Physical || attempts[0].FirstByteBudgetMs != 0 {
		t.Fatalf("context fallback attempts = %q err=%v", successLog.RouteAttemptedChannels, err)
	}

	streamSuccess := call(`{"model":"claude-sonnet","max_tokens":1024,"stream":true,"stream_context":true,"messages":[{"role":"user","content":"hello"}]}`, "context-stream-success")
	if streamSuccess.Code != http.StatusOK || !strings.Contains(streamSuccess.Body.String(), "fallback succeeded") {
		t.Fatalf("stream context fallback response = status %d body %q", streamSuccess.Code, streamSuccess.Body.String())
	}
	var streamSuccessLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&streamSuccessLog).Error; err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(streamSuccessLog.RouteAttemptedChannels), &attempts); err != nil || len(attempts) != 1 || attempts[0].Reason != "context_limit" || !attempts[0].Physical || attempts[0].FirstByteBudgetMs != 0 {
		t.Fatalf("stream context fallback attempts = %q err=%v", streamSuccessLog.RouteAttemptedChannels, err)
	}

	messagesBeforePreflight := primaryMessageCalls
	largeContent := strings.Repeat("a", 1_200_000)
	preflightSuccess := call(fmt.Sprintf(`{"model":"claude-sonnet","max_tokens":64000,"messages":[{"role":"user","content":%q}]}`, largeContent), "context-preflight")
	if preflightSuccess.Code != http.StatusOK || !strings.Contains(preflightSuccess.Body.String(), "fallback succeeded") {
		t.Fatalf("preflight fallback response = status %d body %q", preflightSuccess.Code, preflightSuccess.Body.String())
	}
	if primaryCountCalls != 1 || primaryMessageCalls != messagesBeforePreflight {
		t.Fatalf("preflight calls = count %d messages %d, messages before %d", primaryCountCalls, primaryMessageCalls, messagesBeforePreflight)
	}
	var preflightLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&preflightLog).Error; err != nil {
		t.Fatal(err)
	}
	if preflightLog.RouteAttemptedChannels != "" || preflightLog.RouteReason != gatewayRouteReasonSmartPrimary || preflightLog.RealFallback || preflightLog.PriorPhysicallyDispatchedFailureCount != 0 {
		t.Fatalf("preflight route telemetry = reason %q attempts %q real=%t prior=%d", preflightLog.RouteReason, preflightLog.RouteAttemptedChannels, preflightLog.RealFallback, preflightLog.PriorPhysicallyDispatchedFailureCount)
	}

	allContext := call(`{"model":"claude-sonnet","max_tokens":64000,"all_context":true,"messages":[{"role":"user","content":"huge"}]}`, "context-all-failed")
	if allContext.Code != http.StatusBadRequest || !strings.Contains(allContext.Body.String(), gatewayPublicContextLimitMessage) {
		t.Fatalf("all-context response = status %d body %q", allContext.Code, allContext.Body.String())
	}
	for _, forbidden := range []string{"968522", "877808", "context-primary", "context-fallback"} {
		if strings.Contains(allContext.Body.String(), forbidden) {
			t.Fatalf("all-context response leaked %q: %q", forbidden, allContext.Body.String())
		}
	}
	var allContextLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&allContextLog).Error; err != nil {
		t.Fatal(err)
	}
	if allContextLog.StatusCode != http.StatusBadRequest || allContextLog.RouteReason != gatewayRouteReasonAllFailed {
		t.Fatalf("all-context log = %+v", allContextLog)
	}
	if allContextLog.RouteAttemptedChannels == "" || !allContextLog.RealFallback || allContextLog.PriorPhysicallyDispatchedFailureCount != 1 {
		t.Fatalf("all-context route telemetry = attempts %q real=%t prior=%d", allContextLog.RouteAttemptedChannels, allContextLog.RealFallback, allContextLog.PriorPhysicallyDispatchedFailureCount)
	}
	if err := json.Unmarshal([]byte(allContextLog.RouteAttemptedChannels), &attempts); err != nil || len(attempts) != 2 {
		t.Fatalf("all-context route attempts = %q err=%v", allContextLog.RouteAttemptedChannels, err)
	}
	for _, attempt := range attempts {
		if attempt.Reason != "context_limit" || !attempt.Physical || attempt.EligibleRank == 0 {
			t.Fatalf("all-context route attempt = %+v", attempt)
		}
	}
	for _, target := range []struct {
		channelID uint
		groupName string
	}{{primaryChannel.ID, "primary"}, {fallbackChannel.ID, "fallback"}} {
		key, err := store.FindUpstreamKey(target.channelID, target.groupName)
		if err != nil {
			t.Fatal(err)
		}
		if key.LastRoutingFailureAt != nil || key.RoutingCooldownUntil != nil {
			t.Fatalf("context limit cooled channel %d/%s: %+v", target.channelID, target.groupName, key)
		}
	}
}

func TestTransientGatewayStatusIncludesTransparentFallbackFailures(t *testing.T) {
	for _, status := range []int{
		http.StatusRequestTimeout,
		http.StatusConflict,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusServiceUnavailable,
	} {
		if !shouldFallbackGatewayStatus(status) || !isTransientGatewayStatus(status) {
			t.Fatalf("status %d must use transparent fallback and adaptive cooldown", status)
		}
	}
	if isTransientGatewayStatus(http.StatusPaymentRequired) {
		t.Fatal("402 must remain a request-scoped fallback")
	}
	if shouldFallbackGatewayStatus(600) || isTransientGatewayStatus(600) {
		t.Fatal("nonstandard status 600 must not be treated as a server error")
	}
}

func TestGatewayProxySilentlyRetriesStreamCapacityNoticeOnCurrentCandidate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var capacityCalls, fallbackCalls int
	capacityUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capacityCalls++
		requestBody, _ := io.ReadAll(r.Body)
		if strings.Contains(string(requestBody), `"test_mode":"non_stream_notice"`) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"output":[{"type":"message","content":[{"type":"output_text","text":"⚠ Selected model is at capacity. Please try a different model."}]}]}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: heartbeat\ndata: {\"type\":\"heartbeat\"}\n\n"))
		w.(http.Flusher).Flush()
		if strings.Contains(string(requestBody), `"test_mode":"unmarked_stream_notice"`) {
			_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Selected model is at capacity. Please try a different model.\"}\n\n"))
			return
		}
		if strings.Contains(string(requestBody), `"test_mode":"unmarked_overload_text"`) {
			_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_overloaded\"}}\n\n" +
				"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"item\":{\"id\":\"msg_overloaded\",\"type\":\"message\",\"content\":[]}}\n\n" +
				"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_overloaded\",\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
				"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_overloaded\",\"delta\":\"Our servers are currently overloaded. Please try again later.\"}\n\n" +
				"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"sequence_number\":4,\"item_id\":\"msg_overloaded\",\"text\":\"Our servers are currently overloaded. Please try again later.\"}\n\n" +
				"event: response.content_part.done\ndata: {\"type\":\"response.content_part.done\",\"sequence_number\":5,\"item_id\":\"msg_overloaded\",\"part\":{\"type\":\"output_text\",\"text\":\"Our servers are currently overloaded. Please try again later.\"}}\n\n" +
				"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"item\":{\"id\":\"msg_overloaded\",\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"Our servers are currently overloaded. Please try again later.\"}]}}\n\n" +
				"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_overloaded\",\"status\":\"completed\",\"error\":null}}\n\n"))
			return
		}
		if strings.Contains(string(requestBody), `"test_mode":"split_capacity_notice"`) {
			_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at \"}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"capacity. Please try a different model.\"}\n\n"))
			return
		}
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n"))
	}))
	defer capacityUpstream.Close()
	fallbackUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackCalls++
		requestBody, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		if strings.Contains(string(requestBody), `"test_mode":"all_capacity_notice"`) {
			_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n"))
			return
		}
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"fallback succeeded\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_capacity_fallback\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer fallbackUpstream.Close()

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("capacity-fallback-test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	capacityChannel := storage.Channel{Name: "capacity", Type: storage.ChannelTypeNewAPI, SiteURL: capacityUpstream.URL, MonitorEnabled: true}
	fallbackChannel := storage.Channel{Name: "fallback", Type: storage.ChannelTypeNewAPI, SiteURL: fallbackUpstream.URL, MonitorEnabled: true}
	for _, channel := range []*storage.Channel{&capacityChannel, &fallbackChannel} {
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: capacityChannel.ID, ModelName: "capacity", Ratio: 0.1}); err != nil {
		t.Fatalf("upsert capacity rate: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: fallbackChannel.ID, ModelName: "fallback", Ratio: 1}); err != nil {
		t.Fatalf("upsert fallback rate: %v", err)
	}
	for _, upstream := range []struct {
		channelID uint
		groupName string
		plainKey  string
		keyID     int64
	}{
		{channelID: capacityChannel.ID, groupName: "capacity", plainKey: "sk-capacity", keyID: 1},
		{channelID: fallbackChannel.ID, groupName: "fallback", plainKey: "sk-fallback", keyID: 2},
	} {
		ciphertext, err := cipher.Encrypt(upstream.plainKey)
		if err != nil {
			t.Fatalf("encrypt upstream key: %v", err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: upstream.channelID, GroupName: upstream.groupName, GroupRatio: 1,
			APIKeyCipher: ciphertext, APIKeyPrefix: "sk-test", UpstreamKeyID: upstream.keyID,
			UpstreamKeyName: "BOG test", ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: upstream.channelID != 1,
		}); err != nil {
			t.Fatalf("upsert upstream key: %v", err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(upstream.channelID, upstream.groupName)
		if err != nil {
			t.Fatalf("capture upstream group generation: %v", err)
		}
		channel, err := channels.FindByID(upstream.channelID)
		if err != nil {
			t.Fatalf("reload upstream channel: %v", err)
		}
		if err := store.UpdateUpstreamKeyModelFamiliesForIdentity(
			storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision),
			"codex", "gpt-test", time.Now(), "",
		); err != nil {
			t.Fatalf("persist upstream model facts: %v", err)
		}
	}
	const downstreamToken = "sk-downstream-capacity-fallback"
	downstream := storage.GatewayDownstreamKey{
		Name: "capacity-fallback", App: storage.GatewayAppCodex, ModelFamily: "codex",
		KeyHash: gateway.HashKey(downstreamToken), APIKeyCipher: "cipher", KeyPrefix: "sk-test", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(&downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	keyManager := gatewayProxyTestKeyManager{groups: map[uint][]connector.APIKeyGroup{
		capacityChannel.ID: {{Name: "capacity", Ratio: 0.1}},
		fallbackChannel.ID: {{Name: "fallback", Ratio: 1}},
	}}
	svc := gateway.NewService(channels, rates, store, cipher, keyManager, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	ctx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
	ctx.Request.Header.Set(gatewaySessionHeader, "capacity-fallback-session")
	gatewayProxy(ctx, &Deps{Gateway: svc})

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "event: response.completed") || !strings.Contains(recorder.Body.String(), "fallback succeeded") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) {
		t.Fatalf("gateway response = status %d body %q", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(strings.ToLower(recorder.Body.String()), "selected model is at capacity") {
		t.Fatalf("capacity notice escaped after retry exhaustion: %q", recorder.Body.String())
	}
	if capacityCalls != gatewayCandidateRetryAttempts || fallbackCalls != 1 {
		t.Fatalf("upstream calls capacity=%d fallback=%d, want %d/1", capacityCalls, fallbackCalls, gatewayCandidateRetryAttempts)
	}
	for _, header := range []string{"X-BridgeOps-Channel-ID", "X-BridgeOps-Channel", "X-BridgeOps-Group", "X-Upstream-Ops-Channel-ID", "X-Upstream-Ops-Channel", "X-Upstream-Ops-Group"} {
		if got := recorder.Header().Get(header); got != "" {
			t.Fatalf("internal route header %s was exposed as %q", header, got)
		}
	}
	failedKey, err := store.FindUpstreamKey(capacityChannel.ID, "capacity")
	if err != nil {
		t.Fatalf("find capacity upstream key: %v", err)
	}
	if failedKey.LastRoutingFailureAt == nil || failedKey.RoutingCooldownUntil != nil || failedKey.RoutingFailureCount != gatewayCandidateRetryAttempts || failedKey.LastRoutingFailureKind != gateway.RoutingFailureTransport {
		t.Fatalf("capacity retries did not isolate the failed route: %+v", failedKey)
	}
	var requestLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&requestLog).Error; err != nil {
		t.Fatalf("find request log: %v", err)
	}
	if requestLog.ChannelID != fallbackChannel.ID || requestLog.RouteReason != gatewayRouteReasonFallback || requestLog.RouteSelectionRank != 2 || requestLog.SessionHash == "" || requestLog.StatusCode != http.StatusOK || requestLog.TerminalFailureCode != "" || !strings.Contains(requestLog.RouteDetails, "upstream capacity, concurrency, or rate limit exceeded") {
		t.Fatalf("request log = %+v", requestLog)
	}
	var attempts []gatewayRouteAttemptTrace
	if err := json.Unmarshal([]byte(requestLog.RouteAttemptedChannels), &attempts); err != nil {
		t.Fatalf("decode attempted channels: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Reason != "stream_account_limit" || attempts[0].StatusCode != http.StatusOK {
		t.Fatalf("capacity notice attempt = %#v", attempts)
	}
	capacityGeneration, err := store.CaptureUpstreamGroupGeneration(capacityChannel.ID, "capacity")
	if err != nil {
		t.Fatalf("capture capacity generation: %v", err)
	}
	capacityHealthFence := storage.NewGatewayRoutingHealthFence(capacityGeneration, capacityChannel.UpstreamAccountIdentityRevision)
	// The remaining requests exercise independent response classifications.
	// Reset the shared fixture through the same confirmed probe recovery that can
	// release a production isolation.
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(capacityHealthFence, storage.GatewayAppCodex, time.Now().UTC()); err != nil {
		t.Fatalf("reset capacity routing health: %v", err)
	}
	nonStreamingRecorder := httptest.NewRecorder()
	nonStreamingCtx, _ := gin.CreateTestContext(nonStreamingRecorder)
	nonStreamingCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":false,"test_mode":"non_stream_notice"}`))
	nonStreamingCtx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
	nonStreamingCtx.Request.Header.Set(gatewaySessionHeader, "capacity-non-stream-session")
	gatewayProxy(nonStreamingCtx, &Deps{Gateway: svc})
	if nonStreamingRecorder.Code != http.StatusOK || !strings.Contains(nonStreamingRecorder.Body.String(), "Selected model is at capacity") || strings.Contains(nonStreamingRecorder.Body.String(), "fallback succeeded") {
		t.Fatalf("non-stream response was retried or changed: status %d body %q", nonStreamingRecorder.Code, nonStreamingRecorder.Body.String())
	}
	if capacityCalls != gatewayCandidateRetryAttempts+1 || fallbackCalls != 1 {
		t.Fatalf("non-stream capacity response used fallback: capacity=%d fallback=%d", capacityCalls, fallbackCalls)
	}
	var nonStreamingLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&nonStreamingLog).Error; err != nil {
		t.Fatalf("find non-stream request log: %v", err)
	}
	if nonStreamingLog.ChannelID != capacityChannel.ID || nonStreamingLog.RouteAttemptedChannels != "" {
		t.Fatalf("non-stream capacity response was marked as a fallback: %+v", nonStreamingLog)
	}

	unmarkedRecorder := httptest.NewRecorder()
	unmarkedCtx, _ := gin.CreateTestContext(unmarkedRecorder)
	unmarkedCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true,"test_mode":"unmarked_stream_notice"}`))
	unmarkedCtx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
	unmarkedCtx.Request.Header.Set(gatewaySessionHeader, "capacity-unmarked-stream-session")
	gatewayProxy(unmarkedCtx, &Deps{Gateway: svc})
	if unmarkedRecorder.Code != http.StatusOK || !strings.Contains(unmarkedRecorder.Body.String(), "Selected model is at capacity") || strings.Contains(unmarkedRecorder.Body.String(), "fallback succeeded") {
		t.Fatalf("unmarked stream response was retried or changed: status %d body %q", unmarkedRecorder.Code, unmarkedRecorder.Body.String())
	}
	if capacityCalls != gatewayCandidateRetryAttempts+2 || fallbackCalls != 1 {
		t.Fatalf("unmarked stream capacity response used fallback: capacity=%d fallback=%d", capacityCalls, fallbackCalls)
	}

	overloadRecorder := httptest.NewRecorder()
	overloadCtx, _ := gin.CreateTestContext(overloadRecorder)
	overloadCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true,"test_mode":"unmarked_overload_text"}`))
	overloadCtx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
	overloadCtx.Request.Header.Set(gatewaySessionHeader, "capacity-unmarked-overload-session")
	gatewayProxy(overloadCtx, &Deps{Gateway: svc})
	if overloadRecorder.Code != http.StatusOK || !strings.Contains(overloadRecorder.Body.String(), "event: response.failed") || strings.Contains(overloadRecorder.Body.String(), "fallback succeeded") || strings.Contains(overloadRecorder.Body.String(), "Our servers are currently overloaded. Please try again later.") {
		t.Fatalf("accepted overload response was replayed or exposed: status %d body %q", overloadRecorder.Code, overloadRecorder.Body.String())
	}
	if capacityCalls != gatewayCandidateRetryAttempts+3 || fallbackCalls != 1 {
		t.Fatalf("accepted overload response attempts = capacity=%d fallback=%d", capacityCalls, fallbackCalls)
	}
	var overloadLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&overloadLog).Error; err != nil {
		t.Fatalf("find accepted overload request log: %v", err)
	}
	if overloadLog.ChannelID != capacityChannel.ID || overloadLog.StatusCode != http.StatusBadGateway || !overloadLog.UpstreamAccepted || overloadLog.TerminalFailureCode == "" || overloadLog.PriorPhysicallyDispatchedFailureCount != 0 || overloadLog.RouteAttemptedChannels != "" {
		t.Fatalf("accepted overload audit = %+v", overloadLog)
	}
	failedKey, err = store.FindUpstreamKey(capacityChannel.ID, "capacity")
	if err != nil {
		t.Fatalf("find accepted capacity upstream key: %v", err)
	}
	if failedKey.LastRoutingFailureAt != nil || failedKey.RoutingCooldownUntil != nil || failedKey.RoutingFailureCount != 0 {
		t.Fatalf("accepted overload changed routing health: %+v", failedKey)
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(capacityHealthFence, storage.GatewayAppCodex, time.Now().UTC()); err != nil {
		t.Fatalf("reset capacity routing health after unmarked overload: %v", err)
	}

	splitRecorder := httptest.NewRecorder()
	splitCtx, _ := gin.CreateTestContext(splitRecorder)
	splitCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true,"test_mode":"split_capacity_notice"}`))
	splitCtx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
	splitCtx.Request.Header.Set(gatewaySessionHeader, "capacity-split-session")
	gatewayProxy(splitCtx, &Deps{Gateway: svc})
	if splitRecorder.Code != http.StatusOK || !strings.Contains(splitRecorder.Body.String(), "event: response.completed") || !strings.Contains(splitRecorder.Body.String(), "fallback succeeded") || strings.Contains(splitRecorder.Body.String(), gatewayPublicRetryPromptMessage) || strings.Contains(splitRecorder.Body.String(), "Selected model is at capacity") {
		t.Fatalf("split capacity notice response = status %d body %q", splitRecorder.Code, splitRecorder.Body.String())
	}
	if capacityCalls != 2*gatewayCandidateRetryAttempts+3 || fallbackCalls != 2 {
		t.Fatalf("split capacity notice did not fall back after confirmation: capacity=%d fallback=%d", capacityCalls, fallbackCalls)
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(capacityHealthFence, storage.GatewayAppCodex, time.Now().UTC()); err != nil {
		t.Fatalf("reset capacity routing health before all-failed request: %v", err)
	}

	allFailureRecorder := httptest.NewRecorder()
	allFailureCtx, _ := gin.CreateTestContext(allFailureRecorder)
	allFailureCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true,"test_mode":"all_capacity_notice"}`))
	allFailureCtx.Request.Header.Set("Authorization", "Bearer "+downstreamToken)
	allFailureCtx.Request.Header.Set(gatewaySessionHeader, "capacity-all-failed-session")
	gatewayProxy(allFailureCtx, &Deps{Gateway: svc})
	if allFailureRecorder.Code != http.StatusOK || !strings.Contains(allFailureRecorder.Body.String(), gatewayPublicRetryPromptMessage) || !strings.Contains(allFailureRecorder.Body.String(), "event: response.completed") || strings.Contains(allFailureRecorder.Body.String(), "event: response.failed") {
		t.Fatalf("all-capacity response = status %d body %q", allFailureRecorder.Code, allFailureRecorder.Body.String())
	}
	for _, forbidden := range []string{"Selected model is at capacity", "fallback succeeded", "capacity/"} {
		if strings.Contains(allFailureRecorder.Body.String(), forbidden) {
			t.Fatalf("all-capacity response leaked %q: %q", forbidden, allFailureRecorder.Body.String())
		}
	}
	if capacityCalls != 3*gatewayCandidateRetryAttempts+3 || fallbackCalls != 2+gatewayCandidateRetryAttempts {
		t.Fatalf("all-capacity response attempts = capacity=%d fallback=%d", capacityCalls, fallbackCalls)
	}
	var allFailureLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&allFailureLog).Error; err != nil {
		t.Fatalf("find all-capacity request log: %v", err)
	}
	if allFailureLog.RouteReason != gatewayRouteReasonAllFailed || allFailureLog.StatusCode != http.StatusBadGateway || allFailureLog.TerminalFailureCode != "retry_prompt_after_exhaustion" {
		t.Fatalf("all-capacity route log = %+v", allFailureLog)
	}
	if err := json.Unmarshal([]byte(allFailureLog.RouteAttemptedChannels), &attempts); err != nil || len(attempts) != 2 {
		t.Fatalf("all-capacity attempted channels = %q err=%v", allFailureLog.RouteAttemptedChannels, err)
	}
	for _, attempt := range attempts {
		if attempt.Reason != "stream_account_limit" || attempt.StatusCode != http.StatusOK {
			t.Fatalf("all-capacity attempt = %#v", attempt)
		}
	}
}

func TestGatewayForwardDoesNotCooldownOnUpstreamKeyPreparationRateLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var fallbackCalls int
	fallbackUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"fallback after key limit\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_key_limit_fallback\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer fallbackUpstream.Close()

	db := openTestDB(t)
	_, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{
		{channelID: 1, name: "limited", groupName: "limited", siteURL: "https://limited.example", ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallbackUpstream.URL, ratio: 1},
	})
	if err := store.DisableUpstreamKey(1, "limited", "", gatewayCandidateGenerationArgs(decision.Candidates[0])...); err != nil {
		t.Fatalf("disable limited upstream key: %v", err)
	}
	cipher, err := appcrypto.NewCipher("gateway-forward-candidates-test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	keyManager := gatewayProxyTestKeyManager{
		groups: map[uint][]connector.APIKeyGroup{
			1: {{Name: "limited", Ratio: 0.1}},
			2: {{Name: "fallback", Ratio: 1}},
		},
		groupErrors: map[uint]error{
			1: connector.HTTPStatusError(http.StatusTooManyRequests, []byte(`{"error":"account rate limit"}`)),
		},
	}
	svc := gateway.NewService(storage.NewChannels(db), storage.NewRates(db), store, cipher, keyManager, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback after key limit") || fallbackCalls != 1 {
		t.Fatalf("gateway response = status %d body %q calls=%d", recorder.Code, recorder.Body.String(), fallbackCalls)
	}
	limited, err := store.FindUpstreamKeyAny(1, "limited")
	if err != nil {
		t.Fatalf("find limited upstream key: %v", err)
	}
	if limited.LastRoutingFailureAt != nil || limited.RoutingCooldownUntil != nil || limited.LastError != "" {
		t.Fatalf("upstream key preparation rate limit changed shared upstream state: %+v", limited)
	}
	var requestLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&requestLog).Error; err != nil {
		t.Fatalf("find request log: %v", err)
	}
	if requestLog.RouteReason != gatewayRouteReasonSmartPrimary || requestLog.RouteAttemptedChannels != "" || requestLog.RealFallback || requestLog.PriorPhysicallyDispatchedFailureCount != 0 || !strings.Contains(requestLog.RouteDetails, "upstream key creation hit capacity") {
		t.Fatalf("request log = %+v", requestLog)
	}
}

func TestGatewayForwardRateLimitExhaustionCoolsAndFallsBack(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var firstCalls, secondCalls, fallbackCalls int
	capacityHandler := func(counter *int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			*counter = *counter + 1
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"rate limit reached"}}`))
		}
	}
	first := httptest.NewServer(capacityHandler(&firstCalls))
	defer first.Close()
	second := httptest.NewServer(capacityHandler(&secondCalls))
	defer second.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"third candidate succeeded\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_rate_limit_fallback\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "first", groupName: "first", siteURL: first.URL, ratio: 0.1},
		{channelID: 2, name: "second", groupName: "second", siteURL: second.URL, ratio: 0.2},
		{channelID: 3, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "third candidate succeeded") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || firstCalls != gatewayCandidateRetryAttempts || secondCalls != gatewayCandidateRetryAttempts || fallbackCalls != 1 {
		t.Fatalf("gateway response = status %d body %q calls=%d/%d/%d", recorder.Code, recorder.Body.String(), firstCalls, secondCalls, fallbackCalls)
	}
	key, err := store.FindUpstreamKey(1, "first")
	if err != nil || key.LastRoutingFailureAt == nil || key.RoutingCooldownUntil != nil || key.RoutingFailureCount != gatewayCandidateRetryAttempts || key.LastRoutingFailureKind != gateway.RoutingFailureHTTPStatus || key.LastRoutingFailureStatus != http.StatusTooManyRequests {
		t.Fatalf("rate limit confirmation was not isolated: key=%+v err=%v", key, err)
	}
	logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(logs) != 1 {
		t.Fatalf("list failed request log: total=%d logs=%d err=%v", total, len(logs), err)
	}
	if !strings.Contains(logs[0].RouteAttemptedChannels, `"eligible_rank":1`) || !strings.Contains(logs[0].RouteAttemptedChannels, `"eligible_rank":2`) || logs[0].RouteReason != gatewayRouteReasonFallback || logs[0].TerminalFailureCode != "" {
		t.Fatalf("fallback attempt trace = %+v", logs[0])
	}
}

func TestGatewayForwardServiceUnavailableCapacityTextCoolsAndFallsBackAfterThreeFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var failedCalls, fallbackCalls int
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		failedCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"message":"Service temporarily unavailable"}}`))
	}))
	defer failed.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"fallback succeeded\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "unavailable", groupName: "primary", siteURL: failed.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback succeeded") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) {
		t.Fatalf("response = status %d body %q", recorder.Code, recorder.Body.String())
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.RoutingFailureCount != gatewayCandidateRetryAttempts || key.LastRoutingFailureAt == nil || key.RoutingCooldownUntil != nil || key.LastRoutingFailureStatus != http.StatusServiceUnavailable {
		t.Fatalf("routing failure = %+v err=%v", key, err)
	}
	if failedCalls != gatewayCandidateRetryAttempts || fallbackCalls != 1 {
		t.Fatalf("upstream calls = %d/%d, want %d/1", failedCalls, fallbackCalls, gatewayCandidateRetryAttempts)
	}
}

func TestGatewayForwardRetriesPaymentRequiredWorkspaceWithoutCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var failedCalls, fallbackCalls int
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		failedCalls++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"detail":{"code":"deactivated_workspace"}}`))
	}))
	defer failed.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"fallback after workspace deactivation\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "deactivated", groupName: "workspace", siteURL: failed.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "available", siteURL: fallback.URL, ratio: 1},
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback after workspace deactivation") {
		t.Fatalf("gateway response = status %d body %q", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "deactivated_workspace") || failedCalls != 1 || fallbackCalls != 1 {
		t.Fatalf("workspace failure leaked or fallback calls differ: body=%q calls=%d/%d", recorder.Body.String(), failedCalls, fallbackCalls)
	}
	failedKey, err := store.FindUpstreamKey(1, "workspace")
	if err != nil {
		t.Fatalf("find failed upstream key: %v", err)
	}
	if failedKey.LastRoutingFailureAt != nil || failedKey.RoutingCooldownUntil != nil || failedKey.LastRoutingFailureStatus != 0 {
		t.Fatalf("payment-required response changed shared routing state: %+v", failedKey)
	}
	logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(logs) != 1 {
		t.Fatalf("list request logs: total=%d logs=%d err=%v", total, len(logs), err)
	}
	log := logs[0]
	if log.ChannelID != 2 || log.RouteReason != gatewayRouteReasonFallback || !strings.Contains(log.RouteDetails, "deactivated_workspace") || !strings.Contains(log.RouteDetails, "status 402") {
		t.Fatalf("fallback request log = %+v", log)
	}
	if !strings.Contains(log.RouteAttemptedChannels, `"reason":"upstream_account_unavailable"`) || !strings.Contains(log.RouteAttemptedChannels, `"status_code":402`) {
		t.Fatalf("fallback attempt trace = %q", log.RouteAttemptedChannels)
	}
}

func TestGatewayForwardPassesThroughMetadataOnlyStreamWithoutCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var fallbackCalls int
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer empty.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"fallback after empty stream\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "empty", groupName: "empty", siteURL: empty.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	failureAt := time.Now().Add(-time.Second)
	if err := store.RecordUpstreamKeyRoutingFailure(1, "empty", gateway.RoutingFailureTransport, 0, "existing failure", failureAt); err != nil {
		t.Fatalf("record existing routing failure: %v", err)
	}
	before, err := store.FindUpstreamKey(1, "empty")
	if err != nil {
		t.Fatalf("find existing routing failure: %v", err)
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "[DONE]") || strings.Contains(recorder.Body.String(), "fallback after empty stream") || fallbackCalls != 0 {
		t.Fatalf("gateway response = status %d body %q fallback calls=%d", recorder.Code, recorder.Body.String(), fallbackCalls)
	}
	emptyKey, err := store.FindUpstreamKey(1, "empty")
	if err != nil {
		t.Fatalf("find empty upstream key: %v", err)
	}
	if emptyKey.LastRoutingFailureAt == nil || before.LastRoutingFailureAt == nil || !emptyKey.LastRoutingFailureAt.Equal(*before.LastRoutingFailureAt) || emptyKey.RoutingFailureCount != before.RoutingFailureCount || emptyKey.RoutingCooldownUntil != nil || before.RoutingCooldownUntil != nil {
		t.Fatalf("metadata-only stream changed shared routing state: before=%+v after=%+v", before, emptyKey)
	}
	logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(logs) != 1 {
		t.Fatalf("list request logs: total=%d logs=%d err=%v", total, len(logs), err)
	}
	if logs[0].ChannelID != 1 || logs[0].RouteReason != gatewayRouteReasonSmartPrimary || logs[0].RouteAttemptedChannels != "" {
		t.Fatalf("metadata-only stream request log = %+v", logs[0])
	}
}

func TestGatewayForwardNonOKMetadataOnlyStreamExhaustionCoolsAndFallsBack(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var emptyCalls, fallbackCalls int
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		emptyCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer empty.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"fallback after non-OK empty stream\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "empty", groupName: "empty", siteURL: empty.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback after non-OK empty stream") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || emptyCalls != gatewayCandidateRetryAttempts || fallbackCalls != 1 {
		t.Fatalf("gateway response = status %d body %q calls=%d/%d", recorder.Code, recorder.Body.String(), emptyCalls, fallbackCalls)
	}
	emptyKey, err := store.FindUpstreamKey(1, "empty")
	if err != nil {
		t.Fatalf("find empty upstream key: %v", err)
	}
	if emptyKey.LastRoutingFailureKind != gateway.RoutingFailureTransport || emptyKey.RoutingFailureCount != gatewayCandidateRetryAttempts || emptyKey.RoutingCooldownUntil != nil {
		t.Fatalf("non-OK empty stream did not enter channel isolation: %+v", emptyKey)
	}
}

func TestGatewayForwardMalformedResponsesEOFExhaustionCoolsAndFallsBack(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var malformedCalls, fallbackCalls int
	malformed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		malformedCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"must-not-leak\""))
	}))
	defer malformed.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"fallback succeeded\"}\n\n" +
			"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_fallback\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "malformed", groupName: "malformed", siteURL: malformed.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback succeeded") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || strings.Contains(recorder.Body.String(), "must-not-leak") {
		t.Fatalf("gateway response = status %d body %q", recorder.Code, recorder.Body.String())
	}
	if malformedCalls != gatewayCandidateRetryAttempts || fallbackCalls != 1 {
		t.Fatalf("upstream calls = malformed %d fallback %d, want %d/1", malformedCalls, fallbackCalls, gatewayCandidateRetryAttempts)
	}
	logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(logs) != 1 {
		t.Fatalf("list request logs: total=%d logs=%d err=%v", total, len(logs), err)
	}
	if logs[0].ChannelID != 2 || logs[0].RouteReason != gatewayRouteReasonFallback || logs[0].StatusCode != http.StatusOK || logs[0].TerminalFailureCode != "" || !strings.Contains(logs[0].RouteDetails, "malformed JSON frame") {
		t.Fatalf("malformed exhaustion request log = %+v", logs[0])
	}
}

func TestGatewayForwardAcceptedSSEErrorDoesNotReplayOrCoolRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var failedCalls, fallbackCalls int
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		failedCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Oneapi-Request-Id", "req-accepted-failed")
		created := fmt.Sprintf(`data: {"type":"response.created","response":{"id":"resp_failed","instructions":%q}}`, strings.Repeat("x", 20<<10))
		_, _ = w.Write([]byte(strings.Join([]string{
			"event: response.created",
			created,
			"",
			"event: response.output_item.added",
			`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"msg_failed","type":"message","status":"in_progress","role":"assistant","content":[]}}`,
			"",
			"event: response.content_part.added",
			`data: {"type":"response.content_part.added","item_id":"msg_failed","output_index":0,"content_index":0,"part":{"type":"output_text","text":"","annotations":[]}}`,
			"",
			"event: response.failed",
			`data: {"type":"response.failed","response":{"error":{"type":"overloaded_error","message":"upstream overloaded before output"}}}`,
			"",
		}, "\n")))
	}))
	defer failed.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"delta\":\"fallback after stream error\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "failed", groupName: "failed", siteURL: failed.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "event: response.completed") || !strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || strings.Contains(recorder.Body.String(), "event: response.failed") || strings.Contains(recorder.Body.String(), "upstream overloaded") || strings.Contains(recorder.Body.String(), "fallback after stream error") || failedCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("gateway response = status %d body %q calls=%d/%d", recorder.Code, recorder.Body.String(), failedCalls, fallbackCalls)
	}
	logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(logs) != 1 {
		t.Fatalf("list request logs: total=%d logs=%d err=%v", total, len(logs), err)
	}
	if !strings.Contains(logs[0].Error, "upstream overloaded before output") || logs[0].StatusCode != http.StatusBadGateway || logs[0].TerminalFailureCode != "transient_after_acceptance" || logs[0].RouteReason != gatewayRouteReasonSmartPrimary || logs[0].ChannelID != 1 || !logs[0].UpstreamAccepted || logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-accepted-failed" {
		t.Fatalf("accepted stream failure log did not preserve route, billing, and hidden error facts: %+v", logs[0])
	}
	failedKey, err := store.FindUpstreamKey(1, "failed")
	if err != nil {
		t.Fatalf("find failed upstream key: %v", err)
	}
	if failedKey.LastRoutingFailureKind != "" || failedKey.RoutingFailureCount != 0 || failedKey.RoutingCooldownUntil != nil {
		t.Fatalf("accepted stream error changed global routing health: %+v", failedKey)
	}
}

func TestGatewayForwardAcceptedMalformedResponsesFramePromptsRetryWithoutReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var failedCalls, fallbackCalls int
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		failedCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Oneapi-Request-Id", "req-accepted-malformed")
		_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_accepted_malformed\"}}\n\n" +
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"delta\":\"truncated\"\n\n"))
	}))
	defer failed.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte("unexpected fallback"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "failed", groupName: "failed", siteURL: failed.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "event: response.completed") || !strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || strings.Contains(recorder.Body.String(), "truncated") || failedCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("accepted malformed response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), failedCalls, fallbackCalls)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
	if err != nil || len(logs) != 1 || logs[0].StatusCode != http.StatusBadGateway || logs[0].TerminalFailureCode != "transient_after_acceptance" || !logs[0].UpstreamAccepted || logs[0].UpstreamRequestID != "req-accepted-malformed" || !strings.Contains(logs[0].Error, "malformed JSON frame") {
		t.Fatalf("accepted malformed audit=%+v err=%v", logs, err)
	}
}

func TestGatewayForwardAcceptedCapacityNoticeDoesNotReplayOrMarkHealthy(t *testing.T) {
	for _, completeStructure := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete_structure_%t", completeStructure), func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			var primaryCalls, fallbackCalls int
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls++
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Oneapi-Request-Id", "req-accepted-capacity")
				_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_capacity\"}}\n\n"))
				if completeStructure {
					_, _ = w.Write([]byte("event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_capacity\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
						"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_capacity\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[]}}\n\n" +
						"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_capacity\",\"output_index\":0,\"content_index\":0,\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n"))
				} else {
					_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at \"}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"capacity. Please try a different model.\"}\n\n"))
				}
			}))
			defer primary.Close()
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls++
				_, _ = w.Write([]byte("data: {\"delta\":\"unexpected fallback\"}\n\n"))
			}))
			defer fallback.Close()

			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			failureAt := time.Now().Add(-time.Second)
			if err := store.RecordUpstreamKeyRoutingFailure(1, "primary", gateway.RoutingFailureTransport, 0, "existing failure", failureAt); err != nil {
				t.Fatalf("record existing routing failure: %v", err)
			}
			before, err := store.FindUpstreamKey(1, "primary")
			if err != nil {
				t.Fatalf("find existing routing failure: %v", err)
			}
			recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
			if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "event: response.completed") != completeStructure || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) != completeStructure || strings.Contains(recorder.Body.String(), "event: response.failed") == completeStructure || strings.Contains(strings.ToLower(recorder.Body.String()), "selected model is at capacity") || strings.Contains(recorder.Body.String(), "unexpected fallback") || primaryCalls != 1 || fallbackCalls != 0 {
				t.Fatalf("accepted capacity response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
			}
			logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
			if err != nil || total != 1 || len(logs) != 1 {
				t.Fatalf("list request logs: total=%d logs=%d err=%v", total, len(logs), err)
			}
			wantTerminal := "upstream_5xx"
			if completeStructure {
				wantTerminal = "transient_after_acceptance"
			}
			if logs[0].ChannelID != 1 || logs[0].StatusCode != http.StatusBadGateway || logs[0].TerminalFailureCode != wantTerminal || !logs[0].UpstreamAccepted || logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-accepted-capacity" || !strings.Contains(logs[0].Error, "capacity") {
				t.Fatalf("accepted capacity log=%+v", logs[0])
			}
			key, err := store.FindUpstreamKey(1, "primary")
			if err != nil || key.LastRoutingFailureAt == nil || before.LastRoutingFailureAt == nil || !key.LastRoutingFailureAt.Equal(*before.LastRoutingFailureAt) || key.RoutingFailureCount != before.RoutingFailureCount || key.RoutingCooldownUntil != nil || before.RoutingCooldownUntil != nil {
				t.Fatalf("accepted capacity changed routing health: before=%+v after=%+v err=%v", before, key, err)
			}
		})
	}
}

func TestGatewayForwardAcceptedInterruptionNeverReplays(t *testing.T) {
	for _, ending := range []string{"completed", "eof", "transport"} {
		t.Run(ending, func(t *testing.T) {
			var primaryCalls, fallbackCalls atomic.Int32
			created := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_interrupted\"}}\n\n"
			payload := created + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"Our servers are currently overloaded. Please try again later.\"}\n\n"
			if ending == "completed" {
				payload += "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_interrupted\",\"status\":\"completed\",\"error\":null}}\n\n"
			}
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				primaryCalls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				if ending == "transport" {
					w.Header().Set("Content-Length", strconv.Itoa(len(payload)+100))
				}
				_, _ = io.WriteString(w, payload)
			}))
			defer primary.Close()
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fallbackCalls.Add(1)
				_, _ = io.WriteString(w, "unexpected fallback")
			}))
			defer fallback.Close()
			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
				{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
			})
			response := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
			if primaryCalls.Load() != 1 || fallbackCalls.Load() != 0 || strings.Contains(response.Body.String(), gatewayConfirmedUpstreamInterruptionMessage) || strings.Contains(response.Body.String(), "unexpected fallback") {
				t.Fatalf("accepted interruption replay/leak: calls=%d/%d body=%q", primaryCalls.Load(), fallbackCalls.Load(), response.Body.String())
			}
			logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
			if err != nil || len(logs) != 1 || !logs[0].UpstreamAccepted || logs[0].ChannelID != 1 || logs[0].StatusCode != http.StatusBadGateway || logs[0].TerminalFailureCode == "" {
				t.Fatalf("accepted interruption audit=%+v err=%v", logs, err)
			}
			key, err := store.FindUpstreamKey(1, "primary")
			if err != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
				t.Fatalf("accepted interruption polluted health: %+v %v", key, err)
			}
		})
	}
}

func TestGatewayForwardAcceptedTransportBreakTerminatesWithoutReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls, fallbackCalls int
	acceptedFrame := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_broken\"}}\n\n"
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(acceptedFrame)+128))
		w.Header().Set("Connection", "close")
		w.Header().Set("X-Oneapi-Request-Id", "req-accepted-broken")
		_, _ = w.Write([]byte(acceptedFrame))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte("data: {\"delta\":\"unexpected fallback\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "resp_broken") || !strings.Contains(recorder.Body.String(), "event: response.completed") || !strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || strings.Contains(recorder.Body.String(), "event: response.failed") || strings.Contains(recorder.Body.String(), "unexpected fallback") || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("accepted broken response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(logs) != 1 {
		t.Fatalf("accepted broken logs: total=%d logs=%+v err=%v", total, logs, err)
	}
	if logs[0].ChannelID != 1 || logs[0].StatusCode != http.StatusBadGateway || logs[0].TerminalFailureCode != "transient_after_acceptance" || !logs[0].UpstreamAccepted || logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-accepted-broken" || !strings.Contains(logs[0].Error, "stream read failed after upstream acceptance") {
		t.Fatalf("accepted broken log=%+v", logs[0])
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("accepted transport break changed routing health: key=%+v err=%v", key, err)
	}
}

func TestGatewayForwardAcceptedPartialTextTransportBreakCompletesRetryWithoutReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls, fallbackCalls int
	partial := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_partial_broken\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_partial_broken\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_partial_broken\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_partial_broken\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial upstream answer. \"}\n\n"
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(partial)+128))
		w.Header().Set("Connection", "close")
		w.Header().Set("X-Oneapi-Request-Id", "req-accepted-partial-broken")
		_, _ = w.Write([]byte(partial))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte("data: {\"delta\":\"unexpected fallback\"}\n\n"))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":true}`)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "partial upstream answer. ") || !strings.Contains(recorder.Body.String(), "event: response.completed") || !strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || strings.Contains(recorder.Body.String(), "event: response.failed") || strings.Contains(recorder.Body.String(), "unexpected fallback") || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("accepted partial broken response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	payloads := gatewayStreamJSONPayloads(recorder.Body.Bytes())
	terminal := payloads[len(payloads)-1]
	response, _ := terminal["response"].(map[string]any)
	outputItems, _ := response["output"].([]any)
	item, _ := outputItems[0].(map[string]any)
	content, _ := item["content"].([]any)
	part, _ := content[0].(map[string]any)
	if part["text"] != "partial upstream answer. "+gatewayPublicRetryPromptMessage {
		t.Fatalf("completed retry text=%#v", part["text"])
	}
	logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(logs) != 1 {
		t.Fatalf("accepted partial broken logs: total=%d logs=%+v err=%v", total, logs, err)
	}
	if logs[0].ChannelID != 1 || logs[0].StatusCode != http.StatusBadGateway || logs[0].TerminalFailureCode != "transient_after_acceptance" || !logs[0].UpstreamAccepted || logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-accepted-partial-broken" || !strings.Contains(logs[0].Error, "stream read failed after upstream acceptance") {
		t.Fatalf("accepted partial broken log=%+v", logs[0])
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("accepted partial transport break changed routing health: key=%+v err=%v", key, err)
	}
}

func TestGatewayForwardPartialBufferedSuccessDoesNotReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var primaryCalls, fallbackCalls int
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls++
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "512")
		w.Header().Set("Connection", "close")
		w.Header().Set("X-Oneapi-Request-Id", "req-partial-buffered")
		_, _ = w.Write([]byte(`{"output_text":"partial result`))
	}))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		_, _ = w.Write([]byte(`{"output_text":"unexpected fallback"}`))
	}))
	defer fallback.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: primary.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallback.URL, ratio: 1},
	})
	recorder := runGatewayRetryTestRequest(svc, decision, "/v1/responses", `{"model":"gpt-test","stream":false}`)
	if recorder.Code != http.StatusBadGateway || !strings.Contains(recorder.Body.String(), gatewayPublicStreamFailureMessage) || strings.Contains(recorder.Body.String(), "partial result") || strings.Contains(recorder.Body.String(), "unexpected fallback") || primaryCalls != 1 || fallbackCalls != 0 {
		t.Fatalf("partial buffered response=%d %q calls=%d/%d", recorder.Code, recorder.Body.String(), primaryCalls, fallbackCalls)
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 1)
	if err != nil || len(logs) != 1 {
		t.Fatalf("list partial buffered log=%+v err=%v", logs, err)
	}
	if logs[0].ChannelID != 1 || logs[0].UpstreamAccepted || logs[0].BillingCommitted || logs[0].UpstreamRequestID != "req-partial-buffered" || !strings.Contains(logs[0].Error, "read response body after upstream body started") {
		t.Fatalf("partial buffered log=%+v", logs[0])
	}
	var partialAttempts []gatewayRouteAttemptTrace
	if err := json.Unmarshal([]byte(logs[0].RouteAttemptedChannels), &partialAttempts); err != nil || len(partialAttempts) != 1 || !partialAttempts[0].Physical || partialAttempts[0].ChannelID != 1 || partialAttempts[0].Reason != "response_body_started" {
		t.Fatalf("partial buffered dispatch evidence=%q traces=%+v err=%v", logs[0].RouteAttemptedChannels, partialAttempts, err)
	}
	if logs[0].PriorPhysicallyDispatchedFailureCount != 0 || logs[0].TerminalFailureCode != "response_body_started" {
		t.Fatalf("partial buffered telemetry = prior %d terminal %q", logs[0].PriorPhysicallyDispatchedFailureCount, logs[0].TerminalFailureCode)
	}
	key, err := store.FindUpstreamKey(1, "primary")
	if err != nil || key.LastRoutingFailureAt != nil || key.RoutingFailureCount != 0 || key.RoutingCooldownUntil != nil {
		t.Fatalf("partial buffered response changed routing health: key=%+v err=%v", key, err)
	}
}

func TestGatewayForwardHidesAllFailedDetailsFromDownstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"Authorization: Bearer tiny-secret upstream exploded"}`))
	}))
	defer failed.Close()

	svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{
		channelID: 1, name: "failed-channel", groupName: "failed-group", siteURL: failed.URL, ratio: 1,
	}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test"}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test"}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || !strings.Contains(recorder.Body.String(), `"status":"completed"`) {
		t.Fatalf("gateway response = status %d body %q", recorder.Code, recorder.Body.String())
	}
	for _, internal := range []string{"tiny-secret", "upstream", "failed-channel", "failed-group"} {
		if strings.Contains(recorder.Body.String(), internal) {
			t.Fatalf("downstream response leaked %q: %q", internal, recorder.Body.String())
		}
	}
	logs, total, _, err := store.ListRequestLogsPageFiltered(1, 10, storage.GatewayRequestLogFilter{})
	if err != nil || total != 1 || len(logs) != 1 || logs[0].StatusCode != http.StatusBadGateway || logs[0].TerminalFailureCode != "retry_prompt_after_exhaustion" || logs[0].Error == "" {
		t.Fatalf("all-failed retry prompt log: total=%d logs=%+v err=%v", total, logs, err)
	}
	if logs[0].RouteReason != gatewayRouteReasonAllFailed || !strings.Contains(logs[0].RouteDetails, "failed-channel/failed-group") || !strings.Contains(logs[0].RouteDetails, "upstream exploded") {
		t.Fatalf("administrator route details missing: %+v", logs[0])
	}
	if strings.Contains(logs[0].RouteDetails, "tiny-secret") || strings.Contains(logs[0].Error, "tiny-secret") {
		t.Fatalf("request log leaked the short credential: %+v", logs[0])
	}
}

func TestPrepareGatewayStreamPreludeTreatsDoneAndMetadataLimitAsEmpty(t *testing.T) {
	metadataFrame := "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\",\"content\":[]}}\n\n"
	longDelta := "data: {\"delta\":\"" + strings.Repeat("x", gatewayStreamPreludeLimit+1024) + "\"}"
	for _, tc := range []struct {
		name         string
		path         string
		message      string
		wantUseful   bool
		wantLimit    bool
		wantError    bool
		wantAccepted bool
	}{
		{name: "done", path: "/v1/responses", message: "data: [DONE]\n\n"},
		{name: "responses accepted", path: "/v1/responses", message: "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\"}}\n\n", wantAccepted: true},
		{name: "responses without id are not accepted", path: "/v1/responses", message: "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{}}\n\n"},
		{name: "responses mismatched event is not accepted", path: "/v1/responses", message: "event: response.in_progress\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\"}}\n\n"},
		{name: "responses frame on claude path is not accepted", path: "/v1/messages", message: "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\"}}\n\n"},
		{name: "claude accepted", path: "/v1/messages", message: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"role\":\"assistant\"}}\n\n", wantAccepted: true},
		{name: "claude without id is not accepted", path: "/v1/messages", message: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"role\":\"assistant\"}}\n\n"},
		{name: "claude content block accepted", path: "/v1/messages", message: "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n", wantAccepted: true},
		{name: "claude content block without index is not accepted", path: "/v1/messages", message: "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"text\"}}\n\n"},
		{name: "claude message delta accepted", path: "/v1/messages", message: "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":null}}\n\n", wantAccepted: true},
		{name: "chat accepted", path: "/v1/chat/completions", message: "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n", wantAccepted: true},
		{name: "chat without id is not accepted", path: "/v1/chat/completions", message: "data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"},
		{name: "chat empty delta accepted", path: "/v1/chat/completions", message: "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{}}]}\n\n", wantAccepted: true},
		{name: "chat empty choices accepted", path: "/v1/chat/completions", message: "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"choices\":[]}\n\n", wantAccepted: true},
		{name: "legacy completion delta accepted", path: "/v1/completions", message: "data: {\"id\":\"cmpl_123\",\"object\":\"text_completion\",\"choices\":[{\"text\":\"\",\"index\":0,\"finish_reason\":null}]}\n\n", wantAccepted: true},
		{name: "legacy completion without text is not accepted", path: "/v1/completions", message: "data: {\"id\":\"cmpl_123\",\"object\":\"text_completion\",\"choices\":[{\"index\":0,\"finish_reason\":null}]}\n\n", wantUseful: true},
		{name: "legacy completion without index is not accepted", path: "/v1/completions", message: "data: {\"id\":\"cmpl_123\",\"object\":\"text_completion\",\"choices\":[{\"text\":\"partial\",\"finish_reason\":null}]}\n\n", wantUseful: true},
		{name: "responses output item accepted", path: "/v1/responses", message: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"msg_123\",\"type\":\"message\",\"content\":[]}}\n\n", wantAccepted: true},
		{name: "responses future output item accepted", path: "/v1/responses", message: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"item_123\",\"type\":\"future_tool_call\"}}\n\n", wantAccepted: true},
		{name: "responses output item without id is not accepted", path: "/v1/responses", message: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\",\"content\":[]}}\n\n"},
		{name: "responses content part accepted", path: "/v1/responses", message: "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_123\",\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n", wantAccepted: true},
		{name: "responses event-only ping", path: "/v1/responses", message: "event: ping\n\n"},
		{name: "responses event-only heartbeat", path: "/v1/responses", message: "event: heartbeat\n\n"},
		{name: "responses event-only keep-alive", path: "/v1/responses", message: "event: keep-alive\n\n"},
		{name: "unknown metadata is not accepted", message: "event: heartbeat\ndata: {\"type\":\"heartbeat\"}\n\n"},
		{name: "output item added", message: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\",\"content\":[]}}\n\n"},
		{name: "empty function call added", message: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"name\":\"exec_command\",\"arguments\":\"\"}}\n\n"},
		{name: "content part added", message: "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n"},
		{name: "reasoning part added", message: "event: response.reasoning_summary_part.added\ndata: {\"type\":\"response.reasoning_summary_part.added\",\"part\":{\"type\":\"summary_text\",\"text\":\"\"}}\n\n"},
		{name: "message item with content", message: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"ready\"}]}}\n\n", wantUseful: true},
		{name: "upstream mcp call", message: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"mcp_call\",\"name\":\"mutating-tool\"}}\n\n", wantUseful: true},
		{name: "metadata limit", message: strings.Repeat(metadataFrame, 1400), wantLimit: true},
		{name: "long delta", message: longDelta, wantUseful: true},
		{name: "normal context discussion", message: "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"The context window token limit was exceeded in the example.\"}}\n\n", wantUseful: true},
		{name: "error", message: "event: error\ndata: {\"error\":{\"message\":\"upstream failed\"}}\n\n", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := tc.message
			path := tc.path
			if path == "" {
				path = "/v1/responses"
			}
			body, prelude, useful, accepted, err := prepareGatewayStreamPrelude(io.NopCloser(strings.NewReader(message)), path)
			if err != nil {
				t.Fatalf("prepare stream prelude: %v", err)
			}
			if useful != tc.wantUseful {
				t.Fatalf("useful=%t, want %t for %q", useful, tc.wantUseful, prelude)
			}
			if accepted != tc.wantAccepted {
				t.Fatalf("accepted=%t, want %t for %q", accepted, tc.wantAccepted, prelude)
			}
			if tc.wantLimit && len(prelude) < gatewayStreamPreludeLimit {
				t.Fatalf("metadata prelude = %d bytes, want at least %d", len(prelude), gatewayStreamPreludeLimit)
			}
			if tc.wantUseful && len(prelude) > gatewayStreamPreludeLimit {
				t.Fatalf("long delta prelude was not bounded: %d bytes", len(prelude))
			}
			if gotError := gatewayStreamFrameHasError(prelude); gotError != tc.wantError {
				t.Fatalf("error=%t, want %t for %q", gotError, tc.wantError, prelude)
			}
			replayed, err := io.ReadAll(body)
			if err != nil || string(replayed) != message {
				t.Fatalf("replayed=%d/%d err=%v", len(replayed), len(message), err)
			}
		})
	}
}

func TestPrepareGatewayStreamPreludeRejectsMalformedResponsesFrame(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "truncated json", body: "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\""},
		{name: "empty data", body: "event: response.output_text.delta\ndata:\n"},
		{name: "event without data", body: "event: response.output_text.delta\n"},
		{name: "complete malformed json", body: "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"\n\n"},
		{name: "complete empty data", body: "event: response.output_text.delta\ndata:\n\n"},
		{name: "complete event without data", body: "event: response.output_text.delta\n\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			acceptedCalls := 0
			prepared, prelude, useful, accepted, err := prepareGatewayStreamPreludeWithAcceptance(
				io.NopCloser(strings.NewReader(test.body)),
				"/v1/responses",
				func(gatewayStreamAcceptanceEvidence) error {
					acceptedCalls++
					return nil
				},
			)
			if !errors.Is(err, errGatewayMalformedResponsesFrame) || useful || accepted || acceptedCalls != 0 {
				t.Fatalf("malformed prelude = %q useful=%t accepted=%t calls=%d err=%v", prelude, useful, accepted, acceptedCalls, err)
			}
			replayed, readErr := io.ReadAll(prepared)
			if readErr != nil || string(replayed) != test.body {
				t.Fatalf("replayed malformed prelude = %q err=%v", replayed, readErr)
			}
		})
	}
}

func TestPrepareGatewayStreamPreludeAcceptsMultilineResponsesJSONAtEOF(t *testing.T) {
	body := "event: response.output_text.delta\ndata: {\"type\":\ndata: \"response.output_text.delta\",\"delta\":\"partial\"}\n"
	prepared, prelude, useful, accepted, err := prepareGatewayStreamPrelude(
		io.NopCloser(strings.NewReader(body)),
		"/v1/responses",
	)
	if err != nil || !useful || accepted {
		t.Fatalf("multiline prelude = %q useful=%t accepted=%t err=%v", prelude, useful, accepted, err)
	}
	replayed, readErr := io.ReadAll(prepared)
	if readErr != nil || string(replayed) != body {
		t.Fatalf("replayed multiline prelude = %q err=%v", replayed, readErr)
	}
}

func TestGatewayCacheAffinityKeepsFallbackWithinSameSession(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewGateway(db)
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{
		DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 2, GroupName: "fallback", ModelName: "gpt-test",
		Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, RouteReason: gatewayRouteReasonFallback, RouteSelectionRank: 2,
		RouteAttemptedChannels: `[{"channel_id":1,"channel_name":"primary","group_name":"primary","reason":"upstream_status","status_code":503,"eligible_rank":1,"physically_dispatched":true}]`,
		CreatedAt:              time.Now(),
	}); err != nil {
		t.Fatalf("append fallback route: %v", err)
	}
	svc := gateway.NewService(nil, nil, store, nil, nil, nil)
	candidates := []gateway.Candidate{
		{Channel: storage.Channel{ID: 1, Name: "primary"}, Rate: storage.RateSnapshot{ModelName: "primary"}, EffectivePrice: 1, LatencySamples: 10},
		{Channel: storage.Channel{ID: 2, Name: "fallback"}, Rate: storage.RateSnapshot{ModelName: "fallback"}, EffectivePrice: 1},
	}
	reordered, applied := gatewayCacheAffinityCandidates(&Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1}, "gpt-test", "session-a", candidates)
	if !applied || reordered[0].Channel.ID != 2 {
		t.Fatalf("same-session fallback affinity = %+v, applied=%t", reordered, applied)
	}
	otherKey, applied := gatewayCacheAffinityCandidates(&Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 2}, "gpt-test", "session-a", candidates)
	if applied || otherKey[0].Channel.ID != 1 {
		t.Fatalf("other downstream key was affected: %+v, applied=%t", otherKey, applied)
	}
	otherSession, applied := gatewayCacheAffinityCandidates(&Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1}, "gpt-test", "session-b", candidates)
	if applied || otherSession[0].Channel.ID != 1 {
		t.Fatalf("other session was affected: %+v, applied=%t", otherSession, applied)
	}
}

func TestGatewayCacheAffinityKeepsCurrentWinnerWithinSameSession(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewGateway(db)
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{
		DownstreamKeyID: 1, SessionHash: "session-current-winner", ChannelID: 2, GroupName: "primary", ModelName: "gpt-test",
		Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, RouteReason: gatewayRouteReasonSmartPrimary, RouteSelectionRank: 1,
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("append primary route: %v", err)
	}
	svc := gateway.NewService(nil, nil, store, nil, nil, nil)
	candidates := []gateway.Candidate{
		{Channel: storage.Channel{ID: 2, Name: "primary"}, Rate: storage.RateSnapshot{ModelName: "primary"}, EffectivePrice: 1},
	}
	ordered, applied := gatewayCacheAffinityCandidates(&Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1}, "gpt-test", "session-current-winner", candidates)
	if !applied || len(ordered) != 1 || ordered[0].Channel.ID != 2 {
		t.Fatalf("current winner affinity = %+v, applied=%t", ordered, applied)
	}
}

func TestGatewayCacheAffinityDoesNotPromoteMoreExpensiveRoute(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewGateway(db)
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{
		DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 2, GroupName: "fallback", ModelName: "gpt-test",
		Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, RouteReason: gatewayRouteReasonFallback, RouteSelectionRank: 2,
		RouteAttemptedChannels: `[{"channel_id":1,"channel_name":"cheaper","group_name":"primary","reason":"upstream_status","status_code":503,"eligible_rank":1,"physically_dispatched":true}]`,
		CreatedAt:              time.Now(),
	}); err != nil {
		t.Fatalf("append fallback route: %v", err)
	}
	svc := gateway.NewService(nil, nil, store, nil, nil, nil)
	candidates := []gateway.Candidate{
		{Channel: storage.Channel{ID: 1, Name: "cheaper"}, Rate: storage.RateSnapshot{ModelName: "primary"}, EffectivePrice: 0.1, LatencySamples: 10},
		{Channel: storage.Channel{ID: 2, Name: "expensive-fallback"}, Rate: storage.RateSnapshot{ModelName: "fallback"}, EffectivePrice: 1},
	}
	reordered, applied := gatewayCacheAffinityCandidates(&Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1}, "gpt-test", "session-a", candidates)
	if applied || reordered[0].Channel.ID != 1 {
		t.Fatalf("more expensive affinity route should not displace smart primary: %+v, applied=%t", reordered, applied)
	}
}

func TestGatewayCacheAffinityDoesNotPromoteRouteCostlierThanLowerRankedCandidate(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewGateway(db)
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{
		DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 3, GroupName: "fallback", ModelName: "gpt-test",
		Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, RouteReason: gatewayRouteReasonFallback, RouteSelectionRank: 3,
		RouteAttemptedChannels: `[{"channel_id":1,"channel_name":"fast","group_name":"fast","reason":"upstream_status","status_code":503,"eligible_rank":1,"physically_dispatched":true},{"channel_id":2,"channel_name":"cheapest","group_name":"cheap","reason":"upstream_status","status_code":503,"eligible_rank":2,"physically_dispatched":true}]`,
		CreatedAt:              time.Now(),
	}); err != nil {
		t.Fatalf("append fallback route: %v", err)
	}
	svc := gateway.NewService(nil, nil, store, nil, nil, nil)
	candidates := []gateway.Candidate{
		{Channel: storage.Channel{ID: 1, Name: "fast"}, Rate: storage.RateSnapshot{ModelName: "fast"}, EffectivePrice: 1, LatencySamples: 10},
		{Channel: storage.Channel{ID: 2, Name: "cheapest"}, Rate: storage.RateSnapshot{ModelName: "cheap"}, EffectivePrice: 0.1},
		{Channel: storage.Channel{ID: 3, Name: "fallback"}, Rate: storage.RateSnapshot{ModelName: "fallback"}, EffectivePrice: 0.5},
	}
	reordered, applied := gatewayCacheAffinityCandidates(&Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1}, "gpt-test", "session-a", candidates)
	if applied || reordered[0].Channel.ID != 1 {
		t.Fatalf("affinity route costlier than a lower-ranked eligible candidate was promoted: %+v, applied=%t", reordered, applied)
	}
}

func TestGatewaySessionAffinityOriginRejectsModelFilteredOrigins(t *testing.T) {
	origin := &storage.GatewayRecentRoute{
		ID:            1,
		RouteReason:   gatewayRouteReasonModelFiltered,
		SelectionRank: 1,
		RouteDetails:  "selected compatible/group (eligible priority #1)",
	}
	if gatewaySessionAffinityOriginAuditable(origin) {
		t.Fatalf("model-filtered origin without filter evidence was accepted: %+v", origin)
	}
	origin.RouteDetails = "higher-ranked unavailable/group filtered: model gpt-test is not listed by the upstream group; selected compatible/group (eligible priority #1)"
	if gatewaySessionAffinityOriginAuditable(origin) {
		t.Fatalf("model-filtered origin with text evidence was accepted: %+v", origin)
	}
}

func TestGatewaySessionAffinityFallbackRequiresCompleteRankedEvidence(t *testing.T) {
	valid := `[{"channel_id":1,"channel_name":"first","group_name":"first","reason":"upstream_status","status_code":503,"eligible_rank":1,"physically_dispatched":true},{"channel_id":2,"channel_name":"second","group_name":"second","reason":"transport","eligible_rank":2,"physically_dispatched":true}]`
	for _, tc := range []struct {
		name  string
		trace string
		want  bool
	}{
		{name: "complete", trace: valid, want: true},
		{name: "legacy missing physical fact", trace: `[{"channel_id":1,"group_name":"first","reason":"upstream_status","eligible_rank":1},{"channel_id":2,"group_name":"second","reason":"transport","eligible_rank":2}]`, want: false},
		{name: "explicit preparation failure", trace: `[{"channel_id":1,"group_name":"first","reason":"upstream_status","eligible_rank":1,"physically_dispatched":false},{"channel_id":2,"group_name":"second","reason":"transport","eligible_rank":2,"physically_dispatched":true}]`, want: false},
		{name: "missing predecessor", trace: `[{"channel_id":1,"group_name":"first","reason":"upstream_status","eligible_rank":1,"physically_dispatched":true}]`, want: false},
		{name: "duplicate rank", trace: `[{"channel_id":1,"group_name":"first","reason":"upstream_status","eligible_rank":1,"physically_dispatched":true},{"channel_id":2,"group_name":"second","reason":"transport","eligible_rank":1,"physically_dispatched":true}]`, want: false},
		{name: "gapped rank", trace: `[{"channel_id":1,"group_name":"first","reason":"upstream_status","eligible_rank":1,"physically_dispatched":true},{"channel_id":2,"group_name":"second","reason":"transport","eligible_rank":3,"physically_dispatched":true}]`, want: false},
		{name: "non failure", trace: `[{"channel_id":1,"group_name":"first","reason":"candidate_resolution","eligible_rank":1,"physically_dispatched":true},{"channel_id":2,"group_name":"second","reason":"transport","eligible_rank":2,"physically_dispatched":true}]`, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := &storage.GatewayRecentRoute{ID: 1, RouteReason: gatewayRouteReasonFallback, SelectionRank: 3, RouteAttemptedChannels: tc.trace}
			if got := gatewaySessionAffinityOriginAuditable(route); got != tc.want {
				t.Fatalf("auditable=%t, want %t for %+v", got, tc.want, route)
			}
		})
	}
}

func TestGatewayCacheAffinitySkipsUnauditableLegacyOrigin(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewGateway(db)
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{
		DownstreamKeyID: 1, SessionHash: "session-a", ChannelID: 2, GroupName: "fallback", ModelName: "gpt-test",
		Method: http.MethodPost, Path: "/v1/responses", StatusCode: http.StatusOK, RouteReason: gatewayRouteReasonFallback, RouteSelectionRank: 2,
		CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("append legacy fallback route: %v", err)
	}
	svc := gateway.NewService(nil, nil, store, nil, nil, nil)
	candidates := []gateway.Candidate{
		{Channel: storage.Channel{ID: 1, Name: "primary"}, Rate: storage.RateSnapshot{ModelName: "primary"}, EffectivePrice: 1, LatencySamples: 10},
		{Channel: storage.Channel{ID: 2, Name: "fallback"}, Rate: storage.RateSnapshot{ModelName: "fallback"}, EffectivePrice: 1},
	}
	reordered, applied := gatewayCacheAffinityCandidates(&Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1}, "gpt-test", "session-a", candidates)
	if applied || reordered[0].Channel.ID != 1 {
		t.Fatalf("legacy origin without selection evidence changed current routing: %+v, applied=%t", reordered, applied)
	}
}

func TestGatewayCandidateFirstByteBudgetUsesOnlyFixedAndAuditableAffinityPolicy(t *testing.T) {
	configured := 30 * time.Second
	auditableOrigin := &storage.GatewayRecentRoute{
		ID:            41,
		RouteReason:   gatewayRouteReasonSmartPrimary,
		SelectionRank: 1,
	}
	for _, test := range []struct {
		name         string
		decision     *gatewayRouteDecision
		budget       time.Duration
		imageRequest bool
		want         time.Duration
	}{
		{name: "ordinary request", decision: &gatewayRouteDecision{SmartRouting: true}, budget: configured, want: configured},
		{name: "recent upstream success does not extend ordinary budget", decision: &gatewayRouteDecision{SmartRouting: true}, budget: configured, want: configured},
		{name: "fixed route disables first-byte deadline", decision: &gatewayRouteDecision{SmartRouting: false}, budget: configured, want: 0},
		{name: "fixed route ignores affinity metadata", decision: &gatewayRouteDecision{SmartRouting: false, AffinityApplied: true, AffinityOrigin: auditableOrigin}, budget: configured, want: 0},
		{name: "auditable affinity", decision: &gatewayRouteDecision{SmartRouting: true, AffinityApplied: true, AffinityOrigin: auditableOrigin}, budget: configured, want: gatewaySessionAffinityFirstByteTimeout},
		{name: "direct image keeps configured budget", decision: &gatewayRouteDecision{SmartRouting: true, AffinityApplied: true, AffinityOrigin: auditableOrigin}, budget: configured, imageRequest: isGatewayImageGenerationRequest("/v1/images/generations", []byte(`{"model":"gpt-image-1"}`)), want: configured},
		{name: "responses image tool keeps configured budget", decision: &gatewayRouteDecision{SmartRouting: true, AffinityApplied: true, AffinityOrigin: auditableOrigin}, budget: configured, imageRequest: isGatewayImageGenerationRequest("/v1/responses", []byte(`{"model":"gpt-5","tools":[{"type":"image_generation"}]}`)), want: configured},
		{name: "missing affinity origin", decision: &gatewayRouteDecision{SmartRouting: true, AffinityApplied: true}, budget: configured, want: configured},
		{name: "unauditable affinity origin", decision: &gatewayRouteDecision{SmartRouting: true, AffinityApplied: true, AffinityOrigin: &storage.GatewayRecentRoute{ID: 42, RouteReason: gatewayRouteReasonFallback, SelectionRank: 2}}, budget: configured, want: configured},
		{name: "larger configured budget remains", decision: &gatewayRouteDecision{SmartRouting: true, AffinityApplied: true, AffinityOrigin: auditableOrigin}, budget: 2 * time.Minute, want: 2 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := gatewayCandidateFirstByteBudget(test.decision, test.budget, test.imageRequest); got != test.want {
				t.Fatalf("first-byte budget = %s, want %s", got, test.want)
			}
		})
	}
}

func TestGatewayForwardWaitingProbeTimeoutFallsBackWithoutPrematureCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var stalledCalls, fallbackCalls int
	stalledUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "Reply OK.") {
			stalledCalls++
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: heartbeat\ndata: {\"type\":\"heartbeat\"}\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer stalledUpstream.Close()
	fallbackUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"fallback after timeout\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_fallback_timeout\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer fallbackUpstream.Close()

	db := openTestDB(t)
	svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{
		{channelID: 1, name: "stalled", groupName: "stalled", siteURL: stalledUpstream.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallbackUpstream.URL, ratio: 1},
	})
	const candidateBudget = 500 * time.Millisecond
	svc.UpdateRoutingConfig(gateway.RoutingConfig{WaitingProbeInterval: 20 * time.Millisecond, WaitingProbeTimeout: candidateBudget, FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: candidateBudget}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "fallback after timeout") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) {
		t.Fatalf("gateway response = status %d body %q", recorder.Code, recorder.Body.String())
	}
	if stalledCalls != 1 || fallbackCalls != 1 {
		t.Fatalf("upstream calls stalled=%d fallback=%d, want 1/1", stalledCalls, fallbackCalls)
	}
	stalledKey, err := store.FindUpstreamKey(1, "stalled")
	if err != nil {
		t.Fatalf("find stalled upstream key: %v", err)
	}
	if stalledKey.LastRoutingFailureKind != gateway.RoutingFailureResponseTimeout || stalledKey.LastRoutingFailureStatus != http.StatusGatewayTimeout || stalledKey.LastRoutingFailureAt == nil || stalledKey.RoutingCooldownUntil != nil || stalledKey.RoutingFailureCount != 1 {
		t.Fatalf("single exhausted timeout was not retained without isolation: %+v", stalledKey)
	}
	var requestLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&requestLog).Error; err != nil {
		t.Fatalf("find request log: %v", err)
	}
	if requestLog.ChannelID != 2 || requestLog.GroupName != "fallback" || requestLog.RouteReason != gatewayRouteReasonFallback || requestLog.StatusCode != http.StatusOK || requestLog.TerminalFailureCode != "" || !strings.Contains(requestLog.RouteDetails, "response timed out before first output") || requestLog.Error != "" {
		t.Fatalf("request log = %+v", requestLog)
	}
}

func TestGatewayForwardBoundsColdModelProbeWithinCandidateTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var stalledModelCalls, stalledResponseCalls, fallbackCalls int
	stalledUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			stalledModelCalls++
			<-r.Context().Done()
		case "/v1/responses":
			stalledResponseCalls++
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer stalledUpstream.Close()
	fallbackUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalls++
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":0,\"delta\":\"fallback after model probe timeout\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_fallback_probe\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer fallbackUpstream.Close()

	db := openTestDB(t)
	svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{
		{channelID: 1, name: "cold", groupName: "cold", siteURL: stalledUpstream.URL, ratio: 0.1},
		{channelID: 2, name: "fallback", groupName: "fallback", siteURL: fallbackUpstream.URL, ratio: 1},
	})
	if err := store.UpdateUpstreamKeyModelFamilies(
		1, "cold", "", "gpt-test", time.Now(), "", gatewayCandidateGenerationArgs(decision.Candidates[0])...,
	); err != nil {
		t.Fatalf("clear cold upstream model families: %v", err)
	}
	const candidateBudget = 500 * time.Millisecond
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: candidateBudget}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	startedAt := time.Now()
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, startedAt)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "event: response.completed") || !strings.Contains(recorder.Body.String(), "fallback after model probe timeout") || strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) {
		t.Fatalf("gateway response = status %d body %q", recorder.Code, recorder.Body.String())
	}
	if stalledModelCalls != 1 || stalledResponseCalls != 0 || fallbackCalls != 1 {
		t.Fatalf("upstream calls models=%d stalled_response=%d fallback=%d, want 1/0/1", stalledModelCalls, stalledResponseCalls, fallbackCalls)
	}
	if elapsed := time.Since(startedAt); elapsed > 2*candidateBudget+300*time.Millisecond {
		t.Fatalf("cold model probe request took %s, want bounded per-candidate timeouts", elapsed)
	}
	var requestLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&requestLog).Error; err != nil {
		t.Fatalf("find request log: %v", err)
	}
	if requestLog.ChannelID != 2 || requestLog.GroupName != "fallback" || requestLog.RouteReason != gatewayRouteReasonSmartPrimary || requestLog.RouteAttemptedChannels != "" || requestLog.RealFallback || requestLog.StatusCode != http.StatusOK || requestLog.TerminalFailureCode != "" || !strings.Contains(requestLog.RouteDetails, "response timed out before first output") || requestLog.Error != "" {
		t.Fatalf("request log = %+v", requestLog)
	}
}

func TestGatewaySuccessfulStreamAfterFirstByteDoesNotEnterCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_success\"}}\n\n"))
		w.(http.Flusher).Flush()
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"first output\"}\n\n"))
		w.(http.Flusher).Flush()
		time.Sleep(550 * time.Millisecond)
		_, _ = w.Write([]byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_success\",\"status\":\"completed\",\"error\":null}}\n\n"))
	}))
	defer upstream.Close()

	db := openTestDB(t)
	svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{
		{channelID: 1, name: "primary", groupName: "primary", siteURL: upstream.URL, ratio: 1},
	})
	const firstByteBudget = 500 * time.Millisecond
	svc.UpdateRoutingConfig(gateway.RoutingConfig{FailurePolicy: gateway.RoutingFailurePolicy{ResponseTimeout: firstByteBudget}})
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
	startedAt := time.Now()
	gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, startedAt)
	elapsed := time.Since(startedAt)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "first output") {
		t.Fatalf("gateway response = status %d body %q", recorder.Code, recorder.Body.String())
	}
	if elapsed <= firstByteBudget {
		t.Fatalf("completed stream in %s, want total response time beyond first-byte budget %s", elapsed, firstByteBudget)
	}
	stored, err := store.FindUpstreamKey(1, "primary")
	if err != nil {
		t.Fatalf("find upstream key: %v", err)
	}
	if stored.LastRoutingFailureAt != nil || stored.RoutingCooldownUntil != nil {
		t.Fatalf("completed stream entered global cooldown: %+v", stored)
	}
	var requestLog storage.GatewayRequestLog
	if err := db.Order("id DESC").First(&requestLog).Error; err != nil {
		t.Fatalf("find request log: %v", err)
	}
	if requestLog.FirstByteMs < 80 || requestLog.FirstByteMs >= firstByteBudget.Milliseconds() {
		t.Fatalf("first useful output latency = %dms, want metadata excluded and output inside %s budget", requestLog.FirstByteMs, firstByteBudget)
	}
}

func TestGatewayStreamErrorAfterOutputDoesNotKeepSessionAffinity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		errorMessage  string
		errorFragment string
	}{
		{
			name:          "capacity",
			errorMessage:  "Stream disconnected before completion: Concurrency limit exceeded for account, please retry later",
			errorFragment: "Concurrency limit exceeded",
		},
		{
			name:          "generic upstream error",
			errorMessage:  "upstream internal error",
			errorFragment: "upstream internal error",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			var upstreamCalls int
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				upstreamCalls++
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_stream_error\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"partial output\"}\n\n"))
				_, _ = w.Write([]byte(`data: {"error":{"message":"` + tc.errorMessage + `"}}` + "\n\n"))
			}))
			defer upstream.Close()

			db := openTestDB(t)
			svc, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{
				{channelID: 1, name: "primary", groupName: "primary", siteURL: upstream.URL, ratio: 1},
			})
			sessionHash := strings.Repeat("a", 64)
			if err := store.AppendRequestLog(&storage.GatewayRequestLog{
				DownstreamKeyID: 1, ChannelID: 1, GroupName: "primary", ModelName: "gpt-test",
				SessionHash: sessionHash, Method: http.MethodPost, Path: "/v1/responses",
				StatusCode: http.StatusOK, RouteReason: gatewayRouteReasonSmartPrimary, RouteSelectionRank: 1, CreatedAt: time.Now().Add(-time.Second),
			}); err != nil {
				t.Fatalf("append prior successful route: %v", err)
			}
			svc.MarkUpstreamGroupRoutingFailure(1, "primary", gateway.RoutingFailureTransport, http.StatusBadGateway, "previous transport failure", gatewayCandidateGenerationArgs(decision.Candidates[0])...)
			before, err := store.FindUpstreamKey(1, "primary")
			if err != nil {
				t.Fatalf("find upstream key before stream failure: %v", err)
			}
			decision.SmartRouting = true
			decision.SessionHash = sessionHash
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-test","stream":true}`))
			gatewayForward(ctx, &Deps{Gateway: svc}, &storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: true}, decision, "gpt-test", "", []byte(`{"model":"gpt-test","stream":true}`), gatewayModelMappingResult{Model: "gpt-test"}, time.Now())

			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "partial output") {
				var latest storage.GatewayRequestLog
				_ = db.Order("id DESC").First(&latest).Error
				t.Fatalf("gateway response = status %d body %q upstream_calls=%d latest_log=%+v", recorder.Code, recorder.Body.String(), upstreamCalls, latest)
			}
			if strings.Contains(recorder.Body.String(), tc.errorFragment) || !strings.Contains(recorder.Body.String(), gatewayPublicStreamFailureMessage) {
				t.Fatalf("downstream stream exposed upstream error: %q", recorder.Body.String())
			}
			var requestLog storage.GatewayRequestLog
			if err := db.Order("id DESC").First(&requestLog).Error; err != nil {
				t.Fatalf("find request log: %v", err)
			}
			if !strings.Contains(requestLog.Error, tc.errorFragment) || storage.IsSuccessfulGatewayRequest(&requestLog) || requestLog.CostReconcileAt != nil {
				t.Fatalf("stream error log must be failed: %+v", requestLog)
			}
			route, err := store.LastSuccessfulRoute(1, sessionHash, "gpt-test", time.Now().Add(-time.Minute))
			if err != nil {
				t.Fatalf("last successful route: %v", err)
			}
			if route != nil {
				t.Fatalf("stream error kept stale session affinity: %+v", route)
			}
			upstreamKey, err := store.FindUpstreamKey(1, "primary")
			if err != nil {
				t.Fatalf("find upstream key: %v", err)
			}
			if upstreamKey.LastRoutingFailureKind != before.LastRoutingFailureKind || upstreamKey.RoutingFailureCount != before.RoutingFailureCount || upstreamKey.LastRoutingFailureAt == nil || before.LastRoutingFailureAt == nil || !upstreamKey.LastRoutingFailureAt.Equal(*before.LastRoutingFailureAt) || upstreamKey.RoutingCooldownUntil != nil || before.RoutingCooldownUntil != nil {
				t.Fatalf("stream error changed global routing state: before=%+v after=%+v", before, upstreamKey)
			}
		})
	}
}

func TestGatewaySSECapacityNotification(t *testing.T) {
	for _, payload := range []string{
		"event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n",
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at \"}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"capacity. Please try a different model.\"}\n\n",
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Our servers are currently overloaded. Please try again later.\"}\n\n",
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Our servers are currently overloaded. Please \"}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"try again later.\"}\n\n",
	} {
		if !isGatewaySSECapacityNotification([]byte(payload)) {
			t.Fatalf("capacity notification was not recognized: %q", payload)
		}
	}
	for _, payload := range []string{
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Selected model is at capacity. Please try a different model.\"}\n\n",
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Our servers are currently overloaded. Please try again later. Continue with the answer.\"}\n\n",
		"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"text\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n",
		`{"type":"response.output_text.delta","delta":"⚠ Selected model is at capacity. Please try a different model."}`,
		`{"output":[{"type":"message","content":[{"type":"output_text","text":"⚠ Selected model is at capacity. Please try a different model."}]}]}`,
	} {
		if isGatewaySSECapacityNotification([]byte(payload)) {
			t.Fatalf("ordinary output was classified as capacity notice: %q", payload)
		}
	}
	partial := []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at \"}\n\n")
	if isGatewaySSECapacityNotification(partial) || !gatewaySSECapacityNotificationPending(partial) {
		t.Fatalf("capacity notification prefix state was not preserved: %q", partial)
	}
	unmarkedPartial := []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Our servers are currently overloaded. Please \"}\n\n")
	if isGatewaySSECapacityNotification(unmarkedPartial) || !gatewaySSECapacityNotificationPending(unmarkedPartial) {
		t.Fatalf("unmarked capacity notification prefix state was not preserved: %q", unmarkedPartial)
	}
	prepared, prelude, useful, accepted, err := prepareGatewayStreamPrelude(io.NopCloser(bytes.NewReader(partial)), "/v1/responses")
	if err != nil || !useful || accepted || !bytes.Equal(prelude, partial) {
		t.Fatalf("unfinished warning prelude = %q useful=%t accepted=%t err=%v", prelude, useful, accepted, err)
	}
	replayed, err := io.ReadAll(prepared)
	if err != nil || !bytes.Equal(replayed, partial) {
		t.Fatalf("unfinished warning replay = %q err=%v", replayed, err)
	}
}

func TestPrepareGatewayStreamPreludePreservesAcceptanceThroughConfirmedInterruption(t *testing.T) {
	const created = "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_overloaded\"}}\n\n"
	const completed = "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":3,\"response\":{\"id\":\"resp_overloaded\",\"status\":\"completed\",\"error\":null}}\n\n"
	const interruption = "Our servers are currently overloaded. Please try again later."
	for _, tc := range []struct {
		name              string
		body              string
		wantInterruption  bool
		wantAccepted      bool
		wantAcceptedCalls int
	}{
		{
			name:              "single interruption delta",
			body:              created + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"" + interruption + "\"}\n\n" + completed,
			wantInterruption:  true,
			wantAccepted:      true,
			wantAcceptedCalls: 1,
		},
		{
			name: "split interruption deltas",
			body: created + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"Our servers are currently over\"}\n\n" +
				"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"delta\":\"loaded. Please try again later.\"}\n\n" + completed,
			wantInterruption:  true,
			wantAccepted:      true,
			wantAcceptedCalls: 1,
		},
		{
			name: "standard responses closing frames",
			body: created + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"item_id\":\"msg_overloaded\",\"delta\":\"" + interruption + "\"}\n\n" +
				"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"sequence_number\":2,\"item_id\":\"msg_overloaded\",\"text\":\"" + interruption + "\"}\n\n" +
				"event: response.content_part.done\ndata: {\"type\":\"response.content_part.done\",\"sequence_number\":3,\"item_id\":\"msg_overloaded\",\"part\":{\"type\":\"output_text\",\"text\":\"" + interruption + "\"}}\n\n" +
				"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":4,\"item\":{\"id\":\"msg_overloaded\",\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"" + interruption + "\"}]}}\n\n" + completed,
			wantInterruption:  true,
			wantAccepted:      true,
			wantAcceptedCalls: 1,
		},
		{
			name: "quoted interruption continues as normal output",
			body: created + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"" + interruption + "\"}\n\n" +
				"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"delta\":\" This sentence is part of the answer.\"}\n\n" + completed,
			wantAccepted:      true,
			wantAcceptedCalls: 1,
		},
		{
			name: "interruption done frame contains additional output",
			body: created + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"" + interruption + "\"}\n\n" +
				"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"sequence_number\":2,\"text\":\"" + interruption + " This sentence is part of the answer.\"}\n\n" + completed,
			wantAccepted:      true,
			wantAcceptedCalls: 1,
		},
		{
			name:              "warning-marked capacity remains accepted",
			body:              created + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n",
			wantAccepted:      true,
			wantAcceptedCalls: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acceptedCalls := 0
			prepared, prelude, useful, accepted, err := prepareGatewayStreamPreludeWithAcceptance(
				io.NopCloser(strings.NewReader(tc.body)),
				"/v1/responses",
				func(gatewayStreamAcceptanceEvidence) error {
					acceptedCalls++
					return nil
				},
			)
			if err != nil || !useful || accepted != tc.wantAccepted || acceptedCalls != tc.wantAcceptedCalls {
				t.Fatalf("prelude useful=%t accepted=%t calls=%d err=%v body=%q", useful, accepted, acceptedCalls, err, prelude)
			}
			if gotRetry := isGatewaySSEUpstreamInterruptionNotification(prelude); gotRetry != tc.wantInterruption {
				t.Fatalf("confirmed interruption=%t, want %t for %q", gotRetry, tc.wantInterruption, prelude)
			}
			replayed, readErr := io.ReadAll(prepared)
			if readErr != nil || string(replayed) != tc.body {
				t.Fatalf("replayed interruption prelude=%q err=%v", replayed, readErr)
			}
		})
	}
}

func TestPrepareGatewayStreamPreludeReturnsAtConfirmedInterruptionTerminal(t *testing.T) {
	const payload = "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_open\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"Our servers are currently overloaded. Please try again later.\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_open\",\"status\":\"completed\",\"error\":null}}\n\n"
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	type result struct {
		body     io.ReadCloser
		prelude  []byte
		useful   bool
		accepted bool
		err      error
	}
	resultCh := make(chan result, 1)
	go func() {
		body, prelude, useful, accepted, err := prepareGatewayStreamPrelude(reader, "/v1/responses")
		resultCh <- result{body: body, prelude: prelude, useful: useful, accepted: accepted, err: err}
	}()
	if _, err := io.WriteString(writer, payload); err != nil {
		t.Fatalf("write open interruption stream: %v", err)
	}
	select {
	case got := <-resultCh:
		defer got.body.Close()
		if got.err != nil || !got.useful || !got.accepted || string(got.prelude) != payload {
			t.Fatalf("open interruption prelude=%q useful=%t accepted=%t err=%v", got.prelude, got.useful, got.accepted, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("confirmed interruption waited for the upstream connection to close")
	}
}

func TestGatewayStreamErrorAfterPrelude(t *testing.T) {
	for _, message := range []string{
		"data: {\"error\":\"Stream disconnected before completion: Concurrency limit exceeded for account, please retry later\"}\n\n",
		"data: {\"error\":\"Stream disconnected before completion: Upstream rate limit exceeded, please retry later\"}\n\n",
		"data: {\"error\":\"Stream disconnected before completion: Selected model is at capacity. Please try a different model.\"}\n\n",
		"data: {\"error\":{\"message\":\"upstream internal error\"}}\n\n",
		"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"response failed upstream error\"}}}\n\n",
	} {
		failure := gatewayStreamError([]byte("data: {\"delta\":\"first chunk\"}\n\n"+message), "sk-secret-token")
		if failure == "" {
			t.Fatalf("stream error was not detected: %q", message)
		}
		if strings.Contains(failure, "sk-secret-token") {
			t.Fatalf("stream failure leaked key: %q", failure)
		}
	}
	if failure := gatewayStreamError([]byte("data: {\"delta\":\"normal output\"}\n\n"), ""); failure != "" {
		t.Fatalf("normal stream content was classified as an error: %q", failure)
	}
}

func TestGatewaySSEStreamSanitizerRedactsErrorFrames(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		body string
		want string
	}{
		{
			name: "responses failure",
			path: "/v1/responses",
			body: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n" +
				"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"Stream disconnected before completion: secret upstream detail\"}}}\n\n",
			want: "event: response.failed",
		},
		{
			name: "chat failure",
			path: "/v1/chat/completions",
			body: "data: {\"delta\":\"partial\"}\n\n" +
				"data: {\"error\":{\"message\":\"secret upstream detail\"}}\n\n",
			want: gatewayPublicStreamFailureMessage,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.body)), raw, "sk-private", tc.path)
			got, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read sanitized stream: %v", err)
			}
			text := string(got)
			if !strings.Contains(text, "partial") || !strings.Contains(text, tc.want) {
				t.Fatalf("sanitized stream = %q, want partial output and %q", text, tc.want)
			}
			if strings.Contains(text, "secret upstream detail") || strings.Contains(text, "Stream disconnected") {
				t.Fatalf("sanitized stream leaked upstream detail: %q", text)
			}
			for _, forbidden := range []string{"upstream", "newapi", "sub2api", "upstream_error"} {
				if strings.Contains(strings.ToLower(text), forbidden) {
					t.Fatalf("sanitized stream exposed implementation marker %q: %q", forbidden, text)
				}
			}
			if !strings.Contains(sanitizer.ErrorText(), "secret upstream detail") {
				t.Fatalf("admin error detail was not retained: %q", sanitizer.ErrorText())
			}
		})
	}
}

func TestGatewaySSEStreamSanitizerCapturesOnlyStructuredFailureEvidence(t *testing.T) {
	for _, tc := range []struct {
		name             string
		body             string
		invalidatesKey   bool
		modelUnsupported bool
	}{
		{
			name: "explicit authentication error",
			body: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n" +
				"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"authentication_error\"}}\n\n",
			invalidatesKey: true,
		},
		{
			name: "explicit forbidden status",
			body: "data: {\"delta\":\"partial\"}\n\n" +
				"event: error\ndata: {\"status\":403}\n\n",
			invalidatesKey: true,
		},
		{
			name: "explicit model not found",
			body: "data: {\"delta\":\"partial\"}\n\n" +
				"event: error\ndata: {\"code\":\"model_not_found\"}\n\n",
			modelUnsupported: true,
		},
		{
			name: "ordinary generated text",
			body: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"model_not_found and invalid_api_key are plain text\"}\n\n" +
				"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{
			name: "null error is not evidence",
			body: "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"error\":null},\"code\":\"model_not_found\"}\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer := newGatewaySSEStreamSanitizer(
				io.NopCloser(strings.NewReader(tc.body)),
				&gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit},
				"sk-private",
				"/v1/responses",
			)
			if _, err := io.ReadAll(sanitizer); err != nil {
				t.Fatalf("read sanitized stream: %v", err)
			}
			evidence := sanitizer.FailureEvidence()
			if evidence.InvalidatesKey != tc.invalidatesKey || evidence.ModelUnsupported != tc.modelUnsupported {
				t.Fatalf("failure evidence = %+v, want invalidates=%t model_unsupported=%t", evidence, tc.invalidatesKey, tc.modelUnsupported)
			}
		})
	}
}

func TestGatewaySSEStreamSanitizerTerminatesTransportErrorAfterLateAcceptance(t *testing.T) {
	acceptedFrame := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_late\"}}\n\n"
	transportErr := errors.New("wire exploded with secret provider detail")
	body := io.NopCloser(io.MultiReader(strings.NewReader(acceptedFrame), iotest.ErrReader(transportErr)))
	sanitizer := newGatewaySSEStreamSanitizer(body, &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", "/v1/responses")
	acceptedCalls := 0
	sanitizer.onAccepted = func() { acceptedCalls++ }
	got, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read accepted transport failure: %v", err)
	}
	text := string(got)
	if !strings.Contains(text, "resp_late") || !strings.Contains(text, "event: response.failed") || !strings.Contains(text, gatewayPublicStreamFailureMessage) {
		t.Fatalf("accepted transport failure output=%q", text)
	}
	if strings.Contains(text, "wire exploded") || strings.Contains(text, "secret provider detail") {
		t.Fatalf("accepted transport failure leaked upstream detail: %q", text)
	}
	if !sanitizer.accepted || acceptedCalls != 1 || !strings.Contains(sanitizer.ErrorText(), "wire exploded") {
		t.Fatalf("accepted transport failure state: accepted=%t calls=%d error=%q", sanitizer.accepted, acceptedCalls, sanitizer.ErrorText())
	}
}

func TestGatewaySSEStreamSanitizerLimitsCapacityNoticeToPreOutputCodexStreams(t *testing.T) {
	capacity := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"⚠ Selected model is at capacity. Please try a different model.\"}\n\n"
	terminal := "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_capacity\",\"status\":\"completed\",\"error\":null}}\n\n"
	for _, tc := range []struct {
		name   string
		body   string
		detect bool
	}{
		{name: "after real output", body: "data: {\"delta\":\"real output\"}\n\n" + capacity + terminal, detect: true},
		{name: "non codex responses", body: capacity + terminal, detect: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.body)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "", "/v1/responses")
			sanitizer.detectCapacityNotice = tc.detect
			got, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read stream: %v", err)
			}
			if sanitizer.ErrorText() != "" || !strings.Contains(string(got), "Selected model is at capacity") {
				t.Fatalf("capacity stream was incorrectly terminated: output=%q error=%q", got, sanitizer.ErrorText())
			}
		})
	}
}

func TestGatewaySSEStreamSanitizerSuppressesLateConfirmedInterruption(t *testing.T) {
	const interruption = "Our servers are currently overloaded. Please try again later."
	const prefix = "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_late_overload\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"item\":{\"id\":\"msg_late_overload\",\"type\":\"message\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_late_overload\",\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_late_overload\",\"delta\":\"partial answer\"}\n\n"
	const terminal = "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":6,\"response\":{\"id\":\"resp_late_overload\",\"status\":\"completed\",\"error\":null}}\n\n"
	for _, tc := range []struct {
		name   string
		frames string
	}{
		{
			name:   "single delta",
			frames: "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_late_overload\",\"delta\":\"" + interruption + "\"}\n\n",
		},
		{
			name: "split deltas",
			frames: "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_late_overload\",\"delta\":\"Our servers are currently over\"}\n\n" +
				"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":5,\"item_id\":\"msg_late_overload\",\"delta\":\"loaded. Please try again later.\"}\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(prefix+tc.frames+terminal)), nil, "", "/v1/responses")
			sanitizer.detectCapacityNotice = true
			got, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read sanitized interruption stream: %v", err)
			}
			text := string(got)
			if !strings.Contains(text, "partial answer") || (!strings.Contains(text, gatewayPublicRetryPromptMessage) && !strings.Contains(text, gatewayPublicStreamFailureMessage)) || (!strings.Contains(text, "event: response.completed") && !strings.Contains(text, "event: response.failed")) {
				t.Fatalf("sanitized interruption stream=%q", text)
			}
			if strings.Contains(text, interruption) || strings.Contains(text, "Our servers are currently over") {
				t.Fatalf("confirmed interruption escaped downstream: %q", text)
			}
			if !strings.Contains(sanitizer.ErrorText(), interruption) {
				t.Fatalf("administrator interruption detail was not retained: %q", sanitizer.ErrorText())
			}
		})
	}
}

func TestGatewayPublicErrorsDoNotExposeProviderImplementation(t *testing.T) {
	publicValues := []string{
		gatewayPublicFailureMessage,
		gatewayPublicStreamFailureMessage,
		string(gatewayPublicStreamErrorFrame([]byte("event: provider.failure\ndata: {}\n\n"), "/v1/chat/completions")),
		string(gatewayPublicStreamErrorFrame([]byte("event: response.failed\ndata: {}\n\n"), "/v1/responses")),
		string(gatewayPublicStreamErrorFrame([]byte("event: error\ndata: {}\n\n"), "/v1/messages")),
	}
	for _, value := range publicValues {
		lower := strings.ToLower(value)
		for _, forbidden := range []string{"upstream", "newapi", "sub2api", "upstream-ops"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("public error exposed %q: %q", forbidden, value)
			}
		}
	}
}

func TestGatewaySSEStreamSanitizerFailsClosedForOversizedFrame(t *testing.T) {
	secret := "secret error at oversized frame tail"
	body := "data: {\"padding\":\"" + strings.Repeat("x", gatewayStreamFrameLimit) + "\",\"error\":{\"message\":\"" + secret + "\"}}\n\n"
	raw := &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(body)), raw, "sk-private", "/v1/responses")
	got, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read sanitized oversized stream: %v", err)
	}
	text := string(got)
	if !strings.Contains(text, gatewayPublicStreamFailureMessage) {
		t.Fatalf("oversized stream did not return the fixed downstream error: %q", text)
	}
	for _, forbidden := range []string{secret, "padding", strings.Repeat("x", 64)} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("oversized stream leaked %q: %q", forbidden, text)
		}
	}
	if !strings.Contains(sanitizer.ErrorText(), "exceeded") || !strings.Contains(sanitizer.ErrorText(), "safety limit") {
		t.Fatalf("oversized stream did not retain an admin safety error: %q", sanitizer.ErrorText())
	}
	if evidence := sanitizer.FailureEvidence(); evidence.InvalidatesKey || evidence.ModelUnsupported {
		t.Fatalf("oversized incomplete frame produced failure evidence: %+v", evidence)
	}
}

func TestSanitizeGatewayProbeErrorRedactsKey(t *testing.T) {
	got := sanitizeGatewayProbeError([]byte(`{"error":"bad sk-secret-token Authorization: Bearer sk-secret-token"}`), "sk-secret-token")
	if strings.Contains(got, "sk-secret-token") {
		t.Fatalf("sanitized error leaked key: %q", got)
	}
	if !strings.Contains(got, "[redacted]") {
		t.Fatalf("sanitized error did not include redaction marker: %q", got)
	}
}

func TestGatewayResponseSnippetRedactsKey(t *testing.T) {
	got := gatewayResponseSnippet(strings.NewReader(`upstream echoed sk-secret-token`), "sk-secret-token")
	if strings.Contains(got, "sk-secret-token") {
		t.Fatalf("snippet leaked key: %q", got)
	}
}

func TestGatewayFirstByteReadCloserRecordsFirstBodyData(t *testing.T) {
	tracker := gatewayFirstByteTracker{startedAt: time.Now().Add(-time.Millisecond)}
	reader := &gatewayFirstByteReadCloser{
		ReadCloser: io.NopCloser(strings.NewReader("data: first token\n\n")),
		tracker:    &tracker,
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("read body: %v", err)
	}
	if tracker.LatencyMs() <= 0 {
		t.Fatalf("first body data was not timed: %+v", tracker)
	}
}

func TestParseGatewayUsageDoesNotInventCostFromTokens(t *testing.T) {
	metrics := parseGatewayUsageMetrics([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}`))
	if metrics.TotalTokens != 30 {
		t.Fatalf("total tokens = %d, want 30", metrics.TotalTokens)
	}
	if metrics.Cost != 0 || metrics.CostEstimated || metrics.CostSource != "" {
		t.Fatalf("unexpected invented cost: %+v", metrics)
	}
	if !metrics.InputTokensPresent || !metrics.OutputTokensPresent || metrics.CacheCreationTokensPresent || metrics.CacheReadTokensPresent {
		t.Fatalf("usage parser did not preserve missing cache components: %+v", metrics)
	}
}

func TestParseGatewayUsagePresenceDistinguishesPartialAndFallbackObjects(t *testing.T) {
	tests := []struct {
		name        string
		payload     string
		wantInput   bool
		wantOutput  bool
		wantWrite   bool
		wantRead    bool
		wantInvalid bool
	}{
		{
			name:       "explicit zero components",
			payload:    `{"usage":{"input_tokens":0,"output_tokens":0,"cache_write_tokens":0,"cached_tokens":0}}`,
			wantInput:  true,
			wantOutput: true,
			wantWrite:  true,
			wantRead:   true,
		},
		{
			name:      "partial usage object",
			payload:   `{"usage":{"input_tokens":0}}`,
			wantInput: true,
		},
		{
			name:       "omitted cache components remain unknown",
			payload:    `{"usage":{"input_tokens":12,"output_tokens":3}}`,
			wantInput:  true,
			wantOutput: true,
		},
		{
			name:        "invalid cache components remain unknown",
			payload:     `{"usage":{"input_tokens":12,"output_tokens":3,"cache_write_tokens":null,"cached_tokens":-1}}`,
			wantInput:   true,
			wantOutput:  true,
			wantInvalid: true,
		},
		{
			name:        "fractional tokens are invalid",
			payload:     `{"usage":{"input_tokens":1.5,"output_tokens":"2.25","cache_write_tokens":0.5,"cached_tokens":"3.75"}}`,
			wantInvalid: true,
		},
		{
			name:        "overflow tokens are invalid",
			payload:     `{"usage":{"input_tokens":9223372036854775808,"output_tokens":"9223372036854775808","cache_write_tokens":1e30,"cached_tokens":"1e30"}}`,
			wantInvalid: true,
		},
		{
			name:       "top-level compatibility fields are not a complete usage object",
			payload:    `{"input_tokens":0,"output_tokens":0}`,
			wantInput:  true,
			wantOutput: true,
		},
		{
			name:    "no usage fields",
			payload: `{"response":{"id":"resp_without_usage"}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := parseGatewayUsageMetrics([]byte(test.payload))
			if metrics.InputTokensPresent != test.wantInput || metrics.OutputTokensPresent != test.wantOutput ||
				metrics.CacheCreationTokensPresent != test.wantWrite || metrics.CacheReadTokensPresent != test.wantRead ||
				metrics.UsageInvalid != test.wantInvalid {
				t.Fatalf("usage presence = input:%t output:%t write:%t read:%t, want input:%t output:%t write:%t read:%t; metrics=%+v",
					metrics.InputTokensPresent, metrics.OutputTokensPresent, metrics.CacheCreationTokensPresent, metrics.CacheReadTokensPresent,
					test.wantInput, test.wantOutput, test.wantWrite, test.wantRead, metrics)
			}
		})
	}
}

func TestParseGatewayUsageInvalidAliasCannotBeMaskedByValidAlias(t *testing.T) {
	tests := []struct {
		name              string
		payload           string
		wantCacheCreation int64
		wantCacheRead     int64
	}{
		{name: "input", payload: `{"usage":{"prompt_tokens":null,"input_tokens":12,"output_tokens":3,"total_tokens":15}}`},
		{name: "output", payload: `{"usage":{"input_tokens":12,"completion_tokens":-1,"output_tokens":3,"total_tokens":15}}`},
		{name: "cache creation", payload: `{"usage":{"input_tokens":12,"output_tokens":3,"cache_creation_tokens":false,"cache_write_tokens":4,"total_tokens":15}}`, wantCacheCreation: 4},
		{name: "cache read", payload: `{"usage":{"input_tokens":12,"output_tokens":3,"cache_read_tokens":"invalid","cached_tokens":5,"total_tokens":15}}`, wantCacheRead: 5},
		{name: "cache details", payload: `{"usage":{"input_tokens":12,"output_tokens":3,"cache_write_tokens":4,"input_tokens_details":{"cache_creation_tokens":false},"total_tokens":15}}`, wantCacheCreation: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := parseGatewayUsageMetricsForPath([]byte(test.payload), "/v1/responses")
			if metrics.PromptTokens != 12 || metrics.CompletionTokens != 3 || metrics.CacheCreationTokens != test.wantCacheCreation || metrics.CacheReadTokens != test.wantCacheRead {
				t.Fatalf("valid token aliases were not retained: %+v", metrics)
			}
			if !metrics.InputTokensPresent || !metrics.OutputTokensPresent ||
				metrics.CacheCreationTokensPresent != (test.wantCacheCreation > 0) || metrics.CacheReadTokensPresent != (test.wantCacheRead > 0) {
				t.Fatalf("valid token aliases lost presence: %+v", metrics)
			}
			if !metrics.UsageInvalid {
				t.Fatalf("valid token alias masked an invalid alias: %+v", metrics)
			}
		})
	}
}

func TestParseGatewayUsageRejectsAggregateTokenOverflow(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		payload string
	}{
		{
			name:    "anthropic input plus cache",
			path:    "/v1/messages",
			payload: `{"usage":{"input_tokens":9223372036854775807,"cache_read_input_tokens":1,"cache_creation_input_tokens":0,"output_tokens":0}}`,
		},
		{
			name:    "derived total",
			path:    "/v1/responses",
			payload: `{"usage":{"input_tokens":9223372036854775807,"output_tokens":1,"cache_read_tokens":0,"cache_write_tokens":0}}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics := parseGatewayUsageMetricsForPath([]byte(test.payload), test.path)
			if !metrics.UsageInvalid {
				t.Fatalf("overflowing usage was accepted: %+v", metrics)
			}
			if metrics.PromptTokens < 0 || metrics.CompletionTokens < 0 || metrics.TotalTokens < 0 {
				t.Fatalf("overflowing usage wrapped negative: %+v", metrics)
			}
		})
	}
}

func TestParseGatewayUsageExplicitCost(t *testing.T) {
	metrics := parseGatewayUsageMetrics([]byte(`{"usage":{"input_tokens":10,"output_tokens":20,"cost":0.000123}}`))
	if metrics.PromptTokens != 10 || metrics.CompletionTokens != 20 || metrics.TotalTokens != 30 {
		t.Fatalf("unexpected token metrics: %+v", metrics)
	}
	if metrics.Cost != 0.000123 || !metrics.CostObserved || metrics.CostEstimated || metrics.CostSource != "cost" {
		t.Fatalf("unexpected cost metrics: %+v", metrics)
	}
}

func TestGatewayRequestReasoningEffort(t *testing.T) {
	tests := map[string]string{
		`{"model":"gpt-test","reasoning_effort":"high"}`:            "high",
		`{"model":"gpt-test","reasoning":{"effort":"medium"}}`:      "medium",
		`{"model":"claude-test","thinking":{"budget_tokens":1024}}`: "1024",
		`{"model":"gpt-test","reasoningEffort":"minimal"}`:          "minimal",
		`{"model":"gpt-test","reasoning":{"effort":"   low   "}}`:   "low",
		`{"model":"gpt-test"}`:                                      "",
	}
	for raw, want := range tests {
		if got := gatewayRequestReasoningEffort([]byte(raw)); got != want {
			t.Fatalf("reasoning effort for %s = %q, want %q", raw, got, want)
		}
	}
}

func TestParseGatewayUsageExplicitZeroCostBlocksEstimate(t *testing.T) {
	metrics := parseGatewayUsageMetrics([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30,"cost":0}}`))
	metrics = estimateGatewayCost(metrics, 10, 10)
	if metrics.Cost != 0 || !metrics.CostObserved || metrics.CostEstimated || metrics.CostSource != "cost" {
		t.Fatalf("unexpected explicit zero cost metrics: %+v", metrics)
	}
}

func TestParseGatewayUsageNestedResponse(t *testing.T) {
	metrics := parseGatewayUsageMetrics([]byte(`{"response":{"id":"resp_123","usage":{"input_tokens":12,"output_tokens":34}}}`))
	if metrics.PromptTokens != 12 || metrics.CompletionTokens != 34 || metrics.TotalTokens != 46 {
		t.Fatalf("unexpected nested response metrics: %+v", metrics)
	}
}

func TestParseGatewayUsageCacheTokens(t *testing.T) {
	metrics := parseGatewayUsageMetricsForPath([]byte(`{"usage":{"input_tokens":10,"cache_creation_input_tokens":3,"cache_creation_tokens":3,"cache_write_input_tokens":2,"cache_write_tokens":3,"cached_creation_tokens":1,"cache_read_input_tokens":7,"cache_read_tokens":7,"cached_tokens":6,"output_tokens":20}}`), "/v1/messages")
	if metrics.PromptTokens != 20 || metrics.CacheCreationTokens != 3 || metrics.CacheReadTokens != 7 || metrics.TotalTokens != 40 {
		t.Fatalf("unexpected claude cache metrics: %+v", metrics)
	}
	openai := parseGatewayUsageMetricsForPath([]byte(`{"usage":{"prompt_tokens":50,"input_tokens":48,"completion_tokens":20,"output_tokens":18,"cache_read_tokens":30,"prompt_tokens_details":{"cached_tokens":30}}}`), "/v1/chat/completions")
	if openai.PromptTokens != 50 || openai.CompletionTokens != 20 || openai.CacheReadTokens != 30 || openai.TotalTokens != 70 {
		t.Fatalf("unexpected openai cache metrics: %+v", openai)
	}
}

func TestParseGatewayUsageOpenAIResponsesCacheDetails(t *testing.T) {
	metrics := parseGatewayUsageMetricsForPath([]byte(`{"response":{"id":"resp_123","usage":{"input_tokens":50,"output_tokens":20,"cache_creation_input_tokens":5,"cache_read_tokens":25,"input_tokens_details":{"cached_tokens":30,"cache_write_tokens":5,"cache_creation_tokens":4}}}}`), "/v1/responses")
	if metrics.PromptTokens != 50 || metrics.CompletionTokens != 20 || metrics.CacheCreationTokens != 5 || metrics.CacheReadTokens != 30 || metrics.TotalTokens != 70 {
		t.Fatalf("unexpected responses cache metrics: %+v", metrics)
	}

	topLevelAliases := parseGatewayUsageMetricsForPath([]byte(`{"usage":{"input_tokens":20,"output_tokens":2,"cache_creation_input_tokens":5,"cache_write_input_tokens":6,"cache_read_input_tokens":3,"cache_read_tokens":4}}`), "/v1/responses")
	if topLevelAliases.PromptTokens != 20 || topLevelAliases.CacheCreationTokens != 6 || topLevelAliases.CacheReadTokens != 4 || topLevelAliases.TotalTokens != 22 {
		t.Fatalf("unexpected responses top-level cache metrics: %+v", topLevelAliases)
	}
}

func TestParseGatewayStreamUsageMetrics(t *testing.T) {
	raw := []byte("event: response.created\n" +
		"data: {\"response\":{\"id\":\"resp_123\"}}\n\n" +
		"event: response.completed\n" +
		"data: {\"response\":{\"usage\":{\"input_tokens\":21,\"output_tokens\":9,\"input_tokens_details\":{\"cached_tokens\":8}}}}\n\n" +
		"data: [DONE]\n")
	metrics := parseGatewayStreamUsageMetricsForPath(raw, "/v1/responses")
	if metrics.PromptTokens != 21 || metrics.CompletionTokens != 9 || metrics.CacheReadTokens != 8 || metrics.TotalTokens != 30 {
		t.Fatalf("unexpected stream metrics: %+v", metrics)
	}
}

func TestParseGatewayStreamUsageMetricsMultilineData(t *testing.T) {
	raw := []byte("event: response.completed\n" +
		"data: {\"response\":{\"usage\":\n" +
		"data: {\"input_tokens\":21,\"output_tokens\":9}}}\n\n" +
		"data: [DONE]\n")
	metrics := parseGatewayStreamUsageMetrics(raw)
	if metrics.PromptTokens != 21 || metrics.CompletionTokens != 9 || metrics.TotalTokens != 30 {
		t.Fatalf("unexpected multiline stream metrics: %+v", metrics)
	}
}

func TestParseGatewayStreamUsageMergePreservesExplicitZeroPresence(t *testing.T) {
	raw := []byte("data: {\"usage\":{\"input_tokens\":12,\"cached_tokens\":0}}\n\n" +
		"data: {\"usage\":{\"output_tokens\":0,\"cache_write_tokens\":0}}\n\n")
	metrics := parseGatewayStreamUsageMetricsForPath(raw, "/v1/responses")
	if metrics.PromptTokens != 12 || metrics.CompletionTokens != 0 || metrics.CacheReadTokens != 0 || metrics.CacheCreationTokens != 0 {
		t.Fatalf("unexpected merged stream values: %+v", metrics)
	}
	if !metrics.InputTokensPresent || !metrics.OutputTokensPresent || !metrics.CacheCreationTokensPresent || !metrics.CacheReadTokensPresent {
		t.Fatalf("merged stream lost explicit zero component presence: %+v", metrics)
	}
}

func TestParseGatewayStreamUsageMergePreservesOmittedCacheComponentsAcrossUsageFrames(t *testing.T) {
	raw := []byte("data: {\"usage\":{\"input_tokens\":12}}\n\n" +
		"data: {\"usage\":{\"output_tokens\":0}}\n\n")
	metrics := parseGatewayStreamUsageMetricsForPath(raw, "/v1/responses")
	if metrics.PromptTokens != 12 || metrics.CompletionTokens != 0 || metrics.CacheReadTokens != 0 || metrics.CacheCreationTokens != 0 {
		t.Fatalf("unexpected merged stream values: %+v", metrics)
	}
	if !metrics.InputTokensPresent || !metrics.OutputTokensPresent || metrics.CacheCreationTokensPresent || metrics.CacheReadTokensPresent {
		t.Fatalf("merged stream invented omitted cache components: %+v", metrics)
	}
}

func TestParseGatewayStreamUsageInvalidAliasRemainsStickyAcrossFrames(t *testing.T) {
	raw := []byte("data: {\"usage\":{\"prompt_tokens\":null,\"input_tokens\":12}}\n\n" +
		"data: {\"usage\":{\"input_tokens\":15,\"output_tokens\":3}}\n\n")
	metrics := parseGatewayStreamUsageMetricsForPath(raw, "/v1/responses")
	if metrics.PromptTokens != 15 || metrics.CompletionTokens != 3 || !metrics.InputTokensPresent || !metrics.OutputTokensPresent {
		t.Fatalf("unexpected merged stream usage: %+v", metrics)
	}
	if !metrics.UsageInvalid {
		t.Fatalf("later valid stream usage cleared an invalid alias: %+v", metrics)
	}
}

func TestGatewayTailBufferKeepsTail(t *testing.T) {
	var buf gatewayTailBuffer
	buf.limit = 8
	if _, err := buf.Write([]byte("12345")); err != nil {
		t.Fatalf("write first chunk: %v", err)
	}
	if _, err := buf.Write([]byte("67890")); err != nil {
		t.Fatalf("write second chunk: %v", err)
	}
	if got := string(buf.Bytes()); got != "34567890" {
		t.Fatalf("tail buffer = %q, want %q", got, "34567890")
	}
}

func TestEstimateGatewayCostFromGroupRatio(t *testing.T) {
	metrics := estimateGatewayCost(gatewayUsageMetrics{
		PromptTokens:     100,
		CompletionTokens: 50,
		TotalTokens:      150,
	}, 2, 6)
	if metrics.Cost != 0.001 || !metrics.CostEstimated || metrics.CostSource != "group_ratio_default_quota" {
		t.Fatalf("unexpected estimated cost metrics: %+v", metrics)
	}
}

func TestEstimateGatewayCostIncludesCacheTokensInLegacyFallback(t *testing.T) {
	metrics := estimateGatewayCost(gatewayUsageMetrics{
		PromptTokens:        100,
		CacheCreationTokens: 20,
		CacheReadTokens:     60,
		CompletionTokens:    10,
		TotalTokens:         110,
	}, 2, 4)
	want := (20*2 + 80*2*gatewayFallbackCacheInputPriceMultiplier + 10*4) / float64(gatewayDefaultQuotaPerUSD)
	if metrics.Cost != want || !metrics.CostEstimated || metrics.CostSource != "group_ratio_default_quota" {
		t.Fatalf("cache tokens must be included in legacy estimate: got %+v, want cost %v", metrics, want)
	}
}

func TestEstimateGatewayCostClampsInvalidTokenMetricsInLegacyFallback(t *testing.T) {
	metrics := estimateGatewayCost(gatewayUsageMetrics{
		PromptTokens:        100,
		CacheCreationTokens: -20,
		CacheReadTokens:     120,
		CompletionTokens:    -10,
		TotalTokens:         90,
	}, 2, 4)
	want := (120 * 2 * gatewayFallbackCacheInputPriceMultiplier) / float64(gatewayDefaultQuotaPerUSD)
	if metrics.Cost != want || !metrics.CostEstimated || metrics.CostSource != "group_ratio_default_quota" {
		t.Fatalf("invalid token metrics must use the same non-negative fallback as the UI: got %+v, want cost %v", metrics, want)
	}
}

func TestEstimateGatewayCostUsesNormalizedTokenAvailability(t *testing.T) {
	metrics := estimateGatewayCost(gatewayUsageMetrics{PromptTokens: 100}, 2, 4)
	want := float64(100*2) / gatewayDefaultQuotaPerUSD
	if metrics.Cost != want || !metrics.CostEstimated {
		t.Fatalf("component tokens must be sufficient without total_tokens: got %+v, want cost %v", metrics, want)
	}

	metrics = estimateGatewayCost(gatewayUsageMetrics{
		PromptTokens:        -100,
		CacheCreationTokens: -20,
		CacheReadTokens:     -30,
		CompletionTokens:    -10,
		TotalTokens:         100,
	}, 2, 4)
	if metrics.Cost != want || !metrics.CostEstimated {
		t.Fatalf("invalid component tokens must fall back to normalized total_tokens: got %+v, want cost %v", metrics, want)
	}
}

func TestEstimateGatewayCostRejectsNonFiniteInputRatio(t *testing.T) {
	metrics := estimateGatewayCost(gatewayUsageMetrics{PromptTokens: 100, TotalTokens: 100}, math.NaN(), 2)
	if metrics.CostObserved || metrics.Cost != 0 {
		t.Fatalf("non-finite input ratio must not create an estimate: %+v", metrics)
	}

	metrics = estimateGatewayCost(gatewayUsageMetrics{CompletionTokens: 100, TotalTokens: 100}, 2, math.Inf(1))
	want := float64(100*2) / gatewayDefaultQuotaPerUSD
	if metrics.Cost != want || !metrics.CostEstimated {
		t.Fatalf("non-finite completion ratio must fall back to input ratio: got %+v, want cost %v", metrics, want)
	}

	metrics = estimateGatewayCost(gatewayUsageMetrics{PromptTokens: 100, TotalTokens: 100}, math.MaxFloat64, 2)
	if metrics.CostObserved || metrics.Cost != 0 {
		t.Fatalf("overflowing finite ratio must not create an estimate: %+v", metrics)
	}

	metrics = estimateGatewayCost(gatewayUsageMetrics{PromptTokens: 1, TotalTokens: 1}, math.SmallestNonzeroFloat64, 2)
	if metrics.CostObserved || metrics.Cost != 0 {
		t.Fatalf("underflowing finite ratio must not create a zero estimate: %+v", metrics)
	}
}

func TestMergeGatewayUsageMetricsPrefersActualCost(t *testing.T) {
	estimated := gatewayUsageMetrics{Cost: 0.5, CostObserved: true, CostEstimated: true, CostSource: "estimated_cost"}
	actual := gatewayUsageMetrics{Cost: 0.1, CostObserved: true, CostSource: "cost"}
	metrics := mergeGatewayUsageMetrics(estimated, actual)
	if metrics.Cost != 0.1 || metrics.CostEstimated || metrics.CostSource != "cost" {
		t.Fatalf("actual cost should win over estimated cost: %+v", metrics)
	}

	metrics = mergeGatewayUsageMetrics(actual, estimated)
	if metrics.Cost != 0.1 || metrics.CostEstimated || metrics.CostSource != "cost" {
		t.Fatalf("estimated cost should not overwrite actual cost: %+v", metrics)
	}
}

func TestApplyGatewayRequestPricingUsageKeepsOperationSemantics(t *testing.T) {
	metrics := gatewayUsageMetrics{
		PromptTokens: 100, CompletionTokens: 20, CacheCreationTokens: 10, CacheReadTokens: 30,
		InputTokensPresent: true, OutputTokensPresent: true, CacheCreationTokensPresent: true, CacheReadTokensPresent: true,
	}
	textLog := &storage.GatewayRequestLog{Path: "/v1/responses", TotalTokens: 120}
	applyGatewayRequestPricingUsage(textLog, metrics, false)
	if textLog.PromptTokens != 100 || textLog.CompletionTokens != 20 || textLog.CacheCreationTokens != 10 || textLog.CacheReadTokens != 30 ||
		!textLog.UsageInputPresent || !textLog.UsageOutputPresent || !textLog.UsageCacheWritePresent || !textLog.UsageCacheReadPresent {
		t.Fatalf("text pricing usage = %+v", textLog)
	}

	embeddingLog := &storage.GatewayRequestLog{
		Path: "/v1/embeddings", PromptTokens: 100, CompletionTokens: 20,
		CacheCreationTokens: 10, CacheReadTokens: 30, TotalTokens: 120,
	}
	applyGatewayRequestPricingUsage(embeddingLog, metrics, false)
	if embeddingLog.PromptTokens != 100 || embeddingLog.CompletionTokens != 0 || embeddingLog.CacheCreationTokens != 0 || embeddingLog.CacheReadTokens != 0 ||
		embeddingLog.TotalTokens != 0 || !embeddingLog.UsageInputPresent || embeddingLog.UsageOutputPresent ||
		embeddingLog.UsageCacheWritePresent || embeddingLog.UsageCacheReadPresent {
		t.Fatalf("embedding pricing usage = %+v", embeddingLog)
	}

	operationUnits := int64(1)
	imageLog := &storage.GatewayRequestLog{
		Path: "/v1/responses", UsageOperationPresent: true, OperationUnits: &operationUnits,
	}
	applyGatewayRequestPricingUsage(imageLog, metrics, true)
	if !imageLog.UsageOperationPresent || imageLog.OperationUnits == nil || *imageLog.OperationUnits != 1 ||
		imageLog.UsageInputPresent || imageLog.UsageOutputPresent || imageLog.UsageCacheWritePresent || imageLog.UsageCacheReadPresent {
		t.Fatalf("image operation usage was overwritten: %+v", imageLog)
	}
}

func TestApplyGatewayRequestPricingUsageRejectsImpossibleCacheTuple(t *testing.T) {
	metrics := gatewayUsageMetrics{
		PromptTokens: 10, CompletionTokens: 2, CacheCreationTokens: 7, CacheReadTokens: 5, TotalTokens: 12,
		InputTokensPresent: true, OutputTokensPresent: true, CacheCreationTokensPresent: true, CacheReadTokensPresent: true,
	}
	log := &storage.GatewayRequestLog{
		Path: "/v1/responses", Cost: 99, RawCost: numberPointer(99), CostEstimated: true, CostSource: "estimated_cost",
	}
	applyGatewayRequestPricingUsage(log, metrics, false)
	if log.CostStatus != storage.GatewayCostStatusUnavailable || log.CostUnavailableReason != storage.GatewayPricingUnavailableUsageInvalid ||
		log.Cost != 0 || log.RawCost != nil || log.CostEstimated || log.CostSource != "" {
		t.Fatalf("impossible cache tuple did not fail closed: %+v", log)
	}
	if log.PromptTokens != 10 || log.CompletionTokens != 2 || log.CacheCreationTokens != 7 || log.CacheReadTokens != 5 || log.TotalTokens != 12 ||
		!log.UsageInputPresent || !log.UsageOutputPresent || !log.UsageCacheReadPresent || !log.UsageCacheWritePresent {
		t.Fatalf("impossible cache tuple lost audit evidence: %+v", log)
	}
}

func TestApplyGatewayRequestUsageAndCostPreservesFailureFacts(t *testing.T) {
	metrics := parseGatewayUsageMetricsForPath([]byte(`{"usage":{"input_tokens":20,"output_tokens":4,"cache_read_tokens":5,"cache_write_tokens":0,"total_tokens":24,"actual_cost":0.25}}`), "/v1/responses")
	log := &storage.GatewayRequestLog{Path: "/v1/responses"}
	applyGatewayRequestUsageAndCost(log, metrics, false, 0.5)
	if log.PromptTokens != 20 || log.CompletionTokens != 4 || log.CacheReadTokens != 5 || log.CacheCreationTokens != 0 || log.TotalTokens != 24 {
		t.Fatalf("terminal usage facts were not copied: %+v", log)
	}
	if log.Cost != 0.125 || log.RawCost == nil || *log.RawCost != 0.25 || log.CostEstimated || log.CostSource != "actual_cost" {
		t.Fatalf("terminal cost facts were not copied: %+v", log)
	}
	if !log.UsageInputPresent || !log.UsageOutputPresent || !log.UsageCacheReadPresent || !log.UsageCacheWritePresent {
		t.Fatalf("terminal usage presence was not copied: %+v", log)
	}
}

func TestApplyGatewayRequestUsageAndCostKeepsActualCostWhenUsageInvalid(t *testing.T) {
	metrics := parseGatewayUsageMetricsForPath([]byte(`{"usage":{"input_tokens":10,"output_tokens":2,"cache_read_tokens":20,"cache_write_tokens":0,"total_tokens":12,"actual_cost":0.25}}`), "/v1/responses")
	log := &storage.GatewayRequestLog{Path: "/v1/responses"}
	applyGatewayRequestUsageAndCost(log, metrics, false, 1)
	if log.Cost != 0.25 || log.RawCost == nil || *log.RawCost != 0.25 || log.CostSource != "actual_cost" || log.CostUnavailableReason != "" {
		t.Fatalf("actual cost was discarded with invalid usage: %+v", log)
	}
}

func TestApplyGatewayRequestPricingUsageKeepsMissingCacheComponentsUnknown(t *testing.T) {
	metrics := parseGatewayUsageMetricsForPath(
		[]byte(`{"usage":{"input_tokens":100,"output_tokens":20}}`),
		"/v1/responses",
	)
	log := &storage.GatewayRequestLog{Path: "/v1/responses"}
	applyGatewayRequestPricingUsage(log, metrics, false)
	if !log.UsageInputPresent || !log.UsageOutputPresent || log.UsageCacheReadPresent || log.UsageCacheWritePresent {
		t.Fatalf("missing cache usage was not preserved: %+v", log)
	}

	inputPrice, outputPrice, cacheReadPrice, cacheWritePrice := 5.0, 30.0, 0.5, 6.25
	estimate := storage.EvaluateGatewayRequestPriceFact(&storage.GatewayRequestPriceFact{
		SchemaVersion:       storage.GatewayRequestPriceFactSchemaVersion,
		Currency:            "USD",
		Unit:                storage.GatewayPricingUnitPerMillionTokens,
		Operation:           storage.GatewayPricingOperationTextGeneration,
		Protocol:            storage.GatewayPricingProtocolOpenAIResponses,
		InputUnitPrice:      &inputPrice,
		OutputUnitPrice:     &outputPrice,
		CacheReadUnitPrice:  &cacheReadPrice,
		CacheWriteUnitPrice: &cacheWritePrice,
	}, storage.GatewayRequestPricingUsage{
		InputTokens:  &log.PromptTokens,
		OutputTokens: &log.CompletionTokens,
	})
	if estimate.Cost != nil || estimate.Status != storage.GatewayCostStatusUnavailable || estimate.UnavailableReason != storage.GatewayPricingUnavailableUsageIncomplete {
		t.Fatalf("incomplete cache usage produced a precise estimate: %+v", estimate)
	}
}

func TestListGatewayRequestLogsReportsPerformanceWindowAndConcurrencyScope(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	store := storage.NewGateway(db)
	base := time.Now().Add(-time.Second)
	for i := 0; i < 5; i++ {
		if err := store.AppendRequestLog(&storage.GatewayRequestLog{
			DownstreamKeyID:  1,
			ChannelID:        2,
			ModelName:        "gpt-test",
			Method:           "POST",
			Path:             "/v1/responses",
			StatusCode:       http.StatusOK,
			PromptTokens:     10,
			CompletionTokens: 5,
			TotalTokens:      15,
			CreatedAt:        base.Add(-time.Duration(i) * 20 * time.Second),
		}); err != nil {
			t.Fatalf("append log: %v", err)
		}
	}
	gatewayInFlightRequests.Store(3)
	t.Cleanup(func() { gatewayInFlightRequests.Store(0) })
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/gateway/request-logs?page=1&page_size=20", nil)
	listGatewayRequestLogs(ctx, &Deps{Gateway: gateway.NewService(nil, nil, store, nil, nil, nil)})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data struct {
			Summary storage.GatewayRequestLogSummary `json:"summary"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	summary := response.Data.Summary
	if summary.CurrentConcurrency != 3 || summary.CurrentConcurrencyScope != "instance" {
		t.Fatalf("concurrency = %d/%q, want 3/instance", summary.CurrentConcurrency, summary.CurrentConcurrencyScope)
	}
	if summary.PerformanceWindowSeconds != 300 || summary.PerformanceRequests != 5 || summary.RPM != 1 || summary.TPM != 15 {
		t.Fatalf("performance summary = %+v", summary)
	}
}

func TestGatewayProbeConfigEndpointUsesWhitelistedDTO(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "private channel", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: "https://private.example", Username: "private-user", PasswordCipher: "private-password",
		MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "private-group", Ratio: 0.1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "private-group", APIKeyCipher: "private-cipher",
		APIKeyPrefix: "private-prefix", ModelFamilies: "codex", ModelIDs: "gpt-mini", Enabled: true,
		LastError: `request https://private.example failed for private-user with private-prefix password=short-pass body={"token":"json-short"}`,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/gateway/probe-config", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	for _, forbidden := range []string{
		"site_url", "username", "password_cipher", "api_key_cipher", "api_key_prefix",
		"https://private.example", "private-user", "private-password", "private-cipher", "private-prefix", "short-pass", "json-short",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("probe config leaked %q: %s", forbidden, body)
		}
	}
	var payload struct {
		Data []gateway.ProbeConfigChannel `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode probe config: %v", err)
	}
	if len(payload.Data) != 1 || len(payload.Data[0].Groups) != 1 || !payload.Data[0].Groups[0].ProbeEnabled || !payload.Data[0].Groups[0].ProbeEligible {
		t.Fatalf("probe config payload = %+v", payload)
	}
}

func TestGatewayProbeGroupEndpointUpdatesConfigAndRoutePreview(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "group probe", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: "https://probe.example", PasswordCipher: "secret", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "group", Ratio: 0.1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "group", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "cipher", ModelFamilies: "codex", ModelIDs: "gpt-mini", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "group")
	if err != nil {
		t.Fatalf("capture upstream group generation: %v", err)
	}
	if err := store.UpdateUpstreamKeyModelFamiliesForIdentity(
		storage.NewGatewayModelFactFence(generation, channel.UpstreamAccountIdentityRevision),
		"codex", "gpt-mini", time.Now(), "",
	); err != nil {
		t.Fatalf("persist upstream model facts: %v", err)
	}
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)})

	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		router.ServeHTTP(recorder, req)
		return recorder
	}
	updated := request(http.MethodPatch, "/api/gateway/probe-group", fmt.Sprintf(`{"channel_id":%d,"group_name":" group ","enabled":false}`, channel.ID))
	if updated.Code != http.StatusOK {
		t.Fatalf("disable group status=%d body=%s", updated.Code, updated.Body.String())
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "group")
	if err != nil || stored.LatencyProbeEnabled {
		t.Fatalf("stored group probe = %+v err=%v, want false", stored, err)
	}

	configResponse := request(http.MethodGet, "/api/gateway/probe-config", "")
	var configPayload struct {
		Data []gateway.ProbeConfigChannel `json:"data"`
	}
	if configResponse.Code != http.StatusOK || json.Unmarshal(configResponse.Body.Bytes(), &configPayload) != nil {
		t.Fatalf("probe config status=%d body=%s", configResponse.Code, configResponse.Body.String())
	}
	if len(configPayload.Data) != 1 || len(configPayload.Data[0].Groups) != 1 || configPayload.Data[0].Groups[0].ProbeEnabled || !strings.Contains(strings.Join(configPayload.Data[0].Groups[0].ProbeBlockers, ","), "probe_disabled") {
		t.Fatalf("probe config after disable = %+v", configPayload.Data)
	}

	previewResponse := request(http.MethodGet, "/api/gateway/route-preview?app=codex&model=gpt-mini", "")
	var previewPayload struct {
		Data []gatewayRouteCandidateOutput `json:"data"`
	}
	if previewResponse.Code != http.StatusOK || json.Unmarshal(previewResponse.Body.Bytes(), &previewPayload) != nil {
		t.Fatalf("route preview status=%d body=%s", previewResponse.Code, previewResponse.Body.String())
	}
	if len(previewPayload.Data) != 1 || !previewPayload.Data[0].ProbeDisabled || previewPayload.Data[0].ProbeEnabled {
		t.Fatalf("route preview after disable = %+v", previewPayload.Data)
	}
	missing := request(http.MethodPatch, "/api/gateway/probe-group", `{"channel_id":999,"group_name":"missing","enabled":false}`)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing group status=%d body=%s", missing.Code, missing.Body.String())
	}
}

func TestGatewayManualRoutePreviewRetainsUnavailableMembers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "temporarily unavailable", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: "https://unavailable.example", PasswordCipher: "secret", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "group", Ratio: 0.1, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "group", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "cipher", ModelFamilies: "codex", ModelIDs: "gpt-mini", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	group := &storage.GatewayCandidateGroup{Name: "manual choices"}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "group"}}); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	key := &storage.GatewayDownstreamKey{
		Name: "manual client", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
		KeyHash: "manual-preview", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	if err := channels.MarkSyncFailure(channel.ID, "temporarily unavailable"); err != nil {
		t.Fatalf("mark channel unavailable: %v", err)
	}
	healthy := &storage.Channel{
		Name: "healthy", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: "https://healthy.example", PasswordCipher: "secret", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(healthy); err != nil {
		t.Fatalf("create healthy channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: healthy.ID, ModelName: "healthy-group", Ratio: 0.2, LastSeenAt: time.Now()}); err != nil {
		t.Fatalf("upsert healthy rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: healthy.ID, GroupName: "healthy-group", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "cipher", ModelFamilies: "codex", ModelIDs: "gpt-mini", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert healthy key: %v", err)
	}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{
		{ChannelID: channel.ID, GroupName: "group"},
		{ChannelID: healthy.ID, GroupName: "healthy-group"},
	}); err != nil {
		t.Fatalf("add healthy candidate: %v", err)
	}

	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/gateway/route-preview?downstream_key_id=%d&manual_selection=1", key.ID), nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Data []gatewayRouteCandidateOutput `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Data) != 2 {
		t.Fatalf("manual route preview = %+v", payload.Data)
	}
	foundUnavailable, foundHealthy := false, false
	for _, candidate := range payload.Data {
		switch candidate.ChannelID {
		case channel.ID:
			foundUnavailable = candidate.RoutingAvailable != nil && !*candidate.RoutingAvailable && strings.Contains(strings.Join(candidate.AvailabilityReasons, ","), "channel_error")
		case healthy.ID:
			foundHealthy = candidate.RoutingAvailable != nil && *candidate.RoutingAvailable && len(candidate.AvailabilityReasons) == 0
		}
	}
	if !foundUnavailable || !foundHealthy {
		t.Fatalf("manual route availability = %+v", payload.Data)
	}
}

func TestGatewayCandidateGroupEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "candidate", Type: storage.ChannelTypeNewAPI, SiteURL: "https://candidate.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "cheap", Ratio: 0.5}); err != nil {
		t.Fatalf("create rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "cheap", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "cipher", Enabled: true,
	}); err != nil {
		t.Fatalf("create upstream key: %v", err)
	}
	cheapGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "cheap")
	if err != nil {
		t.Fatalf("capture cheap generation: %v", err)
	}
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)})

	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		router.ServeHTTP(recorder, req)
		return recorder
	}
	cheapMember := fmt.Sprintf(`{"channel_id":%d,"group_name":"cheap","upstream_group_id":%d,"presence_generation":%d}`, channel.ID, cheapGeneration.ID, cheapGeneration.PresenceGeneration)
	created := request(http.MethodPost, "/api/gateway/candidate-groups", fmt.Sprintf(`{"name":"low cost","members":[%s]}`, cheapMember))
	if created.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}
	var createPayload struct {
		Data storage.GatewayCandidateGroup `json:"data"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if createPayload.Data.ID == 0 || len(createPayload.Data.Members) != 1 {
		t.Fatalf("created group = %+v", createPayload.Data)
	}
	updated := request(http.MethodPatch, fmt.Sprintf("/api/gateway/candidate-groups/%d", createPayload.Data.ID), fmt.Sprintf(`{"name":"primary","expected_membership_version":%d,"members":[%s]}`, createPayload.Data.MembershipVersion, cheapMember))
	if updated.Code != http.StatusOK || !strings.Contains(updated.Body.String(), `"name":"primary"`) {
		t.Fatalf("update status=%d body=%s", updated.Code, updated.Body.String())
	}
	listed := request(http.MethodGet, "/api/gateway/candidate-groups", "")
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"name":"primary"`) {
		t.Fatalf("list status=%d body=%s", listed.Code, listed.Body.String())
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "paused", Ratio: 0.4}); err != nil {
		t.Fatalf("create paused rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "paused", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", Enabled: true}); err != nil {
		t.Fatalf("create paused upstream key: %v", err)
	}
	if err := store.UpdateLatencyProbeEnabled(channel.ID, "paused", false); err != nil {
		t.Fatalf("pause upstream group: %v", err)
	}
	pausedGeneration, err := store.CaptureUpstreamGroupGeneration(channel.ID, "paused")
	if err != nil {
		t.Fatalf("capture paused generation: %v", err)
	}
	paused := request(http.MethodPost, "/api/gateway/candidate-groups", fmt.Sprintf(`{"name":"paused","members":[{"channel_id":%d,"group_name":"paused","upstream_group_id":%d,"presence_generation":%d}]}`, channel.ID, pausedGeneration.ID, pausedGeneration.PresenceGeneration))
	if paused.Code != http.StatusOK || !strings.Contains(paused.Body.String(), `"group_name":"paused"`) {
		t.Fatalf("paused member status=%d body=%s", paused.Code, paused.Body.String())
	}
	deleted := request(http.MethodDelete, fmt.Sprintf("/api/gateway/candidate-groups/%d", createPayload.Data.ID), "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", deleted.Code, deleted.Body.String())
	}
}

func TestGatewayDownstreamKeyMappingPatchCannotBypassCandidateModelValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	service := gateway.NewService(channels, rates, store, nil, nil, nil)

	createGroup := func(name, modelIDs string) *storage.GatewayCandidateGroup {
		t.Helper()
		channel := &storage.Channel{Name: name, Type: storage.ChannelTypeNewAPI, SiteURL: "https://" + name + ".example", PasswordCipher: "cipher", MonitorEnabled: true}
		if err := channels.Create(channel); err != nil {
			t.Fatal(err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: name, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: name, APIKeyCipher: "cipher", APIKeyPrefix: "sk",
			OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
			ModelFamilies:       "codex", ModelIDs: modelIDs, ModelsSyncedAt: &now,
			Enabled: true, LatencyProbeEnabled: true,
		}); err != nil {
			t.Fatal(err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, name, "codex", modelIDs, now, ""); err != nil {
			t.Fatal(err)
		}
		group, err := service.SaveCandidateGroupWithMappingFamily(0, name, "codex", []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: name}})
		if err != nil {
			t.Fatal(err)
		}
		return group
	}
	supported := createGroup("api-supported", "gpt-5.4")
	unsupported := createGroup("api-unsupported", "gpt-4.1")
	key := &storage.GatewayDownstreamKey{
		Name: "api-mapping", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: supported.ID,
		ModelMappingStrategy: "off", KeyHash: "api-mapping-validation", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: service})
	patch := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPatch, fmt.Sprintf("/api/gateway/downstream-keys/%d", key.ID), strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, request)
		return recorder
	}

	rejected := patch(`{"model_mapping_strategy":"manual","model_mapping_model":"missing-model","model_mapping_reasoning_effort":"high"}`)
	if rejected.Code != http.StatusBadRequest || !strings.Contains(rejected.Body.String(), "does not support") {
		t.Fatalf("unsupported mapping status=%d body=%s", rejected.Code, rejected.Body.String())
	}
	accepted := patch(`{"model_mapping_strategy":"manual","model_mapping_model":"GPT-5.4","model_mapping_reasoning_effort":"high"}`)
	if accepted.Code != http.StatusOK {
		t.Fatalf("case-insensitive mapping status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	rejectedSwitch := patch(fmt.Sprintf(`{"candidate_target_mode":"candidate_group","candidate_group_id":%d}`, unsupported.ID))
	if rejectedSwitch.Code != http.StatusBadRequest || !strings.Contains(rejectedSwitch.Body.String(), "does not support") {
		t.Fatalf("unsupported group switch status=%d body=%s", rejectedSwitch.Code, rejectedSwitch.Body.String())
	}
}

func TestGatewayKeyGroupsAndUpstreamGroupChangesEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "upstream", Type: storage.ChannelTypeNewAPI, SiteURL: "https://upstream.example", PasswordCipher: "x", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cheap", "promo"} {
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: name, Ratio: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "cheap", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "cheap-cipher", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{
		Channels: channels,
		Gateway:  gateway.NewService(channels, rates, store, nil, nil, nil),
		Monitor:  &upstreamGroupChangePreflightMonitor{failures: map[uint]error{}},
	})
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		router.ServeHTTP(recorder, req)
		return recorder
	}

	created := request(http.MethodPost, "/api/gateway/key-groups", `{"name":"Dev"}`)
	if created.Code != http.StatusOK {
		t.Fatalf("create key group status=%d body=%s", created.Code, created.Body.String())
	}
	var keyGroup struct {
		Data storage.GatewayKeyGroup `json:"data"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &keyGroup); err != nil || keyGroup.Data.ID == 0 {
		t.Fatalf("decode key group=%+v err=%v", keyGroup, err)
	}
	updated := request(http.MethodPatch, fmt.Sprintf("/api/gateway/key-groups/%d", keyGroup.Data.ID), `{"name":"Dev workstation"}`)
	if updated.Code != http.StatusOK || !strings.Contains(updated.Body.String(), "Dev workstation") {
		t.Fatalf("update key group status=%d body=%s", updated.Code, updated.Body.String())
	}

	candidateGroup := &storage.GatewayCandidateGroup{Name: "primary"}
	if err := store.SaveCandidateGroup(candidateGroup, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "cheap"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "promo", "added", 17, "BOG - promo", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", UpstreamKeyID: 17, UpstreamKeyName: "BOG - promo",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "promo-cipher", Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "promo", "codex", "gpt-5", time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	changes := request(http.MethodGet, "/api/gateway/upstream-group-changes", "")
	if changes.Code != http.StatusOK {
		t.Fatalf("list changes status=%d body=%s", changes.Code, changes.Body.String())
	}
	var changePayload struct {
		Data []storage.GatewayUpstreamGroupChange `json:"data"`
	}
	if err := json.Unmarshal(changes.Body.Bytes(), &changePayload); err != nil || len(changePayload.Data) != 1 {
		t.Fatalf("decode changes=%+v err=%v", changePayload, err)
	}
	resolved := request(http.MethodPost, fmt.Sprintf("/api/gateway/upstream-group-changes/%d/resolve", changePayload.Data[0].ID), fmt.Sprintf(`{"candidate_group_ids":[%d]}`, candidateGroup.ID))
	if resolved.Code != http.StatusOK {
		t.Fatalf("resolve change status=%d body=%s", resolved.Code, resolved.Body.String())
	}
	assigned, err := store.FindCandidateGroup(candidateGroup.ID)
	if err != nil || len(assigned.Members) != 2 {
		t.Fatalf("assigned candidate group=%+v err=%v", assigned, err)
	}
	history := request(http.MethodGet, "/api/gateway/upstream-group-changes/history?page_size=50", "")
	if history.Code != http.StatusOK {
		t.Fatalf("history status=%d body=%s", history.Code, history.Body.String())
	}
	var historyPayload struct {
		Data struct {
			Items      []storage.GatewayUpstreamGroupChange `json:"items"`
			NextCursor string                               `json:"next_cursor"`
		} `json:"data"`
	}
	if err := json.Unmarshal(history.Body.Bytes(), &historyPayload); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if len(historyPayload.Data.Items) != 1 || historyPayload.Data.Items[0].Status != "assigned" || historyPayload.Data.NextCursor != "" {
		t.Fatalf("history payload=%+v", historyPayload.Data)
	}
	var historyWire struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(history.Body.Bytes(), &historyWire); err != nil {
		t.Fatalf("decode history wire contract: %v", err)
	}
	for _, legacyField := range []string{"total", "page", "page_size", "pages"} {
		if _, exists := historyWire.Data[legacyField]; exists {
			t.Fatalf("cursor history unexpectedly returned legacy %s field: %s", legacyField, history.Body.String())
		}
	}
	invalidCursor := request(http.MethodGet, "/api/gateway/upstream-group-changes/history?cursor=not-a-cursor", "")
	if invalidCursor.Code != http.StatusBadRequest {
		t.Fatalf("invalid history cursor status=%d body=%s", invalidCursor.Code, invalidCursor.Body.String())
	}
	deleted := request(http.MethodDelete, fmt.Sprintf("/api/gateway/key-groups/%d", keyGroup.Data.ID), "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete key group status=%d body=%s", deleted.Code, deleted.Body.String())
	}
}

func TestGatewayAutomaticAssignmentServiceTierRulePreviewAndExecute(t *testing.T) {
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{Name: "tier api", Type: storage.ChannelTypeNewAPI, SiteURL: "https://tier-api.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"seed", "tiered"} {
		if err := db.Create(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: name, Ratio: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "seed", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "seed-cipher", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "tiered", Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	target := &storage.GatewayCandidateGroup{Name: "pro target"}
	if err := store.SaveCandidateGroup(target, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "tiered", "added", 1, "managed", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "tiered", "codex", "gpt-5.4", time.Now(), ""); err != nil {
		t.Fatal(err)
	}
	var identity storage.GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "tiered").First(&identity).Error; err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "operator:automatic-assignment")
		c.Next()
	})
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(channels, storage.NewRates(db), store, nil, nil, nil)})
	request := func(method, path, body string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		router.ServeHTTP(recorder, req)
		return recorder
	}

	providerTierResponse := request(http.MethodPatch, fmt.Sprintf("/api/gateway/upstream-groups/%d/service-tier", identity.ID), `{"service_tier":"pro","source":"provider"}`)
	if providerTierResponse.Code != http.StatusBadRequest {
		t.Fatalf("administrator provider tier response = %d %s", providerTierResponse.Code, providerTierResponse.Body.String())
	}
	tierResponse := request(http.MethodPatch, fmt.Sprintf("/api/gateway/upstream-groups/%d/service-tier", identity.ID), `{"service_tier":"pro","source":"manual"}`)
	if tierResponse.Code != http.StatusOK {
		t.Fatalf("tier response = %d %s", tierResponse.Code, tierResponse.Body.String())
	}
	configResponse := request(http.MethodGet, "/api/gateway/automatic-assignment", "")
	var configPayload struct {
		Data storage.GatewayAutomaticAssignmentConfig `json:"data"`
	}
	if err := json.Unmarshal(configResponse.Body.Bytes(), &configPayload); err != nil || configResponse.Code != http.StatusOK {
		t.Fatalf("config response = %d %s err=%v", configResponse.Code, configResponse.Body.String(), err)
	}
	if configPayload.Data.ModelCatalog.Version == "" || len(configPayload.Data.ModelCatalog.Families) < 14 {
		t.Fatalf("model catalog missing from automatic assignment config: %+v", configPayload.Data.ModelCatalog)
	}
	ruleBody := fmt.Sprintf(`{"ruleset_version":%d,"rule":{"candidate_group_id":%d,"enabled":true,"priority":100,"model_families":["codex"],"family_match_mode":"single_any","service_tiers":["pro"]}}`, configPayload.Data.State.RulesetVersion, target.ID)
	ruleResponse := request(http.MethodPost, "/api/gateway/automatic-assignment/rules", ruleBody)
	if ruleResponse.Code != http.StatusOK {
		t.Fatalf("rule response = %d %s", ruleResponse.Code, ruleResponse.Body.String())
	}
	previewResponse := request(http.MethodPost, "/api/gateway/automatic-assignment/preview", `{}`)
	var previewPayload struct {
		Data []storage.GatewayAutomaticAssignmentPreview `json:"data"`
	}
	if err := json.Unmarshal(previewResponse.Body.Bytes(), &previewPayload); err != nil || previewResponse.Code != http.StatusOK || len(previewPayload.Data) != 1 {
		t.Fatalf("preview response = %d %s err=%v", previewResponse.Code, previewResponse.Body.String(), err)
	}
	preview := previewPayload.Data[0]
	if preview.State != "ready" || preview.EvaluationToken == "" {
		t.Fatalf("preview = %+v", preview)
	}
	executeBody := fmt.Sprintf(`{"change_id":%d,"evaluation_token":%q}`, preview.ChangeID, preview.EvaluationToken)
	executeResponse := request(http.MethodPost, "/api/gateway/automatic-assignment/execute", executeBody)
	if executeResponse.Code != http.StatusOK {
		t.Fatalf("execute response = %d %s", executeResponse.Code, executeResponse.Body.String())
	}
	change, err := store.FindUpstreamGroupChange(preview.ChangeID)
	if err != nil || change.Status != "assigned" {
		t.Fatalf("assigned change = %+v err=%v", change, err)
	}
	var audit storage.GatewayUpstreamGroupAssignmentAudit
	if err := db.Where("change_id = ?", preview.ChangeID).Order("id DESC").First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	if audit.Actor != "operator:automatic-assignment" {
		t.Fatalf("automatic assignment audit actor = %q", audit.Actor)
	}
}

func TestGatewayAutomaticAssignmentMutationAPIsRequireExpectedVersions(t *testing.T) {
	db := testdb.Open(t)
	store := storage.NewGateway(db)
	candidate := &storage.GatewayCandidateGroup{Name: "versioned target"}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}
	keyGroup := &storage.GatewayKeyGroup{Name: "versioned keys"}
	if err := store.SaveKeyGroup(keyGroup); err != nil {
		t.Fatal(err)
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: gateway.NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, nil, nil)})
	request := func(method, path, body string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		router.ServeHTTP(recorder, req)
		return recorder
	}
	assertVersionRequired := func(name string, response *httptest.ResponseRecorder) {
		t.Helper()
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "expected version is required") {
			t.Fatalf("%s response = %d %s", name, response.Code, response.Body.String())
		}
	}

	assertVersionRequired("state update", request(http.MethodPatch, "/api/gateway/automatic-assignment/state", `{"backfill_complete":false,"auto_execute_enabled":false}`))
	assertVersionRequired("rule create", request(http.MethodPost, "/api/gateway/automatic-assignment/rules", fmt.Sprintf(`{"rule":{"candidate_group_id":%d,"enabled":true,"priority":1,"model_families":["codex"],"family_match_mode":"single_any"}}`, candidate.ID)))
	assertVersionRequired("rule delete", request(http.MethodDelete, "/api/gateway/automatic-assignment/rules/1", ""))
	config, err := store.AutomaticAssignmentConfig()
	if err != nil {
		t.Fatal(err)
	}
	createRule := request(http.MethodPost, "/api/gateway/automatic-assignment/rules", fmt.Sprintf(`{"ruleset_version":%d,"rule":{"candidate_group_id":%d,"enabled":true,"priority":1,"model_families":["codex"],"family_match_mode":"single_any"}}`, config.State.RulesetVersion, candidate.ID))
	if createRule.Code != http.StatusOK {
		t.Fatalf("create versioned rule response = %d %s", createRule.Code, createRule.Body.String())
	}
	var createdRule struct {
		Data struct {
			Rule  storage.GatewayCandidateGroupAutoRule     `json:"rule"`
			State storage.GatewayCandidateGroupRulesetState `json:"state"`
		} `json:"data"`
	}
	if err := json.Unmarshal(createRule.Body.Bytes(), &createdRule); err != nil || createdRule.Data.Rule.ID == 0 {
		t.Fatalf("decode created rule = %+v err=%v", createdRule.Data.Rule, err)
	}
	missingRuleVersion := fmt.Sprintf(`{"ruleset_version":%d,"rule":{"candidate_group_id":%d,"enabled":true,"priority":2,"model_families":["codex"],"family_match_mode":"single_any"}}`, createdRule.Data.State.RulesetVersion, candidate.ID)
	assertVersionRequired("rule update", request(http.MethodPatch, fmt.Sprintf("/api/gateway/automatic-assignment/rules/%d", createdRule.Data.Rule.ID), missingRuleVersion))

	createPolicy := request(http.MethodPost, "/api/gateway/key-group-route-policies", fmt.Sprintf(`{"expected_version":0,"policy":{"key_group_id":%d,"gateway_app":"codex","model_family":"codex","default_candidate_group_id":%d,"target_mode":"candidate_group","enabled":true}}`, keyGroup.ID, candidate.ID))
	if createPolicy.Code != http.StatusOK {
		t.Fatalf("create policy response = %d %s", createPolicy.Code, createPolicy.Body.String())
	}
	var created struct {
		Data storage.GatewayKeyGroupRoutePolicy `json:"data"`
	}
	if err := json.Unmarshal(createPolicy.Body.Bytes(), &created); err != nil || created.Data.ID == 0 || created.Data.PolicyVersion == 0 {
		t.Fatalf("decode created policy = %+v err=%v", created.Data, err)
	}
	policyBody := fmt.Sprintf(`{"policy":{"id":%d,"key_group_id":%d,"gateway_app":"codex","model_family":"codex","default_candidate_group_id":%d,"target_mode":"candidate_group","enabled":true,"policy_version":%d}}`, created.Data.ID, keyGroup.ID, candidate.ID, created.Data.PolicyVersion)
	assertVersionRequired("policy update", request(http.MethodPatch, fmt.Sprintf("/api/gateway/key-group-route-policies/%d", created.Data.ID), policyBody))
	assertVersionRequired("policy delete", request(http.MethodDelete, fmt.Sprintf("/api/gateway/key-group-route-policies/%d", created.Data.ID), ""))
	deletePolicy := request(http.MethodDelete, fmt.Sprintf("/api/gateway/key-group-route-policies/%d?expected_version=%d", created.Data.ID, created.Data.PolicyVersion), "")
	if deletePolicy.Code != http.StatusOK {
		t.Fatalf("versioned policy delete = %d %s", deletePolicy.Code, deletePolicy.Body.String())
	}
}

func TestUpdateGatewayLatencyProbeModelChecksDisabledAndMissingGroups(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewGateway(db)
	if err := db.Create(&storage.Channel{
		ID: 1, Name: "probe model", Type: storage.ChannelTypeNewAPI, SiteURL: "https://probe-model.example", PasswordCipher: "cipher",
	}).Error; err != nil {
		t.Fatalf("create probe-model channel: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: 1, GroupName: "disabled", APIKeyCipher: "cipher", ModelFamilies: "codex", ModelIDs: "gpt-mini", Enabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key: %v", err)
	}
	if err := store.DisableUpstreamKey(1, "disabled", "expired"); err != nil {
		t.Fatalf("disable upstream key: %v", err)
	}
	d := &Deps{Gateway: gateway.NewService(nil, nil, store, nil, nil, nil)}
	call := func(body string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPatch, "/api/gateway/latency-probe-model", strings.NewReader(body))
		ctx.Request.Header.Set("Content-Type", "application/json")
		updateGatewayLatencyProbeModel(ctx, d)
		return recorder
	}
	if response := call(`{"channel_id":1,"group_name":"disabled","model_name":"gpt-mini"}`); response.Code != http.StatusOK {
		t.Fatalf("disabled group update status=%d body=%s", response.Code, response.Body.String())
	}
	if stored, err := store.FindUpstreamKeyAny(1, "disabled"); err != nil || stored.LatencyProbeModel != "gpt-mini" {
		t.Fatalf("disabled group update = %+v err=%v", stored, err)
	}
	if response := call(`{"channel_id":1,"group_name":"disabled","model_name":""}`); response.Code != http.StatusOK {
		t.Fatalf("disabled group clear status=%d body=%s", response.Code, response.Body.String())
	}
	if response := call(`{"channel_id":1,"group_name":"missing","model_name":""}`); response.Code != http.StatusBadRequest {
		t.Fatalf("missing group clear status=%d body=%s", response.Code, response.Body.String())
	}
}
