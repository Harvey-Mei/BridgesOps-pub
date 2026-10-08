package api

import (
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
	"github.com/gin-gonic/gin"
)

type gatewayBalanceFactForwardFixture struct {
	channels *storage.Channels
	store    *storage.Gateway
	service  *gateway.Service
	channel  storage.Channel
	decision *gatewayRouteDecision
}

func newGatewayBalanceFactForwardFixture(t *testing.T, siteURL string) gatewayBalanceFactForwardFixture {
	t.Helper()
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	channel := storage.Channel{
		Name: "balance-fact", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex,
		SiteURL: siteURL, PasswordCipher: "test", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(&channel); err != nil {
		t.Fatalf("create balance-fact channel: %v", err)
	}
	now := time.Now().UTC()
	if _, err := rates.Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1, LastSeenAt: now,
	}); err != nil {
		t.Fatalf("upsert balance-fact rate: %v", err)
	}
	cipher, err := appcrypto.NewCipher("gateway-balance-fact-flow-test-secret")
	if err != nil {
		t.Fatalf("new balance-fact cipher: %v", err)
	}
	ciphertext, err := cipher.Encrypt("sk-balance-fact")
	if err != nil {
		t.Fatalf("encrypt balance-fact key: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", GroupRatio: 1,
		APIKeyCipher: ciphertext, APIKeyPrefix: "sk-test", UpstreamKeyID: 1,
		UpstreamKeyName: "BOG balance fact", ModelFamilies: "codex", ModelIDs: "gpt-test",
		ModelsSyncedAt: &now, Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("upsert balance-fact key: %v", err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "BOG balance fact", now); err != nil {
		t.Fatalf("record balance-fact generation: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil {
		t.Fatalf("capture balance-fact generation: %v", err)
	}
	service := gateway.NewService(channels, rates, store, cipher, gatewayProxyTestKeyManager{
		groups: map[uint][]connector.APIKeyGroup{
			channel.ID: {{Name: "primary", Ratio: 1}},
		},
	}, nil)
	decision := &gatewayRouteDecision{SmartRouting: true, Candidates: []gateway.Candidate{{
		Channel: channel, Rate: storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1, CompletionRatio: 1},
		UpstreamGeneration: generation, App: storage.GatewayAppCodex, RawRatio: 1, RawCompletionRatio: 1,
	}}}
	return gatewayBalanceFactForwardFixture{
		channels: channels, store: store, service: service, channel: channel, decision: decision,
	}
}

func (f gatewayBalanceFactForwardFixture) forward(t *testing.T, smartRouting bool) *httptest.ResponseRecorder {
	t.Helper()
	body := `{"model":"gpt-test","messages":[]}`
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	f.decision.SmartRouting = smartRouting
	gatewayForward(
		ctx,
		&Deps{Channels: f.channels, Gateway: f.service},
		&storage.GatewayDownstreamKey{ID: 1, App: storage.GatewayAppCodex, SmartRouting: smartRouting},
		f.decision,
		"gpt-test",
		"",
		[]byte(body),
		gatewayModelMappingResult{Model: "gpt-test"},
		time.Now(),
	)
	return recorder
}

func (f gatewayBalanceFactForwardFixture) storedChannel(t *testing.T) *storage.Channel {
	t.Helper()
	channel, err := f.channels.FindByID(f.channel.ID)
	if err != nil {
		t.Fatalf("read balance-fact channel: %v", err)
	}
	return channel
}

func TestGatewayStructuredAccountBalanceStatusMarksFactWithoutInvalidatingKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/v1/models" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-test"}]}`))
					return
				}
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"code":"insufficient_balance","message":"Insufficient account balance"}}`))
			}))
			defer upstream.Close()

			fixture := newGatewayBalanceFactForwardFixture(t, upstream.URL)
			fixture.forward(t, true)
			channel := fixture.storedChannel(t)
			if !channel.HasCurrentUpstreamAccountBalanceInsufficient() || channel.LastError != "" {
				t.Fatalf("structured balance status did not write only the current fact: %+v", channel)
			}
			key, err := fixture.store.FindUpstreamKeyAny(channel.ID, "primary")
			if err != nil || !key.Enabled || strings.TrimSpace(key.APIKeyCipher) == "" || calls.Load() != 1 {
				t.Fatalf("structured balance status changed key state: key=%+v calls=%d err=%v", key, calls.Load(), err)
			}
		})
	}
}

func TestGatewayUnstructuredAuthorizationStatusDoesNotMarkBalanceFact(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "plain balance text", status: http.StatusUnauthorized, body: "Insufficient account balance"},
		{name: "generic permission JSON", status: http.StatusForbidden, body: `{"error":{"message":"permission denied"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/v1/models" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-test"}]}`))
					return
				}
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer upstream.Close()

			fixture := newGatewayBalanceFactForwardFixture(t, upstream.URL)
			fixture.forward(t, true)
			channel := fixture.storedChannel(t)
			if channel.HasCurrentUpstreamAccountBalanceInsufficient() {
				t.Fatalf("unstructured authorization response wrote balance fact: %+v", channel)
			}
			key, err := fixture.store.FindUpstreamKeyAny(channel.ID, "primary")
			if err != nil || key.Enabled || calls.Load() != 1 {
				t.Fatalf("unstructured authorization response did not follow key-auth behavior: key=%+v calls=%d err=%v", key, calls.Load(), err)
			}
		})
	}
}

func TestGatewaySuccessfulFixedInferenceClearsCurrentBalanceFact(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl-test","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer upstream.Close()

	fixture := newGatewayBalanceFactForwardFixture(t, upstream.URL)
	if err := fixture.channels.MarkUpstreamAccountBalanceInsufficient(
		fixture.channel.ID, fixture.channel.UpstreamAccountIdentityRevision, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed current balance fact: %v", err)
	}
	recorder := fixture.forward(t, false)
	if recorder.Code != http.StatusOK || calls.Load() != 1 || !strings.Contains(recorder.Body.String(), `"content":"ok"`) {
		t.Fatalf("fixed inference response=%d body=%q calls=%d", recorder.Code, recorder.Body.String(), calls.Load())
	}
	if channel := fixture.storedChannel(t); channel.HasCurrentUpstreamAccountBalanceInsufficient() {
		t.Fatalf("successful real inference retained balance fact: %+v", channel)
	}
}

func TestGatewayModelsSuccessDoesNotClearCurrentBalanceFact(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-test"}]}`))
	}))
	defer upstream.Close()

	fixture := newGatewayBalanceFactForwardFixture(t, upstream.URL)
	if err := fixture.channels.MarkUpstreamAccountBalanceInsufficient(
		fixture.channel.ID, fixture.channel.UpstreamAccountIdentityRevision, time.Now().UTC(),
	); err != nil {
		t.Fatalf("seed current balance fact: %v", err)
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	gatewayProxyModelsWithTimeout(ctx, &Deps{Channels: fixture.channels, Gateway: fixture.service}, &storage.GatewayDownstreamKey{
		App: storage.GatewayAppCodex, SmartRouting: false,
		ManualChannelID: fixture.channel.ID, ManualGroupName: "primary",
	}, time.Second)
	if recorder.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("models response=%d body=%q calls=%d", recorder.Code, recorder.Body.String(), calls.Load())
	}
	if channel := fixture.storedChannel(t); !channel.HasCurrentUpstreamAccountBalanceInsufficient() {
		t.Fatalf("models success cleared current balance fact: %+v", channel)
	}
}
