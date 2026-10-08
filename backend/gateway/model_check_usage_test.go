package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func TestModelCheckLiveModelsAndVisibleUsageStaySeparateFromRouting(t *testing.T) {
	const secret = "private-test-key"
	var modelsFail atomic.Bool
	var inferenceCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+secret {
			t.Error("not using the selected managed key")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/models" {
			if modelsFail.Load() {
				w.WriteHeader(503)
				return
			}
			fmt.Fprint(w, `{"data":[{"id":"gpt-fresh"}]}`)
			return
		}
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if inferenceCalls.Add(1) == 2 {
			w.WriteHeader(504)
			fmt.Fprint(w, secret+" internal error")
			return
		}
		w.Header().Set("X-Oneapi-Request-Id", "pelican-request")
		fmt.Fprint(w, `{"model":"gpt-fresh","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":100,"completion_tokens":30,"total_tokens":130,"prompt_tokens_details":{"cached_tokens":80}}}`)
	}))
	defer server.Close()
	db := testdb.Open(t)
	channels, rates, store := storage.NewChannels(db), storage.NewRates(db), storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("model-check-usage-secret")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "Chosen", Type: storage.ChannelTypeNewAPI, GatewayApp: storage.GatewayAppCodex, SiteURL: server.URL, MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "selected", Ratio: 0.5, CompletionRatio: 2, LastSeenAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	ciphertext, err := cipher.Encrypt(secret)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "selected", UpstreamKeyID: 7, UpstreamKeyName: "BOG selected", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: ciphertext, APIKeyPrefix: "test", ModelFamilies: "codex", ModelIDs: "gpt-stale", Enabled: true, LatencyProbeEnabled: true}); err != nil {
		t.Fatal(err)
	}
	recordModelCheckGeneration(t, store, channel.ID, "selected")
	if err := store.SaveCandidateGroup(&storage.GatewayCandidateGroup{Name: "allowed"}, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "selected"}}); err != nil {
		t.Fatal(err)
	}
	service := NewService(channels, rates, store, cipher, nil, nil)
	choice := modelcheck.TargetDescriptor{ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, ChannelName: channel.Name, GroupName: "selected", App: storage.GatewayAppCodex}
	models, err := service.ReadManualModelTargets(context.Background(), choice)
	if err != nil || len(models) != 1 || models[0].ModelName != "gpt-fresh" {
		t.Fatalf("live models=%+v err=%v", models, err)
	}
	modelsFail.Store(true)
	if _, err := service.ReadManualModelTargets(context.Background(), choice); err == nil {
		t.Fatal("model read failed but silently used cached names")
	}
	choice.ModelName = "gpt-fresh"
	var events []modelcheck.ProgressEvent
	ctx := modelcheck.WithProgressObserver(context.Background(), func(e modelcheck.ProgressEvent) { events = append(events, e) })
	for _, kind := range []string{"candy_shapes", modelcheck.PelicanProbeKind} {
		_, err := service.RunProbe(ctx, choice, modelcheck.ProbeRequest{Kind: kind, Prompt: "test", MaxTokens: 8192})
		if kind == "candy_shapes" && err != nil {
			t.Fatal(err)
		}
		if kind == modelcheck.PelicanProbeKind && err == nil {
			t.Fatal("missing HTTP failure")
		}
	}
	if inferenceCalls.Load() != 2 {
		t.Fatal("a detection request was replayed")
	}
	logs, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 10)
	if err != nil || len(logs) != 2 {
		t.Fatalf("visible usage=%d err=%v", len(logs), err)
	}
	for _, log := range logs {
		if log.ChannelID != channel.ID || log.GroupName != "selected" || log.ModelName != "gpt-fresh" || log.RouteReason != "model_check" || log.CostStatus != storage.GatewayCostStatusUnavailable {
			t.Fatalf("invalid usage identity or fabricated price: %+v", log)
		}
		if log.StatusCode == 200 && (log.PromptTokens != 100 || log.CompletionTokens != 30 || log.CacheReadTokens != 80 || log.TotalTokens != 130 || log.UpstreamRequestID != "pelican-request") {
			t.Fatalf("lost usage: %+v", log)
		}
	}
	var rawLogs []storage.GatewayRequestLog
	if err := db.Find(&rawLogs).Error; err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(struct {
		Logs   any
		Events any
	}{rawLogs, events})
	if strings.Contains(string(raw), secret) {
		t.Fatal("credentials leaked into usage/log events")
	}
	if len(events) != 5 || events[0].Type != "request_sent" || events[2].Type != "response" {
		t.Fatalf("missing live request/response events: %+v", events)
	}
	// A definitely nonzero duration makes the sample exclusion meaningful.
	if err := db.Model(&storage.GatewayRequestLog{}).Where("route_reason = ?", "model_check").Update("latency_ms", 5000).Error; err != nil {
		t.Fatal(err)
	}
	latencies, err := store.ListRecentSuccessfulLatencyLogsSince(time.Now().Add(-time.Hour), 10)
	if err != nil || len(latencies) != 0 {
		t.Fatalf("model checks polluted latency samples: %+v %v", latencies, err)
	}
	stats, err := store.ListSuccessfulLatencyStatsSince(time.Now().Add(-time.Hour))
	if err != nil || len(stats) != 0 {
		t.Fatalf("model checks polluted latency stats: %+v %v", stats, err)
	}
}

func TestModelCheckUsageRejectsCredentialIDsAndUndispatchedAttempts(t *testing.T) {
	for _, rejectDispatch := range []bool{false, true} {
		t.Run(fmt.Sprint(rejectDispatch), func(t *testing.T) {
			const secret = "private-model-check-secret"
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("X-Oneapi-Request-Id", "echo-"+secret)
				fmt.Fprint(w, `{"model":"gpt-test","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":10,"completion_tokens":1}}`)
			}))
			defer server.Close()
			db := testdb.Open(t)
			channels, rates, store := storage.NewChannels(db), storage.NewRates(db), storage.NewGateway(db)
			channel := &storage.Channel{Name: "probe", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL, MonitorEnabled: true}
			if err := channels.Create(channel); err != nil {
				t.Fatal(err)
			}
			if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "selected", Ratio: 1, LastSeenAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			cipher, err := appcrypto.NewCipher("model-check-usage-secret")
			if err != nil {
				t.Fatal(err)
			}
			encrypted, err := cipher.Encrypt(secret)
			if err != nil {
				t.Fatal(err)
			}
			key := &storage.GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "selected", UpstreamKeyID: 7, UpstreamKeyName: "BOG selected", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: encrypted, APIKeyPrefix: "test", ModelFamilies: "codex", ModelIDs: "gpt-test", Enabled: true}
			if err := store.UpsertUpstreamKey(key); err != nil {
				t.Fatal(err)
			}
			recordModelCheckGeneration(t, store, channel.ID, "selected")
			invalidated := false
			if rejectDispatch {
				if err := db.Callback().Query().After("gorm:query").Register("invalidate-after-usage-snapshot", func(tx *gorm.DB) {
					if tx.Statement.Table == "rs" && !invalidated {
						invalidated = true
						if err := db.Model(&storage.GatewayUpstreamKey{}).Where("channel_id = ?", channel.ID).Update("enabled", false).Error; err != nil {
							tx.AddError(err)
						}
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			service := NewService(channels, rates, store, cipher, nil, nil)
			var events []modelcheck.ProgressEvent
			ctx := modelcheck.WithProgressObserver(context.Background(), func(e modelcheck.ProgressEvent) { events = append(events, e) })
			_, err = service.RunProbe(ctx, modelcheck.TargetDescriptor{ChannelID: channel.ID, UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision, GroupName: "selected", ModelName: "gpt-test", App: storage.GatewayAppCodex}, modelcheck.ProbeRequest{Kind: modelcheck.PelicanProbeKind, Prompt: "test", MaxTokens: 32})
			var logs []storage.GatewayRequestLog
			if queryErr := db.Find(&logs).Error; queryErr != nil {
				t.Fatal(queryErr)
			}
			if rejectDispatch {
				if !invalidated || err == nil || calls.Load() != 0 || len(logs) != 0 || len(events) != 0 {
					t.Fatalf("undispatched attempt was audited: invalidated=%t calls=%d logs=%d events=%d err=%v", invalidated, calls.Load(), len(logs), len(events), err)
				}
			} else {
				if err != nil || calls.Load() != 1 || len(logs) != 1 || logs[0].UpstreamRequestID != "" {
					t.Fatal("credential request ID was saved or valid probe failed", err)
				}
				encoded, _ := json.Marshal(struct {
					Logs   any
					Events any
				}{logs, events})
				if strings.Contains(string(encoded), secret) {
					t.Fatal("credential echoed into audit")
				}
			}
		})
	}
}

func TestModelCheckRequestIDRejectsCredentialEchoForBothProviders(t *testing.T) {
	for _, provider := range []storage.ChannelType{storage.ChannelTypeNewAPI, storage.ChannelTypeSub2API} {
		for _, value := range []string{"safe-id", "secret-token", "prefix-secret-token-suffix"} {
			headers := http.Header{}
			headers.Set("X-Oneapi-Request-Id", value)
			headers.Set("X-Client-Request-ID", value)
			got := modelCheckRequestID(headers, provider, "secret-token")
			if (value == "safe-id" && got != value) || (value != "safe-id" && got != "") {
				t.Fatal("unsafe request ID handling", provider)
			}
		}
	}
}

func TestModelCheckCostIdentityReadFailureRetriesBeforeAndAfterLookup(t *testing.T) {
	for _, failAt := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			db := testdb.Open(t)
			channels, store := storage.NewChannels(db), storage.NewGateway(db)
			channel := &storage.Channel{Name: "cost", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid"}
			if err := channels.Create(channel); err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			details, _ := json.Marshal(modelCheckUsageDetails{Kind: modelcheck.PelicanProbeKind, CostUnitEpoch: channel.CostUnitEpoch, FundingTrusted: true})
			factor := 1.0
			log := storage.GatewayRequestLog{ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision, GroupName: "selected", ModelName: "gpt-test", Method: "POST", Path: "/v1/chat/completions", StatusCode: 200, PromptTokens: 10, UpstreamKeyID: 7, UpstreamKeyName: "BOG selected", RouteReason: "model_check", RouteDetails: string(details), FundingFactor: &factor, CostStatus: storage.GatewayCostStatusUnavailable, CostReconcileAt: &now, CreatedAt: now}
			log.PriceCanonicalGroupName, log.PriceModelName = "selected", "gpt-test"
			log.UpstreamGroupID, log.UpstreamGroupGeneration = 1, 1
			log.UpstreamKeyLocalID, log.UpstreamKeyGeneration = 1, 1
			log.PriceOperation, log.PriceProtocol = storage.GatewayPricingOperationTextGeneration, storage.GatewayPricingProtocolOpenAIChatCompletions
			log.UsageSchema, log.UsageSchemaVersion = storage.GatewayPricingUsageSchemaTokensV1, 1
			log.CostUnavailableReason = storage.GatewayPricingUnavailableFactMissing
			if err := store.AppendRequestLog(&log); err != nil {
				t.Fatal(err)
			}
			keys := &fakeKeyManager{requestCost: &connector.RequestCostResult{UsageID: "pelican-cost", Cost: .02, Source: "newapi_quota"}}
			service := NewService(channels, storage.NewRates(db), store, nil, keys, nil)
			// Drive reconciliation explicitly, without an unbounded test worker.
			service.costWorkerOnce.Do(func() {})
			reads := 0
			if err := db.Callback().Query().Before("gorm:query").Register("model-check-temporary-read", func(tx *gorm.DB) {
				if tx.Statement.Table == "channels" {
					reads++
					if reads == max(1, failAt) {
						tx.AddError(errors.New("temporary database read failure"))
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			if failAt == 0 {
				log.ID, log.CostReconcileAt = 0, nil
				if err := service.finishModelCheckUsage(&log, modelcheck.ProbeResponse{StatusCode: 200, PromptTokens: 10, TotalTokens: 10}, nil, now.Add(-time.Second)); err != nil {
					t.Fatal(err)
				}
			} else {
				service.reconcilePendingCost(log, now.Add(-time.Second), now)
			}
			var saved storage.GatewayRequestLog
			if err := db.First(&saved, log.ID).Error; err != nil {
				t.Fatal(err)
			}
			wantTries := 1
			if failAt == 0 {
				wantTries = 0
			}
			if saved.CostReconcileAt == nil || saved.CostReconcileTries != wantTries || saved.CostStatus == storage.GatewayCostStatusReconciled {
				t.Fatalf("temporary identity failure became terminal: %+v", saved)
			}
			service.reconcilePendingCost(saved, now.Add(-time.Second), now)
			if err := db.First(&saved, log.ID).Error; err != nil {
				t.Fatal(err)
			}
			if saved.CostStatus != storage.GatewayCostStatusReconciled || saved.Cost != .02 {
				t.Fatalf("did not recover: %+v", saved)
			}
			changed := log
			changed.ChannelIdentityRevision++
			if current, err := service.internalProbeCostIdentityCurrent(changed); err != nil || current {
				t.Fatal("accepted changed account")
			}
			changed = log
			changed.RouteDetails = `{"kind":"pelican_svg","funding_trusted":false}`
			if current, err := service.internalProbeCostIdentityCurrent(changed); err != nil || current {
				t.Fatal("accepted untrusted funding")
			}
		})
	}
}
