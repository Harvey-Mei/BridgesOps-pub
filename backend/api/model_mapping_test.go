package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/clauderadar"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestRewriteGatewayRequestForMappingUpdatesModelAndReasoning(t *testing.T) {
	body, err := rewriteGatewayRequestForMapping([]byte(`{"model":"gpt-5-mini","reasoning":{"effort":"medium"},"input":"hello"}`), "/v1/responses", storage.GatewayAppCodex, "gpt-5.4", "high")
	if err != nil {
		t.Fatalf("rewrite request: %v", err)
	}
	var payload struct {
		Model     string `json:"model"`
		Reasoning struct {
			Effort string `json:"effort"`
		} `json:"reasoning"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode rewritten request: %v", err)
	}
	if payload.Model != "gpt-5.4" || payload.Reasoning.Effort != "high" {
		t.Fatalf("rewritten request = %+v", payload)
	}
}

func TestRewriteGatewayRequestForMappingPreservesCodexMax(t *testing.T) {
	body, err := rewriteGatewayRequestForMapping([]byte(`{"model":"gpt-5.4","input":"hello"}`), "/v1/responses", storage.GatewayAppCodex, "gpt-5.6-terra", "max")
	if err != nil {
		t.Fatalf("rewrite request: %v", err)
	}
	if got := gatewayRequestModel(body); got != "gpt-5.6-terra" {
		t.Fatalf("rewritten model = %q, want gpt-5.6-terra", got)
	}
	if got := gatewayRequestReasoningEffort(body); got != "max" {
		t.Fatalf("rewritten reasoning effort = %q, want max", got)
	}
}

func TestRewriteGatewayRequestForMappingCreatesClaudeThinkingBudget(t *testing.T) {
	body, err := rewriteGatewayRequestForMapping([]byte(`{"model":"claude-sonnet","thinking":{"type":"disabled","budget_tokens":1024},"messages":[{"role":"user","content":"hello"}]}`), "/v1/messages", storage.GatewayAppClaude, "claude-opus", "2048")
	if err != nil {
		t.Fatalf("rewrite request: %v", err)
	}
	var payload struct {
		Model    string `json:"model"`
		Thinking struct {
			Type         string `json:"type"`
			BudgetTokens int64  `json:"budget_tokens"`
		} `json:"thinking"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode rewritten request: %v", err)
	}
	if payload.Model != "claude-opus" || payload.Thinking.Type != "enabled" || payload.Thinking.BudgetTokens != 2048 {
		t.Fatalf("rewritten request = %+v", payload)
	}
	if _, err := rewriteGatewayRequestForMapping([]byte(`{"model":"claude-sonnet"}`), "/v1/messages", storage.GatewayAppClaude, "claude-opus", "high"); err == nil {
		t.Fatal("non-numeric Claude thinking budget was accepted")
	}
}

func TestRewriteGatewayRequestForMappingCreatesCodexReasoningWhenMissing(t *testing.T) {
	body, err := rewriteGatewayRequestForMapping([]byte(`{"model":"gpt-5-mini","thinking":{"type":"disabled","budget_tokens":1024},"messages":[{"role":"user","content":"hello"}]}`), "/v1/chat/completions", storage.GatewayAppCodex, "gpt-5.4", "high")
	if err != nil {
		t.Fatalf("rewrite request: %v", err)
	}
	if got := gatewayRequestReasoningEffort(body); got != "high" {
		t.Fatalf("rewritten reasoning effort = %q, want high", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode rewritten request: %v", err)
	}
	thinking, ok := objectValue(payload, "thinking")
	if !ok || thinking["type"] != "enabled" || thinking["budget_tokens"] != nil {
		t.Fatalf("rewritten thinking = %#v", thinking)
	}
}

func TestForceGatewayModelMappingReasoningEffortUsesControlledDefaults(t *testing.T) {
	checks := []struct {
		name     string
		app      storage.GatewayApp
		strategy gateway.ModelMappingStrategy
		selected gateway.ModelMappingOption
		want     string
	}{
		{name: "codex radar", app: storage.GatewayAppCodex, strategy: gateway.ModelMappingStrategyIQ, selected: gateway.ModelMappingOption{Model: "gpt-5.4", ReasoningEffort: "medium"}, want: "medium"},
		{name: "codex radar max", app: storage.GatewayAppCodex, strategy: gateway.ModelMappingStrategyIQ, selected: gateway.ModelMappingOption{Model: "gpt-5.6-terra", ReasoningEffort: "max"}, want: "max"},
		{name: "codex default", app: storage.GatewayAppCodex, strategy: gateway.ModelMappingStrategyCost, selected: gateway.ModelMappingOption{Model: "gpt-5.4"}, want: "high"},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			selected, err := forceGatewayModelMappingReasoningEffort(&storage.GatewayDownstreamKey{App: check.app}, check.strategy, check.selected)
			if err != nil {
				t.Fatalf("force reasoning: %v", err)
			}
			if selected.ReasoningEffort != check.want {
				t.Fatalf("reasoning effort = %q, want %q", selected.ReasoningEffort, check.want)
			}
		})
	}
	if _, err := forceGatewayModelMappingReasoningEffort(&storage.GatewayDownstreamKey{App: storage.GatewayAppCodex}, gateway.ModelMappingStrategyManual, gateway.ModelMappingOption{Model: "gpt-5.4"}); err == nil {
		t.Fatal("manual mapping accepted an empty reasoning effort")
	}
	if _, err := forceGatewayModelMappingReasoningEffort(&storage.GatewayDownstreamKey{App: storage.GatewayAppClaude}, gateway.ModelMappingStrategyIQ, gateway.ModelMappingOption{Model: "claude-opus"}); err == nil {
		t.Fatal("automatic Claude mapping invented a default thinking budget")
	}
}

func TestGatewayModelMappingResultWritesRequestedAndActualValues(t *testing.T) {
	result := newGatewayModelMappingResult("gpt-5-mini", "low", []byte(`{"model":"gpt-5-mini"}`))
	result.Strategy = gateway.ModelMappingStrategyIQ
	result.Model = "gpt-5.4"
	result.ReasoningEffort = "high"
	result.Details = "智商优先（Codex Radar）：gpt-5-mini / low -> gpt-5.4 / high"
	log := &storage.GatewayRequestLog{ModelName: result.Model, ReasoningEffort: result.ReasoningEffort}
	result.applyToLog(log)
	if log.RequestedModelName != "gpt-5-mini" || log.RequestedReasoningEffort != "low" || log.ModelMappingStrategy != "iq" || log.ModelMappingDetails != result.Details {
		t.Fatalf("mapping log = %+v", log)
	}
}

func TestGatewayModelMappingResultAllocatesOnlyForRewriteAndDropsFallbackBody(t *testing.T) {
	original := []byte(`{"model":"gpt-5-mini","reasoning":{"effort":"low"},"input":"hello"}`)
	result := newGatewayModelMappingResult("gpt-5-mini", "low", original)
	if result.RequestedBody != nil {
		t.Fatalf("constructor retained a fallback body before rewrite: %d bytes", len(result.RequestedBody))
	}
	if len(result.Body) == 0 || &result.Body[0] != &original[0] {
		t.Fatal("constructor copied the immutable request body")
	}

	rewritten, err := rewriteGatewayRequestForMapping(result.Body, "/v1/responses", storage.GatewayAppCodex, "gpt-5.4", "high")
	if err != nil {
		t.Fatalf("rewrite request: %v", err)
	}
	if len(rewritten) == 0 || &rewritten[0] == &original[0] {
		t.Fatal("rewrite unexpectedly reused the original backing array")
	}
	if got := gatewayRequestModel(original); got != "gpt-5-mini" {
		t.Fatalf("rewrite mutated original model = %q", got)
	}

	mapped := result
	mapped.Strategy = gateway.ModelMappingStrategyManual
	mapped.Applied = true
	mapped.RequestedBody = mapped.Body
	mapped.Body = rewritten
	finalized := gatewayModelMappingFinalized(mapped)
	if finalized.RequestedBody != nil || len(finalized.Body) == 0 || &finalized.Body[0] != &rewritten[0] {
		t.Fatal("successful mapping retained the original fallback body")
	}

	fallback := gatewayModelMappingAutoDisabled(mapped, context.DeadlineExceeded)
	if fallback.RequestedBody != nil || len(fallback.Body) == 0 || &fallback.Body[0] != &original[0] {
		t.Fatal("auto-disabled mapping copied or failed to restore the original body")
	}
}

func TestGatewayModelMappingPreviewUsesManualSelection(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	deps := &Deps{Gateway: gateway.NewService(nil, nil, store, nil, nil, nil)}
	key := &storage.GatewayDownstreamKey{
		App:                         storage.GatewayAppCodex,
		ModelFamily:                 "codex",
		CandidateGroupID:            1,
		ModelMappingStrategy:        "manual",
		ModelMappingModel:           "gpt-5.4",
		ModelMappingReasoningEffort: "high",
	}
	preview := gatewayModelMappingPreview(deps, key, &storage.GatewayCandidateGroup{ID: 1, ModelMappingFamily: "codex"})
	if preview == nil {
		t.Fatal("preview is nil")
	}
	if preview.Strategy != "manual" || preview.Model != "gpt-5.4" || preview.ReasoningEffort != "high" || preview.Source != "手动设置" || preview.Error != "" {
		t.Fatalf("preview = %+v", preview)
	}
	key.ModelMappingStrategy = "off"
	if preview := gatewayModelMappingPreview(deps, key, nil); preview != nil {
		t.Fatalf("disabled mapping preview = %+v, want nil", preview)
	}
}

func TestGatewayModelMappingPreviewKeepsClaudeReferenceWithoutInventingAPIModel(t *testing.T) {
	radarServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"iq":{"models":[{"key":"m2","name":"Sonnet 5 max","historical_only":true,"score":120},{"key":"m5","name":"Fable 5 xhigh","score":120}]}}`))
	}))
	defer radarServer.Close()

	radar := clauderadar.New(clauderadar.Options{URL: radarServer.URL, Client: radarServer.Client()})
	if _, err := radar.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh Claude radar: %v", err)
	}
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	deps := &Deps{Gateway: gateway.NewService(nil, nil, storage.NewGateway(db), nil, nil, nil), ClaudeRadar: radar}
	key := &storage.GatewayDownstreamKey{
		App:                  storage.GatewayAppClaude,
		ModelFamily:          "claude",
		CandidateGroupID:     2,
		ModelMappingStrategy: "iq",
	}
	preview := gatewayModelMappingPreview(deps, key, &storage.GatewayCandidateGroup{ID: 2, ModelMappingFamily: "claude"})
	if preview == nil {
		t.Fatal("preview is nil")
	}
	if preview.ConfigKey != "m5" || preview.Label != "Fable 5 xhigh" {
		t.Fatalf("preview reference = %+v, want active m5/Fable 5 xhigh", preview)
	}
	if preview.Model != "" || preview.ReasoningEffort != "" {
		t.Fatalf("preview invented executable mapping = %+v", preview)
	}
	if preview.Error != errClaudeRadarModelBindingRequired.Error() {
		t.Fatalf("preview error = %q, want %q", preview.Error, errClaudeRadarModelBindingRequired)
	}
}

func TestClaudeRadarMappingOptionsFailClosedForHistoricalOnlyPayload(t *testing.T) {
	radarServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"iq":{"models":[{"key":"m2","name":"Sonnet 5 max","historical_only":true,"score":120}]}}`))
	}))
	defer radarServer.Close()

	radar := clauderadar.New(clauderadar.Options{URL: radarServer.URL, Client: radarServer.Client()})
	if _, err := radar.Refresh(context.Background()); err != nil {
		t.Fatalf("refresh Claude radar: %v", err)
	}
	snapshot := radar.Snapshot()
	if len(snapshot.History) != 1 || snapshot.History[0].ConfigKey != "m2" {
		t.Fatalf("snapshot history = %+v, want historical m2", snapshot.History)
	}
	if options := claudeRadarMappingOptions(snapshot); len(options) != 0 {
		t.Fatalf("mapping options = %+v, want empty", options)
	}

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	store := storage.NewGateway(db)
	group := &storage.GatewayCandidateGroup{Name: "claude-historical-only", ModelMappingFamily: "claude"}
	if err := db.Create(group).Error; err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	result := newGatewayModelMappingResult("claude-sonnet", "", []byte(`{"model":"claude-sonnet","messages":[]}`))
	_, err = resolveGatewayModelMapping(
		&Deps{Gateway: gateway.NewService(nil, nil, store, nil, nil, nil), ClaudeRadar: radar},
		&storage.GatewayDownstreamKey{
			App:                  storage.GatewayAppClaude,
			ModelFamily:          "claude",
			CandidateGroupID:     group.ID,
			ModelMappingStrategy: "iq",
		},
		result,
		"/v1/messages",
	)
	if err == nil {
		t.Fatal("request-level Claude mapping succeeded without an active payload model")
	}
}

func TestGatewayCandidatesWithManualMappingFiltersWithoutRescoring(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	for _, seed := range []struct {
		name   string
		group  string
		ratio  float64
		models string
	}{
		{name: "first", group: "priority", ratio: 0.1, models: "gpt-4.1"},
		{name: "second", group: "compatible", ratio: 0.2, models: "gpt-5.4"},
		{name: "third", group: "fallback", ratio: 0.3, models: "gpt-5.4"},
		{name: "later", group: "later-incompatible", ratio: 0.4, models: "gpt-4.0"},
	} {
		channel := storage.Channel{Name: seed.name, Type: storage.ChannelTypeNewAPI, SiteURL: "https://" + seed.name + ".example", PasswordCipher: "cipher", MonitorEnabled: true}
		if err := channels.Create(&channel); err != nil {
			t.Fatalf("create %s channel: %v", seed.name, err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: seed.group, Ratio: seed.ratio}); err != nil {
			t.Fatalf("upsert %s rate: %v", seed.name, err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: seed.group, OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
			APIKeyCipher: "cipher", ModelFamilies: "codex", ModelIDs: seed.models, Enabled: true,
		}); err != nil {
			t.Fatalf("upsert %s upstream key: %v", seed.name, err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, seed.group, "codex", seed.models, time.Now(), ""); err != nil {
			t.Fatalf("synchronize %s upstream models: %v", seed.name, err)
		}
	}
	allChannels, err := channels.List()
	if err != nil {
		t.Fatalf("list channels: %v", err)
	}
	group := &storage.GatewayCandidateGroup{Name: "mapped", ModelMappingFamily: "codex"}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{
		{ChannelID: allChannels[0].ID, GroupName: "priority"},
		{ChannelID: allChannels[1].ID, GroupName: "compatible"},
		{ChannelID: allChannels[2].ID, GroupName: "fallback"},
		{ChannelID: allChannels[3].ID, GroupName: "later-incompatible"},
	}); err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	downstream := &storage.GatewayDownstreamKey{
		Name:                        "mapped-key",
		App:                         storage.GatewayAppCodex,
		ModelFamily:                 "codex",
		CandidateGroupID:            group.ID,
		ModelMappingStrategy:        "manual",
		ModelMappingModel:           "gpt-5.4",
		ModelMappingReasoningEffort: "high",
		KeyHash:                     "mapping-key",
		APIKeyCipher:                "cipher",
		Enabled:                     true,
		SmartRouting:                true,
	}
	if err := store.CreateDownstreamKey(downstream); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	decision, mapping, err := gatewayCandidatesWithModelMapping(
		&Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)},
		downstream,
		"gpt-4.1",
		"low",
		[]byte(`{"model":"gpt-4.1","reasoning":{"effort":"low"}}`),
		"/v1/responses",
		"",
	)
	if err != nil {
		t.Fatalf("resolve mapped candidates: %v", err)
	}
	if !mapping.Applied || mapping.Model != "gpt-5.4" || mapping.RequestedBody != nil || len(decision.Candidates) != 2 || decision.Candidates[0].Channel.Name != "second" {
		t.Fatalf("mapping=%+v candidates=%+v", mapping, decision.Candidates)
	}
	if len(decision.Exclusions) != 2 || decision.Exclusions[0].ChannelName != "first" || decision.Exclusions[0].Reason != "model_not_listed" || decision.Exclusions[1].ChannelName != "later" || decision.Exclusions[1].Reason != "model_not_listed" {
		t.Fatalf("mapping exclusions=%+v", decision.Exclusions)
	}
	details := gatewayRouteDetails(decision, nil, decision.Candidates[0])
	for _, want := range []string{"mapped-model eligible priority #1", "pre-mapping priority #2", "higher pre-mapping priority #1 first/priority filtered"} {
		if !strings.Contains(details, want) {
			t.Fatalf("mapped route details %q missing %q", details, want)
		}
	}
	if strings.Contains(details, "higher pre-mapping priority #4 later/later-incompatible filtered") {
		t.Fatalf("later incompatible candidate was described as causal: %q", details)
	}
	if got := gatewayRequestModel(mapping.Body); got != "gpt-5.4" {
		t.Fatalf("rewritten model = %q", got)
	}
	if err := store.AppendRequestLog(&storage.GatewayRequestLog{
		DownstreamKeyID: downstream.ID, ChannelID: allChannels[2].ID, GroupName: "fallback", ModelName: "gpt-5.4",
		SessionHash: "mapping-session", Method: "POST", Path: "/v1/responses", StatusCode: 200, RouteReason: gatewayRouteReasonFallback, RouteSelectionRank: 2,
		RouteDetails: "attempt failed second/compatible: HTTP 503; selected third/fallback (eligible priority #2)",
		CreatedAt:    time.Now(),
	}); err != nil {
		t.Fatalf("append mapped fallback route: %v", err)
	}
	decision, mapping, err = gatewayCandidatesWithModelMapping(
		&Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)},
		downstream,
		"gpt-4.1",
		"low",
		[]byte(`{"model":"gpt-4.1","reasoning":{"effort":"low"}}`),
		"/v1/responses",
		"mapping-session",
	)
	if err != nil || !mapping.Applied || decision.AffinityApplied || len(decision.Candidates) != 2 || decision.Candidates[0].Channel.Name != "second" {
		t.Fatalf("mapped session affinity cost guard = decision=%+v mapping=%+v err=%v", decision, mapping, err)
	}

	if err := store.MarkModelCapability(allChannels[0].ID, "priority", "gpt-5.4", false, "stale negative fact"); err != nil {
		t.Fatalf("seed stale negative model capability: %v", err)
	}
	downstream.SmartRouting = false
	downstream.ManualChannelID = allChannels[0].ID
	downstream.ManualGroupName = "priority"
	if err := store.UpdateDownstreamKey(downstream); err != nil {
		t.Fatalf("save fixed mapped route: %v", err)
	}
	decision, mapping, err = gatewayCandidatesWithModelMapping(
		&Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)},
		downstream,
		"gpt-4.1",
		"low",
		[]byte(`{"model":"gpt-4.1","reasoning":{"effort":"low"}}`),
		"/v1/responses",
		"mapping-session",
	)
	if err == nil {
		t.Fatalf("fixed mapped route accepted a model absent from its exact synchronized ModelIDs: decision=%+v mapping=%+v", decision, mapping)
	}
	if reason, ok := gateway.ManualRouteGateReasonOf(err); !ok || reason != gateway.ManualRouteGateReasonModelNotListed {
		t.Fatalf("fixed mapped route gate = %v (%v), want %q", reason, err, gateway.ManualRouteGateReasonModelNotListed)
	}
	if mapping.Applied || mapping.RequestedBody != nil {
		t.Fatalf("rejected fixed mapping was left executable: %+v", mapping)
	}
	// Missing model-sync evidence and a stale negative capability are not
	// structural incompatibilities. A fixed route remains one direct attempt in
	// that case; it must not acquire adaptive filtering or a fallback.
	if err := store.UpdateUpstreamKeyModelFamilies(allChannels[0].ID, "priority", "codex", "", time.Now().Add(time.Second), ""); err != nil {
		t.Fatalf("clear fixed route model-list evidence: %v", err)
	}
	if err := db.Model(&storage.GatewayModelCapability{}).
		Where("channel_id = ? AND lower(btrim(group_name)) = ? AND model_name = ?", allChannels[0].ID, "priority", "gpt-5.4").
		Update("last_checked_at", time.Now().Add(-25*time.Hour)).Error; err != nil {
		t.Fatalf("age fixed route negative capability: %v", err)
	}
	decision, mapping, err = gatewayCandidatesWithModelMapping(
		&Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)},
		downstream,
		"gpt-4.1",
		"low",
		[]byte(`{"model":"gpt-4.1","reasoning":{"effort":"low"}}`),
		"/v1/responses",
		"mapping-session",
	)
	if err != nil || !mapping.Applied || mapping.Model != "gpt-5.4" || mapping.RequestedBody != nil || decision.AffinityApplied || len(decision.Candidates) != 1 || decision.Candidates[0].Channel.Name != "first" {
		t.Fatalf("fixed mapped route rejected unknown/stale capability facts: decision=%+v mapping=%+v err=%v", decision, mapping, err)
	}
	downstream.SmartRouting = true
	if err := store.UpdateDownstreamKey(downstream); err != nil {
		t.Fatalf("restore smart mapped route: %v", err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(allChannels[0].ID, "priority", "codex", "gpt-4.1", time.Now().Add(2*time.Second), ""); err != nil {
		t.Fatalf("restore synchronized model-list evidence: %v", err)
	}

	if err := db.Model(&storage.GatewayDownstreamKey{}).Where("id = ?", downstream.ID).Update("model_mapping_model", "gpt-5.9").Error; err != nil {
		t.Fatalf("seed unsupported persisted mapping: %v", err)
	}
	downstream.ModelMappingModel = "gpt-5.9"
	decision, mapping, err = gatewayCandidatesWithModelMapping(
		&Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)},
		downstream,
		"gpt-4.1",
		"low",
		[]byte(`{"model":"gpt-4.1","reasoning":{"effort":"low"}}`),
		"/v1/responses",
		"",
	)
	if err != nil {
		t.Fatalf("unsupported mapped model did not disable mapping: %v", err)
	}
	if mapping.Applied || mapping.Model != "gpt-4.1" || mapping.RequestedBody != nil || gatewayRequestModel(mapping.Body) != "gpt-4.1" || mapping.Details == "" || len(decision.Candidates) != 1 || decision.Candidates[0].Channel.Name != "first" {
		t.Fatalf("failed mapping=%+v candidates=%+v", mapping, decision.Candidates)
	}
	stored, err := store.FindDownstreamKey(downstream.ID)
	if err != nil {
		t.Fatalf("find disabled mapping key: %v", err)
	}
	if stored.ModelMappingStrategy != string(gateway.ModelMappingStrategyDisabled) {
		t.Fatalf("mapping strategy = %q, want off", stored.ModelMappingStrategy)
	}

	downstream.ModelMappingModel = "gpt-5.4"
	decision, mapping, err = gatewayCandidatesWithModelMapping(
		&Deps{Gateway: gateway.NewService(channels, rates, store, nil, nil, nil)},
		downstream,
		"gpt-4.1",
		"low",
		[]byte(`{"model":"gpt-4.1","input":"embedding text"}`),
		"/v1/embeddings",
		"",
	)
	if err != nil {
		t.Fatalf("resolve non-chat candidates: %v", err)
	}
	if mapping.Applied || mapping.Strategy != gateway.ModelMappingStrategyDisabled || mapping.Model != "gpt-4.1" || gatewayRequestModel(mapping.Body) != "gpt-4.1" || len(decision.Candidates) != 1 || decision.Candidates[0].Channel.Name != "first" {
		t.Fatalf("non-chat mapping=%+v candidates=%+v", mapping, decision.Candidates)
	}
}

func TestMappedModelAdmissionAcceptsExactDirectAliasInCandidateGroup(t *testing.T) {
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	channel := &storage.Channel{
		Name: "direct mapped alias", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://direct-mapped-alias.invalid", PasswordCipher: "cipher",
		CredentialMode: storage.CredentialModeDirect, GatewayApp: storage.GatewayAppClaude,
		DirectModels: storage.EncodeDirectModels([]string{"vendor-alias"}), MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.NewRates(db).Upsert(&storage.RateSnapshot{
		ChannelID: channel.ID, ModelName: storage.DirectChannelGroupName,
		Ratio: 1, CompletionRatio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	store := storage.NewGateway(db)
	credential := &storage.ChannelDirectCredential{
		ChannelID: channel.ID, GroupName: storage.DirectChannelGroupName,
		BaseURL: "https://direct-mapped-alias.invalid", APIKeyCipher: "cipher",
		Models: storage.EncodeDirectModels([]string{"vendor-alias"}),
		Ratio:  1, CompletionRatio: 1, Enabled: true, Revision: 1,
	}
	if err := db.Create(credential).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertDirectUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: storage.DirectChannelGroupName,
		UpstreamKeyName: "direct", APIKeyCipher: "cipher", Enabled: true,
		DirectCredentialID: &credential.ID, DirectCredentialRevision: credential.Revision,
		DirectBaseURL: credential.BaseURL, ModelFamilies: "claude", ModelIDs: "vendor-alias",
	}, channel.UpstreamAccountIdentityRevision); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, storage.DirectChannelGroupName, "added", 0, "direct", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	group := &storage.GatewayCandidateGroup{Name: "direct mapped alias group", ModelMappingFamily: "claude"}
	if err := store.SaveCandidateGroup(group, []storage.GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: storage.DirectChannelGroupName}}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	downstream := &storage.GatewayDownstreamKey{App: storage.GatewayAppClaude, ModelFamily: "claude", CandidateGroupID: group.ID}
	deps := &Deps{Channels: channels, Gateway: gateway.NewService(channels, storage.NewRates(db), store, nil, nil, nil)}
	if !gatewayMappedModelAllowedForCandidateGroup(deps, downstream, loaded, "vendor-alias") {
		t.Fatal("exact direct alias was rejected before candidate filtering")
	}
	if gatewayMappedModelAllowedForCandidateGroup(deps, downstream, loaded, "Vendor-Alias") {
		t.Fatal("case-mismatched direct alias was accepted")
	}
}
