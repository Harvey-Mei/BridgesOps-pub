package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/auth"
	"github.com/Harvey-Mei/BridgeOps/backend/codexradar"
	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/runtimeconfig"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayManualMappedTargetPreviewMissingCandidateGroupUsesSafeUnresolvedContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	store := storage.NewGateway(db)
	key := &storage.GatewayDownstreamKey{
		Name: "manual mapped missing group", App: storage.GatewayAppCodex, ModelFamily: "codex",
		ModelMappingStrategy: string(gateway.ModelMappingStrategyManual), ModelMappingModel: "server-only-gpt-5.5", ModelMappingReasoningEffort: "high",
		KeyHash: "manual-mapped-missing-group-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	// The regular configuration path prevents this dangling reference. Keep a
	// legacy/race state in the test so the HTTP contract owns its safe fallback.
	if err := db.Model(&storage.GatewayDownstreamKey{}).Where("id = ?", key.ID).Updates(map[string]any{
		"candidate_group_id":    uint(987654321),
		"candidate_target_mode": "candidate_group",
	}).Error; err != nil {
		t.Fatalf("make candidate group reference missing: %v", err)
	}

	service := gateway.NewService(storage.NewChannels(db), storage.NewRates(db), store, nil, nil, nil)
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: service})
	path := "/api/gateway/manual-mapped-target-preview?downstream_key_id=" + strconv.FormatUint(uint64(key.ID), 10)

	baseline := httptest.NewRecorder()
	router.ServeHTTP(baseline, httptest.NewRequest(http.MethodGet, path, nil))
	baselineOutput, baselineData := mustGatewayManualMappedTargetPreviewOutput(t, baseline)
	assertGatewayManualMappedTargetUnresolved(t, baselineOutput, baselineData)

	// Only downstream_key_id is authoritative. These values deliberately look
	// like useful target/model fields but must neither influence nor be echoed
	// by the server-owned projection.
	injectedPath := path + "&model=client-secret-model&requested_model=client-secret-model&channel_id=444&candidate_member_id=555&upstream_group_id=666&group_name=private-group"
	injected := httptest.NewRecorder()
	router.ServeHTTP(injected, httptest.NewRequest(http.MethodGet, injectedPath, nil))
	injectedOutput, injectedData := mustGatewayManualMappedTargetPreviewOutput(t, injected)
	assertGatewayManualMappedTargetUnresolved(t, injectedOutput, injectedData)
	if !reflect.DeepEqual(baselineOutput, injectedOutput) || !reflect.DeepEqual(baselineData, injectedData) {
		t.Fatalf("injected query changed server-owned preview: baseline=%+v/%s injected=%+v/%s", baselineOutput, baseline.Body.String(), injectedOutput, injected.Body.String())
	}

	for _, secret := range []string{
		"manual mapped target candidate group no longer exists",
		"server-only-gpt-5.5",
		"client-secret-model",
		"private-group",
		"987654321",
	} {
		if strings.Contains(injected.Body.String(), secret) {
			t.Fatalf("preview leaked internal or injected value %q: %s", secret, injected.Body.String())
		}
	}
}

func TestGatewayManualMappedTargetPreviewManualMappingWithoutValidReasoningIsUnresolved(t *testing.T) {
	for _, test := range []struct {
		name   string
		effort string
	}{
		{name: "missing reasoning", effort: ""},
		{name: "invalid reasoning", effort: "invalid-effort"},
	} {
		t.Run(test.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			db := openTestDB(t)
			channels := storage.NewChannels(db)
			rates := storage.NewRates(db)
			store := storage.NewGateway(db)
			service := gateway.NewService(channels, rates, store, nil, nil, nil)
			channel := &storage.Channel{
				Name: "manual reasoning preview " + test.name, Type: storage.ChannelTypeNewAPI,
				SiteURL: "https://manual-reasoning-preview.example", MonitorEnabled: true,
			}
			if err := channels.Create(channel); err != nil {
				t.Fatalf("create channel: %v", err)
			}
			if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1}); err != nil {
				t.Fatalf("save candidate rate: %v", err)
			}
			if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
				ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "cipher", APIKeyPrefix: "sk",
				OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted, Enabled: true, LatencyProbeEnabled: true,
			}); err != nil {
				t.Fatalf("create candidate upstream key: %v", err)
			}
			group, err := service.SaveCandidateGroupWithMappingFamily(0, "manual reasoning preview", "codex", []storage.GatewayCandidateGroupMember{{
				ChannelID: channel.ID, GroupName: "primary",
			}})
			if err != nil {
				t.Fatalf("save candidate group: %v", err)
			}
			key := &storage.GatewayDownstreamKey{
				Name: "manual reasoning mismatch", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
				ModelMappingStrategy: string(gateway.ModelMappingStrategyManual), ModelMappingModel: "server-only-manual-model", ModelMappingReasoningEffort: test.effort,
				KeyHash: "manual-reasoning-" + strings.ReplaceAll(test.name, " ", "-"), KeyPrefix: "sk", Enabled: true, SmartRouting: true,
			}
			if err := store.CreateDownstreamKey(key); err != nil {
				t.Fatalf("create downstream key: %v", err)
			}

			router := gin.New()
			registerGateway(router.Group("/api"), &Deps{Gateway: service})
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet,
				"/api/gateway/manual-mapped-target-preview?downstream_key_id="+strconv.FormatUint(uint64(key.ID), 10), nil))
			output, data := mustGatewayManualMappedTargetPreviewOutput(t, recorder)
			if output.MappingState != gatewayManualMappedTargetMappingStateUnresolved || output.GateProvenance != gatewayManualMappedTargetUnresolvedProvenance {
				t.Fatalf("manual mapping with %s advertised as resolved: %+v body=%s", test.name, output, recorder.Body.String())
			}
			if _, exists := data["error"]; exists || strings.Contains(recorder.Body.String(), "server-only-manual-model") {
				t.Fatalf("manual unresolved preview leaked mapping detail: %s", recorder.Body.String())
			}
			for _, target := range output.Targets {
				if target.ManualMappedModelGate || target.GateProvenance != gatewayManualMappedTargetUnresolvedProvenance {
					t.Fatalf("manual unresolved target retained exact-model gate: %+v", target)
				}
			}
		})
	}
}

func TestGatewayManualMappedTargetPreviewAutomaticMappingRejectsDataPlaneModelMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	service := gateway.NewService(channels, rates, store, nil, nil, nil)
	channel := &storage.Channel{
		Name: "automatic mapping preview", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://automatic-mapping-preview.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1}); err != nil {
		t.Fatalf("save candidate rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", APIKeyCipher: "cipher", APIKeyPrefix: "sk",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted, Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("create candidate upstream key: %v", err)
	}
	group, err := service.SaveCandidateGroupWithMappingFamily(0, "automatic mapping preview", "codex", []storage.GatewayCandidateGroupMember{{
		ChannelID: channel.ID, GroupName: "primary",
	}})
	if err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	key := &storage.GatewayDownstreamKey{
		Name: "automatic mapping mismatch", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
		ModelMappingStrategy: string(gateway.ModelMappingStrategyIQ), KeyHash: "automatic-mapping-mismatch-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	radarServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model_iq":{"comparisons":{"wrong-family":{"label":"wrong family","latest":{"model":"gemini-2.5-pro","reasoning_effort":"high","score":99}}}}}`))
	}))
	t.Cleanup(radarServer.Close)
	radar := codexradar.New(codexradar.Options{URL: radarServer.URL, Client: radarServer.Client()})
	if _, err := radar.Refresh(context.Background()); err != nil {
		t.Fatalf("seed codex radar: %v", err)
	}

	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: service, CodexRadar: radar})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet,
		"/api/gateway/manual-mapped-target-preview?downstream_key_id="+strconv.FormatUint(uint64(key.ID), 10), nil))
	output, data := mustGatewayManualMappedTargetPreviewOutput(t, recorder)
	if output.MappingState != gatewayManualMappedTargetMappingStateUnresolved || output.GateProvenance != gatewayManualMappedTargetUnresolvedProvenance {
		t.Fatalf("automatic mismatched model advertised as resolved: %+v body=%s", output, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "gemini-2.5-pro") {
		t.Fatalf("automatic preview leaked rejected mapped model: %s", recorder.Body.String())
	}
	if _, exists := data["error"]; exists {
		t.Fatalf("automatic unresolved preview leaked error field: %s", recorder.Body.String())
	}
	for _, target := range output.Targets {
		if target.ManualMappedModelGate || target.GateProvenance != gatewayManualMappedTargetUnresolvedProvenance {
			t.Fatalf("automatic mismatched target retained exact-model gate: %+v", target)
		}
	}
}

func TestGatewayManualMappedTargetPreviewAutomaticCodexMappingUsesSharedAdmissionAndResolves(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	service := gateway.NewService(channels, rates, store, nil, nil, nil)
	channel := &storage.Channel{
		Name: "automatic mapping resolved", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://automatic-mapping-resolved.example", MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1}); err != nil {
		t.Fatalf("save candidate rate: %v", err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 1, UpstreamKeyName: "managed-primary",
		APIKeyCipher: "cipher", APIKeyPrefix: "sk", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		Enabled: true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("create upstream key: %v", err)
	}
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "managed-primary", observedAt); err != nil {
		t.Fatalf("record upstream group: %v", err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
	if err != nil || !generation.Valid() {
		t.Fatalf("capture upstream generation: generation=%+v err=%v", generation, err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", "gpt-5.4", generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
		t.Fatalf("save synchronized model facts: %v", err)
	}
	group, err := service.SaveCandidateGroupWithMappingFamily(0, "automatic mapping resolved", "codex", []storage.GatewayCandidateGroupMember{{
		ChannelID: channel.ID, GroupName: "primary",
	}})
	if err != nil {
		t.Fatalf("save candidate group: %v", err)
	}
	key := &storage.GatewayDownstreamKey{
		Name: "automatic mapping resolved", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
		ModelMappingStrategy: string(gateway.ModelMappingStrategyIQ), KeyHash: "automatic-mapping-resolved-key", KeyPrefix: "sk", Enabled: true, SmartRouting: true,
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create downstream key: %v", err)
	}

	radarServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model_iq":{"comparisons":{"codex":{"label":"Codex","latest":{"model":"gpt-5.4","reasoning_effort":"high","score":99}}}}}`))
	}))
	t.Cleanup(radarServer.Close)
	radar := codexradar.New(codexradar.Options{URL: radarServer.URL, Client: radarServer.Client()})
	if _, err := radar.Refresh(context.Background()); err != nil {
		t.Fatalf("seed codex radar: %v", err)
	}

	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: service, CodexRadar: radar})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet,
		"/api/gateway/manual-mapped-target-preview?downstream_key_id="+strconv.FormatUint(uint64(key.ID), 10), nil))
	output, data := mustGatewayManualMappedTargetPreviewOutput(t, recorder)
	if output.MappingState != gatewayManualMappedTargetMappingStateResolved || output.GateProvenance != gatewayManualMappedTargetGateProvenance {
		t.Fatalf("automatic Codex mapping did not resolve: %+v body=%s", output, recorder.Body.String())
	}
	if len(output.Targets) != 1 {
		t.Fatalf("automatic Codex targets=%+v", output.Targets)
	}
	target := output.Targets[0]
	if target.CandidateMemberID == 0 || target.ChannelID != channel.ID || target.GroupName != "primary" || target.StructuralBlocked || !target.ManualMappedModelGate || target.GateProvenance != gatewayManualMappedTargetGateProvenance || target.Reason != "" {
		t.Fatalf("automatic Codex target=%+v", target)
	}
	assertGatewayManualMappedTargetNestedAllowlist(t, data, recorder.Body.String())
	if strings.Contains(recorder.Body.String(), "gpt-5.4") {
		t.Fatalf("automatic preview leaked server-resolved model: %s", recorder.Body.String())
	}
}

func TestGatewayAutomaticMappedFixedTargetPATCHRevalidatesServerResolvedModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	service := gateway.NewService(channels, rates, store, nil, nil, nil)

	addTarget := func(name, modelIDs string) (*storage.Channel, storage.UpstreamGroupGeneration) {
		t.Helper()
		channel := &storage.Channel{
			Name: name, Type: storage.ChannelTypeNewAPI,
			SiteURL: "https://" + strings.ReplaceAll(name, " ", "-") + ".example", MonitorEnabled: true,
		}
		if err := channels.Create(channel); err != nil {
			t.Fatalf("create %s channel: %v", name, err)
		}
		if _, err := rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: "primary", Ratio: 1}); err != nil {
			t.Fatalf("save %s rate: %v", name, err)
		}
		if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
			ChannelID: channel.ID, GroupName: "primary", UpstreamKeyID: 1, UpstreamKeyName: "managed-primary",
			APIKeyCipher: "cipher", APIKeyPrefix: "sk", OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
			Enabled: true, LatencyProbeEnabled: true,
		}); err != nil {
			t.Fatalf("create %s upstream key: %v", name, err)
		}
		observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
		if err := store.RecordUpstreamGroupChange(channel.ID, "primary", "added", 1, "managed-primary", observedAt); err != nil {
			t.Fatalf("record %s upstream group: %v", name, err)
		}
		generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "primary")
		if err != nil || !generation.Valid() {
			t.Fatalf("capture %s upstream generation: generation=%+v err=%v", name, generation, err)
		}
		if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "primary", "codex", modelIDs, generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
			t.Fatalf("save %s synchronized model facts: %v", name, err)
		}
		return channel, generation
	}

	compatible, compatibleGeneration := addTarget("automatic save compatible", "gpt-5.4")
	incompatible, incompatibleGeneration := addTarget("automatic save incompatible", "gpt-4.1")
	group, err := service.SaveCandidateGroupWithMappingFamily(0, "automatic save targets", "codex", []storage.GatewayCandidateGroupMember{
		{ChannelID: compatible.ID, GroupName: "primary", UpstreamGroupID: compatibleGeneration.ID, PresenceGeneration: compatibleGeneration.PresenceGeneration},
		{ChannelID: incompatible.ID, GroupName: "primary", UpstreamGroupID: incompatibleGeneration.ID, PresenceGeneration: incompatibleGeneration.PresenceGeneration},
	})
	if err != nil {
		t.Fatalf("save automatic candidate group: %v", err)
	}
	automaticKey := &storage.GatewayDownstreamKey{
		Name: "automatic fixed route change", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
		ModelMappingStrategy: string(gateway.ModelMappingStrategyIQ), KeyHash: "automatic-fixed-route-change", KeyPrefix: "sk", Enabled: true,
		SmartRouting: false, ManualChannelID: compatible.ID, ManualGroupName: "primary",
	}
	if err := store.CreateDownstreamKey(automaticKey); err != nil {
		t.Fatalf("create automatic fixed key: %v", err)
	}
	automaticKey, err = store.PatchDownstreamKey(automaticKey.ID, func(current *storage.GatewayDownstreamKey) error {
		current.SmartRouting = false
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("configure automatic fixed key: %v", err)
	}
	disabledKey := &storage.GatewayDownstreamKey{
		Name: "automatic mapping enable", App: storage.GatewayAppCodex, ModelFamily: "codex", CandidateGroupID: group.ID,
		ModelMappingStrategy: string(gateway.ModelMappingStrategyDisabled), KeyHash: "automatic-mapping-enable", KeyPrefix: "sk", Enabled: true,
		SmartRouting: false, ManualChannelID: incompatible.ID, ManualGroupName: "primary",
	}
	if err := store.CreateDownstreamKey(disabledKey); err != nil {
		t.Fatalf("create disabled-mapping fixed key: %v", err)
	}
	disabledKey, err = store.PatchDownstreamKey(disabledKey.ID, func(current *storage.GatewayDownstreamKey) error {
		current.SmartRouting = false
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("configure disabled-mapping fixed key: %v", err)
	}

	radarServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model_iq":{"comparisons":{"codex":{"label":"Codex","latest":{"model":"gpt-5.4","reasoning_effort":"high","score":99}}}}}`))
	}))
	t.Cleanup(radarServer.Close)
	radar := codexradar.New(codexradar.Options{URL: radarServer.URL, Client: radarServer.Client()})
	if _, err := radar.Refresh(context.Background()); err != nil {
		t.Fatalf("seed codex radar: %v", err)
	}
	router := gin.New()
	registerGateway(router.Group("/api"), &Deps{Gateway: service, CodexRadar: radar})

	patch := func(keyID uint, body string) *httptest.ResponseRecorder {
		t.Helper()
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPatch, "/api/gateway/downstream-keys/"+strconv.FormatUint(uint64(keyID), 10), strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(recorder, request)
		return recorder
	}

	routeChange := patch(automaticKey.ID, `{"manual_channel_id":`+strconv.FormatUint(uint64(incompatible.ID), 10)+`,"manual_group_name":"primary"}`)
	if routeChange.Code != http.StatusBadRequest || !strings.Contains(routeChange.Body.String(), "automatic mapped route target is structurally blocked") {
		t.Fatalf("automatic route-only mismatch status=%d body=%s", routeChange.Code, routeChange.Body.String())
	}
	storedAutomatic, err := store.FindDownstreamKey(automaticKey.ID)
	if err != nil {
		t.Fatalf("reload rejected automatic route change: %v", err)
	}
	if storedAutomatic.ManualChannelID != compatible.ID || storedAutomatic.ManualGroupName != "primary" || storedAutomatic.ModelMappingStrategy != string(gateway.ModelMappingStrategyIQ) {
		t.Fatalf("rejected automatic route change was not atomic: %+v", storedAutomatic)
	}

	enableMapping := patch(disabledKey.ID, `{"model_mapping_strategy":"iq"}`)
	if enableMapping.Code != http.StatusBadRequest || !strings.Contains(enableMapping.Body.String(), "automatic mapped route target is structurally blocked") {
		t.Fatalf("automatic mapping enable mismatch status=%d body=%s", enableMapping.Code, enableMapping.Body.String())
	}
	storedDisabled, err := store.FindDownstreamKey(disabledKey.ID)
	if err != nil {
		t.Fatalf("reload rejected automatic mapping enable: %v", err)
	}
	if storedDisabled.ModelMappingStrategy != string(gateway.ModelMappingStrategyDisabled) || storedDisabled.ManualChannelID != incompatible.ID {
		t.Fatalf("rejected automatic mapping enable was not atomic: %+v", storedDisabled)
	}
}

func TestRegisteredGatewayManualMappedTargetPreviewRejectsAnonymousRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	authService, err := auth.New("admin", "password", "manual-mapped-preview-secret", time.Hour)
	if err != nil {
		t.Fatalf("create auth service: %v", err)
	}
	runtime := runtimeconfig.New("", "", nil, nil, nil, authService, nil, config.ProxyConfig{}, config.UpstreamConfig{}, nil)
	router := gin.New()
	Register(router, &Deps{
		Runtime: runtime,
		Gateway: gateway.NewService(nil, nil, storage.NewGateway(db), nil, nil, nil),
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/gateway/manual-mapped-target-preview?downstream_key_id=1", nil))
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous mapped-target preview status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func mustGatewayManualMappedTargetPreviewOutput(t *testing.T, recorder *httptest.ResponseRecorder) (gatewayManualMappedTargetPreviewOutput, map[string]json.RawMessage) {
	t.Helper()
	if recorder.Code != http.StatusOK {
		t.Fatalf("preview status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode preview envelope: %v body=%s", err, recorder.Body.String())
	}
	if len(envelope) != 1 {
		t.Fatalf("preview envelope is not allowlisted: %s", recorder.Body.String())
	}
	rawData, ok := envelope["data"]
	if !ok {
		t.Fatalf("preview envelope omitted data: %s", recorder.Body.String())
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(rawData, &data); err != nil {
		t.Fatalf("decode preview data: %v body=%s", err, recorder.Body.String())
	}
	allowed := map[string]struct{}{
		"mapping_state":   {},
		"gate_provenance": {},
		"targets":         {},
	}
	for field := range data {
		if _, ok := allowed[field]; !ok {
			t.Fatalf("preview data leaked non-allowlisted field %q: %s", field, recorder.Body.String())
		}
	}
	if len(data) != len(allowed) {
		t.Fatalf("preview data omitted required allowlisted field: %s", recorder.Body.String())
	}
	var output gatewayManualMappedTargetPreviewOutput
	if err := json.Unmarshal(rawData, &output); err != nil {
		t.Fatalf("decode preview output: %v body=%s", err, recorder.Body.String())
	}
	return output, data
}

func assertGatewayManualMappedTargetUnresolved(t *testing.T, output gatewayManualMappedTargetPreviewOutput, data map[string]json.RawMessage) {
	t.Helper()
	if output.MappingState != gatewayManualMappedTargetMappingStateUnresolved || output.GateProvenance != gatewayManualMappedTargetUnresolvedProvenance {
		t.Fatalf("preview unresolved state=%+v", output)
	}
	if output.Targets == nil || len(output.Targets) != 0 || string(data["targets"]) != "[]" {
		t.Fatalf("unresolved preview targets must be concrete empty list: output=%+v raw=%s", output, data["targets"])
	}
}

func assertGatewayManualMappedTargetNestedAllowlist(t *testing.T, data map[string]json.RawMessage, body string) {
	t.Helper()
	var targets []map[string]json.RawMessage
	if err := json.Unmarshal(data["targets"], &targets); err != nil {
		t.Fatalf("decode preview targets: %v body=%s", err, body)
	}
	allowed := map[string]struct{}{
		"candidate_member_id":      {},
		"channel_id":               {},
		"group_name":               {},
		"structural_blocked":       {},
		"manual_mapped_model_gate": {},
		"gate_provenance":          {},
		"reason":                   {},
	}
	for _, target := range targets {
		for field := range target {
			if _, ok := allowed[field]; !ok {
				t.Fatalf("preview target leaked non-allowlisted field %q: %s", field, body)
			}
		}
	}
}
