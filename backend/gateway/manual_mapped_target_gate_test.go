package gateway

import (
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

type manualMappedTargetGateFixture struct {
	service  *Service
	channels *storage.Channels
	rates    *storage.Rates
	store    *storage.Gateway
}

func newManualMappedTargetGateFixture(t *testing.T) manualMappedTargetGateFixture {
	t.Helper()
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	return manualMappedTargetGateFixture{
		service:  NewService(channels, rates, store, nil, nil, nil),
		channels: channels,
		rates:    rates,
		store:    store,
	}
}

func (f manualMappedTargetGateFixture) addTarget(t *testing.T, channelName, groupName, modelIDs string) (*storage.Channel, storage.UpstreamGroupGeneration) {
	return f.addTargetWithMode(t, channelName, groupName, modelIDs, storage.CredentialModePassword)
}

func (f manualMappedTargetGateFixture) addTargetWithMode(t *testing.T, channelName, groupName, modelIDs string, credentialMode storage.CredentialMode) (*storage.Channel, storage.UpstreamGroupGeneration) {
	t.Helper()
	channel := &storage.Channel{
		Name: channelName, Type: storage.ChannelTypeNewAPI,
		SiteURL:        "https://" + strings.ReplaceAll(channelName, " ", "-") + ".example",
		PasswordCipher: "cipher", CredentialMode: credentialMode, MonitorEnabled: true,
	}
	if credentialMode == storage.CredentialModeDirect {
		channel.DirectModels = storage.EncodeDirectModels(strings.Split(modelIDs, ","))
	}
	if err := f.channels.Create(channel); err != nil {
		t.Fatalf("create channel %q: %v", channelName, err)
	}
	if _, err := f.rates.Upsert(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1, CompletionRatio: 1}); err != nil {
		t.Fatalf("upsert rate %q/%q: %v", channelName, groupName, err)
	}
	if err := f.store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: groupName, APIKeyCipher: "cipher", APIKeyPrefix: "sk",
		UpstreamKeyID: 1, UpstreamKeyName: "managed-" + channelName,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceAdopted,
		Enabled:             true, LatencyProbeEnabled: true,
	}); err != nil {
		t.Fatalf("upsert upstream key %q/%q: %v", channelName, groupName, err)
	}
	observedAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := f.store.RecordUpstreamGroupChange(channel.ID, groupName, "added", 1, "managed-"+channelName, observedAt); err != nil {
		t.Fatalf("record current generation %q/%q: %v", channelName, groupName, err)
	}
	generation, err := f.store.CaptureUpstreamGroupGeneration(channel.ID, groupName)
	if err != nil || !generation.Valid() {
		t.Fatalf("capture current generation %q/%q: generation=%+v err=%v", channelName, groupName, generation, err)
	}
	if err := f.store.UpdateUpstreamKeyModelFamilies(channel.ID, groupName, "codex", modelIDs, generation.DiscoveredAt.Add(time.Second), "", generation); err != nil {
		t.Fatalf("save current model facts %q/%q: %v", channelName, groupName, err)
	}
	return channel, generation
}

func TestDirectManualMappedTargetAcceptsExplicitVendorAlias(t *testing.T) {
	fixture := newManualMappedTargetGateFixture(t)
	target, _ := fixture.addTargetWithMode(t, "direct vendor alias", storage.DirectChannelGroupName, "vendor-alias", storage.CredentialModeDirect)
	group := mustSaveManualMappedTargetGroup(t, fixture, "direct vendor alias targets", []storage.GatewayCandidateGroupMember{{
		ChannelID: target.ID, GroupName: storage.DirectChannelGroupName,
	}})
	key := mustCreateManualMappedFixedKey(t, fixture, "direct vendor alias key", group, target.ID, storage.DirectChannelGroupName, "vendor-alias")

	projections, err := fixture.service.ManualMappedTargetGateProjectionsForDownstreamKey(key.ID)
	if err != nil || len(projections) != 1 || projections[0].StructuralBlocked || projections[0].Reason != "" {
		t.Fatalf("direct batch projection = %+v err=%v", projections, err)
	}
	projection, err := fixture.service.ManualMappedTargetGateProjectionForDownstreamKey(key.ID)
	if err != nil || projection.StructuralBlocked || projection.Reason != "" {
		t.Fatalf("direct fixed projection = %+v err=%v", projection, err)
	}
}

func TestDirectManualRouteUsesCurrentAllowlistWhenSynchronizedFactsAreStale(t *testing.T) {
	fixture := newManualMappedTargetGateFixture(t)
	target, _ := fixture.addTargetWithMode(t, "direct current allowlist", storage.DirectChannelGroupName, "cached-old", storage.CredentialModeDirect)
	target.DirectModels = storage.EncodeDirectModels([]string{"Vendor-Alias"})
	if err := fixture.channels.UpdateAdminFields(target, storage.ChannelAdminFieldDirectModels); err != nil {
		t.Fatalf("update direct allowlist: %v", err)
	}

	allowed, err := fixture.service.ManualCandidateForApp(0, storage.GatewayAppCodex, target.ID, storage.DirectChannelGroupName, "Vendor-Alias")
	if err != nil || len(allowed) != 1 {
		t.Fatalf("current direct allowlist was not authoritative: candidates=%+v err=%v", allowed, err)
	}
	if rejected, err := fixture.service.ManualCandidateForApp(0, storage.GatewayAppCodex, target.ID, storage.DirectChannelGroupName, "cached-old"); err == nil || len(rejected) != 0 {
		t.Fatalf("stale synchronized direct model was accepted: candidates=%+v err=%v", rejected, err)
	} else if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonModelNotListed {
		t.Fatalf("stale direct model gate reason = %q ok=%t err=%v", reason, ok, err)
	}
}

func mustSaveManualMappedTargetGroup(t *testing.T, f manualMappedTargetGateFixture, name string, members []storage.GatewayCandidateGroupMember) *storage.GatewayCandidateGroup {
	t.Helper()
	group, err := f.service.SaveCandidateGroupWithMappingFamily(0, name, "codex", members)
	if err != nil {
		t.Fatalf("save candidate group %q: %v", name, err)
	}
	return group
}

func mustCreateManualMappedFixedKey(t *testing.T, f manualMappedTargetGateFixture, name string, group *storage.GatewayCandidateGroup, channelID uint, groupName, model string) *storage.GatewayDownstreamKey {
	t.Helper()
	key := &storage.GatewayDownstreamKey{
		Name: name, App: storage.GatewayAppCodex, ModelFamily: "codex",
		CandidateGroupID: group.ID, ManualChannelID: channelID, ManualGroupName: groupName,
		ModelMappingStrategy: string(ModelMappingStrategyManual), ModelMappingModel: model, ModelMappingReasoningEffort: "high",
		KeyHash: name + "-hash", KeyPrefix: "sk", Enabled: true, SmartRouting: false,
	}
	if err := f.store.CreateDownstreamKey(key); err != nil {
		t.Fatalf("create manual mapped key %q: %v", name, err)
	}
	stored, err := f.store.PatchDownstreamKey(key.ID, func(current *storage.GatewayDownstreamKey) error {
		current.SmartRouting = false
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("configure manual mapped key %q: %v", name, err)
	}
	if stored.SmartRouting || stored.ModelMappingStrategy != string(ModelMappingStrategyManual) || stored.ModelMappingModel != model {
		t.Fatalf("manual mapped key %q persistence = %+v", name, stored)
	}
	return stored
}

func projectionForTarget(t *testing.T, projections []ManualMappedTargetGateProjection, channelID uint, groupName string) ManualMappedTargetGateProjection {
	t.Helper()
	for _, projection := range projections {
		if projection.ChannelID == channelID && projection.CanonicalGroupName == canonicalGroupName(groupName) {
			return projection
		}
	}
	t.Fatalf("target %d/%q missing from projections: %+v", channelID, groupName, projections)
	return ManualMappedTargetGateProjection{}
}

func TestManualMappedTargetGateProjectionsUsePersistedManualModelAndCurrentFacts(t *testing.T) {
	fixture := newManualMappedTargetGateFixture(t)
	first, firstGeneration := fixture.addTarget(t, "mapped first", "Primary", "gpt-5.5")
	second, secondGeneration := fixture.addTarget(t, "mapped second", "Secondary", "gpt-5.4")
	group := mustSaveManualMappedTargetGroup(t, fixture, "mapped projection targets", []storage.GatewayCandidateGroupMember{
		{ChannelID: first.ID, GroupName: "Primary"},
		{ChannelID: second.ID, GroupName: "Secondary"},
	})
	key := mustCreateManualMappedFixedKey(t, fixture, "mapped projection key", group, first.ID, "Primary", "gpt-5.5")

	plural, err := fixture.service.ManualMappedTargetGateProjectionsForDownstreamKey(key.ID)
	if err != nil || len(plural) != 2 {
		t.Fatalf("member projections = %+v err=%v", plural, err)
	}
	firstProjection := projectionForTarget(t, plural, first.ID, "Primary")
	if firstProjection.CandidateMemberID == 0 || firstProjection.StructuralBlocked || !firstProjection.ManualMappedModelGate || firstProjection.MappingUnresolved || firstProjection.Reason != "" {
		t.Fatalf("supported persisted mapped target projection = %+v", firstProjection)
	}
	secondProjection := projectionForTarget(t, plural, second.ID, "Secondary")
	if secondProjection.CandidateMemberID == 0 || !secondProjection.StructuralBlocked || !secondProjection.ManualMappedModelGate || secondProjection.MappingUnresolved || secondProjection.Reason != ManualRouteGateReasonModelNotListed {
		t.Fatalf("exact model mismatch projection = %+v", secondProjection)
	}

	singular, err := fixture.service.ManualMappedTargetGateProjectionForDownstreamKey(key.ID)
	if err != nil || singular.ChannelID != first.ID || singular.CanonicalGroupName != "primary" || singular.StructuralBlocked || !singular.ManualMappedModelGate || singular.MappingUnresolved {
		t.Fatalf("configured target projection = %+v err=%v", singular, err)
	}

	// Failed current sync preserves diagnostics but cannot prove an exact model
	// mismatch. The target therefore remains a direct fixed-route attempt.
	if err := fixture.store.RecordUpstreamKeyModelsError(second.ID, "Secondary", secondGeneration.DiscoveredAt.Add(2*time.Second), "temporary sync failure", secondGeneration); err != nil {
		t.Fatalf("record failed model sync: %v", err)
	}
	plural, err = fixture.service.ManualMappedTargetGateProjectionsForDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("projections after failed sync: %v", err)
	}
	secondProjection = projectionForTarget(t, plural, second.ID, "Secondary")
	if secondProjection.StructuralBlocked || !secondProjection.ManualMappedModelGate || secondProjection.Reason != "" {
		t.Fatalf("failed current sync remained an exact-model blocker: %+v", secondProjection)
	}

	// A current negative capability remains a structural target fact.
	if err := fixture.store.MarkModelCapability(first.ID, "Primary", "gpt-5.5", false, "confirmed unsupported", firstGeneration); err != nil {
		t.Fatalf("record current negative capability: %v", err)
	}
	singular, err = fixture.service.ManualMappedTargetGateProjectionForDownstreamKey(key.ID)
	if err != nil || !singular.StructuralBlocked || singular.Reason != ManualRouteGateReasonModelNotSupported || !singular.ManualMappedModelGate {
		t.Fatalf("current negative capability projection = %+v err=%v", singular, err)
	}

	// A same-name replacement establishes a new presence generation. Its old
	// model sync and negative capability facts cannot block the fixed target.
	replacedAt := time.Now().UTC().Add(time.Second).Truncate(time.Microsecond)
	if err := fixture.store.RecordUpstreamGroupChange(first.ID, "Primary", "removed", 1, "managed-mapped-first", replacedAt); err != nil {
		t.Fatalf("retire first target generation: %v", err)
	}
	if err := fixture.store.RecordUpstreamGroupChange(first.ID, "Primary", "added", 1, "managed-mapped-first", replacedAt.Add(time.Second)); err != nil {
		t.Fatalf("record replacement target generation: %v", err)
	}
	if _, err := fixture.service.SaveCandidateGroupWithMappingFamily(group.ID, group.Name, "codex", []storage.GatewayCandidateGroupMember{{ChannelID: first.ID, GroupName: "Primary"}}); err != nil {
		t.Fatalf("re-add replacement target to candidate group: %v", err)
	}
	singular, err = fixture.service.ManualMappedTargetGateProjectionForDownstreamKey(key.ID)
	if err != nil || singular.StructuralBlocked || singular.Reason != "" || !singular.ManualMappedModelGate {
		t.Fatalf("old-generation model facts blocked replacement target: %+v err=%v", singular, err)
	}
}

func TestManualMappedTargetGateProjectionKeepsLegacyMappingUnresolvedBaseline(t *testing.T) {
	fixture := newManualMappedTargetGateFixture(t)
	target, _ := fixture.addTarget(t, "legacy mapped target", "primary", "gpt-5.5")
	group := mustSaveManualMappedTargetGroup(t, fixture, "legacy mapped targets", []storage.GatewayCandidateGroupMember{{ChannelID: target.ID, GroupName: "primary"}})
	key := mustCreateManualMappedFixedKey(t, fixture, "legacy mapped key", group, target.ID, "primary", "")

	plural, err := fixture.service.ManualMappedTargetGateProjectionsForDownstreamKey(key.ID)
	if err != nil || len(plural) != 1 {
		t.Fatalf("legacy member projections = %+v err=%v", plural, err)
	}
	projection := plural[0]
	if projection.StructuralBlocked || projection.ManualMappedModelGate || !projection.MappingUnresolved || projection.Reason != "" {
		t.Fatalf("legacy mapping did not return unresolved baseline: %+v", projection)
	}
	if _, err := fixture.service.ManualMappedTargetGateProjectionForDownstreamKey(key.ID); err == nil || !strings.Contains(err.Error(), "manual model mapping requires a model") {
		t.Fatalf("strict configured-target projection accepted invalid manual mapping: %v", err)
	}
}

func TestFixedManualMappedTargetSaveUsesExactPersistedModel(t *testing.T) {
	fixture := newManualMappedTargetGateFixture(t)
	first, _ := fixture.addTarget(t, "save first", "first", "gpt-5.4")
	routeOnly, _ := fixture.addTarget(t, "save route only", "route-only", "gpt-5.3")
	replacement, _ := fixture.addTarget(t, "save replacement", "replacement", "gpt-5.4")
	primary := mustSaveManualMappedTargetGroup(t, fixture, "save primary", []storage.GatewayCandidateGroupMember{
		{ChannelID: first.ID, GroupName: "first"},
		{ChannelID: routeOnly.ID, GroupName: "route-only"},
	})
	replacementGroup := mustSaveManualMappedTargetGroup(t, fixture, "save replacement", []storage.GatewayCandidateGroupMember{{ChannelID: replacement.ID, GroupName: "replacement"}})
	key := mustCreateManualMappedFixedKey(t, fixture, "save mapped key", primary, first.ID, "first", "gpt-5.4")

	// A route-only PATCH must use the already persisted manual mapping model;
	// otherwise it could save a target whose current exact ModelIDs reject it.
	routeOnlyGroup := "route-only"
	if _, err := fixture.service.UpdateDownstreamKeyWithCandidateTargetAndMapping(key.ID, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, &routeOnly.ID, &routeOnlyGroup); err == nil {
		t.Fatal("route-only patch accepted an exact mapped-model mismatch")
	} else if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonModelNotListed {
		t.Fatalf("route-only patch reason = %q ok=%t err=%v", reason, ok, err)
	}
	stored, err := fixture.store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("reload rejected route-only patch: %v", err)
	}
	if stored.CandidateGroupID != primary.ID || stored.ManualChannelID != first.ID || stored.ManualGroupName != "first" || stored.ModelMappingModel != "gpt-5.4" {
		t.Fatalf("rejected route-only patch changed key: %+v", stored)
	}

	// This one PATCH changes mapping, candidate-group context, and fixed
	// target. The exact target gate runs before the retained group-wide mapping
	// configuration validator, so the API gets a stable target reason instead
	// of treating a group configuration error as the target verdict.
	targetMode := "candidate_group"
	mappedModel := "gpt-5.5"
	replacementGroupName := "replacement"
	if _, err := fixture.service.UpdateDownstreamKeyWithCandidateTargetAndMapping(key.ID, nil, nil, nil, nil, &replacementGroup.ID, &targetMode, nil, nil, &mappedModel, nil, nil, &replacement.ID, &replacementGroupName); err == nil {
		t.Fatal("mapping-plus-candidate-group patch accepted an exact mapped-model mismatch")
	} else if reason, ok := ManualRouteGateReasonOf(err); !ok || reason != ManualRouteGateReasonModelNotListed {
		t.Fatalf("mapping-plus-candidate-group patch reason = %q ok=%t err=%v", reason, ok, err)
	}
	stored, err = fixture.store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("reload rejected combined patch: %v", err)
	}
	if stored.CandidateGroupID != primary.ID || stored.ManualChannelID != first.ID || stored.ManualGroupName != "first" || stored.ModelMappingModel != "gpt-5.4" {
		t.Fatalf("rejected combined patch changed key: %+v", stored)
	}
}
