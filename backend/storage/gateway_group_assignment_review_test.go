package storage

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestAutomaticAssignmentDraftMergesAndReplacesSavedRule(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "draft preview", Type: ChannelTypeNewAPI, SiteURL: "https://draft-preview.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	targets := []*GatewayCandidateGroup{{Name: "saved target"}, {Name: "draft target"}}
	for _, target := range targets {
		if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
			t.Fatal(err)
		}
	}
	state, err := store.automaticAssignmentState(db)
	if err != nil {
		t.Fatal(err)
	}
	saved := &GatewayCandidateGroupAutoRule{
		CandidateGroupID: targets[0].ID, Enabled: true, Priority: 20,
		ModelFamilies: []string{"codex"}, FamilyMatchMode: "single_any",
	}
	if _, err := store.SaveAutomaticAssignmentRule(saved, state.RulesetVersion); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "live", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "live", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "live-cipher", Enabled: true, LatencyProbeEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "live", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "live", "codex", "gpt-5.4", now, ""); err != nil {
		t.Fatal(err)
	}
	var change GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ? AND kind = ?", channel.ID, "live", "added").First(&change).Error; err != nil {
		t.Fatal(err)
	}

	newDraft := GatewayCandidateGroupAutoRule{
		CandidateGroupID: targets[1].ID, Enabled: true, Priority: 10,
		ModelFamilies: []string{"codex"}, FamilyMatchMode: "single_any",
	}
	previews, err := store.PreviewAutomaticAssignmentDraft(newDraft, []uint{change.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(previews) != 1 || !reflect.DeepEqual(previews[0].SuggestedCandidateGroupIDs, []uint{targets[0].ID}) {
		t.Fatalf("new draft did not merge with saved rules: %+v", previews)
	}

	editDraft := *saved
	editDraft.CandidateGroupID = targets[1].ID
	previews, err = store.PreviewAutomaticAssignmentDraft(editDraft, []uint{change.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(previews) != 1 || !reflect.DeepEqual(previews[0].SuggestedCandidateGroupIDs, []uint{targets[1].ID}) {
		t.Fatalf("edited draft did not replace its saved rule: %+v", previews)
	}
}

func TestDeleteCandidateGroupBumpsRulesetWhenRuleCascades(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	candidate := &GatewayCandidateGroup{Name: "cascade target"}
	if err := db.Create(candidate).Error; err != nil {
		t.Fatal(err)
	}
	config, err := store.AutomaticAssignmentConfig()
	if err != nil {
		t.Fatal(err)
	}
	rule := &GatewayCandidateGroupAutoRule{
		CandidateGroupID: candidate.ID,
		Enabled:          true,
		Priority:         1,
		ModelFamilies:    []string{"codex"},
		FamilyMatchMode:  "single_any",
	}
	state, err := store.SaveAutomaticAssignmentRule(rule, config.State.RulesetVersion)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteCandidateGroup(candidate.ID); err != nil {
		t.Fatal(err)
	}
	after, err := store.AutomaticAssignmentConfig()
	if err != nil {
		t.Fatal(err)
	}
	if after.State.RulesetVersion != state.RulesetVersion+1 || len(after.Rules) != 0 {
		t.Fatalf("ruleset after cascading target deletion = version %d rules %+v, want version %d and no rules", after.State.RulesetVersion, after.Rules, state.RulesetVersion+1)
	}
}

func TestAutomaticAssignmentModelIDConditionsPreserveCase(t *testing.T) {
	group := GatewayUpstreamGroup{ModelIDs: []string{"Vendor/CaseModel"}}
	for _, matchType := range []string{"exact", "prefix"} {
		t.Run(matchType, func(t *testing.T) {
			value := "Vendor/CaseModel"
			if matchType == "prefix" {
				value = "Vendor/Case"
			}
			rule := GatewayCandidateGroupAutoRule{
				CandidateGroupID: 1, Enabled: true,
				ModelIDConditions:        []GatewayModelIDCondition{{Match: matchType, Value: value}},
				ModelConditionQuantifier: "all", ModelFactCoverage: "all",
			}
			if err := normalizeAndValidateAutomaticRule(&rule); err != nil {
				t.Fatal(err)
			}
			if rule.ModelIDConditions[0].Value != value {
				t.Fatalf("condition value was case-normalized: %q", rule.ModelIDConditions[0].Value)
			}
			if got := evaluateAutomaticRule(rule, group, time.Now()); !got.Matched {
				t.Fatalf("case-exact %s condition did not match: %+v", matchType, got)
			}
			rule.ModelIDConditions[0].Value = "vendor/case"
			if got := evaluateAutomaticRule(rule, group, time.Now()); got.Matched {
				t.Fatalf("case-mismatched %s condition unexpectedly matched: %+v", matchType, got)
			}
		})
	}
}

func TestAutomaticAssignmentCapabilitySelectorsPreserveCase(t *testing.T) {
	now := time.Now().UTC()
	group := GatewayUpstreamGroup{
		PresenceGeneration: 1,
		ModelFamilyCodes:   []string{"codex"},
		ModelIDs:           []string{"Vendor/CaseModel"},
		CapabilityProfile: map[string]map[string]any{
			"Vendor/CaseModel": {
				"supported": true, "evidence_source": "probed", "presence_generation": uint64(1),
				"last_checked_at": now.Format(time.RFC3339Nano), "safe_input_tokens": int64(200_000),
			},
		},
	}
	for _, selectorType := range []string{"exact", "prefix"} {
		t.Run(selectorType, func(t *testing.T) {
			value := "Vendor/CaseModel"
			if selectorType == "prefix" {
				value = "Vendor/Case"
			}
			rule := GatewayCandidateGroupAutoRule{
				CandidateGroupID: 1, Enabled: true, ModelFamilies: []string{"codex"}, FamilyMatchMode: "single_any",
				CapabilityConditions: []GatewayCapabilityCondition{{
					SelectorType: selectorType, SelectorValues: []string{value}, Quantifier: "all_models",
					EvidenceSource: "probed", MaxAgeHours: 1, MinSafeInput: 100_000,
				}},
			}
			if err := normalizeAndValidateAutomaticRule(&rule); err != nil {
				t.Fatal(err)
			}
			if rule.CapabilityConditions[0].SelectorValues[0] != value {
				t.Fatalf("selector value was case-normalized: %q", rule.CapabilityConditions[0].SelectorValues[0])
			}
			if got := evaluateAutomaticRule(rule, group, now); !got.Matched {
				t.Fatalf("case-exact %s selector did not match: %+v", selectorType, got)
			}
			rule.CapabilityConditions[0].SelectorValues = []string{"vendor/case"}
			if got := evaluateAutomaticRule(rule, group, now); got.Matched {
				t.Fatalf("case-mismatched %s selector unexpectedly matched: %+v", selectorType, got)
			}
		})
	}
}

func TestProbedGatewayAppSourceIsReachableFromProbeWrite(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "probed app source", Type: ChannelTypeNewAPI, SiteURL: "https://probed-app.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "pro", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "pro", Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.RecordUpstreamGroupChange(channel.ID, "pro", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "pro", "codex", "gpt-5.4", now, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkModelCapabilityForApp(channel.ID, "pro", "gpt-5.4", GatewayAppCodex, true, ""); err != nil {
		t.Fatal(err)
	}
	var group GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "pro").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if group.GatewayAppSources["codex"] != "probed" || !containsString(group.ProbedGatewayApps, "codex") {
		t.Fatalf("probe did not persist probed app evidence: %+v", group)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "pro", "codex", "gpt-5.4", now.Add(time.Minute), ""); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if group.GatewayAppSources["codex"] != "probed" || !containsString(group.ProbedGatewayApps, "codex") {
		t.Fatalf("catalog refresh displaced probed app evidence: %+v", group)
	}
	rule := GatewayCandidateGroupAutoRule{
		CandidateGroupID: 1, Enabled: true, ModelFamilies: []string{"codex"}, FamilyMatchMode: "single_any",
		GatewayApps: []string{"codex"}, AppMatchMode: "exact_set", AcceptedAppSources: []string{"probed"},
	}
	if err := normalizeAndValidateAutomaticRule(&rule); err != nil {
		t.Fatal(err)
	}
	if got := evaluateAutomaticRule(rule, group, time.Now()); !got.Matched {
		t.Fatalf("probed app source was not usable by a rule: %+v", got)
	}
	catalogRule := rule
	catalogRule.AcceptedAppSources = []string{"catalog_compatible"}
	if got := evaluateAutomaticRule(catalogRule, group, time.Now()); !got.Matched {
		t.Fatalf("probed app source did not satisfy catalog-compatible evidence requirement: %+v", got)
	}

	if err := store.RecordUpstreamGroupChange(channel.ID, "pro", "removed", 1, "managed", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "pro", "added", 2, "managed-2", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(group.ProbedGatewayApps) != 0 || len(group.GatewayAppSources) != 0 {
		t.Fatalf("probed app evidence crossed presence generation before model sync: %+v", group)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "pro", "codex", "gpt-5.4", group.DiscoveredAt.Add(time.Second), ""); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(group.ProbedGatewayApps) != 0 || group.GatewayAppSources["codex"] != "catalog_compatible" {
		t.Fatalf("old probed app evidence returned after new-generation model sync: %+v", group)
	}
	if got := evaluateAutomaticRule(rule, group, time.Now()); got.Matched || !got.FactsIncomplete || !containsString(got.MissingFacts, "gateway_app_evidence_missing") {
		t.Fatalf("catalog-compatible evidence unexpectedly satisfied a probed-only rule: %+v", got)
	}
	if err := store.MarkModelCapabilityForApp(channel.ID, "pro", "gpt-5.4", GatewayAppCodex, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if group.GatewayAppSources["codex"] != "probed" || !containsString(group.ProbedGatewayApps, "codex") {
		t.Fatalf("new-generation probe did not restore probed app evidence: %+v", group)
	}
}

func TestPolicyInitializedKeyAllowsUnrelatedUpdatesAfterPolicyChanges(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "policy key lifecycle", Type: ChannelTypeNewAPI, SiteURL: "https://policy-key.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	candidate := &GatewayCandidateGroup{Name: "policy target"}
	if err := store.SaveCandidateGroup(candidate, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	keyGroup := &GatewayKeyGroup{Name: "policy keys"}
	if err := store.SaveKeyGroup(keyGroup); err != nil {
		t.Fatal(err)
	}
	targetID := candidate.ID
	policy := &GatewayKeyGroupRoutePolicy{
		KeyGroupID: keyGroup.ID, GatewayApp: "codex", ModelFamily: "codex",
		DefaultCandidateGroupID: &targetID, TargetMode: "candidate_group", Enabled: true,
	}
	if err := store.SaveKeyGroupRoutePolicy(policy, 0); err != nil {
		t.Fatal(err)
	}
	key := &GatewayDownstreamKey{
		Name: "policy initialized", App: GatewayAppCodex, ModelFamily: "codex", KeyGroupID: keyGroup.ID,
		KeyHash: "policy-initialized-key", APIKeyCipher: "cipher", KeyPrefix: "sk-review", Enabled: true, SmartRouting: true,
	}
	applied, err := store.ApplyKeyGroupRoutePolicy(key)
	if err != nil || !applied {
		t.Fatalf("apply policy: applied=%t err=%v", applied, err)
	}
	if err := store.CreateDownstreamKey(key); err != nil {
		t.Fatal(err)
	}

	changed := *policy
	changed.TargetMode = "all_candidates"
	changed.DefaultCandidateGroupID = nil
	if err := store.SaveKeyGroupRoutePolicy(&changed, policy.PolicyVersion); err != nil {
		t.Fatal(err)
	}
	key.Name = "renamed after policy change"
	if err := store.UpdateDownstreamKey(key); err != nil {
		t.Fatalf("unrelated update after policy change: %v", err)
	}
	if key.CandidateGroupID != candidate.ID || key.CandidateTargetMode != "candidate_group" || key.RoutePolicyVersion == nil || *key.RoutePolicyVersion != 1 {
		t.Fatalf("policy change rewrote initialized key: %+v", key)
	}

	if err := store.DeleteKeyGroupRoutePolicy(policy.ID, changed.PolicyVersion); err != nil {
		t.Fatal(err)
	}
	key, err = store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	key.Name = "renamed after policy deletion"
	if err := store.UpdateDownstreamKey(key); err != nil {
		t.Fatalf("unrelated update after policy deletion: %v", err)
	}
	if key.CandidateGroupID != candidate.ID || key.CandidateTargetMode != "candidate_group" || key.CandidateTargetSource != "key_group_policy" || key.RoutePolicyID != nil || key.RoutePolicyVersion == nil || *key.RoutePolicyVersion != 1 {
		t.Fatalf("policy deletion invalidated initialized key snapshot: %+v", key)
	}
}

func TestManualAssignmentIncompleteFactsRemainExplicitInChangeAndAudit(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "manual incomplete", Type: ChannelTypeNewAPI, SiteURL: "https://manual-incomplete.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	for _, groupName := range []string{"seed", "unclassified"} {
		if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "seed", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "seed-cipher", Enabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	target := &GatewayCandidateGroup{Name: "manual incomplete target"}
	if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "unclassified", UpstreamKeyID: 1, UpstreamKeyName: "managed", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted, APIKeyCipher: "cipher", Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.RecordUpstreamGroupChange(channel.ID, "unclassified", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "unclassified", "", "Vendor-Unclassified-Model", now, ""); err != nil {
		t.Fatal(err)
	}
	var change GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ? AND kind = ?", channel.ID, "unclassified", "added").First(&change).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.AssignUpstreamGroupChange(change.ID, []uint{target.ID}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&change, change.ID).Error; err != nil {
		t.Fatal(err)
	}
	if change.Status != "assigned" || change.AssignmentReasonCode != "manual_override_incomplete_facts" {
		t.Fatalf("manual incomplete assignment lost its change reason: %+v", change)
	}
	var audit GatewayUpstreamGroupAssignmentAudit
	if err := db.Where("change_id = ?", change.ID).Order("id DESC").First(&audit).Error; err != nil {
		t.Fatal(err)
	}
	if audit.Action != "manual_override_incomplete_facts" || audit.Explanation["reason_code"] != "manual_override_incomplete_facts" {
		t.Fatalf("manual incomplete assignment lost its audit reason: %+v", audit)
	}
	missing, ok := audit.Explanation["missing_facts"].([]any)
	if !ok {
		t.Fatalf("manual incomplete audit missing facts has unexpected type: %#v", audit.Explanation["missing_facts"])
	}
	found := false
	for _, reason := range missing {
		if reason == "unclassified_models" {
			found = true
		}
	}
	if !found {
		t.Fatalf("manual incomplete audit did not retain unclassified_models: %+v", audit.Explanation)
	}
}

func TestLowPriorityIncompleteRuleDoesNotBlockHigherPriorityWinner(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "priority completeness", Type: ChannelTypeNewAPI, SiteURL: "https://priority-completeness.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	targets := []*GatewayCandidateGroup{{Name: "high complete target"}, {Name: "low incomplete target"}}
	for _, target := range targets {
		if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
			t.Fatal(err)
		}
	}
	state, err := store.automaticAssignmentState(db)
	if err != nil {
		t.Fatal(err)
	}
	high := &GatewayCandidateGroupAutoRule{
		CandidateGroupID: targets[0].ID, Enabled: true, Priority: 100,
		ModelFamilies: []string{"codex"}, FamilyMatchMode: "single_any",
	}
	state, err = store.SaveAutomaticAssignmentRule(high, state.RulesetVersion)
	if err != nil {
		t.Fatal(err)
	}
	low := &GatewayCandidateGroupAutoRule{
		CandidateGroupID: targets[1].ID, Enabled: true, Priority: 10,
		ModelFamilies: []string{"codex"}, FamilyMatchMode: "single_any",
		CapabilityConditions: []GatewayCapabilityCondition{{
			SelectorType: "exact", SelectorValues: []string{"gpt-5.4"}, Quantifier: "all_models",
			EvidenceSource: "probed", MaxAgeHours: 1, MinSafeInput: 100_000,
		}},
	}
	state, err = store.SaveAutomaticAssignmentRule(low, state.RulesetVersion)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "live", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "live", OwnershipProvenance: GatewayUpstreamKeyProvenanceAdopted,
		APIKeyCipher: "live-cipher", Enabled: true, LatencyProbeEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "live", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "live", "codex", "gpt-5.4", now, ""); err != nil {
		t.Fatal(err)
	}
	previews, err := store.PreviewAutomaticAssignments(nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(previews) != 1 || previews[0].State != "ready" || previews[0].ReasonCode != "unique_highest_priority_match" || !reflect.DeepEqual(previews[0].SuggestedCandidateGroupIDs, []uint{targets[0].ID}) {
		t.Fatalf("low-priority incomplete rule blocked high-priority winner: %+v", previews)
	}

	for _, priority := range []int{high.Priority, high.Priority + 1} {
		low.Priority = priority
		state, err = store.SaveAutomaticAssignmentRule(low, state.RulesetVersion)
		if err != nil {
			t.Fatal(err)
		}
		previews, err = store.PreviewAutomaticAssignments(nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(previews) != 1 || previews[0].State != "awaiting_facts" || !containsString(previews[0].MissingFacts, "capability_evidence_missing_or_stale") {
			t.Fatalf("priority %d incomplete rule did not block the complete priority %d winner: %+v", priority, high.Priority, previews)
		}
	}
}

func TestDeleteChannelCleansAutomaticAssignmentState(t *testing.T) {
	db := openTestDB(t)
	store := NewGateway(db)
	channels := NewChannels(db)
	channel := &Channel{Name: "assignment cleanup", Type: ChannelTypeNewAPI, SiteURL: "https://assignment-cleanup.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "live", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "live")
	candidate := &GatewayCandidateGroup{Name: "assignment cleanup target"}
	if err := store.SaveCandidateGroup(candidate, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "live"}}); err != nil {
		t.Fatal(err)
	}

	var upstreamGroup GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "live").First(&upstreamGroup).Error; err != nil {
		t.Fatal(err)
	}
	for name, model := range map[string]any{
		"audit":    &GatewayUpstreamGroupAssignmentAudit{},
		"override": &GatewayUpstreamGroupAssignmentOverride{},
		"claim":    &GatewayCandidateGroupMembershipClaim{},
		"member":   &GatewayCandidateGroupMember{},
	} {
		var count int64
		if err := db.Model(model).Where("upstream_group_id = ?", upstreamGroup.ID).Count(&count).Error; err != nil {
			t.Fatalf("count %s before channel delete: %v", name, err)
		}
		if count == 0 {
			t.Fatalf("expected %s before channel delete", name)
		}
	}

	var assignmentOverride GatewayUpstreamGroupAssignmentOverride
	if err := db.Where("upstream_group_id = ?", upstreamGroup.ID).First(&assignmentOverride).Error; err != nil {
		t.Fatal(err)
	}
	if err := channels.Delete(channel.ID); !errors.Is(err, ErrChannelDeletionPending) {
		t.Fatal(err)
	}

	checks := []struct {
		name  string
		model any
		query string
		arg   any
	}{
		{name: "upstream group", model: &GatewayUpstreamGroup{}, query: "channel_id = ?", arg: channel.ID},
		{name: "audit", model: &GatewayUpstreamGroupAssignmentAudit{}, query: "upstream_group_id = ?", arg: upstreamGroup.ID},
		{name: "override", model: &GatewayUpstreamGroupAssignmentOverride{}, query: "upstream_group_id = ?", arg: upstreamGroup.ID},
		{name: "override target", model: &GatewayUpstreamGroupAssignmentOverrideTarget{}, query: "override_id = ?", arg: assignmentOverride.ID},
		{name: "claim", model: &GatewayCandidateGroupMembershipClaim{}, query: "upstream_group_id = ?", arg: upstreamGroup.ID},
		{name: "member", model: &GatewayCandidateGroupMember{}, query: "upstream_group_id = ?", arg: upstreamGroup.ID},
	}
	for _, check := range checks {
		var count int64
		if err := db.Model(check.model).Where(check.query, check.arg).Count(&count).Error; err != nil {
			t.Fatalf("count %s after channel delete: %v", check.name, err)
		}
		if count == 0 {
			t.Fatalf("delete prepare removed %s rows", check.name)
		}
	}
}
