package storage

import (
	"errors"
	"testing"
	"time"
)

func TestProbeEvidenceDoesNotPopulateFactsWithoutCurrentRate(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "probe without rate", Type: ChannelTypeNewAPI, SiteURL: "https://probe-without-rate.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "removed", ModelIDs: "gpt-5.4", ModelFamilies: "codex",
		Enabled: true, LatencyProbeEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := store.RecordUpstreamGroupChange(channel.ID, "removed", "added", 1, "managed", now); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkModelCapabilityForApp(channel.ID, "removed", "gpt-5.4", GatewayAppCodex, true, ""); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("unfenced probe capability error = %v", err)
	}
	if err := store.ObserveModelContextLimit(channel.ID, "removed", "gpt-5.4", 1_000_000, 800_000, 50_000, now); !errors.Is(err, ErrUpstreamGroupGenerationChanged) {
		t.Fatalf("unfenced context observation error = %v", err)
	}
	var capabilityCount int64
	if err := db.Model(&GatewayModelCapability{}).
		Where("channel_id = ? AND group_name = ?", channel.ID, "removed").
		Count(&capabilityCount).Error; err != nil {
		t.Fatal(err)
	}
	if capabilityCount != 0 {
		t.Fatalf("probe persisted %d capability rows without a current rate", capabilityCount)
	}
	var group GatewayUpstreamGroup
	if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "removed").First(&group).Error; err != nil {
		t.Fatal(err)
	}
	if group.FactsComplete || len(group.ModelIDs) != 0 || len(group.ProbedGatewayApps) != 0 || len(group.GatewayAppSources) != 0 {
		t.Fatalf("probe evidence populated facts without a current rate: %+v", group)
	}

	rateAt := now.Add(time.Minute)
	if _, err := NewRates(db).Upsert(&RateSnapshot{ChannelID: channel.ID, ModelName: "removed", Ratio: 1, FirstSeenAt: rateAt, LastSeenAt: rateAt}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "removed", "codex", "gpt-5.4", rateAt, ""); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&group, group.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(group.CapabilityProfile) != 0 || len(group.ProbedGatewayApps) != 0 || group.GatewayAppSources["codex"] != "catalog_compatible" {
		t.Fatalf("pre-rate probe evidence leaked after the rate returned: %+v", group)
	}
}

func TestGenerationFencedModelEvidencePersistsWithoutCurrentRate(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "fixed evidence without rate", Type: ChannelTypeNewAPI, SiteURL: "https://fixed-evidence.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "fixed", APIKeyCipher: "cipher",
		Enabled: true, LatencyProbeEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	discoveredAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := store.RecordUpstreamGroupChange(channel.ID, "fixed", "added", 1, "managed", discoveredAt); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, "fixed")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "fixed", "codex", "gpt-5.4", discoveredAt.Add(time.Second), "", generation); err != nil {
		t.Fatalf("write fenced model families without rate: %v", err)
	}
	if err := store.MarkModelCapability(channel.ID, "fixed", "gpt-5.5", false, "unsupported", generation); err != nil {
		t.Fatalf("write fenced capability without rate: %v", err)
	}
	if err := store.ObserveModelContextLimit(channel.ID, "fixed", "gpt-5.4", 10_000, 8_000, 1_000, discoveredAt.Add(2*time.Second), generation); err != nil {
		t.Fatalf("write fenced context limit without rate: %v", err)
	}

	key, err := store.FindUpstreamKeyAny(channel.ID, "fixed")
	if err != nil {
		t.Fatal(err)
	}
	if key.ModelFamilies != "codex" || key.ModelIDs != "gpt-5.4" || key.ModelsSyncedAt == nil {
		t.Fatalf("fenced no-rate model facts = %+v", key)
	}
	unsupported, err := store.FindModelCapability(channel.ID, "fixed", "gpt-5.5")
	if err != nil || unsupported == nil || unsupported.Supported {
		t.Fatalf("fenced no-rate negative capability = %+v err=%v", unsupported, err)
	}
	contextLimit, err := store.FindModelCapability(channel.ID, "fixed", "gpt-5.4")
	if err != nil || contextLimit == nil || contextLimit.SafeInputTokens != 8_000 {
		t.Fatalf("fenced no-rate context capability = %+v err=%v", contextLimit, err)
	}
	var group GatewayUpstreamGroup
	if err := db.First(&group, generation.ID).Error; err != nil {
		t.Fatal(err)
	}
	if group.FactsComplete || len(group.ModelIDs) != 0 || len(group.CapabilityProfile) != 0 {
		t.Fatalf("no-rate evidence populated assignment facts: %+v", group)
	}
}

func TestEnablingAutomaticAssignmentDoesNotApplyExistingReadyChanges(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "future sync only", Type: ChannelTypeNewAPI, SiteURL: "https://future-sync.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "seed", Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
	target := &GatewayCandidateGroup{Name: "future sync target"}
	if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
		t.Fatal(err)
	}
	state, err := store.automaticAssignmentState(db)
	if err != nil {
		t.Fatal(err)
	}
	state, err = store.SaveAutomaticAssignmentRule(&GatewayCandidateGroupAutoRule{
		CandidateGroupID: target.ID,
		Enabled:          true,
		Priority:         100,
		ModelFamilies:    []string{"codex"},
		FamilyMatchMode:  "single_any",
	}, state.RulesetVersion)
	if err != nil {
		t.Fatal(err)
	}

	discoveredAt := time.Now().UTC().Round(time.Second)
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "ready", Ratio: 1, FirstSeenAt: discoveredAt, LastSeenAt: discoveredAt}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "ready", Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "ready", "added", 1, "managed", discoveredAt); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "ready", "codex", "gpt-5.4", discoveredAt.Add(time.Second), ""); err != nil {
		t.Fatal(err)
	}
	previews, err := store.PreviewAutomaticAssignments(nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(previews) != 1 || previews[0].State != "ready" {
		t.Fatalf("existing change is not ready before enabling: %+v", previews)
	}

	updated, err := store.UpdateAutomaticAssignmentState(state.RulesetVersion, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.AutoExecuteEnabled || !updated.BackfillComplete {
		t.Fatalf("automatic assignment state = %+v", updated)
	}
	var change GatewayUpstreamGroupChange
	if err := db.First(&change, previews[0].ChangeID).Error; err != nil {
		t.Fatal(err)
	}
	if change.Status != "pending" {
		t.Fatalf("enabling automatic assignment applied an existing ready change: %+v", change)
	}
	var claims int64
	if err := db.Model(&GatewayCandidateGroupMembershipClaim{}).
		Where("upstream_group_id = ? AND source_kind = ? AND active = ?", previews[0].UpstreamGroupID, "auto_rule", true).
		Count(&claims).Error; err != nil {
		t.Fatal(err)
	}
	if claims != 0 {
		t.Fatalf("enabling automatic assignment created %d claims for an existing ready change", claims)
	}

	futureAt := discoveredAt.Add(2 * time.Second)
	if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: "future", Ratio: 1, FirstSeenAt: futureAt, LastSeenAt: futureAt}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "future", Enabled: true, LatencyProbeEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, "future", "added", 2, "managed", futureAt); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "future", "codex", "gpt-5.4", futureAt.Add(time.Second), ""); err != nil {
		t.Fatal(err)
	}
	var futureChange GatewayUpstreamGroupChange
	if err := db.Where("channel_id = ? AND group_name = ? AND kind = ?", channel.ID, "future", "added").First(&futureChange).Error; err != nil {
		t.Fatal(err)
	}
	if futureChange.Status != "assigned" {
		t.Fatalf("a later synchronization was not automatically applied: %+v", futureChange)
	}
	var syncAudit GatewayUpstreamGroupAssignmentAudit
	if err := db.Where("change_id = ?", futureChange.ID).Order("id DESC").First(&syncAudit).Error; err != nil {
		t.Fatal(err)
	}
	if syncAudit.TriggerSource != GatewayAssignmentTriggerSync {
		t.Fatalf("future synchronization trigger = %q", syncAudit.TriggerSource)
	}
}

func TestPersistentOverridesRequireExplicitExecution(t *testing.T) {
	for _, action := range []string{"assign", "ignore"} {
		t.Run(action, func(t *testing.T) {
			db := openTestDB(t)
			channels := NewChannels(db)
			store := NewGateway(db)
			channel := &Channel{Name: "persistent override " + action, Type: ChannelTypeNewAPI, SiteURL: "https://persistent-override-" + action + ".example", MonitorEnabled: true}
			if err := channels.Create(channel); err != nil {
				t.Fatal(err)
			}
			for _, groupName := range []string{"seed", "persistent"} {
				if err := db.Create(&RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}).Error; err != nil {
					t.Fatal(err)
				}
			}
			createUnsyncedTrustedAssignmentKeyFixture(t, db, channel.ID, "seed")
			target := &GatewayCandidateGroup{Name: "persistent override target " + action}
			if err := store.SaveCandidateGroup(target, []GatewayCandidateGroupMember{{ChannelID: channel.ID, GroupName: "seed"}}); err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&GatewayUpstreamKey{
				ChannelID: channel.ID, GroupName: "persistent", UpstreamKeyID: 1, UpstreamKeyName: "managed",
				OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated,
				APIKeyCipher:        "persistent-cipher", Enabled: true, LatencyProbeEnabled: true,
			}).Error; err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Round(time.Second)
			if err := store.RecordUpstreamGroupChange(channel.ID, "persistent", "added", 1, "managed", now); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateUpstreamKeyModelFamilies(channel.ID, "persistent", "codex", "gpt-5.4", now.Add(time.Second), ""); err != nil {
				t.Fatal(err)
			}

			var group GatewayUpstreamGroup
			if err := db.Where("channel_id = ? AND canonical_group_name = ?", channel.ID, "persistent").First(&group).Error; err != nil {
				t.Fatal(err)
			}
			override := GatewayUpstreamGroupAssignmentOverride{
				UpstreamGroupID: group.ID, PresenceGeneration: group.PresenceGeneration, Action: action, Reason: "persistent test override",
				CreatedBy: "test", OverrideVersion: 1, CreatedAt: now, UpdatedAt: now,
			}
			if err := db.Create(&override).Error; err != nil {
				t.Fatal(err)
			}
			if action == "assign" {
				targetID := target.ID
				if err := db.Create(&GatewayUpstreamGroupAssignmentOverrideTarget{
					OverrideID: override.ID, CandidateGroupID: &targetID, CandidateGroupIDSnapshot: target.ID, CreatedAt: now,
				}).Error; err != nil {
					t.Fatal(err)
				}
			}

			assigned, failures := store.ReevaluateUpstreamGroupAutomaticAssignment(channel.ID, "persistent", false)
			if assigned != 0 || len(failures) != 0 {
				t.Fatalf("execute=false assigned=%d failures=%v", assigned, failures)
			}
			var change GatewayUpstreamGroupChange
			if err := db.Where("upstream_group_id = ? AND kind = ?", group.ID, "added").First(&change).Error; err != nil {
				t.Fatal(err)
			}
			if change.Status != "pending" || change.ResolvedAt != nil {
				t.Fatalf("execute=false resolved persistent override: %+v", change)
			}
			if change.AssignmentState != "ready" || change.AssignmentReasonCode != "manual_"+action+"_override" {
				t.Fatalf("execute=false did not retain preview-only state: %+v", change)
			}
			for name, model := range map[string]any{
				"member": &GatewayCandidateGroupMember{},
				"claim":  &GatewayCandidateGroupMembershipClaim{},
				"audit":  &GatewayUpstreamGroupAssignmentAudit{},
			} {
				var count int64
				query := db.Model(model).Where("upstream_group_id = ?", group.ID)
				if name == "audit" {
					query = query.Where("change_id = ?", change.ID)
				}
				if err := query.Count(&count).Error; err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Fatalf("execute=false created %d %s rows", count, name)
				}
			}

			assigned, failures = store.ReevaluateUpstreamGroupAutomaticAssignment(channel.ID, "persistent", true)
			if assigned != 1 || len(failures) != 0 {
				t.Fatalf("execute=true assigned=%d failures=%v", assigned, failures)
			}
			if err := db.First(&change, change.ID).Error; err != nil {
				t.Fatal(err)
			}
			wantStatus := "assigned"
			if action == "ignore" {
				wantStatus = "ignored"
			}
			if change.Status != wantStatus || change.ResolvedAt == nil {
				t.Fatalf("execute=true did not apply persistent override with disabled global state: %+v", change)
			}
		})
	}
}
