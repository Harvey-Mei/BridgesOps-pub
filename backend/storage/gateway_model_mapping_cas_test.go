package storage

import (
	"errors"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestDisableDownstreamKeyModelMappingIfCurrentDoesNotOverwriteNewMapping(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	store := NewGateway(db)
	group := &GatewayCandidateGroup{Name: "mapping-cas-group"}
	if err := db.Create(group).Error; err != nil {
		t.Fatalf("create candidate group: %v", err)
	}
	key := &GatewayDownstreamKey{
		Name:                        "mapping-cas",
		App:                         GatewayAppCodex,
		ModelFamily:                 "codex",
		ModelMappingStrategy:        "manual",
		ModelMappingModel:           "gpt-old",
		ModelMappingReasoningEffort: "high",
		CandidateGroupID:            group.ID,
		KeyHash:                     "mapping-cas-hash",
		KeyPrefix:                   "sk",
		Enabled:                     true,
		SmartRouting:                true,
	}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	stale, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("load stale mapping: %v", err)
	}
	staleGroup, err := store.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatalf("load candidate group: %v", err)
	}
	replacementGroupID := group.ID + 1000
	if err := db.Model(&GatewayDownstreamKey{}).Where("id = ?", key.ID).Updates(map[string]any{
		"model_mapping_strategy":         "manual",
		"model_mapping_model":            "gpt-new",
		"model_mapping_reasoning_effort": "xhigh",
		"candidate_group_id":             replacementGroupID,
		"candidate_target_mode":          "candidate_group",
	}).Error; err != nil {
		t.Fatalf("replace mapping: %v", err)
	}
	if err := store.DisableDownstreamKeyModelMappingIfCurrent(stale, staleGroup); !errors.Is(err, ErrDownstreamKeyModelMappingChanged) {
		t.Fatalf("stale disable error = %v", err)
	}
	current, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("load current mapping: %v", err)
	}
	if current.ModelMappingStrategy != "manual" || current.ModelMappingModel != "gpt-new" || current.ModelMappingReasoningEffort != "xhigh" || current.CandidateGroupID != replacementGroupID {
		t.Fatalf("stale request overwrote newer mapping: %+v", current)
	}
}

func TestDisableDownstreamKeyModelMappingIfCurrentTreatsAlreadyDisabledAsIdempotent(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	store := NewGateway(db)
	group := &GatewayCandidateGroup{Name: "mapping-cas-idempotent-group"}
	if err := db.Create(group).Error; err != nil {
		t.Fatalf("create candidate group: %v", err)
	}
	key := &GatewayDownstreamKey{
		Name:                        "mapping-cas-idempotent",
		App:                         GatewayAppCodex,
		ModelFamily:                 "codex",
		ModelMappingStrategy:        "balanced",
		ModelMappingModel:           "gpt-5.4",
		ModelMappingReasoningEffort: "high",
		CandidateGroupID:            group.ID,
		KeyHash:                     "mapping-cas-idempotent-hash",
		KeyPrefix:                   "sk",
		Enabled:                     true,
		SmartRouting:                true,
	}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	stale, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("load mapping: %v", err)
	}
	staleGroup, err := store.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatalf("load candidate group: %v", err)
	}
	if err := store.DisableDownstreamKeyModelMappingIfCurrent(stale, staleGroup); err != nil {
		t.Fatalf("first disable: %v", err)
	}
	if err := store.DisableDownstreamKeyModelMappingIfCurrent(stale, staleGroup); err != nil {
		t.Fatalf("idempotent disable: %v", err)
	}
	current, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("load disabled mapping: %v", err)
	}
	if current.ModelMappingStrategy != "off" || current.ModelMappingModel != "gpt-5.4" || current.ModelMappingReasoningEffort != "high" {
		t.Fatalf("disabled mapping lost review fields: %+v", current)
	}
}

func TestDisableDownstreamKeyModelMappingIfCurrentRejectsChangedCandidateGroupVersion(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	store := NewGateway(db)
	group := &GatewayCandidateGroup{Name: "mapping-cas-membership"}
	if err := db.Create(group).Error; err != nil {
		t.Fatalf("create candidate group: %v", err)
	}
	key := &GatewayDownstreamKey{
		Name:                        "mapping-cas-membership-key",
		App:                         GatewayAppCodex,
		ModelFamily:                 "codex",
		CandidateGroupID:            group.ID,
		ModelMappingStrategy:        "balanced",
		ModelMappingModel:           "gpt-5.4",
		ModelMappingReasoningEffort: "high",
		KeyHash:                     "mapping-cas-membership-hash",
		KeyPrefix:                   "sk",
		Enabled:                     true,
		SmartRouting:                true,
	}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create downstream key: %v", err)
	}
	staleKey, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("load stale mapping: %v", err)
	}
	staleGroup, err := store.FindCandidateGroup(group.ID)
	if err != nil {
		t.Fatalf("load stale candidate group: %v", err)
	}
	if err := db.Model(&GatewayCandidateGroup{}).Where("id = ?", group.ID).
		Update("membership_version", staleGroup.MembershipVersion+1).Error; err != nil {
		t.Fatalf("advance candidate membership version: %v", err)
	}
	if err := store.DisableDownstreamKeyModelMappingIfCurrent(staleKey, staleGroup); !errors.Is(err, ErrDownstreamKeyModelMappingChanged) {
		t.Fatalf("changed candidate membership version error = %v", err)
	}
	current, err := store.FindDownstreamKey(key.ID)
	if err != nil {
		t.Fatalf("load current mapping: %v", err)
	}
	if current.ModelMappingStrategy != "balanced" || current.ModelMappingModel != "gpt-5.4" || current.CandidateGroupID != group.ID {
		t.Fatalf("candidate membership change disabled or rewrote mapping: %+v", current)
	}
	if err := db.Model(&GatewayDownstreamKey{}).Where("id = ?", key.ID).
		Update("model_mapping_strategy", "off").Error; err != nil {
		t.Fatalf("disable mapping concurrently: %v", err)
	}
	if err := store.DisableDownstreamKeyModelMappingIfCurrent(staleKey, staleGroup); err != nil {
		t.Fatalf("changed membership version did not accept already-disabled mapping: %v", err)
	}
}
