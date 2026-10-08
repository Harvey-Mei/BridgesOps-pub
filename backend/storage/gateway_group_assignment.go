package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/modelcatalog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrGatewayRulesetChanged        = errors.New("automatic assignment rules changed; reload and try again")
	ErrGatewayEvaluationStale       = errors.New("automatic assignment preview is stale; preview again")
	ErrGatewayExpectedVersionNeeded = errors.New("expected version is required; reload and try again")
)

var gatewayServiceTierPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

const (
	GatewayAssignmentTriggerSync            = "sync"
	GatewayAssignmentTriggerRuleRecheck     = "rule_recheck"
	GatewayAssignmentTriggerManual          = "manual_assign"
	GatewayAssignmentTriggerOverrideReapply = "override_reapply"
)

type GatewayAutomaticAssignmentConfig struct {
	State          GatewayCandidateGroupRulesetState     `json:"state"`
	Rules          []GatewayCandidateGroupAutoRule       `json:"rules"`
	UpstreamGroups []GatewayUpstreamGroup                `json:"upstream_groups"`
	Audits         []GatewayUpstreamGroupAssignmentAudit `json:"audits"`
	ModelCatalog   GatewayModelFamilyCatalog             `json:"model_catalog"`
}

type gatewayAutomaticAssignmentBackfillKey struct {
	GatewayUpstreamKey          `gorm:"embedded"`
	ModelFactCanonicalGroupName string `gorm:"column:model_fact_canonical_group_name"`
}

func (key gatewayAutomaticAssignmentBackfillKey) modelFactFence() (GatewayModelFactFence, error) {
	fence := GatewayModelFactFence{
		ChannelID:               key.ChannelID,
		ChannelIdentityRevision: key.ModelFactsChannelIdentityRevision,
		UpstreamGroupID:         key.ModelFactsUpstreamGroupID,
		PresenceGeneration:      key.ModelFactsPresenceGeneration,
		CanonicalGroupName:      strings.TrimSpace(key.ModelFactCanonicalGroupName),
	}
	if !fence.Valid() {
		return GatewayModelFactFence{}, ErrUpstreamGroupGenerationChanged
	}
	return fence, nil
}

func currentGatewayAutomaticAssignmentBackfillKeysQuery(db *gorm.DB, cursor uint, limit int) *gorm.DB {
	return db.Table("gateway_upstream_keys AS keys").
		Select("keys.*, model_fact_groups.canonical_group_name AS model_fact_canonical_group_name").
		Joins(`JOIN gateway_upstream_groups AS model_fact_groups
			ON model_fact_groups.id = keys.model_facts_upstream_group_id
		   AND model_fact_groups.channel_id = keys.channel_id
		   AND model_fact_groups.canonical_group_name = lower(btrim(keys.group_name))
		   AND model_fact_groups.present = TRUE
		   AND model_fact_groups.presence_generation = keys.model_facts_presence_generation`).
		Joins(`JOIN channels AS model_fact_channels
			ON model_fact_channels.id = keys.channel_id
		   AND model_fact_channels.upstream_account_identity_revision = keys.model_facts_channel_identity_revision`).
		Where("keys.id > ? AND (btrim(COALESCE(keys.model_ids, '')) <> '' OR btrim(COALESCE(keys.models_error, '')) <> '')", cursor).
		Order("keys.id ASC").
		Limit(limit)
}

type GatewayModelFamilyCatalog struct {
	Version  string                           `json:"version"`
	Families []GatewayModelFamilyCatalogEntry `json:"families"`
}

type GatewayModelFamilyCatalogEntry struct {
	Code           string   `json:"code"`
	DisplayName    string   `json:"display_name"`
	Aliases        []string `json:"aliases"`
	CompatibleApps []string `json:"compatible_apps"`
	Icon           string   `json:"icon"`
	BrandColor     string   `json:"brand_color"`
	AutoAssignable bool     `json:"auto_assignable"`
}

type GatewayAutomaticRuleMatch struct {
	RuleID           uint     `json:"rule_id"`
	CandidateGroupID uint     `json:"candidate_group_id"`
	Priority         int      `json:"priority"`
	Matched          bool     `json:"matched"`
	FactsIncomplete  bool     `json:"facts_incomplete"`
	Reasons          []string `json:"reasons"`
	MissingFacts     []string `json:"missing_facts"`
}

type GatewayAutomaticAssignmentPreview struct {
	ChangeID                   uint                        `json:"change_id"`
	UpstreamGroupID            uint                        `json:"upstream_group_id"`
	ChannelID                  uint                        `json:"channel_id"`
	GroupName                  string                      `json:"group_name"`
	PresenceGeneration         uint64                      `json:"presence_generation"`
	ModelFamilyCodes           []string                    `json:"model_family_codes"`
	ModelIDs                   []string                    `json:"model_ids"`
	GatewayApps                []string                    `json:"gateway_apps"`
	ChannelType                string                      `json:"channel_type"`
	ServiceTier                string                      `json:"service_tier,omitempty"`
	ServiceTierSource          string                      `json:"service_tier_source,omitempty"`
	State                      string                      `json:"state"`
	ReasonCode                 string                      `json:"reason_code"`
	MissingFacts               []string                    `json:"missing_facts"`
	SuggestedCandidateGroupIDs []uint                      `json:"suggested_candidate_group_ids"`
	MatchedRuleIDs             []uint                      `json:"matched_rule_ids"`
	RuleMatches                []GatewayAutomaticRuleMatch `json:"rule_matches"`
	EvaluationToken            string                      `json:"evaluation_token,omitempty"`
	EvaluatedAt                time.Time                   `json:"evaluated_at"`
}

func (g *Gateway) AutomaticAssignmentConfig() (*GatewayAutomaticAssignmentConfig, error) {
	state, err := g.automaticAssignmentState(g.db)
	if err != nil {
		return nil, err
	}
	rules := []GatewayCandidateGroupAutoRule{}
	if err := g.db.Order("candidate_group_id ASC, priority DESC, id ASC").Find(&rules).Error; err != nil {
		return nil, err
	}
	groups := []GatewayUpstreamGroup{}
	if err := g.db.Order("channel_id ASC, canonical_group_name ASC").Find(&groups).Error; err != nil {
		return nil, err
	}
	audits := []GatewayUpstreamGroupAssignmentAudit{}
	if err := g.db.Order("created_at DESC, id DESC").Limit(200).Find(&audits).Error; err != nil {
		return nil, err
	}
	catalog := ModelFamilyCatalog()
	return &GatewayAutomaticAssignmentConfig{State: *state, Rules: rules, UpstreamGroups: groups, Audits: audits, ModelCatalog: catalog}, nil
}

func ModelFamilyCatalog() GatewayModelFamilyCatalog {
	catalog := GatewayModelFamilyCatalog{Version: modelcatalog.Version(), Families: []GatewayModelFamilyCatalogEntry{}}
	for _, family := range modelcatalog.Families() {
		catalog.Families = append(catalog.Families, GatewayModelFamilyCatalogEntry{
			Code: family.Code, DisplayName: family.DisplayName, Aliases: family.Aliases,
			CompatibleApps: family.CompatibleApps, Icon: family.Icon,
			BrandColor: family.BrandColor, AutoAssignable: family.AutoAssignable,
		})
	}
	return catalog
}

func (g *Gateway) BackfillAutomaticAssignmentFacts(limit int) (int, error) {
	if limit <= 0 {
		limit = 500
	}
	processed := 0
	var cursor uint
	for {
		keys := []gatewayAutomaticAssignmentBackfillKey{}
		if err := currentGatewayAutomaticAssignmentBackfillKeysQuery(g.db, cursor, limit).Find(&keys).Error; err != nil {
			return processed, err
		}
		for _, key := range keys {
			modelFence, err := key.modelFactFence()
			if err != nil {
				return processed, err
			}
			at := time.Now()
			if key.ModelsSyncedAt != nil && !key.ModelsSyncedAt.IsZero() {
				at = *key.ModelsSyncedAt
			}
			if err := g.db.Transaction(func(tx *gorm.DB) error {
				if err := lockUpstreamGroupParentChannelTx(tx, key.ChannelID); err != nil {
					return err
				}
				if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
					return err
				}
				if _, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, key.ChannelID, key.GroupName); err != nil {
					return err
				}
				_, err := updateUpstreamGroupFactsIfChannelExistsTx(tx, key.ChannelID, key.GroupName, strings.Split(key.ModelIDs, ","), at, key.ModelsError, modelFence)
				return err
			}); err != nil {
				return processed, err
			}
			cursor = key.ID
			processed++
		}
		if len(keys) < limit {
			break
		}
	}
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		return tx.Model(&GatewayUpstreamGroup{}).
			Where("present = ? AND incomplete_reasons @> ?::jsonb", true, `["migration_backfill_pending"]`).
			Updates(map[string]any{
				"facts_complete":     false,
				"incomplete_reasons": gatewayJSONB([]string{"models_not_synced_for_generation"}),
				"facts_version":      gorm.Expr("facts_version + 1"),
				"updated_at":         time.Now(),
			}).Error
	})
	if err != nil {
		return processed, err
	}
	return processed, nil
}

func (g *Gateway) ReconcileAutomaticAssignmentPresence(limit int) (int, error) {
	if limit <= 0 {
		limit = 500
	}
	processed := 0
	var cursor uint
	for {
		rates := []RateSnapshot{}
		if err := g.db.Where("id > ?", cursor).Order("id ASC").Limit(limit).Find(&rates).Error; err != nil {
			return processed, err
		}
		for _, rate := range rates {
			if err := g.reconcileAutomaticAssignmentPresence(rate.ChannelID, rate.ModelName, rate.LastSeenAt); err != nil {
				return processed, err
			}
			cursor = rate.ID
			processed++
		}
		if len(rates) < limit {
			break
		}
	}
	present := []GatewayUpstreamGroup{}
	if err := g.db.Where("present = ?", true).Order("id ASC").Find(&present).Error; err != nil {
		return processed, err
	}
	// A rate seen above may disappear after its first reconciliation. Recheck
	// every still-present identity under the canonical presence fence.
	for _, group := range present {
		rawName := group.CanonicalGroupName
		if len(group.RawGroupNames) > 0 {
			rawName = group.RawGroupNames[0]
		}
		if err := g.reconcileAutomaticAssignmentPresence(group.ChannelID, rawName, time.Now()); err != nil {
			return processed, err
		}
	}
	return processed, nil
}

// Recovery scans must rebuild durable state without applying pending changes.
// The detached scan only selects identities to inspect: the actual presence
// decision is re-read under the same canonical RateSnapshot fence used by
// UpsertLoaded and DeleteSnapshot. Live sync uses RecordUpstreamGroupChange,
// which intentionally re-evaluates and may auto-execute a newly completed
// assignment.
func (g *Gateway) reconcileAutomaticAssignmentPresence(channelID uint, groupName string, at time.Time) error {
	groupName = strings.TrimSpace(groupName)
	if channelID == 0 || groupName == "" {
		return errors.New("invalid upstream group change")
	}
	if at.IsZero() {
		at = time.Now()
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, channelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		canonicalGroupName, err := lockRateSnapshotPresenceDomainTx(tx, channelID, groupName)
		if err != nil {
			return err
		}
		return reconcileAutomaticAssignmentPresenceLockedTx(tx, channelID, groupName, canonicalGroupName, at)
	})
}

// reconcileAutomaticAssignmentPresenceLockedTx derives durable group presence
// from the canonical rate inventory. The caller must hold the parent channel,
// automatic-assignment, and canonical rate-presence locks in that order.
func reconcileAutomaticAssignmentPresenceLockedTx(tx *gorm.DB, channelID uint, groupName, canonicalGroupName string, at time.Time) error {
	present, err := canonicalRateSnapshotExistsTx(tx, channelID, canonicalGroupName)
	if err != nil {
		return err
	}
	// A missing rate is a missing pricing fact, not proof that an
	// administrator-configured fixed target disappeared upstream. Keep the
	// current structural generation alive while it has a manual or persistent
	// override claim. Automatic rule claims remain rate-derived and are retired
	// below so they cannot outlive the generation that produced them.
	persistentMembership := false
	if !present {
		persistentMembership, err = upstreamGroupHasPersistentMembershipThroughCurrentGenerationTx(tx, channelID, canonicalGroupName)
		if err != nil {
			return err
		}
	}
	effectivePresence := present || persistentMembership
	kind := "added"
	if !present {
		kind = "removed"
		at = time.Now()
	}
	identity, newGeneration, err := ensureUpstreamGroupIdentityTx(tx, channelID, groupName, effectivePresence, at)
	if err != nil {
		return err
	}
	if !present {
		retire := retireUpstreamGroupGenerationTx
		if persistentMembership {
			retire = retireAutomaticUpstreamGroupGenerationTx
		}
		if err := retire(tx, identity, at); err != nil {
			return err
		}
	}
	if kind == "added" && !newGeneration {
		recorded, err := upstreamGroupGenerationAddedChangeExistsTx(tx, identity.ID, identity.PresenceGeneration)
		if err != nil {
			return err
		}
		if recorded {
			return nil
		}
	}
	var existing GatewayUpstreamGroupChange
	err = tx.Where("upstream_group_id = ?", identity.ID).Order("id DESC").First(&existing).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	if err == nil && existing.Kind == kind && !newGeneration {
		return nil
	}
	change := newUpstreamGroupChange(channelID, groupName, kind, 0, "", at)
	change.UpstreamGroupID = &identity.ID
	generation := identity.PresenceGeneration
	change.PresenceGeneration = &generation
	if kind == "added" {
		change.AssignmentState = "awaiting_facts"
		change.AssignmentReasonCode = "models_not_synced_for_generation"
	}
	return tx.Create(change).Error
}

func (g *Gateway) automaticAssignmentState(db *gorm.DB) (*GatewayCandidateGroupRulesetState, error) {
	var state GatewayCandidateGroupRulesetState
	err := db.First(&state, int16(1)).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		state = GatewayCandidateGroupRulesetState{ID: 1, RulesetVersion: 1}
		if err := db.Create(&state).Error; err != nil {
			return nil, err
		}
		return &state, nil
	}
	return &state, err
}

func (g *Gateway) UpdateAutomaticAssignmentState(expectedVersion uint64, backfillComplete, autoExecute bool) (*GatewayCandidateGroupRulesetState, error) {
	if expectedVersion == 0 {
		return nil, ErrGatewayExpectedVersionNeeded
	}
	var updated GatewayCandidateGroupRulesetState
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		state, err := g.lockAutomaticAssignmentState(tx)
		if err != nil {
			return err
		}
		if state.RulesetVersion != expectedVersion {
			return ErrGatewayRulesetChanged
		}
		if autoExecute && !backfillComplete {
			return errors.New("automatic assignment cannot be enabled before backfill is confirmed")
		}
		if backfillComplete {
			var pending int64
			if err := tx.Model(&GatewayUpstreamGroup{}).
				Where("incomplete_reasons @> ?::jsonb", `["migration_backfill_pending"]`).
				Count(&pending).Error; err != nil {
				return err
			}
			if pending > 0 {
				return fmt.Errorf("automatic assignment backfill still has %d unprocessed upstream groups", pending)
			}
		}
		changed := state.BackfillComplete != backfillComplete || state.AutoExecuteEnabled != autoExecute
		updates := map[string]any{
			"backfill_complete":    backfillComplete,
			"auto_execute_enabled": autoExecute,
			"updated_at":           time.Now(),
		}
		if changed {
			updates["ruleset_version"] = gorm.Expr("ruleset_version + 1")
		}
		if err := tx.Model(state).Updates(updates).Error; err != nil {
			return err
		}
		return tx.First(&updated, int16(1)).Error
	})
	return &updated, err
}

func (g *Gateway) lockAutomaticAssignmentState(tx *gorm.DB) (*GatewayCandidateGroupRulesetState, error) {
	var state GatewayCandidateGroupRulesetState
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&state, int16(1)).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		state = GatewayCandidateGroupRulesetState{ID: 1, RulesetVersion: 1}
		if err := tx.Create(&state).Error; err != nil {
			return nil, err
		}
		return &state, nil
	}
	return &state, err
}

func (g *Gateway) SetUpstreamGroupServiceTier(id uint, tier, source string) (*GatewayUpstreamGroup, error) {
	tier = strings.ToLower(strings.TrimSpace(tier))
	if tier != "" && !gatewayServiceTierPattern.MatchString(tier) {
		return nil, errors.New("service tier must use lowercase letters, digits, underscores, or hyphens")
	}
	source = strings.ToLower(strings.TrimSpace(source))
	if source == "" {
		source = "manual"
	}
	if source != "manual" && source != "provider" {
		return nil, errors.New("invalid service tier source")
	}
	var group GatewayUpstreamGroup
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&group, id).Error; err != nil {
			return err
		}
		if group.ServiceTier == tier && group.ServiceTierSource == source {
			return nil
		}
		group.ServiceTier = tier
		group.ServiceTierSource = source
		group.FactsVersion++
		group.UpdatedAt = time.Now()
		return tx.Save(&group).Error
	})
	if err != nil {
		return nil, err
	}
	g.ReevaluateUpstreamGroupAutomaticAssignment(group.ChannelID, group.CanonicalGroupName, true)
	return &group, nil
}

func (g *Gateway) SaveAutomaticAssignmentRule(rule *GatewayCandidateGroupAutoRule, expectedRulesetVersion uint64) (*GatewayCandidateGroupRulesetState, error) {
	if rule == nil {
		return nil, errors.New("automatic assignment rule is required")
	}
	if err := normalizeAndValidateAutomaticRule(rule); err != nil {
		return nil, err
	}
	if expectedRulesetVersion == 0 {
		return nil, ErrGatewayExpectedVersionNeeded
	}
	if rule.ID != 0 && rule.RuleVersion == 0 {
		return nil, ErrGatewayExpectedVersionNeeded
	}
	var state GatewayCandidateGroupRulesetState
	err := g.db.Transaction(func(tx *gorm.DB) error {
		locked, err := g.lockAutomaticAssignmentState(tx)
		if err != nil {
			return err
		}
		if locked.RulesetVersion != expectedRulesetVersion {
			return ErrGatewayRulesetChanged
		}
		if err := lockCandidateGroup(tx, rule.CandidateGroupID); err != nil {
			return err
		}
		if rule.ID == 0 {
			rule.RuleVersion = 1
			if err := tx.Create(rule).Error; err != nil {
				return err
			}
		} else {
			var current GatewayCandidateGroupAutoRule
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, rule.ID).Error; err != nil {
				return err
			}
			if current.RuleVersion != rule.RuleVersion {
				return ErrGatewayRulesetChanged
			}
			rule.RuleVersion = current.RuleVersion + 1
			rule.CreatedAt = current.CreatedAt
			if err := tx.Save(rule).Error; err != nil {
				return err
			}
		}
		locked.RulesetVersion++
		locked.UpdatedAt = time.Now()
		if err := tx.Save(locked).Error; err != nil {
			return err
		}
		state = *locked
		return nil
	})
	if err == nil {
		g.PreviewAutomaticAssignments(nil, true)
	}
	return &state, err
}

func (g *Gateway) DeleteAutomaticAssignmentRule(id uint, expectedRulesetVersion uint64, revokeApplied bool) (*GatewayCandidateGroupRulesetState, error) {
	if expectedRulesetVersion == 0 {
		return nil, ErrGatewayExpectedVersionNeeded
	}
	var state GatewayCandidateGroupRulesetState
	err := g.db.Transaction(func(tx *gorm.DB) error {
		locked, err := g.lockAutomaticAssignmentState(tx)
		if err != nil {
			return err
		}
		if locked.RulesetVersion != expectedRulesetVersion {
			return ErrGatewayRulesetChanged
		}
		var rule GatewayCandidateGroupAutoRule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&rule, id).Error; err != nil {
			return err
		}
		if revokeApplied {
			if err := revokeMembershipClaimsTx(tx, "auto_rule", uint64(rule.ID), time.Now()); err != nil {
				return err
			}
		}
		if err := tx.Delete(&rule).Error; err != nil {
			return err
		}
		locked.RulesetVersion++
		locked.UpdatedAt = time.Now()
		if err := tx.Save(locked).Error; err != nil {
			return err
		}
		state = *locked
		return nil
	})
	if err == nil {
		g.PreviewAutomaticAssignments(nil, true)
	}
	return &state, err
}

func normalizeAndValidateAutomaticRule(rule *GatewayCandidateGroupAutoRule) error {
	if rule.CandidateGroupID == 0 {
		return errors.New("candidate group is required")
	}
	rule.ModelFamilies = normalizeCodes(rule.ModelFamilies)
	rule.GatewayApps = normalizeCodes(rule.GatewayApps)
	rule.AcceptedAppSources = normalizeCodes(rule.AcceptedAppSources)
	rule.ChannelTypes = normalizeCodes(rule.ChannelTypes)
	rule.ServiceTiers = normalizeCodes(rule.ServiceTiers)
	if rule.ModelFamilies == nil {
		rule.ModelFamilies = []string{}
	}
	if rule.GatewayApps == nil {
		rule.GatewayApps = []string{}
	}
	if rule.ChannelTypes == nil {
		rule.ChannelTypes = []string{}
	}
	if rule.ServiceTiers == nil {
		rule.ServiceTiers = []string{}
	}
	if rule.ModelIDConditions == nil {
		rule.ModelIDConditions = []GatewayModelIDCondition{}
	}
	if rule.CapabilityConditions == nil {
		rule.CapabilityConditions = []GatewayCapabilityCondition{}
	}
	if len(rule.AcceptedAppSources) == 0 {
		rule.AcceptedAppSources = []string{"catalog_compatible"}
	}
	if rule.FamilyMatchMode == "" {
		rule.FamilyMatchMode = "single_any"
	}
	if rule.AppMatchMode == "" {
		rule.AppMatchMode = "exact_set"
	}
	if rule.ModelConditionQuantifier == "" {
		rule.ModelConditionQuantifier = "any"
	}
	if rule.ModelFactCoverage == "" {
		rule.ModelFactCoverage = "matched"
	}
	if !stringIn(rule.FamilyMatchMode, "single_any", "exact_set", "contains_all") {
		return errors.New("invalid family match mode")
	}
	if !stringIn(rule.AppMatchMode, "exact_set", "contains_all") {
		return errors.New("invalid app match mode")
	}
	if !stringIn(rule.ModelConditionQuantifier, "any", "all") || !stringIn(rule.ModelFactCoverage, "matched", "all") {
		return errors.New("invalid model id matching options")
	}
	knownFamilies := make(map[string]struct{})
	for _, family := range modelcatalog.FamilyOrder() {
		knownFamilies[family] = struct{}{}
	}
	for _, family := range rule.ModelFamilies {
		if family == "other" {
			return errors.New("unclassified model family cannot be used for automatic assignment")
		}
		if _, ok := knownFamilies[family]; !ok {
			return fmt.Errorf("unknown model family %q", family)
		}
	}
	for _, app := range rule.GatewayApps {
		if !stringIn(app, "codex", "claude", "gemini") {
			return fmt.Errorf("unknown gateway app %q", app)
		}
	}
	for _, source := range rule.AcceptedAppSources {
		if !stringIn(source, "catalog_compatible", "probed") {
			return fmt.Errorf("unknown gateway app evidence source %q", source)
		}
	}
	for _, channelType := range rule.ChannelTypes {
		if !stringIn(channelType, string(ChannelTypeNewAPI), string(ChannelTypeSub2API)) {
			return fmt.Errorf("unknown channel type %q", channelType)
		}
	}
	for _, tier := range rule.ServiceTiers {
		if !gatewayServiceTierPattern.MatchString(tier) {
			return fmt.Errorf("invalid service tier %q", tier)
		}
	}
	conditions := make([]GatewayModelIDCondition, 0, len(rule.ModelIDConditions))
	for _, condition := range rule.ModelIDConditions {
		condition.Match = strings.ToLower(strings.TrimSpace(condition.Match))
		condition.Value = strings.TrimSpace(condition.Value)
		if !stringIn(condition.Match, "exact", "prefix") || condition.Value == "" {
			return errors.New("model id conditions require exact or prefix and a value")
		}
		conditions = append(conditions, condition)
	}
	rule.ModelIDConditions = conditions
	if len(rule.ModelFamilies) == 0 && (len(rule.ModelIDConditions) == 0 || rule.ModelFactCoverage != "all") {
		return errors.New("automatic rules require model families or model id conditions with full fact coverage")
	}
	capabilityConditions := make([]GatewayCapabilityCondition, 0, len(rule.CapabilityConditions))
	for _, condition := range rule.CapabilityConditions {
		condition.SelectorType = strings.ToLower(strings.TrimSpace(condition.SelectorType))
		condition.Quantifier = strings.ToLower(strings.TrimSpace(condition.Quantifier))
		condition.EvidenceSource = strings.ToLower(strings.TrimSpace(condition.EvidenceSource))
		condition.SelectorValues = uniqueSortedStrings(condition.SelectorValues)
		if !stringIn(condition.SelectorType, "family", "exact", "prefix", "matched_model_ids") || !stringIn(condition.Quantifier, "any_model", "all_models") {
			return errors.New("invalid capability condition selector")
		}
		if condition.SelectorType == "matched_model_ids" && len(condition.SelectorValues) > 0 {
			return errors.New("matched model id capability selectors cannot include selector values")
		}
		if condition.SelectorType != "matched_model_ids" && len(condition.SelectorValues) == 0 {
			return errors.New("capability condition selector values are required")
		}
		if condition.EvidenceSource != "probed" || condition.MaxAgeHours <= 0 {
			return errors.New("capability conditions require probed evidence and a positive freshness window")
		}
		if condition.MinContextWindow <= 0 && condition.MinSafeInput <= 0 {
			return errors.New("capability condition requires a minimum context or safe input size")
		}
		capabilityConditions = append(capabilityConditions, condition)
	}
	rule.CapabilityConditions = capabilityConditions
	return nil
}

func normalizeCodes(values []string) []string {
	for index := range values {
		values[index] = strings.ToLower(strings.TrimSpace(values[index]))
	}
	return uniqueSortedStrings(values)
}

func stringIn(value string, allowed ...string) bool {
	for _, item := range allowed {
		if value == item {
			return true
		}
	}
	return false
}

func (g *Gateway) PreviewAutomaticAssignments(changeIDs []uint, persist bool) ([]GatewayAutomaticAssignmentPreview, error) {
	query := g.db.Where("kind = ? AND status = ?", "added", "pending").Order("id ASC")
	if len(changeIDs) > 0 {
		query = query.Where("id IN ?", changeIDs)
	}
	changes := []GatewayUpstreamGroupChange{}
	if err := query.Find(&changes).Error; err != nil {
		return nil, err
	}
	previews := make([]GatewayAutomaticAssignmentPreview, 0, len(changes))
	for _, change := range changes {
		preview, err := g.previewAutomaticAssignment(change.ID, persist, nil)
		if err != nil {
			return nil, err
		}
		previews = append(previews, *preview)
	}
	return previews, nil
}

func (g *Gateway) PreviewAutomaticAssignmentDraft(rule GatewayCandidateGroupAutoRule, changeIDs []uint) ([]GatewayAutomaticAssignmentPreview, error) {
	if err := normalizeAndValidateAutomaticRule(&rule); err != nil {
		return nil, err
	}
	query := g.db.Where("kind = ? AND status = ?", "added", "pending").Order("id ASC")
	if len(changeIDs) > 0 {
		query = query.Where("id IN ?", changeIDs)
	}
	changes := []GatewayUpstreamGroupChange{}
	if err := query.Find(&changes).Error; err != nil {
		return nil, err
	}
	previews := make([]GatewayAutomaticAssignmentPreview, 0, len(changes))
	for _, change := range changes {
		preview, err := g.previewAutomaticAssignment(change.ID, false, &rule)
		if err != nil {
			return nil, err
		}
		preview.EvaluationToken = ""
		previews = append(previews, *preview)
	}
	return previews, nil
}

func (g *Gateway) previewAutomaticAssignment(changeID uint, persist bool, draft *GatewayCandidateGroupAutoRule) (*GatewayAutomaticAssignmentPreview, error) {
	var preview *GatewayAutomaticAssignmentPreview
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&change, changeID).Error; err != nil {
			return err
		}
		result, err := evaluateAutomaticAssignmentTx(tx, &change, draft)
		if err != nil {
			return err
		}
		if persist && draft == nil {
			if err := tx.Model(&change).Updates(map[string]any{
				"assignment_state":              result.State,
				"assignment_reason_code":        result.ReasonCode,
				"suggested_candidate_group_ids": gatewayJSONB(result.SuggestedCandidateGroupIDs),
				"evaluated_at":                  result.EvaluatedAt,
				"evaluation_token":              result.EvaluationToken,
				"facts_digest":                  resultFactsDigest(result),
			}).Error; err != nil {
				return err
			}
		}
		preview = result
		return nil
	})
	return preview, err
}

func evaluateAutomaticAssignmentTx(tx *gorm.DB, change *GatewayUpstreamGroupChange, draft *GatewayCandidateGroupAutoRule) (*GatewayAutomaticAssignmentPreview, error) {
	if change == nil || change.Kind != "added" || change.Status != "pending" || change.UpstreamGroupID == nil || change.PresenceGeneration == nil {
		return nil, ErrUpstreamGroupChangeSuperseded
	}
	var group GatewayUpstreamGroup
	if err := tx.First(&group, *change.UpstreamGroupID).Error; err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	result := &GatewayAutomaticAssignmentPreview{
		ChangeID: change.ID, UpstreamGroupID: group.ID, ChannelID: group.ChannelID,
		GroupName: change.GroupName, PresenceGeneration: group.PresenceGeneration,
		ModelFamilyCodes: append([]string(nil), group.ModelFamilyCodes...), ModelIDs: append([]string(nil), group.ModelIDs...),
		GatewayApps: append([]string(nil), group.GatewayApps...), ChannelType: string(group.ChannelType),
		ServiceTier: group.ServiceTier, ServiceTierSource: group.ServiceTierSource,
		MissingFacts: []string{}, SuggestedCandidateGroupIDs: []uint{}, MatchedRuleIDs: []uint{},
		RuleMatches: []GatewayAutomaticRuleMatch{}, EvaluatedAt: now,
	}
	if !group.Present || group.PresenceGeneration != *change.PresenceGeneration {
		result.State, result.ReasonCode = "superseded", "group_generation_changed"
		return result, nil
	}

	state, err := automaticAssignmentStateTx(tx)
	if err != nil {
		return nil, err
	}
	var override GatewayUpstreamGroupAssignmentOverride
	overrideErr := tx.Where("upstream_group_id = ? AND presence_generation = ?", group.ID, group.PresenceGeneration).First(&override).Error
	if overrideErr != nil && !errors.Is(overrideErr, gorm.ErrRecordNotFound) {
		return nil, overrideErr
	}
	if overrideErr == nil {
		if override.Action == "ignore" {
			result.State, result.ReasonCode = "ready", "manual_ignore_override"
			result.EvaluationToken, err = automaticEvaluationToken(tx, change, &group, state, override.OverrideVersion, nil)
			return result, err
		}
		if err := tx.Where("override_id = ?", override.ID).Order("candidate_group_id_snapshot ASC").Find(&override.Targets).Error; err != nil {
			return nil, err
		}
		for _, target := range override.Targets {
			if target.CandidateGroupID == nil {
				result.State, result.ReasonCode = "conflict", "override_target_deleted"
				result.SuggestedCandidateGroupIDs = append(result.SuggestedCandidateGroupIDs, target.CandidateGroupIDSnapshot)
				result.EvaluationToken, err = automaticEvaluationToken(tx, change, &group, state, override.OverrideVersion, result.SuggestedCandidateGroupIDs)
				return result, err
			}
			result.SuggestedCandidateGroupIDs = append(result.SuggestedCandidateGroupIDs, *target.CandidateGroupID)
		}
		result.State, result.ReasonCode = "ready", "manual_assign_override"
		if !group.FactsComplete {
			result.ReasonCode = "manual_override_incomplete_facts"
			result.MissingFacts = uniqueSortedStrings(append(result.MissingFacts, group.IncompleteReasons...))
		}
		result.EvaluationToken, err = automaticEvaluationToken(tx, change, &group, state, override.OverrideVersion, result.SuggestedCandidateGroupIDs)
		return result, err
	}

	if !group.FactsComplete || group.IdentityConflict || len(group.UnclassifiedModelIDs) > 0 || len(group.AmbiguousModelIDs) > 0 {
		result.State, result.ReasonCode = "awaiting_facts", "facts_incomplete"
		result.MissingFacts = append(result.MissingFacts, group.IncompleteReasons...)
		if group.IdentityConflict {
			result.MissingFacts = append(result.MissingFacts, "identity_conflict")
		}
		result.MissingFacts = uniqueSortedStrings(result.MissingFacts)
		result.EvaluationToken, err = automaticEvaluationToken(tx, change, &group, state, 0, nil)
		return result, err
	}

	rules := []GatewayCandidateGroupAutoRule{}
	query := tx.Where("enabled = ?", true)
	if draft != nil && draft.ID > 0 {
		query = query.Where("id <> ?", draft.ID)
	}
	if err := query.Order("priority DESC, id ASC").Find(&rules).Error; err != nil {
		return nil, err
	}
	if draft != nil && draft.Enabled {
		rules = append(rules, *draft)
	}
	highestPriority := 0
	hasHighest := false
	targets := make(map[uint]struct{})
	incompleteMatches := make([]GatewayAutomaticRuleMatch, 0)
	for _, rule := range rules {
		match := evaluateAutomaticRule(rule, group, now)
		result.RuleMatches = append(result.RuleMatches, match)
		if match.FactsIncomplete {
			incompleteMatches = append(incompleteMatches, match)
			continue
		}
		if !match.Matched {
			continue
		}
		result.MatchedRuleIDs = append(result.MatchedRuleIDs, rule.ID)
		if !hasHighest || rule.Priority > highestPriority {
			hasHighest, highestPriority = true, rule.Priority
			targets = map[uint]struct{}{rule.CandidateGroupID: {}}
		} else if rule.Priority == highestPriority {
			targets[rule.CandidateGroupID] = struct{}{}
		}
	}
	for _, match := range incompleteMatches {
		if !hasHighest || match.Priority >= highestPriority {
			result.MissingFacts = append(result.MissingFacts, match.MissingFacts...)
		}
	}
	result.MissingFacts = uniqueSortedStrings(result.MissingFacts)
	if len(result.MissingFacts) > 0 {
		result.State, result.ReasonCode = "awaiting_facts", result.MissingFacts[0]
	} else if !hasHighest {
		result.State, result.ReasonCode = "unmatched", "no_rule_matched"
	} else {
		for target := range targets {
			result.SuggestedCandidateGroupIDs = append(result.SuggestedCandidateGroupIDs, target)
		}
		sort.Slice(result.SuggestedCandidateGroupIDs, func(i, j int) bool {
			return result.SuggestedCandidateGroupIDs[i] < result.SuggestedCandidateGroupIDs[j]
		})
		if len(result.SuggestedCandidateGroupIDs) == 1 {
			result.State, result.ReasonCode = "ready", "unique_highest_priority_match"
		} else {
			result.State, result.ReasonCode = "conflict", "equal_priority_targets"
		}
	}
	result.EvaluationToken, err = automaticEvaluationToken(tx, change, &group, state, 0, result.SuggestedCandidateGroupIDs)
	return result, err
}

func automaticAssignmentStateTx(tx *gorm.DB) (*GatewayCandidateGroupRulesetState, error) {
	var state GatewayCandidateGroupRulesetState
	if err := tx.First(&state, int16(1)).Error; err != nil {
		return nil, err
	}
	return &state, nil
}

func evaluateAutomaticRule(rule GatewayCandidateGroupAutoRule, group GatewayUpstreamGroup, now time.Time) GatewayAutomaticRuleMatch {
	match := GatewayAutomaticRuleMatch{RuleID: rule.ID, CandidateGroupID: rule.CandidateGroupID, Priority: rule.Priority, Matched: true, Reasons: []string{}, MissingFacts: []string{}}
	fail := func(reason string) { match.Matched = false; match.Reasons = append(match.Reasons, reason) }
	incomplete := func(reason string) {
		match.Matched = false
		match.FactsIncomplete = true
		match.MissingFacts = append(match.MissingFacts, reason)
	}

	if len(rule.ModelFamilies) > 0 {
		switch rule.FamilyMatchMode {
		case "single_any":
			if len(group.ModelFamilyCodes) != 1 || !containsString(rule.ModelFamilies, group.ModelFamilyCodes[0]) {
				fail("model_family_not_matched")
			} else {
				match.Reasons = append(match.Reasons, "model_family_matched")
			}
		case "exact_set":
			if !sameStringSet(group.ModelFamilyCodes, rule.ModelFamilies) {
				fail("model_family_set_not_exact")
			} else {
				match.Reasons = append(match.Reasons, "model_family_set_exact")
			}
		case "contains_all":
			if !containsAllStrings(group.ModelFamilyCodes, rule.ModelFamilies) {
				fail("model_family_missing")
			} else {
				match.Reasons = append(match.Reasons, "model_family_contains_all")
			}
		}
	}
	if len(rule.GatewayApps) > 0 {
		appsMatched := sameStringSet(group.GatewayApps, rule.GatewayApps)
		if rule.AppMatchMode == "contains_all" {
			appsMatched = containsAllStrings(group.GatewayApps, rule.GatewayApps)
		}
		if !appsMatched {
			fail("gateway_app_not_matched")
		} else {
			for _, app := range rule.GatewayApps {
				if !gatewayAppEvidenceAccepted(group.GatewayAppSources[app], rule.AcceptedAppSources) {
					incomplete("gateway_app_evidence_missing")
				}
			}
			match.Reasons = append(match.Reasons, "gateway_app_matched")
		}
	}
	if len(rule.ChannelTypes) > 0 {
		if !containsString(rule.ChannelTypes, string(group.ChannelType)) {
			fail("channel_type_not_matched")
		} else {
			match.Reasons = append(match.Reasons, "channel_type_matched")
		}
	}
	if len(rule.ServiceTiers) > 0 {
		if strings.TrimSpace(group.ServiceTier) == "" {
			incomplete("service_tier_missing")
		} else if !containsString(rule.ServiceTiers, group.ServiceTier) {
			fail("service_tier_not_matched")
		} else {
			match.Reasons = append(match.Reasons, "service_tier_matched")
		}
	}
	if len(rule.ModelIDConditions) > 0 {
		matchedModels := make(map[string]struct{})
		conditionHits := make([]bool, len(rule.ModelIDConditions))
		for conditionIndex, condition := range rule.ModelIDConditions {
			for _, modelID := range group.ModelIDs {
				matched := modelID == condition.Value
				if condition.Match == "prefix" {
					matched = strings.HasPrefix(modelID, condition.Value)
				}
				if matched {
					conditionHits[conditionIndex] = true
					matchedModels[modelID] = struct{}{}
				}
			}
		}
		conditionsMatched := false
		if rule.ModelConditionQuantifier == "all" {
			conditionsMatched = true
			for _, hit := range conditionHits {
				conditionsMatched = conditionsMatched && hit
			}
		} else {
			for _, hit := range conditionHits {
				conditionsMatched = conditionsMatched || hit
			}
		}
		if rule.ModelFactCoverage == "all" && len(matchedModels) != len(group.ModelIDs) {
			conditionsMatched = false
		}
		if !conditionsMatched {
			fail("model_id_not_matched")
		} else {
			match.Reasons = append(match.Reasons, "model_id_matched")
		}
	}
	for _, condition := range rule.CapabilityConditions {
		models := capabilitySelectorModels(condition, rule, group)
		if len(models) == 0 {
			incomplete("capability_selector_empty")
			continue
		}
		hits := 0
		available := 0
		for _, modelID := range models {
			profile, ok := group.CapabilityProfile[modelID]
			if !ok || !capabilityFresh(profile, condition, group.PresenceGeneration, now) {
				continue
			}
			available++
			if capabilityThresholdsMatch(profile, condition) {
				hits++
			}
		}
		if available != len(models) {
			incomplete("capability_evidence_missing_or_stale")
			continue
		}
		conditionMatched := hits > 0
		if condition.Quantifier == "all_models" {
			conditionMatched = hits == len(models)
		}
		if !conditionMatched {
			fail("capability_threshold_not_matched")
		} else {
			match.Reasons = append(match.Reasons, "capability_matched")
		}
	}
	match.Reasons = uniqueSortedStrings(match.Reasons)
	match.MissingFacts = uniqueSortedStrings(match.MissingFacts)
	for _, reason := range match.Reasons {
		if stringIn(reason, "model_family_not_matched", "model_family_set_not_exact", "model_family_missing", "gateway_app_not_matched", "channel_type_not_matched", "service_tier_not_matched", "model_id_not_matched", "capability_threshold_not_matched") {
			match.FactsIncomplete = false
			match.MissingFacts = []string{}
			break
		}
	}
	return match
}

func gatewayAppEvidenceAccepted(actual string, accepted []string) bool {
	if containsString(accepted, actual) {
		return true
	}
	// A successful protocol probe is stronger evidence than catalog compatibility.
	return actual == "probed" && containsString(accepted, "catalog_compatible")
}

func capabilitySelectorModels(condition GatewayCapabilityCondition, rule GatewayCandidateGroupAutoRule, group GatewayUpstreamGroup) []string {
	selected := []string{}
	for _, modelID := range group.ModelIDs {
		switch condition.SelectorType {
		case "family":
			families := modelcatalog.FamiliesFromModel(modelID)
			for _, family := range families {
				if containsString(condition.SelectorValues, family) {
					selected = append(selected, modelID)
				}
			}
		case "exact":
			if containsString(condition.SelectorValues, modelID) {
				selected = append(selected, modelID)
			}
		case "prefix":
			for _, prefix := range condition.SelectorValues {
				if strings.HasPrefix(modelID, prefix) {
					selected = append(selected, modelID)
				}
			}
		case "matched_model_ids":
			for _, modelCondition := range rule.ModelIDConditions {
				if (modelCondition.Match == "exact" && modelID == modelCondition.Value) || (modelCondition.Match == "prefix" && strings.HasPrefix(modelID, modelCondition.Value)) {
					selected = append(selected, modelID)
				}
			}
		}
	}
	return uniqueSortedStrings(selected)
}

func capabilityFresh(profile map[string]any, condition GatewayCapabilityCondition, generation uint64, now time.Time) bool {
	if supported, ok := profile["supported"].(bool); !ok || !supported {
		return false
	}
	if fmt.Sprint(profile["evidence_source"]) != condition.EvidenceSource {
		return false
	}
	if uint64(numberFromAny(profile["presence_generation"])) != generation {
		return false
	}
	checked, err := time.Parse(time.RFC3339Nano, fmt.Sprint(profile["last_checked_at"]))
	return err == nil && !checked.Add(time.Duration(condition.MaxAgeHours)*time.Hour).Before(now)
}

func capabilityThresholdsMatch(profile map[string]any, condition GatewayCapabilityCondition) bool {
	return (condition.MinContextWindow <= 0 || numberFromAny(profile["context_window_tokens"]) >= condition.MinContextWindow) &&
		(condition.MinSafeInput <= 0 || numberFromAny(profile["safe_input_tokens"]) >= condition.MinSafeInput)
}

func numberFromAny(value any) int64 {
	switch number := value.(type) {
	case int:
		return int64(number)
	case int64:
		return number
	case uint64:
		return int64(number)
	case float64:
		return int64(number)
	case json.Number:
		parsed, _ := number.Int64()
		return parsed
	default:
		return 0
	}
}

func automaticEvaluationToken(tx *gorm.DB, change *GatewayUpstreamGroupChange, group *GatewayUpstreamGroup, state *GatewayCandidateGroupRulesetState, overrideVersion uint64, candidateGroupIDs []uint) (string, error) {
	type targetVersion struct {
		ID                uint   `json:"id"`
		MembershipVersion uint64 `json:"membership_version"`
	}
	targets := make([]targetVersion, 0, len(candidateGroupIDs))
	if len(candidateGroupIDs) > 0 {
		var groups []GatewayCandidateGroup
		if err := tx.Select("id", "membership_version").Where("id IN ?", candidateGroupIDs).Order("id ASC").Find(&groups).Error; err != nil {
			return "", err
		}
		for _, target := range groups {
			targets = append(targets, targetVersion{ID: target.ID, MembershipVersion: target.MembershipVersion})
		}
	}
	payload := struct {
		ChangeID            uint            `json:"change_id"`
		ChangeStatus        string          `json:"change_status"`
		Generation          uint64          `json:"generation"`
		FactsVersion        uint64          `json:"facts_version"`
		FactsRevision       string          `json:"facts_revision"`
		ServiceTier         string          `json:"service_tier"`
		Classification      string          `json:"classification"`
		RulesetVersion      uint64          `json:"ruleset_version"`
		OverrideVersion     uint64          `json:"override_version"`
		TargetGroupVersions []targetVersion `json:"target_group_versions"`
	}{change.ID, change.Status, group.PresenceGeneration, group.FactsVersion, group.SourceRevision, group.ServiceTier, group.ClassificationVersion, state.RulesetVersion, overrideVersion, targets}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func resultFactsDigest(result *GatewayAutomaticAssignmentPreview) string {
	payload := struct {
		Generation uint64   `json:"generation"`
		Families   []string `json:"families"`
		Models     []string `json:"models"`
		Apps       []string `json:"apps"`
		Tier       string   `json:"tier"`
	}{result.PresenceGeneration, result.ModelFamilyCodes, result.ModelIDs, result.GatewayApps, result.ServiceTier}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func gatewayJSONB(value any) clause.Expr {
	raw, _ := json.Marshal(value)
	return gorm.Expr("?::jsonb", string(raw))
}

func (g *Gateway) ExecuteAutomaticAssignment(changeID uint, token, actor, trigger string) (*GatewayAutomaticAssignmentPreview, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, errors.New("evaluation token is required")
	}
	if strings.TrimSpace(actor) == "" {
		actor = "administrator"
	}
	trigger = strings.TrimSpace(trigger)
	if trigger != GatewayAssignmentTriggerSync && trigger != GatewayAssignmentTriggerRuleRecheck && trigger != GatewayAssignmentTriggerManual {
		return nil, fmt.Errorf("invalid automatic assignment trigger %q", trigger)
	}
	var observedChange GatewayUpstreamGroupChange
	if err := g.db.Select("id", "channel_id").First(&observedChange, changeID).Error; err != nil {
		return nil, err
	}
	if observedChange.ChannelID == 0 {
		return nil, ErrUpstreamGroupGenerationChanged
	}
	var result *GatewayAutomaticAssignmentPreview
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if err := lockUpstreamGroupParentChannelTx(tx, observedChange.ChannelID); err != nil {
			return err
		}
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&change, changeID).Error; err != nil {
			return err
		}
		if change.ChannelID != observedChange.ChannelID {
			return ErrUpstreamGroupGenerationChanged
		}
		lockedState, err := g.lockAutomaticAssignmentState(tx)
		if err != nil {
			return err
		}
		preview, err := evaluateAutomaticAssignmentTx(tx, &change, nil)
		if err != nil {
			return err
		}
		if preview.EvaluationToken != token || change.EvaluationToken == nil || *change.EvaluationToken != token {
			return ErrGatewayEvaluationStale
		}
		if preview.State != "ready" {
			return fmt.Errorf("automatic assignment is not ready: %s", preview.ReasonCode)
		}
		persistentOverride := preview.ReasonCode == "manual_ignore_override" || preview.ReasonCode == "manual_assign_override" || preview.ReasonCode == "manual_override_incomplete_facts"
		if actor == "system" && !persistentOverride && (!lockedState.BackfillComplete || !lockedState.AutoExecuteEnabled) {
			return ErrGatewayEvaluationStale
		}
		var override GatewayUpstreamGroupAssignmentOverride
		overrideErr := tx.Where("upstream_group_id = ? AND presence_generation = ?", preview.UpstreamGroupID, preview.PresenceGeneration).First(&override).Error
		if overrideErr != nil && !errors.Is(overrideErr, gorm.ErrRecordNotFound) {
			return overrideErr
		}
		if len(preview.SuggestedCandidateGroupIDs) != 1 && overrideErr != nil {
			return errors.New("automatic assignment requires exactly one target")
		}
		for _, candidateGroupID := range preview.SuggestedCandidateGroupIDs {
			if err := lockCandidateGroup(tx, candidateGroupID); err != nil {
				return err
			}
		}
		if _, err := lockUpstreamRoutingHealthDomainAdvisoryTx(tx, preview.ChannelID, preview.GroupName); err != nil {
			return err
		}
		var group GatewayUpstreamGroup
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&group, preview.UpstreamGroupID).Error; err != nil {
			return err
		}
		lockedPreview, err := evaluateAutomaticAssignmentTx(tx, &change, nil)
		if err != nil {
			return err
		}
		if lockedPreview.EvaluationToken != token {
			return ErrGatewayEvaluationStale
		}
		preview = lockedPreview
		state, err := automaticAssignmentStateTx(tx)
		if err != nil {
			return err
		}
		if overrideErr == nil && override.Action == "ignore" {
			return finalizeAutomaticChangeTx(tx, &change, &group, state, &override, preview, "ignored", "override_ignore", actor)
		}
		if overrideErr == nil && override.Action == "assign" {
			if err := lockCurrentGenerationAssignmentKeyTx(tx, upstreamGroupGeneration(group), &change); err != nil {
				return err
			}
		}
		action := "auto_assigned"
		if overrideErr == nil {
			action = "override_assigned"
			if preview.ReasonCode == "manual_override_incomplete_facts" {
				action = preview.ReasonCode
			}
		}
		auditTrigger := trigger
		if overrideErr == nil {
			auditTrigger = GatewayAssignmentTriggerOverrideReapply
		}
		audit, err := createAssignmentAuditTx(tx, &change, &group, state, overrideVersion(&override, overrideErr), action, auditTrigger, preview, actor)
		if err != nil {
			return err
		}
		if overrideErr == nil {
			for _, targetID := range preview.SuggestedCandidateGroupIDs {
				if err := ensureMembershipClaimTx(tx, targetID, &group, change.GroupName, "override", uint64(override.ID), time.Now()); err != nil {
					return err
				}
			}
		} else {
			winning := make(map[uint]struct{})
			winningPriority := 0
			hasWinningPriority := false
			for _, match := range preview.RuleMatches {
				if match.Matched && match.CandidateGroupID == preview.SuggestedCandidateGroupIDs[0] {
					if !hasWinningPriority || match.Priority > winningPriority {
						hasWinningPriority = true
						winningPriority = match.Priority
						winning = map[uint]struct{}{match.RuleID: {}}
					} else if match.Priority == winningPriority {
						winning[match.RuleID] = struct{}{}
					}
				}
			}
			for ruleID := range winning {
				if err := ensureMembershipClaimTx(tx, preview.SuggestedCandidateGroupIDs[0], &group, change.GroupName, "auto_rule", uint64(ruleID), time.Now()); err != nil {
					return err
				}
			}
		}
		if err := tx.Model(&change).Updates(map[string]any{
			"status": "assigned", "assignment_state": "assigned", "assignment_reason_code": preview.ReasonCode,
			"resolved_at": time.Now(), "evaluated_at": preview.EvaluatedAt, "evaluation_token": token,
			"suggested_candidate_group_ids": gatewayJSONB(preview.SuggestedCandidateGroupIDs),
		}).Error; err != nil {
			return err
		}
		_ = audit
		result = preview
		return nil
	})
	return result, err
}

func saveAssignmentOverrideTx(tx *gorm.DB, upstreamGroupID uint, presenceGeneration uint64, action string, candidateGroupIDs []uint, reason, actor string, at time.Time) (*GatewayUpstreamGroupAssignmentOverride, error) {
	if !stringIn(action, "assign", "ignore") {
		return nil, errors.New("invalid assignment override action")
	}
	if upstreamGroupID == 0 || presenceGeneration == 0 {
		return nil, ErrUpstreamGroupGenerationChanged
	}
	if action == "assign" && len(candidateGroupIDs) == 0 {
		return nil, errors.New("assignment override requires a target")
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "administrator"
	}
	var current GatewayUpstreamGroup
	currentErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "present", "presence_generation").
		First(&current, upstreamGroupID).Error
	if errors.Is(currentErr, gorm.ErrRecordNotFound) || currentErr == nil && (!current.Present || current.PresenceGeneration != presenceGeneration) {
		return nil, ErrUpstreamGroupGenerationChanged
	}
	if currentErr != nil {
		return nil, currentErr
	}
	var override GatewayUpstreamGroupAssignmentOverride
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("upstream_group_id = ?", upstreamGroupID).First(&override).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		override = GatewayUpstreamGroupAssignmentOverride{
			UpstreamGroupID: upstreamGroupID, PresenceGeneration: presenceGeneration,
			Action: action, Reason: strings.TrimSpace(reason), CreatedBy: actor,
			OverrideVersion: 1, CreatedAt: at, UpdatedAt: at,
		}
		if err := tx.Create(&override).Error; err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		if override.PresenceGeneration != presenceGeneration {
			if err := revokeMembershipClaimsTx(tx, "override", uint64(override.ID), at); err != nil {
				return nil, err
			}
		}
		override.PresenceGeneration = presenceGeneration
		override.Action = action
		override.Reason = strings.TrimSpace(reason)
		override.CreatedBy = actor
		override.OverrideVersion++
		override.UpdatedAt = at
		if err := tx.Save(&override).Error; err != nil {
			return nil, err
		}
		if err := tx.Where("override_id = ?", override.ID).Delete(&GatewayUpstreamGroupAssignmentOverrideTarget{}).Error; err != nil {
			return nil, err
		}
	}
	if action == "assign" {
		for _, candidateGroupID := range candidateGroupIDs {
			targetID := candidateGroupID
			target := GatewayUpstreamGroupAssignmentOverrideTarget{
				OverrideID: override.ID, CandidateGroupID: &targetID, CandidateGroupIDSnapshot: candidateGroupID, CreatedAt: at,
			}
			if err := tx.Create(&target).Error; err != nil {
				return nil, err
			}
		}
	}
	return &override, nil
}

func syncManualCandidateOverrideTx(tx *gorm.DB, upstreamGroupID, candidateGroupID uint, selected bool, actor string, at time.Time) error {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "administrator"
	}
	var group GatewayUpstreamGroup
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&group, upstreamGroupID).Error; err != nil {
		return err
	}
	if !group.Present || group.PresenceGeneration == 0 {
		return ErrUpstreamGroupGenerationChanged
	}
	var existing GatewayUpstreamGroupAssignmentOverride
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("upstream_group_id = ?", upstreamGroupID).First(&existing).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	targetSet, err := activeCandidateGroupIDsForGenerationTx(tx, group.ID, group.PresenceGeneration)
	if err != nil {
		return err
	}
	tombstoneSnapshots := make(map[uint]struct{})
	if existing.ID != 0 && existing.PresenceGeneration == group.PresenceGeneration && existing.Action == "assign" {
		var targets []GatewayUpstreamGroupAssignmentOverrideTarget
		if err := tx.Where("override_id = ?", existing.ID).Find(&targets).Error; err != nil {
			return err
		}
		for _, target := range targets {
			if target.CandidateGroupID == nil {
				tombstoneSnapshots[target.CandidateGroupIDSnapshot] = struct{}{}
			}
		}
	}
	if selected {
		targetSet[candidateGroupID] = struct{}{}
	} else {
		delete(targetSet, candidateGroupID)
	}
	if existing.ID != 0 {
		if err := revokeMembershipClaimsTx(tx, "override", uint64(existing.ID), at); err != nil {
			return err
		}
	}
	targets := make([]uint, 0, len(targetSet))
	for targetID := range targetSet {
		targets = append(targets, targetID)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
	var override *GatewayUpstreamGroupAssignmentOverride
	if len(targets) > 0 {
		override, err = saveAssignmentOverrideTx(tx, upstreamGroupID, group.PresenceGeneration, "assign", targets, "Candidate groups saved by administrator", actor, at)
		if err != nil {
			return err
		}
	} else if len(tombstoneSnapshots) == 0 {
		override, err = saveAssignmentOverrideTx(tx, upstreamGroupID, group.PresenceGeneration, "ignore", nil, "Candidate groups saved by administrator; no targets selected", actor, at)
		if err != nil {
			return err
		}
	} else {
		if existing.ID == 0 || existing.PresenceGeneration != group.PresenceGeneration {
			return errors.New("deleted assignment targets require a current-generation override")
		}
		existing.PresenceGeneration = group.PresenceGeneration
		existing.Action = "assign"
		existing.Reason = "Candidate groups saved by administrator; deleted targets require explicit resolution"
		existing.CreatedBy = actor
		existing.OverrideVersion++
		existing.UpdatedAt = at
		if err := tx.Save(&existing).Error; err != nil {
			return err
		}
		if err := tx.Where("override_id = ?", existing.ID).Delete(&GatewayUpstreamGroupAssignmentOverrideTarget{}).Error; err != nil {
			return err
		}
		override = &existing
	}
	for snapshot := range tombstoneSnapshots {
		tombstone := GatewayUpstreamGroupAssignmentOverrideTarget{
			OverrideID: override.ID, CandidateGroupID: nil, CandidateGroupIDSnapshot: snapshot, CreatedAt: at,
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&tombstone).Error; err != nil {
			return err
		}
	}
	if err := createManualOverrideAuditTx(tx, &group, override, actor, at); err != nil {
		return err
	}
	if err := revokeCurrentGenerationClaimsOutsideTargetsTx(tx, &group, targetSet, at); err != nil {
		return err
	}
	rawName := group.CanonicalGroupName
	if len(group.RawGroupNames) > 0 {
		rawName = group.RawGroupNames[0]
	}
	for _, targetID := range targets {
		if err := ensureMembershipClaimTx(tx, targetID, &group, rawName, "override", uint64(override.ID), at); err != nil {
			return err
		}
	}
	return nil
}

func activeCandidateGroupIDsForGenerationTx(tx *gorm.DB, upstreamGroupID uint, presenceGeneration uint64) (map[uint]struct{}, error) {
	var ids []uint
	if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
		Distinct("gateway_candidate_group_membership_claims.candidate_group_id").
		Joins("JOIN gateway_candidate_groups ON gateway_candidate_groups.id = gateway_candidate_group_membership_claims.candidate_group_id").
		Where("gateway_candidate_group_membership_claims.upstream_group_id = ? AND gateway_candidate_group_membership_claims.presence_generation = ? AND gateway_candidate_group_membership_claims.active = ?", upstreamGroupID, presenceGeneration, true).
		Pluck("gateway_candidate_group_membership_claims.candidate_group_id", &ids).Error; err != nil {
		return nil, err
	}
	result := make(map[uint]struct{}, len(ids))
	for _, id := range ids {
		result[id] = struct{}{}
	}
	return result, nil
}

func revokeCurrentGenerationClaimsOutsideTargetsTx(tx *gorm.DB, group *GatewayUpstreamGroup, targets map[uint]struct{}, at time.Time) error {
	if group == nil || group.ID == 0 || group.PresenceGeneration == 0 {
		return ErrUpstreamGroupGenerationChanged
	}
	var claims []GatewayCandidateGroupMembershipClaim
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("upstream_group_id = ? AND presence_generation = ? AND active = ?", group.ID, group.PresenceGeneration, true).
		Find(&claims).Error; err != nil {
		return err
	}
	for _, claim := range claims {
		if _, keep := targets[claim.CandidateGroupID]; keep {
			continue
		}
		if err := tx.Model(&claim).Updates(map[string]any{"active": false, "revoked_at": at}).Error; err != nil {
			return err
		}
		var remaining int64
		if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
			Where("candidate_group_id = ? AND upstream_group_id = ? AND presence_generation = ? AND active = ?", claim.CandidateGroupID, group.ID, group.PresenceGeneration, true).
			Count(&remaining).Error; err != nil {
			return err
		}
		if remaining == 0 {
			if err := tx.Where("candidate_group_id = ? AND upstream_group_id = ?", claim.CandidateGroupID, group.ID).Delete(&GatewayCandidateGroupMember{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&GatewayCandidateGroup{}).Where("id = ?", claim.CandidateGroupID).
			UpdateColumn("membership_version", gorm.Expr("membership_version + 1")).Error; err != nil {
			return err
		}
	}
	return nil
}

func createManualOverrideAuditTx(tx *gorm.DB, group *GatewayUpstreamGroup, override *GatewayUpstreamGroupAssignmentOverride, actor string, at time.Time) error {
	state, err := automaticAssignmentStateTx(tx)
	if err != nil {
		return err
	}
	targets := []uint{}
	overrideVersion := uint64(0)
	var sourceRef *uint64
	action := "manual_override_cleared"
	if override != nil {
		overrideVersion = override.OverrideVersion
		value := uint64(override.ID)
		sourceRef = &value
		action = "manual_override_updated"
		var rows []GatewayUpstreamGroupAssignmentOverrideTarget
		if err := tx.Where("override_id = ? AND candidate_group_id IS NOT NULL", override.ID).Find(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			if row.CandidateGroupID != nil {
				targets = append(targets, *row.CandidateGroupID)
			}
		}
	}
	rawName := group.CanonicalGroupName
	if len(group.RawGroupNames) > 0 {
		rawName = group.RawGroupNames[0]
	}
	audit := GatewayUpstreamGroupAssignmentAudit{
		UpstreamGroupID: group.ID, RawGroupName: rawName, CanonicalGroupName: group.CanonicalGroupName,
		PresenceGeneration: group.PresenceGeneration, ClassificationVersion: group.ClassificationVersion,
		FactsVersion: group.FactsVersion, RulesetVersion: state.RulesetVersion, OverrideVersion: overrideVersion,
		Action: action, TriggerSource: "manual_assign", SourceRef: sourceRef, MatchedRuleIDs: []uint{},
		TargetCandidateGroupIDs: targets, FactsSnapshot: map[string]any{"facts_digest": group.SourceRevision},
		Explanation: map[string]any{"reason": "candidate_group_saved_by_administrator"}, Actor: actor, CreatedAt: at,
	}
	return tx.Create(&audit).Error
}

func (g *Gateway) IgnoreUpstreamGroupChange(id uint, reason, actor string, at time.Time) error {
	return g.IgnoreUpstreamGroupChangeWithContext(context.Background(), id, reason, actor, at)
}

func (g *Gateway) IgnoreUpstreamGroupChangeWithContext(ctx context.Context, id uint, reason, actor string, at time.Time) error {
	if at.IsZero() {
		at = time.Now()
	}
	if strings.TrimSpace(actor) == "" {
		actor = "administrator"
	}
	db := g.db
	if ctx != nil {
		db = db.WithContext(ctx)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := lockAutomaticAssignmentMutationTx(tx); err != nil {
			return err
		}
		var change GatewayUpstreamGroupChange
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND kind = ? AND status = ?", id, "added", "pending").First(&change).Error; err != nil {
			return err
		}
		if change.UpstreamGroupID == nil || change.PresenceGeneration == nil {
			return ErrUpstreamGroupChangeSuperseded
		}
		var group GatewayUpstreamGroup
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&group, *change.UpstreamGroupID).Error; err != nil {
			return err
		}
		if !group.Present || group.PresenceGeneration != *change.PresenceGeneration {
			return ErrUpstreamGroupChangeSuperseded
		}
		override, err := saveAssignmentOverrideTx(tx, group.ID, group.PresenceGeneration, "ignore", nil, reason, actor, at)
		if err != nil {
			return err
		}
		if err := revokeMembershipClaimsTx(tx, "override", uint64(override.ID), at); err != nil {
			return err
		}
		state, err := automaticAssignmentStateTx(tx)
		if err != nil {
			return err
		}
		preview := &GatewayAutomaticAssignmentPreview{
			ChangeID: change.ID, UpstreamGroupID: group.ID, ChannelID: group.ChannelID, GroupName: change.GroupName,
			PresenceGeneration: group.PresenceGeneration, ModelFamilyCodes: group.ModelFamilyCodes, ModelIDs: group.ModelIDs,
			GatewayApps: group.GatewayApps, ChannelType: string(group.ChannelType), ServiceTier: group.ServiceTier,
			ServiceTierSource: group.ServiceTierSource, State: "ready", ReasonCode: "manual_ignore_override",
			MissingFacts: group.IncompleteReasons, SuggestedCandidateGroupIDs: []uint{}, MatchedRuleIDs: []uint{},
			RuleMatches: []GatewayAutomaticRuleMatch{}, EvaluatedAt: at,
		}
		if _, err := createAssignmentAuditTx(tx, &change, &group, state, override.OverrideVersion, "manual_ignore", "manual_ignore", preview, actor); err != nil {
			return err
		}
		return tx.Model(&change).Updates(map[string]any{
			"status": "ignored", "assignment_state": "ignored", "assignment_reason_code": "manual_ignore_override",
			"evaluated_at": at, "resolved_at": at,
		}).Error
	})
}

func overrideVersion(override *GatewayUpstreamGroupAssignmentOverride, err error) uint64 {
	if err == nil && override != nil {
		return override.OverrideVersion
	}
	return 0
}

func finalizeAutomaticChangeTx(tx *gorm.DB, change *GatewayUpstreamGroupChange, group *GatewayUpstreamGroup, state *GatewayCandidateGroupRulesetState, override *GatewayUpstreamGroupAssignmentOverride, preview *GatewayAutomaticAssignmentPreview, status, action, actor string) error {
	if _, err := createAssignmentAuditTx(tx, change, group, state, override.OverrideVersion, action, GatewayAssignmentTriggerOverrideReapply, preview, actor); err != nil {
		return err
	}
	return tx.Model(change).Updates(map[string]any{
		"status": status, "assignment_state": status, "assignment_reason_code": preview.ReasonCode,
		"resolved_at": time.Now(), "evaluated_at": preview.EvaluatedAt, "evaluation_token": preview.EvaluationToken,
	}).Error
}

func createAssignmentAuditTx(tx *gorm.DB, change *GatewayUpstreamGroupChange, group *GatewayUpstreamGroup, state *GatewayCandidateGroupRulesetState, overrideVersion uint64, action, trigger string, preview *GatewayAutomaticAssignmentPreview, actor string) (*GatewayUpstreamGroupAssignmentAudit, error) {
	audit := &GatewayUpstreamGroupAssignmentAudit{
		ChangeID: &change.ID, UpstreamGroupID: group.ID, RawGroupName: change.GroupName,
		CanonicalGroupName: group.CanonicalGroupName, PresenceGeneration: group.PresenceGeneration,
		ClassificationVersion: group.ClassificationVersion, FactsVersion: group.FactsVersion,
		RulesetVersion: state.RulesetVersion, OverrideVersion: overrideVersion,
		Action: action, TriggerSource: trigger, MatchedRuleIDs: preview.MatchedRuleIDs,
		TargetCandidateGroupIDs: preview.SuggestedCandidateGroupIDs,
		FactsSnapshot:           map[string]any{"model_families": group.ModelFamilyCodes, "model_ids": group.ModelIDs, "gateway_apps": group.GatewayApps, "channel_type": group.ChannelType, "service_tier": group.ServiceTier, "service_tier_source": group.ServiceTierSource},
		Explanation:             map[string]any{"state": preview.State, "reason_code": preview.ReasonCode, "missing_facts": preview.MissingFacts, "rule_matches": preview.RuleMatches},
		Actor:                   actor, CreatedAt: time.Now(),
	}
	return audit, tx.Create(audit).Error
}

func ensureMembershipClaimTx(tx *gorm.DB, candidateGroupID uint, group *GatewayUpstreamGroup, rawGroupName, sourceKind string, sourceID uint64, at time.Time) error {
	claim := GatewayCandidateGroupMembershipClaim{
		CandidateGroupID: candidateGroupID, UpstreamGroupID: group.ID, PresenceGeneration: group.PresenceGeneration,
		SourceKind: sourceKind, SourceID: sourceID, Active: true, CreatedAt: at,
	}
	created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&claim)
	if created.Error != nil {
		return created.Error
	}
	if created.RowsAffected == 0 {
		if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
			Where("candidate_group_id = ? AND upstream_group_id = ? AND presence_generation = ? AND source_kind = ? AND source_id = ?", candidateGroupID, group.ID, group.PresenceGeneration, sourceKind, sourceID).
			Updates(map[string]any{"active": true, "revoked_at": nil}).Error; err != nil {
			return err
		}
	}
	member := GatewayCandidateGroupMember{CandidateGroupID: candidateGroupID, UpstreamGroupID: group.ID, ChannelID: group.ChannelID, GroupName: strings.TrimSpace(rawGroupName)}
	memberCreated := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&member)
	if memberCreated.Error != nil {
		return memberCreated.Error
	}
	if created.RowsAffected > 0 || memberCreated.RowsAffected > 0 {
		return tx.Model(&GatewayCandidateGroup{}).Where("id = ?", candidateGroupID).
			UpdateColumn("membership_version", gorm.Expr("membership_version + 1")).Error
	}
	return nil
}

func revokeMembershipClaimsTx(tx *gorm.DB, sourceKind string, sourceID uint64, at time.Time) error {
	claims := []GatewayCandidateGroupMembershipClaim{}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("source_kind = ? AND source_id = ? AND active = ?", sourceKind, sourceID, true).Find(&claims).Error; err != nil {
		return err
	}
	for _, claim := range claims {
		if err := tx.Model(&claim).Updates(map[string]any{"active": false, "revoked_at": at}).Error; err != nil {
			return err
		}
		var remaining int64
		if err := tx.Model(&GatewayCandidateGroupMembershipClaim{}).
			Where("candidate_group_id = ? AND upstream_group_id = ? AND presence_generation = ? AND active = ?", claim.CandidateGroupID, claim.UpstreamGroupID, claim.PresenceGeneration, true).
			Count(&remaining).Error; err != nil {
			return err
		}
		if remaining == 0 {
			if err := tx.Where("candidate_group_id = ? AND upstream_group_id = ?", claim.CandidateGroupID, claim.UpstreamGroupID).Delete(&GatewayCandidateGroupMember{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&GatewayCandidateGroup{}).Where("id = ?", claim.CandidateGroupID).
			UpdateColumn("membership_version", gorm.Expr("membership_version + 1")).Error; err != nil {
			return err
		}
	}
	return nil
}

func (g *Gateway) ReevaluatePendingAutomaticAssignments(execute bool) (int, map[uint]string) {
	previews, err := g.PreviewAutomaticAssignments(nil, true)
	if err != nil {
		return 0, map[uint]string{0: err.Error()}
	}
	return g.executeAutomaticPreviews(previews, execute, GatewayAssignmentTriggerRuleRecheck)
}

func (g *Gateway) ReevaluateUpstreamGroupAutomaticAssignment(channelID uint, groupName string, execute bool) (int, map[uint]string) {
	canonical, err := canonicalUpstreamGroupName(g.db, groupName)
	if err != nil {
		return 0, map[uint]string{0: err.Error()}
	}
	var identity GatewayUpstreamGroup
	if err := g.db.Where("channel_id = ? AND canonical_group_name = ?", channelID, canonical).First(&identity).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, map[uint]string{}
		}
		return 0, map[uint]string{0: err.Error()}
	}
	var change GatewayUpstreamGroupChange
	if err := g.db.Where("upstream_group_id = ? AND presence_generation = ? AND kind = ? AND status = ?", identity.ID, identity.PresenceGeneration, "added", "pending").Order("id DESC").First(&change).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, map[uint]string{}
		}
		return 0, map[uint]string{0: err.Error()}
	}
	previews, err := g.PreviewAutomaticAssignments([]uint{change.ID}, true)
	if err != nil {
		return 0, map[uint]string{change.ID: err.Error()}
	}
	return g.executeAutomaticPreviews(previews, execute, GatewayAssignmentTriggerSync)
}

func (g *Gateway) executeAutomaticPreviews(previews []GatewayAutomaticAssignmentPreview, execute bool, trigger string) (int, map[uint]string) {
	if !execute {
		return 0, map[uint]string{}
	}
	state, err := g.automaticAssignmentState(g.db)
	if err != nil {
		return 0, map[uint]string{0: err.Error()}
	}
	assigned := 0
	failures := make(map[uint]string)
	for _, original := range previews {
		refreshed, err := g.PreviewAutomaticAssignments([]uint{original.ChangeID}, true)
		if err != nil {
			failures[original.ChangeID] = err.Error()
			continue
		}
		if len(refreshed) != 1 {
			failures[original.ChangeID] = "automatic assignment preview is no longer available"
			continue
		}
		preview := refreshed[0]
		persistentOverride := preview.ReasonCode == "manual_ignore_override" ||
			preview.ReasonCode == "manual_assign_override" ||
			preview.ReasonCode == "manual_override_incomplete_facts"
		allowExecute := persistentOverride || (state.BackfillComplete && state.AutoExecuteEnabled)
		if !allowExecute || preview.State != "ready" {
			continue
		}
		if _, err := g.ExecuteAutomaticAssignment(preview.ChangeID, preview.EvaluationToken, "system", trigger); err != nil {
			failures[preview.ChangeID] = err.Error()
			continue
		}
		assigned++
	}
	return assigned, failures
}

func containsString(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func containsAllStrings(have, required []string) bool {
	for _, item := range required {
		if !containsString(have, item) {
			return false
		}
	}
	return true
}

func sameStringSet(left, right []string) bool {
	return len(uniqueSortedStrings(left)) == len(uniqueSortedStrings(right)) && containsAllStrings(left, right)
}
