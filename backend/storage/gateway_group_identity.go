package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Harvey-Mei/BridgeOps/backend/modelcatalog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrUpstreamGroupGenerationChanged = errors.New("upstream group presence generation changed")

type UpstreamGroupGeneration struct {
	ID                 uint
	ChannelID          uint
	CanonicalGroupName string
	PresenceGeneration uint64
	DiscoveredAt       time.Time
}

// GatewayRoutingHealthFence identifies the only mutable routing-health domain
// a data-plane observation is allowed to change. Group generations alone are
// insufficient because a channel ID can be rebound to a different account.
type GatewayRoutingHealthFence struct {
	ChannelID               uint
	ChannelIdentityRevision uint64
	UpstreamGroupID         uint
	PresenceGeneration      uint64
	CanonicalGroupName      string
}

func (f GatewayRoutingHealthFence) Valid() bool {
	return f.ChannelID > 0 && f.ChannelIdentityRevision > 0 && f.UpstreamGroupID > 0 &&
		f.PresenceGeneration > 0 && strings.TrimSpace(f.CanonicalGroupName) != ""
}

func NewGatewayRoutingHealthFence(generation UpstreamGroupGeneration, channelIdentityRevision uint64) GatewayRoutingHealthFence {
	return GatewayRoutingHealthFence{
		ChannelID: generation.ChannelID, ChannelIdentityRevision: channelIdentityRevision,
		UpstreamGroupID: generation.ID, PresenceGeneration: generation.PresenceGeneration,
		CanonicalGroupName: strings.TrimSpace(generation.CanonicalGroupName),
	}
}

// MatchesUpstreamKey reports whether a persisted mutable health summary belongs
// to this exact account and present group generation. A legacy zero tuple is
// deliberately not current health.
func (f GatewayRoutingHealthFence) MatchesUpstreamKey(key *GatewayUpstreamKey) bool {
	return key != nil && f.Valid() && key.ChannelID == f.ChannelID &&
		key.RoutingHealthUpstreamGroupID == f.UpstreamGroupID &&
		key.RoutingHealthPresenceGeneration == f.PresenceGeneration &&
		key.RoutingHealthChannelIdentityRevision == f.ChannelIdentityRevision
}

func (f GatewayRoutingHealthFence) UpstreamGeneration() UpstreamGroupGeneration {
	return UpstreamGroupGeneration{
		ID: f.UpstreamGroupID, ChannelID: f.ChannelID, CanonicalGroupName: f.CanonicalGroupName,
		PresenceGeneration: f.PresenceGeneration,
	}
}

func (g UpstreamGroupGeneration) Valid() bool {
	return g.ID > 0 && g.ChannelID > 0 && strings.TrimSpace(g.CanonicalGroupName) != "" && g.PresenceGeneration > 0
}

func (g *Gateway) CaptureUpstreamGroupGeneration(channelID uint, groupName string) (UpstreamGroupGeneration, error) {
	canonical, err := canonicalUpstreamGroupName(g.db, groupName)
	if err != nil {
		return UpstreamGroupGeneration{}, err
	}
	var group GatewayUpstreamGroup
	err = g.db.Select("id", "channel_id", "canonical_group_name", "presence_generation", "discovered_at").
		Where("channel_id = ? AND canonical_group_name = ? AND present = ?", channelID, canonical, true).
		First(&group).Error
	if err != nil {
		return UpstreamGroupGeneration{}, err
	}
	return upstreamGroupGeneration(group), nil
}

// CanonicalUpstreamGroupName applies the same database-backed normalization
// used by generation fences. Service-layer recovery flows use it when the
// group is already absent and therefore cannot capture a present generation.
func (g *Gateway) CanonicalUpstreamGroupName(groupName string) (string, error) {
	if g == nil || g.db == nil {
		return "", errors.New("gateway store is required")
	}
	return canonicalUpstreamGroupName(g.db, groupName)
}

func (g *Gateway) ListUpstreamGroupGenerations(channelIDs []uint) ([]UpstreamGroupGeneration, error) {
	if len(channelIDs) == 0 {
		return []UpstreamGroupGeneration{}, nil
	}
	var groups []GatewayUpstreamGroup
	if err := g.db.Select("id", "channel_id", "canonical_group_name", "presence_generation", "discovered_at").
		Where("channel_id IN ? AND present = ?", channelIDs, true).
		Find(&groups).Error; err != nil {
		return nil, err
	}
	out := make([]UpstreamGroupGeneration, 0, len(groups))
	for _, group := range groups {
		out = append(out, upstreamGroupGeneration(group))
	}
	return out, nil
}

func upstreamGroupGeneration(group GatewayUpstreamGroup) UpstreamGroupGeneration {
	return UpstreamGroupGeneration{
		ID: group.ID, ChannelID: group.ChannelID, CanonicalGroupName: group.CanonicalGroupName,
		PresenceGeneration: group.PresenceGeneration, DiscoveredAt: group.DiscoveredAt,
	}
}

func requireUpstreamGroupGenerationTx(tx *gorm.DB, expected UpstreamGroupGeneration) error {
	if !expected.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	var current GatewayUpstreamGroup
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "channel_id", "canonical_group_name", "presence_generation", "present").
		Where("id = ?", expected.ID).First(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrUpstreamGroupGenerationChanged
	}
	if err != nil {
		return err
	}
	if !current.Present || current.ChannelID != expected.ChannelID ||
		current.CanonicalGroupName != expected.CanonicalGroupName ||
		current.PresenceGeneration != expected.PresenceGeneration {
		return ErrUpstreamGroupGenerationChanged
	}
	return nil
}

func validateUpstreamGroupName(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("upstream group name is required")
	}
	if !utf8.ValidString(raw) {
		return "", errors.New("upstream group name must be valid UTF-8")
	}
	if utf8.RuneCountInString(raw) > 128 {
		return "", errors.New("upstream group name exceeds 128 characters")
	}
	return raw, nil
}

// canonicalUpstreamGroupName deliberately delegates case normalization to
// PostgreSQL. Migration 000013 built identities with lower(btrim(...)); using
// the same database operation at runtime keeps non-ASCII names independent of
// Go's Unicode tables and the database collation cannot disagree with itself.
func canonicalUpstreamGroupName(db *gorm.DB, raw string) (string, error) {
	raw, err := validateUpstreamGroupName(raw)
	if err != nil {
		return "", err
	}
	if db == nil {
		return "", errors.New("database is required to canonicalize upstream group name")
	}
	var canonical string
	if err := db.Raw("SELECT lower(btrim(?))", raw).Scan(&canonical).Error; err != nil {
		return "", err
	}
	if canonical == "" {
		return "", errors.New("upstream group name is required")
	}
	return canonical, nil
}

func lockAutomaticAssignmentMutationTx(tx *gorm.DB) error {
	return tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", "bridgeops:automatic-group-assignment").Error
}

// lockUpstreamGroupParentChannelTx must be acquired before the automatic
// assignment and canonical-group locks whenever a transaction may create the
// first GatewayUpstreamGroup row. Channel deletion uses the same parent-first
// order before enumerating canonical presence domains.
func lockUpstreamGroupParentChannelTx(tx *gorm.DB, channelID uint) error {
	if tx == nil || channelID == 0 {
		return errors.New("channel id is required")
	}
	_, err := requireOrdinaryChannelWriteTx(tx, channelID)
	return err
}

func updateUpstreamGroupFactsTx(tx *gorm.DB, channelID uint, rawName string, modelIDs []string, syncedAt time.Time, syncErr string, fences ...GatewayModelFactFence) (*GatewayUpstreamGroup, error) {
	return rebuildUpstreamGroupFactsTx(tx, channelID, rawName, modelIDs, syncedAt, syncedAt, syncErr, fences...)
}

func rebuildUpstreamGroupFactsTx(tx *gorm.DB, channelID uint, rawName string, modelIDs []string, modelsSyncedAt time.Time, factsBuiltAt time.Time, syncErr string, fences ...GatewayModelFactFence) (*GatewayUpstreamGroup, error) {
	group, _, err := ensureUpstreamGroupIdentityTx(tx, channelID, rawName, true, factsBuiltAt)
	if err != nil {
		return nil, err
	}
	var modelFence GatewayModelFactFence
	switch len(fences) {
	case 0:
		modelFence, err = currentGatewayModelFactFenceTx(tx, channelID, group.CanonicalGroupName)
	case 1:
		modelFence = fences[0]
		if err = requireGatewayModelFactFenceTx(tx, modelFence); err == nil &&
			(modelFence.ChannelID != group.ChannelID || modelFence.UpstreamGroupID != group.ID ||
				modelFence.PresenceGeneration != group.PresenceGeneration || modelFence.CanonicalGroupName != group.CanonicalGroupName) {
			err = ErrUpstreamGroupGenerationChanged
		}
	default:
		err = ErrUpstreamGroupGenerationChanged
	}
	if err != nil {
		return nil, err
	}
	if modelsSyncedAt.Before(group.DiscoveredAt) {
		return group, nil
	}
	if group.ModelsSyncedAt != nil && modelsSyncedAt.Before(*group.ModelsSyncedAt) {
		return group, nil
	}
	modelIDs = uniqueSortedStrings(modelIDs)
	identityConflict, err := upstreamGroupIdentityFactsConflictTx(tx, channelID, group.CanonicalGroupName, modelFence)
	if err != nil {
		return nil, err
	}
	group.IdentityConflict = identityConflict
	families := make([]string, 0)
	unclassified := make([]string, 0)
	ambiguous := make([]string, 0)
	for _, modelID := range modelIDs {
		matches := modelcatalog.FamiliesFromModel(modelID)
		switch len(matches) {
		case 0:
			unclassified = append(unclassified, modelID)
		case 1:
			families = append(families, matches[0])
		default:
			ambiguous = append(ambiguous, modelID)
		}
	}
	families = modelcatalog.SortFamilies(families)
	apps := modelcatalog.CompatibleApps(families)
	appSources := make(map[string]string, len(apps))
	for _, app := range apps {
		appSources[app] = "catalog_compatible"
	}
	group.ProbedGatewayApps = normalizeCodes(group.ProbedGatewayApps)
	for _, app := range group.ProbedGatewayApps {
		if containsString(apps, app) {
			appSources[app] = "probed"
		}
	}

	capabilities := []GatewayModelCapability{}
	if err := tx.Where(
		"channel_id = ? AND lower(btrim(group_name)) = ? AND upstream_group_id = ? AND presence_generation = ? AND channel_identity_revision = ?",
		channelID, group.CanonicalGroupName, modelFence.UpstreamGroupID, modelFence.PresenceGeneration, modelFence.ChannelIdentityRevision,
	).
		Order("model_name ASC").Find(&capabilities).Error; err != nil {
		return nil, err
	}
	capabilityProfile := make(map[string]map[string]any)
	for _, capability := range capabilities {
		if !capability.Supported || strings.TrimSpace(capability.LastError) != "" {
			continue
		}
		if capability.LastCheckedAt.Before(group.DiscoveredAt) {
			continue
		}
		capabilityProfile[capability.ModelName] = map[string]any{
			"supported":                 true,
			"context_window_tokens":     capability.ContextWindowTokens,
			"safe_input_tokens":         capability.SafeInputTokens,
			"context_safety_tokens":     capability.ContextSafetyTokens,
			"last_checked_at":           capability.LastCheckedAt.UTC().Format(time.RFC3339Nano),
			"context_limit_observed_at": capability.ContextLimitObserved,
			"evidence_source":           "probed",
			"presence_generation":       group.PresenceGeneration,
		}
	}

	incomplete := make([]string, 0)
	if strings.TrimSpace(syncErr) != "" {
		incomplete = append(incomplete, "models_sync_error")
	}
	if len(modelIDs) == 0 {
		incomplete = append(incomplete, "model_ids_missing")
	}
	if len(unclassified) > 0 {
		incomplete = append(incomplete, "unclassified_models")
	}
	if len(ambiguous) > 0 {
		incomplete = append(incomplete, "ambiguous_models")
	}
	if group.IdentityConflict {
		incomplete = append(incomplete, "identity_conflict")
	}
	revisionPayload := struct {
		Generation              uint64                    `json:"generation"`
		ChannelIdentityRevision uint64                    `json:"channel_identity_revision"`
		ModelIDs                []string                  `json:"model_ids"`
		Families                []string                  `json:"families"`
		Unclassified            []string                  `json:"unclassified"`
		Ambiguous               []string                  `json:"ambiguous"`
		Apps                    []string                  `json:"apps"`
		AppSources              map[string]string         `json:"app_sources"`
		ProbedApps              []string                  `json:"probed_apps"`
		Capability              map[string]map[string]any `json:"capability"`
		SyncError               string                    `json:"sync_error"`
		CatalogVersion          string                    `json:"catalog_version"`
		IdentityConflict        bool                      `json:"identity_conflict"`
	}{group.PresenceGeneration, modelFence.ChannelIdentityRevision, modelIDs, families, unclassified, ambiguous, apps, appSources, group.ProbedGatewayApps, capabilityProfile, strings.TrimSpace(syncErr), modelcatalog.Version(), identityConflict}
	rawRevision, err := json.Marshal(revisionPayload)
	if err != nil {
		return nil, err
	}
	revisionSum := sha256.Sum256(rawRevision)
	revision := hex.EncodeToString(revisionSum[:])
	if group.SourceRevision != revision {
		group.FactsVersion++
	}
	group.ModelFamilyCodes = families
	group.ModelIDs = modelIDs
	group.UnclassifiedModelIDs = unclassified
	group.AmbiguousModelIDs = ambiguous
	group.GatewayApps = apps
	group.GatewayAppSources = appSources
	group.CapabilityProfile = capabilityProfile
	group.ClassificationVersion = modelcatalog.Version()
	group.FactsComplete = len(incomplete) == 0
	group.IncompleteReasons = incomplete
	group.SourceRevision = revision
	group.ModelsSyncedAt = &modelsSyncedAt
	group.FactsBuiltAt = &factsBuiltAt
	group.ModelFactsChannelIdentityRevision = modelFence.ChannelIdentityRevision
	group.UpdatedAt = factsBuiltAt
	if err := tx.Save(group).Error; err != nil {
		return nil, err
	}
	return group, nil
}

func upstreamGroupIdentityFactsConflictTx(tx *gorm.DB, channelID uint, canonicalName string, fence GatewayModelFactFence) (bool, error) {
	var keys []GatewayUpstreamKey
	if err := tx.Select("group_name", "model_ids", "models_error").
		Where(
			"channel_id = ? AND lower(btrim(group_name)) = ? AND model_facts_upstream_group_id = ? AND model_facts_presence_generation = ? AND model_facts_channel_identity_revision = ?",
			channelID, canonicalName, fence.UpstreamGroupID, fence.PresenceGeneration, fence.ChannelIdentityRevision,
		).
		Order("group_name ASC, id ASC").Find(&keys).Error; err != nil {
		return false, err
	}
	rawNames := make(map[string]struct{})
	signatures := make(map[string]struct{})
	for _, key := range keys {
		raw := strings.TrimSpace(key.GroupName)
		if raw == "" {
			continue
		}
		rawNames[raw] = struct{}{}
		models := uniqueSortedStrings(strings.Split(key.ModelIDs, ","))
		if len(models) == 0 && strings.TrimSpace(key.ModelsError) == "" {
			continue
		}
		signatures[strings.Join(models, "\x00")+"\x01"+strings.TrimSpace(key.ModelsError)] = struct{}{}
	}
	return len(rawNames) > 1 && len(signatures) > 1, nil
}

func updateUpstreamGroupFactsIfChannelExistsTx(tx *gorm.DB, channelID uint, rawName string, modelIDs []string, syncedAt time.Time, syncErr string, fences ...GatewayModelFactFence) (*GatewayUpstreamGroup, error) {
	return rebuildUpstreamGroupFactsIfChannelExistsTx(tx, channelID, rawName, modelIDs, syncedAt, syncedAt, syncErr, fences...)
}

func rebuildUpstreamGroupFactsIfChannelExistsTx(tx *gorm.DB, channelID uint, rawName string, modelIDs []string, modelsSyncedAt time.Time, factsBuiltAt time.Time, syncErr string, fences ...GatewayModelFactFence) (*GatewayUpstreamGroup, error) {
	hasCurrentRate, err := upstreamGroupHasCurrentRateTx(tx, channelID, rawName)
	if err != nil {
		return nil, err
	}
	if !hasCurrentRate {
		return nil, nil
	}
	return rebuildUpstreamGroupFactsTx(tx, channelID, rawName, modelIDs, modelsSyncedAt, factsBuiltAt, syncErr, fences...)
}

func upstreamGroupHasCurrentRateTx(tx *gorm.DB, channelID uint, rawName string) (bool, error) {
	canonical, err := canonicalUpstreamGroupName(tx, rawName)
	if err != nil {
		return false, err
	}
	var currentRateCount int64
	if err := tx.Model(&RateSnapshot{}).
		Where("channel_id = ? AND lower(btrim(model_name)) = ?", channelID, canonical).
		Count(&currentRateCount).Error; err != nil {
		return false, err
	}
	return currentRateCount > 0, nil
}

func ensureUpstreamGroupIdentityTx(tx *gorm.DB, channelID uint, rawName string, present bool, at time.Time) (*GatewayUpstreamGroup, bool, error) {
	canonical, err := canonicalUpstreamGroupName(tx, rawName)
	if err != nil {
		return nil, false, err
	}
	if channelID == 0 {
		return nil, false, errors.New("channel id is required")
	}
	if at.IsZero() {
		at = time.Now()
	}
	rawName = strings.TrimSpace(rawName)

	var group GatewayUpstreamGroup
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("channel_id = ? AND canonical_group_name = ?", channelID, canonical).
		First(&group).Error
	if err == nil {
		group.RawGroupNames = appendUniqueSorted(group.RawGroupNames, rawName)
		if at.After(group.UpdatedAt) {
			group.UpdatedAt = at
		}
		newGeneration := false
		if present && !group.Present {
			newGeneration = true
			group.Present = true
			group.PresenceGeneration++
			group.DiscoveredAt = at
			group.RemovedAt = nil
			group.ModelFamilyCodes = []string{}
			group.ModelIDs = []string{}
			group.UnclassifiedModelIDs = []string{}
			group.AmbiguousModelIDs = []string{}
			group.GatewayApps = []string{}
			group.GatewayAppSources = map[string]string{}
			group.ProbedGatewayApps = []string{}
			group.CapabilityProfile = map[string]map[string]any{}
			group.ClassificationVersion = ""
			group.FactsComplete = false
			group.IncompleteReasons = []string{"models_not_synced_for_generation"}
			group.FactsVersion++
			group.SourceRevision = ""
			group.ModelsSyncedAt = nil
			group.FactsBuiltAt = nil
			group.ModelFactsChannelIdentityRevision = 0
			group.IdentityConflict = false
			if group.ServiceTierSource == "provider" {
				group.ServiceTier = ""
				group.ServiceTierSource = "manual"
			}
		} else if !present && group.Present {
			group.Present = false
			group.RemovedAt = &at
			group.FactsComplete = false
			group.IncompleteReasons = []string{"group_not_present"}
			group.FactsVersion++
		}
		if err := tx.Save(&group).Error; err != nil {
			return nil, false, err
		}
		return &group, newGeneration, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}

	var channel Channel
	if err := tx.Select("id", "type").First(&channel, channelID).Error; err != nil {
		return nil, false, err
	}
	group = GatewayUpstreamGroup{
		ChannelID:            channelID,
		CanonicalGroupName:   canonical,
		RawGroupNames:        []string{rawName},
		Present:              present,
		PresenceGeneration:   1,
		DiscoveredAt:         at,
		ModelFamilyCodes:     []string{},
		ModelIDs:             []string{},
		UnclassifiedModelIDs: []string{},
		AmbiguousModelIDs:    []string{},
		GatewayApps:          []string{},
		GatewayAppSources:    map[string]string{},
		ProbedGatewayApps:    []string{},
		ChannelType:          channel.Type,
		CapabilityProfile:    map[string]map[string]any{},
		IncompleteReasons:    []string{"models_not_synced_for_generation"},
		FactsVersion:         1,
	}
	if !present {
		group.RemovedAt = &at
		group.IncompleteReasons = []string{"group_not_present"}
	}
	if err := tx.Create(&group).Error; err != nil {
		return nil, false, err
	}
	if !present && group.Present {
		// GORM applies the model's default:true tag to a false boolean on create.
		// Keep a newly discovered removal identity absent within this transaction.
		if err := tx.Model(&group).UpdateColumn("present", false).Error; err != nil {
			return nil, false, err
		}
		group.Present = false
	}
	return &group, true, nil
}

func appendUniqueSorted(values []string, value string) []string {
	seen := make(map[string]struct{}, len(values)+1)
	out := make([]string, 0, len(values)+1)
	for _, item := range values {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	if _, ok := seen[value]; !ok {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
