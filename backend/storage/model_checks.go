package storage

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const modelCheckDiscoveryUsageWindow = 30 * 24 * time.Hour

type GatewayModelCheckSource struct {
	ChannelID                       uint
	UpstreamAccountIdentityRevision uint64
	ChannelName                     string
	GroupName                       string
	App                             GatewayApp
	CredentialMode                  CredentialMode
	ModelIDs                        []string
	LatencyProbeModel               string
	RecentModels                    []string
}

type GatewayModelCheckRecentModel struct {
	ChannelID uint
	GroupName string
	ModelName string
}

// ListModelCheckSources returns only groups that are in a candidate group or
// have carried real gateway traffic recently. Probe and API-test traffic are
// deliberately excluded so internal jobs cannot make themselves permanent
// detection targets.
func (g *Gateway) ListModelCheckSources(now time.Time) ([]GatewayModelCheckSource, error) {
	if g == nil || g.db == nil {
		return nil, fmt.Errorf("gateway store is unavailable")
	}
	if now.IsZero() {
		now = time.Now()
	}

	var keys []GatewayUpstreamKey
	if err := g.db.Where("enabled = ?", true).
		Order("channel_id ASC, group_name ASC").Find(&keys).Error; err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []GatewayModelCheckSource{}, nil
	}

	channelIDs := make([]uint, 0, len(keys))
	seenChannels := make(map[uint]struct{}, len(keys))
	for i := range keys {
		if _, ok := seenChannels[keys[i].ChannelID]; ok {
			continue
		}
		seenChannels[keys[i].ChannelID] = struct{}{}
		channelIDs = append(channelIDs, keys[i].ChannelID)
	}
	var channels []Channel
	if err := g.db.Where("id IN ?", channelIDs).Find(&channels).Error; err != nil {
		return nil, err
	}
	channelByID := make(map[uint]Channel, len(channels))
	for i := range channels {
		channelByID[channels[i].ID] = channels[i]
	}

	var members []GatewayCandidateGroupMember
	if err := g.db.Where("channel_id IN ?", channelIDs).Find(&members).Error; err != nil {
		return nil, err
	}
	candidatePairs := make(map[string]struct{}, len(members))
	for i := range members {
		candidatePairs[modelCheckPairKey(members[i].ChannelID, members[i].GroupName)] = struct{}{}
	}

	var recent []GatewayModelCheckRecentModel
	if err := g.db.Model(&GatewayRequestLog{}).
		Select("channel_id, group_name, model_name").
		Where("created_at >= ?", now.Add(-modelCheckDiscoveryUsageWindow)).
		Where(gatewayRequestRecordSQL("")).
		Where("status_code >= ? AND status_code < ?", 200, 400).
		Where("TRIM(COALESCE(error, '')) = ''").
		Where("COALESCE(route_reason, '') NOT IN ?", []string{"latency_probe", "api_test", "model_check"}).
		Where("TRIM(COALESCE(model_name, '')) <> ''").
		Group("channel_id, group_name, model_name").
		Scan(&recent).Error; err != nil {
		return nil, err
	}
	recentByPair := make(map[string][]string)
	for i := range recent {
		pair := modelCheckPairKey(recent[i].ChannelID, recent[i].GroupName)
		recentByPair[pair] = appendUniqueString(recentByPair[pair], recent[i].ModelName)
	}

	out := make([]GatewayModelCheckSource, 0, len(keys))
	for i := range keys {
		key := keys[i]
		channel, ok := channelByID[key.ChannelID]
		if !ok {
			continue
		}
		pair := modelCheckPairKey(key.ChannelID, key.GroupName)
		recentModels := recentByPair[pair]
		if _, candidate := candidatePairs[pair]; !candidate && len(recentModels) == 0 {
			continue
		}
		modelIDs := splitModelCheckCSV(key.ModelIDs)
		if channel.CredentialMode == CredentialModeDirect {
			if key.DirectCredentialID == nil && key.DirectCredentialRevision == 0 && strings.TrimSpace(key.DirectBaseURL) == "" {
				directModels, parseErr := ParseDirectModels(channel.DirectModels)
				if parseErr != nil {
					// A malformed legacy direct configuration must fail closed.
					continue
				}
				modelIDs = directModels
			} else if key.DirectCredentialID == nil || *key.DirectCredentialID == 0 || key.DirectCredentialRevision == 0 || strings.TrimSpace(key.DirectBaseURL) == "" || len(modelIDs) == 0 {
				continue
			}
			recentModels = filterExactModelIDs(recentModels, modelIDs)
			if _, candidate := candidatePairs[pair]; !candidate && len(recentModels) == 0 {
				continue
			}
		}
		out = append(out, GatewayModelCheckSource{
			ChannelID:                       key.ChannelID,
			UpstreamAccountIdentityRevision: channel.UpstreamAccountIdentityRevision,
			ChannelName:                     channel.Name,
			GroupName:                       strings.TrimSpace(key.GroupName),
			App:                             channel.GatewayApp,
			CredentialMode:                  channel.CredentialMode,
			ModelIDs:                        modelIDs,
			LatencyProbeModel:               strings.TrimSpace(key.LatencyProbeModel),
			RecentModels:                    recentModels,
		})
	}
	return out, nil
}

func filterExactModelIDs(models, allowed []string) []string {
	out := make([]string, 0, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		for _, selected := range allowed {
			if strings.TrimSpace(selected) == model {
				out = appendUniqueString(out, model)
				break
			}
		}
	}
	return out
}

func modelCheckPairKey(channelID uint, groupName string) string {
	return fmt.Sprintf("%d\x00%s", channelID, strings.TrimSpace(groupName))
}

func splitModelCheckCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		out = appendUniqueString(out, part)
	}
	return out
}

func appendUniqueString(items []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return items
	}
	for _, item := range items {
		if item == value {
			return items
		}
	}
	return append(items, value)
}

type ModelCheckTargetDiscovery struct {
	ChannelID                       uint
	UpstreamAccountIdentityRevision uint64
	ChannelName                     string
	GroupName                       string
	ModelName                       string
	App                             GatewayApp
}

type ModelCheckChannelSummary struct {
	ChannelID        uint                   `json:"channel_id"`
	Status           ModelCheckTargetStatus `json:"status"`
	AlertCount       int                    `json:"alert_count"`
	UnavailableCount int                    `json:"unavailable_count"`
	LastCheckedAt    *time.Time             `json:"last_checked_at,omitempty"`
	LowestScore      *float64               `json:"lowest_score,omitempty"`
}

type ModelChecks struct{ db *gorm.DB }

func NewModelChecks(db *gorm.DB) *ModelChecks { return &ModelChecks{db: db} }

func (r *ModelChecks) SyncTargets(discovered []ModelCheckTargetDiscovery, now time.Time) ([]ModelCheckTarget, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("model check store is unavailable")
	}
	if now.IsZero() {
		now = time.Now()
	}
	if err := r.db.Transaction(func(tx *gorm.DB) error {
		var existing []ModelCheckTarget
		if err := tx.Find(&existing).Error; err != nil {
			return err
		}
		byKey := make(map[string]*ModelCheckTarget, len(existing))
		for i := range existing {
			byKey[modelCheckTargetKey(existing[i].ChannelID, existing[i].GroupName, existing[i].ModelName, existing[i].App)] = &existing[i]
		}
		seen := make(map[uint]struct{}, len(discovered))
		// Deactivation is only safe for a channel identity that this exact
		// discovery pass has successfully fenced. A late discovery result for
		// account A must not make an already-discovered account B target look
		// absent merely because A's descriptor was discarded below.
		currentDiscoveryRevisions := make(map[uint]uint64, len(discovered))
		for _, item := range discovered {
			item.ChannelName = strings.TrimSpace(item.ChannelName)
			item.GroupName = strings.TrimSpace(item.GroupName)
			item.ModelName = strings.TrimSpace(item.ModelName)
			if item.ChannelID == 0 || item.UpstreamAccountIdentityRevision == 0 || item.GroupName == "" || item.ModelName == "" || item.App == "" {
				continue
			}
			// A discovery result must acquire the same channel-row fence as run
			// persistence. A source fetched for account A is harmlessly discarded
			// if an administrator has already changed this channel to account B.
			if err := requireUpstreamAccountIdentityRevisionTx(tx, item.ChannelID, item.UpstreamAccountIdentityRevision); err != nil {
				if errors.Is(err, ErrStaleChannelAccountIdentityRevision) || errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return err
			}
			currentDiscoveryRevisions[item.ChannelID] = item.UpstreamAccountIdentityRevision
			key := modelCheckTargetKey(item.ChannelID, item.GroupName, item.ModelName, item.App)
			current := byKey[key]
			if current == nil {
				created := ModelCheckTarget{
					ChannelID:                       item.ChannelID,
					UpstreamAccountIdentityRevision: item.UpstreamAccountIdentityRevision,
					ChannelName:                     item.ChannelName,
					GroupName:                       item.GroupName,
					ModelName:                       item.ModelName,
					App:                             item.App,
					Enabled:                         true,
					Active:                          true,
					LastDiscoveredAt:                now,
					LastStatus:                      ModelCheckTargetPending,
				}
				if err := tx.Create(&created).Error; err != nil {
					return err
				}
				current = &created
				byKey[key] = current
			} else {
				updates := map[string]any{"active": true, "last_discovered_at": now}
				if current.ChannelName != item.ChannelName {
					updates["channel_name"] = item.ChannelName
				}
				if current.UpstreamAccountIdentityRevision != item.UpstreamAccountIdentityRevision {
					// Preserve the operator's enabled preference, but a target's
					// health baseline and alerts are facts about the old account.
					// Reset them before making the same channel row represent B.
					updates["upstream_account_identity_revision"] = item.UpstreamAccountIdentityRevision
					updates["last_checked_at"] = nil
					updates["last_status"] = ModelCheckTargetPending
					updates["last_score"] = nil
					updates["baseline_score"] = nil
					updates["baseline_samples"] = 0
					updates["recent_anomalies"] = 0
					updates["consecutive_errors"] = 0
					updates["alert_active"] = false
					updates["alert_reason"] = ""
					updates["alerted_at"] = nil
					updates["recovered_at"] = nil
					updates["last_actual_model"] = ""
					updates["last_error"] = ""
				}
				if err := tx.Model(&ModelCheckTarget{}).Where("id = ?", current.ID).Updates(updates).Error; err != nil {
					return err
				}
			}
			seen[current.ID] = struct{}{}
		}
		staleIDsByRevision := make(map[uint64][]uint)
		for i := range existing {
			if !existing[i].Active || existing[i].UpstreamAccountIdentityRevision == 0 {
				continue
			}
			if _, ok := seen[existing[i].ID]; !ok {
				currentRevision, current := currentDiscoveryRevisions[existing[i].ChannelID]
				// The descriptor-only discovery API cannot attest a channel that
				// produced no targets. Without a successfully current identity,
				// leaving this target unchanged is safer than letting a stale empty
				// or account-A result deactivate account B's target.
				if !current || currentRevision != existing[i].UpstreamAccountIdentityRevision {
					continue
				}
				staleIDsByRevision[existing[i].UpstreamAccountIdentityRevision] = append(staleIDsByRevision[existing[i].UpstreamAccountIdentityRevision], existing[i].ID)
			}
		}
		for revision, staleIDs := range staleIDsByRevision {
			// Keep the revision predicate in the write as a final CAS fence. The
			// channel row is locked above, but this also makes a target-row race
			// fail harmlessly instead of deactivating a newer identity.
			if err := tx.Model(&ModelCheckTarget{}).
				Where("id IN ? AND upstream_account_identity_revision = ?", staleIDs, revision).
				Update("active", false).Error; err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return r.ListTargets(true)
}

func modelCheckTargetKey(channelID uint, groupName, modelName string, app GatewayApp) string {
	return fmt.Sprintf("%d\x00%s\x00%s\x00%s", channelID, strings.TrimSpace(groupName), strings.TrimSpace(modelName), app)
}

func (r *ModelChecks) ListTargets(activeOnly bool) ([]ModelCheckTarget, error) {
	var list []ModelCheckTarget
	query := currentModelCheckTargetsQuery(r.db)
	if activeOnly {
		query = query.Where("model_check_targets.active = ?", true)
	}
	if err := query.Order("model_check_targets.alert_active DESC, model_check_targets.last_status DESC, model_check_targets.channel_name ASC, model_check_targets.group_name ASC, model_check_targets.model_name ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *ModelChecks) FindTarget(id uint) (*ModelCheckTarget, error) {
	var target ModelCheckTarget
	if err := currentModelCheckTargetsQuery(r.db).Where("model_check_targets.id = ?", id).First(&target).Error; err != nil {
		return nil, err
	}
	return &target, nil
}

func (r *ModelChecks) EnsureManualTarget(item ModelCheckTargetDiscovery, now time.Time) (*ModelCheckTarget, error) {
	var target ModelCheckTarget
	err := r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, item.ChannelID, item.UpstreamAccountIdentityRevision); err != nil {
			return err
		}
		if item.GroupName == "" || item.ModelName == "" || item.App == "" {
			return errors.New("model check target is incomplete")
		}
		identity := "channel_id = ? AND group_name = ? AND model_name = ? AND app = ?"
		args := []any{item.ChannelID, item.GroupName, item.ModelName, item.App}
		// Explicit false in a map avoids GORM's default:true struct handling.
		if err := tx.Model(&ModelCheckTarget{}).Clauses(clause.OnConflict{DoNothing: true}).Create(map[string]any{
			"channel_id": item.ChannelID, "upstream_account_identity_revision": item.UpstreamAccountIdentityRevision,
			"channel_name": item.ChannelName, "group_name": item.GroupName, "model_name": item.ModelName, "app": item.App,
			"enabled": true, "active": false, "last_discovered_at": now, "created_at": now, "updated_at": now,
		}).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(identity, args...).First(&target).Error; err != nil {
			return err
		}
		if target.UpstreamAccountIdentityRevision != item.UpstreamAccountIdentityRevision {
			target = ModelCheckTarget{ID: target.ID, ChannelID: item.ChannelID, UpstreamAccountIdentityRevision: item.UpstreamAccountIdentityRevision,
				ChannelName: item.ChannelName, GroupName: item.GroupName, ModelName: item.ModelName, App: item.App, Enabled: target.Enabled, Active: false,
				LastDiscoveredAt: now, LastStatus: ModelCheckTargetPending, CreatedAt: target.CreatedAt}
			return tx.Save(&target).Error
		}
		target.ChannelName = item.ChannelName
		return tx.Model(&target).Update("channel_name", item.ChannelName).Error
	})
	return &target, err
}

func (r *ModelChecks) SetTargetEnabled(id uint, enabled bool) error {
	if r == nil || r.db == nil || id == 0 {
		return fmt.Errorf("model check target is required")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		var target ModelCheckTarget
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&target, id).Error; err != nil {
			return err
		}
		if err := requireUpstreamAccountIdentityRevisionTx(tx, target.ChannelID, target.UpstreamAccountIdentityRevision); err != nil {
			return err
		}
		result := tx.Model(&ModelCheckTarget{}).
			Where("id = ? AND upstream_account_identity_revision = ?", target.ID, target.UpstreamAccountIdentityRevision).
			Update("enabled", enabled)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrStaleChannelAccountIdentityRevision
		}
		return nil
	})
}

func (r *ModelChecks) ListDueTargets(limit int) ([]ModelCheckTarget, error) {
	if limit <= 0 {
		limit = 1
	}
	var list []ModelCheckTarget
	if err := currentModelCheckTargetsQuery(r.db).Where("model_check_targets.enabled = ? AND model_check_targets.active = ?", true, true).
		Order("CASE WHEN model_check_targets.last_checked_at IS NULL THEN 0 ELSE 1 END ASC, model_check_targets.last_checked_at ASC, model_check_targets.id ASC").
		Limit(limit).Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *ModelChecks) ListRecentRuns(targetID uint, limit int) ([]ModelCheckRun, error) {
	if limit <= 0 {
		limit = 3
	}
	var list []ModelCheckRun
	if err := currentModelCheckRunsQuery(r.db).
		Where("model_check_runs.target_id = ? AND model_check_runs.status <> ?", targetID, ModelCheckRunDeferred).
		Order("model_check_runs.finished_at DESC, model_check_runs.id DESC").Limit(limit).Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *ModelChecks) ListRunsPage(targetID uint, page, pageSize int) ([]ModelCheckRun, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	query := currentModelCheckRunsQuery(r.db).Where("model_check_runs.target_id = ? AND model_check_runs.id IN (?)", targetID, latestModelCheckResultIDs(r.db))
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []ModelCheckRun
	if err := query.Order("model_check_runs.finished_at DESC, model_check_runs.id DESC").Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

const ModelCheckResultLimit = 12

func latestModelCheckResultIDs(db *gorm.DB) *gorm.DB {
	return db.Model(&ModelCheckRun{}).Select("id").Order("finished_at DESC, id DESC").Limit(ModelCheckResultLimit)
}

func (r *ModelChecks) LatestModelCheckResults() ([]ModelCheckRun, error) {
	var runs []ModelCheckRun
	err := currentModelCheckRunsQuery(r.db).Where("model_check_runs.id IN (?)", latestModelCheckResultIDs(r.db)).
		Order("model_check_runs.finished_at DESC, model_check_runs.id DESC").Find(&runs).Error
	return runs, err
}

// Compact older payloads while retaining the small request/status ledger used
// by the rolling budget and per-target alert recovery. Each transaction is
// bounded, including the first pass over pre-existing history.
func trimModelCheckEvidence(tx *gorm.DB) error {
	ids := tx.Model(&ModelCheckRun{}).Select("id").
		Where("id NOT IN (?) AND evidence_json <> ?", latestModelCheckResultIDs(tx), "[]").Order("id ASC").Limit(500)
	return tx.Model(&ModelCheckRun{}).Where("id IN (?)", ids).
		Updates(map[string]any{"evidence_json": "[]", "anomaly_reasons": "[]", "error": ""}).Error
}

func (r *ModelChecks) TrimResultEvidence() error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(721461903)").Error; err != nil {
			return err
		}
		return trimModelCheckEvidence(tx)
	})
}

func saveModelCheckRun(tx *gorm.DB, run *ModelCheckRun) error {
	if err := tx.Exec("SELECT pg_advisory_xact_lock(721461903)").Error; err != nil {
		return err
	}
	if err := tx.Create(run).Error; err != nil {
		return err
	}
	return trimModelCheckEvidence(tx)
}

func (r *ModelChecks) CountRequestsSince(since time.Time) (int64, error) {
	var total int64
	if err := r.db.Model(&ModelCheckRun{}).Where("finished_at >= ?", since).
		Select("COALESCE(SUM(request_count), 0)").Scan(&total).Error; err != nil {
		return 0, err
	}
	return total, nil
}

// SaveRun persists audit and request-budget evidence without changing the
// target's health state. It is used for partially executed deferred runs.
func (r *ModelChecks) SaveRun(run *ModelCheckRun) error {
	if run == nil {
		return fmt.Errorf("model check run is required")
	}
	if run.Status != ModelCheckRunDeferred || run.RequestCount < 1 {
		return fmt.Errorf("only a partially executed deferred model check run may be saved without its target")
	}
	if r == nil || r.db == nil {
		return fmt.Errorf("model check store is unavailable")
	}
	if run.ChannelID == 0 || run.TargetID == 0 || run.UpstreamAccountIdentityRevision == 0 {
		return ErrStaleChannelAccountIdentityRevision
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, run.ChannelID, run.UpstreamAccountIdentityRevision); err != nil {
			return err
		}
		var target ModelCheckTarget
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&target, run.TargetID).Error; err != nil {
			return err
		}
		if !modelCheckRunMatchesTarget(run, &target) {
			return ErrStaleChannelAccountIdentityRevision
		}
		return saveModelCheckRun(tx, run)
	})
}

func (r *ModelChecks) SaveRunAndTarget(run *ModelCheckRun, target *ModelCheckTarget) error {
	if run == nil || target == nil {
		return fmt.Errorf("model check run and target are required")
	}
	if run.UpstreamAccountIdentityRevision == 0 || target.UpstreamAccountIdentityRevision == 0 || !modelCheckRunMatchesTarget(run, target) {
		return ErrStaleChannelAccountIdentityRevision
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := requireUpstreamAccountIdentityRevisionTx(tx, target.ChannelID, target.UpstreamAccountIdentityRevision); err != nil {
			return err
		}
		var current ModelCheckTarget
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&current, target.ID).Error; err != nil {
			return err
		}
		if current.UpstreamAccountIdentityRevision != target.UpstreamAccountIdentityRevision || !modelCheckRunMatchesTarget(run, &current) {
			return ErrStaleChannelAccountIdentityRevision
		}
		if err := saveModelCheckRun(tx, run); err != nil {
			return err
		}
		// Do not overwrite operator toggles, discovery state, or identity
		// metadata with the snapshot held across a long model request.
		return tx.Model(&current).Updates(map[string]any{
			"last_checked_at": target.LastCheckedAt, "last_status": target.LastStatus, "last_score": target.LastScore,
			"baseline_score": target.BaselineScore, "baseline_samples": target.BaselineSamples,
			"recent_anomalies": target.RecentAnomalies, "consecutive_errors": target.ConsecutiveErrors,
			"alert_active": target.AlertActive, "alert_reason": target.AlertReason, "alerted_at": target.AlertedAt,
			"recovered_at": target.RecoveredAt, "last_actual_model": target.LastActualModel, "last_error": target.LastError,
		}).Error
	})
}

func (r *ModelChecks) ChannelSummaries(channelIDs []uint) (map[uint]ModelCheckChannelSummary, error) {
	out := make(map[uint]ModelCheckChannelSummary)
	if len(channelIDs) == 0 {
		return out, nil
	}
	var targets []ModelCheckTarget
	if err := currentModelCheckTargetsQuery(r.db).Where("model_check_targets.channel_id IN ? AND model_check_targets.active = ? AND model_check_targets.enabled = ?", channelIDs, true, true).Find(&targets).Error; err != nil {
		return nil, err
	}
	for i := range targets {
		target := targets[i]
		summary := out[target.ChannelID]
		summary.ChannelID = target.ChannelID
		if target.AlertActive || target.LastStatus == ModelCheckTargetSuspect {
			summary.AlertCount++
			summary.Status = ModelCheckTargetSuspect
		} else if target.LastStatus == ModelCheckTargetUnavailable {
			summary.UnavailableCount++
			if summary.Status != ModelCheckTargetSuspect {
				summary.Status = ModelCheckTargetUnavailable
			}
		} else if summary.Status == "" || (summary.Status == ModelCheckTargetPending && target.LastStatus == ModelCheckTargetHealthy) {
			summary.Status = target.LastStatus
		}
		if target.LastCheckedAt != nil && (summary.LastCheckedAt == nil || target.LastCheckedAt.After(*summary.LastCheckedAt)) {
			copy := *target.LastCheckedAt
			summary.LastCheckedAt = &copy
		}
		if target.LastScore != nil && (summary.LowestScore == nil || *target.LastScore < *summary.LowestScore) {
			copy := *target.LastScore
			summary.LowestScore = &copy
		}
		out[target.ChannelID] = summary
	}
	return out, nil
}

func (r *ModelChecks) DeleteRunsBefore(cutoff time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 500
	}
	var ids []uint64
	if err := r.db.Model(&ModelCheckRun{}).Where("finished_at < ?", cutoff).Order("id ASC").Limit(limit).Pluck("id", &ids).Error; err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	result := r.db.Where("id IN ?", ids).Delete(&ModelCheckRun{})
	return result.RowsAffected, result.Error
}

// currentModelCheckTargetsQuery hides status rows that belonged to a previous
// account identity. An account switch is allowed to finish before the next
// discovery pass, so reads must fence independently of SyncTargets.
func currentModelCheckTargetsQuery(db *gorm.DB) *gorm.DB {
	return db.Model(&ModelCheckTarget{}).
		Joins("JOIN channels ON channels.id = model_check_targets.channel_id AND channels.upstream_account_identity_revision = model_check_targets.upstream_account_identity_revision")
}

// currentModelCheckRunsQuery preserves old evidence in storage but only
// returns runs belonging to the exact current target and channel identity.
// A history line from account A must never become apparent history for B.
func currentModelCheckRunsQuery(db *gorm.DB) *gorm.DB {
	return db.Model(&ModelCheckRun{}).
		Joins("JOIN model_check_targets ON model_check_targets.id = model_check_runs.target_id AND model_check_targets.channel_id = model_check_runs.channel_id AND model_check_targets.upstream_account_identity_revision = model_check_runs.upstream_account_identity_revision").
		Joins("JOIN channels ON channels.id = model_check_targets.channel_id AND channels.upstream_account_identity_revision = model_check_targets.upstream_account_identity_revision")
}

func modelCheckRunMatchesTarget(run *ModelCheckRun, target *ModelCheckTarget) bool {
	return run != nil && target != nil &&
		run.TargetID == target.ID &&
		run.ChannelID == target.ChannelID &&
		run.UpstreamAccountIdentityRevision == target.UpstreamAccountIdentityRevision &&
		strings.TrimSpace(run.GroupName) == strings.TrimSpace(target.GroupName) &&
		strings.TrimSpace(run.ModelName) == strings.TrimSpace(target.ModelName) &&
		run.App == target.App
}

func sortModelCheckStrings(items []string) []string {
	out := append([]string(nil), items...)
	sort.Strings(out)
	return out
}
