package storage

import (
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const latencyProbeStateRetention = 30 * 24 * time.Hour

// GatewayLatencyProbeState is durable scheduler state for one channel/group,
// app and upstream-group generation. It deliberately contains no key material.
type GatewayLatencyProbeState struct {
	ID                      uint64     `gorm:"primaryKey" json:"id"`
	ChannelID               uint       `gorm:"not null;uniqueIndex:idx_gateway_latency_probe_scope,priority:1;index:idx_gateway_latency_probe_due,priority:2" json:"channel_id"`
	ChannelIdentityRevision uint64     `gorm:"not null;default:0;uniqueIndex:idx_gateway_latency_probe_scope,priority:2" json:"-"`
	GroupName               string     `gorm:"size:128;not null;uniqueIndex:idx_gateway_latency_probe_scope,priority:3" json:"group_name"`
	GatewayApp              GatewayApp `gorm:"size:32;not null;uniqueIndex:idx_gateway_latency_probe_scope,priority:4;index:idx_gateway_latency_probe_due,priority:3" json:"gateway_app"`
	UpstreamGroupID         uint64     `gorm:"not null;default:0;uniqueIndex:idx_gateway_latency_probe_scope,priority:5" json:"upstream_group_id"`
	PresenceGeneration      uint64     `gorm:"not null;default:0;uniqueIndex:idx_gateway_latency_probe_scope,priority:6" json:"presence_generation"`
	NextProbeAt             *time.Time `gorm:"index:idx_gateway_latency_probe_due,priority:1" json:"next_probe_at,omitempty"`
	LastProbeAt             *time.Time `json:"last_probe_at,omitempty"`
	ConfirmationOutcome     string     `gorm:"size:16;not null;default:''" json:"confirmation_outcome,omitempty"`
	ConfirmationCount       int        `gorm:"not null;default:0" json:"confirmation_count,omitempty"`
	LastConfirmedAt         *time.Time `json:"last_confirmed_at,omitempty"`
	// LastConfirmedRecoveryAt is intentionally separate from LastConfirmedAt:
	// it keeps the newest completed three-success probe recovery even after a
	// later failed or mixed confirmation updates scheduler diagnostics.
	LastConfirmedRecoveryAt *time.Time `json:"last_confirmed_recovery_at,omitempty"`
	CreatedAt               time.Time  `json:"created_at"`
	UpdatedAt               time.Time  `json:"updated_at"`
}

func (GatewayLatencyProbeState) TableName() string { return "gateway_latency_probe_states" }

func normalizeLatencyProbeStateApp(app GatewayApp) (GatewayApp, error) {
	app = GatewayApp(strings.ToLower(strings.TrimSpace(string(app))))
	switch app {
	case GatewayAppCodex, GatewayAppClaude, GatewayAppGemini:
		return app, nil
	default:
		return "", errors.New("invalid gateway app for latency probe state")
	}
}

func (g *Gateway) normalizeLatencyProbeStateScope(state *GatewayLatencyProbeState) error {
	if g == nil || g.db == nil {
		return errors.New("gateway store is unavailable")
	}
	if state == nil || state.ChannelID == 0 {
		return errors.New("latency probe state channel is required")
	}
	canonical, err := canonicalUpstreamGroupName(g.db, state.GroupName)
	if err != nil {
		return err
	}
	app, err := normalizeLatencyProbeStateApp(state.GatewayApp)
	if err != nil {
		return err
	}
	state.GroupName = canonical
	state.GatewayApp = app
	if state.ConfirmationCount < 0 || state.ConfirmationCount > 3 {
		return errors.New("latency probe confirmation count is out of range")
	}
	if state.ConfirmationOutcome != "" && state.ConfirmationOutcome != "success" && state.ConfirmationOutcome != "failure" {
		return errors.New("latency probe confirmation outcome is invalid")
	}
	return nil
}

// FindLatencyProbeState returns the generation-scoped state, if present.
func (g *Gateway) FindLatencyProbeState(channelID uint, groupName string, app GatewayApp, generation UpstreamGroupGeneration) (*GatewayLatencyProbeState, error) {
	state := &GatewayLatencyProbeState{ChannelID: channelID, GroupName: groupName, GatewayApp: app}
	if generation.Valid() {
		state.UpstreamGroupID = uint64(generation.ID)
		state.PresenceGeneration = generation.PresenceGeneration
	}
	if err := g.normalizeLatencyProbeStateScope(state); err != nil {
		return nil, err
	}
	return g.findLatencyProbeState(state, nil)
}

// FindLatencyProbeStateForRoutingHealth returns only state from the exact
// account and present group generation captured by a routing candidate.
func (g *Gateway) FindLatencyProbeStateForRoutingHealth(fence GatewayRoutingHealthFence, app GatewayApp) (*GatewayLatencyProbeState, error) {
	if !fence.Valid() {
		return nil, ErrUpstreamGroupGenerationChanged
	}
	state := &GatewayLatencyProbeState{
		ChannelID: fence.ChannelID, ChannelIdentityRevision: fence.ChannelIdentityRevision,
		GroupName: fence.CanonicalGroupName, GatewayApp: app,
		UpstreamGroupID: uint64(fence.UpstreamGroupID), PresenceGeneration: fence.PresenceGeneration,
	}
	if err := g.normalizeLatencyProbeStateScope(state); err != nil {
		return nil, err
	}
	return g.findLatencyProbeState(state, &fence)
}

func (g *Gateway) findLatencyProbeState(state *GatewayLatencyProbeState, fence *GatewayRoutingHealthFence) (*GatewayLatencyProbeState, error) {
	var found GatewayLatencyProbeState
	query := g.db.Table("gateway_latency_probe_states AS states").Select("states.*").
		Where("states.channel_id = ? AND states.group_name = ? AND states.gateway_app = ? AND states.upstream_group_id = ? AND states.presence_generation = ?",
			state.ChannelID, state.GroupName, state.GatewayApp, state.UpstreamGroupID, state.PresenceGeneration)
	if fence != nil {
		query = query.Joins("JOIN channels ON channels.id = states.channel_id AND channels.upstream_account_identity_revision = states.channel_identity_revision").
			Where("states.channel_identity_revision = ?", fence.ChannelIdentityRevision)
	} else if state.UpstreamGroupID > 0 {
		// The compatibility reader resolves only the current account revision; it
		// never returns a retained row from a prior account identity.
		query = query.Joins("JOIN channels ON channels.id = states.channel_id AND channels.upstream_account_identity_revision = states.channel_identity_revision")
	} else {
		query = query.Where("states.channel_identity_revision = 0")
	}
	err := query.First(&found).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &found, nil
}

// ListLatencyProbeStates loads only state belonging to the currently present
// upstream generation. A legacy, unscoped row remains visible only until that
// route has a current identity, so it cannot influence a recreated group.
// Removed generations are intentionally excluded so a long-lived scheduler
// cannot grow an in-memory history map or reuse stale confirmation state.
func (g *Gateway) ListLatencyProbeStates() ([]GatewayLatencyProbeState, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	var states []GatewayLatencyProbeState
	if err := g.db.Where(`
		(
			channel_identity_revision = 0
			AND
			upstream_group_id = 0
			AND presence_generation = 0
			AND NOT EXISTS (
				SELECT 1
				  FROM public.gateway_upstream_groups AS current_groups
				 WHERE current_groups.channel_id = gateway_latency_probe_states.channel_id
				   AND current_groups.canonical_group_name = gateway_latency_probe_states.group_name
				   AND current_groups.present = TRUE
			)
		)
		OR EXISTS (
			SELECT 1
			  FROM public.gateway_upstream_groups AS groups
			  JOIN public.channels AS channels
			    ON channels.id = groups.channel_id
			   AND channels.upstream_account_identity_revision = gateway_latency_probe_states.channel_identity_revision
			 WHERE groups.id = gateway_latency_probe_states.upstream_group_id
			   AND groups.channel_id = gateway_latency_probe_states.channel_id
			   AND groups.canonical_group_name = gateway_latency_probe_states.group_name
			   AND groups.present = TRUE
			   AND groups.presence_generation = gateway_latency_probe_states.presence_generation
		)`).Order("channel_id ASC, channel_identity_revision ASC, gateway_app ASC, group_name ASC, upstream_group_id ASC, presence_generation ASC").Find(&states).Error; err != nil {
		return nil, err
	}
	return states, nil
}

// PruneLatencyProbeStates removes only old rows whose generation is no longer
// present. The NOT EXISTS guard is the same identity check used by late-write
// protection, so deleting history cannot race a replacement generation into
// accepting an old probe result. A zero retention uses the bounded default.
func (g *Gateway) PruneLatencyProbeStates(retention time.Duration) (int64, error) {
	if g == nil || g.db == nil {
		return 0, errors.New("gateway store is unavailable")
	}
	if retention <= 0 {
		retention = latencyProbeStateRetention
	}
	cutoff := time.Now().UTC().Add(-retention)
	result := g.db.Exec(`
		DELETE FROM public.gateway_latency_probe_states AS states
		 WHERE states.updated_at < ?
		   AND (
				(
					states.upstream_group_id <> 0
					AND NOT EXISTS (
						SELECT 1
						  FROM public.gateway_upstream_groups AS groups
						  JOIN public.channels AS channels
						    ON channels.id = groups.channel_id
						   AND channels.upstream_account_identity_revision = states.channel_identity_revision
						 WHERE groups.id = states.upstream_group_id
						   AND groups.channel_id = states.channel_id
						   AND groups.canonical_group_name = states.group_name
						   AND groups.present = TRUE
						   AND groups.presence_generation = states.presence_generation
					)
				)
				OR (
					states.upstream_group_id = 0
					AND states.presence_generation = 0
					AND (
						states.channel_identity_revision <> 0
						OR
						NOT EXISTS (
							SELECT 1
							  FROM public.gateway_upstream_keys AS keys
							 WHERE keys.channel_id = states.channel_id
							   AND lower(btrim(keys.group_name)) = lower(btrim(states.group_name))
						)
						OR NOT EXISTS (
							SELECT 1
							  FROM public.rate_snapshots AS rates
							 WHERE rates.channel_id = states.channel_id
							   AND lower(btrim(rates.model_name)) = lower(btrim(states.group_name))
						)
						OR EXISTS (
							SELECT 1
							  FROM public.gateway_upstream_groups AS current_groups
							 WHERE current_groups.channel_id = states.channel_id
							   AND current_groups.canonical_group_name = states.group_name
							   AND current_groups.present = TRUE
						)
					)
				)
			)`, cutoff)
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}

// NextLatencyProbeDueAt returns the earliest wake hint for an isolated route.
// Healthy historical probe state is deliberately ignored: real
// requests provide passive latency samples and must not wake active probes.
// The eligibility joins also reject removed or replaced group generations.
func (g *Gateway) NextLatencyProbeDueAt(cadence ...time.Duration) (*time.Time, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	_ = cadence // Retained for compatibility with existing scheduler callers.
	var row struct {
		NextProbeAt *time.Time `gorm:"column:next_probe_at"`
	}
	err := g.db.Raw(`
		SELECT MIN(
			CASE
				WHEN states.next_probe_at <= CURRENT_TIMESTAMP
				THEN CURRENT_TIMESTAMP
				ELSE states.next_probe_at
			END
		) AS next_probe_at
		  FROM public.gateway_latency_probe_states AS states
		  JOIN public.channels AS channels
		    ON channels.id = states.channel_id
		  JOIN public.rate_snapshots AS rates
		    ON rates.channel_id = states.channel_id
		   AND lower(btrim(rates.model_name)) = states.group_name
		  JOIN LATERAL (
			SELECT candidate_keys.*
			  FROM public.gateway_upstream_keys AS candidate_keys
			 WHERE candidate_keys.channel_id = states.channel_id
			   AND lower(btrim(candidate_keys.group_name)) = states.group_name
			 ORDER BY candidate_keys.enabled DESC,
			          candidate_keys.provisioning ASC,
			          (btrim(COALESCE(candidate_keys.api_key_cipher, '')) <> '') DESC,
			          candidate_keys.models_synced_at DESC NULLS LAST,
			          candidate_keys.id DESC
			 LIMIT 1
		  ) AS keys ON TRUE
		  JOIN public.gateway_upstream_groups AS groups
		    ON groups.id = states.upstream_group_id
		   AND groups.channel_id = states.channel_id
		   AND groups.canonical_group_name = states.group_name
		   AND groups.present = TRUE
		   AND groups.presence_generation = states.presence_generation
		  LEFT JOIN public.channel_direct_credentials AS direct_credentials
		    ON direct_credentials.id = keys.direct_credential_id
		 WHERE states.next_probe_at IS NOT NULL
		   AND keys.routing_health_upstream_group_id = states.upstream_group_id
		   AND keys.routing_health_presence_generation = states.presence_generation
		   AND keys.routing_health_channel_identity_revision = states.channel_identity_revision
		   AND (
				(
					keys.last_routing_failure_at IS NOT NULL
					AND keys.last_routing_failure_kind = 'manual_cooldown'
					AND keys.routing_cooldown_until > CURRENT_TIMESTAMP
				)
				OR keys.routing_isolated_at IS NOT NULL
		   )
		   AND channels.monitor_enabled = TRUE
		   AND COALESCE(channels.last_error, '') = ''
		   AND channels.upstream_account_identity_revision = states.channel_identity_revision
		   AND keys.enabled = TRUE
		   AND keys.provisioning = FALSE
		   AND btrim(COALESCE(keys.api_key_cipher, '')) <> ''
		   AND btrim(COALESCE(keys.model_ids, '')) <> ''
		   AND (
				(
					channels.credential_mode = 'direct'
					AND lower(btrim(channels.gateway_app)) = states.gateway_app
					AND direct_credentials.id IS NOT NULL
					AND direct_credentials.channel_id = channels.id
					AND lower(btrim(direct_credentials.group_name)) = states.group_name
					AND direct_credentials.enabled = TRUE
					AND direct_credentials.revision = keys.direct_credential_revision
					AND btrim(COALESCE(direct_credentials.api_key_cipher, '')) <> ''
					AND direct_credentials.api_key_cipher = keys.api_key_cipher
					AND rtrim(btrim(direct_credentials.base_url), '/') = rtrim(btrim(keys.direct_base_url), '/')
					AND jsonb_typeof(direct_credentials.models::jsonb) = 'array'
					AND jsonb_array_length(direct_credentials.models::jsonb) > 0
					AND direct_credentials.models::jsonb = to_jsonb(regexp_split_to_array(btrim(keys.model_ids), '[[:space:]]*,[[:space:]]*'))
				)
				OR (
					channels.credential_mode <> 'direct'
					AND keys.models_synced_at IS NOT NULL
					AND keys.models_synced_at >= groups.discovered_at
					AND btrim(COALESCE(keys.models_error, '')) = ''
					AND keys.model_facts_upstream_group_id = groups.id
					AND keys.model_facts_presence_generation = groups.presence_generation
					AND keys.model_facts_channel_identity_revision = channels.upstream_account_identity_revision
					AND btrim(COALESCE(keys.model_families, '')) <> ''
					AND (
						(states.gateway_app = 'codex' AND regexp_split_to_array(lower(keys.model_families), '[[:space:]]*,[[:space:]]*') && ARRAY['codex', 'grok', 'deepseek', 'qwen', 'mistral', 'llama', 'glm', 'minimax', 'moonshot', 'kimi', 'yi', 'ernie', 'other']::text[])
						OR (states.gateway_app = 'claude' AND regexp_split_to_array(lower(keys.model_families), '[[:space:]]*,[[:space:]]*') && ARRAY['claude']::text[])
						OR (states.gateway_app = 'gemini' AND regexp_split_to_array(lower(keys.model_families), '[[:space:]]*,[[:space:]]*') && ARRAY['gemini']::text[])
					)
				)
			)`).Scan(&row).Error
	if err != nil {
		return nil, err
	}
	return row.NextProbeAt, nil
}

// upsertLatencyProbeStateTx preserves the newest confirmed outcome when a
// caller's scheduler clock predates a completed probe. A completed probe can
// persist its recovery fact before the outer scheduler saves the next-due
// time; that later save must never regress the recovery watermark.
func upsertLatencyProbeStateTx(tx *gorm.DB, state *GatewayLatencyProbeState) error {
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "channel_id"}, {Name: "channel_identity_revision"}, {Name: "group_name"}, {Name: "gateway_app"},
			{Name: "upstream_group_id"}, {Name: "presence_generation"},
		},
		DoUpdates: clause.Assignments(map[string]any{
			"next_probe_at": gorm.Expr("EXCLUDED.next_probe_at"),
			"last_probe_at": gorm.Expr(`
				CASE
					WHEN EXCLUDED.last_probe_at IS NULL THEN gateway_latency_probe_states.last_probe_at
					WHEN gateway_latency_probe_states.last_probe_at IS NULL
						OR EXCLUDED.last_probe_at >= gateway_latency_probe_states.last_probe_at
					THEN EXCLUDED.last_probe_at
					ELSE gateway_latency_probe_states.last_probe_at
				END`),
			"confirmation_outcome": gorm.Expr(`
				CASE
					WHEN EXCLUDED.last_confirmed_at IS NULL THEN EXCLUDED.confirmation_outcome
					WHEN gateway_latency_probe_states.last_confirmed_at IS NULL
						OR EXCLUDED.last_confirmed_at >= gateway_latency_probe_states.last_confirmed_at
					THEN EXCLUDED.confirmation_outcome
					ELSE gateway_latency_probe_states.confirmation_outcome
				END`),
			"confirmation_count": gorm.Expr(`
				CASE
					WHEN EXCLUDED.last_confirmed_at IS NULL THEN EXCLUDED.confirmation_count
					WHEN gateway_latency_probe_states.last_confirmed_at IS NULL
						OR EXCLUDED.last_confirmed_at >= gateway_latency_probe_states.last_confirmed_at
					THEN EXCLUDED.confirmation_count
					ELSE gateway_latency_probe_states.confirmation_count
				END`),
			"last_confirmed_at": gorm.Expr(`
				CASE
					WHEN EXCLUDED.last_confirmed_at IS NULL THEN NULL
					WHEN gateway_latency_probe_states.last_confirmed_at IS NULL
						OR EXCLUDED.last_confirmed_at >= gateway_latency_probe_states.last_confirmed_at
					THEN EXCLUDED.last_confirmed_at
					ELSE gateway_latency_probe_states.last_confirmed_at
				END`),
			"last_confirmed_recovery_at": gorm.Expr(`
				CASE
					WHEN EXCLUDED.last_confirmed_recovery_at IS NULL THEN gateway_latency_probe_states.last_confirmed_recovery_at
					WHEN gateway_latency_probe_states.last_confirmed_recovery_at IS NULL
						OR EXCLUDED.last_confirmed_recovery_at >= gateway_latency_probe_states.last_confirmed_recovery_at
					THEN EXCLUDED.last_confirmed_recovery_at
					ELSE gateway_latency_probe_states.last_confirmed_recovery_at
				END`),
			"updated_at": gorm.Expr("EXCLUDED.updated_at"),
		}),
	}).Create(state).Error
}

// UpsertLatencyProbeState persists the next due time and the last confirmed
// three-sample outcome. Scheduler-owned timestamps are monotonic so a
// completion-time recovery fact cannot be overwritten by an earlier run-start
// timestamp from the same scheduler pass.
func (g *Gateway) UpsertLatencyProbeState(state *GatewayLatencyProbeState) error {
	return g.upsertLatencyProbeState(state, nil)
}

// UpsertLatencyProbeStateForRoutingHealth persists scheduler evidence only for
// the exact candidate identity captured before the probe started.
func (g *Gateway) UpsertLatencyProbeStateForRoutingHealth(fence GatewayRoutingHealthFence, state *GatewayLatencyProbeState) error {
	if !fence.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	if state == nil {
		return errors.New("latency probe state is required")
	}
	state.ChannelID = fence.ChannelID
	state.ChannelIdentityRevision = fence.ChannelIdentityRevision
	state.GroupName = fence.CanonicalGroupName
	state.UpstreamGroupID = uint64(fence.UpstreamGroupID)
	state.PresenceGeneration = fence.PresenceGeneration
	return g.upsertLatencyProbeState(state, &fence)
}

func (g *Gateway) upsertLatencyProbeState(state *GatewayLatencyProbeState, fence *GatewayRoutingHealthFence) error {
	if err := g.normalizeLatencyProbeStateScope(state); err != nil {
		return err
	}
	now := time.Now().UTC()
	if state.UpdatedAt.IsZero() {
		state.UpdatedAt = now
	}
	if state.UpstreamGroupID == 0 || state.PresenceGeneration == 0 {
		if state.UpstreamGroupID != 0 || state.PresenceGeneration != 0 {
			return ErrUpstreamGroupGenerationChanged
		}
	}
	return g.db.Transaction(func(tx *gorm.DB) error {
		channel, err := requireOrdinaryChannelWriteTx(tx, state.ChannelID)
		if err != nil {
			return err
		}
		if state.UpstreamGroupID == 0 {
			if state.ChannelIdentityRevision != 0 || fence != nil {
				return ErrUpstreamGroupGenerationChanged
			}
			return upsertLatencyProbeStateTx(tx, state)
		}
		generation := UpstreamGroupGeneration{
			ID: uint(state.UpstreamGroupID), ChannelID: state.ChannelID,
			CanonicalGroupName: state.GroupName, PresenceGeneration: state.PresenceGeneration,
		}
		if err := requireUpstreamGroupGenerationTx(tx, generation); err != nil {
			return err
		}
		if fence != nil {
			if channel.UpstreamAccountIdentityRevision != fence.ChannelIdentityRevision ||
				state.ChannelIdentityRevision != fence.ChannelIdentityRevision ||
				generation != fence.UpstreamGeneration() {
				return ErrStaleChannelAccountIdentityRevision
			}
		} else if state.ChannelIdentityRevision == 0 {
			// Compatibility callers are bound to the revision observed while the
			// channel row is locked. Production probe observations use the explicit
			// fence entry point above and never reconstruct this value late.
			state.ChannelIdentityRevision = channel.UpstreamAccountIdentityRevision
		} else if state.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision {
			return ErrStaleChannelAccountIdentityRevision
		}
		return upsertLatencyProbeStateTx(tx, state)
	})
}
