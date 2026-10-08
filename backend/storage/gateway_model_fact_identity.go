package storage

import (
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

// GatewayModelFactFence binds mutable model discovery and capability evidence
// to the account and present upstream-group generation that produced it.
type GatewayModelFactFence struct {
	ChannelID               uint
	ChannelIdentityRevision uint64
	UpstreamGroupID         uint
	PresenceGeneration      uint64
	CanonicalGroupName      string
}

func NewGatewayModelFactFence(generation UpstreamGroupGeneration, channelIdentityRevision uint64) GatewayModelFactFence {
	return GatewayModelFactFence{
		ChannelID:               generation.ChannelID,
		ChannelIdentityRevision: channelIdentityRevision,
		UpstreamGroupID:         generation.ID,
		PresenceGeneration:      generation.PresenceGeneration,
		CanonicalGroupName:      strings.TrimSpace(generation.CanonicalGroupName),
	}
}

func (f GatewayModelFactFence) Valid() bool {
	return f.ChannelID > 0 && f.ChannelIdentityRevision > 0 && f.UpstreamGroupID > 0 &&
		f.PresenceGeneration > 0 && strings.TrimSpace(f.CanonicalGroupName) != ""
}

func (f GatewayModelFactFence) UpstreamGeneration() UpstreamGroupGeneration {
	return UpstreamGroupGeneration{
		ID:                 f.UpstreamGroupID,
		ChannelID:          f.ChannelID,
		CanonicalGroupName: f.CanonicalGroupName,
		PresenceGeneration: f.PresenceGeneration,
	}
}

func (f GatewayModelFactFence) MatchesUpstreamKey(key *GatewayUpstreamKey) bool {
	return key != nil && f.Valid() && key.ChannelID == f.ChannelID &&
		key.ModelFactsUpstreamGroupID == f.UpstreamGroupID &&
		key.ModelFactsPresenceGeneration == f.PresenceGeneration &&
		key.ModelFactsChannelIdentityRevision == f.ChannelIdentityRevision
}

func (f GatewayModelFactFence) MatchesCapability(capability *GatewayModelCapability) bool {
	return capability != nil && f.Valid() && capability.ChannelID == f.ChannelID &&
		capability.UpstreamGroupID == f.UpstreamGroupID &&
		capability.PresenceGeneration == f.PresenceGeneration &&
		capability.ChannelIdentityRevision == f.ChannelIdentityRevision
}

func requireGatewayModelFactFenceTx(tx *gorm.DB, expected GatewayModelFactFence) error {
	if tx == nil || !expected.Valid() {
		return ErrUpstreamGroupGenerationChanged
	}
	channel, err := requireOrdinaryChannelWriteTx(tx, expected.ChannelID)
	if err != nil {
		return err
	}
	if channel.UpstreamAccountIdentityRevision != expected.ChannelIdentityRevision {
		return ErrUpstreamGroupGenerationChanged
	}
	if err := requireUpstreamGroupGenerationTx(tx, expected.UpstreamGeneration()); err != nil {
		return err
	}
	return nil
}

func currentGatewayModelFactFenceTx(tx *gorm.DB, channelID uint, groupName string) (GatewayModelFactFence, error) {
	if tx == nil || channelID == 0 {
		return GatewayModelFactFence{}, ErrUpstreamGroupGenerationChanged
	}
	channel, err := requireOrdinaryChannelWriteTx(tx, channelID)
	if err != nil {
		return GatewayModelFactFence{}, err
	}
	canonical, err := canonicalUpstreamGroupName(tx, groupName)
	if err != nil {
		return GatewayModelFactFence{}, err
	}
	var group GatewayUpstreamGroup
	err = tx.Where("channel_id = ? AND canonical_group_name = ? AND present = ?", channelID, canonical, true).
		First(&group).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return GatewayModelFactFence{}, ErrUpstreamGroupGenerationChanged
	}
	if err != nil {
		return GatewayModelFactFence{}, err
	}
	fence := NewGatewayModelFactFence(upstreamGroupGeneration(group), channel.UpstreamAccountIdentityRevision)
	if !fence.Valid() {
		return GatewayModelFactFence{}, ErrUpstreamGroupGenerationChanged
	}
	return fence, nil
}

// lockGatewayModelFactWriteTx is called after the channel and automatic-
// assignment locks. It preserves the established advisory-before-group order,
// then verifies the account revision while the parent channel remains locked.
func lockGatewayModelFactWriteTx(tx *gorm.DB, channelID uint, groupName string, at time.Time, explicit *GatewayModelFactFence, generations ...UpstreamGroupGeneration) (string, GatewayModelFactFence, error) {
	channel, err := requireOrdinaryChannelWriteTx(tx, channelID)
	if err != nil {
		return "", GatewayModelFactFence{}, err
	}
	if explicit != nil {
		if !explicit.Valid() || explicit.ChannelID != channelID ||
			channel.UpstreamAccountIdentityRevision != explicit.ChannelIdentityRevision {
			return "", GatewayModelFactFence{}, ErrUpstreamGroupGenerationChanged
		}
		canonical, generation, err := lockModelSyncUpstreamGenerationTx(tx, channelID, groupName, at, explicit.UpstreamGeneration())
		if err != nil {
			return "", GatewayModelFactFence{}, err
		}
		if generation == nil || canonical != explicit.CanonicalGroupName {
			return "", GatewayModelFactFence{}, ErrUpstreamGroupGenerationChanged
		}
		return canonical, *explicit, nil
	}
	canonical, generation, err := lockModelSyncUpstreamGenerationTx(tx, channelID, groupName, at, generations...)
	if err != nil {
		return "", GatewayModelFactFence{}, err
	}
	if generation == nil {
		return "", GatewayModelFactFence{}, ErrUpstreamGroupGenerationChanged
	}
	fence := NewGatewayModelFactFence(*generation, channel.UpstreamAccountIdentityRevision)
	if !fence.Valid() {
		return "", GatewayModelFactFence{}, ErrUpstreamGroupGenerationChanged
	}
	return canonical, fence, nil
}

func currentGatewayModelCapabilityScope(db *gorm.DB) *gorm.DB {
	return db.Where(`EXISTS (
		SELECT 1
		  FROM public.channels AS model_fact_channels
		  JOIN public.gateway_upstream_groups AS model_fact_groups
		    ON model_fact_groups.id = gateway_model_capabilities.upstream_group_id
		   AND model_fact_groups.channel_id = gateway_model_capabilities.channel_id
		   AND model_fact_groups.canonical_group_name = lower(btrim(gateway_model_capabilities.group_name))
		   AND model_fact_groups.present = TRUE
		   AND model_fact_groups.presence_generation = gateway_model_capabilities.presence_generation
		 WHERE model_fact_channels.id = gateway_model_capabilities.channel_id
		   AND model_fact_channels.upstream_account_identity_revision = gateway_model_capabilities.channel_identity_revision
	)`)
}
