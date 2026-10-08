package gateway

import (
	"context"
	"errors"
	"github.com/Harvey-Mei/BridgeOps/backend/modelcheck"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"strings"
	"time"
)

// ReadManualModelTargets reads only the selected group's authenticated model
// endpoint. Cached names are used to discover groups, never as a fallback list.
func (s *Service) ReadManualModelTargets(ctx context.Context, selected modelcheck.TargetDescriptor) ([]modelcheck.TargetDescriptor, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	options, err := s.DiscoverManualTargets(ctx, time.Now())
	if err != nil {
		return nil, err
	}
	var group *modelcheck.TargetDescriptor
	for _, option := range options {
		if option.ChannelID == selected.ChannelID && option.GroupName == selected.GroupName && option.App == selected.App && option.UpstreamAccountIdentityRevision == selected.UpstreamAccountIdentityRevision {
			copy := option
			group = &copy
			break
		}
	}
	if group == nil {
		return nil, modelcheck.ErrManualTargetUnavailable
	}
	channel, err := s.Channels.FindByID(group.ChannelID)
	if err != nil || channel.UpstreamAccountIdentityRevision != group.UpstreamAccountIdentityRevision || modelProbeChannelBlocked(*channel) {
		return nil, modelcheck.ErrManualTargetUnavailable
	}
	generation, err := s.Store.CaptureUpstreamGroupGeneration(group.ChannelID, group.GroupName)
	if err != nil || !generation.Valid() {
		return nil, modelcheck.ErrManualTargetUnavailable
	}
	cached, err := s.Store.FindUpstreamKey(group.ChannelID, group.GroupName)
	if err != nil {
		return nil, modelcheck.ErrManualTargetUnavailable
	}
	account, err := s.Channels.UpstreamAccountConcurrencyForChannel(group.ChannelID)
	if err != nil {
		return nil, err
	}
	candidate := Candidate{Channel: *channel, Rate: storage.RateSnapshot{ChannelID: group.ChannelID, ModelName: group.GroupName}, UpstreamGeneration: generation, ModelFactGeneration: generation, KeyStateGeneration: generation, UpstreamAccountConcurrency: account, App: group.App}
	var directModels []string
	if channel.CredentialMode == storage.CredentialModeDirect {
		var available bool
		directModels, available = directProjectedModels(*channel, *cached)
		if !available {
			return nil, modelcheck.ErrManualTargetUnavailable
		}
		candidate.ModelIDs = directModels
		candidate.DirectBaseURL = cached.DirectBaseURL
	}
	lease, err := s.EnsureUpstreamAPIKeyLeaseForGroup(ctx, group.ChannelID, group.GroupName, nil, cached.GroupRatio)
	if err != nil {
		return nil, errors.New("读取分组模型失败：检测凭据暂不可用")
	}
	if err := s.Channels.RequireUpstreamAccountIdentityRevision(group.ChannelID, group.UpstreamAccountIdentityRevision); err != nil {
		return nil, modelcheck.ErrManualTargetUnavailable
	}
	resolved, err := s.resolveGatewayProbeTarget(ctx, channel)
	if err != nil {
		return nil, errors.New("读取分组模型失败：网络出口暂不可用")
	}
	if candidate.DirectBaseURL != "" {
		resolved.SiteURL = strings.TrimRight(candidate.DirectBaseURL, "/")
	}
	_, models, err := s.probeModelFamilies(ctx, resolved, lease.Key, candidate, lease)
	if err != nil {
		return nil, errors.New("读取该分组的上游模型失败，请稍后刷新重试")
	}
	if err := s.Channels.RequireUpstreamAccountIdentityRevision(group.ChannelID, group.UpstreamAccountIdentityRevision); err != nil {
		return nil, modelcheck.ErrManualTargetUnavailable
	}
	// The dispatch fence protects this exact Key and group; recheck the group
	// after the response too, so a removed/recreated group cannot supply options.
	current, err := s.Store.CaptureUpstreamGroupGeneration(group.ChannelID, group.GroupName)
	if err != nil || current.ID != generation.ID || current.PresenceGeneration != generation.PresenceGeneration {
		return nil, modelcheck.ErrManualTargetUnavailable
	}
	if len(models) > 2000 {
		return nil, errors.New("上游模型列表超过 2000 项，请在渠道端收敛模型范围")
	}
	out := make([]modelcheck.TargetDescriptor, 0, len(models))
	for _, model := range models {
		if channel.CredentialMode == storage.CredentialModeDirect {
			if !directModelIDSupportsRequest(directModels, model) {
				continue
			}
		} else if !modelIDSupportsRequest(models, model, group.App) {
			continue
		}
		item := *group
		item.ModelName = model
		out = append(out, item)
	}
	return out, nil
}
