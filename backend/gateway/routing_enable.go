package gateway

import (
	"errors"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func (s *Service) EnableUpstreamGroupRouting(upstreamGroupID uint, presenceGeneration, channelIdentityRevision uint64, actor, idempotencyKey, reason string) (*storage.GatewayRoutingEnableResult, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	result, err := s.Store.EnableUpstreamGroupRouting(upstreamGroupID, presenceGeneration, channelIdentityRevision, actor, idempotencyKey, reason, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	s.forgetUpstreamTouch(result.ChannelID, result.CanonicalGroupName, storage.UpstreamGroupGeneration{
		ID: result.UpstreamGroupID, ChannelID: result.ChannelID,
		CanonicalGroupName: result.CanonicalGroupName, PresenceGeneration: result.PresenceGeneration,
	})
	return result, nil
}

// RecoverUpstreamGroupRoutingFromConfirmedLatencyProbeAt records only a
// completed three-success latency-probe confirmation. It intentionally does
// not call TouchUpstreamGroupAt: background probe traffic is not a real
// downstream inference success and must not advance the cross-key success
// watermark used to clear adaptive routing health.
func (s *Service) RecoverUpstreamGroupRoutingFromConfirmedLatencyProbeAt(channelID uint, groupName string, app storage.GatewayApp, at time.Time, generations ...storage.UpstreamGroupGeneration) {
	if s == nil || s.Store == nil {
		return
	}
	if err := s.Store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbe(channelID, groupName, app, at, generations...); err != nil &&
		!errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) && s.Log != nil {
		s.Log.Warn("record confirmed latency probe recovery failed", "channel_id", channelID, "group", groupName, "app", app, "err", err)
	}
}

func (s *Service) RecoverUpstreamGroupRoutingFromConfirmedLatencyProbeForRoutingHealthAt(fence storage.GatewayRoutingHealthFence, app storage.GatewayApp, at time.Time) {
	if s == nil || s.Store == nil || !fence.Valid() {
		return
	}
	if err := s.Store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(fence, app, at); err != nil &&
		!errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) &&
		!errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) && s.Log != nil {
		s.Log.Warn("record identity-fenced confirmed latency probe recovery failed", "channel_id", fence.ChannelID, "group", fence.CanonicalGroupName, "app", app, "err", err)
	}
}
