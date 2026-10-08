package gateway

import (
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type routingLatencyObservation struct {
	id        uint
	latencyMs float64
	createdAt time.Time
}

type routingLatencyCache struct {
	initMu      sync.Mutex
	mu          sync.RWMutex
	initialized bool
	capacity    int
	window      time.Duration
	groups      map[candidateLatencyKey][]routingLatencyObservation
	channels    map[uint][]routingLatencyObservation
}

func (s *Service) resetRoutingLatencyCache() {
	if s == nil {
		return
	}
	cache := &s.latencyCache
	cache.initMu.Lock()
	cache.mu.Lock()
	cache.initialized = false
	cache.capacity = 0
	cache.window = 0
	cache.groups = nil
	cache.channels = nil
	cache.mu.Unlock()
	cache.initMu.Unlock()
}

func (s *Service) routingLatencySnapshot(window time.Duration, minimum int) (candidateLatencySamples, error) {
	if s == nil {
		return candidateLatencySamples{}, nil
	}
	return s.routingLatencySnapshotWithStore(s.Store, window, minimum)
}

func (s *Service) routingLatencySnapshotWithStore(store *storage.Gateway, window time.Duration, minimum int) (candidateLatencySamples, error) {
	if minimum <= 0 {
		minimum = defaultRoutingLatencySampleSize
	}
	if window <= 0 {
		window = defaultRoutingLatencyWindow
	}
	out := candidateLatencySamples{
		groups: make(map[candidateLatencyKey][]float64), channels: make(map[uint][]float64), minimum: minimum,
	}
	if s == nil || store == nil {
		return out, nil
	}
	cache := &s.latencyCache
	cache.initMu.Lock()
	cache.mu.RLock()
	initialized := cache.initialized && cache.capacity == minimum && cache.window == window
	cache.mu.RUnlock()
	if !initialized {
		limit := minimum * 500
		if limit < 5000 {
			limit = 5000
		}
		logs, err := store.ListRecentSuccessfulLatencyLogsSince(time.Now().Add(-window), limit)
		if err != nil {
			cache.initMu.Unlock()
			return out, err
		}
		groups := make(map[candidateLatencyKey][]routingLatencyObservation)
		channels := make(map[uint][]routingLatencyObservation)
		for _, log := range logs {
			if log.ChannelID == 0 || log.LatencyMs <= 0 {
				continue
			}
			observation := routingLatencyObservation{id: log.ID, latencyMs: float64(log.LatencyMs), createdAt: log.CreatedAt}
			groupKey := candidateLatencyKey{ChannelID: log.ChannelID, GroupName: normalizeLatencyGroup(log.GroupName)}
			if len(groups[groupKey]) < minimum {
				groups[groupKey] = append(groups[groupKey], observation)
			}
			if len(channels[log.ChannelID]) < minimum {
				channels[log.ChannelID] = append(channels[log.ChannelID], observation)
			}
		}
		cache.mu.Lock()
		cache.initialized = true
		cache.capacity = minimum
		cache.window = window
		cache.groups = groups
		cache.channels = channels
		cache.mu.Unlock()
	}
	cache.initMu.Unlock()

	cutoff := time.Now().Add(-window)
	cache.mu.Lock()
	for key, observations := range cache.groups {
		observations = activeLatencyObservations(observations, cutoff, minimum)
		if len(observations) == 0 {
			delete(cache.groups, key)
			continue
		}
		cache.groups[key] = observations
		values := make([]float64, len(observations))
		for index := range observations {
			values[index] = observations[index].latencyMs
		}
		out.groups[key] = values
	}
	for channelID, observations := range cache.channels {
		observations = activeLatencyObservations(observations, cutoff, minimum)
		if len(observations) == 0 {
			delete(cache.channels, channelID)
			continue
		}
		cache.channels[channelID] = observations
		values := make([]float64, len(observations))
		for index := range observations {
			values[index] = observations[index].latencyMs
		}
		out.channels[channelID] = values
	}
	cache.mu.Unlock()
	return out, nil
}

func (s *Service) recordRoutingLatency(log *storage.GatewayRequestLog) {
	if s == nil || log == nil || log.ChannelID == 0 || log.LatencyMs <= 0 || !storage.IsSuccessfulGatewayRequest(log) {
		return
	}
	cache := &s.latencyCache
	cache.initMu.Lock()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	defer cache.initMu.Unlock()
	if !cache.initialized || cache.capacity <= 0 {
		return
	}
	createdAt := log.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	if createdAt.Before(time.Now().Add(-cache.window)) {
		return
	}
	observation := routingLatencyObservation{id: log.ID, latencyMs: float64(effectiveLatencyMs(log)), createdAt: createdAt}
	groupKey := candidateLatencyKey{ChannelID: log.ChannelID, GroupName: normalizeLatencyGroup(log.GroupName)}
	cache.groups[groupKey] = prependLatencyObservation(cache.groups[groupKey], observation, cache.capacity)
	cache.channels[log.ChannelID] = prependLatencyObservation(cache.channels[log.ChannelID], observation, cache.capacity)
}

func effectiveLatencyMs(log *storage.GatewayRequestLog) int64 {
	if log.FirstByteMs > 0 {
		return log.FirstByteMs
	}
	return log.LatencyMs
}

func normalizeLatencyGroup(value string) string {
	return canonicalGroupName(value)
}

func prependLatencyObservation(values []routingLatencyObservation, observation routingLatencyObservation, capacity int) []routingLatencyObservation {
	for _, value := range values {
		if observation.id > 0 && value.id == observation.id {
			return values
		}
	}
	values = append(values, routingLatencyObservation{})
	copy(values[1:], values[:len(values)-1])
	values[0] = observation
	if len(values) > capacity {
		values = values[:capacity]
	}
	return values
}

func activeLatencyObservations(values []routingLatencyObservation, cutoff time.Time, capacity int) []routingLatencyObservation {
	out := values[:0]
	for _, value := range values {
		if value.createdAt.Before(cutoff) {
			continue
		}
		out = append(out, value)
		if len(out) >= capacity {
			break
		}
	}
	return out
}
