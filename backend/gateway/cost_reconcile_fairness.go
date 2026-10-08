package gateway

import (
	"sync"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

// costReconcileIdentity is the upstream billing identity whose usage endpoint
// and pagination budget are shared. Historical rows without a persisted
// upstream key deliberately collapse to one identity per channel.
type costReconcileIdentity struct {
	channelID     uint
	upstreamKeyID int64
}

type costReconcileIdentityLimiter struct {
	mu   sync.Mutex
	busy map[costReconcileIdentity]struct{}
}

func newCostReconcileIdentityLimiter() *costReconcileIdentityLimiter {
	return &costReconcileIdentityLimiter{busy: make(map[costReconcileIdentity]struct{})}
}

func (l *costReconcileIdentityLimiter) snapshot() map[costReconcileIdentity]struct{} {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	snapshot := make(map[costReconcileIdentity]struct{}, len(l.busy))
	for identity := range l.busy {
		snapshot[identity] = struct{}{}
	}
	return snapshot
}

func (l *costReconcileIdentityLimiter) tryAcquire(identity costReconcileIdentity) bool {
	if l == nil || identity.channelID == 0 {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.busy[identity]; exists {
		return false
	}
	l.busy[identity] = struct{}{}
	return true
}

func (l *costReconcileIdentityLimiter) release(identity costReconcileIdentity) {
	if l == nil {
		return
	}
	l.mu.Lock()
	delete(l.busy, identity)
	l.mu.Unlock()
}

func requestCostReconcileIdentity(log storage.GatewayRequestLog) costReconcileIdentity {
	identity := costReconcileIdentity{channelID: log.ChannelID}
	if log.UpstreamKeyID > 0 {
		identity.upstreamKeyID = log.UpstreamKeyID
	}
	return identity
}

// selectFairRequestCostReconciles preserves the storage-provided due order but
// admits at most one request for each non-busy billing identity. A slow or
// deeply paginated key therefore cannot consume every global lookup slot.
func selectFairRequestCostReconciles(pending []storage.GatewayRequestLog, limit int, busy map[costReconcileIdentity]struct{}) []storage.GatewayRequestLog {
	if limit <= 0 || len(pending) == 0 {
		return nil
	}
	selected := make([]storage.GatewayRequestLog, 0, min(limit, len(pending)))
	admitted := make(map[costReconcileIdentity]struct{}, len(busy)+limit)
	for identity := range busy {
		admitted[identity] = struct{}{}
	}
	for _, log := range pending {
		identity := requestCostReconcileIdentity(log)
		if _, exists := admitted[identity]; exists {
			continue
		}
		admitted[identity] = struct{}{}
		selected = append(selected, log)
		if len(selected) == limit {
			break
		}
	}
	return selected
}
