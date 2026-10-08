package storage

import (
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestNewGatewayRoutingHealthFencePreservesDatabaseCanonicalName(t *testing.T) {
	generation := UpstreamGroupGeneration{
		ID: 9, ChannelID: 4, CanonicalGroupName: "  \u0130  ", PresenceGeneration: 2,
	}
	fence := NewGatewayRoutingHealthFence(generation, 3)
	if fence.CanonicalGroupName != "\u0130" {
		t.Fatalf("canonical group name was rewritten: %q", fence.CanonicalGroupName)
	}
}

func TestRoutingHealthAndProbeRecoveryRejectPriorChannelIdentity(t *testing.T) {
	store, generation := routingEnableFixture(t, "identity-fenced-probe")
	var channel Channel
	if err := store.db.First(&channel, generation.ChannelID).Error; err != nil {
		t.Fatal(err)
	}
	oldFence := NewGatewayRoutingHealthFence(generation, channel.UpstreamAccountIdentityRevision)
	base := time.Now().UTC().Truncate(time.Microsecond)
	recoveryAt := base.Add(2 * time.Second)
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(oldFence, GatewayAppCodex, recoveryAt); err != nil {
		t.Fatalf("record old-account probe recovery: %v", err)
	}
	state, err := store.FindLatencyProbeStateForRoutingHealth(oldFence, GatewayAppCodex)
	if err != nil || state == nil || state.ChannelIdentityRevision != oldFence.ChannelIdentityRevision ||
		state.LastConfirmedRecoveryAt == nil || !state.LastConfirmedRecoveryAt.Equal(recoveryAt) {
		t.Fatalf("old-account probe state = %+v err=%v", state, err)
	}

	newRevision := channel.UpstreamAccountIdentityRevision + 1
	if err := store.db.Session(&gorm.Session{SkipHooks: true}).Model(&Channel{}).
		Where("id = ?", channel.ID).
		UpdateColumn("upstream_account_identity_revision", newRevision).Error; err != nil {
		t.Fatal(err)
	}
	newFence := NewGatewayRoutingHealthFence(generation, newRevision)
	// This event predates the old account's recovery watermark. It must still
	// start the new account's independent failure chain.
	newAccountFailureAt := base.Add(time.Second)
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(newFence, "transport", 0, "new account failure", newAccountFailureAt); err != nil {
		t.Fatalf("record new-account failure: %v", err)
	}
	key, err := store.FindUpstreamKey(generation.ChannelID, generation.CanonicalGroupName)
	if err != nil || key.RoutingFailureCount != 1 || key.LastRoutingFailureAt == nil ||
		!key.LastRoutingFailureAt.Equal(newAccountFailureAt) || !newFence.MatchesUpstreamKey(key) {
		t.Fatalf("new-account routing health = %+v err=%v", key, err)
	}

	if err := store.TouchUpstreamKeyForRoutingHealth(oldFence, base.Add(3*time.Second)); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale success error = %v", err)
	}
	if err := store.RecordUpstreamKeyRoutingFailureForRoutingHealth(oldFence, "transport", 0, "stale failure", base.Add(3*time.Second)); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale failure error = %v", err)
	}
	if err := store.RecoverUpstreamKeyRoutingFromConfirmedLatencyProbeForRoutingHealth(oldFence, GatewayAppCodex, base.Add(3*time.Second)); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale recovery error = %v", err)
	}
	if err := store.UpsertLatencyProbeStateForRoutingHealth(oldFence, &GatewayLatencyProbeState{
		GatewayApp: GatewayAppCodex,
	}); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale scheduler state error = %v", err)
	}
	if staleState, err := store.FindLatencyProbeStateForRoutingHealth(oldFence, GatewayAppCodex); err != nil || staleState != nil {
		t.Fatalf("stale account state remained current: state=%+v err=%v", staleState, err)
	}
	states, err := store.ListLatencyProbeStates()
	if err != nil {
		t.Fatal(err)
	}
	for _, current := range states {
		if current.ChannelID == generation.ChannelID && current.ChannelIdentityRevision == oldFence.ChannelIdentityRevision {
			t.Fatalf("old account probe state remained schedulable: %+v", current)
		}
	}
}
