package storage

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestManagedKeyMaintenanceBackoffIsIdentityFenced(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "managed-key-backoff", Type: ChannelTypeNewAPI, SiteURL: "https://backoff.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
	first, err := store.RecordManagedKeyMaintenanceFailure(channel.ID, channel.UpstreamAccountIdentityRevision, GatewayManagedKeyFailureHTTPStatus, 429, observedAt)
	if err != nil {
		t.Fatal(err)
	}
	if first.FailureCount != 1 || first.RetryAt == nil || !first.RetryAt.Equal(observedAt.Add(10*time.Minute)) || first.LastFailureStatus != 429 {
		t.Fatalf("first managed-key backoff = %+v", first)
	}
	secondAt := observedAt.Add(2 * time.Minute)
	second, err := store.RecordManagedKeyMaintenanceFailure(channel.ID, channel.UpstreamAccountIdentityRevision, GatewayManagedKeyFailureTimeout, 504, secondAt)
	if err != nil {
		t.Fatal(err)
	}
	if second.FailureCount != 2 || second.RetryAt == nil || !second.RetryAt.Equal(secondAt.Add(30*time.Minute)) || second.LastFailureStatus != 0 {
		t.Fatalf("second managed-key backoff = %+v", second)
	}

	newRevision := channel.UpstreamAccountIdentityRevision + 1
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).Update("upstream_account_identity_revision", newRevision).Error; err != nil {
		t.Fatal(err)
	}
	if stale, err := store.ManagedKeyMaintenanceState(channel.ID, channel.UpstreamAccountIdentityRevision); err != nil || stale != nil {
		t.Fatalf("old identity backoff remained active: state=%+v err=%v", stale, err)
	}
	if _, err := store.RecordManagedKeyMaintenanceFailure(channel.ID, channel.UpstreamAccountIdentityRevision, GatewayManagedKeyFailureHTTPStatus, 429, observedAt); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale identity recorded backoff: %v", err)
	}
	replacement, err := store.RecordManagedKeyMaintenanceFailure(channel.ID, newRevision, GatewayManagedKeyFailureHTTPStatus, 503, observedAt)
	if err != nil || replacement.FailureCount != 1 || replacement.ChannelIdentityRevision != newRevision {
		t.Fatalf("new identity backoff = %+v err=%v", replacement, err)
	}
	if err := store.ClearManagedKeyMaintenanceFailure(channel.ID, channel.UpstreamAccountIdentityRevision); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale identity cleared backoff: %v", err)
	}
	if err := store.ClearManagedKeyMaintenanceFailure(channel.ID, newRevision); err != nil {
		t.Fatal(err)
	}
	if cleared, err := store.ManagedKeyMaintenanceState(channel.ID, newRevision); err != nil || cleared != nil {
		t.Fatalf("managed-key backoff was not cleared: state=%+v err=%v", cleared, err)
	}
}

func TestManagedKeyMaintenanceBackoffRejectsInvalidHTTPStatus(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	store := NewGateway(db)
	for _, status := range []int{0, 99, 600} {
		if _, err := store.RecordManagedKeyMaintenanceFailure(1, 1, GatewayManagedKeyFailureHTTPStatus, status, time.Now()); err == nil {
			t.Fatalf("invalid HTTP status %d was accepted", status)
		}
	}
}

func TestManagedKeyMaintenanceIgnoresLegacyRecoveryPendingState(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "legacy-recovery-pending", Type: ChannelTypeNewAPI, SiteURL: "https://recovery.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	observedAt := time.Date(2026, time.August, 12, 2, 39, 0, 0, time.UTC)
	if _, err := store.RecordManagedKeyMaintenanceFailure(channel.ID, channel.UpstreamAccountIdentityRevision, GatewayManagedKeyFailureRecoveryPending, 0, observedAt); err != nil {
		t.Fatal(err)
	}
	if state, err := store.ManagedKeyMaintenanceState(channel.ID, channel.UpstreamAccountIdentityRevision); err != nil || state != nil {
		t.Fatalf("legacy recovery-pending state remained active: state=%+v err=%v", state, err)
	}
	transientAt := observedAt.Add(time.Minute)
	state, err := store.RecordManagedKeyMaintenanceFailure(channel.ID, channel.UpstreamAccountIdentityRevision, GatewayManagedKeyFailureHTTPStatus, http.StatusTooManyRequests, transientAt)
	if err != nil {
		t.Fatal(err)
	}
	if state.FailureCount != 1 || state.RetryAt == nil || !state.RetryAt.Equal(transientAt.Add(10*time.Minute)) {
		t.Fatalf("first real failure inherited legacy ladder: %+v", state)
	}
}
