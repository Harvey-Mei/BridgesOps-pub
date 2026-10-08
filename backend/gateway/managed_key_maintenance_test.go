package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type managedKeyTimeoutError struct{}

func (managedKeyTimeoutError) Error() string   { return "provider management timeout" }
func (managedKeyTimeoutError) Timeout() bool   { return true }
func (managedKeyTimeoutError) Temporary() bool { return true }

var _ net.Error = managedKeyTimeoutError{}

type managedKeyRecoveryPendingTestError struct{}

func (managedKeyRecoveryPendingTestError) Error() string                   { return "recovery pending" }
func (managedKeyRecoveryPendingTestError) ManagedKeyRecoveryPending() bool { return true }

func TestManagedKeyMaintenanceFailureClassification(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		err        error
		wantKind   string
		wantStatus int
		want       bool
	}{
		{name: "429", ctx: context.Background(), err: connector.HTTPStatusError(http.StatusTooManyRequests, nil), wantKind: storage.GatewayManagedKeyFailureHTTPStatus, wantStatus: 429, want: true},
		{name: "503", ctx: context.Background(), err: connector.HTTPStatusError(http.StatusServiceUnavailable, nil), wantKind: storage.GatewayManagedKeyFailureHTTPStatus, wantStatus: 503, want: true},
		{name: "authentication", ctx: context.Background(), err: connector.HTTPStatusError(http.StatusUnauthorized, nil)},
		{name: "network timeout", ctx: context.Background(), err: managedKeyTimeoutError{}, wantKind: storage.GatewayManagedKeyFailureTimeout, want: true},
		{name: "internal deadline", ctx: context.Background(), err: context.DeadlineExceeded, wantKind: storage.GatewayManagedKeyFailureTimeout, want: true},
		{name: "expired request context", ctx: expiredManagedKeyMaintenanceContext(), err: context.DeadlineExceeded, wantKind: storage.GatewayManagedKeyFailureTimeout, want: true},
		{name: "recovery pending", ctx: context.Background(), err: managedKeyRecoveryPendingTestError{}},
		{name: "wrapped recovery pending", ctx: context.Background(), err: fmt.Errorf("maintain managed key: %w", managedKeyRecoveryPendingTestError{})},
		{name: "caller canceled", ctx: canceledManagedKeyMaintenanceContext(), err: context.Canceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind, status, transient := managedKeyMaintenanceFailure(test.ctx, test.err)
			if kind != test.wantKind || status != test.wantStatus || transient != test.want {
				t.Fatalf("classification = kind %q status %d transient %v", kind, status, transient)
			}
		})
	}
}

func expiredManagedKeyMaintenanceContext() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	return ctx
}

func canceledManagedKeyMaintenanceContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestEnsureUpstreamKeyPersistsAndHonorsManagedKeyMaintenanceBackoff(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "managed-key-maintenance-backoff")
	keys := &fakeKeyManager{
		groups:    []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		createErr: connector.HTTPStatusError(http.StatusTooManyRequests, nil),
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	if plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1); err == nil || plain != "" {
		t.Fatalf("first managed-key failure = %q, %v", plain, err)
	}
	state, err := store.ManagedKeyMaintenanceState(channel.ID, channel.UpstreamAccountIdentityRevision)
	if err != nil || state == nil || state.FailureCount != 1 || state.RetryAt == nil || !state.RetryAt.After(time.Now()) || state.LastFailureStatus != 429 {
		t.Fatalf("persisted managed-key backoff = %+v, err=%v", state, err)
	}
	groupCalls, createCalls := keys.listGroupCalls, len(keys.created)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	var deferred *ManagedKeyMaintenanceDeferredError
	if plain != "" || !errors.As(err, &deferred) {
		t.Fatalf("deferred managed-key maintenance = %q, %v", plain, err)
	}
	if keys.listGroupCalls != groupCalls || len(keys.created) != createCalls {
		t.Fatalf("deferred maintenance reached provider: groups %d->%d creates %d->%d", groupCalls, keys.listGroupCalls, createCalls, len(keys.created))
	}
}

func TestManagedKeyMaintenanceBackoffDoesNotBlockUsableLocalKey(t *testing.T) {
	channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixture(t, "managed-key-backoff-local-key")
	plain := "sk-still-usable"
	encrypted, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "promo", GroupRatio: 1, UpstreamKeyID: 77,
		UpstreamKeyName: upstreamProvisioningKeyPrefix + "1234567890abcdef", APIKeyCipher: encrypted,
		APIKeyPrefix: keyPrefix(plain), Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordManagedKeyMaintenanceFailure(channel.ID, channel.UpstreamAccountIdentityRevision, storage.GatewayManagedKeyFailureHTTPStatus, 429, time.Now()); err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{groupsErr: errors.New("provider management API must not be called")}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	got, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if err != nil || got != plain || keys.listGroupCalls != 0 {
		t.Fatalf("usable key during maintenance backoff = %q, calls=%d, err=%v", got, keys.listGroupCalls, err)
	}
}

func TestManagedKeyMaintenanceSuccessIsFencedToStartingIdentityRevision(t *testing.T) {
	db, channel, channels, rates, store, cipher := newUpstreamKeyRecoveryFixtureWithDB(t, "managed-key-backoff-latest-identity")
	oldRevision := channel.UpstreamAccountIdentityRevision
	if _, err := store.RecordManagedKeyMaintenanceFailure(channel.ID, oldRevision, storage.GatewayManagedKeyFailureHTTPStatus, 429, time.Now().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&storage.GatewayManagedKeyMaintenanceState{}).Where("channel_id = ?", channel.ID).Update("retry_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	keys := &fakeKeyManager{
		groups: []connector.APIKeyGroup{{Name: "promo", Ratio: 1}},
		beforeListGroups: func() {
			if err := db.Model(&storage.Channel{}).Where("id = ?", channel.ID).
				Update("upstream_account_identity_revision", oldRevision+1).Error; err != nil {
				t.Fatalf("advance identity revision: %v", err)
			}
		},
		listAPIKeys: func(connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			return &connector.APIKeyPage{Page: 1, Pages: 1}, nil
		},
	}
	svc := NewService(channels, rates, store, cipher, keys, nil)
	plain, err := svc.EnsureUpstreamAPIKeyForGroup(context.Background(), channel.ID, "promo", nil, 1)
	if plain == "" || err != nil {
		t.Fatalf("managed-key result after identity advance = %q, %v", plain, err)
	}
	if keys.listGroupCalls != 2 {
		t.Fatalf("managed-key identity advance retries = %d, want one retry", keys.listGroupCalls)
	}
	if current, err := store.ManagedKeyMaintenanceState(channel.ID, oldRevision+1); err != nil || current != nil {
		t.Fatalf("stale success changed latest identity backoff: state=%+v err=%v", current, err)
	}
}
