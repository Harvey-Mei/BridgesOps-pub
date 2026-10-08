package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptrace"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

type dispatchInvalidationFixture struct {
	channel           *storage.Channel
	dispatchFence     UpstreamDispatchFence
	invalidationFence storage.GatewayUpstreamKeyInvalidationFence
}

type dispatchInvalidationRoundTripperFunc func(*http.Request) (*http.Response, error)

func (fn dispatchInvalidationRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func newDispatchInvalidationFixture(t *testing.T, db *gorm.DB, suffix string) dispatchInvalidationFixture {
	t.Helper()
	channels := storage.NewChannels(db)
	store := storage.NewGateway(db)
	channel := &storage.Channel{
		Name: "dispatch invalidation " + suffix, Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://dispatch-invalidation.invalid", CredentialMode: storage.CredentialModePassword,
		MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	groupName := "primary-" + suffix
	if err := db.Create(&storage.RateSnapshot{ChannelID: channel.ID, ModelName: groupName, Ratio: 1}).Error; err != nil {
		t.Fatal(err)
	}
	key := &storage.GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: groupName, APIKeyCipher: "cipher-" + suffix,
		APIKeyPrefix: "prefix-" + suffix, Enabled: true,
	}
	if err := db.Create(key).Error; err != nil {
		t.Fatal(err)
	}
	if err := store.RecordUpstreamGroupChange(channel.ID, groupName, "added", int64(channel.ID), "managed-"+suffix, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(channel.ID, groupName)
	if err != nil {
		t.Fatal(err)
	}
	return dispatchInvalidationFixture{
		channel: channel,
		dispatchFence: UpstreamDispatchFence{
			ChannelID: channel.ID, GroupName: groupName,
			CredentialMode: channel.CredentialMode, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
			LocalKeyID: key.ID, KeyGeneration: key.KeyGeneration, App: storage.GatewayAppCodex,
		},
		invalidationFence: storage.NewGatewayUpstreamKeyInvalidationFence(
			generation, channel.UpstreamAccountIdentityRevision, key.ID, key.KeyGeneration,
		),
	}
}

func TestDispatchFenceAndAuthenticationInvalidationAreStrictlyOrderedPostgres(t *testing.T) {
	requireDirectGatewayDB(t)
	dbA := testdb.Open(t)
	dbB, err := testdb.ReopenWithError(t, dbA)
	if err != nil {
		t.Fatal(err)
	}
	serviceA := NewService(storage.NewChannels(dbA), storage.NewRates(dbA), storage.NewGateway(dbA), nil, nil, nil)
	serviceB := NewService(storage.NewChannels(dbB), storage.NewRates(dbB), storage.NewGateway(dbB), nil, nil, nil)

	t.Run("dispatch already at transport boundary precedes invalidation", func(t *testing.T) {
		fixture := newDispatchInvalidationFixture(t, dbA, "dispatch")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://dispatch-invalidation.invalid/v1/models", nil)
		if err != nil {
			t.Fatal(err)
		}
		fenceValidated := make(chan struct{})
		releaseFenceCallback := make(chan struct{})
		transportCalled := make(chan struct{})
		writeHeaders := make(chan struct{})
		releaseTransport := make(chan struct{})
		dispatchDone := make(chan error, 1)
		go func() {
			response, err := serviceA.RoundTripWithUpstreamDispatchFence(
				ctx, fixture.dispatchFence,
				func(lockedCtx context.Context) error {
					if !storage.ChannelOperationLockHeld(lockedCtx, fixture.channel.ID) {
						return errors.New("dispatch callback did not carry channel operation lock")
					}
					close(fenceValidated)
					select {
					case <-releaseFenceCallback:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
				request,
				dispatchInvalidationRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
					close(transportCalled)
					select {
					case <-writeHeaders:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					trace := httptrace.ContextClientTrace(request.Context())
					if trace == nil || trace.WroteHeaders == nil {
						return nil, errors.New("transport-entry trace is unavailable")
					}
					trace.WroteHeaders()
					select {
					case <-releaseTransport:
						return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}),
			)
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			dispatchDone <- err
		}()
		select {
		case <-fenceValidated:
		case <-ctx.Done():
			t.Fatal("dispatch fence was not validated")
		}

		invalidationDone := make(chan error, 1)
		go func() {
			invalidationDone <- serviceB.Store.DisableUpstreamKeyForIdentityContext(ctx, fixture.invalidationFence, "observed 401")
		}()
		if err := waitForDispatchInvalidationAdvisoryWaiter(dbA, fixture.channel.ID, 3*time.Second); err != nil {
			close(releaseFenceCallback)
			t.Fatal(err)
		}
		select {
		case err := <-invalidationDone:
			close(releaseFenceCallback)
			t.Fatalf("invalidation crossed an active dispatch boundary: %v", err)
		default:
		}
		close(releaseFenceCallback)
		select {
		case <-transportCalled:
		case <-ctx.Done():
			t.Fatal("configured RoundTripper was not called")
		}
		granted, waiting, err := dispatchInvalidationAdvisoryLockState(dbA, fixture.channel.ID)
		if err != nil {
			t.Fatal(err)
		}
		if granted != 1 || waiting != 1 {
			t.Fatalf("lock state before real transport entry: granted=%d waiting=%d", granted, waiting)
		}
		close(writeHeaders)
		if err := <-invalidationDone; err != nil {
			t.Fatalf("invalidation after dispatch failed: %v", err)
		}
		select {
		case err := <-dispatchDone:
			t.Fatalf("RoundTripper completed before release: %v", err)
		default:
		}
		close(releaseTransport)
		if err := <-dispatchDone; err != nil {
			t.Fatalf("dispatch boundary failed: %v", err)
		}
		stored, err := serviceA.Store.FindUpstreamKeyByID(fixture.dispatchFence.LocalKeyID)
		if err != nil || stored.Enabled || stored.APIKeyCipher != "" {
			t.Fatalf("ordered invalidation result: key=%+v err=%v", stored, err)
		}
	})

	t.Run("invalidation precedes and rejects the next dispatch", func(t *testing.T) {
		fixture := newDispatchInvalidationFixture(t, dbA, "invalidate")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		invalidationCommitted := make(chan struct{})
		releaseInvalidation := make(chan struct{})
		invalidationDone := make(chan error, 1)
		go func() {
			invalidationDone <- serviceA.WithChannelOperationLockAndThen(ctx, fixture.channel.ID, func(lockedCtx context.Context) error {
				if err := serviceA.WithUpstreamDispatchFenceAndThen(lockedCtx, fixture.dispatchFence, func(dispatchCtx context.Context) error {
					if !storage.ChannelOperationLockHeld(dispatchCtx, fixture.channel.ID) {
						return errors.New("nested dispatch lost channel operation lock")
					}
					return nil
				}); err != nil {
					return err
				}
				authEvidenceCtx, cancelAuthEvidence := context.WithCancel(lockedCtx)
				cancelAuthEvidence()
				if err := serviceA.InvalidateUpstreamGroupKeyForIdentityContext(authEvidenceCtx, fixture.invalidationFence); err != nil {
					return err
				}
				close(invalidationCommitted)
				select {
				case <-releaseInvalidation:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
		select {
		case <-invalidationCommitted:
		case <-ctx.Done():
			t.Fatal("reentrant invalidation did not commit")
		}

		dispatchCallbackRan := false
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://dispatch-invalidation.invalid/v1/models", nil)
		if err != nil {
			t.Fatal(err)
		}
		dispatchDone := make(chan error, 1)
		go func() {
			response, err := serviceB.RoundTripWithUpstreamDispatchFence(
				ctx, fixture.dispatchFence, nil, request,
				dispatchInvalidationRoundTripperFunc(func(*http.Request) (*http.Response, error) {
					dispatchCallbackRan = true
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
				}),
			)
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			dispatchDone <- err
		}()
		if err := waitForDispatchInvalidationAdvisoryWaiter(dbA, fixture.channel.ID, 3*time.Second); err != nil {
			close(releaseInvalidation)
			t.Fatal(err)
		}
		select {
		case err := <-dispatchDone:
			close(releaseInvalidation)
			t.Fatalf("dispatch crossed an active invalidation boundary: %v", err)
		default:
		}
		close(releaseInvalidation)
		if err := <-invalidationDone; err != nil {
			t.Fatalf("invalidation boundary failed: %v", err)
		}
		if err := <-dispatchDone; err == nil {
			t.Fatal("dispatch accepted the key disabled by the preceding invalidation")
		}
		if dispatchCallbackRan {
			t.Fatal("transport callback ran after the key was invalidated")
		}
	})
}

const dispatchInvalidationAdvisoryLockNamespace int64 = 0x424f000000000000

func dispatchInvalidationAdvisoryLockID(channelID uint) int64 {
	return dispatchInvalidationAdvisoryLockNamespace ^ int64(uint64(channelID)&0x00ffffffffffffff)
}

func dispatchInvalidationAdvisoryLockState(db *gorm.DB, channelID uint) (granted, waiting int, err error) {
	lockID := dispatchInvalidationAdvisoryLockID(channelID)
	err = db.Raw(`
		SELECT
			COUNT(*) FILTER (WHERE granted),
			COUNT(*) FILTER (WHERE NOT granted)
		FROM pg_locks
		WHERE locktype = 'advisory'
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		  AND ((classid::bigint << 32) | objid::bigint) = ?`, lockID).Row().Scan(&granted, &waiting)
	return
}

func waitForDispatchInvalidationAdvisoryWaiter(db *gorm.DB, channelID uint, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		_, waiting, err := dispatchInvalidationAdvisoryLockState(db, channelID)
		if err != nil {
			return err
		}
		if waiting > 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for channel-operation advisory-lock waiter")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
