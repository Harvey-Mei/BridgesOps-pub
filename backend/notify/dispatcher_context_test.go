package notify

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

type cancelingContextCooldownStore struct {
	cancel    context.CancelFunc
	claimedAt time.Time
	releases  int
}

func (s *cancelingContextCooldownStore) TryClaimCooldown(uint, storage.NotificationEvent, time.Duration) (bool, error) {
	return false, errors.New("non-context cooldown path must not be used")
}

func (s *cancelingContextCooldownStore) TryClaimCooldownContext(context.Context, uint, storage.NotificationEvent, time.Duration) (bool, time.Time, error) {
	s.claimedAt = time.Now().UTC().Truncate(time.Microsecond)
	s.cancel()
	return true, s.claimedAt, nil
}

func (s *cancelingContextCooldownStore) ReleaseCooldownClaimContext(_ context.Context, _ uint, _ storage.NotificationEvent, claimedAt time.Time) error {
	if !claimedAt.Equal(s.claimedAt) {
		return errors.New("released a different cooldown claim")
	}
	s.releases++
	return nil
}

func TestDispatchReleasesCooldownWhenIdentityContextExpiresBeforeNetworkSend(t *testing.T) {
	db := testdb.Open(t)
	repo := storage.NewNotifications(db)
	if err := repo.CreateChannel(&storage.NotificationChannel{
		Name: "not reached", Type: storage.NotifyWebhook, ConfigCipher: "not-decrypted", Enabled: true,
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}
	cipher, err := crypto.NewCipher("notification-context-cooldown-test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelingContextCooldownStore{cancel: cancel}
	dispatcher := NewDispatcherWithCooldown(repo, cipher, nil, Policy{
		BalanceLowCooldown: time.Hour,
		SendMaxAttempts:    1,
	}, store)
	err = dispatcher.Dispatch(ctx, Message{
		Event: storage.EventBalanceLow, ChannelID: 42,
		Subject: "stale account", Body: "must not be sent",
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatch error = %v, want context canceled", err)
	}
	if store.releases != 1 {
		t.Fatalf("cooldown releases = %d, want 1", store.releases)
	}
	logs, err := repo.ListLogs(10)
	if err != nil {
		t.Fatalf("list notification logs: %v", err)
	}
	if len(logs) != 0 {
		t.Fatalf("canceled dispatch persisted logs: %+v", logs)
	}
}
