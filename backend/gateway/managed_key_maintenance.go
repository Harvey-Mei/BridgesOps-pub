package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

const managedKeyRemoteOperationTimeout = 2 * time.Second
const managedKeyMaintenanceCleanupTimeout = 3 * time.Second

func managedKeyRemoteOperationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, managedKeyRemoteOperationTimeout)
}

// ManagedKeyMaintenanceDeferredError is a local admission result, not a
// provider failure. Callers should expose it as skipped work and must not turn
// it into channel, routing or model-capability failure state.
type ManagedKeyMaintenanceDeferredError struct {
	RetryAt time.Time
}

type managedKeyMaintenanceProbeContextKey struct{}

// withManagedKeyMaintenanceProbe permits the bounded confirmation attempts of
// one latency probe to finish after the first transient preparation failure.
// Each attempt still records the durable backoff for later scheduler passes.
func withManagedKeyMaintenanceProbe(ctx context.Context) context.Context {
	return context.WithValue(ctx, managedKeyMaintenanceProbeContextKey{}, true)
}

func managedKeyMaintenanceProbeBypass(ctx context.Context) bool {
	value, _ := ctx.Value(managedKeyMaintenanceProbeContextKey{}).(bool)
	return value
}

func (e *ManagedKeyMaintenanceDeferredError) Error() string {
	if e == nil || e.RetryAt.IsZero() {
		return "managed upstream key maintenance is temporarily deferred"
	}
	return fmt.Sprintf("managed upstream key maintenance is deferred until %s", e.RetryAt.UTC().Format(time.RFC3339))
}

func (*ManagedKeyMaintenanceDeferredError) ManagedKeyMaintenanceDeferred() bool { return true }

func managedKeyMaintenanceFailure(ctx context.Context, err error) (kind string, status int, transient bool) {
	if err == nil || errors.Is(err, context.Canceled) || ctx != nil && errors.Is(ctx.Err(), context.Canceled) {
		return "", 0, false
	}
	if errors.Is(err, context.DeadlineExceeded) || ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return storage.GatewayManagedKeyFailureTimeout, 0, true
	}
	var recoveryPending interface{ ManagedKeyRecoveryPending() bool }
	if errors.As(err, &recoveryPending) && recoveryPending.ManagedKeyRecoveryPending() {
		// A complete inventory already advanced the operation's durable absence
		// confirmation. Treating that expected wait as a provider outage would
		// defer the whole channel longer than the confirmation window and prevent
		// the next scheduler pass from completing recovery.
		return "", 0, false
	}
	var httpErr *connector.HTTPError
	if errors.As(err, &httpErr) {
		status = httpErr.StatusCode
		if status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests || status >= 500 && status <= 599 {
			return storage.GatewayManagedKeyFailureHTTPStatus, status, true
		}
		return "", 0, false
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return storage.GatewayManagedKeyFailureTimeout, 0, true
	}
	return "", 0, false
}

func (s *Service) managedKeyMaintenanceDeferred(channelID uint, identityRevision uint64, now time.Time) (*ManagedKeyMaintenanceDeferredError, error) {
	return s.managedKeyMaintenanceDeferredContext(context.Background(), channelID, identityRevision, now)
}

func (s *Service) managedKeyMaintenanceDeferredContext(ctx context.Context, channelID uint, identityRevision uint64, now time.Time) (*ManagedKeyMaintenanceDeferredError, error) {
	if s == nil || s.Store == nil {
		return nil, errors.New("managed upstream key maintenance state is unavailable")
	}
	state, err := s.Store.WithContext(ctx).ManagedKeyMaintenanceState(channelID, identityRevision)
	if err != nil || state == nil || state.RetryAt == nil {
		return nil, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if state.RetryAt.After(now) {
		return &ManagedKeyMaintenanceDeferredError{RetryAt: state.RetryAt.UTC()}, nil
	}
	return nil, nil
}

func (s *Service) recordManagedKeyMaintenanceFailure(ctx context.Context, channelID uint, identityRevision uint64, operationErr error) error {
	if err := s.persistManagedKeyMaintenanceFailure(ctx, channelID, identityRevision, operationErr); err != nil {
		return errors.Join(operationErr, fmt.Errorf("record managed upstream key maintenance backoff: %w", err))
	}
	return operationErr
}

func (s *Service) persistManagedKeyMaintenanceFailure(ctx context.Context, channelID uint, identityRevision uint64, operationErr error) error {
	kind, status, transient := managedKeyMaintenanceFailure(ctx, operationErr)
	if !transient {
		return nil
	}
	if _, err := s.Store.WithContext(ctx).RecordManagedKeyMaintenanceFailure(channelID, identityRevision, kind, status, time.Now().UTC()); err != nil {
		return err
	}
	return nil
}

func (s *Service) clearManagedKeyMaintenanceFailure(channelID uint, identityRevision uint64) error {
	return s.clearManagedKeyMaintenanceFailureContext(context.Background(), channelID, identityRevision)
}

func (s *Service) clearManagedKeyMaintenanceFailureContext(ctx context.Context, channelID uint, identityRevision uint64) error {
	if s == nil || s.Store == nil {
		return errors.New("managed upstream key maintenance state is unavailable")
	}
	return s.Store.WithContext(ctx).ClearManagedKeyMaintenanceFailure(channelID, identityRevision)
}

// ensureUpstreamAPIKeyForGroup keeps the existing local-key hot path outside
// the provider-management circuit breaker. Only work that would list, reveal,
// recover or create a remote key is admitted through the durable state.
func (s *Service) ensureUpstreamAPIKeyForGroup(ctx context.Context, channelID uint, requestedGroup string, requestedGroupID *int64, requestedGroupRatio float64, identityRepairClaimed bool, rebuildingManagedKeys bool) (string, error) {
	// API-origin discovery can advance the account identity while a remote
	// group lookup is in flight. Retry once against the refreshed identity; the
	// durable provisioning claim makes a possible post-create retry recoverable.
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		plain, err := s.ensureUpstreamAPIKeyForGroupAttempt(ctx, channelID, requestedGroup, requestedGroupID, requestedGroupRatio, identityRepairClaimed, rebuildingManagedKeys)
		if err == nil || !errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
			return plain, err
		}
		lastErr = err
	}
	return "", lastErr
}

func (s *Service) ensureUpstreamAPIKeyForGroupAttempt(ctx context.Context, channelID uint, requestedGroup string, requestedGroupID *int64, requestedGroupRatio float64, identityRepairClaimed bool, rebuildingManagedKeys bool) (string, error) {
	cacheGroupName := strings.TrimSpace(requestedGroup)
	if plain, ok, err := s.cachedUsableUpstreamKeyContext(ctx, channelID, cacheGroupName, identityRepairClaimed); err != nil {
		return "", err
	} else if ok {
		return plain, nil
	}
	channels := s.Channels.WithContext(ctx)
	channel, err := channels.FindByID(channelID)
	if err != nil {
		return "", err
	}
	if channel.CredentialMode == storage.CredentialModeDirect {
		if s.Cipher == nil {
			return "", errors.New("direct API key decryption is unavailable")
		}
		credentials, listErr := channels.ListDirectCredentials(channelID)
		if listErr != nil {
			return "", listErr
		}
		if len(credentials) == 0 {
			credentials, listErr = channels.EnsureLegacyDirectCredential(channel)
			if listErr != nil {
				return "", listErr
			}
		}
		var credential *storage.ChannelDirectCredential
		for index := range credentials {
			if credentials[index].Enabled && strings.EqualFold(strings.TrimSpace(credentials[index].GroupName), cacheGroupName) {
				credential = &credentials[index]
				break
			}
		}
		if credential == nil {
			return "", gorm.ErrRecordNotFound
		}
		plain, decryptErr := s.Cipher.Decrypt(credential.APIKeyCipher)
		if decryptErr != nil {
			return "", fmt.Errorf("decrypt direct API key: %w", decryptErr)
		}
		plain = strings.TrimSpace(plain)
		if plain == "" {
			return "", errors.New("direct API key is empty")
		}
		if err := channels.RequireOrdinaryChannelWrite(ctx, channelID); err != nil {
			return "", err
		}
		if err := s.saveDirectUpstreamKeyContext(ctx, channelID, channel.UpstreamAccountIdentityRevision, *credential, plain); err != nil {
			return "", err
		}
		return plain, nil
	}
	identityRevision := channel.UpstreamAccountIdentityRevision
	deferred, err := s.managedKeyMaintenanceDeferredContext(ctx, channelID, identityRevision, time.Now().UTC())
	if err != nil {
		return "", err
	}
	if deferred != nil && !managedKeyMaintenanceProbeBypass(ctx) {
		// Model synchronization can still use an enabled encrypted key whose old
		// remote identity metadata needs repair. Lease-producing data-plane callers
		// pass identityRepairClaimed=true and remain fail closed.
		if !identityRepairClaimed {
			if plain, ok, localErr := s.cachedManagedKeyMaterialWithoutIdentityRepairContext(ctx, channelID, cacheGroupName); localErr != nil {
				return "", localErr
			} else if ok {
				return plain, nil
			}
		}
		return "", deferred
	}

	plain, operationErr := s.ensureUpstreamAPIKeyForGroupRemote(ctx, channelID, requestedGroup, requestedGroupID, requestedGroupRatio, identityRevision, rebuildingManagedKeys)
	latest, latestErr := channels.FindByID(channelID)
	if latestErr != nil {
		if operationErr != nil {
			return "", errors.Join(operationErr, fmt.Errorf("reload managed upstream key maintenance identity: %w", latestErr))
		}
		return "", latestErr
	}
	if latest.UpstreamAccountIdentityRevision != identityRevision {
		if operationErr != nil {
			return "", errors.Join(operationErr, storage.ErrStaleChannelAccountIdentityRevision)
		}
		return "", storage.ErrStaleChannelAccountIdentityRevision
	}
	if operationErr != nil {
		return "", s.recordManagedKeyMaintenanceFailure(ctx, channelID, identityRevision, operationErr)
	}
	clearCtx := ctx
	if ctx != nil && ctx.Err() != nil {
		var cancel context.CancelFunc
		clearCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), managedKeyMaintenanceCleanupTimeout)
		defer cancel()
	}
	if err := s.clearManagedKeyMaintenanceFailureContext(clearCtx, channelID, identityRevision); err != nil {
		return "", err
	}
	return plain, nil
}

func (s *Service) cachedManagedKeyMaterialWithoutIdentityRepair(channelID uint, groupName string) (string, bool, error) {
	return s.cachedManagedKeyMaterialWithoutIdentityRepairContext(context.Background(), channelID, groupName)
}

func (s *Service) cachedManagedKeyMaterialWithoutIdentityRepairContext(ctx context.Context, channelID uint, groupName string) (string, bool, error) {
	if s == nil || s.Store == nil || s.Cipher == nil || s.Channels == nil || channelID == 0 || strings.TrimSpace(groupName) == "" {
		return "", false, nil
	}
	cached, err := s.Store.WithContext(ctx).FindUpstreamKeyAny(channelID, groupName)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !cached.Enabled || cached.Provisioning || strings.TrimSpace(cached.APIKeyCipher) == "" {
		return "", false, nil
	}
	plain, err := s.Cipher.Decrypt(cached.APIKeyCipher)
	if err != nil {
		return "", false, err
	}
	plain = strings.TrimSpace(plain)
	if plain == "" {
		return "", false, nil
	}
	if err := s.Channels.WithContext(ctx).RequireOrdinaryChannelWrite(ctx, channelID); err != nil {
		return "", false, err
	}
	return plain, true, nil
}

func (s *Service) reconcileManagedUpstreamKeysWithBackoff(ctx context.Context, channelID uint, identityRevision uint64, activeGroups map[string]struct{}, activeGroupNames map[string]string, alreadyDeletedIDs map[int64]struct{}) ([]managedUpstreamKeyReconcileFailure, bool, error) {
	deferred, err := s.managedKeyMaintenanceDeferredContext(ctx, channelID, identityRevision, time.Now().UTC())
	if err != nil {
		return nil, false, err
	}
	if deferred != nil && !managedKeyMaintenanceProbeBypass(ctx) {
		return nil, true, nil
	}
	failures, operationErr := s.reconcileManagedUpstreamKeysForSync(ctx, channelID, identityRevision, activeGroups, activeGroupNames, alreadyDeletedIDs)
	if operationErr != nil {
		return failures, false, s.recordManagedKeyMaintenanceFailure(ctx, channelID, identityRevision, operationErr)
	}
	for _, failure := range failures {
		if _, _, transient := managedKeyMaintenanceFailure(ctx, failure.Err); !transient {
			continue
		}
		// Preserve a durable retry for partial provider failures without turning
		// that group into a channel-wide abort for the current pass.
		if err := s.persistManagedKeyMaintenanceFailure(ctx, channelID, identityRevision, failure.Err); err != nil {
			return failures, false, fmt.Errorf("record partial managed upstream key maintenance backoff: %w", err)
		}
		return failures, false, nil
	}
	if err := s.clearManagedKeyMaintenanceFailureContext(ctx, channelID, identityRevision); err != nil {
		return failures, false, err
	}
	return failures, false, nil
}

func (s *Service) removeStaleUpstreamGroupWithBackoff(ctx context.Context, channelID uint, groupName string, identityRevision uint64, preserveCandidates bool) (int64, string, bool, error) {
	deferred, err := s.managedKeyMaintenanceDeferredContext(ctx, channelID, identityRevision, time.Now().UTC())
	if err != nil {
		return 0, "", false, err
	}
	if deferred != nil && !managedKeyMaintenanceProbeBypass(ctx) {
		return 0, "", true, nil
	}
	keyID, keyName, operationErr := s.removeStaleUpstreamGroup(ctx, channelID, groupName, identityRevision, preserveCandidates)
	if operationErr != nil {
		return keyID, keyName, false, s.recordManagedKeyMaintenanceFailure(ctx, channelID, identityRevision, operationErr)
	}
	clearCtx := ctx
	if ctx != nil && ctx.Err() != nil {
		var cancel context.CancelFunc
		clearCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), managedKeyMaintenanceCleanupTimeout)
		defer cancel()
	}
	if err := s.clearManagedKeyMaintenanceFailureContext(clearCtx, channelID, identityRevision); err != nil {
		return keyID, keyName, false, err
	}
	return keyID, keyName, false, nil
}
