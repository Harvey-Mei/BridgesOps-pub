package gateway

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	appcrypto "github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

type channelDeletionTestKeyManager struct {
	mu                sync.Mutex
	inventory         []connector.APIKey
	inventoryPageHook func(context.Context, connector.APIKeyQuery) (*connector.APIKeyPage, error)
	groups            []connector.APIKeyGroup
	revealed          map[int64]string
	revealHook        func(context.Context, int64) (string, error)
	deleteCalls       []int64
	deleteErr         error
	deleteHook        func(context.Context, int64) error
	prepareDeleteHook func(context.Context, int64) error
	removeOnDelete    bool
}

func (m *channelDeletionTestKeyManager) inventoryPage(ctx context.Context, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.inventoryPageHook != nil {
		return m.inventoryPageHook(ctx, query)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = len(m.inventory)
		if pageSize == 0 {
			pageSize = 1
		}
	}
	page := query.Page
	if page <= 0 {
		page = 1
	}
	total := len(m.inventory)
	pages := 0
	if total > 0 {
		pages = (total + pageSize - 1) / pageSize
	}
	start := (page - 1) * pageSize
	end := start + pageSize
	if start > total {
		start = total
	}
	if end > total {
		end = total
	}
	items := append([]connector.APIKey(nil), m.inventory[start:end]...)
	return &connector.APIKeyPage{
		Items: items, Total: int64(total), Page: page, PageSize: pageSize, Pages: pages,
	}, nil
}

func (m *channelDeletionTestKeyManager) delete(ctx context.Context, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteHook != nil {
		if err := m.deleteHook(ctx, id); err != nil {
			return err
		}
	}
	m.deleteCalls = append(m.deleteCalls, id)
	if m.removeOnDelete {
		for index := range m.inventory {
			if m.inventory[index].ID == id {
				m.inventory = append(m.inventory[:index], m.inventory[index+1:]...)
				break
			}
		}
	}
	return m.deleteErr
}

func (m *channelDeletionTestKeyManager) ListAPIKeys(ctx context.Context, _ uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	return m.inventoryPage(ctx, query)
}

func (m *channelDeletionTestKeyManager) ListAPIKeyGroups(ctx context.Context, _ uint) ([]connector.APIKeyGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]connector.APIKeyGroup(nil), m.groups...), nil
}

func (m *channelDeletionTestKeyManager) CreateAPIKey(context.Context, uint, connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	return nil, errors.New("channel deletion test key creation is unsupported")
}

func (m *channelDeletionTestKeyManager) RevealAPIKey(ctx context.Context, _ uint, id int64) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if m.revealHook != nil {
		return m.revealHook(ctx, id)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.revealed[id], nil
}

func (m *channelDeletionTestKeyManager) PrepareManagedAPIKeyDeletion(ctx context.Context, _ uint, expected connector.APIKey, _ uint64) (func() error, error) {
	return func() error { return m.delete(ctx, expected.ID) }, nil
}

func (m *channelDeletionTestKeyManager) ListAPIKeysForChannelDeletion(ctx context.Context, _ uint, _, _ uint64, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	return m.inventoryPage(ctx, query)
}

func (m *channelDeletionTestKeyManager) ListAPIKeyGroupsForChannelDeletion(ctx context.Context, _ uint, _, _ uint64) ([]connector.APIKeyGroup, error) {
	return m.ListAPIKeyGroups(ctx, 0)
}

func (m *channelDeletionTestKeyManager) RevealAPIKeyForChannelDeletion(ctx context.Context, _ uint, _, _ uint64, id int64) (string, error) {
	return m.RevealAPIKey(ctx, 0, id)
}

func (m *channelDeletionTestKeyManager) PrepareAPIKeyDeletionForChannelDeletion(ctx context.Context, _ uint, _, _ uint64, id int64) (func() error, error) {
	if m.prepareDeleteHook != nil {
		if err := m.prepareDeleteHook(ctx, id); err != nil {
			return nil, err
		}
	}
	return func() error { return m.delete(ctx, id) }, nil
}

type channelDeletionCoordinatorFixture struct {
	db       *gorm.DB
	channels *storage.Channels
	rates    *storage.Rates
	store    *storage.Gateway
	cipher   *appcrypto.Cipher
	channel  *storage.Channel
}

func newChannelDeletionCoordinatorFixture(t *testing.T) channelDeletionCoordinatorFixture {
	t.Helper()
	db := testdb.Open(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	store := storage.NewGateway(db)
	cipher, err := appcrypto.NewCipher("channel-deletion-coordinator-test-secret")
	if err != nil {
		t.Fatalf("create channel deletion test cipher: %v", err)
	}
	channel := &storage.Channel{
		Name: t.Name(), Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://channel-deletion.invalid", MonitorEnabled: true, LatencyProbeEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create channel deletion test channel: %v", err)
	}
	return channelDeletionCoordinatorFixture{
		db: db, channels: channels, rates: rates, store: store, cipher: cipher, channel: channel,
	}
}

func (f channelDeletionCoordinatorFixture) addKey(t *testing.T, group, provenance, plain string, remoteID int64, remoteName string) storage.GatewayUpstreamKey {
	t.Helper()
	if _, err := f.rates.Upsert(&storage.RateSnapshot{
		ChannelID: f.channel.ID, ModelName: group, Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create channel deletion rate %s: %v", group, err)
	}
	ciphertext := ""
	prefix := ""
	if plain != "" {
		var err error
		ciphertext, err = f.cipher.Encrypt(plain)
		if err != nil {
			t.Fatalf("encrypt channel deletion key: %v", err)
		}
		prefix = keyPrefix(plain)
	}
	key := storage.GatewayUpstreamKey{
		ChannelID: f.channel.ID, GroupName: group, UpstreamKeyID: remoteID,
		UpstreamKeyName: remoteName, OwnershipProvenance: provenance,
		APIKeyCipher: ciphertext, APIKeyPrefix: prefix, Enabled: true,
	}
	if err := f.db.Create(&key).Error; err != nil {
		t.Fatalf("create channel deletion local key: %v", err)
	}
	return key
}

func (f channelDeletionCoordinatorFixture) prepare(t *testing.T) storage.ChannelDeletionIntent {
	t.Helper()
	if err := f.channels.DeleteContext(context.Background(), f.channel.ID); !errors.Is(err, storage.ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion: %v", err)
	}
	var intent storage.ChannelDeletionIntent
	if err := f.db.Where("channel_id = ?", f.channel.ID).First(&intent).Error; err != nil {
		t.Fatalf("load prepared channel deletion intent: %v", err)
	}
	return intent
}

func channelDeletionTestRemote(id int64, name, group string) connector.APIKey {
	return connector.APIKey{
		ID: id, Name: name, Status: "active", Group: group, GroupName: group,
		UnlimitedQuota: true, ExpiredTime: -1, CrossGroupRetry: true,
		Source: connector.APIKeySourceNewAPI,
		ObservedFields: connector.APIKeyObservedFields{
			Quota: true, UnlimitedQuota: true, ExpiredTime: true, AllowIPs: true,
			ModelLimitsEnabled: true, ModelLimits: true, CrossGroupRetry: true,
		},
	}
}

func TestChannelDeletionCoordinatorFinalizesZeroKeyChannelAndRetainsAudit(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	if err := fixture.db.Create(&storage.AuthSession{ChannelID: fixture.channel.ID, AccessTokenCipher: "frozen-session"}).Error; err != nil {
		t.Fatalf("create frozen auth session: %v", err)
	}
	intent := fixture.prepare(t)
	manager := &channelDeletionTestKeyManager{revealed: make(map[int64]string)}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)

	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil {
		t.Fatalf("reconcile zero-key channel deletion: %v", err)
	}
	if result.IntentsScanned != 1 || result.Finalized != 1 || result.Failed != 0 {
		t.Fatalf("zero-key reconcile result = %+v", result)
	}
	if err := fixture.db.First(&storage.Channel{}, fixture.channel.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("physically deleted channel lookup error = %v", err)
	}
	var finalAudit storage.ChannelDeletionFinalAudit
	if err := fixture.db.Where("intent_id = ?", intent.ID).First(&finalAudit).Error; err != nil {
		t.Fatalf("load surviving final audit: %v", err)
	}
	if finalAudit.ChannelID != fixture.channel.ID || finalAudit.PreparedKeyCount != 0 ||
		finalAudit.DeleteConfirmedCount != 0 || finalAudit.AuthSessionCleanupStatus != "deleted" {
		t.Fatalf("zero-key final audit = %+v", finalAudit)
	}
	var storedIntent storage.ChannelDeletionIntent
	if err := fixture.db.First(&storedIntent, intent.ID).Error; err != nil {
		t.Fatalf("load surviving finalized intent: %v", err)
	}
	if storedIntent.Status != storage.ChannelDeletionIntentFinalized || storedIntent.FinalizedAt == nil {
		t.Fatalf("surviving intent = %+v", storedIntent)
	}
}

func TestChannelDeletionCoordinatorFinalizesDirectChannelWithoutRemoteKeyCalls(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	if err := fixture.db.Model(&storage.Channel{}).Where("id = ?", fixture.channel.ID).Updates(map[string]any{
		"credential_mode": storage.CredentialModeDirect,
		"password_cipher": "direct-channel-cipher",
		"direct_models":   storage.EncodeDirectModels([]string{"vendor-alias"}),
		"gateway_app":     storage.GatewayAppCodex,
	}).Error; err != nil {
		t.Fatalf("configure direct channel: %v", err)
	}
	if err := fixture.db.Create(&storage.GatewayUpstreamKey{
		ChannelID: fixture.channel.ID, GroupName: storage.DirectChannelGroupName,
		UpstreamKeyID: 0, UpstreamKeyName: storage.DirectChannelUpstreamKeyName,
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceUnknown,
		APIKeyCipher:        "direct-routing-cipher", APIKeyPrefix: "sk-direct", Enabled: true,
	}).Error; err != nil {
		t.Fatalf("create direct routing key: %v", err)
	}
	if _, err := fixture.rates.Upsert(&storage.RateSnapshot{
		ChannelID: fixture.channel.ID, ModelName: storage.DirectChannelGroupName,
		Ratio: 1, LastSeenAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create direct rate: %v", err)
	}
	intent := fixture.prepare(t)

	inventoryCalls := 0
	manager := &channelDeletionTestKeyManager{
		revealed: make(map[int64]string),
		inventoryPageHook: func(context.Context, connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			inventoryCalls++
			return nil, errors.New("direct channel must not inspect remote managed keys")
		},
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil {
		t.Fatalf("reconcile direct channel deletion: %v", err)
	}
	if result.IntentsScanned != 1 || result.Finalized != 1 || result.Failed != 0 {
		t.Fatalf("direct deletion reconcile result = %+v", result)
	}
	if inventoryCalls != 0 || len(manager.deleteCalls) != 0 {
		t.Fatalf("direct deletion made remote key calls: inventory=%d deletes=%v", inventoryCalls, manager.deleteCalls)
	}
	var finalAudit storage.ChannelDeletionFinalAudit
	if err := fixture.db.Where("intent_id = ?", intent.ID).First(&finalAudit).Error; err != nil {
		t.Fatalf("load direct deletion final audit: %v", err)
	}
	if finalAudit.PreparedKeyCount != 0 || finalAudit.DeleteConfirmedCount != 0 || finalAudit.RetainedByReviewCount != 0 {
		t.Fatalf("direct deletion final audit = %+v", finalAudit)
	}
}

func TestChannelDeletionCoordinatorSeparatesCreatedAdoptedAndUnknownOwnership(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	createdPlain := "sk-created-channel-deletion-owned"
	createdName := upstreamKeyNameForGroup("created")
	adoptedName := "operator-adopted-key"
	created := fixture.addKey(t, "created", storage.GatewayUpstreamKeyProvenanceCreated, createdPlain, 101, createdName)
	adopted := fixture.addKey(t, "adopted", storage.GatewayUpstreamKeyProvenanceAdopted, "", 102, adoptedName)
	unknown := fixture.addKey(t, "unknown", storage.GatewayUpstreamKeyProvenanceUnknown, "", 103, "unknown-key")
	intent := fixture.prepare(t)
	manager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{
			channelDeletionTestRemote(101, createdName, "created"),
			channelDeletionTestRemote(102, adoptedName, "adopted"),
			channelDeletionTestRemote(103, "unknown-key", "unknown"),
		},
		revealed: map[int64]string{101: createdPlain}, removeOnDelete: true,
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)

	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil {
		t.Fatalf("reconcile mixed ownership channel deletion: %v", err)
	}
	if result.Finalized != 1 || result.Failed != 0 {
		t.Fatalf("mixed ownership reconcile result = %+v", result)
	}
	var claims []storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ?", intent.ID).Order("local_key_id ASC").Find(&claims).Error; err != nil {
		t.Fatalf("load mixed ownership claims: %v", err)
	}
	statusByLocalID := make(map[uint]storage.ChannelDeletionKeyClaimStatus, len(claims))
	for _, claim := range claims {
		statusByLocalID[claim.LocalKeyID] = claim.Status
	}
	if statusByLocalID[created.ID] != storage.ChannelDeletionKeyDeleteConfirmed ||
		statusByLocalID[adopted.ID] != storage.ChannelDeletionKeyRetainedAdopted ||
		statusByLocalID[unknown.ID] != storage.ChannelDeletionKeyRetainedByReview {
		t.Fatalf("mixed ownership statuses = %+v", statusByLocalID)
	}
	manager.mu.Lock()
	deleteCalls := append([]int64(nil), manager.deleteCalls...)
	manager.mu.Unlock()
	if len(deleteCalls) != 1 || deleteCalls[0] != 101 {
		t.Fatalf("remote delete calls = %v, want only created key", deleteCalls)
	}
	var storedIntent storage.ChannelDeletionIntent
	if err := fixture.db.First(&storedIntent, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedIntent.Status != storage.ChannelDeletionIntentFinalized {
		t.Fatalf("mixed ownership intent status = %q", storedIntent.Status)
	}
	if err := fixture.db.First(&storage.Channel{}, fixture.channel.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("automatically resolved channel deletion lookup error = %v", err)
	}
	unknownClaimID := uint64(0)
	for _, claim := range claims {
		if claim.LocalKeyID == unknown.ID {
			unknownClaimID = claim.ID
			break
		}
	}
	var finalAudit storage.ChannelDeletionFinalAudit
	if err := fixture.db.Where("intent_id = ?", intent.ID).First(&finalAudit).Error; err != nil {
		t.Fatalf("load mixed ownership final audit: %v", err)
	}
	if finalAudit.PreparedKeyCount != 3 || finalAudit.DeleteConfirmedCount != 1 ||
		finalAudit.RetainedAdoptedCount != 1 || finalAudit.RetainedByReviewCount != 1 {
		t.Fatalf("mixed ownership final audit = %+v", finalAudit)
	}
	var retainedEvent storage.ChannelDeletionKeyEvidence
	if err := fixture.db.Where("key_claim_id = ? AND event_kind = ?", unknownClaimID, "manual_review_retained").First(&retainedEvent).Error; err != nil {
		t.Fatalf("load automatic retention evidence: %v", err)
	}
	if retainedEvent.Actor != channelDeletionCoordinatorActor || retainedEvent.Reason != "automatic_policy_retained" || retainedEvent.EvidenceDigest == "" {
		t.Fatalf("automatic retention evidence = %+v", retainedEvent)
	}
}

func TestChannelDeletionCoordinatorAutomaticallyRequeuesExistingManagedReview(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-existing-managed-review"
	name := upstreamKeyNameForGroup("existing-review")
	local := fixture.addKey(t, "existing-review", storage.GatewayUpstreamKeyProvenanceCreated, plain, 151, name)
	intent := fixture.prepare(t)
	at := time.Now().UTC()
	if err := fixture.db.Model(&storage.ChannelDeletionKeyClaim{}).
		Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).
		Updates(map[string]any{
			"status": storage.ChannelDeletionKeyManualReviewRequired, "outcome_reason": "remote_contract_changed",
			"outcome_actor": "legacy-coordinator", "outcome_at": at, "last_error": "remote_contract_changed", "updated_at": at,
		}).Error; err != nil {
		t.Fatalf("stage existing managed review: %v", err)
	}
	if err := fixture.db.Model(&storage.ChannelDeletionIntent{}).Where("id = ?", intent.ID).
		Updates(map[string]any{"status": storage.ChannelDeletionIntentManualReviewRequired, "last_error": "remote_contract_changed", "updated_at": at}).Error; err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.Model(&storage.Channel{}).Where("id = ?", fixture.channel.ID).
		Updates(map[string]any{"deletion_state": storage.ChannelDeletionIntentManualReviewRequired, "updated_at": at}).Error; err != nil {
		t.Fatal(err)
	}
	manager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(151, name, "existing-review")},
		revealed:  map[int64]string{151: plain}, removeOnDelete: true,
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)

	first, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || first.Finalized != 0 || first.Failed != 0 {
		t.Fatalf("automatic requeue pass result=%+v err=%v", first, err)
	}
	second, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || second.Finalized != 1 || second.Failed != 0 {
		t.Fatalf("automatic deletion pass result=%+v err=%v", second, err)
	}
	if len(manager.deleteCalls) != 1 || manager.deleteCalls[0] != 151 {
		t.Fatalf("automatic managed review delete calls = %v", manager.deleteCalls)
	}
	var requeueEvidence storage.ChannelDeletionKeyEvidence
	if err := fixture.db.Where("intent_id = ? AND event_kind = ?", intent.ID, "manual_review_requeued").First(&requeueEvidence).Error; err != nil {
		t.Fatalf("load automatic requeue evidence: %v", err)
	}
	if requeueEvidence.Actor != channelDeletionCoordinatorActor || requeueEvidence.Reason != "automatic_policy_revalidation" {
		t.Fatalf("automatic requeue evidence = %+v", requeueEvidence)
	}
}

func TestChannelDeletionCoordinatorDeletesReservedNameWithIncompleteProviderFactsAndChangedID(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	name := "BOG-p-aabbccddeeff0011"
	fixture.addKey(t, "reserved", storage.GatewayUpstreamKeyProvenanceUnknown, "", 171, name)
	fixture.prepare(t)
	manager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{{ID: 172, Name: name}}, removeOnDelete: true,
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)

	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || result.Finalized != 1 || result.Failed != 0 {
		t.Fatalf("reserved-name automatic deletion result=%+v err=%v", result, err)
	}
	if len(manager.deleteCalls) != 1 || manager.deleteCalls[0] != 172 {
		t.Fatalf("reserved-name delete calls = %v", manager.deleteCalls)
	}
}

func TestChannelDeletionCoordinatorRetainsDuplicateReservedNamesWithoutDelete(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	name := "BOG-p-1122334455667788"
	fixture.addKey(t, "duplicate", storage.GatewayUpstreamKeyProvenanceCreated, "", 181, name)
	intent := fixture.prepare(t)
	manager := &channelDeletionTestKeyManager{inventory: []connector.APIKey{{ID: 181, Name: name}, {ID: 182, Name: name}}}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)

	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || result.Finalized != 1 || result.Failed != 0 {
		t.Fatalf("duplicate reserved-name result=%+v err=%v", result, err)
	}
	if len(manager.deleteCalls) != 0 {
		t.Fatalf("duplicate reserved names reached remote delete: %v", manager.deleteCalls)
	}
	var claim storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ?", intent.ID).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.Status != storage.ChannelDeletionKeyRetainedByReview || claim.OutcomeReason != "inventory_read_failed" {
		t.Fatalf("duplicate reserved-name outcome = %+v", claim)
	}
}

func TestChannelDeletionCoordinatorRecoversUnknownDeleteAfterRestart(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-delete-unknown-result"
	name := upstreamKeyNameForGroup("restart")
	local := fixture.addKey(t, "restart", storage.GatewayUpstreamKeyProvenanceCreated, plain, 201, name)
	intent := fixture.prepare(t)
	firstManager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(201, name, "restart")},
		revealed:  map[int64]string{201: plain}, removeOnDelete: true,
		deleteErr: errors.New("transport failed after DELETE may have been sent"),
	}
	firstService := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, firstManager, nil)
	if result, err := firstService.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig()); err != nil || result.Failed != 0 || result.Finalized != 0 {
		t.Fatalf("first unknown-delete reconcile result=%+v err=%v", result, err)
	}
	var uncertain storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&uncertain).Error; err != nil {
		t.Fatal(err)
	}
	if uncertain.Status != storage.ChannelDeletionKeyProcessingUnknown || uncertain.ClaimEpoch != 1 {
		t.Fatalf("uncertain delete claim = %+v", uncertain)
	}

	// The first pass intentionally schedules retry with backoff. Simulate the
	// next startup after that retry becomes due instead of racing the clock.
	if err := fixture.db.Model(&storage.ChannelDeletionIntent{}).
		Where("id = ?", intent.ID).
		Update("next_attempt_at", time.Now().UTC().Add(-time.Second)).Error; err != nil {
		t.Fatalf("make restart deletion retry due: %v", err)
	}
	restartedManager := &channelDeletionTestKeyManager{revealed: make(map[int64]string)}
	restartedService := NewService(storage.NewChannels(fixture.db), storage.NewRates(fixture.db), storage.NewGateway(fixture.db), fixture.cipher, restartedManager, nil)
	result, err := restartedService.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil {
		t.Fatalf("restart reconciliation: %v", err)
	}
	if result.Finalized != 1 || result.Failed != 0 {
		t.Fatalf("restart reconcile result = %+v", result)
	}
	var confirmed storage.ChannelDeletionKeyClaim
	if err := fixture.db.First(&confirmed, uncertain.ID).Error; err != nil {
		t.Fatalf("load surviving confirmed claim: %v", err)
	}
	if confirmed.Status != storage.ChannelDeletionKeyDeleteConfirmed || confirmed.ClaimEpoch != 2 || confirmed.OutcomeReason != "remote_absence_confirmed_after_recovery" {
		t.Fatalf("recovered claim = %+v", confirmed)
	}
	var evidenceCount int64
	if err := fixture.db.Model(&storage.ChannelDeletionKeyEvidence{}).Where("key_claim_id = ?", confirmed.ID).Count(&evidenceCount).Error; err != nil {
		t.Fatal(err)
	}
	if evidenceCount < 5 {
		t.Fatalf("surviving recovery evidence count = %d", evidenceCount)
	}
	var finalAudit storage.ChannelDeletionFinalAudit
	if err := fixture.db.Where("intent_id = ?", intent.ID).First(&finalAudit).Error; err != nil {
		t.Fatalf("load surviving recovery final audit: %v", err)
	}
	if finalAudit.PreparedKeyCount != 1 || finalAudit.DeleteConfirmedCount != 1 || finalAudit.KeyOutcomeDigest == "" {
		t.Fatalf("recovery final audit = %+v", finalAudit)
	}
}

func TestChannelDeletionKeyLeaseExpiresAndCanBeTakenOver(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-delete-lease"
	name := upstreamKeyNameForGroup("lease")
	local := fixture.addKey(t, "lease", storage.GatewayUpstreamKeyProvenanceCreated, plain, 301, name)
	intent := fixture.prepare(t)
	var prepared storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&prepared).Error; err != nil {
		t.Fatal(err)
	}
	fence := storage.ChannelDeletionFence{
		ChannelID: fixture.channel.ID, IntentID: intent.ID, DeletionEpoch: intent.DeletionEpoch,
		IdentityRevision: intent.ChannelIdentityRevision,
	}
	first, acquired, err := fixture.channels.ClaimChannelDeletionKey(context.Background(), fence, prepared.ID, 75*time.Millisecond)
	if err != nil || !acquired {
		t.Fatalf("first lease acquire claim=%+v acquired=%t err=%v", first, acquired, err)
	}
	secondChannels := storage.NewChannels(fixture.db.Session(&gorm.Session{NewDB: true}))
	current, acquired, err := secondChannels.ClaimChannelDeletionKey(context.Background(), fence, prepared.ID, time.Second)
	if err != nil || acquired || current.ClaimEpoch != first.ClaimEpoch {
		t.Fatalf("live lease takeover claim=%+v acquired=%t err=%v", current, acquired, err)
	}
	wait := time.Until(*first.LeaseExpiresAt) + 40*time.Millisecond
	if wait > 0 {
		time.Sleep(wait)
	}
	taken, acquired, err := secondChannels.ClaimChannelDeletionKey(context.Background(), fence, prepared.ID, time.Second)
	if err != nil || !acquired {
		t.Fatalf("expired lease takeover claim=%+v acquired=%t err=%v", taken, acquired, err)
	}
	if taken.ClaimEpoch != first.ClaimEpoch+1 || taken.ClaimedAt == nil || taken.LeaseExpiresAt == nil {
		t.Fatalf("taken-over lease = %+v", taken)
	}
}

func TestChannelDeletionErrorClassificationFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		wantRetry bool
	}{
		{name: "unknown deterministic", err: errors.New("decode contract failed")},
		{name: "invalid inventory", err: invalidChannelDeletionInventory("drift")},
		{name: "http 400", err: connector.HTTPStatusError(http.StatusBadRequest, nil)},
		{name: "http 408", err: connector.HTTPStatusError(http.StatusRequestTimeout, nil), wantRetry: true},
		{name: "http 425", err: connector.HTTPStatusError(http.StatusTooEarly, nil), wantRetry: true},
		{name: "http 429", err: connector.HTTPStatusError(http.StatusTooManyRequests, nil), wantRetry: true},
		{name: "http 500", err: connector.HTTPStatusError(http.StatusInternalServerError, nil), wantRetry: true},
		{name: "deadline", err: context.DeadlineExceeded, wantRetry: true},
		{name: "temporary network", err: &net.DNSError{Err: "temporary", IsTemporary: true}, wantRetry: true},
		{name: "permanent network", err: &net.DNSError{Err: "certificate verify failed"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := channelDeletionErrorIsRetryable(test.err); got != test.wantRetry {
				t.Fatalf("retryable = %t, want %t for %v", got, test.wantRetry, test.err)
			}
		})
	}
}

func TestChannelDeletionCoordinatorNeverReplaysUnknownDeleteWhileRemoteKeyRemains(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-delete-unknown-still-present"
	name := upstreamKeyNameForGroup("unknown-still-present")
	local := fixture.addKey(t, "unknown-still-present", storage.GatewayUpstreamKeyProvenanceCreated, plain, 202, name)
	intent := fixture.prepare(t)
	firstManager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(202, name, "unknown-still-present")},
		revealed:  map[int64]string{202: plain},
		deleteErr: errors.New("transport failed after DELETE may have been sent"),
	}
	firstService := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, firstManager, nil)
	if result, err := firstService.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig()); err != nil || result.Failed != 0 || result.Finalized != 0 {
		t.Fatalf("first unknown-delete reconcile result=%+v err=%v", result, err)
	}
	if len(firstManager.deleteCalls) != 1 || firstManager.deleteCalls[0] != 202 {
		t.Fatalf("first unknown-delete calls = %v", firstManager.deleteCalls)
	}
	var uncertain storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&uncertain).Error; err != nil {
		t.Fatal(err)
	}
	if uncertain.Status != storage.ChannelDeletionKeyProcessingUnknown || uncertain.ClaimEpoch != 1 {
		t.Fatalf("first uncertain claim = %+v", uncertain)
	}
	past := time.Now().UTC().Add(-time.Second)
	if err := fixture.db.Model(&storage.ChannelDeletionIntent{}).Where("id = ?", intent.ID).Update("next_attempt_at", past).Error; err != nil {
		t.Fatal(err)
	}

	revealCalls := 0
	restartedManager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(202, name, "unknown-still-present")},
		revealHook: func(context.Context, int64) (string, error) {
			revealCalls++
			return "", errors.New("unknown-delete recovery must not reveal or reverify")
		},
	}
	restartedService := NewService(storage.NewChannels(fixture.db), storage.NewRates(fixture.db), storage.NewGateway(fixture.db), fixture.cipher, restartedManager, nil)
	result, err := restartedService.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || result.IntentsScanned != 1 || result.Finalized != 0 || result.Failed != 0 {
		t.Fatalf("present unknown-delete recovery result=%+v err=%v", result, err)
	}
	if revealCalls != 0 || len(restartedManager.deleteCalls) != 0 {
		t.Fatalf("present unknown-delete recovery made reveal=%d delete=%v", revealCalls, restartedManager.deleteCalls)
	}
	var stillUncertain storage.ChannelDeletionKeyClaim
	if err := fixture.db.First(&stillUncertain, uncertain.ID).Error; err != nil {
		t.Fatal(err)
	}
	if stillUncertain.Status != storage.ChannelDeletionKeyProcessingUnknown || stillUncertain.ClaimEpoch != 2 ||
		stillUncertain.OutcomeReason != "remote_delete_still_present_after_recovery" {
		t.Fatalf("recovered present claim = %+v", stillUncertain)
	}
	var pending storage.ChannelDeletionIntent
	if err := fixture.db.First(&pending, intent.ID).Error; err != nil {
		t.Fatal(err)
	}
	if pending.NextAttemptAt == nil || !pending.NextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf("unknown-delete retry was not durably backed off: %+v", pending)
	}
	result, err = restartedService.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || result.IntentsScanned != 0 || len(restartedManager.deleteCalls) != 0 {
		t.Fatalf("backed-off unknown-delete reconcile result=%+v deletes=%v err=%v", result, restartedManager.deleteCalls, err)
	}
}

func TestChannelDeletionCoordinatorPersistsUnknownWhenRecoveryInventoryIsCanceled(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-delete-recovery-canceled"
	name := upstreamKeyNameForGroup("recovery-canceled")
	local := fixture.addKey(t, "recovery-canceled", storage.GatewayUpstreamKeyProvenanceCreated, plain, 221, name)
	intent := fixture.prepare(t)
	firstManager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(221, name, "recovery-canceled")},
		revealed:  map[int64]string{221: plain},
		deleteErr: errors.New("DELETE outcome is unknown"),
	}
	firstService := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, firstManager, nil)
	if result, err := firstService.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig()); err != nil || result.Failed != 0 {
		t.Fatalf("seed unknown delete result=%+v err=%v", result, err)
	}
	if err := fixture.db.Model(&storage.ChannelDeletionIntent{}).Where("id = ?", intent.ID).
		Update("next_attempt_at", time.Now().UTC().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}

	recoveryCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := &channelDeletionTestKeyManager{
		inventoryPageHook: func(context.Context, connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			cancel()
			return nil, context.Canceled
		},
	}
	service := NewService(storage.NewChannels(fixture.db), storage.NewRates(fixture.db), storage.NewGateway(fixture.db), fixture.cipher, manager, nil)
	result, err := service.ReconcileChannelDeletions(recoveryCtx, DefaultChannelDeletionCoordinatorConfig())
	if !errors.Is(err, context.Canceled) || result.Failed != 1 {
		t.Fatalf("canceled recovery result=%+v err=%v", result, err)
	}
	var uncertain storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&uncertain).Error; err != nil {
		t.Fatal(err)
	}
	if uncertain.Status != storage.ChannelDeletionKeyProcessingUnknown || uncertain.ClaimEpoch != 2 {
		t.Fatalf("canceled recovery claim = %+v", uncertain)
	}
}

func TestChannelDeletionCoordinatorPersistsUnknownWhenDeleteReturnsAfterCancellation(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-delete-callback-canceled"
	name := upstreamKeyNameForGroup("callback-canceled")
	local := fixture.addKey(t, "callback-canceled", storage.GatewayUpstreamKeyProvenanceCreated, plain, 231, name)
	intent := fixture.prepare(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deleteAttempted := false
	manager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(231, name, "callback-canceled")},
		revealed:  map[int64]string{231: plain},
		deleteHook: func(context.Context, int64) error {
			deleteAttempted = true
			cancel()
			return errors.New("DELETE transport result is unknown")
		},
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	result, err := service.ReconcileChannelDeletions(ctx, DefaultChannelDeletionCoordinatorConfig())
	if !errors.Is(err, context.Canceled) || result.Failed != 1 || !deleteAttempted {
		t.Fatalf("canceled delete callback result=%+v attempted=%t err=%v", result, deleteAttempted, err)
	}
	var uncertain storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&uncertain).Error; err != nil {
		t.Fatal(err)
	}
	if uncertain.Status != storage.ChannelDeletionKeyProcessingUnknown || uncertain.ClaimEpoch != 1 {
		t.Fatalf("canceled delete callback claim = %+v", uncertain)
	}
}

func TestChannelDeletionCoordinatorPersistsUnknownWhenPostDeleteInventoryIsCanceled(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-delete-post-inventory-canceled"
	name := upstreamKeyNameForGroup("post-inventory-canceled")
	local := fixture.addKey(t, "post-inventory-canceled", storage.GatewayUpstreamKeyProvenanceCreated, plain, 241, name)
	intent := fixture.prepare(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := &channelDeletionTestKeyManager{
		inventory:      []connector.APIKey{channelDeletionTestRemote(241, name, "post-inventory-canceled")},
		revealed:       map[int64]string{241: plain},
		removeOnDelete: true,
		deleteHook: func(context.Context, int64) error {
			cancel()
			return nil
		},
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	result, err := service.ReconcileChannelDeletions(ctx, DefaultChannelDeletionCoordinatorConfig())
	if !errors.Is(err, context.Canceled) || result.Failed != 1 {
		t.Fatalf("canceled post-delete inventory result=%+v err=%v", result, err)
	}
	var uncertain storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&uncertain).Error; err != nil {
		t.Fatal(err)
	}
	if uncertain.Status != storage.ChannelDeletionKeyProcessingUnknown || uncertain.ClaimEpoch != 1 {
		t.Fatalf("canceled post-delete inventory claim = %+v", uncertain)
	}
	manager.mu.Lock()
	deleteCalls := append([]int64(nil), manager.deleteCalls...)
	remaining := append([]connector.APIKey(nil), manager.inventory...)
	manager.mu.Unlock()
	if len(deleteCalls) != 1 || len(remaining) != 0 {
		t.Fatalf("post-delete cancellation remote state calls=%v remaining=%v", deleteCalls, remaining)
	}

	if err := fixture.db.Model(&storage.ChannelDeletionIntent{}).Where("id = ?", intent.ID).
		Update("next_attempt_at", time.Now().UTC().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	recoveryManager := &channelDeletionTestKeyManager{}
	recoveryService := NewService(storage.NewChannels(fixture.db), storage.NewRates(fixture.db), storage.NewGateway(fixture.db), fixture.cipher, recoveryManager, nil)
	result, err = recoveryService.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || result.Finalized != 1 || result.Failed != 0 || len(recoveryManager.deleteCalls) != 0 {
		t.Fatalf("post-delete cancellation recovery result=%+v deletes=%v err=%v", result, recoveryManager.deleteCalls, err)
	}
}

func TestChannelDeletionCoordinatorFinalizesLegacyGrouplessClaimAfterAutomaticRetention(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	local := storage.GatewayUpstreamKey{
		ChannelID: fixture.channel.ID, UpstreamKeyID: 203, UpstreamKeyName: "legacy-groupless-key",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceUnknown, Enabled: true,
	}
	if err := fixture.db.Create(&local).Error; err != nil {
		t.Fatal(err)
	}
	intent := fixture.prepare(t)
	var claim storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.Status != storage.ChannelDeletionKeyManualReviewRequired || claim.UpstreamGroupID != nil ||
		claim.PresenceGeneration != nil || claim.CanonicalGroupName != "" {
		t.Fatalf("legacy groupless prepared claim = %+v", claim)
	}
	inventoryCalls := 0
	manager := &channelDeletionTestKeyManager{inventoryPageHook: func(context.Context, connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		inventoryCalls++
		return nil, errors.New("retained legacy claim must not read remote inventory")
	}}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	if err := service.resolveChannelDeletionManualClaimByPolicy(context.Background(), channelDeletionFence(intent), claim); err != nil {
		t.Fatalf("automatically retain legacy groupless claim: %v", err)
	}
	var retained storage.ChannelDeletionKeyClaim
	if err := fixture.db.First(&retained, claim.ID).Error; err != nil {
		t.Fatal(err)
	}
	if retained.Status != storage.ChannelDeletionKeyRetainedByReview || retained.OutcomeReason != "automatic_policy_retained" {
		t.Fatalf("retained legacy groupless claim = %+v", retained)
	}

	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || result.Finalized != 1 || result.Failed != 0 || inventoryCalls != 0 {
		t.Fatalf("legacy groupless restart result=%+v inventory_calls=%d err=%v", result, inventoryCalls, err)
	}
}

func TestLocateChannelDeletionReservedNameUsesUniqueNameWhileLegacyIdentityStaysBound(t *testing.T) {
	reserved := storage.ChannelDeletionKeyClaim{UpstreamKeyID: 41, UpstreamKeyName: "BOG-p-fedcba9876543210"}
	legacy := storage.ChannelDeletionKeyClaim{UpstreamKeyID: 41, UpstreamKeyName: "BOG - legacy"}
	inventory := channelDeletionInventory{Keys: []connector.APIKey{
		{ID: 42, Name: reserved.UpstreamKeyName},
		{ID: 43, Name: legacy.UpstreamKeyName},
	}}
	reservedLocated := locateChannelDeletionKey(inventory, &reserved)
	if reservedLocated.Remote == nil || reservedLocated.Remote.ID != 42 || reservedLocated.Reason != "" {
		t.Fatalf("reserved name location = %+v", reservedLocated)
	}
	legacyLocated := locateChannelDeletionKey(inventory, &legacy)
	if legacyLocated.Remote != nil || legacyLocated.Reason != "remote_name_reused" {
		t.Fatalf("legacy name location = %+v", legacyLocated)
	}
}

func TestChannelDeletionLegacyCreatedKeyUsesExactIdentityAndSecretWithoutProviderFingerprint(t *testing.T) {
	const secret = "sk-legacy-owned-secret"
	fence := storage.ChannelDeletionFence{ChannelID: 7, IntentID: 8, DeletionEpoch: 9, IdentityRevision: 10}
	claim := &storage.ChannelDeletionKeyClaim{
		UpstreamKeyID: 51, UpstreamKeyName: "BOG - legacy-owned",
		OwnershipProvenance: storage.GatewayUpstreamKeyProvenanceCreated,
		LocalKey:            &storage.GatewayUpstreamKey{GroupName: "legacy"},
	}
	remote := connector.APIKey{ID: 51, Name: claim.UpstreamKeyName}
	manager := &channelDeletionTestKeyManager{revealed: map[int64]string{51: secret}}
	fingerprint, revealedHash, reason, err := (&Service{}).verifyChannelDeletionRemoteKey(
		context.Background(), manager, fence, storage.ChannelTypeNewAPI, claim, remote, HashKey(secret),
	)
	if err != nil || reason != "" || fingerprint == "" || revealedHash != HashKey(secret) {
		t.Fatalf("legacy exact-identity verification fingerprint=%q secret_hash=%q reason=%q err=%v", fingerprint, revealedHash, reason, err)
	}
}

func TestChannelDeletionCoordinatorCommitsFirstRevealSecretDrift(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	localSecret := "sk-channel-delete-local-secret"
	remoteSecret := "sk-channel-delete-drifted-secret"
	name := upstreamKeyNameForGroup("first-secret-drift")
	local := fixture.addKey(t, "first-secret-drift", storage.GatewayUpstreamKeyProvenanceCreated, localSecret, 701, name)
	intent := fixture.prepare(t)
	manager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(701, name, "first-secret-drift")},
		revealed:  map[int64]string{701: remoteSecret},
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	if result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig()); err != nil || result.Failed != 0 || result.Finalized != 1 {
		t.Fatalf("first secret-drift reconcile result=%+v err=%v", result, err)
	}

	var claim storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	claim.LocalKey = &local
	wantOwnership, err := channelDeletionOwnershipDigestWithRemote(
		claim, claim.LocalOwnershipClaim, HashKey(localSecret), 701, HashKey(remoteSecret),
	)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Status != storage.ChannelDeletionKeyRetainedByReview || claim.OutcomeReason != "remote_secret_changed" ||
		claim.ObservedRemoteID != 701 || claim.OwnershipDigest != wantOwnership {
		t.Fatalf("first secret-drift claim = %+v, want observed target and drift commitment", claim)
	}
	var evidence storage.ChannelDeletionKeyEvidence
	if err := fixture.db.Where("key_claim_id = ? AND event_kind = ?", claim.ID, "outcome_recorded").Order("id DESC").First(&evidence).Error; err != nil {
		t.Fatal(err)
	}
	if evidence.ObservedRemoteID != 701 || evidence.OwnershipDigest != wantOwnership {
		t.Fatalf("first secret-drift evidence = %+v", evidence)
	}
	if len(manager.deleteCalls) != 0 {
		t.Fatalf("first secret-drift delete calls = %v", manager.deleteCalls)
	}
}

func TestChannelDeletionCoordinatorCommitsSecondRevealSecretDrift(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	localSecret := "sk-channel-delete-revalidation-local"
	driftedSecret := "sk-channel-delete-revalidation-drift"
	name := upstreamKeyNameForGroup("second-secret-drift")
	local := fixture.addKey(t, "second-secret-drift", storage.GatewayUpstreamKeyProvenanceCreated, localSecret, 702, name)
	intent := fixture.prepare(t)
	revealCount := 0
	manager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(702, name, "second-secret-drift")},
		revealHook: func(ctx context.Context, id int64) (string, error) {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			if id != 702 {
				return "", errors.New("unexpected remote key id")
			}
			revealCount++
			if revealCount == 1 {
				return localSecret, nil
			}
			return driftedSecret, nil
		},
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	if result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig()); err != nil || result.Failed != 0 || result.Finalized != 1 {
		t.Fatalf("second secret-drift reconcile result=%+v err=%v", result, err)
	}

	var claim storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	claim.LocalKey = &local
	wantOwnership, err := channelDeletionOwnershipDigestWithRemote(
		claim, claim.LocalOwnershipClaim, HashKey(localSecret), 702, HashKey(driftedSecret),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstOwnership, err := channelDeletionOwnershipDigestWithRemote(
		claim, claim.LocalOwnershipClaim, HashKey(localSecret), 702, HashKey(localSecret),
	)
	if err != nil {
		t.Fatal(err)
	}
	if revealCount != 2 || claim.Status != storage.ChannelDeletionKeyRetainedByReview || claim.OutcomeReason != "remote_facts_changed" ||
		claim.ObservedRemoteID != 702 || claim.OwnershipDigest != wantOwnership || claim.OwnershipDigest == firstOwnership {
		t.Fatalf("second secret-drift claim = %+v reveal_count=%d", claim, revealCount)
	}
	var evidence storage.ChannelDeletionKeyEvidence
	if err := fixture.db.Where("key_claim_id = ? AND event_kind = ?", claim.ID, "outcome_recorded").Order("id DESC").First(&evidence).Error; err != nil {
		t.Fatal(err)
	}
	if evidence.ObservedRemoteID != 702 || evidence.OwnershipDigest != wantOwnership {
		t.Fatalf("second secret-drift evidence = %+v", evidence)
	}
	if len(manager.deleteCalls) != 0 {
		t.Fatalf("second secret-drift delete calls = %v", manager.deleteCalls)
	}
}

func TestChannelDeletionProvisioningClaimBindsFirstRemoteIDAcrossRetry(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	name := "BOG-p-0123456789abcdef"
	local := fixture.addKey(t, "provisioning-retry", storage.GatewayUpstreamKeyProvenanceProvisioningClaim, "", 0, name)
	startedAt := time.Now().UTC()
	if err := fixture.db.Model(&storage.GatewayUpstreamKey{}).Where("id = ?", local.ID).Updates(map[string]any{
		"provisioning": true, "provisioning_started_at": startedAt,
	}).Error; err != nil {
		t.Fatalf("stage provisioning claim: %v", err)
	}
	intent := fixture.prepare(t)
	inventories := 0
	firstManager := &channelDeletionTestKeyManager{inventory: []connector.APIKey{channelDeletionTestRemote(801, name, "provisioning-retry")}}
	firstManager.inventoryPageHook = func(ctx context.Context, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
		inventories++
		if inventories == 2 {
			return nil, &net.DNSError{Err: "temporary inventory failure", IsTemporary: true}
		}
		return (&channelDeletionTestKeyManager{inventory: firstManager.inventory}).inventoryPage(ctx, query)
	}
	firstService := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, firstManager, nil)
	if finalized, err := firstService.reconcileChannelDeletionIntent(
		context.Background(), firstManager, intent, DefaultChannelDeletionCoordinatorConfig(),
	); err == nil || finalized || !strings.Contains(err.Error(), "retryable channel deletion key failure") {
		t.Fatalf("first provisioning retry finalized=%t err=%v", finalized, err)
	}

	var retrying storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&retrying).Error; err != nil {
		t.Fatal(err)
	}
	if retrying.Status != storage.ChannelDeletionKeyPendingVerification || retrying.ObservedRemoteID != 801 {
		t.Fatalf("retrying provisioning claim = %+v", retrying)
	}
	var boundEvents int64
	if err := fixture.db.Model(&storage.ChannelDeletionKeyEvidence{}).
		Where("key_claim_id = ? AND event_kind = ? AND observed_remote_id = ?", retrying.ID, "remote_identity_bound", 801).
		Count(&boundEvents).Error; err != nil {
		t.Fatal(err)
	}
	if boundEvents != 1 {
		t.Fatalf("remote identity binding events = %d, want 1", boundEvents)
	}

	replacementManager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(802, name, "provisioning-retry")},
		revealed:  map[int64]string{802: "sk-same-name-replacement"}, removeOnDelete: true,
	}
	replacementService := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, replacementManager, nil)
	if finalized, err := replacementService.reconcileChannelDeletionIntent(
		context.Background(), replacementManager, intent, DefaultChannelDeletionCoordinatorConfig(),
	); err != nil || !finalized {
		t.Fatalf("replacement provisioning reconcile finalized=%t err=%v", finalized, err)
	}
	var reviewed storage.ChannelDeletionKeyClaim
	if err := fixture.db.First(&reviewed, retrying.ID).Error; err != nil {
		t.Fatal(err)
	}
	if reviewed.Status != storage.ChannelDeletionKeyRetainedByReview || reviewed.OutcomeReason != "remote_name_reused" || reviewed.ObservedRemoteID != 801 {
		t.Fatalf("same-name replacement claim = %+v", reviewed)
	}
	if len(replacementManager.deleteCalls) != 0 {
		t.Fatalf("same-name replacement reached remote delete: %v", replacementManager.deleteCalls)
	}
}

func TestChannelDeletionProvisioningClaimPersistsObservedDeleteTarget(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	name := "BOG-p-fedcba9876543210"
	local := fixture.addKey(t, "provisioning-delete", storage.GatewayUpstreamKeyProvenanceProvisioningClaim, "", 0, name)
	startedAt := time.Now().UTC()
	if err := fixture.db.Model(&storage.GatewayUpstreamKey{}).Where("id = ?", local.ID).Updates(map[string]any{
		"provisioning": true, "provisioning_started_at": startedAt,
	}).Error; err != nil {
		t.Fatalf("stage provisioning claim: %v", err)
	}
	intent := fixture.prepare(t)
	manager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(803, name, "provisioning-delete")},
		revealed:  map[int64]string{803: "sk-provisioned-remote"}, removeOnDelete: true,
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || result.Finalized != 1 || result.Failed != 0 {
		t.Fatalf("provisioning deletion result=%+v err=%v", result, err)
	}
	var claim storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.Status != storage.ChannelDeletionKeyDeleteConfirmed || claim.UpstreamKeyID != 0 || claim.ObservedRemoteID != 803 {
		t.Fatalf("provisioning deletion claim = %+v", claim)
	}
	var evidence storage.ChannelDeletionKeyEvidence
	if err := fixture.db.Where("key_claim_id = ? AND event_kind = ?", claim.ID, "outcome_recorded").Order("id DESC").First(&evidence).Error; err != nil {
		t.Fatal(err)
	}
	if evidence.ObservedRemoteID != 803 {
		t.Fatalf("provisioning deletion evidence = %+v", evidence)
	}
	var finalAudit storage.ChannelDeletionFinalAudit
	if err := fixture.db.Where("intent_id = ?", intent.ID).First(&finalAudit).Error; err != nil {
		t.Fatal(err)
	}
	if finalAudit.DeleteConfirmedCount != 1 || finalAudit.KeyOutcomeDigestVersion != 2 {
		t.Fatalf("provisioning final audit = %+v", finalAudit)
	}
}

func TestChannelDeletionCoordinatorConfirmsStableInitialAbsence(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-deletion-initially-absent"
	name := upstreamKeyNameForGroup("initially-absent")
	local := fixture.addKey(t, "initially-absent", storage.GatewayUpstreamKeyProvenanceCreated, plain, 804, name)
	intent := fixture.prepare(t)
	manager := &channelDeletionTestKeyManager{revealed: make(map[int64]string)}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if err != nil || result.Finalized != 1 || result.Failed != 0 {
		t.Fatalf("initially absent deletion result=%+v err=%v", result, err)
	}
	var claim storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.Status != storage.ChannelDeletionKeyInitiallyAbsentConfirmed || claim.ObservedRemoteID != 0 {
		t.Fatalf("initially absent claim = %+v", claim)
	}
	var finalAudit storage.ChannelDeletionFinalAudit
	if err := fixture.db.Where("intent_id = ?", intent.ID).First(&finalAudit).Error; err != nil {
		t.Fatal(err)
	}
	if finalAudit.InitiallyAbsentCount != 1 {
		t.Fatalf("initially absent final audit = %+v", finalAudit)
	}
}

func TestChannelDeletionCoordinatorRejectsAccountIdentityDriftBeforeRemoteRead(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-deletion-account-drift"
	name := upstreamKeyNameForGroup("account-drift")
	local := fixture.addKey(t, "account-drift", storage.GatewayUpstreamKeyProvenanceCreated, plain, 805, name)
	intent := fixture.prepare(t)
	nextRevision := intent.ChannelIdentityRevision + 1
	if err := fixture.db.Model(&storage.Channel{}).Where("id = ?", fixture.channel.ID).Updates(map[string]any{
		"upstream_account_identity_revision": nextRevision,
		"deletion_identity_revision":         nextRevision,
	}).Error; err != nil {
		t.Fatalf("stage account identity drift: %v", err)
	}
	inventoryCalls := 0
	manager := &channelDeletionTestKeyManager{
		inventoryPageHook: func(context.Context, connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			inventoryCalls++
			return nil, errors.New("remote inventory must not be read after identity drift")
		},
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)
	result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
	if !errors.Is(err, storage.ErrStaleChannelDeletionFence) || result.Failed != 1 || result.Finalized != 0 {
		t.Fatalf("account identity drift result=%+v err=%v", result, err)
	}
	if inventoryCalls != 0 || len(manager.deleteCalls) != 0 {
		t.Fatalf("account identity drift reached remote inventory=%d delete=%v", inventoryCalls, manager.deleteCalls)
	}
	var claim storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&claim).Error; err != nil {
		t.Fatal(err)
	}
	if claim.Status != storage.ChannelDeletionKeyPendingVerification || claim.ClaimEpoch != 0 {
		t.Fatalf("account identity drift mutated claim = %+v", claim)
	}
}

func TestChannelDeletionInventoryRejectsDuplicateRemoteIdentityAcrossPages(t *testing.T) {
	for _, test := range []struct {
		name       string
		first      connector.APIKey
		second     connector.APIKey
		wantReason string
	}{
		{
			name:       "duplicate id",
			first:      channelDeletionTestRemote(401, "first-name", "duplicate-id"),
			second:     channelDeletionTestRemote(401, "second-name", "duplicate-id"),
			wantReason: "api key id 401 is ambiguous",
		},
		{
			name:       "duplicate name",
			first:      channelDeletionTestRemote(402, "reused-name", "duplicate-name"),
			second:     channelDeletionTestRemote(403, "reused-name", "duplicate-name"),
			wantReason: `api key name "reused-name" is ambiguous`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			manager := &channelDeletionTestKeyManager{
				inventoryPageHook: func(ctx context.Context, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
					calls++
					item := test.first
					if query.Page == 2 {
						item = test.second
					}
					return &connector.APIKeyPage{
						Items: []connector.APIKey{item}, Total: 2,
						Page: query.Page, PageSize: 1, Pages: 2,
					}, nil
				},
			}
			_, err := (&Service{}).listChannelDeletionInventory(
				context.Background(), manager,
				storage.ChannelDeletionFence{ChannelID: 1, IntentID: 1, DeletionEpoch: 1, IdentityRevision: 1},
				ChannelDeletionCoordinatorConfig{InventoryTimeout: time.Second, InventoryPageSize: 1, InventoryMaxPages: 3},
			)
			if !errors.Is(err, errChannelDeletionInventoryInvalid) || !strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("duplicate inventory error = %v, want reason containing %q", err, test.wantReason)
			}
			if calls != 2 {
				t.Fatalf("inventory page calls = %d, want 2", calls)
			}
		})
	}
}

func TestChannelDeletionInventoryRejectsPaginationDriftAndSafePageLimit(t *testing.T) {
	fence := storage.ChannelDeletionFence{ChannelID: 1, IntentID: 1, DeletionEpoch: 1, IdentityRevision: 1}

	t.Run("pagination metadata drift", func(t *testing.T) {
		manager := &channelDeletionTestKeyManager{
			inventoryPageHook: func(ctx context.Context, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				total, pages := int64(2), 2
				if query.Page == 2 {
					total, pages = 3, 3
				}
				return &connector.APIKeyPage{
					Items: []connector.APIKey{channelDeletionTestRemote(int64(410+query.Page), "drift", "drift")},
					Total: total, Page: query.Page, PageSize: 1, Pages: pages,
				}, nil
			},
		}
		_, err := (&Service{}).listChannelDeletionInventory(
			context.Background(), manager, fence,
			ChannelDeletionCoordinatorConfig{InventoryTimeout: time.Second, InventoryPageSize: 1, InventoryMaxPages: 4},
		)
		if !errors.Is(err, errChannelDeletionInventoryInvalid) || !strings.Contains(err.Error(), "pagination changed during scan") {
			t.Fatalf("pagination drift error = %v", err)
		}
	})

	t.Run("safe page limit", func(t *testing.T) {
		calls := 0
		manager := &channelDeletionTestKeyManager{
			inventoryPageHook: func(ctx context.Context, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				calls++
				name := "unbounded-first"
				if query.Page == 2 {
					name = "unbounded-second"
				}
				return &connector.APIKeyPage{
					Items: []connector.APIKey{channelDeletionTestRemote(int64(420+query.Page), name, "limit")},
					Page:  query.Page, PageSize: 1,
				}, nil
			},
		}
		_, err := (&Service{}).listChannelDeletionInventory(
			context.Background(), manager, fence,
			ChannelDeletionCoordinatorConfig{InventoryTimeout: time.Second, InventoryPageSize: 1, InventoryMaxPages: 2},
		)
		if !errors.Is(err, errChannelDeletionInventoryInvalid) || !strings.Contains(err.Error(), "scan exceeds the safe page limit") {
			t.Fatalf("safe page limit error = %v", err)
		}
		if calls != 2 {
			t.Fatalf("inventory page calls = %d, want bounded maximum 2", calls)
		}
	})
}

func TestChannelDeletionInventoryHonorsWholeScanTimeout(t *testing.T) {
	deadlines := make([]time.Time, 0, 2)
	calls := 0
	manager := &channelDeletionTestKeyManager{
		inventoryPageHook: func(ctx context.Context, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
			calls++
			deadline, ok := ctx.Deadline()
			if !ok {
				return nil, errors.New("inventory page context has no deadline")
			}
			deadlines = append(deadlines, deadline)
			if query.Page == 1 {
				return &connector.APIKeyPage{
					Items: []connector.APIKey{channelDeletionTestRemote(431, "timeout-first", "timeout")},
					Total: 2, Page: 1, PageSize: 1, Pages: 2,
				}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	started := time.Now()
	_, err := (&Service{}).listChannelDeletionInventory(
		context.Background(), manager,
		storage.ChannelDeletionFence{ChannelID: 1, IntentID: 1, DeletionEpoch: 1, IdentityRevision: 1},
		ChannelDeletionCoordinatorConfig{InventoryTimeout: 25 * time.Millisecond, InventoryPageSize: 1, InventoryMaxPages: 2},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("inventory timeout error = %v, want context deadline exceeded", err)
	}
	if calls != 2 || len(deadlines) != 2 || !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("inventory scan deadlines = %v across %d calls, want one shared two-page deadline", deadlines, calls)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("inventory timeout took %s, want a bounded whole-scan deadline", elapsed)
	}
}

func TestChannelDeletionRemoteVerificationRejectsSourceMismatchAndInvalidSub2APIGroupID(t *testing.T) {
	fence := storage.ChannelDeletionFence{ChannelID: 1, IntentID: 1, DeletionEpoch: 1, IdentityRevision: 1}
	groupName := "remote-identity"
	keyName := upstreamKeyNameForGroup(groupName)
	claim := &storage.ChannelDeletionKeyClaim{
		UpstreamKeyID: 501, UpstreamKeyName: keyName,
		LocalKey: &storage.GatewayUpstreamKey{GroupName: groupName},
	}
	manager := &channelDeletionTestKeyManager{revealed: make(map[int64]string)}

	t.Run("source mismatch", func(t *testing.T) {
		remote := channelDeletionTestRemote(501, keyName, groupName)
		remote.Source = connector.APIKeySourceSub2API
		_, _, reason, err := (&Service{}).verifyChannelDeletionRemoteKey(
			context.Background(), manager, fence, storage.ChannelTypeNewAPI, claim, remote, "",
		)
		if err != nil || reason != "remote_identity_changed" {
			t.Fatalf("source mismatch reason=%q err=%v", reason, err)
		}
	})

	for _, test := range []struct {
		name    string
		groupID int64
	}{
		{name: "zero Sub2API group id", groupID: 0},
		{name: "negative Sub2API group id", groupID: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			groupID := test.groupID
			remote := connector.APIKey{
				ID: 501, Name: keyName, Status: "active", GroupName: groupName,
				GroupID: &groupID, Source: connector.APIKeySourceSub2API,
				ObservedFields: connector.APIKeyObservedFields{
					Quota: true, ExpiresAt: true, IPWhitelist: true, IPBlacklist: true,
					RateLimit5h: true, RateLimit1d: true, RateLimit7d: true,
				},
			}
			_, _, reason, err := (&Service{}).verifyChannelDeletionRemoteKey(
				context.Background(), manager, fence, storage.ChannelTypeSub2API, claim, remote, "",
			)
			if err != nil || reason != "sub2api_group_identity_invalid" {
				t.Fatalf("Sub2API group id %d reason=%q err=%v", test.groupID, reason, err)
			}
		})
	}
}

func TestChannelDeletionCoordinatorStopsBeforeRemoteDeleteOnFrozenSecretMutation(t *testing.T) {
	for _, test := range []struct {
		name   string
		column string
		value  string
	}{
		{name: "ciphertext", column: "api_key_cipher", value: "mutated-ciphertext"},
		{name: "prefix", column: "api_key_prefix", value: "sk-mutated-prefix"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newChannelDeletionCoordinatorFixture(t)
			plain := "sk-channel-deletion-frozen-secret"
			name := upstreamKeyNameForGroup("frozen-secret")
			local := fixture.addKey(t, "frozen-secret", storage.GatewayUpstreamKeyProvenanceCreated, plain, 601, name)
			intent := fixture.prepare(t)
			mutationApplied := false
			manager := &channelDeletionTestKeyManager{
				inventory: []connector.APIKey{channelDeletionTestRemote(601, name, "frozen-secret")},
				revealed:  map[int64]string{601: plain}, removeOnDelete: true,
			}
			manager.prepareDeleteHook = func(ctx context.Context, remoteID int64) error {
				if remoteID != 601 {
					return errors.New("unexpected remote key selected for deletion")
				}
				mutationApplied = true
				return fixture.db.WithContext(ctx).Model(&storage.GatewayUpstreamKey{}).
					Where("id = ?", local.ID).Update(test.column, test.value).Error
			}
			service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)

			result, err := service.ReconcileChannelDeletions(context.Background(), DefaultChannelDeletionCoordinatorConfig())
			if !errors.Is(err, storage.ErrChannelDeletionKeyFreezeChanged) || result.Failed != 1 || result.Finalized != 0 {
				t.Fatalf("frozen %s mutation result=%+v err=%v", test.name, result, err)
			}
			if !mutationApplied {
				t.Fatalf("frozen %s mutation hook was not reached", test.name)
			}
			manager.mu.Lock()
			deleteCalls := append([]int64(nil), manager.deleteCalls...)
			manager.mu.Unlock()
			if len(deleteCalls) != 0 {
				t.Fatalf("frozen %s mutation reached remote delete: %v", test.name, deleteCalls)
			}
			var claim storage.ChannelDeletionKeyClaim
			if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&claim).Error; err != nil {
				t.Fatal(err)
			}
			if claim.Status != storage.ChannelDeletionKeyProcessing || claim.ClaimEpoch != 1 {
				t.Fatalf("claim state after frozen %s mutation: %+v", test.name, claim)
			}
		})
	}
}

func TestChannelDeletionCoordinatorStopsBeforeRemoteDeleteWhenClaimIsTakenOver(t *testing.T) {
	fixture := newChannelDeletionCoordinatorFixture(t)
	plain := "sk-channel-deletion-pre-callback-takeover"
	name := upstreamKeyNameForGroup("pre-callback")
	local := fixture.addKey(t, "pre-callback", storage.GatewayUpstreamKeyProvenanceCreated, plain, 701, name)
	intent := fixture.prepare(t)
	var prepared storage.ChannelDeletionKeyClaim
	if err := fixture.db.Where("intent_id = ? AND local_key_id = ?", intent.ID, local.ID).First(&prepared).Error; err != nil {
		t.Fatal(err)
	}
	fence := channelDeletionFence(intent)
	config := DefaultChannelDeletionCoordinatorConfig()
	var takeover *storage.ChannelDeletionKeyClaim
	manager := &channelDeletionTestKeyManager{
		inventory: []connector.APIKey{channelDeletionTestRemote(701, name, "pre-callback")},
		revealed:  map[int64]string{701: plain}, removeOnDelete: true,
	}
	manager.prepareDeleteHook = func(ctx context.Context, remoteID int64) error {
		if remoteID != 701 {
			return errors.New("unexpected remote key selected for deletion")
		}
		var processing storage.ChannelDeletionKeyClaim
		if err := fixture.db.WithContext(ctx).First(&processing, prepared.ID).Error; err != nil {
			return err
		}
		if processing.ClaimedAt == nil {
			return errors.New("processing claim has no acquisition time")
		}
		expired := processing.ClaimedAt.Add(time.Microsecond)
		result := fixture.db.WithContext(ctx).Model(&storage.ChannelDeletionKeyClaim{}).
			Where("id = ? AND status = ?", prepared.ID, storage.ChannelDeletionKeyProcessing).
			Update("lease_expires_at", expired)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("processing claim was unavailable for takeover")
		}
		var acquired bool
		var err error
		takeover, acquired, err = storage.NewChannels(fixture.db.Session(&gorm.Session{NewDB: true})).ClaimChannelDeletionKey(
			ctx, fence, prepared.ID, config.ClaimLease,
		)
		if err != nil {
			return err
		}
		if !acquired {
			return errors.New("expired processing claim was not taken over")
		}
		return nil
	}
	service := NewService(fixture.channels, fixture.rates, fixture.store, fixture.cipher, manager, nil)

	result, err := service.ReconcileChannelDeletions(context.Background(), config)
	if !errors.Is(err, storage.ErrChannelDeletionKeyClaimLost) || result.Failed != 1 || result.Finalized != 0 {
		t.Fatalf("pre-callback takeover result=%+v err=%v", result, err)
	}
	manager.mu.Lock()
	deleteCalls := append([]int64(nil), manager.deleteCalls...)
	manager.mu.Unlock()
	if len(deleteCalls) != 0 {
		t.Fatalf("stale claimant executed remote delete callback: %v", deleteCalls)
	}
	if takeover == nil || takeover.Status != storage.ChannelDeletionKeyProcessing || takeover.ClaimEpoch != prepared.ClaimEpoch+2 {
		t.Fatalf("takeover claim = %+v, prepared claim = %+v", takeover, prepared)
	}
}
