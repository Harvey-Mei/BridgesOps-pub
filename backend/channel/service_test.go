package channel

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/config"
	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	_ "github.com/Harvey-Mei/BridgeOps/backend/connector/newapi"
	"github.com/Harvey-Mei/BridgeOps/backend/crypto"
	"github.com/Harvey-Mei/BridgeOps/backend/netproxy"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func TestDeleteAPIKeyRejectsDirectRemoteDeletion(t *testing.T) {
	err := (&Service{}).DeleteAPIKey(context.Background(), 1, 7)
	if !errors.Is(err, ErrDirectAPIKeyDeletionDisabled) {
		t.Fatalf("direct delete error = %v", err)
	}
}

type managedDeletionRevalidationConnector struct {
	connector.Connector
	items   []connector.APIKey
	deleted []int64
	queries []connector.APIKeyQuery
}

type apiKeyRecoveryInventoryConnector struct {
	connector.Connector
	searchItems []connector.APIKey
	fullItems   []connector.APIKey
	queries     []connector.APIKeyQuery
}

type pagedAPIKeyRecoveryConnector struct {
	connector.Connector
	pages map[int]*connector.APIKeyPage
}

func (f *pagedAPIKeyRecoveryConnector) ListAPIKeys(_ context.Context, _ *connector.Channel, _ *connector.AuthSession, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	page := f.pages[query.Page]
	if page == nil {
		return nil, errors.New("unexpected recovery inventory page")
	}
	copy := *page
	copy.Items = append([]connector.APIKey(nil), page.Items...)
	return &copy, nil
}

func (f *apiKeyRecoveryInventoryConnector) ListAPIKeys(_ context.Context, _ *connector.Channel, _ *connector.AuthSession, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	f.queries = append(f.queries, query)
	items := []connector.APIKey{}
	if strings.TrimSpace(query.Search) == "" {
		items = append(items, f.fullItems...)
	} else {
		items = append(items, f.searchItems...)
	}
	return &connector.APIKeyPage{Items: items, Page: 1, PageSize: query.PageSize, Pages: 1, Total: int64(len(items))}, nil
}

func (f *managedDeletionRevalidationConnector) ListAPIKeys(_ context.Context, _ *connector.Channel, _ *connector.AuthSession, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	f.queries = append(f.queries, query)
	return &connector.APIKeyPage{Items: append([]connector.APIKey(nil), f.items...), Page: 1, Pages: 1, Total: int64(len(f.items))}, nil
}

func (f *managedDeletionRevalidationConnector) DeleteAPIKey(_ context.Context, _ *connector.Channel, _ *connector.AuthSession, id int64) error {
	f.deleted = append(f.deleted, id)
	return nil
}

func TestRevalidateAndDeleteManagedAPIKeyRejectsRemoteIdentityDrift(t *testing.T) {
	groupID := int64(7)
	expected := connector.APIKey{
		ID: 19, Name: "BOG-p-0123456789abcdef", Status: "active", GroupName: "old", GroupID: &groupID,
		Source: connector.APIKeySourceSub2API, ObservedFields: connector.APIKeyObservedFields{Quota: true},
	}

	for _, test := range []struct {
		name    string
		current connector.APIKey
	}{
		{name: "moved group", current: func() connector.APIKey { key := expected; key.GroupName = "new"; return key }()},
		{name: "reused id", current: func() connector.APIKey { key := expected; key.Name = "BOG-p-fedcba9876543210"; return key }()},
	} {
		t.Run(test.name, func(t *testing.T) {
			remote := &managedDeletionRevalidationConnector{items: []connector.APIKey{test.current}}
			err := revalidateAndDeleteManagedAPIKey(context.Background(), remote, &connector.Channel{}, &connector.AuthSession{}, expected)
			if !errors.Is(err, connector.ErrManagedAPIKeyDeletionIdentityChanged) || len(remote.deleted) != 0 || len(remote.queries) != 1 ||
				!remote.queries[0].RequireCompleteGroupIdentity || remote.queries[0].Search != expected.Name {
				t.Fatalf("revalidated delete err=%v deleted=%v queries=%+v", err, remote.deleted, remote.queries)
			}
		})
	}
}

func TestRevalidateAndDeleteManagedAPIKeyDeletesOnlyExactIdentity(t *testing.T) {
	expected := connector.APIKey{ID: 20, Name: "BOG-p-abcdef0123456789", Status: "active", GroupName: "current", Source: connector.APIKeySourceNewAPI}
	remote := &managedDeletionRevalidationConnector{items: []connector.APIKey{expected}}
	if err := revalidateAndDeleteManagedAPIKey(context.Background(), remote, &connector.Channel{}, &connector.AuthSession{}, expected); err != nil {
		t.Fatalf("revalidated delete: %v", err)
	}
	if len(remote.deleted) != 1 || remote.deleted[0] != expected.ID || len(remote.queries) != 1 ||
		!remote.queries[0].RequireCompleteGroupIdentity || remote.queries[0].Search != expected.Name {
		t.Fatalf("deleted=%v queries=%+v", remote.deleted, remote.queries)
	}

	remote = &managedDeletionRevalidationConnector{}
	if err := revalidateAndDeleteManagedAPIKey(context.Background(), remote, &connector.Channel{}, &connector.AuthSession{}, expected); err != nil || len(remote.deleted) != 0 ||
		len(remote.queries) != 2 || remote.queries[0].Search != expected.Name || remote.queries[1].Search != "" {
		t.Fatalf("already absent delete err=%v deleted=%v queries=%+v", err, remote.deleted, remote.queries)
	}
}

type fakeHTTPConfigConnector struct {
	cfg connector.HTTPConfig
}

func (f *fakeHTTPConfigConnector) SetHTTPConfig(cfg connector.HTTPConfig) {
	f.cfg = cfg
}

type fakeTurnstileConnector struct {
	connector.Connector
	siteKey    string
	siteKeyErr error
	loginCalls int
}

type fakeTokenRefreshConnector struct {
	connector.Connector
	checkErr     error
	refreshCalls int
}

type fakeConcurrentTokenRefreshConnector struct {
	connector.Connector
	checkCalls   int
	refreshCalls int
}

type fakeConcurrentTokenIdentityChangeConnector struct {
	connector.Connector
	winnerCheckStarted chan struct{}
	allowWinnerCheck   chan struct{}
	checkCalls         int
	refreshCalls       int
}

type fakeReusableSessionConnector struct {
	connector.Connector
	checkCalls int
	loginCalls int
}

type fakeDiscoveredOriginConnector struct {
	connector.Connector
	discovered string
}

type fakeAccountConcurrencyConnector struct {
	connector.Connector
	fact *connector.AccountConcurrencyFact
}

func (f *fakeAccountConcurrencyConnector) GetTurnstileSiteKey(context.Context, *connector.Channel) (string, error) {
	return "", nil
}

func (f *fakeAccountConcurrencyConnector) Login(context.Context, *connector.Channel) (*connector.AuthSession, error) {
	return &connector.AuthSession{
		UserID:             f.fact.AccountID,
		AccessToken:        "access",
		ExpiresAt:          time.Now().Add(time.Hour),
		AccountConcurrency: f.fact,
	}, nil
}

func (f *fakeDiscoveredOriginConnector) GetTurnstileSiteKey(_ context.Context, channel *connector.Channel) (string, error) {
	channel.SiteURL = f.discovered
	return "", nil
}

func (f *fakeDiscoveredOriginConnector) Login(context.Context, *connector.Channel) (*connector.AuthSession, error) {
	return &connector.AuthSession{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour),
	}, nil
}

func (f *fakeReusableSessionConnector) CheckAuth(context.Context, *connector.Channel, *connector.AuthSession) error {
	f.checkCalls++
	return nil
}

func (f *fakeReusableSessionConnector) Login(context.Context, *connector.Channel) (*connector.AuthSession, error) {
	f.loginCalls++
	return nil, errors.New("login must not be called for a valid session")
}

func (f *fakeTokenRefreshConnector) CheckAuth(context.Context, *connector.Channel, *connector.AuthSession) error {
	return f.checkErr
}

func (f *fakeTokenRefreshConnector) RefreshSession(context.Context, *connector.Channel, *connector.AuthSession) (*connector.AuthSession, error) {
	f.refreshCalls++
	return &connector.AuthSession{AccessToken: "refreshed", RefreshToken: "rotated"}, nil
}

func (f *fakeConcurrentTokenRefreshConnector) CheckAuth(_ context.Context, _ *connector.Channel, session *connector.AuthSession) error {
	f.checkCalls++
	if session != nil && session.AccessToken == "old" {
		return connector.HTTPStatusError(http.StatusUnauthorized, []byte(`{"error":"expired"}`))
	}
	return nil
}

func (f *fakeConcurrentTokenRefreshConnector) RefreshSession(context.Context, *connector.Channel, *connector.AuthSession) (*connector.AuthSession, error) {
	f.refreshCalls++
	return &connector.AuthSession{AccessToken: "loser", RefreshToken: "loser-refresh"}, nil
}

func (f *fakeConcurrentTokenIdentityChangeConnector) CheckAuth(_ context.Context, _ *connector.Channel, session *connector.AuthSession) error {
	f.checkCalls++
	if session != nil && session.AccessToken == "old" {
		return connector.HTTPStatusError(http.StatusUnauthorized, []byte(`{"error":"expired"}`))
	}
	if session != nil && session.AccessToken == "winner" {
		close(f.winnerCheckStarted)
		<-f.allowWinnerCheck
	}
	return nil
}

func (f *fakeConcurrentTokenIdentityChangeConnector) RefreshSession(context.Context, *connector.Channel, *connector.AuthSession) (*connector.AuthSession, error) {
	f.refreshCalls++
	return &connector.AuthSession{AccessToken: "loser", RefreshToken: "loser-refresh"}, nil
}

func (f *fakeTurnstileConnector) GetTurnstileSiteKey(context.Context, *connector.Channel) (string, error) {
	return f.siteKey, f.siteKeyErr
}

func (f *fakeTurnstileConnector) Login(context.Context, *connector.Channel) (*connector.AuthSession, error) {
	f.loginCalls++
	return nil, errors.New("unexpected login")
}

func testService(t *testing.T) (*Service, *crypto.Cipher) {
	t.Helper()
	db := testdb.Open(t)

	cipher, err := crypto.NewCipher("12345678901234567890123456789012")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	svc := NewService(
		storage.NewChannels(db),
		storage.NewAuthSessions(db),
		storage.NewCaptchas(db),
		storage.NewRates(db),
		storage.NewMonitorLogs(db),
		cipher,
	)
	svc.SetNetworkProxies(storage.NewNetworkProxies(db))
	return svc, cipher
}

func testAPIKeyCreationService(t *testing.T, siteURL string) *Service {
	t.Helper()
	svc, _ := testAPIKeyCreationServiceWithDB(t, siteURL)
	return svc
}

func testAPIKeyCreationServiceWithDB(t *testing.T, siteURL string) (*Service, *gorm.DB) {
	t.Helper()
	db := testdb.Open(t)

	cipher, err := crypto.NewCipher("12345678901234567890123456789012")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	credential, err := cipher.Encrypt(`{"access_token":"test-token","user_id":"1"}`)
	if err != nil {
		t.Fatalf("encrypt credential: %v", err)
	}
	channels := storage.NewChannels(db)
	if err := channels.Create(&storage.Channel{
		Name: "api-key-test", Type: storage.ChannelTypeNewAPI, SiteURL: siteURL, Username: "tester",
		PasswordCipher: credential, CredentialMode: storage.CredentialModeToken, MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	return NewService(
		channels,
		storage.NewAuthSessions(db),
		storage.NewCaptchas(db),
		storage.NewRates(db),
		storage.NewMonitorLogs(db),
		cipher,
	), db
}

func TestLockOrdinaryChannelOperationReusesHeldChannelLock(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name: "nested-channel-operation-lock", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://example.com", Username: "owner", PasswordCipher: "credential",
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	outerUnlock, err := svc.Channels.LockChannelOperation(ctx, channel.ID)
	if err != nil {
		t.Fatalf("lock outer channel operation: %v", err)
	}
	defer outerUnlock()
	heldCtx := storage.WithChannelOperationLockHeld(ctx, channel.ID)

	lockedCtx, innerUnlock, err := svc.lockOrdinaryChannelOperation(heldCtx, channel.ID)
	if err != nil {
		t.Fatalf("reuse held channel operation lock: %v", err)
	}
	if innerUnlock == nil {
		t.Fatal("reused channel operation lock returned a nil unlock")
	}
	innerUnlock()
	if !storage.ChannelOperationLockHeld(lockedCtx, channel.ID) {
		t.Fatal("reused channel operation context lost its ownership marker")
	}
}

func TestLockOrdinaryChannelOperationStillRejectsPendingDeletionWhenLockIsHeld(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name: "held-lock-deletion-fence", Type: storage.ChannelTypeNewAPI,
		SiteURL: "https://example.com", Username: "owner", PasswordCipher: "credential",
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := svc.Channels.Delete(channel.ID); !errors.Is(err, storage.ErrChannelDeletionPending) {
		t.Fatalf("prepare channel deletion: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	outerUnlock, err := svc.Channels.LockChannelOperation(ctx, channel.ID)
	if err != nil {
		t.Fatalf("lock outer channel operation: %v", err)
	}
	defer outerUnlock()
	heldCtx := storage.WithChannelOperationLockHeld(ctx, channel.ID)

	_, innerUnlock, err := svc.lockOrdinaryChannelOperation(heldCtx, channel.ID)
	if !errors.Is(err, storage.ErrChannelDeletionPending) {
		t.Fatalf("held-lock deletion admission error = %v, want ErrChannelDeletionPending", err)
	}
	if innerUnlock != nil {
		t.Fatal("rejected held-lock operation returned an unlock")
	}
}

func TestDeletePreparesChannelAndPreservesAuthSession(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name:           "delete-prepare",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "owner",
		PasswordCipher: "credential",
		MonitorEnabled: true,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Hour)
	if err := svc.AuthSessions.Upsert(&storage.AuthSession{
		ChannelID:         channel.ID,
		UserID:            "remote-owner",
		AccessTokenCipher: "encrypted-access-token",
		ExpiresAt:         &expiresAt,
	}); err != nil {
		t.Fatalf("create auth session: %v", err)
	}

	err := svc.Delete(context.Background(), channel.ID)
	if !errors.Is(err, storage.ErrChannelDeletionPending) {
		t.Fatalf("delete error = %v, want ErrChannelDeletionPending", err)
	}
	stored, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatalf("find prepared channel: %v", err)
	}
	if !stored.DeletionPending || stored.DeletionEpoch == 0 || stored.DeletionIdentityRevision == 0 {
		t.Fatalf("channel deletion fence was not prepared: %+v", stored)
	}
	session, err := svc.AuthSessions.FindByChannel(channel.ID)
	if err != nil {
		t.Fatalf("find retained auth session: %v", err)
	}
	if session == nil || session.UserID != "remote-owner" || session.AccessTokenCipher != "encrypted-access-token" {
		t.Fatalf("auth session was not retained: %+v", session)
	}
}

func TestResolveRejectsUnmigratedLegacyProxySetting(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt("secret")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	withLegacyProxy := &storage.Channel{
		ID:             1,
		Name:           "legacy-proxy",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: enc,
		ProxyEnabled:   true,
	}
	if _, err := svc.Resolve(context.Background(), withLegacyProxy); err == nil || !strings.Contains(err.Error(), "unmigrated legacy") {
		t.Fatalf("legacy proxy resolve error = %v", err)
	}

	withoutProxy := *withLegacyProxy
	withoutProxy.ProxyEnabled = false
	resolved, err := svc.Resolve(context.Background(), &withoutProxy)
	if err != nil {
		t.Fatalf("resolve without proxy: %v", err)
	}
	if resolved.ProxyURL != "" {
		t.Fatalf("proxy url = %q, want empty", resolved.ProxyURL)
	}
}

func TestMigrateLegacyNetworkProxyBindings(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt("secret")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	legacy := &storage.Channel{
		Name:           "legacy-proxy",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: enc,
		ProxyEnabled:   true,
	}
	if err := svc.Channels.Create(legacy); err != nil {
		t.Fatal(err)
	}
	migration, err := svc.MigrateLegacyNetworkProxyBindings(config.ProxyConfig{
		Enabled:  true,
		Protocol: "socks5",
		Host:     "127.0.0.1",
		Port:     1080,
		Username: "u",
		Password: "p",
	})
	if err != nil {
		t.Fatal(err)
	}
	if migration.Bound != 1 || migration.ClearedToDirect != 0 {
		t.Fatalf("migration = %#v", migration)
	}
	stored, err := svc.Channels.FindByID(legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NetworkProxyID == nil || !stored.ProxyEnabled {
		t.Fatalf("legacy channel was not bound: %#v", stored)
	}
	resolved, err := svc.Resolve(context.Background(), stored)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ProxyURL != "socks5://u:p@127.0.0.1:1080" {
		t.Fatalf("proxy URL = %q", resolved.ProxyURL)
	}
	again, err := svc.MigrateLegacyNetworkProxyBindings(config.ProxyConfig{Enabled: true, Protocol: "socks5", Host: "127.0.0.1", Port: 1080, Username: "u", Password: "p"})
	if err != nil || again.Bound != 0 || again.ClearedToDirect != 0 {
		t.Fatalf("repeat migration = %#v err=%v", again, err)
	}
	profiles, err := svc.NetworkProxies.List()
	if err != nil || len(profiles) != 1 {
		t.Fatalf("profiles = %#v err=%v", profiles, err)
	}
}

func TestMigrateLegacyNetworkProxyBindingsClearsDisabledSystemProxy(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt("secret")
	if err != nil {
		t.Fatal(err)
	}
	legacy := &storage.Channel{Name: "legacy-direct", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com", Username: "u", PasswordCipher: enc, ProxyEnabled: true}
	if err := svc.Channels.Create(legacy); err != nil {
		t.Fatal(err)
	}
	migration, err := svc.MigrateLegacyNetworkProxyBindings(config.ProxyConfig{})
	if err != nil || migration.Bound != 0 || migration.ClearedToDirect != 1 {
		t.Fatalf("migration = %#v err=%v", migration, err)
	}
	stored, err := svc.Channels.FindByID(legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ProxyEnabled || stored.NetworkProxyID != nil {
		t.Fatalf("legacy channel did not become direct: %#v", stored)
	}
}

func TestResolveUsesBoundNetworkProxyInsteadOfLegacyGlobalProxy(t *testing.T) {
	svc, cipher := testService(t)
	rawCredentials, err := (netproxy.Credentials{Username: "bound", Password: "secret"}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	cipherText, err := cipher.Encrypt(rawCredentials)
	if err != nil {
		t.Fatal(err)
	}
	profile := &storage.NetworkProxy{
		Name:             "bound-socks",
		Protocol:         "socks5",
		Host:             "127.0.0.1",
		Port:             1080,
		CredentialCipher: cipherText,
		Enabled:          true,
	}
	if err := svc.NetworkProxies.Create(profile); err != nil {
		t.Fatal(err)
	}
	passwordCipher, err := cipher.Encrypt("channel-password")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name:           "bound-channel",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: passwordCipher,
		ProxyEnabled:   true,
		NetworkProxyID: &profile.ID,
	}
	resolved, err := svc.Resolve(context.Background(), channel)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ProxyURL != "socks5://bound:secret@127.0.0.1:1080" {
		t.Fatalf("proxy URL = %q", resolved.ProxyURL)
	}
}

func TestResolveNetworkProxyCacheKeyIgnoresHealthTestTelemetry(t *testing.T) {
	svc, cipher := testService(t)
	passwordCipher, err := cipher.Encrypt("channel-password")
	if err != nil {
		t.Fatal(err)
	}
	profile := &storage.NetworkProxy{Name: "stable-cache", Protocol: "http", Host: "127.0.0.1", Port: 8080, Enabled: true}
	if err := svc.NetworkProxies.Create(profile); err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name:           "bound-channel",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: passwordCipher,
		ProxyEnabled:   true,
		NetworkProxyID: &profile.ID,
	}
	before, err := svc.Resolve(context.Background(), channel)
	if err != nil {
		t.Fatal(err)
	}
	latency := int64(12)
	if err := svc.NetworkProxies.UpdateTestResult(profile.ID, time.Now().UTC(), &latency, "203.0.113.11", ""); err != nil {
		t.Fatal(err)
	}
	after, err := svc.Resolve(context.Background(), channel)
	if err != nil {
		t.Fatal(err)
	}
	if before.ProxyCacheKey == "" || before.ProxyCacheKey != after.ProxyCacheKey {
		t.Fatalf("proxy cache key changed after telemetry update: before=%q after=%q", before.ProxyCacheKey, after.ProxyCacheKey)
	}
}

func TestResolveRejectsDisabledBoundNetworkProxyWithoutLegacyFallback(t *testing.T) {
	svc, cipher := testService(t)
	profile := &storage.NetworkProxy{Name: "disabled", Protocol: "http", Host: "127.0.0.1", Port: 8080, Enabled: false}
	if err := svc.NetworkProxies.Create(profile); err != nil {
		t.Fatal(err)
	}
	passwordCipher, err := cipher.Encrypt("channel-password")
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.Resolve(context.Background(), &storage.Channel{
		Name:           "bound-channel",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: passwordCipher,
		ProxyEnabled:   true,
		NetworkProxyID: &profile.ID,
	})
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("resolve error = %v", err)
	}
}

func TestApplyHTTPConfigUsesUpstreamConfig(t *testing.T) {
	svc, _ := testService(t)
	svc.UpdateUpstreamConfig(config.UpstreamConfig{
		TimeoutSeconds: 45,
		UserAgent:      "custom-agent",
	})

	conn := &fakeHTTPConfigConnector{}
	svc.ApplyHTTPConfig(conn)

	if conn.cfg.Timeout != 45*time.Second {
		t.Fatalf("timeout = %s", conn.cfg.Timeout)
	}
	if conn.cfg.UserAgent != "custom-agent" {
		t.Fatalf("user agent = %q", conn.cfg.UserAgent)
	}
}

func TestApplyHTTPConfigUsesDefaults(t *testing.T) {
	svc, _ := testService(t)
	conn := &fakeHTTPConfigConnector{}
	svc.ApplyHTTPConfig(conn)

	if conn.cfg.Timeout != time.Duration(config.DefaultUpstreamTimeoutSeconds)*time.Second {
		t.Fatalf("timeout = %s", conn.cfg.Timeout)
	}
	if conn.cfg.UserAgent != config.DefaultUpstreamUserAgent {
		t.Fatalf("user agent = %q", conn.cfg.UserAgent)
	}
}

func TestBuildSessionFromSub2APITokenCredentialIncludesRefreshToken(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt(`{"access_token":"access","refresh_token":"refresh"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	session, err := svc.buildSessionFromToken(&storage.Channel{
		Type:           storage.ChannelTypeSub2API,
		PasswordCipher: enc,
	})
	if err != nil {
		t.Fatalf("buildSessionFromToken: %v", err)
	}
	if session.AccessToken != "access" {
		t.Fatalf("access token = %q, want access", session.AccessToken)
	}
	if session.RefreshToken != "refresh" {
		t.Fatalf("refresh token = %q, want refresh", session.RefreshToken)
	}
}

func TestLoginStopsBeforeSub2APILoginWhenTurnstileHasNoSolver(t *testing.T) {
	svc, _ := testService(t)
	conn := &fakeTurnstileConnector{siteKey: "turnstile-site-key"}
	channel := &storage.Channel{
		Type:             storage.ChannelTypeSub2API,
		SiteURL:          "https://example.com",
		CredentialMode:   storage.CredentialModePassword,
		TurnstileEnabled: false,
		CaptchaConfigID:  nil,
	}

	_, err := svc.login(context.Background(), channel, &connector.Channel{SiteURL: channel.SiteURL}, conn)
	if err == nil {
		t.Fatal("login succeeded, want actionable Turnstile error")
	}
	for _, want := range []string{"Cloudflare Turnstile", "手动登录接管", "access_token", "refresh_token"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err, want)
		}
	}
	if conn.loginCalls != 0 {
		t.Fatalf("login calls = %d, want 0", conn.loginCalls)
	}
}

func TestPrepareTurnstileAllowsSub2APILoginWhenUpstreamDisabledIt(t *testing.T) {
	svc, _ := testService(t)
	conn := &fakeTurnstileConnector{}
	channel := &storage.Channel{
		Type:           storage.ChannelTypeSub2API,
		CredentialMode: storage.CredentialModePassword,
	}

	if err := svc.prepareTurnstile(context.Background(), channel, &connector.Channel{}, conn); err != nil {
		t.Fatalf("prepareTurnstile: %v", err)
	}
}

func TestEnsureSessionPersistsDiscoveredAPIOriginSeparatelyFromHomepage(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name:           "split-origin",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://www.example.com",
		PasswordCipher: "encrypted",
		CredentialMode: storage.CredentialModePassword,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	previousRevision := channel.UpstreamAccountIdentityRevision
	resolved := &connector.Channel{SiteURL: channel.SiteURL}
	conn := &fakeDiscoveredOriginConnector{discovered: "https://api.example.com"}
	if _, err := svc.EnsureSession(context.Background(), channel, resolved, conn); err != nil {
		t.Fatalf("EnsureSession: %v", err)
	}
	stored, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SiteURL != "https://www.example.com" {
		t.Fatalf("stored homepage URL = %q, want unchanged homepage", stored.SiteURL)
	}
	if stored.APIURL != conn.discovered {
		t.Fatalf("stored API URL = %q, want %q", stored.APIURL, conn.discovered)
	}
	if stored.UpstreamAccountIdentityRevision != previousRevision+1 || channel.UpstreamAccountIdentityRevision != stored.UpstreamAccountIdentityRevision {
		t.Fatalf("discovered API identity revision: stored=%d in-memory=%d previous=%d", stored.UpstreamAccountIdentityRevision, channel.UpstreamAccountIdentityRevision, previousRevision)
	}
	savedSession, err := svc.AuthSessions.FindByChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if savedSession != nil {
		t.Fatalf("old-revision session survived API identity discovery: %#v", savedSession)
	}
}

func TestEnsureSessionPersistsExplicitAccountConcurrencyFact(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name:           "account-concurrency",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://www.example.com",
		APIURL:         "https://api.example.com",
		PasswordCipher: "encrypted",
		CredentialMode: storage.CredentialModePassword,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	limit := 3
	observedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	fact := &connector.AccountConcurrencyFact{
		Provider: connector.TypeSub2API, AccountID: "account-42", Limit: &limit,
		Scope: connector.AccountConcurrencyScopeAccount, Source: "sub2api.auth.login.user.concurrency", ObservedAt: observedAt,
	}
	resolved := &connector.Channel{Type: connector.TypeSub2API, SiteURL: channel.APIURL, APIOriginKnown: true}
	if _, err := svc.EnsureSession(context.Background(), channel, resolved, &fakeAccountConcurrencyConnector{fact: fact}); err != nil {
		t.Fatalf("EnsureSession: %v", err)
	}
	snapshot, err := svc.Channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil || snapshot.Limit == nil || *snapshot.Limit != 3 || snapshot.State() != "bounded" ||
		snapshot.Scope != storage.UpstreamConcurrencyScopeAccount || snapshot.Source != fact.Source ||
		snapshot.ObservedAt == nil || !snapshot.ObservedAt.Equal(observedAt) {
		t.Fatalf("stored account concurrency snapshot = %#v", snapshot)
	}
	savedSession, err := svc.AuthSessions.FindByChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if savedSession == nil || savedSession.UserID != "" {
		t.Fatalf("Sub2API account id leaked into persisted session: %#v", savedSession)
	}
}

func TestPersistAccountConcurrencyFactRebindsAuthenticatedUnknownAccount(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name: "account-concurrency-unknown", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://www.example.com", APIURL: "https://api.example.com",
		PasswordCipher: "encrypted", CredentialMode: storage.CredentialModePassword,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	resolved := &connector.Channel{Type: connector.TypeSub2API, SiteURL: channel.APIURL, APIOriginKnown: true}
	base := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	limit := 3
	if err := svc.PersistAccountConcurrencyFact(channel, resolved, &connector.AccountConcurrencyFact{
		Provider: connector.TypeSub2API, AccountID: "account-a", Limit: &limit,
		Scope: connector.AccountConcurrencyScopeAccount, Source: "sub2api.auth.me.concurrency", ObservedAt: base,
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.PersistAccountConcurrencyFact(channel, resolved, &connector.AccountConcurrencyFact{
		Provider: connector.TypeSub2API, AccountID: "account-b", Limit: nil,
		Scope: connector.AccountConcurrencyScopeAccount, Source: "sub2api.auth.me.concurrency", ObservedAt: base.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := svc.Channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil || snapshot.AccountRecordID == 0 || snapshot.Limit != nil || snapshot.State() != "unknown" {
		t.Fatalf("authenticated unknown account snapshot = %#v", snapshot)
	}
}

func TestChannelUpdateRejectsLateOldAccountFactAndDoesNotInheritLimit(t *testing.T) {
	svc, cipher := testService(t)
	encrypted, err := cipher.Encrypt("old-password")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "late-account-fact", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://old.example.com", APIURL: "https://old-api.example.com", Username: "old-user",
		PasswordCipher: encrypted, CredentialMode: storage.CredentialModePassword,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	oldRevision := channel.UpstreamAccountIdentityRevision
	limit := 2
	base := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	oldResolved := &connector.Channel{Type: connector.TypeSub2API, SiteURL: channel.APIURL, APIOriginKnown: true}
	oldFact := &connector.AccountConcurrencyFact{
		Provider: connector.TypeSub2API, AccountID: "old-account", Limit: &limit,
		Scope: connector.AccountConcurrencyScopeAccount, Source: "sub2api.auth.me.concurrency", ObservedAt: base,
	}
	if err := svc.PersistAccountConcurrencyFact(channel, oldResolved, oldFact); err != nil {
		t.Fatal(err)
	}
	if err := svc.persistSession(channel, &connector.AuthSession{AccessToken: "old-session", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}

	newSiteURL := "https://new.example.com"
	newUsername := "new-user"
	expectedAdminRevision := channel.AdminRevision
	updated, err := svc.Update(channel.ID, UpdateInput{ExpectedAdminRevision: &expectedAdminRevision, SiteURL: &newSiteURL, Username: &newUsername})
	if err != nil {
		t.Fatal(err)
	}
	if updated.UpstreamAccountIdentityRevision != oldRevision+1 {
		t.Fatalf("updated identity revision = %d, want %d", updated.UpstreamAccountIdentityRevision, oldRevision+1)
	}
	unknown, err := svc.Channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unknown != nil {
		t.Fatalf("new account with unknown limit inherited old fact: %#v", unknown)
	}

	oldFact.ObservedAt = base.Add(time.Hour)
	if err := svc.PersistAccountConcurrencyFact(channel, oldResolved, oldFact); !errors.Is(err, storage.ErrStaleUpstreamAccountConcurrencyObservation) {
		t.Fatalf("late old fact error = %v", err)
	}
	unknown, err = svc.Channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unknown != nil {
		t.Fatalf("late old fact rebound updated channel: %#v", unknown)
	}
	if err := svc.persistSession(channel, &connector.AuthSession{AccessToken: "late-old-session", ExpiresAt: time.Now().Add(time.Hour)}); !errors.Is(err, storage.ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("late old session persist error = %v", err)
	}
	saved, err := svc.AuthSessions.FindByChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if saved != nil {
		t.Fatalf("late old session was recreated: %#v", saved)
	}
}

func TestChannelUpdatePersistsRechargeMultiplierFields(t *testing.T) {
	svc, cipher := testService(t)
	encrypted, err := cipher.Encrypt("password")
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "recharge-settings", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "user", PasswordCipher: encrypted,
		CredentialMode: storage.CredentialModePassword, RechargeMultiplierMode: connector.RechargeMultiplierModeMultiply,
		MonitorEnabled: true,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	expectedAdminRevision := channel.AdminRevision
	multiplier := 2.5
	mode := connector.RechargeMultiplierModeDivide
	updated, err := svc.Update(channel.ID, UpdateInput{
		ExpectedAdminRevision:  &expectedAdminRevision,
		RechargeMultiplier:     &multiplier,
		RechargeMultiplierMode: &mode,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.RechargeMultiplier == nil || *updated.RechargeMultiplier != multiplier || updated.RechargeMultiplierMode != mode || updated.AdminRevision != expectedAdminRevision+1 {
		t.Fatalf("updated recharge settings = %#v", updated)
	}
	stored, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.RechargeMultiplier == nil || *stored.RechargeMultiplier != multiplier || stored.RechargeMultiplierMode != mode {
		t.Fatalf("stored recharge settings = %#v", stored)
	}
}

func TestChannelUpdateRequiresAdminRevisionAndClearsExplicitNullableMultiplier(t *testing.T) {
	svc, cipher := testService(t)
	encrypted, err := cipher.Encrypt("password")
	if err != nil {
		t.Fatal(err)
	}
	multiplier := 2.5
	channel := &storage.Channel{
		Name: "nullable-recharge-settings", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "user", PasswordCipher: encrypted,
		CredentialMode: storage.CredentialModePassword, RechargeMultiplier: &multiplier,
		RechargeMultiplierMode: connector.RechargeMultiplierModeMultiply, MonitorEnabled: true,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Update(channel.ID, UpdateInput{RechargeMultiplierSet: true}); !errors.Is(err, storage.ErrChannelAdminRevisionRequired) {
		t.Fatalf("missing administrator revision error = %v", err)
	}
	expectedAdminRevision := channel.AdminRevision
	updated, err := svc.Update(channel.ID, UpdateInput{
		ExpectedAdminRevision: &expectedAdminRevision,
		RechargeMultiplierSet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.RechargeMultiplier != nil || updated.AdminRevision != expectedAdminRevision+1 {
		t.Fatalf("cleared recharge settings = %#v", updated)
	}
	stored, err := svc.Channels.FindByID(channel.ID)
	if err != nil || stored.RechargeMultiplier != nil {
		t.Fatalf("stored recharge multiplier = %v err=%v", stored.RechargeMultiplier, err)
	}
}

func TestTokenUsernameEditPreservesAccountIdentitySessionAndConcurrencyBinding(t *testing.T) {
	svc, cipher := testService(t)
	encrypted, err := cipher.Encrypt(`{"access_token":"token"}`)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "token-note", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://api.example.com", APIURL: "https://api.example.com", Username: "old note",
		PasswordCipher: encrypted, CredentialMode: storage.CredentialModeToken, MonitorEnabled: true,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	limit := 3
	resolved := &connector.Channel{Type: connector.TypeSub2API, SiteURL: channel.APIURL, APIOriginKnown: true}
	if err := svc.PersistAccountConcurrencyFact(channel, resolved, &connector.AccountConcurrencyFact{
		Provider: connector.TypeSub2API, AccountID: "token-account", Limit: &limit,
		Scope: connector.AccountConcurrencyScopeAccount, Source: "sub2api.auth.me.concurrency", ObservedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.persistSession(channel, &connector.AuthSession{AccessToken: "session", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	identityRevision := channel.UpstreamAccountIdentityRevision
	expectedAdminRevision := channel.AdminRevision
	note := "new note"
	updated, err := svc.Update(channel.ID, UpdateInput{ExpectedAdminRevision: &expectedAdminRevision, Username: &note})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Username != note || updated.UpstreamAccountIdentityRevision != identityRevision || updated.AdminRevision != expectedAdminRevision+1 {
		t.Fatalf("token note update changed identity: %#v", updated)
	}
	if session, err := svc.AuthSessions.FindByChannel(channel.ID); err != nil || session == nil {
		t.Fatalf("token note update cleared session: %#v err=%v", session, err)
	}
	if snapshot, err := svc.Channels.UpstreamAccountConcurrencyForChannel(channel.ID); err != nil || snapshot == nil || snapshot.Limit == nil || *snapshot.Limit != limit {
		t.Fatalf("token note update cleared account binding: %#v err=%v", snapshot, err)
	}
}

func TestNormalizeRechargeMultiplierSettingsRejectsSilentFallbacks(t *testing.T) {
	value := 2.5
	got, mode, err := normalizeRechargeMultiplierSettings(&value, "", connector.RechargeMultiplierModeMultiply)
	if err != nil || got == nil || *got != value || mode != connector.RechargeMultiplierModeMultiply {
		t.Fatalf("normalized default settings: value=%v mode=%q err=%v", got, mode, err)
	}
	zero := 0.0
	if _, _, err := normalizeRechargeMultiplierSettings(&zero, connector.RechargeMultiplierModeDivide, connector.RechargeMultiplierModeMultiply); err == nil {
		t.Fatal("zero recharge multiplier was silently accepted")
	}
	if _, err := normalizeRechargeMultiplierMode("subtract", ""); err == nil {
		t.Fatal("unknown recharge multiplier mode was silently normalized")
	}
}

func TestResolveUsesAPIURLAndHomepageEditClearsIt(t *testing.T) {
	svc, cipher := testService(t)
	encrypted, err := cipher.Encrypt("password")
	if err != nil {
		t.Fatal(err)
	}
	stored := &storage.Channel{
		Name:           "separate-urls",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://www.example.com",
		APIURL:         "https://api.example.com",
		Username:       "user",
		PasswordCipher: encrypted,
		CredentialMode: storage.CredentialModePassword,
	}
	if err := svc.Channels.Create(stored); err != nil {
		t.Fatal(err)
	}

	resolved, err := svc.Resolve(context.Background(), stored)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if resolved.SiteURL != stored.APIURL {
		t.Fatalf("resolved API base = %q, want %q", resolved.SiteURL, stored.APIURL)
	}
	if resolved.HomepageURL != stored.SiteURL {
		t.Fatalf("resolved homepage = %q, want %q", resolved.HomepageURL, stored.SiteURL)
	}
	if !resolved.APIOriginKnown {
		t.Fatal("resolved channel did not mark the persisted API origin as known")
	}

	newHomepage := "https://new.example.com"
	expectedAdminRevision := stored.AdminRevision
	updated, err := svc.Update(stored.ID, UpdateInput{ExpectedAdminRevision: &expectedAdminRevision, SiteURL: &newHomepage})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.SiteURL != newHomepage || updated.APIURL != "" {
		t.Fatalf("updated URLs: site=%q api=%q", updated.SiteURL, updated.APIURL)
	}
	resolved, err = svc.Resolve(context.Background(), updated)
	if err != nil {
		t.Fatalf("Resolve after homepage edit: %v", err)
	}
	if resolved.APIOriginKnown || resolved.SiteURL != newHomepage || resolved.HomepageURL != newHomepage {
		t.Fatalf("resolved fallback URLs after homepage edit: %+v", resolved)
	}
}

func TestPrepareTurnstileDoesNotBlockUnconfiguredSub2APIWhenPublicSettingsUnavailable(t *testing.T) {
	svc, _ := testService(t)
	conn := &fakeTurnstileConnector{siteKeyErr: errors.New("settings unavailable")}
	channel := &storage.Channel{
		Type:             storage.ChannelTypeSub2API,
		CredentialMode:   storage.CredentialModePassword,
		TurnstileEnabled: false,
	}

	if err := svc.prepareTurnstile(context.Background(), channel, &connector.Channel{}, conn); err != nil {
		t.Fatalf("prepareTurnstile: %v", err)
	}
}

func TestEnsureTokenSessionDoesNotRefreshOnTransientAuthCheckError(t *testing.T) {
	svc, cipher := testService(t)
	credential, err := cipher.Encrypt(`{"access_token":"access","refresh_token":"refresh"}`)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "token", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: credential, CredentialMode: storage.CredentialModeToken}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	conn := &fakeTokenRefreshConnector{checkErr: connector.HTTPStatusError(500, []byte("temporary"))}
	if _, err := svc.EnsureSession(context.Background(), channel, &connector.Channel{}, conn); err == nil || !strings.Contains(err.Error(), "temporary") {
		t.Fatalf("EnsureSession error = %v", err)
	}
	if conn.refreshCalls != 0 {
		t.Fatalf("refresh calls = %d, want 0", conn.refreshCalls)
	}
}

func TestEnsureTokenSessionReusesConcurrentRefreshWinner(t *testing.T) {
	svc, cipher := testService(t)
	oldCredential, err := cipher.Encrypt(`{"access_token":"old","refresh_token":"old-refresh"}`)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "token-concurrent-refresh", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "u", PasswordCipher: oldCredential,
		CredentialMode: storage.CredentialModeToken,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	identityRevision := channel.UpstreamAccountIdentityRevision
	winnerCredential, err := cipher.Encrypt(`{"access_token":"winner","refresh_token":"winner-refresh"}`)
	if err != nil {
		t.Fatal(err)
	}
	winner := *channel
	winner.PasswordCipher = winnerCredential
	if err := svc.Channels.UpdateTokenCredentialForIdentity(&winner, oldCredential); err != nil {
		t.Fatalf("persist concurrent winner: %v", err)
	}

	conn := &fakeConcurrentTokenRefreshConnector{}
	session, err := svc.EnsureSession(context.Background(), channel, &connector.Channel{}, conn)
	if err != nil {
		t.Fatalf("EnsureSession: %v", err)
	}
	if session.AccessToken != "winner" || session.RefreshToken != "winner-refresh" {
		t.Fatalf("session = %#v, want concurrent winner", session)
	}
	if conn.checkCalls != 3 || conn.refreshCalls != 1 {
		t.Fatalf("connector calls = check:%d refresh:%d", conn.checkCalls, conn.refreshCalls)
	}
	if channel.PasswordCipher != winnerCredential {
		t.Fatal("in-memory channel did not adopt concurrent winner credential")
	}
	stored, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.PasswordCipher != winnerCredential || stored.UpstreamAccountIdentityRevision != identityRevision {
		t.Fatalf("stored channel = %#v, want winner credential with unchanged identity", stored)
	}
	if stored.LastError != "" || stored.NextSyncRetryAt != nil {
		t.Fatalf("credential CAS conflict polluted sync recovery state: %+v", stored)
	}
}

func TestEnsureTokenSessionRejectsConcurrentWinnerAfterIdentityChange(t *testing.T) {
	svc, cipher := testService(t)
	oldCredential, err := cipher.Encrypt(`{"access_token":"old","refresh_token":"old-refresh"}`)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{
		Name: "token-concurrent-identity-change", Type: storage.ChannelTypeSub2API,
		SiteURL: "https://old.example.com", Username: "u", PasswordCipher: oldCredential,
		CredentialMode: storage.CredentialModeToken,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	winnerCredential, err := cipher.Encrypt(`{"access_token":"winner","refresh_token":"winner-refresh"}`)
	if err != nil {
		t.Fatal(err)
	}
	winner := *channel
	winner.PasswordCipher = winnerCredential
	if err := svc.Channels.UpdateTokenCredentialForIdentity(&winner, oldCredential); err != nil {
		t.Fatalf("persist concurrent winner: %v", err)
	}

	conn := &fakeConcurrentTokenIdentityChangeConnector{
		winnerCheckStarted: make(chan struct{}),
		allowWinnerCheck:   make(chan struct{}),
	}
	type ensureResult struct {
		session *connector.AuthSession
		err     error
	}
	result := make(chan ensureResult, 1)
	go func() {
		session, ensureErr := svc.EnsureSession(context.Background(), channel, &connector.Channel{}, conn)
		result <- ensureResult{session: session, err: ensureErr}
	}()
	<-conn.winnerCheckStarted

	replacement, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	replacement.SiteURL = "https://new.example.com"
	if err := svc.Channels.UpdateWithUpstreamAccountIdentityChange(replacement); err != nil {
		t.Fatalf("switch upstream identity: %v", err)
	}
	close(conn.allowWinnerCheck)

	got := <-result
	if got.session != nil || !errors.Is(got.err, storage.ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("EnsureSession result = session %#v err %v, want stale identity", got.session, got.err)
	}
	stored, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SiteURL != replacement.SiteURL || stored.UpstreamAccountIdentityRevision == channel.UpstreamAccountIdentityRevision {
		t.Fatalf("stored identity = site %q revision %d, old revision %d", stored.SiteURL, stored.UpstreamAccountIdentityRevision, channel.UpstreamAccountIdentityRevision)
	}
	if stored.LastError != "" || stored.NextSyncRetryAt != nil {
		t.Fatalf("stale winner polluted replacement recovery state: %+v", stored)
	}
}

func TestEnsureSessionReusesValidSessionAndClearsStaleError(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name:           "reusable",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "user",
		PasswordCipher: "cipher",
		CredentialMode: storage.CredentialModePassword,
		LastError:      "newapi login: status 429",
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := svc.persistSession(channel, &connector.AuthSession{AccessToken: "valid", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	conn := &fakeReusableSessionConnector{}
	if _, err := svc.EnsureSession(context.Background(), channel, &connector.Channel{}, conn); err != nil {
		t.Fatalf("EnsureSession: %v", err)
	}
	if conn.checkCalls != 1 || conn.loginCalls != 0 {
		t.Fatalf("check calls=%d login calls=%d", conn.checkCalls, conn.loginCalls)
	}
	updated, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.LastError != "" {
		t.Fatalf("last error = %q, want empty", updated.LastError)
	}
}

func TestTestLoginKeepsPendingRateSyncRecovery(t *testing.T) {
	svc, cipher := testService(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/user/self" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{}}`))
	}))
	defer server.Close()

	password, err := cipher.Encrypt("password")
	if err != nil {
		t.Fatal(err)
	}
	retryAt := time.Now().Add(5 * time.Minute)
	channel := &storage.Channel{
		Name: "pending-rate-sync", Type: storage.ChannelTypeNewAPI, SiteURL: server.URL,
		Username: "user", PasswordCipher: password, CredentialMode: storage.CredentialModePassword,
		LastError: "倍率采集失败", SyncFailureCount: 1, NextSyncRetryAt: &retryAt,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := svc.persistSession(channel, &connector.AuthSession{AccessToken: "valid", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if err := svc.TestLogin(context.Background(), channel.ID); err != nil {
		t.Fatalf("TestLogin: %v", err)
	}
	updated, err := svc.Channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.LastError != "倍率采集失败" || updated.SyncFailureCount != 1 || updated.NextSyncRetryAt == nil {
		t.Fatalf("login test cleared pending rate recovery: %+v", updated)
	}
}

func TestPrepareTurnstileReportsMissingUpstreamSiteKey(t *testing.T) {
	svc, _ := testService(t)
	conn := &fakeTurnstileConnector{siteKeyErr: connector.ErrTurnstileSiteKeyMissing}
	channel := &storage.Channel{Type: storage.ChannelTypeSub2API, CredentialMode: storage.CredentialModePassword}
	if err := svc.prepareTurnstile(context.Background(), channel, &connector.Channel{}, conn); err == nil || !strings.Contains(err.Error(), "手动登录接管") {
		t.Fatalf("prepareTurnstile error = %v", err)
	}
}

func TestPersistSessionStoresRefreshToken(t *testing.T) {
	svc, _ := testService(t)
	channel := &storage.Channel{
		Name: "persist-session", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.com",
		Username: "user", PasswordCipher: "encrypted", CredentialMode: storage.CredentialModePassword,
	}
	if err := svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	session := &connector.AuthSession{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if err := svc.persistSession(channel, session); err != nil {
		t.Fatalf("persistSession: %v", err)
	}

	saved, err := svc.AuthSessions.FindByChannel(channel.ID)
	if err != nil {
		t.Fatalf("FindByChannel: %v", err)
	}
	if saved == nil {
		t.Fatal("saved session is nil")
	}
	if saved.RefreshTokenCipher == "" {
		t.Fatal("refresh token cipher is empty")
	}

	decrypted, err := svc.decryptSession(saved)
	if err != nil {
		t.Fatalf("decryptSession: %v", err)
	}
	if decrypted.RefreshToken != "refresh" {
		t.Fatalf("refresh token = %q, want refresh", decrypted.RefreshToken)
	}
}

func TestPersistTokenCredentialStoresSub2APIRefreshToken(t *testing.T) {
	svc, cipher := testService(t)
	oldCred, err := cipher.Encrypt(`{"access_token":"old","refresh_token":"old-refresh"}`)
	if err != nil {
		t.Fatalf("encrypt old credential: %v", err)
	}
	ch := &storage.Channel{
		Name:           "sub",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: oldCred,
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}
	if err := svc.Channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	identityRevision := ch.UpstreamAccountIdentityRevision

	err = svc.persistTokenCredential(ch, &connector.AuthSession{
		AccessToken:  "new",
		RefreshToken: "new-refresh",
	})
	if err != nil {
		t.Fatalf("persistTokenCredential: %v", err)
	}

	updated, err := svc.Channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	raw, err := cipher.Decrypt(updated.PasswordCipher)
	if err != nil {
		t.Fatalf("decrypt credential: %v", err)
	}
	var cred Sub2APITokenCredential
	if err := json.Unmarshal([]byte(raw), &cred); err != nil {
		t.Fatalf("unmarshal credential: %v", err)
	}
	if cred.AccessToken != "new" {
		t.Fatalf("access token = %q, want new", cred.AccessToken)
	}
	if cred.RefreshToken != "new-refresh" {
		t.Fatalf("refresh token = %q, want new-refresh", cred.RefreshToken)
	}
	if updated.UpstreamAccountIdentityRevision != identityRevision {
		t.Fatalf("identity revision = %d, want unchanged %d", updated.UpstreamAccountIdentityRevision, identityRevision)
	}
}

func TestClearLoginInfoClearsTokenCredentialAndSession(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt(`{"cookie":"session=abc","user_id":"1"}`)
	if err != nil {
		t.Fatalf("encrypt credential: %v", err)
	}
	ch := &storage.Channel{
		Name:           "newapi-token",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "memo",
		PasswordCipher: enc,
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
		LastError:      "old error",
	}
	if err := svc.Channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := svc.persistSession(ch, &connector.AuthSession{
		AccessToken:  "access",
		RefreshToken: "refresh",
		Cookie:       "session=abc",
		ExpiresAt:    time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("persistSession: %v", err)
	}

	updated, err := svc.ClearLoginInfo(ch.ID)
	if err != nil {
		t.Fatalf("ClearLoginInfo: %v", err)
	}
	if updated.PasswordCipher != "" {
		t.Fatalf("password cipher = %q, want empty", updated.PasswordCipher)
	}
	if updated.LastError != "" {
		t.Fatalf("last error = %q, want empty", updated.LastError)
	}
	saved, err := svc.AuthSessions.FindByChannel(ch.ID)
	if err != nil {
		t.Fatalf("FindByChannel: %v", err)
	}
	if saved != nil {
		t.Fatalf("auth session = %#v, want nil", saved)
	}
	if _, err := svc.buildSessionFromToken(updated); err == nil {
		t.Fatal("buildSessionFromToken after clear succeeded, want error")
	}
}

func TestClearLoginInfoKeepsPasswordCredential(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt("password")
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	ch := &storage.Channel{
		Name:           "password-channel",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: enc,
		CredentialMode: storage.CredentialModePassword,
		MonitorEnabled: true,
		LastError:      "old error",
	}
	if err := svc.Channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := svc.persistSession(ch, &connector.AuthSession{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("persistSession: %v", err)
	}

	updated, err := svc.ClearLoginInfo(ch.ID)
	if err != nil {
		t.Fatalf("ClearLoginInfo: %v", err)
	}
	if updated.PasswordCipher != enc {
		t.Fatal("password credential was cleared in password mode")
	}
	saved, err := svc.AuthSessions.FindByChannel(ch.ID)
	if err != nil {
		t.Fatalf("FindByChannel: %v", err)
	}
	if saved != nil {
		t.Fatalf("auth session = %#v, want nil", saved)
	}
}

func TestCreateAPIKeyRetryAfterIDOnlyResponseRevealFailureDoesNotPostAgain(t *testing.T) {
	var createCalls atomic.Int32
	var revealCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			createCalls.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":9}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/9/key":
			if revealCalls.Add(1) <= 3 {
				http.Error(w, "reveal not ready", http.StatusNotFound)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-real-created"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc := testAPIKeyCreationService(t, srv.URL)
	req := connector.APIKeyCreateRequest{Name: "managed", Group: "default"}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("first create = %#v, %v; want reveal error", key, err)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("create POST calls after first request = %d, want 1", got)
	}

	spec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, req)
	if err != nil {
		t.Fatalf("create operation spec: %v", err)
	}
	operation, err := svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil {
		t.Fatalf("find persisted operation: %v", err)
	}
	if operation.RemoteID == nil || *operation.RemoteID != 9 || operation.Status != storage.ChannelAPIKeyOperationPending {
		t.Fatalf("persisted operation after reveal failure = %#v", operation)
	}

	// A new Service instance exercises process-restart recovery from the same DB.
	restarted := NewService(
		svc.Channels,
		svc.AuthSessions,
		svc.Captchas,
		svc.Rates,
		svc.MonitorLogs,
		svc.Cipher,
	)
	key, err := restarted.CreateAPIKey(context.Background(), 1, req)
	if err != nil {
		t.Fatalf("retry create: %v", err)
	}
	if key == nil || key.ID != 9 || key.Key != "sk-real-created" {
		t.Fatalf("retry key = %#v", key)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("create POST calls after retry = %d, want 1", got)
	}
	if got := revealCalls.Load(); got != 4 {
		t.Fatalf("reveal calls = %d, want 4", got)
	}
	operation, err = restarted.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil {
		t.Fatalf("find completed operation: %v", err)
	}
	if operation.Status != storage.ChannelAPIKeyOperationCompleted || operation.CompletedAt == nil {
		t.Fatalf("completed operation = %#v", operation)
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		t.Fatalf("marshal operation: %v", err)
	}
	if strings.Contains(string(encoded), key.Key) {
		t.Fatal("persisted operation exposed the plaintext API key")
	}
}

func TestCreateAPIKeyRetryRecoversExactFingerprintWithoutPost(t *testing.T) {
	var createCalls atomic.Int32
	var listCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self/groups":
			_, _ = w.Write([]byte(`{"success":true,"data":{"default":{"ratio":1,"desc":""}}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			createCalls.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"data":{}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/search":
			if listCalls.Add(1) <= 2 {
				http.Error(w, "list temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"data": map[string]any{
					"items": []map[string]any{{
						"id": 13, "name": "recover", "status": 1, "group": "default",
						"created_time": time.Now().Unix(), "expired_time": -1, "remain_quota": 0,
						"unlimited_quota": false, "model_limits_enabled": false, "model_limits": "",
						"allow_ips": "", "cross_group_retry": false,
					}},
					"total": 1, "page": 1, "page_size": 100, "pages": 1,
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/13/key":
			_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-recovered"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc := testAPIKeyCreationService(t, srv.URL)
	req := connector.APIKeyCreateRequest{Name: "recover", Group: "default"}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("first create = %#v, %v; want unresolved operation", key, err)
	}
	key, err := svc.CreateAPIKey(context.Background(), 1, req)
	if err != nil {
		t.Fatalf("retry exact recovery: %v", err)
	}
	if key == nil || key.ID != 13 || key.Key != "sk-recovered" {
		t.Fatalf("recovered key = %#v", key)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("create POST calls = %d, want 1", got)
	}
}

func TestCreateAPIKeyRetriesExplicit429AfterCompleteAbsence(t *testing.T) {
	var createCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			if createCalls.Add(1) == 1 {
				http.Error(w, "rate limited", http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":29,"name":"retry-429","group":"default"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/search":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/29/key":
			_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-retried-429"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc := testAPIKeyCreationService(t, srv.URL)
	req := connector.APIKeyCreateRequest{Name: "retry-429", Group: "default"}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("first 429 create = %#v, %v", key, err)
	}
	spec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, req)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil || operation.RejectedPostStatus != http.StatusTooManyRequests || operation.RejectedPostAt == nil {
		t.Fatalf("persisted rejected POST = %#v, %v", operation, err)
	}
	if _, err := svc.APIKeyOps.RecordRejectedPost(operation.ID, operation.OperationGeneration, http.StatusTooManyRequests, time.Now().Add(-channelAPIKeyRejectedPostRetryAfter-time.Minute)); err != nil {
		t.Fatal(err)
	}
	key, err := svc.CreateAPIKey(context.Background(), 1, req)
	if err != nil || key == nil || key.ID != 29 || key.Key != "sk-retried-429" {
		t.Fatalf("retried 429 create = %#v, %v", key, err)
	}
	if got := createCalls.Load(); got != 2 {
		t.Fatalf("explicit 429 create POST calls = %d, want 2", got)
	}
	operation, err = svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil || operation.Status != storage.ChannelAPIKeyOperationCompleted || operation.RejectedRetryCount != 1 || operation.RejectedRetryAt == nil {
		t.Fatalf("completed rejected POST retry = %#v, %v", operation, err)
	}
}

func TestCreateAPIKeyRotatesWhenAuthorized429RetryBecomesCommitUnknown(t *testing.T) {
	var createCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self/groups":
			_, _ = w.Write([]byte(`{"success":true,"data":{"default":{"ratio":1,"desc":""}}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			switch createCalls.Add(1) {
			case 1:
				http.Error(w, "rate limited", http.StatusTooManyRequests)
			case 2:
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			default:
				_, _ = w.Write([]byte(`{"success":true,"data":{"id":30,"name":"retry-unknown","group":"default"}}`))
			}
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/search":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/30/key":
			_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-retry-unknown"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc, db := testAPIKeyCreationServiceWithDB(t, srv.URL)
	store := storage.NewGateway(db)
	if err := store.RecordUpstreamGroupChange(1, "default", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(1, "default")
	if err != nil {
		t.Fatal(err)
	}
	channelRecord, err := svc.Channels.FindByID(1)
	if err != nil {
		t.Fatal(err)
	}
	req := connector.APIKeyCreateRequest{
		Name: "retry-unknown", Group: "default",
		RecoveryFence: &connector.APIKeyCreateRecoveryFence{
			ChannelIdentityRevision: channelRecord.UpstreamAccountIdentityRevision,
			UpstreamGroupID:         generation.ID,
			PresenceGeneration:      generation.PresenceGeneration,
			CanonicalGroupName:      generation.CanonicalGroupName,
		},
	}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("first 429 create = %#v, %v", key, err)
	}
	spec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, req)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil {
		t.Fatal(err)
	}
	rejectedAt := time.Now().Add(-channelAPIKeyRejectedPostRetryAfter - time.Minute)
	if _, err := svc.APIKeyOps.RecordRejectedPost(operation.ID, operation.OperationGeneration, http.StatusTooManyRequests, rejectedAt); err != nil {
		t.Fatal(err)
	}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("authorized retry commit-unknown = %#v, %v", key, err)
	}
	operation, err = svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil || operation.RejectedPostStatus != http.StatusTooManyRequests || operation.RejectedPostAt == nil ||
		operation.RejectedRetryAt == nil || operation.RejectedRetryCount != 1 {
		t.Fatalf("commit-unknown retry state = %#v, %v", operation, err)
	}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("retry absence confirmation = %#v, %v", key, err)
	}
	operation, err = svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil || operation.AbsenceConfirmationCount != 1 {
		t.Fatalf("first retry absence confirmation = %#v, %v", operation, err)
	}
	pastConfirmation := time.Now().Add(-channelAPIKeyAbsenceMinimumInterval - time.Minute)
	if err := db.Model(&storage.ChannelAPIKeyOperation{}).Where("id = ?", operation.ID).
		Updates(map[string]any{
			"absence_first_confirmed_at": pastConfirmation,
			"absence_last_confirmed_at":  pastConfirmation,
		}).Error; err != nil {
		t.Fatal(err)
	}
	key, err := svc.CreateAPIKey(context.Background(), 1, req)
	if err != nil || key == nil || key.ID != 30 || key.Key != "sk-retry-unknown" {
		t.Fatalf("rotated retry create = %#v, %v", key, err)
	}
	if createCalls.Load() != 3 {
		t.Fatalf("create POST calls = %d, want 3", createCalls.Load())
	}
}

func TestCreateAPIKeyCommitUnknownNeverPostsAgain(t *testing.T) {
	var createCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			createCalls.Add(1)
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/search":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc := testAPIKeyCreationService(t, srv.URL)
	req := connector.APIKeyCreateRequest{Name: "commit-unknown", Group: "default"}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("first commit-unknown create = %#v, %v", key, err)
	}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("commit-unknown recovery = %#v, %v", key, err)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("commit-unknown create POST calls = %d, want 1", got)
	}
}

func TestCreateAPIKeyRotatesCommitUnknownAfterFencedCompleteAbsence(t *testing.T) {
	var createCalls atomic.Int32
	var searchCalls atomic.Int32
	var fullInventoryCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self/groups":
			_, _ = w.Write([]byte(`{"success":true,"data":{"default":{"ratio":1,"desc":""}}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			if createCalls.Add(1) == 1 {
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":41,"name":"managed-rotation","group":"default"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/search":
			searchCalls.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/":
			fullInventoryCalls.Add(1)
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/41/key":
			_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-managed-rotation"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc, db := testAPIKeyCreationServiceWithDB(t, srv.URL)
	store := storage.NewGateway(db)
	if err := store.RecordUpstreamGroupChange(1, "default", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(1, "default")
	if err != nil {
		t.Fatal(err)
	}
	channelRecord, err := svc.Channels.FindByID(1)
	if err != nil {
		t.Fatal(err)
	}
	req := connector.APIKeyCreateRequest{
		Name: "managed-rotation", Group: "default",
		RecoveryFence: &connector.APIKeyCreateRecoveryFence{
			ChannelIdentityRevision: channelRecord.UpstreamAccountIdentityRevision,
			UpstreamGroupID:         generation.ID, PresenceGeneration: generation.PresenceGeneration,
			CanonicalGroupName: generation.CanonicalGroupName,
		},
	}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("first commit-unknown create = %#v, %v", key, err)
	}
	if key, err := svc.CreateAPIKey(context.Background(), 1, req); err == nil || key != nil {
		t.Fatalf("first absence confirmation = %#v, %v", key, err)
	}
	spec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, req)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil || operation.AbsenceConfirmationCount != 1 || operation.AbsenceLastConfirmedAt == nil {
		t.Fatalf("persisted first absence confirmation = %#v, %v", operation, err)
	}
	pastConfirmation := time.Now().Add(-channelAPIKeyAbsenceMinimumInterval - time.Minute)
	if err := db.Model(&storage.ChannelAPIKeyOperation{}).Where("id = ?", operation.ID).
		Updates(map[string]any{
			"absence_first_confirmed_at": pastConfirmation,
			"absence_last_confirmed_at":  pastConfirmation,
		}).Error; err != nil {
		t.Fatal(err)
	}
	key, err := svc.CreateAPIKey(context.Background(), 1, req)
	if err != nil || key == nil || key.ID != 41 || key.Key != "sk-managed-rotation" {
		t.Fatalf("rotated create = %#v, %v", key, err)
	}
	if createCalls.Load() != 2 || searchCalls.Load() < 3 || fullInventoryCalls.Load() < 3 {
		t.Fatalf("recovery calls = create:%d search:%d full:%d", createCalls.Load(), searchCalls.Load(), fullInventoryCalls.Load())
	}
	var current storage.ChannelAPIKeyOperation
	if err := db.Where("channel_id = ? AND provider = ? AND scope_hash = ?", 1, storage.ChannelTypeNewAPI, spec.ScopeHash).
		First(&current).Error; err != nil {
		t.Fatal(err)
	}
	var history []storage.ChannelAPIKeyOperationGeneration
	if err := db.Where("operation_id = ?", current.ID).Order("operation_generation ASC").Find(&history).Error; err != nil {
		t.Fatal(err)
	}
	if current.Status != storage.ChannelAPIKeyOperationCompleted || current.OperationGeneration != 2 ||
		len(history) != 1 || history[0].Status != storage.ChannelAPIKeyOperationSuperseded ||
		history[0].OperationGeneration != 1 || history[0].SupersededByGeneration != 2 {
		t.Fatalf("operation lineage = current:%#v history:%#v", current, history)
	}
}

func TestCreateAPIKeyRotatesLegacyNewAPIFingerprintAfterFencedCompleteAbsence(t *testing.T) {
	const managedName = "BOG-p-0123456789abcdef"
	var createCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self/groups":
			_, _ = w.Write([]byte(`{"success":true,"data":{"managed":{"ratio":1,"desc":""}}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/search":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			if createCalls.Add(1) != 1 {
				http.Error(w, "duplicate create", http.StatusInternalServerError)
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":44,"name":"` + managedName + `","group":"managed"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/44/key":
			_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-legacy-fingerprint-rotated"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc, db := testAPIKeyCreationServiceWithDB(t, srv.URL)
	store := storage.NewGateway(db)
	if err := store.RecordUpstreamGroupChange(1, "managed", "added", 0, "", time.Now()); err != nil {
		t.Fatal(err)
	}
	generation, err := store.CaptureUpstreamGroupGeneration(1, "managed")
	if err != nil {
		t.Fatal(err)
	}
	channelRecord, err := svc.Channels.FindByID(1)
	if err != nil {
		t.Fatal(err)
	}
	crossGroupRetry := false
	currentRequest := connector.APIKeyCreateRequest{
		Name: managedName, Group: "managed", CrossGroupRetry: &crossGroupRetry,
		RecoveryFence: &connector.APIKeyCreateRecoveryFence{
			ChannelIdentityRevision: channelRecord.UpstreamAccountIdentityRevision,
			UpstreamGroupID:         generation.ID,
			PresenceGeneration:      generation.PresenceGeneration,
			CanonicalGroupName:      generation.CanonicalGroupName,
		},
	}
	currentSpec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, currentRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(currentSpec.LegacyRecoveryFingerprints) != 1 {
		t.Fatalf("legacy recovery fingerprints = %v", currentSpec.LegacyRecoveryFingerprints)
	}
	legacyRequest := currentRequest
	legacyCrossGroupRetry := true
	legacyRequest.CrossGroupRetry = &legacyCrossGroupRetry
	legacySpec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, legacyRequest)
	if err != nil {
		t.Fatal(err)
	}
	if legacySpec.RequestFingerprint != currentSpec.LegacyRecoveryFingerprints[0] {
		t.Fatalf("legacy fingerprint = %q, want %q", legacySpec.RequestFingerprint, currentSpec.LegacyRecoveryFingerprints[0])
	}
	fence, err := channelAPIKeyOperationRecoveryFence(currentRequest)
	if err != nil {
		t.Fatal(err)
	}
	operation, mayPost, err := svc.APIKeyOps.Begin(storage.ChannelAPIKeyOperationStart{
		ChannelID: 1, Provider: storage.ChannelTypeNewAPI,
		ScopeHash: legacySpec.ScopeHash, RequestFingerprint: legacySpec.RequestFingerprint,
		ManagedName: legacySpec.ManagedName, GroupIdentity: legacySpec.GroupIdentity,
		CustomKeyHash: legacySpec.CustomKeyHash, RecoveryGroupName: legacySpec.Request.Group,
		RecoveryFence: fence,
	})
	if err != nil || !mayPost {
		t.Fatalf("seed legacy operation = %#v mayPost=%v err=%v", operation, mayPost, err)
	}

	if key, err := svc.CreateAPIKey(context.Background(), 1, currentRequest); err == nil || key != nil {
		t.Fatalf("first legacy absence confirmation = %#v, %v", key, err)
	}
	operation, err = svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, currentSpec.ScopeHash)
	if err != nil || operation.AbsenceConfirmationCount != 1 || operation.RequestFingerprint != legacySpec.RequestFingerprint {
		t.Fatalf("first legacy absence state = %#v, %v", operation, err)
	}
	if got := createCalls.Load(); got != 0 {
		t.Fatalf("create POST calls after first confirmation = %d, want 0", got)
	}

	pastConfirmation := time.Now().Add(-channelAPIKeyAbsenceMinimumInterval - time.Minute)
	if err := db.Model(&storage.ChannelAPIKeyOperation{}).Where("id = ?", operation.ID).
		Updates(map[string]any{
			"absence_first_confirmed_at": pastConfirmation,
			"absence_last_confirmed_at":  pastConfirmation,
		}).Error; err != nil {
		t.Fatal(err)
	}
	key, err := svc.CreateAPIKey(context.Background(), 1, currentRequest)
	if err != nil || key == nil || key.ID != 44 || key.Key != "sk-legacy-fingerprint-rotated" {
		t.Fatalf("rotated legacy create = %#v, %v", key, err)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("create POST calls after rotation = %d, want 1", got)
	}
	operation, err = svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, currentSpec.ScopeHash)
	if err != nil || operation.Status != storage.ChannelAPIKeyOperationCompleted || operation.OperationGeneration != 2 ||
		operation.RequestFingerprint != currentSpec.RequestFingerprint {
		t.Fatalf("current legacy rotation operation = %#v, %v", operation, err)
	}
	var history []storage.ChannelAPIKeyOperationGeneration
	if err := db.Where("operation_id = ?", operation.ID).Order("operation_generation ASC").Find(&history).Error; err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Status != storage.ChannelAPIKeyOperationSuperseded ||
		history[0].OperationGeneration != 1 || history[0].RequestFingerprint != legacySpec.RequestFingerprint ||
		history[0].SupersededByGeneration != 2 {
		t.Fatalf("legacy rotation history = %#v", history)
	}
}

func TestCreateAPIKeyConcurrentRequestsOnlyPostOnce(t *testing.T) {
	var createCalls atomic.Int32
	createEntered := make(chan struct{})
	releaseCreate := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self/groups":
			_, _ = w.Write([]byte(`{"success":true,"data":{"default":{"ratio":1,"desc":""}}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/search":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			if createCalls.Add(1) == 1 {
				close(createEntered)
				<-releaseCreate
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":11,"name":"concurrent","group":"default"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/11/key":
			_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-concurrent"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc := testAPIKeyCreationService(t, srv.URL)
	req := connector.APIKeyCreateRequest{Name: "concurrent", Group: "default"}
	type createResult struct {
		key *connector.APIKey
		err error
	}
	firstDone := make(chan createResult, 1)
	secondDone := make(chan createResult, 1)
	go func() {
		key, err := svc.CreateAPIKey(context.Background(), 1, req)
		firstDone <- createResult{key: key, err: err}
	}()
	select {
	case <-createEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("first create did not reach remote POST")
	}
	go func() {
		key, err := svc.CreateAPIKey(context.Background(), 1, req)
		secondDone <- createResult{key: key, err: err}
	}()

	var second createResult
	select {
	case second = <-secondDone:
		close(releaseCreate)
		t.Fatalf("concurrent retry completed before the in-flight create: %#v, %v", second.key, second.err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseCreate)
	var first createResult
	select {
	case first = <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("first create did not finish")
	}
	if first.err != nil || first.key == nil || first.key.Key != "sk-concurrent" {
		t.Fatalf("first create = %#v, %v", first.key, first.err)
	}
	select {
	case second = <-secondDone:
	case <-time.After(3 * time.Second):
		t.Fatal("serialized concurrent retry did not finish")
	}
	if second.err != nil || second.key == nil || second.key.ID != first.key.ID || second.key.Key != first.key.Key {
		t.Fatalf("serialized concurrent retry = %#v, %v; first = %#v", second.key, second.err, first.key)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("concurrent create POST calls = %d, want 1", got)
	}
}

func TestNewAPIKeyRecoveryRejectsMismatchedManualKey(t *testing.T) {
	now := time.Now()
	operation := &storage.ChannelAPIKeyOperation{
		Provider:      storage.ChannelTypeNewAPI,
		CreatedAt:     now.Add(-time.Second),
		ManagedName:   "managed",
		GroupIdentity: "default",
	}
	req := connector.APIKeyCreateRequest{Name: "managed", Group: "default"}
	observed := connector.APIKeyObservedFields{
		Quota: true, UnlimitedQuota: true, ExpiredTime: true, AllowIPs: true,
		ModelLimitsEnabled: true, ModelLimits: true, CrossGroupRetry: true,
	}
	createdAt := now
	item := connector.APIKey{
		ID: 15, Name: "managed", Group: "default", Status: "active", Source: connector.APIKeySourceNewAPI,
		ExpiredTime: -1, CreatedAt: &createdAt, ObservedFields: observed,
	}
	if !channelAPIKeyScopeMatches(storage.ChannelTypeNewAPI, req, item) || !newAPIKeyFingerprintMatches(operation, req, item) {
		t.Fatal("exact managed key did not match its creation fingerprint")
	}
	item.Quota = 1
	if newAPIKeyFingerprintMatches(operation, req, item) {
		t.Fatal("manual key with an explicit quota mismatch was accepted")
	}
	item.Quota = 0
	item.ObservedFields.ModelLimits = false
	if newAPIKeyFingerprintMatches(operation, req, item) {
		t.Fatal("ordinary name recovery accepted an incomplete creation fingerprint")
	}
	item.ObservedFields.ModelLimits = true
	oldCreatedAt := operation.CreatedAt.Add(-time.Minute)
	item.CreatedAt = &oldCreatedAt
	if newAPIKeyFingerprintMatches(operation, req, item) {
		t.Fatal("pre-existing manual key was accepted as the new operation")
	}
}

func TestChannelAPIKeyRecoveryFallsBackToStrictCompleteInventory(t *testing.T) {
	now := time.Now().UTC()
	observed := connector.APIKeyObservedFields{
		Quota: true, UnlimitedQuota: true, ExpiredTime: true, AllowIPs: true,
		ModelLimitsEnabled: true, ModelLimits: true, CrossGroupRetry: true,
	}
	remote := connector.APIKey{
		ID: 51, Name: "managed-full-inventory", Group: "default", GroupName: "default",
		Status: "active", Source: connector.APIKeySourceNewAPI, CreatedAt: &now,
		ExpiredTime: -1, ObservedFields: observed,
	}
	conn := &apiKeyRecoveryInventoryConnector{fullItems: []connector.APIKey{remote}}
	operation := &storage.ChannelAPIKeyOperation{
		Provider: storage.ChannelTypeNewAPI, ManagedName: remote.Name,
		GroupIdentity: "default", CreatedAt: now.Add(-time.Minute),
	}
	req := connector.APIKeyCreateRequest{Name: remote.Name, Group: "default"}
	spec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, req)
	if err != nil {
		t.Fatal(err)
	}
	recovered, _, err := (&Service{}).recoverChannelAPIKeyOperation(context.Background(), nil, conn, nil, operation, spec)
	if err != nil || recovered == nil || recovered.ID != remote.ID {
		t.Fatalf("full-inventory recovery = %#v err=%v", recovered, err)
	}
	if len(conn.queries) != 2 || conn.queries[0].Search != remote.Name || conn.queries[0].RequireCompleteGroupIdentity ||
		conn.queries[1].Search != "" || !conn.queries[1].RequireCompleteGroupIdentity {
		t.Fatalf("recovery inventory queries = %#v", conn.queries)
	}
}

func TestChannelAPIKeyRecoveryFallsBackWhenSearchReturnsMismatchedSameName(t *testing.T) {
	now := time.Now().UTC()
	observed := connector.APIKeyObservedFields{
		Quota: true, UnlimitedQuota: true, ExpiredTime: true, AllowIPs: true,
		ModelLimitsEnabled: true, ModelLimits: true, CrossGroupRetry: true,
	}
	remote := connector.APIKey{
		ID: 52, Name: "managed-search-mismatch", Group: "default", GroupName: "default",
		Status: "active", Source: connector.APIKeySourceNewAPI, CreatedAt: &now,
		ExpiredTime: -1, ObservedFields: observed,
	}
	mismatched := remote
	mismatched.ID = 51
	mismatched.Group = "manual"
	mismatched.GroupName = "manual"
	conn := &apiKeyRecoveryInventoryConnector{
		searchItems: []connector.APIKey{mismatched},
		fullItems:   []connector.APIKey{mismatched, remote},
	}
	operation := &storage.ChannelAPIKeyOperation{
		Provider: storage.ChannelTypeNewAPI, ManagedName: remote.Name,
		GroupIdentity: "default", CreatedAt: now.Add(-time.Minute),
	}
	req := connector.APIKeyCreateRequest{Name: remote.Name, Group: "default"}
	spec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, req)
	if err != nil {
		t.Fatal(err)
	}
	recovered, _, err := (&Service{}).recoverChannelAPIKeyOperation(context.Background(), nil, conn, nil, operation, spec)
	if err != nil || recovered == nil || recovered.ID != remote.ID {
		t.Fatalf("same-name mismatch full-inventory recovery = %#v err=%v", recovered, err)
	}
	if len(conn.queries) != 2 || conn.queries[0].Search != remote.Name || conn.queries[1].Search != "" ||
		!conn.queries[1].RequireCompleteGroupIdentity {
		t.Fatalf("same-name mismatch recovery queries = %#v", conn.queries)
	}
}

func TestChannelAPIKeyRecoveryPageCompleteHonorsProviderPageSize(t *testing.T) {
	fullProviderPage := &connector.APIKeyPage{
		Items:    make([]connector.APIKey, 50),
		Page:     1,
		PageSize: 50,
	}
	complete, err := channelAPIKeyRecoveryPageComplete(1, fullProviderPage)
	if err != nil || complete {
		t.Fatalf("full provider-capped page = complete %v, err %v", complete, err)
	}
	fullProviderPage.Items = fullProviderPage.Items[:49]
	complete, err = channelAPIKeyRecoveryPageComplete(1, fullProviderPage)
	if err != nil || !complete {
		t.Fatalf("short provider-capped page = complete %v, err %v", complete, err)
	}
}

func TestChannelAPIKeyStrictInventoryRejectsCrossPageDriftAndDuplicates(t *testing.T) {
	for _, test := range []struct {
		name  string
		pages map[int]*connector.APIKeyPage
	}{
		{
			name: "pagination drift",
			pages: map[int]*connector.APIKeyPage{
				1: {Items: []connector.APIKey{{ID: 1}}, Page: 1, PageSize: 1, Pages: 2, Total: 2},
				2: {Items: []connector.APIKey{{ID: 2}}, Page: 2, PageSize: 1, Pages: 3, Total: 3},
			},
		},
		{
			name: "duplicate remote id",
			pages: map[int]*connector.APIKeyPage{
				1: {Items: []connector.APIKey{{ID: 1}}, Page: 1, PageSize: 1, Pages: 2, Total: 2},
				2: {Items: []connector.APIKey{{ID: 1}}, Page: 2, PageSize: 1, Pages: 2, Total: 2},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := &pagedAPIKeyRecoveryConnector{pages: test.pages}
			items, err := listChannelAPIKeysForRecovery(context.Background(), nil, conn, nil, "", true)
			if err == nil || items != nil {
				t.Fatalf("strict inventory = %#v, err %v", items, err)
			}
		})
	}
}

func TestSub2APIKeyRecoveryRequiresExplicitSafeConfiguration(t *testing.T) {
	now := time.Now()
	groupID := int64(3)
	operation := &storage.ChannelAPIKeyOperation{
		Provider:  storage.ChannelTypeSub2API,
		CreatedAt: now.Add(-time.Second),
	}
	req := connector.APIKeyCreateRequest{Name: "managed", GroupID: &groupID}
	observed := connector.APIKeyObservedFields{
		Quota: true, ExpiresAt: true, IPWhitelist: true, IPBlacklist: true,
		RateLimit5h: true, RateLimit1d: true, RateLimit7d: true,
	}
	createdAt := now
	item := connector.APIKey{
		ID: 16, Name: "managed", GroupID: &groupID, Status: "active", Source: connector.APIKeySourceSub2API,
		CreatedAt: &createdAt, ObservedFields: observed,
	}
	if !channelAPIKeyScopeMatches(storage.ChannelTypeSub2API, req, item) || !sub2APIKeyFingerprintMatches(operation, req, item) {
		t.Fatal("explicit safe Sub2API key did not match")
	}
	item.ObservedFields.Quota = false
	if sub2APIKeyFingerprintMatches(operation, req, item) {
		t.Fatal("Sub2API recovery accepted an omitted quota field")
	}
	item.ObservedFields.Quota = true
	item.RateLimit5h = 1
	if sub2APIKeyFingerprintMatches(operation, req, item) {
		t.Fatal("Sub2API recovery accepted a nonzero rate limit")
	}
	expiredAt := now.Add(-time.Minute)
	item.RateLimit5h = 0
	item.ExpiresAt = &expiredAt
	if sub2APIKeyFingerprintMatches(operation, req, item) {
		t.Fatal("Sub2API recovery accepted an expired key")
	}
}

func TestNewAPIProvisioningKeyAcceptsOmittedFieldsAndRejectsContradictions(t *testing.T) {
	now := time.Now().UTC()
	operation := &storage.ChannelAPIKeyOperation{
		Provider: storage.ChannelTypeNewAPI, CreatedAt: now,
	}
	req := connector.APIKeyCreateRequest{Name: "BOG-p-0123456789abcdef", Group: "managed"}
	minimal := connector.APIKey{
		ID: 17, Name: req.Name, Status: "active", Source: connector.APIKeySourceNewAPI,
	}
	if !channelAPIKeyReturnedIdentityMatches(operation, req, minimal) {
		t.Fatal("partial provisioning create response was rejected")
	}
	crossGroupRetry := true
	req.CrossGroupRetry = &crossGroupRetry
	normalized := minimal
	normalized.ObservedFields.CrossGroupRetry = true
	if !channelAPIKeyReturnedIdentityMatches(operation, req, normalized) {
		t.Fatal("group-pinned normalization of a provisioning key was rejected")
	}
	manualRequest := req
	manualRequest.Name = "manual"
	normalized.Name = manualRequest.Name
	if channelAPIKeyReturnedIdentityMatches(operation, manualRequest, normalized) {
		t.Fatal("group-pinned normalization was accepted for an ordinary key")
	}
	req.CrossGroupRetry = nil

	contradictions := []struct {
		name   string
		mutate func(*connector.APIKey)
	}{
		{name: "name", mutate: func(item *connector.APIKey) { item.Name = "BOG-p-fedcba9876543210" }},
		{name: "status", mutate: func(item *connector.APIKey) { item.Status = "disabled" }},
		{name: "group", mutate: func(item *connector.APIKey) { item.Group = "other" }},
		{name: "group name", mutate: func(item *connector.APIKey) { item.GroupName = "other" }},
		{name: "quota", mutate: func(item *connector.APIKey) { item.Quota = 1; item.ObservedFields.Quota = true }},
		{name: "unlimited quota", mutate: func(item *connector.APIKey) { item.UnlimitedQuota = true; item.ObservedFields.UnlimitedQuota = true }},
		{name: "expired time", mutate: func(item *connector.APIKey) { item.ExpiredTime = 0; item.ObservedFields.ExpiredTime = true }},
		{name: "model limits enabled", mutate: func(item *connector.APIKey) {
			item.ModelLimitsEnabled = true
			item.ObservedFields.ModelLimitsEnabled = true
		}},
		{name: "model limits", mutate: func(item *connector.APIKey) { item.ModelLimits = "gpt-test"; item.ObservedFields.ModelLimits = true }},
		{name: "allow ips", mutate: func(item *connector.APIKey) { item.AllowIPs = "127.0.0.1"; item.ObservedFields.AllowIPs = true }},
		{name: "cross group retry", mutate: func(item *connector.APIKey) { item.CrossGroupRetry = true; item.ObservedFields.CrossGroupRetry = true }},
		{name: "explicit null", mutate: func(item *connector.APIKey) { item.ObservedFields.Quota = true; item.NullFields.Quota = true }},
		{name: "source", mutate: func(item *connector.APIKey) { item.Source = connector.APIKeySourceSub2API }},
		{name: "old creation", mutate: func(item *connector.APIKey) { createdAt := now.Add(-time.Minute); item.CreatedAt = &createdAt }},
	}
	for _, test := range contradictions {
		t.Run(test.name, func(t *testing.T) {
			item := minimal
			test.mutate(&item)
			if channelAPIKeyReturnedIdentityMatches(operation, req, item) {
				t.Fatal("contradictory successful create response was accepted")
			}
		})
	}
}

func TestNewAPICreateResponseIdentityAcceptsOnlyOmittedFields(t *testing.T) {
	req := connector.APIKeyCreateRequest{Name: "BOG-p-0123456789abcdef", Group: "managed"}
	minimal := connector.APIKey{ID: 17, Status: "unknown", Source: connector.APIKeySourceNewAPI}
	if !channelAPIKeyCreateResponseIdentityMatches(storage.ChannelTypeNewAPI, req, minimal) {
		t.Fatal("ID-only NewAPI create response was rejected")
	}
	contradictions := []connector.APIKey{
		{ID: 17, Name: "BOG-p-fedcba9876543210", Status: "unknown", Source: connector.APIKeySourceNewAPI},
		{ID: 17, Group: "other", Status: "unknown", Source: connector.APIKeySourceNewAPI},
		{ID: 17, Status: "disabled", Source: connector.APIKeySourceNewAPI},
		{ID: 17, Status: "unknown", Source: connector.APIKeySourceSub2API},
		{ID: 17, Status: "unknown", Source: connector.APIKeySourceNewAPI, ObservedIdentityFields: connector.APIKeyIdentityFields{Status: true}},
		{ID: 17, Source: connector.APIKeySourceNewAPI, ObservedIdentityFields: connector.APIKeyIdentityFields{Name: true}},
		{ID: 17, Status: "unknown", Source: connector.APIKeySourceNewAPI, ObservedIdentityFields: connector.APIKeyIdentityFields{Group: true}},
		{ID: 17, Status: "unknown", Source: connector.APIKeySourceNewAPI, NullIdentityFields: connector.APIKeyIdentityFields{Name: true}},
		{ID: 17, Status: "unknown", Source: connector.APIKeySourceNewAPI, NullIdentityFields: connector.APIKeyIdentityFields{Group: true}},
		{ID: 17, Status: "unknown", Source: connector.APIKeySourceNewAPI, NullIdentityFields: connector.APIKeyIdentityFields{Status: true}},
	}
	for _, item := range contradictions {
		if channelAPIKeyCreateResponseIdentityMatches(storage.ChannelTypeNewAPI, req, item) {
			t.Fatalf("contradictory NewAPI create response was accepted: %#v", item)
		}
	}
}

func TestNewAPIIncompleteFingerprintRequiresPersistedEvidence(t *testing.T) {
	item := connector.APIKey{
		ID: 21, Name: "ordinary", Group: "managed", Status: "active", Source: connector.APIKeySourceNewAPI,
	}
	req := connector.APIKeyCreateRequest{Name: item.Name, Group: item.Group}
	remoteID := item.ID
	bound := &storage.ChannelAPIKeyOperation{
		Provider: storage.ChannelTypeNewAPI, CreatedAt: time.Now().UTC(), RemoteID: &remoteID,
	}
	if !newAPIKeyFingerprintMatches(bound, req, item) {
		t.Fatal("bound remote ID did not allow omitted fields")
	}
	wrongRemoteID := remoteID + 1
	bound.RemoteID = &wrongRemoteID
	if newAPIKeyFingerprintMatches(bound, req, item) {
		t.Fatal("another bound remote ID allowed omitted fields")
	}

	custom := &storage.ChannelAPIKeyOperation{
		Provider: storage.ChannelTypeNewAPI, CreatedAt: time.Now().UTC(), CustomKeyHash: channelAPIKeyHash("sk-expected"),
	}
	item.Key = "sk-expected"
	if !channelAPIKeyReturnedIdentityMatches(custom, req, item) {
		t.Fatal("matching returned custom key did not allow omitted fields")
	}
	item.Key = "sk-other"
	if channelAPIKeyReturnedIdentityMatches(custom, req, item) {
		t.Fatal("mismatched returned custom key allowed binding")
	}
}

func TestNewAPIOrdinaryAndForgedProvisioningNamesRequireCompleteFingerprint(t *testing.T) {
	operation := &storage.ChannelAPIKeyOperation{Provider: storage.ChannelTypeNewAPI, CreatedAt: time.Now().UTC()}
	for _, name := range []string{
		"ordinary", "BOG-p-0123456789abc", "BOG-p-0123456789abcdef0",
		"BOG-p-0123456789abcdeg", "BOG-p-0123456789ABCDEF", "prefix-BOG-p-0123456789abcdef",
	} {
		req := connector.APIKeyCreateRequest{Name: name, Group: "managed"}
		item := connector.APIKey{ID: 18, Name: name, Group: req.Group, Status: "active", Source: connector.APIKeySourceNewAPI}
		if channelAPIKeyReturnedIdentityMatches(operation, req, item) {
			t.Fatalf("partial fingerprint accepted for name %q", name)
		}
	}
}

func TestNewAPIProvisioningKeyRecoveryRejectsDuplicateCandidates(t *testing.T) {
	operation := &storage.ChannelAPIKeyOperation{Provider: storage.ChannelTypeNewAPI, CreatedAt: time.Now().UTC()}
	req := connector.APIKeyCreateRequest{Name: "BOG-p-0123456789abcdef", Group: "managed"}
	spec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, req)
	if err != nil {
		t.Fatal(err)
	}
	items := []connector.APIKey{
		{ID: 19, Name: req.Name, Group: req.Group, Status: "active", Source: connector.APIKeySourceNewAPI},
		{ID: 20, Name: req.Name, Group: req.Group, Status: "active", Source: connector.APIKeySourceNewAPI},
	}
	if recovered, _, err := selectChannelAPIKeyRecoveryCandidate(context.Background(), nil, &apiKeyRecoveryInventoryConnector{}, nil, operation, spec, items); !errors.Is(err, errChannelAPIKeyRecoveryMismatch) || recovered != nil {
		t.Fatalf("duplicate recovery candidates = %#v, %v", recovered, err)
	}
}

func TestNewAPIGenerationTwoRecoversPartialProvisioningKeyWithoutPost(t *testing.T) {
	const managedName = "BOG-p-0123456789abcdef"
	var createCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":1}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/token/search":
			_, _ = w.Write([]byte(`{"success":true,"data":{"items":[{"id":31,"name":"` + managedName + `","status":1,"group":"managed"}],"total":1,"page":1,"page_size":100,"pages":1}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/31/key":
			_, _ = w.Write([]byte(`{"success":true,"data":{"key":"sk-generation-two"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/token/":
			createCalls.Add(1)
			http.Error(w, "unexpected duplicate create", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	svc, db := testAPIKeyCreationServiceWithDB(t, srv.URL)
	req := connector.APIKeyCreateRequest{Name: managedName, Group: "managed"}
	spec, err := makeChannelAPIKeyCreateSpec(storage.ChannelTypeNewAPI, req)
	if err != nil {
		t.Fatal(err)
	}
	operation, mayPost, err := svc.APIKeyOps.Begin(storage.ChannelAPIKeyOperationStart{
		ChannelID: 1, Provider: storage.ChannelTypeNewAPI, ScopeHash: spec.ScopeHash,
		RequestFingerprint: spec.RequestFingerprint, ManagedName: spec.ManagedName,
		GroupIdentity: spec.GroupIdentity, CustomKeyHash: spec.CustomKeyHash,
	})
	if err != nil || !mayPost {
		t.Fatalf("seed operation = %#v mayPost=%v err=%v", operation, mayPost, err)
	}
	if err := db.Model(&storage.ChannelAPIKeyOperation{}).Where("id = ?", operation.ID).
		Update("operation_generation", 2).Error; err != nil {
		t.Fatal(err)
	}

	key, err := svc.CreateAPIKey(context.Background(), 1, req)
	if err != nil || key == nil || key.ID != 31 || key.Key != "sk-generation-two" {
		t.Fatalf("generation two recovery = %#v, %v", key, err)
	}
	if createCalls.Load() != 0 {
		t.Fatalf("generation two recovery POST calls = %d, want 0", createCalls.Load())
	}
	operation, err = svc.APIKeyOps.Find(1, storage.ChannelTypeNewAPI, spec.ScopeHash)
	if err != nil || operation.Status != storage.ChannelAPIKeyOperationCompleted ||
		operation.OperationGeneration != 2 || operation.RemoteID == nil || *operation.RemoteID != 31 {
		t.Fatalf("completed generation two operation = %#v, %v", operation, err)
	}
}
