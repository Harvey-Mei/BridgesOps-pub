package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm/clause"
)

func TestChannelAdminUpdateRejectsFieldsOutsideWhitelist(t *testing.T) {
	channel := &Channel{ID: 1, UpstreamAccountIdentityRevision: 1, AdminRevision: 1}
	_, _, _, err := channelAdminFieldValue(channel, ChannelAdminField("password_cipher = 'overwritten'"))
	if err == nil || !strings.Contains(err.Error(), "not writable") {
		t.Fatalf("unknown administrator field error = %v", err)
	}
}

func TestChannelAdminIdentityFieldsCoverEveryAccountSelectingField(t *testing.T) {
	base := Channel{
		SiteURL: "https://www.example.com", APIURL: "https://api.example.com", Username: "user",
		PasswordCipher: "credential-a", CredentialMode: CredentialModePassword,
		LoginExtraParams: `{"tenant":"a"}`, Name: "name-a", MonitorEnabled: true,
	}
	tests := []struct {
		name   string
		field  ChannelAdminField
		change func(*Channel)
		want   bool
	}{
		{name: "homepage", field: ChannelAdminFieldSiteURL, change: func(c *Channel) { c.SiteURL = "https://other.example.com" }, want: true},
		{name: "api origin", field: ChannelAdminFieldAPIURL, change: func(c *Channel) { c.APIURL = "https://other-api.example.com" }, want: true},
		{name: "username", field: ChannelAdminFieldUsername, change: func(c *Channel) { c.Username = "other-user" }, want: true},
		{name: "credential", field: ChannelAdminFieldPasswordCipher, change: func(c *Channel) { c.PasswordCipher = "credential-b" }, want: true},
		{name: "credential mode", field: ChannelAdminFieldCredentialMode, change: func(c *Channel) { c.CredentialMode = CredentialModeToken }, want: true},
		{name: "login params", field: ChannelAdminFieldLoginExtraParams, change: func(c *Channel) { c.LoginExtraParams = `{"tenant":"b"}` }, want: true},
		{name: "display-only", field: ChannelAdminFieldName, change: func(c *Channel) { c.Name = "name-b" }, want: false},
		{name: "monitor-only", field: ChannelAdminFieldMonitorEnabled, change: func(c *Channel) { c.MonitorEnabled = false }, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			after := base
			test.change(&after)
			if got := channelAdminIdentityFieldChanged(&base, &after, test.field); got != test.want {
				t.Fatalf("channelAdminIdentityFieldChanged = %v, want %v", got, test.want)
			}
		})
	}
}

func TestChannelAdminUpdateRejectsStaleNonIdentityEditor(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "stale-editor", Type: ChannelTypeSub2API,
		SiteURL: "https://old.example.com", APIURL: "https://old-api.example.com", Username: "old-user",
		PasswordCipher: "old-cipher", CredentialMode: CredentialModePassword, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	stale, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}

	fresh.SiteURL = "https://new.example.com"
	fresh.APIURL = "https://new-api.example.com"
	fresh.Username = "new-user"
	fresh.PasswordCipher = "new-cipher"
	if err := channels.UpdateWithUpstreamAccountIdentityChange(fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.UpstreamAccountIdentityRevision != stale.UpstreamAccountIdentityRevision+1 {
		t.Fatalf("identity revision = %d, want %d", fresh.UpstreamAccountIdentityRevision, stale.UpstreamAccountIdentityRevision+1)
	}

	stale.Name = "stale-rename-must-fail"
	if err := channels.UpdateAdminFields(stale, ChannelAdminFieldName); !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("stale non-identity edit error = %v", err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != "stale-editor" ||
		stored.SiteURL != fresh.SiteURL || stored.APIURL != fresh.APIURL ||
		stored.Username != fresh.Username || stored.PasswordCipher != fresh.PasswordCipher ||
		stored.UpstreamAccountIdentityRevision != fresh.UpstreamAccountIdentityRevision {
		t.Fatalf("stale editor overwrote current channel: %#v", stored)
	}
}

func TestTokenCredentialRotationPreservesIdentityFactsAndFencesStaleRefresh(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "token-rotation", Type: ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "display",
		PasswordCipher: "old-token", CredentialMode: CredentialModeToken, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	stale := *channel
	stateBefore, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatalf("read initial cost state: %v", err)
	}

	expectedCredential := channel.PasswordCipher
	channel.PasswordCipher = "rotated-token"
	if err := channels.UpdateTokenCredentialForIdentity(channel, expectedCredential); err != nil {
		t.Fatalf("rotate token credential: %v", err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.PasswordCipher != "rotated-token" {
		t.Fatalf("password cipher = %q, want rotated-token", stored.PasswordCipher)
	}
	if stored.UpstreamAccountIdentityRevision != stale.UpstreamAccountIdentityRevision {
		t.Fatalf("identity revision = %d, want unchanged %d", stored.UpstreamAccountIdentityRevision, stale.UpstreamAccountIdentityRevision)
	}
	stateAfter, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatalf("read rotated cost state: %v", err)
	}
	if stateAfter.Version != stateBefore.Version {
		t.Fatalf("cost state version = %d, want unchanged %d", stateAfter.Version, stateBefore.Version)
	}

	stale.PasswordCipher = "late-token"
	if err := channels.UpdateTokenCredentialForIdentity(&stale, "old-token"); !errors.Is(err, ErrStaleChannelTokenCredential) {
		t.Fatalf("stale token refresh error = %v", err)
	}
	stored, err = channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.PasswordCipher != "rotated-token" {
		t.Fatalf("stale refresh overwrote token credential: %q", stored.PasswordCipher)
	}
}

func TestChannelAdminUpdateRejectsStaleFullFormPayload(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "full-form", Type: ChannelTypeSub2API, GatewayApp: GatewayAppCodex,
		SiteURL: "https://example.com", Username: "user", SortOrder: 1,
		PasswordCipher: "cipher", CredentialMode: CredentialModePassword,
		MonitorEnabled: true, BalanceThreshold: MinimumChannelBalanceThreshold,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	staleForm, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	concurrentEditor, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	concurrentEditor.MonitorEnabled = false
	if err := channels.UpdateAdminFields(concurrentEditor, ChannelAdminFieldMonitorEnabled); err != nil {
		t.Fatal(err)
	}
	if concurrentEditor.AdminRevision != staleForm.AdminRevision+1 {
		t.Fatalf("admin revision = %d, want %d", concurrentEditor.AdminRevision, staleForm.AdminRevision+1)
	}

	// This is the legacy browser shape: every visible field is resubmitted even
	// though the operator changed only the name. The admin revision must reject
	// the old monitor/proxy/sort snapshot instead of treating it as a patch.
	staleForm.Name = "stale-full-form-name"
	err = channels.UpdateAdminFields(staleForm,
		ChannelAdminFieldName,
		ChannelAdminFieldGatewayApp,
		ChannelAdminFieldSiteURL,
		ChannelAdminFieldAPIURL,
		ChannelAdminFieldUsername,
		ChannelAdminFieldSortOrder,
		ChannelAdminFieldCredentialMode,
		ChannelAdminFieldLoginExtraParams,
		ChannelAdminFieldTurnstileEnabled,
		ChannelAdminFieldIgnoreAnnouncements,
		ChannelAdminFieldSubscriptionEnabled,
		ChannelAdminFieldNetworkProxyID,
		ChannelAdminFieldProxyEnabled,
		ChannelAdminFieldCaptchaConfigID,
		ChannelAdminFieldBalanceThreshold,
		ChannelAdminFieldMonitorEnabled,
	)
	if !errors.Is(err, ErrStaleChannelAdminRevision) {
		t.Fatalf("stale full-form error = %v", err)
	}
	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != channel.Name || stored.MonitorEnabled || stored.AdminRevision != concurrentEditor.AdminRevision {
		t.Fatalf("stale full form overwrote concurrent edit: %#v", stored)
	}
}

func TestChannelIdentityEditPreservesConcurrentNonIdentityPatch(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	channel := &Channel{
		Name: "before-concurrent-edit", Type: ChannelTypeSub2API,
		SiteURL: "https://old.example.com", APIURL: "https://old-api.example.com", Username: "user",
		PasswordCipher: "cipher", CredentialMode: CredentialModePassword, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	identityEditor, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	nonIdentityEditor, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}

	nonIdentityEditor.Name = "concurrent-name"
	if err := channels.UpdateAdminFields(nonIdentityEditor, ChannelAdminFieldName); err != nil {
		t.Fatal(err)
	}
	identityEditor.SiteURL = "https://new.example.com"
	identityEditor.APIURL = ""
	if err := channels.UpdateAdminFields(identityEditor, ChannelAdminFieldSiteURL, ChannelAdminFieldAPIURL); err != nil {
		t.Fatal(err)
	}

	stored, err := channels.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Name != nonIdentityEditor.Name || stored.SiteURL != identityEditor.SiteURL || stored.APIURL != "" {
		t.Fatalf("disjoint administrator patches overwrote each other: %#v", stored)
	}
	if stored.UpstreamAccountIdentityRevision != channel.UpstreamAccountIdentityRevision+1 {
		t.Fatalf("identity revision = %d, want %d", stored.UpstreamAccountIdentityRevision, channel.UpstreamAccountIdentityRevision+1)
	}
}

func TestInvalidateAdminAuthStateFencesLatePasswordLogin(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	sessions := NewAuthSessions(db)
	channel := &Channel{
		Name: "clear-login-fence", Type: ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "user", PasswordCipher: "cipher",
		CredentialMode: CredentialModePassword, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	oldIdentityRevision := channel.UpstreamAccountIdentityRevision
	oldAdminRevision := channel.AdminRevision
	if err := sessions.UpsertForChannelIdentityRevision(channel.ID, oldIdentityRevision, &AuthSession{
		ChannelID: channel.ID, AccessTokenCipher: "old-session",
	}); err != nil {
		t.Fatal(err)
	}
	if err := channels.InvalidateAdminAuthState(channel); err != nil {
		t.Fatal(err)
	}
	if channel.UpstreamAccountIdentityRevision != oldIdentityRevision+1 || channel.AdminRevision != oldAdminRevision+1 {
		t.Fatalf("clear-login revisions: identity=%d admin=%d", channel.UpstreamAccountIdentityRevision, channel.AdminRevision)
	}
	if saved, err := sessions.FindByChannel(channel.ID); err != nil || saved != nil {
		t.Fatalf("cleared session = %#v err=%v", saved, err)
	}
	err := sessions.UpsertForChannelIdentityRevision(channel.ID, oldIdentityRevision, &AuthSession{
		ChannelID: channel.ID, AccessTokenCipher: "late-old-session",
	})
	if !errors.Is(err, ErrStaleChannelAccountIdentityRevision) {
		t.Fatalf("late password login error = %v", err)
	}
	if saved, err := sessions.FindByChannel(channel.ID); err != nil || saved != nil {
		t.Fatalf("late password login recreated session = %#v err=%v", saved, err)
	}
}

func TestChannelAdminUpdateLocksChannelBeforeTargetProxy(t *testing.T) {
	db := openTestDB(t)
	updateDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatal(err)
	}
	probeDB, err := testdb.ReopenWithError(t, db)
	if err != nil {
		t.Fatal(err)
	}
	proxies := NewNetworkProxies(db)
	proxy := &NetworkProxy{Name: "channel-lock-order", Protocol: "http", Host: "proxy.example", Port: 8080, Enabled: true}
	if err := proxies.Create(proxy); err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	channel := &Channel{
		Name: "channel-lock-order", Type: ChannelTypeNewAPI, SiteURL: "https://example.com",
		Username: "user", PasswordCipher: "cipher", CredentialMode: CredentialModePassword, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}

	proxyTx := db.Begin()
	if proxyTx.Error != nil {
		t.Fatal(proxyTx.Error)
	}
	proxyReleased := false
	if err := lockNetworkProxyForBinding(proxyTx, &proxy.ID); err != nil {
		_ = proxyTx.Rollback().Error
		t.Fatal(err)
	}

	updated := *channel
	updated.NetworkProxyID = &proxy.ID
	updated.ProxyEnabled = true
	updateDone := make(chan error, 1)
	go func() {
		updateDone <- NewChannels(updateDB).UpdateAdminFields(
			&updated,
			ChannelAdminFieldNetworkProxyID,
			ChannelAdminFieldProxyEnabled,
		)
	}()
	updateFinished := false
	defer func() {
		if !proxyReleased {
			_ = proxyTx.Rollback().Error
		}
		if !updateFinished {
			select {
			case <-updateDone:
			case <-time.After(3 * time.Second):
			}
		}
	}()

	// The target proxy is deliberately unavailable. A correct updater still
	// owns the channel row while it waits; the former Save path waited on the
	// proxy first and left this NOWAIT lock available.
	lockedChannelFirst := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		probeTx := probeDB.Begin()
		if probeTx.Error != nil {
			t.Fatal(probeTx.Error)
		}
		var probe Channel
		err := probeTx.Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).
			Select("id").First(&probe, channel.ID).Error
		_ = probeTx.Rollback().Error
		if err == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !isPostgresLockNotAvailable(err) {
			t.Fatalf("probe channel lock: %v", err)
		}
		lockedChannelFirst = true
		break
	}
	if !lockedChannelFirst {
		t.Fatal("channel update waited on the proxy before locking the channel row")
	}

	if err := proxyTx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	proxyReleased = true
	select {
	case err := <-updateDone:
		updateFinished = true
		if err != nil {
			t.Fatalf("channel update after proxy release: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("channel update did not finish after target proxy was released")
	}
}

func isPostgresLockNotAvailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}
