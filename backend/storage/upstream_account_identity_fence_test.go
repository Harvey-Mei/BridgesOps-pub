package storage

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestUpstreamAccountIdentityObservationFencesAdminAndAPIOrigin(t *testing.T) {
	db := openTestDB(t)
	repo := NewChannels(db)
	channel := &Channel{
		Name: "identity-observation-fences", Type: ChannelTypeSub2API,
		SiteURL: "https://site.example.test/home", APIURL: "https://api.example.test/v1",
		Username: "user", PasswordCipher: "cipher", CredentialMode: CredentialModePassword,
	}
	if err := repo.Create(channel); err != nil {
		t.Fatal(err)
	}
	base := UpstreamAccountIdentityObservation{
		ChannelID: channel.ID, ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channel.AdminRevision, Provider: ChannelTypeSub2API,
		APIOrigin: "https://user:secret@API.Example.test:443/auth/me?token=hidden",
		AccountID: "account-a", Limit: upstreamIdentityIntPointer(2),
		Scope: UpstreamConcurrencyScopeAccount, Source: "sub2api.auth.me",
		ObservedAt: time.Date(2026, 7, 29, 10, 0, 0, 123456000, time.UTC),
	}

	wrongOrigin := base
	wrongOrigin.APIOrigin = channel.SiteURL
	if _, err := repo.ObserveUpstreamAccountIdentity(wrongOrigin); !errors.Is(err, ErrStaleUpstreamAccountObservation) {
		t.Fatalf("wrong API origin error = %v", err)
	}
	wrongAdmin := base
	wrongAdmin.ExpectedAdminRevision++
	if _, err := repo.ObserveUpstreamAccountIdentity(wrongAdmin); !errors.Is(err, ErrStaleUpstreamAccountObservation) {
		t.Fatalf("wrong admin revision error = %v", err)
	}

	snapshot, err := repo.ObserveUpstreamAccountIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ChannelIdentityRevision != channel.UpstreamAccountIdentityRevision {
		t.Fatalf("identity revision = %d, want %d", snapshot.ChannelIdentityRevision, channel.UpstreamAccountIdentityRevision)
	}

	conflict := base
	conflict.Limit = upstreamIdentityIntPointer(3)
	if _, err := repo.ObserveUpstreamAccountIdentity(conflict); !errors.Is(err, ErrConflictingUpstreamAccountObservation) {
		t.Fatalf("equal-time concurrency conflict error = %v", err)
	}
	missingAdmin := base
	missingAdmin.ExpectedAdminRevision = 0
	if _, err := repo.ObserveUpstreamAccountIdentity(missingAdmin); err == nil {
		t.Fatal("observation without an administrator revision succeeded")
	}
	missingScope := base
	missingScope.Scope = ""
	if _, err := repo.ObserveUpstreamAccountIdentity(missingScope); err == nil {
		t.Fatal("observation without explicit account scope succeeded")
	}

	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).UpdateColumn("admin_revision", channel.AdminRevision+1).Error; err != nil {
		t.Fatal(err)
	}
	late := base
	late.ObservedAt = base.ObservedAt.Add(time.Minute)
	if _, err := repo.ObserveUpstreamAccountIdentity(late); !errors.Is(err, ErrStaleUpstreamAccountObservation) {
		t.Fatalf("pre-admin-update observation error = %v", err)
	}
	late.ExpectedAdminRevision++
	if _, err := repo.ObserveUpstreamAccountIdentity(late); err != nil {
		t.Fatalf("current admin observation error = %v", err)
	}
	var audits int64
	if err := db.Model(&GatewayChannelUpstreamAccountBindingAudit{}).Where("channel_id = ?", channel.ID).Count(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("same-account refresh appended %d binding audits, want 1 initial binding", audits)
	}
}

func TestUpstreamAccountIdentityRejectsBigintRevisionOverflow(t *testing.T) {
	db := openTestDB(t)
	repo := NewChannels(db)
	channel := &Channel{
		Name: "identity-observation-overflow", Type: ChannelTypeSub2API,
		SiteURL: "https://api.example.test", Username: "user", PasswordCipher: "cipher",
	}
	if err := repo.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&Channel{}).Where("id = ?", channel.ID).
		UpdateColumn("upstream_account_identity_revision", int64(math.MaxInt64)).Error; err != nil {
		t.Fatal(err)
	}
	observation := UpstreamAccountIdentityObservation{
		ChannelID: channel.ID, ExpectedIdentityRevision: uint64(math.MaxInt64),
		ExpectedAdminRevision: channel.AdminRevision, Provider: ChannelTypeSub2API,
		APIOrigin: channel.SiteURL, AccountID: "account-a", Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me", ObservedAt: time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC),
	}
	if _, err := repo.ObserveUpstreamAccountIdentity(observation); err != nil {
		t.Fatalf("first max-revision binding error = %v", err)
	}
	observation.AccountID = "account-b"
	observation.ObservedAt = observation.ObservedAt.Add(time.Minute)
	if _, err := repo.ObserveUpstreamAccountIdentity(observation); !errors.Is(err, ErrUpstreamAccountIdentityRevisionExhausted) {
		t.Fatalf("overflow switch error = %v", err)
	}
	var accounts int64
	if err := db.Model(&GatewayUpstreamAccount{}).Count(&accounts).Error; err != nil {
		t.Fatal(err)
	}
	if accounts != 1 {
		t.Fatalf("overflow switch left %d account records, want 1", accounts)
	}
}

func TestUpstreamAccountIdentitySwitchRollsBackWhenAuditInsertFails(t *testing.T) {
	db := openTestDB(t)
	repo := NewChannels(db)
	channel := &Channel{
		Name: "identity-observation-rollback", Type: ChannelTypeSub2API,
		SiteURL: "https://api.example.test", Username: "user", PasswordCipher: "cipher",
	}
	if err := repo.Create(channel); err != nil {
		t.Fatal(err)
	}
	base := UpstreamAccountIdentityObservation{
		ChannelID: channel.ID, ExpectedIdentityRevision: channel.UpstreamAccountIdentityRevision,
		ExpectedAdminRevision: channel.AdminRevision, Provider: ChannelTypeSub2API,
		APIOrigin: channel.SiteURL, AccountID: "account-a", Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me", ObservedAt: time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC),
	}
	if _, err := repo.ObserveUpstreamAccountIdentity(base); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelCostState{}).Where("channel_id = ?", channel.ID).Updates(map[string]any{
		"raw_balance": 17, "actual_inventory": 11, "raw_total_cost": 9, "actual_total_cost": 7,
		"funding_factor": 0.7, "version": 4, "source": "advertised",
	}).Error; err != nil {
		t.Fatal(err)
	}
	beforeState, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	beforeChannel, err := repo.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	key := &GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "rollback", APIKeyCipher: "cipher", Enabled: true,
		ModelFamilies: "codex", ModelIDs: "model-a",
	}
	if err := db.Create(key).Error; err != nil {
		t.Fatal(err)
	}
	announcement := &UpstreamAnnouncement{
		ChannelID: channel.ID, SourceKey: "rollback", Content: "old account",
		FirstSeenAt: base.ObservedAt,
	}
	if err := db.Create(announcement).Error; err != nil {
		t.Fatal(err)
	}
	var beforeBinding GatewayChannelUpstreamAccountBinding
	if err := db.Where("channel_id = ?", channel.ID).Take(&beforeBinding).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`
		CREATE FUNCTION test_reject_identity_union_audit() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test identity audit failure'; END $$;
		CREATE TRIGGER test_reject_identity_union_audit
		BEFORE INSERT ON gateway_channel_upstream_account_binding_audits
		FOR EACH ROW EXECUTE FUNCTION test_reject_identity_union_audit()
	`).Error; err != nil {
		t.Fatal(err)
	}

	changed := base
	changed.AccountID = "account-b"
	changed.ObservedAt = base.ObservedAt.Add(time.Minute)
	if _, err := repo.ObserveUpstreamAccountIdentity(changed); err == nil || !strings.Contains(err.Error(), "test identity audit failure") {
		t.Fatalf("audit failure switch error = %v", err)
	}
	stored, err := repo.FindByID(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	afterState, err := NewCosting(db).FindState(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	var afterBinding GatewayChannelUpstreamAccountBinding
	if err := db.Where("channel_id = ?", channel.ID).Take(&afterBinding).Error; err != nil {
		t.Fatal(err)
	}
	var afterKey GatewayUpstreamKey
	if err := db.First(&afterKey, key.ID).Error; err != nil {
		t.Fatal(err)
	}
	var announcements int64
	if err := db.Model(&UpstreamAnnouncement{}).Where("channel_id = ?", channel.ID).Count(&announcements).Error; err != nil {
		t.Fatal(err)
	}
	if stored.UpstreamAccountIdentityRevision != beforeChannel.UpstreamAccountIdentityRevision || stored.CostUnitEpoch != beforeChannel.CostUnitEpoch ||
		afterBinding.UpstreamAccountID != beforeBinding.UpstreamAccountID || afterBinding.ChannelIdentityRevision != beforeBinding.ChannelIdentityRevision ||
		afterState.Version != beforeState.Version || afterState.RawBalance != beforeState.RawBalance || afterState.RawTotalCost != beforeState.RawTotalCost ||
		!afterKey.Enabled || afterKey.ModelIDs != key.ModelIDs || announcements != 1 {
		t.Fatalf("failed switch partially committed: channel=%+v binding=%+v state=%+v key=%+v announcements=%d", stored, afterBinding, afterState, afterKey, announcements)
	}
	var accounts, audits int64
	if err := db.Model(&GatewayUpstreamAccount{}).Count(&accounts).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&GatewayChannelUpstreamAccountBindingAudit{}).Where("channel_id = ?", channel.ID).Count(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if accounts != 1 || audits != 1 {
		t.Fatalf("failed switch left accounts=%d audits=%d", accounts, audits)
	}
}

func TestUpstreamAccountIdentityObservationValidationAndRedaction(t *testing.T) {
	for _, raw := range []string{
		"api.example.test",
		"ftp://api.example.test",
		"https:///missing-host",
		"https://api.example.test:0",
		"https://api.example.test:65536",
		"https://api.example.test:not-a-port",
	} {
		if _, err := normalizeUpstreamAccountIdentityAPIOrigin(raw); err == nil {
			t.Fatalf("invalid API origin %q was accepted", raw)
		}
	}

	observation := UpstreamAccountIdentityObservation{
		ChannelID: 1, ExpectedIdentityRevision: 1, ExpectedAdminRevision: 1,
		Provider: ChannelTypeSub2API, APIOrigin: "https://private.example.test/v1",
		AccountID: "raw-account-id", Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me", ObservedAt: time.Now().UTC(),
	}
	payload, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), observation.APIOrigin) || strings.Contains(string(payload), observation.AccountID) {
		t.Fatalf("observation JSON exposed raw identity: %s", payload)
	}
}

func upstreamIdentityIntPointer(value int) *int { return &value }
