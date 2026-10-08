package storage

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestNormalizeUpstreamAccountAPIOriginRemovesNonIdentityParts(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{raw: "HTTPS://user:secret@Example.COM:443/api/v1?token=secret#fragment", want: "https://example.com"},
		{raw: "http://Example.COM.:80/path", want: "http://example.com"},
		{raw: "https://Example.COM:8443/path", want: "https://example.com:8443"},
		{raw: "https://[2001:db8::1]:443/api", want: "https://[2001:db8::1]"},
	}
	for _, test := range tests {
		got, err := normalizeUpstreamAccountAPIOrigin(test.raw)
		if err != nil {
			t.Fatalf("normalize %q: %v", test.raw, err)
		}
		if got != test.want {
			t.Fatalf("normalize %q = %q, want %q", test.raw, got, test.want)
		}
	}
	for _, raw := range []string{"", "example.com", "ftp://example.com", "https:///missing-host"} {
		if _, err := normalizeUpstreamAccountAPIOrigin(raw); err == nil {
			t.Fatalf("normalize %q succeeded, want error", raw)
		}
	}
}

func TestObserveUpstreamAccountConcurrencyMergesIdentityAndRejectsStaleFacts(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	firstChannel := createConcurrencyTestChannel(t, channels, "account-one", ChannelTypeSub2API)
	secondChannel := createConcurrencyTestChannel(t, channels, "account-two", ChannelTypeSub2API)
	observedAt := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	first := observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: firstChannel.ID, ChannelIdentityRevision: firstChannel.UpstreamAccountIdentityRevision, Provider: ChannelTypeSub2API,
		APIOrigin: "https://user:secret@API.Example.com:443/v1?private=value", ExternalAccountID: "upstream-user-42",
		Limit: concurrencyIntPointer(2), Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.login.user.concurrency", ObservedAt: observedAt,
	})
	second := observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: secondChannel.ID, ChannelIdentityRevision: secondChannel.UpstreamAccountIdentityRevision, Provider: ChannelTypeSub2API,
		APIOrigin: "https://api.example.com/another/path", ExternalAccountID: "upstream-user-42",
		Limit: concurrencyIntPointer(4), Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: observedAt.Add(time.Minute),
	})
	if first.AccountRecordID == 0 || first.AccountRecordID != second.AccountRecordID ||
		first.ChannelIdentityRevision != firstChannel.UpstreamAccountIdentityRevision ||
		second.ChannelIdentityRevision != secondChannel.UpstreamAccountIdentityRevision {
		t.Fatalf("same provider/origin/account did not merge: first=%#v second=%#v", first, second)
	}
	unknownChannel := createConcurrencyTestChannel(t, channels, "account-unknown", ChannelTypeSub2API)
	batch, err := channels.UpstreamAccountConcurrencyForChannels([]uint{firstChannel.ID, secondChannel.ID, unknownChannel.ID, firstChannel.ID, 0})
	if err != nil {
		t.Fatal(err)
	}
	firstBatch, firstFound := batch[firstChannel.ID]
	secondBatch, secondFound := batch[secondChannel.ID]
	if !firstFound || !secondFound || firstBatch.AccountRecordID != secondBatch.AccountRecordID ||
		firstBatch.ChannelIdentityRevision != firstChannel.UpstreamAccountIdentityRevision ||
		secondBatch.ChannelIdentityRevision != secondChannel.UpstreamAccountIdentityRevision ||
		firstBatch.Limit == nil || secondBatch.Limit == nil || *firstBatch.Limit != 4 || *secondBatch.Limit != 4 {
		t.Fatalf("shared-account batch snapshots = %#v", batch)
	}
	if _, found := batch[unknownChannel.ID]; found {
		t.Fatalf("unbound channel received synthetic concurrency snapshot: %#v", batch[unknownChannel.ID])
	}

	updated, err := channels.UpstreamAccountConcurrencyForChannel(firstChannel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated == nil || updated.Limit == nil || *updated.Limit != 4 || updated.Source != "sub2api.auth.me.concurrency" {
		t.Fatalf("merged account fact = %#v", updated)
	}

	stale := observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: firstChannel.ID, ChannelIdentityRevision: firstChannel.UpstreamAccountIdentityRevision, Provider: ChannelTypeSub2API,
		APIOrigin: "https://api.example.com", ExternalAccountID: "upstream-user-42",
		Limit: concurrencyIntPointer(1), Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.login.user.concurrency", ObservedAt: observedAt.Add(-time.Minute),
	})
	if stale.Limit == nil || *stale.Limit != 4 {
		t.Fatalf("stale fact overwrote current limit: %#v", stale)
	}

	_, err = channels.ObserveUpstreamAccountConcurrency(UpstreamAccountConcurrencyObservation{
		ChannelID: firstChannel.ID, ChannelIdentityRevision: firstChannel.UpstreamAccountIdentityRevision, Provider: ChannelTypeSub2API,
		APIOrigin: "https://api.example.com", ExternalAccountID: "upstream-user-42",
		Limit: concurrencyIntPointer(5), Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: observedAt.Add(time.Minute),
	})
	if err == nil || !strings.Contains(err.Error(), "same observation time") {
		t.Fatalf("equal-time conflicting fact error = %v", err)
	}

	var account GatewayUpstreamAccount
	if err := db.First(&account, first.AccountRecordID).Error; err != nil {
		t.Fatal(err)
	}
	if strings.Contains(account.APIOriginHash, "example") || strings.Contains(account.ExternalAccountIDHash, "upstream") ||
		len(account.APIOriginHash) != 64 || len(account.ExternalAccountIDHash) != 64 {
		t.Fatalf("account identity was not stored as fixed hashes: %#v", account)
	}
	payload, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "account_record_id") || strings.Contains(string(payload), "upstream-user-42") || strings.Contains(string(payload), "api.example.com") {
		t.Fatalf("snapshot exposed internal account identity: %s", payload)
	}
}

func TestObserveUpstreamAccountConcurrencyAuditsBindingChangesAndKeepsProviderBoundary(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	channel := createConcurrencyTestChannel(t, channels, "binding-change", ChannelTypeSub2API)
	newAPIChannel := createConcurrencyTestChannel(t, channels, "newapi-boundary", ChannelTypeNewAPI)
	base := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	accountA := observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision, Provider: ChannelTypeSub2API, APIOrigin: "https://api.example.com", ExternalAccountID: "account-a",
		Limit: concurrencyIntPointer(2), Scope: UpstreamConcurrencyScopeAccount, Source: "sub2api.auth.me.concurrency", ObservedAt: base,
	})
	accountB := observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision, Provider: ChannelTypeSub2API, APIOrigin: "https://api.example.com", ExternalAccountID: "account-b",
		Limit: concurrencyIntPointer(-1), Scope: UpstreamConcurrencyScopeAccount, Source: "sub2api.auth.me.concurrency", ObservedAt: base.Add(2 * time.Minute),
	})
	if accountA.AccountRecordID == accountB.AccountRecordID || accountB.Limit == nil || *accountB.Limit != 0 || accountB.State() != "unlimited" {
		t.Fatalf("binding change snapshots: a=%#v b=%#v", accountA, accountB)
	}

	observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision, Provider: ChannelTypeSub2API, APIOrigin: "https://api.example.com", ExternalAccountID: "account-a",
		Limit: concurrencyIntPointer(8), Scope: UpstreamConcurrencyScopeAccount, Source: "sub2api.auth.me.concurrency", ObservedAt: base.Add(time.Minute),
	})
	current, err := channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current == nil || current.AccountRecordID != accountB.AccountRecordID {
		t.Fatalf("late old-account fact rebound channel: %#v", current)
	}

	var audits []GatewayChannelUpstreamAccountBindingAudit
	if err := db.Where("channel_id = ?", channel.ID).Order("id ASC").Find(&audits).Error; err != nil {
		t.Fatal(err)
	}
	if len(audits) != 2 || audits[0].PreviousUpstreamAccountID != nil || audits[1].PreviousUpstreamAccountID == nil ||
		*audits[1].PreviousUpstreamAccountID != accountA.AccountRecordID || audits[1].UpstreamAccountID != accountB.AccountRecordID {
		t.Fatalf("binding audits = %#v", audits)
	}

	_, err = channels.ObserveUpstreamAccountConcurrency(UpstreamAccountConcurrencyObservation{
		ChannelID: newAPIChannel.ID, ChannelIdentityRevision: newAPIChannel.UpstreamAccountIdentityRevision, Provider: ChannelTypeSub2API, APIOrigin: "https://api.example.com", ExternalAccountID: "account-a",
		Limit: concurrencyIntPointer(2), Scope: UpstreamConcurrencyScopeAccount, Source: "sub2api.auth.me.concurrency", ObservedAt: base,
	})
	if err == nil || !strings.Contains(err.Error(), "does not match channel provider") {
		t.Fatalf("cross-provider binding error = %v", err)
	}
}

func TestObserveUpstreamAccountConcurrencyBindsAuthenticatedUnknownAccount(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	channel := createConcurrencyTestChannel(t, channels, "unknown-binding", ChannelTypeSub2API)
	base := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	accountA := observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		Provider: ChannelTypeSub2API, APIOrigin: "https://api.example.com", ExternalAccountID: "account-a",
		Limit: concurrencyIntPointer(2), Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: base,
	})
	accountB := observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		Provider: ChannelTypeSub2API, APIOrigin: "https://api.example.com", ExternalAccountID: "account-b",
		Limit: nil, Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: base.Add(2 * time.Minute),
	})
	if accountA.AccountRecordID == accountB.AccountRecordID || accountB.AccountRecordID == 0 || accountB.Limit != nil || accountB.State() != "unknown" {
		t.Fatalf("authenticated unknown account binding: a=%#v b=%#v", accountA, accountB)
	}

	// A late explicit fact for the old account may refresh that account record,
	// but cannot rebind the channel away from the newer authenticated identity.
	observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: channel.UpstreamAccountIdentityRevision,
		Provider: ChannelTypeSub2API, APIOrigin: "https://api.example.com", ExternalAccountID: "account-a",
		Limit: concurrencyIntPointer(8), Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: base.Add(time.Minute),
	})
	current, err := channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current == nil || current.AccountRecordID != accountB.AccountRecordID || current.Limit != nil {
		t.Fatalf("late old-account fact rebound authenticated unknown account: %#v", current)
	}
}

func TestUpstreamAccountIdentityRevisionRejectsLateOldFactAndLeavesUnknownUnbound(t *testing.T) {
	db := testdb.Open(t)
	channels := NewChannels(db)
	channel := createConcurrencyTestChannel(t, channels, "identity-revision", ChannelTypeSub2API)
	oldRevision := channel.UpstreamAccountIdentityRevision
	base := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	observeConcurrencyTestFact(t, channels, UpstreamAccountConcurrencyObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: oldRevision,
		Provider: ChannelTypeSub2API, APIOrigin: "https://old.example.com", ExternalAccountID: "old-account",
		Limit: concurrencyIntPointer(2), Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: base,
	})
	channel.SiteURL = "https://new.example.com"
	channel.Username = "new-user"
	channel.PasswordCipher = "new-encrypted-credential"
	if err := channels.UpdateWithUpstreamAccountIdentityChange(channel); err != nil {
		t.Fatal(err)
	}
	if channel.UpstreamAccountIdentityRevision != oldRevision+1 {
		t.Fatalf("identity revision = %d, want %d", channel.UpstreamAccountIdentityRevision, oldRevision+1)
	}
	unknown, err := channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unknown != nil {
		t.Fatalf("new unknown account inherited old concurrency: %#v", unknown)
	}
	batch, err := channels.UpstreamAccountConcurrencyForChannels([]uint{channel.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := batch[channel.ID]; found {
		t.Fatalf("stale identity-revision binding appeared in batch: %#v", batch[channel.ID])
	}

	_, err = channels.ObserveUpstreamAccountConcurrency(UpstreamAccountConcurrencyObservation{
		ChannelID: channel.ID, ChannelIdentityRevision: oldRevision,
		Provider: ChannelTypeSub2API, APIOrigin: "https://old.example.com", ExternalAccountID: "old-account",
		Limit: concurrencyIntPointer(8), Scope: UpstreamConcurrencyScopeAccount,
		Source: "sub2api.auth.me.concurrency", ObservedAt: base.Add(time.Hour),
	})
	if !errors.Is(err, ErrStaleUpstreamAccountConcurrencyObservation) {
		t.Fatalf("late old-account observation error = %v", err)
	}
	unknown, err = channels.UpstreamAccountConcurrencyForChannel(channel.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unknown != nil {
		t.Fatalf("late old-account observation rebound channel: %#v", unknown)
	}
}

func createConcurrencyTestChannel(t *testing.T, channels *Channels, name string, provider ChannelType) *Channel {
	t.Helper()
	channel := &Channel{
		Name: name, Type: provider, SiteURL: "https://example.com", Username: "user",
		PasswordCipher: "encrypted", CredentialMode: CredentialModePassword, MonitorEnabled: true,
	}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	return channel
}

func observeConcurrencyTestFact(t *testing.T, channels *Channels, observation UpstreamAccountConcurrencyObservation) *UpstreamAccountConcurrencySnapshot {
	t.Helper()
	snapshot, err := channels.ObserveUpstreamAccountConcurrency(observation)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func concurrencyIntPointer(value int) *int { return &value }
