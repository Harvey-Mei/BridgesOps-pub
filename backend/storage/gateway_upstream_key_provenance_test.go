package storage

import (
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestChannelDeletionManagedOwnershipClaimAcceptsOnlyReservedUnknownName(t *testing.T) {
	reserved := GatewayUpstreamKey{
		ID: 9, ChannelID: 7, GroupName: "managed", UpstreamKeyID: 0,
		UpstreamKeyName: "BOG-p-0123456789abcdef", OwnershipProvenance: GatewayUpstreamKeyProvenanceUnknown,
	}
	digest, err := ChannelDeletionManagedOwnershipClaimDigest(reserved)
	if err != nil || len(digest) != 64 {
		t.Fatalf("reserved unknown ownership digest=%q err=%v", digest, err)
	}
	reserved.UpstreamKeyName = "BOG - manually named"
	if _, err := ChannelDeletionManagedOwnershipClaimDigest(reserved); err == nil || !strings.Contains(err.Error(), "not deletable") {
		t.Fatalf("ordinary unknown name ownership error = %v", err)
	}
}

func TestGatewayUpstreamKeyOwnershipProvenanceIsExplicitAndPreserved(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "ownership-provenance", Type: ChannelTypeNewAPI, SiteURL: "https://ownership.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	record := &GatewayUpstreamKey{ChannelID: channel.ID, GroupName: "legacy", APIKeyCipher: "cipher", APIKeyPrefix: "prefix", Enabled: true}
	if err := store.UpsertUpstreamKey(record); err != nil {
		t.Fatal(err)
	}
	stored, err := store.FindUpstreamKeyAny(channel.ID, "legacy")
	if err != nil || stored.OwnershipProvenance != GatewayUpstreamKeyProvenanceUnknown {
		t.Fatalf("legacy provenance = %+v err=%v", stored, err)
	}
	record.OwnershipProvenance = GatewayUpstreamKeyProvenanceAdopted
	if err := store.UpsertUpstreamKey(record); err != nil {
		t.Fatal(err)
	}
	record.OwnershipProvenance = ""
	record.LastError = "retry"
	if err := store.UpsertUpstreamKey(record); err != nil {
		t.Fatal(err)
	}
	stored, err = store.FindUpstreamKeyAny(channel.ID, "legacy")
	if err != nil || stored.OwnershipProvenance != GatewayUpstreamKeyProvenanceAdopted {
		t.Fatalf("preserved provenance = %+v err=%v", stored, err)
	}

	claimed, claimedByCaller, fresh, err := store.ClaimUpstreamKeyProvisioning(channel.ID, "fresh", "BOG-p-0123456789abcdef", 1, time.Now(), time.Minute)
	if err != nil || !claimedByCaller || !fresh || claimed == nil || claimed.OwnershipProvenance != GatewayUpstreamKeyProvenanceProvisioningClaim {
		t.Fatalf("provisioning claim = %+v claimed=%v fresh=%v err=%v", claimed, claimedByCaller, fresh, err)
	}
}

func TestDisableUpstreamKeyDropsCreatedOwnershipBeforeSecretRecovery(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channels := NewChannels(db)
	store := NewGateway(db)
	channel := &Channel{Name: "ownership-disable", Type: ChannelTypeNewAPI, SiteURL: "https://ownership-disable.example", MonitorEnabled: true}
	if err := channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "managed", UpstreamKeyID: 73, UpstreamKeyName: "BOG-p-0123456789abcdef",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "original-cipher", APIKeyPrefix: "sk-original", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	if err := store.DisableUpstreamKey(channel.ID, "managed", "invalidated"); err != nil {
		t.Fatal(err)
	}
	disabled, err := store.FindUpstreamKeyAny(channel.ID, "managed")
	if err != nil {
		t.Fatal(err)
	}
	if disabled.APIKeyCipher != "" || disabled.APIKeyPrefix != "" || disabled.Enabled {
		t.Fatalf("disabled key retained usable secret state: %+v", disabled)
	}
	if disabled.OwnershipProvenance != GatewayUpstreamKeyProvenanceUnknown {
		t.Fatalf("disabled key provenance = %q, want %q", disabled.OwnershipProvenance, GatewayUpstreamKeyProvenanceUnknown)
	}
	if disabled.UpstreamKeyID != 73 || disabled.UpstreamKeyName != "BOG-p-0123456789abcdef" {
		t.Fatalf("disabled key lost remote recovery identity: %+v", disabled)
	}

	// Secret recovery intentionally omits provenance. Upsert must preserve the
	// fail-closed unknown state instead of resurrecting the old created claim.
	if err := store.UpsertUpstreamKey(&GatewayUpstreamKey{
		ChannelID: channel.ID, GroupName: "managed", UpstreamKeyID: 73, UpstreamKeyName: "BOG-p-0123456789abcdef",
		APIKeyCipher: "replacement-cipher", APIKeyPrefix: "sk-replacement", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.FindUpstreamKeyAny(channel.ID, "managed")
	if err != nil {
		t.Fatal(err)
	}
	if recovered.OwnershipProvenance != GatewayUpstreamKeyProvenanceUnknown {
		t.Fatalf("recovered key provenance = %q, want %q", recovered.OwnershipProvenance, GatewayUpstreamKeyProvenanceUnknown)
	}
	if !recovered.Enabled || recovered.APIKeyCipher != "replacement-cipher" || recovered.APIKeyPrefix != "sk-replacement" {
		t.Fatalf("replacement secret was not saved: %+v", recovered)
	}
}
