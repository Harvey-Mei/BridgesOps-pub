package storage

import (
	"errors"
	"testing"
	"time"
)

func TestCurrentGenerationStructuralAssignmentKeyAllowsUnsyncedAndProbeDisabledTrustedKeys(t *testing.T) {
	expected := UpstreamGroupGeneration{
		ID: 17, ChannelID: 4, CanonicalGroupName: "image-pro", PresenceGeneration: 3, DiscoveredAt: time.Now(),
	}
	groupID := expected.ID
	generation := expected.PresenceGeneration
	change := &GatewayUpstreamGroupChange{
		ChannelID: expected.ChannelID, GroupName: " IMAGE-PRO ", UpstreamGroupID: &groupID, PresenceGeneration: &generation,
		UpstreamKeyID: 0, UpstreamKeyName: "temporary-provider-name",
	}

	for _, provenance := range []string{GatewayUpstreamKeyProvenanceCreated, GatewayUpstreamKeyProvenanceAdopted} {
		key := &GatewayUpstreamKey{
			ChannelID: expected.ChannelID, GroupName: "image-pro", UpstreamKeyID: 0,
			OwnershipProvenance: provenance, APIKeyCipher: "cipher", Enabled: true,
			LatencyProbeEnabled: false, ModelsSyncedAt: nil, ModelsError: "model sync has not completed",
		}
		for _, entry := range []struct {
			name   string
			change *GatewayUpstreamGroupChange
		}{{name: "direct edit", change: nil}, {name: "change review", change: change}} {
			t.Run(provenance+"/"+entry.name, func(t *testing.T) {
				if err := validateCurrentGenerationStructuralAssignmentKey(expected, entry.change, key); err != nil {
					t.Fatalf("unsynced trusted key was rejected: %v", err)
				}
			})
		}
	}
}

func TestCurrentGenerationStructuralAssignmentKeyDoesNotUseRemoteMutationOwnershipForRoutingAssignment(t *testing.T) {
	expected := UpstreamGroupGeneration{ID: 21, ChannelID: 8, CanonicalGroupName: "premium", PresenceGeneration: 2, DiscoveredAt: time.Now()}
	groupID := expected.ID
	generation := expected.PresenceGeneration
	change := &GatewayUpstreamGroupChange{
		ChannelID: expected.ChannelID, GroupName: "premium", UpstreamGroupID: &groupID, PresenceGeneration: &generation,
		UpstreamKeyID: 51, UpstreamKeyName: "managed-premium",
	}
	for _, provenance := range []string{"", GatewayUpstreamKeyProvenanceUnknown, GatewayUpstreamKeyProvenanceProvisioningClaim, "invalid"} {
		key := &GatewayUpstreamKey{
			ChannelID: expected.ChannelID, GroupName: "premium", UpstreamKeyID: 51, UpstreamKeyName: "managed-premium",
			OwnershipProvenance: provenance, APIKeyCipher: "cipher", Enabled: true,
		}
		for _, entry := range []struct {
			name   string
			change *GatewayUpstreamGroupChange
		}{{name: "direct edit", change: nil}, {name: "change review", change: change}} {
			t.Run(provenance+"/"+entry.name, func(t *testing.T) {
				if err := validateCurrentGenerationStructuralAssignmentKey(expected, entry.change, key); err != nil {
					t.Fatalf("usable routing key with provenance %q was rejected: %v", provenance, err)
				}
			})
		}
	}
}

func TestCurrentGenerationStructuralAssignmentKeyRejectsUnavailableAndStaleKeys(t *testing.T) {
	expected := UpstreamGroupGeneration{ID: 31, ChannelID: 12, CanonicalGroupName: "codex", PresenceGeneration: 5, DiscoveredAt: time.Now()}
	groupID := expected.ID
	generation := expected.PresenceGeneration
	base := GatewayUpstreamKey{
		ChannelID: expected.ChannelID, GroupName: "codex", UpstreamKeyID: 71, UpstreamKeyName: "managed-codex",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceCreated, APIKeyCipher: "cipher", Enabled: true,
	}
	change := GatewayUpstreamGroupChange{
		ChannelID: expected.ChannelID, GroupName: "codex", UpstreamGroupID: &groupID, PresenceGeneration: &generation,
		UpstreamKeyID: base.UpstreamKeyID, UpstreamKeyName: base.UpstreamKeyName,
	}

	tests := []struct {
		name string
		key  *GatewayUpstreamKey
		edit func(*GatewayUpstreamGroupChange)
		want error
	}{
		{name: "missing key", key: nil, want: ErrUpstreamGroupAssignmentKeyUnavailable},
		{name: "disabled key", key: func() *GatewayUpstreamKey { key := base; key.Enabled = false; return &key }(), want: ErrUpstreamGroupAssignmentKeyUnavailable},
		{name: "provisioning key", key: func() *GatewayUpstreamKey { key := base; key.Provisioning = true; return &key }(), want: ErrUpstreamGroupAssignmentKeyUnavailable},
		{name: "missing cipher", key: func() *GatewayUpstreamKey { key := base; key.APIKeyCipher = ""; return &key }(), want: ErrUpstreamGroupAssignmentKeyUnavailable},
		{name: "remote identity drift within current generation", key: func() *GatewayUpstreamKey { key := base; key.UpstreamKeyID++; return &key }()},
		{name: "generation drift", key: &base, edit: func(change *GatewayUpstreamGroupChange) { stale := generation - 1; change.PresenceGeneration = &stale }, want: ErrUpstreamGroupGenerationChanged},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			currentChange := change
			if test.edit != nil {
				test.edit(&currentChange)
			}
			err := validateCurrentGenerationStructuralAssignmentKey(expected, &currentChange, test.key)
			if test.want == nil {
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
			} else if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}
