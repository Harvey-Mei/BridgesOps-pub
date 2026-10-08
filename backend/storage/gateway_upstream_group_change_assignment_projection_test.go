package storage

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestListUpstreamGroupChangesProjectsCurrentAssignmentKey(t *testing.T) {
	store, _, _, recorded := newManualAssignmentKeyFixture(t, 7, "historical-key", &GatewayUpstreamKey{
		UpstreamKeyID: 81, UpstreamKeyName: "BOG-p-current-key",
		OwnershipProvenance: GatewayUpstreamKeyProvenanceUnknown,
		Enabled:             true, APIKeyCipher: "current-cipher",
	})

	changes, err := store.ListUpstreamGroupChanges(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].ID != recorded.ID {
		t.Fatalf("pending changes = %+v", changes)
	}
	projected := changes[0]
	if !projected.AssignmentActionable || projected.AssignmentBlockedReason != "" {
		t.Fatalf("current usable key projection = %+v", projected)
	}
	if projected.UpstreamKeyID != 81 || projected.UpstreamKeyName != "BOG-p-current-key" {
		t.Fatalf("current key identity was not projected: %+v", projected)
	}
	payload, err := json.Marshal(projected)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"assignment_actionable":true`) {
		t.Fatalf("projection JSON = %s", payload)
	}
}

func TestListUpstreamGroupChangesProjectsUnavailableCurrentKeys(t *testing.T) {
	tests := []struct {
		name       string
		current    *GatewayUpstreamKey
		wantReason string
	}{
		{name: "missing", wantReason: assignmentBlockedReasonMissingKey},
		{name: "disabled", current: &GatewayUpstreamKey{UpstreamKeyID: 82, UpstreamKeyName: "disabled", Enabled: false, APIKeyCipher: "cipher"}, wantReason: assignmentBlockedReasonDisabledKey},
		{name: "provisioning", current: &GatewayUpstreamKey{UpstreamKeyID: 83, UpstreamKeyName: "provisioning", Enabled: false, Provisioning: true}, wantReason: assignmentBlockedReasonProvisioningKey},
		{name: "missing cipher", current: &GatewayUpstreamKey{UpstreamKeyID: 84, UpstreamKeyName: "missing-cipher", Enabled: true}, wantReason: assignmentBlockedReasonMissingCipher},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, db, _, change := newManualAssignmentKeyFixture(t, 7, "historical-key", test.current)
			if test.name == "disabled" {
				if err := db.Model(&GatewayUpstreamKey{}).
					Where("channel_id = ? AND lower(btrim(group_name)) = ?", change.ChannelID, "promo").
					Update("enabled", false).Error; err != nil {
					t.Fatal(err)
				}
			}
			changes, err := store.ListUpstreamGroupChanges(true)
			if err != nil {
				t.Fatal(err)
			}
			if len(changes) != 1 {
				t.Fatalf("pending changes = %+v", changes)
			}
			if changes[0].AssignmentActionable || changes[0].AssignmentBlockedReason != test.wantReason {
				t.Fatalf("unavailable key projection = %+v, want reason %q", changes[0], test.wantReason)
			}
		})
	}
}

func TestListUpstreamGroupChangesSupersedesStaleGeneration(t *testing.T) {
	store, db, _, recorded := newManualAssignmentKeyFixture(t, 7, "historical-key", &GatewayUpstreamKey{
		UpstreamKeyID: 85, UpstreamKeyName: "current-key", Enabled: true, APIKeyCipher: "cipher",
	})
	if recorded.UpstreamGroupID == nil {
		t.Fatal("fixture did not bind the upstream group")
	}
	if err := db.Model(&GatewayUpstreamGroup{}).Where("id = ?", *recorded.UpstreamGroupID).
		UpdateColumn("presence_generation", gorm.Expr("presence_generation + 1")).Error; err != nil {
		t.Fatal(err)
	}

	pending, err := store.ListUpstreamGroupChanges(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("stale pending changes = %+v", pending)
	}
	if err := db.First(&recorded, recorded.ID).Error; err != nil {
		t.Fatal(err)
	}
	if recorded.Status != "superseded" || recorded.AssignmentState != "superseded" || recorded.AssignmentReasonCode != "group_generation_changed" || recorded.ResolvedAt == nil {
		t.Fatalf("stale change was not reconciled: %+v", recorded)
	}
}

func TestListUpstreamGroupChangesSupersedesDuplicatePendingIdentity(t *testing.T) {
	store, db, _, recorded := newManualAssignmentKeyFixture(t, 7, "historical-key", &GatewayUpstreamKey{
		UpstreamKeyID: 91, UpstreamKeyName: "current-key", Enabled: true, APIKeyCipher: "cipher",
	})
	if err := db.Exec("DROP INDEX gateway_upstream_group_changes_pending_added_identity_idx").Error; err != nil {
		t.Fatal(err)
	}
	duplicate := recorded
	duplicate.ID = 0
	duplicate.DetectedAt = recorded.DetectedAt.Add(time.Second)
	duplicate.CreatedAt = time.Time{}
	duplicate.UpdatedAt = time.Time{}
	duplicate.EvaluationToken = nil
	if err := db.Create(&duplicate).Error; err != nil {
		t.Fatal(err)
	}

	pending, err := store.ListUpstreamGroupChanges(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ID != duplicate.ID {
		t.Fatalf("deduplicated pending changes = %+v, want newest id %d", pending, duplicate.ID)
	}
	if err := db.First(&recorded, recorded.ID).Error; err != nil {
		t.Fatal(err)
	}
	if recorded.Status != "superseded" || recorded.AssignmentState != "superseded" || recorded.AssignmentReasonCode != "duplicate_pending_identity" || recorded.ResolvedAt == nil {
		t.Fatalf("older duplicate was not superseded: %+v", recorded)
	}
}

func TestListUpstreamGroupChangesBlocksMismatchedCanonicalGroup(t *testing.T) {
	store, db, _, recorded := newManualAssignmentKeyFixture(t, 7, "historical-key", &GatewayUpstreamKey{
		UpstreamKeyID: 86, UpstreamKeyName: "current-key", Enabled: true, APIKeyCipher: "cipher",
	})
	if err := db.Model(&GatewayUpstreamGroupChange{}).Where("id = ?", recorded.ID).
		UpdateColumn("group_name", "different-group").Error; err != nil {
		t.Fatal(err)
	}

	pending, err := store.ListUpstreamGroupChanges(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("mismatched canonical group remained pending: %+v", pending)
	}
	if err := db.First(&recorded, recorded.ID).Error; err != nil {
		t.Fatal(err)
	}
	if recorded.Status != "superseded" || recorded.AssignmentReasonCode != "group_generation_changed" {
		t.Fatalf("mismatched canonical group was not reconciled: %+v", recorded)
	}
}
