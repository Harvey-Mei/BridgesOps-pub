package storage

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestApplyCostUnitRebasePersistsCompleteRateItemEvidence(t *testing.T) {
	db := openTestDB(t)
	channel := &Channel{
		Name: "rebase-rate-item-evidence", Type: ChannelTypeSub2API,
		SiteURL: "https://example.com", Username: "u", PasswordCipher: "x",
	}
	if err := NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	repo := NewCosting(db)
	next, audit, expectedVersion := prepareCostUnitRebase(t, db, repo, channel.ID)
	audit.RateTransitionItems[0].DescriptionAfter = "plus after unit rebase"
	audit.RateTransitionItems[1].DescriptionAfter = "promo after unit rebase"

	replayed, err := repo.ApplyCostUnitRebase(next, audit, expectedVersion)
	if err != nil || replayed {
		t.Fatalf("initial cost-unit rebase = replayed %t, err=%v", replayed, err)
	}

	var storedAudit ChannelCostUnitRebase
	if err := db.First(&storedAudit, audit.ID).Error; err != nil {
		t.Fatal(err)
	}
	var rows []ChannelCostUnitRebaseRateItem
	if err := db.Where("rebase_id = ?", audit.ID).Order("ordinal ASC, id ASC").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != storedAudit.RateEvidenceGroupCount || len(rows) != len(audit.RateTransitionItems) {
		t.Fatalf("persisted rebase rate items = %d, audit count = %d, input count = %d", len(rows), storedAudit.RateEvidenceGroupCount, len(audit.RateTransitionItems))
	}
	expectedItems := append([]CostUnitRateTransitionItem(nil), audit.RateTransitionItems...)
	sort.Slice(expectedItems, func(i, j int) bool {
		return strings.ToLower(strings.TrimSpace(expectedItems[i].ModelName)) < strings.ToLower(strings.TrimSpace(expectedItems[j].ModelName))
	})
	items := make([]CostUnitRateTransitionItem, len(rows))
	for index, row := range rows {
		if row.RebaseID != audit.ID || row.Ordinal != index {
			t.Fatalf("persisted rebase rate item[%d] identity = rebase %d ordinal %d", index, row.RebaseID, row.Ordinal)
		}
		expected := expectedItems[index]
		if row.ModelName != strings.TrimSpace(expected.ModelName) ||
			row.DescriptionBefore != strings.TrimSpace(expected.DescriptionBefore) ||
			row.DescriptionAfter != strings.TrimSpace(expected.DescriptionAfter) ||
			!financiallyEqual(row.RatioBefore, expected.RatioBefore) ||
			!financiallyEqual(row.RatioAfter, expected.RatioAfter) ||
			!financiallyEqual(row.CompletionRatioBefore, expected.CompletionRatioBefore) ||
			!financiallyEqual(row.CompletionRatioAfter, expected.CompletionRatioAfter) ||
			!row.CreatedAt.Equal(audit.AppliedAt) {
			t.Fatalf("persisted rebase rate item[%d] = %+v, want %+v at %v", index, row, expected, audit.AppliedAt)
		}
		items[index] = CostUnitRateTransitionItem{
			ModelName: row.ModelName, DescriptionBefore: row.DescriptionBefore, DescriptionAfter: row.DescriptionAfter,
			RatioBefore: row.RatioBefore, RatioAfter: row.RatioAfter,
			CompletionRatioBefore: row.CompletionRatioBefore, CompletionRatioAfter: row.CompletionRatioAfter,
		}
	}
	digest, err := CostUnitRateEvidenceDigest(items)
	if err != nil {
		t.Fatal(err)
	}
	if digest != storedAudit.RateEvidenceDigest || digest != audit.RateEvidenceDigest {
		t.Fatalf("persisted rebase rate digest = %q, stored audit = %q, input = %q", digest, storedAudit.RateEvidenceDigest, audit.RateEvidenceDigest)
	}

	replayAudit := &ChannelCostUnitRebase{
		ChannelID: channel.ID, IdempotencyKey: audit.IdempotencyKey,
		RequestFingerprint: audit.RequestFingerprint, Actor: audit.Actor,
	}
	replayed, err = repo.ApplyCostUnitRebase(&ChannelCostState{ChannelID: channel.ID}, replayAudit, expectedVersion)
	if err != nil || !replayed {
		t.Fatalf("idempotent cost-unit rebase replay = replayed %t, err=%v", replayed, err)
	}
	var count int64
	if err := db.Model(&ChannelCostUnitRebaseRateItem{}).Where("rebase_id = ?", audit.ID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != int64(len(rows)) {
		t.Fatalf("rebase rate items after replay = %d, want %d", count, len(rows))
	}
}

func TestApplyCostUnitRebaseReplayRejectsRateItemEvidenceDrift(t *testing.T) {
	tests := []struct {
		name   string
		tamper func(*testing.T, *gorm.DB, uint64)
	}{
		{
			name: "missing child row",
			tamper: func(t *testing.T, db *gorm.DB, rebaseID uint64) {
				t.Helper()
				if err := db.Exec(`DELETE FROM public.channel_cost_unit_rebase_rate_items WHERE rebase_id = ? AND ordinal = 0`, rebaseID).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "digest mismatch",
			tamper: func(t *testing.T, db *gorm.DB, rebaseID uint64) {
				t.Helper()
				if err := db.Exec(`UPDATE public.channel_cost_unit_rebase_rate_items SET ratio_after = ratio_after + 1 WHERE rebase_id = ? AND ordinal = 0`, rebaseID).Error; err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestDB(t)
			channel := &Channel{
				Name: "rebase-rate-item-replay-" + test.name, Type: ChannelTypeSub2API,
				SiteURL: "https://example.com", Username: "u", PasswordCipher: "x",
			}
			if err := NewChannels(db).Create(channel); err != nil {
				t.Fatal(err)
			}
			repo := NewCosting(db)
			next, audit, expectedVersion := prepareCostUnitRebase(t, db, repo, channel.ID)
			if replayed, err := repo.ApplyCostUnitRebase(next, audit, expectedVersion); err != nil || replayed {
				t.Fatalf("initial cost-unit rebase = replayed %t, err=%v", replayed, err)
			}

			if err := db.Exec(`ALTER TABLE public.channel_cost_unit_rebase_rate_items DISABLE TRIGGER USER`).Error; err != nil {
				t.Fatal(err)
			}
			tampered := false
			defer func() {
				if tampered {
					if err := db.Exec(`ALTER TABLE public.channel_cost_unit_rebase_rate_items ENABLE TRIGGER USER`).Error; err != nil {
						t.Errorf("re-enable append-only trigger: %v", err)
					}
				}
			}()
			tampered = true
			test.tamper(t, db, audit.ID)
			if err := db.Exec(`ALTER TABLE public.channel_cost_unit_rebase_rate_items ENABLE TRIGGER USER`).Error; err != nil {
				t.Fatal(err)
			}
			tampered = false

			replayAudit := &ChannelCostUnitRebase{
				ChannelID: channel.ID, IdempotencyKey: audit.IdempotencyKey,
				RequestFingerprint: audit.RequestFingerprint, Actor: audit.Actor,
			}
			replayed, err := repo.ApplyCostUnitRebase(&ChannelCostState{ChannelID: channel.ID}, replayAudit, expectedVersion)
			if replayed || !errors.Is(err, ErrCostUnitRebaseInvalid) {
				t.Fatalf("replay with drifted rate items = replayed %t, err=%v, want %v", replayed, err, ErrCostUnitRebaseInvalid)
			}
		})
	}
}
