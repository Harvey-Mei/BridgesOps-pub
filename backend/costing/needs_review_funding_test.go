package costing

import (
	"errors"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
	"gorm.io/gorm"
)

func TestFundingWritersCannotClearCostUnitReview(t *testing.T) {
	t.Run("confirm pending candidate", func(t *testing.T) {
		db, err := testdb.OpenWithError(t)
		if err != nil {
			t.Fatal(err)
		}
		channel := &storage.Channel{Name: "review-confirm", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
		if err := storage.NewChannels(db).Create(channel); err != nil {
			t.Fatal(err)
		}
		svc := NewService(storage.NewCosting(db))
		state, err := svc.RecordBalance(BalanceInput{ChannelID: channel.ID, ExpectedVersion: 1, RawBalance: 50, SampledAt: time.Now(), Source: "upstream"})
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := svc.DetectFunding(FundingCandidateInput{
			ChannelID: channel.ID, Source: "balance_increase", ExternalID: "review-pending",
			CreditedAmount: 100, RawBalanceBefore: 50, RawBalanceDetected: 150,
			FundingFactorBefore: state.FundingFactor, DetectedAt: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
		markCostUnitReviewForTest(t, db, channel.ID, state.Version)

		next, _, err := svc.ConfirmFundingCandidate(channel.ID, candidate.ID, 50, state.Version, "resolve before rebase")
		if err != nil {
			t.Fatal(err)
		}
		assertCostUnitReviewPreserved(t, next, state.Version+1)
	})

	t.Run("manual funding", func(t *testing.T) {
		db, err := testdb.OpenWithError(t)
		if err != nil {
			t.Fatal(err)
		}
		channel := &storage.Channel{Name: "review-manual", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
		if err := storage.NewChannels(db).Create(channel); err != nil {
			t.Fatal(err)
		}
		markCostUnitReviewForTest(t, db, channel.ID, 1)
		svc := NewService(storage.NewCosting(db))

		next, _, err := svc.RecordFunding(FundingInput{
			ChannelID: channel.ID, ExpectedVersion: 1, PaidAmount: 10, CreditedAmount: 20,
			Source: "manual_adjustment", EffectiveAt: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
		assertCostUnitReviewPreserved(t, next, 2)
	})
}

func TestGenericCostErrorCannotReplaceCostUnitReview(t *testing.T) {
	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatal(err)
	}
	channel := &storage.Channel{Name: "review-error", Type: storage.ChannelTypeSub2API, SiteURL: "https://example.com", Username: "u", PasswordCipher: "cipher"}
	if err := storage.NewChannels(db).Create(channel); err != nil {
		t.Fatal(err)
	}
	markCostUnitReviewForTest(t, db, channel.ID, 1)
	svc := NewService(storage.NewCosting(db))

	next, err := svc.RecordError(channel.ID, 1, "cost_refresh", errors.New("temporary upstream failure"))
	if err != nil {
		t.Fatal(err)
	}
	assertCostUnitReviewPreserved(t, next, 1)
}

func markCostUnitReviewForTest(t *testing.T, db *gorm.DB, channelID uint, version uint64) {
	t.Helper()
	factor := 0.1
	sampledAt := time.Now().UTC()
	result := db.Model(&storage.ChannelCostState{}).Where("channel_id = ? AND version = ?", channelID, version).Updates(map[string]any{
		"status":                              StateStatusNeedsReview,
		"last_error":                          ErrCostUnitChangeRequiresRebase.Error(),
		"observed_provider_factor":            factor,
		"observed_provider_factor_source":     "advertised",
		"observed_provider_factor_sampled_at": sampledAt,
	})
	if result.Error != nil {
		t.Fatal(result.Error)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("mark review affected %d rows", result.RowsAffected)
	}
}

func assertCostUnitReviewPreserved(t *testing.T, state *storage.ChannelCostState, wantVersion uint64) {
	t.Helper()
	if state == nil || state.Status != StateStatusNeedsReview || state.LastError != ErrCostUnitChangeRequiresRebase.Error() || state.Version != wantVersion {
		t.Fatalf("cost-unit review was not preserved: %+v", state)
	}
}
