package storage

import (
	"testing"
	"time"
)

func TestFundingCandidatesPendingPaginationUsesConfirmationOrder(t *testing.T) {
	db := openTestDB(t)
	repo := NewCosting(db)
	base := time.Now().UTC().Truncate(time.Second)
	older := base.Add(-time.Hour)
	items := []ChannelFundingCandidate{
		{ChannelID: 1, Source: "test", ExternalID: "recent", Status: FundingCandidatePending, DetectedAt: base},
		{ChannelID: 1, Source: "test", ExternalID: "late-old", Status: FundingCandidatePending, OccurredAt: &older, DetectedAt: base.Add(time.Minute)},
		{ChannelID: 1, Source: "test", ExternalID: "same-time", Status: FundingCandidatePending, DetectedAt: older},
	}
	for i := range items {
		if err := db.Create(&items[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	for offset, want := range []uint64{items[1].ID, items[2].ID, items[0].ID} {
		page, total, err := repo.ListFundingCandidates(1, FundingCandidatePending, 1, offset)
		if err != nil || total != 3 || len(page) != 1 || page[0].ID != want {
			t.Fatalf("page %d = %+v, total=%d err=%v, want %d", offset, page, total, err, want)
		}
	}
	// General audit listings keep their existing newest-detected-first order.
	page, _, err := repo.ListFundingCandidates(1, "", 1, 0)
	if err != nil || len(page) != 1 || page[0].ID != items[1].ID {
		t.Fatalf("audit order changed: %+v %v", page, err)
	}
}
