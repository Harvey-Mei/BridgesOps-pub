package storage

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func directCredentialFixture(id uint, group string) ChannelDirectCredential {
	return ChannelDirectCredential{
		ID: id, ChannelID: 41, GroupName: group, BaseURL: "https://" + strings.ToLower(group) + ".example",
		APIKeyCipher: "cipher-" + group, APIKeyPrefix: "sk-" + group,
		Models: EncodeDirectModels([]string{"model-" + group}), Ratio: 1, CompletionRatio: 1,
		Enabled: true, Revision: 7,
		CreatedAt: time.Date(2026, 8, 1, 1, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 2, 1, 0, 0, 0, time.UTC),
	}
}

func TestPrepareChannelDirectCredentialReplacementKeepsStableIDsAndOnlyDesiredRows(t *testing.T) {
	primary := directCredentialFixture(10, "Primary")
	secondary := directCredentialFixture(11, "Secondary")
	updatedAt := time.Date(2026, 8, 3, 1, 0, 0, 0, time.UTC)

	prepared, err := prepareChannelDirectCredentialReplacement(41, []ChannelDirectCredential{primary, secondary}, []ChannelDirectCredential{
		{
			ID: primary.ID, GroupName: primary.GroupName, BaseURL: primary.BaseURL,
			APIKeyCipher: primary.APIKeyCipher, APIKeyPrefix: primary.APIKeyPrefix,
			Models: primary.Models, Ratio: 1.5, CompletionRatio: primary.CompletionRatio,
			Enabled: primary.Enabled,
		},
	}, updatedAt)
	if err != nil {
		t.Fatalf("prepare replacement: %v", err)
	}
	if len(prepared) != 1 {
		t.Fatalf("prepared rows = %d, want only retained row", len(prepared))
	}
	if prepared[0].ID != primary.ID || prepared[0].Revision != primary.Revision+1 {
		t.Fatalf("retained identity = id %d revision %d, want id %d revision %d", prepared[0].ID, prepared[0].Revision, primary.ID, primary.Revision+1)
	}
	if !prepared[0].UpdatedAt.Equal(updatedAt) {
		t.Fatalf("changed updated_at = %v, want %v", prepared[0].UpdatedAt, updatedAt)
	}
}

func TestPrepareChannelDirectCredentialReplacementKeepsNoopRevisionAndTimestamp(t *testing.T) {
	existing := directCredentialFixture(10, "Primary")
	desired := existing
	desired.ChannelID = 0
	desired.Revision = 0
	desired.CreatedAt = time.Time{}
	desired.UpdatedAt = time.Time{}

	prepared, err := prepareChannelDirectCredentialReplacement(41, []ChannelDirectCredential{existing}, []ChannelDirectCredential{desired}, existing.UpdatedAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("prepare no-op replacement: %v", err)
	}
	if len(prepared) != 1 || prepared[0].ID != existing.ID || prepared[0].Revision != existing.Revision {
		t.Fatalf("no-op replacement = %+v", prepared)
	}
	if !prepared[0].UpdatedAt.Equal(existing.UpdatedAt) {
		t.Fatalf("no-op updated_at = %v, want %v", prepared[0].UpdatedAt, existing.UpdatedAt)
	}
}

func TestPrepareChannelDirectCredentialReplacementAllowsStableIDGroupSwap(t *testing.T) {
	primary := directCredentialFixture(10, "Primary")
	secondary := directCredentialFixture(11, "Secondary")
	first := primary
	first.GroupName = secondary.GroupName
	second := secondary
	second.GroupName = primary.GroupName

	prepared, err := prepareChannelDirectCredentialReplacement(41, []ChannelDirectCredential{primary, secondary}, []ChannelDirectCredential{first, second}, time.Now())
	if err != nil {
		t.Fatalf("prepare swapped groups: %v", err)
	}
	if len(prepared) != 2 || prepared[0].ID != primary.ID || prepared[0].GroupName != secondary.GroupName ||
		prepared[1].ID != secondary.ID || prepared[1].GroupName != primary.GroupName {
		t.Fatalf("swapped stable identities = %+v", prepared)
	}
	if prepared[0].Revision != primary.Revision+1 || prepared[1].Revision != secondary.Revision+1 {
		t.Fatalf("swapped revisions = %d, %d", prepared[0].Revision, prepared[1].Revision)
	}
}

func TestPrepareChannelDirectCredentialReplacementRejectsNormalizedDuplicates(t *testing.T) {
	first := directCredentialFixture(0, "Primary")
	second := directCredentialFixture(0, "primary")
	first.GroupName = "  Primary "

	_, err := prepareChannelDirectCredentialReplacement(41, nil, []ChannelDirectCredential{first, second}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("normalized duplicate error = %v", err)
	}
}

func TestPrepareChannelDirectCredentialReplacementRejectsNonFiniteRatios(t *testing.T) {
	tests := []struct {
		name            string
		ratio           float64
		completionRatio float64
	}{
		{name: "input NaN", ratio: math.NaN(), completionRatio: 1},
		{name: "input positive infinity", ratio: math.Inf(1), completionRatio: 1},
		{name: "output NaN", ratio: 1, completionRatio: math.NaN()},
		{name: "output negative infinity", ratio: 1, completionRatio: math.Inf(-1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := directCredentialFixture(0, "Primary")
			item.Ratio = test.ratio
			item.CompletionRatio = test.completionRatio
			_, err := prepareChannelDirectCredentialReplacement(41, nil, []ChannelDirectCredential{item}, time.Now())
			if err == nil || !strings.Contains(err.Error(), "invalid ratio") {
				t.Fatalf("invalid ratio error = %v", err)
			}
		})
	}
}

func TestPrepareChannelDirectCredentialReplacementRejectsRevisionOverflow(t *testing.T) {
	existing := directCredentialFixture(10, "Primary")
	existing.Revision = maxChannelDirectCredentialRevision
	desired := existing
	desired.Ratio = 2

	_, err := prepareChannelDirectCredentialReplacement(41, []ChannelDirectCredential{existing}, []ChannelDirectCredential{desired}, time.Now())
	if err == nil || !strings.Contains(err.Error(), "revision exhausted") {
		t.Fatalf("revision overflow error = %v", err)
	}
}

func TestPrepareChannelDirectCredentialReplacementRejectsReservedGroup(t *testing.T) {
	item := directCredentialFixture(0, ReservedInternalUpstreamGroupPrefix+"test")
	_, err := prepareChannelDirectCredentialReplacement(41, nil, []ChannelDirectCredential{item}, time.Now())
	if !errors.Is(err, ErrReservedInternalUpstreamGroupName) {
		t.Fatalf("reserved group error = %v", err)
	}
}
