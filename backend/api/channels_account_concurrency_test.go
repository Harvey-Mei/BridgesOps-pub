package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestChannelOutputExposesOnlyPublicAccountConcurrencyFact(t *testing.T) {
	limit := 3
	observedAt := time.Date(2026, 7, 23, 20, 0, 0, 0, time.UTC)
	output := channelOutput{
		Channel: storage.Channel{ID: 7, Name: "bounded"},
		UpstreamAccountConcurrency: &storage.UpstreamAccountConcurrencySnapshot{
			AccountRecordID: 91,
			Limit:           &limit,
			Scope:           storage.UpstreamConcurrencyScopeAccount,
			Source:          "sub2api.auth.me.concurrency",
			ObservedAt:      &observedAt,
		},
	}
	raw, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("marshal channel output: %v", err)
	}
	text := string(raw)
	for _, want := range []string{
		`"upstream_account_concurrency"`,
		`"concurrency_limit":3`,
		`"concurrency_scope":"account"`,
		`"concurrency_source":"sub2api.auth.me.concurrency"`,
		`"concurrency_observed_at":"2026-07-23T20:00:00Z"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("channel output %s missing %s", text, want)
		}
	}
	if strings.Contains(text, "account_record_id") || strings.Contains(text, `"91"`) {
		t.Fatalf("channel output leaked internal account identity: %s", text)
	}
}

func TestChannelOutputOmitsUnknownAccountConcurrency(t *testing.T) {
	raw, err := json.Marshal(channelOutput{Channel: storage.Channel{ID: 8, Name: "unknown"}})
	if err != nil {
		t.Fatalf("marshal channel output: %v", err)
	}
	if strings.Contains(string(raw), "upstream_account_concurrency") {
		t.Fatalf("unknown account concurrency was presented as a fact: %s", raw)
	}
}

func TestPublicChannelAccountConcurrencyRequiresCompleteExplicitProvenance(t *testing.T) {
	limit := 4
	observedAt := time.Now()
	valid := storage.UpstreamAccountConcurrencySnapshot{
		AccountRecordID: 31,
		Limit:           &limit,
		Scope:           storage.UpstreamConcurrencyScopeAccount,
		Source:          "sub2api.auth.me.concurrency",
		ObservedAt:      &observedAt,
	}
	if snapshot, ok := publicChannelAccountConcurrencySnapshot(valid); !ok || snapshot == nil || snapshot.AccountRecordID != 0 || snapshot.Limit == valid.Limit || snapshot.ObservedAt == valid.ObservedAt {
		t.Fatalf("valid public snapshot was not independently copied: %#v, ok=%v", snapshot, ok)
	}

	invalid := []storage.UpstreamAccountConcurrencySnapshot{
		{Scope: storage.UpstreamConcurrencyScopeAccount, Source: valid.Source, ObservedAt: &observedAt},
		{Limit: &limit, Scope: "key", Source: valid.Source, ObservedAt: &observedAt},
		{Limit: &limit, Scope: storage.UpstreamConcurrencyScopeAccount, ObservedAt: &observedAt},
		{Limit: &limit, Scope: storage.UpstreamConcurrencyScopeAccount, Source: valid.Source},
	}
	for _, snapshot := range invalid {
		if public, ok := publicChannelAccountConcurrencySnapshot(snapshot); ok || public != nil {
			t.Fatalf("incomplete concurrency fact became public: %#v", snapshot)
		}
	}
}
