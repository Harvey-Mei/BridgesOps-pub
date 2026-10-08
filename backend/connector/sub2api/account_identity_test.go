package sub2api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
)

func TestSub2APIAccountProfileUsesOnlyAuthenticatedAccountFields(t *testing.T) {
	observedAt := time.Date(2026, 7, 26, 1, 2, 3, 0, time.UTC)
	accountID, fact := sub2APIAccountProfile(json.RawMessage(`{
		"id": 42,
		"concurrency": 3,
		"current_concurrency": 99
	}`), false, sub2APIConcurrencySourceAuthMe, observedAt)
	if accountID != "42" || fact == nil || fact.Provider != connector.TypeSub2API || fact.AccountID != "42" ||
		fact.Scope != connector.AccountConcurrencyScopeAccount || fact.Source != sub2APIConcurrencySourceAuthMe ||
		fact.Limit == nil || *fact.Limit != 3 || !fact.ObservedAt.Equal(observedAt) {
		t.Fatalf("account identity fact = %#v", fact)
	}
}

func TestSub2APIAccountProfileRequiresCanonicalIDAndKeepsUnknownLimit(t *testing.T) {
	for _, body := range []string{`{}`, `{"id":null}`, `{"id":0}`, `{"id":""}`} {
		if accountID, fact := sub2APIAccountProfile(json.RawMessage(body), false, sub2APIConcurrencySourceAuthMe, time.Now()); accountID != "" || fact != nil {
			t.Fatalf("invalid account body %s produced account %q, fact %#v", body, accountID, fact)
		}
	}
	accountID, fact := sub2APIAccountProfile(json.RawMessage(`{"user":{"id":"acct-7"}}`), true, sub2APIConcurrencySourceLogin, time.Now())
	if accountID != "acct-7" || fact == nil || fact.AccountID != "acct-7" || fact.Limit != nil {
		t.Fatalf("unknown-limit identity = %#v", fact)
	}
}
