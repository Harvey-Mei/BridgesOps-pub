package clauderadar

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestRefreshCachesValidPayload(t *testing.T) {
	const payload = `{"updated_at":"2026-07-13T10:00:00Z","iq":{"updated_at":"2026-07-13T10:00:00Z","models":[{"key":"claude-sonnet-4-5","name":"Claude Sonnet 4.5","score":92,"iq":[92],"pass":[8],"valid":[10],"invalid":[2],"cost":[0.08],"time":[0.5],"tokens":[1000],"latest_at":"2026-07-13T10:00:00Z"}]}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q", got)
		}
		if got := r.Header.Get("User-Agent"); got == "" {
			t.Fatal("User-Agent is empty")
		}
		_, _ = io.WriteString(w, payload)
	}))
	defer srv.Close()

	s := New(Options{URL: srv.URL, Client: srv.Client()})
	snapshot, err := s.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if snapshot.Stale || string(snapshot.Payload) != payload {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if len(snapshot.History) != 1 || snapshot.History[0].ConfigKey != "claude-sonnet-4-5" {
		t.Fatalf("history = %+v", snapshot.History)
	}
}

func TestRefreshMetadataChangesDoNotCreateHistory(t *testing.T) {
	var body atomic.Value
	body.Store(`{"updated_at":"first","iq":{"updated_at":"first","notes":"initial","models":[{"key":"z-model","name":"Z","score":80,"iq":[80],"pass":[7],"valid":[9],"invalid":[2],"cost":[0.1],"time":[0.4],"tokens":[900],"latest_at":"first"},{"key":"a-model","name":"A","score":90,"iq":[90],"pass":[8],"valid":[10],"invalid":[2],"cost":[0.08],"time":[0.3],"tokens":[1000],"latest_at":"first"}]}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body.Load().(string))
	}))
	defer srv.Close()

	s := New(Options{URL: srv.URL, Client: srv.Client()})
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("first Refresh: %v", err)
	}
	body.Store(`{"updated_at":"second","iq":{"updated_at":"second","notes":"changed note","latest_policy":"new policy","models":[{"key":"a-model","name":"A","score":90,"iq":[90],"pass":[8],"valid":[10],"invalid":[2],"cost":[0.08],"time":[0.3],"tokens":[1000],"latest_at":"second"},{"key":"z-model","name":"Z","score":80,"iq":[80],"pass":[7],"valid":[9],"invalid":[2],"cost":[0.1],"time":[0.4],"tokens":[900],"latest_at":"second"}]}}`)
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("metadata Refresh: %v", err)
	}
	if got := len(s.Snapshot().History); got != 2 {
		t.Fatalf("history after metadata-only update = %d, want 2", got)
	}

	body.Store(`{"updated_at":"third","iq":{"models":[{"key":"z-model","name":"Z","score":81,"iq":[81],"pass":[7],"valid":[9],"invalid":[2],"cost":[0.1],"time":[0.4],"tokens":[900]},{"key":"a-model","name":"A","score":90,"iq":[90],"pass":[8],"valid":[10],"invalid":[2],"cost":[0.08],"time":[0.3],"tokens":[1000]}]}}`)
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("model Refresh: %v", err)
	}
	if got := len(s.Snapshot().History); got != 4 {
		t.Fatalf("history after model update = %d, want 4", got)
	}
}

func TestRefreshRejectsInvalidIQAndRetainsLastPayload(t *testing.T) {
	var body atomic.Value
	const valid = `{"iq":{"models":[{"key":"claude-haiku","name":"Claude Haiku","score":75}]}}`
	body.Store(valid)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body.Load().(string))
	}))
	defer srv.Close()

	s := New(Options{URL: srv.URL, Client: srv.Client()})
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}
	first := s.Snapshot()
	invalid := []string{
		`{"iq":{}}`,
		`{"iq":"bad"}`,
		`{"iq":{"models":null}}`,
		`{"iq":{"models":[]}}`,
		`{"iq":{"models":[{"name":"missing-key"}]}}`,
		`{"iq":{"models":"bad"}}`,
		`{"ok":false,"iq":{"models":[{"key":"claude-haiku"}]}}`,
	}
	for _, payload := range invalid {
		body.Store(payload)
		snapshot, err := s.Refresh(context.Background())
		if err == nil {
			t.Fatalf("Refresh(%s) unexpectedly succeeded", payload)
		}
		if string(snapshot.Payload) != string(first.Payload) || !snapshot.Stale {
			t.Fatalf("invalid response replaced cache for %s: %+v", payload, snapshot)
		}
	}
}
