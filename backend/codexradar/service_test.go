package codexradar

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/Harvey-Mei/BridgeOps/backend/testdb"
)

func TestRefreshCachesPayloadAndMetadata(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q", got)
		}
		if got := r.Header.Get("User-Agent"); got == "" {
			t.Fatal("User-Agent is empty")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"schema_version":"2.0","model_iq":{"latest":{"score":75}}}`)
	}))
	defer srv.Close()

	s := New(Options{URL: srv.URL})
	snap, err := s.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if snap.Stale {
		t.Fatalf("fresh snapshot stale = %v", snap.Stale)
	}
	if string(snap.Payload) != `{"schema_version":"2.0","model_iq":{"latest":{"score":75}}}` {
		t.Fatalf("payload = %s", snap.Payload)
	}
	if snap.SourceURL != srv.URL || snap.FetchedAt == nil || snap.LastAttemptAt == nil {
		t.Fatalf("metadata = %+v", snap)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func TestRefreshPersistsOnlyChangedModelSnapshotsAndRestores(t *testing.T) {
	var body atomic.Value
	body.Store(`{"monitored_at":"first","model_iq":{"comparisons":{"gpt":{"label":"GPT","latest":{"model":"gpt-test","score":75,"cost_usd":1.2,"wall_seconds":10}}}}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body.Load().(string))
	}))
	t.Cleanup(srv.Close)

	db, err := testdb.OpenWithError(t)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	store := storage.NewCodexRadarStore(db)

	s := New(Options{URL: srv.URL, Client: srv.Client(), Store: store})
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	body.Store(`{"monitored_at":"metadata-only","model_iq":{"comparisons":{"gpt":{"label":"GPT","latest":{"model":"gpt-test","score":75,"cost_usd":1.2,"wall_seconds":10}}}}}`)
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("metadata refresh: %v", err)
	}
	if got := len(s.Snapshot().History); got != 1 {
		t.Fatalf("history after metadata-only update = %d, want 1", got)
	}
	body.Store(`{"monitored_at":"changed","model_iq":{"comparisons":{"gpt":{"label":"GPT","latest":{"model":"gpt-test","score":90,"cost_usd":1.5,"wall_seconds":9}}}}}`)
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("changed refresh: %v", err)
	}
	if got := len(s.Snapshot().History); got != 2 {
		t.Fatalf("history after model update = %d, want 2", got)
	}

	restored := New(Options{URL: srv.URL, Client: srv.Client(), Store: store})
	snapshot := restored.Snapshot()
	if len(snapshot.Payload) == 0 || len(snapshot.History) != 2 {
		t.Fatalf("restored snapshot = payload %d bytes, history %d", len(snapshot.Payload), len(snapshot.History))
	}
}

func TestRefreshFailureRetainsLastSuccessfulPayload(t *testing.T) {
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			http.Error(w, "temporary outage", http.StatusBadGateway)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	}))
	defer srv.Close()

	s := New(Options{URL: srv.URL})
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}
	first := s.Snapshot()
	fail.Store(true)
	stale, err := s.Refresh(context.Background())
	if err == nil {
		t.Fatal("Refresh unexpectedly succeeded")
	}
	if stale.Stale != true {
		t.Fatalf("stale = %v", stale.Stale)
	}
	if string(stale.Payload) != string(first.Payload) {
		t.Fatalf("payload changed after failure: %s", stale.Payload)
	}
	if stale.LastError == "" || stale.LastAttemptAt == nil || stale.FetchedAt == nil {
		t.Fatalf("failure metadata = %+v", stale)
	}
}

func TestRefreshRejectsInvalidPayloadWithoutOverwriting(t *testing.T) {
	var body atomic.Value
	body.Store(`{"schema_version":"2.0","status":"ok"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body.Load().(string))
	}))
	defer srv.Close()

	s := New(Options{URL: srv.URL})
	if _, err := s.Refresh(context.Background()); err != nil {
		t.Fatalf("initial Refresh: %v", err)
	}
	first := s.Snapshot()
	body.Store("<html>not json</html>")
	if _, err := s.Refresh(context.Background()); err == nil {
		t.Fatal("invalid payload unexpectedly succeeded")
	}
	got := s.Snapshot()
	if string(got.Payload) != string(first.Payload) || !got.Stale {
		t.Fatalf("invalid response replaced cache: %+v", got)
	}
}

func TestRefreshHonorsContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := New(Options{URL: srv.URL, Client: &http.Client{Timeout: time.Second}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Refresh(ctx)
	if err == nil {
		t.Fatal("Refresh unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("error = %v", err)
	}
}
