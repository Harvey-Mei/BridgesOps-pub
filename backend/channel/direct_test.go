package channel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNormalizeDirectBaseURL(t *testing.T) {
	got, err := normalizeDirectBaseURL(" https://example.com/openai/v1/?ignored=yes#fragment ")
	if err != nil {
		t.Fatalf("normalizeDirectBaseURL: %v", err)
	}
	if got != "https://example.com/openai" {
		t.Fatalf("normalized base URL = %q", got)
	}
	for _, raw := range []string{"", "example.com/v1", "ftp://example.com/v1"} {
		if _, err := normalizeDirectBaseURL(raw); err == nil {
			t.Errorf("normalizeDirectBaseURL(%q) accepted invalid URL", raw)
		}
	}
}

func TestProbeDirectModelsUsesCompatibleHeadersAndNormalizesModels(t *testing.T) {
	const apiKey = "direct-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("request path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiKey {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("x-api-key"); got != apiKey {
			t.Errorf("x-api-key = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":" model-a "},{"id":"model-a"},{"id":"model-b"}]}`))
	}))
	defer server.Close()

	models, err := (&Service{}).ProbeDirectModels(context.Background(), server.URL+"/v1", apiKey, nil)
	if err != nil {
		t.Fatalf("ProbeDirectModels: %v", err)
	}
	if strings.Join(models, ",") != "model-a,model-b" {
		t.Fatalf("models = %#v", models)
	}
}

func TestProbeDirectModelsDoesNotLeakCredentialsOrResponseBody(t *testing.T) {
	const (
		apiKey = "direct-secret"
		body   = "provider-private-error"
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	_, err := (&Service{}).ProbeDirectModels(context.Background(), server.URL, apiKey, nil)
	if err == nil {
		t.Fatal("ProbeDirectModels unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), apiKey) || strings.Contains(err.Error(), body) {
		t.Fatalf("probe error leaked sensitive material: %q", err)
	}
}

func TestProbeDirectModelsDoesNotFollowRedirect(t *testing.T) {
	var redirected bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected = true
	}))
	defer target.Close()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()

	_, err := (&Service{}).ProbeDirectModels(context.Background(), server.URL, "direct-secret", nil)
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("ProbeDirectModels redirect error = %v", err)
	}
	if redirected {
		t.Fatal("ProbeDirectModels followed the redirect")
	}
}

func TestProbeDirectModelsClassifiesCloudflareChallenge(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("cf-mitigated", "challenge")
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("<html><title>Just a moment...</title></html>"))
	}))
	defer server.Close()

	_, err := (&Service{}).ProbeDirectModels(context.Background(), server.URL, "direct-secret", nil)
	if err == nil || !strings.Contains(err.Error(), "Cloudflare") {
		t.Fatalf("ProbeDirectModels challenge error = %v", err)
	}
	if strings.Contains(err.Error(), "direct-secret") || strings.Contains(err.Error(), "Just a moment") {
		t.Fatalf("ProbeDirectModels leaked request or response details: %v", err)
	}
}
