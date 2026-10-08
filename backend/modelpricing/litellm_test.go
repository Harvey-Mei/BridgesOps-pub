package modelpricing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseLiteLLMExactModelAndTieredPrices(t *testing.T) {
	body := []byte(`{
		"azure/gpt-test": {"input_cost_per_token": "do-not-parse"},
		"gpt-test": {
			"input_cost_per_token": 0.000004,
			"output_cost_per_token": 0.000020,
			"cache_read_input_token_cost": 0.0000004,
			"cache_creation_input_token_cost": 0.000005,
			"input_cost_per_token_above_272k_tokens": 0.000008,
			"output_cost_per_token_above_272k_tokens": 0.000030,
			"cache_read_input_token_cost_above_272k_tokens": 0.0000008,
			"cache_creation_input_token_cost_above_272k_tokens": 0.000010
		},
		"unrelated": {"input_cost_per_token": 99}
	}`)
	observedAt := time.Date(2026, 8, 30, 11, 12, 13, 456789000, time.FixedZone("test", 8*60*60))
	snapshot, err := ParseLiteLLM(body, []string{"gpt-test", "missing-model"}, observedAt)
	if err != nil {
		t.Fatalf("parse catalog: %v", err)
	}
	if snapshot.SourceURL != LiteLLMSourceURL || !snapshot.ObservedAt.Equal(observedAt.UTC()) || snapshot.ObservedAt.Location() != time.UTC {
		t.Fatalf("snapshot provenance = %+v", snapshot)
	}
	digest := sha256.Sum256(body)
	if want := hex.EncodeToString(digest[:]); snapshot.SHA256 != want {
		t.Fatalf("snapshot SHA256 = %q, want %q", snapshot.SHA256, want)
	}
	if len(snapshot.Candidates) != 1 || len(snapshot.MissingModels) != 1 || snapshot.MissingModels[0] != "missing-model" {
		t.Fatalf("snapshot candidates = %+v, missing = %+v", snapshot.Candidates, snapshot.MissingModels)
	}
	candidate := snapshot.Candidates["gpt-test"]
	if candidate.ModelName != "gpt-test" || candidate.Above272KThresholdTokens != 272_000 || !candidate.HasAnyAbove272KPrice {
		t.Fatalf("candidate metadata = %+v", candidate)
	}
	assertPrice(t, "short input", candidate.Short.InputUSDPerMillion, 4)
	assertPrice(t, "short output", candidate.Short.OutputUSDPerMillion, 20)
	assertPrice(t, "short cache read", candidate.Short.CacheReadUSDPerMillion, 0.4)
	assertPrice(t, "short cache write", candidate.Short.CacheWriteUSDPerMillion, 5)
	assertPrice(t, "long input", candidate.Above272K.InputUSDPerMillion, 8)
	assertPrice(t, "long output", candidate.Above272K.OutputUSDPerMillion, 30)
	assertPrice(t, "long cache read", candidate.Above272K.CacheReadUSDPerMillion, 0.8)
	assertPrice(t, "long cache write", candidate.Above272K.CacheWriteUSDPerMillion, 10)
	if !candidate.ShortCacheReadPriceKnown || !candidate.ShortCacheWritePriceKnown ||
		!candidate.Above272KCacheReadPriceKnown || !candidate.Above272KCacheWritePriceKnown {
		t.Fatalf("candidate cache presence = %+v", candidate)
	}
}

func TestParseLiteLLMPreservesMissingCachePrices(t *testing.T) {
	snapshot, err := ParseLiteLLM(
		[]byte(`{"model-no-cache":{"input_cost_per_token":0,"output_cost_per_token":0}}`),
		[]string{"model-no-cache"},
		time.Now(),
	)
	if err != nil {
		t.Fatalf("parse catalog: %v", err)
	}
	candidate := snapshot.Candidates["model-no-cache"]
	assertPrice(t, "published zero input", candidate.Short.InputUSDPerMillion, 0)
	assertPrice(t, "published zero output", candidate.Short.OutputUSDPerMillion, 0)
	if candidate.Short.CacheReadUSDPerMillion != nil || candidate.Short.CacheWriteUSDPerMillion != nil ||
		candidate.ShortCacheReadPriceKnown || candidate.ShortCacheWritePriceKnown || candidate.HasAnyAbove272KPrice {
		t.Fatalf("missing cache price became a published zero: %+v", candidate)
	}
}

func TestParseLiteLLMSkipsModelsWithoutCompleteTokenPrices(t *testing.T) {
	body := []byte(`{
		"gpt-5.6-sol":{"input_cost_per_token":0.000004,"output_cost_per_token":0.000020,"cache_read_input_token_cost":0.0000004,"cache_creation_input_token_cost":0.000005},
		"gpt-image-1":{"input_cost_per_token":0.000005,"output_cost_per_image_token":0.000040},
		"null-input":{"input_cost_per_token":null,"output_cost_per_token":0.000020}
	}`)
	snapshot, err := ParseLiteLLM(body, []string{"gpt-image-1", "gpt-5.6-sol", "null-input"}, time.Now())
	if err != nil {
		t.Fatalf("parse mixed catalog: %v", err)
	}
	candidate := snapshot.Candidates["gpt-5.6-sol"]
	if len(snapshot.Candidates) != 1 || candidate.ModelName != "gpt-5.6-sol" {
		t.Fatalf("valid candidate was not preserved: %+v", snapshot.Candidates)
	}
	assertPrice(t, "production model input", candidate.Short.InputUSDPerMillion, 4)
	assertPrice(t, "production model output", candidate.Short.OutputUSDPerMillion, 20)
	assertPrice(t, "production model cache read", candidate.Short.CacheReadUSDPerMillion, 0.4)
	assertPrice(t, "production model cache write", candidate.Short.CacheWriteUSDPerMillion, 5)
	if len(snapshot.UnsupportedModels) != 2 || snapshot.UnsupportedModels[0] != "gpt-image-1" || snapshot.UnsupportedModels[1] != "null-input" {
		t.Fatalf("unsupported models = %+v", snapshot.UnsupportedModels)
	}
	if len(snapshot.MissingModels) != 0 {
		t.Fatalf("present unsupported models were classified as missing: %+v", snapshot.MissingModels)
	}
}

func TestParseLiteLLMRejectsMalformedCatalogs(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "top-level array", body: `[]`},
		{name: "trailing JSON", body: `{"model":{"input_cost_per_token":1,"output_cost_per_token":1}} {}`},
		{name: "duplicate exact key", body: `{"model":{"input_cost_per_token":1,"output_cost_per_token":1},"model":{"input_cost_per_token":1,"output_cost_per_token":1}}`},
		{name: "duplicate price field", body: `{"model":{"input_cost_per_token":1,"input_cost_per_token":0,"output_cost_per_token":1}}`},
		{name: "wrong type", body: `{"model":{"input_cost_per_token":"1","output_cost_per_token":1}}`},
		{name: "negative short", body: `{"model":{"input_cost_per_token":-1,"output_cost_per_token":1}}`},
		{name: "missing output does not hide negative input", body: `{"model":{"input_cost_per_token":-1}}`},
		{name: "missing output does not hide malformed cache", body: `{"model":{"input_cost_per_token":1,"cache_read_input_token_cost":"1"}}`},
		{name: "excessive short", body: `{"model":{"input_cost_per_token":1.000001,"output_cost_per_token":1}}`},
		{name: "negative long", body: `{"model":{"input_cost_per_token":0.1,"output_cost_per_token":0.1,"input_cost_per_token_above_272k_tokens":-1}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseLiteLLM([]byte(test.body), []string{"model"}, time.Now())
			if !errors.Is(err, ErrInvalidCatalog) {
				t.Fatalf("error = %v, want ErrInvalidCatalog", err)
			}
		})
	}
	if _, err := ParseLiteLLM([]byte(`{"model":{"input_cost_per_token":1,"output_cost_per_token":1}}`), []string{"model"}, time.Time{}); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("zero observed_at error = %v", err)
	}
}

func TestParseLiteLLMPriceFieldNamesAreCaseSensitive(t *testing.T) {
	t.Run("case variant required fields are absent", func(t *testing.T) {
		snapshot, err := ParseLiteLLM(
			[]byte(`{"model":{"Input_Cost_Per_Token":0.000004,"OUTPUT_COST_PER_TOKEN":0.000020}}`),
			[]string{"model"},
			time.Now(),
		)
		if err != nil {
			t.Fatalf("parse catalog: %v", err)
		}
		if len(snapshot.Candidates) != 0 || len(snapshot.UnsupportedModels) != 1 || snapshot.UnsupportedModels[0] != "model" {
			t.Fatalf("case-variant required fields were accepted: %+v", snapshot)
		}
	})

	t.Run("case variant optional field is ignored", func(t *testing.T) {
		snapshot, err := ParseLiteLLM(
			[]byte(`{"model":{"input_cost_per_token":0.000004,"output_cost_per_token":0.000020,"Cache_Read_Input_Token_Cost":0.99}}`),
			[]string{"model"},
			time.Now(),
		)
		if err != nil {
			t.Fatalf("parse catalog: %v", err)
		}
		candidate := snapshot.Candidates["model"]
		if candidate.Short.CacheReadUSDPerMillion != nil || candidate.ShortCacheReadPriceKnown {
			t.Fatalf("case-variant cache field was accepted: %+v", candidate)
		}
	})
}

func TestParseLiteLLMMalformedErrorsDoNotLeakUpstreamData(t *testing.T) {
	t.Run("long numeric literal", func(t *testing.T) {
		literal := strings.Repeat("9", 8192)
		body := []byte(`{"model":{"input_cost_per_token":` + literal + `,"output_cost_per_token":1}}`)
		_, err := ParseLiteLLM(body, []string{"model"}, time.Now())
		if !errors.Is(err, ErrInvalidCatalog) {
			t.Fatalf("error = %v, want ErrInvalidCatalog", err)
		}
		if strings.Contains(err.Error(), literal) || len(err.Error()) > 512 {
			t.Fatalf("error leaked or amplified numeric literal: length=%d", len(err.Error()))
		}
	})

	t.Run("long duplicate object key", func(t *testing.T) {
		key := strings.Repeat("private-upstream-key-", 512)
		body := []byte(`{"` + key + `":{},"` + key + `":{},"model":{"input_cost_per_token":1,"output_cost_per_token":1}}`)
		_, err := ParseLiteLLM(body, []string{"model"}, time.Now())
		if !errors.Is(err, ErrInvalidCatalog) {
			t.Fatalf("error = %v, want ErrInvalidCatalog", err)
		}
		if strings.Contains(err.Error(), key) || len(err.Error()) > 512 {
			t.Fatalf("error leaked or amplified duplicate key: length=%d", len(err.Error()))
		}
	})
}

func TestParseLiteLLMValidatesExactModelNames(t *testing.T) {
	body := []byte(`{"model":{"input_cost_per_token":1,"output_cost_per_token":1}}`)
	invalid := [][]string{
		nil,
		{},
		{""},
		{" model"},
		{"model "},
		{"model\x00name"},
		{"model", "model"},
		{strings.Repeat("x", 257)},
	}
	for _, names := range invalid {
		if _, err := ParseLiteLLM(body, names, time.Now()); !errors.Is(err, ErrInvalidModelName) {
			t.Fatalf("model names %#v error = %v", names, err)
		}
	}
}

func TestFetchLiteLLMUsesFixedURLAndInjectedClient(t *testing.T) {
	body := []byte(`{"gpt-test":{"input_cost_per_token":0.000004,"output_cost_per_token":0.00002}}`)
	var originalRedirectCalls atomic.Int64
	client := &http.Client{
		Timeout: 37 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			originalRedirectCalls.Add(1)
			return nil
		},
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodGet || request.URL.String() != LiteLLMSourceURL {
				t.Fatalf("request target = %s %s", request.Method, request.URL)
			}
			if request.URL.Scheme != "https" || request.URL.Host != "raw.githubusercontent.com" ||
				request.URL.Path != "/BerriAI/litellm/main/model_prices_and_context_window.json" || request.URL.RawQuery != "" {
				t.Fatalf("request URL = %+v", request.URL)
			}
			if request.Header.Get("Accept") != "application/json" || request.Header.Get("User-Agent") != "BridgeOps-model-pricing" {
				t.Fatalf("request headers = %+v", request.Header)
			}
			return &http.Response{
				StatusCode:    http.StatusOK,
				Header:        http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
				Body:          io.NopCloser(strings.NewReader(string(body))),
				ContentLength: int64(len(body)),
				Request:       request,
			}, nil
		}),
	}
	snapshot, err := FetchLiteLLM(context.Background(), client, []string{"gpt-test"})
	if err != nil {
		t.Fatalf("fetch catalog: %v", err)
	}
	if len(snapshot.Candidates) != 1 || snapshot.ObservedAt.IsZero() || snapshot.SourceURL != LiteLLMSourceURL {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if client.Timeout != 37*time.Second || client.CheckRedirect == nil || originalRedirectCalls.Load() != 0 {
		t.Fatalf("injected client was mutated or its redirect policy was used")
	}
	if err := client.CheckRedirect(&http.Request{}, nil); err != nil || originalRedirectCalls.Load() != 1 {
		t.Fatalf("original redirect policy was not preserved: calls=%d err=%v", originalRedirectCalls.Load(), err)
	}
}

func TestFetchLiteLLMRejectsRedirectWithoutFollowing(t *testing.T) {
	var calls atomic.Int64
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{"https://example.invalid/private"}},
			Body:       io.NopCloser(strings.NewReader("redirect body must not be parsed")),
			Request:    request,
		}, nil
	})}
	_, err := FetchLiteLLM(context.Background(), client, []string{"gpt-test"})
	if err == nil || !strings.Contains(err.Error(), "HTTP status 302") {
		t.Fatalf("redirect error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("transport calls = %d, want 1", calls.Load())
	}
}

func TestFetchLiteLLMEnforcesResponseLimit(t *testing.T) {
	t.Run("declared length", func(t *testing.T) {
		client := staticResponseClient(&http.Response{
			StatusCode:    http.StatusOK,
			Body:          io.NopCloser(strings.NewReader("{}")),
			ContentLength: LiteLLMMaxResponseBytes + 1,
		})
		if _, err := FetchLiteLLM(context.Background(), client, []string{"model"}); !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("streamed length", func(t *testing.T) {
		client := staticResponseClient(&http.Response{
			StatusCode:    http.StatusOK,
			Body:          io.NopCloser(io.LimitReader(zeroReader{}, LiteLLMMaxResponseBytes+1)),
			ContentLength: -1,
		})
		if _, err := FetchLiteLLM(context.Background(), client, []string{"model"}); !errors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestFetchLiteLLMRejectsNonSuccessWithoutLeakingBody(t *testing.T) {
	client := staticResponseClient(&http.Response{
		StatusCode:    http.StatusBadGateway,
		Body:          io.NopCloser(strings.NewReader("private upstream response")),
		ContentLength: int64(len("private upstream response")),
	})
	_, err := FetchLiteLLM(context.Background(), client, []string{"model"})
	if err == nil || !strings.Contains(err.Error(), "HTTP status 502") || strings.Contains(err.Error(), "private") {
		t.Fatalf("error = %v", err)
	}
}

func TestFixedLiteLLMURLValidation(t *testing.T) {
	valid, err := url.Parse(LiteLLMSourceURL)
	if err != nil || !isFixedLiteLLMURL(valid) {
		t.Fatalf("fixed URL is invalid: %v", err)
	}
	for _, raw := range []string{
		"http://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json",
		"https://raw.githubusercontent.com.evil.invalid/BerriAI/litellm/main/model_prices_and_context_window.json",
		"https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json?ref=other",
		"https://raw.githubusercontent.com/BerriAI/litellm/main/other.json",
	} {
		candidate, parseErr := url.Parse(raw)
		if parseErr == nil && isFixedLiteLLMURL(candidate) {
			t.Fatalf("unsafe URL accepted: %s", raw)
		}
	}
}

func assertPrice(t *testing.T, name string, got *float64, want float64) {
	t.Helper()
	if got == nil || math.Abs(*got-want) > 1e-12 {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func staticResponseClient(response *http.Response) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response.Request = request
		return response, nil
	})}
}

type zeroReader struct{}

func (zeroReader) Read(buffer []byte) (int, error) {
	for index := range buffer {
		buffer[index] = 0
	}
	return len(buffer), nil
}
