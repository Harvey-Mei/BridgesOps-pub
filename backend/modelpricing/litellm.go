// Package modelpricing fetches untrusted external price catalogs and turns
// exact model entries into validated price candidates. It does not persist or
// activate candidates; callers must keep that policy decision separate.
package modelpricing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// LiteLLMSourceURL is deliberately not configurable. A caller-controlled URL
	// would turn a catalog refresh into an SSRF primitive.
	LiteLLMSourceURL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"

	LiteLLMMaxResponseBytes    int64   = 16 << 20
	Above272KThresholdTokens   int64   = 272_000
	MaximumUSDPerMillionTokens float64 = 1_000_000
	defaultHTTPTimeout                 = 10 * time.Second
	maximumRequestedModels             = 4096
)

var (
	ErrInvalidModelName = errors.New("model price model name is invalid")
	ErrInvalidCatalog   = errors.New("model price catalog is invalid")
	ErrResponseTooLarge = errors.New("model price catalog response is too large")
)

var liteLLMSource, _ = url.Parse(LiteLLMSourceURL)

// Tier contains USD prices per one million tokens. Pointers preserve the
// distinction between a published zero and an unpublished component.
type Tier struct {
	InputUSDPerMillion      *float64 `json:"input_usd_per_million,omitempty"`
	OutputUSDPerMillion     *float64 `json:"output_usd_per_million,omitempty"`
	CacheReadUSDPerMillion  *float64 `json:"cache_read_usd_per_million,omitempty"`
	CacheWriteUSDPerMillion *float64 `json:"cache_write_usd_per_million,omitempty"`
}

// Candidate is one exact top-level LiteLLM model entry. Above272K retains all
// four fields independently because third-party catalogs do not publish every
// cache component for every model.
type Candidate struct {
	ModelName                     string `json:"model_name"`
	Short                         Tier   `json:"short"`
	Above272K                     Tier   `json:"above_272k"`
	Above272KThresholdTokens      int64  `json:"above_272k_threshold_tokens"`
	HasAnyAbove272KPrice          bool   `json:"has_any_above_272k_price"`
	ShortCacheReadPriceKnown      bool   `json:"short_cache_read_price_known"`
	ShortCacheWritePriceKnown     bool   `json:"short_cache_write_price_known"`
	Above272KCacheReadPriceKnown  bool   `json:"above_272k_cache_read_price_known"`
	Above272KCacheWritePriceKnown bool   `json:"above_272k_cache_write_price_known"`
}

// Snapshot identifies the complete upstream response as well as the subset of
// exact entries requested by the caller. MissingModels and UnsupportedModels
// preserve input order. Unsupported entries exist in the catalog but do not
// publish both input and output token prices.
type Snapshot struct {
	SourceURL         string               `json:"source_url"`
	SHA256            string               `json:"sha256"`
	ObservedAt        time.Time            `json:"observed_at"`
	Candidates        map[string]Candidate `json:"candidates"`
	MissingModels     []string             `json:"missing_models,omitempty"`
	UnsupportedModels []string             `json:"unsupported_models,omitempty"`
}

// FetchLiteLLM downloads the fixed catalog with redirects disabled. A nil
// client uses a dedicated ten-second client; an injected client keeps its
// caller-defined timeout, transport, jar, and other settings.
func FetchLiteLLM(ctx context.Context, client *http.Client, exactModelNames []string) (*Snapshot, error) {
	if ctx == nil {
		return nil, errors.New("model price fetch context is required")
	}
	if err := validateExactModelNames(exactModelNames); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, LiteLLMSourceURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create model price request: %w", err)
	}
	if !isFixedLiteLLMURL(request.URL) {
		return nil, errors.New("fixed model price source URL is invalid")
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "BridgeOps-model-pricing")

	fetchClient := client
	if fetchClient == nil {
		fetchClient = &http.Client{Timeout: defaultHTTPTimeout}
	}
	clientCopy := *fetchClient
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	response, err := clientCopy.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch model price catalog: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch model price catalog: unexpected HTTP status %d", response.StatusCode)
	}
	if response.ContentLength > LiteLLMMaxResponseBytes {
		return nil, ErrResponseTooLarge
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, LiteLLMMaxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read model price catalog: %w", err)
	}
	if int64(len(body)) > LiteLLMMaxResponseBytes {
		return nil, ErrResponseTooLarge
	}
	return ParseLiteLLM(body, exactModelNames, time.Now().UTC())
}

// ParseLiteLLM parses only exact top-level keys requested by the caller. It
// never searches aliases, provider prefixes, or case variants.
func ParseLiteLLM(body []byte, exactModelNames []string, observedAt time.Time) (*Snapshot, error) {
	if err := validateExactModelNames(exactModelNames); err != nil {
		return nil, err
	}
	if int64(len(body)) > LiteLLMMaxResponseBytes {
		return nil, ErrResponseTooLarge
	}
	if observedAt.IsZero() {
		return nil, fmt.Errorf("%w: observed_at is required", ErrInvalidCatalog)
	}

	catalog, err := decodeLiteLLMCatalog(body)
	if err != nil {
		return nil, err
	}

	digest := sha256.Sum256(body)
	snapshot := &Snapshot{
		SourceURL:  LiteLLMSourceURL,
		SHA256:     hex.EncodeToString(digest[:]),
		ObservedAt: observedAt.UTC(),
		Candidates: make(map[string]Candidate, len(exactModelNames)),
	}
	for _, modelName := range exactModelNames {
		raw, ok := catalog[modelName]
		if !ok {
			snapshot.MissingModels = append(snapshot.MissingModels, modelName)
			continue
		}
		candidate, supported, err := parseLiteLLMCandidate(modelName, raw)
		if err != nil {
			return nil, err
		}
		if !supported {
			snapshot.UnsupportedModels = append(snapshot.UnsupportedModels, modelName)
			continue
		}
		snapshot.Candidates[modelName] = candidate
	}
	return snapshot, nil
}

func parseLiteLLMCandidate(modelName string, raw json.RawMessage) (Candidate, bool, error) {
	fields, err := decodeJSONObject(raw)
	if err != nil {
		return Candidate{}, false, fmt.Errorf("%w: model %q: %v", ErrInvalidCatalog, modelName, err)
	}
	price := func(name string) (*float64, error) {
		value, err := optionalJSONNumber(fields, name)
		if err != nil {
			return nil, fmt.Errorf("%w: model %q field %q: %v", ErrInvalidCatalog, modelName, name, err)
		}
		return value, nil
	}
	input, err := price("input_cost_per_token")
	if err != nil {
		return Candidate{}, false, err
	}
	output, err := price("output_cost_per_token")
	if err != nil {
		return Candidate{}, false, err
	}
	cacheRead, err := price("cache_read_input_token_cost")
	if err != nil {
		return Candidate{}, false, err
	}
	cacheWrite, err := price("cache_creation_input_token_cost")
	if err != nil {
		return Candidate{}, false, err
	}
	longInput, err := price("input_cost_per_token_above_272k_tokens")
	if err != nil {
		return Candidate{}, false, err
	}
	longOutput, err := price("output_cost_per_token_above_272k_tokens")
	if err != nil {
		return Candidate{}, false, err
	}
	longCacheRead, err := price("cache_read_input_token_cost_above_272k_tokens")
	if err != nil {
		return Candidate{}, false, err
	}
	longCacheWrite, err := price("cache_creation_input_token_cost_above_272k_tokens")
	if err != nil {
		return Candidate{}, false, err
	}

	short, err := tierFromPerToken(
		modelName,
		"short",
		input,
		output,
		cacheRead,
		cacheWrite,
	)
	if err != nil {
		return Candidate{}, false, err
	}
	above, err := tierFromPerToken(
		modelName,
		"above_272k",
		longInput,
		longOutput,
		longCacheRead,
		longCacheWrite,
	)
	if err != nil {
		return Candidate{}, false, err
	}
	if input == nil || output == nil {
		return Candidate{}, false, nil
	}
	hasAbove := longInput != nil || longOutput != nil || longCacheRead != nil || longCacheWrite != nil
	return Candidate{
		ModelName:                     modelName,
		Short:                         short,
		Above272K:                     above,
		Above272KThresholdTokens:      Above272KThresholdTokens,
		HasAnyAbove272KPrice:          hasAbove,
		ShortCacheReadPriceKnown:      cacheRead != nil,
		ShortCacheWritePriceKnown:     cacheWrite != nil,
		Above272KCacheReadPriceKnown:  longCacheRead != nil,
		Above272KCacheWritePriceKnown: longCacheWrite != nil,
	}, true, nil
}

func decodeLiteLLMCatalog(body []byte) (map[string]json.RawMessage, error) {
	catalog, err := decodeJSONObject(body)
	if err != nil {
		return nil, fmt.Errorf("%w: catalog JSON object is malformed", ErrInvalidCatalog)
	}
	return catalog, nil
}

func decodeJSONObject(body []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	start, err := decoder.Token()
	if err != nil {
		return nil, errors.New("JSON decoding failed")
	}
	if delimiter, ok := start.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("JSON object is required")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errors.New("JSON object key is malformed")
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("object key must be a string")
		}
		if _, duplicate := fields[key]; duplicate {
			return nil, errors.New("JSON object contains a duplicate member")
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, errors.New("JSON object member is malformed")
		}
		fields[key] = raw
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, errors.New("JSON object is incomplete")
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != '}' {
		return nil, errors.New("JSON object is incomplete")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return fields, nil
}

func optionalJSONNumber(fields map[string]json.RawMessage, name string) (*float64, error) {
	raw, ok := fields[name]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var value float64
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.New("price must be a finite JSON number")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return &value, nil
}

func tierFromPerToken(modelName, tierName string, input, output, cacheRead, cacheWrite *float64) (Tier, error) {
	converted := make([]*float64, 4)
	for index, value := range []*float64{input, output, cacheRead, cacheWrite} {
		if value == nil {
			continue
		}
		perMillion := *value * 1_000_000
		if *value < 0 || math.IsNaN(*value) || math.IsInf(*value, 0) ||
			math.IsNaN(perMillion) || math.IsInf(perMillion, 0) || perMillion > MaximumUSDPerMillionTokens {
			return Tier{}, fmt.Errorf("%w: model %q has invalid %s price", ErrInvalidCatalog, modelName, tierName)
		}
		converted[index] = &perMillion
	}
	return Tier{
		InputUSDPerMillion: converted[0], OutputUSDPerMillion: converted[1],
		CacheReadUSDPerMillion: converted[2], CacheWriteUSDPerMillion: converted[3],
	}, nil
}

func validateExactModelNames(modelNames []string) error {
	if len(modelNames) == 0 || len(modelNames) > maximumRequestedModels {
		return ErrInvalidModelName
	}
	seen := make(map[string]struct{}, len(modelNames))
	for _, name := range modelNames {
		if name == "" || strings.TrimSpace(name) != name || strings.ContainsRune(name, '\x00') ||
			!utf8.ValidString(name) || utf8.RuneCountInString(name) > 256 {
			return ErrInvalidModelName
		}
		if _, exists := seen[name]; exists {
			return ErrInvalidModelName
		}
		seen[name] = struct{}{}
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return errors.New("trailing JSON is malformed")
	}
	return nil
}

func isFixedLiteLLMURL(candidate *url.URL) bool {
	if candidate == nil || liteLLMSource == nil {
		return false
	}
	return candidate.Scheme == "https" && candidate.User == nil &&
		candidate.Host == liteLLMSource.Host && candidate.Path == liteLLMSource.Path &&
		candidate.RawQuery == "" && candidate.Fragment == ""
}
