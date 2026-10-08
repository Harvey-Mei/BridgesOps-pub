package api

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func projectGatewayResponseForTest(t *testing.T, payload []byte, path string) gatewayBufferedResponseFacts {
	t.Helper()
	directory, err := prepareGatewayUpstreamResponseSpoolDirectory(filepath.Join(t.TempDir(), "spool"))
	if err != nil {
		t.Fatalf("prepare spool directory: %v", err)
	}
	spool, err := spoolGatewayUpstreamResponse(context.Background(), bytes.NewReader(payload), gatewayUpstreamResponseSpoolOptions{
		TempDir: directory, MaxBytes: int64(len(payload)), PrefixBytes: min(int64(len(payload)), 32),
	})
	if err != nil {
		t.Fatalf("spool response: %v", err)
	}
	defer spool.Close()
	facts, err := projectGatewayUpstreamResponse(context.Background(), spool, gatewayUpstreamResponseProjectionOptions{RequestPath: path})
	if err != nil {
		t.Fatalf("project response: %v", err)
	}
	return facts
}

func TestProjectGatewayResponseFindsUsageAndErrorAfterLargeString(t *testing.T) {
	payload := []byte(`{"data":[{"b64_json":"` + strings.Repeat("A", 256<<10) + `"}],"usage":{"input_tokens":12,"output_tokens":5,"total_tokens":17},"cost":1.25,"response":{"status":"failed","error":{"type":"rate_limit_error","status_code":429,"message":"busy"}}}`)
	facts := projectGatewayResponseForTest(t, payload, "/v1/responses")
	if facts.Usage.PromptTokens != 12 || facts.Usage.CompletionTokens != 5 || facts.Usage.TotalTokens != 17 || !facts.Usage.CostObserved || facts.Usage.Cost != 1.25 {
		t.Fatalf("usage facts = %+v", facts.Usage)
	}
	if !facts.Usage.InputTokensPresent || !facts.Usage.OutputTokensPresent || facts.Usage.CacheCreationTokensPresent || facts.Usage.CacheReadTokensPresent {
		t.Fatalf("projected usage did not preserve missing cache components: %+v", facts.Usage)
	}
	classification := classifyGatewayStreamFailure(facts.ClassificationJSON)
	if !facts.Image.DataNonEmpty || !facts.StructuredErrorFound || !classification.Found || classification.Status != 429 {
		t.Fatalf("projection facts=%+v classification=%+v json=%s", facts, classification, facts.ClassificationJSON)
	}
}

func TestProjectGatewayResponseUsagePresencePreservesMissingAndExplicitZero(t *testing.T) {
	partial := projectGatewayResponseForTest(t, []byte(`{"usage":{"input_tokens":0}}`), "/v1/responses")
	if !partial.Usage.InputTokensPresent || partial.Usage.OutputTokensPresent || partial.Usage.CacheCreationTokensPresent || partial.Usage.CacheReadTokensPresent {
		t.Fatalf("partial projected usage was promoted to complete: %+v", partial.Usage)
	}

	fallback := projectGatewayResponseForTest(t, []byte(`{"input_tokens":0,"output_tokens":0}`), "/v1/responses")
	if !fallback.Usage.InputTokensPresent || !fallback.Usage.OutputTokensPresent || fallback.Usage.CacheCreationTokensPresent || fallback.Usage.CacheReadTokensPresent {
		t.Fatalf("fallback token fields were treated as a complete usage object: %+v", fallback.Usage)
	}

	explicitZero := projectGatewayResponseForTest(t, []byte(`{"usage":{"input_tokens":0,"output_tokens":0,"cache_write_tokens":0,"cached_tokens":0}}`), "/v1/responses")
	if !explicitZero.Usage.InputTokensPresent || !explicitZero.Usage.OutputTokensPresent || !explicitZero.Usage.CacheCreationTokensPresent || !explicitZero.Usage.CacheReadTokensPresent {
		t.Fatalf("projected explicit zero components were lost: %+v", explicitZero.Usage)
	}

	invalidCache := projectGatewayResponseForTest(t, []byte(`{"usage":{"input_tokens":12,"output_tokens":3,"cache_write_tokens":null,"cached_tokens":-1}}`), "/v1/responses")
	if !invalidCache.Usage.InputTokensPresent || !invalidCache.Usage.OutputTokensPresent || invalidCache.Usage.CacheCreationTokensPresent || invalidCache.Usage.CacheReadTokensPresent || !invalidCache.Usage.UsageInvalid {
		t.Fatalf("projected invalid cache components were promoted to explicit zeroes: %+v", invalidCache.Usage)
	}

	invalidNumbers := projectGatewayResponseForTest(t, []byte(`{"usage":{"input_tokens":1.5,"output_tokens":"2.25","cache_write_tokens":9223372036854775808,"cached_tokens":"1e30"}}`), "/v1/responses")
	if invalidNumbers.Usage.InputTokensPresent || invalidNumbers.Usage.OutputTokensPresent || invalidNumbers.Usage.CacheCreationTokensPresent || invalidNumbers.Usage.CacheReadTokensPresent || !invalidNumbers.Usage.UsageInvalid {
		t.Fatalf("projected fractional or overflowing tokens were accepted: %+v", invalidNumbers.Usage)
	}
}

func TestProjectGatewayResponsePreservesFractionalUsageCost(t *testing.T) {
	facts := projectGatewayResponseForTest(t, []byte(`{"error":{"message":"rejected"},"usage":{"input_tokens":40,"output_tokens":4,"cache_read_tokens":0,"cache_write_tokens":0,"total_tokens":44,"actual_cost":0.25}}`), "/v1/responses")
	if !facts.Usage.CostObserved || facts.Usage.CostEstimated || facts.Usage.CostSource != "actual_cost" || facts.Usage.Cost != 0.25 {
		t.Fatalf("fractional usage cost = %+v", facts.Usage)
	}
}

func TestProjectGatewayResponseInvalidAliasCannotBeMaskedByValidAlias(t *testing.T) {
	for _, test := range []struct {
		name              string
		payload           string
		wantCacheCreation int64
	}{
		{name: "scalar alias", payload: `{"usage":{"prompt_tokens":null,"input_tokens":12,"output_tokens":3,"total_tokens":15}}`},
		{name: "details alias", payload: `{"usage":{"input_tokens":12,"cache_write_tokens":4,"input_tokens_details":{"cache_creation_tokens":false},"output_tokens":3,"total_tokens":15}}`, wantCacheCreation: 4},
		{name: "container alias", payload: `{"usage":{"prompt_tokens":{},"input_tokens":12,"output_tokens":3,"total_tokens":15}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts := projectGatewayResponseForTest(t, []byte(test.payload), "/v1/responses")
			if facts.Usage.PromptTokens != 12 || facts.Usage.CompletionTokens != 3 || facts.Usage.CacheCreationTokens != test.wantCacheCreation {
				t.Fatalf("valid projected token aliases were not retained: %+v", facts.Usage)
			}
			if !facts.Usage.InputTokensPresent || !facts.Usage.OutputTokensPresent || facts.Usage.CacheCreationTokensPresent != (test.wantCacheCreation > 0) {
				t.Fatalf("valid projected token aliases lost presence: %+v", facts.Usage)
			}
			if !facts.Usage.UsageInvalid {
				t.Fatalf("valid projected token alias masked an invalid alias: %+v", facts.Usage)
			}
		})
	}
}

func TestProjectGatewayResponseRejectsAggregateTokenOverflow(t *testing.T) {
	facts := projectGatewayResponseForTest(t, []byte(`{"usage":{"input_tokens":9223372036854775807,"cache_read_input_tokens":1,"cache_creation_input_tokens":0,"output_tokens":1}}`), "/v1/messages")
	if !facts.Usage.UsageInvalid {
		t.Fatalf("projected overflowing usage was accepted: %+v", facts.Usage)
	}
	if facts.Usage.PromptTokens < 0 || facts.Usage.CompletionTokens < 0 || facts.Usage.TotalTokens < 0 {
		t.Fatalf("projected overflowing usage wrapped negative: %+v", facts.Usage)
	}
}

func TestProjectGatewayResponseDetectsCompletedImageWithoutCapturingResult(t *testing.T) {
	payload := []byte(`{"status":"completed","output":[{"type":"image_generation_call","status":"completed","result":"` + strings.Repeat("B", 256<<10) + `"}]}`)
	facts := projectGatewayResponseForTest(t, payload, "/v1/responses")
	if !facts.Image.ResponsesCompletedImageResult {
		t.Fatalf("image facts = %+v", facts.Image)
	}
	if bytes.Contains(facts.ClassificationJSON, []byte("BBBB")) {
		t.Fatal("classification projection retained image result")
	}
}

func TestProjectGatewayResponseRejectsMalformedAndOversizedMetadata(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "malformed surrogate", payload: `{"error":{"message":"\uD800"}}`, wantErr: errGatewayResponseProjectionInvalid},
		{name: "oversized error", payload: `{"error":{"message":"` + strings.Repeat("x", gatewayResponseProjectionMaxStringBytes+1) + `"}}`, wantErr: errGatewayResponseProjectionTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, err := prepareGatewayUpstreamResponseSpoolDirectory(filepath.Join(t.TempDir(), "spool"))
			if err != nil {
				t.Fatalf("prepare spool directory: %v", err)
			}
			spool, err := spoolGatewayUpstreamResponse(context.Background(), strings.NewReader(test.payload), gatewayUpstreamResponseSpoolOptions{
				TempDir: directory, MaxBytes: int64(len(test.payload)),
			})
			if err != nil {
				t.Fatalf("spool response: %v", err)
			}
			defer spool.Close()
			_, err = projectGatewayUpstreamResponse(context.Background(), spool, gatewayUpstreamResponseProjectionOptions{})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("projection error = %v, want %v", err, test.wantErr)
			}
		})
	}
}
