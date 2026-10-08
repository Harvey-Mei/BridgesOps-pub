package api

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestGatewayStreamRecordsVisibleThinkingMarkupWithoutRewritingContent(t *testing.T) {
	raw := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"<thinking>private plan</thinking>answer\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_thinking\",\"status\":\"completed\",\"error\":null}}\n\n"
	tail := &gatewayTailBuffer{limit: 4096}
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(raw)), tail, "secret", "/v1/responses")
	got, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read sanitized stream: %v", err)
	}
	if !bytes.Equal(got, []byte(raw)) {
		t.Fatalf("stream was unexpectedly rewritten:\n%s", got)
	}
	if warning := sanitizer.WarningText(); !strings.Contains(warning, "<thinking>") {
		t.Fatalf("warning = %q", warning)
	}
}

func TestGatewayStreamDoesNotFlagOrdinaryReasoningEvent(t *testing.T) {
	raw := "event: response.reasoning_summary_text.delta\ndata: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"brief summary\"}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(raw)), nil, "", "/v1/responses")
	if _, err := io.ReadAll(sanitizer); err != nil {
		t.Fatalf("read sanitized stream: %v", err)
	}
	if warning := sanitizer.WarningText(); warning != "" {
		t.Fatalf("ordinary reasoning event warning = %q", warning)
	}
}

func TestGatewayStreamDoesNotFlagThinkingMarkupInMetadata(t *testing.T) {
	raw := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"instructions\":\"explain <thinking> examples\"}}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(raw)), nil, "", "/v1/responses")
	if _, err := io.ReadAll(sanitizer); err != nil {
		t.Fatalf("read sanitized stream: %v", err)
	}
	if warning := sanitizer.WarningText(); warning != "" {
		t.Fatalf("metadata thinking markup warning = %q", warning)
	}
}
