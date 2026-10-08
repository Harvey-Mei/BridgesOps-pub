package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	gatewayservice "github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

type gatewaySSEProtocolCase struct {
	name         string
	path         string
	partial      string
	terminal     string
	wantSequence int64
}

func gatewaySSEProtocolCases() []gatewaySSEProtocolCase {
	return []gatewaySSEProtocolCase{
		{
			name: "responses",
			path: "/v1/responses",
			partial: "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_7\"}}\n\n" +
				"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":8,\"delta\":\"partial\"}\n\n",
			terminal:     "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":9,\"response\":{\"id\":\"resp_7\",\"status\":\"completed\"}}\n\n",
			wantSequence: 9,
		},
		{
			name:     "chat completions",
			path:     "/v1/chat/completions",
			partial:  "data: {\"id\":\"chat_7\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n",
			terminal: "data: [DONE]\n\n",
		},
		{
			name:     "legacy completions",
			path:     "/v1/completions",
			partial:  "data: {\"id\":\"cmpl_7\",\"object\":\"text_completion\",\"choices\":[{\"text\":\"partial\",\"index\":0,\"logprobs\":null,\"finish_reason\":null}]}\n\n",
			terminal: "data: [DONE]\n\n",
		},
		{
			name: "claude messages",
			path: "/v1/messages",
			partial: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_7\",\"role\":\"assistant\"}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n",
			terminal: "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
		},
	}
}

func readGatewaySSEProtocolStream(t *testing.T, body string, path string) (*gatewaySSEStreamSanitizer, string, error) {
	t.Helper()
	sanitizer := newGatewaySSEStreamSanitizer(
		io.NopCloser(strings.NewReader(body)),
		&gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit},
		"sk-private",
		path,
	)
	output, err := io.ReadAll(sanitizer)
	return sanitizer, string(output), err
}

func TestGatewayResponsesTransientFailureAfterAcceptanceBecomesRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_retryable\"}}\n\n" +
		"event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"sequence_number\":8,\"response\":{\"id\":\"resp_retryable\",\"status\":\"in_progress\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":9,\"output_index\":0,\"item\":{\"id\":\"msg_existing\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":10,\"item_id\":\"msg_existing\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[]}}\n\n" +
		"event: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":11,\"response\":{\"id\":\"resp_retryable\",\"status\":\"failed\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(
		io.NopCloser(strings.NewReader(body)),
		&gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit},
		"sk-private",
		"/v1/responses",
	)
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read retry prompt stream: %v", err)
	}
	text := string(output)
	if !sanitizer.RetryPrompted() || !sanitizer.protocolTerminal || !strings.Contains(text, "event: response.output_text.delta\n") || !strings.Contains(text, "event: response.completed\n") || !strings.Contains(text, gatewayPublicRetryPromptMessage) {
		t.Fatalf("retry prompt state=%t terminal=%t output=%q", sanitizer.RetryPrompted(), sanitizer.protocolTerminal, text)
	}
	if strings.Contains(text, "event: response.failed") || strings.Contains(text, "currently overloaded") || !strings.Contains(sanitizer.ErrorText(), "currently overloaded") {
		t.Fatalf("retry prompt leaked or lost failure detail: output=%q error=%q", text, sanitizer.ErrorText())
	}
	payloads := gatewayStreamJSONPayloads(output)
	if got := gatewayTestResponseEventTypes(payloads); got != "response.created,response.in_progress,response.output_item.added,response.content_part.added,response.output_text.delta,response.output_text.done,response.content_part.done,response.output_item.done,response.completed" {
		t.Fatalf("retry prompt event lifecycle=%q output=%q", got, text)
	}
	terminal := payloads[len(payloads)-1]
	response, _ := terminal["response"].(map[string]any)
	if terminal["type"] != "response.completed" || terminal["sequence_number"] != float64(15) || response["id"] != "resp_retryable" || response["status"] != "completed" || response["error"] != nil {
		t.Fatalf("retry prompt terminal=%#v", terminal)
	}
	items, _ := response["output"].([]any)
	if len(items) != 1 {
		t.Fatalf("retry prompt output=%#v", response["output"])
	}
	item, _ := items[0].(map[string]any)
	if item["id"] != "msg_existing" {
		t.Fatalf("retry prompt did not reuse pending item: %#v", item)
	}
}

func TestGatewayResponsesOverloadWithoutSequenceBeforeOutputBecomesRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_missing_sequence\"}}\n\n" +
		"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_missing_sequence\",\"status\":\"failed\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(
		io.NopCloser(strings.NewReader(body)),
		&gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit},
		"sk-private",
		"/v1/responses",
	)
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read missing-sequence overload stream: %v", err)
	}
	text := string(output)
	if !sanitizer.RetryPrompted() || !strings.Contains(text, "event: response.completed\n") || strings.Contains(text, "event: response.failed\n") || !strings.Contains(text, gatewayPublicRetryPromptMessage) {
		t.Fatalf("missing-sequence overload prompted=%t output=%q", sanitizer.RetryPrompted(), text)
	}
	if got := gatewayTestResponseEventTypes(gatewayStreamJSONPayloads(output)); got != "response.created,response.output_item.added,response.content_part.added,response.output_text.delta,response.output_text.done,response.content_part.done,response.output_item.done,response.completed" {
		t.Fatalf("missing-sequence overload lifecycle=%q output=%q", got, text)
	}
}

func TestGatewayResponsesStreamReadErrorAfterAcceptanceBecomesRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_stream_read_error\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_stream_read_error\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_stream_read_error\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":4,\"response\":{\"id\":\"resp_stream_read_error\",\"status\":\"failed\",\"error\":{\"type\":\"stream_read_error\",\"message\":\"private upstream read detail\"}}}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(body)), nil, "", "/v1/responses")
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read stream_read_error retry stream: %v", err)
	}
	text := string(output)
	if !sanitizer.RetryPrompted() || !strings.Contains(text, "event: response.completed\n") || strings.Contains(text, "event: response.failed\n") || strings.Contains(text, "private upstream read detail") || !strings.Contains(sanitizer.ErrorText(), "private upstream read detail") {
		t.Fatalf("stream_read_error prompted=%t error=%q output=%q", sanitizer.RetryPrompted(), sanitizer.ErrorText(), text)
	}
}

func TestGatewayResponsesMalformedFrameAfterAcceptanceBecomesRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_malformed\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"delta\":\"truncated\"\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(
		io.NopCloser(strings.NewReader(body)),
		&gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit},
		"sk-private",
		"/v1/responses",
	)
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read malformed retry prompt stream: %v", err)
	}
	text := string(output)
	if !sanitizer.RetryPrompted() || !strings.Contains(text, "event: response.completed\n") || !strings.Contains(text, gatewayPublicRetryPromptMessage) {
		t.Fatalf("malformed retry prompt=%q state=%t", text, sanitizer.RetryPrompted())
	}
	if strings.Contains(text, "truncated") || strings.Contains(text, "response.failed") || !strings.Contains(sanitizer.ErrorText(), "malformed JSON frame") {
		t.Fatalf("malformed frame leaked or lost audit detail: output=%q error=%q", text, sanitizer.ErrorText())
	}
}

func TestGatewayResponsesMalformedFrameAfterReconstructableTextBecomesRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_partial\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_partial\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":5,\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"delta\":\"truncated\"\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(body)), nil, "", "/v1/responses")
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read malformed partial stream: %v", err)
	}
	text := string(output)
	if !sanitizer.RetryPrompted() || !strings.Contains(text, "visible") || !strings.Contains(text, "event: response.completed\n") || strings.Contains(text, "event: response.failed\n") || !strings.Contains(text, gatewayPublicRetryPromptMessage) || strings.Contains(text, "truncated") {
		t.Fatalf("malformed partial output=%q retry_prompted=%t", text, sanitizer.RetryPrompted())
	}
}

func gatewayTestResponseEventTypes(payloads []map[string]any) string {
	types := make([]string, 0, len(payloads))
	for _, payload := range payloads {
		types = append(types, gatewayStreamFailureString(payload["type"]))
	}
	return strings.Join(types, ",")
}

func TestGatewayResponsesPermanentFailureAfterAcceptanceStaysFailed(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":2,\"response\":{\"id\":\"resp_permanent\"}}\n\n" +
		"event: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":3,\"response\":{\"id\":\"resp_permanent\",\"status\":\"failed\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"invalid request\"}}}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(
		io.NopCloser(strings.NewReader(body)),
		&gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit},
		"sk-private",
		"/v1/responses",
	)
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read permanent failure stream: %v", err)
	}
	text := string(output)
	if sanitizer.RetryPrompted() || !strings.Contains(text, "event: response.failed\n") || strings.Contains(text, gatewayPublicRetryPromptMessage) || strings.Contains(text, "invalid request") {
		t.Fatalf("permanent failure output=%q retry_prompted=%t", text, sanitizer.RetryPrompted())
	}
}

func TestGatewayResponsesAcceptedEOFBecomesRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":4,\"response\":{\"id\":\"resp_eof\"}}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(
		io.NopCloser(strings.NewReader(body)),
		&gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit},
		"sk-private",
		"/v1/responses",
	)
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read accepted EOF stream: %v", err)
	}
	text := string(output)
	if !sanitizer.RetryPrompted() || !strings.Contains(text, "event: response.completed\n") || !strings.Contains(text, gatewayPublicRetryPromptMessage) || strings.Contains(text, "event: response.failed") {
		t.Fatalf("accepted EOF retry prompt=%q state=%t", text, sanitizer.RetryPrompted())
	}
	if got := gatewayTestResponseEventTypes(gatewayStreamJSONPayloads(output)); got != "response.created,response.output_item.added,response.content_part.added,response.output_text.delta,response.output_text.done,response.content_part.done,response.output_item.done,response.completed" {
		t.Fatalf("accepted EOF lifecycle=%q output=%q", got, text)
	}
}

func TestGatewayResponsesPartialTextEOFBecomesCompletedRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":4,\"response\":{\"id\":\"resp_partial_eof\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":5,\"output_index\":0,\"item\":{\"id\":\"msg_partial\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":6,\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[],\"logprobs\":[]}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":7,\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial \",\"logprobs\":[]}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":8,\"item_id\":\"msg_partial\",\"output_index\":0,\"content_index\":0,\"delta\":\"answer. \",\"logprobs\":[]}\n\n"
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(body)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", "/v1/responses")
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read partial EOF retry stream: %v", err)
	}
	if !sanitizer.RetryPrompted() || !strings.Contains(string(output), "event: response.completed\n") || strings.Contains(string(output), "event: response.failed\n") || !strings.Contains(sanitizer.ErrorText(), "unexpected EOF") {
		t.Fatalf("partial EOF state prompted=%t error=%q output=%q", sanitizer.RetryPrompted(), sanitizer.ErrorText(), output)
	}
	payloads := gatewayStreamJSONPayloads(output)
	if got := gatewayTestResponseEventTypes(payloads); got != "response.created,response.output_item.added,response.content_part.added,response.output_text.delta,response.output_text.delta,response.output_text.delta,response.output_text.done,response.content_part.done,response.output_item.done,response.completed" {
		t.Fatalf("partial EOF lifecycle=%q output=%q", got, output)
	}
	wantText := "partial answer. " + gatewayPublicRetryPromptMessage
	for _, payload := range payloads {
		switch payload["type"] {
		case "response.output_text.done":
			if payload["text"] != wantText {
				t.Fatalf("output_text.done text=%#v want=%q", payload["text"], wantText)
			}
		case "response.content_part.done":
			part, _ := payload["part"].(map[string]any)
			if part["text"] != wantText {
				t.Fatalf("content_part.done text=%#v want=%q", part["text"], wantText)
			}
		case "response.output_item.done":
			item, _ := payload["item"].(map[string]any)
			content, _ := item["content"].([]any)
			part, _ := content[0].(map[string]any)
			if part["text"] != wantText {
				t.Fatalf("output_item.done text=%#v want=%q", part["text"], wantText)
			}
		case "response.completed":
			response, _ := payload["response"].(map[string]any)
			outputItems, _ := response["output"].([]any)
			item, _ := outputItems[0].(map[string]any)
			content, _ := item["content"].([]any)
			part, _ := content[0].(map[string]any)
			if part["text"] != wantText {
				t.Fatalf("response.completed text=%#v want=%q", part["text"], wantText)
			}
		}
	}
	for index, payload := range payloads {
		if payload["sequence_number"] != float64(index+4) {
			t.Fatalf("payload %d sequence=%#v want=%d", index, payload["sequence_number"], index+4)
		}
	}
}

func TestGatewayResponsesPartialTextReadErrorBecomesCompletedRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_read_error\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_read_error\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_read_error\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_read_error\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n"
	sourceErr := errors.New("secret transport EOF")
	source := &gatewaySSEProtocolErrorReader{reader: strings.NewReader(body), err: sourceErr}
	sanitizer := newGatewaySSEStreamSanitizer(source, nil, "sk-private", "/v1/responses")
	sanitizer.retryPromptEnabled = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read partial source error: %v", err)
	}
	if !sanitizer.RetryPrompted() || !strings.Contains(string(output), "event: response.completed\n") || strings.Contains(string(output), "secret transport EOF") || !strings.Contains(sanitizer.ErrorText(), "secret transport EOF") {
		t.Fatalf("partial source error prompted=%t error=%q output=%q", sanitizer.RetryPrompted(), sanitizer.ErrorText(), output)
	}
}

func TestGatewayResponsesPartialTextUpstreamContextCanceledBecomesCompletedRetryPrompt(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_context_canceled\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_context_canceled\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_context_canceled\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_context_canceled\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n"
	source := &gatewaySSEProtocolErrorReader{reader: strings.NewReader(body), err: context.Canceled}
	sanitizer := newGatewaySSEStreamSanitizer(source, nil, "sk-private", "/v1/responses")
	sanitizer.retryPromptEnabled = true
	sanitizer.clientCanceled = func() bool { return false }
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read upstream context cancellation: %v", err)
	}
	if !sanitizer.RetryPrompted() || !strings.Contains(string(output), "event: response.completed\n") || strings.Contains(string(output), "event: response.failed\n") || !strings.Contains(sanitizer.ErrorText(), "context canceled") {
		t.Fatalf("upstream context cancellation prompted=%t error=%q output=%q", sanitizer.RetryPrompted(), sanitizer.ErrorText(), output)
	}
}

func TestGatewayResponsesPartialTextDownstreamContextCanceledDoesNotSynthesizeCompletion(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_client_canceled\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_client_canceled\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_client_canceled\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_client_canceled\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n"
	source := &gatewaySSEProtocolErrorReader{reader: strings.NewReader(body), err: context.Canceled}
	sanitizer := newGatewaySSEStreamSanitizer(source, nil, "sk-private", "/v1/responses")
	sanitizer.retryPromptEnabled = true
	sanitizer.clientCanceled = func() bool { return true }
	output, err := io.ReadAll(sanitizer)
	if !errors.Is(err, context.Canceled) || sanitizer.RetryPrompted() || strings.Contains(string(output), "event: response.completed\n") || strings.Contains(string(output), gatewayPublicRetryPromptMessage) {
		t.Fatalf("downstream cancellation err=%v prompted=%t output=%q", err, sanitizer.RetryPrompted(), output)
	}
}

func TestGatewayResponsesPartialTextRetryRejectsIncompatibleLifecycle(t *testing.T) {
	created := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_reject\"}}\n\n"
	item := "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_reject\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n"
	part := "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_reject\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n"
	validDelta := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_reject\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n"
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "missing item and part", body: created + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"item_id\":\"msg_reject\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n"},
		{name: "wrong item id", body: created + item + part + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_other\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n"},
		{name: "regressed sequence", body: created + item + part + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_reject\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n"},
		{name: "missing sequence", body: created + item + part + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_reject\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\"}\n\n"},
		{name: "nonempty logprobs", body: created + item + part + "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_reject\",\"output_index\":0,\"content_index\":0,\"delta\":\"visible\",\"logprobs\":[{}]}\n\n"},
		{name: "done event", body: created + item + part + validDelta + "event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"sequence_number\":5,\"item_id\":\"msg_reject\",\"output_index\":0,\"content_index\":0,\"text\":\"visible\"}\n\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(test.body)), nil, "", "/v1/responses")
			sanitizer.retryPromptEnabled = true
			output, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read incompatible partial stream: %v", err)
			}
			if sanitizer.RetryPrompted() || !strings.Contains(string(output), "event: response.failed\n") || strings.Contains(string(output), gatewayPublicRetryPromptMessage) {
				t.Fatalf("incompatible partial prompted=%t output=%q", sanitizer.RetryPrompted(), output)
			}
		})
	}
}

func TestGatewayResponsesPartialTextRetryRejectsTextOverflow(t *testing.T) {
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader("")), nil, "", "/v1/responses")
	sanitizer.retryPromptEnabled = true
	sanitizer.accepted = true
	sanitizer.useful = true
	sanitizer.retryPromptEligible = true
	sanitizer.retryPromptTransportFailure = true
	sanitizer.retryPromptResponseCreated = true
	sanitizer.retryPromptItemStarted = true
	sanitizer.retryPromptPartStarted = true
	sanitizer.retryPromptMessageID = "msg_overflow"
	sanitizer.retryPromptResponseID = "resp_overflow"
	sanitizer.retryPromptText = strings.Repeat("x", gatewayResponsesRetryPromptTextLimit)
	sanitizer.retryPromptTextDeltaSeen = true
	sanitizer.retryPromptSequenceSeen = true
	sanitizer.retryPromptSequenceComplete = true
	delta := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":9,\"item_id\":\"msg_overflow\",\"output_index\":0,\"content_index\":0,\"delta\":\"y\"}\n\n"
	sanitizer.observeResponsesRetryPromptStructure([]byte(delta))
	sanitizer.appendPublicStreamErrorFrame(nil)
	if sanitizer.RetryPrompted() || sanitizer.retryPromptStructureCompatible || !strings.Contains(string(sanitizer.pending), "event: response.failed\n") {
		t.Fatalf("overflow state prompted=%t compatible=%t output=%q", sanitizer.RetryPrompted(), sanitizer.retryPromptStructureCompatible, sanitizer.pending)
	}
}

func TestGatewayResponsesIncompatibleAcceptedStructureStaysFailed(t *testing.T) {
	failure := "event: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":3,\"response\":{\"id\":\"resp_1\",\"status\":\"failed\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"temporarily unavailable\"}}}\n\n"
	for _, test := range []struct {
		name   string
		prefix string
	}{
		{
			name: "reasoning item",
			prefix: "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\"}}\n\n" +
				"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"reasoning_1\",\"type\":\"reasoning\",\"status\":\"in_progress\",\"content\":[]}}\n\n",
		},
		{
			name: "user message item",
			prefix: "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\"}}\n\n" +
				"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_user\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"user\",\"content\":[]}}\n\n",
		},
		{
			name:   "in progress before created",
			prefix: "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n",
		},
		{
			name: "response id drift",
			prefix: "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\"}}\n\n" +
				"event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"sequence_number\":2,\"response\":{\"id\":\"resp_other\",\"status\":\"in_progress\"}}\n\n",
		},
		{
			name: "queued with output",
			prefix: "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\"}}\n\n" +
				"event: response.queued\ndata: {\"type\":\"response.queued\",\"sequence_number\":2,\"response\":{\"id\":\"resp_1\",\"status\":\"queued\",\"output\":[{\"type\":\"message\"}]}}\n\n",
		},
		{
			name: "in progress with output",
			prefix: "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\"}}\n\n" +
				"event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"sequence_number\":2,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\",\"output\":[{\"type\":\"message\"}]}}\n\n",
		},
		{
			name:   "created with failed status",
			prefix: "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\",\"status\":\"failed\",\"error\":{\"message\":\"private\"}}}\n\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(test.prefix+failure)), nil, "", "/v1/responses")
			sanitizer.retryPromptEnabled = true
			output, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read incompatible response structure: %v", err)
			}
			if sanitizer.RetryPrompted() || !strings.Contains(string(output), "event: response.failed\n") || strings.Contains(string(output), gatewayPublicRetryPromptMessage) {
				t.Fatalf("incompatible structure was converted: prompted=%t output=%q", sanitizer.RetryPrompted(), output)
			}
		})
	}
}

func TestGatewayResponsesSuppressedLifecycleErrorDoesNotAdvanceRetryPrompt(t *testing.T) {
	created := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\"}}\n\n"
	for _, test := range []struct {
		name  string
		frame string
	}{
		{
			name:  "output item added",
			frame: "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_hidden\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]},\"error\":{\"type\":\"overloaded_error\",\"message\":\"temporarily unavailable\"}}\n\n",
		},
		{
			name:  "content part added",
			frame: "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_hidden\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"},\"error\":{\"type\":\"overloaded_error\",\"message\":\"temporarily unavailable\"}}\n\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(created+test.frame)), nil, "", "/v1/responses")
			sanitizer.retryPromptEnabled = true
			output, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read suppressed lifecycle error: %v", err)
			}
			text := string(output)
			if sanitizer.RetryPrompted() || !strings.Contains(text, "event: response.failed\n") || strings.Contains(text, gatewayPublicRetryPromptMessage) || strings.Contains(text, "msg_hidden") {
				t.Fatalf("suppressed lifecycle error advanced retry state: prompted=%t output=%q", sanitizer.RetryPrompted(), text)
			}
		})
	}
}

func TestWriteGatewayRetryPromptMatchesResponsesMode(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name      string
		streaming bool
		mediaType string
	}{
		{name: "streaming", streaming: true, mediaType: "text/event-stream"},
		{name: "buffered", mediaType: "application/json"},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			writeGatewayRetryPrompt(ctx, test.streaming, "resp_retry_test")
			if recorder.Code != http.StatusOK || !strings.HasPrefix(recorder.Header().Get("Content-Type"), test.mediaType) || !strings.Contains(recorder.Body.String(), gatewayPublicRetryPromptMessage) || !strings.Contains(recorder.Body.String(), `"status":"completed"`) || strings.Contains(recorder.Body.String(), "response.failed") {
				t.Fatalf("retry prompt response=%d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
			}
			if test.streaming && (!strings.Contains(recorder.Body.String(), "event: response.output_text.delta\n") || !strings.Contains(recorder.Body.String(), "event: response.completed\n")) {
				t.Fatalf("streaming retry prompt omitted events: %q", recorder.Body.String())
			}
			if test.streaming {
				if got := gatewayTestResponseEventTypes(gatewayStreamJSONPayloads(recorder.Body.Bytes())); got != "response.created,response.output_item.added,response.content_part.added,response.output_text.delta,response.output_text.done,response.content_part.done,response.output_item.done,response.completed" {
					t.Fatalf("streaming retry prompt lifecycle=%q body=%q", got, recorder.Body.String())
				}
			}
		})
	}
}

func TestGatewayRetryPromptAfterExhaustionRequiresTransientEvidence(t *testing.T) {
	transient := newGatewayRouteAttempt(gatewayservice.Candidate{}, "upstream_status", http.StatusServiceUnavailable)
	permanent := newGatewayRouteAttempt(gatewayservice.Candidate{}, "upstream_status", http.StatusForbidden)
	unsupported := newGatewayRouteAttempt(gatewayservice.Candidate{}, "model_unsupported", http.StatusNotFound)
	challenge := newGatewayRouteAttempt(gatewayservice.Candidate{}, "upstream_status", http.StatusForbidden)
	challenge.Retryable = isTransientGatewayResponse(http.StatusForbidden, []byte("cf-mitigated: challenge"))
	permanentTransport := newGatewayRouteAttempt(gatewayservice.Candidate{}, "transport", 0)
	permanentTransport.Retryable = false
	for _, test := range []struct {
		name     string
		smart    bool
		app      string
		path     string
		image    bool
		status   int
		attempts []gatewayRouteAttempt
		want     bool
	}{
		{name: "transient Codex Responses", smart: true, app: "codex", path: "/v1/responses", status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{transient}, want: true},
		{name: "transient challenge", smart: true, app: "codex", path: "/v1/responses", status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{challenge}, want: true},
		{name: "manual route", app: "codex", path: "/v1/responses", status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{transient}},
		{name: "permanent status", smart: true, app: "codex", path: "/v1/responses", status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{permanent}},
		{name: "permanent transport setup", smart: true, app: "codex", path: "/v1/responses", status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{permanentTransport}},
		{name: "unsupported model", smart: true, app: "codex", path: "/v1/responses", status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{unsupported}},
		{name: "image request", smart: true, app: "codex", path: "/v1/responses", image: true, status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{transient}},
		{name: "other app", smart: true, app: "openai", path: "/v1/responses", status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{transient}},
		{name: "other path", smart: true, app: "codex", path: "/v1/chat/completions", status: http.StatusBadGateway, attempts: []gatewayRouteAttempt{transient}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := gatewayRetryPromptAfterExhaustionEligible(test.smart, storage.GatewayApp(test.app), test.path, test.image, test.status, test.attempts); got != test.want {
				t.Fatalf("retry prompt eligibility=%t want=%t attempts=%+v", got, test.want, test.attempts)
			}
		})
	}
}

func assertGatewayPublicTerminal(t *testing.T, output, path string, wantSequence int64) {
	t.Helper()
	if strings.Count(output, gatewayPublicStreamFailureMessage) != 1 {
		t.Fatalf("generic terminal count in %s output = %d, output=%q", path, strings.Count(output, gatewayPublicStreamFailureMessage), output)
	}
	if strings.Contains(output, "sk-private") || strings.Contains(output, "secret provider") {
		t.Fatalf("public terminal leaked private detail: %q", output)
	}
	if strings.Contains(path, "/responses") {
		if !strings.Contains(output, "event: response.failed\n") {
			t.Fatalf("responses terminal did not use response.failed: %q", output)
		}
		payloads := gatewayStreamJSONPayloads([]byte(output))
		var terminal map[string]any
		for _, payload := range payloads {
			response, _ := payload["response"].(map[string]any)
			errorValue, _ := response["error"].(map[string]any)
			if payload["type"] == "response.failed" && errorValue["message"] == gatewayPublicStreamFailureMessage {
				assertGatewayResponsesFailureSchema(t, response)
				terminal = payload
			}
		}
		if terminal == nil {
			t.Fatalf("responses terminal payload = %#v, want response.failed", payloads)
		}
		sequence, ok := terminal["sequence_number"].(float64)
		if !ok || int64(sequence) != wantSequence {
			t.Fatalf("responses terminal sequence = %#v, want %d", terminal["sequence_number"], wantSequence)
		}
		return
	}
	if strings.Contains(path, "/messages") {
		if !strings.Contains(output, "event: error\n") || !strings.Contains(output, `"type":"error"`) {
			t.Fatalf("claude terminal did not use error event: %q", output)
		}
		return
	}
	if strings.Contains(output, "event: error\n") {
		t.Fatalf("chat terminal unexpectedly added an event name: %q", output)
	}
	if !strings.Contains(output, `data: {"error":{"message":"request failed before completion"`) {
		t.Fatalf("chat terminal did not use generic error envelope: %q", output)
	}
}

func assertGatewayResponsesFailureSchema(t *testing.T, response map[string]any) {
	t.Helper()
	for _, field := range []string{
		"id", "object", "created_at", "status", "completed_at", "error", "incomplete_details",
		"instructions", "max_output_tokens", "model", "output", "parallel_tool_calls",
		"previous_response_id", "reasoning", "store", "temperature", "text", "tool_choice",
		"tools", "top_p", "truncation", "usage", "user", "metadata",
	} {
		if _, present := response[field]; !present {
			t.Fatalf("responses failure omitted required response field %q: %#v", field, response)
		}
	}
	if response["object"] != "response" || response["status"] != "failed" {
		t.Fatalf("responses failure object/status = %#v/%#v", response["object"], response["status"])
	}
}

func TestGatewaySSESanitizerRequiresProtocolCompletionAfterAcceptedOutput(t *testing.T) {
	for _, tc := range gatewaySSEProtocolCases() {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer, output, err := readGatewaySSEProtocolStream(t, tc.partial, tc.path)
			if err != nil {
				t.Fatalf("read clean EOF stream: %v", err)
			}
			if !sanitizer.accepted || !sanitizer.useful {
				t.Fatalf("partial stream acceptance state: accepted=%t useful=%t", sanitizer.accepted, sanitizer.useful)
			}
			assertGatewayPublicTerminal(t, output, tc.path, tc.wantSequence)
			if sanitizer.ErrorText() == "" || !strings.Contains(sanitizer.ErrorText(), "unexpected EOF") {
				t.Fatalf("clean EOF error detail = %q", sanitizer.ErrorText())
			}
		})
	}
}

func TestGatewaySSESanitizerPreservesProtocolCompletion(t *testing.T) {
	for _, tc := range gatewaySSEProtocolCases() {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer, output, err := readGatewaySSEProtocolStream(t, tc.partial+tc.terminal, tc.path)
			if err != nil {
				t.Fatalf("read completed stream: %v", err)
			}
			if sanitizer.ErrorText() != "" || !sanitizer.protocolTerminal {
				t.Fatalf("completed stream state: terminal=%t error=%q", sanitizer.protocolTerminal, sanitizer.ErrorText())
			}
			if strings.Contains(output, gatewayPublicStreamFailureMessage) {
				t.Fatalf("completed stream received synthetic error: %q", output)
			}
			if !strings.Contains(output, tc.terminal) {
				t.Fatalf("completed stream lost protocol terminal: %q", output)
			}
		})
	}
}

type gatewaySSEProtocolErrorReader struct {
	reader *strings.Reader
	err    error
	lost   bool
}

func (r *gatewaySSEProtocolErrorReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		return n, nil
	}
	r.lost = true
	if err == io.EOF {
		return 0, r.err
	}
	return 0, err
}

func (r *gatewaySSEProtocolErrorReader) Close() error { return nil }

func TestGatewaySSESanitizerSourceErrorsUseOneProtocolTerminal(t *testing.T) {
	for _, tc := range gatewaySSEProtocolCases() {
		t.Run(tc.name, func(t *testing.T) {
			sourceErr := errors.New("secret provider read failure")
			source := &gatewaySSEProtocolErrorReader{reader: strings.NewReader(tc.partial), err: sourceErr}
			sanitizer := newGatewaySSEStreamSanitizer(source, &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
			output, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read source error stream: %v", err)
			}
			assertGatewayPublicTerminal(t, string(output), tc.path, tc.wantSequence)
			if !strings.Contains(sanitizer.ErrorText(), "secret provider read failure") {
				t.Fatalf("source error detail = %q", sanitizer.ErrorText())
			}
		})
	}
}

func TestGatewaySSESanitizerOwnerLossSourceErrorsUseOneProtocolTerminal(t *testing.T) {
	for _, tc := range gatewaySSEProtocolCases() {
		t.Run(tc.name, func(t *testing.T) {
			source := &gatewaySSEProtocolErrorReader{
				reader: strings.NewReader(tc.partial),
				err:    errors.New("owner-fenced provider read failure"),
			}
			sanitizer := newGatewaySSEStreamSanitizer(source, &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
			sanitizer.ownerLeaseLost = func() bool { return source.lost }
			output, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read owner-loss source error stream: %v", err)
			}
			assertGatewayPublicTerminal(t, string(output), tc.path, tc.wantSequence)
			if !sanitizer.OwnerLeaseLost() || !strings.Contains(sanitizer.ErrorText(), "owner lease loss") {
				t.Fatalf("owner-loss source error state: owner=%t error=%q", sanitizer.OwnerLeaseLost(), sanitizer.ErrorText())
			}
		})
	}
}

func TestGatewaySSESanitizerOwnerFenceAcceptanceUsesProtocolTerminal(t *testing.T) {
	for _, tc := range gatewaySSEProtocolCases() {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.partial)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
			sanitizer.onAcceptedEvidence = func(gatewayStreamAcceptanceEvidence) error {
				return gatewayservice.ErrRequestDispatchOwnerLeaseLost
			}
			output, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read owner-fenced stream: %v", err)
			}
			wantSequence := tc.wantSequence
			if strings.Contains(tc.path, "/responses") {
				// The first accepted frame is suppressed when its durable mark is
				// fenced, so the replacement error occupies that frame's sequence.
				wantSequence = 7
			}
			assertGatewayPublicTerminal(t, string(output), tc.path, wantSequence)
			if !sanitizer.OwnerLeaseLost() || !strings.Contains(sanitizer.ErrorText(), "owner lease loss") {
				t.Fatalf("owner-fenced state: owner=%t error=%q", sanitizer.OwnerLeaseLost(), sanitizer.ErrorText())
			}
			if strings.Contains(string(output), "partial") {
				t.Fatalf("accepted frame leaked after owner fence: %q", output)
			}
		})
	}
}

func TestGatewaySSESanitizerOwnerFenceBeforePendingOutputSuppressesBufferedFrames(t *testing.T) {
	for _, tc := range gatewaySSEProtocolCases() {
		t.Run(tc.name, func(t *testing.T) {
			fenced := false
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.partial+tc.terminal)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
			sanitizer.ownerLeaseLost = func() bool { return fenced }
			if err := sanitizer.readFrame(); err != nil {
				t.Fatalf("stage pending upstream frame: %v", err)
			}
			if len(sanitizer.pending) == 0 {
				t.Fatal("upstream frame was not staged before the owner fence")
			}
			fenced = true
			output, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read owner-fenced pending stream: %v", err)
			}
			if !sanitizer.OwnerLeaseLost() || !strings.Contains(sanitizer.ErrorText(), "owner lease loss") {
				t.Fatalf("owner-fenced pending state: owner=%t error=%q", sanitizer.OwnerLeaseLost(), sanitizer.ErrorText())
			}
			if strings.Contains(string(output), "partial") || strings.Contains(string(output), tc.terminal) {
				t.Fatalf("owner-fenced pending output leaked upstream frame: %q", output)
			}
			if strings.Contains(tc.path, "/responses") && strings.Contains(string(output), "resp_7") {
				t.Fatalf("owner-fenced pending output leaked unpublished response id: %q", output)
			}
			assertGatewayPublicTerminal(t, string(output), tc.path, 8)
		})
	}
}

func TestGatewaySSESanitizerOwnerFenceCompletesPartiallyPublishedFrame(t *testing.T) {
	for _, tc := range []struct {
		name         string
		path         string
		body         string
		wantSequence int64
	}{
		{
			name:         "responses",
			path:         "/v1/responses",
			body:         "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_partial\",\"object\":\"response\",\"status\":\"in_progress\"}}\n\n",
			wantSequence: 8,
		},
		{
			name: "chat completions",
			path: "/v1/chat/completions",
			body: "data: {\"id\":\"chat_partial\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n",
		},
		{
			name: "claude messages",
			path: "/v1/messages",
			body: "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_partial\",\"role\":\"assistant\"}}\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cuts := []int{
				max(1, strings.Index(tc.body, "\n")/2),
				strings.Index(tc.body, "data:") + len("data: ") + 12,
				len(tc.body) - 1,
			}
			for index, cut := range cuts {
				t.Run(strconv.Itoa(index), func(t *testing.T) {
					fenced := false
					sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.body)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
					sanitizer.ownerLeaseLost = func() bool { return fenced }
					first := make([]byte, cut)
					if n, err := sanitizer.Read(first); err != nil || n != cut {
						t.Fatalf("partial read = %d, %v, want %d", n, err, cut)
					}
					if strings.Contains(tc.path, "/responses") && (sanitizer.responseID != "" || sanitizer.responseSnapshot != nil) {
						t.Fatalf("partial read prematurely promoted response state: id=%q snapshot=%#v", sanitizer.responseID, sanitizer.responseSnapshot)
					}
					fenced = true
					rest, err := io.ReadAll(sanitizer)
					if err != nil {
						t.Fatalf("read owner-fenced remainder: %v", err)
					}
					output := string(first) + string(rest)
					if !strings.HasPrefix(output, tc.body) {
						t.Fatalf("partially published frame was not completed: %q", output)
					}
					assertGatewayPublicTerminal(t, output, tc.path, tc.wantSequence)
				})
			}
		})
	}
}

func TestGatewaySSESanitizerOwnerFenceCompletesPartiallyPublishedMetadataWithTerminal(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		body string
	}{
		{name: "responses event-only ping", path: "/v1/responses", body: "event: ping\n\n"},
		{name: "chat comment", path: "/v1/chat/completions", body: ": keepalive\n\n"},
		{name: "claude event-only ping", path: "/v1/messages", body: "event: ping\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fenced := false
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.body)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
			sanitizer.ownerLeaseLost = func() bool { return fenced }
			cut := max(1, len(tc.body)/2)
			first := make([]byte, cut)
			if n, err := sanitizer.Read(first); err != nil || n != cut {
				t.Fatalf("partial metadata read = %d, %v, want %d", n, err, cut)
			}
			if sanitizer.accepted || sanitizer.useful {
				t.Fatalf("metadata unexpectedly changed acceptance state: accepted=%t useful=%t", sanitizer.accepted, sanitizer.useful)
			}
			fenced = true
			rest, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read owner-fenced metadata remainder: %v", err)
			}
			output := string(first) + string(rest)
			if !strings.HasPrefix(output, tc.body) {
				t.Fatalf("partially published metadata was not completed: %q", output)
			}
			if !sanitizer.OwnerLeaseLost() || !strings.Contains(sanitizer.ErrorText(), "owner lease loss") {
				t.Fatalf("metadata owner-fence state: owner=%t error=%q", sanitizer.OwnerLeaseLost(), sanitizer.ErrorText())
			}
			assertGatewayPublicTerminal(t, output, tc.path, 0)
		})
	}
}

func TestGatewaySSESanitizerOwnerFenceTerminatesAfterPublishedMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		path   string
		first  string
		second string
	}{
		{name: "responses", path: "/v1/responses", first: "event: ping\n\n", second: "event: heartbeat\n\n"},
		{name: "chat completions", path: "/v1/chat/completions", first: ": first-keepalive\n\n", second: ": second-keepalive\n\n"},
		{name: "claude messages", path: "/v1/messages", first: "event: ping\n\n", second: "event: heartbeat\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, stageSecond := range []bool{false, true} {
				name := "after complete metadata"
				if stageSecond {
					name = "suppresses next unpublished metadata"
				}
				t.Run(name, func(t *testing.T) {
					fenced := false
					body := tc.first
					if stageSecond {
						body += tc.second
					}
					sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(body)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
					sanitizer.ownerLeaseLost = func() bool { return fenced }
					first := make([]byte, len(tc.first))
					if n, err := sanitizer.Read(first); err != nil || n != len(tc.first) {
						t.Fatalf("complete metadata read = %d, %v, want %d", n, err, len(tc.first))
					}
					if sanitizer.accepted || sanitizer.useful || !sanitizer.downstreamOutputSeen {
						t.Fatalf("metadata output state: accepted=%t useful=%t output=%t", sanitizer.accepted, sanitizer.useful, sanitizer.downstreamOutputSeen)
					}
					if stageSecond {
						if err := sanitizer.readFrame(); err != nil {
							t.Fatalf("stage second metadata frame: %v", err)
						}
						if len(sanitizer.pending) == 0 || sanitizer.pendingOutputStarted {
							t.Fatalf("second metadata was not staged as unpublished: pending=%d started=%t", len(sanitizer.pending), sanitizer.pendingOutputStarted)
						}
					}
					fenced = true
					rest, err := io.ReadAll(sanitizer)
					if err != nil {
						t.Fatalf("read owner-fenced metadata stream: %v", err)
					}
					output := string(first) + string(rest)
					if !strings.HasPrefix(output, tc.first) || strings.Contains(output, tc.second) {
						t.Fatalf("metadata owner-fence output = %q", output)
					}
					assertGatewayPublicTerminal(t, output, tc.path, 0)
				})
			}
		})
	}
}

func TestGatewaySSESanitizerOwnerFenceKeepsSyntheticTerminalAfterPublishedMetadata(t *testing.T) {
	metadata := "event: ping\n\n"
	malformed := "event: response.output_text.delta\ndata:\n\n"
	fenced := false
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(metadata+malformed)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", "/v1/responses")
	sanitizer.ownerLeaseLost = func() bool { return fenced }

	first := make([]byte, len(metadata))
	if n, err := sanitizer.Read(first); err != nil || n != len(metadata) {
		t.Fatalf("complete metadata read = %d, %v, want %d", n, err, len(metadata))
	}
	if err := sanitizer.readFrame(); err != nil {
		t.Fatalf("stage malformed frame terminal: %v", err)
	}
	if !sanitizer.syntheticTerminal || len(sanitizer.pending) == 0 || sanitizer.pendingOutputStarted {
		t.Fatalf("synthetic terminal was not staged as unpublished: synthetic=%t pending=%d started=%t", sanitizer.syntheticTerminal, len(sanitizer.pending), sanitizer.pendingOutputStarted)
	}

	fenced = true
	rest, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read owner-fenced synthetic terminal: %v", err)
	}
	output := string(first) + string(rest)
	if !strings.HasPrefix(output, metadata) || strings.Contains(output, malformed) {
		t.Fatalf("published metadata synthetic output = %q", output)
	}
	assertGatewayPublicTerminal(t, output, "/v1/responses", 0)
}

func TestGatewaySSESanitizerOwnerFenceCompletesPartiallyPublishedProtocolTerminal(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		body string
	}{
		{
			name: "responses",
			path: "/v1/responses",
			body: "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":9,\"response\":{\"id\":\"resp_complete\",\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{name: "chat completions", path: "/v1/chat/completions", body: "data: [DONE]\n\n"},
		{name: "legacy completions", path: "/v1/completions", body: "data: [DONE]\n\n"},
		{name: "claude messages", path: "/v1/messages", body: "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cuts := []int{
				max(1, strings.Index(tc.body, "\n")/2),
				strings.Index(tc.body, "data:") + len("data: ") + 2,
				len(tc.body) - 1,
			}
			for index, cut := range cuts {
				t.Run(strconv.Itoa(index), func(t *testing.T) {
					fenced := false
					sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.body)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
					sanitizer.ownerLeaseLost = func() bool { return fenced }
					first := make([]byte, cut)
					if n, err := sanitizer.Read(first); err != nil || n != cut {
						t.Fatalf("partial terminal read = %d, %v, want %d", n, err, cut)
					}
					if !sanitizer.protocolTerminal || !sanitizer.pendingOutputStarted {
						t.Fatalf("partial terminal state: terminal=%t started=%t", sanitizer.protocolTerminal, sanitizer.pendingOutputStarted)
					}
					fenced = true
					rest, err := io.ReadAll(sanitizer)
					if err != nil {
						t.Fatalf("read owner-fenced terminal remainder: %v", err)
					}
					output := string(first) + string(rest)
					if output != tc.body || strings.Contains(output, gatewayPublicStreamFailureMessage) {
						t.Fatalf("partially published terminal changed after owner fence: %q", output)
					}
					if !sanitizer.OwnerLeaseLost() || sanitizer.syntheticTerminal {
						t.Fatalf("terminal owner-fence state: owner=%t synthetic=%t", sanitizer.OwnerLeaseLost(), sanitizer.syntheticTerminal)
					}
				})
			}
		})
	}
}

func TestGatewaySSESanitizerOwnerFenceAfterSyntheticPendingKeepsOneTerminal(t *testing.T) {
	for _, tc := range []struct {
		name         string
		path         string
		body         string
		leaked       string
		wantSequence int64
	}{
		{
			name:         "responses",
			path:         "/v1/responses",
			body:         "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"delta\":\"unsent responses partial\"}",
			leaked:       "unsent responses partial",
			wantSequence: 5,
		},
		{
			name:   "chat completions",
			path:   "/v1/chat/completions",
			body:   "data: {\"id\":\"chat_1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"unsent chat partial\"}}]}",
			leaked: "unsent chat partial",
		},
		{
			name:   "claude messages",
			path:   "/v1/messages",
			body:   "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"unsent claude partial\"}}",
			leaked: "unsent claude partial",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fenced := false
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.body)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "sk-private", tc.path)
			sanitizer.ownerLeaseLost = func() bool { return fenced }
			if err := sanitizer.readFrame(); !errors.Is(err, io.EOF) {
				t.Fatalf("stage synthetic terminal: %v", err)
			}
			if !sanitizer.syntheticTerminal || len(sanitizer.pending) == 0 {
				t.Fatalf("synthetic terminal was not staged: %+v", sanitizer)
			}
			fenced = true
			output, err := io.ReadAll(sanitizer)
			if err != nil {
				t.Fatalf("read owner-fenced synthetic terminal: %v", err)
			}
			if !sanitizer.OwnerLeaseLost() {
				t.Fatal("owner loss was not recorded after a synthetic terminal was staged")
			}
			if strings.Contains(string(output), tc.leaked) {
				t.Fatalf("owner-fenced synthetic pending leaked upstream output: %q", output)
			}
			assertGatewayPublicTerminal(t, string(output), tc.path, tc.wantSequence)
		})
	}
}

func TestGatewayResponsesOwnerFenceReplacesUnpublishedPartialTextRetry(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\"resp_fenced_retry\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_fenced_retry\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_fenced_retry\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_fenced_retry\",\"output_index\":0,\"content_index\":0,\"delta\":\"unpublished partial\"}\n\n"
	fenced := false
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(body)), nil, "", "/v1/responses")
	sanitizer.retryPromptEnabled = true
	sanitizer.ownerLeaseLost = func() bool { return fenced }
	if err := sanitizer.readFrame(); err != nil {
		t.Fatalf("stage created frame: %v", err)
	}
	sanitizer.pending = nil
	sanitizer.pendingResponseID = ""
	sanitizer.pendingResponse = nil
	sanitizer.pendingResponseBytes = 0
	sanitizer.pendingSequenceNext = 0
	sanitizer.pendingSequenceBytes = 0
	if err := sanitizer.readFrame(); err != nil {
		t.Fatalf("stage item frame: %v", err)
	}
	sanitizer.pending = nil
	sanitizer.pendingSequenceNext = 0
	sanitizer.pendingSequenceBytes = 0
	if err := sanitizer.readFrame(); err != nil {
		t.Fatalf("stage part frame: %v", err)
	}
	sanitizer.pending = nil
	sanitizer.pendingSequenceNext = 0
	sanitizer.pendingSequenceBytes = 0
	if err := sanitizer.readFrame(); err != nil {
		t.Fatalf("stage partial text frame: %v", err)
	}
	if err := sanitizer.readFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("stage partial retry terminal: %v", err)
	}
	if !sanitizer.RetryPrompted() || !sanitizer.syntheticTerminal || len(sanitizer.pending) == 0 {
		t.Fatalf("partial retry was not staged: prompted=%t synthetic=%t pending=%d", sanitizer.RetryPrompted(), sanitizer.syntheticTerminal, len(sanitizer.pending))
	}
	fenced = true
	output, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read owner-fenced partial retry: %v", err)
	}
	if sanitizer.RetryPrompted() || strings.Contains(string(output), "unpublished partial") || strings.Contains(string(output), gatewayPublicRetryPromptMessage) {
		t.Fatalf("owner fence preserved unpublished retry: prompted=%t output=%q", sanitizer.RetryPrompted(), output)
	}
	assertGatewayPublicTerminal(t, string(output), "/v1/responses", 5)
}

type gatewaySSEReadCounter struct {
	reads int
}

func (r *gatewaySSEReadCounter) Read([]byte) (int, error) {
	r.reads++
	return 0, errors.New("source must not be read after owner fence")
}

func (r *gatewaySSEReadCounter) Close() error { return nil }

func TestGatewaySSESanitizerOwnerFenceBeforeReadReturnsOwnerSentinel(t *testing.T) {
	source := &gatewaySSEReadCounter{}
	sanitizer := newGatewaySSEStreamSanitizer(source, nil, "sk-private", "/v1/responses")
	sanitizer.ownerLeaseLost = func() bool { return true }
	buffer := make([]byte, 128)
	if _, err := sanitizer.Read(buffer); !errors.Is(err, gatewayservice.ErrRequestDispatchOwnerLeaseLost) {
		t.Fatalf("pre-read owner fence error=%v, want owner sentinel", err)
	}
	if source.reads != 0 || !sanitizer.OwnerLeaseLost() {
		t.Fatalf("pre-read fence state: reads=%d owner=%t", source.reads, sanitizer.OwnerLeaseLost())
	}
}

func TestGatewaySSESanitizerLateOwnerFenceDoesNotDuplicateDeliveredTerminal(t *testing.T) {
	for _, tc := range gatewaySSEProtocolCases() {
		t.Run(tc.name, func(t *testing.T) {
			fenced := false
			sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(tc.terminal)), nil, "sk-private", tc.path)
			sanitizer.ownerLeaseLost = func() bool { return fenced }
			buffer := make([]byte, len(tc.terminal)+128)
			n, err := sanitizer.Read(buffer)
			if err != nil || string(buffer[:n]) != tc.terminal {
				t.Fatalf("delivered terminal=%q err=%v, want %q", buffer[:n], err, tc.terminal)
			}
			fenced = true
			n, err = sanitizer.Read(buffer)
			if n != 0 || !errors.Is(err, io.EOF) {
				t.Fatalf("late owner fence read=(%d,%v), want EOF", n, err)
			}
			if !sanitizer.OwnerLeaseLost() || strings.Contains(string(buffer[:n]), gatewayPublicStreamFailureMessage) {
				t.Fatalf("late owner fence state: owner=%t output=%q", sanitizer.OwnerLeaseLost(), buffer[:n])
			}
		})
	}
}

func TestGatewaySSESanitizerMetadataOnlyCleanEOFDoesNotCreateFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		body string
	}{
		{name: "responses ping", path: "/v1/responses", body: "event: ping\ndata: {\"type\":\"ping\"}\n\n"},
		{name: "chat comment", path: "/v1/chat/completions", body: ": keepalive\n\n"},
		{name: "claude ping", path: "/v1/messages", body: "event: ping\ndata: {\"type\":\"ping\"}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer, output, err := readGatewaySSEProtocolStream(t, tc.body, tc.path)
			if err != nil || output != tc.body {
				t.Fatalf("metadata-only stream output=%q err=%v", output, err)
			}
			if sanitizer.accepted || sanitizer.useful || sanitizer.ErrorText() != "" || strings.Contains(output, gatewayPublicStreamFailureMessage) {
				t.Fatalf("metadata-only stream changed state: accepted=%t useful=%t error=%q output=%q", sanitizer.accepted, sanitizer.useful, sanitizer.ErrorText(), output)
			}
		})
	}
}

func TestGatewayPublicResponsesErrorFrameIsStrictlyStructured(t *testing.T) {
	frame := gatewayPublicStreamErrorFrame([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":41,\"response_id\":\"resp_41\"}\n\n"), "/v1/responses")
	payloads := gatewayStreamJSONPayloads(frame)
	if !strings.Contains(string(frame), "event: response.failed\n") || len(payloads) != 1 || payloads[0]["type"] != "response.failed" || payloads[0]["sequence_number"] != float64(41) {
		t.Fatalf("responses error frame = %q", frame)
	}
	response, _ := payloads[0]["response"].(map[string]any)
	if response["id"] != "resp_41" || response["status"] != "failed" {
		t.Fatalf("responses failure response = %#v", response)
	}
	assertGatewayResponsesFailureSchema(t, response)
}

func TestGatewayResponsesSyntheticFailureReusesPublishedResponseID(t *testing.T) {
	sanitizer, output, err := readGatewaySSEProtocolStream(t,
		"event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_published\",\"object\":\"response\",\"created_at\":123,\"status\":\"in_progress\",\"model\":\"gpt-test\",\"metadata\":{\"published\":true}}}\n\n"+
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":8,\"delta\":\"partial\"}\n\n",
		"/v1/responses",
	)
	if err != nil {
		t.Fatalf("read incomplete responses stream: %v", err)
	}
	payloads := gatewayStreamJSONPayloads([]byte(output))
	terminal := payloads[len(payloads)-1]
	response, _ := terminal["response"].(map[string]any)
	if terminal["type"] != "response.failed" || response["id"] != "resp_published" || sanitizer.responseID != "resp_published" {
		t.Fatalf("synthetic responses terminal = %#v, response id state = %q", terminal, sanitizer.responseID)
	}
	metadata, _ := response["metadata"].(map[string]any)
	if response["created_at"] != float64(123) || response["model"] != "gpt-test" || metadata["published"] != true {
		t.Fatalf("synthetic terminal did not retain published response snapshot: %#v", response)
	}
	assertGatewayResponsesFailureSchema(t, response)
}

func TestGatewayResponsesSyntheticFailureReusesPendingResponseIDAtEOF(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_pending\"}}\n"
	sanitizer, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
	if err != nil {
		t.Fatalf("read incomplete responses stream: %v", err)
	}
	payloads := gatewayStreamJSONPayloads([]byte(output))
	terminal := payloads[len(payloads)-1]
	response, _ := terminal["response"].(map[string]any)
	if terminal["type"] != "response.failed" || response["id"] != "resp_pending" {
		t.Fatalf("synthetic terminal after pending response = %#v", terminal)
	}
	if !strings.Contains(output, "\n\nevent: response.failed\n") {
		t.Fatalf("pending response and synthetic terminal were not separated: %q", output)
	}
	if sanitizer.pendingResponseID != "" || sanitizer.responseID != "resp_pending" {
		t.Fatalf("response id state after delivery = published %q pending %q", sanitizer.responseID, sanitizer.pendingResponseID)
	}
}

func TestGatewayResponsesPendingSnapshotCommitsBeforeSyntheticTerminal(t *testing.T) {
	fenced := false
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_boundary\",\"object\":\"response\",\"created_at\":123,\"status\":\"in_progress\",\"model\":\"gpt-test\"}}\n"
	sanitizer := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(body)), &gatewayTailBuffer{limit: gatewayStreamUsageCaptureLimit}, "", "/v1/responses")
	sanitizer.ownerLeaseLost = func() bool { return fenced }
	if err := sanitizer.readFrame(); !errors.Is(err, io.EOF) {
		t.Fatalf("stage EOF response and terminal: %v", err)
	}
	boundary := sanitizer.pendingResponseBytes
	if boundary <= 0 || boundary >= len(sanitizer.pending) {
		t.Fatalf("pending response boundary = %d, pending bytes = %d", boundary, len(sanitizer.pending))
	}
	first := make([]byte, boundary)
	if n, err := sanitizer.Read(first); err != nil || n != boundary {
		t.Fatalf("read through response boundary = %d, %v", n, err)
	}
	if sanitizer.responseID != "resp_boundary" || sanitizer.responseSnapshot["model"] != "gpt-test" {
		t.Fatalf("response boundary did not commit snapshot: id=%q snapshot=%#v", sanitizer.responseID, sanitizer.responseSnapshot)
	}
	fenced = true
	rest, err := io.ReadAll(sanitizer)
	if err != nil {
		t.Fatalf("read owner-fenced terminal: %v", err)
	}
	payloads := gatewayStreamJSONPayloads(rest)
	terminal := payloads[len(payloads)-1]
	response, _ := terminal["response"].(map[string]any)
	if terminal["type"] != "response.failed" || response["id"] != "resp_boundary" || response["model"] != "gpt-test" {
		t.Fatalf("owner-fenced terminal after response boundary = %#v", terminal)
	}
}

func TestGatewaySSESanitizerSuppressesTruncatedResponsesJSONAtEOF(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_truncated\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":8,\"delta\":\"secret-fragment\""
	sanitizer, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
	if err != nil {
		t.Fatalf("read truncated responses stream: %v", err)
	}
	if strings.Contains(output, "secret-fragment") || strings.Count(output, "event: response.failed\n") != 1 {
		t.Fatalf("truncated responses fragment was not replaced by one terminal: %q", output)
	}
	if !strings.Contains(sanitizer.ErrorText(), "truncated JSON frame") {
		t.Fatalf("truncated responses detail = %q", sanitizer.ErrorText())
	}
	payloads := gatewayStreamJSONPayloads([]byte(output))
	terminal := payloads[len(payloads)-1]
	response, _ := terminal["response"].(map[string]any)
	if terminal["type"] != "response.failed" || response["id"] != "resp_truncated" {
		t.Fatalf("truncated responses terminal = %#v", terminal)
	}
}

func TestGatewaySSESanitizerSuppressesEmptyResponsesDataAtEOF(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_empty_data\"}}\n\n" +
		"event: response.output_text.delta\ndata:\n"
	sanitizer, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
	if err != nil {
		t.Fatalf("read empty-data responses stream: %v", err)
	}
	if strings.Contains(output, "event: response.output_text.delta") || strings.Count(output, "event: response.failed\n") != 1 {
		t.Fatalf("empty Responses data frame was not replaced: %q", output)
	}
	if !strings.Contains(sanitizer.ErrorText(), "truncated JSON frame") {
		t.Fatalf("empty-data Responses detail = %q", sanitizer.ErrorText())
	}
}

func TestGatewaySSESanitizerSuppressesMalformedResponsesJSONAtFrameBoundary(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "malformed json", body: "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\"\n\n"},
		{name: "empty data", body: "event: response.output_text.delta\ndata:\n\n"},
		{name: "event without data", body: "event: response.output_text.delta\n\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_malformed_boundary\"}}\n\n" + test.body
			sanitizer, output, err := readGatewaySSEProtocolStream(t, stream, "/v1/responses")
			if err != nil {
				t.Fatalf("read malformed Responses stream: %v", err)
			}
			if strings.Contains(output, test.body) || strings.Count(output, "event: response.failed\n") != 1 {
				t.Fatalf("malformed Responses frame was not replaced: %q", output)
			}
			if !strings.Contains(sanitizer.ErrorText(), "malformed JSON frame") {
				t.Fatalf("malformed Responses detail = %q", sanitizer.ErrorText())
			}
			assertGatewayPublicTerminal(t, output, "/v1/responses", 8)
		})
	}
}

func TestGatewaySSESanitizerPassesResponsesEventOnlyKeepalives(t *testing.T) {
	for _, eventName := range []string{"ping", "heartbeat", "keep-alive"} {
		t.Run(eventName, func(t *testing.T) {
			keepalive := "event: " + eventName + "\n\n"
			body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_keepalive\"}}\n\n" +
				keepalive +
				"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":8,\"response\":{\"id\":\"resp_keepalive\",\"status\":\"completed\"}}\n\n"
			sanitizer, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
			if err != nil {
				t.Fatalf("read Responses keepalive stream: %v", err)
			}
			if sanitizer.ErrorText() != "" || !sanitizer.protocolTerminal {
				t.Fatalf("keepalive state: terminal=%t error=%q output=%q", sanitizer.protocolTerminal, sanitizer.ErrorText(), output)
			}
			if !strings.Contains(output, keepalive) || strings.Contains(output, "event: response.failed\n") {
				t.Fatalf("event-only keepalive was not passed through: %q", output)
			}
		})
	}
}

func TestGatewayResponsesSyntheticFailureDoesNotRegressSequence(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_sequence\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":8,\"delta\":\"partial\"}\n\n" +
		"event: response.failed\ndata: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_sequence\",\"status\":\"failed\",\"error\":{\"message\":\"private upstream failure\"}}}\n\n"
	_, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
	if err != nil {
		t.Fatalf("read regressed failure stream: %v", err)
	}
	if strings.Contains(output, "private upstream failure") || strings.Count(output, "event: response.failed\n") != 1 {
		t.Fatalf("regressed upstream failure was not replaced: %q", output)
	}
	assertGatewayPublicTerminal(t, output, "/v1/responses", 9)
}

func TestGatewaySSESanitizerAcceptsMultilineResponsesJSONAtEOF(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_multiline\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\ndata: \"response.output_text.delta\",\"sequence_number\":8,\"delta\":\"partial\"}\n"
	sanitizer, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
	if err != nil {
		t.Fatalf("read multiline responses stream: %v", err)
	}
	if !strings.Contains(output, "data: {\"type\":\ndata: \"response.output_text.delta\"") || strings.Count(output, "event: response.failed\n") != 1 {
		t.Fatalf("multiline Responses JSON was rejected: %q", output)
	}
	if strings.Contains(sanitizer.ErrorText(), "truncated JSON frame") {
		t.Fatalf("multiline Responses JSON marked malformed: %q", sanitizer.ErrorText())
	}
}

func TestGatewaySSESanitizerNormalizesIncompleteResponsesCompletion(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_normalized\"}}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_normalized\"}}\n\n"
	sanitizer, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
	if err != nil {
		t.Fatalf("read responses stream: %v", err)
	}
	if sanitizer.ErrorText() != "" || !sanitizer.protocolTerminal || strings.Contains(output, gatewayPublicStreamFailureMessage) {
		t.Fatalf("normalized completion state: terminal=%t error=%q output=%q", sanitizer.protocolTerminal, sanitizer.ErrorText(), output)
	}
	payloads := gatewayStreamJSONPayloads([]byte(output))
	terminal := payloads[len(payloads)-1]
	response, _ := terminal["response"].(map[string]any)
	if terminal["type"] != "response.completed" || terminal["sequence_number"] != float64(8) || response["status"] != "completed" {
		t.Fatalf("normalized completion = %#v", terminal)
	}
}

func TestGatewaySSESanitizerNormalizesRegressedResponsesCompletionSequence(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_regressed\"}}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":6,\"response\":{\"id\":\"resp_regressed\",\"status\":\"completed\"}}\n\n"
	sanitizer, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
	if err != nil {
		t.Fatalf("read responses stream: %v", err)
	}
	payloads := gatewayStreamJSONPayloads([]byte(output))
	terminal := payloads[len(payloads)-1]
	if sanitizer.ErrorText() != "" || !sanitizer.protocolTerminal || terminal["type"] != "response.completed" || terminal["sequence_number"] != float64(8) {
		t.Fatalf("normalized regressed completion: terminal=%#v state=%+v output=%q", terminal, sanitizer, output)
	}
}

func TestGatewayResponsesCompletedIsTrustedOnlyWithSuccessfulResponse(t *testing.T) {
	for _, tc := range []struct {
		name     string
		frame    string
		accepted bool
	}{
		{
			name:     "completed with null error",
			frame:    "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"error\":null}}\n\n",
			accepted: true,
		},
		{
			name:     "completed without error field",
			frame:    "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n",
			accepted: true,
		},
		{
			name:  "missing response id",
			frame: "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{
			name:     "missing sequence",
			frame:    "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"error\":null}}\n\n",
			accepted: true,
		},
		{
			name:     "missing status",
			frame:    "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"resp_1\",\"error\":null}}\n\n",
			accepted: true,
		},
		{
			name:  "missing response object",
			frame: "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n",
		},
		{
			name:  "payload type mismatch",
			frame: "event: response.completed\ndata: {\"type\":\"response.in_progress\",\"sequence_number\":5,\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{
			name:  "event type mismatch",
			frame: "event: response.in_progress\ndata: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{
			name:  "empty response id",
			frame: "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"  \",\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{
			name:  "fractional sequence",
			frame: "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":5.5,\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{
			name:  "string sequence",
			frame: "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":\"5\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"error\":null}}\n\n",
		},
		{
			name:  "failed status",
			frame: "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"resp_1\",\"status\":\"failed\",\"error\":null}}\n\n",
		},
		{
			name:  "nonempty error",
			frame: "event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"error\":{\"message\":\"failed\"}}}\n\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence, accepted := gatewayStreamFrameAcceptanceEvidence([]byte(tc.frame), "/v1/responses")
			if accepted != tc.accepted {
				t.Fatalf("accepted=%t evidence=%+v, want accepted=%t", accepted, evidence, tc.accepted)
			}
			if tc.accepted && evidence.EventType != "response.completed" {
				t.Fatalf("evidence=%+v, want response.completed", evidence)
			}
		})
	}
}

func TestGatewaySSESanitizerRejectsMalformedResponsesCompletion(t *testing.T) {
	body := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"
	sanitizer, output, err := readGatewaySSEProtocolStream(t, body, "/v1/responses")
	if err != nil {
		t.Fatalf("read malformed completion stream: %v", err)
	}
	if !strings.Contains(output, "partial") || strings.Count(output, gatewayPublicStreamFailureMessage) != 1 {
		t.Fatalf("malformed completion output=%q, want partial plus one generic terminal", output)
	}
	if strings.Contains(output, "event: response.completed") || strings.Contains(output, "\"type\":\"response.completed\"") {
		t.Fatalf("malformed completion leaked before the generic terminal: %q", output)
	}
	if sanitizer.protocolTerminal && sanitizer.ErrorText() == "" {
		t.Fatalf("malformed completion was treated as a successful terminal: state=%+v", sanitizer)
	}
}

func TestGatewaySSESanitizerRejectsMalformedClaudeMessageStop(t *testing.T) {
	prefix := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"role\":\"assistant\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\n"
	for _, tc := range []struct {
		name     string
		terminal string
	}{
		{name: "missing payload", terminal: "event: message_stop\n\n"},
		{name: "payload type mismatch", terminal: "event: message_stop\ndata: {\"type\":\"message_delta\"}\n\n"},
		{name: "event type mismatch", terminal: "event: message_delta\ndata: {\"type\":\"message_stop\"}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer, output, err := readGatewaySSEProtocolStream(t, prefix+tc.terminal, "/v1/messages")
			if err != nil {
				t.Fatalf("read malformed message_stop stream: %v", err)
			}
			if !strings.Contains(output, "partial") || strings.Count(output, gatewayPublicStreamFailureMessage) != 1 {
				t.Fatalf("malformed message_stop output=%q, want partial plus one generic terminal", output)
			}
			if strings.Contains(output, "event: message_stop") || strings.Contains(output, `"type":"message_stop"`) {
				t.Fatalf("malformed message_stop leaked before the generic terminal: %q", output)
			}
			if sanitizer.protocolTerminal && sanitizer.ErrorText() == "" {
				t.Fatalf("malformed message_stop was treated as a successful terminal: state=%+v", sanitizer)
			}
		})
	}
}

func TestGatewaySSESanitizerRejectsMixedChatDoneFrame(t *testing.T) {
	prefix := "data: {\"id\":\"chat_1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n"
	for _, tc := range []struct {
		name     string
		terminal string
	}{
		{name: "extra data", terminal: "data: [DONE]\ndata: {\"unexpected\":true}\n\n"},
		{name: "event field", terminal: "event: done\ndata: [DONE]\n\n"},
		{name: "error event", terminal: "event: error\ndata: [DONE]\n\n"},
		{name: "leading field whitespace", terminal: " data: [DONE]\n\n"},
		{name: "trailing marker whitespace", terminal: "data: [DONE] \n\n"},
		{name: "marker suffix", terminal: "data: [DONE]unexpected\n\n"},
		{name: "bare marker", terminal: "[DONE]\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer, output, err := readGatewaySSEProtocolStream(t, prefix+tc.terminal, "/v1/chat/completions")
			if err != nil {
				t.Fatalf("read mixed chat done stream: %v", err)
			}
			if !strings.Contains(output, "partial") || strings.Count(output, gatewayPublicStreamFailureMessage) != 1 {
				t.Fatalf("mixed chat done output=%q, want partial plus one generic terminal", output)
			}
			if strings.Contains(output, "[DONE]") || strings.Contains(output, "event: done") {
				t.Fatalf("mixed chat done frame leaked before the generic terminal: %q", output)
			}
			if sanitizer.ErrorText() == "" {
				t.Fatal("mixed chat done frame did not record a failure")
			}
		})
	}
}

func TestGatewayLegacyCompletionAcceptanceRequiresDataOnlyFrame(t *testing.T) {
	payload := `{"id":"cmpl_1","object":"text_completion","choices":[{"text":"partial","index":0,"finish_reason":null}]}`
	if _, accepted := gatewayStreamFrameAcceptanceEvidence([]byte("event: heartbeat\ndata: "+payload+"\n\n"), "/v1/completions"); accepted {
		t.Fatal("legacy completion delta with an arbitrary event field established acceptance")
	}
	if evidence, accepted := gatewayStreamFrameAcceptanceEvidence([]byte("data: "+payload+"\n\n"), "/v1/completions"); !accepted || evidence.EventType != "text_completion.delta" || evidence.ObjectID != "cmpl_1" {
		t.Fatalf("data-only legacy completion acceptance = %+v accepted=%t", evidence, accepted)
	}
}

func TestGatewaySSESanitizerRejectsMixedLegacyCompletionDoneFrame(t *testing.T) {
	prefix := "data: {\"id\":\"cmpl_1\",\"object\":\"text_completion\",\"choices\":[{\"text\":\"partial\",\"index\":0,\"finish_reason\":null}]}\n\n"
	for _, tc := range []struct {
		name     string
		terminal string
	}{
		{name: "extra data", terminal: "data: [DONE]\ndata: {\"unexpected\":true}\n\n"},
		{name: "event field", terminal: "event: done\ndata: [DONE]\n\n"},
		{name: "error event", terminal: "event: error\ndata: [DONE]\n\n"},
		{name: "leading field whitespace", terminal: " data: [DONE]\n\n"},
		{name: "trailing marker whitespace", terminal: "data: [DONE] \n\n"},
		{name: "marker suffix", terminal: "data: [DONE]unexpected\n\n"},
		{name: "bare marker", terminal: "[DONE]\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sanitizer, output, err := readGatewaySSEProtocolStream(t, prefix+tc.terminal, "/v1/completions")
			if err != nil {
				t.Fatalf("read mixed legacy done stream: %v", err)
			}
			if !strings.Contains(output, "partial") || strings.Count(output, gatewayPublicStreamFailureMessage) != 1 {
				t.Fatalf("mixed legacy done output=%q, want partial plus one generic terminal", output)
			}
			if strings.Contains(output, "[DONE]") || strings.Contains(output, "event: done") {
				t.Fatalf("mixed legacy done frame leaked before the generic terminal: %q", output)
			}
			if sanitizer.ErrorText() == "" {
				t.Fatal("mixed legacy done frame did not record a failure")
			}
		})
	}
}

func TestGatewaySSEFrameContainsDoneOnlyAtTerminalFieldStart(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame string
		want  bool
	}{
		{name: "strict terminal", frame: "data: [DONE]\n\n", want: true},
		{name: "marker suffix", frame: "data: [DONE]unexpected\n\n", want: true},
		{name: "bare marker", frame: "[DONE]\n\n", want: true},
		{name: "json string", frame: "data: {\"text\":\"[DONE]unexpected\"}\n\n", want: false},
		{name: "ordinary text", frame: "data: ordinary [DONE] text\n\n", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := gatewaySSEFrameContainsDone([]byte(tc.frame)); got != tc.want {
				t.Fatalf("gatewaySSEFrameContainsDone(%q) = %t, want %t", tc.frame, got, tc.want)
			}
		})
	}
}
