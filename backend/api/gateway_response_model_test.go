package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestGatewayResponseModelAudit(t *testing.T) {
	for _, test := range []struct{ name, body, want string }{
		{"chat", `{"model":"gpt-example-2026-09-01","usage":{"prompt_tokens":20}}`, "gpt-example-2026-09-01"},
		{"responses lifecycle", `{"type":"response.created","response":{"model":"gpt-example"}}`, "gpt-example"},
		{"messages lifecycle", `{"type":"message_start","message":{"model":"claude-example"}}`, "claude-example"},
		{"nested tool model is not response evidence", `{"output":[{"model":"wrong"}]}`, ""},
		{"invalid model", `{"model":"bad\nmodel"}`, ""},
		{"oversized metadata", `{"model":"` + strings.Repeat("x", 4096) + `"}`, ""},
		{"absent", `{"usage":{"input_tokens":20}}`, ""},
		{"root after response", `{"response":{"model":"actual"},"model":"wrapper"}`, "actual"},
		{"root before response", `{"model":"wrapper","response":{"model":"actual"}}`, "actual"},
		{"key-like metadata", `{"model":"sk-test-secret"}`, ""},
		{"oversized numeric metadata", `{"model":` + strings.Repeat("1", 1024) + `}`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			metrics := parseGatewayUsageMetricsForPath([]byte(test.body), "/v1/responses")
			projected := projectGatewayResponseForTest(t, []byte(test.body), "/v1/responses")
			stream := parseGatewayStreamUsageMetricsForPath([]byte("data: "+test.body+"\n\n"), "/v1/responses")
			for _, got := range []string{metrics.ResponseModel, projected.Usage.ResponseModel, stream.ResponseModel} {
				if got != test.want {
					t.Fatalf("reported model = %q, want %q", got, test.want)
				}
			}
			log := storage.GatewayRequestLog{ModelName: "sent-model", UpstreamModelName: "billing-model"}
			applyGatewayRequestUsageAndCost(&log, metrics, false, 1)
			if log.UpstreamResponseModel != test.want || log.ModelName != "sent-model" || log.UpstreamModelName != "billing-model" {
				t.Fatalf("response evidence changed another model identity: %+v", log)
			}
		})
	}
}

func TestGatewayResponseModelSurvivesStreamTailEviction(t *testing.T) {
	input := "data: {\"model\":\"reported-model\",\"choices\":[]}\n\n" + strings.Repeat("data: {\"choices\":[{\"delta\":{\"content\":\"text\"}}]}\n\n", 100) + "data: [DONE]\n\n"
	tail := &gatewayTailBuffer{limit: 128}
	s := newGatewaySSEStreamSanitizer(io.NopCloser(strings.NewReader(input)), tail, "", "/v1/chat/completions", nil)
	output, err := io.ReadAll(s)
	if err != nil || string(output) != input || s.responseModel != "reported-model" || strings.Contains(string(tail.Bytes()), "reported-model") {
		t.Fatalf("stream model lost or delivery changed: model=%q error=%v", s.responseModel, err)
	}
}

func TestGatewayRequestModelMismatchUsesSentModel(t *testing.T) {
	for _, test := range []struct {
		response string
		mismatch *bool
	}{
		{"", nil}, {"sent-model", boolPointerForModelTest(false)}, {"different-model", boolPointerForModelTest(true)},
	} {
		out := gatewayRequestLogOutputFromItem(storage.GatewayRequestLogItem{
			ModelName: "sent-model", RequestedModelName: "client-alias", UpstreamModelName: "billing-model", UpstreamResponseModel: test.response,
		})
		if (out.UpstreamModelMismatch == nil) != (test.mismatch == nil) || (test.mismatch != nil && *out.UpstreamModelMismatch != *test.mismatch) {
			t.Fatalf("incorrect comparison for response %q: %+v", test.response, out.UpstreamModelMismatch)
		}
		encoded, err := json.Marshal(out)
		if err != nil || test.mismatch == nil && strings.Contains(string(encoded), "upstream_model_mismatch") {
			t.Fatal("missing response model became mismatch evidence")
		}
	}
}

func boolPointerForModelTest(value bool) *bool { return &value }

func TestGatewayRoutePriceOutputPreservesZeroInputShare(t *testing.T) {
	encoded, err := json.Marshal(gatewayRouteCandidateOutput{EffectivePriceBasis: "model_price_usd_per_million", PriceInputShare: 0})
	if err != nil || !strings.Contains(string(encoded), `"price_input_share":0`) {
		t.Fatal("zero input share omitted", err)
	}
}

func TestGatewayForwardPersistsResponseModel(t *testing.T) {
	for _, mode := range []string{"json", "large-json", "sse", "credential-echo"} {
		t.Run(mode, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				model := "reported-model"
				if mode == "credential-echo" {
					model = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				}
				if mode == "sse" {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"model\":\"reported-model\",\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
					return
				}
				content := "ok"
				if mode == "large-json" {
					content = strings.Repeat("x", 256<<10)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"model": model, "choices": []any{map[string]any{"message": map[string]any{"content": content}}}, "usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 2}})
			}))
			defer upstream.Close()
			svc, store, decision := setupGatewayForwardWithCachedCandidates(t, []gatewayForwardTestCandidate{{channelID: 1, name: "audit", groupName: "primary", siteURL: upstream.URL, ratio: 1}})
			request := `{"model":"gpt-test","stream":false}`
			if mode == "sse" {
				request = `{"model":"gpt-test","stream":true}`
			}
			response := runGatewayRetryTestRequestWithMode(svc, decision, "/v1/chat/completions", request, false)
			if response.Code != http.StatusOK {
				t.Fatalf("forward status = %d", response.Code)
			}
			rows, err := store.ListRequestLogsFiltered(storage.GatewayRequestLogFilter{}, 2)
			want := "reported-model"
			if mode == "credential-echo" {
				want = ""
			}
			if err != nil || len(rows) != 1 || rows[0].UpstreamResponseModel != want {
				t.Fatalf("persisted response model: rows=%+v, error=%v", rows, err)
			}
		})
	}
}
