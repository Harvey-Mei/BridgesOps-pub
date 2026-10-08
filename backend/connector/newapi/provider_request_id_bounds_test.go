package newapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/providerid"
)

func TestLookupRequestCostProviderRequestIDBoundariesAndExactCommonPrefixes(t *testing.T) {
	for _, test := range []struct {
		name string
		size int
	}{
		{name: "legacy 128 bytes", size: 128},
		{name: "maximum 512 bytes", size: providerid.MaxBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			size := test.size
			prefix := strings.Repeat("p", size-1)
			requestID := prefix + "a"
			otherID := prefix + "b"
			now := time.Now().UTC().Truncate(time.Second)
			var usageCalls atomic.Int32
			mux := http.NewServeMux()
			mux.HandleFunc("/api/status", func(writer http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(writer).Encode(map[string]any{"success": true, "data": map[string]any{"quota_per_unit": 1}})
			})
			mux.HandleFunc("/api/log/self", func(writer http.ResponseWriter, request *http.Request) {
				usageCalls.Add(1)
				if got := request.URL.Query().Get("request_id"); got != requestID {
					t.Errorf("request_id query bytes=%d value=%q, want exact %q", len(got), got, requestID)
				}
				_ = json.NewEncoder(writer).Encode(map[string]any{"success": true, "data": map[string]any{
					"items": []map[string]any{
						{"id": 8, "created_at": now.Unix(), "token_id": 9, "model_name": "gpt-test", "quota": 2, "request_id": requestID},
						{"id": 7, "created_at": now.Unix(), "token_id": 9, "model_name": "gpt-test", "quota": 9, "request_id": otherID},
					},
					"total": 2, "page": 1, "page_size": 100, "pages": 1,
				}})
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: requestID, StartedAt: now, FinishedAt: now,
			})
			if err != nil {
				t.Fatalf("LookupRequestCost(%d bytes): %v", size, err)
			}
			if result == nil || result.Cost != 2 || usageCalls.Load() != 2 {
				t.Fatalf("LookupRequestCost(%d bytes) calls=%d result=%+v", size, usageCalls.Load(), result)
			}
		})
	}
}

func TestLookupRequestCostRejectsOverlongProviderRequestIDs(t *testing.T) {
	t.Run("query before network", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			calls.Add(1)
		}))
		defer server.Close()
		result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
			APIKeyID: 9, RequestID: strings.Repeat("q", providerid.MaxBytes+1),
		})
		if result != nil || !errors.Is(err, providerid.ErrInvalid) || calls.Load() != 0 {
			t.Fatalf("overlong query calls=%d result=%+v err=%v", calls.Load(), result, err)
		}
	})

	t.Run("usage response without truncation", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Second)
		mux := http.NewServeMux()
		mux.HandleFunc("/api/status", func(writer http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(writer).Encode(map[string]any{"success": true, "data": map[string]any{"quota_per_unit": 1}})
		})
		mux.HandleFunc("/api/log/self", func(writer http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(writer).Encode(map[string]any{"success": true, "data": map[string]any{
				"items": []map[string]any{{"id": 7, "created_at": now.Unix(), "token_id": 9, "quota": 2, "request_id": strings.Repeat("r", providerid.MaxBytes+1)}},
				"total": 1, "page": 1, "page_size": 100, "pages": 1,
			}})
		})
		server := httptest.NewServer(mux)
		defer server.Close()
		result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
			APIKeyID: 9, RequestID: "valid-request-id", StartedAt: now, FinishedAt: now,
		})
		if result != nil || !errors.Is(err, providerid.ErrInvalid) {
			t.Fatalf("overlong usage response result=%+v err=%v", result, err)
		}
	})
}

func TestLookupRequestCostDoesNotCollapseProviderRequestIDWhitespace(t *testing.T) {
	for _, usageRequestID := range []string{
		" provider-id",
		"provider-id ",
		"\u00a0provider-id",
		"provider-id\u00a0",
	} {
		t.Run(usageRequestID, func(t *testing.T) {
			var usageCalls atomic.Int32
			mux := http.NewServeMux()
			mux.HandleFunc("/api/status", func(writer http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(writer).Encode(map[string]any{"success": true, "data": map[string]any{"quota_per_unit": 1}})
			})
			mux.HandleFunc("/api/log/self", func(writer http.ResponseWriter, request *http.Request) {
				usageCalls.Add(1)
				if got := request.URL.Query().Get("request_id"); got != "provider-id" {
					t.Errorf("request_id query = %q, want exact provider-id", got)
				}
				_ = json.NewEncoder(writer).Encode(map[string]any{"success": true, "data": map[string]any{
					"items": []map[string]any{{"id": 7, "created_at": 1, "token_id": 9, "quota": 2, "request_id": usageRequestID}},
					"total": 1, "page": 1, "page_size": 100, "pages": 1,
				}})
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: "provider-id",
			})
			if result != nil || err == nil || !strings.Contains(err.Error(), "usage record not found") || usageCalls.Load() != 2 {
				t.Fatalf("whitespace ID %q calls=%d result=%+v err=%v", usageRequestID, usageCalls.Load(), result, err)
			}
		})
	}
}

func TestLookupRequestCostRejectsInvalidUTF8ProviderRequestID(t *testing.T) {
	validBody := []byte(`{"success":true,"data":{"items":[{"id":7,"created_at":1,"token_id":9,"quota":2,"request_id":"bad\ufffdid"}],"total":1,"page":1,"page_size":100,"pages":1}}`)
	invalidBody := append([]byte(`{"success":true,"data":{"items":[{"id":7,"created_at":1,"token_id":9,"quota":2,"request_id":"bad`), 0xff)
	invalidBody = append(invalidBody, []byte(`id"}],"total":1,"page":1,"page_size":100,"pages":1}}`)...)

	for _, test := range []struct {
		name            string
		validFirstPage  bool
		wantUsageCalls  int32
		wantErrorPrefix string
	}{
		{name: "initial page", wantUsageCalls: 1, wantErrorPrefix: "usage response is not valid UTF-8"},
		{name: "final snapshot", validFirstPage: true, wantUsageCalls: 2, wantErrorPrefix: "final snapshot usage response is not valid UTF-8"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var usageCalls atomic.Int32
			mux := http.NewServeMux()
			mux.HandleFunc("/api/status", func(writer http.ResponseWriter, _ *http.Request) {
				_, _ = writer.Write([]byte(`{"success":true,"data":{"quota_per_unit":1}}`))
			})
			mux.HandleFunc("/api/log/self", func(writer http.ResponseWriter, _ *http.Request) {
				call := usageCalls.Add(1)
				if test.validFirstPage && call == 1 {
					_, _ = writer.Write(validBody)
					return
				}
				_, _ = writer.Write(invalidBody)
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: "bad\ufffdid",
			})
			if result != nil || err == nil || !strings.Contains(err.Error(), test.wantErrorPrefix) || usageCalls.Load() != test.wantUsageCalls {
				t.Fatalf("calls=%d result=%+v err=%v, want %q", usageCalls.Load(), result, err, test.wantErrorPrefix)
			}
		})
	}
}
