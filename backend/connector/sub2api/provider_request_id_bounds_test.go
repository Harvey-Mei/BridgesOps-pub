package sub2api

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
			prefix := "client:" + strings.Repeat("p", size-len("client:")-1)
			requestID := prefix + "a"
			otherID := prefix + "b"
			now := time.Now().UTC().Truncate(time.Second)
			var usageCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				usageCalls.Add(1)
				if got := request.URL.Query().Get("request_id"); got != requestID {
					t.Errorf("request_id query bytes=%d value=%q, want exact %q", len(got), got, requestID)
				}
				_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "message": "success", "data": map[string]any{
					"items": []map[string]any{
						{"id": 8, "api_key_id": 9, "request_id": requestID, "model": "gpt-test", "actual_cost": 2, "created_at": now.Format(time.RFC3339Nano)},
						{"id": 7, "api_key_id": 9, "request_id": otherID, "model": "gpt-test", "actual_cost": 9, "created_at": now.Add(-time.Second).Format(time.RFC3339Nano)},
					},
					"total": 2, "page": 1, "page_size": 1000, "pages": 1,
				}})
			}))
			defer server.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
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
	for name, requestID := range map[string]string{
		"query over 512 bytes":       strings.Repeat("q", providerid.MaxBytes+1),
		"canonical prefix overflows": strings.Repeat("q", providerid.MaxBytes),
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				calls.Add(1)
			}))
			defer server.Close()
			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: requestID,
			})
			if result != nil || !errors.Is(err, providerid.ErrInvalid) || calls.Load() != 0 {
				t.Fatalf("overlong query calls=%d result=%+v err=%v", calls.Load(), result, err)
			}
		})
	}

	t.Run("usage response without truncation", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Second)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "message": "success", "data": map[string]any{
				"items": []map[string]any{{
					"id": 7, "api_key_id": 9, "request_id": strings.Repeat("r", providerid.MaxBytes+1),
					"actual_cost": 2, "created_at": now.Format(time.RFC3339Nano),
				}},
				"total": 1, "page": 1, "page_size": 1000, "pages": 1,
			}})
		}))
		defer server.Close()
		result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
			APIKeyID: 9, RequestID: "client:valid-request-id", StartedAt: now, FinishedAt: now,
		})
		if result != nil || !errors.Is(err, providerid.ErrInvalid) {
			t.Fatalf("overlong usage response result=%+v err=%v", result, err)
		}
	})
}

func TestLookupRequestCostDoesNotCollapseProviderRequestIDWhitespace(t *testing.T) {
	for _, usageRequestID := range []string{
		" client:provider-id",
		"client: provider-id",
		"client:provider-id ",
		"\u00a0client:provider-id",
		"client:\u00a0provider-id",
		"client:provider-id\u00a0",
	} {
		t.Run(usageRequestID, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Second)
			var usageCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				usageCalls.Add(1)
				if got := request.URL.Query().Get("request_id"); got != "client:provider-id" {
					t.Errorf("request_id query = %q, want exact canonical provider ID", got)
				}
				_ = json.NewEncoder(writer).Encode(map[string]any{"code": 0, "message": "success", "data": map[string]any{
					"items": []map[string]any{{
						"id": 7, "api_key_id": 9, "request_id": usageRequestID, "actual_cost": 2,
						"created_at": now.Format(time.RFC3339Nano),
					}},
					"total": 1, "page": 1, "page_size": 1000, "pages": 1,
				}})
			}))
			defer server.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: "provider-id", StartedAt: now, FinishedAt: now,
			})
			if result != nil || err == nil || !strings.Contains(err.Error(), "usage record not found") || usageCalls.Load() != 2 {
				t.Fatalf("whitespace ID %q calls=%d result=%+v err=%v", usageRequestID, usageCalls.Load(), result, err)
			}
		})
	}
}

func TestLookupRequestCostRejectsInvalidUTF8ProviderRequestID(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	validBody := []byte(`{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"request_id":"client:bad\ufffdid","actual_cost":2,"created_at":"` + now.Format(time.RFC3339Nano) + `"}],"total":1,"page":1,"page_size":1000,"pages":1}}`)
	invalidBody := append([]byte(`{"code":0,"message":"success","data":{"items":[{"id":7,"api_key_id":9,"request_id":"client:bad`), 0xff)
	invalidBody = append(invalidBody, []byte(`id","actual_cost":2,"created_at":"`+now.Format(time.RFC3339Nano)+`"}],"total":1,"page":1,"page_size":1000,"pages":1}}`)...)

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
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				call := usageCalls.Add(1)
				if test.validFirstPage && call == 1 {
					_, _ = writer.Write(validBody)
					return
				}
				_, _ = writer.Write(invalidBody)
			}))
			defer server.Close()

			result, err := New().LookupRequestCost(context.Background(), &connector.Channel{SiteURL: server.URL}, &connector.AuthSession{AccessToken: "token"}, connector.RequestCostQuery{
				APIKeyID: 9, RequestID: "bad\ufffdid", StartedAt: now, FinishedAt: now,
			})
			if result != nil || err == nil || !strings.Contains(err.Error(), test.wantErrorPrefix) || usageCalls.Load() != test.wantUsageCalls {
				t.Fatalf("calls=%d result=%+v err=%v, want %q", usageCalls.Load(), result, err, test.wantErrorPrefix)
			}
		})
	}
}
