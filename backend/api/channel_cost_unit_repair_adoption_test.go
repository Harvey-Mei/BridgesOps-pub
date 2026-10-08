package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type channelCostUnitRepairAdopterFake struct {
	previewResult *storage.CostUnitRepairAdoptionPreview
	previewErr    error
	previewCalls  int
	previewAnchor storage.CostUnitRepairAdoptionAnchor

	applyResult         *storage.ChannelCostUnitRepairAdoption
	applyReplayed       bool
	applyErr            error
	applyCalls          int
	applyAnchor         storage.CostUnitRepairAdoptionAnchor
	applyPreviewDigest  string
	applyIdempotencyKey string
	applyActor          string
}

func (f *channelCostUnitRepairAdopterFake) PreviewCostUnitRepairAdoption(anchor storage.CostUnitRepairAdoptionAnchor) (*storage.CostUnitRepairAdoptionPreview, error) {
	f.previewCalls++
	f.previewAnchor = anchor
	return f.previewResult, f.previewErr
}

func (f *channelCostUnitRepairAdopterFake) ApplyCostUnitRepairAdoption(anchor storage.CostUnitRepairAdoptionAnchor, previewDigest, idempotencyKey, actor string) (*storage.ChannelCostUnitRepairAdoption, bool, error) {
	f.applyCalls++
	f.applyAnchor = anchor
	f.applyPreviewDigest = previewDigest
	f.applyIdempotencyKey = idempotencyKey
	f.applyActor = actor
	return f.applyResult, f.applyReplayed, f.applyErr
}

func channelCostUnitRepairAdoptionTestRouter(t *testing.T, adopter channelCostUnitRepairAdopter) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "  repair-admin  ")
		c.Next()
	})
	registerChannels(router.Group("/api"), &Deps{CostUnitRepair: adopter})
	return router
}

func channelCostUnitRepairAdoptionRequest(t *testing.T, path string, body any, idempotencyKey string) *http.Request {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return req
}

func testCostUnitRepairAdoptionAnchor() storage.CostUnitRepairAdoptionAnchor {
	return storage.CostUnitRepairAdoptionAnchor{
		ChannelID: 32, ExpectedIdentityRevision: 7, ExpectedAccountRecordID: 11,
		ExpectedStateVersion: 19, ExpectedCostUnitEpoch: 3, SourceEventHighWater: 60,
		ResetEventID: 59, CorrectionEventID: 60, RequestLogHighWater: 305032,
		CostSampledAtBefore:    time.Date(2026, time.July, 26, 1, 46, 23, 0, time.UTC),
		CostSampledAtAfter:     time.Date(2026, time.July, 26, 2, 33, 4, 0, time.UTC),
		AffectedStartedAt:      time.Date(2026, time.July, 26, 1, 46, 23, 337852000, time.UTC),
		AffectedEndedAt:        time.Date(2026, time.July, 26, 2, 33, 4, 411928000, time.UTC),
		PreviousProviderFactor: "0.1", CurrentProviderFactor: "1", FactorSource: "advertised",
	}
}

func TestChannelCostUnitRepairAdoptionPreviewBuildsServerOwnedAnchor(t *testing.T) {
	wantAnchor := testCostUnitRepairAdoptionAnchor()
	fake := &channelCostUnitRepairAdopterFake{previewResult: &storage.CostUnitRepairAdoptionPreview{
		Anchor: wantAnchor, Digest: strings.Repeat("a", 64),
		RequestRestatement: &storage.CostUnitRequestRestatementEvidence{RequestLogHighWater: wantAnchor.RequestLogHighWater},
	}}
	input := channelCostUnitRepairAdoptionPreviewInput{
		ResetEventID: wantAnchor.ResetEventID, CorrectionEventID: wantAnchor.CorrectionEventID,
		CostSampledAtBefore: wantAnchor.CostSampledAtBefore, CostSampledAtAfter: wantAnchor.CostSampledAtAfter,
		AffectedStartedAt: wantAnchor.AffectedStartedAt, AffectedEndedAt: wantAnchor.AffectedEndedAt,
		PreviousProviderFactor: wantAnchor.PreviousProviderFactor, CurrentProviderFactor: wantAnchor.CurrentProviderFactor,
	}
	recorder := httptest.NewRecorder()
	channelCostUnitRepairAdoptionTestRouter(t, fake).ServeHTTP(recorder,
		channelCostUnitRepairAdoptionRequest(t, "/api/channels/32/cost-unit-repair-adoption/preview", input, ""))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if fake.previewCalls != 1 {
		t.Fatalf("preview calls = %d", fake.previewCalls)
	}
	got := fake.previewAnchor
	if got.ChannelID != 32 || got.FactorSource != "advertised" {
		t.Fatalf("server-owned anchor fields = %+v", got)
	}
	if got.ResetEventID != wantAnchor.ResetEventID || got.CorrectionEventID != wantAnchor.CorrectionEventID ||
		got.PreviousProviderFactor != wantAnchor.PreviousProviderFactor || got.CurrentProviderFactor != wantAnchor.CurrentProviderFactor {
		t.Fatalf("preview anchor = %+v", got)
	}
	if got.ExpectedIdentityRevision != 0 || got.ExpectedStateVersion != 0 || got.RequestLogHighWater != 0 {
		t.Fatalf("preview accepted server-generated evidence: %+v", got)
	}
}

func TestChannelCostUnitRepairAdoptionApplyPassesApprovedPreviewAndActor(t *testing.T) {
	anchor := testCostUnitRepairAdoptionAnchor()
	adoption := &storage.ChannelCostUnitRepairAdoption{
		ID: 9, ChannelID: 32, ResetEventID: 59, CorrectionEventID: 60,
		Actor: "repair-admin", PreviewDigest: strings.Repeat("b", 64),
	}
	fake := &channelCostUnitRepairAdopterFake{applyResult: adoption, applyReplayed: true}
	recorder := httptest.NewRecorder()
	channelCostUnitRepairAdoptionTestRouter(t, fake).ServeHTTP(recorder,
		channelCostUnitRepairAdoptionRequest(t, "/api/channels/32/cost-unit-repair-adoption", channelCostUnitRepairAdoptionApplyInput{
			Anchor: anchor, PreviewDigest: adoption.PreviewDigest,
		}, "  july-26-repair-adoption  "))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if fake.applyCalls != 1 || fake.applyAnchor != anchor {
		t.Fatalf("apply call = %d, anchor=%+v", fake.applyCalls, fake.applyAnchor)
	}
	if fake.applyPreviewDigest != adoption.PreviewDigest || fake.applyIdempotencyKey != "july-26-repair-adoption" || fake.applyActor != "repair-admin" {
		t.Fatalf("approval metadata = digest %q, key %q, actor %q", fake.applyPreviewDigest, fake.applyIdempotencyKey, fake.applyActor)
	}
	var response struct {
		Data channelCostUnitRepairAdoptionResult `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Data.Adoption == nil || response.Data.Adoption.ID != adoption.ID || !response.Data.IdempotentReplay {
		t.Fatalf("response = %+v", response.Data)
	}
}

func TestChannelCostUnitRepairAdoptionRejectsRouteMismatchAndClientControlledPreviewFields(t *testing.T) {
	t.Run("route mismatch", func(t *testing.T) {
		fake := &channelCostUnitRepairAdopterFake{}
		recorder := httptest.NewRecorder()
		channelCostUnitRepairAdoptionTestRouter(t, fake).ServeHTTP(recorder,
			channelCostUnitRepairAdoptionRequest(t, "/api/channels/33/cost-unit-repair-adoption", channelCostUnitRepairAdoptionApplyInput{
				Anchor: testCostUnitRepairAdoptionAnchor(), PreviewDigest: strings.Repeat("c", 64),
			}, "route-mismatch"))
		if recorder.Code != http.StatusBadRequest || fake.applyCalls != 0 {
			t.Fatalf("status = %d, calls = %d, body=%s", recorder.Code, fake.applyCalls, recorder.Body.String())
		}
	})

	for _, field := range []string{`"channel_id":32`, `"factor_source":"manual"`, `"actor":"forged"`} {
		t.Run(field, func(t *testing.T) {
			fake := &channelCostUnitRepairAdopterFake{}
			body := fmt.Sprintf(`{"reset_event_id":59,"correction_event_id":60,%s}`, field)
			req := httptest.NewRequest(http.MethodPost, "/api/channels/32/cost-unit-repair-adoption/preview", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			channelCostUnitRepairAdoptionTestRouter(t, fake).ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest || fake.previewCalls != 0 {
				t.Fatalf("status = %d, calls = %d, body=%s", recorder.Code, fake.previewCalls, recorder.Body.String())
			}
		})
	}
}

func TestChannelCostUnitRepairAdoptionRequiresValidIdempotencyKey(t *testing.T) {
	for _, key := range []string{"", "invalid key", strings.Repeat("a", 129)} {
		t.Run(fmt.Sprintf("key length %d", len(key)), func(t *testing.T) {
			fake := &channelCostUnitRepairAdopterFake{}
			recorder := httptest.NewRecorder()
			channelCostUnitRepairAdoptionTestRouter(t, fake).ServeHTTP(recorder,
				channelCostUnitRepairAdoptionRequest(t, "/api/channels/32/cost-unit-repair-adoption", channelCostUnitRepairAdoptionApplyInput{
					Anchor: testCostUnitRepairAdoptionAnchor(), PreviewDigest: strings.Repeat("d", 64),
				}, key))
			if recorder.Code != http.StatusBadRequest || fake.applyCalls != 0 {
				t.Fatalf("status = %d, calls = %d, body=%s", recorder.Code, fake.applyCalls, recorder.Body.String())
			}
		})
	}
}

func TestChannelCostUnitRepairAdoptionRejectsClientControlledApplyFieldsAndInvalidBodies(t *testing.T) {
	anchorJSON, err := json.Marshal(testCostUnitRepairAdoptionAnchor())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"actor":"forged"`, `"idempotency_key":"forged"`, `"channel_id":32`} {
		t.Run(field, func(t *testing.T) {
			fake := &channelCostUnitRepairAdopterFake{}
			body := fmt.Sprintf(`{"anchor":%s,"preview_digest":%q,%s}`, anchorJSON, strings.Repeat("d", 64), field)
			req := httptest.NewRequest(http.MethodPost, "/api/channels/32/cost-unit-repair-adoption", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "reject-client-fields")
			recorder := httptest.NewRecorder()
			channelCostUnitRepairAdoptionTestRouter(t, fake).ServeHTTP(recorder, req)
			if recorder.Code != http.StatusBadRequest || fake.applyCalls != 0 {
				t.Fatalf("status = %d, calls = %d, body=%s", recorder.Code, fake.applyCalls, recorder.Body.String())
			}
		})
	}

	validBody := fmt.Sprintf(`{"anchor":%s,"preview_digest":%q}`, anchorJSON, strings.Repeat("d", 64))
	for _, test := range []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "trailing JSON", body: validBody + `{}`, wantStatus: http.StatusBadRequest},
		{name: "oversized body", body: validBody + strings.Repeat(" ", channelCostUnitRebaseBodyLimit), wantStatus: http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &channelCostUnitRepairAdopterFake{}
			req := httptest.NewRequest(http.MethodPost, "/api/channels/32/cost-unit-repair-adoption", strings.NewReader(test.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "reject-invalid-body")
			recorder := httptest.NewRecorder()
			channelCostUnitRepairAdoptionTestRouter(t, fake).ServeHTTP(recorder, req)
			if recorder.Code != test.wantStatus || fake.applyCalls != 0 {
				t.Fatalf("status = %d, want %d, calls = %d, body=%s", recorder.Code, test.wantStatus, fake.applyCalls, recorder.Body.String())
			}
		})
	}
}

func TestChannelCostUnitRepairAdoptionMapsDomainErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantError  string
	}{
		{name: "not found", err: gorm.ErrRecordNotFound, wantStatus: http.StatusNotFound, wantError: "channel or repair evidence was not found"},
		{name: "invalid anchor", err: fmt.Errorf("private detail: %w", storage.ErrCostUnitRepairAdoptionInvalid), wantStatus: http.StatusUnprocessableEntity, wantError: storage.ErrCostUnitRepairAdoptionInvalid.Error()},
		{name: "preview changed", err: fmt.Errorf("private detail: %w", storage.ErrCostUnitRepairAdoptionConflict), wantStatus: http.StatusConflict, wantError: storage.ErrCostUnitRepairAdoptionConflict.Error()},
		{name: "restatement changed", err: fmt.Errorf("private detail: %w", storage.ErrCostUnitRequestRestatementConflict), wantStatus: http.StatusConflict, wantError: storage.ErrCostUnitRequestRestatementConflict.Error()},
		{name: "identity unbound", err: fmt.Errorf("private detail: %w", storage.ErrUpstreamAccountIdentityUnbound), wantStatus: http.StatusConflict, wantError: storage.ErrUpstreamAccountIdentityUnbound.Error()},
		{name: "unknown", err: errors.New("private database detail"), wantStatus: http.StatusInternalServerError, wantError: "cost unit repair adoption failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &channelCostUnitRepairAdopterFake{applyErr: tt.err}
			recorder := httptest.NewRecorder()
			channelCostUnitRepairAdoptionTestRouter(t, fake).ServeHTTP(recorder,
				channelCostUnitRepairAdoptionRequest(t, "/api/channels/32/cost-unit-repair-adoption", channelCostUnitRepairAdoptionApplyInput{
					Anchor: testCostUnitRepairAdoptionAnchor(), PreviewDigest: strings.Repeat("e", 64),
				}, "domain-error"))
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			var response struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(response.Error, tt.wantError) || strings.Contains(response.Error, "private detail") || strings.Contains(response.Error, "private database detail") {
				t.Fatalf("error = %q, want stable %q", response.Error, tt.wantError)
			}
		})
	}
}

func TestChannelCostUnitRepairAdoptionUnavailableWithoutDependency(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	registerChannels(router.Group("/api"), &Deps{})
	for _, path := range []string{
		"/api/channels/32/cost-unit-repair-adoption/preview",
		"/api/channels/32/cost-unit-repair-adoption",
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, channelCostUnitRepairAdoptionRequest(t, path, struct{}{}, "dependency-check"))
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("path %s status = %d, body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
}
