package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/costing"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
)

type channelCostUnitRebaserFake struct {
	result    *costing.CostUnitRebaseResult
	err       error
	calls     int
	channelID uint
	command   costing.CostUnitRebaseCommand
}

type channelCostBasisOverriderFake struct {
	result  *costing.CostBasisOverrideResult
	err     error
	calls   int
	channel uint
	command costing.CostBasisOverrideCommand
}

func (f *channelCostBasisOverriderFake) OverrideCostBasis(channelID uint, command costing.CostBasisOverrideCommand) (*costing.CostBasisOverrideResult, error) {
	f.calls++
	f.channel = channelID
	f.command = command
	return f.result, f.err
}

func channelCostBasisOverrideTestRouter(t *testing.T, overrider channelCostBasisOverrider) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "  cost-admin  ")
		c.Next()
	})
	registerChannels(router.Group("/api"), &Deps{CostBasisOverride: overrider})
	return router
}

func (f *channelCostUnitRebaserFake) RebaseCostUnit(_ context.Context, channelID uint, command costing.CostUnitRebaseCommand) (*costing.CostUnitRebaseResult, error) {
	f.calls++
	f.channelID = channelID
	f.command = command
	return f.result, f.err
}

func channelCostUnitRebaseTestRouter(t *testing.T, rebaser channelCostUnitRebaser) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("authSubject", "  cost-admin  ")
		c.Next()
	})
	registerChannels(router.Group("/api"), &Deps{CostUnitRebaser: rebaser})
	return router
}

func channelCostUnitRebaseRequest(body, idempotencyKey string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/channels/32/cost-unit-rebase", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return req
}

func TestChannelCostUnitRebasePassesAdministratorIntent(t *testing.T) {
	fake := &channelCostUnitRebaserFake{result: &costing.CostUnitRebaseResult{
		Audit: &storage.ChannelCostUnitRebase{ChannelID: 32, AppliedStateVersion: 18},
		State: &storage.ChannelCostState{ChannelID: 32, Version: 18},
	}}
	router := channelCostUnitRebaseTestRouter(t, fake)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, channelCostUnitRebaseRequest(
		`{"expected_version":17,"previous_multiplier":10,"current_multiplier":1,"note":"provider changed its accounting unit"}`,
		"  lima-ai-10-to-1  ",
	))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if fake.calls != 1 || fake.channelID != 32 {
		t.Fatalf("rebaser calls = %d, channel ID = %d", fake.calls, fake.channelID)
	}
	command := fake.command
	if command.ExpectedVersion != 17 || command.PreviousMultiplier != 10 || command.CurrentMultiplier != 1 {
		t.Fatalf("monetary intent = %+v", command)
	}
	if command.IdempotencyKey != "lima-ai-10-to-1" || command.Actor != "cost-admin" || command.Note != "provider changed its accounting unit" {
		t.Fatalf("audit intent = %+v", command)
	}
}

func TestChannelCostBasisOverridePassesFencesAndReason(t *testing.T) {
	fake := &channelCostBasisOverriderFake{result: &costing.CostBasisOverrideResult{
		Audit: &storage.ChannelCostBasisOverride{ChannelID: 32, AppliedStateVersion: 18},
	}}
	recorder := httptest.NewRecorder()
	router := channelCostBasisOverrideTestRouter(t, fake)
	req := httptest.NewRequest(http.MethodPost, "/api/channels/32/cost-basis-override", strings.NewReader(
		`{"expected_version":17,"expected_identity_revision":4,"expected_cost_unit_epoch":9,"funding_factor":0.25,"note":"finance ticket 42"}`,
	))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "manual-override-42")
	router.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK || fake.calls != 1 || fake.channel != 32 {
		t.Fatalf("status=%d body=%s calls=%d channel=%d", recorder.Code, recorder.Body.String(), fake.calls, fake.channel)
	}
	if fake.command.ExpectedVersion != 17 || fake.command.ExpectedIdentityRevision != 4 || fake.command.ExpectedCostUnitEpoch != 9 ||
		fake.command.FundingFactor != 0.25 || fake.command.Actor != "cost-admin" || fake.command.Note != "finance ticket 42" || fake.command.IdempotencyKey != "manual-override-42" {
		t.Fatalf("override command = %+v", fake.command)
	}
}

func TestChannelCostBasisOverrideReadinessAllowsArchivingPendingFundingCandidates(t *testing.T) {
	channel := &storage.Channel{ID: 32, UpstreamAccountIdentityRevision: 4, CostUnitEpoch: 9}
	state := &storage.ChannelCostState{ChannelID: 32, Version: 17, CostUnitEpoch: 9, FundingFactor: 0.25}
	readiness := buildChannelCostBasisOverrideReadiness(channel, state, 3)
	if !readiness.Eligible || readiness.PendingFundingCandidates != 3 || len(readiness.Blockers) != 0 {
		t.Fatalf("readiness = %+v", readiness)
	}
}

func TestChannelCostBasisOverrideRejectsClientControlledActorAndInvalidFactor(t *testing.T) {
	for _, body := range []string{
		`{"expected_version":17,"expected_identity_revision":4,"expected_cost_unit_epoch":9,"funding_factor":-1,"note":"reason"}`,
		`{"expected_version":17,"expected_identity_revision":4,"expected_cost_unit_epoch":9,"note":"reason"}`,
		`{"expected_version":17,"expected_identity_revision":4,"expected_cost_unit_epoch":9,"funding_factor":0.25,"note":"reason","actor":"forged"}`,
	} {
		fake := &channelCostBasisOverriderFake{}
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/channels/32/cost-basis-override", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "manual-override-invalid")
		channelCostBasisOverrideTestRouter(t, fake).ServeHTTP(recorder, req)
		if recorder.Code != http.StatusBadRequest || fake.calls != 0 {
			t.Fatalf("body=%s status=%d calls=%d response=%s", body, recorder.Code, fake.calls, recorder.Body.String())
		}
	}
}

func TestChannelCostUnitRebasePassesFrozenRequestRestatementEvidence(t *testing.T) {
	buckets := []time.Time{
		time.Date(2026, 7, 26, 1, 45, 0, 0, time.UTC),
		time.Date(2026, 7, 26, 2, 0, 0, 0, time.UTC),
	}
	digest, err := storage.CostUnitRequestRestatementBucketDigest(buckets)
	if err != nil {
		t.Fatal(err)
	}
	evidence := &storage.CostUnitRequestRestatementEvidence{
		RequestLogHighWater:    305032,
		AffectedStartedAt:      time.Date(2026, 7, 26, 1, 46, 23, 337852000, time.UTC),
		AffectedEndedAt:        time.Date(2026, 7, 26, 2, 3, 4, 411928000, time.UTC),
		OldFundingFactor:       "0.10050346486909446",
		NewFundingFactor:       "1.00503464869094",
		ExpectedPopulationRows: 50, ExpectedNewRatioRows: 45, ExpectedOldRatioRows: 3,
		ExpectedRatioMismatchRows: 2, ExpectedLatencyProbeRows: 0,
		ExpectedRestatementRows: 40, ExpectedAlreadyCorrectCostedRows: 2,
		ExpectedRawCostSum: "1.083942315", ExpectedRecordedCostSum: "0.1089399583757274",
		ExpectedCorrectedCostSum: "1.0893995837572692", ExpectedUndercountSum: "0.9804596253815418",
		ExpectedAlreadyCorrectRawCostSum: "0.009882900", ExpectedAllCostedRawCostSum: "1.093825215",
		ExpectedBucketStarts: buckets, ExpectedBucketDigest: digest,
	}
	body, err := json.Marshal(channelCostUnitRebaseInput{
		ExpectedVersion: 17, PreviousMultiplier: 10, CurrentMultiplier: 1,
		RequestRestatement: evidence,
	})
	if err != nil {
		t.Fatal(err)
	}
	fake := &channelCostUnitRebaserFake{result: &costing.CostUnitRebaseResult{
		Audit: &storage.ChannelCostUnitRebase{ChannelID: 32, AppliedStateVersion: 18},
	}}
	recorder := httptest.NewRecorder()
	channelCostUnitRebaseTestRouter(t, fake).ServeHTTP(recorder, channelCostUnitRebaseRequest(string(body), "request-restatement"))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	got := fake.command.RequestRestatement
	if got == nil || got.RequestLogHighWater != evidence.RequestLogHighWater ||
		got.OldFundingFactor != evidence.OldFundingFactor || got.NewFundingFactor != evidence.NewFundingFactor ||
		got.ExpectedRestatementRows != evidence.ExpectedRestatementRows || got.ExpectedBucketDigest != digest {
		t.Fatalf("request restatement evidence = %+v", got)
	}
}

func TestChannelCostUnitRebaseRejectsClientControlledFields(t *testing.T) {
	tests := []struct {
		name  string
		field string
	}{
		{name: "raw balance", field: `"raw_balance":9.63`},
		{name: "raw today cost", field: `"raw_today_cost":0.71`},
		{name: "raw total cost", field: `"raw_total_cost":80.60`},
		{name: "actor", field: `"actor":"forged-administrator"`},
		{name: "idempotency key", field: `"idempotency_key":"forged-key"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &channelCostUnitRebaserFake{}
			router := channelCostUnitRebaseTestRouter(t, fake)
			body := fmt.Sprintf(`{"expected_version":17,"previous_multiplier":10,"current_multiplier":1,%s}`, tt.field)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, channelCostUnitRebaseRequest(body, "reject-client-evidence"))

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
			}
			if fake.calls != 0 {
				t.Fatalf("rebaser called %d times for unknown field", fake.calls)
			}
		})
	}
}

func TestChannelCostUnitRebaseRequiresValidIdempotencyKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "missing"},
		{name: "invalid characters", key: "invalid key"},
		{name: "too long", key: strings.Repeat("a", 129)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &channelCostUnitRebaserFake{}
			router := channelCostUnitRebaseTestRouter(t, fake)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, channelCostUnitRebaseRequest(
				`{"expected_version":17,"previous_multiplier":10,"current_multiplier":1}`,
				tt.key,
			))

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
			}
			if fake.calls != 0 {
				t.Fatalf("rebaser called %d times for invalid idempotency key", fake.calls)
			}
		})
	}
}

func TestChannelCostUnitRebaseRejectsOversizedOrInvalidIntentBeforeRebaser(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "oversized trailing body",
			body:       `{"expected_version":17,"previous_multiplier":10,"current_multiplier":1}` + strings.Repeat(" ", channelCostUnitRebaseBodyLimit),
			wantStatus: http.StatusRequestEntityTooLarge,
		},
		{
			name:       "oversized note",
			body:       fmt.Sprintf(`{"expected_version":17,"previous_multiplier":10,"current_multiplier":1,"note":%q}`, strings.Repeat("n", 1025)),
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &channelCostUnitRebaserFake{}
			router := channelCostUnitRebaseTestRouter(t, fake)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, channelCostUnitRebaseRequest(tt.body, "invalid-intent"))

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			if fake.calls != 0 {
				t.Fatalf("rebaser called %d times for invalid intent", fake.calls)
			}
		})
	}
}

func TestChannelCostUnitRebaseMapsDomainErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantError  string
	}{
		{
			name:       "stale state conflict",
			err:        fmt.Errorf("stale request: %w", storage.ErrCostStateConflict),
			wantStatus: http.StatusConflict,
			wantError:  storage.ErrCostStateConflict.Error(),
		},
		{
			name:       "pending funding conflict",
			err:        fmt.Errorf("pending funding: %w", storage.ErrPendingFundingCandidate),
			wantStatus: http.StatusConflict,
			wantError:  storage.ErrPendingFundingCandidate.Error(),
		},
		{
			name:       "idempotency conflict",
			err:        fmt.Errorf("different request: %w", storage.ErrCostUnitRebaseIdempotencyConflict),
			wantStatus: http.StatusConflict,
			wantError:  storage.ErrCostUnitRebaseIdempotencyConflict.Error(),
		},
		{
			name:       "request restatement conflict",
			err:        fmt.Errorf("population drift: %w", storage.ErrCostUnitRequestRestatementConflict),
			wantStatus: http.StatusConflict,
			wantError:  storage.ErrCostUnitRequestRestatementConflict.Error(),
		},
		{
			name:       "evidence mismatch",
			err:        fmt.Errorf("sample mismatch: %w", costing.ErrCostUnitRebaseEvidenceMismatch),
			wantStatus: http.StatusUnprocessableEntity,
			wantError:  costing.ErrCostUnitRebaseEvidenceMismatch.Error(),
		},
		{
			name:       "rebase not required",
			err:        fmt.Errorf("no regression: %w", costing.ErrCostUnitRebaseNotRequired),
			wantStatus: http.StatusUnprocessableEntity,
			wantError:  costing.ErrCostUnitRebaseNotRequired.Error(),
		},
		{
			name:       "unexplained regression",
			err:        fmt.Errorf("wrong unit boundary: %w", costing.ErrCostUnitRebaseUnexplainedRegression),
			wantStatus: http.StatusUnprocessableEntity,
			wantError:  costing.ErrCostUnitRebaseUnexplainedRegression.Error(),
		},
		{
			name:       "invalid persisted evidence",
			err:        fmt.Errorf("invalid audit: %w", storage.ErrCostUnitRebaseInvalid),
			wantStatus: http.StatusUnprocessableEntity,
			wantError:  storage.ErrCostUnitRebaseInvalid.Error(),
		},
		{
			name:       "invalid request restatement evidence",
			err:        fmt.Errorf("invalid numeric proof: %w", storage.ErrCostUnitRequestRestatementInvalid),
			wantStatus: http.StatusUnprocessableEntity,
			wantError:  storage.ErrCostUnitRequestRestatementInvalid.Error(),
		},
		{
			name:       "upstream collection",
			err:        fmt.Errorf("private connector detail: %w", costing.ErrCostUnitRebaseUpstream),
			wantStatus: http.StatusBadGateway,
			wantError:  "failed to collect trusted upstream monetary evidence",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &channelCostUnitRebaserFake{err: tt.err}
			router := channelCostUnitRebaseTestRouter(t, fake)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, channelCostUnitRebaseRequest(
				`{"expected_version":17,"previous_multiplier":10,"current_multiplier":1}`,
				"domain-error-mapping",
			))

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			var response struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(response.Error, tt.wantError) {
				t.Fatalf("error = %q, want it to contain %q", response.Error, tt.wantError)
			}
			if strings.Contains(response.Error, "sample mismatch") || strings.Contains(response.Error, "wrong unit boundary") {
				t.Fatalf("wrapped financial evidence leaked in response: %q", response.Error)
			}
			if tt.wantStatus == http.StatusBadGateway && strings.Contains(response.Error, "private connector detail") {
				t.Fatalf("upstream detail leaked in response: %q", response.Error)
			}
			if fake.calls != 1 {
				t.Fatalf("rebaser calls = %d", fake.calls)
			}
		})
	}
}

func TestChannelCostUnitRebaseReturnsSuccessAndReplay(t *testing.T) {
	appliedAt := time.Date(2026, time.July, 26, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name   string
		result *costing.CostUnitRebaseResult
	}{
		{
			name: "applied",
			result: &costing.CostUnitRebaseResult{
				Audit: &storage.ChannelCostUnitRebase{
					ID: 7, ChannelID: 32, AppliedStateVersion: 18,
					PreviousMultiplier: 10, CurrentMultiplier: 1, AppliedAt: appliedAt,
				},
				State: &storage.ChannelCostState{ChannelID: 32, Version: 18, FundingFactor: 1.005},
			},
		},
		{
			name: "idempotent replay",
			result: &costing.CostUnitRebaseResult{
				Audit: &storage.ChannelCostUnitRebase{
					ID: 7, ChannelID: 32, AppliedStateVersion: 18,
					PreviousMultiplier: 10, CurrentMultiplier: 1, AppliedAt: appliedAt,
				},
				IdempotentReplay: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &channelCostUnitRebaserFake{result: tt.result}
			router := channelCostUnitRebaseTestRouter(t, fake)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, channelCostUnitRebaseRequest(
				`{"expected_version":17,"previous_multiplier":10,"current_multiplier":1}`,
				"response-shape",
			))

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
			}
			var response struct {
				Data costing.CostUnitRebaseResult `json:"data"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Data.Audit == nil || response.Data.Audit.ID != 7 || response.Data.Audit.ChannelID != 32 {
				t.Fatalf("audit response = %+v", response.Data.Audit)
			}
			if response.Data.IdempotentReplay != tt.result.IdempotentReplay {
				t.Fatalf("idempotent_replay = %v, want %v", response.Data.IdempotentReplay, tt.result.IdempotentReplay)
			}
			if tt.result.IdempotentReplay {
				if response.Data.State != nil {
					t.Fatalf("replay unexpectedly returned state: %+v", response.Data.State)
				}
			} else if response.Data.State == nil || response.Data.State.Version != 18 {
				t.Fatalf("applied state response = %+v", response.Data.State)
			}
		})
	}
}
