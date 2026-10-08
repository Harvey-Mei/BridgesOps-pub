package api

import (
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	gatewayservice "github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

func TestGatewayForwardPersistsUsagePresenceForFrozenPriceCalculation(t *testing.T) {
	tests := []struct {
		name             string
		contentType      string
		statusCode       int
		responseBody     string
		requestBody      string
		wantLogStatus    int
		wantInputTokens  int64
		wantOutputTokens int64
		wantComplete     bool
		wantInvalid      bool
		wantReportedCost *float64
	}{
		{
			name:             "non-streaming success",
			contentType:      "application/json",
			statusCode:       http.StatusOK,
			responseBody:     `{"id":"resp_non_stream","output":[],"usage":{"input_tokens":100,"output_tokens":20,"cache_read_tokens":0,"cache_write_tokens":0,"total_tokens":120}}`,
			requestBody:      `{"model":"gpt-test"}`,
			wantLogStatus:    http.StatusOK,
			wantInputTokens:  100,
			wantOutputTokens: 20,
			wantComplete:     true,
		},
		{
			name:        "streaming success",
			contentType: "text/event-stream",
			statusCode:  http.StatusOK,
			responseBody: "event: response.output_text.delta\n" +
				"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"delta\":\"ok\"}\n\n" +
				"event: response.completed\n" +
				"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_stream\",\"status\":\"completed\",\"usage\":{\"input_tokens\":80,\"output_tokens\":10,\"cache_read_tokens\":0,\"cache_write_tokens\":0,\"total_tokens\":90}}}\n\n",
			requestBody:      `{"model":"gpt-test","stream":true}`,
			wantLogStatus:    http.StatusOK,
			wantInputTokens:  80,
			wantOutputTokens: 10,
			wantComplete:     true,
		},
		{
			name:             "failed response with usage",
			contentType:      "application/json",
			statusCode:       http.StatusBadRequest,
			responseBody:     `{"error":{"type":"invalid_request_error","message":"request rejected"},"usage":{"input_tokens":40,"output_tokens":4,"cache_read_tokens":0,"cache_write_tokens":0,"total_tokens":44,"actual_cost":0.25}}`,
			requestBody:      `{"model":"gpt-test"}`,
			wantLogStatus:    http.StatusBadGateway,
			wantInputTokens:  40,
			wantOutputTokens: 4,
			wantComplete:     true,
			wantReportedCost: numberPointer(0.25),
		},
		{
			name:             "failed response with impossible cache tuple",
			contentType:      "application/json",
			statusCode:       http.StatusBadRequest,
			responseBody:     `{"error":{"type":"invalid_request_error","message":"request rejected"},"usage":{"input_tokens":10,"output_tokens":2,"cache_read_tokens":20,"cache_write_tokens":0,"total_tokens":12}}`,
			requestBody:      `{"model":"gpt-test"}`,
			wantLogStatus:    http.StatusBadGateway,
			wantInputTokens:  10,
			wantOutputTokens: 2,
			wantComplete:     true,
			wantInvalid:      true,
		},
		{
			name:             "success with omitted cache components",
			contentType:      "application/json",
			statusCode:       http.StatusOK,
			responseBody:     `{"id":"resp_incomplete","output":[],"usage":{"input_tokens":60,"output_tokens":6,"total_tokens":66}}`,
			requestBody:      `{"model":"gpt-test"}`,
			wantLogStatus:    http.StatusOK,
			wantInputTokens:  60,
			wantOutputTokens: 6,
		},
	}

	gin.SetMode(gin.TestMode)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", test.contentType)
				writer.WriteHeader(test.statusCode)
				_, _ = writer.Write([]byte(test.responseBody))
			}))
			defer upstream.Close()

			db := openTestDB(t)
			service, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
				channelID: 1, name: "priced", groupName: "priced", siteURL: upstream.URL, ratio: 1,
			}})
			decision.SmartRouting = false
			decision.Candidates[0].FundingFactor = 1
			if _, err := store.CreateModelPrice(storage.GatewayModelPriceInput{
				ModelName: "gpt-test", InputUnitPriceUSD: 5, OutputUnitPriceUSD: 30,
				CacheReadUnitPriceUSD: numberPointer(0.5), CacheWriteUnitPriceUSD: numberPointer(6.25), Enabled: true,
			}, "pricing-presence-test"); err != nil {
				t.Fatalf("create model price: %v", err)
			}

			recorder := runGatewayRetryTestRequestWithMode(service, decision, "/v1/responses", test.requestBody, false)
			if recorder.Code != test.wantLogStatus {
				t.Fatalf("gateway response status = %d body=%q, want %d", recorder.Code, recorder.Body.String(), test.wantLogStatus)
			}

			var log storage.GatewayRequestLog
			if err := db.Order("id DESC").First(&log).Error; err != nil {
				t.Fatalf("load request log: %v", err)
			}
			if log.StatusCode != test.wantLogStatus || log.PromptTokens != test.wantInputTokens || log.CompletionTokens != test.wantOutputTokens ||
				log.CacheReadTokens != 0 || log.CacheCreationTokens != 0 {
				if !test.wantInvalid || log.CacheReadTokens != 20 || log.CacheCreationTokens != 0 {
					t.Fatalf("persisted token usage = %+v", log)
				}
			}
			if log.TotalTokens != test.wantInputTokens+test.wantOutputTokens {
				t.Fatalf("persisted total tokens = %d, want %d: %+v", log.TotalTokens, test.wantInputTokens+test.wantOutputTokens, log)
			}
			if !log.UsageInputPresent || !log.UsageOutputPresent {
				t.Fatalf("persisted primary token presence is incomplete: %+v", log)
			}
			if test.wantInvalid {
				if log.CostStatus != storage.GatewayCostStatusUnavailable || log.CostUnavailableReason != storage.GatewayPricingUnavailableUsageInvalid ||
					log.Cost != 0 || log.RawCost != nil || log.CostEstimated || log.CostSource != "" {
					t.Fatalf("invalid terminal usage produced a price: %+v", log)
				}
			} else if test.wantReportedCost != nil {
				if log.CostStatus != storage.GatewayCostStatusReported || log.CostEstimated || log.CostSource != "actual_cost" ||
					math.Abs(log.Cost-*test.wantReportedCost) > 1e-12 || log.RawCost == nil || math.Abs(*log.RawCost-*test.wantReportedCost) > 1e-12 {
					t.Fatalf("reported terminal cost = %+v, want %g", log, *test.wantReportedCost)
				}
			} else if test.wantComplete {
				if !log.UsageCacheReadPresent || !log.UsageCacheWritePresent {
					t.Fatalf("persisted cache component presence is incomplete: %+v", log)
				}
				wantCost := (float64(test.wantInputTokens)*5 + float64(test.wantOutputTokens)*30) / 1_000_000
				if log.PriceFactID == nil || log.CostStatus != storage.GatewayCostStatusEstimated || !log.CostEstimated ||
					log.CostSource != "price_fact" || math.Abs(log.Cost-wantCost) > 1e-12 {
					t.Fatalf("frozen price calculation = fact:%v status:%q estimated:%t source:%q cost:%g, want %g",
						log.PriceFactID, log.CostStatus, log.CostEstimated, log.CostSource, log.Cost, wantCost)
				}
			} else if log.PriceFactID == nil || log.UsageCacheReadPresent || log.UsageCacheWritePresent ||
				log.CostStatus != storage.GatewayCostStatusUnavailable || log.CostEstimated || log.Cost != 0 ||
				log.CostSource != "" || log.CostUnavailableReason != storage.GatewayPricingUnavailableUsageIncomplete {
				t.Fatalf("incomplete cache usage produced a precise estimate: %+v", log)
			}
		})
	}
}

func TestGatewayRequestDispatchSnapshotFreezesPricingIdentity(t *testing.T) {
	tests := []struct {
		name                  string
		path                  string
		body                  string
		wantOperation         string
		wantProtocol          string
		wantUsageSchema       string
		wantOperationUnitsOne bool
	}{
		{
			name:                  "images generation",
			path:                  "/v1/images/generations",
			body:                  `{"model":"gpt-image-test"}`,
			wantOperation:         storage.GatewayImageOperationGeneration,
			wantProtocol:          storage.GatewayImageProtocolImages,
			wantUsageSchema:       storage.GatewayPricingUsageSchemaOperationV1,
			wantOperationUnitsOne: true,
		},
		{
			name:                  "responses image generation",
			path:                  "/v1/responses",
			body:                  `{"model":"gpt-test","tools":[{"type":"image_generation"}]}`,
			wantOperation:         storage.GatewayImageOperationGeneration,
			wantProtocol:          storage.GatewayImageProtocolResponses,
			wantUsageSchema:       storage.GatewayPricingUsageSchemaOperationV1,
			wantOperationUnitsOne: true,
		},
		{
			name:            "responses text generation",
			path:            "/v1/responses",
			body:            `{"model":"gpt-test","input":"hello"}`,
			wantOperation:   storage.GatewayPricingOperationTextGeneration,
			wantProtocol:    storage.GatewayPricingProtocolOpenAIResponses,
			wantUsageSchema: storage.GatewayPricingUsageSchemaTokensV1,
		},
		{
			name:            "chat completions",
			path:            "/v1/chat/completions",
			body:            `{"model":"gpt-test","messages":[]}`,
			wantOperation:   storage.GatewayPricingOperationTextGeneration,
			wantProtocol:    storage.GatewayPricingProtocolOpenAIChatCompletions,
			wantUsageSchema: storage.GatewayPricingUsageSchemaTokensV1,
		},
		{
			name:            "legacy completions",
			path:            "/v1/completions",
			body:            `{"model":"gpt-test","prompt":"hello"}`,
			wantOperation:   storage.GatewayPricingOperationTextGeneration,
			wantProtocol:    storage.GatewayPricingProtocolOpenAICompletions,
			wantUsageSchema: storage.GatewayPricingUsageSchemaTokensV1,
		},
		{
			name:            "anthropic messages",
			path:            "/v1/messages",
			body:            `{"model":"claude-test","messages":[]}`,
			wantOperation:   storage.GatewayPricingOperationTextGeneration,
			wantProtocol:    storage.GatewayPricingProtocolAnthropicMessages,
			wantUsageSchema: storage.GatewayPricingUsageSchemaTokensV1,
		},
		{
			name:            "embeddings",
			path:            "/v1/embeddings",
			body:            `{"model":"embedding-test","input":"hello"}`,
			wantOperation:   storage.GatewayPricingOperationEmbedding,
			wantProtocol:    storage.GatewayPricingProtocolOpenAIEmbeddings,
			wantUsageSchema: storage.GatewayPricingUsageSchemaInputTokensV1,
		},
	}

	gin.SetMode(gin.TestMode)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context, _ := gin.CreateTestContext(httptest.NewRecorder())
			context.Request = httptest.NewRequest(http.MethodPost, test.path, nil)
			mapping := newGatewayModelMappingResult("gpt-test", "", []byte(test.body))
			route := gatewayRouteSnapshot{
				Candidate: gatewayservice.Candidate{
					Channel: storage.Channel{ID: 11, Name: "channel", UpstreamAccountIdentityRevision: 7},
					Rate: storage.RateSnapshot{
						ID:              21,
						ChannelID:       11,
						ModelName:       "primary",
						Ratio:           1,
						CompletionRatio: 1,
					},
					UpstreamGeneration: storage.UpstreamGroupGeneration{ID: 31, ChannelID: 11, CanonicalGroupName: "primary", PresenceGeneration: 5},
				},
				ChannelID:               11,
				ChannelName:             "channel",
				GroupName:               "primary",
				ChannelIdentityRevision: 7,
				UpstreamGroupID:         31,
				UpstreamGroupGeneration: 5,
				UpstreamKeyID:           44,
				UpstreamKeyName:         "bridgeops-primary",
				UpstreamKeyLocalID:      61,
				UpstreamKeyGeneration:   3,
			}

			snapshot, err := gatewayRequestDispatchSnapshot(
				context,
				7,
				nil,
				route,
				"gpt-test",
				"",
				mapping,
				time.Unix(1_700_000_000, 0).UTC(),
				nil,
				nil,
				nil,
				0,
			)
			if err != nil {
				t.Fatalf("gatewayRequestDispatchSnapshot: %v", err)
			}
			if snapshot.PriceOperation != test.wantOperation || snapshot.PriceProtocol != test.wantProtocol || snapshot.UsageSchema != test.wantUsageSchema {
				t.Fatalf(
					"pricing identity = %q/%q/%q, want %q/%q/%q",
					snapshot.PriceOperation,
					snapshot.PriceProtocol,
					snapshot.UsageSchema,
					test.wantOperation,
					test.wantProtocol,
					test.wantUsageSchema,
				)
			}
			if snapshot.UsageSchemaVersion != 1 {
				t.Fatalf("usage schema version = %d, want 1", snapshot.UsageSchemaVersion)
			}
			if snapshot.ChannelIdentityRevision != 7 || snapshot.UpstreamGroupID != 31 || snapshot.UpstreamGroupGeneration != 5 ||
				snapshot.UpstreamKeyLocalID != 61 || snapshot.UpstreamKeyGeneration != 3 {
				t.Fatalf("selection provenance = %+v", snapshot)
			}
			if test.wantOperationUnitsOne {
				if !snapshot.UsageOperationPresent || snapshot.OperationUnits == nil || *snapshot.OperationUnits != 1 {
					t.Fatalf("operation usage = present:%t units:%v, want present with one unit", snapshot.UsageOperationPresent, snapshot.OperationUnits)
				}
			} else if snapshot.UsageOperationPresent || snapshot.OperationUnits != nil {
				t.Fatalf("operation usage = present:%t units:%v, want absent", snapshot.UsageOperationPresent, snapshot.OperationUnits)
			}
		})
	}
}

func TestPopulateGatewayRequestDispatchPricingSnapshotRejectsUnsupportedMethod(t *testing.T) {
	snapshot := storage.GatewayRequestDispatchSnapshot{Method: http.MethodGet, Path: "/v1/responses"}
	populateGatewayRequestDispatchPricingSnapshot(&snapshot, []byte(`{"model":"gpt-test","input":"hello"}`))
	if snapshot.PriceOperation != "" || snapshot.PriceProtocol != "" || snapshot.UsageSchema != "" || snapshot.UsageSchemaVersion != 0 {
		t.Fatalf("non-POST pricing identity was populated: %+v", snapshot)
	}
}

func TestGatewayRouteMultiplierProvenanceRequiresPersistedRateAndCompleteTuple(t *testing.T) {
	generation := storage.UpstreamGroupGeneration{
		ID: 31, ChannelID: 11, CanonicalGroupName: "primary", PresenceGeneration: 5,
	}
	realZeroRate := gatewayservice.Candidate{
		Channel: storage.Channel{ID: 11, Name: "channel", UpstreamAccountIdentityRevision: 7},
		Rate: storage.RateSnapshot{
			ID: 21, ChannelID: 11, ModelName: "primary", Ratio: 0, CompletionRatio: 0,
		},
		UpstreamGeneration: generation,
		RawRatio:           0,
		RawCompletionRatio: 0,
		FundingFactor:      1,
		CostBasisVersion:   9,
		CostBasisSource:    "default",
	}
	withLeaseKey := func(route gatewayRouteSnapshot) gatewayRouteSnapshot {
		route.UpstreamKeyID = 44
		route.UpstreamKeyName = "bridgeops-primary"
		route.UpstreamKeyLocalID = 61
		route.UpstreamKeyGeneration = 3
		return route
	}
	requestSnapshot := func(route gatewayRouteSnapshot) storage.GatewayRequestDispatchSnapshot {
		context, _ := gin.CreateTestContext(httptest.NewRecorder())
		context.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		snapshot, err := gatewayRequestDispatchSnapshot(
			context, 7, nil, route, "gpt-test", "",
			newGatewayModelMappingResult("gpt-test", "", []byte(`{"model":"gpt-test","input":"hello"}`)),
			time.Unix(1_700_000_000, 0).UTC(), nil, nil, nil, 0,
		)
		if err != nil {
			t.Fatalf("gatewayRequestDispatchSnapshot: %v", err)
		}
		return snapshot
	}

	t.Run("fixed route without rate does not borrow side-effect generations", func(t *testing.T) {
		noRate := realZeroRate
		noRate.Rate.ID = 0
		noRate.UpstreamGeneration = storage.UpstreamGroupGeneration{}
		noRate.ModelFactGeneration = generation
		noRate.KeyStateGeneration = generation
		route := withLeaseKey(gatewayRouteSnapshotForCandidate(noRate))
		if route.UpstreamGroupID != 0 || route.UpstreamGroupGeneration != 0 || gatewayRouteSelectionGeneration(noRate).Valid() {
			t.Fatalf("no-rate route borrowed a non-rate generation: %+v", route)
		}
		if _, complete := route.multiplierProvenance(); complete {
			t.Fatal("no-rate route produced complete multiplier provenance")
		}
		log := &storage.GatewayRequestLog{
			Cost: 1.25, RawCost: numberPointer(2.5), CostEstimated: true, CostSource: "sub2api_actual_cost",
		}
		route.applyToLog(log)
		assertGatewayScopeIncompletePricing(t, log)
		snapshot := requestSnapshot(route)
		if snapshot.GroupRatio != 0 || snapshot.CompletionRatio != 0 || snapshot.RawGroupRatio != nil ||
			snapshot.RawCompletionRatio != nil || snapshot.FundingFactor != nil || snapshot.CostBasisVersion != 0 ||
			snapshot.CostBasisSource != "" || snapshot.ChannelIdentityRevision != 0 || snapshot.UpstreamGroupID != 0 ||
			snapshot.UpstreamGroupGeneration != 0 || snapshot.UpstreamKeyLocalID != 0 || snapshot.UpstreamKeyGeneration != 0 {
			t.Fatalf("no-rate dispatch snapshot retained multiplier provenance: %+v", snapshot)
		}
	})

	t.Run("persisted zero multiplier remains a trusted fact", func(t *testing.T) {
		route := withLeaseKey(gatewayRouteSnapshotForCandidate(realZeroRate))
		provenance, complete := route.multiplierProvenance()
		if !complete || provenance.GroupRatio != 0 || provenance.CompletionRatio != 0 ||
			provenance.RawGroupRatio == nil || *provenance.RawGroupRatio != 0 ||
			provenance.RawCompletionRatio == nil || *provenance.RawCompletionRatio != 0 ||
			provenance.FundingFactor == nil || *provenance.FundingFactor != 1 {
			t.Fatalf("persisted 0x provenance = %+v complete=%t", provenance, complete)
		}
		log := &storage.GatewayRequestLog{}
		route.applyToLog(log)
		if log.CostUnavailableReason != "" || log.CostStatus == storage.GatewayCostStatusUnavailable ||
			log.ChannelIdentityRevision != 7 || log.UpstreamGroupID != 31 || log.UpstreamGroupGeneration != 5 ||
			log.UpstreamKeyLocalID != 61 || log.UpstreamKeyGeneration != 3 || log.RawGroupRatio == nil ||
			*log.RawGroupRatio != 0 || log.RawCompletionRatio == nil || *log.RawCompletionRatio != 0 {
			t.Fatalf("persisted 0x log projection was fenced: %+v", log)
		}
		snapshot := requestSnapshot(route)
		if snapshot.ChannelIdentityRevision != 7 || snapshot.UpstreamGroupID != 31 || snapshot.UpstreamGroupGeneration != 5 ||
			snapshot.UpstreamKeyLocalID != 61 || snapshot.UpstreamKeyGeneration != 3 || snapshot.RawGroupRatio == nil ||
			*snapshot.RawGroupRatio != 0 || snapshot.RawCompletionRatio == nil || *snapshot.RawCompletionRatio != 0 {
			t.Fatalf("persisted 0x dispatch snapshot lost provenance: %+v", snapshot)
		}
	})
}

func TestGatewayDirectTerminalLogsBeforeLeaseArePricingFenced(t *testing.T) {
	candidate := gatewayservice.Candidate{
		Channel: storage.Channel{ID: 11, Name: "channel", UpstreamAccountIdentityRevision: 7},
		Rate: storage.RateSnapshot{
			ID: 21, ChannelID: 11, ModelName: "primary", Ratio: 2, CompletionRatio: 3,
		},
		UpstreamGeneration: storage.UpstreamGroupGeneration{
			ID: 31, ChannelID: 11, CanonicalGroupName: "primary", PresenceGeneration: 5,
		},
		RawRatio: 4, RawCompletionRatio: 6, FundingFactor: 0.5,
		CostBasisVersion: 9, CostBasisSource: "recharge_history",
	}
	routeBeforeLease := gatewayRouteSnapshotForCandidate(candidate)
	start := time.Unix(1_700_000_000, 0).UTC()

	t.Run("client cancellation while acquiring lease", func(t *testing.T) {
		log := gatewayClientCanceledRequestLog(
			7, &gatewayRouteDecision{SmartRouting: false, Candidates: []gatewayservice.Candidate{candidate}}, routeBeforeLease,
			"gpt-test", "", gatewayModelMappingResult{Model: "gpt-test"}, false, "", "", http.MethodPost,
			"/v1/responses", start, start.Add(time.Second),
		)
		assertGatewayScopeIncompletePricing(t, log)
	})

	t.Run("lease failure terminal log", func(t *testing.T) {
		log := &storage.GatewayRequestLog{
			Cost: 1.25, RawCost: numberPointer(2.5), CostEstimated: true, CostSource: "sub2api_actual_cost",
		}
		routeBeforeLease.applyToLog(log)
		assertGatewayScopeIncompletePricing(t, log)
	})
}

func TestGatewayPreLeaseDirectTerminalFenceSurvivesFundingCorrections(t *testing.T) {
	for _, correction := range []string{"append funding event", "confirm funding candidate"} {
		t.Run(correction, func(t *testing.T) {
			db := openTestDB(t)
			_, store, decision := setupGatewayForwardWithCachedCandidatesOnDB(t, db, []gatewayForwardTestCandidate{{
				channelID: 1, name: "manual", groupName: "primary", siteURL: "https://primary.example", ratio: 2,
			}})
			routeBeforeLease := gatewayRouteSnapshotForCandidate(decision.Candidates[0])
			effectiveAt := time.Now().UTC().Add(-time.Minute)
			log := gatewayClientCanceledRequestLog(
				7, decision, routeBeforeLease, "gpt-test", "", gatewayModelMappingResult{Model: "gpt-test"}, false,
				"", "", http.MethodPost, "/v1/responses", effectiveAt, effectiveAt.Add(time.Second),
			)
			log.CreatedAt = effectiveAt.Add(time.Second)
			assertGatewayScopeIncompletePricing(t, log)
			if err := store.AppendRequestLog(log); err != nil {
				t.Fatalf("append pre-lease terminal log: %v", err)
			}

			repo := storage.NewCosting(db)
			state, err := repo.FindState(1)
			if err != nil {
				t.Fatalf("load channel cost state: %v", err)
			}
			next := *state
			next.FundingFactor = 2
			next.Source = "funding_correction"
			event := &storage.ChannelFundingEvent{
				ChannelID: 1, Kind: "funding_correction", FundingFactorBefore: state.FundingFactor,
				FundingFactorAfter: next.FundingFactor, EffectiveAt: effectiveAt,
			}
			switch correction {
			case "append funding event":
				err = repo.AppendFundingEvent(&next, event, state.Version)
			case "confirm funding candidate":
				candidate := &storage.ChannelFundingCandidate{
					ChannelID: 1, Source: "balance_increase", ExternalID: "pre-lease-direct-terminal",
					Status: storage.FundingCandidatePending, CreditedAmount: 10, RawBalanceDetected: 10,
					FundingFactorBefore: state.FundingFactor, DetectedAt: effectiveAt.Add(-time.Second),
				}
				if createErr := db.Create(candidate).Error; createErr != nil {
					t.Fatalf("create funding candidate: %v", createErr)
				}
				err = repo.ConfirmFundingCandidate(&next, event, candidate.ID, state.Version)
			}
			if err != nil {
				t.Fatalf("apply %s: %v", correction, err)
			}
			var stored storage.GatewayRequestLog
			if err := db.First(&stored, log.ID).Error; err != nil {
				t.Fatalf("reload pre-lease terminal log: %v", err)
			}
			assertGatewayScopeIncompletePricing(t, &stored)
		})
	}
}

func assertGatewayScopeIncompletePricing(t *testing.T, log *storage.GatewayRequestLog) {
	t.Helper()
	if log == nil || log.GroupRatio != 0 || log.CompletionRatio != 0 || log.RawGroupRatio != nil ||
		log.RawCompletionRatio != nil || log.FundingFactor != nil || log.CostBasisVersion != 0 || log.CostBasisSource != "" ||
		log.ChannelIdentityRevision != 0 || log.UpstreamGroupID != 0 || log.UpstreamGroupGeneration != 0 ||
		log.UpstreamKeyLocalID != 0 || log.UpstreamKeyGeneration != 0 || log.Cost != 0 || log.RawCost != nil ||
		log.CostEstimated || log.CostSource != "" || log.CostStatus != storage.GatewayCostStatusUnavailable ||
		log.CostUnavailableReason != storage.GatewayPricingUnavailableScopeIncomplete {
		t.Fatalf("request log retained incomplete pricing provenance: %+v", log)
	}
}

func TestGatewayRequestDispatchSnapshotLeaseIdentityFenceIsAllOrNothing(t *testing.T) {
	gin.SetMode(gin.TestMode)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	startedAt := time.Unix(1_700_000_000, 0).UTC()

	selected := gatewayservice.Candidate{
		Channel: storage.Channel{ID: 11, Name: "channel", UpstreamAccountIdentityRevision: 7},
		Rate: storage.RateSnapshot{
			ID: 21, ChannelID: 11, ModelName: "primary", Ratio: 2, CompletionRatio: 3,
		},
		UpstreamGeneration: storage.UpstreamGroupGeneration{
			ID: 31, ChannelID: 11, CanonicalGroupName: "primary", PresenceGeneration: 5,
		},
		RawRatio:           4,
		RawCompletionRatio: 6,
		FundingFactor:      0.5,
		CostBasisVersion:   9,
		CostBasisSource:    "recharge_history",
	}
	withLeaseKey := func(route gatewayRouteSnapshot) gatewayRouteSnapshot {
		route.UpstreamKeyID = 44
		route.UpstreamKeyName = "bridgeops-primary"
		route.UpstreamKeyLocalID = 61
		route.UpstreamKeyGeneration = 3
		return route
	}
	requestSnapshot := func(route gatewayRouteSnapshot) storage.GatewayRequestDispatchSnapshot {
		snapshot, err := gatewayRequestDispatchSnapshot(
			context, 7, nil, route, "gpt-test", "",
			newGatewayModelMappingResult("gpt-test", "", []byte(`{"model":"gpt-test","input":"hello"}`)),
			startedAt, nil, nil, nil, 0,
		)
		if err != nil {
			t.Fatalf("gatewayRequestDispatchSnapshot: %v", err)
		}
		return snapshot
	}

	unchanged := gatewayRouteSnapshotAfterLeaseChannel(gatewayRouteSnapshotForCandidate(selected), selected)
	unchanged = withLeaseKey(unchanged)
	if unchanged.MultiplierProvenanceInvalidated {
		t.Fatal("unchanged lease identity invalidated multiplier provenance")
	}
	trusted := requestSnapshot(unchanged)
	if trusted.GroupRatio != 2 || trusted.CompletionRatio != 3 || trusted.RawGroupRatio == nil || *trusted.RawGroupRatio != 4 ||
		trusted.RawCompletionRatio == nil || *trusted.RawCompletionRatio != 6 || trusted.FundingFactor == nil || *trusted.FundingFactor != 0.5 ||
		trusted.CostBasisVersion != 9 || trusted.CostBasisSource != "recharge_history" || trusted.ChannelIdentityRevision != 7 ||
		trusted.UpstreamGroupID != 31 || trusted.UpstreamGroupGeneration != 5 || trusted.UpstreamKeyLocalID != 61 || trusted.UpstreamKeyGeneration != 3 {
		t.Fatalf("unchanged lease lost frozen provenance: %+v", trusted)
	}

	discovered := selected
	discovered.Channel.UpstreamAccountIdentityRevision = 8
	invalidated := gatewayRouteSnapshotAfterLeaseChannel(gatewayRouteSnapshotForCandidate(selected), discovered)
	invalidated = withLeaseKey(invalidated)
	if !invalidated.MultiplierProvenanceInvalidated || invalidated.Candidate.Channel.UpstreamAccountIdentityRevision != 8 {
		t.Fatalf("identity-changing lease route = %+v", invalidated)
	}
	projected := &storage.GatewayRequestLog{
		Cost: 1.25, RawCost: numberPointer(2.5), CostEstimated: true, CostSource: "sub2api_actual_cost",
	}
	invalidated.applyToLog(projected)
	if projected.GroupRatio != 0 || projected.CompletionRatio != 0 || projected.RawGroupRatio != nil || projected.RawCompletionRatio != nil ||
		projected.FundingFactor != nil || projected.CostBasisVersion != 0 || projected.CostBasisSource != "" ||
		projected.ChannelIdentityRevision != 0 || projected.UpstreamGroupID != 0 || projected.UpstreamGroupGeneration != 0 ||
		projected.UpstreamKeyLocalID != 0 || projected.UpstreamKeyGeneration != 0 || projected.Cost != 0 || projected.RawCost != nil ||
		projected.CostEstimated || projected.CostSource != "" || projected.CostStatus != storage.GatewayCostStatusUnavailable ||
		projected.CostUnavailableReason != storage.GatewayPricingUnavailableScopeIncomplete {
		t.Fatalf("identity-changing lease log projection retained pricing provenance: %+v", projected)
	}

	// A same-candidate confirmation retry starts from the post-discovery channel
	// snapshot. The caller's sticky state must keep its multiplier provenance
	// unavailable rather than treating the old rate/funding basis as newly
	// selected under revision 8.
	retry := gatewayRouteSnapshotForCandidate(discovered)
	candidateMultiplierProvenanceInvalidated := invalidated.MultiplierProvenanceInvalidated
	retry.MultiplierProvenanceInvalidated = candidateMultiplierProvenanceInvalidated
	retry = gatewayRouteSnapshotAfterLeaseChannel(retry, discovered)
	candidateMultiplierProvenanceInvalidated = candidateMultiplierProvenanceInvalidated || retry.MultiplierProvenanceInvalidated
	if !retry.MultiplierProvenanceInvalidated || !candidateMultiplierProvenanceInvalidated {
		t.Fatal("same-identity retry cleared sticky multiplier provenance invalidation")
	}
	retry = withLeaseKey(retry)
	unavailable := requestSnapshot(retry)
	if unavailable.GroupRatio != 0 || unavailable.CompletionRatio != 0 || unavailable.RawGroupRatio != nil ||
		unavailable.RawCompletionRatio != nil || unavailable.FundingFactor != nil || unavailable.CostBasisVersion != 0 ||
		unavailable.CostBasisSource != "" || unavailable.ChannelIdentityRevision != 0 || unavailable.UpstreamGroupID != 0 ||
		unavailable.UpstreamGroupGeneration != 0 || unavailable.UpstreamKeyLocalID != 0 || unavailable.UpstreamKeyGeneration != 0 {
		t.Fatalf("identity-changing lease retained partial multiplier provenance: %+v", unavailable)
	}
}
