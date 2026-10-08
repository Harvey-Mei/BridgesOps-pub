package monitor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/connector"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

type modelPricingProviderFunc func(context.Context, *connector.Channel, *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error)

func (fn modelPricingProviderFunc) GetModelPrices(ctx context.Context, channel *connector.Channel, session *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
	return fn(ctx, channel, session)
}

func TestRefreshRatesCommitsProviderModelPricesAfterRateSnapshot(t *testing.T) {
	var harness *monitorTestHarness
	var channelRecord *storage.Channel
	var pricingCalls atomic.Int32
	var rateCommittedBeforePricing atomic.Bool
	var authenticatedPricingSession atomic.Bool

	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/user/self":
			_, _ = writer.Write([]byte(`{"success":true,"data":{"id":7}}`))
		case "/api/user/self/groups":
			if request.Header.Get("Cookie") == "session=pricing" && request.Header.Get("New-Api-User") == "7" {
				authenticatedPricingSession.Store(true)
			}
			_, _ = writer.Write([]byte(`{"success":true,"data":{"default":{"ratio":1,"desc":"default"}}}`))
		case "/api/status":
			if harness != nil && channelRecord != nil {
				items, err := harness.rates.ListByChannel(channelRecord.ID)
				if err == nil && len(items) == 1 && items[0].ModelName == "default" {
					rateCommittedBeforePricing.Store(true)
				}
			}
			_, _ = writer.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
		case "/api/pricing":
			pricingCalls.Add(1)
			if request.Header.Get("Cookie") == "session=pricing" && request.Header.Get("New-Api-User") == "7" {
				authenticatedPricingSession.Store(true)
			}
			_, _ = writer.Write([]byte(`{
				"success":true,
				"pricing_version":"price-v1",
				"usable_group":{"default":"default"},
				"group_ratio":{"default":1},
				"data":[{
					"model_name":"gpt-5.6-sol","quota_type":0,"model_ratio":2.5,
					"completion_ratio":6,"cache_ratio":0.1,"create_cache_ratio":1.25,
					"enable_groups":["default"]
				}]
			}`))
		default:
			http.Error(writer, request.URL.Path, http.StatusNotFound)
		}
	}))
	defer upstream.Close()

	channelRecord = &storage.Channel{
		Name: "provider-pricing", Type: storage.ChannelTypeNewAPI, SiteURL: upstream.URL,
		CredentialMode: storage.CredentialModeToken, MonitorEnabled: true, IgnoreAnnouncements: true,
	}
	harness = newMonitorTestHarness(t, channelRecord)
	setMonitorCredential(t, harness, channelRecord, `{"cookie":"session=pricing","user_id":"7"}`)
	seedMonitorModelPriceGroup(t, harness, channelRecord, "default", 1)

	if err := harness.service.RefreshRates(context.Background(), channelRecord); err != nil {
		t.Fatalf("RefreshRates: %v", err)
	}
	if pricingCalls.Load() != 1 || !rateCommittedBeforePricing.Load() || !authenticatedPricingSession.Load() {
		t.Fatalf("pricing calls=%d rate committed=%t authenticated=%t", pricingCalls.Load(), rateCommittedBeforePricing.Load(), authenticatedPricingSession.Load())
	}

	var price storage.GatewayChannelModelPrice
	if err := harness.db.Where("channel_id = ? AND upstream_group_name = ? AND model_name = ?", channelRecord.ID, "default", "gpt-5.6-sol").Take(&price).Error; err != nil {
		t.Fatalf("load committed model price: %v", err)
	}
	if price.ChannelIdentityRevision != channelRecord.UpstreamAccountIdentityRevision ||
		price.CostUnitEpoch != channelRecord.CostUnitEpoch || price.UpstreamGroupName != "default" || price.GroupRatio != 1 ||
		price.InputUnitPriceUSD != 5 || price.OutputUnitPriceUSD != 30 ||
		price.CacheReadUnitPriceUSD == nil || *price.CacheReadUnitPriceUSD != 0.5 ||
		price.CacheWriteUnitPriceUSD == nil || *price.CacheWriteUnitPriceUSD != 6.25 ||
		price.SourceVersion != "price-v1" || len(price.SourceChecksum) != 64 {
		t.Fatalf("committed model price = %+v", price)
	}
	storedChannel, err := harness.channels.FindByID(channelRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedChannel.ModelPriceObservedAt == nil || !storedChannel.ModelPriceObservedAt.Equal(price.SourceObservedAt) {
		t.Fatalf("model price watermark=%v source observed=%v", storedChannel.ModelPriceObservedAt, price.SourceObservedAt)
	}
}

func TestRefreshChannelModelPricesFailureIsolationAndCAS(t *testing.T) {
	tests := []struct {
		name           string
		provider       func(time.Time) modelPricingProviderFunc
		identityOffset uint64
		epochOffset    uint64
		cancel         bool
		wantError      error
	}{
		{
			name: "unsupported",
			provider: func(time.Time) modelPricingProviderFunc {
				return func(context.Context, *connector.Channel, *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
					return nil, connector.ErrUnsupported
				}
			},
		},
		{
			name: "ordinary provider failure",
			provider: func(time.Time) modelPricingProviderFunc {
				return func(context.Context, *connector.Channel, *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
					return nil, errors.New("temporary pricing failure")
				}
			},
		},
		{
			name: "incomplete snapshot",
			provider: func(time.Time) modelPricingProviderFunc {
				return func(context.Context, *connector.Channel, *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
					return &connector.ModelPriceSnapshotResult{}, nil
				}
			},
		},
		{
			name: "older complete snapshot",
			provider: func(seedAt time.Time) modelPricingProviderFunc {
				return func(context.Context, *connector.Channel, *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
					return monitorModelPriceSnapshot(seedAt.Add(-time.Second), "older"), nil
				}
			},
		},
		{
			name: "context canceled",
			provider: func(time.Time) modelPricingProviderFunc {
				return func(ctx context.Context, _ *connector.Channel, _ *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
					return nil, ctx.Err()
				}
			},
			cancel: true, wantError: context.Canceled,
		},
		{
			name: "identity revision stale",
			provider: func(seedAt time.Time) modelPricingProviderFunc {
				return func(context.Context, *connector.Channel, *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
					return monitorModelPriceSnapshot(seedAt.Add(time.Second), "newer"), nil
				}
			},
			identityOffset: 1, wantError: storage.ErrStaleChannelAccountIdentityRevision,
		},
		{
			name: "cost unit epoch stale",
			provider: func(seedAt time.Time) modelPricingProviderFunc {
				return func(context.Context, *connector.Channel, *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
					return monitorModelPriceSnapshot(seedAt.Add(time.Second), "newer"), nil
				}
			},
			epochOffset: 1, wantError: storage.ErrCostUnitEpochConflict,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			channelRecord := &storage.Channel{
				Name: "price-errors", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid",
				CredentialMode: storage.CredentialModeToken,
			}
			harness := newMonitorTestHarness(t, channelRecord)
			harness.service.log = slog.New(slog.NewTextHandler(io.Discard, nil))
			current, err := harness.channels.FindByID(channelRecord.ID)
			if err != nil {
				t.Fatal(err)
			}
			seedMonitorModelPriceGroup(t, harness, current, "default", 1)
			seedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
			seed := monitorModelPriceSnapshot(seedAt, "trusted")
			if err := harness.rates.ReplaceChannelModelPriceSnapshot(context.Background(), storage.GatewayChannelModelPriceSnapshotInput{
				ChannelID: current.ID, ExpectedIdentityRevision: current.UpstreamAccountIdentityRevision,
				ExpectedCostUnitEpoch: current.CostUnitEpoch, SourceVersion: seed.SourceVersion,
				SourceChecksum: seed.SourceChecksum, ObservedAt: seed.ObservedAt,
				Items: monitorModelPriceStorageItems(seed.Items),
			}); err != nil {
				t.Fatalf("seed trusted price: %v", err)
			}

			ctx := context.Background()
			if test.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			resolved := &connector.Channel{ID: current.ID, Type: connector.TypeNewAPI, SiteURL: current.SiteURL}
			session := &connector.AuthSession{UserID: "7", Cookie: "session=trusted"}
			err = harness.service.refreshChannelModelPrices(
				ctx, current, resolved, session, test.provider(seedAt),
				current.UpstreamAccountIdentityRevision+test.identityOffset,
				current.CostUnitEpoch+test.epochOffset,
			)
			if test.wantError == nil && err != nil {
				t.Fatalf("refreshChannelModelPrices error = %v", err)
			}
			if test.wantError != nil && !errors.Is(err, test.wantError) {
				t.Fatalf("refreshChannelModelPrices error = %v, want %v", err, test.wantError)
			}

			var preserved storage.GatewayChannelModelPrice
			if findErr := harness.db.Where("channel_id = ? AND upstream_group_name = ? AND model_name = ?", current.ID, "default", "gpt-test").Take(&preserved).Error; findErr != nil {
				t.Fatalf("load preserved trusted price: %v", findErr)
			}
			if preserved.SourceVersion != "trusted" || !preserved.SourceObservedAt.Equal(seedAt) || preserved.InputUnitPriceUSD != 4 {
				t.Fatalf("trusted price changed after optional sync failure: %+v", preserved)
			}
		})
	}
}

func TestRefreshChannelModelPricesPassesExactSession(t *testing.T) {
	channelRecord := &storage.Channel{
		Name: "same-session", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid",
		CredentialMode: storage.CredentialModeToken,
	}
	harness := newMonitorTestHarness(t, channelRecord)
	current, err := harness.channels.FindByID(channelRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	seedMonitorModelPriceGroup(t, harness, current, "default", 1)
	resolved := &connector.Channel{ID: current.ID, Type: connector.TypeNewAPI, SiteURL: current.SiteURL}
	session := &connector.AuthSession{UserID: "7", Cookie: "session=exact"}
	observedAt := time.Now().UTC().Truncate(time.Microsecond)
	provider := modelPricingProviderFunc(func(_ context.Context, gotChannel *connector.Channel, gotSession *connector.AuthSession) (*connector.ModelPriceSnapshotResult, error) {
		if gotChannel != resolved || gotSession != session {
			t.Fatalf("pricing received channel/session %p/%p, want %p/%p", gotChannel, gotSession, resolved, session)
		}
		return monitorModelPriceSnapshot(observedAt, "exact-session"), nil
	})
	if err := harness.service.refreshChannelModelPrices(
		context.Background(), current, resolved, session, provider,
		current.UpstreamAccountIdentityRevision, current.CostUnitEpoch,
	); err != nil {
		t.Fatalf("refreshChannelModelPrices: %v", err)
	}
	var stored storage.GatewayChannelModelPrice
	if err := harness.db.Where("channel_id = ? AND upstream_group_name = ? AND model_name = ?", current.ID, "default", "gpt-test").Take(&stored).Error; err != nil {
		t.Fatalf("load exact-session price: %v", err)
	}
	if stored.LongContextBasis != "whole_request" || stored.LongContextThresholdTokens == nil || *stored.LongContextThresholdTokens != 272_000 ||
		stored.LongContextInputUnitPriceUSD == nil || *stored.LongContextInputUnitPriceUSD != 8 ||
		stored.LongContextOutputUnitPriceUSD == nil || *stored.LongContextOutputUnitPriceUSD != 30 ||
		stored.LongContextCacheReadUnitPriceUSD == nil || *stored.LongContextCacheReadUnitPriceUSD != 0.8 ||
		stored.LongContextCacheWriteUnitPriceUSD == nil || *stored.LongContextCacheWriteUnitPriceUSD != 10 {
		t.Fatalf("long-context price was not preserved: %+v", stored)
	}
}

func monitorModelPriceSnapshot(observedAt time.Time, version string) *connector.ModelPriceSnapshotResult {
	groupRatio, cacheRead, cacheWrite := 1.0, 0.4, 5.0
	threshold, longInput, longOutput, longCacheRead, longCacheWrite := int64(272_000), 8.0, 30.0, 0.8, 10.0
	return &connector.ModelPriceSnapshotResult{
		Complete: true, SourceVersion: version,
		SourceChecksum: strings.Repeat("a", 64), QuotaPerUnit: 500_000, ObservedAt: observedAt,
		Items: []connector.ModelPriceResult{{
			ModelName: "gpt-test", UpstreamGroupName: "default", GroupRatio: &groupRatio,
			InputUnitPriceUSD: 4, OutputUnitPriceUSD: 20,
			CacheReadUnitPriceUSD: &cacheRead, CacheWriteUnitPriceUSD: &cacheWrite,
			LongContextBasis: "whole_request", LongContextThresholdTokens: &threshold,
			LongContextInputUnitPriceUSD: &longInput, LongContextOutputUnitPriceUSD: &longOutput,
			LongContextCacheReadUnitPriceUSD: &longCacheRead, LongContextCacheWriteUnitPriceUSD: &longCacheWrite,
		}},
	}
}

func seedMonitorModelPriceGroup(t *testing.T, harness *monitorTestHarness, channelRecord *storage.Channel, groupName string, ratio float64) {
	t.Helper()
	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	if err := storage.NewGateway(harness.db).RecordUpstreamGroupChange(
		channelRecord.ID, groupName, "added", 0, "", observedAt,
	); err != nil {
		t.Fatalf("seed model price group generation: %v", err)
	}
	if _, err := harness.rates.Upsert(&storage.RateSnapshot{
		ChannelID: channelRecord.ID, ModelName: groupName, Ratio: ratio,
		CostUnitEpoch: channelRecord.CostUnitEpoch, LastSeenAt: observedAt,
	}); err != nil {
		t.Fatalf("seed model price group rate: %v", err)
	}
}

func monitorModelPriceStorageItems(items []connector.ModelPriceResult) []storage.GatewayChannelModelPriceSnapshotItem {
	converted := make([]storage.GatewayChannelModelPriceSnapshotItem, 0, len(items))
	for _, item := range items {
		converted = append(converted, storage.GatewayChannelModelPriceSnapshotItem{
			UpstreamGroupID: item.UpstreamGroupID, UpstreamGroupName: item.UpstreamGroupName,
			GroupRatio: item.GroupRatio, ModelName: item.ModelName,
			InputUnitPriceUSD: item.InputUnitPriceUSD, OutputUnitPriceUSD: item.OutputUnitPriceUSD,
			CacheReadUnitPriceUSD: item.CacheReadUnitPriceUSD, CacheWriteUnitPriceUSD: item.CacheWriteUnitPriceUSD,
			LongContextBasis: item.LongContextBasis, LongContextThresholdTokens: item.LongContextThresholdTokens,
			LongContextInputUnitPriceUSD:      item.LongContextInputUnitPriceUSD,
			LongContextOutputUnitPriceUSD:     item.LongContextOutputUnitPriceUSD,
			LongContextCacheReadUnitPriceUSD:  item.LongContextCacheReadUnitPriceUSD,
			LongContextCacheWriteUnitPriceUSD: item.LongContextCacheWriteUnitPriceUSD,
		})
	}
	return converted
}
