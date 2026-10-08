package modelprice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/modelpricing"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
)

const (
	defaultSyncInterval   = 24 * time.Hour
	defaultRequestTimeout = 20 * time.Second
	maxSyncModels         = 2000
)

type Result struct {
	SourceURL         string                                   `json:"source_url"`
	SourceChecksum    string                                   `json:"source_checksum,omitempty"`
	SourceObservedAt  time.Time                                `json:"source_observed_at,omitempty"`
	DiscoveredModels  int                                      `json:"discovered_models"`
	MatchedModels     int                                      `json:"matched_models"`
	UnsupportedModels []string                                 `json:"unsupported_models"`
	Applied           storage.GatewayAutomaticModelPriceResult `json:"applied"`
}

type modelPriceStore interface {
	ListUsedModels(context.Context, time.Time) ([]storage.GatewayUsedModel, error)
	ApplyAutomaticModelPrices(context.Context, []storage.GatewayAutomaticModelPriceInput, string) (storage.GatewayAutomaticModelPriceResult, error)
}

type Service struct {
	store    modelPriceStore
	client   *http.Client
	logger   *slog.Logger
	interval time.Duration
	mu       sync.Mutex
	now      func() time.Time
}

func New(store *storage.Gateway, logger *slog.Logger) *Service {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	client := &http.Client{
		Transport: transport,
		Timeout:   defaultRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &Service{store: store, client: client, logger: logger, interval: defaultSyncInterval, now: time.Now}
}

func automaticInput(candidate modelpricing.Candidate, sourceURL, checksum string, observedAt time.Time) (storage.GatewayAutomaticModelPriceInput, bool) {
	short := candidate.Short
	if short.InputUSDPerMillion == nil || short.OutputUSDPerMillion == nil {
		return storage.GatewayAutomaticModelPriceInput{}, false
	}
	input := storage.GatewayAutomaticModelPriceInput{
		ModelName:              candidate.ModelName,
		InputUnitPriceUSD:      *short.InputUSDPerMillion,
		OutputUnitPriceUSD:     *short.OutputUSDPerMillion,
		CacheReadUnitPriceUSD:  short.CacheReadUSDPerMillion,
		CacheWriteUnitPriceUSD: short.CacheWriteUSDPerMillion,
		Source:                 storage.GatewayModelPriceSourceLiteLLM,
		SourceURL:              sourceURL,
		SourceChecksum:         checksum,
		SourceObservedAt:       observedAt,
	}
	if candidate.HasAnyAbove272KPrice {
		above := candidate.Above272K
		if candidate.Above272KThresholdTokens != modelpricing.Above272KThresholdTokens ||
			above.InputUSDPerMillion == nil || above.OutputUSDPerMillion == nil {
			return storage.GatewayAutomaticModelPriceInput{}, false
		}
		threshold := candidate.Above272KThresholdTokens
		input.LongContextThresholdTokens = &threshold
		input.LongContextInputUnitPriceUSD = above.InputUSDPerMillion
		input.LongContextOutputUnitPriceUSD = above.OutputUSDPerMillion
		input.LongContextCacheReadUnitPriceUSD = above.CacheReadUSDPerMillion
		input.LongContextCacheWriteUnitPriceUSD = above.CacheWriteUSDPerMillion
	}
	return input, true
}

func (s *Service) SyncNow(ctx context.Context) (Result, error) {
	result := Result{SourceURL: modelpricing.LiteLLMSourceURL, UnsupportedModels: []string{}}
	if s == nil || s.store == nil {
		return result, errors.New("model price sync store is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.mu.TryLock() {
		return result, errors.New("model price sync is already running")
	}
	defer s.mu.Unlock()

	usedModels, err := s.store.ListUsedModels(ctx, s.now().UTC().Add(-30*24*time.Hour))
	if err != nil {
		return result, fmt.Errorf("list used models: %w", err)
	}
	modelSet := make(map[string]struct{}, len(usedModels))
	for _, item := range usedModels {
		name := strings.TrimSpace(item.ModelName)
		if name != "" && (item.Price == nil || item.Price.AutoUpdate) {
			modelSet[name] = struct{}{}
		}
	}
	if len(modelSet) > maxSyncModels {
		return result, fmt.Errorf("model price sync model limit exceeded: %d", len(modelSet))
	}
	result.DiscoveredModels = len(modelSet)
	if len(modelSet) == 0 {
		return result, nil
	}

	modelNames := make([]string, 0, len(modelSet))
	for modelName := range modelSet {
		modelNames = append(modelNames, modelName)
	}
	sort.Strings(modelNames)
	snapshot, err := modelpricing.FetchLiteLLM(ctx, s.client, modelNames)
	if err != nil {
		return result, err
	}
	observedAt := snapshot.ObservedAt.UTC().Truncate(time.Microsecond)
	result.SourceURL = snapshot.SourceURL
	result.SourceChecksum = snapshot.SHA256
	result.SourceObservedAt = observedAt
	inputs := make([]storage.GatewayAutomaticModelPriceInput, 0, len(modelNames))
	for _, modelName := range modelNames {
		candidate, exists := snapshot.Candidates[modelName]
		if !exists {
			result.UnsupportedModels = append(result.UnsupportedModels, modelName)
			continue
		}
		input, ok := automaticInput(candidate, snapshot.SourceURL, snapshot.SHA256, observedAt)
		if !ok {
			result.UnsupportedModels = append(result.UnsupportedModels, modelName)
			continue
		}
		inputs = append(inputs, input)
	}
	result.MatchedModels = len(inputs)
	applied, err := s.store.ApplyAutomaticModelPrices(ctx, inputs, "automatic-litellm-sync")
	if err != nil {
		return result, fmt.Errorf("apply automatic model prices: %w", err)
	}
	result.Applied = applied
	return result, nil
}

func (s *Service) Run(ctx context.Context) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	run := func() {
		result, err := s.SyncNow(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) && s.logger != nil {
				s.logger.Warn("automatic model price sync failed", "err", err)
			}
			return
		}
		if s.logger != nil {
			s.logger.Info("automatic model price sync completed",
				"discovered", result.DiscoveredModels, "matched", result.MatchedModels,
				"created", result.Applied.Created, "updated", result.Applied.Updated,
				"manual_locked", result.Applied.ManualLocked, "unsupported", len(result.UnsupportedModels))
		}
	}
	run()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}
