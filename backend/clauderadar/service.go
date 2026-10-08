// Package clauderadar fetches the public Claude Code Radar summary.
package clauderadar

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

const (
	DefaultURL                  = "https://claudecoderadar.com/data/claude-code-radar.json"
	DefaultHistoryRetentionDays = 90
	defaultTimeout              = 15 * time.Second
	maxPayloadSize              = 2 << 20
)

type Options struct {
	URL    string
	Client *http.Client
	Logger *slog.Logger
	Store  *storage.CodexRadarStore
}

type HistoryPoint struct {
	ObservedAt      time.Time `json:"observed_at"`
	ConfigKey       string    `json:"config_key"`
	Label           string    `json:"label,omitempty"`
	Model           string    `json:"model,omitempty"`
	ReasoningEffort string    `json:"reasoning_effort,omitempty"`
	Score           *float64  `json:"score,omitempty"`
	Status          string    `json:"status,omitempty"`
	Passed          *int      `json:"passed,omitempty"`
	Tasks           *int      `json:"tasks,omitempty"`
	ValidTasks      *int      `json:"valid_tasks,omitempty"`
	Invalid         *int      `json:"invalid,omitempty"`
	WallSeconds     *float64  `json:"wall_seconds,omitempty"`
	CostUSD         *float64  `json:"cost_usd,omitempty"`
	TotalTokens     *int64    `json:"total_tokens,omitempty"`
}

type Snapshot struct {
	Payload       json.RawMessage `json:"payload,omitempty"`
	History       []HistoryPoint  `json:"history,omitempty"`
	SourceURL     string          `json:"source_url"`
	FetchedAt     *time.Time      `json:"fetched_at,omitempty"`
	LastAttemptAt *time.Time      `json:"last_attempt_at,omitempty"`
	LastError     string          `json:"last_error,omitempty"`
	RefreshError  string          `json:"refresh_error,omitempty"`
	Stale         bool            `json:"stale"`
}

type Service struct {
	mu        sync.RWMutex
	refreshMu sync.Mutex
	url       string
	client    *http.Client
	log       *slog.Logger
	store     *storage.CodexRadarStore

	payload       json.RawMessage
	history       []HistoryPoint
	modelHash     string
	fetchedAt     time.Time
	lastAttemptAt time.Time
	lastError     string
}

func New(opts Options) *Service {
	endpoint := strings.TrimSpace(opts.URL)
	if endpoint == "" {
		endpoint = DefaultURL
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}
	s := &Service{url: endpoint, client: client, log: opts.Logger, store: opts.Store}
	s.restore()
	return s
}

func (s *Service) URL() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.url
}

func (s *Service) Snapshot() Snapshot {
	if s == nil {
		return Snapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

func (s *Service) Refresh(ctx context.Context) (Snapshot, error) {
	if s == nil {
		return Snapshot{}, errors.New("claude radar service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	attemptedAt := time.Now().UTC()
	s.mu.Lock()
	s.lastAttemptAt = attemptedAt
	endpoint := s.url
	client := s.client
	s.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err == nil {
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "BridgeOps-ClaudeRadar/1.0")
	}
	if err == nil {
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
				err = fmt.Errorf("claude radar status %d", resp.StatusCode)
			} else {
				body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxPayloadSize+1))
				if readErr != nil {
					err = fmt.Errorf("read claude radar response: %w", readErr)
				} else if len(body) > maxPayloadSize {
					err = fmt.Errorf("claude radar response exceeds %d bytes", maxPayloadSize)
				} else if err = validatePayload(body); err == nil {
					s.applySuccess(endpoint, body, time.Now().UTC())
					return s.Snapshot(), nil
				}
			}
		}
	}
	if err == nil {
		err = errors.New("claude radar refresh failed")
	}
	s.mu.Lock()
	s.lastError = err.Error()
	snap := s.snapshotLocked()
	s.mu.Unlock()
	if s.store != nil {
		if persistErr := s.store.SaveAttempt(endpoint, attemptedAt, err.Error()); persistErr != nil {
			s.logWarn("claude radar attempt persistence failed", "err", persistErr)
		}
	}
	if s.log != nil {
		s.log.Warn("claude radar refresh failed", "url", endpoint, "err", err)
	}
	return snap, err
}

func (s *Service) applySuccess(endpoint string, body []byte, fetchedAt time.Time) {
	modelHash := modelFingerprint(body)
	points := extractModelPoints(body, fetchedAt)
	if s.store != nil {
		if err := s.store.SaveSuccess(endpoint, string(body), hashBytes(body), modelHash, fetchedAt, toStoragePoints(points, endpoint, modelHash)); err != nil {
			s.logWarn("claude radar success persistence failed", "err", err)
		}
	}
	s.mu.Lock()
	if modelHash != s.modelHash && len(points) > 0 {
		s.history = append(s.history, points...)
	}
	s.modelHash = modelHash
	s.payload = append(s.payload[:0], body...)
	s.fetchedAt = fetchedAt
	s.lastError = ""
	s.pruneHistoryLocked(fetchedAt.AddDate(0, 0, -DefaultHistoryRetentionDays))
	s.mu.Unlock()
	if s.store != nil {
		if _, err := s.store.DeleteHistoryBefore(fetchedAt.AddDate(0, 0, -DefaultHistoryRetentionDays)); err != nil {
			s.logWarn("claude radar history cleanup failed", "err", err)
		}
	}
}

func (s *Service) restore() {
	if s.store == nil {
		return
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -DefaultHistoryRetentionDays)
	if _, err := s.store.DeleteHistoryBefore(cutoff); err != nil {
		s.logWarn("claude radar startup history cleanup failed", "err", err)
	}
	state, err := s.store.LoadState(s.url)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		s.logWarn("claude radar state restore failed", "err", err)
	}
	points, pointsErr := s.store.ListModelPoints(s.url, cutoff)
	if pointsErr != nil {
		s.logWarn("claude radar history restore failed", "err", pointsErr)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if state != nil {
		s.payload = []byte(state.PayloadJSON)
		s.modelHash = state.ModelHash
		s.fetchedAt = valueTime(state.FetchedAt)
		s.lastAttemptAt = valueTime(state.LastAttemptAt)
		s.lastError = state.LastError
	}
	for _, point := range points {
		s.history = append(s.history, fromStoragePoint(point))
	}
	s.pruneHistoryLocked(cutoff)
}

func (s *Service) logWarn(message string, args ...any) {
	if s.log != nil {
		s.log.Warn(message, args...)
	}
}

func (s *Service) pruneHistoryLocked(cutoff time.Time) {
	kept := s.history[:0]
	for _, point := range s.history {
		if point.ObservedAt.IsZero() || !point.ObservedAt.Before(cutoff) {
			kept = append(kept, point)
		}
	}
	s.history = kept
}

func validatePayload(body []byte) error {
	if len(strings.TrimSpace(string(body))) == 0 {
		return errors.New("claude radar response is empty")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode claude radar response: %w", err)
	}
	if envelope == nil {
		return errors.New("claude radar response must be a JSON object")
	}
	if raw, ok := envelope["ok"]; ok {
		var success bool
		if err := json.Unmarshal(raw, &success); err == nil && !success {
			return errors.New("claude radar response reported ok=false")
		}
	}
	iqRaw, ok := envelope["iq"]
	if !ok || len(iqRaw) == 0 || string(iqRaw) == "null" {
		return errors.New("claude radar response is missing iq data")
	}
	var iq map[string]json.RawMessage
	if err := json.Unmarshal(iqRaw, &iq); err != nil || iq == nil {
		return errors.New("claude radar response iq must be an object")
	}
	modelsRaw, ok := iq["models"]
	if !ok || len(modelsRaw) == 0 || string(modelsRaw) == "null" {
		return errors.New("claude radar response is missing iq.models")
	}
	var models []json.RawMessage
	if err := json.Unmarshal(modelsRaw, &models); err != nil || models == nil {
		return errors.New("claude radar response iq.models must be an array")
	}
	for _, modelRaw := range models {
		var model struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal(modelRaw, &model); err == nil && strings.TrimSpace(model.Key) != "" {
			return nil
		}
	}
	return errors.New("claude radar response iq.models has no model with a non-empty key")
}

type sourceModel struct {
	Key       string     `json:"key"`
	Name      string     `json:"name"`
	Score     *float64   `json:"score"`
	IQ        []*float64 `json:"iq"`
	Pass      []*float64 `json:"pass"`
	Valid     []*float64 `json:"valid"`
	Invalid   []*float64 `json:"invalid"`
	Cost      []*float64 `json:"cost"`
	TimeHours []*float64 `json:"time"`
	Tokens    []*float64 `json:"tokens"`
	LatestAt  string     `json:"latest_at"`
}

type sourcePayload struct {
	IQ struct {
		Models []sourceModel `json:"models"`
	} `json:"iq"`
}

func extractModelPoints(body []byte, observedAt time.Time) []HistoryPoint {
	var payload sourcePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil
	}
	points := make([]HistoryPoint, 0, len(payload.IQ.Models))
	for _, model := range payload.IQ.Models {
		if strings.TrimSpace(model.Key) == "" {
			continue
		}
		point := HistoryPoint{
			ObservedAt: observedAt,
			ConfigKey:  model.Key,
			Label:      model.Name,
			Model:      model.Name,
			Score:      model.Score,
			Passed:     lastInt(model.Pass),
			Tasks:      lastInt(model.Valid),
			ValidTasks: lastInt(model.Valid),
			Invalid:    lastInt(model.Invalid),
			CostUSD:    lastNumber(model.Cost),
			TotalTokens: func() *int64 {
				value := lastNumber(model.Tokens)
				if value == nil {
					return nil
				}
				result := int64(*value)
				return &result
			}(),
		}
		if hours := lastNumber(model.TimeHours); hours != nil {
			seconds := *hours * 3600
			point.WallSeconds = &seconds
		}
		points = append(points, point)
	}
	return points
}

func lastNumber(values []*float64) *float64 {
	for index := len(values) - 1; index >= 0; index-- {
		if values[index] != nil {
			value := *values[index]
			return &value
		}
	}
	return nil
}

func lastInt(values []*float64) *int {
	value := lastNumber(values)
	if value == nil {
		return nil
	}
	result := int(*value)
	return &result
}

func toStoragePoints(points []HistoryPoint, sourceURL, snapshotHash string) []storage.CodexRadarModelPoint {
	result := make([]storage.CodexRadarModelPoint, 0, len(points))
	for _, point := range points {
		result = append(result, storage.CodexRadarModelPoint{
			SourceURL: sourceURL, SnapshotHash: snapshotHash, ConfigKey: point.ConfigKey,
			Label: point.Label, Model: point.Model, ReasoningEffort: point.ReasoningEffort,
			ObservedAt: point.ObservedAt, Score: point.Score, Status: point.Status,
			Passed: point.Passed, Tasks: point.Tasks, ValidTasks: point.ValidTasks,
			Invalid: point.Invalid, WallSeconds: point.WallSeconds, CostUSD: point.CostUSD,
			TotalTokens: point.TotalTokens,
		})
	}
	return result
}

func fromStoragePoint(point storage.CodexRadarModelPoint) HistoryPoint {
	return HistoryPoint{ObservedAt: point.ObservedAt, ConfigKey: point.ConfigKey, Label: point.Label, Model: point.Model, ReasoningEffort: point.ReasoningEffort, Score: point.Score, Status: point.Status, Passed: point.Passed, Tasks: point.Tasks, ValidTasks: point.ValidTasks, Invalid: point.Invalid, WallSeconds: point.WallSeconds, CostUSD: point.CostUSD, TotalTokens: point.TotalTokens}
}

func hashBytes(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

func modelFingerprint(body []byte) string {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return hashBytes(body)
	}
	iqRaw, ok := envelope["iq"]
	if !ok {
		return hashBytes(body)
	}
	var iq map[string]json.RawMessage
	if err := json.Unmarshal(iqRaw, &iq); err != nil || iq == nil {
		return hashBytes(body)
	}
	modelsRaw, ok := iq["models"]
	if !ok {
		return hashBytes(body)
	}
	var models []json.RawMessage
	if err := json.Unmarshal(modelsRaw, &models); err != nil {
		return hashBytes(body)
	}

	// The endpoint publishes model metrics alongside volatile timestamps and
	// explanatory notes. Keep the metric objects, normalize their key order,
	// and sort models by key so harmless ordering/metadata changes do not add a
	// new history snapshot.
	type fingerprintModel struct {
		key  string
		data map[string]any
	}
	normalized := make([]fingerprintModel, 0, len(models))
	for _, modelRaw := range models {
		var model map[string]any
		if err := json.Unmarshal(modelRaw, &model); err != nil || model == nil {
			continue
		}
		key, _ := model["key"].(string)
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		model["key"] = key
		stripVolatileModelFields(model)
		normalized = append(normalized, fingerprintModel{key: key, data: model})
	}
	if len(normalized) == 0 {
		return hashBytes(body)
	}
	sort.SliceStable(normalized, func(i, j int) bool { return normalized[i].key < normalized[j].key })
	canonicalModels := make([]map[string]any, 0, len(normalized))
	for _, model := range normalized {
		canonicalModels = append(canonicalModels, model.data)
	}
	canonical, err := json.Marshal(struct {
		Models []map[string]any `json:"models"`
	}{Models: canonicalModels})
	if err != nil {
		return hashBytes(body)
	}
	return hashBytes(canonical)
}

// stripVolatileModelFields removes fields that describe when/how the public
// measurement was generated rather than the model's measured metrics.
func stripVolatileModelFields(value map[string]any) {
	for key, nested := range value {
		if isVolatileModelField(key) {
			delete(value, key)
			continue
		}
		switch item := nested.(type) {
		case map[string]any:
			stripVolatileModelFields(item)
		case []any:
			for _, element := range item {
				if object, ok := element.(map[string]any); ok {
					stripVolatileModelFields(object)
				}
			}
		}
	}
}

func isVolatileModelField(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "latest_at", "updated_at", "last_updated_at", "fetched_at", "observed_at", "timestamp", "timestamps", "notes", "latest_policy":
		return true
	default:
		return false
	}
}

func valueTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.UTC()
}

func (s *Service) snapshotLocked() Snapshot {
	out := Snapshot{SourceURL: s.url, LastError: s.lastError, RefreshError: s.lastError, Stale: len(s.payload) > 0 && !s.fetchedAt.IsZero() && s.lastError != ""}
	if len(s.payload) > 0 {
		out.Payload = append(json.RawMessage(nil), s.payload...)
	}
	if len(s.history) > 0 {
		out.History = append([]HistoryPoint(nil), s.history...)
	}
	if !s.fetchedAt.IsZero() {
		value := s.fetchedAt
		out.FetchedAt = &value
	}
	if !s.lastAttemptAt.IsZero() {
		value := s.lastAttemptAt
		out.LastAttemptAt = &value
	}
	return out
}
