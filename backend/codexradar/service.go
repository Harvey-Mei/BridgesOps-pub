// Package codexradar fetches the public Codex Reset Radar summary.
package codexradar

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
	"strings"
	"sync"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"gorm.io/gorm"
)

const (
	// DefaultURL is the public, machine-readable Codex Reset Radar endpoint.
	DefaultURL = "https://codexradar.com/current.json"

	// DefaultHistoryRetentionDays bounds the local model history. The source
	// updates infrequently, so only changed model snapshots are stored.
	DefaultHistoryRetentionDays = 90

	defaultTimeout = 15 * time.Second
	maxPayloadSize = 2 << 20
)

// Options controls a Radar service. Client and URL are injectable for tests and
// deployments that need to route this request through a custom transport.
type Options struct {
	URL    string
	Client *http.Client
	Logger *slog.Logger
	Store  *storage.CodexRadarStore
}

// HistoryPoint is a normalized model snapshot captured when the upstream model
// data changes. It is intentionally smaller than the raw public payload.
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

// Snapshot is the last known Radar payload and fetch status. Payload is kept as
// raw JSON so schema additions from the public endpoint do not require a
// BridgeOps release before they can be displayed by the frontend. History only
// contains normalized points captured when model data actually changed.
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

// URL returns the configured source URL.
func (s *Service) URL() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.url
}

// Snapshot returns a consistent copy of the in-memory state. A stale payload
// remains available after a failed refresh and is explicitly marked stale.
func (s *Service) Snapshot() Snapshot {
	if s == nil {
		return Snapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked()
}

// Refresh fetches and validates the public summary. A failed request updates
// only the attempt/error metadata, preserving the last successful payload and
// its local history.
func (s *Service) Refresh(ctx context.Context) (Snapshot, error) {
	if s == nil {
		return Snapshot{}, errors.New("codex radar service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Avoid overlapping scheduled and manually-triggered requests. This also
	// prevents a slow failed request from racing a successful one and replacing
	// its metadata out of order.
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	now := time.Now().UTC()
	s.mu.Lock()
	s.lastAttemptAt = now
	endpoint := s.url
	client := s.client
	s.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err == nil {
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "BridgeOps-CodexRadar/1.0")
	}
	if err == nil {
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
				err = fmt.Errorf("codex radar status %d", resp.StatusCode)
			} else {
				body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxPayloadSize+1))
				if readErr != nil {
					err = fmt.Errorf("read codex radar response: %w", readErr)
				} else if len(body) > maxPayloadSize {
					err = fmt.Errorf("codex radar response exceeds %d bytes", maxPayloadSize)
				} else if err = validatePayload(body); err == nil {
					s.applySuccess(endpoint, body, time.Now().UTC())
					return s.Snapshot(), nil
				}
			}
		}
	}

	if err == nil {
		err = errors.New("codex radar refresh failed")
	}
	s.mu.Lock()
	s.lastError = err.Error()
	snap := s.snapshotLocked()
	s.mu.Unlock()
	if s.store != nil {
		if persistErr := s.store.SaveAttempt(endpoint, now, err.Error()); persistErr != nil {
			s.logWarn("codex radar attempt persistence failed", "err", persistErr)
		}
	}
	if s.log != nil {
		s.log.Warn("codex radar refresh failed", "url", endpoint, "err", err)
	}
	return snap, err
}

func (s *Service) applySuccess(endpoint string, body []byte, fetchedAt time.Time) {
	modelHash := modelFingerprint(body)
	payloadHash := hashBytes(body)
	points := extractModelPoints(body, fetchedAt)
	if s.store != nil {
		if err := s.store.SaveSuccess(endpoint, string(body), payloadHash, modelHash, fetchedAt, toStoragePoints(points, endpoint, modelHash)); err != nil {
			s.logWarn("codex radar success persistence failed", "err", err)
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
			s.logWarn("codex radar history cleanup failed", "err", err)
		}
	}
}

func (s *Service) restore() {
	if s.store == nil {
		return
	}
	now := time.Now().UTC()
	cutoff := now.AddDate(0, 0, -DefaultHistoryRetentionDays)
	if _, err := s.store.DeleteHistoryBefore(cutoff); err != nil {
		s.logWarn("codex radar startup history cleanup failed", "err", err)
	}
	state, err := s.store.LoadState(s.url)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		s.logWarn("codex radar state restore failed", "err", err)
	}
	points, pointsErr := s.store.ListModelPoints(s.url, cutoff)
	if pointsErr != nil {
		s.logWarn("codex radar history restore failed", "err", pointsErr)
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
		return errors.New("codex radar response is empty")
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("decode codex radar response: %w", err)
	}
	if payload == nil {
		return errors.New("codex radar response must be a JSON object")
	}
	// Reject generic error objects and empty objects without overfitting the
	// nested public schema. These markers have remained stable in current.json.
	for _, key := range []string{"model_iq", "monitored_at", "schema_version", "service", "status"} {
		if _, ok := payload[key]; ok {
			return nil
		}
	}
	return errors.New("codex radar response is missing summary fields")
}

type sourcePoint struct {
	Score           *float64 `json:"score"`
	Status          string   `json:"status"`
	Passed          *int     `json:"passed"`
	Tasks           *int     `json:"tasks"`
	ValidTasks      *int     `json:"valid_tasks"`
	Invalid         *int     `json:"invalid"`
	WallSeconds     *float64 `json:"wall_seconds"`
	CostUSD         *float64 `json:"cost_usd"`
	TotalTokens     *int64   `json:"total_tokens"`
	Model           string   `json:"model"`
	ReasoningEffort string   `json:"reasoning_effort"`
}

type sourceComparison struct {
	Label  string       `json:"label"`
	Model  string       `json:"model"`
	Latest *sourcePoint `json:"latest"`
}

type sourceModelIQ struct {
	Latest      *sourcePoint                `json:"latest"`
	Comparisons map[string]sourceComparison `json:"comparisons"`
}

type sourcePayload struct {
	ModelIQ sourceModelIQ `json:"model_iq"`
}

func extractModelPoints(body []byte, observedAt time.Time) []HistoryPoint {
	var payload sourcePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil
	}
	points := make([]HistoryPoint, 0, len(payload.ModelIQ.Comparisons))
	for key, comparison := range payload.ModelIQ.Comparisons {
		if comparison.Latest == nil {
			continue
		}
		points = append(points, newHistoryPoint(key, comparison.Label, comparison.Latest, observedAt))
	}
	if len(points) == 0 && payload.ModelIQ.Latest != nil {
		points = append(points, newHistoryPoint("overall_latest", "总体最新测试", payload.ModelIQ.Latest, observedAt))
	}
	return points
}

func newHistoryPoint(key, label string, point *sourcePoint, observedAt time.Time) HistoryPoint {
	return HistoryPoint{
		ObservedAt:      observedAt,
		ConfigKey:       key,
		Label:           label,
		Model:           point.Model,
		ReasoningEffort: point.ReasoningEffort,
		Score:           point.Score,
		Status:          point.Status,
		Passed:          point.Passed,
		Tasks:           point.Tasks,
		ValidTasks:      point.ValidTasks,
		Invalid:         point.Invalid,
		WallSeconds:     point.WallSeconds,
		CostUSD:         point.CostUSD,
		TotalTokens:     point.TotalTokens,
	}
}

func toStoragePoints(points []HistoryPoint, sourceURL, snapshotHash string) []storage.CodexRadarModelPoint {
	result := make([]storage.CodexRadarModelPoint, 0, len(points))
	for _, point := range points {
		result = append(result, storage.CodexRadarModelPoint{
			SourceURL:       sourceURL,
			SnapshotHash:    snapshotHash,
			ConfigKey:       point.ConfigKey,
			Label:           point.Label,
			Model:           point.Model,
			ReasoningEffort: point.ReasoningEffort,
			ObservedAt:      point.ObservedAt,
			Score:           point.Score,
			Status:          point.Status,
			Passed:          point.Passed,
			Tasks:           point.Tasks,
			ValidTasks:      point.ValidTasks,
			Invalid:         point.Invalid,
			WallSeconds:     point.WallSeconds,
			CostUSD:         point.CostUSD,
			TotalTokens:     point.TotalTokens,
		})
	}
	return result
}

func fromStoragePoint(point storage.CodexRadarModelPoint) HistoryPoint {
	return HistoryPoint{
		ObservedAt:      point.ObservedAt,
		ConfigKey:       point.ConfigKey,
		Label:           point.Label,
		Model:           point.Model,
		ReasoningEffort: point.ReasoningEffort,
		Score:           point.Score,
		Status:          point.Status,
		Passed:          point.Passed,
		Tasks:           point.Tasks,
		ValidTasks:      point.ValidTasks,
		Invalid:         point.Invalid,
		WallSeconds:     point.WallSeconds,
		CostUSD:         point.CostUSD,
		TotalTokens:     point.TotalTokens,
	}
}

func hashBytes(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

// modelFingerprint deliberately hashes only model_iq. Changes to monitored_at,
// prediction text, or other metadata do not create duplicate model history rows.
func modelFingerprint(body []byte) string {
	var envelope map[string]json.RawMessage
	target := json.RawMessage(body)
	if err := json.Unmarshal(body, &envelope); err == nil {
		if modelIQ, ok := envelope["model_iq"]; ok {
			target = modelIQ
		}
	}
	var canonical any
	if err := json.Unmarshal(target, &canonical); err == nil {
		if normalized, marshalErr := json.Marshal(canonical); marshalErr == nil {
			target = normalized
		}
	}
	return hashBytes(target)
}

func valueTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.UTC()
}

func (s *Service) snapshotLocked() Snapshot {
	out := Snapshot{
		SourceURL:    s.url,
		LastError:    s.lastError,
		RefreshError: s.lastError,
		Stale:        len(s.payload) > 0 && s.fetchedAt.IsZero() == false && s.lastError != "",
	}
	if len(s.payload) > 0 {
		out.Payload = append(json.RawMessage(nil), s.payload...)
	}
	if len(s.history) > 0 {
		out.History = append([]HistoryPoint(nil), s.history...)
	}
	if !s.fetchedAt.IsZero() {
		v := s.fetchedAt
		out.FetchedAt = &v
	}
	if !s.lastAttemptAt.IsZero() {
		v := s.lastAttemptAt
		out.LastAttemptAt = &v
	}
	return out
}
