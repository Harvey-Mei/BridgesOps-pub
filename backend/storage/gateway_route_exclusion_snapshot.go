package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// GatewayRouteExclusionSnapshotVersion is independent from request
	// telemetry. It permits the snapshot schema to evolve without reinterpreting
	// historical version-zero request rows.
	GatewayRouteExclusionSnapshotVersion  = 1
	gatewayRouteExclusionSnapshotMaxRows  = 64
	gatewayRouteExclusionSnapshotMaxBytes = 32 * 1024
)

// GatewayRouteExclusionSnapshot is the frozen routing-state evidence captured
// when a logical request chooses its candidate set. It intentionally excludes
// channel labels, error details, credentials, request data, and upstream
// response data. An unavailable generation is represented explicitly with a
// zero upstream_group_id and presence_generation rather than guessed later.
type GatewayRouteExclusionSnapshot struct {
	Version      int                                  `json:"version"`
	CapturedAt   time.Time                            `json:"captured_at"`
	TotalEntries int                                  `json:"total_entries"`
	Truncated    bool                                 `json:"truncated"`
	Entries      []GatewayRouteExclusionSnapshotEntry `json:"entries"`
}

type GatewayRouteExclusionSnapshotEntry struct {
	ChannelID          uint       `json:"channel_id"`
	GroupName          string     `json:"group_name"`
	Reason             string     `json:"reason"`
	UpstreamGroupID    uint       `json:"upstream_group_id,omitempty"`
	PresenceGeneration uint64     `json:"presence_generation,omitempty"`
	FailureCount       int        `json:"failure_count,omitempty"`
	CooldownUntil      *time.Time `json:"cooldown_until,omitempty"`
}

func gatewayRouteExclusionReasonAllowed(value string) bool {
	switch value {
	case "upstream_generation_unavailable", "upstream_temporarily_unavailable", "model_capability_unknown", "model_not_listed", "model_not_supported":
		return true
	default:
		return false
	}
}

func normalizeGatewayRouteExclusionSnapshotText(value string) (GatewayRouteExclusionSnapshot, string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot is required")
	}
	if len(value) > gatewayRouteExclusionSnapshotMaxBytes {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot exceeds its safe length")
	}
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	var snapshot GatewayRouteExclusionSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return GatewayRouteExclusionSnapshot{}, "", fmt.Errorf("route exclusion snapshot is invalid: %w", err)
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot has trailing data")
	}
	if snapshot.Version != GatewayRouteExclusionSnapshotVersion {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot version is invalid")
	}
	if snapshot.CapturedAt.IsZero() {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot capture time is required")
	}
	snapshot.CapturedAt = snapshot.CapturedAt.UTC()
	if snapshot.Entries == nil {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot entries must be an array")
	}
	if len(snapshot.Entries) > gatewayRouteExclusionSnapshotMaxRows {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot has too many entries")
	}
	if snapshot.TotalEntries < len(snapshot.Entries) || snapshot.TotalEntries > 1_000_000 {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot total is invalid")
	}
	if snapshot.Truncated != (snapshot.TotalEntries > len(snapshot.Entries)) {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot truncation marker is invalid")
	}
	seen := make(map[string]struct{}, len(snapshot.Entries))
	for index := range snapshot.Entries {
		entry := &snapshot.Entries[index]
		if entry.ChannelID == 0 {
			return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot channel is required")
		}
		if !utf8.ValidString(entry.GroupName) || utf8.RuneCountInString(entry.GroupName) > 128 {
			return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot group is invalid")
		}
		for _, value := range entry.GroupName {
			if unicode.IsControl(value) {
				return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot group is invalid")
			}
		}
		canonicalGroup := strings.ToLower(strings.TrimSpace(entry.GroupName))
		if canonicalGroup == "" || canonicalGroup != entry.GroupName {
			return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot group must be canonical")
		}
		entry.Reason = strings.TrimSpace(entry.Reason)
		if !gatewayRouteExclusionReasonAllowed(entry.Reason) {
			return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot reason is invalid")
		}
		if entry.FailureCount < 0 || entry.FailureCount > 1_000_000 {
			return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot failure count is invalid")
		}
		if entry.Reason == "upstream_generation_unavailable" {
			if entry.UpstreamGroupID != 0 || entry.PresenceGeneration != 0 || entry.FailureCount != 0 || entry.CooldownUntil != nil {
				return GatewayRouteExclusionSnapshot{}, "", errors.New("unavailable generation snapshot must not claim current state")
			}
		} else {
			if entry.UpstreamGroupID == 0 || entry.PresenceGeneration == 0 {
				return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot generation is required")
			}
			if entry.FailureCount == 0 && entry.CooldownUntil != nil {
				return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot has incomplete cooldown facts")
			}
			if entry.CooldownUntil != nil && entry.CooldownUntil.IsZero() {
				return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot has invalid cooldown deadline")
			}
			if entry.Reason == "upstream_temporarily_unavailable" && entry.FailureCount == 0 {
				return GatewayRouteExclusionSnapshot{}, "", errors.New("cooldown exclusion snapshot lacks cooldown facts")
			}
			if entry.CooldownUntil != nil {
				until := entry.CooldownUntil.UTC()
				entry.CooldownUntil = &until
			}
		}
		key := fmt.Sprintf("%d\x00%s\x00%s", entry.ChannelID, entry.GroupName, entry.Reason)
		if _, duplicate := seen[key]; duplicate {
			return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot has duplicate entries")
		}
		seen[key] = struct{}{}
	}
	sort.Slice(snapshot.Entries, func(left, right int) bool {
		leftEntry, rightEntry := snapshot.Entries[left], snapshot.Entries[right]
		if leftEntry.ChannelID != rightEntry.ChannelID {
			return leftEntry.ChannelID < rightEntry.ChannelID
		}
		if leftEntry.GroupName != rightEntry.GroupName {
			return leftEntry.GroupName < rightEntry.GroupName
		}
		return leftEntry.Reason < rightEntry.Reason
	})
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return GatewayRouteExclusionSnapshot{}, "", fmt.Errorf("encode route exclusion snapshot: %w", err)
	}
	if len(encoded) > gatewayRouteExclusionSnapshotMaxBytes {
		return GatewayRouteExclusionSnapshot{}, "", errors.New("route exclusion snapshot exceeds its safe length")
	}
	return snapshot, string(encoded), nil
}

// DecodeGatewayRouteExclusionSnapshot is deliberately strict for every reader,
// including the API. Historical version-zero rows return ok=false and must be
// presented as unavailable rather than reconstructed from current state.
func DecodeGatewayRouteExclusionSnapshot(version int, value string) (snapshot GatewayRouteExclusionSnapshot, ok bool) {
	if version != GatewayRouteExclusionSnapshotVersion {
		return GatewayRouteExclusionSnapshot{}, false
	}
	snapshot, _, err := normalizeGatewayRouteExclusionSnapshotText(value)
	if err != nil {
		return GatewayRouteExclusionSnapshot{}, false
	}
	return snapshot, true
}

func normalizeGatewayRouteExclusionSnapshot(version int, value string) (int, string, error) {
	if version == 0 {
		if strings.TrimSpace(value) != "" && strings.TrimSpace(value) != "{}" {
			return 0, "", errors.New("route exclusion snapshot requires a snapshot version")
		}
		return 0, "{}", nil
	}
	if version != GatewayRouteExclusionSnapshotVersion {
		return 0, "", errors.New("route exclusion snapshot version is invalid")
	}
	_, normalized, err := normalizeGatewayRouteExclusionSnapshotText(value)
	if err != nil {
		return 0, "", err
	}
	return version, normalized, nil
}

// NewGatewayRouteExclusionSnapshot serializes trusted, already-redacted
// routing facts. Keeping construction in storage gives every writer the same
// canonical encoding and validation as persistence and API reads.
func NewGatewayRouteExclusionSnapshot(capturedAt time.Time, entries []GatewayRouteExclusionSnapshotEntry, sourceTotals ...int) (string, error) {
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	}
	normalizedEntries := make([]GatewayRouteExclusionSnapshotEntry, len(entries))
	copy(normalizedEntries, entries)
	entries = normalizedEntries
	sort.Slice(entries, func(left, right int) bool {
		leftEntry, rightEntry := entries[left], entries[right]
		if leftEntry.ChannelID != rightEntry.ChannelID {
			return leftEntry.ChannelID < rightEntry.ChannelID
		}
		if leftEntry.GroupName != rightEntry.GroupName {
			return leftEntry.GroupName < rightEntry.GroupName
		}
		return leftEntry.Reason < rightEntry.Reason
	})
	totalEntries := len(entries)
	if len(sourceTotals) > 0 {
		if sourceTotals[0] < totalEntries || sourceTotals[0] > 1_000_000 {
			return "", errors.New("route exclusion snapshot source total is invalid")
		}
		totalEntries = sourceTotals[0]
	}
	if len(entries) > gatewayRouteExclusionSnapshotMaxRows {
		entries = entries[:gatewayRouteExclusionSnapshotMaxRows]
	}
	snapshot := GatewayRouteExclusionSnapshot{
		Version:      GatewayRouteExclusionSnapshotVersion,
		CapturedAt:   capturedAt.UTC(),
		TotalEntries: totalEntries,
		Truncated:    totalEntries > len(entries),
		Entries:      entries,
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	_, normalized, err := normalizeGatewayRouteExclusionSnapshotText(string(encoded))
	return normalized, err
}
