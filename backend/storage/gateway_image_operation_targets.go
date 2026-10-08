package storage

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	gatewayImageOperationTargetInventoryCursorVersion = 1
	gatewayImageOperationTargetInventoryCursorLimit   = 2048
	gatewayImageOperationTargetInventoryDefaultSize   = 250
	gatewayImageOperationTargetInventoryMaximumSize   = 500
)

var (
	ErrGatewayImageOperationTargetInventoryCursorInvalid   = errors.New("image operation target inventory cursor is invalid")
	ErrGatewayImageOperationTargetInventoryPageSizeInvalid = errors.New("image operation target inventory page size is invalid")
	ErrGatewayImageOperationTargetInventorySnapshotChanged = errors.New("image operation target inventory changed; refresh targets")
)

// GatewayImageOperationTarget is a current-generation model target. ModelIDs
// come directly from upstream-key model-sync facts observed after the current
// group generation was discovered, so this inventory does not depend on rates,
// probes, routing availability, or the rate-gated group-fact projection.
type GatewayImageOperationTarget struct {
	ChannelID          uint       `json:"channel_id"`
	ChannelName        string     `json:"channel_name"`
	UpstreamGroupID    uint       `json:"upstream_group_id"`
	GroupName          string     `json:"group_name"`
	PresenceGeneration uint64     `json:"presence_generation"`
	ModelName          string     `json:"model_name"`
	ModelsSyncedAt     *time.Time `json:"models_synced_at,omitempty"`
	GroupFactsVersion  uint64     `json:"group_facts_version"`
}

type GatewayImageOperationTargetInventoryPage struct {
	Items      []GatewayImageOperationTarget `json:"items"`
	NextCursor string                        `json:"next_cursor,omitempty"`
	Snapshot   string                        `json:"snapshot"`
	CapturedAt time.Time                     `json:"captured_at"`
}

type gatewayImageOperationTargetInventoryCursor struct {
	Version         int    `json:"v"`
	Snapshot        string `json:"s"`
	ChannelID       uint   `json:"c"`
	UpstreamGroupID uint   `json:"g"`
	ModelName       string `json:"m"`
}

// ListImageOperationTargetInventoryPage returns one bounded keyset page. The
// cursor carries the complete relevant-facts snapshot; a change between pages
// fails closed instead of letting the browser treat a mixed inventory as
// authoritative.
func (g *Gateway) ListImageOperationTargetInventoryPage(cursor string, pageSize int) (*GatewayImageOperationTargetInventoryPage, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	pageSize, err := normalizeGatewayImageOperationTargetInventoryPageSize(pageSize)
	if err != nil {
		return nil, err
	}
	parsedCursor, err := parseGatewayImageOperationTargetInventoryCursor(cursor)
	if err != nil {
		return nil, err
	}
	snapshot, err := g.imageOperationTargetInventorySnapshot()
	if err != nil {
		return nil, err
	}
	if parsedCursor != nil && parsedCursor.Snapshot != snapshot {
		return nil, ErrGatewayImageOperationTargetInventorySnapshotChanged
	}

	query := g.db.Table("gateway_upstream_groups AS groups").
		Select(`groups.channel_id,
			channels.name AS channel_name,
			groups.id AS upstream_group_id,
			groups.canonical_group_name AS group_name,
			groups.presence_generation,
			btrim(models.model_name) AS model_name,
			MAX(keys.models_synced_at) AS models_synced_at,
			groups.facts_version AS group_facts_version`).
		Joins("JOIN channels ON channels.id = groups.channel_id").
		Joins("JOIN gateway_upstream_keys AS keys ON keys.channel_id = groups.channel_id AND lower(btrim(keys.group_name)) = groups.canonical_group_name").
		Joins("CROSS JOIN LATERAL regexp_split_to_table(COALESCE(keys.model_ids, ''), ',') AS models(model_name)").
		Where("groups.present = TRUE AND keys.models_synced_at IS NOT NULL AND keys.models_synced_at >= groups.discovered_at AND btrim(models.model_name) <> ''").
		Group("groups.channel_id, channels.name, groups.id, groups.canonical_group_name, groups.presence_generation, btrim(models.model_name), groups.facts_version")
	if parsedCursor != nil {
		query = query.Where(`(groups.channel_id > ? OR
			(groups.channel_id = ? AND groups.id > ?) OR
			(groups.channel_id = ? AND groups.id = ? AND btrim(models.model_name) > ?))`,
			parsedCursor.ChannelID,
			parsedCursor.ChannelID, parsedCursor.UpstreamGroupID,
			parsedCursor.ChannelID, parsedCursor.UpstreamGroupID, parsedCursor.ModelName,
		)
	}

	items := make([]GatewayImageOperationTarget, 0, pageSize+1)
	if err := query.
		Order("groups.channel_id ASC, groups.id ASC, btrim(models.model_name) ASC").
		Limit(pageSize + 1).
		Scan(&items).Error; err != nil {
		return nil, err
	}
	page := &GatewayImageOperationTargetInventoryPage{
		Items:      []GatewayImageOperationTarget{},
		Snapshot:   snapshot,
		CapturedAt: time.Now().UTC(),
	}
	if len(items) > pageSize {
		items = items[:pageSize]
		page.NextCursor, err = encodeGatewayImageOperationTargetInventoryCursor(snapshot, items[len(items)-1])
		if err != nil {
			return nil, err
		}
	}
	page.Items = items
	return page, nil
}

func normalizeGatewayImageOperationTargetInventoryPageSize(pageSize int) (int, error) {
	if pageSize <= 0 {
		return gatewayImageOperationTargetInventoryDefaultSize, nil
	}
	if pageSize > gatewayImageOperationTargetInventoryMaximumSize {
		return 0, ErrGatewayImageOperationTargetInventoryPageSizeInvalid
	}
	return pageSize, nil
}

func (g *Gateway) imageOperationTargetInventorySnapshot() (string, error) {
	var snapshot string
	// Include every present generation plus all matching key/model facts. This
	// detects additions, removals, rebirths, model refreshes, and channel display
	// changes while a browser is paging, including groups with no rate snapshot.
	err := g.db.Raw(`SELECT md5(jsonb_build_object(
		'groups', COALESCE((
			SELECT jsonb_agg(jsonb_build_array(
				groups.id,
				groups.channel_id,
				groups.presence_generation,
				groups.discovered_at,
				groups.updated_at,
				channels.name,
				channels.updated_at
			) ORDER BY groups.id)
			FROM gateway_upstream_groups AS groups
			JOIN channels ON channels.id = groups.channel_id
			WHERE groups.present = TRUE
		), '[]'::jsonb),
		'keys', COALESCE((
			SELECT jsonb_agg(jsonb_build_array(
				keys.id,
				keys.channel_id,
				keys.group_name,
				keys.model_ids,
				keys.models_synced_at,
				keys.updated_at
			) ORDER BY keys.id)
			FROM gateway_upstream_keys AS keys
			JOIN gateway_upstream_groups AS groups
				ON groups.channel_id = keys.channel_id
				AND groups.canonical_group_name = lower(btrim(keys.group_name))
				AND groups.present = TRUE
		), '[]'::jsonb)
	)::text)`).Scan(&snapshot).Error
	if err != nil {
		return "", err
	}
	snapshot = strings.TrimSpace(snapshot)
	if snapshot == "" {
		return "", errors.New("image operation target inventory snapshot is unavailable")
	}
	return snapshot, nil
}

func encodeGatewayImageOperationTargetInventoryCursor(snapshot string, item GatewayImageOperationTarget) (string, error) {
	if strings.TrimSpace(snapshot) == "" || item.ChannelID == 0 || item.UpstreamGroupID == 0 || strings.TrimSpace(item.ModelName) == "" {
		return "", fmt.Errorf("%w: boundary", ErrGatewayImageOperationTargetInventoryCursorInvalid)
	}
	payload, err := json.Marshal(gatewayImageOperationTargetInventoryCursor{
		Version: gatewayImageOperationTargetInventoryCursorVersion, Snapshot: snapshot,
		ChannelID: item.ChannelID, UpstreamGroupID: item.UpstreamGroupID, ModelName: item.ModelName,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func parseGatewayImageOperationTargetInventoryCursor(value string) (*gatewayImageOperationTargetInventoryCursor, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if len(value) > gatewayImageOperationTargetInventoryCursorLimit {
		return nil, fmt.Errorf("%w: length", ErrGatewayImageOperationTargetInventoryCursorInvalid)
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(payload) == 0 || len(payload) > gatewayImageOperationTargetInventoryCursorLimit {
		return nil, fmt.Errorf("%w: encoding", ErrGatewayImageOperationTargetInventoryCursorInvalid)
	}
	var cursor gatewayImageOperationTargetInventoryCursor
	if err := json.Unmarshal(payload, &cursor); err != nil ||
		cursor.Version != gatewayImageOperationTargetInventoryCursorVersion ||
		strings.TrimSpace(cursor.Snapshot) == "" || cursor.ChannelID == 0 || cursor.UpstreamGroupID == 0 || strings.TrimSpace(cursor.ModelName) == "" {
		return nil, fmt.Errorf("%w: value", ErrGatewayImageOperationTargetInventoryCursorInvalid)
	}
	return &cursor, nil
}
