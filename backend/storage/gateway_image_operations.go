package storage

import (
	"errors"
	"fmt"
	"net/http"
	"net/textproto"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrGatewayImageCapabilityVersionConflict = errors.New("image operation capability version changed")

const gatewayImageCapabilityAuditListLimit = 100

var (
	imageCapabilityJSONPathPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*(\.[A-Za-z_][A-Za-z0-9_-]*)*$`)
	imageCapabilityHeaderPattern   = regexp.MustCompile(`^[!#$%&'*+.^_` + "`" + `|~0-9A-Za-z-]+$`)
	imageCapabilityStatePattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)
)

type GatewayImageOperationCapabilityFilter struct {
	ChannelID uint
	GroupName string
	ModelName string
	Protocol  string
}

// SaveImageOperationCapabilityConfig applies an optimistic administrator
// update. expectedVersion must be zero for a new row and must match the
// current version for an existing row.
func (g *Gateway) SaveImageOperationCapabilityConfig(capability GatewayImageOperationCapability, generation UpstreamGroupGeneration, expectedVersion uint64, actor string) (*GatewayImageOperationCapability, error) {
	capability.EvidenceSource = GatewayImageEvidenceAdminConfig
	capability.EvidenceDetail = "administrator_configuration"
	capability.EvidenceObservedAt = time.Now().UTC()
	capability.ConfiguredBy = normalizeImageCapabilityActor(actor)
	return g.upsertImageOperationCapability(capability, generation, &expectedVersion, "", 0, capability.ConfiguredBy)
}

// ObserveImageOperationCapability records only evidence derived from a real,
// successful paid request. The caller supplies a request-log reference, never
// a task ID, idempotency value, or raw response.
func (g *Gateway) ObserveImageOperationCapability(capability GatewayImageOperationCapability, generation UpstreamGroupGeneration, requestLogID uint) (*GatewayImageOperationCapability, error) {
	if requestLogID == 0 {
		return nil, errors.New("observed image capability requires a request log")
	}
	capability.EvidenceSource = GatewayImageEvidenceObservedSuccess
	capability.ConfiguredBy = ""
	if capability.EvidenceObservedAt.IsZero() {
		capability.EvidenceObservedAt = time.Now().UTC()
	}
	return g.upsertImageOperationCapability(capability, generation, nil, "request_log", uint64(requestLogID), "gateway")
}

// RecordProviderImageOperationCapability accepts structured provider metadata
// obtained without a paid image POST. Fetching that metadata is intentionally
// outside this repository method so this code cannot become an active probe.
func (g *Gateway) RecordProviderImageOperationCapability(capability GatewayImageOperationCapability, generation UpstreamGroupGeneration, actor string) (*GatewayImageOperationCapability, error) {
	capability.EvidenceSource = GatewayImageEvidenceProviderMetadata
	capability.EvidenceDetail = "provider_metadata"
	capability.ConfiguredBy = ""
	if capability.EvidenceObservedAt.IsZero() {
		capability.EvidenceObservedAt = time.Now().UTC()
	}
	return g.upsertImageOperationCapability(capability, generation, nil, "", 0, normalizeImageCapabilityActor(actor))
}

func (g *Gateway) upsertImageOperationCapability(capability GatewayImageOperationCapability, generation UpstreamGroupGeneration, expectedVersion *uint64, referenceKind string, referenceID uint64, actor string) (*GatewayImageOperationCapability, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway storage is required")
	}
	if err := normalizeGatewayImageOperationCapability(&capability); err != nil {
		return nil, err
	}
	if !generation.Valid() || generation.ChannelID != capability.ChannelID {
		return nil, ErrUpstreamGroupGenerationChanged
	}
	capability.UpstreamGroupID = generation.ID
	capability.CanonicalGroupName = generation.CanonicalGroupName
	capability.PresenceGeneration = generation.PresenceGeneration
	actor = normalizeImageCapabilityActor(actor)
	referenceKind = strings.TrimSpace(referenceKind)
	if referenceKind != "" && referenceKind != "request_log" {
		return nil, errors.New("invalid image capability evidence reference")
	}

	var result GatewayImageOperationCapability
	err := g.db.Transaction(func(tx *gorm.DB) error {
		if _, err := requireOrdinaryChannelWriteTx(tx, generation.ChannelID); err != nil {
			return err
		}
		// The upstream-group row is the serialization point for both creates and
		// updates, so concurrent first observations cannot race past the unique
		// identity constraint and stale generations always fail closed.
		if err := requireUpstreamGroupGenerationTx(tx, generation); err != nil {
			return err
		}
		if capability.EvidenceSource == GatewayImageEvidenceObservedSuccess {
			if err := requireObservedImageCapabilityRequestLogTx(tx, capability, referenceID); err != nil {
				return err
			}
		}
		var existing GatewayImageOperationCapability
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"upstream_group_id = ? AND presence_generation = ? AND model_name = ? AND operation = ? AND protocol = ?",
			generation.ID, generation.PresenceGeneration, capability.ModelName, capability.Operation, capability.Protocol,
		).First(&existing).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if expectedVersion != nil && *expectedVersion != 0 {
				return ErrGatewayImageCapabilityVersionConflict
			}
			capability.ID = 0
			capability.CapabilityVersion = 1
			if err := tx.Create(&capability).Error; err != nil {
				return err
			}
			result = capability
			return appendGatewayImageCapabilityAuditTx(tx, &result, imageCapabilityAuditAction(result.EvidenceSource), referenceKind, referenceID, actor)
		}
		if err != nil {
			return err
		}
		if expectedVersion != nil && *expectedVersion != existing.CapabilityVersion {
			return ErrGatewayImageCapabilityVersionConflict
		}
		lowerPriority := imageCapabilityEvidenceRank(capability.EvidenceSource) < imageCapabilityEvidenceRank(existing.EvidenceSource)
		notNewerSameSource := capability.EvidenceSource == existing.EvidenceSource && !capability.EvidenceObservedAt.After(existing.EvidenceObservedAt)
		if expectedVersion == nil && (lowerPriority || notNewerSameSource) {
			result = existing
			ignoredEvidence := capability
			ignoredEvidence.ID = existing.ID
			ignoredEvidence.CapabilityVersion = existing.CapabilityVersion
			return appendGatewayImageCapabilityAuditTx(tx, &ignoredEvidence, "evidence_ignored", referenceKind, referenceID, actor)
		}
		capability.ID = existing.ID
		capability.CreatedAt = existing.CreatedAt
		capability.CapabilityVersion = existing.CapabilityVersion + 1
		if expectedVersion == nil && capability.EvidenceSource == GatewayImageEvidenceObservedSuccess && existing.EvidenceSource == GatewayImageEvidenceObservedSuccess {
			mergeObservedImageCapability(&capability, existing)
		}
		if err := tx.Model(&existing).Select(
			"channel_id", "upstream_group_id", "canonical_group_name", "presence_generation", "model_name", "operation", "protocol", "endpoint_path",
			"task_mode", "idempotency_key_supported", "idempotency_header", "task_id_returned", "task_id_json_path", "task_query_supported",
			"task_query_method", "task_query_path_template", "task_status_json_path", "task_success_values", "task_failure_values", "evidence_source",
			"evidence_detail", "evidence_observed_at", "configured_by", "capability_version", "updated_at",
		).Updates(&capability).Error; err != nil {
			return err
		}
		if err := tx.First(&result, existing.ID).Error; err != nil {
			return err
		}
		return appendGatewayImageCapabilityAuditTx(tx, &result, imageCapabilityAuditAction(result.EvidenceSource), referenceKind, referenceID, actor)
	})
	if err != nil {
		return nil, err
	}
	decorateGatewayImageOperationCapability(&result)
	return &result, nil
}

func requireObservedImageCapabilityRequestLogTx(tx *gorm.DB, capability GatewayImageOperationCapability, requestLogID uint64) error {
	if requestLogID == 0 {
		return errors.New("observed image capability requires a request log")
	}
	var matches int64
	err := tx.Model(&GatewayRequestLog{}).Where(gatewayRequestRecordSQL("")).Where(`
		id = ? AND channel_id = ? AND lower(btrim(group_name)) = ? AND model_name = ? AND method = ? AND path = ? AND
		status_code >= ? AND status_code < ? AND COALESCE(error, '') = '' AND
		COALESCE(route_reason, '') NOT IN ?`,
		requestLogID, capability.ChannelID, capability.CanonicalGroupName, capability.ModelName, http.MethodPost, capability.EndpointPath,
		http.StatusOK, http.StatusMultipleChoices, []string{"latency_probe", "api_test", "model_check"},
	).Count(&matches).Error
	if err != nil {
		return err
	}
	if matches != 1 {
		return errors.New("observed image capability request log is not a successful normal request")
	}
	return nil
}

func (g *Gateway) FindCurrentImageOperationCapability(channelID uint, groupName, modelName, protocol string) (*GatewayImageOperationCapability, error) {
	items, err := g.ListCurrentImageOperationCapabilities(GatewayImageOperationCapabilityFilter{
		ChannelID: channelID, GroupName: groupName, ModelName: modelName, Protocol: protocol,
	})
	if err != nil || len(items) == 0 {
		return nil, err
	}
	return &items[0], nil
}

func (g *Gateway) ListCurrentImageOperationCapabilities(filter GatewayImageOperationCapabilityFilter) ([]GatewayImageOperationCapability, error) {
	query := g.db.Table("gateway_image_operation_capabilities AS capabilities").
		Select("capabilities.*, channels.name AS channel_name").
		Joins("JOIN gateway_upstream_groups AS groups ON groups.id = capabilities.upstream_group_id AND groups.present = TRUE AND groups.presence_generation = capabilities.presence_generation").
		Joins("JOIN channels ON channels.id = capabilities.channel_id")
	if filter.ChannelID > 0 {
		query = query.Where("capabilities.channel_id = ?", filter.ChannelID)
	}
	if groupName := strings.TrimSpace(filter.GroupName); groupName != "" {
		canonical, err := canonicalUpstreamGroupName(g.db, groupName)
		if err != nil {
			return nil, err
		}
		query = query.Where("capabilities.canonical_group_name = ?", canonical)
	}
	if modelName := strings.TrimSpace(filter.ModelName); modelName != "" {
		query = query.Where("capabilities.model_name = ?", modelName)
	}
	if protocol := strings.TrimSpace(filter.Protocol); protocol != "" {
		query = query.Where("capabilities.protocol = ?", protocol)
	}
	items := []GatewayImageOperationCapability{}
	if err := query.Order("channels.name ASC, capabilities.canonical_group_name ASC, capabilities.model_name ASC, capabilities.protocol ASC").Find(&items).Error; err != nil {
		return nil, err
	}
	for index := range items {
		decorateGatewayImageOperationCapability(&items[index])
	}
	return items, nil
}

func (g *Gateway) ListImageOperationCapabilityAudits(capabilityID uint) ([]GatewayImageOperationCapabilityAudit, error) {
	if capabilityID == 0 {
		return nil, errors.New("image operation capability id is required")
	}
	var capability GatewayImageOperationCapability
	if err := g.db.Select("id").First(&capability, capabilityID).Error; err != nil {
		return nil, err
	}
	query := g.db.Model(&GatewayImageOperationCapabilityAudit{}).Where("capability_id = ?", capabilityID)
	items := []GatewayImageOperationCapabilityAudit{}
	err := query.Order("created_at DESC, id DESC").Limit(gatewayImageCapabilityAuditListLimit).Find(&items).Error
	return items, err
}

func normalizeGatewayImageOperationCapability(capability *GatewayImageOperationCapability) error {
	if capability == nil {
		return errors.New("image operation capability is required")
	}
	capability.ModelName = strings.TrimSpace(capability.ModelName)
	capability.Operation = strings.ToLower(strings.TrimSpace(capability.Operation))
	capability.Protocol = strings.ToLower(strings.TrimSpace(capability.Protocol))
	capability.EndpointPath = strings.TrimSpace(capability.EndpointPath)
	capability.IdempotencyHeader = strings.TrimSpace(capability.IdempotencyHeader)
	capability.TaskIDJSONPath = strings.TrimSpace(capability.TaskIDJSONPath)
	capability.TaskQueryMethod = strings.ToUpper(strings.TrimSpace(capability.TaskQueryMethod))
	capability.TaskQueryPathTemplate = strings.TrimSpace(capability.TaskQueryPathTemplate)
	capability.TaskStatusJSONPath = strings.TrimSpace(capability.TaskStatusJSONPath)
	capability.EvidenceSource = strings.ToLower(strings.TrimSpace(capability.EvidenceSource))
	capability.EvidenceDetail = strings.ToLower(strings.TrimSpace(capability.EvidenceDetail))
	capability.ConfiguredBy = strings.TrimSpace(capability.ConfiguredBy)
	if capability.ChannelID == 0 || capability.ModelName == "" {
		return errors.New("channel_id and model_name are required")
	}
	if !utf8.ValidString(capability.ModelName) || utf8.RuneCountInString(capability.ModelName) > 256 {
		return errors.New("model_name exceeds 256 characters")
	}
	if capability.Operation == "" {
		capability.Operation = GatewayImageOperationGeneration
	}
	if capability.Operation != GatewayImageOperationGeneration {
		return errors.New("unsupported image operation")
	}
	expectedEndpoint := ""
	switch capability.Protocol {
	case GatewayImageProtocolImages:
		expectedEndpoint = "/v1/images/generations"
	case GatewayImageProtocolResponses:
		expectedEndpoint = "/v1/responses"
	default:
		return errors.New("unsupported image operation protocol")
	}
	if capability.EndpointPath == "" {
		capability.EndpointPath = expectedEndpoint
	}
	if capability.EndpointPath != expectedEndpoint {
		return errors.New("image operation endpoint does not match protocol")
	}
	if capability.TaskMode == "" {
		capability.TaskMode = GatewayImageTaskModeUnknown
	}
	switch capability.TaskMode {
	case GatewayImageTaskModeUnknown, GatewayImageTaskModeSync, GatewayImageTaskModeAsync, GatewayImageTaskModeSyncAsync:
	default:
		return errors.New("task_mode must be unknown, sync, async, or sync_async")
	}
	if capability.IdempotencyKeySupported {
		if !validImageCapabilityHeader(capability.IdempotencyHeader) {
			return errors.New("idempotency_header is invalid")
		}
		capability.IdempotencyHeader = textproto.CanonicalMIMEHeaderKey(capability.IdempotencyHeader)
	} else {
		capability.IdempotencyHeader = ""
	}
	if capability.TaskIDReturned {
		if !validImageCapabilityJSONPath(capability.TaskIDJSONPath) {
			return errors.New("task_id_json_path is invalid")
		}
	} else {
		capability.TaskIDJSONPath = ""
	}
	if capability.TaskQuerySupported {
		if capability.TaskQueryMethod != httpMethodGet || !validImageCapabilityTaskQueryPath(capability.TaskQueryPathTemplate) || !validImageCapabilityJSONPath(capability.TaskStatusJSONPath) {
			return errors.New("task query requires GET, a relative {task_id} path, and a status JSON path")
		}
	} else {
		capability.TaskQueryMethod = ""
		capability.TaskQueryPathTemplate = ""
		capability.TaskStatusJSONPath = ""
		capability.TaskSuccessValues = []string{}
		capability.TaskFailureValues = []string{}
	}
	var err error
	if capability.TaskSuccessValues, err = normalizeImageCapabilityStates(capability.TaskSuccessValues); err != nil {
		return fmt.Errorf("task_success_values: %w", err)
	}
	if capability.TaskFailureValues, err = normalizeImageCapabilityStates(capability.TaskFailureValues); err != nil {
		return fmt.Errorf("task_failure_values: %w", err)
	}
	if overlapping := overlappingImageCapabilityStates(capability.TaskSuccessValues, capability.TaskFailureValues); overlapping != "" {
		return fmt.Errorf("task state %q cannot be both successful and failed", overlapping)
	}
	if !imageCapabilityEvidenceSourceAllowed(capability.EvidenceSource) {
		return errors.New("unsupported image capability evidence source")
	}
	if capability.EvidenceObservedAt.IsZero() {
		return errors.New("evidence_observed_at is required")
	}
	capability.EvidenceObservedAt = capability.EvidenceObservedAt.UTC()
	if !imageCapabilityEvidenceDetailAllowed(capability.EvidenceSource, capability.EvidenceDetail) {
		return errors.New("unsupported image capability evidence detail")
	}
	return nil
}

const httpMethodGet = "GET"

func validImageCapabilityHeader(value string) bool {
	if value == "" || len(value) > 128 || !imageCapabilityHeaderPattern.MatchString(value) {
		return false
	}
	switch strings.ToLower(value) {
	case "authorization", "proxy-authorization", "proxy-authenticate", "cookie", "set-cookie", "x-api-key", "api-key",
		"host", "content-length", "connection", "keep-alive", "te", "trailer", "transfer-encoding", "upgrade":
		return false
	default:
		return true
	}
}

func validImageCapabilityJSONPath(value string) bool {
	return value != "" && len(value) <= 256 && imageCapabilityJSONPathPattern.MatchString(value)
}

func validImageCapabilityTaskQueryPath(value string) bool {
	if value == "" || len(value) > 512 || strings.Count(value, "{task_id}") != 1 {
		return false
	}
	for _, char := range value {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	staticPath := strings.Replace(value, "{task_id}", "", 1)
	return strings.HasPrefix(value, "/") && !strings.HasPrefix(value, "//") &&
		!strings.Contains(value, "://") && !strings.Contains(value, "\\") && !strings.Contains(value, "?") && !strings.Contains(value, "#") &&
		!strings.Contains(value, "..") && !strings.ContainsAny(value, " \t") && !strings.ContainsAny(staticPath, "{}%") &&
		!strings.Contains(value, "\r") && !strings.Contains(value, "\n")
}

func normalizeImageCapabilityStates(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if !imageCapabilityStatePattern.MatchString(value) {
			return nil, fmt.Errorf("invalid state %q", value)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
		if len(out) > 16 {
			return nil, errors.New("too many states")
		}
	}
	sort.Strings(out)
	return out, nil
}

func overlappingImageCapabilityStates(successValues, failureValues []string) string {
	success := make(map[string]struct{}, len(successValues))
	for _, value := range successValues {
		success[value] = struct{}{}
	}
	for _, value := range failureValues {
		if _, ok := success[value]; ok {
			return value
		}
	}
	return ""
}

func imageCapabilityEvidenceSourceAllowed(source string) bool {
	switch source {
	case GatewayImageEvidenceObservedSuccess, GatewayImageEvidenceProviderMetadata, GatewayImageEvidenceAdminConfig:
		return true
	default:
		return false
	}
}

func imageCapabilityEvidenceDetailAllowed(source, detail string) bool {
	switch source {
	case GatewayImageEvidenceObservedSuccess:
		return detail == "observed_sync_response" || detail == "observed_async_task"
	case GatewayImageEvidenceProviderMetadata:
		return detail == "provider_metadata"
	case GatewayImageEvidenceAdminConfig:
		return detail == "administrator_configuration"
	default:
		return false
	}
}

func imageCapabilityEvidenceRank(source string) int {
	switch source {
	case GatewayImageEvidenceAdminConfig:
		return 3
	case GatewayImageEvidenceProviderMetadata:
		return 2
	case GatewayImageEvidenceObservedSuccess:
		return 1
	default:
		return 0
	}
}

func imageCapabilityAuditAction(source string) string {
	switch source {
	case GatewayImageEvidenceAdminConfig:
		return "configured"
	case GatewayImageEvidenceProviderMetadata:
		return "provider_metadata"
	default:
		return "observed"
	}
}

func mergeObservedImageCapability(next *GatewayImageOperationCapability, existing GatewayImageOperationCapability) {
	if next == nil {
		return
	}
	next.TaskMode = mergeImageTaskModes(existing.TaskMode, next.TaskMode)
	if existing.IdempotencyKeySupported && !next.IdempotencyKeySupported {
		next.IdempotencyKeySupported = true
		next.IdempotencyHeader = existing.IdempotencyHeader
	}
	if existing.TaskIDReturned && !next.TaskIDReturned && imageTaskModeSupportsAsync(next.TaskMode) {
		next.TaskIDReturned = true
		next.TaskIDJSONPath = existing.TaskIDJSONPath
	}
}

func mergeImageTaskModes(existing, observed GatewayImageTaskMode) GatewayImageTaskMode {
	if existing == GatewayImageTaskModeSyncAsync || observed == GatewayImageTaskModeSyncAsync {
		return GatewayImageTaskModeSyncAsync
	}
	if existing == GatewayImageTaskModeUnknown {
		return observed
	}
	if observed == GatewayImageTaskModeUnknown || existing == observed {
		return existing
	}
	return GatewayImageTaskModeSyncAsync
}

func imageTaskModeSupportsAsync(mode GatewayImageTaskMode) bool {
	return mode == GatewayImageTaskModeAsync || mode == GatewayImageTaskModeSyncAsync
}

func appendGatewayImageCapabilityAuditTx(tx *gorm.DB, capability *GatewayImageOperationCapability, action, referenceKind string, referenceID uint64, actor string) error {
	if capability == nil {
		return errors.New("image capability audit requires capability")
	}
	capabilityID := capability.ID
	audit := GatewayImageOperationCapabilityAudit{
		CapabilityID: &capabilityID, ChannelID: capability.ChannelID, UpstreamGroupID: capability.UpstreamGroupID,
		CanonicalGroupName: capability.CanonicalGroupName, PresenceGeneration: capability.PresenceGeneration,
		ModelName: capability.ModelName, Operation: capability.Operation, Protocol: capability.Protocol,
		Action: action, EvidenceSource: capability.EvidenceSource, EvidenceObservedAt: capability.EvidenceObservedAt,
		EvidenceReferenceKind: referenceKind, EvidenceReferenceID: referenceID, Actor: normalizeImageCapabilityActor(actor),
		CapabilityVersion: capability.CapabilityVersion, Snapshot: imageCapabilityAuditSnapshot(capability), CreatedAt: time.Now().UTC(),
	}
	return tx.Create(&audit).Error
}

func imageCapabilityAuditSnapshot(capability *GatewayImageOperationCapability) map[string]any {
	return map[string]any{
		"endpoint_path": capability.EndpointPath, "task_mode": capability.TaskMode,
		"idempotency_key_supported": capability.IdempotencyKeySupported, "idempotency_header": capability.IdempotencyHeader,
		"task_id_returned": capability.TaskIDReturned, "task_id_json_path": capability.TaskIDJSONPath,
		"task_query_supported": capability.TaskQuerySupported, "task_query_method": capability.TaskQueryMethod,
		"task_query_path_template": capability.TaskQueryPathTemplate, "task_status_json_path": capability.TaskStatusJSONPath,
		"task_success_values": append([]string(nil), capability.TaskSuccessValues...),
		"task_failure_values": append([]string(nil), capability.TaskFailureValues...),
		"evidence_detail":     capability.EvidenceDetail, "automatic_replay_allowed": false,
	}
}

func decorateGatewayImageOperationCapability(capability *GatewayImageOperationCapability) {
	if capability == nil {
		return
	}
	capability.RecoveryComplete = false
	capability.AutomaticReplayAllowed = false
	blockers := make([]string, 0, 6)
	if !imageTaskModeSupportsAsync(capability.TaskMode) {
		blockers = append(blockers, "async_task_mode_not_confirmed")
	}
	if !capability.IdempotencyKeySupported || capability.IdempotencyHeader == "" {
		blockers = append(blockers, "idempotency_key_not_confirmed")
	}
	if !capability.TaskIDReturned || capability.TaskIDJSONPath == "" {
		blockers = append(blockers, "task_id_not_confirmed")
	}
	if !capability.TaskQuerySupported || capability.TaskQueryMethod != httpMethodGet || capability.TaskQueryPathTemplate == "" {
		blockers = append(blockers, "task_query_not_confirmed")
	}
	if capability.TaskStatusJSONPath == "" || len(capability.TaskSuccessValues) == 0 || len(capability.TaskFailureValues) == 0 {
		blockers = append(blockers, "terminal_state_mapping_incomplete")
	}
	// This blocker is deliberately unconditional until durable task recovery,
	// terminal interpretation, and response reconstruction are implemented.
	blockers = append(blockers, "gateway_task_recovery_not_implemented")
	capability.RecoveryBlockers = blockers
}

func normalizeImageCapabilityActor(actor string) string {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return "system"
	}
	if utf8.RuneCountInString(actor) > 128 {
		return string([]rune(actor)[:128])
	}
	return actor
}
