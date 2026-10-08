package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Harvey-Mei/BridgeOps/backend/gateway"
	"github.com/Harvey-Mei/BridgeOps/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type gatewayImageOperationCapabilityInput struct {
	ChannelID                  uint                         `json:"channel_id"`
	ExpectedUpstreamGroupID    uint                         `json:"expected_upstream_group_id"`
	ExpectedPresenceGeneration uint64                       `json:"expected_presence_generation"`
	GroupName                  string                       `json:"group_name"`
	ModelName                  string                       `json:"model_name"`
	Operation                  string                       `json:"operation"`
	Protocol                   string                       `json:"protocol"`
	EndpointPath               string                       `json:"endpoint_path"`
	TaskMode                   storage.GatewayImageTaskMode `json:"task_mode"`
	IdempotencyKeySupported    bool                         `json:"idempotency_key_supported"`
	IdempotencyHeader          string                       `json:"idempotency_header"`
	TaskIDReturned             bool                         `json:"task_id_returned"`
	TaskIDJSONPath             string                       `json:"task_id_json_path"`
	TaskQuerySupported         bool                         `json:"task_query_supported"`
	TaskQueryMethod            string                       `json:"task_query_method"`
	TaskQueryPathTemplate      string                       `json:"task_query_path_template"`
	TaskStatusJSONPath         string                       `json:"task_status_json_path"`
	TaskSuccessValues          []string                     `json:"task_success_values"`
	TaskFailureValues          []string                     `json:"task_failure_values"`
	ExpectedVersion            uint64                       `json:"expected_version"`
}

func listGatewayImageOperationCapabilities(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errors.New("gateway storage is unavailable"))
		return
	}
	filter := storage.GatewayImageOperationCapabilityFilter{}
	if value := strings.TrimSpace(c.Query("channel_id")); value != "" {
		id, err := strconv.ParseUint(value, 10, 64)
		if err != nil || id == 0 || id > uint64(^uint(0)) {
			fail(c, http.StatusBadRequest, errors.New("channel_id is invalid"))
			return
		}
		filter.ChannelID = uint(id)
	}
	filter.GroupName = c.Query("group_name")
	filter.ModelName = c.Query("model")
	filter.Protocol = c.Query("protocol")
	items, err := d.Gateway.Store.ListCurrentImageOperationCapabilities(filter)
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": items})
}

func updateGatewayImageOperationCapability(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errors.New("gateway storage is unavailable"))
		return
	}
	var in gatewayImageOperationCapabilityInput
	if err := c.ShouldBindJSON(&in); err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	groupName := strings.TrimSpace(in.GroupName)
	modelName := strings.TrimSpace(in.ModelName)
	if in.ChannelID == 0 || in.ExpectedUpstreamGroupID == 0 || in.ExpectedPresenceGeneration == 0 || groupName == "" || modelName == "" {
		fail(c, http.StatusBadRequest, errors.New("channel_id, expected upstream group generation, group_name, and model_name are required"))
		return
	}
	actor, err := gatewayAdministratorActor(c, d)
	if err != nil {
		fail(c, http.StatusForbidden, err)
		return
	}
	generation, err := d.Gateway.Store.CaptureUpstreamGroupGeneration(in.ChannelID, groupName)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		fail(c, http.StatusNotFound, errors.New("upstream group is not currently present"))
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	if generation.ID != in.ExpectedUpstreamGroupID || generation.PresenceGeneration != in.ExpectedPresenceGeneration {
		fail(c, http.StatusConflict, errors.New("upstream group changed; refresh capability data"))
		return
	}
	capability := storage.GatewayImageOperationCapability{
		ChannelID: in.ChannelID, CanonicalGroupName: generation.CanonicalGroupName, ModelName: modelName,
		Operation: in.Operation, Protocol: in.Protocol, EndpointPath: in.EndpointPath, TaskMode: in.TaskMode,
		IdempotencyKeySupported: in.IdempotencyKeySupported, IdempotencyHeader: in.IdempotencyHeader,
		TaskIDReturned: in.TaskIDReturned, TaskIDJSONPath: in.TaskIDJSONPath,
		TaskQuerySupported: in.TaskQuerySupported, TaskQueryMethod: in.TaskQueryMethod,
		TaskQueryPathTemplate: in.TaskQueryPathTemplate, TaskStatusJSONPath: in.TaskStatusJSONPath,
		TaskSuccessValues: in.TaskSuccessValues, TaskFailureValues: in.TaskFailureValues,
	}
	result, err := d.Gateway.Store.SaveImageOperationCapabilityConfig(capability, generation, in.ExpectedVersion, actor)
	if errors.Is(err, storage.ErrGatewayImageCapabilityVersionConflict) {
		fail(c, http.StatusConflict, err)
		return
	}
	if errors.Is(err, storage.ErrUpstreamGroupGenerationChanged) {
		fail(c, http.StatusConflict, errors.New("upstream group changed; refresh capability data"))
		return
	}
	if err != nil {
		fail(c, http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": result})
}

func listGatewayImageOperationTargets(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errors.New("gateway storage is unavailable"))
		return
	}
	pageSize := 250
	if raw := strings.TrimSpace(c.Query("page_size")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			fail(c, http.StatusBadRequest, storage.ErrGatewayImageOperationTargetInventoryPageSizeInvalid)
			return
		}
		pageSize = parsed
	}
	page, err := d.Gateway.Store.ListImageOperationTargetInventoryPage(c.Query("cursor"), pageSize)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrGatewayImageOperationTargetInventoryCursorInvalid), errors.Is(err, storage.ErrGatewayImageOperationTargetInventoryPageSizeInvalid):
			fail(c, http.StatusBadRequest, err)
		case errors.Is(err, storage.ErrGatewayImageOperationTargetInventorySnapshotChanged):
			fail(c, http.StatusConflict, err)
		default:
			fail(c, http.StatusInternalServerError, err)
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": page})
}

func listGatewayImageOperationCapabilityAudits(c *gin.Context, d *Deps) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil {
		fail(c, http.StatusInternalServerError, errors.New("gateway storage is unavailable"))
		return
	}
	id, err := uintParam(c, "id")
	if err != nil || id == 0 {
		fail(c, http.StatusBadRequest, errors.New("image operation capability id is invalid"))
		return
	}
	items, err := d.Gateway.Store.ListImageOperationCapabilityAudits(id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		fail(c, http.StatusNotFound, errors.New("image operation capability was not found"))
		return
	}
	if err != nil {
		fail(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": items})
}

// observeGatewayImageOperationSuccess is deliberately called only after the
// normal gateway request has completed successfully. It never sends another
// request and stores no response body or task identifier.
func observeGatewayImageOperationSuccess(d *Deps, request *http.Request, candidate gateway.Candidate, model string, requestBody, nonStreamingBody, streamedBody []byte, responseHeaders http.Header, requestLogID uint) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil || request == nil || requestLogID == 0 || candidate.Channel.ID == 0 || strings.TrimSpace(model) == "" {
		return
	}
	protocol, endpoint, detail, taskMode, taskIDPath, statusPath, found := parseGatewayImageCapabilityEvidence(request.URL.Path, requestBody, nonStreamingBody, streamedBody)
	if !found {
		return
	}
	recordGatewayImageOperationCapabilityEvidence(d, request, candidate, model, responseHeaders, requestLogID, protocol, endpoint, detail, taskMode, taskIDPath, statusPath)
}

func observeGatewayImageOperationSuccessFromFacts(d *Deps, request *http.Request, candidate gateway.Candidate, model string, requestBody []byte, facts gatewayBufferedResponseImageFacts, responseHeaders http.Header, requestLogID uint) {
	if request == nil {
		return
	}
	protocol := ""
	endpoint := request.URL.Path
	detail := ""
	taskMode := storage.GatewayImageTaskMode("")
	taskIDPath := ""
	statusPath := ""
	switch request.URL.Path {
	case "/v1/images/generations":
		protocol = storage.GatewayImageProtocolImages
		if facts.DataNonEmpty {
			detail = "observed_sync_response"
			taskMode = storage.GatewayImageTaskModeSync
		} else if task, ok := firstAcceptedGatewayBufferedImageTask(facts.Tasks); ok {
			detail = "observed_async_task"
			taskMode = storage.GatewayImageTaskModeAsync
			taskIDPath = task.IDPath
			statusPath = task.StatusPath
		}
	case "/v1/responses":
		if !isGatewayImageGenerationRequest(request.URL.Path, requestBody) {
			return
		}
		protocol = storage.GatewayImageProtocolResponses
		var payload struct {
			Background bool `json:"background"`
		}
		if json.Unmarshal(requestBody, &payload) != nil {
			return
		}
		if payload.Background {
			if task, ok := firstAcceptedGatewayBufferedImageTask(facts.Tasks); ok {
				detail = "observed_async_task"
				taskMode = storage.GatewayImageTaskModeAsync
				taskIDPath = task.IDPath
				statusPath = task.StatusPath
			}
		} else if facts.ResponsesCompletedImageResult {
			detail = "observed_sync_response"
			taskMode = storage.GatewayImageTaskModeSync
		}
	default:
		return
	}
	if detail == "" {
		return
	}
	recordGatewayImageOperationCapabilityEvidence(d, request, candidate, model, responseHeaders, requestLogID, protocol, endpoint, detail, taskMode, taskIDPath, statusPath)
}

func firstAcceptedGatewayBufferedImageTask(tasks []gatewayBufferedResponseTaskFact) (gatewayBufferedResponseTaskFact, bool) {
	for _, task := range tasks {
		if strings.TrimSpace(task.ID) != "" && isAcceptedImageTaskStatus(task.Status) {
			return task, true
		}
	}
	return gatewayBufferedResponseTaskFact{}, false
}

func recordGatewayImageOperationCapabilityEvidence(d *Deps, request *http.Request, candidate gateway.Candidate, model string, responseHeaders http.Header, requestLogID uint, protocol, endpoint, detail string, taskMode storage.GatewayImageTaskMode, taskIDPath, statusPath string) {
	if d == nil || d.Gateway == nil || d.Gateway.Store == nil || request == nil || requestLogID == 0 || candidate.Channel.ID == 0 || strings.TrimSpace(model) == "" {
		return
	}
	capability := storage.GatewayImageOperationCapability{
		ChannelID: candidate.Channel.ID, ModelName: strings.TrimSpace(model), Operation: storage.GatewayImageOperationGeneration,
		Protocol: protocol, EndpointPath: endpoint, TaskMode: taskMode, EvidenceDetail: detail,
		EvidenceObservedAt: time.Now().UTC(), TaskIDReturned: taskIDPath != "", TaskIDJSONPath: taskIDPath,
		TaskStatusJSONPath: statusPath,
	}
	if idempotencyHeader, ok := observedImageIdempotencyHeader(request.Header, responseHeaders); ok {
		capability.IdempotencyKeySupported = true
		capability.IdempotencyHeader = idempotencyHeader
	}
	generation := candidate.UpstreamGeneration
	if !generation.Valid() {
		return
	}
	if _, err := d.Gateway.Store.ObserveImageOperationCapability(capability, generation, requestLogID); err != nil && d.Log != nil {
		d.Log.Warn("record gateway image operation capability evidence failed",
			"channel_id", candidate.Channel.ID,
			"group", generation.CanonicalGroupName,
			"presence_generation", generation.PresenceGeneration,
			"model", strings.TrimSpace(model),
			"request_log_id", requestLogID,
			"err", err,
		)
	}
}

func observedImageIdempotencyHeader(requestHeaders, responseHeaders http.Header) (string, bool) {
	if requestHeaders == nil || strings.TrimSpace(requestHeaders.Get("Idempotency-Key")) == "" || responseHeaders == nil {
		return "", false
	}
	for _, key := range []string{"Idempotency-Status", "X-Idempotency-Status", "Idempotency-Replayed", "X-Idempotency-Replayed"} {
		if strings.TrimSpace(responseHeaders.Get(key)) != "" {
			return "Idempotency-Key", true
		}
	}
	return "", false
}

func parseGatewayImageCapabilityEvidence(path string, requestBody, nonStreamingBody, streamedBody []byte) (protocol, endpoint, detail string, taskMode storage.GatewayImageTaskMode, taskIDPath, statusPath string, found bool) {
	background := false
	switch path {
	case "/v1/images/generations":
		protocol, endpoint = storage.GatewayImageProtocolImages, path
	case "/v1/responses":
		var request struct {
			Background bool `json:"background"`
		}
		if json.Unmarshal(requestBody, &request) != nil || !isGatewayImageGenerationRequest(path, requestBody) {
			return "", "", "", "", "", "", false
		}
		background = request.Background
		protocol, endpoint = storage.GatewayImageProtocolResponses, path
	default:
		return "", "", "", "", "", "", false
	}
	roots := imageCapabilityResponseRoots(nonStreamingBody)
	if len(roots) == 0 {
		roots = imageCapabilityResponseRoots(streamedBody)
	}
	if path == "/v1/images/generations" {
		for _, root := range roots {
			if data, ok := root["data"].([]any); ok && len(data) > 0 {
				return protocol, endpoint, "observed_sync_response", storage.GatewayImageTaskModeSync, "", "", true
			}
		}
		for _, root := range roots {
			if taskIDPath, statusPath, status, ok := imageCapabilityTaskEnvelope(root, true); ok && isAcceptedImageTaskStatus(status) {
				return protocol, endpoint, "observed_async_task", storage.GatewayImageTaskModeAsync, taskIDPath, statusPath, true
			}
		}
		return "", "", "", "", "", "", false
	}
	if background {
		for _, root := range roots {
			for _, response := range imageCapabilityResponseObjects(root) {
				if taskIDPath, statusPath, status, ok := imageCapabilityTaskEnvelope(response.value, true); ok && isAcceptedImageTaskStatus(status) {
					return protocol, endpoint, "observed_async_task", storage.GatewayImageTaskModeAsync, response.path + taskIDPath, response.path + statusPath, true
				}
			}
		}
		return "", "", "", "", "", "", false
	}
	// Ordinary streaming Responses emit response.created / in_progress frames
	// before the completed response. Those lifecycle events are not asynchronous
	// task acceptance. Scan the whole captured tail and require a completed image
	// result before recording synchronous support.
	for _, root := range roots {
		for _, response := range imageCapabilityResponseObjects(root) {
			if imageCapabilityResponseIsCompleted(response.value) {
				return protocol, endpoint, "observed_sync_response", storage.GatewayImageTaskModeSync, "", "", true
			}
		}
	}
	return "", "", "", "", "", "", false
}

type imageCapabilityResponseObject struct {
	value map[string]any
	path  string
}

func imageCapabilityResponseObjects(root map[string]any) []imageCapabilityResponseObject {
	items := []imageCapabilityResponseObject{{value: root}}
	if response, ok := root["response"].(map[string]any); ok {
		items = append(items, imageCapabilityResponseObject{value: response, path: "response."})
	}
	return items
}

func imageCapabilityResponseRoots(raw []byte) []map[string]any {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil
	}
	var root map[string]any
	if json.Unmarshal(raw, &root) == nil && root != nil {
		return []map[string]any{root}
	}
	roots := make([]map[string]any, 0)
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		var item map[string]any
		if json.Unmarshal(payload, &item) == nil && item != nil {
			roots = append(roots, item)
		}
	}
	return roots
}

func imageCapabilityTaskEnvelope(root map[string]any, allowGenericRootID bool) (idPath, statusPath, status string, ok bool) {
	for _, candidate := range []struct {
		value          any
		path           string
		allowGenericID bool
	}{
		{root, "", allowGenericRootID},
		{root["task"], "task", true},
		{root["job"], "job", true},
		{root["request"], "request", true},
	} {
		obj, isObject := candidate.value.(map[string]any)
		if !isObject {
			continue
		}
		idKey := ""
		idKeys := []string{"task_id"}
		if candidate.allowGenericID {
			idKeys = append(idKeys, "id", "request_id")
		}
		for _, key := range idKeys {
			if value, exists := obj[key].(string); exists && strings.TrimSpace(value) != "" {
				idKey = key
				break
			}
		}
		statusValue := ""
		statusKey := ""
		for _, key := range []string{"status", "state"} {
			if value, exists := obj[key].(string); exists && strings.TrimSpace(value) != "" {
				statusValue = strings.ToLower(strings.TrimSpace(value))
				statusKey = key
				break
			}
		}
		if idKey != "" && statusKey != "" {
			prefix := candidate.path
			if prefix != "" {
				prefix += "."
			}
			return prefix + idKey, prefix + statusKey, statusValue, true
		}
	}
	return "", "", "", false
}

func isAcceptedImageTaskStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "queued", "pending", "submitted", "in_progress", "processing", "running", "created", "completed", "succeeded":
		return true
	default:
		return false
	}
}

func imageCapabilityResponseIsCompleted(root map[string]any) bool {
	status, _ := root["status"].(string)
	if !strings.EqualFold(strings.TrimSpace(status), "completed") {
		return false
	}
	if output, ok := root["output"].([]any); ok {
		for _, item := range output {
			if object, ok := item.(map[string]any); ok {
				itemType, _ := object["type"].(string)
				if !strings.EqualFold(strings.TrimSpace(itemType), "image_generation_call") {
					continue
				}
				itemStatus, _ := object["status"].(string)
				if itemStatus = strings.ToLower(strings.TrimSpace(itemStatus)); itemStatus != "" && itemStatus != "completed" && itemStatus != "succeeded" {
					continue
				}
				if result, _ := object["result"].(string); strings.TrimSpace(result) != "" {
					return true
				}
			}
		}
	}
	return false
}
