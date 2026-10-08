package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

var (
	errGatewayResponseProjectionInvalid  = errors.New("invalid structured upstream response")
	errGatewayResponseProjectionTooLarge = errors.New("structured upstream response metadata exceeds limit")
)

const (
	gatewayResponseProjectionMaxDepth               = 64
	gatewayResponseProjectionMaxKeyBytes            = 256
	gatewayResponseProjectionMaxStringBytes         = 2048
	gatewayResponseProjectionMaxIdentifierBytes     = 512
	gatewayResponseProjectionMaxClassificationBytes = 64 << 10
	gatewayResponseProjectionMaxTaskFacts           = 16
	gatewayResponseProjectionContextCheckInterval   = 4 << 10
)

type gatewayUpstreamResponseProjectionOptions struct {
	RequestPath string
}

type gatewayBufferedResponseTaskFact struct {
	EnvelopePath string
	IDPath       string
	StatusPath   string
	ID           string
	Status       string
}

type gatewayBufferedResponseImageFacts struct {
	DataNonEmpty                  bool
	Tasks                         []gatewayBufferedResponseTaskFact
	ResponsesCompletedImageResult bool
}

type gatewayBufferedResponseFacts struct {
	Usage                gatewayUsageMetrics
	UsageCostPresent     bool
	ClassificationJSON   []byte
	StructuredErrorFound bool
	ResponseStatus       string
	Image                gatewayBufferedResponseImageFacts
}

type gatewayProjectionScalarKind uint8

const (
	gatewayProjectionScalarNone gatewayProjectionScalarKind = iota
	gatewayProjectionScalarString
	gatewayProjectionScalarNumber
	gatewayProjectionScalarBool
	gatewayProjectionScalarNull
)

type gatewayProjectionScalar struct {
	kind     gatewayProjectionScalarKind
	text     string
	boolean  bool
	nonEmpty bool
	captured bool
}

func (s gatewayProjectionScalar) any() any {
	switch s.kind {
	case gatewayProjectionScalarString:
		return s.text
	case gatewayProjectionScalarNumber:
		return json.Number(s.text)
	case gatewayProjectionScalarBool:
		return s.boolean
	case gatewayProjectionScalarNull:
		return nil
	default:
		return nil
	}
}

type gatewayProjectionValueSummary struct {
	scalar   gatewayProjectionScalar
	nonEmpty bool
}

type gatewayProjectionCaptureMode struct {
	capture         bool
	presenceOnly    bool
	maxBytes        int
	discardOverflow bool
}

type gatewayProjectionObjectState struct {
	fields map[string]gatewayProjectionScalar
}

type gatewayProjectedUsageScope struct {
	seen    bool
	values  map[string]gatewayProjectionScalar
	details map[string]gatewayProjectionScalar
}

type gatewayResponseProjectionParser struct {
	ctx     context.Context
	reader  *bufio.Reader
	read    int64
	options gatewayUpstreamResponseProjectionOptions

	facts gatewayBufferedResponseFacts

	rootClass     map[string]any
	responseClass map[string]any

	rootUsage        gatewayProjectedUsageScope
	responseUsage    gatewayProjectedUsageScope
	rootFallback     gatewayProjectedUsageScope
	responseFallback gatewayProjectedUsageScope

	rootResponseStatus       string
	responseModels           map[string]string
	nestedResponseStatus     string
	rootCompletedImageItem   bool
	nestedCompletedImageItem bool
}

func projectGatewayUpstreamResponse(ctx context.Context, spool *gatewayUpstreamResponseSpool, options gatewayUpstreamResponseProjectionOptions) (gatewayBufferedResponseFacts, error) {
	if ctx == nil || spool == nil {
		return gatewayBufferedResponseFacts{}, errGatewayResponseProjectionInvalid
	}
	if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
		return gatewayBufferedResponseFacts{}, err
	}
	reader, err := spool.Open()
	if err != nil {
		return gatewayBufferedResponseFacts{}, err
	}
	defer reader.Close()

	parser := &gatewayResponseProjectionParser{
		ctx:           ctx,
		reader:        bufio.NewReaderSize(reader, 32<<10),
		options:       options,
		rootClass:     make(map[string]any),
		responseClass: make(map[string]any),
	}
	if err := parser.parse(); err != nil {
		return gatewayBufferedResponseFacts{}, err
	}
	if err := gatewayUpstreamResponseSpoolContextError(ctx); err != nil {
		return gatewayBufferedResponseFacts{}, err
	}
	return parser.facts, nil
}

func (p *gatewayResponseProjectionParser) parse() error {
	if err := p.skipWhitespace(); err != nil {
		return err
	}
	if _, err := p.parseValue(nil, gatewayProjectionCaptureMode{}, 0); err != nil {
		return err
	}
	if err := p.skipWhitespace(); err != nil {
		return err
	}
	if _, err := p.peekByte(); err == nil {
		return fmt.Errorf("%w: trailing data", errGatewayResponseProjectionInvalid)
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	p.finishFacts()
	return gatewayUpstreamResponseSpoolContextError(p.ctx)
}

func (p *gatewayResponseProjectionParser) parseValue(path []string, mode gatewayProjectionCaptureMode, depth int) (gatewayProjectionValueSummary, error) {
	if depth > gatewayResponseProjectionMaxDepth {
		return gatewayProjectionValueSummary{}, fmt.Errorf("%w: nesting exceeds %d", errGatewayResponseProjectionTooLarge, gatewayResponseProjectionMaxDepth)
	}
	if err := p.skipWhitespace(); err != nil {
		return gatewayProjectionValueSummary{}, err
	}
	value, err := p.peekByte()
	if err != nil {
		return gatewayProjectionValueSummary{}, p.syntaxError("missing value", err)
	}
	switch value {
	case '{':
		return p.parseObject(path, depth+1)
	case '[':
		return p.parseArray(path, depth+1)
	case '"':
		text, nonEmpty, err := p.parseString(mode)
		return gatewayProjectionValueSummary{scalar: gatewayProjectionScalar{kind: gatewayProjectionScalarString, text: text, nonEmpty: nonEmpty, captured: mode.capture}, nonEmpty: nonEmpty}, err
	case 't':
		if err := p.consumeLiteral("true"); err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		return gatewayProjectionValueSummary{scalar: gatewayProjectionScalar{kind: gatewayProjectionScalarBool, boolean: true, nonEmpty: true, captured: mode.capture}, nonEmpty: true}, nil
	case 'f':
		if err := p.consumeLiteral("false"); err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		return gatewayProjectionValueSummary{scalar: gatewayProjectionScalar{kind: gatewayProjectionScalarBool, captured: mode.capture}}, nil
	case 'n':
		if err := p.consumeLiteral("null"); err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		return gatewayProjectionValueSummary{scalar: gatewayProjectionScalar{kind: gatewayProjectionScalarNull, captured: mode.capture}}, nil
	default:
		if value != '-' && (value < '0' || value > '9') {
			return gatewayProjectionValueSummary{}, p.syntaxError("invalid value", nil)
		}
		number, err := p.parseNumber(mode.capture && !mode.discardOverflow)
		if err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		return gatewayProjectionValueSummary{scalar: gatewayProjectionScalar{kind: gatewayProjectionScalarNumber, text: number, nonEmpty: true, captured: mode.capture}, nonEmpty: true}, nil
	}
}

func (p *gatewayResponseProjectionParser) parseObject(path []string, depth int) (gatewayProjectionValueSummary, error) {
	if err := p.expectByte('{'); err != nil {
		return gatewayProjectionValueSummary{}, err
	}
	state := gatewayProjectionObjectState{}
	if p.objectIsInteresting(path) {
		state.fields = make(map[string]gatewayProjectionScalar)
	}
	p.markObjectSeen(path)
	if err := p.skipWhitespace(); err != nil {
		return gatewayProjectionValueSummary{}, err
	}
	if value, err := p.peekByte(); err == nil && value == '}' {
		_, _ = p.readByte()
		p.finishObject(path, state, false)
		return gatewayProjectionValueSummary{}, nil
	}
	nonEmpty := false
	for {
		key, _, err := p.parseString(gatewayProjectionCaptureMode{capture: true, maxBytes: gatewayResponseProjectionMaxKeyBytes})
		if err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		if err := p.skipWhitespace(); err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		if err := p.expectByte(':'); err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		fieldPath, err := appendGatewayProjectionPath(path, key)
		if err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		mode := p.captureMode(path, key)
		summary, err := p.parseValue(fieldPath, mode, depth)
		if err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		nonEmpty = true
		if state.fields != nil && summary.scalar.captured {
			state.fields[key] = summary.scalar
		}
		p.recordScalar(fieldPath, summary.scalar)
		p.recordContainerField(path, key, summary)

		if err := p.skipWhitespace(); err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		separator, err := p.readByte()
		if err != nil {
			return gatewayProjectionValueSummary{}, p.syntaxError("unterminated object", err)
		}
		switch separator {
		case '}':
			p.finishObject(path, state, nonEmpty)
			return gatewayProjectionValueSummary{nonEmpty: nonEmpty}, nil
		case ',':
			if err := p.skipWhitespace(); err != nil {
				return gatewayProjectionValueSummary{}, err
			}
			if next, err := p.peekByte(); err != nil || next != '"' {
				return gatewayProjectionValueSummary{}, p.syntaxError("object key must be a string", err)
			}
		default:
			return gatewayProjectionValueSummary{}, p.syntaxError("expected object separator", nil)
		}
	}
}

func (p *gatewayResponseProjectionParser) parseArray(path []string, depth int) (gatewayProjectionValueSummary, error) {
	if err := p.expectByte('['); err != nil {
		return gatewayProjectionValueSummary{}, err
	}
	if err := p.skipWhitespace(); err != nil {
		return gatewayProjectionValueSummary{}, err
	}
	if value, err := p.peekByte(); err == nil && value == ']' {
		_, _ = p.readByte()
		return gatewayProjectionValueSummary{}, nil
	}
	nonEmpty := false
	for {
		if _, err := p.parseValue(path, gatewayProjectionCaptureMode{}, depth); err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		nonEmpty = true
		if gatewayProjectionPathEqual(path, "data") {
			p.facts.Image.DataNonEmpty = true
		}
		if err := p.skipWhitespace(); err != nil {
			return gatewayProjectionValueSummary{}, err
		}
		separator, err := p.readByte()
		if err != nil {
			return gatewayProjectionValueSummary{}, p.syntaxError("unterminated array", err)
		}
		switch separator {
		case ']':
			return gatewayProjectionValueSummary{nonEmpty: nonEmpty}, nil
		case ',':
			continue
		default:
			return gatewayProjectionValueSummary{}, p.syntaxError("expected array separator", nil)
		}
	}
}

func (p *gatewayResponseProjectionParser) captureMode(objectPath []string, key string) gatewayProjectionCaptureMode {
	if key == "model" && gatewayResponseModelPath(objectPath) {
		return gatewayProjectionCaptureMode{capture: true, maxBytes: 256, discardOverflow: true}
	}
	maxBytes := gatewayResponseProjectionMaxStringBytes
	if key == "id" || key == "task_id" || key == "request_id" {
		maxBytes = gatewayResponseProjectionMaxIdentifierBytes
	}
	if gatewayProjectionUsageObjectPath(objectPath) || gatewayProjectionUsageDetailsPath(objectPath) {
		if gatewayProjectionUsageField(key) {
			return gatewayProjectionCaptureMode{capture: true, maxBytes: 128}
		}
	}
	if gatewayProjectionFallbackUsagePath(objectPath) && gatewayProjectionUsageField(key) {
		return gatewayProjectionCaptureMode{capture: true, maxBytes: 128}
	}
	if gatewayProjectionClassificationPath(objectPath) && gatewayProjectionClassificationField(key) {
		return gatewayProjectionCaptureMode{capture: true, maxBytes: maxBytes}
	}
	if gatewayProjectionTaskEnvelopePath(objectPath) && gatewayProjectionTaskField(key) {
		return gatewayProjectionCaptureMode{capture: true, maxBytes: maxBytes}
	}
	if gatewayProjectionOutputItemPath(objectPath) {
		switch key {
		case "type", "status":
			return gatewayProjectionCaptureMode{capture: true, maxBytes: 128}
		case "result":
			return gatewayProjectionCaptureMode{capture: true, presenceOnly: true}
		}
	}
	return gatewayProjectionCaptureMode{}
}

func (p *gatewayResponseProjectionParser) objectIsInteresting(path []string) bool {
	return gatewayProjectionUsageObjectPath(path) || gatewayProjectionUsageDetailsPath(path) ||
		gatewayProjectionClassificationPath(path) || gatewayProjectionTaskEnvelopePath(path) ||
		gatewayProjectionOutputItemPath(path) || gatewayProjectionFallbackUsagePath(path)
}

func (p *gatewayResponseProjectionParser) markObjectSeen(path []string) {
	switch {
	case gatewayProjectionPathEqual(path, "usage"):
		p.rootUsage.seen = true
	case gatewayProjectionPathEqual(path, "response", "usage"):
		p.responseUsage.seen = true
	case len(path) == 0:
		p.rootFallback.seen = true
	case gatewayProjectionPathEqual(path, "response"):
		p.responseFallback.seen = true
	}
}

func (p *gatewayResponseProjectionParser) recordScalar(path []string, scalar gatewayProjectionScalar) {
	if !scalar.captured || len(path) == 0 {
		return
	}
	key := path[len(path)-1]
	objectPath := path[:len(path)-1]
	if key == "model" && gatewayResponseModelPath(objectPath) {
		if p.responseModels == nil {
			p.responseModels = make(map[string]string, 3)
		}
		p.responseModels[strings.Join(objectPath, ".")] = gatewayResponseModel(scalar.any())
	}
	var scope *gatewayProjectedUsageScope
	switch {
	case gatewayProjectionPathEqual(objectPath, "usage"):
		scope = &p.rootUsage
	case gatewayProjectionPathEqual(objectPath, "response", "usage"):
		scope = &p.responseUsage
	case gatewayProjectionPathEqual(objectPath, "usage", "input_tokens_details"), gatewayProjectionPathEqual(objectPath, "usage", "prompt_tokens_details"):
		scope = &p.rootUsage
		if scope.details == nil {
			scope.details = make(map[string]gatewayProjectionScalar)
		}
		scope.details[key] = scalar
		return
	case gatewayProjectionPathEqual(objectPath, "response", "usage", "input_tokens_details"), gatewayProjectionPathEqual(objectPath, "response", "usage", "prompt_tokens_details"):
		scope = &p.responseUsage
		if scope.details == nil {
			scope.details = make(map[string]gatewayProjectionScalar)
		}
		scope.details[key] = scalar
		return
	case len(objectPath) == 0:
		scope = &p.rootFallback
	case gatewayProjectionPathEqual(objectPath, "response"):
		scope = &p.responseFallback
	}
	if scope != nil && gatewayProjectionUsageField(key) {
		if scope.values == nil {
			scope.values = make(map[string]gatewayProjectionScalar)
		}
		scope.values[key] = scalar
	}
}

func (p *gatewayResponseProjectionParser) recordContainerField(objectPath []string, key string, summary gatewayProjectionValueSummary) {
	if !summary.scalar.captured && gatewayProjectionTokenField(key) {
		fieldPath := make([]string, 0, len(objectPath)+1)
		fieldPath = append(fieldPath, objectPath...)
		fieldPath = append(fieldPath, key)
		p.recordScalar(fieldPath, gatewayProjectionScalar{captured: true})
	}
	if key != "error" || !gatewayProjectionClassificationPath(objectPath) || summary.scalar.captured {
		return
	}
	if !summary.nonEmpty {
		return
	}
	p.facts.StructuredErrorFound = true
	switch {
	case len(objectPath) == 0:
		if _, exists := p.rootClass["error"]; !exists {
			p.rootClass["error"] = true
		}
	case gatewayProjectionPathEqual(objectPath, "response"):
		if _, exists := p.responseClass["error"]; !exists {
			p.responseClass["error"] = true
		}
	}
}

func (p *gatewayResponseProjectionParser) finishObject(path []string, state gatewayProjectionObjectState, nonEmpty bool) {
	fields := make(map[string]any)
	for key, scalar := range state.fields {
		fields[key] = scalar.any()
	}
	switch {
	case len(path) == 0:
		mergeGatewayProjectionMap(p.rootClass, gatewayProjectionClassificationMap(fields))
		p.rootResponseStatus = gatewayProjectionStringField(state.fields, "status")
		p.recordTaskFact(path, state.fields)
		p.markClassificationFromFields(fields)
	case gatewayProjectionPathEqual(path, "response"):
		mergeGatewayProjectionMap(p.responseClass, gatewayProjectionClassificationMap(fields))
		p.nestedResponseStatus = gatewayProjectionStringField(state.fields, "status")
		p.recordTaskFact(path, state.fields)
		p.markClassificationFromFields(fields)
	case gatewayProjectionPathEqual(path, "error"):
		if len(fields) > 0 {
			p.rootClass["error"] = fields
			p.facts.StructuredErrorFound = true
		} else if nonEmpty {
			p.rootClass["error"] = true
			p.facts.StructuredErrorFound = true
		}
	case gatewayProjectionPathEqual(path, "response", "error"):
		if len(fields) > 0 {
			p.responseClass["error"] = fields
			p.facts.StructuredErrorFound = true
		} else if nonEmpty {
			p.responseClass["error"] = true
			p.facts.StructuredErrorFound = true
		}
	default:
		if gatewayProjectionTaskEnvelopePath(path) {
			p.recordTaskFact(path, state.fields)
		}
		if gatewayProjectionOutputItemPath(path) {
			p.recordCompletedImageItem(path, state.fields)
		}
	}
}

func (p *gatewayResponseProjectionParser) markClassificationFromFields(fields map[string]any) {
	if value, exists := fields["error"]; exists && gatewayStreamErrorValuePresent(value) {
		p.facts.StructuredErrorFound = true
	}
	kind := strings.ToLower(strings.TrimSpace(gatewayProjectionAnyString(fields["type"])))
	if kind == "error" || strings.HasSuffix(kind, ".failed") {
		p.facts.StructuredErrorFound = true
	}
	status := strings.ToLower(strings.TrimSpace(gatewayProjectionAnyString(fields["status"])))
	if status == "failed" || status == "error" {
		p.facts.StructuredErrorFound = true
	}
}

func (p *gatewayResponseProjectionParser) recordTaskFact(path []string, fields map[string]gatewayProjectionScalar) {
	if len(p.facts.Image.Tasks) >= gatewayResponseProjectionMaxTaskFacts {
		return
	}
	idKey := ""
	for _, key := range []string{"task_id", "id", "request_id"} {
		if value := strings.TrimSpace(gatewayProjectionStringField(fields, key)); value != "" {
			idKey = key
			break
		}
	}
	statusKey := ""
	status := ""
	for _, key := range []string{"status", "state"} {
		if value := strings.ToLower(strings.TrimSpace(gatewayProjectionStringField(fields, key))); value != "" {
			statusKey = key
			status = value
			break
		}
	}
	if idKey == "" || statusKey == "" {
		return
	}
	envelope := strings.Join(path, ".")
	prefix := envelope
	if prefix != "" {
		prefix += "."
	}
	p.facts.Image.Tasks = append(p.facts.Image.Tasks, gatewayBufferedResponseTaskFact{
		EnvelopePath: envelope,
		IDPath:       prefix + idKey,
		StatusPath:   prefix + statusKey,
		ID:           strings.TrimSpace(gatewayProjectionStringField(fields, idKey)),
		Status:       status,
	})
}

func (p *gatewayResponseProjectionParser) recordCompletedImageItem(path []string, fields map[string]gatewayProjectionScalar) {
	if !strings.EqualFold(strings.TrimSpace(gatewayProjectionStringField(fields, "type")), "image_generation_call") {
		return
	}
	status := strings.ToLower(strings.TrimSpace(gatewayProjectionStringField(fields, "status")))
	if status != "" && status != "completed" && status != "succeeded" {
		return
	}
	result, exists := fields["result"]
	if !exists || !result.nonEmpty {
		return
	}
	if gatewayProjectionPathEqual(path, "output") {
		p.rootCompletedImageItem = true
	} else if gatewayProjectionPathEqual(path, "response", "output") {
		p.nestedCompletedImageItem = true
	}
}

func (p *gatewayResponseProjectionParser) finishFacts() {
	if len(p.responseClass) > 0 {
		p.rootClass["response"] = p.responseClass
	}
	if len(p.rootClass) > 0 {
		encoded, err := json.Marshal(p.rootClass)
		if err == nil && len(encoded) <= gatewayResponseProjectionMaxClassificationBytes {
			p.facts.ClassificationJSON = encoded
		} else if err != nil || len(encoded) > gatewayResponseProjectionMaxClassificationBytes {
			p.facts.ClassificationJSON = []byte(`{"error":true}`)
			p.facts.StructuredErrorFound = true
		}
	}
	if p.nestedResponseStatus != "" {
		p.facts.ResponseStatus = p.nestedResponseStatus
	} else {
		p.facts.ResponseStatus = p.rootResponseStatus
	}
	p.facts.Image.ResponsesCompletedImageResult =
		(strings.EqualFold(strings.TrimSpace(p.rootResponseStatus), "completed") && p.rootCompletedImageItem) ||
			(strings.EqualFold(strings.TrimSpace(p.nestedResponseStatus), "completed") && p.nestedCompletedImageItem)

	scope := p.rootUsage
	if !scope.seen {
		scope = p.responseUsage
	}
	if !scope.seen {
		if p.responseFallback.seen {
			scope = p.responseFallback
		} else {
			scope = p.rootFallback
		}
	}
	p.facts.Usage = gatewayProjectionUsageMetrics(scope, p.options.RequestPath)
	for _, envelope := range []string{"response", "message", ""} {
		if model := p.responseModels[envelope]; model != "" {
			p.facts.Usage.ResponseModel = model
			break
		}
	}
	if !p.facts.Usage.CostObserved {
		rootCost := gatewayProjectionUsageMetrics(gatewayProjectedUsageScope{values: p.rootFallback.values}, p.options.RequestPath)
		if rootCost.CostObserved {
			p.facts.Usage.Cost = rootCost.Cost
			p.facts.Usage.CostObserved = true
			p.facts.Usage.CostEstimated = rootCost.CostEstimated
			p.facts.Usage.CostSource = rootCost.CostSource
		}
	}
	p.facts.UsageCostPresent = p.facts.Usage.CostObserved
	if !p.facts.StructuredErrorFound && len(p.facts.ClassificationJSON) > 0 {
		p.facts.StructuredErrorFound = classifyGatewayStreamFailure(p.facts.ClassificationJSON).Found
	}
}

func gatewayProjectionUsageMetrics(scope gatewayProjectedUsageScope, requestPath string) gatewayUsageMetrics {
	values := scope.values
	details := scope.details
	out := gatewayUsageMetrics{}
	input := gatewayProjectionTokenComponent(values, "prompt_tokens", "input_tokens")
	cacheCreation := mergeGatewayTokenComponent(
		gatewayProjectionTokenComponent(values, "cache_creation_input_tokens", "cache_creation_tokens", "cache_write_input_tokens", "cache_write_tokens", "cached_creation_tokens"),
		gatewayProjectionTokenComponent(details, "cache_creation_input_tokens", "cache_creation_tokens", "cache_write_input_tokens", "cache_write_tokens", "cached_creation_tokens"),
	)
	cacheRead := mergeGatewayTokenComponent(
		gatewayProjectionTokenComponent(values, "cache_read_input_tokens", "cache_read_tokens", "cached_tokens"),
		gatewayProjectionTokenComponent(details, "cache_read_input_tokens", "cache_read_tokens", "cached_tokens"),
	)
	output := gatewayProjectionTokenComponent(values, "completion_tokens", "output_tokens")
	total := gatewayProjectionTokenComponent(values, "total_tokens")
	out.PromptTokens = input.value
	out.CompletionTokens = output.value
	out.CacheCreationTokens = cacheCreation.value
	out.CacheReadTokens = cacheRead.value
	out.InputTokensPresent = input.present
	out.OutputTokensPresent = output.present
	out.CacheCreationTokensPresent = cacheCreation.present
	out.CacheReadTokensPresent = cacheRead.present
	out.TotalTokens = total.value
	out.TotalTokensPresent = total.present
	out.UsageInvalid = input.invalid || output.invalid || cacheCreation.invalid || cacheRead.invalid || total.invalid
	if requestPath == "/v1/messages" || requestPath == "/v1/messages/count_tokens" {
		if promptTokens, ok := addGatewayTokenCounts(out.PromptTokens, out.CacheCreationTokens, out.CacheReadTokens); ok {
			out.PromptTokens = promptTokens
		} else {
			out.UsageInvalid = true
		}
	}
	if !out.TotalTokensPresent && !total.invalid {
		if totalTokens, ok := addGatewayTokenCounts(out.PromptTokens, out.CompletionTokens); ok {
			out.TotalTokens = totalTokens
		} else {
			out.UsageInvalid = true
		}
	}
	for _, field := range []string{"cost", "total_cost", "cost_usd", "actual_cost"} {
		if cost, ok := gatewayProjectionFloat(values[field]); ok && cost >= 0 {
			out.Cost = cost
			out.CostObserved = true
			out.CostSource = field
			return out
		}
	}
	if cost, ok := gatewayProjectionFloat(values["estimated_cost"]); ok && cost >= 0 {
		out.Cost = cost
		out.CostObserved = true
		out.CostEstimated = true
		out.CostSource = "estimated_cost"
	}
	return out
}

func (p *gatewayResponseProjectionParser) parseString(mode gatewayProjectionCaptureMode) (string, bool, error) {
	if err := p.expectByte('"'); err != nil {
		return "", false, err
	}
	limit := mode.maxBytes
	if mode.capture && !mode.presenceOnly && limit <= 0 {
		limit = gatewayResponseProjectionMaxStringBytes
	}
	buffer := make([]byte, 0, min(limit, 128))
	nonEmpty := false
	overflow := false
	appendRune := func(value rune) error {
		if !unicode.IsSpace(value) {
			nonEmpty = true
		}
		if !mode.capture || mode.presenceOnly || overflow {
			return nil
		}
		encoded := utf8.AppendRune(nil, value)
		if len(buffer)+len(encoded) > limit {
			if mode.discardOverflow {
				overflow = true
				buffer = nil
				return nil
			}
			return fmt.Errorf("%w: captured string exceeds %d bytes", errGatewayResponseProjectionTooLarge, limit)
		}
		buffer = append(buffer, encoded...)
		return nil
	}
	for {
		value, err := p.readByte()
		if err != nil {
			return "", false, p.syntaxError("unterminated string", err)
		}
		switch value {
		case '"':
			return string(buffer), nonEmpty, nil
		case '\\':
			escaped, err := p.readByte()
			if err != nil {
				return "", false, p.syntaxError("unterminated string escape", err)
			}
			switch escaped {
			case '"', '\\', '/':
				err = appendRune(rune(escaped))
			case 'b':
				err = appendRune('\b')
			case 'f':
				err = appendRune('\f')
			case 'n':
				err = appendRune('\n')
			case 'r':
				err = appendRune('\r')
			case 't':
				err = appendRune('\t')
			case 'u':
				var decoded rune
				decoded, err = p.parseUnicodeEscape()
				if err == nil {
					err = appendRune(decoded)
				}
			default:
				return "", false, p.syntaxError("invalid string escape", nil)
			}
			if err != nil {
				return "", false, err
			}
		default:
			if value < 0x20 {
				return "", false, p.syntaxError("unescaped control character", nil)
			}
			if value < utf8.RuneSelf {
				if err := appendRune(rune(value)); err != nil {
					return "", false, err
				}
				continue
			}
			width := utf8SequenceWidth(value)
			if width == 0 {
				return "", false, p.syntaxError("invalid UTF-8 in string", nil)
			}
			sequence := []byte{value}
			for len(sequence) < width {
				next, err := p.readByte()
				if err != nil {
					return "", false, p.syntaxError("truncated UTF-8 in string", err)
				}
				sequence = append(sequence, next)
			}
			decoded, size := utf8.DecodeRune(sequence)
			if decoded == utf8.RuneError || size != width {
				return "", false, p.syntaxError("invalid UTF-8 in string", nil)
			}
			if err := appendRune(decoded); err != nil {
				return "", false, err
			}
		}
	}
}

func (p *gatewayResponseProjectionParser) parseUnicodeEscape() (rune, error) {
	first, err := p.readHex4()
	if err != nil {
		return 0, err
	}
	value := rune(first)
	if value >= 0xDC00 && value <= 0xDFFF {
		return 0, p.syntaxError("unexpected low surrogate", nil)
	}
	if value < 0xD800 || value > 0xDBFF {
		return value, nil
	}
	backslash, err := p.readByte()
	if err != nil || backslash != '\\' {
		return 0, p.syntaxError("high surrogate is missing pair", err)
	}
	u, err := p.readByte()
	if err != nil || u != 'u' {
		return 0, p.syntaxError("high surrogate is missing pair", err)
	}
	second, err := p.readHex4()
	if err != nil || second < 0xDC00 || second > 0xDFFF {
		return 0, p.syntaxError("invalid surrogate pair", err)
	}
	return utf16.DecodeRune(value, rune(second)), nil
}

func (p *gatewayResponseProjectionParser) readHex4() (uint16, error) {
	var result uint16
	for range 4 {
		value, err := p.readByte()
		if err != nil {
			return 0, p.syntaxError("truncated unicode escape", err)
		}
		result <<= 4
		switch {
		case value >= '0' && value <= '9':
			result |= uint16(value - '0')
		case value >= 'a' && value <= 'f':
			result |= uint16(value-'a') + 10
		case value >= 'A' && value <= 'F':
			result |= uint16(value-'A') + 10
		default:
			return 0, p.syntaxError("invalid unicode escape", nil)
		}
	}
	return result, nil
}

func (p *gatewayResponseProjectionParser) parseNumber(capture bool) (string, error) {
	buffer := make([]byte, 0, 32)
	consume := func() (byte, error) {
		value, err := p.readByte()
		if err == nil && capture {
			if len(buffer) >= 128 {
				return 0, fmt.Errorf("%w: numeric metadata exceeds 128 bytes", errGatewayResponseProjectionTooLarge)
			}
			buffer = append(buffer, value)
		}
		return value, err
	}
	if value, _ := p.peekByte(); value == '-' {
		if _, err := consume(); err != nil {
			return "", err
		}
	}
	value, err := p.peekByte()
	if err != nil {
		return "", p.syntaxError("truncated number", err)
	}
	if value == '0' {
		_, _ = consume()
		if next, err := p.peekByte(); err == nil && next >= '0' && next <= '9' {
			return "", p.syntaxError("number has a leading zero", nil)
		}
	} else if value >= '1' && value <= '9' {
		for {
			next, err := p.peekByte()
			if err != nil || next < '0' || next > '9' {
				break
			}
			_, _ = consume()
		}
	} else {
		return "", p.syntaxError("invalid number", nil)
	}
	if next, err := p.peekByte(); err == nil && next == '.' {
		_, _ = consume()
		if digit, err := p.peekByte(); err != nil || digit < '0' || digit > '9' {
			return "", p.syntaxError("fraction requires a digit", err)
		}
		for {
			digit, err := p.peekByte()
			if err != nil || digit < '0' || digit > '9' {
				break
			}
			_, _ = consume()
		}
	}
	if next, err := p.peekByte(); err == nil && (next == 'e' || next == 'E') {
		_, _ = consume()
		if sign, err := p.peekByte(); err == nil && (sign == '+' || sign == '-') {
			_, _ = consume()
		}
		if digit, err := p.peekByte(); err != nil || digit < '0' || digit > '9' {
			return "", p.syntaxError("exponent requires a digit", err)
		}
		for {
			digit, err := p.peekByte()
			if err != nil || digit < '0' || digit > '9' {
				break
			}
			_, _ = consume()
		}
	}
	if next, err := p.peekByte(); err == nil && !gatewayProjectionValueDelimiter(next) {
		return "", p.syntaxError("invalid character after number", nil)
	}
	return string(buffer), nil
}

func (p *gatewayResponseProjectionParser) consumeLiteral(literal string) error {
	for i := 0; i < len(literal); i++ {
		value, err := p.readByte()
		if err != nil || value != literal[i] {
			return p.syntaxError("invalid literal", err)
		}
	}
	if next, err := p.peekByte(); err == nil && !gatewayProjectionValueDelimiter(next) {
		return p.syntaxError("invalid character after literal", nil)
	}
	return nil
}

func (p *gatewayResponseProjectionParser) skipWhitespace() error {
	for {
		value, err := p.peekByte()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if value != ' ' && value != '\t' && value != '\r' && value != '\n' {
			return nil
		}
		if _, err := p.readByte(); err != nil {
			return err
		}
	}
}

func (p *gatewayResponseProjectionParser) expectByte(expected byte) error {
	value, err := p.readByte()
	if err != nil || value != expected {
		return p.syntaxError(fmt.Sprintf("expected %q", expected), err)
	}
	return nil
}

func (p *gatewayResponseProjectionParser) readByte() (byte, error) {
	value, err := p.reader.ReadByte()
	if err != nil {
		return 0, err
	}
	p.read++
	if p.read%gatewayResponseProjectionContextCheckInterval == 0 {
		if err := gatewayUpstreamResponseSpoolContextError(p.ctx); err != nil {
			return 0, err
		}
	}
	return value, nil
}

func (p *gatewayResponseProjectionParser) peekByte() (byte, error) {
	value, err := p.reader.Peek(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (p *gatewayResponseProjectionParser) syntaxError(message string, cause error) error {
	if cause != nil && !errors.Is(cause, io.EOF) {
		return cause
	}
	return fmt.Errorf("%w at byte %d: %s", errGatewayResponseProjectionInvalid, p.read, message)
}

func appendGatewayProjectionPath(path []string, key string) ([]string, error) {
	if len(path) >= gatewayResponseProjectionMaxDepth || len(key) > gatewayResponseProjectionMaxKeyBytes {
		return nil, errGatewayResponseProjectionTooLarge
	}
	next := make([]string, len(path)+1)
	copy(next, path)
	next[len(path)] = key
	return next, nil
}

func gatewayProjectionPathEqual(path []string, values ...string) bool {
	if len(path) != len(values) {
		return false
	}
	for i := range path {
		if path[i] != values[i] {
			return false
		}
	}
	return true
}

func gatewayProjectionClassificationPath(path []string) bool {
	return len(path) == 0 || gatewayProjectionPathEqual(path, "error") ||
		gatewayProjectionPathEqual(path, "response") || gatewayProjectionPathEqual(path, "response", "error")
}

func gatewayProjectionClassificationField(key string) bool {
	switch key {
	case "type", "code", "status", "status_code", "message", "detail", "error":
		return true
	default:
		return false
	}
}

func gatewayProjectionUsageObjectPath(path []string) bool {
	return gatewayProjectionPathEqual(path, "usage") || gatewayProjectionPathEqual(path, "response", "usage")
}

func gatewayProjectionUsageDetailsPath(path []string) bool {
	return gatewayProjectionPathEqual(path, "usage", "input_tokens_details") ||
		gatewayProjectionPathEqual(path, "usage", "prompt_tokens_details") ||
		gatewayProjectionPathEqual(path, "response", "usage", "input_tokens_details") ||
		gatewayProjectionPathEqual(path, "response", "usage", "prompt_tokens_details")
}

func gatewayProjectionFallbackUsagePath(path []string) bool {
	return len(path) == 0 || gatewayProjectionPathEqual(path, "response")
}

func gatewayProjectionUsageField(key string) bool {
	if gatewayProjectionTokenField(key) {
		return true
	}
	switch key {
	case "cost", "total_cost", "cost_usd", "actual_cost", "estimated_cost":
		return true
	default:
		return false
	}
}

func gatewayProjectionTokenField(key string) bool {
	switch key {
	case "prompt_tokens", "input_tokens", "completion_tokens", "output_tokens", "total_tokens",
		"cache_creation_input_tokens", "cache_creation_tokens", "cache_write_input_tokens", "cache_write_tokens", "cached_creation_tokens",
		"cache_read_input_tokens", "cache_read_tokens", "cached_tokens":
		return true
	default:
		return false
	}
}

func gatewayProjectionTaskEnvelopePath(path []string) bool {
	if len(path) == 0 || gatewayProjectionPathEqual(path, "response") {
		return true
	}
	last := path[len(path)-1]
	return last == "task" || last == "job" || last == "request"
}

func gatewayProjectionTaskField(key string) bool {
	switch key {
	case "task_id", "id", "request_id", "status", "state":
		return true
	default:
		return false
	}
}

func gatewayProjectionOutputItemPath(path []string) bool {
	return gatewayProjectionPathEqual(path, "output") || gatewayProjectionPathEqual(path, "response", "output")
}

func gatewayProjectionStringField(fields map[string]gatewayProjectionScalar, key string) string {
	value, exists := fields[key]
	if !exists {
		return ""
	}
	if value.kind == gatewayProjectionScalarString || value.kind == gatewayProjectionScalarNumber {
		return value.text
	}
	return ""
}

func gatewayProjectionAnyString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
}

func mergeGatewayProjectionMap(destination, source map[string]any) {
	for key, value := range source {
		destination[key] = value
	}
}

func gatewayProjectionClassificationMap(source map[string]any) map[string]any {
	filtered := make(map[string]any)
	for key, value := range source {
		if gatewayProjectionClassificationField(key) {
			filtered[key] = value
		}
	}
	return filtered
}

func gatewayProjectionMaxInt64(values map[string]gatewayProjectionScalar, fields ...string) int64 {
	var result int64
	for _, field := range fields {
		value, _ := gatewayProjectionNonNegativeInt64(values[field])
		result = max(result, value)
	}
	return result
}

func gatewayProjectionNonNegativeInt64Present(values map[string]gatewayProjectionScalar, fields ...string) bool {
	for _, field := range fields {
		if _, ok := gatewayProjectionNonNegativeInt64(values[field]); ok {
			return true
		}
	}
	return false
}

func gatewayProjectionTokenComponent(values map[string]gatewayProjectionScalar, fields ...string) gatewayTokenComponent {
	component := gatewayTokenComponent{}
	for _, field := range fields {
		value, exists := values[field]
		if !exists {
			continue
		}
		number, ok := gatewayProjectionNonNegativeInt64(value)
		if !ok {
			component.invalid = true
			continue
		}
		component.present = true
		component.value = max(component.value, number)
	}
	return component
}

func gatewayProjectionInt64(value gatewayProjectionScalar) int64 {
	number, _ := gatewayProjectionNonNegativeInt64(value)
	return number
}

func gatewayProjectionNonNegativeInt64(value gatewayProjectionScalar) (int64, bool) {
	if value.kind != gatewayProjectionScalarNumber && value.kind != gatewayProjectionScalarString {
		return 0, false
	}
	number, err := strconv.ParseInt(strings.TrimSpace(value.text), 10, 64)
	return number, err == nil && number >= 0
}

func gatewayProjectionFloat(value gatewayProjectionScalar) (float64, bool) {
	if value.kind != gatewayProjectionScalarNumber && value.kind != gatewayProjectionScalarString {
		return 0, false
	}
	number, err := strconv.ParseFloat(strings.TrimSpace(value.text), 64)
	if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, false
	}
	return number, true
}

func gatewayProjectionValueDelimiter(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n' || value == ',' || value == ']' || value == '}'
}

func utf8SequenceWidth(first byte) int {
	switch {
	case first >= 0xC2 && first <= 0xDF:
		return 2
	case first >= 0xE0 && first <= 0xEF:
		return 3
	case first >= 0xF0 && first <= 0xF4:
		return 4
	default:
		return 0
	}
}
