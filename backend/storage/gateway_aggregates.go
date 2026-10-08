package storage

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// v8 counts requests whose local estimate is explicitly unavailable without
	// conflating unknown spend with a legitimate zero-cost request.
	gatewayRequestAggregateVersion             = 8
	gatewayRequestAggregateBatchSize           = 2000
	gatewayRequestBackfillBatchSize            = 200
	gatewayRequestBackfillPause                = 250 * time.Millisecond
	gatewayRequestBackfillConflictInitialPause = 50 * time.Millisecond
	gatewayRequestBackfillConflictMaxPause     = time.Second
	gatewayRequestAggregateBucket              = 15 * time.Minute
)

var (
	errGatewayRequestAggregateClaimConflict   = errors.New("gateway request aggregate batch was claimed concurrently")
	errGatewayRequestAggregateVersionConflict = errors.New("gateway request aggregate data was written by a newer version")
)

type gatewayRequestAggregateKey struct {
	BucketStart         time.Time
	DownstreamKeyID     uint
	ChannelID           uint
	ChannelNameSnapshot string
	ModelName           string
	CostSource          string
	RouteReason         string
	Result              string
}

func aggregateKey(row *GatewayRequestAggregate) gatewayRequestAggregateKey {
	return gatewayRequestAggregateKey{
		BucketStart:         row.BucketStart,
		DownstreamKeyID:     row.DownstreamKeyID,
		ChannelID:           row.ChannelID,
		ChannelNameSnapshot: row.ChannelNameSnapshot,
		ModelName:           row.ModelName,
		CostSource:          row.CostSource,
		RouteReason:         row.RouteReason,
		Result:              row.Result,
	}
}

func aggregateBucketStart(value time.Time) time.Time {
	return value.UTC().Truncate(gatewayRequestAggregateBucket)
}

func aggregateBucketCeil(value time.Time) time.Time {
	value = value.UTC()
	floor := aggregateBucketStart(value)
	if value.Equal(floor) {
		return floor
	}
	return floor.Add(gatewayRequestAggregateBucket)
}

func gatewayRequestResult(log *GatewayRequestLog) string {
	if isGatewayDispatchCostEvidenceRecord(log) {
		return gatewayRequestResultDispatchEvidence
	}
	if IsSuccessfulGatewayRequest(log) {
		return GatewayRequestResultSuccess
	}
	if log != nil {
		errorText := strings.TrimSpace(log.Error)
		if strings.TrimSpace(log.RouteReason) == gatewayRouteReasonClientCanceled ||
			strings.EqualFold(errorText, "context canceled") || errorText == "客户端取消" {
			return GatewayRequestResultCanceled
		}
	}
	return GatewayRequestResultFailed
}

func normalizedAggregateRouteReason(value string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return "smart_primary"
}

func isPerformanceGatewayRequest(log *GatewayRequestLog) bool {
	if !IsSuccessfulGatewayRequest(log) || !strings.EqualFold(strings.TrimSpace(log.Method), "POST") {
		return false
	}
	switch strings.TrimSpace(log.Path) {
	case "/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/responses", "/v1/messages", "/v1/images/generations":
		return true
	default:
		return false
	}
}

func performanceTokensForLog(log *GatewayRequestLog) int64 {
	if log == nil {
		return 0
	}
	input := log.PromptTokens - log.CacheCreationTokens - log.CacheReadTokens
	if input < 0 {
		input = 0
	}
	return input + log.CompletionTokens
}

type gatewayRequestUnitPriceContribution struct {
	InputTokens         int64
	OutputTokens        int64
	InputPricedTokens   int64
	OutputPricedTokens  int64
	CacheCreationTokens int64
	CacheReadTokens     int64
	InputCost           float64
	OutputCost          float64
}

// gatewayRequestUnitPriceContributionForLog keeps cache usage out of the
// uncached-input denominator and attributes a request's total cost between
// input and output using the effective ratios frozen on that request.  The
// upstream connectors currently expose cache token counts but no independent
// cache charge, so cache usage remains separate and deliberately unpriced.
func gatewayRequestUnitPriceContributionForLog(log *GatewayRequestLog) gatewayRequestUnitPriceContribution {
	if log == nil {
		return gatewayRequestUnitPriceContribution{}
	}
	contribution := gatewayRequestUnitPriceContribution{
		InputTokens:         max(log.PromptTokens-log.CacheCreationTokens-log.CacheReadTokens, 0),
		OutputTokens:        max(log.CompletionTokens, 0),
		CacheCreationTokens: max(log.CacheCreationTokens, 0),
		CacheReadTokens:     max(log.CacheReadTokens, 0),
	}
	if !IsGatewayRequestCostBillable(log) {
		return contribution
	}
	status := gatewayRequestCostStatus(log)
	if status == GatewayCostStatusNone || status == GatewayCostStatusUnavailable || log.Cost < 0 {
		return contribution
	}
	if log.Cost == 0 {
		contribution.InputPricedTokens = contribution.InputTokens
		contribution.OutputPricedTokens = contribution.OutputTokens
		return contribution
	}
	inputRatio := log.GroupRatio
	outputRatio := log.CompletionRatio
	if inputRatio <= 0 && outputRatio <= 0 {
		// Historical rows without frozen input/output ratios retain their usage,
		// but cannot safely manufacture a component price from the mixed cost.
		return contribution
	}
	if status != GatewayCostStatusEstimated && (contribution.CacheCreationTokens > 0 || contribution.CacheReadTokens > 0) {
		// Provider-reported totals can include cache charges. Without independent
		// component costs, assigning that mixed total to input/output would inflate
		// both prices. The local estimate is the exception: its formula explicitly
		// excludes cache tokens and is therefore attributable by the frozen ratios.
		return contribution
	}
	if inputRatio <= 0 {
		inputRatio = outputRatio
	}
	if outputRatio <= 0 {
		outputRatio = inputRatio
	}
	inputWeight := float64(contribution.InputTokens) * inputRatio
	outputWeight := float64(contribution.OutputTokens) * outputRatio
	totalWeight := inputWeight + outputWeight
	if totalWeight <= 0 {
		return contribution
	}
	contribution.InputPricedTokens = contribution.InputTokens
	contribution.OutputPricedTokens = contribution.OutputTokens
	contribution.InputCost = log.Cost * inputWeight / totalWeight
	contribution.OutputCost = log.Cost - contribution.InputCost
	if outputWeight <= 0 {
		contribution.InputCost = log.Cost
		contribution.OutputCost = 0
	} else if inputWeight <= 0 {
		contribution.InputCost = 0
		contribution.OutputCost = log.Cost
	}
	return contribution
}

func requestAggregateFromLog(log *GatewayRequestLog) (*GatewayRequestAggregate, bool) {
	if log == nil || (!isGatewayRequestRecord(log) && !isGatewayDispatchCostEvidenceRecord(log)) ||
		(isGatewayRequestRecord(log) && strings.TrimSpace(log.RouteReason) == gatewayRouteReasonLatencyProbe) {
		return nil, false
	}
	createdAt := log.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	} else {
		createdAt = createdAt.UTC()
	}
	result := gatewayRequestResult(log)
	if isGatewayDispatchCostEvidenceRecord(log) {
		row := &GatewayRequestAggregate{
			BucketStart:         aggregateBucketStart(createdAt),
			DownstreamKeyID:     log.DownstreamKeyID,
			ChannelID:           log.ChannelID,
			ChannelNameSnapshot: strings.TrimSpace(log.ChannelNameSnapshot),
			ModelName:           strings.TrimSpace(log.ModelName),
			CostSource:          strings.TrimSpace(log.CostSource),
			RouteReason:         normalizedAggregateRouteReason(log.RouteReason),
			Result:              result,
			FirstRequestAt:      createdAt,
			LastRequestAt:       createdAt,
			Version:             gatewayRequestAggregateVersion,
		}
		if isGatewayDispatchCostEvidencePending(log) {
			row.DispatchEvidencePending = 1
		}
		row.DispatchEvidenceActual = gatewayDispatchCostEvidenceActual(log)
		row.DispatchEvidenceReported = gatewayDispatchCostEvidenceByStatus(log, GatewayCostStatusReported)
		row.DispatchEvidenceReconciled = gatewayDispatchCostEvidenceByStatus(log, GatewayCostStatusReconciled)
		return row, true
	}
	billableCost := 0.0
	costStatus := gatewayRequestCostStatus(log)
	if IsGatewayRequestCostBillable(log) {
		switch costStatus {
		case GatewayCostStatusEstimated, GatewayCostStatusReported, GatewayCostStatusReconciled:
			billableCost = log.Cost
		}
	}
	row := &GatewayRequestAggregate{
		BucketStart:         aggregateBucketStart(createdAt),
		DownstreamKeyID:     log.DownstreamKeyID,
		ChannelID:           log.ChannelID,
		ChannelNameSnapshot: strings.TrimSpace(log.ChannelNameSnapshot),
		ModelName:           strings.TrimSpace(log.ModelName),
		CostSource:          strings.TrimSpace(log.CostSource),
		RouteReason:         normalizedAggregateRouteReason(log.RouteReason),
		Result:              result,
		Requests:            1,
		PromptTokens:        log.PromptTokens,
		CompletionTokens:    log.CompletionTokens,
		CacheCreationTokens: log.CacheCreationTokens,
		CacheReadTokens:     log.CacheReadTokens,
		TotalTokens:         log.TotalTokens,
		TotalCost:           billableCost,
		FirstRequestAt:      createdAt,
		LastRequestAt:       createdAt,
		Version:             gatewayRequestAggregateVersion,
	}
	switch costStatus {
	case GatewayCostStatusEstimated:
		row.EstimatedCost = billableCost
	case GatewayCostStatusReported:
		row.ActualCost = billableCost
		row.ReportedCost = billableCost
	case GatewayCostStatusReconciled:
		row.ActualCost = billableCost
		row.ReconciledCost = billableCost
	case GatewayCostStatusUnavailable:
		row.UnavailableRequests = 1
	}
	if result == GatewayRequestResultSuccess {
		row.LatencyTotalMs = log.LatencyMs
		row.LatencySamples = 1
	}
	if isPerformanceGatewayRequest(log) {
		unitPrice := gatewayRequestUnitPriceContributionForLog(log)
		row.PerformanceRequests = 1
		row.PerformanceTokens = performanceTokensForLog(log)
		row.PerformanceCost = billableCost
		row.PerformanceInputTokens = unitPrice.InputTokens
		row.PerformanceOutputTokens = unitPrice.OutputTokens
		row.PerformanceInputPricedTokens = unitPrice.InputPricedTokens
		row.PerformanceOutputPricedTokens = unitPrice.OutputPricedTokens
		row.PerformanceCacheCreationTokens = unitPrice.CacheCreationTokens
		row.PerformanceCacheReadTokens = unitPrice.CacheReadTokens
		row.PerformanceInputCost = unitPrice.InputCost
		row.PerformanceOutputCost = unitPrice.OutputCost
	}
	return row, true
}

func gatewayChannelNames(tx *gorm.DB, ids []uint) (map[uint]string, error) {
	unique := make(map[uint]struct{}, len(ids))
	for _, id := range ids {
		if id > 0 {
			unique[id] = struct{}{}
		}
	}
	if len(unique) == 0 {
		return map[uint]string{}, nil
	}
	channelIDs := make([]uint, 0, len(unique))
	for id := range unique {
		channelIDs = append(channelIDs, id)
	}
	var channels []Channel
	if err := tx.Select("id", "name").Where("id IN ?", channelIDs).Find(&channels).Error; err != nil {
		return nil, err
	}
	names := make(map[uint]string, len(channels))
	for _, channel := range channels {
		names[channel.ID] = channel.Name
	}
	return names, nil
}

func snapshotGatewayRequestLogChannelName(tx *gorm.DB, log *GatewayRequestLog) error {
	if log == nil || log.ChannelID == 0 || strings.TrimSpace(log.ChannelNameSnapshot) != "" {
		return nil
	}
	names, err := gatewayChannelNames(tx, []uint{log.ChannelID})
	if err != nil {
		return err
	}
	if name := names[log.ChannelID]; strings.TrimSpace(name) != "" {
		log.ChannelNameSnapshot = name
	}
	return nil
}

func snapshotGatewayRequestLogChannelNames(tx *gorm.DB, logs []GatewayRequestLog) error {
	idsByChannel := make(map[uint][]uint)
	for index := range logs {
		log := &logs[index]
		if log.ID == 0 || log.ChannelID == 0 || log.AggregateVersion >= gatewayRequestAggregateVersion || strings.TrimSpace(log.ChannelNameSnapshot) != "" {
			continue
		}
		idsByChannel[log.ChannelID] = append(idsByChannel[log.ChannelID], log.ID)
	}
	if len(idsByChannel) == 0 {
		return nil
	}
	channelIDs := make([]uint, 0, len(idsByChannel))
	for channelID := range idsByChannel {
		channelIDs = append(channelIDs, channelID)
	}
	names, err := gatewayChannelNames(tx, channelIDs)
	if err != nil {
		return err
	}
	for channelID, logIDs := range idsByChannel {
		name := names[channelID]
		if strings.TrimSpace(name) == "" {
			continue
		}
		if err := tx.Model(&GatewayRequestLog{}).
			Where("id IN ? AND (channel_name_snapshot IS NULL OR TRIM(channel_name_snapshot) = '')", logIDs).
			Update("channel_name_snapshot", name).Error; err != nil {
			return err
		}
		for index := range logs {
			if logs[index].ChannelID == channelID && logs[index].AggregateVersion < gatewayRequestAggregateVersion && strings.TrimSpace(logs[index].ChannelNameSnapshot) == "" {
				logs[index].ChannelNameSnapshot = name
			}
		}
	}
	return nil
}

func mergeRequestAggregate(dst, src *GatewayRequestAggregate) {
	if dst == nil || src == nil {
		return
	}
	dstHadRequests := dst.Requests > 0
	dst.Requests = saturatingAggregateInt64Add(dst.Requests, src.Requests)
	dst.PromptTokens = saturatingAggregateInt64Add(dst.PromptTokens, src.PromptTokens)
	dst.CompletionTokens = saturatingAggregateInt64Add(dst.CompletionTokens, src.CompletionTokens)
	dst.CacheCreationTokens = saturatingAggregateInt64Add(dst.CacheCreationTokens, src.CacheCreationTokens)
	dst.CacheReadTokens = saturatingAggregateInt64Add(dst.CacheReadTokens, src.CacheReadTokens)
	dst.TotalTokens = saturatingAggregateInt64Add(dst.TotalTokens, src.TotalTokens)
	dst.TotalCost += src.TotalCost
	dst.EstimatedCost += src.EstimatedCost
	dst.ActualCost += src.ActualCost
	dst.ReportedCost += src.ReportedCost
	dst.ReconciledCost += src.ReconciledCost
	dst.UnavailableRequests = saturatingAggregateInt64Add(dst.UnavailableRequests, src.UnavailableRequests)
	dst.DispatchEvidencePending = saturatingAggregateInt64Add(dst.DispatchEvidencePending, src.DispatchEvidencePending)
	dst.DispatchEvidenceActual += src.DispatchEvidenceActual
	dst.DispatchEvidenceReported += src.DispatchEvidenceReported
	dst.DispatchEvidenceReconciled += src.DispatchEvidenceReconciled
	dst.LatencyTotalMs = saturatingAggregateInt64Add(dst.LatencyTotalMs, src.LatencyTotalMs)
	dst.LatencySamples = saturatingAggregateInt64Add(dst.LatencySamples, src.LatencySamples)
	dst.PerformanceRequests = saturatingAggregateInt64Add(dst.PerformanceRequests, src.PerformanceRequests)
	dst.PerformanceTokens = saturatingAggregateInt64Add(dst.PerformanceTokens, src.PerformanceTokens)
	dst.PerformanceCost += src.PerformanceCost
	dst.PerformanceInputTokens = saturatingAggregateInt64Add(dst.PerformanceInputTokens, src.PerformanceInputTokens)
	dst.PerformanceOutputTokens = saturatingAggregateInt64Add(dst.PerformanceOutputTokens, src.PerformanceOutputTokens)
	dst.PerformanceInputPricedTokens = saturatingAggregateInt64Add(dst.PerformanceInputPricedTokens, src.PerformanceInputPricedTokens)
	dst.PerformanceOutputPricedTokens = saturatingAggregateInt64Add(dst.PerformanceOutputPricedTokens, src.PerformanceOutputPricedTokens)
	dst.PerformanceCacheCreationTokens = saturatingAggregateInt64Add(dst.PerformanceCacheCreationTokens, src.PerformanceCacheCreationTokens)
	dst.PerformanceCacheReadTokens = saturatingAggregateInt64Add(dst.PerformanceCacheReadTokens, src.PerformanceCacheReadTokens)
	dst.PerformanceInputCost += src.PerformanceInputCost
	dst.PerformanceOutputCost += src.PerformanceOutputCost
	if src.Requests > 0 {
		if !dstHadRequests || dst.FirstRequestAt.IsZero() || (!src.FirstRequestAt.IsZero() && src.FirstRequestAt.Before(dst.FirstRequestAt)) {
			dst.FirstRequestAt = src.FirstRequestAt
		}
		if !dstHadRequests || src.LastRequestAt.After(dst.LastRequestAt) {
			dst.LastRequestAt = src.LastRequestAt
		}
	}
}

func saturatingAggregateInt64Add(left, right int64) int64 {
	left = max(left, 0)
	right = max(right, 0)
	if left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func upsertRequestAggregateTx(tx *gorm.DB, row *GatewayRequestAggregate) error {
	if tx == nil || !gatewayRequestAggregateHasContribution(row) {
		return nil
	}
	return tx.Clauses(requestAggregateOnConflict(row)).Create(row).Error
}

func gatewayRequestAggregateHasContribution(row *GatewayRequestAggregate) bool {
	return row != nil && (row.Requests != 0 || row.DispatchEvidencePending != 0 || row.DispatchEvidenceActual != 0 ||
		row.DispatchEvidenceReported != 0 || row.DispatchEvidenceReconciled != 0)
}

func requestAggregateOnConflict(row *GatewayRequestAggregate) clause.OnConflict {
	columns := []clause.Column{
		{Name: "bucket_start"}, {Name: "downstream_key_id"}, {Name: "channel_id"},
		{Name: "channel_name_snapshot"}, {Name: "model_name"}, {Name: "cost_source"}, {Name: "route_reason"}, {Name: "result"},
	}
	updates := map[string]any{
		"requests":                          aggregateInt64ValueUpdate("requests", row.Requests),
		"prompt_tokens":                     aggregateInt64ValueUpdate("prompt_tokens", row.PromptTokens),
		"completion_tokens":                 aggregateInt64ValueUpdate("completion_tokens", row.CompletionTokens),
		"cache_creation_tokens":             aggregateInt64ValueUpdate("cache_creation_tokens", row.CacheCreationTokens),
		"cache_read_tokens":                 aggregateInt64ValueUpdate("cache_read_tokens", row.CacheReadTokens),
		"total_tokens":                      aggregateInt64ValueUpdate("total_tokens", row.TotalTokens),
		"total_cost":                        aggregateValueUpdate("total_cost", row.TotalCost),
		"estimated_cost":                    aggregateValueUpdate("estimated_cost", row.EstimatedCost),
		"actual_cost":                       aggregateValueUpdate("actual_cost", row.ActualCost),
		"reported_cost":                     aggregateValueUpdate("reported_cost", row.ReportedCost),
		"reconciled_cost":                   aggregateValueUpdate("reconciled_cost", row.ReconciledCost),
		"unavailable_requests":              aggregateInt64ValueUpdate("unavailable_requests", row.UnavailableRequests),
		"dispatch_evidence_pending":         aggregateInt64ValueUpdate("dispatch_evidence_pending", row.DispatchEvidencePending),
		"dispatch_evidence_actual":          aggregateValueUpdate("dispatch_evidence_actual", row.DispatchEvidenceActual),
		"dispatch_evidence_reported":        aggregateValueUpdate("dispatch_evidence_reported", row.DispatchEvidenceReported),
		"dispatch_evidence_reconciled":      aggregateValueUpdate("dispatch_evidence_reconciled", row.DispatchEvidenceReconciled),
		"latency_total_ms":                  aggregateInt64ValueUpdate("latency_total_ms", row.LatencyTotalMs),
		"latency_samples":                   aggregateInt64ValueUpdate("latency_samples", row.LatencySamples),
		"performance_requests":              aggregateInt64ValueUpdate("performance_requests", row.PerformanceRequests),
		"performance_tokens":                aggregateInt64ValueUpdate("performance_tokens", row.PerformanceTokens),
		"performance_cost":                  aggregateValueUpdate("performance_cost", row.PerformanceCost),
		"performance_input_tokens":          aggregateInt64ValueUpdate("performance_input_tokens", row.PerformanceInputTokens),
		"performance_output_tokens":         aggregateInt64ValueUpdate("performance_output_tokens", row.PerformanceOutputTokens),
		"performance_input_priced_tokens":   aggregateInt64ValueUpdate("performance_input_priced_tokens", row.PerformanceInputPricedTokens),
		"performance_output_priced_tokens":  aggregateInt64ValueUpdate("performance_output_priced_tokens", row.PerformanceOutputPricedTokens),
		"performance_cache_creation_tokens": aggregateInt64ValueUpdate("performance_cache_creation_tokens", row.PerformanceCacheCreationTokens),
		"performance_cache_read_tokens":     aggregateInt64ValueUpdate("performance_cache_read_tokens", row.PerformanceCacheReadTokens),
		"performance_input_cost":            aggregateValueUpdate("performance_input_cost", row.PerformanceInputCost),
		"performance_output_cost":           aggregateValueUpdate("performance_output_cost", row.PerformanceOutputCost),
		"first_request_at": gorm.Expr(
			"CASE WHEN ? < ? THEN ? WHEN ? > ? THEN ? WHEN ? <= 0 THEN ? WHEN ? <= 0 THEN ? WHEN ? > ? THEN ? ELSE ? END",
			aggregateCurrentColumn("version"), gatewayRequestAggregateVersion, row.FirstRequestAt,
			aggregateCurrentColumn("version"), gatewayRequestAggregateVersion, aggregateCurrentColumn("first_request_at"),
			row.Requests, aggregateCurrentColumn("first_request_at"),
			aggregateCurrentColumn("requests"), row.FirstRequestAt,
			aggregateCurrentColumn("first_request_at"), row.FirstRequestAt, row.FirstRequestAt, aggregateCurrentColumn("first_request_at"),
		),
		"last_request_at": gorm.Expr(
			"CASE WHEN ? < ? THEN ? WHEN ? > ? THEN ? WHEN ? <= 0 THEN ? WHEN ? <= 0 THEN ? WHEN ? < ? THEN ? ELSE ? END",
			aggregateCurrentColumn("version"), gatewayRequestAggregateVersion, row.LastRequestAt,
			aggregateCurrentColumn("version"), gatewayRequestAggregateVersion, aggregateCurrentColumn("last_request_at"),
			row.Requests, aggregateCurrentColumn("last_request_at"),
			aggregateCurrentColumn("requests"), row.LastRequestAt,
			aggregateCurrentColumn("last_request_at"), row.LastRequestAt, row.LastRequestAt, aggregateCurrentColumn("last_request_at"),
		),
	}
	assignments := clause.Assignments(updates)
	// Keep the version marker last so every replacement expression sees the old row.
	assignments = append(assignments, clause.Assignment{
		Column: clause.Column{Name: "version"},
		Value: gorm.Expr(
			"CASE WHEN ? > ? THEN ? ELSE ? END",
			aggregateCurrentColumn("version"), gatewayRequestAggregateVersion,
			aggregateCurrentColumn("version"), gatewayRequestAggregateVersion,
		),
	})
	return clause.OnConflict{Columns: columns, DoUpdates: assignments}
}

func aggregateValueUpdate(column string, value any) clause.Expr {
	return gorm.Expr(
		"CASE WHEN ? < ? THEN ? WHEN ? > ? THEN ? ELSE ? + ? END",
		aggregateCurrentColumn("version"), gatewayRequestAggregateVersion, value,
		aggregateCurrentColumn("version"), gatewayRequestAggregateVersion, aggregateCurrentColumn(column),
		aggregateCurrentColumn(column), value,
	)
}

func aggregateInt64ValueUpdate(column string, value int64) clause.Expr {
	value = max(value, 0)
	return gorm.Expr(
		"CASE WHEN ? < ? THEN ? WHEN ? > ? THEN ? ELSE CAST(LEAST(?::numeric + ?::numeric, ?::numeric) AS bigint) END",
		aggregateCurrentColumn("version"), gatewayRequestAggregateVersion, value,
		aggregateCurrentColumn("version"), gatewayRequestAggregateVersion, aggregateCurrentColumn(column),
		aggregateCurrentColumn(column), value, int64(math.MaxInt64),
	)
}

func aggregateCurrentColumn(name string) clause.Column {
	return clause.Column{Table: clause.CurrentTable, Name: name}
}

func requestAggregateKeyWhere(tx *gorm.DB, key gatewayRequestAggregateKey) *gorm.DB {
	return tx.Where(
		"bucket_start = ? AND downstream_key_id = ? AND channel_id = ? AND channel_name_snapshot = ? AND model_name = ? AND cost_source = ? AND route_reason = ? AND result = ? AND version = ?",
		key.BucketStart, key.DownstreamKeyID, key.ChannelID, key.ChannelNameSnapshot, key.ModelName, key.CostSource, key.RouteReason, key.Result, gatewayRequestAggregateVersion,
	)
}

func subtractRequestAggregateTx(tx *gorm.DB, row *GatewayRequestAggregate) error {
	if tx == nil || !gatewayRequestAggregateHasContribution(row) {
		return nil
	}
	updates := map[string]any{
		"requests":                          gorm.Expr("requests - ?", row.Requests),
		"prompt_tokens":                     gorm.Expr("prompt_tokens - ?", row.PromptTokens),
		"completion_tokens":                 gorm.Expr("completion_tokens - ?", row.CompletionTokens),
		"cache_creation_tokens":             gorm.Expr("cache_creation_tokens - ?", row.CacheCreationTokens),
		"cache_read_tokens":                 gorm.Expr("cache_read_tokens - ?", row.CacheReadTokens),
		"total_tokens":                      gorm.Expr("total_tokens - ?", row.TotalTokens),
		"total_cost":                        gorm.Expr("total_cost - ?", row.TotalCost),
		"estimated_cost":                    gorm.Expr("estimated_cost - ?", row.EstimatedCost),
		"actual_cost":                       gorm.Expr("actual_cost - ?", row.ActualCost),
		"reported_cost":                     gorm.Expr("reported_cost - ?", row.ReportedCost),
		"reconciled_cost":                   gorm.Expr("reconciled_cost - ?", row.ReconciledCost),
		"unavailable_requests":              gorm.Expr("unavailable_requests - ?", row.UnavailableRequests),
		"dispatch_evidence_pending":         gorm.Expr("dispatch_evidence_pending - ?", row.DispatchEvidencePending),
		"dispatch_evidence_actual":          gorm.Expr("dispatch_evidence_actual - ?", row.DispatchEvidenceActual),
		"dispatch_evidence_reported":        gorm.Expr("dispatch_evidence_reported - ?", row.DispatchEvidenceReported),
		"dispatch_evidence_reconciled":      gorm.Expr("dispatch_evidence_reconciled - ?", row.DispatchEvidenceReconciled),
		"latency_total_ms":                  gorm.Expr("latency_total_ms - ?", row.LatencyTotalMs),
		"latency_samples":                   gorm.Expr("latency_samples - ?", row.LatencySamples),
		"performance_requests":              gorm.Expr("performance_requests - ?", row.PerformanceRequests),
		"performance_tokens":                gorm.Expr("performance_tokens - ?", row.PerformanceTokens),
		"performance_cost":                  gorm.Expr("performance_cost - ?", row.PerformanceCost),
		"performance_input_tokens":          gorm.Expr("performance_input_tokens - ?", row.PerformanceInputTokens),
		"performance_output_tokens":         gorm.Expr("performance_output_tokens - ?", row.PerformanceOutputTokens),
		"performance_input_priced_tokens":   gorm.Expr("performance_input_priced_tokens - ?", row.PerformanceInputPricedTokens),
		"performance_output_priced_tokens":  gorm.Expr("performance_output_priced_tokens - ?", row.PerformanceOutputPricedTokens),
		"performance_cache_creation_tokens": gorm.Expr("performance_cache_creation_tokens - ?", row.PerformanceCacheCreationTokens),
		"performance_cache_read_tokens":     gorm.Expr("performance_cache_read_tokens - ?", row.PerformanceCacheReadTokens),
		"performance_input_cost":            gorm.Expr("performance_input_cost - ?", row.PerformanceInputCost),
		"performance_output_cost":           gorm.Expr("performance_output_cost - ?", row.PerformanceOutputCost),
	}
	query := requestAggregateKeyWhere(tx.Model(&GatewayRequestAggregate{}), aggregateKey(row))
	if err := query.Updates(updates).Error; err != nil {
		return err
	}
	return requestAggregateKeyWhere(tx, aggregateKey(row)).
		Where("requests <= 0 AND dispatch_evidence_pending <= 0 AND dispatch_evidence_actual = 0 AND dispatch_evidence_reported = 0 AND dispatch_evidence_reconciled = 0").
		Delete(&GatewayRequestAggregate{}).Error
}

func replaceRequestAggregateTx(tx *gorm.DB, before, after *GatewayRequestLog) error {
	if before == nil || before.AggregateVersion < gatewayRequestAggregateVersion {
		return nil
	}
	if before.AggregateVersion > gatewayRequestAggregateVersion {
		return errGatewayRequestAggregateVersionConflict
	}
	oldRow, oldOK := requestAggregateFromLog(before)
	newRow, newOK := requestAggregateFromLog(after)
	if oldOK {
		if err := subtractRequestAggregateTx(tx, oldRow); err != nil {
			return err
		}
	}
	if newOK {
		return upsertRequestAggregateTx(tx, newRow)
	}
	return nil
}

// A rolled-back process cannot safely reset rows already represented by a
// newer aggregate schema: a later upgrade would replay them into that ledger.
func ensureRequestAggregateResetVersionCompatible(logScope, aggregateScope *gorm.DB) error {
	var log GatewayRequestLog
	result := logScope.Select("id").Where("aggregate_version > ?", gatewayRequestAggregateVersion).Limit(1).Find(&log)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return errGatewayRequestAggregateVersionConflict
	}

	var aggregate GatewayRequestAggregate
	result = aggregateScope.Select("id").Where("version > ?", gatewayRequestAggregateVersion).Limit(1).Find(&aggregate)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return errGatewayRequestAggregateVersionConflict
	}
	return nil
}

// snapshotGatewayRequestHistoryBeforeChannelDelete preserves the channel name
// on legacy logs before the source channel disappears. Only rows that lacked a
// snapshot are marked for rebuild; already-current history stays untouched.
func snapshotGatewayRequestHistoryBeforeChannelDelete(tx *gorm.DB, channelID uint, channelName string) error {
	channelName = strings.TrimSpace(channelName)
	if tx == nil || channelID == 0 || channelName == "" {
		return nil
	}
	logScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND (channel_name_snapshot IS NULL OR TRIM(channel_name_snapshot) = '')", channelID)
	aggregateScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestAggregate{}).
		Where("channel_id = ? AND (channel_name_snapshot IS NULL OR TRIM(channel_name_snapshot) = '')", channelID)
	if err := ensureRequestAggregateResetVersionCompatible(logScope, aggregateScope); err != nil {
		return err
	}
	groupLogScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND (channel_name_snapshot IS NULL OR TRIM(channel_name_snapshot) = '')", channelID)
	if err := ensureGatewayRequestGroupCacheLogResetVersionCompatible(groupLogScope); err != nil {
		return err
	}
	groupScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestGroupCacheAggregate{}).
		Where("channel_id = ? AND (channel_name_snapshot IS NULL OR TRIM(channel_name_snapshot) = '')", channelID)
	if err := ensureGatewayRequestGroupCacheResetVersionCompatible(groupScope); err != nil {
		return err
	}
	result := tx.Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND (channel_name_snapshot IS NULL OR TRIM(channel_name_snapshot) = '') AND aggregate_version <= ? AND group_cache_aggregate_version <= ?", channelID, gatewayRequestAggregateVersion, gatewayRequestGroupCacheAggregateVersion).
		Updates(map[string]any{
			"channel_name_snapshot":         channelName,
			"aggregate_version":             0,
			"group_cache_aggregate_version": 0,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return nil
	}
	if err := tx.Where(
		"channel_id = ? AND version = ? AND (channel_name_snapshot IS NULL OR TRIM(channel_name_snapshot) = '')",
		channelID, gatewayRequestAggregateVersion,
	).Delete(&GatewayRequestAggregate{}).Error; err != nil {
		return err
	}
	return tx.Where(
		"channel_id = ? AND version <= ? AND (channel_name_snapshot IS NULL OR TRIM(channel_name_snapshot) = '')",
		channelID, gatewayRequestGroupCacheAggregateVersion,
	).Delete(&GatewayRequestGroupCacheAggregate{}).Error
}

// migrateGatewayRequestAggregateBatch rebuilds the two ledgers in bounded
// transactions. The group-cache marker is per raw request, rather than a
// global cursor: a live append can therefore safely race the historical pass
// without either replaying or skipping an ID.
func migrateGatewayRequestAggregateBatch(db *gorm.DB, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = gatewayRequestAggregateBatchSize
	}
	processed := 0
	err := db.Transaction(func(tx *gorm.DB) error {
		state, err := lockGatewayRequestGroupCacheBackfillStateTx(tx)
		if err != nil {
			return err
		}
		if state.TargetVersion > gatewayRequestGroupCacheAggregateVersion {
			return errGatewayRequestGroupCacheAggregateVersionConflict
		}
		if state.TargetVersion < gatewayRequestGroupCacheAggregateVersion {
			if err := resetGatewayRequestGroupCacheBackfillTx(tx); err != nil {
				return err
			}
			state, err = lockGatewayRequestGroupCacheBackfillStateTx(tx)
			if err != nil {
				return err
			}
		}
		var futureGroupMarker GatewayRequestLog
		futureResult := tx.Select("id").
			Where("group_cache_aggregate_version > ?", gatewayRequestGroupCacheAggregateVersion).
			Order("id ASC").Take(&futureGroupMarker)
		if futureResult.Error == nil {
			return errGatewayRequestGroupCacheAggregateVersionConflict
		}
		if !errors.Is(futureResult.Error, gorm.ErrRecordNotFound) {
			return futureResult.Error
		}

		var logs []GatewayRequestLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("(group_cache_aggregate_version < ? AND aggregate_version <= ?) OR aggregate_version < ?", gatewayRequestGroupCacheAggregateVersion, gatewayRequestAggregateVersion, gatewayRequestAggregateVersion).
			Order("id ASC").Limit(batchSize).Find(&logs).Error; err != nil {
			return err
		}
		if len(logs) == 0 {
			processedRows := 0
			var ids []uint64
			if err := tx.Model(&GatewayRequestAggregate{}).
				Where("version < ?", gatewayRequestAggregateVersion).
				Order("id ASC").Limit(batchSize).Pluck("id", &ids).Error; err != nil {
				return err
			}
			if len(ids) > 0 {
				result := tx.Where("id IN ? AND version < ?", ids, gatewayRequestAggregateVersion).
					Delete(&GatewayRequestAggregate{})
				if result.Error != nil {
					return result.Error
				}
				processedRows += int(result.RowsAffected)
			}
			groupResult := tx.Exec(`
				DELETE FROM public.gateway_request_group_cache_aggregates AS stale
				 WHERE stale.ctid IN (
					SELECT candidate.ctid
					  FROM public.gateway_request_group_cache_aggregates AS candidate
					 WHERE candidate.version < ?
					 ORDER BY candidate.bucket_start ASC, candidate.channel_id ASC, candidate.group_name ASC, candidate.model_name ASC
					 LIMIT ?
				 )`, gatewayRequestGroupCacheAggregateVersion, batchSize)
			if groupResult.Error != nil {
				return groupResult.Error
			}
			processedRows += int(groupResult.RowsAffected)
			if processedRows == 0 {
				state.TargetVersion = gatewayRequestGroupCacheAggregateVersion
				return completeGatewayRequestGroupCacheBackfillStateTx(tx, state)
			}
			state.Completed = false
			if err := advanceGatewayRequestGroupCacheBackfillStateTx(tx, state, 0); err != nil {
				return err
			}
			processed = processedRows
			return nil
		}
		if err := snapshotGatewayRequestLogChannelNames(tx, logs); err != nil {
			return err
		}

		mainIDs := make([]uint, 0, len(logs))
		mainPending := make(map[uint]struct{}, len(logs))
		groupIDs := make([]uint, 0, len(logs))
		var lastRequestID uint
		for index := range logs {
			log := &logs[index]
			if log.AggregateVersion < gatewayRequestAggregateVersion {
				mainIDs = append(mainIDs, log.ID)
				mainPending[log.ID] = struct{}{}
			}
			if log.GroupCacheAggregateVersion < gatewayRequestGroupCacheAggregateVersion {
				groupIDs = append(groupIDs, log.ID)
			}
			if log.ID > lastRequestID {
				lastRequestID = log.ID
			}
		}
		if len(mainIDs) > 0 {
			legacyUnverifiedEstimateSQL := gatewayRequestLegacyUnverifiedEstimateSQL("")
			claim := tx.Model(&GatewayRequestLog{}).
				Where("id IN ? AND aggregate_version < ?", mainIDs, gatewayRequestAggregateVersion).
				Updates(map[string]any{
					"aggregate_version":       gatewayRequestAggregateVersion,
					"cost_status":             gorm.Expr(gatewayRequestEffectiveCostStatusSQL("")),
					"cost_estimated":          gorm.Expr("CASE WHEN " + legacyUnverifiedEstimateSQL + " THEN FALSE ELSE cost_estimated END"),
					"cost_unavailable_reason": gorm.Expr("CASE WHEN "+legacyUnverifiedEstimateSQL+" THEN ? ELSE cost_unavailable_reason END", GatewayPricingUnavailableLegacyUnverifiedEstimate),
				})
			if claim.Error != nil {
				return claim.Error
			}
			if claim.RowsAffected != int64(len(mainIDs)) {
				return errGatewayRequestAggregateClaimConflict
			}
		}

		grouped := make(map[gatewayRequestAggregateKey]*GatewayRequestAggregate)
		groupCacheGrouped := make(map[gatewayRequestGroupCacheAggregateKey]*GatewayRequestGroupCacheAggregate)
		for index := range logs {
			log := &logs[index]
			if _, pending := mainPending[log.ID]; pending {
				row, ok := requestAggregateFromLog(log)
				if ok {
					key := aggregateKey(row)
					if existing := grouped[key]; existing != nil {
						mergeRequestAggregate(existing, row)
					} else {
						grouped[key] = row
					}
				}
			}
			if log.GroupCacheAggregateVersion < gatewayRequestGroupCacheAggregateVersion {
				groupCacheRow, groupCacheOK := requestGroupCacheAggregateFromLog(log)
				if groupCacheOK {
					key := groupCacheAggregateKey(groupCacheRow)
					if existing := groupCacheGrouped[key]; existing != nil {
						mergeGatewayRequestGroupCacheAggregate(existing, groupCacheRow)
					} else {
						groupCacheGrouped[key] = groupCacheRow
					}
				}
			}
		}
		keys := make([]gatewayRequestAggregateKey, 0, len(grouped))
		for key := range grouped {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if !keys[i].BucketStart.Equal(keys[j].BucketStart) {
				return keys[i].BucketStart.Before(keys[j].BucketStart)
			}
			if keys[i].DownstreamKeyID != keys[j].DownstreamKeyID {
				return keys[i].DownstreamKeyID < keys[j].DownstreamKeyID
			}
			if keys[i].ChannelID != keys[j].ChannelID {
				return keys[i].ChannelID < keys[j].ChannelID
			}
			if keys[i].ChannelNameSnapshot != keys[j].ChannelNameSnapshot {
				return keys[i].ChannelNameSnapshot < keys[j].ChannelNameSnapshot
			}
			if keys[i].ModelName != keys[j].ModelName {
				return keys[i].ModelName < keys[j].ModelName
			}
			if keys[i].CostSource != keys[j].CostSource {
				return keys[i].CostSource < keys[j].CostSource
			}
			if keys[i].RouteReason != keys[j].RouteReason {
				return keys[i].RouteReason < keys[j].RouteReason
			}
			return keys[i].Result < keys[j].Result
		})
		for _, key := range keys {
			if err := upsertRequestAggregateTx(tx, grouped[key]); err != nil {
				return err
			}
		}
		groupCacheKeys := make([]gatewayRequestGroupCacheAggregateKey, 0, len(groupCacheGrouped))
		for key := range groupCacheGrouped {
			groupCacheKeys = append(groupCacheKeys, key)
		}
		sort.Slice(groupCacheKeys, func(i, j int) bool {
			left, right := groupCacheKeys[i], groupCacheKeys[j]
			if !left.BucketStart.Equal(right.BucketStart) {
				return left.BucketStart.Before(right.BucketStart)
			}
			if left.DownstreamKeyID != right.DownstreamKeyID {
				return left.DownstreamKeyID < right.DownstreamKeyID
			}
			if left.ChannelID != right.ChannelID {
				return left.ChannelID < right.ChannelID
			}
			if left.ChannelNameSnapshot != right.ChannelNameSnapshot {
				return left.ChannelNameSnapshot < right.ChannelNameSnapshot
			}
			if left.GroupName != right.GroupName {
				return left.GroupName < right.GroupName
			}
			return left.ModelName < right.ModelName
		})
		for _, key := range groupCacheKeys {
			if err := upsertGatewayRequestGroupCacheAggregateTx(tx, groupCacheGrouped[key]); err != nil {
				return err
			}
		}
		if err := markGatewayRequestGroupCacheLogsProcessedTx(tx, groupIDs); err != nil {
			return err
		}
		state.Completed = false
		state.TargetVersion = gatewayRequestGroupCacheAggregateVersion
		if err := advanceGatewayRequestGroupCacheBackfillStateTx(tx, state, lastRequestID); err != nil {
			return err
		}
		processed = len(logs)
		return nil
	})
	return processed, err
}

func backfillGatewayRequestAggregates(ctx context.Context, db *gorm.DB, batchSize int, pause time.Duration) (int64, error) {
	var total int64
	conflictPause := gatewayRequestBackfillConflictInitialPause
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		processed, err := migrateGatewayRequestAggregateBatch(db.WithContext(ctx), batchSize)
		if errors.Is(err, errGatewayRequestAggregateClaimConflict) {
			if err := waitForGatewayRequestAggregateBackfill(ctx, conflictPause); err != nil {
				return total, err
			}
			conflictPause = nextGatewayRequestAggregateConflictPause(conflictPause)
			continue
		}
		if err != nil {
			return total, err
		}
		conflictPause = gatewayRequestBackfillConflictInitialPause
		if processed == 0 {
			return total, nil
		}
		total += int64(processed)
		if pause > 0 {
			if err := waitForGatewayRequestAggregateBackfill(ctx, pause); err != nil {
				return total, err
			}
		}
	}
}

func nextGatewayRequestAggregateConflictPause(current time.Duration) time.Duration {
	if current <= 0 {
		return gatewayRequestBackfillConflictInitialPause
	}
	if current >= gatewayRequestBackfillConflictMaxPause/2 {
		return gatewayRequestBackfillConflictMaxPause
	}
	return current * 2
}

func waitForGatewayRequestAggregateBackfill(ctx context.Context, pause time.Duration) error {
	if pause <= 0 {
		return nil
	}
	timer := time.NewTimer(pause)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// BackfillGatewayRequestAggregates migrates historical logs after the HTTP
// server is ready. Small transactions and a pause between batches keep the
// live database responsive while old data is converted.
func BackfillGatewayRequestAggregates(ctx context.Context, db *gorm.DB) (int64, error) {
	return backfillGatewayRequestAggregates(ctx, db, gatewayRequestBackfillBatchSize, gatewayRequestBackfillPause)
}

func migrateGatewayRequestAggregates(db *gorm.DB) error {
	_, err := backfillGatewayRequestAggregates(context.Background(), db, gatewayRequestAggregateBatchSize, 0)
	return err
}

// invalidateRequestAggregatesForRangeTx keeps rare historical money corrections
// bounded: affected buckets are rebuilt later by the throttled backfill worker.
func invalidateRequestAggregatesForRangeTx(tx *gorm.DB, channelID uint, start, end time.Time) error {
	if tx == nil || channelID == 0 || !end.After(start) {
		return nil
	}
	bucketStart := aggregateBucketStart(start)
	bucketEnd := aggregateBucketCeil(end)
	logScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at < ?", channelID, bucketStart, bucketEnd)
	aggregateScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestAggregate{}).
		Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ?", channelID, bucketStart, bucketEnd)
	if err := ensureRequestAggregateResetVersionCompatible(logScope, aggregateScope); err != nil {
		return err
	}
	groupLogScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestLog{}).
		Where("channel_id = ? AND created_at >= ? AND created_at < ?", channelID, bucketStart, bucketEnd)
	if err := ensureGatewayRequestGroupCacheLogResetVersionCompatible(groupLogScope); err != nil {
		return err
	}
	groupScope := tx.Session(&gorm.Session{NewDB: true}).Model(&GatewayRequestGroupCacheAggregate{}).
		Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ?", channelID, bucketStart, bucketEnd)
	if err := ensureGatewayRequestGroupCacheResetVersionCompatible(groupScope); err != nil {
		return err
	}
	if err := tx.Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ? AND version <= ?", channelID, bucketStart, bucketEnd, gatewayRequestAggregateVersion).
		Delete(&GatewayRequestAggregate{}).Error; err != nil {
		return err
	}
	if err := tx.Where("channel_id = ? AND bucket_start >= ? AND bucket_start < ? AND version <= ?", channelID, bucketStart, bucketEnd, gatewayRequestGroupCacheAggregateVersion).
		Delete(&GatewayRequestGroupCacheAggregate{}).Error; err != nil {
		return err
	}
	return tx.Model(&GatewayRequestLog{}).Where("channel_id = ? AND created_at >= ? AND created_at < ? AND aggregate_version <= ? AND group_cache_aggregate_version <= ?", channelID, bucketStart, bucketEnd, gatewayRequestAggregateVersion, gatewayRequestGroupCacheAggregateVersion).
		Updates(map[string]any{
			"aggregate_version":             0,
			"group_cache_aggregate_version": 0,
		}).Error
}

func requestAggregatesReady(db *gorm.DB) (bool, error) {
	var log GatewayRequestLog
	err := db.Select("id").
		Where("aggregate_version < ? OR aggregate_version > ?", gatewayRequestAggregateVersion, gatewayRequestAggregateVersion).
		Order("aggregate_version ASC, id ASC").Take(&log).Error
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}
	var aggregate GatewayRequestAggregate
	err = db.Select("id").
		Where("version < ? OR version > ?", gatewayRequestAggregateVersion, gatewayRequestAggregateVersion).
		Order("version ASC, id ASC").Take(&aggregate).Error
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}
	return true, nil
}
