package storage

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	// DashboardMultiplierTrendMaximumRange bounds raw request-log work for the
	// overview multiplier view; longer dashboard windows must be split by UI.
	DashboardMultiplierTrendMaximumRange     = 31 * 24 * time.Hour
	dashboardMultiplierTrendSmoothingBuckets = 3
	// This is a data-integrity bound, not a product price cap. A persisted
	// multiplier outside it cannot describe a useful request fact and must not
	// dominate a user-facing weighted average.
	dashboardMultiplierTrendMaximumRatio = 1_000_000
)

var dashboardMultiplierTrendApps = []string{
	string(GatewayAppCodex),
	string(GatewayAppClaude),
}

// DashboardMultiplierTrend is the dashboard's deliberately narrow view of
// frozen dispatch multiplier facts. It never joins current rate snapshots,
// current downstream-key apps, model names, or price facts. The successful
// request log supplies only the user-visible result, timestamp, token usage,
// and its already-recorded app bucket for honest uncovered denominators. The
// multipliers themselves come only from the exactly-linked final
// physical-dispatch audit, where they were frozen before the upstream send.
type DashboardMultiplierTrend struct {
	BucketSeconds    int64                         `json:"bucket_seconds"`
	SmoothingBuckets int                           `json:"smoothing_buckets"`
	Apps             []DashboardMultiplierTrendApp `json:"apps"`
}

type DashboardMultiplierTrendApp struct {
	App    string                          `json:"app"`
	Points []DashboardMultiplierTrendPoint `json:"points"`
}

// DashboardMultiplierTrendPoint keeps input and output independent. Input
// means non-cache prompt tokens; cache read/write do not have an independently
// frozen multiplier on the request, so they are excluded rather than silently
// attributed to ordinary input. Output means completion tokens.
//
// Covered tokens are the denominator for the multiplier. Total tokens include
// all successful user requests in this app/bucket, including rows whose frozen
// multiplier is absent or invalid, so the UI can show honest coverage.
type DashboardMultiplierTrendPoint struct {
	Bucket                        string   `json:"bucket"`
	SuccessfulRequests            int64    `json:"successful_requests"`
	InputTokens                   int64    `json:"input_tokens"`
	OutputTokens                  int64    `json:"output_tokens"`
	InputCoveredTokens            int64    `json:"input_covered_tokens"`
	OutputCoveredTokens           int64    `json:"output_covered_tokens"`
	InputSampleRequests           int64    `json:"input_sample_requests"`
	OutputSampleRequests          int64    `json:"output_sample_requests"`
	InputMultiplier               *float64 `json:"input_multiplier"`
	OutputMultiplier              *float64 `json:"output_multiplier"`
	SmoothedInputMultiplier       *float64 `json:"smoothed_input_multiplier"`
	SmoothedOutputMultiplier      *float64 `json:"smoothed_output_multiplier"`
	SmoothedInputCoveredTokens    int64    `json:"smoothed_input_covered_tokens"`
	SmoothedOutputCoveredTokens   int64    `json:"smoothed_output_covered_tokens"`
	SmoothedInputSampleRequests   int64    `json:"smoothed_input_sample_requests"`
	SmoothedOutputSampleRequests  int64    `json:"smoothed_output_sample_requests"`
	inputWeightedMultiplierTotal  float64
	outputWeightedMultiplierTotal float64
}

type dashboardMultiplierTrendBucket struct {
	start time.Time
	end   time.Time
	label string
}

type dashboardMultiplierTrendRow struct {
	Bucket                        string
	App                           string
	SuccessfulRequests            int64
	InputTokens                   int64
	OutputTokens                  int64
	InputCoveredTokens            int64
	OutputCoveredTokens           int64
	InputSampleRequests           int64
	OutputSampleRequests          int64
	InputWeightedMultiplierTotal  float64
	OutputWeightedMultiplierTotal float64
}

// DashboardMultiplierTrend returns at most 32 bounded buckets for the
// selected dashboard range. Each component is weighted by actual successful
// user-token usage and smoothed with the current plus two contiguous preceding
// buckets. An uncovered bucket resets that component's smoothing window, so
// stale multipliers are never carried over a factual gap.
func (g *Gateway) DashboardMultiplierTrend(start, end time.Time, timeZone string) (*DashboardMultiplierTrend, error) {
	if g == nil || g.db == nil {
		return nil, errors.New("gateway store is unavailable")
	}
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return nil, errors.New("dashboard multiplier trend requires a non-empty time range")
	}
	start = start.UTC()
	end = end.UTC()
	if end.Sub(start) > DashboardMultiplierTrendMaximumRange {
		return nil, fmt.Errorf("dashboard multiplier trend range exceeds %s", DashboardMultiplierTrendMaximumRange)
	}
	location, err := dashboardMultiplierTrendLocation(timeZone)
	if err != nil {
		return nil, err
	}

	bucketDuration := dashboardMultiplierTrendBucketDuration(end.Sub(start))
	buckets := dashboardMultiplierTrendBuckets(start, end, bucketDuration, location)
	trend := &DashboardMultiplierTrend{
		BucketSeconds:    int64(bucketDuration / time.Second),
		SmoothingBuckets: dashboardMultiplierTrendSmoothingBuckets,
		Apps: []DashboardMultiplierTrendApp{
			{App: string(GatewayAppCodex), Points: dashboardMultiplierTrendEmptyPoints(buckets)},
			{App: string(GatewayAppClaude), Points: dashboardMultiplierTrendEmptyPoints(buckets)},
		},
	}
	if len(buckets) == 0 {
		return trend, nil
	}

	bucketSQL, bucketArgs := dashboardMultiplierTrendBucketSQLForTimestamp(buckets, "request_log.created_at")
	inputTokensSQL := "GREATEST(COALESCE(request_log.prompt_tokens, 0) - COALESCE(request_log.cache_creation_tokens, 0) - COALESCE(request_log.cache_read_tokens, 0), 0)"
	outputTokensSQL := "GREATEST(COALESCE(request_log.completion_tokens, 0), 0)"
	inputRatioValidSQL := dashboardMultiplierTrendRatioValidSQL("dispatch_audit.group_ratio")
	outputRatioValidSQL := dashboardMultiplierTrendRatioValidSQL("dispatch_audit.completion_ratio")

	rows := []dashboardMultiplierTrendRow{}
	query := g.db.Table("gateway_request_logs AS request_log").
		Joins(dashboardMultiplierTrendFinalDispatchAuditJoinSQL()).
		Where(dashboardMultiplierSuccessfulUserRequestSQL("request_log")).
		Where("request_log.telemetry_version = 1 AND LOWER(BTRIM(COALESCE(request_log.gateway_app, ''))) IN (?, ?)", string(GatewayAppCodex), string(GatewayAppClaude)).
		Where("request_log.created_at >= ? AND request_log.created_at < ?", g.requestLogTimeBoundary(start), g.requestLogTimeBoundary(end)).
		Select(bucketSQL+` AS bucket,
			LOWER(BTRIM(COALESCE(request_log.gateway_app, ''))) AS app,
			COALESCE(SUM(1), 0) AS successful_requests,
			COALESCE(SUM(`+inputTokensSQL+`), 0) AS input_tokens,
			COALESCE(SUM(`+outputTokensSQL+`), 0) AS output_tokens,
			COALESCE(SUM(CASE WHEN `+inputRatioValidSQL+` THEN `+inputTokensSQL+` ELSE 0 END), 0) AS input_covered_tokens,
			COALESCE(SUM(CASE WHEN `+outputRatioValidSQL+` THEN `+outputTokensSQL+` ELSE 0 END), 0) AS output_covered_tokens,
			COALESCE(SUM(CASE WHEN `+inputRatioValidSQL+` AND `+inputTokensSQL+` > 0 THEN 1 ELSE 0 END), 0) AS input_sample_requests,
			COALESCE(SUM(CASE WHEN `+outputRatioValidSQL+` AND `+outputTokensSQL+` > 0 THEN 1 ELSE 0 END), 0) AS output_sample_requests,
			COALESCE(SUM(CASE WHEN `+inputRatioValidSQL+` THEN (`+inputTokensSQL+` * dispatch_audit.group_ratio) ELSE 0 END), 0) AS input_weighted_multiplier_total,
			COALESCE(SUM(CASE WHEN `+outputRatioValidSQL+` THEN (`+outputTokensSQL+` * dispatch_audit.completion_ratio) ELSE 0 END), 0) AS output_weighted_multiplier_total`, bucketArgs...).
		Group("bucket, LOWER(BTRIM(COALESCE(request_log.gateway_app, '')))").
		Order("bucket ASC, app ASC")
	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	appIndex := map[string]int{
		string(GatewayAppCodex):  0,
		string(GatewayAppClaude): 1,
	}
	pointIndex := make(map[string]int, len(buckets))
	for index, bucket := range buckets {
		pointIndex[bucket.label] = index
	}
	for _, row := range rows {
		app := strings.ToLower(strings.TrimSpace(row.App))
		appPosition, appOK := appIndex[app]
		pointPosition, pointOK := pointIndex[row.Bucket]
		if !appOK || !pointOK {
			continue
		}
		point := &trend.Apps[appPosition].Points[pointPosition]
		point.SuccessfulRequests = max(point.SuccessfulRequests+row.SuccessfulRequests, 0)
		point.InputTokens = max(point.InputTokens+row.InputTokens, 0)
		point.OutputTokens = max(point.OutputTokens+row.OutputTokens, 0)
		point.InputCoveredTokens = max(point.InputCoveredTokens+row.InputCoveredTokens, 0)
		point.OutputCoveredTokens = max(point.OutputCoveredTokens+row.OutputCoveredTokens, 0)
		point.InputSampleRequests = max(point.InputSampleRequests+row.InputSampleRequests, 0)
		point.OutputSampleRequests = max(point.OutputSampleRequests+row.OutputSampleRequests, 0)
		point.inputWeightedMultiplierTotal += row.InputWeightedMultiplierTotal
		point.outputWeightedMultiplierTotal += row.OutputWeightedMultiplierTotal
	}
	for appIndex := range trend.Apps {
		dashboardMultiplierTrendFinalizePoints(trend.Apps[appIndex].Points)
	}
	return trend, nil
}

func dashboardMultiplierTrendBucketDuration(rangeDuration time.Duration) time.Duration {
	switch {
	case rangeDuration <= 48*time.Hour:
		return 2 * time.Hour
	case rangeDuration <= 8*24*time.Hour:
		return 12 * time.Hour
	default:
		return 24 * time.Hour
	}
}

func dashboardMultiplierTrendLocation(timeZone string) (*time.Location, error) {
	name := strings.TrimSpace(timeZone)
	if name == "" {
		return time.UTC, nil
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("dashboard multiplier trend time zone invalid: %w", err)
	}
	return location, nil
}

func dashboardMultiplierSuccessfulUserRequestSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	routeReasonSQL := "LOWER(TRIM(COALESCE(" + prefix + "route_reason, '')))"
	pathSQL := "LOWER(TRIM(COALESCE(" + prefix + "path, '')))"
	methodSQL := "UPPER(TRIM(COALESCE(" + prefix + "method, '')))"
	return "(" + successfulGatewayRequestSQL(alias) +
		" AND " + prefix + "downstream_key_id > 0" +
		" AND " + methodSQL + " = 'POST'" +
		" AND " + pathSQL + " IN ('/v1/chat/completions', '/v1/completions', '/v1/embeddings', '/v1/responses', '/v1/messages')" +
		" AND " + routeReasonSQL + " NOT IN ('" + gatewayRouteReasonLatencyProbe + "', '" + gatewayRouteReasonKeyTest + "', 'model_check', 'internal')" +
		" AND LEFT(" + routeReasonSQL + ", 9) <> 'internal_')"
}

func dashboardMultiplierTrendBuckets(start, end time.Time, bucketDuration time.Duration, location *time.Location) []dashboardMultiplierTrendBucket {
	if !end.After(start) || bucketDuration <= 0 {
		return nil
	}
	if location == nil {
		location = time.UTC
	}
	first := dashboardMultiplierTrendLocalBucketStart(start, bucketDuration, location)
	buckets := make([]dashboardMultiplierTrendBucket, 0, 32)
	for bucketStart := first; bucketStart.Before(end); {
		bucketEnd := dashboardMultiplierTrendNextLocalBucketStart(bucketStart, bucketDuration, location)
		if !bucketEnd.After(bucketStart) {
			bucketEnd = bucketStart.Add(time.Hour)
		}
		boundedEnd := bucketEnd
		if boundedEnd.After(end) {
			boundedEnd = end
		}
		buckets = append(buckets, dashboardMultiplierTrendBucket{
			start: bucketStart.UTC(),
			end:   boundedEnd.UTC(),
			label: dashboardMultiplierTrendBucketLabel(bucketStart, bucketDuration, location),
		})
		bucketStart = bucketEnd
	}
	return buckets
}

func dashboardMultiplierTrendLocalBucketStart(value time.Time, bucketDuration time.Duration, location *time.Location) time.Time {
	local := value.In(location)
	if bucketDuration >= 24*time.Hour {
		return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, location)
	}
	hours := int(bucketDuration / time.Hour)
	if hours <= 0 {
		return value.UTC()
	}
	return time.Date(local.Year(), local.Month(), local.Day(), (local.Hour()/hours)*hours, 0, 0, 0, location)
}

func dashboardMultiplierTrendNextLocalBucketStart(value time.Time, bucketDuration time.Duration, location *time.Location) time.Time {
	local := value.In(location)
	if bucketDuration >= 24*time.Hour {
		return time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, location)
	}
	return time.Date(local.Year(), local.Month(), local.Day(), local.Hour()+int(bucketDuration/time.Hour), 0, 0, 0, location)
}

func dashboardMultiplierTrendBucketLabel(value time.Time, bucketDuration time.Duration, location *time.Location) string {
	if bucketDuration >= 24*time.Hour {
		return value.In(location).Format("2006-01-02")
	}
	return value.UTC().Format(time.RFC3339)
}

// dashboardMultiplierTrendBucketSQLForTimestamp builds a closed set of
// parameterized bucket predicates for a storage-owned timestamp expression.
// The expression is deliberately supplied by the caller rather than inferred
// from an unqualified column name: multiplier reads join two tables that both
// have timestamps, and an accidental unqualified reference must not silently
// start bucketing by dispatch time.
func dashboardMultiplierTrendBucketSQLForTimestamp(buckets []dashboardMultiplierTrendBucket, timestampSQL string) (string, []any) {
	if len(buckets) == 0 {
		return "NULL", nil
	}
	timestampSQL = strings.TrimSpace(timestampSQL)
	if timestampSQL == "" {
		return "NULL", nil
	}
	var expression strings.Builder
	expression.WriteString("CASE")
	args := make([]any, 0, len(buckets)*3)
	for _, bucket := range buckets {
		expression.WriteString(" WHEN ")
		expression.WriteString(timestampSQL)
		expression.WriteString(" >= ? AND ")
		expression.WriteString(timestampSQL)
		expression.WriteString(" < ? THEN ?")
		args = append(args, bucket.start, bucket.end, bucket.label)
	}
	expression.WriteString(" END")
	return expression.String(), args
}

// dashboardMultiplierTrendBucketSQL is retained for storage-local callers
// that work with one table. Joined reads must instead name their timestamp
// source explicitly through dashboardMultiplierTrendBucketSQLForTimestamp.
func dashboardMultiplierTrendBucketSQL(buckets []dashboardMultiplierTrendBucket) (string, []any) {
	return dashboardMultiplierTrendBucketSQLForTimestamp(buckets, "created_at")
}

// dashboardMultiplierTrendFinalDispatchAuditJoinSQL is the historical-fact
// fence for the overview. Request logs provide the successful user-token
// denominator and their already-recorded app bucket; only an audit that was
// finalized for this exact log and logical trace can supply a multiplier.
// This is intentionally a LEFT JOIN: a v1 request without a trustworthy
// dispatch fact remains visible as zero coverage instead of disappearing and
// making the coverage percentage look better than it is.
//
// The identity and generation guards make old/incomplete rows honest gaps.
// They must not be backfilled from current channel, group, or pricing state:
// doing so would let later configuration changes rewrite the historical view.
func dashboardMultiplierTrendFinalDispatchAuditJoinSQL() string {
	return `LEFT JOIN gateway_request_dispatch_audits AS dispatch_audit
		ON dispatch_audit.request_log_id = request_log.id
		AND request_log.telemetry_version = 1
		AND request_log.logical_request_trace_id <> ''
		AND dispatch_audit.telemetry_version = 1
		AND dispatch_audit.multiplier_provenance_version = 1
		AND dispatch_audit.logical_request_trace_id <> ''
		AND dispatch_audit.logical_request_trace_id = request_log.logical_request_trace_id
		AND dispatch_audit.gateway_app = request_log.gateway_app
		AND dispatch_audit.downstream_key_id = request_log.downstream_key_id
		AND dispatch_audit.channel_id = request_log.channel_id
		AND dispatch_audit.group_name = request_log.group_name
		AND dispatch_audit.upstream_key_id = request_log.upstream_key_id
		AND dispatch_audit.upstream_key_name = request_log.upstream_key_name
		AND dispatch_audit.channel_identity_revision = request_log.channel_identity_revision
		AND dispatch_audit.upstream_group_id = request_log.upstream_group_id
		AND dispatch_audit.upstream_group_generation = request_log.upstream_group_generation
		AND dispatch_audit.upstream_key_local_id = request_log.upstream_key_local_id
		AND dispatch_audit.upstream_key_generation = request_log.upstream_key_generation
		AND dispatch_audit.physically_dispatched = TRUE
		AND dispatch_audit.stage = 'finalized'
		AND dispatch_audit.terminal_at IS NOT NULL
		AND dispatch_audit.terminal_kind = 'success'
		AND dispatch_audit.channel_id > 0
		AND dispatch_audit.channel_identity_revision > 0
		AND dispatch_audit.upstream_group_id > 0
		AND dispatch_audit.upstream_group_generation > 0
		AND dispatch_audit.upstream_key_local_id > 0
		AND dispatch_audit.upstream_key_generation > 0
		AND NOT ` + dashboardMultiplierTrendPricingHardFenceSQL("dispatch_audit") + `
		AND NOT EXISTS (
			SELECT 1
			FROM gateway_request_dispatch_audits AS duplicate_audit
			WHERE duplicate_audit.request_log_id = dispatch_audit.request_log_id
				AND duplicate_audit.id <> dispatch_audit.id
		)`
}

// dashboardMultiplierTrendPricingHardFenceSQL keeps a hard-fenced audit in
// the request-log denominator but prevents its default-zero multiplier fields
// from being mistaken for a valid free-credit fact.
func dashboardMultiplierTrendPricingHardFenceSQL(alias string) string {
	prefix := ""
	if strings.TrimSpace(alias) != "" {
		prefix = strings.TrimSpace(alias) + "."
	}
	reason := "LOWER(TRIM(COALESCE(" + prefix + "cost_unavailable_reason, '')))"
	return "(" + reason + " IN ('" + GatewayPricingUnavailableScopeIncomplete + "', '" + GatewayPricingUnavailableIdentityChanged + "'))"
}

// dashboardMultiplierTrendRatioValidSQL accepts every finite non-negative
// frozen effective multiplier, including a legitimate 0x free-credit rate.
// It refuses negative, non-finite, and implausibly large values. The argument
// is storage-owned SQL (not user input); an empty expression fails closed for
// future callers.
func dashboardMultiplierTrendRatioValidSQL(columnSQL string) string {
	columnSQL = strings.TrimSpace(columnSQL)
	if columnSQL == "" {
		return "FALSE"
	}
	return "(" + columnSQL + " >= 0 AND " + columnSQL + " <= " + fmt.Sprintf("%d", dashboardMultiplierTrendMaximumRatio) + ")"
}

func dashboardMultiplierTrendEmptyPoints(buckets []dashboardMultiplierTrendBucket) []DashboardMultiplierTrendPoint {
	points := make([]DashboardMultiplierTrendPoint, len(buckets))
	for index, bucket := range buckets {
		points[index].Bucket = bucket.label
	}
	return points
}

func dashboardMultiplierTrendFinalizePoints(points []DashboardMultiplierTrendPoint) {
	for index := range points {
		if points[index].InputCoveredTokens > 0 && finiteDashboardMultiplierTotal(points[index].inputWeightedMultiplierTotal) {
			value := points[index].inputWeightedMultiplierTotal / float64(points[index].InputCoveredTokens)
			if finiteDashboardMultiplier(value) {
				points[index].InputMultiplier = &value
			}
		}
		if points[index].OutputCoveredTokens > 0 && finiteDashboardMultiplierTotal(points[index].outputWeightedMultiplierTotal) {
			value := points[index].outputWeightedMultiplierTotal / float64(points[index].OutputCoveredTokens)
			if finiteDashboardMultiplier(value) {
				points[index].OutputMultiplier = &value
			}
		}
	}

	for index := range points {
		// A missing frozen fact resets the component's trailing window. Skipping a
		// gap would let a much older multiplier visually bleed into current data.
		if points[index].InputCoveredTokens > 0 {
			start := index
			for start > 0 && index-start+1 < dashboardMultiplierTrendSmoothingBuckets && points[start-1].InputCoveredTokens > 0 {
				start--
			}
			var weights, weighted float64
			var samples int64
			for previous := start; previous <= index; previous++ {
				weights += float64(points[previous].InputCoveredTokens)
				weighted += points[previous].inputWeightedMultiplierTotal
				samples += points[previous].InputSampleRequests
			}
			if weights > 0 && finiteDashboardMultiplierTotal(weighted) {
				value := weighted / weights
				if finiteDashboardMultiplier(value) {
					points[index].SmoothedInputMultiplier = &value
					points[index].SmoothedInputCoveredTokens = int64(weights)
					points[index].SmoothedInputSampleRequests = samples
				}
			}
		}
		if points[index].OutputCoveredTokens > 0 {
			start := index
			for start > 0 && index-start+1 < dashboardMultiplierTrendSmoothingBuckets && points[start-1].OutputCoveredTokens > 0 {
				start--
			}
			var weights, weighted float64
			var samples int64
			for previous := start; previous <= index; previous++ {
				weights += float64(points[previous].OutputCoveredTokens)
				weighted += points[previous].outputWeightedMultiplierTotal
				samples += points[previous].OutputSampleRequests
			}
			if weights > 0 && finiteDashboardMultiplierTotal(weighted) {
				value := weighted / weights
				if finiteDashboardMultiplier(value) {
					points[index].SmoothedOutputMultiplier = &value
					points[index].SmoothedOutputCoveredTokens = int64(weights)
					points[index].SmoothedOutputSampleRequests = samples
				}
			}
		}
	}
}

func finiteDashboardMultiplier(value float64) bool {
	return value >= 0 && value <= dashboardMultiplierTrendMaximumRatio && !math.IsNaN(value) && !math.IsInf(value, 0)
}

// A weighted total is an intermediate token-times-multiplier quantity, so it
// may legitimately exceed the display multiplier bound. It must nevertheless
// remain finite and non-negative before division produces a final multiplier.
func finiteDashboardMultiplierTotal(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}
