import type { DashboardMultiplierTrend, DashboardMultiplierTrendApp, DashboardMultiplierTrendPoint } from "@/lib/api-types"

export const dashboardMultiplierApps = ["codex", "claude"] as const
export const dashboardMultiplierMaximumRatio = 1_000_000

export type DashboardMultiplierAppName = typeof dashboardMultiplierApps[number]
export type DashboardMultiplierSeries = "input" | "output"

export type DashboardMultiplierCoverage = {
  inputTokens: number
  inputCoveredTokens: number
  inputSampleRequests: number
  outputTokens: number
  outputCoveredTokens: number
  outputSampleRequests: number
}

// This is deliberately a display-state contract rather than a data-quality
// inference. A successful request with an uncovered token denominator must not
// be collapsed into the empty state, and a dangling multiplier value without
// strict per-bucket coverage must not cause the UI to draw a line.
export type DashboardMultiplierTrendDisplayState = "empty" | "uncovered" | "chart"

export type DashboardMultiplierChartPoint = DashboardMultiplierTrendPoint & {
  chart_input_multiplier: number | null
  chart_output_multiplier: number | null
  input_has_coverage: boolean
  output_has_coverage: boolean
}

export function dashboardMultiplierTrendApp(data: DashboardMultiplierTrend | null | undefined, app: DashboardMultiplierAppName): DashboardMultiplierTrendApp {
  const found = data?.apps?.find((item) => item.app === app)
  return found ?? { app, points: [] }
}

export function dashboardMultiplierHasCoverage(data: DashboardMultiplierTrendApp | null | undefined) {
  return Boolean(data?.points.some(dashboardMultiplierPointHasCoverage))
}

export function dashboardMultiplierHasSeriesCoverage(data: DashboardMultiplierTrendApp | null | undefined, series: DashboardMultiplierSeries) {
  return Boolean(data?.points.some((point) => dashboardMultiplierPointHasSeriesCoverage(point, series)))
}

export function dashboardMultiplierSuccessfulRequests(data: DashboardMultiplierTrendApp | null | undefined) {
  return (data?.points ?? []).reduce((total, point) => total + nonNegative(point.successful_requests), 0)
}

export function dashboardMultiplierTrendDisplayState(data: DashboardMultiplierTrendApp | null | undefined): DashboardMultiplierTrendDisplayState {
  if (dashboardMultiplierSuccessfulRequests(data) === 0) return "empty"
  if (dashboardMultiplierHasCoverage(data)) return "chart"
  return "uncovered"
}

export function dashboardMultiplierCoverage(data: DashboardMultiplierTrendApp | null | undefined): DashboardMultiplierCoverage {
  return (data?.points ?? []).reduce<DashboardMultiplierCoverage>((total, point) => {
    const inputTokens = nonNegative(point.input_tokens)
    const inputCoveredTokens = dashboardMultiplierPointHasSeriesCoverage(point, "input")
      ? boundedCoverage(point.input_covered_tokens, inputTokens)
      : 0
    const outputTokens = nonNegative(point.output_tokens)
    const outputCoveredTokens = dashboardMultiplierPointHasSeriesCoverage(point, "output")
      ? boundedCoverage(point.output_covered_tokens, outputTokens)
      : 0
    return {
      inputTokens: total.inputTokens + inputTokens,
      inputCoveredTokens: total.inputCoveredTokens + inputCoveredTokens,
      inputSampleRequests: total.inputSampleRequests + coveredSampleRequests(point.input_sample_requests, inputCoveredTokens),
      outputTokens: total.outputTokens + outputTokens,
      outputCoveredTokens: total.outputCoveredTokens + outputCoveredTokens,
      outputSampleRequests: total.outputSampleRequests + coveredSampleRequests(point.output_sample_requests, outputCoveredTokens),
    }
  }, {
    inputTokens: 0,
    inputCoveredTokens: 0,
    inputSampleRequests: 0,
    outputTokens: 0,
    outputCoveredTokens: 0,
    outputSampleRequests: 0,
  })
}

export function dashboardMultiplierCoveragePercent(covered: number, total: number) {
  if (!Number.isFinite(covered) || !Number.isFinite(total) || total <= 0) return null
  return Math.max(0, Math.min(100, (Math.max(0, covered) / total) * 100))
}

export function dashboardMultiplierPointHasCoverage(point: DashboardMultiplierTrendPoint | null | undefined) {
  return dashboardMultiplierPointHasSeriesCoverage(point, "input") || dashboardMultiplierPointHasSeriesCoverage(point, "output")
}

export function dashboardMultiplierPointHasSeriesCoverage(point: DashboardMultiplierTrendPoint | null | undefined, series: DashboardMultiplierSeries) {
  if (!point || nonNegative(point.successful_requests) <= 0) return false
  const fields = series === "input"
    ? {
        multiplier: point.input_multiplier,
        smoothedMultiplier: point.smoothed_input_multiplier,
        tokens: point.input_tokens,
        coveredTokens: point.input_covered_tokens,
        sampleRequests: point.input_sample_requests,
        smoothedCoveredTokens: point.smoothed_input_covered_tokens,
        smoothedSampleRequests: point.smoothed_input_sample_requests,
      }
    : {
        multiplier: point.output_multiplier,
        smoothedMultiplier: point.smoothed_output_multiplier,
        tokens: point.output_tokens,
        coveredTokens: point.output_covered_tokens,
        sampleRequests: point.output_sample_requests,
        smoothedCoveredTokens: point.smoothed_output_covered_tokens,
        smoothedSampleRequests: point.smoothed_output_sample_requests,
      }
  const totalTokens = nonNegative(fields.tokens)
  const covered = nonNegative(fields.coveredTokens)
  return (
    dashboardMultiplierIsTrustedValue(fields.multiplier) &&
    dashboardMultiplierIsTrustedValue(fields.smoothedMultiplier) &&
    totalTokens > 0 &&
    covered > 0 &&
    covered <= totalTokens &&
    nonNegative(fields.sampleRequests) > 0 &&
    nonNegative(fields.smoothedCoveredTokens) >= covered &&
    nonNegative(fields.smoothedSampleRequests) >= nonNegative(fields.sampleRequests)
  )
}

export function dashboardMultiplierChartPoints(data: DashboardMultiplierTrendApp | null | undefined): DashboardMultiplierChartPoint[] {
  return (data?.points ?? []).map((point) => {
    const inputHasCoverage = dashboardMultiplierPointHasSeriesCoverage(point, "input")
    const outputHasCoverage = dashboardMultiplierPointHasSeriesCoverage(point, "output")
    return {
      ...point,
      chart_input_multiplier: inputHasCoverage ? point.smoothed_input_multiplier : null,
      chart_output_multiplier: outputHasCoverage ? point.smoothed_output_multiplier : null,
      input_has_coverage: inputHasCoverage,
      output_has_coverage: outputHasCoverage,
    }
  })
}

// Recharts can omit payload for a bucket with only null series values. The
// chart's raw bucket, rather than a localized display label, is the stable
// lookup key across time zones and DST changes.
export function dashboardMultiplierPointForBucket<T extends { bucket: string }>(points: T[], bucket: string | number | null | undefined) {
  const expectedBucket = String(bucket ?? "")
  if (!expectedBucket) return null
  return points.find((point) => point.bucket === expectedBucket) ?? null
}

function boundedCoverage(coveredTokens: number | null | undefined, totalTokens: number) {
  const covered = nonNegative(coveredTokens)
  return covered <= totalTokens ? covered : 0
}

function coveredSampleRequests(sampleRequests: number | null | undefined, coveredTokens: number) {
  return coveredTokens > 0 ? nonNegative(sampleRequests) : 0
}

export function dashboardMultiplierIsTrustedValue(value: number | null | undefined) {
  return value != null && Number.isFinite(value) && value >= 0 && value <= dashboardMultiplierMaximumRatio
}

function nonNegative(value: number | null | undefined) {
  return Number.isFinite(value) ? Math.max(0, Number(value)) : 0
}
