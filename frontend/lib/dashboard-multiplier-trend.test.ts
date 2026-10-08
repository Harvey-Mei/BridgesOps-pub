import assert from "node:assert/strict"
import test from "node:test"

import {
  dashboardMultiplierChartPoints,
  dashboardMultiplierCoverage,
  dashboardMultiplierCoveragePercent,
  dashboardMultiplierHasCoverage,
  dashboardMultiplierHasSeriesCoverage,
  dashboardMultiplierIsTrustedValue,
  dashboardMultiplierMaximumRatio,
  dashboardMultiplierPointForBucket,
  dashboardMultiplierSuccessfulRequests,
  dashboardMultiplierTrendApp,
  dashboardMultiplierTrendDisplayState,
} from "./dashboard-multiplier-trend"
import type { DashboardMultiplierTrend, DashboardMultiplierTrendPoint } from "./api-types"

function point(values: Partial<DashboardMultiplierTrendPoint> = {}): DashboardMultiplierTrendPoint {
  return {
    bucket: "2026-07-24T00:00:00Z",
    successful_requests: 0,
    input_tokens: 0,
    output_tokens: 0,
    input_covered_tokens: 0,
    output_covered_tokens: 0,
    input_sample_requests: 0,
    output_sample_requests: 0,
    input_multiplier: null,
    output_multiplier: null,
    smoothed_input_multiplier: null,
    smoothed_output_multiplier: null,
    smoothed_input_covered_tokens: 0,
    smoothed_output_covered_tokens: 0,
    smoothed_input_sample_requests: 0,
    smoothed_output_sample_requests: 0,
    ...values,
  }
}

test("dashboard multiplier series stays separated by frozen app", () => {
  const codexPoint = point({
    successful_requests: 1,
    input_tokens: 101,
    input_covered_tokens: 101,
    input_sample_requests: 1,
    input_multiplier: 2,
    smoothed_input_multiplier: 2,
    smoothed_input_covered_tokens: 101,
    smoothed_input_sample_requests: 1,
  })
  const claudePoint = point({ successful_requests: 1, input_tokens: 202 })
  const data: DashboardMultiplierTrend = {
    bucket_seconds: 7200,
    smoothing_buckets: 3,
    apps: [
      { app: "claude", points: [claudePoint] },
      { app: "codex", points: [codexPoint] },
    ],
  }

  const codex = dashboardMultiplierTrendApp(data, "codex")
  const claude = dashboardMultiplierTrendApp(data, "claude")
  assert.deepEqual(codex.points, [codexPoint])
  assert.deepEqual(claude.points, [claudePoint])
  assert.equal(dashboardMultiplierTrendDisplayState(codex), "chart")
  assert.equal(dashboardMultiplierTrendDisplayState(claude), "uncovered")
})

test("dashboard multiplier coverage is token based, clamps invalid fields, and preserves separate output facts", () => {
  const coverage = dashboardMultiplierCoverage({
    app: "codex",
    points: [
      point({
        successful_requests: 2,
        input_tokens: 100,
        input_covered_tokens: 80,
        input_sample_requests: 2,
        input_multiplier: 2,
        smoothed_input_multiplier: 2,
        smoothed_input_covered_tokens: 80,
        smoothed_input_sample_requests: 2,
        output_tokens: 40,
        output_covered_tokens: 0,
        output_sample_requests: 0,
      }),
      point({
        bucket: "2026-07-24T02:00:00Z",
        successful_requests: 1,
        input_tokens: -5,
        input_covered_tokens: 20,
        input_sample_requests: 1,
        output_tokens: 60,
        output_covered_tokens: 55,
        output_sample_requests: 1,
        output_multiplier: 3,
        smoothed_output_multiplier: 3,
        smoothed_output_covered_tokens: 55,
        smoothed_output_sample_requests: 1,
      }),
    ],
  })

  assert.deepEqual(coverage, {
    inputTokens: 100,
    inputCoveredTokens: 80,
    inputSampleRequests: 2,
    outputTokens: 100,
    outputCoveredTokens: 55,
    outputSampleRequests: 1,
  })
  assert.ok(Math.abs((dashboardMultiplierCoveragePercent(55, 100) ?? 0) - 55) < 0.000001)
  assert.equal(dashboardMultiplierCoveragePercent(120, 100), 100)
  assert.equal(dashboardMultiplierCoveragePercent(1, 0), null)
})

test("dashboard multiplier coverage fails closed when a malformed payload exceeds its token denominator", () => {
  const coverage = dashboardMultiplierCoverage({
    app: "codex",
    points: [point({
      successful_requests: 1,
      input_tokens: 100,
      input_covered_tokens: 101,
      input_sample_requests: 1,
      output_tokens: 10,
      output_covered_tokens: 11,
      output_sample_requests: 1,
    })],
  })

  assert.deepEqual(coverage, {
    inputTokens: 100,
    inputCoveredTokens: 0,
    inputSampleRequests: 0,
    outputTokens: 10,
    outputCoveredTokens: 0,
    outputSampleRequests: 0,
  })
})

test("dashboard multiplier display state preserves successful uncovered denominators without a curve", () => {
  const data = {
    app: "codex",
    points: [point({
      successful_requests: 3,
      input_tokens: 1200,
      output_tokens: 240,
      input_covered_tokens: 0,
      output_covered_tokens: 0,
    })],
  }

  assert.equal(dashboardMultiplierTrendDisplayState(data), "uncovered")
  assert.equal(dashboardMultiplierHasCoverage(data), false)
  assert.equal(dashboardMultiplierSuccessfulRequests(data), 3)
  assert.deepEqual(dashboardMultiplierCoverage(data), {
    inputTokens: 1200,
    inputCoveredTokens: 0,
    inputSampleRequests: 0,
    outputTokens: 240,
    outputCoveredTokens: 0,
    outputSampleRequests: 0,
  })
})

test("dashboard multiplier rejects dangling smoothed values without a successful strict audit fact", () => {
  const noSuccess = {
    app: "codex",
    points: [point({
      input_tokens: 100,
      input_covered_tokens: 100,
      input_sample_requests: 1,
      input_multiplier: 2,
      smoothed_input_multiplier: 2,
      smoothed_input_covered_tokens: 100,
      smoothed_input_sample_requests: 1,
    })],
  }
  const noCoverage = {
    app: "claude",
    points: [point({
      successful_requests: 1,
      input_tokens: 100,
      input_multiplier: 2,
      smoothed_input_multiplier: 2,
    })],
  }
  const zeroDenominator = {
    app: "claude",
    points: [point({
      successful_requests: 1,
      input_covered_tokens: 100,
      input_sample_requests: 1,
      input_multiplier: 2,
      smoothed_input_multiplier: 2,
      smoothed_input_covered_tokens: 100,
      smoothed_input_sample_requests: 1,
    })],
  }
  const legacyDangling = {
    app: "claude",
    points: [point({
      input_multiplier: 2,
      smoothed_input_multiplier: 2,
      smoothed_input_covered_tokens: 100,
      smoothed_input_sample_requests: 1,
    })],
  }

  assert.equal(dashboardMultiplierTrendDisplayState(noSuccess), "empty")
  assert.equal(dashboardMultiplierHasCoverage(noSuccess), false)
  assert.equal(dashboardMultiplierTrendDisplayState(noCoverage), "uncovered")
  assert.equal(dashboardMultiplierHasCoverage(noCoverage), false)
  assert.deepEqual(dashboardMultiplierCoverage(noCoverage), {
    inputTokens: 100,
    inputCoveredTokens: 0,
    inputSampleRequests: 0,
    outputTokens: 0,
    outputCoveredTokens: 0,
    outputSampleRequests: 0,
  })
  assert.equal(dashboardMultiplierTrendDisplayState(zeroDenominator), "uncovered")
  assert.equal(dashboardMultiplierHasCoverage(zeroDenominator), false)
  assert.deepEqual(dashboardMultiplierCoverage(zeroDenominator), {
    inputTokens: 0,
    inputCoveredTokens: 0,
    inputSampleRequests: 0,
    outputTokens: 0,
    outputCoveredTokens: 0,
    outputSampleRequests: 0,
  })
  assert.equal(dashboardMultiplierTrendDisplayState(legacyDangling), "empty")
  assert.equal(dashboardMultiplierHasCoverage(legacyDangling), false)
})

test("dashboard multiplier display state distinguishes no successes from frozen-coverage charts", () => {
  assert.equal(dashboardMultiplierTrendDisplayState({ app: "claude", points: [] }), "empty")

  assert.equal(dashboardMultiplierHasCoverage({
    app: "claude",
    points: [point({
      input_multiplier: 1.5,
      output_multiplier: null,
      smoothed_input_multiplier: null,
      smoothed_output_multiplier: null,
    })],
  }), false)

  const covered = {
    app: "claude",
    points: [point({
      successful_requests: 1,
      input_tokens: 100,
      input_covered_tokens: 100,
      input_sample_requests: 1,
      input_multiplier: 1.5,
      smoothed_input_multiplier: 1.5,
      smoothed_output_multiplier: null,
      smoothed_input_covered_tokens: 100,
      smoothed_input_sample_requests: 1,
    })],
  }
  assert.equal(dashboardMultiplierHasCoverage(covered), true)
  assert.equal(dashboardMultiplierTrendDisplayState(covered), "chart")
})

test("dashboard multiplier draws only bounded, per-series frozen facts and preserves a 0x fact", () => {
  const validInput = (bucket: string, multiplier: number) => point({
    bucket,
    successful_requests: 1,
    input_tokens: 100,
    input_covered_tokens: 100,
    input_sample_requests: 1,
    input_multiplier: multiplier,
    smoothed_input_multiplier: multiplier,
    smoothed_input_covered_tokens: 100,
    smoothed_input_sample_requests: 1,
  })
  const zero = validInput("2026-07-24T00:00:00Z", 0)
  const gap = point({
    bucket: "2026-07-24T02:00:00Z",
    successful_requests: 1,
    input_tokens: 100,
    input_covered_tokens: 100,
    input_sample_requests: 1,
    input_multiplier: 2,
    smoothed_input_multiplier: 2,
  })
  const final = validInput("2026-07-24T04:00:00Z", 4)
  const data = { app: "codex", points: [zero, gap, final] }
  const rows = dashboardMultiplierChartPoints(data)

  assert.equal(dashboardMultiplierIsTrustedValue(0), true)
  assert.equal(dashboardMultiplierIsTrustedValue(-0.01), false)
  assert.equal(dashboardMultiplierIsTrustedValue(Number.NaN), false)
  assert.equal(dashboardMultiplierIsTrustedValue(Number.POSITIVE_INFINITY), false)
  assert.equal(dashboardMultiplierIsTrustedValue(dashboardMultiplierMaximumRatio + 1), false)
  assert.equal(dashboardMultiplierHasSeriesCoverage(data, "input"), true)
  assert.equal(dashboardMultiplierHasSeriesCoverage(data, "output"), false)
  assert.equal(rows[0]?.chart_input_multiplier, 0)
  assert.equal(rows[0]?.chart_output_multiplier, null)
  assert.equal(rows[1]?.chart_input_multiplier, null)
  assert.equal(rows[2]?.chart_input_multiplier, 4)
  assert.equal(dashboardMultiplierPointForBucket(rows, "2026-07-24T02:00:00Z"), rows[1])
  assert.equal(dashboardMultiplierPointForBucket(rows, "localized label must not match"), null)
})

test("dashboard multiplier rejects invalid bounded values rather than charting a malformed payload", () => {
  for (const multiplier of [-0.01, Number.NaN, Number.NEGATIVE_INFINITY, Number.POSITIVE_INFINITY, dashboardMultiplierMaximumRatio + 1]) {
    const data = {
      app: "claude",
      points: [point({
        successful_requests: 1,
        output_tokens: 50,
        output_covered_tokens: 50,
        output_sample_requests: 1,
        output_multiplier: multiplier,
        smoothed_output_multiplier: multiplier,
        smoothed_output_covered_tokens: 50,
        smoothed_output_sample_requests: 1,
      })],
    }
    assert.equal(dashboardMultiplierTrendDisplayState(data), "uncovered")
    assert.equal(dashboardMultiplierChartPoints(data)[0]?.chart_output_multiplier, null)
    assert.deepEqual(dashboardMultiplierCoverage(data), {
      inputTokens: 0,
      inputCoveredTokens: 0,
      inputSampleRequests: 0,
      outputTokens: 50,
      outputCoveredTokens: 0,
      outputSampleRequests: 0,
    })
  }
})
