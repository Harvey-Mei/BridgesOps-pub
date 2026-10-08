import assert from "node:assert/strict"
import test from "node:test"
import {
  gatewayUsageTabPath,
  gatewayUsageTabRoute,
  gatewayUsageRouteBoundaryPath,
} from "./gateway-usage-tabs"
import {
  channelHealthDoughnutLayout,
  doughnutChartRingWidth,
  overviewChannelCostDoughnutLayout,
  usageBreakdownDoughnutLayout,
} from "./doughnut-chart-layout"
import { gatewayUsageRequestTableClassName } from "./gateway-usage-table-style"
import {
  gatewayCostComparisonLabel,
  gatewayCostComparison,
  gatewayCostDifferenceRatioText,
  gatewayCostDifferenceText,
  gatewayCostDisplayAmount,
  gatewayCostDisplayText,
  gatewayCostDisplaySource,
  gatewayCostHasDisplayAmount,
  gatewayCostHasComparison,
  gatewayCostRatioEstimate,
  gatewayCostShowsLegacyEstimateDetails,
  gatewayCostStatusText,
  gatewayCostSystemEstimate,
  gatewayCostUnavailableReasonText,
  gatewayCostUsesPriceFact,
} from "./gateway-cost-display"

test("分组缓存比例页面可以切入并返回请求记录", () => {
  const cachePath = gatewayUsageTabPath("group-cache")
  assert.equal(cachePath, "/gateway/usage/group-cache")

  const requestsPath = gatewayUsageTabPath("requests")
  assert.equal(requestsPath, "/gateway/usage")
  assert.equal(gatewayUsageTabRoute("requests"), "gateway/usage")
})

test("使用记录子页面共享同一个路由边界以保留筛选状态", () => {
  const requestsBoundary = gatewayUsageRouteBoundaryPath("/gateway/usage")
  assert.equal(gatewayUsageRouteBoundaryPath("/gateway/usage/group-cache"), requestsBoundary)
  assert.equal(gatewayUsageRouteBoundaryPath("/gateway/usage/group-cache/"), requestsBoundary)
  assert.equal(gatewayUsageRouteBoundaryPath("/gateway/costs"), "/gateway/costs")
})

test("三个环图布局把数据环宽稳定为原来的 1.5 倍", () => {
  const layouts = [
    { name: "渠道健康", layout: channelHealthDoughnutLayout, previousWidth: 24, outerRadius: 76 },
    { name: "首页渠道费用", layout: overviewChannelCostDoughnutLayout, previousWidth: 34, outerRadius: "72%" },
    { name: "使用记录分布", layout: usageBreakdownDoughnutLayout, previousWidth: 24, outerRadius: "72%" },
  ]

  for (const { name, layout, previousWidth, outerRadius } of layouts) {
    assert.equal(doughnutChartRingWidth(layout), previousWidth * 1.5, name)
    assert.equal(layout.outerRadius, outerRadius, `${name} outer radius`)
  }
})

test("请求明细表头和表体使用明确的字体层级", () => {
  const classes = new Set(gatewayUsageRequestTableClassName.split(/\s+/))
  assert.equal(classes.has("min-w-[920px]"), true)
  assert.equal(classes.has("table-fixed"), true)
  assert.equal(classes.has("text-xs"), true)
  assert.equal(classes.has("[&_td]:overflow-hidden"), true)
  assert.equal(classes.has("[&_thead_th]:font-sans"), true)
  assert.equal(classes.has("[&_thead_th]:text-xs"), true)
  assert.equal(classes.has("[&_thead_th]:font-semibold"), true)
  assert.equal(classes.has("[&_tbody_td]:font-sans"), true)
  assert.equal(classes.has("[&_tbody_td]:text-xs"), true)
  assert.equal(classes.has("[&_tbody_td]:font-normal"), true)
})

test("费用展示按状态区分显式零、无法估算和无费用", () => {
  const formatCost = (value: number) => `$${value.toFixed(2)}`
  assert.equal(gatewayCostDisplayText({ cost_status: "estimated", cost: 0 }, formatCost), "$0.00")
  assert.equal(gatewayCostDisplayText({ cost_status: "reported", cost: 0 }, formatCost), "$0.00")
  assert.equal(gatewayCostDisplayText({ cost_status: "reconciled", cost: 0 }, formatCost), "$0.00")
  assert.equal(gatewayCostDisplayText({ cost_status: "unavailable", cost: 6 }, formatCost), "无法估算")
  assert.equal(gatewayCostDisplayText({ cost_status: "none", cost: 6 }, formatCost), "—")
  assert.equal(gatewayCostHasDisplayAmount("unavailable"), false)
  assert.equal(gatewayCostHasDisplayAmount("none"), false)
  assert.equal(gatewayCostHasDisplayAmount("estimated"), true)
  assert.equal(gatewayCostStatusText("unavailable"), "无法估算")
  assert.equal(gatewayCostStatusText("none"), "无费用")
  assert.equal(gatewayCostStatusText("unexpected"), "费用状态未知")
})

test("使用记录列表优先显示上游费用，再回退到冻结计算费用", () => {
  const formatCost = (value: number) => `$${value.toFixed(2)}`
  const upstreamFirst = {
    cost_status: "reconciled" as const,
    cost: 9,
    display_cost: 0.35,
    calculated_cost: 0.22,
    upstream_cost: 0.35,
  }
  assert.equal(gatewayCostDisplayAmount(upstreamFirst), 0.35)
  assert.equal(gatewayCostDisplayText(upstreamFirst, formatCost), "$0.35")
  assert.equal(gatewayCostDisplaySource(upstreamFirst), "upstream")
  assert.equal(gatewayCostComparisonLabel(gatewayCostDisplaySource(upstreamFirst)), "上游")

  const calculatedFallback = {
    cost_status: "estimated" as const,
    cost: 9,
    display_cost: 0.22,
    calculated_cost: 0.22,
    upstream_cost: null,
  }
  assert.equal(gatewayCostDisplayAmount(calculatedFallback), 0.22)
  assert.equal(gatewayCostDisplaySource(calculatedFallback), "calculated")

  const zeroUpstream = {
    cost_status: "reported" as const,
    cost: 9,
    display_cost: 0,
    calculated_cost: 0.22,
    upstream_cost: 0,
  }
  assert.equal(gatewayCostDisplayText(zeroUpstream, formatCost), "$0.00")
  assert.equal(gatewayCostDisplaySource(zeroUpstream), "upstream")
})

test("系统估计费用在价格事实缺失时仍使用冻结倍率展示", () => {
  assert.equal(gatewayCostSystemEstimate({ calculated_cost: 0.22, upstream_cost: 0.35 }), 0.22)
  assert.equal(gatewayCostSystemEstimate({
    calculated_cost: null,
    upstream_cost: null,
    group_ratio: 2,
    completion_ratio: 3,
    prompt_tokens: 1_000,
    completion_tokens: 500,
    cache_creation_tokens: 100,
    cache_read_tokens: 200,
    total_tokens: 1_500,
  }), 0.00592)
  assert.equal(gatewayCostSystemEstimate({ calculated_cost: null, upstream_cost: null, group_ratio: 0 }), null)
  assert.equal(gatewayCostSystemEstimate({
    price_fact_id: 91,
    calculated_cost: null,
    upstream_cost: null,
    group_ratio: 2,
    completion_ratio: 3,
    prompt_tokens: 1_000,
    completion_tokens: 500,
    cache_creation_tokens: 100,
    cache_read_tokens: 200,
    total_tokens: 1_500,
  }), null)
})

test("新价格字段为空时不回退到历史倍率费用，并保留差异事实", () => {
  const formatCost = (value: number) => `$${value.toFixed(2)}`
  const unavailable = {
    cost_status: "estimated" as const,
    cost: 9,
    display_cost: null,
    calculated_cost: null,
    upstream_cost: null,
  }
  assert.equal(gatewayCostDisplayAmount(unavailable), null)
  assert.equal(gatewayCostDisplayText(unavailable, formatCost), "—")
  assert.equal(gatewayCostDisplaySource(unavailable), "none")

  const omittedAmounts = {
    cost_status: "estimated" as const,
    cost: 99,
    price_fact_id: 42,
    calculated_cost_status: "unavailable",
    calculated_cost_unavailable_reason: "usage_incomplete",
  }
  assert.equal(gatewayCostDisplayAmount(omittedAmounts), null)
  assert.equal(gatewayCostDisplayText(omittedAmounts, formatCost), "—")
  assert.equal(gatewayCostDisplaySource(omittedAmounts), "none")

  const comparison = {
    calculated_cost: 0.2,
    upstream_cost: 0.25,
    cost_difference: 0.05,
    cost_difference_ratio: 0.25,
  }
  assert.equal(gatewayCostHasComparison(comparison), true)
  assert.equal(gatewayCostDifferenceText(comparison, formatCost), "+$0.05")
  assert.equal(gatewayCostDifferenceRatioText(comparison), "25%")
  assert.equal(gatewayCostDifferenceText({ cost_difference: -0.05 }, formatCost), "-$0.05")
  assert.equal(gatewayCostDifferenceRatioText({ cost_difference_ratio: null }), "—")
})

test("费用差异优先使用价格事实并让倍率兜底参与严格百分之五比较", () => {
  assert.deepEqual(gatewayCostComparison({
    calculated_cost: 0.2,
    upstream_cost: 0.25,
  }), {
    systemEstimate: 0.2,
    upstreamCost: 0.25,
    difference: 0.04999999999999999,
    ratio: 0.24999999999999994,
    material: true,
  })

  const ratioRecord = {
    calculated_cost: null,
    upstream_cost: 0.0063,
    group_ratio: 2,
    completion_ratio: 3,
    prompt_tokens: 1_000,
    completion_tokens: 500,
    cache_creation_tokens: 100,
    cache_read_tokens: 200,
    total_tokens: 1_500,
  }
  const fallbackComparison = gatewayCostComparison(ratioRecord)
  assert.equal(fallbackComparison?.systemEstimate, 0.00592)
  assert.equal(fallbackComparison?.material, true)
  assert.equal(gatewayCostHasComparison(ratioRecord), true)

  assert.equal(gatewayCostComparison({ calculated_cost: 0.01, upstream_cost: 0.0105 })?.material, false)
  assert.equal(gatewayCostComparison({ calculated_cost: 0.000065, upstream_cost: 0.00006825 })?.material, false)
  assert.equal(gatewayCostComparison({ calculated_cost: 0.01, upstream_cost: 0.010500000000005 })?.material, true)
  assert.equal(gatewayCostComparison({ calculated_cost: 0.01, upstream_cost: 0.01051 })?.material, true)
  assert.deepEqual(gatewayCostComparison({ calculated_cost: 0, upstream_cost: 1 }), {
    systemEstimate: 0,
    upstreamCost: 1,
    difference: 1,
    ratio: null,
    material: true,
  })

  const unavailableFact = { ...ratioRecord, price_fact_id: 91 }
  assert.equal(gatewayCostComparison(unavailableFact), null)
  assert.equal(gatewayCostHasComparison(unavailableFact), false)

  const sourceOnlyFact = {
    ...ratioRecord,
    cost_status: "estimated" as const,
    cost: 99,
    cost_source: "price_fact",
    price_fact_id: null,
    display_cost: null,
    upstream_cost: null,
  }
  assert.equal(gatewayCostSystemEstimate(sourceOnlyFact), null)
  assert.equal(gatewayCostComparison(sourceOnlyFact), null)
  assert.equal(gatewayCostHasComparison(sourceOnlyFact), false)
  assert.equal(gatewayCostDisplayAmount(sourceOnlyFact), null)
  assert.equal(gatewayCostDisplaySource(sourceOnlyFact), "none")

  const reconciledSourceOnlyFact = { ...sourceOnlyFact, upstream_cost: 0.0063 }
  assert.equal(gatewayCostComparison(reconciledSourceOnlyFact), null)
  assert.equal(gatewayCostDisplayAmount(reconciledSourceOnlyFact), 0.0063)
  assert.equal(gatewayCostDisplaySource(reconciledSourceOnlyFact), "upstream")
})

test("价格事实缺失时按请求冻结倍率显示非权威估算", () => {
  const formatCost = (value: number) => `$${value.toFixed(4)}`
  const ratioOnly = {
    cost_status: "unavailable" as const,
    cost: 0,
    display_cost: null,
    calculated_cost: null,
    upstream_cost: null,
    group_ratio: 2,
    completion_ratio: 3,
    prompt_tokens: 1_000,
    completion_tokens: 500,
    cache_creation_tokens: 100,
    cache_read_tokens: 200,
  }
  assert.deepEqual(gatewayCostRatioEstimate(ratioOnly), {
    input: 0.00292,
    output: 0.003,
    total: 0.00592,
  })
  assert.equal(gatewayCostDisplayAmount(ratioOnly), 0.00592)
  assert.equal(gatewayCostDisplayText(ratioOnly, formatCost), "$0.0059")
  assert.equal(gatewayCostDisplaySource(ratioOnly), "ratio_estimate")
  assert.equal(gatewayCostComparisonLabel(gatewayCostDisplaySource(ratioOnly)), "倍率估算")

  const authoritativeDisplay = { ...ratioOnly, display_cost: 0.25 }
  assert.equal(gatewayCostDisplayAmount(authoritativeDisplay), 0.25)
  assert.equal(gatewayCostDisplaySource(authoritativeDisplay), "display")

  const completionFallsBackToInputRatio = { ...ratioOnly, completion_ratio: 0 }
  assert.equal(gatewayCostRatioEstimate(completionFallsBackToInputRatio)?.output, 0.002)

  const totalTokensOnly = {
    ...ratioOnly,
    prompt_tokens: 0,
    completion_tokens: 0,
    cache_creation_tokens: 0,
    cache_read_tokens: 0,
    total_tokens: 1_500,
  }
  assert.deepEqual(gatewayCostRatioEstimate(totalTokensOnly), {
    input: 0.006,
    output: 0,
    total: 0.006,
  })
  assert.equal(gatewayCostRatioEstimate({ ...ratioOnly, group_ratio: 0 }), null)

  assert.deepEqual(gatewayCostRatioEstimate({
    ...ratioOnly,
    prompt_tokens: 100,
    completion_tokens: -10,
    cache_creation_tokens: -20,
    cache_read_tokens: 120,
    total_tokens: 90,
  }), {
    input: 0.000048,
    output: 0,
    total: 0.000048,
  })
})

test("无法估算原因使用固定文案且价格事实禁用旧倍率拆分", () => {
  assert.equal(gatewayCostUnavailableReasonText("legacy_unverified_estimate"), "历史本地估算未经过上游验证")
  assert.equal(gatewayCostUnavailableReasonText("Bearer should-not-render"), "缺少可验证的计价依据")
  assert.equal(gatewayCostUsesPriceFact({ price_fact_id: 91, cost_source: "" }), true)
  assert.equal(gatewayCostUsesPriceFact({ price_fact_id: null, cost_source: "price_fact" }), true)
  assert.equal(gatewayCostUsesPriceFact({ price_fact_id: null, cost_source: "gateway_estimate" }), false)

  const detailCases = [
    { name: "无法估算", record: { cost_status: "unavailable", price_fact_id: null, cost_source: "gateway_estimate" }, want: false },
    { name: "无费用", record: { cost_status: "none", price_fact_id: null, cost_source: "gateway_estimate" }, want: false },
    { name: "价格事实", record: { cost_status: "estimated", price_fact_id: 91, cost_source: "price_fact" }, want: false },
    { name: "本地估算", record: { cost_status: "estimated", price_fact_id: null, cost_source: "gateway_estimate" }, want: true },
    { name: "上游总额", record: { cost_status: "reported", price_fact_id: null, cost_source: "actual_cost" }, want: true },
  ] as const
  for (const testCase of detailCases) {
    assert.equal(gatewayCostShowsLegacyEstimateDetails(testCase.record), testCase.want, testCase.name)
  }
})
