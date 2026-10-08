import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

import { formatInsightsMoneyTooltip, formatInsightsRateChangeLabel, formatInsightsRateChangeTooltip, insightsChartColors, insightsChartTooltipProps } from "./insights-chart-theme"
import { rateChangeTrendMinWidth } from "./insights-rate-change-trend"

test("trend analysis series use stable and distinct semantic color tokens", () => {
  const colors = Object.values(insightsChartColors)

  assert.equal(new Set(colors).size, colors.length)
  for (const color of colors) assert.match(color, /^var\(--insights-chart-[a-z-]+\)$/)
  assert.notEqual(insightsChartColors.rateUp, insightsChartColors.rateDown)
  assert.notEqual(insightsChartColors.balance, insightsChartColors.cost)

  const css = readFileSync(new URL("../app/globals.css", import.meta.url), "utf8")
  assert.match(css, /\.dark \.insights-chart-palette\s*\{/)
  for (const color of colors) {
    const token = color.slice(4, -1)
    assert.equal(css.match(new RegExp(`${token}:`, "g"))?.length, 2, token)
  }
})

test("trend analysis tooltip follows the active theme", () => {
  assert.equal(insightsChartTooltipProps.contentStyle.backgroundColor, "var(--popover)")
  assert.equal(insightsChartTooltipProps.contentStyle.color, "var(--popover-foreground)")
  assert.equal(insightsChartTooltipProps.contentStyle.border, "1px solid var(--border)")
  assert.equal(insightsChartTooltipProps.isAnimationActive, false)
  assert.equal("itemStyle" in insightsChartTooltipProps, false)
})

test("fund trend tooltip preserves each Recharts series name", () => {
  assert.deepEqual(formatInsightsMoneyTooltip(12.3456, "余额"), ["$12.3456", "余额"])
  assert.deepEqual(formatInsightsMoneyTooltip(1.5, "每日消费"), ["$1.5000", "每日消费"])
})

test("rate change tooltip labels the date and change count clearly", () => {
  assert.deepEqual(formatInsightsRateChangeTooltip(3, "上涨"), ["3 次", "上涨"])
  assert.equal(formatInsightsRateChangeLabel("07/22"), "日期 07/22")
})

test("rate change trend uses three grouped bars on the same scale", () => {
  const page = readFileSync(new URL("../app/insights-page.tsx", import.meta.url), "utf8")
  const start = page.indexOf('title="倍率涨跌趋势"')
  const end = page.indexOf('title="受影响模型"')
  const chart = page.slice(start, end)

  assert.ok(start >= 0)
  assert.ok(end > start)
  assert.equal(chart.includes("stackId="), false)
  assert.match(chart, /barGap=\{4\}/)
  assert.match(chart, /barCategoryGap="24%"/)
  assert.match(chart, /overflow-x-auto/)
  assert.match(chart, /minWidth: rateChangeTrendMinWidth\(dailyChangeChartData\.length\)/)
  assert.match(chart, /aria-label="倍率涨跌趋势，可横向滚动查看所有日期"/)
  assert.match(chart, /interval=\{0\}/)
  assert.match(chart, /formatter=\{formatInsightsRateChangeTooltip\}/)
  assert.match(chart, /labelFormatter=\{formatInsightsRateChangeLabel\}/)
  for (const series of ["up", "down", "other"]) assert.match(chart, new RegExp(`<Bar dataKey="${series}"`))
})

test("rate change trend reserves readable grouped-bar width for long ranges", () => {
  assert.equal(rateChangeTrendMinWidth(0), 480)
  assert.equal(rateChangeTrendMinWidth(7), 480)
  assert.equal(rateChangeTrendMinWidth(30), 1496)
  assert.equal(rateChangeTrendMinWidth(90), 4376)
  assert.equal(rateChangeTrendMinWidth(Number.NaN), 480)
})
