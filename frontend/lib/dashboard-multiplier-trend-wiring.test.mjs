import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const pageSource = readFileSync(new URL("../app/page.tsx", import.meta.url), "utf8")

test("multiplier chart only creates lines from trusted per-bucket series values", () => {
  assert.match(pageSource, /dashboardMultiplierChartPoints\(data\)/)
  assert.match(pageSource, /dashboardMultiplierHasSeriesCoverage\(data, "input"\)/)
  assert.match(pageSource, /dashboardMultiplierHasSeriesCoverage\(data, "output"\)/)
  assert.match(pageSource, /inputHasSeries \? <Line dataKey="chart_input_multiplier"/)
  assert.match(pageSource, /outputHasSeries \? <Line dataKey="chart_output_multiplier"/)
  assert.doesNotMatch(pageSource, /<Line dataKey="smoothed_input_multiplier"/)
  assert.doesNotMatch(pageSource, /<Line dataKey="smoothed_output_multiplier"/)
  assert.match(pageSource, /connectNulls=\{false\}/)
})

test("multiplier chart describes untrusted directions and resolves empty-bucket tooltips by raw bucket", () => {
  assert.match(pageSource, /未推断倍率/)
  assert.match(pageSource, /DashboardMultiplierTrendTooltip rows=\{rows\}/)
  assert.match(pageSource, /dashboardMultiplierPointForBucket\(rows, label\)/)
  assert.match(pageSource, /成功用户请求/)
  assert.match(pageSource, /输入 · 本桶覆盖/)
  assert.match(pageSource, /输出 · 本桶覆盖/)
})
