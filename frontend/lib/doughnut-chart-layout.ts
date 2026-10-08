export type DoughnutChartRadius = number | `${number}%`

export type DoughnutChartLayout = {
  innerRadius: DoughnutChartRadius
  outerRadius: DoughnutChartRadius
}

export const channelHealthDoughnutLayout = {
  innerRadius: 40,
  outerRadius: 76,
} as const satisfies DoughnutChartLayout

export const overviewChannelCostDoughnutLayout = {
  innerRadius: "21%",
  outerRadius: "72%",
} as const satisfies DoughnutChartLayout

export const usageBreakdownDoughnutLayout = {
  innerRadius: "36%",
  outerRadius: "72%",
} as const satisfies DoughnutChartLayout

export function doughnutChartRingWidth(layout: DoughnutChartLayout) {
  const inner = radiusValue(layout.innerRadius)
  const outer = radiusValue(layout.outerRadius)
  if (typeof layout.innerRadius !== typeof layout.outerRadius || outer <= inner) return 0
  return outer - inner
}

function radiusValue(radius: DoughnutChartRadius) {
  return typeof radius === "number" ? radius : Number.parseFloat(radius)
}
