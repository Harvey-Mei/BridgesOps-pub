import { money } from "./format"

export const insightsChartColors = {
  balance: "var(--insights-chart-balance)",
  cost: "var(--insights-chart-cost)",
  rateUp: "var(--insights-chart-rate-up)",
  rateDown: "var(--insights-chart-rate-down)",
  rateOther: "var(--insights-chart-rate-other)",
  impact: "var(--insights-chart-impact)",
} as const

export const insightsChartTooltipProps = {
  contentStyle: {
    backgroundColor: "var(--popover)",
    border: "1px solid var(--border)",
    borderRadius: 6,
    color: "var(--popover-foreground)",
    fontSize: 12,
    boxShadow: "0 8px 24px color-mix(in oklch, var(--foreground) 12%, transparent)",
  },
  labelStyle: { color: "var(--foreground)", fontWeight: 600 },
  cursor: { fill: "color-mix(in oklch, var(--muted) 72%, transparent)" },
  isAnimationActive: false,
} as const

export function formatInsightsMoneyTooltip(value: unknown, name: unknown) {
  return [money(Number(value), { precise: true }), String(name)] as const
}

export function formatInsightsRateChangeTooltip(value: unknown, name: unknown) {
  return [`${Number(value)} 次`, String(name)] as const
}

export function formatInsightsRateChangeLabel(label: unknown) {
  return `日期 ${String(label)}`
}
