const minimumChartWidth = 480
const minimumCategoryWidth = 48
const chartChromeWidth = 56

export function rateChangeTrendMinWidth(dataPointCount: number) {
  const count = Number.isFinite(dataPointCount) ? Math.max(0, Math.floor(dataPointCount)) : 0
  return Math.max(minimumChartWidth, count * minimumCategoryWidth + chartChromeWidth)
}
