import { useCallback, useEffect, useRef, useState, type MouseEvent, type ReactNode, type RefObject } from "react"
import { ArrowRight, ChevronsRight, type LucideIcon } from "lucide-react"
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { CardTitle } from "@/components/ui/card"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { cn } from "@/lib/utils"
import { RadarTooltipPortal } from "@/components/radar/radar-tooltip-portal"
import { useRadarTooltipPlacement, type RadarTooltipPlacement } from "@/components/radar/radar-tooltip-placement"

export type RadarChartMetric = "score" | "cost" | "latency"
export type RadarMetricRow = Record<string, string | number | undefined>
export type RadarMetricSeries = { key: string; label: string; color: string }

const metricLabels: Record<RadarChartMetric, string> = {
  score: "智商分数",
  cost: "测试费用",
  latency: "耗时",
}

export function RadarMetricChart({
  title,
  metric,
  rows,
  series,
  icon: Icon,
  description,
  formatValue,
  formatAxis,
  fallback,
  onMetricChange,
  scrollable = false,
}: {
  title: string
  metric: RadarChartMetric
  rows: RadarMetricRow[]
  series: RadarMetricSeries[]
  icon: LucideIcon
  description: string
  formatValue: (value: number) => string
  formatAxis: (value: number) => string
  fallback?: ReactNode
  onMetricChange: (metric: RadarChartMetric) => void
  scrollable?: boolean
}) {
  const viewportRef = useRef<HTMLDivElement>(null)
  const [canScrollLeft, setCanScrollLeft] = useState(false)
  const [tooltipVisible, setTooltipVisible] = useState(true)
  const { tooltipPlacement, onChartMouseMove } = useRadarTooltipPlacement()
  const valueCount = rows.reduce((count, row) => count + series.filter((item) => typeof row[`${metric}_${item.key}`] === "number").length, 0)
  const chartWidth = Math.max(640, rows.length * 56)

  const scrollToLatest = useCallback(() => {
    const viewport = viewportRef.current
    if (!viewport) return
    if (!scrollable) {
      setCanScrollLeft(false)
      return
    }
    viewport.scrollLeft = viewport.scrollWidth
    setCanScrollLeft(viewport.scrollLeft > 2)
    setTooltipVisible(false)
  }, [scrollable])

  useEffect(() => {
    const frame = window.requestAnimationFrame(scrollToLatest)
    return () => window.cancelAnimationFrame(frame)
  }, [metric, rows, scrollToLatest])

  function handleScroll() {
    const viewport = viewportRef.current
    setCanScrollLeft(Boolean(viewport && viewport.scrollLeft > 2))
    setTooltipVisible(false)
  }

  function handleMouseMove(event: MouseEvent<HTMLDivElement>) {
    setTooltipVisible(true)
    onChartMouseMove(event)
  }

  return (
    <section className="min-w-0 overflow-hidden rounded-lg border border-border bg-background">
      <div className="border-b border-border bg-muted/20 px-4 py-3" title={description}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="flex min-w-0 items-center gap-2">
            <span className="flex size-7 shrink-0 items-center justify-center rounded-md bg-primary/10 text-primary"><Icon className="size-3.5" /></span>
            <CardTitle className="text-lg font-semibold tracking-tight text-foreground">{title}</CardTitle>
          </div>
          <div className="flex items-center gap-1.5">
            {canScrollLeft ? <Button type="button" variant="outline" size="sm" className="h-8 gap-1.5 px-2 text-xs" onClick={scrollToLatest} title="跳到最新数据"><ChevronsRight className="size-3.5" />{"最新"}</Button> : null}
            <Select value={metric} onValueChange={(value) => onMetricChange(value as RadarChartMetric)}>
              <SelectTrigger size="sm" className="h-8 w-28 text-xs" aria-label="选择雷达指标"><SelectValue /></SelectTrigger>
              <SelectContent align="end">
                {(Object.keys(metricLabels) as RadarChartMetric[]).map((value) => <SelectItem key={value} value={value}>{metricLabels[value]}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>
      <div className="p-3">
        {valueCount > series.length ? (
          <div ref={viewportRef} className={cn("pb-1", scrollable ? "overflow-x-auto overscroll-x-contain [scrollbar-width:none] [&::-webkit-scrollbar]:hidden" : "overflow-hidden")} onScroll={handleScroll} onMouseMove={handleMouseMove}>
            <div className="h-80" style={{ width: scrollable ? `max(100%, ${chartWidth}px)` : "100%" }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={rows} margin={{ top: 8, right: 8, left: -18, bottom: 0 }}>
                  <XAxis dataKey="label" minTickGap={28} tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} />
                  <YAxis domain={[0, "auto"]} tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} tickFormatter={formatAxis} />
                  <Tooltip
                    filterNull={false}
                    isAnimationActive={false}
                    content={<RadarMetricTooltip metric={metric} series={series} placement={tooltipPlacement} viewportRef={viewportRef} tooltipVisible={tooltipVisible} formatValue={formatValue} />}
                  />
                  {series.map((item) => <Line key={item.key} type="monotone" dataKey={`${metric}_${item.key}`} name={item.label} stroke={item.color} strokeWidth={2} dot={seriesDotStyle(rows, `${metric}_${item.key}`)} activeDot={{ r: 4 }} connectNulls isAnimationActive={false} />)}
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        ) : fallback ?? <RadarEmptyChart />}
        <div className="mt-2 flex items-center justify-between gap-2 text-[11px] text-muted-foreground">
          <span>{`${rows.length} 个时间点 · ${series.length} 个配置`}</span>
          {scrollable && canScrollLeft ? <span className="inline-flex items-center gap-1"><ArrowRight className="size-3" />{"可横向查看历史"}</span> : <Badge variant="outline" className="h-5 px-1.5 text-[10px]">{scrollable ? "最新" : "自适应"}</Badge>}
        </div>
      </div>
    </section>
  )
}

function RadarEmptyChart() {
  return <div className="flex h-80 items-center justify-center text-xs text-muted-foreground">{"暂无可绘制历史数据"}</div>
}

function seriesDotStyle(rows: RadarMetricRow[], dataKey: string) {
  const count = rows.filter((row) => typeof row[dataKey] === "number").length
  return count <= 1 ? { r: 3 } : rows.length <= 12 ? { r: 2 } : false
}

function RadarMetricTooltip({ active, payload, label, coordinate, metric, series, placement, viewportRef, tooltipVisible, formatValue }: {
  active?: boolean
  payload?: Array<{ dataKey?: string; value?: number | null }>
  label?: string
  coordinate?: { x?: number; y?: number }
  metric: RadarChartMetric
  series: RadarMetricSeries[]
  placement: RadarTooltipPlacement
  viewportRef: RefObject<HTMLDivElement | null>
  tooltipVisible: boolean
  formatValue: (value: number) => string
}) {
  if (!active || !tooltipVisible || !payload?.length) return null
  const entries = payload.map((item, index) => {
    const model = series.find((candidate) => item.dataKey === `${metric}_${candidate.key}`)
    return { item, index, model, color: model?.color ?? "var(--muted-foreground)" }
  }).sort((a, b) => {
    const aValue = typeof a.item.value === "number" && Number.isFinite(a.item.value) ? a.item.value : null
    const bValue = typeof b.item.value === "number" && Number.isFinite(b.item.value) ? b.item.value : null
    if (aValue == null) return bValue == null ? a.index - b.index : 1
    if (bValue == null) return -1
    return bValue - aValue || a.index - b.index
  })
  return <RadarTooltipPortal active={active && tooltipVisible} coordinate={coordinate} placement={placement} viewportRef={viewportRef}><div className="w-full rounded-lg border bg-background px-3 py-2 text-xs shadow-xl"><div className="mb-1 font-medium">{label || "—"}</div>{entries.map(({ item, index, model, color }) => <div key={`${item.dataKey ?? "unknown"}-${index}`} className="flex items-start justify-between gap-3 py-0.5" style={{ color }}><span className="flex min-w-0 items-start gap-1.5"><span className="mt-1 size-2 shrink-0 rounded-full" style={{ backgroundColor: color }} /><span className="break-words">{model?.label ?? "—"}</span></span><span className="shrink-0 font-mono font-medium">{item.value == null ? "—" : formatValue(item.value)}</span></div>)}</div></RadarTooltipPortal>
}
