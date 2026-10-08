import { HelpHint } from "@/components/ui/help-hint"
import { useEffect, useMemo, useRef, useState } from "react"
import { AlertTriangle, ExternalLink, Sparkles, TrendingUp, Gauge, Coins, Timer, type LucideIcon } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import { useCodexRadar } from "@/lib/queries"
import { useTriggerRefresh } from "@/lib/refresh-context"
import type { CodexRadarHistoryPoint, CodexRadarSnapshot } from "@/lib/api-types"
import { compactDateTime, compactTokenCount, dateTime, relativeTime } from "@/lib/format"
import { officialReasoningEffortLabel } from "@/lib/reasoning-effort"
import { cn } from "@/lib/utils"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { StatusMetricCard } from "@/components/ui/status-metric-card"
import { OpenAIIcon } from "@/components/brand-icons"
import { RadarChartControls, RadarModelTableShell, RadarSourceStatus, RadarToolbar } from "@/components/radar/radar-workbench"
import { RadarMetricChart, type RadarChartMetric } from "@/components/radar/radar-metric-chart"

type RadarPayload = {
  monitored_at?: string
  status?: string
  recommended_action?: string
  model_iq?: {
    latest?: RadarLatest
    recent_days?: RadarPoint[]
    comparisons?: Record<string, RadarComparison>
  }
  prediction?: { level?: string; probability_24h?: number; probability_48h?: number; summary?: string; updated_at?: string }
  window?: { open?: boolean; status?: string; title?: string; message?: string; source_url?: string }
  links?: { html?: string; rss?: string; full_api?: string }
}

type RadarLatest = RadarPoint & { model?: string; reasoning_effort?: string; cost_usd?: number; wall_time_human?: string }
type RadarPoint = { date?: string; score?: number; status?: string; passed?: number; tasks?: number; valid_tasks?: number; invalid?: number; wall_seconds?: number; cost_usd?: number; total_tokens?: number }
type RadarComparison = { label?: string; model?: string; reasoning_effort?: string; latest?: RadarLatest; recent_days?: RadarPoint[] }
type RadarComparisonEntry = RadarComparison & { key: string }
type ChartMetric = "score" | "cost" | "latency"
type TimeWindow = number | "all"

const statusLabel: Record<string, string> = { green: "稳定", yellow: "有波动", red: "异常", community_confirmed: "社区确认" }

export default function CodexRadarPage({ embedded = false }: { embedded?: boolean }) {
  const query = useCodexRadar()
  const radarRefetch = useRef(query.refetch)
  radarRefetch.current = query.refetch
  const refresh = useTriggerRefresh()
  const [manualRefreshing, setManualRefreshing] = useState(false)
  const [selectedKeys, setSelectedKeys] = useState<string[]>([])
  const [windowDays, setWindowDays] = useState<TimeWindow>(3)
  const [chartMetric, setChartMetric] = useState<RadarChartMetric>("score")

  useEffect(() => {
    const timer = window.setInterval(() => radarRefetch.current(), 60_000)
    return () => window.clearInterval(timer)
  }, [])
  const payload = (query.data?.payload ?? null) as RadarPayload | null
  const localHistory = query.data?.history
  const comparisons = useMemo<RadarComparisonEntry[]>(
    () => {
      const entries = Object.entries(payload?.model_iq?.comparisons ?? {})
        .map(([key, item]) => ({ ...item, key }))
        .filter((item): item is RadarComparisonEntry => Boolean(item.latest))
      if (entries.length > 0) return mergeLocalHistory(entries.sort((a, b) => (b.latest?.score ?? -Infinity) - (a.latest?.score ?? -Infinity)), localHistory ?? [])
      const latest = payload?.model_iq?.latest
      return latest ? mergeLocalHistory([{ key: "overall_latest", label: "总体最新测试", model: latest.model, reasoning_effort: latest.reasoning_effort, latest, recent_days: payload?.model_iq?.recent_days }], localHistory ?? []) : []
    },
    [payload, localHistory],
  )
  const validSelectedKeys = selectedKeys.filter((key) => comparisons.some((item) => item.key === key))
  const selectedComparisons = validSelectedKeys.length > 0
    ? comparisons.filter((item) => validSelectedKeys.includes(item.key))
    : comparisons
  const chartComparisons = selectedComparisons.length > 0 ? selectedComparisons : comparisons
  const chartRows = useMemo(() => filterRowsByWindow(buildChartRows(chartComparisons), windowDays), [chartComparisons, windowDays])
  const best = comparisons[0]
  const cheapest = comparisons.filter((item) => typeof item.latest?.cost_usd === "number" && Number.isFinite(item.latest.cost_usd)).sort((a, b) => (a.latest?.cost_usd ?? Infinity) - (b.latest?.cost_usd ?? Infinity))[0]
  const fastest = comparisons.filter((item) => typeof item.latest?.wall_seconds === "number" && Number.isFinite(item.latest.wall_seconds)).sort((a, b) => (a.latest?.wall_seconds ?? Infinity) - (b.latest?.wall_seconds ?? Infinity))[0]
  const confident = comparisons.filter((item) => confidenceFor(item) >= 70).length
  const probability = clampProbability(payload?.prediction?.probability_24h)

  async function manualRefresh() {
    setManualRefreshing(true)
    try {
      const result = await apiFetch<CodexRadarSnapshot & { refresh_error?: string }>("/codex-radar/refresh", { method: "POST" })
      query.setData(result)
      refresh()
      toast[result.refresh_error ? "warning" : "success"](result.refresh_error ? `刷新失败，继续使用旧数据：${result.refresh_error}` : "Codex 雷达数据已刷新")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "刷新失败，已保留旧数据")
      query.refetch()
    } finally {
      setManualRefreshing(false)
    }
  }

  if (query.loading && !query.data) return <div className="space-y-4"><RadarToolbar embedded={embedded} icon={OpenAIIcon} title="Codex 雷达" source="codexradar.com 公开摘要" freshness="正在读取缓存" loading refreshing={manualRefreshing} onRefresh={manualRefresh} /><div className="flex min-h-[50vh] items-center justify-center text-sm text-muted-foreground">正在读取雷达数据…</div></div>

  const radarToolbar = <RadarToolbar embedded={embedded} icon={OpenAIIcon} title="Codex 雷达" source="codexradar.com 公开摘要" freshness={`缓存更新 ${relativeTime(query.data?.fetched_at)}`} stale={query.data?.stale} error={Boolean(query.error && !query.data)} refreshing={manualRefreshing} onRefresh={manualRefresh} />

  return (
    <div className="space-y-4">
      {!embedded ? radarToolbar : null}

      {query.error ? <div className="rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-sm text-warning">读取缓存失败：{query.error}</div> : null}
      {!query.data?.payload || comparisons.length === 0 ? <><EmptyRadar error={query.data?.last_error} lastAttempt={query.data?.last_attempt_at} />{embedded ? radarToolbar : null}</> : (
        <>
          <section className="metric-strip grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <Metric icon={Sparkles} label="可比较配置" value={String(comparisons.length)} hint={best ? `最高 ${modelLabel(best)} · ${formatScore(best.latest?.score)}` : "暂无模型数据"} tone="brand" />
            <Metric icon={Gauge} label="最高智商分" value={formatScore(best?.latest?.score)} hint={best ? `${modelLabel(best)} · ${confidenceFor(best)} 信心` : "暂无测试明细"} tone={best?.latest?.status === "green" ? "success" : "warning"} />
            <Metric icon={Coins} label="最低测试费用" value={formatCost(cheapest?.latest?.cost_usd)} hint={cheapest ? `${modelLabel(cheapest)} · ${formatLatency(cheapest.latest?.wall_seconds)}` : "暂无费用数据"} tone="brand" />
            <Metric icon={TrendingUp} label="可用于映射" value={`${confident}/${comparisons.length}`} hint={probability != null ? `重置概率 ${Math.round(probability * 100)}% · ${relativeTime(query.data.fetched_at)}` : "按信心阈值 70 统计"} tone={query.data.stale ? "warning" : "success"} />
          </section>

          {embedded ? radarToolbar : null}

          <section className="space-y-4">
            <RadarChartControls items={comparisons.map((item) => ({ key: item.key, label: modelLabel(item), color: colorForKey(item.key) }))} selectedKeys={validSelectedKeys} onToggle={(key) => setSelectedKeys((current) => { const valid = current.filter((item) => comparisons.some((comparison) => comparison.key === item)); return valid.includes(key) ? valid.filter((item) => item !== key) : [...valid, key] })} onAll={() => setSelectedKeys([])} windowValue={windowDays} windows={[{ value: 1, label: "1 天" }, { value: 3, label: "3 天" }, { value: 7, label: "7 天" }, { value: 30, label: "30 天" }, { value: 90, label: "90 天" }, { value: "all", label: "全部" }]} onWindowChange={(value) => setWindowDays(value as TimeWindow)} rowCount={chartRows.length} historyCount={localHistory?.length ?? 0} />
            <RadarMetricChart
              title="模型趋势"
              metric={chartMetric}
              onMetricChange={setChartMetric}
              rows={chartRows}
              series={chartComparisons.map((item) => ({ key: item.key, label: modelLabel(item), color: colorForKey(item.key) }))}
              icon={chartMetric === "score" ? Sparkles : chartMetric === "cost" ? Coins : Timer}
              description={chartMetric === "score" ? "能力分数（保留源站量表，越高越好）" : chartMetric === "cost" ? "单次测试成本（越低越好）" : "完整测试耗时（越低越好）"}
              formatValue={(value) => chartMetric === "score" ? formatScore(value) : chartMetric === "cost" ? formatCost(value) : formatLatency(value)}
              formatAxis={(value) => chartMetric === "score" ? String(value) : chartMetric === "cost" ? formatCost(value) : `${Math.round(value / 60)}m`}
              scrollable={windowDays === "all"}
              fallback={<MetricSnapshotChart metric={chartMetric} comparisons={chartComparisons} />}
            />
          </section>

          <section className="grid gap-4 xl:grid-cols-[minmax(0,1.25fr)_minmax(320px,0.75fr)]">
            <Card className="gap-0 overflow-hidden rounded-lg border border-border bg-background py-0 shadow-none">
              <CardHeader className="border-b px-5 py-4"><CardTitle className="text-lg font-semibold tracking-tight text-foreground">额度窗口</CardTitle></CardHeader>
              <CardContent className="space-y-4 p-5">
                <div className="rounded-lg border bg-muted/30 p-4"><div className="flex items-center gap-2 text-sm font-medium"><span className={cn("size-2 rounded-full", payload?.window?.open ? "bg-emerald-500" : "bg-muted-foreground/50")} />{payload?.window?.open ? "当前窗口开启" : "当前没有开启窗口"}</div><p className="mt-2 text-sm leading-6 text-muted-foreground">{payload?.window?.message ?? "等待公开摘要更新。"}</p></div>
                <div><div className="flex items-center justify-between text-xs text-muted-foreground"><span>24 小时概率</span><span>{probability != null ? `${Math.round(probability * 100)}%` : "—"}</span></div><Progress className="mt-2" value={probability != null ? probability * 100 : 0} /></div>
                <p className="text-xs leading-5 text-muted-foreground">{payload?.prediction?.summary ?? "暂无预测说明。"}</p>
              </CardContent>
            </Card>
            <Card className="gap-0 overflow-hidden rounded-lg border border-border bg-background py-0 shadow-none">
              <CardHeader className="border-b px-5 py-4"><CardTitle className="text-lg font-semibold tracking-tight text-foreground flex items-center gap-2">映射参考<HelpHint label="映射参考">展示公开测试的能力、成本与速度信号；实际映射还需结合本地渠道价格、可用性和延迟。</HelpHint></CardTitle></CardHeader>
              <CardContent className="space-y-3 p-5">
                <SignalRow label="能力优先" item={best} value={formatScore(best?.latest?.score)} detail="智商分最高" tone="brand" />
                <SignalRow label="成本优先" item={cheapest} value={formatCost(cheapest?.latest?.cost_usd)} detail="单次测试费用最低" tone="brand" />
                <SignalRow label="速度优先" item={fastest} value={formatLatency(fastest?.latest?.wall_seconds)} detail="完整测试耗时最短" tone="brand" />
                <div className="border-t pt-3 text-xs text-muted-foreground">信心分 ≥ 70 的配置：<span className="font-mono font-semibold text-foreground">{confident}</span> / {comparisons.length}</div>
              </CardContent>
            </Card>
          </section>

          <RadarModelTableShell title="模型对比" countLabel={`${comparisons.length} 个配置`}>
            <div className="overflow-x-auto"><table className="w-full min-w-[720px] text-sm"><thead className="bg-muted/20 text-xs text-muted-foreground"><tr><th className="px-5 py-3 text-left font-medium">配置</th><th className="px-4 py-3 text-right font-medium">智商</th><th className="px-4 py-3 text-right font-medium">费用</th><th className="px-4 py-3 text-right font-medium">耗时</th><th className="px-4 py-3 text-right font-medium">通过率</th><th className="px-4 py-3 text-right font-medium">信心</th><th className="px-5 py-3 text-right font-medium">状态</th></tr></thead><tbody>{comparisons.map((item) => <ComparisonRow key={item.key} item={item} />)}</tbody></table></div>
          </RadarModelTableShell>

          <RadarSourceStatus><span>数据来自 Codex 雷达 <a className="font-medium text-foreground underline-offset-2 hover:underline" href={payload?.links?.html ?? query.data.source_url} target="_blank" rel="noreferrer">codexradar.com <ExternalLink className="inline size-3" /></a> · 公开摘要接口</span><span>本地历史：{localHistory?.length ?? 0} 个点 · 最多保留 90 天 · 最近尝试：{dateTime(query.data.last_attempt_at)}</span></RadarSourceStatus>
          {query.data.stale && query.data.last_error ? <div className="flex items-start gap-2 rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-xs text-warning"><AlertTriangle className="mt-0.5 size-4 shrink-0" />上次自动刷新失败：{query.data.last_error}。页面继续使用最近一次成功数据。</div> : null}
        </>
      )}
    </div>
  )
}

function Metric({ icon: Icon, label, value, hint, tone }: { icon: LucideIcon; label: string; value: string; hint: string; tone: "brand" | "success" | "warning" | "danger" }) {
  return <StatusMetricCard icon={Icon} label={label} value={value} hint={hint} tone={tone} valueClassName="text-4xl" />
}

function modelLabel(item: RadarComparison) {
  const reasoningEffort = officialReasoningEffortLabel(item.reasoning_effort)
  const base = String(item.label ?? item.model ?? "未命名配置").replace(/\s+/g, " ").trim()
  if (!reasoningEffort) return base
  const escapedEffort = reasoningEffort.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
  const withoutTrailingEffort = base.replace(new RegExp(`(?:[\\s·/_-]+${escapedEffort})+$`, "i"), "").trim()
  return `${withoutTrailingEffort || base} · ${reasoningEffort}`
}

function formatScore(value?: number) {
  const normalized = normalizeScore(value)
  return normalized == null ? "—" : normalized.toFixed(0)
}

function normalizeScore(value?: number) {
  if (value == null || !Number.isFinite(value)) return undefined
  return Math.max(0, value)
}

function formatCost(value?: number) {
  return value == null || !Number.isFinite(value) ? "—" : `$${value.toFixed(2)}`
}

function formatLatency(value?: number) {
  if (value == null || !Number.isFinite(value)) return "—"
  if (value >= 3600) return `${(value / 3600).toFixed(1)} 小时`
  if (value >= 60) return `${(value / 60).toFixed(1)} 分钟`
  return `${Math.round(value)} 秒`
}

function formatTokens(value?: number) {
  return compactTokenCount(value)
}

function clampProbability(value?: number) {
  if (value == null || !Number.isFinite(value)) return undefined
  return Math.max(0, Math.min(1, value))
}

function SignalRow({ label, item, value, detail, tone }: { label: string; item?: RadarComparison; value: string; detail: string; tone: "brand" | "success" | "warning" }) {
  return <div className="flex items-center gap-3 rounded-lg border bg-muted/20 px-3 py-2.5"><span className={cn("size-2 shrink-0 rounded-full", tone === "success" ? "bg-emerald-500" : tone === "warning" ? "bg-amber-500" : "bg-primary")} /><div className="min-w-0 flex-1"><div className="flex items-center justify-between gap-2 text-xs"><span className="font-medium">{label}</span><span className="font-mono font-semibold">{value}</span></div><div className="mt-1 overflow-x-auto whitespace-nowrap text-[11px] text-muted-foreground" title={item ? modelLabel(item) : undefined}>{item ? modelLabel(item) : "暂无数据"} · {detail}</div></div></div>
}

function passRate(item: RadarComparison) {
  const latest = item.latest
  if (latest?.passed == null || !latest.tasks) return null
  return Math.max(0, Math.min(100, (latest.passed / latest.tasks) * 100))
}

/** 信心不是智商分：它衡量数据是否足够稳定，供后续智能映射过滤。 */
function confidenceFor(item: RadarComparison) {
  const latest = item.latest
  if (!latest) return 0
  const tasks = latest.tasks ?? latest.valid_tasks ?? 0
  const validRatio = tasks > 0 ? Math.max(0, Math.min(1, (latest.valid_tasks ?? tasks - (latest.invalid ?? 0)) / tasks)) : 0.5
  const passRatio = passRate(item) == null ? 0.5 : (passRate(item) ?? 0) / 100
  const history = historyPoints(item).filter((point) => point.score != null)
  const scores = history.map((point) => normalizeScore(point.score) ?? 0)
  const mean = scores.length ? scores.reduce((sum, score) => sum + score, 0) / scores.length : 0
  const deviation = scores.length > 1 ? Math.sqrt(scores.reduce((sum, score) => sum + (score - mean) ** 2, 0) / scores.length) : 30
  const stability = Math.max(0, Math.min(1, 1 - deviation / 45))
  const sampleCoverage = Math.min(1, history.length / 6)
  const base = Math.round((validRatio * 0.4 + passRatio * 0.3 + stability * 0.2 + sampleCoverage * 0.1) * 100)
  // A low pass rate or red status must remain a hard ceiling. Otherwise a
  // stable but failing model could be admitted to the automatic mapper.
  if (latest.status === "red" || (passRate(item) != null && (passRate(item) ?? 0) < 70)) return Math.min(base, Math.round(passRatio * 100))
  return base
}

function formatChartLabel(value?: string) {
  if (!value) return "—"
  const timestamp = pointTimestamp(value)
  if (Number.isFinite(timestamp) && (value.includes("T") || /^\d{4}-\d{2}-\d{2}-(am|pm|n)/.test(value))) {
    return compactDateTime(new Date(timestamp).toISOString())
  }
  return value.replace(/-(am|pm|n)(?:_\d+)?$/, " $1")
}

function buildChartRows(comparisons: RadarComparisonEntry[]) {
  const dates = new Set<string>()
  comparisons.forEach((item) => {
    item.recent_days?.forEach((point) => point.date && dates.add(point.date))
    if (item.latest?.date) dates.add(item.latest.date)
  })
  const rows = [...dates].sort((a, b) => {
    const first = pointTimestamp(a)
    const second = pointTimestamp(b)
    if (Number.isFinite(first) && Number.isFinite(second)) return first - second
    return a.localeCompare(b)
  }).map((date) => {
    const row: Record<string, string | number | undefined> = { date, label: formatChartLabel(date) }
    comparisons.forEach((item) => {
      const point = [item.latest, ...(item.recent_days ?? [])].find((candidate) => candidate?.date === date)
      row[`score_${item.key}`] = normalizeScore(point?.score)
      row[`cost_${item.key}`] = point?.cost_usd
      row[`latency_${item.key}`] = point?.wall_seconds
    })
    return row
  })
  return rows
}

function pointTimestamp(value?: string) {
  if (!value) return Number.NaN
  const parsed = Date.parse(value)
  if (Number.isFinite(parsed)) return parsed
  const match = /^(\d{4}-\d{2}-\d{2})-(am|pm|n)(?:_\d+)?$/.exec(value)
  if (!match) return Number.NaN
  const hour = match[2] === "am" ? "08:00:00" : match[2] === "pm" ? "20:00:00" : "00:00:00"
  return Date.parse(`${match[1]}T${hour}+08:00`)
}

function filterRowsByWindow(rows: Array<Record<string, string | number | undefined>>, windowDays: TimeWindow) {
  if (windowDays === "all" || rows.length === 0) return rows
  const timestamps = rows.map((row) => pointTimestamp(typeof row.date === "string" ? row.date : undefined)).filter(Number.isFinite)
  if (timestamps.length === 0) return rows
  const latest = Math.max(...timestamps)
  const cutoff = latest - windowDays * 24 * 60 * 60 * 1000
  return rows.filter((row) => {
    const timestamp = pointTimestamp(typeof row.date === "string" ? row.date : undefined)
    return !Number.isFinite(timestamp) || timestamp >= cutoff
  })
}

function mergeLocalHistory(entries: RadarComparisonEntry[], history: CodexRadarHistoryPoint[]) {
  if (history.length === 0) return entries
  const pointsByKey = new Map<string, RadarPoint[]>()
  for (const point of history) {
    const translated: RadarPoint = {
      date: point.observed_at,
      score: point.score,
      status: point.status,
      passed: point.passed,
      tasks: point.tasks,
      valid_tasks: point.valid_tasks,
      invalid: point.invalid,
      wall_seconds: point.wall_seconds,
      cost_usd: point.cost_usd,
      total_tokens: point.total_tokens,
    }
    const points = pointsByKey.get(point.config_key) ?? []
    points.push(translated)
    pointsByKey.set(point.config_key, points)
  }
  for (const points of pointsByKey.values()) points.sort((a, b) => pointTimestamp(a.date) - pointTimestamp(b.date))
  return entries.map((item) => {
    const local = pointsByKey.get(item.key) ?? []
    if (local.length === 0) return item
    const earliest = Math.min(...local.map((point) => pointTimestamp(point.date)).filter(Number.isFinite))
    const legacy = (item.recent_days ?? []).filter((point) => {
      const timestamp = pointTimestamp(point.date)
      return !Number.isFinite(timestamp) || timestamp < earliest
    })
    const latest = item.latest && local.length > 0 ? { ...item.latest, date: local[local.length - 1].date } : item.latest
    return { ...item, latest, recent_days: [...legacy, ...local] }
  })
}

function historyPoints(item: RadarComparison) {
  const byDate = new Map<string, RadarPoint>()
  for (const point of item.recent_days ?? []) if (point.date) byDate.set(point.date, point)
  if (item.latest?.date) byDate.set(item.latest.date, item.latest)
  return [...byDate.values()]
}

const chartColors = ["#2563eb", "#0f766e", "#d97706", "#db2777", "#7c3aed", "#0891b2", "#b91c1c", "#4f46e5", "#15803d"]

function colorForKey(key: string) {
  let hash = 0
  for (const character of key) hash = (hash * 31 + character.charCodeAt(0)) | 0
  return chartColors[Math.abs(hash) % chartColors.length]
}

function MetricSnapshotChart({ metric, comparisons }: { metric: ChartMetric; comparisons: RadarComparisonEntry[] }) {
  const values = comparisons.map((item) => ({ item, value: metric === "score" ? normalizeScore(item.latest?.score) : metric === "cost" ? item.latest?.cost_usd : item.latest?.wall_seconds })).filter((entry): entry is { item: RadarComparisonEntry; value: number } => typeof entry.value === "number" && Number.isFinite(entry.value))
  const maximum = Math.max(...values.map((entry) => entry.value), 1)
  return <div className="overflow-x-auto"><div className="flex h-80 min-w-[22rem] flex-col justify-center gap-2 px-1"><p className="mb-1 text-[11px] text-muted-foreground">上游未提供完整历史序列，以下显示最近一次测试对比</p>{values.map(({ item, value }) => <div key={item.key} className="grid grid-cols-[minmax(9rem,max-content)_1fr_auto] items-center gap-2 text-[11px]"><span className="whitespace-nowrap" title={modelLabel(item)}>{modelLabel(item)}</span><div className="h-2 overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full" style={{ width: `${Math.max(4, (value / maximum) * 100)}%`, backgroundColor: colorForKey(item.key) }} /></div><span className="w-14 text-right font-mono">{metric === "score" ? formatScore(value) : metric === "cost" ? formatCost(value) : formatLatency(value)}</span></div>)}</div></div>
}

function ComparisonRow({ item }: { item: RadarComparisonEntry }) {
  const latest = item.latest
  const confidence = confidenceFor(item)
  const rate = passRate(item)
  const reasoningEffort = officialReasoningEffortLabel(item.reasoning_effort)
  return <tr className="border-t transition-colors hover:bg-muted/20"><td className="px-5 py-3"><div className="font-medium">{item.label ?? item.model ?? "未命名配置"}</div><div className="mt-1 text-xs text-muted-foreground">{item.model ?? "—"}{reasoningEffort ? ` · ${reasoningEffort}` : ""}</div></td><td className="px-4 py-3 text-right font-mono font-semibold">{formatScore(latest?.score)}</td><td className="px-4 py-3 text-right tabular-nums">{formatCost(latest?.cost_usd)}<div className="mt-1 text-[10px] text-muted-foreground">{formatTokens(latest?.total_tokens)} tokens</div></td><td className="px-4 py-3 text-right text-muted-foreground">{formatLatency(latest?.wall_seconds)}</td><td className="px-4 py-3 text-right text-muted-foreground">{rate == null ? "—" : `${rate.toFixed(0)}%`}</td><td className="px-4 py-3 text-right"><span className={cn("font-mono font-semibold", confidence >= 70 ? "text-emerald-600" : confidence >= 50 ? "text-amber-600" : "text-rose-600")}>{confidence}</span><div className="mt-1 text-[10px] text-muted-foreground">{confidence >= 70 ? "可映射" : "观察"}</div></td><td className="px-5 py-3 text-right"><Badge variant={latest?.status === "green" ? "default" : latest?.status === "red" ? "destructive" : "secondary"}>{statusLabel[latest?.status ?? ""] ?? latest?.status ?? "—"}</Badge></td></tr>
}

function EmptyRadar({ error, lastAttempt }: { error?: string; lastAttempt?: string | null }) {
  return <div className="flex min-h-[50vh] items-center justify-center rounded-lg border border-dashed bg-muted/20 px-6 text-center"><div><AlertTriangle className={cn("mx-auto size-8", error ? "text-warning" : "text-muted-foreground")} /><h2 className="mt-3 text-base font-semibold">{error ? "雷达暂时不可用" : "暂无雷达缓存"}</h2><p className="mt-1 max-w-md text-sm text-muted-foreground">{error ?? "后台会按配置周期自动抓取，也可以点击右上角立即刷新。"}</p>{lastAttempt ? <p className="mt-2 text-xs text-muted-foreground">最近尝试：{dateTime(lastAttempt)}</p> : null}</div></div>
}
