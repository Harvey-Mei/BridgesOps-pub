import { useEffect, useMemo, useRef, useState } from "react"
import { AlertTriangle, BrainCircuit, Coins, ExternalLink, Sparkles, Timer, type LucideIcon } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import { useClaudeRadar } from "@/lib/queries"
import { useTriggerRefresh } from "@/lib/refresh-context"
import type { ClaudeRadarHistoryPoint, ClaudeRadarSnapshot } from "@/lib/api-types"
import { compactDateTime, dateTime, relativeTime } from "@/lib/format"
import { cn } from "@/lib/utils"
import { StatusMetricCard } from "@/components/ui/status-metric-card"
import { ClaudeIcon } from "@/components/brand-icons"
import { RadarChartControls, RadarModelTableShell, RadarSourceStatus, RadarToolbar } from "@/components/radar/radar-workbench"
import { normalizeClaudeRadarPayload, type ClaudeRadarModel } from "@/components/radar/claude-radar-payload"
import { RadarMetricChart, type RadarChartMetric } from "@/components/radar/radar-metric-chart"

type ClaudeEntry = ClaudeRadarModel
type TimeWindow = number | "all"

const chartColors = ["#2563eb", "#0f766e", "#d97706", "#db2777", "#7c3aed", "#0891b2", "#b91c1c", "#4f46e5", "#15803d", "#be123c"]

export default function ClaudeRadarPage({ embedded = false }: { embedded?: boolean }) {
  const query = useClaudeRadar()
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
  const rawPayload = query.data?.payload
  const payload = useMemo(() => normalizeClaudeRadarPayload(rawPayload), [rawPayload])
  const localHistory = query.data?.history
  const models = useMemo<ClaudeEntry[]>(() => [...(payload?.iq.models ?? [])].sort((a, b) => (b.score ?? -Infinity) - (a.score ?? -Infinity)), [payload])
  const validSelectedKeys = selectedKeys.filter((key) => models.some((model) => model.key === key))
  const chartModels = validSelectedKeys.length > 0 ? models.filter((model) => validSelectedKeys.includes(model.key)) : models
  const chartRows = useMemo(() => filterRows(buildRows(models, payload?.labels ?? [], localHistory ?? []), windowDays), [models, payload?.labels, localHistory, windowDays])
  const best = models[0]
  const cheapest = models.filter((model) => finite(lastValue(model.cost))).sort((a, b) => (lastValue(a.cost) ?? Infinity) - (lastValue(b.cost) ?? Infinity))[0]
  const fastest = models.filter((model) => finite(lastValue(model.time))).sort((a, b) => (lastValue(a.time) ?? Infinity) - (lastValue(b.time) ?? Infinity))[0]

  async function manualRefresh() {
    setManualRefreshing(true)
    try {
      const result = await apiFetch<ClaudeRadarSnapshot & { refresh_error?: string }>("/claude-radar/refresh", { method: "POST" })
      query.setData(result)
      refresh()
      toast[result.refresh_error ? "warning" : "success"](result.refresh_error ? `刷新失败，继续使用旧数据：${result.refresh_error}` : "Claude 雷达数据已刷新")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "刷新失败，已保留旧数据")
      query.refetch()
    } finally {
      setManualRefreshing(false)
    }
  }

  if (query.loading && !query.data) return <div className="space-y-4"><RadarToolbar embedded={embedded} icon={ClaudeIcon} title="Claude 雷达" source="claudecoderadar.com 公开摘要" freshness="正在读取缓存" loading refreshing={manualRefreshing} onRefresh={manualRefresh} /><div className="flex min-h-[50vh] items-center justify-center text-sm text-muted-foreground">正在读取 Claude 雷达…</div></div>

  const radarToolbar = <RadarToolbar embedded={embedded} icon={ClaudeIcon} title="Claude 雷达" source="claudecoderadar.com 公开摘要" freshness={`缓存更新 ${relativeTime(query.data?.fetched_at)}`} stale={query.data?.stale} error={Boolean(query.error && !query.data)} refreshing={manualRefreshing} onRefresh={manualRefresh} />
  const payloadError = rawPayload != null && payload == null ? "Claude 雷达数据格式异常，请刷新后重试。" : query.data?.last_error

  return <div className="space-y-4">
    {!embedded ? radarToolbar : null}
    {query.error ? <div className="rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-sm text-warning">读取缓存失败：{query.error}</div> : null}
    {!payload || models.length === 0 ? <><EmptyClaude error={payloadError} lastAttempt={query.data?.last_attempt_at} />{embedded ? radarToolbar : null}</> : <>
      <section className="metric-strip grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Metric icon={BrainCircuit} label="可比较模型" value={String(models.length)} hint={`数据更新：${relativeTime(payload.updated_at ?? query.data?.fetched_at)}`} tone="brand" />
        <Metric icon={Sparkles} label="最高智商分" value={formatNumber(best?.score)} hint={best?.name ?? "暂无数据"} tone="brand" />
        <Metric icon={Coins} label="最低测试费用" value={formatCost(lastValue(cheapest?.cost))} hint={cheapest?.name ?? "暂无费用数据"} tone="brand" />
        <Metric icon={Timer} label="最快完整耗时" value={formatHours(lastValue(fastest?.time))} hint={fastest?.name ?? "暂无耗时数据"} tone="brand" />
      </section>

      {embedded ? radarToolbar : null}

      <RadarChartControls items={models.map((model) => ({ key: model.key, label: model.name, color: colorForKey(model.key) }))} selectedKeys={validSelectedKeys} onToggle={(key) => setSelectedKeys((current) => { const valid = current.filter((item) => models.some((model) => model.key === item)); return valid.includes(key) ? valid.filter((item) => item !== key) : [...valid, key] })} onAll={() => setSelectedKeys([])} windowValue={windowDays} windows={[{ value: 1, label: "1 天" }, { value: 3, label: "3 天" }, { value: 7, label: "7 天" }, { value: 30, label: "30 天" }, { value: 90, label: "90 天" }, { value: "all", label: "全部" }]} onWindowChange={(value) => setWindowDays(value as TimeWindow)} rowCount={chartRows.length} historyCount={localHistory?.length ?? 0} />

      <RadarMetricChart
        title="模型趋势"
        metric={chartMetric}
        onMetricChange={setChartMetric}
        rows={chartRows}
        series={chartModels.map((model) => ({ key: model.key, label: model.name, color: colorForKey(model.key) }))}
        icon={chartMetric === "score" ? Sparkles : chartMetric === "cost" ? Coins : Timer}
        description={chartMetric === "score" ? "能力分数（保留源站量表，越高越好）" : chartMetric === "cost" ? "单次测评费用（越低越好）" : "完整测评耗时（越低越好）"}
        formatValue={(value) => chartMetric === "score" ? formatNumber(value) : chartMetric === "cost" ? formatCost(value) : formatHours(value / 3600)}
        formatAxis={(value) => chartMetric === "score" ? String(value) : chartMetric === "cost" ? formatCost(value) : `${Math.round(value / 3600)}h`}
      />

      <RadarModelTableShell title="模型对比" countLabel={`${models.length} 个模型`}><div className="overflow-x-auto"><table className="w-full min-w-[860px] text-sm"><thead className="bg-muted/20 text-xs text-muted-foreground"><tr><th className="px-5 py-3 text-left font-medium">模型</th><th className="px-4 py-3 text-right font-medium">IQ</th><th className="px-4 py-3 text-right font-medium">通过</th><th className="px-4 py-3 text-right font-medium">费用</th><th className="px-4 py-3 text-right font-medium">耗时</th><th className="px-4 py-3 text-right font-medium">Cache</th><th className="px-5 py-3 text-right font-medium">最近测量</th></tr></thead><tbody>{models.map((model) => <ClaudeComparisonRow key={model.key} model={model} />)}</tbody></table></div></RadarModelTableShell>
      <RadarSourceStatus><span>数据来自 Claude Code 雷达 <a className="font-medium text-foreground underline-offset-2 hover:underline" href="https://claudecoderadar.com/" target="_blank" rel="noreferrer">claudecoderadar.com <ExternalLink className="inline size-3" /></a> · IQ/费用/耗时公开摘要</span><span>本地历史：{localHistory?.length ?? 0} 个点 · 最多保留 90 天 · 最近尝试：{dateTime(query.data?.last_attempt_at)}</span></RadarSourceStatus>
      {query.data?.stale && query.data.last_error ? <div className="flex items-start gap-2 rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-xs text-warning"><AlertTriangle className="mt-0.5 size-4 shrink-0" />上次自动刷新失败：{query.data.last_error}。页面继续使用最近一次成功数据。</div> : null}
    </>}
  </div>
}

function Metric({ icon: Icon, label, value, hint, tone }: { icon: LucideIcon; label: string; value: string; hint: string; tone: "brand" | "success" | "warning" }) {
  return <StatusMetricCard icon={Icon} label={label} value={value} hint={hint} tone={tone} valueClassName="text-4xl" />
}

function normalizeScore(value?: number) { return value == null || !Number.isFinite(value) ? undefined : Math.max(0, value) }
function formatNumber(value?: number) { const normalized = normalizeScore(value); return normalized == null ? "—" : normalized.toFixed(0) }
function formatCost(value?: number) { return value == null || !Number.isFinite(value) ? "—" : `$${value.toFixed(2)}` }
function formatHours(value?: number) { return value == null || !Number.isFinite(value) ? "—" : `${value.toFixed(1)} 小时` }
function finite(value?: number | null): value is number { return typeof value === "number" && Number.isFinite(value) }
function lastValue(values?: Array<number | null>) { for (let index = (values?.length ?? 0) - 1; index >= 0; index--) if (finite(values?.[index])) return values?.[index] as number; return undefined }

function parseLabel(value: string, yearHint?: number) {
  const match = /^(\d+)\.(\d+)(am|pm)$/.exec(value)
  if (!match) return Date.parse(value)
  const hour = match[3] === "pm" ? "20" : "08"
  const year = yearHint ?? new Date().getUTCFullYear()
  return Date.parse(`${year}-${String(Number(match[1])).padStart(2, "0")}-${String(Number(match[2])).padStart(2, "0")}T${hour}:00:00+08:00`)
}

function formatLabel(value: string) {
  const timestamp = Date.parse(value)
  if (Number.isFinite(timestamp) && value.includes("T")) return compactDateTime(new Date(timestamp).toISOString())
  return value
}

function buildRows(models: ClaudeEntry[], labels: string[], history: ClaudeRadarHistoryPoint[]) {
  const year = new Date().getUTCFullYear()
  const rows = new Map<string, Record<string, string | number | undefined>>()
  labels.forEach((label, index) => {
    const timestamp = parseLabel(label, year)
    const displayLabel = Number.isFinite(timestamp) ? compactDateTime(new Date(timestamp).toISOString()) : label
    const row: Record<string, string | number | undefined> = { date: label, label: displayLabel }
    models.forEach((model) => { row[`score_${model.key}`] = normalizeScore(model.iq?.[index] ?? undefined); row[`cost_${model.key}`] = model.cost?.[index] ?? undefined; row[`latency_${model.key}`] = finite(model.time?.[index]) ? (model.time?.[index] as number) * 3600 : undefined })
    rows.set(label, row)
  })
  history.forEach((point) => {
    const key = point.observed_at
    const row = rows.get(key) ?? { date: key, label: formatLabel(key) }
    row[`score_${point.config_key}`] = normalizeScore(point.score)
    row[`cost_${point.config_key}`] = point.cost_usd
    row[`latency_${point.config_key}`] = point.wall_seconds
    rows.set(key, row)
  })
  const sorted = [...rows.values()].sort((a, b) => {
    const first = parseLabel(String(a.date), year); const second = parseLabel(String(b.date), year)
    return (Number.isFinite(first) ? first : 0) - (Number.isFinite(second) ? second : 0)
  })
  return sorted
}

function filterRows(rows: Array<Record<string, string | number | undefined>>, windowDays: TimeWindow) {
  if (windowDays === "all" || rows.length === 0) return rows
  const timestamps = rows.map((row) => parseLabel(String(row.date))).filter(Number.isFinite)
  if (timestamps.length === 0) return rows
  const cutoff = Math.max(...timestamps) - windowDays * 24 * 60 * 60 * 1000
  return rows.filter((row) => { const timestamp = parseLabel(String(row.date)); return !Number.isFinite(timestamp) || timestamp >= cutoff })
}

function colorForKey(key: string) { let hash = 0; for (const character of key) hash = (hash * 31 + character.charCodeAt(0)) | 0; return chartColors[Math.abs(hash) % chartColors.length] }

function ClaudeComparisonRow({ model }: { model: ClaudeEntry }) {
  const pass = lastValue(model.pass); const valid = lastValue(model.valid); const invalid = lastValue(model.invalid); const cache = lastValue(model.cache)
  return <tr className="border-t transition-colors hover:bg-muted/20"><td className="px-5 py-3"><div className="whitespace-nowrap font-medium">{model.name}</div><div className="mt-1 text-xs text-muted-foreground">{model.key}</div></td><td className="px-4 py-3 text-right font-mono font-semibold">{formatNumber(model.score)}</td><td className="px-4 py-3 text-right text-muted-foreground">{pass != null && valid != null ? `${pass}/${valid}${invalid ? ` · ${invalid} invalid` : ""}` : "—"}</td><td className="px-4 py-3 text-right tabular-nums">{formatCost(lastValue(model.cost))}</td><td className="px-4 py-3 text-right text-muted-foreground">{formatHours(lastValue(model.time))}</td><td className="px-4 py-3 text-right text-muted-foreground">{cache != null ? `${cache.toFixed(1)}%` : "—"}</td><td className="px-5 py-3 text-right text-muted-foreground whitespace-nowrap">{model.latest_at ? dateTime(model.latest_at) : model.latest_label ? formatLabel(model.latest_label) : "—"}</td></tr>
}

function EmptyClaude({ error, lastAttempt }: { error?: string; lastAttempt?: string | null }) { return <div className="flex min-h-[50vh] items-center justify-center rounded-lg border border-dashed bg-muted/20 px-6 text-center"><div><AlertTriangle className={cn("mx-auto size-8", error ? "text-warning" : "text-muted-foreground")} /><h2 className="mt-3 text-base font-semibold">{error ? "Claude 雷达暂时不可用" : "暂无 Claude 雷达缓存"}</h2><p className="mt-1 max-w-md text-sm text-muted-foreground">{error ?? "后台会按配置周期抓取，也可以点击右上角立即刷新。"}</p>{lastAttempt ? <p className="mt-2 text-xs text-muted-foreground">最近尝试：{dateTime(lastAttempt)}</p> : null}</div></div> }
