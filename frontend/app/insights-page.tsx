import { useMemo, useState } from "react"
import { Bar, BarChart, CartesianGrid, ComposedChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts"
import { ArrowDownRight, ArrowUpRight, CircleDollarSign, Gauge, RefreshCw, WalletCards } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { StatusMetricCard } from "@/components/ui/status-metric-card"
import { PageHeader } from "@/components/ui/page-header"
import { useBalanceTrend, useChannels, useCostTrend, useRateChangeAnalytics } from "@/lib/queries"
import { channelTypeLabel, formatRatio, money, relativeTime } from "@/lib/format"
import { formatInsightsMoneyTooltip, formatInsightsRateChangeLabel, formatInsightsRateChangeTooltip, insightsChartColors, insightsChartTooltipProps } from "@/lib/insights-chart-theme"
import { rateChangeTrendMinWidth } from "@/lib/insights-rate-change-trend"
import { cn } from "@/lib/utils"
import type { Channel, RateChangeLog } from "@/lib/api-types"
import type { LucideIcon } from "lucide-react"

type TrendRange = 7 | 30 | 90
export default function InsightsPage() {
  const [days, setDays] = useState<TrendRange>(30)
  const [channelID, setChannelID] = useState("all")
  const channels = useChannels()
  const balance = useBalanceTrend(days)
  const cost = useCostTrend(days)
  const rateChanges = useRateChangeAnalytics(days, channelID === "all" ? undefined : Number(channelID))
  const channelMap = useMemo(() => new Map((channels.data ?? []).map((channel) => [channel.id, channel])), [channels.data])
  const trend = useMemo(() => mergeFundTrends(balance.data ?? [], cost.data ?? []), [balance.data, cost.data])
  const balanceValues = trend.flatMap((point) => point.balance == null ? [] : [point.balance])
  const latestBalance = balanceValues.at(-1) ?? null
  const firstBalance = balanceValues[0] ?? null
  const balanceDelta = latestBalance != null && firstBalance != null ? latestBalance - firstBalance : null
  const windowCost = trend.reduce((sum, point) => sum + (point.cost ?? 0), 0)
  const dailyChanges = useMemo(() => rateChanges.data?.daily ?? [], [rateChanges.data?.daily])
  const dailyChangeChartData = useMemo(() => dailyChanges.map((item) => ({ ...item, label: compactDay(item.day) })), [dailyChanges])
	const modelChanges = rateChanges.data?.models ?? []
  const recentChanges = rateChanges.data?.recent ?? []
  const fundLoading = balance.loading || balance.refreshing || cost.loading || cost.refreshing
  const rateLoading = rateChanges.loading || rateChanges.refreshing
  const loading = fundLoading || rateLoading || channels.loading || channels.refreshing
  const error = balance.error || cost.error || rateChanges.error || channels.error

  const refresh = () => {
    balance.refetch()
    cost.refetch()
    rateChanges.refetch()
    channels.refetch()
  }

  return (
    <section className="insights-chart-palette space-y-4">
      <PageHeader icon={Gauge} title="趋势分析" description="上游资金变化与倍率调整的区间分析" actions={<>
          <div className="inline-flex h-8 items-center rounded-md border border-border bg-muted/20 p-0.5">
            {([7, 30, 90] as TrendRange[]).map((value) => <button key={value} type="button" onClick={() => setDays(value)} className={cn("h-7 rounded px-3 text-xs transition-colors", days === value ? "bg-primary text-primary-foreground shadow-sm" : "text-muted-foreground hover:text-foreground")}>{`${value} 天`}</button>)}
          </div>
          <Button type="button" variant="outline" size="icon-sm" onClick={refresh} disabled={loading} aria-label="刷新趋势" title="刷新趋势"><RefreshCw className={cn("size-3.5", loading && "animate-spin")} /></Button>
        </>} />

      {error ? <div className="rounded-lg border border-danger/25 bg-danger/5 px-4 py-3 text-sm text-danger">{`趋势数据加载失败：${error}`}</div> : null}

      <section className="metric-strip grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <TrendMetric icon={WalletCards} label="期末余额快照" value={money(latestBalance)} hint={`${balanceValues.length} 个有采样日期`} />
        <TrendMetric icon={balanceDelta != null && balanceDelta < 0 ? ArrowDownRight : ArrowUpRight} label="区间余额变化" value={balanceDelta == null ? "—" : signedMoney(balanceDelta)} hint="首个与末个有效余额快照之差" tone="brand" />
        <TrendMetric icon={CircleDollarSign} label="区间上游消费" value={money(windowCost, { precise: true })} hint={`${days} 天每日消费快照汇总`} />
      </section>

      <section className="overflow-hidden rounded-lg border border-border bg-background">
        <AnalysisPanelHeader title="余额与每日消费" description={`${days} 天上游账户快照 · 余额为折线，消费为柱形`} legend={[{ label: "余额", color: insightsChartColors.balance }, { label: "每日消费", color: insightsChartColors.cost }]} />
        <div className="h-96 p-5">
          {fundLoading && trend.length === 0 ? <ChartEmpty text="正在加载资金趋势" /> : trend.length === 0 ? <ChartEmpty text="当前范围暂无资金快照" /> : (
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={trend} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                <XAxis dataKey="label" tickLine={false} axisLine={false} minTickGap={24} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} />
                <YAxis yAxisId="balance" tickLine={false} axisLine={false} width={62} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} tickFormatter={axisMoney} />
                <YAxis yAxisId="cost" orientation="right" tickLine={false} axisLine={false} width={62} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} tickFormatter={axisMoney} />
                <Tooltip {...insightsChartTooltipProps} formatter={formatInsightsMoneyTooltip} />
                <Bar yAxisId="cost" dataKey="cost" name="每日消费" fill={insightsChartColors.cost} radius={[3, 3, 0, 0]} maxBarSize={24} isAnimationActive={false} />
                <Line yAxisId="balance" type="monotone" dataKey="balance" name="余额" stroke={insightsChartColors.balance} strokeWidth={2.5} dot={false} activeDot={{ r: 4, fill: insightsChartColors.balance }} connectNulls={false} isAnimationActive={false} />
              </ComposedChart>
            </ResponsiveContainer>
          )}
        </div>
      </section>

      <div className="flex flex-wrap items-end justify-between gap-3 border-b border-border pb-2 pt-1">
        <div title="渠道筛选仅作用于下方倍率图表和明细"><h2 className="text-lg font-semibold tracking-tight text-foreground">{"倍率分析"}</h2><div className="mt-2 flex flex-wrap gap-2 text-[11px]"><Badge variant="outline" className="border-danger/25 text-danger">{`上涨 ${rateChanges.data?.up ?? 0}`}</Badge><Badge variant="outline" className="border-success/25 text-success">{`下降 ${rateChanges.data?.down ?? 0}`}</Badge><Badge variant="outline" className="text-muted-foreground">{`综合 ${rateChanges.data?.other ?? 0}`}</Badge><span className="self-center text-muted-foreground">{`共 ${rateChanges.data?.total ?? 0} 次`}</span></div></div>
        <Select value={channelID} onValueChange={setChannelID} disabled={rateLoading}>
          <SelectTrigger size="sm" className="h-8 w-48 text-xs"><SelectValue placeholder="全部倍率渠道" /></SelectTrigger>
          <SelectContent align="end"><SelectItem value="all">{"全部倍率渠道"}</SelectItem>{(channels.data ?? []).map((channel) => <SelectItem key={channel.id} value={String(channel.id)}>{channel.name}</SelectItem>)}</SelectContent>
        </Select>
      </div>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.45fr)_minmax(360px,0.55fr)]">
        <div className="min-w-0 overflow-hidden rounded-xl border border-border bg-card">
          <AnalysisPanelHeader title="倍率涨跌趋势" description={`${days} 天每日倍率变动次数`} legend={[{ label: "上涨", color: insightsChartColors.rateUp }, { label: "下降", color: insightsChartColors.rateDown }, { label: "综合调整", color: insightsChartColors.rateOther }]} />
          <div className="h-72 overflow-x-auto overflow-y-hidden p-5 pb-7" role="region" aria-label="倍率涨跌趋势，可横向滚动查看所有日期" tabIndex={0}>
            {rateLoading && dailyChanges.length === 0 ? <ChartEmpty text="正在加载倍率趋势" /> : dailyChanges.length === 0 ? <ChartEmpty text="当前范围暂无倍率变动" /> : (
              <div className="h-full min-w-full" style={{ minWidth: rateChangeTrendMinWidth(dailyChangeChartData.length) }}>
                <ResponsiveContainer width="100%" height="100%"><BarChart data={dailyChangeChartData} margin={{ top: 4, right: 8, left: -8, bottom: 0 }} barGap={4} barCategoryGap="24%"><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} /><XAxis dataKey="label" tickLine={false} axisLine={false} interval={0} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} /><YAxis allowDecimals={false} tickLine={false} axisLine={false} width={36} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} /><Tooltip {...insightsChartTooltipProps} formatter={formatInsightsRateChangeTooltip} labelFormatter={formatInsightsRateChangeLabel} /><Bar dataKey="up" name="上涨" fill={insightsChartColors.rateUp} radius={[3, 3, 0, 0]} maxBarSize={20} isAnimationActive={false} /><Bar dataKey="down" name="下降" fill={insightsChartColors.rateDown} radius={[3, 3, 0, 0]} maxBarSize={20} isAnimationActive={false} /><Bar dataKey="other" name="综合调整" fill={insightsChartColors.rateOther} radius={[3, 3, 0, 0]} maxBarSize={20} isAnimationActive={false} /></BarChart></ResponsiveContainer>
              </div>
            )}
          </div>
        </div>
        <div className="min-w-0 overflow-hidden rounded-xl border border-border bg-card">
          <AnalysisPanelHeader title="受影响模型" description="当前范围变动次数前 8 个渠道 / 模型" />
          <div className="h-72 p-5">
            {rateLoading && modelChanges.length === 0 ? <ChartEmpty text="正在加载模型排行" /> : modelChanges.length === 0 ? <ChartEmpty text="暂无受影响模型" /> : (
              <ResponsiveContainer width="100%" height="100%"><BarChart data={modelChanges.map((item) => { const channelName = item.channel_name?.trim() || `#${item.channel_id}`; return { ...item, label: `${channelName} / ${item.model_name}` } })} layout="vertical" margin={{ top: 0, right: 12, left: 12, bottom: 0 }}><CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} /><XAxis type="number" allowDecimals={false} tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} /><YAxis type="category" dataKey="label" width={148} tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} tickFormatter={compactChannelModel} /><Tooltip {...insightsChartTooltipProps} formatter={(value) => [`${value} 次`, "倍率变动"]} /><Bar dataKey="changes" name="倍率变动" fill={insightsChartColors.impact} radius={[0, 4, 4, 0]} isAnimationActive={false} /></BarChart></ResponsiveContainer>
            )}
          </div>
        </div>
      </section>

      <RateChangeTable items={recentChanges} channelMap={channelMap} loading={rateLoading} />
    </section>
  )
}

function TrendMetric({ icon: Icon, label, value, hint, tone = "brand" }: { icon: LucideIcon; label: string; value: string; hint: string; tone?: "brand" | "success" | "warning" | "muted" }) {
  return <StatusMetricCard icon={Icon} label={label} value={value} hint={hint} tone={tone} valueClassName="text-4xl" />
}

function AnalysisPanelHeader({ title, description, legend = [] }: { title: string; description: string; legend?: Array<{ label: string; color: string }> }) {
  return <div className="flex min-h-12 flex-wrap items-center justify-between gap-x-4 gap-y-2 border-b border-border bg-muted/20 px-5 py-3" title={description}><h2 className="text-lg font-semibold tracking-tight text-foreground">{title}</h2>{legend.length > 0 ? <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground" aria-label={`${title}图例`}>{legend.map((item) => <span key={item.label} className="inline-flex items-center gap-1.5"><span className="size-2 rounded-[2px]" style={{ backgroundColor: item.color }} aria-hidden="true" />{item.label}</span>)}</div> : null}</div>
}

function ChartEmpty({ text }: { text: string }) { return <div className="flex h-full items-center justify-center text-xs text-muted-foreground">{text}</div> }

function RateChangeTable({ items, channelMap, loading }: { items: RateChangeLog[]; channelMap: Map<number, Channel>; loading: boolean }) {
  return (
    <section className="overflow-hidden rounded-lg border border-border bg-background">
      <AnalysisPanelHeader title="最近倍率变动" description="当前倍率渠道与时间范围最近 20 条记录" />
      {loading && items.length === 0 ? <div className="px-5 py-10 text-center text-xs text-muted-foreground">{"正在加载明细"}</div> : items.length === 0 ? <div className="px-5 py-10 text-center text-xs text-muted-foreground">{"暂无倍率变动记录"}</div> : (
        <Table noScroll className="w-full table-fixed">
          <TableHeader className="bg-muted/20"><TableRow><TableHead className="w-32">{"时间"}</TableHead><TableHead className="w-36">{"渠道"}</TableHead><TableHead>{"模型"}</TableHead><TableHead className="w-36 text-right">{"输入倍率"}</TableHead><TableHead className="w-36 text-right">{"输出倍率"}</TableHead><TableHead className="w-24 text-center">{"方向"}</TableHead></TableRow></TableHeader>
          <TableBody>{items.map((item) => {
            const channel = channelMap.get(item.channel_id)
            const direction = rateChangeDirection(item)
            return <TableRow key={item.id}><TableCell className="text-xs text-muted-foreground">{relativeTime(item.changed_at)}</TableCell><TableCell><div className="truncate text-sm font-medium" title={channel?.name}>{channel?.name ?? `#${item.channel_id}`}</div><div className="text-[11px] text-muted-foreground">{channel ? channelTypeLabel(channel.type) : "未知渠道"}</div></TableCell><TableCell className="truncate font-medium" title={item.model_name}>{item.model_name}</TableCell><TableCell className="text-right tabular-nums" title={`上游 ${formatRatio(item.old_ratio)} → ${formatRatio(item.new_ratio)} · 成本系数 ${formatRatio(item.funding_factor_after)}`}>{`${formatRatio(effectiveOldRatio(item))} → ${formatRatio(item.new_effective_ratio)}`}</TableCell><TableCell className="text-right tabular-nums" title={`上游 ${formatRatio(item.old_completion_ratio)} → ${formatRatio(item.new_completion_ratio)}`}>{`${formatRatio(effectiveOldCompletionRatio(item))} → ${formatRatio(item.new_effective_completion_ratio)}`}</TableCell><TableCell className="text-center"><Badge variant="outline" className={cn("h-6", direction === "up" ? "border-danger/25 text-danger" : direction === "down" ? "border-success/25 text-success" : "text-muted-foreground")}>{direction === "up" ? "上涨" : direction === "down" ? "下降" : "综合"}</Badge></TableCell></TableRow>
          })}</TableBody>
        </Table>
      )}
    </section>
  )
}

function mergeFundTrends(balance: Array<{ day: string; balance: number | null }>, cost: Array<{ day: string; cost: number | null }>) {
  const rows = new Map<string, { day: string; label: string; balance: number | null; cost: number | null }>()
  for (const item of balance) rows.set(item.day, { day: item.day, label: compactDay(item.day), balance: item.balance, cost: null })
  for (const item of cost) { const current = rows.get(item.day); rows.set(item.day, { day: item.day, label: compactDay(item.day), balance: current?.balance ?? null, cost: item.cost }) }
  return [...rows.values()].sort((a, b) => a.day.localeCompare(b.day))
}

function compactDay(value: string) { return /^\d{4}-(\d{2})-(\d{2})/.test(value) ? value.slice(5, 10).replace("-", "/") : value }
function compactChannelModel(value: string) { const [channelName, ...modelParts] = value.split(" / "); const modelName = modelParts.join(" / "); return `${channelName.length > 7 ? `${channelName.slice(0, 6)}…` : channelName} / ${modelName.length > 11 ? `${modelName.slice(0, 10)}…` : modelName}` }
function axisMoney(value: number) { return Math.abs(value) >= 1000 ? `$${(value / 1000).toFixed(1)}k` : `$${value.toFixed(value < 10 ? 2 : 0)}` }
function signedMoney(value: number) { return `${value >= 0 ? "+" : "-"}${money(Math.abs(value))}` }

function rateChangeDirection(change: RateChangeLog): "up" | "down" | "other" {
  let up = false
  let down = false
  const oldRatio = effectiveOldRatio(change)
  if (oldRatio != null) {
    up ||= change.new_effective_ratio > oldRatio
    down ||= change.new_effective_ratio < oldRatio
  }
  const oldCompletion = effectiveOldCompletionRatio(change)
  if (oldCompletion != null) {
    up ||= change.new_effective_completion_ratio > oldCompletion
    down ||= change.new_effective_completion_ratio < oldCompletion
  }
  if (up === down) return "other"
  return up ? "up" : "down"
}

function effectiveOldRatio(change: RateChangeLog) { return change.old_effective_ratio ?? (change.old_ratio == null ? null : change.old_ratio * change.funding_factor_before) }
function effectiveOldCompletionRatio(change: RateChangeLog) { return change.old_effective_completion_ratio ?? (change.old_completion_ratio == null ? null : change.old_completion_ratio * change.funding_factor_before) }
