import { useMemo, useRef, useState } from "react"
import { Link } from "react-router-dom"
import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts"
import {
  AlertTriangle,
  CircleDollarSign,
  CircleCheck,
  Clock3,
  Equal,
  HeartPulse,
  LineChart,
  PauseCircle,
  RefreshCw,
  Search,
  WalletCards,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { StatusMetricCard } from "@/components/ui/status-metric-card"
import { PageHeader } from "@/components/ui/page-header"
import { useChannelCostComparison, useChannels } from "@/lib/queries"
import { channelTypeLabel, currentDayCost, money, relativeTime } from "@/lib/format"
import { channelHealthDoughnutLayout } from "@/lib/doughnut-chart-layout"
import { cn } from "@/lib/utils"
import { channelAccountingHasError, channelAccountingNeedsReview } from "@/lib/channel-accounting"
import { BalanceOverview } from "@/components/monitor/balance-overview"
import { ChannelErrorBadge, ChannelErrorDialog } from "@/components/monitor/channel-error-dialog"
import type { Channel, ChannelCostComparisonRow } from "@/lib/api-types"
import type { LucideIcon } from "lucide-react"

type MonitorTone = "brand" | "success" | "warning" | "danger" | "muted"
type MonitorFilter = "attention" | "all" | "error" | "low" | "pending" | "accounting" | "healthy" | "paused"

export default function ChannelMonitorPage() {
  const query = useChannels()
  const costComparison = useChannelCostComparison()
  const [search, setSearch] = useState("")
  const [statusFilter, setStatusFilter] = useState<MonitorFilter>("attention")
  const [errorChannel, setErrorChannel] = useState<Channel | null>(null)
  const errorTriggerRef = useRef<HTMLButtonElement | null>(null)
  const channels = useMemo(() => query.data ?? [], [query.data])
  const monitored = channels.filter((channel) => channel.monitor_enabled)
  const failed = monitored.filter((channel) => monitorStatusKey(channel) === "error")
  const pending = monitored.filter((channel) => monitorStatusKey(channel) === "pending")
  const lowBalance = monitored.filter((channel) => monitorStatusKey(channel) === "low")
  const accountingReview = monitored.filter((channel) => monitorStatusKey(channel) === "accounting")
  const healthy = monitored.filter((channel) => monitorStatusKey(channel) === "healthy")
  const attentionCount = channels.filter(channelNeedsAttention).length
  const paused = channels.length - monitored.length
  const totalBalance = channels.reduce((sum, channel) => sum + (channel.last_balance ?? 0), 0)
  const todayCost = channels.reduce((sum, channel) => sum + (currentDayCost(channel.today_cost, channel.today_cost_at, channel.today_cost_is_today) ?? 0), 0)
  const todayCostSnapshots = channels.filter((channel) => currentDayCost(channel.today_cost, channel.today_cost_at, channel.today_cost_is_today) != null).length
  const sortedRows = useMemo(() => [...channels].sort((a, b) => {
    const rank = monitorStatusRank(a) - monitorStatusRank(b)
    return rank || b.sort_order - a.sort_order || a.name.localeCompare(b.name)
  }), [channels])
  const normalizedSearch = search.trim().toLowerCase()
  const rows = useMemo(() => sortedRows.filter((channel) => {
    const matchesSearch = !normalizedSearch || [channel.name, channelTypeLabel(channel.type)].join(" ").toLowerCase().includes(normalizedSearch)
    if (!matchesSearch) return false
    if (statusFilter === "all") return true
    if (statusFilter === "attention") return channelNeedsAttention(channel)
    return monitorStatusKey(channel) === statusFilter
  }), [normalizedSearch, sortedRows, statusFilter])

  if (query.error && !query.data) {
    return <div className="space-y-4"><PageHeader icon={LineChart} title="渠道监控" description="上游账户健康、资金快照与采集状态" /><MonitorError message={query.error} retry={query.refetch} loading={query.loading} /></div>
  }

  return (
    <div className="space-y-4">
      <PageHeader icon={LineChart} title="渠道监控" description="上游账户健康、资金快照与采集状态" actions={<>
          <Button variant="outline" size="icon-sm" onClick={query.refetch} disabled={query.refreshing} aria-label="刷新渠道监控" title="刷新渠道监控"><RefreshCw className={cn("size-3.5", query.refreshing && "animate-spin")} /></Button>
          <Button size="sm" asChild className="h-8 gap-2"><Link to="/channels"><WalletCards className="size-3.5" />{"渠道管理"}</Link></Button>
        </>} />
      {query.error ? (
        <div className="flex items-center justify-between gap-3 rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-sm">
          <div className="min-w-0 truncate text-warning">{`渠道状态刷新失败：${query.error}`}</div>
          <Button variant="outline" size="sm" onClick={query.refetch} className="h-8 shrink-0" disabled={query.refreshing}>
            {"重试"}
          </Button>
        </div>
      ) : null}

      <section className="metric-strip grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <MonitorMetric
          icon={HeartPulse}
          label="健康渠道"
          value={query.loading && !query.data ? "加载中" : `${healthy.length}/${monitored.length}`}
          hint={`${failed.length} 异常 · ${accountingReview.length} 账务待复核 · ${lowBalance.length} 低余额 · ${pending.length} 待采集`}
          tone={failed.length > 0 ? "danger" : accountingReview.length + lowBalance.length + pending.length > 0 ? "warning" : healthy.length > 0 ? "success" : "muted"}
        />
        <MonitorMetric
          icon={AlertTriangle}
          label="需要关注"
          value={query.loading && !query.data ? "加载中" : `${attentionCount}`}
          hint={`${accountingReview.length} 个账务待复核 · ${paused} 个暂停监控`}
          tone={failed.length > 0 ? "danger" : accountingReview.length + lowBalance.length + pending.length > 0 ? "warning" : "success"}
        />
        <MonitorMetric
          icon={WalletCards}
          label="余额快照"
          value={query.loading && !query.data ? "加载中" : money(totalBalance)}
          hint={`${channels.filter((channel) => channel.last_balance != null).length}/${channels.length} 个渠道有余额快照`}
          tone="brand"
        />
        <MonitorMetric
          icon={CircleDollarSign}
          label="今日消费快照"
          value={query.loading && !query.data ? "加载中" : money(todayCost)}
          hint={`${todayCostSnapshots}/${channels.length} 个渠道今天有采集`}
          tone="brand"
        />
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.7fr)_minmax(340px,0.65fr)]">
        <BalanceOverview />
        <ChannelHealthPanel channels={channels} healthy={healthy.length} failed={failed.length} low={lowBalance.length} pending={pending.length} accounting={accountingReview.length} paused={paused} loading={query.loading && !query.data} />
      </section>

      <ChannelCostComparisonPanel
        rows={costComparison.data?.rows ?? []}
        generatedAt={costComparison.data?.generated_at}
        businessDayStart={costComparison.data?.business_day_start}
        loading={costComparison.loading && !costComparison.data}
        refreshing={costComparison.refreshing}
        error={costComparison.error}
        onRefresh={costComparison.refetch}
      />

      <section className="overflow-hidden rounded-lg border border-border bg-background">
        <div className="border-b border-border bg-muted/20 px-4 py-3 sm:px-5">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="min-w-0">
              <h2 className="text-lg font-semibold tracking-tight text-foreground">{"渠道运行状态"}</h2>
              <span className="text-sm text-muted-foreground">{query.loading && !query.data ? "加载中" : `${channels.length} 个渠道 · ${attentionCount} 个需要关注`}</span>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 border-t border-border pt-3">
            <div className="relative min-w-52 max-w-full flex-1 sm:max-w-xs">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="搜索渠道或上游类型"
                aria-label="搜索渠道或上游类型"
                className="h-8 pl-8 text-xs"
              />
            </div>
            <Select value={statusFilter} onValueChange={(value) => setStatusFilter(value as MonitorFilter)}>
              <SelectTrigger size="sm" className="h-8 w-32 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent align="start">
                <SelectItem value="attention">{"需要关注"}</SelectItem>
                <SelectItem value="all">{"全部渠道"}</SelectItem>
                <SelectItem value="error">{`异常 ${failed.length}`}</SelectItem>
                <SelectItem value="low">{`低余额 ${lowBalance.length}`}</SelectItem>
                <SelectItem value="pending">{`待采集 ${pending.length}`}</SelectItem>
                <SelectItem value="accounting">{`账务待复核 ${accountingReview.length}`}</SelectItem>
                <SelectItem value="healthy">{"健康"}</SelectItem>
                <SelectItem value="paused">{"已暂停"}</SelectItem>
              </SelectContent>
            </Select>
            {(search.trim() || statusFilter !== "all") ? (
              <span className="text-xs text-muted-foreground">{`显示 ${rows.length} / ${channels.length}`}</span>
            ) : null}
          </div>
        </div>
        <div>
          {query.loading && !query.data ? (
            <MonitorEmpty icon={Clock3} title="正在读取" description="等待渠道列表返回。" />
          ) : rows.length === 0 ? (
            <MonitorEmpty icon={LineChart} title={channels.length > 0 ? "没有匹配的渠道" : "暂无渠道"} description={channels.length > 0 ? "调整搜索或状态筛选后重试。" : "创建渠道后会在这里显示运行状态。"} />
          ) : (
            <ChannelStatusTable
              channels={rows}
              onErrorClick={(channel, trigger) => {
                errorTriggerRef.current = trigger
                setErrorChannel(channel)
              }}
            />
          )}
        </div>
      </section>

      <ChannelErrorDialog
        channel={errorChannel}
        onOpenChange={(open) => {
          if (!open) setErrorChannel(null)
        }}
        restoreFocus={() => {
          const trigger = errorTriggerRef.current
          errorTriggerRef.current = null
          if (!trigger?.isConnected) return false
          trigger.focus()
          return document.activeElement === trigger
        }}
      />
    </div>
  )
}

function ChannelCostComparisonPanel({
  rows,
  generatedAt,
  businessDayStart,
  loading,
  refreshing,
  error,
  onRefresh,
}: {
  rows: ChannelCostComparisonRow[]
  generatedAt?: string
  businessDayStart?: string
  loading: boolean
  refreshing: boolean
  error: string | null
  onRefresh: () => void
}) {
  const sorted = useMemo(() => [...rows].sort((left, right) => {
    const leftDifference = Math.abs(left.difference_today ?? 0)
    const rightDifference = Math.abs(right.difference_today ?? 0)
    return rightDifference - leftDifference || right.gateway_cost_24h - left.gateway_cost_24h || left.channel_name.localeCompare(right.channel_name)
  }), [rows])
  const accountTotal = sorted.reduce((sum, row) => sum + (row.account_cost_today ?? 0), 0)
  const gatewayTodayTotal = sorted.reduce((sum, row) => sum + row.gateway_cost_today, 0)
  const gateway24HoursTotal = sorted.reduce((sum, row) => sum + row.gateway_cost_24h, 0)
  return (
    <section className="overflow-hidden rounded-lg border border-border bg-background">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-muted/20 px-4 py-3 sm:px-5">
        <div className="min-w-0">
          <h2 className="text-lg font-semibold tracking-tight text-foreground">{"渠道费用口径对比"}</h2>
          <p className="mt-1 text-xs text-muted-foreground" title={businessDayStart ? `今日窗口开始于 ${businessDayStart}` : undefined}>
            {`今日账户 ${money(accountTotal)} · 今日网关 ${money(gatewayTodayTotal)} · 滚动 24 小时网关 ${money(gateway24HoursTotal)}${generatedAt ? ` · ${relativeTime(generatedAt)}更新` : ""}`}
          </p>
        </div>
        <Button variant="outline" size="icon-sm" onClick={onRefresh} disabled={refreshing} aria-label="刷新渠道费用对比" title="刷新渠道费用对比">
          <RefreshCw className={cn("size-3.5", refreshing && "animate-spin")} />
        </Button>
      </div>
      {error && !rows.length ? <MonitorError message={error} retry={onRefresh} loading={refreshing} /> : loading ? (
        <div className="flex min-h-48 items-center justify-center text-sm text-muted-foreground">{"正在汇总渠道费用"}</div>
      ) : sorted.length === 0 ? (
        <div className="flex min-h-48 items-center justify-center text-sm text-muted-foreground">{"暂无渠道费用数据"}</div>
      ) : (
        <div className="overflow-x-auto">
          <Table className="min-w-[920px]">
            <TableHeader className="bg-muted/20">
              <TableRow className="hover:bg-transparent">
                <TableHead className="w-[18%] px-4">{"渠道"}</TableHead>
                <TableHead className="w-[15%] text-center">{"近24小时网关"}</TableHead>
                <TableHead className="w-[15%] text-center">{"今日网关"}</TableHead>
                <TableHead className="w-[15%] text-center">{"今日账户"}</TableHead>
                <TableHead className="w-[17%] text-center">{"同日差异"}</TableHead>
                <TableHead className="w-[20%] text-center">{"24小时差异"}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {sorted.map((row) => (
                <TableRow key={row.channel_id}>
                  <TableCell className="px-4 py-2.5 font-medium" title={row.channel_name}>{row.channel_name}</TableCell>
                  <TableCell className="text-center font-mono text-xs tabular-nums">{money(row.gateway_cost_24h, { precise: true })}</TableCell>
                  <TableCell className="text-center font-mono text-xs tabular-nums">{money(row.gateway_cost_today, { precise: true })}</TableCell>
                  <TableCell className="text-center font-mono text-xs tabular-nums" title={row.account_cost_sampled_at ? `采样于 ${row.account_cost_sampled_at}` : undefined}>
                    {row.account_cost_today == null ? "待采集" : money(row.account_cost_today, { precise: true })}
                  </TableCell>
                  <TableCell className="text-center"><CostDifferenceBadge value={row.difference_today} sameWindow /></TableCell>
                  <TableCell className="text-center"><CostDifferenceBadge value={row.difference_24h} /></TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </section>
  )
}

function CostDifferenceBadge({ value, sameWindow = false }: { value?: number | null; sameWindow?: boolean }) {
  if (value == null) return <Badge variant="outline" className="text-muted-foreground">{"待采集"}</Badge>
  const tolerance = 0.005
  if (Math.abs(value) < tolerance) {
    return <Badge variant="outline" className="gap-1 border-success/25 bg-success/5 text-success"><Equal className="size-3" />{"基本一致"}</Badge>
  }
  const accountHigher = value > 0
  const label = accountHigher ? `账户多 ${money(Math.abs(value), { precise: true })}` : `网关多 ${money(Math.abs(value), { precise: true })}`
  return (
    <Badge
      variant="outline"
      className={cn("max-w-full", sameWindow ? "border-warning/30 bg-warning/5 text-warning" : "text-muted-foreground")}
      title={sameWindow ? "同为当前业务日窗口，差异可能来自非网关消费、上游延迟入账或费用尚未对账" : "账户是今日自然日，网关是滚动 24 小时，窗口不同会产生正常差异"}
    >
      {label}
    </Badge>
  )
}

function ChannelStatusTable({
  channels,
  onErrorClick,
}: {
  channels: Channel[]
  onErrorClick: (channel: Channel, trigger: HTMLButtonElement) => void
}) {
  return (
    <Table noScroll className="w-full table-fixed">
      <TableHeader className="bg-muted/20">
        <TableRow className="hover:bg-transparent">
          <TableHead className="w-[14%] px-3">{"渠道"}</TableHead>
          <TableHead className="w-[10%] text-center">{"状态"}</TableHead>
          <TableHead className="w-[14%] text-center">{"余额"}</TableHead>
          <TableHead className="w-[14%] text-center">{"今日消费"}</TableHead>
          <TableHead className="w-[14%] text-center">{"累计消费"}</TableHead>
          <TableHead className="w-[14%] text-center">{"最后采集"}</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {channels.map((channel) => {
          const status = monitorStatus(channel)
          return (
            <TableRow key={channel.id}>
              <TableCell className="px-3 py-3 align-middle">
                <div className="min-w-0">
                  <div className="truncate font-medium text-foreground" title={channel.name}>{channel.name}</div>
                  <div className="mt-0.5 truncate text-xs text-muted-foreground">{channelTypeLabel(channel.type)}</div>
                </div>
              </TableCell>
              <TableCell className="text-center align-middle">
                <div className="flex flex-col items-center gap-1">
                  {channel.last_error ? (
                    <ChannelErrorBadge channel={channel} onClick={(trigger) => onErrorClick(channel, trigger)} />
                  ) : (
                    <MonitorStatusBadge status={status} />
                  )}
                </div>
              </TableCell>
              <TableCell className="text-center align-middle tabular-nums">{channel.last_balance == null ? "—" : money(channel.last_balance)}</TableCell>
              <TableCell className="text-center align-middle tabular-nums">{currentDayCost(channel.today_cost, channel.today_cost_at, channel.today_cost_is_today) == null ? "—" : money(currentDayCost(channel.today_cost, channel.today_cost_at, channel.today_cost_is_today) ?? 0)}</TableCell>
              <TableCell className="text-center align-middle tabular-nums">{channel.total_cost == null ? "—" : money(channel.total_cost)}</TableCell>
              <TableCell className="text-center align-middle text-xs text-muted-foreground" title={channel.last_balance_at ?? undefined}>
                {channel.last_balance_at ? relativeTime(channel.last_balance_at) : "未采集"}
              </TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}

function ChannelHealthPanel({ channels, healthy, failed, low, pending, accounting, paused, loading }: { channels: Channel[]; healthy: number; failed: number; low: number; pending: number; accounting: number; paused: number; loading: boolean }) {
  const data = [
    { name: "健康", value: healthy, color: "var(--success)" },
    { name: "异常", value: failed, color: "var(--danger)" },
    { name: "低余额", value: low, color: "var(--warning)" },
    { name: "待采集", value: pending, color: "var(--chart-1)" },
    { name: "账务待复核", value: accounting, color: "var(--warning)" },
    { name: "已暂停", value: paused, color: "var(--muted-foreground)" },
  ].filter((item) => item.value > 0)
  const attention = channels
    .filter(channelNeedsAttention)
    .sort((left, right) => monitorStatusRank(left) - monitorStatusRank(right))
    .slice(0, 5)
  return (
    <section className="flex min-h-0 flex-col overflow-hidden rounded-lg border border-border bg-background">
      <div className="border-b border-border bg-muted/20 px-5 py-4" title="按渠道当前采集与资金状态分类">
        <h2 className="text-lg font-semibold tracking-tight text-foreground">{"健康状态分布"}</h2>
      </div>
      <div className="grid min-h-0 flex-1 grid-rows-[190px_auto] px-5 py-4">
        {loading ? <div className="flex items-center justify-center text-xs text-muted-foreground">{"正在加载状态"}</div> : data.length === 0 ? <div className="flex items-center justify-center text-xs text-muted-foreground">{"暂无渠道"}</div> : (
          <div className="relative min-h-0">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={data} dataKey="value" nameKey="name" {...channelHealthDoughnutLayout} paddingAngle={2} stroke="var(--background)" strokeWidth={2} isAnimationActive={false}>
                  {data.map((item) => <Cell key={item.name} fill={item.color} />)}
                </Pie>
                <Tooltip formatter={(value, name) => [`${value} 个`, name]} />
              </PieChart>
            </ResponsiveContainer>
            <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
              <span className="text-2xl font-semibold tabular-nums text-foreground">{channels.length}</span>
              <span className="text-[11px] text-muted-foreground">{"全部渠道"}</span>
            </div>
          </div>
        )}
        <div className="min-h-0 border-t border-border pt-3">
          <div className="flex flex-wrap gap-x-3 gap-y-1.5">
            {data.map((item) => <span key={item.name} className="inline-flex items-center gap-1.5 text-[11px] text-muted-foreground"><i className="size-2 rounded-full" style={{ backgroundColor: item.color }} />{`${item.name} ${item.value}`}</span>)}
          </div>
          <div className="mt-3 text-[11px] font-medium text-muted-foreground">{"优先处理"}</div>
          {attention.length > 0 ? (
            <div className="mt-1 divide-y divide-border">
              {attention.map((channel) => {
                const status = monitorStatus(channel)
                return <div key={channel.id} className="flex items-center justify-between gap-3 py-2"><span className="min-w-0 truncate text-xs font-medium text-foreground" title={channel.name}>{channel.name}</span><div className="flex shrink-0 flex-col items-end gap-1"><MonitorStatusBadge status={status} /></div></div>
              })}
            </div>
          ) : <div className="mt-3 flex items-center gap-2 text-xs text-success"><CircleCheck className="size-4" />{"当前没有需处理渠道"}</div>}
        </div>
      </div>
    </section>
  )
}

function MonitorMetric({
  icon: Icon,
  label,
  value,
  hint,
  tone,
}: {
  icon: LucideIcon
  label: string
  value: string
  hint: string
  tone: MonitorTone
}) {
  return <StatusMetricCard icon={Icon} label={label} value={value} hint={hint} tone={tone} valueClassName="text-4xl" />
}

function MonitorStatusBadge({ status }: { status: ReturnType<typeof monitorStatus> }) {
  const Icon = status.icon
  return (
    <Badge variant="outline" className={cn("h-6 gap-1.5 rounded-md px-2 text-[11px] font-medium", status.className)}>
      <Icon className="size-3" />
      {status.label}
    </Badge>
  )
}

function channelNeedsAttention(channel: Channel) {
  return ["error", "accounting", "low", "pending"].includes(monitorStatusKey(channel))
}

function monitorStatusKey(channel: Channel): Exclude<MonitorFilter, "all"> {
  if (channelAccountingHasError(channel)) return "error"
  if (channelAccountingNeedsReview(channel)) return "accounting"
  if (!channel.monitor_enabled) return "paused"
  if (channel.last_error) return "error"
  if (!channel.last_balance_at || channel.last_balance == null) return "pending"
  if (isLowBalance(channel)) return "low"
  return "healthy"
}

function monitorStatus(channel: Channel) {
  if (channelAccountingHasError(channel)) {
    return { label: "账务异常", icon: AlertTriangle, className: "border-danger/25 bg-danger/5 text-danger" }
  }
  if (channelAccountingNeedsReview(channel)) {
    return { label: "账务待复核", icon: AlertTriangle, className: "border-warning/25 bg-warning/5 text-warning" }
  }
  if (!channel.monitor_enabled) {
    return { label: "已暂停", icon: PauseCircle, className: "border-border bg-muted/40 text-muted-foreground" }
  }
  if (channel.last_error) {
    return { label: "异常", icon: AlertTriangle, className: "border-danger/25 bg-danger/5 text-danger" }
  }
  if (!channel.last_balance_at || channel.last_balance == null) {
    return { label: "待采集", icon: Clock3, className: "border-warning/25 bg-warning/5 text-warning" }
  }
  if (isLowBalance(channel)) {
    return { label: "低余额", icon: AlertTriangle, className: "border-warning/25 bg-warning/5 text-warning" }
  }
  return { label: "已采集", icon: CircleCheck, className: "border-success/25 bg-success/5 text-success" }
}

function isLowBalance(channel: Channel) {
  return channel.balance_threshold > 0 && channel.last_balance != null && channel.last_balance < channel.balance_threshold
}

function monitorStatusRank(channel: Channel) {
  const ranks: Record<ReturnType<typeof monitorStatusKey>, number> = { error: 0, accounting: 1, low: 2, pending: 3, healthy: 4, paused: 5, attention: 6 }
  return ranks[monitorStatusKey(channel)]
}

function MonitorEmpty({ icon: Icon, title, description }: { icon: LucideIcon; title: string; description: string }) {
  return (
    <div className="flex min-h-72 flex-col items-center justify-center p-6 text-center">
      <span className="flex size-10 items-center justify-center rounded-lg bg-muted text-muted-foreground">
        <Icon className="size-5" />
      </span>
      <div className="mt-3 text-sm font-medium text-foreground">{title}</div>
      <p className="mt-1 text-xs text-muted-foreground">{description}</p>
    </div>
  )
}

function MonitorError({ message, retry, loading }: { message: string; retry: () => void; loading: boolean }) {
  return (
    <div className="flex min-h-[60vh] items-center justify-center">
      <div className="w-full max-w-lg rounded-lg border border-danger/25 bg-background p-6 text-center shadow-none">
        <span className="mx-auto flex size-10 items-center justify-center rounded-lg bg-danger/10 text-danger">
          <AlertTriangle className="size-5" />
        </span>
        <h2 className="mt-4 text-base font-semibold text-foreground">{"渠道状态加载失败"}</h2>
        <p className="mt-2 break-words text-sm leading-6 text-muted-foreground">{message}</p>
        <Button onClick={retry} disabled={loading} className="mt-5 gap-2">
          <RefreshCw className={cn("size-4", loading && "animate-spin")} />
          {loading ? "重试中" : "重新加载"}
        </Button>
      </div>
    </div>
  )
}
