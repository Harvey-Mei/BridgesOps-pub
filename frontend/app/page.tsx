import { useEffect, useMemo, useState } from "react"
import type { ReactNode } from "react"
import { Link } from "react-router-dom"
import { Bar, BarChart, CartesianGrid, Cell, ComposedChart, Legend, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts"
import {
  AlertTriangle,
  ArrowRight,
  Activity,
  BarChart3,
  CircleDollarSign,
  Clock3,
  Gauge,
  LayoutDashboard,
  Plus,
  RefreshCw,
  ShieldCheck,
  WalletCards,
  Zap,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { PageHeader } from "@/components/ui/page-header"
import { StatusMetricCard } from "@/components/ui/status-metric-card"
import { useDashboardMultiplierTrend, useDashboardSummary, useGatewayRequestLogAnalytics, useGatewayRequestLogs, useSystemConfig } from "@/lib/queries"
import { channelTypeLabel, compactCount, compactDateTime, compactTokenCount, dateTimeInputValue, formatRatio, money, normalizeDisplayTimeZone, relativeTime } from "@/lib/format"
import { dashboardMultiplierChartPoints, dashboardMultiplierCoverage, dashboardMultiplierCoveragePercent, dashboardMultiplierHasSeriesCoverage, dashboardMultiplierIsTrustedValue, dashboardMultiplierPointForBucket, dashboardMultiplierSuccessfulRequests, dashboardMultiplierTrendApp, dashboardMultiplierTrendDisplayState } from "@/lib/dashboard-multiplier-trend"
import { overviewChannelCostDoughnutLayout } from "@/lib/doughnut-chart-layout"
import { cn } from "@/lib/utils"
import { useAddChannel } from "@/lib/add-channel-context"
import { useRefreshTimestamp } from "@/lib/refresh-context"
import type { DashboardMultiplierTrendApp, DashboardSummary, GatewayRequestLogBreakdownItem, GatewayRequestLogTrendPoint, RateChangeLog } from "@/lib/api-types"
import type { LucideIcon } from "lucide-react"

type Tone = "brand" | "success" | "warning" | "danger" | "muted"

const overviewChartColors = ["#2563eb", "#0891b2", "#4f46e5", "#0f766e", "#64748b", "#7c3aed"]

export default function Page() {
  const [rangeHours, setRangeHours] = useState(24)
  const [rangeAnchor, setRangeAnchor] = useState(() => Date.now())
  const [usageQueryTimeZone, setUsageQueryTimeZone] = useState<string | null>(null)
  const refreshTimestamp = useRefreshTimestamp()
  const systemConfig = useSystemConfig()
  const configuredTimeZone = systemConfig.data?.effective_time_zone?.trim() ?? ""
  const usageTimeZone = configuredTimeZone ? normalizeDisplayTimeZone(configuredTimeZone) : ""
  const usageTimeZoneReady = usageTimeZone !== "" && usageQueryTimeZone === usageTimeZone
  useEffect(() => {
    if (!usageTimeZone) return
    setUsageQueryTimeZone(usageTimeZone)
  }, [usageTimeZone])
  const effectiveRangeAnchor = Math.max(rangeAnchor, refreshTimestamp)
  const rangeWindow = useMemo(() => {
    if (!usageTimeZoneReady) return {}
    return {
      start: dateTimeInputValue(new Date(effectiveRangeAnchor - rangeHours * 60 * 60 * 1000), usageTimeZone),
      end: dateTimeInputValue(new Date(effectiveRangeAnchor), usageTimeZone),
    }
  }, [effectiveRangeAnchor, rangeHours, usageTimeZone, usageTimeZoneReady])
  const summary = useDashboardSummary()
  const gateway = useGatewayRequestLogs(1, 1, usageTimeZoneReady, rangeWindow, usageTimeZone)
  const analytics = useGatewayRequestLogAnalytics(usageTimeZoneReady, rangeWindow, usageTimeZone)
  const multiplierTrend = useDashboardMultiplierTrend(usageTimeZoneReady, rangeWindow, usageTimeZone)
  const { openAdd } = useAddChannel()
  const data = summary.data
  const loaded = Boolean(data)
  const totalChannels = data?.total_channels ?? 0
  const monitoredChannels = (data?.channels ?? []).filter((channel) => channel.monitor_enabled)
  const healthyChannels = monitoredChannels.filter((channel) => !channel.last_error).length
  const failedChannels = monitoredChannels.length - healthyChannels
  const pausedChannels = Math.max(0, totalChannels - monitoredChannels.length)
  const healthyRate = monitoredChannels.length > 0 ? Math.round((healthyChannels / monitoredChannels.length) * 100) : 0
  const riskItems = buildRiskItems(data)
  const recentRateChanges = data?.recent_rate_changes?.slice(0, 5) ?? []
  const channelNames = new Map((data?.channels ?? []).map((channel) => [channel.id, channel.name]))
  const usageDataVisible = usageTimeZoneReady
  const gatewaySummary = usageDataVisible ? gateway.data?.summary : null
  const requestTotal = gatewaySummary?.total_requests ?? 0
  const completedRequests = (gatewaySummary?.success ?? 0) + (gatewaySummary?.failed ?? 0)
  const requestSuccessRate = completedRequests > 0 ? ((gatewaySummary?.success ?? 0) / completedRequests) * 100 : 0
  const usageDataPending = systemConfig.loading || (!systemConfig.error && !usageTimeZoneReady) || gateway.loading
  const analyticsPending = usageDataPending || analytics.loading
  const multiplierTrendPending = systemConfig.loading || (!systemConfig.error && !usageTimeZoneReady) || multiplierTrend.loading
  const usageDataError = systemConfig.error || gateway.error || analytics.error || multiplierTrend.error
  const trendRows = (usageDataVisible ? analytics.data?.trend ?? [] : []).map((point) => ({ ...point, label: overviewBucketLabel(point.bucket, usageTimeZone) }))
  const codexMultiplierTrend = dashboardMultiplierTrendApp(usageDataVisible ? multiplierTrend.data : undefined, "codex")
  const claudeMultiplierTrend = dashboardMultiplierTrendApp(usageDataVisible ? multiplierTrend.data : undefined, "claude")
  const channelCostRows = (usageDataVisible ? analytics.data?.channels ?? [] : []).filter((item) => item.total_cost > 0).slice(0, 6).map((item) => ({ ...item, label: compactOverviewLabel(item.name) }))
  const refreshDashboard = () => {
    summary.refetch()
    systemConfig.refetch()
    gateway.refetch()
    analytics.refetch()
    multiplierTrend.refetch()
    setRangeAnchor(Date.now())
  }

  if (summary.error && !data) {
    return <div className="space-y-4"><PageHeader icon={LayoutDashboard} title="运营总览" description="上游账户状态与网关请求表现" /><DashboardError message={summary.error} retry={summary.refetch} loading={summary.loading} /></div>
  }

  return (
    <div className="space-y-4">
      <PageHeader
        icon={LayoutDashboard}
        title="运营总览"
        description="上游账户状态与网关请求表现"
        actions={<>
          <div className="inline-flex h-8 items-center rounded-md border border-border bg-muted/20 p-0.5">
            {[{ label: "24 小时", value: 24 }, { label: "7 天", value: 168 }, { label: "30 天", value: 720 }].map((option) => (
              <button key={option.value} type="button" onClick={() => { setRangeHours(option.value); setRangeAnchor(Date.now()) }} className={cn("h-7 rounded px-3 text-xs transition-colors", rangeHours === option.value ? "bg-primary text-primary-foreground shadow-sm" : "text-muted-foreground hover:text-foreground")}>{option.label}</button>
            ))}
          </div>
          <Button type="button" size="sm" className="h-8 gap-1.5 text-xs" onClick={openAdd}><Plus className="size-3.5" />{"添加渠道"}</Button>
          <Button type="button" variant="outline" size="icon-sm" onClick={refreshDashboard} disabled={summary.refreshing || systemConfig.refreshing || gateway.refreshing || analytics.refreshing || multiplierTrend.refreshing} aria-label="刷新总览" title="刷新总览">
            <RefreshCw className={cn("size-3.5", (summary.refreshing || systemConfig.refreshing || gateway.refreshing || analytics.refreshing || multiplierTrend.refreshing) && "animate-spin")} />
          </Button>
        </>}
      />
      {summary.error ? (
        <div className="flex items-center justify-between gap-3 rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-sm">
          <div className="min-w-0 truncate text-warning">{`数据刷新失败：${summary.error}`}</div>
          <Button variant="outline" size="sm" onClick={summary.refetch} className="h-8 shrink-0" disabled={summary.refreshing}>
            {"重试"}
          </Button>
        </div>
      ) : null}
      {usageDataError ? (
        <div className="flex items-center justify-between gap-3 rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-sm">
          <div className="min-w-0 truncate text-warning">{`网关统计刷新失败：${usageDataError}`}</div>
          <Button variant="outline" size="sm" onClick={refreshDashboard} className="h-8 shrink-0" disabled={systemConfig.refreshing || gateway.refreshing || analytics.refreshing || multiplierTrend.refreshing}>{"重试"}</Button>
        </div>
      ) : null}

      <section className="metric-strip grid gap-4 sm:grid-cols-2 xl:grid-cols-6">
        <ConsoleMetric
          icon={Activity}
          label={`${rangeLabel(rangeHours)}请求`}
          value={usageDataPending ? "加载中" : compactCount(requestTotal)}
          hint={`成功率 ${requestSuccessRate.toFixed(1)}% · 失败 ${compactCount(gatewaySummary?.failed)} · 取消 ${compactCount(gatewaySummary?.canceled)}`}
          tone={requestSuccessRate >= 98 ? "success" : completedRequests > 0 ? "warning" : "muted"}
        />
        <ConsoleMetric
          icon={CircleDollarSign}
          label="网关费用"
          value={usageDataPending ? "加载中" : money(gatewaySummary?.total_cost)}
          hint={`${rangeLabel(rangeHours)} · 下游 API 请求口径 · 上游已返回 ${money(gatewaySummary?.reported_cost)} · 已核对 ${money(gatewaySummary?.reconciled_cost)} · 本地估算 ${money(gatewaySummary?.estimated_cost)}`}
          tone="brand"
        />
        <ConsoleMetric
          icon={WalletCards}
          label="上游账户余额"
          value={loaded ? money(data?.total_balance ?? 0) : "加载中"}
          hint={data?.lowest_balance ? `最低 ${data.lowest_balance.name} · ${money(data.lowest_balance.balance)}` : "暂无余额快照"}
          tone="brand"
        />
        <ConsoleMetric
          icon={Gauge}
          label="今日账户消费"
          value={loaded ? money(data?.today_total_cost ?? 0) : "加载中"}
          hint={loaded ? `上游账户口径，按系统时区当天 · 账户累计 ${money(data?.total_cost ?? 0)}` : "正在读取账户费用"}
          tone="brand"
        />
        <ConsoleMetric
          icon={ShieldCheck}
          label="渠道健康"
          value={loaded ? (monitoredChannels.length > 0 ? `${healthyChannels}/${monitoredChannels.length}` : "—") : "加载中"}
          hint={loaded ? `${healthyRate}% 正常 · ${failedChannels} 异常 · ${pausedChannels} 暂停` : "正在读取渠道状态"}
          tone={failedChannels > 0 ? "danger" : healthyChannels > 0 ? "success" : "muted"}
        />
        <ConsoleMetric
          icon={Zap}
          label="当前并发"
          value={usageDataPending ? "加载中" : compactCount(gatewaySummary?.current_concurrency)}
          hint={`${compactCount(gatewaySummary?.rpm)} RPM · ${compactTokenCount(gatewaySummary?.tpm)} TPM · 当前实例实时值`}
          tone={(gatewaySummary?.current_concurrency ?? 0) > 0 ? "success" : "brand"}
        />
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.7fr)_minmax(340px,0.65fr)]">
        <OverviewChartPanel title="请求与费用趋势" description={`${rangeLabel(rangeHours)} · 本地估算、上游已返回与已核对费用`}>
          {analyticsPending ? <ChartPlaceholder text="正在加载趋势" /> : trendRows.length === 0 ? <ChartPlaceholder text="当前时间范围暂无请求" /> : (
            <SynchronizedTrendBars rows={trendRows} />
          )}
        </OverviewChartPanel>
        <OverviewChartPanel title="渠道费用排行" description="下游网关请求归属 · 前 6 个渠道">
          {analyticsPending ? <ChartPlaceholder text="正在加载排行" /> : channelCostRows.length === 0 ? <ChartPlaceholder text="暂无渠道费用" /> : (
            <div className="flex h-full min-h-0 flex-col">
              <div className="min-h-0 flex-1">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={channelCostRows} dataKey="total_cost" nameKey="label" cx="50%" cy="50%" {...overviewChannelCostDoughnutLayout} paddingAngle={2}>
                      {channelCostRows.map((item, index) => <Cell key={item.name} fill={overviewChartColors[index % overviewChartColors.length]} />)}
                    </Pie>
                    <Tooltip content={<OverviewChannelCostTooltip />} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="grid shrink-0 grid-cols-2 gap-x-4 gap-y-1.5 text-xs">
                {channelCostRows.map((item, index) => <div key={item.name} className="flex min-w-0 items-center gap-2"><span className="size-2.5 shrink-0 rounded-full" style={{ backgroundColor: overviewChartColors[index % overviewChartColors.length] }} /><span className="truncate text-muted-foreground" title={item.name}>{item.label}</span><span className="ml-auto shrink-0 text-right tabular-nums"><span className="block font-medium text-foreground">{money(item.total_cost)}</span><span className="block text-[10px] text-muted-foreground">{`返回 ${money(item.reported_cost)} · 核对 ${money(item.reconciled_cost)}`}</span></span></div>)}
              </div>
            </div>
          )}
        </OverviewChartPanel>
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.45fr)_minmax(320px,0.55fr)]">
        <section className="overflow-hidden rounded-xl border border-border bg-card shadow-sm">
          <div className="border-b border-border bg-muted/20 px-5 py-4">
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0">
                <h2 className="text-lg font-semibold tracking-tight text-foreground">{"当前关注"}</h2>
                <p className="mt-1 text-xs text-muted-foreground">
                  {loaded
                    ? riskItems.length > 0
                      ? `${riskItems.length} 项需要处理`
                      : monitoredChannels.length > 0
                        ? "当前无已知异常"
                        : "当前没有启用渠道监控"
                    : "正在读取渠道状态"}
                </p>
              </div>
              <div className="flex items-center gap-1">
                <Button variant="ghost" size="icon" onClick={summary.refetch} className="size-8" disabled={summary.refreshing}>
                  <RefreshCw className={cn("size-3.5", summary.refreshing && "animate-spin")} />
                  <span className="sr-only">{"刷新"}</span>
                </Button>
                <Button variant="ghost" size="sm" asChild className="h-8 px-2 text-xs">
                  <Link to="/channel-monitor">
                    {"监控"}
                    <ArrowRight className="ml-1 size-3.5" />
                  </Link>
                </Button>
              </div>
            </div>
          </div>
          <div>
            {loaded ? (
              riskItems.length > 0 ? (
                <div className="divide-y divide-border">
                  {riskItems.slice(0, 8).map((item) => (
                    <RiskRow key={item.key} item={item} />
                  ))}
                </div>
              ) : (
                <div className="p-4">
                  <EmptyState icon={ShieldCheck} title="暂无异常" description="当前没有登录失败或低余额渠道。" />
                </div>
              )
            ) : (
              <div className="p-4">
                <EmptyState icon={Clock3} title="正在读取" description="等待后台快照返回。" />
              </div>
            )}
          </div>
        </section>

        <section className="overflow-hidden rounded-xl border border-border bg-card shadow-sm">
          <div className="border-b border-border bg-muted/20 px-5 py-4">
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0" title="最近 5 条倍率变化">
                <h2 className="text-lg font-semibold tracking-tight text-foreground">{"近期倍率变动"}</h2>
              </div>
              <Button variant="ghost" size="sm" asChild className="h-8 px-2 text-xs">
                <Link to="/insights">
                  {"趋势"}
                  <ArrowRight className="ml-1 size-3.5" />
                </Link>
              </Button>
            </div>
          </div>
          <div>
            {loaded && recentRateChanges.length > 0 ? (
              <div className="divide-y divide-border">
                {recentRateChanges.map((change) => (
                  <RateChangeRow key={change.id} change={change} channelName={channelNames.get(change.channel_id)} />
                ))}
              </div>
            ) : (
              <div className="p-4">
                <EmptyState
                  icon={BarChart3}
                  title={loaded ? "暂无变动" : "正在读取"}
                  description={loaded ? "最近没有倍率变化记录。" : "等待倍率快照返回。"}
                />
              </div>
            )}
          </div>
        </section>
      </section>

      <section className="grid gap-4 xl:grid-cols-2">
        <OverviewChartPanel title="Codex 实际倍率趋势" description="成功请求的 Codex app 与 Token 分母来自 v1 请求记录；覆盖和输入/输出倍率只读取严格最终派发审计中的冻结事实。仅用当前及前两连续有事实桶按 Token 加权平滑。">
          {multiplierTrendPending ? <ChartPlaceholder text="正在加载实际倍率" /> : multiplierTrend.error ? <ChartPlaceholder text="实际倍率趋势加载失败" /> : <DashboardMultiplierChart data={codexMultiplierTrend} timeZone={usageTimeZone} smoothingBuckets={multiplierTrend.data?.smoothing_buckets ?? 3} />}
        </OverviewChartPanel>
        <OverviewChartPanel title="Claude 实际倍率趋势" description="成功请求的 Claude app 与 Token 分母来自 v1 请求记录；覆盖和输入/输出倍率只读取严格最终派发审计中的冻结事实。仅用当前及前两连续有事实桶按 Token 加权平滑。">
          {multiplierTrendPending ? <ChartPlaceholder text="正在加载实际倍率" /> : multiplierTrend.error ? <ChartPlaceholder text="实际倍率趋势加载失败" /> : <DashboardMultiplierChart data={claudeMultiplierTrend} timeZone={usageTimeZone} smoothingBuckets={multiplierTrend.data?.smoothing_buckets ?? 3} />}
        </OverviewChartPanel>
      </section>
    </div>
  )
}

function OverviewChartPanel({ title, description, children }: { title: string; description: string; children: ReactNode }) {
  return (
    <section className="min-w-0 overflow-hidden rounded-xl border border-border bg-card shadow-sm" title={description}>
      <div className="border-b border-border bg-muted/40 px-5 py-4 dark:bg-card">
        <h2 className="text-lg font-semibold tracking-tight text-foreground">{title}</h2>
      </div>
      <div className="h-60 p-4">{children}</div>
    </section>
  )
}

function SynchronizedTrendBars({ rows }: { rows: Array<GatewayRequestLogTrendPoint & { label: string }> }) {
  return (
    <div className="grid h-full min-h-0 gap-4 lg:grid-cols-2">
      <div className="flex min-h-0 flex-col">
        <div className="mb-2 text-sm font-medium text-foreground">{"请求数"}</div>
        <div className="min-h-0 flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={rows} syncId="overview-trend" margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
              <XAxis dataKey="label" tickLine={false} axisLine={false} minTickGap={24} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} />
              <YAxis allowDecimals={false} tickLine={false} axisLine={false} width={44} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} />
              <Tooltip content={<OverviewTrendTooltip />} cursor={{ fill: "var(--muted)" }} />
              <Bar dataKey="requests" name="请求" fill="var(--chart-1)" radius={[4, 4, 0, 0]} maxBarSize={28} isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className="flex min-h-0 flex-col">
        <div className="mb-2 text-sm font-medium text-foreground">{"费用状态"}</div>
        <div className="min-h-0 flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={rows} syncId="overview-trend" margin={{ top: 4, right: 8, left: -4, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
              <XAxis dataKey="label" tickLine={false} axisLine={false} minTickGap={24} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} />
              <YAxis tickLine={false} axisLine={false} width={56} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} tickFormatter={(value) => formatOverviewAxisCost(Number(value))} />
              <Tooltip content={<OverviewTrendTooltip />} cursor={{ fill: "var(--muted)" }} />
              <Legend iconType="circle" iconSize={7} wrapperStyle={{ fontSize: 10 }} />
              <Bar dataKey="estimated_cost" name="本地估算" stackId="cost" fill="#d97706" maxBarSize={28} isAnimationActive={false} />
              <Bar dataKey="reported_cost" name="上游已返回" stackId="cost" fill="#2563eb" maxBarSize={28} isAnimationActive={false} />
              <Bar dataKey="reconciled_cost" name="已核对" stackId="cost" fill="#0f766e" radius={[4, 4, 0, 0]} maxBarSize={28} isAnimationActive={false} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}

type DashboardMultiplierChartRow = ReturnType<typeof dashboardMultiplierChartPoints>[number] & { label: string }

function DashboardMultiplierChart({ data, timeZone, smoothingBuckets }: { data: DashboardMultiplierTrendApp; timeZone: string; smoothingBuckets: number }) {
  const rows = useMemo<DashboardMultiplierChartRow[]>(() => dashboardMultiplierChartPoints(data).map((point) => ({ ...point, label: overviewBucketLabel(point.bucket, timeZone) })), [data, timeZone])
  const coverage = useMemo(() => dashboardMultiplierCoverage(data), [data])
  const displayState = dashboardMultiplierTrendDisplayState(data)
  const successfulRequests = dashboardMultiplierSuccessfulRequests(data)
  const inputHasSeries = useMemo(() => dashboardMultiplierHasSeriesCoverage(data, "input"), [data])
  const outputHasSeries = useMemo(() => dashboardMultiplierHasSeriesCoverage(data, "output"), [data])
  if (displayState === "empty") {
    return <div className="flex h-full flex-col items-center justify-center px-4 text-center text-xs text-muted-foreground" data-multiplier-state="empty"><span>{"当前时间范围暂无成功用户请求。"}</span></div>
  }
  if (displayState === "uncovered") {
    return (
      <div className="flex h-full flex-col items-center justify-center px-4 text-center text-xs text-muted-foreground" role="status" data-multiplier-state="uncovered">
        <span className="font-medium text-foreground">{"未推断倍率"}</span>
        <div className="mt-2 grid w-full max-w-md gap-x-4 gap-y-1 text-[10px] sm:grid-cols-3" title="成功请求的 app 和 Token 分母来自 v1 请求记录；覆盖只计关联到严格最终派发审计并具有相应冻结倍率的请求。没有可绘制的冻结倍率时不使用当前价格推断。">
          <span>{`成功用户请求 ${compactCount(successfulRequests)} 笔`}</span>
          <span>{`输入覆盖 ${dashboardMultiplierCoverageText(coverage.inputCoveredTokens, coverage.inputTokens)}`}</span>
          <span>{`输出覆盖 ${dashboardMultiplierCoverageText(coverage.outputCoveredTokens, coverage.outputTokens)}`}</span>
        </div>
        <span className="mt-2 max-w-md">{"已保留成功请求和 Token 分母；缺少可验证的冻结派发事实，因此不绘制或推测历史倍率。"}</span>
      </div>
    )
  }
  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="mb-2 grid gap-x-4 gap-y-1 text-[10px] text-muted-foreground sm:grid-cols-2" title="成功请求的 app 和 Token 分母来自 v1 请求记录；覆盖只计关联到严格最终派发审计并具有相应冻结倍率的请求。样本数仅计有相应冻结倍率且 Token 大于零的请求。">
        <span>{`输入覆盖 ${dashboardMultiplierCoverageText(coverage.inputCoveredTokens, coverage.inputTokens)} · ${compactCount(coverage.inputSampleRequests)} 笔${inputHasSeries ? "" : " · 未推断倍率"}`}</span>
        <span>{`输出覆盖 ${dashboardMultiplierCoverageText(coverage.outputCoveredTokens, coverage.outputTokens)} · ${compactCount(coverage.outputSampleRequests)} 笔${outputHasSeries ? "" : " · 未推断倍率"}`}</span>
        <span>{`成功用户请求 ${compactCount(successfulRequests)} 笔`}</span>
        <span>{`${Math.max(1, smoothingBuckets)} 个连续有事实桶 Token 加权平滑`}</span>
      </div>
      <div className="min-h-0 flex-1">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={rows} syncId="overview-multiplier-trend" syncMethod="value" margin={{ top: 4, right: 10, left: -4, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
            <XAxis dataKey="bucket" tickLine={false} axisLine={false} minTickGap={24} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} tickFormatter={(value) => overviewBucketLabel(String(value), timeZone)} />
            <YAxis tickLine={false} axisLine={false} width={64} tick={{ fill: "var(--muted-foreground)", fontSize: 10 }} tickFormatter={(value) => formatOverviewMultiplier(Number(value))} />
            <Tooltip content={<DashboardMultiplierTrendTooltip rows={rows} timeZone={timeZone} />} cursor={{ stroke: "var(--muted-foreground)", strokeWidth: 1 }} />
            <Legend iconType="circle" iconSize={7} wrapperStyle={{ fontSize: 10 }} />
            {inputHasSeries ? <Line dataKey="chart_input_multiplier" name="输入 · 平滑" type="monotone" stroke="#2563eb" strokeWidth={2.25} dot={false} connectNulls={false} isAnimationActive={false} /> : null}
            {outputHasSeries ? <Line dataKey="chart_output_multiplier" name="输出 · 平滑" type="monotone" stroke="#0f766e" strokeWidth={2.25} dot={false} connectNulls={false} isAnimationActive={false} /> : null}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function DashboardMultiplierTrendTooltip({ active, payload, label, rows, timeZone }: { active?: boolean; payload?: Array<{ payload?: DashboardMultiplierChartRow }>; label?: string | number; rows: DashboardMultiplierChartRow[]; timeZone: string }) {
  const row = label == null ? payload?.[0]?.payload ?? null : dashboardMultiplierPointForBucket(rows, label)
  if (!active || !row) return null
  return (
    <div className="max-w-72 rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="font-medium text-foreground">{overviewBucketLabel(row.bucket, timeZone)}</div>
      <div className="mt-1 grid grid-cols-[minmax(0,1fr)_auto] gap-x-4 gap-y-1 text-muted-foreground">
        <span>{"成功用户请求"}</span><span className="text-right font-mono text-foreground">{compactCount(row.successful_requests)}</span>
        <span className="text-primary">{"输入 · 本桶 / 平滑"}</span><span className="text-right font-mono text-primary">{dashboardMultiplierSeriesTooltipText(row.input_multiplier, row.chart_input_multiplier, row.input_has_coverage)}</span>
        <span>{"输入 · 本桶覆盖"}</span><span className="text-right font-mono text-foreground">{dashboardMultiplierCoverageText(row.input_covered_tokens, row.input_tokens, row.input_has_coverage)}</span>
        <span>{"输入 · 平滑窗口"}</span><span className="text-right font-mono text-foreground">{dashboardMultiplierSeriesWindowText(row.smoothed_input_covered_tokens, row.smoothed_input_sample_requests, row.input_has_coverage)}</span>
        <span className="text-success">{"输出 · 本桶 / 平滑"}</span><span className="text-right font-mono text-success">{dashboardMultiplierSeriesTooltipText(row.output_multiplier, row.chart_output_multiplier, row.output_has_coverage)}</span>
        <span>{"输出 · 本桶覆盖"}</span><span className="text-right font-mono text-foreground">{dashboardMultiplierCoverageText(row.output_covered_tokens, row.output_tokens, row.output_has_coverage)}</span>
        <span>{"输出 · 平滑窗口"}</span><span className="text-right font-mono text-foreground">{dashboardMultiplierSeriesWindowText(row.smoothed_output_covered_tokens, row.smoothed_output_sample_requests, row.output_has_coverage)}</span>
      </div>
    </div>
  )
}

function ChartPlaceholder({ text }: { text: string }) {
  return <div className="flex h-full items-center justify-center text-xs text-muted-foreground">{text}</div>
}

function OverviewTrendTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ payload?: GatewayRequestLogTrendPoint }>; label?: string }) {
  if (!active || !payload?.length) return null
  const point = payload[0]?.payload
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="font-medium text-foreground">{label}</div>
      <div className="mt-1 grid grid-cols-2 gap-x-5 gap-y-1 text-muted-foreground">
        <span>{"请求"}</span><span className="text-right font-mono text-foreground">{compactCount(point?.requests)}</span>
        <span>{"成功 / 失败"}</span><span className="text-right font-mono text-foreground">{`${compactCount(point?.success)} / ${compactCount(point?.failed)}`}</span>
        <span>{"Token"}</span><span className="text-right font-mono text-foreground">{compactTokenCount(point?.total_tokens)}</span>
        <span>{"本地估算"}</span><span className="text-right font-mono text-warning">{money(point?.estimated_cost, { precise: true })}</span>
        <span>{"上游已返回"}</span><span className="text-right font-mono text-primary">{money(point?.reported_cost, { precise: true })}</span>
        <span>{"已核对"}</span><span className="text-right font-mono text-success">{money(point?.reconciled_cost, { precise: true })}</span>
        <span>{"费用合计"}</span><span className="text-right font-mono text-foreground">{money(point?.total_cost, { precise: true })}</span>
      </div>
    </div>
  )
}

function OverviewChannelCostTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload?: GatewayRequestLogBreakdownItem }> }) {
  if (!active || !payload?.length) return null
  const row = payload[0]?.payload
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="max-w-56 truncate font-medium text-foreground" title={row?.name}>{row?.name || "—"}</div>
      <div className="mt-1 grid grid-cols-2 gap-x-5 gap-y-1 text-muted-foreground">
        <span>{"总费用"}</span><span className="text-right font-mono text-foreground">{money(row?.total_cost, { precise: true })}</span>
        <span>{"上游已返回"}</span><span className="text-right font-mono text-primary">{money(row?.reported_cost, { precise: true })}</span>
        <span>{"已核对"}</span><span className="text-right font-mono text-success">{money(row?.reconciled_cost, { precise: true })}</span>
      </div>
    </div>
  )
}

function rangeLabel(hours: number) {
  if (hours === 24) return "近 24 小时"
  if (hours === 168) return "近 7 天"
  return "近 30 天"
}

function overviewBucketLabel(value: string, timeZone: string) {
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value.slice(5).replace("-", "/")
  return /[T ]\d{2}:\d{2}/.test(value) ? compactDateTime(value, timeZone) : value
}

function compactOverviewLabel(value: string) {
  const text = value.trim() || "未知"
  return text.length > 12 ? `${text.slice(0, 11)}…` : text
}

function formatOverviewAxisCost(value: number) {
  if (!Number.isFinite(value)) return "$0"
  if (Math.abs(value) < 0.01 && value !== 0) return `$${value.toFixed(4)}`
  return `$${value.toFixed(2)}`
}

function formatOverviewMultiplier(value: number | null | undefined) {
  if (!dashboardMultiplierIsTrustedValue(value)) return "—"
  if (value === 0) return "0x"
  return `${formatRatio(value)}x`
}

function dashboardMultiplierSeriesTooltipText(raw: number | null | undefined, smoothed: number | null | undefined, hasCoverage: boolean) {
  if (!hasCoverage || !dashboardMultiplierIsTrustedValue(raw) || !dashboardMultiplierIsTrustedValue(smoothed)) return "未推断倍率"
  return `${formatOverviewMultiplier(raw)} / ${formatOverviewMultiplier(smoothed)}`
}

function dashboardMultiplierSeriesWindowText(coveredTokens: number, sampleRequests: number, hasCoverage: boolean) {
  if (!hasCoverage) return "未推断倍率"
  return `${compactTokenCount(coveredTokens)} · ${compactCount(sampleRequests)} 笔`
}

function dashboardMultiplierCoverageText(covered: number, total: number, hasCoverage = true) {
  const safeTotal = Number.isFinite(total) ? Math.max(0, total) : 0
  const safeCovered = hasCoverage && Number.isFinite(covered) && covered >= 0 && covered <= safeTotal ? covered : 0
  const percent = dashboardMultiplierCoveragePercent(safeCovered, safeTotal)
  return `${compactTokenCount(safeCovered)} / ${compactTokenCount(safeTotal)}${percent == null ? "" : ` (${percent.toFixed(1)}%)`}`
}

function buildRiskItems(data: DashboardSummary | null | undefined) {
  if (!data) return []
  const items: Array<{ key: string; title: string; description: string; tone: Tone }> = []
  for (const channel of data.channels ?? []) {
    if (!channel.monitor_enabled) continue
    if (channel.last_error) {
      items.push({
        key: `error-${channel.id}`,
        title: `${channel.name} 登录或采集异常`,
        description: channel.last_error,
        tone: "danger",
      })
    } else if (channel.balance_threshold > 0 && channel.last_balance != null && channel.last_balance < channel.balance_threshold) {
      items.push({
        key: `balance-${channel.id}`,
        title: `${channel.name} 余额偏低`,
        description: `${channelTypeLabel(channel.type)} · 当前 ${money(channel.last_balance)}`,
        tone: "warning",
      })
    }
  }
  return items.sort((a, b) => tonePriority(b.tone) - tonePriority(a.tone))
}

function DashboardError({ message, retry, loading }: { message: string; retry: () => void; loading: boolean }) {
  return (
    <div className="flex min-h-[60vh] items-center justify-center">
      <div className="w-full max-w-lg rounded-lg border border-danger/25 bg-background p-6 text-center shadow-none">
        <span className="mx-auto flex size-10 items-center justify-center rounded-lg bg-danger/10 text-danger">
          <AlertTriangle className="size-5" />
        </span>
        <h2 className="mt-4 text-base font-semibold text-foreground">{"总览数据加载失败"}</h2>
        <p className="mt-2 break-words text-sm leading-6 text-muted-foreground">{message}</p>
        <Button onClick={retry} disabled={loading} className="mt-5 gap-2">
          <RefreshCw className={cn("size-4", loading && "animate-spin")} />
          {loading ? "重试中" : "重新加载"}
        </Button>
      </div>
    </div>
  )
}

function ConsoleMetric({
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
  tone: Tone
}) {
  return <StatusMetricCard icon={Icon} label={label} value={value} hint={hint} tone={tone} valueClassName="text-4xl" />
}

function RiskRow({ item }: { item: { title: string; description: string; tone: Tone } }) {
  return (
    <div className="flex items-start gap-3 px-5 py-3.5">
      <span className={cn("mt-1.5 size-2.5 shrink-0 rounded-full", toneDotClass(item.tone))} />
      <div className="min-w-0">
        <div className="truncate text-sm font-medium text-foreground">{item.title}</div>
        <p className="mt-1 line-clamp-2 text-xs leading-5 text-muted-foreground">{item.description}</p>
      </div>
    </div>
  )
}

function RateChangeRow({ change, channelName }: { change: RateChangeLog; channelName?: string }) {
  return (
    <div className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="min-w-0">
        <div className="truncate text-sm font-medium text-foreground">
          {channelName || `渠道 #${change.channel_id}`}
          <span className="mx-1.5 text-muted-foreground">{"·"}</span>
          {change.model_name}
        </div>
        <p className="mt-1 truncate text-xs text-muted-foreground">{relativeTime(change.changed_at)}</p>
      </div>
      <div className="shrink-0 rounded-full border border-border bg-muted/40 px-2.5 py-1 text-xs font-medium tabular-nums text-foreground">
        {formatRatio(change.old_ratio)}
        <span className="mx-1 text-muted-foreground">{"→"}</span>
        {formatRatio(change.new_ratio)}
      </div>
    </div>
  )
}

function EmptyState({
  icon: Icon,
  title,
  description,
}: {
  icon: LucideIcon
  title: string
  description: string
}) {
  return (
    <div className="flex min-h-28 flex-col items-center justify-center px-4 py-3 text-center">
      <Icon className="size-6 text-muted-foreground" />
      <div className="mt-3 text-sm font-medium text-foreground">{title}</div>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">{description}</p>
    </div>
  )
}

function tonePriority(tone: Tone) {
  if (tone === "danger") return 2
  if (tone === "warning") return 1
  return 0
}

function toneDotClass(tone: Tone) {
  switch (tone) {
    case "success":
      return "bg-success"
    case "warning":
      return "bg-warning"
    case "danger":
      return "bg-danger"
    case "brand":
      return "bg-brand"
    default:
      return "bg-muted-foreground"
  }
}
