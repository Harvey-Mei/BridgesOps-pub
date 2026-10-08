"use client"

import { Area, AreaChart, Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis, CartesianGrid } from "recharts"
import { useIsMobile } from "@/hooks/use-mobile"
import { useBalanceTrend, useCostTrend, useDashboardSummary } from "@/lib/queries"
import { currentDayCost, money } from "@/lib/format"
import { cn } from "@/lib/utils"

function formatY(n: number) {
  if (n === 0) return "$0"
  if (n >= 1000) return `$${(n / 1000).toFixed(n >= 10000 ? 0 : 1)}K`
  if (n >= 100) return `$${n.toFixed(0)}`
  return `$${n.toFixed(n >= 10 ? 1 : 2)}`
}

/**
 * niceCeil 把最大值向上取整到一个"好看的"刻度，避免曲线贴顶。
 * 例如 47 → 50；478 → 500；12,300 → 15,000。
 */
function niceCeil(n: number): number {
  if (!Number.isFinite(n) || n <= 0) return 10
  const padded = n * 1.15
  const mag = Math.pow(10, Math.floor(Math.log10(padded)))
  const norm = padded / mag
  const step = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10
  return step * mag
}

function formatDay(iso: string) {
  const businessDay = /^(\d{4})-(\d{2})-(\d{2})(?:T|$)/.exec(iso)
  if (businessDay) return `${Number(businessDay[2])}月${Number(businessDay[3])}日`
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleDateString("zh-CN", { month: "numeric", day: "numeric" })
}

interface ChartPoint {
  day: string
  balance: number | null
  cost: number | null
}

interface TooltipPayloadItem {
  dataKey?: string
  value: number
}

function ChartTooltip({ active, payload, label }: { active?: boolean; payload?: TooltipPayloadItem[]; label?: string }) {
  if (!active || !payload?.length) return null
  const balance = payload.find((p) => p.dataKey === "balance")?.value
  const cost = payload.find((p) => p.dataKey === "cost")?.value
  return (
    <div className="rounded-lg border border-border bg-popover px-3 py-2 shadow-md">
      <p className="text-xs text-muted-foreground">{label}</p>
      {balance != null ? (
        <p className="text-sm font-semibold text-brand">
          {"余额："}{money(balance)}
        </p>
      ) : null}
      {cost != null ? (
        <p className="mt-1 text-sm font-semibold text-warning">
          {"消费："}{money(cost)}
        </p>
      ) : null}
    </div>
  )
}

export function BalanceOverview() {
  const isMobile = useIsMobile()
  const trend = useBalanceTrend(7)
  const costTrend = useCostTrend(7)
  const summary = useDashboardSummary()

  const channels = summary.data?.channels ?? []
  const trendMap = new Map<string, ChartPoint>()

  for (const point of trend.data ?? []) {
    const key = point.day
    const existing = trendMap.get(key)
    trendMap.set(key, {
      day: formatDay(point.day),
      balance: point.balance,
      cost: existing?.cost ?? null,
    })
  }
  for (const point of costTrend.data ?? []) {
    const key = point.day
    const existing = trendMap.get(key)
    trendMap.set(key, {
      day: existing?.day ?? formatDay(point.day),
      balance: existing?.balance ?? null,
      cost: point.cost,
    })
  }

  const data = Array.from(trendMap.entries())
    .sort(([a], [b]) => new Date(a).getTime() - new Date(b).getTime())
    .map(([, value]) => value)
  const balanceValues = data.map((d) => d.balance ?? 0)
  const costValues = data.map((d) => d.cost ?? 0)
  const latestBalance = [...data].reverse().find((d) => d.balance != null)?.balance ?? null
  const sevenDayCost = data.reduce((sum, point) => sum + (point.cost ?? 0), 0)
  const sampledDays = data.filter((point) => point.balance != null || point.cost != null).length
  const yMax = data.length > 0 ? niceCeil(Math.max(...balanceValues)) : 10
  const costMax = data.length > 0 ? niceCeil(Math.max(...costValues)) : 10
  const isLoading = trend.loading || costTrend.loading
  const trendError = trend.error || costTrend.error
  const chartMargin = isMobile
    ? { top: 6, right: 4, left: -18, bottom: 0 }
    : { top: 8, right: 12, left: 0, bottom: 0 }
  const dot = isMobile ? false : { r: 4, fill: "var(--background)", strokeWidth: 2 }
  const activeDot = isMobile ? { r: 4, strokeWidth: 0 } : { r: 5, strokeWidth: 0 }

  return (
    <section className="flex min-h-0 flex-col overflow-hidden rounded-lg border border-border bg-background lg:min-h-88">
      <div className="border-b border-border bg-muted/20 px-4 pb-4 sm:px-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0" title="按最近采样展示余额、消费趋势和各上游渠道的资金状态">
            <h2 className="text-lg font-semibold tracking-tight text-foreground">{"余额概览"}</h2>
          </div>
          <span className="inline-flex h-8 shrink-0 items-center rounded-md border border-border bg-background/70 px-2.5 text-xs font-medium text-muted-foreground">
            {"最近 7 天"}
          </span>
        </div>
      </div>
      <div className="flex min-h-0 flex-1 flex-col px-4 pt-4 sm:px-6">
        <div className="mb-3 flex flex-wrap items-center gap-x-5 gap-y-2 border-b border-border pb-3 text-xs">
          <span className="inline-flex items-center gap-1.5">
            <span className="text-muted-foreground">{"最新余额"}</span>
            <span className="font-semibold text-foreground" title="来自最近一次余额采样">
              {isLoading ? "加载中" : latestBalance == null ? "—" : money(latestBalance)}
            </span>
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="text-muted-foreground">{"7 日消费"}</span>
            <span className="font-semibold text-foreground" title="按每日消费快照汇总">
              {isLoading ? "加载中" : money(sevenDayCost)}
            </span>
          </span>
          <span className="inline-flex items-center gap-1.5">
            <span className="text-muted-foreground">{"采样天数"}</span>
            <span className="font-semibold text-foreground" title="用于绘制当前趋势">
              {isLoading ? "加载中" : `${sampledDays} 天`}
            </span>
          </span>
        </div>
        <div className="grid min-h-0 w-full grid-cols-2 gap-4">
          {isLoading ? (
            <div className="col-span-2 flex h-72 items-center justify-center text-xs text-muted-foreground">{"加载中…"}</div>
          ) : trendError ? (
            <div className="col-span-2 flex h-72 items-center justify-center px-6 text-center text-xs text-danger">{`趋势数据加载失败：${trendError}`}</div>
          ) : data.length === 0 ? (
            <div className="col-span-2 flex h-72 items-center justify-center text-xs text-muted-foreground">
              {"暂无趋势采样，等待下次扫描或手动刷新"}
            </div>
          ) : (
            <>
              <div className="min-w-0">
                <div className="mb-2 flex items-center gap-1.5 text-xs font-medium text-foreground">
                  <span className="size-2 rounded-full bg-chart-1" />
                  <span>{"余额"}</span>
                </div>
                <div className="h-72 min-h-0 w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data} syncId="balance-cost-trend" syncMethod="index" margin={chartMargin}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                      <XAxis dataKey="day" tickLine={false} axisLine={false} interval={isMobile ? 1 : 0} minTickGap={isMobile ? 8 : 5} tick={{ fill: "var(--muted-foreground)", fontSize: isMobile ? 10 : 11 }} dy={isMobile ? 6 : 8} />
                      <YAxis tickLine={false} axisLine={false} width={isMobile ? 40 : 48} tick={{ fill: "var(--muted-foreground)", fontSize: isMobile ? 10 : 11 }} tickFormatter={formatY} domain={[0, yMax]} />
                      <Tooltip content={<ChartTooltip />} cursor={{ stroke: "var(--border)", strokeDasharray: "4 4" }} />
                      <Area type="monotone" dataKey="balance" stroke="var(--chart-1)" fill="var(--chart-1)" fillOpacity={0.14} strokeWidth={2} dot={dot} activeDot={{ ...activeDot, fill: "var(--chart-1)" }} isAnimationActive={false} />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </div>
              <div className="min-w-0">
                <div className="mb-2 flex items-center gap-1.5 text-xs font-medium text-foreground">
                  <span className="size-2 rounded-full bg-warning" />
                  <span>{"每日消费"}</span>
                </div>
                <div className="h-72 min-h-0 w-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={data} syncId="balance-cost-trend" syncMethod="index" margin={chartMargin}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                      <XAxis dataKey="day" tickLine={false} axisLine={false} interval={isMobile ? 1 : 0} minTickGap={isMobile ? 8 : 5} tick={{ fill: "var(--muted-foreground)", fontSize: isMobile ? 10 : 11 }} dy={isMobile ? 6 : 8} />
                      <YAxis tickLine={false} axisLine={false} width={isMobile ? 40 : 48} tick={{ fill: "var(--muted-foreground)", fontSize: isMobile ? 10 : 11 }} tickFormatter={formatY} domain={[0, costMax]} />
                      <Tooltip content={<ChartTooltip />} cursor={{ fill: "var(--muted)" }} />
                      <Bar dataKey="cost" fill="var(--warning)" radius={[3, 3, 0, 0]} maxBarSize={36} isAnimationActive={false} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </>
          )}
        </div>

        {/* per-channel chips */}
        {channels.length > 0 ? (
          <div className="mt-3 flex max-h-24 shrink-0 flex-wrap items-center gap-x-5 gap-y-2 overflow-y-auto border-t border-border pt-3">
            {channels.map((c) => {
              const isFailed = !!c.last_error
              const isUnknown = c.last_balance == null
              return (
                <span key={c.id} className="inline-flex max-w-full items-center gap-1.5 text-xs">
                  <span
                    className={cn(
                      "size-2 rounded-full",
                      isFailed ? "bg-danger" : isUnknown ? "bg-muted-foreground/40" : "bg-success",
                    )}
                  />
                  <span className="max-w-32 truncate font-medium text-foreground sm:max-w-none">{c.name}</span>
                  <span className="min-w-0 tabular-nums text-muted-foreground">
                    {money(c.last_balance)}
                    {" · 今日 "}
                    {money(currentDayCost(c.today_cost, c.today_cost_at, c.today_cost_is_today))}
                  </span>
                </span>
              )
            })}
          </div>
        ) : null}
      </div>
    </section>
  )
}
