"use client"

import { ArrowUpRight, DollarSign, MessageSquare, Wallet } from "lucide-react"
import { cn } from "@/lib/utils"
import { useDashboardSummary, useRateChanges } from "@/lib/queries"
import { isTodayInDisplayTimeZone, money } from "@/lib/format"
import type { LucideIcon } from "lucide-react"
import type { ReactNode } from "react"

interface Kpi {
  label: string
  description: string
  value: ReactNode
  icon: LucideIcon
  tone: "brand" | "success" | "warning" | "danger" | "muted"
  badge: string
  footer: ReactNode
}

function countTodayChanges(changes: { changed_at: string }[]) {
  return changes.filter((change) => isTodayInDisplayTimeZone(change.changed_at)).length
}

export function KpiRow() {
  const summary = useDashboardSummary()
  const recentChanges = useRateChanges(1, 100)

  const data = summary.data
  const loaded = Boolean(data)
  const total = data?.total_channels ?? 0
  const active = data?.active_channels ?? 0
  const failed = data?.failed_channels ?? 0
  const totalBalance = data?.total_balance ?? 0
  const todayTotalCost = data?.today_total_cost ?? 0
  const totalCost = data?.total_cost ?? 0
  const lowest = data?.lowest_balance ?? null

  const todayChangeCount = countTodayChanges(recentChanges.data?.items ?? [])

  const kpis: Kpi[] = [
    {
      label: "总余额",
      description: "上游账户资金池",
      value: money(totalBalance),
      icon: DollarSign,
      tone: loaded ? "brand" : "muted",
      badge: loaded ? "余额" : "加载中",
      footer: lowest ? (
        <span className="text-muted-foreground">
          {"最低："}
          <span className="font-medium text-foreground">{lowest.name}</span>
          {" "}
          <span className="text-warning">{money(lowest.balance)}</span>
        </span>
      ) : (
        <span className="text-muted-foreground">{"—"}</span>
      ),
    },
    {
      label: "今日总消费",
      description: "今日实际扣费",
      value: money(todayTotalCost),
      icon: Wallet,
      tone: todayTotalCost > 0 ? "warning" : "muted",
      badge: "今日",
      footer: (
        <span className="text-muted-foreground">
          {todayTotalCost > 0 ? "按实际扣费统计" : "今日暂无消费"}
        </span>
      ),
    },
    {
      label: "累计消费",
      description: "历史费用合计",
      value: money(totalCost),
      icon: DollarSign,
      tone: "brand",
      badge: "全量",
      footer: (
        <span className="text-muted-foreground">
          {totalCost > 0 ? "全渠道累计实际扣费" : "暂无累计消费"}
        </span>
      ),
    },
    {
      label: "渠道状态",
      description: "健康渠道覆盖",
      value: (
        <span>
          {active}
          <span className="mx-1 text-lg font-normal text-muted-foreground">{"/"}</span>
          <span className="text-lg font-normal text-muted-foreground">{total}</span>
        </span>
      ),
      icon: MessageSquare,
      tone: failed > 0 ? "danger" : active > 0 ? "success" : "muted",
      badge: failed > 0 ? "需处理" : active > 0 ? "健康" : total > 0 ? "未采集" : loaded ? "无渠道" : "加载中",
      footer: (
        <span className="text-muted-foreground">
          <span className="text-success font-medium">{active} 健康</span>
          {failed > 0 ? (
            <>
              {" · "}
              <span className="text-danger font-medium">{failed} 失败</span>
            </>
          ) : null}
        </span>
      ),
    },
    {
      label: "今日倍率变动",
      description: "价格策略波动",
      value: (
        <span className={cn(todayChangeCount > 0 ? "text-danger" : "text-foreground")}>
          {todayChangeCount}
        </span>
      ),
      icon: ArrowUpRight,
      tone: todayChangeCount > 0 ? "danger" : "muted",
      badge: "今日",
      footer: (
        <span className="text-muted-foreground">
          {todayChangeCount > 0 ? `检测到 ${todayChangeCount} 次变动` : "今日无变动"}
        </span>
      ),
    },
  ]

  return (
    <section className="grid grid-cols-1 gap-px overflow-hidden rounded-lg border border-border bg-border sm:grid-cols-2 xl:grid-cols-5">
        {kpis.map((k) => (
          <div
            key={k.label}
            className="group min-w-0 overflow-hidden bg-background transition-colors hover:bg-muted/20"
          >
            <div className={cn("h-1", toneBarClass(k.tone))} />
            <div className="flex min-h-28 flex-col">
              <div className="flex items-start justify-between gap-3 p-3 pb-2.5">
                <div className="min-w-0">
                  <div className="flex min-w-0 items-center gap-2">
                    <span className="truncate text-xs font-medium text-muted-foreground">{k.label}</span>
                    <span className={cn("shrink-0 rounded-full px-2 py-0.5 text-[10px] font-medium", toneBadgeClass(k.tone))}>
                      {k.badge}
                    </span>
                  </div>
                  <p className="mt-1 truncate text-center text-[11px] leading-4 text-muted-foreground">
                    {k.description}
                  </p>
                  <p className="mt-1.5 truncate text-center text-2xl font-semibold tracking-tight text-foreground">
                    {k.value}
                  </p>
                </div>
                <span className={cn("flex size-8 shrink-0 items-center justify-center rounded-lg ring-1 ring-inset transition-transform group-hover:scale-105", toneIconClass(k.tone))}>
                  <k.icon className="size-4" />
                </span>
              </div>
              <div className="mt-auto border-t border-border bg-muted/20 px-3 py-2.5">
                <p className="min-w-0 text-center text-xs leading-4">{k.footer}</p>
              </div>
            </div>
          </div>
        ))}
    </section>
  )
}

function toneIconClass(tone: Kpi["tone"]) {
  switch (tone) {
    case "brand":
      return "bg-brand/10 text-brand ring-brand/15"
    case "success":
      return "bg-success/10 text-success ring-success/15"
    case "warning":
      return "bg-warning/10 text-warning ring-warning/15"
    case "danger":
      return "bg-danger/10 text-danger ring-danger/15"
    default:
      return "bg-muted text-muted-foreground ring-border"
  }
}

function toneBadgeClass(tone: Kpi["tone"]) {
  switch (tone) {
    case "brand":
      return "bg-brand/10 text-brand"
    case "success":
      return "bg-success/10 text-success"
    case "warning":
      return "bg-warning/10 text-warning"
    case "danger":
      return "bg-danger/10 text-danger"
    default:
      return "bg-muted text-muted-foreground"
  }
}

function toneBarClass(tone: Kpi["tone"]) {
  switch (tone) {
    case "brand":
      return "bg-brand/70"
    case "success":
      return "bg-success/70"
    case "warning":
      return "bg-warning/70"
    case "danger":
      return "bg-danger/70"
    default:
      return "bg-muted"
  }
}
