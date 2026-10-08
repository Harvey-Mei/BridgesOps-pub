import type { ComponentType, ReactNode } from "react"
import { RefreshCw } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { PageHeader } from "@/components/ui/page-header"
import { cn } from "@/lib/utils"

export type RadarControlItem = {
  key: string
  label: string
  color: string
}

export type RadarWindowOption = {
  value: number | "all"
  label: string
}

export function RadarToolbar({ icon: Icon, title, source, freshness, stale, loading = false, error = false, refreshing, onRefresh, embedded = false }: {
  icon: ComponentType<{ className?: string }>
  title: string
  source: string
  freshness: string
  stale?: boolean
  loading?: boolean
  error?: boolean
  refreshing: boolean
  onRefresh: () => void
  embedded?: boolean
}) {
  if (embedded) {
    return <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-card px-4 py-3 shadow-sm">
      <div className="flex min-w-0 items-center gap-2 text-sm text-muted-foreground">
        <Icon className="size-4 shrink-0 text-brand" />
        <span className="truncate">{source}</span>
        <span className="hidden sm:inline">{"·"}</span>
        <span className="hidden truncate sm:inline">{freshness}</span>
      </div>
      <div className="flex items-center gap-2">
        <Badge variant={stale || loading || error ? "outline" : "secondary"}>{loading ? "读取中" : error ? "读取失败" : stale ? "使用缓存" : "数据正常"}</Badge>
        <Button type="button" variant="outline" size="sm" onClick={onRefresh} disabled={refreshing || loading}>
          <RefreshCw className={cn("size-3.5", refreshing && "animate-spin")} />
          {refreshing ? "刷新中" : "立即刷新"}
        </Button>
      </div>
    </div>
  }
  return <PageHeader icon={Icon} title={title} description={`${source} · ${freshness}`} actions={<>
      <Badge variant={stale || loading || error ? "outline" : "secondary"}>{loading ? "读取中" : error ? "读取失败" : stale ? "使用缓存" : "数据正常"}</Badge>
      <Button type="button" variant="outline" size="sm" onClick={onRefresh} disabled={refreshing || loading}>
        <RefreshCw className={cn("size-3.5", refreshing && "animate-spin")} />
        {refreshing ? "刷新中" : "立即刷新"}
      </Button>
    </>} />
}

export function RadarChartControls({ items, selectedKeys, onToggle, onAll, windowValue, windows, onWindowChange, rowCount, historyCount }: {
  items: RadarControlItem[]
  selectedKeys: string[]
  onToggle: (key: string) => void
  onAll: () => void
  windowValue: number | "all"
  windows: RadarWindowOption[]
  onWindowChange: (value: number | "all") => void
  rowCount: number
  historyCount?: number
}) {
  const showingAll = selectedKeys.length === 0
  return (
    <section className="overflow-hidden rounded-lg border border-border bg-background">
      <div className="flex min-w-0 flex-wrap items-start gap-3 px-5 py-4">
        <span className="shrink-0 text-sm font-medium text-muted-foreground" title={`当前 ${rowCount} 个时间点${historyCount == null ? "" : ` · 本地历史 ${historyCount} 个点`}`}>{"模型"}</span>
        <div className="flex min-w-0 flex-1 flex-wrap gap-2 overflow-x-auto pb-0.5 [scrollbar-width:thin]" role="group" aria-label="图表模型">
          {items.map((item) => {
            const active = showingAll || selectedKeys.includes(item.key)
            return <button key={item.key} type="button" title={item.label} aria-pressed={active} onClick={() => onToggle(item.key)} className={cn("inline-flex h-8 shrink-0 items-center gap-1.5 whitespace-nowrap rounded-md border px-3 text-sm transition-colors", active ? "bg-muted/60 font-medium text-foreground" : "border-border bg-background text-muted-foreground hover:bg-muted")} style={active ? { borderColor: item.color } : undefined}><i className="size-2 shrink-0 rounded-full" style={{ backgroundColor: item.color }} aria-hidden="true" />{item.label}</button>
          })}
        </div>
        <Button type="button" variant={showingAll ? "default" : "outline"} size="sm" className="h-8 shrink-0 px-2.5 text-xs" onClick={onAll}>{showingAll ? "全部" : "清除选择"}</Button>
        <div className="flex shrink-0 items-center gap-1.5 border-l border-border pl-2" role="group" aria-label="图表时间窗口">
          <span className="hidden text-[11px] text-muted-foreground sm:inline">{"窗口"}</span>
          {windows.map((option) => <Button key={String(option.value)} type="button" aria-pressed={windowValue === option.value} variant={windowValue === option.value ? "secondary" : "ghost"} size="sm" className="h-8 px-2 text-[11px]" onClick={() => onWindowChange(option.value)}>{option.label}</Button>)}
        </div>
      </div>

    </section>
  )
}

export function RadarModelTableShell({ title, countLabel, children }: { title: string; countLabel: string; children: ReactNode }) {
  return <section className="overflow-hidden rounded-lg border border-border bg-background"><div className="flex items-center justify-between gap-3 border-b border-border bg-muted/20 px-5 py-3"><h2 className="text-lg font-semibold tracking-tight text-foreground">{title}</h2><span className="text-xs text-muted-foreground">{countLabel}</span></div>{children}</section>
}

export function RadarSourceStatus({ children }: { children: ReactNode }) {
  return <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border pt-3 text-xs text-muted-foreground">{children}</div>
}

export function RadarCompactLegend({ items }: { items: RadarControlItem[] }) {
  return <div className="mt-2 flex max-w-full gap-3 overflow-x-auto pb-1">{items.map((item) => <span key={item.key} className="inline-flex shrink-0 items-center gap-1 text-[10px] text-muted-foreground"><i className="size-1.5 rounded-full" style={{ backgroundColor: item.color }} aria-hidden="true" /><span title={item.label}>{item.label}</span></span>)}</div>
}
