"use client"

import { useMemo, useRef, useState } from "react"
import { ArrowDownRight, ArrowUpRight } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { apiFetch } from "@/lib/api"
import type { RateChangeLog, RateChangeLogPage } from "@/lib/api-types"
import { channelTypeLabel, formatRatio, ratioDelta, relativeTime, shortTime } from "@/lib/format"
import { useChannels, useDashboardSummary } from "@/lib/queries"
import { cn } from "@/lib/utils"

const DIALOG_SIZE = 20

export function MultiplierChanges() {
  const summary = useDashboardSummary()
  const channels = useChannels()
  const [detailOpen, setDetailOpen] = useState(false)
  const [page, setPage] = useState(0)
  const [feed, setFeed] = useState<RateChangeLog[]>([])
  const [feedMeta, setFeedMeta] = useState<{ total: number; pages: number }>({
    total: 0,
    pages: 1,
  })
  const [feedLoading, setFeedLoading] = useState(false)
  const [feedError, setFeedError] = useState<string | null>(null)
  const feedLoadingGenerationRef = useRef<number | null>(null)
  const feedGenerationRef = useRef(0)

  const channelMap = useMemo(() => {
    const m = new Map<number, { name: string; type: string }>()
    for (const c of channels.data ?? []) m.set(c.id, { name: c.name, type: c.type })
    return m
  }, [channels.data])

  const items = summary.data?.recent_rate_changes ?? []
  const latest = items[0] ?? null
  const upCount = items.filter((item) => ratioDelta(effectiveOldRatio(item), item.new_effective_ratio).direction === "up").length
  const downCount = items.filter((item) => ratioDelta(effectiveOldRatio(item), item.new_effective_ratio).direction === "down").length

  function loadNextPage() {
    if (feedLoadingGenerationRef.current === feedGenerationRef.current || page >= feedMeta.pages) return
    void loadPage(page + 1, false, feedGenerationRef.current)
  }

  async function loadPage(targetPage: number, replace: boolean, generation: number) {
    if (feedLoadingGenerationRef.current === generation) return
    feedLoadingGenerationRef.current = generation
    setFeedLoading(true)
    setFeedError(null)
    try {
      const res = await apiFetch<RateChangeLogPage>(`/rate-changes?page=${targetPage}&page_size=${DIALOG_SIZE}`)
      if (generation !== feedGenerationRef.current) return
      const next = Array.isArray(res?.items) ? res.items : []
      setFeed((current) => replace ? next : appendUniqueByID(current, next))
      setFeedMeta({ total: res?.total ?? 0, pages: Math.max(1, res?.pages ?? 1) })
      setPage(targetPage)
    } catch (error) {
      if (generation === feedGenerationRef.current) setFeedError((error as Error).message || "加载倍率变动记录失败")
    } finally {
      if (feedLoadingGenerationRef.current === generation) {
        feedLoadingGenerationRef.current = null
        if (generation === feedGenerationRef.current) setFeedLoading(false)
      }
    }
  }

  function openDetail() {
    const generation = feedGenerationRef.current + 1
    feedGenerationRef.current = generation
    setFeed([])
    setFeedMeta({ total: items.length, pages: 1 })
    setFeedError(null)
    setPage(0)
    setDetailOpen(true)
    void loadPage(1, true, generation)
  }

  function closeDetail() {
    feedGenerationRef.current += 1
    setFeedLoading(false)
    setDetailOpen(false)
  }

  return (
    <>
      <section className="flex min-h-0 flex-col overflow-hidden rounded-lg border border-border bg-background">
        <div className="border-b border-border bg-muted/20 px-4 py-4 sm:px-6">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
            <div className="min-w-0" title="监控上游分组倍率变化，优先发现涨价、降价和策略调整">
              <h2 className="text-lg font-semibold tracking-tight text-foreground">{"最近倍率变动"}</h2>
            </div>
            <Button size="sm" variant="ghost" className="h-8 shrink-0 px-2 text-xs" onClick={openDetail}>
              {"查看更多"}
            </Button>
          </div>
          <div className="flex flex-wrap items-center gap-x-5 gap-y-2 border-t border-border pt-3 text-xs">
            <span className="inline-flex items-center gap-1.5">
              <span className="text-muted-foreground">{"最近记录"}</span>
              <span className="font-semibold text-foreground" title="来自总览快照的最近变动">
                {summary.loading ? "加载中" : `${items.length}`}
              </span>
            </span>
            <span className="inline-flex min-w-0 items-center gap-1.5">
              <span className="shrink-0 text-muted-foreground">{"最近变动"}</span>
              <span className="truncate font-semibold text-foreground" title={summary.loading ? "正在读取倍率记录" : latest ? `${latest.model_name} · ${formatRatio(effectiveOldRatio(latest))} → ${formatRatio(latest.new_effective_ratio)}` : "暂无倍率变动"}>
                {summary.loading ? "加载中" : latest ? relativeTime(latest.changed_at) : "—"}
              </span>
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="text-muted-foreground">{"涨价 / 降价"}</span>
              <span className={cn(
                "font-semibold",
                summary.loading ? "text-foreground" : upCount > 0 ? "text-danger" : downCount > 0 ? "text-success" : "text-foreground",
              )} title={summary.loading ? "正在读取倍率记录" : upCount > 0 ? "近期存在涨价，需要关注成本" : downCount > 0 ? "近期以降价为主" : "近期价格稳定"}>
                {summary.loading ? "加载中" : `${upCount} / ${downCount}`}
              </span>
            </span>
          </div>
        </div>
        <div className="min-h-0 flex-1 px-0">
          {summary.error ? (
            <p className="px-6 py-6 text-xs text-danger">{`倍率事件加载失败：${summary.error}`}</p>
          ) : summary.loading ? (
            <p className="px-6 py-6 text-xs text-muted-foreground">{"加载中…"}</p>
          ) : items.length === 0 ? (
            <p className="px-6 py-6 text-xs text-muted-foreground">{"暂无倍率变动记录"}</p>
          ) : (
            <div>
              <ul className="divide-y divide-border">
                {items.map((item) => (
                  <MultiplierChangeRow key={item.id} item={item} channelMap={channelMap} />
                ))}
              </ul>
            </div>
          )}
        </div>
      </section>

      <Dialog open={detailOpen} onOpenChange={(open) => { if (open) setDetailOpen(true); else closeDetail() }}>
        <DialogContent className="max-h-[85vh] overflow-hidden sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>{"最近倍率变动"}</DialogTitle>
          </DialogHeader>
          <div className="max-h-[60vh] overflow-y-auto rounded-md border border-border">
            <ul className="divide-y divide-border">
              {feed.map((item) => (
                <MultiplierChangeRow key={item.id} item={item} channelMap={channelMap} compact />
              ))}
              {feedLoading && feed.length === 0 ? (
                <li className="px-4 py-6 text-sm text-muted-foreground">{"加载中…"}</li>
              ) : null}
              {!feedLoading && feed.length === 0 ? (
                <li className="px-4 py-6 text-sm text-muted-foreground">{"暂无倍率变动记录"}</li>
              ) : null}
              {feedLoading && feed.length > 0 ? (
                <li className="px-4 py-3 text-xs text-muted-foreground">{"加载更多中…"}</li>
              ) : null}
            </ul>
          </div>
          {feedError ? <p className="text-sm text-danger">{feedError}</p> : null}
          <div className="flex items-center justify-between">
            <span className="text-xs text-muted-foreground">
              {feedMeta.total > 0 ? `已加载 ${feed.length} / ${feedMeta.total} 条` : ""}
            </span>
            <Button
              variant="outline"
              size="sm"
              disabled={feedLoading || page >= feedMeta.pages}
              onClick={loadNextPage}
            >
              {feedLoading && page > 1 ? "加载中…" : "查看更多"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </>
  )
}

function MultiplierChangeRow({
  item,
  channelMap,
  compact = false,
}: {
  item: RateChangeLog
  channelMap: Map<number, { name: string; type: string }>
  compact?: boolean
}) {
  const ch = channelMap.get(item.channel_id)
  const delta = ratioDelta(effectiveOldRatio(item), item.new_effective_ratio)
  const isUp = delta.direction === "up"
  const chType = ch?.type ?? ""

  return (
    <li className={cn("flex items-start gap-2.5 sm:gap-3", compact ? "px-4 py-3" : "px-4 py-3.5 sm:px-6")}>
      <div className="flex flex-col items-center gap-0.5 pt-1">
        <span className={cn("size-2 rounded-full", isUp ? "bg-danger" : "bg-success")} />
      </div>
      <div className="shrink-0 text-[11px] leading-relaxed text-muted-foreground sm:text-xs">
        <p>{relativeTime(item.changed_at)}</p>
        <p>{shortTime(item.changed_at)}</p>
      </div>

      <div className="min-w-0 flex-1">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <span className="truncate text-sm font-semibold text-foreground">{item.model_name}</span>
          <span
            className={cn(
              "inline-flex items-center rounded-md px-1.5 py-0.5 text-[10px] font-medium ring-1 ring-inset",
              chType === "newapi"
                ? "bg-brand/10 text-brand ring-brand/20"
                : "bg-foreground/5 text-foreground ring-border",
            )}
          >
            {ch?.name ?? `#${item.channel_id}`}
            {chType ? <span className="ml-1 opacity-60">{channelTypeLabel(chType)}</span> : null}
          </span>
        </div>
        <div className="mt-1.5 flex items-center text-xs">
          <div>
            <span className="text-muted-foreground">{"倍率"}</span>
            <p className="mt-0.5 tabular-nums">
              <span className="text-muted-foreground">
                {formatRatio(effectiveOldRatio(item))}
              </span>
              <span className="mx-1 text-muted-foreground">{"→"}</span>
              <span className={cn("font-medium", isUp ? "text-danger" : "text-success")}>
                {formatRatio(item.new_effective_ratio)}
              </span>
            </p>
          </div>
        </div>
      </div>

      <div className="shrink-0 pt-0.5">
        <span
          className={cn(
            "inline-flex items-center gap-0.5 rounded-md px-2 py-1 text-xs font-semibold",
            isUp ? "bg-danger/10 text-danger" : "bg-success/10 text-success",
          )}
        >
          {isUp ? <ArrowUpRight className="size-3" /> : <ArrowDownRight className="size-3" />}
          {delta.pct}
        </span>
      </div>
    </li>
  )
}

function appendUniqueByID<T extends { id: number }>(current: T[], next: T[]) {
  const seen = new Set(current.map((item) => item.id))
  return [...current, ...next.filter((item) => !seen.has(item.id))]
}

function effectiveOldRatio(item: RateChangeLog) {
  return item.old_effective_ratio ?? (item.old_ratio == null ? null : item.old_ratio * item.funding_factor_before)
}
