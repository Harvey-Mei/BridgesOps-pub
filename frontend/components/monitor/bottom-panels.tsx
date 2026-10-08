"use client"

import { Fragment, useRef, useState } from "react"
import { toast } from "sonner"
import {
  AlertTriangle,
  ArrowUpRight,
  Bell,
  CheckCircle2,
  Clock3,
  RefreshCw,
  KeyRound,
  Pencil,
  Plus,
  Send,
  ShieldX,
  TestTube2,
  Trash2,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { useConfirm } from "@/components/ui/confirm-dialog"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  useAnnouncements,
  useCaptchaConfigs,
  useDashboardSummary,
  useNotificationChannels,
  useNotificationLogs,
} from "@/lib/queries"
import { apiFetch } from "@/lib/api"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { channelTypeLabel, dateTime, decimal, money, relativeTime } from "@/lib/format"
import { cn } from "@/lib/utils"
import { CaptchaFormDialog } from "@/components/monitor/captcha-form-dialog"
import { NotificationFormDialog } from "@/components/monitor/notification-form-dialog"
import type { LucideIcon } from "lucide-react"
import type {
  CaptchaConfig,
  NotificationChannel,
  NotificationEvent,
  NotificationChannelType,
  NotificationLog,
  NotificationLogPage,
  UpstreamAnnouncement,
} from "@/lib/api-types"

const eventMeta: Record<NotificationEvent, { icon: LucideIcon; cls: string }> = {
  balance_low: { icon: AlertTriangle, cls: "text-warning" },
  login_failed: { icon: ShieldX, cls: "text-danger" },
  captcha_failed: { icon: KeyRound, cls: "text-danger" },
  rate_changed: { icon: ArrowUpRight, cls: "text-brand" },
  rate_structure_changed: { icon: ArrowUpRight, cls: "text-brand" },
  rate_added: { icon: Plus, cls: "text-brand" },
  rate_removed: { icon: Trash2, cls: "text-warning" },
  announcement: { icon: Bell, cls: "text-brand" },
  monitor_failed: { icon: ShieldX, cls: "text-danger" },
  model_check_alert: { icon: AlertTriangle, cls: "text-danger" },
  model_check_recovered: { icon: CheckCircle2, cls: "text-success" },
  subscription_daily_remaining_low: { icon: AlertTriangle, cls: "text-warning" },
  subscription_weekly_remaining_low: { icon: AlertTriangle, cls: "text-warning" },
  subscription_monthly_remaining_low: { icon: AlertTriangle, cls: "text-warning" },
  subscription_expiring: { icon: Clock3, cls: "text-warning" },
}

const FEED_PREVIEW_SIZE = 10
const FEED_DIALOG_SIZE = 20

export function AlertFeed() {
  const [detailOpen, setDetailOpen] = useState(false)
  const [page, setPage] = useState(0)
  const [feed, setFeed] = useState<NotificationLog[]>([])
  const [feedMeta, setFeedMeta] = useState<{ total: number; pages: number }>({
    total: 0,
    pages: 1,
  })
  const [feedLoading, setFeedLoading] = useState(false)
  const [feedError, setFeedError] = useState<string | null>(null)
  const feedLoadingGenerationRef = useRef<number | null>(null)
  const feedGenerationRef = useRef(0)
  const preview = useNotificationLogs(1, FEED_PREVIEW_SIZE)
  const items = preview.data?.items ?? []
  const latest = items[0] ?? null
  const failedPreviewCount = items.filter((item) => !item.success).length

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
      const res = await apiFetch<NotificationLogPage>(`/notifications/logs?page=${targetPage}&page_size=${FEED_DIALOG_SIZE}`)
      if (generation !== feedGenerationRef.current) return
      const next = Array.isArray(res?.items) ? res.items : []
      setFeed((current) => replace ? next : appendUniqueByID(current, next))
      setFeedMeta({ total: res?.total ?? 0, pages: Math.max(1, res?.pages ?? 1) })
      setPage(targetPage)
    } catch (error) {
      if (generation === feedGenerationRef.current) setFeedError((error as Error).message || "加载告警记录失败")
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
    setFeedMeta({ total: 0, pages: 1 })
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
            <div className="min-w-0" title="聚合通知发送记录，快速判断告警是否正常送达">
              <h2 className="text-lg font-semibold tracking-tight text-foreground">{"告警动态"}</h2>
            </div>
            <Button size="sm" variant="ghost" className="h-8 shrink-0 px-2 text-xs" onClick={openDetail}>
              {"查看更多"}
            </Button>
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-x-5 gap-y-1">
            <FeedMetric
              label="记录总数"
              value={preview.loading ? "加载中" : `${preview.data?.total ?? 0}`}
              hint={preview.loading ? "正在读取告警记录" : "当前保留的发送审计"}
            />
            <FeedMetric
              label="最近动态"
              value={preview.loading ? "加载中" : latest ? relativeTime(latest.sent_at) : "—"}
              hint={preview.loading ? "正在读取告警记录" : latest ? truncateText(latest.subject, 16) : "暂无告警记录"}
            />
            <FeedMetric
              label="近 10 条失败"
              value={preview.loading ? "加载中" : `${failedPreviewCount}`}
              hint={preview.loading ? "正在读取告警记录" : failedPreviewCount > 0 ? "需要检查通知渠道" : "近期发送稳定"}
              tone={preview.loading ? "muted" : failedPreviewCount > 0 ? "danger" : "success"}
            />
          </div>
        </div>
        <div className="min-h-0 flex-1 px-0">
          {preview.error ? (
            <p className="px-6 py-4 text-xs text-danger">{`告警记录加载失败：${preview.error}`}</p>
          ) : preview.loading ? (
            <p className="px-6 py-4 text-xs text-muted-foreground">{"加载中…"}</p>
          ) : items.length === 0 ? (
            <p className="px-6 py-4 text-xs text-muted-foreground">{"暂无告警记录"}</p>
          ) : (
            <div>
              <ul className="divide-y divide-border">
                {items.map((a) => {
                  const meta = eventMeta[a.event] ?? { icon: AlertTriangle, cls: "text-muted-foreground" }
                  return (
                    <li key={a.id} className="px-4 py-3 sm:px-6">
                      <div className="flex min-w-0 items-start gap-2.5">
                        <meta.icon className={cn("size-4 shrink-0", meta.cls)} />
                        <div className="min-w-0 flex-1">
                          <div className="flex min-w-0 items-start justify-between gap-3">
                            <p className="min-w-0 flex-1 truncate text-sm text-foreground">{a.subject}</p>
                            <span className="shrink-0 text-xs text-muted-foreground">
                              {relativeTime(a.sent_at)}
                            </span>
                          </div>
                          <p className="truncate text-xs text-muted-foreground">
                            {a.channel_name
                              ? `${a.channel_name}${a.channel_type ? ` · ${channelTypeLabel(a.channel_type)}` : ""}`
                              : `渠道 #${a.channel_id}${a.channel_type ? ` · ${channelTypeLabel(a.channel_type)}` : ""}`}
                          </p>
                        </div>
                      </div>
                    </li>
                  )
                })}
              </ul>
            </div>
          )}
        </div>
      </section>

      <Dialog open={detailOpen} onOpenChange={(open) => { if (open) setDetailOpen(true); else closeDetail() }}>
        <DialogContent className="max-h-[85vh] overflow-hidden sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>{"告警动态"}</DialogTitle>
          </DialogHeader>
          <div className="max-h-[60vh] overflow-y-auto overscroll-contain rounded-md border border-border">
            <ul className="divide-y divide-border">
              {feed.map((a) => {
                const meta = eventMeta[a.event] ?? { icon: AlertTriangle, cls: "text-muted-foreground" }
                return (
                  <li key={a.id} className="px-4 py-3">
                    <div className="flex min-w-0 items-start gap-2.5">
                        <meta.icon className={cn("mt-0.5 size-4 shrink-0", meta.cls)} />
                        <div className="min-w-0 flex-1">
                          <div className="flex min-w-0 items-start justify-between gap-3">
                            <p className="min-w-0 flex-1 text-sm font-medium text-foreground">{a.subject}</p>
                            <span className="shrink-0 text-xs text-muted-foreground">
                              {relativeTime(a.sent_at)}
                            </span>
                          </div>
                          <p className="mt-1 text-xs text-muted-foreground">
                            {a.channel_name
                              ? `${a.channel_name}${a.channel_type ? ` · ${channelTypeLabel(a.channel_type)}` : ""}`
                              : `渠道 #${a.channel_id}${a.channel_type ? ` · ${channelTypeLabel(a.channel_type)}` : ""}`}
                          </p>
                          {a.body ? (
                            <p className="mt-1 text-xs leading-5 text-muted-foreground">
                              {a.body}
                            </p>
                          ) : null}
                          {!a.success && a.error_message ? (
                            <p className="mt-1 text-xs leading-5 text-danger">{a.error_message}</p>
                          ) : null}
                        </div>
                    </div>
                  </li>
                )
              })}
              {feedLoading && feed.length === 0 ? (
                <li className="px-4 py-6 text-sm text-muted-foreground">{"加载中…"}</li>
              ) : null}
              {!feedLoading && feed.length === 0 ? (
                <li className="px-4 py-6 text-sm text-muted-foreground">{"暂无告警记录"}</li>
              ) : null}
              {feedLoading && feed.length > 0 ? (
                <li className="px-4 py-3 text-xs text-muted-foreground">{"加载更多中…"}</li>
              ) : null}
            </ul>
          </div>
          {feedError ? <p className="text-sm text-danger">{feedError}</p> : null}
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
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

function FeedMetric({
  label,
  value,
  hint,
  tone = "muted",
}: {
  label: string
  value: string
  hint: string
  tone?: "success" | "danger" | "muted"
}) {
  return (
    <div className={cn(
      "flex min-w-0 items-center gap-2 py-1",
      "text-foreground",
    )} title={hint} aria-label={`${label}，${value}，${hint}`} tabIndex={0}>
      <div className="text-xs font-medium text-muted-foreground">{label}</div>
      <div className={cn(
        "truncate text-sm font-semibold tabular-nums",
        tone === "danger" ? "text-danger" : "text-foreground",
      )}>
        {value}
      </div>
    </div>
  )
}

function truncateText(value: string, max: number) {
  const text = value.trim()
  if (text.length <= max) return text
  return `${text.slice(0, max - 1)}…`
}

export function UpstreamAnnouncements() {
  const summary = useDashboardSummary()
  const preview = useAnnouncements(1, FEED_PREVIEW_SIZE)
  const [active, setActive] = useState<UpstreamAnnouncement | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)
  const [page, setPage] = useState(0)
  const [feed, setFeed] = useState<UpstreamAnnouncement[]>([])
  const [feedMeta, setFeedMeta] = useState<{ total: number; pages: number }>({
    total: 0,
    pages: 1,
  })
  const [feedLoading, setFeedLoading] = useState(false)
  const [feedError, setFeedError] = useState<string | null>(null)
  const feedLoadingGenerationRef = useRef<number | null>(null)
  const feedGenerationRef = useRef(0)
  const items = preview.data?.items ?? []
  const channels = summary.data?.channels ?? []
  const latest = items[0] ?? null
  const latestWhen = latest ? latest.published_at || latest.first_seen_at : ""
  const previewChannelCount = new Set(items.map((item) => item.channel_id)).size

  const channelByID = new Map(channels.map((c) => [c.id, c] as const))
  const activeChannel = active ? channelByID.get(active.channel_id) : null

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
      const res = await apiFetch<{ items: UpstreamAnnouncement[]; total: number; pages: number }>(`/announcements?page=${targetPage}&page_size=${FEED_DIALOG_SIZE}`)
      if (generation !== feedGenerationRef.current) return
      const next = Array.isArray(res?.items) ? res.items : []
      setFeed((current) => replace ? next : appendUniqueByID(current, next))
      setFeedMeta({ total: res?.total ?? 0, pages: Math.max(1, res?.pages ?? 1) })
      setPage(targetPage)
    } catch (error) {
      if (generation === feedGenerationRef.current) setFeedError((error as Error).message || "加载上游公告失败")
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
    setFeedMeta({ total: 0, pages: 1 })
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
            <div className="min-w-0" title="跟踪上游站点公告，及时发现模型、计费和渠道策略变化">
              <h2 className="text-lg font-semibold tracking-tight text-foreground">{"上游公告"}</h2>
            </div>
            {preview.data?.total ? (
              <Button size="sm" variant="ghost" className="h-8 shrink-0 px-2 text-xs" onClick={openDetail}>
                {"查看更多"}
              </Button>
            ) : null}
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-x-5 gap-y-1">
            <FeedMetric
              label="公告总数"
              value={preview.loading ? "加载中" : `${preview.data?.total ?? 0}`}
              hint={preview.loading ? "正在读取上游公告" : "当前保留的公告记录"}
            />
            <FeedMetric
              label="最近公告"
              value={preview.loading ? "加载中" : latest ? relativeTime(latestWhen) : "—"}
              hint={preview.loading ? "正在读取上游公告" : latest ? truncateText(announcementTitle(latest), 16) : "暂无上游公告"}
            />
            <FeedMetric
              label="近期渠道"
              value={preview.loading ? "加载中" : `${previewChannelCount}`}
              hint={preview.loading ? "正在读取上游公告" : previewChannelCount > 0 ? "近 10 条涉及的上游数" : "暂无渠道公告"}
            />
          </div>
        </div>
        <div className="min-h-0 flex-1 px-0">
          {preview.error ? (
            <p className="px-6 py-4 text-xs text-danger">{`公告数据加载失败：${preview.error}`}</p>
          ) : preview.loading ? (
            <p className="px-6 py-4 text-xs text-muted-foreground">{"加载中…"}</p>
          ) : items.length === 0 ? (
            <p className="px-6 py-4 text-xs text-muted-foreground">{"暂无上游公告"}</p>
          ) : (
            <div>
              <ul className="divide-y divide-border">
                {items.map((item) => {
                  const ch = channelByID.get(item.channel_id)
                  const title = announcementTitle(item)
                  const when = item.published_at || item.first_seen_at
                  return (
                    <li key={item.id}>
                      <button
                        type="button"
                        className="block w-full px-4 py-3 text-left transition-colors hover:bg-muted/40 sm:px-6"
                        onClick={() => { closeDetail(); setActive(item) }}
                      >
                        <div className="mb-1.5 flex min-w-0 items-start justify-between gap-3">
                          <div className="flex min-w-0 flex-1 items-start gap-2">
                            <span className="min-w-0 flex-1 truncate text-sm font-medium text-foreground">{title}</span>
                            {item.type ? (
                              <Badge variant="outline" className="h-5 shrink-0 px-1.5 text-[10px]">
                                {item.type}
                              </Badge>
                            ) : null}
                          </div>
                          <span className="shrink-0 text-xs text-muted-foreground">{relativeTime(when)}</span>
                        </div>
                        <div className="flex min-w-0">
                          <span className="truncate text-xs text-muted-foreground">
                            {ch ? `${ch.name} · ${channelTypeLabel(ch.type)}` : `渠道 #${item.channel_id}`}
                          </span>
                        </div>
                      </button>
                    </li>
                  )
                })}
              </ul>
            </div>
          )}
        </div>
      </section>

      <Dialog open={active !== null} onOpenChange={(open) => !open && setActive(null)}>
        <DialogContent className="max-h-[85vh] overflow-hidden sm:max-w-2xl">
          {active ? (
            <>
              <DialogHeader>
                <DialogTitle className="pr-8 leading-6">{announcementTitle(active)}</DialogTitle>
              </DialogHeader>
              <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                <Badge variant="outline">
                  {activeChannel
                    ? `${activeChannel.name} · ${channelTypeLabel(activeChannel.type)}`
                    : `渠道 #${active.channel_id}`}
                </Badge>
                {active.type ? <Badge variant="outline">{active.type}</Badge> : null}
                <span>{dateTime(active.published_at || active.first_seen_at)}</span>
              </div>
              <div className="max-h-[56vh] overflow-y-auto overscroll-contain rounded-md border border-border">
                <MarkdownContent content={active.content} />
              </div>
              {active.link ? (
                <a
                  href={active.link}
                  target="_blank"
                  rel="noreferrer"
                  className="text-xs text-brand hover:underline"
                >
                  {active.link}
                </a>
              ) : null}
            </>
          ) : null}
        </DialogContent>
      </Dialog>

      <Dialog open={detailOpen} onOpenChange={(open) => { if (open) setDetailOpen(true); else closeDetail() }}>
        <DialogContent className="max-h-[85vh] overflow-hidden sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>{"上游公告"}</DialogTitle>
          </DialogHeader>
          <div className="max-h-[60vh] overflow-y-auto overscroll-contain rounded-md border border-border">
            <ul className="divide-y divide-border">
              {feed.map((item) => {
                const ch = channelByID.get(item.channel_id)
                const when = item.published_at || item.first_seen_at
                return (
                  <li key={item.id}>
                    <button
                      type="button"
                      className="block w-full px-4 py-3 text-left transition-colors hover:bg-muted/40 sm:px-6"
                      onClick={() => { closeDetail(); setActive(item) }}
                    >
                      <div className="mb-1.5 flex min-w-0 items-start justify-between gap-3">
                        <div className="flex min-w-0 flex-1 items-start gap-2">
                          <span className="min-w-0 flex-1 truncate text-sm font-medium text-foreground">
                            {announcementTitle(item)}
                          </span>
                          {item.type ? (
                            <Badge variant="outline" className="h-5 shrink-0 px-1.5 text-[10px]">
                              {item.type}
                            </Badge>
                          ) : null}
                        </div>
                        <span className="shrink-0 text-xs text-muted-foreground">
                          {relativeTime(when)}
                        </span>
                      </div>
                      <div className="flex min-w-0">
                        <span className="truncate text-xs text-muted-foreground">
                          {ch ? `${ch.name} · ${channelTypeLabel(ch.type)}` : `渠道 #${item.channel_id}`}
                        </span>
                      </div>
                    </button>
                  </li>
                )
              })}
              {feedLoading && feed.length === 0 ? (
                <li className="px-4 py-6 text-sm text-muted-foreground">{"加载中…"}</li>
              ) : null}
              {!feedLoading && feed.length === 0 ? (
                <li className="px-4 py-6 text-sm text-muted-foreground">{"暂无上游公告"}</li>
              ) : null}
              {feedLoading && feed.length > 0 ? (
                <li className="px-4 py-3 text-xs text-muted-foreground">{"加载更多中…"}</li>
              ) : null}
            </ul>
          </div>
          {feedError ? <p className="text-sm text-danger">{feedError}</p> : null}
          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
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

const captchaTypeLabel: Record<string, string> = {
  capsolver: "CapSolver",
  "2captcha": "2Captcha",
  anticaptcha: "AntiCaptcha",
  yescaptcha: "YesCaptcha",
}

export function CaptchaStatus() {
  const query = useCaptchaConfigs()
  const { confirm, dialog: confirmDialog } = useConfirm()
  const [editing, setEditing] = useState<CaptchaConfig | null>(null)
  const [open, setOpen] = useState(false)
  const [busyIDs, setBusyIDs] = useState<Set<number>>(() => new Set())
  const configs = Array.isArray(query.data) ? query.data : []
  const enabledConfigs = configs.filter((item) => item.enabled).length
  const balanceNormalConfigs = configs.filter((item) => item.enabled && item.balance_at && !item.balance_error).length
  const proxyConfigs = configs.filter((item) => item.proxy_enabled).length
  const balanceErrorConfigs = configs.filter((item) => item.balance_error).length
  const balanceSnapshotConfigs = configs.filter((item) => item.last_balance != null).length
  const firstLoadFailed = Boolean(query.error && !query.data)

  function setConfigBusy(id: number, busy: boolean) {
    setBusyIDs((current) => {
      const next = new Set(current)
      if (busy) next.add(id)
      else next.delete(id)
      return next
    })
  }

  async function handleRefreshBalance(c: CaptchaConfig) {
    setConfigBusy(c.id, true)
    try {
      const updated = await apiFetch<CaptchaConfig>(`/captcha-configs/${c.id}/refresh-balance`, { method: "POST" })
      query.setData((current) => (current ?? []).map((item) => item.id === updated.id ? updated : item))
      query.refetch()
      toast.success(`已更新 ${c.name} 剩余额度`)
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "更新失败")
      query.refetch()
    } finally {
      setConfigBusy(c.id, false)
    }
  }

  async function handleDelete(c: CaptchaConfig) {
    const ok = await confirm({
      title: `删除打码配置 ${c.name}？`,
      description: "删除后引用此配置的渠道将无法自动过码，需要重新指定打码 provider。",
      confirmLabel: "删除",
      destructive: true,
    })
    if (!ok) return
    setConfigBusy(c.id, true)
    try {
      await apiFetch(`/captcha-configs/${c.id}`, { method: "DELETE" })
      query.setData((current) => (current ?? []).filter((item) => item.id !== c.id))
      query.refetch()
      toast.success(`已删除 ${c.name}`)
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "删除失败")
    } finally {
      setConfigBusy(c.id, false)
    }
  }

  return (
    <section className="overflow-hidden rounded-lg border border-border bg-background">
      <div className="flex flex-wrap items-start justify-between gap-3 border-b border-border bg-primary/[0.035] px-4 py-3">
          <div className="min-w-0" title="管理上游登录和 Turnstile 自动处理使用的打码 provider">
            <h2 className="text-lg font-semibold tracking-tight text-foreground">{"打码平台"}</h2>
          </div>
          <div className="flex items-center gap-2">
            <Button size="sm" variant="outline" className="h-8 shrink-0 gap-1 text-xs" onClick={query.refetch} disabled={query.loading || query.refreshing || busyIDs.size > 0}>
              <RefreshCw className={cn("size-3.5", (query.loading || query.refreshing) && "animate-spin")} />
              {"刷新列表"}
            </Button>
            <Button size="sm" className="h-8 shrink-0 gap-1 px-2 text-xs" onClick={() => { setEditing(null); setOpen(true) }}>
              <Plus className="size-3" />
              {"新增服务"}
            </Button>
          </div>
      </div>
      {query.error ? (
        <div className="flex items-center justify-between gap-3 border-b border-destructive/20 bg-destructive/5 px-4 py-3 text-sm">
          <span className="min-w-0 truncate text-destructive">{`数据刷新失败：${query.error}`}</span>
          <Button size="sm" variant="outline" className="h-8 shrink-0" onClick={query.refetch} disabled={query.loading || query.refreshing}>{"重试"}</Button>
        </div>
      ) : null}
      <div className="flex flex-wrap items-center gap-x-6 gap-y-1 border-b border-border px-4 py-2">
          <FeedMetric
            label="余额正常 / 启用"
            value={query.loading ? "加载中" : firstLoadFailed ? "—" : `${balanceNormalConfigs} / ${enabledConfigs}`}
            hint={query.loading ? "正在读取验证码服务" : firstLoadFailed ? "读取失败" : `共 ${configs.length} 个服务`}
            tone={!query.loading && !firstLoadFailed && balanceNormalConfigs === enabledConfigs ? "success" : "muted"}
          />
          <FeedMetric
            label="余额快照"
            value={query.loading ? "加载中" : firstLoadFailed ? "—" : `${balanceSnapshotConfigs}`}
            hint={query.loading ? "正在读取验证码服务" : firstLoadFailed ? "读取失败" : `${balanceSnapshotConfigs} / ${configs.length} 已采集`}
          />
          <FeedMetric
            label="余额异常"
            value={query.loading ? "加载中" : firstLoadFailed ? "—" : `${balanceErrorConfigs}`}
            hint={query.loading ? "正在读取验证码服务" : firstLoadFailed ? "读取失败" : balanceErrorConfigs > 0 ? "需要检查 provider 连接" : "暂无余额错误"}
            tone={query.loading || firstLoadFailed ? "muted" : balanceErrorConfigs > 0 ? "danger" : "success"}
          />
          <FeedMetric
            label="代理访问"
            value={query.loading ? "加载中" : firstLoadFailed ? "—" : `${proxyConfigs}`}
            hint={query.loading ? "正在读取验证码服务" : firstLoadFailed ? "读取失败" : "使用全局代理出口"}
          />
      </div>
      <div>
        {query.loading && configs.length === 0 ? (
          <p className="px-6 py-4 text-xs text-muted-foreground">{"加载中…"}</p>
        ) : query.error && configs.length === 0 ? (
          <p className="px-6 py-8 text-center text-sm text-destructive">{"无法读取打码平台配置，请重试。"}</p>
        ) : configs.length === 0 ? (
          <div className="m-4 border border-dashed border-border bg-muted/10 px-6 py-8 text-center">
            <p className="text-sm font-medium text-foreground">{"暂未配置打码服务"}</p>
            <p className="mt-1 text-xs text-muted-foreground">{"新增服务后，可供需要 Turnstile 验证的上游渠道使用。"}</p>
          </div>
        ) : (
          <Table noScroll className="table-fixed">
            <TableHeader>
              <TableRow className="bg-primary/[0.025] hover:bg-primary/[0.025]">
                <TableHead>{"服务"}</TableHead>
                <TableHead className="w-32">{"平台"}</TableHead>
                <TableHead className="w-32">{"访问方式"}</TableHead>
                <TableHead className="w-36 text-right">{"余额"}</TableHead>
                <TableHead className="w-64">{"最近采集"}</TableHead>
                <TableHead className="w-24 text-center">{"状态"}</TableHead>
                <TableHead className="w-32 text-right">{"操作"}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {configs.map((p) => (
                <TableRow key={p.id}>
                  <TableCell><span className="block truncate text-sm font-semibold text-foreground" title={p.name}>{p.name}</span></TableCell>
                  <TableCell className="w-32 text-xs text-muted-foreground">{captchaTypeLabel[p.type] ?? p.type}</TableCell>
                  <TableCell className="w-32 text-xs">{p.proxy_enabled ? "全局代理" : "直接连接"}</TableCell>
                  <TableCell className="w-36 text-right text-sm font-medium tabular-nums">{formatCaptchaBalance(p)}</TableCell>
                  <TableCell className={cn("w-64 text-xs", p.balance_error ? "text-destructive" : "text-muted-foreground")}>
                    <span className="block truncate" title={p.balance_error || (p.balance_at ? `更新于 ${relativeTime(p.balance_at)}` : "未更新")}>
                      {p.balance_error ? p.balance_error : p.balance_at ? `更新于 ${relativeTime(p.balance_at)}` : "未更新"}
                    </span>
                  </TableCell>
                  <TableCell className="w-24 text-center">
                    <Badge variant={p.enabled ? "secondary" : "outline"} className={p.enabled ? "text-success" : "text-muted-foreground"}>{p.enabled ? "已启用" : "已禁用"}</Badge>
                  </TableCell>
                  <TableCell className="w-32">
                    <div className="flex justify-end gap-1">
                      <Button size="icon-sm" variant="outline" className="size-7" title={`刷新 ${p.name} 余额`} aria-label={`刷新 ${p.name} 余额`} disabled={busyIDs.size > 0} onClick={() => handleRefreshBalance(p)}><RefreshCw className={cn("size-3.5", busyIDs.has(p.id) && "animate-spin")} /></Button>
                      <Button size="icon-sm" variant="outline" className="size-7" title={`编辑 ${p.name}`} aria-label={`编辑 ${p.name}`} disabled={busyIDs.size > 0} onClick={() => { setEditing(p); setOpen(true) }}><Pencil className="size-3.5" /></Button>
                      <Button size="icon-sm" variant="outline" className="size-7 text-destructive hover:bg-destructive/10 hover:text-destructive" title={`删除 ${p.name}`} aria-label={`删除 ${p.name}`} disabled={busyIDs.size > 0} onClick={() => handleDelete(p)}><Trash2 className="size-3.5" /></Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </div>

      <CaptchaFormDialog
        open={open}
        onOpenChange={(v) => {
          setOpen(v)
          if (!v) setEditing(null)
        }}
        config={editing}
      />

      {confirmDialog}
    </section>
  )
}

function formatCaptchaBalance(c: CaptchaConfig) {
  if (c.last_balance == null) return "余额 —"
  if (c.balance_unit === "points") return `${decimal(c.last_balance, 0)} 点`
  return money(c.last_balance, { precise: true })
}

const notifyTypeIcon: Partial<Record<NotificationChannelType, LucideIcon>> = {
  telegram: Send,
  webhook: Send,
  email: Send,
  wecom: Send,
  dingtalk: Send,
  feishu: Send,
  serverchan3: Send,
}

export function NotificationStatus() {
  const channelsQuery = useNotificationChannels()
  const logs = useNotificationLogs(1, 10)
  const refresh = useTriggerRefresh()
  const { confirm, dialog: confirmDialog } = useConfirm()
  const [editing, setEditing] = useState<NotificationChannel | null>(null)
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState<number | null>(null)

  const channels = Array.isArray(channelsQuery.data) ? channelsQuery.data : []
  const enabledChannels = channels.filter((item) => item.enabled).length
  const disabledChannels = channels.length - enabledChannels
  const totalLogs = logs.data?.items ?? []
  const lastSent = totalLogs.length > 0 ? totalLogs[0] : null
  const recentFailed = totalLogs.filter((l) => !l.success).length

  async function handleDelete(c: NotificationChannel) {
    const ok = await confirm({
      title: `删除通知渠道 ${c.name}？`,
      description: "删除后系统将不再向该渠道推送告警，历史发送记录会保留以便审计。",
      confirmLabel: "删除",
      destructive: true,
    })
    if (!ok) return
    setBusy(c.id)
    try {
      await apiFetch(`/notifications/channels/${c.id}`, { method: "DELETE" })
      toast.success(`已删除 ${c.name}`)
      refresh()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "删除失败")
    } finally {
      setBusy(null)
    }
  }

  async function handleTest(c: NotificationChannel) {
    setBusy(c.id)
    try {
      const res = await apiFetch<{ ok: boolean; error?: string }>(
        `/notifications/channels/${c.id}/test`,
        { method: "POST" },
      )
      if (res.ok) {
        toast.success(`已发送测试消息到 ${c.name}`)
      } else {
        toast.error(`测试失败：${res.error ?? "未知错误"}`)
      }
      refresh()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "测试失败")
    } finally {
      setBusy(null)
    }
  }

  return (
    <section className="overflow-hidden rounded-lg border border-border bg-background">
      <div className="border-b border-border bg-muted/20 px-4 py-3">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0" title="配置告警推送目标，快速确认发送状态和最近失败情况">
            <h2 className="text-lg font-semibold tracking-tight text-foreground">{"通知渠道"}</h2>
          </div>
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              variant="outline"
              className="h-8 shrink-0 gap-1 text-xs"
              onClick={() => {
                channelsQuery.refetch()
                logs.refetch()
              }}
              disabled={channelsQuery.loading || channelsQuery.refreshing || logs.loading || logs.refreshing}
            >
              <RefreshCw className={cn("size-3.5", (channelsQuery.loading || channelsQuery.refreshing || logs.loading || logs.refreshing) && "animate-spin")} />
              {"刷新"}
            </Button>
            <Button size="sm" className="h-8 shrink-0 gap-1 text-xs" onClick={() => { setEditing(null); setOpen(true) }}>
              <Plus className="size-3" />
              {"新增渠道"}
            </Button>
          </div>
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-x-6 gap-y-1 border-b border-border px-4 py-2">
          <NotificationMetric
            label="渠道总数"
            value={channelsQuery.loading ? "加载中" : channelsQuery.error ? "读取失败" : `${channels.length}`}
            hint={channelsQuery.loading ? "正在读取渠道配置" : channelsQuery.error ?? `${enabledChannels} 启用 · ${disabledChannels} 禁用`}
            tone={channelsQuery.error ? "danger" : "muted"}
          />
          <NotificationMetric
            label="上次发送"
            value={logs.loading ? "加载中" : logs.error ? "读取失败" : lastSent ? relativeTime(lastSent.sent_at) : "—"}
            hint={logs.loading ? "正在读取发送记录" : logs.error ?? (lastSent ? (lastSent.success ? "最近一次成功返回" : "最近一次发送失败") : "暂无发送记录")}
            tone={logs.error ? "danger" : "muted"}
          />
          <NotificationMetric
            label="近 10 条失败"
            value={logs.loading ? "加载中" : logs.error ? "读取失败" : `${recentFailed}`}
            hint={logs.loading ? "正在读取发送记录" : logs.error ?? (recentFailed === 0 ? "近期发送稳定" : "需要检查渠道配置")}
            tone={logs.loading ? "muted" : logs.error ? "danger" : recentFailed === 0 ? "success" : "danger"}
          />
      </div>
      <div>
        {channelsQuery.error ? (
          <p className="px-4 py-5 text-sm text-destructive">{channelsQuery.error}</p>
        ) : channelsQuery.loading ? (
          <p className="px-4 py-5 text-xs text-muted-foreground">{"加载中…"}</p>
        ) : channels.length === 0 ? (
          <div className="m-4 border border-dashed border-border bg-muted/20 p-6 text-center">
            <p className="text-sm font-medium text-foreground">{"暂未配置通知渠道"}</p>
            <p className="mt-1 text-xs text-muted-foreground">{"新增邮箱、Webhook 或聊天工具后，系统会把告警推送到对应渠道。"}</p>
          </div>
        ) : (
          <ul className="divide-y divide-border">
            {channels.map((c) => {
              const Icon = notifyTypeIcon[c.type] ?? Send
              const subCount = parseSubCount(c.subscriptions)
              return (
                <li key={c.id} className="bg-background p-3 transition-colors hover:bg-muted/20">
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex min-w-0 items-start gap-3">
                      <span className={cn(
                        "flex size-8 shrink-0 items-center justify-center rounded-lg ring-1 ring-inset",
                        c.enabled ? "bg-brand/10 text-brand ring-brand/15" : "bg-muted text-muted-foreground ring-border",
                      )}>
                        <Icon className="size-4" />
                      </span>
                      <div className="min-w-0">
                        <div className="flex min-w-0 flex-wrap items-center gap-1.5">
                          <p className="truncate text-sm font-semibold text-foreground">{c.name}</p>
                          <Badge variant={c.enabled ? "secondary" : "outline"} className="h-5 px-1.5 text-[10px]">
                            {c.enabled ? "启用" : "禁用"}
                          </Badge>
                        </div>
                        <p className="mt-1 text-[11px] text-muted-foreground">
                          {notificationTypeLabel(c.type)}
                          {" · "}
                          {subCount === 0 ? "订阅全部事件" : `${subCount} 条订阅规则`}
                        </p>
                      </div>
                    </div>
                    <div className="flex shrink-0 items-center gap-0.5">
                      <Button
                        size="icon"
                        variant="ghost"
                        className="h-7 w-7"
                        title="测试发送"
                        aria-label={`测试发送 ${c.name}`}
                        disabled={busy === c.id}
                        onClick={() => handleTest(c)}
                      >
                        <TestTube2 className="size-3.5" />
                      </Button>
                      <Button
                        size="icon"
                        variant="ghost"
                        className="h-7 w-7"
                        title="编辑"
                        aria-label={`编辑 ${c.name}`}
                        onClick={() => {
                          setEditing(c)
                          setOpen(true)
                        }}
                      >
                        <Pencil className="size-3.5" />
                      </Button>
                      <Button
                        size="icon"
                        variant="ghost"
                        className="h-7 w-7 text-destructive hover:bg-destructive/10 hover:text-destructive"
                        title="删除"
                        aria-label={`删除 ${c.name}`}
                        disabled={busy === c.id}
                        onClick={() => handleDelete(c)}
                      >
                        <Trash2 className="size-3.5" />
                      </Button>
                    </div>
                  </div>
                </li>
              )
            })}
          </ul>
        )}
      </div>

      <NotificationFormDialog
        open={open}
        onOpenChange={(v) => {
          setOpen(v)
          if (!v) setEditing(null)
        }}
        channel={editing}
      />

      {confirmDialog}
    </section>
  )
}

function NotificationMetric({
  label,
  value,
  hint,
  tone = "muted",
}: {
  label: string
  value: string
  hint: string
  tone?: "success" | "danger" | "muted"
}) {
  return (
    <div className={cn(
      "flex min-w-0 items-center gap-2 py-1",
      "text-foreground",
    )} title={hint} aria-label={`${label}，${value}，${hint}`} tabIndex={0}>
      <div className="text-xs font-medium text-muted-foreground">{label}</div>
      <div className={cn(
        "truncate text-sm font-semibold tabular-nums",
        tone === "danger" ? "text-danger" : "text-foreground",
      )}>
        {value}
      </div>
    </div>
  )
}

function notificationTypeLabel(value: NotificationChannelType) {
  switch (value) {
    case "telegram":
      return "Telegram"
    case "webhook":
      return "Webhook"
    case "email":
      return "Email"
    case "wecom":
      return "企业微信"
    case "dingtalk":
      return "钉钉"
    case "feishu":
      return "飞书"
    case "serverchan3":
      return "ServerChan³"
    default:
      return value
  }
}

function parseSubCount(raw?: string): number {
  if (!raw) return 0
  try {
    const arr = JSON.parse(raw)
    return Array.isArray(arr) ? arr.length : 0
  } catch {
    return 0
  }
}

function announcementTitle(item: UpstreamAnnouncement): string {
  const title = item.title?.trim()
  if (title) return title
  const content = item.content.trim()
  if (!content) return "上游公告"
  const chars = Array.from(content)
  return chars.length > 32 ? `${chars.slice(0, 32).join("")}...` : content
}

function MarkdownContent({ content }: { content: string }) {
  const blocks = parseMarkdownBlocks(content)
  if (blocks.length === 0) {
    return <div className="p-4 text-sm leading-6 text-muted-foreground">{"—"}</div>
  }
  return (
    <div className="space-y-3 break-words p-4 text-sm leading-6 text-foreground">
      {blocks.map((block, idx) => {
        switch (block.type) {
          case "heading":
            return (
              <div key={idx} className={cn("font-semibold text-foreground", block.level === 1 ? "text-lg" : "text-base")}>
                {renderInlineMarkdown(block.text)}
              </div>
            )
          case "quote":
            return (
              <blockquote key={idx} className="border-l-2 border-border pl-3 text-muted-foreground">
                {renderInlineMarkdown(block.text)}
              </blockquote>
            )
          case "code":
            return (
              <pre key={idx} className="overflow-x-auto rounded-md bg-muted px-3 py-2 font-mono text-xs leading-5">
                <code>{block.text}</code>
              </pre>
            )
          case "list":
            return (
              <ul key={idx} className="space-y-1 pl-5">
                {block.items.map((item, itemIdx) => (
                  <li key={itemIdx} className="list-disc">
                    {renderInlineMarkdown(item)}
                  </li>
                ))}
              </ul>
            )
          default:
            return <p key={idx}>{renderInlineMarkdown(block.text)}</p>
        }
      })}
    </div>
  )
}

type MarkdownBlock =
  | { type: "heading"; level: number; text: string }
  | { type: "quote"; text: string }
  | { type: "code"; text: string }
  | { type: "list"; items: string[] }
  | { type: "paragraph"; text: string }

function parseMarkdownBlocks(content: string): MarkdownBlock[] {
  const lines = content.replace(/\r\n/g, "\n").split("\n")
  const blocks: MarkdownBlock[] = []
  let paragraph: string[] = []
  let list: string[] = []
  let code: string[] | null = null

  const flushParagraph = () => {
    const text = paragraph.join("\n").trim()
    if (text) blocks.push({ type: "paragraph", text })
    paragraph = []
  }
  const flushList = () => {
    if (list.length > 0) blocks.push({ type: "list", items: list })
    list = []
  }

  for (const line of lines) {
    if (line.trim().startsWith("```")) {
      flushParagraph()
      flushList()
      if (code) {
        blocks.push({ type: "code", text: code.join("\n") })
        code = null
      } else {
        code = []
      }
      continue
    }
    if (code) {
      code.push(line)
      continue
    }

    const trimmed = line.trim()
    if (!trimmed) {
      flushParagraph()
      flushList()
      continue
    }

    const heading = /^(#{1,6})\s+(.+)$/.exec(trimmed)
    if (heading) {
      flushParagraph()
      flushList()
      blocks.push({ type: "heading", level: heading[1].length, text: heading[2] })
      continue
    }

    const quote = /^>\s?(.+)$/.exec(trimmed)
    if (quote) {
      flushParagraph()
      flushList()
      blocks.push({ type: "quote", text: quote[1] })
      continue
    }

    const bullet = /^[-*+]\s+(.+)$/.exec(trimmed)
    if (bullet) {
      flushParagraph()
      list.push(bullet[1])
      continue
    }

    flushList()
    paragraph.push(line)
  }
  flushParagraph()
  flushList()
  if (code && code.length > 0) blocks.push({ type: "code", text: code.join("\n") })
  return blocks
}

function renderInlineMarkdown(text: string) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`|\[[^\]]+\]\([^)]+\)|https?:\/\/\S+)/g)
  return parts.map((part, idx) => {
    if (!part) return null
    if (part.startsWith("**") && part.endsWith("**")) {
      return <strong key={idx}>{part.slice(2, -2)}</strong>
    }
    if (part.startsWith("`") && part.endsWith("`")) {
      return (
        <code key={idx} className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
          {part.slice(1, -1)}
        </code>
      )
    }
    const link = /^\[([^\]]+)\]\(([^)]+)\)$/.exec(part)
    if (link) {
      return (
        <a key={idx} href={link[2]} target="_blank" rel="noreferrer" className="text-brand hover:underline">
          {link[1]}
        </a>
      )
    }
    if (/^https?:\/\//.test(part)) {
      return (
        <a key={idx} href={part} target="_blank" rel="noreferrer" className="text-brand hover:underline">
          {part}
        </a>
      )
    }
    return (
      <Fragment key={idx}>
        {part.split("\n").map((line, lineIdx, arr) => (
          <Fragment key={lineIdx}>
            {line}
            {lineIdx < arr.length - 1 ? <br /> : null}
          </Fragment>
        ))}
      </Fragment>
    )
  })
}

export function BottomPanels() {
  return (
    <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
      <AlertFeed />
      <UpstreamAnnouncements />
    </div>
  )
}

function appendUniqueByID<T extends { id: number }>(current: T[], next: T[]) {
  const seen = new Set(current.map((item) => item.id))
  return [...current, ...next.filter((item) => !seen.has(item.id))]
}
