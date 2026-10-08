"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { toast } from "sonner"
import {
  ArrowUpDown,
  AlertTriangle,
  BadgeDollarSign,
  CheckCircle2,
  ChevronDown,
  CreditCard,
  ExternalLink,
  KeyRound,
  Loader2,
  LogIn,
  Pause,
  Pencil,
  Play,
  Plus,
  RefreshCw,
  Search,
  Tags,
  Trash2,
  WalletCards,
  Gift,
  X,
  XCircle,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Progress } from "@/components/ui/progress"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { StatusMetricCard as ChannelHeaderMetric } from "@/components/ui/status-metric-card"
import { PageHeader } from "@/components/ui/page-header"
import { useConfirm } from "@/components/ui/confirm-dialog"
import { useChannels, useChannelRates } from "@/lib/queries"
import { apiFetch } from "@/lib/api"
import { channelAccountingHasError, channelAccountingNeedsReview } from "@/lib/channel-accounting"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { channelTypeLabel, compactCount, currentDayCost, decimal, formatRatio, money, relativeTime } from "@/lib/format"
import { channelAccountConcurrencyPresentation } from "@/lib/channel-account-concurrency"
import { cn } from "@/lib/utils"
import { syncAllChannelsStream, syncChannelStream, testLoginStream, type ProgressEvent } from "@/lib/sync-stream"
import type { Channel, ChannelDeletionAccepted, ChannelRedeemResult, RateSnapshot } from "@/lib/api-types"
import { channelDeletionStatusLabel, isChannelDeletionPending, markChannelDeletionPending } from "@/lib/channel-deletion"
import { ChannelFormDialog } from "@/components/monitor/channel-form-dialog"
import { ChannelRedeemDialog } from "@/components/monitor/channel-redeem-dialog"
import { ChannelRechargeDialog } from "@/components/monitor/channel-recharge-dialog"
import { ChannelAPIKeysDialog } from "@/components/monitor/channel-api-keys-dialog"
import { ChannelCostDialog } from "@/components/monitor/channel-cost-dialog"
import { ChannelErrorBadge, ChannelErrorDialog } from "@/components/monitor/channel-error-dialog"
import { ChannelDeletionReviewBadge, ChannelDeletionReviewDialog } from "@/components/monitor/channel-deletion-review-dialog"
import {
  ChannelSubscriptionUsageMetricTiles,
} from "@/components/monitor/channel-subscription-usage-dialog"

type Status = "healthy" | "low" | "insufficient" | "failed" | "idle" | "accounting_review" | "deletion_pending"
type GroupSortMode = "channel-asc" | "channel-desc" | "ratio-asc" | "ratio-desc"
type ChannelTypeFilter = "all" | Channel["type"] | "direct"
type ChannelStatusFilter = "all" | "healthy" | "failed" | "low_balance" | "needs_funding" | "accounting_review" | "paused" | "idle" | "deletion_pending"

const channelStatusFilterOptions: { value: ChannelStatusFilter; label: string }[] = [
  { value: "all", label: "全部状态" },
  { value: "healthy", label: "健康" },
  { value: "failed", label: "异常" },
  { value: "low_balance", label: "低余额 / 需充值" },
  { value: "needs_funding", label: "待确认充值" },
  { value: "accounting_review", label: "账务待复核" },
  { value: "paused", label: "暂停监控" },
  { value: "idle", label: "尚未采集" },
  { value: "deletion_pending", label: "待远端清理" },
]

function balanceObservationError(channel: Channel) {
  const message = channel.balance_error?.trim()
  return message || null
}

function hasAccountBalanceInsufficientFact(channel: Channel) {
  // A boolean without its observation timestamp is an incomplete fact and must
  // not be presented as a current upstream account decision.
  return channel.upstream_account_balance_insufficient === true && Boolean(channel.upstream_account_balance_insufficient_at)
}

function isBalanceRiskStatus(status: Status) {
  return status === "low" || status === "insufficient"
}

function statusOf(c: Channel): Status {
  if (isChannelDeletionPending(c)) return "deletion_pending"
  if (c.last_error) return "failed"
  if (c.credential_mode === "direct") return "healthy"
  if (channelAccountingHasError(c)) return "failed"
  if (hasAccountBalanceInsufficientFact(c)) return "insufficient"
  if (balanceObservationError(c)) return "failed"
  if (channelAccountingNeedsReview(c)) return "accounting_review"
  if (c.last_balance == null) return "idle"
  if (c.balance_threshold > 0 && c.last_balance < c.balance_threshold) return "low"
  return "healthy"
}

function matchesChannelStatusFilter(channel: Channel, filter: ChannelStatusFilter) {
  switch (filter) {
    case "healthy":
      return statusOf(channel) === "healthy"
    case "failed":
      return statusOf(channel) === "failed"
    case "low_balance":
      return isBalanceRiskStatus(statusOf(channel))
    case "needs_funding":
      return (channel.pending_funding_count ?? 0) > 0
    case "accounting_review":
      return channelAccountingNeedsReview(channel)
    case "paused":
      return !channel.monitor_enabled
    case "idle":
      return statusOf(channel) === "idle"
    case "deletion_pending":
      return statusOf(channel) === "deletion_pending"
    default:
      return true
  }
}

const statusMap: Record<Status, { label: string; cls: string }> = {
  healthy: { label: "健康", cls: "text-success bg-success/10" },
  low: { label: "低余额", cls: "text-warning bg-warning/10" },
  insufficient: { label: "账号余额不足", cls: "text-danger bg-danger/10" },
  failed: { label: "异常", cls: "text-danger bg-danger/10" },
  idle: { label: "尚未采集", cls: "text-muted-foreground bg-muted/40" },
  accounting_review: { label: "账务待复核", cls: "text-warning bg-warning/10" },
  deletion_pending: { label: "待远端清理", cls: "text-warning bg-warning/10" },
}

function gatewayAppLabel(app: string | undefined) {
  if (app === "claude") return "Claude"
  if (app === "gemini") return "Gemini"
  return "Codex / GPT"
}

function channelRequestCount(channel: Channel) {
  return Number(channel.request_count ?? 0)
}

function BalanceObservationErrorBadge({ channel }: { channel: Channel }) {
  const message = balanceObservationError(channel)
  if (!message) return null
  const observedAt = channel.balance_error_at
  return (
    <Tooltip delayDuration={150}>
      <TooltipTrigger asChild>
        <span
          tabIndex={0}
          className="inline-flex max-w-full cursor-help items-center gap-1 rounded-md border border-warning/30 bg-warning/5 px-1.5 py-0.5 text-[10px] font-medium leading-4 text-warning ring-1 ring-inset ring-warning/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1"
          aria-label={`${channel.name} 余额采集错误${observedAt ? `，${relativeTime(observedAt)}` : "，时间未知"}`}
        >
          <AlertTriangle className="size-3 shrink-0" />
          <span>{"余额采集错误"}</span>
        </span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-80 space-y-1 text-xs">
        <p className="font-medium">{"余额采集错误"}</p>
        <p className="whitespace-pre-wrap break-words">{message}</p>
        <p className="text-muted-foreground">{observedAt ? `最近观测：${relativeTime(observedAt)}` : "观测时间未知"}</p>
      </TooltipContent>
    </Tooltip>
  )
}

function AccountBalanceInsufficientBadge({ channel }: { channel: Channel }) {
  if (!hasAccountBalanceInsufficientFact(channel)) return null
  const observedAt = channel.upstream_account_balance_insufficient_at
  return (
    <Tooltip delayDuration={150}>
      <TooltipTrigger asChild>
        <span
          tabIndex={0}
          className="inline-flex max-w-full cursor-help items-center gap-1 rounded-md border border-danger/25 bg-danger/5 px-1.5 py-0.5 text-[10px] font-medium leading-4 text-danger ring-1 ring-inset ring-danger/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1"
          aria-label={`${channel.name} 账号余额不足，结构化上游事实${observedAt ? `，${relativeTime(observedAt)}` : ""}`}
        >
          <WalletCards className="size-3 shrink-0" />
          <span>{"账号余额不足"}</span>
        </span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-80 space-y-1 text-xs">
        <p className="font-medium">{"结构化账号余额不足事实"}</p>
        <p>{"上游明确返回账号余额不足；这不是本地余额阈值判断，也不代表本地余额快照被改成了 0。"}</p>
        <p className="text-muted-foreground">{observedAt ? `最近观测：${relativeTime(observedAt)}` : "观测时间未知"}</p>
      </TooltipContent>
    </Tooltip>
  )
}

/** ratioTone 按倍率给 chip 上色，与 ChannelRatesPanel 共用同一套规则。 */
function ratioTone(r: number): string {
  if (r <= 0.8) return "bg-success/10 text-success ring-success/20"
  if (r > 2) return "bg-danger/10 text-danger ring-danger/20"
  if (r > 1.2) return "bg-warning/10 text-warning ring-warning/20"
  return "bg-muted text-foreground ring-border"
}

type InlineRate = RateSnapshot & {
  effectiveRatio: number
  effectiveCompletionRatio: number
}

function InlineRateChip({ rate, fundingFactor, preview = false }: { rate: InlineRate; fundingFactor: number; preview?: boolean }) {
  return (
    <Tooltip delayDuration={150}>
      <TooltipTrigger asChild>
        <span className={cn("inline-flex min-w-0 max-w-full cursor-default items-center gap-1 rounded px-1.5 text-[11px]! ring-1 ring-inset transition-colors hover:bg-muted/60", preview ? "h-6" : "py-0.5", ratioTone(rate.effectiveRatio))}>
          <span data-rate-name className={cn("min-w-0 font-medium", preview ? "truncate" : "[overflow-wrap:anywhere]")}>{rate.model_name}</span>
          <span className="shrink-0 rounded bg-primary/10 px-1 font-semibold tabular-nums text-primary ring-1 ring-inset ring-primary/15">
            {formatRatio(rate.effectiveRatio)}
          </span>
        </span>
      </TooltipTrigger>
      <TooltipContent side="top" className="z-[80] max-w-xs text-xs">
        <p className="font-medium">{rate.model_name}</p>
        {Math.abs(fundingFactor - 1) > 0.000001 ? (
          <div className="mt-1 space-y-0.5 tabular-nums">
            <p>{`上游倍率：输入 ${formatRatio(rate.ratio)} / 输出 ${formatRatio(rate.completion_ratio > 0 ? rate.completion_ratio : rate.ratio)}`}</p>
            <p>{`充值换算：x ${formatRatio(fundingFactor)}`}</p>
            <p className="font-medium">{`有效倍率：输入 ${formatRatio(rate.effectiveRatio)} / 输出 ${formatRatio(rate.effectiveCompletionRatio)}`}</p>
          </div>
        ) : null}
        {rate.description ? <p className="mt-0.5 text-muted-foreground">{rate.description}</p> : <p className="mt-0.5 italic text-muted-foreground">{"(无描述)"}</p>}
        <p className="mt-0.5 text-muted-foreground">{`最近更新：${relativeTime(rate.last_seen_at)}`}</p>
      </TooltipContent>
    </Tooltip>
  )
}

/** Keep the table row fixed; overflow details live in a side popover. */
function InlineRates({ channel, compact = false }: { channel: Channel; compact?: boolean }) {
  const { data, loading } = useChannelRates(channel.id)
  const fundingFactor = channel.funding_factor > 0 ? channel.funding_factor : 1
  const rates: InlineRate[] = [...(data ?? [])]
    .map((rate) => ({
      ...rate,
      effectiveRatio: rate.ratio * fundingFactor,
      effectiveCompletionRatio: (rate.completion_ratio > 0 ? rate.completion_ratio : rate.ratio) * fundingFactor,
    }))
    .sort((a, b) => a.effectiveRatio - b.effectiveRatio)
  const [open, setOpen] = useState(false)
  const [hasOverflow, setHasOverflow] = useState(false)
  const chipBoxRef = useRef<HTMLDivElement>(null)
  const chipContentRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const element = chipBoxRef.current
    const content = chipContentRef.current
    if (!element || !content) return
    const check = () => {
      const overflow = content.scrollHeight > element.clientHeight + 1 ||
        Array.from(content.querySelectorAll<HTMLElement>("[data-rate-name]")).some(name => name.scrollWidth > name.clientWidth + 1)
      setHasOverflow(overflow)
      if (!overflow) setOpen(false)
    }
    check()
    const observer = new ResizeObserver(check)
    observer.observe(element)
    observer.observe(content)
    return () => observer.disconnect()
  }, [compact, data, fundingFactor, hasOverflow, loading])

  if (loading || rates.length === 0) return null

  const preview = (
    <div ref={chipBoxRef} className={cn("flex h-[52px] min-w-0 items-center overflow-hidden pr-7", compact && "2xl:h-20", hasOverflow && "items-start")}>
      <div ref={chipContentRef} className="flex w-full min-w-0 shrink-0 flex-wrap content-start gap-1">
        {rates.map((rate) => <InlineRateChip key={rate.id} rate={rate} fundingFactor={fundingFactor} preview />)}
      </div>
    </div>
  )

  if (!hasOverflow) {
    return <div className={cn("relative w-full max-w-full", !compact && "mt-3 border-t border-border pt-2.5")}>{preview}</div>
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button type="button" className={cn("relative block w-full max-w-full cursor-pointer text-left", !compact && "mt-3 border-t border-border pt-2.5")} aria-label={`查看 ${channel.name} 的全部分组`} aria-expanded={open}>
          {preview}
          <span className="pointer-events-none absolute bottom-0 right-0 inline-flex size-6 items-center justify-center rounded-md bg-background text-muted-foreground ring-1 ring-inset ring-border">
            <ChevronDown className={cn("size-3 transition-transform", open && "rotate-180")} />
          </span>
        </button>
      </PopoverTrigger>
      <PopoverContent side="bottom" align="start" sideOffset={6} collisionPadding={16} className="z-[60] w-[min(26rem,calc(100vw-2rem))] p-0">
        <div className="flex items-center justify-between gap-3 border-b border-border bg-muted/20 px-3 py-2.5">
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold" title={channel.name}>{channel.name}</div>
            <div className="text-[11px] text-muted-foreground">{"分组列表"}</div>
          </div>
          <Badge className="shrink-0" variant="outline">{`${rates.length} 个`}</Badge>
        </div>
        <ScrollArea className="h-72">
          <div className="flex flex-wrap content-start gap-1.5 p-3">
            {rates.map((rate) => <InlineRateChip key={rate.id} rate={rate} fundingFactor={fundingFactor} />)}
          </div>
        </ScrollArea>
      </PopoverContent>
    </Popover>
  )
}

interface ChannelGroupRow {
  key: string
  channel: Channel
  rate: RateSnapshot
}

function ChannelGroupsDialog({
  open,
  onOpenChange,
  channels,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  channels: Channel[]
}) {
  const [query, setQuery] = useState("")
  const [sortMode, setSortMode] = useState<GroupSortMode>("channel-asc")
  const [rows, setRows] = useState<ChannelGroupRow[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    let cancelled = false
    setLoading(true)
    setError(null)

    Promise.all(
      channels.map(async (channel) => {
        const rates = await apiFetch<RateSnapshot[]>(`/channels/${channel.id}/rates`)
        return { channel, rates }
      }),
    )
      .then((result) => {
        if (cancelled) return
        setRows(
          result.flatMap(({ channel, rates }) =>
            rates.map((rate) => ({
              key: `${channel.id}-${rate.id}`,
              channel,
              rate,
            })),
          ),
        )
      })
      .catch((e: Error) => {
        if (!cancelled) setError(e.message || "加载分组失败")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [open, channels])

  const filteredRows = useMemo(() => {
    const q = query.trim().toLowerCase()
    return rows
      .filter(({ channel, rate }) => {
        if (!q) return true
        return [
          channel.name,
          channelTypeLabel(channel.type),
          rate.model_name,
          rate.description ?? "",
          formatRatio(rate.ratio),
        ].some((value) => value.toLowerCase().includes(q))
      })
      .sort((a, b) => {
        if (sortMode === "ratio-asc" || sortMode === "ratio-desc") {
          const diff = a.rate.ratio - b.rate.ratio
          return sortMode === "ratio-asc" ? diff : -diff
        }
        const diff = a.channel.name.localeCompare(b.channel.name, "zh-CN")
          || a.rate.model_name.localeCompare(b.rate.model_name, "zh-CN")
        return sortMode === "channel-asc" ? diff : -diff
      })
  }, [query, rows, sortMode])

  const channelCount = new Set(rows.map((row) => row.channel.id)).size

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="grid h-[calc(100dvh-1rem)] w-[calc(100vw-1rem)] max-w-none grid-rows-[auto_auto_minmax(0,1fr)] overflow-hidden sm:h-[min(760px,calc(100dvh-3rem))] sm:w-[86vw] sm:max-w-[86vw]">
        <DialogHeader>
          <DialogTitle className="text-base font-medium">{"分组"}</DialogTitle>
          <DialogDescription className="text-xs">
            {loading ? "正在加载全部渠道分组" : `${rows.length} 个分组 · ${channelCount} 个渠道`}
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <div className="relative min-w-0 flex-1">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="搜索渠道、分组或倍率"
              className="h-9 pl-8 text-xs"
            />
          </div>
          <Select value={sortMode} onValueChange={(value) => setSortMode(value as GroupSortMode)}>
            <SelectTrigger className="h-9 w-full gap-2 text-xs sm:w-40">
              <ArrowUpDown className="size-4 text-muted-foreground" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent align="end">
              <SelectItem value="channel-asc">{"渠道 A-Z"}</SelectItem>
              <SelectItem value="channel-desc">{"渠道 Z-A"}</SelectItem>
              <SelectItem value="ratio-asc">{"倍率从低到高"}</SelectItem>
              <SelectItem value="ratio-desc">{"倍率从高到低"}</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div className="h-full min-h-0 min-w-0 overflow-auto rounded-md border">
          <Table className="min-w-[760px] text-xs">
            <TableHeader>
              <TableRow>
                <TableHead className="h-9 font-medium text-muted-foreground">{"渠道"}</TableHead>
                <TableHead className="h-9 font-medium text-muted-foreground">{"类型"}</TableHead>
                <TableHead className="h-9 font-medium text-muted-foreground">{"分组"}</TableHead>
                <TableHead className="h-9 text-right font-medium text-muted-foreground">{"倍率"}</TableHead>
                <TableHead className="h-9 text-right font-medium text-muted-foreground">{"更新"}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-xs text-muted-foreground">
                    {"加载中…"}
                  </TableCell>
                </TableRow>
              ) : error ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-xs text-danger">
                    {error}
                  </TableCell>
                </TableRow>
              ) : filteredRows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-xs text-muted-foreground">
                    {rows.length === 0 ? "暂无分组数据" : "没有匹配的分组"}
                  </TableCell>
                </TableRow>
              ) : (
                filteredRows.map(({ key, channel, rate }) => (
                  <TableRow key={key}>
                    <TableCell>{channel.name}</TableCell>
                    <TableCell>
                      <span
                        className={cn(
                          "inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-normal ring-1 ring-inset",
                          channel.type === "newapi"
                            ? "bg-brand/10 text-brand ring-brand/20"
                            : "bg-sky-500/10 text-sky-700 ring-sky-500/25 dark:text-sky-300",
                        )}
                      >
                        {channelTypeLabel(channel.type)}
                      </span>
                    </TableCell>
                    <TableCell className="min-w-60 whitespace-normal">
                      <div>{rate.model_name}</div>
                      {rate.description ? (
                        <div className="mt-0.5 line-clamp-2 text-xs text-muted-foreground">
                          {rate.description}
                        </div>
                      ) : null}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      <span className={cn("rounded px-1.5 py-0.5 ring-1 ring-inset", ratioTone(rate.ratio))}>
                        {formatRatio(rate.ratio)}
                      </span>
                    </TableCell>
                    <TableCell className="text-right text-muted-foreground">
                      {relativeTime(rate.last_seen_at)}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </DialogContent>
    </Dialog>
  )
}

interface ChannelSyncState {
  running: boolean
  events: ProgressEvent[]
  finalOk: boolean | null
}

function emptySyncState(): ChannelSyncState {
  return { running: false, events: [], finalOk: null }
}

interface BulkSyncState {
  running: boolean
  completed: number
  total: number
  events: ProgressEvent[]
  finalOk: boolean | null
}

type SyncAction = "sync" | "test-login"

type SyncDialogState =
  | { kind: "single"; channel: Channel; action: SyncAction }
  | { kind: "bulk" }

const stageLabel: Record<ProgressEvent["stage"], string> = {
  discovery: "API 地址",
  captcha: "打码",
  session: "会话",
  login: "登录",
  balance: "余额",
  cost: "消费",
  subscription: "订阅",
  rates: "倍率",
  api_keys: "受管 API Key",
  done: "完成",
  error: "失败",
}

const stageOrder: Record<ProgressEvent["stage"], number> = {
  discovery: 1,
  captcha: 2,
  session: 3,
  login: 4,
  balance: 5,
  cost: 6,
  subscription: 7,
  rates: 8,
  api_keys: 9,
  done: 10,
  error: 10,
}

/** 按 stage 去重，每个 stage 只留最后一条事件（"在做中→完成"会被覆盖成完成态）。 */
function deriveSteps(events: ProgressEvent[]): ProgressEvent[] {
  const byStage = new Map<ProgressEvent["stage"], ProgressEvent>()
  for (const ev of events) byStage.set(ev.stage, ev)
  return [...byStage.values()].sort((a, b) => stageOrder[a.stage] - stageOrder[b.stage])
}

function SyncEventList({ events, running }: { events: ProgressEvent[]; running: boolean }) {
  const steps = deriveSteps(events)

  return (
    <div>
      {steps.length === 0 ? (
        <div className="flex items-center gap-2 rounded-md border border-dashed border-border px-3 py-5 text-xs text-muted-foreground">
          {running ? <Loader2 className="size-3.5 shrink-0 animate-spin" /> : null}
          <span>{running ? "准备中…" : "暂无步骤记录"}</span>
        </div>
      ) : (
        <ul className="space-y-2">
          {steps.map((ev) => {
            // 终止态：stage=done 或 error；显式 ok=true / false 也算
            const failed = ev.stage === "error" || ev.ok === false
            const succeeded = ev.stage === "done" || ev.ok === true
            const running = !failed && !succeeded
            const Icon = running ? Loader2 : failed ? XCircle : CheckCircle2
            const tone = running ? "text-muted-foreground" : failed ? "text-danger" : "text-success"
            return (
              <li
                key={ev.stage}
                className="flex items-start gap-2 text-xs"
              >
                <Icon
                  className={cn("size-3.5 shrink-0", tone, running && "animate-spin")}
                />
                <span className="w-9 shrink-0 text-[11px] text-muted-foreground">
                  {stageLabel[ev.stage]}
                </span>
                <div className="min-w-0 flex-1 overflow-x-auto">
                  <span
                    className={cn(
                      "block whitespace-pre-wrap break-words",
                      failed ? "text-danger" : running ? "text-foreground/80" : "text-foreground",
                    )}
                  >
                    {ev.message}
                  </span>
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}

function syncStateLabel(state: ChannelSyncState) {
  if (state.running) return "处理中"
  if (state.finalOk === true) return "已完成"
  if (state.finalOk === false) return "失败"
  return "等待中"
}

function SyncProgressDialog({
  open,
  dialog,
  channels,
  syncState,
  bulkSync,
  onOpenChange,
}: {
  open: boolean
  dialog: SyncDialogState | null
  channels: Channel[]
  syncState: Record<number, ChannelSyncState>
  bulkSync: BulkSyncState
  onOpenChange: (open: boolean) => void
}) {
  if (!dialog) return null

  const isBulk = dialog.kind === "bulk"
  const singleState = isBulk ? null : syncState[dialog.channel.id] ?? emptySyncState()
  const busy = isBulk ? bulkSync.running : !!singleState?.running
  const latestSingleEvent = singleState?.events[singleState.events.length - 1]
  const failedBulkChannels = channels.filter((channel) => syncState[channel.id]?.finalOk === false).length
  const finishedBulkChannels = channels.filter((channel) => syncState[channel.id]?.finalOk != null).length
  const bulkTotal = bulkSync.total || channels.length
  const bulkCompleted = Math.max(bulkSync.completed, finishedBulkChannels)
  const bulkProgress = bulkTotal > 0 ? Math.min(100, (bulkCompleted / bulkTotal) * 100) : 0
  const singleActionLabel = !isBulk && dialog.action === "test-login" ? "测试登录" : "同步"
  const resultTone = isBulk
    ? failedBulkChannels > 0 || bulkSync.finalOk === false
      ? "danger"
      : !busy && bulkSync.finalOk === true
        ? "success"
        : "brand"
    : singleState?.finalOk === false
      ? "danger"
      : singleState?.finalOk === true
        ? "success"
        : "brand"
  const ResultIcon = resultTone === "danger" ? XCircle : resultTone === "success" ? CheckCircle2 : RefreshCw
  const resultIconClass = resultTone === "danger" ? "bg-danger/10 text-danger ring-danger/20" : resultTone === "success" ? "bg-success/10 text-success ring-success/20" : "bg-brand/10 text-brand ring-brand/20"

  return (
    <Dialog
      open={open}
      onOpenChange={(nextOpen) => {
        if (nextOpen || !busy) onOpenChange(nextOpen)
      }}
    >
      <DialogContent
        className={cn("sm:max-w-md", isBulk && "sm:max-w-lg")}
        showCloseButton={!busy}
      >
        <DialogHeader className="pr-7">
          <DialogTitle className="flex items-center gap-2 text-base">
            <span className={cn("flex size-8 shrink-0 items-center justify-center rounded-md ring-1 ring-inset", resultIconClass)}>
              <ResultIcon className={cn("size-4", busy && "animate-spin")} />
            </span>
            {isBulk ? "同步全部渠道" : `${singleActionLabel} · ${dialog.channel.name}`}
          </DialogTitle>
          <DialogDescription className="text-xs">
            {isBulk
              ? busy
                ? `正在按顺序处理 ${bulkTotal} 个渠道`
                : failedBulkChannels > 0
                  ? `${failedBulkChannels} 个渠道失败，请查看下方结果`
                  : bulkSync.finalOk === true && bulkCompleted >= bulkTotal
                    ? "全部渠道已完成同步"
                    : "同步已结束，部分渠道未返回终态"
              : busy
                ? `${dialog.channel.name} 正在${singleActionLabel.toLowerCase()}，请稍候`
                : singleState?.finalOk === false
                  ? "本次操作未成功，可关闭窗口后重试"
                  : "本次操作已完成"}
          </DialogDescription>
        </DialogHeader>

        {isBulk ? (
          <div className="space-y-3">
            <div className="rounded-lg border border-border bg-muted/20 px-3 py-3">
              <div className="flex items-center justify-between gap-3 text-xs">
                <span className="font-medium text-foreground">{busy ? "同步进度" : "同步结果"}</span>
                <span className="tabular-nums text-muted-foreground">{bulkCompleted} / {bulkTotal}</span>
              </div>
              <Progress className="mt-2 h-1.5" value={bulkProgress} />
            </div>
            <ScrollArea className="h-60 rounded-lg border border-border">
              <div className="divide-y divide-border">
                {channels.map((channel) => {
                  const state = syncState[channel.id] ?? emptySyncState()
                  const latest = state.events[state.events.length - 1]
                  const failed = state.finalOk === false
                  const succeeded = state.finalOk === true
                  const Icon = state.running ? Loader2 : failed ? XCircle : succeeded ? CheckCircle2 : RefreshCw
                  const iconTone = state.running ? "text-brand" : failed ? "text-danger" : succeeded ? "text-success" : "text-muted-foreground"
                  return (
                    <div key={channel.id} className="px-3 py-2.5">
                      <div className="flex items-center gap-2">
                        <Icon className={cn("size-3.5 shrink-0", iconTone, state.running && "animate-spin")} />
                        <span className="min-w-0 flex-1 truncate text-xs font-medium" title={channel.name}>{channel.name}</span>
                        <span className={cn("shrink-0 text-[11px]", failed ? "text-danger" : succeeded ? "text-success" : state.running ? "text-brand" : "text-muted-foreground")}>
                          {syncStateLabel(state)}
                        </span>
                      </div>
                      <p className={cn("mt-1 truncate pl-5 text-[11px]", failed ? "text-danger" : "text-muted-foreground")} title={latest?.message}>
                        {latest?.message ?? "等待进入同步队列"}
                      </p>
                    </div>
                  )
                })}
              </div>
            </ScrollArea>
            {!busy && bulkSync.events.length > 0 ? (
              <p className={cn("text-xs", bulkSync.finalOk === false || failedBulkChannels > 0 ? "text-danger" : "text-success")}>
                {bulkSync.events[bulkSync.events.length - 1]?.message}
              </p>
            ) : null}
          </div>
        ) : (
          <div className="space-y-3">
            <div className={cn("flex items-start gap-3 rounded-lg border px-3 py-3", singleState?.finalOk === false ? "border-danger/25 bg-danger/5" : singleState?.finalOk === true ? "border-success/25 bg-success/5" : "border-border bg-muted/20")}>
              {singleState?.running ? <Loader2 className="mt-0.5 size-4 shrink-0 animate-spin text-brand" /> : singleState?.finalOk === false ? <XCircle className="mt-0.5 size-4 shrink-0 text-danger" /> : singleState?.finalOk === true ? <CheckCircle2 className="mt-0.5 size-4 shrink-0 text-success" /> : <RefreshCw className="mt-0.5 size-4 shrink-0 text-brand" />}
              <div className="min-w-0">
                <p className="text-xs font-medium">{singleState?.running ? `正在${singleActionLabel}` : singleState?.finalOk === false ? `${singleActionLabel}失败` : singleState?.finalOk === true ? `${singleActionLabel}完成` : `准备${singleActionLabel}`}</p>
                <p className={cn("mt-1 break-words text-[11px]", singleState?.finalOk === false ? "text-danger" : "text-muted-foreground")}>
                  {latestSingleEvent?.message ?? "正在建立连接…"}
                </p>
              </div>
            </div>
            <div className="rounded-lg border border-border">
              <div className="border-b border-border bg-muted/20 px-3 py-2 text-xs font-medium">{"处理步骤"}</div>
              <ScrollArea className="h-52 px-3 py-3">
                <SyncEventList events={singleState?.events ?? []} running={!!singleState?.running} />
              </ScrollArea>
            </div>
          </div>
        )}

        {!busy ? (
          <DialogFooter>
            <Button type="button" size="sm" onClick={() => onOpenChange(false)}>
              {isBulk && (failedBulkChannels > 0 || bulkSync.finalOk === false) ? "关闭" : "完成"}
            </Button>
          </DialogFooter>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}

export function ChannelCards() {
  const { data: channels, loading: channelsLoading, refreshing: channelsRefreshing, setData: setChannels } = useChannels()
  const refresh = useTriggerRefresh()
  const { confirm, dialog: confirmDialog } = useConfirm()
  const [editing, setEditing] = useState<Channel | null>(null)
  const [creating, setCreating] = useState(false)
  const [groupsOpen, setGroupsOpen] = useState(false)
  const [redeeming, setRedeeming] = useState<Channel | null>(null)
  const [recharging, setRecharging] = useState<Channel | null>(null)
  const [costing, setCosting] = useState<Channel | null>(null)
  const [managingKeys, setManagingKeys] = useState<Channel | null>(null)
  const [deletionReviewChannel, setDeletionReviewChannel] = useState<Channel | null>(null)
  const [errorChannel, setErrorChannel] = useState<Channel | null>(null)
  const errorTriggerRef = useRef<HTMLButtonElement | null>(null)
  const [channelSearch, setChannelSearch] = useState("")
  const [typeFilter, setTypeFilter] = useState<ChannelTypeFilter>("all")
  const [statusFilter, setStatusFilter] = useState<ChannelStatusFilter>("all")
  const [busyAction, setBusyAction] = useState<string | null>(null)
  const busyActionRef = useRef<string | null>(null)
  const [syncDialog, setSyncDialog] = useState<SyncDialogState | null>(null)
  // 每个渠道当前 sync 进度 + 历史事件，事件只在同步弹窗内展示。
  const [syncState, setSyncState] = useState<Record<number, ChannelSyncState>>({})
  const [bulkSync, setBulkSync] = useState<BulkSyncState>({ running: false, completed: 0, total: 0, events: [], finalOk: null })
  const syncRunRef = useRef(false)
  const anySyncRunning = bulkSync.running || Object.values(syncState).some((s) => s.running)
  const visibleChannels = useMemo(
    () => [...(channels ?? [])].sort((a, b) =>
      channelRequestCount(b) - channelRequestCount(a)
      || (b.sort_order ?? 0) - (a.sort_order ?? 0)
      || a.id - b.id,
    ),
    [channels],
  )
  const allChannels = useMemo(() => channels ?? [], [channels])
  const hasDeletionPending = allChannels.some(isChannelDeletionPending)
  const totalChannels = visibleChannels.length
  const normalizedChannelSearch = channelSearch.trim().toLocaleLowerCase()
  const statusCountChannels = useMemo(
    () => visibleChannels.filter((channel) => {
      const matchesSearch = !normalizedChannelSearch || [
        channel.name,
        channel.credential_mode === "direct" ? "直连 API" : channelTypeLabel(channel.type),
      ].some((value) => value.toLocaleLowerCase().includes(normalizedChannelSearch))
      const matchesType = typeFilter === "all"
        || (typeFilter === "direct"
          ? channel.credential_mode === "direct"
          : channel.type === typeFilter && channel.credential_mode !== "direct")
      return matchesSearch && matchesType
    }),
    [normalizedChannelSearch, typeFilter, visibleChannels],
  )
  const filteredChannels = useMemo(
    () => statusCountChannels.filter((channel) => matchesChannelStatusFilter(channel, statusFilter)),
    [statusCountChannels, statusFilter],
  )
  const statusFilterCounts = useMemo(() => ({
    all: statusCountChannels.length,
    healthy: statusCountChannels.filter((channel) => statusOf(channel) === "healthy").length,
    failed: statusCountChannels.filter((channel) => statusOf(channel) === "failed").length,
    low_balance: statusCountChannels.filter((channel) => isBalanceRiskStatus(statusOf(channel))).length,
    needs_funding: statusCountChannels.filter((channel) => (channel.pending_funding_count ?? 0) > 0).length,
    accounting_review: statusCountChannels.filter((channel) => channelAccountingNeedsReview(channel)).length,
    paused: statusCountChannels.filter((channel) => !channel.monitor_enabled).length,
    idle: statusCountChannels.filter((channel) => statusOf(channel) === "idle").length,
    deletion_pending: statusCountChannels.filter((channel) => statusOf(channel) === "deletion_pending").length,
  }), [statusCountChannels])
  const filtersActive = normalizedChannelSearch !== "" || typeFilter !== "all" || statusFilter !== "all"
  const healthyChannels = allChannels.filter((item) => statusOf(item) === "healthy").length
  const lowBalanceChannels = allChannels.filter((item) => isBalanceRiskStatus(statusOf(item))).length
  const failedChannels = allChannels.filter((item) => statusOf(item) === "failed").length
  const pausedChannels = allChannels.filter((item) => !item.monitor_enabled).length
  const pendingFunding = allChannels.reduce((sum, item) => sum + (item.pending_funding_count ?? 0), 0)

  function focusNextIssue() {
    const target = visibleChannels.find((item) => statusOf(item) === "failed")
      ?? visibleChannels.find((item) => isBalanceRiskStatus(statusOf(item)))
    if (!target) return
    document.getElementById(`channel-row-${target.id}`)?.scrollIntoView({ behavior: "smooth", block: "center" })
  }

  function patchSync(id: number, fn: (prev: ChannelSyncState) => ChannelSyncState) {
    setSyncState((s) => ({ ...s, [id]: fn(s[id] ?? emptySyncState()) }))
  }

  async function startStream(channel: Channel, action: SyncAction) {
    if (syncRunRef.current || anySyncRunning) return
    syncRunRef.current = true
    setSyncDialog({ kind: "single", channel, action })
    patchSync(channel.id, () => ({
      running: true,
      events: [],
      finalOk: null,
    }))
    let sawError = false
    const stream = action === "sync" ? syncChannelStream : testLoginStream
    try {
      await stream(channel.id, {
        onEvent: (ev) => {
          if (ev.stage === "error" || ev.ok === false) sawError = true
          patchSync(channel.id, (prev) => ({
            ...prev,
            events: [...prev.events, ev],
          }))
        },
      })
      const ok = !sawError
      patchSync(channel.id, (prev) => ({
        ...prev,
        running: false,
        finalOk: ok,
      }))
    } catch (e) {
      const err = e as Error
      const failureLabel = action === "sync" ? "同步失败" : "测试登录失败"
      patchSync(channel.id, (prev) => ({
        ...prev,
        running: false,
        finalOk: false,
        events: [...prev.events, {
          stage: "error",
          message: err.message || failureLabel,
          time: new Date().toISOString(),
        }],
      }))
    } finally {
      syncRunRef.current = false
      refresh()
    }
  }

  async function startBulkSync() {
    const list = channels ?? []
    if (list.length === 0 || hasDeletionPending || syncRunRef.current || anySyncRunning) return
    syncRunRef.current = true
    setSyncDialog({ kind: "bulk" })

    setSyncState((current) => {
      const next = { ...current }
      for (const channel of list) {
        next[channel.id] = { running: true, events: [], finalOk: null }
      }
      return next
    })

    setBulkSync({ running: true, completed: 0, total: list.length, events: [], finalOk: null })
    try {
      await syncAllChannelsStream({
        onEvent: (ev) => {
          if (ev.channel_id != null) {
            patchSync(ev.channel_id, (prev) => ({
              ...prev,
              events: [...prev.events, ev],
              running: ev.stage !== "done" && ev.stage !== "error",
              finalOk: ev.stage === "done" ? true : ev.stage === "error" ? false : prev.finalOk,
            }))
          }

          setBulkSync((prev) => {
            let next = prev
            if (ev.index != null && ev.total != null) {
              const completed = ev.stage === "done" || ev.stage === "error"
                ? Math.max(next.completed, ev.index)
                : next.completed
              next = {
                ...next,
                completed,
                total: ev.total,
              }
            }
            if (ev.channel_id == null) {
              next = {
                ...next,
                events: [...next.events, ev],
                finalOk: ev.stage === "done" ? true : ev.stage === "error" ? false : next.finalOk,
              }
            }
            return next
          })
        },
      })
    } catch (e) {
      const err = e as Error
      setBulkSync((prev) => ({
        ...prev,
        finalOk: false,
        events: [...prev.events, { stage: "error", message: err.message || "批量同步失败", time: new Date().toISOString() }],
      }))
    } finally {
      setSyncState((s) => {
        const next: Record<number, ChannelSyncState> = {}
        for (const [id, state] of Object.entries(s)) {
          next[Number(id)] = { ...state, running: false }
        }
        return next
      })
      setBulkSync((prev) => ({ ...prev, running: false }))
      syncRunRef.current = false
      refresh()
    }
  }

  async function withBusy(key: string, fn: () => Promise<unknown>) {
    if (busyActionRef.current) return
    busyActionRef.current = key
    setBusyAction(key)
    try {
      await fn()
      refresh()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "操作失败")
    } finally {
      busyActionRef.current = null
      setBusyAction(null)
    }
  }

  function renderRedeemSummary(result: ChannelRedeemResult) {
    if (result.type === "subscription") {
      const group = result.group_name ? ` · ${result.group_name}` : ""
      const days = result.validity_days ? ` · ${result.validity_days} 天` : ""
      return `${result.message || "兑换成功"}${group}${days}`
    }
    if (result.type === "concurrency") {
      const extra = result.new_concurrency != null ? ` · 当前并发 ${result.new_concurrency}` : ""
      return `${result.message || "兑换成功"}${extra}`
    }
    const extra = result.new_balance != null ? ` · 当前余额 ${money(result.new_balance)}` : ""
    return `${result.message || "兑换成功"}${extra}`
  }

  return (
    <section className="space-y-6">
      <PageHeader icon={WalletCards} title="渠道管理" description="集中查看上游健康、余额、分组倍率和同步状态。" />

      <div className="metric-strip grid gap-4 sm:grid-cols-2 xl:grid-cols-5">
          <ChannelHeaderMetric
            label="渠道总数"
            value={channelsLoading && totalChannels === 0 ? "加载中" : `${totalChannels}`}
            hint="当前显示全部渠道"
            icon={Tags}
            tone="brand"
          />
          <ChannelHeaderMetric
            label="健康渠道"
            value={channelsLoading ? "加载中" : `${healthyChannels}`}
            hint="余额正常且最近无错误"
            icon={CheckCircle2}
            tone="success"
          />
          <ChannelHeaderMetric
            label="低余额 / 失败"
            value={channelsLoading ? "加载中" : `${lowBalanceChannels} / ${failedChannels}`}
            hint={failedChannels > 0 ? "存在登录或采集异常" : lowBalanceChannels > 0 ? "存在余额低于阈值" : "暂无风险渠道"}
            icon={AlertTriangle}
            tone={failedChannels > 0 ? "danger" : lowBalanceChannels > 0 ? "warning" : "success"}
            onClick={failedChannels > 0 || lowBalanceChannels > 0 ? focusNextIssue : undefined}
          />
          <ChannelHeaderMetric
            label="暂停监控"
            value={channelsLoading ? "加载中" : `${pausedChannels}`}
            hint={pausedChannels > 0 ? "暂停渠道不会参与监控" : "全部渠道正常监控"}
            icon={Pause}
            tone="warning"
          />
          <ChannelHeaderMetric
            label="待确认充值"
            value={channelsLoading ? "加载中" : `${pendingFunding}`}
            hint={pendingFunding > 0 ? "需要补充实际付款金额" : "成本账本已核对"}
            icon={BadgeDollarSign}
            tone={pendingFunding > 0 ? "warning" : "success"}
          />
      </div>

      {channelsLoading && channels == null ? (
        <p className="rounded-xl border border-dashed border-border bg-card px-4 py-12 text-center text-sm text-muted-foreground shadow-sm">
          {"加载中…"}
        </p>
      ) : totalChannels === 0 ? (
        <div className="rounded-xl border border-dashed border-border bg-card px-6 py-8 text-center shadow-sm">
          <span className="mx-auto flex size-14 items-center justify-center rounded-xl bg-muted text-muted-foreground">
            <Tags className="size-6" />
          </span>
          <p className="mt-4 text-base font-medium text-foreground">{"还没有任何渠道。"}</p>
          <p className="mt-1 text-sm text-muted-foreground">{"添加一个上游渠道后即可开始管理余额、费用和分组。"}</p>
          <Button
            size="sm"
            className="mt-5 gap-1.5 rounded-lg px-4"
            onClick={() => {
              setEditing(null)
              setCreating(true)
            }}
          >
            <Plus className="size-3.5" />
            {"添加第一个渠道"}
          </Button>
        </div>
      ) : (
        <div className="overflow-hidden rounded-xl border border-border bg-card shadow-sm">
          <div className="border-b border-border bg-muted/40 px-5 py-5 sm:px-6 dark:bg-card">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
            <div className="min-w-0" title="按调用量排序，集中查看每个上游的运行状态和成本">
              <div className="flex items-center gap-2">
                <h2 className="text-lg font-semibold tracking-tight text-foreground">{"渠道列表"}</h2>
                <span className="rounded-md bg-muted px-2 py-0.5 text-[11px] font-medium tabular-nums text-muted-foreground">{totalChannels}</span>
              </div>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                className="gap-1.5 text-xs"
                disabled={channelsLoading || channelsRefreshing || anySyncRunning}
                onClick={refresh}
              >
                <RefreshCw className={cn("size-3.5", (channelsLoading || channelsRefreshing) && "animate-spin")} />
                {channelsRefreshing ? "刷新中" : "刷新"}
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="gap-1.5 text-xs"
                disabled={anySyncRunning || hasDeletionPending}
                title={hasDeletionPending ? "待删除渠道完成远端清理前不可批量同步" : "同步全部渠道"}
                onClick={() => void startBulkSync()}
              >
                <RefreshCw className={cn("size-3.5", bulkSync.running && "animate-spin")} />
                {bulkSync.running ? `同步中 ${bulkSync.completed}/${bulkSync.total}` : "同步全部"}
              </Button>
              <Button
                variant="outline"
                size="sm"
                className="gap-1.5 text-xs"
                disabled={channelsLoading || totalChannels === 0}
                onClick={() => setGroupsOpen(true)}
              >
                <Tags className="size-3.5" />
                {"分组"}
              </Button>
              <Button
                size="sm"
                className="gap-1.5 text-xs"
                onClick={() => {
                  setEditing(null)
                  setCreating(true)
                }}
              >
                <Plus className="size-3.5" />
                {"新增"}
              </Button>
            </div>
          </div>
          </div>
          <div className="border-b border-brand/10 bg-brand/5 px-5 py-3 sm:px-6">
            <div className="flex flex-col gap-2 lg:flex-row lg:items-center">
              <div className="relative min-w-52 flex-1 lg:max-w-sm">
                <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
                <Input
                  value={channelSearch}
                  onChange={(event) => setChannelSearch(event.target.value)}
                  placeholder="搜索渠道名称"
                  aria-label="搜索渠道名称"
                  className="h-8 bg-background pl-8 text-xs"
                />
              </div>
              <div className="grid grid-cols-2 gap-2 sm:flex sm:flex-wrap sm:items-center">
                <Select value={typeFilter} onValueChange={(value) => setTypeFilter(value as ChannelTypeFilter)}>
                  <SelectTrigger size="sm" className="h-8 w-full bg-background text-xs sm:w-32" aria-label="筛选上游类型">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent align="start">
                    <SelectItem value="all">{"全部类型"}</SelectItem>
                    <SelectItem value="direct">{"直连 API"}</SelectItem>
                    <SelectItem value="newapi">{"NewAPI"}</SelectItem>
                    <SelectItem value="sub2api">{"Sub2API"}</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={statusFilter} onValueChange={(value) => setStatusFilter(value as ChannelStatusFilter)}>
                  <SelectTrigger size="sm" className="h-8 w-full bg-background text-xs sm:w-36" aria-label="筛选渠道状态">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent align="start">
                    {channelStatusFilterOptions.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {`${option.label} (${statusFilterCounts[option.value]})`}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="flex min-h-8 items-center justify-between gap-2 text-xs text-muted-foreground lg:ml-auto lg:justify-end">
                <span className="tabular-nums">{filtersActive ? `显示 ${filteredChannels.length} / ${totalChannels}` : `${totalChannels} 个渠道`}</span>
                {filtersActive ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-8 gap-1 px-2 text-xs"
                    onClick={() => {
                      setChannelSearch("")
                      setTypeFilter("all")
                      setStatusFilter("all")
                    }}
                  >
                    <X className="size-3.5" />
                    {"清除"}
                  </Button>
                ) : null}
              </div>
            </div>
          </div>
          <div className="overflow-x-auto">
            <Table noScroll className="min-w-[960px] w-full table-fixed">
              <TableHeader className="bg-muted/40 dark:bg-muted/40">
                <TableRow className="bg-muted/40 hover:bg-muted/40 dark:bg-muted/40 dark:hover:bg-muted/40">
                  <TableHead className="h-10 w-[19%] 2xl:w-[18%] px-2 text-left text-xs font-semibold text-muted-foreground">{"渠道 / 协议"}</TableHead>
                  <TableHead className="h-10 w-[10%] 2xl:w-[8%] px-2 text-center text-xs font-semibold text-muted-foreground">{"状态"}</TableHead>
                  <TableHead className="h-10 w-[14%] 2xl:w-[13%] px-2 text-center text-xs font-semibold text-muted-foreground">{"余额 / 消费"}</TableHead>
                  <TableHead className="h-10 w-[9%] px-2 text-center text-xs font-semibold text-muted-foreground">{"充值成本"}</TableHead>
                  <TableHead className="h-10 w-[9%] px-2 text-center text-xs font-semibold text-muted-foreground">{"订阅"}</TableHead>
                  <TableHead className="h-10 w-[16%] 2xl:w-[18%] px-2 text-center text-xs font-semibold text-muted-foreground">{"分组 / 倍率"}</TableHead>
                  <TableHead className="h-10 w-[8%] 2xl:w-[9%] px-2 text-center text-xs font-semibold text-muted-foreground">{"调用 / 更新"}</TableHead>
                  <TableHead className="h-10 w-[15%] 2xl:w-[16%] px-2 text-center text-xs font-semibold text-muted-foreground">{"操作"}</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredChannels.map((c) => {
                  const isDirect = c.credential_mode === "direct"
                  const status = statusOf(c)
                  const meta = status === "deletion_pending"
                    ? { ...statusMap[status], label: channelDeletionStatusLabel(c.deletion_state) }
                    : statusMap[status]
                  const sync = syncState[c.id] ?? emptySyncState()
                  const accountConcurrency = isDirect
                    ? null
                    : channelAccountConcurrencyPresentation(c.upstream_account_concurrency)
                  const deletionPending = isChannelDeletionPending(c)
                  const accountingAttention = !isDirect && (channelAccountingNeedsReview(c) || channelAccountingHasError(c))
                  return (
                    <TableRow id={`channel-row-${c.id}`} key={c.id} className="hover:bg-muted/30">
                      <TableCell className="whitespace-normal px-3 py-3 text-left align-middle">
                        <div className="min-w-0">
                          <div className="min-w-0">
                            <div className="flex min-w-0 items-center gap-1">
                              <button type="button" className="min-w-0 truncate text-base font-semibold leading-6 text-foreground hover:text-brand hover:underline disabled:no-underline" title={`编辑 ${c.name}`} aria-label={`编辑 ${c.name}`} disabled={deletionPending} onClick={() => { setEditing(c); setCreating(true) }}>{c.name}</button>
                              <Tooltip delayDuration={150}>
                                <TooltipTrigger asChild>
                                  <Button asChild variant="ghost" size="icon-sm" className="size-6 shrink-0 text-muted-foreground hover:text-foreground">
                                    <a href={c.site_url} target="_blank" rel="noopener noreferrer" aria-label={`新窗口打开 ${c.name} 站点地址`}>
                                      <ExternalLink className="size-3" />
                                    </a>
                                  </Button>
                                </TooltipTrigger>
                                <TooltipContent side="top" className="text-xs">{"新窗口打开站点地址"}</TooltipContent>
                              </Tooltip>
                            </div>
                          </div>
                        </div>
                        <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1">
                          <div className="truncate text-xs font-medium text-foreground">{gatewayAppLabel(c.gateway_app)}</div>
                          <div>
                            <span className={cn(
                              "inline-flex shrink-0 items-center rounded px-1.5 py-0.5 text-[10px] font-medium leading-4 ring-1 ring-inset",
                              isDirect
                                ? "bg-muted text-foreground ring-border"
                                : c.type === "newapi"
                                  ? "bg-brand/10 text-brand ring-brand/20"
                                  : "bg-sky-500/10 text-sky-700 ring-sky-500/25 dark:text-sky-300",
                            )}>{isDirect ? "直连 API" : channelTypeLabel(c.type)}</span>
                          </div>
                          {accountConcurrency ? <span className="truncate text-[11px] text-muted-foreground" tabIndex={0} title={`${accountConcurrency.label}；同一账号共享此额度，并非实时占用；${accountConcurrency.sourceLabel} · ${relativeTime(accountConcurrency.observedAt)}`}>{accountConcurrency.label}</span> : null}
                        </div>
                      </TableCell>
                      <TableCell className="whitespace-normal px-2 py-3 text-center align-middle">
                        <div className="space-y-1.5">
                          {!isDirect && channelAccountingHasError(c) && !deletionPending ? (
                            <button type="button" className="inline-flex items-center rounded-full bg-danger/10 px-1.5 py-0.5 text-[10px] font-medium text-danger ring-1 ring-inset ring-danger/20" onClick={() => setCosting(c)}>
                              {"账务异常"}
                            </button>
                          ) : status === "failed" ? (
                            c.last_error ? (
                              <ChannelErrorBadge
                                channel={c}
                                onClick={(trigger) => {
                                  errorTriggerRef.current = trigger
                                  setErrorChannel(c)
                                }}
                              />
                            ) : (
                              <span className={cn("inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium ring-1 ring-inset", meta.cls)}>{meta.label}</span>
                            )
                          ) : (
                            <span className={cn("inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium ring-1 ring-inset", meta.cls)}>{meta.label}</span>
                          )}
                          {!isDirect ? <BalanceObservationErrorBadge channel={c} /> : null}
                          {!isDirect ? <AccountBalanceInsufficientBadge channel={c} /> : null}
                          {deletionPending ? <div><ChannelDeletionReviewBadge channel={c} onClick={() => setDeletionReviewChannel(c)} /></div> : null}
                          {!c.monitor_enabled ? <div><span className="inline-flex rounded-full bg-amber-50 px-1.5 py-0.5 text-[10px] font-medium leading-4 text-amber-700 ring-1 ring-inset ring-amber-200 dark:bg-warning/10 dark:text-warning dark:ring-warning/20">{"暂停监控"}</span></div> : null}
                          <div className="text-[10px] text-muted-foreground">{isDirect ? "不采集余额" : c.balance_threshold > 0 ? `阈值 ${money(c.balance_threshold)}` : "无阈值"}</div>
                        </div>
                      </TableCell>
                      <TableCell className="whitespace-normal px-2 py-3 text-center align-middle">
                        {isDirect ? (
                          <span className="text-xs text-muted-foreground">{"仅记录网关用量"}</span>
                        ) : (
                        <dl className="mx-auto grid gap-1.5 text-xs leading-4" title={`累计消费 ${money(c.total_cost)}`}>
                          <div className="flex items-center justify-center gap-2">
                            <dt className="shrink-0 text-muted-foreground">{"余额"}</dt>
                            <Tooltip delayDuration={150}>
                              <TooltipTrigger asChild><dd className="truncate font-semibold tabular-nums">{money(c.last_balance)}</dd></TooltipTrigger>
                              <TooltipContent side="top" className="max-w-80 space-y-1 text-xs">
                                <p>{(c.raw_last_balance ?? c.last_balance) != null ? `上游原始余额 ${decimal(c.raw_last_balance ?? c.last_balance, 4)}` : "上游原始余额未采集"}</p>
                                {balanceObservationError(c) ? <p className="text-warning">{"最近一次余额采集失败；现有余额为上次可信快照。"}</p> : null}
                              </TooltipContent>
                            </Tooltip>
                          </div>
                          <div className="flex items-center justify-center gap-2">
                            <dt className="shrink-0 text-muted-foreground">{"今日"}</dt>
                            <dd className="truncate font-medium tabular-nums">{money(currentDayCost(c.today_cost, c.today_cost_at, c.today_cost_is_today))}</dd>
                          </div>
                        </dl>
                        )}
                      </TableCell>
                      <TableCell className="whitespace-normal px-2 py-3 text-center align-middle">
                        {isDirect ? (
                          <div className="mx-auto flex h-12 w-full flex-col justify-center text-center">
                            <span className="text-[11px] font-semibold tabular-nums">{"×1"}</span>
                            <span className="mt-0.5 text-[10px] text-muted-foreground">{"直连固定"}</span>
                          </div>
                        ) : (
                        <button type="button" disabled={deletionPending} className={cn("mx-auto flex h-12 w-full flex-col justify-center rounded-lg px-1 py-1 text-center transition-colors hover:bg-muted/40 disabled:cursor-not-allowed disabled:opacity-50", accountingAttention || c.pending_funding_count > 0 ? "bg-warning/5" : "")} onClick={() => setCosting(c)}>
                          <span className="flex items-center justify-center gap-1 text-[11px] font-semibold tabular-nums"><BadgeDollarSign className="size-3 text-primary" />{`×${decimal(c.funding_factor ?? 1, 4)}`}</span>
                          <span className={cn("mt-0.5 block truncate text-[10px]", accountingAttention || c.pending_funding_count > 0 ? "text-warning" : "text-muted-foreground")}>{channelAccountingHasError(c) ? "账务异常" : channelAccountingNeedsReview(c) ? "账务待复核" : c.pending_funding_count > 0 ? `待确认 ${c.pending_funding_count}` : c.funding_factor_source === "legacy_unknown" ? "待校准" : "已核对"}</span>
                        </button>
                        )}
                      </TableCell>
                      <TableCell className="whitespace-normal px-2 py-3 text-center align-middle"><ChannelSubscriptionUsageMetricTiles channel={c} compact /></TableCell>
                      <TableCell className="whitespace-normal px-2 py-3 text-center align-middle"><div className="flex justify-center"><InlineRates channel={c} compact /></div></TableCell>
                      <TableCell className="px-2 py-3 text-center align-middle"><div className="text-sm font-medium tabular-nums" title="累计请求">{compactCount(channelRequestCount(c))}</div><div className="mt-1 truncate text-xs text-muted-foreground" title="余额更新">{relativeTime(c.last_balance_at ?? c.updated_at)}</div></TableCell>
                      <TableCell className="whitespace-normal px-2 py-3 text-center align-middle">
                        <div className="mx-auto grid w-fit grid-cols-3 gap-1 min-[1800px]:grid-cols-5" role="group" aria-label={`${c.name} 的操作`}>
                          <Button type="button" variant="outline" size="icon-sm" className="size-9" title={`同步 ${c.name}`} aria-label={`同步 ${c.name}`} disabled={deletionPending || !!sync.running || anySyncRunning} onClick={() => void startStream(c, "sync")}>
                            <RefreshCw className={cn("size-3.5", sync.running && "animate-spin")} />
                          </Button>
                          <Button type="button" variant="outline" size="icon-sm" className="size-9" title={`编辑 ${c.name}`} aria-label={`编辑渠道 ${c.name}`} disabled={deletionPending} onClick={() => { setEditing(c); setCreating(true) }}><Pencil className="size-3.5" /></Button>
                          <Button type="button" variant="outline" size="icon-sm" className="size-9" title={`${isDirect ? "测试直连 API" : "测试登录"} ${c.name}`} aria-label={`${isDirect ? "测试直连 API" : "测试登录"} ${c.name}`} disabled={deletionPending || !!sync.running || anySyncRunning} onClick={() => void startStream(c, "test-login")}><LogIn className="size-3.5" /></Button>
                          <Button type="button" variant="outline" size="icon-sm" className="size-9" title={`${c.monitor_enabled ? "暂停监控" : "恢复监控"} ${c.name}`} aria-label={`${c.monitor_enabled ? "暂停监控" : "恢复监控"} ${c.name}`} disabled={deletionPending || busyAction !== null} onClick={() => void withBusy(`toggle-${c.id}`, () => apiFetch(`/channels/${c.id}/${c.monitor_enabled ? "disable" : "enable"}`, { method: "POST", body: JSON.stringify({ expected_admin_revision: c.admin_revision }) }))}>{busyAction === `toggle-${c.id}` ? <Loader2 className="size-3.5 animate-spin" /> : c.monitor_enabled ? <Pause className="size-3.5" /> : <Play className="size-3.5" />}</Button>
                              {!isDirect ? <>
                                <Button type="button" variant="outline" size="icon-sm" className="size-9" title={`充值 / 订阅 ${c.name}`} aria-label={`充值 / 订阅 ${c.name}`} disabled={deletionPending} onClick={() => setRecharging(c)}><CreditCard className="size-3.5" /></Button>
                                <Button type="button" variant="outline" size="icon-sm" className="size-9" title={`兑换码 ${c.name}`} aria-label={`兑换码 ${c.name}`} disabled={deletionPending} onClick={() => setRedeeming(c)}><Gift className="size-3.5" /></Button>
                                <Button type="button" variant="outline" size="icon-sm" className="size-9" title={`管理渠道密钥 ${c.name}`} aria-label={`管理渠道密钥 ${c.name}`} disabled={deletionPending} onClick={() => setManagingKeys(c)}><KeyRound className="size-3.5" /></Button>
                              </> : null}
                              <Button type="button" variant="outline" size="icon-sm" className="size-9 text-muted-foreground hover:text-destructive" title={`${isDirect ? "清空直连 API Key" : "清空登录信息"} ${c.name}`} aria-label={`${isDirect ? "清空直连 API Key" : "清空登录信息"} ${c.name}`} disabled={deletionPending || busyAction !== null} onClick={async () => {
                                const ok = await confirm({ title: isDirect ? `清空 ${c.name} 的直连 API Key？` : `清空 ${c.name} 的登录信息？`, description: isDirect ? "将删除已保存的直连 API Key 并立即停止该渠道路由；重新填写 Key 并同步后才能恢复。" : "将清空缓存会话；Token 模式还会清空已保存的 Access Token、Refresh Token 和 NewAPI Cookie。账号密码本身不会删除。", confirmLabel: "清空", destructive: true })
                                if (!ok) return
                                void withBusy(`clear-login-${c.id}`, async () => { await apiFetch(`/channels/${c.id}/clear-login-info`, { method: "POST" }); toast.success(isDirect ? "已清空直连 API Key" : "已清空登录信息") })
                              }}>{busyAction === `clear-login-${c.id}` ? <Loader2 className="size-3.5 animate-spin" /> : <XCircle className="size-3.5" />}</Button>
                              <Button type="button" variant="outline" size="icon-sm" className={cn("size-9", deletionPending ? "text-warning" : "text-destructive hover:text-destructive")} title={`${deletionPending ? "查看删除复核" : "删除渠道"} ${c.name}`} aria-label={`${deletionPending ? "查看删除复核" : "删除渠道"} ${c.name}`} disabled={busyAction !== null} onClick={async () => {
                                if (deletionPending) {
                                  setDeletionReviewChannel(c)
                                  return
                                }
                                const ok = await confirm({ title: `删除渠道 ${c.name}？`, description: "删除请求会先准备本地删除状态并保留渠道、登录凭据和审计证据，待远端受管密钥完成核验和清理后才会物理删除。", confirmLabel: "准备删除", destructive: true })
                                if (!ok) return
                                void withBusy(`delete-${c.id}`, async () => {
                                  const result = await apiFetch<ChannelDeletionAccepted>(`/channels/${c.id}`, { method: "DELETE" })
                                  setChannels((current) => markChannelDeletionPending(current ?? [], result))
                                  setDeletionReviewChannel(c)
                                  toast.warning(result.message || "本地删除已准备，远端清理尚未完成。")
                                })
                              }}>{busyAction === `delete-${c.id}` ? <Loader2 className="size-3.5 animate-spin" /> : deletionPending ? <AlertTriangle className="size-3.5" /> : <Trash2 className="size-3.5" />}</Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })}
                {filteredChannels.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={8} className="h-28 px-4 text-center text-sm text-muted-foreground">
                      {"当前筛选没有匹配渠道"}
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
          </div>
        </div>
      )}

      <ChannelFormDialog
        open={creating}
        onOpenChange={(v) => {
          setCreating(v)
          if (!v) setEditing(null)
        }}
        channel={editing}
      />

      <ChannelGroupsDialog
        open={groupsOpen}
        onOpenChange={setGroupsOpen}
        channels={channels ?? []}
      />

      <ChannelRedeemDialog
        open={redeeming != null}
        onOpenChange={(v) => {
          if (!v) setRedeeming(null)
        }}
        channel={redeeming}
        onSuccess={(result) => {
          toast.success(renderRedeemSummary(result))
        }}
      />

      <ChannelRechargeDialog
        open={recharging != null}
        onOpenChange={(v) => {
          if (!v) setRecharging(null)
        }}
        channel={recharging}
      />

      <ChannelCostDialog
        open={costing != null}
        onOpenChange={(v) => {
          if (!v) setCosting(null)
        }}
        channel={costing}
      />

      <ChannelAPIKeysDialog
        open={managingKeys != null}
        onOpenChange={(v) => {
          if (!v) setManagingKeys(null)
        }}
        channel={managingKeys}
      />

      <ChannelDeletionReviewDialog
        channel={deletionReviewChannel}
        onOpenChange={(open) => {
          if (!open) setDeletionReviewChannel(null)
        }}
        onChanged={refresh}
      />

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

      <SyncProgressDialog
        open={syncDialog != null}
        dialog={syncDialog}
        channels={visibleChannels}
        syncState={syncState}
        bulkSync={bulkSync}
        onOpenChange={(open) => {
          if (!open) setSyncDialog(null)
        }}
      />

      {confirmDialog}
    </section>
  )
}
