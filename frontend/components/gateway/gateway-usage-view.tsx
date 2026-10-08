import { memo, useEffect, useMemo, useRef, useState, type ReactNode } from "react"
import {
  ArrowDown,
  CornerDownRight,
  ArrowUp,
  ArrowUpDown,
  BarChart3,
  CheckCircle2,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  Columns3,
  Copy,
  ExternalLink,
  FileText,
  Info,
  Link2,
  RefreshCw,
  Route,
  Search,
  SlidersHorizontal,
  Snowflake,
  TriangleAlert,
  XCircle,
} from "lucide-react"
import { toast } from "sonner"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import {
  Bar,
  Cell,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip as ChartTooltip,
  XAxis,
  YAxis,
} from "recharts"
import type {
  Channel,
  GatewayDownstreamKey,
  GatewayManualRoutingCooldownResult,
  GatewayRequestLog,
  GatewayRequestLogAnalytics,
  GatewayRequestLogBreakdownItem,
  GatewayRequestLogGroupCacheRatio,
  GatewayRequestLogGroupCacheRatioPage,
  GatewayRequestLogTrendPoint,
} from "@/lib/api-types"
import { apiFetch } from "@/lib/api"
import { compactCount, compactDateTime, compactTokenCount, dateTime, decimal, formatCacheReadRatio } from "@/lib/format"
import { cn } from "@/lib/utils"
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { GatewayClientIpCell } from "@/components/gateway/gateway-client-ip-cell"
import { ModelFamilyMark } from "@/components/model-family-mark"
import { modelFamilyFromModelName } from "@/lib/model-families"
import { gatewayRequestHasRealFallback, gatewayRouteAttemptBudgetLabel, gatewayRouteAttemptLocation, gatewayRouteAttemptReasonLabel, gatewayRouteOutcome, gatewayRouteTraceCopyText, gatewaySessionAffinityRemaining } from "@/lib/gateway-route-attempts"
import { gatewayModelAudit } from "@/lib/gateway-model-audit"
import { gatewayRetryStatus } from "@/lib/gateway-retry-status"
import {
  gatewayConnectionEventClassName,
  gatewayConnectionEventDetail,
  gatewayConnectionEventLabel,
  gatewayRequestConnectionEvent,
} from "@/lib/gateway-connection-events"
import {
  gatewayRouteExclusionCooldownLabel,
  gatewayRouteExclusionCoverageLabel,
  gatewayRouteExclusionGenerationLabel,
  gatewayRouteExclusionLocation,
  gatewayRouteExclusionReasonLabel,
  gatewayRouteExclusionSnapshotCopyText,
  gatewayRouteExclusionSnapshotState,
} from "@/lib/gateway-route-exclusions"
import { officialReasoningEffortLabel } from "@/lib/reasoning-effort"
import type { GatewayUsageTab } from "@/lib/gateway-usage-tabs"
import { usageBreakdownDoughnutLayout } from "@/lib/doughnut-chart-layout"
import { gatewayUsageRequestTableClassName } from "@/lib/gateway-usage-table-style"
import {
  gatewayFrozenRatios,
  gatewayCostComparisonLabel,
  gatewayCostComparison,
  gatewayCostDifferenceRatioText,
  gatewayCostDifferenceText,
  gatewayCostDisplayText,
  gatewayCostDisplaySource,
  gatewayCostRatioEstimate,
  gatewayCostStatusText,
  gatewayCostSystemEstimate,
  gatewayCostUnavailableReasonText,
  gatewayCostUsesPriceFact,
} from "@/lib/gateway-cost-display"
import {
  gatewayFirstByteTone,
  gatewayManualCooldownIdempotencyKey,
  gatewayRequestCanBeManuallyCooled,
} from "@/lib/gateway-first-byte"

type UsageFilterForm = {
  start: string
  end: string
  upstreamRequestID: string
  downstreamKeyID: string
  modelName: string
  channelID: string
  groupName: string
  result: string
  routeReason: string
}

type UsageSortKey = "created_at" | "model_name" | "total_tokens" | "cost" | "status_code" | "latency_ms"
type UsageSortOrder = "asc" | "desc"

type UsageColumnKey = "clientIp" | "channel" | "tokens" | "ratios" | "cost" | "status" | "latency"

const usageColumnOptions: Array<{ key: UsageColumnKey; label: string }> = [
  { key: "clientIp", label: "下游 IP" },
  { key: "channel", label: "渠道 / 路由" },
  { key: "tokens", label: "Token" },
  { key: "ratios", label: "倍率" },
  { key: "cost", label: "费用" },
  { key: "status", label: "状态" },
  { key: "latency", label: "耗时" },
]

const defaultUsageColumns: Record<UsageColumnKey, boolean> = {
  clientIp: true,
  channel: true,
  tokens: true,
  ratios: true,
  cost: true,
  status: true,
  latency: true,
}

const usageColumnsStorageKey = "bridgeops-usage-visible-columns"

type GatewayUsageViewProps = {
  tab: GatewayUsageTab
  total: number
  timeZone: string
  itemCount: number
  items: GatewayRequestLog[]
  loading: boolean
  pending: boolean
  error: string | null
  activeFilterCount: number
  exporting: boolean
  draftFilters: UsageFilterForm
  downstreamKeys: GatewayDownstreamKey[]
  channels: Channel[]
  models: string[]
  groups: string[]
  analytics: GatewayRequestLogAnalytics | null | undefined
  analyticsLoading: boolean
  analyticsError: string | null
  groupCachePage: GatewayRequestLogGroupCacheRatioPage | null | undefined
  groupCacheLoading: boolean
  groupCachePending: boolean
  groupCacheError: string | null
  groupCachePageNumber: number
  groupCachePageSize: number
  groupCacheCanGoBack: boolean
  rangeStart: number
  rangeEnd: number
  pageSize: number
  currentPage: number
  totalPages: number
  pagerNumbers: number[]
  sortBy: UsageSortKey
  sortOrder: UsageSortOrder
  manualCooldownMinutes: number
  onExport: () => void
  onRefresh: () => void
  onApplyFilters: () => void
  onResetFilters: () => void
  onUpdateFilter: (key: keyof UsageFilterForm, value: string) => void
  onSort: (key: UsageSortKey) => void
  onPageSizeChange: (pageSize: number) => void
  onGoToPage: (page: number) => void
  onGroupCacheNext: () => void
  onGroupCacheBack: () => void
  onGroupCachePageSizeChange: (pageSize: number) => void
}

export function GatewayUsageView({
  tab,
  total,
  timeZone,
  itemCount,
  items,
  loading,
  pending,
  error,
  activeFilterCount,
  exporting,
  draftFilters,
  downstreamKeys,
  channels,
  models,
  groups,
  analytics,
  analyticsLoading,
  analyticsError,
  groupCachePage,
  groupCacheLoading,
  groupCachePending,
  groupCacheError,
  groupCachePageNumber,
  groupCachePageSize,
  groupCacheCanGoBack,
  rangeStart,
  rangeEnd,
  pageSize,
  currentPage,
  totalPages,
  pagerNumbers,
  sortBy,
  sortOrder,
  manualCooldownMinutes,
  onExport,
  onRefresh,
  onApplyFilters,
  onResetFilters,
  onUpdateFilter,
  onSort,
  onPageSizeChange,
  onGoToPage,
  onGroupCacheNext,
  onGroupCacheBack,
  onGroupCachePageSizeChange,
}: GatewayUsageViewProps) {
  const [visibleColumns, setVisibleColumns] = useState<Record<UsageColumnKey, boolean>>(defaultUsageColumns)
  const [selectedLog, setSelectedLog] = useState<GatewayRequestLog | null>(null)
  const [selectedGroupCacheRow, setSelectedGroupCacheRow] = useState<GatewayRequestLogGroupCacheRatio | null>(null)
  const [coolingRequestLogIDs, setCoolingRequestLogIDs] = useState<Set<number>>(() => new Set())
  const coolingRequestLogIDsRef = useRef<Set<number>>(new Set())
  const manualCooldownIdempotencyKeys = useRef<Map<number, string>>(new Map())
  const [advancedOpen, setAdvancedOpen] = useState(() => Boolean(
    draftFilters.upstreamRequestID
    || (draftFilters.result && draftFilters.result !== "__all__")
    || (draftFilters.routeReason && draftFilters.routeReason !== "__all__")
    || (draftFilters.groupName && draftFilters.groupName !== "__all__"),
  ))

  useEffect(() => {
    setSelectedLog((current) => current ? items.find((item) => item.id === current.id) ?? null : null)
  }, [items])

  useEffect(() => {
    if (tab === "group-cache") setSelectedLog(null)
    else setSelectedGroupCacheRow(null)
  }, [tab])

  useEffect(() => {
    try {
      const saved = window.localStorage.getItem(usageColumnsStorageKey)
      if (!saved) return
      const parsed = JSON.parse(saved) as Partial<Record<UsageColumnKey, boolean>>
      setVisibleColumns({ ...defaultUsageColumns, ...parsed })
    } catch {
      // 忽略浏览器存储损坏，继续使用默认列。
    }
  }, [])

  function updateVisibleColumn(key: UsageColumnKey, checked: boolean) {
    setVisibleColumns((current) => {
      const next = { ...current, [key]: checked }
      try {
        window.localStorage.setItem(usageColumnsStorageKey, JSON.stringify(next))
      } catch {
        // 无法持久化时仍保留当前页面的选择。
      }
      return next
    })
  }

  function resetVisibleColumns() {
    setVisibleColumns(defaultUsageColumns)
    try {
      window.localStorage.removeItem(usageColumnsStorageKey)
    } catch {
      // 忽略浏览器存储错误。
    }
  }

  async function applyManualRoutingCooldown(log: GatewayRequestLog) {
    if (!gatewayRequestCanBeManuallyCooled(log) || coolingRequestLogIDsRef.current.has(log.id)) return
    coolingRequestLogIDsRef.current.add(log.id)
    setCoolingRequestLogIDs((current) => new Set(current).add(log.id))
    const idempotencyKey = manualCooldownIdempotencyKeys.current.get(log.id) ?? gatewayManualCooldownIdempotencyKey(log.id)
    manualCooldownIdempotencyKeys.current.set(log.id, idempotencyKey)
    try {
      const result = await apiFetch<GatewayManualRoutingCooldownResult>(`/gateway/request-logs/${log.id}/routing-cooldown`, {
        method: "POST",
        headers: {
          "Idempotency-Key": idempotencyKey,
        },
      })
      manualCooldownIdempotencyKeys.current.delete(log.id)
      const route = [result.channel_name, result.group_name].filter(Boolean).join(" · ")
      toast.success(`${route || "当前渠道"} 已冷却 ${result.duration_minutes} 分钟，至 ${dateTime(result.cooldown_until, timeZone)}`)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "手动冷却失败")
    } finally {
      coolingRequestLogIDsRef.current.delete(log.id)
      setCoolingRequestLogIDs((current) => {
        const next = new Set(current)
        next.delete(log.id)
        return next
      })
    }
  }

  const visibleOptionalColumnCount = usageColumnOptions.filter(({ key }) => visibleColumns[key]).length
  const tableColumnCount = 3 + visibleOptionalColumnCount
  const channelHomeURLs = useMemo(() => {
    const next = new Map<number, string>()
    for (const channel of channels) {
      const url = safeChannelHomeURL(channel.site_url)
      if (url) next.set(channel.id, url)
    }
    return next
  }, [channels])
  const advancedFilterCount = [
    draftFilters.upstreamRequestID,
    draftFilters.result && draftFilters.result !== "__all__" ? draftFilters.result : "",
    draftFilters.routeReason && draftFilters.routeReason !== "__all__" ? draftFilters.routeReason : "",
    draftFilters.groupName && draftFilters.groupName !== "__all__" ? draftFilters.groupName : "",
  ].filter(Boolean).length
  const filterPending = tab === "group-cache" ? groupCachePending : pending

  return (
    <section className="flex min-w-0 flex-col gap-4">
      <section className="order-1 overflow-hidden rounded-lg border border-border bg-background">
        <div className="bg-muted/20 p-2.5">
        <div className="flex min-w-0 flex-wrap items-center gap-1.5 text-xs font-medium text-muted-foreground">
          <SlidersHorizontal className="size-3.5" />
          <span>{"筛选条件"}</span>
          <Badge variant="secondary" className="h-5 px-1.5 text-[10px]">
            {tab === "group-cache" ? `第 ${groupCachePageNumber} 页 · ${groupCachePage?.items.length ?? 0} 组` : `${total}条`}
          </Badge>
          {activeFilterCount > 0 ? <Badge variant="outline" className="h-5 px-1.5 text-[10px]">{`已筛选 ${activeFilterCount}`}</Badge> : null}
          <Badge variant="outline" className="h-5 px-1.5 text-[10px]">{`时区 ${timeZone}`}</Badge>
        </div>

        <div className="mt-2 grid min-w-0 gap-1.5 md:grid-cols-2 xl:grid-cols-[14rem_14rem_repeat(4,minmax(0,1fr))]">
          <label className="min-w-0 space-y-1">
            <span className="text-xs font-medium text-muted-foreground">{"开始时间"}</span>
            <Input className="h-8 min-w-0" type="datetime-local" value={draftFilters.start} onChange={(event) => onUpdateFilter("start", event.target.value)} />
          </label>
          <label className="min-w-0 space-y-1">
            <span className="text-xs font-medium text-muted-foreground">{"结束时间"}</span>
            <Input className="h-8 min-w-0" type="datetime-local" value={draftFilters.end} onChange={(event) => onUpdateFilter("end", event.target.value)} />
          </label>
          <label className="min-w-0 space-y-1">
            <span className="text-xs font-medium text-muted-foreground">{"下游 API Key"}</span>
            <Select value={draftFilters.downstreamKeyID} onValueChange={(value) => onUpdateFilter("downstreamKeyID", value)}>
              <SelectTrigger className="h-8 w-full min-w-0"><SelectValue placeholder="全部" /></SelectTrigger>
              <SelectContent className="max-h-72">
                <SelectItem value="__all__">{"全部"}</SelectItem>
                {downstreamKeys.map((key) => {
                  const groupName = key.key_group_name?.trim() || "未分组"
                  return <SelectItem key={key.id} value={String(key.id)}>{`${key.name} · ${groupName}`}</SelectItem>
                })}
              </SelectContent>
            </Select>
          </label>
          <label className="min-w-0 space-y-1">
            <span className="text-xs font-medium text-muted-foreground">{"上游渠道"}</span>
            <Select value={draftFilters.channelID} onValueChange={(value) => onUpdateFilter("channelID", value)}>
              <SelectTrigger className="h-8 w-full min-w-0"><SelectValue placeholder="全部" /></SelectTrigger>
              <SelectContent className="max-h-72">
                <SelectItem value="__all__">{"全部"}</SelectItem>
                {channels.map((channel) => <SelectItem key={channel.id} value={String(channel.id)}>{channel.name}</SelectItem>)}
              </SelectContent>
            </Select>
          </label>
          <label className="min-w-0 space-y-1">
            <span className="text-xs font-medium text-muted-foreground">{"模型"}</span>
            <Select value={draftFilters.modelName} onValueChange={(value) => onUpdateFilter("modelName", value)}>
              <SelectTrigger className="h-8 w-full min-w-0"><SelectValue placeholder="全部" /></SelectTrigger>
              <SelectContent className="max-h-72">
                <SelectItem value="__all__">{"全部"}</SelectItem>
                {models.map((model) => <SelectItem key={model} value={model}>{model}</SelectItem>)}
                {models.length === 0 ? <SelectItem value="__no_models__" disabled>{"暂无可选模型"}</SelectItem> : null}
              </SelectContent>
            </Select>
          </label>
          <label className="min-w-0 space-y-1">
            <span className="text-xs font-medium text-muted-foreground">{"上游分组"}</span>
            <Select value={draftFilters.groupName} onValueChange={(value) => onUpdateFilter("groupName", value)}>
              <SelectTrigger className="h-8 w-full min-w-0"><SelectValue placeholder="全部" /></SelectTrigger>
              <SelectContent className="max-h-72">
                <SelectItem value="__all__">{"全部"}</SelectItem>
                <SelectItem value="__unassigned__">{"未分组"}</SelectItem>
                {groups.filter((group) => group.trim() !== "").map((group) => <SelectItem key={group} value={group}>{group}</SelectItem>)}
                {groups.length === 0 ? <SelectItem value="__no_groups__" disabled>{"暂无可选分组"}</SelectItem> : null}
              </SelectContent>
            </Select>
          </label>
        </div>

        <Collapsible open={advancedOpen} onOpenChange={setAdvancedOpen}>
          <div className="mt-2 flex min-w-0 flex-wrap items-center gap-1.5 border-t border-border pt-2">
            <CollapsibleTrigger asChild>
              <Button type="button" size="sm" variant="ghost" className="h-8 gap-1.5 px-2 text-xs">
                <SlidersHorizontal className="size-3.5" />
                {"高级筛选"}
                {advancedFilterCount > 0 ? <Badge variant="secondary" className="h-5 px-1.5 text-[10px]">{advancedFilterCount}</Badge> : null}
                <ChevronDown className={cn("size-3.5 transition-transform", advancedOpen && "rotate-180")} />
              </Button>
            </CollapsibleTrigger>
            <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
              <Button type="button" size="sm" onClick={onApplyFilters} disabled={filterPending}>
                {"查询"}
              </Button>
              <Button type="button" size="sm" variant="outline" onClick={() => { onResetFilters(); setAdvancedOpen(false) }} disabled={filterPending}>{"重置"}</Button>
              <Button type="button" size="sm" variant="outline" onClick={onRefresh} disabled={filterPending}>
                <RefreshCw className={filterPending ? "size-3.5 animate-spin" : "size-3.5"} />
                {"刷新"}
              </Button>
              {tab === "requests" ? (
                <Button type="button" size="sm" variant="outline" onClick={onExport} disabled={exporting || pending || total === 0}>
                  {exporting ? "导出中" : "导出 CSV"}
                </Button>
              ) : null}
            </div>
          </div>
          <CollapsibleContent>
            <div className="mt-2 grid min-w-0 gap-1.5 border-t border-border pt-2 md:grid-cols-2 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,2fr)]">
              <label className="min-w-0 space-y-1">
                <span className="text-xs font-medium text-muted-foreground">{"结果"}</span>
                <Select value={draftFilters.result} onValueChange={(value) => onUpdateFilter("result", value)}>
                  <SelectTrigger className="h-8 w-full min-w-0"><SelectValue /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="__all__">{"全部结果"}</SelectItem>
                    <SelectItem value="success">{"成功"}</SelectItem>
                    <SelectItem value="failed">{"失败"}</SelectItem>
                    <SelectItem value="canceled">{"客户端取消"}</SelectItem>
                  </SelectContent>
                </Select>
              </label>
              <label className="min-w-0 space-y-1">
                <span className="text-xs font-medium text-muted-foreground">{"路由结果"}</span>
                <Select value={draftFilters.routeReason} onValueChange={(value) => onUpdateFilter("routeReason", value)}>
                  <SelectTrigger className="h-8 w-full min-w-0"><SelectValue /></SelectTrigger>
                  <SelectContent className="max-h-72">
                    <SelectItem value="__all__">{"全部路由"}</SelectItem>
                    <SelectItem value="smart_primary">{"智能首选"}</SelectItem>
                    <SelectItem value="manual">{"手动固定"}</SelectItem>
                    <SelectItem value="session_affinity">{"会话亲和"}</SelectItem>
                    <SelectItem value="fallback">{"故障回退"}</SelectItem>
                    <SelectItem value="model_filtered">{"模型过滤"}</SelectItem>
                    <SelectItem value="all_failed">{"全部失败"}</SelectItem>
                    <SelectItem value="client_canceled">{"客户端取消"}</SelectItem>
                  </SelectContent>
                </Select>
              </label>
              <label className="min-w-0 space-y-1 md:col-span-2 xl:col-span-1">
                <span className="text-xs font-medium text-muted-foreground">{"上游请求 ID"}</span>
                <span className="relative block">
                  <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    className="h-8 min-w-0 bg-background pl-8 font-mono text-xs"
                    value={draftFilters.upstreamRequestID}
                    placeholder="精确搜索上游请求 ID"
                    onChange={(event) => onUpdateFilter("upstreamRequestID", event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key !== "Enter" || event.nativeEvent.isComposing) return
                      event.preventDefault()
                      onApplyFilters()
                    }}
                  />
                </span>
              </label>
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>
      </section>

      {tab === "group-cache" ? (
        <>
          <GroupCacheRatioTable
            rows={groupCachePage?.items ?? []}
            loading={groupCacheLoading}
            pending={groupCachePending}
            error={groupCacheError}
            pageNumber={groupCachePageNumber}
            pageSize={groupCachePageSize}
            canGoBack={groupCacheCanGoBack}
            canGoNext={Boolean(groupCachePage?.next_cursor)}
            onBack={onGroupCacheBack}
            onNext={onGroupCacheNext}
            onPageSizeChange={onGroupCachePageSizeChange}
            onOpenDetail={setSelectedGroupCacheRow}
          />
          <GroupCacheDetailDialog row={selectedGroupCacheRow} onOpenChange={(open) => !open && setSelectedGroupCacheRow(null)} />
        </>
      ) : (
        <>
          <details className="order-2 rounded-lg border border-border bg-card">
            <summary className="cursor-pointer px-4 py-2.5 text-sm font-medium text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">用量分析<span className="ml-2 font-normal text-muted-foreground">趋势与分布</span></summary>
            <UsageAnalyticsOverview analytics={analytics} loading={analyticsLoading} error={analyticsError} timeZone={timeZone} />
          </details>

          <section className="order-3 min-w-0 rounded-lg border border-border bg-background">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-muted/20 px-4 py-2.5">
          <div className="flex items-baseline gap-3">
            <h3 className="text-lg font-semibold tracking-tight text-foreground">{"请求明细"}</h3>
            <p className="text-xs text-muted-foreground">
              {total > 0 && itemCount > 0 ? `显示 ${rangeStart}-${rangeEnd} / ${total} 条` : `共 ${total} 条`}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            {loading || pending ? (
              <Badge variant="outline" className="gap-1.5">
                <RefreshCw className="size-3 animate-spin" />
                {"刷新中"}
              </Badge>
            ) : null}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button type="button" variant="outline" size="sm" className="h-8 gap-1.5">
                  <Columns3 className="size-3.5" />
                  {`列 ${visibleOptionalColumnCount + 3}/${usageColumnOptions.length + 3}`}
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end" className="w-48">
                <DropdownMenuCheckboxItem checked disabled onCheckedChange={() => undefined}>
                  {"时间 / Key / 模型"}
                </DropdownMenuCheckboxItem>
                <DropdownMenuSeparator />
                {usageColumnOptions.map(({ key, label }) => (
                  <DropdownMenuCheckboxItem
                    key={key}
                    checked={visibleColumns[key]}
                    onCheckedChange={(checked) => updateVisibleColumn(key, checked === true)}
                  >
                    {label}
                  </DropdownMenuCheckboxItem>
                ))}
                <DropdownMenuSeparator />
                <DropdownMenuItem onSelect={resetVisibleColumns}>
                  {"恢复默认列"}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>

        <Table className={cn(gatewayUsageRequestTableClassName, visibleColumns.clientIp && "min-w-[1320px]")}>
            <TableHeader className="sticky top-0 z-10 bg-background shadow-[0_1px_0_hsl(var(--border))]">
              <TableRow>
                <SortableUsageHead label="Key / 时间" sortKey="created_at" activeKey={sortBy} order={sortOrder} onSort={onSort} className={visibleColumns.clientIp ? "w-[12%]" : "w-[13%]"} />
                {visibleColumns.clientIp ? <TableHead className="w-[10%]">{"下游 IP"}</TableHead> : null}
                {visibleColumns.channel ? <TableHead className={visibleColumns.clientIp ? "w-[14%]" : "w-[15%]"}>{"渠道 / 路由"}</TableHead> : null}
                <SortableUsageHead label="模型" sortKey="model_name" activeKey={sortBy} order={sortOrder} onSort={onSort} className={visibleColumns.clientIp ? "w-[21%]" : "w-[22%]"} />
                {visibleColumns.tokens ? <SortableUsageHead label="Token" sortKey="total_tokens" activeKey={sortBy} order={sortOrder} onSort={onSort} className={visibleColumns.clientIp ? "w-[11%]" : "w-[12%]"} /> : null}
                {visibleColumns.ratios ? <TableHead className={cn("text-center", visibleColumns.clientIp ? "w-[5%]" : "w-[8%]")}>{"倍率"}</TableHead> : null}
                {visibleColumns.cost ? <SortableUsageHead label="费用" sortKey="cost" activeKey={sortBy} order={sortOrder} onSort={onSort} className={visibleColumns.clientIp ? "w-[9%]" : "w-[10%]"} align="right" /> : null}
                {visibleColumns.status ? <SortableUsageHead label="状态" sortKey="status_code" activeKey={sortBy} order={sortOrder} onSort={onSort} className={visibleColumns.clientIp ? "w-[6%]" : "w-[7%]"} align="center" /> : null}
                {visibleColumns.latency ? <SortableUsageHead label="耗时" sortKey="latency_ms" activeKey={sortBy} order={sortOrder} onSort={onSort} className="w-[8%]" /> : null}
                <TableHead className={cn("text-center", visibleColumns.clientIp ? "w-[4%]" : "w-[5%]")}>{"详情"}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((log) => (
                <UsageLogRow
                  key={log.id}
                  log={log}
                  timeZone={timeZone}
                  visibleColumns={visibleColumns}
                  channelHomeURL={channelHomeURLs.get(log.channel_id)}
                  manualCooldownMinutes={manualCooldownMinutes}
                  cooling={coolingRequestLogIDs.has(log.id)}
                  onManualCooldown={applyManualRoutingCooldown}
                  onOpenDetail={setSelectedLog}
                />
              ))}
              {!loading && itemCount === 0 ? (
                <TableRow>
                  <TableCell colSpan={tableColumnCount} className="h-28 text-center text-xs text-muted-foreground">
                    {"暂无使用记录"}
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
        </Table>

        {error ? (
          <p className="border-t border-border px-5 py-3 text-xs text-destructive lg:px-6">{error}</p>
        ) : null}

        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border px-5 py-3 lg:px-6">
          <div className="text-xs text-muted-foreground">
            {total > 0 && itemCount > 0 ? `显示 ${rangeStart}-${rangeEnd} / ${total} 条` : `共 ${total} 条`}
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <span>{"每页"}</span>
              <Select
                value={String(pageSize)}
                disabled={pending}
                onValueChange={(value) => onPageSizeChange(Number(value))}
              >
                <SelectTrigger size="sm" className="h-8 w-20 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent align="end">
                  {[20, 50, 100].map((value) => (
                    <SelectItem key={value} value={String(value)}>
                      {value}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-1">
              <Button
                type="button"
                variant="outline"
                size="icon-sm"
                disabled={currentPage <= 1 || pending}
                onClick={() => onGoToPage(1)}
                aria-label="首页"
                title="首页"
              >
                <ChevronsLeft className="size-3.5" />
              </Button>
              <Button
                type="button"
                variant="outline"
                size="icon-sm"
                disabled={currentPage <= 1 || pending}
                onClick={() => onGoToPage(currentPage - 1)}
                aria-label="上一页"
                title="上一页"
              >
                <ChevronLeft className="size-3.5" />
              </Button>
              {pagerNumbers.map((pageNumber) => (
                <Button
                  key={pageNumber}
                  type="button"
                  variant={pageNumber === currentPage ? "default" : "outline"}
                  size="sm"
                  className="h-8 min-w-8 px-2 text-xs"
                  disabled={pending}
                  onClick={() => onGoToPage(pageNumber)}
                  aria-label={`第 ${pageNumber} 页`}
                  aria-current={pageNumber === currentPage ? "page" : undefined}
                >
                  {pageNumber}
                </Button>
              ))}
              <Button
                type="button"
                variant="outline"
                size="icon-sm"
                disabled={currentPage >= totalPages || pending}
                onClick={() => onGoToPage(currentPage + 1)}
                aria-label="下一页"
                title="下一页"
              >
                <ChevronRight className="size-3.5" />
              </Button>
              <Button
                type="button"
                variant="outline"
                size="icon-sm"
                disabled={currentPage >= totalPages || pending}
                onClick={() => onGoToPage(totalPages)}
                aria-label="末页"
                title="末页"
              >
                <ChevronsRight className="size-3.5" />
              </Button>
            </div>
          </div>
        </div>
          </section>

          <UsageDetailDialog log={selectedLog} timeZone={timeZone} onOpenChange={(open) => !open && setSelectedLog(null)} />
        </>
      )}
    </section>
  )
}

const UsageAnalyticsOverview = memo(function UsageAnalyticsOverview({
  analytics,
  loading,
  error,
  timeZone,
}: {
  analytics: GatewayRequestLogAnalytics | null | undefined
  loading: boolean
  error: string | null
  timeZone: string
}) {
  const trend = Array.isArray(analytics?.trend) ? analytics.trend : []
  const models = Array.isArray(analytics?.models_by_tokens) ? analytics.models_by_tokens : []
  const channels = Array.isArray(analytics?.channels) ? analytics.channels : []

  return (
    <section className="order-1 min-w-0 overflow-x-auto overflow-y-hidden rounded-lg border border-border bg-background">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-muted/20 px-4 py-2.5">
        <div className="flex items-center gap-2">
          <BarChart3 className="size-3.5 text-primary" />
          <h3 className="text-lg font-semibold tracking-tight text-foreground">{"用量概览"}</h3>
        </div>
        <Badge variant="outline" className="h-6 px-2 text-[10px]">{"趋势 · 模型 · 渠道"}</Badge>
      </div>
      {error ? (
        <div className="border-b border-destructive/20 bg-destructive/5 px-4 py-3 text-xs text-destructive">
          {`用量图表加载失败：${error}`}
        </div>
      ) : null}
      <div className={cn("grid min-w-[1100px] grid-cols-[2fr_1.25fr_1fr_1fr] gap-px bg-border", error && "opacity-60")}>
        <UsageRequestCostChart trend={trend} loading={loading} timeZone={timeZone} />
        <UsageTokenChart trend={trend} loading={loading} timeZone={timeZone} />
        <UsageModelChart models={models} loading={loading} />
        <UsageChannelChart channels={channels} loading={loading} />
      </div>
    </section>
  )
})

const GroupCacheRatioTable = memo(function GroupCacheRatioTable({
  rows,
  loading,
  pending,
  error,
  pageNumber,
  pageSize,
  canGoBack,
  canGoNext,
  onBack,
  onNext,
  onPageSizeChange,
  onOpenDetail,
}: {
  rows: GatewayRequestLogGroupCacheRatio[]
  loading: boolean
  pending: boolean
  error: string | null
  pageNumber: number
  pageSize: number
  canGoBack: boolean
  canGoNext: boolean
  onBack: () => void
  onNext: () => void
  onPageSizeChange: (pageSize: number) => void
  onOpenDetail: (row: GatewayRequestLogGroupCacheRatio) => void
}) {
  const summary = useMemo(() => rows.reduce((current, row) => ({
    promptTokens: current.promptTokens + Math.max(0, row.prompt_tokens),
    cacheReadTokens: current.cacheReadTokens + Math.max(0, row.cache_read_tokens),
    cacheCreationTokens: current.cacheCreationTokens + Math.max(0, row.cache_creation_tokens),
    validRequests: current.validRequests + Math.max(0, row.valid_requests),
    noDataGroups: current.noDataGroups + (row.ratio == null ? 1 : 0),
  }), {
    promptTokens: 0,
    cacheReadTokens: 0,
    cacheCreationTokens: 0,
    validRequests: 0,
    noDataGroups: 0,
  }), [rows])
  const weightedRatio = summary.promptTokens > 0 ? summary.cacheReadTokens / summary.promptTokens : null

  return (
    <section className="order-3 min-w-0 overflow-hidden rounded-lg border border-border bg-background">
      <div className="flex min-h-12 flex-wrap items-center justify-between gap-2 border-b border-border bg-muted/20 px-4 py-2.5">
        <div className="flex min-w-0 items-center gap-2">
          <BarChart3 className="size-3.5 text-primary" />
          <div className="min-w-0">
            <h3 className="text-lg font-semibold tracking-tight text-foreground">{"分组缓存比例"}</h3>
            <p className="truncate text-xs text-muted-foreground">{`第 ${pageNumber} 页 · 本页 ${rows.length} 组`}</p>
          </div>
          <Tooltip>
            <TooltipTrigger asChild>
              <button type="button" className="inline-flex size-6 shrink-0 items-center justify-center rounded-md text-muted-foreground hover:bg-muted hover:text-foreground" aria-label="查看分组缓存比例统计口径" title="查看分组缓存比例统计口径">
                <Info className="size-3.5" />
              </button>
            </TooltipTrigger>
            <TooltipContent className="max-w-72 text-xs">
              {"仅统计当前筛选中的真实网关请求；探测请求已排除。比例为缓存读取 Token / 输入 Token，输入 Token 缺失或为 0 时不生成数据。"}
            </TooltipContent>
          </Tooltip>
        </div>
        <div className="flex items-center gap-2">
          {pending ? <RefreshCw className="size-3.5 animate-spin text-muted-foreground" aria-label="正在刷新缓存比例" /> : null}
          <Badge variant="outline" className="h-6 px-2 text-[10px]">{"按输入 Token 汇总"}</Badge>
        </div>
      </div>
      {error ? (
        <div className="border-b border-destructive/20 bg-destructive/5 px-4 py-3 text-xs text-destructive">{"分组缓存统计加载失败，请刷新后重试。"}</div>
      ) : null}
      <div className="grid grid-cols-2 gap-2 border-b border-border bg-background p-3 sm:grid-cols-4">
        <GroupCacheMetric label="统计分组" value={compactCount(rows.length)} hint="当前页分组数" />
        <GroupCacheMetric label="缓存读取占比" value={formatGroupCacheRatio(weightedRatio)} hint="缓存读取 Token / 输入 Token" />
        <GroupCacheMetric label="输入 Token" value={compactTokenCount(summary.promptTokens)} hint="当前页输入 Token" />
        <GroupCacheMetric label="缓存读取 Token" value={compactTokenCount(summary.cacheReadTokens)} hint={`${summary.noDataGroups > 0 ? `${summary.noDataGroups} 组无有效分母 · ` : ""}${compactCount(summary.validRequests)} 个有效请求`} />
      </div>
      <div className="max-h-[calc(100dvh-22rem)] min-h-72 overflow-auto">
        <Table className="min-w-[920px] table-fixed">
          <TableHeader className="sticky top-0 z-10 bg-muted/90 backdrop-blur">
            <TableRow>
              <TableHead className="w-[27%]">{"渠道 / 分组"}</TableHead>
              <TableHead className="w-[24%]">{"缓存读取占比"}</TableHead>
              <TableHead className="w-[22%] text-right">{"输入 / 读取 Token"}</TableHead>
              <TableHead className="w-[12%] text-right">{"缓存写入"}</TableHead>
              <TableHead className="w-[11%] text-right">{"有效请求"}</TableHead>
              <TableHead className="w-[4%] text-center">{"详情"}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading ? (
              <TableRow><TableCell colSpan={6} className="h-24 text-center text-sm text-muted-foreground">{"加载统计中..."}</TableCell></TableRow>
            ) : rows.length === 0 ? (
              <TableRow><TableCell colSpan={6} className="h-24 text-center text-sm text-muted-foreground">{"当前筛选暂无分组缓存数据"}</TableCell></TableRow>
            ) : rows.map((row) => {
              const groupName = row.group_name.trim() || "未分组"
              return (
                <TableRow key={`${row.channel_id}:${row.channel_name}:${row.group_name}`}>
                  <TableCell className="min-w-0 align-middle">
                    <span className="block truncate font-medium" title={row.channel_name}>{row.channel_name}</span>
                    <Badge variant="secondary" className="mt-1 max-w-full"><span className="truncate" title={groupName}>{groupName}</span></Badge>
                  </TableCell>
                  <TableCell className="align-middle">
                    <div className="min-w-0">
                      <div className="flex items-center gap-1.5">
                        <span className="font-semibold tabular-nums">{formatGroupCacheRatio(row.ratio)}</span>
                        <Badge variant="outline" className="h-5 px-1.5 text-[10px]">{row.ratio == null ? "无分母" : "有数据"}</Badge>
                      </div>
                      <div className="mt-1.5 h-1.5 overflow-hidden rounded-full bg-muted" aria-hidden="true">
                        <div className="h-full rounded-full bg-primary/70 transition-[width]" style={{ width: `${groupCacheRatioBarWidth(row.ratio)}%` }} />
                      </div>
                    </div>
                  </TableCell>
                  <TableCell className="text-right font-mono text-xs tabular-nums">
                    <div>{`输入 ${compactTokenCount(row.prompt_tokens)}`}</div>
                    <div className="mt-1 text-muted-foreground">{`读取 ${compactTokenCount(row.cache_read_tokens)}`}</div>
                  </TableCell>
                  <TableCell className="text-right font-mono tabular-nums">{compactTokenCount(row.cache_creation_tokens)}</TableCell>
                  <TableCell className="text-right font-mono tabular-nums">{compactCount(row.valid_requests)}</TableCell>
                  <TableCell className="text-center">
                    <Button type="button" size="icon-sm" variant="ghost" onClick={() => onOpenDetail(row)} aria-label={`查看 ${row.channel_name} / ${groupName} 缓存详情`} title="查看缓存详情">
                      <Info className="size-3.5" />
                    </Button>
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-border px-4 py-3">
        <span className="text-xs text-muted-foreground">{`第 ${pageNumber} 页 · 本页 ${rows.length} 组`}</span>
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <span>{"每页"}</span>
            <Select value={String(pageSize)} disabled={pending} onValueChange={(value) => onPageSizeChange(Number(value))}>
              <SelectTrigger size="sm" className="h-8 w-20 text-xs" aria-label="缓存比例每页数量">
                <SelectValue />
              </SelectTrigger>
              <SelectContent align="end">
                {[20, 50, 100].map((value) => <SelectItem key={value} value={String(value)}>{value}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
          <Button type="button" variant="outline" size="icon-sm" disabled={!canGoBack || pending} onClick={onBack} aria-label="缓存比例上一页" title="上一页">
            <ChevronLeft className="size-3.5" />
          </Button>
          <Button type="button" variant="outline" size="icon-sm" disabled={!canGoNext || pending} onClick={onNext} aria-label="缓存比例下一页" title="下一页">
            <ChevronRight className="size-3.5" />
          </Button>
        </div>
      </div>
    </section>
  )
})

function GroupCacheMetric({ label, value, hint }: { label: string; value: string; hint: string }) {
  return (
    <div className="min-w-0 rounded-md border border-border/80 bg-muted/10 px-3 py-2.5">
      <div className="truncate text-[11px] text-muted-foreground">{label}</div>
      <div className="mt-1 truncate font-mono text-lg font-semibold tabular-nums text-foreground" title={value}>{value}</div>
      <div className="mt-0.5 truncate text-[10px] text-muted-foreground" title={hint}>{hint}</div>
    </div>
  )
}

function GroupCacheDetailDialog({ row, onOpenChange }: { row: GatewayRequestLogGroupCacheRatio | null; onOpenChange: (open: boolean) => void }) {
  if (!row) return null
  const groupName = row.group_name.trim() || "未分组"
  return (
    <Dialog open onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{`${row.channel_name} / ${groupName} · 缓存详情`}</DialogTitle>
          <DialogDescription>{"当前筛选范围内的分组缓存读取统计"}</DialogDescription>
        </DialogHeader>
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          <GroupCacheMetric label="缓存读取占比" value={formatGroupCacheRatio(row.ratio)} hint="读取 / 输入" />
          <GroupCacheMetric label="有效请求" value={compactCount(row.valid_requests)} hint="统计请求数" />
          <GroupCacheMetric label="输入 Token" value={compactTokenCount(row.prompt_tokens)} hint="分母" />
          <GroupCacheMetric label="缓存读取" value={compactTokenCount(row.cache_read_tokens)} hint="分子" />
        </div>
        <div className="grid gap-2 rounded-md border border-border/80 bg-muted/10 px-3 py-2 text-sm">
          <div className="flex items-center justify-between gap-4"><span className="text-muted-foreground">{"缓存写入 Token"}</span><span className="font-mono tabular-nums">{compactTokenCount(row.cache_creation_tokens)}</span></div>
          <div className="flex items-center justify-between gap-4"><span className="text-muted-foreground">{"统计口径"}</span><span className="text-right text-xs">{"缓存读取 Token / 输入 Token"}</span></div>
          {row.ratio == null ? <div className="border-t border-border/70 pt-2 text-xs text-muted-foreground">{"当前范围没有有效的正输入 Token 分母，因此不生成比例。"}</div> : null}
        </div>
      </DialogContent>
    </Dialog>
  )
}

function formatGroupCacheRatio(value: number | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(1)}%` : "无数据"
}

function groupCacheRatioBarWidth(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return 0
  return Math.min(100, value * 100)
}

const usagePieColors = ["#3b82f6", "#10b981", "#f59e0b", "#8b5cf6", "#06b6d4", "#f97316"]
const usageRequestCostTrendMargin = { top: 4, right: 8, left: -8, bottom: 2 }
const usageRequestCostTrendYAxisWidth = 48
const usageTrendXAxisHeight = 46
const usageTrendAxisTick = { fill: "var(--muted-foreground)", fontSize: 9 }

const UsageRequestCostChart = memo(function UsageRequestCostChart({ trend, loading, timeZone }: { trend: GatewayRequestLogTrendPoint[]; loading: boolean; timeZone: string }) {
  const rows = useMemo(() => trend.map((point) => ({ ...point, label: formatChartBucket(point.bucket, timeZone) })), [timeZone, trend])
  return <UsageChartPanel title="请求与费用趋势">
    <div className="grid h-full min-h-0 grid-rows-2 gap-2">
      {loading ? <UsageChartEmpty text="正在加载趋势" /> : rows.length === 0 ? <UsageChartEmpty text="暂无趋势数据" /> : (
        <ResponsiveContainer width="100%" height="100%"><ComposedChart data={rows} syncId="gateway-usage-trend" syncMethod="index" margin={usageRequestCostTrendMargin}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} /><XAxis dataKey="label" tick={false} tickLine={false} axisLine={false} height={usageTrendXAxisHeight} padding={{ left: 0, right: 0 }} /><YAxis allowDecimals={false} tickLine={false} axisLine={false} tick={usageTrendAxisTick} width={usageRequestCostTrendYAxisWidth} /><ChartTooltip content={<UsageOverviewTrendTooltip metric="requests" />} allowEscapeViewBox={{ x: false, y: false }} cursor={{ fill: "var(--muted)" }} animationDuration={0} isAnimationActive={false} wrapperStyle={{ pointerEvents: "none" }} /><Bar dataKey="requests" name="请求" fill="#3b82f633" stroke="#3b82f6" radius={[3, 3, 0, 0]} maxBarSize={28} isAnimationActive={false} /><Line dataKey="requests" name="请求趋势" type="monotone" stroke="#2563eb" strokeWidth={2} dot={false} isAnimationActive={false} />
        </ComposedChart></ResponsiveContainer>
      )}
      {loading ? <UsageChartEmpty text="正在加载趋势" /> : rows.length === 0 ? <UsageChartEmpty text="暂无趋势数据" /> : (
        <ResponsiveContainer width="100%" height="100%"><ComposedChart data={rows} syncId="gateway-usage-trend" syncMethod="index" margin={usageRequestCostTrendMargin}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} /><XAxis dataKey="label" tickLine={false} axisLine={false} tick={usageTrendAxisTick} height={usageTrendXAxisHeight} angle={-35} textAnchor="end" tickMargin={8} minTickGap={8} interval="preserveStartEnd" padding={{ left: 0, right: 0 }} /><YAxis tickLine={false} axisLine={false} tick={usageTrendAxisTick} width={usageRequestCostTrendYAxisWidth} tickFormatter={(value) => `$${Number(value).toFixed(2)}`} /><ChartTooltip content={<UsageOverviewTrendTooltip metric="cost" />} allowEscapeViewBox={{ x: false, y: false }} cursor={{ fill: "var(--muted)" }} animationDuration={0} isAnimationActive={false} wrapperStyle={{ pointerEvents: "none" }} /><Bar dataKey="estimated_cost" name="本地估算" stackId="cost" fill="#f59e0b" maxBarSize={28} isAnimationActive={false} /><Bar dataKey="reported_cost" name="上游已返回" stackId="cost" fill="#3b82f6" maxBarSize={28} isAnimationActive={false} /><Bar dataKey="reconciled_cost" name="已核对" stackId="cost" fill="#10b981" radius={[3, 3, 0, 0]} maxBarSize={28} isAnimationActive={false} />
        </ComposedChart></ResponsiveContainer>
      )}
    </div>
  </UsageChartPanel>
})

const UsageTokenChart = memo(function UsageTokenChart({ trend, loading, timeZone }: { trend: GatewayRequestLogTrendPoint[]; loading: boolean; timeZone: string }) {
  const rows = useMemo(() => trend.map((point) => ({
    ...point,
    label: formatChartBucket(point.bucket, timeZone),
    cache_ratio: cacheRatioPercent(point),
  })), [timeZone, trend])
  return <UsageChartPanel title="Token 使用趋势">
    {loading ? <UsageChartEmpty text="正在加载 Token 趋势" /> : rows.length === 0 ? <UsageChartEmpty text="暂无 Token 数据" /> : (
      <ResponsiveContainer width="100%" height="100%"><ComposedChart data={rows} syncId="gateway-usage-trend" syncMethod="index" margin={{ top: 8, right: 4, left: -8, bottom: 2 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} /><XAxis dataKey="label" tickLine={false} axisLine={false} tick={usageTrendAxisTick} height={usageTrendXAxisHeight} angle={-35} textAnchor="end" tickMargin={8} minTickGap={8} interval="preserveStartEnd" /><YAxis yAxisId="tokens" tickLine={false} axisLine={false} width={42} tick={usageTrendAxisTick} tickFormatter={(value) => compactNumber(Number(value))} /><YAxis yAxisId="cache" orientation="right" domain={[0, 100]} ticks={[0, 25, 50, 75, 100]} tickLine={false} axisLine={false} width={36} tick={usageTrendAxisTick} tickFormatter={(value) => `${value}%`} /><ChartTooltip content={<UsageOverviewTrendTooltip metric="tokens" />} allowEscapeViewBox={{ x: false, y: false }} animationDuration={0} isAnimationActive={false} wrapperStyle={{ pointerEvents: "none" }} /><Legend iconType="circle" iconSize={7} wrapperStyle={{ fontSize: 10 }} /><Line yAxisId="tokens" type="monotone" dataKey="prompt_tokens" name="输入" stroke="#3b82f6" strokeWidth={2} dot={false} isAnimationActive={false} /><Line yAxisId="tokens" type="monotone" dataKey="completion_tokens" name="输出" stroke="#10b981" strokeWidth={2} dot={false} isAnimationActive={false} /><Line yAxisId="cache" type="monotone" dataKey="cache_ratio" name="缓存比例" stroke="#f59e0b" strokeWidth={2} dot={false} isAnimationActive={false} />
      </ComposedChart></ResponsiveContainer>
    )}
  </UsageChartPanel>
})

function cacheRatioPercent(point: Pick<GatewayRequestLogTrendPoint, "cache_read_tokens" | "prompt_tokens"> | null | undefined) {
  const read = Number(point?.cache_read_tokens ?? 0)
  const prompt = Number(point?.prompt_tokens ?? 0)
  if (!Number.isFinite(prompt) || prompt <= 0) return null
  return Math.min(100, Math.max(0, (read / prompt) * 100))
}

function formatCacheRatioPercent(point: Pick<GatewayRequestLogTrendPoint, "cache_read_tokens" | "prompt_tokens"> | null | undefined) {
  const ratio = cacheRatioPercent(point)
  return ratio == null ? "—" : `${ratio.toFixed(1)}%`
}

const UsageModelChart = memo(function UsageModelChart({ models, loading }: { models: GatewayRequestLogBreakdownItem[]; loading: boolean }) {
  const rows = useMemo(() => models.filter((item) => item.total_tokens > 0).slice(0, 6).map((item) => ({ ...item, label: compactUsageLabel(item.name) })), [models])
  return <UsageChartPanel title="模型 Token 分布">
    {loading ? <UsageChartEmpty text="正在加载模型分布" /> : rows.length === 0 ? <UsageChartEmpty text="暂无模型数据" /> : (
      <ResponsiveContainer width="100%" height="100%"><PieChart><Pie data={rows} dataKey="total_tokens" nameKey="label" {...usageBreakdownDoughnutLayout} paddingAngle={2} stroke="var(--background)" strokeWidth={2} isAnimationActive={false}>{rows.map((item, index) => <Cell key={item.name} fill={usagePieColors[index % usagePieColors.length]} />)}</Pie><ChartTooltip content={<UsageBreakdownChartTooltip metric="tokens" />} /><Legend iconType="circle" iconSize={7} wrapperStyle={{ fontSize: 10 }} formatter={(value) => compactUsageLabel(String(value))} /></PieChart></ResponsiveContainer>
    )}
  </UsageChartPanel>
})

const UsageChannelChart = memo(function UsageChannelChart({ channels, loading }: { channels: GatewayRequestLogBreakdownItem[]; loading: boolean }) {
  const rows = useMemo(() => channels.filter((item) => item.total_cost > 0).slice(0, 6).map((item) => ({ ...item, label: compactUsageLabel(item.name) })), [channels])
  return <UsageChartPanel title="渠道费用分布">
    {loading ? <UsageChartEmpty text="正在加载渠道分布" /> : rows.length === 0 ? <UsageChartEmpty text="暂无渠道数据" /> : (
      <ResponsiveContainer width="100%" height="100%"><PieChart><Pie data={rows} dataKey="total_cost" nameKey="label" {...usageBreakdownDoughnutLayout} paddingAngle={2} stroke="var(--background)" strokeWidth={2} isAnimationActive={false}>{rows.map((item, index) => <Cell key={item.name} fill={usagePieColors[index % usagePieColors.length]} />)}</Pie><ChartTooltip content={<UsageBreakdownChartTooltip metric="cost" />} /><Legend iconType="circle" iconSize={7} wrapperStyle={{ fontSize: 10 }} formatter={(value) => compactUsageLabel(String(value))} /></PieChart></ResponsiveContainer>
    )}
  </UsageChartPanel>
})

function UsageChartPanel({ title, className, children }: { title: string; className?: string; children: ReactNode }) {
  return (
    <div className={cn("min-w-0 bg-background p-4", className)}>
      <h4 className="text-lg font-semibold tracking-tight text-foreground">{title}</h4>
      <div className="mt-3 h-60">{children}</div>
    </div>
  )
}

function UsageChartEmpty({ text }: { text: string }) {
  return <div className="flex h-full items-center justify-center text-xs text-muted-foreground">{text}</div>
}

function UsageOverviewTrendTooltip({ active, payload, label, metric }: { active?: boolean; payload?: Array<{ dataKey?: string; value?: number; payload?: GatewayRequestLogTrendPoint }>; label?: string; metric: "requests" | "cost" | "tokens" }) {
  const className = cn(
    "rounded-lg border border-border bg-popover px-3 py-1 text-xs shadow-md",
    metric === "cost" ? "h-36 w-56" : "h-24 w-48",
  )
  if (!active || !payload?.length) return <div className={className} aria-hidden="true" />
  const point = payload[0]?.payload
  return (
    <div className={className}>
      <div className="font-medium text-foreground">{label}</div>
      <div className="mt-1 grid grid-cols-2 gap-x-4 gap-y-1 text-muted-foreground">
        {metric === "requests" ? (
          <>
            <span>{"请求"}</span><span className="text-right font-mono text-foreground">{formatInteger(point?.requests)}</span>
            <span>{"成功 / 失败 / 取消"}</span><span className="text-right font-mono text-foreground">{`${formatInteger(point?.success)} / ${formatInteger(point?.failed)} / ${formatInteger(point?.canceled)}`}</span>
          </>
        ) : metric === "cost" ? (
          <>
            <span>{"总费用"}</span><span className="text-right font-mono text-foreground">{formatCost(point?.total_cost)}</span>
            <span>{"本地估算"}</span><span className="text-right font-mono text-warning">{formatCost(point?.estimated_cost)}</span>
            <span>{"上游已返回"}</span><span className="text-right font-mono text-primary">{formatCost(point?.reported_cost)}</span>
            <span>{"已核对"}</span><span className="text-right font-mono text-success">{formatCost(point?.reconciled_cost)}</span>
            <span>{"无法估算请求"}</span><span className="text-right font-mono text-warning">{formatInteger(point?.unavailable_requests)}</span>
            <span>{"Token"}</span><span className="text-right font-mono text-foreground">{formatTokenCount(point?.total_tokens)}</span>
          </>
        ) : <>
          <span>{"输入 / 输出"}</span><span className="text-right font-mono text-foreground">{`${formatTokenCount(point?.prompt_tokens)} / ${formatTokenCount(point?.completion_tokens)}`}</span>
          <span>{"缓存比例"}</span><span className="text-right font-mono text-foreground">{formatCacheRatioPercent(point)}</span>
        </>}
      </div>
    </div>
  )
}

function UsageBreakdownChartTooltip({ active, payload, metric }: { active?: boolean; payload?: Array<{ payload?: GatewayRequestLogBreakdownItem & { label?: string } }>; metric: "tokens" | "cost" }) {
  if (!active || !payload?.length) return null
  const item = payload[0]?.payload
  return (
    <div className="rounded-lg border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="max-w-64 break-all font-medium text-foreground">{item?.name || "—"}</div>
      {metric === "tokens" ? (
        <div className="mt-1 text-muted-foreground">{`Token ${formatTokenCount(item?.total_tokens)} · 请求 ${formatInteger(item?.requests)} · 无法估算 ${formatInteger(item?.unavailable_requests)}`}</div>
      ) : (
        <div className="mt-1 grid grid-cols-2 gap-x-4 gap-y-1 text-muted-foreground">
          <span>{"总费用"}</span><span className="text-right font-mono text-foreground">{formatCost(item?.total_cost)}</span>
          <span>{"本地估算"}</span><span className="text-right font-mono text-warning">{formatCost(item?.estimated_cost)}</span>
          <span>{"上游已返回"}</span><span className="text-right font-mono text-primary">{formatCost(item?.reported_cost)}</span>
          <span>{"已核对"}</span><span className="text-right font-mono text-success">{formatCost(item?.reconciled_cost)}</span>
          <span>{"无法估算请求"}</span><span className="text-right font-mono text-warning">{formatInteger(item?.unavailable_requests)}</span>
          <span>{"请求"}</span><span className="text-right font-mono text-foreground">{formatInteger(item?.requests)}</span>
        </div>
      )}
    </div>
  )
}

function SortableUsageHead({ label, sortKey, activeKey, order, onSort, className, align = "left" }: { label: string; sortKey: UsageSortKey; activeKey: UsageSortKey; order: UsageSortOrder; onSort: (key: UsageSortKey) => void; className?: string; align?: "left" | "right" | "center" }) {
  const active = sortKey === activeKey
  const nextOrderLabel = active && order === "desc" ? "升序" : "降序"
  return (
    <TableHead className={className} aria-sort={active ? (order === "asc" ? "ascending" : "descending") : "none"}>
      <button type="button" className={cn("inline-flex w-full items-center gap-1 text-xs font-semibold hover:text-foreground", align === "right" && "justify-end", align === "center" && "justify-center")} onClick={() => onSort(sortKey)} aria-label={`${label}，切换为${nextOrderLabel}`}>
        <span>{label}</span>
        {active ? (order === "asc" ? <ArrowUp className="size-3 text-primary" /> : <ArrowDown className="size-3 text-primary" />) : <ArrowUpDown className="size-3 text-muted-foreground/70" />}
      </button>
    </TableHead>
  )
}

function UsageRequestModels({ log, onOpenDetail }: { log: GatewayRequestLog; onOpenDetail: (log: GatewayRequestLog) => void }) {
  const audit = gatewayModelAudit(log)
  return (
    <button type="button" className="grid w-full min-w-0 grid-cols-[1rem_minmax(0,1fr)] grid-rows-2 items-center gap-x-1.5 gap-y-0.5 rounded text-left leading-5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" onClick={() => onOpenDetail(log)} aria-label={`查看请求 #${log.id} 模型详情`} aria-describedby={`usage-model-description-${log.id}`}>
      <span id={`usage-model-description-${log.id}`} className="sr-only">{`请求模型：${audit.sent || "未记录"}；思考等级：${formatReasoningEffort(log.reasoning_effort) || "未记录"}；上游返回模型：${audit.response || "未报告"}`}</span>
      <ModelFamilyMark family={modelFamilyFromModelName(log.model_name)} />
      <span className="flex min-w-0 items-center gap-1.5"><span className="min-w-0 truncate text-[13px]" title={`请求模型：${audit.sent || "—"}`}><span className="sr-only">请求模型：</span>{audit.sent || "—"}</span>{log.reasoning_effort?.trim() ? <Badge variant="outline" className="h-5 shrink-0 px-1.5 py-0 text-[12px] font-normal leading-4 text-muted-foreground" title={`思考等级：${formatReasoningEffort(log.reasoning_effort)}`}>{formatReasoningEffort(log.reasoning_effort)}</Badge> : null}</span>
      <CornerDownRight className="mx-auto size-3 text-muted-foreground" aria-hidden="true" />
      <span className={cn("min-w-0 truncate text-[13px]", audit.differs ? "text-destructive" : "text-muted-foreground")} title={`上游返回模型：${audit.response || "未报告"}`}><span className="sr-only">上游返回模型：</span>{audit.response || "未报告"}</span>
    </button>
  )
}

function UsageDetailDialog({ log, timeZone, onOpenChange }: { log: GatewayRequestLog | null; timeZone: string; onOpenChange: (open: boolean) => void }) {
  if (!log) return null
  const canceled = isClientCanceledLog(log)
  const connectionEvent = gatewayRequestConnectionEvent(log)
  const successful = usageRouteOutcome(log) === "selected"
  const retryStatus = gatewayRetryStatus(log)
  const successfulFallback = isSuccessfulFallbackLog(log)
  const modelMappingRecorded = hasModelMappingRecord(log)
  const modelAudit = gatewayModelAudit(log)
  const thinkingLeak = hasThinkingLeakWarning(log)
  const usesPriceFact = gatewayCostUsesPriceFact(log)
  const frozenRatios = gatewayFrozenRatios(log)
  const hasFrozenRatio = frozenRatios !== null
  const displayCost = gatewayCostDisplayText(log, formatCost)
  const displayCostSource = gatewayCostDisplaySource(log)
  const costComparison = gatewayCostComparison(log)
  const systemEstimate = gatewayCostSystemEstimate(log)
  const terminalFailureLabel = gatewayTerminalFailureLabel(log.terminal_failure_code)
  const attempts = Array.isArray(log.route_attempts) ? log.route_attempts : []
  const hasRouteExclusionTelemetry = Number.isFinite(log.route_exclusion_snapshot_version)
  const showRouteTrace = hasRouteExclusionTelemetry || attempts.length > 0 || log.route_reason === "fallback" || log.route_reason === "all_failed" || log.route_reason === "session_affinity"
  return (
    <Dialog open onOpenChange={onOpenChange}>
      <DialogContent className="grid h-[calc(100dvh-1rem)] w-[calc(100vw-1rem)] max-w-none grid-rows-[auto_minmax(0,1fr)] gap-0 overflow-hidden p-0 sm:h-[min(820px,calc(100dvh-3rem))] sm:w-[min(1240px,calc(100vw-3rem))] sm:max-w-none sm:p-0">
        <DialogHeader className="border-b border-border px-5 py-4 pr-12">
          <DialogTitle className="text-base">{showRouteTrace ? `请求 #${log.id} · 路由详情` : `请求 #${log.id}`}</DialogTitle>
          <DialogDescription>{`${dateTime(log.created_at, timeZone)} · ${gatewayRequestKeyLabel(log)}`}</DialogDescription>
        </DialogHeader>
        <div className="min-h-0 space-y-3 overflow-y-auto p-4">
          <section aria-label="模型信息" className="grid gap-x-6 gap-y-2 rounded-lg border border-border bg-muted/15 px-4 py-3 sm:grid-cols-2">
            <div className="min-w-0"><div className="mb-1 text-xs text-muted-foreground">请求模型</div><div className="break-all font-mono text-sm">{modelAudit.sent || "—"}</div></div>
            <div className="min-w-0"><div className="mb-1 text-xs text-muted-foreground">上游返回模型</div><div className={cn("break-all font-mono text-sm", modelAudit.differs && "text-destructive")}>{modelAudit.response || "未报告"}</div></div>
            {modelMappingRecorded ? <div className="min-w-0 border-t border-border pt-2 text-xs sm:col-span-2"><span className="text-muted-foreground">映射前 </span><span className="break-all font-mono">{modelWithReasoning(log.requested_model_name, log.requested_reasoning_effort)}</span><span className="ml-3 text-muted-foreground">{modelMappingStrategyLabel(log.model_mapping_strategy)}</span>{log.model_mapping_details ? <p className="mt-1 break-words text-muted-foreground">{log.model_mapping_details}</p> : null}</div> : null}
          </section>
          {retryStatus ? <div className="border-l-2 border-warning bg-warning/5 px-3 py-2 text-xs text-muted-foreground">{retryStatus.detail}</div> : null}
          {connectionEvent ? (
            <div className={cn("rounded-lg border px-4 py-3 text-xs leading-5", gatewayConnectionEventClassName(connectionEvent))}>
              <div className="font-medium">{gatewayConnectionEventLabel(connectionEvent)}</div>
              <div className="mt-1">{gatewayConnectionEventDetail(connectionEvent)}</div>
            </div>
          ) : null}
          {showRouteTrace ? <RouteTraceDetails log={log} timeZone={timeZone} /> : null}
          <div className="grid items-start gap-3 lg:grid-cols-2">
            <UsageDetailSection title="请求信息" icon={<FileText className="size-4 text-primary" />}>
              <UsageDetailLine label="结果" value={canceled ? "客户端取消" : retryStatus?.prompted ? retryStatus.label : successfulFallback ? "成功（已回退）" : successful ? "成功" : `HTTP ${log.status_code}`} />
              <UsageDetailLine label="接口" value={`${log.method} ${log.path}`} mono />
              <UsageDetailLine label="下游 Key ID" value={`#${log.downstream_key_id}`} mono />
              <UsageDetailCopyLine label="上游请求 ID" value={log.upstream_request_id} />
              <UsageDetailCopyLine label="下游 IP" value={log.client_ip} />
              <UsageDetailLine label="首字 / 总耗时" value={`${formatFirstByteLatency(log.first_byte_ms)} / ${formatLatency(log.latency_ms)}`} />
            </UsageDetailSection>
            <UsageDetailSection title="渠道与路由" icon={<Route className="size-4 text-primary" />}>
              <UsageDetailLine label="渠道" value={routeChannelLabel(log)} />
              <UsageDetailLine label="分组" value={log.group_name || "—"} />
              <UsageDetailLine label="上游 Key" value={[log.upstream_key_name, (log.upstream_key_id ?? 0) > 0 ? `#${log.upstream_key_id}` : ""].filter(Boolean).join(" · ") || "—"} mono />
              <UsageDetailLine label="路由原因" value={requestRouteReasonLabel(log)} />
              {(log.route_selection_rank ?? 0) > 0 ? <UsageDetailLine label="正常候选排名" value={`#${log.route_selection_rank}`} /> : null}
              {(log.session_affinity_origin_request_id ?? 0) > 0 ? <UsageDetailLine label="亲和来源" value={`请求 #${log.session_affinity_origin_request_id}`} mono /> : null}
              {(log.session_affinity_origin_request_id ?? 0) > 0 ? <SessionAffinityExpiryDetailLine value={log.session_affinity_expires_at} timeZone={timeZone} /> : null}
              {upstreamModelChanged(log) ? <UsageDetailLine label="上游计费模型" value={log.upstream_model_name || "—"} mono /> : null}
              <UsageDetailLine label="思考程度" value={log.reasoning_effort ? formatReasoningEffort(log.reasoning_effort) : "—"} />
            </UsageDetailSection>
          </div>
          <UsageDetailSection title="Token 与费用" icon={<BarChart3 className="size-4 text-primary" />} contentClassName="grid divide-y divide-border px-0 md:grid-cols-2 md:divide-x md:divide-y-0">
            <div className="divide-y divide-border px-3">
              <UsageDetailLine label="总 Token" value={decimal(log.total_tokens, 0)} mono />
              <UsageDetailLine label="输入 / 输出" value={`${decimal(log.prompt_tokens, 0)} / ${decimal(log.completion_tokens, 0)}`} mono />
              <UsageDetailLine label="缓存写入 / 读取" value={`${decimal(log.cache_creation_tokens, 0)} / ${decimal(log.cache_read_tokens, 0)}`} mono />
              <UsageDetailLine label="缓存比例" value={formatCacheReadRatio(log)} mono />
              {usesPriceFact ? (
                <>
                  <UsageDetailLine label="价格模型" value={log.price_model_name || "—"} mono />
                  <UsageDetailLine label="操作 / 协议" value={priceFactOperationLabel(log)} mono />
                </>
              ) : null}
              {hasFrozenRatio ? <UsageDetailLine label="请求时冻结输入 / 输出倍率" value={`${formatRatio(log.group_ratio)}x / ${formatRatio(frozenRatios?.output ?? log.group_ratio)}x`} mono /> : null}
              {log.raw_group_ratio != null ? <UsageDetailLine label="原始输入 / 输出倍率" value={`${formatRatio(log.raw_group_ratio)}x / ${formatRatio(log.raw_completion_ratio ?? log.raw_group_ratio)}x`} mono /> : null}
              {log.funding_factor != null ? <UsageDetailLine label="资金系数" value={`${formatRatio(log.funding_factor)}x`} mono /> : null}
            </div>
            <div className="divide-y divide-border px-3">
              <UsageDetailLine label="费用状态" value={costStatusLabel(log)} />
              <UsageDetailLine label="列表费用" value={displayCost} mono />
              <UsageDetailLine label="列表来源" value={gatewayCostComparisonLabel(displayCostSource)} />
              <UsageDetailLine label="系统估计费用" value={costAmountText(systemEstimate)} mono />
              {log.calculated_cost == null && systemEstimate == null && log.calculated_cost_status === "unavailable" ? <UsageDetailLine label="计算不可用原因" value={gatewayCostUnavailableReasonText(log.calculated_cost_unavailable_reason)} /> : null}
              <UsageDetailLine label="上游费用" value={costAmountText(log.upstream_cost)} mono />
              {costComparison ? (
                <>
                  <UsageDetailLine label="费用差异" value={costDifferenceText(costComparison.difference)} mono />
                  <UsageDetailLine label="差异比例" value={costDifferenceRatioText(costComparison.ratio)} mono />
                  {costComparison.material ? <UsageDetailLine label="差异标记" value="差异较大，建议核对" /> : null}
                </>
              ) : null}
              {log.cost_status === "unavailable" ? <UsageDetailLine label="无法估算原因" value={gatewayCostUnavailableReasonText(log.cost_unavailable_reason)} /> : null}
              <UsageDetailLine label="费用来源" value={log.cost_source || "未提供"} mono />
              {usesPriceFact ? (
                <>
                  <UsageDetailLine label="计价事实" value={priceFactIdentityLabel(log)} mono />
                  <UsageDetailLine label="币种 / 单位" value={priceFactMoneyLabel(log)} mono />
                  <UsageDetailLine label="价格来源" value={log.price_source || "—"} mono />
                  <UsageDetailLine label="用量口径" value={priceFactUsageLabel(log)} mono />
                </>
              ) : null}
              {isTerminalCostReconcileFailure(log) ? <UsageDetailCopyLine label="未核对原因" value={log.cost_reconcile_error} /> : null}
            </div>
          </UsageDetailSection>
          {thinkingLeak ? (
            <div className="rounded-lg border border-destructive/25 bg-destructive/5 px-4 py-3 text-xs leading-5 text-destructive">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2 font-medium">
                  <TriangleAlert className="size-4 shrink-0" />
                  <span>{"响应思考泄漏"}</span>
                </div>
                <Button type="button" variant="ghost" size="sm" className="h-7 px-2" onClick={() => void copyUsageIdentifier(thinkingLeak, "响应告警")}>
                  <Copy className="size-3.5" />
                  {"复制"}
                </Button>
              </div>
              <div className="mt-1 whitespace-pre-wrap break-words text-foreground">{thinkingLeak}</div>
            </div>
          ) : null}
          {log.error ? (
            <div className={cn("rounded-lg border px-4 py-3 text-xs leading-5", canceled ? "border-border bg-muted/20 text-muted-foreground" : "border-destructive/25 bg-destructive/5 text-destructive")}>
              <div className="flex items-center justify-between gap-3">
                <div className="font-medium">{canceled ? "取消信息" : terminalFailureLabel || "错误信息"}</div>
                <Button type="button" variant="ghost" size="sm" className="h-7 px-2" onClick={() => void copyUsageIdentifier(gatewayErrorLabel(log.error), canceled ? "取消信息" : "错误信息")}>
                  <Copy className="size-3.5" />
                  {"复制"}
                </Button>
              </div>
              <div className="mt-1 whitespace-pre-wrap break-words font-mono">{gatewayErrorLabel(log.error)}</div>
            </div>
          ) : null}
        </div>
      </DialogContent>
    </Dialog>
  )
}

function RouteTraceDetails({ log, timeZone }: { log: GatewayRequestLog; timeZone: string }) {
  const attempts = Array.isArray(log.route_attempts) ? log.route_attempts : []
  const affinityOriginRequestID = log.session_affinity_origin_request_id ?? 0
  const outcome = usageRouteOutcome(log)
  const canceled = outcome === "canceled"
  const selected = outcome === "selected"
  const selectedLocation = gatewayRouteAttemptLocation({
    channel_id: log.channel_id,
    channel_name: log.channel_name,
    group_name: log.group_name,
    reason: "",
    physically_dispatched: false,
    first_byte_budget_ms: 0,
  })
  const routeTraceCopyText = gatewayRouteTraceCopyText(attempts, {
    channelID: log.channel_id,
    channelName: log.channel_name,
    groupName: log.group_name,
    eligibleRank: log.route_selection_rank,
    outcome,
    affinityOriginRequestID,
    affinityExpiresAt: log.session_affinity_expires_at,
    affinityOverriddenCandidates: log.session_affinity_overridden_candidates,
    includeMissingAttemptsNotice: log.route_reason === "fallback" || log.route_reason === "all_failed",
  })
  const copyText = `${routeTraceCopyText}\n\n${gatewayRouteExclusionSnapshotCopyText(log, (value) => dateTime(value, timeZone))}`
  return (
    <section aria-label="路由轨迹" className="overflow-hidden rounded-lg border border-border bg-background text-[13px] leading-5">
      <div className="flex items-center justify-between gap-3 border-b border-border bg-muted/20 px-3 py-2">
        <div className="flex min-w-0 items-center gap-2 text-sm font-semibold text-foreground">
          <Route className="size-4 shrink-0 text-primary" />
          <span>{"路由轨迹"}</span>
          {attempts.length > 0 ? <span className="text-xs font-normal text-muted-foreground">{`${attempts.length} 次失败尝试`}</span> : null}
        </div>
        <Button type="button" variant="ghost" size="sm" className="h-7 shrink-0 px-2" onClick={() => void copyUsageIdentifier(copyText, "路由轨迹")}>
          <Copy className="size-3.5" />
          {"复制"}
        </Button>
      </div>
      <div className="divide-y divide-border">
        {affinityOriginRequestID > 0 ? <SessionAffinityRouteDetails log={log} timeZone={timeZone} /> : null}
        <RouteExclusionSnapshotDetails log={log} timeZone={timeZone} />
        {attempts.length > 0 ? (
          <ol className="divide-y divide-border" aria-label="失败候选尝试顺序">
            {attempts.map((attempt, index) => (
              <li key={`${attempt.channel_id ?? "unknown"}:${attempt.group_name ?? ""}:${index}`} className="grid grid-cols-[1.5rem_minmax(0,1fr)] items-start gap-x-2 px-3 py-2 text-foreground">
                <span className="flex size-5 items-center justify-center rounded bg-warning/10 font-mono text-xs text-warning">{index + 1}</span>
                <div className="min-w-0 space-y-1">
                  <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
                    <span className="min-w-0 font-medium [overflow-wrap:anywhere]">{gatewayRouteAttemptLocation(attempt)}</span>
                    <span className="flex shrink-0 flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
                      {(attempt.status_code ?? 0) > 0 ? <span className="font-mono text-warning">{`HTTP ${attempt.status_code}`}</span> : null}
                      {attempt.physically_dispatched === true ? <span>已发起</span> : null}
                      {(attempt.eligible_rank ?? 0) > 0 ? <span>{`正常排名 #${attempt.eligible_rank}`}</span> : null}
                      {gatewayRouteAttemptBudgetLabel(attempt.first_byte_budget_ms) ? <span className="tabular-nums">{`首字预算 ${gatewayRouteAttemptBudgetLabel(attempt.first_byte_budget_ms)}`}</span> : null}
                    </span>
                  </div>
                  <div className="text-xs text-muted-foreground">{gatewayRouteAttemptReasonLabel(attempt.reason)}</div>
                  {String(attempt.summary ?? "").trim() ? (
                    <div className="max-h-24 overflow-y-auto whitespace-pre-wrap text-xs text-foreground [overflow-wrap:anywhere]" aria-label={`第 ${index + 1} 次尝试的具体故障`}>{String(attempt.summary).trim()}</div>
                  ) : null}
                </div>
              </li>
            ))}
          </ol>
        ) : log.route_reason === "fallback" || log.route_reason === "all_failed" ? (
          <div className="px-3 py-2 text-xs text-muted-foreground">{"历史记录未保存结构化失败轨迹，不展示未结构化原文。"}</div>
        ) : null}
        <div className="grid grid-cols-[1.5rem_minmax(0,1fr)] items-center gap-2 bg-muted/10 px-3 py-2">
          {selected ? (
            <span className="flex size-5 items-center justify-center text-success"><CheckCircle2 className="size-4" /></span>
          ) : (
            <span className={cn("flex size-5 items-center justify-center", canceled ? "text-muted-foreground" : "text-destructive")}><XCircle className="size-4" /></span>
          )}
          <div className="flex min-w-0 flex-wrap items-baseline gap-x-3 gap-y-1">
            <div className={cn("font-medium", selected ? "text-success" : canceled ? "text-muted-foreground" : "text-destructive")}>{selected ? "最终命中" : canceled ? "客户端取消" : "未命中可用候选"}</div>
            {selected ? <div className="min-w-0 font-medium text-foreground [overflow-wrap:anywhere]">{selectedLocation}</div> : null}
            {selected && (log.route_selection_rank ?? 0) > 0 ? <div className="ml-auto text-xs text-muted-foreground">{`正常排名 #${log.route_selection_rank}`}</div> : null}
          </div>
        </div>
      </div>
    </section>
  )
}

function SessionAffinityRouteDetails({ log, timeZone }: { log: GatewayRequestLog; timeZone: string }) {
  const [now, setNow] = useState(() => Date.now())
  const expiresAt = String(log.session_affinity_expires_at ?? "").trim()
  const candidates = Array.isArray(log.session_affinity_overridden_candidates) ? log.session_affinity_overridden_candidates : []

  useEffect(() => {
    if (!expiresAt) return
    const timer = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [expiresAt])

  return (
    <div className="grid grid-cols-[1.5rem_minmax(0,1fr)] gap-2 px-3 py-2">
      <span className="flex size-5 items-center justify-center text-primary"><Link2 className="size-3.5" /></span>
      <div className="min-w-0">
        <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1"><span className="font-medium text-primary">会话亲和来源</span><span className="font-mono text-foreground">{`请求 #${log.session_affinity_origin_request_id}`}</span><span className="ml-auto text-xs text-muted-foreground">{sessionAffinityExpirySummary(expiresAt, now, timeZone)}</span></div>
        {candidates.length > 0 ? (
          <div className="mt-2">
            <div className="text-xs text-muted-foreground">{"被会话亲和覆盖的正常候选"}</div>
            <ul className="mt-1 divide-y divide-border/70" aria-label="被会话亲和覆盖的正常候选">
              {candidates.map((candidate) => {
                const channel = String(candidate.channel_name ?? "").trim() || "未命名渠道"
                const group = String(candidate.group_name ?? "").trim()
                return <li key={`${candidate.channel_id}:${group}:${candidate.eligible_rank}`} className="flex flex-wrap items-center justify-between gap-x-4 gap-y-1 py-1"><span className="min-w-0 font-medium text-foreground [overflow-wrap:anywhere]">{group ? `${channel} / ${group}` : channel}</span><span className="shrink-0 text-xs text-muted-foreground">{`正常排名 #${candidate.eligible_rank}`}</span></li>
              })}
            </ul>
          </div>
        ) : (log.route_selection_rank ?? 0) === 1 ? (
          <div className="mt-2 text-muted-foreground">{"该亲和路由在当时也是正常候选第一位，未覆盖更高优先级候选。"}</div>
        ) : (
          <div className="mt-2 text-muted-foreground">{"该记录未保存被会话亲和覆盖的候选明细。"}</div>
        )}
      </div>
    </div>
  )
}

function SessionAffinityExpiryDetailLine({ value, timeZone }: { value?: string | null; timeZone: string }) {
  const [now, setNow] = useState(() => Date.now())
  const expiresAt = String(value ?? "").trim()
  useEffect(() => {
    if (!expiresAt) return
    const timer = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [expiresAt])
  return <UsageDetailLine label="亲和有效期" value={sessionAffinityExpirySummary(expiresAt, now, timeZone)} />
}

function sessionAffinityExpirySummary(value: string | null | undefined, now: number, timeZone: string) {
  const expiresAt = String(value ?? "").trim()
  const expiresMillis = Date.parse(expiresAt)
  if (!expiresAt || !Number.isFinite(expiresMillis)) return "历史记录未保存会话亲和有效期。"
  return `有效至 ${dateTime(expiresAt, timeZone)} · ${gatewaySessionAffinityRemaining(expiresAt, now)}`
}

function RouteExclusionSnapshotDetails({ log, timeZone }: { log: GatewayRequestLog; timeZone: string }) {
  const state = gatewayRouteExclusionSnapshotState(log)
  if (state.kind === "not_collected") {
    return (
      <div className="px-3 py-2">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          <Info className="size-3.5 shrink-0 text-muted-foreground" />
          <span className="font-medium text-foreground">{"候选排除快照"}</span>
          <Badge variant="outline" className="h-5 px-1.5 text-[10px] font-normal text-muted-foreground">{"未采集 / N/A"}</Badge>
          <span className="text-xs text-muted-foreground">历史记录未保存，不使用当前状态回填。</span>
        </div>
      </div>
    )
  }
  if (state.kind === "unavailable") {
    return (
      <div className="px-3 py-2">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          <TriangleAlert className="size-3.5 shrink-0 text-warning" />
          <span className="font-medium text-foreground">{"候选排除快照"}</span>
          <Badge variant="outline" className="h-5 border-warning/30 px-1.5 text-[10px] font-normal text-warning">{"不可用 / N/A"}</Badge>
          {state.version > 0 ? <Badge variant="outline" className="h-5 px-1.5 font-mono text-[10px] font-normal text-muted-foreground">{`v${state.version}`}</Badge> : null}
          <span className="text-xs text-muted-foreground">快照缺失、损坏或版本不受支持，不使用当前状态回填。</span>
        </div>
      </div>
    )
  }

  const coverage = gatewayRouteExclusionCoverageLabel(state)
  return (
    <div className="px-3 py-2">
      <div className="flex flex-wrap items-start justify-between gap-x-4 gap-y-1">
        <div className="flex flex-wrap items-center gap-2">
          <Info className="size-3.5 shrink-0 text-primary" />
          <span className="font-medium text-foreground">{"候选排除快照"}</span>
          <Badge variant="outline" className="h-5 px-1.5 font-mono text-[10px] font-normal text-muted-foreground">{`v${state.snapshot.version}`}</Badge>
          <Badge variant="outline" className={cn("h-5 px-1.5 text-[10px] font-normal", state.snapshot.truncated ? "border-warning/30 text-warning" : "text-muted-foreground")}>{coverage}</Badge>
        </div>
        <span className="text-xs text-muted-foreground">{`采集于 ${dateTime(state.snapshot.captured_at, timeZone)}`}</span>
      </div>
      {state.snapshot.entries.length === 0 ? (
        <div className="mt-1 text-xs text-muted-foreground">{"该快照未记录结构化排除项。"}</div>
      ) : (
        <ul className="mt-1 divide-y divide-border/70" aria-label="决策时的候选排除项">
          {state.snapshot.entries.map((entry, index) => {
            const cooldown = gatewayRouteExclusionCooldownLabel(entry, (value) => dateTime(value, timeZone))
            return (
              <li key={`${entry.channel_id}:${entry.group_name}:${entry.reason}:${index}`} className="grid gap-1 py-1.5 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)] sm:gap-x-4">
                <div className="min-w-0">
                  <div className="font-medium text-foreground [overflow-wrap:anywhere]">{gatewayRouteExclusionLocation(entry)}</div>
                  <div className="text-xs text-muted-foreground">{gatewayRouteExclusionReasonLabel(entry.reason)}</div>
                </div>
                <div className="min-w-0 text-xs text-muted-foreground sm:text-right">
                  <div className="font-mono text-foreground [overflow-wrap:anywhere]">{gatewayRouteExclusionGenerationLabel(entry)}</div>
                  {cooldown ? <div className="text-warning [overflow-wrap:anywhere]">{cooldown}</div> : null}
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}


function UsageDetailSection({ title, icon, children, contentClassName }: { title: string; icon: ReactNode; children: ReactNode; contentClassName?: string }) {
  return (
    <section className="overflow-hidden rounded-lg border border-border">
      <div className="flex items-center gap-2 border-b border-border bg-muted/20 px-3 py-2 text-sm font-semibold">{icon}{title}</div>
      <div className={cn("divide-y divide-border px-3", contentClassName)}>{children}</div>
    </section>
  )
}

function UsageDetailLine({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="grid grid-cols-[7.5rem_minmax(0,1fr)] gap-3 py-1.5 text-[13px] sm:grid-cols-[8.5rem_minmax(0,1fr)]">
      <span className="text-muted-foreground">{label}</span>
      <span className={cn("min-w-0 text-left text-foreground [overflow-wrap:anywhere]", mono && "font-mono")}>{value}</span>
    </div>
  )
}

function UsageDetailCopyLine({ label, value }: { label: string; value?: string }) {
  const displayValue = value || "—"
  return (
    <div className="grid grid-cols-[7.5rem_minmax(0,1fr)] gap-3 py-1.5 text-[13px] sm:grid-cols-[8.5rem_minmax(0,1fr)]">
      <span className="text-muted-foreground">{label}</span>
      <div className="flex min-w-0 items-start justify-start gap-1.5">
        <span className="min-w-0 break-all text-left font-mono text-foreground">{displayValue}</span>
        {value ? (
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            className="-my-1 size-6 shrink-0"
            onClick={() => void copyUsageIdentifier(value, label)}
            aria-label={`复制${label}`}
            title={`复制${label}`}
          >
            <Copy className="size-3" />
          </Button>
        ) : null}
      </div>
    </div>
  )
}

function formatChartBucket(value: string | undefined, timeZone: string) {
  if (!value) return "—"
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value.slice(5).replace("-", "/")
  return /[T ]\d{2}:\d{2}/.test(value) ? compactDateTime(value, timeZone) : value
}

function compactUsageLabel(value?: string) {
  const text = String(value || "未命名")
  return text.length > 12 ? `${text.slice(0, 11)}…` : text
}

function compactNumber(value: number) {
  if (!Number.isFinite(value)) return "0"
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}k`
  return String(Math.round(value))
}

const UsageLogRow = memo(function UsageLogRow({
  log,
  timeZone,
  visibleColumns,
  channelHomeURL,
  manualCooldownMinutes,
  cooling,
  onManualCooldown,
  onOpenDetail,
}: {
  log: GatewayRequestLog
  timeZone: string
  visibleColumns: Record<UsageColumnKey, boolean>
  channelHomeURL?: string
  manualCooldownMinutes: number
  cooling: boolean
  onManualCooldown: (log: GatewayRequestLog) => void
  onOpenDetail: (log: GatewayRequestLog) => void
}) {
  const clientCanceled = isClientCanceledLog(log)
  const connectionEvent = gatewayRequestConnectionEvent(log)
  const errorLabel = gatewayErrorLabel(log.error)
  const retryStatus = gatewayRetryStatus(log)
  const fullTime = dateTime(log.created_at, timeZone)
  const channelLabel = routeChannelLabel(log)
  const routeLabel = log.route_reason === "fallback" && !gatewayRequestHasRealFallback(log) ? "" : log.route_reason ? routeReasonLabel(log.route_reason) : ""
  const frozenRatios = gatewayFrozenRatios(log)
  const hasFrozenRatio = frozenRatios !== null
  const displayCost = gatewayCostDisplayText(log, formatCost)
  const displayCostSource = gatewayCostDisplaySource(log)
  const costComparison = gatewayCostComparison(log)
  const costTitle = displayCostSource === "ratio_estimate"
    ? `${displayCost}：按请求冻结倍率估算，最终以上游对账为准`
    : log.cost_status === "unavailable" ? `${displayCost}：${gatewayCostUnavailableReasonText(log.cost_unavailable_reason)}` : displayCost
  return (
    <TableRow className="align-middle hover:bg-muted/30">
      <TableCell className="py-2">
        <div className="grid grid-rows-2 gap-0.5 leading-5">
          <div className="truncate font-medium" title={gatewayRequestKeyLabel(log)}>
            {gatewayRequestKeyLabel(log)}
          </div>
          <span className="truncate text-muted-foreground tabular-nums" title={`${fullTime} · ${log.method} ${log.path}`}>{fullTime}</span>
        </div>
      </TableCell>
      {visibleColumns.clientIp ? (
        <TableCell className="py-2">
          <GatewayClientIpCell key={`${log.id}:${log.client_ip || "missing"}`} requestLogID={log.id} ip={log.client_ip} source={log.client_ip_source} />
        </TableCell>
      ) : null}
      {visibleColumns.channel ? (
        <TableCell className="py-2">
          <div className="grid grid-rows-2 gap-0.5 leading-5">
            <div className="flex min-w-0 items-center gap-1">
              <span className="min-w-0 truncate font-medium" title={channelLabel}>{channelLabel}</span>
              {channelHomeURL ? (
                <Tooltip delayDuration={150}>
                  <TooltipTrigger asChild>
                    <a href={channelHomeURL} target="_blank" rel="noreferrer noopener" className="inline-flex size-5 shrink-0 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-primary/10 hover:text-primary focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring" aria-label={`打开 ${channelLabel} 渠道主页`}>
                      <ExternalLink className="size-3" />
                    </a>
                  </TooltipTrigger>
                  <TooltipContent>{"打开渠道主页"}</TooltipContent>
                </Tooltip>
              ) : null}
              {routeLabel ? (
                isSuccessfulFallbackLog(log) ? (
                  <button type="button" className="shrink-0" onClick={() => onOpenDetail(log)} aria-label={`查看请求 #${log.id} 回退详情`} title="查看回退详情">
                    <Badge variant="outline" className="h-5 px-1.5 py-0 text-[11px]! font-normal text-muted-foreground transition-colors hover:border-warning/50 hover:bg-warning/5 hover:text-warning">{routeLabel}</Badge>
                  </button>
                ) : <Badge variant="outline" className="h-5 shrink-0 px-1.5 py-0 text-[11px]! font-normal text-muted-foreground" title={routeLabel}>{routeLabel}</Badge>
              ) : null}
            </div>
            <div className="flex h-5 min-w-0 items-center">
              {log.group_name ? <Badge variant="outline" className="h-5 max-w-full truncate px-1.5 py-0 text-[11px]! font-normal text-muted-foreground" title={log.group_name}>{log.group_name}</Badge> : null}
            </div>
          </div>
        </TableCell>
      ) : null}
      <TableCell className="py-2">
        <UsageRequestModels log={log} onOpenDetail={onOpenDetail} />
      </TableCell>
      {visibleColumns.tokens ? (
        <TableCell className="w-48 py-2">
          <div className="grid grid-rows-2 gap-0.5 text-xs leading-5">
            <div className="flex h-5 items-center gap-2 whitespace-nowrap tabular-nums" title={`总 ${decimal(log.total_tokens, 0)} · 输入 ${decimal(log.prompt_tokens, 0)} · 输出 ${decimal(log.completion_tokens, 0)}`}>
              <span className="inline-flex shrink-0 items-center gap-1 text-xs text-muted-foreground" title="输入 Token">
                <ArrowDown className="size-3 text-emerald-500" />
                {formatTokenCount(log.prompt_tokens)}
              </span>
              <span className="inline-flex shrink-0 items-center gap-1 text-xs text-muted-foreground" title="输出 Token">
                <ArrowUp className="size-3 text-violet-500" />
                {formatTokenCount(log.completion_tokens)}
              </span>
            </div>
            <div className="flex h-5 min-w-0 items-center gap-2 whitespace-nowrap text-xs">
              <Badge variant="outline" className="h-5 shrink-0 border-amber-500/30 px-1.5 text-[10px] text-amber-600 dark:text-amber-400" title={`缓存读取 ${decimal(log.cache_read_tokens, 0)} / 输入 ${decimal(log.prompt_tokens, 0)} · 缓存写入 ${decimal(log.cache_creation_tokens, 0)}`}>
                {`缓存 ${formatCacheReadRatio(log)}`}
              </Badge>

            </div>
          </div>
        </TableCell>
      ) : null}
      {visibleColumns.ratios ? (
        <TableCell className="py-2 text-center">
          <span className="leading-5 tabular-nums" title={`请求时冻结倍率：${hasFrozenRatio ? `输入 ${formatRatio(frozenRatios.input)}x / 输出 ${formatRatio(frozenRatios.output)}x` : "未记录"}；资金系数 ${log.funding_factor == null ? "未记录" : formatRatio(log.funding_factor) + "x"}，已计入；原始输入 ${log.raw_group_ratio == null ? "未记录" : formatRatio(log.raw_group_ratio) + "x"} / 输出 ${log.raw_completion_ratio == null ? "未记录" : formatRatio(log.raw_completion_ratio) + "x"}`}>
            {hasFrozenRatio ? `${formatRatio(frozenRatios.input)}x` : "—"}
          </span>
        </TableCell>
      ) : null}
      {visibleColumns.cost ? (
        <TableCell className="py-2 text-right">
          <div className="space-y-0.5 leading-5">
            <div className="flex h-5 items-center justify-end gap-1">
              <span className="font-medium tabular-nums" title={costTitle}>{displayCost}</span>
              <CostDetailTooltip log={log} />
            </div>
            <div className="flex h-5 items-center justify-end gap-1">
              <CostStatusBadge log={log} />
              {costComparison?.material ? <button type="button" onClick={() => onOpenDetail(log)} title="费用差异超过 5%，查看明细"><Badge variant="outline" className="h-5 border-warning/40 text-warning">{"差异"}</Badge></button> : null}
            </div>
          </div>
        </TableCell>
      ) : null}
      {visibleColumns.status ? (
        <TableCell className="w-26 py-2 text-center">
          <div className="flex min-h-[2.625rem] flex-col items-center justify-center gap-0.5 [&_[data-slot=badge]]:h-5 [&_[data-slot=badge]]:py-0 [&_[data-slot=badge]]:text-[12px]! [&_[data-slot=badge]]:leading-4 [&>button]:h-5">
            <Badge className="tabular-nums" variant={clientCanceled || (log.status_code >= 200 && log.status_code < 400) ? "outline" : "destructive"}>
              {clientCanceled ? "客户端取消" : log.status_code}
            </Badge>
            {connectionEvent ? (
              <button type="button" onClick={() => onOpenDetail(log)} aria-label={`查看请求 #${log.id} ${gatewayConnectionEventLabel(connectionEvent)}`} title={gatewayConnectionEventDetail(connectionEvent)}>
                <Badge variant="outline" className={cn("max-w-full truncate", gatewayConnectionEventClassName(connectionEvent))}>{gatewayConnectionEventLabel(connectionEvent)}</Badge>
              </button>
            ) : null}
            {retryStatus && !clientCanceled ? (
              <button type="button" onClick={() => onOpenDetail(log)} aria-label={`查看请求 #${log.id} ${retryStatus.detail}`} title={retryStatus.detail}>
                <Badge variant="outline" className="border-warning/40 bg-warning/5 text-warning">{retryStatus.label}</Badge>
              </button>
            ) : null}
            {errorLabel && !clientCanceled && !retryStatus?.prompted ? (
              <button type="button" onClick={() => onOpenDetail(log)} aria-label={`查看请求 #${log.id} 异常详情`} title="查看异常详情">
                <Badge variant="destructive">{"异常"}</Badge>
              </button>
            ) : hasThinkingLeakWarning(log) ? (
              <button type="button" onClick={() => onOpenDetail(log)} aria-label={`查看请求 #${log.id} 响应思考泄漏`} title="查看响应思考泄漏">
                <Badge variant="destructive">{"思考泄漏"}</Badge>
              </button>
            ) : isSuccessfulFallbackLog(log) ? (
              <button type="button" onClick={() => onOpenDetail(log)} aria-label={`查看请求 #${log.id} 上游回退记录`} title="查看上游回退记录">
                <Badge variant="outline" className="border-warning/40 bg-warning/5 text-warning">{"已回退"}</Badge>
              </button>
            ) : null}
          </div>
        </TableCell>
      ) : null}
      {visibleColumns.latency ? (
        <TableCell className="w-30 whitespace-nowrap py-2 text-xs tabular-nums">
          <div className="space-y-0.5 leading-5">
            <div className="flex h-5 items-center gap-1">
              <span
                className={cn("min-w-0 truncate", firstByteLatencyClassName(log.first_byte_ms))}
                title={firstByteLatencyTitle(log.first_byte_ms)}
              >
                {formatFirstByteLatency(log.first_byte_ms)}
              </span>
              {gatewayRequestCanBeManuallyCooled(log) ? (
                <Tooltip delayDuration={150}>
                  <TooltipTrigger asChild>
                    <span className="inline-flex size-5 shrink-0">
                      <Button
                        type="button"
                        size="icon-sm"
                        variant="ghost"
                        className="size-5 p-0 text-sky-600 hover:bg-sky-500/10 hover:text-sky-700 dark:text-sky-400 dark:hover:text-sky-300"
                        disabled={cooling}
                        onClick={() => onManualCooldown(log)}
                        aria-label={`将请求 #${log.id} 命中的渠道和分组手动冷却`}
                      >
                        <Snowflake className={cn("size-3", cooling && "animate-spin")} />
                      </Button>
                    </span>
                  </TooltipTrigger>
                  <TooltipContent>
                    {cooling ? "正在应用手动冷却" : `将本次请求实际命中的渠道和分组暂停智能路由 ${manualCooldownMinutes} 分钟`}
                  </TooltipContent>
                </Tooltip>
              ) : null}
            </div>
            <div className="text-muted-foreground" title="从请求开始到响应完成的总耗时">{`总 ${formatLatency(log.latency_ms)}`}</div>
          </div>
        </TableCell>
      ) : null}
      <TableCell className="w-13 py-2 text-center">
        <Button type="button" size="icon-sm" variant="ghost" onClick={() => onOpenDetail(log)} aria-label={`查看请求 #${log.id} 详情`} title="查看完整详情">
          <FileText className="size-3.5" />
        </Button>
      </TableCell>
    </TableRow>
  )
})

function hasThinkingLeakWarning(log: GatewayRequestLog) {
  if (log.thinking_leak_warning) {
    return "该渠道把内部思考标签作为普通正文返回。BridgeOps 已记录命中渠道、分组、模型和路由信息；完整对话正文未写入日志。"
  }
  const details = log.route_details || ""
  if (!details.includes("<thinking>")) return ""
  return "该渠道把内部思考标签作为普通正文返回。BridgeOps 已记录命中渠道、分组、模型和路由信息；完整对话正文未写入日志。"
}

async function copyUsageIdentifier(value: string, label: string) {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(value)
    } else {
      fallbackCopyUsageIdentifier(value)
    }
    toast.success(`${label}已复制`)
  } catch {
    try {
      fallbackCopyUsageIdentifier(value)
      toast.success(`${label}已复制`)
    } catch {
      toast.error("复制失败")
    }
  }
}

function fallbackCopyUsageIdentifier(value: string) {
  const textarea = document.createElement("textarea")
  const previousFocus = document.activeElement
  textarea.value = value
  textarea.style.position = "fixed"
  textarea.style.opacity = "0"
  // Keep the selection inside the active dialog's focus trap on HTTP previews.
  const container = document.querySelector('[data-slot="dialog-content"][data-state="open"]') ?? document.body
  container.appendChild(textarea)
  try {
    textarea.focus()
    textarea.select()
    if (!document.execCommand("copy")) throw new Error("copy failed")
  } finally {
    textarea.remove()
    if (previousFocus instanceof HTMLElement && previousFocus.isConnected) previousFocus.focus()
  }
}

function CostStatusBadge({ log }: { log: GatewayRequestLog }) {
  if (isTerminalCostReconcileFailure(log)) {
    return <Badge variant="outline" className="h-5 border-warning/40 px-1.5 text-[10px] text-warning" title="费用核对未完成，不代表请求失败；请在请求详情中查看原因">{"未核对"}</Badge>
  }
  if (gatewayCostDisplaySource(log) === "ratio_estimate") {
    return <Badge variant="outline" className="h-5 border-warning/40 px-1.5 text-[10px] text-warning" title="按请求冻结倍率估算，仅供即时参考，最终以上游对账为准">{"估"}</Badge>
  }
  switch (log.cost_status) {
    case "estimated":
      return <Badge variant="outline" className="h-5 border-warning/40 px-1.5 text-[10px] text-warning" title="按本地计价规则估算，等待上游返回">{"估"}</Badge>
    case "unavailable":
      return <Badge variant="outline" className="h-5 border-warning/40 px-1.5 text-[10px] text-warning" title={`无法估算：${gatewayCostUnavailableReasonText(log.cost_unavailable_reason)}`}>{"缺"}</Badge>
    case "reported":
      return <Badge variant="outline" className="h-5 border-primary/30 px-1.5 text-[10px] text-primary" title="上游已返回费用，等待核对">{"报"}</Badge>
    case "reconciled":
      return <Badge variant="outline" className="h-5 border-success/30 px-1.5 text-[10px] text-success" title="已与上游用量记录核对">{"验"}</Badge>
    default:
      return null
  }
}

function isTerminalCostReconcileFailure(log: GatewayRequestLog) {
  return Boolean(log.cost_reconcile_error?.trim()) && !log.cost_reconcile_at
}

function costStatusLabel(log: GatewayRequestLog) {
  if (isTerminalCostReconcileFailure(log)) return "费用未核对"
  if (gatewayCostDisplaySource(log) === "ratio_estimate") return "倍率估算（非账单）"
  return gatewayCostStatusText(log.cost_status)
}

function CostDetailTooltip({ log }: { log: GatewayRequestLog }) {
  const usesPriceFact = gatewayCostUsesPriceFact(log)
  const ratioEstimate = usesPriceFact ? null : gatewayCostRatioEstimate(log)
  const frozenRatios = gatewayFrozenRatios(log)
  const hasFrozenRatio = frozenRatios !== null
  const displayCost = gatewayCostDisplayText(log, formatCost)
  const displayCostSource = gatewayCostDisplaySource(log)
  const costComparison = gatewayCostComparison(log)
  const systemEstimate = gatewayCostSystemEstimate(log)
  return (
    <Tooltip delayDuration={150}>
      <TooltipTrigger asChild>
        <button
          type="button"
          className="inline-flex size-5 shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground transition-colors hover:bg-primary/10 hover:text-primary focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring"
          aria-label="查看费用明细"
        >
          <Info className="size-3.5" />
        </button>
      </TooltipTrigger>
      <TooltipContent side="left" className="max-w-xs bg-popover p-3 text-popover-foreground shadow-lg">
        <div className="space-y-2 text-xs">
          <div className="flex items-center justify-between gap-4 font-medium">
            <span>{"总费用"}</span>
            <span className={cn("font-mono", log.cost_status === "unavailable" ? "text-warning" : log.cost_status === "none" ? "text-muted-foreground" : "text-emerald-600 dark:text-emerald-400")}>{displayCost}</span>
          </div>
          <CostDetailLine label="费用状态" value={costStatusLabel(log)} />
          <CostDetailLine label="列表来源" value={gatewayCostComparisonLabel(displayCostSource)} />
          <CostDetailLine label="系统估计费用" value={costAmountText(systemEstimate)} />
          {log.calculated_cost == null && systemEstimate == null && log.calculated_cost_status === "unavailable" ? <CostDetailLine label="计算不可用原因" value={gatewayCostUnavailableReasonText(log.calculated_cost_unavailable_reason)} /> : null}
          <CostDetailLine label="上游费用" value={costAmountText(log.upstream_cost)} />
          {costComparison ? (
            <>
              <CostDetailLine label="费用差异" value={costDifferenceText(costComparison.difference)} />
              <CostDetailLine label="差异比例" value={costDifferenceRatioText(costComparison.ratio)} />
              {costComparison.material ? <CostDetailLine label="差异标记" value="差异较大，建议核对" /> : null}
            </>
          ) : null}
          {log.cost_status === "unavailable" ? <CostDetailLine label="无法估算原因" value={gatewayCostUnavailableReasonText(log.cost_unavailable_reason)} /> : null}
          <CostDetailLine label="费用来源" value={log.cost_source || "未提供"} />
          <CostDetailLine label="输入 Token" value={decimal(log.prompt_tokens, 0)} />
          <CostDetailLine label="输出 Token" value={decimal(log.completion_tokens, 0)} />
          <CostDetailLine label="缓存写入 Token" value={decimal(log.cache_creation_tokens, 0)} />
          <CostDetailLine label="缓存读取 Token" value={decimal(log.cache_read_tokens, 0)} />
          {usesPriceFact ? (
            <>
              <CostDetailLine label="计价事实" value={priceFactIdentityLabel(log)} />
              <CostDetailLine label="价格模型" value={log.price_model_name || "—"} />
              <CostDetailLine label="操作 / 协议" value={priceFactOperationLabel(log)} />
              <CostDetailLine label="币种 / 单位" value={priceFactMoneyLabel(log)} />
              <CostDetailLine label="价格来源" value={log.price_source || "—"} />
              <CostDetailLine label="用量口径" value={priceFactUsageLabel(log)} />
            </>
          ) : null}
          {hasFrozenRatio ? (
            <>
              <CostDetailLine label="请求时冻结输入倍率" value={`${formatRatio(log.group_ratio)}x`} />
              <CostDetailLine label="请求时冻结输出倍率" value={`${formatRatio(frozenRatios?.output ?? log.group_ratio)}x`} />
            </>
          ) : null}
          {ratioEstimate !== null ? (
            <div className="rounded-md border border-border/80 bg-muted/40 p-2">
              <div className="mb-1 text-[11px] text-muted-foreground">{"冻结倍率估算拆分（非权威账单）"}</div>
              <CostDetailLine label="输入约" value={formatCost(ratioEstimate.input)} />
              <CostDetailLine label="输出约" value={formatCost(ratioEstimate.output)} />
            </div>
          ) : null}
          <p className="text-[11px] leading-4 text-muted-foreground">
            {displayCostSource === "ratio_estimate"
              ? "当前缺少可验证价格事实或上游费用，列表按请求时冻结倍率即时估算；该金额不写入权威账单，最终以上游对账为准。"
              : log.cost_status === "unavailable"
              ? "该记录没有可验证的计价依据；数据库中可能保留的旧金额只用于审计，不作为账单展示。"
              : usesPriceFact
                ? "费用按请求派发时冻结的价格事实与用量口径计算，旧倍率不参与本次账单展示。"
                : "如果上游只返回总费用，BridgeOps 会展示可核对字段；精确输入/输出/cache 分项取决于上游是否返回对应明细。"}
          </p>
        </div>
      </TooltipContent>
    </Tooltip>
  )
}

function CostDetailLine({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-4">
      <span className="text-muted-foreground">{label}</span>
      <span className="min-w-0 break-all font-mono text-right">{value}</span>
    </div>
  )
}

function formatInteger(value: number | null | undefined) {
  return compactCount(value ?? 0)
}

function formatTokenCount(value: number | null | undefined) {
  return compactTokenCount(value ?? 0)
}

function formatCost(value: number | null | undefined) {
  return `$${(value ?? 0).toFixed(6)}`
}

function costAmountText(value: number | null | undefined) {
  return typeof value === "number" && Number.isFinite(value) ? formatCost(value) : "—"
}

function costDifferenceText(value: number | null | undefined) {
  return gatewayCostDifferenceText({ cost_difference: value }, formatCost)
}

function costDifferenceRatioText(value: number | null | undefined) {
  return gatewayCostDifferenceRatioText({ cost_difference_ratio: value })
}

function formatRatio(value: number | null | undefined) {
  return Number(value ?? 0).toLocaleString(undefined, {
    maximumFractionDigits: 4,
  })
}

function priceFactIdentityLabel(log: GatewayRequestLog) {
  const values: string[] = []
  const factID = Number(log.price_fact_id ?? 0)
  const factVersion = Number(log.price_fact_version ?? 0)
  const schemaVersion = Number(log.price_fact_schema_version ?? 0)
  if (Number.isFinite(factID) && factID > 0) values.push(`#${factID}`)
  if (Number.isFinite(factVersion) && factVersion > 0) values.push(`事实 v${factVersion}`)
  if (Number.isFinite(schemaVersion) && schemaVersion > 0) values.push(`结构 v${schemaVersion}`)
  return values.join(" · ") || "已冻结"
}

function priceFactOperationLabel(log: GatewayRequestLog) {
  return joinCostMetadata([log.price_operation, log.price_protocol])
}

function priceFactMoneyLabel(log: GatewayRequestLog) {
  return joinCostMetadata([log.price_currency, log.price_unit])
}

function priceFactUsageLabel(log: GatewayRequestLog) {
  const schema = String(log.usage_schema ?? "").trim()
  const version = Number(log.usage_schema_version ?? 0)
  return joinCostMetadata([schema, Number.isFinite(version) && version > 0 ? `v${version}` : ""])
}

function joinCostMetadata(values: Array<string | null | undefined>) {
  return values.map((value) => String(value ?? "").trim()).filter(Boolean).join(" · ") || "—"
}

function formatReasoningEffort(value: string | null | undefined) {
  return officialReasoningEffortLabel(value)
}

function modelWithReasoning(model: string | null | undefined, reasoningEffort: string | null | undefined) {
  const modelName = String(model ?? "").trim() || "—"
  const effort = String(reasoningEffort ?? "").trim()
  return effort ? `${modelName} / ${formatReasoningEffort(effort)}` : modelName
}

function hasModelMappingRecord(log: GatewayRequestLog) {
  const strategy = String(log.model_mapping_strategy ?? "").trim()
  return strategy !== "" && strategy !== "off" && (Boolean(String(log.requested_model_name ?? "").trim()) || Boolean(String(log.model_mapping_details ?? "").trim()))
}

function modelMappingStrategyLabel(value: string | null | undefined) {
  const labels: Record<string, string> = {
    iq: "智商优先",
    cost: "费用优先",
    time: "耗时优先",
    balanced: "综合平衡",
    manual: "手动固定",
  }
  return labels[String(value ?? "").trim().toLowerCase()] ?? "模型映射"
}

function formatLatency(value: number | null | undefined) {
  const ms = value ?? 0
  return `${(ms / 1000).toFixed(2)} s`
}

function formatFirstByteLatency(value: number | null | undefined) {
  if ((value ?? 0) <= 0) return "—"
  return formatLatency(value)
}

function firstByteLatencyClassName(value: number | null | undefined) {
  switch (gatewayFirstByteTone(value)) {
    case "fast":
      return "font-medium text-success"
    case "moderate":
      return "font-medium text-warning"
    case "slow":
      return "font-medium text-destructive"
    default:
      return "text-muted-foreground"
  }
}

function firstByteLatencyTitle(value: number | null | undefined) {
  const prefix = "从请求开始到读取到上游首段正文的时间"
  switch (gatewayFirstByteTone(value)) {
    case "fast":
      return `${prefix}；10 秒内`
    case "moderate":
      return `${prefix}；10–30 秒`
    case "slow":
      return `${prefix}；超过 30 秒，可手动冷却本次请求命中的路由`
    default:
      return `${prefix}；暂无数据`
  }
}

function routeReasonLabel(reason: string) {
  switch (reason) {
    case "manual":
      return "手动固定"
    case "session_affinity":
      return "会话亲和"
    case "model_filtered":
      return "模型过滤"
    case "fallback":
      return "故障回退"
    case "all_failed":
      return "全部失败"
    case "client_canceled":
      return "客户端取消"
    case "dispatch_recovery":
      return "调度恢复"
    case "api_test":
    case "key_connectivity_test":
      return "API 测试"
    case "model_check":
      return "鹈鹕检测"
    default:
      return "智能首选"
  }
}

function requestRouteReasonLabel(log: GatewayRequestLog) {
  if (log.route_reason === "fallback" && !gatewayRequestHasRealFallback(log)) return "历史智能路由"
  return routeReasonLabel(log.route_reason)
}

function routeChannelLabel(log: GatewayRequestLog) {
  return log.channel_name?.trim() || (routeAttemptedChannelLabel(log) ? `失败候选 ${routeAttemptedChannelLabel(log)}` : "未命名渠道")
}

function safeChannelHomeURL(value: string | null | undefined) {
  try {
    const url = new URL(String(value ?? "").trim())
    return url.protocol === "https:" || url.protocol === "http:" ? url.href : ""
  } catch {
    return ""
  }
}

function routeAttemptedChannelLabel(log: GatewayRequestLog) {
  const attempts = Array.isArray(log.route_attempts) ? log.route_attempts : []
  return attempts.map(gatewayRouteAttemptLocation).join("、")
}

function upstreamModelChanged(log: GatewayRequestLog) {
  const upstream = String(log.upstream_model_name ?? "").trim()
  return upstream !== "" && upstream !== String(log.model_name ?? "").trim()
}

function isClientCanceledLog(log: GatewayRequestLog) {
	return log.route_reason === "client_canceled" || gatewayErrorLabel(log.error) === "客户端取消"
}

function isSuccessfulFallbackLog(log: GatewayRequestLog) {
  return gatewayRequestHasRealFallback(log) && usageRouteOutcome(log) === "selected"
}

function usageRouteOutcome(log: GatewayRequestLog) {
  return gatewayRouteOutcome({
    statusCode: log.status_code,
    routeReason: log.route_reason,
    hasError: gatewayErrorLabel(log.error) !== "",
    canceled: isClientCanceledLog(log),
  })
}

function gatewayRequestKeyLabel(log: GatewayRequestLog) {
	if (log.route_reason === "model_check") {
		const kind = log.model_check_kind
		return kind === "pelican_svg" ? "鹈鹕绘制" : kind === "candy_shapes" ? "糖果题" : "鹈鹕检测"
	}
	const name = String(log.downstream_key_name ?? "").trim() || "未命名 Key"
	const group = String(log.downstream_key_group_name ?? "").trim()
	return group ? `${group} / ${name}` : name
}

function gatewayErrorLabel(error: string | null | undefined) {
  const text = (error ?? "").trim()
  if (!text) return ""
  if (text.toLowerCase() === "context canceled" || text === "客户端取消") {
    return "客户端取消"
  }
  return text
}

function gatewayTerminalFailureLabel(value: string | null | undefined) {
  switch (String(value ?? "").trim().toLowerCase()) {
    case "response_timeout":
      return "上游首字超时"
    case "upstream_stream_truncated":
      return "上游流在完成前中断"
    case "malformed_terminal_frame":
      return "上游终止帧格式异常"
    case "malformed_json_frame":
      return "上游 JSON 帧格式异常"
    case "stream_error":
      return "上游流错误"
    default:
      return ""
  }
}
