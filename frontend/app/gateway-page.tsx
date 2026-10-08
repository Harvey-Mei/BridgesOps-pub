import { FormEvent, memo, useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react"
import { NavLink } from "react-router-dom"
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts"
import {
  Activity,
  ArrowRight,
  BarChart3,
  BellRing,
  CalendarDays,
  ChevronDown,
  ChevronsDownUp,
  ChevronsUpDown,
  CircleCheck,
  CircleDollarSign,
  Coins,
  Copy,
  Database,
  FileCode2,
  FileText,
  Gauge,
  MessageSquareText,
  Power,
  RefreshCw,
  RotateCcw,
  Search,
  SlidersHorizontal,
  Timer,
  TriangleAlert,
  type LucideIcon,
} from "lucide-react"
import { toast } from "sonner"
import { apiFetch, getToken, type ApiError } from "@/lib/api"
import { gatewayRequestLogExportURL, useChannels, useGatewayCandidateGroups, useGatewayDownstreamKeys, useGatewayKeyGroups, useGatewayKeyRoutePreviews, useGatewayProbeConfig, useGatewayRequestLogAnalytics, useGatewayRequestLogFilters, useGatewayRequestLogGroupCachePage, useGatewayRequestLogs, useSystemConfig } from "@/lib/queries"
import type {
  GatewayApp,
  GatewayDownstreamKey,
  GatewayDownstreamKeyCreateResult,
  GatewayModelFamily,
  GatewayProbeConfigChannel,
  GatewayRequestLogAnalytics,
  GatewayRequestLogBreakdownItem,
  GatewayRequestLogSummary,
  GatewayRequestLogTrendPoint,
  GatewayRouteCandidate,
  GatewayRoutingEnableResult,
  SystemConfig,
} from "@/lib/api-types"
import { beginKeyOperation, finishKeyEditing, finishKeyOperation } from "@/lib/gateway-key-operation-state"
import { buildModelMappingPatch, candidateGroupSelectionChanged, modelMappingCandidateGroupCompatible, modelMappingDraftCandidateGroupCompatible, type ModelMappingDraft } from "@/lib/model-mapping-editor"
import { compactCount, compactDateTime, compactTokenCount, confirmDisplayTimeZone, dateTimeInputValue, getDisplayTimeZone, normalizeDisplayTimeZone, relativeTime, setDisplayTimeZone } from "@/lib/format"
import { candidateTargetMode, candidateTargetPatchForEdit } from "@/lib/gateway-key-route-policy"
import { gatewayUsageTabPath, type GatewayUsageTab } from "@/lib/gateway-usage-tabs"
import {
  createGatewayGroupCachePagination,
  gatewayGroupCacheCanGoBack,
  gatewayGroupCacheCurrentCursor,
  gatewayGroupCacheGoBack,
  gatewayGroupCacheGoNext,
  gatewayGroupCachePageNumber,
  resetGatewayGroupCachePagination,
  type GatewayGroupCachePaginationState,
} from "@/lib/gateway-group-cache-pagination"
import { gatewayRoutingEnableBlockedReason, gatewayRoutingEnableEligible, gatewayRoutingEnableIdempotencyKey, gatewayRoutingEnableSuccessMessage, type GatewayRoutingEnableCandidate } from "@/lib/gateway-routing-enable"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Switch } from "@/components/ui/switch"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { useConfirm } from "@/components/ui/confirm-dialog"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { GatewayKeysView } from "@/components/gateway/gateway-keys-view"
import {
  ClientImportDialog,
  type ClientImportRequest,
} from "@/components/gateway/client-import-dialog"
import {
  base64Encode,
  base64UrlEncode,
  buildCCSwitchUsageScript,
  buildNativeClientConfigFiles as buildClientConfigFiles,
  ccswitchUsageBaseURL,
  clientImportCandidateRoutable,
  clientImportModelForApp,
} from "@/components/gateway/client-import-data"
import { GatewayUsageView } from "@/components/gateway/gateway-usage-view"
import { ImageOperationCapabilitiesDialog } from "@/components/gateway/image-operation-capabilities-dialog"
import { ModelPricesPanel } from "@/components/gateway/model-prices-panel"
import { NumberDraftInput } from "@/components/number-draft-input"
import { StatusMetricCard } from "@/components/ui/status-metric-card"
import { PageHeader } from "@/components/ui/page-header"
import {
  candidateAvailabilityLabel,
  candidateAvailabilityReasons,
  manualMappedTargetSelectionState,
} from "@/lib/route-candidate-availability"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"

export type GatewayView = "keys" | "usage" | "costs" | "config"

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

type CostQuickRange = "today" | "yesterday" | "24h" | "7d" | "14d" | "30d" | "month" | "lastMonth"

const costQuickRangeOptions: Array<{ value: CostQuickRange; label: string }> = [
  { value: "today", label: "今天" },
  { value: "yesterday", label: "昨天" },
  { value: "24h", label: "近 24 小时" },
  { value: "7d", label: "近 7 天" },
  { value: "14d", label: "近 14 天" },
  { value: "30d", label: "近 30 天" },
  { value: "month", label: "本月" },
  { value: "lastMonth", label: "上月" },
]

type UsageSortKey = "created_at" | "model_name" | "total_tokens" | "cost" | "status_code" | "latency_ms"
type UsageSortOrder = "asc" | "desc"

type GatewayKeyTestProtocol = "chat" | "responses" | "messages"
const gatewayKeyTestEndpoints: Record<GatewayKeyTestProtocol, string> = {
  chat: "/v1/chat/completions",
  responses: "/v1/responses",
  messages: "/v1/messages",
}
const gatewayKeyTestModelStoragePrefix = "bridgeops.gateway.key-test-model.v1."

type GatewayKeyTestResult = {
  keyName: string
  method: string
  url: string
  model: string
  prompt: string
  requestBody: string
  responseBody: string
  status: number | null
  durationMs: number | null
  success: boolean | null
}

type ProbeErrorDetail = {
  title: string
  message: string
}

const emptyUsageFilterForm: UsageFilterForm = {
  start: "",
  end: "",
  upstreamRequestID: "",
  downstreamKeyID: "__all__",
  modelName: "__all__",
  channelID: "__all__",
  groupName: "__all__",
  result: "__all__",
  routeReason: "__all__",
}

function recentUsageFilterForm(hours = 24, timeZone: string = getDisplayTimeZone()): UsageFilterForm {
  const end = new Date()
  end.setSeconds(0, 0)
  end.setMinutes(end.getMinutes() + 1)
  const start = new Date(end.getTime() - hours * 60 * 60 * 1000)
  return {
    ...emptyUsageFilterForm,
    start: dateTimeInputValue(start, timeZone),
    end: dateTimeInputValue(end, timeZone),
  }
}

function costQuickRangeDates(range: CostQuickRange, timeZone: string): Pick<UsageFilterForm, "start" | "end"> {
  const { end } = recentUsageFilterForm(0, timeZone)
  const currentDay = dateTimeInputValue(new Date(), timeZone).slice(0, 10)
  const currentMonthStart = `${currentDay.slice(0, 8)}01`
  switch (range) {
    case "today":
      return { start: `${currentDay}T00:00`, end }
    case "yesterday": {
      const yesterday = shiftCalendarDate(currentDay, -1)
      return { start: `${yesterday}T00:00`, end: `${currentDay}T00:00` }
    }
    case "7d":
      return { start: `${shiftCalendarDate(currentDay, -6)}T00:00`, end }
    case "14d":
      return { start: `${shiftCalendarDate(currentDay, -13)}T00:00`, end }
    case "30d":
      return { start: `${shiftCalendarDate(currentDay, -29)}T00:00`, end }
    case "month":
      return { start: `${currentMonthStart}T00:00`, end }
    case "lastMonth": {
      const previousMonthEnd = shiftCalendarDate(currentMonthStart, -1)
      return { start: `${previousMonthEnd.slice(0, 8)}01T00:00`, end: `${currentMonthStart}T00:00` }
    }
    case "24h":
      break
  }
  const dates = recentUsageFilterForm(24, timeZone)
  return { start: dates.start, end: dates.end }
}

function shiftCalendarDate(value: string, days: number) {
  const date = new Date(`${value}T12:00:00Z`)
  date.setUTCDate(date.getUTCDate() + days)
  return date.toISOString().slice(0, 10)
}

function costRangeLabel(range: CostQuickRange | null, start: string, end: string) {
  const preset = costQuickRangeOptions.find((option) => option.value === range)
  if (preset) return preset.label
  if (!start || !end) return "自定义范围"
  const compact = (value: string) => value.replace("T", " ").slice(5, 16)
  return `${compact(start)} 至 ${compact(end)}`
}

type GatewayHeroStat = {
  label: string
  value: ReactNode
  hint?: ReactNode
  tone: "brand" | "success" | "warning" | "muted"
  icon: LucideIcon
}

export default function GatewayPage({ view, usageTab = "requests" }: { view: GatewayView; usageTab?: GatewayUsageTab }) {
  const keyView = view === "keys"
  const configView = view === "config"
  const usageView = view === "usage"
  const costView = view === "costs"
  const usageDataView = usageView || costView
  const [configSection, setConfigSection] = useState("routing-scoring")
  const [usagePage, setUsagePage] = useState(1)
  const [usagePageSize, setUsagePageSize] = useState(20)
  const [groupCachePagination, setGroupCachePagination] = useState(() => createGatewayGroupCachePagination())
  const [groupCacheNavigationLocked, setGroupCacheNavigationLocked] = useState(false)
  const groupCacheNavigationLockedRef = useRef(false)
  const [usageDraftFilters, setUsageDraftFilters] = useState<UsageFilterForm>(() => recentUsageFilterForm())
  const [usageFilters, setUsageFilters] = useState<UsageFilterForm>(() => recentUsageFilterForm())
  const [usageLiveRangeHours, setUsageLiveRangeHours] = useState<number | null>(24)
  const [costQuickRange, setCostQuickRange] = useState<CostQuickRange | null>("24h")
  const [usageSortBy, setUsageSortBy] = useState<UsageSortKey>("created_at")
  const [usageSortOrder, setUsageSortOrder] = useState<UsageSortOrder>("desc")
  const [usageFiltersTimeZone, setUsageFiltersTimeZone] = useState<string | null>(null)
  const [usageRuntimeTimeZone, setUsageRuntimeTimeZone] = useState(getDisplayTimeZone)
  const keys = useGatewayDownstreamKeys(keyView || usageDataView)
  const candidateGroups = useGatewayCandidateGroups(keyView)
  const keyGroups = useGatewayKeyGroups(keyView)
  const channels = useChannels(usageDataView)
  const gatewayConfig = useSystemConfig(configView || usageDataView)
  const usageSystemTimeZone = gatewayConfig.data?.effective_time_zone
  const usageDisplayTimeZone = normalizeDisplayTimeZone(usageRuntimeTimeZone)
  const usageTimeZoneReady = Boolean(usageSystemTimeZone?.trim()) && usageFiltersTimeZone === usageDisplayTimeZone
  const usageQueriesEnabled = usageDataView && usageTimeZoneReady
  const usageFilterOptions = useGatewayRequestLogFilters(usageQueriesEnabled, usageFilters, usageDisplayTimeZone)
  const routePreviewEnabled = keyView
  const keyRoutePreviews = useGatewayKeyRoutePreviews(keys.data ?? [], routePreviewEnabled)
  const probeConfig = useGatewayProbeConfig(configView || keyView)
  const usageRequestLogs = useGatewayRequestLogs(usagePage, usagePageSize, usageView && usageTab === "requests" && usageTimeZoneReady, {
    ...usageFilters,
    sortBy: usageSortBy,
    sortOrder: usageSortOrder,
  }, usageDisplayTimeZone)
  const costSummaryLogs = useGatewayRequestLogs(1, 1, costView && usageTimeZoneReady, {
    ...usageFilters,
    sortBy: "created_at",
    sortOrder: "desc",
  }, usageDisplayTimeZone)
  const requestLogs = costView ? costSummaryLogs : usageRequestLogs
  const requestAnalytics = useGatewayRequestLogAnalytics(usageQueriesEnabled && (costView || usageTab === "requests"), usageFilters, usageDisplayTimeZone)
  const groupCachePage = useGatewayRequestLogGroupCachePage(
    usageQueriesEnabled && usageView && usageTab === "group-cache",
    gatewayGroupCacheCurrentCursor(groupCachePagination),
    groupCachePagination.pageSize,
    usageFilters,
    usageDisplayTimeZone,
  )
  const [name, setName] = useState("default")
  const [app, setApp] = useState<GatewayApp>("codex")
  const [modelFamily, setModelFamily] = useState<GatewayModelFamily>("codex")
  const [candidateGroupID, setCandidateGroupID] = useState("__all__")
  const [keyGroupID, setKeyGroupID] = useState("__none__")
  const [showCreate, setShowCreate] = useState(false)
  const [creating, setCreating] = useState(false)
  const [created, setCreated] = useState<GatewayDownstreamKeyCreateResult | null>(null)
  const [editingID, setEditingID] = useState<number | null>(null)
  const [editingName, setEditingName] = useState("")
  const [savingKeyIDs, setSavingKeyIDs] = useState<Set<number>>(() => new Set())
  const savingKeyIDsRef = useRef<Set<number>>(new Set())
  const [routeExclusionSaving, setRouteExclusionSaving] = useState<string | null>(null)
  const routeExclusionSavingRef = useRef<string | null>(null)
  const [testingID, setTestingID] = useState<number | null>(null)
  const [testDraft, setTestDraft] = useState<{ key: GatewayDownstreamKey; model: string; models: string[]; protocol: GatewayKeyTestProtocol } | null>(null)
  const [testModelPickerOpen, setTestModelPickerOpen] = useState(false)
  const [testModelSearch, setTestModelSearch] = useState("")
  const testInFlightRef = useRef(false)
  const testAbortRef = useRef<AbortController | null>(null)
  const [keyTestResult, setKeyTestResult] = useState<GatewayKeyTestResult | null>(null)
  useEffect(() => () => testAbortRef.current?.abort(), [])
  const [probeErrorDetail, setProbeErrorDetail] = useState<ProbeErrorDetail | null>(null)
  const [syncingModels, setSyncingModels] = useState(false)
  const [configForm, setConfigForm] = useState<SystemConfig | null>(null)
  const [savingConfig, setSavingConfig] = useState(false)
  const [applyingConfig, setApplyingConfig] = useState(false)
  const [gatewayConfigPendingApply, setGatewayConfigPendingApply] = useState(false)
  const [exportingUsage, setExportingUsage] = useState(false)
  const [syncingChannelID, setSyncingChannelID] = useState<number | null>(null)
  const [rebuildingChannelID, setRebuildingChannelID] = useState<number | null>(null)
  const [routingEnableSavingKeys, setRoutingEnableSavingKeys] = useState<Set<string>>(() => new Set())
  const [probeSearch, setProbeSearch] = useState("")
  const [probeIssuesOnly, setProbeIssuesOnly] = useState(false)
  const [expandedProbeChannels, setExpandedProbeChannels] = useState<Set<number>>(() => new Set())
  const [dirtyGatewayNumberFields, setDirtyGatewayNumberFields] = useState<Set<string>>(() => new Set())
  const [visibleKeyIDs, setVisibleKeyIDs] = useState<Record<string, boolean>>({})
  const [clientImportRequest, setClientImportRequest] = useState<ClientImportRequest | null>(null)
  const gatewayConfigSnapshot = useRef<string | null>(null)
  const configFormRef = useRef<SystemConfig | null>(null)
  const acceptNextGatewayConfig = useRef(false)
  const autoSyncedModelsRef = useRef(false)
  const routingEnableIdempotencyKeys = useRef<Map<string, string>>(new Map())
  const { confirm, dialog } = useConfirm()
  const baseURL = useMemo(() => {
    if (typeof window === "undefined") return ""
    return `${window.location.origin}/v1`
  }, [])
  const originURL = useMemo(() => {
    if (typeof window === "undefined") return ""
    return window.location.origin
  }, [])
  const usageTimeZoneBlocked = usageDataView && !usageTimeZoneReady
  const usageTimeZonePending = usageDataView && (gatewayConfig.loading || (!gatewayConfig.error && usageTimeZoneBlocked))
  const usageDataVisible = !usageTimeZoneBlocked
  const usageSummary = usageDataVisible ? requestLogs.data?.summary : null
  const usageItems = usageDataVisible && Array.isArray(requestLogs.data?.items) ? requestLogs.data.items : []
  const usageTotal = usageDataVisible ? requestLogs.data?.total ?? 0 : 0
  const usageTotalPages = Math.max(1, usageDataVisible ? requestLogs.data?.pages ?? 1 : 1)
  const usageResponsePage = usageDataVisible ? requestLogs.data?.page ?? usagePage : usagePage
  const usageResponsePageSize = usageDataVisible ? requestLogs.data?.page_size ?? usagePageSize : usagePageSize
  const usageCurrentPage = Math.min(usageResponsePage, usageTotalPages)
  const usageItemCount = usageItems.length
  const usageRangeStart = usageTotal === 0 || usageItemCount === 0 ? 0 : (usageCurrentPage - 1) * usageResponsePageSize + 1
  const usageRangeEnd = usageRangeStart === 0 ? 0 : Math.min(usageRangeStart + usageItemCount - 1, usageTotal)
  const usagePagerNumbers = pageNumbers(usageCurrentPage, usageTotalPages)
  const usageParamsPending = usageView && usageTab === "requests"
    && requestLogs.data !== null
    && requestLogs.error === null
    && (usageResponsePage !== usagePage || usageResponsePageSize !== usagePageSize)
  const groupCacheRequestResolved = Boolean(groupCachePage.requestPath) && groupCachePage.resolvedPath === groupCachePage.requestPath
  const groupCacheRequestAwaiting = !groupCachePage.error && !groupCacheRequestResolved
  const groupCachePending = usageTimeZonePending || groupCacheRequestAwaiting || groupCachePage.loading || groupCachePage.refreshing || groupCacheNavigationLocked
  const usagePending = usageView && usageTab === "group-cache"
    ? groupCachePending
    : usageTimeZonePending || requestLogs.loading || usageParamsPending
  const usageActiveFilterCount = [
    usageFilters.start,
    usageFilters.end,
    usageFilters.upstreamRequestID,
    usageFilters.downstreamKeyID !== "__all__",
    usageFilters.modelName !== "__all__",
    usageFilters.channelID !== "__all__",
    usageFilters.groupName !== "__all__",
    usageFilters.result !== "__all__",
    usageFilters.routeReason !== "__all__",
  ].filter(Boolean).length
  const configCandidates = useMemo(() => (
    probeConfigCandidates(probeConfig.data ?? []).sort((a, b) => (
      (a.gateway_app ?? "").localeCompare(b.gateway_app ?? "")
      || routeCandidateGroupName(a).localeCompare(routeCandidateGroupName(b))
      || routeCandidateEffectivePrice(a) - routeCandidateEffectivePrice(b)
      || routeCandidateChannelName(a).localeCompare(routeCandidateChannelName(b))
    ))
  ), [probeConfig.data])
  const probeChannelGroups = useMemo(() => groupProbeCandidates(configCandidates, probeConfig.data ?? []), [configCandidates, probeConfig.data])
  const probeMinimumSamples = configForm?.scheduler.latencyProbeSampleSize ?? 10
  const filteredProbeChannelGroups = useMemo(() => filterProbeChannelGroups(
    probeChannelGroups,
    probeSearch,
    probeIssuesOnly,
    probeMinimumSamples,
  ), [probeChannelGroups, probeSearch, probeIssuesOnly, probeMinimumSamples])
  const probeIssueChannelCount = useMemo(() => probeChannelGroups.filter((group) => (
    probeGroupIssueCount(group, probeMinimumSamples) > 0
  )).length, [probeChannelGroups, probeMinimumSamples])
  const gatewayConfigDirty = Boolean(
    dirtyGatewayNumberFields.size > 0
    || (
      configForm
      && gatewayConfigSnapshot.current
      && JSON.stringify(configForm) !== gatewayConfigSnapshot.current
    ),
  )
  configFormRef.current = configForm
  const gatewayHeroStats = view === "usage" && usageTab === "requests"
    ? gatewayHeroStatsForView({ usagePending, usageTotal, usageSummary, usageActiveFilterCount })
    : []

  const syncModelFamilies = useCallback(async () => {
    setSyncingModels(true)
    try {
      const result = await apiFetch<{ items: Array<{ error?: string }>; total: number }>("/gateway/model-families/sync", {
        method: "POST",
      })
      keyRoutePreviews.refetch()
      probeConfig.refetch()
      const failed = result.items.filter((item) => item.error).length
      toast.success(failed > 0 ? `模型能力已同步，${failed} 个分组失败` : `模型能力已同步，共 ${result.total} 个分组`)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "同步模型能力失败")
    } finally {
      setSyncingModels(false)
    }
  }, [keyRoutePreviews, probeConfig])

  useEffect(() => {
    if (usageView && usagePage > usageTotalPages) setUsagePage(usageTotalPages)
  }, [usagePage, usageTotalPages, usageView])

  useEffect(() => {
    if (!groupCacheNavigationLockedRef.current) return
    if (groupCachePage.loading || groupCachePage.refreshing) return
    if (!groupCachePage.error && groupCachePage.resolvedPath !== groupCachePage.requestPath) return
    groupCacheNavigationLockedRef.current = false
    setGroupCacheNavigationLocked(false)
  }, [groupCachePage.error, groupCachePage.loading, groupCachePage.refreshing, groupCachePage.requestPath, groupCachePage.resolvedPath])

  const resetGroupCachePaginationForScope = useCallback(() => {
    groupCacheNavigationLockedRef.current = false
    setGroupCacheNavigationLocked(false)
    setGroupCachePagination((current) => resetGatewayGroupCachePagination(current))
  }, [])

  useEffect(() => {
    if (!usageDataView || !usageSystemTimeZone?.trim()) return
    const runtimeTimeZone = setDisplayTimeZone(usageSystemTimeZone)
    setUsageRuntimeTimeZone((current) => current === runtimeTimeZone ? current : runtimeTimeZone)
  }, [usageDataView, usageSystemTimeZone])

  useEffect(() => {
    if (!usageDataView || !usageSystemTimeZone?.trim()) return
    if (usageFiltersTimeZone === usageDisplayTimeZone) return
    const range = costView
      ? costQuickRange == null ? null : costQuickRangeDates(costQuickRange, usageDisplayTimeZone)
      : usageLiveRangeHours == null ? null : recentUsageFilterForm(usageLiveRangeHours, usageDisplayTimeZone)
    if (range != null) {
      setUsageDraftFilters((current) => ({ ...current, start: range.start, end: range.end }))
      setUsageFilters((current) => ({ ...current, start: range.start, end: range.end }))
      setUsagePage(1)
      resetGroupCachePaginationForScope()
    }
    setUsageFiltersTimeZone(usageDisplayTimeZone)
  }, [costQuickRange, costView, resetGroupCachePaginationForScope, usageDataView, usageDisplayTimeZone, usageFiltersTimeZone, usageLiveRangeHours, usageSystemTimeZone])

  useEffect(() => {
    const next = gatewayConfig.data?.config
    if (!next) return
    const snapshot = JSON.stringify(next)
    if (acceptNextGatewayConfig.current) {
      acceptNextGatewayConfig.current = false
      gatewayConfigSnapshot.current = snapshot
      setConfigForm(next)
      setDirtyGatewayNumberFields(new Set())
      return
    }
    setConfigForm((current) => {
      const dirty = Boolean(
        dirtyGatewayNumberFields.size > 0
        || (
          current
          && gatewayConfigSnapshot.current
          && JSON.stringify(current) !== gatewayConfigSnapshot.current
        ),
      )
      if (dirty || (current && JSON.stringify(current) === snapshot)) return current
      return next
    })
    gatewayConfigSnapshot.current = snapshot
  }, [gatewayConfig.data, dirtyGatewayNumberFields])

  useEffect(() => {
    if (!configView || autoSyncedModelsRef.current || syncingModels) return
    if (!probeConfig.data) return
    const hasMissingModels = configCandidates.some((candidate) => routeCandidateModelIDs(candidate).length === 0)
    if (!hasMissingModels) return
    autoSyncedModelsRef.current = true
    void syncModelFamilies()
  }, [configView, configCandidates, probeConfig.data, syncingModels, syncModelFamilies])

  useEffect(() => {
    if (!configView || (probeSearch.trim() === "" && !probeIssuesOnly)) return
    const visibleIDs = filteredProbeChannelGroups.map((group) => group.channelID)
    if (visibleIDs.length === 0) return
    setExpandedProbeChannels((current) => {
      const next = new Set(current)
      let changed = false
      for (const channelID of visibleIDs) {
        if (next.has(channelID)) continue
        next.add(channelID)
        changed = true
      }
      return changed ? next : current
    })
  }, [configView, probeSearch, probeIssuesOnly, filteredProbeChannelGroups])

  function goToUsagePage(page: number) {
    const nextPage = Math.min(usageTotalPages, Math.max(1, page))
    if (nextPage === usagePage) {
      requestLogs.refetch()
      return
    }
    setUsagePage(nextPage)
  }

  function beginGroupCacheNavigation(next: GatewayGroupCachePaginationState) {
    if (groupCacheNavigationLockedRef.current || next === groupCachePagination) return
    groupCacheNavigationLockedRef.current = true
    setGroupCacheNavigationLocked(true)
    setGroupCachePagination(next)
  }

  function goToNextGroupCachePage() {
    beginGroupCacheNavigation(gatewayGroupCacheGoNext(groupCachePagination, groupCachePage.data?.next_cursor))
  }

  function goToPreviousGroupCachePage() {
    beginGroupCacheNavigation(gatewayGroupCacheGoBack(groupCachePagination))
  }

  function updateGroupCachePageSize(pageSize: number) {
    beginGroupCacheNavigation(resetGatewayGroupCachePagination(groupCachePagination, pageSize))
  }

  function applyUsageFilters() {
    const next = { ...usageDraftFilters }
    setUsageDraftFilters(next)
    setUsageFilters(next)
    setUsagePage(1)
    resetGroupCachePaginationForScope()
  }

  function resetUsageFilters() {
    const next = recentUsageFilterForm(24, usageDisplayTimeZone)
    setUsageLiveRangeHours(24)
    setUsageDraftFilters(next)
    setUsageFilters(next)
    setUsagePage(1)
    resetGroupCachePaginationForScope()
  }

  function resetCostFilters() {
    const range = costQuickRangeDates("24h", usageDisplayTimeZone)
    const next = { ...emptyUsageFilterForm, ...range }
    setCostQuickRange("24h")
    setUsageDraftFilters(next)
    setUsageFilters(next)
    setUsagePage(1)
    resetGroupCachePaginationForScope()
  }

  function updateUsageFilter<K extends keyof UsageFilterForm>(key: K, value: UsageFilterForm[K]) {
    if (key === "start" || key === "end") {
      setUsageLiveRangeHours(null)
      setCostQuickRange(null)
    }
    setUsageDraftFilters((current) => ({ ...current, [key]: value }))
  }

  function applyUsageQuickRange(hours: number) {
    const range = recentUsageFilterForm(hours, usageDisplayTimeZone)
    const next = {
      ...usageDraftFilters,
      start: range.start,
      end: range.end,
    }
    setUsageLiveRangeHours(hours)
    setUsageDraftFilters(next)
    setUsageFilters(next)
    setUsagePage(1)
    resetGroupCachePaginationForScope()
  }

  function applyCostRange(range: CostQuickRange | null, start: string, end: string) {
    const next = {
      ...usageDraftFilters,
      start,
      end,
    }
    setCostQuickRange(range)
    setUsageDraftFilters(next)
    setUsageFilters(next)
    setUsagePage(1)
    resetGroupCachePaginationForScope()
  }

  function refreshUsageData() {
    if (usageLiveRangeHours != null) {
      const range = recentUsageFilterForm(usageLiveRangeHours, usageDisplayTimeZone)
      const rangeChanged = range.start !== usageFilters.start || range.end !== usageFilters.end
      setUsageDraftFilters((current) => ({ ...current, start: range.start, end: range.end }))
      setUsageFilters((current) => ({ ...current, start: range.start, end: range.end }))
      setUsagePage(1)
      if (rangeChanged) {
        resetGroupCachePaginationForScope()
        return
      }
    }
    requestLogs.refetch()
    requestAnalytics.refetch()
    groupCachePage.refetch()
  }

  function updateUsageSort(key: UsageSortKey) {
    if (usageSortBy === key) {
      setUsageSortOrder((current) => current === "desc" ? "asc" : "desc")
    } else {
      setUsageSortBy(key)
      setUsageSortOrder("desc")
    }
    setUsagePage(1)
  }

  async function exportUsageLogs() {
    setExportingUsage(true)
    try {
      const exportPath = gatewayRequestLogExportURL({
        ...usageFilters,
        sortBy: usageSortBy,
        sortOrder: usageSortOrder,
      }, usageDisplayTimeZone)
      const requestURL = exportPath.startsWith("/api") ? exportPath : `/api${exportPath}`
      const headers = new Headers()
      const token = getToken()
      if (token) headers.set("Authorization", `Bearer ${token}`)
      const resp = await fetch(requestURL, { headers })
      const blob = await resp.blob()
      const disposition = resp.headers.get("content-disposition") ?? ""
      if (!resp.ok) {
        const text = await blob.text()
        throw new Error(text || `HTTP ${resp.status}`)
      }
      const filename = contentDispositionFilename(disposition) || `gateway-request-logs-${new Date().toISOString().slice(0, 10)}.csv`
      const blobURL = URL.createObjectURL(blob)
      const link = document.createElement("a")
      link.href = blobURL
      link.download = filename
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(blobURL)
      toast.success("使用记录已导出")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "导出使用记录失败")
    } finally {
      setExportingUsage(false)
    }
  }

  async function handleCreate(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    const trimmed = name.trim()
    if (!trimmed) {
      toast.error("请输入 key 名称")
      return
    }
    setCreating(true)
    setCreated(null)
    try {
      const result = await apiFetch<GatewayDownstreamKeyCreateResult>("/gateway/downstream-keys", {
        method: "POST",
        body: JSON.stringify({
          name: trimmed,
          app,
          model_family: modelFamily,
          candidate_group_id: candidateGroupID.startsWith("__") ? 0 : Number(candidateGroupID),
          candidate_target_mode: candidateTargetMode(candidateGroupID),
          key_group_id: keyGroupID === "__none__" ? 0 : Number(keyGroupID),
        }),
      })
      setCreated(result)
      setName("")
      setCandidateGroupID("__all__")
      setKeyGroupID("__none__")
      keys.refetch()
      toast.success("下游 key 已创建")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "创建失败")
    } finally {
      setCreating(false)
    }
  }

  function handleCreateAppChange(nextApp: GatewayApp) {
    setApp(nextApp)
    setModelFamily(nextApp === "claude" ? "claude" : nextApp === "gemini" ? "gemini" : "codex")
  }

  async function copy(value: string, label = "内容") {
    if (!value) return
    try {
      const writeClipboard = navigator.clipboard?.writeText?.bind(navigator.clipboard)
      if (writeClipboard) {
        await writeClipboard(value)
      } else {
        fallbackCopy(value)
      }
      toast.success(`${label}已复制`)
    } catch {
      try {
        fallbackCopy(value)
        toast.success(`${label}已复制`)
      } catch {
        toast.error("复制失败")
      }
    }
  }

  function toggleKeyVisible(id: string) {
    setVisibleKeyIDs((prev) => ({ ...prev, [id]: !prev[id] }))
  }

  function openClientImport(apiKey: string, providerName: string, app: GatewayApp, contextLabel?: string, modelName?: string) {
    if (!apiKey) {
      toast.error("当前 Key 没有可导出的明文")
      return
    }
    setClientImportRequest({
      apiKey,
      providerName,
      contextLabel: contextLabel?.trim() || providerName,
      defaultApp: app,
      modelName: clientImportModelForApp(modelName, app),
    })
  }

  function launchCCSwitch(apiKey: string, keyName: string, app: GatewayApp, modelName?: string) {
    if (typeof window === "undefined" || !apiKey || !baseURL) return
    const isClaude = app === "claude"
    const providerName = keyName.trim() || gatewayAppShortLabel(app)
    const model = clientImportModelForApp(modelName, app)
    const clientConfig = buildCCSwitchConfig(apiKey, app, originURL, model)
    const usageScript = buildCCSwitchUsageScript()
    const encodedUsageScript = base64UrlEncode(usageScript)
    const params = new URLSearchParams({
      resource: "provider",
      app,
      name: providerName,
      endpoint: isClaude ? originURL : baseURL,
      apiKey,
      homepage: window.location.origin,
      icon: gatewayAppIcon(app),
      notes: "BridgeOps API gateway",
    })
    const usageBaseURL = ccswitchUsageBaseURL(originURL)
    if (usageBaseURL) {
      params.set("usageEnabled", "true")
      params.set("usageBaseUrl", usageBaseURL)
      params.set("usageScript", encodedUsageScript)
      params.set("usageAutoInterval", "30")
    } else {
      toast.info("当前页面不是 HTTPS，CCSwitch 不会启用自动用量查询")
    }
    if (model) {
      params.set("model", model)
      if (app === "claude") {
        params.set("haikuModel", model)
        params.set("sonnetModel", model)
        params.set("opusModel", model)
      }
    }
    if (clientConfig) {
      params.set("config", base64Encode(clientConfig.content))
      params.set("configFormat", clientConfig.format)
    }
    window.location.assign(`ccswitch://v1/import?${params.toString()}`)
    toast.success("正在打开 CC Switch")
  }

  function renderClientConfigPopover(apiKey: string, app: GatewayApp, modelName?: string, disabled = false, buttonClassName?: string) {
    const currentKey = keys.data?.find(key => key.api_key === apiKey)
    return <Button type="button" size="sm" variant="outline" disabled={disabled || !apiKey} className={buttonClassName} onClick={() => openClientImport(apiKey, currentKey?.name || "BridgeOps", app, currentKey?.name, modelName)}><FileCode2 className="size-3.5" />客户端配置</Button>
  }

  function openKeyTest(key: GatewayDownstreamKey) {
    const models = Array.from(new Set((keyRoutePreviews.previews[key.id]?.data ?? []).filter(candidate => !candidate.disabled_for_key && !candidate.cooling_down).flatMap(candidate => candidate.model_ids ?? []).filter(Boolean))).sort()
    setKeyTestResult(null)
    setTestModelPickerOpen(false)
    setTestModelSearch("")
    setTestDraft({ key, model: restoreGatewayTestModel(key, models), models, protocol: key.app === "claude" ? "messages" : "chat" })
  }

  function selectKeyTestModel(model: string) {
    if (!testDraft) return
    setTestDraft({ ...testDraft, model })
    setTestModelPickerOpen(false)
    try {
      window.localStorage.setItem(`${gatewayKeyTestModelStoragePrefix}${testDraft.key.id}`, JSON.stringify({
        model,
        custom: !testDraft.models.includes(model),
      }))
    } catch {
      // Restricted browser storage must not prevent manual testing.
    }
  }

  async function testGatewayKey(key: GatewayDownstreamKey, model: string, protocol: GatewayKeyTestProtocol) {
    if (testInFlightRef.current || !model.trim()) return
    if (!key.api_key) {
      toast.error("旧 key 没有保存明文，无法直接测试")
      return
    }
    testInFlightRef.current = true
    const controller = new AbortController()
    testAbortRef.current = controller
    let timedOut = false
    let requestSent = false
    const timeout = window.setTimeout(() => {
      timedOut = true
      controller.abort()
    }, 60_000)
    setTestingID(key.id)
    const prompt = "请只回复：BridgeOps Key 测试成功"
    const isClaude = protocol === "messages"
    const url = `${originURL}${gatewayKeyTestEndpoints[protocol]}`
    const request = protocol === "responses"
      ? { model, max_output_tokens: 32, input: prompt }
      : { model, max_tokens: 32, messages: [{ role: "user", content: prompt }] }
    const initialResult: GatewayKeyTestResult = {
      keyName: key.name,
      method: "POST",
      url,
      model,
      prompt,
      requestBody: JSON.stringify(request, null, 2),
      responseBody: "等待响应...",
      status: null,
      durationMs: null,
      success: null,
    }
    setKeyTestResult(initialResult)
    const startedAt = performance.now()
    try {
      const { token: testToken } = await apiFetch<{ token: string }>(`/gateway/downstream-keys/${key.id}/test-token`, {
        method: "POST",
        signal: controller.signal,
      })
      controller.signal.throwIfAborted()
      const headers: Record<string, string> = isClaude
        ? { "Content-Type": "application/json", "anthropic-version": "2023-06-01", "x-api-key": key.api_key }
        : { "Content-Type": "application/json", Authorization: `Bearer ${key.api_key}` }
      headers["X-BridgeOps-Key-Test"] = testToken
      requestSent = true
      const resp = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(request),
        signal: controller.signal,
      })
      const text = await resp.text()
      let formatted = text
      try {
        formatted = JSON.stringify(JSON.parse(text), null, 2)
      } catch {
        // Keep non-JSON upstream responses intact for diagnosis.
      }
      setKeyTestResult({ ...initialResult, responseBody: formatted || "(空响应)", status: resp.status, durationMs: Math.round(performance.now() - startedAt), success: resp.ok })
      if (resp.ok) toast.success(`${gatewayAppShortLabel(key.app)} Key 连通正常`)
      else toast.error(`Key 测试失败：HTTP ${resp.status}`)
    } catch (err) {
      const interrupted = controller.signal.aborted
      const message = interrupted
        ? `${timedOut ? "等待超过 60 秒" : "已停止等待"}。${requestSent ? "服务端可能已收到请求，结果尚未确认。" : "未发送测试请求。"}`
        : err instanceof Error ? err.message : "测试失败"
      setKeyTestResult({ ...initialResult, responseBody: message, durationMs: Math.round(performance.now() - startedAt), success: interrupted ? null : false })
      if (!interrupted) toast.error(message)
    } finally {
      window.clearTimeout(timeout)
      if (testAbortRef.current === controller) testAbortRef.current = null
      testInFlightRef.current = false
      setTestingID(null)
    }
  }

  async function syncChannelModelFamilies(channelID: number) {
    setSyncingChannelID(channelID)
    try {
      const result = await apiFetch<{ items: Array<{ error?: string }>; total: number }>(`/gateway/model-families/sync?channel_id=${channelID}`, {
        method: "POST",
      })
      refetchGatewayCandidateData()
      const failed = result.items.filter((item) => item.error).length
      toast.success(failed > 0 ? `已刷新模型列表，${failed} 个分组失败` : `已刷新模型列表，共 ${result.total} 个分组`)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "刷新模型列表失败")
    } finally {
      setSyncingChannelID(null)
    }
  }

  async function rebuildManagedUpstreamKeys(channelID: number, channelName: string) {
    const approved = await confirm({
      title: `重建 ${channelName} 的受管 Key？`,
      description: "将删除该渠道上所有由 BridgeOps 创建的受管 Key，并按当前上游分组重新创建。手工 Key、候选分组和历史请求不会删除。",
      confirmLabel: "删除并重建",
      destructive: true,
    })
    if (!approved) return
    setRebuildingChannelID(channelID)
    try {
      const result = await apiFetch<{ matched: number; deleted: number; recreated: number; items: Array<{ error?: string }> }>(`/gateway/upstream-keys/${channelID}/rebuild-managed`, {
        method: "POST",
        body: JSON.stringify({ confirm: "DELETE_AND_REBUILD_MANAGED_KEYS" }),
      })
      refetchGatewayCandidateData()
      const failed = result.items.filter((item) => item.error).length
      if (failed > 0) toast.warning(
        `已删除 ${result.deleted} 个受管 Key并重建 ${result.recreated} 个分组，${failed} 个分组仍失败`,
      )
      else toast.success(
        `已删除 ${result.deleted} 个受管 Key并重建 ${result.recreated} 个分组`,
      )
    } catch (err) {
      const apiError = err as ApiError
      const partial = (apiError.body as { data?: { matched?: number; deleted?: number } } | undefined)?.data
      toast.error(partial
        ? `重建未完成：已删除 ${partial.deleted ?? 0}/${partial.matched ?? 0} 个受管 Key。${apiError.message}`
        : err instanceof Error ? err.message : "重建受管 Key 失败")
    } finally {
      setRebuildingChannelID(null)
    }
  }

  async function enableRoutingGroup(candidate: GatewayRoutingEnableCandidate) {
    if (!gatewayRoutingEnableEligible(candidate)) {
      toast.error(`${gatewayRoutingEnableBlockedReason(candidate)}，不能解除路由冷却`)
      return
    }
    const upstreamGroupID = Number(candidate.upstream_group_id)
    const presenceGeneration = Number(candidate.presence_generation)
    const channelIdentityRevision = Number(candidate.channel_identity_revision)
    const groupLabel = String(candidate.group_name ?? "").trim() || `分组 #${upstreamGroupID}`
    const savingKey = `${upstreamGroupID}:${channelIdentityRevision}:${presenceGeneration}`
    if (routingEnableSavingKeys.has(savingKey)) return
    const idempotencyKey = routingEnableIdempotencyKeys.current.get(savingKey)
      ?? gatewayRoutingEnableIdempotencyKey(upstreamGroupID, presenceGeneration, channelIdentityRevision)
    routingEnableIdempotencyKeys.current.set(savingKey, idempotencyKey)
    setRoutingEnableSavingKeys((current) => new Set(current).add(savingKey))
    try {
      const result = await apiFetch<GatewayRoutingEnableResult>(`/gateway/upstream-groups/${upstreamGroupID}/routing-enable`, {
        method: "POST",
        headers: { "Idempotency-Key": idempotencyKey },
        body: JSON.stringify({
          presence_generation: presenceGeneration,
          channel_identity_revision: channelIdentityRevision,
          reason: "operator_one_click_enable",
        }),
      })
      routingEnableIdempotencyKeys.current.delete(savingKey)
      refetchGatewayCandidateData()
      toast.success(gatewayRoutingEnableSuccessMessage(result, groupLabel))
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "启用分组路由失败")
    } finally {
      setRoutingEnableSavingKeys((current) => {
        const next = new Set(current)
        next.delete(savingKey)
        return next
      })
    }
  }

  function isRoutingGroupEnableSaving(candidate: GatewayRoutingEnableCandidate) {
    return routingEnableSavingKeys.has(`${candidate.upstream_group_id ?? 0}:${candidate.channel_identity_revision ?? 0}:${candidate.presence_generation ?? 0}`)
  }

  function setProbeChannelExpanded(channelID: number, expanded: boolean) {
    setExpandedProbeChannels((current) => {
      const next = new Set(current)
      if (expanded) next.add(channelID)
      else next.delete(channelID)
      return next
    })
  }

  function setVisibleProbeChannelsExpanded(expanded: boolean) {
    setExpandedProbeChannels((current) => {
      const next = new Set(current)
      for (const group of filteredProbeChannelGroups) {
        if (expanded) next.add(group.channelID)
        else next.delete(group.channelID)
      }
      return next
    })
  }

  async function saveGatewayConfig() {
    const currentConfigForm = configFormRef.current
    if (!currentConfigForm) return
    const shouldSave = gatewayConfigDirty
    if (!shouldSave && !gatewayConfigPendingApply) return
    setSavingConfig(true)
    try {
      if (shouldSave) {
        await apiFetch("/settings/config/gateway", {
          method: "PATCH",
          body: JSON.stringify(currentConfigForm),
        })
        setGatewayConfigPendingApply(true)
        acceptNextGatewayConfig.current = true
        gatewayConfig.refetch()
      }
      setApplyingConfig(true)
      const result = await apiFetch<{ message: string }>("/settings/apply", { method: "POST" })
      setUsageRuntimeTimeZone(confirmDisplayTimeZone(currentConfigForm.app.timeZone))
      toast.success(result.message)
      setGatewayConfigPendingApply(false)
      refetchGatewayCandidateData()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存并应用参数失败")
    } finally {
      setSavingConfig(false)
      setApplyingConfig(false)
    }
  }

  async function refreshGatewayConfigForm() {
    if (gatewayConfigDirty) {
      const discard = await confirm({
        title: "放弃未保存的参数更改？",
        description: "模型能力刷新和冷却恢复操作不属于参数更改，不受影响。",
        confirmLabel: "放弃并刷新",
        destructive: true,
      })
      if (!discard) return
    }
    const currentServerConfig = gatewayConfig.data?.config
    if (currentServerConfig) {
      gatewayConfigSnapshot.current = JSON.stringify(currentServerConfig)
      setConfigForm(currentServerConfig)
    }
    setDirtyGatewayNumberFields(new Set())
    acceptNextGatewayConfig.current = true
    gatewayConfig.refetch()
  }

  function updateGatewayNumberDraftState(key: string, dirty: boolean) {
    setDirtyGatewayNumberFields((current) => {
      const next = new Set(current)
      if (dirty) next.add(key)
      else next.delete(key)
      if (next.size === current.size && Array.from(next).every((item) => current.has(item))) return current
      return next
    })
  }

  function updateSchedulerField<K extends keyof SystemConfig["scheduler"]>(key: K, value: SystemConfig["scheduler"][K]) {
    setConfigForm((prev) => prev ? {
      ...prev,
      scheduler: {
        ...prev.scheduler,
        [key]: value,
      },
    } : prev)
  }

  function updateLatencyScoreBoundary(key: "latencyFullScoreMs" | "latencyZeroScoreMs", rawValue: number) {
    const value = Math.max(1, Math.round(rawValue || 1))
    setConfigForm((prev) => {
      if (!prev) return prev
      const fullScoreMs = key === "latencyFullScoreMs" ? value : prev.scheduler.latencyFullScoreMs
      const zeroScoreMs = key === "latencyZeroScoreMs" ? value : prev.scheduler.latencyZeroScoreMs
      return {
        ...prev,
        scheduler: {
          ...prev.scheduler,
          latencyFullScoreMs: Math.max(1, Math.min(fullScoreMs, zeroScoreMs - 1000)),
          latencyZeroScoreMs: Math.max(zeroScoreMs, fullScoreMs + 1000),
        },
      }
    })
  }

  function updateRoutingResponseTimeout(rawValue: number) {
    const value = Math.max(1, Math.round(rawValue || 1))
    setConfigForm((prev) => prev ? {
      ...prev,
      scheduler: {
        ...prev.scheduler,
        routingFailures: {
          ...prev.scheduler.routingFailures,
          responseTimeoutSeconds: value,
        },
      },
    } : prev)
  }

  function updateManualRoutingCooldown(rawValue: number) {
    const value = Math.min(1440, Math.max(1, Math.round(rawValue || 1)))
    setConfigForm((prev) => prev ? {
      ...prev,
      scheduler: {
        ...prev.scheduler,
        routingFailures: {
          ...prev.scheduler.routingFailures,
          manualCooldownMinutes: value,
        },
      },
    } : prev)
  }

  function refetchGatewayCandidateData() {
    keyRoutePreviews.refetch()
    probeConfig.refetch()
  }

  function beginSavingKey(id: number) {
    const next = beginKeyOperation(savingKeyIDsRef.current, id)
    if (!next.started) return false
    savingKeyIDsRef.current = next.activeKeyIDs
    setSavingKeyIDs(next.activeKeyIDs)
    return true
  }

  function finishSavingKey(id: number) {
    const next = finishKeyOperation(savingKeyIDsRef.current, id)
    savingKeyIDsRef.current = next
    setSavingKeyIDs(next)
  }

  function startEdit(key: GatewayDownstreamKey) {
    setEditingID(key.id)
    setEditingName(key.name)
  }

  function cancelEdit() {
    setEditingID(null)
  }

  async function saveEdit(id: number, keyGroupID: string, candidateGroupID: string) {
    const trimmed = editingName.trim()
    if (!trimmed) {
      toast.error("请输入 key 名称")
      return
    }
    const key = keys.data?.find((item) => item.id === id)
    if (key && candidateGroupSelectionChanged(key.candidate_group_id, candidateGroupID) && !modelMappingCandidateGroupCompatible(key, candidateGroupID, candidateGroups.data ?? [], configCandidates)) {
      toast.error("请先关闭或调整模型映射")
      return
    }
    if (!beginSavingKey(id)) return
    try {
	  const current = (keys.data ?? []).find((key) => key.id === id)
	  if (!current) throw new Error("Key 已变化，请刷新后重试")
	  const nextKeyGroupID = keyGroupID === "__none__" ? 0 : Number(keyGroupID)
	  const payload: Record<string, unknown> = { name: trimmed }
	  if (nextKeyGroupID !== Number(current.key_group_id ?? 0)) payload.key_group_id = nextKeyGroupID
	  Object.assign(payload, candidateTargetPatchForEdit(current, nextKeyGroupID, candidateGroupID))
	      await apiFetch(`/gateway/downstream-keys/${id}`, {
	        method: "PATCH",
			body: JSON.stringify(payload),
	      })
      setEditingID((current) => finishKeyEditing(current, id))
      keys.refetch()
      keyRoutePreviews.refetch()
      toast.success("Key 配置已更新")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "更新失败")
    } finally {
      finishSavingKey(id)
    }
  }

  async function saveModelMapping(id: number, draft: ModelMappingDraft) {
    if (draft.strategy === "manual" && !draft.model.trim()) {
      toast.error("请选择固定模型")
      return
    }
    if (draft.strategy === "manual" && !draft.reasoningEffort.trim()) {
      toast.error("请选择固定思考强度")
      return
    }
    const key = keys.data?.find((item) => item.id === id)
    if (!key) {
      toast.error("Key 配置已变化，请刷新后重试")
      return
    }
    if (!modelMappingDraftCandidateGroupCompatible(key, draft, key.candidate_group_id ?? 0, candidateGroups.data ?? [], configCandidates)) {
      toast.error("当前候选分组不支持该模型映射")
      return
    }
    if (!beginSavingKey(id)) return
    try {
      await apiFetch(`/gateway/downstream-keys/${id}`, {
        method: "PATCH",
        body: JSON.stringify(buildModelMappingPatch(draft)),
      })
      keys.refetch()
      keyRoutePreviews.refetch()
      toast.success(draft.strategy === "off" ? "已按下游请求选择模型" : "模型映射已更新")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "更新模型映射失败")
    } finally {
      finishSavingKey(id)
    }
  }

  async function toggleEnabled(id: number, enabled: boolean) {
    if (!beginSavingKey(id)) return
    try {
      await apiFetch(`/gateway/downstream-keys/${id}`, {
        method: "PATCH",
        body: JSON.stringify({ enabled }),
      })
      keys.refetch()
      toast.success(enabled ? "key 已启用" : "key 已停用")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "更新失败")
    } finally {
      finishSavingKey(id)
    }
  }

  async function updateRouting(key: GatewayDownstreamKey, smartRouting: boolean, candidate?: GatewayRouteCandidate) {
    if (!smartRouting && !candidate) {
      toast.error("请先选择固定候选")
      return
    }
    if (!beginSavingKey(key.id)) return
    try {
      await apiFetch(`/gateway/downstream-keys/${key.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          smart_routing: smartRouting,
          manual_channel_id: candidate?.channel_id ?? key.manual_channel_id ?? 0,
          manual_group_name: candidate?.group_name ?? key.manual_group_name ?? "",
        }),
      })
      keys.refetch()
		keyRoutePreviews.refetch()
      toast.success(smartRouting ? "已开启智能路由" : "已固定候选渠道")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "更新路由失败")
    } finally {
      finishSavingKey(key.id)
    }
  }

  async function toggleSmartRouting(key: GatewayDownstreamKey, checked: boolean, candidates: GatewayRouteCandidate[]) {
    if (checked) {
      await updateRouting(key, true)
      return
    }
    const current = selectedManualCandidate(key, candidates) ?? candidates[0]
    if (!current) {
      toast.error("当前类型没有可用候选，无法关闭智能路由")
      return
    }
    await updateRouting(key, false, current)
  }

  async function toggleRouteExclusion(key: GatewayDownstreamKey, candidate: GatewayRouteCandidate, disabled: boolean) {
    const savingKey = routeExclusionSavingKey(key, candidate)
    if (routeExclusionSavingRef.current != null) return false
    routeExclusionSavingRef.current = savingKey
    setRouteExclusionSaving(savingKey)
    try {
      await apiFetch(`/gateway/downstream-keys/${key.id}/route-exclusions`, {
        method: "PATCH",
        body: JSON.stringify({
          channel_id: candidate.channel_id,
          group_name: routeCandidateGroupName(candidate),
          disabled,
        }),
      })
      keys.refetch()
      keyRoutePreviews.refetch()
      toast.success(disabled ? "已对此 Key 禁用该候选" : "已对此 Key 启用该候选")
      return true
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "更新候选状态失败")
      return false
    } finally {
      if (routeExclusionSavingRef.current === savingKey) {
        routeExclusionSavingRef.current = null
        setRouteExclusionSaving((current) => current === savingKey ? null : current)
      }
    }
  }

  async function deleteKey(id: number, keyName: string) {
    const ok = await confirm({
      title: "删除下游 key",
      description: `删除 ${keyName} 后，使用该 key 的下游请求会立即失效。`,
      confirmLabel: "删除",
      destructive: true,
    })
    if (!ok) return
    if (!beginSavingKey(id)) return
    try {
      await apiFetch(`/gateway/downstream-keys/${id}`, { method: "DELETE" })
      keys.refetch()
      if (created?.key.id === id) setCreated(null)
      toast.success("key 已删除")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "删除失败")
    } finally {
      finishSavingKey(id)
    }
  }

	function renderRouteCandidateItem(
		key: GatewayDownstreamKey,
		candidate: GatewayRouteCandidate,
		rank: number,
		onChanged?: (disabled: boolean) => void,
		interactionsDisabled = false,
		) {
			const disabled = candidate.disabled_for_key
			const cooling = candidate.cooling_down
			const mappedTargetPreview = keyRoutePreviews.previews[key.id]
			const mappedTargetState = manualMappedTargetSelectionState(candidate, mappedTargetPreview?.mappedTargetData)
			const mappedTargetPending = mappedTargetPreview == null || mappedTargetPreview.mappedTargetLoading
			const mappedTargetFailed = Boolean(mappedTargetPreview?.mappedTargetError)
			const manualSelectable = !mappedTargetPending && !mappedTargetFailed && mappedTargetPreview.mappedTargetData != null && mappedTargetState.selectable
			const manualBlocker = mappedTargetState.blockingReasons.map(candidateAvailabilityLabel).join("、")
			const smartRouteUnavailable = !clientImportCandidateRoutable(candidate)
		const unavailable = key.smart_routing ? smartRouteUnavailable : !manualSelectable
    const saving = routeExclusionSaving === routeExclusionSavingKey(key, candidate)
		const anotherCandidateSaving = routeExclusionSaving != null && !saving
		const selected = !key.smart_routing && selectedManualCandidate(key, [candidate]) !== undefined
		const routingEnableSaving = isRoutingGroupEnableSaving(candidate)
		const routingEnableAllowed = gatewayRoutingEnableEligible(candidate)
		const routingEnableBlocker = gatewayRoutingEnableBlockedReason(candidate)
		const manualCooldownActive = candidate.last_routing_failure_kind === "manual_cooldown" && candidate.cooldown_remaining > 0
    return (
      <div key={routeCandidateKey(candidate)} className={cn("grid min-h-12 min-w-[1080px] grid-cols-[2rem_minmax(11rem,1.5fr)_6rem_6rem_5rem_4rem_minmax(16rem,1fr)] items-center gap-3 border-b border-border px-3 py-2 text-sm", unavailable ? "bg-muted/20 text-muted-foreground" : "bg-card")}>
        <Badge variant={rank === 1 && !unavailable ? "default" : "outline"} className="h-6 min-w-7 justify-center px-1 text-xs">{rank > 0 ? rank : "—"}</Badge>
        <div className="flex min-w-0 items-center gap-2" title={`${routeCandidateChannelName(candidate)} / ${routeCandidateGroupName(candidate)} · ${gatewayAppShortLabel(candidate.gateway_app)}`}>
          <span className="min-w-0 truncate font-medium">{routeCandidateChannelName(candidate)}</span>
          <span className="shrink-0 text-muted-foreground">/</span>
          <span className="min-w-0 truncate text-muted-foreground">{routeCandidateGroupName(candidate)}</span>
        </div>
        <Badge variant="outline" className="h-6 w-fit max-w-full truncate text-xs" title={candidateAvailabilityReasons(candidate).map(candidateAvailabilityLabel).join("、")}>{disabled ? "已禁用" : cooling ? (manualCooldownActive ? "手动冷却" : "冷却中") : unavailable ? "不可用" : selected ? "当前固定" : "可用"}</Badge>
        <span className="truncate tabular-nums" title={`${candidate.effective_price_basis === "model_price_usd_per_million" ? `模型单价参考成本 · 输入占比 ${Math.round((candidate.price_input_share ?? 0.5) * 100)}% · 已计入资金系数；未命中部分按输入与写入较高价保守计算` : "分组倍率比较 · 完整模型价格暂不可比"}${candidate.cache_ratio_known ? ` · 缓存 ${(Math.max(0, Math.min(1, Number(candidate.cache_ratio) || 0)) * 100).toFixed(1)}%` : ""}`}>{routeCandidateRateSnapshotMissing(candidate) ? "倍率 暂无" : candidate.effective_price_basis === "model_price_usd_per_million" ? `$${formatRatio(routeCandidateEffectivePrice(candidate))}/M` : `${formatRatio(routeCandidateEffectivePrice(candidate))}x`}</span>
        <span className="truncate tabular-nums" title={`${candidate.latency_samples || 0} 次成功样本`}>{candidate.avg_latency_ms > 0 ? formatLatency(candidate.avg_latency_ms) : "—"}</span>
        <span className="tabular-nums" title={`价格 ${formatPriorityScore(candidate.price_score)} · 延迟 ${formatPriorityScore(candidate.latency_score)}`}>{formatPriorityScore(candidate.score)}</span>
            <div className="flex items-center justify-end gap-1.5">
			{cooling ? (
				<Button
					type="button"
					size="sm"
					variant="outline"
					disabled={interactionsDisabled || routingEnableSaving || !routingEnableAllowed}
					onClick={() => { if (!interactionsDisabled) void enableRoutingGroup(candidate) }}
					className="h-8 px-2 text-xs"
					aria-label={`一键启用 ${routeCandidateChannelName(candidate)} / ${routeCandidateGroupName(candidate)} 路由`}
					title={interactionsDisabled
						? "候选库存加载中"
						: routingEnableAllowed
						? "清除当前代次的路由健康阻断和自适应失败链"
							: routingEnableBlocker}
				>
					<Power className={routingEnableSaving ? "size-3 animate-pulse" : "size-3"} />
					{routingEnableSaving ? "启用中" : "一键启用"}
				</Button>
			) : null}
				{!key.smart_routing ? (
	                <Button
	                  type="button"
	                  size="sm"
					variant={selected ? "default" : "outline"}
						disabled={interactionsDisabled || savingKeyIDs.has(key.id) || !manualSelectable}
						onClick={() => { if (!interactionsDisabled && manualSelectable) void updateRouting(key, false, candidate) }}
	                  className="h-8 px-2 text-xs"
						title={mappedTargetPending
							? "正在核对固定目标"
							: mappedTargetFailed
								? "固定目标核对失败，请刷新后重试"
								: manualBlocker || undefined}
						aria-label={manualBlocker ? `${selected ? "当前固定" : "设为固定"}。当前不可选择：${manualBlocker}` : undefined}
	                >
	                  {selected ? "当前固定" : "设为固定"}
                </Button>
              ) : null}
              <span title={selected ? "当前 Key 正固定到此候选；请先切换固定候选或开启智能路由后再禁用。" : undefined}>
                <Button
                  type="button"
                  size="sm"
                  variant={disabled ? "outline" : "secondary"}
					disabled={interactionsDisabled || saving || anotherCandidateSaving || savingKeyIDs.has(key.id) || selected}
				  onClick={() => {
					if (interactionsDisabled) return
					void toggleRouteExclusion(key, candidate, !disabled).then((changed) => {
						if (changed) onChanged?.(!disabled)
					})
				  }}
                  className={cn(
                    "h-8 px-2 text-xs",
                    !disabled ? "text-destructive hover:text-destructive" : undefined,
                  )}
                  aria-label={selected ? "当前固定候选，暂不可禁用" : disabled ? "对此 Key 启用候选" : "对此 Key 禁用候选"}
                >
                  {saving ? "处理中" : disabled ? "启用" : "禁用"}
                </Button>
              </span>
            </div>
        {((cooling && !routingEnableAllowed) || (!key.smart_routing && (mappedTargetPending || mappedTargetFailed || manualBlocker))) ? <div className="col-span-7 pl-11">
			  {cooling && !routingEnableAllowed ? <p className="text-xs leading-4 text-muted-foreground">{`无法一键启用：${routingEnableBlocker}`}</p> : null}
			  {!key.smart_routing && mappedTargetPending ? <p className="text-xs leading-4 text-muted-foreground">{"正在核对固定目标"}</p> : null}
			  {!key.smart_routing && mappedTargetFailed ? <p className="text-xs leading-4 text-warning">{"固定目标核对失败，请刷新后重试"}</p> : null}
			  {!key.smart_routing && !mappedTargetPending && !mappedTargetFailed && manualBlocker ? <p className="text-xs leading-4 text-warning">{`固定不可选：${manualBlocker}`}</p> : null}
        </div> : null}
      </div>
    )
  }

  return (
    <section className="space-y-4">
      {view !== "keys" ? <PageHeader
        icon={view === "usage" ? BarChart3 : view === "costs" ? CircleDollarSign : Gauge}
        title={view === "usage" ? "使用记录" : view === "costs" ? "费用统计" : "网关配置"}
        description={view === "usage" ? "查看下游请求、路由结果和性能表现" : view === "costs" ? "汇总网关费用、Token 和费用可信度" : "配置模型价格、路由评分、被动健康信号和会话策略"}
        actions={view === "usage" ? (
          <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto sm:justify-end">
            <nav className="flex w-full items-center rounded-lg border border-border bg-card p-1 shadow-sm sm:w-auto" aria-label="使用记录视图">
              <GatewayUsageTabLink to={gatewayUsageTabPath("requests")} icon={FileText} active={usageTab === "requests"}>请求记录</GatewayUsageTabLink>
              <GatewayUsageTabLink to={gatewayUsageTabPath("group-cache")} icon={Database} active={usageTab === "group-cache"}>分组缓存比例</GatewayUsageTabLink>
            </nav>
            <div className="flex items-center gap-1 rounded-lg border border-brand/15 bg-brand/5 p-1">
              {[
                { label: "1天", hours: 24 },
                { label: "2天", hours: 48 },
                { label: "7天", hours: 24 * 7 },
                { label: "30天", hours: 24 * 30 },
              ].map((option) => (
                <Button key={option.hours} type="button" size="sm" variant={usageLiveRangeHours === option.hours ? "default" : "ghost"} className="h-8 px-3 text-xs" onClick={() => applyUsageQuickRange(option.hours)} disabled={usagePending}>
                  {option.label}
                </Button>
              ))}
            </div>
          </div>
        ) : undefined}
      /> : null}
        {gatewayHeroStats.length > 0 ? (
          <div className="metric-strip grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            {gatewayHeroStats.map((stat) => (
              <StatusMetricCard
                key={stat.label}
                label={stat.label}
                value={stat.value}
                hint={stat.hint}
                tone={stat.tone}
                icon={stat.icon}
                valueClassName="text-4xl"
              />
            ))}
          </div>
        ) : null}

      <div className="min-w-0 space-y-4">
      {view === "keys" ? (
        <GatewayKeysView
          keys={keys.data ?? []}
          loading={keys.loading}
          refreshing={keys.refreshing}
          error={keys.error}
          showCreate={showCreate}
          listHeaderActions={(
            <>
              <button
                type="button"
                className="flex h-9 min-w-0 items-center gap-1.5 rounded-lg border border-border bg-card px-3 font-mono text-xs font-medium text-foreground transition-colors hover:bg-muted"
                onClick={() => void copy(baseURL, "Base URL")}
                title={`复制 Base URL：${baseURL}`}
              >
                <span className="shrink-0 font-sans">{"Base URL"}</span>
                <span className="max-w-52 truncate">{baseURL}</span>
                <Copy className="size-3.5 shrink-0" />
              </button>
              <Button type="button" size="sm" variant="outline" asChild>
                <NavLink to="/gateway/upstream-changes"><BellRing className="size-3.5" aria-hidden="true" />{"上游变更"}</NavLink>
              </Button>
              <ImageOperationCapabilitiesDialog />
              <Button type="button" variant="outline" size="sm" onClick={syncModelFamilies} disabled={syncingModels}>
                <RefreshCw className={syncingModels ? "size-3.5 animate-spin" : "size-3.5"} />
                {"同步模型能力"}
              </Button>
            </>
          )}
          name={name}
          app={app}
          modelFamily={modelFamily}
          candidateGroupID={candidateGroupID}
          keyGroupID={keyGroupID}
          candidateGroups={candidateGroups.data ?? []}
          keyGroups={keyGroups.data ?? []}
          candidateGroupsLoading={candidateGroups.loading}
          candidateInventoryLoading={probeConfig.loading}
          groupCandidates={configCandidates}
          creating={creating}
          created={created}
          visibleKeyIDs={visibleKeyIDs}
          editingID={editingID}
          editingName={editingName}
          savingKeyIDs={savingKeyIDs}
          testingID={testingID}
          keyRoutePreviews={keyRoutePreviews.previews}
          onRefresh={() => { keys.refetch(); keyRoutePreviews.refetch() }}
          onToggleCreate={() => {
            setCreated(null)
            setShowCreate(true)
          }}
          onCopy={copy}
          onCreate={handleCreate}
          onCancelCreate={() => {
            setShowCreate(false)
            setCreated(null)
          }}
          onNameChange={setName}
          onAppChange={handleCreateAppChange}
          onModelFamilyChange={setModelFamily}
          onCandidateGroupChange={setCandidateGroupID}
          onKeyGroupChange={(value) => {
            setKeyGroupID(value)
            setCandidateGroupID(value === "__none__" ? "__all__" : "__policy__")
          }}
          onCandidateGroupsChanged={() => {
            candidateGroups.refetch()
            keys.refetch()
            keyRoutePreviews.refetch()
          }}
          onEnableRoutingGroup={(candidate) => { void enableRoutingGroup(candidate) }}
          isRoutingGroupEnableSaving={isRoutingGroupEnableSaving}
          onKeyGroupsChanged={() => {
            keyGroups.refetch()
            keys.refetch()
          }}
          onToggleKeyVisible={toggleKeyVisible}
          onImportCCSwitch={openClientImport}
          renderClientConfigPopover={renderClientConfigPopover}
          renderRouteCandidateItem={renderRouteCandidateItem}
          onEditingNameChange={setEditingName}
          onToggleSmartRouting={toggleSmartRouting}
          onSetManualRoute={(key, candidate) => { void updateRouting(key, false, candidate) }}
          onToggleEnabled={toggleEnabled}
          onStartEdit={startEdit}
          onSaveEdit={saveEdit}
          onSaveModelMapping={saveModelMapping}
          onCancelEdit={cancelEdit}
          onDeleteKey={deleteKey}
          onTestKey={openKeyTest}
        />
      ) : null}
      <ClientImportDialog
        request={clientImportRequest}
        baseURL={baseURL}
        originURL={originURL}
        open={clientImportRequest !== null}
        onOpenChange={(open) => { if (!open) setClientImportRequest(null) }}
        onLaunchCCSwitch={(current, selectedApp) => {
          setClientImportRequest(null)
          launchCCSwitch(current.apiKey, current.providerName, selectedApp, current.modelName)
        }}
        onLaunchDeepLink={(url, clientName) => {
          if (typeof window === "undefined") return
          setClientImportRequest(null)
          window.location.assign(url)
          toast.success(`正在打开 ${clientName}`)
        }}
        onCopy={copy}
        buildNativeConfigFiles={(apiKey, selectedApp, modelName) => buildClientConfigFiles(apiKey, selectedApp, baseURL, originURL, modelName)}
      />
      <Dialog open={testDraft !== null} onOpenChange={(open) => { if (!open && testingID === null) { setTestDraft(null); setKeyTestResult(null) } }}>
        <DialogContent className="sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2"><MessageSquareText className="size-5 text-brand" />{`${testDraft?.key.name || "Key"} · 连通性测试`}</DialogTitle>
          </DialogHeader>
          {testDraft ? <div className="space-y-3">
            <div className="flex items-end gap-3">
              <div className="grid min-w-0 flex-1 gap-2">
                <label htmlFor="gateway-test-model" className="text-sm font-medium">测试模型</label>
                <Popover open={testModelPickerOpen} onOpenChange={open => { setTestModelPickerOpen(open); setTestModelSearch("") }}>
                  <PopoverTrigger asChild><Button id="gateway-test-model" role="combobox" aria-expanded={testModelPickerOpen} variant="outline" className="w-full min-w-0 justify-between font-normal" disabled={testingID !== null}><span className="truncate">{testDraft.model || "选择或输入模型 ID"}</span><ChevronDown className="size-4 shrink-0 text-muted-foreground" /></Button></PopoverTrigger>
                  <PopoverContent align="start" className="w-[var(--radix-popover-trigger-width)] p-2">
                    <Input value={testModelSearch} onChange={event => setTestModelSearch(event.target.value)} placeholder="搜索或输入模型 ID" aria-label="搜索或输入测试模型" />
                    <div className="mt-2 max-h-60 overflow-y-auto">
                      {testDraft.models.filter(model => model.toLowerCase().includes(testModelSearch.trim().toLowerCase())).map(model => <Button key={model} variant={model === testDraft.model ? "secondary" : "ghost"} className="h-9 w-full justify-start font-mono text-xs" onClick={() => selectKeyTestModel(model)}><span className="truncate">{model}</span></Button>)}
                      {testModelSearch.trim() && !testDraft.models.includes(testModelSearch.trim()) ? <Button variant="ghost" className="h-auto min-h-9 w-full justify-start whitespace-normal break-all text-left" onClick={() => selectKeyTestModel(testModelSearch.trim())}>使用“{testModelSearch.trim()}”</Button> : null}
                      {!testDraft.models.length && !testModelSearch.trim() ? <p className="px-2 py-3 text-sm text-muted-foreground">输入模型 ID</p> : null}
                    </div>
                  </PopoverContent>
                </Popover>
              </div>
              <label className="grid w-64 shrink-0 gap-2"><span className="text-sm font-medium">测试协议</span><Select value={testDraft.protocol} onValueChange={protocol => setTestDraft({ ...testDraft, protocol: protocol as GatewayKeyTestProtocol })} disabled={testingID !== null}><SelectTrigger className="w-full" aria-label="测试协议"><SelectValue /></SelectTrigger><SelectContent>{Object.entries(gatewayKeyTestEndpoints).map(([value, endpoint]) => <SelectItem key={value} value={value}>{`POST ${endpoint}`}</SelectItem>)}</SelectContent></Select></label>
              {testingID !== null
                ? <Button variant="outline" onClick={() => testAbortRef.current?.abort()}>停止等待</Button>
                : <Button disabled={!testDraft.model.trim()} onClick={() => void testGatewayKey(testDraft.key, testDraft.model.trim(), testDraft.protocol)}>开始测试</Button>}
            </div>
            <p className="text-sm text-muted-foreground">发送一次真实请求，可能产生费用；仍遵循当前 Key 的模型映射和路由配置。</p>
          </div> : null}
          {keyTestResult ? <GatewayKeyTestResultPanel result={keyTestResult} /> : null}
        </DialogContent>
      </Dialog>
      <Dialog open={probeErrorDetail !== null} onOpenChange={(open) => { if (!open) setProbeErrorDetail(null) }}>
        <DialogContent className="w-[min(96vw,48rem)] max-w-none sm:max-w-2xl">
          <DialogHeader className="pr-8">
            <DialogTitle className="flex min-w-0 items-center gap-2 text-base">
              <span className="flex size-8 shrink-0 items-center justify-center rounded-md bg-danger/10 text-danger ring-1 ring-inset ring-danger/20">
                <TriangleAlert className="size-4" />
              </span>
              <span className="min-w-0 truncate">{probeErrorDetail?.title || "异常详情"}</span>
            </DialogTitle>
            <DialogDescription className="sr-only">{"查看并复制完整的上游同步异常信息"}</DialogDescription>
          </DialogHeader>
          <div className="max-h-[min(60vh,30rem)] overflow-auto rounded-lg border border-danger/20 bg-danger/5 p-3">
            <pre className="whitespace-pre-wrap break-words font-mono text-xs leading-5 text-foreground">
              {probeErrorDetail?.message || "暂无异常内容"}
            </pre>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" size="sm" onClick={() => void copy(probeErrorDetail?.message || "", "异常详情")} disabled={!probeErrorDetail?.message}>
              <Copy className="size-3.5" />
              {"一键复制"}
            </Button>
            <Button type="button" size="sm" onClick={() => setProbeErrorDetail(null)}>{"关闭"}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      {view === "config" ? (
      <section className="space-y-4">
        <div className="sticky top-0 z-30 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-background/95 px-3 py-2.5 shadow-sm backdrop-blur">
          <nav className="flex flex-wrap items-center gap-1" aria-label="参数分区">
            {[
              ["routing-scoring", "路由策略"],
              ["model-prices", "模型价格"],
              ["channel-lifecycle", "保活与倍率"],
              ["upstream-health", "健康状态"],
            ].map(([target, label]) => (
              <button key={target} type="button" aria-pressed={configSection === target} aria-controls={target} onClick={() => setConfigSection(target)} className={cn("inline-flex h-9 items-center rounded-lg px-3 text-sm transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring", configSection === target ? "bg-accent font-medium text-accent-foreground" : "text-muted-foreground hover:bg-muted hover:text-foreground")}>
                {label}
              </button>
            ))}
          </nav>
          <div className="flex flex-wrap items-center gap-2">
            <Badge
              variant={gatewayConfigDirty ? "secondary" : "outline"}
              className={gatewayConfigPendingApply && !gatewayConfigDirty ? "border-amber-300 bg-amber-50 text-amber-700" : undefined}
            >
              {gatewayConfigDirty ? "有未保存更改" : gatewayConfigPendingApply ? "应用失败待重试" : "已应用"}
            </Badge>
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => void refreshGatewayConfigForm()}
              disabled={gatewayConfig.loading || savingConfig || applyingConfig}
            >
              <RefreshCw className={gatewayConfig.loading ? "size-3.5 animate-spin" : "size-3.5"} />
              {gatewayConfigDirty ? "放弃更改" : "刷新"}
            </Button>
            <Button
              type="button"
              size="sm"
              onClick={() => saveGatewayConfig()}
              disabled={!configForm || (!gatewayConfigDirty && !gatewayConfigPendingApply) || savingConfig || applyingConfig}
            >
              {applyingConfig ? "应用中" : "保存并应用"}
            </Button>
          </div>
        </div>
        <div className="space-y-4">
          <div hidden={configSection !== "model-prices"}><ModelPricesPanel /></div>
          {gatewayConfig.error ? (
            <div className="rounded-lg border border-destructive/25 bg-destructive/5 px-4 py-3 text-sm text-destructive">{gatewayConfig.error}</div>
          ) : null}
          {configForm ? (
            <div className="gateway-settings-workspace overflow-hidden rounded-2xl border border-border bg-card">
              <div hidden={configSection !== "routing-scoring"} id="routing-scoring" className="scroll-mt-20 rounded-lg border border-border bg-background p-4" title="价格分与延迟分建议合计 100；延迟信号按最近样本与回看窗口滤波后参与评分">
                <h3 className="text-lg font-semibold">{"评分权重与延迟信号"}</h3>
                <div className="mt-3 grid gap-3 md:grid-cols-3 xl:grid-cols-4">
                  <GatewayConfigField label="价格满分">
                    <NumberDraftInput draftKey="routingPriceScoreMax" value={configForm.scheduler.routingPriceScoreMax} min={0} integer={false} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("routingPriceScoreMax", value)} />
                  </GatewayConfigField>
                  <GatewayConfigField label="延迟满分">
                    <NumberDraftInput draftKey="routingLatencyScoreMax" value={configForm.scheduler.routingLatencyScoreMax} min={0} integer={false} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("routingLatencyScoreMax", value)} />
                  </GatewayConfigField>
                  <GatewayConfigField label="最近样本数">
                    <NumberDraftInput draftKey="latencyProbeSampleSize" value={configForm.scheduler.latencyProbeSampleSize} min={1} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("latencyProbeSampleSize", value)} />
                  </GatewayConfigField>
                  <GatewayConfigField label="统计回看小时">
                    <NumberDraftInput draftKey="latencyStatsWindowHours" value={configForm.scheduler.latencyStatsWindowHours} min={1} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("latencyStatsWindowHours", value)} />
                  </GatewayConfigField>
                  <GatewayConfigField label="满分延迟（毫秒）">
                    <NumberDraftInput
                      draftKey="latencyFullScoreMs"
                      value={configForm.scheduler.latencyFullScoreMs}
                      min={1}
                      onDraftDirtyChange={updateGatewayNumberDraftState}
                      onValueChange={(value) => updateLatencyScoreBoundary("latencyFullScoreMs", value)}
                    />
                  </GatewayConfigField>
                  <GatewayConfigField label="0分延迟（毫秒）">
                    <NumberDraftInput
                      draftKey="latencyZeroScoreMs"
                      value={configForm.scheduler.latencyZeroScoreMs}
                      min={1}
                      onDraftDirtyChange={updateGatewayNumberDraftState}
                      onValueChange={(value) => updateLatencyScoreBoundary("latencyZeroScoreMs", value)}
                    />
                  </GatewayConfigField>
                  <GatewayConfigField label="暂无延迟得分系数">
                    <NumberDraftInput draftKey="unknownLatencyScoreFactor" value={configForm.scheduler.unknownLatencyScoreFactor} min={0.01} max={1} integer={false} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("unknownLatencyScoreFactor", Math.min(1, Math.max(0.01, value)))} />
                  </GatewayConfigField>
                </div>
              </div>
              <div hidden={configSection !== "routing-scoring"} id="routing-failures" className="scroll-mt-20 rounded-lg border border-border bg-background p-4" title="准备超时约束请求准备阶段。智能文本请求等待期间每 30 秒检查可用性，检查超过 2 分钟无响应才结束等待；已受理请求不重发。固定路由不使用智能回退或冷却">
                <h3 className="text-lg font-semibold">{"路由故障"}</h3>
                <div className="mt-3 grid gap-3 md:grid-cols-3">
                  <GatewayConfigField label="请求准备超时（秒）">
                    <NumberDraftInput
                      draftKey="routingFailures.responseTimeoutSeconds"
                      value={configForm.scheduler.routingFailures.responseTimeoutSeconds}
                      min={1}
                      onDraftDirtyChange={updateGatewayNumberDraftState}
                      onValueChange={updateRoutingResponseTimeout}
                    />
                  </GatewayConfigField>
                  <GatewayConfigField label="手动冷却（分钟）">
                    <NumberDraftInput
                      draftKey="routingFailures.manualCooldownMinutes"
                      value={configForm.scheduler.routingFailures.manualCooldownMinutes}
                      min={1}
                      max={1440}
                      onDraftDirtyChange={updateGatewayNumberDraftState}
                      onValueChange={updateManualRoutingCooldown}
                    />
                  </GatewayConfigField>
                </div>
              </div>
              <div hidden={configSection !== "routing-scoring"} id="cache-pricing" className="scroll-mt-20 rounded-lg border border-border bg-background p-4" title="智能路由按成功的真实下游数据面缓存比例折算输入价格；冷却恢复检查、测试、模型检查、内部调用和失败请求不进入统计">
                <h3 className="text-lg font-semibold">{"缓存价格折算"}</h3>
                <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <GatewayConfigField label="缓存输入价格倍率">
                    <NumberDraftInput draftKey="cacheInputPriceMultiplier" value={configForm.scheduler.cacheInputPriceMultiplier} min={0} max={1} integer={false} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("cacheInputPriceMultiplier", Math.min(1, Math.max(0, value)))} />
                  </GatewayConfigField>
                  <GatewayConfigField label="最低有效输入 Token">
                    <NumberDraftInput draftKey="cachePriceMinInputTokens" value={configForm.scheduler.cachePriceMinInputTokens} min={1} max={1_000_000_000_000} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("cachePriceMinInputTokens", Math.min(1_000_000_000_000, Math.max(1, Math.round(value))))} />
                  </GatewayConfigField>
                  <GatewayConfigField label="统计输入 Token 窗口">
                    <NumberDraftInput draftKey="cachePriceWindowInputTokens" value={configForm.scheduler.cachePriceWindowInputTokens} min={1} max={1_000_000_000_000} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("cachePriceWindowInputTokens", Math.min(1_000_000_000_000, Math.max(1, Math.round(value))))} />
                  </GatewayConfigField>
                  <GatewayConfigField label="统计失效天数">
                    <NumberDraftInput draftKey="cacheStatsMaxAgeDays" value={configForm.scheduler.cacheStatsMaxAgeDays} min={1} max={365} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("cacheStatsMaxAgeDays", Math.min(365, Math.max(1, Math.round(value))))} />
                  </GatewayConfigField>
                </div>
              </div>
              <div hidden={configSection !== "channel-lifecycle"} id="channel-lifecycle" className="scroll-mt-20 rounded-lg border border-border bg-background p-4" title="保活维持上游登录态；倍率快照会直接影响成本优先路由">
                <h3 className="text-lg font-semibold">{"渠道保活与倍率刷新"}</h3>
                <div className="mt-3 grid gap-3 md:grid-cols-3">
                  <div className="flex items-center justify-between rounded-md border border-border bg-background px-3 py-2">
                    <div>
                      <div className="text-xs font-medium">{"自动保活"}</div>
                      <div className="text-[11px] text-muted-foreground">{"关闭后只在手动刷新或网关请求时校验"}</div>
                    </div>
                    <Switch checked={configForm.scheduler.sessionKeepAliveEnabled} onCheckedChange={(checked) => updateSchedulerField("sessionKeepAliveEnabled", checked)} />
                  </div>
                  <GatewayConfigField label="保活周期（分钟）">
                    <NumberDraftInput
                      draftKey="sessionKeepAliveIntervalMinutes"
                      value={configForm.scheduler.sessionKeepAliveIntervalMinutes}
                      min={1}
                      onDraftDirtyChange={updateGatewayNumberDraftState}
                      onValueChange={(value) => updateSchedulerField("sessionKeepAliveIntervalMinutes", value)}
                    />
                  </GatewayConfigField>
                  <GatewayConfigField label="倍率快照刷新（分钟）">
                    <NumberDraftInput
                      draftKey="rateRefreshIntervalMinutes"
                      value={configForm.scheduler.rateRefreshIntervalMinutes}
                      min={0}
                      onDraftDirtyChange={updateGatewayNumberDraftState}
                      onValueChange={(value) => updateSchedulerField("rateRefreshIntervalMinutes", value)}
                    />
                  </GatewayConfigField>
                </div>
                <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
                  {"NewAPI 浏览器 Cookie 如果在上游服务端彻底失效，系统无法无凭据刷新；建议优先使用账号密码模式或系统访问令牌。"}
                </p>
              </div>
              <div hidden={configSection !== "channel-lifecycle"} id="funding-detection" className="scroll-mt-20 rounded-lg border border-border bg-background p-4" title="低于此金额的余额变化仍会记录，但不会生成待确认的充值到账项">
                <h3 className="text-lg font-semibold">{"到账识别"}</h3>
                <div className="mt-3 grid gap-3 md:grid-cols-3">
                  <GatewayConfigField label="最小到账金额">
                    <NumberDraftInput
                      draftKey="fundingDetectionThreshold"
                      value={configForm.scheduler.fundingDetectionThreshold}
                      min={0.99}
                      integer={false}
                      onDraftDirtyChange={updateGatewayNumberDraftState}
                      onValueChange={(value) => updateSchedulerField("fundingDetectionThreshold", value)}
                    />
                  </GatewayConfigField>
                </div>
              </div>
              <div hidden={configSection !== "routing-scoring"} id="session-affinity" className="scroll-mt-20 rounded-lg border border-border bg-background p-4" title="首选渠道稳定后会强制回到首选，避免长期挂在高价亲和渠道上">
                <h3 className="text-lg font-semibold">{"会话亲和"}</h3>
                <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  <GatewayConfigField label="亲和有效期（分钟）">
                    <NumberDraftInput draftKey="sessionAffinityMaxAgeMinutes" value={configForm.scheduler.sessionAffinityMaxAgeMinutes} min={1} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("sessionAffinityMaxAgeMinutes", value)} />
                  </GatewayConfigField>
                  <GatewayConfigField label="首选稳定样本数">
                    <NumberDraftInput draftKey="sessionAffinityStableSamples" value={configForm.scheduler.sessionAffinityStableSamples} min={1} onDraftDirtyChange={updateGatewayNumberDraftState} onValueChange={(value) => updateSchedulerField("sessionAffinityStableSamples", value)} />
                  </GatewayConfigField>
                </div>
              </div>
            </div>
          ) : (
            <div className="p-4 text-sm text-muted-foreground">{"加载参数中"}</div>
          )}
        </div>

        <div hidden={configSection !== "upstream-health"} id="upstream-health" className="scroll-mt-20 overflow-hidden rounded-lg border border-border bg-background">
          <GatewaySectionHeader
            title="上游健康状态"
            description="延迟由真实请求被动记录；只有进入路由冷却恢复阶段时，系统才会自动探测。"
            actions={(
              <Button type="button" size="sm" variant="outline" onClick={syncModelFamilies} disabled={syncingModels}>
                <RefreshCw className={syncingModels ? "size-3.5 animate-spin" : "size-3.5"} />
                {"刷新全部模型"}
              </Button>
            )}
          />
          {probeConfig.error ? (
            <div className="border-b border-border px-4 py-3 text-sm text-destructive">{probeConfig.error}</div>
          ) : null}
          <div className="grid gap-3 border-b border-border bg-card p-3 sm:grid-cols-3">
            <StatusMetricCard icon={Activity} label="渠道" value={probeChannelGroups.length} hint="全部已配置渠道" tone="brand" valueClassName="text-4xl" />
            <StatusMetricCard icon={SlidersHorizontal} label="分组" value={uniqueProbeGroupCount(configCandidates)} hint="已同步倍率分组" tone="muted" valueClassName="text-4xl" />
            <StatusMetricCard icon={TriangleAlert} label="配置异常" value={probeIssueChannelCount} hint={probeIssueChannelCount > 0 ? "需要检查" : "当前正常"} tone={probeIssueChannelCount > 0 ? "warning" : "success"} valueClassName="text-4xl" />
          </div>
          <div className="border-b border-border bg-muted/10 p-3">
            <div className="grid gap-3 xl:grid-cols-[minmax(20rem,1fr)_auto] xl:items-center">
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  value={probeSearch}
                  onChange={(event) => setProbeSearch(event.target.value)}
                  placeholder="搜索渠道、分组或模型能力"
                  className="pl-9"
                />
              </div>
              <div className="flex flex-wrap items-center justify-end gap-2">
                <Button
                  type="button"
                  size="sm"
                  variant={probeIssuesOnly ? "secondary" : "outline"}
                  onClick={() => setProbeIssuesOnly((current) => !current)}
                  aria-pressed={probeIssuesOnly}
                >
                  <TriangleAlert className="size-3.5" />
                  {`只看配置异常 (${probeIssueChannelCount})`}
                </Button>
                <Button type="button" size="sm" variant="outline" onClick={() => setVisibleProbeChannelsExpanded(true)} disabled={filteredProbeChannelGroups.length === 0}>
                  <ChevronsUpDown className="size-3.5" />
                  {"展开"}
                </Button>
                <Button type="button" size="sm" variant="outline" onClick={() => setVisibleProbeChannelsExpanded(false)} disabled={filteredProbeChannelGroups.length === 0}>
                  <ChevronsDownUp className="size-3.5" />
                  {"收起"}
                </Button>
              </div>
            </div>
            <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
              <span>{`显示 ${filteredProbeChannelGroups.length}/${probeChannelGroups.length} 个已配置渠道`}</span>
              <span>{"延迟由真实请求被动记录；仅在路由冷却恢复阶段自动探测。"}</span>
            </div>
          </div>
          <div className="space-y-2 p-3">
            {filteredProbeChannelGroups.map((group) => {
              const expanded = expandedProbeChannels.has(group.channelID)
              const issueCount = probeGroupIssueCount(group, probeMinimumSamples)
              const averageLatency = probeGroupAverageLatency(group)
              const probeRows = groupProbeRows(group.candidates)
              const availabilityLabel = group.availabilityReasons[0]
                ? probeAvailabilityLabel(group.availabilityReasons[0])
                : ""
              return (
                <Collapsible
                  key={`probe-channel-${group.channelID}`}
                  open={expanded}
                  onOpenChange={(open) => setProbeChannelExpanded(group.channelID, open)}
                  className="overflow-hidden rounded-md border border-border bg-background"
                >
                  <div className="flex min-h-14 items-stretch">
                    <CollapsibleTrigger asChild>
                      <button
                        type="button"
                        className="flex min-w-0 flex-1 items-center gap-3 px-3 py-2.5 text-left hover:bg-muted/25 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset"
                        aria-label={`${expanded ? "收起" : "展开"}${group.channelName || `渠道 ${group.channelID}`}`}
                      >
                        <ChevronDown className={cn("size-4 shrink-0 text-muted-foreground transition-transform", expanded ? "rotate-180" : "")} />
                        <span className="min-w-0 flex-1">
                          <span className="flex flex-wrap items-center gap-2">
                            <span className="max-w-64 truncate text-sm font-semibold text-foreground" title={group.channelName}>{group.channelName || `渠道 ${group.channelID}`}</span>
                            {group.channelType ? <Badge variant="secondary" className="uppercase">{group.channelType}</Badge> : null}
                            <Badge variant="outline">{`${uniqueProbeGroupCount(group.candidates)} 个分组`}</Badge>
                            {!group.routingAvailable ? (
                              <Badge variant="outline" className="border-destructive/35 bg-destructive/5 text-destructive" title={group.availabilityReasons.map(probeAvailabilityLabel).join("、") || group.lastError}>
                                {availabilityLabel || "当前不可路由"}
                              </Badge>
                            ) : null}
                            {issueCount > 0 ? (
                              <Badge variant="outline" className="border-amber-300 bg-amber-50 text-amber-700 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-300">
                                <TriangleAlert className="size-3" />
                                {`${issueCount} 项需关注`}
                              </Badge>
                            ) : null}
                          </span>
                          <span className="mt-1 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
                            <span>{group.routingAvailable ? "真实请求持续记录健康信号" : "当前不可路由"}</span>
                            <span>{averageLatency > 0 ? `达标平均延迟 ${formatLatency(averageLatency)}` : "暂无达标延迟样本"}</span>
                          </span>
                        </span>
                      </button>
                    </CollapsibleTrigger>
                    <div className="flex shrink-0 items-center gap-3 border-l border-border px-3">
                      <Button
                        type="button"
                        size="icon-sm"
                        variant="ghost"
                        onClick={() => void rebuildManagedUpstreamKeys(group.channelID, group.channelName || `渠道 ${group.channelID}`)}
                        disabled={rebuildingChannelID === group.channelID || syncingChannelID === group.channelID}
                        aria-label={`重建 ${group.channelName} 受管 Key`}
                        title="删除并重建该渠道的受管 Key"
                      >
                        <RotateCcw className={rebuildingChannelID === group.channelID ? "size-3.5 animate-spin" : "size-3.5"} />
                      </Button>
                      <Button
                        type="button"
                        size="icon-sm"
                        variant="ghost"
                        onClick={() => syncChannelModelFamilies(group.channelID)}
                        disabled={syncingChannelID === group.channelID || probeChannelSyncBlocked(group)}
                        aria-label={`刷新 ${group.channelName} 模型`}
                        title="刷新该渠道模型"
                      >
                        <RefreshCw className={syncingChannelID === group.channelID ? "size-3.5 animate-spin" : "size-3.5"} />
                      </Button>
                    </div>
                  </div>
                  <CollapsibleContent className="border-t border-border">
                    <Table className="min-w-[640px] table-fixed">
                      <TableHeader className="bg-muted/15">
                        <TableRow>
                          <TableHead className="w-52">{"分组"}</TableHead>
                          <TableHead className="w-60">{"价格 / 被动延迟"}</TableHead>
                          <TableHead>{"模型能力 / 健康"}</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {probeRows.map((row) => {
                          const candidate = row.candidates[0]
                          const families = probeRowFamilies(row)
                          const apps = probeRowApps(row)
                          const hasUnsupportedApp = row.candidates.some((item) => item.probe_blockers?.includes("no_supported_gateway_app"))
                          const rowIssues = probeRowIssueLabels(row, probeMinimumSamples)
                          const latency = probeRowLatency(row)
                          const latencySamplesTitle = latency.rawSamples.length > 0
                            ? `最近样本：${latency.rawSamples.map((value) => formatLatency(value)).join("、")}`
                            : "暂无原始样本"
                          const modelsError = probeRowModelsError(row)
                          const upstreamError = probeRowUpstreamError(row)
                          const probeErrorMessage = [
                            modelsError ? `${probeRowModelIDs(row).length > 0 ? "模型更新" : "模型同步"}：\n${modelsError}` : "",
                            upstreamError ? `上游 Key：\n${upstreamError}` : "",
                          ].filter(Boolean).join("\n\n")
                          const routingFailure = probeRowRoutingFailure(row)
                          const modelsSyncedAt = probeRowModelsSyncedAt(row)
                          const routingEnableCandidate = row.candidates.find((item) => item.cooling_down) ?? candidate
                          const routingEnableSavingKey = `${routingEnableCandidate.upstream_group_id ?? 0}:${routingEnableCandidate.channel_identity_revision ?? 0}:${routingEnableCandidate.presence_generation ?? 0}`
                          const routingEnableSaving = routingEnableSavingKeys.has(routingEnableSavingKey)
                          const routingEnableAllowed = gatewayRoutingEnableEligible(routingEnableCandidate)
                          return (
                            <TableRow key={row.key}>
                              <TableCell className="whitespace-normal">
                                <div className="flex min-w-0 items-start gap-2">
                                  <div className="min-w-0">
                                    <div className="max-w-48 truncate font-medium" title={row.groupName}>{row.groupName || "-"}</div>
                                    <div className="mt-1 flex max-w-52 flex-wrap gap-1">
                                      {families.map((family) => (
                                        <Badge key={family} variant="secondary" className="h-5 px-1.5 text-[10px]">{modelFamilyLabel(family)}</Badge>
                                      ))}
                                    </div>
                                    <div className="mt-1 text-xs text-muted-foreground">
                                      {hasUnsupportedApp ? "无兼容网关协议" : `${apps.map((item) => gatewayAppShortLabel(item)).join(" / ")} 兼容协议`}
                                    </div>
                                  </div>
                                  {rowIssues.length > 0 ? (
                                    <span title={rowIssues.join("、")}>
                                      <TriangleAlert className="size-3.5 shrink-0 text-amber-500" aria-label={rowIssues.join("、")} />
                                    </span>
                                  ) : null}
                                </div>
                              </TableCell>
                              <TableCell>
                                <div>{probeRowPriceLabel(row)}</div>
                                <div className="mt-0.5 text-xs text-muted-foreground" title={latencySamplesTitle}>
                                  {latency.qualified && latency.average > 0
                                    ? `${formatLatency(latency.average)} · ${latency.samples} 个样本 · ${probeLatencySourceLabel(latency.source)}`
                                    : `真实请求样本 ${latency.samples}/${probeMinimumSamples} · 尚未参与评分`}
                                </div>
                              </TableCell>
                              <TableCell>
                                <div className="min-w-0 text-xs text-muted-foreground">
                                  <div>
                                    {hasUnsupportedApp ? "无兼容网关协议" : modelsSyncedAt ? `模型能力 ${relativeTime(modelsSyncedAt)}` : "模型能力未同步"}
                                  </div>
                                  {probeErrorMessage ? (
                                    <button
                                      type="button"
                                      className="mt-1 inline-flex h-6 items-center gap-1 rounded-md border border-danger/25 bg-danger/5 px-2 text-[11px] font-medium text-danger hover:bg-danger/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-danger/30"
                                      aria-haspopup="dialog"
                                      aria-label={`查看 ${group.channelName} / ${row.groupName} 异常详情`}
                                      title="查看完整异常详情"
                                      onClick={() => setProbeErrorDetail({
                                        title: `${group.channelName} / ${row.groupName} · 异常详情`,
                                        message: probeErrorMessage,
                                      })}
                                    >
                                      <TriangleAlert className="size-3" />
                                      {"异常"}
                                    </button>
                                  ) : null}
                                  {routingFailure?.at ? (
                                    <div className="max-w-44 truncate text-amber-600 dark:text-amber-300" title={routingFailure.detail || routingFailure.label}>
                                      {`${routingFailure.label} · ${relativeTime(routingFailure.at)}`}
                                    </div>
                                  ) : null}
                                  {routingEnableCandidate.cooling_down ? (
                                    <>
                                      <Button
                                        type="button"
                                        size="sm"
                                        variant="outline"
                                        className="mt-1 h-7 px-2 text-[11px]"
                                        disabled={routingEnableSaving || !routingEnableAllowed}
                                        onClick={() => void enableRoutingGroup(routingEnableCandidate)}
                                        aria-label={`一键启用 ${group.channelName} / ${row.groupName} 路由`}
                                        title={routingEnableAllowed
                                          ? "清除当前代次的路由健康阻断和自适应失败链"
                                          : "本地 Key 缺失、停用、仍在创建，或分组代次已变化"}
                                      >
                                        <Power className={routingEnableSaving ? "size-3 animate-pulse" : "size-3"} />
                                        {routingEnableSaving ? "启用中" : "一键启用"}
                                      </Button>
                                      {!routingEnableAllowed ? <p className="mt-1 text-[10px] leading-4 text-muted-foreground">{"无法一键启用：本地 Key 缺失、停用、仍在创建，或分组代次已变化"}</p> : null}
                                    </>
                                  ) : null}
                                </div>
                              </TableCell>
                            </TableRow>
                          )
                        })}
                        {probeRows.length === 0 ? (
                          <TableRow>
                            <TableCell colSpan={3} className="h-20 text-center text-sm text-muted-foreground">
                              {"暂无倍率分组；请先在渠道管理中刷新倍率。"}
                            </TableCell>
                          </TableRow>
                        ) : null}
                      </TableBody>
                    </Table>
                  </CollapsibleContent>
                </Collapsible>
              )
            })}
            {!probeConfig.loading && filteredProbeChannelGroups.length === 0 ? (
              <div className="flex min-h-28 items-center justify-center rounded-md border border-dashed border-border px-4 text-center text-sm text-muted-foreground">
                {probeChannelGroups.length === 0 ? "暂无已配置渠道" : "没有符合当前搜索和筛选条件的渠道"}
              </div>
            ) : null}
          </div>
        </div>
      </section>
      ) : null}
      {view === "usage" ? (
        <GatewayUsageView
          tab={usageTab}
          total={usageTotal}
          timeZone={usageDisplayTimeZone}
          itemCount={usageItemCount}
          items={usageItems}
          loading={usageTimeZonePending || requestLogs.loading}
          pending={usagePending}
          error={gatewayConfig.error ?? requestLogs.error}
          activeFilterCount={usageActiveFilterCount}
          exporting={exportingUsage}
          draftFilters={usageDraftFilters}
          downstreamKeys={keys.data ?? []}
          channels={channels.data ?? []}
          models={usageFilterOptions.data?.models ?? []}
          groups={usageFilterOptions.data?.groups ?? []}
          analytics={usageDataVisible ? requestAnalytics.data : null}
          analyticsLoading={usageTimeZonePending || requestAnalytics.loading}
          analyticsError={gatewayConfig.error ?? requestAnalytics.error}
          groupCachePage={usageDataVisible && groupCacheRequestResolved ? groupCachePage.data : null}
          groupCacheLoading={usageTimeZonePending || groupCacheRequestAwaiting || groupCachePage.loading}
          groupCachePending={groupCachePending}
          groupCacheError={gatewayConfig.error ?? groupCachePage.error}
          groupCachePageNumber={gatewayGroupCachePageNumber(groupCachePagination)}
          groupCachePageSize={groupCachePagination.pageSize}
          groupCacheCanGoBack={gatewayGroupCacheCanGoBack(groupCachePagination)}
          rangeStart={usageRangeStart}
          rangeEnd={usageRangeEnd}
          pageSize={usagePageSize}
          currentPage={usageCurrentPage}
          totalPages={usageTotalPages}
          pagerNumbers={usagePagerNumbers}
          onExport={exportUsageLogs}
          sortBy={usageSortBy}
          sortOrder={usageSortOrder}
          manualCooldownMinutes={gatewayConfig.data?.config.scheduler.routingFailures.manualCooldownMinutes ?? 10}
          onRefresh={refreshUsageData}
          onApplyFilters={applyUsageFilters}
          onResetFilters={resetUsageFilters}
          onUpdateFilter={updateUsageFilter}
          onSort={updateUsageSort}
          onPageSizeChange={(nextPageSize) => {
            setUsagePageSize(nextPageSize)
            setUsagePage(1)
          }}
          onGoToPage={goToUsagePage}
          onGroupCacheNext={goToNextGroupCachePage}
          onGroupCacheBack={goToPreviousGroupCachePage}
          onGroupCachePageSizeChange={updateGroupCachePageSize}
        />
      ) : null}
      {view === "costs" ? (
        <GatewayCostsView
          summary={usageSummary}
          pending={usagePending}
          draftFilters={usageDraftFilters}
          downstreamKeys={keys.data ?? []}
          channels={channels.data ?? []}
          models={usageFilterOptions.data?.models ?? []}
          groups={usageFilterOptions.data?.groups ?? []}
          analytics={usageDataVisible ? requestAnalytics.data : null}
          analyticsLoading={usageTimeZonePending || requestAnalytics.loading}
          timeZone={usageDisplayTimeZone}
          quickRange={costQuickRange}
          onApplyRange={applyCostRange}
          onApplyFilters={applyUsageFilters}
          onResetFilters={resetCostFilters}
          onUpdateFilter={updateUsageFilter}
        />
      ) : null}
          {dialog}
      </div>
    </section>
  )
}

function GatewayUsageTabLink({ to, icon: Icon, active, children }: { to: string; icon: LucideIcon; active: boolean; children: ReactNode }) {
  return (
    <NavLink
      to={to}
      aria-current={active ? "page" : undefined}
      className={cn(
        "inline-flex h-8 min-w-0 flex-1 items-center justify-center gap-1.5 whitespace-nowrap rounded-md px-2 text-sm font-medium transition-colors sm:flex-none sm:gap-2 sm:px-3",
        active ? "bg-primary text-primary-foreground shadow-sm" : "text-muted-foreground hover:bg-muted hover:text-foreground",
      )}
    >
      <Icon className="size-4 shrink-0" />
      {children}
    </NavLink>
  )
}

function gatewayHeroStatsForView({
  usagePending,
  usageTotal,
  usageSummary,
  usageActiveFilterCount,
}: {
  usagePending: boolean
  usageTotal: number
  usageSummary: GatewayRequestLogSummary | null | undefined
  usageActiveFilterCount: number
}): GatewayHeroStat[] {
  const completedRequests = (usageSummary?.success ?? 0) + (usageSummary?.failed ?? 0)
  const requestSuccessRate = successRate(usageSummary)
  return [
    {
      label: "使用记录",
      value: usagePending ? "加载中" : usageTotal,
      hint: usageActiveFilterCount > 0 ? `已筛选 ${usageActiveFilterCount}` : "全部记录",
      tone: "brand",
      icon: Activity,
    },
    {
      label: "成功率",
      value: usagePending ? "加载中" : formatPercent(requestSuccessRate),
      hint: `${formatInteger(usageSummary?.success)} 成功 · ${formatInteger(usageSummary?.failed)} 失败 · ${formatInteger(usageSummary?.canceled)} 取消`,
      tone: completedRequests === 0 ? "muted" : requestSuccessRate >= 0.98 ? "success" : "warning",
      icon: CircleCheck,
    },
    {
      label: "Token",
      value: usagePending ? "加载中" : formatTokenMetric(usageSummary?.total_tokens),
      hint: `输入 ${formatTokenMetric(usageSummary?.prompt_tokens)} · 输出 ${formatTokenMetric(usageSummary?.completion_tokens)}`,
      tone: "warning",
      icon: Coins,
    },
    {
      label: "平均耗时",
      value: usagePending ? "加载中" : formatLatency(usageSummary?.avg_latency_ms),
      hint: "当前筛选范围",
      tone: "muted",
      icon: Timer,
    },
  ]
}

function GatewayKeyTestResultPanel({ result }: { result: GatewayKeyTestResult }) {
  const pending = result.success === null && result.durationMs === null
  const status = pending ? "等待响应…" : result.status !== null ? `HTTP ${result.status}` : result.success === null ? "未确认" : "连接失败"
  return (
    <div className="space-y-3">
      <div className="overflow-hidden rounded-lg border bg-muted/20">
        <div className="flex items-center justify-between border-b px-3 py-2 text-sm">
          <span className={cn("font-medium", result.success === true ? "text-success" : result.success === false ? "text-danger" : "text-muted-foreground")}>{status}</span>
          {result.durationMs !== null ? <span className="text-muted-foreground">{result.durationMs} ms</span> : null}
        </div>
        <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-words p-3 font-mono text-xs leading-6"><code>{result.responseBody}</code></pre>
      </div>
      <details className="rounded-lg border">
        <summary className="cursor-pointer px-3 py-2 text-sm text-muted-foreground">请求详情</summary>
        <div className="border-t px-3 py-2 font-mono text-xs">{`${result.method} ${new URL(result.url).pathname}`}</div>
        <pre className="max-h-56 overflow-auto px-3 pb-3 font-mono text-xs leading-6"><code>{result.requestBody}</code></pre>
      </details>
    </div>
  )
}

function GatewayCostsView({
  summary,
  pending,
  draftFilters,
  downstreamKeys,
  channels,
  models,
  groups,
  analytics,
  analyticsLoading,
  timeZone,
  quickRange,
  onApplyRange,
  onApplyFilters,
  onResetFilters,
  onUpdateFilter,
}: {
  summary: GatewayRequestLogSummary | null | undefined
  pending: boolean
  draftFilters: UsageFilterForm
  downstreamKeys: GatewayDownstreamKey[]
  channels: Array<{ id: number; name: string }>
  models: string[]
  groups: string[]
  analytics: GatewayRequestLogAnalytics | null | undefined
  analyticsLoading: boolean
  timeZone: string
  quickRange: CostQuickRange | null
  onApplyRange: (range: CostQuickRange | null, start: string, end: string) => void
  onApplyFilters: () => void
  onResetFilters: () => void
  onUpdateFilter: (key: keyof UsageFilterForm, value: string) => void
}) {
  const [rangePickerOpen, setRangePickerOpen] = useState(false)
  const [pendingRange, setPendingRange] = useState<CostQuickRange | null>(quickRange)
  const [rangeStart, setRangeStart] = useState(draftFilters.start)
  const [rangeEnd, setRangeEnd] = useState(draftFilters.end)

  useEffect(() => {
    if (!rangePickerOpen) return
    setPendingRange(quickRange)
    setRangeStart(draftFilters.start)
    setRangeEnd(draftFilters.end)
  }, [draftFilters.end, draftFilters.start, quickRange, rangePickerOpen])

  function chooseRange(range: CostQuickRange) {
    const dates = costQuickRangeDates(range, timeZone)
    setPendingRange(range)
    setRangeStart(dates.start)
    setRangeEnd(dates.end)
  }

  function applyRange() {
    if (!rangeStart || !rangeEnd) return
    onApplyRange(pendingRange, rangeStart, rangeEnd)
    setRangePickerOpen(false)
  }

  return (
    <section className="space-y-4">
      <section className="overflow-hidden rounded-lg border border-border bg-card shadow-sm" title="费用统计的时间、下游 Key、渠道和模型范围">
        <div className="flex min-h-12 flex-col gap-2 border-b border-border bg-muted/20 px-4 py-2 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-center gap-2">
            <span className="metric-surface-brand flex size-7 shrink-0 items-center justify-center rounded-md text-brand">
              <SlidersHorizontal className="size-3.5" />
            </span>
            <h2 className="text-lg font-semibold text-foreground">{"筛选范围"}</h2>
          </div>
          <div className="flex items-center" aria-label="时间范围">
            <Popover open={rangePickerOpen} onOpenChange={setRangePickerOpen}>
              <PopoverTrigger asChild>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  className={cn("h-8 max-w-full gap-2 px-2.5 text-xs", rangePickerOpen && "border-brand/45 bg-brand/5")}
                  title="选择统计时间范围"
                >
                  <CalendarDays className="size-3.5" />
                  <span className="max-w-56 truncate">{costRangeLabel(quickRange, draftFilters.start, draftFilters.end)}</span>
                  <ChevronDown className={cn("size-3.5 text-muted-foreground transition-transform", rangePickerOpen && "rotate-180")} />
                </Button>
              </PopoverTrigger>
              <PopoverContent align="end" className="w-[34rem] max-w-[calc(100vw-2rem)] overflow-hidden p-0">
                <div className="grid grid-cols-2 gap-1 p-2">
                  {costQuickRangeOptions.map((option) => (
                    <Button
                      key={option.value}
                      type="button"
                      size="sm"
                      variant={pendingRange === option.value ? "secondary" : "ghost"}
                      className={cn("h-8 justify-start px-3 text-xs", pendingRange === option.value && "bg-brand/10 text-brand hover:bg-brand/15")}
                      onClick={() => chooseRange(option.value)}
                      disabled={pending || analyticsLoading}
                    >
                      {option.label}
                    </Button>
                  ))}
                </div>
                <div className="border-t border-border p-3">
                  <div className="grid items-end gap-3 sm:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)]">
                    <label className="min-w-0 space-y-1.5">
                      <span className="text-xs font-medium text-muted-foreground">{"开始时间"}</span>
                      <Input className="h-9 min-w-0 text-xs" type="datetime-local" value={rangeStart} onChange={(event) => { setPendingRange(null); setRangeStart(event.target.value) }} aria-label="开始时间" />
                    </label>
                    <ArrowRight className="mb-2 hidden size-4 text-muted-foreground sm:block" />
                    <label className="min-w-0 space-y-1.5">
                      <span className="text-xs font-medium text-muted-foreground">{"结束时间"}</span>
                      <Input className="h-9 min-w-0 text-xs" type="datetime-local" value={rangeEnd} onChange={(event) => { setPendingRange(null); setRangeEnd(event.target.value) }} aria-label="结束时间" />
                    </label>
                  </div>
                </div>
                <div className="flex justify-end border-t border-border bg-muted/20 p-3">
                  <Button type="button" size="sm" onClick={applyRange} disabled={pending || analyticsLoading || !rangeStart || !rangeEnd}>
                    <Search className="size-3.5" />
                    {"应用"}
                  </Button>
                </div>
              </PopoverContent>
            </Popover>
          </div>
        </div>
        <div className="p-4">
          <div className="grid min-w-0 gap-3 md:grid-cols-3 xl:grid-cols-[repeat(4,minmax(0,1fr))_auto]">
            <label className="min-w-0 space-y-1.5">
              <span className="text-xs font-medium text-muted-foreground">{"下游 API Key"}</span>
              <Select value={draftFilters.downstreamKeyID} onValueChange={(value) => onUpdateFilter("downstreamKeyID", value)}>
                <SelectTrigger className="h-9 w-full min-w-0"><SelectValue placeholder="全部" /></SelectTrigger>
                <SelectContent className="max-h-72">
                  <SelectItem value="__all__">{"全部"}</SelectItem>
                  {downstreamKeys.map((key) => <SelectItem key={key.id} value={String(key.id)}>{key.name}</SelectItem>)}
                </SelectContent>
              </Select>
            </label>
            <label className="min-w-0 space-y-1.5">
              <span className="text-xs font-medium text-muted-foreground">{"上游渠道"}</span>
              <Select value={draftFilters.channelID} onValueChange={(value) => onUpdateFilter("channelID", value)}>
                <SelectTrigger className="h-9 w-full min-w-0"><SelectValue placeholder="全部" /></SelectTrigger>
                <SelectContent className="max-h-72">
                  <SelectItem value="__all__">{"全部"}</SelectItem>
                  {channels.map((channel) => <SelectItem key={channel.id} value={String(channel.id)}>{channel.name}</SelectItem>)}
                </SelectContent>
              </Select>
            </label>
            <label className="min-w-0 space-y-1.5">
              <span className="text-xs font-medium text-muted-foreground">{"模型"}</span>
              <Select value={draftFilters.modelName} onValueChange={(value) => onUpdateFilter("modelName", value)}>
                <SelectTrigger className="h-9 w-full min-w-0"><SelectValue placeholder="全部" /></SelectTrigger>
                <SelectContent className="max-h-72">
                  <SelectItem value="__all__">{"全部"}</SelectItem>
                  {models.map((model) => <SelectItem key={model} value={model}>{model}</SelectItem>)}
                  {models.length === 0 ? <SelectItem value="__no_models__" disabled>{"暂无可选模型"}</SelectItem> : null}
                </SelectContent>
              </Select>
            </label>
            <label className="min-w-0 space-y-1.5">
              <span className="text-xs font-medium text-muted-foreground">{"上游分组"}</span>
              <Select value={draftFilters.groupName} onValueChange={(value) => onUpdateFilter("groupName", value)}>
                <SelectTrigger className="h-9 w-full min-w-0"><SelectValue placeholder="全部" /></SelectTrigger>
                <SelectContent className="max-h-72">
                  <SelectItem value="__all__">{"全部"}</SelectItem>
                  <SelectItem value="__unassigned__">{"未分组"}</SelectItem>
                  {groups.filter((group) => group.trim() !== "").map((group) => <SelectItem key={group} value={group}>{group}</SelectItem>)}
                </SelectContent>
              </Select>
            </label>
            <div className="flex items-end justify-end gap-2">
              <Button type="button" size="sm" className="h-9" onClick={onApplyFilters} disabled={pending || analyticsLoading}>
                <Search className="size-3.5" />
                {"查询"}
              </Button>
              <Button type="button" size="icon-sm" className="size-9" variant="outline" onClick={() => { setRangePickerOpen(false); onResetFilters() }} disabled={pending || analyticsLoading} aria-label="重置筛选" title="重置筛选">
                <RotateCcw className="size-3.5" />
              </Button>
            </div>
          </div>
        </div>
      </section>

      <GatewayCostStatsCards summary={summary} />
      <GatewayUsageAnalyticsPanel analytics={analytics} loading={analyticsLoading} timeZone={timeZone} />
    </section>
  )
}

function GatewayCostStatsCards({ summary }: { summary: GatewayRequestLogSummary | null | undefined }) {
  const estimatedCost = summary?.estimated_cost ?? 0
  const reportedCost = summary?.reported_cost ?? 0
  const reconciledCost = summary?.reconciled_cost ?? 0
  const completed = (summary?.success ?? 0) + (summary?.failed ?? 0)
  return (
    <div className="metric-strip grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
      <GatewayCostStatCard tone="brand" label="总费用" value={formatCost(summary?.total_cost)} hint={`本地估算 ${formatCost(estimatedCost)} · 上游已返回 ${formatCost(reportedCost)} · 已核对 ${formatCost(reconciledCost)}`} />
      <GatewayCostStatCard tone={estimatedCost > 0 ? "warning" : "muted"} label="本地估算" value={formatCost(estimatedCost)} hint={estimatedCost > 0 ? "等待上游返回用量" : "当前无本地估算"} />
      <GatewayCostStatCard tone={reportedCost > 0 ? "warning" : "muted"} label="上游已返回" value={formatCost(reportedCost)} hint={reportedCost > 0 ? "等待与上游用量记录核对" : "当前无待核对返回"} />
      <GatewayCostStatCard tone={reconciledCost > 0 ? "success" : "muted"} label="已核对" value={formatCost(reconciledCost)} hint={reconciledCost > 0 ? "已匹配上游用量记录" : "当前无已核对费用"} />
      <div className="flex flex-wrap items-center gap-x-6 gap-y-2 px-1 text-sm text-muted-foreground sm:col-span-2 lg:col-span-4">
        <span title={`输入 ${formatTokenMetric(summary?.prompt_tokens)} · 输出 ${formatTokenMetric(summary?.completion_tokens)}`}>Token <strong className="ml-2 font-medium text-foreground">{formatTokenMetric(summary?.total_tokens)}</strong></span>
        <span title={`${formatInteger(summary?.success)} 成功 · ${formatInteger(summary?.failed)} 失败`}>成功率 <strong className={cn("ml-2 font-medium", completed === 0 ? "text-foreground" : successRate(summary) >= 0.98 ? "text-success" : "text-warning")}>{formatPercent(successRate(summary))}</strong></span>
        <span>平均耗时 <strong className="ml-2 font-medium text-foreground">{formatLatency(summary?.avg_latency_ms)}</strong></span>
        <span>{formatInteger(summary?.total_requests)} 次请求</span>
      </div>
    </div>
  )
}

function GatewayCostStatCard({
  label,
  value,
  hint,
  tone,
}: {
  label: string
  value: string
  hint: string
  tone: "brand" | "success" | "warning" | "muted"
}) {
  const Icon = tone === "success" ? CircleCheck : tone === "warning" ? TriangleAlert : tone === "muted" ? Timer : Activity
  return <StatusMetricCard icon={Icon} label={label} value={value} hint={hint} tone={tone} valueClassName="text-3xl" />
}

function successRate(summary: GatewayRequestLogSummary | null | undefined) {
  const completed = (summary?.success ?? 0) + (summary?.failed ?? 0)
  if (completed <= 0) return 0
  return (summary?.success ?? 0) / completed
}

function GatewaySectionHeader({
  title,
  description,
  badge,
  badgeLabel,
  secondaryBadge,
  actions,
}: {
  title: string
  description?: string
  badge?: number | string
  badgeLabel?: string
  secondaryBadge?: string
  actions?: ReactNode
}) {
  return (
    <div className="flex flex-col gap-3 border-b border-border bg-muted/20 px-4 py-4 lg:flex-row lg:items-start lg:justify-between">
      <div className="min-w-0" title={description || undefined}>
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-lg font-semibold tracking-tight text-foreground">{title}</h2>
          {badge != null ? <Badge variant="secondary">{`${badge}${badgeLabel ?? ""}`}</Badge> : null}
          {secondaryBadge ? <Badge variant="outline">{secondaryBadge}</Badge> : null}
        </div>
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
    </div>
  )
}

function GatewayConfigField({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="space-y-1.5">
      <span className="text-xs font-medium text-muted-foreground">{label}</span>
      {children}
    </label>
  )
}

const GatewayUsageAnalyticsPanel = memo(function GatewayUsageAnalyticsPanel({ analytics, loading, timeZone }: { analytics: GatewayRequestLogAnalytics | null | undefined; loading: boolean; timeZone: string }) {
  const trend = Array.isArray(analytics?.trend) ? analytics.trend : []
  const channelRows = Array.isArray(analytics?.channels) ? analytics.channels : []
  const modelRows = Array.isArray(analytics?.models) ? analytics.models : []
  const keyRows = Array.isArray(analytics?.keys) ? analytics.keys : []
  const costSourceRows = Array.isArray(analytics?.cost_sources) ? analytics.cost_sources : []
  const routeReasonRows = Array.isArray(analytics?.route_reasons) ? analytics.route_reasons : []
  return (
    <div className="grid gap-4 xl:grid-cols-12">
      <GatewayCostPanel className="xl:col-span-8" title="费用走势" description="本地估算、上游已返回和已核对费用按当前时间范围聚合">
        <div className="h-72">
          {loading ? (
            <UsageChartEmpty text="加载统计中…" />
          ) : trend.length === 0 ? (
            <UsageChartEmpty text="暂无趋势数据" />
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={trend.map((point) => ({ ...point, day: formatChartDay(point.bucket, timeZone) }))} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                <XAxis dataKey="day" tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 11 }} />
                <YAxis yAxisId="cost" tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 11 }} tickFormatter={formatCompactCost} />
                <YAxis yAxisId="requests" orientation="right" tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 11 }} width={42} />
                <Tooltip content={<UsageTrendTooltip />} cursor={{ stroke: "var(--border)", strokeDasharray: "4 4" }} />
                <Line yAxisId="cost" type="monotone" dataKey="total_cost" name="总费用" stroke="var(--chart-1)" strokeWidth={2.5} dot={false} isAnimationActive={false} />
                <Line yAxisId="cost" type="monotone" dataKey="estimated_cost" name="本地估算" stroke="var(--warning)" strokeWidth={1.75} strokeDasharray="4 3" dot={false} isAnimationActive={false} />
                <Line yAxisId="cost" type="monotone" dataKey="reported_cost" name="上游已返回" stroke="var(--chart-2)" strokeWidth={1.75} dot={false} isAnimationActive={false} />
                <Line yAxisId="cost" type="monotone" dataKey="reconciled_cost" name="已核对" stroke="var(--success)" strokeWidth={1.75} dot={false} isAnimationActive={false} />
                <Line yAxisId="requests" type="monotone" dataKey="requests" name="请求" stroke="var(--muted-foreground)" strokeWidth={1.5} strokeDasharray="3 3" dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
      </GatewayCostPanel>
      <GatewayCostPanel className="xl:col-span-4" title="渠道费用排行" description="按当前筛选范围统计前 8 个渠道">
        <div className="h-72">
          {loading ? (
            <UsageChartEmpty text="加载统计中…" />
          ) : channelRows.length === 0 ? (
            <UsageChartEmpty text="暂无渠道数据" />
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={channelRows.map((item) => ({ ...item, short_name: compactLabel(item.name, 12) }))} layout="vertical" margin={{ top: 4, right: 12, left: 18, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
                <XAxis type="number" tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 11 }} tickFormatter={formatCompactCost} />
                <YAxis type="category" dataKey="short_name" tickLine={false} axisLine={false} tick={{ fill: "var(--muted-foreground)", fontSize: 11 }} width={72} />
                <Tooltip content={<UsageBreakdownTooltip />} cursor={{ fill: "var(--muted)" }} />
                <Bar dataKey="total_cost" name="费用" fill="var(--chart-1)" radius={[0, 4, 4, 0]} maxBarSize={28} isAnimationActive={false} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </GatewayCostPanel>
      <div className="grid min-w-0 gap-4 lg:grid-cols-2 xl:col-span-12 min-[112.5rem]:grid-cols-4">
        <UsageBreakdownList title="模型费用排行" description="实际调用模型的费用、Token、缓存和核对情况" rows={modelRows} loading={loading} emptyText="暂无模型数据" />
        <UsageBreakdownList title="下游 Key 成本排行" description="按下游 Key 聚合费用，便于定位成本来源" rows={keyRows} loading={loading} emptyText="暂无 Key 数据" />
        <UsageBreakdownList title="费用来源" description="区分本地估算、上游已返回与已核对费用" rows={costSourceRows} loading={loading} emptyText="暂无费用来源数据" labelFormatter={costSourceLabel} />
        <UsageBreakdownList title="路由结果" description="查看智能首选、会话亲和与故障回退的占比" rows={routeReasonRows} loading={loading} emptyText="暂无路由数据" labelFormatter={routeReasonLabel} />
      </div>
    </div>
  )
})

function GatewayCostPanel({
  title,
  description,
  className,
  children,
}: {
  title: string
  description: string
  className?: string
  children: ReactNode
}) {
  return (
    <section className={cn("min-w-0 overflow-hidden rounded-lg border border-border bg-card shadow-sm", className)} title={description}>
      <div className="flex h-12 items-center border-b border-border bg-muted/20 px-4">
        <h2 className="text-lg font-semibold text-foreground">{title}</h2>
      </div>
      <div className="p-4">{children}</div>
    </section>
  )
}

function UsageBreakdownList({
  title,
  description,
  rows,
  loading,
  emptyText,
  labelFormatter,
}: {
  title: string
  description: string
  rows: Array<{
    name: string
    requests: number
    total_cost: number
    total_tokens: number
    reported_cost: number
    reconciled_cost: number
    cache_read_tokens: number
    prompt_tokens: number
    avg_latency_ms: number
  }>
  loading: boolean
  emptyText: string
  labelFormatter?: (value: string) => string
}) {
  const maxCost = Math.max(...rows.map((item) => item.total_cost), 0)
  return (
    <GatewayCostPanel title={title} description={description}>
      <div className="h-64 divide-y divide-border overflow-y-auto overscroll-contain rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring" role="region" aria-label={`${title}明细`} tabIndex={0}>
        {loading ? (
          <UsageChartEmpty text="加载统计中…" />
        ) : rows.length === 0 ? (
          <UsageChartEmpty text={emptyText} />
        ) : rows.map((item) => {
          const label = labelFormatter ? labelFormatter(item.name) : item.name
          const width = maxCost > 0 ? Math.max(4, Math.round((item.total_cost / maxCost) * 100)) : 0
          return (
            <div key={`${item.name}-${item.requests}-${item.total_cost}`} className="py-3 first:pt-0 last:pb-0">
              <div className="flex min-w-0 items-baseline justify-between gap-3">
                <div className="min-w-0 truncate text-sm font-medium" title={label}>{label}</div>
                <div className="shrink-0 text-right text-sm font-semibold tabular-nums text-emerald-600 dark:text-emerald-400">{formatCost(item.total_cost)}</div>
              </div>
              <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-muted-foreground">
                <span>{`请求 ${formatInteger(item.requests)}`}</span>
                <span>{`Token ${formatTokenMetric(item.total_tokens)}`}</span>
                <span>{`平均 ${formatLatency(item.avg_latency_ms)}`}</span>
                <span>{`缓存 ${formatCacheHitRate(item)}`}</span>
              </div>
              <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-muted">
                <div className="h-full rounded-full bg-brand" style={{ width: `${width}%` }} />
              </div>
              <div className="mt-2 flex flex-wrap justify-between gap-x-3 gap-y-1 text-[11px] tabular-nums text-muted-foreground">
                <span>{`上游已返回 ${formatCost(item.reported_cost)}`}</span>
                <span>{`已核对 ${formatCost(item.reconciled_cost)}`}</span>
              </div>
            </div>
          )
        })}
      </div>
    </GatewayCostPanel>
  )
}

function UsageChartEmpty({ text }: { text: string }) {
  return <div className="flex h-full items-center justify-center text-xs text-muted-foreground">{text}</div>
}

function UsageTrendTooltip({ active, payload, label }: { active?: boolean; payload?: Array<{ dataKey?: string; value?: number; payload?: GatewayRequestLogTrendPoint }>; label?: string }) {
  if (!active || !payload?.length) return null
  const point = payload[0]?.payload
  return (
    <div className="rounded-lg border border-border bg-popover px-3 py-2 shadow-md">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 grid grid-cols-[auto_auto] gap-x-4 gap-y-1 text-xs">
        <span className="text-muted-foreground">{"总费用"}</span><span className="text-right font-mono text-brand">{formatCost(point?.total_cost)}</span>
        <span className="text-muted-foreground">{"本地估算"}</span><span className="text-right font-mono text-warning">{formatCost(point?.estimated_cost)}</span>
        <span className="text-muted-foreground">{"上游已返回"}</span><span className="text-right font-mono text-primary">{formatCost(point?.reported_cost)}</span>
        <span className="text-muted-foreground">{"已核对"}</span><span className="text-right font-mono text-success">{formatCost(point?.reconciled_cost)}</span>
        <span className="text-muted-foreground">{"请求"}</span><span className="text-right font-mono text-foreground">{formatInteger(point?.requests)}</span>
      </div>
    </div>
  )
}

function UsageBreakdownTooltip({ active, payload }: { active?: boolean; payload?: Array<{ value?: number; payload?: GatewayRequestLogBreakdownItem & { short_name?: string } }> }) {
  if (!active || !payload?.length) return null
  const row = payload[0]?.payload
  return (
    <div className="rounded-lg border border-border bg-popover px-3 py-2 shadow-md">
      <div className="max-w-56 truncate text-xs text-muted-foreground" title={row?.name}>{row?.name}</div>
      <div className="mt-1 text-sm font-semibold text-emerald-600 dark:text-emerald-400">{formatCost(row?.total_cost)}</div>
      <div className="mt-1 grid grid-cols-[auto_auto] gap-x-4 gap-y-1 text-xs text-muted-foreground">
        <span>{"本地估算"}</span><span className="text-right font-mono text-warning">{formatCost(row?.estimated_cost)}</span>
        <span>{"上游已返回"}</span><span className="text-right font-mono text-primary">{formatCost(row?.reported_cost)}</span>
        <span>{"已核对"}</span><span className="text-right font-mono text-success">{formatCost(row?.reconciled_cost)}</span>
        <span>{"请求 / 平均耗时"}</span><span className="text-right font-mono text-foreground">{`${formatInteger(row?.requests)} / ${formatLatency(row?.avg_latency_ms)}`}</span>
      </div>
    </div>
  )
}

function fallbackCopy(value: string) {
  const textarea = document.createElement("textarea")
  const previousFocus = document.activeElement
  textarea.value = value
  textarea.setAttribute("readonly", "")
  textarea.style.position = "fixed"
  textarea.style.opacity = "0"
  const dialogs = document.querySelectorAll('[data-slot="dialog-content"][data-state="open"]')
  const container = dialogs[dialogs.length - 1] ?? document.body
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

function formatInteger(value: number | null | undefined) {
  return compactCount(value ?? 0)
}

function formatCost(value: number | null | undefined) {
  return `$${(value ?? 0).toFixed(6)}`
}

function formatCompactCost(value: number | string | null | undefined) {
  const n = Number(value ?? 0)
  if (!Number.isFinite(n) || n <= 0) return "$0"
  if (n >= 1000) return `$${(n / 1000).toFixed(1)}K`
  if (n >= 1) return `$${n.toFixed(1)}`
  return `$${n.toFixed(4)}`
}

function formatRatio(value: number | null | undefined) {
  return Number(value ?? 0).toLocaleString(undefined, {
    maximumFractionDigits: 4,
  })
}

function formatLatency(value: number | null | undefined) {
  const ms = value ?? 0
  return `${(ms / 1000).toFixed(2)} s`
}

function formatPriorityScore(value: number | null | undefined) {
  return Math.min(100, Math.max(0, value ?? 0)).toFixed(1)
}

function costSourceLabel(source: string) {
  switch (source) {
    case "sub2api_actual_cost":
      return "Sub2API 实际"
    case "newapi_quota":
      return "NewAPI 实际"
    case "group_ratio_default_quota":
      return "本地估算"
    case "未提供费用来源":
      return "未提供费用来源"
    default:
      return source
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
    case "api_test":
    case "key_connectivity_test":
      return "API 测试"
    case "model_check":
      return "鹈鹕检测"
    default:
      return "智能首选"
  }
}

function pageNumbers(currentPage: number, totalPages: number) {
  const first = Math.max(1, currentPage - 3)
  const last = Math.min(totalPages, currentPage + 3)
  return Array.from({ length: last - first + 1 }, (_, index) => first + index)
}

function formatPercent(value: number) {
  return `${(value * 100).toFixed(1)}%`
}

function formatTokenMetric(value: number | null | undefined) {
  return compactTokenCount(value ?? 0)
}

function cacheHitRate(summary: { cache_read_tokens?: number; prompt_tokens?: number } | null | undefined) {
  const read = summary?.cache_read_tokens ?? 0
  const prompt = summary?.prompt_tokens ?? 0
  if (prompt <= 0) return null
  return read / prompt
}

function formatCacheHitRate(summary: { cache_read_tokens?: number; prompt_tokens?: number } | null | undefined) {
  const rate = cacheHitRate(summary)
  return rate == null ? "—" : formatPercent(rate)
}

function contentDispositionFilename(header: string) {
  const match = /filename="?([^"]+)"?/i.exec(header)
  return match?.[1] ?? ""
}

function formatChartDay(value: string, timeZone: string) {
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) return value.slice(5).replace("-", "/")
  return /[T ]\d{2}:\d{2}/.test(value) ? compactDateTime(value, timeZone) : value
}

function compactLabel(value: string, max = 12) {
  const text = value || "-"
  if (text.length <= max) return text
  return `${text.slice(0, max - 1)}…`
}

function routeCandidateGroupName(candidate: GatewayRouteCandidate) {
  return String(candidate.group_name ?? "")
}

function routeCandidateChannelName(candidate: GatewayRouteCandidate) {
  return String(candidate.channel_name ?? "")
}

function routeCandidateModelIDs(candidate: GatewayRouteCandidate) {
  return Array.isArray(candidate.model_ids) ? candidate.model_ids : []
}

function routeCandidateEffectivePrice(candidate: GatewayRouteCandidate) {
  const value = Number(candidate.effective_price ?? 0)
  return Number.isFinite(value) ? value : 0
}

function routeCandidateRateSnapshotMissing(candidate: GatewayRouteCandidate) {
  return candidateAvailabilityReasons(candidate).includes("no_rate_snapshot")
}

type ProbeChannelGroup = {
  channelID: number
  channelName: string
  channelType: string
  routingAvailable: boolean
  availabilityReasons: string[]
  lastError: string
  candidates: GatewayRouteCandidate[]
}

function probeConfigCandidates(channels: GatewayProbeConfigChannel[]) {
  const candidates: GatewayRouteCandidate[] = []
  for (const channel of channels) {
    for (const group of channel.groups ?? []) {
      const apps = group.gateway_apps?.length > 0 ? group.gateway_apps : [channel.gateway_app || "codex"]
      const routingBlockers = Array.from(new Set([
        ...(channel.availability_reasons ?? []),
        ...(group.probe_blockers ?? []),
      ].filter((reason) => reason !== "probe_disabled")))
      for (const app of apps) {
        candidates.push({
          upstream_group_id: group.upstream_group_id,
          presence_generation: group.presence_generation,
          channel_identity_revision: group.channel_identity_revision,
          channel_id: channel.channel_id,
          channel_name: channel.channel_name,
          channel_type: channel.channel_type,
          gateway_app: app,
          credential_mode: channel.credential_mode,
          group_name: group.group_name,
          ratio: group.ratio,
          completion_ratio: group.completion_ratio,
          raw_ratio: group.raw_ratio,
          raw_completion_ratio: group.raw_completion_ratio,
          funding_factor: group.funding_factor,
          cost_basis_version: group.cost_basis_version,
          effective_price: group.effective_price,
          cache_ratio: 0,
          cache_ratio_known: false,
          model_families: group.model_families ?? [],
          model_mapping_families: group.model_mapping_families ?? [],
          model_ids: group.model_ids ?? [],
          latency_probe_model: group.latency_probe_model ?? "",
          models_synced_at: group.models_synced_at,
          models_error: group.models_error,
          avg_latency_ms: group.avg_latency_ms,
          score: 0,
          price_score: 0,
          latency_score: 0,
          latency_samples: group.latency_samples,
          latency_raw_samples: group.latency_raw_samples ?? [],
          probe_estimated_cost_usd: group.probe_estimated_cost_usd,
          disabled_for_key: false,
          cooling_down: group.cooling_down,
          failure_count: group.failure_count,
          cooldown_until: group.cooldown_until,
          cooldown_remaining: group.cooldown_remaining,
          routing_available: routingBlockers.length === 0 && !group.cooling_down,
          availability_reasons: routingBlockers,
          probe_eligible: group.probe_eligible,
          probe_blockers: group.probe_blockers ?? [],
          latency_qualified: group.latency_qualified,
          latency_source: group.latency_source,
          upstream_key_configured: group.upstream_key_configured,
          upstream_key_enabled: group.upstream_key_enabled,
          upstream_key_provisioning: group.upstream_key_provisioning,
          upstream_key_available: group.upstream_key_available,
          upstream_key_last_error: group.last_error,
          last_routing_failure_error: group.last_routing_failure_error,
          last_routing_failure_kind: group.last_routing_failure_kind,
          last_routing_failure_status: group.last_routing_failure_status,
          last_routing_failure_at: group.last_routing_failure_at,
        })
      }
    }
  }
  return candidates
}

function groupProbeCandidates(candidates: GatewayRouteCandidate[], channels: GatewayProbeConfigChannel[]): ProbeChannelGroup[] {
  const groups = new Map<number, ProbeChannelGroup>()
  for (const channel of channels) {
    groups.set(channel.channel_id, {
      channelID: channel.channel_id,
      channelName: channel.channel_name,
      channelType: channel.channel_type,
      routingAvailable: channel.routing_available,
      availabilityReasons: channel.availability_reasons ?? [],
      lastError: channel.last_error ?? "",
      candidates: [],
    })
  }
  for (const candidate of candidates) {
    const channelID = Number(candidate.channel_id ?? 0)
    if (!channelID) continue
    const group = groups.get(channelID)
    if (!group) continue
    group.routingAvailable = group.routingAvailable || candidate.routing_available !== false
    group.availabilityReasons = Array.from(new Set([
      ...group.availabilityReasons,
      ...(candidate.availability_reasons ?? []).filter((reason) => reason !== "probe_disabled"),
    ]))
    if (!group.channelName) group.channelName = routeCandidateChannelName(candidate)
    if (!group.candidates.some((item) => probeCandidateKey(item) === probeCandidateKey(candidate))) {
      group.candidates.push(candidate)
    }
  }
  return Array.from(groups.values()).sort((a, b) => a.channelName.localeCompare(b.channelName) || a.channelID - b.channelID)
}

type ProbeGroupRow = {
  key: string
  groupName: string
  candidates: GatewayRouteCandidate[]
}

function groupProbeRows(candidates: GatewayRouteCandidate[]): ProbeGroupRow[] {
  const rows = new Map<string, ProbeGroupRow>()
  for (const candidate of candidates) {
    const key = routeCandidateKey(candidate)
    const row = rows.get(key) ?? {
      key,
      groupName: routeCandidateGroupName(candidate),
      candidates: [],
    }
    if (!row.candidates.some((item) => probeCandidateKey(item) === probeCandidateKey(candidate))) {
      row.candidates.push(candidate)
    }
    rows.set(key, row)
  }
  return Array.from(rows.values()).sort((a, b) => a.groupName.localeCompare(b.groupName))
}

function probeRowModelIDs(row: ProbeGroupRow) {
  return Array.from(new Set(row.candidates.flatMap(routeCandidateModelIDs)
    .map((model) => String(model ?? "").trim())
    .filter(Boolean)))
    .sort((a, b) => a.localeCompare(b))
}

function probeRowFamilies(row: ProbeGroupRow) {
  return Array.from(new Set(row.candidates.flatMap(candidateModelFamilies)))
    .sort((a, b) => a.localeCompare(b))
}

function probeRowApps(row: ProbeGroupRow) {
  return Array.from(new Set(row.candidates.map((candidate) => candidate.gateway_app || "codex"))) as GatewayApp[]
}

function probeRowPriceLabel(row: ProbeGroupRow) {
  const prices = row.candidates
    .map(routeCandidateEffectivePrice)
    .filter((price) => price > 0)
  if (prices.length === 0) return "暂无"
  const minimum = Math.min(...prices)
  const maximum = Math.max(...prices)
  if (Math.abs(maximum - minimum) < 0.000001) return `${formatRatio(minimum)}x`
  return `${formatRatio(minimum)}x – ${formatRatio(maximum)}x`
}

function probeRowLatency(row: ProbeGroupRow) {
  let average = 0
  let samples = 0
  let qualified = false
  let source: GatewayRouteCandidate["latency_source"] = "none"
  let rawSamples: number[] = []
  for (const candidate of row.candidates) {
    const candidateRawSamples = Array.isArray(candidate.latency_raw_samples)
      ? candidate.latency_raw_samples.map(Number).filter((value) => Number.isFinite(value) && value > 0)
      : []
    const candidateSamples = Math.max(0, Number(candidate.latency_samples ?? 0), candidateRawSamples.length)
    const latency = Math.max(0, Number(candidate.avg_latency_ms ?? 0))
    if (candidateSamples < samples || (candidateSamples === samples && qualified && !candidate.latency_qualified)) continue
    average = latency
    samples = candidateSamples
    qualified = Boolean(candidate.latency_qualified)
    source = candidate.latency_source ?? "none"
    rawSamples = candidateRawSamples
  }
  return { average, samples, qualified, source, rawSamples }
}

function probeRowModelsError(row: ProbeGroupRow) {
  return Array.from(new Set(row.candidates
    .map((candidate) => String(candidate.models_error ?? "").trim())
    .filter(Boolean)))
    .join("；")
}

function probeRowUpstreamError(row: ProbeGroupRow) {
  return Array.from(new Set(row.candidates
    .filter((candidate) => candidate.upstream_key_available !== true)
    .map((candidate) => String(candidate.upstream_key_last_error ?? "").trim())
    .filter(Boolean)))
    .join("；")
}

function probeRowRoutingFailure(row: ProbeGroupRow) {
  const candidate = row.candidates
    .filter((item) => item.last_routing_failure_at)
    .sort((a, b) => new Date(b.last_routing_failure_at ?? 0).getTime() - new Date(a.last_routing_failure_at ?? 0).getTime())[0]
  if (!candidate?.last_routing_failure_at) return null
  const failureAt = new Date(candidate.last_routing_failure_at).getTime()
  const cooldownUntil = new Date(candidate.cooldown_until ?? 0).getTime()
  if (!candidate.cooling_down && cooldownUntil <= Date.now() && failureAt < Date.now() - 10 * 60 * 1000) return null
  const status = Number(candidate.last_routing_failure_status ?? 0)
  const kind = String(candidate.last_routing_failure_kind ?? "").trim()
  const detail = String(candidate.last_routing_failure_error ?? "").trim()
  return {
    at: candidate.last_routing_failure_at,
    label: status > 0 ? `最近路由失败 HTTP ${status}` : kind ? `最近路由失败 ${kind}` : "最近路由失败",
    detail,
  }
}

function probeRowModelsSyncedAt(row: ProbeGroupRow) {
  return row.candidates
    .map((candidate) => String(candidate.models_synced_at ?? "").trim())
    .filter(Boolean)
    .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())[0] ?? ""
}

function probeRowIssueLabels(row: ProbeGroupRow, minimumSamples: number) {
  const issues: string[] = []
  const modelIDs = probeRowModelIDs(row)
  if (probeRowModelsError(row)) issues.push(modelIDs.length > 0 ? "模型更新异常" : "模型同步失败")
  if (probeRowUpstreamError(row)) issues.push("上游 Key 异常")
  if (row.candidates.some((candidate) => candidate.cooling_down)) issues.push("路由暂时冷却")
  if (modelIDs.length === 0) issues.push("模型列表为空")
  const latency = probeRowLatency(row)
  if (latency.samples < minimumSamples) issues.push("延迟样本不足")
  const blockers = Array.from(new Set(row.candidates
    .flatMap((candidate) => candidate.probe_blockers ?? [])
    .filter((blocker) => blocker !== "probe_disabled")))
  for (const blocker of blockers) {
    const label = probeAvailabilityLabel(blocker)
    if (!issues.includes(label)) issues.push(label)
  }
  return issues
}

function probeGroupIssueCount(group: ProbeChannelGroup, minimumSamples: number) {
  const rowIssues = groupProbeRows(group.candidates).filter((row) => probeRowIssueLabels(row, minimumSamples).length > 0).length
  const channelIssue = group.availabilityReasons.length > 0 || group.candidates.length === 0
  return rowIssues + (channelIssue ? 1 : 0)
}

function probeGroupAverageLatency(group: ProbeChannelGroup) {
  let weightedLatency = 0
  let samples = 0
  for (const row of groupProbeRows(group.candidates)) {
    const latency = probeRowLatency(row)
    if (!latency.qualified || latency.samples <= 0 || latency.average <= 0) continue
    weightedLatency += latency.average * latency.samples
    samples += latency.samples
  }
  return samples > 0 ? weightedLatency / samples : 0
}

function filterProbeChannelGroups(
  groups: ProbeChannelGroup[],
  search: string,
  issuesOnly: boolean,
  minimumSamples: number,
) {
  const query = search.trim().toLowerCase()
  const filtered: ProbeChannelGroup[] = []
  for (const group of groups) {
    let rows = groupProbeRows(group.candidates)
    const channelIssue = group.availabilityReasons.length > 0 || group.candidates.length === 0
    if (issuesOnly) {
      const issueRows = rows.filter((row) => probeRowIssueLabels(row, minimumSamples).length > 0)
      if (issueRows.length > 0) rows = issueRows
      else if (!channelIssue) continue
    }
    const channelMatches = query !== "" && group.channelName.toLowerCase().includes(query)
    if (query !== "" && !channelMatches) {
      rows = rows.filter((row) => {
        const searchText = [
          row.groupName,
          ...probeRowFamilies(row),
          ...probeRowFamilies(row).map(modelFamilyLabel),
          ...probeRowApps(row).map(gatewayAppShortLabel),
          ...probeRowModelIDs(row),
        ].join(" ").toLowerCase()
        return searchText.includes(query)
      })
    }
    if (query !== "" && rows.length === 0 && !channelMatches) continue
    filtered.push({
      ...group,
      candidates: rows.flatMap((row) => row.candidates),
    })
  }
  return filtered
}

function probeAvailabilityLabel(reason: string) {
  const labels: Record<string, string> = {
    monitor_disabled: "渠道监控已关闭",
    channel_error: "渠道状态异常",
    low_balance: "余额低于阈值",
    no_rate_snapshot: "暂无倍率快照",
    no_routable_group: "暂无可路由分组",
    upstream_key_missing: "网关上游 Key 未创建",
    upstream_key_disabled: "网关上游 Key 已停用",
    upstream_key_unavailable: "网关上游 Key 不可用",
    models_not_synced: "模型能力尚未同步",
    routing_cooldown: "路由暂时冷却",
    no_supported_gateway_app: "没有兼容协议",
  }
  return labels[reason] ?? reason
}

function probeLatencySourceLabel(source: GatewayRouteCandidate["latency_source"]) {
  if (source === "group") return "分组滤波"
  if (source === "channel") return "渠道回退"
  return "无有效来源"
}

function probeChannelSyncBlocked(group: ProbeChannelGroup) {
  return group.availabilityReasons.some((reason) => (
    reason === "monitor_disabled"
    || reason === "channel_error"
    || reason === "low_balance"
  ))
}

function probeCandidateKey(candidate: GatewayRouteCandidate) {
  return `${candidate.gateway_app}:${routeCandidateKey(candidate)}`
}

function uniqueProbeGroupCount(candidates: GatewayRouteCandidate[]) {
  return new Set(candidates.map((candidate) => routeCandidateKey(candidate))).size
}

function candidateModelFamilies(candidate: GatewayRouteCandidate) {
  const families = Array.isArray(candidate.model_families)
    ? candidate.model_families.map((family) => String(family ?? "").trim()).filter(Boolean)
    : []
  if (families.length > 0) return Array.from(new Set(families))
  return [candidate.gateway_app || "other"]
}

function modelFamilyLabel(family: string) {
  const labels: Record<string, string> = {
    codex: "GPT / Codex",
    grok: "Grok",
    deepseek: "DeepSeek",
    claude: "Claude",
    gemini: "Gemini",
    qwen: "Qwen",
    mistral: "Mistral",
    llama: "Llama",
    glm: "GLM",
    minimax: "MiniMax",
    moonshot: "Moonshot",
    kimi: "Kimi",
    yi: "Yi",
    ernie: "文心",
    other: "其他",
  }
  return labels[family.toLowerCase()] ?? family
}

function routeCandidateKey(candidate: GatewayRouteCandidate) {
  return `${candidate.channel_id}:${routeCandidateGroupName(candidate)}`
}

function selectedManualCandidate(key: GatewayDownstreamKey, candidates: GatewayRouteCandidate[]) {
  return candidates.find((candidate) => candidate.channel_id === key.manual_channel_id && routeCandidateGroupName(candidate) === key.manual_group_name)
}

function routeCandidateValue(candidate: GatewayRouteCandidate) {
  return `${candidate.channel_id}:${encodeURIComponent(routeCandidateGroupName(candidate))}`
}

function routeExclusionSavingKey(key: GatewayDownstreamKey, candidate: GatewayRouteCandidate) {
  return `${key.id}:${routeCandidateValue(candidate)}`
}

interface CCSwitchConfigPayload {
  format: "json"
  content: string
}

function buildCCSwitchConfig(apiKey: string, app: GatewayApp, originURL: string, model?: string): CCSwitchConfigPayload | null {
  if (app !== "claude") return null
  const settings = buildClientConfigFiles(apiKey, app, "", originURL, model).find((file) => file.id === "claude-settings")
  return settings ? { format: "json", content: settings.content } : null
}

function gatewayAppShortLabel(app: GatewayApp | string | undefined) {
  if (app === "claude") return "Claude"
  if (app === "gemini") return "Gemini"
  return "OpenAI"
}

function gatewayAppIcon(app: GatewayApp) {
  if (app === "claude") return "claude"
  if (app === "gemini") return "gemini"
  return "openai"
}

function defaultGatewayModel(app: GatewayApp) {
  if (app === "claude") return "claude-sonnet-4-5"
  if (app === "gemini") return "gemini-2.5-pro"
  return "gpt-5.4-mini"
}

function defaultGatewayTestModel(key: GatewayDownstreamKey) {
  if (key.model_family === "grok") return "grok-4"
  if (key.app === "codex") return "gpt-4o-mini"
  return defaultGatewayModel(key.app)
}

function restoreGatewayTestModel(key: GatewayDownstreamKey, models: string[]) {
  try {
    const saved = JSON.parse(window.localStorage.getItem(`${gatewayKeyTestModelStoragePrefix}${key.id}`) || "null")
    // An empty/loading inventory cannot prove a saved model is unavailable.
    // Explicit custom IDs also remain valid inputs for a manual test.
    if (typeof saved?.model === "string" && saved.model.trim() && (saved.custom === true || models.length === 0 || models.includes(saved.model))) {
      return saved.model.trim()
    }
  } catch {
    // Ignore unavailable storage or an invalid saved preference.
  }
  return models.length ? selectGatewayTestModel(key, models) : ""
}

function selectGatewayTestModel(key: GatewayDownstreamKey, previewModels: string[]) {
  const models = Array.from(new Set(previewModels.map((model) => model.trim()).filter(Boolean)))
  const preferred = defaultGatewayTestModel(key)
  const preferredAvailable = models.find((model) => model.toLowerCase() === preferred.toLowerCase())
  if (preferredAvailable) return preferredAvailable
  return models.find((model) => model.trim().toLowerCase() !== "auto-review") || preferred
}
