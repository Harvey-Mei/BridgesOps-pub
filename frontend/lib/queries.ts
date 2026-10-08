"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { apiFetch } from "@/lib/api"
import { ZonedDateTimeError, zonedDateTimeToISOString } from "@/lib/format"
import { useRefreshTick } from "@/lib/refresh-context"
import type {
  AppVersion,
  BalanceTrendPoint,
  CaptchaConfig,
  Channel,
  ChannelCostComparison,
  ChannelPage,
  CostTrendPoint,
  DashboardMultiplierTrend,
  DashboardSummary,
  GatewayDownstreamKey,
  GatewayCandidateGroup,
  GatewayImageOperationCapability,
  GatewayManualMappedTargetPreview,
	GatewayKeyGroup,
	GatewayModelPrice,
	GatewayUsedModel,
	GatewayUpstreamGroupChange,
  GatewayUpstreamGroupChangeReviewSummary,
  GatewayRequestLogAnalytics,
  GatewayRequestLogGroupCacheRatioPage,
  GatewayRequestLogPage,
  GatewayRequestLogFilters,
  GatewayApp,
  GatewayProbeConfigChannel,
  GatewayRouteCandidate,
  ModelCheckOverview,
  ModelCheckRunsPage,
  NetworkProxy,
  NotificationChannel,
  NotificationLogPage,
  RateChangeLogPage,
  RateChangeAnalytics,
  RateSnapshot,
  SystemConfigResponse,
  UpstreamAnnouncementPage,
  CodexRadarSnapshot,
  ClaudeRadarSnapshot,
} from "@/lib/api-types"

export interface QueryState<T> {
  data: T | null
  loading: boolean
  refreshing: boolean
  error: string | null
  resolvedPath: string | null
  refetch: () => void
  setData: (data: T | ((current: T | null) => T)) => void
}

/**
 * In-flight 请求去重：同一个 URL 在同一个 tick 内只发一次，所有 useApi 共享 Promise。
 *
 * 为什么需要：useDashboardSummary() 在 5 个组件里都被调用，没去重的话每次 mount /
 * refresh 都会发 5 个相同请求。开发环境叠加 StrictMode 翻倍后会更夸张。
 */
const inflight = new Map<string, Promise<unknown>>()

/** Cache 已完成的响应一小段时间，便于同一帧内挂载的多个组件共享结果（即使第一次的 Promise 已经 resolve）。 */
interface CacheEntry {
  data: unknown
  expiresAt: number
}
const cache = new Map<string, CacheEntry>()
const CACHE_TTL_MS = 800
const CHANNELS_CACHE_TTL_MS = 30_000
const CACHE_MAX_ENTRIES = 128
const GATEWAY_ROUTE_PREVIEW_CONCURRENCY = 4

function cacheKey(path: string, tick: number, bump: number) {
	return `${path}#${tick}#${bump}`
}

function pruneCache(now: number) {
	for (const [key, entry] of cache) {
		if (entry.expiresAt <= now) cache.delete(key)
	}
	while (cache.size > CACHE_MAX_ENTRIES) {
		const oldest = cache.keys().next().value
		if (oldest === undefined) return
		cache.delete(oldest)
	}
}

function fetchShared<T>(path: string, key: string): Promise<T> {
	const now = Date.now()
	pruneCache(now)

  const cached = cache.get(key)
  if (cached && cached.expiresAt > now) {
    return Promise.resolve(cached.data as T)
  }

  const existing = inflight.get(key) as Promise<T> | undefined
  if (existing) return existing

	const p = apiFetch<T>(path)
		.then((d) => {
			const cachedAt = Date.now()
			cache.set(key, { data: d, expiresAt: cachedAt + apiCacheTTL(path) })
			pruneCache(cachedAt)
      return d
    })
    .finally(() => {
      // 让下一帧（refresh tick++）拉到新的数据，不要永远 hold 住旧 promise
      inflight.delete(key)
    })
  inflight.set(key, p)
  return p
}

export function apiCacheTTL(path: string) {
	return path === "/channels" ? CHANNELS_CACHE_TTL_MS : CACHE_TTL_MS
}

/**
 * useApi 通用数据获取 hook（stale-while-revalidate）。
 * - 首次加载：loading = true，组件显示加载占位
 * - 后续刷新（refresh tick / refetch）：保留旧 data 继续展示，loading 不切回 true，后台静默拉新
 * - 同 URL + 同 tick 的并发调用共享一次请求
 */
function useApi<T>(
  path: string | null,
  watchRefresh = true,
  reconcile?: (current: T, next: T) => T,
  bypassCacheOnRefetch = false,
): QueryState<T> {
  const [data, setDataState] = useState<T | null>(null)
  const [loading, setLoading] = useState<boolean>(path !== null)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [bump, setBump] = useState(0)
  const [manualRefresh, setManualRefresh] = useState(0)
  const refreshTick = useRefreshTick()
  const globalTick = watchRefresh ? refreshTick : 0

  // 已经拿到过数据吗？用 ref 防止 setLoading 写回触发额外 effect。
  const hasDataRef = useRef(false)
  const requestGenerationRef = useRef(0)
  const activeRequestRef = useRef<{ path: string; key: string; generation: number } | null>(null)
  const pendingRefreshRef = useRef(false)
  const consumedManualRefreshRef = useRef(0)
  const resolvedPathRef = useRef<string | null>(null)
  const mountedRef = useRef(true)

  useEffect(() => {
    mountedRef.current = true
    return () => {
      mountedRef.current = false
      requestGenerationRef.current += 1
      activeRequestRef.current = null
      pendingRefreshRef.current = false
    }
  }, [])

  useEffect(() => {
    if (path === null) {
      requestGenerationRef.current += 1
      activeRequestRef.current = null
      pendingRefreshRef.current = false
      consumedManualRefreshRef.current = manualRefresh
      hasDataRef.current = false
      resolvedPathRef.current = null
      setDataState(null)
      setError(null)
      setLoading(false)
      setRefreshing(false)
      return
    }
    const bypassCache = bypassCacheOnRefetch && manualRefresh > consumedManualRefreshRef.current
    const requestPath = bypassCache ? appendQueryParam(path, "refresh", "1") : path
    const key = cacheKey(requestPath, globalTick, bump)
    const active = activeRequestRef.current
    if (active?.path === path && active.key !== key) {
      pendingRefreshRef.current = true
      if (hasDataRef.current) setRefreshing(true)
      return
    }
    if (bypassCache) consumedManualRefreshRef.current = manualRefresh
    const generation = ++requestGenerationRef.current
    activeRequestRef.current = { path, key, generation }
    // 关键：只有第一次（还没拿到过数据）才展示 loading；后续 polling / refetch 静默进行，
    // 避免组件因 loading=true 短暂消失再回来造成"闪屏"。
    if (!hasDataRef.current) setLoading(true)
    else setRefreshing(true)
    setError(null)
    fetchShared<T>(requestPath, key)
      .then((d) => {
        if (!mountedRef.current || generation !== requestGenerationRef.current) return
        hasDataRef.current = true
        const canReconcile = resolvedPathRef.current === path
        setDataState((current) => current != null && canReconcile && reconcile
          ? reconcile(current, d)
          : d)
        resolvedPathRef.current = path
      })
      .catch((e: Error) => {
        if (!mountedRef.current || generation !== requestGenerationRef.current) return
        setError(e.message)
      })
      .finally(() => {
        if (!mountedRef.current || generation !== requestGenerationRef.current) return
        activeRequestRef.current = null
        if (pendingRefreshRef.current) {
          pendingRefreshRef.current = false
          setBump((current) => current + 1)
          return
        }
        setLoading(false)
        setRefreshing(false)
      })
  }, [path, bump, globalTick, reconcile, bypassCacheOnRefetch, manualRefresh])

  return {
    data,
    loading,
    refreshing,
    error,
    resolvedPath: resolvedPathRef.current,
    refetch: () => {
      if (hasDataRef.current) setRefreshing(true)
      if (bypassCacheOnRefetch) setManualRefresh((current) => current + 1)
      setBump((b) => b + 1)
    },
    setData: (nextData) => {
      requestGenerationRef.current += 1
      activeRequestRef.current = null
      pendingRefreshRef.current = false
      hasDataRef.current = true
      resolvedPathRef.current = path
      setDataState((current) => typeof nextData === "function"
        ? (nextData as (value: T | null) => T)(current)
        : nextData)
      setLoading(false)
      setRefreshing(false)
      setError(null)
    },
  }
}

function appendQueryParam(path: string, key: string, value: string) {
  return `${path}${path.includes("?") ? "&" : "?"}${encodeURIComponent(key)}=${encodeURIComponent(value)}`
}

export function useDashboardSummary() {
  return useApi<DashboardSummary>("/dashboard/summary")
}

export function useAppVersion() {
  return useApi<AppVersion>("/version", false)
}

export function useBalanceTrend(days = 7) {
  return useApi<BalanceTrendPoint[]>(`/dashboard/balance-trend?days=${days}`)
}

export function useCostTrend(days = 7) {
  return useApi<CostTrendPoint[]>(`/dashboard/cost-trend?days=${days}`)
}

export function dashboardMultiplierTrendQuery(filters: GatewayRequestLogQueryFilters | undefined, timeZone: string): GatewayRequestLogQueryResult {
  const result = gatewayRequestLogQuery(undefined, undefined, filters, false, timeZone)
  if (result.error) return result
  const source = new URLSearchParams(result.query)
  const query = new URLSearchParams()
  for (const key of ["start", "end"]) {
    const value = source.get(key)
    if (value) query.set(key, value)
  }
  return { query: query.toString(), error: null }
}

export function useDashboardMultiplierTrend(enabled: boolean, filters: GatewayRequestLogQueryFilters | undefined, timeZone: string) {
  const result = dashboardMultiplierTrendQuery(filters, timeZone)
  const state = useApi<DashboardMultiplierTrend>(
    enabled && Boolean(timeZone.trim()) && !result.error && Boolean(result.query)
      ? `/dashboard/multiplier-trend?${result.query}`
      : null,
    true,
    undefined,
    true,
  )
  return withGatewayRequestLogQueryError(state, result.error)
}

export function useModelCheckOverview(enabled = true) {
  return useApi<ModelCheckOverview>(enabled ? "/model-checks/overview" : null, true, undefined, true)
}

export function useModelCheckLatestRuns() {
	// Full SVGs refresh on demand or after a run, not on every global tick.
	return useApi<ModelCheckRunsPage["items"]>("/model-checks/runs", false)
}

export function useModelCheckManualOptions() {
  return useApi<import("@/lib/api-types").ModelCheckManualOption[]>("/model-checks/manual-options", false)
}

export function useModelCheckManualModels(group?: import("@/lib/api-types").ModelCheckManualOption) {
  const path = group ? `/model-checks/manual-models?${new URLSearchParams({ channel_id: String(group.channel_id), group_name: group.group_name, identity_revision: String(group.identity_revision), app: group.app })}` : null
  const state = useApi<import("@/lib/api-types").ModelCheckManualOption[]>(path, false)
  return { ...state, data: state.resolvedPath === path ? state.data : null, loading: !!path && (state.loading || (!state.error && state.resolvedPath !== path)) }
}

export function useModelCheckRuns(targetID: number | null, page = 1, pageSize = 20, enabled = true) {
  return useApi<ModelCheckRunsPage>(
    enabled && targetID != null
      ? `/model-checks/targets/${targetID}/runs?page=${page}&page_size=${pageSize}`
      : null,
    true,
    undefined,
    true,
  )
}

export function useNetworkProxies(enabled = true) {
  return useApi<NetworkProxy[]>(enabled ? "/network-proxies" : null, true, undefined, true)
}

export function useChannels(enabled = true) {
  return useApi<Channel[]>(enabled ? "/channels" : null)
}

export function useChannelCostComparison(enabled = true) {
  return useApi<ChannelCostComparison>(enabled ? "/channels/cost-comparison" : null, true, undefined, true)
}

export function useChannelsPage(page = 1, pageSize = 9) {
  return useApi<ChannelPage>(`/channels?page=${page}&page_size=${pageSize}`)
}

export function useChannelRates(channelID: number | null) {
  return useApi<RateSnapshot[]>(channelID == null ? null : `/channels/${channelID}/rates`)
}

// useMultiChannelRates 把多个上游渠道的倍率分组拉回来合并去重，
// 供订阅规则"多选渠道 + 指定分组"场景使用。复用 fetchShared 缓存，
// 单渠道请求仍与 useChannelRates 共享，不会重复打接口。
export function useMultiChannelRates(channelIDs: number[]) {
  const [data, setData] = useState<RateSnapshot[] | null>(null)
  const [loading, setLoading] = useState(false)
  const [bump, setBump] = useState(0)
  const refreshTick = useRefreshTick()
  const key = channelIDs.slice().sort((a, b) => a - b).join(",")

  useEffect(() => {
    if (channelIDs.length === 0) {
      setData(null)
      setLoading(false)
      return
    }
    let cancelled = false
    setLoading(true)
    Promise.all(
      channelIDs.map((id) =>
        fetchShared<RateSnapshot[]>(
          `/channels/${id}/rates`,
          cacheKey(`/channels/${id}/rates`, refreshTick, bump),
        ),
      ),
    )
      .then((results) => {
        if (cancelled) return
        setData(results.flat())
      })
      .catch(() => {
        if (!cancelled) setData(null)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
    // channelIDs 是数组引用，用排序后的 key 字符串做依赖避免每次渲染都触发
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, refreshTick, bump])

  return { data, loading, refetch: () => setBump((b) => b + 1) }
}

export function useRateChanges(page = 1, pageSize = 20, channelID?: number) {
  const q = new URLSearchParams()
  q.set("page", String(page))
  q.set("page_size", String(pageSize))
  if (channelID != null) q.set("channel_id", String(channelID))
  return useApi<RateChangeLogPage>(`/rate-changes?${q.toString()}`)
}

export function useRateChangeAnalytics(days = 30, channelID?: number) {
  const q = new URLSearchParams({ days: String(days) })
  if (channelID != null) q.set("channel_id", String(channelID))
  return useApi<RateChangeAnalytics>(`/rate-change-analytics?${q.toString()}`)
}

export function useNotificationChannels() {
  return useApi<NotificationChannel[]>("/notifications/channels")
}

export function useNotificationLogs(page = 1, pageSize = 20) {
  return useApi<NotificationLogPage>(
    `/notifications/logs?page=${page}&page_size=${pageSize}`,
  )
}

export function useAnnouncements(page = 1, pageSize = 20) {
  return useApi<UpstreamAnnouncementPage>(
    `/announcements?page=${page}&page_size=${pageSize}`,
  )
}

export function useCaptchaConfigs(enabled = true) {
  return useApi<CaptchaConfig[]>(enabled ? "/captcha-configs" : null)
}

export function useSystemConfig(enabled = true) {
  return useApi<SystemConfigResponse>(enabled ? "/settings/config" : null)
}

export function useCodexRadar(enabled = true) {
  return useApi<CodexRadarSnapshot>(enabled ? "/codex-radar" : null)
}

export function useClaudeRadar(enabled = true) {
  return useApi<ClaudeRadarSnapshot>(enabled ? "/claude-radar" : null)
}

export function useGatewayDownstreamKeys(enabled = true) {
  return useApi<GatewayDownstreamKey[]>(enabled ? "/gateway/downstream-keys" : null)
}

export function useGatewayCandidateGroups(enabled = true) {
  return useApi<GatewayCandidateGroup[]>(enabled ? "/gateway/candidate-groups" : null)
}

export function useGatewayKeyGroups(enabled = true) {
	return useApi<GatewayKeyGroup[]>(enabled ? "/gateway/key-groups" : null)
}

export function useGatewayUpstreamGroupChanges(enabled = true) {
  return useApi<GatewayUpstreamGroupChange[]>(enabled ? "/gateway/upstream-group-changes" : null)
}

export function useGatewayUpstreamGroupChangeReviewSummary(enabled = true) {
  return useApi<GatewayUpstreamGroupChangeReviewSummary>(enabled ? "/gateway/upstream-group-changes/review-summary" : null)
}

export function useGatewayRoutePreview(app: GatewayApp = "codex", enabled = true) {
  return useApi<GatewayRouteCandidate[]>(enabled ? `/gateway/route-preview?app=${app}` : null)
}

export function useGatewayManualRoutePreview(downstreamKeyID: number | null, enabled = true) {
  const path = enabled && downstreamKeyID != null && downstreamKeyID > 0
    ? `/gateway/route-preview?downstream_key_id=${downstreamKeyID}&manual_selection=1`
    : null
  return useApi<GatewayRouteCandidate[]>(path, false, undefined, true)
}

export function useGatewayManualMappedTargetPreview(downstreamKeyID: number | null, enabled = true) {
  const path = enabled && downstreamKeyID != null && downstreamKeyID > 0
    ? `/gateway/manual-mapped-target-preview?downstream_key_id=${downstreamKeyID}`
    : null
  return useApi<GatewayManualMappedTargetPreview>(path, false, undefined, true)
}

export type GatewayCandidatePreviewModelContext = "requested" | "mapped"

// Candidate management must always be model-qualified. Unlike the generic
// route preview, this endpoint returns every persisted member of the key's
// candidate group so unavailable entries can be managed instead of vanishing.
export function useGatewayCandidateGroupPreview(
  downstreamKeyID: number | null,
  model: string,
  context: GatewayCandidatePreviewModelContext,
  enabled = true,
) {
  const selectedModel = model.trim()
  const path = enabled && downstreamKeyID != null && downstreamKeyID > 0 && selectedModel !== ""
    ? `/gateway/route-preview?downstream_key_id=${downstreamKeyID}&model=${encodeURIComponent(selectedModel)}&candidate_group_members=1&model_context=${context}`
    : null
  // Candidate management is an interactive surface. Global polling must not
  // reorder or move rows while the user is operating on them; mutations call
  // refetch explicitly after applying their local result.
  const query = useApi<GatewayRouteCandidate[]>(path, false, undefined, true)
  return {
    ...query,
    queryPath: path,
  }
}

export type GatewayKeyRoutePreviewState = Pick<QueryState<GatewayRouteCandidate[]>, "data" | "loading" | "error"> & {
  mappedTargetData: GatewayManualMappedTargetPreview | null
  mappedTargetLoading: boolean
  mappedTargetError: string | null
}

export function useGatewayKeyRoutePreviews(keys: GatewayDownstreamKey[], enabled = true) {
  const refreshTick = useRefreshTick()
  const [bump, setBump] = useState(0)
  const [previews, setPreviews] = useState<Record<number, GatewayKeyRoutePreviewState>>({})
  const keySignature = enabled ? keys.map((key) => [
    key.id,
    key.app,
    encodeURIComponent(key.model_family ?? ""),
    key.candidate_group_id ?? 0,
    key.smart_routing ? 1 : 0,
    key.manual_channel_id ?? 0,
    encodeURIComponent(key.manual_group_name ?? ""),
    encodeURIComponent(key.model_mapping_strategy ?? ""),
    encodeURIComponent(key.model_mapping_model ?? ""),
    encodeURIComponent(key.model_mapping_reasoning_effort ?? ""),
  ].join(":")).join(",") : ""
  const previewKeys = useMemo(() => keySignature.split(",").filter(Boolean).map((item) => {
    const [id, app, , , smartRouting] = item.split(":")
    return { id: Number(id), app: app as GatewayApp, smartRouting: smartRouting === "1" }
  }), [keySignature])

  useEffect(() => {
    if (!enabled || previewKeys.length === 0) {
      setPreviews((current) => Object.keys(current).length === 0 ? current : {})
      return
    }
    let cancelled = false
    const controller = new AbortController()
    setPreviews((current) => Object.fromEntries(previewKeys.map((key) => [key.id, {
      data: current[key.id]?.data ?? null,
      // Keep an expanded candidate list visible during refresh. Replacing it
      // with a loading row makes a disable action look like the menu closed.
      loading: current[key.id]?.data == null,
      error: null,
      mappedTargetData: key.smartRouting ? null : current[key.id]?.mappedTargetData ?? null,
      mappedTargetLoading: !key.smartRouting,
      mappedTargetError: null,
    }])))
    const entries: Array<[number, GatewayKeyRoutePreviewState]> = new Array(previewKeys.length)
    let nextIndex = 0
    const loadPreview = async (key: { id: number; app: GatewayApp; smartRouting: boolean }): Promise<GatewayKeyRoutePreviewState> => {
      const path = key.smartRouting
        ? `/gateway/route-preview?app=${key.app}&downstream_key_id=${key.id}`
        : `/gateway/route-preview?downstream_key_id=${key.id}&manual_selection=1`
      const routePromise = apiFetch<GatewayRouteCandidate[]>(path, { signal: controller.signal })
        .then((data) => ({ data, error: null as string | null }))
        .catch((error) => ({
          data: null,
          error: error instanceof Error ? error.message : "候选加载失败",
        }))
      const mappedTargetPromise = key.smartRouting
        ? Promise.resolve({ data: null, error: null as string | null })
        : apiFetch<GatewayManualMappedTargetPreview>(`/gateway/manual-mapped-target-preview?downstream_key_id=${key.id}`, { signal: controller.signal })
          .then((data) => ({ data, error: null as string | null }))
          .catch((error) => ({
            data: null,
            error: error instanceof Error ? error.message : "固定目标核对失败",
          }))
      const [route, mappedTarget] = await Promise.all([routePromise, mappedTargetPromise])
      return {
        data: route.data,
        loading: false,
        error: route.error,
        mappedTargetData: mappedTarget.data,
        mappedTargetLoading: false,
        mappedTargetError: mappedTarget.error,
      }
    }
    const worker = async () => {
      while (!cancelled) {
        const index = nextIndex
        nextIndex += 1
        if (index >= previewKeys.length) return
        const key = previewKeys[index]
        entries[index] = [key.id, await loadPreview(key)]
      }
    }
    const workerCount = Math.min(GATEWAY_ROUTE_PREVIEW_CONCURRENCY, previewKeys.length)
    Promise.all(Array.from({ length: workerCount }, () => worker())).then(() => {
      if (!cancelled) setPreviews(Object.fromEntries(entries))
    })
    return () => {
      cancelled = true
      controller.abort()
    }
  }, [enabled, previewKeys, refreshTick, bump])

  return {
    previews,
    refetch: () => setBump((current) => current + 1),
  }
}

export function useGatewayProbeConfig(enabled = true) {
  return useApi<GatewayProbeConfigChannel[]>(enabled ? "/gateway/probe-config" : null)
}

export function useGatewayModelPrices(enabled = true) {
	return useApi<GatewayModelPrice[]>(enabled ? "/gateway/model-prices" : null, true, undefined, true)
}

export function useGatewayUsedModels(enabled = true, days = 30) {
	const normalizedDays = Number.isInteger(days) && days >= 1 && days <= 90 ? days : 30
	return useApi<GatewayUsedModel[]>(enabled ? `/gateway/model-prices/used-models?days=${normalizedDays}` : null, true, undefined, true)
}

export function useGatewayImageOperationCapabilities(enabled = true) {
  return useApi<GatewayImageOperationCapability[]>(enabled ? "/gateway/image-operation-capabilities" : null)
}

export type GatewayRequestLogQueryFilters = {
  start?: string
  end?: string
  upstreamRequestID?: string
  downstreamKeyID?: string | number
  modelName?: string
  channelID?: string | number
  groupName?: string
  groupUnassigned?: boolean
  result?: string
  routeReason?: string
  sortBy?: string
  sortOrder?: string
}

export type GatewayRequestLogQueryResult =
  | { query: string; error: null }
  | { query: ""; error: ZonedDateTimeError }

export function gatewayRequestLogQuery(
  page: number | undefined,
  pageSize: number | undefined,
  filters: GatewayRequestLogQueryFilters | undefined,
  includeModel: boolean,
  timeZone: string,
): GatewayRequestLogQueryResult {
  const q = new URLSearchParams()
  if (page != null) q.set("page", String(page))
  if (pageSize != null) q.set("page_size", String(pageSize))
  const effectiveTimeZone = timeZone.trim()
  let start = ""
  let end = ""
  try {
    start = effectiveTimeZone && filters?.start ? zonedDateTimeToISOString(filters.start, effectiveTimeZone) : ""
    end = effectiveTimeZone && filters?.end ? zonedDateTimeToISOString(filters.end, effectiveTimeZone) : ""
  } catch (error) {
    if (error instanceof ZonedDateTimeError) return { query: "", error }
    throw error
  }
  if (start) q.set("start", start)
  if (end) q.set("end", end)
  if (filters?.upstreamRequestID) q.set("upstream_request_id", filters.upstreamRequestID)
  if (filters?.downstreamKeyID && filters.downstreamKeyID !== "__all__") q.set("downstream_key_id", String(filters.downstreamKeyID))
  if (includeModel && filters?.modelName && filters.modelName !== "__all__") q.set("model", filters.modelName)
  if (filters?.channelID && filters.channelID !== "__all__") q.set("channel_id", String(filters.channelID))
  if (filters?.groupName && filters.groupName !== "__all__" && filters.groupName !== "__unassigned__") q.set("group_name", filters.groupName)
  if (filters?.groupName === "__unassigned__" || filters?.groupUnassigned) q.set("group_unassigned", "1")
  if (filters?.result && filters.result !== "__all__") q.set("result", filters.result)
  if (filters?.routeReason && filters.routeReason !== "__all__") q.set("route_reason", filters.routeReason)
  if (filters?.sortBy) q.set("sort_by", filters.sortBy)
  if (filters?.sortOrder) q.set("sort_order", filters.sortOrder)
  return { query: q.toString(), error: null }
}

export function useGatewayRequestLogFilters(enabled: boolean, filters: GatewayRequestLogQueryFilters | undefined, timeZone: string) {
  const result = gatewayRequestLogQuery(undefined, undefined, filters, false, timeZone)
  const state = useApi<GatewayRequestLogFilters>(
    enabled && Boolean(timeZone.trim()) && !result.error
      ? `/gateway/request-log-filters${result.query ? `?${result.query}` : ""}`
      : null,
  )
  return withGatewayRequestLogQueryError(state, result.error)
}

export function useGatewayRequestLogs(
  page: number,
  pageSize: number,
  enabled: boolean,
  filters: GatewayRequestLogQueryFilters | undefined,
  timeZone: string,
) {
  const result = gatewayRequestLogQuery(page, pageSize, filters, true, timeZone)
  const state = useApi<GatewayRequestLogPage>(
    enabled && Boolean(timeZone.trim()) && !result.error ? `/gateway/request-logs?${result.query}` : null,
    true,
    reconcileGatewayRequestLogPage,
    true,
  )
  return withGatewayRequestLogQueryError(state, result.error)
}

function reconcileGatewayRequestLogPage(current: GatewayRequestLogPage, next: GatewayRequestLogPage): GatewayRequestLogPage {
  const previousByID = new Map(current.items.map((item) => [item.id, item]))
  let itemsChanged = current.items.length !== next.items.length
  const items = next.items.map((item, index) => {
    const previous = previousByID.get(item.id)
    if (previous && shallowEqualRecord(previous, item)) {
      if (current.items[index] !== previous) itemsChanged = true
      return previous
    }
    itemsChanged = true
    return item
  })
  const summary = current.summary && next.summary && shallowEqualRecord(current.summary, next.summary)
    ? current.summary
    : next.summary
  if (!itemsChanged && current.total === next.total && current.page === next.page && current.page_size === next.page_size && current.pages === next.pages && summary === current.summary) {
    return current
  }
  return { ...next, items, summary }
}

function shallowEqualRecord<T extends object>(left: T, right: T) {
  const leftKeys = Object.keys(left) as Array<keyof T>
  const rightKeys = Object.keys(right)
  if (leftKeys.length !== rightKeys.length) return false
  return leftKeys.every((key) => Object.is(left[key], right[key]))
}

export function useGatewayRequestLogAnalytics(enabled: boolean, filters: GatewayRequestLogQueryFilters | undefined, timeZone: string) {
  const result = gatewayRequestLogQuery(undefined, undefined, filters, true, timeZone)
  const state = useApi<GatewayRequestLogAnalytics>(
    enabled && Boolean(timeZone.trim()) && !result.error
      ? `/gateway/request-log-analytics${result.query ? `?${result.query}` : ""}`
      : null,
    true,
    reconcileGatewayRequestLogAnalytics,
    true,
  )
  return withGatewayRequestLogQueryError(state, result.error)
}

export function gatewayRequestLogGroupCachePageQuery(
  cursor: string,
  pageSize: number,
  filters: GatewayRequestLogQueryFilters | undefined,
  timeZone: string,
): GatewayRequestLogQueryResult {
  const result = gatewayRequestLogQuery(undefined, undefined, filters, true, timeZone)
  if (result.error) return result
  const query = new URLSearchParams(result.query)
  query.set("page_size", String(pageSize))
  if (cursor.trim()) query.set("cursor", cursor.trim())
  return { query: query.toString(), error: null }
}

export function useGatewayRequestLogGroupCachePage(
  enabled: boolean,
  cursor: string,
  pageSize: number,
  filters: GatewayRequestLogQueryFilters | undefined,
  timeZone: string,
) {
  const result = gatewayRequestLogGroupCachePageQuery(cursor, pageSize, filters, timeZone)
  const requestPath = enabled && Boolean(timeZone.trim()) && !result.error
    ? `/gateway/request-log-group-cache-ratios?${result.query}`
    : null
  const state = useApi<GatewayRequestLogGroupCacheRatioPage>(
    requestPath,
    true,
    reconcileGatewayRequestLogGroupCachePage,
    true,
  )
  return { ...withGatewayRequestLogQueryError(state, result.error), requestPath }
}

function reconcileGatewayRequestLogGroupCachePage(
  current: GatewayRequestLogGroupCacheRatioPage,
  next: GatewayRequestLogGroupCacheRatioPage,
): GatewayRequestLogGroupCacheRatioPage {
  const items = reconcileRecordArray(
    current.items,
    next.items,
    (item) => `${item.channel_id}:${item.group_name}`,
  )
  if (items === current.items && current.next_cursor === next.next_cursor && current.page_size === next.page_size) return current
  return { ...next, items }
}

function reconcileGatewayRequestLogAnalytics(current: GatewayRequestLogAnalytics, next: GatewayRequestLogAnalytics): GatewayRequestLogAnalytics {
  const trend = reconcileRecordArray(current.trend, next.trend, (item) => item.bucket)
  const modelUnitPrices = reconcileRecordArray(current.model_unit_prices ?? [], next.model_unit_prices ?? [], gatewayRequestLogModelUnitPriceKey)
  const channels = reconcileRecordArray(current.channels, next.channels, breakdownItemKey)
  const models = reconcileRecordArray(current.models, next.models, breakdownItemKey)
  const modelsByTokens = reconcileRecordArray(current.models_by_tokens, next.models_by_tokens, breakdownItemKey)
  const keys = reconcileRecordArray(current.keys, next.keys, breakdownItemKey)
  const costSources = reconcileRecordArray(current.cost_sources, next.cost_sources, breakdownItemKey)
  const routeReasons = reconcileRecordArray(current.route_reasons, next.route_reasons, breakdownItemKey)
  const groupCacheRatios = reconcileRecordArray(current.group_cache_ratios ?? [], next.group_cache_ratios ?? [], (item) => `${item.channel_id}:${item.channel_name}:${item.group_name}`)

  if (
    trend === current.trend
    && modelUnitPrices === current.model_unit_prices
    && channels === current.channels
    && models === current.models
    && modelsByTokens === current.models_by_tokens
    && keys === current.keys
    && costSources === current.cost_sources
    && routeReasons === current.route_reasons
    && groupCacheRatios === current.group_cache_ratios
  ) return current

  return {
    ...next,
    trend,
    model_unit_prices: modelUnitPrices,
    channels,
    models,
    models_by_tokens: modelsByTokens,
    keys,
    cost_sources: costSources,
    route_reasons: routeReasons,
    group_cache_ratios: groupCacheRatios,
  }
}

function breakdownItemKey(item: GatewayRequestLogAnalytics["channels"][number]) {
  return `${item.channel_id ?? ""}:${item.downstream_key_id ?? ""}:${item.name}`
}

export function gatewayRequestLogModelUnitPriceKey(item: GatewayRequestLogAnalytics["model_unit_prices"][number]) {
  return `${item.bucket}:${item.model_family}:${item.model_name}:${item.token_type}`
}

function reconcileRecordArray<T extends object>(current: T[], next: T[], keyOf: (item: T) => string): T[] {
  const previousByKey = new Map(current.map((item) => [keyOf(item), item]))
  let changed = current.length !== next.length
  const reconciled = next.map((item, index) => {
    const previous = previousByKey.get(keyOf(item))
    if (previous && shallowEqualRecord(previous, item)) {
      if (current[index] !== previous) changed = true
      return previous
    }
    changed = true
    return item
  })
  return changed ? reconciled : current
}

export function gatewayRequestLogExportURL(filters: GatewayRequestLogQueryFilters | undefined, timeZone: string) {
  if (!timeZone.trim()) throw new Error("系统时区尚未加载")
  const result = gatewayRequestLogQuery(undefined, undefined, filters, true, timeZone)
  if (result.error) throw result.error
  return `/gateway/request-logs/export${result.query ? `?${result.query}` : ""}`
}

function withGatewayRequestLogQueryError<T>(state: QueryState<T>, error: ZonedDateTimeError | null) {
  if (!error) return state
  return {
    ...state,
    data: null,
    loading: false,
    refreshing: false,
    error: error.message,
  }
}
